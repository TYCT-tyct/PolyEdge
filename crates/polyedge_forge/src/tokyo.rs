use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use core_types::RefPriceWsFeed;
use feed_reference::MultiSourceRefFeed;
use futures::StreamExt;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::cli::TokyoRelayArgs;
use crate::common::parse_upper_csv;
use crate::models::{
    compute_tokyo_wire_checksum, tokyo_packet_type_tick, TokyoBinanceWire, TokyoReplayRequestWire,
};

async fn tokyo_replay_control_loop(
    socket: Arc<UdpSocket>,
    replay_cache: Arc<RwLock<VecDeque<TokyoBinanceWire>>>,
) {
    let mut buf = vec![0_u8; 2048];
    loop {
        let (n, peer) = match socket.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(?err, "tokyo relay control recv failed");
                continue;
            }
        };
        let req: TokyoReplayRequestWire = match serde_json::from_slice(&buf[..n]) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if req.packet_type != "replay_request" {
            continue;
        }
        let replay_rows: Vec<TokyoBinanceWire> = {
            let cache = replay_cache.read().await;
            cache
                .iter()
                .filter(|row| {
                    row.symbol.eq_ignore_ascii_case(&req.symbol)
                        && row.relay_seq >= req.from_relay_seq
                        && row.relay_seq <= req.to_relay_seq
                })
                .cloned()
                .collect()
        };
        if replay_rows.is_empty() {
            continue;
        }
        for row in replay_rows {
            let Ok(payload) = serde_json::to_vec(&row) else {
                continue;
            };
            if let Err(err) = socket.send_to(&payload, peer).await {
                tracing::warn!(?err, peer = %peer, "tokyo replay resend failed");
                break;
            }
        }
    }
}

pub async fn run_tokyo_relay(args: TokyoRelayArgs) -> Result<()> {
    let symbols = parse_upper_csv(&args.symbols);
    if symbols.is_empty() {
        anyhow::bail!("FORGE_SYMBOLS is empty");
    }
    let target: SocketAddr = args
        .ireland_udp
        .parse()
        .with_context(|| format!("invalid ireland udp addr: {}", args.ireland_udp))?;
    let socket = Arc::new(
        UdpSocket::bind(&args.bind)
            .await
            .with_context(|| format!("bind udp {}", args.bind))?,
    );
    let replay_cache_cap = std::env::var("FORGE_TOKYO_REPLAY_CACHE_SIZE")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(32_768)
        .clamp(4_096, 262_144);
    let replay_cache = Arc::new(RwLock::new(VecDeque::<TokyoBinanceWire>::with_capacity(
        replay_cache_cap.min(65_536),
    )));
    tokio::spawn(tokyo_replay_control_loop(
        Arc::clone(&socket),
        Arc::clone(&replay_cache),
    ));

    tracing::info!(
        bind = %args.bind,
        ireland_udp = %args.ireland_udp,
        ?symbols,
        redundancy = args.redundancy,
        "tokyo relay started"
    );

    // 指数退避重连参数
    const BACKOFF_BASE_MS: u64 = 500;
    const BACKOFF_MAX_MS: u64 = 30_000;
    let mut backoff_ms = BACKOFF_BASE_MS;
    let mut sent: u64 = 0;
    let mut relay_seq: u64 = 0;

    loop {
        let feed = MultiSourceRefFeed::new(Duration::from_millis(25));
        let mut stream = match feed.stream_ticks_ws(symbols.clone()).await {
            Ok(s) => {
                backoff_ms = BACKOFF_BASE_MS;
                s
            }
            Err(err) => {
                tracing::warn!(?err, backoff_ms, "tokyo relay ws connect failed, retrying");
                sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(BACKOFF_MAX_MS);
                continue;
            }
        };

        loop {
            let Some(next) = stream.next().await else {
                tracing::warn!(backoff_ms, "binance stream ended, reconnecting");
                break; // 退出内层 loop，进入外层重连
            };
            let tick = match next {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(?err, "tokyo relay stream error");
                    continue;
                }
            };
            if tick.source.as_str() != "binance_ws" {
                continue;
            }
            let symbol = tick.symbol.to_ascii_uppercase();
            if !symbols.iter().any(|s| s == &symbol) {
                continue;
            }

            let now_ms = Utc::now().timestamp_millis();
            relay_seq = relay_seq.saturating_add(1);
            let mut msg = TokyoBinanceWire {
                packet_type: tokyo_packet_type_tick(),
                relay_seq,
                source_seq: tick.source_seq,
                source: tick.source.to_string(),
                ts_tokyo_recv_ms: tick.recv_ts_ms,
                ts_tokyo_send_ms: now_ms,
                ts_exchange_ms: tick.event_ts_exchange_ms.max(tick.event_ts_ms),
                symbol,
                binance_price: tick.price,
                checksum: String::new(),
            };
            msg.checksum = compute_tokyo_wire_checksum(&msg);
            {
                let mut cache = replay_cache.write().await;
                cache.push_back(msg.clone());
                while cache.len() > replay_cache_cap {
                    cache.pop_front();
                }
            }
            let payload = serde_json::to_vec(&msg)?;
            for _ in 0..args.redundancy.max(1) {
                if let Err(err) = socket.send_to(&payload, target).await {
                    tracing::warn!(?err, "tokyo relay udp send failed");
                }
            }
            sent = sent.saturating_add(1);
            if sent.is_multiple_of(500) {
                tracing::info!(sent, "tokyo relay sent packets");
            }
        }

        sleep(Duration::from_millis(backoff_ms)).await;
        backoff_ms = (backoff_ms * 2).min(BACKOFF_MAX_MS);
    }
}
