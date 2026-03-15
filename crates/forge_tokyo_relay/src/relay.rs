use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use core_types::RefPriceWsFeed;
use feed_reference::MultiSourceRefFeed;
use forge_wire::TokyoBinanceWire;
use futures::StreamExt;
use tokio::net::UdpSocket;
use tokio::time::sleep;

use crate::cli::Cli;
use crate::common::parse_upper_csv;

/* -----------------------------
 * 模块：东京 relay
 * 职责：采集参考价格并转发到爱尔兰 recorder
 * 边界：只处理东京输入与 UDP 输出，不接触交易/API 语义
 * ----------------------------- */

pub async fn run(args: Cli) -> Result<()> {
    let symbols = parse_upper_csv(&args.symbols);
    if symbols.is_empty() {
        anyhow::bail!("FORGE_SYMBOLS is empty");
    }

    let target: SocketAddr = args
        .ireland_udp
        .parse()
        .with_context(|| format!("invalid ireland udp addr: {}", args.ireland_udp))?;
    let socket = UdpSocket::bind(&args.bind)
        .await
        .with_context(|| format!("bind udp {}", args.bind))?;

    tracing::info!(
        bind = %args.bind,
        ireland_udp = %args.ireland_udp,
        ?symbols,
        redundancy = args.redundancy,
        "tokyo relay started"
    );

    const BACKOFF_BASE_MS: u64 = 500;
    const BACKOFF_MAX_MS: u64 = 30_000;
    let mut backoff_ms = BACKOFF_BASE_MS;
    let mut sent: u64 = 0;

    loop {
        let feed = MultiSourceRefFeed::new(Duration::from_millis(25));
        let mut stream = match feed.stream_ticks_ws(symbols.clone()).await {
            Ok(stream) => {
                backoff_ms = BACKOFF_BASE_MS;
                stream
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
                break;
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
            if !symbols.iter().any(|item| item == &symbol) {
                continue;
            }

            let now_ms = Utc::now().timestamp_millis();
            let msg = TokyoBinanceWire {
                ts_tokyo_recv_ms: tick.recv_ts_ms,
                ts_tokyo_send_ms: now_ms,
                ts_exchange_ms: tick.event_ts_exchange_ms.max(tick.event_ts_ms),
                symbol,
                binance_price: tick.price,
            };
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
