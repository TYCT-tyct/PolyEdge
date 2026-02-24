use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use core_types::RefPriceWsFeed;
use feed_reference::MultiSourceRefFeed;
use futures::StreamExt;
use tokio::net::UdpSocket;

use crate::cli::TokyoRelayArgs;
use crate::common::parse_upper_csv;
use crate::models::TokyoBinanceWire;

pub async fn run_tokyo_relay(args: TokyoRelayArgs) -> Result<()> {
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

    let feed = MultiSourceRefFeed::new(Duration::from_millis(25));
    let mut stream = feed.stream_ticks_ws(symbols.clone()).await?;

    let mut sent: u64 = 0;
    loop {
        let Some(next) = stream.next().await else {
            anyhow::bail!("binance stream ended");
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
}
