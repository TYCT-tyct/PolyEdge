use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand};
use tokio::net::UdpSocket;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};

use polyedge_hyper_mesh::health::{evaluate_health, HealthConfig};
use polyedge_hyper_mesh::protocol::HyperMeshFrame;
use polyedge_hyper_mesh::tracker::StreamTracker;

#[derive(Debug, Parser)]
#[command(name = "polyedge-hyper-mesh", version, about = "Standalone Hyper-Mesh transport runtime")]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Recv {
        #[arg(long, default_value = "0.0.0.0:9901")]
        bind: String,
        #[arg(long, default_value_t = 5)]
        report_sec: u64,
    },
    SendTest {
        #[arg(long, default_value = "127.0.0.1:9901")]
        target: String,
        #[arg(long, default_value = "tokyo-primary")]
        stream_id: String,
        #[arg(long, default_value = "BTCUSDT")]
        symbol: String,
        #[arg(long, default_value_t = 100.0)]
        price: f64,
        #[arg(long, default_value_t = 50)]
        interval_ms: u64,
        #[arg(long, default_value_t = 2000)]
        count: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.cmd {
        Command::Recv { bind, report_sec } => recv_loop(&bind, report_sec).await,
        Command::SendTest {
            target,
            stream_id,
            symbol,
            price,
            interval_ms,
            count,
        } => send_test(&target, &stream_id, &symbol, price, interval_ms, count).await,
    }
}

async fn recv_loop(bind: &str, report_sec: u64) -> Result<()> {
    let socket = UdpSocket::bind(bind)
        .await
        .with_context(|| format!("bind udp receiver failed: {bind}"))?;
    info!(bind, "hyper-mesh receiver listening");

    let mut trackers: HashMap<String, StreamTracker> = HashMap::new();
    let cfg = HealthConfig::default();
    let mut report_tick = interval(Duration::from_secs(report_sec.max(1)));
    report_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut buf = vec![0_u8; 2048];
    loop {
        tokio::select! {
            recv_res = socket.recv_from(&mut buf) => {
                let (n, _peer): (usize, SocketAddr) = match recv_res {
                    Ok(v) => v,
                    Err(err) => {
                        warn!(?err, "udp recv error");
                        continue;
                    }
                };
                let frame: HyperMeshFrame = match serde_json::from_slice(&buf[..n]) {
                    Ok(v) => v,
                    Err(err) => {
                        warn!(?err, "invalid hyper-mesh frame");
                        continue;
                    }
                };
                let recv_ms = Utc::now().timestamp_millis();
                let key = format!("{}:{}", frame.stream_id, frame.symbol.to_ascii_uppercase());
                trackers
                    .entry(key)
                    .or_default()
                    .observe(frame.seq, frame.ts_tokyo_send_ms, recv_ms);
            }
            _ = report_tick.tick() => {
                let now_ms = Utc::now().timestamp_millis();
                for (stream, tracker) in &trackers {
                    let snap = tracker.snapshot();
                    let total_expected = snap.received.saturating_add(snap.lost_frames);
                    let gap_rate = if total_expected > 0 {
                        snap.lost_frames as f64 / total_expected as f64
                    } else {
                        0.0
                    };
                    let stale_age_ms = snap
                        .last_recv_ms
                        .map(|ts| now_ms.saturating_sub(ts))
                        .unwrap_or(i64::MAX);
                    let state = evaluate_health(&cfg, gap_rate, snap.mean_latency_ms, stale_age_ms);
                    info!(
                        stream,
                        received = snap.received,
                        gap_count = snap.gap_count,
                        lost_frames = snap.lost_frames,
                        duplicate = snap.duplicate_count,
                        out_of_order = snap.out_of_order_count,
                        mean_latency_ms = format!("{:.2}", snap.mean_latency_ms),
                        stale_age_ms,
                        gap_rate = format!("{:.4}", gap_rate),
                        health = ?state,
                        "hyper-mesh stream report"
                    );
                }
            }
        }
    }
}

async fn send_test(
    target: &str,
    stream_id: &str,
    symbol: &str,
    price: f64,
    interval_ms: u64,
    count: u64,
) -> Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .context("bind udp sender failed")?;
    let mut ticker = interval(Duration::from_millis(interval_ms.max(1)));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    for seq in 1..=count {
        ticker.tick().await;
        let now_ms = Utc::now().timestamp_millis();
        let frame = HyperMeshFrame::new(stream_id, seq, now_ms, now_ms, now_ms, symbol, price);
        let payload = serde_json::to_vec(&frame)?;
        socket
            .send_to(&payload, target)
            .await
            .with_context(|| format!("send frame failed target={target}"))?;
        if seq % 500 == 0 {
            info!(seq, target, "hyper-mesh send-test progress");
        }
    }
    info!(target, count, "hyper-mesh send-test completed");
    Ok(())
}
