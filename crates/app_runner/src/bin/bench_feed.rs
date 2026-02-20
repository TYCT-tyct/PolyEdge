use anyhow::Result;
use futures::StreamExt;
use poly_wire::{
    decode_auto, WirePacket, WIRE_BOOK_TOP24_SIZE, WIRE_MAX_PACKET_SIZE, WIRE_MOMENTUM_TICK32_SIZE,
    WIRE_RELAY_TICK40_SIZE,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};
use tracing_subscriber::FmtSubscriber;

#[derive(Debug, Deserialize)]
struct BookTicker {
    #[serde(default, rename = "E")]
    event_time_ms: u64,
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

#[inline]
fn percentile(sorted: &[f64], q: f64) -> f64 {
    let idx = (((sorted.len() - 1) as f64) * q.clamp(0.0, 1.0)).round() as usize;
    sorted[idx]
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .unwrap();
    tracing::subscriber::set_global_default(
        FmtSubscriber::builder()
            .with_max_level(tracing::Level::INFO)
            .finish(),
    )
    .expect("setting default subscriber failed");

    let symbol = std::env::var("SYMBOL").unwrap_or_else(|_| "btcusdt".to_string());
    let udp_port = 7777;

    info!("🚀 Starting Feed Benchmark (Symbol: {})", symbol);
    info!("   - Direct: Binance WS (Ireland)");
    info!("   - Relay:  UDP Port {} (Tokyo)", udp_port);

    // 1. Setup UDP Listener (Optimized)
    use socket2::{Domain, Protocol, Socket, Type};
    use std::net::SocketAddr;

    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    let _ = socket.set_recv_buffer_size(8 * 1024 * 1024); // 8MB
    let _ = socket.set_nonblocking(true);
    let addr_str = format!("0.0.0.0:{}", udp_port);
    let addr: SocketAddr = addr_str.parse()?;
    #[cfg(target_os = "linux")]
    let _ = socket.set_reuse_port(true);
    let _ = socket.set_reuse_address(true);
    socket.bind(&addr.into())?;

    let socket = UdpSocket::from_std(socket.into())?;

    info!("✅ UDP Listening on {} (Buffer: 8MB)", addr_str);

    // 2. Setup WS Listener
    let ws_url = format!(
        "wss://fstream.binance.com/ws/{}@bookTicker",
        symbol.to_lowercase()
    );
    let (ws_stream, _) = connect_async(&ws_url).await?;
    info!("✅ WS Connected to {}", ws_url);
    let (_, mut read) = ws_stream.split();

    // 3. Shared State
    // Map<event_ts_ms, (ws_recv_ts, udp_recv_ts)>
    let tracker: Arc<Mutex<HashMap<u64, (Option<u64>, Option<u64>)>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // I/O Thread Setup
    struct LogEntry {
        id: u64,
        udp: u64,
        ws: u64,
        delta: f64,
        faster: &'static str,
    }

    let (tx_log, rx_log) = std::sync::mpsc::channel::<LogEntry>();
    let tx_log_udp = tx_log.clone();
    let tx_log_ws = tx_log.clone();
    let (tx_delta, rx_delta) = std::sync::mpsc::channel::<f64>();
    let tx_delta_udp = tx_delta.clone();
    let tx_delta_ws = tx_delta.clone();

    // Spawn Writer Thread
    std::thread::spawn(move || {
        let file = std::fs::File::create("latency_report.csv").unwrap();
        let mut writer = std::io::BufWriter::new(file);
        writeln!(writer, "event_ts_ms,udp_ts,ws_ts,delta_ms,faster_source").unwrap();

        while let Ok(entry) = rx_log.recv() {
            writeln!(
                writer,
                "{},{},{},{:.3},{}",
                entry.id, entry.udp, entry.ws, entry.delta, entry.faster
            )
            .unwrap();
        }
    });

    let tracker_udp = tracker.clone();
    let tracker_ws = tracker.clone();

    // 4. UDP Task
    tokio::spawn(async move {
        let mut buf = [0u8; WIRE_MAX_PACKET_SIZE];
        let mut last_event_ts_ms = 0_u64;
        loop {
            match socket.recv_from(&mut buf).await {
                Ok((amt, _)) => {
                    let recv_ts = now_micros();
                    if amt != WIRE_BOOK_TOP24_SIZE
                        && amt != WIRE_MOMENTUM_TICK32_SIZE
                        && amt != WIRE_RELAY_TICK40_SIZE
                    {
                        continue;
                    }
                    if let Ok(packet) = decode_auto(&buf[..amt]) {
                        let event_ts_ms = match packet {
                            WirePacket::BookTop24(v) => v.ts_micros / 1_000,
                            WirePacket::MomentumTick32(v) => v.ts_micros / 1_000,
                            WirePacket::RelayTick40(v) => v.ts_micros / 1_000,
                        };
                        if last_event_ts_ms > 0 && event_ts_ms < last_event_ts_ms {
                            warn!(
                                "⚠️ UDP timestamp backjump: {} -> {}",
                                last_event_ts_ms, event_ts_ms
                            );
                        }
                        last_event_ts_ms = event_ts_ms;
                        let mut map = tracker_udp.lock().await;
                        let entry = map.entry(event_ts_ms).or_insert((None, None));
                        entry.1 = Some(recv_ts);

                        // Check match inside lock, but do NOT do I/O
                        let match_found = entry.0;

                        if let Some(ws_ts) = match_found {
                            map.remove(&event_ts_ms);

                            let delta = (recv_ts as i64) - (ws_ts as i64);
                            let ms = delta as f64 / 1000.0;
                            let faster = if ms < 0.0 { "UDP" } else { "WS" };

                            let _ = tx_log_udp.send(LogEntry {
                                id: event_ts_ms,
                                udp: recv_ts,
                                ws: ws_ts,
                                delta: ms,
                                faster,
                            });
                            let _ = tx_delta_udp.send(ms);
                        }

                        // Prune (Optimized: Check size less often?)
                        if map.len() > 5000 {
                            map.retain(|k, _| *k > event_ts_ms.saturating_sub(10_000));
                        }
                    }
                }
                Err(e) => warn!("UDP Error: {}", e),
            }
        }
    });

    // 5. WS Loop
    let mut last_stat_time = std::time::Instant::now();
    let mut stats_window: Vec<f64> = Vec::with_capacity(16_384);

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                let recv_ts = now_micros();
                if let Ok(ticker) = serde_json::from_str::<BookTicker>(&text) {
                    if ticker.event_time_ms == 0 {
                        continue;
                    }
                    let mut map = tracker_ws.lock().await;
                    let entry = map.entry(ticker.event_time_ms).or_insert((None, None));
                    entry.0 = Some(recv_ts);

                    let match_found = entry.1;

                    if let Some(udp_ts) = match_found {
                        map.remove(&ticker.event_time_ms);

                        let delta = (udp_ts as i64) - (recv_ts as i64);
                        let ms = delta as f64 / 1000.0;
                        let faster = if ms < 0.0 { "UDP" } else { "WS" };

                        let _ = tx_log_ws.send(LogEntry {
                            id: ticker.event_time_ms,
                            udp: udp_ts,
                            ws: recv_ts,
                            delta: ms,
                            faster,
                        });
                        let _ = tx_delta_ws.send(ms);
                    }

                    if map.len() > 5000 {
                        map.retain(|k, _| *k > ticker.event_time_ms.saturating_sub(10_000));
                    }
                }
            }
            Ok(Message::Ping(_)) => {}
            _ => {}
        }

        // Periodic Stats Log (Every 10s)
        if last_stat_time.elapsed().as_secs() >= 10 {
            while let Ok(delta) = rx_delta.try_recv() {
                stats_window.push(delta);
            }
            if !stats_window.is_empty() {
                stats_window.sort_by(|a, b| a.total_cmp(b));
                let len = stats_window.len();
                let p50 = percentile(&stats_window, 0.50);
                let p90 = percentile(&stats_window, 0.90);
                let p99 = percentile(&stats_window, 0.99);
                let udp_fast_count = stats_window.iter().filter(|&&x| x < 0.0).count();

                info!(
                    "📊 STATS (Last 10s): Count={} | P50={:.3}ms | P90={:.3}ms | P99={:.3}ms | UDP Faster: {}/{} ({:.1}%)",
                    len,
                    p50,
                    p90,
                    p99,
                    udp_fast_count,
                    len,
                    (udp_fast_count as f64 / len as f64) * 100.0
                );

                stats_window.clear();
            }
            last_stat_time = std::time::Instant::now();
        }
    }

    Ok(())
}
