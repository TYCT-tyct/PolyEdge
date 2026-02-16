use anyhow::Result;
use futures::StreamExt;
use poly_wire::WireBookTop;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};
use tracing_subscriber::FmtSubscriber;

#[derive(Debug, Deserialize)]
struct BookTicker {
    u: u64, // updateId
    s: String,
    b: String,
    a: String,
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

fn process_match(id: u64, udp: u64, ws: u64, writer: &mut std::io::BufWriter<File>, stats: &mut Vec<f64>) {
    let delta = (udp as i64) - (ws as i64);
    let ms = delta as f64 / 1000.0;
    let faster = if ms < 0.0 { "UDP" } else { "WS" };

    // Log to CSV
    let _ = writeln!(writer, "{},{},{},{:.3},{}", id, udp, ws, ms, faster);

    // Stats
    stats.push(ms);

    info!("MATCH id={} | Delta: {:.3} ms {}", id, ms, if ms < 0.0 { "⚡ UDP FASTER ⚡" } else { "🐢 UDP SLOWER" });
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider().install_default().unwrap();
    tracing::subscriber::set_global_default(
        FmtSubscriber::builder()
            .with_max_level(tracing::Level::INFO)
            .finish(),
    )
    .expect("setting default subscriber failed");

    let symbol = std::env::var("SYMBOL").unwrap_or_else(|_| "btcusdt".to_string());
    let udp_port = 6666;

    info!("🚀 Starting Feed Benchmark (Symbol: {})", symbol);
    info!("   - Direct: Binance WS (Ireland)");
    info!("   - Relay:  UDP Port {} (Tokyo)", udp_port);

    // 1. Setup UDP Listener (Optimized)
    use socket2::{Socket, Domain, Type, Protocol};
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
    let ws_url = format!("wss://stream.binance.com:9443/ws/{}@bookTicker", symbol.to_lowercase());
    let (ws_stream, _) = connect_async(&ws_url).await?;
    info!("✅ WS Connected to {}", ws_url);
    let (_, mut read) = ws_stream.split();

    // 3. Shared State
    // Map<update_id, (ws_ts, udp_ts)>
    let tracker: Arc<Mutex<HashMap<u64, (Option<u64>, Option<u64>)>>> = Arc::new(Mutex::new(HashMap::new()));


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

    // Stats Shared
    let stats = Arc::new(Mutex::new(Vec::new()));
    let stats_clone = stats.clone();

    // Spawn Writer Thread
    std::thread::spawn(move || {
        let file = std::fs::File::create("latency_report.csv").unwrap();
        let mut writer = std::io::BufWriter::new(file);
        writeln!(writer, "update_id,udp_ts,ws_ts,delta_ms,faster_source").unwrap();

        while let Ok(entry) = rx_log.recv() {
            writeln!(writer, "{},{},{},{:.3},{}", entry.id, entry.udp, entry.ws, entry.delta, entry.faster).unwrap();
            stats_clone.lock().unwrap().push(entry.delta);

            // Only log extreme lag or periodic sample to avoid console spam?
            // Actually, we disable per-tick INFO log to keep console clean.
            // info!("MATCH id={} | Delta: {:.3} ms", entry.id, entry.delta);
        }
    });

    let tracker_udp = tracker.clone();
    let tracker_ws = tracker.clone();

    // 4. UDP Task
    tokio::spawn(async move {
        let mut buf = [0u8; 1024];
        let mut last_seq = 0;
        loop {
            match socket.recv_from(&mut buf).await {
                Ok((amt, _)) => {
                    let recv_ts = now_micros();
                    if let Ok(wire) = bincode::deserialize::<WireBookTop>(&buf[..amt]) {
                         // Gap Detection
                         if last_seq > 0 {
                            if wire.id > last_seq + 1 {
                               warn!("⚠️ UDP Gap: {} -> {} (Lost {})", last_seq, wire.id, wire.id - last_seq - 1);
                            } else if wire.id < last_seq {
                               warn!("⚠️ UDP Restart/Order: {} -> {}", last_seq, wire.id);
                            }
                         }
                         last_seq = wire.id;

                        let mut map = tracker_udp.lock().unwrap();
                        let entry = map.entry(wire.id).or_insert((None, None));
                        entry.1 = Some(recv_ts);

                        // Check match inside lock, but do NOT do I/O
                        let match_found = if let Some(ws_ts) = entry.0 {
                            Some(ws_ts)
                        } else {
                            None
                        };

                        if let Some(ws_ts) = match_found {
                             map.remove(&wire.id);
                             // Release lock immediately
                             // drop(map); // Removed as per instruction

                             let delta = (recv_ts as i64) - (ws_ts as i64);
                             let ms = delta as f64 / 1000.0;
                             let faster = if ms < 0.0 { "UDP" } else { "WS" };

                             tx_log_udp.send(LogEntry {
                                 id: wire.id, udp: recv_ts, ws: ws_ts, delta: ms, faster
                             }).unwrap();
                        }

                        // Prune (Optimized: Check size less often?)
                         if map.len() > 5000 {
                            map.retain(|k, _| *k > wire.id.saturating_sub(10000));
                        }
                    }
                }
                Err(e) => warn!("UDP Error: {}", e),
            }
        }
    });

    // 5. WS Loop
    let mut last_stat_time = std::time::Instant::now();

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                let recv_ts = now_micros();
                if let Ok(ticker) = serde_json::from_str::<BookTicker>(&text) {
                    let mut map = tracker_ws.lock().unwrap();
                    let entry = map.entry(ticker.u).or_insert((None, None));
                    entry.0 = Some(recv_ts);

                    let match_found = if let Some(udp_ts) = entry.1 {
                        Some(udp_ts)
                    } else {
                        None
                    };

                    if let Some(udp_ts) = match_found {
                        map.remove(&ticker.u);

                        let delta = (udp_ts as i64) - (recv_ts as i64);
                        let ms = delta as f64 / 1000.0;
                        let faster = if ms < 0.0 { "UDP" } else { "WS" };

                        tx_log_ws.send(LogEntry {
                             id: ticker.u, udp: udp_ts, ws: recv_ts, delta: ms, faster
                        }).unwrap();
                    }
                }
            }
             Ok(Message::Ping(_)) => {}
            _ => {}
        }

        // Periodic Stats Log (Every 10s)
        if last_stat_time.elapsed().as_secs() >= 10 {
            let mut s = stats.lock().unwrap();
            if !s.is_empty() {
                s.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let len = s.len();
                let p50 = s[len / 2];
                let p90 = s[(len as f64 * 0.9) as usize];
                let p99 = s[(len as f64 * 0.99) as usize];
                let udp_fast_count = s.iter().filter(|&&x| x < 0.0).count();

                info!("📊 STATS (Last 10s): Count={} | P50={:.3}ms | P99={:.3}ms | UDP Faster: {}/{} ({:.1}%)",
                    len, p50, p99, udp_fast_count, len, (udp_fast_count as f64 / len as f64) * 100.0);

                s.clear();
            }
            last_stat_time = std::time::Instant::now();
        }
    }

    Ok(())
}
