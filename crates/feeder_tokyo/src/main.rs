use anyhow::Result;
use futures::StreamExt;
use poly_wire::{WireBookTop, now_micros};
use serde::Deserialize;
use tokio::net::UdpSocket;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
struct BinanceBookTicker {
    u: u64, // order book updateId
    s: String, // symbol
    b: String, // best bid price
    B: String, // best bid qty
    a: String, // best ask price
    A: String, // best ask qty
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider().install_default().unwrap();
    // Configuration
    let target = std::env::var("TARGET").unwrap_or_else(|_| "10.0.3.123:6666".to_string());
    let symbol = std::env::var("SYMBOL").unwrap_or_else(|_| "btcusdt".to_string());

    println!("🚀 Tokyo Feeder Starting...");
    println!("Target: {}", target);
    println!("Symbol: {}", symbol);

    loop {
        match run_feeder(&target, &symbol).await {
            Ok(_) => {
                eprintln!("⚠️ Stream ended unexpectedly. Restarting in 5s...");
            }
            Err(e) => {
                eprintln!("❌ Stream error: {:#}. Restarting in 5s...", e);
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        println!("🔄 Reconnecting...");
    }
}

async fn run_feeder(target: &str, symbol: &str) -> Result<()> {
    // UDP Setup (Optimized with socket2)
    use socket2::{Socket, Domain, Type, Protocol};
    use std::net::SocketAddr;

    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    // Set Send Buffer to 8MB (Kernel might cap this, but we request it)
    let _ = socket.set_send_buffer_size(8 * 1024 * 1024);
    let _ = socket.set_nonblocking(true);

    // Bind to 0.0.0.0:0 (Any port)
    let addr: SocketAddr = "0.0.0.0:0".parse()?;
    socket.bind(&addr.into())?;

    // Convert to Tokio
    let socket = UdpSocket::from_std(socket.into())?;
    socket.connect(target).await?;

    // WebSocket Setup
    let url = format!("wss://stream.binance.com:9443/ws/{}@bookTicker", symbol.to_lowercase());
    let (mut ws_stream, _) = connect_async(&url).await?;
    println!("✅ Connected to Binance WS: {}", url);

    // Stats
    let mut count = 0;
    let mut last_log = std::time::Instant::now();

    while let Some(msg) = ws_stream.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                // 1. Parse JSON
                if let Ok(ticker) = serde_json::from_str::<BinanceBookTicker>(&text) {
                     // 2. Convert to Wire Format
                    let bid = ticker.b.parse::<f64>().unwrap_or_default();
                    let ask = ticker.a.parse::<f64>().unwrap_or_default();

                     if bid > 0.0 && ask > 0.0 {
                         let wire = WireBookTop {
                            bid,
                            ask,
                            ts: now_micros(),
                            id: ticker.u,
                         };

                        // 3. Serialize Binary
                        let encoded = bincode::serialize(&wire)?;

                        // 4. Send UDP
                        socket.send(&encoded).await?;

                        count += 1;
                    }
                }
            }
            Ok(Message::Ping(_)) => {
                 // Auto-pong handled
            }
            Ok(Message::Close(_)) => {
                return Err(anyhow::anyhow!("WebSocket closed by server"));
            }
            Err(e) => {
                return Err(anyhow::anyhow!("WebSocket error: {}", e));
            }
            _ => {}
        }

        if last_log.elapsed().as_secs() >= 10 {
            println!("🔥 Sent {} ticks in last 10s", count);
            count = 0;
            last_log = std::time::Instant::now();
        }
    }

    Ok(())
}
