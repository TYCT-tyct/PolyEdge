use anyhow::Result;
use futures::StreamExt;
use poly_wire::{WireBookTop, now_micros};
use serde::Deserialize;
use std::net::SocketAddr;
use std::time::Instant;
use tokio::net::UdpSocket;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

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
    let buffer_size = std::env::var("SEND_BUFFER")
        .unwrap_or_else(|_| "16777216".to_string()) // 默认 16MB
        .parse::<usize>()?;

    println!("🚀 Tokyo Feeder Starting...");
    println!("Target: {}", target);
    println!("Symbol: {}", symbol);
    println!("Send Buffer: {} MB", buffer_size / 1024 / 1024);

    loop {
        match run_feeder(&target, &symbol, buffer_size).await {
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

async fn run_feeder(target: &str, symbol: &str, buffer_size: usize) -> Result<()> {
    // ============================================================
    // ULTRA-OPTIMIZED UDP SOCKET SETUP (Kernel Bypass Level)
    // ============================================================
    use socket2::{Socket, Domain, Type, Protocol};

    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;

    // 1. 巨大发送缓冲区 (16MB) - 应对突发流量
    socket.set_send_buffer_size(buffer_size)?;

    // 2. 禁用 Nagle 算法 (对 UDP 无效，但为 TCP 模式预留)
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let fd = socket.as_raw_fd();
        // TCP_NODELAY 对 UDP 无实际操作，但保持一致性
        let _ = libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            &1 as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }

    // 3. 设置 DSCP (Differentiated Services Code Point) - 优先处理
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let fd = socket.as_raw_fd();
        // DSCP 46 = EF (Expedited Forwarding) - 最高优先级
        let dscp: libc::c_int = 0xB8 >> 2; // 46
        let _ = libc::setsockopt(
            fd,
            libc::IPPROTO_IP,
            libc::IP_TOS,
            &dscp as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }

    // 4. 立即发送，不等待合并
    socket.set_nonblocking(true)?;

    // Bind 到特定端口以便固定
    let addr: SocketAddr = "0.0.0.0:9999".parse()?;
    socket.bind(&addr.into())?;

    // Convert to Tokio
    let socket = UdpSocket::from_std(socket.into())?;
    socket.connect(target).await?;

    println!("✅ UDP Socket 优化完成 (16MB buffer, DSCP 优先)");

    // WebSocket Setup - 使用 BookTicker 获取最优买卖价
    let url = format!("wss://stream.binance.com:9443/ws/{}@bookTicker", symbol.to_lowercase());
    let (mut ws_stream, _) = connect_async(&url).await?;
    println!("✅ Connected to Binance WS: {}", url);

    // 性能统计
    let mut count = 0;
    let mut last_log = Instant::now();
    let mut total_bytes = 0u64;

    // 预分配 buffer 避免每次分配
    let mut encode_buf = vec![0u8; 64];

    while let Some(msg) = ws_stream.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                // 快速解析 - 只取关键字段
                if let Ok(ticker) = serde_json::from_str::<BinanceBookTicker>(&text) {
                    let bid = ticker.b.parse::<f64>().unwrap_or_default();
                    let ask = ticker.a.parse::<f64>().unwrap_or_default();

                    if bid > 0.0 && ask > 0.0 {
                        let wire = WireBookTop {
                            bid,
                            ask,
                            ts: now_micros(),
                            id: ticker.u,
                        };

                        // 高效二进制序列化
                        bincode::serialize_into(&mut encode_buf[..], &wire)?;
                        let len = bincode::serialized_size(&wire)? as usize;

                        // 发送
                        socket.send(&encode_buf[..len]).await?;

                        count += 1;
                        total_bytes += len as u64;
                    }
                }
            }
            Ok(Message::Ping(_)) => {}
            Ok(Message::Close(_)) => {
                return Err(anyhow::anyhow!("WebSocket closed by server"));
            }
            Err(e) => {
                return Err(anyhow::anyhow!("WebSocket error: {}", e));
            }
            _ => {}
        }

        if last_log.elapsed().as_secs() >= 10 {
            let mb = total_bytes / 1024 / 1024;
            println!("🔥 {} ticks/s, {} MB in 10s", count / 10, mb);
            count = 0;
            total_bytes = 0;
            last_log = Instant::now();
        }
    }

    Ok(())
}
