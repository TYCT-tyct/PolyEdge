use anyhow::{Result, Context};
use async_trait::async_trait;
use core_types::{DynStream, RefPriceFeed, RefTick};
use futures::stream;
use poly_wire::WireBookTop;
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tracing::{info, warn, error};

pub struct UdpBinanceFeed {
    port: u16,
}

impl UdpBinanceFeed {
    pub fn new(port: u16) -> Self {
        Self { port }
    }
}

#[async_trait]
impl RefPriceFeed for UdpBinanceFeed {
    async fn stream_ticks(&self, symbols: Vec<String>) -> Result<DynStream<RefTick>> {
        let port = self.port;
        // We bind to 0.0.0.0
        let addr_str = format!("0.0.0.0:{}", port);

        // Optimized UDP Socket with socket2
        use socket2::{Socket, Domain, Type, Protocol};
        use std::net::SocketAddr;

        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        // Set Receive Buffer to 8MB to handle bursts
        let _ = socket.set_recv_buffer_size(8 * 1024 * 1024);
        let _ = socket.set_nonblocking(true);

        // Reuse Address/Port if possible (optional but good for restarts)
        #[cfg(target_os = "linux")]
        let _ = socket.set_reuse_port(true);
        let _ = socket.set_reuse_address(true);

        let addr: SocketAddr = addr_str.parse()?;
        socket.bind(&addr.into())?;

        // Convert to Tokio
        let socket = UdpSocket::from_std(socket.into())?;

        info!("🚀 UDP Feed Listening on {} (Optimized Buffer: 8MB)", addr_str);

        // Use async-stream or channel to bridge UdpSocket loop to Stream
        // For simplicity, we use channel
        let (tx, rx) = tokio::sync::mpsc::channel(1024);

        // Spawn Background Task
        tokio::spawn(async move {
            let mut buf = [0u8; 1024]; // Max MTU usually 1500
            let mut last_seq = 0;

            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((amt, _src)) => {
                        // Zero-copy deserialization if possible, but here we just decode
                        match bincode::deserialize::<WireBookTop>(&buf[..amt]) {
                            Ok(wire) => {
                                // Gap Detection
                                if last_seq > 0 {
                                    if wire.id > last_seq + 1 {
                                       warn!("⚠️ UDP Gap: {} -> {} (Lost {})", last_seq, wire.id, wire.id - last_seq - 1);
                                    } else if wire.id < last_seq {
                                       warn!("⚠️ UDP Restart/Order: {} -> {}", last_seq, wire.id);
                                    }
                                }
                                last_seq = wire.id;

                                // Convert to RefTick
                                // Calculate Mid Price
                                let mid = (wire.bid + wire.ask) / 2.0;

                                let tick = RefTick {
                                    source: "binance_udp".to_string(),
                                    symbol: "BTCUSDT".to_string(), // TODO: Support multiple? Wire format has no symbol.
                                    event_ts_ms: (wire.ts / 1000) as i64,
                                    recv_ts_ms: chrono::Utc::now().timestamp_millis(),
                                    source_seq: wire.id,
                                    event_ts_exchange_ms: (wire.ts / 1000) as i64,
                                    recv_ts_local_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                                    ingest_ts_local_ns: 0, // Set by engine
                                    price: mid,
                                };

                                if tx.send(Ok(tick)).await.is_err() {
                                    break; // Receiver dropped
                                }
                            }
                            Err(e) => {
                                warn!("UDP Decode Error: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("UDP Recv Error: {}", e);
                        // Backoff or break? Usually strict loop.
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
        });

        // Convert receiver to Stream
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Box::pin(stream))
    }
}
