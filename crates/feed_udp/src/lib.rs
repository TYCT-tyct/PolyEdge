use anyhow::{Result, Context};
use async_trait::async_trait;
use core_types::{DynStream, RefPriceFeed, RefTick};
use futures::stream;
use poly_wire::WireBookTop;
use std::net::SocketAddr;
use std::time::Instant;
use tokio::net::UdpSocket;
use tracing::{info, warn, error, debug};

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
        let addr_str = format!("0.0.0.0:{}", port);

        // ============================================================
        // ULTRA-OPTIMIZED UDP RECEIVE SOCKET
        // ============================================================
        use socket2::{Socket, Domain, Type, Protocol};

        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;

        // 1. 巨大接收缓冲区 (32MB) - 应对突发流量和反压
        socket.set_recv_buffer_size(32 * 1024 * 1024)?;

        // 2. 快速ACK响应
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let fd = socket.as_raw_fd();
            // 设置 UDP 快速回收 (减少TIME_WAIT状态)
            let quick_ack: libc::c_int = 1;
            let _ = libc::setsockopt(
                fd,
                libc::IPPROTO_UDP,
                0x09, // UDP_GRO (如果内核支持)
                &quick_ack as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }

        // 3. 端口复用和快速重启
        #[cfg(target_os = "linux")]
        let _ = socket.set_reuse_port(true);
        let _ = socket.set_reuse_address(true);

        // 4. 非阻塞模式
        socket.set_nonblocking(true)?;

        let addr: SocketAddr = addr_str.parse()?;
        socket.bind(&addr.into())?;

        // Convert to Tokio
        let socket = UdpSocket::from_std(socket.into())?;

        info!("🚀 UDP Feed Listening on {} (Ultra Buffer: 32MB)", addr_str);

        // 预分配 buffer - 避免每次分配
        // 使用 Page-aligned buffer for potential DMA
        let mut buf = vec![0u8; 4096]; // 4KB 预分配，可复用于小包

        // 使用更大的 channel 应对突发
        let (tx, rx) = tokio::sync::mpsc::channel(4096);

        // 统计
        let stats = std::sync::Arc::new(std::sync::Mutex::new(Stats::new()));
        let stats_clone = stats.clone();

        // Spawn Background Task
        tokio::spawn(async move {
            let mut last_seq = 0u64;
            let mut last_log = Instant::now();
            let mut recv_count = 0u64;
            let mut error_count = 0u64;
            let mut gap_count = 0u64;

            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((amt, src)) => {
                        if amt == 0 {
                            continue;
                        }

                        // 高速二进制解析
                        match bincode::deserialize::<WireBookTop>(&buf[..amt]) {
                            Ok(wire) => {
                                // Gap Detection
                                if last_seq > 0 {
                                    if wire.id > last_seq + 1 {
                                        gap_count += 1;
                                        if gap_count % 100 == 0 {
                                            warn!("⚠️ UDP Gap: {} -> {} (Lost {})",
                                                last_seq, wire.id, wire.id - last_seq - 1);
                                        }
                                    } else if wire.id < last_seq {
                                        // 序列重置，正常情况
                                        debug!("🔄 UDP Seq reset: {} -> {}", last_seq, wire.id);
                                    }
                                }
                                last_seq = wire.id;
                                recv_count += 1;

                                // Convert to RefTick - 使用高性能时间戳
                                let mid = (wire.bid + wire.ask) / 2.0;
                                let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_nanos() as i64;

                                let tick = RefTick {
                                    source: "binance_udp".to_string(),
                                    symbol: "BTCUSDT".to_string(),
                                    event_ts_ms: (wire.ts / 1000) as i64,
                                    recv_ts_ms: now / 1_000_000,
                                    source_seq: wire.id,
                                    event_ts_exchange_ms: (wire.ts / 1000) as i64,
                                    recv_ts_local_ns: now,
                                    ingest_ts_local_ns: 0,
                                    price: mid,
                                };

                                if tx.send(Ok(tick)).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                error_count += 1;
                                if error_count % 1000 == 0 {
                                    warn!("UDP Decode Error: {} ({} errors)", e, error_count);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error_count += 1;
                        if error_count < 10 || error_count % 1000 == 0 {
                            error!("UDP Recv Error: {}", e);
                        }
                        tokio::time::sleep(std::time::Duration::from_micros(100)).await;
                    }
                }

                // 每10秒打印统计
                if last_log.elapsed().as_secs() >= 10 {
                    let mut s = stats_clone.lock().unwrap();
                    s.recv_count = recv_count;
                    s.error_count = error_count;
                    s.gap_count = gap_count;
                    info!("📊 UDP: {} msg/s, {} errors, {} gaps",
                        recv_count / 10, error_count, gap_count);

                    recv_count = 0;
                    error_count = 0;
                    gap_count = 0;
                    last_log = Instant::now();
                }
            }
        });

        // Convert receiver to Stream
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Box::pin(stream))
    }
}

#[derive(Debug)]
struct Stats {
    recv_count: u64,
    error_count: u64,
    gap_count: u64,
}

impl Stats {
    fn new() -> Self {
        Self {
            recv_count: 0,
            error_count: 0,
            gap_count: 0,
        }
    }
}
