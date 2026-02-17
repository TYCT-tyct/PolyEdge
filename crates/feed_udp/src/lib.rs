use anyhow::{Context, Result};
use async_trait::async_trait;
use core_types::{DynStream, RefPriceFeed, RefTick};
use poly_wire::{decode_book_top24, WIRE_BOOK_TOP24_SIZE};
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

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
        let symbol = symbols
            .first()
            .cloned()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "BTCUSDT".to_string());

        let addr: SocketAddr = format!("0.0.0.0:{}", self.port)
            .parse()
            .context("parse udp bind addr")?;
        let socket = UdpSocket::bind(addr).await.context("bind udp feed socket")?;
        let (tx, rx) = mpsc::channel::<Result<RefTick>>(16_384);

        tokio::spawn(async move {
            let mut buf = [0u8; WIRE_BOOK_TOP24_SIZE];
            loop {
                let recv = socket.recv_from(&mut buf).await;
                let (amt, _) = match recv {
                    Ok(v) => v,
                    Err(err) => {
                        let _ = tx.send(Err(err.into())).await;
                        continue;
                    }
                };
                if amt != WIRE_BOOK_TOP24_SIZE {
                    continue;
                }
                let packet = match decode_book_top24(&buf) {
                    Ok(pkt) => pkt,
                    Err(err) => {
                        let _ = tx.send(Err(err.into())).await;
                        continue;
                    }
                };

                let recv_ns = now_ns();
                let event_ms = (packet.ts_micros / 1_000) as i64;
                let mid = (packet.bid + packet.ask) * 0.5;
                if !mid.is_finite() || mid <= 0.0 {
                    continue;
                }

                let tick = RefTick {
                    source: "binance_udp".to_string(),
                    symbol: symbol.clone(),
                    event_ts_ms: event_ms,
                    recv_ts_ms: recv_ns / 1_000_000,
                    source_seq: stable_udp_seq(packet.ts_micros, packet.bid, packet.ask),
                    event_ts_exchange_ms: event_ms,
                    recv_ts_local_ns: recv_ns,
                    ingest_ts_local_ns: now_ns(),
                    price: mid,
                };

                if tx.send(Ok(tick)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

#[inline]
fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

#[inline]
fn stable_udp_seq(ts_micros: u64, bid: f64, ask: f64) -> u64 {
    let mut h = ts_micros
        ^ bid.to_bits().rotate_left(13)
        ^ ask.to_bits().rotate_right(7)
        ^ 0x9E37_79B9_7F4A_7C15;
    if h == 0 {
        h = 1;
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_udp_seq_is_non_zero() {
        assert_ne!(stable_udp_seq(0, 1.0, 1.0), 0);
    }
}

