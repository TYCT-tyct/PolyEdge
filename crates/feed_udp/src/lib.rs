use anyhow::{Context, Result};
use async_trait::async_trait;
use core_types::{DynStream, RefPriceFeed, RefTick};
use poly_wire::{decode_book_top24, WIRE_BOOK_TOP24_SIZE};
use std::collections::HashMap;
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
        let (tx, rx) = mpsc::channel::<Result<RefTick>>(16_384);
        let normalized_symbols = normalize_symbols(&symbols);
        let bindings = udp_bindings(self.port, &normalized_symbols);
        if bindings.is_empty() {
            let symbol = normalized_symbols
                .first()
                .cloned()
                .unwrap_or_else(|| "BTCUSDT".to_string());
            let addr: SocketAddr = format!("0.0.0.0:{}", self.port)
                .parse()
                .context("parse udp bind addr")?;
            let socket = UdpSocket::bind(addr).await.context("bind udp feed socket")?;
            spawn_recv_loop(socket, symbol, tx.clone());
        } else {
            for (symbol, port) in bindings {
                let addr: SocketAddr = format!("0.0.0.0:{port}")
                    .parse()
                    .context("parse udp bind addr")?;
                let socket = UdpSocket::bind(addr).await.with_context(|| {
                    format!("bind udp feed socket for symbol={symbol} port={port}")
                })?;
                spawn_recv_loop(socket, symbol, tx.clone());
            }
        }
        drop(tx);

        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

fn spawn_recv_loop(socket: UdpSocket, symbol: String, tx: mpsc::Sender<Result<RefTick>>) {
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
}

fn normalize_symbols(symbols: &[String]) -> Vec<String> {
    let mut out: Vec<String> = symbols
        .iter()
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    if out.is_empty() {
        out.push("BTCUSDT".to_string());
    }
    out
}

fn udp_bindings(default_port: u16, symbols: &[String]) -> Vec<(String, u16)> {
    let raw = std::env::var("POLYEDGE_UDP_SYMBOL_PORTS").unwrap_or_default();
    udp_bindings_from_raw(default_port, symbols, &raw)
}

fn udp_bindings_from_raw(default_port: u16, symbols: &[String], raw: &str) -> Vec<(String, u16)> {
    let map = parse_symbol_port_map(raw);
    if map.is_empty() {
        if symbols.len() <= 1 {
            return vec![];
        }
        return symbols
            .iter()
            .enumerate()
            .filter_map(|(idx, symbol)| {
                let delta = u16::try_from(idx).ok()?;
                let port = default_port.checked_add(delta)?;
                Some((symbol.clone(), port))
            })
            .collect();
    }
    let mut out = Vec::<(String, u16)>::new();
    for symbol in symbols {
        if let Some(port) = map.get(symbol).copied() {
            out.push((symbol.clone(), port));
        }
    }
    if out.is_empty() && symbols.len() == 1 {
        out.push((symbols[0].clone(), default_port));
    }
    out
}

fn parse_symbol_port_map(raw: &str) -> HashMap<String, u16> {
    let mut out = HashMap::<String, u16>::new();
    for item in raw.split(',') {
        let token = item.trim();
        if token.is_empty() {
            continue;
        }
        let mut parts = token.split(':');
        let symbol = parts.next().unwrap_or_default().trim().to_ascii_uppercase();
        let port = parts.next().unwrap_or_default().trim();
        if symbol.is_empty() || port.is_empty() || parts.next().is_some() {
            continue;
        }
        if let Ok(parsed_port) = port.parse::<u16>() {
            out.insert(symbol, parsed_port);
        }
    }
    out
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

    #[test]
    fn parse_symbol_port_map_accepts_valid_pairs() {
        let map = parse_symbol_port_map("BTCUSDT:6666,ETHUSDT:6667");
        assert_eq!(map.get("BTCUSDT"), Some(&6666));
        assert_eq!(map.get("ETHUSDT"), Some(&6667));
    }

    #[test]
    fn parse_symbol_port_map_ignores_invalid_tokens() {
        let map = parse_symbol_port_map("BTCUSDT:6666,bad,ETHUSDT:notaport, :7777");
        assert_eq!(map.get("BTCUSDT"), Some(&6666));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn udp_bindings_defaults_to_sequential_ports_for_multi_symbol() {
        let symbols = vec![
            "BTCUSDT".to_string(),
            "ETHUSDT".to_string(),
            "SOLUSDT".to_string(),
        ];
        let bindings = udp_bindings_from_raw(6666, &symbols, "");
        assert_eq!(
            bindings,
            vec![
                ("BTCUSDT".to_string(), 6666),
                ("ETHUSDT".to_string(), 6667),
                ("SOLUSDT".to_string(), 6668),
            ]
        );
    }
}
