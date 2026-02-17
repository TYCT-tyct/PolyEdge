use anyhow::{Context, Result};
use futures::StreamExt;
use poly_wire::{encode_book_top24, now_micros, WireBookTop24, WIRE_BOOK_TOP24_SIZE};
use std::net::UdpSocket;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

const KEY_BID: &[u8] = br#""b":""#;
const KEY_ASK: &[u8] = br#""a":""#;
const KEY_EVENT_MS: &[u8] = br#""E":"#;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let symbol = std::env::var("SYMBOL").unwrap_or_else(|_| "btcusdt".to_string());
    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:9999".to_string());
    let target = std::env::var("TARGET").unwrap_or_else(|_| "10.0.3.123:6666".to_string());

    let socket = UdpSocket::bind(&bind_addr)
        .with_context(|| format!("bind sender UDP socket at {bind_addr}"))?;
    socket
        .connect(&target)
        .with_context(|| format!("connect sender UDP socket to {target}"))?;
    socket
        .set_nonblocking(true)
        .context("set sender UDP socket nonblocking")?;

    eprintln!("sender: bind={bind_addr} target={target} symbol={symbol}");

    let mut packet_buf = [0u8; WIRE_BOOK_TOP24_SIZE];
    let mut packets: u64 = 0;
    let mut dropped_would_block: u64 = 0;
    let mut last_log = std::time::Instant::now();

    loop {
        let url = format!("wss://fstream.binance.com/ws/{}@bookTicker", symbol);
        let (mut ws_stream, _) = match connect_async(&url).await {
            Ok(v) => v,
            Err(err) => {
                eprintln!("sender: websocket connect error: {err}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };

        while let Some(frame) = ws_stream.next().await {
            let Ok(message) = frame else {
                break;
            };
            let Message::Text(text) = message else {
                continue;
            };

            if let Some(packet) = parse_book_ticker(&text) {
                encode_book_top24(&packet, &mut packet_buf).context("encode 24-byte packet")?;

                match socket.send(&packet_buf) {
                    Ok(_) => {
                        packets = packets.saturating_add(1);
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        dropped_would_block = dropped_would_block.saturating_add(1);
                    }
                    Err(err) => return Err(err).context("udp send failed"),
                }
            }

            if last_log.elapsed().as_secs() >= 5 {
                eprintln!(
                    "sender: packets={} dropped_would_block={}",
                    packets, dropped_would_block
                );
                last_log = std::time::Instant::now();
            }
        }

        eprintln!("sender: websocket disconnected, reconnecting...");
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }
}

#[inline]
fn parse_book_ticker(payload: &str) -> Option<WireBookTop24> {
    let bytes = payload.as_bytes();
    let bid = extract_quoted_f64(bytes, KEY_BID)?;
    let ask = extract_quoted_f64(bytes, KEY_ASK)?;
    let event_ms = extract_u64(bytes, KEY_EVENT_MS)?;

    let ts_micros = if event_ms > 0 {
        event_ms.saturating_mul(1_000)
    } else {
        now_micros()
    };

    Some(WireBookTop24 {
        ts_micros,
        bid,
        ask,
    })
}

#[inline]
fn extract_quoted_f64(payload: &[u8], key_with_quote: &[u8]) -> Option<f64> {
    let start = find_subslice(payload, key_with_quote)? + key_with_quote.len();
    let end_rel = payload.get(start..)?.iter().position(|&b| b == b'"')?;
    let end = start + end_rel;
    std::str::from_utf8(payload.get(start..end)?)
        .ok()?
        .parse()
        .ok()
}

#[inline]
fn extract_u64(payload: &[u8], key: &[u8]) -> Option<u64> {
    let start = find_subslice(payload, key)? + key.len();
    let tail = payload.get(start..)?;
    let mut end_rel = 0usize;
    while end_rel < tail.len() && tail[end_rel].is_ascii_digit() {
        end_rel += 1;
    }
    if end_rel == 0 {
        return None;
    }
    std::str::from_utf8(&tail[..end_rel]).ok()?.parse().ok()
}

#[inline]
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
