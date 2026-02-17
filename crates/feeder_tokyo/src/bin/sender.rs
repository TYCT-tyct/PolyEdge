use anyhow::{Context, Result};
use futures::StreamExt;
use poly_wire::{encode_book_top24, now_micros, WireBookTop24, WIRE_BOOK_TOP24_SIZE};
use std::net::UdpSocket;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

const KEY_BID: &[u8] = br#""b":""#;
const KEY_ASK: &[u8] = br#""a":""#;
const KEY_EVENT_MS: &[u8] = br#""E":"#;

#[derive(Debug, Clone)]
struct Route {
    symbol: String,
    bind_addr: String,
    target: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let routes = resolve_routes();
    if routes.is_empty() {
        return Err(anyhow::anyhow!("no sender routes configured"));
    }
    eprintln!("sender: routes={routes:?}");

    for route in routes {
        tokio::spawn(async move {
            loop {
                if let Err(err) = run_route(&route).await {
                    eprintln!(
                        "sender: route symbol={} target={} failed: {err}",
                        route.symbol, route.target
                    );
                }
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            }
        });
    }
    futures::future::pending::<()>().await;
    Ok(())
}

fn resolve_routes() -> Vec<Route> {
    let from_targets = parse_symbol_targets(
        &std::env::var("SYMBOL_TARGETS").unwrap_or_default(),
        &std::env::var("BIND_BASE_PORT").unwrap_or_else(|_| "9999".to_string()),
    );
    if !from_targets.is_empty() {
        return from_targets;
    }
    let symbol = std::env::var("SYMBOL").unwrap_or_else(|_| "btcusdt".to_string());
    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:9999".to_string());
    let target = std::env::var("TARGET").unwrap_or_else(|_| "10.0.3.123:6666".to_string());
    vec![Route {
        symbol: symbol.trim().to_ascii_lowercase(),
        bind_addr,
        target,
    }]
}

fn parse_symbol_targets(raw: &str, bind_base_port_raw: &str) -> Vec<Route> {
    let bind_base_port = bind_base_port_raw.parse::<u16>().unwrap_or(9999);
    let mut routes = Vec::<Route>::new();
    for (idx, token) in raw.split(',').enumerate() {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        let mut pair = token.split('=');
        let symbol = pair.next().unwrap_or_default().trim().to_ascii_lowercase();
        let target = pair.next().unwrap_or_default().trim().to_string();
        if symbol.is_empty() || target.is_empty() || pair.next().is_some() {
            continue;
        }
        let bind_addr = format!("0.0.0.0:{}", bind_base_port.saturating_add(idx as u16));
        routes.push(Route {
            symbol,
            bind_addr,
            target,
        });
    }
    routes
}

async fn run_route(route: &Route) -> Result<()> {
    let socket = UdpSocket::bind(&route.bind_addr)
        .with_context(|| format!("bind sender UDP socket at {}", route.bind_addr))?;
    socket
        .connect(&route.target)
        .with_context(|| format!("connect sender UDP socket to {}", route.target))?;
    socket
        .set_nonblocking(true)
        .context("set sender UDP socket nonblocking")?;

    eprintln!(
        "sender: bind={} target={} symbol={}",
        route.bind_addr, route.target, route.symbol
    );

    let mut packet_buf = [0u8; WIRE_BOOK_TOP24_SIZE];
    let mut packets: u64 = 0;
    let mut dropped_would_block: u64 = 0;
    let mut dropped_conn_refused: u64 = 0;
    let mut last_log = std::time::Instant::now();

    loop {
        let url = format!("wss://fstream.binance.com/ws/{}@bookTicker", route.symbol);
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
                    Err(err) if err.kind() == std::io::ErrorKind::ConnectionRefused => {
                        dropped_conn_refused = dropped_conn_refused.saturating_add(1);
                    }
                    Err(err) => {
                        eprintln!("sender: udp send error for {}: {err}", route.symbol);
                    }
                }
            }

            if last_log.elapsed().as_secs() >= 5 {
                eprintln!(
                    "sender: symbol={} packets={} dropped_would_block={} dropped_conn_refused={}",
                    route.symbol, packets, dropped_would_block, dropped_conn_refused
                );
                last_log = std::time::Instant::now();
            }
        }

        eprintln!(
            "sender: websocket disconnected for symbol={}, reconnecting...",
            route.symbol
        );
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_symbol_targets_accepts_valid_tokens() {
        let routes = parse_symbol_targets("btcusdt=10.0.3.123:6666,ethusdt=10.0.3.123:6667", "9999");
        assert_eq!(routes.len(), 2);
        assert_eq!(routes[0].symbol, "btcusdt");
        assert_eq!(routes[0].bind_addr, "0.0.0.0:9999");
        assert_eq!(routes[1].symbol, "ethusdt");
        assert_eq!(routes[1].bind_addr, "0.0.0.0:10000");
    }

    #[test]
    fn parse_symbol_targets_skips_invalid_tokens() {
        let routes = parse_symbol_targets("bad,btcusdt=10.0.3.123:6666, =x", "9999");
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].symbol, "btcusdt");
    }
}
