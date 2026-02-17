use anyhow::{Context, Result};
use futures::StreamExt;
use poly_wire::{encode_book_top24, now_micros, WireBookTop24, WIRE_BOOK_TOP24_SIZE};
use std::collections::HashMap;
use std::net::{SocketAddr, UdpSocket};
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

const KEY_BID: &[u8] = br#""b":""#;
const KEY_ASK: &[u8] = br#""a":""#;
const KEY_EVENT_MS: &[u8] = br#""E":"#;

#[derive(Debug, Clone)]
struct Route {
    symbol: String,
    bind_addr: String,
    target: String,
    core_id: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
struct SenderTuning {
    redundancy: u8,
    sndbuf_bytes: Option<usize>,
}

fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut routes = resolve_routes();
    if routes.is_empty() {
        anyhow::bail!("no sender routes configured");
    }
    assign_route_cores(&mut routes);
    let tuning = load_sender_tuning();
    eprintln!("sender: routes={routes:?} tuning={tuning:?}");

    let mut handles = Vec::with_capacity(routes.len());
    for route in routes {
        let tuning_local = tuning;
        let name = format!("sender-{}", route.symbol);
        let handle = std::thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                if let Some(core_id) = route.core_id {
                    if let Err(err) = pin_current_thread(core_id) {
                        eprintln!(
                            "sender: symbol={} failed to pin thread to core {}: {}",
                            route.symbol, core_id, err
                        );
                    } else {
                        eprintln!("sender: symbol={} pinned to core {}", route.symbol, core_id);
                    }
                }

                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(v) => v,
                    Err(err) => {
                        eprintln!("sender: symbol={} runtime build failed: {}", route.symbol, err);
                        return;
                    }
                };

                rt.block_on(async move {
                    loop {
                        if let Err(err) = run_route(&route, tuning_local).await {
                            eprintln!(
                                "sender: route symbol={} target={} failed: {}",
                                route.symbol, route.target, err
                            );
                        }
                        tokio::time::sleep(Duration::from_millis(300)).await;
                    }
                });
            })
            .with_context(|| format!("spawn thread {name}"))?;
        handles.push(handle);
    }

    for h in handles {
        if let Err(err) = h.join() {
            eprintln!("sender: worker thread panic: {:?}", err);
        }
    }
    Ok(())
}

fn load_sender_tuning() -> SenderTuning {
    let redundancy = std::env::var("POLYEDGE_UDP_REDUNDANCY")
        .ok()
        .and_then(|v| v.parse::<u8>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1)
        .min(8);
    let sndbuf_bytes = std::env::var("POLYEDGE_UDP_SNDBUF_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0);
    SenderTuning {
        redundancy,
        sndbuf_bytes,
    }
}

fn resolve_routes() -> Vec<Route> {
    let from_targets = parse_symbol_targets(
        &std::env::var("SYMBOL_TARGETS").unwrap_or_default(),
        &std::env::var("BIND_BASE_PORT").unwrap_or_else(|_| "9999".to_string()),
    );
    if !from_targets.is_empty() {
        return from_targets
            .into_iter()
            .map(|(symbol, bind_addr, target)| Route {
                symbol,
                bind_addr,
                target,
                core_id: None,
            })
            .collect();
    }
    let symbol = std::env::var("SYMBOL").unwrap_or_else(|_| "btcusdt".to_string());
    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:9999".to_string());
    let target = std::env::var("TARGET").unwrap_or_else(|_| "10.0.3.123:6666".to_string());
    vec![Route {
        symbol: symbol.trim().to_ascii_lowercase(),
        bind_addr,
        target,
        core_id: None,
    }]
}

fn assign_route_cores(routes: &mut [Route]) {
    let per_symbol = parse_symbol_core_map(&std::env::var("POLYEDGE_SENDER_PIN_CORES").unwrap_or_default());
    let fallback = std::env::var("POLYEDGE_SENDER_PIN_CORE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok());
    for route in routes {
        route.core_id = per_symbol.get(route.symbol.as_str()).copied().or(fallback);
    }
}

fn parse_symbol_targets(raw: &str, bind_base_port_raw: &str) -> Vec<(String, String, String)> {
    let bind_base_port = bind_base_port_raw.parse::<u16>().unwrap_or(9999);
    let mut routes = Vec::<(String, String, String)>::new();
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
        routes.push((symbol, bind_addr, target));
    }
    routes
}

fn parse_symbol_core_map(raw: &str) -> HashMap<String, usize> {
    let mut out = HashMap::<String, usize>::new();
    for item in raw.split(',') {
        let token = item.trim();
        if token.is_empty() {
            continue;
        }
        let mut pair = token.split(':');
        let symbol = pair.next().unwrap_or_default().trim().to_ascii_lowercase();
        let core = pair.next().unwrap_or_default().trim();
        if symbol.is_empty() || core.is_empty() || pair.next().is_some() {
            continue;
        }
        if let Ok(core_id) = core.parse::<usize>() {
            out.insert(symbol, core_id);
        }
    }
    out
}

async fn run_route(route: &Route, tuning: SenderTuning) -> Result<()> {
    let target_addr: SocketAddr = route
        .target
        .parse()
        .with_context(|| format!("parse sender target {}", route.target))?;
    let socket = UdpSocket::bind(&route.bind_addr)
        .with_context(|| format!("bind sender UDP socket at {}", route.bind_addr))?;
    apply_udp_sender_socket_tuning(&socket, tuning.sndbuf_bytes)?;
    socket
        .set_nonblocking(true)
        .context("set sender UDP socket nonblocking")?;

    eprintln!(
        "sender: bind={} target={} symbol={} redundancy={} sndbuf={:?}",
        route.bind_addr, route.target, route.symbol, tuning.redundancy, tuning.sndbuf_bytes
    );

    let mut packet_buf = [0u8; WIRE_BOOK_TOP24_SIZE];
    let mut frames: u64 = 0;
    let mut packets_ok: u64 = 0;
    let mut dropped_would_block: u64 = 0;
    let mut dropped_conn_refused: u64 = 0;
    let mut dropped_other: u64 = 0;
    let mut last_log = std::time::Instant::now();

    loop {
        let url = format!("wss://fstream.binance.com/ws/{}@bookTicker", route.symbol);
        let (mut ws_stream, _) = match connect_async(&url).await {
            Ok(v) => v,
            Err(err) => {
                eprintln!("sender: websocket connect error: {err}");
                tokio::time::sleep(Duration::from_secs(1)).await;
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
                frames = frames.saturating_add(1);

                for _ in 0..tuning.redundancy {
                    match socket.send_to(&packet_buf, target_addr) {
                        Ok(_) => packets_ok = packets_ok.saturating_add(1),
                        Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                            dropped_would_block = dropped_would_block.saturating_add(1);
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::ConnectionRefused => {
                            dropped_conn_refused = dropped_conn_refused.saturating_add(1);
                        }
                        Err(_) => dropped_other = dropped_other.saturating_add(1),
                    }
                }
            }

            if last_log.elapsed().as_secs() >= 5 {
                eprintln!(
                    "sender: symbol={} frames={} packets_ok={} dropped_would_block={} dropped_conn_refused={} dropped_other={}",
                    route.symbol,
                    frames,
                    packets_ok,
                    dropped_would_block,
                    dropped_conn_refused,
                    dropped_other
                );
                last_log = std::time::Instant::now();
            }
        }

        eprintln!(
            "sender: websocket disconnected for symbol={}, reconnecting...",
            route.symbol
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

#[cfg(target_os = "linux")]
fn apply_udp_sender_socket_tuning(socket: &UdpSocket, sndbuf_bytes: Option<usize>) -> Result<()> {
    if let Some(bytes) = sndbuf_bytes {
        let value: libc::c_int = bytes.try_into().unwrap_or(libc::c_int::MAX);
        let rc = unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                (&value as *const libc::c_int).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            anyhow::bail!("set SO_SNDBUF={} failed: {}", bytes, err);
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn apply_udp_sender_socket_tuning(_socket: &UdpSocket, _sndbuf_bytes: Option<usize>) -> Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn pin_current_thread(core_id: usize) -> Result<()> {
    let mut cpuset: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    unsafe {
        libc::CPU_ZERO(&mut cpuset);
        libc::CPU_SET(core_id, &mut cpuset);
        let rc = libc::pthread_setaffinity_np(
            libc::pthread_self(),
            std::mem::size_of::<libc::cpu_set_t>(),
            &cpuset,
        );
        if rc != 0 {
            let err = std::io::Error::from_raw_os_error(rc);
            anyhow::bail!("pthread_setaffinity_np(core={core_id}) failed: {err}");
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn pin_current_thread(_core_id: usize) -> Result<()> {
    Ok(())
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
        assert_eq!(routes[0].0, "btcusdt");
        assert_eq!(routes[0].1, "0.0.0.0:9999");
        assert_eq!(routes[1].0, "ethusdt");
        assert_eq!(routes[1].1, "0.0.0.0:10000");
    }

    #[test]
    fn parse_symbol_targets_skips_invalid_tokens() {
        let routes = parse_symbol_targets("bad,btcusdt=10.0.3.123:6666, =x", "9999");
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].0, "btcusdt");
    }

    #[test]
    fn parse_symbol_core_map_accepts_valid_entries() {
        let map = parse_symbol_core_map("btcusdt:2,ethusdt:3");
        assert_eq!(map.get("btcusdt"), Some(&2usize));
        assert_eq!(map.get("ethusdt"), Some(&3usize));
    }
}
