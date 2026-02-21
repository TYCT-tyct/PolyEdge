use anyhow::{Context, Result};
use core_types::{Direction, RefTick};
use direction_detector::{DirectionConfig, DirectionDetector};
use futures::StreamExt;
use poly_wire::now_micros;
use std::net::{SocketAddr, UdpSocket};
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

const KEY_BID: &[u8] = br#""b":""#;
const KEY_ASK: &[u8] = br#""a":""#;
const KEY_EVENT_MS: &[u8] = br#""E":"#;

// The Blind Box Trigger format:
// [0] Magic: 0xBB
// [1] Action: 0x01 (Up), 0x02 (Down)
// [2] Symbol Length: u8
// [3..] Symbol Bytes

#[derive(Debug, Clone)]
struct Route {
    symbol: String,
    target: String,
    core_id: Option<usize>,
}

fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut routes = resolve_routes();
    if routes.is_empty() {
        anyhow::bail!("no brain routes configured");
    }
    assign_route_cores(&mut routes);
    eprintln!("brain: starting with routes={routes:?}");

    let mut handles = Vec::with_capacity(routes.len());
    for route in routes {
        let name = format!("brain-{}", route.symbol);
        let handle = std::thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                if let Some(core_id) = route.core_id {
                    let _ = pin_current_thread(core_id);
                }

                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                rt.block_on(async move {
                    loop {
                        if let Err(err) = run_brain(&route).await {
                            eprintln!("brain: route symbol={} failed: {}", route.symbol, err);
                        }
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                    }
                });
            })
            .with_context(|| format!("spawn thread {name}"))?;
        handles.push(handle);
    }

    for h in handles {
        let _ = h.join();
    }
    Ok(())
}

fn resolve_routes() -> Vec<Route> {
    let raw = std::env::var("SYMBOL_TARGETS").unwrap_or_default();
    let mut out = Vec::new();
    for token in raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        let mut pair = token.split('=');
        let symbol = pair.next().unwrap_or_default().trim().to_ascii_lowercase();
        let target = pair.next().unwrap_or_default().trim().to_string();
        if symbol.is_empty() || target.is_empty() {
            continue;
        }
        out.push(Route {
            symbol,
            target,
            core_id: None,
        });
    }
    if out.is_empty() {
        let symbol = std::env::var("SYMBOL").unwrap_or_else(|_| "btcusdt".to_string());
        let target = std::env::var("TARGET").unwrap_or_else(|_| "10.0.3.123:6666".to_string());
        out.push(Route {
            symbol: symbol.to_ascii_lowercase(),
            target,
            core_id: None,
        });
    }
    out
}

fn assign_route_cores(routes: &mut [Route]) {
    let fallback = std::env::var("POLYEDGE_SENDER_PIN_CORE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok());
    for route in routes {
        route.core_id = fallback;
    }
}

async fn run_brain(route: &Route) -> Result<()> {
    let target_addr: SocketAddr = route.target.parse()?;

    // Create an emphemeral UDP socket perfectly tuned to blast signals
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    apply_udp_sender_socket_tuning(&socket)?;
    socket.set_nonblocking(true)?;

    eprintln!(
        "brain: active target={} symbol={}",
        route.target, route.symbol
    );

    // Turn off source vote gate because the brain only sees Binance locally!
    let mut cfg = DirectionConfig::default();
    cfg.min_ticks_for_signal = 3;
    cfg.enable_source_vote_gate = false;
    cfg.require_secondary_confirmation = false;

    let mut detector = DirectionDetector::new(cfg);
    let mut last_trigger_time = Instant::now() - Duration::from_secs(60);

    let redundancy = 5; // Blast 5 times to bypass packet drops

    loop {
        let endpoint_candidates =
            pick_best_fstream_ws_endpoint(fstream_ws_endpoints(&route.symbol)).await;
        let mut ws_stream = None;
        for endpoint in endpoint_candidates {
            if let Ok(Ok((ws, _))) = timeout(Duration::from_secs(3), connect_async(&endpoint)).await
            {
                ws_stream = Some(ws);
                break;
            }
        }
        let Some(mut ws_stream) = ws_stream else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };

        while let Some(frame) = ws_stream.next().await {
            let Ok(Message::Text(text)) = frame else {
                continue;
            };

            let bytes = text.as_bytes();
            let bid = extract_quoted_f64(bytes, KEY_BID);
            let ask = extract_quoted_f64(bytes, KEY_ASK);
            let event_ms = extract_i64(bytes, KEY_EVENT_MS);

            if let (Some(b), Some(a), Some(ems)) = (bid, ask, event_ms) {
                let mid = (b + a) * 0.5;
                let now_ns = now_micros() * 1000;
                let ref_tick = RefTick {
                    source: "binance".into(),
                    symbol: route.symbol.clone(),
                    event_ts_ms: ems,
                    recv_ts_ms: ems,
                    source_seq: 0,
                    event_ts_exchange_ms: ems,
                    recv_ts_local_ns: now_ns as i64,
                    ingest_ts_local_ns: now_ns as i64,
                    ts_first_hop_ms: None,
                    price: mid,
                };

                detector.on_tick(&ref_tick);

                if let Some(signal) = detector.evaluate(&route.symbol, ems) {
                    if signal.direction != Direction::Neutral
                        && last_trigger_time.elapsed() >= Duration::from_millis(500)
                    {
                        last_trigger_time = Instant::now();

                        let action_byte = match signal.direction {
                            Direction::Up => 0x01,
                            Direction::Down => 0x02,
                            _ => 0x00,
                        };

                        // Construct Blind-Box Payload
                        let mut payload = Vec::with_capacity(2 + 1 + route.symbol.len());
                        payload.push(0xBB); // Magic byte
                        payload.push(action_byte);
                        payload.push(route.symbol.len() as u8);
                        payload.extend_from_slice(route.symbol.as_bytes());

                        for _ in 0..redundancy {
                            let _ = socket.send_to(&payload, target_addr);
                        }

                        eprintln!(
                            "brain: [BLIND-BOX TRIGGER FIRED] symbol={} action={:?} payload_len={}",
                            route.symbol,
                            signal.direction,
                            payload.len()
                        );
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

async fn pick_best_fstream_ws_endpoint(endpoints: Vec<String>) -> Vec<String> {
    if endpoints.len() <= 1 {
        return endpoints;
    }
    let mut join_set = tokio::task::JoinSet::new();
    for ep in endpoints.iter().cloned() {
        join_set.spawn(async move {
            let started = Instant::now();
            let ok = timeout(Duration::from_secs(2), connect_async(&ep)).await;
            if let Ok(Ok((ws, _))) = ok {
                drop(ws);
                Some((ep, started.elapsed().as_secs_f64() * 1000.0))
            } else {
                None
            }
        });
    }
    let mut results = Vec::new();
    while let Some(res) = join_set.join_next().await {
        if let Ok(Some(v)) = res {
            results.push(v);
        }
    }
    if results.is_empty() {
        return endpoints;
    }
    results.sort_by(|a, b| a.1.total_cmp(&b.1));
    let best = results[0].0.clone();
    let mut out = vec![best.clone()];
    for ep in endpoints {
        if ep != best {
            out.push(ep);
        }
    }
    out
}

fn fstream_ws_endpoints(symbol: &str) -> Vec<String> {
    vec![
        format!("wss://fstream.binance.com/ws/{}@bookTicker", symbol),
        format!("wss://fstream1.binance.com/ws/{}@bookTicker", symbol),
        format!("wss://fstream2.binance.com/ws/{}@bookTicker", symbol),
    ]
}

#[cfg(target_os = "linux")]
fn apply_udp_sender_socket_tuning(socket: &UdpSocket) -> Result<()> {
    let value: libc::c_int = 1048576;
    unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            (&value as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn apply_udp_sender_socket_tuning(_socket: &UdpSocket) -> Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn pin_current_thread(core_id: usize) -> Result<()> {
    let mut cpuset: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    unsafe {
        libc::CPU_ZERO(&mut cpuset);
        libc::CPU_SET(core_id, &mut cpuset);
        libc::pthread_setaffinity_np(
            libc::pthread_self(),
            std::mem::size_of::<libc::cpu_set_t>(),
            &cpuset,
        );
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn pin_current_thread(_core_id: usize) -> Result<()> {
    Ok(())
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
fn extract_i64(payload: &[u8], key: &[u8]) -> Option<i64> {
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
