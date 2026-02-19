use anyhow::{Context, Result};
use async_trait::async_trait;
use core_types::{DynStream, RefPriceFeed, RefTick};
use poly_wire::{
    decode_auto, WirePacket, WIRE_BOOK_TOP24_SIZE, WIRE_MAX_PACKET_SIZE, WIRE_MOMENTUM_TICK32_SIZE,
};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr, UdpSocket as StdUdpSocket};
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub struct UdpBinanceFeed {
    port: u16,
}

static UDP_LOCAL_DROP_COUNT: AtomicU64 = AtomicU64::new(0);

pub fn udp_local_drop_count() -> u64 {
    UDP_LOCAL_DROP_COUNT.load(Ordering::Relaxed)
}

#[derive(Debug, Clone)]
struct UdpLocalPolicy {
    local_only: bool,
    allow_private: bool,
    allowlist: HashSet<IpAddr>,
}

impl UdpLocalPolicy {
    fn from_env() -> Self {
        let local_only = std::env::var("POLYEDGE_UDP_LOCAL_ONLY")
            .ok()
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "True"))
            .unwrap_or(true);
        let allow_private = std::env::var("POLYEDGE_UDP_LOCAL_ALLOW_PRIVATE")
            .ok()
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "True"))
            .unwrap_or(false);
        let allowlist =
            parse_ip_allowlist(&std::env::var("POLYEDGE_UDP_LOCAL_ALLOW").unwrap_or_default());
        Self {
            local_only,
            allow_private,
            allowlist,
        }
    }

    fn allows(&self, addr: &SocketAddr) -> bool {
        if !self.local_only {
            return true;
        }
        let ip = addr.ip();
        if ip.is_loopback() {
            return true;
        }
        if self.allow_private && is_private_ip(&ip) {
            return true;
        }
        self.allowlist.contains(&ip)
    }
}

#[derive(Debug, Clone, Copy)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
struct UdpRecvTuning {
    rcvbuf_bytes: Option<usize>,
    busy_poll_us: Option<u32>,
    user_spin: bool,
    drop_on_full: bool,
}

impl UdpRecvTuning {
    fn from_env() -> Self {
        let rcvbuf_bytes = std::env::var("POLYEDGE_UDP_RCVBUF_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0);
        let busy_poll_us = std::env::var("POLYEDGE_UDP_BUSY_POLL_US")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .filter(|v| *v > 0);
        let user_spin = std::env::var("POLYEDGE_UDP_USER_SPIN")
            .ok()
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "True"))
            .unwrap_or(false);
        let drop_on_full = std::env::var("POLYEDGE_UDP_DROP_ON_FULL")
            .ok()
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "True"))
            .unwrap_or(true);
        Self {
            rcvbuf_bytes,
            busy_poll_us,
            user_spin,
            drop_on_full,
        }
    }
}

impl UdpBinanceFeed {
    pub fn new(port: u16) -> Self {
        Self { port }
    }
}

#[async_trait]
impl RefPriceFeed for UdpBinanceFeed {
    async fn stream_ticks(&self, symbols: Vec<String>) -> Result<DynStream<RefTick>> {
        let rx_queue_cap = std::env::var("POLYEDGE_UDP_RX_QUEUE_CAP")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(4_096)
            .clamp(512, 65_536);
        let (tx, rx) = mpsc::channel::<Result<RefTick>>(rx_queue_cap);
        let normalized_symbols = normalize_symbols(&symbols);
        let bindings = udp_bindings(self.port, &normalized_symbols);
        let tuning = UdpRecvTuning::from_env();
        let core_map =
            parse_port_core_map(&std::env::var("POLYEDGE_UDP_PIN_CORES").unwrap_or_default());

        if bindings.is_empty() {
            let symbol = normalized_symbols
                .first()
                .cloned()
                .unwrap_or_else(|| "BTCUSDT".to_string());
            let addr: SocketAddr = format!("0.0.0.0:{}", self.port)
                .parse()
                .context("parse udp bind addr")?;
            let socket = StdUdpSocket::bind(addr).context("bind udp feed socket")?;
            let core_id = core_map.get(&self.port).copied();
            spawn_recv_loop(socket, symbol, tx.clone(), tuning, core_id)?;
        } else {
            for (symbol, port) in bindings {
                let addr: SocketAddr = format!("0.0.0.0:{port}")
                    .parse()
                    .context("parse udp bind addr")?;
                let socket = StdUdpSocket::bind(addr).with_context(|| {
                    format!("bind udp feed socket for symbol={symbol} port={port}")
                })?;
                let core_id = core_map.get(&port).copied();
                spawn_recv_loop(socket, symbol, tx.clone(), tuning, core_id)?;
            }
        }
        drop(tx);

        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

fn spawn_recv_loop(
    socket: StdUdpSocket,
    symbol: String,
    tx: mpsc::Sender<Result<RefTick>>,
    tuning: UdpRecvTuning,
    core_id: Option<usize>,
) -> Result<()> {
    socket
        .set_nonblocking(tuning.user_spin)
        .context("configure udp nonblocking")?;
    apply_udp_recv_socket_tuning(&socket, tuning)?;

    std::thread::Builder::new()
        .name(format!("udp-recv-{symbol}"))
        .spawn(move || {
            if let Some(core) = core_id {
                if let Err(err) = pin_current_thread(core) {
                    let _ = tx.blocking_send(Err(err));
                }
            }

            let mut buf = [0u8; WIRE_MAX_PACKET_SIZE];
            let mut local_policy = UdpLocalPolicy::from_env();
            let mut local_policy_refresh_at = Instant::now();
            loop {
                let recv = socket.recv_from(&mut buf);
                let (amt, src) = match recv {
                    Ok(v) => v,
                    Err(err)
                        if tuning.user_spin && err.kind() == std::io::ErrorKind::WouldBlock =>
                    {
                        std::hint::spin_loop();
                        continue;
                    }
                    Err(err) => {
                        let send_err = if tuning.drop_on_full {
                            match tx.try_send(Err(err.into())) {
                                Ok(_) | Err(mpsc::error::TrySendError::Full(_)) => false,
                                Err(mpsc::error::TrySendError::Closed(_)) => true,
                            }
                        } else {
                            tx.blocking_send(Err(err.into())).is_err()
                        };
                        if send_err {
                            break;
                        }
                        continue;
                    }
                };
                if local_policy_refresh_at.elapsed() >= Duration::from_secs(1) {
                    local_policy = UdpLocalPolicy::from_env();
                    local_policy_refresh_at = Instant::now();
                }
                if !local_policy.allows(&src) {
                    UDP_LOCAL_DROP_COUNT.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                if amt != WIRE_BOOK_TOP24_SIZE
                    && amt != WIRE_MOMENTUM_TICK32_SIZE
                    && amt != poly_wire::WIRE_RELAY_TICK40_SIZE
                {
                    continue;
                }

                let recv_ns = now_ns();
                let (ts_micros, bid, ask, ts_first_hop_ms) = match decode_auto(&buf[..amt]) {
                    Ok(WirePacket::BookTop24(pkt)) => (pkt.ts_micros, pkt.bid, pkt.ask, None),
                    Ok(WirePacket::MomentumTick32(pkt)) => (pkt.ts_micros, pkt.bid, pkt.ask, None),
                    Ok(WirePacket::RelayTick40(pkt)) => {
                        (pkt.ts_micros, pkt.bid, pkt.ask, Some(pkt.ts_first_hop_ms))
                    }
                    Err(err) => {
                        let send_err = if tuning.drop_on_full {
                            match tx.try_send(Err(err.into())) {
                                Ok(_) | Err(mpsc::error::TrySendError::Full(_)) => false,
                                Err(mpsc::error::TrySendError::Closed(_)) => true,
                            }
                        } else {
                            tx.blocking_send(Err(err.into())).is_err()
                        };
                        if send_err {
                            break;
                        }
                        continue;
                    }
                };

                let event_ms = (ts_micros / 1_000) as i64;
                let mid = (bid + ask) * 0.5;
                if !mid.is_finite() || mid <= 0.0 {
                    continue;
                }

                let tick = RefTick {
                    source: "binance_udp".to_string(),
                    symbol: symbol.clone(),
                    event_ts_ms: event_ms,
                    recv_ts_ms: recv_ns / 1_000_000,
                    source_seq: stable_udp_seq(ts_micros, bid, ask),
                    event_ts_exchange_ms: event_ms,
                    recv_ts_local_ns: recv_ns,
                    ingest_ts_local_ns: now_ns(),
                    ts_first_hop_ms,
                    price: mid,
                };

                let send_err = if tuning.drop_on_full {
                    match tx.try_send(Ok(tick)) {
                        Ok(_) | Err(mpsc::error::TrySendError::Full(_)) => false,
                        Err(mpsc::error::TrySendError::Closed(_)) => true,
                    }
                } else {
                    tx.blocking_send(Ok(tick)).is_err()
                };
                if send_err {
                    break;
                }
            }
        })
        .context("spawn udp receiver thread")?;

    Ok(())
}

#[cfg(target_os = "linux")]
fn apply_udp_recv_socket_tuning(socket: &StdUdpSocket, tuning: UdpRecvTuning) -> Result<()> {
    if let Some(bytes) = tuning.rcvbuf_bytes {
        let value: libc::c_int = bytes.try_into().unwrap_or(libc::c_int::MAX);
        let rc = unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                (&value as *const libc::c_int).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            anyhow::bail!("set SO_RCVBUF={} failed: {}", bytes, err);
        }
    }
    if let Some(us) = tuning.busy_poll_us {
        let value: libc::c_int = us.try_into().unwrap_or(libc::c_int::MAX);
        let rc = unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_BUSY_POLL,
                (&value as *const libc::c_int).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            anyhow::bail!("set SO_BUSY_POLL={} failed: {}", us, err);
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn apply_udp_recv_socket_tuning(_socket: &StdUdpSocket, _tuning: UdpRecvTuning) -> Result<()> {
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

fn parse_port_core_map(raw: &str) -> HashMap<u16, usize> {
    let mut out = HashMap::<u16, usize>::new();
    for item in raw.split(',') {
        let token = item.trim();
        if token.is_empty() {
            continue;
        }
        let mut pair = token.split(':');
        let port = pair.next().unwrap_or_default().trim();
        let core = pair.next().unwrap_or_default().trim();
        if port.is_empty() || core.is_empty() || pair.next().is_some() {
            continue;
        }
        if let (Ok(port_id), Ok(core_id)) = (port.parse::<u16>(), core.parse::<usize>()) {
            out.insert(port_id, core_id);
        }
    }
    out
}

fn parse_ip_allowlist(raw: &str) -> HashSet<IpAddr> {
    let mut out = HashSet::new();
    for token in raw.split(',') {
        let candidate = token.trim();
        if candidate.is_empty() {
            continue;
        }
        if let Ok(ip) = candidate.parse::<IpAddr>() {
            out.insert(ip);
        }
    }
    out
}

fn is_private_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_private() || v4.is_link_local(),
        IpAddr::V6(v6) => v6.is_unique_local() || v6.is_unicast_link_local(),
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

    #[test]
    fn parse_port_core_map_accepts_pairs() {
        let map = parse_port_core_map("6666:2,6667:3");
        assert_eq!(map.get(&6666), Some(&2usize));
        assert_eq!(map.get(&6667), Some(&3usize));
    }

    #[test]
    fn parse_ip_allowlist_accepts_valid_ips() {
        let allowlist = parse_ip_allowlist("127.0.0.1,10.0.0.2,::1,bad");
        assert!(allowlist.contains(&"127.0.0.1".parse::<IpAddr>().unwrap()));
        assert!(allowlist.contains(&"10.0.0.2".parse::<IpAddr>().unwrap()));
        assert!(allowlist.contains(&"::1".parse::<IpAddr>().unwrap()));
        assert_eq!(allowlist.len(), 3);
    }
}
