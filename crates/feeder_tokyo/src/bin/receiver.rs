use anyhow::{Context, Result};
use poly_wire::{
    decode_auto, now_micros, WirePacket, WIRE_BOOK_TOP24_SIZE, WIRE_MAX_PACKET_SIZE,
    WIRE_MOMENTUM_TICK32_SIZE, WIRE_RELAY_TICK40_SIZE,
};
use std::net::UdpSocket;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;

fn main() -> Result<()> {
    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:6666".to_string());
    let print_every = std::env::var("PRINT_EVERY")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1);
    let pin_core = std::env::var("PIN_CORE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok());
    let busy_poll_us = std::env::var("BUSY_POLL_US")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|v| *v > 0);
    let rcvbuf_bytes = std::env::var("RCVBUF_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0);

    if let Some(core_id) = pin_core {
        pin_current_thread(core_id)?;
    }

    let socket = UdpSocket::bind(&bind_addr)
        .with_context(|| format!("bind receiver UDP socket at {bind_addr}"))?;
    apply_udp_socket_tuning(&socket, rcvbuf_bytes, busy_poll_us)?;
    socket
        .set_nonblocking(true)
        .context("set receiver UDP socket nonblocking")?;

    eprintln!(
        "receiver: listening={} packet_sizes=[{},{},{}] print_every={} (busy-spin mode)",
        bind_addr,
        WIRE_BOOK_TOP24_SIZE,
        WIRE_MOMENTUM_TICK32_SIZE,
        WIRE_RELAY_TICK40_SIZE,
        print_every
    );

    let mut buf = [0u8; WIRE_MAX_PACKET_SIZE];
    let mut last_packet_ts: u64 = 0;
    let mut recv_ok: u64 = 0;
    let mut dropped_out_of_order: u64 = 0;
    let mut dropped_size: u64 = 0;

    loop {
        match socket.recv_from(&mut buf) {
            Ok((amt, _src)) => {
                if amt != WIRE_BOOK_TOP24_SIZE
                    && amt != WIRE_MOMENTUM_TICK32_SIZE
                    && amt != WIRE_RELAY_TICK40_SIZE
                {
                    dropped_size = dropped_size.saturating_add(1);
                    continue;
                }

                let (ts_micros, bid, ask) = match decode_auto(&buf[..amt]) {
                    Ok(WirePacket::BookTop24(pkt)) => (pkt.ts_micros, pkt.bid, pkt.ask),
                    Ok(WirePacket::MomentumTick32(pkt)) => (pkt.ts_micros, pkt.bid, pkt.ask),
                    Ok(WirePacket::RelayTick40(pkt)) => (pkt.ts_micros, pkt.bid, pkt.ask),
                    Err(_) => continue,
                };

                if ts_micros < last_packet_ts {
                    dropped_out_of_order = dropped_out_of_order.saturating_add(1);
                    continue;
                }
                last_packet_ts = ts_micros;

                recv_ok = recv_ok.saturating_add(1);
                if recv_ok.is_multiple_of(print_every) {
                    let now = now_micros();
                    let latency_us = now.saturating_sub(ts_micros);
                    println!(
                        "latency_us={} bid={:.8} ask={:.8} recv_ok={} drop_ooo={} drop_size={}",
                        latency_us, bid, ask, recv_ok, dropped_out_of_order, dropped_size
                    );
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                // Busy-wait for lowest wakeup latency.
                std::hint::spin_loop();
            }
            Err(err) => return Err(err).context("udp recv_from failed"),
        }
    }
}

#[cfg(target_os = "linux")]
fn apply_udp_socket_tuning(
    socket: &UdpSocket,
    rcvbuf_bytes: Option<usize>,
    busy_poll_us: Option<u32>,
) -> Result<()> {
    if let Some(bytes) = rcvbuf_bytes {
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
            anyhow::bail!("set SO_RCVBUF failed: {}", std::io::Error::last_os_error());
        }
    }
    if let Some(us) = busy_poll_us {
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
            anyhow::bail!(
                "set SO_BUSY_POLL failed: {}",
                std::io::Error::last_os_error()
            );
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn apply_udp_socket_tuning(
    _socket: &UdpSocket,
    _rcvbuf_bytes: Option<usize>,
    _busy_poll_us: Option<u32>,
) -> Result<()> {
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
            anyhow::bail!(
                "pthread_setaffinity_np(core={core_id}) failed: {}",
                std::io::Error::from_raw_os_error(rc)
            );
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn pin_current_thread(_core_id: usize) -> Result<()> {
    Ok(())
}
