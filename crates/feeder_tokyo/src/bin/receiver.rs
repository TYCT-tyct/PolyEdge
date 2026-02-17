use anyhow::{Context, Result};
use poly_wire::{decode_book_top24, now_micros, WIRE_BOOK_TOP24_SIZE};
use std::net::UdpSocket;

fn main() -> Result<()> {
    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:6666".to_string());
    let print_every = std::env::var("PRINT_EVERY")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1);

    let socket = UdpSocket::bind(&bind_addr)
        .with_context(|| format!("bind receiver UDP socket at {bind_addr}"))?;
    socket
        .set_nonblocking(true)
        .context("set receiver UDP socket nonblocking")?;

    eprintln!(
        "receiver: listening={} packet_size={} print_every={} (busy-spin mode)",
        bind_addr, WIRE_BOOK_TOP24_SIZE, print_every
    );

    let mut buf = [0u8; WIRE_BOOK_TOP24_SIZE];
    let mut last_packet_ts: u64 = 0;
    let mut recv_ok: u64 = 0;
    let mut dropped_out_of_order: u64 = 0;
    let mut dropped_size: u64 = 0;

    loop {
        match socket.recv_from(&mut buf) {
            Ok((amt, _src)) => {
                if amt != WIRE_BOOK_TOP24_SIZE {
                    dropped_size = dropped_size.saturating_add(1);
                    continue;
                }

                let packet = match decode_book_top24(&buf) {
                    Ok(pkt) => pkt,
                    Err(_) => continue,
                };

                if packet.ts_micros < last_packet_ts {
                    dropped_out_of_order = dropped_out_of_order.saturating_add(1);
                    continue;
                }
                last_packet_ts = packet.ts_micros;

                recv_ok = recv_ok.saturating_add(1);
                if recv_ok % print_every == 0 {
                    let now = now_micros();
                    let latency_us = now.saturating_sub(packet.ts_micros);
                    println!(
                        "latency_us={} bid={:.8} ask={:.8} recv_ok={} drop_ooo={} drop_size={}",
                        latency_us,
                        packet.bid,
                        packet.ask,
                        recv_ok,
                        dropped_out_of_order,
                        dropped_size
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
