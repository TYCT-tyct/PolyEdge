use bincode::Options;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

/// Tight 24-byte quote packet for pure UDP relay hot path.
/// Layout (Little Endian):
/// [ts_micros: u64][bid: f64][ask: f64]
pub const WIRE_BOOK_TOP24_SIZE: usize = 24;
/// Momentum packet extension.
/// Layout (Little Endian):
/// [ts_micros: u64][bid: f64][ask: f64][velocity_bps_per_sec: f64]
pub const WIRE_MOMENTUM_TICK32_SIZE: usize = 32;
pub const WIRE_MAX_PACKET_SIZE: usize = WIRE_MOMENTUM_TICK32_SIZE;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub struct WireBookTop24 {
    pub ts_micros: u64,
    pub bid: f64,
    pub ask: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub struct WireMomentumTick32 {
    pub ts_micros: u64,
    pub bid: f64,
    pub ask: f64,
    pub velocity_bps_per_sec: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireMode {
    Fixed24,
    Fixed32,
    Auto,
}

impl WireMode {
    pub fn from_env(var_name: &str) -> Self {
        let raw = std::env::var(var_name).unwrap_or_else(|_| "auto".to_string());
        Self::parse(&raw)
    }

    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "24" | "book24" | "top24" | "fixed24" => Self::Fixed24,
            "32" | "momentum32" | "tick32" | "fixed32" => Self::Fixed32,
            "auto" | _ => Self::Auto,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WirePacket {
    BookTop24(WireBookTop24),
    MomentumTick32(WireMomentumTick32),
}

#[derive(Debug)]
pub enum WireDecodeError {
    UnsupportedSize(usize),
    Bincode(bincode::Error),
}

impl std::fmt::Display for WireDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedSize(sz) => write!(f, "unsupported wire packet size: {}", sz),
            Self::Bincode(err) => write!(f, "wire decode error: {}", err),
        }
    }
}

impl std::error::Error for WireDecodeError {}

/// Explicit little-endian + fixed-width encoding for deterministic wire format.
#[inline]
pub fn wire_options() -> impl Options {
    bincode::DefaultOptions::new()
        .with_little_endian()
        .with_fixint_encoding()
}

#[inline]
pub fn encode_book_top24(
    packet: &WireBookTop24,
    out: &mut [u8; WIRE_BOOK_TOP24_SIZE],
) -> Result<(), bincode::Error> {
    let mut cursor = Cursor::new(out.as_mut_slice());
    wire_options().serialize_into(&mut cursor, packet)
}

#[inline]
pub fn decode_book_top24(
    input: &[u8; WIRE_BOOK_TOP24_SIZE],
) -> Result<WireBookTop24, bincode::Error> {
    wire_options().deserialize(input.as_slice())
}

#[inline]
pub fn encode_momentum_tick32(
    packet: &WireMomentumTick32,
    out: &mut [u8; WIRE_MOMENTUM_TICK32_SIZE],
) -> Result<(), bincode::Error> {
    let mut cursor = Cursor::new(out.as_mut_slice());
    wire_options().serialize_into(&mut cursor, packet)
}

#[inline]
pub fn decode_momentum_tick32(
    input: &[u8; WIRE_MOMENTUM_TICK32_SIZE],
) -> Result<WireMomentumTick32, bincode::Error> {
    wire_options().deserialize(input.as_slice())
}

#[inline]
pub fn decode_auto(input: &[u8]) -> Result<WirePacket, WireDecodeError> {
    match input.len() {
        WIRE_BOOK_TOP24_SIZE => {
            let mut buf = [0u8; WIRE_BOOK_TOP24_SIZE];
            buf.copy_from_slice(input);
            decode_book_top24(&buf)
                .map(WirePacket::BookTop24)
                .map_err(WireDecodeError::Bincode)
        }
        WIRE_MOMENTUM_TICK32_SIZE => {
            let mut buf = [0u8; WIRE_MOMENTUM_TICK32_SIZE];
            buf.copy_from_slice(input);
            decode_momentum_tick32(&buf)
                .map(WirePacket::MomentumTick32)
                .map_err(WireDecodeError::Bincode)
        }
        other => Err(WireDecodeError::UnsupportedSize(other)),
    }
}

#[inline]
pub fn encode_with_mode(
    packet24: &WireBookTop24,
    velocity_bps_per_sec: f64,
    mode: WireMode,
    out: &mut [u8; WIRE_MAX_PACKET_SIZE],
) -> Result<usize, bincode::Error> {
    match mode {
        WireMode::Fixed24 => {
            let mut view = [0u8; WIRE_BOOK_TOP24_SIZE];
            encode_book_top24(packet24, &mut view)?;
            out[..WIRE_BOOK_TOP24_SIZE].copy_from_slice(&view);
            Ok(WIRE_BOOK_TOP24_SIZE)
        }
        WireMode::Fixed32 | WireMode::Auto => {
            // Auto prefers the richer packet while receivers stay backward-compatible.
            let mut view = [0u8; WIRE_MOMENTUM_TICK32_SIZE];
            let packet32 = WireMomentumTick32 {
                ts_micros: packet24.ts_micros,
                bid: packet24.bid,
                ask: packet24.ask,
                velocity_bps_per_sec,
            };
            encode_momentum_tick32(&packet32, &mut view)?;
            out[..WIRE_MOMENTUM_TICK32_SIZE].copy_from_slice(&view);
            Ok(WIRE_MOMENTUM_TICK32_SIZE)
        }
    }
}

/// Helper to get current micros
pub fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_24_size_is_stable() {
        let sample = WireBookTop24 {
            ts_micros: 1,
            bid: 100.25,
            ask: 100.75,
        };
        let bytes = wire_options()
            .serialize(&sample)
            .expect("serialize WireBookTop24");
        assert_eq!(bytes.len(), WIRE_BOOK_TOP24_SIZE);
    }

    #[test]
    fn wire_24_roundtrip() {
        let sample = WireBookTop24 {
            ts_micros: 42,
            bid: 61321.5,
            ask: 61322.0,
        };
        let mut out = [0u8; WIRE_BOOK_TOP24_SIZE];
        encode_book_top24(&sample, &mut out).expect("encode");
        let decoded = decode_book_top24(&out).expect("decode");
        assert_eq!(decoded, sample);
    }

    #[test]
    fn wire_32_roundtrip() {
        let sample = WireMomentumTick32 {
            ts_micros: 42,
            bid: 61321.5,
            ask: 61322.0,
            velocity_bps_per_sec: 7.5,
        };
        let mut out = [0u8; WIRE_MOMENTUM_TICK32_SIZE];
        encode_momentum_tick32(&sample, &mut out).expect("encode 32");
        let decoded = decode_momentum_tick32(&out).expect("decode 32");
        assert_eq!(decoded, sample);
    }

    #[test]
    fn wire_decode_auto_handles_24_and_32() {
        let book24 = WireBookTop24 {
            ts_micros: 10,
            bid: 100.1,
            ask: 100.2,
        };
        let mut raw24 = [0u8; WIRE_BOOK_TOP24_SIZE];
        encode_book_top24(&book24, &mut raw24).expect("encode 24");
        match decode_auto(&raw24).expect("decode auto 24") {
            WirePacket::BookTop24(v) => assert_eq!(v, book24),
            _ => panic!("expected WirePacket::BookTop24"),
        }

        let mut raw32 = [0u8; WIRE_MOMENTUM_TICK32_SIZE];
        let n = encode_with_mode(&book24, 3.2, WireMode::Fixed32, &mut raw32).expect("encode mode");
        assert_eq!(n, WIRE_MOMENTUM_TICK32_SIZE);
        match decode_auto(&raw32).expect("decode auto 32") {
            WirePacket::MomentumTick32(v) => {
                assert_eq!(v.ts_micros, book24.ts_micros);
                assert_eq!(v.bid, book24.bid);
                assert_eq!(v.ask, book24.ask);
                assert_eq!(v.velocity_bps_per_sec, 3.2);
            }
            _ => panic!("expected WirePacket::MomentumTick32"),
        }
    }

    #[test]
    fn wire_mode_parse_default_auto() {
        assert_eq!(WireMode::parse("24"), WireMode::Fixed24);
        assert_eq!(WireMode::parse("32"), WireMode::Fixed32);
        assert_eq!(WireMode::parse("auto"), WireMode::Auto);
        assert_eq!(WireMode::parse("unknown"), WireMode::Auto);
    }
}
