use bincode::Options;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

/// Tight 24-byte quote packet for pure UDP relay hot path.
/// Layout (Little Endian):
/// [ts_micros: u64][bid: f64][ask: f64]
pub const WIRE_BOOK_TOP24_SIZE: usize = 24;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub struct WireBookTop24 {
    pub ts_micros: u64,
    pub bid: f64,
    pub ask: f64,
}

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
}
