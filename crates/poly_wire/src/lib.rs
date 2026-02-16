use serde::{Deserialize, Serialize};

/// The binary wire format for ultra-low latency UDP transport.
/// Optimized for "bookTicker" (Best Bid / Best Ask).
/// Size: 8 (bid) + 8 (ask) + 8 (ts) = 24 bytes (Payload).
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct WireBookTop {
    pub bid: f64,
    pub ask: f64,
    pub ts: u64,
    pub id: u64, // Microseconds since epoch
}

/// Helper to get current micros
pub fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}
