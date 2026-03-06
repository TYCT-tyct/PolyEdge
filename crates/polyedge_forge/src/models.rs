use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokyoBinanceWire {
    #[serde(default = "tokyo_packet_type_tick")]
    pub packet_type: String,
    #[serde(default)]
    pub relay_seq: u64,
    #[serde(default)]
    pub source_seq: u64,
    #[serde(default)]
    pub source: String,
    pub ts_tokyo_recv_ms: i64,
    pub ts_tokyo_send_ms: i64,
    pub ts_exchange_ms: i64,
    pub symbol: String,
    pub binance_price: f64,
    #[serde(default)]
    pub checksum: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TokyoBinanceLocal {
    pub relay_seq: u64,
    pub source_seq: u64,
    pub source: String,
    pub ts_tokyo_recv_ms: i64,
    pub ts_tokyo_send_ms: i64,
    pub ts_exchange_ms: i64,
    pub ts_ireland_recv_ms: i64,
    pub binance_price: f64,
    pub checksum_ok: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokyoReplayRequestWire {
    #[serde(default = "tokyo_packet_type_replay_request")]
    pub packet_type: String,
    pub symbol: String,
    pub from_relay_seq: u64,
    pub to_relay_seq: u64,
    pub requested_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayTickRow {
    pub ts_ireland_recv_ms: i64,
    pub symbol: String,
    pub source: String,
    pub relay_seq: u64,
    pub source_seq: u64,
    pub ts_tokyo_recv_ms: i64,
    pub ts_tokyo_send_ms: i64,
    pub ts_exchange_ms: i64,
    pub binance_price: f64,
    pub checksum: String,
    pub checksum_ok: bool,
}

#[derive(Debug, Clone)]
pub struct ChainlinkLocal {
    pub symbol: String,
    pub ts_exchange_ms: i64,
    pub ts_ireland_recv_ms: i64,
    pub price: f64,
}

#[derive(Debug, Clone)]
pub struct MarketMeta {
    pub market_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub title: String,
    pub target_price: Option<f64>,
    pub end_ts_ms: i64,
    pub start_ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRow {
    pub schema_version: &'static str,
    pub ingest_seq: u64,
    pub ts_ireland_sample_ms: i64,

    pub ts_tokyo_recv_ms: Option<i64>,
    pub ts_tokyo_send_ms: Option<i64>,
    pub ts_exchange_ms: Option<i64>,
    pub ts_ireland_recv_ms: Option<i64>,
    pub tokyo_relay_proc_lag_ms: Option<f64>,
    pub cross_region_net_lag_ms: Option<f64>,
    pub path_lag_ms: Option<f64>,

    pub symbol: String,

    pub ts_pm_recv_ms: i64,
    pub market_id: String,
    pub timeframe: String,
    pub title: String,

    pub target_price: Option<f64>,

    pub mid_yes: f64,
    pub mid_no: f64,
    pub mid_yes_smooth: f64,
    pub mid_no_smooth: f64,
    pub bid_yes: f64,
    pub ask_yes: f64,
    pub bid_no: f64,
    pub ask_no: f64,
    pub bid_size_yes: f64,
    pub ask_size_yes: f64,
    pub bid_size_no: f64,
    pub ask_size_no: f64,

    pub binance_price: Option<f64>,
    pub pm_live_btc_price: Option<f64>,
    pub ts_pm_live_exchange_ms: Option<i64>,
    pub ts_pm_live_recv_ms: Option<i64>,
    pub chainlink_price: Option<f64>,
    pub ts_chainlink_exchange_ms: Option<i64>,
    pub ts_chainlink_recv_ms: Option<i64>,

    pub delta_price: Option<f64>,
    pub delta_pct: Option<f64>,
    pub delta_pct_smooth: Option<f64>,
    pub remaining_ms: i64,
    pub velocity_bps_per_sec: Option<f64>,
    pub acceleration: Option<f64>,

    pub round_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoundRow {
    pub round_id: String,
    pub market_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub title: String,
    pub start_ts_ms: i64,
    pub end_ts_ms: i64,
    pub target_price: f64,
    pub settle_price: f64,
    pub label_up: bool,
    pub ts_recorded_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestLogRow {
    pub ts_ms: i64,
    pub level: String,
    pub component: String,
    pub message: String,
}

#[derive(Debug)]
pub enum PersistEvent {
    Snapshot(Box<SnapshotRow>),
    RelayTick(Box<RelayTickRow>),
    Round(RoundRow),
    Log(IngestLogRow),
}

#[derive(Debug, Clone, Copy)]
pub struct MotionState {
    pub ts_ms: i64,
    pub ema_price: f64,
    pub ema_velocity: f64,
}

pub fn tokyo_packet_type_tick() -> String {
    "tick".to_string()
}

pub fn tokyo_packet_type_replay_request() -> String {
    "replay_request".to_string()
}

pub fn compute_tokyo_wire_checksum(wire: &TokyoBinanceWire) -> String {
    let mut hasher = Sha256::new();
    hasher.update(wire.packet_type.as_bytes());
    hasher.update(wire.relay_seq.to_le_bytes());
    hasher.update(wire.source_seq.to_le_bytes());
    hasher.update(wire.source.as_bytes());
    hasher.update(wire.ts_tokyo_recv_ms.to_le_bytes());
    hasher.update(wire.ts_tokyo_send_ms.to_le_bytes());
    hasher.update(wire.ts_exchange_ms.to_le_bytes());
    hasher.update(wire.symbol.as_bytes());
    hasher.update(wire.binance_price.to_le_bytes());
    let digest = hasher.finalize();
    let mut out = String::with_capacity(16);
    for b in &digest[..8] {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{
        compute_tokyo_wire_checksum, tokyo_packet_type_tick, TokyoBinanceWire,
    };

    fn sample_wire() -> TokyoBinanceWire {
        TokyoBinanceWire {
            packet_type: tokyo_packet_type_tick(),
            relay_seq: 42,
            source_seq: 7,
            source: "binance_ws".to_string(),
            ts_tokyo_recv_ms: 1_700_000_000_000,
            ts_tokyo_send_ms: 1_700_000_000_005,
            ts_exchange_ms: 1_700_000_000_001,
            symbol: "BTCUSDT".to_string(),
            binance_price: 98_765.43,
            checksum: String::new(),
        }
    }

    #[test]
    fn tokyo_wire_checksum_is_stable_for_same_payload() {
        let wire = sample_wire();
        let a = compute_tokyo_wire_checksum(&wire);
        let b = compute_tokyo_wire_checksum(&wire);
        assert_eq!(a, b);
        assert!(!a.is_empty());
    }

    #[test]
    fn tokyo_wire_checksum_changes_when_payload_changes() {
        let wire = sample_wire();
        let checksum_a = compute_tokyo_wire_checksum(&wire);

        let mut changed = sample_wire();
        changed.relay_seq = changed.relay_seq.saturating_add(1);
        let checksum_b = compute_tokyo_wire_checksum(&changed);

        assert_ne!(checksum_a, checksum_b);
    }
}
