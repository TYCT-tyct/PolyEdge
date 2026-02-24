use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokyoBinanceWire {
    pub ts_tokyo_recv_ms: i64,
    pub ts_tokyo_send_ms: i64,
    pub ts_exchange_ms: i64,
    pub symbol: String,
    pub binance_price: f64,
}

#[derive(Debug, Clone)]
pub struct TokyoBinanceLocal {
    pub ts_tokyo_recv_ms: i64,
    pub ts_exchange_ms: i64,
    pub ts_ireland_recv_ms: i64,
    pub binance_price: f64,
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
    pub ts_exchange_ms: Option<i64>,
    pub ts_ireland_recv_ms: Option<i64>,
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

    pub binance_price: Option<f64>,
    pub pm_live_btc_price: Option<f64>,
    pub ts_pm_live_exchange_ms: Option<i64>,
    pub ts_pm_live_recv_ms: Option<i64>,
    // Backward-compatible alias fields used by older dashboards/readers.
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
    Round(RoundRow),
    Log(IngestLogRow),
}

#[derive(Debug, Clone, Copy)]
pub struct MotionState {
    pub ts_ms: i64,
    pub ema_price: f64,
    pub ema_velocity: f64,
}
