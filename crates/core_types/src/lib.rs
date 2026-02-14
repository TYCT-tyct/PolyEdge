use std::fmt;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RefTick {
    pub source: String,
    pub symbol: String,
    pub event_ts_ms: i64,
    pub recv_ts_ms: i64,
    #[serde(default)]
    pub event_ts_exchange_ms: i64,
    #[serde(default)]
    pub recv_ts_local_ns: i64,
    pub price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BookTop {
    pub market_id: String,
    pub token_id_yes: String,
    pub token_id_no: String,
    pub bid_yes: f64,
    pub ask_yes: f64,
    pub bid_no: f64,
    pub ask_no: f64,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BookLevel {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BookSnapshot {
    pub market_id: String,
    pub asset_id: String,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    pub ts_exchange_ms: i64,
    pub recv_ts_local_ns: i64,
    pub hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BookDelta {
    pub market_id: String,
    pub asset_id: String,
    pub side: BookSide,
    pub price: f64,
    pub size: f64,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub ts_exchange_ms: i64,
    pub recv_ts_local_ns: i64,
    pub hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderbookStateDigest {
    pub market_id: String,
    pub asset_id: String,
    pub best_bid: f64,
    pub best_ask: f64,
    pub spread: f64,
    pub ts_exchange_ms: i64,
    pub recv_ts_local_ns: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BookUpdate {
    Snapshot(BookSnapshot),
    Delta(BookDelta),
    Digest(OrderbookStateDigest),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Signal {
    pub market_id: String,
    pub fair_yes: f64,
    pub edge_bps_bid: f64,
    pub edge_bps_ask: f64,
    pub confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OrderSide {
    BuyYes,
    SellYes,
    BuyNo,
    SellNo,
}

impl fmt::Display for OrderSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::BuyYes => "buy_yes",
            Self::SellYes => "sell_yes",
            Self::BuyNo => "buy_no",
            Self::SellNo => "sell_no",
        };
        f.write_str(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QuoteIntent {
    pub market_id: String,
    pub side: OrderSide,
    pub price: f64,
    pub size: f64,
    pub ttl_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderAck {
    pub order_id: String,
    pub market_id: String,
    pub accepted: bool,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FillEvent {
    pub order_id: String,
    pub market_id: String,
    pub side: OrderSide,
    pub price: f64,
    pub size: f64,
    pub fee: f64,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RiskDecision {
    pub allow: bool,
    pub reason: String,
    pub capped_size: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PnLSnapshot {
    pub ts: DateTime<Utc>,
    pub realized: f64,
    pub unrealized: f64,
    pub max_drawdown_pct: f64,
    pub daily_pnl: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InventoryState {
    pub market_id: String,
    pub net_yes: f64,
    pub net_no: f64,
    pub exposure_notional: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RiskContext {
    pub market_id: String,
    pub symbol: String,
    pub order_count: usize,
    pub proposed_size: f64,
    pub market_notional: f64,
    pub asset_notional: f64,
    pub drawdown_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ToxicRegime {
    Safe,
    Caution,
    Danger,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToxicFeatures {
    pub market_id: String,
    pub symbol: String,
    pub markout_1s: f64,
    pub markout_5s: f64,
    pub markout_10s: f64,
    pub spread_bps: f64,
    pub microprice_drift: f64,
    pub stale_ms: f64,
    pub imbalance: f64,
    pub cancel_burst: f64,
    pub ts_ns: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToxicDecision {
    pub market_id: String,
    pub symbol: String,
    pub tox_score: f64,
    pub regime: ToxicRegime,
    pub reason_codes: Vec<String>,
    pub ts_ns: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QuoteEval {
    pub market_id: String,
    pub symbol: String,
    pub survival_10ms: f64,
    pub maker_markout_10s_bps: f64,
    pub adverse_flag: bool,
    pub ts_ns: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketHealth {
    pub market_id: String,
    pub symbol: String,
    pub symbol_missing_rate: f64,
    pub no_quote_rate: f64,
    pub pending_exposure: f64,
    pub queue_fill_proxy: f64,
    pub ts_ns: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GateContext {
    pub window_id: u64,
    pub min_outcomes: usize,
    pub eval_window_sec: u64,
    pub ready: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunAuditRow {
    pub run_id: String,
    pub cycle: u64,
    pub trial: u64,
    pub gate_ready: bool,
    pub gate_pass: bool,
    pub rollback_applied: bool,
    pub ts_utc: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EdgeAttribution {
    StaleQuote,
    BookMoved,
    SpreadTooWide,
    LiquidityThin,
    LatencyTail,
    FeeOverrun,
    AdverseSelection,
    InventoryBias,
    SignalLag,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ShadowShot {
    pub shot_id: String,
    pub market_id: String,
    pub symbol: String,
    pub side: OrderSide,
    #[serde(default)]
    pub survival_probe_price: f64,
    pub intended_price: f64,
    pub size: f64,
    pub edge_gross_bps: f64,
    pub edge_net_bps: f64,
    pub fee_paid_bps: f64,
    pub rebate_est_bps: f64,
    #[serde(default)]
    pub tox_score: f64,
    pub delay_ms: u64,
    pub t0_ns: i64,
    pub min_edge_bps: f64,
    pub ttl_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ShadowOutcome {
    pub shot_id: String,
    pub market_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub delay_ms: u64,
    pub fillable: bool,
    pub slippage_bps: Option<f64>,
    pub pnl_1s_bps: Option<f64>,
    pub pnl_5s_bps: Option<f64>,
    pub pnl_10s_bps: Option<f64>,
    pub net_markout_1s_bps: Option<f64>,
    pub net_markout_5s_bps: Option<f64>,
    pub net_markout_10s_bps: Option<f64>,
    #[serde(default)]
    pub queue_fill_prob: f64,
    #[serde(default)]
    pub is_outlier: bool,
    #[serde(default = "default_robust_weight")]
    pub robust_weight: f64,
    pub attribution: EdgeAttribution,
    pub ts_ns: i64,
}

fn default_robust_weight() -> f64 {
    1.0
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ControlCommand {
    Pause,
    Resume,
    Flatten,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EngineEvent {
    RefTick(RefTick),
    BookTop(BookTop),
    BookSnapshot(BookSnapshot),
    BookDelta(BookDelta),
    BookDigest(OrderbookStateDigest),
    Signal(Signal),
    ToxicFeatures(ToxicFeatures),
    ToxicDecision(ToxicDecision),
    QuoteIntent(QuoteIntent),
    QuoteEval(QuoteEval),
    OrderAck(OrderAck),
    Fill(FillEvent),
    ShadowShot(ShadowShot),
    ShadowOutcome(ShadowOutcome),
    Pnl(PnLSnapshot),
    Control(ControlCommand),
}

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("feed disconnected: {0}")]
    FeedDisconnected(String),
    #[error("invalid state: {0}")]
    InvalidState(String),
    #[error("execution failed: {0}")]
    Execution(String),
}

pub type DynStream<T> = BoxStream<'static, Result<T>>;

#[async_trait]
pub trait MarketFeed: Send + Sync {
    async fn stream_books(&self) -> Result<DynStream<BookTop>>;
}

#[async_trait]
pub trait RefPriceFeed: Send + Sync {
    async fn stream_ticks(&self, symbols: Vec<String>) -> Result<DynStream<RefTick>>;
}

#[async_trait]
pub trait RefPriceWsFeed: Send + Sync {
    async fn stream_ticks_ws(&self, symbols: Vec<String>) -> Result<DynStream<RefTick>>;
}

#[async_trait]
pub trait PolymarketBookWsFeed: Send + Sync {
    async fn stream_book(&self, token_ids: Vec<String>) -> Result<DynStream<BookUpdate>>;
}

pub trait FairValueModel: Send + Sync {
    fn evaluate(&self, tick: &RefTick, book: &BookTop) -> Signal;
}

pub trait QuotePolicy: Send + Sync {
    fn build_quotes(&self, signal: &Signal, inventory: &InventoryState) -> Vec<QuoteIntent>;

    fn build_quotes_with_toxicity(
        &self,
        signal: &Signal,
        inventory: &InventoryState,
        _toxicity: &ToxicDecision,
    ) -> Vec<QuoteIntent> {
        self.build_quotes(signal, inventory)
    }
}

pub trait ToxicityModel: Send + Sync {
    fn evaluate(&self, features: &ToxicFeatures) -> ToxicDecision;
}

#[async_trait]
pub trait ExecutionVenue: Send + Sync {
    async fn place_order(&self, intent: QuoteIntent) -> Result<OrderAck>;
    async fn cancel_order(&self, order_id: &str, market_id: &str) -> Result<()>;
    async fn flatten_all(&self) -> Result<()>;
}

pub trait RiskManager: Send + Sync {
    fn evaluate(&self, ctx: &RiskContext) -> RiskDecision;
}

#[async_trait]
pub trait ReplaySource: Send + Sync {
    async fn next_event(&mut self) -> Result<Option<EngineEvent>>;
}

pub fn new_id() -> String {
    Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_side_display() {
        assert_eq!(OrderSide::BuyYes.to_string(), "buy_yes");
        assert_eq!(OrderSide::SellNo.to_string(), "sell_no");
    }

    #[test]
    fn engine_event_json_roundtrip() {
        let tick = EngineEvent::RefTick(RefTick {
            source: "binance".to_string(),
            symbol: "BTCUSDT".to_string(),
            event_ts_ms: 1,
            recv_ts_ms: 2,
            event_ts_exchange_ms: 1,
            recv_ts_local_ns: 2,
            price: 50000.0,
        });

        let raw = serde_json::to_string(&tick).expect("serialize");
        let parsed: EngineEvent = serde_json::from_str(&raw).expect("deserialize");
        assert_eq!(parsed, tick);
    }
}
