use std::fmt;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RefTick {
    pub source: SmolStr,
    pub symbol: String,
    pub event_ts_ms: i64,
    pub recv_ts_ms: i64,
    #[serde(default)]
    pub source_seq: u64,
    #[serde(default)]
    pub event_ts_exchange_ms: i64,
    #[serde(default)]
    pub recv_ts_local_ns: i64,
    /// Local timestamp taken after decoding/parsing and right before enqueueing/publishing.
    /// This enables measuring pure local decode/ingest latency independent of exchange clocks.
    #[serde(default)]
    pub ingest_ts_local_ns: i64,
    /// First-hop local timestamp recorded at Tokyo relay ingress.
    /// Used to split exchange-lag vs private-path-lag in reports.
    #[serde(default)]
    pub ts_first_hop_ms: Option<i64>,
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
    #[serde(default)]
    pub recv_ts_local_ns: i64,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum TimeframeClass {
    Tf5m,
    Tf15m,
    Tf1h,
    Tf1d,
}

impl fmt::Display for TimeframeClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Tf5m => "5m",
            Self::Tf15m => "15m",
            Self::Tf1h => "1h",
            Self::Tf1d => "1d",
        };
        f.write_str(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Direction {
    Up,
    Down,
    Neutral,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DirectionSignal {
    pub symbol: String,
    pub direction: Direction,
    /// Price move magnitude in percentage points, e.g. 0.35 means +0.35%.
    pub magnitude_pct: f64,
    /// Confidence in [0,1], derived from multi-source consistency.
    pub confidence: f64,
    pub recommended_tf: TimeframeClass,
    /// First derivative of price move in bps/s.
    #[serde(default)]
    pub velocity_bps_per_sec: f64,
    /// Second derivative in (bps/s)/s.
    #[serde(default)]
    pub acceleration: f64,
    /// Count of consecutive same-direction ticks used by the triple-confirm gate.
    #[serde(default)]
    pub tick_consistency: u8,
    /// Whether the triple-confirm gate passed (velocity + acceleration/volume + consecutive ticks).
    /// Used by downstream scorers (e.g. WinRateScore) to assess signal quality.
    #[serde(default)]
    pub triple_confirm: bool,
    /// Whether the velocity exceeded the momentum-spike multiplier threshold.
    #[serde(default)]
    pub momentum_spike: bool,
    pub ts_ns: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CapitalUpdate {
    pub available_usdc: f64,
    /// "Base quote size" used by Predator C+ sizing logic. Interpreted as USDC notional.
    pub base_quote_size: f64,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProbabilityEstimate {
    pub p_fast: f64,
    pub p_settle: f64,
    pub confidence: f64,
    #[serde(default)]
    pub settlement_source_degraded: bool,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Early,
    Momentum,
    Maturity,
    Late,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutionIntent {
    pub market_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub style: ExecutionStyle,
    pub stage: Stage,
    pub edge_net_bps: f64,
    pub confidence: f64,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceHealth {
    pub source: String,
    pub latency_ms: f64,
    pub jitter_ms: f64,
    pub out_of_order_rate: f64,
    pub gap_rate: f64,
    pub price_deviation_bps: f64,
    #[serde(default)]
    pub freshness_score: f64,
    pub score: f64,
    #[serde(default)]
    pub sample_count: u64,
    #[serde(default)]
    pub ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TimeframeOpp {
    pub timeframe: TimeframeClass,
    pub market_id: String,
    pub symbol: String,
    pub direction: Direction,
    pub side: OrderSide,
    pub entry_price: f64,
    pub size: f64,
    pub edge_gross_bps: f64,
    pub edge_net_bps: f64,
    pub edge_net_usdc: f64,
    pub fee_bps: f64,
    pub lock_minutes: f64,
    pub density: f64,
    pub confidence: f64,
    pub ts_ms: i64,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStyle {
    Maker,
    Taker,
    Arb,
}

impl fmt::Display for ExecutionStyle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Maker => "maker",
            Self::Taker => "taker",
            Self::Arb => "arb",
        };
        f.write_str(value)
    }
}

fn default_execution_style() -> ExecutionStyle {
    ExecutionStyle::Maker
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderTimeInForce {
    Fak,
    Fok,
    Gtc,
    PostOnly,
}

impl fmt::Display for OrderTimeInForce {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Fak => "FAK",
            Self::Fok => "FOK",
            Self::Gtc => "GTC",
            Self::PostOnly => "POST_ONLY",
        };
        f.write_str(value)
    }
}

fn default_order_tif() -> OrderTimeInForce {
    OrderTimeInForce::PostOnly
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
pub struct OrderIntentV2 {
    pub market_id: String,
    pub side: OrderSide,
    #[serde(default)]
    pub token_id: Option<String>,
    pub price: f64,
    pub size: f64,
    pub ttl_ms: u64,
    #[serde(default = "default_execution_style")]
    pub style: ExecutionStyle,
    #[serde(default = "default_order_tif")]
    pub tif: OrderTimeInForce,
    #[serde(default)]
    pub client_order_id: Option<String>,
    #[serde(default)]
    pub max_slippage_bps: f64,
    #[serde(default)]
    pub fee_rate_bps: f64,
    #[serde(default)]
    pub expected_edge_net_bps: f64,
    #[serde(default)]
    pub hold_to_resolution: bool,
    /// B: 预序列化 JSON payload — 在信号确认后立即构建，跳过 place_order_v2 里的 serde 开销。
    /// 设置后 execution_clob 直接用此字节串作为 HTTP body，不再重新序列化。
    /// `None` 时回退到原始序列化路径，保证向后兼容。
    #[serde(skip)]
    pub prebuilt_payload: Option<Vec<u8>>,
}

impl From<QuoteIntent> for OrderIntentV2 {
    fn from(value: QuoteIntent) -> Self {
        Self {
            market_id: value.market_id,
            side: value.side,
            token_id: None,
            price: value.price,
            size: value.size,
            ttl_ms: value.ttl_ms,
            style: ExecutionStyle::Maker,
            tif: OrderTimeInForce::PostOnly,
            client_order_id: None,
            max_slippage_bps: 0.0,
            fee_rate_bps: 0.0,
            expected_edge_net_bps: 0.0,
            hold_to_resolution: false,
            prebuilt_payload: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderAckV2 {
    pub order_id: String,
    pub market_id: String,
    pub accepted: bool,
    #[serde(default)]
    pub accepted_size: f64,
    #[serde(default)]
    pub reject_code: Option<String>,
    #[serde(default)]
    pub exchange_latency_ms: f64,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FillEvent {
    pub order_id: String,
    pub market_id: String,
    pub side: OrderSide,
    #[serde(default = "default_execution_style")]
    pub style: ExecutionStyle,
    pub price: f64,
    pub size: f64,
    pub fee: f64,
    #[serde(default)]
    pub mid_price: Option<f64>,
    #[serde(default)]
    pub slippage_bps: Option<f64>,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PaperAction {
    Enter,
    Add,
    ReversalExit,
    LateHeavy,
    DoubleSide,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PaperIntent {
    pub ts_ms: i64,
    pub order_id: String,
    pub market_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub stage: Stage,
    pub direction: Direction,
    pub velocity_bps_per_sec: f64,
    pub edge_bps: f64,
    pub prob_fast: f64,
    pub prob_settle: f64,
    pub confidence: f64,
    pub action: PaperAction,
    pub intent: ExecutionStyle,
    pub requested_size_usdc: f64,
    pub requested_size_contracts: f64,
    pub entry_price: f64,
    pub seat_layer: Option<String>,
    pub tuned_params_before: Option<serde_json::Value>,
    pub tuned_params_after: Option<serde_json::Value>,
    pub rollback_triggered: Option<String>,
    pub shadow_pnl_comparison: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PaperFill {
    pub ts_ms: i64,
    pub order_id: String,
    pub market_id: String,
    pub side: OrderSide,
    pub style: ExecutionStyle,
    pub requested_size_usdc: f64,
    pub executed_size_usdc: f64,
    pub entry_price: f64,
    pub fill_price: f64,
    pub mid_price: f64,
    pub slippage_bps: f64,
    pub fee_usdc: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PaperTradeRecord {
    pub ts_ms: i64,
    pub paper_mode: String,
    pub market_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub stage: Stage,
    pub direction: Direction,
    pub velocity_bps_per_sec: f64,
    pub edge_bps: f64,
    pub prob_fast: f64,
    pub prob_settle: f64,
    pub confidence: f64,
    pub action: PaperAction,
    pub intent: ExecutionStyle,
    pub requested_size_usdc: f64,
    pub executed_size_usdc: f64,
    pub entry_price: f64,
    pub fill_price: f64,
    pub slippage_bps: f64,
    pub fee_usdc: f64,
    pub realized_pnl_usdc: f64,
    pub bankroll_before: f64,
    pub bankroll_after: f64,
    pub settlement_price: f64,
    pub chainlink_settlement_price: Option<f64>,
    pub settlement_source: String,
    pub forced_settlement: bool,
    pub trade_duration_ms: i64,
    pub seat_layer: Option<String>,
    pub tuned_params_before: Option<serde_json::Value>,
    pub tuned_params_after: Option<serde_json::Value>,
    pub rollback_triggered: Option<String>,
    pub shadow_pnl_comparison: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PaperDailySummary {
    pub utc_day: String,
    pub starting_bankroll: f64,
    pub ending_bankroll: f64,
    pub daily_roi_pct: f64,
    pub trades: u64,
    pub win_rate: f64,
    pub fee_total_usdc: f64,
    pub pnl_total_usdc: f64,
    pub avg_trade_duration_ms: f64,
    pub median_trade_duration_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PaperLiveReport {
    pub ts_ms: i64,
    pub run_id: String,
    pub initial_capital: f64,
    pub bankroll: f64,
    pub trades: u64,
    pub wins: u64,
    pub losses: u64,
    pub win_rate: f64,
    pub roi_pct: f64,
    pub max_drawdown_pct: f64,
    pub fee_total_usdc: f64,
    pub pnl_total_usdc: f64,
    pub fee_ratio: f64,
    pub avg_trade_duration_ms: f64,
    pub median_trade_duration_ms: f64,
    pub trade_count_source: String,
    pub open_positions_count: usize,
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
    pub proposed_notional_usdc: f64,
    pub market_notional: f64,
    pub asset_notional: f64,
    pub drawdown_pct: f64,
    pub loss_streak: u32,
    pub now_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ToxicRegime {
    Safe,
    Caution,
    Danger,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Regime {
    Active,
    Quiet,
    Defend,
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
pub struct OpportunityEval {
    pub market_id: String,
    pub symbol: String,
    pub delay_ms: u64,
    pub ev_net_usdc: f64,
    pub fee_usdc: f64,
    pub slippage_usdc: f64,
    pub stale: bool,
    pub ts_ns: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ExecutionFunnel {
    pub seen: u64,
    pub candidate: u64,
    pub quoted: u64,
    pub accepted: u64,
    pub filled: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CapitalState {
    pub equity_usdc: f64,
    pub alloc_usdc: f64,
    pub kelly_fraction: f64,
    pub drawdown_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RateBudgetState {
    pub rps_limit: f64,
    pub burst: f64,
    pub tokens: f64,
    pub last_refill_ns: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct EnginePnLBreakdown {
    pub maker_usdc: f64,
    pub taker_usdc: f64,
    pub arb_usdc: f64,
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
    #[serde(default = "default_execution_style")]
    pub execution_style: ExecutionStyle,
    /// Positive means: our fast reference tick arrived earlier than the Polymarket book update.
    /// See docs/metrics_contract.md for the contract definition.
    #[serde(default)]
    pub book_top_lag_ms: f64,
    /// Execution venue request/ack duration only (real in live mode).
    #[serde(default)]
    pub ack_only_ms: f64,
    /// End-to-end decision+execution latency (see app_runner for definition).
    #[serde(default)]
    pub tick_to_ack_ms: f64,
    /// `book_top_lag_ms - tick_to_ack_ms`; positive means we likely had time to capture.
    #[serde(default)]
    pub capturable_window_ms: f64,
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
    /// Whether the opportunity still existed at the probe delay (price still crossable),
    /// ignoring queue position / competition modeling.
    #[serde(default)]
    pub survived: bool,
    pub fillable: bool,
    #[serde(default = "default_execution_style")]
    pub execution_style: ExecutionStyle,
    pub slippage_bps: Option<f64>,
    pub pnl_1s_bps: Option<f64>,
    pub pnl_5s_bps: Option<f64>,
    pub pnl_10s_bps: Option<f64>,
    pub net_markout_1s_bps: Option<f64>,
    pub net_markout_5s_bps: Option<f64>,
    pub net_markout_10s_bps: Option<f64>,
    #[serde(default)]
    pub entry_notional_usdc: f64,
    #[serde(default)]
    pub net_markout_10s_usdc: Option<f64>,
    #[serde(default)]
    pub roi_notional_10s_bps: Option<f64>,
    #[serde(default)]
    pub queue_fill_prob: f64,
    #[serde(default)]
    pub is_stale_tick: bool,
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
    DirectionSignal(DirectionSignal),
    ToxicFeatures(ToxicFeatures),
    ToxicDecision(ToxicDecision),
    QuoteIntent(QuoteIntent),
    QuoteEval(QuoteEval),
    OrderAck(OrderAck),
    Fill(FillEvent),
    ShadowShot(ShadowShot),
    ShadowOutcome(ShadowOutcome),
    CapitalUpdate(CapitalUpdate),
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
    async fn place_order_v2(&self, intent: OrderIntentV2) -> Result<OrderAckV2> {
        let ack = self
            .place_order(QuoteIntent {
                market_id: intent.market_id.clone(),
                side: intent.side.clone(),
                price: intent.price,
                size: intent.size,
                ttl_ms: intent.ttl_ms,
            })
            .await?;
        Ok(OrderAckV2 {
            order_id: ack.order_id,
            market_id: ack.market_id,
            accepted: ack.accepted,
            accepted_size: if ack.accepted { intent.size } else { 0.0 },
            reject_code: if ack.accepted {
                None
            } else {
                Some("rejected".to_string())
            },
            exchange_latency_ms: 0.0,
            ts_ms: ack.ts_ms,
        })
    }
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
            source: "binance".into(),
            symbol: "BTCUSDT".into(),
            event_ts_ms: 1,
            recv_ts_ms: 2,
            source_seq: 0,
            event_ts_exchange_ms: 1,
            recv_ts_local_ns: 2,
            ingest_ts_local_ns: 2,
            ts_first_hop_ms: None,
            price: 50000.0,
        });

        let raw = serde_json::to_string(&tick).expect("serialize");
        let parsed: EngineEvent = serde_json::from_str(&raw).expect("deserialize");
        assert_eq!(parsed, tick);
    }
}
