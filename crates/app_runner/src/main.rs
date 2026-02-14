use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock as StdRwLock};
use std::time::{Duration, Instant};

use anyhow::Result;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use core_types::{
    new_id, BookTop, ControlCommand, EdgeAttribution, EngineEvent, EnginePnLBreakdown,
    ExecutionStyle, ExecutionVenue, FairValueModel, InventoryState, MarketFeed, MarketHealth,
    OrderAck, OrderIntentV2, OrderSide, OrderTimeInForce, QuoteEval, QuotePolicy, RefPriceFeed,
    RefTick, RiskContext, RiskManager, ShadowOutcome, ShadowShot, ToxicDecision, ToxicFeatures,
    ToxicRegime,
};
use execution_clob::{ClobExecution, ExecutionMode};
use fair_value::{BasisMrConfig, BasisMrFairValue};
use feed_polymarket::PolymarketFeed;
use feed_reference::MultiSourceRefFeed;
use futures::StreamExt;
use infra_bus::RingBus;
use market_discovery::{DiscoveryConfig, MarketDiscovery};
use observability::{init_metrics, init_tracing};
use paper_executor::ShadowExecutor;
use portfolio::PortfolioBook;
use reqwest::Client;
use risk_engine::{DefaultRiskManager, RiskLimits};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use strategy_maker::{MakerConfig, MakerQuotePolicy};
use tokio::sync::{mpsc, RwLock};

mod stats_utils;
mod control_api;
mod engine_core;
mod gate_eval;
mod orchestration;
use stats_utils::{
    estimate_uptime_pct, freshness_ms, now_ns, percentile, push_capped, robust_filter_iqr,
    value_to_f64,
};
use engine_core::{
    classify_execution_error_reason, classify_execution_style, edge_for_side,
    is_policy_block_reason, is_quote_reject_reason, normalize_reject_code,
};

#[derive(Clone)]
struct AppState {
    paused: Arc<RwLock<bool>>,
    bus: RingBus<EngineEvent>,
    portfolio: Arc<PortfolioBook>,
    execution: Arc<ClobExecution>,
    _shadow: Arc<ShadowExecutor>,
    prometheus: metrics_exporter_prometheus::PrometheusHandle,
    strategy_cfg: Arc<RwLock<MakerConfig>>,
    fair_value_cfg: Arc<StdRwLock<BasisMrConfig>>,
    toxicity_cfg: Arc<RwLock<ToxicityConfig>>,
    allocator_cfg: Arc<RwLock<AllocatorConfig>>,
    risk_limits: Arc<RwLock<RiskLimits>>,
    tox_state: Arc<RwLock<HashMap<String, MarketToxicState>>>,
    shadow_stats: Arc<ShadowStats>,
    perf_profile: Arc<RwLock<PerfProfile>>,
}

#[derive(Debug, Clone)]
struct FeeRateEntry {
    fee_bps: f64,
    fetched_at: Instant,
}

#[derive(Debug, Clone)]
struct ScoringState {
    scoring_true: u64,
    scoring_total: u64,
    rebate_bps_est: f64,
    fetched_at: Instant,
}

#[derive(Clone)]
struct EngineShared {
    latest_books: Arc<RwLock<HashMap<String, BookTop>>>,
    market_to_symbol: Arc<RwLock<HashMap<String, String>>>,
    token_to_symbol: Arc<RwLock<HashMap<String, String>>>,
    fee_cache: Arc<RwLock<HashMap<String, FeeRateEntry>>>,
    fee_refresh_inflight: Arc<RwLock<HashMap<String, Instant>>>,
    scoring_cache: Arc<RwLock<HashMap<String, ScoringState>>>,
    scoring_refresh_inflight: Arc<RwLock<HashMap<String, Instant>>>,
    http: Client,
    clob_endpoint: String,
    strategy_cfg: Arc<RwLock<MakerConfig>>,
    fair_value_cfg: Arc<StdRwLock<BasisMrConfig>>,
    toxicity_cfg: Arc<RwLock<ToxicityConfig>>,
    risk_limits: Arc<RwLock<RiskLimits>>,
    universe_symbols: Arc<Vec<String>>,
    rate_limit_rps: f64,
    scoring_rebate_factor: f64,
    tox_state: Arc<RwLock<HashMap<String, MarketToxicState>>>,
    shadow_stats: Arc<ShadowStats>,
}

#[derive(Serialize)]
struct HealthResp {
    status: &'static str,
    paused: bool,
}

#[derive(Debug, Deserialize)]
struct StrategyReloadReq {
    min_edge_bps: Option<f64>,
    ttl_ms: Option<u64>,
    inventory_skew: Option<f64>,
    base_quote_size: Option<f64>,
    max_spread: Option<f64>,
    basis_k_revert: Option<f64>,
    basis_z_cap: Option<f64>,
    basis_min_confidence: Option<f64>,
    taker_trigger_bps: Option<f64>,
    taker_max_slippage_bps: Option<f64>,
    stale_tick_filter_ms: Option<f64>,
    market_tier_profile: Option<String>,
    capital_fraction_kelly: Option<f64>,
    variance_penalty_lambda: Option<f64>,
}

#[derive(Debug, Serialize)]
struct StrategyReloadResp {
    maker: MakerConfig,
    fair_value: BasisMrConfig,
}

#[derive(Debug, Deserialize)]
struct RiskReloadReq {
    max_market_notional: Option<f64>,
    max_asset_notional: Option<f64>,
    max_open_orders: Option<usize>,
    daily_drawdown_cap_pct: Option<f64>,
    max_loss_streak: Option<u32>,
    cooldown_sec: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct TakerReloadReq {
    trigger_bps: Option<f64>,
    max_slippage_bps: Option<f64>,
    stale_tick_filter_ms: Option<f64>,
    market_tier_profile: Option<String>,
}

#[derive(Debug, Serialize)]
struct TakerReloadResp {
    trigger_bps: f64,
    max_slippage_bps: f64,
    stale_tick_filter_ms: f64,
    market_tier_profile: String,
}

#[derive(Debug, Deserialize)]
struct AllocatorReloadReq {
    capital_fraction_kelly: Option<f64>,
    variance_penalty_lambda: Option<f64>,
    active_top_n_markets: Option<usize>,
    taker_weight: Option<f64>,
    maker_weight: Option<f64>,
    arb_weight: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AllocatorConfig {
    capital_fraction_kelly: f64,
    variance_penalty_lambda: f64,
    active_top_n_markets: usize,
    taker_weight: f64,
    maker_weight: f64,
    arb_weight: f64,
}

impl Default for AllocatorConfig {
    fn default() -> Self {
        Self {
            capital_fraction_kelly: 0.35,
            variance_penalty_lambda: 0.25,
            active_top_n_markets: 8,
            taker_weight: 0.7,
            maker_weight: 0.2,
            arb_weight: 0.1,
        }
    }
}

#[derive(Debug, Serialize)]
struct AllocatorReloadResp {
    allocator: AllocatorConfig,
}

#[derive(Debug, Serialize)]
struct RiskReloadResp {
    risk: RiskLimits,
}

#[derive(Debug, Deserialize)]
struct ToxicityReloadReq {
    safe_threshold: Option<f64>,
    caution_threshold: Option<f64>,
    cooldown_min_sec: Option<u64>,
    cooldown_max_sec: Option<u64>,
    min_market_score: Option<f64>,
    active_top_n_markets: Option<usize>,
    markout_1s_caution_bps: Option<f64>,
    markout_5s_caution_bps: Option<f64>,
    markout_10s_caution_bps: Option<f64>,
    markout_1s_danger_bps: Option<f64>,
    markout_5s_danger_bps: Option<f64>,
    markout_10s_danger_bps: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct PerfProfileReloadReq {
    tail_guard: Option<f64>,
    io_flush_batch: Option<usize>,
    io_queue_capacity: Option<usize>,
    json_mode: Option<String>,
    io_drop_on_full: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExecutionConfig {
    mode: String,
    rate_limit_rps: f64,
    http_timeout_ms: u64,
    clob_endpoint: String,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            mode: "paper".to_string(),
            rate_limit_rps: 15.0,
            http_timeout_ms: 3000,
            clob_endpoint: "https://clob.polymarket.com".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PerfProfile {
    tail_guard: f64,
    io_flush_batch: usize,
    io_queue_capacity: usize,
    json_mode: String,
    io_drop_on_full: bool,
}

impl Default for PerfProfile {
    fn default() -> Self {
        Self {
            tail_guard: 0.99,
            io_flush_batch: 64,
            io_queue_capacity: 16_384,
            json_mode: "typed".to_string(),
            io_drop_on_full: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ToxicityConfig {
    w1: f64,
    w2: f64,
    w3: f64,
    w4: f64,
    w5: f64,
    w6: f64,
    safe_threshold: f64,
    caution_threshold: f64,
    k_spread: f64,
    cooldown_min_sec: u64,
    cooldown_max_sec: u64,
    min_market_score: f64,
    active_top_n_markets: usize,
    markout_1s_caution_bps: f64,
    markout_5s_caution_bps: f64,
    markout_10s_caution_bps: f64,
    markout_1s_danger_bps: f64,
    markout_5s_danger_bps: f64,
    markout_10s_danger_bps: f64,
}

impl Default for ToxicityConfig {
    fn default() -> Self {
        Self {
            w1: 0.30,
            w2: 0.25,
            w3: 0.20,
            w4: 0.10,
            w5: 0.10,
            w6: 0.05,
            safe_threshold: 0.35,
            caution_threshold: 0.65,
            k_spread: 1.5,
            cooldown_min_sec: 30,
            cooldown_max_sec: 120,
            min_market_score: 70.0,
            active_top_n_markets: 8,
            markout_1s_caution_bps: -4.0,
            markout_5s_caution_bps: -6.0,
            markout_10s_caution_bps: -8.0,
            markout_1s_danger_bps: -10.0,
            markout_5s_danger_bps: -14.0,
            markout_10s_danger_bps: -18.0,
        }
    }
}

#[derive(Debug, Clone)]
struct MarketToxicState {
    symbol: String,
    markout_1s: Vec<f64>,
    markout_5s: Vec<f64>,
    markout_10s: Vec<f64>,
    attempted: u64,
    no_quote: u64,
    symbol_missing: u64,
    last_tox_score: f64,
    last_regime: ToxicRegime,
    cooldown_until_ms: i64,
}

impl Default for MarketToxicState {
    fn default() -> Self {
        Self {
            symbol: String::new(),
            markout_1s: Vec::new(),
            markout_5s: Vec::new(),
            markout_10s: Vec::new(),
            attempted: 0,
            no_quote: 0,
            symbol_missing: 0,
            last_tox_score: 0.0,
            last_regime: ToxicRegime::Safe,
            cooldown_until_ms: 0,
        }
    }
}

#[derive(Debug, Serialize, Clone)]
struct ToxicityMarketRow {
    market_rank: usize,
    market_id: String,
    symbol: String,
    tox_score: f64,
    regime: ToxicRegime,
    market_score: f64,
    markout_10s_bps: f64,
    no_quote_rate: f64,
    symbol_missing_rate: f64,
    pending_exposure: f64,
    active_for_quoting: bool,
}

#[derive(Debug, Serialize)]
struct ToxicityLiveReport {
    ts_ms: i64,
    average_tox_score: f64,
    safe_count: usize,
    caution_count: usize,
    danger_count: usize,
    rows: Vec<ToxicityMarketRow>,
}

#[derive(Debug, Serialize)]
struct ToxicityFinalReport {
    pass: bool,
    failed_reasons: Vec<String>,
    live: ToxicityLiveReport,
}

#[derive(Debug, Serialize)]
struct GateEvaluation {
    window_id: u64,
    gate_ready: bool,
    min_outcomes: usize,
    pass: bool,
    data_valid_ratio: f64,
    seq_gap_rate: f64,
    ts_inversion_rate: f64,
    stale_tick_drop_ratio: f64,
    fillability_10ms: f64,
    net_edge_p50_bps: f64,
    net_edge_p10_bps: f64,
    net_markout_10s_usdc_p50: f64,
    roi_notional_10s_bps_p50: f64,
    pnl_10s_p50_bps_raw: f64,
    pnl_10s_p50_bps_robust: f64,
    pnl_10s_sample_count: usize,
    pnl_10s_outlier_ratio: f64,
    eligible_count: u64,
    executed_count: u64,
    executed_over_eligible: f64,
    ev_net_usdc_p50: f64,
    ev_net_usdc_p10: f64,
    ev_positive_ratio: f64,
    quote_block_ratio: f64,
    policy_block_ratio: f64,
    strategy_uptime_pct: f64,
    tick_to_ack_p99_ms: f64,
    decision_queue_wait_p99_ms: f64,
    decision_compute_p99_ms: f64,
    source_latency_p99_ms: f64,
    local_backlog_p99_ms: f64,
    failed_reasons: Vec<String>,
}

#[derive(Debug, Serialize, Clone, Default)]
struct LatencyBreakdown {
    feed_in_p50_ms: f64,
    feed_in_p90_ms: f64,
    feed_in_p99_ms: f64,
    signal_p50_us: f64,
    signal_p90_us: f64,
    signal_p99_us: f64,
    quote_p50_us: f64,
    quote_p90_us: f64,
    quote_p99_us: f64,
    risk_p50_us: f64,
    risk_p90_us: f64,
    risk_p99_us: f64,
    decision_queue_wait_p50_ms: f64,
    decision_queue_wait_p90_ms: f64,
    decision_queue_wait_p99_ms: f64,
    decision_compute_p50_ms: f64,
    decision_compute_p90_ms: f64,
    decision_compute_p99_ms: f64,
    tick_to_decision_p50_ms: f64,
    tick_to_decision_p90_ms: f64,
    tick_to_decision_p99_ms: f64,
    ack_only_p50_ms: f64,
    ack_only_p90_ms: f64,
    ack_only_p99_ms: f64,
    tick_to_ack_p50_ms: f64,
    tick_to_ack_p90_ms: f64,
    tick_to_ack_p99_ms: f64,
    parse_p99_us: f64,
    io_queue_p99_ms: f64,
    bus_lag_p99_ms: f64,
    shadow_fill_p50_ms: f64,
    shadow_fill_p90_ms: f64,
    shadow_fill_p99_ms: f64,
    source_latency_p50_ms: f64,
    source_latency_p90_ms: f64,
    source_latency_p99_ms: f64,
    local_backlog_p50_ms: f64,
    local_backlog_p90_ms: f64,
    local_backlog_p99_ms: f64,
}

#[derive(Debug, Serialize)]
struct ShadowLiveReport {
    window_id: u64,
    window_shots: usize,
    window_outcomes: usize,
    gate_ready: bool,
    gate_fail_reasons: Vec<String>,
    observe_only: bool,
    started_at_ms: i64,
    elapsed_sec: u64,
    total_shots: usize,
    total_outcomes: usize,
    data_valid_ratio: f64,
    seq_gap_rate: f64,
    ts_inversion_rate: f64,
    stale_tick_drop_ratio: f64,
    quote_attempted: u64,
    quote_blocked: u64,
    policy_blocked: u64,
    fillability_5ms: f64,
    fillability_10ms: f64,
    fillability_25ms: f64,
    survival_5ms: f64,
    survival_10ms: f64,
    survival_25ms: f64,
    net_edge_p50_bps: f64,
    net_edge_p10_bps: f64,
    pnl_1s_p50_bps: f64,
    pnl_5s_p50_bps: f64,
    pnl_10s_p50_bps: f64,
    pnl_10s_p50_bps_raw: f64,
    pnl_10s_p50_bps_robust: f64,
    net_markout_10s_usdc_p50: f64,
    roi_notional_10s_bps_p50: f64,
    pnl_10s_sample_count: usize,
    pnl_10s_outlier_ratio: f64,
    eligible_count: u64,
    executed_count: u64,
    executed_over_eligible: f64,
    ev_net_usdc_p50: f64,
    ev_net_usdc_p10: f64,
    ev_positive_ratio: f64,
    quote_block_ratio: f64,
    policy_block_ratio: f64,
    queue_depth_p99: f64,
    event_backlog_p99: f64,
    tick_to_decision_p50_ms: f64,
    tick_to_decision_p90_ms: f64,
    tick_to_decision_p99_ms: f64,
    decision_queue_wait_p99_ms: f64,
    decision_compute_p99_ms: f64,
    source_latency_p99_ms: f64,
    local_backlog_p99_ms: f64,
    ack_only_p50_ms: f64,
    ack_only_p90_ms: f64,
    ack_only_p99_ms: f64,
    strategy_uptime_pct: f64,
    tick_to_ack_p99_ms: f64,
    ref_ticks_total: u64,
    book_ticks_total: u64,
    ref_freshness_ms: i64,
    book_freshness_ms: i64,
    blocked_reason_counts: HashMap<String, u64>,
    latency: LatencyBreakdown,
    market_scorecard: Vec<MarketScoreRow>,
}

#[derive(Debug, Serialize)]
struct ShadowFinalReport {
    live: ShadowLiveReport,
    gate: GateEvaluation,
}

#[derive(Debug, Serialize, Clone)]
struct EnginePnlRow {
    engine: String,
    samples: usize,
    total_usdc: f64,
    p50_usdc: f64,
    p10_usdc: f64,
    positive_ratio: f64,
}

#[derive(Debug, Serialize, Clone)]
struct EnginePnlReport {
    window_id: u64,
    breakdown: EnginePnLBreakdown,
    rows: Vec<EnginePnlRow>,
}

#[derive(Debug, Serialize, Clone)]
struct MarketScoreRow {
    market_id: String,
    symbol: String,
    shots: usize,
    outcomes: usize,
    fillability_10ms: f64,
    net_edge_p50_bps: f64,
    net_edge_p10_bps: f64,
    pnl_10s_p50_bps: f64,
    net_markout_10s_usdc_p50: f64,
    roi_notional_10s_bps_p50: f64,
}

struct ShadowStats {
    window_id: AtomicU64,
    started_at: RwLock<Instant>,
    started_at_ms: AtomicI64,
    quote_attempted: AtomicU64,
    quote_blocked: AtomicU64,
    policy_blocked: AtomicU64,
    seen_count: AtomicU64,
    candidate_count: AtomicU64,
    quoted_count: AtomicU64,
    eligible_count: AtomicU64,
    executed_count: AtomicU64,
    filled_count: AtomicU64,
    blocked_reasons: RwLock<HashMap<String, u64>>,
    ref_ticks_total: AtomicU64,
    book_ticks_total: AtomicU64,
    last_ref_tick_ms: AtomicI64,
    last_book_tick_ms: AtomicI64,
    shots: RwLock<Vec<ShadowShot>>,
    outcomes: RwLock<Vec<ShadowOutcome>>,
    decision_queue_wait_ms: RwLock<Vec<f64>>,
    decision_compute_ms: RwLock<Vec<f64>>,
    tick_to_decision_ms: RwLock<Vec<f64>>,
    ack_only_ms: RwLock<Vec<f64>>,
    tick_to_ack_ms: RwLock<Vec<f64>>,
    feed_in_ms: RwLock<Vec<f64>>,
    source_latency_ms: RwLock<Vec<f64>>,
    local_backlog_ms: RwLock<Vec<f64>>,
    signal_us: RwLock<Vec<f64>>,
    quote_us: RwLock<Vec<f64>>,
    risk_us: RwLock<Vec<f64>>,
    shadow_fill_ms: RwLock<Vec<f64>>,
    queue_depth: RwLock<Vec<f64>>,
    event_backlog: RwLock<Vec<f64>>,
    parse_us: RwLock<Vec<f64>>,
    io_queue_depth: RwLock<Vec<f64>>,
    data_total: AtomicU64,
    data_invalid: AtomicU64,
    seq_gap: AtomicU64,
    ts_inversion: AtomicU64,
    stale_tick_dropped: AtomicU64,
    observe_only: AtomicBool,
}

impl ShadowStats {
    const SHADOW_CAP: usize = 200_000;
    const SAMPLE_CAP: usize = 65_536;
    const GATE_MIN_OUTCOMES: usize = 30;

    fn new() -> Self {
        Self {
            window_id: AtomicU64::new(0),
            started_at: RwLock::new(Instant::now()),
            started_at_ms: AtomicI64::new(Utc::now().timestamp_millis()),
            quote_attempted: AtomicU64::new(0),
            quote_blocked: AtomicU64::new(0),
            policy_blocked: AtomicU64::new(0),
            seen_count: AtomicU64::new(0),
            candidate_count: AtomicU64::new(0),
            quoted_count: AtomicU64::new(0),
            eligible_count: AtomicU64::new(0),
            executed_count: AtomicU64::new(0),
            filled_count: AtomicU64::new(0),
            blocked_reasons: RwLock::new(HashMap::new()),
            ref_ticks_total: AtomicU64::new(0),
            book_ticks_total: AtomicU64::new(0),
            last_ref_tick_ms: AtomicI64::new(0),
            last_book_tick_ms: AtomicI64::new(0),
            shots: RwLock::new(Vec::new()),
            outcomes: RwLock::new(Vec::new()),
            decision_queue_wait_ms: RwLock::new(Vec::new()),
            decision_compute_ms: RwLock::new(Vec::new()),
            tick_to_decision_ms: RwLock::new(Vec::new()),
            ack_only_ms: RwLock::new(Vec::new()),
            tick_to_ack_ms: RwLock::new(Vec::new()),
            feed_in_ms: RwLock::new(Vec::new()),
            source_latency_ms: RwLock::new(Vec::new()),
            local_backlog_ms: RwLock::new(Vec::new()),
            signal_us: RwLock::new(Vec::new()),
            quote_us: RwLock::new(Vec::new()),
            risk_us: RwLock::new(Vec::new()),
            shadow_fill_ms: RwLock::new(Vec::new()),
            queue_depth: RwLock::new(Vec::new()),
            event_backlog: RwLock::new(Vec::new()),
            parse_us: RwLock::new(Vec::new()),
            io_queue_depth: RwLock::new(Vec::new()),
            data_total: AtomicU64::new(0),
            data_invalid: AtomicU64::new(0),
            seq_gap: AtomicU64::new(0),
            ts_inversion: AtomicU64::new(0),
            stale_tick_dropped: AtomicU64::new(0),
            observe_only: AtomicBool::new(false),
        }
    }

    async fn reset(&self) -> u64 {
        let window_id = self.window_id.fetch_add(1, Ordering::Relaxed) + 1;
        *self.started_at.write().await = Instant::now();
        self.started_at_ms
            .store(Utc::now().timestamp_millis(), Ordering::Relaxed);
        self.quote_attempted.store(0, Ordering::Relaxed);
        self.quote_blocked.store(0, Ordering::Relaxed);
        self.policy_blocked.store(0, Ordering::Relaxed);
        self.seen_count.store(0, Ordering::Relaxed);
        self.candidate_count.store(0, Ordering::Relaxed);
        self.quoted_count.store(0, Ordering::Relaxed);
        self.eligible_count.store(0, Ordering::Relaxed);
        self.executed_count.store(0, Ordering::Relaxed);
        self.filled_count.store(0, Ordering::Relaxed);
        self.ref_ticks_total.store(0, Ordering::Relaxed);
        self.book_ticks_total.store(0, Ordering::Relaxed);
        self.last_ref_tick_ms.store(0, Ordering::Relaxed);
        self.last_book_tick_ms.store(0, Ordering::Relaxed);
        self.blocked_reasons.write().await.clear();
        self.shots.write().await.clear();
        self.outcomes.write().await.clear();
        self.decision_queue_wait_ms.write().await.clear();
        self.decision_compute_ms.write().await.clear();
        self.tick_to_decision_ms.write().await.clear();
        self.ack_only_ms.write().await.clear();
        self.tick_to_ack_ms.write().await.clear();
        self.feed_in_ms.write().await.clear();
        self.source_latency_ms.write().await.clear();
        self.local_backlog_ms.write().await.clear();
        self.signal_us.write().await.clear();
        self.quote_us.write().await.clear();
        self.risk_us.write().await.clear();
        self.shadow_fill_ms.write().await.clear();
        self.queue_depth.write().await.clear();
        self.event_backlog.write().await.clear();
        self.parse_us.write().await.clear();
        self.io_queue_depth.write().await.clear();
        self.data_total.store(0, Ordering::Relaxed);
        self.data_invalid.store(0, Ordering::Relaxed);
        self.seq_gap.store(0, Ordering::Relaxed);
        self.ts_inversion.store(0, Ordering::Relaxed);
        self.stale_tick_dropped.store(0, Ordering::Relaxed);
        self.observe_only.store(false, Ordering::Relaxed);
        window_id
    }

    async fn push_shot(&self, shot: ShadowShot) {
        let ingest_seq = next_normalized_ingest_seq();
        let source_seq = shot.t0_ns.max(0) as u64;
        append_jsonl(
            &dataset_path("normalized", "shadow_shots.jsonl"),
            &serde_json::json!({
                "ts_ms": Utc::now().timestamp_millis(),
                "source_seq": source_seq,
                "ingest_seq": ingest_seq,
                "shot": shot
            }),
        );
        let mut shots = self.shots.write().await;
        push_capped(&mut shots, shot, Self::SHADOW_CAP);
    }

    async fn push_outcome(&self, outcome: ShadowOutcome) {
        let ingest_seq = next_normalized_ingest_seq();
        let source_seq = outcome.ts_ns.max(0) as u64;
        append_jsonl(
            &dataset_path("normalized", "shadow_outcomes.jsonl"),
            &serde_json::json!({
                "ts_ms": Utc::now().timestamp_millis(),
                "source_seq": source_seq,
                "ingest_seq": ingest_seq,
                "outcome": outcome
            }),
        );
        let mut outcomes = self.outcomes.write().await;
        push_capped(&mut outcomes, outcome, Self::SHADOW_CAP);
    }

    async fn push_tick_to_decision_ms(&self, ms: f64) {
        let mut v = self.tick_to_decision_ms.write().await;
        push_capped(&mut v, ms, Self::SAMPLE_CAP);
    }

    async fn push_decision_queue_wait_ms(&self, ms: f64) {
        let mut v = self.decision_queue_wait_ms.write().await;
        push_capped(&mut v, ms, Self::SAMPLE_CAP);
    }

    async fn push_decision_compute_ms(&self, ms: f64) {
        let mut v = self.decision_compute_ms.write().await;
        push_capped(&mut v, ms, Self::SAMPLE_CAP);
    }

    async fn push_ack_only_ms(&self, ms: f64) {
        let mut v = self.ack_only_ms.write().await;
        push_capped(&mut v, ms, Self::SAMPLE_CAP);
    }

    async fn push_tick_to_ack_ms(&self, ms: f64) {
        let mut v = self.tick_to_ack_ms.write().await;
        push_capped(&mut v, ms, Self::SAMPLE_CAP);
    }

    async fn push_feed_in_ms(&self, ms: f64) {
        let mut v = self.feed_in_ms.write().await;
        push_capped(&mut v, ms, Self::SAMPLE_CAP);
    }

    async fn push_source_latency_ms(&self, ms: f64) {
        let mut v = self.source_latency_ms.write().await;
        push_capped(&mut v, ms, Self::SAMPLE_CAP);
    }

    async fn push_local_backlog_ms(&self, ms: f64) {
        let mut v = self.local_backlog_ms.write().await;
        push_capped(&mut v, ms, Self::SAMPLE_CAP);
    }

    async fn push_signal_us(&self, us: f64) {
        let mut v = self.signal_us.write().await;
        push_capped(&mut v, us, Self::SAMPLE_CAP);
    }

    async fn push_quote_us(&self, us: f64) {
        let mut v = self.quote_us.write().await;
        push_capped(&mut v, us, Self::SAMPLE_CAP);
    }

    async fn push_risk_us(&self, us: f64) {
        let mut v = self.risk_us.write().await;
        push_capped(&mut v, us, Self::SAMPLE_CAP);
    }

    async fn push_shadow_fill_ms(&self, ms: f64) {
        let mut v = self.shadow_fill_ms.write().await;
        push_capped(&mut v, ms, Self::SAMPLE_CAP);
    }

    async fn push_queue_depth(&self, depth: f64) {
        let mut v = self.queue_depth.write().await;
        push_capped(&mut v, depth, Self::SAMPLE_CAP);
    }

    async fn push_event_backlog(&self, depth: f64) {
        let mut v = self.event_backlog.write().await;
        push_capped(&mut v, depth, Self::SAMPLE_CAP);
    }

    async fn push_parse_us(&self, us: f64) {
        let mut v = self.parse_us.write().await;
        push_capped(&mut v, us, Self::SAMPLE_CAP);
    }

    async fn push_io_queue_depth(&self, depth: f64) {
        let mut v = self.io_queue_depth.write().await;
        push_capped(&mut v, depth, Self::SAMPLE_CAP);
    }

    fn mark_attempted(&self) {
        self.quote_attempted.fetch_add(1, Ordering::Relaxed);
    }

    fn mark_seen(&self) {
        self.seen_count.fetch_add(1, Ordering::Relaxed);
    }

    fn mark_candidate(&self) {
        self.candidate_count.fetch_add(1, Ordering::Relaxed);
    }

    fn mark_quoted(&self, n: u64) {
        self.quoted_count.fetch_add(n, Ordering::Relaxed);
    }

    fn mark_eligible(&self) {
        self.eligible_count.fetch_add(1, Ordering::Relaxed);
    }

    fn mark_executed(&self) {
        self.executed_count.fetch_add(1, Ordering::Relaxed);
    }

    fn mark_filled(&self, n: u64) {
        self.filled_count.fetch_add(n, Ordering::Relaxed);
    }

    fn mark_blocked(&self) {
        self.quote_blocked.fetch_add(1, Ordering::Relaxed);
    }

    fn mark_policy_blocked(&self) {
        self.policy_blocked.fetch_add(1, Ordering::Relaxed);
    }

    async fn mark_blocked_with_reason(&self, reason: &str) {
        if is_quote_reject_reason(reason) {
            self.mark_blocked();
        }
        if is_policy_block_reason(reason) {
            self.mark_policy_blocked();
        }
        let mut reasons = self.blocked_reasons.write().await;
        *reasons.entry(reason.to_string()).or_insert(0) += 1;
    }

    async fn record_issue(&self, reason: &str) {
        let mut reasons = self.blocked_reasons.write().await;
        *reasons.entry(reason.to_string()).or_insert(0) += 1;
    }

    fn mark_ref_tick(&self, ts_ms: i64) {
        self.ref_ticks_total.fetch_add(1, Ordering::Relaxed);
        self.last_ref_tick_ms.store(ts_ms, Ordering::Relaxed);
    }

    fn mark_book_tick(&self, ts_ms: i64) {
        self.book_ticks_total.fetch_add(1, Ordering::Relaxed);
        self.last_book_tick_ms.store(ts_ms, Ordering::Relaxed);
    }

    fn mark_data_validity(&self, valid: bool) {
        self.data_total.fetch_add(1, Ordering::Relaxed);
        if !valid {
            self.data_invalid.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn mark_ts_inversion(&self) {
        self.ts_inversion.fetch_add(1, Ordering::Relaxed);
    }

    fn mark_stale_tick_dropped(&self) {
        self.stale_tick_dropped.fetch_add(1, Ordering::Relaxed);
    }

    fn observe_only(&self) -> bool {
        self.observe_only.load(Ordering::Relaxed)
    }

    fn set_observe_only(&self, v: bool) {
        self.observe_only.store(v, Ordering::Relaxed);
    }
}

impl ShadowStats {
    async fn build_live_report(&self) -> ShadowLiveReport {
        const PRIMARY_DELAY_MS: u64 = 10;
        let shots = self.shots.read().await.clone();
        let outcomes = self.outcomes.read().await.clone();
        let decision_queue_wait_ms = self.decision_queue_wait_ms.read().await.clone();
        let decision_compute_ms = self.decision_compute_ms.read().await.clone();
        let tick_to_decision_ms = self.tick_to_decision_ms.read().await.clone();
        let ack_only_ms = self.ack_only_ms.read().await.clone();
        let tick_to_ack_ms = self.tick_to_ack_ms.read().await.clone();
        let feed_in_ms = self.feed_in_ms.read().await.clone();
        let source_latency_ms = self.source_latency_ms.read().await.clone();
        let local_backlog_ms = self.local_backlog_ms.read().await.clone();
        let signal_us = self.signal_us.read().await.clone();
        let quote_us = self.quote_us.read().await.clone();
        let risk_us = self.risk_us.read().await.clone();
        let shadow_fill_ms = self.shadow_fill_ms.read().await.clone();
        let queue_depth = self.queue_depth.read().await.clone();
        let event_backlog = self.event_backlog.read().await.clone();
        let parse_us = self.parse_us.read().await.clone();
        let io_queue_depth = self.io_queue_depth.read().await.clone();
        let blocked_reason_counts = self.blocked_reasons.read().await.clone();

        let fillability_5 = fillability_ratio(&outcomes, 5);
        let fillability_10 = fillability_ratio(&outcomes, 10);
        let fillability_25 = fillability_ratio(&outcomes, 25);

        let shots_primary = shots
            .iter()
            .filter(|s| s.delay_ms == PRIMARY_DELAY_MS)
            .collect::<Vec<_>>();
        let outcomes_primary = outcomes
            .iter()
            .filter(|o| o.delay_ms == PRIMARY_DELAY_MS)
            .collect::<Vec<_>>();
        let outcomes_primary_valid = outcomes_primary
            .iter()
            .filter(|o| !o.is_stale_tick)
            .collect::<Vec<_>>();
        let net_edges = shots_primary
            .iter()
            .map(|s| s.edge_net_bps)
            .collect::<Vec<_>>();
        let net_edge_p50 = percentile(&net_edges, 0.50).unwrap_or(0.0);
        let net_edge_p10 = percentile(&net_edges, 0.10).unwrap_or(0.0);
        let pnl_1s = outcomes_primary_valid
            .iter()
            .filter_map(|o| o.net_markout_1s_bps.or(o.pnl_1s_bps))
            .collect::<Vec<_>>();
        let pnl_5s = outcomes_primary_valid
            .iter()
            .filter_map(|o| o.net_markout_5s_bps.or(o.pnl_5s_bps))
            .collect::<Vec<_>>();
        let pnl_10s = outcomes_primary_valid
            .iter()
            .filter_map(|o| o.net_markout_10s_bps.or(o.pnl_10s_bps))
            .collect::<Vec<_>>();
        let net_markout_10s_usdc = outcomes_primary_valid
            .iter()
            .filter_map(|o| o.net_markout_10s_usdc)
            .collect::<Vec<_>>();
        let roi_notional_10s_bps = outcomes_primary_valid
            .iter()
            .filter_map(|o| o.roi_notional_10s_bps)
            .collect::<Vec<_>>();
        let pnl_1s_p50 = percentile(&pnl_1s, 0.50).unwrap_or(0.0);
        let pnl_5s_p50 = percentile(&pnl_5s, 0.50).unwrap_or(0.0);
        let pnl_10s_p50_raw = percentile(&pnl_10s, 0.50).unwrap_or(0.0);
        let net_markout_10s_usdc_p50 = percentile(&net_markout_10s_usdc, 0.50).unwrap_or(0.0);
        let roi_notional_10s_bps_p50 = percentile(&roi_notional_10s_bps, 0.50).unwrap_or(0.0);
        let ev_net_usdc_p50 = percentile(&net_markout_10s_usdc, 0.50).unwrap_or(0.0);
        let ev_net_usdc_p10 = percentile(&net_markout_10s_usdc, 0.10).unwrap_or(0.0);
        let ev_positive_ratio = if net_markout_10s_usdc.is_empty() {
            0.0
        } else {
            net_markout_10s_usdc.iter().filter(|v| **v > 0.0).count() as f64
                / net_markout_10s_usdc.len() as f64
        };
        let (pnl_10s_filtered, pnl_10s_outlier_ratio) = robust_filter_iqr(&pnl_10s);
        let pnl_10s_p50_robust = percentile(&pnl_10s_filtered, 0.50).unwrap_or(pnl_10s_p50_raw);
        let pnl_10s_sample_count = pnl_10s.len();

        let attempted = self.quote_attempted.load(Ordering::Relaxed);
        let blocked = self.quote_blocked.load(Ordering::Relaxed);
        let policy_blocked = self.policy_blocked.load(Ordering::Relaxed);
        let eligible_count = self.eligible_count.load(Ordering::Relaxed);
        let executed_count = self.executed_count.load(Ordering::Relaxed);
        let executed_over_eligible = if eligible_count == 0 {
            0.0
        } else {
            executed_count as f64 / eligible_count as f64
        };
        let block_ratio = if attempted == 0 {
            0.0
        } else {
            blocked as f64 / attempted as f64
        };
        let policy_block_ratio = if attempted + policy_blocked == 0 {
            0.0
        } else {
            policy_blocked as f64 / (attempted + policy_blocked) as f64
        };

        let elapsed = self.started_at.read().await.elapsed();
        let uptime_pct = estimate_uptime_pct(elapsed);
        let tick_to_ack_p99 = percentile(&tick_to_ack_ms, 0.99).unwrap_or(0.0);
        let scorecard = build_market_scorecard(&shots, &outcomes);
        let ref_ticks_total = self.ref_ticks_total.load(Ordering::Relaxed);
        let book_ticks_total = self.book_ticks_total.load(Ordering::Relaxed);
        let now_ms = Utc::now().timestamp_millis();
        let last_ref_tick_ms = self.last_ref_tick_ms.load(Ordering::Relaxed);
        let last_book_tick_ms = self.last_book_tick_ms.load(Ordering::Relaxed);
        let ref_freshness_ms = freshness_ms(now_ms, last_ref_tick_ms);
        let book_freshness_ms = freshness_ms(now_ms, last_book_tick_ms);
        let data_total = self.data_total.load(Ordering::Relaxed);
        let data_invalid = self.data_invalid.load(Ordering::Relaxed);
        let seq_gap = self.seq_gap.load(Ordering::Relaxed);
        let ts_inversion = self.ts_inversion.load(Ordering::Relaxed);
        let stale_tick_dropped = self.stale_tick_dropped.load(Ordering::Relaxed);
        let data_valid_ratio = if data_total == 0 {
            1.0
        } else {
            1.0 - (data_invalid as f64 / data_total as f64)
        };
        let seq_gap_rate = if data_total == 0 {
            0.0
        } else {
            seq_gap as f64 / data_total as f64
        };
        let ts_inversion_rate = if data_total == 0 {
            0.0
        } else {
            ts_inversion as f64 / data_total as f64
        };
        let stale_tick_drop_ratio = if data_total == 0 {
            0.0
        } else {
            stale_tick_dropped as f64 / data_total as f64
        };

        let latency = LatencyBreakdown {
            feed_in_p50_ms: percentile(&feed_in_ms, 0.50).unwrap_or(0.0),
            feed_in_p90_ms: percentile(&feed_in_ms, 0.90).unwrap_or(0.0),
            feed_in_p99_ms: percentile(&feed_in_ms, 0.99).unwrap_or(0.0),
            signal_p50_us: percentile(&signal_us, 0.50).unwrap_or(0.0),
            signal_p90_us: percentile(&signal_us, 0.90).unwrap_or(0.0),
            signal_p99_us: percentile(&signal_us, 0.99).unwrap_or(0.0),
            quote_p50_us: percentile(&quote_us, 0.50).unwrap_or(0.0),
            quote_p90_us: percentile(&quote_us, 0.90).unwrap_or(0.0),
            quote_p99_us: percentile(&quote_us, 0.99).unwrap_or(0.0),
            risk_p50_us: percentile(&risk_us, 0.50).unwrap_or(0.0),
            risk_p90_us: percentile(&risk_us, 0.90).unwrap_or(0.0),
            risk_p99_us: percentile(&risk_us, 0.99).unwrap_or(0.0),
            decision_queue_wait_p50_ms: percentile(&decision_queue_wait_ms, 0.50).unwrap_or(0.0),
            decision_queue_wait_p90_ms: percentile(&decision_queue_wait_ms, 0.90).unwrap_or(0.0),
            decision_queue_wait_p99_ms: percentile(&decision_queue_wait_ms, 0.99).unwrap_or(0.0),
            decision_compute_p50_ms: percentile(&decision_compute_ms, 0.50).unwrap_or(0.0),
            decision_compute_p90_ms: percentile(&decision_compute_ms, 0.90).unwrap_or(0.0),
            decision_compute_p99_ms: percentile(&decision_compute_ms, 0.99).unwrap_or(0.0),
            tick_to_decision_p50_ms: percentile(&tick_to_decision_ms, 0.50).unwrap_or(0.0),
            tick_to_decision_p90_ms: percentile(&tick_to_decision_ms, 0.90).unwrap_or(0.0),
            tick_to_decision_p99_ms: percentile(&tick_to_decision_ms, 0.99).unwrap_or(0.0),
            ack_only_p50_ms: percentile(&ack_only_ms, 0.50).unwrap_or(0.0),
            ack_only_p90_ms: percentile(&ack_only_ms, 0.90).unwrap_or(0.0),
            ack_only_p99_ms: percentile(&ack_only_ms, 0.99).unwrap_or(0.0),
            tick_to_ack_p50_ms: percentile(&tick_to_ack_ms, 0.50).unwrap_or(0.0),
            tick_to_ack_p90_ms: percentile(&tick_to_ack_ms, 0.90).unwrap_or(0.0),
            tick_to_ack_p99_ms: tick_to_ack_p99,
            parse_p99_us: percentile(&parse_us, 0.99).unwrap_or(0.0),
            io_queue_p99_ms: percentile(&io_queue_depth, 0.99).unwrap_or(0.0),
            bus_lag_p99_ms: percentile(&event_backlog, 0.99).unwrap_or(0.0),
            shadow_fill_p50_ms: percentile(&shadow_fill_ms, 0.50).unwrap_or(0.0),
            shadow_fill_p90_ms: percentile(&shadow_fill_ms, 0.90).unwrap_or(0.0),
            shadow_fill_p99_ms: percentile(&shadow_fill_ms, 0.99).unwrap_or(0.0),
            source_latency_p50_ms: percentile(&source_latency_ms, 0.50).unwrap_or(0.0),
            source_latency_p90_ms: percentile(&source_latency_ms, 0.90).unwrap_or(0.0),
            source_latency_p99_ms: percentile(&source_latency_ms, 0.99).unwrap_or(0.0),
            local_backlog_p50_ms: percentile(&local_backlog_ms, 0.50).unwrap_or(0.0),
            local_backlog_p90_ms: percentile(&local_backlog_ms, 0.90).unwrap_or(0.0),
            local_backlog_p99_ms: percentile(&local_backlog_ms, 0.99).unwrap_or(0.0),
        };

        let mut live = ShadowLiveReport {
            window_id: self.window_id.load(Ordering::Relaxed),
            window_shots: shots.len(),
            window_outcomes: outcomes.len(),
            gate_ready: outcomes.len() >= Self::GATE_MIN_OUTCOMES,
            gate_fail_reasons: Vec::new(),
            observe_only: self.observe_only(),
            started_at_ms: self.started_at_ms.load(Ordering::Relaxed),
            elapsed_sec: elapsed.as_secs(),
            total_shots: shots.len(),
            total_outcomes: outcomes.len(),
            data_valid_ratio,
            seq_gap_rate,
            ts_inversion_rate,
            stale_tick_drop_ratio,
            quote_attempted: attempted,
            quote_blocked: blocked,
            policy_blocked,
            fillability_5ms: fillability_5,
            fillability_10ms: fillability_10,
            fillability_25ms: fillability_25,
            survival_5ms: fillability_5,
            survival_10ms: fillability_10,
            survival_25ms: fillability_25,
            net_edge_p50_bps: net_edge_p50,
            net_edge_p10_bps: net_edge_p10,
            pnl_1s_p50_bps: pnl_1s_p50,
            pnl_5s_p50_bps: pnl_5s_p50,
            pnl_10s_p50_bps: pnl_10s_p50_raw,
            pnl_10s_p50_bps_raw: pnl_10s_p50_raw,
            pnl_10s_p50_bps_robust: pnl_10s_p50_robust,
            net_markout_10s_usdc_p50,
            roi_notional_10s_bps_p50,
            pnl_10s_sample_count,
            pnl_10s_outlier_ratio,
            eligible_count,
            executed_count,
            executed_over_eligible,
            ev_net_usdc_p50,
            ev_net_usdc_p10,
            ev_positive_ratio,
            quote_block_ratio: block_ratio,
            policy_block_ratio,
            queue_depth_p99: percentile(&queue_depth, 0.99).unwrap_or(0.0),
            event_backlog_p99: percentile(&event_backlog, 0.99).unwrap_or(0.0),
            tick_to_decision_p50_ms: latency.tick_to_decision_p50_ms,
            tick_to_decision_p90_ms: latency.tick_to_decision_p90_ms,
            tick_to_decision_p99_ms: latency.tick_to_decision_p99_ms,
            decision_queue_wait_p99_ms: latency.decision_queue_wait_p99_ms,
            decision_compute_p99_ms: latency.decision_compute_p99_ms,
            source_latency_p99_ms: latency.source_latency_p99_ms,
            local_backlog_p99_ms: latency.local_backlog_p99_ms,
            ack_only_p50_ms: latency.ack_only_p50_ms,
            ack_only_p90_ms: latency.ack_only_p90_ms,
            ack_only_p99_ms: latency.ack_only_p99_ms,
            strategy_uptime_pct: uptime_pct,
            tick_to_ack_p99_ms: tick_to_ack_p99,
            ref_ticks_total,
            book_ticks_total,
            ref_freshness_ms,
            book_freshness_ms,
            blocked_reason_counts,
            latency,
            market_scorecard: scorecard,
        };
        live.gate_fail_reasons =
            gate_eval::compute_gate_fail_reasons(&live, Self::GATE_MIN_OUTCOMES);
        live.gate_ready = live.window_outcomes >= Self::GATE_MIN_OUTCOMES;
        live
    }

    async fn build_final_report(&self) -> ShadowFinalReport {
        let live = self.build_live_report().await;
        let failed = live.gate_fail_reasons.clone();

        let gate = GateEvaluation {
            window_id: live.window_id,
            gate_ready: live.gate_ready,
            min_outcomes: Self::GATE_MIN_OUTCOMES,
            pass: failed.is_empty(),
            data_valid_ratio: live.data_valid_ratio,
            seq_gap_rate: live.seq_gap_rate,
            ts_inversion_rate: live.ts_inversion_rate,
            stale_tick_drop_ratio: live.stale_tick_drop_ratio,
            fillability_10ms: live.fillability_10ms,
            net_edge_p50_bps: live.net_edge_p50_bps,
            net_edge_p10_bps: live.net_edge_p10_bps,
            net_markout_10s_usdc_p50: live.net_markout_10s_usdc_p50,
            roi_notional_10s_bps_p50: live.roi_notional_10s_bps_p50,
            pnl_10s_p50_bps_raw: live.pnl_10s_p50_bps_raw,
            pnl_10s_p50_bps_robust: live.pnl_10s_p50_bps_robust,
            pnl_10s_sample_count: live.pnl_10s_sample_count,
            pnl_10s_outlier_ratio: live.pnl_10s_outlier_ratio,
            eligible_count: live.eligible_count,
            executed_count: live.executed_count,
            executed_over_eligible: live.executed_over_eligible,
            ev_net_usdc_p50: live.ev_net_usdc_p50,
            ev_net_usdc_p10: live.ev_net_usdc_p10,
            ev_positive_ratio: live.ev_positive_ratio,
            quote_block_ratio: live.quote_block_ratio,
            policy_block_ratio: live.policy_block_ratio,
            strategy_uptime_pct: live.strategy_uptime_pct,
            tick_to_ack_p99_ms: live.tick_to_ack_p99_ms,
            decision_queue_wait_p99_ms: live.decision_queue_wait_p99_ms,
            decision_compute_p99_ms: live.decision_compute_p99_ms,
            source_latency_p99_ms: live.latency.source_latency_p99_ms,
            local_backlog_p99_ms: live.latency.local_backlog_p99_ms,
            failed_reasons: failed,
        };
        ShadowFinalReport { live, gate }
    }

    async fn build_engine_pnl_report(&self) -> EnginePnlReport {
        const PRIMARY_DELAY_MS: u64 = 10;
        let shots = self.shots.read().await.clone();
        let outcomes = self.outcomes.read().await.clone();
        let mut style_by_shot = HashMap::<String, ExecutionStyle>::new();
        for shot in shots.iter().filter(|s| s.delay_ms == PRIMARY_DELAY_MS) {
            style_by_shot.insert(shot.shot_id.clone(), shot.execution_style.clone());
        }

        let mut maker = Vec::<f64>::new();
        let mut taker = Vec::<f64>::new();
        let mut arb = Vec::<f64>::new();

        for outcome in outcomes
            .iter()
            .filter(|o| o.delay_ms == PRIMARY_DELAY_MS && !o.is_stale_tick)
        {
            let Some(markout) = outcome.net_markout_10s_usdc else {
                continue;
            };
            let style = style_by_shot
                .get(&outcome.shot_id)
                .cloned()
                .unwrap_or_else(|| outcome.execution_style.clone());
            match style {
                ExecutionStyle::Maker => maker.push(markout),
                ExecutionStyle::Taker => taker.push(markout),
                ExecutionStyle::Arb => arb.push(markout),
            }
        }

        let maker_total = maker.iter().sum::<f64>();
        let taker_total = taker.iter().sum::<f64>();
        let arb_total = arb.iter().sum::<f64>();

        EnginePnlReport {
            window_id: self.window_id.load(Ordering::Relaxed),
            breakdown: EnginePnLBreakdown {
                maker_usdc: maker_total,
                taker_usdc: taker_total,
                arb_usdc: arb_total,
            },
            rows: vec![
                build_engine_pnl_row("maker", &maker),
                build_engine_pnl_row("taker", &taker),
                build_engine_pnl_row("arb", &arb),
            ],
        }
    }
}

fn build_engine_pnl_row(engine: &str, values: &[f64]) -> EnginePnlRow {
    let samples = values.len();
    let total_usdc = values.iter().sum::<f64>();
    let p50_usdc = percentile(values, 0.50).unwrap_or(0.0);
    let p10_usdc = percentile(values, 0.10).unwrap_or(0.0);
    let positive_ratio = if values.is_empty() {
        0.0
    } else {
        values.iter().filter(|v| **v > 0.0).count() as f64 / values.len() as f64
    };
    EnginePnlRow {
        engine: engine.to_string(),
        samples,
        total_usdc,
        p50_usdc,
        p10_usdc,
        positive_ratio,
    }
}

fn main() -> Result<()> {
    install_rustls_provider();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async_main())
}

async fn async_main() -> Result<()> {
    init_tracing("app_runner");
    let prometheus = init_metrics();
    ensure_dataset_dirs();

    let execution_cfg = load_execution_config();
    let universe_cfg = load_universe_config();
    let bus = RingBus::new(16_384);
    let portfolio = Arc::new(PortfolioBook::default());
    let exec_mode = if execution_cfg.mode.eq_ignore_ascii_case("live") {
        ExecutionMode::Live
    } else {
        ExecutionMode::Paper
    };
    let execution = Arc::new(ClobExecution::new_with_timeout(
        exec_mode,
        execution_cfg.clob_endpoint.clone(),
        Duration::from_millis(execution_cfg.http_timeout_ms),
    ));
    let shadow = Arc::new(ShadowExecutor::default());
    let strategy_cfg = Arc::new(RwLock::new(load_strategy_config()));
    let fair_value_cfg = Arc::new(StdRwLock::new(load_fair_value_config()));
    let toxicity_cfg = Arc::new(RwLock::new(ToxicityConfig::default()));
    let risk_limits = Arc::new(RwLock::new(load_risk_limits_config()));
    let perf_profile = Arc::new(RwLock::new(load_perf_profile_config()));
    let allocator_cfg = {
        let strategy = strategy_cfg.read().await.clone();
        let tox = toxicity_cfg.read().await.clone();
        Arc::new(RwLock::new(AllocatorConfig {
            capital_fraction_kelly: strategy.capital_fraction_kelly,
            variance_penalty_lambda: strategy.variance_penalty_lambda,
            active_top_n_markets: tox.active_top_n_markets,
            ..AllocatorConfig::default()
        }))
    };
    let tox_state = Arc::new(RwLock::new(HashMap::new()));
    let shadow_stats = Arc::new(ShadowStats::new());
    let paused = Arc::new(RwLock::new(false));
    let universe_symbols = Arc::new(universe_cfg.assets.clone());
    init_jsonl_writer(perf_profile.clone()).await;

    let state = AppState {
        paused: paused.clone(),
        bus: bus.clone(),
        portfolio: portfolio.clone(),
        execution: execution.clone(),
        _shadow: shadow.clone(),
        prometheus,
        strategy_cfg: strategy_cfg.clone(),
        fair_value_cfg: fair_value_cfg.clone(),
        toxicity_cfg: toxicity_cfg.clone(),
        allocator_cfg: allocator_cfg.clone(),
        risk_limits: risk_limits.clone(),
        tox_state: tox_state.clone(),
        shadow_stats: shadow_stats.clone(),
        perf_profile: perf_profile.clone(),
    };

    let scoring_rebate_factor = std::env::var("POLYEDGE_SCORING_REBATE_FACTOR")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.25)
        .clamp(0.0, 1.0);

    let shared = Arc::new(EngineShared {
        latest_books: Arc::new(RwLock::new(HashMap::new())),
        market_to_symbol: Arc::new(RwLock::new(HashMap::new())),
        token_to_symbol: Arc::new(RwLock::new(HashMap::new())),
        fee_cache: Arc::new(RwLock::new(HashMap::new())),
        fee_refresh_inflight: Arc::new(RwLock::new(HashMap::new())),
        scoring_cache: Arc::new(RwLock::new(HashMap::new())),
        scoring_refresh_inflight: Arc::new(RwLock::new(HashMap::new())),
        http: Client::new(),
        clob_endpoint: execution_cfg.clob_endpoint.clone(),
        strategy_cfg,
        fair_value_cfg,
        toxicity_cfg,
        risk_limits,
        universe_symbols: universe_symbols.clone(),
        rate_limit_rps: execution_cfg.rate_limit_rps.max(0.1),
        scoring_rebate_factor,
        tox_state,
        shadow_stats,
    });

    spawn_reference_feed(
        bus.clone(),
        shared.shadow_stats.clone(),
        (*universe_symbols).clone(),
    );
    spawn_market_feed(
        bus.clone(),
        shared.shadow_stats.clone(),
        (*universe_symbols).clone(),
    );
    spawn_strategy_engine(
        bus.clone(),
        portfolio,
        execution.clone(),
        shadow,
        paused.clone(),
        shared.clone(),
    );
    orchestration::spawn_periodic_report_persistor(
        shared.shadow_stats.clone(),
        shared.tox_state.clone(),
        execution.clone(),
        shared.toxicity_cfg.clone(),
    );
    orchestration::spawn_data_reconcile_task(bus.clone(), paused.clone(), shared.shadow_stats.clone());

    let app = control_api::build_router(state);

    let addr: SocketAddr = "0.0.0.0:8080".parse()?;
    tracing::info!(%addr, "control api started");
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;
    Ok(())
}

fn install_rustls_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

fn spawn_reference_feed(
    bus: RingBus<EngineEvent>,
    stats: Arc<ShadowStats>,
    symbols: Vec<String>,
) {
    const TS_INVERSION_TOLERANCE_MS: i64 = 250;
    tokio::spawn(async move {
        let feed = MultiSourceRefFeed::new(Duration::from_millis(50));
        if symbols.is_empty() {
            tracing::warn!("reference feed symbols empty; using fallback BTCUSDT");
        }
        let symbols = if symbols.is_empty() {
            vec!["BTCUSDT".to_string()]
        } else {
            symbols
        };
        let Ok(mut stream) = feed.stream_ticks(symbols).await else {
            tracing::error!("reference feed failed to start");
            return;
        };
        let mut ingest_seq: u64 = 0;
        let mut last_source_ts_by_stream: HashMap<String, i64> = HashMap::new();

        while let Some(item) = stream.next().await {
            match item {
                Ok(tick) => {
                    ingest_seq = ingest_seq.saturating_add(1);
                    let source_ts = tick.event_ts_exchange_ms.max(tick.event_ts_ms);
                    let source_seq = source_ts.max(0) as u64;
                    let valid = !tick.symbol.is_empty()
                        && tick.price.is_finite()
                        && tick.price > 0.0
                        && source_seq > 0;
                    stats.mark_data_validity(valid);
                    let stream_key = format!("{}:{}", tick.source, tick.symbol);
                    if let Some(prev) = last_source_ts_by_stream.get(&stream_key).copied() {
                        if source_ts + TS_INVERSION_TOLERANCE_MS < prev {
                            stats.mark_ts_inversion();
                        }
                    }
                    last_source_ts_by_stream.insert(stream_key, source_ts);
                    stats.mark_ref_tick(tick.recv_ts_ms);
                    let tick_json = serde_json::to_value(&tick).unwrap_or(serde_json::json!({}));
                    let hash = sha256_hex(&tick_json.to_string());
                    append_jsonl(
                        &dataset_path("raw", "ref_ticks.jsonl"),
                        &serde_json::json!({
                            "ts_ms": Utc::now().timestamp_millis(),
                            "source_seq": source_seq,
                            "ingest_seq": ingest_seq,
                            "valid": valid,
                            "sha256": hash,
                            "tick": tick_json
                        }),
                    );
                    if !valid {
                        stats.record_issue("invalid_ref_tick").await;
                        continue;
                    }
                    let _ = bus.publish(EngineEvent::RefTick(tick));
                }
                Err(err) => {
                    tracing::warn!(?err, "reference feed event error");
                }
            }
        }
    });
}

fn spawn_market_feed(bus: RingBus<EngineEvent>, stats: Arc<ShadowStats>, symbols: Vec<String>) {
    const TS_INVERSION_TOLERANCE_MS: i64 = 250;
    tokio::spawn(async move {
        let feed = PolymarketFeed::new_with_symbols(Duration::from_millis(50), symbols);
        let Ok(mut stream) = feed.stream_books().await else {
            tracing::error!("market feed failed to start");
            return;
        };
        let mut ingest_seq: u64 = 0;
        let mut last_source_ts_by_market: HashMap<String, i64> = HashMap::new();

        while let Some(item) = stream.next().await {
            match item {
                Ok(book) => {
                    ingest_seq = ingest_seq.saturating_add(1);
                    let source_ts = book.ts_ms;
                    let source_seq = source_ts.max(0) as u64;
                    let valid = !book.market_id.is_empty()
                        && book.bid_yes.is_finite()
                        && book.ask_yes.is_finite()
                        && book.bid_no.is_finite()
                        && book.ask_no.is_finite()
                        && source_seq > 0;
                    stats.mark_data_validity(valid);
                    if let Some(prev) = last_source_ts_by_market.get(&book.market_id).copied() {
                        if source_ts + TS_INVERSION_TOLERANCE_MS < prev {
                            stats.mark_ts_inversion();
                        }
                    }
                    last_source_ts_by_market.insert(book.market_id.clone(), source_ts);
                    stats.mark_book_tick(book.ts_ms);
                    let book_json = serde_json::to_value(&book).unwrap_or(serde_json::json!({}));
                    let hash = sha256_hex(&book_json.to_string());
                    append_jsonl(
                        &dataset_path("raw", "book_tops.jsonl"),
                        &serde_json::json!({
                            "ts_ms": Utc::now().timestamp_millis(),
                            "source_seq": source_seq,
                            "ingest_seq": ingest_seq,
                            "valid": valid,
                            "sha256": hash,
                            "book": book_json
                        }),
                    );
                    if !valid {
                        stats.record_issue("invalid_book_top").await;
                        continue;
                    }
                    let _ = bus.publish(EngineEvent::BookTop(book));
                }
                Err(err) => {
                    tracing::warn!(?err, "market feed event error");
                }
            }
        }
    });
}

fn spawn_strategy_engine(
    bus: RingBus<EngineEvent>,
    portfolio: Arc<PortfolioBook>,
    execution: Arc<ClobExecution>,
    shadow: Arc<ShadowExecutor>,
    paused: Arc<RwLock<bool>>,
    shared: Arc<EngineShared>,
) {
    tokio::spawn(async move {
        let fair = BasisMrFairValue::new(shared.fair_value_cfg.clone());
        let mut latest_ticks: HashMap<String, RefTick> = HashMap::new();
        let mut market_inventory: HashMap<String, InventoryState> = HashMap::new();
        let mut market_rate_budget: HashMap<String, TokenBucket> = HashMap::new();
        let mut global_rate_budget = TokenBucket::new(
            shared.rate_limit_rps,
            (shared.rate_limit_rps * 2.0).max(1.0),
        );
        let mut rx = bus.subscribe();
        let mut last_discovery_refresh = Instant::now() - Duration::from_secs(3600);
        let mut last_symbol_retry_refresh = Instant::now() - Duration::from_secs(3600);
        refresh_market_symbol_map(&shared).await;

        loop {
            if last_discovery_refresh.elapsed() >= Duration::from_secs(300) {
                refresh_market_symbol_map(&shared).await;
                last_discovery_refresh = Instant::now();
            }

            let recv = rx.recv().await;
            let Ok(event) = recv else {
                continue;
            };
            let dispatch_start = Instant::now();

            match event {
                EngineEvent::RefTick(tick) => {
                    let parse_us = dispatch_start.elapsed().as_secs_f64() * 1_000_000.0;
                    shared.shadow_stats.push_parse_us(parse_us).await;
                    insert_latest_tick(&mut latest_ticks, tick);
                }
                EngineEvent::BookTop(mut book) => {
                    let parse_us = dispatch_start.elapsed().as_secs_f64() * 1_000_000.0;
                    shared.shadow_stats.push_parse_us(parse_us).await;
                    // Coalesce bursty queue traffic to the freshest observable state.
                    // This trims local backlog and avoids spending cycles on superseded snapshots.
                    let mut coalesced = 0_u64;
                    let dynamic_cap = rx.len().saturating_add(64).min(4_096);
                    let max_coalesced = dynamic_cap.max(256);
                    while coalesced < max_coalesced as u64 {
                        match rx.try_recv() {
                            Ok(EngineEvent::BookTop(next_book)) => {
                                book = next_book;
                                coalesced += 1;
                            }
                            Ok(EngineEvent::RefTick(next_tick)) => {
                                insert_latest_tick(&mut latest_ticks, next_tick);
                                coalesced += 1;
                            }
                            Ok(_) => {
                                // This subscriber doesn't consume other event kinds.
                                // Drain them here to keep queue pressure bounded.
                                coalesced += 1;
                            }
                            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {
                                shared.shadow_stats.record_issue("bus_lagged").await;
                                break;
                            }
                            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
                        }
                    }
                    if coalesced > 0 {
                        metrics::counter!("runtime.coalesced_events").increment(coalesced);
                    }

                    let backlog_depth = rx.len() as f64;
                    shared.shadow_stats.push_event_backlog(backlog_depth).await;
                    metrics::histogram!("runtime.event_backlog_depth").record(backlog_depth);
                    let queue_depth = execution.open_orders_count() as f64;
                    shared.shadow_stats.push_queue_depth(queue_depth).await;
                    metrics::histogram!("runtime.open_order_depth").record(queue_depth);
                    let io_depth = current_jsonl_queue_depth() as f64;
                    shared.shadow_stats.push_io_queue_depth(io_depth).await;
                    metrics::histogram!("runtime.jsonl_queue_depth").record(io_depth);
                    shared
                        .latest_books
                        .write()
                        .await
                        .insert(book.market_id.clone(), book.clone());

                    let fills = shadow.on_book(&book);
                    shared.shadow_stats.mark_filled(fills.len() as u64);
                    for fill in fills {
                        portfolio.apply_fill(&fill);
                        let _ = bus.publish(EngineEvent::Fill(fill.clone()));
                        let ingest_seq = next_normalized_ingest_seq();
                        append_jsonl(
                            &dataset_path("normalized", "fills.jsonl"),
                            &serde_json::json!({
                                "ts_ms": Utc::now().timestamp_millis(),
                                "source_seq": Utc::now().timestamp_millis().max(0) as u64,
                                "ingest_seq": ingest_seq,
                                "fill": fill
                            }),
                        );
                    }

                    if *paused.read().await {
                        shared.shadow_stats.record_issue("paused").await;
                        continue;
                    }
                    if shared.shadow_stats.observe_only() {
                        shared.shadow_stats.record_issue("observe_only").await;
                        continue;
                    }

                    let symbol = pick_market_symbol(&shared, &book).await;
                    let Some(symbol) = symbol else {
                        if market_is_tracked(&shared, &book).await {
                            {
                                let mut states = shared.tox_state.write().await;
                                let st = states.entry(book.market_id.clone()).or_default();
                                st.symbol_missing = st.symbol_missing.saturating_add(1);
                            }
                            shared.shadow_stats.record_issue("symbol_missing").await;
                        } else {
                            shared.shadow_stats.record_issue("market_untracked").await;
                        }
                        if last_symbol_retry_refresh.elapsed() >= Duration::from_secs(15) {
                            refresh_market_symbol_map(&shared).await;
                            last_symbol_retry_refresh = Instant::now();
                        }
                        continue;
                    };
                    let tick = latest_ticks
                        .get(&symbol)
                        .cloned()
                        .or_else(|| latest_ticks.values().max_by_key(|t| t.recv_ts_ms).cloned());
                    let Some(tick) = tick else {
                        shared.shadow_stats.record_issue("tick_missing").await;
                        continue;
                    };
                    shared.shadow_stats.mark_seen();

                    let latency_sample = estimate_feed_latency(&tick, &book);
                    let feed_in_ms = latency_sample.feed_in_ms;
                    let stale_tick_filter_ms = shared.strategy_cfg.read().await.stale_tick_filter_ms;
                    if feed_in_ms > stale_tick_filter_ms {
                        shared.shadow_stats.mark_stale_tick_dropped();
                        shared.shadow_stats.record_issue("stale_tick_dropped").await;
                        continue;
                    }
                    shared.shadow_stats.push_feed_in_ms(feed_in_ms).await;
                    shared
                        .shadow_stats
                        .push_source_latency_ms(latency_sample.source_latency_ms)
                        .await;
                    shared
                        .shadow_stats
                        .push_local_backlog_ms(latency_sample.local_backlog_ms)
                        .await;
                    shared
                        .shadow_stats
                        .push_decision_queue_wait_ms(latency_sample.local_backlog_ms)
                        .await;
                    metrics::histogram!("latency.feed_in_ms").record(feed_in_ms);
                    metrics::histogram!("latency.source_latency_ms")
                        .record(latency_sample.source_latency_ms);
                    metrics::histogram!("latency.local_backlog_ms")
                        .record(latency_sample.local_backlog_ms);
                    let signal_start = Instant::now();
                    let signal = fair.evaluate(&tick, &book);
                    if signal.edge_bps_bid > 0.0 || signal.edge_bps_ask > 0.0 {
                        shared.shadow_stats.mark_candidate();
                    }
                    let signal_us = signal_start.elapsed().as_secs_f64() * 1_000_000.0;
                    shared.shadow_stats.push_signal_us(signal_us).await;
                    metrics::histogram!("latency.signal_us").record(signal_us);
                    let _ = bus.publish(EngineEvent::Signal(signal.clone()));

                    let cfg = shared.strategy_cfg.read().await.clone();
                    let tox_cfg = shared.toxicity_cfg.read().await.clone();
                    let quote_start = Instant::now();
                    let inventory = market_inventory
                        .entry(book.market_id.clone())
                        .or_insert_with(|| inventory_for_market(&portfolio, &book.market_id))
                        .clone();
                    let pending_market_exposure =
                        execution.open_order_notional_for_market(&book.market_id);
                    let pending_total_exposure = execution.open_order_notional_total();

                    let (
                        tox_features,
                        tox_decision,
                        market_score,
                        pending_exposure,
                        no_quote_rate,
                        symbol_missing_rate,
                        markout_samples,
                        markout_10s_p50,
                        markout_10s_p25,
                        active_by_rank,
                    ) = {
                        let mut states = shared.tox_state.write().await;
                        let (
                            features,
                            decision,
                            score,
                            pending_exposure,
                            no_quote_rate,
                            symbol_missing_rate,
                            markout_samples,
                            markout_10s_p50,
                            markout_10s_p25,
                        ) = {
                            let st = states.entry(book.market_id.clone()).or_default();
                            st.attempted = st.attempted.saturating_add(1);
                            st.symbol = symbol.clone();

                            let features = build_toxic_features(
                                &book,
                                &symbol,
                                feed_in_ms,
                                signal.fair_yes,
                                st,
                            );
                            let mut decision = evaluate_toxicity(&features, &tox_cfg);
                            let score_scale = (tox_cfg.k_spread / 1.5).clamp(0.25, 4.0);
                            decision.tox_score = (decision.tox_score * score_scale).clamp(0.0, 1.0);
                            decision.regime = if decision.tox_score >= tox_cfg.caution_threshold {
                                ToxicRegime::Danger
                            } else if decision.tox_score >= tox_cfg.safe_threshold {
                                ToxicRegime::Caution
                            } else {
                                ToxicRegime::Safe
                            };
                            let now_ms = Utc::now().timestamp_millis();

                            if now_ms < st.cooldown_until_ms {
                                decision.regime = ToxicRegime::Danger;
                                decision.reason_codes.push("cooldown_active".to_string());
                            }
                            if matches!(decision.regime, ToxicRegime::Danger) {
                                let cool = cooldown_secs_for_score(decision.tox_score, &tox_cfg);
                                st.cooldown_until_ms =
                                    st.cooldown_until_ms.max(now_ms + (cool as i64) * 1_000);
                            }
                            let markout_samples = st
                                .markout_1s
                                .len()
                                .max(st.markout_5s.len())
                                .max(st.markout_10s.len());
                            if markout_samples < 20 {
                                decision.tox_score =
                                    decision.tox_score.min(tox_cfg.safe_threshold * 0.8);
                                decision.regime = ToxicRegime::Safe;
                                decision
                                    .reason_codes
                                    .push("warmup_samples_lt_20".to_string());
                            }
                            st.last_tox_score = decision.tox_score;
                            st.last_regime = decision.regime.clone();
                            let score =
                                compute_market_score(st, decision.tox_score, markout_samples);
                            let markout_10s_p50 = percentile(&st.markout_10s, 0.50).unwrap_or(0.0);
                            let markout_10s_p25 = percentile(&st.markout_10s, 0.25).unwrap_or(0.0);
                            let pending_exposure = pending_market_exposure;
                            let attempted = st.attempted.max(1);
                            let no_quote_rate = st.no_quote as f64 / attempted as f64;
                            let symbol_missing_rate = st.symbol_missing as f64 / attempted as f64;
                            (
                                features,
                                decision,
                                score,
                                pending_exposure,
                                no_quote_rate,
                                symbol_missing_rate,
                                markout_samples,
                                markout_10s_p50,
                                markout_10s_p25,
                            )
                        };
                        let active_by_rank = is_market_in_top_n(
                            &states,
                            &book.market_id,
                            tox_cfg.active_top_n_markets,
                        );
                        (
                            features,
                            decision,
                            score,
                            pending_exposure,
                            no_quote_rate,
                            symbol_missing_rate,
                            markout_samples,
                            markout_10s_p50,
                            markout_10s_p25,
                            active_by_rank,
                        )
                    };
                    let spread_yes = (book.ask_yes - book.bid_yes).max(0.0);
                    let effective_min_edge_bps = adaptive_min_edge_bps(
                        cfg.min_edge_bps,
                        tox_decision.tox_score,
                        markout_samples,
                        no_quote_rate,
                        markout_10s_p50,
                        markout_10s_p25,
                    );
                    let effective_max_spread = adaptive_max_spread(
                        cfg.max_spread,
                        tox_decision.tox_score,
                        markout_samples,
                    );
                    let queue_fill_proxy =
                        estimate_queue_fill_proxy(tox_decision.tox_score, spread_yes, feed_in_ms);

                    if spread_yes > effective_max_spread {
                        {
                            let mut states = shared.tox_state.write().await;
                            let st = states.entry(book.market_id.clone()).or_default();
                            st.no_quote = st.no_quote.saturating_add(1);
                        }
                        shared
                            .shadow_stats
                            .mark_blocked_with_reason("no_quote_spread")
                            .await;
                        continue;
                    }
                    if signal.confidence <= 0.0 {
                        {
                            let mut states = shared.tox_state.write().await;
                            let st = states.entry(book.market_id.clone()).or_default();
                            st.no_quote = st.no_quote.saturating_add(1);
                        }
                        shared
                            .shadow_stats
                            .mark_blocked_with_reason("no_quote_confidence")
                            .await;
                        continue;
                    }

                    let _ = bus.publish(EngineEvent::ToxicFeatures(tox_features.clone()));
                    let _ = bus.publish(EngineEvent::ToxicDecision(tox_decision.clone()));
                    let ingest_seq_features = next_normalized_ingest_seq();
                    append_jsonl(
                        &dataset_path("normalized", "tox_features.jsonl"),
                        &serde_json::json!({
                            "ts_ms": Utc::now().timestamp_millis(),
                            "source_seq": book.ts_ms.max(0) as u64,
                            "ingest_seq": ingest_seq_features,
                            "features": tox_features
                        }),
                    );
                    let ingest_seq_decisions = next_normalized_ingest_seq();
                    append_jsonl(
                        &dataset_path("normalized", "tox_decisions.jsonl"),
                        &serde_json::json!({
                            "ts_ms": Utc::now().timestamp_millis(),
                            "source_seq": book.ts_ms.max(0) as u64,
                            "ingest_seq": ingest_seq_decisions,
                            "decision": tox_decision
                        }),
                    );
                    let market_health = MarketHealth {
                        market_id: book.market_id.clone(),
                        symbol: symbol.clone(),
                        symbol_missing_rate,
                        no_quote_rate,
                        pending_exposure,
                        queue_fill_proxy,
                        ts_ns: now_ns(),
                    };
                    let ingest_seq_health = next_normalized_ingest_seq();
                    append_jsonl(
                        &dataset_path("normalized", "market_health.jsonl"),
                        &serde_json::json!({
                            "ts_ms": Utc::now().timestamp_millis(),
                            "source_seq": book.ts_ms.max(0) as u64,
                            "ingest_seq": ingest_seq_health,
                            "health": market_health
                        }),
                    );

                    if market_score < tox_cfg.min_market_score {
                        shared
                            .shadow_stats
                            .mark_blocked_with_reason("market_score_low")
                            .await;
                        continue;
                    }
                    if !active_by_rank {
                        shared
                            .shadow_stats
                            .mark_blocked_with_reason("market_rank_blocked")
                            .await;
                        continue;
                    }

                    let mut effective_cfg = cfg.clone();
                    effective_cfg.min_edge_bps = effective_min_edge_bps;
                    let policy = MakerQuotePolicy::new(effective_cfg);
                    let mut intents =
                        policy.build_quotes_with_toxicity(&signal, &inventory, &tox_decision);
                    if !intents.is_empty() {
                        let markout_dispersion = (markout_10s_p50 - markout_10s_p25).abs();
                        let var_penalty = (cfg.variance_penalty_lambda
                            * (markout_dispersion / 200.0).clamp(0.0, 1.0))
                        .clamp(0.0, 0.90);
                        let kelly_scale =
                            (cfg.capital_fraction_kelly * signal.confidence * (1.0 - var_penalty))
                                .clamp(0.05, 1.0);
                        for intent in &mut intents {
                            intent.size = (intent.size * kelly_scale).max(0.01);
                        }
                    }
                    if !intents.is_empty() {
                        shared.shadow_stats.mark_quoted(intents.len() as u64);
                    }
                    let quote_us = quote_start.elapsed().as_secs_f64() * 1_000_000.0;
                    shared.shadow_stats.push_quote_us(quote_us).await;
                    metrics::histogram!("latency.quote_us").record(quote_us);

                    if intents.is_empty() {
                        {
                            let mut states = shared.tox_state.write().await;
                            let st = states.entry(book.market_id.clone()).or_default();
                            st.no_quote = st.no_quote.saturating_add(1);
                        }
                        let edge_blocked = signal.edge_bps_bid < effective_min_edge_bps
                            && signal.edge_bps_ask < effective_min_edge_bps;
                        if edge_blocked {
                            shared
                                .shadow_stats
                                .mark_blocked_with_reason("no_quote_edge")
                                .await;
                        } else {
                            shared
                                .shadow_stats
                                .mark_blocked_with_reason("no_quote_policy")
                                .await;
                        }
                        continue;
                    }

                    let fee_bps = get_fee_rate_bps_cached(&shared, &book.market_id).await;
                    let drawdown = portfolio.snapshot().max_drawdown_pct;

                    for mut intent in intents.drain(..) {
                        let intent_decision_start = Instant::now();
                        shared.shadow_stats.mark_attempted();

                        let risk_start = Instant::now();
                        let ctx = RiskContext {
                            market_id: intent.market_id.clone(),
                            symbol: symbol.clone(),
                            order_count: execution.open_orders_count(),
                            proposed_size: intent.size,
                            market_notional: inventory.exposure_notional + pending_market_exposure,
                            asset_notional: inventory.exposure_notional + pending_total_exposure,
                            drawdown_pct: drawdown,
                        };
                        let limits = shared.risk_limits.read().await.clone();
                        let risk = DefaultRiskManager::new(limits);
                        let decision = risk.evaluate(&ctx);
                        let risk_us = risk_start.elapsed().as_secs_f64() * 1_000_000.0;
                        shared.shadow_stats.push_risk_us(risk_us).await;
                        metrics::histogram!("latency.risk_us").record(risk_us);

                        if !decision.allow {
                            shared
                                .shadow_stats
                                .mark_blocked_with_reason(&format!("risk:{}", decision.reason))
                                .await;
                            metrics::counter!("strategy.blocked").increment(1);
                            continue;
                        }
                        if decision.capped_size <= 0.0 {
                            shared
                                .shadow_stats
                                .mark_blocked_with_reason("risk_capped_zero")
                                .await;
                            metrics::counter!("strategy.blocked").increment(1);
                            continue;
                        }
                        intent.size = intent.size.min(decision.capped_size);

                        let edge_gross = edge_for_side(&signal, &intent.side);
                        let rebate_est_bps =
                            get_rebate_bps_cached(&shared, &book.market_id, fee_bps).await;
                        let edge_net = edge_gross - fee_bps + rebate_est_bps;
                        if edge_net < effective_min_edge_bps {
                            shared
                                .shadow_stats
                                .mark_blocked_with_reason("edge_below_threshold")
                                .await;
                            continue;
                        }

                        let mut force_taker = false;
                        if should_force_taker(&cfg, &tox_decision, edge_net, signal.confidence) {
                            let aggressive_price = aggressive_price_for_side(&book, &intent.side);
                            let passive_price = intent.price.max(1e-6);
                            let price_move_bps =
                                ((aggressive_price - intent.price).abs() / passive_price) * 10_000.0;
                            if price_move_bps <= cfg.taker_max_slippage_bps.max(0.0) {
                                intent.price = aggressive_price;
                                intent.ttl_ms = intent.ttl_ms.min(150);
                                force_taker = true;
                            } else {
                                shared
                                    .shadow_stats
                                    .mark_blocked_with_reason("taker_slippage_budget")
                                    .await;
                                continue;
                            }
                        }
                        let per_market_rps = (shared.rate_limit_rps / 8.0).max(0.5);
                        let market_bucket =
                            market_rate_budget.entry(book.market_id.clone()).or_insert_with(|| {
                                TokenBucket::new(per_market_rps, (per_market_rps * 2.0).max(1.0))
                            });
                        if !global_rate_budget.try_take(1.0) {
                            shared
                                .shadow_stats
                                .mark_blocked_with_reason("rate_budget_global")
                                .await;
                            continue;
                        }
                        if !market_bucket.try_take(1.0) {
                            shared
                                .shadow_stats
                                .mark_blocked_with_reason("rate_budget_market")
                                .await;
                            continue;
                        }
                        shared.shadow_stats.mark_eligible();

                        let decision_compute_ms =
                            intent_decision_start.elapsed().as_secs_f64() * 1_000.0;
                        let tick_to_decision_ms = latency_sample.local_backlog_ms + decision_compute_ms;
                        let place_start = Instant::now();
                        let execution_style = if force_taker {
                            ExecutionStyle::Taker
                        } else {
                            classify_execution_style(&book, &intent)
                        };
                        let tif = match execution_style {
                            ExecutionStyle::Maker => OrderTimeInForce::PostOnly,
                            ExecutionStyle::Taker | ExecutionStyle::Arb => OrderTimeInForce::Fak,
                        };
                        let v2_intent = OrderIntentV2 {
                            market_id: intent.market_id.clone(),
                            side: intent.side.clone(),
                            price: intent.price,
                            size: intent.size,
                            ttl_ms: intent.ttl_ms,
                            style: execution_style.clone(),
                            tif,
                            max_slippage_bps: cfg.taker_max_slippage_bps,
                            fee_rate_bps: fee_bps,
                            expected_edge_net_bps: edge_net,
                            hold_to_resolution: false,
                        };
                        match execution.place_order_v2(v2_intent).await {
                            Ok(ack_v2) if ack_v2.accepted => {
                                let accepted_size = ack_v2.accepted_size.max(0.0).min(intent.size);
                                if accepted_size <= 0.0 {
                                    shared
                                        .shadow_stats
                                        .mark_blocked_with_reason("exchange_reject_zero_size")
                                        .await;
                                    continue;
                                }
                                intent.size = accepted_size;
                                shared.shadow_stats.mark_executed();
                                if execution.is_live() && execution_style == ExecutionStyle::Maker {
                                    maybe_spawn_scoring_refresh(
                                        &shared,
                                        &book.market_id,
                                        &ack_v2.order_id,
                                        fee_bps,
                                        Instant::now(),
                                        Duration::from_secs(2),
                                    )
                                    .await;
                                }
                                let ack_only_ms = if ack_v2.exchange_latency_ms > 0.0 {
                                    ack_v2.exchange_latency_ms
                                } else {
                                    place_start.elapsed().as_secs_f64() * 1_000.0
                                };
                                let tick_to_ack_ms = tick_to_decision_ms + ack_only_ms;
                                shared
                                    .shadow_stats
                                    .push_decision_compute_ms(decision_compute_ms)
                                    .await;
                                shared
                                    .shadow_stats
                                    .push_tick_to_decision_ms(tick_to_decision_ms)
                                    .await;
                                shared.shadow_stats.push_ack_only_ms(ack_only_ms).await;
                                shared
                                    .shadow_stats
                                    .push_tick_to_ack_ms(tick_to_ack_ms)
                                    .await;
                                metrics::histogram!("latency.tick_to_decision_ms")
                                    .record(tick_to_decision_ms);
                                metrics::histogram!("latency.decision_compute_ms")
                                    .record(decision_compute_ms);
                                metrics::histogram!("latency.decision_queue_wait_ms")
                                    .record(latency_sample.local_backlog_ms);
                                metrics::histogram!("latency.ack_only_ms").record(ack_only_ms);
                                metrics::histogram!("latency.tick_to_ack_ms")
                                    .record(tick_to_ack_ms);

                                let ack = OrderAck {
                                    order_id: ack_v2.order_id,
                                    market_id: ack_v2.market_id,
                                    accepted: true,
                                    ts_ms: ack_v2.ts_ms,
                                };
                                let _ = bus.publish(EngineEvent::OrderAck(ack.clone()));
                                shadow.register_order(&ack, intent.clone());

                                for delay_ms in [5_u64, 10_u64, 25_u64] {
                                    let shot = ShadowShot {
                                        shot_id: new_id(),
                                        market_id: intent.market_id.clone(),
                                        symbol: symbol.clone(),
                                        side: intent.side.clone(),
                                        execution_style: execution_style.clone(),
                                        // Use taker top-of-book only for "opportunity survival" probing.
                                        // Keep intended_price as maker entry for markout/PnL attribution.
                                        survival_probe_price: aggressive_price_for_side(
                                            &book,
                                            &intent.side,
                                        ),
                                        intended_price: intent.price,
                                        size: intent.size,
                                        edge_gross_bps: edge_gross,
                                        edge_net_bps: edge_net,
                                        fee_paid_bps: fee_bps,
                                        rebate_est_bps,
                                        delay_ms,
                                        t0_ns: now_ns(),
                                        min_edge_bps: effective_min_edge_bps,
                                        tox_score: tox_decision.tox_score,
                                        ttl_ms: intent.ttl_ms,
                                    };
                                    shared.shadow_stats.push_shot(shot.clone()).await;
                                    let _ = bus.publish(EngineEvent::ShadowShot(shot.clone()));
                                    spawn_shadow_outcome_task(shared.clone(), bus.clone(), shot);
                                }
                            }
                            Ok(ack_v2) => {
                                let reject_code = ack_v2
                                    .reject_code
                                    .as_deref()
                                    .map(normalize_reject_code)
                                    .unwrap_or_else(|| "unknown".to_string());
                                shared
                                    .shadow_stats
                                    .mark_blocked_with_reason(&format!("exchange_reject_{reject_code}"))
                                    .await;
                                metrics::counter!("execution.place_rejected").increment(1);
                            }
                            Err(err) => {
                                let reason = classify_execution_error_reason(&err);
                                shared
                                    .shadow_stats
                                    .mark_blocked_with_reason(reason)
                                    .await;
                                tracing::warn!(?err, "place_order failed");
                                metrics::counter!("execution.place_error").increment(1);
                            }
                        }
                    }

                    market_inventory.insert(
                        book.market_id.clone(),
                        inventory_for_market(&portfolio, &book.market_id),
                    );
                }
                EngineEvent::Control(ControlCommand::Pause) => {
                    *paused.write().await = true;
                }
                EngineEvent::Control(ControlCommand::Resume) => {
                    *paused.write().await = false;
                }
                EngineEvent::Control(ControlCommand::Flatten) => {
                    if let Err(err) = execution.flatten_all().await {
                        tracing::warn!(?err, "flatten from control event failed");
                    }
                }
                _ => {}
            }
        }
    });
}

fn spawn_shadow_outcome_task(
    shared: Arc<EngineShared>,
    bus: RingBus<EngineEvent>,
    shot: ShadowShot,
) {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(shot.delay_ms)).await;

        let book = shared
            .latest_books
            .read()
            .await
            .get(&shot.market_id)
            .cloned();
        let latency_ms = ((now_ns() - shot.t0_ns).max(0) as f64) / 1_000_000.0;
        shared.shadow_stats.push_shadow_fill_ms(latency_ms).await;
        metrics::histogram!("latency.shadow_fill_ms").record(latency_ms);

        let (
            fillable,
            slippage_bps,
            queue_fill_prob,
            attribution,
            pnl_1s_bps,
            pnl_5s_bps,
            pnl_10s_bps,
        ) = if let Some(book) = book {
            let (fillable, slippage_bps, queue_fill_prob) =
                evaluate_fillable(&shot, &book, latency_ms);
            if fillable {
                let p1 = pnl_after_horizon(&shared, &shot, Duration::from_secs(1)).await;
                let p5 = pnl_after_horizon(&shared, &shot, Duration::from_secs(5)).await;
                let p10 = pnl_after_horizon(&shared, &shot, Duration::from_secs(10)).await;
                let attribution = classify_filled_outcome(shot.edge_net_bps, p10, slippage_bps);
                (
                    true,
                    slippage_bps,
                    queue_fill_prob,
                    attribution,
                    p1,
                    p5,
                    p10,
                )
            } else {
                let attribution =
                    classify_unfilled_outcome(&book, latency_ms, shot.delay_ms, queue_fill_prob);
                (
                    false,
                    slippage_bps,
                    queue_fill_prob,
                    attribution,
                    None,
                    None,
                    None,
                )
            }
        } else {
            (
                false,
                None,
                0.0,
                EdgeAttribution::SignalLag,
                None,
                None,
                None,
            )
        };
        let net_markout_1s_bps = net_markout(pnl_1s_bps, &shot);
        let net_markout_5s_bps = net_markout(pnl_5s_bps, &shot);
        let net_markout_10s_bps = net_markout(pnl_10s_bps, &shot);
        let entry_notional_usdc = estimate_entry_notional_usdc(&shot);
        let net_markout_10s_usdc = bps_to_usdc(net_markout_10s_bps, entry_notional_usdc);
        let roi_notional_10s_bps = roi_bps_from_usdc(net_markout_10s_usdc, entry_notional_usdc);

        let outcome = ShadowOutcome {
            shot_id: shot.shot_id.clone(),
            market_id: shot.market_id.clone(),
            symbol: shot.symbol.clone(),
            side: shot.side.clone(),
            delay_ms: shot.delay_ms,
            fillable,
            execution_style: shot.execution_style.clone(),
            slippage_bps,
            pnl_1s_bps,
            pnl_5s_bps,
            pnl_10s_bps,
            net_markout_1s_bps,
            net_markout_5s_bps,
            net_markout_10s_bps,
            entry_notional_usdc,
            net_markout_10s_usdc,
            roi_notional_10s_bps,
            queue_fill_prob,
            is_stale_tick: false,
            is_outlier: false,
            robust_weight: 1.0,
            attribution,
            ts_ns: now_ns(),
        };
        shared.shadow_stats.push_outcome(outcome.clone()).await;
        update_toxic_state_from_outcome(&shared, &outcome).await;
        if shot.delay_ms == 10 {
            let eval = QuoteEval {
                market_id: shot.market_id.clone(),
                symbol: shot.symbol.clone(),
                survival_10ms: if outcome.fillable { 1.0 } else { 0.0 },
                maker_markout_10s_bps: outcome.net_markout_10s_bps.unwrap_or(0.0),
                adverse_flag: outcome.net_markout_10s_bps.unwrap_or(0.0) < 0.0,
                ts_ns: now_ns(),
            };
            let ingest_seq = next_normalized_ingest_seq();
            append_jsonl(
                &dataset_path("normalized", "quote_eval.jsonl"),
                &serde_json::json!({
                    "ts_ms": Utc::now().timestamp_millis(),
                    "source_seq": shot.t0_ns.max(0) as u64,
                    "ingest_seq": ingest_seq,
                    "eval": eval
                }),
            );
            let _ = bus.publish(EngineEvent::QuoteEval(eval));
        }
        let _ = bus.publish(EngineEvent::ShadowOutcome(outcome));
    });
}

async fn refresh_market_symbol_map(shared: &EngineShared) {
    let discovery = MarketDiscovery::new(DiscoveryConfig {
        symbols: (*shared.universe_symbols).clone(),
        ..DiscoveryConfig::default()
    });
    match discovery.discover().await {
        Ok(markets) => {
            let mut market_map = HashMap::new();
            let mut token_map = HashMap::new();
            for m in markets {
                market_map.insert(m.market_id.clone(), m.symbol.clone());
                if let Some(t) = m.token_id_yes {
                    token_map.insert(t, m.symbol.clone());
                }
                if let Some(t) = m.token_id_no {
                    token_map.insert(t, m.symbol.clone());
                }
            }
            {
                let mut map = shared.market_to_symbol.write().await;
                *map = market_map;
            }
            {
                let mut map = shared.token_to_symbol.write().await;
                *map = token_map;
            }
        }
        Err(err) => {
            tracing::warn!(?err, "market discovery refresh failed");
        }
    }
}

async fn pick_market_symbol(shared: &EngineShared, book: &BookTop) -> Option<String> {
    if let Some(v) = shared
        .market_to_symbol
        .read()
        .await
        .get(&book.market_id)
        .cloned()
    {
        return Some(v);
    }
    let token_map = shared.token_to_symbol.read().await;
    token_map
        .get(&book.token_id_yes)
        .cloned()
        .or_else(|| token_map.get(&book.token_id_no).cloned())
}

async fn market_is_tracked(shared: &EngineShared, book: &BookTop) -> bool {
    if shared
        .market_to_symbol
        .read()
        .await
        .contains_key(&book.market_id)
    {
        return true;
    }
    let token_map = shared.token_to_symbol.read().await;
    token_map.contains_key(&book.token_id_yes) || token_map.contains_key(&book.token_id_no)
}

fn inventory_for_market(portfolio: &PortfolioBook, market_id: &str) -> InventoryState {
    let positions = portfolio.positions();
    if let Some(pos) = positions.get(market_id) {
        InventoryState {
            market_id: market_id.to_string(),
            net_yes: pos.yes,
            net_no: pos.no,
            exposure_notional: pos.yes.abs() + pos.no.abs(),
        }
    } else {
        InventoryState {
            market_id: market_id.to_string(),
            net_yes: 0.0,
            net_no: 0.0,
            exposure_notional: 0.0,
        }
    }
}
fn build_toxic_features(
    book: &BookTop,
    symbol: &str,
    stale_ms: f64,
    fair_yes: f64,
    state: &MarketToxicState,
) -> ToxicFeatures {
    let mid_yes = ((book.bid_yes + book.ask_yes) * 0.5).max(0.0001);
    let spread_bps = ((book.ask_yes - book.bid_yes).max(0.0) / mid_yes) * 10_000.0;
    let microprice_drift = fair_yes - mid_yes;
    let imbalance_den = (book.bid_yes + book.ask_yes + book.bid_no + book.ask_no).abs();
    let imbalance = if imbalance_den <= 1e-12 {
        0.0
    } else {
        ((book.bid_yes + book.ask_no) - (book.ask_yes + book.bid_no)) / imbalance_den
    };
    let attempted = state.attempted.max(1);
    let cancel_burst = (state.no_quote as f64 / attempted as f64).clamp(0.0, 1.0);

    ToxicFeatures {
        market_id: book.market_id.clone(),
        symbol: symbol.to_string(),
        markout_1s: percentile(&state.markout_1s, 0.50).unwrap_or(0.0),
        markout_5s: percentile(&state.markout_5s, 0.50).unwrap_or(0.0),
        markout_10s: percentile(&state.markout_10s, 0.50).unwrap_or(0.0),
        spread_bps,
        microprice_drift,
        stale_ms,
        imbalance,
        cancel_burst,
        ts_ns: now_ns(),
    }
}

fn evaluate_toxicity(features: &ToxicFeatures, cfg: &ToxicityConfig) -> ToxicDecision {
    let neg_markout_1s = (-features.markout_1s).max(0.0) / 20.0;
    let neg_markout_5s = (-features.markout_5s).max(0.0) / 20.0;
    let neg_markout_10s = (-features.markout_10s).max(0.0) / 20.0;
    let spread_z = features.spread_bps / 50.0;
    let microprice_drift_z = (features.microprice_drift.abs() * 10_000.0) / 20.0;
    let stale_z = features.stale_ms / 1_500.0;
    let raw = cfg.w1 * neg_markout_1s
        + cfg.w2 * neg_markout_5s
        + cfg.w3 * neg_markout_10s
        + cfg.w4 * spread_z
        + cfg.w5 * microprice_drift_z
        + cfg.w6 * stale_z;
    let markout_1s_danger = features.markout_1s <= cfg.markout_1s_danger_bps;
    let markout_5s_danger = features.markout_5s <= cfg.markout_5s_danger_bps;
    let markout_10s_danger = features.markout_10s <= cfg.markout_10s_danger_bps;
    let markout_1s_caution = features.markout_1s <= cfg.markout_1s_caution_bps;
    let markout_5s_caution = features.markout_5s <= cfg.markout_5s_caution_bps;
    let markout_10s_caution = features.markout_10s <= cfg.markout_10s_caution_bps;

    let mut horizon_boost = 0.0;
    if markout_1s_danger {
        horizon_boost += 0.30;
    } else if markout_1s_caution {
        horizon_boost += 0.15;
    }
    if markout_5s_danger {
        horizon_boost += 0.25;
    } else if markout_5s_caution {
        horizon_boost += 0.12;
    }
    if markout_10s_danger {
        horizon_boost += 0.20;
    } else if markout_10s_caution {
        horizon_boost += 0.10;
    }
    let tox_score = (sigmoid(raw) + horizon_boost).clamp(0.0, 1.0);

    let mut reasons = Vec::new();
    if markout_1s_danger {
        reasons.push("markout_1s_danger".to_string());
    } else if markout_1s_caution {
        reasons.push("markout_1s_caution".to_string());
    } else if features.markout_1s < 0.0 {
        reasons.push("markout_1s_negative".to_string());
    }
    if markout_5s_danger {
        reasons.push("markout_5s_danger".to_string());
    } else if markout_5s_caution {
        reasons.push("markout_5s_caution".to_string());
    } else if features.markout_5s < 0.0 {
        reasons.push("markout_5s_negative".to_string());
    }
    if markout_10s_danger {
        reasons.push("markout_10s_danger".to_string());
    } else if markout_10s_caution {
        reasons.push("markout_10s_caution".to_string());
    } else if features.markout_10s < 0.0 {
        reasons.push("markout_10s_negative".to_string());
    }
    if features.spread_bps > 60.0 {
        reasons.push("spread_wide".to_string());
    }
    if features.stale_ms > 1_500.0 {
        reasons.push("stale_feed".to_string());
    }
    if reasons.is_empty() {
        reasons.push("normal".to_string());
    }

    let regime = if markout_1s_danger || markout_5s_danger || markout_10s_danger {
        ToxicRegime::Danger
    } else if markout_1s_caution || markout_5s_caution || markout_10s_caution {
        ToxicRegime::Caution
    } else if tox_score >= cfg.caution_threshold {
        ToxicRegime::Danger
    } else if tox_score >= cfg.safe_threshold {
        ToxicRegime::Caution
    } else {
        ToxicRegime::Safe
    };

    ToxicDecision {
        market_id: features.market_id.clone(),
        symbol: features.symbol.clone(),
        tox_score,
        regime,
        reason_codes: reasons,
        ts_ns: now_ns(),
    }
}

fn compute_market_score(state: &MarketToxicState, tox_score: f64, markout_samples: usize) -> f64 {
    let attempted = state.attempted.max(1);
    let no_quote_rate = state.no_quote as f64 / attempted as f64;
    let symbol_missing_rate = state.symbol_missing as f64 / attempted as f64;
    let markout_10s = percentile(&state.markout_10s, 0.50).unwrap_or(0.0);
    if markout_samples < 20 {
        let warmup_score = 80.0 - no_quote_rate * 6.0 - symbol_missing_rate * 6.0;
        return warmup_score.clamp(45.0, 100.0);
    }
    let score = 70.0 + (markout_10s * 1.5).clamp(-30.0, 30.0)
        - no_quote_rate * 25.0
        - symbol_missing_rate * 30.0
        - tox_score * 20.0;
    score.clamp(0.0, 100.0)
}

fn adaptive_min_edge_bps(
    base_min_edge_bps: f64,
    tox_score: f64,
    markout_samples: usize,
    no_quote_rate: f64,
    markout_10s_p50: f64,
    markout_10s_p25: f64,
) -> f64 {
    if markout_samples < 20 {
        return (base_min_edge_bps * 0.5).max(1.0);
    }
    let mut out = base_min_edge_bps * (1.0 + tox_score * 0.6);
    if markout_10s_p50 < 0.0 {
        out += (-markout_10s_p50) * 0.50;
    }
    if markout_10s_p25 < 0.0 {
        out += (-markout_10s_p25) * 0.35;
    }
    if no_quote_rate > 0.95 {
        out *= 0.9;
    }
    out.clamp(1.0, base_min_edge_bps * 2.5)
}

fn adaptive_max_spread(base_max_spread: f64, tox_score: f64, markout_samples: usize) -> f64 {
    if markout_samples < 20 {
        return (base_max_spread * 1.2).clamp(0.003, 0.08);
    }
    (base_max_spread * (1.0 - tox_score * 0.35)).clamp(0.002, base_max_spread)
}

fn should_force_taker(
    cfg: &MakerConfig,
    tox: &ToxicDecision,
    edge_net_bps: f64,
    confidence: f64,
) -> bool {
    if edge_net_bps < cfg.taker_trigger_bps.max(0.0) {
        return false;
    }
    if matches!(tox.regime, ToxicRegime::Danger) {
        return false;
    }

    let profile = cfg.market_tier_profile.to_ascii_lowercase();
    let aggressive_profile = profile.contains("taker")
        || profile.contains("aggressive")
        || profile.contains("latency");

    if matches!(tox.regime, ToxicRegime::Caution) && !aggressive_profile {
        return false;
    }

    if confidence < 0.55 && !aggressive_profile {
        return false;
    }

    true
}

fn estimate_queue_fill_proxy(tox_score: f64, spread_yes: f64, feed_in_ms: f64) -> f64 {
    let spread_pen = (spread_yes / 0.03).clamp(0.0, 1.0);
    let latency_pen = (feed_in_ms / 800.0).clamp(0.0, 1.0);
    (1.0 - (tox_score * 0.45 + spread_pen * 0.35 + latency_pen * 0.20)).clamp(0.0, 1.0)
}

fn estimate_queue_fill_prob(shot: &ShadowShot, book: &BookTop, latency_ms: f64) -> f64 {
    let spread = match shot.side {
        OrderSide::BuyYes | OrderSide::SellYes => (book.ask_yes - book.bid_yes).max(0.0),
        OrderSide::BuyNo | OrderSide::SellNo => (book.ask_no - book.bid_no).max(0.0),
    };
    let spread_pen = (spread / 0.03).clamp(0.0, 1.0);
    let delay_pen = (shot.delay_ms as f64 / 25.0).clamp(0.0, 1.0);
    let latency_pen = (latency_ms / 100.0).clamp(0.0, 1.0);
    (1.0 - (shot.tox_score * 0.35 + spread_pen * 0.30 + delay_pen * 0.20 + latency_pen * 0.15))
        .clamp(0.0, 1.0)
}

fn is_market_in_top_n(
    states: &HashMap<String, MarketToxicState>,
    market_id: &str,
    top_n: usize,
) -> bool {
    if top_n == 0 {
        return true;
    }
    let mut ranked = states
        .iter()
        .map(|(id, st)| {
            let samples = st
                .markout_1s
                .len()
                .max(st.markout_5s.len())
                .max(st.markout_10s.len());
            (
                id.clone(),
                compute_market_score(st, st.last_tox_score, samples),
            )
        })
        .collect::<Vec<_>>();
    if ranked.is_empty() {
        return true;
    }
    ranked.sort_by(|a, b| b.1.total_cmp(&a.1));
    ranked
        .into_iter()
        .take(top_n.max(1))
        .any(|(id, _)| id == market_id)
}

fn cooldown_secs_for_score(tox_score: f64, cfg: &ToxicityConfig) -> u64 {
    let t = tox_score.clamp(0.0, 1.0);
    ((cfg.cooldown_min_sec as f64)
        + ((cfg.cooldown_max_sec as f64) - (cfg.cooldown_min_sec as f64)) * t)
        .round() as u64
}

fn net_markout(markout_bps: Option<f64>, shot: &ShadowShot) -> Option<f64> {
    markout_bps.map(|v| v - shot.fee_paid_bps + shot.rebate_est_bps)
}

fn estimate_entry_notional_usdc(shot: &ShadowShot) -> f64 {
    (shot.intended_price.max(0.0) * shot.size.max(0.0)).max(0.0)
}

fn bps_to_usdc(bps: Option<f64>, notional_usdc: f64) -> Option<f64> {
    if notional_usdc <= 0.0 {
        return None;
    }
    bps.map(|v| (v / 10_000.0) * notional_usdc)
}

fn roi_bps_from_usdc(markout_usdc: Option<f64>, notional_usdc: f64) -> Option<f64> {
    if notional_usdc <= 0.0 {
        return None;
    }
    markout_usdc.map(|v| (v / notional_usdc) * 10_000.0)
}

async fn update_toxic_state_from_outcome(shared: &EngineShared, outcome: &ShadowOutcome) {
    if !outcome.fillable || outcome.delay_ms != 10 {
        return;
    }
    let mut states = shared.tox_state.write().await;
    let st = states.entry(outcome.market_id.clone()).or_default();
    if let Some(v) = outcome.net_markout_1s_bps.or(outcome.pnl_1s_bps) {
        push_rolling(&mut st.markout_1s, v, 2048);
    }
    if let Some(v) = outcome.net_markout_5s_bps.or(outcome.pnl_5s_bps) {
        push_rolling(&mut st.markout_5s, v, 2048);
    }
    if let Some(v) = outcome.net_markout_10s_bps.or(outcome.pnl_10s_bps) {
        push_rolling(&mut st.markout_10s, v, 2048);
    }
}

fn push_rolling(dst: &mut Vec<f64>, value: f64, cap: usize) {
    dst.push(value);
    if dst.len() > cap {
        let drop_n = dst.len() - cap;
        dst.drain(0..drop_n);
    }
}

fn sigmoid(x: f64) -> f64 {
    if x >= 0.0 {
        1.0 / (1.0 + (-x).exp())
    } else {
        let ex = x.exp();
        ex / (1.0 + ex)
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct FeedLatencySample {
    feed_in_ms: f64,
    source_latency_ms: f64,
    local_backlog_ms: f64,
}

#[derive(Debug, Clone)]
struct TokenBucket {
    rps: f64,
    burst: f64,
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(rps: f64, burst: f64) -> Self {
        let rps = rps.max(0.1);
        let burst = burst.max(1.0);
        Self {
            rps,
            burst,
            tokens: burst,
            last_refill: Instant::now(),
        }
    }

    fn try_take(&mut self, n: f64) -> bool {
        let now = Instant::now();
        let dt = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;
        self.tokens = (self.tokens + dt * self.rps).clamp(0.0, self.burst);
        if self.tokens >= n {
            self.tokens -= n;
            true
        } else {
            false
        }
    }
}

fn estimate_feed_latency(tick: &RefTick, book: &BookTop) -> FeedLatencySample {
    let now = now_ns();
    let now_ms = now / 1_000_000;
    let tick_source_ms = tick.event_ts_exchange_ms.max(tick.event_ts_ms);
    let tick_ingest_ms = (tick.recv_ts_ms - tick_source_ms).max(0) as f64;
    let book_recv_ms = if book.recv_ts_local_ns > 0 {
        (book.recv_ts_local_ns / 1_000_000).max(0)
    } else {
        now_ms
    };
    let book_ingest_ms = (book_recv_ms - book.ts_ms).max(0) as f64;
    let source_latency_ms = tick_ingest_ms.max(book_ingest_ms);

    let latest_recv_ns = if book.recv_ts_local_ns > 0 {
        tick.recv_ts_local_ns.max(book.recv_ts_local_ns)
    } else {
        tick.recv_ts_local_ns
    };
    let local_backlog_ms = if latest_recv_ns > 0 {
        ((now - latest_recv_ns).max(0) as f64) / 1_000_000.0
    } else {
        (now_ms - tick.recv_ts_ms.max(book.ts_ms)).max(0) as f64
    };

    let feed_in_ms = source_latency_ms + local_backlog_ms;
    FeedLatencySample {
        feed_in_ms,
        source_latency_ms,
        local_backlog_ms,
    }
}

fn ref_source_rank(source: &str) -> u8 {
    match source {
        "binance_ws" => 3,
        "coinbase_ws" => 2,
        "bybit_ws" => 1,
        _ => 0,
    }
}

fn ref_event_ts_ms(tick: &RefTick) -> i64 {
    tick.event_ts_exchange_ms.max(tick.event_ts_ms)
}

fn should_replace_ref_tick(current: &RefTick, next: &RefTick) -> bool {
    let current_event = ref_event_ts_ms(current);
    let next_event = ref_event_ts_ms(next);
    if next_event + 50 < current_event {
        return false;
    }
    if next_event > current_event + 1 {
        return true;
    }
    if next.recv_ts_local_ns > current.recv_ts_local_ns + 1_000_000 {
        return true;
    }
    ref_source_rank(next.source.as_str()) >= ref_source_rank(current.source.as_str())
}

fn insert_latest_tick(latest_ticks: &mut HashMap<String, RefTick>, tick: RefTick) {
    match latest_ticks.get(tick.symbol.as_str()) {
        Some(current) => {
            if should_replace_ref_tick(current, &tick) {
                latest_ticks.insert(tick.symbol.clone(), tick);
            }
        }
        None => {
            latest_ticks.insert(tick.symbol.clone(), tick);
        }
    }
}

async fn get_fee_rate_bps_cached(shared: &EngineShared, market_id: &str) -> f64 {
    const DEFAULT_FEE_BPS: f64 = 2.0;
    const TTL: Duration = Duration::from_secs(60);
    const REFRESH_BACKOFF: Duration = Duration::from_secs(3);

    let now = Instant::now();
    let (cached_fee, needs_refresh) = if let Some(entry) =
        shared.fee_cache.read().await.get(market_id).cloned()
    {
        (
            entry.fee_bps,
            now.duration_since(entry.fetched_at) >= TTL || entry.fee_bps <= 0.0,
        )
    } else {
        (DEFAULT_FEE_BPS, true)
    };

    if needs_refresh {
        maybe_spawn_fee_refresh(shared, market_id, now, REFRESH_BACKOFF).await;
    }

    cached_fee
}

async fn get_rebate_bps_cached(shared: &EngineShared, market_id: &str, fee_bps: f64) -> f64 {
    const TTL: Duration = Duration::from_secs(120);
    let now = Instant::now();
    let maybe = shared.scoring_cache.read().await.get(market_id).cloned();
    match maybe {
        Some(entry) if now.duration_since(entry.fetched_at) <= TTL => {
            entry.rebate_bps_est.clamp(0.0, fee_bps.max(0.0))
        }
        _ => 0.0,
    }
}

async fn maybe_spawn_fee_refresh(
    shared: &EngineShared,
    market_id: &str,
    now: Instant,
    refresh_backoff: Duration,
) {
    {
        let inflight = shared.fee_refresh_inflight.read().await;
        if let Some(last_attempt) = inflight.get(market_id) {
            if now.duration_since(*last_attempt) < refresh_backoff {
                return;
            }
        }
    }

    {
        let mut inflight = shared.fee_refresh_inflight.write().await;
        if let Some(last_attempt) = inflight.get(market_id) {
            if now.duration_since(*last_attempt) < refresh_backoff {
                return;
            }
        }
        inflight.insert(market_id.to_string(), now);
    }

    let market = market_id.to_string();
    let http = shared.http.clone();
    let clob_endpoint = shared.clob_endpoint.clone();
    let fee_cache = shared.fee_cache.clone();
    let inflight = shared.fee_refresh_inflight.clone();
    tokio::spawn(async move {
        if let Some(fee_bps) = fetch_fee_rate_bps(&http, &clob_endpoint, &market).await {
            fee_cache.write().await.insert(
                market.clone(),
                FeeRateEntry {
                    fee_bps,
                    fetched_at: Instant::now(),
                },
            );
        }
        inflight.write().await.remove(&market);
    });
}

async fn maybe_spawn_scoring_refresh(
    shared: &EngineShared,
    market_id: &str,
    order_id: &str,
    fee_bps: f64,
    now: Instant,
    refresh_backoff: Duration,
) {
    if market_id.is_empty() || order_id.is_empty() {
        return;
    }
    {
        let inflight = shared.scoring_refresh_inflight.read().await;
        if let Some(last_attempt) = inflight.get(market_id) {
            if now.duration_since(*last_attempt) < refresh_backoff {
                return;
            }
        }
    }
    {
        let mut inflight = shared.scoring_refresh_inflight.write().await;
        if let Some(last_attempt) = inflight.get(market_id) {
            if now.duration_since(*last_attempt) < refresh_backoff {
                return;
            }
        }
        inflight.insert(market_id.to_string(), now);
    }

    let market = market_id.to_string();
    let order = order_id.to_string();
    let http = shared.http.clone();
    let clob_endpoint = shared.clob_endpoint.clone();
    let scoring_cache = shared.scoring_cache.clone();
    let inflight = shared.scoring_refresh_inflight.clone();
    let rebate_factor = shared.scoring_rebate_factor;
    tokio::spawn(async move {
        if let Some((scoring_ok, raw)) =
            fetch_order_scoring(&http, &clob_endpoint, &order).await
        {
            let mut cache = scoring_cache.write().await;
            let mut entry = cache.get(&market).cloned().unwrap_or(ScoringState {
                scoring_true: 0,
                scoring_total: 0,
                rebate_bps_est: 0.0,
                fetched_at: Instant::now(),
            });
            entry.scoring_total = entry.scoring_total.saturating_add(1);
            if scoring_ok {
                entry.scoring_true = entry.scoring_true.saturating_add(1);
            }
            let hit_ratio = if entry.scoring_total == 0 {
                0.0
            } else {
                (entry.scoring_true as f64 / entry.scoring_total as f64).clamp(0.0, 1.0)
            };
            entry.rebate_bps_est = (fee_bps.max(0.0) * hit_ratio * rebate_factor).clamp(0.0, fee_bps.max(0.0));
            entry.fetched_at = Instant::now();
            let log_row = serde_json::json!({
                "ts_ms": Utc::now().timestamp_millis(),
                "market_id": market,
                "order_id": order,
                "scoring_ok": scoring_ok,
                "scoring_true": entry.scoring_true,
                "scoring_total": entry.scoring_total,
                "hit_ratio": hit_ratio,
                "rebate_bps_est": entry.rebate_bps_est,
                "raw": raw
            });
            cache.insert(market.clone(), entry);
            append_jsonl(
                &dataset_path("normalized", "scoring_feedback.jsonl"),
                &log_row,
            );
        }
        inflight.write().await.remove(&market);
    });
}

async fn fetch_fee_rate_bps(http: &Client, clob_endpoint: &str, market_id: &str) -> Option<f64> {
    let base = clob_endpoint.trim_end_matches('/');
    let endpoints = [
        format!("{base}/fee-rate?market_id={market_id}"),
        format!("{base}/fee-rate?market={market_id}"),
        format!("{base}/fee-rate?token_id={market_id}"),
    ];

    for url in endpoints {
        let Ok(resp) = http.get(&url).send().await else {
            continue;
        };
        let Ok(resp) = resp.error_for_status() else {
            continue;
        };
        let Ok(v) = resp.json::<serde_json::Value>().await else {
            continue;
        };
        let candidate = v
            .get("fee_rate_bps")
            .and_then(value_to_f64)
            .or_else(|| v.get("feeRateBps").and_then(value_to_f64))
            .or_else(|| v.get("makerFeeRateBps").and_then(value_to_f64))
            .or_else(|| v.get("maker_fee_rate_bps").and_then(value_to_f64));
        if candidate.is_some() {
            return candidate;
        }
    }
    None
}

async fn fetch_order_scoring(
    http: &Client,
    clob_endpoint: &str,
    order_id: &str,
) -> Option<(bool, serde_json::Value)> {
    let base = clob_endpoint.trim_end_matches('/');
    let endpoints = [
        format!("{base}/order-scoring?order_id={order_id}"),
        format!("{base}/order-scoring?orderId={order_id}"),
        format!("{base}/orders-scoring?order_id={order_id}"),
    ];
    for url in endpoints {
        let Ok(resp) = http.get(&url).send().await else {
            continue;
        };
        let Ok(resp) = resp.error_for_status() else {
            continue;
        };
        let Ok(value) = resp.json::<serde_json::Value>().await else {
            continue;
        };
        let scoring = value
            .get("scoring")
            .and_then(|v| v.as_bool())
            .or_else(|| value.get("is_scoring").and_then(|v| v.as_bool()))
            .or_else(|| value.get("isScoring").and_then(|v| v.as_bool()))
            .or_else(|| value.get("eligible").and_then(|v| v.as_bool()))
            .or_else(|| value.as_bool())
            .unwrap_or(false);
        return Some((scoring, value));
    }
    None
}

async fn pnl_after_horizon(
    shared: &EngineShared,
    shot: &ShadowShot,
    horizon: Duration,
) -> Option<f64> {
    tokio::time::sleep(horizon).await;
    let book = shared
        .latest_books
        .read()
        .await
        .get(&shot.market_id)
        .cloned()?;
    let mark = mid_for_side(&book, &shot.side);
    if shot.intended_price <= 0.0 {
        return None;
    }
    let pnl = match shot.side {
        OrderSide::BuyYes | OrderSide::BuyNo => {
            ((mark - shot.intended_price) / shot.intended_price) * 10_000.0
        }
        OrderSide::SellYes | OrderSide::SellNo => {
            ((shot.intended_price - mark) / shot.intended_price) * 10_000.0
        }
    };
    Some(pnl)
}

fn evaluate_fillable(
    shot: &ShadowShot,
    book: &BookTop,
    latency_ms: f64,
) -> (bool, Option<f64>, f64) {
    let probe_px = if shot.survival_probe_price > 0.0 {
        shot.survival_probe_price
    } else {
        shot.intended_price
    };
    let (crossable, fill_px) = match shot.side {
        OrderSide::BuyYes => (probe_px >= book.ask_yes, book.ask_yes),
        OrderSide::SellYes => (probe_px <= book.bid_yes, book.bid_yes),
        OrderSide::BuyNo => (probe_px >= book.ask_no, book.ask_no),
        OrderSide::SellNo => (probe_px <= book.bid_no, book.bid_no),
    };
    if !crossable || probe_px <= 0.0 {
        return (false, None, 0.0);
    }
    let queue_fill_prob = estimate_queue_fill_prob(shot, book, latency_ms);
    if queue_fill_prob < 0.55 {
        return (false, None, queue_fill_prob);
    }
    let mut slippage = match shot.side {
        OrderSide::BuyYes | OrderSide::BuyNo => ((fill_px - probe_px) / probe_px) * 10_000.0,
        OrderSide::SellYes | OrderSide::SellNo => ((probe_px - fill_px) / probe_px) * 10_000.0,
    };
    slippage += (1.0 - queue_fill_prob) * 8.0;
    (true, Some(slippage), queue_fill_prob)
}

fn classify_unfilled_outcome(
    book: &BookTop,
    latency_ms: f64,
    delay_ms: u64,
    queue_fill_prob: f64,
) -> EdgeAttribution {
    let spread = (book.ask_yes - book.bid_yes).max(0.0);
    if delay_ms >= 400 {
        return EdgeAttribution::StaleQuote;
    }
    if book.ask_yes <= 0.0 || book.bid_yes <= 0.0 {
        return EdgeAttribution::LiquidityThin;
    }
    if spread > 0.05 {
        return EdgeAttribution::SpreadTooWide;
    }
    if queue_fill_prob < 0.55 {
        return EdgeAttribution::LatencyTail;
    }
    if latency_ms > 100.0 {
        return EdgeAttribution::LatencyTail;
    }
    EdgeAttribution::BookMoved
}

fn classify_filled_outcome(
    edge_net_bps: f64,
    pnl_10s_bps: Option<f64>,
    slippage_bps: Option<f64>,
) -> EdgeAttribution {
    if edge_net_bps < 0.0 {
        return EdgeAttribution::FeeOverrun;
    }
    if slippage_bps.unwrap_or(0.0) > edge_net_bps.abs() {
        return EdgeAttribution::SignalLag;
    }
    if pnl_10s_bps.unwrap_or(0.0) < 0.0 {
        return EdgeAttribution::AdverseSelection;
    }
    EdgeAttribution::Unknown
}

fn mid_for_side(book: &BookTop, side: &OrderSide) -> f64 {
    match side {
        OrderSide::BuyYes | OrderSide::SellYes => (book.bid_yes + book.ask_yes) * 0.5,
        OrderSide::BuyNo | OrderSide::SellNo => (book.bid_no + book.ask_no) * 0.5,
    }
}

fn aggressive_price_for_side(book: &BookTop, side: &OrderSide) -> f64 {
    match side {
        OrderSide::BuyYes => book.ask_yes,
        OrderSide::SellYes => book.bid_yes,
        OrderSide::BuyNo => book.ask_no,
        OrderSide::SellNo => book.bid_no,
    }
}

async fn build_toxicity_live_report(
    tox_state: Arc<RwLock<HashMap<String, MarketToxicState>>>,
    shadow_stats: Arc<ShadowStats>,
    execution: Arc<ClobExecution>,
    toxicity_cfg: Arc<RwLock<ToxicityConfig>>,
) -> ToxicityLiveReport {
    let cfg = toxicity_cfg.read().await.clone();
    let states = tox_state.read().await.clone();
    let shots = shadow_stats.shots.read().await.clone();
    let outcomes = shadow_stats.outcomes.read().await.clone();

    let mut rows = Vec::<ToxicityMarketRow>::new();
    let mut safe = 0usize;
    let mut caution = 0usize;
    let mut danger = 0usize;
    let mut tox_sum = 0.0;

    for (market_id, st) in states {
        let symbol = if !st.symbol.is_empty() {
            st.symbol.clone()
        } else {
            shots
                .iter()
                .find(|s| s.market_id == market_id)
                .map(|s| s.symbol.clone())
                .or_else(|| {
                    outcomes
                        .iter()
                        .find(|o| o.market_id == market_id)
                        .map(|o| o.symbol.clone())
                })
                .unwrap_or_else(|| "UNKNOWN".to_string())
        };
        let attempted = st.attempted.max(1);
        let no_quote_rate = st.no_quote as f64 / attempted as f64;
        let symbol_missing_rate = st.symbol_missing as f64 / attempted as f64;
        let markout_10s_bps = percentile(&st.markout_10s, 0.50).unwrap_or(0.0);
        let markout_samples = st
            .markout_1s
            .len()
            .max(st.markout_5s.len())
            .max(st.markout_10s.len());
        let market_score = compute_market_score(&st, st.last_tox_score, markout_samples);
        let pending_exposure = execution.open_order_notional_for_market(&market_id);

        tox_sum += st.last_tox_score;
        match st.last_regime {
            ToxicRegime::Safe => safe += 1,
            ToxicRegime::Caution => caution += 1,
            ToxicRegime::Danger => danger += 1,
        }

        rows.push(ToxicityMarketRow {
            market_rank: 0,
            market_id,
            symbol,
            tox_score: st.last_tox_score,
            regime: st.last_regime,
            market_score,
            markout_10s_bps,
            no_quote_rate,
            symbol_missing_rate,
            pending_exposure,
            active_for_quoting: false,
        });
    }

    rows.sort_by(|a, b| b.market_score.total_cmp(&a.market_score));
    let top_n = cfg.active_top_n_markets;
    for (idx, row) in rows.iter_mut().enumerate() {
        row.market_rank = idx + 1;
        row.active_for_quoting = top_n == 0 || (idx < top_n);
    }
    let avg = if rows.is_empty() {
        0.0
    } else {
        tox_sum / (rows.len() as f64)
    };

    ToxicityLiveReport {
        ts_ms: Utc::now().timestamp_millis(),
        average_tox_score: avg,
        safe_count: safe,
        caution_count: caution,
        danger_count: danger,
        rows,
    }
}

fn load_fair_value_config() -> BasisMrConfig {
    let path = Path::new("configs/strategy.toml");
    let Ok(raw) = fs::read_to_string(path) else {
        return BasisMrConfig::default();
    };

    let mut cfg = BasisMrConfig::default();
    let mut in_section = false;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            in_section = line == "[fair_value.basis_mr]";
            continue;
        }
        if !in_section {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim();
        let val = v.trim().trim_matches('"');
        match key {
            "enabled" => {
                if let Ok(parsed) = val.parse::<bool>() {
                    cfg.enabled = parsed;
                }
            }
            "alpha_mean" => {
                if let Ok(parsed) = val.parse::<f64>() {
                    cfg.alpha_mean = parsed.clamp(0.0, 1.0);
                }
            }
            "alpha_var" => {
                if let Ok(parsed) = val.parse::<f64>() {
                    cfg.alpha_var = parsed.clamp(0.0, 1.0);
                }
            }
            "alpha_ret" => {
                if let Ok(parsed) = val.parse::<f64>() {
                    cfg.alpha_ret = parsed.clamp(0.0, 1.0);
                }
            }
            "alpha_vol" => {
                if let Ok(parsed) = val.parse::<f64>() {
                    cfg.alpha_vol = parsed.clamp(0.0, 1.0);
                }
            }
            "k_revert" => {
                if let Ok(parsed) = val.parse::<f64>() {
                    cfg.k_revert = parsed.clamp(0.0, 5.0);
                }
            }
            "z_cap" => {
                if let Ok(parsed) = val.parse::<f64>() {
                    cfg.z_cap = parsed.clamp(0.5, 8.0);
                }
            }
            "min_confidence" => {
                if let Ok(parsed) = val.parse::<f64>() {
                    cfg.min_confidence = parsed.clamp(0.0, 1.0);
                }
            }
            "warmup_ticks" => {
                if let Ok(parsed) = val.parse::<usize>() {
                    cfg.warmup_ticks = parsed.max(1);
                }
            }
            _ => {}
        }
    }
    cfg
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UniverseConfig {
    assets: Vec<String>,
    market_types: Vec<String>,
    tier_whitelist: Vec<String>,
    tier_blacklist: Vec<String>,
}

impl Default for UniverseConfig {
    fn default() -> Self {
        Self {
            assets: vec![
                "BTCUSDT".to_string(),
                "ETHUSDT".to_string(),
                "SOLUSDT".to_string(),
                "XRPUSDT".to_string(),
            ],
            market_types: vec![
                "updown".to_string(),
                "above_below".to_string(),
                "range".to_string(),
            ],
            tier_whitelist: Vec::new(),
            tier_blacklist: Vec::new(),
        }
    }
}

fn parse_toml_array_of_strings(val: &str) -> Vec<String> {
    let trimmed = val.trim();
    if !(trimmed.starts_with('[') && trimmed.ends_with(']')) {
        return Vec::new();
    }
    let inner = &trimmed[1..trimmed.len() - 1];
    inner
        .split(',')
        .map(|s| s.trim().trim_matches('"').trim_matches('\'').to_string())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
}

fn parse_toml_array_for_key(raw: &str, key: &str) -> Option<Vec<String>> {
    let mut collecting = false;
    let mut buf = String::new();

    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        if !collecting {
            let Some((k, v)) = trimmed.split_once('=') else {
                continue;
            };
            if k.trim() != key {
                continue;
            }
            let value = v.trim();
            buf.push_str(value);
            collecting = !(value.starts_with('[') && value.ends_with(']'));
            if !collecting {
                break;
            }
            continue;
        }

        // Keep concatenating multiline array items until closing ']'.
        buf.push_str(trimmed);
        if trimmed.ends_with(']') {
            break;
        }
    }

    if buf.is_empty() {
        return None;
    }
    let parsed = parse_toml_array_of_strings(&buf);
    if parsed.is_empty() {
        None
    } else {
        Some(parsed)
    }
}

fn load_strategy_config() -> MakerConfig {
    let path = Path::new("configs/strategy.toml");
    let Ok(raw) = fs::read_to_string(path) else {
        return MakerConfig::default();
    };
    let mut cfg = MakerConfig::default();
    let mut in_maker = false;
    let mut in_taker = false;
    let mut in_online = false;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            in_maker = line == "[maker]";
            in_taker = line == "[taker]";
            in_online = line == "[online_calibration]";
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim();
        let val = v.trim().trim_matches('"');
        if in_maker {
            match key {
                "base_quote_size" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.base_quote_size = parsed.max(0.01);
                    }
                }
                "min_edge_bps" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.min_edge_bps = parsed.max(0.0);
                    }
                }
                "inventory_skew" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.inventory_skew = parsed.clamp(0.0, 1.0);
                    }
                }
                "max_spread" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.max_spread = parsed.max(0.0001);
                    }
                }
                "ttl_ms" => {
                    if let Ok(parsed) = val.parse::<u64>() {
                        cfg.ttl_ms = parsed.max(50);
                    }
                }
                _ => {}
            }
        } else if in_taker {
            match key {
                "trigger_bps" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.taker_trigger_bps = parsed.max(0.0);
                    }
                }
                "max_slippage_bps" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.taker_max_slippage_bps = parsed.max(0.0);
                    }
                }
                "stale_tick_filter_ms" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.stale_tick_filter_ms = parsed.clamp(50.0, 5_000.0);
                    }
                }
                "market_tier_profile" => {
                    cfg.market_tier_profile = val.to_string();
                }
                _ => {}
            }
        } else if in_online {
            match key {
                "capital_fraction_kelly" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.capital_fraction_kelly = parsed.clamp(0.01, 1.0);
                    }
                }
                "variance_penalty_lambda" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.variance_penalty_lambda = parsed.clamp(0.0, 5.0);
                    }
                }
                _ => {}
            }
        }
    }
    cfg
}

fn load_risk_limits_config() -> RiskLimits {
    let path = Path::new("configs/risk.toml");
    let Ok(raw) = fs::read_to_string(path) else {
        let mut defaults = RiskLimits::default();
        defaults.max_drawdown_pct = 0.015;
        return defaults;
    };
    let mut cfg = RiskLimits::default();
    cfg.max_drawdown_pct = 0.015;
    let mut in_max = false;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            in_max = line == "[max]";
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim();
        let val = v.trim().trim_matches('"');
        if in_max {
            match key {
                "market_notional" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.max_market_notional = parsed.max(0.0);
                    }
                }
                "asset_notional" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.max_asset_notional = parsed.max(0.0);
                    }
                }
                "open_orders" => {
                    if let Ok(parsed) = val.parse::<usize>() {
                        cfg.max_open_orders = parsed.max(1);
                    }
                }
                "max_loss_streak" => {
                    if let Ok(parsed) = val.parse::<u32>() {
                        cfg.max_loss_streak = parsed.max(1);
                    }
                }
                "cooldown_sec" => {
                    if let Ok(parsed) = val.parse::<u64>() {
                        cfg.cooldown_sec = parsed.max(1);
                    }
                }
                _ => {}
            }
        }
        if key == "drawdown_stop_pct" {
            if let Ok(parsed) = val.parse::<f64>() {
                cfg.max_drawdown_pct = parsed.clamp(0.001, 1.0);
            }
        } else if key == "max_loss_streak" {
            if let Ok(parsed) = val.parse::<u32>() {
                cfg.max_loss_streak = parsed.max(1);
            }
        } else if key == "cooldown_sec" {
            if let Ok(parsed) = val.parse::<u64>() {
                cfg.cooldown_sec = parsed.max(1);
            }
        }
    }
    cfg
}

fn load_execution_config() -> ExecutionConfig {
    let path = Path::new("configs/execution.toml");
    let Ok(raw) = fs::read_to_string(path) else {
        return ExecutionConfig::default();
    };
    let mut cfg = ExecutionConfig::default();
    let mut in_execution = false;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            in_execution = line == "[execution]";
            continue;
        }
        if !in_execution {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim();
        let val = v.trim().trim_matches('"');
        match key {
            "mode" => cfg.mode = val.to_string(),
            "rate_limit_rps" => {
                if let Ok(parsed) = val.parse::<f64>() {
                    cfg.rate_limit_rps = parsed.max(0.1);
                }
            }
            "http_timeout_ms" => {
                if let Ok(parsed) = val.parse::<u64>() {
                    cfg.http_timeout_ms = parsed.max(100);
                }
            }
            "clob_endpoint" => cfg.clob_endpoint = val.to_string(),
            _ => {}
        }
    }
    cfg
}

fn load_universe_config() -> UniverseConfig {
    let path = Path::new("configs/universe.toml");
    let Ok(raw) = fs::read_to_string(path) else {
        return UniverseConfig::default();
    };
    let mut cfg = UniverseConfig::default();
    if let Some(parsed) = parse_toml_array_for_key(&raw, "assets") {
        cfg.assets = parsed;
    }
    if let Some(parsed) = parse_toml_array_for_key(&raw, "market_types") {
        cfg.market_types = parsed;
    }
    if let Some(parsed) = parse_toml_array_for_key(&raw, "tier_whitelist") {
        cfg.tier_whitelist = parsed;
    }
    if let Some(parsed) = parse_toml_array_for_key(&raw, "tier_blacklist") {
        cfg.tier_blacklist = parsed;
    }
    cfg
}

fn load_perf_profile_config() -> PerfProfile {
    let path = Path::new("configs/latency.toml");
    let Ok(raw) = fs::read_to_string(path) else {
        return PerfProfile::default();
    };

    let mut cfg = PerfProfile::default();
    let mut in_runtime = false;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            in_runtime = line == "[runtime]";
            continue;
        }
        if !in_runtime {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim();
        let val = v.trim().trim_matches('"');
        match key {
            "tail_guard" => {
                if let Ok(parsed) = val.parse::<f64>() {
                    cfg.tail_guard = parsed.clamp(0.50, 0.9999);
                }
            }
            "io_flush_batch" => {
                if let Ok(parsed) = val.parse::<usize>() {
                    cfg.io_flush_batch = parsed.clamp(1, 4096);
                }
            }
            "io_queue_capacity" => {
                if let Ok(parsed) = val.parse::<usize>() {
                    cfg.io_queue_capacity = parsed.clamp(256, 262_144);
                }
            }
            "json_mode" => {
                cfg.json_mode = val.to_string();
            }
            "io_drop_on_full" => {
                if let Ok(parsed) = val.parse::<bool>() {
                    cfg.io_drop_on_full = parsed;
                }
            }
            _ => {}
        }
    }
    cfg
}

fn ensure_dataset_dirs() {
    for bucket in ["raw", "normalized", "reports"] {
        let path = dataset_dir(bucket);
        let _ = fs::create_dir_all(path);
    }
}

fn dataset_date() -> String {
    Utc::now().format("%Y-%m-%d").to_string()
}

fn dataset_dir(kind: &str) -> PathBuf {
    PathBuf::from("datasets").join(kind).join(dataset_date())
}

fn dataset_path(kind: &str, filename: &str) -> PathBuf {
    dataset_dir(kind).join(filename)
}

fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn count_jsonl_lines(path: &Path) -> i64 {
    let Ok(raw) = fs::read_to_string(path) else {
        return 0;
    };
    raw.lines().filter(|l| !l.trim().is_empty()).count() as i64
}

#[derive(Debug)]
struct JsonlWriteReq {
    path: PathBuf,
    line: String,
}

static JSONL_WRITER: OnceLock<mpsc::Sender<JsonlWriteReq>> = OnceLock::new();
static JSONL_QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);
static JSONL_QUEUE_CAP: AtomicU64 = AtomicU64::new(0);
static JSONL_DROP_ON_FULL: AtomicBool = AtomicBool::new(true);
static NORMALIZED_INGEST_SEQ: AtomicU64 = AtomicU64::new(0);

fn next_normalized_ingest_seq() -> u64 {
    NORMALIZED_INGEST_SEQ.fetch_add(1, Ordering::Relaxed) + 1
}

async fn init_jsonl_writer(perf_profile: Arc<RwLock<PerfProfile>>) {
    if JSONL_WRITER.get().is_some() {
        return;
    }
    let cfg = perf_profile.read().await.clone();
    let (tx, mut rx) = mpsc::channel::<JsonlWriteReq>(cfg.io_queue_capacity.max(256));
    JSONL_QUEUE_CAP.store(cfg.io_queue_capacity.max(256) as u64, Ordering::Relaxed);
    JSONL_DROP_ON_FULL.store(cfg.io_drop_on_full, Ordering::Relaxed);
    if JSONL_WRITER.set(tx.clone()).is_err() {
        return;
    }
    tokio::spawn(async move {
        let mut batch = Vec::<JsonlWriteReq>::new();
        let mut ticker = tokio::time::interval(Duration::from_millis(200));
        loop {
            tokio::select! {
                maybe_req = rx.recv() => {
                    match maybe_req {
                        Some(req) => {
                            batch.push(req);
                            let flush_batch = perf_profile.read().await.io_flush_batch.max(1);
                            if batch.len() >= flush_batch {
                                let to_flush = std::mem::take(&mut batch);
                                let _ = tokio::task::spawn_blocking(move || flush_jsonl_batch_sync(to_flush)).await;
                            }
                        }
                        None => {
                            if !batch.is_empty() {
                                let to_flush = std::mem::take(&mut batch);
                                let _ = tokio::task::spawn_blocking(move || flush_jsonl_batch_sync(to_flush)).await;
                            }
                            break;
                        }
                    }
                }
                _ = ticker.tick() => {
                    if !batch.is_empty() {
                        let to_flush = std::mem::take(&mut batch);
                        let _ = tokio::task::spawn_blocking(move || flush_jsonl_batch_sync(to_flush)).await;
                    }
                }
            }
            let cap = JSONL_QUEUE_CAP.load(Ordering::Relaxed) as usize;
            JSONL_QUEUE_DEPTH.store(cap.saturating_sub(tx.capacity()) as u64, Ordering::Relaxed);
        }
    });
}

fn flush_jsonl_batch_sync(batch: Vec<JsonlWriteReq>) {
    let mut grouped = HashMap::<PathBuf, Vec<String>>::new();
    for req in batch {
        grouped.entry(req.path).or_default().push(req.line);
    }
    for (path, lines) in grouped {
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&path) {
            for line in lines {
                let _ = writeln!(file, "{line}");
            }
        }
    }
}

fn current_jsonl_queue_depth() -> u64 {
    JSONL_QUEUE_DEPTH.load(Ordering::Relaxed)
}

fn append_jsonl(path: &Path, value: &serde_json::Value) {
    let line = value.to_string();
    if let Some(tx) = JSONL_WRITER.get() {
        let req = JsonlWriteReq {
            path: path.to_path_buf(),
            line: line.clone(),
        };
        match tx.try_send(req) {
            Ok(_) => {
                let cap = JSONL_QUEUE_CAP.load(Ordering::Relaxed) as usize;
                JSONL_QUEUE_DEPTH
                    .store(cap.saturating_sub(tx.capacity()) as u64, Ordering::Relaxed);
                return;
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                metrics::counter!("io.jsonl.queue_full").increment(1);
                if JSONL_DROP_ON_FULL.load(Ordering::Relaxed) {
                    metrics::counter!("io.jsonl.dropped").increment(1);
                    return;
                }
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                metrics::counter!("io.jsonl.queue_closed").increment(1);
                if JSONL_DROP_ON_FULL.load(Ordering::Relaxed) {
                    metrics::counter!("io.jsonl.dropped").increment(1);
                    return;
                }
            }
        }
    }
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{line}");
    }
}

fn persist_live_report_files(live: &ShadowLiveReport) {
    let reports_dir = dataset_dir("reports");
    let _ = fs::create_dir_all(&reports_dir);

    let live_json_path = reports_dir.join("shadow_live_latest.json");
    if let Ok(raw) = serde_json::to_string_pretty(live) {
        let _ = fs::write(live_json_path, raw);
    }
}

fn persist_engine_pnl_report(report: &EnginePnlReport) {
    let reports_dir = dataset_dir("reports");
    let _ = fs::create_dir_all(&reports_dir);

    let json_path = reports_dir.join("engine_pnl_breakdown_latest.json");
    if let Ok(raw) = serde_json::to_string_pretty(report) {
        let _ = fs::write(json_path, raw);
    }

    let csv_path = reports_dir.join("engine_pnl_breakdown.csv");
    let mut rows = String::new();
    rows.push_str("window_id,engine,samples,total_usdc,p50_usdc,p10_usdc,positive_ratio\n");
    for row in &report.rows {
        rows.push_str(&format!(
            "{},{},{},{:.6},{:.6},{:.6},{:.6}\n",
            report.window_id,
            row.engine,
            row.samples,
            row.total_usdc,
            row.p50_usdc,
            row.p10_usdc,
            row.positive_ratio
        ));
    }
    let _ = fs::write(csv_path, rows);
}

fn persist_final_report_files(report: &ShadowFinalReport) {
    let reports_dir = dataset_dir("reports");
    let _ = fs::create_dir_all(&reports_dir);

    let md_path = reports_dir.join("report_shadow_12h.md");
    let gate_label = if report.gate.pass { "PASS" } else { "FAIL" };
    let mut md = String::new();
    md.push_str("# Shadow 12h Report\n\n");
    md.push_str(&format!("- gate: {gate_label}\n"));
    md.push_str(&format!(
        "- fillability@10ms: {:.4}\n",
        report.gate.fillability_10ms
    ));
    md.push_str(&format!(
        "- net_edge_p50_bps: {:.4}\n",
        report.gate.net_edge_p50_bps
    ));
    md.push_str(&format!(
        "- net_edge_p10_bps: {:.4}\n",
        report.gate.net_edge_p10_bps
    ));
    md.push_str(&format!(
        "- net_markout_10s_usdc_p50: {:.6}\n",
        report.gate.net_markout_10s_usdc_p50
    ));
    md.push_str(&format!(
        "- roi_notional_10s_bps_p50: {:.6}\n",
        report.gate.roi_notional_10s_bps_p50
    ));
    md.push_str(&format!(
        "- ev_net_usdc_p50: {:.6}\n",
        report.gate.ev_net_usdc_p50
    ));
    md.push_str(&format!(
        "- ev_net_usdc_p10: {:.6}\n",
        report.gate.ev_net_usdc_p10
    ));
    md.push_str(&format!(
        "- ev_positive_ratio: {:.4}\n",
        report.gate.ev_positive_ratio
    ));
    md.push_str(&format!(
        "- executed_over_eligible: {:.4}\n",
        report.gate.executed_over_eligible
    ));
    md.push_str(&format!(
        "- eligible_count: {}\n",
        report.gate.eligible_count
    ));
    md.push_str(&format!(
        "- executed_count: {}\n",
        report.gate.executed_count
    ));
    md.push_str(&format!(
        "- pnl_10s_p50_bps_raw: {:.4}\n",
        report.gate.pnl_10s_p50_bps_raw
    ));
    md.push_str(&format!(
        "- pnl_10s_p50_bps_robust: {:.4}\n",
        report.gate.pnl_10s_p50_bps_robust
    ));
    md.push_str(&format!(
        "- pnl_10s_sample_count: {}\n",
        report.gate.pnl_10s_sample_count
    ));
    md.push_str(&format!(
        "- pnl_10s_outlier_ratio: {:.4}\n",
        report.gate.pnl_10s_outlier_ratio
    ));
    md.push_str(&format!(
        "- quote_block_ratio: {:.4}\n",
        report.gate.quote_block_ratio
    ));
    md.push_str(&format!(
        "- policy_block_ratio: {:.4}\n",
        report.gate.policy_block_ratio
    ));
    md.push_str(&format!(
        "- strategy_uptime_pct: {:.2}\n",
        report.gate.strategy_uptime_pct
    ));
    md.push_str(&format!(
        "- data_valid_ratio: {:.5}\n",
        report.gate.data_valid_ratio
    ));
    md.push_str(&format!(
        "- seq_gap_rate: {:.5}\n",
        report.gate.seq_gap_rate
    ));
    md.push_str(&format!(
        "- ts_inversion_rate: {:.5}\n",
        report.gate.ts_inversion_rate
    ));
    md.push_str(&format!(
        "- stale_tick_drop_ratio: {:.5}\n",
        report.gate.stale_tick_drop_ratio
    ));
    md.push_str(&format!(
        "- tick_to_ack_p99_ms: {:.4}\n\n",
        report.gate.tick_to_ack_p99_ms
    ));
    md.push_str(&format!(
        "- tick_to_decision_p99_ms: {:.4}\n",
        report.live.tick_to_decision_p99_ms
    ));
    md.push_str(&format!(
        "- ack_only_p99_ms: {:.4}\n",
        report.live.ack_only_p99_ms
    ));
    md.push_str(&format!(
        "- decision_queue_wait_p99_ms: {:.4}\n",
        report.gate.decision_queue_wait_p99_ms
    ));
    md.push_str(&format!(
        "- decision_compute_p99_ms: {:.4}\n",
        report.gate.decision_compute_p99_ms
    ));
    md.push_str(&format!(
        "- source_latency_p99_ms: {:.4}\n",
        report.gate.source_latency_p99_ms
    ));
    md.push_str(&format!(
        "- local_backlog_p99_ms: {:.4}\n",
        report.gate.local_backlog_p99_ms
    ));
    md.push_str(&format!(
        "- queue_depth_p99: {:.4}\n",
        report.live.queue_depth_p99
    ));
    md.push_str(&format!(
        "- event_backlog_p99: {:.4}\n\n",
        report.live.event_backlog_p99
    ));
    md.push_str(&format!(
        "- quote_attempted: {}\n- quote_blocked: {}\n- policy_blocked: {}\n- ref_ticks_total: {}\n- book_ticks_total: {}\n- ref_freshness_ms: {}\n- book_freshness_ms: {}\n\n",
        report.live.quote_attempted,
        report.live.quote_blocked,
        report.live.policy_blocked,
        report.live.ref_ticks_total,
        report.live.book_ticks_total,
        report.live.ref_freshness_ms,
        report.live.book_freshness_ms
    ));
    if report.gate.failed_reasons.is_empty() {
        md.push_str("## Failed Reasons\n- none\n");
    } else {
        md.push_str("## Failed Reasons\n");
        for reason in &report.gate.failed_reasons {
            md.push_str(&format!("- {reason}\n"));
        }
    }
    if report.live.blocked_reason_counts.is_empty() {
        md.push_str("\n## Blocked Reasons\n- none\n");
    } else {
        md.push_str("\n## Blocked Reasons\n");
        let mut rows = report.live.blocked_reason_counts.iter().collect::<Vec<_>>();
        rows.sort_by(|a, b| b.1.cmp(a.1));
        for (reason, count) in rows {
            md.push_str(&format!("- {}: {}\n", reason, count));
        }
    }
    let _ = fs::write(md_path, md);

    let latency_csv = reports_dir.join("latency_breakdown_12h.csv");
    let mut latency_rows = String::new();
    latency_rows.push_str("stage,p50,p90,p99,unit\n");
    latency_rows.push_str(&format!(
        "feed_in,{:.6},{:.6},{:.6},ms\n",
        report.live.latency.feed_in_p50_ms,
        report.live.latency.feed_in_p90_ms,
        report.live.latency.feed_in_p99_ms
    ));
    latency_rows.push_str(&format!(
        "signal,{:.6},{:.6},{:.6},us\n",
        report.live.latency.signal_p50_us,
        report.live.latency.signal_p90_us,
        report.live.latency.signal_p99_us
    ));
    latency_rows.push_str(&format!(
        "quote,{:.6},{:.6},{:.6},us\n",
        report.live.latency.quote_p50_us,
        report.live.latency.quote_p90_us,
        report.live.latency.quote_p99_us
    ));
    latency_rows.push_str(&format!(
        "risk,{:.6},{:.6},{:.6},us\n",
        report.live.latency.risk_p50_us,
        report.live.latency.risk_p90_us,
        report.live.latency.risk_p99_us
    ));
    latency_rows.push_str(&format!(
        "decision_queue_wait,{:.6},{:.6},{:.6},ms\n",
        report.live.latency.decision_queue_wait_p50_ms,
        report.live.latency.decision_queue_wait_p90_ms,
        report.live.latency.decision_queue_wait_p99_ms
    ));
    latency_rows.push_str(&format!(
        "decision_compute,{:.6},{:.6},{:.6},ms\n",
        report.live.latency.decision_compute_p50_ms,
        report.live.latency.decision_compute_p90_ms,
        report.live.latency.decision_compute_p99_ms
    ));
    latency_rows.push_str(&format!(
        "tick_to_decision,{:.6},{:.6},{:.6},ms\n",
        report.live.latency.tick_to_decision_p50_ms,
        report.live.latency.tick_to_decision_p90_ms,
        report.live.latency.tick_to_decision_p99_ms
    ));
    latency_rows.push_str(&format!(
        "ack_only,{:.6},{:.6},{:.6},ms\n",
        report.live.latency.ack_only_p50_ms,
        report.live.latency.ack_only_p90_ms,
        report.live.latency.ack_only_p99_ms
    ));
    latency_rows.push_str(&format!(
        "tick_to_ack,{:.6},{:.6},{:.6},ms\n",
        report.live.latency.tick_to_ack_p50_ms,
        report.live.latency.tick_to_ack_p90_ms,
        report.live.latency.tick_to_ack_p99_ms
    ));
    latency_rows.push_str(&format!(
        "parse,{:.6},{:.6},{:.6},us\n",
        0.0, 0.0, report.live.latency.parse_p99_us
    ));
    latency_rows.push_str(&format!(
        "io_queue,{:.6},{:.6},{:.6},count\n",
        0.0, 0.0, report.live.latency.io_queue_p99_ms
    ));
    latency_rows.push_str(&format!(
        "bus_lag,{:.6},{:.6},{:.6},count\n",
        0.0, 0.0, report.live.latency.bus_lag_p99_ms
    ));
    latency_rows.push_str(&format!(
        "shadow_fill,{:.6},{:.6},{:.6},ms\n",
        report.live.latency.shadow_fill_p50_ms,
        report.live.latency.shadow_fill_p90_ms,
        report.live.latency.shadow_fill_p99_ms
    ));
    latency_rows.push_str(&format!(
        "source_latency,{:.6},{:.6},{:.6},ms\n",
        report.live.latency.source_latency_p50_ms,
        report.live.latency.source_latency_p90_ms,
        report.live.latency.source_latency_p99_ms
    ));
    latency_rows.push_str(&format!(
        "local_backlog,{:.6},{:.6},{:.6},ms\n",
        report.live.latency.local_backlog_p50_ms,
        report.live.latency.local_backlog_p90_ms,
        report.live.latency.local_backlog_p99_ms
    ));
    let _ = fs::write(latency_csv, latency_rows);

    let score_path = reports_dir.join("market_scorecard.csv");
    let mut score_rows = String::new();
    score_rows.push_str("market_id,symbol,shots,outcomes,fillability_10ms,net_edge_p50_bps,net_edge_p10_bps,pnl_10s_p50_bps,net_markout_10s_usdc_p50,roi_notional_10s_bps_p50\n");
    for row in &report.live.market_scorecard {
        score_rows.push_str(&format!(
            "{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6}\n",
            row.market_id,
            row.symbol,
            row.shots,
            row.outcomes,
            row.fillability_10ms,
            row.net_edge_p50_bps,
            row.net_edge_p10_bps,
            row.pnl_10s_p50_bps,
            row.net_markout_10s_usdc_p50,
            row.roi_notional_10s_bps_p50
        ));
    }
    let _ = fs::write(score_path, score_rows);

    let fixlist_path = reports_dir.join("next_fixlist.md");
    let mut fixlist = String::new();
    fixlist.push_str("# Next Fixlist\n\n");
    if report.gate.failed_reasons.is_empty() {
        fixlist.push_str("- Gate passed. Keep conservative limits and continue monitoring.\n");
    } else {
        for reason in &report.gate.failed_reasons {
            fixlist.push_str(&format!("- {reason}\n"));
        }
    }
    let _ = fs::write(fixlist_path, fixlist);

    let truth_manifest_path = reports_dir.join("truth_manifest.json");
    let truth_manifest = serde_json::json!({
        "generated_at_utc": Utc::now().to_rfc3339(),
        "metrics_contract_version": "2026-02-14.v1",
        "window": {
            "window_id": report.live.window_id,
            "window_shots": report.live.window_shots,
            "window_outcomes": report.live.window_outcomes,
            "gate_ready": report.live.gate_ready,
        },
        "data_chain": {
            "raw_fields": ["sha256", "source_seq", "ingest_seq", "event_ts_exchange_ms", "recv_ts_local_ns"],
            "normalized_fields": ["source_seq", "ingest_seq", "market_id", "symbol", "delay_ms", "fillable", "net_markout_10s_usdc"],
            "invalid_excluded_from_gate": true
        },
        "formulas": {
            "quote_block_ratio": "quote_blocked / (quote_attempted + quote_blocked)",
            "policy_block_ratio": "policy_blocked / (quote_attempted + policy_blocked)",
            "policy_blocked_scope": "risk:* and risk_capped_zero only",
            "executed_over_eligible": "executed_count / eligible_count",
            "ev_net_usdc_p50": "p50(net_markout_10s_usdc)",
            "ev_positive_ratio": "count(net_markout_10s_usdc > 0) / count(valid outcomes)"
        },
        "hard_gates": {
            "data_valid_ratio_min": 0.999,
            "seq_gap_rate_max": 0.001,
            "ts_inversion_rate_max": 0.0005,
            "tick_to_ack_p99_ms_max": 450.0,
            "decision_compute_p99_ms_max": 2.0,
            "feed_in_p99_ms_max": 800.0,
            "executed_over_eligible_min": 0.60,
            "quote_block_ratio_max": 0.10,
            "policy_block_ratio_max": 0.10,
            "ev_net_usdc_p50_min": 0.0,
            "roi_notional_10s_bps_p50_min": 0.0
        }
    });
    if let Ok(raw) = serde_json::to_string_pretty(&truth_manifest) {
        let _ = fs::write(truth_manifest_path, raw);
    }
}

fn persist_toxicity_report_files(report: &ToxicityLiveReport) {
    let reports_dir = dataset_dir("reports");
    let _ = fs::create_dir_all(&reports_dir);

    let live_json_path = reports_dir.join("toxicity_live_latest.json");
    if let Ok(raw) = serde_json::to_string_pretty(report) {
        let _ = fs::write(live_json_path, raw);
    }

    let csv_path = reports_dir.join("toxicity_scorecard.csv");
    let mut rows = String::new();
    rows.push_str("market_rank,active_for_quoting,market_id,symbol,tox_score,regime,market_score,markout_10s_bps,no_quote_rate,symbol_missing_rate,pending_exposure\n");
    for row in &report.rows {
        rows.push_str(&format!(
            "{},{},{},{},{:.6},{:?},{:.6},{:.6},{:.6},{:.6},{:.6}\n",
            row.market_rank,
            row.active_for_quoting,
            row.market_id,
            row.symbol,
            row.tox_score,
            row.regime,
            row.market_score,
            row.markout_10s_bps,
            row.no_quote_rate,
            row.symbol_missing_rate,
            row.pending_exposure
        ));
    }
    let _ = fs::write(csv_path, rows);
}

fn fillability_ratio(outcomes: &[ShadowOutcome], delay_ms: u64) -> f64 {
    let scoped = outcomes
        .iter()
        .filter(|o| o.delay_ms == delay_ms)
        .collect::<Vec<_>>();
    if scoped.is_empty() {
        return 0.0;
    }
    let filled = scoped.iter().filter(|o| o.fillable).count();
    filled as f64 / scoped.len() as f64
}

fn build_market_scorecard(shots: &[ShadowShot], outcomes: &[ShadowOutcome]) -> Vec<MarketScoreRow> {
    const PRIMARY_DELAY_MS: u64 = 10;
    let mut keys: HashMap<(String, String), ()> = HashMap::new();
    for shot in shots {
        keys.insert((shot.market_id.clone(), shot.symbol.clone()), ());
    }
    for outcome in outcomes {
        keys.insert((outcome.market_id.clone(), outcome.symbol.clone()), ());
    }

    let mut rows = Vec::new();
    for (market_id, symbol) in keys.into_keys() {
        let market_shots = shots
            .iter()
            .filter(|s| s.market_id == market_id && s.symbol == symbol)
            .cloned()
            .collect::<Vec<_>>();
        let market_outcomes = outcomes
            .iter()
            .filter(|o| o.market_id == market_id && o.symbol == symbol)
            .cloned()
            .collect::<Vec<_>>();
        let market_shots_primary = market_shots
            .iter()
            .filter(|s| s.delay_ms == PRIMARY_DELAY_MS)
            .collect::<Vec<_>>();
        let market_outcomes_primary = market_outcomes
            .iter()
            .filter(|o| o.delay_ms == PRIMARY_DELAY_MS)
            .collect::<Vec<_>>();

        let net_edges = market_shots_primary
            .iter()
            .map(|s| s.edge_net_bps)
            .collect::<Vec<_>>();
        let pnl_10s = market_outcomes_primary
            .iter()
            .filter_map(|o| o.net_markout_10s_bps.or(o.pnl_10s_bps))
            .collect::<Vec<_>>();
        let net_markout_10s_usdc = market_outcomes_primary
            .iter()
            .filter_map(|o| o.net_markout_10s_usdc)
            .collect::<Vec<_>>();
        let roi_notional_10s_bps = market_outcomes_primary
            .iter()
            .filter_map(|o| o.roi_notional_10s_bps)
            .collect::<Vec<_>>();

        rows.push(MarketScoreRow {
            market_id,
            symbol,
            shots: market_shots_primary.len(),
            outcomes: market_outcomes_primary.len(),
            fillability_10ms: fillability_ratio(&market_outcomes, 10),
            net_edge_p50_bps: percentile(&net_edges, 0.50).unwrap_or(0.0),
            net_edge_p10_bps: percentile(&net_edges, 0.10).unwrap_or(0.0),
            pnl_10s_p50_bps: percentile(&pnl_10s, 0.50).unwrap_or(0.0),
            net_markout_10s_usdc_p50: percentile(&net_markout_10s_usdc, 0.50).unwrap_or(0.0),
            roi_notional_10s_bps_p50: percentile(&roi_notional_10s_bps, 0.50).unwrap_or(0.0),
        });
    }

    rows.sort_by(|a, b| {
        b.net_markout_10s_usdc_p50
            .total_cmp(&a.net_markout_10s_usdc_p50)
    });
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::QuoteIntent;

    #[test]
    fn robust_filter_marks_outliers() {
        let values = vec![1.0, 1.1, 1.2, 1.3, 99.0];
        let (filtered, outlier_ratio) = robust_filter_iqr(&values);
        assert!(filtered.len() < values.len());
        assert!(outlier_ratio > 0.0);
    }

    #[test]
    fn robust_filter_small_sample_passthrough() {
        let values = vec![1.0, 2.0, 3.0, 4.0];
        let (filtered, outlier_ratio) = robust_filter_iqr(&values);
        assert_eq!(filtered, values);
        assert_eq!(outlier_ratio, 0.0);
    }

    #[test]
    fn percentile_basic_behavior() {
        let values = vec![1.0, 5.0, 3.0, 2.0, 4.0];
        assert_eq!(percentile(&values, 0.50), Some(3.0));
        assert_eq!(percentile(&values, 0.0), Some(1.0));
        assert_eq!(percentile(&values, 1.0), Some(5.0));
    }

    #[test]
    fn classify_execution_style_for_yes_side() {
        let book = BookTop {
            market_id: "m".to_string(),
            token_id_yes: "yes".to_string(),
            token_id_no: "no".to_string(),
            bid_yes: 0.49,
            ask_yes: 0.51,
            bid_no: 0.49,
            ask_no: 0.51,
            ts_ms: 1,
            recv_ts_local_ns: 1_000_000,
        };
        let maker = QuoteIntent {
            market_id: "m".to_string(),
            side: OrderSide::BuyYes,
            price: 0.50,
            size: 1.0,
            ttl_ms: 300,
        };
        let taker = QuoteIntent {
            price: 0.51,
            ..maker.clone()
        };
        assert_eq!(classify_execution_style(&book, &maker), ExecutionStyle::Maker);
        assert_eq!(classify_execution_style(&book, &taker), ExecutionStyle::Taker);
    }

    #[test]
    fn normalize_reject_code_sanitizes_non_alnum() {
        let normalized = normalize_reject_code("HTTP 429/Too Many Requests");
        assert_eq!(normalized, "http_429_too_many_requests");
    }

    #[test]
    fn estimate_feed_latency_separates_source_and_backlog() {
        let now = now_ns();
        let now_ms = now / 1_000_000;
        let tick = RefTick {
            source: "binance_ws".to_string(),
            symbol: "BTCUSDT".to_string(),
            event_ts_ms: now_ms - 300,
            recv_ts_ms: now_ms - 200,
            event_ts_exchange_ms: now_ms - 300,
            recv_ts_local_ns: now - 200_000_000,
            price: 70_000.0,
        };
        let book = BookTop {
            market_id: "m".to_string(),
            token_id_yes: "yes".to_string(),
            token_id_no: "no".to_string(),
            bid_yes: 0.49,
            ask_yes: 0.51,
            bid_no: 0.49,
            ask_no: 0.51,
            ts_ms: now_ms - 40,
            recv_ts_local_ns: now - 20_000_000,
        };

        let sample = estimate_feed_latency(&tick, &book);
        assert!(sample.source_latency_ms >= 95.0);
        assert!(sample.source_latency_ms <= 110.0);
        assert!(sample.local_backlog_ms >= 10.0);
        assert!(sample.local_backlog_ms <= 40.0);
        assert!(sample.feed_in_ms >= sample.source_latency_ms);
        assert!((sample.feed_in_ms - (sample.source_latency_ms + sample.local_backlog_ms)).abs() < 3.0);
    }

    #[test]
    fn should_force_taker_respects_profile_and_regime() {
        let mut cfg = MakerConfig::default();
        cfg.taker_trigger_bps = 10.0;
        cfg.market_tier_profile = "balanced".to_string();

        let safe = ToxicDecision {
            market_id: "m".to_string(),
            symbol: "BTCUSDT".to_string(),
            tox_score: 0.1,
            regime: ToxicRegime::Safe,
            reason_codes: vec![],
            ts_ns: 1,
        };
        assert!(should_force_taker(&cfg, &safe, 12.0, 0.8));
        assert!(!should_force_taker(&cfg, &safe, 8.0, 0.8));
        assert!(!should_force_taker(&cfg, &safe, 12.0, 0.4));

        let caution = ToxicDecision {
            regime: ToxicRegime::Caution,
            ..safe.clone()
        };
        assert!(!should_force_taker(&cfg, &caution, 12.0, 0.8));
        cfg.market_tier_profile = "latency_aggressive".to_string();
        assert!(should_force_taker(&cfg, &caution, 12.0, 0.8));
    }

    #[test]
    fn parse_toml_array_for_key_handles_multiline_arrays() {
        let raw = r#"
assets = [
  "BTCUSDT",
  "ETHUSDT",
  "XRPUSDT",
]
market_types = ["updown", "range"]
"#;
        let assets = parse_toml_array_for_key(raw, "assets").unwrap_or_default();
        assert_eq!(
            assets,
            vec![
                "BTCUSDT".to_string(),
                "ETHUSDT".to_string(),
                "XRPUSDT".to_string()
            ]
        );
        let market_types = parse_toml_array_for_key(raw, "market_types").unwrap_or_default();
        assert_eq!(
            market_types,
            vec!["updown".to_string(), "range".to_string()]
        );
    }

    #[test]
    fn parse_toml_array_for_key_returns_none_for_missing_or_empty() {
        let raw = "foo = 1\nassets = []\n";
        assert!(parse_toml_array_for_key(raw, "missing").is_none());
        assert!(parse_toml_array_for_key(raw, "assets").is_none());
    }
}
