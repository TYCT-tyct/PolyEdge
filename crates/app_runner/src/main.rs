use std::collections::{HashMap, VecDeque};
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
    new_id, BookTop, CapitalUpdate, ControlCommand, Direction, DirectionSignal, EdgeAttribution,
    EngineEvent, EnginePnLBreakdown, ExecutionStyle, ExecutionVenue, FairValueModel, InventoryState,
    MarketFeed, MarketHealth, OrderAck, OrderIntentV2, OrderSide, OrderTimeInForce, QuoteEval,
    QuoteIntent, QuotePolicy, RefPriceFeed, RefTick, RiskContext, RiskManager, ShadowOutcome,
    ShadowShot, Signal, TimeframeClass, TimeframeOpp, ToxicDecision, ToxicFeatures, ToxicRegime,
};
use dashmap::DashMap;
use direction_detector::{DirectionConfig, DirectionDetector};
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
use settlement_compounder::{CompounderConfig, SettlementCompounder};
use sha2::{Digest, Sha256};
use strategy_maker::{MakerConfig, MakerQuotePolicy};
use taker_sniper::{TakerAction, TakerSniper, TakerSniperConfig};
use timeframe_router::{RouterConfig, TimeframeRouter};
use tokio::sync::{mpsc, RwLock};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod control_api;
mod engine_core;
mod gate_eval;
mod orchestration;
mod stats_utils;
use engine_core::{
    classify_execution_error_reason, classify_execution_style, is_policy_block_reason,
    is_quote_reject_reason, normalize_reject_code,
};
use stats_utils::{
    freshness_ms, now_ns, percentile, policy_block_ratio, push_capped,
    percentile_deque_capped, quote_block_ratio, robust_filter_iqr, value_to_f64,
};

#[derive(Clone)]
struct AppState {
    paused: Arc<RwLock<bool>>,
    bus: RingBus<EngineEvent>,
    portfolio: Arc<PortfolioBook>,
    execution: Arc<ClobExecution>,
    _shadow: Arc<ShadowExecutor>,
    prometheus: metrics_exporter_prometheus::PrometheusHandle,
    strategy_cfg: Arc<RwLock<Arc<MakerConfig>>>,
    fair_value_cfg: Arc<StdRwLock<BasisMrConfig>>,
    toxicity_cfg: Arc<RwLock<Arc<ToxicityConfig>>>,
    allocator_cfg: Arc<RwLock<AllocatorConfig>>,
    risk_limits: Arc<StdRwLock<RiskLimits>>,
    tox_state: Arc<RwLock<HashMap<String, MarketToxicState>>>,
    shadow_stats: Arc<ShadowStats>,
    perf_profile: Arc<RwLock<PerfProfile>>,
    shared: Arc<EngineShared>,
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

#[derive(Debug, Clone)]
struct SignalCacheEntry {
    signal: Signal,
    ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum PredatorCPriority {
    MakerFirst,
    TakerFirst,
    TakerOnly,
}

impl Default for PredatorCPriority {
    fn default() -> Self {
        Self::TakerFirst
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct PredatorCConfig {
    enabled: bool,
    priority: PredatorCPriority,
    direction_detector: DirectionConfig,
    taker_sniper: TakerSniperConfig,
    router: RouterConfig,
    compounder: CompounderConfig,
}

impl Default for PredatorCConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            priority: PredatorCPriority::TakerFirst,
            direction_detector: DirectionConfig::default(),
            taker_sniper: TakerSniperConfig::default(),
            router: RouterConfig::default(),
            compounder: CompounderConfig::default(),
        }
    }
}

#[derive(Clone)]
struct EngineShared {
    latest_books: Arc<RwLock<HashMap<String, BookTop>>>,
    latest_signals: Arc<DashMap<String, SignalCacheEntry>>,
    market_to_symbol: Arc<RwLock<HashMap<String, String>>>,
    token_to_symbol: Arc<RwLock<HashMap<String, String>>>,
    market_to_timeframe: Arc<RwLock<HashMap<String, TimeframeClass>>>,
    symbol_to_markets: Arc<RwLock<HashMap<String, Vec<String>>>>,
    fee_cache: Arc<RwLock<HashMap<String, FeeRateEntry>>>,
    fee_refresh_inflight: Arc<RwLock<HashMap<String, Instant>>>,
    scoring_cache: Arc<RwLock<HashMap<String, ScoringState>>>,
    scoring_refresh_inflight: Arc<RwLock<HashMap<String, Instant>>>,
    http: Client,
    clob_endpoint: String,
    strategy_cfg: Arc<RwLock<Arc<MakerConfig>>>,
    fusion_cfg: Arc<RwLock<FusionConfig>>,
    edge_model_cfg: Arc<RwLock<EdgeModelConfig>>,
    exit_cfg: Arc<RwLock<ExitConfig>>,
    fair_value_cfg: Arc<StdRwLock<BasisMrConfig>>,
    toxicity_cfg: Arc<RwLock<Arc<ToxicityConfig>>>,
    risk_manager: Arc<DefaultRiskManager>,
    universe_symbols: Arc<Vec<String>>,
    universe_market_types: Arc<Vec<String>>,
    universe_timeframes: Arc<Vec<String>>,
    rate_limit_rps: f64,
    scoring_rebate_factor: f64,
    tox_state: Arc<RwLock<HashMap<String, MarketToxicState>>>,
    shadow_stats: Arc<ShadowStats>,
    predator_cfg: Arc<RwLock<PredatorCConfig>>,
    predator_direction_detector: Arc<RwLock<DirectionDetector>>,
    predator_latest_direction: Arc<RwLock<HashMap<String, DirectionSignal>>>,
    predator_taker_sniper: Arc<RwLock<TakerSniper>>,
    predator_router: Arc<RwLock<TimeframeRouter>>,
    predator_compounder: Arc<RwLock<SettlementCompounder>>,
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
    min_eval_notional_usdc: Option<f64>,
    min_expected_edge_usdc: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct FusionReloadReq {
    enable_udp: Option<bool>,
    mode: Option<String>,
    udp_port: Option<u16>,
    dedupe_window_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct EdgeModelReloadReq {
    model: Option<String>,
    gate_mode: Option<String>,
    version: Option<String>,
    base_gate_bps: Option<f64>,
    congestion_penalty_bps: Option<f64>,
    latency_penalty_bps: Option<f64>,
    fail_cost_bps: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct ExitReloadReq {
    enabled: Option<bool>,
    time_stop_ms: Option<u64>,
    edge_decay_bps: Option<f64>,
    adverse_move_bps: Option<f64>,
    flatten_on_trigger: Option<bool>,
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
struct FusionConfig {
    enable_udp: bool,
    mode: String,
    udp_port: u16,
    dedupe_window_ms: i64,
}

impl Default for FusionConfig {
    fn default() -> Self {
        Self {
            enable_udp: true,
            mode: "active_active".to_string(),
            udp_port: 6666,
            dedupe_window_ms: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EdgeModelConfig {
    model: String,
    gate_mode: String,
    version: String,
    base_gate_bps: f64,
    congestion_penalty_bps: f64,
    latency_penalty_bps: f64,
    fail_cost_bps: f64,
}

impl Default for EdgeModelConfig {
    fn default() -> Self {
        Self {
            model: "ev_net".to_string(),
            gate_mode: "dynamic".to_string(),
            version: "v2-ev-net".to_string(),
            base_gate_bps: 0.0,
            congestion_penalty_bps: 2.0,
            latency_penalty_bps: 2.0,
            fail_cost_bps: 1.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExitConfig {
    enabled: bool,
    time_stop_ms: u64,
    edge_decay_bps: f64,
    adverse_move_bps: f64,
    flatten_on_trigger: bool,
}

impl Default for ExitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            time_stop_ms: 2_000,
            edge_decay_bps: -2.0,
            adverse_move_bps: -8.0,
            flatten_on_trigger: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExecutionConfig {
    mode: String,
    rate_limit_rps: f64,
    http_timeout_ms: u64,
    clob_endpoint: String,
    #[serde(default)]
    order_endpoint: Option<String>,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            mode: "paper".to_string(),
            rate_limit_rps: 15.0,
            http_timeout_ms: 3000,
            clob_endpoint: "https://clob.polymarket.com".to_string(),
            order_endpoint: None,
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
    markout_1s: VecDeque<f64>,
    markout_5s: VecDeque<f64>,
    markout_10s: VecDeque<f64>,
    attempted: u64,
    no_quote: u64,
    symbol_missing: u64,
    last_tox_score: f64,
    last_regime: ToxicRegime,
    market_score: f64,
    cooldown_until_ms: i64,
}

impl Default for MarketToxicState {
    fn default() -> Self {
        Self {
            symbol: String::new(),
            markout_1s: VecDeque::new(),
            markout_5s: VecDeque::new(),
            markout_10s: VecDeque::new(),
            attempted: 0,
            no_quote: 0,
            symbol_missing: 0,
            last_tox_score: 0.0,
            last_regime: ToxicRegime::Safe,
            market_score: 80.0,
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
    book_latency_p50_ms: f64,
    book_latency_p90_ms: f64,
    book_latency_p99_ms: f64,
    local_backlog_p50_ms: f64,
    local_backlog_p90_ms: f64,
    local_backlog_p99_ms: f64,
    ref_decode_p50_ms: f64,
    ref_decode_p90_ms: f64,
    ref_decode_p99_ms: f64,
    /// `book_top_lag_ms` distribution *for the same primary-delay executed shots* that have a
    /// measured `ack_only_ms` (i.e. same sample set as capturable_window).
    book_top_lag_at_ack_p50_ms: f64,
    book_top_lag_at_ack_p90_ms: f64,
    book_top_lag_at_ack_p99_ms: f64,
    book_top_lag_at_ack_n: u64,
    capturable_window_p50_ms: f64,
    capturable_window_p75_ms: f64,
    capturable_window_p90_ms: f64,
    capturable_window_p99_ms: f64,
    /// Ratio of samples where capturable_window_ms > 0.0.
    profitable_window_ratio: f64,
    /// Number of capturable window samples included in the distribution.
    capturable_window_n: u64,
    /// Number of ack_only_ms samples (only meaningful in live execution).
    ack_only_n: u64,
    /// Number of tick_to_ack_ms samples.
    tick_to_ack_n: u64,
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
    // Survival probe is an "orderless" latency-arb competitiveness metric:
    // at T0 we observe a crossable top-of-book price, then check at +Δ whether
    // it is still crossable. This intentionally does not depend on order acks.
    survival_probe_5ms: f64,
    survival_probe_10ms: f64,
    survival_probe_25ms: f64,
    survival_probe_5ms_n: u64,
    survival_probe_10ms_n: u64,
    survival_probe_25ms_n: u64,
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
    lag_half_life_ms: f64,
    ack_only_p50_ms: f64,
    ack_only_p90_ms: f64,
    ack_only_p99_ms: f64,
    strategy_uptime_pct: f64,
    tick_to_ack_p99_ms: f64,
    ref_ticks_total: u64,
    book_ticks_total: u64,
    ref_freshness_ms: i64,
    book_freshness_ms: i64,
    book_top_lag_p50_ms: f64,
    book_top_lag_p90_ms: f64,
    book_top_lag_p99_ms: f64,
    book_top_lag_by_symbol_p50_ms: HashMap<String, f64>,
    survival_10ms_by_symbol: HashMap<String, f64>,
    survival_probe_10ms_by_symbol: HashMap<String, f64>,
    blocked_reason_counts: HashMap<String, u64>,
    source_mix_ratio: HashMap<String, f64>,
    exit_reason_top: Vec<(String, u64)>,
    edge_model_version: String,
    latency: LatencyBreakdown,
    market_scorecard: Vec<MarketScoreRow>,
    predator_c_enabled: bool,
    direction_signals_up: u64,
    direction_signals_down: u64,
    direction_signals_neutral: u64,
    taker_sniper_fired: u64,
    taker_sniper_skipped: u64,
    taker_sniper_skip_reasons_top: Vec<(String, u64)>,
    router_locked_by_tf_usdc: HashMap<String, f64>,
    capital_available_usdc: f64,
    capital_base_quote_size: f64,
    capital_halt: bool,
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

#[derive(Debug, Clone, Copy, Default)]
struct SurvivalProbeCounters {
    n_5: u64,
    s_5: u64,
    n_10: u64,
    s_10: u64,
    n_25: u64,
    s_25: u64,
}

impl SurvivalProbeCounters {
    fn record(&mut self, delay_ms: u64, survived: bool) {
        let (n, s) = match delay_ms {
            5 => (&mut self.n_5, &mut self.s_5),
            10 => (&mut self.n_10, &mut self.s_10),
            25 => (&mut self.n_25, &mut self.s_25),
            _ => return,
        };
        *n = n.saturating_add(1);
        if survived {
            *s = s.saturating_add(1);
        }
    }

    fn ratio(&self, delay_ms: u64) -> f64 {
        let (n, s) = match delay_ms {
            5 => (self.n_5, self.s_5),
            10 => (self.n_10, self.s_10),
            25 => (self.n_25, self.s_25),
            _ => (0, 0),
        };
        if n == 0 {
            0.0
        } else {
            s as f64 / n as f64
        }
    }

    fn n(&self, delay_ms: u64) -> u64 {
        match delay_ms {
            5 => self.n_5,
            10 => self.n_10,
            25 => self.n_25,
            _ => 0,
        }
    }
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
    exit_reasons: RwLock<HashMap<String, u64>>,
    ref_ticks_total: AtomicU64,
    book_ticks_total: AtomicU64,
    ref_source_counts: RwLock<HashMap<String, u64>>,
    ref_dedupe_dropped: AtomicU64,
    last_ref_tick_ms: AtomicI64,
    last_book_tick_ms: AtomicI64,
    shots: RwLock<Vec<ShadowShot>>,
    outcomes: RwLock<Vec<ShadowOutcome>>,
    samples: RwLock<ShadowSamples>,
    book_top_lag_by_symbol_ms: RwLock<HashMap<String, Vec<f64>>>,
    survival_probe_overall: RwLock<SurvivalProbeCounters>,
    survival_probe_by_symbol: RwLock<HashMap<String, SurvivalProbeCounters>>,
    data_total: AtomicU64,
    data_invalid: AtomicU64,
    seq_gap: AtomicU64,
    ts_inversion: AtomicU64,
    stale_tick_dropped: AtomicU64,
    loss_streak: AtomicU64,
    observe_only: AtomicBool,
    paused: AtomicBool,
    paused_since_ms: AtomicU64,
    paused_total_ms: AtomicU64,
    predator_c_enabled: AtomicBool,
    predator_dir_up: AtomicU64,
    predator_dir_down: AtomicU64,
    predator_dir_neutral: AtomicU64,
    predator_taker_fired: AtomicU64,
    predator_taker_skipped: AtomicU64,
    predator_taker_skip_reasons: RwLock<HashMap<String, u64>>,
    predator_router_locked_by_tf_usdc: RwLock<HashMap<String, f64>>,
    predator_capital: RwLock<CapitalUpdate>,
    predator_capital_halt: AtomicBool,
}

#[derive(Debug, Clone, Default)]
struct ShadowSamples {
    decision_queue_wait_ms: Vec<f64>,
    decision_compute_ms: Vec<f64>,
    tick_to_decision_ms: Vec<f64>,
    ack_only_ms: Vec<f64>,
    tick_to_ack_ms: Vec<f64>,
    capturable_window_ms: Vec<f64>,
    feed_in_ms: Vec<f64>,
    source_latency_ms: Vec<f64>,
    book_latency_ms: Vec<f64>,
    local_backlog_ms: Vec<f64>,
    ref_decode_ms: Vec<f64>,
    book_top_lag_ms: Vec<f64>,
    signal_us: Vec<f64>,
    quote_us: Vec<f64>,
    risk_us: Vec<f64>,
    shadow_fill_ms: Vec<f64>,
    queue_depth: Vec<f64>,
    event_backlog: Vec<f64>,
    parse_us: Vec<f64>,
    io_queue_depth: Vec<f64>,
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
            exit_reasons: RwLock::new(HashMap::new()),
            ref_ticks_total: AtomicU64::new(0),
            book_ticks_total: AtomicU64::new(0),
            ref_source_counts: RwLock::new(HashMap::new()),
            ref_dedupe_dropped: AtomicU64::new(0),
            last_ref_tick_ms: AtomicI64::new(0),
            last_book_tick_ms: AtomicI64::new(0),
            shots: RwLock::new(Vec::new()),
            outcomes: RwLock::new(Vec::new()),
            samples: RwLock::new(ShadowSamples::default()),
            book_top_lag_by_symbol_ms: RwLock::new(HashMap::new()),
            survival_probe_overall: RwLock::new(SurvivalProbeCounters::default()),
            survival_probe_by_symbol: RwLock::new(HashMap::new()),
            data_total: AtomicU64::new(0),
            data_invalid: AtomicU64::new(0),
            seq_gap: AtomicU64::new(0),
            ts_inversion: AtomicU64::new(0),
            stale_tick_dropped: AtomicU64::new(0),
            loss_streak: AtomicU64::new(0),
            observe_only: AtomicBool::new(false),
            paused: AtomicBool::new(false),
            paused_since_ms: AtomicU64::new(0),
            paused_total_ms: AtomicU64::new(0),
            predator_c_enabled: AtomicBool::new(false),
            predator_dir_up: AtomicU64::new(0),
            predator_dir_down: AtomicU64::new(0),
            predator_dir_neutral: AtomicU64::new(0),
            predator_taker_fired: AtomicU64::new(0),
            predator_taker_skipped: AtomicU64::new(0),
            predator_taker_skip_reasons: RwLock::new(HashMap::new()),
            predator_router_locked_by_tf_usdc: RwLock::new(HashMap::new()),
            predator_capital: RwLock::new(CapitalUpdate {
                available_usdc: 0.0,
                base_quote_size: 0.0,
                ts_ms: 0,
            }),
            predator_capital_halt: AtomicBool::new(false),
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
        self.ref_dedupe_dropped.store(0, Ordering::Relaxed);
        self.last_ref_tick_ms.store(0, Ordering::Relaxed);
        self.last_book_tick_ms.store(0, Ordering::Relaxed);
        self.blocked_reasons.write().await.clear();
        self.exit_reasons.write().await.clear();
        self.ref_source_counts.write().await.clear();
        self.shots.write().await.clear();
        self.outcomes.write().await.clear();
        *self.samples.write().await = ShadowSamples::default();
        self.book_top_lag_by_symbol_ms.write().await.clear();
        *self.survival_probe_overall.write().await = SurvivalProbeCounters::default();
        self.survival_probe_by_symbol.write().await.clear();
        self.data_total.store(0, Ordering::Relaxed);
        self.data_invalid.store(0, Ordering::Relaxed);
        self.seq_gap.store(0, Ordering::Relaxed);
        self.ts_inversion.store(0, Ordering::Relaxed);
        self.stale_tick_dropped.store(0, Ordering::Relaxed);
        self.loss_streak.store(0, Ordering::Relaxed);
        self.observe_only.store(false, Ordering::Relaxed);
        self.paused.store(false, Ordering::Relaxed);
        self.paused_since_ms.store(0, Ordering::Relaxed);
        self.paused_total_ms.store(0, Ordering::Relaxed);
        self.predator_c_enabled.store(false, Ordering::Relaxed);
        self.predator_dir_up.store(0, Ordering::Relaxed);
        self.predator_dir_down.store(0, Ordering::Relaxed);
        self.predator_dir_neutral.store(0, Ordering::Relaxed);
        self.predator_taker_fired.store(0, Ordering::Relaxed);
        self.predator_taker_skipped.store(0, Ordering::Relaxed);
        self.predator_taker_skip_reasons.write().await.clear();
        self.predator_router_locked_by_tf_usdc.write().await.clear();
        *self.predator_capital.write().await = CapitalUpdate {
            available_usdc: 0.0,
            base_quote_size: 0.0,
            ts_ms: 0,
        };
        self.predator_capital_halt.store(false, Ordering::Relaxed);
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
        // Loss-streak only considers primary delay (10ms) *fillable* outcomes, so "no fills"
        // does not trip risk controls.
        const PRIMARY_DELAY_MS: u64 = 10;
        if outcome.delay_ms == PRIMARY_DELAY_MS && outcome.fillable {
            if outcome.net_markout_10s_usdc.unwrap_or(0.0) < 0.0 {
                self.loss_streak.fetch_add(1, Ordering::Relaxed);
            } else {
                self.loss_streak.store(0, Ordering::Relaxed);
            }
        }
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

    fn loss_streak(&self) -> u32 {
        self.loss_streak.load(Ordering::Relaxed).min(u32::MAX as u64) as u32
    }

    async fn window_outcomes_len(&self) -> usize {
        self.outcomes.read().await.len()
    }

    async fn push_tick_to_decision_ms(&self, ms: f64) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.tick_to_decision_ms, ms, Self::SAMPLE_CAP);
    }

    async fn push_depth_sample(&self, event_backlog: f64, queue_depth: f64, io_queue_depth: f64) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.event_backlog, event_backlog, Self::SAMPLE_CAP);
        push_capped(&mut s.queue_depth, queue_depth, Self::SAMPLE_CAP);
        push_capped(&mut s.io_queue_depth, io_queue_depth, Self::SAMPLE_CAP);
    }

    async fn push_latency_sample(
        &self,
        feed_in_ms: f64,
        source_latency_ms: f64,
        book_latency_ms: f64,
        local_backlog_ms: f64,
        ref_decode_ms: f64,
    ) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.feed_in_ms, feed_in_ms, Self::SAMPLE_CAP);
        push_capped(&mut s.source_latency_ms, source_latency_ms, Self::SAMPLE_CAP);
        push_capped(&mut s.book_latency_ms, book_latency_ms, Self::SAMPLE_CAP);
        push_capped(&mut s.local_backlog_ms, local_backlog_ms, Self::SAMPLE_CAP);
        push_capped(&mut s.ref_decode_ms, ref_decode_ms, Self::SAMPLE_CAP);
        // Contract: decision_queue_wait is the queue/backlog time we spent waiting locally.
        push_capped(&mut s.decision_queue_wait_ms, local_backlog_ms, Self::SAMPLE_CAP);
    }

    async fn push_decision_compute_ms(&self, ms: f64) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.decision_compute_ms, ms, Self::SAMPLE_CAP);
    }

    async fn push_ack_only_ms(&self, ms: f64) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.ack_only_ms, ms, Self::SAMPLE_CAP);
    }

    async fn push_tick_to_ack_ms(&self, ms: f64) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.tick_to_ack_ms, ms, Self::SAMPLE_CAP);
    }

    async fn push_capturable_window_ms(&self, ms: f64) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.capturable_window_ms, ms, Self::SAMPLE_CAP);
    }

    async fn push_book_top_lag_ms(&self, symbol: &str, ms: f64) {
        {
            let mut s = self.samples.write().await;
            push_capped(&mut s.book_top_lag_ms, ms, Self::SAMPLE_CAP);
        }

        let mut by_symbol = self.book_top_lag_by_symbol_ms.write().await;
        let entry = by_symbol.entry(symbol.to_string()).or_default();
        push_capped(entry, ms, 4_096);
    }

    async fn record_survival_probe(&self, symbol: &str, delay_ms: u64, survived: bool) {
        {
            let mut c = self.survival_probe_overall.write().await;
            c.record(delay_ms, survived);
        }
        let mut by_symbol = self.survival_probe_by_symbol.write().await;
        let entry = by_symbol.entry(symbol.to_string()).or_default();
        entry.record(delay_ms, survived);
    }

    async fn push_signal_us(&self, us: f64) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.signal_us, us, Self::SAMPLE_CAP);
    }

    async fn push_quote_us(&self, us: f64) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.quote_us, us, Self::SAMPLE_CAP);
    }

    async fn push_risk_us(&self, us: f64) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.risk_us, us, Self::SAMPLE_CAP);
    }

    async fn push_shadow_fill_ms(&self, ms: f64) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.shadow_fill_ms, ms, Self::SAMPLE_CAP);
    }

    async fn push_parse_us(&self, us: f64) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.parse_us, us, Self::SAMPLE_CAP);
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

    async fn mark_ref_tick(&self, source: &str, ts_ms: i64) {
        self.ref_ticks_total.fetch_add(1, Ordering::Relaxed);
        self.last_ref_tick_ms.store(ts_ms, Ordering::Relaxed);
        let mut counts = self.ref_source_counts.write().await;
        *counts.entry(source.to_string()).or_insert(0) += 1;
    }

    fn mark_ref_dedupe_dropped(&self) {
        self.ref_dedupe_dropped.fetch_add(1, Ordering::Relaxed);
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

    async fn record_exit_reason(&self, reason: &str) {
        let mut reasons = self.exit_reasons.write().await;
        *reasons.entry(reason.to_string()).or_insert(0) += 1;
    }

    fn observe_only(&self) -> bool {
        self.observe_only.load(Ordering::Relaxed)
    }

    fn set_observe_only(&self, v: bool) {
        self.observe_only.store(v, Ordering::Relaxed);
    }

    fn set_paused(&self, v: bool) {
        let now_ms = Utc::now().timestamp_millis().max(0) as u64;
        let was = self.paused.swap(v, Ordering::Relaxed);
        if was == v {
            return;
        }
        if v {
            self.paused_since_ms.store(now_ms, Ordering::Relaxed);
            return;
        }
        let since = self.paused_since_ms.swap(0, Ordering::Relaxed);
        if since > 0 {
            self.paused_total_ms
                .fetch_add(now_ms.saturating_sub(since), Ordering::Relaxed);
        }
    }

    fn set_predator_enabled(&self, v: bool) {
        self.predator_c_enabled.store(v, Ordering::Relaxed);
    }

    fn mark_predator_direction(&self, dir: &Direction) {
        match dir {
            Direction::Up => {
                self.predator_dir_up.fetch_add(1, Ordering::Relaxed);
            }
            Direction::Down => {
                self.predator_dir_down.fetch_add(1, Ordering::Relaxed);
            }
            Direction::Neutral => {
                self.predator_dir_neutral.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn mark_predator_taker_fired(&self) {
        self.predator_taker_fired.fetch_add(1, Ordering::Relaxed);
    }

    async fn mark_predator_taker_skipped(&self, reason: &str) {
        self.predator_taker_skipped.fetch_add(1, Ordering::Relaxed);
        let mut reasons = self.predator_taker_skip_reasons.write().await;
        *reasons.entry(reason.to_string()).or_insert(0) += 1;
    }

    async fn set_predator_router_locked_by_tf_usdc(&self, locked: HashMap<String, f64>) {
        *self.predator_router_locked_by_tf_usdc.write().await = locked;
    }

    async fn set_predator_capital(&self, update: CapitalUpdate, halt: bool) {
        *self.predator_capital.write().await = update;
        self.predator_capital_halt.store(halt, Ordering::Relaxed);
    }

    fn uptime_pct(&self, elapsed: Duration) -> f64 {
        let elapsed_ms = elapsed.as_millis() as u64;
        if elapsed_ms == 0 {
            return 100.0;
        }
        let now_ms = Utc::now().timestamp_millis().max(0) as u64;
        let base_paused = self.paused_total_ms.load(Ordering::Relaxed);
        let paused_extra = if self.paused.load(Ordering::Relaxed) {
            let since = self.paused_since_ms.load(Ordering::Relaxed);
            if since > 0 {
                now_ms.saturating_sub(since)
            } else {
                0
            }
        } else {
            0
        };
        let paused_ms = base_paused.saturating_add(paused_extra).min(elapsed_ms);
        let uptime_ms = elapsed_ms.saturating_sub(paused_ms);
        ((uptime_ms as f64) * 100.0 / elapsed_ms as f64).clamp(0.0, 100.0)
    }
}

impl ShadowStats {
    async fn build_live_report(&self) -> ShadowLiveReport {
        const PRIMARY_DELAY_MS: u64 = 10;
        let ShadowSamples {
            decision_queue_wait_ms,
            decision_compute_ms,
            tick_to_decision_ms,
            ack_only_ms,
            tick_to_ack_ms,
            capturable_window_ms,
            feed_in_ms,
            source_latency_ms,
            book_latency_ms,
            local_backlog_ms,
            ref_decode_ms,
            book_top_lag_ms,
            signal_us,
            quote_us,
            risk_us,
            shadow_fill_ms,
            queue_depth,
            event_backlog,
            parse_us,
            io_queue_depth,
        } = self.samples.read().await.clone();
        let book_top_lag_by_symbol_ms = self.book_top_lag_by_symbol_ms.read().await.clone();
        let mut blocked_reason_counts = self.blocked_reasons.read().await.clone();
        let source_counts = self.ref_source_counts.read().await.clone();
        let exit_reason_counts = self.exit_reasons.read().await.clone();
        let survival_probe_overall = *self.survival_probe_overall.read().await;
        let survival_probe_by_symbol = self.survival_probe_by_symbol.read().await.clone();
        let elapsed = self.started_at.read().await.elapsed();
        let uptime_pct = self.uptime_pct(elapsed);

        // Avoid cloning large shot/outcome vectors per request. Under stress polling this can
        // cause massive allocation churn and even OOM kills (esp. when market_scorecard is large).
        let shots_guard = self.shots.read().await;
        let outcomes_guard = self.outcomes.read().await;

        let fillability_5 = fillability_ratio(&outcomes_guard, 5);
        let fillability_10 = fillability_ratio(&outcomes_guard, 10);
        let fillability_25 = fillability_ratio(&outcomes_guard, 25);
        let survival_5 = survival_ratio(&outcomes_guard, 5);
        let survival_10 = survival_ratio(&outcomes_guard, 10);
        let survival_25 = survival_ratio(&outcomes_guard, 25);
        let survival_probe_5 = survival_probe_overall.ratio(5);
        let survival_probe_10 = survival_probe_overall.ratio(10);
        let survival_probe_25 = survival_probe_overall.ratio(25);
        let survival_probe_5_n = survival_probe_overall.n(5);
        let survival_probe_10_n = survival_probe_overall.n(10);
        let survival_probe_25_n = survival_probe_overall.n(25);

        let shots_primary = shots_guard
            .iter()
            .filter(|s| s.delay_ms == PRIMARY_DELAY_MS)
            .collect::<Vec<_>>();
        let outcomes_primary = outcomes_guard
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
        let quote_block_ratio = quote_block_ratio(attempted, blocked);
        let policy_ratio = policy_block_ratio(attempted, policy_blocked);

        let tick_to_ack_p99 = percentile(&tick_to_ack_ms, 0.99).unwrap_or(0.0);
        let scorecard = build_market_scorecard(&shots_guard, &outcomes_guard);
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
        let dedupe_dropped = self.ref_dedupe_dropped.load(Ordering::Relaxed);
        if dedupe_dropped > 0 {
            blocked_reason_counts.insert("ref_dedupe_dropped".to_string(), dedupe_dropped);
        }
        let mut source_mix_ratio = HashMap::new();
        if ref_ticks_total > 0 {
            for (source, cnt) in &source_counts {
                source_mix_ratio.insert(source.clone(), (*cnt as f64 / ref_ticks_total as f64).clamp(0.0, 1.0));
            }
        }
        let mut exit_reason_top = exit_reason_counts.into_iter().collect::<Vec<_>>();
        exit_reason_top.sort_by(|a, b| b.1.cmp(&a.1));
        exit_reason_top.truncate(8);
        let lag_half_life_ms = percentile(
            &book_top_lag_ms
                .iter()
                .copied()
                .filter(|v| v.is_finite() && *v >= 0.0)
                .collect::<Vec<_>>(),
            0.50,
        )
        .unwrap_or(0.0);

        let book_top_lag_p50_ms = percentile(&book_top_lag_ms, 0.50).unwrap_or(0.0);
        let book_top_lag_p90_ms = percentile(&book_top_lag_ms, 0.90).unwrap_or(0.0);
        let book_top_lag_p99_ms = percentile(&book_top_lag_ms, 0.99).unwrap_or(0.0);
        let mut book_top_lag_by_symbol_p50_ms = HashMap::new();
        for (sym, samples) in book_top_lag_by_symbol_ms {
            book_top_lag_by_symbol_p50_ms.insert(sym, percentile(&samples, 0.50).unwrap_or(0.0));
        }

        let mut survival_10ms_by_symbol = HashMap::new();
        let mut survival_counts: HashMap<String, (u64, u64)> = HashMap::new();
        for o in &outcomes_primary_valid {
            let o = *o;
            let e = survival_counts.entry(o.symbol.clone()).or_insert((0, 0));
            e.0 = e.0.saturating_add(1);
            if o.survived {
                e.1 = e.1.saturating_add(1);
            }
        }
        for (sym, (n, s)) in survival_counts {
            survival_10ms_by_symbol.insert(sym, if n == 0 { 0.0 } else { s as f64 / n as f64 });
        }
        let mut survival_probe_10ms_by_symbol = HashMap::new();
        for (sym, c) in survival_probe_by_symbol {
            survival_probe_10ms_by_symbol.insert(sym, c.ratio(10));
        }

        let shots_primary_at_ack = shots_primary
            .iter()
            .copied()
            .filter(|s| s.ack_only_ms > 0.0)
            .collect::<Vec<_>>();
        let book_top_lag_at_ack_ms = shots_primary_at_ack
            .iter()
            .map(|s| s.book_top_lag_ms)
            .collect::<Vec<_>>();

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
            book_latency_p50_ms: percentile(&book_latency_ms, 0.50).unwrap_or(0.0),
            book_latency_p90_ms: percentile(&book_latency_ms, 0.90).unwrap_or(0.0),
            book_latency_p99_ms: percentile(&book_latency_ms, 0.99).unwrap_or(0.0),
            local_backlog_p50_ms: percentile(&local_backlog_ms, 0.50).unwrap_or(0.0),
            local_backlog_p90_ms: percentile(&local_backlog_ms, 0.90).unwrap_or(0.0),
            local_backlog_p99_ms: percentile(&local_backlog_ms, 0.99).unwrap_or(0.0),
            ref_decode_p50_ms: percentile(&ref_decode_ms, 0.50).unwrap_or(0.0),
            ref_decode_p90_ms: percentile(&ref_decode_ms, 0.90).unwrap_or(0.0),
            ref_decode_p99_ms: percentile(&ref_decode_ms, 0.99).unwrap_or(0.0),
            book_top_lag_at_ack_p50_ms: percentile(&book_top_lag_at_ack_ms, 0.50).unwrap_or(0.0),
            book_top_lag_at_ack_p90_ms: percentile(&book_top_lag_at_ack_ms, 0.90).unwrap_or(0.0),
            book_top_lag_at_ack_p99_ms: percentile(&book_top_lag_at_ack_ms, 0.99).unwrap_or(0.0),
            book_top_lag_at_ack_n: book_top_lag_at_ack_ms.len() as u64,
            capturable_window_p50_ms: percentile(&capturable_window_ms, 0.50).unwrap_or(0.0),
            capturable_window_p75_ms: percentile(&capturable_window_ms, 0.75).unwrap_or(0.0),
            capturable_window_p90_ms: percentile(&capturable_window_ms, 0.90).unwrap_or(0.0),
            capturable_window_p99_ms: percentile(&capturable_window_ms, 0.99).unwrap_or(0.0),
            profitable_window_ratio: if capturable_window_ms.is_empty() {
                0.0
            } else {
                capturable_window_ms.iter().filter(|v| **v > 0.0).count() as f64
                    / capturable_window_ms.len() as f64
            },
            capturable_window_n: capturable_window_ms.len() as u64,
            ack_only_n: ack_only_ms.len() as u64,
            tick_to_ack_n: tick_to_ack_ms.len() as u64,
        };

        let total_shots = shots_guard.len();
        let total_outcomes = outcomes_guard.len();

        // IMPORTANT: drop large read-guards before awaiting on other locks below.
        drop(shots_guard);
        drop(outcomes_guard);

        let predator_c_enabled = self.predator_c_enabled.load(Ordering::Relaxed);
        let direction_signals_up = self.predator_dir_up.load(Ordering::Relaxed);
        let direction_signals_down = self.predator_dir_down.load(Ordering::Relaxed);
        let direction_signals_neutral = self.predator_dir_neutral.load(Ordering::Relaxed);
        let taker_sniper_fired = self.predator_taker_fired.load(Ordering::Relaxed);
        let taker_sniper_skipped = self.predator_taker_skipped.load(Ordering::Relaxed);
        let skip_reasons = self.predator_taker_skip_reasons.read().await.clone();
        let mut skip_top = skip_reasons.into_iter().collect::<Vec<_>>();
        skip_top.sort_by(|a, b| b.1.cmp(&a.1));
        skip_top.truncate(10);
        let router_locked_by_tf_usdc = self.predator_router_locked_by_tf_usdc.read().await.clone();
        let capital = self.predator_capital.read().await.clone();
        let capital_halt = self.predator_capital_halt.load(Ordering::Relaxed);

        let mut live = ShadowLiveReport {
            window_id: self.window_id.load(Ordering::Relaxed),
            window_shots: total_shots,
            window_outcomes: total_outcomes,
            gate_ready: total_outcomes >= Self::GATE_MIN_OUTCOMES,
            gate_fail_reasons: Vec::new(),
            observe_only: self.observe_only(),
            started_at_ms: self.started_at_ms.load(Ordering::Relaxed),
            elapsed_sec: elapsed.as_secs(),
            total_shots,
            total_outcomes,
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
            survival_5ms: survival_5,
            survival_10ms: survival_10,
            survival_25ms: survival_25,
            survival_probe_5ms: survival_probe_5,
            survival_probe_10ms: survival_probe_10,
            survival_probe_25ms: survival_probe_25,
            survival_probe_5ms_n: survival_probe_5_n,
            survival_probe_10ms_n: survival_probe_10_n,
            survival_probe_25ms_n: survival_probe_25_n,
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
            quote_block_ratio,
            policy_block_ratio: policy_ratio,
            queue_depth_p99: percentile(&queue_depth, 0.99).unwrap_or(0.0),
            event_backlog_p99: percentile(&event_backlog, 0.99).unwrap_or(0.0),
            tick_to_decision_p50_ms: latency.tick_to_decision_p50_ms,
            tick_to_decision_p90_ms: latency.tick_to_decision_p90_ms,
            tick_to_decision_p99_ms: latency.tick_to_decision_p99_ms,
            decision_queue_wait_p99_ms: latency.decision_queue_wait_p99_ms,
            decision_compute_p99_ms: latency.decision_compute_p99_ms,
            source_latency_p99_ms: latency.source_latency_p99_ms,
            local_backlog_p99_ms: latency.local_backlog_p99_ms,
            lag_half_life_ms,
            ack_only_p50_ms: latency.ack_only_p50_ms,
            ack_only_p90_ms: latency.ack_only_p90_ms,
            ack_only_p99_ms: latency.ack_only_p99_ms,
            strategy_uptime_pct: uptime_pct,
            tick_to_ack_p99_ms: tick_to_ack_p99,
            ref_ticks_total,
            book_ticks_total,
            ref_freshness_ms,
            book_freshness_ms,
            book_top_lag_p50_ms,
            book_top_lag_p90_ms,
            book_top_lag_p99_ms,
            book_top_lag_by_symbol_p50_ms,
            survival_10ms_by_symbol,
            survival_probe_10ms_by_symbol,
            blocked_reason_counts,
            source_mix_ratio,
            exit_reason_top,
            edge_model_version: "unknown".to_string(),
            latency,
            market_scorecard: scorecard,
            predator_c_enabled,
            direction_signals_up,
            direction_signals_down,
            direction_signals_neutral,
            taker_sniper_fired,
            taker_sniper_skipped,
            taker_sniper_skip_reasons_top: skip_top,
            router_locked_by_tf_usdc,
            capital_available_usdc: capital.available_usdc,
            capital_base_quote_size: capital.base_quote_size,
            capital_halt,
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
    let _guard = init_tracing("app_runner");
    let prometheus = init_metrics();
    ensure_dataset_dirs();

    let execution_cfg = load_execution_config();
    let universe_cfg = load_universe_config();
    let bus = RingBus::new(16_384);
    let portfolio = Arc::new(PortfolioBook::default());
    let live_armed = std::env::var("POLYEDGE_LIVE_ARMED")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    let exec_mode = if execution_cfg.mode.eq_ignore_ascii_case("live") && live_armed {
        ExecutionMode::Live
    } else {
        if execution_cfg.mode.eq_ignore_ascii_case("live") && !live_armed {
            tracing::warn!(
                "execution.mode=live but POLYEDGE_LIVE_ARMED is not true; forcing paper mode"
            );
        }
        ExecutionMode::Paper
    };
    let order_endpoint = execution_cfg
        .order_endpoint
        .clone()
        .unwrap_or_else(|| execution_cfg.clob_endpoint.clone());
    let execution = Arc::new(ClobExecution::new_with_timeout(
        exec_mode,
        order_endpoint,
        Duration::from_millis(execution_cfg.http_timeout_ms),
    ));

    // Optional: prewarm the execution HTTP client pool to reduce first-ack latency spikes.
    // Uses the *same* reqwest client inside the execution layer (unlike ad-hoc curl probes).
    if let Ok(raw) = std::env::var("POLYEDGE_HTTP_PREWARM_URLS") {
        let urls = raw
            .split(',')
            .map(str::trim)
            .filter(|u| u.starts_with("http://") || u.starts_with("https://"))
            .map(|u| u.to_string())
            .collect::<Vec<_>>();
        if !urls.is_empty() {
            tracing::info!(count = urls.len(), "prewarming execution http pool");
            let exec = execution.clone();
            tokio::spawn(async move {
                exec.prewarm_urls(&urls).await;
            });
        }
    }
    let shadow = Arc::new(ShadowExecutor::default());
    let strategy_cfg = Arc::new(RwLock::new(Arc::new(load_strategy_config())));
    let fusion_cfg = Arc::new(RwLock::new(load_fusion_config()));
    let edge_model_cfg = Arc::new(RwLock::new(load_edge_model_config()));
    let exit_cfg = Arc::new(RwLock::new(load_exit_config()));
    let fair_value_cfg = Arc::new(StdRwLock::new(load_fair_value_config()));
    let toxicity_cfg = Arc::new(RwLock::new(Arc::new(ToxicityConfig::default())));
    let risk_limits = Arc::new(StdRwLock::new(load_risk_limits_config()));
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
    let universe_market_types = Arc::new(universe_cfg.market_types.clone());
    let universe_timeframes = Arc::new(universe_cfg.timeframes.clone());
    init_jsonl_writer(perf_profile.clone()).await;

    let scoring_rebate_factor = std::env::var("POLYEDGE_SCORING_REBATE_FACTOR")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        // Worst-case by default: assume no rebate unless we have hard evidence.
        .unwrap_or(0.0)
        .clamp(0.0, 1.0);

    let risk_manager = Arc::new(DefaultRiskManager::new(risk_limits.clone()));
    let predator_cfg = Arc::new(RwLock::new(load_predator_c_config()));
    let predator_cfg0 = predator_cfg.read().await.clone();
    let predator_direction_detector = Arc::new(RwLock::new(DirectionDetector::new(
        predator_cfg0.direction_detector.clone(),
    )));
    let predator_latest_direction = Arc::new(RwLock::new(HashMap::new()));
    let predator_taker_sniper = Arc::new(RwLock::new(TakerSniper::new(
        predator_cfg0.taker_sniper.clone(),
    )));
    let predator_router = Arc::new(RwLock::new(TimeframeRouter::new(predator_cfg0.router.clone())));
    let predator_compounder = Arc::new(RwLock::new(SettlementCompounder::new(
        predator_cfg0.compounder.clone(),
    )));
    let shared = Arc::new(EngineShared {
        latest_books: Arc::new(RwLock::new(HashMap::new())),
        latest_signals: Arc::new(DashMap::new()),
        market_to_symbol: Arc::new(RwLock::new(HashMap::new())),
        token_to_symbol: Arc::new(RwLock::new(HashMap::new())),
        market_to_timeframe: Arc::new(RwLock::new(HashMap::new())),
        symbol_to_markets: Arc::new(RwLock::new(HashMap::new())),
        fee_cache: Arc::new(RwLock::new(HashMap::new())),
        fee_refresh_inflight: Arc::new(RwLock::new(HashMap::new())),
        scoring_cache: Arc::new(RwLock::new(HashMap::new())),
        scoring_refresh_inflight: Arc::new(RwLock::new(HashMap::new())),
        http: Client::new(),
        clob_endpoint: execution_cfg.clob_endpoint.clone(),
        strategy_cfg,
        fusion_cfg: fusion_cfg.clone(),
        edge_model_cfg: edge_model_cfg.clone(),
        exit_cfg: exit_cfg.clone(),
        fair_value_cfg,
        toxicity_cfg,
        risk_manager,
        universe_symbols: universe_symbols.clone(),
        universe_market_types: universe_market_types.clone(),
        universe_timeframes: universe_timeframes.clone(),
        rate_limit_rps: execution_cfg.rate_limit_rps.max(0.1),
        scoring_rebate_factor,
        tox_state,
        shadow_stats,
        predator_cfg: predator_cfg.clone(),
        predator_direction_detector,
        predator_latest_direction,
        predator_taker_sniper,
        predator_router,
        predator_compounder,
    });

    let state = AppState {
        paused: paused.clone(),
        bus: bus.clone(),
        portfolio: portfolio.clone(),
        execution: execution.clone(),
        _shadow: shadow.clone(),
        prometheus,
        strategy_cfg: shared.strategy_cfg.clone(),
        fair_value_cfg: shared.fair_value_cfg.clone(),
        toxicity_cfg: shared.toxicity_cfg.clone(),
        allocator_cfg: allocator_cfg.clone(),
        risk_limits: risk_limits.clone(),
        tox_state: shared.tox_state.clone(),
        shadow_stats: shared.shadow_stats.clone(),
        perf_profile: perf_profile.clone(),
        shared: shared.clone(),
    };

    spawn_reference_feed(
        bus.clone(),
        shared.shadow_stats.clone(),
        (*universe_symbols).clone(),
        shared.fusion_cfg.clone(),
    );
    spawn_market_feed(
        bus.clone(),
        shared.shadow_stats.clone(),
        (*universe_symbols).clone(),
        (*universe_market_types).clone(),
        (*universe_timeframes).clone(),
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
    orchestration::spawn_data_reconcile_task(
        bus.clone(),
        paused.clone(),
        shared.shadow_stats.clone(),
    );

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
    fusion_cfg: Arc<RwLock<FusionConfig>>,
) {
    #[derive(Clone, Copy)]
    enum RefLane {
        Direct,
        Udp,
    }
    const TS_INVERSION_TOLERANCE_MS: i64 = 250;
    const TS_BACKJUMP_RESET_MS: i64 = 5_000;
    tokio::spawn(async move {
        let symbols = if symbols.is_empty() {
            vec!["BTCUSDT".to_string()]
        } else {
            symbols
        };
        let (tx, mut rx) = mpsc::channel::<(RefLane, Result<RefTick>)>(32_768);

        let tx_direct = tx.clone();
        let symbols_direct = symbols.clone();
        tokio::spawn(async move {
            loop {
                let feed = MultiSourceRefFeed::new(Duration::from_millis(50));
                match feed.stream_ticks(symbols_direct.clone()).await {
                    Ok(mut stream) => {
                        while let Some(item) = stream.next().await {
                            if tx_direct.send((RefLane::Direct, item)).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(err) => tracing::warn!(?err, "direct reference feed failed to start"),
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });

        let tx_udp = tx.clone();
        let symbols_udp = symbols.clone();
        let fusion_cfg_udp = fusion_cfg.clone();
        tokio::spawn(async move {
            loop {
                let cfg = fusion_cfg_udp.read().await.clone();
                if !cfg.enable_udp || cfg.mode == "direct_only" {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
                let feed = feed_udp::UdpBinanceFeed::new(cfg.udp_port);
                match feed.stream_ticks(symbols_udp.clone()).await {
                    Ok(mut stream) => {
                        while let Some(item) = stream.next().await {
                            if tx_udp.send((RefLane::Udp, item)).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(err) => tracing::warn!(?err, "udp reference feed failed to start"),
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
        drop(tx);

        let mut ingest_seq: u64 = 0;
        let mut last_source_ts_by_stream: HashMap<String, i64> = HashMap::new();
        let mut last_published_by_symbol: HashMap<String, (i64, f64)> = HashMap::new();

        while let Some((lane, item)) = rx.recv().await {
            match item {
                Ok(tick) => {
                    let fusion = fusion_cfg.read().await.clone();
                    let is_anchor = is_anchor_ref_source(tick.source.as_str());
                    if fusion.mode == "udp_only" && !matches!(lane, RefLane::Udp) && !is_anchor {
                        continue;
                    }
                    if fusion.mode == "direct_only" && !matches!(lane, RefLane::Direct) && !is_anchor {
                        continue;
                    }
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
                            let back_jump_ms = prev.saturating_sub(source_ts);
                            if back_jump_ms > TS_BACKJUMP_RESET_MS {
                                stats.record_issue("ref_ts_backjump_reset").await;
                            } else {
                                stats.mark_ts_inversion();
                            }
                        }
                    }
                    last_source_ts_by_stream.insert(stream_key, source_ts);
                    if let Some((prev_ts, prev_px)) = last_published_by_symbol.get(&tick.symbol) {
                        if (source_ts - *prev_ts).abs() <= fusion.dedupe_window_ms
                            && (tick.price - *prev_px).abs() <= f64::EPSILON
                        {
                            stats.mark_ref_dedupe_dropped();
                            continue;
                        }
                    }
                    last_published_by_symbol.insert(tick.symbol.clone(), (source_ts, tick.price));
                    stats.mark_ref_tick(tick.source.as_str(), tick.recv_ts_ms).await;
                    // Hot-path logging: avoid dynamic JSON trees + extra stringify passes.
                    // Build a single JSONL line and hand it to the async writer.
                    let tick_json =
                        serde_json::to_string(&tick).unwrap_or_else(|_| "{}".to_string());
                    let hash = sha256_hex(&tick_json);
                    let line = format!(
                        "{{\"ts_ms\":{},\"source_seq\":{},\"ingest_seq\":{},\"valid\":{},\"sha256\":\"{}\",\"tick\":{}}}",
                        Utc::now().timestamp_millis(),
                        source_seq,
                        ingest_seq,
                        valid,
                        hash,
                        tick_json
                    );
                    append_jsonl_line(&dataset_path("raw", "ref_ticks.jsonl"), line);
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

fn spawn_market_feed(
    bus: RingBus<EngineEvent>,
    stats: Arc<ShadowStats>,
    symbols: Vec<String>,
    market_types: Vec<String>,
    timeframes: Vec<String>,
) {
    const TS_INVERSION_TOLERANCE_MS: i64 = 250;
    const TS_BACKJUMP_RESET_MS: i64 = 5_000;
    // If we see *no* market messages for this long, treat the WS stream as stuck and reconnect.
    // Keep this comfortably above "normal quiet" to avoid hammering gamma discovery on reconnection.
    // 5s is too aggressive for quiet windows and creates reconnect churn.
    // Keep stale protection, but allow a calmer idle window to reduce needless reconnects.
    const BOOK_IDLE_TIMEOUT_MS: u64 = 20_000;
    const RECONNECT_BASE_MS: u64 = 250;
    const RECONNECT_MAX_MS: u64 = 10_000;
    tokio::spawn(async move {
        let mut reconnects: u64 = 0;
        loop {
            let feed = PolymarketFeed::new_with_universe(
                Duration::from_millis(50),
                symbols.clone(),
                market_types.clone(),
                timeframes.clone(),
            );
            let Ok(mut stream) = feed.stream_books().await else {
                reconnects = reconnects.saturating_add(1);
                let backoff_ms =
                    (RECONNECT_BASE_MS.saturating_mul(reconnects.min(40))).min(RECONNECT_MAX_MS);
                tracing::warn!(
                    reconnects,
                    backoff_ms,
                    "market feed failed to start; reconnecting"
                );
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                continue;
            };
            let mut ingest_seq: u64 = 0;
            let mut last_source_ts_by_market: HashMap<String, i64> = HashMap::new();
            // Only reset reconnect backoff once we observe actual book traffic (not just a successful
            // handshake). This avoids a reconnect storm when discovery/WS is returning 200 but no
            // updates are delivered.
            let mut saw_any_book = false;

            loop {
                let next = tokio::time::timeout(
                    Duration::from_millis(BOOK_IDLE_TIMEOUT_MS),
                    stream.next(),
                )
                .await;
                let item = match next {
                    Ok(v) => v,
                    Err(_) => {
                        tracing::warn!(
                            timeout_ms = BOOK_IDLE_TIMEOUT_MS,
                            saw_any_book,
                            reconnects,
                            "market feed idle timeout; reconnecting"
                        );
                        break;
                    }
                };

                let Some(item) = item else {
                    tracing::warn!("market feed stream ended; reconnecting");
                    break;
                };

                match item {
                    Ok(book) => {
                        if !saw_any_book {
                            saw_any_book = true;
                            if reconnects > 0 {
                                tracing::info!(reconnects, "market feed reconnected");
                            }
                            reconnects = 0;
                        }
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
                                let back_jump_ms = prev.saturating_sub(source_ts);
                                if back_jump_ms > TS_BACKJUMP_RESET_MS {
                                    stats.record_issue("book_ts_backjump_reset").await;
                                } else {
                                    stats.mark_ts_inversion();
                                }
                            }
                        }
                        last_source_ts_by_market.insert(book.market_id.clone(), source_ts);
                        // IMPORTANT: book freshness must be based on *local* receive time, not the
                        // exchange/server-provided ts_ms (which can be skewed/backjump).
                        let book_tick_ms = if book.recv_ts_local_ns > 0 {
                            (book.recv_ts_local_ns / 1_000_000).max(0)
                        } else {
                            Utc::now().timestamp_millis()
                        };
                        stats.mark_book_tick(book_tick_ms);
                        let book_json =
                            serde_json::to_string(&book).unwrap_or_else(|_| "{}".to_string());
                        let hash = sha256_hex(&book_json);
                        let line = format!(
                            "{{\"ts_ms\":{},\"source_seq\":{},\"ingest_seq\":{},\"valid\":{},\"sha256\":\"{}\",\"book\":{}}}",
                            Utc::now().timestamp_millis(),
                            source_seq,
                            ingest_seq,
                            valid,
                            hash,
                            book_json
                        );
                        append_jsonl_line(&dataset_path("raw", "book_tops.jsonl"), line);
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

            reconnects = reconnects.saturating_add(1);
            let backoff_ms =
                (RECONNECT_BASE_MS.saturating_mul(reconnects.min(40))).min(RECONNECT_MAX_MS);
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
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
        // Separate fast reference ticks (exchange WS) from anchor ticks (Chainlink RTDS).
        // Fast ticks drive stale filtering + fair value evaluation; anchor ticks are tracked for
        // auditing/diagnostics so the trigger stays latency-sensitive.
        let mut latest_fast_ticks: HashMap<String, RefTick> = HashMap::new();
        let mut latest_anchor_ticks: HashMap<String, RefTick> = HashMap::new();
        let mut market_inventory: HashMap<String, InventoryState> = HashMap::new();
        let mut market_rate_budget: HashMap<String, TokenBucket> = HashMap::new();
        let mut untracked_issue_cooldown: HashMap<String, Instant> = HashMap::new();
        let mut book_lag_sample_at: HashMap<String, Instant> = HashMap::new();
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
                    if !is_anchor_ref_source(tick.source.as_str()) {
                        shared
                            .predator_direction_detector
                            .write()
                            .await
                            .on_tick(&tick);
                    }
                    insert_latest_ref_tick(&mut latest_fast_ticks, &mut latest_anchor_ticks, tick);
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
                                if !is_anchor_ref_source(next_tick.source.as_str()) {
                                    shared
                                        .predator_direction_detector
                                        .write()
                                        .await
                                        .on_tick(&next_tick);
                                }
                                insert_latest_ref_tick(
                                    &mut latest_fast_ticks,
                                    &mut latest_anchor_ticks,
                                    next_tick,
                                );
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
                    metrics::histogram!("runtime.event_backlog_depth").record(backlog_depth);
                    let queue_depth = execution.open_orders_count() as f64;
                    metrics::histogram!("runtime.open_order_depth").record(queue_depth);
                    let io_depth = current_jsonl_queue_depth() as f64;
                    shared
                        .shadow_stats
                        .push_depth_sample(backlog_depth, queue_depth, io_depth)
                        .await;
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
                            let now = Instant::now();
                            let should_record = untracked_issue_cooldown
                                .get(&book.market_id)
                                .map(|last| last.elapsed() >= Duration::from_secs(60))
                                .unwrap_or(true);
                            if should_record {
                                shared.shadow_stats.record_issue("market_untracked").await;
                                untracked_issue_cooldown.insert(book.market_id.clone(), now);
                            }
                        }
                        if last_symbol_retry_refresh.elapsed() >= Duration::from_secs(15) {
                            refresh_market_symbol_map(&shared).await;
                            last_symbol_retry_refresh = Instant::now();
                        }
                        continue;
                    };
                    let tick_fast = pick_latest_tick(&latest_fast_ticks, &symbol);
                    let Some(tick_fast) = tick_fast else {
                        shared.shadow_stats.record_issue("tick_missing").await;
                        continue;
                    };
                    let tick_anchor = pick_latest_tick(&latest_anchor_ticks, &symbol);
                    if let Some(anchor) = tick_anchor {
                        let now_ms = Utc::now().timestamp_millis();
                        let age_ms = now_ms - ref_event_ts_ms(anchor);
                        if age_ms > 5_000 {
                            shared.shadow_stats.record_issue("anchor_stale").await;
                        }
                    } else {
                        shared.shadow_stats.record_issue("anchor_missing").await;
                    }
                    // For latency-sensitive trading, evaluate fair value on the fastest observable
                    // reference tick. The Chainlink anchor is tracked for correctness auditing and
                    // can be used for future calibration, but should not slow down the trigger.
                    let eval_tick = tick_fast;
                    shared.shadow_stats.mark_seen();

                    // Positive value means: our fast reference tick arrived earlier than the
                    // Polymarket book update (i.e. the exploitable lag window).
                    //
                    // Guardrail: if the tick is already old, this is not a meaningful "lag window"
                    // measurement and would inflate p50/p99. We only sample when the tick is fresh.
                    let tick_age_ms = freshness_ms(Utc::now().timestamp_millis(), tick_fast.recv_ts_ms);
                    let stale_ms = tick_age_ms.max(0) as f64;
                    let book_top_lag_ms = if tick_age_ms <= 1_500
                        && tick_fast.recv_ts_local_ns > 0
                        && book.recv_ts_local_ns > 0
                    {
                        ((book.recv_ts_local_ns - tick_fast.recv_ts_local_ns).max(0) as f64)
                            / 1_000_000.0
                    } else {
                        0.0
                    };
                    let should_sample_book_lag = book_lag_sample_at
                        .get(&symbol)
                        .map(|t| t.elapsed() >= Duration::from_millis(200))
                        .unwrap_or(true);
                    if should_sample_book_lag {
                        book_lag_sample_at.insert(symbol.clone(), Instant::now());
                        shared
                            .shadow_stats
                            .push_book_top_lag_ms(&symbol, book_top_lag_ms)
                            .await;
                    }

                    let latency_sample = estimate_feed_latency(tick_fast, &book);
                    let feed_in_ms = latency_sample.feed_in_ms;
                    let stale_tick_filter_ms =
                        shared.strategy_cfg.read().await.stale_tick_filter_ms;
                    // Stale tick filtering must be based on *local* staleness/age, not exchange
                    // event timestamps (which can be skewed across venues).
                    if stale_ms > stale_tick_filter_ms {
                        shared.shadow_stats.mark_stale_tick_dropped();
                        shared.shadow_stats.record_issue("stale_tick_dropped").await;
                        continue;
                    }
                    shared
                        .shadow_stats
                        .push_latency_sample(
                            feed_in_ms,
                            latency_sample.source_latency_ms,
                            latency_sample.book_latency_ms,
                            latency_sample.local_backlog_ms,
                            latency_sample.ref_decode_ms,
                        )
                        .await;
                    metrics::histogram!("latency.feed_in_ms").record(feed_in_ms);
                    metrics::histogram!("latency.source_latency_ms")
                        .record(latency_sample.source_latency_ms);
                    metrics::histogram!("latency.book_latency_ms").record(latency_sample.book_latency_ms);
                    metrics::histogram!("latency.local_backlog_ms")
                        .record(latency_sample.local_backlog_ms);
                    metrics::histogram!("latency.ref_decode_ms").record(latency_sample.ref_decode_ms);
                    let signal_start = Instant::now();
                    let signal = fair.evaluate(eval_tick, &book);
                    if signal.edge_bps_bid > 0.0 || signal.edge_bps_ask > 0.0 {
                        shared.shadow_stats.mark_candidate();
                    }
                    let signal_us = signal_start.elapsed().as_secs_f64() * 1_000_000.0;
                    shared.shadow_stats.push_signal_us(signal_us).await;
                    metrics::histogram!("latency.signal_us").record(signal_us);
                    let _ = bus.publish(EngineEvent::Signal(signal.clone()));
                    shared.latest_signals.insert(
                        book.market_id.clone(),
                        SignalCacheEntry {
                            signal: signal.clone(),
                            ts_ms: Utc::now().timestamp_millis(),
                        },
                    );
                    if should_sample_book_lag && book_top_lag_ms >= 5.0 {
                        // "Orderless" survival probe: measure whether an observed top-of-book
                        // price survives for +Δ ms, independent of order placement.
                        let mid_yes = (book.bid_yes + book.ask_yes) * 0.5;
                        let probe_side = if signal.fair_yes >= mid_yes {
                            OrderSide::BuyYes
                        } else {
                            OrderSide::BuyNo
                        };
                        let probe_px = aggressive_price_for_side(&book, &probe_side);
                        if probe_px > 0.0 {
                            for delay_ms in [5_u64, 10_u64, 25_u64] {
                                spawn_survival_probe_task(
                                    shared.clone(),
                                    book.market_id.clone(),
                                    symbol.clone(),
                                    probe_side.clone(),
                                    probe_px,
                                    delay_ms,
                                );
                            }
                        }
                    }

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
                        // Step A: snapshot (read-lock) the state needed for computation.
                        let (
                            attempted0,
                            no_quote0,
                            symbol_missing0,
                            cooldown_until_ms0,
                            markout_samples0,
                            markout_1s_p50,
                            markout_5s_p50,
                            markout_10s_p50,
                            markout_10s_p25,
                        ) = {
                            let states = shared.tox_state.read().await;
                            if let Some(st) = states.get(&book.market_id) {
                                let samples = st
                                    .markout_1s
                                    .len()
                                    .max(st.markout_5s.len())
                                    .max(st.markout_10s.len());
                                (
                                    st.attempted,
                                    st.no_quote,
                                    st.symbol_missing,
                                    st.cooldown_until_ms,
                                    samples,
                                    percentile_deque_capped(&st.markout_1s, 0.50, 1024)
                                        .unwrap_or(0.0),
                                    percentile_deque_capped(&st.markout_5s, 0.50, 1024)
                                        .unwrap_or(0.0),
                                    percentile_deque_capped(&st.markout_10s, 0.50, 2048)
                                        .unwrap_or(0.0),
                                    percentile_deque_capped(&st.markout_10s, 0.25, 2048)
                                        .unwrap_or(0.0),
                                )
                            } else {
                                (0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0)
                            }
                        };

                        // Step B: compute without holding a write lock.
                        let attempted1 = attempted0.saturating_add(1).max(1);
                        let tox_features = build_toxic_features(
                            &book,
                            &symbol,
                            stale_ms,
                            signal.fair_yes,
                            attempted1,
                            no_quote0,
                            markout_1s_p50,
                            markout_5s_p50,
                            markout_10s_p50,
                        );
                        let mut tox_decision = evaluate_toxicity(&tox_features, &tox_cfg);
                        let score_scale = (tox_cfg.k_spread / 1.5).clamp(0.25, 4.0);
                        tox_decision.tox_score = (tox_decision.tox_score * score_scale).clamp(0.0, 1.0);
                        tox_decision.regime = if tox_decision.tox_score >= tox_cfg.caution_threshold {
                            ToxicRegime::Danger
                        } else if tox_decision.tox_score >= tox_cfg.safe_threshold {
                            ToxicRegime::Caution
                        } else {
                            ToxicRegime::Safe
                        };

                        let now_ms = Utc::now().timestamp_millis();
                        let mut cooldown_until_ms1 = cooldown_until_ms0;
                        if now_ms < cooldown_until_ms0 {
                            tox_decision.regime = ToxicRegime::Danger;
                            tox_decision.reason_codes.push("cooldown_active".to_string());
                        }
                        // Note: cooldown is based on the pre-warmup regime, matching the old behavior.
                        if matches!(tox_decision.regime, ToxicRegime::Danger) {
                            let cool = cooldown_secs_for_score(tox_decision.tox_score, &tox_cfg);
                            cooldown_until_ms1 =
                                cooldown_until_ms1.max(now_ms + (cool as i64) * 1_000);
                        }
                        let markout_samples = markout_samples0;
                        if markout_samples < 20 {
                            tox_decision.tox_score =
                                tox_decision.tox_score.min(tox_cfg.safe_threshold * 0.8);
                            tox_decision.regime = ToxicRegime::Safe;
                            tox_decision
                                .reason_codes
                                .push("warmup_samples_lt_20".to_string());
                        }

                        let market_score = compute_market_score_from_snapshot(
                            attempted1,
                            no_quote0,
                            symbol_missing0,
                            tox_decision.tox_score,
                            markout_samples,
                            markout_10s_p50,
                        );
                        let no_quote_rate = (no_quote0 as f64 / attempted1 as f64).clamp(0.0, 1.0);
                        let symbol_missing_rate =
                            (symbol_missing0 as f64 / attempted1 as f64).clamp(0.0, 1.0);

                        // Step C: short write-back only.
                        {
                            let mut states = shared.tox_state.write().await;
                            let st = states.entry(book.market_id.clone()).or_default();
                            st.attempted = st.attempted.saturating_add(1);
                            st.symbol = symbol.clone();
                            st.last_tox_score = tox_decision.tox_score;
                            st.last_regime = tox_decision.regime.clone();
                            st.cooldown_until_ms = st.cooldown_until_ms.max(cooldown_until_ms1);
                            st.market_score = market_score;
                        }

                        // Step D: rank decision (read-lock, uses updated market_score).
                        let active_by_rank = {
                            let states = shared.tox_state.read().await;
                            is_market_in_top_n(&states, &book.market_id, tox_cfg.active_top_n_markets)
                        };

                        (
                            tox_features,
                            tox_decision,
                            market_score,
                            pending_market_exposure,
                            no_quote_rate,
                            symbol_missing_rate,
                            markout_samples,
                            markout_10s_p50,
                            markout_10s_p25,
                            active_by_rank,
                        )
                    };
                    let spread_yes = (book.ask_yes - book.bid_yes).max(0.0);
                    let window_outcomes = shared.shadow_stats.window_outcomes_len().await;
                    let base_min_edge_bps = adaptive_min_edge_bps(
                        cfg.min_edge_bps,
                        tox_decision.tox_score,
                        markout_samples,
                        no_quote_rate,
                        markout_10s_p50,
                        markout_10s_p25,
                        window_outcomes,
                        ShadowStats::GATE_MIN_OUTCOMES,
                    );
                    let edge_model_cfg = shared.edge_model_cfg.read().await.clone();
                    let effective_min_edge_bps = base_min_edge_bps
                        + edge_gate_bps(
                            &edge_model_cfg,
                            tox_decision.tox_score,
                            latency_sample.local_backlog_ms,
                            latency_sample.source_latency_ms,
                            no_quote_rate,
                        );
                    let effective_max_spread = adaptive_max_spread(
                        cfg.max_spread,
                        tox_decision.tox_score,
                        markout_samples,
                    );
                    if should_observe_only_symbol(
                        &symbol,
                        &cfg,
                        &tox_decision,
                        stale_ms,
                        spread_yes,
                        book_top_lag_ms,
                    ) {
                        shared
                            .shadow_stats
                            .mark_blocked_with_reason("symbol_quality_guard")
                            .await;
                        continue;
                    }
                    let queue_fill_proxy =
                        estimate_queue_fill_proxy(tox_decision.tox_score, spread_yes, stale_ms);

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

                    let predator_cfg = shared.predator_cfg.read().await.clone();
                    if predator_cfg.enabled
                        && matches!(
                            predator_cfg.priority,
                            PredatorCPriority::TakerFirst | PredatorCPriority::TakerOnly
                        )
                    {
                        let now_ms = Utc::now().timestamp_millis();
                        let res = run_predator_c_for_symbol(
                            &shared,
                            &bus,
                            &portfolio,
                            &execution,
                            &shadow,
                            &mut market_rate_budget,
                            &mut global_rate_budget,
                            &predator_cfg,
                            &symbol,
                            tick_fast.recv_ts_ms,
                            tick_fast.recv_ts_local_ns,
                            now_ms,
                        )
                        .await;
                        if matches!(predator_cfg.priority, PredatorCPriority::TakerOnly) {
                            continue;
                        }
                        if matches!(predator_cfg.priority, PredatorCPriority::TakerFirst)
                            && res.attempted > 0
                        {
                            continue;
                        }
                    }

                    let mut effective_cfg = (*cfg).clone();
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
                        if predator_cfg.enabled
                            && matches!(predator_cfg.priority, PredatorCPriority::MakerFirst)
                        {
                            let now_ms = Utc::now().timestamp_millis();
                            let _ = run_predator_c_for_symbol(
                                &shared,
                                &bus,
                                &portfolio,
                                &execution,
                                &shadow,
                                &mut market_rate_budget,
                                &mut global_rate_budget,
                                &predator_cfg,
                                &symbol,
                                tick_fast.recv_ts_ms,
                                tick_fast.recv_ts_local_ns,
                                now_ms,
                            )
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
                        let proposed_notional_usdc =
                            (intent.price.max(0.0) * intent.size.max(0.0)).max(0.0);
                        let ctx = RiskContext {
                            market_id: intent.market_id.clone(),
                            symbol: symbol.clone(),
                            order_count: execution.open_orders_count(),
                            proposed_size: intent.size,
                            proposed_notional_usdc,
                            market_notional: inventory.exposure_notional + pending_market_exposure,
                            asset_notional: inventory.exposure_notional + pending_total_exposure,
                            drawdown_pct: drawdown,
                            loss_streak: shared.shadow_stats.loss_streak(),
                            now_ms: Utc::now().timestamp_millis(),
                        };
                        let decision = shared.risk_manager.evaluate(&ctx);
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

                        let edge_gross = edge_for_intent(signal.fair_yes, &intent);
                        let rebate_est_bps =
                            get_rebate_bps_cached(&shared, &book.market_id, fee_bps).await;
                        let edge_net =
                            edge_gross - fee_bps + rebate_est_bps - edge_model_cfg.fail_cost_bps;
                        if edge_net < effective_min_edge_bps {
                            shared
                                .shadow_stats
                                .mark_blocked_with_reason("edge_below_threshold")
                                .await;
                            continue;
                        }
                        let intended_notional_usdc =
                            (intent.price.max(0.0) * intent.size.max(0.0)).max(0.0);
                        if intended_notional_usdc < cfg.min_eval_notional_usdc {
                            shared
                                .shadow_stats
                                .mark_blocked_with_reason("tiny_notional")
                                .await;
                            continue;
                        }
                        let edge_net_usdc = (edge_net / 10_000.0) * intended_notional_usdc;
                        if edge_net_usdc < cfg.min_expected_edge_usdc {
                            shared
                                .shadow_stats
                                .mark_blocked_with_reason("edge_notional_too_small")
                                .await;
                            continue;
                        }

                        let mut force_taker = false;
                        if should_force_taker(
                            &cfg,
                            &tox_decision,
                            edge_net,
                            signal.confidence,
                            markout_samples,
                            no_quote_rate,
                            window_outcomes,
                            ShadowStats::GATE_MIN_OUTCOMES,
                            &symbol,
                        ) {
                            let aggressive_price = aggressive_price_for_side(&book, &intent.side);
                            let passive_price = intent.price.max(1e-6);
                            let price_move_bps = ((aggressive_price - intent.price).abs()
                                / passive_price)
                                * 10_000.0;
                            let taker_slippage_budget = adaptive_taker_slippage_bps(
                                cfg.taker_max_slippage_bps,
                                &cfg.market_tier_profile,
                                &symbol,
                                markout_samples,
                                no_quote_rate,
                                window_outcomes,
                                ShadowStats::GATE_MIN_OUTCOMES,
                            );
                            if price_move_bps <= taker_slippage_budget {
                                intent.price = aggressive_price;
                                intent.ttl_ms = intent.ttl_ms.min(150);
                                force_taker = true;
                            } else {
                                shared
                                    .shadow_stats
                                    .mark_blocked_with_reason("taker_slippage_budget")
                                    .await;
                                // If the market moved too far for a safe taker cross, keep the
                                // passive maker quote instead of dropping the opportunity.
                                force_taker = false;
                            }
                        }
                        let per_market_rps = (shared.rate_limit_rps / 8.0).max(0.5);
                        let market_bucket = market_rate_budget
                            .entry(book.market_id.clone())
                            .or_insert_with(|| {
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
                        let tick_to_decision_ms =
                            latency_sample.local_backlog_ms + decision_compute_ms;
                        // Measure order-build cost separately from execution RTT.
                        let order_build_start = Instant::now();
                        let execution_style = if force_taker {
                            ExecutionStyle::Taker
                        } else {
                            classify_execution_style(&book, &intent)
                        };
                        let tif = match execution_style {
                            ExecutionStyle::Maker => OrderTimeInForce::PostOnly,
                            ExecutionStyle::Taker | ExecutionStyle::Arb => OrderTimeInForce::Fak,
                        };
                        let token_id = match intent.side {
                            OrderSide::BuyYes | OrderSide::SellYes => book.token_id_yes.clone(),
                            OrderSide::BuyNo | OrderSide::SellNo => book.token_id_no.clone(),
                        };
                        let v2_intent = OrderIntentV2 {
                            market_id: intent.market_id.clone(),
                            token_id: Some(token_id),
                            side: intent.side.clone(),
                            price: intent.price,
                            size: intent.size,
                            ttl_ms: intent.ttl_ms,
                            style: execution_style.clone(),
                            tif,
                            max_slippage_bps: cfg.taker_max_slippage_bps,
                            fee_rate_bps: fee_bps,
                            expected_edge_net_bps: edge_net,
                            client_order_id: Some(new_id()),
                            hold_to_resolution: false,
                        };
                        let order_build_ms = order_build_start.elapsed().as_secs_f64() * 1_000.0;
                        // Start measuring ack-only RTT right before the await.
                        let place_start = Instant::now();
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
                                    // In paper/shadow mode we still track local place_order RTT as
                                    // the ack proxy so latency distributions are measurable.
                                    place_start.elapsed().as_secs_f64() * 1_000.0
                                };
                                let tick_to_ack_ms =
                                    tick_to_decision_ms + order_build_ms + ack_only_ms;
                                let capturable_window_ms = if ack_only_ms > 0.0 {
                                    book_top_lag_ms - tick_to_ack_ms
                                } else {
                                    0.0
                                };
                                shared
                                    .shadow_stats
                                    .push_decision_compute_ms(decision_compute_ms)
                                    .await;
                                shared
                                    .shadow_stats
                                    .push_tick_to_decision_ms(tick_to_decision_ms)
                                    .await;
                                if ack_only_ms > 0.0 {
                                    shared.shadow_stats.push_ack_only_ms(ack_only_ms).await;
                                    shared
                                        .shadow_stats
                                        .push_tick_to_ack_ms(tick_to_ack_ms)
                                        .await;
                                    shared
                                        .shadow_stats
                                        .push_capturable_window_ms(capturable_window_ms)
                                        .await;
                                }
                                metrics::histogram!("latency.tick_to_decision_ms")
                                    .record(tick_to_decision_ms);
                                metrics::histogram!("latency.decision_compute_ms")
                                    .record(decision_compute_ms);
                                metrics::histogram!("latency.decision_queue_wait_ms")
                                    .record(latency_sample.local_backlog_ms);
                                if ack_only_ms > 0.0 {
                                    metrics::histogram!("latency.ack_only_ms").record(ack_only_ms);
                                    metrics::histogram!("latency.tick_to_ack_ms")
                                        .record(tick_to_ack_ms);
                                    metrics::histogram!("latency.capturable_window_ms")
                                        .record(capturable_window_ms);
                                }

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
                                        book_top_lag_ms,
                                        ack_only_ms,
                                        tick_to_ack_ms,
                                        capturable_window_ms,
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
                                    .mark_blocked_with_reason(&format!(
                                        "exchange_reject_{reject_code}"
                                    ))
                                    .await;
                                metrics::counter!("execution.place_rejected").increment(1);
                            }
                            Err(err) => {
                                let reason = classify_execution_error_reason(&err);
                                shared.shadow_stats.mark_blocked_with_reason(reason).await;
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
                    shared.shadow_stats.set_paused(true);
                }
                EngineEvent::Control(ControlCommand::Resume) => {
                    *paused.write().await = false;
                    shared.shadow_stats.set_paused(false);
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

#[derive(Debug, Default, Clone, Copy)]
struct PredatorExecResult {
    attempted: u64,
    executed: u64,
}

async fn run_predator_c_for_symbol(
    shared: &Arc<EngineShared>,
    bus: &RingBus<EngineEvent>,
    portfolio: &Arc<PortfolioBook>,
    execution: &Arc<ClobExecution>,
    shadow: &Arc<ShadowExecutor>,
    market_rate_budget: &mut HashMap<String, TokenBucket>,
    global_rate_budget: &mut TokenBucket,
    predator_cfg: &PredatorCConfig,
    symbol: &str,
    tick_fast_recv_ts_ms: i64,
    tick_fast_recv_ts_local_ns: i64,
    now_ms: i64,
) -> PredatorExecResult {
    shared.shadow_stats.set_predator_enabled(predator_cfg.enabled);
    if !predator_cfg.enabled {
        return PredatorExecResult::default();
    }

    let direction_signal = {
        let det = shared.predator_direction_detector.read().await;
        det.evaluate(symbol, now_ms)
    };
    let Some(direction_signal) = direction_signal else {
        shared
            .shadow_stats
            .mark_predator_taker_skipped("no_direction_signal")
            .await;
        return PredatorExecResult::default();
    };

    shared.shadow_stats.mark_predator_direction(&direction_signal.direction);
    {
        let mut map = shared.predator_latest_direction.write().await;
        map.insert(symbol.to_string(), direction_signal.clone());
    }
    let _ = bus.publish(EngineEvent::DirectionSignal(direction_signal.clone()));

    if matches!(direction_signal.direction, Direction::Neutral) {
        shared
            .shadow_stats
            .mark_predator_taker_skipped("neutral_direction")
            .await;
        return PredatorExecResult::default();
    }

    let market_ids = shared
        .symbol_to_markets
        .read()
        .await
        .get(symbol)
        .cloned()
        .unwrap_or_default();
    if market_ids.is_empty() {
        shared
            .shadow_stats
            .mark_predator_taker_skipped("no_symbol_markets")
            .await;
        return PredatorExecResult::default();
    }

    let maker_cfg = shared.strategy_cfg.read().await.clone();
    let edge_model_cfg = shared.edge_model_cfg.read().await.clone();
    let tf_weights = HashMap::from([
        (
            TimeframeClass::Tf5m,
            timeframe_weight(shared, &TimeframeClass::Tf5m).await,
        ),
        (
            TimeframeClass::Tf15m,
            timeframe_weight(shared, &TimeframeClass::Tf15m).await,
        ),
        (
            TimeframeClass::Tf1h,
            timeframe_weight(shared, &TimeframeClass::Tf1h).await,
        ),
        (
            TimeframeClass::Tf1d,
            timeframe_weight(shared, &TimeframeClass::Tf1d).await,
        ),
    ]);

    let (quote_notional_usdc, total_capital_usdc) = if predator_cfg.compounder.enabled {
        let c = shared.predator_compounder.read().await;
        (c.recommended_quote_notional_usdc(), c.available())
    } else {
        (0.0, predator_cfg.compounder.initial_capital_usdc.max(0.0))
    };

    let side = match direction_signal.direction {
        Direction::Up => OrderSide::BuyYes,
        Direction::Down => OrderSide::BuyNo,
        Direction::Neutral => OrderSide::BuyYes,
    };

    // Build candidate opportunities using cached fair values per market (do NOT call fair.evaluate
    // here; the fair model is stateful per symbol and would be advanced multiple times).
    #[derive(Debug, Clone)]
    struct PredatorCandIn {
        market_id: String,
        timeframe: TimeframeClass,
        entry_price: f64,
        spread: f64,
        fee_bps: f64,
        edge_gross_bps: f64,
        edge_net_bps: f64,
        size: f64,
    }

    let market_ids = market_ids.into_iter().take(32).collect::<Vec<_>>();
    let tf_by_market = {
        let map = shared.market_to_timeframe.read().await;
        market_ids
            .iter()
            .filter_map(|id| map.get(id).cloned().map(|tf| (id.clone(), tf)))
            .collect::<HashMap<String, TimeframeClass>>()
    };
    let book_by_market = {
        let map = shared.latest_books.read().await;
        market_ids
            .iter()
            .filter_map(|id| map.get(id).cloned().map(|b| (id.clone(), b)))
            .collect::<HashMap<String, BookTop>>()
    };

    let mut cand_inputs = Vec::<PredatorCandIn>::new();
    for market_id in market_ids {
        let Some(timeframe) = tf_by_market.get(&market_id).cloned() else {
            continue;
        };
        let Some(book) = book_by_market.get(&market_id).cloned() else {
            continue;
        };
        let sig_entry = shared
            .latest_signals
            .get(&market_id)
            .map(|v| v.value().clone());
        let Some(sig_entry) = sig_entry else {
            continue;
        };
        if now_ms.saturating_sub(sig_entry.ts_ms) > 5_000 {
            continue;
        }

        let entry_price = aggressive_price_for_side(&book, &side);
        let spread = spread_for_side(&book, &side);
        let fee_bps = get_fee_rate_bps_cached(shared, &market_id).await;
        let rebate_est_bps = get_rebate_bps_cached(shared, &market_id, fee_bps).await;
        let edge_gross_bps =
            edge_gross_bps_for_side(sig_entry.signal.fair_yes, &side, entry_price);
        let edge_net_bps = edge_gross_bps - fee_bps + rebate_est_bps - edge_model_cfg.fail_cost_bps;
        let base_size = if predator_cfg.compounder.enabled {
            (quote_notional_usdc / entry_price.max(1e-6)).max(0.01)
        } else {
            maker_cfg.base_quote_size.max(0.01)
        };
        let tf_weight = tf_weights.get(&timeframe).copied().unwrap_or(1.0).clamp(0.0, 1.0);
        if tf_weight <= 0.0 {
            continue;
        }
        let size = (base_size * tf_weight).max(0.01);

        cand_inputs.push(PredatorCandIn {
            market_id,
            timeframe,
            entry_price,
            spread,
            fee_bps,
            edge_gross_bps,
            edge_net_bps,
            size,
        });
    }

    let mut candidates: Vec<TimeframeOpp> = Vec::new();
    let mut skip_reasons: Vec<String> = Vec::new();
    {
        let mut sniper = shared.predator_taker_sniper.write().await;
        for cin in cand_inputs {
            let decision = sniper.evaluate(
                &cin.market_id,
                symbol,
                cin.timeframe.clone(),
                &direction_signal,
                cin.entry_price,
                cin.spread,
                cin.fee_bps,
                cin.edge_gross_bps,
                cin.edge_net_bps,
                cin.size,
                now_ms,
            );
            match decision.action {
                TakerAction::Fire => {
                    if let Some(opp) = decision.opportunity {
                        candidates.push(opp);
                    }
                }
                TakerAction::Skip => {
                    skip_reasons.push(decision.reason);
                }
            }
        }
    }
    for reason in skip_reasons {
        shared
            .shadow_stats
            .mark_predator_taker_skipped(reason.as_str())
            .await;
    }

    if candidates.is_empty() {
        return PredatorExecResult::default();
    }

    // Router selects + locks.
    let mut locked_opps: Vec<TimeframeOpp> = Vec::new();
    let mut locked_by_tf_usdc: HashMap<String, f64> = HashMap::new();
    {
        let mut router = shared.predator_router.write().await;
        let selected = router.route(candidates, total_capital_usdc.max(0.0), now_ms);
        for opp in selected {
            if router.lock(&opp, now_ms) {
                locked_opps.push(opp);
            }
        }
        for (tf, v) in router.locked_by_tf_usdc(now_ms) {
            locked_by_tf_usdc.insert(tf.to_string(), v);
        }
    }
    shared
        .shadow_stats
        .set_predator_router_locked_by_tf_usdc(locked_by_tf_usdc)
        .await;

    if locked_opps.is_empty() {
        return PredatorExecResult::default();
    }

    let mut out = PredatorExecResult::default();
    for opp in locked_opps {
        shared.shadow_stats.mark_predator_taker_fired();
        let res = predator_execute_opportunity(
            shared,
            bus,
            portfolio,
            execution,
            shadow,
            market_rate_budget,
            global_rate_budget,
            &maker_cfg,
            predator_cfg,
            &direction_signal,
            &opp,
            tick_fast_recv_ts_ms,
            tick_fast_recv_ts_local_ns,
            now_ms,
        )
        .await;
        out.attempted = out.attempted.saturating_add(res.attempted);
        out.executed = out.executed.saturating_add(res.executed);
    }
    out
}

async fn predator_execute_opportunity(
    shared: &Arc<EngineShared>,
    bus: &RingBus<EngineEvent>,
    portfolio: &Arc<PortfolioBook>,
    execution: &Arc<ClobExecution>,
    shadow: &Arc<ShadowExecutor>,
    market_rate_budget: &mut HashMap<String, TokenBucket>,
    global_rate_budget: &mut TokenBucket,
    maker_cfg: &MakerConfig,
    predator_cfg: &PredatorCConfig,
    _direction_signal: &DirectionSignal,
    opp: &TimeframeOpp,
    tick_fast_recv_ts_ms: i64,
    tick_fast_recv_ts_local_ns: i64,
    now_ms: i64,
) -> PredatorExecResult {
    let mut intent = QuoteIntent {
        market_id: opp.market_id.clone(),
        side: opp.side.clone(),
        price: opp.entry_price,
        size: opp.size,
        ttl_ms: maker_cfg.ttl_ms,
    };

    shared.shadow_stats.mark_attempted();

    let book = shared.latest_books.read().await.get(&intent.market_id).cloned();
    let Some(book) = book else {
        shared
            .shadow_stats
            .mark_predator_taker_skipped("book_missing")
            .await;
        return PredatorExecResult::default();
    };

    // Same interpretation as the maker path: positive means our fast ref tick arrived earlier
    // than the Polymarket book update. For Predator, the ref tick is per-symbol while the book
    // is per-market, so this is still a useful (if approximate) window estimate.
    let tick_age_ms = freshness_ms(now_ms, tick_fast_recv_ts_ms);
    let book_top_lag_ms = if tick_age_ms <= 1_500
        && tick_fast_recv_ts_local_ns > 0
        && book.recv_ts_local_ns > 0
    {
        ((book.recv_ts_local_ns - tick_fast_recv_ts_local_ns).max(0) as f64) / 1_000_000.0
    } else {
        0.0
    };

    let proposed_notional_usdc = (intent.price.max(0.0) * intent.size.max(0.0)).max(0.0);
    let inventory = inventory_for_market(portfolio, &intent.market_id);
    let pending_market_exposure = execution.open_order_notional_for_market(&intent.market_id);
    let pending_total_exposure = execution.open_order_notional_total();
    let drawdown = portfolio.snapshot().max_drawdown_pct;
    let ctx = RiskContext {
        market_id: intent.market_id.clone(),
        symbol: opp.symbol.clone(),
        order_count: execution.open_orders_count(),
        proposed_size: intent.size,
        proposed_notional_usdc,
        market_notional: inventory.exposure_notional + pending_market_exposure,
        asset_notional: inventory.exposure_notional + pending_total_exposure,
        drawdown_pct: drawdown,
        loss_streak: shared.shadow_stats.loss_streak(),
        now_ms,
    };
    let decision = shared.risk_manager.evaluate(&ctx);
    if !decision.allow {
        shared
            .shadow_stats
            .mark_blocked_with_reason(&format!("risk:{}", decision.reason))
            .await;
        return PredatorExecResult::default();
    }
    if decision.capped_size <= 0.0 {
        shared
            .shadow_stats
            .mark_blocked_with_reason("risk_capped_zero")
            .await;
        return PredatorExecResult::default();
    }
    intent.size = intent.size.min(decision.capped_size);

    let intended_notional_usdc = (intent.price.max(0.0) * intent.size.max(0.0)).max(0.0);
    if intended_notional_usdc < maker_cfg.min_eval_notional_usdc {
        shared
            .shadow_stats
            .mark_blocked_with_reason("tiny_notional")
            .await;
        return PredatorExecResult::default();
    }

    let edge_net_usdc = (opp.edge_net_bps / 10_000.0) * intended_notional_usdc;
    let edge_model_cfg = shared.edge_model_cfg.read().await.clone();
    let dynamic_gate_bps = edge_gate_bps(
        &edge_model_cfg,
        0.0,
        tick_age_ms as f64,
        book_top_lag_ms,
        0.0,
    );
    if opp.edge_net_bps < maker_cfg.min_edge_bps + dynamic_gate_bps {
        shared
            .shadow_stats
            .mark_blocked_with_reason("edge_below_dynamic_gate")
            .await;
        return PredatorExecResult::default();
    }
    if edge_net_usdc < maker_cfg.min_expected_edge_usdc {
        shared
            .shadow_stats
            .mark_blocked_with_reason("edge_notional_too_small")
            .await;
        return PredatorExecResult::default();
    }

    let per_market_rps = (shared.rate_limit_rps / 8.0).max(0.5);
    let market_bucket = market_rate_budget
        .entry(intent.market_id.clone())
        .or_insert_with(|| TokenBucket::new(per_market_rps, (per_market_rps * 2.0).max(1.0)));
    if !global_rate_budget.try_take(1.0) {
        shared
            .shadow_stats
            .mark_blocked_with_reason("rate_budget_global")
            .await;
        return PredatorExecResult::default();
    }
    if !market_bucket.try_take(1.0) {
        shared
            .shadow_stats
            .mark_blocked_with_reason("rate_budget_market")
            .await;
        return PredatorExecResult::default();
    }

    shared.shadow_stats.mark_eligible();
    let order_build_start = Instant::now();
    let token_id = match intent.side {
        OrderSide::BuyYes | OrderSide::SellYes => book.token_id_yes.clone(),
        OrderSide::BuyNo | OrderSide::SellNo => book.token_id_no.clone(),
    };
    let v2_intent = OrderIntentV2 {
        market_id: intent.market_id.clone(),
        token_id: Some(token_id),
        side: intent.side.clone(),
        price: intent.price,
        size: intent.size,
        ttl_ms: intent.ttl_ms,
        style: ExecutionStyle::Taker,
        tif: OrderTimeInForce::Fak,
        max_slippage_bps: maker_cfg.taker_max_slippage_bps,
        fee_rate_bps: opp.fee_bps,
        expected_edge_net_bps: opp.edge_net_bps,
        client_order_id: Some(new_id()),
        hold_to_resolution: false,
    };
    let order_build_ms = order_build_start.elapsed().as_secs_f64() * 1_000.0;
    let place_start = Instant::now();

    let mut out = PredatorExecResult::default();
    out.attempted = out.attempted.saturating_add(1);

    match execution.place_order_v2(v2_intent).await {
        Ok(ack_v2) if ack_v2.accepted => {
            let accepted_size = ack_v2.accepted_size.max(0.0).min(intent.size);
            if accepted_size <= 0.0 {
                shared
                    .shadow_stats
                    .mark_blocked_with_reason("exchange_reject_zero_size")
                    .await;
                return out;
            }
            intent.size = accepted_size;
            shared.shadow_stats.mark_executed();
            out.executed = out.executed.saturating_add(1);

            let ack_only_ms = if ack_v2.exchange_latency_ms > 0.0 {
                ack_v2.exchange_latency_ms
            } else {
                // Keep latency visibility in non-live mode via local order path RTT.
                place_start.elapsed().as_secs_f64() * 1_000.0
            };
            let tick_to_ack_ms = order_build_ms + ack_only_ms;
            let capturable_window_ms = if ack_only_ms > 0.0 {
                book_top_lag_ms - tick_to_ack_ms
            } else {
                0.0
            };
            if ack_only_ms > 0.0 {
                shared.shadow_stats.push_ack_only_ms(ack_only_ms).await;
                shared
                    .shadow_stats
                    .push_tick_to_ack_ms(tick_to_ack_ms)
                    .await;
                shared
                    .shadow_stats
                    .push_capturable_window_ms(capturable_window_ms)
                    .await;
                metrics::histogram!("latency.ack_only_ms").record(ack_only_ms);
                metrics::histogram!("latency.tick_to_ack_ms").record(tick_to_ack_ms);
                metrics::histogram!("latency.capturable_window_ms").record(capturable_window_ms);
            }

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
                    symbol: opp.symbol.clone(),
                    side: intent.side.clone(),
                    execution_style: ExecutionStyle::Taker,
                    book_top_lag_ms,
                    ack_only_ms,
                    tick_to_ack_ms,
                    capturable_window_ms,
                    survival_probe_price: aggressive_price_for_side(&book, &intent.side),
                    intended_price: intent.price,
                    size: intent.size,
                    edge_gross_bps: opp.edge_gross_bps,
                    edge_net_bps: opp.edge_net_bps,
                    fee_paid_bps: opp.fee_bps,
                    rebate_est_bps: 0.0,
                    delay_ms,
                    t0_ns: now_ns(),
                    min_edge_bps: predator_cfg.taker_sniper.min_edge_net_bps,
                    tox_score: 0.0,
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
            shared.shadow_stats.mark_blocked_with_reason(reason).await;
            tracing::warn!(?err, "predator place_order failed");
            metrics::counter!("execution.place_error").increment(1);
        }
    }

    out
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
            survived,
            fillable,
            slippage_bps,
            queue_fill_prob,
            attribution,
            pnl_1s_bps,
            pnl_5s_bps,
            pnl_10s_bps,
        ) = if let Some(book) = book {
            let survived = evaluate_survival(&shot, &book);
            let (fillable, slippage_bps, queue_fill_prob) =
                evaluate_fillable(&shot, &book, latency_ms);
            if fillable {
                let p1 = pnl_after_horizon(&shared, &shot, Duration::from_secs(1)).await;
                let p5 = pnl_after_horizon(&shared, &shot, Duration::from_secs(5)).await;
                let p10 = pnl_after_horizon(&shared, &shot, Duration::from_secs(10)).await;
                let attribution = classify_filled_outcome(shot.edge_net_bps, p10, slippage_bps);
                (
                    survived,
                    true,
                    slippage_bps,
                    queue_fill_prob,
                    attribution,
                    p1,
                    p5,
                    p10,
                )
            } else {
                let attribution = classify_unfilled_outcome(
                    &book,
                    latency_ms,
                    shot.delay_ms,
                    survived,
                    queue_fill_prob,
                );
                (
                    survived,
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
            survived,
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
        if shot.delay_ms == 10 {
            let exit_cfg = shared.exit_cfg.read().await.clone();
            if exit_cfg.enabled {
                let elapsed_ms = ((now_ns() - shot.t0_ns).max(0) as f64) / 1_000_000.0;
                let net_10s = outcome.net_markout_10s_bps.unwrap_or(0.0);
                let exit_reason = if net_10s <= exit_cfg.adverse_move_bps {
                    Some("adverse_move")
                } else if net_10s <= exit_cfg.edge_decay_bps {
                    Some("edge_decay")
                } else if elapsed_ms > exit_cfg.time_stop_ms as f64 {
                    Some("time_stop")
                } else {
                    None
                };
                if let Some(reason) = exit_reason {
                    shared.shadow_stats.record_exit_reason(reason).await;
                    if exit_cfg.flatten_on_trigger {
                        let _ = bus.publish(EngineEvent::Control(ControlCommand::Flatten));
                    }
                }
            }
        }
        update_toxic_state_from_outcome(&shared, &outcome).await;
        if shot.delay_ms == 10 {
            if let Some(pnl_usdc) = outcome.net_markout_10s_usdc {
                let compounder_enabled = shared.predator_cfg.read().await.compounder.enabled;
                if compounder_enabled {
                    let (update, halt) = {
                        let mut c = shared.predator_compounder.write().await;
                        let update = c.on_markout(pnl_usdc);
                        (update, c.halted())
                    };
                    shared
                        .shadow_stats
                        .set_predator_capital(update.clone(), halt)
                        .await;
                    let _ = bus.publish(EngineEvent::CapitalUpdate(update));

                    if halt {
                        let live_armed = std::env::var("POLYEDGE_LIVE_ARMED")
                            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
                            .unwrap_or(false);
                        if live_armed {
                            shared.shadow_stats.record_issue("capital_halt").await;
                            let _ = bus.publish(EngineEvent::Control(ControlCommand::Pause));
                            let _ = bus.publish(EngineEvent::Control(ControlCommand::Flatten));
                        }
                    }
                }
            }
            let eval = QuoteEval {
                market_id: shot.market_id.clone(),
                symbol: shot.symbol.clone(),
                survival_10ms: if outcome.survived { 1.0 } else { 0.0 },
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

fn spawn_survival_probe_task(
    shared: Arc<EngineShared>,
    market_id: String,
    symbol: String,
    side: OrderSide,
    probe_px: f64,
    delay_ms: u64,
) {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        let book = shared.latest_books.read().await.get(&market_id).cloned();
        let survived = match book {
            Some(ref b) => is_crossable(&side, probe_px, b),
            None => false,
        };
        shared
            .shadow_stats
            .record_survival_probe(&symbol, delay_ms, survived)
            .await;
    });
}

async fn refresh_market_symbol_map(shared: &EngineShared) {
    let discovery = MarketDiscovery::new(DiscoveryConfig {
        symbols: (*shared.universe_symbols).clone(),
        market_types: (*shared.universe_market_types).clone(),
        timeframes: (*shared.universe_timeframes).clone(),
        ..DiscoveryConfig::default()
    });
    match discovery.discover().await {
        Ok(markets) => {
            let mut market_map = HashMap::new();
            let mut token_map = HashMap::new();
            let mut timeframe_map = HashMap::new();
            let mut symbol_to_markets = HashMap::<String, Vec<String>>::new();
            for m in markets {
                market_map.insert(m.market_id.clone(), m.symbol.clone());
                symbol_to_markets
                    .entry(m.symbol.clone())
                    .or_default()
                    .push(m.market_id.clone());
                if let Some(tf) = m
                    .timeframe
                    .as_deref()
                    .and_then(parse_timeframe_class)
                {
                    timeframe_map.insert(m.market_id.clone(), tf);
                }
                if let Some(t) = m.token_id_yes {
                    token_map.insert(t, m.symbol.clone());
                }
                if let Some(t) = m.token_id_no {
                    token_map.insert(t, m.symbol.clone());
                }
            }
            for v in symbol_to_markets.values_mut() {
                v.sort();
                v.dedup();
            }
            {
                let mut map = shared.market_to_symbol.write().await;
                *map = market_map;
            }
            {
                let mut map = shared.token_to_symbol.write().await;
                *map = token_map;
            }
            {
                let mut map = shared.market_to_timeframe.write().await;
                *map = timeframe_map;
            }
            {
                let mut map = shared.symbol_to_markets.write().await;
                *map = symbol_to_markets;
            }
        }
        Err(err) => {
            tracing::warn!(?err, "market discovery refresh failed");
        }
    }
}

fn parse_timeframe_class(tf: &str) -> Option<TimeframeClass> {
    match tf.trim().to_ascii_lowercase().as_str() {
        "5m" | "tf5m" => Some(TimeframeClass::Tf5m),
        "15m" | "tf15m" => Some(TimeframeClass::Tf15m),
        "1h" | "tf1h" => Some(TimeframeClass::Tf1h),
        "1d" | "tf1d" => Some(TimeframeClass::Tf1d),
        _ => None,
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
    attempted: u64,
    no_quote: u64,
    markout_1s_p50: f64,
    markout_5s_p50: f64,
    markout_10s_p50: f64,
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
    let attempted = attempted.max(1);
    let cancel_burst = (no_quote as f64 / attempted as f64).clamp(0.0, 1.0);

    ToxicFeatures {
        market_id: book.market_id.clone(),
        symbol: symbol.to_string(),
        markout_1s: markout_1s_p50,
        markout_5s: markout_5s_p50,
        markout_10s: markout_10s_p50,
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
    let markout_10s = percentile_deque_capped(&state.markout_10s, 0.50, 2048).unwrap_or(0.0);
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

fn compute_market_score_from_snapshot(
    attempted: u64,
    no_quote: u64,
    symbol_missing: u64,
    tox_score: f64,
    markout_samples: usize,
    markout_10s_p50: f64,
) -> f64 {
    let attempted = attempted.max(1);
    let no_quote_rate = no_quote as f64 / attempted as f64;
    let symbol_missing_rate = symbol_missing as f64 / attempted as f64;
    if markout_samples < 20 {
        let warmup_score = 80.0 - no_quote_rate * 6.0 - symbol_missing_rate * 6.0;
        return warmup_score.clamp(45.0, 100.0);
    }
    let score = 70.0 + (markout_10s_p50 * 1.5).clamp(-30.0, 30.0)
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
    window_outcomes: usize,
    gate_min_outcomes: usize,
) -> f64 {
    if window_outcomes < gate_min_outcomes {
        let progress = (window_outcomes as f64 / gate_min_outcomes.max(1) as f64).clamp(0.0, 1.0);
        let warmup_floor = (base_min_edge_bps * 0.15).max(0.5);
        let warmup_target = (base_min_edge_bps * 0.50).max(warmup_floor);
        let mut warmup_edge = warmup_floor + (warmup_target - warmup_floor) * progress;
        if no_quote_rate > 0.85 {
            warmup_edge *= 0.80;
        }
        return warmup_edge.clamp(0.25, base_min_edge_bps.max(0.25));
    }

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

fn edge_gate_bps(
    cfg: &EdgeModelConfig,
    tox_score: f64,
    local_backlog_ms: f64,
    source_latency_ms: f64,
    no_quote_rate: f64,
) -> f64 {
    if !cfg.gate_mode.eq_ignore_ascii_case("dynamic") {
        return cfg.base_gate_bps.max(0.0);
    }
    let congestion = cfg.congestion_penalty_bps * no_quote_rate.clamp(0.0, 1.0);
    let latency = cfg.latency_penalty_bps
        * ((local_backlog_ms / 100.0).clamp(0.0, 1.0)
            + (source_latency_ms / 800.0).clamp(0.0, 1.0));
    let tox = cfg.fail_cost_bps * tox_score.clamp(0.0, 1.0);
    (cfg.base_gate_bps + congestion + latency + tox).max(0.0)
}

fn adaptive_max_spread(base_max_spread: f64, tox_score: f64, markout_samples: usize) -> f64 {
    if markout_samples < 20 {
        return (base_max_spread * 1.2).clamp(0.003, 0.08);
    }
    (base_max_spread * (1.0 - tox_score * 0.35)).clamp(0.002, base_max_spread)
}

async fn timeframe_weight(shared: &Arc<EngineShared>, timeframe: &TimeframeClass) -> f64 {
    let baseline = match timeframe {
        TimeframeClass::Tf5m | TimeframeClass::Tf15m => 1.0,
        TimeframeClass::Tf1h => 0.35,
        TimeframeClass::Tf1d => 0.20,
    };
    if matches!(timeframe, TimeframeClass::Tf5m | TimeframeClass::Tf15m) {
        return baseline;
    }

    let outcomes = shared.shadow_stats.outcomes.read().await;
    if outcomes.is_empty() {
        return baseline;
    }
    let market_tf = shared.market_to_timeframe.read().await;
    let mut markouts = Vec::new();
    for o in outcomes.iter().rev() {
        if o.delay_ms != 10 || !o.fillable {
            continue;
        }
        let Some(tf) = market_tf.get(&o.market_id) else {
            continue;
        };
        if tf != timeframe {
            continue;
        }
        if let Some(v) = o.net_markout_10s_bps {
            markouts.push(v);
        }
        if markouts.len() >= 200 {
            break;
        }
    }

    if markouts.len() < 30 {
        return baseline;
    }
    let p50 = percentile(&markouts, 0.50).unwrap_or(0.0);
    if p50 <= -8.0 {
        0.05
    } else if p50 <= -2.0 {
        0.12
    } else if p50 <= 0.0 {
        0.20
    } else if p50 <= 3.0 {
        baseline
    } else {
        (baseline * 1.4).clamp(0.0, 1.0)
    }
}

fn should_force_taker(
    cfg: &MakerConfig,
    tox: &ToxicDecision,
    edge_net_bps: f64,
    confidence: f64,
    markout_samples: usize,
    no_quote_rate: f64,
    window_outcomes: usize,
    gate_min_outcomes: usize,
    symbol: &str,
) -> bool {
    let profile = cfg.market_tier_profile.to_ascii_lowercase();
    let aggressive_profile =
        profile.contains("taker") || profile.contains("aggressive") || profile.contains("latency");

    let warmup_factor = if window_outcomes < gate_min_outcomes {
        let progress = (window_outcomes as f64 / gate_min_outcomes.max(1) as f64).clamp(0.0, 1.0);
        // Lower trigger during warmup so the funnel can collect enough comparable samples.
        (0.70 + 0.30 * progress).clamp(0.60, 1.0)
    } else {
        1.0
    };
    let no_quote_factor = if no_quote_rate > 0.90 { 0.85 } else { 1.0 };
    let mut trigger_bps = cfg.taker_trigger_bps.max(0.0) * warmup_factor * no_quote_factor;
    if symbol.eq_ignore_ascii_case("SOLUSDT")
        && (profile.contains("sol_guard") || !aggressive_profile)
    {
        trigger_bps *= 1.25;
    }

    if edge_net_bps < trigger_bps {
        return false;
    }
    if matches!(tox.regime, ToxicRegime::Danger) {
        return false;
    }

    if matches!(tox.regime, ToxicRegime::Caution) && !aggressive_profile {
        return false;
    }

    let min_conf = if markout_samples < 20 || window_outcomes < gate_min_outcomes {
        0.45
    } else {
        0.55
    };
    if confidence < min_conf && !aggressive_profile {
        return false;
    }

    true
}

fn adaptive_taker_slippage_bps(
    base_slippage_bps: f64,
    market_tier_profile: &str,
    symbol: &str,
    markout_samples: usize,
    no_quote_rate: f64,
    window_outcomes: usize,
    gate_min_outcomes: usize,
) -> f64 {
    let mut out = base_slippage_bps.max(1.0);
    let profile = market_tier_profile.to_ascii_lowercase();
    let aggressive = profile.contains("aggressive") || profile.contains("latency");

    if window_outcomes < gate_min_outcomes || markout_samples < 20 {
        out *= if no_quote_rate > 0.85 { 1.40 } else { 1.20 };
    }
    if aggressive {
        out *= 1.10;
    }
    if symbol.eq_ignore_ascii_case("SOLUSDT") && (profile.contains("sol_guard") || !aggressive) {
        out *= 0.80;
    }

    out.clamp(5.0, 60.0)
}

fn should_observe_only_symbol(
    symbol: &str,
    cfg: &MakerConfig,
    tox: &ToxicDecision,
    stale_ms: f64,
    spread_yes: f64,
    book_top_lag_ms: f64,
) -> bool {
    if !symbol.eq_ignore_ascii_case("SOLUSDT") {
        return false;
    }
    let profile = cfg.market_tier_profile.to_ascii_lowercase();
    if !(profile.contains("sol_guard") || profile.contains("balanced")) {
        return false;
    }

    // Keep SOL observe-only unless the local "ref lead vs book" lag is within a tight bound.
    // This avoids letting one slow/volatile venue degrade the overall engine quality.
    book_top_lag_ms > 130.0
        || matches!(tox.regime, ToxicRegime::Danger)
        || stale_ms > 250.0
        || spread_yes > 0.020
}

fn estimate_queue_fill_proxy(tox_score: f64, spread_yes: f64, stale_ms: f64) -> f64 {
    let spread_pen = (spread_yes / 0.03).clamp(0.0, 1.0);
    let latency_pen = (stale_ms / 800.0).clamp(0.0, 1.0);
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
        .map(|(id, st)| (id.clone(), st.market_score))
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
    let samples = st
        .markout_1s
        .len()
        .max(st.markout_5s.len())
        .max(st.markout_10s.len());
    st.market_score = compute_market_score(st, st.last_tox_score, samples);
}

fn push_rolling(dst: &mut VecDeque<f64>, value: f64, cap: usize) {
    dst.push_back(value);
    while dst.len() > cap {
        dst.pop_front();
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
    book_latency_ms: f64,
    local_backlog_ms: f64,
    ref_decode_ms: f64,
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
    // IMPORTANT: `source_latency_ms` is the external *reference* (CEX) tick latency proxy.
    // Do not mix in Polymarket book timestamps here; track book latency separately.
    let source_latency_ms = tick_ingest_ms;
    let ref_decode_ms = if tick.recv_ts_local_ns > 0 && tick.ingest_ts_local_ns > 0 {
        ((tick.ingest_ts_local_ns - tick.recv_ts_local_ns).max(0) as f64) / 1_000_000.0
    } else {
        0.0
    };

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

    // IMPORTANT: `feed_in_ms` is the *local* recv->ingest latency (decode + enqueue), intended
    // to be <5ms and independent of exchange clock skew. Use source_latency_ms/local_backlog_ms
    // separately for "how old is this data?" debugging/guardrails.
    let feed_in_ms = ref_decode_ms;
    FeedLatencySample {
        feed_in_ms,
        source_latency_ms,
        book_latency_ms: book_ingest_ms,
        local_backlog_ms,
        ref_decode_ms,
    }
}

fn is_anchor_ref_source(source: &str) -> bool {
    source == "chainlink_rtds"
}

fn ref_event_ts_ms(tick: &RefTick) -> i64 {
    tick.event_ts_exchange_ms.max(tick.event_ts_ms)
}

fn should_replace_ref_tick(current: &RefTick, next: &RefTick) -> bool {
    let current_event = ref_event_ts_ms(current);
    let next_event = ref_event_ts_ms(next);
    // Guard: ignore ticks whose event timestamp is wildly in the future vs our local receive time.
    // (This can happen under clock skew or malformed payloads and would break latency ranking.)
    if next_event > next.recv_ts_ms + 5_000 {
        return false;
    }

    // Priority 1: Chainlink RTDS has <5ms latency, always prefer when timestamps are recent
    if next.source == "chainlink_rtds" && next_event + 50 >= current_event {
        return true;
    }
    if current.source == "chainlink_rtds" && current_event + 50 >= next_event {
        return false;
    }
    // Goal: "fastest observable tick" for latency-sensitive triggers.
    // Event timestamps across sources are not perfectly comparable, so we bias towards lower
    // `recv_ts_ms - event_ts_ms` latency (i.e. faster wire path), with a small guard against
    // extreme back-jumps in event time.
    let current_latency_ms = (current.recv_ts_ms - current_event).max(0) as f64;
    let next_latency_ms = (next.recv_ts_ms - next_event).max(0) as f64;

    const EVENT_BACKJUMP_TOL_MS: i64 = 250;
    const LATENCY_DOMINATE_MS: f64 = 20.0;
    const LATENCY_TIE_EPS_MS: f64 = 5.0;

    if next_event + EVENT_BACKJUMP_TOL_MS < current_event {
        // Allow a big latency improvement to override moderate event-time skew.
        return next_latency_ms + LATENCY_DOMINATE_MS < current_latency_ms;
    }

    if next_latency_ms + LATENCY_TIE_EPS_MS < current_latency_ms {
        return true;
    }
    if current_latency_ms + LATENCY_TIE_EPS_MS < next_latency_ms {
        return false;
    }

    // Within ~5ms latency tie: prefer the newer event time, otherwise keep first-arriving tick.
    next_event > current_event + 1
}

fn should_replace_anchor_tick(current: &RefTick, next: &RefTick) -> bool {
    let current_event = ref_event_ts_ms(current);
    let next_event = ref_event_ts_ms(next);
    if next_event + 50 < current_event {
        return false;
    }
    if next_event > current_event + 1 {
        return true;
    }
    next.recv_ts_local_ns > current.recv_ts_local_ns + 1_000_000
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

fn insert_latest_anchor_tick(latest_ticks: &mut HashMap<String, RefTick>, tick: RefTick) {
    match latest_ticks.get(tick.symbol.as_str()) {
        Some(current) => {
            if should_replace_anchor_tick(current, &tick) {
                latest_ticks.insert(tick.symbol.clone(), tick);
            }
        }
        None => {
            latest_ticks.insert(tick.symbol.clone(), tick);
        }
    }
}

fn insert_latest_ref_tick(
    latest_fast_ticks: &mut HashMap<String, RefTick>,
    latest_anchor_ticks: &mut HashMap<String, RefTick>,
    tick: RefTick,
) {
    if is_anchor_ref_source(tick.source.as_str()) {
        insert_latest_anchor_tick(latest_anchor_ticks, tick);
    } else {
        insert_latest_tick(latest_fast_ticks, tick);
    }
}

fn pick_latest_tick<'a>(
    latest_ticks: &'a HashMap<String, RefTick>,
    symbol: &str,
) -> Option<&'a RefTick> {
    latest_ticks
        .get(symbol)
        .or_else(|| latest_ticks.values().max_by_key(|t| t.recv_ts_ms))
}

async fn get_fee_rate_bps_cached(shared: &EngineShared, market_id: &str) -> f64 {
    const DEFAULT_FEE_BPS: f64 = 2.0;
    const TTL: Duration = Duration::from_secs(60);
    const REFRESH_BACKOFF: Duration = Duration::from_secs(3);

    let now = Instant::now();
    let (cached_fee, needs_refresh) =
        if let Some(entry) = shared.fee_cache.read().await.get(market_id).cloned() {
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
        if let Some((scoring_ok, raw)) = fetch_order_scoring(&http, &clob_endpoint, &order).await {
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
            // Conservative estimate: maker rebates are a fraction of taker fees and depend on
            // scoring + market share. Default rebate_factor is 0.0 unless explicitly configured.
            // Cap the pool fraction at 20% of fee_bps as a hard upper bound.
            entry.rebate_bps_est = (fee_bps.max(0.0) * 0.20 * hit_ratio * rebate_factor)
                .clamp(0.0, fee_bps.max(0.0));
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

fn evaluate_survival(shot: &ShadowShot, book: &BookTop) -> bool {
    let probe_px = if shot.survival_probe_price > 0.0 {
        shot.survival_probe_price
    } else {
        shot.intended_price
    };
    is_crossable(&shot.side, probe_px, book)
}

fn is_crossable(side: &OrderSide, probe_px: f64, book: &BookTop) -> bool {
    if probe_px <= 0.0 {
        return false;
    }
    match side {
        OrderSide::BuyYes => probe_px >= book.ask_yes,
        OrderSide::SellYes => probe_px <= book.bid_yes,
        OrderSide::BuyNo => probe_px >= book.ask_no,
        OrderSide::SellNo => probe_px <= book.bid_no,
    }
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
    survived: bool,
    queue_fill_prob: f64,
) -> EdgeAttribution {
    let spread = (book.ask_yes - book.bid_yes).max(0.0);
    if delay_ms >= 400 {
        return EdgeAttribution::StaleQuote;
    }
    if !survived {
        return EdgeAttribution::BookMoved;
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

fn spread_for_side(book: &BookTop, side: &OrderSide) -> f64 {
    match side {
        OrderSide::BuyYes | OrderSide::SellYes => (book.ask_yes - book.bid_yes).max(0.0),
        OrderSide::BuyNo | OrderSide::SellNo => (book.ask_no - book.bid_no).max(0.0),
    }
}

fn fair_for_side(fair_yes: f64, side: &OrderSide) -> f64 {
    match side {
        OrderSide::BuyYes | OrderSide::SellYes => fair_yes,
        OrderSide::BuyNo | OrderSide::SellNo => (1.0 - fair_yes).clamp(0.001, 0.999),
    }
}

fn edge_gross_bps_for_side(fair_yes: f64, side: &OrderSide, entry_price: f64) -> f64 {
    let fair = fair_for_side(fair_yes, side);
    let px = entry_price.max(1e-6);
    match side {
        OrderSide::BuyYes | OrderSide::BuyNo => ((fair - px) / px) * 10_000.0,
        OrderSide::SellYes | OrderSide::SellNo => ((px - fair) / px) * 10_000.0,
    }
}

fn edge_for_intent(fair_yes: f64, intent: &QuoteIntent) -> f64 {
    let px = intent.price.max(1e-6);
    let fair = match intent.side {
        OrderSide::BuyYes | OrderSide::SellYes => fair_yes,
        OrderSide::BuyNo | OrderSide::SellNo => (1.0 - fair_yes).clamp(0.001, 0.999),
    };
    match intent.side {
        // Expected edge vs. intended entry price in bps of entry.
        OrderSide::BuyYes | OrderSide::BuyNo => ((fair - px) / px) * 10_000.0,
        OrderSide::SellYes | OrderSide::SellNo => ((px - fair) / px) * 10_000.0,
    }
}

async fn build_toxicity_live_report(
    tox_state: Arc<RwLock<HashMap<String, MarketToxicState>>>,
    shadow_stats: Arc<ShadowStats>,
    execution: Arc<ClobExecution>,
    toxicity_cfg: Arc<RwLock<Arc<ToxicityConfig>>>,
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
        let markout_10s_bps = percentile_deque_capped(&st.markout_10s, 0.50, 2048).unwrap_or(0.0);
        let markout_samples = st
            .markout_1s
            .len()
            .max(st.markout_5s.len())
            .max(st.markout_10s.len());
        let market_score = if st.market_score > 0.0 {
            st.market_score
        } else {
            compute_market_score(&st, st.last_tox_score, markout_samples)
        };
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
    timeframes: Vec<String>,
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
            timeframes: vec![
                "5m".to_string(),
                "15m".to_string(),
                "1h".to_string(),
                "1d".to_string(),
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
                "min_eval_notional_usdc" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.min_eval_notional_usdc = parsed.max(0.0);
                    }
                }
                "min_expected_edge_usdc" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.min_expected_edge_usdc = parsed.max(0.0);
                    }
                }
                _ => {}
            }
        }
    }
    cfg
}

fn load_fusion_config() -> FusionConfig {
    let path = Path::new("configs/strategy.toml");
    let Ok(raw) = fs::read_to_string(path) else {
        return FusionConfig::default();
    };
    let mut cfg = FusionConfig::default();
    let mut in_section = false;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            in_section = line == "[fusion]";
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
            "enable_udp" => {
                if let Ok(parsed) = val.parse::<bool>() {
                    cfg.enable_udp = parsed;
                }
            }
            "mode" => {
                let norm = val.to_ascii_lowercase();
                cfg.mode = match norm.as_str() {
                    "active_active" | "direct_only" | "udp_only" => norm,
                    _ => cfg.mode,
                };
            }
            "udp_port" => {
                if let Ok(parsed) = val.parse::<u16>() {
                    cfg.udp_port = parsed.max(1);
                }
            }
            "dedupe_window_ms" => {
                if let Ok(parsed) = val.parse::<i64>() {
                    cfg.dedupe_window_ms = parsed.clamp(0, 2_000);
                }
            }
            _ => {}
        }
    }
    cfg
}

fn load_edge_model_config() -> EdgeModelConfig {
    let path = Path::new("configs/strategy.toml");
    let Ok(raw) = fs::read_to_string(path) else {
        return EdgeModelConfig::default();
    };
    let mut cfg = EdgeModelConfig::default();
    let mut in_section = false;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            in_section = line == "[edge_model]";
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
            "model" => cfg.model = val.to_string(),
            "gate_mode" => cfg.gate_mode = val.to_string(),
            "version" => cfg.version = val.to_string(),
            "base_gate_bps" => {
                if let Ok(parsed) = val.parse::<f64>() {
                    cfg.base_gate_bps = parsed.max(0.0);
                }
            }
            "congestion_penalty_bps" => {
                if let Ok(parsed) = val.parse::<f64>() {
                    cfg.congestion_penalty_bps = parsed.max(0.0);
                }
            }
            "latency_penalty_bps" => {
                if let Ok(parsed) = val.parse::<f64>() {
                    cfg.latency_penalty_bps = parsed.max(0.0);
                }
            }
            "fail_cost_bps" => {
                if let Ok(parsed) = val.parse::<f64>() {
                    cfg.fail_cost_bps = parsed.max(0.0);
                }
            }
            _ => {}
        }
    }
    cfg
}

fn load_exit_config() -> ExitConfig {
    let path = Path::new("configs/strategy.toml");
    let Ok(raw) = fs::read_to_string(path) else {
        return ExitConfig::default();
    };
    let mut cfg = ExitConfig::default();
    let mut in_section = false;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            in_section = line == "[exit]";
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
            "time_stop_ms" => {
                if let Ok(parsed) = val.parse::<u64>() {
                    cfg.time_stop_ms = parsed.clamp(50, 60_000);
                }
            }
            "edge_decay_bps" => {
                if let Ok(parsed) = val.parse::<f64>() {
                    cfg.edge_decay_bps = parsed;
                }
            }
            "adverse_move_bps" => {
                if let Ok(parsed) = val.parse::<f64>() {
                    cfg.adverse_move_bps = parsed;
                }
            }
            "flatten_on_trigger" => {
                if let Ok(parsed) = val.parse::<bool>() {
                    cfg.flatten_on_trigger = parsed;
                }
            }
            _ => {}
        }
    }
    cfg
}

fn load_predator_c_config() -> PredatorCConfig {
    let path = Path::new("configs/strategy.toml");
    let Ok(raw) = fs::read_to_string(path) else {
        return PredatorCConfig::default();
    };

    let mut cfg = PredatorCConfig::default();

    let mut in_root = false;
    let mut in_dir = false;
    let mut in_sniper = false;
    let mut in_router = false;
    let mut in_compounder = false;

    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            in_root = line == "[predator_c]";
            in_dir = line == "[predator_c.direction_detector]";
            in_sniper = line == "[predator_c.taker_sniper]";
            in_router = line == "[predator_c.router]";
            in_compounder = line == "[predator_c.compounder]";
            continue;
        }
        if !(in_root || in_dir || in_sniper || in_router || in_compounder) {
            continue;
        }

        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim();
        let val = v.trim().trim_matches('"');

        if in_root {
            match key {
                "enabled" => {
                    if let Ok(parsed) = val.parse::<bool>() {
                        cfg.enabled = parsed;
                    }
                }
                "priority" => {
                    let norm = val.trim().to_ascii_lowercase();
                    cfg.priority = match norm.as_str() {
                        "maker_first" => PredatorCPriority::MakerFirst,
                        "taker_first" => PredatorCPriority::TakerFirst,
                        "taker_only" => PredatorCPriority::TakerOnly,
                        _ => cfg.priority.clone(),
                    };
                }
                _ => {}
            }
            continue;
        }

        if in_dir {
            match key {
                "window_max_sec" => {
                    if let Ok(parsed) = val.parse::<u64>() {
                        cfg.direction_detector.window_max_sec = parsed.max(10);
                    }
                }
                "threshold_5m_pct" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.direction_detector.threshold_5m_pct = parsed.max(0.0);
                    }
                }
                "threshold_15m_pct" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.direction_detector.threshold_15m_pct = parsed.max(0.0);
                    }
                }
                "threshold_1h_pct" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.direction_detector.threshold_1h_pct = parsed.max(0.0);
                    }
                }
                "threshold_1d_pct" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.direction_detector.threshold_1d_pct = parsed.max(0.0);
                    }
                }
                "lookback_short_sec" => {
                    if let Ok(parsed) = val.parse::<u64>() {
                        cfg.direction_detector.lookback_short_sec = parsed.max(1);
                    }
                }
                "lookback_long_sec" => {
                    if let Ok(parsed) = val.parse::<u64>() {
                        cfg.direction_detector.lookback_long_sec = parsed.max(1);
                    }
                }
                "min_sources_for_high_confidence" => {
                    if let Ok(parsed) = val.parse::<usize>() {
                        cfg.direction_detector.min_sources_for_high_confidence = parsed.max(1);
                    }
                }
                "min_ticks_for_signal" => {
                    if let Ok(parsed) = val.parse::<usize>() {
                        cfg.direction_detector.min_ticks_for_signal = parsed.max(1);
                    }
                }
                _ => {}
            }
            continue;
        }

        if in_sniper {
            match key {
                "min_direction_confidence" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.taker_sniper.min_direction_confidence = parsed.clamp(0.0, 1.0);
                    }
                }
                "min_edge_net_bps" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.taker_sniper.min_edge_net_bps = parsed.max(0.0);
                    }
                }
                "max_spread" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.taker_sniper.max_spread = parsed.max(0.0001);
                    }
                }
                "cooldown_ms_per_market" => {
                    if let Ok(parsed) = val.parse::<u64>() {
                        cfg.taker_sniper.cooldown_ms_per_market = parsed;
                    }
                }
                _ => {}
            }
            continue;
        }

        if in_router {
            match key {
                "max_locked_pct_5m" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.router.max_locked_pct_5m = parsed.clamp(0.0, 1.0);
                    }
                }
                "max_locked_pct_15m" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.router.max_locked_pct_15m = parsed.clamp(0.0, 1.0);
                    }
                }
                "max_locked_pct_1h" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.router.max_locked_pct_1h = parsed.clamp(0.0, 1.0);
                    }
                }
                "max_locked_pct_1d" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.router.max_locked_pct_1d = parsed.clamp(0.0, 1.0);
                    }
                }
                "max_concurrent_positions" => {
                    if let Ok(parsed) = val.parse::<usize>() {
                        cfg.router.max_concurrent_positions = parsed.max(1);
                    }
                }
                "liquidity_reserve_pct" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.router.liquidity_reserve_pct = parsed.clamp(0.0, 0.95);
                    }
                }
                "max_order_notional_usdc" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.router.max_order_notional_usdc = parsed.max(0.0);
                    }
                }
                "max_total_notional_usdc" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.router.max_total_notional_usdc = parsed.max(0.0);
                    }
                }
                _ => {}
            }
            continue;
        }

        if in_compounder {
            match key {
                "enabled" => {
                    if let Ok(parsed) = val.parse::<bool>() {
                        cfg.compounder.enabled = parsed;
                    }
                }
                "initial_capital_usdc" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.compounder.initial_capital_usdc = parsed.max(0.0);
                    }
                }
                "compound_ratio" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.compounder.compound_ratio = parsed.clamp(0.0, 1.0);
                    }
                }
                "position_fraction" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.compounder.position_fraction = parsed.clamp(0.0, 1.0);
                    }
                }
                "min_quote_size" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.compounder.min_quote_size = parsed.max(0.0);
                    }
                }
                "daily_loss_cap_usdc" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.compounder.daily_loss_cap_usdc = parsed.max(0.0);
                    }
                }
                _ => {}
            }
            continue;
        }
    }

    cfg
}

fn load_risk_limits_config() -> RiskLimits {
    let path = Path::new("configs/strategy.toml");
    let Ok(raw) = fs::read_to_string(path) else {
        println!("Warn: strategy.toml not found for risk config, using defaults");
        return RiskLimits::default();
    };

    let mut cfg = RiskLimits::default();
    // Set safer defaults than the hardcoded 1.5% if parsing fails
    cfg.max_drawdown_pct = 0.20;
    cfg.max_asset_notional = 50.0;
    cfg.max_market_notional = 10.0;

    let mut section = "";
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            section = line;
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim();
        let val = v.trim().trim_matches('"');

        match section {
            "[risk_controls.exposure_limits]" => {
                match key {
                    "max_total_exposure_usdc" => {
                        if let Ok(p) = val.parse::<f64>() { cfg.max_asset_notional = p.max(0.0); }
                    }
                    "max_per_market_exposure_usdc" => {
                        if let Ok(p) = val.parse::<f64>() { cfg.max_market_notional = p.max(0.0); }
                    }
                    "max_concurrent_positions" => {
                        if let Ok(p) = val.parse::<usize>() { cfg.max_open_orders = p.max(1); }
                    }
                    _ => {}
                }
            }
            "[risk_controls.kill_switch]" => {
                match key {
                    "max_drawdown_pct" => {
                        if let Ok(p) = val.parse::<f64>() { cfg.max_drawdown_pct = p.clamp(0.001, 1.0); }
                    }
                    _ => {}
                }
            }
            _ => {}
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
            "order_endpoint" => {
                let v = val.to_string();
                if v.is_empty() {
                    cfg.order_endpoint = None;
                } else {
                    cfg.order_endpoint = Some(v);
                }
            }
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
    if let Some(parsed) = parse_toml_array_for_key(&raw, "timeframes") {
        cfg.timeframes = parsed;
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

fn append_jsonl_sync(path: &Path, line: &str) {
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{line}");
    }
}

fn append_jsonl_line(path: &Path, line: String) {
    if let Some(tx) = JSONL_WRITER.get() {
        let req = JsonlWriteReq {
            path: path.to_path_buf(),
            line,
        };
        match tx.try_send(req) {
            Ok(_) => {
                let cap = JSONL_QUEUE_CAP.load(Ordering::Relaxed) as usize;
                JSONL_QUEUE_DEPTH.store(cap.saturating_sub(tx.capacity()) as u64, Ordering::Relaxed);
                return;
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(req)) => {
                metrics::counter!("io.jsonl.queue_full").increment(1);
                if JSONL_DROP_ON_FULL.load(Ordering::Relaxed) {
                    metrics::counter!("io.jsonl.dropped").increment(1);
                    return;
                }
                append_jsonl_sync(&req.path, &req.line);
                return;
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(req)) => {
                metrics::counter!("io.jsonl.queue_closed").increment(1);
                if JSONL_DROP_ON_FULL.load(Ordering::Relaxed) {
                    metrics::counter!("io.jsonl.dropped").increment(1);
                    return;
                }
                append_jsonl_sync(&req.path, &req.line);
                return;
            }
        }
    }
    append_jsonl_sync(path, &line);
}

fn append_jsonl(path: &Path, value: &serde_json::Value) {
    append_jsonl_line(path, value.to_string());
}

static LAST_LIVE_REPORT_PERSIST_MS: AtomicI64 = AtomicI64::new(0);

fn persist_live_report_files(live: &ShadowLiveReport) {
    // Throttle file persistence. /report/shadow/live can be polled at high frequency (storm tests)
    // and pretty-json serialization + fs::write per request is unnecessary and can destabilize the
    // process under load.
    let now_ms = Utc::now().timestamp_millis();
    let last_ms = LAST_LIVE_REPORT_PERSIST_MS.load(Ordering::Relaxed);
    if now_ms.saturating_sub(last_ms) < 1_000 {
        return;
    }
    if LAST_LIVE_REPORT_PERSIST_MS
        .compare_exchange(last_ms, now_ms, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return;
    }

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
    let mut total = 0_u64;
    let mut filled = 0_u64;
    for o in outcomes {
        if o.delay_ms != delay_ms {
            continue;
        }
        total = total.saturating_add(1);
        if o.fillable {
            filled = filled.saturating_add(1);
        }
    }
    if total == 0 {
        0.0
    } else {
        filled as f64 / total as f64
    }
}

fn survival_ratio(outcomes: &[ShadowOutcome], delay_ms: u64) -> f64 {
    let mut total = 0_u64;
    let mut survived = 0_u64;
    for o in outcomes {
        if o.delay_ms != delay_ms {
            continue;
        }
        total = total.saturating_add(1);
        if o.survived {
            survived = survived.saturating_add(1);
        }
    }
    if total == 0 {
        0.0
    } else {
        survived as f64 / total as f64
    }
}

fn build_market_scorecard(shots: &[ShadowShot], outcomes: &[ShadowOutcome]) -> Vec<MarketScoreRow> {
    const PRIMARY_DELAY_MS: u64 = 10;
    const MAX_ROWS: usize = 200;

    #[derive(Default)]
    struct Agg {
        market_id: String,
        symbol: String,
        shots_primary: usize,
        outcomes_primary: usize,
        filled_10ms: u64,
        total_10ms: u64,
        net_edges: Vec<f64>,
        pnl_10s: Vec<f64>,
        net_markout_10s_usdc: Vec<f64>,
        roi_notional_10s_bps: Vec<f64>,
    }

    // Single-pass aggregation to avoid O(N^2) cloning/filtering. This keeps /report/shadow/live
    // stable under stress polling.
    let mut by_key: HashMap<(String, String), Agg> = HashMap::new();

    for s in shots {
        if s.delay_ms != PRIMARY_DELAY_MS {
            continue;
        }
        let key = (s.market_id.clone(), s.symbol.clone());
        let entry = by_key.entry(key).or_insert_with(|| Agg {
            market_id: s.market_id.clone(),
            symbol: s.symbol.clone(),
            ..Agg::default()
        });
        entry.shots_primary = entry.shots_primary.saturating_add(1);
        entry.net_edges.push(s.edge_net_bps);
    }

    for o in outcomes {
        if o.delay_ms != PRIMARY_DELAY_MS {
            continue;
        }
        let key = (o.market_id.clone(), o.symbol.clone());
        let entry = by_key.entry(key).or_insert_with(|| Agg {
            market_id: o.market_id.clone(),
            symbol: o.symbol.clone(),
            ..Agg::default()
        });
        entry.outcomes_primary = entry.outcomes_primary.saturating_add(1);
        entry.total_10ms = entry.total_10ms.saturating_add(1);
        if o.fillable {
            entry.filled_10ms = entry.filled_10ms.saturating_add(1);
        }
        if let Some(v) = o.net_markout_10s_bps.or(o.pnl_10s_bps) {
            entry.pnl_10s.push(v);
        }
        if let Some(v) = o.net_markout_10s_usdc {
            entry.net_markout_10s_usdc.push(v);
        }
        if let Some(v) = o.roi_notional_10s_bps {
            entry.roi_notional_10s_bps.push(v);
        }
    }

    let mut rows = Vec::with_capacity(by_key.len());
    for (_, agg) in by_key {
        let fillability_10ms = if agg.total_10ms == 0 {
            0.0
        } else {
            agg.filled_10ms as f64 / agg.total_10ms as f64
        };
        rows.push(MarketScoreRow {
            market_id: agg.market_id,
            symbol: agg.symbol,
            shots: agg.shots_primary,
            outcomes: agg.outcomes_primary,
            fillability_10ms,
            net_edge_p50_bps: percentile(&agg.net_edges, 0.50).unwrap_or(0.0),
            net_edge_p10_bps: percentile(&agg.net_edges, 0.10).unwrap_or(0.0),
            pnl_10s_p50_bps: percentile(&agg.pnl_10s, 0.50).unwrap_or(0.0),
            net_markout_10s_usdc_p50: percentile(&agg.net_markout_10s_usdc, 0.50).unwrap_or(0.0),
            roi_notional_10s_bps_p50: percentile(&agg.roi_notional_10s_bps, 0.50).unwrap_or(0.0),
        });
    }

    rows.sort_by(|a, b| {
        b.net_markout_10s_usdc_p50
            .total_cmp(&a.net_markout_10s_usdc_p50)
    });
    rows.truncate(MAX_ROWS);
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
        assert_eq!(
            classify_execution_style(&book, &maker),
            ExecutionStyle::Maker
        );
        assert_eq!(
            classify_execution_style(&book, &taker),
            ExecutionStyle::Taker
        );
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
            source_seq: now_ms as u64,
            event_ts_ms: now_ms - 300,
            recv_ts_ms: now_ms - 200,
            event_ts_exchange_ms: now_ms - 300,
            recv_ts_local_ns: now - 200_000_000,
            ingest_ts_local_ns: now - 198_000_000,
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
        assert!(sample.feed_in_ms >= 1.0);
        assert!(sample.feed_in_ms <= 4.0);
        assert!((sample.feed_in_ms - sample.ref_decode_ms).abs() < 0.0001);
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
        assert!(should_force_taker(
            &cfg, &safe, 12.0, 0.8, 30, 0.1, 40, 30, "BTCUSDT"
        ));
        assert!(!should_force_taker(
            &cfg, &safe, 8.0, 0.8, 30, 0.1, 40, 30, "BTCUSDT"
        ));
        assert!(!should_force_taker(
            &cfg, &safe, 12.0, 0.4, 30, 0.1, 40, 30, "BTCUSDT"
        ));

        let caution = ToxicDecision {
            regime: ToxicRegime::Caution,
            ..safe.clone()
        };
        assert!(!should_force_taker(
            &cfg, &caution, 12.0, 0.8, 30, 0.1, 40, 30, "BTCUSDT"
        ));
        cfg.market_tier_profile = "latency_aggressive".to_string();
        assert!(should_force_taker(
            &cfg, &caution, 12.0, 0.8, 30, 0.1, 40, 30, "BTCUSDT"
        ));
    }

    #[test]
    fn adaptive_min_edge_bps_warmup_relaxes_threshold() {
        let relaxed = adaptive_min_edge_bps(5.0, 0.3, 0, 0.95, 0.0, 0.0, 0, 30);
        assert!(relaxed <= 1.0);
        let progressed = adaptive_min_edge_bps(5.0, 0.3, 0, 0.20, 0.0, 0.0, 25, 30);
        assert!(progressed >= relaxed);
    }

    #[test]
    fn sol_guard_observe_only_for_high_latency_or_spread() {
        let mut cfg = MakerConfig::default();
        cfg.market_tier_profile = "balanced_sol_guard".to_string();
        let tox = ToxicDecision {
            market_id: "m".to_string(),
            symbol: "SOLUSDT".to_string(),
            tox_score: 0.2,
            regime: ToxicRegime::Safe,
            reason_codes: vec![],
            ts_ns: 1,
        };
        assert!(should_observe_only_symbol(
            "SOLUSDT", &cfg, &tox, 120.0, 0.01, 180.0
        ));
        assert!(should_observe_only_symbol(
            "SOLUSDT", &cfg, &tox, 260.0, 0.01, 80.0
        ));
        assert!(should_observe_only_symbol(
            "SOLUSDT", &cfg, &tox, 120.0, 0.03, 80.0
        ));
        assert!(!should_observe_only_symbol(
            "SOLUSDT", &cfg, &tox, 120.0, 0.01, 80.0
        ));
        assert!(!should_observe_only_symbol(
            "BTCUSDT", &cfg, &tox, 260.0, 0.03, 999.0
        ));
    }

    #[test]
    fn quote_block_ratio_matches_contract_and_is_bounded() {
        assert_eq!(quote_block_ratio(0, 0), 0.0);
        assert_eq!(quote_block_ratio(10, 0), 0.0);
        assert_eq!(quote_block_ratio(0, 10), 1.0);

        let r = quote_block_ratio(10, 2);
        assert!(r > 0.0 && r < 1.0);
    }

    #[test]
    fn policy_block_ratio_matches_contract_and_is_bounded() {
        assert_eq!(policy_block_ratio(0, 0), 0.0);
        assert_eq!(policy_block_ratio(10, 0), 0.0);
        assert_eq!(policy_block_ratio(0, 10), 1.0);

        let r = policy_block_ratio(10, 2);
        assert!(r > 0.0 && r < 1.0);
    }

    #[test]
    fn uptime_pct_is_bounded() {
        let stats = ShadowStats::new();
        let u = stats.uptime_pct(Duration::from_secs(1));
        assert!((0.0..=100.0).contains(&u));
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
timeframes = ["5m", "15m", "1h", "1d"]
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
        let timeframes = parse_toml_array_for_key(raw, "timeframes").unwrap_or_default();
        assert_eq!(
            timeframes,
            vec![
                "5m".to_string(),
                "15m".to_string(),
                "1h".to_string(),
                "1d".to_string()
            ]
        );
    }

    #[test]
    fn parse_toml_array_for_key_returns_none_for_missing_or_empty() {
        let raw = "foo = 1\nassets = []\n";
        assert!(parse_toml_array_for_key(raw, "missing").is_none());
        assert!(parse_toml_array_for_key(raw, "assets").is_none());
    }
}
