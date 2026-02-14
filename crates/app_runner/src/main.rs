use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
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
    new_id, BookTop, ControlCommand, EdgeAttribution, EngineEvent, ExecutionVenue, FairValueModel,
    InventoryState, MarketFeed, MarketHealth, OrderSide, QuoteEval, QuotePolicy, RefPriceFeed,
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
use strategy_maker::{MakerConfig, MakerQuotePolicy};
use tokio::sync::{mpsc, RwLock};

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
    tox_state: Arc<RwLock<HashMap<String, MarketToxicState>>>,
    shadow_stats: Arc<ShadowStats>,
    perf_profile: Arc<RwLock<PerfProfile>>,
}

#[derive(Debug, Clone)]
struct FeeRateEntry {
    fee_bps: f64,
    fetched_at: Instant,
}

#[derive(Clone)]
struct EngineShared {
    latest_books: Arc<RwLock<HashMap<String, BookTop>>>,
    market_to_symbol: Arc<RwLock<HashMap<String, String>>>,
    token_to_symbol: Arc<RwLock<HashMap<String, String>>>,
    fee_cache: Arc<RwLock<HashMap<String, FeeRateEntry>>>,
    http: Client,
    strategy_cfg: Arc<RwLock<MakerConfig>>,
    fair_value_cfg: Arc<StdRwLock<BasisMrConfig>>,
    toxicity_cfg: Arc<RwLock<ToxicityConfig>>,
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
}

#[derive(Debug, Serialize)]
struct StrategyReloadResp {
    maker: MakerConfig,
    fair_value: BasisMrConfig,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PerfProfile {
    tail_guard: f64,
    io_flush_batch: usize,
    io_queue_capacity: usize,
    json_mode: String,
}

impl Default for PerfProfile {
    fn default() -> Self {
        Self {
            tail_guard: 0.99,
            io_flush_batch: 64,
            io_queue_capacity: 16_384,
            json_mode: "typed".to_string(),
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
    fillability_10ms: f64,
    net_edge_p50_bps: f64,
    net_edge_p10_bps: f64,
    pnl_10s_p50_bps_raw: f64,
    pnl_10s_p50_bps_robust: f64,
    pnl_10s_sample_count: usize,
    pnl_10s_outlier_ratio: f64,
    quote_block_ratio: f64,
    policy_block_ratio: f64,
    strategy_uptime_pct: f64,
    tick_to_ack_p99_ms: f64,
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
}

#[derive(Debug, Serialize)]
struct ShadowLiveReport {
    window_id: u64,
    window_shots: usize,
    window_outcomes: usize,
    gate_ready: bool,
    gate_fail_reasons: Vec<String>,
    started_at_ms: i64,
    elapsed_sec: u64,
    total_shots: usize,
    total_outcomes: usize,
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
    pnl_10s_sample_count: usize,
    pnl_10s_outlier_ratio: f64,
    quote_block_ratio: f64,
    policy_block_ratio: f64,
    queue_depth_p99: f64,
    event_backlog_p99: f64,
    tick_to_decision_p50_ms: f64,
    tick_to_decision_p90_ms: f64,
    tick_to_decision_p99_ms: f64,
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
struct MarketScoreRow {
    market_id: String,
    symbol: String,
    shots: usize,
    outcomes: usize,
    fillability_10ms: f64,
    net_edge_p50_bps: f64,
    net_edge_p10_bps: f64,
    pnl_10s_p50_bps: f64,
}

struct ShadowStats {
    window_id: AtomicU64,
    started_at: RwLock<Instant>,
    started_at_ms: AtomicI64,
    quote_attempted: AtomicU64,
    quote_blocked: AtomicU64,
    policy_blocked: AtomicU64,
    blocked_reasons: RwLock<HashMap<String, u64>>,
    ref_ticks_total: AtomicU64,
    book_ticks_total: AtomicU64,
    last_ref_tick_ms: AtomicI64,
    last_book_tick_ms: AtomicI64,
    shots: RwLock<Vec<ShadowShot>>,
    outcomes: RwLock<Vec<ShadowOutcome>>,
    tick_to_decision_ms: RwLock<Vec<f64>>,
    ack_only_ms: RwLock<Vec<f64>>,
    tick_to_ack_ms: RwLock<Vec<f64>>,
    feed_in_ms: RwLock<Vec<f64>>,
    signal_us: RwLock<Vec<f64>>,
    quote_us: RwLock<Vec<f64>>,
    risk_us: RwLock<Vec<f64>>,
    shadow_fill_ms: RwLock<Vec<f64>>,
    queue_depth: RwLock<Vec<f64>>,
    event_backlog: RwLock<Vec<f64>>,
    parse_us: RwLock<Vec<f64>>,
    io_queue_depth: RwLock<Vec<f64>>,
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
            blocked_reasons: RwLock::new(HashMap::new()),
            ref_ticks_total: AtomicU64::new(0),
            book_ticks_total: AtomicU64::new(0),
            last_ref_tick_ms: AtomicI64::new(0),
            last_book_tick_ms: AtomicI64::new(0),
            shots: RwLock::new(Vec::new()),
            outcomes: RwLock::new(Vec::new()),
            tick_to_decision_ms: RwLock::new(Vec::new()),
            ack_only_ms: RwLock::new(Vec::new()),
            tick_to_ack_ms: RwLock::new(Vec::new()),
            feed_in_ms: RwLock::new(Vec::new()),
            signal_us: RwLock::new(Vec::new()),
            quote_us: RwLock::new(Vec::new()),
            risk_us: RwLock::new(Vec::new()),
            shadow_fill_ms: RwLock::new(Vec::new()),
            queue_depth: RwLock::new(Vec::new()),
            event_backlog: RwLock::new(Vec::new()),
            parse_us: RwLock::new(Vec::new()),
            io_queue_depth: RwLock::new(Vec::new()),
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
        self.ref_ticks_total.store(0, Ordering::Relaxed);
        self.book_ticks_total.store(0, Ordering::Relaxed);
        self.last_ref_tick_ms.store(0, Ordering::Relaxed);
        self.last_book_tick_ms.store(0, Ordering::Relaxed);
        self.blocked_reasons.write().await.clear();
        self.shots.write().await.clear();
        self.outcomes.write().await.clear();
        self.tick_to_decision_ms.write().await.clear();
        self.ack_only_ms.write().await.clear();
        self.tick_to_ack_ms.write().await.clear();
        self.feed_in_ms.write().await.clear();
        self.signal_us.write().await.clear();
        self.quote_us.write().await.clear();
        self.risk_us.write().await.clear();
        self.shadow_fill_ms.write().await.clear();
        self.queue_depth.write().await.clear();
        self.event_backlog.write().await.clear();
        self.parse_us.write().await.clear();
        self.io_queue_depth.write().await.clear();
        window_id
    }

    async fn push_shot(&self, shot: ShadowShot) {
        append_jsonl(
            &dataset_path("normalized", "shadow_shots.jsonl"),
            &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "shot": shot}),
        );
        let mut shots = self.shots.write().await;
        push_capped(&mut shots, shot, Self::SHADOW_CAP);
    }

    async fn push_outcome(&self, outcome: ShadowOutcome) {
        append_jsonl(
            &dataset_path("normalized", "shadow_outcomes.jsonl"),
            &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "outcome": outcome}),
        );
        let mut outcomes = self.outcomes.write().await;
        push_capped(&mut outcomes, outcome, Self::SHADOW_CAP);
    }

    async fn push_tick_to_decision_ms(&self, ms: f64) {
        let mut v = self.tick_to_decision_ms.write().await;
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
}

fn is_quote_reject_reason(reason: &str) -> bool {
    reason == "execution_error" || reason.starts_with("exchange_reject")
}

fn is_policy_block_reason(reason: &str) -> bool {
    reason.starts_with("risk:")
        || reason.starts_with("market_")
        || reason.starts_with("no_quote_")
        || matches!(reason, "risk_capped_zero" | "edge_below_threshold")
}
impl ShadowStats {
    async fn build_live_report(&self) -> ShadowLiveReport {
        const PRIMARY_DELAY_MS: u64 = 10;
        let shots = self.shots.read().await.clone();
        let outcomes = self.outcomes.read().await.clone();
        let tick_to_decision_ms = self.tick_to_decision_ms.read().await.clone();
        let ack_only_ms = self.ack_only_ms.read().await.clone();
        let tick_to_ack_ms = self.tick_to_ack_ms.read().await.clone();
        let feed_in_ms = self.feed_in_ms.read().await.clone();
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
        let net_edges = shots_primary
            .iter()
            .map(|s| s.edge_net_bps)
            .collect::<Vec<_>>();
        let net_edge_p50 = percentile(&net_edges, 0.50).unwrap_or(0.0);
        let net_edge_p10 = percentile(&net_edges, 0.10).unwrap_or(0.0);
        let pnl_1s = outcomes_primary
            .iter()
            .filter_map(|o| o.net_markout_1s_bps.or(o.pnl_1s_bps))
            .collect::<Vec<_>>();
        let pnl_5s = outcomes_primary
            .iter()
            .filter_map(|o| o.net_markout_5s_bps.or(o.pnl_5s_bps))
            .collect::<Vec<_>>();
        let pnl_10s = outcomes_primary
            .iter()
            .filter_map(|o| o.net_markout_10s_bps.or(o.pnl_10s_bps))
            .collect::<Vec<_>>();
        let pnl_1s_p50 = percentile(&pnl_1s, 0.50).unwrap_or(0.0);
        let pnl_5s_p50 = percentile(&pnl_5s, 0.50).unwrap_or(0.0);
        let pnl_10s_p50_raw = percentile(&pnl_10s, 0.50).unwrap_or(0.0);
        let (pnl_10s_filtered, pnl_10s_outlier_ratio) = robust_filter_iqr(&pnl_10s);
        let pnl_10s_p50_robust = percentile(&pnl_10s_filtered, 0.50).unwrap_or(pnl_10s_p50_raw);
        let pnl_10s_sample_count = pnl_10s.len();

        let attempted = self.quote_attempted.load(Ordering::Relaxed);
        let blocked = self.quote_blocked.load(Ordering::Relaxed);
        let policy_blocked = self.policy_blocked.load(Ordering::Relaxed);
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
        };

        let mut live = ShadowLiveReport {
            window_id: self.window_id.load(Ordering::Relaxed),
            window_shots: shots.len(),
            window_outcomes: outcomes.len(),
            gate_ready: outcomes.len() >= Self::GATE_MIN_OUTCOMES,
            gate_fail_reasons: Vec::new(),
            started_at_ms: self.started_at_ms.load(Ordering::Relaxed),
            elapsed_sec: elapsed.as_secs(),
            total_shots: shots.len(),
            total_outcomes: outcomes.len(),
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
            pnl_10s_sample_count,
            pnl_10s_outlier_ratio,
            quote_block_ratio: block_ratio,
            policy_block_ratio,
            queue_depth_p99: percentile(&queue_depth, 0.99).unwrap_or(0.0),
            event_backlog_p99: percentile(&event_backlog, 0.99).unwrap_or(0.0),
            tick_to_decision_p50_ms: latency.tick_to_decision_p50_ms,
            tick_to_decision_p90_ms: latency.tick_to_decision_p90_ms,
            tick_to_decision_p99_ms: latency.tick_to_decision_p99_ms,
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
        live.gate_fail_reasons = compute_gate_fail_reasons(&live, Self::GATE_MIN_OUTCOMES);
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
            fillability_10ms: live.fillability_10ms,
            net_edge_p50_bps: live.net_edge_p50_bps,
            net_edge_p10_bps: live.net_edge_p10_bps,
            pnl_10s_p50_bps_raw: live.pnl_10s_p50_bps_raw,
            pnl_10s_p50_bps_robust: live.pnl_10s_p50_bps_robust,
            pnl_10s_sample_count: live.pnl_10s_sample_count,
            pnl_10s_outlier_ratio: live.pnl_10s_outlier_ratio,
            quote_block_ratio: live.quote_block_ratio,
            policy_block_ratio: live.policy_block_ratio,
            strategy_uptime_pct: live.strategy_uptime_pct,
            tick_to_ack_p99_ms: live.tick_to_ack_p99_ms,
            failed_reasons: failed,
        };
        ShadowFinalReport { live, gate }
    }
}

fn compute_gate_fail_reasons(live: &ShadowLiveReport, min_outcomes: usize) -> Vec<String> {
    let mut failed = Vec::new();
    if live.window_outcomes < min_outcomes {
        failed.push(format!(
            "gate_not_ready outcomes {} < {}",
            live.window_outcomes, min_outcomes
        ));
    }
    if live.fillability_10ms < 0.60 {
        failed.push(format!(
            "fillability@10ms {:.3} < 0.600",
            live.fillability_10ms
        ));
    }
    if live.net_edge_p50_bps <= 0.0 {
        failed.push(format!("net_edge_p50 {:.3} <= 0", live.net_edge_p50_bps));
    }
    if live.net_edge_p10_bps < -1.0 {
        failed.push(format!("net_edge_p10 {:.3} < -1", live.net_edge_p10_bps));
    }
    if live.pnl_10s_p50_bps_robust <= 0.0 {
        failed.push(format!(
            "pnl_10s_p50_robust {:.3} <= 0",
            live.pnl_10s_p50_bps_robust
        ));
    }
    if live.quote_block_ratio >= 0.10 {
        failed.push(format!(
            "quote_block_ratio {:.4} >= 0.10",
            live.quote_block_ratio
        ));
    }
    if live.strategy_uptime_pct < 99.0 {
        failed.push(format!("uptime {:.2}% < 99%", live.strategy_uptime_pct));
    }
    if live.tick_to_ack_p99_ms >= 450.0 {
        failed.push(format!(
            "tick_to_ack_p99 {:.3}ms >= 450ms",
            live.tick_to_ack_p99_ms
        ));
    }
    if live.ref_ticks_total == 0 {
        failed.push("ref_ticks_total == 0".to_string());
    }
    if live.book_ticks_total == 0 {
        failed.push("book_ticks_total == 0".to_string());
    }
    if live.ref_freshness_ms > 1_000 {
        failed.push(format!("ref_freshness_ms {} > 1000", live.ref_freshness_ms));
    }
    if live.book_freshness_ms > 1_500 {
        failed.push(format!("book_freshness_ms {} > 1500", live.book_freshness_ms));
    }
    failed
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

    let bus = RingBus::new(16_384);
    let portfolio = Arc::new(PortfolioBook::default());
    let execution = Arc::new(ClobExecution::new(
        ExecutionMode::Paper,
        "https://clob.polymarket.com".to_string(),
    ));
    let shadow = Arc::new(ShadowExecutor::default());
    let strategy_cfg = Arc::new(RwLock::new(MakerConfig::default()));
    let fair_value_cfg = Arc::new(StdRwLock::new(load_fair_value_config()));
    let toxicity_cfg = Arc::new(RwLock::new(ToxicityConfig::default()));
    let perf_profile = Arc::new(RwLock::new(load_perf_profile_config()));
    let tox_state = Arc::new(RwLock::new(HashMap::new()));
    let shadow_stats = Arc::new(ShadowStats::new());
    let paused = Arc::new(RwLock::new(false));
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
        tox_state: tox_state.clone(),
        shadow_stats: shadow_stats.clone(),
        perf_profile: perf_profile.clone(),
    };

    let shared = Arc::new(EngineShared {
        latest_books: Arc::new(RwLock::new(HashMap::new())),
        market_to_symbol: Arc::new(RwLock::new(HashMap::new())),
        token_to_symbol: Arc::new(RwLock::new(HashMap::new())),
        fee_cache: Arc::new(RwLock::new(HashMap::new())),
        http: Client::new(),
        strategy_cfg,
        fair_value_cfg,
        toxicity_cfg,
        tox_state,
        shadow_stats,
    });

    spawn_reference_feed(bus.clone(), shared.shadow_stats.clone());
    spawn_market_feed(bus.clone(), shared.shadow_stats.clone());
    spawn_strategy_engine(
        bus.clone(),
        portfolio,
        execution.clone(),
        shadow,
        paused,
        shared.clone(),
    );
    spawn_periodic_report_persistor(
        shared.shadow_stats.clone(),
        shared.tox_state.clone(),
        execution.clone(),
        shared.toxicity_cfg.clone(),
    );

    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/state/positions", get(positions))
        .route("/state/pnl", get(pnl))
        .route("/report/shadow/live", get(report_shadow_live))
        .route("/report/shadow/final", get(report_shadow_final))
        .route("/report/toxicity/live", get(report_toxicity_live))
        .route("/report/toxicity/final", get(report_toxicity_final))
        .route("/control/pause", post(pause))
        .route("/control/resume", post(resume))
        .route("/control/flatten", post(flatten))
        .route("/control/reset_shadow", post(reset_shadow))
        .route("/control/reload_strategy", post(reload_strategy))
        .route("/control/reload_toxicity", post(reload_toxicity))
        .route("/control/reload_perf_profile", post(reload_perf_profile))
        .with_state(state);

    let addr: SocketAddr = "0.0.0.0:8080".parse()?;
    tracing::info!(%addr, "control api started");
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;
    Ok(())
}

fn install_rustls_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

fn spawn_reference_feed(bus: RingBus<EngineEvent>, stats: Arc<ShadowStats>) {
    tokio::spawn(async move {
        let feed = MultiSourceRefFeed::new(Duration::from_millis(50));
        let symbols = vec![
            "BTCUSDT".to_string(),
            "ETHUSDT".to_string(),
            "SOLUSDT".to_string(),
            "XRPUSDT".to_string(),
        ];
        let Ok(mut stream) = feed.stream_ticks(symbols).await else {
            tracing::error!("reference feed failed to start");
            return;
        };

        while let Some(item) = stream.next().await {
            match item {
                Ok(tick) => {
                    stats.mark_ref_tick(tick.recv_ts_ms);
                    append_jsonl(
                        &dataset_path("raw", "ref_ticks.jsonl"),
                        &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "tick": tick}),
                    );
                    let _ = bus.publish(EngineEvent::RefTick(tick));
                }
                Err(err) => {
                    tracing::warn!(?err, "reference feed event error");
                }
            }
        }
    });
}

fn spawn_market_feed(bus: RingBus<EngineEvent>, stats: Arc<ShadowStats>) {
    tokio::spawn(async move {
        let feed = PolymarketFeed::new(Duration::from_millis(50));
        let Ok(mut stream) = feed.stream_books().await else {
            tracing::error!("market feed failed to start");
            return;
        };

        while let Some(item) = stream.next().await {
            match item {
                Ok(book) => {
                    stats.mark_book_tick(book.ts_ms);
                    append_jsonl(
                        &dataset_path("raw", "book_tops.jsonl"),
                        &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "book": book}),
                    );
                    let _ = bus.publish(EngineEvent::BookTop(book));
                }
                Err(err) => {
                    tracing::warn!(?err, "market feed event error");
                }
            }
        }
    });
}

fn spawn_periodic_report_persistor(
    stats: Arc<ShadowStats>,
    tox_state: Arc<RwLock<HashMap<String, MarketToxicState>>>,
    execution: Arc<ClobExecution>,
    toxicity_cfg: Arc<RwLock<ToxicityConfig>>,
) {
    tokio::spawn(async move {
        let mut last_final = Instant::now() - Duration::from_secs(600);
        loop {
            let live = stats.build_live_report().await;
            persist_live_report_files(&live);
            let tox_live = build_toxicity_live_report(
                tox_state.clone(),
                stats.clone(),
                execution.clone(),
                toxicity_cfg.clone(),
            )
            .await;
            persist_toxicity_report_files(&tox_live);

            if last_final.elapsed() >= Duration::from_secs(300) {
                let final_report = stats.build_final_report().await;
                persist_final_report_files(&final_report);
                last_final = Instant::now();
            }

            tokio::time::sleep(Duration::from_secs(30)).await;
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
        let risk = DefaultRiskManager::new(RiskLimits::default());
        let mut latest_ticks: HashMap<String, RefTick> = HashMap::new();
        let mut market_inventory: HashMap<String, InventoryState> = HashMap::new();
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
                    latest_ticks.insert(tick.symbol.clone(), tick);
                }
                EngineEvent::BookTop(book) => {
                    let parse_us = dispatch_start.elapsed().as_secs_f64() * 1_000_000.0;
                    shared.shadow_stats.push_parse_us(parse_us).await;
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

                    for fill in shadow.on_book(&book) {
                        portfolio.apply_fill(&fill);
                        let _ = bus.publish(EngineEvent::Fill(fill.clone()));
                        append_jsonl(
                            &dataset_path("normalized", "fills.jsonl"),
                            &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "fill": fill}),
                        );
                    }

                    if *paused.read().await {
                        shared.shadow_stats.record_issue("paused").await;
                        continue;
                    }

                    let symbol = pick_market_symbol(&shared, &book).await;
                    let Some(symbol) = symbol else {
                        {
                            let mut states = shared.tox_state.write().await;
                            let st = states.entry(book.market_id.clone()).or_default();
                            st.symbol_missing = st.symbol_missing.saturating_add(1);
                        }
                        shared.shadow_stats.record_issue("symbol_missing").await;
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

                    let feed_in_ms = estimate_feed_in_latency_ms(&tick, &book);
                    shared.shadow_stats.push_feed_in_ms(feed_in_ms).await;
                    metrics::histogram!("latency.feed_in_ms").record(feed_in_ms);

                    let tick_to_decision_start = Instant::now();
                    let signal_start = Instant::now();
                    let signal = fair.evaluate(&tick, &book);
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
                    append_jsonl(
                        &dataset_path("normalized", "tox_features.jsonl"),
                        &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "features": tox_features}),
                    );
                    append_jsonl(
                        &dataset_path("normalized", "tox_decisions.jsonl"),
                        &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "decision": tox_decision}),
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
                    append_jsonl(
                        &dataset_path("normalized", "market_health.jsonl"),
                        &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "health": market_health}),
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
                        let rebate_est_bps = estimate_rebate_bps(&book.market_id, fee_bps);
                        let edge_net = edge_gross - fee_bps + rebate_est_bps;
                        if edge_net < effective_min_edge_bps {
                            shared
                                .shadow_stats
                                .mark_blocked_with_reason("edge_below_threshold")
                                .await;
                            continue;
                        }

                        let tick_to_decision_ms =
                            tick_to_decision_start.elapsed().as_secs_f64() * 1_000.0;
                        let place_start = Instant::now();
                        match execution.place_order(intent.clone()).await {
                            Ok(ack) => {
                                let ack_only_ms = place_start.elapsed().as_secs_f64() * 1_000.0;
                                let tick_to_ack_ms = tick_to_decision_ms + ack_only_ms;
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
                                metrics::histogram!("latency.ack_only_ms").record(ack_only_ms);
                                metrics::histogram!("latency.tick_to_ack_ms")
                                    .record(tick_to_ack_ms);

                                let _ = bus.publish(EngineEvent::OrderAck(ack.clone()));
                                shadow.register_order(&ack, intent.clone());

                                for delay_ms in [5_u64, 10_u64, 25_u64] {
                                    let shot = ShadowShot {
                                        shot_id: new_id(),
                                        market_id: intent.market_id.clone(),
                                        symbol: symbol.clone(),
                                        side: intent.side.clone(),
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
                            Err(err) => {
                                shared
                                    .shadow_stats
                                    .mark_blocked_with_reason("execution_error")
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

        let outcome = ShadowOutcome {
            shot_id: shot.shot_id.clone(),
            market_id: shot.market_id.clone(),
            symbol: shot.symbol.clone(),
            side: shot.side.clone(),
            delay_ms: shot.delay_ms,
            fillable,
            slippage_bps,
            pnl_1s_bps,
            pnl_5s_bps,
            pnl_10s_bps,
            net_markout_1s_bps,
            net_markout_5s_bps,
            net_markout_10s_bps,
            queue_fill_prob,
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
            append_jsonl(
                &dataset_path("normalized", "quote_eval.jsonl"),
                &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "eval": eval}),
            );
            let _ = bus.publish(EngineEvent::QuoteEval(eval));
        }
        let _ = bus.publish(EngineEvent::ShadowOutcome(outcome));
    });
}

async fn refresh_market_symbol_map(shared: &EngineShared) {
    let discovery = MarketDiscovery::new(DiscoveryConfig::default());
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
fn edge_for_side(signal: &core_types::Signal, side: &OrderSide) -> f64 {
    match side {
        OrderSide::BuyYes => signal.edge_bps_bid,
        OrderSide::SellYes => signal.edge_bps_ask,
        OrderSide::BuyNo => signal.edge_bps_ask,
        OrderSide::SellNo => signal.edge_bps_bid,
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

fn estimate_rebate_bps(_market_id: &str, _fee_bps: f64) -> f64 {
    // Keep conservative by default; avoid optimistic rebate assumptions.
    0.0
}

fn net_markout(markout_bps: Option<f64>, shot: &ShadowShot) -> Option<f64> {
    markout_bps.map(|v| v - shot.fee_paid_bps + shot.rebate_est_bps)
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

fn push_capped<T>(dst: &mut Vec<T>, value: T, cap: usize) {
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

fn estimate_feed_in_latency_ms(tick: &RefTick, book: &BookTop) -> f64 {
    let now = now_ns();
    if tick.recv_ts_local_ns > 0 {
        return ((now - tick.recv_ts_local_ns).max(0) as f64) / 1_000_000.0;
    }
    let now_ms = now / 1_000_000;
    let anchor = tick.recv_ts_ms.max(book.ts_ms);
    (now_ms - anchor).max(0) as f64
}

async fn get_fee_rate_bps_cached(shared: &EngineShared, market_id: &str) -> f64 {
    const TTL: Duration = Duration::from_secs(60);
    if let Some(entry) = shared.fee_cache.read().await.get(market_id).cloned() {
        if entry.fetched_at.elapsed() < TTL {
            return entry.fee_bps;
        }
    }

    let fee_bps = fetch_fee_rate_bps(&shared.http, market_id)
        .await
        .unwrap_or(2.0);
    shared.fee_cache.write().await.insert(
        market_id.to_string(),
        FeeRateEntry {
            fee_bps,
            fetched_at: Instant::now(),
        },
    );
    fee_bps
}

async fn fetch_fee_rate_bps(http: &Client, market_id: &str) -> Option<f64> {
    let endpoints = [
        format!("https://clob.polymarket.com/fee-rate?market_id={market_id}"),
        format!("https://clob.polymarket.com/fee-rate?market={market_id}"),
        format!("https://clob.polymarket.com/fee-rate?token_id={market_id}"),
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

async fn health(State(state): State<AppState>) -> Json<HealthResp> {
    Json(HealthResp {
        status: "ok",
        paused: *state.paused.read().await,
    })
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let body = state.prometheus.render();
    ([("content-type", "text/plain; version=0.0.4")], body)
}

async fn positions(State(state): State<AppState>) -> Json<HashMap<String, portfolio::Position>> {
    Json(state.portfolio.positions())
}

async fn pnl(State(state): State<AppState>) -> Json<core_types::PnLSnapshot> {
    Json(state.portfolio.snapshot())
}

async fn pause(State(state): State<AppState>) -> impl IntoResponse {
    *state.paused.write().await = true;
    let _ = state
        .bus
        .publish(EngineEvent::Control(ControlCommand::Pause));
    Json(serde_json::json!({"ok": true, "paused": true}))
}

async fn resume(State(state): State<AppState>) -> impl IntoResponse {
    *state.paused.write().await = false;
    let _ = state
        .bus
        .publish(EngineEvent::Control(ControlCommand::Resume));
    Json(serde_json::json!({"ok": true, "paused": false}))
}

async fn flatten(State(state): State<AppState>) -> impl IntoResponse {
    match state.execution.flatten_all().await {
        Ok(_) => {
            let _ = state
                .bus
                .publish(EngineEvent::Control(ControlCommand::Flatten));
            Json(serde_json::json!({"ok": true})).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"ok": false, "error": err.to_string()})),
        )
            .into_response(),
    }
}

async fn reset_shadow(State(state): State<AppState>) -> impl IntoResponse {
    let window_id = state.shadow_stats.reset().await;
    state.tox_state.write().await.clear();
    Json(serde_json::json!({"ok": true, "shadow_reset": true, "window_id": window_id}))
}

async fn reload_strategy(
    State(state): State<AppState>,
    Json(req): Json<StrategyReloadReq>,
) -> Json<StrategyReloadResp> {
    let mut cfg = state.strategy_cfg.write().await;
    if let Some(v) = req.min_edge_bps {
        cfg.min_edge_bps = v.max(0.0);
    }
    if let Some(v) = req.ttl_ms {
        cfg.ttl_ms = v.max(50);
    }
    if let Some(v) = req.inventory_skew {
        cfg.inventory_skew = v.clamp(0.0, 1.0);
    }
    if let Some(v) = req.base_quote_size {
        cfg.base_quote_size = v.max(0.01);
    }
    if let Some(v) = req.max_spread {
        cfg.max_spread = v.max(0.0001);
    }
    let mut fair_cfg = state
        .fair_value_cfg
        .read()
        .map(|g| g.clone())
        .unwrap_or_else(|_| BasisMrConfig::default());
    if let Some(v) = req.basis_k_revert {
        fair_cfg.k_revert = v.clamp(0.0, 5.0);
    }
    if let Some(v) = req.basis_z_cap {
        fair_cfg.z_cap = v.clamp(0.5, 8.0);
    }
    if let Some(v) = req.basis_min_confidence {
        fair_cfg.min_confidence = v.clamp(0.0, 1.0);
    }
    if let Ok(mut guard) = state.fair_value_cfg.write() {
        *guard = fair_cfg.clone();
    }
    let maker_cfg = cfg.clone();
    drop(cfg);
    append_jsonl(
        &dataset_path("reports", "strategy_reload.jsonl"),
        &serde_json::json!({
            "ts_ms": Utc::now().timestamp_millis(),
            "maker": maker_cfg,
            "fair_value": fair_cfg
        }),
    );
    Json(StrategyReloadResp {
        maker: maker_cfg,
        fair_value: fair_cfg,
    })
}

async fn reload_toxicity(
    State(state): State<AppState>,
    Json(req): Json<ToxicityReloadReq>,
) -> Json<ToxicityConfig> {
    let mut cfg = state.toxicity_cfg.write().await;
    if let Some(v) = req.safe_threshold {
        cfg.safe_threshold = v.clamp(0.0, 1.0);
    }
    if let Some(v) = req.caution_threshold {
        cfg.caution_threshold = v.clamp(0.0, 1.0);
    }
    if let Some(v) = req.cooldown_min_sec {
        cfg.cooldown_min_sec = v.max(1);
    }
    if let Some(v) = req.cooldown_max_sec {
        cfg.cooldown_max_sec = v.max(cfg.cooldown_min_sec);
    }
    if let Some(v) = req.min_market_score {
        cfg.min_market_score = v.clamp(0.0, 100.0);
    }
    if let Some(v) = req.active_top_n_markets {
        cfg.active_top_n_markets = v;
    }
    if let Some(v) = req.markout_1s_caution_bps {
        cfg.markout_1s_caution_bps = v;
    }
    if let Some(v) = req.markout_5s_caution_bps {
        cfg.markout_5s_caution_bps = v;
    }
    if let Some(v) = req.markout_10s_caution_bps {
        cfg.markout_10s_caution_bps = v;
    }
    if let Some(v) = req.markout_1s_danger_bps {
        cfg.markout_1s_danger_bps = v;
    }
    if let Some(v) = req.markout_5s_danger_bps {
        cfg.markout_5s_danger_bps = v;
    }
    if let Some(v) = req.markout_10s_danger_bps {
        cfg.markout_10s_danger_bps = v;
    }
    if cfg.safe_threshold > cfg.caution_threshold {
        let safe = cfg.safe_threshold;
        cfg.safe_threshold = cfg.caution_threshold;
        cfg.caution_threshold = safe;
    }
    if cfg.markout_1s_caution_bps < cfg.markout_1s_danger_bps {
        let v = cfg.markout_1s_caution_bps;
        cfg.markout_1s_caution_bps = cfg.markout_1s_danger_bps;
        cfg.markout_1s_danger_bps = v;
    }
    if cfg.markout_5s_caution_bps < cfg.markout_5s_danger_bps {
        let v = cfg.markout_5s_caution_bps;
        cfg.markout_5s_caution_bps = cfg.markout_5s_danger_bps;
        cfg.markout_5s_danger_bps = v;
    }
    if cfg.markout_10s_caution_bps < cfg.markout_10s_danger_bps {
        let v = cfg.markout_10s_caution_bps;
        cfg.markout_10s_caution_bps = cfg.markout_10s_danger_bps;
        cfg.markout_10s_danger_bps = v;
    }
    append_jsonl(
        &dataset_path("reports", "toxicity_reload.jsonl"),
        &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "config": *cfg}),
    );
    Json(cfg.clone())
}

async fn reload_perf_profile(
    State(state): State<AppState>,
    Json(req): Json<PerfProfileReloadReq>,
) -> Json<PerfProfile> {
    let mut cfg = state.perf_profile.write().await;
    if let Some(v) = req.tail_guard {
        cfg.tail_guard = v.clamp(0.50, 0.9999);
    }
    if let Some(v) = req.io_flush_batch {
        cfg.io_flush_batch = v.clamp(1, 4096);
    }
    if let Some(v) = req.io_queue_capacity {
        cfg.io_queue_capacity = v.clamp(256, 262_144);
    }
    if let Some(v) = req.json_mode {
        cfg.json_mode = v;
    }
    append_jsonl(
        &dataset_path("reports", "perf_profile_reload.jsonl"),
        &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "config": *cfg}),
    );
    Json(cfg.clone())
}

async fn report_shadow_live(State(state): State<AppState>) -> Json<ShadowLiveReport> {
    let live = state.shadow_stats.build_live_report().await;
    persist_live_report_files(&live);
    Json(live)
}

async fn report_shadow_final(State(state): State<AppState>) -> Json<ShadowFinalReport> {
    let final_report = state.shadow_stats.build_final_report().await;
    persist_final_report_files(&final_report);
    Json(final_report)
}

async fn report_toxicity_live(State(state): State<AppState>) -> Json<ToxicityLiveReport> {
    let live = build_toxicity_live_report(
        state.tox_state.clone(),
        state.shadow_stats.clone(),
        state.execution.clone(),
        state.toxicity_cfg.clone(),
    )
    .await;
    persist_toxicity_report_files(&live);
    Json(live)
}

async fn report_toxicity_final(State(state): State<AppState>) -> Json<ToxicityFinalReport> {
    let cfg = state.toxicity_cfg.read().await.clone();
    let live = build_toxicity_live_report(
        state.tox_state.clone(),
        state.shadow_stats.clone(),
        state.execution.clone(),
        state.toxicity_cfg.clone(),
    )
    .await;
    let mut failed = Vec::new();
    if live
        .rows
        .iter()
        .filter(|r| r.active_for_quoting)
        .any(|r| r.regime == ToxicRegime::Danger || r.market_score < cfg.min_market_score)
    {
        failed.push("active_market_danger_or_low_score_present".to_string());
    }
    if live.average_tox_score > 0.65 {
        failed.push("average_tox_score_above_0.65".to_string());
    }
    let p50_markout = percentile(
        &live
            .rows
            .iter()
            .map(|r| r.markout_10s_bps)
            .collect::<Vec<_>>(),
        0.50,
    )
    .unwrap_or(0.0);
    let p25_markout = percentile(
        &live
            .rows
            .iter()
            .map(|r| r.markout_10s_bps)
            .collect::<Vec<_>>(),
        0.25,
    )
    .unwrap_or(0.0);
    if p50_markout <= 0.0 {
        failed.push(format!("pnl_10s_p50_bps {:.4} <= 0", p50_markout));
    }
    if p25_markout <= -20.0 {
        failed.push(format!("pnl_10s_p25_bps {:.4} <= -20", p25_markout));
    }
    let pass = failed.is_empty();
    let final_report = ToxicityFinalReport {
        pass,
        failed_reasons: failed,
        live,
    };
    persist_toxicity_report_files(&final_report.live);
    Json(final_report)
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

#[derive(Debug)]
struct JsonlWriteReq {
    path: PathBuf,
    line: String,
}

static JSONL_WRITER: OnceLock<mpsc::Sender<JsonlWriteReq>> = OnceLock::new();
static JSONL_QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);
static JSONL_QUEUE_CAP: AtomicU64 = AtomicU64::new(0);

async fn init_jsonl_writer(perf_profile: Arc<RwLock<PerfProfile>>) {
    if JSONL_WRITER.get().is_some() {
        return;
    }
    let cfg = perf_profile.read().await.clone();
    let (tx, mut rx) = mpsc::channel::<JsonlWriteReq>(cfg.io_queue_capacity.max(256));
    JSONL_QUEUE_CAP.store(cfg.io_queue_capacity.max(256) as u64, Ordering::Relaxed);
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
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                metrics::counter!("io.jsonl.queue_closed").increment(1);
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
    let _ = fs::write(latency_csv, latency_rows);

    let score_path = reports_dir.join("market_scorecard.csv");
    let mut score_rows = String::new();
    score_rows.push_str("market_id,symbol,shots,outcomes,fillability_10ms,net_edge_p50_bps,net_edge_p10_bps,pnl_10s_p50_bps\n");
    for row in &report.live.market_scorecard {
        score_rows.push_str(&format!(
            "{},{},{},{},{:.6},{:.6},{:.6},{:.6}\n",
            row.market_id,
            row.symbol,
            row.shots,
            row.outcomes,
            row.fillability_10ms,
            row.net_edge_p50_bps,
            row.net_edge_p10_bps,
            row.pnl_10s_p50_bps
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

        rows.push(MarketScoreRow {
            market_id,
            symbol,
            shots: market_shots_primary.len(),
            outcomes: market_outcomes_primary.len(),
            fillability_10ms: fillability_ratio(&market_outcomes, 10),
            net_edge_p50_bps: percentile(&net_edges, 0.50).unwrap_or(0.0),
            net_edge_p10_bps: percentile(&net_edges, 0.10).unwrap_or(0.0),
            pnl_10s_p50_bps: percentile(&pnl_10s, 0.50).unwrap_or(0.0),
        });
    }

    rows.sort_by(|a, b| b.net_edge_p50_bps.total_cmp(&a.net_edge_p50_bps));
    rows
}

fn percentile(values: &[f64], p: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let idx = ((sorted.len() - 1) as f64 * p.clamp(0.0, 1.0)).round() as usize;
    sorted.get(idx).copied()
}

fn robust_filter_iqr(values: &[f64]) -> (Vec<f64>, f64) {
    if values.len() < 5 {
        return (values.to_vec(), 0.0);
    }
    let q1 = percentile(values, 0.25).unwrap_or(0.0);
    let q3 = percentile(values, 0.75).unwrap_or(0.0);
    let iqr = (q3 - q1).max(1e-9);
    let lower = q1 - 1.5 * iqr;
    let upper = q3 + 1.5 * iqr;
    let filtered = values
        .iter()
        .copied()
        .filter(|v| *v >= lower && *v <= upper)
        .collect::<Vec<_>>();
    if filtered.is_empty() {
        return (values.to_vec(), 0.0);
    }
    let outlier_ratio = (values.len().saturating_sub(filtered.len())) as f64 / values.len() as f64;
    (filtered, outlier_ratio)
}

fn freshness_ms(now_ms: i64, last_ms: i64) -> i64 {
    if last_ms <= 0 {
        return i64::MAX;
    }
    (now_ms - last_ms).max(0)
}

fn estimate_uptime_pct(_elapsed: Duration) -> f64 {
    100.0
}

fn value_to_f64(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        serde_json::Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

fn now_ns() -> i64 {
    Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp_millis() * 1_000_000)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
