use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::{Duration, Instant};

use chrono::Utc;
use core_types::{
    BookTop, CapitalUpdate, Direction, DirectionSignal, EngineEvent, EnginePnLBreakdown,
    ExecutionStyle, ProbabilityEstimate, RefTick, Regime, ShadowOutcome, ShadowShot, Signal,
    SourceHealth, TimeframeClass, ToxicRegime,
};
use dashmap::DashMap;
use direction_detector::{DirectionConfig, DirectionDetector};
use execution_clob::ClobExecution;
use exit_manager::{ExitManager, ExitManagerConfig, ExitReason};
use fair_value::BasisMrConfig;
use infra_bus::RingBus;
use paper_executor::ShadowExecutor;
use portfolio::PortfolioBook;
use probability_engine::{ProbabilityEngine, ProbabilityEngineConfig};
use reqwest::Client;
use risk_engine::{DefaultRiskManager, RiskLimits};
use serde::{Deserialize, Serialize};
use settlement_compounder::{CompounderConfig, SettlementCompounder};
use strategy_maker::MakerConfig;
use taker_sniper::{TakerSniper, TakerSniperConfig};
use timeframe_router::{RouterConfig, TimeframeRouter};
use tokio::sync::RwLock;

use crate::engine_core::{is_gate_block_reason, is_policy_block_reason, is_quote_reject_reason};
use crate::paper_runtime::PaperRuntimeHandle;
use crate::gate_eval;
use crate::report_io::{
    append_jsonl, build_market_scorecard, dataset_path, fillability_ratio,
    next_normalized_ingest_seq, survival_ratio,
};
use crate::seat_runtime::SeatRuntimeHandle;
use crate::stats_utils::{
    freshness_ms, percentile, policy_block_ratio, push_capped, quote_block_ratio, ratio_u64,
    robust_filter_iqr,
};

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) paused: Arc<RwLock<bool>>,
    pub(crate) bus: RingBus<EngineEvent>,
    pub(crate) portfolio: Arc<PortfolioBook>,
    pub(crate) execution: Arc<ClobExecution>,
    pub(crate) _shadow: Arc<ShadowExecutor>,
    pub(crate) prometheus: metrics_exporter_prometheus::PrometheusHandle,
    pub(crate) strategy_cfg: Arc<RwLock<Arc<MakerConfig>>>,
    pub(crate) fair_value_cfg: Arc<StdRwLock<BasisMrConfig>>,
    pub(crate) toxicity_cfg: Arc<RwLock<Arc<ToxicityConfig>>>,
    pub(crate) allocator_cfg: Arc<RwLock<AllocatorConfig>>,
    pub(crate) risk_limits: Arc<StdRwLock<RiskLimits>>,
    pub(crate) tox_state: Arc<RwLock<HashMap<String, MarketToxicState>>>,
    pub(crate) shadow_stats: Arc<ShadowStats>,
    pub(crate) perf_profile: Arc<RwLock<PerfProfile>>,
    pub(crate) shared: Arc<EngineShared>,
    pub(crate) seat: Arc<SeatRuntimeHandle>,
    pub(crate) paper: Arc<PaperRuntimeHandle>,
}

#[derive(Debug, Clone)]
pub(crate) enum StrategyIngress {
    RefTick(RefTick),
    BookTop(BookTop),
}

#[derive(Debug, Clone)]
pub(crate) struct StrategyIngressMsg {
    pub(crate) enqueued_ns: i64,
    pub(crate) payload: StrategyIngress,
}

#[derive(Debug, Clone)]
pub(crate) struct FeeRateEntry {
    pub(crate) fee_bps: f64,
    pub(crate) fetched_at: Instant,
}

#[derive(Debug, Clone)]
pub(crate) struct ScoringState {
    pub(crate) rebate_bps_est: f64,
    pub(crate) fetched_at: Instant,
}

#[derive(Debug, Clone)]
pub(crate) struct SignalCacheEntry {
    pub(crate) signal: Signal,
    pub(crate) ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub(crate) enum PredatorCPriority {
    MakerFirst,
    #[default]
    TakerFirst,
    TakerOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct PredatorCConfig {
    pub(crate) enabled: bool,
    pub(crate) priority: PredatorCPriority,
    pub(crate) direction_detector: DirectionConfig,
    pub(crate) probability_engine: ProbabilityEngineConfig,
    pub(crate) taker_sniper: TakerSniperConfig,
    pub(crate) strategy_d: PredatorDConfig,
    pub(crate) regime: PredatorRegimeConfig,
    pub(crate) cross_symbol: PredatorCrossSymbolConfig,
    pub(crate) router: RouterConfig,
    pub(crate) compounder: CompounderConfig,
    #[serde(default)]
    pub(crate) v52: V52Config,
}

impl Default for PredatorCConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            priority: PredatorCPriority::TakerFirst,
            direction_detector: DirectionConfig::default(),
            probability_engine: ProbabilityEngineConfig::default(),
            taker_sniper: TakerSniperConfig::default(),
            strategy_d: PredatorDConfig::default(),
            regime: PredatorRegimeConfig::default(),
            cross_symbol: PredatorCrossSymbolConfig::default(),
            router: RouterConfig::default(),
            compounder: CompounderConfig::default(),
            v52: V52Config::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct V52Config {
    pub(crate) time_phase: V52TimePhaseConfig,
    pub(crate) execution: V52ExecutionConfig,
    pub(crate) dual_arb: V52DualArbConfig,
    pub(crate) reversal: V52ReversalConfig,
}

impl Default for V52Config {
    fn default() -> Self {
        Self {
            time_phase: V52TimePhaseConfig::default(),
            execution: V52ExecutionConfig::default(),
            dual_arb: V52DualArbConfig::default(),
            reversal: V52ReversalConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct V52TimePhaseConfig {
    pub(crate) early_min_ratio: f64,
    pub(crate) late_max_ratio: f64,
    pub(crate) early_size_scale: f64,
    pub(crate) maturity_size_scale: f64,
    pub(crate) late_size_scale: f64,
    pub(crate) allow_timeframes: Vec<String>,
}

impl Default for V52TimePhaseConfig {
    fn default() -> Self {
        Self {
            early_min_ratio: 0.55,
            late_max_ratio: 0.10,
            early_size_scale: 0.80,
            maturity_size_scale: 1.00,
            late_size_scale: 1.25,
            allow_timeframes: vec!["5m".to_string(), "15m".to_string()],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct V52ExecutionConfig {
    pub(crate) late_force_taker_remaining_ms: u64,
    pub(crate) maker_wait_ms_before_force: u64,
    pub(crate) apply_force_taker_in_maturity: bool,
    pub(crate) apply_force_taker_in_late: bool,
    pub(crate) alpha_window_enabled: bool,
    pub(crate) alpha_window_move_bps: f64,
    pub(crate) alpha_window_poll_ms: u64,
    pub(crate) alpha_window_max_wait_ms: u64,
    pub(crate) require_compounder_when_live: bool,
}

impl Default for V52ExecutionConfig {
    fn default() -> Self {
        Self {
            late_force_taker_remaining_ms: 30_000,
            maker_wait_ms_before_force: 800,
            apply_force_taker_in_maturity: true,
            apply_force_taker_in_late: true,
            alpha_window_enabled: true,
            alpha_window_move_bps: 3.0,
            alpha_window_poll_ms: 10,
            alpha_window_max_wait_ms: 1_000,
            require_compounder_when_live: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct V52DualArbConfig {
    pub(crate) enabled: bool,
    pub(crate) safety_margin_bps: f64,
    pub(crate) threshold: f64,
    pub(crate) fee_buffer_mode: String,
}

impl Default for V52DualArbConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            safety_margin_bps: 3.0,
            threshold: 0.99,
            fee_buffer_mode: "conservative_taker".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct V52ReversalConfig {
    pub(crate) same_market_opposite_first: bool,
}

impl Default for V52ReversalConfig {
    fn default() -> Self {
        Self {
            same_market_opposite_first: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct PredatorDConfig {
    pub(crate) enabled: bool,
    pub(crate) min_gap_bps: f64,
    pub(crate) min_edge_net_bps: f64,
    pub(crate) min_confidence: f64,
    pub(crate) max_notional_usdc: f64,
    pub(crate) cooldown_ms_per_market: u64,
}

impl Default for PredatorDConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_gap_bps: 25.0,
            min_edge_net_bps: 15.0,
            min_confidence: 0.65,
            max_notional_usdc: 25.0,
            cooldown_ms_per_market: 500,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct PredatorRegimeConfig {
    pub(crate) enabled: bool,
    pub(crate) active_min_confidence: f64,
    pub(crate) active_min_magnitude_pct: f64,
    pub(crate) defend_tox_score: f64,
    pub(crate) defend_on_toxic_danger: bool,
    pub(crate) defend_min_source_health: f64,
    pub(crate) quiet_min_edge_multiplier: f64,
    pub(crate) quiet_chunk_scale: f64,
}

impl Default for PredatorRegimeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            active_min_confidence: 0.75,
            active_min_magnitude_pct: 0.10,
            defend_tox_score: 0.70,
            defend_on_toxic_danger: true,
            defend_min_source_health: 0.45,
            quiet_min_edge_multiplier: 1.25,
            quiet_chunk_scale: 0.50,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct PredatorCrossSymbolConfig {
    pub(crate) enabled: bool,
    pub(crate) leader_symbol: String,
    pub(crate) follower_symbols: Vec<String>,
    pub(crate) min_leader_confidence: f64,
    pub(crate) min_leader_magnitude_pct: f64,
    pub(crate) follower_stale_confidence_max: f64,
    pub(crate) max_correlated_positions: usize,
}

impl Default for PredatorCrossSymbolConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            leader_symbol: "BTCUSDT".to_string(),
            follower_symbols: vec!["ETHUSDT".to_string(), "SOLUSDT".to_string()],
            min_leader_confidence: 0.80,
            min_leader_magnitude_pct: 0.12,
            follower_stale_confidence_max: 0.65,
            max_correlated_positions: 2,
        }
    }
}

#[derive(Clone)]
pub(crate) struct EngineShared {
    pub(crate) latest_books: Arc<RwLock<HashMap<String, BookTop>>>,
    pub(crate) latest_signals: Arc<DashMap<String, SignalCacheEntry>>,
    pub(crate) latest_fast_ticks: Arc<DashMap<String, RefTick>>,
    pub(crate) latest_anchor_ticks: Arc<DashMap<String, RefTick>>,
    pub(crate) market_to_symbol: Arc<RwLock<HashMap<String, String>>>,
    pub(crate) token_to_symbol: Arc<RwLock<HashMap<String, String>>>,
    pub(crate) market_to_timeframe: Arc<RwLock<HashMap<String, TimeframeClass>>>,
    pub(crate) symbol_to_markets: Arc<RwLock<HashMap<String, Vec<String>>>>,
    pub(crate) fee_cache: Arc<RwLock<HashMap<String, FeeRateEntry>>>,
    pub(crate) fee_refresh_inflight: Arc<RwLock<HashMap<String, Instant>>>,
    pub(crate) scoring_cache: Arc<RwLock<HashMap<String, ScoringState>>>,
    pub(crate) http: Client,
    pub(crate) clob_endpoint: String,
    pub(crate) strategy_cfg: Arc<RwLock<Arc<MakerConfig>>>,
    pub(crate) settlement_cfg: Arc<RwLock<SettlementConfig>>,
    pub(crate) source_health_cfg: Arc<RwLock<SourceHealthConfig>>,
    pub(crate) source_health_latest: Arc<RwLock<HashMap<String, SourceHealth>>>,
    pub(crate) settlement_prices: Arc<RwLock<HashMap<String, f64>>>,
    pub(crate) fusion_cfg: Arc<RwLock<FusionConfig>>,
    pub(crate) edge_model_cfg: Arc<RwLock<EdgeModelConfig>>,
    pub(crate) exit_cfg: Arc<RwLock<ExitConfig>>,
    pub(crate) fair_value_cfg: Arc<StdRwLock<BasisMrConfig>>,
    pub(crate) toxicity_cfg: Arc<RwLock<Arc<ToxicityConfig>>>,
    pub(crate) risk_manager: Arc<DefaultRiskManager>,
    pub(crate) risk_limits: Arc<StdRwLock<RiskLimits>>,
    pub(crate) universe_symbols: Arc<Vec<String>>,
    pub(crate) universe_market_types: Arc<Vec<String>>,
    pub(crate) universe_timeframes: Arc<Vec<String>>,
    pub(crate) rate_limit_rps: f64,
    pub(crate) tox_state: Arc<RwLock<HashMap<String, MarketToxicState>>>,
    pub(crate) shadow_stats: Arc<ShadowStats>,
    pub(crate) predator_cfg: Arc<RwLock<PredatorCConfig>>,
    pub(crate) predator_direction_detector: Arc<RwLock<DirectionDetector>>,
    pub(crate) predator_latest_direction: Arc<RwLock<HashMap<String, DirectionSignal>>>,
    pub(crate) predator_latest_probability: Arc<RwLock<HashMap<String, ProbabilityEstimate>>>,
    pub(crate) predator_probability_engine: Arc<RwLock<ProbabilityEngine>>,
    pub(crate) predator_taker_sniper: Arc<RwLock<TakerSniper>>,
    pub(crate) predator_d_last_fire_ms: Arc<RwLock<HashMap<String, i64>>>,
    pub(crate) predator_router: Arc<RwLock<TimeframeRouter>>,
    pub(crate) predator_compounder: Arc<RwLock<SettlementCompounder>>,
    pub(crate) predator_exit_manager: Arc<RwLock<ExitManager>>,
    /// WSS User Channel fill broadcaster — None in paper mode
    pub(crate) wss_fill_tx:
        Option<Arc<tokio::sync::broadcast::Sender<execution_clob::wss_user_feed::WssFillEvent>>>,
}

#[derive(Serialize)]
pub(crate) struct HealthResp {
    pub(crate) status: &'static str,
    pub(crate) paused: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct StrategyReloadReq {
    pub(crate) min_edge_bps: Option<f64>,
    pub(crate) ttl_ms: Option<u64>,
    pub(crate) inventory_skew: Option<f64>,
    pub(crate) base_quote_size: Option<f64>,
    pub(crate) max_spread: Option<f64>,
    pub(crate) basis_k_revert: Option<f64>,
    pub(crate) basis_z_cap: Option<f64>,
    pub(crate) basis_min_confidence: Option<f64>,
    pub(crate) taker_trigger_bps: Option<f64>,
    pub(crate) taker_max_slippage_bps: Option<f64>,
    pub(crate) stale_tick_filter_ms: Option<f64>,
    pub(crate) market_tier_profile: Option<String>,
    pub(crate) capital_fraction_kelly: Option<f64>,
    pub(crate) variance_penalty_lambda: Option<f64>,
    pub(crate) min_eval_notional_usdc: Option<f64>,
    pub(crate) min_expected_edge_usdc: Option<f64>,
    pub(crate) v52: Option<V52Config>,
    pub(crate) v52_time_phase: Option<V52TimePhaseConfig>,
    pub(crate) v52_execution: Option<V52ExecutionConfig>,
    pub(crate) v52_dual_arb: Option<V52DualArbConfig>,
    pub(crate) v52_reversal: Option<V52ReversalConfig>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct FusionReloadReq {
    pub(crate) enable_udp: Option<bool>,
    pub(crate) mode: Option<String>,
    pub(crate) udp_port: Option<u16>,
    pub(crate) dedupe_window_ms: Option<i64>,
    pub(crate) dedupe_price_bps: Option<f64>,
    pub(crate) udp_share_cap: Option<f64>,
    pub(crate) jitter_threshold_ms: Option<f64>,
    pub(crate) fallback_arm_duration_ms: Option<u64>,
    pub(crate) fallback_cooldown_sec: Option<u64>,
    pub(crate) udp_local_only: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct EdgeModelReloadReq {
    pub(crate) model: Option<String>,
    pub(crate) gate_mode: Option<String>,
    pub(crate) version: Option<String>,
    pub(crate) base_gate_bps: Option<f64>,
    pub(crate) congestion_penalty_bps: Option<f64>,
    pub(crate) latency_penalty_bps: Option<f64>,
    pub(crate) fail_cost_bps: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ProbabilityReloadReq {
    pub(crate) momentum_gain: Option<f64>,
    pub(crate) lag_penalty_per_ms: Option<f64>,
    pub(crate) confidence_floor: Option<f64>,
    pub(crate) sigma_annual: Option<f64>,
    pub(crate) horizon_sec: Option<f64>,
    pub(crate) drift_annual: Option<f64>,
    pub(crate) velocity_drift_gain: Option<f64>,
    pub(crate) acceleration_drift_gain: Option<f64>,
    pub(crate) fair_blend_weight: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SourceHealthReloadReq {
    pub(crate) min_samples: Option<u64>,
    pub(crate) gap_window_ms: Option<i64>,
    pub(crate) jitter_limit_ms: Option<f64>,
    pub(crate) deviation_limit_bps: Option<f64>,
    pub(crate) freshness_limit_ms: Option<f64>,
    pub(crate) min_score_for_trading: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ExitReloadReq {
    pub(crate) enabled: Option<bool>,
    pub(crate) t100ms_reversal_bps: Option<f64>,
    pub(crate) t300ms_reversal_bps: Option<f64>,
    pub(crate) convergence_exit_ratio: Option<f64>,
    pub(crate) time_stop_ms: Option<u64>,
    pub(crate) edge_decay_bps: Option<f64>,
    pub(crate) adverse_move_bps: Option<f64>,
    pub(crate) flatten_on_trigger: Option<bool>,
    pub(crate) t3_take_ratio: Option<f64>,
    pub(crate) t15_min_unrealized_usdc: Option<f64>,
    pub(crate) t60_true_prob_floor: Option<f64>,
    pub(crate) t300_force_exit_ms: Option<u64>,
    pub(crate) t300_hold_prob_threshold: Option<f64>,
    pub(crate) t300_hold_time_to_expiry_ms: Option<u64>,
    pub(crate) max_single_trade_loss_usdc: Option<f64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct StrategyReloadResp {
    pub(crate) maker: MakerConfig,
    pub(crate) fair_value: BasisMrConfig,
    pub(crate) v52: V52Config,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RiskReloadReq {
    pub(crate) max_market_notional: Option<f64>,
    pub(crate) max_asset_notional: Option<f64>,
    pub(crate) max_open_orders: Option<usize>,
    pub(crate) daily_drawdown_cap_pct: Option<f64>,
    pub(crate) max_loss_streak: Option<u32>,
    pub(crate) cooldown_sec: Option<u64>,
    pub(crate) progressive_enabled: Option<bool>,
    pub(crate) drawdown_tier1_ratio: Option<f64>,
    pub(crate) drawdown_tier2_ratio: Option<f64>,
    pub(crate) tier1_size_scale: Option<f64>,
    pub(crate) tier2_size_scale: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TakerReloadReq {
    pub(crate) trigger_bps: Option<f64>,
    pub(crate) max_slippage_bps: Option<f64>,
    pub(crate) stale_tick_filter_ms: Option<f64>,
    pub(crate) market_tier_profile: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct TakerReloadResp {
    pub(crate) trigger_bps: f64,
    pub(crate) max_slippage_bps: f64,
    pub(crate) stale_tick_filter_ms: f64,
    pub(crate) market_tier_profile: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AllocatorReloadReq {
    pub(crate) capital_fraction_kelly: Option<f64>,
    pub(crate) variance_penalty_lambda: Option<f64>,
    pub(crate) active_top_n_markets: Option<usize>,
    pub(crate) taker_weight: Option<f64>,
    pub(crate) maker_weight: Option<f64>,
    pub(crate) arb_weight: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AllocatorConfig {
    pub(crate) capital_fraction_kelly: f64,
    pub(crate) variance_penalty_lambda: f64,
    pub(crate) active_top_n_markets: usize,
    pub(crate) taker_weight: f64,
    pub(crate) maker_weight: f64,
    pub(crate) arb_weight: f64,
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
pub(crate) struct AllocatorReloadResp {
    pub(crate) allocator: AllocatorConfig,
}

#[derive(Debug, Serialize)]
pub(crate) struct RiskReloadResp {
    pub(crate) risk: RiskLimits,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ToxicityReloadReq {
    pub(crate) safe_threshold: Option<f64>,
    pub(crate) caution_threshold: Option<f64>,
    pub(crate) cooldown_min_sec: Option<u64>,
    pub(crate) cooldown_max_sec: Option<u64>,
    pub(crate) min_market_score: Option<f64>,
    pub(crate) active_top_n_markets: Option<usize>,
    pub(crate) markout_1s_caution_bps: Option<f64>,
    pub(crate) markout_5s_caution_bps: Option<f64>,
    pub(crate) markout_10s_caution_bps: Option<f64>,
    pub(crate) markout_1s_danger_bps: Option<f64>,
    pub(crate) markout_5s_danger_bps: Option<f64>,
    pub(crate) markout_10s_danger_bps: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PerfProfileReloadReq {
    pub(crate) tail_guard: Option<f64>,
    pub(crate) io_flush_batch: Option<usize>,
    pub(crate) io_queue_capacity: Option<usize>,
    pub(crate) json_mode: Option<String>,
    pub(crate) io_drop_on_full: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FusionConfig {
    pub(crate) enable_udp: bool,
    pub(crate) mode: String,
    pub(crate) udp_port: u16,
    pub(crate) dedupe_window_ms: i64,
    pub(crate) dedupe_price_bps: f64,
    pub(crate) udp_share_cap: f64,
    pub(crate) jitter_threshold_ms: f64,
    pub(crate) fallback_arm_duration_ms: u64,
    pub(crate) fallback_cooldown_sec: u64,
    pub(crate) udp_local_only: bool,
}

impl Default for FusionConfig {
    fn default() -> Self {
        Self {
            enable_udp: true,
            mode: "direct_only".to_string(),
            udp_port: 6666,
            dedupe_window_ms: 120,
            dedupe_price_bps: 0.2,
            udp_share_cap: 0.35,
            jitter_threshold_ms: 25.0,
            fallback_arm_duration_ms: 8_000,
            fallback_cooldown_sec: 300,
            udp_local_only: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EdgeModelConfig {
    pub(crate) model: String,
    pub(crate) gate_mode: String,
    pub(crate) version: String,
    pub(crate) base_gate_bps: f64,
    pub(crate) congestion_penalty_bps: f64,
    pub(crate) latency_penalty_bps: f64,
    pub(crate) fail_cost_bps: f64,
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
pub(crate) struct ExitConfig {
    pub(crate) enabled: bool,
    pub(crate) t100ms_reversal_bps: f64,
    pub(crate) t300ms_reversal_bps: f64,
    pub(crate) convergence_exit_ratio: f64,
    pub(crate) time_stop_ms: u64,
    pub(crate) edge_decay_bps: f64,
    pub(crate) adverse_move_bps: f64,
    pub(crate) flatten_on_trigger: bool,
    pub(crate) t3_take_ratio: f64,
    pub(crate) t15_min_unrealized_usdc: f64,
    pub(crate) t60_true_prob_floor: f64,
    pub(crate) t300_force_exit_ms: u64,
    pub(crate) t300_hold_prob_threshold: f64,
    pub(crate) t300_hold_time_to_expiry_ms: u64,
    pub(crate) max_single_trade_loss_usdc: f64,
}

impl Default for ExitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            t100ms_reversal_bps: -3.0,
            t300ms_reversal_bps: -2.0,
            convergence_exit_ratio: 0.85,
            time_stop_ms: 300_000,
            edge_decay_bps: -2.0,
            adverse_move_bps: -8.0,
            flatten_on_trigger: true,
            t3_take_ratio: 0.60,
            t15_min_unrealized_usdc: 0.0,
            t60_true_prob_floor: 0.70,
            t300_force_exit_ms: 300_000,
            t300_hold_prob_threshold: 0.95,
            t300_hold_time_to_expiry_ms: 300_000,
            max_single_trade_loss_usdc: 1.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SettlementConfig {
    pub(crate) enabled: bool,
    pub(crate) endpoint: String,
    pub(crate) required_for_live: bool,
    pub(crate) poll_interval_ms: u64,
    pub(crate) timeout_ms: u64,
    pub(crate) symbols: Vec<String>,
}

impl Default for SettlementConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: String::new(),
            required_for_live: true,
            poll_interval_ms: 1_000,
            timeout_ms: 800,
            symbols: vec![
                "BTCUSDT".to_string(),
                "ETHUSDT".to_string(),
                "SOLUSDT".to_string(),
                "XRPUSDT".to_string(),
            ],
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct LiveGateStatus {
    pub(crate) ready: bool,
    pub(crate) reason: String,
}

pub(crate) fn settlement_live_gate_status(cfg: &SettlementConfig) -> LiveGateStatus {
    if !cfg.required_for_live {
        return LiveGateStatus {
            ready: true,
            reason: "settlement_required_for_live_disabled".to_string(),
        };
    }
    if !cfg.enabled {
        return LiveGateStatus {
            ready: false,
            reason: "settlement_feed_disabled".to_string(),
        };
    }
    if cfg.endpoint.trim().is_empty() {
        return LiveGateStatus {
            ready: false,
            reason: "settlement_endpoint_empty".to_string(),
        };
    }
    LiveGateStatus {
        ready: true,
        reason: "ok".to_string(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SourceHealthConfig {
    pub(crate) min_samples: u64,
    pub(crate) gap_window_ms: i64,
    pub(crate) jitter_limit_ms: f64,
    pub(crate) deviation_limit_bps: f64,
    pub(crate) freshness_limit_ms: f64,
    pub(crate) min_score_for_trading: f64,
}

impl Default for SourceHealthConfig {
    fn default() -> Self {
        Self {
            min_samples: 64,
            gap_window_ms: 2_000,
            jitter_limit_ms: 12.0,
            deviation_limit_bps: 30.0,
            freshness_limit_ms: 2_000.0,
            min_score_for_trading: 0.45,
        }
    }
}

pub(crate) fn to_exit_manager_config(cfg: &ExitConfig) -> ExitManagerConfig {
    ExitManagerConfig {
        t100ms_reversal_bps: cfg.t100ms_reversal_bps,
        t300ms_reversal_bps: cfg.t300ms_reversal_bps,
        convergence_exit_ratio: cfg.convergence_exit_ratio.clamp(0.0, 1.0),
        t3_take_ratio: cfg.t3_take_ratio.clamp(0.0, 5.0),
        t15_min_unrealized_usdc: cfg.t15_min_unrealized_usdc,
        t60_true_prob_floor: cfg.t60_true_prob_floor.clamp(0.0, 1.0),
        t300_force_exit_ms: cfg.t300_force_exit_ms.max(1_000),
        t300_hold_prob_threshold: cfg.t300_hold_prob_threshold.clamp(0.0, 1.0),
        t300_hold_time_to_expiry_ms: cfg.t300_hold_time_to_expiry_ms.max(1_000),
        max_single_trade_loss_usdc: cfg.max_single_trade_loss_usdc.max(0.0),
    }
}

pub(crate) fn exit_reason_label(reason: ExitReason) -> &'static str {
    match reason {
        ExitReason::StopLoss => "stop_loss",
        ExitReason::Reversal100ms => "t_plus_100ms_reversal",
        ExitReason::Reversal300ms => "t_plus_300ms_reversal",
        // 价格收敛到公允价值 85%+，套利利润已吃满
        ExitReason::ConvergenceExit => "convergence_exit",
        ExitReason::TakeProfit3s => "t_plus_3s",
        ExitReason::TakeProfit15s => "t_plus_15s",
        ExitReason::ProbGuard60s => "t_plus_60s_prob_guard",
        ExitReason::ForceClose300s => "t_plus_300s_force",
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ExecutionConfig {
    pub(crate) mode: String,
    pub(crate) rate_limit_rps: f64,
    pub(crate) http_timeout_ms: u64,
    pub(crate) clob_endpoint: String,
    #[serde(default)]
    pub(crate) order_endpoint: Option<String>,
    #[serde(default)]
    pub(crate) order_backup_endpoint: Option<String>,
    #[serde(default)]
    pub(crate) order_failover_timeout_ms: u64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            mode: "paper".to_string(),
            rate_limit_rps: 15.0,
            http_timeout_ms: 3000,
            clob_endpoint: "https://clob.polymarket.com".to_string(),
            order_endpoint: None,
            order_backup_endpoint: None,
            order_failover_timeout_ms: 200,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PerfProfile {
    pub(crate) tail_guard: f64,
    pub(crate) io_flush_batch: usize,
    pub(crate) io_queue_capacity: usize,
    pub(crate) json_mode: String,
    pub(crate) io_drop_on_full: bool,
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
pub(crate) struct ToxicityConfig {
    pub(crate) w1: f64,
    pub(crate) w2: f64,
    pub(crate) w3: f64,
    pub(crate) w4: f64,
    pub(crate) w5: f64,
    pub(crate) w6: f64,
    pub(crate) safe_threshold: f64,
    pub(crate) caution_threshold: f64,
    pub(crate) k_spread: f64,
    pub(crate) cooldown_min_sec: u64,
    pub(crate) cooldown_max_sec: u64,
    pub(crate) min_market_score: f64,
    pub(crate) active_top_n_markets: usize,
    pub(crate) markout_1s_caution_bps: f64,
    pub(crate) markout_5s_caution_bps: f64,
    pub(crate) markout_10s_caution_bps: f64,
    pub(crate) markout_1s_danger_bps: f64,
    pub(crate) markout_5s_danger_bps: f64,
    pub(crate) markout_10s_danger_bps: f64,
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
pub(crate) struct MarketToxicState {
    pub(crate) symbol: String,
    pub(crate) markout_1s: VecDeque<f64>,
    pub(crate) markout_5s: VecDeque<f64>,
    pub(crate) markout_10s: VecDeque<f64>,
    pub(crate) attempted: u64,
    pub(crate) no_quote: u64,
    pub(crate) symbol_missing: u64,
    pub(crate) last_tox_score: f64,
    pub(crate) last_regime: ToxicRegime,
    pub(crate) market_score: f64,
    pub(crate) cooldown_until_ms: i64,
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
pub(crate) struct ToxicityMarketRow {
    pub(crate) market_rank: usize,
    pub(crate) market_id: String,
    pub(crate) symbol: String,
    pub(crate) tox_score: f64,
    pub(crate) regime: ToxicRegime,
    pub(crate) market_score: f64,
    pub(crate) markout_10s_bps: f64,
    pub(crate) no_quote_rate: f64,
    pub(crate) symbol_missing_rate: f64,
    pub(crate) pending_exposure: f64,
    pub(crate) active_for_quoting: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct ToxicityLiveReport {
    pub(crate) ts_ms: i64,
    pub(crate) average_tox_score: f64,
    pub(crate) safe_count: usize,
    pub(crate) caution_count: usize,
    pub(crate) danger_count: usize,
    pub(crate) rows: Vec<ToxicityMarketRow>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ToxicityFinalReport {
    pub(crate) pass: bool,
    pub(crate) failed_reasons: Vec<String>,
    pub(crate) live: ToxicityLiveReport,
}

#[derive(Debug, Serialize)]
pub(crate) struct GateEvaluation {
    pub(crate) window_id: u64,
    pub(crate) gate_ready: bool,
    pub(crate) min_outcomes: usize,
    pub(crate) pass: bool,
    pub(crate) data_valid_ratio: f64,
    pub(crate) seq_gap_rate: f64,
    pub(crate) ts_inversion_rate: f64,
    pub(crate) stale_tick_drop_ratio: f64,
    pub(crate) fillability_10ms: f64,
    pub(crate) net_edge_p50_bps: f64,
    pub(crate) net_edge_p10_bps: f64,
    pub(crate) net_markout_10s_usdc_p50: f64,
    pub(crate) roi_notional_10s_bps_p50: f64,
    pub(crate) pnl_10s_p50_bps_raw: f64,
    pub(crate) pnl_10s_p50_bps_robust: f64,
    pub(crate) pnl_10s_sample_count: usize,
    pub(crate) pnl_10s_outlier_ratio: f64,
    pub(crate) eligible_count: u64,
    pub(crate) executed_count: u64,
    pub(crate) executed_over_eligible: f64,
    pub(crate) ev_net_usdc_p50: f64,
    pub(crate) ev_net_usdc_p10: f64,
    pub(crate) ev_positive_ratio: f64,
    pub(crate) quote_block_ratio: f64,
    pub(crate) policy_block_ratio: f64,
    pub(crate) gate_blocked: u64,
    pub(crate) gate_block_ratio: f64,
    pub(crate) strategy_uptime_pct: f64,
    pub(crate) tick_to_ack_p99_ms: f64,
    pub(crate) decision_queue_wait_p99_ms: f64,
    pub(crate) decision_compute_p99_ms: f64,
    pub(crate) source_latency_p99_ms: f64,
    pub(crate) exchange_lag_p99_ms: f64,
    pub(crate) path_lag_p99_ms: f64,
    pub(crate) local_backlog_p99_ms: f64,
    pub(crate) failed_reasons: Vec<String>,
}

#[derive(Debug, Serialize, Clone, Default)]
pub(crate) struct LatencyBreakdown {
    pub(crate) feed_in_p50_ms: f64,
    pub(crate) feed_in_p90_ms: f64,
    pub(crate) feed_in_p99_ms: f64,
    pub(crate) signal_p50_us: f64,
    pub(crate) signal_p90_us: f64,
    pub(crate) signal_p99_us: f64,
    pub(crate) quote_p50_us: f64,
    pub(crate) quote_p90_us: f64,
    pub(crate) quote_p99_us: f64,
    pub(crate) risk_p50_us: f64,
    pub(crate) risk_p90_us: f64,
    pub(crate) risk_p99_us: f64,
    pub(crate) decision_queue_wait_p50_ms: f64,
    pub(crate) decision_queue_wait_p90_ms: f64,
    pub(crate) decision_queue_wait_p99_ms: f64,
    pub(crate) decision_compute_p50_ms: f64,
    pub(crate) decision_compute_p90_ms: f64,
    pub(crate) decision_compute_p99_ms: f64,
    pub(crate) tick_to_decision_p50_ms: f64,
    pub(crate) tick_to_decision_p90_ms: f64,
    pub(crate) tick_to_decision_p99_ms: f64,
    pub(crate) ack_only_p50_ms: f64,
    pub(crate) ack_only_p90_ms: f64,
    pub(crate) ack_only_p99_ms: f64,
    pub(crate) tick_to_ack_p50_ms: f64,
    pub(crate) tick_to_ack_p90_ms: f64,
    pub(crate) tick_to_ack_p99_ms: f64,
    pub(crate) parse_p99_us: f64,
    pub(crate) io_queue_p99_ms: f64,
    pub(crate) bus_lag_p99_ms: f64,
    pub(crate) shadow_fill_p50_ms: f64,
    pub(crate) shadow_fill_p90_ms: f64,
    pub(crate) shadow_fill_p99_ms: f64,
    pub(crate) source_latency_p50_ms: f64,
    pub(crate) source_latency_p90_ms: f64,
    pub(crate) source_latency_p99_ms: f64,
    pub(crate) exchange_lag_p50_ms: f64,
    pub(crate) exchange_lag_p90_ms: f64,
    pub(crate) exchange_lag_p99_ms: f64,
    pub(crate) path_lag_p50_ms: f64,
    pub(crate) path_lag_p90_ms: f64,
    pub(crate) path_lag_p99_ms: f64,
    pub(crate) path_lag_coverage_ratio: f64,
    pub(crate) book_latency_p50_ms: f64,
    pub(crate) book_latency_p90_ms: f64,
    pub(crate) book_latency_p99_ms: f64,
    pub(crate) local_backlog_p50_ms: f64,
    pub(crate) local_backlog_p90_ms: f64,
    pub(crate) local_backlog_p99_ms: f64,
    pub(crate) ref_decode_p50_ms: f64,
    pub(crate) ref_decode_p90_ms: f64,
    pub(crate) ref_decode_p99_ms: f64,
    /// `book_top_lag_ms` distribution *for the same primary-delay executed shots* that have a
    /// measured `ack_only_ms` (i.e. same sample set as capturable_window).
    pub(crate) book_top_lag_at_ack_p50_ms: f64,
    pub(crate) book_top_lag_at_ack_p90_ms: f64,
    pub(crate) book_top_lag_at_ack_p99_ms: f64,
    pub(crate) book_top_lag_at_ack_n: u64,
    pub(crate) capturable_window_p50_ms: f64,
    pub(crate) capturable_window_p75_ms: f64,
    pub(crate) capturable_window_p90_ms: f64,
    pub(crate) capturable_window_p99_ms: f64,
    pub(crate) alpha_window_p50_ms: f64,
    pub(crate) alpha_window_p90_ms: f64,
    pub(crate) alpha_window_p99_ms: f64,
    pub(crate) alpha_window_hit_ratio: f64,
    pub(crate) alpha_window_n: u64,
    /// Ratio of samples where capturable_window_ms > 0.0.
    pub(crate) profitable_window_ratio: f64,
    /// Number of capturable window samples included in the distribution.
    pub(crate) capturable_window_n: u64,
    /// Number of ack_only_ms samples (only meaningful in live execution).
    pub(crate) ack_only_n: u64,
    /// Number of tick_to_ack_ms samples.
    pub(crate) tick_to_ack_n: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct ShadowLiveReport {
    pub(crate) window_id: u64,
    pub(crate) window_shots: usize,
    pub(crate) window_outcomes: usize,
    // Backward-compatible strict gate bit used by existing risk/optimizer scripts.
    pub(crate) gate_ready: bool,
    // Explicit strict/effective split to avoid semantic ambiguity in external audits.
    pub(crate) gate_ready_strict: bool,
    pub(crate) gate_ready_effective: bool,
    pub(crate) gate_fail_reasons: Vec<String>,
    pub(crate) observe_only: bool,
    pub(crate) started_at_ms: i64,
    pub(crate) elapsed_sec: u64,
    pub(crate) total_shots: usize,
    pub(crate) total_outcomes: usize,
    pub(crate) data_valid_ratio: f64,
    pub(crate) seq_gap_rate: f64,
    pub(crate) ts_inversion_rate: f64,
    pub(crate) stale_tick_drop_ratio: f64,
    pub(crate) quote_attempted: u64,
    pub(crate) quote_blocked: u64,
    pub(crate) policy_blocked: u64,
    pub(crate) fillability_5ms: f64,
    pub(crate) fillability_10ms: f64,
    pub(crate) fillability_25ms: f64,
    pub(crate) survival_5ms: f64,
    pub(crate) survival_10ms: f64,
    pub(crate) survival_25ms: f64,
    // Survival probe is an "orderless" latency-arb competitiveness metric:
    // at T0 we observe a crossable top-of-book price, then check at +Δ whether
    // it is still crossable. This intentionally does not depend on order acks.
    pub(crate) survival_probe_5ms: f64,
    pub(crate) survival_probe_10ms: f64,
    pub(crate) survival_probe_25ms: f64,
    pub(crate) survival_probe_5ms_n: u64,
    pub(crate) survival_probe_10ms_n: u64,
    pub(crate) survival_probe_25ms_n: u64,
    pub(crate) net_edge_p50_bps: f64,
    pub(crate) net_edge_p10_bps: f64,
    pub(crate) pnl_1s_p50_bps: f64,
    pub(crate) pnl_5s_p50_bps: f64,
    pub(crate) pnl_10s_p50_bps: f64,
    pub(crate) pnl_10s_p50_bps_raw: f64,
    pub(crate) pnl_10s_p50_bps_robust: f64,
    pub(crate) net_markout_10s_usdc_p50: f64,
    pub(crate) roi_notional_10s_bps_p50: f64,
    pub(crate) pnl_10s_sample_count: usize,
    pub(crate) pnl_10s_outlier_ratio: f64,
    pub(crate) eligible_count: u64,
    pub(crate) executed_count: u64,
    pub(crate) executed_over_eligible: f64,
    pub(crate) ev_net_usdc_p50: f64,
    pub(crate) ev_net_usdc_p10: f64,
    pub(crate) ev_positive_ratio: f64,
    pub(crate) quote_block_ratio: f64,
    pub(crate) policy_block_ratio: f64,
    pub(crate) gate_blocked: u64,
    pub(crate) gate_block_ratio: f64,
    pub(crate) queue_depth_p99: f64,
    pub(crate) event_backlog_p99: f64,
    pub(crate) tick_to_decision_p50_ms: f64,
    pub(crate) tick_to_decision_p90_ms: f64,
    pub(crate) tick_to_decision_p99_ms: f64,
    pub(crate) decision_queue_wait_p99_ms: f64,
    pub(crate) decision_compute_p99_ms: f64,
    pub(crate) source_latency_p99_ms: f64,
    pub(crate) exchange_lag_p99_ms: f64,
    pub(crate) path_lag_p99_ms: f64,
    pub(crate) local_backlog_p99_ms: f64,
    pub(crate) lag_half_life_ms: f64,
    pub(crate) probability_total: u64,
    pub(crate) settlement_source_degraded_ratio: f64,
    pub(crate) settle_fast_delta_p50_bps: f64,
    pub(crate) settle_fast_delta_p90_bps: f64,
    pub(crate) probability_confidence_p50: f64,
    pub(crate) ack_only_p50_ms: f64,
    pub(crate) ack_only_p90_ms: f64,
    pub(crate) ack_only_p99_ms: f64,
    pub(crate) alpha_window_p50_ms: f64,
    pub(crate) alpha_window_p90_ms: f64,
    pub(crate) alpha_window_p99_ms: f64,
    pub(crate) alpha_window_hit_ratio: f64,
    pub(crate) alpha_window_n: u64,
    pub(crate) strategy_uptime_pct: f64,
    pub(crate) tick_to_ack_p99_ms: f64,
    pub(crate) ref_ticks_total: u64,
    pub(crate) book_ticks_total: u64,
    pub(crate) ref_freshness_ms: i64,
    pub(crate) book_freshness_ms: i64,
    pub(crate) book_top_lag_p50_ms: f64,
    pub(crate) book_top_lag_p90_ms: f64,
    pub(crate) book_top_lag_p99_ms: f64,
    pub(crate) book_top_lag_by_symbol_p50_ms: HashMap<String, f64>,
    pub(crate) survival_10ms_by_symbol: HashMap<String, f64>,
    pub(crate) survival_probe_10ms_by_symbol: HashMap<String, f64>,
    pub(crate) blocked_reason_counts: HashMap<String, u64>,
    pub(crate) policy_block_reason_distribution: HashMap<String, u64>,
    pub(crate) gate_block_reason_distribution: HashMap<String, u64>,
    pub(crate) source_mix_ratio: HashMap<String, f64>,
    pub(crate) udp_share_effective: f64,
    pub(crate) udp_local_drop_count: u64,
    pub(crate) share_cap_drop_count: u64,
    pub(crate) fallback_state: String,
    pub(crate) fallback_trigger_reason_distribution: HashMap<String, u64>,
    pub(crate) source_health: Vec<SourceHealth>,
    pub(crate) exit_reason_top: Vec<(String, u64)>,
    pub(crate) edge_model_version: String,
    pub(crate) latency: LatencyBreakdown,
    pub(crate) market_scorecard: Vec<MarketScoreRow>,
    pub(crate) predator_c_enabled: bool,
    pub(crate) direction_signals_up: u64,
    pub(crate) direction_signals_down: u64,
    pub(crate) direction_signals_neutral: u64,
    pub(crate) taker_sniper_fired: u64,
    pub(crate) taker_sniper_skipped: u64,
    pub(crate) predator_regime_active: u64,
    pub(crate) predator_regime_quiet: u64,
    pub(crate) predator_regime_defend: u64,
    pub(crate) predator_cross_symbol_fired: u64,
    pub(crate) taker_sniper_skip_reasons_top: Vec<(String, u64)>,
    pub(crate) last_30s_taker_fallback_count: u64,
    pub(crate) router_locked_by_tf_usdc: HashMap<String, f64>,
    pub(crate) capital_available_usdc: f64,
    pub(crate) capital_base_quote_size: f64,
    pub(crate) capital_halt: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct ShadowFinalReport {
    pub(crate) live: ShadowLiveReport,
    pub(crate) gate: GateEvaluation,
}

#[derive(Debug, Serialize, Clone)]
pub(crate) struct EnginePnlRow {
    pub(crate) engine: String,
    pub(crate) samples: usize,
    pub(crate) total_usdc: f64,
    pub(crate) p50_usdc: f64,
    pub(crate) p10_usdc: f64,
    pub(crate) positive_ratio: f64,
}

#[derive(Debug, Serialize, Clone)]
pub(crate) struct EnginePnlReport {
    pub(crate) window_id: u64,
    pub(crate) breakdown: EnginePnLBreakdown,
    pub(crate) rows: Vec<EnginePnlRow>,
}

#[derive(Debug, Serialize, Clone)]
pub(crate) struct MarketScoreRow {
    pub(crate) market_id: String,
    pub(crate) symbol: String,
    pub(crate) shots: usize,
    pub(crate) outcomes: usize,
    pub(crate) fillability_10ms: f64,
    pub(crate) net_edge_p50_bps: f64,
    pub(crate) net_edge_p10_bps: f64,
    pub(crate) pnl_10s_p50_bps: f64,
    pub(crate) net_markout_10s_usdc_p50: f64,
    pub(crate) roi_notional_10s_bps_p50: f64,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SurvivalProbeCounters {
    pub(crate) n_5: u64,
    pub(crate) s_5: u64,
    pub(crate) n_10: u64,
    pub(crate) s_10: u64,
    pub(crate) n_25: u64,
    pub(crate) s_25: u64,
}

impl SurvivalProbeCounters {
    pub(crate) fn record(&mut self, delay_ms: u64, survived: bool) {
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

    pub(crate) fn ratio(&self, delay_ms: u64) -> f64 {
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

    pub(crate) fn n(&self, delay_ms: u64) -> u64 {
        match delay_ms {
            5 => self.n_5,
            10 => self.n_10,
            25 => self.n_25,
            _ => 0,
        }
    }
}

pub(crate) struct ShadowStats {
    pub(crate) window_id: AtomicU64,
    pub(crate) started_at: RwLock<Instant>,
    pub(crate) started_at_ms: AtomicI64,
    pub(crate) quote_attempted: AtomicU64,
    pub(crate) quote_blocked: AtomicU64,
    pub(crate) policy_blocked: AtomicU64,
    pub(crate) seen_count: AtomicU64,
    pub(crate) candidate_count: AtomicU64,
    pub(crate) quoted_count: AtomicU64,
    pub(crate) eligible_count: AtomicU64,
    pub(crate) executed_count: AtomicU64,
    pub(crate) filled_count: AtomicU64,
    pub(crate) blocked_reasons: RwLock<HashMap<String, u64>>,
    pub(crate) exit_reasons: RwLock<HashMap<String, u64>>,
    pub(crate) ref_ticks_total: AtomicU64,
    pub(crate) book_ticks_total: AtomicU64,
    pub(crate) ref_source_counts: DashMap<String, u64>,
    pub(crate) ref_dedupe_dropped: AtomicU64,
    pub(crate) share_cap_drop_count: AtomicU64,
    pub(crate) udp_local_drop_count_baseline: AtomicU64,
    pub(crate) fallback_state_code: AtomicU64,
    pub(crate) fallback_trigger_reasons: RwLock<HashMap<String, u64>>,
    pub(crate) last_ref_tick_ms: AtomicI64,
    pub(crate) last_book_tick_ms: AtomicI64,
    pub(crate) shots: RwLock<Vec<ShadowShot>>,
    pub(crate) outcomes: RwLock<Vec<ShadowOutcome>>,
    pub(crate) samples: RwLock<ShadowSamples>,
    pub(crate) book_top_lag_by_symbol_ms: RwLock<HashMap<String, Vec<f64>>>,
    pub(crate) survival_probe_overall: RwLock<SurvivalProbeCounters>,
    pub(crate) survival_probe_by_symbol: RwLock<HashMap<String, SurvivalProbeCounters>>,
    pub(crate) data_total: AtomicU64,
    pub(crate) data_invalid: AtomicU64,
    pub(crate) seq_gap: AtomicU64,
    pub(crate) ts_inversion: AtomicU64,
    pub(crate) stale_tick_dropped: AtomicU64,
    pub(crate) loss_streak: AtomicU64,
    pub(crate) observe_only: AtomicBool,
    pub(crate) paused: AtomicBool,
    pub(crate) paused_since_ms: AtomicU64,
    pub(crate) paused_total_ms: AtomicU64,
    pub(crate) predator_c_enabled: AtomicBool,
    pub(crate) predator_dir_up: AtomicU64,
    pub(crate) predator_dir_down: AtomicU64,
    pub(crate) predator_dir_neutral: AtomicU64,
    pub(crate) predator_taker_fired: AtomicU64,
    pub(crate) predator_taker_skipped: AtomicU64,
    pub(crate) predator_regime_active: AtomicU64,
    pub(crate) predator_regime_quiet: AtomicU64,
    pub(crate) predator_regime_defend: AtomicU64,
    pub(crate) predator_cross_symbol_fired: AtomicU64,
    pub(crate) predator_last_30s_taker_fallback_count: AtomicU64,
    pub(crate) predator_taker_skip_reasons: RwLock<HashMap<String, u64>>,
    pub(crate) predator_router_locked_by_tf_usdc: RwLock<HashMap<String, f64>>,
    pub(crate) predator_capital: RwLock<CapitalUpdate>,
    pub(crate) predator_capital_halt: AtomicBool,
    pub(crate) probability_total: AtomicU64,
    pub(crate) probability_degraded: AtomicU64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ShadowSamples {
    pub(crate) decision_queue_wait_ms: Vec<f64>,
    pub(crate) decision_compute_ms: Vec<f64>,
    pub(crate) tick_to_decision_ms: Vec<f64>,
    pub(crate) ack_only_ms: Vec<f64>,
    pub(crate) tick_to_ack_ms: Vec<f64>,
    pub(crate) capturable_window_ms: Vec<f64>,
    pub(crate) alpha_window_ms: Vec<f64>,
    pub(crate) alpha_window_hit: Vec<f64>,
    pub(crate) feed_in_ms: Vec<f64>,
    pub(crate) source_latency_ms: Vec<f64>,
    pub(crate) exchange_lag_ms: Vec<f64>,
    pub(crate) path_lag_ms: Vec<f64>,
    pub(crate) book_latency_ms: Vec<f64>,
    pub(crate) local_backlog_ms: Vec<f64>,
    pub(crate) ref_decode_ms: Vec<f64>,
    pub(crate) book_top_lag_ms: Vec<f64>,
    pub(crate) signal_us: Vec<f64>,
    pub(crate) quote_us: Vec<f64>,
    pub(crate) risk_us: Vec<f64>,
    pub(crate) shadow_fill_ms: Vec<f64>,
    pub(crate) queue_depth: Vec<f64>,
    pub(crate) event_backlog: Vec<f64>,
    pub(crate) parse_us: Vec<f64>,
    pub(crate) io_queue_depth: Vec<f64>,
    pub(crate) settle_fast_delta_bps: Vec<f64>,
    pub(crate) probability_confidence: Vec<f64>,
}

impl ShadowStats {
    const SHADOW_CAP: usize = 200_000;
    const SAMPLE_CAP: usize = 65_536;
    pub(crate) const GATE_MIN_OUTCOMES: usize = 30;

    pub(crate) fn new() -> Self {
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
            ref_source_counts: DashMap::new(),
            ref_dedupe_dropped: AtomicU64::new(0),
            share_cap_drop_count: AtomicU64::new(0),
            udp_local_drop_count_baseline: AtomicU64::new(feed_udp::udp_local_drop_count()),
            fallback_state_code: AtomicU64::new(0),
            fallback_trigger_reasons: RwLock::new(HashMap::new()),
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
            predator_regime_active: AtomicU64::new(0),
            predator_regime_quiet: AtomicU64::new(0),
            predator_regime_defend: AtomicU64::new(0),
            predator_cross_symbol_fired: AtomicU64::new(0),
            predator_last_30s_taker_fallback_count: AtomicU64::new(0),
            predator_taker_skip_reasons: RwLock::new(HashMap::new()),
            predator_router_locked_by_tf_usdc: RwLock::new(HashMap::new()),
            predator_capital: RwLock::new(CapitalUpdate {
                available_usdc: 0.0,
                base_quote_size: 0.0,
                ts_ms: 0,
            }),
            predator_capital_halt: AtomicBool::new(false),
            probability_total: AtomicU64::new(0),
            probability_degraded: AtomicU64::new(0),
        }
    }

    pub(crate) async fn reset(&self) -> u64 {
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
        self.share_cap_drop_count.store(0, Ordering::Relaxed);
        self.udp_local_drop_count_baseline
            .store(feed_udp::udp_local_drop_count(), Ordering::Relaxed);
        self.fallback_state_code.store(0, Ordering::Relaxed);
        self.last_ref_tick_ms.store(0, Ordering::Relaxed);
        self.last_book_tick_ms.store(0, Ordering::Relaxed);
        self.blocked_reasons.write().await.clear();
        self.exit_reasons.write().await.clear();
        self.ref_source_counts.clear();
        self.fallback_trigger_reasons.write().await.clear();
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
        self.predator_regime_active.store(0, Ordering::Relaxed);
        self.predator_regime_quiet.store(0, Ordering::Relaxed);
        self.predator_regime_defend.store(0, Ordering::Relaxed);
        self.predator_cross_symbol_fired.store(0, Ordering::Relaxed);
        self.predator_last_30s_taker_fallback_count
            .store(0, Ordering::Relaxed);
        self.predator_taker_skip_reasons.write().await.clear();
        self.predator_router_locked_by_tf_usdc.write().await.clear();
        *self.predator_capital.write().await = CapitalUpdate {
            available_usdc: 0.0,
            base_quote_size: 0.0,
            ts_ms: 0,
        };
        self.predator_capital_halt.store(false, Ordering::Relaxed);
        self.probability_total.store(0, Ordering::Relaxed);
        self.probability_degraded.store(0, Ordering::Relaxed);
        window_id
    }

    #[inline]
    pub(crate) fn is_current_window_ts_ns(&self, ts_ns: i64) -> bool {
        let started_at_ms = self.started_at_ms.load(Ordering::Relaxed);
        if started_at_ms <= 0 {
            return true;
        }
        let ts_ms = ts_ns / 1_000_000;
        // 1ms tolerance for clock granularity and ordering jitter around reset.
        ts_ms.saturating_add(1) >= started_at_ms
    }

    pub(crate) async fn push_shot(&self, shot: ShadowShot) {
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

    pub(crate) async fn push_outcome(&self, outcome: ShadowOutcome) {
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

    pub(crate) fn loss_streak(&self) -> u32 {
        self.loss_streak
            .load(Ordering::Relaxed)
            .min(u32::MAX as u64) as u32
    }

    pub(crate) async fn push_depth_sample(
        &self,
        event_backlog: f64,
        queue_depth: f64,
        io_queue_depth: f64,
    ) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.event_backlog, event_backlog, Self::SAMPLE_CAP);
        push_capped(&mut s.queue_depth, queue_depth, Self::SAMPLE_CAP);
        push_capped(&mut s.io_queue_depth, io_queue_depth, Self::SAMPLE_CAP);
    }

    pub(crate) async fn push_latency_sample(
        &self,
        feed_in_ms: f64,
        source_latency_ms: f64,
        exchange_lag_ms: f64,
        path_lag_ms: f64,
        book_latency_ms: f64,
        local_backlog_ms: f64,
        ref_decode_ms: f64,
    ) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.feed_in_ms, feed_in_ms, Self::SAMPLE_CAP);
        push_capped(
            &mut s.source_latency_ms,
            source_latency_ms,
            Self::SAMPLE_CAP,
        );
        push_capped(&mut s.exchange_lag_ms, exchange_lag_ms, Self::SAMPLE_CAP);
        if path_lag_ms.is_finite() && path_lag_ms >= 0.0 {
            push_capped(&mut s.path_lag_ms, path_lag_ms, Self::SAMPLE_CAP);
        }
        push_capped(&mut s.book_latency_ms, book_latency_ms, Self::SAMPLE_CAP);
        push_capped(&mut s.local_backlog_ms, local_backlog_ms, Self::SAMPLE_CAP);
        push_capped(&mut s.ref_decode_ms, ref_decode_ms, Self::SAMPLE_CAP);
    }

    pub(crate) async fn push_decision_queue_wait_ms(&self, ms: f64) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.decision_queue_wait_ms, ms, Self::SAMPLE_CAP);
    }

    pub(crate) async fn push_decision_compute_ms(&self, ms: f64) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.decision_compute_ms, ms, Self::SAMPLE_CAP);
    }

    pub(crate) async fn push_tick_to_decision_ms(&self, ms: f64) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.tick_to_decision_ms, ms, Self::SAMPLE_CAP);
    }

    pub(crate) async fn push_ack_only_ms(&self, ms: f64) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.ack_only_ms, ms, Self::SAMPLE_CAP);
    }

    pub(crate) async fn push_tick_to_ack_ms(&self, ms: f64) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.tick_to_ack_ms, ms, Self::SAMPLE_CAP);
    }

    pub(crate) async fn push_capturable_window_ms(&self, ms: f64) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.capturable_window_ms, ms, Self::SAMPLE_CAP);
    }

    pub(crate) async fn push_alpha_window_sample(&self, ms: f64, hit: bool) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.alpha_window_ms, ms.max(0.0), Self::SAMPLE_CAP);
        push_capped(
            &mut s.alpha_window_hit,
            if hit { 1.0 } else { 0.0 },
            Self::SAMPLE_CAP,
        );
    }

    pub(crate) async fn push_book_top_lag_ms(&self, symbol: &str, ms: f64) {
        {
            let mut s = self.samples.write().await;
            push_capped(&mut s.book_top_lag_ms, ms, Self::SAMPLE_CAP);
        }

        let mut by_symbol = self.book_top_lag_by_symbol_ms.write().await;
        let entry = by_symbol.entry(symbol.to_string()).or_default();
        push_capped(entry, ms, 4_096);
    }

    pub(crate) fn book_top_lag_p50_ms_for_symbol_sync(&self, symbol: &str) -> Option<f64> {
        let by_symbol = self.book_top_lag_by_symbol_ms.try_read().ok()?;
        let samples = by_symbol.get(symbol)?;
        if samples.len() < 8 {
            return None;
        }
        let mut sorted = samples.clone();
        sorted.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let idx = (sorted.len() as f64 * 0.50) as usize;
        sorted.get(idx.min(sorted.len().saturating_sub(1))).copied()
    }

    pub(crate) async fn record_survival_probe(&self, symbol: &str, delay_ms: u64, survived: bool) {
        {
            let mut c = self.survival_probe_overall.write().await;
            c.record(delay_ms, survived);
        }
        let mut by_symbol = self.survival_probe_by_symbol.write().await;
        let entry = by_symbol.entry(symbol.to_string()).or_default();
        entry.record(delay_ms, survived);
    }

    pub(crate) async fn push_signal_us(&self, us: f64) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.signal_us, us, Self::SAMPLE_CAP);
    }

    pub(crate) async fn push_shadow_fill_ms(&self, ms: f64) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.shadow_fill_ms, ms, Self::SAMPLE_CAP);
    }

    pub(crate) async fn push_parse_us(&self, us: f64) {
        let mut s = self.samples.write().await;
        push_capped(&mut s.parse_us, us, Self::SAMPLE_CAP);
    }

    pub(crate) async fn push_probability_sample(&self, estimate: &ProbabilityEstimate) {
        self.probability_total.fetch_add(1, Ordering::Relaxed);
        if estimate.settlement_source_degraded {
            self.probability_degraded.fetch_add(1, Ordering::Relaxed);
        }
        let mut s = self.samples.write().await;
        let delta_bps =
            ((estimate.p_settle - estimate.p_fast).abs() * 10_000.0).clamp(0.0, 10_000.0);
        push_capped(&mut s.settle_fast_delta_bps, delta_bps, Self::SAMPLE_CAP);
        push_capped(
            &mut s.probability_confidence,
            estimate.confidence.clamp(0.0, 1.0),
            Self::SAMPLE_CAP,
        );
    }

    pub(crate) fn mark_attempted(&self) {
        self.quote_attempted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn mark_seen(&self) {
        self.seen_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn mark_candidate(&self) {
        self.candidate_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn mark_eligible(&self) {
        self.eligible_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn mark_executed(&self) {
        self.executed_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn mark_filled(&self, n: u64) {
        self.filled_count.fetch_add(n, Ordering::Relaxed);
    }

    pub(crate) fn mark_blocked(&self) {
        self.quote_blocked.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn mark_policy_blocked(&self) {
        self.policy_blocked.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) async fn mark_blocked_with_reason(&self, reason: &str) {
        if is_quote_reject_reason(reason) {
            self.mark_blocked();
        }
        if is_policy_block_reason(reason) {
            self.mark_policy_blocked();
        }
        let mut reasons = self.blocked_reasons.write().await;
        *reasons.entry(reason.to_string()).or_insert(0) += 1;
    }

    pub(crate) async fn record_issue(&self, reason: &str) {
        let mut reasons = self.blocked_reasons.write().await;
        *reasons.entry(reason.to_string()).or_insert(0) += 1;
    }

    pub(crate) fn mark_ref_tick(&self, source: &str, ts_ms: i64) {
        self.ref_ticks_total.fetch_add(1, Ordering::Relaxed);
        self.last_ref_tick_ms.store(ts_ms, Ordering::Relaxed);
        self.ref_source_counts
            .entry(source.to_string())
            .and_modify(|count| *count = count.saturating_add(1))
            .or_insert(1);
    }

    pub(crate) fn mark_ref_dedupe_dropped(&self) {
        self.ref_dedupe_dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn mark_share_cap_drop(&self) {
        self.share_cap_drop_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) async fn mark_fallback_trigger_reason(&self, reason: &str) {
        let mut reasons = self.fallback_trigger_reasons.write().await;
        *reasons.entry(reason.to_string()).or_insert(0) += 1;
    }

    pub(crate) fn set_fallback_state(&self, state: &str) {
        let code = match state {
            "ws_primary" => 0,
            "armed" => 1,
            "udp_fallback" => 2,
            "cooldown" => 3,
            _ => 0,
        };
        self.fallback_state_code.store(code, Ordering::Relaxed);
    }

    pub(crate) fn mark_book_tick(&self, ts_ms: i64) {
        self.book_ticks_total.fetch_add(1, Ordering::Relaxed);
        self.last_book_tick_ms.store(ts_ms, Ordering::Relaxed);
    }

    pub(crate) fn mark_data_validity(&self, valid: bool) {
        self.data_total.fetch_add(1, Ordering::Relaxed);
        if !valid {
            self.data_invalid.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn mark_ts_inversion(&self) {
        self.ts_inversion.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn mark_stale_tick_dropped(&self) {
        self.stale_tick_dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) async fn record_exit_reason(&self, reason: &str) {
        let mut reasons = self.exit_reasons.write().await;
        *reasons.entry(reason.to_string()).or_insert(0) += 1;
    }

    pub(crate) fn observe_only(&self) -> bool {
        self.observe_only.load(Ordering::Relaxed)
    }

    pub(crate) fn set_observe_only(&self, v: bool) {
        self.observe_only.store(v, Ordering::Relaxed);
    }

    pub(crate) fn set_paused(&self, v: bool) {
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

    pub(crate) fn set_predator_enabled(&self, v: bool) {
        self.predator_c_enabled.store(v, Ordering::Relaxed);
    }

    pub(crate) fn mark_predator_direction(&self, dir: &Direction) {
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

    pub(crate) fn mark_predator_taker_fired(&self) {
        self.predator_taker_fired.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn mark_predator_last_30s_taker_fallback(&self) {
        self.predator_last_30s_taker_fallback_count
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn mark_predator_regime(&self, regime: &Regime) {
        match regime {
            Regime::Active => {
                self.predator_regime_active.fetch_add(1, Ordering::Relaxed);
            }
            Regime::Quiet => {
                self.predator_regime_quiet.fetch_add(1, Ordering::Relaxed);
            }
            Regime::Defend => {
                self.predator_regime_defend.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn mark_predator_cross_symbol_fired(&self) {
        self.predator_cross_symbol_fired
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) async fn mark_predator_taker_skipped(&self, reason: &str) {
        self.predator_taker_skipped.fetch_add(1, Ordering::Relaxed);
        let mut reasons = self.predator_taker_skip_reasons.write().await;
        *reasons.entry(reason.to_string()).or_insert(0) += 1;
    }

    pub(crate) async fn set_predator_router_locked_by_tf_usdc(&self, locked: HashMap<String, f64>) {
        *self.predator_router_locked_by_tf_usdc.write().await = locked;
    }

    pub(crate) async fn set_predator_capital(&self, update: CapitalUpdate, halt: bool) {
        *self.predator_capital.write().await = update;
        self.predator_capital_halt.store(halt, Ordering::Relaxed);
    }

    pub(crate) fn uptime_pct(&self, elapsed: Duration) -> f64 {
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
    pub(crate) async fn build_live_report(&self) -> ShadowLiveReport {
        const PRIMARY_DELAY_MS: u64 = 10;
        let ShadowSamples {
            decision_queue_wait_ms,
            decision_compute_ms,
            tick_to_decision_ms,
            ack_only_ms,
            tick_to_ack_ms,
            capturable_window_ms,
            alpha_window_ms,
            alpha_window_hit,
            feed_in_ms,
            source_latency_ms,
            exchange_lag_ms,
            path_lag_ms,
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
            settle_fast_delta_bps,
            probability_confidence,
        } = self.samples.read().await.clone();
        let book_top_lag_by_symbol_ms = self.book_top_lag_by_symbol_ms.read().await.clone();
        let mut blocked_reason_counts = self.blocked_reasons.read().await.clone();
        let source_counts = self
            .ref_source_counts
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect::<HashMap<_, _>>();
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
        let probability_total = self.probability_total.load(Ordering::Relaxed);
        let probability_degraded = self.probability_degraded.load(Ordering::Relaxed);
        let settlement_source_degraded_ratio = if probability_total == 0 {
            0.0
        } else {
            probability_degraded as f64 / probability_total as f64
        };
        let settle_fast_delta_p50_bps = percentile(&settle_fast_delta_bps, 0.50).unwrap_or(0.0);
        let settle_fast_delta_p90_bps = percentile(&settle_fast_delta_bps, 0.90).unwrap_or(0.0);
        let probability_confidence_p50 = percentile(&probability_confidence, 0.50).unwrap_or(0.0);
        if dedupe_dropped > 0 {
            blocked_reason_counts.insert("ref_dedupe_dropped".to_string(), dedupe_dropped);
        }
        let mut policy_block_reason_distribution = HashMap::new();
        let mut gate_block_reason_distribution = HashMap::new();
        let mut gate_blocked: u64 = 0;
        if policy_blocked > 0 {
            for (reason, count) in &blocked_reason_counts {
                if is_policy_block_reason(reason.as_str()) {
                    policy_block_reason_distribution.insert(reason.clone(), *count);
                }
            }
        }
        for (reason, count) in &blocked_reason_counts {
            if is_gate_block_reason(reason.as_str()) {
                gate_block_reason_distribution.insert(reason.clone(), *count);
                gate_blocked = gate_blocked.saturating_add(*count);
            }
        }
        let gate_block_ratio = ratio_u64(gate_blocked, attempted.saturating_add(gate_blocked));
        let mut source_mix_ratio = HashMap::new();
        if ref_ticks_total > 0 {
            for (source, cnt) in &source_counts {
                source_mix_ratio.insert(
                    source.clone(),
                    (*cnt as f64 / ref_ticks_total as f64).clamp(0.0, 1.0),
                );
            }
        }
        let udp_share_effective = source_mix_ratio.get("binance_udp").copied().unwrap_or(0.0);
        let udp_local_drop_count = feed_udp::udp_local_drop_count()
            .saturating_sub(self.udp_local_drop_count_baseline.load(Ordering::Relaxed));
        let share_cap_drop_count = self.share_cap_drop_count.load(Ordering::Relaxed);
        let fallback_state = match self.fallback_state_code.load(Ordering::Relaxed) {
            1 => "armed",
            2 => "udp_fallback",
            3 => "cooldown",
            _ => "ws_primary",
        }
        .to_string();
        let fallback_trigger_reason_distribution =
            self.fallback_trigger_reasons.read().await.clone();
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
            exchange_lag_p50_ms: percentile(&exchange_lag_ms, 0.50).unwrap_or(0.0),
            exchange_lag_p90_ms: percentile(&exchange_lag_ms, 0.90).unwrap_or(0.0),
            exchange_lag_p99_ms: percentile(&exchange_lag_ms, 0.99).unwrap_or(0.0),
            path_lag_p50_ms: percentile(&path_lag_ms, 0.50).unwrap_or(0.0),
            path_lag_p90_ms: percentile(&path_lag_ms, 0.90).unwrap_or(0.0),
            path_lag_p99_ms: percentile(&path_lag_ms, 0.99).unwrap_or(0.0),
            path_lag_coverage_ratio: ratio_u64(
                path_lag_ms.len() as u64,
                source_latency_ms.len() as u64,
            ),
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
            alpha_window_p50_ms: percentile(&alpha_window_ms, 0.50).unwrap_or(0.0),
            alpha_window_p90_ms: percentile(&alpha_window_ms, 0.90).unwrap_or(0.0),
            alpha_window_p99_ms: percentile(&alpha_window_ms, 0.99).unwrap_or(0.0),
            alpha_window_hit_ratio: if alpha_window_hit.is_empty() {
                0.0
            } else {
                alpha_window_hit.iter().sum::<f64>() / alpha_window_hit.len() as f64
            },
            alpha_window_n: alpha_window_ms.len() as u64,
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
        let predator_regime_active = self.predator_regime_active.load(Ordering::Relaxed);
        let predator_regime_quiet = self.predator_regime_quiet.load(Ordering::Relaxed);
        let predator_regime_defend = self.predator_regime_defend.load(Ordering::Relaxed);
        let predator_cross_symbol_fired = self.predator_cross_symbol_fired.load(Ordering::Relaxed);
        let last_30s_taker_fallback_count = self
            .predator_last_30s_taker_fallback_count
            .load(Ordering::Relaxed);
        let skip_reasons = self.predator_taker_skip_reasons.read().await.clone();
        let mut skip_top = skip_reasons.into_iter().collect::<Vec<_>>();
        skip_top.sort_by(|a, b| b.1.cmp(&a.1));
        skip_top.truncate(10);
        let router_locked_by_tf_usdc = self.predator_router_locked_by_tf_usdc.read().await.clone();
        let capital = self.predator_capital.read().await.clone();
        let capital_halt = self.predator_capital_halt.load(Ordering::Relaxed);

        let gate_ready_strict = total_outcomes >= Self::GATE_MIN_OUTCOMES;
        let gate_ready_effective = gate_ready_strict || eligible_count > 0 || executed_count > 0;

        let mut live = ShadowLiveReport {
            window_id: self.window_id.load(Ordering::Relaxed),
            window_shots: total_shots,
            window_outcomes: total_outcomes,
            gate_ready: gate_ready_strict,
            gate_ready_strict,
            gate_ready_effective,
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
            gate_blocked,
            gate_block_ratio,
            queue_depth_p99: percentile(&queue_depth, 0.99).unwrap_or(0.0),
            event_backlog_p99: percentile(&event_backlog, 0.99).unwrap_or(0.0),
            tick_to_decision_p50_ms: latency.tick_to_decision_p50_ms,
            tick_to_decision_p90_ms: latency.tick_to_decision_p90_ms,
            tick_to_decision_p99_ms: latency.tick_to_decision_p99_ms,
            decision_queue_wait_p99_ms: latency.decision_queue_wait_p99_ms,
            decision_compute_p99_ms: latency.decision_compute_p99_ms,
            source_latency_p99_ms: latency.source_latency_p99_ms,
            exchange_lag_p99_ms: latency.exchange_lag_p99_ms,
            path_lag_p99_ms: latency.path_lag_p99_ms,
            local_backlog_p99_ms: latency.local_backlog_p99_ms,
            lag_half_life_ms,
            probability_total,
            settlement_source_degraded_ratio,
            settle_fast_delta_p50_bps,
            settle_fast_delta_p90_bps,
            probability_confidence_p50,
            ack_only_p50_ms: latency.ack_only_p50_ms,
            ack_only_p90_ms: latency.ack_only_p90_ms,
            ack_only_p99_ms: latency.ack_only_p99_ms,
            alpha_window_p50_ms: latency.alpha_window_p50_ms,
            alpha_window_p90_ms: latency.alpha_window_p90_ms,
            alpha_window_p99_ms: latency.alpha_window_p99_ms,
            alpha_window_hit_ratio: latency.alpha_window_hit_ratio,
            alpha_window_n: latency.alpha_window_n,
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
            policy_block_reason_distribution,
            gate_block_reason_distribution,
            source_mix_ratio,
            udp_share_effective,
            udp_local_drop_count,
            share_cap_drop_count,
            fallback_state,
            fallback_trigger_reason_distribution,
            source_health: Vec::new(),
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
            predator_regime_active,
            predator_regime_quiet,
            predator_regime_defend,
            predator_cross_symbol_fired,
            taker_sniper_skip_reasons_top: skip_top,
            last_30s_taker_fallback_count,
            router_locked_by_tf_usdc,
            capital_available_usdc: capital.available_usdc,
            capital_base_quote_size: capital.base_quote_size,
            capital_halt,
        };
        live.gate_fail_reasons =
            gate_eval::compute_gate_fail_reasons(&live, Self::GATE_MIN_OUTCOMES);
        live.gate_ready_strict = live.window_outcomes >= Self::GATE_MIN_OUTCOMES;
        live.gate_ready_effective =
            live.gate_ready_strict || live.eligible_count > 0 || live.executed_count > 0;
        live.gate_ready = live.gate_ready_strict;
        live
    }

    pub(crate) async fn build_final_report(&self) -> ShadowFinalReport {
        let live = self.build_live_report().await;
        let failed = live.gate_fail_reasons.clone();

        let gate = GateEvaluation {
            window_id: live.window_id,
            gate_ready: live.gate_ready_strict,
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
            gate_blocked: live.gate_blocked,
            gate_block_ratio: live.gate_block_ratio,
            strategy_uptime_pct: live.strategy_uptime_pct,
            tick_to_ack_p99_ms: live.tick_to_ack_p99_ms,
            decision_queue_wait_p99_ms: live.decision_queue_wait_p99_ms,
            decision_compute_p99_ms: live.decision_compute_p99_ms,
            source_latency_p99_ms: live.latency.source_latency_p99_ms,
            exchange_lag_p99_ms: live.latency.exchange_lag_p99_ms,
            path_lag_p99_ms: live.latency.path_lag_p99_ms,
            local_backlog_p99_ms: live.latency.local_backlog_p99_ms,
            failed_reasons: failed,
        };
        ShadowFinalReport { live, gate }
    }

    pub(crate) async fn build_engine_pnl_report(&self) -> EnginePnlReport {
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

pub(crate) fn build_engine_pnl_row(engine: &str, values: &[f64]) -> EnginePnlRow {
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
