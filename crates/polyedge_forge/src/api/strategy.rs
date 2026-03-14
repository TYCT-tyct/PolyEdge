use super::*;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::sync::OwnedSemaphorePermit;

#[derive(Debug, Clone, PartialEq)]
pub(super) struct StrategySample {
    pub(super) ts_ms: i64,
    pub(super) round_id: String,
    pub(super) remaining_ms: i64,
    pub(super) p_up: f64,
    pub(super) delta_pct: f64,
    pub(super) velocity: f64,
    pub(super) acceleration: f64,
    pub(super) bid_yes: f64,
    pub(super) ask_yes: f64,
    pub(super) bid_no: f64,
    pub(super) ask_no: f64,
    pub(super) spread_up: f64,
    pub(super) spread_down: f64,
    pub(super) spread_mid: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct StrategyRuntimeConfig {
    entry_threshold_base: f64,
    entry_threshold_cap: f64,
    spread_limit_prob: f64,
    entry_edge_prob: f64,
    entry_min_potential_cents: f64,
    entry_max_price_cents: f64,
    min_hold_ms: i64,
    stop_loss_cents: f64,
    reverse_signal_threshold: f64,
    reverse_signal_ticks: usize,
    trail_activate_profit_cents: f64,
    trail_drawdown_cents: f64,
    take_profit_near_max_cents: f64,
    endgame_take_profit_cents: f64,
    endgame_remaining_ms: i64,
    liquidity_widen_prob: f64,
    cooldown_ms: i64,
    max_entries_per_round: usize,
    max_exec_spread_cents: f64,
    slippage_cents_per_side: f64,
    fee_cents_per_side: f64,
    emergency_wide_spread_penalty_ratio: f64,
    stop_loss_grace_ticks: usize,
    stop_loss_hard_mult: f64,
    stop_loss_reverse_extra_ticks: usize,
    loss_cluster_limit: usize,
    loss_cluster_cooldown_ms: i64,
    noise_gate_enabled: bool,
    noise_gate_threshold_add: f64,
    noise_gate_edge_add: f64,
    noise_gate_spread_scale: f64,
    vic_enabled: bool,
    vic_target_entries_per_hour: f64,
    vic_deadband_ratio: f64,
    vic_threshold_relax_max: f64,
    vic_edge_relax_max: f64,
    vic_spread_relax_max: f64,
}

const STRATEGY_PROFILE_PROFIT_MAX: &str = "fev1_manual_profit_max_2026_02_27";
const STRATEGY_PROFILE_HI_FREQ: &str = "fev1_manual_hi_freq_2026_02_27";
const STRATEGY_PROFILE_HI_WIN: &str = "fev1_manual_hi_win_2026_02_27";
const STRATEGY_PROFILE_BALANCED: &str = "fev1_manual_balanced_2026_02_28";
const STRATEGY_PROFILE_CAND_GROWTH_MIX: &str = "fev1_cand_growth_mix_2026_02_28";
const STRATEGY_PROFILE_BTC_5M_BALANCE: &str = "btc5m_balance_2026_03_06";
const STRATEGY_PROFILE_SOL_5M_SHARP: &str = "sol5m_sharp_2026_03_07";

#[derive(Clone)]
struct StrategySampleCacheEntry {
    created_at: Instant,
    samples: Arc<Vec<StrategySample>>,
}

include!("strategy/config.rs");
include!("strategy/runtime.rs");
include!("strategy/handlers.rs");
include!("strategy/optimize.rs");
include!("strategy/autotune.rs");
