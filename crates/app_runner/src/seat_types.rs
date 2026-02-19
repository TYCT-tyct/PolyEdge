use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SeatLayer {
    Layer0,
    Layer1,
    Layer2,
    Layer3,
}

impl SeatLayer {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Layer0 => "layer0",
            Self::Layer1 => "layer1",
            Self::Layer2 => "layer2",
            Self::Layer3 => "layer3",
        }
    }

    pub(crate) fn step_limit_pct(self) -> f64 {
        match self {
            Self::Layer0 => 0.03,
            Self::Layer1 => 0.05,
            Self::Layer2 | Self::Layer3 => 0.12,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SeatConfig {
    pub(crate) enabled: bool,
    pub(crate) control_base_url: String,
    pub(crate) optimizer_url: String,
    pub(crate) runtime_tick_sec: u64,
    pub(crate) activation_check_sec: u64,
    pub(crate) layer1_interval_sec: u64,
    pub(crate) layer2_interval_sec: u64,
    pub(crate) layer3_interval_sec: u64,
    pub(crate) layer2_shadow_sec: u64,
    pub(crate) layer3_shadow_sec: u64,
    pub(crate) smoothing_sec: u64,
    pub(crate) monitor_sec: u64,
    pub(crate) rollback_pause_sec: u64,
    pub(crate) global_pause_sec: u64,
    pub(crate) layer0_lock_sec: u64,
    pub(crate) layer1_min_trades: u64,
    pub(crate) layer2_min_trades: u64,
    pub(crate) layer2_min_uptime_sec: u64,
    pub(crate) layer3_min_trades: u64,
    pub(crate) layer3_min_uptime_sec: u64,
    pub(crate) black_swan_lock_sec: u64,
    pub(crate) source_health_floor: f64,
    pub(crate) history_retention_days: u32,
    pub(crate) objective_drawdown_penalty: f64,
}

impl Default for SeatConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            control_base_url: "http://127.0.0.1:8080".to_string(),
            optimizer_url: std::env::var("POLYEDGE_SEAT_OPTIMIZER_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:8091".to_string()),
            runtime_tick_sec: 60,
            activation_check_sec: 3_600,
            layer1_interval_sec: 3_600,
            layer2_interval_sec: 21_600,
            layer3_interval_sec: 86_400,
            layer2_shadow_sec: 2_700,
            layer3_shadow_sec: 6_000,
            smoothing_sec: 1_800,
            monitor_sec: 3_600,
            rollback_pause_sec: 86_400,
            global_pause_sec: 172_800,
            layer0_lock_sec: 172_800,
            layer1_min_trades: 300,
            layer2_min_trades: 800,
            layer2_min_uptime_sec: 72 * 3_600,
            layer3_min_trades: 2_000,
            layer3_min_uptime_sec: 14 * 24 * 3_600,
            black_swan_lock_sec: 172_800,
            source_health_floor: 0.30,
            history_retention_days: 180,
            objective_drawdown_penalty: 5.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub(crate) struct SeatParameterSet {
    pub(crate) position_fraction: Option<f64>,
    pub(crate) early_size_scale: Option<f64>,
    pub(crate) maturity_size_scale: Option<f64>,
    pub(crate) late_size_scale: Option<f64>,
    pub(crate) min_edge_net_bps: Option<f64>,
    pub(crate) convergence_exit_ratio: Option<f64>,
    pub(crate) min_velocity_bps_per_sec: Option<f64>,
    pub(crate) capital_fraction_kelly: Option<f64>,
    pub(crate) t100ms_reversal_bps: Option<f64>,
    pub(crate) t300ms_reversal_bps: Option<f64>,
    pub(crate) max_single_trade_loss_usdc: Option<f64>,
    pub(crate) risk_max_drawdown_pct: Option<f64>,
    pub(crate) risk_max_market_notional: Option<f64>,
    pub(crate) maker_min_edge_bps: Option<f64>,
    pub(crate) basis_k_revert: Option<f64>,
    pub(crate) basis_z_cap: Option<f64>,
}

impl SeatParameterSet {
    pub(crate) fn is_empty(&self) -> bool {
        self == &Self::default()
    }

    pub(crate) fn merge_over(&self, base: &Self) -> Self {
        let mut out = base.clone();
        macro_rules! merge_field {
            ($name:ident) => {
                if self.$name.is_some() {
                    out.$name = self.$name;
                }
            };
        }
        merge_field!(position_fraction);
        merge_field!(early_size_scale);
        merge_field!(maturity_size_scale);
        merge_field!(late_size_scale);
        merge_field!(min_edge_net_bps);
        merge_field!(convergence_exit_ratio);
        merge_field!(min_velocity_bps_per_sec);
        merge_field!(capital_fraction_kelly);
        merge_field!(t100ms_reversal_bps);
        merge_field!(t300ms_reversal_bps);
        merge_field!(max_single_trade_loss_usdc);
        merge_field!(risk_max_drawdown_pct);
        merge_field!(risk_max_market_notional);
        merge_field!(maker_min_edge_bps);
        merge_field!(basis_k_revert);
        merge_field!(basis_z_cap);
        out
    }

    pub(crate) fn clamp_relative(&self, current: &Self, max_step_pct: f64) -> Self {
        fn clamp_opt(candidate: Option<f64>, current: Option<f64>, pct: f64) -> Option<f64> {
            match (candidate, current) {
                (Some(next), Some(cur)) if cur.is_finite() && next.is_finite() => {
                    let step = cur.abs() * pct.max(0.0);
                    if step > 0.0 {
                        Some(next.clamp(cur - step, cur + step))
                    } else {
                        Some(next)
                    }
                }
                (Some(next), _) => Some(next),
                (None, cur) => cur,
            }
        }
        Self {
            position_fraction: clamp_opt(
                self.position_fraction,
                current.position_fraction,
                max_step_pct,
            ),
            early_size_scale: clamp_opt(
                self.early_size_scale,
                current.early_size_scale,
                max_step_pct,
            ),
            maturity_size_scale: clamp_opt(
                self.maturity_size_scale,
                current.maturity_size_scale,
                max_step_pct,
            ),
            late_size_scale: clamp_opt(
                self.late_size_scale,
                current.late_size_scale,
                max_step_pct,
            ),
            min_edge_net_bps: clamp_opt(
                self.min_edge_net_bps,
                current.min_edge_net_bps,
                max_step_pct,
            ),
            convergence_exit_ratio: clamp_opt(
                self.convergence_exit_ratio,
                current.convergence_exit_ratio,
                max_step_pct,
            ),
            min_velocity_bps_per_sec: clamp_opt(
                self.min_velocity_bps_per_sec,
                current.min_velocity_bps_per_sec,
                max_step_pct,
            ),
            capital_fraction_kelly: clamp_opt(
                self.capital_fraction_kelly,
                current.capital_fraction_kelly,
                max_step_pct,
            ),
            t100ms_reversal_bps: clamp_opt(
                self.t100ms_reversal_bps,
                current.t100ms_reversal_bps,
                max_step_pct,
            ),
            t300ms_reversal_bps: clamp_opt(
                self.t300ms_reversal_bps,
                current.t300ms_reversal_bps,
                max_step_pct,
            ),
            max_single_trade_loss_usdc: clamp_opt(
                self.max_single_trade_loss_usdc,
                current.max_single_trade_loss_usdc,
                max_step_pct,
            ),
            risk_max_drawdown_pct: clamp_opt(
                self.risk_max_drawdown_pct,
                current.risk_max_drawdown_pct,
                max_step_pct,
            ),
            risk_max_market_notional: clamp_opt(
                self.risk_max_market_notional,
                current.risk_max_market_notional,
                max_step_pct,
            ),
            maker_min_edge_bps: clamp_opt(
                self.maker_min_edge_bps,
                current.maker_min_edge_bps,
                max_step_pct,
            ),
            basis_k_revert: clamp_opt(self.basis_k_revert, current.basis_k_revert, max_step_pct),
            basis_z_cap: clamp_opt(self.basis_z_cap, current.basis_z_cap, max_step_pct),
        }
    }

    pub(crate) fn exp_blend_towards(&mut self, target: &Self, alpha: f64) {
        fn blend(cur: Option<f64>, target: Option<f64>, alpha: f64) -> Option<f64> {
            match (cur, target) {
                (Some(c), Some(t)) => Some(c + (t - c) * alpha),
                (None, Some(t)) => Some(t),
                (Some(c), None) => Some(c),
                (None, None) => None,
            }
        }
        self.position_fraction = blend(self.position_fraction, target.position_fraction, alpha);
        self.early_size_scale = blend(self.early_size_scale, target.early_size_scale, alpha);
        self.maturity_size_scale =
            blend(self.maturity_size_scale, target.maturity_size_scale, alpha);
        self.late_size_scale = blend(self.late_size_scale, target.late_size_scale, alpha);
        self.min_edge_net_bps = blend(self.min_edge_net_bps, target.min_edge_net_bps, alpha);
        self.convergence_exit_ratio = blend(
            self.convergence_exit_ratio,
            target.convergence_exit_ratio,
            alpha,
        );
        self.min_velocity_bps_per_sec = blend(
            self.min_velocity_bps_per_sec,
            target.min_velocity_bps_per_sec,
            alpha,
        );
        self.capital_fraction_kelly = blend(
            self.capital_fraction_kelly,
            target.capital_fraction_kelly,
            alpha,
        );
        self.t100ms_reversal_bps =
            blend(self.t100ms_reversal_bps, target.t100ms_reversal_bps, alpha);
        self.t300ms_reversal_bps =
            blend(self.t300ms_reversal_bps, target.t300ms_reversal_bps, alpha);
        self.max_single_trade_loss_usdc = blend(
            self.max_single_trade_loss_usdc,
            target.max_single_trade_loss_usdc,
            alpha,
        );
        self.risk_max_drawdown_pct = blend(
            self.risk_max_drawdown_pct,
            target.risk_max_drawdown_pct,
            alpha,
        );
        self.risk_max_market_notional = blend(
            self.risk_max_market_notional,
            target.risk_max_market_notional,
            alpha,
        );
        self.maker_min_edge_bps = blend(self.maker_min_edge_bps, target.maker_min_edge_bps, alpha);
        self.basis_k_revert = blend(self.basis_k_revert, target.basis_k_revert, alpha);
        self.basis_z_cap = blend(self.basis_z_cap, target.basis_z_cap, alpha);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct SeatObjectiveSnapshot {
    pub(crate) ev_usdc_p50: f64,
    pub(crate) max_drawdown_pct: f64,
    pub(crate) roi_notional_10s_bps_p50: f64,
    pub(crate) win_rate: f64,
    pub(crate) source_health_min: f64,
    pub(crate) volatility_proxy: f64,
    pub(crate) objective: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct SeatObjectivePoint {
    pub(crate) ts_ms: i64,
    pub(crate) objective: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SeatSmoothingState {
    pub(crate) layer: SeatLayer,
    pub(crate) old_params: SeatParameterSet,
    pub(crate) target_params: SeatParameterSet,
    pub(crate) current_params: SeatParameterSet,
    pub(crate) baseline: SeatObjectiveSnapshot,
    pub(crate) started_ms: i64,
    pub(crate) end_ms: i64,
    pub(crate) next_step_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SeatMonitorState {
    pub(crate) layer: SeatLayer,
    pub(crate) old_params: SeatParameterSet,
    pub(crate) new_params: SeatParameterSet,
    pub(crate) baseline: SeatObjectiveSnapshot,
    pub(crate) pre_switch_objective_24h: f64,
    pub(crate) started_ms: i64,
    pub(crate) end_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct SeatStyleVector {
    pub(crate) volatility_proxy: f64,
    pub(crate) source_health_min: f64,
    pub(crate) roi_notional_10s_bps_p50: f64,
    pub(crate) win_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SeatStyleMemoryEntry {
    pub(crate) style_id: String,
    pub(crate) vector: SeatStyleVector,
    pub(crate) params: SeatParameterSet,
    pub(crate) objective: f64,
    pub(crate) updated_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SeatRuntimeState {
    pub(crate) version: u32,
    pub(crate) started_ms: i64,
    pub(crate) current_layer: SeatLayer,
    pub(crate) forced_layer: Option<SeatLayer>,
    pub(crate) paused: bool,
    pub(crate) pause_reason: Option<String>,
    pub(crate) global_pause_until_ms: i64,
    pub(crate) layer0_lock_until_ms: i64,
    pub(crate) layer_pause_until_ms: BTreeMap<String, i64>,
    pub(crate) active_shadow_until_ms: i64,
    pub(crate) smoothing: Option<SeatSmoothingState>,
    pub(crate) monitor: Option<SeatMonitorState>,
    pub(crate) manual_override: Option<SeatParameterSet>,
    pub(crate) last_tune_ms_by_layer: BTreeMap<String, i64>,
    pub(crate) last_activation_check_ms: i64,
    pub(crate) last_decision_ts_ms: i64,
    pub(crate) decision_seq: u64,
    pub(crate) degrade_streak: u32,
    pub(crate) rollback_streak: u32,
    pub(crate) live_fill_total: u64,
    pub(crate) live_fill_seen: u64,
    pub(crate) proxy_trade_total: u64,
    pub(crate) proxy_trade_seen: u64,
    pub(crate) trade_count_source: String,
    pub(crate) objective_history: Vec<SeatObjectivePoint>,
    pub(crate) volatility_history: Vec<SeatObjectivePoint>,
    pub(crate) last_objective: Option<SeatObjectiveSnapshot>,
    pub(crate) pre_switch_objective_24h: Option<f64>,
    pub(crate) post_switch_eval_due_ms: i64,
    pub(crate) post_switch_start_ms: i64,
    pub(crate) post_switch_baseline_24h: Option<f64>,
    pub(crate) post_switch_layer: Option<SeatLayer>,
    pub(crate) last_params: SeatParameterSet,
    pub(crate) style_memory: Vec<SeatStyleMemoryEntry>,
    pub(crate) last_archive_ts_ms: i64,
}

impl Default for SeatRuntimeState {
    fn default() -> Self {
        Self {
            version: 1,
            started_ms: chrono::Utc::now().timestamp_millis(),
            current_layer: SeatLayer::Layer0,
            forced_layer: None,
            paused: false,
            pause_reason: None,
            global_pause_until_ms: 0,
            layer0_lock_until_ms: 0,
            layer_pause_until_ms: BTreeMap::new(),
            active_shadow_until_ms: 0,
            smoothing: None,
            monitor: None,
            manual_override: None,
            last_tune_ms_by_layer: BTreeMap::new(),
            last_activation_check_ms: 0,
            last_decision_ts_ms: 0,
            decision_seq: 0,
            degrade_streak: 0,
            rollback_streak: 0,
            live_fill_total: 0,
            live_fill_seen: 0,
            proxy_trade_total: 0,
            proxy_trade_seen: 0,
            trade_count_source: "proxy".to_string(),
            objective_history: Vec::new(),
            volatility_history: Vec::new(),
            last_objective: None,
            pre_switch_objective_24h: None,
            post_switch_eval_due_ms: 0,
            post_switch_start_ms: 0,
            post_switch_baseline_24h: None,
            post_switch_layer: None,
            last_params: SeatParameterSet::default(),
            style_memory: Vec::new(),
            last_archive_ts_ms: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct SeatValidationResult {
    pub(crate) mc_runs: u32,
    pub(crate) mc_pass: bool,
    pub(crate) walk_forward_pass: bool,
    pub(crate) shadow_pass: bool,
    #[serde(default)]
    pub(crate) walk_forward_windows: u32,
    #[serde(default)]
    pub(crate) walk_forward_score: f64,
    #[serde(default)]
    pub(crate) mc_ev_delta_p50: f64,
    #[serde(default)]
    pub(crate) mc_drawdown_p95: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct SeatOptimizerMeta {
    #[serde(default)]
    pub(crate) style_match_score: f64,
    #[serde(default)]
    pub(crate) style_match_count: u32,
    #[serde(default)]
    pub(crate) top_k_size: u32,
    #[serde(default)]
    pub(crate) objective_uplift_estimate: f64,
    #[serde(default)]
    pub(crate) rl_signal: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct SeatOptimizerProposal {
    pub(crate) layer: String,
    pub(crate) candidate: SeatParameterSet,
    pub(crate) validation: SeatValidationResult,
    #[serde(default)]
    pub(crate) meta: SeatOptimizerMeta,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct SeatLockState {
    pub(crate) paused: bool,
    pub(crate) global_pause_until_ms: i64,
    pub(crate) layer0_lock_until_ms: i64,
    pub(crate) active_shadow_until_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SeatDecisionRecord {
    pub(crate) ts_ms: i64,
    pub(crate) layer: SeatLayer,
    #[serde(default)]
    pub(crate) previous: SeatParameterSet,
    pub(crate) candidate: SeatParameterSet,
    pub(crate) baseline: SeatObjectiveSnapshot,
    pub(crate) decision: String,
    pub(crate) rollback: bool,
    pub(crate) lock_state: SeatLockState,
    pub(crate) trade_count_source: String,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SeatStatusReport {
    pub(crate) ts_ms: i64,
    pub(crate) enabled: bool,
    pub(crate) paused: bool,
    pub(crate) pause_reason: Option<String>,
    pub(crate) current_layer: SeatLayer,
    pub(crate) forced_layer: Option<SeatLayer>,
    pub(crate) global_pause_until_ms: i64,
    pub(crate) layer0_lock_until_ms: i64,
    pub(crate) active_shadow_until_ms: i64,
    pub(crate) trade_count: u64,
    pub(crate) trade_count_source: String,
    pub(crate) started_ms: i64,
    pub(crate) last_decision_ts_ms: i64,
    pub(crate) degrade_streak: u32,
    pub(crate) rollback_streak: u32,
    pub(crate) smoothing_active: bool,
    pub(crate) monitor_active: bool,
    pub(crate) manual_override_active: bool,
    pub(crate) last_objective: Option<SeatObjectiveSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SeatForceLayerReq {
    pub(crate) layer: Option<SeatLayer>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct SeatManualOverrideReq {
    pub(crate) params: SeatParameterSet,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_clamp_respects_pct() {
        let current = SeatParameterSet {
            position_fraction: Some(0.15),
            min_edge_net_bps: Some(200.0),
            ..SeatParameterSet::default()
        };
        let candidate = SeatParameterSet {
            position_fraction: Some(0.20),
            min_edge_net_bps: Some(260.0),
            ..SeatParameterSet::default()
        };
        let clamped = candidate.clamp_relative(&current, 0.05);
        assert_eq!(clamped.position_fraction, Some(0.1575));
        assert_eq!(clamped.min_edge_net_bps, Some(210.0));
    }
}
