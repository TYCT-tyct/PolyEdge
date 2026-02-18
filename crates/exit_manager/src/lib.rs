use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExitManagerConfig {
    /// Early reversal threshold (bps) for the 100ms..300ms window.
    pub t100ms_reversal_bps: f64,
    /// Reversal threshold (bps) for the 300ms..3s window.
    pub t300ms_reversal_bps: f64,
    /// Convergence ratio threshold. Exit once PM has closed enough of the fair-value gap.
    pub convergence_exit_ratio: f64,
    /// T+3s take-profit ratio against entry edge.
    pub t3_take_ratio: f64,
    /// Minimum unrealized PnL at T+15s.
    pub t15_min_unrealized_usdc: f64,
    /// Probability guard floor at T+60s.
    pub t60_true_prob_floor: f64,
    /// Hard max holding time in ms.
    pub t300_force_exit_ms: u64,
    /// Allow-hold probability threshold near expiry.
    pub t300_hold_prob_threshold: f64,
    /// Allow-hold remaining-time threshold in ms.
    pub t300_hold_time_to_expiry_ms: u64,
    /// Max allowed loss per position in USDC.
    pub max_single_trade_loss_usdc: f64,
}

impl Default for ExitManagerConfig {
    fn default() -> Self {
        Self {
            t100ms_reversal_bps: -3.0,
            t300ms_reversal_bps: -2.0,
            convergence_exit_ratio: 0.85,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PositionLifecycle {
    pub position_id: String,
    pub market_id: String,
    pub symbol: String,
    pub opened_at_ms: i64,
    pub entry_edge_usdc: f64,
    pub entry_notional_usdc: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketEvalInput {
    pub now_ms: i64,
    pub unrealized_pnl_usdc: f64,
    pub true_prob: f64,
    pub time_to_expiry_ms: i64,
    /// Current PM YES mid used for convergence checks.
    pub pm_mid_yes: f64,
    /// PM YES mid at entry.
    pub entry_pm_mid_yes: f64,
    /// Fair YES value at entry.
    pub entry_fair_yes: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExitReason {
    /// Hard stop on max per-trade loss.
    StopLoss,
    /// Sharp reversal in 100ms..300ms.
    Reversal100ms,
    /// Sustained reversal in 300ms..3s.
    Reversal300ms,
    /// PM price has converged enough to fair value.
    ConvergenceExit,
    /// Profit target reached at/after T+3s.
    TakeProfit3s,
    /// Any positive pnl at/after T+15s.
    TakeProfit15s,
    /// Probability guard at/after T+60s.
    ProbGuard60s,
    /// Hard close after max hold time.
    ForceClose300s,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExitAction {
    pub position_id: String,
    pub market_id: String,
    pub symbol: String,
    pub reason: ExitReason,
}

#[derive(Debug)]
pub struct ExitManager {
    cfg: ExitManagerConfig,
    open: HashMap<String, PositionLifecycle>,
}

impl ExitManager {
    pub fn new(cfg: ExitManagerConfig) -> Self {
        Self {
            cfg,
            open: HashMap::new(),
        }
    }

    pub fn cfg(&self) -> &ExitManagerConfig {
        &self.cfg
    }

    pub fn set_cfg(&mut self, cfg: ExitManagerConfig) {
        self.cfg = cfg;
    }

    pub fn register(&mut self, position: PositionLifecycle) {
        self.open.insert(position.position_id.clone(), position);
    }

    pub fn close(&mut self, position_id: &str) -> Option<PositionLifecycle> {
        self.open.remove(position_id)
    }

    pub fn open_count(&self) -> usize {
        self.open.len()
    }

    pub fn evaluate_market(
        &mut self,
        market_id: &str,
        input: MarketEvalInput,
    ) -> Option<ExitAction> {
        let position = self
            .open
            .values()
            .filter(|p| p.market_id == market_id)
            .max_by_key(|p| p.opened_at_ms)
            .cloned()?;
        let reason = self.evaluate_position(&position, &input)?;
        self.open.remove(&position.position_id);
        Some(ExitAction {
            position_id: position.position_id,
            market_id: position.market_id,
            symbol: position.symbol,
            reason,
        })
    }

    fn evaluate_position(
        &self,
        position: &PositionLifecycle,
        input: &MarketEvalInput,
    ) -> Option<ExitReason> {
        let elapsed_ms = input.now_ms.saturating_sub(position.opened_at_ms).max(0) as u64;
        let true_prob = input.true_prob.clamp(0.0, 1.0);

        if input.unrealized_pnl_usdc <= -self.cfg.max_single_trade_loss_usdc {
            return Some(ExitReason::StopLoss);
        }

        if position.entry_notional_usdc > 0.0 && (100..300).contains(&elapsed_ms) {
            let pnl_bps = (input.unrealized_pnl_usdc / position.entry_notional_usdc) * 10_000.0;
            if pnl_bps <= self.cfg.t100ms_reversal_bps {
                return Some(ExitReason::Reversal100ms);
            }
        }

        if position.entry_notional_usdc > 0.0 && (300..3_000).contains(&elapsed_ms) {
            let pnl_bps = (input.unrealized_pnl_usdc / position.entry_notional_usdc) * 10_000.0;
            if pnl_bps <= self.cfg.t300ms_reversal_bps {
                return Some(ExitReason::Reversal300ms);
            }
        }

        if self.cfg.convergence_exit_ratio > 0.0
            && input.pm_mid_yes > 0.0
            && input.entry_pm_mid_yes > 0.0
            && input.entry_fair_yes > 0.0
        {
            let gap_total = (input.entry_fair_yes - input.entry_pm_mid_yes).abs();
            if gap_total > 1e-6 {
                let gap_remaining = (input.entry_fair_yes - input.pm_mid_yes).abs();
                let convergence_ratio = 1.0 - (gap_remaining / gap_total);
                if convergence_ratio >= self.cfg.convergence_exit_ratio {
                    return Some(ExitReason::ConvergenceExit);
                }
            }
        }

        if elapsed_ms >= 3_000 {
            let t3_target = position.entry_edge_usdc.max(0.0) * self.cfg.t3_take_ratio;
            if input.unrealized_pnl_usdc > t3_target {
                return Some(ExitReason::TakeProfit3s);
            }
        }

        if elapsed_ms >= 15_000 && input.unrealized_pnl_usdc > self.cfg.t15_min_unrealized_usdc {
            return Some(ExitReason::TakeProfit15s);
        }

        if elapsed_ms >= 60_000 && true_prob <= self.cfg.t60_true_prob_floor {
            return Some(ExitReason::ProbGuard60s);
        }

        if elapsed_ms >= self.cfg.t300_force_exit_ms {
            let allow_hold = true_prob > self.cfg.t300_hold_prob_threshold
                && input.time_to_expiry_ms >= 0
                && (input.time_to_expiry_ms as u64) < self.cfg.t300_hold_time_to_expiry_ms;
            if !allow_hold {
                return Some(ExitReason::ForceClose300s);
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_position(opened_at_ms: i64) -> PositionLifecycle {
        PositionLifecycle {
            position_id: "p1".to_string(),
            market_id: "m1".to_string(),
            symbol: "BTCUSDT".to_string(),
            opened_at_ms,
            entry_edge_usdc: 1.0,
            entry_notional_usdc: 50.0,
        }
    }

    fn eval(
        now_ms: i64,
        unrealized_pnl_usdc: f64,
        true_prob: f64,
        time_to_expiry_ms: i64,
    ) -> MarketEvalInput {
        MarketEvalInput {
            now_ms,
            unrealized_pnl_usdc,
            true_prob,
            time_to_expiry_ms,
            pm_mid_yes: 0.0,
            entry_pm_mid_yes: 0.0,
            entry_fair_yes: 0.0,
        }
    }

    #[test]
    fn stop_loss_triggers_immediately() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        let action = manager.evaluate_market("m1", eval(1_100, -1.1, 0.9, 600_000));
        assert!(matches!(
            action.map(|a| a.reason),
            Some(ExitReason::StopLoss)
        ));
    }

    #[test]
    fn reversal_100ms_triggers_on_fast_reversal() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        let action = manager.evaluate_market("m1", eval(1_150, -0.02, 0.9, 600_000));
        assert!(matches!(
            action.map(|a| a.reason),
            Some(ExitReason::Reversal100ms)
        ));
    }

    #[test]
    fn reversal_100ms_does_not_trigger_too_early() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        let action = manager.evaluate_market("m1", eval(1_050, -0.02, 0.9, 600_000));
        assert!(action.is_none());
    }

    #[test]
    fn reversal_100ms_does_not_trigger_on_small_loss() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        let action = manager.evaluate_market("m1", eval(1_150, -0.005, 0.9, 600_000));
        assert!(action.is_none());
    }

    #[test]
    fn reversal_300ms_triggers_before_t3() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        let action = manager.evaluate_market("m1", eval(1_400, -0.01, 0.9, 500_000));
        assert!(matches!(
            action.map(|a| a.reason),
            Some(ExitReason::Reversal300ms)
        ));
    }

    #[test]
    fn t3_take_profit_triggers() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        let action = manager.evaluate_market("m1", eval(4_500, 0.7, 0.8, 500_000));
        assert!(matches!(
            action.map(|a| a.reason),
            Some(ExitReason::TakeProfit3s)
        ));
    }

    #[test]
    fn t15_positive_exit_triggers() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        let action = manager.evaluate_market("m1", eval(20_500, 0.01, 0.9, 450_000));
        assert!(matches!(
            action.map(|a| a.reason),
            Some(ExitReason::TakeProfit15s)
        ));
    }

    #[test]
    fn t60_prob_guard_triggers_on_low_prob() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        let action = manager.evaluate_market("m1", eval(62_000, -0.2, 0.65, 360_000));
        assert!(matches!(
            action.map(|a| a.reason),
            Some(ExitReason::ProbGuard60s)
        ));
    }

    #[test]
    fn t300_force_close_triggers() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        let action = manager.evaluate_market("m1", eval(305_000, -0.01, 0.93, 1_000_000));
        assert!(matches!(
            action.map(|a| a.reason),
            Some(ExitReason::ForceClose300s)
        ));
    }

    #[test]
    fn t300_allows_hold_near_expiry_with_high_prob() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        let action = manager.evaluate_market("m1", eval(305_000, -0.01, 0.97, 240_000));
        assert!(action.is_none());
        assert_eq!(manager.open_count(), 1);
    }

    #[test]
    fn evaluate_market_uses_latest_position() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(PositionLifecycle {
            position_id: "old".to_string(),
            opened_at_ms: 1_000,
            ..sample_position(1_000)
        });
        manager.register(PositionLifecycle {
            position_id: "new".to_string(),
            opened_at_ms: 10_000,
            ..sample_position(10_000)
        });
        let action = manager.evaluate_market("m1", eval(15_000, 1.0, 0.9, 600_000));
        let Some(action) = action else {
            panic!("expected action");
        };
        assert_eq!(action.position_id, "new");
    }

    #[test]
    fn convergence_exit_triggers_at_85_percent() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        let action = manager.evaluate_market(
            "m1",
            MarketEvalInput {
                now_ms: 1_500,
                unrealized_pnl_usdc: 0.5,
                true_prob: 0.9,
                time_to_expiry_ms: 600_000,
                pm_mid_yes: 0.785,
                entry_pm_mid_yes: 0.70,
                entry_fair_yes: 0.80,
            },
        );
        assert!(matches!(
            action.map(|a| a.reason),
            Some(ExitReason::ConvergenceExit)
        ));
    }

    #[test]
    fn convergence_exit_does_not_trigger_below_threshold() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        let action = manager.evaluate_market(
            "m1",
            MarketEvalInput {
                now_ms: 1_500,
                unrealized_pnl_usdc: 0.3,
                true_prob: 0.9,
                time_to_expiry_ms: 600_000,
                pm_mid_yes: 0.77,
                entry_pm_mid_yes: 0.70,
                entry_fair_yes: 0.80,
            },
        );
        assert!(action.is_none());
    }

    #[test]
    fn convergence_exit_skipped_when_pm_mid_zero() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        let action = manager.evaluate_market(
            "m1",
            MarketEvalInput {
                now_ms: 1_500,
                unrealized_pnl_usdc: 0.5,
                true_prob: 0.9,
                time_to_expiry_ms: 600_000,
                pm_mid_yes: 0.0,
                entry_pm_mid_yes: 0.70,
                entry_fair_yes: 0.80,
            },
        );
        assert!(action.is_none());
    }
}
