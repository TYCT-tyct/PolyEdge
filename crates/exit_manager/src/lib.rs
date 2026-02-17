use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExitManagerConfig {
    pub t3_take_ratio: f64,
    pub t15_min_unrealized_usdc: f64,
    pub t60_true_prob_floor: f64,
    pub t300_force_exit_ms: u64,
    pub t300_hold_prob_threshold: f64,
    pub t300_hold_time_to_expiry_ms: u64,
    pub max_single_trade_loss_usdc: f64,
}

impl Default for ExitManagerConfig {
    fn default() -> Self {
        Self {
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExitReason {
    StopLoss,
    TakeProfit3s,
    TakeProfit15s,
    ProbGuard60s,
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

    pub fn evaluate_market(&mut self, market_id: &str, input: MarketEvalInput) -> Option<ExitAction> {
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

    #[test]
    fn stop_loss_triggers_immediately() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        let action = manager.evaluate_market(
            "m1",
            MarketEvalInput {
                now_ms: 1_100,
                unrealized_pnl_usdc: -1.1,
                true_prob: 0.9,
                time_to_expiry_ms: 600_000,
            },
        );
        assert!(matches!(action.map(|a| a.reason), Some(ExitReason::StopLoss)));
    }

    #[test]
    fn t3_take_profit_triggers() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        let action = manager.evaluate_market(
            "m1",
            MarketEvalInput {
                now_ms: 4_500,
                unrealized_pnl_usdc: 0.7,
                true_prob: 0.8,
                time_to_expiry_ms: 500_000,
            },
        );
        assert!(matches!(
            action.map(|a| a.reason),
            Some(ExitReason::TakeProfit3s)
        ));
    }

    #[test]
    fn t15_positive_exit_triggers() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        let action = manager.evaluate_market(
            "m1",
            MarketEvalInput {
                now_ms: 20_500,
                unrealized_pnl_usdc: 0.01,
                true_prob: 0.9,
                time_to_expiry_ms: 450_000,
            },
        );
        assert!(matches!(
            action.map(|a| a.reason),
            Some(ExitReason::TakeProfit15s)
        ));
    }

    #[test]
    fn t60_prob_guard_triggers_on_low_prob() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        let action = manager.evaluate_market(
            "m1",
            MarketEvalInput {
                now_ms: 62_000,
                unrealized_pnl_usdc: -0.2,
                true_prob: 0.65,
                time_to_expiry_ms: 360_000,
            },
        );
        assert!(matches!(
            action.map(|a| a.reason),
            Some(ExitReason::ProbGuard60s)
        ));
    }

    #[test]
    fn t300_force_close_triggers() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        let action = manager.evaluate_market(
            "m1",
            MarketEvalInput {
                now_ms: 305_000,
                unrealized_pnl_usdc: -0.01,
                true_prob: 0.93,
                time_to_expiry_ms: 1_000_000,
            },
        );
        assert!(matches!(
            action.map(|a| a.reason),
            Some(ExitReason::ForceClose300s)
        ));
    }

    #[test]
    fn t300_allows_hold_near_expiry_with_high_prob() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        let action = manager.evaluate_market(
            "m1",
            MarketEvalInput {
                now_ms: 305_000,
                unrealized_pnl_usdc: -0.01,
                true_prob: 0.97,
                time_to_expiry_ms: 240_000,
            },
        );
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

        let action = manager.evaluate_market(
            "m1",
            MarketEvalInput {
                now_ms: 15_000,
                unrealized_pnl_usdc: 1.0,
                true_prob: 0.9,
                time_to_expiry_ms: 600_000,
            },
        );
        let Some(action) = action else {
            panic!("expected action");
        };
        assert_eq!(action.position_id, "new");
    }
}
