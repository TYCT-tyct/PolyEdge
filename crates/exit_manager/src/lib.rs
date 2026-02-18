use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// ============================================================
// 配置 — 所有时间阈值和触发条件集中在此
// 修改行为只需改配置，不需要改逻辑
// ============================================================
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExitManagerConfig {
    /// T+100ms 极早反转阈值 (bps)。入场后 100-300ms 内 PnL 低于此值立即离场。
    /// 设计哲学: 我们比市场快，如果入场后立刻亏损，说明信号错误，立即认错比等待更优。
    pub t100ms_reversal_bps: f64,
    /// T+300ms 反转阈值 (bps)。入场后 300ms-3s 内 PnL 低于此值离场。
    pub t300ms_reversal_bps: f64,
    /// T+3s 止盈比例。PnL > entry_edge * ratio 时止盈。
    pub t3_take_ratio: f64,
    /// T+15s 最低盈利要求 (USDC)。任何正收益即离场。
    pub t15_min_unrealized_usdc: f64,
    /// T+60s 概率保护下限。true_prob 低于此值时止损。
    pub t60_true_prob_floor: f64,
    /// 强制平仓时间 (ms)。超过此时间无条件平仓。
    pub t300_force_exit_ms: u64,
    /// 允许持仓到结算的概率门槛。
    pub t300_hold_prob_threshold: f64,
    /// 允许持仓到结算的剩余时间门槛 (ms)。
    pub t300_hold_time_to_expiry_ms: u64,
    /// 单笔最大亏损 (USDC)。超过立即止损。
    pub max_single_trade_loss_usdc: f64,
}

impl Default for ExitManagerConfig {
    fn default() -> Self {
        Self {
            // 极早反转: 100ms 内亏 3 bps → 信号错误，立即离场
            t100ms_reversal_bps: -3.0,
            // 早期反转: 300ms 内亏 1 bps → 方向可能错误，离场
            t300ms_reversal_bps: -1.0,
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

// ============================================================
// 退出原因 — 每个原因对应一个明确的退出策略
// 顺序即优先级: StopLoss > Reversal100ms > Reversal300ms > ...
// ============================================================
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExitReason {
    /// 单笔亏损超过硬性上限，无条件止损
    StopLoss,
    /// 入场后 100-300ms 内快速反转，信号可能错误
    Reversal100ms,
    /// 入场后 300ms-3s 内持续反转
    Reversal300ms,
    /// T+3s 已达到目标盈利比例，锁定利润
    TakeProfit3s,
    /// T+15s 任何正收益即离场，避免长期持仓风险
    TakeProfit15s,
    /// T+60s 概率已下降，市场方向不利，止损
    ProbGuard60s,
    /// 超过最大持仓时间，强制平仓
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

    // --------------------------------------------------------
    // 退出决策树 — 按优先级从高到低检查
    // 每个检查点对应一个明确的市场假设被打破的场景
    // --------------------------------------------------------
    fn evaluate_position(
        &self,
        position: &PositionLifecycle,
        input: &MarketEvalInput,
    ) -> Option<ExitReason> {
        let elapsed_ms = input.now_ms.saturating_sub(position.opened_at_ms).max(0) as u64;
        let true_prob = input.true_prob.clamp(0.0, 1.0);

        // 第一道: 硬性止损 — 任何时间点，亏损超限立即离场
        if input.unrealized_pnl_usdc <= -self.cfg.max_single_trade_loss_usdc {
            return Some(ExitReason::StopLoss);
        }

        // 第二道: T+100ms 极早反转检测
        // 假设: 我们比市场快 150ms。如果入场后 100ms 内就亏损，说明信号是假的。
        // 立即认错比等待更优，因为市场会继续反向移动。
        if position.entry_notional_usdc > 0.0 && (100..300).contains(&elapsed_ms) {
            let pnl_bps = (input.unrealized_pnl_usdc / position.entry_notional_usdc) * 10_000.0;
            if pnl_bps <= self.cfg.t100ms_reversal_bps {
                return Some(ExitReason::Reversal100ms);
            }
        }

        // 第三道: T+300ms 反转检测
        // 假设: 如果 300ms 后仍在亏损，套利窗口已关闭，市场已反应
        if position.entry_notional_usdc > 0.0 && (300..3_000).contains(&elapsed_ms) {
            let pnl_bps = (input.unrealized_pnl_usdc / position.entry_notional_usdc) * 10_000.0;
            if pnl_bps <= self.cfg.t300ms_reversal_bps {
                return Some(ExitReason::Reversal300ms);
            }
        }

        // 第四道: T+3s 止盈
        // 假设: 3s 后价格已充分反应，锁定 60% 的预期 edge
        if elapsed_ms >= 3_000 {
            let t3_target = position.entry_edge_usdc.max(0.0) * self.cfg.t3_take_ratio;
            if input.unrealized_pnl_usdc > t3_target {
                return Some(ExitReason::TakeProfit3s);
            }
        }

        // 第五道: T+15s 任何正收益离场
        // 假设: 15s 后继续持有的风险大于收益
        if elapsed_ms >= 15_000 && input.unrealized_pnl_usdc > self.cfg.t15_min_unrealized_usdc {
            return Some(ExitReason::TakeProfit15s);
        }

        // 第六道: T+60s 概率保护
        // 假设: 60s 后如果概率已下降，方向判断错误，止损
        if elapsed_ms >= 60_000 && true_prob <= self.cfg.t60_true_prob_floor {
            return Some(ExitReason::ProbGuard60s);
        }

        // 第七道: 强制平仓
        // 例外: 概率极高且接近结算时，允许持有到结算（Mode B 收敛策略）
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
    fn reversal_100ms_triggers_on_fast_reversal() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        // T+150ms, pnl = -0.02 USDC on 50 notional = -4 bps < -3 bps threshold
        let action = manager.evaluate_market(
            "m1",
            MarketEvalInput {
                now_ms: 1_150,
                unrealized_pnl_usdc: -0.02,
                true_prob: 0.9,
                time_to_expiry_ms: 600_000,
            },
        );
        assert!(matches!(
            action.map(|a| a.reason),
            Some(ExitReason::Reversal100ms)
        ));
    }

    #[test]
    fn reversal_100ms_does_not_trigger_too_early() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        // T+50ms — 还没到 100ms 窗口，不应触发
        let action = manager.evaluate_market(
            "m1",
            MarketEvalInput {
                now_ms: 1_050,
                unrealized_pnl_usdc: -0.02,
                true_prob: 0.9,
                time_to_expiry_ms: 600_000,
            },
        );
        // 50ms 内不触发 Reversal100ms（也不触发 StopLoss，因为 -0.02 < 1.0）
        assert!(action.is_none());
    }

    #[test]
    fn reversal_100ms_does_not_trigger_on_small_loss() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        // T+150ms, pnl = -0.005 USDC = -1 bps > -3 bps threshold → 不触发
        let action = manager.evaluate_market(
            "m1",
            MarketEvalInput {
                now_ms: 1_150,
                unrealized_pnl_usdc: -0.005,
                true_prob: 0.9,
                time_to_expiry_ms: 600_000,
            },
        );
        assert!(action.is_none());
    }

    #[test]
    fn reversal_300ms_triggers_before_t3() {
        let mut manager = ExitManager::new(ExitManagerConfig::default());
        manager.register(sample_position(1_000));
        // T+400ms, pnl = -0.01 USDC = -2 bps < -1 bps threshold
        let action = manager.evaluate_market(
            "m1",
            MarketEvalInput {
                now_ms: 1_400,
                unrealized_pnl_usdc: -0.01,
                true_prob: 0.9,
                time_to_expiry_ms: 500_000,
            },
        );
        assert!(matches!(
            action.map(|a| a.reason),
            Some(ExitReason::Reversal300ms)
        ));
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
