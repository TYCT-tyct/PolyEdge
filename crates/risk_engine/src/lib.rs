use core_types::{RiskContext, RiskDecision, RiskManager};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskLimits {
    pub max_market_notional: f64,
    pub max_asset_notional: f64,
    pub max_open_orders: usize,
    pub max_drawdown_pct: f64,
    pub max_loss_streak: u32,
    pub cooldown_sec: u64,
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            max_market_notional: 50.0,
            max_asset_notional: 100.0,
            max_open_orders: 20,
            max_drawdown_pct: 0.01,
            max_loss_streak: 5,
            cooldown_sec: 60,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DefaultRiskManager {
    limits: RiskLimits,
}

impl DefaultRiskManager {
    pub fn new(limits: RiskLimits) -> Self {
        Self { limits }
    }
}

impl RiskManager for DefaultRiskManager {
    fn evaluate(&self, ctx: &RiskContext) -> RiskDecision {
        if ctx.drawdown_pct >= self.limits.max_drawdown_pct {
            return RiskDecision {
                allow: false,
                reason: "daily drawdown stop reached".to_string(),
                capped_size: 0.0,
            };
        }

        if ctx.order_count >= self.limits.max_open_orders {
            return RiskDecision {
                allow: false,
                reason: "max open orders reached".to_string(),
                capped_size: 0.0,
            };
        }

        if ctx.market_notional >= self.limits.max_market_notional {
            return RiskDecision {
                allow: false,
                reason: "market notional limit reached".to_string(),
                capped_size: 0.0,
            };
        }

        if ctx.asset_notional >= self.limits.max_asset_notional {
            return RiskDecision {
                allow: false,
                reason: "asset notional limit reached".to_string(),
                capped_size: 0.0,
            };
        }

        let remaining = (self.limits.max_market_notional - ctx.market_notional).max(0.0);
        RiskDecision {
            allow: true,
            reason: "ok".to_string(),
            capped_size: ctx.proposed_size.min(remaining),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_on_drawdown() {
        let manager = DefaultRiskManager::new(RiskLimits::default());
        let ctx = RiskContext {
            market_id: "m".to_string(),
            symbol: "BTCUSDT".to_string(),
            order_count: 0,
            proposed_size: 1.0,
            market_notional: 1.0,
            asset_notional: 1.0,
            drawdown_pct: 0.02,
        };
        let d = manager.evaluate(&ctx);
        assert!(!d.allow);
    }
}
