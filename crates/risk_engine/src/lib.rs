use std::sync::{Arc, Mutex, RwLock};

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
            max_drawdown_pct: 0.015,
            max_loss_streak: 5,
            cooldown_sec: 60,
        }
    }
}

#[derive(Debug, Default)]
struct RiskState {
    cooldown_until_ms: i64,
}

pub struct DefaultRiskManager {
    limits: Arc<RwLock<RiskLimits>>,
    state: Mutex<RiskState>,
}

impl DefaultRiskManager {
    pub fn new(limits: Arc<RwLock<RiskLimits>>) -> Self {
        Self {
            limits,
            state: Mutex::new(RiskState::default()),
        }
    }
}

impl RiskManager for DefaultRiskManager {
    fn evaluate(&self, ctx: &RiskContext) -> RiskDecision {
        let limits = self
            .limits
            .read()
            .map(|g| g.clone())
            .unwrap_or_else(|_| RiskLimits::default());

        let now_ms = ctx.now_ms;

        // Cooldown gate (e.g. after a loss streak trigger).
        if let Ok(st) = self.state.lock() {
            if now_ms > 0 && now_ms < st.cooldown_until_ms {
                return RiskDecision {
                    allow: false,
                    reason: "cooldown".to_string(),
                    capped_size: 0.0,
                };
            }
        }

        // Hard drawdown stop.
        if ctx.drawdown_pct >= limits.max_drawdown_pct {
            return RiskDecision {
                allow: false,
                reason: "drawdown_stop".to_string(),
                capped_size: 0.0,
            };
        }

        // Loss streak stop: once tripped, enter cooldown window.
        if ctx.loss_streak >= limits.max_loss_streak && limits.max_loss_streak > 0 {
            if let Ok(mut st) = self.state.lock() {
                if now_ms > 0 {
                    st.cooldown_until_ms =
                        st.cooldown_until_ms.max(now_ms + (limits.cooldown_sec as i64) * 1_000);
                }
            }
            return RiskDecision {
                allow: false,
                reason: "loss_streak".to_string(),
                capped_size: 0.0,
            };
        }

        // Open order count cap.
        if ctx.order_count >= limits.max_open_orders {
            return RiskDecision {
                allow: false,
                reason: "open_orders_limit".to_string(),
                capped_size: 0.0,
            };
        }

        // Notional caps. RiskContext carries precomputed notional in USDC.
        let proposed_notional = ctx.proposed_notional_usdc.max(0.0);

        if ctx.market_notional + proposed_notional > limits.max_market_notional {
            let remaining = (limits.max_market_notional - ctx.market_notional).max(0.0);
            let capped_size = if proposed_notional <= 0.0 {
                0.0
            } else {
                // Scale down size proportionally; caller uses it as a cap.
                (ctx.proposed_size * (remaining / proposed_notional)).max(0.0)
            };
            return RiskDecision {
                allow: capped_size > 0.0,
                reason: "market_notional_limit".to_string(),
                capped_size,
            };
        }

        if ctx.asset_notional + proposed_notional > limits.max_asset_notional {
            let remaining = (limits.max_asset_notional - ctx.asset_notional).max(0.0);
            let capped_size = if proposed_notional <= 0.0 {
                0.0
            } else {
                (ctx.proposed_size * (remaining / proposed_notional)).max(0.0)
            };
            return RiskDecision {
                allow: capped_size > 0.0,
                reason: "asset_notional_limit".to_string(),
                capped_size,
            };
        }

        RiskDecision {
            allow: true,
            reason: "ok".to_string(),
            capped_size: ctx.proposed_size.max(0.0),
        }
    }
}

