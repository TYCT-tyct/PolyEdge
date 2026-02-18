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
    pub progressive_enabled: bool,
    pub drawdown_tier1_ratio: f64,
    pub drawdown_tier2_ratio: f64,
    pub tier1_size_scale: f64,
    pub tier2_size_scale: f64,
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
            progressive_enabled: true,
            drawdown_tier1_ratio: 0.50,
            drawdown_tier2_ratio: 0.80,
            tier1_size_scale: 0.70,
            tier2_size_scale: 0.40,
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
        let limits = match self.limits.read() {
            Ok(guard) => guard.clone(),
            Err(_) => {
                // Lock poisoned - fail closed for safety
                return RiskDecision {
                    allow: false,
                    reason: "risk_lock_poisoned".to_string(),
                    capped_size: 0.0,
                };
            }
        };

        let now_ms = ctx.now_ms;

        // Cooldown gate (e.g. after a loss streak trigger).
        let cooldown_active = self
            .state
            .lock()
            .map(|st| now_ms > 0 && now_ms < st.cooldown_until_ms)
            .unwrap_or(false); // Fail closed if lock fails

        if cooldown_active {
            return RiskDecision {
                allow: false,
                reason: "cooldown".to_string(),
                capped_size: 0.0,
            };
        }

        let drawdown_abs = ctx.drawdown_pct.abs();
        // Hard drawdown stop - use abs to handle negative drawdown values
        if drawdown_abs >= limits.max_drawdown_pct {
            return RiskDecision {
                allow: false,
                reason: "drawdown_stop".to_string(),
                capped_size: 0.0,
            };
        }
        let mut progressive_scale: f64 = 1.0;
        let mut progressive_reason: Option<&'static str> = None;
        if limits.progressive_enabled && limits.max_drawdown_pct > 0.0 {
            let tier1 = limits.max_drawdown_pct * limits.drawdown_tier1_ratio.clamp(0.05, 0.99);
            let tier2 = limits.max_drawdown_pct
                * limits
                    .drawdown_tier2_ratio
                    .clamp(limits.drawdown_tier1_ratio.clamp(0.05, 0.99), 0.999);
            if drawdown_abs >= tier2 {
                progressive_scale = progressive_scale.min(limits.tier2_size_scale.clamp(0.01, 1.0));
                progressive_reason = Some("drawdown_tier2");
            } else if drawdown_abs >= tier1 {
                progressive_scale = progressive_scale.min(limits.tier1_size_scale.clamp(0.01, 1.0));
                progressive_reason = Some("drawdown_tier1");
            }
        }

        // Loss streak stop: once tripped, enter cooldown window.
        if ctx.loss_streak >= limits.max_loss_streak && limits.max_loss_streak > 0 {
            if let Ok(mut st) = self.state.lock() {
                if now_ms > 0 {
                    // Start cooldown window from "now", not from previous value.
                    st.cooldown_until_ms =
                        now_ms.saturating_add((limits.cooldown_sec as i64).saturating_mul(1_000));
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

        // Normalize existing notional to handle potential negative values
        let market_notional = ctx.market_notional.max(0.0);
        let asset_notional = ctx.asset_notional.max(0.0);

        if market_notional + proposed_notional > limits.max_market_notional {
            let remaining = (limits.max_market_notional - market_notional).max(0.0);
            let capped_size = if proposed_notional <= 0.0 {
                0.0
            } else {
                // Scale down size proportionally; caller uses it as a cap.
                // Use epsilon to prevent division by zero
                (ctx.proposed_size * (remaining / proposed_notional)).max(0.0)
            };
            return RiskDecision {
                allow: capped_size > 0.0,
                reason: "market_notional_limit".to_string(),
                capped_size: (capped_size * progressive_scale).max(0.0),
            };
        }

        if asset_notional + proposed_notional > limits.max_asset_notional {
            let remaining = (limits.max_asset_notional - asset_notional).max(0.0);
            let capped_size = if proposed_notional <= 0.0 {
                0.0
            } else {
                (ctx.proposed_size * (remaining / proposed_notional)).max(0.0)
            };
            return RiskDecision {
                allow: capped_size > 0.0,
                reason: "asset_notional_limit".to_string(),
                capped_size: (capped_size * progressive_scale).max(0.0),
            };
        }

        let capped = (ctx.proposed_size.max(0.0) * progressive_scale).max(0.0);
        RiskDecision {
            allow: capped > 0.0,
            reason: progressive_reason.unwrap_or("ok").to_string(),
            capped_size: capped,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(now_ms: i64) -> RiskContext {
        RiskContext {
            market_id: "m1".to_string(),
            symbol: "BTCUSDT".to_string(),
            order_count: 0,
            proposed_size: 1.0,
            proposed_notional_usdc: 1.0,
            market_notional: 0.0,
            asset_notional: 0.0,
            drawdown_pct: 0.0,
            loss_streak: 0,
            now_ms,
        }
    }

    #[test]
    fn loss_streak_sets_cooldown_from_current_time() {
        let limits = Arc::new(RwLock::new(RiskLimits {
            max_loss_streak: 1,
            cooldown_sec: 60,
            ..RiskLimits::default()
        }));
        let rm = DefaultRiskManager::new(limits);

        let mut tripped = ctx(1_000_000);
        tripped.loss_streak = 1;
        let d = rm.evaluate(&tripped);
        assert!(!d.allow);
        assert_eq!(d.reason, "loss_streak");

        // Should be blocked by cooldown at +59s.
        let d = rm.evaluate(&ctx(1_059_000));
        assert!(!d.allow);
        assert_eq!(d.reason, "cooldown");

        // Cooldown should end at +60s.
        let d = rm.evaluate(&ctx(1_060_000));
        assert!(d.allow);
        assert_eq!(d.reason, "ok");
    }

    #[test]
    fn notional_cap_scales_size_proportionally() {
        let limits = Arc::new(RwLock::new(RiskLimits {
            max_market_notional: 10.0,
            ..RiskLimits::default()
        }));
        let rm = DefaultRiskManager::new(limits);
        let d = rm.evaluate(&RiskContext {
            market_notional: 8.0,
            proposed_notional_usdc: 4.0,
            proposed_size: 2.0,
            ..ctx(1_000_000)
        });
        assert!(d.allow);
        // Remaining notional is 2.0 out of requested 4.0 => 50% size cap.
        assert!((d.capped_size - 1.0).abs() < 1e-9);
    }

    #[test]
    fn progressive_drawdown_scales_size_before_hard_stop() {
        let limits = Arc::new(RwLock::new(RiskLimits {
            max_drawdown_pct: 0.20,
            drawdown_tier1_ratio: 0.50,
            drawdown_tier2_ratio: 0.80,
            tier1_size_scale: 0.70,
            tier2_size_scale: 0.40,
            progressive_enabled: true,
            ..RiskLimits::default()
        }));
        let rm = DefaultRiskManager::new(limits);
        let mut low = ctx(1_000_000);
        low.drawdown_pct = 0.11;
        let d_low = rm.evaluate(&low);
        assert!(d_low.allow);
        assert_eq!(d_low.reason, "drawdown_tier1");
        assert!((d_low.capped_size - 0.70).abs() < 1e-9);

        let mut high = ctx(1_000_100);
        high.drawdown_pct = 0.17;
        let d_high = rm.evaluate(&high);
        assert!(d_high.allow);
        assert_eq!(d_high.reason, "drawdown_tier2");
        assert!((d_high.capped_size - 0.40).abs() < 1e-9);
    }
}
