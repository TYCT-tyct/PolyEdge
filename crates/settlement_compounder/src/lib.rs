use core_types::CapitalUpdate;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompounderConfig {
    pub enabled: bool,
    pub initial_capital_usdc: f64,
    /// Share of profits to compound (1.0 = 100%).
    pub compound_ratio: f64,
    /// Recommended per-order notional = available * position_fraction (bounded by min_quote_size).
    pub position_fraction: f64,
    /// Minimum per-order notional in USDC.
    pub min_quote_size: f64,
    /// Hard-stop threshold (USDC). If daily_pnl <= -cap, compounder marks halted.
    pub daily_loss_cap_usdc: f64,
}

impl Default for CompounderConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            initial_capital_usdc: 100.0,
            compound_ratio: 1.0,
            position_fraction: 0.15,
            min_quote_size: 1.0,
            daily_loss_cap_usdc: 1.0,
        }
    }
}

#[derive(Debug)]
pub struct SettlementCompounder {
    cfg: CompounderConfig,
    available_usdc: f64,
    initial_usdc: f64,
    total_pnl: f64,
    daily_pnl: f64,
    daily_epoch_day: i64,
    win_count: u64,
    loss_count: u64,
    halted: bool,
}

impl SettlementCompounder {
    pub fn new(cfg: CompounderConfig) -> Self {
        let initial = cfg.initial_capital_usdc.max(0.0);
        let now = now_ms();
        Self {
            cfg,
            available_usdc: initial,
            initial_usdc: initial,
            total_pnl: 0.0,
            daily_pnl: 0.0,
            daily_epoch_day: epoch_day_from_ms(now),
            win_count: 0,
            loss_count: 0,
            halted: false,
        }
    }

    pub fn cfg(&self) -> &CompounderConfig {
        &self.cfg
    }

    pub fn set_cfg(&mut self, mut cfg: CompounderConfig) {
        // Validate and clamp config values to valid ranges
        cfg.compound_ratio = cfg.compound_ratio.clamp(0.0, 1.0);
        cfg.position_fraction = cfg.position_fraction.clamp(0.0, 1.0);
        cfg.min_quote_size = cfg.min_quote_size.max(0.0);
        cfg.daily_loss_cap_usdc = cfg.daily_loss_cap_usdc.max(0.0);
        self.cfg = cfg;
        if self.initial_usdc <= 0.0 {
            self.initial_usdc = self.cfg.initial_capital_usdc.max(0.0);
        }
    }

    pub fn available(&self) -> f64 {
        self.available_usdc
    }

    pub fn total_pnl(&self) -> f64 {
        self.total_pnl
    }

    pub fn daily_pnl(&self) -> f64 {
        self.daily_pnl
    }

    pub fn win_rate(&self) -> f64 {
        let total = self.win_count + self.loss_count;
        if total == 0 {
            0.0
        } else {
            (self.win_count as f64 / total as f64).clamp(0.0, 1.0)
        }
    }

    pub fn halted(&self) -> bool {
        self.halted
    }

    pub fn recommended_quote_notional_usdc(&self) -> f64 {
        if !self.cfg.enabled {
            return 0.0;
        }
        (self.available_usdc.max(0.0) * self.cfg.position_fraction.clamp(0.0, 1.0))
            .max(self.cfg.min_quote_size.max(0.0))
    }

    pub fn on_markout(&mut self, pnl_usdc: f64) -> CapitalUpdate {
        let ts_ms = now_ms();
        self.on_markout_at(pnl_usdc, ts_ms)
    }

    fn on_markout_at(&mut self, pnl_usdc: f64, ts_ms: i64) -> CapitalUpdate {
        self.rollover_day_if_needed(ts_ms);
        if !self.cfg.enabled {
            return CapitalUpdate {
                available_usdc: self.available_usdc,
                base_quote_size: 0.0,
                ts_ms,
            };
        }

        self.total_pnl += pnl_usdc;
        self.daily_pnl += pnl_usdc;

        if pnl_usdc > 0.0 {
            self.win_count = self.win_count.saturating_add(1);
            self.available_usdc += pnl_usdc * self.cfg.compound_ratio.clamp(0.0, 1.0);
        } else if pnl_usdc < 0.0 {
            self.loss_count = self.loss_count.saturating_add(1);
            self.available_usdc += pnl_usdc;
        }

        if self.available_usdc < 0.0 {
            self.available_usdc = 0.0;
        }

        if self.cfg.daily_loss_cap_usdc > 0.0 && self.daily_pnl <= -self.cfg.daily_loss_cap_usdc {
            self.halted = true;
        }

        CapitalUpdate {
            available_usdc: self.available_usdc,
            base_quote_size: self.recommended_quote_notional_usdc(),
            ts_ms,
        }
    }

    pub fn reset_daily(&mut self) {
        self.daily_pnl = 0.0;
        self.halted = false;
        self.daily_epoch_day = epoch_day_from_ms(now_ms());
    }

    fn rollover_day_if_needed(&mut self, ts_ms: i64) {
        let day = epoch_day_from_ms(ts_ms);
        if day != self.daily_epoch_day {
            self.daily_pnl = 0.0;
            self.halted = false;
            self.daily_epoch_day = day;
        }
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[inline]
fn epoch_day_from_ms(ts_ms: i64) -> i64 {
    ts_ms.div_euclid(86_400_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compound_increases_available() {
        let mut c = SettlementCompounder::new(CompounderConfig {
            enabled: true,
            initial_capital_usdc: 100.0,
            compound_ratio: 1.0,
            position_fraction: 0.15,
            min_quote_size: 1.0,
            daily_loss_cap_usdc: 0.0,
        });
        let u = c.on_markout(10.0);
        assert!((u.available_usdc - 110.0).abs() < 1e-9);
    }

    #[test]
    fn loss_decreases_available() {
        let mut c = SettlementCompounder::new(CompounderConfig {
            enabled: true,
            initial_capital_usdc: 100.0,
            compound_ratio: 1.0,
            position_fraction: 0.15,
            min_quote_size: 1.0,
            daily_loss_cap_usdc: 0.0,
        });
        let u = c.on_markout(-20.0);
        assert!((u.available_usdc - 80.0).abs() < 1e-9);
    }

    #[test]
    fn quote_notional_scales_with_capital() {
        let c = SettlementCompounder::new(CompounderConfig {
            enabled: true,
            initial_capital_usdc: 200.0,
            compound_ratio: 1.0,
            position_fraction: 0.15,
            min_quote_size: 1.0,
            daily_loss_cap_usdc: 0.0,
        });
        assert!((c.recommended_quote_notional_usdc() - 30.0).abs() < 1e-9);
    }

    #[test]
    fn halt_on_daily_cap() {
        let mut c = SettlementCompounder::new(CompounderConfig {
            enabled: true,
            initial_capital_usdc: 100.0,
            compound_ratio: 1.0,
            position_fraction: 0.15,
            min_quote_size: 1.0,
            daily_loss_cap_usdc: 10.0,
        });
        c.on_markout(-11.0);
        assert!(c.halted());
    }

    #[test]
    fn never_negative_available() {
        let mut c = SettlementCompounder::new(CompounderConfig {
            enabled: true,
            initial_capital_usdc: 5.0,
            compound_ratio: 1.0,
            position_fraction: 0.15,
            min_quote_size: 1.0,
            daily_loss_cap_usdc: 0.0,
        });
        c.on_markout(-20.0);
        assert!(c.available() >= 0.0);
    }

    #[test]
    fn daily_halt_auto_recovers_after_day_rollover() {
        let mut c = SettlementCompounder::new(CompounderConfig {
            enabled: true,
            initial_capital_usdc: 100.0,
            compound_ratio: 1.0,
            position_fraction: 0.15,
            min_quote_size: 1.0,
            daily_loss_cap_usdc: 10.0,
        });

        // Day 0: trigger halt.
        let _ = c.on_markout_at(-11.0, 1_000);
        assert!(c.halted());

        // Day 1: a new markout should roll daily state and clear halt.
        let _ = c.on_markout_at(1.0, 86_400_000 + 2_000);
        assert!(!c.halted());
        assert!(c.daily_pnl() > 0.0);
    }
}
