use std::collections::HashMap;

use core_types::{TimeframeClass, TimeframeOpp};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RouterConfig {
    pub max_locked_pct_5m: f64,
    pub max_locked_pct_15m: f64,
    pub max_locked_pct_1h: f64,
    pub max_locked_pct_1d: f64,
    pub max_concurrent_positions: usize,
    pub liquidity_reserve_pct: f64,
    /// Hard caps for micro-live and safety. Defaults are permissive for paper/shadow.
    pub max_order_notional_usdc: f64,
    pub max_total_notional_usdc: f64,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            max_locked_pct_5m: 0.30,
            max_locked_pct_15m: 0.40,
            max_locked_pct_1h: 0.50,
            max_locked_pct_1d: 0.30,
            max_concurrent_positions: 8,
            liquidity_reserve_pct: 0.20,
            max_order_notional_usdc: 1_000_000.0,
            max_total_notional_usdc: 1_000_000.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RouterLock {
    pub market_id: String,
    pub timeframe: TimeframeClass,
    pub notional_usdc: f64,
    pub locked_at_ms: i64,
    pub unlock_at_ms: i64,
}

#[derive(Debug)]
pub struct TimeframeRouter {
    cfg: RouterConfig,
    locks_by_market: HashMap<String, RouterLock>,
}

impl TimeframeRouter {
    pub fn new(cfg: RouterConfig) -> Self {
        Self {
            cfg,
            locks_by_market: HashMap::new(),
        }
    }

    pub fn cfg(&self) -> &RouterConfig {
        &self.cfg
    }

    pub fn set_cfg(&mut self, cfg: RouterConfig) {
        self.cfg = cfg;
    }

    pub fn prune_expired(&mut self, now_ms: i64) {
        self.locks_by_market.retain(|_, l| l.unlock_at_ms > now_ms);
    }

    pub fn active_positions(&mut self, now_ms: i64) -> usize {
        self.prune_expired(now_ms);
        self.locks_by_market.len()
    }

    pub fn locked_total_usdc(&mut self, now_ms: i64) -> f64 {
        self.prune_expired(now_ms);
        self.locks_by_market.values().map(|l| l.notional_usdc).sum()
    }

    pub fn locked_by_tf_usdc(&mut self, now_ms: i64) -> HashMap<TimeframeClass, f64> {
        self.prune_expired(now_ms);
        let mut out: HashMap<TimeframeClass, f64> = HashMap::new();
        for l in self.locks_by_market.values() {
            *out.entry(l.timeframe.clone()).or_insert(0.0) += l.notional_usdc;
        }
        out
    }

    pub fn snapshot_locks(&mut self, now_ms: i64) -> Vec<RouterLock> {
        self.prune_expired(now_ms);
        self.locks_by_market.values().cloned().collect()
    }

    pub fn route(
        &mut self,
        mut candidates: Vec<TimeframeOpp>,
        total_capital_usdc: f64,
        now_ms: i64,
    ) -> Vec<TimeframeOpp> {
        self.prune_expired(now_ms);

        if candidates.is_empty() {
            return Vec::new();
        }
        if total_capital_usdc <= 0.0 {
            return Vec::new();
        }

        if self.locks_by_market.len() >= self.cfg.max_concurrent_positions {
            return Vec::new();
        }

        let locked_total = self
            .locks_by_market
            .values()
            .map(|l| l.notional_usdc)
            .sum::<f64>();
        let reserve =
            (total_capital_usdc * self.cfg.liquidity_reserve_pct.clamp(0.0, 0.95)).max(0.0);
        let mut deployable = (total_capital_usdc - reserve - locked_total).max(0.0);
        if deployable < 1e-9 {
            return Vec::new();
        }

        // Highest density first.
        candidates.sort_by(|a, b| b.density.total_cmp(&a.density));

        let mut out: Vec<TimeframeOpp> = Vec::new();
        let max_new = self
            .cfg
            .max_concurrent_positions
            .saturating_sub(self.locks_by_market.len())
            .max(0);

        let locked_by_tf = self.locked_by_tf_usdc(now_ms);

        for opp in candidates {
            if out.len() >= max_new {
                break;
            }
            if self.locks_by_market.contains_key(&opp.market_id) {
                continue;
            }
            let notional = (opp.entry_price.max(0.0) * opp.size.max(0.0)).max(0.0);
            if notional <= 0.0 {
                continue;
            }
            if notional > self.cfg.max_order_notional_usdc.max(0.0) {
                continue;
            }
            if locked_total
                + out
                    .iter()
                    .map(|o| (o.entry_price * o.size).max(0.0))
                    .sum::<f64>()
                + notional
                > self.cfg.max_total_notional_usdc.max(0.0)
            {
                continue;
            }

            let tf_limit =
                (self.max_locked_pct(&opp.timeframe).clamp(0.0, 1.0) * total_capital_usdc).max(0.0);
            let tf_locked = *locked_by_tf.get(&opp.timeframe).unwrap_or(&0.0);
            if tf_locked + notional > tf_limit {
                continue;
            }
            if notional > deployable {
                continue;
            }

            deployable -= notional;
            out.push(opp);
        }

        out
    }

    pub fn lock(&mut self, opp: &TimeframeOpp, now_ms: i64) -> bool {
        self.prune_expired(now_ms);
        if self.locks_by_market.contains_key(&opp.market_id) {
            return false;
        }
        let notional = (opp.entry_price.max(0.0) * opp.size.max(0.0)).max(0.0);
        if notional <= 0.0 {
            return false;
        }
        let lock_ms = (opp.lock_minutes.max(0.0) * 60_000.0).round() as i64;
        let unlock_at = now_ms.saturating_add(lock_ms.max(0));
        self.locks_by_market.insert(
            opp.market_id.clone(),
            RouterLock {
                market_id: opp.market_id.clone(),
                timeframe: opp.timeframe.clone(),
                notional_usdc: notional,
                locked_at_ms: now_ms,
                unlock_at_ms: unlock_at,
            },
        );
        true
    }

    pub fn unlock_market(&mut self, market_id: &str) -> bool {
        self.locks_by_market.remove(market_id).is_some()
    }

    fn max_locked_pct(&self, tf: &TimeframeClass) -> f64 {
        match tf {
            TimeframeClass::Tf5m => self.cfg.max_locked_pct_5m,
            TimeframeClass::Tf15m => self.cfg.max_locked_pct_15m,
            TimeframeClass::Tf1h => self.cfg.max_locked_pct_1h,
            TimeframeClass::Tf1d => self.cfg.max_locked_pct_1d,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::{Direction, OrderSide};

    fn opp(
        tf: TimeframeClass,
        density: f64,
        market_id: &str,
        entry_price: f64,
        size: f64,
    ) -> TimeframeOpp {
        TimeframeOpp {
            timeframe: tf,
            market_id: market_id.to_string(),
            symbol: "BTCUSDT".to_string(),
            direction: Direction::Up,
            side: OrderSide::BuyYes,
            entry_price,
            size,
            target_l2_size: 0.0,
            edge_gross_bps: 20.0,
            edge_net_bps: 10.0,
            edge_net_usdc: 0.1,
            fee_bps: 2.0,
            lock_minutes: 15.0,
            density,
            confidence: 0.9,
            ts_ms: 0,
        }
    }

    #[test]
    fn sorts_by_density_desc() {
        let mut router = TimeframeRouter::new(RouterConfig {
            max_concurrent_positions: 8,
            liquidity_reserve_pct: 0.0,
            ..RouterConfig::default()
        });
        let routed = router.route(
            vec![
                opp(TimeframeClass::Tf15m, 0.1, "m1", 0.5, 10.0),
                opp(TimeframeClass::Tf15m, 0.2, "m2", 0.5, 10.0),
            ],
            100.0,
            1_000,
        );
        assert_eq!(routed.len(), 2);
        assert_eq!(routed[0].market_id, "m2");
    }

    #[test]
    fn respects_max_positions() {
        let mut router = TimeframeRouter::new(RouterConfig {
            max_concurrent_positions: 1,
            liquidity_reserve_pct: 0.0,
            ..RouterConfig::default()
        });
        // Pre-lock one market.
        router.locks_by_market.insert(
            "m0".to_string(),
            RouterLock {
                market_id: "m0".to_string(),
                timeframe: TimeframeClass::Tf15m,
                notional_usdc: 10.0,
                locked_at_ms: 0,
                unlock_at_ms: 10_000,
            },
        );
        let routed = router.route(
            vec![opp(TimeframeClass::Tf15m, 0.2, "m1", 0.5, 10.0)],
            100.0,
            1_000,
        );
        assert!(routed.is_empty());
    }

    #[test]
    fn respects_liquidity_reserve() {
        let mut router = TimeframeRouter::new(RouterConfig {
            max_concurrent_positions: 8,
            liquidity_reserve_pct: 0.20,
            ..RouterConfig::default()
        });
        // total=10, reserve=2 => deployable=8; order notional=9 => should skip.
        let routed = router.route(
            vec![opp(TimeframeClass::Tf15m, 0.2, "m1", 0.9, 10.0)],
            10.0,
            1_000,
        );
        assert!(routed.is_empty());
    }

    #[test]
    fn respects_timeframe_lock_limit() {
        let mut router = TimeframeRouter::new(RouterConfig {
            max_locked_pct_5m: 0.30,
            max_concurrent_positions: 8,
            liquidity_reserve_pct: 0.0,
            ..RouterConfig::default()
        });
        // 5m lock already 3 out of 10 => 30% cap reached.
        router.locks_by_market.insert(
            "m0".to_string(),
            RouterLock {
                market_id: "m0".to_string(),
                timeframe: TimeframeClass::Tf5m,
                notional_usdc: 3.0,
                locked_at_ms: 0,
                unlock_at_ms: 10_000,
            },
        );
        let routed = router.route(
            vec![opp(TimeframeClass::Tf5m, 0.2, "m1", 0.5, 10.0)],
            10.0,
            1_000,
        );
        assert!(routed.is_empty());
    }
}
