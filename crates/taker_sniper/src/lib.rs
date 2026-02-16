use std::collections::HashMap;

use core_types::{Direction, DirectionSignal, TimeframeClass, TimeframeOpp};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TakerSniperConfig {
    pub min_direction_confidence: f64,
    pub min_edge_net_bps: f64,
    pub max_spread: f64,
    pub cooldown_ms_per_market: u64,
}

impl Default for TakerSniperConfig {
    fn default() -> Self {
        Self {
            min_direction_confidence: 0.70,
            min_edge_net_bps: 10.0,
            max_spread: 0.08,
            cooldown_ms_per_market: 800,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TakerAction {
    Fire,
    Skip,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TakerDecision {
    pub action: TakerAction,
    pub opportunity: Option<TimeframeOpp>,
    pub reason: String,
}

#[derive(Debug)]
pub struct TakerSniper {
    cfg: TakerSniperConfig,
    last_fire_ms_by_market: HashMap<String, i64>,
}

impl TakerSniper {
    pub fn new(cfg: TakerSniperConfig) -> Self {
        Self {
            cfg,
            last_fire_ms_by_market: HashMap::new(),
        }
    }

    pub fn cfg(&self) -> &TakerSniperConfig {
        &self.cfg
    }

    pub fn set_cfg(&mut self, cfg: TakerSniperConfig) {
        self.cfg = cfg;
    }

    pub fn evaluate(
        &mut self,
        market_id: &str,
        symbol: &str,
        timeframe: TimeframeClass,
        direction_signal: &DirectionSignal,
        entry_price: f64,
        spread: f64,
        fee_bps: f64,
        edge_gross_bps: f64,
        edge_net_bps: f64,
        size: f64,
        now_ms: i64,
    ) -> TakerDecision {
        if matches!(direction_signal.direction, Direction::Neutral) {
            return skip("neutral_direction");
        }
        if direction_signal.confidence < self.cfg.min_direction_confidence {
            return skip("low_confidence");
        }
        if entry_price <= 0.0 {
            return skip("bad_price");
        }
        if spread > self.cfg.max_spread {
            return skip("spread_too_wide");
        }
        if edge_net_bps < self.cfg.min_edge_net_bps {
            return skip("edge_below_threshold");
        }
        if size <= 0.0 {
            return skip("size_zero");
        }
        if self.cfg.cooldown_ms_per_market > 0 {
            if let Some(last) = self.last_fire_ms_by_market.get(market_id) {
                let age = now_ms.saturating_sub(*last);
                if age >= 0 && (age as u64) < self.cfg.cooldown_ms_per_market {
                    return skip("cooldown_active");
                }
            }
        }

        let lock_minutes = lock_minutes_for_timeframe(&timeframe);
        let notional_usdc = (entry_price.max(0.0) * size.max(0.0)).max(0.0);
        let edge_net_usdc = (edge_net_bps / 10_000.0) * notional_usdc;
        let density = if lock_minutes <= 0.0 {
            0.0
        } else {
            edge_net_usdc / lock_minutes
        };
        let opp = TimeframeOpp {
            timeframe,
            market_id: market_id.to_string(),
            symbol: symbol.to_string(),
            direction: direction_signal.direction.clone(),
            side: direction_to_side(&direction_signal.direction),
            entry_price,
            size,
            edge_gross_bps,
            edge_net_bps,
            edge_net_usdc,
            fee_bps,
            lock_minutes,
            density,
            confidence: direction_signal.confidence,
            ts_ms: now_ms,
        };
        self.last_fire_ms_by_market
            .insert(market_id.to_string(), now_ms);
        TakerDecision {
            action: TakerAction::Fire,
            opportunity: Some(opp),
            reason: "fire".to_string(),
        }
    }
}

fn skip(reason: &str) -> TakerDecision {
    TakerDecision {
        action: TakerAction::Skip,
        opportunity: None,
        reason: reason.to_string(),
    }
}

fn lock_minutes_for_timeframe(tf: &TimeframeClass) -> f64 {
    match tf {
        TimeframeClass::Tf5m => 5.0,
        TimeframeClass::Tf15m => 15.0,
        TimeframeClass::Tf1h => 60.0,
        TimeframeClass::Tf1d => 1440.0,
    }
}

fn direction_to_side(dir: &Direction) -> core_types::OrderSide {
    match dir {
        Direction::Up => core_types::OrderSide::BuyYes,
        Direction::Down => core_types::OrderSide::BuyNo,
        Direction::Neutral => core_types::OrderSide::BuyYes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::{Direction, DirectionSignal, TimeframeClass};

    fn up_signal(confidence: f64) -> DirectionSignal {
        DirectionSignal {
            symbol: "BTCUSDT".to_string(),
            direction: Direction::Up,
            magnitude_pct: 0.20,
            confidence,
            recommended_tf: TimeframeClass::Tf15m,
            ts_ns: 1,
        }
    }

    #[test]
    fn fires_on_strong_signal() {
        let mut sniper = TakerSniper::new(TakerSniperConfig {
            min_direction_confidence: 0.7,
            min_edge_net_bps: 5.0,
            max_spread: 0.08,
            cooldown_ms_per_market: 0,
        });
        let sig = up_signal(0.9);
        let d = sniper.evaluate(
            "m1",
            "BTCUSDT",
            TimeframeClass::Tf15m,
            &sig,
            0.52,
            0.01,
            2.0,
            30.0,
            28.0,
            10.0,
            1_000_000,
        );
        assert!(matches!(d.action, TakerAction::Fire));
        assert!(d.opportunity.is_some());
    }

    #[test]
    fn skips_low_confidence() {
        let mut sniper = TakerSniper::new(TakerSniperConfig::default());
        let sig = up_signal(0.5);
        let d = sniper.evaluate(
            "m1",
            "BTCUSDT",
            TimeframeClass::Tf15m,
            &sig,
            0.52,
            0.01,
            2.0,
            30.0,
            28.0,
            10.0,
            1_000_000,
        );
        assert!(matches!(d.action, TakerAction::Skip));
        assert_eq!(d.reason, "low_confidence");
    }

    #[test]
    fn skips_edge_below_threshold() {
        let mut sniper = TakerSniper::new(TakerSniperConfig {
            min_edge_net_bps: 25.0,
            ..TakerSniperConfig::default()
        });
        let sig = up_signal(0.9);
        let d = sniper.evaluate(
            "m1",
            "BTCUSDT",
            TimeframeClass::Tf15m,
            &sig,
            0.52,
            0.01,
            2.0,
            30.0,
            24.0,
            10.0,
            1_000_000,
        );
        assert!(matches!(d.action, TakerAction::Skip));
        assert_eq!(d.reason, "edge_below_threshold");
    }

    #[test]
    fn cooldown_blocks_repeated_fire() {
        let mut sniper = TakerSniper::new(TakerSniperConfig {
            min_edge_net_bps: 5.0,
            cooldown_ms_per_market: 1_000,
            ..TakerSniperConfig::default()
        });
        let sig = up_signal(0.9);
        let d1 = sniper.evaluate(
            "m1",
            "BTCUSDT",
            TimeframeClass::Tf15m,
            &sig,
            0.52,
            0.01,
            2.0,
            30.0,
            28.0,
            10.0,
            1_000_000,
        );
        assert!(matches!(d1.action, TakerAction::Fire));
        let d2 = sniper.evaluate(
            "m1",
            "BTCUSDT",
            TimeframeClass::Tf15m,
            &sig,
            0.52,
            0.01,
            2.0,
            30.0,
            28.0,
            10.0,
            1_000_500,
        );
        assert!(matches!(d2.action, TakerAction::Skip));
        assert_eq!(d2.reason, "cooldown_active");
    }
}

