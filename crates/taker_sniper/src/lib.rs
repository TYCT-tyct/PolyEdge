use std::collections::HashMap;

use core_types::{Direction, DirectionSignal, TimeframeClass, TimeframeOpp};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TakerSniperConfig {
    pub min_direction_confidence: f64,
    pub min_edge_net_bps: f64,
    pub max_spread: f64,
    pub cooldown_ms_per_market: u64,
    pub gatling_enabled: bool,
    pub gatling_chunk_notional_usdc: f64,
    pub gatling_min_chunks: usize,
    pub gatling_max_chunks: usize,
    pub gatling_spacing_ms: u64,
    pub gatling_stop_on_reject: bool,
}

impl Default for TakerSniperConfig {
    fn default() -> Self {
        Self {
            min_direction_confidence: 0.60,
            min_edge_net_bps: 10.0,
            max_spread: 0.08,
            cooldown_ms_per_market: 800,
            gatling_enabled: true,
            gatling_chunk_notional_usdc: 5.0,
            gatling_min_chunks: 1,
            gatling_max_chunks: 4,
            gatling_spacing_ms: 12,
            gatling_stop_on_reject: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TakerAction {
    Fire,
    Skip,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FireChunk {
    pub size: f64,
    pub send_delay_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FirePlan {
    pub opportunity: TimeframeOpp,
    pub chunks: Vec<FireChunk>,
    pub stop_on_reject: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TakerDecision {
    pub action: TakerAction,
    pub fire_plan: Option<FirePlan>,
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
        let dynamic_min_edge =
            dynamic_fee_gate_min_edge_bps(entry_price, direction_signal.confidence);
        let min_edge_required = self.cfg.min_edge_net_bps.max(dynamic_min_edge);
        if edge_net_bps < min_edge_required {
            return skip("fee_gate_too_expensive");
        }
        if size <= 0.0 {
            return skip("size_zero");
        }
        if self.cfg.cooldown_ms_per_market > 0 {
            if let Some(last) = self.last_fire_ms_by_market.get(market_id) {
                let age = now_ms.saturating_sub(*last);
                // saturating_sub already ensures age >= 0
                if (age as u64) < self.cfg.cooldown_ms_per_market {
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
        let fire_plan = build_fire_plan(&self.cfg, opp);
        TakerDecision {
            action: TakerAction::Fire,
            fire_plan: Some(fire_plan),
            reason: "fire".to_string(),
        }
    }
}

fn skip(reason: &str) -> TakerDecision {
    TakerDecision {
        action: TakerAction::Skip,
        fire_plan: None,
        reason: reason.to_string(),
    }
}

fn build_fire_plan(cfg: &TakerSniperConfig, opportunity: TimeframeOpp) -> FirePlan {
    let total_size = opportunity.size.max(0.0);
    let notional = (opportunity.entry_price.max(0.0) * total_size).max(0.0);
    let min_chunks = cfg.gatling_min_chunks.max(1);
    let max_chunks = cfg.gatling_max_chunks.max(min_chunks);
    let desired_chunks = if cfg.gatling_enabled && cfg.gatling_chunk_notional_usdc > 0.0 {
        ((notional / cfg.gatling_chunk_notional_usdc).ceil() as usize).clamp(min_chunks, max_chunks)
    } else {
        1
    };

    let mut chunks = Vec::with_capacity(desired_chunks);
    if desired_chunks == 1 {
        chunks.push(FireChunk {
            size: total_size,
            send_delay_ms: 0,
        });
    } else {
        let mut remain = total_size;
        let base = total_size / desired_chunks as f64;
        for idx in 0..desired_chunks {
            let mut size = if idx + 1 == desired_chunks { remain } else { base };
            if idx + 1 != desired_chunks {
                size = size.max(0.01);
                remain = (remain - size).max(0.0);
            }
            chunks.push(FireChunk {
                size,
                send_delay_ms: if idx == 0 { 0 } else { cfg.gatling_spacing_ms },
            });
        }
    }

    FirePlan {
        opportunity,
        chunks,
        stop_on_reject: cfg.gatling_stop_on_reject,
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

#[inline]
fn dynamic_fee_gate_min_edge_bps(entry_price: f64, confidence: f64) -> f64 {
    let p = entry_price.clamp(0.0, 1.0);
    let base_gate = if p >= 0.92 || p <= 0.08 {
        20.0
    } else if p >= 0.85 || p <= 0.15 {
        35.0
    } else if p >= 0.75 || p <= 0.25 {
        60.0
    } else if p >= 0.60 || p <= 0.40 {
        90.0
    } else {
        120.0
    };
    let confidence_relax = (1.0 - (confidence.clamp(0.0, 1.0) - 0.5).max(0.0) * 0.4)
        .clamp(0.75, 1.0);
    base_gate * confidence_relax
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
            velocity_bps_per_sec: 7.5,
            acceleration: 0.8,
            tick_consistency: 3,
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
            ..TakerSniperConfig::default()
        });
        let sig = up_signal(0.9);
        let d = sniper.evaluate(
            "m1",
            "BTCUSDT",
            TimeframeClass::Tf15m,
            &sig,
            0.95,
            0.01,
            2.0,
            30.0,
            32.0,
            10.0,
            1_000_000,
        );
        assert!(matches!(d.action, TakerAction::Fire));
        assert!(d.fire_plan.is_some());
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
            32.0,
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
        assert_eq!(d.reason, "fee_gate_too_expensive");
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
            0.95,
            0.01,
            2.0,
            30.0,
            32.0,
            10.0,
            1_000_000,
        );
        assert!(matches!(d1.action, TakerAction::Fire));
        let d2 = sniper.evaluate(
            "m1",
            "BTCUSDT",
            TimeframeClass::Tf15m,
            &sig,
            0.95,
            0.01,
            2.0,
            30.0,
            32.0,
            10.0,
            1_000_500,
        );
        assert!(matches!(d2.action, TakerAction::Skip));
        assert_eq!(d2.reason, "cooldown_active");
    }

    #[test]
    fn dynamic_fee_gate_blocks_mid_price_without_large_edge() {
        let mut sniper = TakerSniper::new(TakerSniperConfig {
            min_edge_net_bps: 10.0,
            ..TakerSniperConfig::default()
        });
        let sig = up_signal(0.9);
        let d = sniper.evaluate(
            "m1",
            "BTCUSDT",
            TimeframeClass::Tf15m,
            &sig,
            0.50,
            0.01,
            2.0,
            60.0,
            95.0,
            10.0,
            1_000_000,
        );
        assert!(matches!(d.action, TakerAction::Skip));
        assert_eq!(d.reason, "fee_gate_too_expensive");
    }

    #[test]
    fn dynamic_fee_gate_allows_extreme_price_with_small_edge() {
        let mut sniper = TakerSniper::new(TakerSniperConfig {
            min_edge_net_bps: 10.0,
            ..TakerSniperConfig::default()
        });
        let sig = up_signal(0.9);
        let d = sniper.evaluate(
            "m1",
            "BTCUSDT",
            TimeframeClass::Tf15m,
            &sig,
            0.95,
            0.01,
            2.0,
            60.0,
            40.0,
            10.0,
            1_000_000,
        );
        assert!(matches!(d.action, TakerAction::Fire));
    }

    #[test]
    fn dynamic_fee_gate_relaxes_with_higher_confidence() {
        let mut sniper = TakerSniper::new(TakerSniperConfig {
            min_edge_net_bps: 10.0,
            ..TakerSniperConfig::default()
        });
        let low = up_signal(0.55);
        let high = up_signal(0.95);
        let d_low = sniper.evaluate(
            "m1",
            "BTCUSDT",
            TimeframeClass::Tf15m,
            &low,
            0.50,
            0.01,
            2.0,
            60.0,
            95.0,
            10.0,
            1_000_000,
        );
        assert!(matches!(d_low.action, TakerAction::Skip));
        let d_high = sniper.evaluate(
            "m2",
            "BTCUSDT",
            TimeframeClass::Tf15m,
            &high,
            0.50,
            0.01,
            2.0,
            60.0,
            105.0,
            10.0,
            1_000_000,
        );
        assert!(matches!(d_high.action, TakerAction::Fire));
    }

    #[test]
    fn gatling_plan_splits_into_chunks() {
        let mut sniper = TakerSniper::new(TakerSniperConfig {
            gatling_enabled: true,
            gatling_chunk_notional_usdc: 2.0,
            gatling_min_chunks: 2,
            gatling_max_chunks: 5,
            ..TakerSniperConfig::default()
        });
        let sig = up_signal(0.9);
        let d = sniper.evaluate(
            "m1",
            "BTCUSDT",
            TimeframeClass::Tf15m,
            &sig,
            0.90,
            0.01,
            2.0,
            80.0,
            120.0,
            10.0,
            1_000_000,
        );
        assert!(matches!(d.action, TakerAction::Fire));
        let Some(plan) = d.fire_plan else {
            panic!("expected fire plan");
        };
        assert!(plan.chunks.len() >= 2);
        assert!(plan.stop_on_reject);
        let total: f64 = plan.chunks.iter().map(|c| c.size).sum();
        assert!((total - 10.0).abs() < 1e-6);
    }
}
