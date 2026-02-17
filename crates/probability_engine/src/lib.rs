use core_types::{Direction, DirectionSignal, ProbabilityEstimate, Signal};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProbabilityEngineConfig {
    pub momentum_gain: f64,
    pub lag_penalty_per_ms: f64,
    pub confidence_floor: f64,
}

impl Default for ProbabilityEngineConfig {
    fn default() -> Self {
        Self {
            momentum_gain: 2.0,
            lag_penalty_per_ms: 0.0002,
            confidence_floor: 0.05,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProbabilityEngine {
    cfg: ProbabilityEngineConfig,
}

impl Default for ProbabilityEngine {
    fn default() -> Self {
        Self::new(ProbabilityEngineConfig::default())
    }
}

impl ProbabilityEngine {
    pub fn new(cfg: ProbabilityEngineConfig) -> Self {
        Self { cfg }
    }

    pub fn cfg(&self) -> &ProbabilityEngineConfig {
        &self.cfg
    }

    pub fn set_cfg(&mut self, cfg: ProbabilityEngineConfig) {
        self.cfg = cfg;
    }

    pub fn estimate(
        &self,
        signal: &Signal,
        direction_signal: &DirectionSignal,
        settlement_prob_yes: Option<f64>,
        book_top_lag_ms: f64,
        now_ms: i64,
    ) -> ProbabilityEstimate {
        let base_fast = signal.fair_yes.clamp(0.0, 1.0);
        let directional_sign = match direction_signal.direction {
            Direction::Up => 1.0,
            Direction::Down => -1.0,
            Direction::Neutral => 0.0,
        };
        let magnitude = (direction_signal.magnitude_pct / 100.0).clamp(0.0, 0.20);
        let directional_bias = directional_sign * magnitude * self.cfg.momentum_gain;
        let p_fast = (base_fast + directional_bias).clamp(0.0, 1.0);
        let p_settle = settlement_prob_yes.unwrap_or(p_fast).clamp(0.0, 1.0);

        let lag_penalty = (book_top_lag_ms.max(0.0) * self.cfg.lag_penalty_per_ms).clamp(0.0, 0.60);
        let confidence = (direction_signal.confidence - lag_penalty)
            .clamp(self.cfg.confidence_floor, 1.0);

        ProbabilityEstimate {
            p_fast,
            p_settle,
            confidence,
            settlement_source_degraded: settlement_prob_yes.is_none(),
            ts_ms: now_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::{Direction, TimeframeClass};

    fn signal() -> Signal {
        Signal {
            market_id: "m1".to_string(),
            fair_yes: 0.52,
            edge_bps_bid: 0.0,
            edge_bps_ask: 0.0,
            confidence: 0.7,
        }
    }

    fn direction(up: bool) -> DirectionSignal {
        DirectionSignal {
            symbol: "BTCUSDT".to_string(),
            direction: if up { Direction::Up } else { Direction::Down },
            magnitude_pct: 0.40,
            confidence: 0.9,
            recommended_tf: TimeframeClass::Tf15m,
            velocity_bps_per_sec: 6.0,
            acceleration: 0.7,
            tick_consistency: 3,
            ts_ns: 1,
        }
    }

    #[test]
    fn estimate_marks_degraded_when_settlement_missing() {
        let engine = ProbabilityEngine::default();
        let p = engine.estimate(&signal(), &direction(true), None, 10.0, 123);
        assert!(p.settlement_source_degraded);
        assert!((0.0..=1.0).contains(&p.p_fast));
        assert!((0.0..=1.0).contains(&p.p_settle));
    }

    #[test]
    fn estimate_uses_settlement_when_provided() {
        let engine = ProbabilityEngine::default();
        let p = engine.estimate(&signal(), &direction(true), Some(0.61), 5.0, 123);
        assert!(!p.settlement_source_degraded);
        assert!((p.p_settle - 0.61).abs() < 1e-9);
    }

    #[test]
    fn lag_penalty_reduces_confidence() {
        let engine = ProbabilityEngine::default();
        let p_low = engine.estimate(&signal(), &direction(true), None, 2.0, 123);
        let p_high = engine.estimate(&signal(), &direction(true), None, 200.0, 123);
        assert!(p_high.confidence < p_low.confidence);
    }
}

