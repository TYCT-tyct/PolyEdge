use core_types::{Direction, DirectionSignal, ProbabilityEstimate, Signal};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProbabilityEngineConfig {
    pub momentum_gain: f64,
    pub lag_penalty_per_ms: f64,
    pub confidence_floor: f64,
    /// Black-Scholes/GBM annualized sigma.
    pub sigma_annual: f64,
    /// Horizon for near-term digital direction probability.
    pub horizon_sec: f64,
    /// Base annualized drift component.
    pub drift_annual: f64,
    /// Scale from velocity (bps/s) to annualized drift component.
    pub velocity_drift_gain: f64,
    /// Scale from acceleration ((bps/s)/s) to annualized drift component.
    pub acceleration_drift_gain: f64,
    /// Blend weight for fair-value prior (0..1). Remaining weight uses BS/GBM model.
    pub fair_blend_weight: f64,
}

impl Default for ProbabilityEngineConfig {
    fn default() -> Self {
        Self {
            momentum_gain: 2.0,
            lag_penalty_per_ms: 0.0002,
            confidence_floor: 0.05,
            sigma_annual: 0.90,
            horizon_sec: 30.0,
            drift_annual: 0.0,
            velocity_drift_gain: 0.35,
            acceleration_drift_gain: 0.02,
            fair_blend_weight: 0.35,
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
        let p_bs = bs_directional_probability(direction_signal, &self.cfg);
        let blend_w = self.cfg.fair_blend_weight.clamp(0.0, 1.0);
        let p_fast = ((base_fast * blend_w) + (p_bs * (1.0 - blend_w))).clamp(0.0, 1.0);
        let p_settle = settlement_prob_yes.unwrap_or(p_fast).clamp(0.0, 1.0);

        let lag_penalty = (book_top_lag_ms.max(0.0) * self.cfg.lag_penalty_per_ms).clamp(0.0, 0.60);
        let settle_alignment = 1.0 - (p_fast - p_settle).abs().min(1.0);
        let confidence = (direction_signal.confidence * settle_alignment - lag_penalty)
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

#[inline]
fn bs_directional_probability(
    direction_signal: &DirectionSignal,
    cfg: &ProbabilityEngineConfig,
) -> f64 {
    const SEC_PER_YEAR: f64 = 31_536_000.0;
    let t = (cfg.horizon_sec.max(0.1) / SEC_PER_YEAR).clamp(1e-9, 1.0);
    let sigma = cfg.sigma_annual.max(1e-6);
    let sign = match direction_signal.direction {
        Direction::Up => 1.0,
        Direction::Down => -1.0,
        Direction::Neutral => 0.0,
    };
    let momentum_component =
        sign * (direction_signal.magnitude_pct.abs() / 100.0) * cfg.momentum_gain * 0.25;
    let velocity_component =
        sign * (direction_signal.velocity_bps_per_sec / 10_000.0) * cfg.velocity_drift_gain;
    let acceleration_component =
        sign * (direction_signal.acceleration / 10_000.0) * cfg.acceleration_drift_gain;
    let drift = cfg.drift_annual + momentum_component + velocity_component + acceleration_component;
    let denom = sigma * t.sqrt();
    let d2 = ((drift - 0.5 * sigma * sigma) * t) / denom.max(1e-9);
    normal_cdf(d2)
}

#[inline]
fn normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf_approx(x / std::f64::consts::SQRT_2))
}

// Abramowitz and Stegun 7.1.26 approximation, sufficient for strategy gating.
#[inline]
fn erf_approx(x: f64) -> f64 {
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();
    let t = 1.0 / (1.0 + 0.327_591_1 * x);
    let a1 = 0.254_829_592;
    let a2 = -0.284_496_736;
    let a3 = 1.421_413_741;
    let a4 = -1.453_152_027;
    let a5 = 1.061_405_429;
    let y = 1.0 - (((((a5 * t + a4) * t + a3) * t + a2) * t + a1) * t * (-x * x).exp());
    sign * y
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
            triple_confirm: true,
            momentum_spike: false,
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

    #[test]
    fn bs_probability_respects_direction() {
        let engine = ProbabilityEngine::default();
        let p_up = engine.estimate(&signal(), &direction(true), None, 5.0, 123);
        let p_dn = engine.estimate(&signal(), &direction(false), None, 5.0, 123);
        assert!(p_up.p_fast > p_dn.p_fast);
    }

    #[test]
    fn confidence_penalized_when_settlement_diverges() {
        let engine = ProbabilityEngine::default();
        let aligned = engine.estimate(&signal(), &direction(true), Some(0.60), 5.0, 123);
        let diverged = engine.estimate(&signal(), &direction(true), Some(0.10), 5.0, 123);
        assert!(diverged.confidence < aligned.confidence);
    }
}
