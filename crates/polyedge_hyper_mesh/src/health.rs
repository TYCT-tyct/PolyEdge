#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthState {
    Normal,
    Caution,
    Lockdown,
}

#[derive(Debug, Clone)]
pub struct HealthConfig {
    pub caution_gap_rate: f64,
    pub lockdown_gap_rate: f64,
    pub caution_latency_ms: f64,
    pub lockdown_latency_ms: f64,
    pub stale_limit_ms: i64,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            caution_gap_rate: 0.01,
            lockdown_gap_rate: 0.03,
            caution_latency_ms: 35.0,
            lockdown_latency_ms: 70.0,
            stale_limit_ms: 2_500,
        }
    }
}

pub fn evaluate_health(
    cfg: &HealthConfig,
    gap_rate: f64,
    mean_latency_ms: f64,
    stale_age_ms: i64,
) -> HealthState {
    let stale = stale_age_ms >= cfg.stale_limit_ms;
    if gap_rate >= cfg.lockdown_gap_rate || mean_latency_ms >= cfg.lockdown_latency_ms || stale {
        return HealthState::Lockdown;
    }
    if gap_rate >= cfg.caution_gap_rate || mean_latency_ms >= cfg.caution_latency_ms {
        return HealthState::Caution;
    }
    HealthState::Normal
}
