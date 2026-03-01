pub mod health;
pub mod protocol;
pub mod tracker;

#[cfg(test)]
mod tests {
    use crate::health::{evaluate_health, HealthConfig, HealthState};
    use crate::tracker::StreamTracker;

    #[test]
    fn tracker_detects_gap_and_duplicate() {
        let mut tracker = StreamTracker::new();
        tracker.observe(1, 1000, 1010);
        tracker.observe(2, 1010, 1020);
        tracker.observe(4, 1020, 1030);
        tracker.observe(4, 1030, 1040);

        let snapshot = tracker.snapshot();
        assert_eq!(snapshot.received, 4);
        assert_eq!(snapshot.gap_count, 1);
        assert_eq!(snapshot.lost_frames, 1);
        assert_eq!(snapshot.duplicate_count, 1);
    }

    #[test]
    fn health_goes_lockdown_when_gap_rate_high() {
        let cfg = HealthConfig::default();
        let state = evaluate_health(
            &cfg, 0.08, // 8% gap rate
            2.0,  // low latency
            10,
        );
        assert_eq!(state, HealthState::Lockdown);
    }
}
