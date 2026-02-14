use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct MonotonicClock {
    boot: Instant,
    offset_ns: AtomicI64,
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self {
            boot: Instant::now(),
            offset_ns: AtomicI64::new(0),
        }
    }
}

impl MonotonicClock {
    pub fn now_ns(&self) -> i64 {
        let elapsed = self.boot.elapsed().as_nanos() as i64;
        elapsed + self.offset_ns.load(Ordering::Relaxed)
    }

    pub fn now_ms(&self) -> i64 {
        self.now_ns() / 1_000_000
    }

    pub fn utc_now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    pub fn apply_offset_ns(&self, offset_ns: i64) {
        self.offset_ns.store(offset_ns, Ordering::Relaxed);
    }

    pub fn offset_ns(&self) -> i64 {
        self.offset_ns.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NtpSample {
    pub source: String,
    pub measured_offset_ns: i64,
    pub roundtrip_ns: i64,
    pub ts_ms: i64,
}

#[derive(Debug, Default)]
pub struct NtpOffsetMonitor {
    history: RwLock<Vec<NtpSample>>,
}

impl NtpOffsetMonitor {
    pub fn push(&self, sample: NtpSample) {
        let mut h = self.history.write();
        h.push(sample);
        if h.len() > 1024 {
            let drop_n = h.len() - 1024;
            h.drain(0..drop_n);
        }
    }

    pub fn latest_offset_ns(&self) -> i64 {
        self.history
            .read()
            .last()
            .map(|s| s.measured_offset_ns)
            .unwrap_or(0)
    }

    pub fn smoothed_offset_ns(&self, window: usize) -> i64 {
        let h = self.history.read();
        if h.is_empty() {
            return 0;
        }
        let start = h.len().saturating_sub(window.max(1));
        let slice = &h[start..];
        let sum: i64 = slice.iter().map(|s| s.measured_offset_ns).sum();
        sum / slice.len() as i64
    }

    pub fn len(&self) -> usize {
        self.history.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.history.read().is_empty()
    }
}

pub fn sleep_until(deadline: Instant) {
    let now = Instant::now();
    if deadline > now {
        std::thread::sleep(deadline - now);
    }
}

pub fn duration_from_us(us: u64) -> Duration {
    Duration::from_micros(us)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic_clock_moves_forward() {
        let clock = MonotonicClock::default();
        let a = clock.now_ns();
        std::thread::sleep(Duration::from_millis(1));
        let b = clock.now_ns();
        assert!(b > a);
    }

    #[test]
    fn ntp_smoothing_works() {
        let monitor = NtpOffsetMonitor::default();
        monitor.push(NtpSample {
            source: "a".to_string(),
            measured_offset_ns: 10,
            roundtrip_ns: 100,
            ts_ms: 1,
        });
        monitor.push(NtpSample {
            source: "b".to_string(),
            measured_offset_ns: 20,
            roundtrip_ns: 100,
            ts_ms: 2,
        });
        assert_eq!(monitor.smoothed_offset_ns(2), 15);
    }
}
