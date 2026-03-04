//! Data Quality Monitoring Module
//!
//! This module provides real-time data quality scoring, latency monitoring,
//! anomaly detection, and data smoothing for the trading strategy.
//!
//! MAJOR CHANGE: This module was added as part of the comprehensive data
//! collection optimization (v2.0-stale-quote-sniping-optimization).

use serde::{Deserialize, Serialize};

/// Ring buffer for efficient circular data storage
/// O(1) push, O(n) iteration - much faster than Vec for sliding windows
#[derive(Debug, Clone)]
pub struct RingBuffer<T> {
    buffer: Vec<T>,
    head: usize,
    size: usize,
    capacity: usize,
}

impl<T: Default + Clone> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: vec![T::default(); capacity],
            head: 0,
            size: 0,
            capacity,
        }
    }

    /// Push a new value - O(1) complexity
    pub fn push(&mut self, value: T) {
        self.buffer[self.head] = value;
        self.head = (self.head + 1) % self.capacity;
        if self.size < self.capacity {
            self.size += 1;
        }
    }

    /// Get current size
    pub fn len(&self) -> usize {
        self.size
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Iterate over elements in order (oldest first)
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &T> {
        let start = if self.size == self.capacity {
            self.head
        } else {
            0
        };
        (0..self.size).map(move |i| &self.buffer[(start + i) % self.capacity])
    }

    /// Get last element (most recent)
    pub fn last(&self) -> Option<&T> {
        if self.is_empty() {
            return None;
        }
        let idx = if self.size == self.capacity {
            (self.head + self.capacity - 1) % self.capacity
        } else {
            self.size - 1
        };
        Some(&self.buffer[idx])
    }

    /// Calculate mean for numeric types
    pub fn mean(&self) -> f64
    where
        T: Into<f64> + Copy,
    {
        if self.is_empty() {
            return 0.0;
        }
        self.iter().map(|&v| v.into()).sum::<f64>() / self.size as f64
    }
}

/// Exponential Moving Average smoother
/// Reduces noise while maintaining responsiveness
#[derive(Debug, Clone)]
pub struct EmaSmoother {
    alpha: f64, // Smoothing factor (0-1), higher = more responsive
    value: Option<f64>,
}

impl EmaSmoother {
    /// Create new EMA smoother
    /// alpha: 0.0-1.0, typical values: 0.1 (slow) to 0.5 (fast)
    pub fn new(alpha: f64) -> Self {
        Self {
            alpha: alpha.clamp(0.001, 0.999),
            value: None,
        }
    }

    /// Update with new value, return smoothed value
    pub fn update(&mut self, new_value: f64) -> f64 {
        let smoothed = match self.value {
            Some(prev) => self.alpha * new_value + (1.0 - self.alpha) * prev,
            None => new_value,
        };
        self.value = Some(smoothed);
        smoothed
    }

    /// Get current smoothed value
    pub fn current(&self) -> Option<f64> {
        self.value
    }

    /// Reset smoother
    pub fn reset(&mut self) {
        self.value = None;
    }
}

/// Anomaly detector using Z-score method
#[derive(Debug, Clone)]
pub struct AnomalyDetector {
    window_size: usize,
    threshold: f64,
    history: RingBuffer<f64>,
}

impl AnomalyDetector {
    /// Create new anomaly detector
    /// window_size: number of historical samples to use
    /// threshold: Z-score threshold (typical: 3.0 for 99.7% confidence)
    pub fn new(window_size: usize, threshold: f64) -> Self {
        Self {
            window_size,
            threshold,
            history: RingBuffer::new(window_size),
        }
    }

    /// Check if value is anomalous
    /// Returns (is_anomaly, z_score)
    pub fn check(&mut self, value: f64) -> (bool, f64) {
        // Need minimum samples
        if self.history.len() < 5 {
            self.history.push(value);
            return (false, 0.0);
        }

        let mean = self.history.mean();
        let variance: f64 = self
            .history
            .iter()
            .map(|&v| (v - mean).powi(2))
            .sum::<f64>()
            / self.history.len() as f64;
        let std_dev = variance.sqrt().max(1e-9);

        let z_score = (value - mean) / std_dev;
        let is_anomaly = z_score.abs() > self.threshold;

        // Don't add anomalies to history to prevent contamination
        if !is_anomaly {
            self.history.push(value);
        }

        (is_anomaly, z_score)
    }
}

/// Latency monitor for tracking data delays
#[derive(Debug, Clone)]
pub struct LatencyMonitor {
    binance_recv_latency_ms: RingBuffer<f64>,
    pm_recv_latency_ms: RingBuffer<f64>,
    processing_latency_ms: RingBuffer<f64>,
}

impl Default for LatencyMonitor {
    fn default() -> Self {
        Self::new(100)
    }
}

impl LatencyMonitor {
    pub fn new(window_size: usize) -> Self {
        Self {
            binance_recv_latency_ms: RingBuffer::new(window_size),
            pm_recv_latency_ms: RingBuffer::new(window_size),
            processing_latency_ms: RingBuffer::new(window_size),
        }
    }

    /// Record Binance data reception latency
    pub fn record_binance_latency(&mut self, sent_ts_ms: i64) {
        let now = chrono::Utc::now().timestamp_millis();
        let latency = (now - sent_ts_ms).max(0) as f64;
        self.binance_recv_latency_ms.push(latency);
    }

    /// Record Polymarket data reception latency
    pub fn record_pm_latency(&mut self, sent_ts_ms: i64) {
        let now = chrono::Utc::now().timestamp_millis();
        let latency = (now - sent_ts_ms).max(0) as f64;
        self.pm_recv_latency_ms.push(latency);
    }

    /// Record processing latency
    pub fn record_processing_latency(&mut self, start_ts_ms: i64) {
        let now = chrono::Utc::now().timestamp_millis();
        let latency = (now - start_ts_ms).max(0) as f64;
        self.processing_latency_ms.push(latency);
    }

    /// Get average Binance latency
    pub fn avg_binance_latency_ms(&self) -> f64 {
        self.binance_recv_latency_ms.mean()
    }

    /// Get average Polymarket latency
    pub fn avg_pm_latency_ms(&self) -> f64 {
        self.pm_recv_latency_ms.mean()
    }

    /// Get average processing latency
    pub fn avg_processing_latency_ms(&self) -> f64 {
        self.processing_latency_ms.mean()
    }

    /// Get max Binance latency (for detecting spikes)
    pub fn max_binance_latency_ms(&self) -> f64 {
        self.binance_recv_latency_ms
            .iter()
            .fold(0.0_f64, |acc, &value| acc.max(value))
    }
}

/// Comprehensive data quality metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataQualityMetrics {
    /// Overall quality score (0-1)
    pub overall_score: f64,
    /// Binance data latency in ms
    pub binance_latency_ms: f64,
    /// Polymarket data latency in ms
    pub pm_latency_ms: f64,
    /// Processing latency in ms
    pub processing_latency_ms: f64,
    /// Staleness calculation confidence (0-1)
    pub staleness_confidence: f64,
    /// Data freshness score (0-1)
    pub freshness_score: f64,
    /// Whether price anomaly was detected
    pub price_anomaly_detected: bool,
    /// Z-score of last price (if anomaly detected)
    pub price_z_score: f64,
    /// Last valid timestamp
    pub last_valid_ts_ms: i64,
    /// Number of consecutive data errors
    pub consecutive_errors: u32,
    /// Whether data is currently healthy
    pub is_healthy: bool,
}

impl Default for DataQualityMetrics {
    fn default() -> Self {
        Self {
            overall_score: 1.0,
            binance_latency_ms: 0.0,
            pm_latency_ms: 0.0,
            processing_latency_ms: 0.0,
            staleness_confidence: 1.0,
            freshness_score: 1.0,
            price_anomaly_detected: false,
            price_z_score: 0.0,
            last_valid_ts_ms: 0,
            consecutive_errors: 0,
            is_healthy: true,
        }
    }
}

impl DataQualityMetrics {
    /// Calculate overall quality score
    pub fn calculate_score(&mut self) {
        // Latency penalties (exponential decay)
        let latency_score = if self.binance_latency_ms > 500.0 || self.pm_latency_ms > 500.0 {
            0.0 // Critical - data too stale
        } else {
            let binance_penalty = (-self.binance_latency_ms / 200.0).exp();
            let pm_penalty = (-self.pm_latency_ms / 200.0).exp();
            (binance_penalty + pm_penalty) / 2.0
        };

        // Freshness score based on staleness confidence
        let freshness = self.staleness_confidence * self.freshness_score;

        // Anomaly penalty
        let anomaly_penalty = if self.price_anomaly_detected {
            0.5
        } else {
            1.0
        };

        // Error penalty
        let error_penalty = (-(self.consecutive_errors as f64) / 5.0).exp();

        // Overall score (weighted combination)
        self.overall_score =
            (latency_score * 0.3 + freshness * 0.3 + anomaly_penalty * 0.2 + error_penalty * 0.2)
                .clamp(0.0, 1.0);

        // Health check
        self.is_healthy = self.overall_score > 0.3
            && !self.price_anomaly_detected
            && self.consecutive_errors < 10;
    }
}

/// Data stream logger for persistence and replay
pub struct DataStreamLogger {
    buffer: Vec<u8>,
    flush_threshold: usize,
    log_dir: std::path::PathBuf,
    current_file: Option<std::fs::File>,
    samples_written: u64,
}

impl DataStreamLogger {
    pub fn new(log_dir: &str, flush_threshold_kb: usize) -> std::io::Result<Self> {
        let path = std::path::PathBuf::from(log_dir);
        std::fs::create_dir_all(&path)?;

        Ok(Self {
            buffer: Vec::with_capacity(flush_threshold_kb * 1024),
            flush_threshold: flush_threshold_kb * 1024,
            log_dir: path,
            current_file: None,
            samples_written: 0,
        })
    }

    /// Log a data sample
    pub fn log_sample<T: Serialize>(&mut self, sample: &T) -> std::io::Result<()> {
        let json = serde_json::to_string(sample)?;
        self.buffer.extend_from_slice(json.as_bytes());
        self.buffer.push(b'\n');

        if self.buffer.len() >= self.flush_threshold {
            self.flush()?;
        }

        self.samples_written += 1;
        Ok(())
    }

    /// Flush buffer to disk
    pub fn flush(&mut self) -> std::io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Create new file if needed (rotate hourly)
        if self.current_file.is_none() {
            let filename = format!(
                "data_stream_{}.jsonl",
                chrono::Utc::now().format("%Y%m%d_%H")
            );
            let path = self.log_dir.join(filename);
            self.current_file = Some(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)?,
            );
        }

        if let Some(ref mut file) = self.current_file {
            use std::io::Write;
            file.write_all(&self.buffer)?;
            file.flush()?;
        }

        self.buffer.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_buffer() {
        let mut buf = RingBuffer::new(3);
        buf.push(1.0);
        buf.push(2.0);
        buf.push(3.0);
        assert_eq!(buf.len(), 3);

        buf.push(4.0); // Should overwrite 1.0
        assert_eq!(buf.len(), 3);

        let values: Vec<f64> = buf.iter().copied().collect();
        assert_eq!(values, vec![2.0, 3.0, 4.0]);
    }

    #[test]
    fn test_ema_smoother() {
        let mut ema = EmaSmoother::new(0.5);
        assert_eq!(ema.update(10.0), 10.0);
        assert_eq!(ema.update(20.0), 15.0); // 0.5*20 + 0.5*10
        assert_eq!(ema.update(30.0), 22.5); // 0.5*30 + 0.5*15
    }

    #[test]
    fn test_anomaly_detector() {
        let mut detector = AnomalyDetector::new(10, 3.0);

        // Feed normal values
        for i in 0..10 {
            let (is_anomaly, _) = detector.check(i as f64 * 1.0);
            assert!(!is_anomaly);
        }

        // Check normal value
        let (is_anomaly, z_score) = detector.check(10.0);
        assert!(
            !is_anomaly,
            "Normal value flagged as anomaly: z={}",
            z_score
        );

        // Check extreme outlier
        let (is_anomaly, z_score) = detector.check(100.0);
        assert!(is_anomaly, "Outlier not detected: z={}", z_score);
        assert!(z_score > 3.0);
    }
}
