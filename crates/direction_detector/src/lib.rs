use std::collections::{HashMap, VecDeque};

use core_types::{Direction, DirectionSignal, RefTick, TimeframeClass};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DirectionConfig {
    /// Maximum window length for stored ticks (seconds).
    pub window_max_sec: u64,
    /// Thresholds in percent (e.g. 0.10 means 0.10%).
    pub threshold_5m_pct: f64,
    pub threshold_15m_pct: f64,
    pub threshold_1h_pct: f64,
    pub threshold_1d_pct: f64,
    /// Lookback windows (seconds).
    pub lookback_short_sec: u64,
    pub lookback_long_sec: u64,
    /// Multi-source consistency.
    pub min_sources_for_high_confidence: usize,
    /// Cold-start guard.
    pub min_ticks_for_signal: usize,
}

impl Default for DirectionConfig {
    fn default() -> Self {
        Self {
            window_max_sec: 120,
            threshold_5m_pct: 0.05,
            threshold_15m_pct: 0.10,
            threshold_1h_pct: 0.20,
            threshold_1d_pct: 0.50,
            lookback_short_sec: 15,
            lookback_long_sec: 60,
            min_sources_for_high_confidence: 2,
            min_ticks_for_signal: 5,
        }
    }
}

#[derive(Debug, Default)]
struct SymbolWindow {
    // (recv_ts_ms, price)
    ticks: VecDeque<(i64, f64)>,
    latest_by_source: HashMap<String, f64>,
}

#[derive(Debug)]
pub struct DirectionDetector {
    windows: HashMap<String, SymbolWindow>,
    cfg: DirectionConfig,
}

impl DirectionDetector {
    pub fn new(cfg: DirectionConfig) -> Self {
        Self {
            windows: HashMap::new(),
            cfg,
        }
    }

    pub fn cfg(&self) -> &DirectionConfig {
        &self.cfg
    }

    pub fn set_cfg(&mut self, cfg: DirectionConfig) {
        self.cfg = cfg;
        // Keep windows; pruning will happen on next tick/evaluate.
    }

    pub fn on_tick(&mut self, tick: &RefTick) {
        let w = self
            .windows
            .entry(tick.symbol.clone())
            .or_insert_with(SymbolWindow::default);

        w.ticks.push_back((tick.recv_ts_ms, tick.price));
        w.latest_by_source
            .insert(tick.source.clone(), tick.price);

        let cutoff = tick
            .recv_ts_ms
            .saturating_sub((self.cfg.window_max_sec as i64).saturating_mul(1_000));
        while matches!(w.ticks.front(), Some((ts, _)) if *ts < cutoff) {
            w.ticks.pop_front();
        }
    }

    pub fn evaluate(&self, symbol: &str, now_ms: i64) -> Option<DirectionSignal> {
        let w = self.windows.get(symbol)?;
        if w.ticks.len() < self.cfg.min_ticks_for_signal {
            return None;
        }

        let latest = w.ticks.back().map(|(_, px)| *px)?;
        if latest <= 0.0 {
            return None;
        }

        let anchor_short_ms =
            now_ms.saturating_sub((self.cfg.lookback_short_sec as i64).saturating_mul(1_000));
        let anchor_long_ms =
            now_ms.saturating_sub((self.cfg.lookback_long_sec as i64).saturating_mul(1_000));
        let anchor_short = find_anchor_price(&w.ticks, anchor_short_ms)?;
        let anchor_long = find_anchor_price(&w.ticks, anchor_long_ms)?;
        if anchor_short <= 0.0 || anchor_long <= 0.0 {
            return None;
        }

        let ret_short = (latest - anchor_short) / anchor_short;
        let ret_long = (latest - anchor_long) / anchor_long;
        let magnitude_pct = (ret_short * 0.7 + ret_long * 0.3) * 100.0;

        let direction = if magnitude_pct > self.cfg.threshold_15m_pct {
            Direction::Up
        } else if magnitude_pct < -self.cfg.threshold_15m_pct {
            Direction::Down
        } else {
            Direction::Neutral
        };

        let recommended_tf = recommended_timeframe(&self.cfg, magnitude_pct.abs());

        let confidence = compute_confidence(
            &w.latest_by_source,
            anchor_short,
            &direction,
            self.cfg.min_sources_for_high_confidence,
        );

        Some(DirectionSignal {
            symbol: symbol.to_string(),
            direction,
            magnitude_pct,
            confidence,
            recommended_tf,
            ts_ns: (now_ms.max(0) as i64) * 1_000_000,
        })
    }
}

fn find_anchor_price(ticks: &VecDeque<(i64, f64)>, target_ms: i64) -> Option<f64> {
    // Prefer the newest price at-or-before target_ms. This is stable under sparse sampling.
    for (ts, px) in ticks.iter().rev() {
        if *ts <= target_ms {
            return Some(*px);
        }
    }
    None
}

fn recommended_timeframe(cfg: &DirectionConfig, abs_magnitude_pct: f64) -> TimeframeClass {
    if abs_magnitude_pct >= cfg.threshold_1d_pct {
        TimeframeClass::Tf1d
    } else if abs_magnitude_pct >= cfg.threshold_1h_pct {
        TimeframeClass::Tf1h
    } else if abs_magnitude_pct >= cfg.threshold_15m_pct {
        TimeframeClass::Tf15m
    } else {
        TimeframeClass::Tf5m
    }
}

fn compute_confidence(
    latest_by_source: &HashMap<String, f64>,
    anchor_short: f64,
    direction: &Direction,
    min_sources_for_high_confidence: usize,
) -> f64 {
    let total = latest_by_source.len();
    if total == 0 || anchor_short <= 0.0 {
        return 0.0;
    }
    let mut consistent = 0usize;
    for px in latest_by_source.values() {
        let ret = (*px - anchor_short) / anchor_short;
        let ok = match direction {
            Direction::Up => ret > 0.0,
            Direction::Down => ret < 0.0,
            Direction::Neutral => ret.abs() <= 0.0,
        };
        if ok {
            consistent += 1;
        }
    }
    let mut c = (consistent as f64 / total as f64).clamp(0.0, 1.0);
    if consistent >= min_sources_for_high_confidence {
        c = c.max(0.85);
    }
    c
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tick(source: &str, symbol: &str, recv_ts_ms: i64, price: f64) -> RefTick {
        RefTick {
            source: source.to_string(),
            symbol: symbol.to_string(),
            event_ts_ms: recv_ts_ms,
            recv_ts_ms,
            event_ts_exchange_ms: recv_ts_ms,
            recv_ts_local_ns: recv_ts_ms * 1_000_000,
            price,
        }
    }

    #[test]
    fn no_signal_when_cold_start() {
        let mut det = DirectionDetector::new(DirectionConfig {
            min_ticks_for_signal: 5,
            ..DirectionConfig::default()
        });
        let now = 1_000_000i64;
        det.on_tick(&tick("binance", "BTCUSDT", now - 60_000, 100.0));
        det.on_tick(&tick("binance", "BTCUSDT", now - 15_000, 100.0));
        det.on_tick(&tick("binance", "BTCUSDT", now, 100.2));
        assert!(det.evaluate("BTCUSDT", now).is_none());
    }

    #[test]
    fn up_direction_when_move_exceeds_threshold() {
        let mut det = DirectionDetector::new(DirectionConfig {
            min_ticks_for_signal: 3,
            threshold_15m_pct: 0.10,
            ..DirectionConfig::default()
        });
        let now = 1_000_000i64;
        det.on_tick(&tick("binance", "BTCUSDT", now - 60_000, 100.0));
        det.on_tick(&tick("binance", "BTCUSDT", now - 15_000, 100.0));
        det.on_tick(&tick("binance", "BTCUSDT", now, 100.2));
        let sig = det.evaluate("BTCUSDT", now).unwrap();
        assert_eq!(sig.direction, Direction::Up);
        assert!(sig.magnitude_pct > 0.10);
    }

    #[test]
    fn recommended_timeframe_mapping() {
        let cfg = DirectionConfig::default();
        assert_eq!(recommended_timeframe(&cfg, 0.01), TimeframeClass::Tf5m);
        assert_eq!(recommended_timeframe(&cfg, 0.10), TimeframeClass::Tf15m);
        assert_eq!(recommended_timeframe(&cfg, 0.25), TimeframeClass::Tf1h);
        assert_eq!(recommended_timeframe(&cfg, 0.60), TimeframeClass::Tf1d);
    }

    #[test]
    fn multi_source_high_confidence_floor() {
        let mut det = DirectionDetector::new(DirectionConfig {
            min_ticks_for_signal: 3,
            min_sources_for_high_confidence: 2,
            ..DirectionConfig::default()
        });
        let now = 1_000_000i64;
        det.on_tick(&tick("binance", "BTCUSDT", now - 60_000, 100.0));
        det.on_tick(&tick("coinbase", "BTCUSDT", now - 15_000, 100.0));
        det.on_tick(&tick("binance", "BTCUSDT", now, 100.2));
        det.on_tick(&tick("coinbase", "BTCUSDT", now, 100.21));
        let sig = det.evaluate("BTCUSDT", now).unwrap();
        assert_eq!(sig.direction, Direction::Up);
        assert!(sig.confidence >= 0.85);
    }
}

