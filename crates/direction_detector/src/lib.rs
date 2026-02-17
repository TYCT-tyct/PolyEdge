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
    /// Triple-confirm gate: minimum consecutive same-direction ticks.
    pub min_consecutive_ticks: u8,
    /// Triple-confirm gate: minimum absolute velocity in bps/s.
    pub min_velocity_bps_per_sec: f64,
    /// Triple-confirm gate: minimum directional acceleration.
    pub min_acceleration: f64,
    /// If abs(velocity) is above this multiple of min_velocity, treat as momentum spike.
    pub momentum_spike_multiplier: f64,
    /// Enable source vote gate: Binance confirmation is mandatory.
    pub enable_source_vote_gate: bool,
    /// Require Chainlink secondary source confirmation when available.
    pub require_secondary_confirmation: bool,
    /// Ignore source snapshots older than this window when voting.
    pub source_vote_max_age_ms: i64,
}

impl Default for DirectionConfig {
    fn default() -> Self {
        Self {
            window_max_sec: 120,
            threshold_5m_pct: 0.03,
            threshold_15m_pct: 0.05,
            threshold_1h_pct: 0.20,
            threshold_1d_pct: 0.50,
            lookback_short_sec: 15,
            lookback_long_sec: 60,
            min_sources_for_high_confidence: 2,
            min_ticks_for_signal: 5,
            min_consecutive_ticks: 2,
            min_velocity_bps_per_sec: 3.0,
            min_acceleration: 0.0,
            momentum_spike_multiplier: 1.8,
            enable_source_vote_gate: true,
            require_secondary_confirmation: true,
            source_vote_max_age_ms: 2_000,
        }
    }
}

#[derive(Debug, Default)]
struct SymbolWindow {
    // (recv_ts_ms, price)
    ticks: VecDeque<(i64, f64)>,
    latest_by_source: HashMap<String, f64>,
    latest_ts_by_source: HashMap<String, i64>,
}

#[derive(Debug)]
pub struct DirectionDetector {
    windows: HashMap<String, SymbolWindow>,
    cfg: DirectionConfig,
    /// Tick counter for lazy pruning - only prune every N ticks
    tick_counter: usize,
}

impl DirectionDetector {
    pub fn new(cfg: DirectionConfig) -> Self {
        Self {
            windows: HashMap::new(),
            cfg,
            tick_counter: 0,
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
            .or_default();

        w.ticks.push_back((tick.recv_ts_ms, tick.price));
        w.latest_by_source
            .insert(tick.source.clone(), tick.price);
        w.latest_ts_by_source
            .insert(tick.source.clone(), tick.recv_ts_ms);

        // Lazy pruning: only prune every 100 ticks to reduce CPU overhead
        self.tick_counter += 1;
        if self.tick_counter >= 100 {
            self.tick_counter = 0;
            let cutoff = tick
                .recv_ts_ms
                .saturating_sub((self.cfg.window_max_sec as i64).saturating_mul(1_000));
            while matches!(w.ticks.front(), Some((ts, _)) if *ts < cutoff) {
                w.ticks.pop_front();
            }
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
        let (velocity_bps_per_sec, acceleration, tick_consistency) =
            kinematics_from_ticks(&w.ticks);

        let mut raw_direction = if magnitude_pct > self.cfg.threshold_15m_pct {
            Direction::Up
        } else if magnitude_pct < -self.cfg.threshold_15m_pct {
            Direction::Down
        } else {
            Direction::Neutral
        };
        let velocity_direction = if velocity_bps_per_sec > 0.0 {
            Direction::Up
        } else if velocity_bps_per_sec < 0.0 {
            Direction::Down
        } else {
            Direction::Neutral
        };
        let velocity_spike_only = matches!(raw_direction, Direction::Neutral)
            && !matches!(velocity_direction, Direction::Neutral)
            && tick_consistency >= self.cfg.min_consecutive_ticks.max(1)
            && velocity_bps_per_sec.abs()
                >= self.cfg.min_velocity_bps_per_sec.max(0.0)
                    * self.cfg.momentum_spike_multiplier.max(1.0);
        if velocity_spike_only {
            raw_direction = velocity_direction;
        }
        let direction_sign = match raw_direction {
            Direction::Up => 1.0,
            Direction::Down => -1.0,
            Direction::Neutral => 0.0,
        };
        let velocity_abs = velocity_bps_per_sec.abs();
        let directional_acceleration = acceleration * direction_sign;
        let momentum_spike = velocity_abs
            >= self.cfg.min_velocity_bps_per_sec.max(0.0)
                * self.cfg.momentum_spike_multiplier.max(1.0);
        let triple_confirm = !matches!(raw_direction, Direction::Neutral)
            && tick_consistency >= self.cfg.min_consecutive_ticks.max(1)
            && velocity_abs >= self.cfg.min_velocity_bps_per_sec.max(0.0)
            && (directional_acceleration >= self.cfg.min_acceleration || momentum_spike);
        let vote = source_vote(
            &w.latest_by_source,
            &w.latest_ts_by_source,
            anchor_short,
            &raw_direction,
            now_ms,
            self.cfg.source_vote_max_age_ms,
        );
        let vote_passed = if !self.cfg.enable_source_vote_gate {
            true
        } else if matches!(raw_direction, Direction::Neutral) {
            true
        } else if self.cfg.require_secondary_confirmation {
            vote.binance_confirms && (!vote.secondary_available || vote.secondary_confirms)
        } else {
            vote.binance_confirms
        };
        let direction = if triple_confirm && vote_passed {
            raw_direction
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
            velocity_bps_per_sec,
            acceleration,
            tick_consistency,
            ts_ns: now_ms.max(0) * 1_000_000,
        })
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct SourceVote {
    binance_confirms: bool,
    secondary_available: bool,
    secondary_confirms: bool,
}

fn source_vote(
    latest_by_source: &HashMap<String, f64>,
    latest_ts_by_source: &HashMap<String, i64>,
    anchor_short: f64,
    direction: &Direction,
    now_ms: i64,
    max_age_ms: i64,
) -> SourceVote {
    if anchor_short <= 0.0 || matches!(direction, Direction::Neutral) {
        return SourceVote::default();
    }
    let mut out = SourceVote::default();
    let max_age_ms = max_age_ms.max(50);
    for (source, px) in latest_by_source {
        let Some(ts_ms) = latest_ts_by_source.get(source).copied() else {
            continue;
        };
        if now_ms.saturating_sub(ts_ms) > max_age_ms {
            continue;
        }
        let ret = (*px - anchor_short) / anchor_short;
        let confirms = match direction {
            Direction::Up => ret > 0.0,
            Direction::Down => ret < 0.0,
            Direction::Neutral => false,
        };
        if is_binance_source(source) {
            out.binance_confirms |= confirms;
        } else if is_chainlink_source(source) {
            out.secondary_available = true;
            out.secondary_confirms |= confirms;
        }
    }
    out
}

fn is_binance_source(source: &str) -> bool {
    source.to_ascii_lowercase().contains("binance")
}

fn is_chainlink_source(source: &str) -> bool {
    source.to_ascii_lowercase().contains("chainlink")
}

fn kinematics_from_ticks(ticks: &VecDeque<(i64, f64)>) -> (f64, f64, u8) {
    if ticks.len() < 3 {
        return (0.0, 0.0, 0);
    }
    let mut it = ticks.iter().rev();
    let (t2, p2) = *it.next().unwrap_or(&(0, 0.0));
    let (t1, p1) = *it.next().unwrap_or(&(0, 0.0));
    let (t0, p0) = *it.next().unwrap_or(&(0, 0.0));
    let v2 = velocity_bps_per_sec(t1, p1, t2, p2);
    let v1 = velocity_bps_per_sec(t0, p0, t1, p1);
    let dt_s = ((t2 - t1).max(1) as f64) / 1_000.0;
    let acceleration = (v2 - v1) / dt_s.max(1e-6);
    let consistency = consecutive_direction_count(ticks);
    (v2, acceleration, consistency)
}

fn velocity_bps_per_sec(t0_ms: i64, p0: f64, t1_ms: i64, p1: f64) -> f64 {
    if p0 <= 0.0 || p1 <= 0.0 {
        return 0.0;
    }
    let dt_ms = (t1_ms - t0_ms).max(1) as f64;
    let dt_s = dt_ms / 1_000.0;
    let ret = (p1 - p0) / p0;
    let v = (ret * 10_000.0) / dt_s.max(1e-6);
    if v.is_finite() {
        v
    } else {
        0.0
    }
}

fn consecutive_direction_count(ticks: &VecDeque<(i64, f64)>) -> u8 {
    if ticks.len() < 2 {
        return 0;
    }
    let mut count: u8 = 0;
    let mut prev_sign: i8 = 0;
    for win in ticks.iter().rev().zip(ticks.iter().rev().skip(1)) {
        let ((_, p_new), (_, p_old)) = win;
        if *p_old <= 0.0 {
            break;
        }
        let delta = p_new - p_old;
        let sign = if delta > 0.0 {
            1
        } else if delta < 0.0 {
            -1
        } else {
            0
        };
        if sign == 0 {
            break;
        }
        if prev_sign == 0 {
            prev_sign = sign;
            count = count.saturating_add(1);
            continue;
        }
        if sign == prev_sign {
            count = count.saturating_add(1);
        } else {
            break;
        }
    }
    count
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
    // Use a small threshold for Neutral instead of exact equality
    // Floating-point exact equality is nearly impossible, so we use 0.1% threshold
    const NEUTRAL_THRESHOLD: f64 = 0.001;
    for px in latest_by_source.values() {
        let ret = (*px - anchor_short) / anchor_short;
        let ok = match direction {
            Direction::Up => ret > 0.0,
            Direction::Down => ret < 0.0,
            Direction::Neutral => ret.abs() <= NEUTRAL_THRESHOLD,
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
            source_seq: 0,
            event_ts_exchange_ms: recv_ts_ms,
            recv_ts_local_ns: recv_ts_ms * 1_000_000,
            ingest_ts_local_ns: recv_ts_ms * 1_000_000,
            price,
        }
    }

    #[test]
    fn no_signal_when_cold_start() {
        let mut det = DirectionDetector::new(DirectionConfig {
            min_ticks_for_signal: 5,
            require_secondary_confirmation: false,
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
            min_consecutive_ticks: 1,
            min_velocity_bps_per_sec: 0.0,
            require_secondary_confirmation: false,
            ..DirectionConfig::default()
        });
        let now = 1_000_000i64;
        det.on_tick(&tick("binance", "BTCUSDT", now - 60_000, 100.0));
        det.on_tick(&tick("binance", "BTCUSDT", now - 15_000, 100.0));
        det.on_tick(&tick("binance", "BTCUSDT", now, 100.2));
        let sig = det.evaluate("BTCUSDT", now).unwrap();
        assert_eq!(sig.direction, Direction::Up);
        assert!(sig.magnitude_pct > 0.10);
        assert!(sig.velocity_bps_per_sec > 0.0);
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
            min_consecutive_ticks: 1,
            min_velocity_bps_per_sec: 0.0,
            require_secondary_confirmation: true,
            ..DirectionConfig::default()
        });
        let now = 1_000_000i64;
        det.on_tick(&tick("binance", "BTCUSDT", now - 60_000, 100.0));
        det.on_tick(&tick("chainlink_rtds", "BTCUSDT", now - 15_000, 100.0));
        det.on_tick(&tick("binance", "BTCUSDT", now, 100.2));
        det.on_tick(&tick("chainlink_rtds", "BTCUSDT", now, 100.21));
        let sig = det.evaluate("BTCUSDT", now).unwrap();
        assert_eq!(sig.direction, Direction::Up);
        assert!(sig.confidence >= 0.85);
    }

    #[test]
    fn triple_confirm_blocks_single_tick_fakeout() {
        let mut det = DirectionDetector::new(DirectionConfig {
            min_ticks_for_signal: 3,
            threshold_15m_pct: 0.10,
            min_consecutive_ticks: 2,
            min_velocity_bps_per_sec: 1.0,
            require_secondary_confirmation: false,
            ..DirectionConfig::default()
        });
        let now = 1_000_000i64;
        det.on_tick(&tick("binance", "BTCUSDT", now - 60_000, 100.0));
        det.on_tick(&tick("binance", "BTCUSDT", now - 15_000, 100.0));
        det.on_tick(&tick("binance", "BTCUSDT", now - 1_000, 100.2));
        det.on_tick(&tick("binance", "BTCUSDT", now, 100.1));
        let sig = det.evaluate("BTCUSDT", now).unwrap();
        assert_eq!(sig.direction, Direction::Neutral);
    }

    #[test]
    fn triple_confirm_passes_consistent_move() {
        let mut det = DirectionDetector::new(DirectionConfig {
            min_ticks_for_signal: 4,
            threshold_15m_pct: 0.05,
            min_consecutive_ticks: 2,
            min_velocity_bps_per_sec: 1.0,
            require_secondary_confirmation: false,
            ..DirectionConfig::default()
        });
        let now = 1_000_000i64;
        det.on_tick(&tick("binance", "BTCUSDT", now - 60_000, 100.0));
        det.on_tick(&tick("binance", "BTCUSDT", now - 2_000, 100.05));
        det.on_tick(&tick("binance", "BTCUSDT", now - 1_000, 100.10));
        det.on_tick(&tick("binance", "BTCUSDT", now, 100.20));
        let sig = det.evaluate("BTCUSDT", now).unwrap();
        assert_eq!(sig.direction, Direction::Up);
        assert!(sig.tick_consistency >= 2);
    }

    #[test]
    fn source_vote_blocks_when_binance_unconfirmed() {
        let mut det = DirectionDetector::new(DirectionConfig {
            min_ticks_for_signal: 3,
            min_consecutive_ticks: 1,
            min_velocity_bps_per_sec: 0.0,
            require_secondary_confirmation: true,
            ..DirectionConfig::default()
        });
        let now = 1_000_000i64;
        det.on_tick(&tick("binance_ws", "BTCUSDT", now - 60_000, 100.0));
        det.on_tick(&tick("chainlink_rtds", "BTCUSDT", now - 60_000, 100.0));
        det.on_tick(&tick("binance_ws", "BTCUSDT", now - 1_000, 99.9));
        det.on_tick(&tick("chainlink_rtds", "BTCUSDT", now, 100.2));
        let sig = det.evaluate("BTCUSDT", now).unwrap();
        assert_eq!(sig.direction, Direction::Neutral);
    }

    #[test]
    fn source_vote_passes_with_binance_plus_secondary() {
        let mut det = DirectionDetector::new(DirectionConfig {
            min_ticks_for_signal: 3,
            threshold_15m_pct: 0.10,
            min_consecutive_ticks: 1,
            min_velocity_bps_per_sec: 0.0,
            require_secondary_confirmation: true,
            ..DirectionConfig::default()
        });
        let now = 1_000_000i64;
        det.on_tick(&tick("binance_ws", "BTCUSDT", now - 60_000, 100.0));
        det.on_tick(&tick("chainlink_rtds", "BTCUSDT", now - 60_000, 100.0));
        det.on_tick(&tick("binance_ws", "BTCUSDT", now - 1_000, 100.2));
        det.on_tick(&tick("chainlink_rtds", "BTCUSDT", now, 100.21));
        let sig = det.evaluate("BTCUSDT", now).unwrap();
        assert_eq!(sig.direction, Direction::Up);
    }

    #[test]
    fn source_vote_accepts_chainlink_as_secondary() {
        let mut det = DirectionDetector::new(DirectionConfig {
            min_ticks_for_signal: 3,
            threshold_15m_pct: 0.10,
            min_consecutive_ticks: 1,
            min_velocity_bps_per_sec: 0.0,
            require_secondary_confirmation: true,
            ..DirectionConfig::default()
        });
        let now = 1_000_000i64;
        det.on_tick(&tick("binance_udp", "BTCUSDT", now - 60_000, 100.0));
        det.on_tick(&tick("chainlink_rtds", "BTCUSDT", now - 60_000, 100.0));
        det.on_tick(&tick("binance_udp", "BTCUSDT", now - 1_000, 100.2));
        det.on_tick(&tick("chainlink_rtds", "BTCUSDT", now, 100.21));
        let sig = det.evaluate("BTCUSDT", now).unwrap();
        assert_eq!(sig.direction, Direction::Up);
    }

    #[test]
    fn velocity_spike_overrides_magnitude_neutral() {
        let mut det = DirectionDetector::new(DirectionConfig {
            min_ticks_for_signal: 4,
            threshold_15m_pct: 0.10,
            min_consecutive_ticks: 2,
            min_velocity_bps_per_sec: 5.0,
            momentum_spike_multiplier: 1.8,
            require_secondary_confirmation: false,
            ..DirectionConfig::default()
        });
        let now = 1_000_000i64;
        det.on_tick(&tick("binance_ws", "BTCUSDT", now - 60_000, 100.0));
        det.on_tick(&tick("binance_ws", "BTCUSDT", now - 1_000, 100.00));
        det.on_tick(&tick("binance_ws", "BTCUSDT", now - 800, 100.04));
        det.on_tick(&tick("binance_ws", "BTCUSDT", now - 600, 100.08));
        let sig = det.evaluate("BTCUSDT", now).unwrap();
        assert_eq!(sig.direction, Direction::Up);
        assert!(sig.magnitude_pct < 0.10);
    }
}
