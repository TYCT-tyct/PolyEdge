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
    /// Triple-confirm gate: minimum short/long tick-rate ratio treated as volume spike proxy.
    pub min_tick_rate_spike_ratio: f64,
    /// Window for short-horizon tick-rate in ms.
    pub tick_rate_short_ms: i64,
    /// Window for long-horizon tick-rate in ms.
    pub tick_rate_long_ms: i64,
    /// Enable source vote gate: Binance confirmation is mandatory.
    pub enable_source_vote_gate: bool,
    /// Require Chainlink secondary source confirmation when available.
    pub require_secondary_confirmation: bool,
    /// Ignore source snapshots older than this window when voting.
    pub source_vote_max_age_ms: i64,
    /// 快速确认速度阈值 (bps/s)。
    /// 当 velocity 超过此值时，只需 1 个同向 Tick 即可触发（不等 min_consecutive_ticks）。
    /// 设计哲学: 极强动量本身就是高置信度信号，等待第 2 个 Tick 会损失 10-50ms 窗口。
    pub fast_confirm_velocity_bps_per_sec: f64,
}

impl Default for DirectionConfig {
    fn default() -> Self {
        Self {
            window_max_sec: 120,
            // 5min 市场波动更小，阈值更低才能捕获信号
            threshold_5m_pct: 0.02,
            threshold_15m_pct: 0.05,
            threshold_1h_pct: 0.20,
            threshold_1d_pct: 0.50,
            lookback_short_sec: 15,
            lookback_long_sec: 60,
            min_sources_for_high_confidence: 2,
            min_ticks_for_signal: 3,
            min_consecutive_ticks: 2,
            min_velocity_bps_per_sec: 3.0,
            min_acceleration: 0.0,
            momentum_spike_multiplier: 1.8,
            min_tick_rate_spike_ratio: 1.8,
            tick_rate_short_ms: 300,
            tick_rate_long_ms: 3_000,
            enable_source_vote_gate: true,
            require_secondary_confirmation: true,
            source_vote_max_age_ms: 2_000,
            // 极强动量: velocity > 30 bps/s → 单 Tick 触发，不等第 2 个
            fast_confirm_velocity_bps_per_sec: 30.0,
        }
    }
}

// ============================================================
// SymbolWindow — 每个 symbol 的滑动窗口状态
// 设计原则: on_tick 时增量维护，evaluate 时 O(1) 读取缓存
// 消除 evaluate 路径上的所有 O(n) 扫描
// ============================================================
#[derive(Debug, Default)]
struct SymbolWindow {
    // (recv_ts_ms, price) — 按时间戳升序
    ticks: VecDeque<(i64, f64)>,
    // 各数据源最新价格和时间戳
    latest_by_source: HashMap<String, f64>,
    latest_ts_by_source: HashMap<String, i64>,

    // --------------------------------------------------------
    // 增量维护的滑动窗口计数器 (on_tick O(1) 更新)
    // 消除 evaluate 里两次 O(n) 全量扫描
    // --------------------------------------------------------
    /// 最近 tick_rate_short_ms 内的 tick 数
    short_window_count: u32,
    /// 最近 tick_rate_long_ms 内的 tick 数
    long_window_count: u32,
    /// 连续同向 tick 计数 (增量维护)
    consecutive_dir_count: u8,
    /// 最后一个 tick 的方向符号: -1/0/1
    last_dir_sign: i8,

    // --------------------------------------------------------
    // 源类型缓存 — 避免每次 is_binance/chainlink 分配 String
    // --------------------------------------------------------
    /// 已见过的 Binance 源 key (第一次见到时缓存)
    binance_source_key: Option<String>,
    /// 已见过的 Chainlink 源 key (第一次见到时缓存)
    chainlink_source_key: Option<String>,
}

#[derive(Debug)]
pub struct DirectionDetector {
    windows: HashMap<String, SymbolWindow>,
    cfg: DirectionConfig,
    /// Tick counter for lazy pruning - only prune every N ticks
    tick_counter: usize,
    /// Cached tick_rate_short_ms as i64 for hot-path use
    cfg_tick_rate_short_ms: i64,
    /// Cached tick_rate_long_ms as i64 for hot-path use
    cfg_tick_rate_long_ms: i64,
}

impl DirectionDetector {
    pub fn new(cfg: DirectionConfig) -> Self {
        let short = cfg.tick_rate_short_ms;
        let long = cfg.tick_rate_long_ms;
        Self {
            windows: HashMap::new(),
            cfg,
            tick_counter: 0,
            cfg_tick_rate_short_ms: short,
            cfg_tick_rate_long_ms: long,
        }
    }

    pub fn cfg(&self) -> &DirectionConfig {
        &self.cfg
    }

    pub fn set_cfg(&mut self, cfg: DirectionConfig) {
        self.cfg_tick_rate_short_ms = cfg.tick_rate_short_ms;
        self.cfg_tick_rate_long_ms = cfg.tick_rate_long_ms;
        self.cfg = cfg;
        // Keep windows; pruning will happen on next tick/evaluate.
    }

    pub fn on_tick(&mut self, tick: &RefTick) {
        let w = self.windows.entry(tick.symbol.clone()).or_default();

        // --------------------------------------------------------
        // 源类型缓存: 第一次见到 binance/chainlink 源时记录 key
        // 后续 source_vote 直接比较 key，不再 to_ascii_lowercase()
        // --------------------------------------------------------
        if w.binance_source_key.is_none() && is_binance_source(&tick.source) {
            w.binance_source_key = Some(tick.source.to_string());
        }
        if w.chainlink_source_key.is_none() && is_chainlink_source(&tick.source) {
            w.chainlink_source_key = Some(tick.source.to_string());
        }

        w.latest_by_source
            .insert(tick.source.to_string(), tick.price);
        w.latest_ts_by_source
            .insert(tick.source.to_string(), tick.recv_ts_ms);

        // --------------------------------------------------------
        // 增量维护连续方向计数
        // 只看最新 tick 与上一个 tick 的方向关系，O(1)
        // --------------------------------------------------------
        if let Some(&(_, prev_price)) = w.ticks.back() {
            if prev_price > 0.0 && tick.price > 0.0 {
                let delta = tick.price - prev_price;
                let sign: i8 = if delta > 0.0 {
                    1
                } else if delta < 0.0 {
                    -1
                } else {
                    0
                };
                if sign == 0 {
                    w.consecutive_dir_count = 0;
                    w.last_dir_sign = 0;
                } else if sign == w.last_dir_sign {
                    w.consecutive_dir_count = w.consecutive_dir_count.saturating_add(1);
                } else {
                    w.consecutive_dir_count = 1;
                    w.last_dir_sign = sign;
                }
            }
        }

        w.ticks.push_back((tick.recv_ts_ms, tick.price));

        // --------------------------------------------------------
        // 增量维护滑动窗口计数器
        // 新 tick 进来时: short/long count +1
        // 过期 tick 弹出时: 相应 count -1
        // --------------------------------------------------------
        let short_ms = self.cfg_tick_rate_short_ms;
        let long_ms = self.cfg_tick_rate_long_ms;
        let now = tick.recv_ts_ms;
        w.short_window_count += 1;
        w.long_window_count += 1;

        // 懒惰剪枝: 每 100 ticks 清理过期数据，同时修正计数器
        self.tick_counter += 1;
        if self.tick_counter >= 100 {
            self.tick_counter = 0;
            let window_cutoff =
                now.saturating_sub((self.cfg.window_max_sec as i64).saturating_mul(1_000));
            let short_cutoff = now.saturating_sub(short_ms);
            let long_cutoff = now.saturating_sub(long_ms);
            // 重新精确计算计数器（每 100 ticks 一次，分摊成本）
            w.short_window_count =
                w.ticks.iter().filter(|(ts, _)| *ts >= short_cutoff).count() as u32;
            w.long_window_count =
                w.ticks.iter().filter(|(ts, _)| *ts >= long_cutoff).count() as u32;
            while matches!(w.ticks.front(), Some((ts, _)) if *ts < window_cutoff) {
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
        // kinematics_from_ticks \u73b0\u5728\u53ea\u8fd4\u56de (velocity, acceleration)
        // tick_consistency \u7531 SymbolWindow \u589e\u91cf\u7ef4\u62a4\uff0cO(1) \u8bfb\u53d6
        let (velocity_bps_per_sec, acceleration) = kinematics_from_ticks(&w.ticks);
        let tick_consistency = w.consecutive_dir_count;

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
        // --------------------------------------------------------
        // volume_spike: 直接用缓存的滑动窗口计数器，O(1) 读取
        // 替代原来的 tick_rate_spike_ratio() O(2n) 全量扫描
        // --------------------------------------------------------
        let short_ms = self.cfg_tick_rate_short_ms.clamp(50, 10_000);
        let long_ms = self
            .cfg_tick_rate_long_ms
            .max(short_ms + 50)
            .clamp(100, 60_000);
        let short_rate = w.short_window_count as f64 / (short_ms as f64 / 1_000.0);
        let long_rate = (w.long_window_count as f64 / (long_ms as f64 / 1_000.0)).max(1e-6);
        let tick_rate_ratio = short_rate / long_rate;
        let volume_spike = tick_rate_ratio >= self.cfg.min_tick_rate_spike_ratio.max(1.0);
        // --------------------------------------------------------
        // 速度分级快速确认:
        //   极强动量 (velocity > fast_confirm_threshold) → 单 Tick 即可触发
        //   普通动量 → 需要 min_consecutive_ticks 个同向 Tick
        // 设计哲学: 极强动量本身是高置信度，等待第 2 个 Tick 会损失套利窗口
        // --------------------------------------------------------
        let required_ticks = if velocity_abs >= self.cfg.fast_confirm_velocity_bps_per_sec.max(1.0)
        {
            1u8
        } else {
            self.cfg.min_consecutive_ticks.max(1)
        };
        // 直接读缓存的 tick_consistency，不再调用 consecutive_direction_count()
        let tick_consistency = w.consecutive_dir_count;
        let triple_confirm = !matches!(raw_direction, Direction::Neutral)
            && tick_consistency >= required_ticks
            && velocity_abs >= self.cfg.min_velocity_bps_per_sec.max(0.0)
            && (directional_acceleration >= self.cfg.min_acceleration
                || momentum_spike
                || volume_spike);
        let vote = source_vote_cached(
            &w.latest_by_source,
            &w.latest_ts_by_source,
            w.binance_source_key.as_deref(),
            w.chainlink_source_key.as_deref(),
            anchor_short,
            &raw_direction,
            now_ms,
            self.cfg.source_vote_max_age_ms,
        );
        // 门控关闭 或 方向中性 → 直接放行；否则检查数据源确认
        let vote_passed = !self.cfg.enable_source_vote_gate
            || matches!(raw_direction, Direction::Neutral)
            || if self.cfg.require_secondary_confirmation {
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
            triple_confirm,
            momentum_spike,
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

// ============================================================
// source_vote_cached — 利用缓存的源 key 避免热路径 String 分配
// 替代原来的 source_vote() 里每次 is_binance/chainlink_source() 的 alloc
// ============================================================
fn source_vote_cached(
    latest_by_source: &HashMap<String, f64>,
    latest_ts_by_source: &HashMap<String, i64>,
    binance_key: Option<&str>,
    chainlink_key: Option<&str>,
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

    // 检查 Binance 源确认
    if let Some(key) = binance_key {
        if let (Some(&px), Some(&ts)) = (latest_by_source.get(key), latest_ts_by_source.get(key)) {
            if now_ms.saturating_sub(ts) <= max_age_ms {
                let ret = (px - anchor_short) / anchor_short;
                out.binance_confirms = match direction {
                    Direction::Up => ret > 0.0,
                    Direction::Down => ret < 0.0,
                    Direction::Neutral => false,
                };
            }
        }
    }

    // 检查 Chainlink 源确认
    if let Some(key) = chainlink_key {
        if let (Some(&px), Some(&ts)) = (latest_by_source.get(key), latest_ts_by_source.get(key)) {
            out.secondary_available = true;
            if now_ms.saturating_sub(ts) <= max_age_ms {
                let ret = (px - anchor_short) / anchor_short;
                out.secondary_confirms = match direction {
                    Direction::Up => ret > 0.0,
                    Direction::Down => ret < 0.0,
                    Direction::Neutral => false,
                };
            }
        }
    }

    out
}

// ============================================================
// is_binance/chainlink_source — 无分配版本
// 原来的 to_ascii_lowercase() 每次分配新 String
// 现在用 bytes 比较，零分配
// ============================================================
#[inline]
fn is_binance_source(source: &str) -> bool {
    let b = source.as_bytes();
    b.windows(7).any(|w| w.eq_ignore_ascii_case(b"binance"))
}

#[inline]
fn is_chainlink_source(source: &str) -> bool {
    let b = source.as_bytes();
    b.windows(9).any(|w| w.eq_ignore_ascii_case(b"chainlink"))
}

// ============================================================
// kinematics_from_ticks — 计算速度和加速度
// 注意: tick_consistency 现在由 SymbolWindow 增量维护
// 这里只返回 (velocity, acceleration)，不再调用 consecutive_direction_count()
// ============================================================
fn kinematics_from_ticks(ticks: &VecDeque<(i64, f64)>) -> (f64, f64) {
    if ticks.len() < 3 {
        return (0.0, 0.0);
    }
    let mut it = ticks.iter().rev();
    let (t2, p2) = *it.next().unwrap_or(&(0, 0.0));
    let (t1, p1) = *it.next().unwrap_or(&(0, 0.0));
    let (t0, p0) = *it.next().unwrap_or(&(0, 0.0));
    let v2 = velocity_bps_per_sec(t1, p1, t2, p2);
    let v1 = velocity_bps_per_sec(t0, p0, t1, p1);
    let dt_s = ((t2 - t1).max(1) as f64) / 1_000.0;
    let acceleration = (v2 - v1) / dt_s.max(1e-6);
    (v2, acceleration)
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

// ============================================================
// find_anchor_price — 二分查找 O(log n)
// deque 按时间戳升序，partition_point 找到第一个 ts > target_ms 的位置
// 取其前一个元素，即最新的 ts <= target_ms 的价格
// ============================================================
#[inline]
fn find_anchor_price(ticks: &VecDeque<(i64, f64)>, target_ms: i64) -> Option<f64> {
    // partition_point: 返回第一个不满足条件的索引
    // 条件: ts <= target_ms，所以返回的是第一个 ts > target_ms 的位置
    let idx = ticks.partition_point(|(ts, _)| *ts <= target_ms);
    if idx == 0 {
        return None; // 所有 tick 都在 target_ms 之后
    }
    ticks.get(idx - 1).map(|(_, px)| *px)
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
    if latest_by_source.is_empty() || anchor_short <= 0.0 {
        return 0.0;
    }
    let mut consistent = 0usize;
    let mut total = 0usize;
    // Use a small threshold for Neutral instead of exact equality
    // Floating-point exact equality is nearly impossible, so we use 0.1% threshold
    const NEUTRAL_THRESHOLD: f64 = 0.001;
    for (source, px) in latest_by_source {
        if is_chainlink_source(source) {
            continue;
        }
        total += 1;
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
    if total == 0 {
        total = latest_by_source.len();
        consistent = 0;
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
            ts_first_hop_ms: None,
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

    #[test]
    fn volume_spike_passes_triple_confirm_even_with_low_acceleration() {
        let mut det = DirectionDetector::new(DirectionConfig {
            min_ticks_for_signal: 4,
            threshold_15m_pct: 0.05,
            lookback_short_sec: 1,
            lookback_long_sec: 3,
            min_consecutive_ticks: 2,
            min_velocity_bps_per_sec: 0.5,
            min_acceleration: 10_000.0,
            min_tick_rate_spike_ratio: 2.0,
            tick_rate_short_ms: 300,
            tick_rate_long_ms: 3_000,
            require_secondary_confirmation: false,
            ..DirectionConfig::default()
        });
        let now = 2_000_000i64;
        det.on_tick(&tick("binance_ws", "BTCUSDT", now - 4_000, 100.0));
        det.on_tick(&tick("binance_ws", "BTCUSDT", now - 2_000, 100.02));
        det.on_tick(&tick("binance_ws", "BTCUSDT", now - 200, 100.07));
        det.on_tick(&tick("binance_ws", "BTCUSDT", now - 120, 100.12));
        det.on_tick(&tick("binance_ws", "BTCUSDT", now - 40, 100.17));
        let sig = det.evaluate("BTCUSDT", now).unwrap();
        assert_eq!(sig.direction, Direction::Up);
        assert!(sig.tick_consistency >= 2);
    }
}
