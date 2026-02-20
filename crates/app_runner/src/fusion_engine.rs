use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Instant;

use core_types::{BookTop, RefTick};
use dashmap::DashMap;

use crate::state::FusionConfig;
use crate::stats_utils::now_ns;

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct FeedLatencySample {
    pub(super) feed_in_ms: f64,
    pub(super) source_latency_ms: f64,
    pub(super) exchange_lag_ms: f64,
    pub(super) path_lag_ms: f64,
    pub(super) book_latency_ms: f64,
    pub(super) local_backlog_ms: f64,
    pub(super) ref_decode_ms: f64,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct PathLagCalibState {
    floor_ms: f64,
    initialized: bool,
    updated_ns: i64,
}

pub(super) static PATH_LAG_CALIB: OnceLock<DashMap<String, PathLagCalibState>> = OnceLock::new();

pub(super) fn path_lag_calib_map() -> &'static DashMap<String, PathLagCalibState> {
    PATH_LAG_CALIB.get_or_init(DashMap::new)
}

pub(super) fn calibrate_path_lag_ms(source: &str, observed_delta_ms: f64, now_ns: i64) -> f64 {
    const FLOOR_RISE_MS_PER_SEC: f64 = 0.20;
    const FLOOR_CLAMP_MIN_MS: f64 = -10_000.0;
    const FLOOR_CLAMP_MAX_MS: f64 = 10_000.0;
    let key = source.to_string();
    let mut state = path_lag_calib_map()
        .get(&key)
        .map(|v| *v)
        .unwrap_or_default();
    if !state.initialized {
        state.floor_ms = observed_delta_ms;
        state.initialized = true;
        state.updated_ns = now_ns;
    } else {
        let dt_sec = ((now_ns - state.updated_ns).max(0) as f64) / 1_000_000_000.0;
        let mut floor = state.floor_ms + dt_sec * FLOOR_RISE_MS_PER_SEC;
        if observed_delta_ms < floor {
            floor = observed_delta_ms;
        }
        state.floor_ms = floor.clamp(FLOOR_CLAMP_MIN_MS, FLOOR_CLAMP_MAX_MS);
        state.updated_ns = now_ns;
    }
    path_lag_calib_map().insert(key, state);
    (observed_delta_ms - state.floor_ms).max(0.0)
}

#[cfg(test)]
pub(super) fn reset_path_lag_calib_for_tests() {
    if let Some(map) = PATH_LAG_CALIB.get() {
        map.clear();
    }
}

#[derive(Debug, Clone)]
pub(super) struct TokenBucket {
    pub(super) rps: f64,
    pub(super) burst: f64,
    pub(super) tokens: f64,
    pub(super) last_refill: Instant,
}

impl TokenBucket {
    pub(super) fn new(rps: f64, burst: f64) -> Self {
        let rps = rps.max(0.1);
        let burst = burst.max(1.0);
        Self {
            rps,
            burst,
            tokens: burst,
            last_refill: Instant::now(),
        }
    }

    pub(super) fn try_take(&mut self, n: f64) -> bool {
        let now = Instant::now();
        let dt = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;
        self.tokens = (self.tokens + dt * self.rps).clamp(0.0, self.burst);
        if self.tokens >= n {
            self.tokens -= n;
            true
        } else {
            false
        }
    }
}

pub(super) fn estimate_feed_latency(tick: &RefTick, book: &BookTop) -> FeedLatencySample {
    let now = now_ns();
    let now_ms = now / 1_000_000;
    let tick_source_ms = tick.event_ts_exchange_ms.max(tick.event_ts_ms);
    let tick_ingest_ms = (tick.recv_ts_ms - tick_source_ms).max(0) as f64;
    let exchange_lag_ms = tick
        .ts_first_hop_ms
        .map(|first| (first - tick_source_ms).max(0) as f64)
        .unwrap_or(tick_ingest_ms);
    let path_lag_ms = tick
        .ts_first_hop_ms
        .map(|first| calibrate_path_lag_ms(&tick.source, (tick.recv_ts_ms - first) as f64, now))
        .unwrap_or(f64::NAN);
    let book_recv_ms = if book.recv_ts_local_ns > 0 {
        (book.recv_ts_local_ns / 1_000_000).max(0)
    } else {
        now_ms
    };
    let book_ingest_ms = (book_recv_ms - book.ts_ms).max(0) as f64;
    // IMPORTANT: `source_latency_ms` is the external *reference* (CEX) tick latency proxy.
    // Do not mix in Polymarket book timestamps here; track book latency separately.
    let source_latency_ms = tick_ingest_ms;
    let ref_decode_ms = if tick.recv_ts_local_ns > 0 && tick.ingest_ts_local_ns > 0 {
        ((tick.ingest_ts_local_ns - tick.recv_ts_local_ns).max(0) as f64) / 1_000_000.0
    } else {
        0.0
    };

    let latest_recv_ns = if book.recv_ts_local_ns > 0 {
        tick.recv_ts_local_ns.max(book.recv_ts_local_ns)
    } else {
        tick.recv_ts_local_ns
    };
    let local_backlog_ms = if latest_recv_ns > 0 {
        ((now - latest_recv_ns).max(0) as f64) / 1_000_000.0
    } else {
        (now_ms - tick.recv_ts_ms.max(book.ts_ms)).max(0) as f64
    };

    // IMPORTANT: `feed_in_ms` is the *local* recv->ingest latency (decode + enqueue), intended
    // to be <5ms and independent of exchange clock skew. Use source_latency_ms/local_backlog_ms
    // separately for "how old is this data?" debugging/guardrails.
    let feed_in_ms = ref_decode_ms;
    FeedLatencySample {
        feed_in_ms,
        source_latency_ms,
        exchange_lag_ms,
        path_lag_ms,
        book_latency_ms: book_ingest_ms,
        local_backlog_ms,
        ref_decode_ms,
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CoalescePolicy {
    pub(super) max_events: usize,
    pub(super) budget_us: u64,
}

pub(super) fn compute_coalesce_policy(
    queue_len: usize,
    lag_p50_ms: Option<f64>,
    strategy_max_coalesce: usize,
    strategy_coalesce_min: usize,
    strategy_coalesce_budget_us: u64,
) -> CoalescePolicy {
    // For shallow queues, process immediately to minimize tail latency.
    if queue_len < 12 {
        return CoalescePolicy {
            max_events: 0,
            budget_us: 0,
        };
    }

    // If queue is already deep, prioritize drain speed to collapse backlog spikes quickly.
    if queue_len >= 128 {
        let max_events = queue_len
            .min(strategy_max_coalesce)
            .max(strategy_coalesce_min);
        let budget_us = strategy_coalesce_budget_us.max(800).min(3_000);
        return CoalescePolicy {
            max_events,
            budget_us,
        };
    }

    let dynamic_cap = queue_len
        .saturating_div(2)
        .saturating_add(8)
        .min(strategy_max_coalesce);
    let max_events = dynamic_cap.max(strategy_coalesce_min);
    let budget_us = match lag_p50_ms {
        Some(l) if l >= 80.0 => 220_u64,
        Some(l) if l >= 40.0 => strategy_coalesce_budget_us.min(160),
        Some(l) if l >= 15.0 => 80_u64,
        Some(_) => 40_u64,
        None => strategy_coalesce_budget_us.min(120),
    };

    CoalescePolicy {
        max_events,
        budget_us,
    }
}

pub(super) fn is_anchor_ref_source(source: &str) -> bool {
    source == "chainlink_rtds"
}

#[inline]
pub(super) fn fast_tick_allowed_in_fusion_mode(source: &str, mode: &str) -> bool {
    match mode {
        "udp_only" => source == "binance_udp",
        "direct_only" => source != "binance_udp" && !is_anchor_ref_source(source),
        "active_active" | "websocket_primary" => true,
        // Unknown future modes: keep permissive to avoid accidental data blackout.
        _ => true,
    }
}

#[inline]
pub(super) fn should_arm_ws_primary_fallback(
    mode: &str,
    ws_cap_ready: bool,
    ws_breach_persisted: bool,
    fallback_active: bool,
) -> bool {
    mode == "websocket_primary" && ws_breach_persisted && !ws_cap_ready && !fallback_active
}

#[inline]
pub(super) fn should_enforce_udp_share_cap(
    mode: &str,
    fallback_active: bool,
    share_high: bool,
) -> bool {
    matches!(mode, "active_active" | "websocket_primary") && !fallback_active && share_high
}

pub(super) fn ref_event_ts_ms(tick: &RefTick) -> i64 {
    tick.event_ts_exchange_ms.max(tick.event_ts_ms)
}

pub(super) fn should_replace_ref_tick(current: &RefTick, next: &RefTick) -> bool {
    let current_event = ref_event_ts_ms(current);
    let next_event = ref_event_ts_ms(next);
    // Guard: ignore ticks whose event timestamp is wildly in the future vs our local receive time.
    // (This can happen under clock skew or malformed payloads and would break latency ranking.)
    if next_event > next.recv_ts_ms + 5_000 {
        return false;
    }

    let staleness_budget_us = fusion_staleness_budget_us_for_source(next.source.as_str());
    if next.recv_ts_local_ns > 0
        && current.recv_ts_local_ns > 0
        && next.source != current.source
        && next.recv_ts_local_ns > current.recv_ts_local_ns
    {
        let arrival_delta_us = (next.recv_ts_local_ns - current.recv_ts_local_ns) / 1_000;
        if arrival_delta_us > staleness_budget_us && next_event <= current_event + 1 {
            return false;
        }
    }

    // Priority 1: Chainlink RTDS has <5ms latency, always prefer when timestamps are recent
    if next.source == "chainlink_rtds" && next_event + 50 >= current_event {
        return true;
    }
    if current.source == "chainlink_rtds" && current_event + 50 >= next_event {
        return false;
    }
    // Goal: "fastest observable tick" for latency-sensitive triggers.
    // Event timestamps across sources are not perfectly comparable, so we bias towards lower
    // `recv_ts_ms - event_ts_ms` latency (i.e. faster wire path), with a small guard against
    // extreme back-jumps in event time.
    let current_latency_ms = (current.recv_ts_ms - current_event).max(0) as f64;
    let next_latency_ms = (next.recv_ts_ms - next_event).max(0) as f64;

    const EVENT_BACKJUMP_TOL_MS: i64 = 250;
    const LATENCY_DOMINATE_MS: f64 = 20.0;
    const LATENCY_TIE_EPS_MS: f64 = 5.0;

    if next_event + EVENT_BACKJUMP_TOL_MS < current_event {
        // Allow a big latency improvement to override moderate event-time skew.
        return next_latency_ms + LATENCY_DOMINATE_MS < current_latency_ms;
    }

    if next_latency_ms + LATENCY_TIE_EPS_MS < current_latency_ms {
        return true;
    }
    if current_latency_ms + LATENCY_TIE_EPS_MS < next_latency_ms {
        return false;
    }

    // Within ~5ms latency tie: prefer the newer event time, otherwise keep first-arriving tick.
    next_event > current_event + 1
}

// Source-specific fusion staleness budgets:
// - UDP path (`binance_udp`) has sub-millisecond latency, so use a tighter 200us window.
// - Other sources (direct WS, Chainlink) keep a looser 600us default.
pub(super) fn fusion_staleness_budget_us_for_source(next_source: &str) -> i64 {
    static BUDGET_UDP_US: OnceLock<i64> = OnceLock::new();
    static BUDGET_DEFAULT_US: OnceLock<i64> = OnceLock::new();

    // UDP path: aggressive freshness window (default 200us).
    if next_source.contains("udp") {
        return *BUDGET_UDP_US.get_or_init(|| {
            std::env::var("POLYEDGE_FUSION_STALENESS_UDP_US")
                .ok()
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(200)
                .clamp(50, 2_000)
        });
    }

    // Other sources: default 600us window.
    *BUDGET_DEFAULT_US.get_or_init(|| {
        std::env::var("POLYEDGE_FUSION_STALENESS_BUDGET_US")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(600)
            .clamp(50, 5_000)
    })
}

pub(super) fn udp_min_freshness_score() -> f64 {
    static UDP_MIN_FRESHNESS_SCORE: OnceLock<f64> = OnceLock::new();
    *UDP_MIN_FRESHNESS_SCORE.get_or_init(|| {
        std::env::var("POLYEDGE_UDP_MIN_FRESHNESS_SCORE")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.55)
            .clamp(0.0, 1.0)
    })
}

pub(super) fn ws_primary_fallback_gap_ns() -> i64 {
    3_000 * 1_000_000
}

pub(super) fn udp_downweight_keep_every() -> usize {
    static UDP_KEEP_EVERY: OnceLock<usize> = OnceLock::new();
    *UDP_KEEP_EVERY.get_or_init(|| {
        std::env::var("POLYEDGE_UDP_DOWNWEIGHT_KEEP_EVERY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(3)
            .clamp(2, 32)
    })
}

#[inline]
pub(super) fn should_log_ref_tick(ingest_seq: u64) -> bool {
    static SAMPLE_EVERY: OnceLock<u64> = OnceLock::new();
    let sample_every = *SAMPLE_EVERY.get_or_init(|| {
        std::env::var("POLYEDGE_REF_TICK_LOG_SAMPLE_EVERY")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(32)
            .clamp(1, 1024)
    });
    ingest_seq % sample_every == 0
}

pub(super) fn is_ref_tick_duplicate(
    current_source_ts_ms: i64,
    current_price: f64,
    prev_source_ts_ms: i64,
    prev_price: f64,
    cfg: &FusionConfig,
) -> bool {
    if (current_source_ts_ms - prev_source_ts_ms).abs() > cfg.dedupe_window_ms {
        return false;
    }
    if !current_price.is_finite() || !prev_price.is_finite() {
        return false;
    }
    let denom = prev_price.abs().max(1e-9);
    let rel_bps = ((current_price - prev_price).abs() / denom) * 10_000.0;
    rel_bps <= cfg.dedupe_price_bps
}

pub(super) fn max_book_top_diff_bps(
    prev_bid_yes: f64,
    prev_ask_yes: f64,
    prev_bid_no: f64,
    prev_ask_no: f64,
    next: &BookTop,
) -> f64 {
    fn diff_bps(a: f64, b: f64) -> f64 {
        if !a.is_finite() || !b.is_finite() {
            return f64::INFINITY;
        }
        let denom = a.abs().max(1e-9);
        ((a - b).abs() / denom) * 10_000.0
    }

    diff_bps(prev_bid_yes, next.bid_yes)
        .max(diff_bps(prev_ask_yes, next.ask_yes))
        .max(diff_bps(prev_bid_no, next.bid_no))
        .max(diff_bps(prev_ask_no, next.ask_no))
}

pub(super) fn should_replace_anchor_tick(current: &RefTick, next: &RefTick) -> bool {
    let current_event = ref_event_ts_ms(current);
    let next_event = ref_event_ts_ms(next);
    if next_event + 50 < current_event {
        return false;
    }
    if next_event > current_event + 1 {
        return true;
    }
    next.recv_ts_local_ns > current.recv_ts_local_ns + 1_000_000
}

pub(super) fn insert_latest_tick(latest_ticks: &mut HashMap<String, RefTick>, tick: RefTick) {
    if let Some(current) = latest_ticks.get_mut(tick.symbol.as_str()) {
        if should_replace_ref_tick(current, &tick) {
            *current = tick;
        }
    } else {
        latest_ticks.insert(tick.symbol.clone(), tick);
    }
}

pub(super) fn insert_latest_anchor_tick(
    latest_ticks: &mut HashMap<String, RefTick>,
    tick: RefTick,
) {
    if let Some(current) = latest_ticks.get_mut(tick.symbol.as_str()) {
        if should_replace_anchor_tick(current, &tick) {
            *current = tick;
        }
    } else {
        latest_ticks.insert(tick.symbol.clone(), tick);
    }
}

pub(super) fn upsert_latest_tick_slot(
    latest_ticks: &DashMap<String, RefTick>,
    tick: RefTick,
    should_replace: fn(&RefTick, &RefTick) -> bool,
) -> Option<i64> {
    if let Some(mut current) = latest_ticks.get_mut(&tick.symbol) {
        if !should_replace(current.value(), &tick) {
            return None;
        }
        let delta_ns = if current.source != tick.source
            && current.recv_ts_local_ns > 0
            && tick.recv_ts_local_ns > 0
        {
            Some((tick.recv_ts_local_ns - current.recv_ts_local_ns).unsigned_abs() as i64)
        } else {
            None
        };
        *current.value_mut() = tick;
        return delta_ns;
    }
    latest_ticks.insert(tick.symbol.clone(), tick);
    None
}

pub(super) fn insert_latest_ref_tick(
    latest_fast_ticks: &mut HashMap<String, RefTick>,
    latest_anchor_ticks: &mut HashMap<String, RefTick>,
    tick: RefTick,
) {
    if is_anchor_ref_source(tick.source.as_str()) {
        insert_latest_anchor_tick(latest_anchor_ticks, tick);
    } else {
        insert_latest_tick(latest_fast_ticks, tick);
    }
}

pub(super) fn pick_latest_tick<'a>(
    latest_ticks: &'a HashMap<String, RefTick>,
    symbol: &str,
) -> Option<&'a RefTick> {
    latest_ticks.get(symbol)
}
