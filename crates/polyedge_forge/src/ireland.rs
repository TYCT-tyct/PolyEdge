use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use core_types::{BookTop, MarketFeed, RefPriceWsFeed};
use feed_polymarket::PolymarketFeed;
use feed_reference::MultiSourceRefFeed;
use futures::StreamExt;
use market_discovery::{DiscoveryConfig, MarketDescriptor, MarketDiscovery};
use reqwest::Client;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

use crate::api::{run_api_server, ApiConfig};
use crate::cli::{IrelandApiArgs, IrelandRecorderArgs};
use crate::common::{parse_lower_csv, parse_timestamp_ms, parse_upper_csv, timeframe_to_ms};
use crate::db_sink::{normalize_opt_url, run_db_sink, DbEvent, DbSinkConfig};
use crate::market_data_exchange::{
    market_in_sampling_window, market_pair_key, trim_candidate_pool,
};
use crate::market_switch::resolve_switch_snapshot;
use crate::models::{
    ChainlinkLocal, MarketMeta, MotionState, PersistEvent, RoundRow, SnapshotRow,
    TokyoBinanceLocal, TokyoBinanceWire,
};
use crate::persist::{log_ingest, persist_event};

// --- 后台 Target Fetch 请求/响应定义 ---
struct TargetFetchReq {
    cache_key: String,
    symbol: String,
    timeframe: String,
    start_ts_ms: i64,
}

struct TargetFetchRes {
    cache_key: String,
    target_price: Option<f64>,
}

// Removed: MARKET_FUTURE_GUARD_DEFAULT_MS (replaced by market_prestart_allow_ms)
const MARKET_PRESTART_ALLOW_DEFAULT_MS: i64 = 30_000;
const MARKET_SAMPLE_END_GRACE_DEFAULT_MS: i64 = 300;
const MARKET_STALE_GUARD_DEFAULT_MS: i64 = 1_000;
const MOTION_PRICE_TAU_SEC: f64 = 1.2;
const MOTION_VELOCITY_TAU_SEC: f64 = 1.8;
const MOTION_MIN_DT_SEC: f64 = 0.02;
const MOTION_MAX_DT_SEC: f64 = 5.0;
const MOTION_VELOCITY_ABS_CAP: f64 = 5_000.0;
const MOTION_ACCEL_ABS_CAP: f64 = 25_000.0;
const PROB_SMOOTH_TAU_SEC: f64 = 4.0;
const DELTA_SMOOTH_TAU_SEC: f64 = 2.4;
const DELTA_MAX_STEP_PCT_PER_SEC: f64 = 0.18;
const PROB_RAW_MAX_STEP_PER_SEC: f64 = 0.45;
const PROB_RAW_MAX_STEP_MID_CLOSE_PER_SEC: f64 = 0.80;
const PROB_RAW_MAX_STEP_NEAR_CLOSE_PER_SEC: f64 = 1.20;
const PROB_STATE_RETENTION_MS: i64 = 2 * 60 * 60 * 1000;
const TARGET_RETRY_BACKOFF_MS: i64 = 1_200;
const RECORDER_HOUSEKEEPING_INTERVAL_MS: i64 = 10_000;
const RECORDER_STALE_STATE_RETENTION_MS: i64 = 20 * 60 * 1000;
const TARGET_CACHE_RETENTION_MS: i64 = 2 * 60 * 60 * 1000;
const TARGET_RETRY_RETENTION_MS: i64 = 20 * 60 * 1000;
const ROUND_META_RETENTION_MS: i64 = 6 * 60 * 60 * 1000;
const EMITTED_ROUND_RETENTION_MS: i64 = 6 * 60 * 60 * 1000;
const DISCOVERY_EMPTY_GRACE_DEFAULT_MS: i64 = 8_000;
const SAMPLE_GAP_WARN_MS: i64 = 1000;
const ROUND_DROP_ON_QUALITY_FAIL_DEFAULT: bool = false;
const ROUND_FINALIZE_RETRY_GRACE_MS: i64 = 45_000;
const ROUND_FINALIZE_DEFER_LOG_THROTTLE_MS: i64 = 15_000;
const TOKYO_INPUT_STALE_GUARD_DEFAULT_MS: i64 = 2_500;
const INPUT_STALE_WARN_THROTTLE_MS: i64 = 15_000;
const MARKET_SELECTION_FALLBACK_PREWARM_DEFAULT_MS: i64 = 300_000;
const MARKET_CANDIDATE_POOL_DEFAULT_SIZE: usize = 3;
const DISCOVERY_PREWARM_LEAD_DEFAULT_MS: i64 = 300_000;
const DISCOVERY_PREWARM_REFRESH_DEFAULT_SEC: u64 = 2;
const MARKET_SWITCH_MIN_HOLD_DEFAULT_MS: i64 = 1_500;
const BOOK_STALE_MAX_AGE_5M_MS: i64 = 8_000;
const BOOK_STALE_MAX_AGE_15M_MS: i64 = 12_000;
const QUOTE_FALLBACK_MAX_AGE_5M_MS: i64 = 12_000;
const QUOTE_FALLBACK_MAX_AGE_15M_MS: i64 = 20_000;
const QUOTE_CROSS_MARKET_BRIDGE_MAX_AGE_MS: i64 = 1_500;
const SWITCH_GUARD_LOG_THROTTLE_MS: i64 = 10_000;
const SWITCH_STATS_REPORT_INTERVAL_MS: i64 = 60_000;

fn env_flag(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .map(|v| {
            !matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "off" | "no"
            )
        })
        .unwrap_or(default)
}

fn chainlink_runtime_enabled() -> bool {
    env_flag("FORGE_CHAINLINK_ENABLED", false)
}

fn round_drop_on_quality_fail_enabled() -> bool {
    env_flag(
        "FORGE_ROUND_DROP_ON_QUALITY_FAIL",
        ROUND_DROP_ON_QUALITY_FAIL_DEFAULT,
    )
}

#[derive(Debug, Clone)]
struct ActiveMarketFilter {
    by_symbol: HashMap<String, HashSet<String>>,
    symbols: Vec<String>,
    timeframes: Vec<String>,
}

impl ActiveMarketFilter {
    fn from_inputs(active_symbols: &[String], active_tfs: &[String], raw: &str) -> Self {
        let allowed_symbols: HashSet<String> = active_symbols
            .iter()
            .map(|v| v.to_ascii_uppercase())
            .collect();
        let allowed_tfs: HashSet<String> =
            active_tfs.iter().map(|v| v.to_ascii_lowercase()).collect();

        let mut default_by_symbol: HashMap<String, HashSet<String>> = HashMap::new();
        for symbol in &allowed_symbols {
            let tfs = allowed_tfs.iter().cloned().collect::<HashSet<_>>();
            if !tfs.is_empty() {
                default_by_symbol.insert(symbol.clone(), tfs);
            }
        }

        let mut custom_by_symbol: HashMap<String, HashSet<String>> = HashMap::new();
        for entry in raw.split([',', ';']) {
            let item = entry.trim();
            if item.is_empty() {
                continue;
            }
            let Some((symbol_raw, tfs_raw)) = item.split_once(':') else {
                continue;
            };
            let symbol = symbol_raw.trim().to_ascii_uppercase();
            if !allowed_symbols.contains(&symbol) {
                continue;
            }
            for tf in tfs_raw
                .split(['|', '/'])
                .map(|v| v.trim().to_ascii_lowercase())
                .filter(|v| !v.is_empty() && allowed_tfs.contains(v))
            {
                custom_by_symbol
                    .entry(symbol.clone())
                    .or_default()
                    .insert(tf);
            }
        }

        let mut by_symbol = if custom_by_symbol.is_empty() {
            default_by_symbol
        } else {
            custom_by_symbol
        };
        by_symbol.retain(|_, tfs| !tfs.is_empty());

        let mut symbols = by_symbol.keys().cloned().collect::<Vec<_>>();
        symbols.sort();
        let mut timeframes = by_symbol
            .values()
            .flat_map(|set| set.iter().cloned())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        timeframes.sort();

        Self {
            by_symbol,
            symbols,
            timeframes,
        }
    }

    fn allows(&self, symbol: &str, timeframe: &str) -> bool {
        let symbol = symbol.to_ascii_uppercase();
        let timeframe = timeframe.to_ascii_lowercase();
        self.by_symbol
            .get(&symbol)
            .map(|tfs| tfs.contains(&timeframe))
            .unwrap_or(false)
    }

    fn summary(&self) -> String {
        let mut entries = self
            .by_symbol
            .iter()
            .map(|(symbol, tfs)| {
                let mut tf_list = tfs.iter().cloned().collect::<Vec<_>>();
                tf_list.sort();
                format!("{symbol}:{}", tf_list.join("|"))
            })
            .collect::<Vec<_>>();
        entries.sort();
        entries.join(",")
    }
}

fn round_end_ts_ms_from_round_id(round_id: &str) -> Option<i64> {
    let mut parts = round_id.rsplitn(3, '_');
    let start_ms = parts.next()?.trim().parse::<i64>().ok()?;
    let timeframe = parts.next()?;
    let tf_ms = timeframe_to_ms(timeframe)?;
    Some(start_ms.saturating_add(tf_ms))
}

fn market_candidate_is_warmable(
    m: &MarketMeta,
    now_ms: i64,
    prewarm_ms: i64,
    stale_guard_ms: i64,
) -> bool {
    now_ms.saturating_add(prewarm_ms) >= m.start_ts_ms
        && now_ms <= m.end_ts_ms.saturating_add(stale_guard_ms)
}

fn round_meta_is_fresh(round_id: &str, now_ms: i64, retention_ms: i64) -> bool {
    round_end_ts_ms_from_round_id(round_id)
        .map(|round_end_ms| now_ms <= round_end_ms.saturating_add(retention_ms))
        .unwrap_or(false)
}

fn trim_map_by_key_ts_desc<V, F>(map: &mut HashMap<String, V>, cap: usize, mut ts_of: F) -> usize
where
    F: FnMut(&str, &V) -> i64,
{
    if cap == 0 || map.len() <= cap {
        return 0;
    }
    let mut ordered = map
        .iter()
        .map(|(k, v)| (k.clone(), ts_of(k, v)))
        .collect::<Vec<_>>();
    ordered.sort_unstable_by(|a, b| b.1.cmp(&a.1));
    let mut dropped = 0usize;
    for (k, _) in ordered.into_iter().skip(cap) {
        if map.remove(&k).is_some() {
            dropped = dropped.saturating_add(1);
        }
    }
    dropped
}

fn ema_alpha_from_tau(dt_s: f64, tau_s: f64) -> f64 {
    if !dt_s.is_finite() || !tau_s.is_finite() || dt_s <= 0.0 || tau_s <= 0.0 {
        return 1.0;
    }
    // Continuous-time EMA mapped to discrete step: alpha = 1 - exp(-dt/tau).
    let a = 1.0 - (-dt_s / tau_s).exp();
    a.clamp(0.0, 1.0)
}

#[derive(Debug, Clone, Copy)]
struct ProbSmoothState {
    ts_ms: i64,
    ema_up: f64,
    ema_delta_pct: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
struct RoundQualityPolicy {
    min_coverage_ratio: f64,
    min_sample_ratio: f64,
    max_gap_ms: i64,
    start_tolerance_ms: i64,
    end_tolerance_ms: i64,
    settle_stale_tolerance_ms: i64,
}

impl RoundQualityPolicy {
    fn from_args(args: &IrelandRecorderArgs, sample_period_ms: i64) -> Self {
        let base_gap = sample_period_ms.saturating_mul(20);
        let base_tol = sample_period_ms.saturating_mul(8);
        Self {
            min_coverage_ratio: args.round_min_coverage_ratio.clamp(0.50, 1.0),
            min_sample_ratio: args.round_min_sample_ratio.clamp(0.50, 1.0),
            max_gap_ms: args.round_max_gap_ms.max(base_gap).max(1_000),
            start_tolerance_ms: args.round_start_tolerance_ms.max(base_tol).max(500),
            end_tolerance_ms: args.round_end_tolerance_ms.max(base_tol).max(500),
            settle_stale_tolerance_ms: sample_period_ms.saturating_mul(120).max(12_000),
        }
    }
}

#[derive(Debug)]
struct RoundQualityReport {
    accept: bool,
    reasons: Vec<String>,
    sample_count: usize,
    expected_samples: usize,
    reachable_expected_samples: usize,
    coverage_ratio: f64,
    sample_ratio: f64,
    reachable_sample_ratio: f64,
    sample_ratio_gate: f64,
    max_gap_ms: i64,
    avg_gap_ms: i64,
    start_delay_ms: i64,
    end_missing_ms: i64,
    settle_stale_ms: i64,
}

#[derive(Debug)]
struct RoundBuffer {
    round_id: String,
    market_id: String,
    symbol: String,
    timeframe: String,
    title: String,
    start_ts_ms: i64,
    end_ts_ms: i64,
    timeframe_ms: i64,
    snapshots: Vec<SnapshotRow>,
    first_sample_ms: Option<i64>,
    last_sample_ms: Option<i64>,
    max_gap_ms: i64,
    target_price_latest: Option<f64>,
    settle_price_latest: Option<f64>,
    settle_price_latest_ts_ms: Option<i64>,
}

impl RoundBuffer {
    fn new(round_id: String, market: &MarketMeta) -> Self {
        let timeframe_ms = (market.end_ts_ms - market.start_ts_ms)
            .max(timeframe_to_ms(&market.timeframe).unwrap_or(0))
            .max(1);
        Self {
            round_id,
            market_id: market.market_id.clone(),
            symbol: market.symbol.clone(),
            timeframe: market.timeframe.clone(),
            title: market.title.clone(),
            start_ts_ms: market.start_ts_ms,
            end_ts_ms: market.end_ts_ms,
            timeframe_ms,
            snapshots: Vec::with_capacity(16_000),
            first_sample_ms: None,
            last_sample_ms: None,
            max_gap_ms: 0,
            target_price_latest: market.target_price,
            settle_price_latest: None,
            settle_price_latest_ts_ms: None,
        }
    }

    fn push_snapshot(&mut self, row: SnapshotRow) {
        let ts = row.ts_ireland_sample_ms;
        if let Some(prev) = self.last_sample_ms {
            let gap = ts.saturating_sub(prev);
            if gap > self.max_gap_ms {
                self.max_gap_ms = gap;
            }
        }
        if self.first_sample_ms.is_none() {
            self.first_sample_ms = Some(ts);
        }
        self.last_sample_ms = Some(ts);
        if let Some(v) = row.target_price {
            self.target_price_latest = Some(v);
        }
        if let Some(v) = row.binance_price {
            self.observe_settle_price(v, ts);
        }
        self.snapshots.push(row);
    }

    fn observe_settle_price(&mut self, price: f64, ts_ms: i64) {
        if !price.is_finite() || price <= 0.0 {
            return;
        }
        self.settle_price_latest = Some(price);
        self.settle_price_latest_ts_ms = Some(ts_ms.max(0));
    }

    fn evaluate(&self, policy: &RoundQualityPolicy, sample_period_ms: i64) -> RoundQualityReport {
        let expected_window_ms = self.timeframe_ms.max(1);
        let expected_samples = ((expected_window_ms as f64 / sample_period_ms.max(1) as f64).ceil()
            as usize)
            .saturating_add(1);
        let sample_count = self.snapshots.len();

        let first = self.first_sample_ms.unwrap_or(0);
        let last = self.last_sample_ms.unwrap_or(0);
        let coverage_ms = last.saturating_sub(first).max(0);
        let avg_gap_ms = if sample_count >= 2 {
            coverage_ms
                .div_euclid((sample_count.saturating_sub(1)) as i64)
                .max(1)
        } else {
            i64::MAX
        };
        let coverage_ratio = coverage_ms as f64 / expected_window_ms as f64;
        let sample_ratio = sample_count as f64 / expected_samples.max(1) as f64;
        let effective_period_ms = if sample_count >= 2 {
            avg_gap_ms.max(sample_period_ms.max(1))
        } else {
            sample_period_ms.max(1)
        };
        let reachable_expected_samples = ((expected_window_ms as f64 / effective_period_ms as f64)
            .ceil() as usize)
            .saturating_add(1);
        let reachable_sample_ratio = sample_count as f64 / reachable_expected_samples.max(1) as f64;
        let sample_ratio_gate = sample_ratio.max(reachable_sample_ratio);
        let start_delay_ms = first.saturating_sub(self.start_ts_ms).max(0);
        let end_missing_ms = self.end_ts_ms.saturating_sub(last).max(0);
        let settle_stale_ms = self
            .settle_price_latest_ts_ms
            .map(|v| self.end_ts_ms.saturating_sub(v).max(0))
            .unwrap_or(i64::MAX);

        let mut reasons = Vec::<String>::new();
        if sample_count < 2 {
            reasons.push("sample_count<2".to_string());
        }
        if start_delay_ms > policy.start_tolerance_ms {
            reasons.push(format!("late_start={}ms", start_delay_ms));
        }
        if end_missing_ms > policy.end_tolerance_ms {
            reasons.push(format!("early_end={}ms", end_missing_ms));
        }
        if coverage_ratio < policy.min_coverage_ratio {
            reasons.push(format!("coverage_ratio={:.4}", coverage_ratio));
        }
        if sample_ratio_gate < policy.min_sample_ratio {
            reasons.push(format!(
                "sample_ratio_gate={:.4}(static={:.4},reachable={:.4})",
                sample_ratio_gate, sample_ratio, reachable_sample_ratio
            ));
        }
        let avg_gap_guard_ms = policy
            .max_gap_ms
            .min(sample_period_ms.saturating_mul(8))
            .max(sample_period_ms.saturating_mul(2));
        if sample_count >= 3 && avg_gap_ms > avg_gap_guard_ms {
            reasons.push(format!("avg_gap={}ms", avg_gap_ms));
        }
        if self.max_gap_ms > policy.max_gap_ms {
            reasons.push(format!("max_gap={}ms", self.max_gap_ms));
        }
        if self.target_price_latest.is_none() {
            reasons.push("missing_target_price".to_string());
        }
        if self.settle_price_latest.is_none() {
            reasons.push("missing_settle_price".to_string());
        } else if settle_stale_ms > policy.settle_stale_tolerance_ms {
            reasons.push(format!("stale_settle={}ms", settle_stale_ms));
        }

        RoundQualityReport {
            accept: reasons.is_empty(),
            reasons,
            sample_count,
            expected_samples,
            reachable_expected_samples,
            coverage_ratio,
            sample_ratio,
            reachable_sample_ratio,
            sample_ratio_gate,
            max_gap_ms: self.max_gap_ms,
            avg_gap_ms,
            start_delay_ms,
            end_missing_ms,
            settle_stale_ms,
        }
    }
}

fn maybe_update_round_settle_from_tokyo(
    buffer: &mut RoundBuffer,
    tokyo_by_symbol: &HashMap<String, TokyoBinanceLocal>,
) {
    if let Some(tokyo) = tokyo_by_symbol.get(&buffer.symbol) {
        buffer.observe_settle_price(tokyo.binance_price, tokyo.ts_ireland_recv_ms);
    }
}

#[derive(Debug)]
struct RoundCommit {
    snapshots: Vec<SnapshotRow>,
    round: RoundRow,
}

#[derive(Debug, Clone, Copy)]
struct StableQuote {
    bid_yes: f64,
    ask_yes: f64,
    bid_no: f64,
    ask_no: f64,
}

#[derive(Debug, Clone)]
struct QuoteCacheEntry {
    market_id: String,
    ts_ms: i64,
    quote: StableQuote,
}

fn quote_fallback_max_age_ms(timeframe: &str) -> i64 {
    match timeframe {
        "5m" => QUOTE_FALLBACK_MAX_AGE_5M_MS,
        "15m" => QUOTE_FALLBACK_MAX_AGE_15M_MS,
        _ => QUOTE_FALLBACK_MAX_AGE_5M_MS,
    }
}

fn book_stale_max_age_ms(timeframe: &str) -> i64 {
    match timeframe {
        "5m" => BOOK_STALE_MAX_AGE_5M_MS,
        "15m" => BOOK_STALE_MAX_AGE_15M_MS,
        _ => BOOK_STALE_MAX_AGE_5M_MS,
    }
}

fn book_seen_ts_ms(book: &BookTop) -> i64 {
    if book.recv_ts_local_ns > 0 {
        book.recv_ts_local_ns / 1_000_000
    } else {
        book.ts_ms
    }
}

fn pick_fresh_book_for_market<'a>(
    market: &MarketMeta,
    now_ms: i64,
    books: &'a HashMap<String, BookTop>,
) -> Option<&'a BookTop> {
    let max_age_ms = book_stale_max_age_ms(&market.timeframe);
    books
        .get(&market.market_id)
        .filter(|book| now_ms.saturating_sub(book_seen_ts_ms(book)) <= max_age_ms)
}

fn pick_fresh_cached_quote(
    market_id: &str,
    market_cached: Option<&QuoteCacheEntry>,
    pair_cached: Option<&QuoteCacheEntry>,
    now_ms: i64,
    max_age_ms: i64,
) -> Option<StableQuote> {
    if let Some(cached) = market_cached {
        if now_ms.saturating_sub(cached.ts_ms) <= max_age_ms {
            return Some(cached.quote);
        }
    }
    if let Some(cached) = pair_cached {
        let age_ms = now_ms.saturating_sub(cached.ts_ms);
        let same_market = cached.market_id == market_id;
        let allowed_age = if same_market {
            max_age_ms
        } else {
            QUOTE_CROSS_MARKET_BRIDGE_MAX_AGE_MS
        };
        if age_ms <= allowed_age {
            return Some(cached.quote);
        }
    }
    None
}

fn fused_up_probability(quote: &StableQuote, prev_up: Option<f64>) -> f64 {
    let up_from_yes = ((quote.bid_yes + quote.ask_yes) * 0.5).clamp(0.0, 1.0);
    let up_from_no = (1.0 - (quote.bid_no + quote.ask_no) * 0.5).clamp(0.0, 1.0);
    let diff = (up_from_yes - up_from_no).abs();
    let spread_yes = (quote.ask_yes - quote.bid_yes).abs().clamp(0.0, 1.0);
    let spread_no = (quote.ask_no - quote.bid_no).abs().clamp(0.0, 1.0);

    let fused = if diff <= 0.10 {
        (up_from_yes + up_from_no) * 0.5
    } else if spread_yes + 0.005 < spread_no {
        up_from_yes
    } else if spread_no + 0.005 < spread_yes {
        up_from_no
    } else if let Some(prev) = prev_up {
        if (up_from_yes - prev).abs() <= (up_from_no - prev).abs() {
            up_from_yes
        } else {
            up_from_no
        }
    } else {
        (up_from_yes + up_from_no) * 0.5
    };

    fused.clamp(0.0, 1.0)
}

fn sanitize_prob(v: f64) -> Option<f64> {
    if v.is_finite() {
        Some(v.clamp(0.0, 1.0))
    } else {
        None
    }
}

fn normalize_side_quotes(
    bid: Option<f64>,
    ask: Option<f64>,
    cached_bid: Option<f64>,
    cached_ask: Option<f64>,
) -> Option<(f64, f64)> {
    let mut b = bid.or(cached_bid);
    let mut a = ask.or(cached_ask);
    match (b, a) {
        (Some(bv), Some(av)) => {
            b = Some(bv);
            a = Some(av);
        }
        (Some(v), None) => {
            b = Some(v);
            a = Some(v);
        }
        (None, Some(v)) => {
            b = Some(v);
            a = Some(v);
        }
        (None, None) => return None,
    }
    let mut bid = b?;
    let mut ask = a?;
    if ask < bid {
        std::mem::swap(&mut ask, &mut bid);
    }
    Some((bid.clamp(0.0, 1.0), ask.clamp(0.0, 1.0)))
}

fn stabilize_book_quotes(
    book: &BookTop,
    cached: Option<(f64, f64, f64, f64)>,
) -> Option<StableQuote> {
    let cached_by = cached.map(|v| v.0);
    let cached_ay = cached.map(|v| v.1);
    let cached_bn = cached.map(|v| v.2);
    let cached_an = cached.map(|v| v.3);

    let (bid_yes, ask_yes) = normalize_side_quotes(
        sanitize_prob(book.bid_yes),
        sanitize_prob(book.ask_yes),
        cached_by,
        cached_ay,
    )?;
    let (bid_no, ask_no) = normalize_side_quotes(
        sanitize_prob(book.bid_no),
        sanitize_prob(book.ask_no),
        cached_bn,
        cached_an,
    )?;

    Some(StableQuote {
        bid_yes,
        ask_yes,
        bid_no,
        ask_no,
    })
}

async fn flush_round_commit(
    commit: RoundCommit,
    sink_tx: Option<&mpsc::Sender<DbEvent>>,
    persist_tx: &mpsc::UnboundedSender<PersistEvent>,
    sink_snapshot_already_sent: bool,
) {
    let mut sink_closed = false;
    for row in commit.snapshots {
        if !sink_closed && !sink_snapshot_already_sent {
            if let Some(tx) = sink_tx {
                if let Err(err) = tx.send(DbEvent::Snapshot(Box::new(row.clone()))).await {
                    sink_closed = true;
                    tracing::warn!(?err, "db sink closed while committing accepted snapshots");
                }
            }
        }
        if let Err(err) = persist_tx.send(PersistEvent::Snapshot(Box::new(row))) {
            tracing::error!(?err, "persist snapshot send failed (accepted round commit)");
        }
    }

    if !sink_closed {
        if let Some(tx) = sink_tx {
            if let Err(err) = tx.send(DbEvent::Round(commit.round.clone())).await {
                tracing::warn!(?err, "db sink closed while committing accepted round");
            }
        }
    }
    if let Err(err) = persist_tx.send(PersistEvent::Round(commit.round)) {
        tracing::error!(?err, "persist round send failed (accepted round commit)");
    }
}

pub async fn run_ireland_recorder(args: IrelandRecorderArgs) -> Result<()> {
    let supported_symbols = parse_upper_csv(&args.supported_symbols);
    let active_symbols = parse_upper_csv(&args.active_symbols);
    let active_tfs = parse_lower_csv(&args.active_timeframes);

    if active_symbols.is_empty() || active_tfs.is_empty() {
        anyhow::bail!("active symbols/timeframes must not be empty");
    }
    let market_filter = ActiveMarketFilter::from_inputs(
        &active_symbols,
        &active_tfs,
        &args.active_symbol_timeframes,
    );
    if market_filter.by_symbol.is_empty() {
        anyhow::bail!("active symbol/timeframe map resolves to empty set");
    }
    let subscribe_symbols = market_filter.symbols.clone();
    let subscribe_tfs = market_filter.timeframes.clone();
    let required_pair_keys: HashSet<String> = market_filter
        .by_symbol
        .iter()
        .flat_map(|(symbol, tfs)| tfs.iter().map(move |tf| format!("{symbol}:{tf}")))
        .collect();
    let active_pair_count = required_pair_keys.len().max(1);
    let motion_state_cap = active_pair_count.saturating_mul(64).clamp(256, 20_000);
    let target_cache_cap = active_pair_count.saturating_mul(1024).clamp(2_048, 200_000);
    let round_state_cap = active_pair_count.saturating_mul(2048).clamp(4_096, 400_000);
    let active_symbols_set: HashSet<String> = subscribe_symbols
        .iter()
        .map(|v| v.to_ascii_uppercase())
        .collect();
    let tokyo_input_stale_guard_ms = std::env::var("FORGE_TOKYO_INPUT_STALE_GUARD_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(TOKYO_INPUT_STALE_GUARD_DEFAULT_MS)
        .clamp(500, 30_000);
    // Removed unused market_future_guard_ms - using prestart_allow for boundary control
    let market_prestart_allow_ms = std::env::var("FORGE_MARKET_PRESTART_ALLOW_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(MARKET_PRESTART_ALLOW_DEFAULT_MS)
        .clamp(0, 10 * 60 * 1000);
    let market_sample_end_grace_ms = std::env::var("FORGE_MARKET_SAMPLE_END_GRACE_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(MARKET_SAMPLE_END_GRACE_DEFAULT_MS)
        .clamp(0, 5_000);
    let market_stale_guard_ms = std::env::var("FORGE_MARKET_STALE_GUARD_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(MARKET_STALE_GUARD_DEFAULT_MS)
        .clamp(300, 10_000);
    let market_selection_fallback_prewarm_ms =
        std::env::var("FORGE_MARKET_SELECTION_FALLBACK_PREWARM_MS")
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .unwrap_or(MARKET_SELECTION_FALLBACK_PREWARM_DEFAULT_MS)
            .clamp(30_000, 15 * 60 * 1_000);
    let market_candidate_pool_size = std::env::var("FORGE_MARKET_CANDIDATE_POOL_SIZE")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(MARKET_CANDIDATE_POOL_DEFAULT_SIZE)
        .clamp(2, 8);
    let market_switch_min_hold_ms = std::env::var("FORGE_MARKET_SWITCH_MIN_HOLD_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(MARKET_SWITCH_MIN_HOLD_DEFAULT_MS)
        .clamp(0, 15_000);
    let discovery_empty_grace_ms = std::env::var("FORGE_DISCOVERY_EMPTY_GRACE_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(DISCOVERY_EMPTY_GRACE_DEFAULT_MS)
        .clamp(10_000, 10 * 60 * 1000);
    let round_drop_on_quality_fail = round_drop_on_quality_fail_enabled();

    let root = PathBuf::from(&args.data_root);
    fs::create_dir_all(root.join("snapshot_100ms")).ok();
    fs::create_dir_all(root.join("rounds")).ok();
    fs::create_dir_all(root.join("ingest_log")).ok();

    let sample_period_ms = args.sample_ms.max(20) as i64;
    let quality_policy = RoundQualityPolicy::from_args(&args, sample_period_ms);

    tracing::info!(
        data_root = %args.data_root,
        udp_bind = %args.udp_bind,
        sample_ms = args.sample_ms,
        round_min_coverage_ratio = quality_policy.min_coverage_ratio,
        round_min_sample_ratio = quality_policy.min_sample_ratio,
        round_max_gap_ms = quality_policy.max_gap_ms,
        round_start_tolerance_ms = quality_policy.start_tolerance_ms,
        round_end_tolerance_ms = quality_policy.end_tolerance_ms,
        settle_stale_tolerance_ms = quality_policy.settle_stale_tolerance_ms,
        tokyo_input_stale_guard_ms = tokyo_input_stale_guard_ms,
        // Removed: market_future_guard_ms (unused)
        market_prestart_allow_ms = market_prestart_allow_ms,
        market_sample_end_grace_ms = market_sample_end_grace_ms,
        market_stale_guard_ms = market_stale_guard_ms,
        market_selection_fallback_prewarm_ms = market_selection_fallback_prewarm_ms,
        market_candidate_pool_size = market_candidate_pool_size,
        market_switch_min_hold_ms = market_switch_min_hold_ms,
        discovery_empty_grace_ms = discovery_empty_grace_ms,
        round_drop_on_quality_fail = round_drop_on_quality_fail,
        market_filter = %market_filter.summary(),
        ?supported_symbols,
        ?active_symbols,
        ?active_tfs,
        active_pair_count = active_pair_count,
        motion_state_cap = motion_state_cap,
        target_cache_cap = target_cache_cap,
        round_state_cap = round_state_cap,
        api_bind = %args.api_bind,
        clickhouse_url = %args.clickhouse_url,
        clickhouse_snapshot_ttl_days = args.clickhouse_snapshot_ttl_days,
        clickhouse_processed_ttl_days = args.clickhouse_processed_ttl_days,
        clickhouse_round_ttl_days = args.clickhouse_round_ttl_days,
        redis_url = %args.redis_url,
        "ireland recorder started"
    );

    let (persist_tx, mut persist_rx) = mpsc::unbounded_channel::<PersistEvent>();

    let writer_root = root.clone();
    tokio::spawn(async move {
        while let Some(ev) = persist_rx.recv().await {
            if let Err(err) = persist_event(&writer_root, ev) {
                tracing::error!(?err, "persist failed");
            }
        }
    });

    let sink_cfg = DbSinkConfig {
        clickhouse_url: normalize_opt_url(&args.clickhouse_url),
        clickhouse_database: args.clickhouse_database.clone(),
        clickhouse_snapshot_table: args.clickhouse_snapshot_table.clone(),
        clickhouse_processed_table: args.clickhouse_processed_table.clone(),
        clickhouse_round_table: args.clickhouse_round_table.clone(),
        clickhouse_snapshot_ttl_days: args.clickhouse_snapshot_ttl_days.clamp(1, 3650),
        clickhouse_processed_ttl_days: args.clickhouse_processed_ttl_days.clamp(1, 3650),
        clickhouse_round_ttl_days: args.clickhouse_round_ttl_days.clamp(1, 3650),
        redis_url: normalize_opt_url(&args.redis_url),
        redis_prefix: args.redis_prefix.clone(),
        redis_ttl_sec: args.redis_ttl_sec.max(60),
        batch_size: args.sink_batch_size.clamp(10, 5000),
        flush_ms: args.sink_flush_ms.clamp(100, 5000),
        queue_cap: args.sink_queue_cap.clamp(1000, 200000),
    };
    let sink_tx = if sink_cfg.clickhouse_url.is_some() || sink_cfg.redis_url.is_some() {
        let (tx, rx) = mpsc::channel::<DbEvent>(sink_cfg.queue_cap);
        tokio::spawn(run_db_sink(sink_cfg.clone(), rx));
        Some(tx)
    } else {
        None
    };

    if !args.disable_api && !args.api_bind.trim().is_empty() {
        let api_cfg = ApiConfig {
            bind: args.api_bind.clone(),
            clickhouse_url: normalize_opt_url(&args.clickhouse_url),
            redis_url: normalize_opt_url(&args.redis_url),
            redis_prefix: args.redis_prefix.clone(),
            dashboard_dist_dir: Some(args.dashboard_dist.clone()),
        };
        tokio::spawn(async move {
            if let Err(err) = run_api_server(api_cfg).await {
                tracing::error!(?err, "forge api server exited");
            }
        });
    }

    let (commit_tx, mut commit_rx) = mpsc::channel::<RoundCommit>(64);
    let commit_sink = sink_tx.clone();
    let commit_persist = persist_tx.clone();
    tokio::spawn(async move {
        while let Some(commit) = commit_rx.recv().await {
            flush_round_commit(commit, commit_sink.as_ref(), &commit_persist, true).await;
        }
    });

    log_ingest(&persist_tx, "info", "recorder", "startup complete");

    let (tokyo_tx, mut tokyo_rx) = mpsc::unbounded_channel::<TokyoBinanceWire>();
    let (chainlink_tx, mut chainlink_rx) = mpsc::unbounded_channel::<ChainlinkLocal>();
    let (book_tx, mut book_rx) = mpsc::unbounded_channel::<BookTop>();
    let (market_tx, mut market_rx) = mpsc::unbounded_channel::<Vec<MarketMeta>>();

    // target price 异步获取通道
    let (target_req_tx, mut target_req_rx) = mpsc::unbounded_channel::<TargetFetchReq>();
    let (target_res_tx, mut target_res_rx) = mpsc::unbounded_channel::<TargetFetchRes>();
    let chainlink_enabled = chainlink_runtime_enabled();

    // 提取的后台任务，接收请求并串行执行 fallback
    let bg_persist_tx = persist_tx.clone();
    tokio::spawn(async move {
        let vatic_http = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_default();

        while let Some(req) = target_req_rx.recv().await {
            let round_start_sec = aligned_round_timestamp_sec(req.start_ts_ms, &req.timeframe);
            let mut resolved_price = None;

            if let Some(v) = fetch_target_from_vatic_market(
                &vatic_http,
                &req.symbol,
                &req.timeframe,
                round_start_sec,
            )
            .await
            {
                resolved_price = Some(v);
                log_ingest(
                    &bg_persist_tx,
                    "info",
                    "target_price",
                    &format!(
                        "source=vatic_market symbol={} tf={} start_ms={} target={}",
                        req.symbol, req.timeframe, req.start_ts_ms, v
                    ),
                );
            } else if let Some(v) =
                fetch_target_from_vatic(&vatic_http, &req.symbol, &req.timeframe, round_start_sec)
                    .await
            {
                resolved_price = Some(v);
                log_ingest(
                    &bg_persist_tx,
                    "warn",
                    "target_price",
                    &format!(
                        "source=vatic_timestamp_fallback symbol={} tf={} start_ms={} target={}",
                        req.symbol, req.timeframe, req.start_ts_ms, v
                    ),
                );
            } else if let Some(v) = fetch_target_from_vatic_active(
                &vatic_http,
                &req.symbol,
                &req.timeframe,
                round_start_sec,
            )
            .await
            {
                resolved_price = Some(v);
                log_ingest(
                    &bg_persist_tx,
                    "warn",
                    "target_price",
                    &format!(
                        "source=vatic_active_fallback symbol={} tf={} start_ms={} target={}",
                        req.symbol, req.timeframe, req.start_ts_ms, v
                    ),
                );
            } else if chainlink_enabled {
                if let Some(v) = fetch_target_from_vatic_chainlink(
                    &vatic_http,
                    &req.symbol,
                    &req.timeframe,
                    round_start_sec,
                )
                .await
                {
                    resolved_price = Some(v);
                    log_ingest(
                        &bg_persist_tx,
                        "warn",
                        "target_price",
                        &format!(
                            "source=vatic_chainlink_fallback symbol={} tf={} start_ms={} target={}",
                            req.symbol, req.timeframe, req.start_ts_ms, v
                        ),
                    );
                }
            }

            if resolved_price.is_none() {
                if let Some(v) =
                    fetch_target_from_official_binance(&req.symbol, req.start_ts_ms).await
                {
                    resolved_price = Some(v);
                    log_ingest(
                        &bg_persist_tx,
                        "warn",
                        "target_price",
                        &format!(
                            "source=binance_official_fallback symbol={} tf={} start_ms={} target={}",
                            req.symbol, req.timeframe, req.start_ts_ms, v
                        ),
                    );
                }
            }

            let _ = target_res_tx.send(TargetFetchRes {
                cache_key: req.cache_key,
                target_price: resolved_price,
            });
        }
    });

    spawn_tokyo_udp_receiver(args.udp_bind.clone(), tokyo_tx, persist_tx.clone());
    if chainlink_enabled {
        spawn_chainlink_reader(subscribe_symbols.clone(), chainlink_tx, persist_tx.clone());
    } else {
        log_ingest(
            &persist_tx,
            "info",
            "chainlink",
            "chainlink runtime disabled by FORGE_CHAINLINK_ENABLED=false",
        );
    }
    spawn_book_reader(
        subscribe_symbols.clone(),
        subscribe_tfs.clone(),
        book_tx,
        persist_tx.clone(),
    );
    spawn_market_discovery_reader(
        subscribe_symbols.clone(),
        subscribe_tfs.clone(),
        args.discovery_refresh_sec.max(1),
        market_tx,
        persist_tx.clone(),
    );

    let mut tokyo_by_symbol: HashMap<String, TokyoBinanceLocal> = HashMap::new();
    let mut chainlink_by_symbol: HashMap<String, ChainlinkLocal> = HashMap::new();
    let mut book_by_market: HashMap<String, BookTop> = HashMap::new();
    let mut quote_cache_by_market: HashMap<String, QuoteCacheEntry> = HashMap::new();
    let mut quote_cache_by_pair: HashMap<String, QuoteCacheEntry> = HashMap::new();
    let mut markets_by_id: HashMap<String, MarketMeta> = HashMap::new();
    let mut candidate_markets_by_pair: HashMap<String, Vec<MarketMeta>> = HashMap::new();
    if let Ok(seed_markets) =
        discover_markets_from_target_cache(&subscribe_symbols, &subscribe_tfs).await
    {
        let seed_now_ms = Utc::now().timestamp_millis();
        for market in seed_markets {
            if !market_filter.allows(&market.symbol, &market.timeframe) {
                continue;
            }
            if seed_now_ms.saturating_add(market_prestart_allow_ms) < market.start_ts_ms {
                continue;
            }
            if seed_now_ms > market.end_ts_ms.saturating_add(market_stale_guard_ms) {
                continue;
            }
            candidate_markets_by_pair
                .entry(market_pair_key(&market))
                .or_default()
                .push(market.clone());
            markets_by_id.insert(market.market_id.clone(), market);
        }
        for markets in candidate_markets_by_pair.values_mut() {
            let now_ms = Utc::now().timestamp_millis();
            trim_candidate_pool(
                markets,
                now_ms,
                market_prestart_allow_ms,
                market_sample_end_grace_ms,
                market_candidate_pool_size,
            );
        }
        if !markets_by_id.is_empty() {
            let mut seeded_pairs = markets_by_id
                .values()
                .map(market_pair_key)
                .collect::<Vec<_>>();
            seeded_pairs.sort();
            seeded_pairs.dedup();
            tracing::info!(
                seeded_markets = markets_by_id.len(),
                seeded_pairs = seeded_pairs.join(","),
                "market meta cache prewarm applied"
            );
        }
    }
    let mut motion_by_key: HashMap<String, MotionState> = HashMap::new();
    let mut prob_smooth_by_round: HashMap<String, ProbSmoothState> = HashMap::new();
    let mut emitted_rounds: HashMap<String, i64> = HashMap::new();
    let mut round_buffers: HashMap<String, RoundBuffer> = HashMap::new();
    let mut target_cache: HashMap<String, (i64, f64)> = HashMap::new();
    let mut target_retry_after_ms: HashMap<String, i64> = HashMap::new();
    let mut target_anchor_by_round: HashMap<String, f64> = HashMap::new();
    let mut stale_tokyo_warn_by_symbol: HashMap<String, i64> = HashMap::new();
    let mut stale_book_warn_by_pair: HashMap<String, i64> = HashMap::new();
    let mut round_finalize_defer_warn_at_ms: HashMap<String, i64> = HashMap::new();
    let mut committed_rounds: HashMap<String, i64> = HashMap::new();
    let mut discovery_empty_since_ms: Option<i64> = None;
    let mut switch_epoch_by_pair: HashMap<String, u64> = HashMap::new();
    let mut switch_market_by_pair: HashMap<String, String> = HashMap::new();
    let mut switch_last_change_ms_by_pair: HashMap<String, i64> = HashMap::new();
    let mut switch_expected_start_by_pair: HashMap<String, i64> = HashMap::new();
    let mut switch_guard_warn_by_pair: HashMap<String, i64> = HashMap::new();
    let mut switch_count_by_pair: HashMap<String, u64> = HashMap::new();
    let mut switch_guard_suppress_count_by_pair: HashMap<String, u64> = HashMap::new();
    let mut sampling_gap_count_by_pair: HashMap<String, u64> = HashMap::new();
    let mut book_fallback_count_by_pair: HashMap<String, u64> = HashMap::new();

    let mut ingest_seq: u64 = 0;
    let mut last_sample_time_ms: i64 = 0;
    let mut sample_gap_warn_by_pair: HashMap<String, i64> = HashMap::new();

    let mut ticker = tokio::time::interval(Duration::from_millis(sample_period_ms as u64));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut next_housekeeping_ms = Utc::now()
        .timestamp_millis()
        .saturating_add(RECORDER_HOUSEKEEPING_INTERVAL_MS);
    let mut next_switch_stats_report_ms = Utc::now()
        .timestamp_millis()
        .saturating_add(SWITCH_STATS_REPORT_INTERVAL_MS);

    loop {
        tokio::select! {
            Some(msg) = tokyo_rx.recv() => {
                let now = Utc::now().timestamp_millis();
                tokyo_by_symbol.insert(msg.symbol.clone(), TokyoBinanceLocal {
                    ts_tokyo_recv_ms: msg.ts_tokyo_recv_ms,
                    ts_tokyo_send_ms: msg.ts_tokyo_send_ms,
                    ts_exchange_ms: msg.ts_exchange_ms,
                    ts_ireland_recv_ms: now,
                    binance_price: msg.binance_price,
                });
            }
            Some(msg) = chainlink_rx.recv() => {
                chainlink_by_symbol.insert(msg.symbol.clone(), msg);
            }
            Some(book) = book_rx.recv() => {
                book_by_market.insert(book.market_id.clone(), book);
            }
            Some(markets) = market_rx.recv() => {
                let received = markets.len();
                let now_ms = Utc::now().timestamp_millis();
                let prev_markets = markets_by_id.clone();
                let prev_primary_by_pair: HashMap<String, MarketMeta> = prev_markets
                    .values()
                    .map(|m| (market_pair_key(m), m.clone()))
                    .collect();
                let prev_candidate_markets_by_pair = candidate_markets_by_pair.clone();
                markets_by_id.clear();
                for m in markets {
                    if market_filter.allows(&m.symbol, &m.timeframe) {
                        markets_by_id.insert(m.market_id.clone(), m);
                    }
                }
                let mut pair_seen: HashSet<String> = markets_by_id
                    .values()
                    .map(|m| format!("{}:{}", m.symbol, m.timeframe))
                    .collect();
                let mut pair_has_warmable_candidate: HashMap<String, bool> = HashMap::new();
                for market in markets_by_id.values() {
                    let pair_key = market_pair_key(market);
                    let warmable = market_candidate_is_warmable(
                        market,
                        now_ms,
                        market_selection_fallback_prewarm_ms,
                        market_stale_guard_ms,
                    );
                    pair_has_warmable_candidate
                        .entry(pair_key)
                        .and_modify(|v| *v |= warmable)
                        .or_insert(warmable);
                }
                let mut sticky_reused = 0usize;
                for prev in prev_markets.values() {
                    if !market_filter.allows(&prev.symbol, &prev.timeframe) {
                        continue;
                    }
                    let pair_key = market_pair_key(prev);
                    if pair_seen.contains(&pair_key)
                        && pair_has_warmable_candidate
                            .get(&pair_key)
                            .copied()
                            .unwrap_or(false)
                    {
                        continue;
                    }
                    // Discovery can temporarily miss active markets; keep the most recent
                    // still-valid market meta to avoid short data gaps on critical pairs.
                    if !market_candidate_is_warmable(
                        prev,
                        now_ms,
                        market_selection_fallback_prewarm_ms,
                        market_stale_guard_ms,
                    ) {
                        continue;
                    }
                    markets_by_id.insert(prev.market_id.clone(), prev.clone());
                    pair_seen.insert(pair_key.clone());
                    let prev_warmable = market_candidate_is_warmable(
                        prev,
                        now_ms,
                        market_selection_fallback_prewarm_ms,
                        market_stale_guard_ms,
                    );
                    pair_has_warmable_candidate
                        .entry(pair_key)
                        .and_modify(|v| *v |= prev_warmable)
                        .or_insert(prev_warmable);
                    sticky_reused = sticky_reused.saturating_add(1);
                }
                let mut candidates_by_pair: HashMap<String, Vec<MarketMeta>> = HashMap::new();
                for market in markets_by_id.values() {
                    let pair_key = market_pair_key(market);
                    candidates_by_pair
                        .entry(pair_key)
                        .or_default()
                        .push(market.clone());
                }
                let mut cache_repaired = 0usize;
                // Keep candidates aligned with feed_polymarket subscription IDs as well.
                // This prevents stale gaps when discovery and WS subscription briefly diverge.
                if let Ok(cache_markets) =
                    discover_markets_from_target_cache(&subscribe_symbols, &subscribe_tfs).await
                {
                    for market in cache_markets {
                        if !market_filter.allows(&market.symbol, &market.timeframe) {
                            continue;
                        }
                        let pair_key = market_pair_key(&market);
                        let entry = candidates_by_pair.entry(pair_key).or_default();
                        if entry.iter().any(|m| m.market_id == market.market_id) {
                            continue;
                        }
                        entry.push(market);
                        cache_repaired = cache_repaired.saturating_add(1);
                    }
                }
                for markets in candidates_by_pair.values_mut() {
                    trim_candidate_pool(
                        markets,
                        now_ms,
                        market_prestart_allow_ms,
                        market_sample_end_grace_ms,
                        market_candidate_pool_size,
                    );
                }
                let mut selected_by_pair: HashMap<String, MarketMeta> = HashMap::new();
                for pair_key in &required_pair_keys {
                    let Some(markets) = candidates_by_pair.get(pair_key) else {
                        continue;
                    };
                    if let Some(snapshot) = resolve_switch_snapshot(
                        pair_key,
                        markets,
                        now_ms,
                        market_prestart_allow_ms,
                        market_selection_fallback_prewarm_ms,
                        market_sample_end_grace_ms,
                        market_stale_guard_ms,
                    ) {
                        selected_by_pair.insert(pair_key.clone(), snapshot.active_market);
                    }
                }
                let mut missing_required_pairs: HashSet<String> = required_pair_keys
                    .iter()
                    .filter(|pair| !selected_by_pair.contains_key(*pair))
                    .cloned()
                    .collect();
                if !missing_required_pairs.is_empty() {
                    let mut repaired_pairs = Vec::<String>::new();
                    for pair in missing_required_pairs.clone() {
                        let fallback = prev_candidate_markets_by_pair
                            .get(&pair)
                            .and_then(|prev_candidates| {
                                resolve_switch_snapshot(
                                    &pair,
                                    prev_candidates,
                                    now_ms,
                                    market_prestart_allow_ms,
                                    market_selection_fallback_prewarm_ms,
                                    market_sample_end_grace_ms,
                                    market_stale_guard_ms,
                                )
                            })
                            .map(|snapshot| snapshot.active_market);
                        if let Some(best_effort) = fallback {
                            selected_by_pair.insert(pair.clone(), best_effort.clone());
                            candidates_by_pair
                                .entry(pair.clone())
                                .or_default()
                                .push(best_effort);
                            repaired_pairs.push(pair);
                        }
                    }
                    missing_required_pairs = required_pair_keys
                        .iter()
                        .filter(|pair| !selected_by_pair.contains_key(*pair))
                        .cloned()
                        .collect();
                    // If discovery and cache are both temporarily sparse, keep required pairs alive
                    // with previous still-valid primary markets.
                    if !missing_required_pairs.is_empty() {
                        let mut prev_repaired_pairs = Vec::<String>::new();
                        for pair in missing_required_pairs.clone() {
                            let Some(prev) = prev_primary_by_pair.get(&pair) else {
                                continue;
                            };
                            if !market_candidate_is_warmable(
                                prev,
                                now_ms,
                                market_selection_fallback_prewarm_ms,
                                market_stale_guard_ms,
                            ) {
                                continue;
                            }
                            selected_by_pair.insert(pair.clone(), prev.clone());
                            markets_by_id.insert(prev.market_id.clone(), prev.clone());
                            let entry = candidates_by_pair.entry(pair.clone()).or_default();
                            if !entry.iter().any(|m| m.market_id == prev.market_id) {
                                entry.push(prev.clone());
                            }
                            prev_repaired_pairs.push(pair);
                        }
                        repaired_pairs.extend(prev_repaired_pairs.into_iter().map(|v| format!("{v}:prev")));
                    }
                    missing_required_pairs = required_pair_keys
                        .iter()
                        .filter(|pair| !selected_by_pair.contains_key(*pair))
                        .cloned()
                        .collect();
                    log_ingest(
                        &persist_tx,
                        "warn",
                        "discovery",
                        &format!(
                            "required pairs missing after selection: {} (best_effort_repaired={})",
                            missing_required_pairs
                                .iter()
                                .cloned()
                                .collect::<Vec<_>>()
                                .join(","),
                            repaired_pairs.join(",")
                        ),
                    );
                }
                let selected_empty_from_discovery = selected_by_pair.is_empty();
                let mut discovery_keepalive_reused = false;
                let mut discovery_keepalive_elapsed_ms = 0_i64;
                if selected_empty_from_discovery {
                    let since = *discovery_empty_since_ms.get_or_insert(now_ms);
                    discovery_keepalive_elapsed_ms = now_ms.saturating_sub(since);
                    let valid_prev_primary_by_pair = prev_primary_by_pair
                        .iter()
                        .filter_map(|(pair, market)| {
                            if market_candidate_is_warmable(
                                market,
                                now_ms,
                                market_selection_fallback_prewarm_ms,
                                market_stale_guard_ms,
                            ) {
                                Some((pair.clone(), market.clone()))
                            } else {
                                None
                            }
                        })
                        .collect::<HashMap<_, _>>();
                    if !valid_prev_primary_by_pair.is_empty()
                        && discovery_keepalive_elapsed_ms <= discovery_empty_grace_ms
                    {
                        selected_by_pair = valid_prev_primary_by_pair;
                        markets_by_id.clear();
                        for market in selected_by_pair.values() {
                            markets_by_id.insert(market.market_id.clone(), market.clone());
                        }
                        for (pair, prev_candidates) in prev_candidate_markets_by_pair {
                            let valid_candidates = prev_candidates
                                .into_iter()
                                .filter(|market| {
                                    market_candidate_is_warmable(
                                        market,
                                        now_ms,
                                        market_selection_fallback_prewarm_ms,
                                        market_stale_guard_ms,
                                    )
                                })
                                .collect::<Vec<_>>();
                            if valid_candidates.is_empty() {
                                continue;
                            }
                            if candidates_by_pair.is_empty() {
                                candidates_by_pair.insert(pair.clone(), valid_candidates);
                            } else {
                                candidates_by_pair.entry(pair).or_insert(valid_candidates);
                            }
                        }
                        discovery_keepalive_reused = true;
                    }
                } else {
                    discovery_empty_since_ms = None;
                }
                if selected_by_pair.len() != markets_by_id.len() {
                    let mut reduced: HashMap<String, MarketMeta> = HashMap::new();
                    for market in selected_by_pair.values() {
                        reduced.insert(market.market_id.clone(), market.clone());
                    }
                    markets_by_id = reduced;
                }
                for markets in candidates_by_pair.values_mut() {
                    trim_candidate_pool(
                        markets,
                        now_ms,
                        market_prestart_allow_ms,
                        market_sample_end_grace_ms,
                        market_candidate_pool_size,
                    );
                }
                candidate_markets_by_pair = candidates_by_pair;
                let mut kept_pairs = markets_by_id
                    .values()
                    .map(|m| format!("{}:{}", m.symbol, m.timeframe))
                    .collect::<Vec<_>>();
                kept_pairs.sort();
                kept_pairs.dedup();
                let mut selected_rounds = selected_by_pair
                    .values()
                    .map(|m| {
                        format!(
                            "{}:{}:{}->{}(start_s={},end_s={})",
                            m.symbol,
                            m.timeframe,
                            m.start_ts_ms,
                            m.end_ts_ms,
                            m.start_ts_ms.saturating_sub(now_ms) / 1_000,
                            m.end_ts_ms.saturating_sub(now_ms) / 1_000
                        )
                    })
                    .collect::<Vec<_>>();
                selected_rounds.sort();
                tracing::info!(
                    received_markets = received,
                    kept_markets = markets_by_id.len(),
                    sticky_reused = sticky_reused,
                    cache_repaired = cache_repaired,
                    discovery_keepalive_reused = discovery_keepalive_reused,
                    discovery_keepalive_elapsed_ms = discovery_keepalive_elapsed_ms,
                    kept_pairs = kept_pairs.join(","),
                    selected_rounds = selected_rounds.join(";"),
                    "market meta update applied"
                );
            }
            Some(res) = target_res_rx.recv() => {
                if let Some(v) = res.target_price {
                    let now_ms = Utc::now().timestamp_millis();
                    target_cache.insert(res.cache_key.clone(), (now_ms, v));
                    target_retry_after_ms.remove(&res.cache_key);
                } else {
                    target_retry_after_ms.insert(
                        res.cache_key,
                        Utc::now().timestamp_millis().saturating_add(TARGET_RETRY_BACKOFF_MS),
                    );
                }
            }
            _ = ticker.tick() => {
                let now_ms = Utc::now().timestamp_millis();
                prob_smooth_by_round.retain(|_, v| {
                    now_ms.saturating_sub(v.ts_ms) <= PROB_STATE_RETENTION_MS
                });
                for buffer in round_buffers.values_mut() {
                    maybe_update_round_settle_from_tokyo(buffer, &tokyo_by_symbol);
                }

                // Gap detection: warn if sampling interval exceeds threshold
                if last_sample_time_ms > 0 {
                    let gap_ms = now_ms.saturating_sub(last_sample_time_ms);
                    if gap_ms > SAMPLE_GAP_WARN_MS {
                        for pair_key in required_pair_keys.iter() {
                            if !candidate_markets_by_pair.contains_key(pair_key) {
                                continue;
                            }
                            let warn_at = sample_gap_warn_by_pair
                                .entry(pair_key.to_string())
                                .or_insert(0);
                            sampling_gap_count_by_pair
                                .entry(pair_key.to_string())
                                .and_modify(|v| *v = v.saturating_add(1))
                                .or_insert(1);
                            if now_ms.saturating_sub(*warn_at) >= INPUT_STALE_WARN_THROTTLE_MS {
                                log_ingest(
                                    &persist_tx,
                                    "warn",
                                    "sampling_gap",
                                    &format!(
                                        "sample_gap_detected pair={} gap_ms={} threshold_ms={}",
                                        pair_key, gap_ms, SAMPLE_GAP_WARN_MS
                                    ),
                                );
                                *warn_at = now_ms;
                            }
                        }
                    }
                }

                let mut sampling_pairs = required_pair_keys.iter().cloned().collect::<Vec<_>>();
                sampling_pairs.sort();
                for pair_key in sampling_pairs {
                    let fallback_candidates;
                    let pair_candidates = if let Some(existing) = candidate_markets_by_pair.get(&pair_key) {
                        existing.as_slice()
                    } else {
                        let mut split = pair_key.split(':');
                        let symbol = split.next().unwrap_or_default();
                        let timeframe = split.next().unwrap_or_default();
                        fallback_candidates = markets_by_id
                            .values()
                            .filter(|market| {
                                market.symbol.eq_ignore_ascii_case(symbol)
                                    && market.timeframe.eq_ignore_ascii_case(timeframe)
                            })
                            .cloned()
                            .collect::<Vec<_>>();
                        if fallback_candidates.is_empty() {
                            continue;
                        }
                        fallback_candidates.as_slice()
                    };

                    let Some(switch_snapshot) = resolve_switch_snapshot(
                        &pair_key,
                        pair_candidates,
                        now_ms,
                        market_prestart_allow_ms,
                        market_selection_fallback_prewarm_ms,
                        market_sample_end_grace_ms,
                        market_stale_guard_ms,
                    ) else {
                        continue;
                    };

                    let mut market = switch_snapshot.active_market.clone();
                    if let Some(previous_market_id) = switch_market_by_pair.get(&pair_key) {
                        if previous_market_id != &market.market_id {
                            let previous_market = pair_candidates
                                .iter()
                                .find(|candidate| candidate.market_id == *previous_market_id);
                            if let Some(previous_market) = previous_market {
                                let same_round = previous_market.start_ts_ms == market.start_ts_ms;
                                let previous_expected_start = switch_expected_start_by_pair
                                    .get(&pair_key)
                                    .copied()
                                    .unwrap_or(i64::MIN);
                                let expected_round_changed =
                                    previous_expected_start != switch_snapshot.expected_start_ms;
                                let recently_switched = switch_last_change_ms_by_pair
                                    .get(&pair_key)
                                    .copied()
                                    .map(|ts| now_ms.saturating_sub(ts) < market_switch_min_hold_ms)
                                    .unwrap_or(false);
                                let previous_still_warmable = market_in_sampling_window(
                                    previous_market,
                                    now_ms,
                                    market_prestart_allow_ms,
                                    market_sample_end_grace_ms,
                                );
                                let guard_hit = previous_still_warmable
                                    && (same_round
                                        || (!expected_round_changed && recently_switched));
                                if guard_hit {
                                    market = previous_market.clone();
                                    switch_guard_suppress_count_by_pair
                                        .entry(pair_key.clone())
                                        .and_modify(|v| *v = v.saturating_add(1))
                                        .or_insert(1);
                                    let warn_at =
                                        switch_guard_warn_by_pair.entry(pair_key.clone()).or_insert(0);
                                    if now_ms.saturating_sub(*warn_at)
                                        >= SWITCH_GUARD_LOG_THROTTLE_MS
                                    {
                                        let reason = if same_round {
                                            "same_round_sticky"
                                        } else {
                                            "min_hold_guard"
                                        };
                                        log_ingest(
                                            &persist_tx,
                                            "info",
                                            "market_switch_guard",
                                            &format!(
                                                "pair={} keep_market_id={} reject_market_id={} reason={} hold_ms={} expected_start_ms={}",
                                                pair_key,
                                                previous_market.market_id,
                                                switch_snapshot.active_market.market_id,
                                                reason,
                                                market_switch_min_hold_ms,
                                                switch_snapshot.expected_start_ms,
                                            ),
                                        );
                                        *warn_at = now_ms;
                                    }
                                }
                            }
                        }
                    }

                    let book = pick_fresh_book_for_market(&market, now_ms, &book_by_market);

                    let future_sampling_allowed = now_ms + market_prestart_allow_ms < market.start_ts_ms
                        && book.is_some();
                    if now_ms + market_prestart_allow_ms < market.start_ts_ms && !future_sampling_allowed {
                        continue;
                    }
                    if now_ms > market.end_ts_ms.saturating_add(market_sample_end_grace_ms) {
                        continue;
                    }

                    let switch_epoch = {
                        let previous = switch_market_by_pair.get(&pair_key);
                        if previous != Some(&market.market_id) {
                            let epoch = switch_epoch_by_pair
                                .entry(pair_key.clone())
                                .and_modify(|v| *v = v.saturating_add(1))
                                .or_insert(1);
                            let next_id = switch_snapshot
                                .next_market
                                .as_ref()
                                .map(|m| m.market_id.as_str())
                                .unwrap_or("-");
                            let next2_id = switch_snapshot
                                .next2_market
                                .as_ref()
                                .map(|m| m.market_id.as_str())
                                .unwrap_or("-");
                            log_ingest(
                                &persist_tx,
                                "info",
                                "market_switch",
                                &format!(
                                    "pair={} epoch={} market_id={} expected_start_ms={} phase={:?} degraded={} reason={} next_market_id={} next2_market_id={}",
                                    switch_snapshot.pair_key,
                                    *epoch,
                                    market.market_id,
                                    switch_snapshot.expected_start_ms,
                                    switch_snapshot.phase,
                                    switch_snapshot.degraded,
                                    switch_snapshot.reason,
                                    next_id,
                                    next2_id,
                                ),
                            );
                            switch_market_by_pair.insert(pair_key.clone(), market.market_id.clone());
                            switch_last_change_ms_by_pair.insert(pair_key.clone(), now_ms);
                            switch_count_by_pair
                                .entry(pair_key.clone())
                                .and_modify(|v| *v = v.saturating_add(1))
                                .or_insert(1);
                        }
                        switch_expected_start_by_pair
                            .insert(pair_key.clone(), switch_snapshot.expected_start_ms);
                        *switch_epoch_by_pair.entry(pair_key.clone()).or_insert(0)
                    };

                    let max_quote_fallback_age_ms = quote_fallback_max_age_ms(&market.timeframe);
                    let market_cached_quote = quote_cache_by_market.get(&market.market_id);
                    let pair_cached_quote = quote_cache_by_pair.get(&pair_key);
                    let market_cached_quote_sides = market_cached_quote.map(|v| {
                        (
                            v.quote.bid_yes,
                            v.quote.ask_yes,
                            v.quote.bid_no,
                            v.quote.ask_no,
                        )
                    });
                    let market_cached_ts_ms = market_cached_quote.map(|v| v.ts_ms);
                    let pair_cached_ts_ms = pair_cached_quote.map(|v| v.ts_ms);
                    let mut used_cached_quote = false;
                    let stable_quote = match book {
                        Some(book_top) => {
                            stabilize_book_quotes(book_top, market_cached_quote_sides).or_else(|| {
                            used_cached_quote = true;
                            pick_fresh_cached_quote(
                                &market.market_id,
                                market_cached_quote,
                                pair_cached_quote,
                                now_ms,
                                max_quote_fallback_age_ms,
                            )
                        })
                        }
                        None => {
                            used_cached_quote = true;
                            pick_fresh_cached_quote(
                                &market.market_id,
                                market_cached_quote,
                                pair_cached_quote,
                                now_ms,
                                max_quote_fallback_age_ms,
                            )
                        }
                    };
                    let Some(stable_quote) = stable_quote else {
                        if book.is_some() {
                            log_ingest(
                                &persist_tx,
                                "warn",
                                "book_quality",
                                &format!("invalid book skipped market_id={}", market.market_id),
                            );
                        }
                        continue;
                    };
                    if used_cached_quote {
                        book_fallback_count_by_pair
                            .entry(pair_key.clone())
                            .and_modify(|v| *v = v.saturating_add(1))
                            .or_insert(1);
                        let warn_at = stale_book_warn_by_pair.entry(pair_key.clone()).or_insert(0);
                        if now_ms.saturating_sub(*warn_at) >= INPUT_STALE_WARN_THROTTLE_MS {
                            log_ingest(
                                &persist_tx,
                                "warn",
                                "book_quality",
                                &format!(
                                    "book_missing_fallback_used symbol={} tf={} market_id={} fallback_age_limit_ms={}",
                                    market.symbol,
                                    market.timeframe,
                                    market.market_id,
                                    max_quote_fallback_age_ms
                                ),
                            );
                            *warn_at = now_ms;
                        }
                    } else {
                        let cache_entry = QuoteCacheEntry {
                            market_id: market.market_id.clone(),
                            ts_ms: now_ms,
                            quote: stable_quote,
                        };
                        quote_cache_by_market
                            .insert(market.market_id.clone(), cache_entry.clone());
                        quote_cache_by_pair.insert(pair_key.clone(), cache_entry);
                    }

                    let tokyo = tokyo_by_symbol.get(&market.symbol);
                    let chainlink = chainlink_by_symbol.get(&market.symbol);
                    let tokyo_fresh = tokyo.filter(|row| {
                        now_ms.saturating_sub(row.ts_ireland_recv_ms)
                            <= tokyo_input_stale_guard_ms
                    });
                    if tokyo_fresh.is_none() {
                        let warn_at = stale_tokyo_warn_by_symbol
                            .entry(market.symbol.clone())
                            .or_insert(0);
                        if now_ms.saturating_sub(*warn_at) >= INPUT_STALE_WARN_THROTTLE_MS {
                            let age_ms = tokyo
                                .map(|row| now_ms.saturating_sub(row.ts_ireland_recv_ms))
                                .unwrap_or(i64::MAX);
                            log_ingest(
                                &persist_tx,
                                "warn",
                                "tokyo_price",
                                &format!(
                                    "tokyo_price_unavailable symbol={} tf={} age_ms={} guard_ms={}",
                                    market.symbol,
                                    market.timeframe,
                                    age_ms,
                                    tokyo_input_stale_guard_ms
                                ),
                            );
                            *warn_at = now_ms;
                        }
                    }
                    let binance_price = tokyo_fresh
                        .map(|v| v.binance_price)
                        .filter(|v| v.is_finite() && *v > 0.0);
                    let round_start = market.start_ts_ms;
                    let round_id = format!("{}_{}_{}", market.symbol, market.timeframe, round_start);
                    let mut target_price = market.target_price;
                    if target_price.is_none() {
                        let cache_key =
                            format!("{}|{}|{}", market.symbol, market.timeframe, market.start_ts_ms);
                        if let Some((cached_at, cached_price)) = target_cache.get(&cache_key).copied() {
                            if now_ms.saturating_sub(cached_at) <= 60_000 {
                                target_price = Some(cached_price);
                            }
                        }
                        let retry_after = target_retry_after_ms.get(&cache_key).copied().unwrap_or(0);
                        if target_price.is_none() && now_ms >= retry_after {
                            // Chainlink fallback is optional and currently disabled by default.
                            if chainlink_enabled {
                                if let Some(v) = pick_official_chainlink_target(
                                    chainlink,
                                    market.start_ts_ms,
                                    &market.timeframe,
                                ) {
                                    target_cache.insert(cache_key.clone(), (now_ms, v));
                                    target_retry_after_ms.remove(&cache_key);
                                    target_price = Some(v);
                                    log_ingest(
                                        &persist_tx,
                                        "warn",
                                        "target_price",
                                        &format!(
                                            "source=chainlink_official_fallback symbol={} tf={} start_ms={} target={}",
                                            market.symbol, market.timeframe, market.start_ts_ms, v
                                        ),
                                    );
                                } else {
                                    // Offload target fetch to background worker to keep ticker non-blocking.
                                    let _ = target_req_tx.send(TargetFetchReq {
                                        cache_key: cache_key.clone(),
                                        symbol: market.symbol.clone(),
                                        timeframe: market.timeframe.clone(),
                                        start_ts_ms: market.start_ts_ms,
                                    });
                                    // Rate-limit follow-up fetches for the same key.
                                    target_retry_after_ms.insert(
                                        cache_key,
                                        now_ms.saturating_add(TARGET_RETRY_BACKOFF_MS),
                                    );
                                }
                            } else {
                                // Offload target fetch to background worker to keep ticker non-blocking.
                                let _ = target_req_tx.send(TargetFetchReq {
                                    cache_key: cache_key.clone(),
                                    symbol: market.symbol.clone(),
                                    timeframe: market.timeframe.clone(),
                                    start_ts_ms: market.start_ts_ms,
                                });
                                // Rate-limit follow-up fetches for the same key.
                                target_retry_after_ms.insert(
                                    cache_key,
                                    now_ms.saturating_add(TARGET_RETRY_BACKOFF_MS),
                                );
                            }
                        }
                    }
                    if target_price.is_none() {
                        if let Some(anchor) = target_anchor_by_round.get(&round_id).copied() {
                            target_price = Some(anchor);
                        }
                    } else if let Some(target) = target_price
                        .filter(|v| v.is_finite() && *v > 0.0)
                    {
                        target_anchor_by_round.insert(round_id.clone(), target);
                    }
                    if switch_snapshot.degraded {
                        let warn_at = stale_book_warn_by_pair.entry(pair_key.clone()).or_insert(0);
                        if now_ms.saturating_sub(*warn_at) >= INPUT_STALE_WARN_THROTTLE_MS {
                            log_ingest(
                                &persist_tx,
                                "warn",
                                "market_switch",
                                &format!(
                                    "pair={} epoch={} degraded_active_market market_id={} reason={} phase={:?}",
                                    pair_key,
                                    switch_epoch,
                                    market.market_id,
                                    switch_snapshot.reason,
                                    switch_snapshot.phase,
                                ),
                            );
                            *warn_at = now_ms;
                        }
                    }

                    let delta_price = match (binance_price, target_price) {
                        (Some(px), Some(target)) => Some(px - target),
                        _ => None,
                    };
                    let delta_pct = match (delta_price, target_price) {
                        (Some(d), Some(target)) if target > 0.0 => Some((d / target) * 100.0),
                        _ => None,
                    };
                    let prev_prob_state = prob_smooth_by_round.get(&round_id).copied();
                    let raw_mid_yes_norm =
                        fused_up_probability(&stable_quote, prev_prob_state.map(|s| s.ema_up));
                    let raw_mid_no_norm = (1.0 - raw_mid_yes_norm).clamp(0.0, 1.0);
                    let (mid_yes_smooth, mid_no_smooth, delta_pct_smooth) = {
                        if let Some(prev) = prev_prob_state {
                            let dt_ms = now_ms.saturating_sub(prev.ts_ms);
                            let dt_s_raw = (dt_ms as f64 / 1000.0).max(0.0);
                            let dt_s = dt_s_raw.clamp(MOTION_MIN_DT_SEC, MOTION_MAX_DT_SEC);
                            let remaining_ms_now =
                                market.end_ts_ms.saturating_sub(now_ms).max(0);
                            let prob_tau = if remaining_ms_now <= 30_000 {
                                0.9
                            } else if remaining_ms_now <= 60_000 {
                                1.8
                            } else {
                                PROB_SMOOTH_TAU_SEC
                            };
                            let delta_tau = if remaining_ms_now <= 30_000 {
                                0.9
                            } else {
                                DELTA_SMOOTH_TAU_SEC
                            };
                            let alpha_prob = ema_alpha_from_tau(dt_s, prob_tau);
                            let step_cap_per_sec = if remaining_ms_now <= 30_000 {
                                PROB_RAW_MAX_STEP_NEAR_CLOSE_PER_SEC
                            } else if remaining_ms_now <= 60_000 {
                                PROB_RAW_MAX_STEP_MID_CLOSE_PER_SEC
                            } else {
                                PROB_RAW_MAX_STEP_PER_SEC
                            };
                            let max_raw_step = step_cap_per_sec * dt_s;
                            let gated_raw_up = raw_mid_yes_norm.clamp(
                                prev.ema_up - max_raw_step,
                                prev.ema_up + max_raw_step,
                            );
                            let ema_up =
                                (prev.ema_up + alpha_prob * (gated_raw_up - prev.ema_up))
                                    .clamp(0.0, 1.0);
                            let ema_delta = match (delta_pct, prev.ema_delta_pct) {
                                (Some(raw), Some(prev_d)) if raw.is_finite() && prev_d.is_finite() => {
                                    let alpha_delta = ema_alpha_from_tau(dt_s, delta_tau);
                                    let mut next = prev_d + alpha_delta * (raw - prev_d);
                                    let max_step = DELTA_MAX_STEP_PCT_PER_SEC * dt_s;
                                    next = next.clamp(prev_d - max_step, prev_d + max_step);
                                    Some(next)
                                }
                                (Some(raw), _) if raw.is_finite() => Some(raw),
                                (None, Some(prev_d)) if prev_d.is_finite() => Some(prev_d),
                                _ => None,
                            };
                            prob_smooth_by_round.insert(
                                round_id.clone(),
                                ProbSmoothState {
                                    ts_ms: now_ms,
                                    ema_up,
                                    ema_delta_pct: ema_delta,
                                },
                            );
                            (ema_up, (1.0 - ema_up).clamp(0.0, 1.0), ema_delta)
                        } else {
                            prob_smooth_by_round.insert(
                                round_id.clone(),
                                ProbSmoothState {
                                    ts_ms: now_ms,
                                    ema_up: raw_mid_yes_norm,
                                    ema_delta_pct: delta_pct,
                                },
                            );
                            (raw_mid_yes_norm, raw_mid_no_norm, delta_pct)
                        }
                    };

                    let motion_key = format!("{}|{}", market.symbol, market.timeframe);
                    let (velocity, acceleration) = match binance_price {
                        Some(px) if px.is_finite() && px > 0.0 => {
                            if let Some(prev) = motion_by_key.get(&motion_key).copied() {
                                let dt_ms = now_ms.saturating_sub(prev.ts_ms);
                                if dt_ms > 0 {
                                    let dt_s_raw = dt_ms as f64 / 1000.0;
                                    let dt_s = dt_s_raw.clamp(MOTION_MIN_DT_SEC, MOTION_MAX_DT_SEC);
                                    let prev_ema_price = prev.ema_price.max(1e-9);
                                    let alpha_price = ema_alpha_from_tau(dt_s, MOTION_PRICE_TAU_SEC);
                                    let ema_price = prev_ema_price
                                        + alpha_price * (px - prev_ema_price);
                                    let vel_raw =
                                        ((ema_price - prev_ema_price) / prev_ema_price) * 10_000.0
                                            / dt_s;
                                    let vel_raw =
                                        vel_raw.clamp(-MOTION_VELOCITY_ABS_CAP, MOTION_VELOCITY_ABS_CAP);
                                    let alpha_vel = ema_alpha_from_tau(dt_s, MOTION_VELOCITY_TAU_SEC);
                                    let ema_vel = prev.ema_velocity
                                        + alpha_vel * (vel_raw - prev.ema_velocity);
                                    let ema_vel =
                                        ema_vel.clamp(-MOTION_VELOCITY_ABS_CAP, MOTION_VELOCITY_ABS_CAP);
                                    let acc_raw = (ema_vel - prev.ema_velocity) / dt_s;
                                    let acc =
                                        acc_raw.clamp(-MOTION_ACCEL_ABS_CAP, MOTION_ACCEL_ABS_CAP);
                                    motion_by_key.insert(
                                        motion_key,
                                        MotionState {
                                            ts_ms: now_ms,
                                            ema_price,
                                            ema_velocity: ema_vel,
                                        },
                                    );
                                    (Some(ema_vel), Some(acc))
                                } else {
                                    (Some(prev.ema_velocity), Some(0.0))
                                }
                            } else {
                                motion_by_key.insert(
                                    motion_key,
                                    MotionState {
                                        ts_ms: now_ms,
                                        ema_price: px,
                                        ema_velocity: 0.0,
                                    },
                                );
                                (Some(0.0), Some(0.0))
                            }
                        }
                        _ => (None, None),
                    };

                    if emitted_rounds.contains_key(&round_id) {
                        continue;
                    }

                    let ts_pm_recv_ms = if let Some(book_top) = book {
                        if book_top.recv_ts_local_ns > 0 {
                            book_top.recv_ts_local_ns / 1_000_000
                        } else {
                            book_top.ts_ms
                        }
                    } else {
                        market_cached_ts_ms.or(pair_cached_ts_ms).unwrap_or(now_ms)
                    };

                    ingest_seq = ingest_seq.saturating_add(1);
                    let row = SnapshotRow {
                        schema_version: "forge_snapshot_v1",
                        ingest_seq,
                        ts_ireland_sample_ms: now_ms,
                        ts_tokyo_recv_ms: tokyo_fresh.map(|v| v.ts_tokyo_recv_ms),
                        ts_tokyo_send_ms: tokyo_fresh.map(|v| v.ts_tokyo_send_ms),
                        ts_exchange_ms: tokyo_fresh.map(|v| v.ts_exchange_ms),
                        ts_ireland_recv_ms: tokyo_fresh.map(|v| v.ts_ireland_recv_ms),
                        tokyo_relay_proc_lag_ms: tokyo_fresh.map(|v| {
                            v.ts_tokyo_send_ms.saturating_sub(v.ts_tokyo_recv_ms) as f64
                        }),
                        cross_region_net_lag_ms: tokyo_fresh.map(|v| {
                            v.ts_ireland_recv_ms.saturating_sub(v.ts_tokyo_send_ms) as f64
                        }),
                        path_lag_ms: tokyo_fresh
                            .map(|v| v.ts_ireland_recv_ms.saturating_sub(v.ts_tokyo_recv_ms) as f64),
                        symbol: market.symbol.clone(),
                        ts_pm_recv_ms,
                        market_id: market.market_id.clone(),
                        timeframe: market.timeframe.clone(),
                        title: market.title.clone(),
                        target_price,
                        mid_yes: raw_mid_yes_norm,
                        mid_no: raw_mid_no_norm,
                        mid_yes_smooth,
                        mid_no_smooth,
                        bid_yes: stable_quote.bid_yes,
                        ask_yes: stable_quote.ask_yes,
                        bid_no: stable_quote.bid_no,
                        ask_no: stable_quote.ask_no,
                        binance_price,
                        pm_live_btc_price: chainlink.map(|v| v.price),
                        ts_pm_live_exchange_ms: chainlink.map(|v| v.ts_exchange_ms),
                        ts_pm_live_recv_ms: chainlink.map(|v| v.ts_ireland_recv_ms),
                        chainlink_price: chainlink.map(|v| v.price),
                        ts_chainlink_exchange_ms: chainlink.map(|v| v.ts_exchange_ms),
                        ts_chainlink_recv_ms: chainlink.map(|v| v.ts_ireland_recv_ms),
                        delta_price,
                        delta_pct,
                        delta_pct_smooth,
                        remaining_ms: market.end_ts_ms.saturating_sub(now_ms).max(0),
                        velocity_bps_per_sec: velocity,
                        acceleration,
                        round_id: round_id.clone(),
                    };
                    let round_buf = round_buffers
                        .entry(round_id.clone())
                        .or_insert_with(|| RoundBuffer::new(round_id.clone(), &market));
                    if let Some(tx) = sink_tx.as_ref() {
                        if let Err(err) = tx.send(DbEvent::Snapshot(Box::new(row.clone()))).await {
                            tracing::warn!(?err, "db sink closed while sending live snapshot");
                        }
                    }
                    round_buf.push_snapshot(row);

                    // Update last sample time for gap detection
                    last_sample_time_ms = now_ms;

                    let remaining_ms = market.end_ts_ms.saturating_sub(now_ms);
                    if remaining_ms <= 0 && !emitted_rounds.contains_key(&round_id) {
                        if let Some(mut buffer) = round_buffers.remove(&round_id) {
                            maybe_update_round_settle_from_tokyo(&mut buffer, &tokyo_by_symbol);
                            let report = buffer.evaluate(&quality_policy, sample_period_ms);
                            let target = buffer.target_price_latest.unwrap_or(0.0);
                            let settle = buffer.settle_price_latest.unwrap_or(0.0);
                            let settle_stale = report
                                .reasons
                                .iter()
                                .any(|r| r.starts_with("stale_settle="));
                            let finalize_age_ms = now_ms.saturating_sub(buffer.end_ts_ms).max(0);
                            let force_finalize = finalize_age_ms >= ROUND_FINALIZE_RETRY_GRACE_MS;
                            if !(target.is_finite() && target > 0.0 && settle.is_finite() && settle > 0.0)
                            {
                                if !force_finalize {
                                    round_buffers.insert(round_id.clone(), buffer);
                                    let warn_at = round_finalize_defer_warn_at_ms
                                        .entry(round_id.clone())
                                        .or_insert(0);
                                    if now_ms.saturating_sub(*warn_at)
                                        >= ROUND_FINALIZE_DEFER_LOG_THROTTLE_MS
                                    {
                                        log_ingest(
                                            &persist_tx,
                                            "warn",
                                            "round_quality",
                                            &format!(
                                                "defer_round_finalize {} age_ms={} target={} settle={} reason=invalid_target_or_settle",
                                                round_id, finalize_age_ms, target, settle
                                            ),
                                        );
                                        *warn_at = now_ms;
                                    }
                                    continue;
                                }
                                emitted_rounds.insert(round_id.clone(), now_ms);
                                round_finalize_defer_warn_at_ms.remove(&round_id);
                                log_ingest(
                                    &persist_tx,
                                    "warn",
                                    "round_quality",
                                    &format!(
                                        "drop_invalid_round {} age_ms={} target={} settle={} reason=invalid_target_or_settle",
                                        round_id, finalize_age_ms, target, settle
                                    ),
                                );
                                continue;
                            }
                            if settle_stale && !force_finalize {
                                round_buffers.insert(round_id.clone(), buffer);
                                let warn_at = round_finalize_defer_warn_at_ms
                                    .entry(round_id.clone())
                                    .or_insert(0);
                                if now_ms.saturating_sub(*warn_at)
                                    >= ROUND_FINALIZE_DEFER_LOG_THROTTLE_MS
                                {
                                    log_ingest(
                                        &persist_tx,
                                        "warn",
                                        "round_quality",
                                        &format!(
                                            "defer_round_finalize {} age_ms={} target={} settle={} reason=stale_settle",
                                            round_id, finalize_age_ms, target, settle
                                        ),
                                    );
                                    *warn_at = now_ms;
                                }
                                continue;
                            }

                            target_anchor_by_round.remove(&round_id);
                            prob_smooth_by_round.remove(&round_id);
                            let round = RoundRow {
                                round_id: buffer.round_id.clone(),
                                market_id: buffer.market_id.clone(),
                                symbol: buffer.symbol.clone(),
                                timeframe: buffer.timeframe.clone(),
                                title: buffer.title.clone(),
                                start_ts_ms: buffer.start_ts_ms,
                                end_ts_ms: buffer.end_ts_ms,
                                target_price: target,
                                settle_price: settle,
                                label_up: settle > target,
                                ts_recorded_ms: now_ms,
                            };
                            if committed_rounds.contains_key(&round.round_id) {
                                emitted_rounds.insert(round_id.clone(), now_ms);
                                round_finalize_defer_warn_at_ms.remove(&round_id);
                                log_ingest(
                                    &persist_tx,
                                    "warn",
                                    "round_quality",
                                    &format!(
                                        "skip_duplicate_round {} reason=already_committed_in_process",
                                        round.round_id
                                    ),
                                );
                                continue;
                            }
                            if !report.accept && round_drop_on_quality_fail {
                                emitted_rounds.insert(round_id.clone(), now_ms);
                                round_finalize_defer_warn_at_ms.remove(&round_id);
                                let reason = report.reasons.join(",");
                                log_ingest(
                                    &persist_tx,
                                    "warn",
                                    "round_quality",
                                    &format!(
                                        "drop_low_quality_round {} samples={} expected={} reachable_expected={} coverage={:.3} sample_ratio={:.3} reachable_ratio={:.3} gate_ratio={:.3} avg_gap_ms={} max_gap_ms={} start_delay_ms={} end_missing_ms={} settle_stale_ms={} reason={}",
                                        round_id,
                                        report.sample_count,
                                        report.expected_samples,
                                        report.reachable_expected_samples,
                                        report.coverage_ratio,
                                        report.sample_ratio,
                                        report.reachable_sample_ratio,
                                        report.sample_ratio_gate,
                                        report.avg_gap_ms,
                                        report.max_gap_ms,
                                        report.start_delay_ms,
                                        report.end_missing_ms,
                                        report.settle_stale_ms,
                                        reason
                                    ),
                                );
                                tracing::warn!(
                                    round_id = %round_id,
                                    accepted = report.accept,
                                    reasons = %reason,
                                    "drop round due to quality gate"
                                );
                                continue;
                            }
                            let snapshot_count = buffer.snapshots.len();
                            if let Err(err) = commit_tx
                                .send(RoundCommit {
                                    snapshots: buffer.snapshots,
                                    round,
                                })
                                .await
                            {
                                emitted_rounds.insert(round_id.clone(), now_ms);
                                round_finalize_defer_warn_at_ms.remove(&round_id);
                                tracing::error!(?err, "commit queue closed; dropping round");
                            } else {
                                emitted_rounds.insert(round_id.clone(), now_ms);
                                round_finalize_defer_warn_at_ms.remove(&round_id);
                                committed_rounds.insert(round_id.clone(), now_ms);
                                if report.accept {
                                    log_ingest(
                                        &persist_tx,
                                        "info",
                                        "round_quality",
                                        &format!(
                                            "accepted {} samples={} expected={} reachable_expected={} coverage={:.3} sample_ratio={:.3} reachable_ratio={:.3} gate_ratio={:.3} avg_gap_ms={} max_gap_ms={}",
                                            round_id,
                                            report.sample_count,
                                            report.expected_samples,
                                            report.reachable_expected_samples,
                                            report.coverage_ratio,
                                            report.sample_ratio,
                                            report.reachable_sample_ratio,
                                            report.sample_ratio_gate,
                                            report.avg_gap_ms,
                                            report.max_gap_ms
                                        ),
                                    );
                                } else {
                                    let reason = report.reasons.join(",");
                                    log_ingest(
                                        &persist_tx,
                                        "warn",
                                        "round_quality",
                                        &format!(
                                            "quality_warn {} samples={} expected={} reachable_expected={} coverage={:.3} sample_ratio={:.3} reachable_ratio={:.3} gate_ratio={:.3} avg_gap_ms={} max_gap_ms={} start_delay_ms={} end_missing_ms={} settle_stale_ms={} reason={}",
                                            round_id,
                                            report.sample_count,
                                            report.expected_samples,
                                            report.reachable_expected_samples,
                                            report.coverage_ratio,
                                            report.sample_ratio,
                                            report.reachable_sample_ratio,
                                            report.sample_ratio_gate,
                                            report.avg_gap_ms,
                                            report.max_gap_ms,
                                            report.start_delay_ms,
                                            report.end_missing_ms,
                                            report.settle_stale_ms,
                                            reason
                                        ),
                                    );
                                }
                                tracing::info!(
                                    round_id = %round_id,
                                    sample_count = snapshot_count,
                                    expected_samples = report.expected_samples,
                                    reachable_expected_samples = report.reachable_expected_samples,
                                    coverage_ratio = report.coverage_ratio,
                                    sample_ratio = report.sample_ratio,
                                    reachable_sample_ratio = report.reachable_sample_ratio,
                                    sample_ratio_gate = report.sample_ratio_gate,
                                    avg_gap_ms = report.avg_gap_ms,
                                    max_gap_ms = report.max_gap_ms,
                                    accepted = report.accept,
                                    "round committed"
                                );
                            }
                        }
                    }
                }

                let stale_round_ids: Vec<String> = round_buffers
                    .iter()
                    .filter_map(|(rid, buf)| {
                        if now_ms > buf.end_ts_ms.saturating_add(market_stale_guard_ms)
                            && !emitted_rounds.contains_key(rid)
                        {
                            Some(rid.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                for rid in stale_round_ids {
                    if let Some(mut buffer) = round_buffers.remove(&rid) {
                        maybe_update_round_settle_from_tokyo(&mut buffer, &tokyo_by_symbol);
                        let report = buffer.evaluate(&quality_policy, sample_period_ms);
                        let target = buffer.target_price_latest.unwrap_or(0.0);
                        let settle = buffer.settle_price_latest.unwrap_or(0.0);
                        let settle_stale = report
                            .reasons
                            .iter()
                            .any(|r| r.starts_with("stale_settle="));
                        let finalize_age_ms = now_ms.saturating_sub(buffer.end_ts_ms).max(0);
                        let force_finalize = finalize_age_ms >= ROUND_FINALIZE_RETRY_GRACE_MS;

                        if !(target.is_finite() && target > 0.0 && settle.is_finite() && settle > 0.0)
                        {
                            if !force_finalize {
                                round_buffers.insert(rid.clone(), buffer);
                                let warn_at =
                                    round_finalize_defer_warn_at_ms.entry(rid.clone()).or_insert(0);
                                if now_ms.saturating_sub(*warn_at)
                                    >= ROUND_FINALIZE_DEFER_LOG_THROTTLE_MS
                                {
                                    log_ingest(
                                        &persist_tx,
                                        "warn",
                                        "round_quality",
                                        &format!(
                                            "defer_stale_round_finalize {} age_ms={} target={} settle={} reason=invalid_target_or_settle",
                                            rid, finalize_age_ms, target, settle
                                        ),
                                    );
                                    *warn_at = now_ms;
                                }
                                continue;
                            }
                            emitted_rounds.insert(rid.clone(), now_ms);
                            round_finalize_defer_warn_at_ms.remove(&rid);
                            log_ingest(
                                &persist_tx,
                                "warn",
                                "round_quality",
                                &format!(
                                    "drop_invalid_stale_round {} age_ms={} target={} settle={} reason=invalid_target_or_settle",
                                    rid, finalize_age_ms, target, settle
                                ),
                            );
                            continue;
                        }
                        if settle_stale && !force_finalize {
                            round_buffers.insert(rid.clone(), buffer);
                            let warn_at = round_finalize_defer_warn_at_ms.entry(rid.clone()).or_insert(0);
                            if now_ms.saturating_sub(*warn_at)
                                >= ROUND_FINALIZE_DEFER_LOG_THROTTLE_MS
                            {
                                log_ingest(
                                    &persist_tx,
                                    "warn",
                                    "round_quality",
                                    &format!(
                                        "defer_stale_round_finalize {} age_ms={} target={} settle={} reason=stale_settle",
                                        rid, finalize_age_ms, target, settle
                                    ),
                                );
                                *warn_at = now_ms;
                            }
                            continue;
                        }

                        target_anchor_by_round.remove(&rid);
                        prob_smooth_by_round.remove(&rid);
                        let round = RoundRow {
                            round_id: buffer.round_id.clone(),
                            market_id: buffer.market_id.clone(),
                            symbol: buffer.symbol.clone(),
                            timeframe: buffer.timeframe.clone(),
                            title: buffer.title.clone(),
                            start_ts_ms: buffer.start_ts_ms,
                            end_ts_ms: buffer.end_ts_ms,
                            target_price: target,
                            settle_price: settle,
                            label_up: settle > target,
                            ts_recorded_ms: now_ms,
                        };
                        if committed_rounds.contains_key(&round.round_id) {
                            emitted_rounds.insert(rid.clone(), now_ms);
                            round_finalize_defer_warn_at_ms.remove(&rid);
                            log_ingest(
                                &persist_tx,
                                "warn",
                                "round_quality",
                                &format!(
                                    "skip_duplicate_stale_round {} reason=already_committed_in_process",
                                    round.round_id
                                ),
                            );
                            continue;
                        }
                        if !report.accept && round_drop_on_quality_fail {
                            emitted_rounds.insert(rid.clone(), now_ms);
                            round_finalize_defer_warn_at_ms.remove(&rid);
                            let reason = if report.reasons.is_empty() {
                                "quality_reject".to_string()
                            } else {
                                report.reasons.join(",")
                            };
                            log_ingest(
                                &persist_tx,
                                "warn",
                                "round_quality",
                                &format!(
                                    "drop_low_quality_stale_round {} samples={} expected={} reachable_expected={} coverage={:.3} sample_ratio={:.3} reachable_ratio={:.3} gate_ratio={:.3} avg_gap_ms={} max_gap_ms={} settle_stale_ms={} reason={}",
                                    rid,
                                    report.sample_count,
                                    report.expected_samples,
                                    report.reachable_expected_samples,
                                    report.coverage_ratio,
                                    report.sample_ratio,
                                    report.reachable_sample_ratio,
                                    report.sample_ratio_gate,
                                    report.avg_gap_ms,
                                    report.max_gap_ms,
                                    report.settle_stale_ms,
                                    reason
                                ),
                            );
                            tracing::warn!(
                                round_id = %rid,
                                accepted = report.accept,
                                reasons = %reason,
                                "drop stale round due to quality gate"
                            );
                            continue;
                        }
                        let reason = if report.reasons.is_empty() {
                            "stale_round_without_close".to_string()
                        } else {
                            report.reasons.join(",")
                        };
                        if let Err(err) = commit_tx
                            .send(RoundCommit {
                                snapshots: buffer.snapshots,
                                round,
                            })
                            .await
                        {
                            emitted_rounds.insert(rid.clone(), now_ms);
                            round_finalize_defer_warn_at_ms.remove(&rid);
                            tracing::error!(?err, "commit queue closed; dropping stale round");
                            continue;
                        }
                        emitted_rounds.insert(rid.clone(), now_ms);
                        round_finalize_defer_warn_at_ms.remove(&rid);
                        committed_rounds.insert(rid.clone(), now_ms);
                        log_ingest(
                            &persist_tx,
                            "warn",
                            "round_quality",
                            &format!(
                                "stale_committed {} samples={} expected={} reachable_expected={} coverage={:.3} sample_ratio={:.3} reachable_ratio={:.3} gate_ratio={:.3} avg_gap_ms={} max_gap_ms={} settle_stale_ms={} reason={}",
                                rid,
                                report.sample_count,
                                report.expected_samples,
                                report.reachable_expected_samples,
                                report.coverage_ratio,
                                report.sample_ratio,
                                report.reachable_sample_ratio,
                                report.sample_ratio_gate,
                                report.avg_gap_ms,
                                report.max_gap_ms,
                                report.settle_stale_ms,
                                reason
                            ),
                        );
                        tracing::warn!(
                            round_id = %rid,
                            sample_count = report.sample_count,
                            expected_samples = report.expected_samples,
                            reachable_expected_samples = report.reachable_expected_samples,
                            coverage_ratio = report.coverage_ratio,
                            sample_ratio = report.sample_ratio,
                            reachable_sample_ratio = report.reachable_sample_ratio,
                            sample_ratio_gate = report.sample_ratio_gate,
                            avg_gap_ms = report.avg_gap_ms,
                            max_gap_ms = report.max_gap_ms,
                            reasons = %reason,
                            "stale round committed with quality warning"
                        );
                    }
                }

                if now_ms >= next_housekeeping_ms {
                    next_housekeeping_ms = now_ms.saturating_add(RECORDER_HOUSEKEEPING_INTERVAL_MS);

                    let mut active_market_ids: HashSet<&str> =
                        markets_by_id.keys().map(String::as_str).collect();
                    for candidates in candidate_markets_by_pair.values() {
                        for market in candidates {
                            active_market_ids.insert(market.market_id.as_str());
                        }
                    }
                    book_by_market.retain(|market_id, _| active_market_ids.contains(market_id.as_str()));
                    quote_cache_by_market
                        .retain(|market_id, _| active_market_ids.contains(market_id.as_str()));
                    quote_cache_by_pair.retain(|pair, entry| {
                        required_pair_keys.contains(pair)
                            && now_ms.saturating_sub(entry.ts_ms)
                                <= RECORDER_STALE_STATE_RETENTION_MS
                    });

                    tokyo_by_symbol.retain(|symbol, row| {
                        active_symbols_set.contains(symbol.as_str())
                            && now_ms.saturating_sub(row.ts_ireland_recv_ms)
                                <= RECORDER_STALE_STATE_RETENTION_MS
                    });
                    chainlink_by_symbol.retain(|symbol, row| {
                        active_symbols_set.contains(symbol.as_str())
                            && now_ms.saturating_sub(row.ts_ireland_recv_ms)
                                <= RECORDER_STALE_STATE_RETENTION_MS
                    });
                    stale_tokyo_warn_by_symbol
                        .retain(|symbol, _| active_symbols_set.contains(symbol.as_str()));
                    stale_book_warn_by_pair.retain(|pair, _| required_pair_keys.contains(pair));
                    sample_gap_warn_by_pair.retain(|pair, _| required_pair_keys.contains(pair));
                    switch_market_by_pair.retain(|pair, _| required_pair_keys.contains(pair));
                    switch_epoch_by_pair.retain(|pair, _| required_pair_keys.contains(pair));
                    switch_last_change_ms_by_pair.retain(|pair, _| required_pair_keys.contains(pair));
                    switch_expected_start_by_pair.retain(|pair, _| required_pair_keys.contains(pair));
                    switch_guard_warn_by_pair.retain(|pair, _| required_pair_keys.contains(pair));
                    switch_count_by_pair.retain(|pair, _| required_pair_keys.contains(pair));
                    switch_guard_suppress_count_by_pair
                        .retain(|pair, _| required_pair_keys.contains(pair));
                    sampling_gap_count_by_pair.retain(|pair, _| required_pair_keys.contains(pair));
                    book_fallback_count_by_pair.retain(|pair, _| required_pair_keys.contains(pair));
                    round_finalize_defer_warn_at_ms.retain(|round_id, warn_at_ms| {
                        round_meta_is_fresh(round_id, now_ms, ROUND_META_RETENTION_MS)
                            && now_ms.saturating_sub(*warn_at_ms) <= EMITTED_ROUND_RETENTION_MS
                    });
                    motion_by_key.retain(|key, state| {
                        let symbol = key.split('|').next().unwrap_or_default();
                        active_symbols_set.contains(symbol)
                            && now_ms.saturating_sub(state.ts_ms) <= RECORDER_STALE_STATE_RETENTION_MS
                    });

                    target_cache.retain(|_, (cached_at, _)| {
                        now_ms.saturating_sub(*cached_at) <= TARGET_CACHE_RETENTION_MS
                    });
                    target_retry_after_ms.retain(|_, retry_after_ms| {
                        now_ms <= retry_after_ms.saturating_add(TARGET_RETRY_RETENTION_MS)
                    });
                    target_anchor_by_round.retain(|round_id, _| {
                        round_meta_is_fresh(round_id, now_ms, ROUND_META_RETENTION_MS)
                    });
                    emitted_rounds.retain(|round_id, closed_at_ms| {
                        now_ms.saturating_sub(*closed_at_ms) <= EMITTED_ROUND_RETENTION_MS
                            && round_meta_is_fresh(round_id, now_ms, ROUND_META_RETENTION_MS)
                    });
                    committed_rounds.retain(|round_id, committed_at_ms| {
                        now_ms.saturating_sub(*committed_at_ms) <= EMITTED_ROUND_RETENTION_MS
                            && round_meta_is_fresh(round_id, now_ms, ROUND_META_RETENTION_MS)
                    });

                    let mut dropped_state_entries = 0usize;
                    dropped_state_entries = dropped_state_entries.saturating_add(
                        trim_map_by_key_ts_desc(&mut motion_by_key, motion_state_cap, |_, v| v.ts_ms),
                    );
                    dropped_state_entries = dropped_state_entries.saturating_add(
                        trim_map_by_key_ts_desc(&mut target_cache, target_cache_cap, |_, (cached_at, _)| *cached_at),
                    );
                    dropped_state_entries = dropped_state_entries.saturating_add(
                        trim_map_by_key_ts_desc(&mut target_retry_after_ms, target_cache_cap, |_, retry_after_ms| *retry_after_ms),
                    );
                    dropped_state_entries = dropped_state_entries.saturating_add(
                        trim_map_by_key_ts_desc(&mut prob_smooth_by_round, round_state_cap, |_, v| v.ts_ms),
                    );
                    dropped_state_entries = dropped_state_entries.saturating_add(
                        trim_map_by_key_ts_desc(&mut target_anchor_by_round, round_state_cap, |round_id, _| {
                            round_end_ts_ms_from_round_id(round_id).unwrap_or(i64::MIN)
                        }),
                    );
                    dropped_state_entries = dropped_state_entries.saturating_add(
                        trim_map_by_key_ts_desc(&mut emitted_rounds, round_state_cap, |_, closed_at_ms| *closed_at_ms),
                    );
                    dropped_state_entries = dropped_state_entries.saturating_add(
                        trim_map_by_key_ts_desc(&mut committed_rounds, round_state_cap, |_, committed_at_ms| *committed_at_ms),
                    );
                    if dropped_state_entries > 0 {
                        tracing::warn!(
                            dropped_state_entries,
                            motion_state_len = motion_by_key.len(),
                            target_cache_len = target_cache.len(),
                            target_retry_len = target_retry_after_ms.len(),
                            prob_state_len = prob_smooth_by_round.len(),
                            target_anchor_len = target_anchor_by_round.len(),
                            emitted_round_len = emitted_rounds.len(),
                            committed_round_len = committed_rounds.len(),
                            "housekeeping state cap enforced"
                        );
                    }

                    if now_ms >= next_switch_stats_report_ms {
                        next_switch_stats_report_ms =
                            now_ms.saturating_add(SWITCH_STATS_REPORT_INTERVAL_MS);
                        let mut pairs = required_pair_keys.iter().cloned().collect::<Vec<_>>();
                        pairs.sort();
                        for pair in pairs {
                            let switch_count =
                                switch_count_by_pair.get(&pair).copied().unwrap_or(0);
                            let guard_suppressed = switch_guard_suppress_count_by_pair
                                .get(&pair)
                                .copied()
                                .unwrap_or(0);
                            let gap_count =
                                sampling_gap_count_by_pair.get(&pair).copied().unwrap_or(0);
                            let fallback_count =
                                book_fallback_count_by_pair.get(&pair).copied().unwrap_or(0);
                            let current_market = switch_market_by_pair
                                .get(&pair)
                                .cloned()
                                .unwrap_or_else(|| "-".to_string());
                            let current_epoch =
                                switch_epoch_by_pair.get(&pair).copied().unwrap_or(0);
                            log_ingest(
                                &persist_tx,
                                "info",
                                "collector_switch_stats",
                                &format!(
                                    "pair={} epoch={} market_id={} switches={} guard_suppressed={} sampling_gaps={} book_fallbacks={}",
                                    pair,
                                    current_epoch,
                                    current_market,
                                    switch_count,
                                    guard_suppressed,
                                    gap_count,
                                    fallback_count
                                ),
                            );
                        }
                    }
                }
            }
        }
    }
}

pub async fn run_ireland_api(args: IrelandApiArgs) -> Result<()> {
    let api_cfg = ApiConfig {
        bind: args.bind,
        clickhouse_url: normalize_opt_url(&args.clickhouse_url),
        redis_url: normalize_opt_url(&args.redis_url),
        redis_prefix: args.redis_prefix,
        dashboard_dist_dir: Some(args.dashboard_dist),
    };
    run_api_server(api_cfg).await
}

fn spawn_tokyo_udp_receiver(
    bind: String,
    tx: mpsc::UnboundedSender<TokyoBinanceWire>,
    persist: mpsc::UnboundedSender<PersistEvent>,
) {
    tokio::spawn(async move {
        let socket = match UdpSocket::bind(&bind).await {
            Ok(v) => v,
            Err(err) => {
                log_ingest(
                    &persist,
                    "error",
                    "udp_receiver",
                    &format!("bind failed: {err}"),
                );
                return;
            }
        };
        log_ingest(
            &persist,
            "info",
            "udp_receiver",
            &format!("listening on {bind}"),
        );

        let mut buf = vec![0_u8; 2048];
        loop {
            let (n, _peer) = match socket.recv_from(&mut buf).await {
                Ok(v) => v,
                Err(err) => {
                    log_ingest(
                        &persist,
                        "warn",
                        "udp_receiver",
                        &format!("recv error: {err}"),
                    );
                    continue;
                }
            };
            let msg: TokyoBinanceWire = match serde_json::from_slice(&buf[..n]) {
                Ok(v) => v,
                Err(err) => {
                    log_ingest(
                        &persist,
                        "warn",
                        "udp_receiver",
                        &format!("json parse error: {err}"),
                    );
                    continue;
                }
            };
            if tx.send(msg).is_err() {
                break;
            }
        }
    });
}

fn spawn_chainlink_reader(
    symbols: Vec<String>,
    tx: mpsc::UnboundedSender<ChainlinkLocal>,
    persist: mpsc::UnboundedSender<PersistEvent>,
) {
    tokio::spawn(async move {
        loop {
            let feed = MultiSourceRefFeed::new(Duration::from_millis(50));
            let stream = feed.stream_ticks_ws(symbols.clone()).await;
            let mut stream = match stream {
                Ok(v) => v,
                Err(err) => {
                    log_ingest(
                        &persist,
                        "warn",
                        "chainlink",
                        &format!("stream start failed: {err}"),
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            while let Some(next) = stream.next().await {
                let tick = match next {
                    Ok(v) => v,
                    Err(err) => {
                        log_ingest(
                            &persist,
                            "warn",
                            "chainlink",
                            &format!("stream item error: {err}"),
                        );
                        break;
                    }
                };
                if tick.source.as_str() != "chainlink_rtds" {
                    continue;
                }
                let msg = ChainlinkLocal {
                    symbol: tick.symbol.to_ascii_uppercase(),
                    ts_exchange_ms: tick.event_ts_exchange_ms.max(tick.event_ts_ms),
                    ts_ireland_recv_ms: Utc::now().timestamp_millis(),
                    price: tick.price,
                };
                if tx.send(msg).is_err() {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(800)).await;
        }
    });
}

fn spawn_book_reader(
    symbols: Vec<String>,
    timeframes: Vec<String>,
    tx: mpsc::UnboundedSender<BookTop>,
    persist: mpsc::UnboundedSender<PersistEvent>,
) {
    tokio::spawn(async move {
        let mut retry_wait = Duration::from_secs(1);
        let max_retry_wait = Duration::from_secs(20);
        loop {
            let feed = PolymarketFeed::new_with_universe(
                Duration::from_millis(50),
                symbols.clone(),
                vec!["updown".to_string()],
                timeframes.clone(),
            );
            let stream = feed.stream_books().await;
            let mut stream = match stream {
                Ok(v) => v,
                Err(err) => {
                    log_ingest(
                        &persist,
                        "warn",
                        "polymarket_book",
                        &format!("stream start failed: {err}"),
                    );
                    tokio::time::sleep(retry_wait).await;
                    retry_wait = std::cmp::min(max_retry_wait, retry_wait.saturating_mul(2));
                    continue;
                }
            };
            let mut saw_book = false;
            while let Some(next) = stream.next().await {
                let book = match next {
                    Ok(v) => v,
                    Err(err) => {
                        log_ingest(
                            &persist,
                            "warn",
                            "polymarket_book",
                            &format!("stream item error: {err}"),
                        );
                        break;
                    }
                };
                if !saw_book {
                    saw_book = true;
                    retry_wait = Duration::from_secs(1);
                }
                if tx.send(book).is_err() {
                    return;
                }
            }
            if !saw_book {
                log_ingest(
                    &persist,
                    "warn",
                    "polymarket_book",
                    &format!(
                        "stream ended without data; backing off {}ms",
                        retry_wait.as_millis()
                    ),
                );
            }
            tokio::time::sleep(retry_wait).await;
            retry_wait = std::cmp::min(max_retry_wait, retry_wait.saturating_mul(2));
        }
    });
}

fn spawn_market_discovery_reader(
    symbols: Vec<String>,
    timeframes: Vec<String>,
    refresh_sec: u64,
    tx: mpsc::UnboundedSender<Vec<MarketMeta>>,
    persist: mpsc::UnboundedSender<PersistEvent>,
) {
    fn has_near_upcoming_markets(
        markets: &[MarketMeta],
        now_ms: i64,
        prewarm_lead_ms: i64,
    ) -> bool {
        markets.iter().any(|m| {
            let lead = m.start_ts_ms.saturating_sub(now_ms);
            lead >= 0 && lead <= prewarm_lead_ms
        })
    }

    tokio::spawn(async move {
        let base_refresh = refresh_sec.max(2);
        let discovery_prewarm_lead_ms = std::env::var("FORGE_DISCOVERY_PREWARM_LEAD_MS")
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .unwrap_or(DISCOVERY_PREWARM_LEAD_DEFAULT_MS)
            .clamp(60_000, 15 * 60 * 1_000);
        let discovery_prewarm_refresh_sec = std::env::var("FORGE_DISCOVERY_PREWARM_REFRESH_SEC")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(DISCOVERY_PREWARM_REFRESH_DEFAULT_SEC)
            .clamp(1, 10)
            .min(base_refresh);
        let mut current_wait = base_refresh;
        let max_wait = base_refresh.saturating_mul(8).min(600);
        let mut last_markets: Vec<MarketMeta> = Vec::new();
        loop {
            match discover_markets(&symbols, &timeframes).await {
                Ok(markets) => {
                    let now_ms = Utc::now().timestamp_millis();
                    let prewarm_polling =
                        has_near_upcoming_markets(&markets, now_ms, discovery_prewarm_lead_ms);
                    current_wait = if prewarm_polling {
                        discovery_prewarm_refresh_sec
                    } else {
                        base_refresh
                    };
                    tracing::info!(
                        market_count = markets.len(),
                        prewarm_polling = prewarm_polling,
                        poll_wait_sec = current_wait,
                        "discovery succeeded"
                    );
                    log_ingest(
                        &persist,
                        "info",
                        "discovery",
                        &format!("discovered {} active markets", markets.len()),
                    );
                    if tx.send(markets.clone()).is_err() {
                        return;
                    }
                    last_markets = markets;
                }
                Err(err) => {
                    if let Ok(fallback) =
                        discover_markets_from_target_cache(&symbols, &timeframes).await
                    {
                        let now_ms = Utc::now().timestamp_millis();
                        let prewarm_polling =
                            has_near_upcoming_markets(&fallback, now_ms, discovery_prewarm_lead_ms);
                        current_wait = if prewarm_polling {
                            discovery_prewarm_refresh_sec
                        } else {
                            base_refresh
                        };
                        tracing::warn!(
                            fallback_market_count = fallback.len(),
                            error = %err,
                            prewarm_polling = prewarm_polling,
                            poll_wait_sec = current_wait,
                            "discovery failed; using cache fallback"
                        );
                        log_ingest(
                            &persist,
                            "warn",
                            "discovery",
                            &format!(
                                "discover failed, recovered {} markets from cache fallback: {err}",
                                fallback.len()
                            ),
                        );
                        if tx.send(fallback.clone()).is_err() {
                            return;
                        }
                        last_markets = fallback;
                        tokio::time::sleep(Duration::from_secs(current_wait.max(1))).await;
                        continue;
                    }
                    if !last_markets.is_empty() {
                        let now_ms = Utc::now().timestamp_millis();
                        let prewarm_polling = has_near_upcoming_markets(
                            &last_markets,
                            now_ms,
                            discovery_prewarm_lead_ms,
                        );
                        current_wait = if prewarm_polling {
                            discovery_prewarm_refresh_sec
                        } else {
                            base_refresh
                        };
                        tracing::warn!(
                            fallback_market_count = last_markets.len(),
                            error = %err,
                            prewarm_polling = prewarm_polling,
                            poll_wait_sec = current_wait,
                            "discovery failed; reusing last known markets"
                        );
                        log_ingest(
                            &persist,
                            "warn",
                            "discovery",
                            &format!(
                                "discover failed, reusing last known {} markets: {err}",
                                last_markets.len()
                            ),
                        );
                        if tx.send(last_markets.clone()).is_err() {
                            return;
                        }
                        tokio::time::sleep(Duration::from_secs(current_wait.max(1))).await;
                        continue;
                    }
                    log_ingest(
                        &persist,
                        "warn",
                        "discovery",
                        &format!("discover failed: {err}"),
                    );
                    tracing::warn!(error = %err, "discovery failed with no fallback");
                    current_wait = (current_wait.saturating_mul(2)).min(max_wait);
                }
            }
            tokio::time::sleep(Duration::from_secs(current_wait.max(1))).await;
        }
    });
}

fn target_market_cache_file_path() -> PathBuf {
    if let Ok(raw) = std::env::var("POLYEDGE_TARGET_MARKET_CACHE_FILE") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }
    std::env::temp_dir().join("polyedge_target_market_cache.json")
}

fn target_market_cache_key(symbols: &[String], timeframes: &[String]) -> String {
    let mut symbols_norm = symbols
        .iter()
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    symbols_norm.sort();
    symbols_norm.dedup();
    let mut timeframes_norm = timeframes
        .iter()
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    timeframes_norm.sort();
    timeframes_norm.dedup();
    format!(
        "symbols={}|types=updown|tfs={}",
        symbols_norm.join(","),
        timeframes_norm.join(",")
    )
}

fn detect_symbol_from_question(question: &str, allowed_symbols: &[String]) -> Option<String> {
    let text = question.to_ascii_uppercase();
    let aliases: [(&str, [&str; 3]); 15] = [
        ("BTCUSDT", ["BITCOIN", "BTC", "XBT"]),
        ("ETHUSDT", ["ETHEREUM", "ETH", "ETHER"]),
        ("SOLUSDT", ["SOLANA", "SOL", "SOLAN"]),
        ("XRPUSDT", ["RIPPLE", "XRP", "XRP"]),
        ("BNBUSDT", ["BINANCE", "BNB", "BNB"]),
        ("DOGEUSDT", ["DOGECOIN", "DOGE", "DOGE"]),
        ("ADAUSDT", ["CARDANO", "ADA", "ADA"]),
        ("AVAXUSDT", ["AVALANCHE", "AVAX", "AVAX"]),
        ("LINKUSDT", ["CHAINLINK", "LINK", "LINK"]),
        ("MATICUSDT", ["POLYGON", "MATIC", "POL"]),
        ("LTCUSDT", ["LITECOIN", "LTC", "LTC"]),
        ("DOTUSDT", ["POLKADOT", "DOT", "DOT"]),
        ("TRXUSDT", ["TRON", "TRX", "TRX"]),
        ("TONUSDT", ["TONCOIN", "TON", "TON"]),
        ("NEARUSDT", ["NEAR", "NEAR", "NEAR"]),
    ];
    for (symbol, keys) in aliases {
        if !allowed_symbols
            .iter()
            .any(|s| s.eq_ignore_ascii_case(symbol))
        {
            continue;
        }
        if keys.iter().any(|k| text.contains(k)) {
            return Some(symbol.to_string());
        }
    }
    None
}

async fn discover_markets_from_target_cache(
    symbols: &[String],
    timeframes: &[String],
) -> Result<Vec<MarketMeta>> {
    let cache_path = target_market_cache_file_path();
    let raw = tokio::fs::read_to_string(&cache_path)
        .await
        .map_err(|err| anyhow::anyhow!("read cache failed ({}): {err}", cache_path.display()))?;
    let root: serde_json::Value = serde_json::from_str(&raw).map_err(|err| {
        anyhow::anyhow!("parse cache json failed ({}): {err}", cache_path.display())
    })?;
    let cache_key = target_market_cache_key(symbols, timeframes);
    let Some(markets_obj) = root.get(&cache_key).and_then(serde_json::Value::as_object) else {
        anyhow::bail!("cache key not found: {cache_key}");
    };

    let user_agent = std::env::var("POLYEDGE_DISCOVERY_USER_AGENT")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| {
            "Mozilla/5.0 (compatible; PolyEdgeBot/1.0; +https://github.com/TYCT-tyct/PolyEdge)"
                .to_string()
        });
    let http = Client::builder()
        .user_agent(user_agent)
        .connect_timeout(Duration::from_secs(6))
        .timeout(Duration::from_secs(12))
        .build()
        .unwrap_or_else(|_| Client::new());

    let mut out = Vec::<MarketMeta>::new();
    for (market_id, market_v) in markets_obj {
        let Some(timeframe) = market_v
            .get("timeframe")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        if !timeframes
            .iter()
            .any(|tf| tf.eq_ignore_ascii_case(timeframe))
        {
            continue;
        }
        let detail_url = format!("https://gamma-api.polymarket.com/markets/{market_id}");
        let detail = match http.get(&detail_url).send().await {
            Ok(resp) => match resp.error_for_status() {
                Ok(ok) => ok,
                Err(_) => continue,
            },
            Err(_) => continue,
        };
        let detail_json: serde_json::Value = match detail.json().await {
            Ok(v) => v,
            Err(_) => continue,
        };
        let Some(question) = detail_json
            .get("question")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
        else {
            continue;
        };
        let Some(symbol) = detect_symbol_from_question(&question, symbols) else {
            continue;
        };
        let Some(end_ts_ms) = parse_timestamp_ms(
            detail_json
                .get("endDate")
                .and_then(serde_json::Value::as_str),
        ) else {
            continue;
        };
        let tf_ms = timeframe_to_ms(timeframe).unwrap_or(300_000);
        let start_ts_ms = end_ts_ms.saturating_sub(tf_ms);
        out.push(MarketMeta {
            market_id: market_id.clone(),
            symbol,
            timeframe: timeframe.to_string(),
            title: question,
            target_price: None,
            end_ts_ms,
            start_ts_ms,
        });
    }

    if out.is_empty() {
        anyhow::bail!("cache fallback produced no market meta");
    }
    Ok(out)
}

async fn discover_markets(symbols: &[String], timeframes: &[String]) -> Result<Vec<MarketMeta>> {
    let discovery = MarketDiscovery::new(DiscoveryConfig {
        symbols: symbols.to_vec(),
        market_types: vec!["updown".to_string()],
        timeframes: timeframes.to_vec(),
        ..DiscoveryConfig::default()
    });

    let markets = discovery.discover().await?;
    let mut out = Vec::<MarketMeta>::new();

    for m in markets {
        let Some(tf) = m.timeframe.clone() else {
            continue;
        };
        let Some(end_ts_ms) = parse_timestamp_ms(m.end_date.as_deref()) else {
            continue;
        };
        let tf_ms = timeframe_to_ms(&tf).unwrap_or(300_000);
        let start_ts_ms = end_ts_ms.saturating_sub(tf_ms);
        out.push(to_market_meta(m, tf, start_ts_ms, end_ts_ms));
    }

    if out.is_empty() {
        anyhow::bail!("discovery returned no usable markets");
    }

    Ok(out)
}

fn to_market_meta(m: MarketDescriptor, tf: String, start_ts_ms: i64, end_ts_ms: i64) -> MarketMeta {
    let symbol = m.symbol.to_ascii_uppercase();
    let target_price = m.price_to_beat;
    MarketMeta {
        market_id: m.market_id,
        symbol,
        timeframe: tf,
        title: m.question.clone(),
        target_price,
        end_ts_ms,
        start_ts_ms,
    }
}

fn vatic_market_type(timeframe: &str) -> Option<&'static str> {
    match timeframe {
        "5m" => Some("5min"),
        "15m" => Some("15min"),
        "30m" => Some("30min"),
        "1h" => Some("1hour"),
        "2h" => Some("2hour"),
        "4h" => Some("4hour"),
        _ => None,
    }
}

fn parse_target_from_json_value(v: &serde_json::Value) -> Option<f64> {
    if let Some(v) = v
        .get("datapoint")
        .and_then(|x| x.get("price"))
        .and_then(|x| x.as_f64())
        .filter(|x| x.is_finite() && *x > 0.0)
    {
        return Some(v);
    }
    v.get("target_price")
        .and_then(|x| x.as_f64())
        .or_else(|| v.get("target").and_then(|x| x.as_f64()))
        .or_else(|| v.get("price").and_then(|x| x.as_f64()))
        .filter(|x| x.is_finite() && *x > 0.0)
}

fn parse_window_start_sec(v: &serde_json::Value) -> Option<i64> {
    v.get("windowStart")
        .and_then(|x| x.as_i64())
        .or_else(|| v.get("market_start").and_then(|x| x.as_i64()))
        .or_else(|| {
            v.get("market")
                .and_then(|m| m.get("timestamp_start"))
                .and_then(|x| x.as_i64())
        })
}

fn symbol_to_asset(symbol: &str) -> String {
    symbol
        .strip_suffix("USDT")
        .unwrap_or(symbol)
        .to_ascii_lowercase()
}

fn is_chainlink_backed_timeframe(timeframe: &str) -> bool {
    matches!(timeframe, "5m" | "15m" | "4h")
}

fn pick_official_chainlink_target(
    chainlink: Option<&ChainlinkLocal>,
    round_start_ms: i64,
    timeframe: &str,
) -> Option<f64> {
    if !is_chainlink_backed_timeframe(timeframe) {
        return None;
    }
    let cl = chainlink?;
    let px = cl.price;
    if !(px.is_finite() && px > 0.0) {
        return None;
    }
    // Accept Chainlink prints close to round boundary as official fallback target.
    let dt = cl.ts_exchange_ms.saturating_sub(round_start_ms).abs();
    if dt <= 120_000 {
        return Some(px);
    }
    None
}

async fn fetch_target_from_vatic_market(
    http: &Client,
    symbol: &str,
    timeframe: &str,
    market_start_sec: i64,
) -> Option<f64> {
    let market_type = vatic_market_type(timeframe)?;
    let asset = symbol_to_asset(symbol);
    let url = format!(
        "https://api.vatic.trading/api/v1/history/market?asset={asset}&type={market_type}&marketStart={market_start_sec}"
    );
    let resp = http.get(url).send().await.ok()?;
    let json: serde_json::Value = resp.json().await.ok()?;
    parse_target_from_json_value(&json)
}

async fn fetch_target_from_vatic_active(
    http: &Client,
    symbol: &str,
    timeframe: &str,
    market_start_sec: i64,
) -> Option<f64> {
    let market_type = vatic_market_type(timeframe)?;
    let asset = symbol_to_asset(symbol);
    let url = format!(
        "https://api.vatic.trading/api/v1/targets/active?asset={asset}&types={market_type}"
    );
    let resp = http.get(url).send().await.ok()?;
    let json: serde_json::Value = resp.json().await.ok()?;

    if let Some(v) = parse_target_from_json_value(&json).filter(|_| {
        parse_window_start_sec(&json)
            .map(|w| w == market_start_sec)
            .unwrap_or(false)
    }) {
        return Some(v);
    }
    if let Some(v) = json
        .get("data")
        .filter(|item| {
            parse_window_start_sec(item)
                .map(|w| w == market_start_sec)
                .unwrap_or(false)
        })
        .and_then(parse_target_from_json_value)
    {
        return Some(v);
    }
    if let Some(v) = json
        .get("result")
        .filter(|item| {
            parse_window_start_sec(item)
                .map(|w| w == market_start_sec)
                .unwrap_or(false)
        })
        .and_then(parse_target_from_json_value)
    {
        return Some(v);
    }

    let results = json.get("results").and_then(|v| v.as_array())?;
    for item in results {
        let mt = item
            .get("marketType")
            .or_else(|| item.get("market_type"))
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if !mt.eq_ignore_ascii_case(market_type) {
            continue;
        }
        let ok = item.get("ok").and_then(|v| v.as_bool()).unwrap_or(true);
        if !ok {
            continue;
        }
        let Some(window_start) = parse_window_start_sec(item) else {
            continue;
        };
        if window_start != market_start_sec {
            continue;
        }
        if let Some(v) = parse_target_from_json_value(item) {
            return Some(v);
        }
    }
    None
}

async fn fetch_target_from_vatic(
    http: &Client,
    symbol: &str,
    timeframe: &str,
    timestamp_sec: i64,
) -> Option<f64> {
    let market_type = vatic_market_type(timeframe)?;
    let asset = symbol_to_asset(symbol);
    let url = format!(
        "https://api.vatic.trading/api/v1/targets/timestamp?asset={asset}&type={market_type}&timestamp={timestamp_sec}"
    );
    let resp = http.get(url).send().await.ok()?;
    let json: serde_json::Value = resp.json().await.ok()?;
    parse_target_from_json_value(&json)
}

async fn fetch_target_from_vatic_chainlink(
    http: &Client,
    symbol: &str,
    timeframe: &str,
    timestamp_sec: i64,
) -> Option<f64> {
    if !is_chainlink_backed_timeframe(timeframe) {
        return None;
    }
    let market_type = vatic_market_type(timeframe)?;
    let asset = symbol_to_asset(symbol);
    let url = format!(
        "https://api.vatic.trading/api/v1/targets/chainlink?asset={asset}&type={market_type}&timestamp={timestamp_sec}"
    );
    let resp = http.get(url).send().await.ok()?;
    let json: serde_json::Value = resp.json().await.ok()?;
    parse_target_from_json_value(&json)
}

async fn fetch_target_from_official_binance(symbol: &str, market_start_ms: i64) -> Option<f64> {
    let http = reqwest::Client::new();
    let start_ms = market_start_ms.max(0);
    let end_ms = start_ms.saturating_add(5_000);
    let url = format!(
        "https://api.binance.com/api/v3/aggTrades?symbol={symbol}&startTime={start_ms}&endTime={end_ms}&limit=1000"
    );
    let resp = http.get(url).send().await.ok()?;
    let trades: serde_json::Value = resp.json().await.ok()?;
    let mut px_qty = 0.0f64;
    let mut qty = 0.0f64;
    if let Some(arr) = trades.as_array() {
        for row in arr {
            let p = row
                .get("p")
                .and_then(|v| v.as_str())
                .and_then(|v| v.parse::<f64>().ok())
                .filter(|v| v.is_finite() && *v > 0.0)?;
            let q = row
                .get("q")
                .and_then(|v| v.as_str())
                .and_then(|v| v.parse::<f64>().ok())
                .filter(|v| v.is_finite() && *v > 0.0)?;
            px_qty += p * q;
            qty += q;
        }
    }
    if qty > 0.0 {
        return Some(px_qty / qty);
    }

    let kline_url = format!(
        "https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m&startTime={start_ms}&limit=1"
    );
    let resp = http.get(kline_url).send().await.ok()?;
    let klines: serde_json::Value = resp.json().await.ok()?;
    let first = klines.as_array()?.first()?;
    let open = first
        .as_array()?
        .get(1)?
        .as_str()?
        .parse::<f64>()
        .ok()
        .filter(|v| v.is_finite() && *v > 0.0)?;
    Some(open)
}

fn aligned_round_timestamp_sec(start_ts_ms: i64, timeframe: &str) -> i64 {
    let raw_sec = start_ts_ms.div_euclid(1_000);
    let bucket = match timeframe {
        "5m" => 5 * 60,
        "15m" => 15 * 60,
        "30m" => 30 * 60,
        "1h" => 60 * 60,
        "2h" => 2 * 60 * 60,
        "4h" => 4 * 60 * 60,
        _ => 5 * 60,
    };
    raw_sec.div_euclid(bucket) * bucket
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::SnapshotRow;
    use serde_json::json;

    #[test]
    fn parse_window_start_from_results_item() {
        let v = json!({
            "windowStart": 1771918200_i64,
            "price": 63262.98
        });
        assert_eq!(parse_window_start_sec(&v), Some(1771918200_i64));
    }

    #[test]
    fn parse_window_start_from_market_payload() {
        let v = json!({
            "market": {
                "timestamp_start": 1771918200_i64
            },
            "datapoint": {
                "price": 63262.98
            }
        });
        assert_eq!(parse_window_start_sec(&v), Some(1771918200_i64));
    }

    #[test]
    fn align_round_start_to_bucket() {
        let ts_ms = 1771918380123_i64;
        assert_eq!(aligned_round_timestamp_sec(ts_ms, "5m"), 1771918200_i64);
        assert_eq!(aligned_round_timestamp_sec(ts_ms, "15m"), 1771918200_i64);
    }

    #[test]
    fn active_market_filter_honors_symbol_tf_map() {
        let symbols = vec![
            "BTCUSDT".to_string(),
            "ETHUSDT".to_string(),
            "SOLUSDT".to_string(),
            "XRPUSDT".to_string(),
        ];
        let tfs = vec!["5m".to_string(), "15m".to_string()];
        let f = ActiveMarketFilter::from_inputs(
            &symbols,
            &tfs,
            "BTCUSDT:5m|15m,ETHUSDT:5m,SOLUSDT:5m,XRPUSDT:5m",
        );
        assert!(f.allows("BTCUSDT", "5m"));
        assert!(f.allows("BTCUSDT", "15m"));
        assert!(f.allows("ETHUSDT", "5m"));
        assert!(!f.allows("ETHUSDT", "15m"));
        assert!(!f.allows("DOGEUSDT", "5m"));
    }

    #[test]
    fn active_market_filter_falls_back_to_defaults_when_map_invalid() {
        let symbols = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        let tfs = vec!["5m".to_string(), "15m".to_string()];
        let f = ActiveMarketFilter::from_inputs(&symbols, &tfs, "DOGEUSDT:1m");
        assert!(f.allows("BTCUSDT", "5m"));
        assert!(f.allows("BTCUSDT", "15m"));
        assert!(f.allows("ETHUSDT", "5m"));
        assert!(f.allows("ETHUSDT", "15m"));
    }

    #[test]
    fn sampling_window_uses_tight_end_grace_for_fast_switch() {
        let market = MarketMeta {
            market_id: "m".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "5m".to_string(),
            title: "BTC".to_string(),
            target_price: None,
            start_ts_ms: 1_000,
            end_ts_ms: 2_000,
        };
        assert!(market_in_sampling_window(&market, 1_999, 30_000, 300));
        assert!(market_in_sampling_window(&market, 2_250, 30_000, 300));
        assert!(!market_in_sampling_window(&market, 2_301, 30_000, 300));
    }

    #[test]
    fn warmable_candidate_accepts_farther_prestart_window() {
        let market = MarketMeta {
            market_id: "m".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "5m".to_string(),
            title: "BTC".to_string(),
            target_price: None,
            start_ts_ms: 100_000,
            end_ts_ms: 400_000,
        };
        assert!(!market_candidate_is_warmable(
            &market,
            20_000,
            60_000,
            MARKET_STALE_GUARD_DEFAULT_MS
        ));
        assert!(market_candidate_is_warmable(
            &market,
            20_000,
            90_000,
            MARKET_STALE_GUARD_DEFAULT_MS
        ));
    }

    #[test]
    fn warmable_candidate_still_rejects_long_ended_round() {
        let market = MarketMeta {
            market_id: "m".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "5m".to_string(),
            title: "BTC".to_string(),
            target_price: None,
            start_ts_ms: 1_000,
            end_ts_ms: 2_000,
        };
        assert!(market_candidate_is_warmable(
            &market,
            2_000 + MARKET_STALE_GUARD_DEFAULT_MS,
            180_000,
            MARKET_STALE_GUARD_DEFAULT_MS
        ));
        assert!(!market_candidate_is_warmable(
            &market,
            2_000 + MARKET_STALE_GUARD_DEFAULT_MS + 1,
            180_000,
            MARKET_STALE_GUARD_DEFAULT_MS
        ));
    }

    #[test]
    fn trim_candidate_pool_keeps_current_next_next2() {
        let now_ms = 1_000_000;
        let mk = |id: &str, start: i64, end: i64| MarketMeta {
            market_id: id.to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "5m".to_string(),
            title: "BTC".to_string(),
            target_price: None,
            start_ts_ms: start,
            end_ts_ms: end,
        };
        let mut markets = vec![
            mk("past", now_ms - 600_000, now_ms - 300_000),
            mk("current", now_ms - 60_000, now_ms + 240_000),
            mk("next", now_ms + 240_000, now_ms + 540_000),
            mk("next2", now_ms + 540_000, now_ms + 840_000),
            mk("next3", now_ms + 840_000, now_ms + 1_140_000),
        ];
        trim_candidate_pool(&mut markets, now_ms, 30_000, 300, 3);
        let ids = markets.into_iter().map(|m| m.market_id).collect::<Vec<_>>();
        assert_eq!(ids, vec!["current", "next", "next2"]);
    }

    fn dummy_snapshot(round_id: &str, ts_ms: i64) -> SnapshotRow {
        SnapshotRow {
            schema_version: "v1",
            ingest_seq: 1,
            ts_ireland_sample_ms: ts_ms,
            ts_tokyo_recv_ms: None,
            ts_tokyo_send_ms: None,
            ts_exchange_ms: None,
            ts_ireland_recv_ms: None,
            tokyo_relay_proc_lag_ms: None,
            cross_region_net_lag_ms: None,
            path_lag_ms: None,
            symbol: "BTCUSDT".to_string(),
            ts_pm_recv_ms: ts_ms,
            market_id: "m".to_string(),
            timeframe: "5m".to_string(),
            title: "BTC".to_string(),
            target_price: Some(100_000.0),
            mid_yes: 0.5,
            mid_no: 0.5,
            mid_yes_smooth: 0.5,
            mid_no_smooth: 0.5,
            bid_yes: 0.49,
            ask_yes: 0.51,
            bid_no: 0.49,
            ask_no: 0.51,
            binance_price: Some(100_100.0),
            pm_live_btc_price: None,
            ts_pm_live_exchange_ms: None,
            ts_pm_live_recv_ms: None,
            chainlink_price: None,
            ts_chainlink_exchange_ms: None,
            ts_chainlink_recv_ms: None,
            delta_price: Some(100.0),
            delta_pct: Some(0.001),
            delta_pct_smooth: Some(0.001),
            remaining_ms: 0,
            velocity_bps_per_sec: Some(0.0),
            acceleration: Some(0.0),
            round_id: round_id.to_string(),
        }
    }

    #[test]
    fn round_quality_accepts_reachable_baseline_when_coverage_is_good() {
        let market = MarketMeta {
            market_id: "m".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "5m".to_string(),
            title: "BTC".to_string(),
            target_price: Some(100_000.0),
            start_ts_ms: 0,
            end_ts_ms: 300_000,
        };
        let mut buffer = RoundBuffer::new("BTCUSDT_5m_0".to_string(), &market);
        let sample = dummy_snapshot(&buffer.round_id, 0);
        buffer.snapshots = vec![sample; 1_100];
        buffer.first_sample_ms = Some(0);
        buffer.last_sample_ms = Some(299_700);
        buffer.max_gap_ms = 320;
        buffer.target_price_latest = Some(100_000.0);
        buffer.settle_price_latest = Some(100_100.0);
        buffer.settle_price_latest_ts_ms = Some(299_700);
        let policy = RoundQualityPolicy {
            min_coverage_ratio: 0.90,
            min_sample_ratio: 0.85,
            max_gap_ms: 2_500,
            start_tolerance_ms: 35_000,
            end_tolerance_ms: 10_000,
            settle_stale_tolerance_ms: 2_500,
        };
        let report = buffer.evaluate(&policy, 100);
        assert!(report.accept);
        assert!(report.sample_ratio < 0.5);
        assert!(report.reachable_sample_ratio > 0.95);
        assert!(report.sample_ratio_gate > policy.min_sample_ratio);
    }

    #[test]
    fn round_quality_rejects_sparse_stream_even_if_reachable_ratio_is_high() {
        let market = MarketMeta {
            market_id: "m".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "5m".to_string(),
            title: "BTC".to_string(),
            target_price: Some(100_000.0),
            start_ts_ms: 0,
            end_ts_ms: 300_000,
        };
        let mut buffer = RoundBuffer::new("BTCUSDT_5m_0".to_string(), &market);
        let sample = dummy_snapshot(&buffer.round_id, 0);
        buffer.snapshots = vec![sample; 220];
        buffer.first_sample_ms = Some(0);
        buffer.last_sample_ms = Some(299_700);
        buffer.max_gap_ms = 1_400;
        buffer.target_price_latest = Some(100_000.0);
        buffer.settle_price_latest = Some(100_100.0);
        buffer.settle_price_latest_ts_ms = Some(299_700);
        let policy = RoundQualityPolicy {
            min_coverage_ratio: 0.90,
            min_sample_ratio: 0.85,
            max_gap_ms: 2_500,
            start_tolerance_ms: 35_000,
            end_tolerance_ms: 10_000,
            settle_stale_tolerance_ms: 2_500,
        };
        let report = buffer.evaluate(&policy, 100);
        assert!(!report.accept);
        assert!(report.reachable_sample_ratio > 0.95);
        assert!(report.reasons.iter().any(|r| r.starts_with("avg_gap=")));
    }

    #[test]
    fn round_end_ts_parses_numeric_round_id_ms() {
        assert_eq!(
            round_end_ts_ms_from_round_id("BTCUSDT_5m_1772279700000"),
            Some(1772280000000)
        );
    }

    #[test]
    fn round_meta_freshness_uses_round_end_plus_retention() {
        let rid = "BTCUSDT_5m_1772279700000";
        assert!(round_meta_is_fresh(rid, 1772280000000, 60_000));
        assert!(round_meta_is_fresh(rid, 1772280060000, 60_000));
        assert!(!round_meta_is_fresh(rid, 1772280060001, 60_000));
    }
}
