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
use crate::cli::IrelandRecorderArgs;
use crate::common::{
    parse_lower_csv, parse_price_from_text, parse_timestamp_ms, parse_upper_csv, timeframe_to_ms,
};
use crate::db_sink::{normalize_opt_url, run_db_sink, DbEvent, DbSinkConfig};
use crate::models::{
    ChainlinkLocal, MarketMeta, MotionState, PersistEvent, RoundRow, SnapshotRow,
    TokyoBinanceLocal, TokyoBinanceWire,
};
use crate::persist::{log_ingest, persist_event};

const MARKET_FUTURE_GUARD_MS: i64 = 500;
const MARKET_STALE_GUARD_MS: i64 = 5_000;

#[derive(Debug, Clone, Copy)]
struct RoundQualityPolicy {
    min_coverage_ratio: f64,
    min_sample_ratio: f64,
    max_gap_ms: i64,
    start_tolerance_ms: i64,
    end_tolerance_ms: i64,
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
        }
    }
}

#[derive(Debug)]
struct RoundQualityReport {
    accept: bool,
    reasons: Vec<String>,
    sample_count: usize,
    expected_samples: usize,
    coverage_ratio: f64,
    sample_ratio: f64,
    max_gap_ms: i64,
    start_delay_ms: i64,
    end_missing_ms: i64,
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
            self.settle_price_latest = Some(v);
        }
        self.snapshots.push(row);
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
        let coverage_ratio = coverage_ms as f64 / expected_window_ms as f64;
        let sample_ratio = sample_count as f64 / expected_samples.max(1) as f64;
        let start_delay_ms = first.saturating_sub(self.start_ts_ms).max(0);
        let end_missing_ms = self.end_ts_ms.saturating_sub(last).max(0);

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
        if sample_ratio < policy.min_sample_ratio {
            reasons.push(format!("sample_ratio={:.4}", sample_ratio));
        }
        if self.max_gap_ms > policy.max_gap_ms {
            reasons.push(format!("max_gap={}ms", self.max_gap_ms));
        }
        if self.target_price_latest.is_none() {
            reasons.push("missing_target_price".to_string());
        }
        if self.settle_price_latest.is_none() {
            reasons.push("missing_settle_price".to_string());
        }

        RoundQualityReport {
            accept: reasons.is_empty(),
            reasons,
            sample_count,
            expected_samples,
            coverage_ratio,
            sample_ratio,
            max_gap_ms: self.max_gap_ms,
            start_delay_ms,
            end_missing_ms,
        }
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

fn stabilize_book_quotes(book: &BookTop, cached: Option<(f64, f64, f64, f64)>) -> Option<StableQuote> {
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
        ?supported_symbols,
        ?active_symbols,
        ?active_tfs,
        api_bind = %args.api_bind,
        clickhouse_url = %args.clickhouse_url,
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
        clickhouse_round_table: args.clickhouse_round_table.clone(),
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

    if !args.api_bind.trim().is_empty() {
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

    spawn_tokyo_udp_receiver(args.udp_bind.clone(), tokyo_tx, persist_tx.clone());
    spawn_chainlink_reader(active_symbols.clone(), chainlink_tx, persist_tx.clone());
    spawn_book_reader(
        active_symbols.clone(),
        active_tfs.clone(),
        book_tx,
        persist_tx.clone(),
    );
    spawn_market_discovery_reader(
        active_symbols.clone(),
        active_tfs.clone(),
        args.discovery_refresh_sec.max(1),
        market_tx,
        persist_tx.clone(),
    );

    let mut tokyo_by_symbol: HashMap<String, TokyoBinanceLocal> = HashMap::new();
    let mut chainlink_by_symbol: HashMap<String, ChainlinkLocal> = HashMap::new();
    let mut book_by_market: HashMap<String, BookTop> = HashMap::new();
    let mut quote_cache_by_market: HashMap<String, (f64, f64, f64, f64)> = HashMap::new();
    let mut markets_by_id: HashMap<String, MarketMeta> = HashMap::new();
    let mut motion_by_key: HashMap<String, MotionState> = HashMap::new();
    let mut emitted_rounds: HashSet<String> = HashSet::new();
    let mut round_buffers: HashMap<String, RoundBuffer> = HashMap::new();
    let mut target_cache: HashMap<String, (i64, f64)> = HashMap::new();
    let vatic_http = Client::builder()
        .timeout(Duration::from_millis(1500))
        .build()?;

    let mut ingest_seq: u64 = 0;

    let mut ticker = tokio::time::interval(Duration::from_millis(sample_period_ms as u64));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            Some(msg) = tokyo_rx.recv() => {
                let now = Utc::now().timestamp_millis();
                tokyo_by_symbol.insert(msg.symbol.clone(), TokyoBinanceLocal {
                    ts_tokyo_recv_ms: msg.ts_tokyo_recv_ms,
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
                markets_by_id.clear();
                for m in markets {
                    markets_by_id.insert(m.market_id.clone(), m);
                }
            }
            _ = ticker.tick() => {
                let now_ms = Utc::now().timestamp_millis();
                for market in markets_by_id.values() {
                    if now_ms + MARKET_FUTURE_GUARD_MS < market.start_ts_ms {
                        continue;
                    }
                    if now_ms > market.end_ts_ms.saturating_add(MARKET_STALE_GUARD_MS) {
                        continue;
                    }

                    let Some(book) = book_by_market.get(&market.market_id) else {
                        continue;
                    };
                    let stable_quote = match stabilize_book_quotes(
                        book,
                        quote_cache_by_market.get(&market.market_id).copied(),
                    ) {
                        Some(v) => v,
                        None => {
                            log_ingest(
                                &persist_tx,
                                "warn",
                                "book_quality",
                                &format!("invalid book skipped market_id={}", market.market_id),
                            );
                            continue;
                        }
                    };
                    quote_cache_by_market.insert(
                        market.market_id.clone(),
                        (
                            stable_quote.bid_yes,
                            stable_quote.ask_yes,
                            stable_quote.bid_no,
                            stable_quote.ask_no,
                        ),
                    );

                    let tokyo = tokyo_by_symbol.get(&market.symbol);
                    let chainlink = chainlink_by_symbol.get(&market.symbol);
                    let binance_price = tokyo.map(|v| v.binance_price);
                    let mut target_price = market.target_price;
                    if target_price.is_none() {
                        let cache_key =
                            format!("{}|{}|{}", market.symbol, market.timeframe, market.start_ts_ms);
                        if let Some((cached_at, cached_price)) = target_cache.get(&cache_key).copied() {
                            if now_ms.saturating_sub(cached_at) <= 60_000 {
                                target_price = Some(cached_price);
                            }
                        }
                        if target_price.is_none() {
                            if let Some(v) = fetch_target_from_vatic(
                                &vatic_http,
                                &market.symbol,
                                &market.timeframe,
                                market.start_ts_ms / 1000,
                            )
                            .await
                            {
                                target_cache.insert(cache_key, (now_ms, v));
                                target_price = Some(v);
                            }
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

                    let motion_key = format!("{}|{}", market.symbol, market.timeframe);
                    let (velocity, acceleration) = match binance_price {
                        Some(px) if px.is_finite() && px > 0.0 => {
                            if let Some(prev) = motion_by_key.get(&motion_key).copied() {
                                let dt_ms = now_ms.saturating_sub(prev.ts_ms);
                                if dt_ms > 0 {
                                    let dt_s = dt_ms as f64 / 1000.0;
                                    let vel = ((px - prev.price) / prev.price) * 10_000.0 / dt_s;
                                    let acc = (vel - prev.velocity) / dt_s;
                                    motion_by_key.insert(
                                        motion_key,
                                        MotionState {
                                            ts_ms: now_ms,
                                            price: px,
                                            velocity: vel,
                                        },
                                    );
                                    (Some(vel), Some(acc))
                                } else {
                                    (Some(prev.velocity), Some(0.0))
                                }
                            } else {
                                motion_by_key.insert(
                                    motion_key,
                                    MotionState {
                                        ts_ms: now_ms,
                                        price: px,
                                        velocity: 0.0,
                                    },
                                );
                                (Some(0.0), Some(0.0))
                            }
                        }
                        _ => (None, None),
                    };

                    let round_start = market.start_ts_ms;
                    let round_id = format!("{}_{}_{}", market.symbol, market.timeframe, round_start);
                    if emitted_rounds.contains(&round_id) {
                        continue;
                    }

                    ingest_seq = ingest_seq.saturating_add(1);
                    let row = SnapshotRow {
                        schema_version: "forge_snapshot_v1",
                        ingest_seq,
                        ts_ireland_sample_ms: now_ms,
                        ts_tokyo_recv_ms: tokyo.map(|v| v.ts_tokyo_recv_ms),
                        ts_exchange_ms: tokyo.map(|v| v.ts_exchange_ms),
                        ts_ireland_recv_ms: tokyo.map(|v| v.ts_ireland_recv_ms),
                        path_lag_ms: tokyo
                            .map(|v| (v.ts_ireland_recv_ms - v.ts_tokyo_recv_ms).max(0) as f64),
                        symbol: market.symbol.clone(),
                        ts_pm_recv_ms: if book.recv_ts_local_ns > 0 {
                            book.recv_ts_local_ns / 1_000_000
                        } else {
                            book.ts_ms
                        },
                        market_id: market.market_id.clone(),
                        timeframe: market.timeframe.clone(),
                        title: market.title.clone(),
                        target_price,
                        mid_yes: (stable_quote.bid_yes + stable_quote.ask_yes) * 0.5,
                        mid_no: (stable_quote.bid_no + stable_quote.ask_no) * 0.5,
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
                        remaining_ms: market.end_ts_ms.saturating_sub(now_ms),
                        velocity_bps_per_sec: velocity,
                        acceleration,
                        round_id: round_id.clone(),
                    };
                    let round_buf = round_buffers
                        .entry(round_id.clone())
                        .or_insert_with(|| RoundBuffer::new(round_id.clone(), market));
                    if let Some(tx) = sink_tx.as_ref() {
                        if let Err(err) = tx.send(DbEvent::Snapshot(Box::new(row.clone()))).await {
                            tracing::warn!(?err, "db sink closed while sending live snapshot");
                        }
                    }
                    round_buf.push_snapshot(row);

                    let remaining_ms = market.end_ts_ms.saturating_sub(now_ms);
                    if remaining_ms <= 0 && !emitted_rounds.contains(&round_id) {
                        emitted_rounds.insert(round_id.clone());
                        if let Some(buffer) = round_buffers.remove(&round_id) {
                            let report = buffer.evaluate(&quality_policy, sample_period_ms);
                            let target = buffer.target_price_latest.unwrap_or(0.0);
                            let settle = buffer.settle_price_latest.unwrap_or(0.0);
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
                            let snapshot_count = buffer.snapshots.len();
                            if let Err(err) = commit_tx
                                .send(RoundCommit {
                                    snapshots: buffer.snapshots,
                                    round,
                                })
                                .await
                            {
                                tracing::error!(?err, "commit queue closed; dropping round");
                            } else {
                                if report.accept {
                                    log_ingest(
                                        &persist_tx,
                                        "info",
                                        "round_quality",
                                        &format!(
                                            "accepted {} samples={} expected={} coverage={:.3} sample_ratio={:.3} max_gap_ms={}",
                                            round_id,
                                            report.sample_count,
                                            report.expected_samples,
                                            report.coverage_ratio,
                                            report.sample_ratio,
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
                                            "quality_warn {} samples={} expected={} coverage={:.3} sample_ratio={:.3} max_gap_ms={} start_delay_ms={} end_missing_ms={} reason={}",
                                            round_id,
                                            report.sample_count,
                                            report.expected_samples,
                                            report.coverage_ratio,
                                            report.sample_ratio,
                                            report.max_gap_ms,
                                            report.start_delay_ms,
                                            report.end_missing_ms,
                                            reason
                                        ),
                                    );
                                }
                                tracing::info!(
                                    round_id = %round_id,
                                    sample_count = snapshot_count,
                                    expected_samples = report.expected_samples,
                                    coverage_ratio = report.coverage_ratio,
                                    sample_ratio = report.sample_ratio,
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
                        if now_ms > buf.end_ts_ms.saturating_add(MARKET_STALE_GUARD_MS)
                            && !emitted_rounds.contains(rid)
                        {
                            Some(rid.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                for rid in stale_round_ids {
                    emitted_rounds.insert(rid.clone());
                    if let Some(buffer) = round_buffers.remove(&rid) {
                        let report = buffer.evaluate(&quality_policy, sample_period_ms);
                        let target = buffer.target_price_latest.unwrap_or(0.0);
                        let settle = buffer.settle_price_latest.unwrap_or(0.0);
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
                            tracing::error!(?err, "commit queue closed; dropping stale round");
                            continue;
                        }
                        log_ingest(
                            &persist_tx,
                            "warn",
                            "round_quality",
                            &format!(
                                "stale_committed {} samples={} expected={} coverage={:.3} sample_ratio={:.3} max_gap_ms={} reason={}",
                                rid,
                                report.sample_count,
                                report.expected_samples,
                                report.coverage_ratio,
                                report.sample_ratio,
                                report.max_gap_ms,
                                reason
                            ),
                        );
                        tracing::warn!(
                            round_id = %rid,
                            sample_count = report.sample_count,
                            expected_samples = report.expected_samples,
                            coverage_ratio = report.coverage_ratio,
                            sample_ratio = report.sample_ratio,
                            max_gap_ms = report.max_gap_ms,
                            reasons = %reason,
                            "stale round committed with quality warning"
                        );
                    }
                }
            }
        }
    }
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
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
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
                if tx.send(book).is_err() {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
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
    tokio::spawn(async move {
        loop {
            match discover_markets(&symbols, &timeframes).await {
                Ok(markets) => {
                    log_ingest(
                        &persist,
                        "info",
                        "discovery",
                        &format!("discovered {} active markets", markets.len()),
                    );
                    if tx.send(markets).is_err() {
                        return;
                    }
                }
                Err(err) => {
                    log_ingest(
                        &persist,
                        "warn",
                        "discovery",
                        &format!("discover failed: {err}"),
                    );
                }
            }
            tokio::time::sleep(Duration::from_secs(refresh_sec.max(1))).await;
        }
    });
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

    Ok(out)
}

fn to_market_meta(m: MarketDescriptor, tf: String, start_ts_ms: i64, end_ts_ms: i64) -> MarketMeta {
    let symbol = m.symbol.to_ascii_uppercase();
    MarketMeta {
        market_id: m.market_id,
        symbol,
        timeframe: tf,
        title: m.question.clone(),
        target_price: parse_price_from_text(&m.question),
        end_ts_ms,
        start_ts_ms,
    }
}

async fn fetch_target_from_vatic(
    http: &Client,
    symbol: &str,
    timeframe: &str,
    timestamp_sec: i64,
) -> Option<f64> {
    let market_type = match timeframe {
        "5m" => "5min",
        "15m" => "15min",
        _ => return None,
    };
    let asset = symbol
        .strip_suffix("USDT")
        .unwrap_or(symbol)
        .to_ascii_lowercase();
    let url = format!(
        "https://api.vatic.trading/api/v1/targets/timestamp?asset={asset}&type={market_type}&timestamp={timestamp_sec}"
    );
    let resp = http.get(url).send().await.ok()?;
    let json: serde_json::Value = resp.json().await.ok()?;
    json.get("target_price")
        .and_then(|v| v.as_f64())
        .or_else(|| json.get("target").and_then(|v| v.as_f64()))
        .or_else(|| json.get("price").and_then(|v| v.as_f64()))
}
