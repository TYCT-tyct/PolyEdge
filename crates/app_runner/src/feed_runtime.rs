use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::Utc;
use core_types::{EngineEvent, MarketFeed, RefPriceFeed, RefTick, SourceHealth};
use feed_polymarket::PolymarketFeed;
use feed_reference::MultiSourceRefFeed;
use futures::StreamExt;
use infra_bus::RingBus;
use smol_str::SmolStr;
use tokio::sync::{mpsc, RwLock};

use crate::fusion_engine::{
    is_anchor_ref_source, is_ref_tick_duplicate, max_book_top_diff_bps, ref_event_ts_ms,
    should_log_ref_tick, should_replace_anchor_tick, should_replace_ref_tick,
    upsert_latest_tick_slot_local,
};
use crate::report_io::{append_jsonl_line, dataset_path, sha256_hex};
use crate::state::{
    EngineShared, FusionConfig, ShadowStats, SourceHealthConfig, StrategyIngress,
    StrategyIngressMsg,
};
use crate::stats_utils::{now_ns, value_to_f64};
use crate::{publish_if_telemetry_subscribers, spawn_detached};

pub(super) fn spawn_reference_feed(
    bus: RingBus<EngineEvent>,
    stats: Arc<ShadowStats>,
    symbols: Vec<String>,
    fusion_cfg: Arc<RwLock<FusionConfig>>,
    shared: Arc<EngineShared>,
    strategy_tx: mpsc::Sender<StrategyIngressMsg>,
) {
    #[derive(Clone, Copy)]
    enum RefLane {
        Direct,
        Udp,
    }
    const TS_INVERSION_TOLERANCE_MS: i64 = 250;
    const TS_BACKJUMP_RESET_MS: i64 = 5_000;
    spawn_detached("reference_feed_orchestrator", true, async move {
        let symbols = if symbols.is_empty() {
            vec!["BTCUSDT".to_string()]
        } else {
            symbols
        };
        let ref_merge_queue_cap = std::env::var("POLYEDGE_REF_MERGE_QUEUE_CAP")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(4_096)
            .clamp(1_024, 32_768);
        let ref_merge_drop_on_full = std::env::var("POLYEDGE_REF_MERGE_DROP_ON_FULL")
            .ok()
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "True"))
            .unwrap_or(true);
        let strategy_ingress_drop_on_full = std::env::var("POLYEDGE_STRATEGY_INGRESS_DROP_ON_FULL")
            .ok()
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "True"))
            .unwrap_or(true);
        let strategy_ref_ingress_enabled = std::env::var("POLYEDGE_STRATEGY_REF_INGRESS_ENABLED")
            .ok()
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "True"))
            .unwrap_or(false);
        let (tx, mut rx) = mpsc::channel::<(RefLane, Result<RefTick>)>(ref_merge_queue_cap);

        let tx_direct = tx.clone();
        let symbols_direct = symbols.clone();
        spawn_detached("reference_feed_direct_lane", true, async move {
            loop {
                let feed = MultiSourceRefFeed::new(Duration::from_millis(50));
                match feed.stream_ticks(symbols_direct.clone()).await {
                    Ok(mut stream) => {
                        while let Some(item) = stream.next().await {
                            let lane_item = (RefLane::Direct, item);
                            if ref_merge_drop_on_full {
                                match tx_direct.try_send(lane_item) {
                                    Ok(()) => {}
                                    Err(mpsc::error::TrySendError::Full(_)) => {
                                        metrics::counter!("fusion.ref_merge_drop").increment(1);
                                    }
                                    Err(mpsc::error::TrySendError::Closed(_)) => return,
                                }
                            } else if tx_direct.send(lane_item).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(err) => tracing::warn!(?err, "direct reference feed failed to start"),
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });

        let tx_udp = tx.clone();
        let symbols_udp = symbols.clone();
        let fusion_cfg_udp = fusion_cfg.clone();
        spawn_detached("reference_feed_udp_lane", true, async move {
            loop {
                let cfg = fusion_cfg_udp.read().await.clone();
                if !cfg.enable_udp || cfg.mode == "direct_only" {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
                let feed = feed_udp::UdpBinanceFeed::new(cfg.udp_port);
                match feed.stream_ticks(symbols_udp.clone()).await {
                    Ok(mut stream) => {
                        while let Some(item) = stream.next().await {
                            let lane_item = (RefLane::Udp, item);
                            if ref_merge_drop_on_full {
                                match tx_udp.try_send(lane_item) {
                                    Ok(()) => {}
                                    Err(mpsc::error::TrySendError::Full(_)) => {
                                        metrics::counter!("fusion.ref_merge_drop").increment(1);
                                    }
                                    Err(mpsc::error::TrySendError::Closed(_)) => return,
                                }
                            } else if tx_udp.send(lane_item).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(err) => tracing::warn!(?err, "udp reference feed failed to start"),
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
        drop(tx);

        let mut ingest_seq: u64 = 0;
        let mut last_source_ts_by_stream: HashMap<String, i64> = HashMap::new();
        let mut last_published_by_symbol: HashMap<String, (i64, f64)> = HashMap::new();
        let mut source_runtime: HashMap<SmolStr, SourceRuntimeStats> = HashMap::new();
        let mut latest_price_by_symbol_source: HashMap<String, HashMap<SmolStr, f64>> =
            HashMap::new();
        let mut accepted_fast_mix_by_symbol: HashMap<String, (u64, u64)> = HashMap::new();
        let mut local_fast_ticks: HashMap<String, RefTick> = HashMap::new();
        let mut local_anchor_ticks: HashMap<String, RefTick> = HashMap::new();
        let mut accepted_fast_mix_total: (u64, u64) = (0, 0);
        let mut fusion = fusion_cfg.read().await.clone();
        let mut last_fusion_mode = fusion.mode.clone();
        let fusion_cfg_refresh_interval_ms = std::env::var("POLYEDGE_FUSION_CFG_REFRESH_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(200)
            .clamp(50, 2_000);
        let mut fusion_cfg_refresh_at = Instant::now();
        let mut source_health_cfg = shared.source_health_cfg.read().await.clone();
        let mut source_health_cfg_refresh_at = Instant::now();
        let ref_tick_bus_enabled = std::env::var("POLYEDGE_REF_TICK_BUS_ENABLED")
            .ok()
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "True"))
            .unwrap_or(false);

        while let Some((lane, item)) = rx.recv().await {
            match item {
                Ok(tick) => {
                    if fusion_cfg_refresh_at.elapsed()
                        >= Duration::from_millis(fusion_cfg_refresh_interval_ms)
                    {
                        let next_fusion = fusion_cfg.read().await.clone();
                        if next_fusion.mode != last_fusion_mode {
                            accepted_fast_mix_by_symbol.clear();
                            accepted_fast_mix_total = (0, 0);
                            stats
                                .record_issue("fusion_mode_switch_fast_cache_reset")
                                .await;
                            last_fusion_mode = next_fusion.mode.clone();
                        }
                        fusion = next_fusion;
                        fusion_cfg_refresh_at = Instant::now();
                    }
                    let is_anchor = is_anchor_ref_source(tick.source.as_str());

                    if fusion.mode == "direct_only"
                        && !matches!(lane, RefLane::Direct)
                        && !is_anchor
                    {
                        continue;
                    }
                    ingest_seq = ingest_seq.saturating_add(1);
                    let source_ts = tick.event_ts_exchange_ms.max(tick.event_ts_ms);
                    let source_seq = source_ts.max(0) as u64;
                    let valid = !tick.symbol.is_empty()
                        && tick.price.is_finite()
                        && tick.price > 0.0
                        && source_seq > 0;
                    stats.mark_data_validity(valid);
                    let stream_key = format!("{}:{}", tick.source, tick.symbol);
                    if let Some(prev) = last_source_ts_by_stream.get(&stream_key).copied() {
                        if source_ts + TS_INVERSION_TOLERANCE_MS < prev {
                            let back_jump_ms = prev.saturating_sub(source_ts);
                            if back_jump_ms > TS_BACKJUMP_RESET_MS {
                                stats.record_issue("ref_ts_backjump_reset").await;
                            } else {
                                stats.mark_ts_inversion();
                            }
                        }
                    }
                    last_source_ts_by_stream.insert(stream_key, source_ts);
                    if let Some((prev_ts, prev_px)) = last_published_by_symbol.get(&tick.symbol) {
                        if is_ref_tick_duplicate(source_ts, tick.price, *prev_ts, *prev_px, &fusion)
                        {
                            stats.mark_ref_dedupe_dropped();
                            continue;
                        }
                    }
                    last_published_by_symbol.insert(tick.symbol.clone(), (source_ts, tick.price));
                    if source_health_cfg_refresh_at.elapsed() >= Duration::from_millis(500) {
                        source_health_cfg = shared.source_health_cfg.read().await.clone();
                        source_health_cfg_refresh_at = Instant::now();
                    }
                    let source_key = tick.source.clone();
                    let symbol_key = tick.symbol.clone();
                    let recv_ts_ms = tick.recv_ts_ms;
                    if source_key == "binance_ws" {
                        let _recv_ns = if tick.recv_ts_local_ns > 0 {
                            tick.recv_ts_local_ns
                        } else {
                            now_ns()
                        };
                    }
                    let latency_ms = recv_ts_ms.saturating_sub(source_ts).max(0) as f64;
                    let runtime = source_runtime.entry(source_key.clone()).or_default();
                    runtime.sample_count = runtime.sample_count.saturating_add(1);
                    runtime.latency_sum_ms += latency_ms;
                    runtime.latency_sq_sum_ms += latency_ms * latency_ms;
                    if runtime.last_event_ts_ms > 0
                        && source_ts + TS_INVERSION_TOLERANCE_MS < runtime.last_event_ts_ms
                    {
                        runtime.out_of_order_count = runtime.out_of_order_count.saturating_add(1);
                    }
                    if runtime.last_recv_ts_ms > 0
                        && recv_ts_ms.saturating_sub(runtime.last_recv_ts_ms)
                            > source_health_cfg.gap_window_ms
                    {
                        runtime.gap_count = runtime.gap_count.saturating_add(1);
                    }
                    runtime.last_event_ts_ms = source_ts;
                    runtime.last_recv_ts_ms = recv_ts_ms;

                    {
                        let per_symbol = latest_price_by_symbol_source
                            .entry(symbol_key.clone())
                            .or_default();
                        per_symbol.insert(source_key.clone(), tick.price);
                        if per_symbol.len() >= 2 {
                            let values = per_symbol.values().copied().collect::<Vec<_>>();
                            if let Some(median) = median_price(&values) {
                                if median.is_finite() && median > 0.0 {
                                    let dev_bps = ((tick.price - median).abs() / median * 10_000.0)
                                        .clamp(0.0, 10_000.0);
                                    if runtime.sample_count <= 1 {
                                        runtime.deviation_ema_bps = dev_bps;
                                    } else {
                                        runtime.deviation_ema_bps =
                                            (runtime.deviation_ema_bps * 0.90) + (dev_bps * 0.10);
                                    }
                                }
                            }
                        }
                    }
                    let source_health = build_source_health_snapshot(
                        source_key.as_str(),
                        runtime,
                        &source_health_cfg,
                        Utc::now().timestamp_millis(),
                    );
                    if runtime.sample_count % 8 == 0 {
                        let mut map = shared.source_health_latest.write().await;
                        map.insert(source_key.to_string(), source_health.clone());
                    }

                    if should_log_ref_tick(ingest_seq) {
                        let tick_json =
                            serde_json::to_string(&tick).unwrap_or_else(|_| "{}".to_string());
                        let line = format!(
                            "{{\"ts_ms\":{},\"source_seq\":{},\"ingest_seq\":{},\"valid\":{},\"tick\":{}}}",
                            Utc::now().timestamp_millis(),
                            source_seq,
                            ingest_seq,
                            valid,
                            tick_json
                        );
                        append_jsonl_line(&dataset_path("raw", "ref_ticks.jsonl"), line);
                    }
                    if !valid {
                        stats.record_issue("invalid_ref_tick").await;
                        continue;
                    }
                    stats.mark_ref_tick(tick.source.as_str(), tick.recv_ts_ms);
                    if source_key == "binance_udp" || source_key == "binance_ws" {
                        let entry = accepted_fast_mix_by_symbol
                            .entry(symbol_key.clone())
                            .or_default();
                        if source_key == "binance_udp" {
                            entry.0 = entry.0.saturating_add(1);
                            accepted_fast_mix_total.0 = accepted_fast_mix_total.0.saturating_add(1);
                        } else {
                            entry.1 = entry.1.saturating_add(1);
                            accepted_fast_mix_total.1 = accepted_fast_mix_total.1.saturating_add(1);
                        }
                        let total = entry.0.saturating_add(entry.1);
                        if total > 100_000 {
                            entry.0 /= 2;
                            entry.1 /= 2;
                        }
                        let total_global = accepted_fast_mix_total
                            .0
                            .saturating_add(accepted_fast_mix_total.1);
                        if total_global > 1_000_000 {
                            accepted_fast_mix_total.0 /= 2;
                            accepted_fast_mix_total.1 /= 2;
                        }
                    }

                    if is_anchor {
                        let _ = upsert_latest_tick_slot_local(
                            &mut local_anchor_ticks,
                            tick.clone(),
                            should_replace_anchor_tick,
                        );
                    } else if let Some(delta_ns) = upsert_latest_tick_slot_local(
                        &mut local_fast_ticks,
                        tick.clone(),
                        should_replace_ref_tick,
                    ) {
                        metrics::histogram!("fusion.arrive_delta_ns").record(delta_ns as f64);
                    }

                    if strategy_ref_ingress_enabled {
                        let ingress = StrategyIngressMsg {
                            enqueued_ns: now_ns(),
                            payload: StrategyIngress::RefTick(tick.clone()),
                        };
                        if strategy_ingress_drop_on_full {
                            match strategy_tx.try_send(ingress) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Full(_)) => {
                                    metrics::counter!("strategy.ingress_ref_drop").increment(1);
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => break,
                            }
                        } else if strategy_tx.send(ingress).await.is_err() {
                            break;
                        }
                    }

                    if ref_tick_bus_enabled {
                        publish_if_telemetry_subscribers(&bus, EngineEvent::RefTick(tick));
                    }
                }
                Err(err) => {
                    tracing::warn!(?err, "reference feed event error");
                }
            }
        }
    });
}

pub(super) fn spawn_settlement_feed(shared: Arc<EngineShared>) {
    spawn_detached("settlement_feed_orchestrator", true, async move {
        loop {
            let cfg = shared.settlement_cfg.read().await.clone();
            if !cfg.enabled || cfg.endpoint.trim().is_empty() {
                tokio::time::sleep(Duration::from_millis(1_000)).await;
                continue;
            }

            let req = shared
                .http
                .get(cfg.endpoint.clone())
                .timeout(Duration::from_millis(cfg.timeout_ms.max(100)));
            match req.send().await {
                Ok(resp) => match resp.json::<serde_json::Value>().await {
                    Ok(value) => {
                        let mut updates = HashMap::<String, f64>::new();
                        let maybe_object = value.as_object();
                        if let Some(obj) = maybe_object {
                            for symbol in &cfg.symbols {
                                for key in settlement_symbol_keys(symbol) {
                                    if let Some(price) = obj.get(&key).and_then(value_to_f64) {
                                        if price.is_finite() && price > 0.0 {
                                            updates.insert(symbol.clone(), price);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        if !updates.is_empty() {
                            let mut map = shared.settlement_prices.write().await;
                            for (k, v) in updates {
                                map.insert(k, v);
                            }
                        }
                    }
                    Err(err) => {
                        tracing::warn!(?err, "settlement feed json decode failed");
                        metrics::counter!("settlement.feed.decode_error").increment(1);
                    }
                },
                Err(err) => {
                    tracing::warn!(?err, "settlement feed poll failed");
                    metrics::counter!("settlement.feed.poll_error").increment(1);
                }
            }

            tokio::time::sleep(Duration::from_millis(cfg.poll_interval_ms.max(250))).await;
        }
    });
}

pub(super) fn settlement_symbol_keys(symbol: &str) -> Vec<String> {
    let mut keys = Vec::with_capacity(4);
    let normalized = symbol.trim().to_ascii_uppercase();
    keys.push(normalized.clone());
    if let Some(base) = normalized.strip_suffix("USDT") {
        keys.push(base.to_string());
        keys.push(format!("{base}USD"));
    }
    keys.push(normalized.replace('_', ""));
    keys
}

pub(super) async fn settlement_prob_yes_for_symbol(
    shared: &Arc<EngineShared>,
    symbol: &str,
    p_fast_yes: f64,
    latest_fast_ticks: &std::collections::HashMap<String, RefTick>,
    latest_anchor_ticks: &std::collections::HashMap<String, RefTick>,
    now_ms: i64,
) -> Option<f64> {
    let settle_price = {
        let anchor = latest_anchor_ticks
            .get(symbol)
            .filter(|tick| {
                is_anchor_ref_source(tick.source.as_str())
                    && now_ms.saturating_sub(ref_event_ts_ms(tick)) <= 5_000
            })
            .map(|tick| tick.price);
        if let Some(v) = anchor {
            Some(v)
        } else {
            shared.settlement_prices.read().await.get(symbol).copied()
        }
    }?;
    if settle_price <= 0.0 {
        return None;
    }
    let fast_price = latest_fast_ticks.get(symbol).map(|t| t.price)?;
    if fast_price <= 0.0 {
        return None;
    }
    Some(blend_settlement_probability(
        p_fast_yes,
        fast_price,
        settle_price,
    ))
}

#[inline]
pub(super) fn blend_settlement_probability(
    p_fast_yes: f64,
    fast_price: f64,
    settle_price: f64,
) -> f64 {
    // When fast and settlement feeds diverge, pull probability toward 0.5.
    // This keeps execution conservative until settlement alignment recovers.
    let gap = ((fast_price - settle_price) / settle_price)
        .abs()
        .clamp(0.0, 0.03);
    let settle_blend = (gap * 20.0).clamp(0.0, 0.25);
    let p = (p_fast_yes.clamp(0.0, 1.0) * (1.0 - settle_blend)) + (0.5 * settle_blend);
    p.clamp(0.0, 1.0)
}

#[derive(Debug, Default, Clone)]
pub(super) struct SourceRuntimeStats {
    pub(super) sample_count: u64,
    pub(super) latency_sum_ms: f64,
    pub(super) latency_sq_sum_ms: f64,
    pub(super) out_of_order_count: u64,
    pub(super) gap_count: u64,
    pub(super) last_event_ts_ms: i64,
    pub(super) last_recv_ts_ms: i64,
    pub(super) deviation_ema_bps: f64,
}

pub(super) fn median_price(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let mid = sorted.len() / 2;
    Some(if sorted.len() % 2 == 0 {
        (sorted[mid - 1] + sorted[mid]) * 0.5
    } else {
        sorted[mid]
    })
}

pub(super) fn build_source_health_snapshot(
    source: &str,
    stats: &SourceRuntimeStats,
    cfg: &SourceHealthConfig,
    now_ms: i64,
) -> SourceHealth {
    let n = stats.sample_count.max(1) as f64;
    let latency_ms = (stats.latency_sum_ms / n).max(0.0);
    let variance = (stats.latency_sq_sum_ms / n) - (latency_ms * latency_ms);
    let jitter_ms = variance.max(0.0).sqrt();
    let out_of_order_rate = (stats.out_of_order_count as f64 / n).clamp(0.0, 1.0);
    let gap_rate = (stats.gap_count as f64 / n).clamp(0.0, 1.0);
    let price_deviation_bps = stats.deviation_ema_bps.max(0.0);
    let freshness_age_ms = if stats.last_recv_ts_ms > 0 {
        now_ms.saturating_sub(stats.last_recv_ts_ms).max(0) as f64
    } else {
        cfg.freshness_limit_ms * 4.0
    };
    let freshness_penalty = (freshness_age_ms / cfg.freshness_limit_ms.max(1e-6)).clamp(0.0, 2.0);
    let freshness_score = (1.0 - freshness_penalty * 0.5).clamp(0.0, 1.0);

    let jitter_penalty = (jitter_ms / cfg.jitter_limit_ms.max(1e-6)).clamp(0.0, 2.0);
    let deviation_penalty =
        (price_deviation_bps / cfg.deviation_limit_bps.max(1e-6)).clamp(0.0, 2.0);
    let out_of_order_penalty = (out_of_order_rate / 0.02).clamp(0.0, 2.0);
    let gap_penalty = (gap_rate / 0.02).clamp(0.0, 2.0);
    let coverage = (stats.sample_count as f64 / cfg.min_samples.max(1) as f64).clamp(0.0, 1.0);
    let raw_score = 1.0
        - (0.30 * jitter_penalty)
        - (0.25 * deviation_penalty)
        - (0.18 * out_of_order_penalty)
        - (0.12 * gap_penalty)
        - (0.15 * freshness_penalty);
    let score = (raw_score.clamp(0.0, 1.0) * (0.25 + 0.75 * coverage)).clamp(0.0, 1.0);

    SourceHealth {
        source: source.to_string(),
        latency_ms,
        jitter_ms,
        out_of_order_rate,
        gap_rate,
        price_deviation_bps,
        freshness_score,
        score,
        sample_count: stats.sample_count,
        ts_ms: now_ms,
    }
}

pub(super) fn spawn_market_feed(
    bus: RingBus<EngineEvent>,
    stats: Arc<ShadowStats>,
    symbols: Vec<String>,
    market_types: Vec<String>,
    timeframes: Vec<String>,
    strategy_tx: mpsc::Sender<StrategyIngressMsg>,
) {
    const TS_INVERSION_TOLERANCE_MS: i64 = 250;
    const TS_BACKJUMP_RESET_MS: i64 = 5_000;
    // If we see *no* market messages for this long, treat the WS stream as stuck and reconnect.
    // Keep this comfortably above "normal quiet" to avoid hammering gamma discovery on reconnection.
    // 5s is too aggressive for quiet windows and creates reconnect churn.
    // Keep stale protection, but allow a calmer idle window to reduce needless reconnects.
    const BOOK_IDLE_TIMEOUT_MS: u64 = 20_000;
    const RECONNECT_BASE_MS: u64 = 250;
    const RECONNECT_MAX_MS: u64 = 10_000;
    spawn_detached("market_feed_orchestrator", true, async move {
        let strategy_ingress_drop_on_full = std::env::var("POLYEDGE_STRATEGY_INGRESS_DROP_ON_FULL")
            .ok()
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "True"))
            .unwrap_or(true);
        let strategy_book_dedupe_window_ms =
            std::env::var("POLYEDGE_BOOK_INGRESS_DEDUPE_WINDOW_MS")
                .ok()
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(25)
                .clamp(0, 500);
        let strategy_book_dedupe_bps = std::env::var("POLYEDGE_BOOK_INGRESS_DEDUPE_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.05)
            .clamp(0.0, 5.0);
        let mut reconnects: u64 = 0;
        loop {
            let feed = PolymarketFeed::new_with_universe(
                Duration::from_millis(50),
                symbols.clone(),
                market_types.clone(),
                timeframes.clone(),
            );
            let Ok(mut stream) = feed.stream_books().await else {
                reconnects = reconnects.saturating_add(1);
                let backoff_ms =
                    (RECONNECT_BASE_MS.saturating_mul(reconnects.min(40))).min(RECONNECT_MAX_MS);
                tracing::warn!(
                    reconnects,
                    backoff_ms,
                    "market feed failed to start; reconnecting"
                );
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                continue;
            };
            let mut ingest_seq: u64 = 0;
            let mut last_source_ts_by_market: HashMap<String, i64> = HashMap::new();
            let mut last_enqueued_book_by_market: HashMap<String, (i64, f64, f64, f64, f64)> =
                HashMap::new();
            // Only reset reconnect backoff once we observe actual book traffic (not just a successful
            // handshake). This avoids a reconnect storm when discovery/WS is returning 200 but no
            // updates are delivered.
            let mut saw_any_book = false;

            loop {
                let next = tokio::time::timeout(
                    Duration::from_millis(BOOK_IDLE_TIMEOUT_MS),
                    stream.next(),
                )
                .await;
                let item = match next {
                    Ok(v) => v,
                    Err(_) => {
                        tracing::warn!(
                            timeout_ms = BOOK_IDLE_TIMEOUT_MS,
                            saw_any_book,
                            reconnects,
                            "market feed idle timeout; reconnecting"
                        );
                        break;
                    }
                };

                let Some(item) = item else {
                    tracing::warn!("market feed stream ended; reconnecting");
                    break;
                };

                match item {
                    Ok(book) => {
                        if !saw_any_book {
                            saw_any_book = true;
                            if reconnects > 0 {
                                tracing::info!(reconnects, "market feed reconnected");
                            }
                            reconnects = 0;
                        }
                        ingest_seq = ingest_seq.saturating_add(1);
                        let source_ts = book.ts_ms;
                        let source_seq = source_ts.max(0) as u64;
                        let valid = !book.market_id.is_empty()
                            && book.bid_yes.is_finite()
                            && book.ask_yes.is_finite()
                            && book.bid_no.is_finite()
                            && book.ask_no.is_finite()
                            && source_seq > 0;
                        stats.mark_data_validity(valid);
                        if let Some(prev) = last_source_ts_by_market.get(&book.market_id).copied() {
                            if source_ts + TS_INVERSION_TOLERANCE_MS < prev {
                                let back_jump_ms = prev.saturating_sub(source_ts);
                                if back_jump_ms > TS_BACKJUMP_RESET_MS {
                                    stats.record_issue("book_ts_backjump_reset").await;
                                } else {
                                    stats.mark_ts_inversion();
                                }
                            }
                        }
                        last_source_ts_by_market.insert(book.market_id.clone(), source_ts);
                        // IMPORTANT: book freshness must be based on *local* receive time, not the
                        // exchange/server-provided ts_ms (which can be skewed/backjump).
                        let book_tick_ms = if book.recv_ts_local_ns > 0 {
                            (book.recv_ts_local_ns / 1_000_000).max(0)
                        } else {
                            Utc::now().timestamp_millis()
                        };
                        stats.mark_book_tick(book_tick_ms);
                        let book_json =
                            serde_json::to_string(&book).unwrap_or_else(|_| "{}".to_string());
                        let hash = sha256_hex(&book_json);
                        let line = format!(
                            "{{\"ts_ms\":{},\"source_seq\":{},\"ingest_seq\":{},\"valid\":{},\"sha256\":\"{}\",\"book\":{}}}",
                            Utc::now().timestamp_millis(),
                            source_seq,
                            ingest_seq,
                            valid,
                            hash,
                            book_json
                        );
                        append_jsonl_line(&dataset_path("raw", "book_tops.jsonl"), line);
                        if !valid {
                            stats.record_issue("invalid_book_top").await;
                            continue;
                        }

                        if strategy_book_dedupe_window_ms > 0 {
                            let book_recv_ms = if book.recv_ts_local_ns > 0 {
                                (book.recv_ts_local_ns / 1_000_000).max(0)
                            } else {
                                Utc::now().timestamp_millis()
                            };
                            if let Some((
                                prev_recv_ms,
                                prev_bid_yes,
                                prev_ask_yes,
                                prev_bid_no,
                                prev_ask_no,
                            )) = last_enqueued_book_by_market.get(&book.market_id)
                            {
                                let within_window = (book_recv_ms - *prev_recv_ms).abs()
                                    <= strategy_book_dedupe_window_ms;
                                let max_diff_bps = max_book_top_diff_bps(
                                    *prev_bid_yes,
                                    *prev_ask_yes,
                                    *prev_bid_no,
                                    *prev_ask_no,
                                    &book,
                                );
                                if within_window && max_diff_bps <= strategy_book_dedupe_bps {
                                    metrics::counter!("strategy.ingress_book_dedupe_drop")
                                        .increment(1);
                                    continue;
                                }
                            }
                            last_enqueued_book_by_market.insert(
                                book.market_id.clone(),
                                (
                                    book_recv_ms,
                                    book.bid_yes,
                                    book.ask_yes,
                                    book.bid_no,
                                    book.ask_no,
                                ),
                            );
                        }

                        let ingress = StrategyIngressMsg {
                            enqueued_ns: now_ns(),
                            payload: StrategyIngress::BookTop(book.clone()),
                        };
                        if strategy_ingress_drop_on_full {
                            match strategy_tx.try_send(ingress) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Full(_)) => {
                                    metrics::counter!("strategy.ingress_book_drop").increment(1);
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => break,
                            }
                        } else if strategy_tx.send(ingress).await.is_err() {
                            break;
                        }

                        publish_if_telemetry_subscribers(&bus, EngineEvent::BookTop(book));
                    }
                    Err(err) => {
                        tracing::warn!(?err, "market feed event error");
                    }
                }
            }

            reconnects = reconnects.saturating_add(1);
            let backoff_ms =
                (RECONNECT_BASE_MS.saturating_mul(reconnects.min(40))).min(RECONNECT_MAX_MS);
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        }
    });
}
