use std::time::{Duration, Instant};

use chrono::Utc;
use core_types::{BookTop, EdgeAttribution, OrderIntentV2, OrderSide, QuoteIntent, ShadowShot};
use reqwest::Client;
use serde::Serialize;

use crate::report_io::{append_jsonl, dataset_path};
use crate::state::{EngineShared, FeeRateEntry, ScoringState};
use crate::stats_utils::value_to_f64;
use crate::strategy_policy::estimate_queue_fill_prob;
use crate::spawn_detached;

pub(super) async fn get_fee_rate_bps_cached(shared: &EngineShared, market_id: &str) -> f64 {
    const DEFAULT_FEE_BPS: f64 = 2.0;
    const TTL: Duration = Duration::from_secs(60);
    const REFRESH_BACKOFF: Duration = Duration::from_secs(3);

    let now = Instant::now();
    let (cached_fee, needs_refresh) =
        if let Some(entry) = shared.fee_cache.read().await.get(market_id).cloned() {
            (
                entry.fee_bps,
                now.duration_since(entry.fetched_at) >= TTL || entry.fee_bps <= 0.0,
            )
        } else {
            (DEFAULT_FEE_BPS, true)
        };

    if needs_refresh {
        maybe_spawn_fee_refresh(shared, market_id, now, REFRESH_BACKOFF).await;
    }

    cached_fee
}

pub(super) async fn get_rebate_bps_cached(
    shared: &EngineShared,
    market_id: &str,
    fee_bps: f64,
) -> f64 {
    const TTL: Duration = Duration::from_secs(120);
    let now = Instant::now();
    let maybe = shared.scoring_cache.read().await.get(market_id).cloned();
    match maybe {
        Some(entry) if now.duration_since(entry.fetched_at) <= TTL => {
            entry.rebate_bps_est.clamp(0.0, fee_bps.max(0.0))
        }
        _ => 0.0,
    }
}

pub(super) async fn maybe_spawn_fee_refresh(
    shared: &EngineShared,
    market_id: &str,
    now: Instant,
    refresh_backoff: Duration,
) {
    {
        let inflight = shared.fee_refresh_inflight.read().await;
        if let Some(last_attempt) = inflight.get(market_id) {
            if now.duration_since(*last_attempt) < refresh_backoff {
                return;
            }
        }
    }

    {
        let mut inflight = shared.fee_refresh_inflight.write().await;
        if let Some(last_attempt) = inflight.get(market_id) {
            if now.duration_since(*last_attempt) < refresh_backoff {
                return;
            }
        }
        inflight.insert(market_id.to_string(), now);
    }

    let market = market_id.to_string();
    let http = shared.http.clone();
    let clob_endpoint = shared.clob_endpoint.clone();
    let fee_cache = shared.fee_cache.clone();
    let inflight = shared.fee_refresh_inflight.clone();
    spawn_detached("fee_refresh", false, async move {
        if let Some(fee_bps) = fetch_fee_rate_bps(&http, &clob_endpoint, &market).await {
            fee_cache.write().await.insert(
                market.clone(),
                FeeRateEntry {
                    fee_bps,
                    fetched_at: Instant::now(),
                },
            );
        }
        inflight.write().await.remove(&market);
    });
}

pub(super) async fn maybe_spawn_scoring_refresh(
    shared: &EngineShared,
    market_id: &str,
    order_id: &str,
    fee_bps: f64,
    now: Instant,
    refresh_backoff: Duration,
) {
    if market_id.is_empty() || order_id.is_empty() {
        return;
    }
    {
        let inflight = shared.scoring_refresh_inflight.read().await;
        if let Some(last_attempt) = inflight.get(market_id) {
            if now.duration_since(*last_attempt) < refresh_backoff {
                return;
            }
        }
    }
    {
        let mut inflight = shared.scoring_refresh_inflight.write().await;
        if let Some(last_attempt) = inflight.get(market_id) {
            if now.duration_since(*last_attempt) < refresh_backoff {
                return;
            }
        }
        inflight.insert(market_id.to_string(), now);
    }

    let market = market_id.to_string();
    let order = order_id.to_string();
    let http = shared.http.clone();
    let clob_endpoint = shared.clob_endpoint.clone();
    let scoring_cache = shared.scoring_cache.clone();
    let inflight = shared.scoring_refresh_inflight.clone();
    let rebate_factor = shared.scoring_rebate_factor;
    spawn_detached("scoring_refresh", false, async move {
        if let Some((scoring_ok, raw)) = fetch_order_scoring(&http, &clob_endpoint, &order).await {
            let mut cache = scoring_cache.write().await;
            let mut entry = cache.get(&market).cloned().unwrap_or(ScoringState {
                scoring_true: 0,
                scoring_total: 0,
                rebate_bps_est: 0.0,
                fetched_at: Instant::now(),
            });
            entry.scoring_total = entry.scoring_total.saturating_add(1);
            if scoring_ok {
                entry.scoring_true = entry.scoring_true.saturating_add(1);
            }
            let hit_ratio = if entry.scoring_total == 0 {
                0.0
            } else {
                (entry.scoring_true as f64 / entry.scoring_total as f64).clamp(0.0, 1.0)
            };
            // Conservative estimate: maker rebates are a fraction of taker fees and depend on
            // scoring + market share. Default rebate_factor is 0.0 unless explicitly configured.
            // Cap the pool fraction at 20% of fee_bps as a hard upper bound.
            entry.rebate_bps_est =
                (fee_bps.max(0.0) * 0.20 * hit_ratio * rebate_factor).clamp(0.0, fee_bps.max(0.0));
            entry.fetched_at = Instant::now();
            let log_row = serde_json::json!({
                "ts_ms": Utc::now().timestamp_millis(),
                "market_id": market,
                "order_id": order,
                "scoring_ok": scoring_ok,
                "scoring_true": entry.scoring_true,
                "scoring_total": entry.scoring_total,
                "hit_ratio": hit_ratio,
                "rebate_bps_est": entry.rebate_bps_est,
                "raw": raw
            });
            cache.insert(market.clone(), entry);
            append_jsonl(
                &dataset_path("normalized", "scoring_feedback.jsonl"),
                &log_row,
            );
        }
        inflight.write().await.remove(&market);
    });
}

pub(super) async fn fetch_fee_rate_bps(
    http: &Client,
    clob_endpoint: &str,
    market_id: &str,
) -> Option<f64> {
    let base = clob_endpoint.trim_end_matches('/');
    let endpoints = [
        format!("{base}/fee-rate?market_id={market_id}"),
        format!("{base}/fee-rate?market={market_id}"),
        format!("{base}/fee-rate?token_id={market_id}"),
    ];

    for url in endpoints {
        let Ok(resp) = http.get(&url).send().await else {
            continue;
        };
        let Ok(resp) = resp.error_for_status() else {
            continue;
        };
        let Ok(v) = resp.json::<serde_json::Value>().await else {
            continue;
        };
        let candidate = v
            .get("fee_rate_bps")
            .and_then(value_to_f64)
            .or_else(|| v.get("feeRateBps").and_then(value_to_f64))
            .or_else(|| v.get("makerFeeRateBps").and_then(value_to_f64))
            .or_else(|| v.get("maker_fee_rate_bps").and_then(value_to_f64));
        if candidate.is_some() {
            return candidate;
        }
    }
    None
}

pub(super) async fn fetch_order_scoring(
    http: &Client,
    clob_endpoint: &str,
    order_id: &str,
) -> Option<(bool, serde_json::Value)> {
    let base = clob_endpoint.trim_end_matches('/');
    let endpoints = [
        format!("{base}/order-scoring?order_id={order_id}"),
        format!("{base}/order-scoring?orderId={order_id}"),
        format!("{base}/orders-scoring?order_id={order_id}"),
    ];
    for url in endpoints {
        let Ok(resp) = http.get(&url).send().await else {
            continue;
        };
        let Ok(resp) = resp.error_for_status() else {
            continue;
        };
        let Ok(value) = resp.json::<serde_json::Value>().await else {
            continue;
        };
        let scoring = value
            .get("scoring")
            .and_then(|v| v.as_bool())
            .or_else(|| value.get("is_scoring").and_then(|v| v.as_bool()))
            .or_else(|| value.get("isScoring").and_then(|v| v.as_bool()))
            .or_else(|| value.get("eligible").and_then(|v| v.as_bool()))
            .or_else(|| value.as_bool())
            .unwrap_or(false);
        return Some((scoring, value));
    }
    None
}

pub(super) async fn pnl_after_horizon(
    shared: &EngineShared,
    shot: &ShadowShot,
    horizon: Duration,
) -> Option<f64> {
    tokio::time::sleep(horizon).await;
    let book = shared
        .latest_books
        .read()
        .await
        .get(&shot.market_id)
        .cloned()?;
    let mark = mid_for_side(&book, &shot.side);
    if shot.intended_price <= 0.0 {
        return None;
    }
    let pnl = match shot.side {
        OrderSide::BuyYes | OrderSide::BuyNo => {
            ((mark - shot.intended_price) / shot.intended_price) * 10_000.0
        }
        OrderSide::SellYes | OrderSide::SellNo => {
            ((shot.intended_price - mark) / shot.intended_price) * 10_000.0
        }
    };
    Some(pnl)
}

pub(super) fn evaluate_survival(shot: &ShadowShot, book: &BookTop) -> bool {
    let probe_px = if shot.survival_probe_price > 0.0 {
        shot.survival_probe_price
    } else {
        shot.intended_price
    };
    is_crossable(&shot.side, probe_px, book)
}

pub(super) fn is_crossable(side: &OrderSide, probe_px: f64, book: &BookTop) -> bool {
    if probe_px <= 0.0 {
        return false;
    }
    match side {
        OrderSide::BuyYes => probe_px >= book.ask_yes,
        OrderSide::SellYes => probe_px <= book.bid_yes,
        OrderSide::BuyNo => probe_px >= book.ask_no,
        OrderSide::SellNo => probe_px <= book.bid_no,
    }
}

pub(super) fn evaluate_fillable(
    shot: &ShadowShot,
    book: &BookTop,
    latency_ms: f64,
) -> (bool, Option<f64>, f64) {
    let probe_px = if shot.survival_probe_price > 0.0 {
        shot.survival_probe_price
    } else {
        shot.intended_price
    };
    let (crossable, fill_px) = match shot.side {
        OrderSide::BuyYes => (probe_px >= book.ask_yes, book.ask_yes),
        OrderSide::SellYes => (probe_px <= book.bid_yes, book.bid_yes),
        OrderSide::BuyNo => (probe_px >= book.ask_no, book.ask_no),
        OrderSide::SellNo => (probe_px <= book.bid_no, book.bid_no),
    };
    if !crossable || probe_px <= 0.0 {
        return (false, None, 0.0);
    }
    let queue_fill_prob = estimate_queue_fill_prob(shot, book, latency_ms);
    // P3: 降低 fillability 门槛从 0.55 到 0.45，允许更多有效机会通过
    if queue_fill_prob < 0.45 {
        return (false, None, queue_fill_prob);
    }
    let mut slippage = match shot.side {
        OrderSide::BuyYes | OrderSide::BuyNo => ((fill_px - probe_px) / probe_px) * 10_000.0,
        OrderSide::SellYes | OrderSide::SellNo => ((probe_px - fill_px) / probe_px) * 10_000.0,
    };
    slippage += (1.0 - queue_fill_prob) * 8.0;
    (true, Some(slippage), queue_fill_prob)
}

pub(super) fn classify_unfilled_outcome(
    book: &BookTop,
    latency_ms: f64,
    delay_ms: u64,
    survived: bool,
    queue_fill_prob: f64,
) -> EdgeAttribution {
    let spread = (book.ask_yes - book.bid_yes).max(0.0);
    if delay_ms >= 400 {
        return EdgeAttribution::StaleQuote;
    }
    if !survived {
        return EdgeAttribution::BookMoved;
    }
    if book.ask_yes <= 0.0 || book.bid_yes <= 0.0 {
        return EdgeAttribution::LiquidityThin;
    }
    if spread > 0.05 {
        return EdgeAttribution::SpreadTooWide;
    }
    // P3: 降低门槛与上面保持一致
    if queue_fill_prob < 0.45 {
        return EdgeAttribution::LatencyTail;
    }
    if latency_ms > 100.0 {
        return EdgeAttribution::LatencyTail;
    }
    EdgeAttribution::BookMoved
}

pub(super) fn classify_filled_outcome(
    edge_net_bps: f64,
    pnl_10s_bps: Option<f64>,
    slippage_bps: Option<f64>,
) -> EdgeAttribution {
    if edge_net_bps < 0.0 {
        return EdgeAttribution::FeeOverrun;
    }
    if slippage_bps.unwrap_or(0.0) > edge_net_bps.abs() {
        return EdgeAttribution::SignalLag;
    }
    if pnl_10s_bps.unwrap_or(0.0) < 0.0 {
        return EdgeAttribution::AdverseSelection;
    }
    EdgeAttribution::Unknown
}

pub(super) fn mid_for_side(book: &BookTop, side: &OrderSide) -> f64 {
    match side {
        OrderSide::BuyYes | OrderSide::SellYes => (book.bid_yes + book.ask_yes) * 0.5,
        OrderSide::BuyNo | OrderSide::SellNo => (book.bid_no + book.ask_no) * 0.5,
    }
}

pub(super) fn aggressive_price_for_side(book: &BookTop, side: &OrderSide) -> f64 {
    match side {
        OrderSide::BuyYes => book.ask_yes,
        OrderSide::SellYes => book.bid_yes,
        OrderSide::BuyNo => book.ask_no,
        OrderSide::SellNo => book.bid_no,
    }
}

#[derive(Serialize)]
pub(super) struct PrebuiltOrderPayload<'a> {
    market_id: &'a str,
    token_id: Option<&'a str>,
    side: &'a str,
    price: f64,
    size: f64,
    ttl_ms: u64,
    style: &'a str,
    tif: &'a str,
    max_slippage_bps: f64,
    fee_rate_bps: f64,
    expected_edge_net_bps: f64,
    hold_to_resolution: bool,
}

pub(super) fn prebuild_order_payload(intent: &OrderIntentV2) -> Option<Vec<u8>> {
    let side = intent.side.to_string();
    let style = intent.style.to_string();
    let tif = intent.tif.to_string();
    let payload = PrebuiltOrderPayload {
        market_id: intent.market_id.as_str(),
        token_id: intent.token_id.as_deref(),
        side: side.as_str(),
        price: intent.price,
        size: intent.size,
        ttl_ms: intent.ttl_ms,
        style: style.as_str(),
        tif: tif.as_str(),
        max_slippage_bps: intent.max_slippage_bps,
        fee_rate_bps: intent.fee_rate_bps,
        expected_edge_net_bps: intent.expected_edge_net_bps,
        hold_to_resolution: intent.hold_to_resolution,
    };
    serde_json::to_vec(&payload).ok()
}

pub(super) fn spread_for_side(book: &BookTop, side: &OrderSide) -> f64 {
    match side {
        OrderSide::BuyYes | OrderSide::SellYes => (book.ask_yes - book.bid_yes).max(0.0),
        OrderSide::BuyNo | OrderSide::SellNo => (book.ask_no - book.bid_no).max(0.0),
    }
}

pub(super) fn fair_for_side(fair_yes: f64, side: &OrderSide) -> f64 {
    match side {
        OrderSide::BuyYes | OrderSide::SellYes => fair_yes,
        OrderSide::BuyNo | OrderSide::SellNo => (1.0 - fair_yes).clamp(0.001, 0.999),
    }
}

pub(super) fn edge_gross_bps_for_side(fair_yes: f64, side: &OrderSide, entry_price: f64) -> f64 {
    let fair = fair_for_side(fair_yes, side);
    let px = entry_price.max(1e-6);
    match side {
        OrderSide::BuyYes | OrderSide::BuyNo => ((fair - px) / px) * 10_000.0,
        OrderSide::SellYes | OrderSide::SellNo => ((px - fair) / px) * 10_000.0,
    }
}

pub(super) fn edge_for_intent(fair_yes: f64, intent: &QuoteIntent) -> f64 {
    let px = intent.price.max(1e-6);
    let fair = match intent.side {
        OrderSide::BuyYes | OrderSide::SellYes => fair_yes,
        OrderSide::BuyNo | OrderSide::SellNo => (1.0 - fair_yes).clamp(0.001, 0.999),
    };
    match intent.side {
        // Expected edge vs. intended entry price in bps of entry.
        OrderSide::BuyYes | OrderSide::BuyNo => ((fair - px) / px) * 10_000.0,
        OrderSide::SellYes | OrderSide::SellNo => ((px - fair) / px) * 10_000.0,
    }
}
