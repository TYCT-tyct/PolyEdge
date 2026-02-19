use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use core_types::{
    new_id, BookTop, Direction, DirectionSignal, EngineEvent, ExecutionStyle, ExecutionVenue,
    OrderAck, OrderIntentV2, OrderSide, OrderTimeInForce, PaperAction, QuoteIntent, Regime,
    RiskContext, RiskManager, ShadowShot, SourceHealth, Stage, TimeframeClass, TimeframeOpp,
    ToxicRegime,
};
use execution_clob::ClobExecution;
use exit_manager::PositionLifecycle;
use infra_bus::RingBus;
use paper_executor::ShadowExecutor;
use portfolio::PortfolioBook;
use risk_engine::RiskLimits;
use strategy_maker::MakerConfig;
use taker_sniper::{FirePlan, TakerAction};

use crate::engine_core::{classify_execution_error_reason, normalize_reject_code};
use crate::paper_runtime::{global_paper_runtime, PaperIntentCtx};
use crate::execution_eval::{
    aggressive_price_for_side, edge_gross_bps_for_side, get_fee_rate_bps_cached,
    get_rebate_bps_cached, mid_for_side, prebuild_order_payload, spread_for_side,
};
use crate::feed_runtime::settlement_prob_yes_for_symbol;
use crate::fusion_engine::TokenBucket;
use crate::state::{
    EngineShared, PredatorCConfig, PredatorCPriority, PredatorRegimeConfig, SourceHealthConfig,
    V52DualArbConfig, V52ExecutionConfig, V52TimePhaseConfig,
};
use crate::stats_utils::{freshness_ms, now_ns};
use crate::strategy_policy::{
    adaptive_size_scale, edge_gate_bps, inventory_for_market, timeframe_weight,
};
use crate::{
    publish_if_telemetry_subscribers, spawn_predator_exit_lifecycle, spawn_detached,
    spawn_shadow_outcome_task, PredatorExecResult,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimePhase {
    Early,
    Maturity,
    Late,
}

pub(crate) fn timeframe_total_ms(timeframe: TimeframeClass) -> Option<i64> {
    match timeframe {
        TimeframeClass::Tf5m => Some(300_000),
        TimeframeClass::Tf15m => Some(900_000),
        _ => None,
    }
}

fn timeframe_tag(timeframe: TimeframeClass) -> &'static str {
    match timeframe {
        TimeframeClass::Tf5m => "5m",
        TimeframeClass::Tf15m => "15m",
        TimeframeClass::Tf1h => "1h",
        TimeframeClass::Tf1d => "1d",
    }
}

pub(crate) fn classify_time_phase(remaining_ratio: f64, cfg: &V52TimePhaseConfig) -> TimePhase {
    if remaining_ratio <= cfg.late_max_ratio {
        TimePhase::Late
    } else if remaining_ratio <= cfg.early_min_ratio {
        TimePhase::Maturity
    } else {
        TimePhase::Early
    }
}

pub(crate) fn time_phase_size_scale(phase: TimePhase, cfg: &V52TimePhaseConfig) -> f64 {
    let scale = match phase {
        TimePhase::Early => cfg.early_size_scale,
        TimePhase::Maturity => cfg.maturity_size_scale,
        TimePhase::Late => cfg.late_size_scale,
    };
    scale.clamp(0.10, 5.0)
}

pub(crate) fn stage_for_phase(phase: TimePhase, momentum: bool) -> Stage {
    match phase {
        TimePhase::Early => Stage::Early,
        TimePhase::Late => Stage::Late,
        TimePhase::Maturity => {
            if momentum {
                Stage::Momentum
            } else {
                Stage::Maturity
            }
        }
    }
}

pub(crate) fn allow_dual_side_arb(
    yes_price: f64,
    no_price: f64,
    fee_rate_bps: f64,
    cfg: &V52DualArbConfig,
) -> bool {
    if !cfg.enabled {
        return false;
    }
    let fee_buffer = (fee_rate_bps.max(0.0) / 10_000.0).max(0.0);
    let safety_margin = (cfg.safety_margin_bps.max(0.0) / 10_000.0).max(0.0);
    yes_price + no_price + fee_buffer + safety_margin < cfg.threshold
}

fn side_to_direction(side: &OrderSide) -> Direction {
    match side {
        OrderSide::BuyYes | OrderSide::SellNo => Direction::Up,
        OrderSide::BuyNo | OrderSide::SellYes => Direction::Down,
    }
}

fn direction_signal_for_side(base: &DirectionSignal, side: &OrderSide) -> DirectionSignal {
    let mut out = base.clone();
    out.direction = side_to_direction(side);
    out
}

pub(crate) fn should_force_taker_fallback(
    phase: TimePhase,
    time_to_expiry_ms: i64,
    cfg: &V52ExecutionConfig,
) -> bool {
    let force_in_phase = match phase {
        TimePhase::Early => false,
        TimePhase::Maturity => cfg.apply_force_taker_in_maturity,
        TimePhase::Late => cfg.apply_force_taker_in_late,
    };
    force_in_phase && time_to_expiry_ms <= cfg.late_force_taker_remaining_ms as i64
}

fn spawn_force_taker_fallback_for_maker(
    shared: Arc<EngineShared>,
    execution: Arc<ClobExecution>,
    shadow: Arc<ShadowExecutor>,
    market_id: String,
    token_id: String,
    side: OrderSide,
    size: f64,
    ttl_ms: u64,
    maker_order_id: String,
    fee_rate_bps: f64,
    expected_edge_net_bps: f64,
    timeframe: TimeframeClass,
    force_remaining_ms: u64,
    maker_wait_ms: u64,
    taker_max_slippage_bps: f64,
) {
    spawn_detached("v52_force_taker_fallback", false, async move {
        if maker_wait_ms > 0 {
            tokio::time::sleep(Duration::from_millis(maker_wait_ms)).await;
        }
        let Some(frame_total_ms) = timeframe_total_ms(timeframe) else {
            return;
        };
        let threshold = force_remaining_ms as i64;
        loop {
            if !execution.has_open_order(&maker_order_id) {
                return;
            }
            let now_ms = chrono::Utc::now().timestamp_millis();
            let remain_ms = (frame_total_ms - now_ms.rem_euclid(frame_total_ms)).max(0);
            if remain_ms <= threshold {
                break;
            }
            let sleep_ms = (remain_ms - threshold).clamp(25, 250) as u64;
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        }
        if !execution.has_open_order(&maker_order_id) {
            return;
        }
        if execution.cancel_order(&maker_order_id, &market_id).await.is_err() {
            return;
        }
        shadow.cancel(&maker_order_id);
        let book = shared.latest_books.read().await.get(&market_id).cloned();
        let Some(book) = book else {
            return;
        };
        let taker_price = aggressive_price_for_side(&book, &side);
        if !taker_price.is_finite() || taker_price <= 0.0 {
            return;
        }
        let order = OrderIntentV2 {
            market_id: market_id.clone(),
            token_id: Some(token_id),
            side: side.clone(),
            price: taker_price,
            size: size.max(0.01),
            ttl_ms: ttl_ms.min(150),
            style: ExecutionStyle::Taker,
            tif: OrderTimeInForce::Fak,
            max_slippage_bps: taker_max_slippage_bps.max(0.0),
            fee_rate_bps,
            expected_edge_net_bps,
            client_order_id: Some(new_id()),
            hold_to_resolution: false,
            prebuilt_payload: None,
        };
        match execution.place_order_v2(order).await {
            Ok(ack) if ack.accepted => {
                shared.shadow_stats.mark_predator_last_30s_taker_fallback();
                shared.shadow_stats.mark_executed();
                let ack_event = OrderAck {
                    order_id: ack.order_id,
                    market_id: ack.market_id,
                    accepted: true,
                    ts_ms: ack.ts_ms,
                };
                shadow.register_order(
                    &ack_event,
                    QuoteIntent {
                        market_id,
                        side,
                        price: taker_price,
                        size: size.max(0.01),
                        ttl_ms: ttl_ms.min(150),
                    },
                    ExecutionStyle::Taker,
                    ((book.bid_yes + book.ask_yes) * 0.5).max(0.0),
                    fee_rate_bps.max(0.0),
                );
            }
            _ => {}
        }
    });
}

fn spawn_alpha_window_probe(
    shared: Arc<EngineShared>,
    market_id: String,
    side: OrderSide,
    anchor_price: f64,
    move_bps: f64,
    poll_ms: u64,
    max_wait_ms: u64,
) {
    if !anchor_price.is_finite() || anchor_price <= 0.0 {
        return;
    }
    let move_abs = (anchor_price * (move_bps.max(0.0) / 10_000.0)).max(1e-6);
    let poll_ms = poll_ms.clamp(1, 200);
    let max_wait_ms = max_wait_ms.clamp(50, 5_000);

    spawn_detached("v52_alpha_window_probe", false, async move {
        let started = Instant::now();
        loop {
            let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;
            let current_price = shared
                .latest_books
                .read()
                .await
                .get(&market_id)
                .map(|book| aggressive_price_for_side(book, &side))
                .unwrap_or(0.0);
            if current_price.is_finite() && current_price > 0.0 {
                let delta = (current_price - anchor_price).abs();
                if delta >= move_abs {
                    shared
                        .shadow_stats
                        .push_alpha_window_sample(elapsed_ms, true)
                        .await;
                    metrics::histogram!("latency.alpha_window_ms").record(elapsed_ms);
                    return;
                }
            }
            if elapsed_ms >= max_wait_ms as f64 {
                let capped = max_wait_ms as f64;
                shared
                    .shadow_stats
                    .push_alpha_window_sample(capped, false)
                    .await;
                metrics::histogram!("latency.alpha_window_ms").record(capped);
                return;
            }
            tokio::time::sleep(Duration::from_millis(poll_ms)).await;
        }
    });
}

pub(super) async fn evaluate_and_route_v52(
    shared: &Arc<EngineShared>,
    bus: &RingBus<EngineEvent>,
    portfolio: &Arc<PortfolioBook>,
    execution: &Arc<ClobExecution>,
    shadow: &Arc<ShadowExecutor>,
    market_rate_budget: &mut HashMap<String, TokenBucket>,
    global_rate_budget: &mut TokenBucket,
    predator_cfg: &PredatorCConfig,
    symbol: &str,
    tick_fast_recv_ts_ms: i64,
    tick_fast_recv_ts_local_ns: i64,
    now_ms: i64,
    direction_override: Option<DirectionSignal>,
) -> PredatorExecResult {
    let mut total = run_predator_c_for_symbol(
        shared,
        bus,
        portfolio,
        execution,
        shadow,
        market_rate_budget,
        global_rate_budget,
        predator_cfg,
        symbol,
        tick_fast_recv_ts_ms,
        tick_fast_recv_ts_local_ns,
        now_ms,
        direction_override.clone(),
    )
    .await;

    let leader_direction = if let Some(sig) = direction_override {
        Some(sig)
    } else {
        shared
            .predator_latest_direction
            .read()
            .await
            .get(symbol)
            .cloned()
    };
    if let Some(leader_direction) = leader_direction {
        let cross_res = run_predator_c_cross_symbol(
            shared,
            bus,
            portfolio,
            execution,
            shadow,
            market_rate_budget,
            global_rate_budget,
            predator_cfg,
            &leader_direction,
            tick_fast_recv_ts_ms,
            tick_fast_recv_ts_local_ns,
            now_ms,
        )
        .await;
        total.attempted = total.attempted.saturating_add(cross_res.attempted);
        total.executed = total.executed.saturating_add(cross_res.executed);
    }
    if predator_cfg.strategy_d.enabled {
        let d_res = run_predator_d_for_symbol(
            shared,
            bus,
            portfolio,
            execution,
            shadow,
            market_rate_budget,
            global_rate_budget,
            predator_cfg,
            symbol,
            tick_fast_recv_ts_ms,
            tick_fast_recv_ts_local_ns,
            now_ms,
        )
        .await;
        total.attempted = total.attempted.saturating_add(d_res.attempted);
        total.executed = total.executed.saturating_add(d_res.executed);
    }
    total
}

pub(super) async fn run_predator_c_for_symbol(
    shared: &Arc<EngineShared>,
    bus: &RingBus<EngineEvent>,
    portfolio: &Arc<PortfolioBook>,
    execution: &Arc<ClobExecution>,
    shadow: &Arc<ShadowExecutor>,
    market_rate_budget: &mut HashMap<String, TokenBucket>,
    global_rate_budget: &mut TokenBucket,
    predator_cfg: &PredatorCConfig,
    symbol: &str,
    tick_fast_recv_ts_ms: i64,
    tick_fast_recv_ts_local_ns: i64,
    now_ms: i64,
    direction_override: Option<DirectionSignal>,
) -> PredatorExecResult {
    shared
        .shadow_stats
        .set_predator_enabled(predator_cfg.enabled);
    if !predator_cfg.enabled {
        return PredatorExecResult::default();
    }
    let live_armed = std::env::var("POLYEDGE_LIVE_ARMED")
        .ok()
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    if live_armed
        && predator_cfg.v52.execution.require_compounder_when_live
        && !predator_cfg.compounder.enabled
    {
        shared
            .shadow_stats
            .mark_blocked_with_reason("compounder_required_when_live")
            .await;
        return PredatorExecResult::default();
    }
    let source_health_cfg = shared.source_health_cfg.read().await.clone();
    let primary_fast_source = shared
        .latest_fast_ticks
        .get(symbol)
        .map(|tick| tick.source.clone());
    let primary_source_health = {
        let map = shared.source_health_latest.read().await;
        let from_fast = primary_fast_source
            .as_ref()
            .and_then(|source| map.get(source.as_str()).cloned());
        let best_binance = map
            .values()
            .filter(|h| h.source.to_ascii_lowercase().contains("binance"))
            .max_by(|a, b| {
                a.score
                    .partial_cmp(&b.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .cloned();
        match (from_fast, best_binance) {
            (Some(a), Some(b)) => Some(if b.score > a.score { b } else { a }),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    };

    let direction_signal = if let Some(sig) = direction_override {
        sig
    } else {
        let mut direction_signal = {
            let det = shared.predator_direction_detector.read().await;
            det.evaluate(symbol, now_ms)
        };
        if direction_signal.is_none() {
            let fallback_signal = shared
                .predator_latest_direction
                .read()
                .await
                .get(symbol)
                .cloned();
            if let Some(cached) = fallback_signal {
                let cached_ms = cached.ts_ns.div_euclid(1_000_000);
                let age_ms = now_ms.saturating_sub(cached_ms);
                if age_ms <= 2_500 {
                    direction_signal = Some(cached);
                }
            }
        }
        let Some(direction_signal) = direction_signal else {
            shared
                .shadow_stats
                .mark_predator_taker_skipped("no_direction_signal")
                .await;
            return PredatorExecResult::default();
        };
        direction_signal
    };

    shared
        .shadow_stats
        .mark_predator_direction(&direction_signal.direction);
    {
        let mut map = shared.predator_latest_direction.write().await;
        map.insert(symbol.to_string(), direction_signal.clone());
    }
    publish_if_telemetry_subscribers(bus, EngineEvent::DirectionSignal(direction_signal.clone()));

    if matches!(direction_signal.direction, Direction::Neutral) {
        shared
            .shadow_stats
            .mark_predator_taker_skipped("neutral_direction")
            .await;
        return PredatorExecResult::default();
    }

    let market_ids = shared
        .symbol_to_markets
        .read()
        .await
        .get(symbol)
        .cloned()
        .unwrap_or_default();
    if market_ids.is_empty() {
        shared
            .shadow_stats
            .mark_predator_taker_skipped("no_symbol_markets")
            .await;
        return PredatorExecResult::default();
    }

    let (symbol_tox_score, symbol_tox_regime) = symbol_toxic_snapshot(shared, &market_ids).await;
    let regime = classify_predator_regime(
        &predator_cfg.regime,
        &direction_signal,
        symbol_tox_score,
        &symbol_tox_regime,
        primary_source_health.as_ref(),
        &source_health_cfg,
    );
    shared.shadow_stats.mark_predator_regime(&regime);
    if matches!(regime, Regime::Defend) {
        shared
            .shadow_stats
            .mark_predator_taker_skipped("predator_regime_defend")
            .await;
        return PredatorExecResult::default();
    }
    if matches!(regime, Regime::Quiet)
        && matches!(predator_cfg.priority, PredatorCPriority::MakerFirst)
    {
        shared
            .shadow_stats
            .mark_predator_taker_skipped("predator_regime_quiet")
            .await;
        return PredatorExecResult::default();
    }

    let maker_cfg = shared.strategy_cfg.read().await.clone();
    let edge_model_cfg = shared.edge_model_cfg.read().await.clone();
    let tf_weights = HashMap::from([
        (
            TimeframeClass::Tf5m,
            timeframe_weight(shared, &TimeframeClass::Tf5m).await,
        ),
        (
            TimeframeClass::Tf15m,
            timeframe_weight(shared, &TimeframeClass::Tf15m).await,
        ),
        (
            TimeframeClass::Tf1h,
            timeframe_weight(shared, &TimeframeClass::Tf1h).await,
        ),
        (
            TimeframeClass::Tf1d,
            timeframe_weight(shared, &TimeframeClass::Tf1d).await,
        ),
    ]);

    let static_quote_notional_usdc = (predator_cfg.compounder.initial_capital_usdc.max(0.0)
        * predator_cfg.compounder.position_fraction.clamp(0.0, 1.0))
    .max(predator_cfg.compounder.min_quote_size.max(0.01));
    let (quote_notional_usdc, total_capital_usdc) = if predator_cfg.compounder.enabled {
        let c = shared.predator_compounder.read().await;
        (c.recommended_quote_notional_usdc(), c.available())
    } else {
        (
            static_quote_notional_usdc,
            predator_cfg.compounder.initial_capital_usdc.max(0.0),
        )
    };

    #[derive(Debug, Clone)]
    struct PredatorCandIn {
        market_id: String,
        timeframe: TimeframeClass,
        side: OrderSide,
        entry_price: f64,
        spread: f64,
        fee_bps: f64,
        edge_gross_bps: f64,
        edge_net_bps: f64,
        size: f64,
        dual_pair: bool,
    }

    let market_ids = market_ids.into_iter().take(32).collect::<Vec<_>>();
    let tf_by_market = {
        let map = shared.market_to_timeframe.read().await;
        market_ids
            .iter()
            .filter_map(|id| map.get(id).cloned().map(|tf| (id.clone(), tf)))
            .collect::<HashMap<String, TimeframeClass>>()
    };
    let book_by_market = {
        let map = shared.latest_books.read().await;
        market_ids
            .iter()
            .filter_map(|id| map.get(id).cloned().map(|b| (id.clone(), b)))
            .collect::<HashMap<String, BookTop>>()
    };

    let mut cand_inputs_by_market: HashMap<String, Vec<PredatorCandIn>> = HashMap::new();
    for market_id in market_ids {
        let Some(timeframe) = tf_by_market.get(&market_id).cloned() else {
            continue;
        };
        let Some(frame_total_ms) = timeframe_total_ms(timeframe.clone()) else {
            shared
                .shadow_stats
                .mark_blocked_with_reason("v52_blocked_timeframe")
                .await;
            continue;
        };
        if !predator_cfg
            .v52
            .time_phase
            .allow_timeframes
            .iter()
            .any(|v| v == timeframe_tag(timeframe.clone()))
        {
            shared
                .shadow_stats
                .mark_blocked_with_reason("v52_timeframe_not_allowed")
                .await;
            continue;
        }
        let Some(book) = book_by_market.get(&market_id).cloned() else {
            continue;
        };
        let sig_entry = shared
            .latest_signals
            .get(&market_id)
            .map(|v| v.value().clone());
        let Some(sig_entry) = sig_entry else {
            continue;
        };
        if now_ms.saturating_sub(sig_entry.ts_ms) > 5_000 {
            continue;
        }

        let time_to_expiry_ms = (frame_total_ms - now_ms.rem_euclid(frame_total_ms)).max(0);
        let remaining_ratio = (time_to_expiry_ms as f64 / frame_total_ms as f64).clamp(0.0, 1.0);
        let time_phase = classify_time_phase(remaining_ratio, &predator_cfg.v52.time_phase);

        let fee_bps = get_fee_rate_bps_cached(shared, &market_id).await;
        let rebate_est_bps = get_rebate_bps_cached(shared, &market_id, fee_bps).await;
        let yes_price = aggressive_price_for_side(&book, &OrderSide::BuyYes);
        let no_price = aggressive_price_for_side(&book, &OrderSide::BuyNo);
        let dual_arb_ok = allow_dual_side_arb(yes_price, no_price, fee_bps, &predator_cfg.v52.dual_arb);
        let book_top_lag_ms = if tick_fast_recv_ts_local_ns > 0 && book.recv_ts_local_ns > 0 {
            ((book.recv_ts_local_ns - tick_fast_recv_ts_local_ns).max(0) as f64) / 1_000_000.0
        } else {
            0.0
        };
        let settlement_prob_yes =
            settlement_prob_yes_for_symbol(shared, symbol, sig_entry.signal.fair_yes, now_ms).await;
        let probability = {
            let engine = shared.predator_probability_engine.read().await;
            engine.estimate(
                &sig_entry.signal,
                &direction_signal,
                settlement_prob_yes,
                book_top_lag_ms,
                now_ms,
            )
        };
        {
            let mut map = shared.predator_latest_probability.write().await;
            map.insert(market_id.clone(), probability.clone());
        }
        shared
            .shadow_stats
            .push_probability_sample(&probability)
            .await;
        let yes_edge_gross = edge_gross_bps_for_side(probability.p_settle, &OrderSide::BuyYes, yes_price);
        let no_edge_gross = edge_gross_bps_for_side(probability.p_settle, &OrderSide::BuyNo, no_price);
        let yes_edge_net = yes_edge_gross - fee_bps + rebate_est_bps - edge_model_cfg.fail_cost_bps;
        let no_edge_net = no_edge_gross - fee_bps + rebate_est_bps - edge_model_cfg.fail_cost_bps;
        if yes_edge_net > 0.0 && no_edge_net > 0.0 && !dual_arb_ok {
            shared
                .shadow_stats
                .mark_blocked_with_reason("dual_arb_gate_blocked")
                .await;
        }
        let tf_weight = tf_weights
            .get(&timeframe)
            .copied()
            .unwrap_or(1.0)
            .clamp(0.0, 1.0);
        if tf_weight <= 0.0 {
            continue;
        }
        let phase_size_scale = time_phase_size_scale(time_phase, &predator_cfg.v52.time_phase);
        let side_size_scale = (tf_weight * phase_size_scale).max(0.0);
        let mk_size = |entry_price: f64| {
            let base_size = if quote_notional_usdc > 0.0 {
                (quote_notional_usdc / entry_price.max(1e-6)).max(0.01)
            } else {
                maker_cfg.base_quote_size.max(0.01)
            };
            (base_size * side_size_scale).max(0.01)
        };
        let push_candidate = |bucket: &mut Vec<PredatorCandIn>,
                              side: OrderSide,
                              entry_price: f64,
                              spread: f64,
                              edge_gross_bps: f64,
                              edge_net_bps: f64,
                              dual_pair: bool| {
            bucket.push(PredatorCandIn {
                market_id: market_id.clone(),
                timeframe: timeframe.clone(),
                side,
                entry_price,
                spread,
                fee_bps,
                edge_gross_bps,
                edge_net_bps,
                size: mk_size(entry_price),
                dual_pair,
            });
        };
        let bucket = cand_inputs_by_market.entry(market_id.clone()).or_default();
        if dual_arb_ok && yes_edge_net > 0.0 && no_edge_net > 0.0 {
            push_candidate(
                bucket,
                OrderSide::BuyYes,
                yes_price,
                spread_for_side(&book, &OrderSide::BuyYes),
                yes_edge_gross,
                yes_edge_net,
                true,
            );
            push_candidate(
                bucket,
                OrderSide::BuyNo,
                no_price,
                spread_for_side(&book, &OrderSide::BuyNo),
                no_edge_gross,
                no_edge_net,
                true,
            );
        } else {
            let (side, entry_price, edge_gross_bps, edge_net_bps) = if no_edge_net > yes_edge_net {
                (OrderSide::BuyNo, no_price, no_edge_gross, no_edge_net)
            } else {
                (OrderSide::BuyYes, yes_price, yes_edge_gross, yes_edge_net)
            };
            push_candidate(
                bucket,
                side.clone(),
                entry_price,
                spread_for_side(&book, &side),
                edge_gross_bps,
                edge_net_bps,
                false,
            );
        }
    }

    let mut fire_plans_by_market: HashMap<String, FirePlan> = HashMap::new();
    let mut dual_fire_pairs: Vec<(FirePlan, FirePlan)> = Vec::new();
    let mut skip_reasons: Vec<String> = Vec::new();
    {
        let mut sniper = shared.predator_taker_sniper.write().await;
        let normalize_plan = |mut plan: FirePlan, cin: &PredatorCandIn| {
            plan.opportunity.side = cin.side.clone();
            plan.opportunity.direction = side_to_direction(&cin.side);
            plan.opportunity.entry_price = cin.entry_price;
            plan.opportunity.edge_gross_bps = cin.edge_gross_bps;
            plan.opportunity.edge_net_bps = cin.edge_net_bps;
            plan.opportunity.ts_ms = now_ms;
            plan
        };
        let evaluate = |sn: &mut taker_sniper::TakerSniper,
                        cin: &PredatorCandIn,
                        sig: &DirectionSignal| {
            sn.evaluate(&taker_sniper::EvaluateCtx {
                market_id: &cin.market_id,
                symbol,
                timeframe: cin.timeframe.clone(),
                direction_signal: sig,
                entry_price: cin.entry_price,
                spread: cin.spread,
                fee_bps: cin.fee_bps,
                edge_gross_bps: cin.edge_gross_bps,
                edge_net_bps: cin.edge_net_bps,
                size: cin.size,
                now_ms,
            })
        };
        for (_, mut cands) in cand_inputs_by_market {
            if cands.is_empty() {
                continue;
            }
            cands.sort_by(|a, b| {
                b.edge_net_bps
                    .partial_cmp(&a.edge_net_bps)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            let dual_eligible = cands.len() == 2 && cands.iter().all(|c| c.dual_pair);
            if dual_eligible {
                let mut dry_cfg = sniper.cfg().clone();
                dry_cfg.cooldown_ms_per_market = 0;
                let mut dry_sniper = taker_sniper::TakerSniper::new(dry_cfg);

                let primary = &cands[0];
                let hedge = &cands[1];
                let primary_sig = direction_signal_for_side(&direction_signal, &primary.side);
                let hedge_sig = direction_signal_for_side(&direction_signal, &hedge.side);

                let primary_probe = evaluate(&mut dry_sniper, primary, &primary_sig);
                let hedge_probe = evaluate(&mut dry_sniper, hedge, &hedge_sig);
                if matches!(primary_probe.action, TakerAction::Fire)
                    && matches!(hedge_probe.action, TakerAction::Fire)
                {
                    let primary_live = evaluate(&mut sniper, primary, &primary_sig);
                    match primary_live.action {
                        TakerAction::Fire => {
                            if let (Some(primary_plan), Some(hedge_plan)) =
                                (primary_live.fire_plan, hedge_probe.fire_plan)
                            {
                                dual_fire_pairs.push((
                                    normalize_plan(primary_plan, primary),
                                    normalize_plan(hedge_plan, hedge),
                                ));
                                continue;
                            }
                            skip_reasons.push("dual_arb_plan_missing".to_string());
                        }
                        TakerAction::Skip => {
                            skip_reasons.push(primary_live.reason);
                        }
                    }
                } else {
                    if matches!(primary_probe.action, TakerAction::Skip) {
                        skip_reasons.push(primary_probe.reason);
                    }
                    if matches!(hedge_probe.action, TakerAction::Skip) {
                        skip_reasons.push(hedge_probe.reason);
                    }
                }
            }

            let best = &cands[0];
            let best_sig = direction_signal_for_side(&direction_signal, &best.side);
            let decision = evaluate(&mut sniper, best, &best_sig);
            match decision.action {
                TakerAction::Fire => {
                    if let Some(plan) = decision.fire_plan {
                        let plan = normalize_plan(plan, best);
                        fire_plans_by_market.insert(plan.opportunity.market_id.clone(), plan);
                    }
                }
                TakerAction::Skip => skip_reasons.push(decision.reason),
            }
        }
    }
    for reason in skip_reasons {
        shared
            .shadow_stats
            .mark_predator_taker_skipped(reason.as_str())
            .await;
    }

    if matches!(regime, Regime::Quiet) {
        let quiet_min_edge = predator_cfg.taker_sniper.min_edge_net_bps.max(0.0)
            * predator_cfg.regime.quiet_min_edge_multiplier.max(1.0);
        fire_plans_by_market.retain(|_, plan| plan.opportunity.edge_net_bps >= quiet_min_edge);
        dual_fire_pairs.retain(|(a, b)| {
            a.opportunity.edge_net_bps >= quiet_min_edge && b.opportunity.edge_net_bps >= quiet_min_edge
        });
        let chunk_scale = predator_cfg.regime.quiet_chunk_scale.clamp(0.05, 1.0);
        for plan in fire_plans_by_market.values_mut() {
            let len = plan.chunks.len();
            if len <= 1 {
                continue;
            }
            let keep = ((len as f64) * chunk_scale).ceil() as usize;
            let keep = keep.clamp(1, len);
            plan.chunks.truncate(keep);
            if let Some(first) = plan.chunks.first_mut() {
                first.send_delay_ms = 0;
            }
        }
        for (plan_a, plan_b) in dual_fire_pairs.iter_mut() {
            for plan in [plan_a, plan_b] {
                let len = plan.chunks.len();
                if len <= 1 {
                    continue;
                }
                let keep = ((len as f64) * chunk_scale).ceil() as usize;
                let keep = keep.clamp(1, len);
                plan.chunks.truncate(keep);
                if let Some(first) = plan.chunks.first_mut() {
                    first.send_delay_ms = 0;
                }
            }
        }
    }

    if fire_plans_by_market.is_empty() && dual_fire_pairs.is_empty() {
        return PredatorExecResult::default();
    }

    let mut locked_plans: Vec<FirePlan> = Vec::new();
    let mut locked_dual_pairs: Vec<(FirePlan, FirePlan)> = Vec::new();
    let mut locked_by_tf_usdc: HashMap<String, f64> = HashMap::new();
    let mut router_skip_reasons: Vec<String> = Vec::new();
    {
        let mut router = shared.predator_router.write().await;
        let selected = router.route(
            fire_plans_by_market
                .values()
                .map(|plan| plan.opportunity.clone())
                .collect::<Vec<_>>(),
            total_capital_usdc.max(0.0),
            now_ms,
        );
        for opp in selected {
            if router.lock(&opp, now_ms) {
                if let Some(plan) = fire_plans_by_market.remove(&opp.market_id) {
                    locked_plans.push(plan);
                }
            }
        }
        for (plan_a, plan_b) in dual_fire_pairs {
            let notional = (plan_a.opportunity.entry_price.max(0.0) * plan_a.opportunity.size.max(0.0))
                + (plan_b.opportunity.entry_price.max(0.0) * plan_b.opportunity.size.max(0.0));
            if notional <= 0.0 {
                continue;
            }
            let mut lock_opp = if plan_a.opportunity.edge_net_bps >= plan_b.opportunity.edge_net_bps {
                plan_a.opportunity.clone()
            } else {
                plan_b.opportunity.clone()
            };
            lock_opp.entry_price = 1.0;
            lock_opp.size = notional.max(0.01);
            if router.lock(&lock_opp, now_ms) {
                locked_dual_pairs.push((plan_a, plan_b));
            } else {
                router_skip_reasons.push("dual_arb_router_lock_blocked".to_string());
            }
        }
        for (tf, v) in router.locked_by_tf_usdc(now_ms) {
            locked_by_tf_usdc.insert(tf.to_string(), v);
        }
    }
    for reason in router_skip_reasons {
        shared
            .shadow_stats
            .mark_predator_taker_skipped(reason.as_str())
            .await;
    }
    shared
        .shadow_stats
        .set_predator_router_locked_by_tf_usdc(locked_by_tf_usdc)
        .await;

    if locked_plans.is_empty() && locked_dual_pairs.is_empty() {
        return PredatorExecResult::default();
    }

    let single_futures: Vec<_> = locked_plans
        .into_iter()
        .map(|plan| {
            let shared = shared.clone();
            let bus = bus.clone();
            let portfolio = portfolio.clone();
            let execution = execution.clone();
            let shadow = shadow.clone();
            let maker_cfg = maker_cfg.clone();
            let direction_signal = direction_signal_for_side(&direction_signal, &plan.opportunity.side);
            let mut market_rate_budget_local = market_rate_budget.clone();
            let mut global_rate_budget_local = global_rate_budget.clone();
            async move {
                execute_fire_plan(
                    &shared,
                    &bus,
                    &portfolio,
                    &execution,
                    &shadow,
                    &mut market_rate_budget_local,
                    &mut global_rate_budget_local,
                    &maker_cfg,
                    predator_cfg,
                    &direction_signal,
                    &plan,
                    tick_fast_recv_ts_ms,
                    tick_fast_recv_ts_local_ns,
                    now_ms,
                )
                .await
            }
        })
        .collect();

    let mut out = PredatorExecResult::default();
    for r in futures::future::join_all(single_futures).await {
        out.attempted = out.attempted.saturating_add(r.attempted);
        out.executed = out.executed.saturating_add(r.executed);
    }

    let dual_futures: Vec<_> = locked_dual_pairs
        .into_iter()
        .map(|(plan_a, plan_b)| {
            let shared = shared.clone();
            let bus = bus.clone();
            let portfolio = portfolio.clone();
            let execution = execution.clone();
            let shadow = shadow.clone();
            let maker_cfg = maker_cfg.clone();
            let dir_a = direction_signal_for_side(&direction_signal, &plan_a.opportunity.side);
            let dir_b = direction_signal_for_side(&direction_signal, &plan_b.opportunity.side);
            let mut market_rate_budget_local_a = market_rate_budget.clone();
            let mut global_rate_budget_local_a = global_rate_budget.clone();
            let mut market_rate_budget_local_b = market_rate_budget.clone();
            let mut global_rate_budget_local_b = global_rate_budget.clone();
            async move {
                let leg_a = execute_fire_plan(
                    &shared,
                    &bus,
                    &portfolio,
                    &execution,
                    &shadow,
                    &mut market_rate_budget_local_a,
                    &mut global_rate_budget_local_a,
                    &maker_cfg,
                    predator_cfg,
                    &dir_a,
                    &plan_a,
                    tick_fast_recv_ts_ms,
                    tick_fast_recv_ts_local_ns,
                    now_ms,
                );
                let leg_b = execute_fire_plan(
                    &shared,
                    &bus,
                    &portfolio,
                    &execution,
                    &shadow,
                    &mut market_rate_budget_local_b,
                    &mut global_rate_budget_local_b,
                    &maker_cfg,
                    predator_cfg,
                    &dir_b,
                    &plan_b,
                    tick_fast_recv_ts_ms,
                    tick_fast_recv_ts_local_ns,
                    now_ms,
                );
                let (res_a, res_b) = tokio::join!(leg_a, leg_b);
                PredatorExecResult {
                    attempted: res_a.attempted.saturating_add(res_b.attempted),
                    executed: res_a.executed.saturating_add(res_b.executed),
                    stop_firing: res_a.stop_firing || res_b.stop_firing,
                }
            }
        })
        .collect();

    for r in futures::future::join_all(dual_futures).await {
        out.attempted = out.attempted.saturating_add(r.attempted);
        out.executed = out.executed.saturating_add(r.executed);
    }
    out
}

async fn execute_fire_plan(
    shared: &Arc<EngineShared>,
    bus: &RingBus<EngineEvent>,
    portfolio: &Arc<PortfolioBook>,
    execution: &Arc<ClobExecution>,
    shadow: &Arc<ShadowExecutor>,
    market_rate_budget: &mut HashMap<String, TokenBucket>,
    global_rate_budget: &mut TokenBucket,
    maker_cfg: &MakerConfig,
    predator_cfg: &PredatorCConfig,
    direction_signal: &DirectionSignal,
    plan: &FirePlan,
    tick_fast_recv_ts_ms: i64,
    tick_fast_recv_ts_local_ns: i64,
    now_ms: i64,
) -> PredatorExecResult {
    let mut plan_result = PredatorExecResult::default();
    let opp = &plan.opportunity;
    for (idx, chunk) in plan.chunks.iter().enumerate() {
        if chunk.size <= 0.0 {
            continue;
        }
        if idx > 0 && chunk.send_delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(chunk.send_delay_ms)).await;
        }
        shared.shadow_stats.mark_predator_taker_fired();
        let res = predator_execute_opportunity(
            shared,
            bus,
            portfolio,
            execution,
            shadow,
            market_rate_budget,
            global_rate_budget,
            maker_cfg,
            predator_cfg,
            direction_signal,
            opp,
            chunk.size,
            tick_fast_recv_ts_ms,
            tick_fast_recv_ts_local_ns,
            now_ms,
        )
        .await;
        plan_result.attempted = plan_result.attempted.saturating_add(res.attempted);
        plan_result.executed = plan_result.executed.saturating_add(res.executed);
        if plan.stop_on_reject && res.stop_firing {
            break;
        }
    }
    plan_result
}

pub(super) async fn run_predator_d_for_symbol(
    shared: &Arc<EngineShared>,
    bus: &RingBus<EngineEvent>,
    portfolio: &Arc<PortfolioBook>,
    execution: &Arc<ClobExecution>,
    shadow: &Arc<ShadowExecutor>,
    market_rate_budget: &mut HashMap<String, TokenBucket>,
    global_rate_budget: &mut TokenBucket,
    predator_cfg: &PredatorCConfig,
    symbol: &str,
    tick_fast_recv_ts_ms: i64,
    tick_fast_recv_ts_local_ns: i64,
    now_ms: i64,
) -> PredatorExecResult {
    let d_cfg = &predator_cfg.strategy_d;
    if !predator_cfg.enabled || !d_cfg.enabled {
        return PredatorExecResult::default();
    }

    let direction_signal = {
        let mut direction_signal = {
            let det = shared.predator_direction_detector.read().await;
            det.evaluate(symbol, now_ms)
        };
        if direction_signal.is_none() {
            let fallback_signal = shared
                .predator_latest_direction
                .read()
                .await
                .get(symbol)
                .cloned();
            if let Some(cached) = fallback_signal {
                let cached_ms = cached.ts_ns.div_euclid(1_000_000);
                let age_ms = now_ms.saturating_sub(cached_ms);
                if age_ms <= 2_500 {
                    direction_signal = Some(cached);
                }
            }
        }
        let Some(sig) = direction_signal else {
            return PredatorExecResult::default();
        };
        sig
    };
    if matches!(direction_signal.direction, Direction::Neutral) {
        return PredatorExecResult::default();
    }

    let side = match direction_signal.direction {
        Direction::Up => OrderSide::BuyYes,
        Direction::Down => OrderSide::BuyNo,
        Direction::Neutral => OrderSide::BuyYes,
    };
    let maker_cfg = shared.strategy_cfg.read().await.clone();
    let edge_model_cfg = shared.edge_model_cfg.read().await.clone();
    let market_ids = shared
        .symbol_to_markets
        .read()
        .await
        .get(symbol)
        .cloned()
        .unwrap_or_default();
    if market_ids.is_empty() {
        return PredatorExecResult::default();
    }

    let mut out = PredatorExecResult::default();
    for market_id in market_ids.into_iter().take(32) {
        if d_cfg.cooldown_ms_per_market > 0 {
            let last_fire = shared
                .predator_d_last_fire_ms
                .read()
                .await
                .get(&market_id)
                .copied()
                .unwrap_or(0);
            if now_ms.saturating_sub(last_fire) < d_cfg.cooldown_ms_per_market as i64 {
                continue;
            }
        }

        let book = shared.latest_books.read().await.get(&market_id).cloned();
        let Some(book) = book else {
            continue;
        };
        let entry_price = aggressive_price_for_side(&book, &side);
        if entry_price <= 0.0 {
            continue;
        }
        let spread = spread_for_side(&book, &side);
        let mid_px = mid_for_side(&book, &side).max(1e-6);
        let spread_bps = (spread / mid_px) * 10_000.0;
        if spread_bps < d_cfg.min_gap_bps {
            continue;
        }

        let sig_entry = shared
            .latest_signals
            .get(&market_id)
            .map(|v| v.value().clone());
        let Some(sig_entry) = sig_entry else {
            continue;
        };
        if now_ms.saturating_sub(sig_entry.ts_ms) > 5_000 {
            continue;
        }

        let book_top_lag_ms = if tick_fast_recv_ts_local_ns > 0 && book.recv_ts_local_ns > 0 {
            ((book.recv_ts_local_ns - tick_fast_recv_ts_local_ns).max(0) as f64) / 1_000_000.0
        } else {
            0.0
        };
        let settlement_prob_yes =
            settlement_prob_yes_for_symbol(shared, symbol, sig_entry.signal.fair_yes, now_ms).await;
        let probability = {
            let engine = shared.predator_probability_engine.read().await;
            engine.estimate(
                &sig_entry.signal,
                &direction_signal,
                settlement_prob_yes,
                book_top_lag_ms,
                now_ms,
            )
        };
        if probability.confidence < d_cfg.min_confidence {
            continue;
        }
        let fee_bps = get_fee_rate_bps_cached(shared, &market_id).await;
        let rebate_est_bps = get_rebate_bps_cached(shared, &market_id, fee_bps).await;
        let edge_gross_bps = edge_gross_bps_for_side(probability.p_settle, &side, entry_price);
        let edge_net_bps = edge_gross_bps - fee_bps + rebate_est_bps - edge_model_cfg.fail_cost_bps;
        if edge_net_bps < d_cfg.min_edge_net_bps {
            continue;
        }
        let timeframe = shared
            .market_to_timeframe
            .read()
            .await
            .get(&market_id)
            .cloned()
            .unwrap_or(TimeframeClass::Tf5m);
        let max_notional = d_cfg.max_notional_usdc.max(maker_cfg.base_quote_size);
        let size = (max_notional / entry_price.max(1e-6)).max(0.01);
        let lock_minutes = match timeframe {
            TimeframeClass::Tf5m => 5.0,
            TimeframeClass::Tf15m => 15.0,
            TimeframeClass::Tf1h => 60.0,
            TimeframeClass::Tf1d => 1440.0,
        };
        let notional_usdc = (entry_price.max(0.0) * size.max(0.0)).max(0.0);
        let edge_net_usdc = (edge_net_bps / 10_000.0) * notional_usdc;
        let density = if lock_minutes <= 0.0 {
            0.0
        } else {
            edge_net_usdc / lock_minutes
        };
        let opp = TimeframeOpp {
            timeframe,
            market_id: market_id.clone(),
            symbol: symbol.to_string(),
            direction: direction_signal.direction.clone(),
            side: side.clone(),
            entry_price,
            size,
            edge_gross_bps,
            edge_net_bps,
            edge_net_usdc,
            fee_bps,
            lock_minutes,
            density,
            confidence: probability.confidence,
            ts_ms: now_ms,
        };
        let res = predator_execute_opportunity(
            shared,
            bus,
            portfolio,
            execution,
            shadow,
            market_rate_budget,
            global_rate_budget,
            &maker_cfg,
            predator_cfg,
            &direction_signal,
            &opp,
            size,
            tick_fast_recv_ts_ms,
            tick_fast_recv_ts_local_ns,
            now_ms,
        )
        .await;
        if res.attempted > 0 {
            shared
                .predator_d_last_fire_ms
                .write()
                .await
                .insert(market_id, now_ms);
        }
        out.attempted = out.attempted.saturating_add(res.attempted);
        out.executed = out.executed.saturating_add(res.executed);
    }
    out
}

pub(super) async fn run_predator_c_cross_symbol(
    shared: &Arc<EngineShared>,
    bus: &RingBus<EngineEvent>,
    portfolio: &Arc<PortfolioBook>,
    execution: &Arc<ClobExecution>,
    shadow: &Arc<ShadowExecutor>,
    market_rate_budget: &mut HashMap<String, TokenBucket>,
    global_rate_budget: &mut TokenBucket,
    predator_cfg: &PredatorCConfig,
    leader_direction: &DirectionSignal,
    tick_fast_recv_ts_ms: i64,
    tick_fast_recv_ts_local_ns: i64,
    now_ms: i64,
) -> PredatorExecResult {
    let cross_cfg = &predator_cfg.cross_symbol;
    if !cross_cfg.enabled {
        return PredatorExecResult::default();
    }
    if !leader_direction
        .symbol
        .eq_ignore_ascii_case(&cross_cfg.leader_symbol)
    {
        return PredatorExecResult::default();
    }
    if matches!(leader_direction.direction, Direction::Neutral) {
        return PredatorExecResult::default();
    }
    if leader_direction.confidence < cross_cfg.min_leader_confidence
        || leader_direction.magnitude_pct.abs() < cross_cfg.min_leader_magnitude_pct
    {
        return PredatorExecResult::default();
    }
    let follower_symbols = cross_cfg
        .follower_symbols
        .iter()
        .filter(|s| !s.eq_ignore_ascii_case(&leader_direction.symbol))
        .cloned()
        .collect::<Vec<_>>();
    if follower_symbols.is_empty() {
        return PredatorExecResult::default();
    }

    let correlated_used =
        correlated_locked_positions_for_symbols(shared, &follower_symbols, now_ms).await;
    let mut slots_left = cross_cfg
        .max_correlated_positions
        .saturating_sub(correlated_used);
    if slots_left == 0 {
        return PredatorExecResult::default();
    }

    let mut out = PredatorExecResult::default();
    for follower in follower_symbols {
        if slots_left == 0 {
            break;
        }
        let follower_sig = {
            let det = shared.predator_direction_detector.read().await;
            det.evaluate(&follower, now_ms)
        };
        if let Some(sig) = follower_sig {
            let same_dir = sig.direction == leader_direction.direction;
            if same_dir && sig.confidence > cross_cfg.follower_stale_confidence_max {
                continue;
            }
        }

        let mut forced = leader_direction.clone();
        forced.symbol = follower.clone();
        let res = run_predator_c_for_symbol(
            shared,
            bus,
            portfolio,
            execution,
            shadow,
            market_rate_budget,
            global_rate_budget,
            predator_cfg,
            &follower,
            tick_fast_recv_ts_ms,
            tick_fast_recv_ts_local_ns,
            now_ms,
            Some(forced),
        )
        .await;
        if res.attempted > 0 {
            slots_left = slots_left.saturating_sub(1);
            shared.shadow_stats.mark_predator_cross_symbol_fired();
        }
        out.attempted = out.attempted.saturating_add(res.attempted);
        out.executed = out.executed.saturating_add(res.executed);
    }
    out
}

pub(super) async fn correlated_locked_positions_for_symbols(
    shared: &Arc<EngineShared>,
    symbols: &[String],
    now_ms: i64,
) -> usize {
    if symbols.is_empty() {
        return 0;
    }
    let locks = {
        let mut router = shared.predator_router.write().await;
        router.snapshot_locks(now_ms)
    };
    if locks.is_empty() {
        return 0;
    }
    let symbol_set = symbols
        .iter()
        .map(|s| s.to_ascii_uppercase())
        .collect::<std::collections::HashSet<_>>();
    let market_to_symbol = shared.market_to_symbol.read().await.clone();
    locks
        .iter()
        .filter(|lock| {
            market_to_symbol
                .get(&lock.market_id)
                .map(|s| symbol_set.contains(&s.to_ascii_uppercase()))
                .unwrap_or(false)
        })
        .count()
}

pub(super) async fn symbol_toxic_snapshot(
    shared: &Arc<EngineShared>,
    market_ids: &[String],
) -> (f64, ToxicRegime) {
    if market_ids.is_empty() {
        return (0.0, ToxicRegime::Safe);
    }
    let states = shared.tox_state.read().await;
    let mut max_score = 0.0_f64;
    let mut has_caution = false;
    let mut has_danger = false;
    for market_id in market_ids {
        if let Some(st) = states.get(market_id) {
            max_score = max_score.max(st.last_tox_score);
            match st.last_regime {
                ToxicRegime::Danger => has_danger = true,
                ToxicRegime::Caution => has_caution = true,
                ToxicRegime::Safe => {}
            }
        }
    }
    let regime = if has_danger {
        ToxicRegime::Danger
    } else if has_caution {
        ToxicRegime::Caution
    } else {
        ToxicRegime::Safe
    };
    (max_score, regime)
}

pub(super) fn classify_predator_regime(
    cfg: &PredatorRegimeConfig,
    direction_signal: &DirectionSignal,
    tox_score: f64,
    tox_regime: &ToxicRegime,
    source_health: Option<&SourceHealth>,
    source_health_cfg: &SourceHealthConfig,
) -> Regime {
    if !cfg.enabled {
        return Regime::Active;
    }
    let source_low = source_health
        .map(|h| {
            h.sample_count >= source_health_cfg.min_samples
                && h.score
                    < cfg
                        .defend_min_source_health
                        .max(source_health_cfg.min_score_for_trading)
                || (h.sample_count >= source_health_cfg.min_samples && h.freshness_score < 0.25)
        })
        .unwrap_or(false);
    if source_low {
        return Regime::Defend;
    }
    if tox_score >= cfg.defend_tox_score {
        return Regime::Defend;
    }
    if cfg.defend_on_toxic_danger && matches!(tox_regime, ToxicRegime::Danger) {
        return Regime::Defend;
    }
    if direction_signal.confidence >= cfg.active_min_confidence
        && direction_signal.magnitude_pct.abs() >= cfg.active_min_magnitude_pct
    {
        Regime::Active
    } else {
        Regime::Quiet
    }
}

pub(super) async fn predator_execute_opportunity(
    shared: &Arc<EngineShared>,
    bus: &RingBus<EngineEvent>,
    portfolio: &Arc<PortfolioBook>,
    execution: &Arc<ClobExecution>,
    shadow: &Arc<ShadowExecutor>,
    market_rate_budget: &mut HashMap<String, TokenBucket>,
    global_rate_budget: &mut TokenBucket,
    maker_cfg: &MakerConfig,
    predator_cfg: &PredatorCConfig,
    direction_signal: &DirectionSignal,
    opp: &TimeframeOpp,
    chunk_size: f64,
    tick_fast_recv_ts_ms: i64,
    tick_fast_recv_ts_local_ns: i64,
    now_ms: i64,
) -> PredatorExecResult {
    let mut intent = QuoteIntent {
        market_id: opp.market_id.clone(),
        side: opp.side.clone(),
        price: opp.entry_price,
        size: chunk_size,
        ttl_ms: maker_cfg.ttl_ms,
    };

    let book = shared
        .latest_books
        .read()
        .await
        .get(&intent.market_id)
        .cloned();
    let Some(book) = book else {
        shared
            .shadow_stats
            .mark_predator_taker_skipped("book_missing")
            .await;
        return PredatorExecResult::default();
    };
    let Some(frame_total_ms) = timeframe_total_ms(opp.timeframe.clone()) else {
        shared
            .shadow_stats
            .mark_blocked_with_reason("v52_blocked_timeframe")
            .await;
        return PredatorExecResult::default();
    };
    let time_to_expiry_ms = (frame_total_ms - now_ms.rem_euclid(frame_total_ms)).max(0);
    let remaining_ratio = (time_to_expiry_ms as f64 / frame_total_ms as f64).clamp(0.0, 1.0);
    let time_phase = classify_time_phase(remaining_ratio, &predator_cfg.v52.time_phase);
    let force_taker_now =
        should_force_taker_fallback(time_phase, time_to_expiry_ms, &predator_cfg.v52.execution);
    let apply_force_taker_for_phase = match time_phase {
        TimePhase::Early => false,
        TimePhase::Maturity => predator_cfg.v52.execution.apply_force_taker_in_maturity,
        TimePhase::Late => predator_cfg.v52.execution.apply_force_taker_in_late,
    };
    if force_taker_now {
        shared.shadow_stats.mark_predator_last_30s_taker_fallback();
    }

    // Same interpretation as the maker path: positive means our fast ref tick arrived earlier
    // than the Polymarket book update. For Predator, the ref tick is per-symbol while the book
    // is per-market, so this is still a useful (if approximate) window estimate.
    let tick_age_ms = freshness_ms(now_ms, tick_fast_recv_ts_ms);
    let book_top_lag_ms =
        if tick_age_ms <= 1_500 && tick_fast_recv_ts_local_ns > 0 && book.recv_ts_local_ns > 0 {
            ((book.recv_ts_local_ns - tick_fast_recv_ts_local_ns).max(0) as f64) / 1_000_000.0
        } else {
            0.0
        };

    let inventory = inventory_for_market(portfolio, &intent.market_id);
    let pending_market_exposure = execution.open_order_notional_for_market(&intent.market_id);
    let pending_total_exposure = execution.open_order_notional_total();
    let drawdown = portfolio.snapshot().max_drawdown_pct;
    let risk_limits_snapshot = shared
        .risk_limits
        .read()
        .map(|g| g.clone())
        .unwrap_or_else(|_| RiskLimits::default());
    let open_orders_now = execution.open_orders_count();
    let risk_open_orders_soft_cap = risk_limits_snapshot.max_open_orders.saturating_sub(1);
    if open_orders_now >= risk_open_orders_soft_cap.max(1) {
        shared
            .shadow_stats
            .mark_blocked_with_reason("open_orders_pressure_precheck")
            .await;
        return PredatorExecResult::default();
    }
    shared.shadow_stats.mark_attempted();
    let (tox_score, tox_regime) = {
        let map = shared.tox_state.read().await;
        map.get(&intent.market_id)
            .map(|st| (st.last_tox_score, st.last_regime.clone()))
            .unwrap_or((0.0, ToxicRegime::Safe))
    };
    let pre_scale = adaptive_size_scale(
        drawdown,
        inventory.exposure_notional + pending_market_exposure,
        inventory.exposure_notional + pending_total_exposure,
        &risk_limits_snapshot,
        tox_score,
        &tox_regime,
    );
    intent.size = (intent.size * pre_scale).max(0.01);
    let proposed_notional_usdc = (intent.price.max(0.0) * intent.size.max(0.0)).max(0.0);
    let ctx = RiskContext {
        market_id: intent.market_id.clone(),
        symbol: opp.symbol.clone(),
        order_count: execution.open_orders_count(),
        proposed_size: intent.size,
        proposed_notional_usdc,
        market_notional: inventory.exposure_notional + pending_market_exposure,
        asset_notional: inventory.exposure_notional + pending_total_exposure,
        drawdown_pct: drawdown,
        loss_streak: shared.shadow_stats.loss_streak(),
        now_ms,
    };
    let decision = shared.risk_manager.evaluate(&ctx);
    if !decision.allow {
        shared
            .shadow_stats
            .mark_blocked_with_reason(&format!("risk:{}", decision.reason))
            .await;
        return PredatorExecResult::default();
    }
    if decision.capped_size <= 0.0 {
        shared
            .shadow_stats
            .mark_blocked_with_reason("risk_capped_zero")
            .await;
        return PredatorExecResult::default();
    }
    intent.size = intent.size.min(decision.capped_size);

    let intended_notional_usdc = (intent.price.max(0.0) * intent.size.max(0.0)).max(0.0);
    if intended_notional_usdc < maker_cfg.min_eval_notional_usdc {
        shared
            .shadow_stats
            .mark_blocked_with_reason("tiny_notional")
            .await;
        return PredatorExecResult::default();
    }

    let edge_net_usdc = (opp.edge_net_bps / 10_000.0) * intended_notional_usdc;
    let edge_model_cfg = shared.edge_model_cfg.read().await.clone();
    let dynamic_gate_bps = edge_gate_bps(
        &edge_model_cfg,
        0.0,
        tick_age_ms as f64,
        book_top_lag_ms,
        0.0,
    );
    if opp.edge_net_bps < maker_cfg.min_edge_bps + dynamic_gate_bps {
        shared
            .shadow_stats
            .mark_blocked_with_reason("edge_below_dynamic_gate")
            .await;
        return PredatorExecResult::default();
    }
    if edge_net_usdc < maker_cfg.min_expected_edge_usdc {
        shared
            .shadow_stats
            .mark_blocked_with_reason("edge_notional_too_small")
            .await;
        return PredatorExecResult::default();
    }

    let per_market_rps = (shared.rate_limit_rps / 8.0).max(0.5);
    let market_bucket = market_rate_budget
        .entry(intent.market_id.clone())
        .or_insert_with(|| TokenBucket::new(per_market_rps, (per_market_rps * 2.0).max(1.0)));
    if !global_rate_budget.try_take(1.0) {
        shared
            .shadow_stats
            .mark_blocked_with_reason("rate_budget_global")
            .await;
        return PredatorExecResult::default();
    }
    if !market_bucket.try_take(1.0) {
        shared
            .shadow_stats
            .mark_blocked_with_reason("rate_budget_market")
            .await;
        return PredatorExecResult::default();
    }

    shared.shadow_stats.mark_eligible();
    let order_build_start = Instant::now();
    let token_id = match intent.side {
        OrderSide::BuyYes | OrderSide::SellYes => book.token_id_yes.clone(),
        OrderSide::BuyNo | OrderSide::SellNo => book.token_id_no.clone(),
    };
    let execution_style = if force_taker_now {
        ExecutionStyle::Taker
    } else {
        ExecutionStyle::Maker
    };
    let v2_intent = OrderIntentV2 {
        market_id: intent.market_id.clone(),
        token_id: Some(token_id.clone()),
        side: intent.side.clone(),
        price: intent.price,
        size: intent.size,
        ttl_ms: intent.ttl_ms,
        style: execution_style.clone(),
        tif: if execution_style == ExecutionStyle::Maker {
            OrderTimeInForce::PostOnly
        } else {
            OrderTimeInForce::Fak
        },
        max_slippage_bps: if execution_style == ExecutionStyle::Maker {
            0.0
        } else {
            maker_cfg.taker_max_slippage_bps
        },
        fee_rate_bps: opp.fee_bps,
        expected_edge_net_bps: opp.edge_net_bps,
        client_order_id: Some(new_id()),
        hold_to_resolution: false,
        prebuilt_payload: None,
    };
    let mut v2_intent = v2_intent;
    if execution.is_live() {
        v2_intent.prebuilt_payload = prebuild_order_payload(&v2_intent);
    }
    let order_build_ms = order_build_start.elapsed().as_secs_f64() * 1_000.0;
    let place_start = Instant::now();

    let mut out = PredatorExecResult::default();
    out.attempted = out.attempted.saturating_add(1);

    match execution.place_order_v2(v2_intent).await {
        Ok(ack_v2) if ack_v2.accepted => {
            let accepted_size = ack_v2.accepted_size.max(0.0).min(intent.size);
            if accepted_size <= 0.0 {
                shared
                    .shadow_stats
                    .mark_blocked_with_reason("exchange_reject_zero_size")
                    .await;
                return out;
            }
            intent.size = accepted_size;
            shared.shadow_stats.mark_executed();
            out.executed = out.executed.saturating_add(1);
            let accepted_notional_usdc = (intent.price.max(0.0) * intent.size.max(0.0)).max(0.0);
            let entry_edge_usdc = (opp.edge_net_bps / 10_000.0) * accepted_notional_usdc;
            {
                let mut exit_mgr = shared.predator_exit_manager.write().await;
                exit_mgr.register(PositionLifecycle {
                    position_id: ack_v2.order_id.clone(),
                    market_id: intent.market_id.clone(),
                    symbol: opp.symbol.clone(),
                    opened_at_ms: now_ms,
                    entry_edge_usdc,
                    entry_notional_usdc: accepted_notional_usdc,
                });
            }
            spawn_predator_exit_lifecycle(
                shared.clone(),
                bus.clone(),
                execution.clone(),
                shadow.clone(),
                ack_v2.order_id.clone(),
                intent.market_id.clone(),
                opp.symbol.clone(),
                intent.side.clone(),
                intent.price,
                intent.size,
                maker_cfg.ttl_ms,
                intent.price + opp.edge_gross_bps / 10_000.0,
            );

            let ack_only_ms = if ack_v2.exchange_latency_ms > 0.0 {
                ack_v2.exchange_latency_ms
            } else {
                // Keep latency visibility in non-live mode via local order path RTT.
                place_start.elapsed().as_secs_f64() * 1_000.0
            };
            let tick_to_ack_ms = order_build_ms + ack_only_ms;
            let capturable_window_ms = if ack_only_ms > 0.0 {
                book_top_lag_ms - tick_to_ack_ms
            } else {
                0.0
            };
            if ack_only_ms > 0.0 {
                shared.shadow_stats.push_ack_only_ms(ack_only_ms).await;
                shared
                    .shadow_stats
                    .push_tick_to_ack_ms(tick_to_ack_ms)
                    .await;
                shared
                    .shadow_stats
                    .push_capturable_window_ms(capturable_window_ms)
                    .await;
                metrics::histogram!("latency.ack_only_ms").record(ack_only_ms);
                metrics::histogram!("latency.tick_to_ack_ms").record(tick_to_ack_ms);
                metrics::histogram!("latency.capturable_window_ms").record(capturable_window_ms);
            }

            let ack = OrderAck {
                order_id: ack_v2.order_id,
                market_id: ack_v2.market_id,
                accepted: true,
                ts_ms: ack_v2.ts_ms,
            };
            publish_if_telemetry_subscribers(bus, EngineEvent::OrderAck(ack.clone()));
            let effective_fee_bps = if execution_style == ExecutionStyle::Maker {
                -get_rebate_bps_cached(shared, &intent.market_id, opp.fee_bps)
                    .await
                    .max(0.0)
            } else {
                opp.fee_bps.max(0.0)
            };
            shadow.register_order(
                &ack,
                intent.clone(),
                execution_style.clone(),
                mid_for_side(&book, &intent.side).max(0.0),
                effective_fee_bps,
            );
            if predator_cfg.v52.execution.alpha_window_enabled {
                let anchor_price = aggressive_price_for_side(&book, &intent.side);
                spawn_alpha_window_probe(
                    shared.clone(),
                    intent.market_id.clone(),
                    intent.side.clone(),
                    anchor_price,
                    predator_cfg.v52.execution.alpha_window_move_bps,
                    predator_cfg.v52.execution.alpha_window_poll_ms,
                    predator_cfg.v52.execution.alpha_window_max_wait_ms,
                );
            }
            if execution_style == ExecutionStyle::Maker && apply_force_taker_for_phase {
                spawn_force_taker_fallback_for_maker(
                    shared.clone(),
                    execution.clone(),
                    shadow.clone(),
                    intent.market_id.clone(),
                    token_id.clone(),
                    intent.side.clone(),
                    intent.size,
                    intent.ttl_ms,
                    ack.order_id.clone(),
                    opp.fee_bps,
                    opp.edge_net_bps,
                    opp.timeframe.clone(),
                    predator_cfg.v52.execution.late_force_taker_remaining_ms,
                    predator_cfg.v52.execution.maker_wait_ms_before_force,
                    maker_cfg.taker_max_slippage_bps,
                );
            }
            if let Some(paper) = global_paper_runtime() {
                let momentum_overlay = matches!(time_phase, TimePhase::Maturity)
                    && direction_signal.velocity_bps_per_sec.abs()
                        >= predator_cfg.direction_detector.min_velocity_bps_per_sec;
                let stage = stage_for_phase(time_phase, momentum_overlay);
                let (prob_fast, prob_settle) = shared
                    .predator_latest_probability
                    .read()
                    .await
                    .get(intent.market_id.as_str())
                    .map(|p| (p.p_fast, p.p_settle))
                    .unwrap_or((0.5, 0.5));
                paper
                    .register_order_intent(
                        &ack,
                        PaperIntentCtx {
                            market_id: intent.market_id.clone(),
                            symbol: opp.symbol.clone(),
                            timeframe: opp.timeframe.to_string(),
                            stage,
                            direction: direction_signal.direction.clone(),
                            velocity_bps_per_sec: direction_signal.velocity_bps_per_sec,
                            edge_bps: opp.edge_net_bps,
                            prob_fast,
                            prob_settle,
                            confidence: opp.confidence,
                            action: PaperAction::Enter,
                            intent: execution_style.clone(),
                            requested_size_usdc: (intent.price * intent.size).max(0.0),
                            requested_size_contracts: intent.size,
                            entry_price: intent.price,
                        },
                    )
                    .await;
            }

            for delay_ms in [5_u64, 10_u64, 25_u64] {
                let shot = ShadowShot {
                    shot_id: new_id(),
                    market_id: intent.market_id.clone(),
                    symbol: opp.symbol.clone(),
                    side: intent.side.clone(),
                    execution_style: execution_style.clone(),
                    book_top_lag_ms,
                    ack_only_ms,
                    tick_to_ack_ms,
                    capturable_window_ms,
                    survival_probe_price: aggressive_price_for_side(&book, &intent.side),
                    intended_price: intent.price,
                    size: intent.size,
                    edge_gross_bps: opp.edge_gross_bps,
                    edge_net_bps: opp.edge_net_bps,
                    fee_paid_bps: opp.fee_bps,
                    rebate_est_bps: if execution_style == ExecutionStyle::Maker {
                        get_rebate_bps_cached(shared, &intent.market_id, opp.fee_bps).await
                    } else {
                        0.0
                    },
                    delay_ms,
                    t0_ns: now_ns(),
                    min_edge_bps: predator_cfg.taker_sniper.min_edge_net_bps,
                    tox_score: 0.0,
                    ttl_ms: intent.ttl_ms,
                };
                shared.shadow_stats.push_shot(shot.clone()).await;
                publish_if_telemetry_subscribers(bus, EngineEvent::ShadowShot(shot.clone()));
                spawn_shadow_outcome_task(shared.clone(), bus.clone(), shot);
            }
        }
        Ok(ack_v2) => {
            let reject_code = ack_v2
                .reject_code
                .as_deref()
                .map(normalize_reject_code)
                .unwrap_or_else(|| "unknown".to_string());
            shared
                .shadow_stats
                .mark_blocked_with_reason(&format!("exchange_reject_{reject_code}"))
                .await;
            metrics::counter!("execution.place_rejected").increment(1);
            out.stop_firing = true;
        }
        Err(err) => {
            let reason = classify_execution_error_reason(&err);
            shared.shadow_stats.mark_blocked_with_reason(reason).await;
            tracing::warn!(?err, "predator place_order failed");
            metrics::counter!("execution.place_error").increment(1);
        }
    }

    out
}
