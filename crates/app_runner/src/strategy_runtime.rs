use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
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
use taker_sniper::{FireChunk, FirePlan, TakerAction};

use crate::engine_core::{classify_execution_error_reason, normalize_reject_code};
use crate::execution_eval::{
    aggressive_price_for_side, calculate_dynamic_taker_fee_bps, edge_gross_bps_for_side,
    get_fee_rate_bps_cached, get_rebate_bps_cached, mid_for_side, prebuild_order_payload,
    spread_for_side,
};
use crate::feed_runtime::settlement_prob_yes_for_symbol;
use crate::fusion_engine::TokenBucket;
use crate::paper_runtime::{global_paper_runtime, PaperIntentCtx};
use crate::state::{
    EngineShared, PredatorCConfig, PredatorCPriority, PredatorRegimeConfig, RollV1ReverseConfig,
    RollV1TimeframeConfig, SourceHealthConfig, StrategyEngineMode, V52ExecutionConfig,
    V52TimePhaseConfig,
};
use crate::stats_utils::{freshness_ms, now_ns};
use crate::strategy_policy::{
    adaptive_size_scale, edge_gate_bps, inventory_for_market, timeframe_weight,
};
use crate::{
    publish_if_telemetry_subscribers, spawn_detached, spawn_predator_exit_lifecycle,
    spawn_shadow_outcome_task, PredatorExecResult,
};
use core_types::RefTick;

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

fn lock_minutes_for_timeframe(timeframe: &TimeframeClass) -> f64 {
    match timeframe {
        TimeframeClass::Tf5m => 5.0,
        TimeframeClass::Tf15m => 15.0,
        TimeframeClass::Tf1h => 60.0,
        TimeframeClass::Tf1d => 1_440.0,
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

fn flatten_side_for_entry(side: &OrderSide) -> OrderSide {
    match side {
        OrderSide::BuyYes => OrderSide::SellYes,
        OrderSide::BuyNo => OrderSide::SellNo,
        OrderSide::SellYes => OrderSide::BuyYes,
        OrderSide::SellNo => OrderSide::BuyNo,
    }
}

fn opposite_buy_side_for_entry(side: &OrderSide) -> OrderSide {
    match side {
        OrderSide::BuyYes | OrderSide::SellNo => OrderSide::BuyNo,
        OrderSide::BuyNo | OrderSide::SellYes => OrderSide::BuyYes,
    }
}

fn token_id_for_side(book: &BookTop, side: &OrderSide) -> String {
    match side {
        OrderSide::BuyYes | OrderSide::SellYes => book.token_id_yes.clone(),
        OrderSide::BuyNo | OrderSide::SellNo => book.token_id_no.clone(),
    }
}

fn maker_price_for_side(book: &BookTop, side: &OrderSide) -> f64 {
    match side {
        OrderSide::BuyYes => book.bid_yes,
        OrderSide::BuyNo => book.bid_no,
        OrderSide::SellYes => book.ask_yes,
        OrderSide::SellNo => book.ask_no,
    }
}

fn side_probability_from_mid_yes(mid_yes: f64, side: &OrderSide) -> f64 {
    match side {
        OrderSide::BuyYes | OrderSide::SellNo => mid_yes,
        OrderSide::BuyNo | OrderSide::SellYes => 1.0 - mid_yes,
    }
}

fn aligned_velocity_for_position_side(raw_velocity_bps_per_sec: f64, side: &OrderSide) -> f64 {
    match side {
        OrderSide::BuyYes | OrderSide::SellNo => raw_velocity_bps_per_sec,
        OrderSide::BuyNo | OrderSide::SellYes => -raw_velocity_bps_per_sec,
    }
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
        if execution
            .cancel_order(&maker_order_id, &market_id)
            .await
            .is_err()
        {
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
            prebuilt_auth: None,
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

async fn place_roll_v1_order_and_track(
    shared: &Arc<EngineShared>,
    bus: &RingBus<EngineEvent>,
    execution: &Arc<ClobExecution>,
    shadow: &Arc<ShadowExecutor>,
    market_id: &str,
    symbol: &str,
    side: OrderSide,
    size: f64,
    style: ExecutionStyle,
    ttl_ms: u64,
    max_slippage_bps: f64,
    fee_rate_bps: f64,
    expected_edge_net_bps: f64,
    paper_action: PaperAction,
    prob_fast: f64,
    prob_settle: f64,
    confidence: f64,
) -> Option<(OrderAck, String)> {
    let book = shared.latest_books.read().await.get(market_id).cloned()?;
    let price = if style == ExecutionStyle::Maker {
        maker_price_for_side(&book, &side)
    } else {
        aggressive_price_for_side(&book, &side)
    };
    if !price.is_finite() || price <= 0.0 {
        return None;
    }
    let timeframe = shared
        .market_to_timeframe
        .read()
        .await
        .get(market_id)
        .cloned()
        .unwrap_or(TimeframeClass::Tf5m);
    let timeframe_label = timeframe_tag(timeframe.clone()).to_string();
    let quote_intent = QuoteIntent {
        market_id: market_id.to_string(),
        side: side.clone(),
        price,
        size: size.max(0.01),
        ttl_ms,
    };
    publish_if_telemetry_subscribers(bus, EngineEvent::QuoteIntent(quote_intent.clone()));
    let intent = OrderIntentV2 {
        market_id: market_id.to_string(),
        token_id: Some(token_id_for_side(&book, &side)),
        side: side.clone(),
        price,
        size: size.max(0.01),
        ttl_ms,
        style: style.clone(),
        tif: if style == ExecutionStyle::Maker {
            OrderTimeInForce::PostOnly
        } else {
            OrderTimeInForce::Fak
        },
        max_slippage_bps: if style == ExecutionStyle::Maker {
            0.0
        } else {
            max_slippage_bps.max(0.0)
        },
        fee_rate_bps,
        expected_edge_net_bps,
        client_order_id: Some(new_id()),
        hold_to_resolution: false,
        prebuilt_payload: None,
        prebuilt_auth: None,
    };
    let ack_v2 = execution.place_order_v2(intent).await.ok()?;
    if !ack_v2.accepted {
        return None;
    }
    let ack = OrderAck {
        order_id: ack_v2.order_id,
        market_id: ack_v2.market_id,
        accepted: true,
        ts_ms: ack_v2.ts_ms,
    };
    publish_if_telemetry_subscribers(bus, EngineEvent::OrderAck(ack.clone()));
    let effective_fee_bps = if style == ExecutionStyle::Maker {
        -get_rebate_bps_cached(shared, market_id, fee_rate_bps)
            .await
            .max(0.0)
    } else {
        calculate_dynamic_taker_fee_bps(&side, price, fee_rate_bps).max(0.0)
    };
    shadow.register_order(
        &ack,
        quote_intent.clone(),
        style.clone(),
        mid_for_side(&book, &side).max(0.0),
        effective_fee_bps,
    );
    if let Some(paper) = global_paper_runtime() {
        paper
            .register_order_intent(
                &ack,
                PaperIntentCtx {
                    market_id: market_id.to_string(),
                    symbol: symbol.to_string(),
                    timeframe: timeframe_label,
                    stage: Stage::Momentum,
                    direction: side_to_direction(&side),
                    velocity_bps_per_sec: 0.0,
                    edge_bps: expected_edge_net_bps,
                    prob_fast,
                    prob_settle,
                    confidence,
                    action: paper_action,
                    intent: style,
                    requested_size_usdc: (price * quote_intent.size).max(0.0),
                    requested_size_contracts: quote_intent.size,
                    entry_price: price,
                },
            )
            .await;
    }
    Some((ack, market_id.to_string()))
}

fn spawn_roll_v1_maker_ttl_fallback(
    shared: Arc<EngineShared>,
    bus: RingBus<EngineEvent>,
    execution: Arc<ClobExecution>,
    shadow: Arc<ShadowExecutor>,
    market_id: String,
    symbol: String,
    maker_order_id: String,
    fallback_side: OrderSide,
    size: f64,
    fee_rate_bps: f64,
    ttl_ms: u64,
    taker_max_slippage_bps: f64,
    paper_action: PaperAction,
) {
    spawn_detached("roll_v1_maker_ttl_fallback", false, async move {
        tokio::time::sleep(Duration::from_millis(ttl_ms.max(10))).await;
        if !execution.has_open_order(&maker_order_id) {
            return;
        }
        if execution.cancel_order(&maker_order_id, &market_id).await.is_err() {
            return;
        }
        shadow.cancel(&maker_order_id);
        let (prob_fast, prob_settle, confidence) = shared
            .predator_latest_probability
            .read()
            .await
            .get(&market_id)
            .map(|p| (p.p_fast, p.p_settle, p.confidence))
            .unwrap_or((0.5, 0.5, 0.0));
        let _ = place_roll_v1_order_and_track(
            &shared,
            &bus,
            &execution,
            &shadow,
            &market_id,
            &symbol,
            fallback_side,
            size,
            ExecutionStyle::Taker,
            150,
            taker_max_slippage_bps,
            fee_rate_bps,
            0.0,
            paper_action,
            prob_fast,
            prob_settle,
            confidence,
        )
        .await;
    });
}

fn spawn_roll_v1_position_lifecycle(
    shared: Arc<EngineShared>,
    bus: RingBus<EngineEvent>,
    execution: Arc<ClobExecution>,
    shadow: Arc<ShadowExecutor>,
    position_id: String,
    market_id: String,
    symbol: String,
    timeframe: TimeframeClass,
    entry_side: OrderSide,
    entry_price: f64,
    size: f64,
    fee_rate_bps: f64,
    tf_cfg: RollV1TimeframeConfig,
    reverse_cfg: RollV1ReverseConfig,
    taker_max_slippage_bps: f64,
) {
    spawn_detached("roll_v1_position_lifecycle", false, async move {
        let mut reverse_start_ms: Option<i64> = None;
        let mut peak_prob = side_probability_from_mid_yes(entry_price.clamp(0.0, 1.0), &entry_side);
        let scan_ms = tf_cfg.scan_interval_ms.clamp(20, 250);
        let tf_label = timeframe_tag(timeframe.clone()).to_string();
        loop {
            tokio::time::sleep(Duration::from_millis(scan_ms)).await;
            let now_ms = Utc::now().timestamp_millis();
            let remaining_ms = if let Some(total_ms) = timeframe_total_ms(timeframe.clone()) {
                (total_ms - now_ms.rem_euclid(total_ms)).max(0)
            } else {
                i64::MAX
            };
            if remaining_ms <= tf_cfg.entry_end_remaining_ms {
                break;
            }
            let book = match shared.latest_books.read().await.get(&market_id).cloned() {
                Some(v) => v,
                None => continue,
            };
            let mid_yes = ((book.bid_yes + book.ask_yes) * 0.5).clamp(0.0, 1.0);
            let side_prob = side_probability_from_mid_yes(mid_yes, &entry_side).clamp(0.0, 1.0);
            if side_prob.is_finite() {
                peak_prob = peak_prob.max(side_prob);
            }
            let drop_pct = if peak_prob > 1e-6 {
                ((peak_prob - side_prob).max(0.0) / peak_prob).clamp(0.0, 1.0)
            } else {
                0.0
            };
            let direction = shared
                .predator_latest_direction
                .read()
                .await
                .get(symbol.as_str())
                .cloned();
            let aligned_velocity_bps = direction
                .as_ref()
                .map(|s| aligned_velocity_for_position_side(s.velocity_bps_per_sec, &entry_side))
                .unwrap_or(0.0);
            let reversal_condition = aligned_velocity_bps <= tf_cfg.reverse_velocity_bps_per_sec
                && drop_pct >= tf_cfg.reverse_drop_pct;
            if reversal_condition {
                let started = reverse_start_ms.get_or_insert(now_ms);
                if now_ms.saturating_sub(*started) < tf_cfg.reverse_persist_ms as i64 {
                    continue;
                }
            } else {
                reverse_start_ms = None;
                continue;
            }

            let strong = aligned_velocity_bps <= reverse_cfg.strong_reversal_velocity_bps_per_sec;
            let close_side = flatten_side_for_entry(&entry_side);
            let reverse_side = opposite_buy_side_for_entry(&entry_side);
            let (prob_fast, prob_settle, confidence) = shared
                .predator_latest_probability
                .read()
                .await
                .get(&market_id)
                .map(|p| (p.p_fast, p.p_settle, p.confidence))
                .unwrap_or((0.5, 0.5, 0.0));
            // Parallel dual-send: submit flatten and reverse legs at the same time.
            let flatten_style = ExecutionStyle::Maker;
            let reverse_style = if strong {
                ExecutionStyle::Taker
            } else {
                ExecutionStyle::Maker
            };
            let (flatten_ack, reverse_ack) = tokio::join!(
                place_roll_v1_order_and_track(
                    &shared,
                    &bus,
                    &execution,
                    &shadow,
                    &market_id,
                    &symbol,
                    close_side.clone(),
                    size,
                    flatten_style,
                    reverse_cfg.maker_first_ttl_ms,
                    0.0,
                    fee_rate_bps,
                    0.0,
                    PaperAction::ReversalExit,
                    prob_fast,
                    prob_settle,
                    confidence,
                ),
                place_roll_v1_order_and_track(
                    &shared,
                    &bus,
                    &execution,
                    &shadow,
                    &market_id,
                    &symbol,
                    reverse_side.clone(),
                    size,
                    reverse_style.clone(),
                    if reverse_style == ExecutionStyle::Maker {
                        reverse_cfg.maker_first_ttl_ms
                    } else {
                        150
                    },
                    taker_max_slippage_bps,
                    fee_rate_bps,
                    0.0,
                    PaperAction::Add,
                    prob_fast,
                    prob_settle,
                    confidence,
                )
            );

            if let Some((ack, _)) = flatten_ack {
                spawn_roll_v1_maker_ttl_fallback(
                    shared.clone(),
                    bus.clone(),
                    execution.clone(),
                    shadow.clone(),
                    market_id.clone(),
                    symbol.clone(),
                    ack.order_id,
                    close_side,
                    size,
                    fee_rate_bps,
                    reverse_cfg.maker_first_ttl_ms,
                    taker_max_slippage_bps,
                    PaperAction::ReversalExit,
                );
            } else {
                let fallback = place_roll_v1_order_and_track(
                    &shared,
                    &bus,
                    &execution,
                    &shadow,
                    &market_id,
                    &symbol,
                    close_side,
                    size,
                    ExecutionStyle::Taker,
                    150,
                    taker_max_slippage_bps,
                    fee_rate_bps,
                    0.0,
                    PaperAction::ReversalExit,
                    prob_fast,
                    prob_settle,
                    confidence,
                )
                .await;
                if fallback.is_none() {
                    shared
                        .shadow_stats
                        .mark_blocked_with_reason_ctx(
                            "roll_v1_reverse_flatten_failed",
                            Some(symbol.as_str()),
                            Some(tf_label.as_str()),
                        )
                        .await;
                    break;
                }
            }

            if reverse_style == ExecutionStyle::Maker {
                if let Some((ack, _)) = reverse_ack {
                    spawn_roll_v1_maker_ttl_fallback(
                        shared.clone(),
                        bus.clone(),
                        execution.clone(),
                        shadow.clone(),
                        market_id.clone(),
                        symbol.clone(),
                        ack.order_id,
                        reverse_side,
                        size,
                        fee_rate_bps,
                        reverse_cfg.maker_first_ttl_ms,
                        taker_max_slippage_bps,
                        PaperAction::Add,
                    );
                } else {
                    let _ = place_roll_v1_order_and_track(
                        &shared,
                        &bus,
                        &execution,
                        &shadow,
                        &market_id,
                        &symbol,
                        reverse_side,
                        size,
                        ExecutionStyle::Taker,
                        150,
                        taker_max_slippage_bps,
                        fee_rate_bps,
                        0.0,
                        PaperAction::Add,
                        prob_fast,
                        prob_settle,
                        confidence,
                    )
                    .await;
                }
                shared.shadow_stats.record_exit_reason("roll_v1_reverse_weak").await;
            } else {
                shared
                    .shadow_stats
                    .record_exit_reason("roll_v1_reverse_strong")
                    .await;
            }
            break;
        }
        let _ = shared
            .predator_exit_manager
            .write()
            .await
            .close(&position_id);
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
    tick_fast: &RefTick,
    latest_fast_ticks: &std::collections::HashMap<String, RefTick>,
    latest_anchor_ticks: &std::collections::HashMap<String, RefTick>,
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
        tick_fast,
        latest_fast_ticks,
        latest_anchor_ticks,
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
            latest_fast_ticks,
            latest_anchor_ticks,
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
            tick_fast,
            latest_fast_ticks,
            latest_anchor_ticks,
            now_ms,
        )
        .await;
        total.attempted = total.attempted.saturating_add(d_res.attempted);
        total.executed = total.executed.saturating_add(d_res.executed);
    }
    total
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RollStage {
    Idle,
    Probe,
    Scale,
    Defend,
    FinalHold,
    Settled,
}

fn roll_cfg_for_timeframe<'a>(
    cfg: &'a PredatorCConfig,
    timeframe: &TimeframeClass,
) -> &'a RollV1TimeframeConfig {
    match timeframe {
        TimeframeClass::Tf5m => &cfg.roll_v1.tf5m,
        TimeframeClass::Tf15m => &cfg.roll_v1.tf15m,
        _ => &cfg.roll_v1.tf5m,
    }
}

fn roll_scale_min_edge_for_timeframe(cfg: &PredatorCConfig, timeframe: &TimeframeClass) -> f64 {
    roll_cfg_for_timeframe(cfg, timeframe).scale_stage_min_edge_net_bps
}

fn scale_size_by_roll_stage(
    stage: RollStage,
    edge_net_bps: f64,
    cfg: &RollV1TimeframeConfig,
) -> f64 {
    let edge_factor = ((edge_net_bps - 2.0) / 30.0).clamp(0.0, 1.0);
    let pct = match stage {
        RollStage::Probe => {
            cfg.probe_add_pct_min + (cfg.probe_add_pct_max - cfg.probe_add_pct_min) * edge_factor
        }
        RollStage::Scale => {
            cfg.scale_add_pct_min + (cfg.scale_add_pct_max - cfg.scale_add_pct_min) * edge_factor
        }
        RollStage::Defend => cfg.scale_add_pct_min.max(0.001),
        RollStage::FinalHold => cfg.final_add_pct,
        RollStage::Idle | RollStage::Settled => 0.0,
    };
    pct.max(0.0)
}

pub(super) async fn evaluate_and_route_roll_v1(
    shared: &Arc<EngineShared>,
    bus: &RingBus<EngineEvent>,
    portfolio: &Arc<PortfolioBook>,
    execution: &Arc<ClobExecution>,
    shadow: &Arc<ShadowExecutor>,
    market_rate_budget: &mut HashMap<String, TokenBucket>,
    global_rate_budget: &mut TokenBucket,
    predator_cfg: &PredatorCConfig,
    symbol: &str,
    tick_fast: &RefTick,
    latest_fast_ticks: &std::collections::HashMap<String, RefTick>,
    latest_anchor_ticks: &std::collections::HashMap<String, RefTick>,
    now_ms: i64,
) -> PredatorExecResult {
    if !predator_cfg.enabled || !predator_cfg.roll_v1.enabled {
        return PredatorExecResult::default();
    }
    if !matches!(
        predator_cfg.strategy_engine.engine_mode,
        StrategyEngineMode::RollV1
    ) {
        return PredatorExecResult::default();
    }
    if !predator_cfg
        .strategy_engine
        .enabled_symbols
        .iter()
        .any(|s| s.eq_ignore_ascii_case(symbol))
    {
        shared
            .shadow_stats
            .mark_blocked_with_reason_ctx("market_not_active", Some(symbol), None)
            .await;
        return PredatorExecResult::default();
    }
    if execution.is_live()
        && predator_cfg.roll_v1.risk.require_compounder_when_live
        && !predator_cfg.compounder.enabled
    {
        shared
            .shadow_stats
            .mark_blocked_with_reason_ctx("compounder_required_when_live", Some(symbol), None)
            .await;
        return PredatorExecResult::default();
    }
    let maker_cfg = shared.strategy_cfg.read().await.clone();
    let edge_model_cfg = shared.edge_model_cfg.read().await.clone();

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
                if now_ms.saturating_sub(cached_ms) <= 2_500 {
                    direction_signal = Some(cached);
                }
            }
        }
        let Some(sig) = direction_signal else {
            shared
                .shadow_stats
                .mark_predator_taker_skipped("no_direction_signal")
                .await;
            return PredatorExecResult::default();
        };
        sig
    };
    let neutral_fallback = matches!(direction_signal.direction, Direction::Neutral);
    if neutral_fallback {
        shared
            .shadow_stats
            .mark_blocked_with_reason_ctx("neutral_direction_fallback", Some(symbol), None)
            .await;
    }

    shared
        .shadow_stats
        .mark_predator_direction(&direction_signal.direction);
    {
        let mut map = shared.predator_latest_direction.write().await;
        map.insert(symbol.to_string(), direction_signal.clone());
    }
    publish_if_telemetry_subscribers(bus, EngineEvent::DirectionSignal(direction_signal.clone()));

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

    let static_quote_notional_usdc = (predator_cfg.compounder.initial_capital_usdc.max(0.0)
        * predator_cfg.compounder.position_fraction.clamp(0.0, 1.0))
    .max(predator_cfg.compounder.min_quote_size.max(0.01));
    let (quote_notional_usdc, total_capital_usdc, daily_pnl_usdc) =
        if predator_cfg.compounder.enabled {
            let c = shared.predator_compounder.read().await;
            (
                c.recommended_quote_notional_usdc(),
                c.available(),
                c.daily_pnl(),
            )
        } else {
            (
                static_quote_notional_usdc,
                predator_cfg.compounder.initial_capital_usdc.max(0.0),
                0.0,
            )
        };

    if predator_cfg.compounder.initial_capital_usdc > 0.0 {
        let daily_loss_pct = ((-daily_pnl_usdc).max(0.0) * 100.0)
            / predator_cfg.compounder.initial_capital_usdc.max(1e-6);
        if daily_loss_pct >= predator_cfg.roll_v1.risk.daily_loss_stop_pct {
            shared
                .shadow_stats
                .mark_blocked_with_reason_ctx("risk_cap", Some(symbol), None)
                .await;
            return PredatorExecResult::default();
        }
    }

    #[derive(Debug, Clone)]
    struct RollCand {
        market_id: String,
        timeframe: TimeframeClass,
        side: OrderSide,
        entry_price: f64,
        edge_gross_bps: f64,
        edge_net_bps: f64,
        size: f64,
        target_l2_size: f64,
        fee_applied: f64,
    }

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
    let scoped_market_ids = if predator_cfg
        .strategy_engine
        .market_scope
        .eq_ignore_ascii_case("near_expiry_active_only")
    {
        let end_map = shared.market_to_end_ts_ms.read().await;
        let mut best_per_tf: HashMap<TimeframeClass, (String, i64)> = HashMap::new();
        for market_id in &market_ids {
            let Some(tf) = tf_by_market.get(market_id).cloned() else {
                continue;
            };
            let Some(end_ms) = end_map.get(market_id).copied() else {
                continue;
            };
            let remain_ms = end_ms.saturating_sub(now_ms);
            if remain_ms < -5_000 {
                continue;
            }
            match best_per_tf.get_mut(&tf) {
                Some((best_id, best_remain_ms)) => {
                    if remain_ms < *best_remain_ms {
                        *best_id = market_id.clone();
                        *best_remain_ms = remain_ms;
                    }
                }
                None => {
                    best_per_tf.insert(tf, (market_id.clone(), remain_ms));
                }
            }
        }
        let mut ids = best_per_tf
            .into_values()
            .map(|(market_id, _)| market_id)
            .collect::<Vec<_>>();
        ids.sort();
        ids
    } else {
        market_ids.clone()
    };

    let mut cands: Vec<RollCand> = Vec::new();
    for market_id in scoped_market_ids.into_iter().take(32) {
        let Some(timeframe) = tf_by_market.get(&market_id).cloned() else {
            continue;
        };
        if !matches!(timeframe, TimeframeClass::Tf5m | TimeframeClass::Tf15m) {
            continue;
        }
        let tf_label = timeframe_tag(timeframe.clone());
        if !predator_cfg
            .strategy_engine
            .enabled_timeframes
            .iter()
            .any(|tf| tf.eq_ignore_ascii_case(tf_label))
        {
            continue;
        }
        let Some(book) = book_by_market.get(&market_id).cloned() else {
            continue;
        };
        let Some(sig_entry) = shared
            .latest_signals
            .get(&market_id)
            .map(|v| v.value().clone())
        else {
            continue;
        };
        if now_ms.saturating_sub(sig_entry.ts_ms) > 5_000 {
            continue;
        }

        let Some(frame_total_ms) = timeframe_total_ms(timeframe.clone()) else {
            continue;
        };
        let remaining_ms = (frame_total_ms - now_ms.rem_euclid(frame_total_ms)).max(0);
        let tf_cfg = roll_cfg_for_timeframe(predator_cfg, &timeframe);
        let too_early = tf_cfg.entry_start_remaining_ms > 0
            && remaining_ms > tf_cfg.entry_start_remaining_ms;
        let too_late = remaining_ms < tf_cfg.entry_end_remaining_ms;
        if too_early || too_late {
            let reason = if too_early {
                "before_entry_window"
            } else {
                "final_window_guard"
            };
            shared
                .shadow_stats
                .mark_blocked_with_reason_ctx(reason, Some(symbol), Some(tf_label))
                .await;
            continue;
        }

        let settlement_prob_yes = settlement_prob_yes_for_symbol(
            shared,
            symbol,
            sig_entry.signal.fair_yes,
            latest_fast_ticks,
            latest_anchor_ticks,
            now_ms,
        )
        .await;
        let probability = {
            let engine = shared.predator_probability_engine.read().await;
            engine.estimate(
                &sig_entry.signal,
                &direction_signal,
                settlement_prob_yes,
                0.0,
                now_ms,
            )
        };
        {
            let mut map = shared.predator_latest_probability.write().await;
            map.insert(market_id.clone(), probability.clone());
        }

        if !neutral_fallback && direction_signal.confidence < tf_cfg.min_direction_confidence {
            shared
                .shadow_stats
                .mark_blocked_with_reason_ctx("low_confidence", Some(symbol), Some(tf_label))
                .await;
            continue;
        }

        let fee_bps = get_fee_rate_bps_cached(shared, &market_id).await;
        let rebate_est_bps = get_rebate_bps_cached(shared, &market_id, fee_bps).await;
        let yes_price = aggressive_price_for_side(&book, &OrderSide::BuyYes);
        let no_price = aggressive_price_for_side(&book, &OrderSide::BuyNo);
        let yes_edge_gross =
            edge_gross_bps_for_side(probability.p_settle, &OrderSide::BuyYes, yes_price);
        let no_edge_gross =
            edge_gross_bps_for_side(probability.p_settle, &OrderSide::BuyNo, no_price);
        let yes_fee_taker_bps =
            calculate_dynamic_taker_fee_bps(&OrderSide::BuyYes, yes_price, fee_bps);
        let no_fee_taker_bps =
            calculate_dynamic_taker_fee_bps(&OrderSide::BuyNo, no_price, fee_bps);
        let yes_edge_net =
            yes_edge_gross - yes_fee_taker_bps + rebate_est_bps - edge_model_cfg.fail_cost_bps;
        let no_edge_net =
            no_edge_gross - no_fee_taker_bps + rebate_est_bps - edge_model_cfg.fail_cost_bps;

        if neutral_fallback && probability.confidence < tf_cfg.min_direction_confidence {
            shared
                .shadow_stats
                .mark_blocked_with_reason_ctx("low_confidence_prob", Some(symbol), Some(tf_label))
                .await;
            continue;
        }

        let (side, entry_price, edge_gross_bps, edge_net_bps, fee_applied) = match direction_signal
            .direction
        {
            Direction::Up => (
                OrderSide::BuyYes,
                yes_price,
                yes_edge_gross,
                yes_edge_net,
                yes_fee_taker_bps,
            ),
            Direction::Down => (
                OrderSide::BuyNo,
                no_price,
                no_edge_gross,
                no_edge_net,
                no_fee_taker_bps,
            ),
            Direction::Neutral => {
                if yes_edge_net >= no_edge_net {
                    (
                        OrderSide::BuyYes,
                        yes_price,
                        yes_edge_gross,
                        yes_edge_net,
                        yes_fee_taker_bps,
                    )
                } else {
                    (
                        OrderSide::BuyNo,
                        no_price,
                        no_edge_gross,
                        no_edge_net,
                        no_fee_taker_bps,
                    )
                }
            }
        };
        if edge_net_bps <= 0.0 {
            shared
                .shadow_stats
                .mark_blocked_with_reason_ctx("fee_negative_ev", Some(symbol), Some(tf_label))
                .await;
            continue;
        }

        let stage = if remaining_ms <= tf_cfg.entry_end_remaining_ms {
            RollStage::Settled
        } else if remaining_ms <= (tf_cfg.entry_end_remaining_ms * 2).max(5_000) {
            RollStage::FinalHold
        } else if direction_signal.velocity_bps_per_sec.abs()
            >= predator_cfg
                .roll_v1
                .reverse
                .strong_reversal_velocity_bps_per_sec
                .abs()
        {
            RollStage::Defend
        } else if edge_net_bps >= tf_cfg.scale_stage_min_edge_net_bps {
            RollStage::Scale
        } else if edge_net_bps > 0.0 {
            RollStage::Probe
        } else {
            RollStage::Idle
        };
        if matches!(stage, RollStage::Idle | RollStage::Settled) {
            continue;
        }

        let pct = scale_size_by_roll_stage(stage, edge_net_bps, tf_cfg);
        let mut notional_usdc = (total_capital_usdc.max(0.0) * pct).max(quote_notional_usdc);
        let cap_per_market = total_capital_usdc.max(0.0) * tf_cfg.max_position_pct_per_market;
        if cap_per_market > 0.0 {
            notional_usdc = notional_usdc.min(cap_per_market);
        }
        let max_total = total_capital_usdc.max(0.0)
            * (predator_cfg.roll_v1.risk.max_total_exposure_pct / 100.0);
        if max_total > 0.0 {
            notional_usdc = notional_usdc.min(max_total);
        }
        let size = (notional_usdc / entry_price.max(1e-6)).max(0.01);
        let target_l2_size = match side {
            OrderSide::BuyYes => book.ask_size_yes,
            OrderSide::BuyNo => book.ask_size_no,
            OrderSide::SellYes => book.bid_size_yes,
            OrderSide::SellNo => book.bid_size_no,
        };
        cands.push(RollCand {
            market_id,
            timeframe,
            side,
            entry_price,
            edge_gross_bps,
            edge_net_bps,
            size,
            target_l2_size,
            fee_applied,
        });
    }

    if cands.is_empty() {
        return PredatorExecResult::default();
    }

    let mut fire_plans_by_market: HashMap<String, FirePlan> = HashMap::new();
    for cand in cands {
        let notional = (cand.entry_price.max(0.0) * cand.size.max(0.0)).max(0.0);
        let plan = FirePlan {
            opportunity: TimeframeOpp {
                timeframe: cand.timeframe.clone(),
                market_id: cand.market_id.clone(),
                symbol: symbol.to_string(),
                direction: side_to_direction(&cand.side),
                side: cand.side.clone(),
                entry_price: cand.entry_price,
                size: cand.size,
                target_l2_size: cand.target_l2_size,
                edge_gross_bps: cand.edge_gross_bps,
                edge_net_bps: cand.edge_net_bps,
                edge_net_usdc: (cand.edge_net_bps / 10_000.0) * notional,
                fee_bps: cand.fee_applied,
                lock_minutes: lock_minutes_for_timeframe(&cand.timeframe),
                density: 1.0,
                confidence: direction_signal.confidence,
                ts_ms: now_ms,
            },
            chunks: vec![FireChunk {
                size: cand.size,
                send_delay_ms: 0,
            }],
            stop_on_reject: true,
        };
        fire_plans_by_market.insert(cand.market_id, plan);
    }

    if fire_plans_by_market.is_empty() {
        return PredatorExecResult::default();
    }
    let mut locked_plans: Vec<FirePlan> = Vec::new();
    let mut locked_by_tf_usdc: HashMap<String, f64> = HashMap::new();
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
        for (tf, v) in router.locked_by_tf_usdc(now_ms) {
            locked_by_tf_usdc.insert(tf.to_string(), v);
        }
    }
    shared
        .shadow_stats
        .set_predator_router_locked_by_tf_usdc(locked_by_tf_usdc)
        .await;
    if locked_plans.is_empty() {
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
            let direction_signal =
                direction_signal_for_side(&direction_signal, &plan.opportunity.side);
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
                    tick_fast,
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
    out
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
    tick_fast: &RefTick,
    latest_fast_ticks: &std::collections::HashMap<String, RefTick>,
    latest_anchor_ticks: &std::collections::HashMap<String, RefTick>,
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
            .mark_blocked_with_reason_ctx("compounder_required_when_live", Some(symbol), None)
            .await;
        return PredatorExecResult::default();
    }
    let source_health_cfg = shared.source_health_cfg.read().await.clone();
    let primary_fast_source = Some(tick_fast.source.clone());
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
        edge_gross_bps: f64,
        edge_net_bps: f64,
        size: f64,
        target_l2_size: f64,
        fee_applied: f64,
        rebate_est_bps: f64,
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
                .mark_blocked_with_reason_ctx(
                    "v52_blocked_timeframe",
                    Some(symbol),
                    Some(timeframe_tag(timeframe.clone())),
                )
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
                .mark_blocked_with_reason_ctx(
                    "v52_timeframe_not_allowed",
                    Some(symbol),
                    Some(timeframe_tag(timeframe.clone())),
                )
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
        let book_top_lag_ms = if tick_fast.recv_ts_local_ns > 0 && book.recv_ts_local_ns > 0 {
            ((book.recv_ts_local_ns - tick_fast.recv_ts_local_ns).max(0) as f64) / 1_000_000.0
        } else {
            0.0
        };
        let settlement_prob_yes = settlement_prob_yes_for_symbol(
            shared,
            symbol,
            sig_entry.signal.fair_yes,
            latest_fast_ticks,
            latest_anchor_ticks,
            now_ms,
        )
        .await;
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
        let yes_edge_gross =
            edge_gross_bps_for_side(probability.p_settle, &OrderSide::BuyYes, yes_price);
        let no_edge_gross =
            edge_gross_bps_for_side(probability.p_settle, &OrderSide::BuyNo, no_price);

        // Dynamic taker fee uses per-market fee-rate ceiling from cache/API.
        let yes_fee_taker_bps =
            calculate_dynamic_taker_fee_bps(&OrderSide::BuyYes, yes_price, fee_bps);
        let no_fee_taker_bps =
            calculate_dynamic_taker_fee_bps(&OrderSide::BuyNo, no_price, fee_bps);

        let yes_edge_net =
            yes_edge_gross - yes_fee_taker_bps + rebate_est_bps - edge_model_cfg.fail_cost_bps;
        let no_edge_net =
            no_edge_gross - no_fee_taker_bps + rebate_est_bps - edge_model_cfg.fail_cost_bps;
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
                              target_l2_size: f64,
                              spread: f64,
                              edge_gross_bps: f64,
                              edge_net_bps: f64,
                              fee_applied: f64,
                              rebate_bps: f64| {
            bucket.push(PredatorCandIn {
                market_id: market_id.clone(),
                timeframe: timeframe.clone(),
                side,
                entry_price,
                spread,
                edge_gross_bps,
                edge_net_bps,
                size: mk_size(entry_price),
                target_l2_size,
                fee_applied,
                rebate_est_bps: rebate_bps,
            });
        };
        let bucket = cand_inputs_by_market.entry(market_id.clone()).or_default();

        let target_l2_size_for_side = |book: &BookTop, side: &OrderSide| -> f64 {
            match side {
                OrderSide::BuyYes => book.ask_size_yes,
                OrderSide::SellYes => book.bid_size_yes,
                OrderSide::BuyNo => book.ask_size_no,
                OrderSide::SellNo => book.bid_size_no,
            }
        };

        let (side, entry_price, edge_gross_bps, edge_net_bps, fee_applied) =
            if no_edge_net > yes_edge_net {
                (
                    OrderSide::BuyNo,
                    no_price,
                    no_edge_gross,
                    no_edge_net,
                    no_fee_taker_bps,
                )
            } else {
                (
                    OrderSide::BuyYes,
                    yes_price,
                    yes_edge_gross,
                    yes_edge_net,
                    yes_fee_taker_bps,
                )
            };
        push_candidate(
            bucket,
            side.clone(),
            entry_price,
            target_l2_size_for_side(&book, &side),
            spread_for_side(&book, &side),
            edge_gross_bps,
            edge_net_bps,
            fee_applied,
            rebate_est_bps,
        );
    }

    let mut fire_plans_by_market: HashMap<String, FirePlan> = HashMap::new();
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
        let evaluate =
            |sn: &mut taker_sniper::TakerSniper, cin: &PredatorCandIn, sig: &DirectionSignal| {
                sn.evaluate(&taker_sniper::EvaluateCtx {
                    market_id: &cin.market_id,
                    symbol,
                    timeframe: cin.timeframe.clone(),
                    direction_signal: sig,
                    entry_price: cin.entry_price,
                    spread: cin.spread,
                    fee_bps: cin.fee_applied,
                    edge_gross_bps: cin.edge_gross_bps,
                    edge_net_bps: cin.edge_net_bps,
                    rebate_est_bps: cin.rebate_est_bps,
                    size: cin.size,
                    target_l2_size: cin.target_l2_size,
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
    }

    if fire_plans_by_market.is_empty() {
        return PredatorExecResult::default();
    }

    let mut locked_plans: Vec<FirePlan> = Vec::new();
    let mut locked_by_tf_usdc: HashMap<String, f64> = HashMap::new();
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
        for (tf, v) in router.locked_by_tf_usdc(now_ms) {
            locked_by_tf_usdc.insert(tf.to_string(), v);
        }
    }
    shared
        .shadow_stats
        .set_predator_router_locked_by_tf_usdc(locked_by_tf_usdc)
        .await;

    if locked_plans.is_empty() {
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
            let direction_signal =
                direction_signal_for_side(&direction_signal, &plan.opportunity.side);
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
                    tick_fast,
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
    tick_fast: &RefTick,
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
            tick_fast,
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
    tick_fast: &RefTick,
    latest_fast_ticks: &std::collections::HashMap<String, RefTick>,
    latest_anchor_ticks: &std::collections::HashMap<String, RefTick>,
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

        let book_top_lag_ms = if tick_fast.recv_ts_local_ns > 0 && book.recv_ts_local_ns > 0 {
            ((book.recv_ts_local_ns - tick_fast.recv_ts_local_ns).max(0) as f64) / 1_000_000.0
        } else {
            0.0
        };
        let settlement_prob_yes = settlement_prob_yes_for_symbol(
            shared,
            symbol,
            sig_entry.signal.fair_yes,
            latest_fast_ticks,
            latest_anchor_ticks,
            now_ms,
        )
        .await;
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
        let taker_fee_bps = calculate_dynamic_taker_fee_bps(&side, entry_price, fee_bps);
        let rebate_est_bps = get_rebate_bps_cached(shared, &market_id, fee_bps).await;
        let edge_gross_bps = edge_gross_bps_for_side(probability.p_settle, &side, entry_price);
        let edge_net_bps =
            edge_gross_bps - taker_fee_bps + rebate_est_bps - edge_model_cfg.fail_cost_bps;
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
            target_l2_size: 0.0, // Used as fallback so target sizing defaults to unbounded Gatling.
            edge_gross_bps,
            edge_net_bps,
            edge_net_usdc,
            fee_bps: taker_fee_bps,
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
            tick_fast,
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
    latest_fast_ticks: &std::collections::HashMap<String, RefTick>,
    latest_anchor_ticks: &std::collections::HashMap<String, RefTick>,
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
        let Some(follower_tick) = latest_fast_ticks.get(&follower) else {
            continue;
        };
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
            follower_tick,
            latest_fast_ticks,
            latest_anchor_ticks,
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
    tick_fast: &RefTick,
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
            .mark_blocked_with_reason_ctx(
                "v52_blocked_timeframe",
                Some(opp.symbol.as_str()),
                Some(timeframe_tag(opp.timeframe.clone())),
            )
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
    let tick_age_ms = freshness_ms(now_ms, tick_fast.recv_ts_ms);
    let book_top_lag_ms =
        if tick_age_ms <= 1_500 && tick_fast.recv_ts_local_ns > 0 && book.recv_ts_local_ns > 0 {
            ((book.recv_ts_local_ns - tick_fast.recv_ts_local_ns).max(0) as f64) / 1_000_000.0
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
            .mark_blocked_with_reason_ctx(
                "open_orders_pressure_precheck",
                Some(opp.symbol.as_str()),
                Some(timeframe_tag(opp.timeframe.clone())),
            )
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
        let reason = format!("risk:{}", decision.reason);
        shared
            .shadow_stats
            .mark_blocked_with_reason_ctx(
                reason.as_str(),
                Some(opp.symbol.as_str()),
                Some(timeframe_tag(opp.timeframe.clone())),
            )
            .await;
        return PredatorExecResult::default();
    }
    if decision.capped_size <= 0.0 {
        shared
            .shadow_stats
            .mark_blocked_with_reason_ctx(
                "risk_capped_zero",
                Some(opp.symbol.as_str()),
                Some(timeframe_tag(opp.timeframe.clone())),
            )
            .await;
        return PredatorExecResult::default();
    }
    intent.size = intent.size.min(decision.capped_size);

    let intended_notional_usdc = (intent.price.max(0.0) * intent.size.max(0.0)).max(0.0);
    if intended_notional_usdc < maker_cfg.min_eval_notional_usdc {
        shared
            .shadow_stats
            .mark_blocked_with_reason_ctx(
                "tiny_notional",
                Some(opp.symbol.as_str()),
                Some(timeframe_tag(opp.timeframe.clone())),
            )
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
            .mark_blocked_with_reason_ctx(
                "edge_below_dynamic_gate",
                Some(opp.symbol.as_str()),
                Some(timeframe_tag(opp.timeframe.clone())),
            )
            .await;
        return PredatorExecResult::default();
    }
    if edge_net_usdc < maker_cfg.min_expected_edge_usdc {
        shared
            .shadow_stats
            .mark_blocked_with_reason_ctx(
                "edge_notional_too_small",
                Some(opp.symbol.as_str()),
                Some(timeframe_tag(opp.timeframe.clone())),
            )
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
            .mark_blocked_with_reason_ctx(
                "rate_budget_global",
                Some(opp.symbol.as_str()),
                Some(timeframe_tag(opp.timeframe.clone())),
            )
            .await;
        return PredatorExecResult::default();
    }
    if !market_bucket.try_take(1.0) {
        shared
            .shadow_stats
            .mark_blocked_with_reason_ctx(
                "rate_budget_market",
                Some(opp.symbol.as_str()),
                Some(timeframe_tag(opp.timeframe.clone())),
            )
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
        prebuilt_auth: None,
    };
    let mut v2_intent = v2_intent;
    if execution.is_live() {
        let is_taker = execution_style == ExecutionStyle::Taker;
        let mut cache_hit = false;
        if is_taker {
            let side_str = match intent.side {
                OrderSide::BuyYes => "buy_yes",
                OrderSide::BuyNo => "buy_no",
                OrderSide::SellYes => "sell_yes",
                OrderSide::SellNo => "sell_no",
            };
            let key = format!("{}:{}", intent.market_id, side_str);
            if let Some(mut cached) = shared.presign_cache.write().await.remove(&key) {
                let age = cached.fetched_at.elapsed();
                // 45s is safely within the 60s time_window
                if age < std::time::Duration::from_secs(45) {
                    v2_intent.price = cached.price;
                    v2_intent.size = cached.size;
                    v2_intent.prebuilt_payload = Some(cached.payload_bytes.clone());

                    let t_sec = chrono::Utc::now().timestamp().to_string();
                    if let Some(hmac) = cached.hmac_signatures.remove(&t_sec) {
                        cached.auth.timestamp_sec = t_sec;
                        cached.auth.hmac_signature = hmac;
                        v2_intent.prebuilt_auth = Some(cached.auth);
                        cache_hit = true;
                        tracing::info!(
                            "🚀 OMEGA-R3 PHASE 2: Cache Hit! Pre-signed EIP712 & HMAC ripped from memory for {} latency bypass.",
                            key
                        );
                    }
                }
            }
        }

        if !cache_hit {
            v2_intent.prebuilt_payload = prebuild_order_payload(&v2_intent);
        }
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
                    .mark_blocked_with_reason_ctx(
                        "exchange_reject_zero_size",
                        Some(opp.symbol.as_str()),
                        Some(timeframe_tag(opp.timeframe.clone())),
                    )
                    .await;
                return out;
            }
            intent.size = accepted_size;
            shared.shadow_stats.mark_executed();
            out.executed = out.executed.saturating_add(1);
            let accepted_notional_usdc = (intent.price.max(0.0) * intent.size.max(0.0)).max(0.0);
            let entry_edge_usdc = (opp.edge_net_bps / 10_000.0) * accepted_notional_usdc;
            if matches!(
                predator_cfg.strategy_engine.engine_mode,
                StrategyEngineMode::RollV1
            ) {
                let tf_cfg = roll_cfg_for_timeframe(predator_cfg, &opp.timeframe).clone();
                let reverse_cfg = predator_cfg.roll_v1.reverse.clone();
                spawn_roll_v1_position_lifecycle(
                    shared.clone(),
                    bus.clone(),
                    execution.clone(),
                    shadow.clone(),
                    ack_v2.order_id.clone(),
                    intent.market_id.clone(),
                    opp.symbol.clone(),
                    opp.timeframe.clone(),
                    intent.side.clone(),
                    intent.price,
                    intent.size,
                    opp.fee_bps,
                    tf_cfg,
                    reverse_cfg,
                    maker_cfg.taker_max_slippage_bps,
                );
            } else {
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
            }

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
                    min_edge_bps: if matches!(
                        predator_cfg.strategy_engine.engine_mode,
                        StrategyEngineMode::RollV1
                    ) {
                        roll_scale_min_edge_for_timeframe(predator_cfg, &opp.timeframe)
                    } else {
                        predator_cfg.taker_sniper.min_edge_net_bps
                    },
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
            let reason = format!("exchange_reject_{reject_code}");
            shared
                .shadow_stats
                .mark_blocked_with_reason_ctx(
                    reason.as_str(),
                    Some(opp.symbol.as_str()),
                    Some(timeframe_tag(opp.timeframe.clone())),
                )
                .await;
            metrics::counter!("execution.place_rejected").increment(1);
            out.stop_firing = true;
        }
        Err(err) => {
            let reason = classify_execution_error_reason(&err);
            shared
                .shadow_stats
                .mark_blocked_with_reason_ctx(
                    reason,
                    Some(opp.symbol.as_str()),
                    Some(timeframe_tag(opp.timeframe.clone())),
                )
                .await;
            tracing::warn!(?err, "predator place_order failed");
            metrics::counter!("execution.place_error").increment(1);
        }
    }

    out
}
