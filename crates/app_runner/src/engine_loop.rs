use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use core_types::{
    new_id, BookTop, ControlCommand, Direction, EdgeAttribution, EngineEvent, ExecutionStyle,
    ExecutionVenue, FairValueModel, InventoryState, MarketHealth, OrderAck, OrderIntentV2,
    OrderSide, OrderTimeInForce, PaperAction, QuoteEval, QuoteIntent, RefTick, ShadowOutcome,
    ShadowShot, Stage, TimeframeClass, ToxicRegime,
};
use execution_clob::ClobExecution;
use exit_manager::{ExitReason, MarketEvalInput};
use fair_value::BasisMrFairValue;
use infra_bus::RingBus;
use market_discovery::{DiscoveryConfig, MarketDiscovery};
use paper_executor::ShadowExecutor;
use portfolio::PortfolioBook;
use tokio::sync::RwLock;

use crate::execution_eval::{
    aggressive_price_for_side, classify_filled_outcome, classify_unfilled_outcome, edge_for_intent,
    evaluate_fillable, evaluate_survival, get_fee_rate_bps_cached, get_rebate_bps_cached,
    is_crossable, pnl_after_horizon,
};
use crate::fusion_engine::{
    compute_coalesce_policy, estimate_feed_latency, fast_tick_allowed_in_fusion_mode,
    insert_latest_ref_tick, is_anchor_ref_source, pick_latest_tick, ref_event_ts_ms, TokenBucket,
};
use crate::paper_runtime::{global_paper_runtime, PaperIntentCtx};
use crate::report_io::{
    append_jsonl, current_jsonl_queue_depth, dataset_path, next_normalized_ingest_seq,
};
use crate::state::CachedPrebuild;
use crate::state::{
    exit_reason_label, EngineShared, SignalCacheEntry, StrategyIngress, StrategyIngressMsg,
};
use crate::stats_utils::{freshness_ms, now_ns, percentile_deque_capped};
use crate::strategy_policy::{
    adaptive_max_spread, bps_to_usdc, build_toxic_features, compute_market_score_from_snapshot,
    cooldown_secs_for_score, estimate_entry_notional_usdc, estimate_queue_fill_proxy,
    evaluate_toxicity, inventory_for_market, is_market_in_top_n, net_markout, roi_bps_from_usdc,
    should_observe_only_symbol,
};
use crate::strategy_runtime::{
    classify_time_phase, evaluate_and_route_roll_v1, evaluate_and_route_v52, stage_for_phase,
    timeframe_total_ms,
};
use crate::toxicity_runtime::update_toxic_state_from_outcome;
use crate::{publish_if_telemetry_subscribers, spawn_detached};
use core_types::PrebuiltAuth;

pub(crate) fn spawn_presign_worker(shared: Arc<EngineShared>) {
    spawn_detached("presign_worker", false, async move {
        // HFT Cache-line predictive pre-signer
        // Every 500ms, pre-build 10 USDC taker bullets for tight markets
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        let client = reqwest::Client::new();
        let gateway = "http://127.0.0.1:9001";
        loop {
            interval.tick().await;

            let markets: Vec<_> = {
                let books = shared.latest_books.read().await;
                books
                    .iter()
                    .filter(|(_, b)| b.ask_yes > 0.0 && b.bid_yes > 0.0)
                    .map(|(k, b)| (k.clone(), b.clone()))
                    .take(5) // Only top 5 to avoid overwhelming gateway
                    .collect()
            };

            for (market_id, book) in markets {
                // Pre-build Buy Yes Taker
                let price = (book.ask_yes * 1.02).clamp(0.01, 0.99); // 2% slippage default
                let req_payload = serde_json::json!({
                    "token_id": book.token_id_yes,
                    "side": "BUY",
                    "price": price,
                    "size": 10.0,
                    "ttl_ms": 30000,
                    "tif": "FAK",
                    "fee_rate_bps": 0.0,
                    "time_window_sec": 60,
                });

                if let Ok(res) = client
                    .post(format!("{}/prebuild_order", gateway))
                    .json(&req_payload)
                    .send()
                    .await
                {
                    if let Ok(json) = res.json::<serde_json::Value>().await {
                        if json["ok"].as_bool().unwrap_or(false) {
                            let payload_str = json["body"].as_str().unwrap_or("");
                            let hmacs = json["hmac_signatures"].as_object().unwrap();
                            let hmac_map: HashMap<String, String> = hmacs
                                .iter()
                                .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
                                .collect();

                            let prebuild = CachedPrebuild {
                                price,
                                size: 10.0,
                                payload_bytes: payload_str.as_bytes().to_vec(),
                                auth: PrebuiltAuth {
                                    api_key: json["api_key"].as_str().unwrap().to_string(),
                                    passphrase: json["api_passphrase"]
                                        .as_str()
                                        .unwrap()
                                        .to_string(),
                                    address: json["address"].as_str().unwrap().to_string(),
                                    timestamp_sec: "".to_string(), // Populated at execution
                                    hmac_signature: "".to_string(), // Populated at execution
                                },
                                hmac_signatures: hmac_map,
                                fetched_at: Instant::now(),
                            };

                            // Insert into cache
                            let key = format!("{}:{}", market_id, "buy_yes");
                            shared.presign_cache.write().await.insert(key, prebuild);
                        }
                    }
                }
            }
        }
    });
}

pub(crate) fn spawn_udp_ghost_receiver(execution: Arc<ClobExecution>, shared: Arc<EngineShared>) {
    spawn_detached("udp_ghost_receiver", true, async move {
        let port = shared.fusion_cfg.read().await.udp_trigger_port;
        let addr = format!("0.0.0.0:{}", port);
        let socket = match tokio::net::UdpSocket::bind(&addr).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("Failed to bind UDP Ghost Receiver on {}: {}", addr, e);
                return;
            }
        };
        tracing::info!("👻 Omega-R3 Gatling Ghost Receiver bound to UDP {}", addr);

        let mut buf = vec![0u8; 1024];
        loop {
            if let Ok((len, _)) = socket.recv_from(&mut buf).await {
                if len < 4 || buf[0] != 0xBB {
                    continue; // Magic byte mismatch
                }
                let action = buf[1];
                let sym_len = buf[2] as usize;
                if len < 3 + sym_len {
                    continue;
                }
                let symbol_bytes = &buf[3..3 + sym_len];
                let Ok(symbol_str) = std::str::from_utf8(symbol_bytes) else {
                    continue;
                };

                let target_side = match action {
                    0x01 => OrderSide::BuyYes,
                    0x02 => OrderSide::SellYes,
                    _ => continue,
                };

                let market_id = {
                    let map = shared.symbol_to_markets.read().await;
                    map.get(symbol_str).and_then(|m| m.first().cloned())
                };
                let Some(mid) = market_id else {
                    continue;
                };

                let book_top = {
                    let books = shared.latest_books.read().await;
                    books.get(&mid).cloned()
                };
                let Some(book) = book_top else {
                    continue;
                };

                let (exec_price, target_l2) = if target_side == OrderSide::BuyYes {
                    (book.ask_yes, book.ask_size_yes)
                } else {
                    (book.bid_yes, book.bid_size_yes)
                };

                if exec_price <= 0.0 || exec_price >= 1.0 || target_l2 <= 0.0 {
                    continue;
                }

                if *shared.draining.read().await {
                    continue; // Operation Silence: Do not accept new ghost gatling orders
                }

                let tf = TimeframeClass::Tf5m;
                let fee_bps =
                    crate::execution_eval::get_fee_rate_bps_cached(&shared, &book.token_id_yes)
                        .await;
                let mut sniper = shared.predator_taker_sniper.write().await;

                // Forge a dummy momentum override signal for execution eval
                let dummy_sig = core_types::DirectionSignal {
                    symbol: symbol_str.to_string(),
                    direction: match target_side {
                        OrderSide::BuyYes | OrderSide::SellNo => core_types::Direction::Up,
                        OrderSide::SellYes | OrderSide::BuyNo => core_types::Direction::Down,
                    },
                    magnitude_pct: 1.0,
                    confidence: 0.99,
                    recommended_tf: tf.clone(),
                    velocity_bps_per_sec: 100.0,
                    acceleration: 0.0,
                    tick_consistency: 3,
                    triple_confirm: true,
                    momentum_spike: true,
                    ts_ns: crate::stats_utils::now_ns(),
                };

                // The edge is mathematically forced to bypass Fee Shield because the Ghost Signal
                // implicitly holds EV confidence. The sniper will just handle chunking.
                let eval_ctx = taker_sniper::EvaluateCtx {
                    market_id: &mid,
                    symbol: symbol_str,
                    timeframe: tf.clone(),
                    direction_signal: &dummy_sig,
                    entry_price: exec_price,
                    spread: book.ask_yes - book.bid_yes,
                    fee_bps,
                    edge_gross_bps: 200.0,
                    edge_net_bps: 200.0,
                    rebate_est_bps: 0.0,
                    size: target_l2,
                    target_l2_size: target_l2,
                    now_ms: chrono::Utc::now().timestamp_millis(),
                };

                let decision = sniper.evaluate(&eval_ctx);
                if let taker_sniper::TakerAction::Fire = decision.action {
                    if let Some(plan) = decision.fire_plan {
                        for chunk in plan.chunks {
                            let intent = core_types::OrderIntentV2 {
                                market_id: mid.clone(),
                                token_id: Some(if target_side == OrderSide::BuyYes {
                                    book.token_id_yes.clone()
                                } else {
                                    book.token_id_no.clone()
                                }),
                                side: target_side.clone(),
                                price: exec_price,
                                size: chunk.size,
                                ttl_ms: 30000,
                                style: core_types::ExecutionStyle::Taker,
                                tif: core_types::OrderTimeInForce::Fak,
                                max_slippage_bps: 100.0,
                                fee_rate_bps: fee_bps,
                                expected_edge_net_bps: 200.0,
                                client_order_id: Some(core_types::new_id()),
                                hold_to_resolution: false,
                                prebuilt_payload: None,
                                prebuilt_auth: None,
                            };
                            let _ = execution.place_order_v2(intent).await;
                            if chunk.send_delay_ms > 0 {
                                tokio::time::sleep(std::time::Duration::from_millis(
                                    chunk.send_delay_ms,
                                ))
                                .await;
                            }
                        }
                        tracing::warn!(
                            "👻 [UDP-FIRE] Ghost Link triggered Gatling burst to market={}",
                            mid
                        );
                    }
                }
            }
        }
    });
}

pub(crate) fn spawn_strategy_engine(
    bus: RingBus<EngineEvent>,
    portfolio: Arc<PortfolioBook>,
    execution: Arc<ClobExecution>,
    shadow: Arc<ShadowExecutor>,
    paused: Arc<RwLock<bool>>,
    shared: Arc<EngineShared>,
    ingress_rx: crossbeam::channel::Receiver<StrategyIngressMsg>,
) {
    spawn_detached("strategy_engine", true, async move {
        // Core Pinning Configuration for Strategy Engine
        let core_id = std::env::var("POLYEDGE_CORE_ENGINE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok());
        if let Some(id) = core_id {
            if let Some(core_ids) = core_affinity::get_core_ids() {
                if id < core_ids.len() {
                    core_affinity::set_for_current(core_ids[id]);
                    tracing::info!("📌 Strategy Engine pinned to CPU Core {}", id);
                } else {
                    tracing::warn!(
                        "⚠️ Invalid POLYEDGE_CORE_ENGINE={}. Available cores: {}",
                        id,
                        core_ids.len()
                    );
                }
            }
        }

        let fair = BasisMrFairValue::new(shared.fair_value_cfg.clone());
        // Separate fast reference ticks (exchange WS) from anchor ticks (Chainlink RTDS).
        // Fast ticks drive stale filtering + fair value evaluation; anchor ticks are tracked for
        // auditing/diagnostics so the trigger stays latency-sensitive.
        let mut latest_fast_ticks: HashMap<String, RefTick> = HashMap::new();
        let mut latest_anchor_ticks: HashMap<String, RefTick> = HashMap::new();
        let mut last_direction_tick_event_ms: HashMap<String, i64> = HashMap::new();
        let mut market_inventory: HashMap<String, InventoryState> = HashMap::new();
        let mut market_rate_budget: HashMap<String, TokenBucket> = HashMap::new();
        let mut untracked_issue_cooldown: HashMap<String, Instant> = HashMap::new();
        let mut book_lag_sample_at: HashMap<String, Instant> = HashMap::new();
        let mut global_rate_budget = TokenBucket::new(
            shared.rate_limit_rps,
            (shared.rate_limit_rps * 2.0).max(1.0),
        );
        let mut control_rx = bus.subscribe();
        let mut last_discovery_refresh = Instant::now() - Duration::from_secs(3600);
        let mut last_symbol_retry_refresh = Instant::now() - Duration::from_secs(3600);
        let mut last_fusion_mode = shared.fusion_cfg.read().await.mode.clone();
        let strategy_max_coalesce = std::env::var("POLYEDGE_STRATEGY_MAX_COALESCE")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(256)
            .clamp(8, 1_024);
        let strategy_coalesce_min = std::env::var("POLYEDGE_STRATEGY_MIN_COALESCE")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(4)
            .clamp(1, 128);
        let strategy_coalesce_budget_us = std::env::var("POLYEDGE_STRATEGY_COALESCE_BUDGET_US")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(120)
            .clamp(20, 2_000);
        let strategy_drop_stale_book_ms = std::env::var("POLYEDGE_STRATEGY_DROP_STALE_BOOK_MS")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .unwrap_or(800.0)
            .clamp(50.0, 5_000.0);
        let strategy_max_decision_backlog_ms = std::env::var("POLYEDGE_MAX_DECISION_BACKLOG_MS")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .unwrap_or(12.0)
            .clamp(0.10, 50.0);
        let price_tape_enabled = std::env::var("POLYEDGE_PRICE_TAPE_ENABLED")
            .ok()
            .map(|v| {
                !matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "0" | "false" | "off" | "no"
                )
            })
            .unwrap_or(true);
        let price_tape_interval_ms = std::env::var("POLYEDGE_PRICE_TAPE_INTERVAL_MS")
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .unwrap_or(250)
            .clamp(20, 5_000);
        let mut stale_book_drops: u64 = 0;
        let mut last_price_tape_ms: HashMap<String, i64> = HashMap::new();
        let symbol_refresh_inflight = Arc::new(AtomicBool::new(false));
        refresh_market_symbol_map(&shared).await;

        loop {
            if last_discovery_refresh.elapsed() >= Duration::from_secs(300) {
                refresh_market_symbol_map(&shared).await;
                last_discovery_refresh = Instant::now();
            }

            // Non-blocking drain of control commands from async bus
            while let Ok(ctrl) = control_rx.try_recv() {
                match ctrl {
                    EngineEvent::Control(ControlCommand::Pause) => {
                        *paused.write().await = true;
                        shared.shadow_stats.set_paused(true);
                    }
                    EngineEvent::Control(ControlCommand::Resume) => {
                        *paused.write().await = false;
                        shared.shadow_stats.set_paused(false);
                    }
                    EngineEvent::Control(ControlCommand::Flatten) => {
                        // Launch flatten async task in background to keep engine spinning
                        let exec_clone = execution.clone();
                        spawn_detached("engine_flatten_cmd", false, async move {
                            let _ = exec_clone.flatten_all().await;
                        });
                    }
                    _ => {}
                }
            }

            // Sync blocking recv on Crossbeam channel
            let ingress_msg = match ingress_rx.recv() {
                Ok(msg) => msg,
                Err(crossbeam::channel::RecvError) => {
                    // Record issue in background to avoid async await here
                    let stats = shared.shadow_stats.clone();
                    tokio::spawn(async move {
                        stats.record_issue("strategy_ingress_closed").await;
                    });
                    break;
                }
            };
            let mut ingress_enqueued_ns = ingress_msg.enqueued_ns;
            let ingress_event = ingress_msg.payload;

            let dispatch_start = Instant::now();
            let current_fusion_mode = shared.fusion_cfg.read().await.mode.clone();
            if current_fusion_mode != last_fusion_mode {
                latest_fast_ticks.retain(|_, tick| {
                    fast_tick_allowed_in_fusion_mode(tick.source.as_str(), &current_fusion_mode)
                });
                last_fusion_mode = current_fusion_mode.clone();
                shared
                    .shadow_stats
                    .record_issue("fusion_mode_switch_fast_cache_reset")
                    .await;
            }

            match ingress_event {
                StrategyIngress::RefTick(tick) => {
                    let parse_us = dispatch_start.elapsed().as_secs_f64() * 1_000_000.0;
                    shared.shadow_stats.push_parse_us(parse_us).await;
                    if !is_anchor_ref_source(tick.source.as_str())
                        && !fast_tick_allowed_in_fusion_mode(
                            tick.source.as_str(),
                            &current_fusion_mode,
                        )
                    {
                        continue;
                    }
                    insert_latest_ref_tick(&mut latest_fast_ticks, &mut latest_anchor_ticks, tick);
                }
                StrategyIngress::BookTop(mut book) => {
                    let parse_us = dispatch_start.elapsed().as_secs_f64() * 1_000_000.0;
                    shared.shadow_stats.push_parse_us(parse_us).await;
                    // Keep only the freshest observable state under burst.
                    let mut coalesced = 0_u64;
                    let coalesce_start = Instant::now();
                    let queue_len = ingress_rx.len();
                    let coalesce_policy = compute_coalesce_policy(
                        queue_len,
                        shared
                            .shadow_stats
                            .book_top_lag_p50_ms_for_symbol_sync(book.market_id.as_str()),
                        strategy_max_coalesce,
                        strategy_coalesce_min,
                        strategy_coalesce_budget_us,
                    );
                    while coalesced < coalesce_policy.max_events as u64 {
                        if coalesced > 0
                            && (coalesce_start.elapsed().as_micros() as u64)
                                >= coalesce_policy.budget_us
                        {
                            break;
                        }
                        match ingress_rx.try_recv() {
                            Ok(StrategyIngressMsg {
                                enqueued_ns,
                                payload: StrategyIngress::BookTop(next_book),
                            }) => {
                                book = next_book;
                                ingress_enqueued_ns = enqueued_ns;
                                coalesced += 1;
                            }
                            Ok(StrategyIngressMsg {
                                payload: StrategyIngress::RefTick(next_tick),
                                ..
                            }) => {
                                let next_is_anchor =
                                    is_anchor_ref_source(next_tick.source.as_str());
                                if !next_is_anchor
                                    && !fast_tick_allowed_in_fusion_mode(
                                        next_tick.source.as_str(),
                                        &current_fusion_mode,
                                    )
                                {
                                    coalesced += 1;
                                    continue;
                                }
                                insert_latest_ref_tick(
                                    &mut latest_fast_ticks,
                                    &mut latest_anchor_ticks,
                                    next_tick,
                                );
                                coalesced += 1;
                            }
                            Err(crossbeam::channel::TryRecvError::Empty) => break,
                            Err(crossbeam::channel::TryRecvError::Disconnected) => {
                                // Background issue logging
                                let stats_clone = shared.shadow_stats.clone();
                                tokio::spawn(async move {
                                    stats_clone
                                        .record_issue("strategy_ingress_disconnected")
                                        .await;
                                });
                                break;
                            }
                        }
                    }
                    if coalesced > 0 {
                        metrics::counter!("runtime.coalesced_events").increment(coalesced);
                    }
                    if book.recv_ts_local_ns > 0 {
                        let book_age_ms =
                            ((now_ns() - book.recv_ts_local_ns).max(0) as f64) / 1_000_000.0;
                        if book_age_ms > strategy_drop_stale_book_ms {
                            stale_book_drops = stale_book_drops.saturating_add(1);
                            metrics::counter!("strategy.stale_book_dropped").increment(1);
                            metrics::histogram!("latency.stale_book_drop_age_ms")
                                .record(book_age_ms);
                            if stale_book_drops.is_multiple_of(128) {
                                shared.shadow_stats.record_issue("stale_book_dropped").await;
                            }
                            continue;
                        }
                    }

                    let backlog_depth = ingress_rx.len() as f64;
                    metrics::histogram!("runtime.event_backlog_depth").record(backlog_depth);
                    let queue_depth = execution.open_orders_count() as f64;
                    metrics::histogram!("runtime.open_order_depth").record(queue_depth);
                    let io_depth = current_jsonl_queue_depth() as f64;
                    shared
                        .shadow_stats
                        .push_depth_sample(backlog_depth, queue_depth, io_depth)
                        .await;
                    metrics::histogram!("runtime.jsonl_queue_depth").record(io_depth);
                    shared
                        .latest_books
                        .write()
                        .await
                        .insert(book.market_id.clone(), book.clone());

                    if let Some(paper) = global_paper_runtime() {
                        let chainlink_settlement_price =
                            if let Some(symbol) = pick_market_symbol(&shared, &book).await {
                                shared.settlement_prices.read().await.get(&symbol).copied()
                            } else {
                                None
                            };
                        paper.on_book(&book, chainlink_settlement_price).await;
                    }
                    let fills = shadow.on_book(&book);
                    shared.shadow_stats.mark_filled(fills.len() as u64);
                    for fill in fills {
                        execution.mark_order_closed_local(&fill.order_id);
                        portfolio.apply_fill(&fill);
                        if let Some(paper) = global_paper_runtime() {
                            paper.on_fill(&fill).await;
                        }
                        publish_if_telemetry_subscribers(&bus, EngineEvent::Fill(fill.clone()));
                        let ingest_seq = next_normalized_ingest_seq();
                        append_jsonl(
                            &dataset_path("normalized", "fills.jsonl"),
                            &serde_json::json!({
                                "ts_ms": Utc::now().timestamp_millis(),
                                "source_seq": Utc::now().timestamp_millis().max(0) as u64,
                                "ingest_seq": ingest_seq,
                                "fill": fill
                            }),
                        );
                    }

                    if *paused.read().await {
                        shared.shadow_stats.record_issue("paused").await;
                        continue;
                    }
                    if shared.shadow_stats.observe_only() {
                        shared.shadow_stats.record_issue("observe_only").await;
                        continue;
                    }

                    let symbol = pick_market_symbol(&shared, &book).await;
                    let Some(symbol) = symbol else {
                        if market_is_tracked(&shared, &book).await {
                            {
                                let mut states = shared.tox_state.write().await;
                                let st = states.entry(book.market_id.clone()).or_default();
                                st.symbol_missing = st.symbol_missing.saturating_add(1);
                            }
                            shared.shadow_stats.record_issue("symbol_missing").await;
                        } else {
                            let now = Instant::now();
                            let should_record = untracked_issue_cooldown
                                .get(&book.market_id)
                                .map(|last| last.elapsed() >= Duration::from_secs(60))
                                .unwrap_or(true);
                            if should_record {
                                shared.shadow_stats.record_issue("market_untracked").await;
                                untracked_issue_cooldown.insert(book.market_id.clone(), now);
                            }
                        }
                        if last_symbol_retry_refresh.elapsed() >= Duration::from_secs(15)
                            && !symbol_refresh_inflight.swap(true, Ordering::AcqRel)
                        {
                            last_symbol_retry_refresh = Instant::now();
                            let shared_refresh = shared.clone();
                            let inflight = symbol_refresh_inflight.clone();
                            spawn_detached("symbol_map_refresh", false, async move {
                                refresh_market_symbol_map(&shared_refresh).await;
                                inflight.store(false, Ordering::Release);
                            });
                        }
                        continue;
                    };
                    if price_tape_enabled {
                        let now_ms = Utc::now().timestamp_millis();
                        let should_emit = last_price_tape_ms
                            .get(&book.market_id)
                            .map(|last_ms| {
                                now_ms.saturating_sub(*last_ms) >= price_tape_interval_ms
                            })
                            .unwrap_or(true);
                        if should_emit {
                            let market_type = shared
                                .market_to_type
                                .read()
                                .await
                                .get(&book.market_id)
                                .cloned()
                                .unwrap_or_else(|| "unknown".to_string());
                            let market_title = shared
                                .market_to_title
                                .read()
                                .await
                                .get(&book.market_id)
                                .cloned()
                                .unwrap_or_default();
                            let timeframe = shared
                                .market_to_timeframe
                                .read()
                                .await
                                .get(&book.market_id)
                                .map(timeframe_class_label)
                                .unwrap_or("unknown");
                            let mid_yes = ((book.bid_yes + book.ask_yes) * 0.5).clamp(0.0, 1.0);
                            let mid_no = ((book.bid_no + book.ask_no) * 0.5).clamp(0.0, 1.0);
                            let ingest_seq = next_normalized_ingest_seq();
                            append_jsonl(
                                &dataset_path("normalized", "market_price_tape.jsonl"),
                                &serde_json::json!({
                                    "ts_ms": now_ms,
                                    "source_seq": now_ms.max(0) as u64,
                                    "ingest_seq": ingest_seq,
                                    "market_id": book.market_id.clone(),
                                    "title": market_title,
                                    "symbol": symbol.clone(),
                                    "market_type": market_type,
                                    "timeframe": timeframe,
                                    "bid_yes": book.bid_yes,
                                    "ask_yes": book.ask_yes,
                                    "bid_no": book.bid_no,
                                    "ask_no": book.ask_no,
                                    "mid_yes": mid_yes,
                                    "mid_no": mid_no,
                                    "ts_exchange_ms": book.ts_ms,
                                }),
                            );
                            last_price_tape_ms.insert(book.market_id.clone(), now_ms);
                        }
                    }
                    let tick_fast_filtered = pick_latest_tick(&latest_fast_ticks, &symbol)
                        .and_then(|tick| {
                            if fast_tick_allowed_in_fusion_mode(
                                tick.source.as_str(),
                                &current_fusion_mode,
                            ) {
                                Some(tick)
                            } else {
                                None
                            }
                        });
                    let Some(tick_fast) = tick_fast_filtered else {
                        shared.shadow_stats.record_issue("tick_missing").await;
                        continue;
                    };
                    let fusion_mode = {
                        let fusion = shared.fusion_cfg.read().await;
                        fusion.mode.clone()
                    };
                    if !fast_tick_allowed_in_fusion_mode(tick_fast.source.as_str(), &fusion_mode) {
                        shared
                            .shadow_stats
                            .record_issue("tick_source_mode_mismatch")
                            .await;
                        continue;
                    }
                    let tick_anchor = pick_latest_tick(&latest_anchor_ticks, &symbol);
                    if let Some(anchor) = tick_anchor {
                        let now_ms = Utc::now().timestamp_millis();
                        let age_ms = now_ms - ref_event_ts_ms(anchor);
                        if age_ms > 5_000 {
                            shared.shadow_stats.record_issue("anchor_stale").await;
                        }
                    } else {
                        shared.shadow_stats.record_issue("anchor_missing").await;
                    }
                    // For latency-sensitive trading, evaluate fair value on the fastest observable
                    // reference tick. The Chainlink anchor is tracked for correctness auditing and
                    // can be used for future calibration, but should not slow down the trigger.
                    let eval_tick = tick_fast;
                    if !is_anchor_ref_source(eval_tick.source.as_str()) {
                        let event_ms = ref_event_ts_ms(eval_tick);
                        let should_update_direction = last_direction_tick_event_ms
                            .get(symbol.as_str())
                            .map(|prev| event_ms > *prev)
                            .unwrap_or(true);
                        if should_update_direction {
                            shared
                                .predator_direction_detector
                                .write()
                                .await
                                .on_tick(eval_tick);
                            last_direction_tick_event_ms.insert(symbol.clone(), event_ms);
                        }
                    }
                    shared.shadow_stats.mark_seen();

                    // Positive value means: our fast reference tick arrived earlier than the
                    // Polymarket book update (i.e. the exploitable lag window).
                    //
                    // Guardrail: if the tick is already old, this is not a meaningful "lag window"
                    // measurement and would inflate p50/p99. We only sample when the tick is fresh.
                    let tick_age_ms =
                        freshness_ms(Utc::now().timestamp_millis(), tick_fast.recv_ts_ms);
                    let stale_ms = tick_age_ms.max(0) as f64;
                    let book_top_lag_ms = if tick_age_ms <= 1_500
                        && tick_fast.recv_ts_local_ns > 0
                        && book.recv_ts_local_ns > 0
                    {
                        ((book.recv_ts_local_ns - tick_fast.recv_ts_local_ns).max(0) as f64)
                            / 1_000_000.0
                    } else {
                        0.0
                    };
                    let should_sample_book_lag = book_lag_sample_at
                        .get(&symbol)
                        .map(|t| t.elapsed() >= Duration::from_millis(200))
                        .unwrap_or(true);
                    if should_sample_book_lag {
                        book_lag_sample_at.insert(symbol.clone(), Instant::now());
                        shared
                            .shadow_stats
                            .push_book_top_lag_ms(&symbol, book_top_lag_ms)
                            .await;
                    }

                    let latency_sample = estimate_feed_latency(tick_fast, &book);
                    let feed_in_ms = latency_sample.feed_in_ms;
                    let stale_tick_filter_ms =
                        shared.strategy_cfg.read().await.stale_tick_filter_ms;
                    shared
                        .shadow_stats
                        .push_latency_sample(
                            feed_in_ms,
                            latency_sample.source_latency_ms,
                            latency_sample.exchange_lag_ms,
                            latency_sample.path_lag_ms,
                            latency_sample.book_latency_ms,
                            latency_sample.local_backlog_ms,
                            latency_sample.ref_decode_ms,
                        )
                        .await;
                    metrics::histogram!("latency.feed_in_ms").record(feed_in_ms);
                    metrics::histogram!("latency.source_latency_ms")
                        .record(latency_sample.source_latency_ms);
                    metrics::histogram!("latency.exchange_lag_ms")
                        .record(latency_sample.exchange_lag_ms);
                    if latency_sample.path_lag_ms.is_finite() && latency_sample.path_lag_ms >= 0.0 {
                        metrics::histogram!("latency.path_lag_ms")
                            .record(latency_sample.path_lag_ms);
                    }
                    metrics::histogram!("latency.book_latency_ms")
                        .record(latency_sample.book_latency_ms);
                    metrics::histogram!("latency.local_backlog_ms")
                        .record(latency_sample.local_backlog_ms);
                    metrics::histogram!("latency.ref_decode_ms")
                        .record(latency_sample.ref_decode_ms);
                    // Stale tick filtering must be based on *local* staleness/age, not exchange
                    // event timestamps (which can be skewed across venues).
                    if stale_ms > stale_tick_filter_ms {
                        shared.shadow_stats.mark_stale_tick_dropped();
                        shared.shadow_stats.record_issue("stale_tick_dropped").await;
                        continue;
                    }
                    if latency_sample.local_backlog_ms > strategy_max_decision_backlog_ms {
                        mark_blocked_for_market(
                            &shared,
                            &book.market_id,
                            &symbol,
                            "decision_backlog_guard",
                        )
                        .await;
                        metrics::counter!("strategy.decision_backlog_guard").increment(1);
                        continue;
                    }
                    let queue_wait_ms = if ingress_enqueued_ns > 0 {
                        ((now_ns() - ingress_enqueued_ns).max(0) as f64) / 1_000_000.0
                    } else {
                        latency_sample.local_backlog_ms
                    };
                    shared
                        .shadow_stats
                        .push_decision_queue_wait_ms(queue_wait_ms)
                        .await;
                    metrics::histogram!("latency.decision_queue_wait_ms").record(queue_wait_ms);
                    let signal_start = Instant::now();
                    let signal = fair.evaluate(eval_tick, &book);
                    if signal.edge_bps_bid > 0.0 || signal.edge_bps_ask > 0.0 {
                        shared.shadow_stats.mark_candidate();
                    }
                    let signal_us = signal_start.elapsed().as_secs_f64() * 1_000_000.0;
                    shared.shadow_stats.push_signal_us(signal_us).await;
                    metrics::histogram!("latency.signal_us").record(signal_us);
                    publish_if_telemetry_subscribers(&bus, EngineEvent::Signal(signal.clone()));
                    shared.latest_signals.insert(
                        book.market_id.clone(),
                        SignalCacheEntry {
                            signal: signal.clone(),
                            ts_ms: Utc::now().timestamp_millis(),
                        },
                    );
                    if should_sample_book_lag && book_top_lag_ms >= 5.0 {
                        // "Orderless" survival probe: measure whether an observed top-of-book
                        // price survives for +Δ ms, independent of order placement.
                        let mid_yes = (book.bid_yes + book.ask_yes) * 0.5;
                        let probe_side = if signal.fair_yes >= mid_yes {
                            OrderSide::BuyYes
                        } else {
                            OrderSide::BuyNo
                        };
                        let probe_px = aggressive_price_for_side(&book, &probe_side);
                        if probe_px > 0.0 {
                            for delay_ms in [5_u64, 10_u64, 25_u64] {
                                spawn_survival_probe_task(
                                    shared.clone(),
                                    book.market_id.clone(),
                                    symbol.clone(),
                                    probe_side.clone(),
                                    probe_px,
                                    delay_ms,
                                );
                            }
                        }
                    }

                    let cfg = shared.strategy_cfg.read().await.clone();
                    let tox_cfg = shared.toxicity_cfg.read().await.clone();
                    let pending_market_exposure =
                        execution.open_order_notional_for_market(&book.market_id);

                    let (
                        tox_features,
                        tox_decision,
                        market_score,
                        pending_exposure,
                        no_quote_rate,
                        symbol_missing_rate,
                        markout_samples,
                        active_by_rank,
                    ) = {
                        // Step A: snapshot (read-lock) the state needed for computation.
                        let (
                            attempted0,
                            no_quote0,
                            symbol_missing0,
                            cooldown_until_ms0,
                            markout_samples0,
                            markout_1s_p50,
                            markout_5s_p50,
                            markout_10s_p50,
                        ) = {
                            let states = shared.tox_state.read().await;
                            if let Some(st) = states.get(&book.market_id) {
                                let samples = st
                                    .markout_1s
                                    .len()
                                    .max(st.markout_5s.len())
                                    .max(st.markout_10s.len());
                                (
                                    st.attempted,
                                    st.no_quote,
                                    st.symbol_missing,
                                    st.cooldown_until_ms,
                                    samples,
                                    percentile_deque_capped(&st.markout_1s, 0.50, 1024)
                                        .unwrap_or(0.0),
                                    percentile_deque_capped(&st.markout_5s, 0.50, 1024)
                                        .unwrap_or(0.0),
                                    percentile_deque_capped(&st.markout_10s, 0.50, 2048)
                                        .unwrap_or(0.0),
                                )
                            } else {
                                (0, 0, 0, 0, 0, 0.0, 0.0, 0.0)
                            }
                        };

                        // Step B: compute without holding a write lock.
                        let attempted1 = attempted0.saturating_add(1).max(1);
                        let tox_features = build_toxic_features(
                            &book,
                            &symbol,
                            stale_ms,
                            signal.fair_yes,
                            attempted1,
                            no_quote0,
                            markout_1s_p50,
                            markout_5s_p50,
                            markout_10s_p50,
                        );
                        let mut tox_decision = evaluate_toxicity(&tox_features, &tox_cfg);
                        let score_scale = (tox_cfg.k_spread / 1.5).clamp(0.25, 4.0);
                        tox_decision.tox_score =
                            (tox_decision.tox_score * score_scale).clamp(0.0, 1.0);
                        tox_decision.regime = if tox_decision.tox_score >= tox_cfg.caution_threshold
                        {
                            ToxicRegime::Danger
                        } else if tox_decision.tox_score >= tox_cfg.safe_threshold {
                            ToxicRegime::Caution
                        } else {
                            ToxicRegime::Safe
                        };

                        let now_ms = Utc::now().timestamp_millis();
                        let mut cooldown_until_ms1 = cooldown_until_ms0;
                        if now_ms < cooldown_until_ms0 {
                            tox_decision.regime = ToxicRegime::Danger;
                            tox_decision
                                .reason_codes
                                .push("cooldown_active".to_string());
                        }
                        // Note: cooldown is based on the pre-warmup regime, matching the old behavior.
                        if matches!(tox_decision.regime, ToxicRegime::Danger) {
                            let cool = cooldown_secs_for_score(tox_decision.tox_score, &tox_cfg);
                            cooldown_until_ms1 =
                                cooldown_until_ms1.max(now_ms + (cool as i64) * 1_000);
                        }
                        let markout_samples = markout_samples0;
                        if markout_samples < 20 {
                            tox_decision.tox_score =
                                tox_decision.tox_score.min(tox_cfg.safe_threshold * 0.8);
                            tox_decision.regime = ToxicRegime::Safe;
                            tox_decision
                                .reason_codes
                                .push("warmup_samples_lt_20".to_string());
                        }

                        let market_score = compute_market_score_from_snapshot(
                            attempted1,
                            no_quote0,
                            symbol_missing0,
                            tox_decision.tox_score,
                            markout_samples,
                            markout_10s_p50,
                        );
                        let no_quote_rate = (no_quote0 as f64 / attempted1 as f64).clamp(0.0, 1.0);
                        let symbol_missing_rate =
                            (symbol_missing0 as f64 / attempted1 as f64).clamp(0.0, 1.0);

                        // Step C: short write-back only.
                        {
                            let mut states = shared.tox_state.write().await;
                            let st = states.entry(book.market_id.clone()).or_default();
                            st.attempted = st.attempted.saturating_add(1);
                            st.symbol = symbol.clone();
                            st.last_tox_score = tox_decision.tox_score;
                            st.last_regime = tox_decision.regime.clone();
                            st.cooldown_until_ms = st.cooldown_until_ms.max(cooldown_until_ms1);
                            st.market_score = market_score;
                        }

                        // Step D: rank decision (read-lock, uses updated market_score).
                        let active_by_rank = {
                            let states = shared.tox_state.read().await;
                            is_market_in_top_n(
                                &states,
                                &book.market_id,
                                tox_cfg.active_top_n_markets,
                            )
                        };

                        (
                            tox_features,
                            tox_decision,
                            market_score,
                            pending_market_exposure,
                            no_quote_rate,
                            symbol_missing_rate,
                            markout_samples,
                            active_by_rank,
                        )
                    };

                    let decision_compute_ms = signal_start.elapsed().as_secs_f64() * 1_000.0;
                    shared
                        .shadow_stats
                        .push_decision_compute_ms(decision_compute_ms)
                        .await;
                    let tick_to_decision_ms =
                        ((now_ns() - tick_fast.recv_ts_local_ns).max(0) as f64) / 1_000_000.0;
                    shared
                        .shadow_stats
                        .push_tick_to_decision_ms(tick_to_decision_ms)
                        .await;

                    let predator_cfg = shared.predator_cfg.read().await.clone();
                    let roll_mode = matches!(
                        predator_cfg.strategy_engine.engine_mode,
                        crate::state::StrategyEngineMode::RollV1
                    );

                    let spread_yes = (book.ask_yes - book.bid_yes).max(0.0);
                    let effective_max_spread = adaptive_max_spread(
                        cfg.max_spread,
                        tox_decision.tox_score,
                        markout_samples,
                    );
                    if !roll_mode
                        && should_observe_only_symbol(
                            &symbol,
                            &cfg,
                            &tox_decision,
                            stale_ms,
                            spread_yes,
                            book_top_lag_ms,
                        )
                    {
                        mark_blocked_for_market(
                            &shared,
                            &book.market_id,
                            &symbol,
                            "symbol_quality_guard",
                        )
                        .await;
                        continue;
                    }
                    let queue_fill_proxy =
                        estimate_queue_fill_proxy(tox_decision.tox_score, spread_yes, stale_ms);

                    if spread_yes > effective_max_spread {
                        {
                            let mut states = shared.tox_state.write().await;
                            let st = states.entry(book.market_id.clone()).or_default();
                            st.no_quote = st.no_quote.saturating_add(1);
                        }
                        shared
                            .shadow_stats
                            .mark_blocked_with_reason_ctx(
                                "no_quote_spread",
                                Some(symbol.as_str()),
                                shared
                                    .market_to_timeframe
                                    .read()
                                    .await
                                    .get(&book.market_id)
                                    .map(timeframe_class_label),
                            )
                            .await;
                        if !roll_mode {
                            continue;
                        }
                    }
                    if signal.confidence <= 0.0 {
                        {
                            let mut states = shared.tox_state.write().await;
                            let st = states.entry(book.market_id.clone()).or_default();
                            st.no_quote = st.no_quote.saturating_add(1);
                        }
                        shared
                            .shadow_stats
                            .mark_blocked_with_reason_ctx(
                                "no_quote_confidence",
                                Some(symbol.as_str()),
                                shared
                                    .market_to_timeframe
                                    .read()
                                    .await
                                    .get(&book.market_id)
                                    .map(timeframe_class_label),
                            )
                            .await;
                        continue;
                    }

                    publish_if_telemetry_subscribers(
                        &bus,
                        EngineEvent::ToxicFeatures(tox_features.clone()),
                    );
                    publish_if_telemetry_subscribers(
                        &bus,
                        EngineEvent::ToxicDecision(tox_decision.clone()),
                    );
                    let ingest_seq_features = next_normalized_ingest_seq();
                    append_jsonl(
                        &dataset_path("normalized", "tox_features.jsonl"),
                        &serde_json::json!({
                            "ts_ms": Utc::now().timestamp_millis(),
                            "source_seq": book.ts_ms.max(0) as u64,
                            "ingest_seq": ingest_seq_features,
                            "features": tox_features
                        }),
                    );
                    let ingest_seq_decisions = next_normalized_ingest_seq();
                    append_jsonl(
                        &dataset_path("normalized", "tox_decisions.jsonl"),
                        &serde_json::json!({
                            "ts_ms": Utc::now().timestamp_millis(),
                            "source_seq": book.ts_ms.max(0) as u64,
                            "ingest_seq": ingest_seq_decisions,
                            "decision": tox_decision
                        }),
                    );
                    let market_health = MarketHealth {
                        market_id: book.market_id.clone(),
                        symbol: symbol.clone(),
                        symbol_missing_rate,
                        no_quote_rate,
                        pending_exposure,
                        queue_fill_proxy,
                        ts_ns: now_ns(),
                    };
                    let ingest_seq_health = next_normalized_ingest_seq();
                    append_jsonl(
                        &dataset_path("normalized", "market_health.jsonl"),
                        &serde_json::json!({
                            "ts_ms": Utc::now().timestamp_millis(),
                            "source_seq": book.ts_ms.max(0) as u64,
                            "ingest_seq": ingest_seq_health,
                            "health": market_health
                        }),
                    );

                    if market_score < tox_cfg.min_market_score {
                        mark_blocked_for_market(
                            &shared,
                            &book.market_id,
                            &symbol,
                            "market_score_low",
                        )
                        .await;
                        if !roll_mode {
                            continue;
                        }
                    }
                    if !active_by_rank {
                        mark_blocked_for_market(
                            &shared,
                            &book.market_id,
                            &symbol,
                            "market_rank_blocked",
                        )
                        .await;
                        if !roll_mode {
                            continue;
                        }
                    }

                    if predator_cfg.enabled {
                        if *shared.draining.read().await {
                            mark_blocked_for_market(
                                &shared,
                                &book.market_id,
                                &symbol,
                                "operation_silence_draining",
                            )
                            .await;
                            continue; // Operation Silence: Reject all new entries, let exits drain.
                        }

                        let now_ms = Utc::now().timestamp_millis();
                        let _ = match predator_cfg.strategy_engine.engine_mode {
                            crate::state::StrategyEngineMode::RollV1 => {
                                evaluate_and_route_roll_v1(
                                    &shared,
                                    &bus,
                                    &portfolio,
                                    &execution,
                                    &shadow,
                                    &mut market_rate_budget,
                                    &mut global_rate_budget,
                                    &predator_cfg,
                                    &symbol,
                                    tick_fast,
                                    &latest_fast_ticks,
                                    &latest_anchor_ticks,
                                    now_ms,
                                )
                                .await
                            }
                            crate::state::StrategyEngineMode::LegacyV52 => {
                                evaluate_and_route_v52(
                                    &shared,
                                    &bus,
                                    &portfolio,
                                    &execution,
                                    &shadow,
                                    &mut market_rate_budget,
                                    &mut global_rate_budget,
                                    &predator_cfg,
                                    &symbol,
                                    tick_fast,
                                    &latest_fast_ticks,
                                    &latest_anchor_ticks,
                                    now_ms,
                                    None,
                                )
                                .await
                            }
                        };
                        continue;
                    }

                    market_inventory.insert(
                        book.market_id.clone(),
                        inventory_for_market(&portfolio, &book.market_id),
                    );
                    shared
                        .shadow_stats
                        .mark_blocked_with_reason_ctx(
                            "predator_c_disabled",
                            Some(symbol.as_str()),
                            shared
                                .market_to_timeframe
                                .read()
                                .await
                                .get(&book.market_id)
                                .map(timeframe_class_label),
                        )
                        .await;
                    continue;
                }
            }
        }
    });
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct PredatorExecResult {
    pub(crate) attempted: u64,
    pub(crate) executed: u64,
    pub(crate) stop_firing: bool,
}

fn opposite_side_for_reentry(side: &OrderSide) -> OrderSide {
    match side {
        OrderSide::BuyYes => OrderSide::BuyNo,
        OrderSide::BuyNo => OrderSide::BuyYes,
        OrderSide::SellYes => OrderSide::SellNo,
        OrderSide::SellNo => OrderSide::SellYes,
    }
}

fn maker_reentry_price_for_side(book: &BookTop, side: &OrderSide) -> f64 {
    match side {
        OrderSide::BuyYes => book.bid_yes,
        OrderSide::BuyNo => book.bid_no,
        OrderSide::SellYes => book.ask_yes,
        OrderSide::SellNo => book.ask_no,
    }
}

pub(crate) fn build_reversal_reentry_order(
    market_id: &str,
    side: &OrderSide,
    book: &BookTop,
    size: f64,
    ttl_ms: u64,
    fee_rate_bps: f64,
) -> Option<OrderIntentV2> {
    let reentry_side = opposite_side_for_reentry(side);
    let price = maker_reentry_price_for_side(book, &reentry_side);
    if !price.is_finite() || price <= 0.0 {
        return None;
    }
    let token_id = match reentry_side {
        OrderSide::BuyYes | OrderSide::SellYes => book.token_id_yes.clone(),
        OrderSide::BuyNo | OrderSide::SellNo => book.token_id_no.clone(),
    };
    Some(OrderIntentV2 {
        market_id: market_id.to_string(),
        token_id: Some(token_id),
        side: reentry_side,
        price,
        size: size.max(0.01),
        ttl_ms: ttl_ms.max(100),
        style: ExecutionStyle::Maker,
        tif: OrderTimeInForce::PostOnly,
        max_slippage_bps: 0.0,
        fee_rate_bps,
        expected_edge_net_bps: 0.0,
        client_order_id: Some(new_id()),
        hold_to_resolution: false,
        prebuilt_payload: None,
        prebuilt_auth: None,
    })
}

async fn reentry_candidate_for_market(
    shared: &Arc<EngineShared>,
    target_market_id: &str,
    side: &OrderSide,
    size: f64,
    maker_ttl_ms: u64,
    maker_min_edge_bps: f64,
    fail_cost_bps: f64,
) -> Option<(OrderIntentV2, BookTop)> {
    let book = shared
        .latest_books
        .read()
        .await
        .get(target_market_id)
        .cloned()?;
    let fee_rate_bps = get_fee_rate_bps_cached(shared, target_market_id).await;
    let mut order = build_reversal_reentry_order(
        target_market_id,
        side,
        &book,
        size,
        maker_ttl_ms,
        fee_rate_bps,
    )?;
    let fair_yes = shared
        .latest_signals
        .get(target_market_id)
        .map(|sig| sig.value().signal.fair_yes.clamp(0.0, 1.0))
        .unwrap_or(0.5);
    let quote_intent = QuoteIntent {
        market_id: target_market_id.to_string(),
        side: order.side.clone(),
        price: order.price,
        size: order.size,
        ttl_ms: order.ttl_ms,
    };
    let rebate_bps = get_rebate_bps_cached(shared, target_market_id, fee_rate_bps).await;
    let edge_net_bps =
        edge_for_intent(fair_yes, &quote_intent) - fee_rate_bps + rebate_bps - fail_cost_bps;
    if edge_net_bps < maker_min_edge_bps {
        return None;
    }
    order.expected_edge_net_bps = edge_net_bps;
    Some((order, book))
}

pub(crate) fn spawn_predator_exit_lifecycle(
    shared: Arc<EngineShared>,
    bus: RingBus<EngineEvent>,
    execution: Arc<ClobExecution>,
    shadow: Arc<ShadowExecutor>,
    position_id: String,
    market_id: String,
    symbol: String,
    side: OrderSide,
    entry_price: f64,
    size: f64,
    maker_ttl_ms: u64,
    entry_fair_yes: f64,
) {
    let mut fill_rx = shared.wss_fill_tx.as_ref().map(|tx| tx.subscribe());
    let entry_pm_mid_yes = entry_price;

    spawn_detached("predator_exit_lifecycle", false, async move {
        let checkpoints = [3_000_u64, 15_000_u64, 60_000_u64, 300_000_u64];
        let mut elapsed_ms = 0_u64;

        'outer: for checkpoint in checkpoints {
            let wait_ms = checkpoint.saturating_sub(elapsed_ms);
            elapsed_ms = checkpoint;

            if wait_ms > 0 {
                let sleep = tokio::time::sleep(Duration::from_millis(wait_ms));
                tokio::pin!(sleep);

                loop {
                    let fill_fut =
                        futures::future::OptionFuture::from(fill_rx.as_mut().map(|rx| rx.recv()));
                    tokio::select! {
                        biased;
                        Some(fill_result) = fill_fut => {
                            if let Ok(fill) = fill_result {
                                if fill.order_id == position_id || fill.market_id == market_id {
                                    tracing::info!(
                                        position_id = %position_id,
                                        market_id = %market_id,
                                        fill_price = fill.price,
                                        fill_size = fill.size,
                                        event_type = %fill.event_type,
                                        "wss_user_feed: fill detected, triggering early exit eval"
                                    );
                                    metrics::counter!("wss.fill_detected").increment(1);
                                    break;
                                }
                            }
                        }
                        _ = &mut sleep => break,
                    }
                }
            }

            let exit_cfg = shared.exit_cfg.read().await.clone();
            if !exit_cfg.enabled {
                continue 'outer;
            }

            let now_ms = Utc::now().timestamp_millis();
            let true_prob = estimate_true_prob_for_side(&shared, &market_id, &side).await;
            let unrealized_pnl_usdc =
                estimate_unrealized_pnl_usdc(&shared, &market_id, &side, entry_price, size)
                    .await
                    .unwrap_or(0.0);
            let time_to_expiry_ms = estimate_time_to_expiry_ms(&shared, &market_id, now_ms).await;

            let pm_mid_yes = {
                let books = shared.latest_books.read().await;
                books
                    .get(&market_id)
                    .map(|b| (b.bid_yes + b.ask_yes) / 2.0)
                    .unwrap_or(0.0)
            };

            let action = {
                let mut manager = shared.predator_exit_manager.write().await;
                manager.evaluate_market(
                    &market_id,
                    MarketEvalInput {
                        now_ms,
                        unrealized_pnl_usdc,
                        true_prob,
                        time_to_expiry_ms,
                        pm_mid_yes,
                        entry_pm_mid_yes,
                        entry_fair_yes,
                    },
                )
            };

            if let Some(action) = action {
                let reason_kind = action.reason.clone();
                let reason = exit_reason_label(reason_kind.clone());
                shared.shadow_stats.record_exit_reason(reason).await;

                // Death Box metrics tracking
                if unrealized_pnl_usdc < 0.0 {
                    match reason_kind {
                        exit_manager::ExitReason::StopLoss
                        | exit_manager::ExitReason::Reversal100ms
                        | exit_manager::ExitReason::Reversal300ms => {
                            shared.shadow_stats.mark_death_box_toxic_reversal();
                        }
                        exit_manager::ExitReason::ForceClose300s
                        | exit_manager::ExitReason::ProbGuard60s => {
                            shared.shadow_stats.mark_death_box_pin_risk();
                        }
                        exit_manager::ExitReason::TakeProfit3s
                        | exit_manager::ExitReason::TakeProfit15s
                        | exit_manager::ExitReason::ConvergenceExit => {
                            shared.shadow_stats.mark_death_box_fee_bleed();
                        }
                    }
                }

                if exit_cfg.flatten_on_trigger {
                    let _ = bus.publish(EngineEvent::Control(ControlCommand::Flatten));
                }
                if matches!(
                    reason_kind,
                    ExitReason::Reversal100ms | ExitReason::Reversal300ms
                ) {
                    let maker_cfg = shared.strategy_cfg.read().await.clone();
                    let fail_cost_bps = shared.edge_model_cfg.read().await.fail_cost_bps;
                    let same_market_first = shared
                        .predator_cfg
                        .read()
                        .await
                        .v52
                        .reversal
                        .same_market_opposite_first;
                    let mut selected: Option<(OrderIntentV2, BookTop, String)> = None;

                    if same_market_first {
                        selected = reentry_candidate_for_market(
                            &shared,
                            &market_id,
                            &side,
                            size,
                            maker_ttl_ms,
                            maker_cfg.min_edge_bps,
                            fail_cost_bps,
                        )
                        .await
                        .map(|(order, book)| (order, book, market_id.clone()));
                        if selected.is_none() {
                            mark_blocked_for_market(
                                &shared,
                                &market_id,
                                symbol.as_str(),
                                "reversal_rebuild_same_market_no_edge",
                            )
                            .await;
                        }
                    }

                    if selected.is_none() {
                        let other_markets = shared
                            .symbol_to_markets
                            .read()
                            .await
                            .get(symbol.as_str())
                            .cloned()
                            .unwrap_or_default();
                        for other_market_id in other_markets {
                            if same_market_first && other_market_id == market_id {
                                continue;
                            }
                            if !same_market_first && other_market_id != market_id {
                                if let Some((order, book)) = reentry_candidate_for_market(
                                    &shared,
                                    other_market_id.as_str(),
                                    &side,
                                    size,
                                    maker_ttl_ms,
                                    maker_cfg.min_edge_bps,
                                    fail_cost_bps,
                                )
                                .await
                                {
                                    selected = Some((order, book, other_market_id));
                                    break;
                                }
                                continue;
                            }
                            if let Some((order, book)) = reentry_candidate_for_market(
                                &shared,
                                other_market_id.as_str(),
                                &side,
                                size,
                                maker_ttl_ms,
                                maker_cfg.min_edge_bps,
                                fail_cost_bps,
                            )
                            .await
                            {
                                selected = Some((order, book, other_market_id));
                                break;
                            }
                        }
                        if selected.is_none() {
                            mark_blocked_for_market(
                                &shared,
                                &market_id,
                                symbol.as_str(),
                                "reversal_rebuild_cross_market_no_edge",
                            )
                            .await;
                        }
                    }

                    if let Some((reentry_order, book, target_market_id)) = selected {
                        let reentry_fee_ref_bps = reentry_order.fee_rate_bps;
                        let quote_intent = QuoteIntent {
                            market_id: reentry_order.market_id.clone(),
                            side: reentry_order.side.clone(),
                            price: reentry_order.price,
                            size: reentry_order.size,
                            ttl_ms: reentry_order.ttl_ms,
                        };
                        publish_if_telemetry_subscribers(
                            &bus,
                            EngineEvent::QuoteIntent(quote_intent.clone()),
                        );
                        match execution.place_order_v2(reentry_order).await {
                            Ok(ack) if ack.accepted => {
                                shared.shadow_stats.mark_executed();
                                let ack_event = OrderAck {
                                    order_id: ack.order_id,
                                    market_id: ack.market_id,
                                    accepted: true,
                                    ts_ms: ack.ts_ms,
                                };
                                publish_if_telemetry_subscribers(
                                    &bus,
                                    EngineEvent::OrderAck(ack_event.clone()),
                                );
                                if let Some(paper) = global_paper_runtime() {
                                    let timeframe_class = shared
                                        .market_to_timeframe
                                        .read()
                                        .await
                                        .get(&target_market_id)
                                        .cloned();
                                    let timeframe = timeframe_class
                                        .as_ref()
                                        .map(ToString::to_string)
                                        .unwrap_or_else(|| "unknown".to_string());
                                    let stage = if let Some(tf) = timeframe_class {
                                        if let Some(total_ms) = timeframe_total_ms(tf.clone()) {
                                            let rem_ms =
                                                (total_ms - now_ms.rem_euclid(total_ms)).max(0);
                                            let rem_ratio =
                                                (rem_ms as f64 / total_ms as f64).clamp(0.0, 1.0);
                                            let predator_cfg =
                                                shared.predator_cfg.read().await.clone();
                                            let phase = classify_time_phase(
                                                rem_ratio,
                                                &predator_cfg.v52.time_phase,
                                            );
                                            stage_for_phase(phase, true)
                                        } else {
                                            Stage::Maturity
                                        }
                                    } else {
                                        Stage::Maturity
                                    };
                                    let entry_price = match quote_intent.side {
                                        OrderSide::BuyYes | OrderSide::SellYes => {
                                            (book.bid_yes + book.ask_yes) * 0.5
                                        }
                                        OrderSide::BuyNo | OrderSide::SellNo => {
                                            (book.bid_no + book.ask_no) * 0.5
                                        }
                                    };
                                    paper
                                        .register_order_intent(
                                            &ack_event,
                                            PaperIntentCtx {
                                                market_id: target_market_id.clone(),
                                                symbol: symbol.clone(),
                                                timeframe,
                                                stage,
                                                direction: match quote_intent.side {
                                                    OrderSide::BuyYes | OrderSide::SellNo => {
                                                        Direction::Up
                                                    }
                                                    OrderSide::BuyNo | OrderSide::SellYes => {
                                                        Direction::Down
                                                    }
                                                },
                                                velocity_bps_per_sec: 0.0,
                                                edge_bps: 0.0,
                                                prob_fast: 0.5,
                                                prob_settle: 0.5,
                                                confidence: 0.0,
                                                action: PaperAction::ReversalExit,
                                                intent: ExecutionStyle::Maker,
                                                requested_size_usdc: (quote_intent.price
                                                    * quote_intent.size)
                                                    .max(0.0),
                                                requested_size_contracts: quote_intent.size,
                                                entry_price,
                                            },
                                        )
                                        .await;
                                }
                                shadow.register_order(
                                    &ack_event,
                                    quote_intent.clone(),
                                    ExecutionStyle::Maker,
                                    ((book.bid_yes + book.ask_yes) * 0.5).max(0.0),
                                    -get_rebate_bps_cached(
                                        &shared,
                                        &target_market_id,
                                        reentry_fee_ref_bps,
                                    )
                                    .await
                                    .max(0.0),
                                );
                            }
                            Ok(_) => {}
                            Err(err) => {
                                tracing::warn!(
                                    market_id = %target_market_id,
                                    symbol = %symbol,
                                    error = %err,
                                    "reversal maker re-entry failed"
                                );
                            }
                        }
                    }
                }
                tracing::info!(
                    position_id = %position_id,
                    market_id = %market_id,
                    symbol = %symbol,
                    reason,
                    "predator exit lifecycle triggered"
                );
                break 'outer;
            }
        }

        // Ensure stale lifecycle entries don't leak forever if no exit action is triggered.
        if elapsed_ms >= 300_000 {
            let _ = shared
                .predator_exit_manager
                .write()
                .await
                .close(&position_id);
        }
    });
}

async fn estimate_true_prob_for_side(
    shared: &Arc<EngineShared>,
    market_id: &str,
    side: &OrderSide,
) -> f64 {
    let p_yes = shared
        .latest_signals
        .get(market_id)
        .map(|sig| sig.value().signal.fair_yes.clamp(0.0, 1.0))
        .unwrap_or(0.5);
    match side {
        OrderSide::BuyYes | OrderSide::SellNo => p_yes,
        OrderSide::BuyNo | OrderSide::SellYes => 1.0 - p_yes,
    }
}

pub(crate) async fn estimate_time_to_expiry_ms(
    shared: &Arc<EngineShared>,
    market_id: &str,
    now_ms: i64,
) -> i64 {
    let timeframe = shared
        .market_to_timeframe
        .read()
        .await
        .get(market_id)
        .cloned();
    let frame_ms = match timeframe {
        Some(TimeframeClass::Tf5m) => 5 * 60 * 1_000,
        Some(TimeframeClass::Tf15m) => 15 * 60 * 1_000,
        Some(TimeframeClass::Tf1h) => 60 * 60 * 1_000,
        Some(TimeframeClass::Tf1d) => 24 * 60 * 60 * 1_000,
        None => return i64::MAX,
    } as i64;
    let rem = frame_ms - now_ms.rem_euclid(frame_ms);
    rem.max(0)
}

async fn estimate_unrealized_pnl_usdc(
    shared: &Arc<EngineShared>,
    market_id: &str,
    side: &OrderSide,
    entry_price: f64,
    size: f64,
) -> Option<f64> {
    let book = shared.latest_books.read().await.get(market_id).cloned()?;
    let mark = match side {
        OrderSide::BuyYes | OrderSide::SellYes => book.bid_yes.max(0.0),
        OrderSide::BuyNo | OrderSide::SellNo => book.bid_no.max(0.0),
    };
    let dir = match side {
        OrderSide::BuyYes | OrderSide::BuyNo => 1.0,
        OrderSide::SellYes | OrderSide::SellNo => -1.0,
    };
    Some((mark - entry_price) * size * dir)
}

pub(crate) fn spawn_shadow_outcome_task(
    shared: Arc<EngineShared>,
    bus: RingBus<EngineEvent>,
    shot: ShadowShot,
) {
    spawn_detached("shadow_outcome", false, async move {
        tokio::time::sleep(Duration::from_millis(shot.delay_ms)).await;
        if !shared.shadow_stats.is_current_window_ts_ns(shot.t0_ns) {
            metrics::counter!("shadow.outcome_window_mismatch").increment(1);
            return;
        }

        let book = shared
            .latest_books
            .read()
            .await
            .get(&shot.market_id)
            .cloned();
        let latency_ms = ((now_ns() - shot.t0_ns).max(0) as f64) / 1_000_000.0;
        shared.shadow_stats.push_shadow_fill_ms(latency_ms).await;
        metrics::histogram!("latency.shadow_fill_ms").record(latency_ms);

        let (
            survived,
            fillable,
            slippage_bps,
            queue_fill_prob,
            attribution,
            pnl_1s_bps,
            pnl_5s_bps,
            pnl_10s_bps,
        ) = if let Some(book) = book {
            let survived = evaluate_survival(&shot, &book);
            let (fillable, slippage_bps, queue_fill_prob) =
                evaluate_fillable(&shot, &book, latency_ms);
            if fillable {
                let p1 = pnl_after_horizon(&shared, &shot, Duration::from_secs(1)).await;
                let p5 = pnl_after_horizon(&shared, &shot, Duration::from_secs(5)).await;
                let p10 = pnl_after_horizon(&shared, &shot, Duration::from_secs(10)).await;
                let attribution = classify_filled_outcome(shot.edge_net_bps, p10, slippage_bps);
                (
                    survived,
                    true,
                    slippage_bps,
                    queue_fill_prob,
                    attribution,
                    p1,
                    p5,
                    p10,
                )
            } else {
                let attribution = classify_unfilled_outcome(
                    &book,
                    latency_ms,
                    shot.delay_ms,
                    survived,
                    queue_fill_prob,
                );
                (
                    survived,
                    false,
                    slippage_bps,
                    queue_fill_prob,
                    attribution,
                    None,
                    None,
                    None,
                )
            }
        } else {
            (
                false,
                false,
                None,
                0.0,
                EdgeAttribution::SignalLag,
                None,
                None,
                None,
            )
        };
        let net_markout_1s_bps = net_markout(pnl_1s_bps, &shot);
        let net_markout_5s_bps = net_markout(pnl_5s_bps, &shot);
        let net_markout_10s_bps = net_markout(pnl_10s_bps, &shot);
        let entry_notional_usdc = estimate_entry_notional_usdc(&shot);
        let net_markout_10s_usdc = bps_to_usdc(net_markout_10s_bps, entry_notional_usdc);
        let roi_notional_10s_bps = roi_bps_from_usdc(net_markout_10s_usdc, entry_notional_usdc);

        let outcome = ShadowOutcome {
            shot_id: shot.shot_id.clone(),
            market_id: shot.market_id.clone(),
            symbol: shot.symbol.clone(),
            side: shot.side.clone(),
            delay_ms: shot.delay_ms,
            survived,
            fillable,
            execution_style: shot.execution_style.clone(),
            slippage_bps,
            pnl_1s_bps,
            pnl_5s_bps,
            pnl_10s_bps,
            net_markout_1s_bps,
            net_markout_5s_bps,
            net_markout_10s_bps,
            entry_notional_usdc,
            net_markout_10s_usdc,
            roi_notional_10s_bps,
            queue_fill_prob,
            is_stale_tick: false,
            is_outlier: false,
            robust_weight: 1.0,
            attribution,
            ts_ns: now_ns(),
        };
        if !shared.shadow_stats.is_current_window_ts_ns(outcome.ts_ns) {
            metrics::counter!("shadow.outcome_window_mismatch").increment(1);
            return;
        }
        shared.shadow_stats.push_outcome(outcome.clone()).await;
        if shot.delay_ms == 10 {
            let exit_cfg = shared.exit_cfg.read().await.clone();
            if exit_cfg.enabled {
                let true_prob = shared
                    .latest_signals
                    .get(&shot.market_id)
                    .map(|sig| {
                        let p_yes = sig.value().signal.fair_yes.clamp(0.0, 1.0);
                        match shot.side {
                            OrderSide::BuyYes | OrderSide::SellNo => p_yes,
                            OrderSide::BuyNo | OrderSide::SellYes => 1.0 - p_yes,
                        }
                    })
                    .unwrap_or(0.5);
                let action = {
                    let mut manager = shared.predator_exit_manager.write().await;
                    manager.evaluate_market(
                        &shot.market_id,
                        MarketEvalInput {
                            now_ms: Utc::now().timestamp_millis(),
                            unrealized_pnl_usdc: outcome.net_markout_10s_usdc.unwrap_or(0.0),
                            true_prob,
                            time_to_expiry_ms: i64::MAX,
                            pm_mid_yes: 0.0,
                            entry_pm_mid_yes: 0.0,
                            entry_fair_yes: 0.0,
                        },
                    )
                };
                if let Some(action) = action {
                    let reason = exit_reason_label(action.reason);
                    shared.shadow_stats.record_exit_reason(reason).await;
                    if exit_cfg.flatten_on_trigger {
                        let _ = bus.publish(EngineEvent::Control(ControlCommand::Flatten));
                    }
                }
            }
        }
        update_toxic_state_from_outcome(&shared, &outcome).await;
        if shot.delay_ms == 10 {
            if let Some(pnl_usdc) = outcome.net_markout_10s_usdc {
                let compounder_enabled = shared.predator_cfg.read().await.compounder.enabled;
                if compounder_enabled {
                    let (update, halt) = {
                        let mut c = shared.predator_compounder.write().await;
                        let update = c.on_markout(pnl_usdc);
                        (update, c.halted())
                    };
                    shared
                        .shadow_stats
                        .set_predator_capital(update.clone(), halt)
                        .await;
                    publish_if_telemetry_subscribers(&bus, EngineEvent::CapitalUpdate(update));

                    if halt {
                        let live_armed = std::env::var("POLYEDGE_LIVE_ARMED")
                            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
                            .unwrap_or(false);
                        if live_armed {
                            shared.shadow_stats.record_issue("capital_halt").await;
                            let _ = bus.publish(EngineEvent::Control(ControlCommand::Pause));
                            let _ = bus.publish(EngineEvent::Control(ControlCommand::Flatten));
                        }
                    }
                }
            }
            let eval = QuoteEval {
                market_id: shot.market_id.clone(),
                symbol: shot.symbol.clone(),
                survival_10ms: if outcome.survived { 1.0 } else { 0.0 },
                maker_markout_10s_bps: outcome.net_markout_10s_bps.unwrap_or(0.0),
                adverse_flag: outcome.net_markout_10s_bps.unwrap_or(0.0) < 0.0,
                ts_ns: now_ns(),
            };
            let ingest_seq = next_normalized_ingest_seq();
            append_jsonl(
                &dataset_path("normalized", "quote_eval.jsonl"),
                &serde_json::json!({
                    "ts_ms": Utc::now().timestamp_millis(),
                    "source_seq": shot.t0_ns.max(0) as u64,
                    "ingest_seq": ingest_seq,
                    "eval": eval
                }),
            );
            publish_if_telemetry_subscribers(&bus, EngineEvent::QuoteEval(eval));
        }
        publish_if_telemetry_subscribers(&bus, EngineEvent::ShadowOutcome(outcome));
    });
}

fn spawn_survival_probe_task(
    shared: Arc<EngineShared>,
    market_id: String,
    symbol: String,
    side: OrderSide,
    probe_px: f64,
    delay_ms: u64,
) {
    spawn_detached("survival_probe", false, async move {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        let book = shared.latest_books.read().await.get(&market_id).cloned();
        let survived = match book {
            Some(ref b) => is_crossable(&side, probe_px, b),
            None => false,
        };
        shared
            .shadow_stats
            .record_survival_probe(&symbol, delay_ms, survived)
            .await;
    });
}

async fn refresh_market_symbol_map(shared: &EngineShared) {
    let discovery = MarketDiscovery::new(DiscoveryConfig {
        symbols: (*shared.universe_symbols).clone(),
        market_types: (*shared.universe_market_types).clone(),
        timeframes: (*shared.universe_timeframes).clone(),
        ..DiscoveryConfig::default()
    });
    match discovery.discover().await {
        Ok(markets) => {
            let mut market_map = HashMap::new();
            let mut market_title_map = HashMap::new();
            let mut market_type_map = HashMap::new();
            let mut token_map = HashMap::new();
            let mut timeframe_map = HashMap::new();
            let mut market_end_ts_map = HashMap::new();
            let mut symbol_to_markets = HashMap::<String, Vec<String>>::new();
            for m in markets {
                market_map.insert(m.market_id.clone(), m.symbol.clone());
                market_title_map.insert(m.market_id.clone(), m.question.clone());
                market_type_map.insert(
                    m.market_id.clone(),
                    m.market_type
                        .as_deref()
                        .map(|v| v.to_ascii_lowercase())
                        .unwrap_or_else(|| "unknown".to_string()),
                );
                symbol_to_markets
                    .entry(m.symbol.clone())
                    .or_default()
                    .push(m.market_id.clone());
                if let Some(tf) = m.timeframe.as_deref().and_then(parse_timeframe_class) {
                    timeframe_map.insert(m.market_id.clone(), tf);
                }
                if let Some(end_ms) = parse_end_date_ms(m.end_date.as_deref()) {
                    market_end_ts_map.insert(m.market_id.clone(), end_ms);
                }
                if let Some(t) = m.token_id_yes {
                    token_map.insert(t, m.symbol.clone());
                }
                if let Some(t) = m.token_id_no {
                    token_map.insert(t, m.symbol.clone());
                }
            }
            for v in symbol_to_markets.values_mut() {
                v.sort();
                v.dedup();
            }
            {
                let mut map = shared.market_to_symbol.write().await;
                *map = market_map;
            }
            {
                let mut map = shared.token_to_symbol.write().await;
                *map = token_map;
            }
            {
                let mut map = shared.market_to_title.write().await;
                *map = market_title_map;
            }
            {
                let mut map = shared.market_to_type.write().await;
                *map = market_type_map;
            }
            {
                let mut map = shared.market_to_timeframe.write().await;
                *map = timeframe_map;
            }
            {
                let mut map = shared.market_to_end_ts_ms.write().await;
                *map = market_end_ts_map;
            }
            {
                let mut map = shared.symbol_to_markets.write().await;
                *map = symbol_to_markets;
            }
        }
        Err(err) => {
            tracing::warn!(?err, "market discovery refresh failed");
        }
    }
}

async fn mark_blocked_for_market(
    shared: &EngineShared,
    market_id: &str,
    symbol: &str,
    reason: &str,
) {
    let timeframe = shared
        .market_to_timeframe
        .read()
        .await
        .get(market_id)
        .map(timeframe_class_label);
    shared
        .shadow_stats
        .mark_blocked_with_reason_ctx(reason, Some(symbol), timeframe)
        .await;
}

fn timeframe_class_label(tf: &TimeframeClass) -> &'static str {
    match tf {
        TimeframeClass::Tf5m => "5m",
        TimeframeClass::Tf15m => "15m",
        TimeframeClass::Tf1h => "1h",
        TimeframeClass::Tf1d => "1d",
    }
}

fn parse_timeframe_class(tf: &str) -> Option<TimeframeClass> {
    match tf.trim().to_ascii_lowercase().as_str() {
        "5m" | "tf5m" => Some(TimeframeClass::Tf5m),
        "15m" | "tf15m" => Some(TimeframeClass::Tf15m),
        "1h" | "tf1h" => Some(TimeframeClass::Tf1h),
        "1d" | "tf1d" => Some(TimeframeClass::Tf1d),
        _ => None,
    }
}

fn parse_end_date_ms(raw: Option<&str>) -> Option<i64> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    chrono::DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|dt| dt.with_timezone(&chrono::Utc).timestamp_millis())
}

async fn pick_market_symbol(shared: &EngineShared, book: &BookTop) -> Option<String> {
    if let Some(v) = shared
        .market_to_symbol
        .read()
        .await
        .get(&book.market_id)
        .cloned()
    {
        return Some(v);
    }
    let token_map = shared.token_to_symbol.read().await;
    token_map
        .get(&book.token_id_yes)
        .cloned()
        .or_else(|| token_map.get(&book.token_id_no).cloned())
}

async fn market_is_tracked(shared: &EngineShared, book: &BookTop) -> bool {
    if shared
        .market_to_symbol
        .read()
        .await
        .contains_key(&book.market_id)
    {
        return true;
    }
    let token_map = shared.token_to_symbol.read().await;
    token_map.contains_key(&book.token_id_yes) || token_map.contains_key(&book.token_id_no)
}
