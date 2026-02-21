use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::{Duration, Instant};

use chrono::Utc;
use core_types::{
    new_id, BookTop, ControlCommand, Direction, DirectionSignal, EngineEvent, ExecutionStyle,
    ExecutionVenue, OrderIntentV2, OrderSide, OrderTimeInForce, RefTick, Regime, Signal,
    SourceHealth, Stage, TimeframeClass, ToxicDecision, ToxicRegime,
};
use dashmap::DashMap;
use direction_detector::DirectionDetector;
use execution_clob::{wss_user_feed::WssFillEvent, ClobExecution, ExecutionMode};
use exit_manager::{ExitManager, PositionLifecycle};
use fair_value::BasisMrConfig;
use infra_bus::RingBus;
use paper_executor::ShadowExecutor;
use probability_engine::ProbabilityEngine;
use reqwest::Client;
use risk_engine::{DefaultRiskManager, RiskLimits};
use settlement_compounder::SettlementCompounder;
use strategy_maker::MakerConfig;
use taker_sniper::TakerSniper;
use timeframe_router::TimeframeRouter;
use tokio::sync::RwLock;
use tokio::time::{sleep, timeout};

use crate::config_loader::parse_toml_array_for_key;
use crate::engine_core::normalize_reject_code;
use crate::engine_loop::build_reversal_reentry_order;
use crate::execution_eval::prebuild_order_payload;
use crate::feed_runtime::{
    blend_settlement_probability, build_source_health_snapshot, SourceRuntimeStats,
};
use crate::fusion_engine::{
    compute_coalesce_policy, estimate_feed_latency, is_ref_tick_duplicate,
    reset_path_lag_calib_for_tests, should_replace_ref_tick, upsert_latest_tick_slot_local,
};
use crate::spawn_predator_exit_lifecycle;
use crate::state::{
    to_exit_manager_config, EdgeModelConfig, EngineShared, ExitConfig, FusionConfig,
    MarketToxicState, PredatorCConfig, PredatorRegimeConfig, SettlementConfig, ShadowStats,
    SignalCacheEntry, SourceHealthConfig, ToxicityConfig, V52DualArbConfig, V52ExecutionConfig,
    V52TimePhaseConfig,
};
use crate::stats_utils::{
    now_ns, percentile, policy_block_ratio, quote_block_ratio, robust_filter_iqr,
};
use crate::strategy_policy::{is_market_in_top_n, should_observe_only_symbol};
use crate::strategy_runtime::{
    allow_dual_side_arb, classify_predator_regime, classify_time_phase,
    should_force_taker_fallback, stage_for_phase, time_phase_size_scale, timeframe_total_ms,
    TimePhase,
};

fn test_engine_shared_with_wss() -> (
    Arc<EngineShared>,
    Arc<tokio::sync::broadcast::Sender<WssFillEvent>>,
) {
    let strategy_cfg = Arc::new(RwLock::new(Arc::new(MakerConfig::default())));
    let settlement_cfg = Arc::new(RwLock::new(SettlementConfig::default()));
    let source_health_cfg = Arc::new(RwLock::new(SourceHealthConfig::default()));
    let fusion_cfg = Arc::new(RwLock::new(FusionConfig::default()));
    let edge_model_cfg = Arc::new(RwLock::new(EdgeModelConfig::default()));
    let exit_cfg_value = ExitConfig {
        enabled: true,
        flatten_on_trigger: true,
        ..ExitConfig::default()
    };
    let exit_cfg = Arc::new(RwLock::new(exit_cfg_value.clone()));
    let fair_value_cfg = Arc::new(StdRwLock::new(BasisMrConfig::default()));
    let toxicity_cfg = Arc::new(RwLock::new(Arc::new(ToxicityConfig::default())));
    let risk_limits = Arc::new(StdRwLock::new(RiskLimits::default()));
    let risk_manager = Arc::new(DefaultRiskManager::new(risk_limits.clone()));
    let predator_cfg_value = PredatorCConfig::default();
    let predator_cfg = Arc::new(RwLock::new(predator_cfg_value.clone()));
    let predator_direction_detector = Arc::new(RwLock::new(DirectionDetector::new(
        predator_cfg_value.direction_detector.clone(),
    )));
    let predator_probability_engine = Arc::new(RwLock::new(ProbabilityEngine::new(
        predator_cfg_value.probability_engine.clone(),
    )));
    let predator_taker_sniper = Arc::new(RwLock::new(TakerSniper::new(
        predator_cfg_value.taker_sniper.clone(),
    )));
    let predator_router = Arc::new(RwLock::new(TimeframeRouter::new(
        predator_cfg_value.router.clone(),
    )));
    let predator_compounder = Arc::new(RwLock::new(SettlementCompounder::new(
        predator_cfg_value.compounder.clone(),
    )));
    let predator_exit_manager = Arc::new(RwLock::new(ExitManager::new(to_exit_manager_config(
        &exit_cfg_value,
    ))));
    let (wss_tx, _rx) = tokio::sync::broadcast::channel(64);
    let wss_tx = Arc::new(wss_tx);

    let shared = Arc::new(EngineShared {
        draining: Arc::new(RwLock::new(false)),
        latest_books: Arc::new(RwLock::new(HashMap::new())),
        latest_signals: Arc::new(DashMap::new()),
        market_to_symbol: Arc::new(RwLock::new(HashMap::new())),
        market_to_title: Arc::new(RwLock::new(HashMap::new())),
        market_to_type: Arc::new(RwLock::new(HashMap::new())),
        token_to_symbol: Arc::new(RwLock::new(HashMap::new())),
        market_to_timeframe: Arc::new(RwLock::new(HashMap::new())),
        symbol_to_markets: Arc::new(RwLock::new(HashMap::new())),
        fee_cache: Arc::new(RwLock::new(HashMap::new())),
        fee_refresh_inflight: Arc::new(RwLock::new(HashMap::new())),
        scoring_cache: Arc::new(RwLock::new(HashMap::new())),
        http: Client::new(),
        clob_endpoint: "http://127.0.0.1:0".to_string(),
        strategy_cfg,
        settlement_cfg,
        source_health_cfg,
        source_health_latest: Arc::new(RwLock::new(HashMap::new())),
        settlement_prices: Arc::new(RwLock::new(HashMap::new())),
        fusion_cfg,
        edge_model_cfg,
        exit_cfg,
        fair_value_cfg,
        toxicity_cfg,
        risk_manager,
        risk_limits,
        universe_symbols: Arc::new(vec!["BTCUSDT".to_string()]),
        universe_market_types: Arc::new(vec!["updown".to_string()]),
        universe_timeframes: Arc::new(vec!["5m".to_string(), "15m".to_string()]),
        rate_limit_rps: 20.0,
        tox_state: Arc::new(RwLock::new(HashMap::new())),
        shadow_stats: Arc::new(ShadowStats::new()),
        predator_cfg,
        predator_direction_detector,
        predator_latest_direction: Arc::new(RwLock::new(HashMap::new())),
        predator_latest_probability: Arc::new(RwLock::new(HashMap::new())),
        predator_probability_engine,
        predator_taker_sniper,
        predator_d_last_fire_ms: Arc::new(RwLock::new(HashMap::new())),
        predator_router,
        predator_compounder,
        predator_exit_manager,
        wss_fill_tx: Some(wss_tx.clone()),
        presign_cache: Arc::new(RwLock::new(HashMap::new())),
    });

    (shared, wss_tx)
}

#[test]
fn robust_filter_marks_outliers() {
    let values = vec![1.0, 1.1, 1.2, 1.3, 99.0];
    let (filtered, outlier_ratio) = robust_filter_iqr(&values);
    assert!(filtered.len() < values.len());
    assert!(outlier_ratio > 0.0);
}

#[test]
fn robust_filter_small_sample_passthrough() {
    let values = vec![1.0, 2.0, 3.0, 4.0];
    let (filtered, outlier_ratio) = robust_filter_iqr(&values);
    assert_eq!(filtered, values);
    assert_eq!(outlier_ratio, 0.0);
}

#[test]
fn percentile_basic_behavior() {
    let values = vec![1.0, 5.0, 3.0, 2.0, 4.0];
    assert_eq!(percentile(&values, 0.50), Some(3.0));
    assert_eq!(percentile(&values, 0.0), Some(1.0));
    assert_eq!(percentile(&values, 1.0), Some(5.0));
}

#[test]
fn ref_tick_dedupe_uses_relative_bps_and_window() {
    let cfg = FusionConfig {
        dedupe_window_ms: 120,
        dedupe_price_bps: 0.2,
        ..FusionConfig::default()
    };
    assert!(is_ref_tick_duplicate(1_000, 100.001, 980, 100.0, &cfg));
    assert!(!is_ref_tick_duplicate(1_500, 100.001, 980, 100.0, &cfg));
    assert!(!is_ref_tick_duplicate(1_000, 100.005, 980, 100.0, &cfg));
}

#[test]
fn fusion_default_stays_safe_baseline() {
    let cfg = FusionConfig::default();
    assert_eq!(cfg.mode, "hyper_mesh");
    assert!(cfg.enable_udp);
    assert!(cfg.udp_share_cap > 0.0 && cfg.udp_share_cap <= 1.0);
    assert!(cfg.jitter_threshold_ms >= 1.0);
}

#[test]
fn reversal_taker_flatten_then_opposite_maker_reentry_order_shape() {
    let book = BookTop {
        market_id: "m".to_string(),
        token_id_yes: "yes".to_string(),
        token_id_no: "no".to_string(),
        bid_yes: 0.48,
        ask_yes: 0.52,
        bid_no: 0.47,
        ask_no: 0.53,
        bid_size_yes: 0.0,
        ask_size_yes: 0.0,
        bid_size_no: 0.0,
        ask_size_no: 0.0,
        ts_ms: 1,
        recv_ts_local_ns: 1_000_000,
    };
    let order = build_reversal_reentry_order("m", &OrderSide::BuyYes, &book, 2.0, 400, 2.5)
        .expect("reentry order");
    assert_eq!(order.side, OrderSide::BuyNo);
    assert_eq!(order.token_id.as_deref(), Some("no"));
    assert_eq!(order.price, book.bid_no);
    assert_eq!(order.style, ExecutionStyle::Maker);
    assert_eq!(order.tif, OrderTimeInForce::PostOnly);
    assert!(order.size > 0.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reversal_taker_to_opposite_maker_reentry_end_to_end_records_report() {
    let (shared, wss_tx) = test_engine_shared_with_wss();
    let bus = RingBus::new(256);
    let execution = Arc::new(ClobExecution::new(
        ExecutionMode::Paper,
        "http://127.0.0.1:0".to_string(),
    ));
    let shadow = Arc::new(ShadowExecutor::default());
    let market_id = "m_reversal".to_string();
    let symbol = "BTCUSDT".to_string();
    let now_ms = Utc::now().timestamp_millis();

    shared.latest_books.write().await.insert(
        market_id.clone(),
        BookTop {
            market_id: market_id.clone(),
            token_id_yes: "yes_token".to_string(),
            token_id_no: "no_token".to_string(),
            bid_yes: 0.45,
            ask_yes: 0.46,
            bid_no: 0.54,
            ask_no: 0.55,
            bid_size_yes: 0.0,
            ask_size_yes: 0.0,
            bid_size_no: 0.0,
            ask_size_no: 0.0,
            ts_ms: now_ms,
            recv_ts_local_ns: now_ns(),
        },
    );
    shared.latest_signals.insert(
        market_id.clone(),
        SignalCacheEntry {
            signal: Signal {
                market_id: market_id.clone(),
                fair_yes: 0.20,
                edge_bps_bid: 12.0,
                edge_bps_ask: -12.0,
                confidence: 0.92,
            },
            ts_ms: now_ms,
        },
    );

    let seen_direction_signal = Arc::new(AtomicBool::new(false));
    let seen_flatten = Arc::new(AtomicBool::new(false));
    let mut rx = bus.subscribe();
    let execution_for_listener = execution.clone();
    let seen_direction_signal_l = seen_direction_signal.clone();
    let seen_flatten_l = seen_flatten.clone();
    let listener = tokio::spawn(async move {
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            match timeout(Duration::from_millis(250), rx.recv()).await {
                Ok(Ok(EngineEvent::DirectionSignal(_))) => {
                    seen_direction_signal_l.store(true, Ordering::Relaxed);
                }
                Ok(Ok(EngineEvent::Control(ControlCommand::Flatten))) => {
                    seen_flatten_l.store(true, Ordering::Relaxed);
                    let _ = execution_for_listener.flatten_all().await;
                }
                _ => {}
            }
        }
    });

    let _ = bus.publish(EngineEvent::DirectionSignal(DirectionSignal {
        symbol: symbol.clone(),
        direction: Direction::Up,
        magnitude_pct: 0.32,
        confidence: 0.93,
        recommended_tf: TimeframeClass::Tf5m,
        velocity_bps_per_sec: 12.0,
        acceleration: 3.0,
        tick_consistency: 4,
        triple_confirm: true,
        momentum_spike: true,
        ts_ns: now_ns(),
    }));

    let entry_ack = execution
        .place_order_v2(OrderIntentV2 {
            market_id: market_id.clone(),
            token_id: Some("yes_token".to_string()),
            side: OrderSide::BuyYes,
            price: 0.52,
            size: 2.0,
            ttl_ms: 500,
            style: ExecutionStyle::Taker,
            tif: OrderTimeInForce::Fak,
            max_slippage_bps: 10.0,
            fee_rate_bps: 2.0,
            expected_edge_net_bps: 15.0,
            client_order_id: Some(new_id()),
            hold_to_resolution: false,
            prebuilt_payload: None,
            prebuilt_auth: None,
        })
        .await
        .expect("paper entry ack");
    assert!(entry_ack.accepted);

    {
        let mut manager = shared.predator_exit_manager.write().await;
        manager.register(PositionLifecycle {
            position_id: entry_ack.order_id.clone(),
            market_id: market_id.clone(),
            symbol: symbol.clone(),
            opened_at_ms: now_ms,
            entry_edge_usdc: 0.20,
            entry_notional_usdc: 1.04,
        });
    }

    spawn_predator_exit_lifecycle(
        shared.clone(),
        bus.clone(),
        execution.clone(),
        shadow,
        entry_ack.order_id.clone(),
        market_id.clone(),
        symbol.clone(),
        OrderSide::BuyYes,
        0.52,
        2.0,
        500,
        0.70,
    );

    sleep(Duration::from_millis(120)).await;
    let _ = wss_tx.send(WssFillEvent {
        order_id: entry_ack.order_id.clone(),
        market_id: market_id.clone(),
        price: 0.45,
        size: 2.0,
        event_type: "trade",
        ts_ms: Utc::now().timestamp_millis(),
    });
    sleep(Duration::from_millis(900)).await;
    let _ = listener.await;

    let report = shared.shadow_stats.build_live_report().await;
    let has_reversal_reason = report.exit_reason_top.iter().any(|(reason, count)| {
        *count > 0 && (reason == "t_plus_100ms_reversal" || reason == "t_plus_300ms_reversal")
    });

    assert!(
        seen_direction_signal.load(Ordering::Relaxed),
        "direction signal stage must be observed"
    );
    assert!(
        seen_flatten.load(Ordering::Relaxed),
        "reversal path must publish flatten control"
    );
    assert!(
        has_reversal_reason,
        "live report must record reversal exit reason"
    );
    assert!(
        report.executed_count >= 1,
        "live report must include executed re-entry orders"
    );
}

#[test]
fn normalize_reject_code_sanitizes_non_alnum() {
    let normalized = normalize_reject_code("HTTP 429/Too Many Requests");
    assert_eq!(normalized, "http_429_too_many_requests");
}

#[test]
fn estimate_feed_latency_separates_source_and_backlog() {
    let now = now_ns();
    let now_ms = now / 1_000_000;
    let tick = RefTick {
        source: "binance_ws".to_string().into(),
        symbol: "BTCUSDT".to_string(),
        source_seq: now_ms as u64,
        event_ts_ms: now_ms - 300,
        recv_ts_ms: now_ms - 200,
        event_ts_exchange_ms: now_ms - 300,
        recv_ts_local_ns: now - 200_000_000,
        ingest_ts_local_ns: now - 198_000_000,
        ts_first_hop_ms: None,
        price: 70_000.0,
    };
    let book = BookTop {
        market_id: "m".to_string(),
        token_id_yes: "yes".to_string(),
        token_id_no: "no".to_string(),
        bid_yes: 0.49,
        ask_yes: 0.51,
        bid_no: 0.49,
        ask_no: 0.51,
        bid_size_yes: 0.0,
        ask_size_yes: 0.0,
        bid_size_no: 0.0,
        ask_size_no: 0.0,
        ts_ms: now_ms - 40,
        recv_ts_local_ns: now - 20_000_000,
    };

    let sample = estimate_feed_latency(&tick, &book);
    assert!(sample.source_latency_ms >= 95.0);
    assert!(sample.source_latency_ms <= 110.0);
    assert!(sample.local_backlog_ms >= 10.0);
    assert!(sample.local_backlog_ms <= 40.0);
    assert!(sample.feed_in_ms >= 1.0);
    assert!(sample.feed_in_ms <= 4.0);
    assert!((sample.feed_in_ms - sample.ref_decode_ms).abs() < 0.0001);
}

#[test]
fn estimate_feed_latency_calibrates_path_lag_with_clock_offset() {
    reset_path_lag_calib_for_tests();
    let now = now_ns();
    let now_ms = now / 1_000_000;
    let book = BookTop {
        market_id: "m".to_string(),
        token_id_yes: "yes".to_string(),
        token_id_no: "no".to_string(),
        bid_yes: 0.49,
        ask_yes: 0.51,
        bid_no: 0.49,
        ask_no: 0.51,
        bid_size_yes: 0.0,
        ask_size_yes: 0.0,
        bid_size_no: 0.0,
        ask_size_no: 0.0,
        ts_ms: now_ms - 10,
        recv_ts_local_ns: now - 5_000_000,
    };

    // First sample establishes floor with a negative raw delta (clock skew baseline).
    let tick_floor = RefTick {
        source: "binance_udp_calib".to_string().into(),
        symbol: "BTCUSDT".to_string(),
        source_seq: 1,
        event_ts_ms: now_ms - 120,
        recv_ts_ms: now_ms - 100,
        event_ts_exchange_ms: now_ms - 120,
        recv_ts_local_ns: now - 100_000_000,
        ingest_ts_local_ns: now - 99_000_000,
        ts_first_hop_ms: Some(now_ms - 90),
        price: 70_000.0,
    };
    let first = estimate_feed_latency(&tick_floor, &book);
    assert!(first.path_lag_ms <= 0.01);

    // Second sample has larger delta and should show positive path lag after calibration.
    let tick_next = RefTick {
        source_seq: 2,
        recv_ts_ms: now_ms - 80,
        ts_first_hop_ms: Some(now_ms - 95),
        ..tick_floor.clone()
    };
    let second = estimate_feed_latency(&tick_next, &book);
    assert!(second.path_lag_ms >= 20.0);
    assert!(second.path_lag_ms <= 35.0);
}

#[test]
fn should_replace_ref_tick_respects_staleness_budget_for_same_event() {
    let current = RefTick {
        source: "binance_ws".to_string().into(),
        symbol: "BTCUSDT".to_string(),
        source_seq: 10,
        event_ts_ms: 1_000,
        recv_ts_ms: 1_010,
        event_ts_exchange_ms: 1_000,
        recv_ts_local_ns: 1_000_000_000,
        ingest_ts_local_ns: 1_000_100_000,
        ts_first_hop_ms: None,
        price: 100.0,
    };
    let mut next = current.clone();
    next.source = "binance_udp".to_string().into();
    next.recv_ts_local_ns = current.recv_ts_local_ns + 2_000_000;
    next.recv_ts_ms = current.recv_ts_ms + 2;

    assert!(!should_replace_ref_tick(&current, &next));
}

#[test]
fn upsert_latest_tick_slot_reports_source_switch_delta() {
    let mut ticks = HashMap::<String, RefTick>::new();
    let first = RefTick {
        source: "binance_ws".to_string().into(),
        symbol: "BTCUSDT".to_string(),
        source_seq: 1,
        event_ts_ms: 1_000,
        recv_ts_ms: 1_010,
        event_ts_exchange_ms: 1_000,
        recv_ts_local_ns: 1_000_000_000,
        ingest_ts_local_ns: 1_000_050_000,
        ts_first_hop_ms: None,
        price: 100.0,
    };
    let mut second = first.clone();
    second.source = "binance_udp".to_string().into();
    second.event_ts_exchange_ms += 2;
    second.recv_ts_ms += 1;
    second.recv_ts_local_ns += 400_000;
    second.price = 100.2;

    let first_delta = upsert_latest_tick_slot_local(&mut ticks, first, should_replace_ref_tick);
    assert!(first_delta.is_none());
    let second_delta = upsert_latest_tick_slot_local(&mut ticks, second, should_replace_ref_tick);
    assert_eq!(second_delta, Some(400_000));
}

#[test]
fn sol_guard_observe_only_for_high_latency_or_spread() {
    let mut cfg = MakerConfig::default();
    cfg.market_tier_profile = "balanced_sol_guard".to_string();
    let tox = ToxicDecision {
        market_id: "m".to_string(),
        symbol: "SOLUSDT".to_string(),
        tox_score: 0.2,
        regime: ToxicRegime::Safe,
        reason_codes: vec![],
        ts_ns: 1,
    };
    assert!(should_observe_only_symbol(
        "SOLUSDT", &cfg, &tox, 120.0, 0.01, 180.0
    ));
    assert!(should_observe_only_symbol(
        "SOLUSDT", &cfg, &tox, 320.0, 0.01, 80.0
    ));
    assert!(should_observe_only_symbol(
        "SOLUSDT", &cfg, &tox, 120.0, 0.03, 80.0
    ));
    assert!(!should_observe_only_symbol(
        "SOLUSDT", &cfg, &tox, 120.0, 0.01, 80.0
    ));
    assert!(!should_observe_only_symbol(
        "BTCUSDT", &cfg, &tox, 260.0, 0.03, 999.0
    ));
}

#[test]
fn quote_block_ratio_matches_contract_and_is_bounded() {
    assert_eq!(quote_block_ratio(0, 0), 0.0);
    assert_eq!(quote_block_ratio(10, 0), 0.0);
    assert_eq!(quote_block_ratio(0, 10), 1.0);

    let r = quote_block_ratio(10, 2);
    assert!(r > 0.0 && r < 1.0);
}

#[test]
fn policy_block_ratio_matches_contract_and_is_bounded() {
    assert_eq!(policy_block_ratio(0, 0), 0.0);
    assert_eq!(policy_block_ratio(10, 0), 0.0);
    assert_eq!(policy_block_ratio(0, 10), 1.0);

    let r = policy_block_ratio(10, 2);
    assert!(r > 0.0 && r < 1.0);
}

#[test]
fn uptime_pct_is_bounded() {
    let stats = ShadowStats::new();
    let u = stats.uptime_pct(Duration::from_secs(1));
    assert!((0.0..=100.0).contains(&u));
}

#[test]
fn parse_toml_array_for_key_handles_multiline_arrays() {
    let raw = r#"
assets = [
  "BTCUSDT",
  "ETHUSDT",
  "XRPUSDT",
]
market_types = ["updown", "range"]
timeframes = ["5m", "15m", "1h", "1d"]
"#;
    let assets = parse_toml_array_for_key(raw, "assets").unwrap_or_default();
    assert_eq!(
        assets,
        vec![
            "BTCUSDT".to_string(),
            "ETHUSDT".to_string(),
            "XRPUSDT".to_string()
        ]
    );
    let market_types = parse_toml_array_for_key(raw, "market_types").unwrap_or_default();
    assert_eq!(
        market_types,
        vec!["updown".to_string(), "range".to_string()]
    );
    let timeframes = parse_toml_array_for_key(raw, "timeframes").unwrap_or_default();
    assert_eq!(
        timeframes,
        vec![
            "5m".to_string(),
            "15m".to_string(),
            "1h".to_string(),
            "1d".to_string()
        ]
    );
}

#[test]
fn parse_toml_array_for_key_returns_none_for_missing_or_empty() {
    let raw = "foo = 1\nassets = []\n";
    assert!(parse_toml_array_for_key(raw, "missing").is_none());
    assert!(parse_toml_array_for_key(raw, "assets").is_none());
}

#[test]
fn blend_settlement_probability_moves_toward_half_when_gap_widens() {
    let p_small_gap = blend_settlement_probability(0.80, 100.0, 99.9);
    let p_large_gap = blend_settlement_probability(0.80, 100.0, 97.5);
    assert!(p_small_gap > 0.70);
    assert!(p_large_gap < p_small_gap);
    assert!(p_large_gap > 0.50);
}

#[test]
fn blend_settlement_probability_is_stable_when_prices_match() {
    let p = blend_settlement_probability(0.63, 100.0, 100.0);
    assert!((p - 0.63).abs() < 1e-9);
}

#[test]
fn source_health_snapshot_penalizes_bad_stream() {
    let cfg = SourceHealthConfig::default();
    let stats = SourceRuntimeStats {
        sample_count: 256,
        latency_sum_ms: 256.0 * 24.0,
        latency_sq_sum_ms: 256.0 * (24.0f64.powi(2) + 18.0f64.powi(2)),
        out_of_order_count: 12,
        gap_count: 16,
        last_event_ts_ms: 1_699_999_994_900,
        last_recv_ts_ms: 1_699_999_995_000,
        deviation_ema_bps: 80.0,
    };
    let row = build_source_health_snapshot("binance", &stats, &cfg, 1_700_000_000_000);
    assert!(row.score < 0.45);
    assert!(row.gap_rate > 0.01);
    assert!(row.out_of_order_rate > 0.01);
    assert!(row.jitter_ms > cfg.jitter_limit_ms);
    assert!(row.freshness_score < 0.5);
}

#[test]
fn source_health_snapshot_stays_high_for_clean_stream() {
    let cfg = SourceHealthConfig::default();
    let stats = SourceRuntimeStats {
        sample_count: 512,
        latency_sum_ms: 512.0 * 4.0,
        latency_sq_sum_ms: 512.0 * (4.0f64.powi(2) + 0.8f64.powi(2)),
        out_of_order_count: 0,
        gap_count: 0,
        last_event_ts_ms: 1_699_999_999_996,
        last_recv_ts_ms: 1_699_999_999_998,
        deviation_ema_bps: 3.0,
    };
    let row = build_source_health_snapshot("coinbase", &stats, &cfg, 1_700_000_000_000);
    assert!(row.score > 0.75);
    assert!(row.jitter_ms < cfg.jitter_limit_ms);
    assert!(row.price_deviation_bps < cfg.deviation_limit_bps);
    assert!(row.freshness_score > 0.9);
}

#[test]
fn classify_predator_regime_defend_on_toxicity_or_source_health() {
    let cfg = PredatorRegimeConfig::default();
    let source_cfg = SourceHealthConfig::default();
    let signal = DirectionSignal {
        symbol: "BTCUSDT".to_string(),
        direction: Direction::Up,
        magnitude_pct: 0.25,
        confidence: 0.92,
        recommended_tf: TimeframeClass::Tf15m,
        velocity_bps_per_sec: 10.0,
        acceleration: 1.0,
        tick_consistency: 3,
        triple_confirm: true,
        momentum_spike: false,
        ts_ns: 1,
    };

    let regime_tox =
        classify_predator_regime(&cfg, &signal, 0.95, &ToxicRegime::Danger, None, &source_cfg);
    assert_eq!(regime_tox, Regime::Defend);

    let source_low = SourceHealth {
        source: "binance_udp".to_string(),
        latency_ms: 20.0,
        jitter_ms: 5.0,
        out_of_order_rate: 0.0,
        gap_rate: 0.0,
        price_deviation_bps: 0.0,
        freshness_score: 1.0,
        score: 0.10,
        sample_count: source_cfg.min_samples,
        ts_ms: 1,
    };
    let regime_source = classify_predator_regime(
        &cfg,
        &signal,
        0.10,
        &ToxicRegime::Safe,
        Some(&source_low),
        &source_cfg,
    );
    assert_eq!(regime_source, Regime::Defend);
}

#[test]
fn classify_predator_regime_switches_between_active_and_quiet() {
    let cfg = PredatorRegimeConfig::default();
    let source_cfg = SourceHealthConfig::default();
    let mut active_signal = DirectionSignal {
        symbol: "BTCUSDT".to_string(),
        direction: Direction::Up,
        magnitude_pct: 0.20,
        confidence: 0.85,
        recommended_tf: TimeframeClass::Tf5m,
        velocity_bps_per_sec: 8.0,
        acceleration: 1.2,
        tick_consistency: 3,
        triple_confirm: true,
        momentum_spike: false,
        ts_ns: 1,
    };

    let active = classify_predator_regime(
        &cfg,
        &active_signal,
        0.15,
        &ToxicRegime::Safe,
        None,
        &source_cfg,
    );
    assert_eq!(active, Regime::Active);

    active_signal.confidence = 0.45;
    active_signal.magnitude_pct = 0.02;
    let quiet = classify_predator_regime(
        &cfg,
        &active_signal,
        0.10,
        &ToxicRegime::Safe,
        None,
        &source_cfg,
    );
    assert_eq!(quiet, Regime::Quiet);
}

#[test]
fn is_market_in_top_n_matches_score_order_without_sorting() {
    let mut states = HashMap::<String, MarketToxicState>::new();
    let mut s1 = MarketToxicState::default();
    s1.market_score = 95.0;
    states.insert("m1".to_string(), s1);
    let mut s2 = MarketToxicState::default();
    s2.market_score = 88.0;
    states.insert("m2".to_string(), s2);
    let mut s3 = MarketToxicState::default();
    s3.market_score = 70.0;
    states.insert("m3".to_string(), s3);

    assert!(is_market_in_top_n(&states, "m1", 1));
    assert!(!is_market_in_top_n(&states, "m2", 1));
    assert!(is_market_in_top_n(&states, "m2", 2));
    assert!(!is_market_in_top_n(&states, "m3", 2));
}

#[test]
fn prebuild_order_payload_uses_intent_tif_and_style() {
    let intent = OrderIntentV2 {
        market_id: "m1".to_string(),
        side: OrderSide::BuyYes,
        token_id: Some("t1".to_string()),
        price: 0.52,
        size: 5.0,
        ttl_ms: 120,
        style: ExecutionStyle::Maker,
        tif: OrderTimeInForce::PostOnly,
        client_order_id: Some("cid1".to_string()),
        max_slippage_bps: 12.0,
        fee_rate_bps: 2.0,
        expected_edge_net_bps: 8.0,
        hold_to_resolution: false,
        prebuilt_payload: None,
        prebuilt_auth: None,
    };
    let bytes = prebuild_order_payload(&intent).expect("payload");
    let value: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
    assert_eq!(value["style"], "maker");
    assert_eq!(value["tif"], "POST_ONLY");
    assert_eq!(value["token_id"], "t1");
}

#[test]
fn compute_coalesce_policy_skips_for_shallow_queue() {
    let p = compute_coalesce_policy(3, Some(10.0), 96, 4, 200);
    assert_eq!(p.max_events, 0);
    assert_eq!(p.budget_us, 0);
}

#[test]
fn compute_coalesce_policy_scales_for_deep_queue() {
    let p = compute_coalesce_policy(40, Some(50.0), 96, 4, 200);
    assert!(p.max_events >= 24);
    assert!(p.max_events <= 40);
    assert_eq!(p.budget_us, 160);

    let fast = compute_coalesce_policy(40, Some(10.0), 96, 4, 200);
    assert_eq!(fast.budget_us, 40);
}

#[test]
fn compute_coalesce_policy_uses_drain_mode_for_very_deep_queue() {
    let p = compute_coalesce_policy(200, Some(60.0), 256, 4, 120);
    assert_eq!(p.max_events, 200);
    assert_eq!(p.budget_us, 800);
}

#[test]
fn v52_time_phase_boundaries_for_5m_and_15m() {
    let cfg = V52TimePhaseConfig::default();
    let tf_5m_total = timeframe_total_ms(TimeframeClass::Tf5m).expect("5m total");
    let tf_15m_total = timeframe_total_ms(TimeframeClass::Tf15m).expect("15m total");
    assert_eq!(tf_5m_total, 300_000);
    assert_eq!(tf_15m_total, 900_000);

    assert_eq!(classify_time_phase(0.56, &cfg), TimePhase::Early);
    assert_eq!(classify_time_phase(0.55, &cfg), TimePhase::Maturity);
    assert_eq!(classify_time_phase(0.11, &cfg), TimePhase::Maturity);
    assert_eq!(classify_time_phase(0.10, &cfg), TimePhase::Late);
    assert_eq!(classify_time_phase(0.01, &cfg), TimePhase::Late);
}

#[test]
fn v52_momentum_overlay_only_in_maturity() {
    assert_eq!(stage_for_phase(TimePhase::Early, true), Stage::Early);
    assert_eq!(stage_for_phase(TimePhase::Late, true), Stage::Late);
    assert_eq!(stage_for_phase(TimePhase::Maturity, false), Stage::Maturity);
    assert_eq!(stage_for_phase(TimePhase::Maturity, true), Stage::Momentum);
}

#[test]
fn v52_phase_size_scale_is_configurable() {
    let mut cfg = V52TimePhaseConfig::default();
    cfg.early_size_scale = 0.7;
    cfg.maturity_size_scale = 1.1;
    cfg.late_size_scale = 1.4;
    assert!((time_phase_size_scale(TimePhase::Early, &cfg) - 0.7).abs() < 1e-9);
    assert!((time_phase_size_scale(TimePhase::Maturity, &cfg) - 1.1).abs() < 1e-9);
    assert!((time_phase_size_scale(TimePhase::Late, &cfg) - 1.4).abs() < 1e-9);
}

#[test]
fn v52_force_taker_rule_applies_to_maturity_and_late() {
    let cfg = V52ExecutionConfig::default();
    assert!(!should_force_taker_fallback(TimePhase::Early, 29_000, &cfg));
    assert!(should_force_taker_fallback(
        TimePhase::Maturity,
        29_000,
        &cfg
    ));
    assert!(should_force_taker_fallback(TimePhase::Late, 29_000, &cfg));
    assert!(!should_force_taker_fallback(
        TimePhase::Maturity,
        31_000,
        &cfg
    ));
}

#[test]
fn v52_dual_arb_threshold_formula() {
    let cfg = V52DualArbConfig::default();
    assert!(allow_dual_side_arb(0.40, 0.40, 2.0, &cfg));
    assert!(!allow_dual_side_arb(0.50, 0.49, 2.0, &cfg));
}
