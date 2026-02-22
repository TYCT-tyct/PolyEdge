use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use dashmap::DashMap;
use direction_detector::DirectionDetector;
use execution_clob::{ClobExecution, ExecutionMode};
use exit_manager::ExitManager;
use infra_bus::RingBus;
use observability::{init_metrics, init_tracing};
use paper_executor::ShadowExecutor;
use portfolio::PortfolioBook;
use probability_engine::ProbabilityEngine;
use reqwest::Client;
use risk_engine::DefaultRiskManager;
use settlement_compounder::SettlementCompounder;
use taker_sniper::TakerSniper;
use timeframe_router::TimeframeRouter;
use tokio::sync::RwLock;

use crate::config_loader::{
    load_edge_model_config, load_execution_config, load_exit_config, load_fair_value_config,
    load_fusion_config, load_perf_profile_config, load_predator_c_config, load_risk_limits_config,
    load_seat_config, load_settlement_config, load_source_health_config, load_strategy_config,
    load_toxicity_config, load_universe_config,
};
use crate::feed_runtime::{spawn_market_feed, spawn_reference_feed, spawn_settlement_feed};
use crate::paper_runtime::{set_global_paper_runtime, PaperRuntimeHandle};
use crate::report_io::{ensure_dataset_dirs, init_jsonl_writer, init_storage_gc_worker};
use crate::seat_runtime::SeatRuntimeHandle;
use crate::state::{
    settlement_live_gate_status, to_exit_manager_config, AllocatorConfig, AppState, EngineShared,
    ShadowStats, StrategyIngressMsg,
};
use crate::{control_api, orchestration, spawn_detached, spawn_strategy_engine};

fn env_flag_enabled(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| {
            let normalized = v.trim().to_ascii_lowercase();
            matches!(normalized.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false)
}

pub(super) async fn async_main() -> Result<()> {
    let _guard = init_tracing("app_runner");
    let prometheus = init_metrics();
    ensure_dataset_dirs();
    let control_port = std::env::var("POLYEDGE_CONTROL_PORT")
        .ok()
        .and_then(|v| v.trim().parse::<u16>().ok())
        .unwrap_or(8080);
    tracing::info!(control_port, "resolved control port");
    std::env::set_var("POLYEDGE_CONTROL_PORT", control_port.to_string());
    let mut seat_cfg = load_seat_config();
    if seat_cfg.control_base_url.trim().is_empty() {
        seat_cfg.control_base_url = format!("http://127.0.0.1:{control_port}");
    }
    let seat = SeatRuntimeHandle::spawn(seat_cfg);
    let seat_fill_counter = seat.live_fill_counter();
    let paper_enabled = std::env::var("POLYEDGE_PAPER_ENABLED")
        .ok()
        .map(|v| {
            !matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "off" | "no"
            )
        })
        .unwrap_or(true);
    let paper_initial_capital = std::env::var("POLYEDGE_PAPER_INITIAL_CAPITAL")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(100.0)
        .max(1.0);
    let paper_run_id = std::env::var("POLYEDGE_PAPER_RUN_ID")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| format!("paper-{}", chrono::Utc::now().format("%Y%m%d%H%M%S")));
    let paper_sqlite_enabled = std::env::var("POLYEDGE_PAPER_SQLITE_ENABLED")
        .ok()
        .map(|v| {
            !matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "off" | "no"
            )
        })
        .unwrap_or(true);
    let paper = PaperRuntimeHandle::new(
        paper_enabled,
        paper_run_id,
        paper_initial_capital,
        paper_sqlite_enabled,
        seat.clone(),
    );
    set_global_paper_runtime(paper.clone());

    let execution_cfg = load_execution_config();
    let universe_cfg = load_universe_config();
    let settlement_cfg_boot = load_settlement_config();
    let bus_capacity = std::env::var("POLYEDGE_BUS_CAPACITY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(32_768)
        .clamp(4_096, 262_144);
    let bus = RingBus::new(bus_capacity);
    let portfolio = Arc::new(PortfolioBook::default());
    let live_armed = env_flag_enabled("POLYEDGE_LIVE_ARMED");
    let force_paper = env_flag_enabled("POLYEDGE_FORCE_PAPER");
    let live_gate = settlement_live_gate_status(&settlement_cfg_boot);
    let exec_mode = if force_paper {
        if execution_cfg.mode.eq_ignore_ascii_case("live") {
            tracing::warn!(
                "POLYEDGE_FORCE_PAPER is enabled; forcing paper mode despite execution.mode=live"
            );
        }
        ExecutionMode::Paper
    } else if execution_cfg.mode.eq_ignore_ascii_case("live") {
        if !live_armed {
            tracing::warn!(
                "execution.mode=live but POLYEDGE_LIVE_ARMED is not true; forcing paper mode"
            );
            ExecutionMode::Paper
        } else if !live_gate.ready {
            tracing::warn!(
                reason = %live_gate.reason,
                "execution.mode=live requested but settlement live gate failed; forcing paper mode"
            );
            ExecutionMode::Paper
        } else {
            ExecutionMode::Live
        }
    } else {
        ExecutionMode::Paper
    };
    let execution = Arc::new(ClobExecution::new_with_order_routing(
        exec_mode,
        execution_cfg.clob_endpoint.clone(),
        execution_cfg.order_endpoint.clone(),
        execution_cfg.order_backup_endpoint.clone(),
        Duration::from_millis(execution_cfg.http_timeout_ms),
        Duration::from_millis(execution_cfg.order_failover_timeout_ms.max(10)),
    ));

    // Optional: prewarm the execution HTTP client pool to reduce first-ack latency spikes.
    // Uses the *same* reqwest client inside the execution layer (unlike ad-hoc curl probes).
    if let Ok(raw) = std::env::var("POLYEDGE_HTTP_PREWARM_URLS") {
        let urls = raw
            .split(',')
            .map(str::trim)
            .filter(|u| u.starts_with("http://") || u.starts_with("https://"))
            .map(|u| u.to_string())
            .collect::<Vec<_>>();
        if !urls.is_empty() {
            tracing::info!(count = urls.len(), "prewarming execution http pool");
            let exec = execution.clone();
            spawn_detached("execution_http_prewarm", false, async move {
                exec.prewarm_urls(&urls).await;
            });
        }
    }
    let shadow = Arc::new(ShadowExecutor::new());
    let strategy_cfg = Arc::new(RwLock::new(Arc::new(load_strategy_config())));
    let settlement_cfg = Arc::new(RwLock::new(settlement_cfg_boot));
    let fusion_cfg = Arc::new(RwLock::new(load_fusion_config()));
    let source_health_cfg = Arc::new(RwLock::new(load_source_health_config()));
    let edge_model_cfg = Arc::new(RwLock::new(load_edge_model_config()));
    let exit_cfg = Arc::new(RwLock::new(load_exit_config()));
    let exit_cfg0 = exit_cfg.read().await.clone();
    let fair_value_cfg = Arc::new(StdRwLock::new(load_fair_value_config()));
    let toxicity_cfg = Arc::new(RwLock::new(Arc::new(load_toxicity_config())));
    let risk_limits = Arc::new(StdRwLock::new(load_risk_limits_config()));
    let perf_profile = Arc::new(RwLock::new(load_perf_profile_config()));
    let allocator_cfg = {
        let strategy = strategy_cfg.read().await.clone();
        let tox = toxicity_cfg.read().await.clone();
        Arc::new(RwLock::new(AllocatorConfig {
            capital_fraction_kelly: strategy.capital_fraction_kelly,
            variance_penalty_lambda: strategy.variance_penalty_lambda,
            active_top_n_markets: tox.active_top_n_markets,
            ..AllocatorConfig::default()
        }))
    };
    let tox_state = Arc::new(RwLock::new(HashMap::new()));
    let shadow_stats = Arc::new(ShadowStats::new());
    let paused = Arc::new(RwLock::new(false));
    let universe_symbols = Arc::new(universe_cfg.assets.clone());
    let universe_market_types = Arc::new(universe_cfg.market_types.clone());
    let universe_timeframes = Arc::new(universe_cfg.timeframes.clone());
    init_jsonl_writer(perf_profile.clone()).await;
    init_storage_gc_worker();

    let risk_manager = Arc::new(DefaultRiskManager::new(risk_limits.clone()));
    let predator_cfg = Arc::new(RwLock::new(load_predator_c_config()));
    let mut predator_cfg0 = predator_cfg.read().await.clone();
    if paper_enabled && predator_cfg0.compounder.enabled {
        // Keep risk/compounder baseline consistent with paper bankroll to avoid
        // premature risk-cap triggers caused by mismatched initial capital bases.
        predator_cfg0.compounder.initial_capital_usdc = paper_initial_capital.max(1.0);
        *predator_cfg.write().await = predator_cfg0.clone();
    }
    if predator_cfg0
        .roll_v1
        .fee_model
        .mode
        .eq_ignore_ascii_case("official_formula")
    {
        std::env::set_var("POLYEDGE_FEE_MODEL", "official_poly_formula");
    } else {
        std::env::set_var("POLYEDGE_FEE_MODEL", "legacy_linear");
    }
    crate::execution_eval::reload_dynamic_fee_config_from_env();
    let predator_direction_detector = Arc::new(RwLock::new(DirectionDetector::new(
        predator_cfg0.direction_detector.clone(),
    )));
    let predator_latest_direction = Arc::new(RwLock::new(HashMap::new()));
    let predator_latest_probability = Arc::new(RwLock::new(HashMap::new()));
    let predator_probability_engine = Arc::new(RwLock::new(ProbabilityEngine::new(
        predator_cfg0.probability_engine.clone(),
    )));
    let predator_taker_sniper = Arc::new(RwLock::new(TakerSniper::new(
        predator_cfg0.taker_sniper.clone(),
    )));
    let predator_d_last_fire_ms = Arc::new(RwLock::new(HashMap::new()));
    let predator_router = Arc::new(RwLock::new(TimeframeRouter::new(
        predator_cfg0.router.clone(),
    )));
    let predator_compounder = Arc::new(RwLock::new(SettlementCompounder::new(
        predator_cfg0.compounder.clone(),
    )));
    let predator_exit_manager = Arc::new(RwLock::new(ExitManager::new(to_exit_manager_config(
        &exit_cfg0,
    ))));
    let draining = Arc::new(RwLock::new(false));
    let shared = Arc::new(EngineShared {
        draining: draining.clone(),
        latest_books: Arc::new(RwLock::new(HashMap::new())),
        latest_signals: Arc::new(DashMap::new()),
        market_to_symbol: Arc::new(RwLock::new(HashMap::new())),
        market_to_title: Arc::new(RwLock::new(HashMap::new())),
        market_to_type: Arc::new(RwLock::new(HashMap::new())),
        token_to_symbol: Arc::new(RwLock::new(HashMap::new())),
        market_to_timeframe: Arc::new(RwLock::new(HashMap::new())),
        market_to_end_ts_ms: Arc::new(RwLock::new(HashMap::new())),
        symbol_to_markets: Arc::new(RwLock::new(HashMap::new())),
        fee_cache: Arc::new(RwLock::new(HashMap::new())),
        fee_refresh_inflight: Arc::new(RwLock::new(HashMap::new())),
        scoring_cache: Arc::new(RwLock::new(HashMap::new())),
        http: Client::new(),
        clob_endpoint: execution_cfg.clob_endpoint.clone(),
        strategy_cfg,
        settlement_cfg: settlement_cfg.clone(),
        source_health_cfg: source_health_cfg.clone(),
        source_health_latest: Arc::new(RwLock::new(HashMap::new())),
        settlement_prices: Arc::new(RwLock::new(HashMap::new())),
        fusion_cfg: fusion_cfg.clone(),
        edge_model_cfg: edge_model_cfg.clone(),
        exit_cfg: exit_cfg.clone(),
        fair_value_cfg,
        toxicity_cfg,
        risk_manager,
        risk_limits: risk_limits.clone(),
        universe_symbols: universe_symbols.clone(),
        universe_market_types: universe_market_types.clone(),
        universe_timeframes: universe_timeframes.clone(),
        rate_limit_rps: execution_cfg.rate_limit_rps.max(0.1),
        tox_state,
        shadow_stats,
        predator_cfg: predator_cfg.clone(),
        predator_direction_detector,
        predator_latest_direction,
        predator_latest_probability,
        predator_probability_engine,
        predator_taker_sniper,
        predator_d_last_fire_ms,
        predator_router,
        predator_compounder,
        predator_exit_manager,
        // WSS User Channel: live 模式下启动实时 fill 通知
        // paper 模式下 wss_fill_tx = None，exit lifecycle 回退到纯 timer 路径
        wss_fill_tx: {
            let is_live = matches!(exec_mode, ExecutionMode::Live);
            let api_key = std::env::var("POLYEDGE_CLOB_API_KEY").unwrap_or_default();
            if is_live && !api_key.is_empty() {
                let wss_url = std::env::var("POLYEDGE_WSS_USER_URL").unwrap_or_else(|_| {
                    "wss://ws-subscriptions-clob.polymarket.com/ws/user".to_string()
                });
                let (tx, _rx) = tokio::sync::broadcast::channel::<
                    execution_clob::wss_user_feed::WssFillEvent,
                >(64);
                let tx = Arc::new(tx);
                let tx_clone = tx.clone();
                let fill_counter = seat_fill_counter.clone();
                tokio::spawn(async move {
                    execution_clob::wss_user_feed::run_wss_loop_with_sender(
                        tx_clone, wss_url, api_key,
                    )
                    .await;
                });
                let mut fill_rx = tx.subscribe();
                let execution_for_fill = execution.clone();
                spawn_detached("seat_live_fill_counter", false, async move {
                    loop {
                        match fill_rx.recv().await {
                            Ok(event) => {
                                if event.event_type == "trade" {
                                    fill_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    execution_for_fill.mark_order_closed_local(&event.order_id);
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                    }
                });
                Some(tx)
            } else {
                None
            }
        },
        presign_cache: Arc::new(RwLock::new(HashMap::new())),
    });
    let strategy_input_queue_cap = std::env::var("POLYEDGE_STRATEGY_INPUT_QUEUE_CAP")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(512)
        .clamp(128, 5_760);
    let (strategy_ingress_tx, strategy_ingress_rx) =
        crossbeam::channel::bounded::<StrategyIngressMsg>(strategy_input_queue_cap);

    let state = AppState {
        paused: paused.clone(),
        draining: draining.clone(),
        bus: bus.clone(),
        portfolio: portfolio.clone(),
        execution: execution.clone(),
        _shadow: shadow.clone(),
        prometheus,
        strategy_cfg: shared.strategy_cfg.clone(),
        fair_value_cfg: shared.fair_value_cfg.clone(),
        toxicity_cfg: shared.toxicity_cfg.clone(),
        allocator_cfg: allocator_cfg.clone(),
        risk_limits: risk_limits.clone(),
        tox_state: shared.tox_state.clone(),
        shadow_stats: shared.shadow_stats.clone(),
        perf_profile: perf_profile.clone(),
        shared: shared.clone(),
        seat: seat.clone(),
        paper: paper.clone(),
    };

    spawn_reference_feed(
        bus.clone(),
        shared.shadow_stats.clone(),
        (*universe_symbols).clone(),
        shared.fusion_cfg.clone(),
        shared.clone(),
        strategy_ingress_tx.clone(),
    );
    spawn_settlement_feed(shared.clone());
    spawn_market_feed(
        bus.clone(),
        shared.shadow_stats.clone(),
        (*universe_symbols).clone(),
        (*universe_market_types).clone(),
        (*universe_timeframes).clone(),
        strategy_ingress_tx,
    );
    spawn_strategy_engine(
        bus.clone(),
        portfolio,
        execution.clone(),
        shadow,
        paused.clone(),
        shared.clone(),
        strategy_ingress_rx,
    );
    if shared.fusion_cfg.read().await.udp_trigger_enabled {
        crate::engine_loop::spawn_udp_ghost_receiver(execution.clone(), shared.clone());
    }
    crate::engine_loop::spawn_presign_worker(shared.clone());
    orchestration::spawn_periodic_report_persistor(
        shared.shadow_stats.clone(),
        shared.tox_state.clone(),
        execution.clone(),
        shared.toxicity_cfg.clone(),
    );
    orchestration::spawn_data_reconcile_task(
        bus.clone(),
        paused.clone(),
        shared.shadow_stats.clone(),
    );

    let app = control_api::build_router(state);

    let addr: SocketAddr = format!("0.0.0.0:{control_port}").parse()?;
    tracing::info!(%addr, "control api started");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind control api listener at {addr}"))?;
    axum::serve(listener, app).await?;
    Ok(())
}

pub(super) fn install_rustls_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}
