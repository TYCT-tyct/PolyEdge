use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::Ordering;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use core_types::{
    ControlCommand, EngineEvent, ExecutionVenue, PaperDailySummary, PaperTradeRecord, ToxicRegime,
    TimeframeClass,
};
use direction_detector::DirectionConfig;
use fair_value::BasisMrConfig;
use probability_engine::ProbabilityEngineConfig;
use serde::{Deserialize, Serialize};
use settlement_compounder::{CompounderConfig, SettlementCompounder};
use taker_sniper::{TakerSniper, TakerSniperConfig};
use timeframe_router::{RouterConfig, TimeframeRouter};

use crate::report_io::{
    append_jsonl, dataset_path, persist_engine_pnl_report, persist_final_report_files,
    persist_live_report_files, persist_toxicity_report_files, JSONL_DROP_ON_FULL,
};
use crate::seat_types::{SeatForceLayerReq, SeatManualOverrideReq};
use crate::state::{
    settlement_live_gate_status, to_exit_manager_config, AllocatorConfig, AllocatorReloadReq,
    AllocatorReloadResp, AppState, EdgeModelConfig, EdgeModelReloadReq, EnginePnlReport,
    ExitConfig, ExitReloadReq, FusionConfig, FusionReloadReq, HealthResp, PerfProfile,
    PerfProfileReloadReq, PredatorCConfig, PredatorCPriority, PredatorCrossSymbolConfig,
    PredatorDConfig, PredatorRegimeConfig, ProbabilityReloadReq, RiskReloadReq, RiskReloadResp,
    ShadowFinalReport, SourceHealthConfig, SourceHealthReloadReq,
    StrategyReloadReq, StrategyReloadResp, TakerReloadReq, TakerReloadResp, ToxicityConfig,
    ToxicityFinalReport, ToxicityLiveReport, ToxicityReloadReq, V52Config, V52DualArbConfig,
    V52ExecutionConfig, V52ReversalConfig, V52TimePhaseConfig,
};
use crate::stats_utils::percentile;
use crate::toxicity_report::build_toxicity_live_report;

pub(super) fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/health/latency", get(health_latency)) // P4: lightweight latency probe
        .route("/metrics", get(metrics))
        .route("/state/positions", get(positions))
        .route("/state/pnl", get(pnl))
        .route("/report/shadow/live", get(report_shadow_live))
        .route("/report/shadow/final", get(report_shadow_final))
        .route("/report/pnl/by_engine", get(report_pnl_by_engine))
        .route("/report/direction", get(report_direction))
        .route("/report/router", get(report_router))
        .route("/report/capital", get(report_capital))
        .route("/report/toxicity/live", get(report_toxicity_live))
        .route("/report/toxicity/final", get(report_toxicity_final))
        .route("/report/seat/status", get(report_seat_status))
        .route("/report/seat/history", get(report_seat_history))
        .route("/report/paper/live", get(report_paper_live))
        .route("/report/paper/history", get(report_paper_history))
        .route("/report/paper/daily", get(report_paper_daily))
        .route("/report/paper/summary", get(report_paper_summary))
        .route("/control/pause", post(pause))
        .route("/control/resume", post(resume))
        .route("/control/drain/enable", post(drain_enable))
        .route("/control/drain/disable", post(drain_disable))
        .route("/control/flatten", post(flatten))
        .route("/control/arm_live", post(arm_live))
        .route("/control/seat/pause", post(seat_pause))
        .route("/control/seat/resume", post(seat_resume))
        .route("/control/seat/force_layer", post(seat_force_layer))
        .route("/control/seat/manual_override", post(seat_manual_override))
        .route("/control/seat/clear_override", post(seat_clear_override))
        .route("/control/paper/reset", post(reset_paper))
        .route("/control/reset_shadow", post(reset_shadow))
        .route("/control/reload_strategy", post(reload_strategy))
        .route("/control/reload_taker", post(reload_taker))
        .route("/control/reload_allocator", post(reload_allocator))
        .route("/control/reload_toxicity", post(reload_toxicity))
        .route("/control/reload_risk", post(reload_risk))
        .route("/control/reload_predator_c", post(reload_predator_c))
        .route("/predator/sync-balance", post(sync_predator_balance))
        .route("/control/reload_fusion", post(reload_fusion))
        .route("/control/reload_edge_model", post(reload_edge_model))
        .route("/control/reload_probability", post(reload_probability))
        .route("/control/reload_source_health", post(reload_source_health))
        .route("/control/reload_exit", post(reload_exit))
        .route("/control/reload_exit_manager", post(reload_exit))
        .route("/control/reload_regime", post(reload_regime))
        .route("/control/reload_perf_profile", post(reload_perf_profile))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Json<HealthResp> {
    let paused = *state.paused.read().await;
    Json(HealthResp {
        status: "ok",
        paused,
    })
}

// P4: lightweight latency probe endpoint.
// Used by storm_test to measure realistic HTTP RTT with minimal server-side work.
async fn health_latency(State(state): State<AppState>) -> Json<serde_json::Value> {
    let paused = *state.paused.read().await;
    let now = std::time::Instant::now();

    // Keep this handler intentionally small and allocation-light.
    Json(serde_json::json!({
        "status": "ok",
        "paused": paused,
        "timestamp_ms": chrono::Utc::now().timestamp_millis(),
        "probe_latency_us": now.elapsed().as_micros() as u64,
        "note": "lightweight endpoint for storm-test RTT probing"
    }))
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        state.prometheus.render(),
    )
}

async fn positions(State(state): State<AppState>) -> Json<HashMap<String, portfolio::Position>> {
    Json(state.portfolio.positions())
}

async fn pnl(State(state): State<AppState>) -> Json<core_types::PnLSnapshot> {
    Json(state.portfolio.snapshot())
}

async fn pause(State(state): State<AppState>) -> impl IntoResponse {
    *state.paused.write().await = true;
    state.shadow_stats.set_paused(true);
    let _ = state
        .bus
        .publish(EngineEvent::Control(ControlCommand::Pause));
    Json(serde_json::json!({"ok": true, "paused": true}))
}

async fn resume(State(state): State<AppState>) -> impl IntoResponse {
    *state.paused.write().await = false;
    state.shadow_stats.set_paused(false);
    let _ = state
        .bus
        .publish(EngineEvent::Control(ControlCommand::Resume));
    Json(serde_json::json!({"ok": true, "paused": false}))
}

async fn drain_enable(State(state): State<AppState>) -> impl IntoResponse {
    *state.draining.write().await = true;
    Json(serde_json::json!({"ok": true, "draining": true}))
}

async fn drain_disable(State(state): State<AppState>) -> impl IntoResponse {
    *state.draining.write().await = false;
    Json(serde_json::json!({"ok": true, "draining": false}))
}

async fn flatten(State(state): State<AppState>) -> impl IntoResponse {
    match state.execution.flatten_all().await {
        Ok(_) => {
            let _ = state
                .bus
                .publish(EngineEvent::Control(ControlCommand::Flatten));
            Json(serde_json::json!({"ok": true})).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"ok": false, "error": err.to_string()})),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct ArmLiveReq {
    armed: Option<bool>,
}

async fn arm_live(State(state): State<AppState>, Json(req): Json<ArmLiveReq>) -> impl IntoResponse {
    let force_paper = std::env::var("POLYEDGE_FORCE_PAPER")
        .ok()
        .map(|v| {
            let normalized = v.trim().to_ascii_lowercase();
            matches!(normalized.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false);
    if force_paper {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "ok": false,
                "armed": false,
                "error": "force_paper_guard_enabled",
            })),
        )
            .into_response();
    }

    let armed = req.armed.unwrap_or(true);
    if !armed {
        std::env::set_var("POLYEDGE_LIVE_ARMED", "false");
        return Json(
            serde_json::json!({"ok": true, "armed": false, "execution_live": state.execution.is_live()}),
        )
        .into_response();
    }

    let settlement_cfg = state.shared.settlement_cfg.read().await.clone();
    let gate = settlement_live_gate_status(&settlement_cfg);
    if !gate.ready {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "ok": false,
                "armed": false,
                "error": "settlement_live_gate_failed",
                "reason": gate.reason,
            })),
        )
            .into_response();
    }

    std::env::set_var("POLYEDGE_LIVE_ARMED", "true");
    let execution_live = state.execution.is_live();
    let payload = serde_json::json!({
        "ok": true,
        "armed": true,
        "execution_live": execution_live,
        "restart_required": !execution_live,
    });
    if execution_live {
        Json(payload).into_response()
    } else {
        (StatusCode::ACCEPTED, Json(payload)).into_response()
    }
}

async fn seat_pause(State(state): State<AppState>) -> impl IntoResponse {
    let status = state.seat.pause("manual_pause".to_string()).await;
    Json(serde_json::json!({"ok": true, "status": status}))
}

async fn seat_resume(State(state): State<AppState>) -> impl IntoResponse {
    let status = state.seat.resume().await;
    Json(serde_json::json!({"ok": true, "status": status}))
}

async fn seat_force_layer(
    State(state): State<AppState>,
    Json(req): Json<SeatForceLayerReq>,
) -> impl IntoResponse {
    let status = state.seat.force_layer(req).await;
    Json(serde_json::json!({"ok": true, "status": status}))
}

async fn seat_manual_override(
    State(state): State<AppState>,
    Json(req): Json<SeatManualOverrideReq>,
) -> impl IntoResponse {
    match state.seat.manual_override(req).await {
        Ok(status) => Json(serde_json::json!({"ok": true, "status": status})).into_response(),
        Err(err) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"ok": false, "error": err.to_string()})),
        )
            .into_response(),
    }
}

async fn seat_clear_override(State(state): State<AppState>) -> impl IntoResponse {
    let status = state.seat.clear_manual_override().await;
    Json(serde_json::json!({"ok": true, "status": status}))
}

#[derive(Debug, Deserialize)]
struct SeatHistoryQuery {
    limit: Option<usize>,
}

async fn report_seat_status(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.seat.status().await)
}

async fn report_seat_history(
    State(state): State<AppState>,
    Query(query): Query<SeatHistoryQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(120).clamp(1, 2_000);
    Json(state.seat.history(limit))
}

#[derive(Debug, Deserialize)]
struct PaperHistoryQuery {
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct MarketSelectionRow {
    market_id: String,
    symbol: String,
    market_type: String,
    timeframe: String,
    title: String,
}

fn timeframe_label(tf: &TimeframeClass) -> &'static str {
    match tf {
        TimeframeClass::Tf5m => "5m",
        TimeframeClass::Tf15m => "15m",
        TimeframeClass::Tf1h => "1h",
        TimeframeClass::Tf1d => "1d",
    }
}

async fn build_market_selection_snapshot(state: &AppState) -> serde_json::Value {
    let symbols = (*state.shared.universe_symbols).clone();
    let market_types = (*state.shared.universe_market_types).clone();
    let timeframes = (*state.shared.universe_timeframes).clone();
    let market_to_symbol = state.shared.market_to_symbol.read().await.clone();
    let market_to_title = state.shared.market_to_title.read().await.clone();
    let market_to_type = state.shared.market_to_type.read().await.clone();
    let market_to_timeframe = state.shared.market_to_timeframe.read().await.clone();

    let mut markets = market_to_symbol
        .iter()
        .map(|(market_id, symbol)| {
            let timeframe = market_to_timeframe
                .get(market_id)
                .map(timeframe_label)
                .unwrap_or("unknown")
                .to_string();
            let title = market_to_title
                .get(market_id)
                .cloned()
                .unwrap_or_default();
            let market_type = market_to_type
                .get(market_id)
                .cloned()
                .unwrap_or_else(|| "unknown".to_string());
            MarketSelectionRow {
                market_id: market_id.clone(),
                symbol: symbol.clone(),
                market_type,
                timeframe,
                title,
            }
        })
        .collect::<Vec<_>>();

    markets.sort_by(|a, b| {
        a.symbol
            .cmp(&b.symbol)
            .then(a.timeframe.cmp(&b.timeframe))
            .then(a.market_id.cmp(&b.market_id))
    });

    let title = format!(
        "PolyEdge Selected Markets: {} + {} + {}",
        symbols.join("/"),
        market_types.join("/"),
        timeframes.join("/")
    );

    let active_templates = markets
        .iter()
        .map(|m| format!("{}|{}|{}", m.symbol, m.market_type, m.timeframe))
        .collect::<HashSet<_>>();
    let mut expected_templates = HashSet::new();
    for s in &symbols {
        for mt in &market_types {
            for tf in &timeframes {
                expected_templates.insert(format!("{s}|{mt}|{tf}"));
            }
        }
    }
    let mut template_missing = expected_templates
        .difference(&active_templates)
        .cloned()
        .collect::<Vec<_>>();
    template_missing.sort();

    serde_json::json!({
        "title": title,
        "symbols": symbols,
        "market_types": market_types,
        "timeframes": timeframes,
        "template_expected_count": expected_templates.len(),
        "template_active_count": active_templates.len(),
        "template_missing_count": template_missing.len(),
        "template_missing": template_missing,
        "total_markets": markets.len(),
        "markets": markets,
        "generated_at_ms": Utc::now().timestamp_millis(),
    })
}

async fn report_paper_live(State(state): State<AppState>) -> impl IntoResponse {
    let live = state.paper.live_report().await;
    let market_selection = build_market_selection_snapshot(&state).await;
    let mut payload = serde_json::to_value(live).unwrap_or_else(|_| serde_json::json!({}));
    if let Some(obj) = payload.as_object_mut() {
        obj.insert("market_selection".to_string(), market_selection);
    }
    Json(payload)
}

async fn report_paper_history(
    State(state): State<AppState>,
    Query(query): Query<PaperHistoryQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(200).clamp(1, 5000);
    let rows: Vec<PaperTradeRecord> = state.paper.history(limit).await;
    Json(rows)
}

async fn report_paper_daily(State(state): State<AppState>) -> impl IntoResponse {
    let rows: Vec<PaperDailySummary> = state.paper.daily().await;
    Json(rows)
}

async fn report_paper_summary(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.paper.summary_json().await)
}

async fn reset_paper(State(state): State<AppState>) -> impl IntoResponse {
    state.paper.reset().await;
    Json(serde_json::json!({"ok": true, "paper_reset": true}))
}

async fn reset_shadow(State(state): State<AppState>) -> impl IntoResponse {
    let window_id = state.shadow_stats.reset().await;
    state.tox_state.write().await.clear();
    {
        let mut router = state.shared.predator_router.write().await;
        *router = TimeframeRouter::new(router.cfg().clone());
    }
    {
        let mut sniper = state.shared.predator_taker_sniper.write().await;
        *sniper = TakerSniper::new(sniper.cfg().clone());
    }
    {
        let cfg = state.shared.predator_cfg.read().await.clone();
        let mut compounder = state.shared.predator_compounder.write().await;
        *compounder = SettlementCompounder::new(cfg.compounder);
    }
    state.shared.predator_d_last_fire_ms.write().await.clear();
    Json(serde_json::json!({"ok": true, "shadow_reset": true, "window_id": window_id}))
}

#[derive(Debug, Deserialize)]
struct PredatorCReloadReq {
    enabled: Option<bool>,
    priority: Option<PredatorCPriority>,
    direction_detector: Option<DirectionConfig>,
    taker_sniper: Option<TakerSniperConfig>,
    strategy_d: Option<PredatorDConfig>,
    regime: Option<PredatorRegimeConfig>,
    cross_symbol: Option<PredatorCrossSymbolConfig>,
    router: Option<RouterConfig>,
    compounder: Option<CompounderConfig>,
    v52: Option<V52Config>,
    v52_time_phase: Option<V52TimePhaseConfig>,
    v52_execution: Option<V52ExecutionConfig>,
    v52_dual_arb: Option<V52DualArbConfig>,
    v52_reversal: Option<V52ReversalConfig>,
}

#[derive(Debug, Serialize)]
struct PredatorCReloadResp {
    predator_c: PredatorCConfig,
}

#[derive(Debug, Deserialize)]
struct PredatorSyncBalanceReq {
    usdc_balance: f64,
}

async fn sync_predator_balance(
    State(state): State<AppState>,
    Json(req): Json<PredatorSyncBalanceReq>,
) -> Json<serde_json::Value> {
    let mut compounder = state.shared.predator_compounder.write().await;
    compounder.sync_balance(req.usdc_balance);
    Json(serde_json::json!({
        "ok": true,
        "synced_usdc_balance": compounder.available()
    }))
}

fn normalize_v52_config(v52: &mut V52Config) {
    v52.time_phase.early_min_ratio = v52.time_phase.early_min_ratio.clamp(0.11, 0.99);
    v52.time_phase.late_max_ratio = v52.time_phase.late_max_ratio.clamp(0.01, 0.54);
    v52.time_phase.early_size_scale = v52.time_phase.early_size_scale.clamp(0.10, 5.0);
    v52.time_phase.maturity_size_scale = v52.time_phase.maturity_size_scale.clamp(0.10, 5.0);
    v52.time_phase.late_size_scale = v52.time_phase.late_size_scale.clamp(0.10, 5.0);
    if v52.time_phase.late_max_ratio >= v52.time_phase.early_min_ratio {
        v52.time_phase.late_max_ratio = 0.10;
        v52.time_phase.early_min_ratio = 0.55;
    }
    v52.time_phase.allow_timeframes = v52
        .time_phase
        .allow_timeframes
        .iter()
        .map(|s| s.to_ascii_lowercase())
        .filter(|s| s == "5m" || s == "15m")
        .collect::<Vec<_>>();
    if v52.time_phase.allow_timeframes.is_empty() {
        v52.time_phase.allow_timeframes = vec!["5m".to_string(), "15m".to_string()];
    }
    v52.execution.late_force_taker_remaining_ms = v52
        .execution
        .late_force_taker_remaining_ms
        .clamp(1_000, 60_000);
    v52.execution.maker_wait_ms_before_force =
        v52.execution.maker_wait_ms_before_force.clamp(50, 10_000);
    v52.execution.alpha_window_move_bps = v52.execution.alpha_window_move_bps.clamp(0.1, 50.0);
    v52.execution.alpha_window_poll_ms = v52.execution.alpha_window_poll_ms.clamp(1, 200);
    v52.execution.alpha_window_max_wait_ms =
        v52.execution.alpha_window_max_wait_ms.clamp(50, 5_000);
    v52.dual_arb.safety_margin_bps = v52.dual_arb.safety_margin_bps.clamp(0.0, 100.0);
    v52.dual_arb.threshold = v52.dual_arb.threshold.clamp(0.50, 1.10);
    v52.dual_arb.fee_buffer_mode = "conservative_taker".to_string();
}

fn normalize_fusion_mode(raw_mode: &str) -> Option<&'static str> {
    match raw_mode {
        "direct_only" => Some("direct_only"),
        "active_active" => Some("active_active"),
        "hyper_mesh" => Some("hyper_mesh"),
        "udp_only" | "websocket_primary" => Some("hyper_mesh"),
        _ => None,
    }
}

async fn reload_predator_c(
    State(state): State<AppState>,
    Json(req): Json<PredatorCReloadReq>,
) -> Json<PredatorCReloadResp> {
    let mut cfg = state.shared.predator_cfg.write().await;
    if let Some(v) = req.enabled {
        cfg.enabled = v;
    }
    if let Some(v) = req.priority {
        cfg.priority = v;
    }
    if let Some(v) = req.direction_detector {
        cfg.direction_detector = v;
    }
    if let Some(v) = req.taker_sniper {
        cfg.taker_sniper = v;
    }
    if let Some(v) = req.strategy_d {
        cfg.strategy_d = v;
    }
    if let Some(v) = req.regime {
        cfg.regime = v;
    }
    if let Some(v) = req.cross_symbol {
        cfg.cross_symbol = v;
    }
    if let Some(v) = req.router {
        cfg.router = v;
    }
    if let Some(v) = req.compounder {
        cfg.compounder = v;
    }
    if let Some(v) = req.v52 {
        cfg.v52 = v;
    }
    if let Some(v) = req.v52_time_phase {
        cfg.v52.time_phase = v;
    }
    if let Some(v) = req.v52_execution {
        cfg.v52.execution = v;
    }
    if let Some(v) = req.v52_dual_arb {
        cfg.v52.dual_arb = v;
    }
    if let Some(v) = req.v52_reversal {
        cfg.v52.reversal = v;
    }
    normalize_v52_config(&mut cfg.v52);
    let snapshot = cfg.clone();
    drop(cfg);

    state.shadow_stats.set_predator_enabled(snapshot.enabled);
    {
        let mut det = state.shared.predator_direction_detector.write().await;
        det.set_cfg(snapshot.direction_detector.clone());
    }
    {
        let mut sniper = state.shared.predator_taker_sniper.write().await;
        sniper.set_cfg(snapshot.taker_sniper.clone());
    }
    {
        let mut router = state.shared.predator_router.write().await;
        router.set_cfg(snapshot.router.clone());
    }
    {
        let mut compounder = state.shared.predator_compounder.write().await;
        compounder.set_cfg(snapshot.compounder.clone());
    }

    append_jsonl(
        &dataset_path("reports", "predator_c_reload.jsonl"),
        &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "predator_c": snapshot}),
    );

    Json(PredatorCReloadResp {
        predator_c: snapshot,
    })
}

async fn reload_regime(
    State(state): State<AppState>,
    Json(regime): Json<PredatorRegimeConfig>,
) -> Json<PredatorRegimeConfig> {
    let mut cfg = state.shared.predator_cfg.write().await;
    cfg.regime = regime.clone();
    let snapshot = cfg.regime.clone();
    drop(cfg);

    append_jsonl(
        &dataset_path("reports", "regime_reload.jsonl"),
        &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "regime": snapshot}),
    );

    Json(regime)
}

async fn reload_fusion(
    State(state): State<AppState>,
    Json(req): Json<FusionReloadReq>,
) -> Json<FusionConfig> {
    let mut cfg = state.shared.fusion_cfg.write().await;
    if let Some(v) = req.enable_udp {
        cfg.enable_udp = v;
    }
    if let Some(v) = req.mode {
        let raw = v.to_ascii_lowercase();
        if let Some(norm) = normalize_fusion_mode(raw.as_str()) {
            if norm != raw {
                tracing::warn!(
                    requested_mode = %raw,
                    normalized_mode = %norm,
                    "legacy fusion mode alias normalized"
                );
            }
            cfg.mode = norm.to_string();
        } else {
            tracing::warn!(requested_mode = %raw, "invalid fusion mode ignored");
        }
    }
    if let Some(v) = req.udp_port {
        cfg.udp_port = v.max(1);
    }
    if let Some(v) = req.dedupe_window_ms {
        cfg.dedupe_window_ms = v.clamp(0, 2_000);
    }
    if let Some(v) = req.dedupe_price_bps {
        cfg.dedupe_price_bps = v.clamp(0.0, 50.0);
    }
    if let Some(v) = req.udp_share_cap {
        cfg.udp_share_cap = v.clamp(0.05, 0.95);
    }
    if let Some(v) = req.jitter_threshold_ms {
        cfg.jitter_threshold_ms = v.clamp(1.0, 2_000.0);
    }
    if let Some(v) = req.fallback_arm_duration_ms {
        cfg.fallback_arm_duration_ms = v.clamp(200, 15_000);
    }
    if let Some(v) = req.fallback_cooldown_sec {
        cfg.fallback_cooldown_sec = v.clamp(0, 3_600);
    }
    if let Some(v) = req.udp_local_only {
        cfg.udp_local_only = v;
    }
    if let Some(v) = req.udp_trigger_enabled {
        cfg.udp_trigger_enabled = v;
    }
    if let Some(v) = req.udp_trigger_port {
        cfg.udp_trigger_port = v.max(1);
    }
    if let Some(ref v) = req.udp_trigger_target {
        cfg.udp_trigger_target = v.clone();
    }

    let snapshot = cfg.clone();
    std::env::set_var(
        "POLYEDGE_UDP_LOCAL_ONLY",
        if snapshot.udp_local_only {
            "true"
        } else {
            "false"
        },
    );
    append_jsonl(
        &dataset_path("reports", "fusion_reload.jsonl"),
        &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "fusion": snapshot}),
    );
    Json(snapshot)
}

async fn reload_edge_model(
    State(state): State<AppState>,
    Json(req): Json<EdgeModelReloadReq>,
) -> Json<EdgeModelConfig> {
    let mut cfg = state.shared.edge_model_cfg.write().await;
    if let Some(v) = req.model {
        cfg.model = v;
    }
    if let Some(v) = req.gate_mode {
        cfg.gate_mode = v;
    }
    if let Some(v) = req.version {
        cfg.version = v;
    }
    if let Some(v) = req.base_gate_bps {
        cfg.base_gate_bps = v.max(0.0);
    }
    if let Some(v) = req.congestion_penalty_bps {
        cfg.congestion_penalty_bps = v.max(0.0);
    }
    if let Some(v) = req.latency_penalty_bps {
        cfg.latency_penalty_bps = v.max(0.0);
    }
    if let Some(v) = req.fail_cost_bps {
        cfg.fail_cost_bps = v.max(0.0);
    }
    let snapshot = cfg.clone();
    append_jsonl(
        &dataset_path("reports", "edge_model_reload.jsonl"),
        &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "edge_model": snapshot}),
    );
    Json(snapshot)
}

async fn reload_probability(
    State(state): State<AppState>,
    Json(req): Json<ProbabilityReloadReq>,
) -> Json<ProbabilityEngineConfig> {
    let mut engine = state.shared.predator_probability_engine.write().await;
    let mut cfg = engine.cfg().clone();
    if let Some(v) = req.momentum_gain {
        cfg.momentum_gain = v.clamp(0.0, 20.0);
    }
    if let Some(v) = req.lag_penalty_per_ms {
        cfg.lag_penalty_per_ms = v.clamp(0.0, 0.1);
    }
    if let Some(v) = req.confidence_floor {
        cfg.confidence_floor = v.clamp(0.0, 1.0);
    }
    if let Some(v) = req.sigma_annual {
        cfg.sigma_annual = v.clamp(0.05, 5.0);
    }
    if let Some(v) = req.horizon_sec {
        cfg.horizon_sec = v.clamp(1.0, 900.0);
    }
    if let Some(v) = req.drift_annual {
        cfg.drift_annual = v.clamp(-10.0, 10.0);
    }
    if let Some(v) = req.velocity_drift_gain {
        cfg.velocity_drift_gain = v.clamp(0.0, 5.0);
    }
    if let Some(v) = req.acceleration_drift_gain {
        cfg.acceleration_drift_gain = v.clamp(0.0, 5.0);
    }
    if let Some(v) = req.fair_blend_weight {
        cfg.fair_blend_weight = v.clamp(0.0, 1.0);
    }
    engine.set_cfg(cfg.clone());
    drop(engine);
    append_jsonl(
        &dataset_path("reports", "probability_reload.jsonl"),
        &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "probability": cfg}),
    );
    Json(cfg)
}

async fn reload_source_health(
    State(state): State<AppState>,
    Json(req): Json<SourceHealthReloadReq>,
) -> Json<SourceHealthConfig> {
    let mut cfg = state.shared.source_health_cfg.write().await;
    if let Some(v) = req.min_samples {
        cfg.min_samples = v.max(1);
    }
    if let Some(v) = req.gap_window_ms {
        cfg.gap_window_ms = v.clamp(50, 60_000);
    }
    if let Some(v) = req.jitter_limit_ms {
        cfg.jitter_limit_ms = v.clamp(0.1, 2_000.0);
    }
    if let Some(v) = req.deviation_limit_bps {
        cfg.deviation_limit_bps = v.clamp(0.1, 10_000.0);
    }
    if let Some(v) = req.freshness_limit_ms {
        cfg.freshness_limit_ms = v.clamp(50.0, 60_000.0);
    }
    if let Some(v) = req.min_score_for_trading {
        cfg.min_score_for_trading = v.clamp(0.0, 1.0);
    }
    let snapshot = cfg.clone();
    append_jsonl(
        &dataset_path("reports", "source_health_reload.jsonl"),
        &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "source_health": snapshot}),
    );
    Json(snapshot)
}

async fn reload_exit(
    State(state): State<AppState>,
    Json(req): Json<ExitReloadReq>,
) -> Json<ExitConfig> {
    let mut cfg = state.shared.exit_cfg.write().await;
    if let Some(v) = req.enabled {
        cfg.enabled = v;
    }
    if let Some(v) = req.t100ms_reversal_bps {
        cfg.t100ms_reversal_bps = v;
    }
    if let Some(v) = req.t300ms_reversal_bps {
        cfg.t300ms_reversal_bps = v;
    }
    if let Some(v) = req.convergence_exit_ratio {
        cfg.convergence_exit_ratio = v.clamp(0.0, 1.0);
    }
    if let Some(v) = req.time_stop_ms {
        cfg.time_stop_ms = v.clamp(50, 60_000);
    }
    if let Some(v) = req.edge_decay_bps {
        cfg.edge_decay_bps = v;
    }
    if let Some(v) = req.adverse_move_bps {
        cfg.adverse_move_bps = v;
    }
    if let Some(v) = req.flatten_on_trigger {
        cfg.flatten_on_trigger = v;
    }
    if let Some(v) = req.t3_take_ratio {
        cfg.t3_take_ratio = v.clamp(0.0, 5.0);
    }
    if let Some(v) = req.t15_min_unrealized_usdc {
        cfg.t15_min_unrealized_usdc = v;
    }
    if let Some(v) = req.t60_true_prob_floor {
        cfg.t60_true_prob_floor = v.clamp(0.0, 1.0);
    }
    if let Some(v) = req.t300_force_exit_ms {
        cfg.t300_force_exit_ms = v.clamp(1_000, 1_800_000);
    }
    if let Some(v) = req.t300_hold_prob_threshold {
        cfg.t300_hold_prob_threshold = v.clamp(0.0, 1.0);
    }
    if let Some(v) = req.t300_hold_time_to_expiry_ms {
        cfg.t300_hold_time_to_expiry_ms = v.clamp(1_000, 1_800_000);
    }
    if let Some(v) = req.max_single_trade_loss_usdc {
        cfg.max_single_trade_loss_usdc = v.max(0.0);
    }
    let manager_cfg = to_exit_manager_config(&cfg);
    state
        .shared
        .predator_exit_manager
        .write()
        .await
        .set_cfg(manager_cfg);
    let snapshot = cfg.clone();
    append_jsonl(
        &dataset_path("reports", "exit_reload.jsonl"),
        &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "exit": snapshot}),
    );
    Json(snapshot)
}

async fn report_direction(State(state): State<AppState>) -> Json<serde_json::Value> {
    let now = Utc::now().timestamp_millis();
    let latest = state.shared.predator_latest_direction.read().await.clone();
    Json(serde_json::json!({"ts_ms": now, "latest": latest}))
}

async fn report_router(State(state): State<AppState>) -> Json<serde_json::Value> {
    let now = Utc::now().timestamp_millis();
    let mut router = state.shared.predator_router.write().await;
    let locks = router.snapshot_locks(now);
    let locked_by_tf = router.locked_by_tf_usdc(now);
    let mut locked_by_tf_usdc = HashMap::<String, f64>::new();
    for (tf, v) in locked_by_tf {
        locked_by_tf_usdc.insert(tf.to_string(), v);
    }
    Json(serde_json::json!({
        "ts_ms": now,
        "locks": locks,
        "locked_by_tf_usdc": locked_by_tf_usdc,
        "active_positions": router.active_positions(now),
        "locked_total_usdc": router.locked_total_usdc(now)
    }))
}

async fn report_capital(State(state): State<AppState>) -> Json<serde_json::Value> {
    let now = Utc::now().timestamp_millis();
    let compounder = state.shared.predator_compounder.read().await;
    let cfg = compounder.cfg().clone();
    Json(serde_json::json!({
        "ts_ms": now,
        "cfg": cfg,
        "available_usdc": compounder.available(),
        "total_pnl_usdc": compounder.total_pnl(),
        "daily_pnl_usdc": compounder.daily_pnl(),
        "halted": compounder.halted(),
        "win_rate": compounder.win_rate(),
        "recommended_quote_notional_usdc": compounder.recommended_quote_notional_usdc(),
    }))
}

async fn reload_strategy(
    State(state): State<AppState>,
    Json(req): Json<StrategyReloadReq>,
) -> Json<StrategyReloadResp> {
    let cur = state.strategy_cfg.read().await.clone();
    let mut next = (*cur).clone();
    if let Some(v) = req.min_edge_bps {
        next.min_edge_bps = v.max(0.0);
    }
    if let Some(v) = req.ttl_ms {
        next.ttl_ms = v.max(50);
    }
    if let Some(v) = req.inventory_skew {
        next.inventory_skew = v.clamp(0.0, 1.0);
    }
    if let Some(v) = req.base_quote_size {
        next.base_quote_size = v.max(0.01);
    }
    if let Some(v) = req.max_spread {
        next.max_spread = v.max(0.0001);
    }
    if let Some(v) = req.taker_trigger_bps {
        next.taker_trigger_bps = v.max(0.0);
    }
    if let Some(v) = req.taker_max_slippage_bps {
        next.taker_max_slippage_bps = v.max(0.0);
    }
    if let Some(v) = req.stale_tick_filter_ms {
        next.stale_tick_filter_ms = v.clamp(50.0, 5_000.0);
    }
    if let Some(v) = req.capital_fraction_kelly {
        next.capital_fraction_kelly = v.clamp(0.01, 1.0);
    }
    if let Some(v) = req.variance_penalty_lambda {
        next.variance_penalty_lambda = v.clamp(0.0, 5.0);
    }
    if let Some(v) = req.min_eval_notional_usdc {
        next.min_eval_notional_usdc = v.max(0.0);
    }
    if let Some(v) = req.min_expected_edge_usdc {
        next.min_expected_edge_usdc = v.max(0.0);
    }
    let v52_cfg = {
        let mut predator_cfg = state.shared.predator_cfg.write().await;
        if let Some(v) = req.v52 {
            predator_cfg.v52 = v;
        }
        if let Some(v) = req.v52_time_phase {
            predator_cfg.v52.time_phase = v;
        }
        if let Some(v) = req.v52_execution {
            predator_cfg.v52.execution = v;
        }
        if let Some(v) = req.v52_dual_arb {
            predator_cfg.v52.dual_arb = v;
        }
        if let Some(v) = req.v52_reversal {
            predator_cfg.v52.reversal = v;
        }
        normalize_v52_config(&mut predator_cfg.v52);
        predator_cfg.v52.clone()
    };
    if let Some(v) = req.market_tier_profile {
        next.market_tier_profile = v;
    }
    let mut fair_cfg = state
        .fair_value_cfg
        .read()
        .map(|g| g.clone())
        .unwrap_or_else(|_| BasisMrConfig::default());
    if let Some(v) = req.basis_k_revert {
        fair_cfg.k_revert = v.clamp(0.0, 5.0);
    }
    if let Some(v) = req.basis_z_cap {
        fair_cfg.z_cap = v.clamp(0.5, 8.0);
    }
    if let Some(v) = req.basis_min_confidence {
        fair_cfg.min_confidence = v.clamp(0.0, 1.0);
    }
    if let Ok(mut guard) = state.fair_value_cfg.write() {
        *guard = fair_cfg.clone();
    }
    *state.strategy_cfg.write().await = std::sync::Arc::new(next.clone());
    let maker_cfg = next;
    append_jsonl(
        &dataset_path("reports", "strategy_reload.jsonl"),
        &serde_json::json!({
            "ts_ms": Utc::now().timestamp_millis(),
            "maker": maker_cfg,
            "fair_value": fair_cfg,
            "v52": v52_cfg
        }),
    );
    Json(StrategyReloadResp {
        maker: maker_cfg,
        fair_value: fair_cfg,
        v52: v52_cfg,
    })
}

async fn reload_taker(
    State(state): State<AppState>,
    Json(req): Json<TakerReloadReq>,
) -> Json<TakerReloadResp> {
    let cur = state.strategy_cfg.read().await.clone();
    let mut next = (*cur).clone();
    if let Some(v) = req.trigger_bps {
        next.taker_trigger_bps = v.max(0.0);
    }
    if let Some(v) = req.max_slippage_bps {
        next.taker_max_slippage_bps = v.max(0.0);
    }
    if let Some(v) = req.stale_tick_filter_ms {
        next.stale_tick_filter_ms = v.clamp(50.0, 5_000.0);
    }
    if let Some(v) = req.market_tier_profile {
        next.market_tier_profile = v;
    }
    *state.strategy_cfg.write().await = std::sync::Arc::new(next.clone());
    let resp = TakerReloadResp {
        trigger_bps: next.taker_trigger_bps,
        max_slippage_bps: next.taker_max_slippage_bps,
        stale_tick_filter_ms: next.stale_tick_filter_ms,
        market_tier_profile: next.market_tier_profile.clone(),
    };
    append_jsonl(
        &dataset_path("reports", "taker_reload.jsonl"),
        &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "taker": resp}),
    );
    Json(resp)
}

async fn reload_allocator(
    State(state): State<AppState>,
    Json(req): Json<AllocatorReloadReq>,
) -> Json<AllocatorReloadResp> {
    let mut allocator = state.allocator_cfg.write().await;
    if let Some(v) = req.capital_fraction_kelly {
        allocator.capital_fraction_kelly = v.clamp(0.01, 1.0);
    }
    if let Some(v) = req.variance_penalty_lambda {
        allocator.variance_penalty_lambda = v.clamp(0.0, 5.0);
    }
    if let Some(v) = req.active_top_n_markets {
        allocator.active_top_n_markets = v.clamp(1, 128);
    }
    if let Some(v) = req.taker_weight {
        allocator.taker_weight = v.max(0.0);
    }
    if let Some(v) = req.maker_weight {
        allocator.maker_weight = v.max(0.0);
    }
    if let Some(v) = req.arb_weight {
        allocator.arb_weight = v.max(0.0);
    }
    let sum = allocator.taker_weight + allocator.maker_weight + allocator.arb_weight;
    if sum > 0.0 {
        allocator.taker_weight /= sum;
        allocator.maker_weight /= sum;
        allocator.arb_weight /= sum;
    } else {
        *allocator = AllocatorConfig::default();
    }

    {
        let cur = state.strategy_cfg.read().await.clone();
        let mut next = (*cur).clone();
        next.capital_fraction_kelly = allocator.capital_fraction_kelly;
        next.variance_penalty_lambda = allocator.variance_penalty_lambda;
        *state.strategy_cfg.write().await = std::sync::Arc::new(next);
    }
    {
        let cur = state.toxicity_cfg.read().await.clone();
        let mut next = (*cur).clone();
        next.active_top_n_markets = allocator.active_top_n_markets;
        *state.toxicity_cfg.write().await = std::sync::Arc::new(next);
    }

    let resp = AllocatorReloadResp {
        allocator: allocator.clone(),
    };
    append_jsonl(
        &dataset_path("reports", "allocator_reload.jsonl"),
        &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "allocator": resp.allocator}),
    );
    Json(resp)
}

async fn reload_risk(
    State(state): State<AppState>,
    Json(req): Json<RiskReloadReq>,
) -> Json<RiskReloadResp> {
    let mut cfg = state.risk_limits.write().unwrap_or_else(|e| e.into_inner());
    if let Some(v) = req.max_market_notional {
        cfg.max_market_notional = v.max(0.0);
    }
    if let Some(v) = req.max_asset_notional {
        cfg.max_asset_notional = v.max(0.0);
    }
    if let Some(v) = req.max_open_orders {
        cfg.max_open_orders = v.max(1);
    }
    if let Some(v) = req.daily_drawdown_cap_pct {
        cfg.max_drawdown_pct = v.clamp(0.001, 1.0);
    }
    if let Some(v) = req.max_loss_streak {
        cfg.max_loss_streak = v.max(1);
    }
    if let Some(v) = req.cooldown_sec {
        cfg.cooldown_sec = v.max(1);
    }
    if let Some(v) = req.progressive_enabled {
        cfg.progressive_enabled = v;
    }
    if let Some(v) = req.drawdown_tier1_ratio {
        cfg.drawdown_tier1_ratio = v.clamp(0.05, 0.99);
    }
    if let Some(v) = req.drawdown_tier2_ratio {
        cfg.drawdown_tier2_ratio = v.clamp(cfg.drawdown_tier1_ratio, 0.999);
    }
    if let Some(v) = req.tier1_size_scale {
        cfg.tier1_size_scale = v.clamp(0.01, 1.0);
    }
    if let Some(v) = req.tier2_size_scale {
        cfg.tier2_size_scale = v.clamp(0.01, 1.0);
    }
    let snapshot = cfg.clone();
    append_jsonl(
        &dataset_path("reports", "risk_reload.jsonl"),
        &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "risk": snapshot}),
    );
    Json(RiskReloadResp { risk: snapshot })
}

async fn reload_toxicity(
    State(state): State<AppState>,
    Json(req): Json<ToxicityReloadReq>,
) -> Json<ToxicityConfig> {
    let cur = state.toxicity_cfg.read().await.clone();
    let mut next = (*cur).clone();
    if let Some(v) = req.safe_threshold {
        next.safe_threshold = v.clamp(0.0, 1.0);
    }
    if let Some(v) = req.caution_threshold {
        next.caution_threshold = v.clamp(0.0, 1.0);
    }
    if let Some(v) = req.cooldown_min_sec {
        next.cooldown_min_sec = v.max(1);
    }
    if let Some(v) = req.cooldown_max_sec {
        next.cooldown_max_sec = v.max(next.cooldown_min_sec);
    }
    if let Some(v) = req.min_market_score {
        next.min_market_score = v.clamp(0.0, 100.0);
    }
    if let Some(v) = req.active_top_n_markets {
        next.active_top_n_markets = v;
    }
    if let Some(v) = req.markout_1s_caution_bps {
        next.markout_1s_caution_bps = v;
    }
    if let Some(v) = req.markout_5s_caution_bps {
        next.markout_5s_caution_bps = v;
    }
    if let Some(v) = req.markout_10s_caution_bps {
        next.markout_10s_caution_bps = v;
    }
    if let Some(v) = req.markout_1s_danger_bps {
        next.markout_1s_danger_bps = v;
    }
    if let Some(v) = req.markout_5s_danger_bps {
        next.markout_5s_danger_bps = v;
    }
    if let Some(v) = req.markout_10s_danger_bps {
        next.markout_10s_danger_bps = v;
    }
    if next.safe_threshold > next.caution_threshold {
        std::mem::swap(&mut next.safe_threshold, &mut next.caution_threshold);
    }
    if next.markout_1s_caution_bps < next.markout_1s_danger_bps {
        std::mem::swap(
            &mut next.markout_1s_caution_bps,
            &mut next.markout_1s_danger_bps,
        );
    }
    if next.markout_5s_caution_bps < next.markout_5s_danger_bps {
        std::mem::swap(
            &mut next.markout_5s_caution_bps,
            &mut next.markout_5s_danger_bps,
        );
    }
    if next.markout_10s_caution_bps < next.markout_10s_danger_bps {
        std::mem::swap(
            &mut next.markout_10s_caution_bps,
            &mut next.markout_10s_danger_bps,
        );
    }
    *state.toxicity_cfg.write().await = std::sync::Arc::new(next.clone());
    append_jsonl(
        &dataset_path("reports", "toxicity_reload.jsonl"),
        &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "config": next}),
    );
    Json(next)
}

async fn reload_perf_profile(
    State(state): State<AppState>,
    Json(req): Json<PerfProfileReloadReq>,
) -> Json<PerfProfile> {
    let mut cfg = state.perf_profile.write().await;
    if let Some(v) = req.tail_guard {
        cfg.tail_guard = v.clamp(0.50, 0.9999);
    }
    if let Some(v) = req.io_flush_batch {
        cfg.io_flush_batch = v.clamp(1, 4096);
    }
    if let Some(v) = req.io_queue_capacity {
        cfg.io_queue_capacity = v.clamp(256, 262_144);
    }
    if let Some(v) = req.json_mode {
        cfg.json_mode = v;
    }
    if let Some(v) = req.io_drop_on_full {
        cfg.io_drop_on_full = v;
        JSONL_DROP_ON_FULL.store(v, Ordering::Relaxed);
    }
    append_jsonl(
        &dataset_path("reports", "perf_profile_reload.jsonl"),
        &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "config": *cfg}),
    );
    Json(cfg.clone())
}

async fn report_shadow_live(State(state): State<AppState>) -> impl IntoResponse {
    let mut live = state.shadow_stats.build_live_report().await;
    live.edge_model_version = state.shared.edge_model_cfg.read().await.version.clone();
    {
        let map = state.shared.source_health_latest.read().await;
        let mut rows = map.values().cloned().collect::<Vec<_>>();
        rows.sort_by(|a, b| b.score.total_cmp(&a.score));
        live.source_health = rows;
    }
    persist_live_report_files(&live);
    let market_selection = build_market_selection_snapshot(&state).await;
    let mut payload = serde_json::to_value(live).unwrap_or_else(|_| serde_json::json!({}));
    if let Some(obj) = payload.as_object_mut() {
        obj.insert("market_selection".to_string(), market_selection);
    }
    Json(payload)
}

async fn report_shadow_final(State(state): State<AppState>) -> Json<ShadowFinalReport> {
    let mut final_report = state.shadow_stats.build_final_report().await;
    final_report.live.edge_model_version = state.shared.edge_model_cfg.read().await.version.clone();
    {
        let map = state.shared.source_health_latest.read().await;
        let mut rows = map.values().cloned().collect::<Vec<_>>();
        rows.sort_by(|a, b| b.score.total_cmp(&a.score));
        final_report.live.source_health = rows;
    }
    persist_final_report_files(&final_report);
    Json(final_report)
}

async fn report_pnl_by_engine(State(state): State<AppState>) -> Json<EnginePnlReport> {
    let report = state.shadow_stats.build_engine_pnl_report().await;
    persist_engine_pnl_report(&report);
    Json(report)
}

async fn report_toxicity_live(State(state): State<AppState>) -> Json<ToxicityLiveReport> {
    let live = build_toxicity_live_report(
        state.tox_state.clone(),
        state.shadow_stats.clone(),
        state.execution.clone(),
        state.toxicity_cfg.clone(),
    )
    .await;
    persist_toxicity_report_files(&live);
    Json(live)
}

async fn report_toxicity_final(State(state): State<AppState>) -> Json<ToxicityFinalReport> {
    let cfg = state.toxicity_cfg.read().await.clone();
    let live = build_toxicity_live_report(
        state.tox_state.clone(),
        state.shadow_stats.clone(),
        state.execution.clone(),
        state.toxicity_cfg.clone(),
    )
    .await;
    let mut failed = Vec::new();
    if live
        .rows
        .iter()
        .filter(|r| r.active_for_quoting)
        .any(|r| r.regime == ToxicRegime::Danger || r.market_score < cfg.min_market_score)
    {
        failed.push("active_market_danger_or_low_score_present".to_string());
    }
    if live.average_tox_score > 0.65 {
        failed.push("average_tox_score_above_0.65".to_string());
    }
    let p50_markout = percentile(
        &live
            .rows
            .iter()
            .map(|r| r.markout_10s_bps)
            .collect::<Vec<_>>(),
        0.50,
    )
    .unwrap_or(0.0);
    let p25_markout = percentile(
        &live
            .rows
            .iter()
            .map(|r| r.markout_10s_bps)
            .collect::<Vec<_>>(),
        0.25,
    )
    .unwrap_or(0.0);
    if p50_markout <= 0.0 {
        failed.push(format!("pnl_10s_p50_bps {:.4} <= 0", p50_markout));
    }
    if p25_markout <= -20.0 {
        failed.push(format!("pnl_10s_p25_bps {:.4} <= -20", p25_markout));
    }
    let pass = failed.is_empty();
    let final_report = ToxicityFinalReport {
        pass,
        failed_reasons: failed,
        live,
    };
    persist_toxicity_report_files(&final_report.live);
    Json(final_report)
}
