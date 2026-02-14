use super::*;

pub(super) fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/state/positions", get(positions))
        .route("/state/pnl", get(pnl))
        .route("/report/shadow/live", get(report_shadow_live))
        .route("/report/shadow/final", get(report_shadow_final))
        .route("/report/pnl/by_engine", get(report_pnl_by_engine))
        .route("/report/toxicity/live", get(report_toxicity_live))
        .route("/report/toxicity/final", get(report_toxicity_final))
        .route("/control/pause", post(pause))
        .route("/control/resume", post(resume))
        .route("/control/flatten", post(flatten))
        .route("/control/reset_shadow", post(reset_shadow))
        .route("/control/reload_strategy", post(reload_strategy))
        .route("/control/reload_taker", post(reload_taker))
        .route("/control/reload_allocator", post(reload_allocator))
        .route("/control/reload_toxicity", post(reload_toxicity))
        .route("/control/reload_risk", post(reload_risk))
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
    let _ = state
        .bus
        .publish(EngineEvent::Control(ControlCommand::Pause));
    Json(serde_json::json!({"ok": true, "paused": true}))
}

async fn resume(State(state): State<AppState>) -> impl IntoResponse {
    *state.paused.write().await = false;
    let _ = state
        .bus
        .publish(EngineEvent::Control(ControlCommand::Resume));
    Json(serde_json::json!({"ok": true, "paused": false}))
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

async fn reset_shadow(State(state): State<AppState>) -> impl IntoResponse {
    let window_id = state.shadow_stats.reset().await;
    state.tox_state.write().await.clear();
    Json(serde_json::json!({"ok": true, "shadow_reset": true, "window_id": window_id}))
}

async fn reload_strategy(
    State(state): State<AppState>,
    Json(req): Json<StrategyReloadReq>,
) -> Json<StrategyReloadResp> {
    let mut cfg = state.strategy_cfg.write().await;
    if let Some(v) = req.min_edge_bps {
        cfg.min_edge_bps = v.max(0.0);
    }
    if let Some(v) = req.ttl_ms {
        cfg.ttl_ms = v.max(50);
    }
    if let Some(v) = req.inventory_skew {
        cfg.inventory_skew = v.clamp(0.0, 1.0);
    }
    if let Some(v) = req.base_quote_size {
        cfg.base_quote_size = v.max(0.01);
    }
    if let Some(v) = req.max_spread {
        cfg.max_spread = v.max(0.0001);
    }
    if let Some(v) = req.taker_trigger_bps {
        cfg.taker_trigger_bps = v.max(0.0);
    }
    if let Some(v) = req.taker_max_slippage_bps {
        cfg.taker_max_slippage_bps = v.max(0.0);
    }
    if let Some(v) = req.stale_tick_filter_ms {
        cfg.stale_tick_filter_ms = v.clamp(50.0, 5_000.0);
    }
    if let Some(v) = req.market_tier_profile {
        cfg.market_tier_profile = v;
    }
    if let Some(v) = req.capital_fraction_kelly {
        cfg.capital_fraction_kelly = v.clamp(0.01, 1.0);
    }
    if let Some(v) = req.variance_penalty_lambda {
        cfg.variance_penalty_lambda = v.clamp(0.0, 5.0);
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
    let maker_cfg = cfg.clone();
    drop(cfg);
    append_jsonl(
        &dataset_path("reports", "strategy_reload.jsonl"),
        &serde_json::json!({
            "ts_ms": Utc::now().timestamp_millis(),
            "maker": maker_cfg,
            "fair_value": fair_cfg
        }),
    );
    Json(StrategyReloadResp {
        maker: maker_cfg,
        fair_value: fair_cfg,
    })
}

async fn reload_taker(
    State(state): State<AppState>,
    Json(req): Json<TakerReloadReq>,
) -> Json<TakerReloadResp> {
    let mut cfg = state.strategy_cfg.write().await;
    if let Some(v) = req.trigger_bps {
        cfg.taker_trigger_bps = v.max(0.0);
    }
    if let Some(v) = req.max_slippage_bps {
        cfg.taker_max_slippage_bps = v.max(0.0);
    }
    if let Some(v) = req.stale_tick_filter_ms {
        cfg.stale_tick_filter_ms = v.clamp(50.0, 5_000.0);
    }
    if let Some(v) = req.market_tier_profile {
        cfg.market_tier_profile = v;
    }
    let resp = TakerReloadResp {
        trigger_bps: cfg.taker_trigger_bps,
        max_slippage_bps: cfg.taker_max_slippage_bps,
        stale_tick_filter_ms: cfg.stale_tick_filter_ms,
        market_tier_profile: cfg.market_tier_profile.clone(),
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
        let mut strategy = state.strategy_cfg.write().await;
        strategy.capital_fraction_kelly = allocator.capital_fraction_kelly;
        strategy.variance_penalty_lambda = allocator.variance_penalty_lambda;
    }
    {
        let mut tox = state.toxicity_cfg.write().await;
        tox.active_top_n_markets = allocator.active_top_n_markets;
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
    let mut cfg = state.risk_limits.write().await;
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
    append_jsonl(
        &dataset_path("reports", "risk_reload.jsonl"),
        &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "risk": *cfg}),
    );
    Json(RiskReloadResp { risk: cfg.clone() })
}

async fn reload_toxicity(
    State(state): State<AppState>,
    Json(req): Json<ToxicityReloadReq>,
) -> Json<ToxicityConfig> {
    let mut cfg = state.toxicity_cfg.write().await;
    if let Some(v) = req.safe_threshold {
        cfg.safe_threshold = v.clamp(0.0, 1.0);
    }
    if let Some(v) = req.caution_threshold {
        cfg.caution_threshold = v.clamp(0.0, 1.0);
    }
    if let Some(v) = req.cooldown_min_sec {
        cfg.cooldown_min_sec = v.max(1);
    }
    if let Some(v) = req.cooldown_max_sec {
        cfg.cooldown_max_sec = v.max(cfg.cooldown_min_sec);
    }
    if let Some(v) = req.min_market_score {
        cfg.min_market_score = v.clamp(0.0, 100.0);
    }
    if let Some(v) = req.active_top_n_markets {
        cfg.active_top_n_markets = v;
    }
    if let Some(v) = req.markout_1s_caution_bps {
        cfg.markout_1s_caution_bps = v;
    }
    if let Some(v) = req.markout_5s_caution_bps {
        cfg.markout_5s_caution_bps = v;
    }
    if let Some(v) = req.markout_10s_caution_bps {
        cfg.markout_10s_caution_bps = v;
    }
    if let Some(v) = req.markout_1s_danger_bps {
        cfg.markout_1s_danger_bps = v;
    }
    if let Some(v) = req.markout_5s_danger_bps {
        cfg.markout_5s_danger_bps = v;
    }
    if let Some(v) = req.markout_10s_danger_bps {
        cfg.markout_10s_danger_bps = v;
    }
    if cfg.safe_threshold > cfg.caution_threshold {
        let safe = cfg.safe_threshold;
        cfg.safe_threshold = cfg.caution_threshold;
        cfg.caution_threshold = safe;
    }
    if cfg.markout_1s_caution_bps < cfg.markout_1s_danger_bps {
        let v = cfg.markout_1s_caution_bps;
        cfg.markout_1s_caution_bps = cfg.markout_1s_danger_bps;
        cfg.markout_1s_danger_bps = v;
    }
    if cfg.markout_5s_caution_bps < cfg.markout_5s_danger_bps {
        let v = cfg.markout_5s_caution_bps;
        cfg.markout_5s_caution_bps = cfg.markout_5s_danger_bps;
        cfg.markout_5s_danger_bps = v;
    }
    if cfg.markout_10s_caution_bps < cfg.markout_10s_danger_bps {
        let v = cfg.markout_10s_caution_bps;
        cfg.markout_10s_caution_bps = cfg.markout_10s_danger_bps;
        cfg.markout_10s_danger_bps = v;
    }
    append_jsonl(
        &dataset_path("reports", "toxicity_reload.jsonl"),
        &serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "config": *cfg}),
    );
    Json(cfg.clone())
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

async fn report_shadow_live(State(state): State<AppState>) -> Json<ShadowLiveReport> {
    let live = state.shadow_stats.build_live_report().await;
    persist_live_report_files(&live);
    Json(live)
}

async fn report_shadow_final(State(state): State<AppState>) -> Json<ShadowFinalReport> {
    let final_report = state.shadow_stats.build_final_report().await;
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
