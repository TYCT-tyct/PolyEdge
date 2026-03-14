use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StrategyPaperSource {
    Replay,
    Live,
    Auto,
}

fn parse_strategy_paper_source(raw: Option<&str>) -> StrategyPaperSource {
    match raw
        .map(|v| v.trim().to_ascii_lowercase())
        .unwrap_or_else(|| "replay".to_string())
        .as_str()
    {
        "live" => StrategyPaperSource::Live,
        "auto" => StrategyPaperSource::Auto,
        _ => StrategyPaperSource::Replay,
    }
}

fn strategy_response_view_meta(view_family: &str, view_label: &str) -> Value {
    json!({
        "family": view_family,
        "label": view_label,
    })
}

fn runtime_view_meta(
    forced_view_meta: Option<(&'static str, &'static str)>,
) -> (&'static str, &'static str) {
    forced_view_meta.unwrap_or(("runtime", "Runtime"))
}

fn build_runtime_bucketed_payload(
    runtime_payload: &Value,
    forced_view_meta: Option<(&'static str, &'static str)>,
    cfg: &StrategyRuntimeConfig,
    config_source: &str,
    baseline_profile: &str,
    config_resolution: Value,
    lookback_minutes: u32,
    max_points: u32,
    samples: &[StrategySample],
    run: Option<&StrategySimulationResult>,
) -> Value {
    let (view_family, view_label) = runtime_view_meta(forced_view_meta);
    let runtime_defaults = LiveRuntimeConfig::from_env();
    let fixed_guard = strategy_fixed_guard_payload(cfg, config_source);
    let current = run
        .map(|v| v.current.clone())
        .or_else(|| runtime_payload.get("current").cloned())
        .unwrap_or(Value::Null);
    let summary = if let Some(v) = run {
        json!({
            "trade_count": v.trade_count,
            "win_rate_pct": v.win_rate_pct,
            "avg_pnl_cents": v.avg_pnl_cents,
            "avg_duration_s": v.avg_duration_s,
            "total_pnl_cents": v.total_pnl_cents,
            "net_pnl_cents": v.net_pnl_cents,
            "gross_pnl_cents": v.gross_pnl_cents,
            "total_cost_cents": v.total_cost_cents,
            "total_entry_fee_cents": v.total_entry_fee_cents,
            "total_exit_fee_cents": v.total_exit_fee_cents,
            "total_slippage_cents": v.total_slippage_cents,
            "total_impact_cents": v.total_impact_cents,
            "net_margin_pct": v.net_margin_pct,
            "max_drawdown_cents": v.max_drawdown_cents,
            "max_profit_trade_cents": v.max_profit_trade_cents,
            "blocked_exits": v.blocked_exits,
            "emergency_wide_exit_count": v.emergency_wide_exit_count,
            "execution_penalty_cents_total": v.execution_penalty_cents_total,
        })
    } else {
        json!({
            "trade_count": 0,
            "win_rate_pct": 0.0,
            "avg_pnl_cents": 0.0,
            "avg_duration_s": 0.0,
            "total_pnl_cents": 0.0,
            "net_pnl_cents": 0.0,
            "gross_pnl_cents": 0.0,
            "total_cost_cents": 0.0,
            "total_entry_fee_cents": 0.0,
            "total_exit_fee_cents": 0.0,
            "total_slippage_cents": 0.0,
            "total_impact_cents": 0.0,
            "net_margin_pct": 0.0,
            "max_drawdown_cents": 0.0,
            "max_profit_trade_cents": 0.0,
            "blocked_exits": 0,
            "emergency_wide_exit_count": 0,
            "execution_penalty_cents_total": 0.0,
        })
    };
    let trades = run.map(|v| v.trades.clone()).unwrap_or_default();
    let signal_decisions = run.map(|v| v.signal_decisions.clone()).unwrap_or_default();

    json!({
        "source": "runtime",
        "view": strategy_response_view_meta(view_family, view_label),
        "execution_target": "paper",
        "live_enabled": runtime_payload
            .get("live_enabled")
            .cloned()
            .unwrap_or_else(|| json!(fev1::ExecutionGate::from_env().live_enabled)),
        "strategy_engine": "forge_fev1",
        "strategy_alias": "FEV1",
        "engine_version": "v1",
        "market_type": runtime_payload
            .get("market_type")
            .cloned()
            .unwrap_or(Value::Null),
        "symbol": runtime_payload.get("symbol").cloned().unwrap_or(Value::Null),
        "lookback_minutes": lookback_minutes,
        "sample_source_mode": "runtime_bucket_1s_from_snapshot_100ms",
        "sample_resolution_ms": 1000,
        "storage_source_mode": "snapshot_100ms",
        "storage_resolution_ms": 100,
        "lookback": strategy_lookback_meta_json(samples, false, lookback_minutes, max_points, 1000),
        "runtime_defaults": {
            "lookback_minutes": runtime_defaults.lookback_minutes,
            "max_points": runtime_defaults.max_points,
            "max_trades": runtime_defaults.max_trades,
        },
        "samples": samples.len(),
        "config_source": config_source,
        "baseline_profile": baseline_profile,
        "config_resolution": config_resolution,
        "fixed_guard": fixed_guard,
        "config": strategy_cfg_json(cfg),
        "paper_cost_model": strategy_paper_cost_model_json(cfg),
        "current": current,
        "summary": summary,
        "trades": trades,
        "signal_decisions": signal_decisions,
        "runtime_current_raw": runtime_payload.get("current").cloned().unwrap_or(Value::Null),
        "runtime_raw_summary": runtime_payload.get("summary").cloned().unwrap_or(Value::Null),
        "runtime_raw_trade_count": runtime_payload
            .get("summary")
            .and_then(|v| v.get("trade_count"))
            .cloned()
            .unwrap_or(Value::Null),
        "runtime_control": runtime_payload
            .get("runtime_control")
            .cloned()
            .unwrap_or(Value::Null),
        "execution_aggressiveness": runtime_payload
            .get("execution_aggressiveness")
            .cloned()
            .unwrap_or(Value::Null),
        "live_execute_requested": runtime_payload
            .get("live_execute_requested")
            .cloned()
            .unwrap_or(Value::Null),
        "live_execute": runtime_payload.get("live_execute").cloned().unwrap_or(Value::Null),
        "live_execute_block_reason": runtime_payload
            .get("live_execute_block_reason")
            .cloned()
            .unwrap_or(Value::Null),
        "live_execution": runtime_payload
            .get("live_execution")
            .cloned()
            .unwrap_or(Value::Null),
        "kill_switch": runtime_payload.get("kill_switch").cloned().unwrap_or(Value::Null),
        "fixed_guard_runtime": runtime_payload
            .get("fixed_guard")
            .cloned()
            .unwrap_or(Value::Null),
    })
}

const STRATEGY_COMPACT_TRADE_ROWS: usize = 8;
const STRATEGY_COMPACT_DECISION_ROWS: usize = 12;
const STRATEGY_COMPACT_EVENT_ROWS: usize = 16;

fn trim_recent_array(value: &mut Value, keep: usize) -> bool {
    let Some(rows) = value.as_array_mut() else {
        return false;
    };
    if rows.len() <= keep {
        return false;
    }
    let trim = rows.len().saturating_sub(keep);
    rows.drain(0..trim);
    true
}

fn trim_object_array_field(
    object: &mut serde_json::Map<String, Value>,
    field: &str,
    keep: usize,
) -> bool {
    object
        .get_mut(field)
        .map(|value| trim_recent_array(value, keep))
        .unwrap_or(false)
}

fn compact_live_execution_payload(value: &mut Value) -> bool {
    let Some(object) = value.as_object_mut() else {
        return false;
    };
    let mut trimmed = false;
    trimmed |= trim_object_array_field(object, "decisions", STRATEGY_COMPACT_DECISION_ROWS);
    trimmed |= trim_object_array_field(object, "paper_records", STRATEGY_COMPACT_DECISION_ROWS);
    trimmed |= trim_object_array_field(object, "live_records", STRATEGY_COMPACT_DECISION_ROWS);
    trimmed |= trim_object_array_field(object, "order_lineage", STRATEGY_COMPACT_DECISION_ROWS);
    trimmed |= trim_object_array_field(object, "events", STRATEGY_COMPACT_EVENT_ROWS);
    if let Some(gated) = object.get_mut("gated").and_then(Value::as_object_mut) {
        trimmed |= trim_object_array_field(
            gated,
            "submitted_decisions",
            STRATEGY_COMPACT_DECISION_ROWS,
        );
        trimmed |=
            trim_object_array_field(gated, "skipped_decisions", STRATEGY_COMPACT_DECISION_ROWS);
    }
    if let Some(execution) = object.get_mut("execution").and_then(Value::as_object_mut) {
        trimmed |= trim_object_array_field(execution, "orders", STRATEGY_COMPACT_EVENT_ROWS);
    }
    trimmed
}

fn compact_strategy_payload(payload: &mut Value) {
    let Some(object) = payload.as_object_mut() else {
        return;
    };
    let mut trimmed = false;
    trimmed |= trim_object_array_field(object, "trades", STRATEGY_COMPACT_TRADE_ROWS);
    trimmed |= trim_object_array_field(object, "signal_decisions", STRATEGY_COMPACT_DECISION_ROWS);
    trimmed |= trim_object_array_field(object, "paper_records", STRATEGY_COMPACT_DECISION_ROWS);
    trimmed |= trim_object_array_field(object, "live_records", STRATEGY_COMPACT_DECISION_ROWS);
    trimmed |= trim_object_array_field(object, "order_lineage", STRATEGY_COMPACT_DECISION_ROWS);
    trimmed |= trim_object_array_field(object, "position_summary", STRATEGY_COMPACT_DECISION_ROWS);
    if let Some(live_execution) = object.get_mut("live_execution") {
        trimmed |= compact_live_execution_payload(live_execution);
    }
    object.insert("payload_mode".to_string(), json!("compact"));
    object.insert("details_available".to_string(), json!(true));
    object.insert("payload_trimmed".to_string(), json!(trimmed));
}

fn build_runtime_snapshot_overview_payload(
    runtime_payload: &Value,
    forced_view_meta: Option<(&'static str, &'static str)>,
    compact: bool,
) -> Value {
    let mut payload = runtime_payload.clone();
    if let Some(object) = payload.as_object_mut() {
        let (family, label) = runtime_view_meta(forced_view_meta);
        object.insert(
            "view".to_string(),
            strategy_response_view_meta(family, label),
        );
        object.insert(
            "overview_mode".to_string(),
            json!("runtime_snapshot_fast_path"),
        );
        object.insert("details_available".to_string(), json!(true));
    }
    if compact {
        compact_strategy_payload(&mut payload);
    }
    payload
}

#[derive(Debug, Deserialize)]
pub(super) struct StrategyPaperQueryParams {
    source: Option<String>,
    profile: Option<String>,
    compact: Option<bool>,
    market_type: Option<String>,
    symbol: Option<String>,
    lookback_minutes: Option<u32>,
    max_points: Option<u32>,
    max_samples: Option<u32>,
    max_trades: Option<u32>,
    full_history: Option<bool>,
    entry_threshold_base: Option<f64>,
    entry_threshold_cap: Option<f64>,
    spread_limit_prob: Option<f64>,
    entry_edge_prob: Option<f64>,
    entry_min_potential_cents: Option<f64>,
    entry_max_price_cents: Option<f64>,
    min_hold_ms: Option<i64>,
    stop_loss_cents: Option<f64>,
    reverse_signal_threshold: Option<f64>,
    reverse_signal_ticks: Option<u32>,
    trail_activate_profit_cents: Option<f64>,
    trail_drawdown_cents: Option<f64>,
    take_profit_near_max_cents: Option<f64>,
    endgame_take_profit_cents: Option<f64>,
    endgame_remaining_ms: Option<i64>,
    liquidity_widen_prob: Option<f64>,
    cooldown_ms: Option<i64>,
    max_entries_per_round: Option<u32>,
    max_exec_spread_cents: Option<f64>,
    slippage_cents_per_side: Option<f64>,
    fee_cents_per_side: Option<f64>,
    emergency_wide_spread_penalty_ratio: Option<f64>,
    stop_loss_grace_ticks: Option<u32>,
    stop_loss_hard_mult: Option<f64>,
    stop_loss_reverse_extra_ticks: Option<u32>,
    loss_cluster_limit: Option<u32>,
    loss_cluster_cooldown_ms: Option<i64>,
    noise_gate_enabled: Option<bool>,
    noise_gate_threshold_add: Option<f64>,
    noise_gate_edge_add: Option<f64>,
    noise_gate_spread_scale: Option<f64>,
    vic_enabled: Option<bool>,
    vic_target_entries_per_hour: Option<f64>,
    vic_deadband_ratio: Option<f64>,
    vic_threshold_relax_max: Option<f64>,
    vic_edge_relax_max: Option<f64>,
    vic_spread_relax_max: Option<f64>,
}

async fn strategy_paper_impl(
    state: ApiState,
    params: StrategyPaperQueryParams,
    forced_source: Option<StrategyPaperSource>,
    forced_view_meta: Option<(&'static str, &'static str)>,
) -> Result<Json<Value>, ApiError> {
    let compact = params.compact.unwrap_or(false);
    let source_mode = forced_source.unwrap_or_else(|| parse_strategy_paper_source(params.source.as_deref()));
    let market_type = resolve_strategy_market_type(params.market_type.as_deref())?;
    let symbol = resolve_strategy_symbol(params.symbol.as_deref())?;
    let runtime_defaults = LiveRuntimeConfig::from_env();
    let runtime_symbol = runtime_defaults.symbol.to_ascii_uppercase();

    if compact
        && matches!(source_mode, StrategyPaperSource::Live)
        && symbol.eq_ignore_ascii_case(&runtime_symbol)
    {
        if let Some(runtime_payload) = state.get_runtime_snapshot(symbol, market_type).await {
            let payload = build_runtime_snapshot_overview_payload(
                &runtime_payload,
                forced_view_meta,
                true,
            );
            return Ok(Json(payload));
        }
    }

    let prefer_live_doc = matches!(
        source_mode,
        StrategyPaperSource::Live | StrategyPaperSource::Auto
    );
    let resolved_cfg = strategy_resolve_effective_config(
        &state,
        symbol,
        market_type,
        params.profile.as_deref(),
        prefer_live_doc,
    )
    .await?;
    let baseline_profile = resolved_cfg.baseline_profile;
    let _live_source_permit = if matches!(
        source_mode,
        StrategyPaperSource::Live | StrategyPaperSource::Auto
    ) {
        match state.strategy_live_source_slots.clone().try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(_) => {
                if let Some(payload) = state.get_runtime_snapshot(symbol, market_type).await {
                    let mut payload = payload;
                    if let Some((family, label)) = forced_view_meta {
                        if let Some(obj) = payload.as_object_mut() {
                            obj.insert(
                                "view".to_string(),
                                strategy_response_view_meta(family, label),
                            );
                        }
                    }
                    if compact {
                        compact_strategy_payload(&mut payload);
                    }
                    return Ok(Json(payload));
                }
                return Err(ApiError::too_many_requests(
                    "live source busy (too many concurrent requests), retry shortly",
                ));
            }
        }
    } else {
        None
    };

    let mut cfg = resolved_cfg.cfg;
    let mut config_source = resolved_cfg.config_source.clone();
    if !matches!(source_mode, StrategyPaperSource::Live) {
        let mut request_overrides_applied = false;
        if let Some(v) = params.entry_threshold_base {
            cfg.entry_threshold_base = v.clamp(0.40, 0.95);
            request_overrides_applied = true;
        }
        if let Some(v) = params.entry_threshold_cap {
            cfg.entry_threshold_cap = v.clamp(cfg.entry_threshold_base, 0.99);
            request_overrides_applied = true;
        }
        if let Some(v) = params.spread_limit_prob {
            cfg.spread_limit_prob = v.clamp(0.005, 0.12);
            request_overrides_applied = true;
        }
        if let Some(v) = params.entry_edge_prob {
            cfg.entry_edge_prob = v.clamp(0.002, 0.25);
            request_overrides_applied = true;
        }
        if let Some(v) = params.entry_min_potential_cents {
            cfg.entry_min_potential_cents = v.clamp(1.0, 70.0);
            request_overrides_applied = true;
        }
        if let Some(v) = params.entry_max_price_cents {
            cfg.entry_max_price_cents = v.clamp(45.0, 98.5);
            request_overrides_applied = true;
        }
        if let Some(v) = params.min_hold_ms {
            cfg.min_hold_ms = v.clamp(0, 240_000);
            request_overrides_applied = true;
        }
        if let Some(v) = params.stop_loss_cents {
            cfg.stop_loss_cents = v.clamp(2.0, 60.0);
            request_overrides_applied = true;
        }
        if let Some(v) = params.reverse_signal_threshold {
            cfg.reverse_signal_threshold = v.clamp(-0.95, -0.02);
            request_overrides_applied = true;
        }
        if let Some(v) = params.reverse_signal_ticks {
            cfg.reverse_signal_ticks = v.clamp(1, 12) as usize;
            request_overrides_applied = true;
        }
        if let Some(v) = params.trail_activate_profit_cents {
            cfg.trail_activate_profit_cents = v.clamp(2.0, 80.0);
            request_overrides_applied = true;
        }
        if let Some(v) = params.trail_drawdown_cents {
            cfg.trail_drawdown_cents = v.clamp(1.0, 50.0);
            request_overrides_applied = true;
        }
        if let Some(v) = params.take_profit_near_max_cents {
            cfg.take_profit_near_max_cents = v.clamp(70.0, 99.5);
            request_overrides_applied = true;
        }
        if let Some(v) = params.endgame_take_profit_cents {
            cfg.endgame_take_profit_cents = v.clamp(50.0, 99.0);
            request_overrides_applied = true;
        }
        if let Some(v) = params.endgame_remaining_ms {
            cfg.endgame_remaining_ms = v.clamp(1_000, 180_000);
            request_overrides_applied = true;
        }
        if let Some(v) = params.liquidity_widen_prob {
            cfg.liquidity_widen_prob = v.clamp(0.01, 0.2);
            request_overrides_applied = true;
        }
        if let Some(v) = params.cooldown_ms {
            cfg.cooldown_ms = v.clamp(0, 120_000);
            request_overrides_applied = true;
        }
        if let Some(v) = params.max_entries_per_round {
            cfg.max_entries_per_round = v.clamp(1, 16) as usize;
            request_overrides_applied = true;
        }
        if let Some(v) = params.max_exec_spread_cents {
            cfg.max_exec_spread_cents = v.clamp(0.2, 30.0);
            request_overrides_applied = true;
        }
        if let Some(v) = params.slippage_cents_per_side {
            cfg.slippage_cents_per_side = v.clamp(0.0, 10.0);
            request_overrides_applied = true;
        }
        if let Some(v) = params.fee_cents_per_side {
            let _ = v;
        }
        cfg.fee_cents_per_side = 0.0;
        if let Some(v) = params.emergency_wide_spread_penalty_ratio {
            cfg.emergency_wide_spread_penalty_ratio = v.clamp(0.2, 3.0);
            request_overrides_applied = true;
        }
        if let Some(v) = params.stop_loss_grace_ticks {
            cfg.stop_loss_grace_ticks = v.clamp(0, 8) as usize;
            request_overrides_applied = true;
        }
        if let Some(v) = params.stop_loss_hard_mult {
            cfg.stop_loss_hard_mult = v.clamp(1.0, 3.0);
            request_overrides_applied = true;
        }
        if let Some(v) = params.stop_loss_reverse_extra_ticks {
            cfg.stop_loss_reverse_extra_ticks = v.clamp(0, 6) as usize;
            request_overrides_applied = true;
        }
        if let Some(v) = params.loss_cluster_limit {
            cfg.loss_cluster_limit = v.clamp(0, 8) as usize;
            request_overrides_applied = true;
        }
        if let Some(v) = params.loss_cluster_cooldown_ms {
            cfg.loss_cluster_cooldown_ms = v.clamp(0, 120_000);
            request_overrides_applied = true;
        }
        if let Some(v) = params.noise_gate_enabled {
            cfg.noise_gate_enabled = v;
            request_overrides_applied = true;
        }
        if let Some(v) = params.noise_gate_threshold_add {
            cfg.noise_gate_threshold_add = v.clamp(0.0, 0.20);
            request_overrides_applied = true;
        }
        if let Some(v) = params.noise_gate_edge_add {
            cfg.noise_gate_edge_add = v.clamp(0.0, 0.12);
            request_overrides_applied = true;
        }
        if let Some(v) = params.noise_gate_spread_scale {
            cfg.noise_gate_spread_scale = v.clamp(0.5, 1.2);
            request_overrides_applied = true;
        }
        if let Some(v) = params.vic_enabled {
            cfg.vic_enabled = v;
            request_overrides_applied = true;
        }
        if let Some(v) = params.vic_target_entries_per_hour {
            cfg.vic_target_entries_per_hour = v.clamp(0.0, 120.0);
            request_overrides_applied = true;
        }
        if let Some(v) = params.vic_deadband_ratio {
            cfg.vic_deadband_ratio = v.clamp(0.0, 0.8);
            request_overrides_applied = true;
        }
        if let Some(v) = params.vic_threshold_relax_max {
            cfg.vic_threshold_relax_max = v.clamp(0.0, 0.2);
            request_overrides_applied = true;
        }
        if let Some(v) = params.vic_edge_relax_max {
            cfg.vic_edge_relax_max = v.clamp(0.0, 0.1);
            request_overrides_applied = true;
        }
        if let Some(v) = params.vic_spread_relax_max {
            cfg.vic_spread_relax_max = v.clamp(0.0, 0.8);
            request_overrides_applied = true;
        }
        if cfg.entry_threshold_cap < cfg.entry_threshold_base {
            cfg.entry_threshold_cap = cfg.entry_threshold_base;
        }
        if request_overrides_applied {
            config_source = format!("{config_source}+request_overrides");
        }
    }

    let fixed_guard = strategy_fixed_guard_payload(&cfg, &config_source);
    let config_resolution = strategy_config_resolution_json(&resolved_cfg);
    let full_history = params.full_history.unwrap_or(false);
    let lookback_minutes = params
        .lookback_minutes
        .unwrap_or(if full_history {
            30 * 24 * 60
        } else {
            runtime_defaults.lookback_minutes
        })
        .clamp(30, 365 * 24 * 60);
    let max_points = strategy_resolve_max_points(
        full_history,
        lookback_minutes,
        params.max_points.or(if full_history {
            None
        } else {
            Some(runtime_defaults.max_points)
        }),
        params.max_samples,
    );
    let _heavy_permit = strategy_acquire_heavy_permit(&state, full_history, max_points).await;
    let max_trades = params
        .max_trades
        .map(|v| v.max(1) as usize)
        .unwrap_or(usize::MAX);

    let mut source_fallback_error: Option<String> = None;
    let mut runtime_snapshot: Option<Value> = None;
    if matches!(
        source_mode,
        StrategyPaperSource::Live | StrategyPaperSource::Auto
    ) {
        if !symbol.eq_ignore_ascii_case(&runtime_symbol) {
            let mismatch_msg = format!(
                "live runtime symbol mismatch: requested={symbol}, runtime={runtime_symbol}"
            );
            if matches!(source_mode, StrategyPaperSource::Live) {
                return Err(ApiError::bad_request(mismatch_msg));
            }
            source_fallback_error = Some(mismatch_msg);
        } else if let Some(payload) = state.get_runtime_snapshot(symbol, market_type).await {
            runtime_snapshot = Some(payload);
        } else {
            let warmup_msg = "live runtime warming up (no snapshot yet)";
            if matches!(source_mode, StrategyPaperSource::Live) {
                return Err(ApiError::bad_request(warmup_msg));
            }
            source_fallback_error = Some(warmup_msg.to_string());
        }
    }

    if matches!(source_mode, StrategyPaperSource::Live) {
        let runtime_payload = runtime_snapshot
            .as_ref()
            .ok_or_else(|| ApiError::bad_request("live runtime warming up (no snapshot yet)"))?;
        let samples = load_live_runtime_samples(&state, symbol, market_type, lookback_minutes, max_points).await?;
        let run = (samples.len() >= 20)
            .then(|| run_strategy_simulation_with_fee_context(&samples, &cfg, max_trades, None));
        let mut payload = build_runtime_bucketed_payload(
            runtime_payload,
            forced_view_meta,
            &cfg,
            &config_source,
            baseline_profile,
            config_resolution,
            lookback_minutes,
            max_points,
            &samples,
            run.as_ref(),
        );
        if compact {
            compact_strategy_payload(&mut payload);
        }
        return Ok(Json(payload));
    }

    let samples = load_strategy_samples(
        &state,
        symbol,
        market_type,
        full_history,
        lookback_minutes,
        max_points,
    )
    .await?;
    if samples.len() < 20 {
        let mut payload = json!({
            "source": "replay",
            "view": strategy_response_view_meta(
                forced_view_meta.map(|(family, _)| family).unwrap_or("replay"),
                forced_view_meta.map(|(_, label)| label).unwrap_or("Replay Research"),
            ),
            "sample_source_mode": "replay_bucket_1s_from_snapshot_100ms",
            "sample_resolution_ms": 1000,
            "storage_source_mode": "snapshot_100ms",
            "storage_resolution_ms": 100,
            "execution_target": "paper",
            "live_enabled": fev1::ExecutionGate::from_env().live_enabled,
            "strategy_engine": "forge_fev1",
            "strategy_alias": "FEV1",
            "source_fallback_error": source_fallback_error,
            "market_type": market_type,
            "symbol": symbol,
            "fixed_guard": fixed_guard,
            "config_resolution": config_resolution,
            "runtime_defaults": {
                "lookback_minutes": runtime_defaults.lookback_minutes,
                "max_points": runtime_defaults.max_points,
                "max_trades": runtime_defaults.max_trades,
            },
            "lookback_minutes": lookback_minutes,
            "lookback": strategy_lookback_meta_json(&samples, full_history, lookback_minutes, max_points, 1000),
            "samples": samples.len(),
            "paper_cost_model": strategy_paper_cost_model_json(&cfg),
            "current": Value::Null,
            "summary": {
                "trade_count": 0,
                "win_rate_pct": 0.0,
                "avg_pnl_cents": 0.0,
                "avg_duration_s": 0.0,
                "total_pnl_cents": 0.0,
                "net_pnl_cents": 0.0,
                "gross_pnl_cents": 0.0,
                "total_cost_cents": 0.0,
                "total_entry_fee_cents": 0.0,
                "total_exit_fee_cents": 0.0,
                "total_slippage_cents": 0.0,
                "total_impact_cents": 0.0,
                "net_margin_pct": 0.0,
                "max_drawdown_cents": 0.0,
            },
            "trades": [],
        });
        if compact {
            compact_strategy_payload(&mut payload);
        }
        return Ok(Json(payload));
    }

    let run = run_strategy_simulation_with_fee_context(&samples, &cfg, max_trades, None);
    let mut payload = json!({
        "source": "replay",
            "view": strategy_response_view_meta(
                forced_view_meta.map(|(family, _)| family).unwrap_or("replay"),
                forced_view_meta.map(|(_, label)| label).unwrap_or("Replay Research"),
            ),
            "sample_source_mode": "replay_bucket_1s_from_snapshot_100ms",
            "sample_resolution_ms": 1000,
            "storage_source_mode": "snapshot_100ms",
            "storage_resolution_ms": 100,
        "execution_target": "paper",
        "live_enabled": fev1::ExecutionGate::from_env().live_enabled,
        "strategy_engine": "forge_fev1",
        "strategy_alias": "FEV1",
        "engine_version": "v1",
        "source_fallback_error": source_fallback_error,
        "market_type": market_type,
        "symbol": symbol,
        "lookback_minutes": lookback_minutes,
        "lookback": strategy_lookback_meta_json(&samples, full_history, lookback_minutes, max_points, 1000),
        "runtime_defaults": {
            "lookback_minutes": runtime_defaults.lookback_minutes,
            "max_points": runtime_defaults.max_points,
            "max_trades": runtime_defaults.max_trades,
        },
        "full_history": full_history,
        "samples": samples.len(),
        "config_source": config_source,
        "baseline_profile": baseline_profile,
        "fixed_guard": fixed_guard,
        "config_resolution": config_resolution,
        "config": strategy_cfg_json(&cfg),
        "paper_cost_model": strategy_paper_cost_model_json(&cfg),
        "current": run.current,
        "summary": {
            "trade_count": run.trade_count,
            "win_rate_pct": run.win_rate_pct,
            "avg_pnl_cents": run.avg_pnl_cents,
            "avg_duration_s": run.avg_duration_s,
            "total_pnl_cents": run.total_pnl_cents,
            "net_pnl_cents": run.net_pnl_cents,
            "gross_pnl_cents": run.gross_pnl_cents,
            "total_cost_cents": run.total_cost_cents,
            "total_entry_fee_cents": run.total_entry_fee_cents,
            "total_exit_fee_cents": run.total_exit_fee_cents,
            "total_slippage_cents": run.total_slippage_cents,
            "total_impact_cents": run.total_impact_cents,
            "net_margin_pct": run.net_margin_pct,
            "max_drawdown_cents": run.max_drawdown_cents,
            "max_profit_trade_cents": run.max_profit_trade_cents,
            "blocked_exits": run.blocked_exits,
            "emergency_wide_exit_count": run.emergency_wide_exit_count,
            "execution_penalty_cents_total": run.execution_penalty_cents_total,
        },
        "trades": run.trades,
    });
    if compact {
        compact_strategy_payload(&mut payload);
    }
    Ok(Json(payload))
}

pub(super) async fn strategy_paper(
    State(state): State<ApiState>,
    Query(params): Query<StrategyPaperQueryParams>,
) -> Result<Json<Value>, ApiError> {
    strategy_paper_impl(state, params, None, None).await
}

pub(super) async fn strategy_runtime(
    State(state): State<ApiState>,
    Query(params): Query<StrategyPaperQueryParams>,
) -> Result<Json<Value>, ApiError> {
    strategy_paper_impl(
        state,
        params,
        Some(StrategyPaperSource::Live),
        Some(("runtime", "Runtime")),
    )
    .await
}

pub(super) async fn strategy_replay(
    State(state): State<ApiState>,
    Query(params): Query<StrategyPaperQueryParams>,
) -> Result<Json<Value>, ApiError> {
    strategy_paper_impl(
        state,
        params,
        Some(StrategyPaperSource::Replay),
        Some(("replay", "Replay Research")),
    )
    .await
}

pub(super) async fn strategy_paper_ledger(
    State(state): State<ApiState>,
    Query(params): Query<StrategyPaperQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let market_type = resolve_strategy_market_type(params.market_type.as_deref())?;
    let symbol = resolve_strategy_symbol(params.symbol.as_deref())?;
    let max_trades = params.max_trades.map(|v| v.max(1) as usize).unwrap_or(200);
    let key = paper_ledger_key(&state.redis_prefix, symbol, market_type);
    let reset_after_ms = state.get_runtime_paper_reset_anchor(symbol, market_type).await;
    let payload = if let Some(v) = read_key_value(&state, &key).await? {
        v
    } else if let Some(runtime_payload) = state.get_runtime_snapshot(symbol, market_type).await {
        build_paper_ledger_payload(None, &runtime_payload, symbol, market_type, reset_after_ms)
    } else {
        json!({
            "source": "ledger",
            "view": {
                "family": "ledger",
                "label": "Paper Ledger",
            },
            "symbol": symbol,
            "market_type": market_type,
            "updated_at_ms": Utc::now().timestamp_millis(),
            "lookback_minutes": 0,
            "samples": 0,
            "paper_reset_after_ms": reset_after_ms,
            "current": Value::Null,
            "summary": summarize_trade_rows(&[]),
            "trades": [],
            "paper_records": [],
            "live_records": [],
        })
    };
    let mut payload = payload;
    if let Some(obj) = payload.as_object_mut() {
        if let Some(trades) = obj.get_mut("trades").and_then(Value::as_array_mut) {
            let trim = trades.len().saturating_sub(max_trades);
            if trim > 0 {
                trades.drain(0..trim);
            }
        }
        obj.insert(
            "view".to_string(),
            strategy_response_view_meta("ledger", "Paper Ledger"),
        );
    }
    if params.compact.unwrap_or(false) {
        compact_strategy_payload(&mut payload);
    }
    Ok(Json(payload))
}

pub(super) async fn strategy_execution_attribution(
    State(state): State<ApiState>,
    Query(params): Query<StrategyPaperQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let market_type = resolve_strategy_market_type(params.market_type.as_deref())?;
    let symbol = resolve_strategy_symbol(params.symbol.as_deref())?;
    let key = paper_ledger_key(&state.redis_prefix, symbol, market_type);
    let reset_after_ms = state.get_runtime_paper_reset_anchor(symbol, market_type).await;
    let ledger_payload = if let Some(v) = read_key_value(&state, &key).await? {
        v
    } else if let Some(runtime_payload) = state.get_runtime_snapshot(symbol, market_type).await {
        build_paper_ledger_payload(None, &runtime_payload, symbol, market_type, reset_after_ms)
    } else {
        json!({})
    };

    let paper_records = ledger_payload
        .get("paper_records")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let live_events = state
        .list_live_events(symbol, market_type, live_event_log_max())
        .await;
    let pending_rows = state.list_pending_orders_for_market(symbol, market_type).await;
    let (fill_by_decision, fills_summary) = build_live_fill_decision_map(&live_events);
    let live_order_lineage = build_live_order_lineage(&live_events, &pending_rows);
    let live_records =
        build_live_completed_records(&paper_records, &fill_by_decision, &live_order_lineage);

    let mut by_position = HashMap::<String, (usize, f64, f64)>::new();
    for row in &live_records {
        let position_id = row
            .get("position_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .unwrap_or("unassigned")
            .to_string();
        let paper_net = row
            .get("pnl_net_cents")
            .and_then(Value::as_f64)
            .or_else(|| row.get("net_pnl_cents").and_then(Value::as_f64))
            .unwrap_or(0.0);
        let live_net = row
            .get("live_fill_pnl_cents_net")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let entry = by_position.entry(position_id).or_insert((0, 0.0, 0.0));
        entry.0 = entry.0.saturating_add(1);
        entry.1 += paper_net;
        entry.2 += live_net;
    }
    let mut position_summary = by_position
        .into_iter()
        .map(|(position_id, (count, paper_net, live_net))| {
            json!({
                "position_id": position_id,
                "record_count": count,
                "paper_realized_pnl_cents": paper_net,
                "live_realized_pnl_cents": live_net,
                "execution_drift_cents": live_net - paper_net,
            })
        })
        .collect::<Vec<_>>();
    position_summary.sort_by_key(|row| {
        std::cmp::Reverse(row.get("record_count").and_then(Value::as_u64).unwrap_or(0))
    });

    let orders = live_events
        .iter()
        .filter(|row| {
            row.get("signal_to_submit_ms").is_some()
                || row.get("submit_to_ack_ms").is_some()
                || row.get("order_latency_ms").is_some()
        })
        .cloned()
        .collect::<Vec<_>>();
    let latency = summarize_live_order_latency(&orders);

    let mut payload = json!({
        "source": "attribution",
        "view": {
            "family": "attribution",
            "label": "Execution Attribution",
        },
        "symbol": symbol,
        "market_type": market_type,
        "updated_at_ms": Utc::now().timestamp_millis(),
        "paper_record_count": paper_records.len(),
        "live_record_count": live_records.len(),
        "order_lineage_count": live_order_lineage.len(),
        "position_count": position_summary.len(),
        "paper_records": paper_records,
        "live_records": live_records,
        "order_lineage": live_order_lineage,
        "fills_summary": fills_summary,
        "latency": latency,
        "position_summary": position_summary,
        "runtime_control": ledger_payload.get("runtime_control").cloned().unwrap_or(Value::Null),
        "current": ledger_payload.get("current").cloned().unwrap_or(Value::Null),
    });
    if params.compact.unwrap_or(false) {
        compact_strategy_payload(&mut payload);
    }
    Ok(Json(payload))
}

pub(super) async fn strategy_live_reset(
    State(state): State<ApiState>,
) -> Result<Json<Value>, ApiError> {
    let now_ms = Utc::now().timestamp_millis();
    let runtime_cfg = LiveRuntimeConfig::from_env();
    {
        let mut events = state.live_events.write().await;
        events.clear();
    }
    {
        let mut seq = state.live_event_seq.write().await;
        *seq = 0;
    }
    {
        let mut pending = state.live_pending_orders.write().await;
        pending.clear();
    }
    {
        let mut guard = state.live_decision_guard.write().await;
        guard.clear();
    }
    {
        let mut snapshots = state.live_runtime_snapshots.write().await;
        snapshots.clear();
    }
    {
        let mut recent_intents = state.live_recent_active_intents.write().await;
        recent_intents.clear();
    }
    {
        let mut controls = state.live_runtime_controls.write().await;
        controls.clear();
    }
    {
        let mut aggr = state.live_execution_aggr_states.write().await;
        aggr.clear();
    }
    {
        let mut kill_switch = state.live_kill_switch.write().await;
        kill_switch.clear();
    }
    {
        let mut inflight = state.live_persist_inflight.write().await;
        inflight.clear();
    }
    {
        let mut cache = state.live_rust_book_cache.write().await;
        cache.clear();
    }
    {
        let mut exec = state.live_rust_executor.write().await;
        exec.take();
    }
    for market in ["5m", "15m"] {
        state
            .put_runtime_paper_reset_anchor(&runtime_cfg.symbol, market, now_ms)
            .await;
        state
            .put_live_position_state(
                &runtime_cfg.symbol,
                market,
                LivePositionState::flat(&runtime_cfg.symbol, market, now_ms),
            )
            .await;
        state
            .put_live_runtime_control(
                &runtime_cfg.symbol,
                market,
                LiveRuntimeControl::normal(now_ms),
            )
            .await;
    }

    let mut deleted_keys = Vec::<String>::new();
    for market in ["5m", "15m"] {
        let key = live_position_state_key(&state.redis_prefix, &runtime_cfg.symbol, market);
        if delete_key(&state, &key).await.is_ok() {
            deleted_keys.push(key);
        }
        let events_key = live_events_key(&state.redis_prefix, &runtime_cfg.symbol, market);
        if delete_key(&state, &events_key).await.is_ok() {
            deleted_keys.push(events_key);
        }
        let control_key =
            live_runtime_control_key(&state.redis_prefix, &runtime_cfg.symbol, market);
        if delete_key(&state, &control_key).await.is_ok() {
            deleted_keys.push(control_key);
        }
        let paper_ledger_key =
            paper_ledger_key(&state.redis_prefix, &runtime_cfg.symbol, market);
        if delete_key(&state, &paper_ledger_key).await.is_ok() {
            deleted_keys.push(paper_ledger_key);
        }
        let reset_anchor_key =
            live_runtime_paper_reset_anchor_key(&state.redis_prefix, &runtime_cfg.symbol, market);
        let _ = write_key_value(&state, &reset_anchor_key, &json!(now_ms), Some(7 * 24 * 3600)).await;
        deleted_keys.push(reset_anchor_key);
    }
    let pending_key = live_pending_orders_key(&state.redis_prefix);
    if delete_key(&state, &pending_key).await.is_ok() {
        deleted_keys.push(pending_key);
    }
    Ok(Json(json!({
        "ok": true,
        "reset_at_ms": now_ms,
        "deleted_redis_keys": deleted_keys
    })))
}

#[derive(Debug, Deserialize)]
pub(super) struct StrategyLiveReconcileResolvedRequest {
    market_type: Option<String>,
    action: Option<String>,
    note: Option<String>,
    force: Option<bool>,
}

fn flattened_reconciled_position(
    position: &LivePositionState,
    now_ms: i64,
    reason: &str,
) -> LivePositionState {
    let mut next = position.clone();
    next.state = "flat".to_string();
    next.side = None;
    next.entry_round_id = None;
    next.entry_market_id = None;
    next.entry_token_id = None;
    next.entry_ts_ms = None;
    next.entry_price_cents = None;
    next.entry_quote_usdc = None;
    next.net_quote_usdc = 0.0;
    next.vwap_entry_cents = None;
    next.last_action = Some("reconcile_resolved".to_string());
    next.last_reason = Some(reason.to_string());
    next.open_add_layers = 0;
    next.position_cost_usdc = 0.0;
    next.position_size_shares = 0.0;
    next.updated_ts_ms = now_ms;
    next
}

pub(super) async fn strategy_live_reconcile_resolved(
    State(state): State<ApiState>,
    Json(body): Json<StrategyLiveReconcileResolvedRequest>,
) -> Result<Json<Value>, ApiError> {
    let market_type = resolve_strategy_market_type(body.market_type.as_deref())?;
    let runtime_cfg = LiveRuntimeConfig::from_env();
    let runtime_symbol = runtime_cfg.symbol;
    let action = body
        .action
        .as_deref()
        .unwrap_or("status")
        .trim()
        .to_ascii_lowercase();
    if action != "status" && action != "clear_local" {
        return Err(ApiError::bad_request(
            "invalid action (allowed: status|clear_local)",
        ));
    }
    let force = body.force.unwrap_or(false);
    let note = body
        .note
        .as_ref()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let now_ms = Utc::now().timestamp_millis();
    let control = state
        .get_live_runtime_control(&runtime_symbol, market_type)
        .await;
    let position = state
        .get_live_position_state(&runtime_symbol, market_type)
        .await;
    let pending_orders = state
        .list_pending_orders_for_market(&runtime_symbol, market_type)
        .await;
    let pending_count = pending_orders.len();

    let mut gamma_detail = None;
    let mut locked_target = None;
    if position.position_size_shares > 0.0 {
        locked_target = build_position_locked_target(market_type, &position);
        if let Some(market_id) = position.entry_market_id.as_deref() {
            gamma_detail = fetch_gamma_market_detail(market_id).await;
        }
    }

    let resolution = analyze_resolved_position_outcome(gamma_detail.as_ref(), &position);
    let wallet = live_runtime_wallet_address();
    let masked_wallet = wallet.as_deref().map(mask_wallet_address);
    let matched_positions = if let Some(wallet) = wallet.as_deref() {
        match fetch_data_api_user_positions(wallet).await {
            Ok(rows) => {
                let filtered = rows
                    .into_iter()
                    .filter(|row| {
                        let token_match = position
                            .entry_token_id
                            .as_deref()
                            .map(|token| row.asset == token)
                            .unwrap_or(false);
                        let condition_match = gamma_detail
                            .as_ref()
                            .and_then(|detail| detail.condition_id.as_deref())
                            .and_then(|condition| row.condition_id.as_deref().map(|v| v == condition))
                            .unwrap_or(false);
                        token_match || condition_match
                    })
                    .collect::<Vec<_>>();
                (Some(filtered), None::<String>)
            }
            Err(err) => (None, Some(err)),
        }
    } else {
        (None, Some("runtime wallet missing".to_string()))
    };
    let venue_check_available = matched_positions.0.is_some();
    let can_clear_local = can_clear_resolved_position_local_state(
        resolution.market_resolved || resolution.market_closed,
        pending_count,
        control.mode,
        venue_check_available,
        force,
    );

    let mut cleared_local = false;
    if action == "clear_local" {
        if position.position_size_shares <= 0.0 || position.side.is_none() {
            return Err(ApiError::bad_request("no local open live position to reconcile"));
        }
        if matches!(control.mode, LiveRuntimeControlMode::Normal) {
            return Err(ApiError::bad_request(
                "live runtime must be paused before clear_local reconcile",
            ));
        }
        if pending_count > 0 {
            return Err(ApiError::bad_request(
                "cannot clear_local while pending orders still exist",
            ));
        }
        if !(resolution.market_resolved || resolution.market_closed) {
            return Err(ApiError::bad_request(
                "market is not resolved/closed; clear_local reconcile refused",
            ));
        }
        if !can_clear_local {
            return Err(ApiError::bad_request(
                "venue claim check unavailable; retry later or use force=true to clear local state only",
            ));
        }
        let next = flattened_reconciled_position(
            &position,
            now_ms,
            "resolved_position_local_clear",
        );
        state
            .put_live_position_state(&runtime_symbol, market_type, next.clone())
            .await;
        state
            .append_live_event(
                &runtime_symbol,
                market_type,
                json!({
                    "event_type": "reconcile",
                    "action": "reconcile_resolved",
                    "accepted": true,
                    "reason": "resolved_position_local_clear",
                    "side": position.side.clone(),
                    "detail": {
                        "force": force,
                        "pending_count": pending_count,
                        "entry_market_id": position.entry_market_id.clone(),
                        "entry_token_id": position.entry_token_id.clone(),
                        "resolution": resolution.clone(),
                        "note": note.clone()
                    }
                }),
            )
            .await;
        persist_live_runtime_state(&state, &runtime_symbol, market_type).await;
        cleared_local = true;
    }

    let local_position_after = state
        .get_live_position_state(&runtime_symbol, market_type)
        .await;
    let matched_rows = matched_positions
        .0
        .clone()
        .unwrap_or_default();
    let locked_target_json = locked_target.as_ref().map(|target| {
        json!({
            "market_id": target.market_id,
            "symbol": target.symbol,
            "timeframe": target.timeframe,
            "token_id_yes": target.token_id_yes,
            "token_id_no": target.token_id_no,
            "end_date": target.end_date
        })
    });
    Ok(Json(json!({
        "ok": true,
        "symbol": runtime_symbol,
        "market_type": market_type,
        "action": action,
        "force": force,
        "control": control,
        "pending_orders": pending_count,
        "locked_target": locked_target_json,
        "local_position_before": position,
        "local_position_after": local_position_after,
        "gamma_market": gamma_detail.as_ref().map(|detail| json!({
            "market_id": detail.market_id,
            "condition_id": detail.condition_id,
            "slug": detail.slug,
            "question": detail.question,
            "active": detail.active,
            "closed": detail.closed,
            "enable_order_book": detail.enable_order_book,
            "end_date": detail.end_date,
            "uma_resolution_status": detail.uma_resolution_status,
            "outcomes": detail.outcomes,
            "outcome_prices": detail.outcome_prices,
            "token_ids": detail.token_ids
        })),
        "resolution": resolution,
        "venue_claim_check": {
            "wallet_present": wallet.is_some(),
            "wallet_masked": masked_wallet,
            "positions_status": if venue_check_available { "ok" } else { "unavailable" },
            "positions_error": matched_positions.1,
            "matching_positions_count": matched_rows.len(),
            "matching_positions": matched_rows
        },
        "can_clear_local": can_clear_local,
        "cleared_local": cleared_local,
        "note": note.clone()
    })))
}

#[derive(Debug, Deserialize)]
pub(super) struct StrategyLiveControlRequest {
    market_type: Option<String>,
    action: String,
    note: Option<String>,
}

pub(super) async fn strategy_live_control(
    State(state): State<ApiState>,
    Json(body): Json<StrategyLiveControlRequest>,
) -> Result<Json<Value>, ApiError> {
    let market_type = resolve_strategy_market_type(body.market_type.as_deref())?;
    let runtime_cfg = LiveRuntimeConfig::from_env();
    let runtime_symbol = runtime_cfg.symbol;
    let action = body.action.trim().to_ascii_lowercase();
    if action.is_empty() {
        return Err(ApiError::bad_request("action is required"));
    }
    let now_ms = Utc::now().timestamp_millis();
    let mut control = state
        .get_live_runtime_control(&runtime_symbol, market_type)
        .await;
    let mut updated = false;
    match action.as_str() {
        "status" => {}
        "graceful_stop" | "stop_graceful" | "stop" => {
            control.mode = LiveRuntimeControlMode::GracefulStop;
            control.requested_at_ms = now_ms;
            control.updated_at_ms = now_ms;
            control.completed_at_ms = None;
            control.note = body
                .note
                .as_ref()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty());
            updated = true;
        }
        "resume_live" | "resume" | "start" => {
            control = LiveRuntimeControl::normal(now_ms);
            control.note = body
                .note
                .as_ref()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty());
            updated = true;
        }
        "force_pause" | "pause" => {
            control.mode = LiveRuntimeControlMode::ForcePause;
            control.requested_at_ms = now_ms;
            control.updated_at_ms = now_ms;
            control.completed_at_ms = None;
            control.note = body
                .note
                .as_ref()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty());
            updated = true;
        }
        _ => {
            return Err(ApiError::bad_request(
                "invalid action (allowed: status|graceful_stop|resume_live|force_pause)",
            ));
        }
    }

    if updated {
        state
            .put_live_runtime_control(&runtime_symbol, market_type, control.clone())
            .await;
        persist_live_runtime_state(&state, &runtime_symbol, market_type).await;
    }

    let position = state
        .get_live_position_state(&runtime_symbol, market_type)
        .await;
    let pending_count = state
        .list_pending_orders_for_market(&runtime_symbol, market_type)
        .await
        .len();
    let flatten_done = position.side.is_none() && pending_count == 0;
    if matches!(control.mode, LiveRuntimeControlMode::GracefulStop)
        && flatten_done
        && control.completed_at_ms.is_none()
    {
        control.completed_at_ms = Some(now_ms);
        control.updated_at_ms = now_ms;
        state
            .put_live_runtime_control(&runtime_symbol, market_type, control.clone())
            .await;
        persist_live_runtime_state(&state, &runtime_symbol, market_type).await;
    }

    Ok(Json(json!({
        "ok": true,
        "symbol": runtime_symbol,
        "market_type": market_type,
        "action": action,
        "control": control,
        "position_side": position.side,
        "pending_orders": pending_count,
        "flatten_done": flatten_done,
        "runtime_effective": {
            "drain_only": matches!(control.mode, LiveRuntimeControlMode::GracefulStop | LiveRuntimeControlMode::ForcePause),
            "live_execute": if matches!(control.mode, LiveRuntimeControlMode::ForcePause) {
                false
            } else if matches!(control.mode, LiveRuntimeControlMode::GracefulStop) {
                !flatten_done
            } else {
                LiveRuntimeConfig::from_env().live_execute
            }
        }
    })))
}

#[derive(Debug, Deserialize)]
pub(super) struct StrategyLiveEventsQueryParams {
    market_type: Option<String>,
    limit: Option<u32>,
    since_ts_ms: Option<i64>,
    order_id: Option<String>,
    action: Option<String>,
    reason: Option<String>,
    accepted: Option<bool>,
}

fn collect_event_latency_values(events: &[Value], key: &str) -> Vec<f64> {
    events
        .iter()
        .filter_map(|ev| ev.get(key).and_then(Value::as_f64))
        .filter(|v| v.is_finite() && *v >= 0.0)
        .collect()
}

fn latency_stats_from_samples(samples: &[f64]) -> Value {
    if samples.is_empty() {
        return json!({
            "count": 0,
            "avg_ms": Value::Null,
            "p50_ms": Value::Null,
            "p95_ms": Value::Null,
            "max_ms": Value::Null
        });
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let avg = sorted.iter().sum::<f64>() / sorted.len() as f64;
    let p50_idx = ((sorted.len() - 1) as f64 * 0.50).round() as usize;
    let p95_idx = ((sorted.len() - 1) as f64 * 0.95).round() as usize;
    json!({
        "count": sorted.len(),
        "avg_ms": avg,
        "p50_ms": sorted.get(p50_idx).copied(),
        "p95_ms": sorted.get(p95_idx).copied(),
        "max_ms": sorted.last().copied()
    })
}

pub(super) async fn strategy_live_events(
    State(state): State<ApiState>,
    Query(params): Query<StrategyLiveEventsQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let market_type = resolve_strategy_market_type(params.market_type.as_deref())?;
    let runtime_symbol = LiveRuntimeConfig::from_env().symbol;
    let limit = params.limit.unwrap_or(400).clamp(1, 5000) as usize;
    let since_ts_ms = params.since_ts_ms.unwrap_or(0);
    let order_id_filter = params
        .order_id
        .as_deref()
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty());
    let action_filter = params
        .action
        .as_deref()
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty());
    let reason_filter = params
        .reason
        .as_deref()
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty());
    let accepted_filter = params.accepted;

    let scanned = state
        .list_live_events(&runtime_symbol, market_type, live_event_log_max())
        .await;
    let mut filtered = scanned
        .into_iter()
        .filter(|ev| {
            let ts_ok = ev.get("ts_ms").and_then(Value::as_i64).unwrap_or(0) >= since_ts_ms;
            if !ts_ok {
                return false;
            }
            if let Some(want) = order_id_filter.as_deref() {
                let got = ev
                    .get("order_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .trim()
                    .to_ascii_lowercase();
                if got != want {
                    return false;
                }
            }
            if let Some(want) = action_filter.as_deref() {
                let got = ev
                    .get("action")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .trim()
                    .to_ascii_lowercase();
                if got != want {
                    return false;
                }
            }
            if let Some(want) = reason_filter.as_deref() {
                let got = ev
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .trim()
                    .to_ascii_lowercase();
                if !got.contains(want) {
                    return false;
                }
            }
            if let Some(want) = accepted_filter {
                let got = ev.get("accepted").and_then(Value::as_bool).unwrap_or(false);
                if got != want {
                    return false;
                }
            }
            true
        })
        .collect::<Vec<_>>();
    if filtered.len() > limit {
        filtered = filtered.split_off(filtered.len().saturating_sub(limit));
    }

    let mut by_reason = HashMap::<String, usize>::new();
    let mut by_action = HashMap::<String, usize>::new();
    let mut by_event_type = HashMap::<String, usize>::new();
    let mut accept_count = 0usize;
    let mut reject_like_count = 0usize;
    for ev in &filtered {
        let reason = ev
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        *by_reason.entry(reason.clone()).or_insert(0) += 1;
        let action = ev
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or("none")
            .to_string();
        *by_action.entry(action).or_insert(0) += 1;
        let event_type = ev
            .get("event_type")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        *by_event_type.entry(event_type).or_insert(0) += 1;
        if ev.get("accepted").and_then(Value::as_bool).unwrap_or(false) {
            accept_count += 1;
        }
        let reason_lc = reason.to_ascii_lowercase();
        if reason_lc.contains("reject")
            || reason_lc.contains("cancel")
            || reason_lc.contains("timeout")
            || reason_lc.contains("blocked")
        {
            reject_like_count += 1;
        }
    }
    let mut reason_top = by_reason.into_iter().collect::<Vec<_>>();
    reason_top.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    reason_top.truncate(20);
    let order_latency =
        latency_stats_from_samples(&collect_event_latency_values(&filtered, "order_latency_ms"));
    let signal_to_submit_latency = latency_stats_from_samples(&collect_event_latency_values(
        &filtered,
        "signal_to_submit_ms",
    ));
    let signal_to_ack_latency =
        latency_stats_from_samples(&collect_event_latency_values(&filtered, "signal_to_ack_ms"));
    let submit_to_ack_latency =
        latency_stats_from_samples(&collect_event_latency_values(&filtered, "submit_to_ack_ms"));

    Ok(Json(json!({
        "ok": true,
        "symbol": runtime_symbol,
        "market_type": market_type,
        "filters": {
            "limit": limit,
            "since_ts_ms": since_ts_ms,
            "order_id": params.order_id,
            "action": params.action,
            "reason": params.reason,
            "accepted": accepted_filter,
        },
        "summary": {
            "event_count": filtered.len(),
            "accept_count": accept_count,
            "reject_like_count": reject_like_count,
            "latency": {
                "order_latency_ms": order_latency,
                "signal_to_submit_ms": signal_to_submit_latency,
                "signal_to_ack_ms": signal_to_ack_latency,
                "submit_to_ack_ms": submit_to_ack_latency
            },
            "reason_top": reason_top,
            "by_action": by_action,
            "by_event_type": by_event_type
        },
        "events": filtered
    })))
}

pub(super) async fn strategy_autotune_latest(
    State(state): State<ApiState>,
    Query(params): Query<StrategyAutotuneLatestQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let market_type = resolve_strategy_market_type(params.market_type.as_deref())?;
    let symbol = resolve_strategy_symbol(params.symbol.as_deref())?;
    let context = normalize_autotune_context(params.context.as_deref(), market_type, symbol);
    let (payload, key, _) = resolve_autotune_doc(&state, &context, market_type).await?;
    let (active_payload, active_key) =
        resolve_autotune_active_doc(&state, market_type, symbol).await?;
    let (live_payload, live_key) = resolve_autotune_live_doc(&state, market_type, symbol).await?;
    Ok(Json(json!({
        "market_type": market_type,
        "symbol": symbol,
        "context": context,
        "key": key,
        "found": payload.is_some(),
        "data": payload,
        "active_key": active_key,
        "active_found": active_payload.is_some(),
        "active_data": active_payload,
        "live_key": live_key,
        "live_found": live_payload.is_some(),
        "live_data": live_payload,
    })))
}

pub(super) async fn strategy_autotune_history(
    State(state): State<ApiState>,
    Query(params): Query<StrategyAutotuneHistoryQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let market_type = resolve_strategy_market_type(params.market_type.as_deref())?;
    let symbol = resolve_strategy_symbol(params.symbol.as_deref())?;
    let context = normalize_autotune_context(params.context.as_deref(), market_type, symbol);
    let limit = params.limit.unwrap_or(20).clamp(1, 200) as usize;
    let active_key = strategy_autotune_active_history_key(&state.redis_prefix, market_type, symbol);
    let mut items = read_key_value(&state, &active_key)
        .await?
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default();
    let candidate_key = strategy_autotune_history_key(&state.redis_prefix, &context);
    let (key_used, source_used) = if items.is_empty() {
        items = read_key_value(&state, &candidate_key)
            .await?
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default();
        (candidate_key, "candidate_context")
    } else {
        (active_key, "active_promotions")
    };
    if items.len() > limit {
        items.truncate(limit);
    }
    Ok(Json(json!({
        "market_type": market_type,
        "symbol": symbol,
        "context": context,
        "key": key_used,
        "source": source_used,
        "limit": limit,
        "count": items.len(),
        "items": items,
    })))
}

pub(super) async fn strategy_autotune_set(
    State(state): State<ApiState>,
    Json(body): Json<StrategyAutotuneSetBody>,
) -> Result<Json<Value>, ApiError> {
    let market_type = resolve_strategy_market_type(body.market_type.as_deref())?;
    let symbol = resolve_strategy_symbol(body.symbol.as_deref())?;
    let context = normalize_autotune_context(body.context.as_deref(), market_type, symbol);
    let key = strategy_autotune_key(&state.redis_prefix, &context);
    let history_key = strategy_autotune_history_key(&state.redis_prefix, &context);
    let (previous, previous_key, _) = resolve_autotune_doc(&state, &context, market_type).await?;

    let wrapped = json!({ "config": body.config });
    let cfg = strategy_cfg_from_payload(StrategyRuntimeConfig::default(), &wrapped);
    let saved_doc = json!({
        "saved_at_ms": Utc::now().timestamp_millis(),
        "market_type": market_type,
        "symbol": symbol,
        "context": context.clone(),
        "source": body.source.unwrap_or_else(|| "manual".to_string()),
        "note": body.note.unwrap_or_default(),
        "config": strategy_cfg_json(&cfg),
    });

    write_key_value(&state, &key, &saved_doc, body.ttl_sec.filter(|v| *v > 0)).await?;

    let mut history: Vec<Value> = read_key_value(&state, &history_key)
        .await?
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default();
    if let Some(prev) = previous.clone() {
        history.insert(0, prev);
    }
    if history.len() > 40 {
        history.truncate(40);
    }
    let _ = write_key_value(&state, &history_key, &Value::Array(history), None).await;

    Ok(Json(json!({
        "ok": true,
        "market_type": market_type,
        "symbol": symbol,
        "context": context,
        "key": key,
        "history_key": history_key,
        "previous_found": previous.is_some(),
        "previous_key": previous_key,
        "saved": saved_doc,
    })))
}

#[cfg(test)]
mod handlers_runtime_view_tests {
    use super::*;

    #[test]
    fn build_runtime_bucketed_payload_marks_runtime_as_bucketed_1s_view() {
        let runtime_payload = json!({
            "symbol": "SOLUSDT",
            "market_type": "5m",
            "current": {
                "round_id": "SOLUSDT_5m_1",
                "timestamp_ms": 1_700_000_000_000i64,
                "suggested_action": "HOLD"
            },
            "summary": {
                "trade_count": 7
            },
            "runtime_control": {
                "mode": "paper_only"
            }
        });
        let samples = vec![StrategySample {
            ts_ms: 1_700_000_000_000,
            round_id: "SOLUSDT_5m_1".to_string(),
            remaining_ms: 120_000,
            p_up: 0.51,
            delta_pct: 0.0,
            velocity: 0.0,
            acceleration: 0.0,
            bid_yes: 0.50,
            ask_yes: 0.51,
            bid_no: 0.49,
            ask_no: 0.50,
            spread_up: 0.01,
            spread_down: 0.01,
            spread_mid: 0.01,
        }];

        let payload = build_runtime_bucketed_payload(
            &runtime_payload,
            Some(("runtime", "Runtime Paper")),
            &StrategyRuntimeConfig::default(),
            "test",
            "default",
            json!({"mode": "test"}),
            60,
            600,
            &samples,
            None,
        );

        assert_eq!(payload.get("source").and_then(Value::as_str), Some("runtime"));
        assert_eq!(
            payload.get("sample_source_mode").and_then(Value::as_str),
            Some("runtime_bucket_1s_from_snapshot_100ms")
        );
        assert_eq!(
            payload.get("sample_resolution_ms").and_then(Value::as_i64),
            Some(1000)
        );
        assert_eq!(
            payload
                .get("runtime_raw_trade_count")
                .and_then(Value::as_i64),
            Some(7)
        );
        assert_eq!(
            payload
                .get("current")
                .and_then(|v| v.get("round_id"))
                .and_then(Value::as_str),
            Some("SOLUSDT_5m_1")
        );
    }

    #[test]
    fn compact_strategy_payload_trims_heavy_arrays() {
        let mut payload = json!({
            "trades": (0..20).map(|idx| json!({"id": idx})).collect::<Vec<_>>(),
            "signal_decisions": (0..20).map(|idx| json!({"decision_id": idx})).collect::<Vec<_>>(),
            "paper_records": (0..20).map(|idx| json!({"decision_id": idx})).collect::<Vec<_>>(),
            "position_summary": (0..20).map(|idx| json!({"position_id": idx})).collect::<Vec<_>>(),
            "live_execution": {
                "events": (0..25).map(|idx| json!({"event_seq": idx})).collect::<Vec<_>>(),
                "execution": {
                    "orders": (0..25).map(|idx| json!({"decision_id": idx})).collect::<Vec<_>>()
                }
            }
        });

        compact_strategy_payload(&mut payload);

        assert_eq!(
            payload.get("payload_mode").and_then(Value::as_str),
            Some("compact")
        );
        assert_eq!(
            payload
                .get("trades")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(STRATEGY_COMPACT_TRADE_ROWS)
        );
        assert_eq!(
            payload
                .get("signal_decisions")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(STRATEGY_COMPACT_DECISION_ROWS)
        );
        assert_eq!(
            payload
                .get("position_summary")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(STRATEGY_COMPACT_DECISION_ROWS)
        );
        assert_eq!(
            payload
                .get("live_execution")
                .and_then(|v| v.get("events"))
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(STRATEGY_COMPACT_EVENT_ROWS)
        );
        assert_eq!(
            payload
                .get("live_execution")
                .and_then(|v| v.get("execution"))
                .and_then(|v| v.get("orders"))
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(STRATEGY_COMPACT_EVENT_ROWS)
        );
    }

    #[test]
    fn build_runtime_snapshot_overview_payload_marks_fast_path() {
        let runtime_payload = json!({
            "source": "runtime",
            "symbol": "SOLUSDT",
            "market_type": "5m",
            "current": {
                "round_id": "SOLUSDT_5m_1",
                "suggested_action": "HOLD"
            },
            "summary": {
                "trade_count": 3
            },
            "trades": (0..12).map(|idx| json!({"id": idx})).collect::<Vec<_>>()
        });

        let payload = build_runtime_snapshot_overview_payload(
            &runtime_payload,
            Some(("runtime", "Runtime Paper")),
            true,
        );

        assert_eq!(
            payload.get("overview_mode").and_then(Value::as_str),
            Some("runtime_snapshot_fast_path")
        );
        assert_eq!(
            payload
                .get("view")
                .and_then(|v| v.get("family"))
                .and_then(Value::as_str),
            Some("runtime")
        );
        assert_eq!(
            payload
                .get("trades")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(STRATEGY_COMPACT_TRADE_ROWS)
        );
    }
}
