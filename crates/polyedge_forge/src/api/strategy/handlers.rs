pub(super) async fn strategy_paper(
    State(state): State<ApiState>,
    Query(params): Query<StrategyPaperQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let source_mode = parse_strategy_paper_source(params.source.as_deref());
    let market_type = resolve_strategy_market_type(params.market_type.as_deref())?;
    let symbol = resolve_strategy_symbol(params.symbol.as_deref())?;
    let baseline_profile = strategy_current_default_profile_name_for_scope(symbol, market_type);
    let _live_source_permit = if matches!(
        source_mode,
        StrategyPaperSource::Live | StrategyPaperSource::Auto
    ) {
        match state.strategy_live_source_slots.clone().try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(_) => {
                if let Some(payload) = state.get_runtime_snapshot(symbol, market_type).await {
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

    let mut cfg = strategy_current_default_config_for_scope(symbol, market_type);
    let mut config_source = baseline_profile;
    if let Some(profile_raw) = params.profile.as_deref() {
        if let Some((profile_name, profile_cfg)) = strategy_profile_from_alias(profile_raw) {
            cfg = profile_cfg;
            config_source = profile_name;
        }
    }
    if !matches!(source_mode, StrategyPaperSource::Live) {
        if let Some(v) = params.entry_threshold_base {
            cfg.entry_threshold_base = v.clamp(0.40, 0.95);
        }
        if let Some(v) = params.entry_threshold_cap {
            cfg.entry_threshold_cap = v.clamp(cfg.entry_threshold_base, 0.99);
        }
        if let Some(v) = params.spread_limit_prob {
            cfg.spread_limit_prob = v.clamp(0.005, 0.12);
        }
        if let Some(v) = params.entry_edge_prob {
            cfg.entry_edge_prob = v.clamp(0.002, 0.25);
        }
        if let Some(v) = params.entry_min_potential_cents {
            cfg.entry_min_potential_cents = v.clamp(1.0, 70.0);
        }
        if let Some(v) = params.entry_max_price_cents {
            cfg.entry_max_price_cents = v.clamp(45.0, 98.5);
        }
        if let Some(v) = params.min_hold_ms {
            cfg.min_hold_ms = v.clamp(0, 240_000);
        }
        if let Some(v) = params.stop_loss_cents {
            cfg.stop_loss_cents = v.clamp(2.0, 60.0);
        }
        if let Some(v) = params.reverse_signal_threshold {
            cfg.reverse_signal_threshold = v.clamp(-0.95, -0.02);
        }
        if let Some(v) = params.reverse_signal_ticks {
            cfg.reverse_signal_ticks = v.clamp(1, 12) as usize;
        }
        if let Some(v) = params.trail_activate_profit_cents {
            cfg.trail_activate_profit_cents = v.clamp(2.0, 80.0);
        }
        if let Some(v) = params.trail_drawdown_cents {
            cfg.trail_drawdown_cents = v.clamp(1.0, 50.0);
        }
        if let Some(v) = params.take_profit_near_max_cents {
            cfg.take_profit_near_max_cents = v.clamp(70.0, 99.5);
        }
        if let Some(v) = params.endgame_take_profit_cents {
            cfg.endgame_take_profit_cents = v.clamp(50.0, 99.0);
        }
        if let Some(v) = params.endgame_remaining_ms {
            cfg.endgame_remaining_ms = v.clamp(1_000, 180_000);
        }
        if let Some(v) = params.liquidity_widen_prob {
            cfg.liquidity_widen_prob = v.clamp(0.01, 0.2);
        }
        if let Some(v) = params.cooldown_ms {
            cfg.cooldown_ms = v.clamp(0, 120_000);
        }
        if let Some(v) = params.max_entries_per_round {
            cfg.max_entries_per_round = v.clamp(1, 16) as usize;
        }
        if let Some(v) = params.max_exec_spread_cents {
            cfg.max_exec_spread_cents = v.clamp(0.2, 30.0);
        }
        if let Some(v) = params.slippage_cents_per_side {
            cfg.slippage_cents_per_side = v.clamp(0.0, 10.0);
        }
        if let Some(v) = params.fee_cents_per_side {
            cfg.fee_cents_per_side = v.clamp(0.0, 12.0);
        }
        if let Some(v) = params.emergency_wide_spread_penalty_ratio {
            cfg.emergency_wide_spread_penalty_ratio = v.clamp(0.2, 3.0);
        }
        if let Some(v) = params.stop_loss_grace_ticks {
            cfg.stop_loss_grace_ticks = v.clamp(0, 8) as usize;
        }
        if let Some(v) = params.stop_loss_hard_mult {
            cfg.stop_loss_hard_mult = v.clamp(1.0, 3.0);
        }
        if let Some(v) = params.stop_loss_reverse_extra_ticks {
            cfg.stop_loss_reverse_extra_ticks = v.clamp(0, 6) as usize;
        }
        if let Some(v) = params.loss_cluster_limit {
            cfg.loss_cluster_limit = v.clamp(0, 8) as usize;
        }
        if let Some(v) = params.loss_cluster_cooldown_ms {
            cfg.loss_cluster_cooldown_ms = v.clamp(0, 120_000);
        }
        if let Some(v) = params.noise_gate_enabled {
            cfg.noise_gate_enabled = v;
        }
        if let Some(v) = params.noise_gate_threshold_add {
            cfg.noise_gate_threshold_add = v.clamp(0.0, 0.20);
        }
        if let Some(v) = params.noise_gate_edge_add {
            cfg.noise_gate_edge_add = v.clamp(0.0, 0.12);
        }
        if let Some(v) = params.noise_gate_spread_scale {
            cfg.noise_gate_spread_scale = v.clamp(0.5, 1.2);
        }
        if let Some(v) = params.vic_enabled {
            cfg.vic_enabled = v;
        }
        if let Some(v) = params.vic_target_entries_per_hour {
            cfg.vic_target_entries_per_hour = v.clamp(0.0, 120.0);
        }
        if let Some(v) = params.vic_deadband_ratio {
            cfg.vic_deadband_ratio = v.clamp(0.0, 0.8);
        }
        if let Some(v) = params.vic_threshold_relax_max {
            cfg.vic_threshold_relax_max = v.clamp(0.0, 0.2);
        }
        if let Some(v) = params.vic_edge_relax_max {
            cfg.vic_edge_relax_max = v.clamp(0.0, 0.1);
        }
        if let Some(v) = params.vic_spread_relax_max {
            cfg.vic_spread_relax_max = v.clamp(0.0, 0.8);
        }
        if cfg.entry_threshold_cap < cfg.entry_threshold_base {
            cfg.entry_threshold_cap = cfg.entry_threshold_base;
        }
    }

    let fixed_guard = strategy_fixed_guard_payload(&cfg, config_source);
    let runtime_defaults = LiveRuntimeConfig::from_env();
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
    let runtime_symbol = runtime_defaults.symbol.to_ascii_uppercase();
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
            return Ok(Json(payload));
        } else {
            let warmup_msg = "live runtime warming up (no snapshot yet)";
            if matches!(source_mode, StrategyPaperSource::Live) {
                return Err(ApiError::bad_request(warmup_msg));
            }
            source_fallback_error = Some(warmup_msg.to_string());
        }
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
        return Ok(Json(json!({
            "source": "replay",
            "execution_target": "paper",
            "live_enabled": fev1::ExecutionGate::from_env().live_enabled,
            "strategy_engine": "forge_fev1",
            "strategy_alias": "FEV1",
            "source_fallback_error": source_fallback_error,
            "market_type": market_type,
            "symbol": symbol,
            "fixed_guard": fixed_guard,
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
        })));
    }

    let run = run_strategy_simulation_with_fee_context(&samples, &cfg, max_trades, None);
    Ok(Json(json!({
        "source": "replay",
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
    })))
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
        let mut controls = state.live_runtime_controls.write().await;
        controls.clear();
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
            .put_live_position_state(
                &runtime_cfg.symbol,
                market,
                LivePositionState::flat(&runtime_cfg.symbol, market, now_ms),
            )
            .await;
        state
            .put_live_runtime_control(&runtime_cfg.symbol, market, LiveRuntimeControl::normal(now_ms))
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
        let control_key = live_runtime_control_key(&state.redis_prefix, &runtime_cfg.symbol, market);
        if delete_key(&state, &control_key).await.is_ok() {
            deleted_keys.push(control_key);
        }
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
    let order_latency = latency_stats_from_samples(&collect_event_latency_values(
        &filtered,
        "order_latency_ms",
    ));
    let signal_to_submit_latency = latency_stats_from_samples(&collect_event_latency_values(
        &filtered,
        "signal_to_submit_ms",
    ));
    let signal_to_ack_latency = latency_stats_from_samples(&collect_event_latency_values(
        &filtered,
        "signal_to_ack_ms",
    ));
    let submit_to_ack_latency = latency_stats_from_samples(&collect_event_latency_values(
        &filtered,
        "submit_to_ack_ms",
    ));

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
    let (active_payload, active_key) = resolve_autotune_active_doc(&state, market_type, symbol).await?;
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
