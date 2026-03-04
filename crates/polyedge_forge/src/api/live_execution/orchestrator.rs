fn json_value_as_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|s| s.trim().parse::<f64>().ok()))
        .filter(|v| v.is_finite())
}

fn json_pick_f64(node: Option<&Value>, keys: &[&str]) -> Option<f64> {
    let source = node?;
    for key in keys {
        if let Some(v) = source.get(*key).and_then(json_value_as_f64) {
            return Some(v);
        }
    }
    None
}

fn normalize_ratio_or_pct(v: f64) -> f64 {
    if v > 1.0 {
        (v / 100.0).clamp(0.0, 1.0)
    } else {
        v.clamp(0.0, 1.0)
    }
}

fn deploy_stage_name(stage: DeployStage) -> &'static str {
    match stage {
        DeployStage::Shadow => "shadow",
        DeployStage::Canary => "canary",
        DeployStage::Full => "full",
    }
}

pub(super) async fn reconcile_live_reports(state: &ApiState, exec_cfg: &LiveExecutionConfig) {
    reconcile_rust_reports(state, exec_cfg).await;
}

pub(super) async fn handle_live_pending_timeouts(state: &ApiState, exec_cfg: &LiveExecutionConfig) {
    handle_pending_timeouts_rust(state, exec_cfg).await;
}

pub(super) async fn flush_entry_pending_before_exit(
    state: &ApiState,
    symbol: &str,
    market_type: &str,
) -> usize {
    let pending_rows = state
        .list_pending_orders_for_market(symbol, market_type)
        .await
        .into_iter()
        .filter(|row| {
            let action = row.action.to_ascii_lowercase();
            action == "enter" || action == "add"
        })
        .collect::<Vec<_>>();
    if pending_rows.is_empty() {
        return 0;
    }

    let Some(ctx) = get_or_init_rust_executor(state).await.ok() else {
        tracing::warn!(
            market_type = market_type,
            "skip pre-exit pending flush: rust sdk executor unavailable"
        );
        return 0;
    };

    let mut cancelled = 0usize;
    for row in pending_rows {
        match ctx.client.cancel_order(&row.order_id).await {
            Ok(resp) => {
                let _ = state.remove_pending_order(&row.order_id).await;
                apply_pending_revert(state, &row, "pre_exit_cancelled").await;
                cancelled = cancelled.saturating_add(1);
                state
                    .append_live_event(
                        &row.symbol,
                        &row.market_type,
                        json!({
                            "accepted": true,
                            "action": row.action,
                            "side": row.side,
                            "round_id": row.round_id,
                            "decision_id": row.decision_id,
                            "decision_key": row.decision_key,
                            "reason": "pre_exit_cancelled",
                            "order_id": row.order_id,
                            "cancelled": resp.canceled,
                            "not_cancelled": resp.not_canceled
                        }),
                    )
                    .await;
            }
            Err(err) => {
                state
                    .append_live_event(
                        &row.symbol,
                        &row.market_type,
                        json!({
                            "accepted": false,
                            "action": row.action,
                            "side": row.side,
                            "round_id": row.round_id,
                            "decision_id": row.decision_id,
                            "decision_key": row.decision_key,
                            "reason": "pre_exit_cancel_failed",
                            "order_id": row.order_id,
                            "error": err.to_string()
                        }),
                    )
                    .await;
            }
        }
    }

    cancelled
}

pub(super) async fn execute_live_orders(
    state: &ApiState,
    exec_cfg: &LiveExecutionConfig,
    symbol: &str,
    market_type: &str,
    target: &LiveMarketTarget,
    position_state: &LivePositionState,
    decisions: &[LiveGatedDecision],
) -> Vec<Value> {
    let deploy_stage = DeployStage::from_env();
    let deploy_fraction = deploy_stage.capital_fraction();
    let runtime_snapshot = state.get_runtime_snapshot(symbol, market_type).await;
    let runtime_summary = runtime_snapshot
        .as_ref()
        .and_then(|v| v.get("summary"));
    let portfolio_value = json_pick_f64(
        runtime_summary,
        &[
            "portfolio_value_usdc",
            "portfolio_value",
            "equity_usdc",
            "balance_usdc",
            "capital_usdc",
        ],
    )
    .or_else(|| {
        json_pick_f64(
            runtime_snapshot.as_ref(),
            &[
                "portfolio_value_usdc",
                "portfolio_value",
                "equity_usdc",
                "balance_usdc",
                "capital_usdc",
            ],
        )
    })
    .unwrap_or_else(|| {
        position_state
            .entry_quote_usdc
            .unwrap_or(exec_cfg.min_quote_usdc)
            .max(position_state.position_cost_usdc)
            .max(exec_cfg.min_quote_usdc)
    });
    let win_rate = json_pick_f64(runtime_summary, &["win_rate", "win_rate_pct"])
        .or_else(|| json_pick_f64(runtime_snapshot.as_ref(), &["win_rate", "win_rate_pct"]))
        .map(normalize_ratio_or_pct)
        .unwrap_or(0.50);
    let avg_win_loss_ratio = json_pick_f64(
        runtime_summary,
        &["avg_win_loss_ratio", "win_loss_ratio", "profit_factor"],
    )
    .or_else(|| {
        json_pick_f64(
            runtime_snapshot.as_ref(),
            &["avg_win_loss_ratio", "win_loss_ratio", "profit_factor"],
        )
    })
    .unwrap_or(1.0)
    .max(0.01);
    let current_drawdown = json_pick_f64(
        runtime_summary,
        &[
            "current_drawdown",
            "current_drawdown_pct",
            "drawdown",
            "drawdown_pct",
        ],
    )
    .or_else(|| {
        json_pick_f64(
            runtime_snapshot.as_ref(),
            &[
                "current_drawdown",
                "current_drawdown_pct",
                "drawdown",
                "drawdown_pct",
            ],
        )
    })
    .map(normalize_ratio_or_pct)
    .unwrap_or(0.0);
    let daily_drawdown = json_pick_f64(
        runtime_summary,
        &["daily_drawdown", "daily_drawdown_pct", "drawdown_today", "drawdown_today_pct"],
    )
    .or_else(|| {
        json_pick_f64(
            runtime_snapshot.as_ref(),
            &[
                "daily_drawdown",
                "daily_drawdown_pct",
                "drawdown_today",
                "drawdown_today_pct",
            ],
        )
    })
    .map(normalize_ratio_or_pct)
    .unwrap_or(0.0);
    let mut prioritized = decisions.to_vec();
    prioritized.sort_by_key(|d| {
        let action = d
            .decision
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        if is_live_exit_action(&action) {
            0_u8
        } else if action == "enter" || action == "add" {
            2_u8
        } else {
            1_u8
        }
    });
    let has_exit_like = prioritized.iter().any(|d| {
        d.decision
            .get("action")
            .and_then(Value::as_str)
            .map(|a| {
                let a = a.to_ascii_lowercase();
                is_live_exit_action(&a)
            })
            .unwrap_or(false)
    });
    let mut deferred = Vec::<Value>::new();
    if has_exit_like {
        prioritized.retain(|d| {
            let action = d
                .decision
                .get("action")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_ascii_lowercase();
            if action == "enter" || action == "add" {
                deferred.push(json!({
                    "ok": false,
                    "accepted": false,
                    "decision_key": d.decision_key,
                    "decision": d.decision,
                    "reason": "deferred_due_exit_priority"
                }));
                false
            } else {
                true
            }
        });
    }
    if has_exit_like {
        let _ = flush_entry_pending_before_exit(state, symbol, market_type).await;
    }
    if should_circuit_break(
        current_drawdown,
        daily_drawdown,
        exec_cfg.max_daily_drawdown,
        exec_cfg.max_total_drawdown,
    ) {
        let mut blocked = prioritized
            .into_iter()
            .map(|d| {
                json!({
                    "ok": false,
                    "accepted": false,
                    "decision_key": d.decision_key,
                    "decision": d.decision,
                    "reason": "circuit_break_triggered",
                    "current_drawdown": current_drawdown,
                    "daily_drawdown": daily_drawdown,
                    "max_daily_drawdown": exec_cfg.max_daily_drawdown,
                    "max_total_drawdown": exec_cfg.max_total_drawdown
                })
            })
            .collect::<Vec<_>>();
        if !deferred.is_empty() {
            blocked.extend(deferred);
        }
        return blocked;
    }

    let kelly_quote = quantize_usdc_micros(
        compute_kelly_size(
            portfolio_value,
            win_rate,
            avg_win_loss_ratio,
            exec_cfg.kelly_fraction,
            exec_cfg.kelly_stage1_cap,
            exec_cfg.kelly_stage2_cap,
            exec_cfg.kelly_stage1_fraction,
            exec_cfg.kelly_stage2_fraction,
        ) * deploy_fraction,
    );

    let mut adjusted = Vec::<LiveGatedDecision>::with_capacity(prioritized.len());
    let mut sizing_rejected = Vec::<Value>::new();
    for mut gated in prioritized {
        let action = gated
            .decision
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        if action == "enter" || action == "add" {
            if deploy_fraction <= 0.0 {
                sizing_rejected.push(json!({
                    "ok": false,
                    "accepted": false,
                    "decision_key": gated.decision_key,
                    "decision": gated.decision,
                    "reason": "deploy_stage_blocks_live_size",
                    "deploy_stage": deploy_stage_name(deploy_stage),
                    "capital_fraction": deploy_fraction
                }));
                continue;
            }
            if kelly_quote < exec_cfg.min_quote_usdc {
                sizing_rejected.push(json!({
                    "ok": false,
                    "accepted": false,
                    "decision_key": gated.decision_key,
                    "decision": gated.decision,
                    "reason": "kelly_size_below_min_quote",
                    "kelly_quote_usdc": kelly_quote,
                    "min_quote_usdc": exec_cfg.min_quote_usdc,
                    "deploy_stage": deploy_stage_name(deploy_stage)
                }));
                continue;
            }
            if let Some(obj) = gated.decision.as_object_mut() {
                obj.insert("quote_size_usdc".to_string(), json!(kelly_quote));
                obj.insert("sizing_mode".to_string(), json!("kelly_dynamic"));
                obj.insert("deploy_stage".to_string(), json!(deploy_stage_name(deploy_stage)));
                obj.insert("deploy_capital_fraction".to_string(), json!(deploy_fraction));
                obj.insert("kelly_portfolio_value".to_string(), json!(portfolio_value));
                obj.insert("kelly_win_rate".to_string(), json!(win_rate));
                obj.insert("kelly_avg_win_loss_ratio".to_string(), json!(avg_win_loss_ratio));
                obj.insert("kelly_quote_usdc".to_string(), json!(kelly_quote));
            }
        }
        adjusted.push(gated);
    }

    let mut out =
        execute_live_orders_via_rust_sdk(state, exec_cfg, target, position_state, &adjusted).await;
    if !sizing_rejected.is_empty() {
        out.extend(sizing_rejected);
    }
    if has_exit_like {
        // Exit path hardening: if no exit pending remains but position is still open,
        // force one more taker exit to avoid residual exposure.
        let ps_after = state.get_live_position_state(symbol, market_type).await;
        let pending_after = state.list_pending_orders_for_market(symbol, market_type).await;
        let has_exit_pending = pending_after.iter().any(|row| is_live_exit_action(&row.action));
        if ps_after.position_size_shares > 1e-6
            && ps_after
                .side
                .as_deref()
                .map(|v| !v.trim().is_empty())
                .unwrap_or(false)
            && !has_exit_pending
        {
            let now_ms = Utc::now().timestamp_millis();
            let side = ps_after
                .side
                .as_deref()
                .unwrap_or("UP")
                .trim()
                .to_ascii_uppercase();
            let round_id = ps_after
                .entry_round_id
                .clone()
                .unwrap_or_else(|| format!("{}_{}_{}", symbol.to_ascii_uppercase(), market_type, now_ms));
            let decision_id = format!("force_flatten:{}:{}:{}", symbol, market_type, now_ms);
            state
                .append_live_event(
                    symbol,
                    market_type,
                    json!({
                        "accepted": true,
                        "action": "exit",
                        "side": side.clone(),
                        "round_id": round_id.clone(),
                        "decision_id": decision_id.clone(),
                        "decision_key": decision_id.clone(),
                        "reason": "flatten_residual_force_exit_triggered",
                        "position_size_shares": ps_after.position_size_shares
                    }),
                )
                .await;
            let decision = json!({
                "decision_id": decision_id.clone(),
                "action": "exit",
                "side": side,
                "round_id": round_id,
                "reason": "flatten_residual_force_exit",
                "quote_size_usdc": ps_after.entry_quote_usdc.unwrap_or(exec_cfg.min_quote_usdc),
                "position_size_shares": ps_after.position_size_shares,
                "tif": "FAK",
                "style": "taker",
                "ttl_ms": 700,
                "max_slippage_bps": (exec_cfg.exit_slippage_bps + 20.0).min(500.0)
            });
            let forced = LiveGatedDecision {
                decision,
                decision_key: decision_id.clone(),
            };
            let mut forced_out =
                execute_live_orders_via_rust_sdk(state, exec_cfg, target, &ps_after, &[forced]).await;
            out.append(&mut forced_out);
        }
    }
    if !deferred.is_empty() {
        out.extend(deferred);
    }
    out
}
