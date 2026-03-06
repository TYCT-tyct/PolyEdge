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
                match ctx.client.order(&row.order_id).await {
                    Ok(order) if pm_is_terminal_fill(&order.status) => {
                        let _ = state.remove_pending_order(&row.order_id).await;
                        let fill_event = fill_event_from_open_order(&order);
                        apply_pending_confirmation(
                            state,
                            &row,
                            "pre_exit_cancel_reconciled_fill",
                            Some(&fill_event),
                        )
                        .await;
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
                                    "reason": "pre_exit_cancel_reconciled_fill",
                                    "order_id": row.order_id,
                                    "cancelled": resp.canceled,
                                    "not_cancelled": resp.not_canceled,
                                    "status": format!("{}", order.status)
                                }),
                            )
                            .await;
                    }
                    Ok(order) if pm_is_terminal_reject(&order.status) => {
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
                                    "not_cancelled": resp.not_canceled,
                                    "status": format!("{}", order.status)
                                }),
                            )
                            .await;
                    }
                    Ok(order) => {
                        force_pause_for_uncertain_pending(
                            state,
                            &row,
                            "pre_exit_cancel_unresolved",
                            json!({
                                "cancelled": resp.canceled,
                                "not_cancelled": resp.not_canceled,
                                "status": format!("{}", order.status)
                            }),
                        )
                        .await;
                    }
                    Err(err) => {
                        if resp.canceled.iter().any(|id| id == &row.order_id) {
                            let _ = state.remove_pending_order(&row.order_id).await;
                            apply_pending_revert(state, &row, "pre_exit_cancelled").await;
                            cancelled = cancelled.saturating_add(1);
                        } else {
                            force_pause_for_uncertain_pending(
                                state,
                                &row,
                                "pre_exit_cancel_status_error",
                                json!({
                                    "cancelled": resp.canceled,
                                    "not_cancelled": resp.not_canceled,
                                    "error": err.to_string()
                                }),
                            )
                            .await;
                        }
                    }
                }
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
    let mut out =
        execute_live_orders_via_rust_sdk(state, exec_cfg, target, position_state, &prioritized)
            .await;
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
