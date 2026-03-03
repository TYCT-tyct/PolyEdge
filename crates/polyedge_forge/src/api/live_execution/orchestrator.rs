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
    if !deferred.is_empty() {
        out.extend(deferred);
    }
    out
}
