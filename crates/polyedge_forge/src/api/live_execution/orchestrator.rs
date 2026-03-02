pub(super) async fn reconcile_live_reports(
    state: &ApiState,
    gateway_cfg: &LiveGatewayConfig,
    mode: LiveExecutorMode,
) {
    match mode {
        LiveExecutorMode::Gateway => reconcile_gateway_reports(state, gateway_cfg).await,
        LiveExecutorMode::RustSdk => reconcile_rust_reports(state, gateway_cfg).await,
    }
}

pub(super) async fn handle_live_pending_timeouts(
    state: &ApiState,
    gateway_cfg: &LiveGatewayConfig,
    mode: LiveExecutorMode,
) {
    match mode {
        LiveExecutorMode::Gateway => handle_pending_timeouts(state, gateway_cfg).await,
        LiveExecutorMode::RustSdk => handle_pending_timeouts_rust(state, gateway_cfg).await,
    }
}

pub(super) async fn flush_entry_pending_before_exit(
    state: &ApiState,
    gateway_cfg: &LiveGatewayConfig,
    mode: LiveExecutorMode,
    market_type: &str,
) -> usize {
    let pending_rows = state
        .list_pending_orders_for_market(market_type)
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

    let mut cancelled = 0usize;
    let client = state.gateway_http_client.as_ref();
    let rust_ctx = if matches!(mode, LiveExecutorMode::RustSdk) {
        get_or_init_rust_executor(state).await.ok()
    } else {
        None
    };

    for row in pending_rows {
        let mut cancel_ok = false;
        let mut cancel_detail = Value::Null;
        let mut cancel_path = "gateway".to_string();

        if let Some(ctx) = rust_ctx.as_ref() {
            cancel_path = "rust_sdk".to_string();
            match ctx.client.cancel_order(&row.order_id).await {
                Ok(resp) => {
                    cancel_ok = true;
                    cancel_detail = json!({
                        "cancelled": resp.canceled,
                        "not_cancelled": resp.not_canceled
                    });
                }
                Err(err) => {
                    cancel_detail = json!({ "rust_error": err.to_string() });
                }
            }
        }

        if !cancel_ok {
            let mut endpoint = gateway_cfg.primary_url.clone();
            let mut cancel_res =
                delete_gateway_order(client, gateway_cfg.timeout_ms, &endpoint, &row.order_id)
                    .await;
            if cancel_res.is_err() {
                if let Some(backup) = gateway_cfg.backup_url.as_deref() {
                    endpoint = backup.to_string();
                    cancel_res = delete_gateway_order(
                        client,
                        gateway_cfg.timeout_ms,
                        &endpoint,
                        &row.order_id,
                    )
                    .await;
                }
            }
            match cancel_res {
                Ok(v) => {
                    cancel_ok = true;
                    cancel_path = format!("gateway:{endpoint}");
                    cancel_detail = v;
                }
                Err(err) => {
                    if cancel_detail.is_null() {
                        cancel_detail = json!({ "gateway_error": err });
                    }
                }
            }
        }

        if cancel_ok {
            let _ = state.remove_pending_order(&row.order_id).await;
            apply_pending_revert(state, &row, "pre_exit_cancelled").await;
            cancelled = cancelled.saturating_add(1);
            state
                .append_live_event(
                    &row.market_type,
                    json!({
                        "accepted": true,
                        "action": row.action,
                        "side": row.side,
                        "round_id": row.round_id,
                        "reason": "pre_exit_cancelled",
                        "order_id": row.order_id,
                        "cancel_path": cancel_path,
                        "cancel_response": cancel_detail
                    }),
                )
                .await;
        } else {
            state
                .append_live_event(
                    &row.market_type,
                    json!({
                        "accepted": false,
                        "action": row.action,
                        "side": row.side,
                        "round_id": row.round_id,
                        "reason": "pre_exit_cancel_failed",
                        "order_id": row.order_id,
                        "cancel_path": cancel_path,
                        "cancel_response": cancel_detail
                    }),
                )
                .await;
        }
    }

    cancelled
}

pub(super) async fn execute_live_orders(
    state: &ApiState,
    gateway_cfg: &LiveGatewayConfig,
    mode: LiveExecutorMode,
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
        let _ = flush_entry_pending_before_exit(state, gateway_cfg, mode, market_type).await;
    }
    let mut out = match mode {
        LiveExecutorMode::Gateway => {
            execute_live_orders_via_gateway(
                state,
                gateway_cfg,
                target,
                position_state,
                &prioritized,
            )
            .await
        }
        LiveExecutorMode::RustSdk => {
            execute_live_orders_via_rust_sdk(
                state,
                gateway_cfg,
                target,
                position_state,
                &prioritized,
            )
            .await
        }
    };
    if !deferred.is_empty() {
        out.extend(deferred);
    }
    out
}

