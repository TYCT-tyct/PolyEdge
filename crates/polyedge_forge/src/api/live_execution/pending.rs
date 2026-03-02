pub(super) async fn apply_pending_confirmation(
    state: &ApiState,
    pending: &LivePendingOrder,
    reason: &str,
    fill_event: Option<&Value>,
) {
    let now_ms = Utc::now().timestamp_millis();
    let mut ps = state.get_live_position_state(&pending.market_type).await;
    let capital_cfg = LiveCapitalConfig::from_env();
    let action = pending.action.to_ascii_lowercase();
    let fill_meta = extract_pending_fill_meta(fill_event, pending);
    let fill_price_cents = fill_meta
        .fill_price_cents
        .unwrap_or(pending.price_cents)
        .max(0.01);
    let fill_size_shares = fill_meta
        .fill_size_shares
        .filter(|v| v.is_finite() && *v > 0.0);
    let fill_quote_usdc = fill_meta
        .fill_quote_usdc
        .or_else(|| fill_size_shares.map(|sz| sz * (fill_price_cents / 100.0)))
        .unwrap_or(pending.quote_size_usdc.max(0.0))
        .max(0.0);
    let fill_shares = if let Some(sz) = fill_size_shares {
        sz.max(0.0)
    } else if fill_meta.fill_quote_usdc.is_some() {
        (fill_quote_usdc / (fill_price_cents / 100.0).max(0.0001)).max(0.0)
    } else if pending.order_size_shares > 0.0 {
        pending.order_size_shares.max(0.0)
    } else {
        (fill_quote_usdc / (fill_price_cents / 100.0).max(0.0001)).max(0.0)
    };
    let fill_cost_usdc = (fill_meta.fee_usdc + fill_meta.slippage_usdc).max(0.0);
    let mut realized_pnl_usdc = 0.0_f64;
    if action == "enter" {
        ps.state = "in_position".to_string();
        ps.side = Some(pending.side.clone());
        ps.entry_round_id = Some(pending.round_id.clone());
        ps.entry_market_id = if pending.market_id.trim().is_empty() {
            None
        } else {
            Some(pending.market_id.trim().to_string())
        };
        ps.entry_token_id = if pending.token_id.trim().is_empty() {
            None
        } else {
            Some(pending.token_id.trim().to_string())
        };
        ps.entry_ts_ms = Some(pending.submitted_ts_ms);
        ps.entry_price_cents = Some(fill_price_cents);
        ps.entry_quote_usdc = Some(fill_quote_usdc);
        ps.net_quote_usdc = fill_quote_usdc;
        ps.vwap_entry_cents = Some(fill_price_cents);
        ps.open_add_layers = 0;
        ps.position_cost_usdc = fill_cost_usdc;
        ps.position_size_shares = fill_shares.max(0.0);
        ps.total_entries = ps.total_entries.saturating_add(1);
        ps.last_fill_pnl_usdc = 0.0;
    } else if action == "add" {
        let prev_quote = ps
            .net_quote_usdc
            .max(ps.entry_quote_usdc.unwrap_or(0.0))
            .max(0.0);
        let add_quote = fill_quote_usdc;
        let prev_price = ps
            .vwap_entry_cents
            .or(ps.entry_price_cents)
            .unwrap_or(fill_price_cents);
        let next_quote = prev_quote + add_quote;
        let next_price = if next_quote > 1e-9 {
            (prev_price * prev_quote + fill_price_cents * add_quote) / next_quote
        } else {
            fill_price_cents
        };
        ps.state = "in_position".to_string();
        ps.side = Some(pending.side.clone());
        ps.entry_round_id = Some(pending.round_id.clone());
        if ps
            .entry_market_id
            .as_deref()
            .map(|v| v.trim().is_empty())
            .unwrap_or(true)
            && !pending.market_id.trim().is_empty()
        {
            ps.entry_market_id = Some(pending.market_id.trim().to_string());
        }
        if ps
            .entry_token_id
            .as_deref()
            .map(|v| v.trim().is_empty())
            .unwrap_or(true)
            && !pending.token_id.trim().is_empty()
        {
            ps.entry_token_id = Some(pending.token_id.trim().to_string());
        }
        ps.entry_ts_ms = Some(pending.submitted_ts_ms);
        ps.entry_price_cents = Some(next_price);
        ps.vwap_entry_cents = Some(next_price);
        ps.entry_quote_usdc = Some(next_quote);
        ps.net_quote_usdc = next_quote;
        ps.total_adds = ps.total_adds.saturating_add(1);
        ps.open_add_layers = ps.open_add_layers.saturating_add(1);
        ps.position_cost_usdc += fill_cost_usdc;
        ps.position_size_shares = (ps.position_size_shares + fill_shares).max(0.0);
        ps.last_fill_pnl_usdc = 0.0;
    } else if action == "reduce" || action == "exit" {
        let entry_price_cents = ps
            .vwap_entry_cents
            .or(ps.entry_price_cents)
            .unwrap_or(0.0)
            .max(0.01);
        let prev_shares = ps.position_size_shares.max(0.0);
        let close_shares = if action == "exit" {
            prev_shares
        } else {
            fill_shares.max(0.0).min(prev_shares)
        };
        if close_shares > 1e-9 {
            let gross_pnl_usdc = close_shares * ((fill_price_cents - entry_price_cents) / 100.0);
            let entry_cost_alloc = if prev_shares > 1e-9 {
                ps.position_cost_usdc * (close_shares / prev_shares).clamp(0.0, 1.0)
            } else {
                0.0
            };
            realized_pnl_usdc = gross_pnl_usdc - entry_cost_alloc - fill_cost_usdc;
            ps.position_cost_usdc = (ps.position_cost_usdc - entry_cost_alloc).max(0.0);
            ps.realized_pnl_usdc += realized_pnl_usdc;
            ps.last_fill_pnl_usdc = realized_pnl_usdc;
        } else {
            ps.last_fill_pnl_usdc = 0.0;
        }
        let remaining_shares = (prev_shares - close_shares).max(0.0);
        let remaining_quote = (remaining_shares * (entry_price_cents / 100.0)).max(0.0);
        if action == "exit" || remaining_shares <= 1e-6 {
            ps.state = "flat".to_string();
            ps.side = None;
            ps.entry_round_id = None;
            ps.entry_market_id = None;
            ps.entry_token_id = None;
            ps.entry_ts_ms = None;
            ps.entry_price_cents = None;
            ps.entry_quote_usdc = None;
            ps.net_quote_usdc = 0.0;
            ps.vwap_entry_cents = None;
            ps.open_add_layers = 0;
            ps.position_cost_usdc = 0.0;
            ps.position_size_shares = 0.0;
            ps.total_exits = ps.total_exits.saturating_add(1);
        } else {
            ps.state = "in_position".to_string();
            ps.entry_quote_usdc = Some(remaining_quote);
            ps.net_quote_usdc = remaining_quote;
            ps.position_size_shares = remaining_shares;
            ps.open_add_layers = ps.open_add_layers.saturating_sub(1);
            ps.total_reduces = ps.total_reduces.saturating_add(1);
        }
    }
    ps.last_action = Some(format!("{}_{}", pending.action, pending.side));
    ps.last_reason = Some(reason.to_string());
    ps.updated_ts_ms = now_ms;
    state
        .put_live_position_state(&pending.market_type, ps)
        .await;
    if realized_pnl_usdc.is_finite() && realized_pnl_usdc.abs() > 1e-9 {
        let _ = state
            .update_capital_after_realized_pnl(
                &pending.market_type,
                &capital_cfg,
                realized_pnl_usdc,
            )
            .await;
    } else if action == "enter" || action == "add" {
        let _ = state
            .update_capital_after_realized_pnl(&pending.market_type, &capital_cfg, 0.0)
            .await;
    }
}

pub(super) async fn apply_pending_revert(
    state: &ApiState,
    pending: &LivePendingOrder,
    reason: &str,
) {
    let now_ms = Utc::now().timestamp_millis();
    let mut ps = state.get_live_position_state(&pending.market_type).await;
    let action = pending.action.to_ascii_lowercase();
    if action == "enter" {
        ps.state = "flat".to_string();
        ps.side = None;
        ps.entry_round_id = None;
        ps.entry_market_id = None;
        ps.entry_token_id = None;
        ps.entry_ts_ms = None;
        ps.entry_price_cents = None;
        ps.entry_quote_usdc = None;
        ps.net_quote_usdc = 0.0;
        ps.vwap_entry_cents = None;
        ps.open_add_layers = 0;
    } else if action == "exit" || action == "reduce" {
        ps.state = "in_position".to_string();
        if ps.side.is_none() {
            ps.side = Some(pending.side.clone());
        }
    }
    ps.last_action = Some(format!("{}_{}", pending.action, pending.side));
    ps.last_reason = Some(reason.to_string());
    ps.updated_ts_ms = now_ms;
    state
        .put_live_position_state(&pending.market_type, ps)
        .await;
}

pub(super) async fn reconcile_gateway_reports(state: &ApiState, gateway_cfg: &LiveGatewayConfig) {
    let since_seq = state.get_gateway_report_seq().await;
    let client = state.gateway_http_client.as_ref();
    let mut endpoint = gateway_cfg.primary_url.clone();
    let mut response =
        get_gateway_reports(client, gateway_cfg.timeout_ms, &endpoint, since_seq, 240).await;
    if response.is_err() {
        if let Some(backup) = gateway_cfg.backup_url.as_deref() {
            endpoint = backup.to_string();
            response =
                get_gateway_reports(client, gateway_cfg.timeout_ms, &endpoint, since_seq, 240)
                    .await;
        }
    }
    let Ok(payload) = response else {
        return;
    };
    let latest_seq = payload
        .get("latest_seq")
        .and_then(Value::as_i64)
        .unwrap_or(since_seq);
    let events = payload
        .get("events")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for ev in events {
        let event_name = ev
            .get("event")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        let order_id = ev
            .get("order_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if order_id.is_empty() {
            continue;
        }
        let pending = state.remove_pending_order(&order_id).await;
        let Some(pending) = pending else {
            continue;
        };
        let state_text = ev
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        let filled_terminal = matches!(state_text.as_str(), "filled" | "executed" | "matched");
        let canceled_terminal = matches!(
            state_text.as_str(),
            "cancelled" | "canceled" | "rejected" | "expired"
        );
        if event_name == "order_terminal" && filled_terminal {
            apply_pending_confirmation(state, &pending, "order_terminal_filled", Some(&ev)).await;
        } else if event_name == "order_terminal" && canceled_terminal {
            apply_pending_revert(state, &pending, "order_terminal_canceled").await;
        } else if event_name == "order_timeout_cancelled" {
            apply_pending_revert(state, &pending, "timeout_cancelled").await;
        } else if event_name == "order_timeout_cancel_failed" {
            handle_cancel_failure_with_escalation(
                state,
                &pending,
                "timeout_cancel_failed",
                json!({
                    "gateway_endpoint": endpoint,
                    "event": ev
                }),
            )
            .await;
            continue;
        } else {
            let retry = pending.clone();
            state.upsert_pending_order(retry.clone()).await;
            state
                .append_live_event(
                    &retry.market_type,
                    json!({
                        "accepted": false,
                        "action": retry.action,
                        "side": retry.side,
                        "round_id": retry.round_id,
                        "reason": format!("gateway_event_ignored:{event_name}"),
                        "order_id": retry.order_id,
                        "gateway_endpoint": endpoint
                    }),
                )
                .await;
            continue;
        }
        state
            .append_live_event(
                &pending.market_type,
                json!({
                    "accepted": true,
                    "action": pending.action,
                    "side": pending.side,
                    "round_id": pending.round_id,
                    "reason": format!("gateway_event:{event_name}"),
                    "order_id": pending.order_id,
                    "gateway_endpoint": endpoint,
                    "event": ev
                }),
            )
            .await;
    }
    state.set_gateway_report_seq(latest_seq).await;
}

pub(super) async fn handle_pending_timeouts(state: &ApiState, gateway_cfg: &LiveGatewayConfig) {
    let now_ms = Utc::now().timestamp_millis();
    let pending = state.list_pending_orders().await;
    if pending.is_empty() {
        return;
    }
    let client = state.gateway_http_client.as_ref();
    for row in pending {
        let elapsed = now_ms.saturating_sub(row.submitted_ts_ms);
        if elapsed < pending_cancel_due_ms(&row) {
            continue;
        }
        let mut endpoint = gateway_cfg.primary_url.clone();
        let mut cancel_res =
            delete_gateway_order(client, gateway_cfg.timeout_ms, &endpoint, &row.order_id).await;
        if cancel_res.is_err() {
            if let Some(backup) = gateway_cfg.backup_url.as_deref() {
                endpoint = backup.to_string();
                cancel_res =
                    delete_gateway_order(client, gateway_cfg.timeout_ms, &endpoint, &row.order_id)
                        .await;
            }
        }
        match cancel_res {
            Ok(v) => {
                let _ = state.remove_pending_order(&row.order_id).await;
                let maker_tif = matches!(
                    row.tif.to_ascii_uppercase().as_str(),
                    "GTD" | "GTC" | "POST_ONLY"
                );
                let exit_like = is_live_exit_action(&row.action);
                let emergency_exit = exit_like && is_emergency_exit_reason(&row.submit_reason);
                let can_retry = if emergency_exit {
                    row.retry_count < 2
                } else {
                    row.retry_count < 1
                };
                if (maker_tif || emergency_exit) && can_retry {
                    let entry_like = is_entry_action(&row.action);
                    let fallback_ttl_ms = if emergency_exit {
                        700
                    } else if entry_like {
                        live_entry_fak_ttl_ms()
                    } else {
                        900
                    };
                    let fallback_slippage_bps = if emergency_exit {
                        (gateway_cfg.exit_slippage_bps + 18.0).max(38.0)
                    } else if entry_like {
                        gateway_cfg
                            .exit_slippage_bps
                            .max(gateway_cfg.entry_slippage_bps)
                            + live_entry_fak_slippage_boost_bps()
                    } else {
                        gateway_cfg
                            .exit_slippage_bps
                            .max(gateway_cfg.entry_slippage_bps)
                            + 10.0
                    };
                    let maybe_target =
                        resolve_live_market_target_with_state(state, &row.market_type)
                            .await
                            .ok();
                    if let Some(target) = maybe_target {
                        let mut effective_target = target.clone();
                        apply_pending_target_override(&mut effective_target, &row);
                        let round_check = json!({ "round_id": row.round_id });
                        let bypass_round_guard = is_live_exit_action(&row.action)
                            && !row.market_id.trim().is_empty()
                            && !row.token_id.trim().is_empty();
                        if !decision_round_matches_target(&round_check, &effective_target)
                            && !bypass_round_guard
                        {
                            state
                                .append_live_event(
                                    &row.market_type,
                                    json!({
                                        "accepted": false,
                                        "action": row.action,
                                        "side": row.side,
                                        "round_id": row.round_id,
                                        "reason": "local_timeout_cancelled_round_target_mismatch",
                                        "order_id": row.order_id,
                                        "target_market_id": effective_target.market_id,
                                        "target_end_date": effective_target.end_date
                                    }),
                                )
                                .await;
                            apply_pending_revert(state, &row, "local_timeout_cancel").await;
                            continue;
                        }
                        let decision = json!({
                            "action": row.action,
                            "side": row.side,
                            "round_id": row.round_id,
                            "price_cents": row.price_cents,
                            "quote_size_usdc": row.quote_size_usdc,
                            "reason": format!("{}_timeout_fallback_fak", row.submit_reason),
                            "tif": "FAK",
                            "style": "taker",
                            "ttl_ms": fallback_ttl_ms,
                            "max_slippage_bps": fallback_slippage_bps
                        });
                        let token_id =
                            token_id_for_decision(&decision, &effective_target).map(str::to_string);
                        let book_snapshot = if let Some(token_id) = token_id {
                            fetch_gateway_book_snapshot(client, gateway_cfg, &token_id).await
                        } else {
                            None
                        };
                        if let Some(payload) = decision_to_live_payload(
                            &decision,
                            &effective_target,
                            gateway_cfg,
                            book_snapshot.as_ref(),
                            Some(row.quote_size_usdc),
                        ) {
                            let (submit_res, submit_endpoint) =
                                submit_gateway_order(client, gateway_cfg, &payload).await;
                            if let Ok(resp) = submit_res {
                                let accepted = resp
                                    .get("accepted")
                                    .and_then(Value::as_bool)
                                    .unwrap_or(false);
                                if accepted {
                                    let order_id = extract_order_id_from_gateway_record(&resp)
                                        .or_else(|| {
                                            extract_order_id_from_gateway_record(
                                                &json!({ "response": resp.clone() }),
                                            )
                                        });
                                    if let Some(order_id) = order_id {
                                        let mut pending = row.clone();
                                        pending.order_id = order_id;
                                        pending.retry_count = pending.retry_count.saturating_add(1);
                                        pending.tif = "FAK".to_string();
                                        pending.style = "taker".to_string();
                                        pending.submitted_ts_ms = now_ms;
                                        pending.cancel_after_ms = if emergency_exit {
                                            700
                                        } else if entry_like {
                                            live_entry_fak_cancel_after_ms()
                                        } else {
                                            1500
                                        };
                                        state.upsert_pending_order(pending).await;
                                        state
                                            .append_live_event(
                                                &row.market_type,
                                                json!({
                                                    "accepted": true,
                                                    "action": row.action,
                                                    "side": row.side,
                                                    "round_id": row.round_id,
                                                    "reason": "local_timeout_cancelled_resubmitted_fak",
                                                    "order_id": row.order_id,
                                                    "gateway_endpoint": submit_endpoint,
                                                    "cancel_response": v.clone(),
                                                    "submit_response": resp,
                                                    "book_snapshot": book_snapshot,
                                                    "emergency_exit": emergency_exit
                                                }),
                                            )
                                            .await;
                                        continue;
                                    }
                                }
                                state
                                    .append_live_event(
                                        &row.market_type,
                                        json!({
                                            "accepted": false,
                                            "action": row.action,
                                            "side": row.side,
                                            "round_id": row.round_id,
                                            "reason": "local_timeout_cancelled_fak_resubmit_rejected",
                                            "order_id": row.order_id,
                                            "gateway_endpoint": submit_endpoint,
                                            "cancel_response": v.clone(),
                                            "submit_response": resp
                                        }),
                                    )
                                    .await;
                            }
                        }
                    }
                }
                apply_pending_revert(state, &row, "local_timeout_cancel").await;
                state
                    .append_live_event(
                        &row.market_type,
                        json!({
                            "accepted": false,
                            "action": row.action,
                            "side": row.side,
                            "round_id": row.round_id,
                            "reason": "local_timeout_cancelled",
                            "order_id": row.order_id,
                            "gateway_endpoint": endpoint,
                            "response": v
                        }),
                    )
                    .await;
            }
            Err(err) => {
                handle_cancel_failure_with_escalation(
                    state,
                    &row,
                    "local_timeout_cancel_failed",
                    json!({
                        "gateway_endpoint": endpoint,
                        "error": err
                    }),
                )
                .await;
            }
        }
    }
}

pub(super) fn token_id_for_decision<'a>(
    decision: &Value,
    target: &'a LiveMarketTarget,
) -> Option<&'a str> {
    let action = decision
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let side = decision
        .get("side")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_uppercase();
    match (action.as_str(), side.as_str()) {
        ("enter", "UP") | ("add", "UP") | ("reduce", "UP") | ("exit", "UP") => {
            Some(target.token_id_yes.as_str())
        }
        ("enter", "DOWN") | ("add", "DOWN") | ("reduce", "DOWN") | ("exit", "DOWN") => {
            Some(target.token_id_no.as_str())
        }
        _ => None,
    }
}

fn align_exit_decision_side_with_position(
    decision: &mut Value,
    position_state: &LivePositionState,
) -> bool {
    let action = decision
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !is_live_exit_action(&action) {
        return false;
    }
    let Some(locked_side) = position_state.side.as_deref() else {
        return false;
    };
    let locked_side = locked_side.trim().to_ascii_uppercase();
    if locked_side.is_empty() {
        return false;
    }
    let current_side = decision
        .get("side")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase();
    if current_side == locked_side {
        return false;
    }
    if let Some(obj) = decision.as_object_mut() {
        obj.insert("side".to_string(), json!(locked_side));
        obj.insert(
            "execution_notes_override".to_string(),
            json!("exit_side_aligned_to_locked_position"),
        );
        return true;
    }
    false
}

fn apply_pending_target_override(target: &mut LiveMarketTarget, row: &LivePendingOrder) {
    if !row.market_id.trim().is_empty() {
        target.market_id = row.market_id.trim().to_string();
    }
    if !row.token_id.trim().is_empty() {
        let token = row.token_id.trim().to_string();
        if row.side.eq_ignore_ascii_case("UP") {
            target.token_id_yes = token.clone();
            if target.token_id_no.trim().is_empty() {
                target.token_id_no = token;
            }
        } else if row.side.eq_ignore_ascii_case("DOWN") {
            target.token_id_no = token.clone();
            if target.token_id_yes.trim().is_empty() {
                target.token_id_yes = token;
            }
        } else {
            target.token_id_yes = token.clone();
            target.token_id_no = token;
        }
    }
    if let Some(end_ms) = parse_round_end_ts_ms(&row.round_id) {
        target.end_date = end_date_iso_from_ms(end_ms);
    }
}

fn build_effective_target_for_decision(
    target: &LiveMarketTarget,
    decision: &Value,
    position_state: &LivePositionState,
) -> LiveMarketTarget {
    let mut effective = target.clone();
    let action = decision
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !is_live_exit_action(&action) {
        return effective;
    }
    if let Some(mid) = position_state.entry_market_id.as_deref() {
        let mid = mid.trim();
        if !mid.is_empty() {
            effective.market_id = mid.to_string();
        }
    }
    if let Some(token) = position_state.entry_token_id.as_deref() {
        let token = token.trim();
        if !token.is_empty() {
            let pos_side = position_state
                .side
                .as_deref()
                .unwrap_or_default()
                .trim()
                .to_ascii_uppercase();
            if pos_side == "DOWN" {
                effective.token_id_no = token.to_string();
                if effective.token_id_yes.trim().is_empty() {
                    effective.token_id_yes = token.to_string();
                }
            } else {
                effective.token_id_yes = token.to_string();
                if effective.token_id_no.trim().is_empty() {
                    effective.token_id_no = token.to_string();
                }
            }
        }
    }
    if let Some(end_ms) = position_state
        .entry_round_id
        .as_deref()
        .and_then(parse_round_end_ts_ms)
    {
        effective.end_date = end_date_iso_from_ms(end_ms);
    }
    effective
}

fn should_bypass_round_match_for_locked_exit(
    decision: &Value,
    position_state: &LivePositionState,
) -> bool {
    let action = decision
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    is_live_exit_action(&action)
        && position_state.position_size_shares > 0.0
        && position_state
            .entry_market_id
            .as_deref()
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false)
        && position_state
            .entry_token_id
            .as_deref()
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false)
}

pub(super) fn build_position_locked_target(
    market_type: &str,
    position_state: &LivePositionState,
) -> Option<LiveMarketTarget> {
    let market_id = position_state.entry_market_id.as_deref()?.trim().to_string();
    let token_id = position_state.entry_token_id.as_deref()?.trim().to_string();
    if market_id.is_empty() || token_id.is_empty() {
        return None;
    }
    let (token_id_yes, token_id_no) = (token_id.clone(), token_id.clone());
    let symbol = position_state
        .entry_round_id
        .as_deref()
        .and_then(|rid| rid.split('_').next())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("BTCUSDT")
        .to_string();
    let end_date = position_state
        .entry_round_id
        .as_deref()
        .and_then(parse_round_end_ts_ms)
        .and_then(end_date_iso_from_ms);
    Some(LiveMarketTarget {
        market_id,
        symbol,
        timeframe: market_type.to_string(),
        token_id_yes,
        token_id_no,
        end_date,
    })
}

pub(super) async fn execute_live_orders_via_gateway(
    state: &ApiState,
    gateway_cfg: &LiveGatewayConfig,
    target: &LiveMarketTarget,
    position_state: &LivePositionState,
    decisions: &[LiveGatedDecision],
) -> Vec<Value> {
    let client = state.gateway_http_client.as_ref();
    let mut out = Vec::<Value>::with_capacity(decisions.len());
    let token_ids = collect_decision_token_ids(target, decisions);
    let mut book_cache = prefetch_gateway_books_for_tokens(client, gateway_cfg, &token_ids).await;
    let exit_quote_override =
        position_state
            .entry_quote_usdc
            .and_then(|v| if v > 0.0 { Some(v) } else { None });
    let exit_size_override = if position_state.position_size_shares > 0.0 {
        Some(position_state.position_size_shares)
    } else {
        None
    };
    for gated in decisions {
        let mut decision = gated.decision.clone();
        let _side_aligned = align_exit_decision_side_with_position(&mut decision, position_state);
        let action = decision
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        let effective_target = build_effective_target_for_decision(target, &decision, position_state);
        let bypass_round_guard = should_bypass_round_match_for_locked_exit(&decision, position_state);
        if !decision_round_matches_target(&decision, &effective_target) && !bypass_round_guard {
            out.push(json!({
                "ok": false,
                "reason": "round_target_mismatch",
                "decision_key": gated.decision_key,
                "decision_round_id": decision.get("round_id").and_then(Value::as_str),
                "target_market_id": effective_target.market_id,
                "target_end_date": effective_target.end_date,
                "decision": decision
            }));
            continue;
        }
        if action == "exit" {
            if let Some(obj) = decision.as_object_mut() {
                if let Some(q) = exit_quote_override {
                    obj.insert("quote_size_usdc".to_string(), json!(q));
                }
                if let Some(sz) = exit_size_override {
                    obj.insert("position_size_shares".to_string(), json!(sz));
                }
            }
        }
        apply_emergency_exit_overrides(&mut decision, gateway_cfg);
        let token_id = token_id_for_decision(&decision, &effective_target).map(str::to_string);
        let book_snapshot = if let Some(token_id) = token_id.clone() {
            match book_cache.get(&token_id) {
                Some(cached) => cached.clone(),
                None => {
                    let fetched = fetch_gateway_book_snapshot(client, gateway_cfg, &token_id).await;
                    book_cache.insert(token_id.clone(), fetched.clone());
                    fetched
                }
            }
        } else {
            None
        };
        let Some(payload) =
            decision_to_live_payload(
                &decision,
                &effective_target,
                gateway_cfg,
                book_snapshot.as_ref(),
                None,
            )
        else {
            out.push(json!({
                "ok": false,
                "reason": "decision_to_payload_failed",
                "decision_key": gated.decision_key,
                "decision": decision,
                "book_snapshot": book_snapshot
            }));
            continue;
        };
        let mut attempt_payload = payload.clone();
        let mut attempts = Vec::<Value>::with_capacity(3);
        let mut accepted = false;
        let mut final_endpoint = gateway_cfg.primary_url.clone();
        let mut final_response: Option<Value> = None;
        let mut final_error: Option<String> = None;

        let order_started = Instant::now();
        for attempt in 0..3usize {
            let attempt_started = Instant::now();
            let (result, used_endpoint) =
                submit_gateway_order(client, gateway_cfg, &attempt_payload).await;
            final_endpoint = used_endpoint.clone();
            let latency_ms = attempt_started.elapsed().as_millis() as u64;
            match result {
                Ok(v) => {
                    let accept = v.get("accepted").and_then(Value::as_bool).unwrap_or(false);
                    let reject_reason = if accept {
                        String::new()
                    } else {
                        extract_gateway_reject_reason(&v)
                    };
                    attempts.push(json!({
                        "attempt": attempt + 1,
                        "endpoint": used_endpoint,
                        "latency_ms": latency_ms,
                        "request": attempt_payload.clone(),
                        "accepted": accept,
                        "reject_reason": if reject_reason.is_empty() { Value::Null } else { json!(reject_reason.clone()) },
                        "response": v
                    }));
                    final_response = Some(v.clone());
                    if accept {
                        accepted = true;
                        break;
                    }
                    if let Some(next_payload) =
                        build_retry_payload(&attempt_payload, &reject_reason, attempt)
                    {
                        attempt_payload = next_payload;
                        continue;
                    }
                    break;
                }
                Err(err) => {
                    attempts.push(json!({
                        "attempt": attempt + 1,
                        "endpoint": used_endpoint,
                        "latency_ms": latency_ms,
                        "request": attempt_payload.clone(),
                        "accepted": false,
                        "error": err
                    }));
                    final_error = Some(err.clone());
                    if let Some(next_payload) = build_retry_payload(&attempt_payload, &err, attempt)
                    {
                        attempt_payload = next_payload;
                        continue;
                    }
                    break;
                }
            }
        }
        out.push(json!({
            "ok": accepted,
            "accepted": accepted,
            "decision_key": gated.decision_key,
            "decision": decision,
            "endpoint": final_endpoint,
            "request": payload,
            "final_request": attempt_payload.clone(),
            "response": final_response,
            "error": final_error,
            "attempts": attempts,
            "order_latency_ms": order_started.elapsed().as_millis() as u64,
            "book_snapshot": book_snapshot,
            "target_market_id": effective_target.market_id,
            "round_guard_bypassed": bypass_round_guard,
            "price_trace": build_execution_price_trace(
                &decision,
                &payload,
                &attempt_payload,
                final_response.as_ref()
            )
        }));
    }
    out
}

pub(super) async fn fetch_live_balance_snapshot(
    state: &ApiState,
) -> Result<LiveBalanceSnapshot, String> {
    const USDC_SCALE: f64 = 1_000_000.0;
    let ctx = get_or_init_rust_executor(state).await?;
    let req = PmBalanceAllowanceRequest::builder()
        .asset_type(PmAssetType::Collateral)
        .build();
    let response = ctx
        .client
        .balance_allowance(req)
        .await
        .map_err(|e| format!("balance_allowance failed: {e}"))?;
    let account = ctx.client.address().to_string();
    let allowance_raw = response
        .allowances
        .iter()
        .find_map(|(addr, amount)| {
            if addr.to_string().eq_ignore_ascii_case(&account) {
                amount.parse::<f64>().ok()
            } else {
                None
            }
        })
        .or_else(|| {
            response
                .allowances
                .values()
                .filter_map(|v| v.parse::<f64>().ok())
                .fold(None, |acc, v| match acc {
                    Some(prev) if prev >= v => Some(prev),
                    _ => Some(v),
                })
        })
        .unwrap_or(0.0);
    let now_ms = Utc::now().timestamp_millis();
    let balance_raw = pm_dec_to_f64(&response.balance).max(0.0);
    let balance = (balance_raw / USDC_SCALE).max(0.0);
    let allowance = (allowance_raw / USDC_SCALE).max(0.0);
    let spendable = balance.min(allowance).max(0.0);
    let raw_allowances = response
        .allowances
        .iter()
        .map(|(addr, amount)| (addr.to_string(), amount.clone()))
        .collect::<HashMap<String, String>>();
    let raw = json!({
        "balance_raw": balance_raw,
        "balance": balance,
        "allowance_raw": allowance_raw,
        "allowances": raw_allowances,
        "account": account,
        "asset_type": "COLLATERAL"
    });
    Ok(LiveBalanceSnapshot {
        fetched_at_ms: now_ms,
        source: "clob_balance_allowance".to_string(),
        balance_usdc: balance,
        allowance_usdc: allowance,
        spendable_usdc: spendable,
        raw,
    })
}

