pub(super) async fn apply_pending_confirmation(
    state: &ApiState,
    pending: &LivePendingOrder,
    reason: &str,
    fill_event: Option<&Value>,
) {
    let now_ms = Utc::now().timestamp_millis();
    let mut ps = state
        .get_live_position_state(&pending.symbol, &pending.market_type)
        .await;
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
    let fill_quote_usdc = quantize_usdc_micros(fill_quote_usdc);
    let fill_shares = if let Some(sz) = fill_size_shares {
        sz.max(0.0)
    } else if fill_meta.fill_quote_usdc.is_some() {
        (fill_quote_usdc / (fill_price_cents / 100.0).max(0.0001)).max(0.0)
    } else if pending.order_size_shares > 0.0 {
        pending.order_size_shares.max(0.0)
    } else {
        (fill_quote_usdc / (fill_price_cents / 100.0).max(0.0001)).max(0.0)
    };
    let fill_cost_usdc =
        quantize_usdc_micros((fill_meta.fee_usdc + fill_meta.slippage_usdc).max(0.0));
    if action == "enter" {
        ps.state = "in_position".to_string();
        ps.symbol = pending.symbol.trim().to_ascii_uppercase();
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
        ps.entry_ts_ms = Some(pending.ack_ts_ms.max(pending.submitted_ts_ms));
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
        let next_quote = quantize_usdc_micros(prev_quote + add_quote);
        let next_price = if next_quote > 1e-9 {
            (prev_price * prev_quote + fill_price_cents * add_quote) / next_quote
        } else {
            fill_price_cents
        };
        ps.state = "in_position".to_string();
        ps.symbol = pending.symbol.trim().to_ascii_uppercase();
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
        ps.entry_ts_ms = Some(pending.ack_ts_ms.max(pending.submitted_ts_ms));
        ps.entry_price_cents = Some(next_price);
        ps.vwap_entry_cents = Some(next_price);
        ps.entry_quote_usdc = Some(next_quote);
        ps.net_quote_usdc = next_quote;
        ps.total_adds = ps.total_adds.saturating_add(1);
        ps.open_add_layers = ps.open_add_layers.saturating_add(1);
        ps.position_cost_usdc =
            micros_to_usdc(usdc_to_micros(ps.position_cost_usdc) + usdc_to_micros(fill_cost_usdc));
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
            let gross_micro = usdc_to_micros(gross_pnl_usdc);
            let alloc_micro = usdc_to_micros(entry_cost_alloc);
            let fill_cost_micro = usdc_to_micros(fill_cost_usdc);
            let realized_micro = gross_micro - alloc_micro - fill_cost_micro;
            let next_cost_micro = (usdc_to_micros(ps.position_cost_usdc) - alloc_micro).max(0);
            ps.position_cost_usdc = micros_to_usdc(next_cost_micro);
            ps.realized_pnl_usdc =
                micros_to_usdc(usdc_to_micros(ps.realized_pnl_usdc) + realized_micro);
            ps.last_fill_pnl_usdc = micros_to_usdc(realized_micro);
        } else {
            ps.last_fill_pnl_usdc = 0.0;
        }
        let remaining_shares = (prev_shares - close_shares).max(0.0);
        let remaining_quote =
            quantize_usdc_micros((remaining_shares * (entry_price_cents / 100.0)).max(0.0));
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
        .put_live_position_state(&pending.symbol, &pending.market_type, ps)
        .await;
    if fill_meta.size_guard_triggered {
        let now_ms = Utc::now().timestamp_millis();
        let mut control = state
            .get_live_runtime_control(&pending.symbol, &pending.market_type)
            .await;
        if control.mode != LiveRuntimeControlMode::ForcePause {
            control.mode = LiveRuntimeControlMode::ForcePause;
            control.requested_at_ms = now_ms;
            control.updated_at_ms = now_ms;
            control.completed_at_ms = None;
            control.note = Some(format!(
                "fill_size_guard:{}:order_id={}",
                reason, pending.order_id
            ));
            state
                .put_live_runtime_control(&pending.symbol, &pending.market_type, control.clone())
                .await;
        }
        state
            .append_live_event(
                &pending.symbol,
                &pending.market_type,
                json!({
                    "accepted": true,
                    "event_type": "guard",
                    "action": pending.action,
                    "side": pending.side,
                    "round_id": pending.round_id,
                    "intent_id": pending.intent_id,
                    "decision_id": pending.decision_id,
                    "decision_key": pending.decision_key,
                    "reason": "fill_size_guard_force_pause",
                    "order_id": pending.order_id,
                    "runtime_mode": control.mode,
                    "runtime_note": control.note,
                    "detail": {
                        "requested_size_shares": pending.order_size_shares,
                        "reported_fill_size_shares": fill_meta.reported_fill_size_shares,
                        "effective_fill_size_shares": fill_meta.fill_size_shares,
                        "requested_quote_usdc": pending.quote_size_usdc,
                        "reported_fill_quote_usdc": fill_meta.reported_fill_quote_usdc,
                        "effective_fill_quote_usdc": fill_meta.fill_quote_usdc,
                        "fill_price_cents": fill_price_cents
                    }
                }),
            )
            .await;
    }
    // Underfill detection: log for observability but don't pause
    // Underfill 可能表明流动性问题或交易所问题，需要监控
    if fill_meta.underfill_detected {
        state
            .append_live_event(
                &pending.symbol,
                &pending.market_type,
                json!({
                    "accepted": true,
                    "event_type": "warning",
                    "action": pending.action,
                    "side": pending.side,
                    "round_id": pending.round_id,
                    "intent_id": pending.intent_id,
                    "decision_id": pending.decision_id,
                    "decision_key": pending.decision_key,
                    "reason": "underfill_detected",
                    "order_id": pending.order_id,
                    "detail": {
                        "requested_size_shares": pending.order_size_shares,
                        "reported_fill_size_shares": fill_meta.reported_fill_size_shares,
                        "fill_shortfall_shares": pending.order_size_shares - fill_meta.reported_fill_size_shares.unwrap_or(0.0),
                        "requested_quote_usdc": pending.quote_size_usdc,
                        "reported_fill_quote_usdc": fill_meta.reported_fill_quote_usdc
                    }
                }),
            )
            .await;
    }
}

pub(super) async fn apply_pending_revert(
    state: &ApiState,
    pending: &LivePendingOrder,
    reason: &str,
) {
    let now_ms = Utc::now().timestamp_millis();
    let mut ps = state
        .get_live_position_state(&pending.symbol, &pending.market_type)
        .await;
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
        .put_live_position_state(&pending.symbol, &pending.market_type, ps)
        .await;
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

fn apply_decision_target_override(target: &mut LiveMarketTarget, decision: &Value) {
    let market_id = decision
        .get("market_id")
        .or_else(|| decision.get("target_market_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty());
    if let Some(market_id) = market_id {
        target.market_id = market_id.to_string();
    }
    let token_id_yes = decision
        .get("token_id_yes")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty());
    if let Some(token_id_yes) = token_id_yes {
        target.token_id_yes = token_id_yes.to_string();
    }
    let token_id_no = decision
        .get("token_id_no")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty());
    if let Some(token_id_no) = token_id_no {
        target.token_id_no = token_id_no.to_string();
    }
    let end_date = decision
        .get("target_end_date")
        .or_else(|| decision.get("end_date"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty());
    if let Some(end_date) = end_date {
        target.end_date = Some(end_date.to_string());
    }
}

fn build_effective_target_for_decision(
    target: &LiveMarketTarget,
    decision: &Value,
    position_state: &LivePositionState,
) -> LiveMarketTarget {
    let mut effective = target.clone();
    apply_decision_target_override(&mut effective, decision);
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

#[derive(Debug, Clone)]
struct LockedExitTargetGuard {
    reason: &'static str,
    detail: Value,
}

async fn validate_locked_exit_target_tradable(
    target: &LiveMarketTarget,
    position_state: &LivePositionState,
) -> Result<(), LockedExitTargetGuard> {
    if position_state.position_size_shares <= 0.0 {
        return Ok(());
    }
    let market_id = target.market_id.trim();
    let token_id = position_state
        .entry_token_id
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_string();
    let now_ms = Utc::now().timestamp_millis();
    let grace_ms = live_locked_exit_market_grace_ms();

    if market_id.is_empty() {
        return Err(LockedExitTargetGuard {
            reason: "locked_exit_market_missing",
            detail: json!({
                "reason": "locked_exit_market_missing",
                "now_ms": now_ms,
                "position_size_shares": position_state.position_size_shares
            }),
        });
    }
    if token_id.is_empty() {
        return Err(LockedExitTargetGuard {
            reason: "locked_exit_token_missing",
            detail: json!({
                "reason": "locked_exit_token_missing",
                "market_id": market_id,
                "now_ms": now_ms,
                "position_size_shares": position_state.position_size_shares
            }),
        });
    }

    if let Some(end_ms) = parse_end_date_ms(target.end_date.as_deref()) {
        if now_ms > end_ms.saturating_add(grace_ms) {
            return Err(LockedExitTargetGuard {
                reason: "locked_exit_market_expired",
                detail: json!({
                    "reason": "locked_exit_market_expired",
                    "market_id": market_id,
                    "target_end_date": target.end_date,
                    "now_ms": now_ms,
                    "grace_ms": grace_ms,
                    "token_id": token_id
                }),
            });
        }
    }

    let Some(detail) = fetch_gamma_market_detail(market_id).await else {
        return Ok(());
    };
    if detail.closed {
        return Err(LockedExitTargetGuard {
            reason: "locked_exit_market_closed",
            detail: json!({
                "reason": "locked_exit_market_closed",
                "market_id": market_id,
                "gamma_market_id": detail.market_id,
                "gamma_active": detail.active,
                "gamma_closed": detail.closed,
                "enable_order_book": detail.enable_order_book,
                "target_end_date": target.end_date,
                "gamma_end_date": detail.end_date,
                "token_id": token_id
            }),
        });
    }
    if !detail.enable_order_book {
        return Err(LockedExitTargetGuard {
            reason: "locked_exit_orderbook_disabled",
            detail: json!({
                "reason": "locked_exit_orderbook_disabled",
                "market_id": market_id,
                "gamma_market_id": detail.market_id,
                "gamma_active": detail.active,
                "gamma_closed": detail.closed,
                "enable_order_book": detail.enable_order_book,
                "gamma_end_date": detail.end_date,
                "token_id": token_id
            }),
        });
    }
    if !detail.token_ids.is_empty() && !detail.token_ids.iter().any(|id| id == &token_id) {
        return Err(LockedExitTargetGuard {
            reason: "locked_exit_token_mismatch",
            detail: json!({
                "reason": "locked_exit_token_mismatch",
                "market_id": market_id,
                "gamma_market_id": detail.market_id,
                "token_id": token_id,
                "gamma_token_ids": detail.token_ids,
                "gamma_end_date": detail.end_date
            }),
        });
    }
    if let Some(end_ms) = parse_end_date_ms(detail.end_date.as_deref()) {
        if now_ms > end_ms.saturating_add(grace_ms) {
            return Err(LockedExitTargetGuard {
                reason: "locked_exit_market_expired",
                detail: json!({
                    "reason": "locked_exit_market_expired",
                    "market_id": market_id,
                    "gamma_market_id": detail.market_id,
                    "target_end_date": target.end_date,
                    "gamma_end_date": detail.end_date,
                    "now_ms": now_ms,
                    "grace_ms": grace_ms,
                    "token_id": token_id
                }),
            });
        }
    }
    Ok(())
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

fn validate_entry_target_readiness(
    decision: &Value,
    target: &LiveMarketTarget,
    now_ms: i64,
) -> Result<(), &'static str> {
    let action = decision
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let is_entry_like = action == "enter" || action == "add";
    if !is_entry_like {
        return Ok(());
    }
    if target.market_id.trim().is_empty() {
        return Err("entry_target_market_missing");
    }
    if token_id_for_decision(decision, target).is_none() {
        return Err("entry_target_token_missing");
    }
    let Some(end_ms) = parse_end_date_ms(target.end_date.as_deref()) else {
        return Err("entry_target_end_date_missing");
    };
    if now_ms > end_ms.saturating_add(live_round_target_match_tolerance_ms()) {
        return Err("entry_target_expired");
    }
    Ok(())
}

struct PreparedLiveDecision {
    decision: Value,
    effective_target: LiveMarketTarget,
    bypass_round_guard: bool,
}

enum PrepareLiveDecisionError {
    RoundTargetMismatch {
        decision: Value,
        effective_target: LiveMarketTarget,
    },
    EntryTargetNotReady {
        decision: Value,
        effective_target: LiveMarketTarget,
        reason: &'static str,
        now_ms: i64,
    },
}

fn prepare_live_decision_for_submission(
    gated: &LiveGatedDecision,
    target: &LiveMarketTarget,
    position_state: &LivePositionState,
    exec_cfg: &LiveExecutionConfig,
    exit_quote_override: Option<f64>,
    exit_size_override: Option<f64>,
) -> Result<PreparedLiveDecision, PrepareLiveDecisionError> {
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
        return Err(PrepareLiveDecisionError::RoundTargetMismatch {
            decision,
            effective_target,
        });
    }
    let now_ms = Utc::now().timestamp_millis();
    if let Err(reason) = validate_entry_target_readiness(&decision, &effective_target, now_ms) {
        return Err(PrepareLiveDecisionError::EntryTargetNotReady {
            decision,
            effective_target,
            reason,
            now_ms,
        });
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
    apply_emergency_exit_overrides(&mut decision, exec_cfg);
    Ok(PreparedLiveDecision {
        decision,
        effective_target,
        bypass_round_guard,
    })
}

fn prepare_live_decision_error_json(
    gated: &LiveGatedDecision,
    error: PrepareLiveDecisionError,
    executor: Option<&str>,
) -> Value {
    let mut row = match error {
        PrepareLiveDecisionError::RoundTargetMismatch {
            decision,
            effective_target,
        } => json!({
            "ok": false,
            "accepted": false,
            "reason": "round_target_mismatch",
            "decision_key": gated.decision_key,
            "decision_round_id": decision.get("round_id").and_then(Value::as_str),
            "target_market_id": effective_target.market_id,
            "target_end_date": effective_target.end_date,
            "decision": decision
        }),
        PrepareLiveDecisionError::EntryTargetNotReady {
            decision,
            effective_target,
            reason,
            now_ms,
        } => json!({
            "ok": false,
            "accepted": false,
            "reason": reason,
            "decision_key": gated.decision_key,
            "decision_round_id": decision.get("round_id").and_then(Value::as_str),
            "target_market_id": effective_target.market_id,
            "target_end_date": effective_target.end_date,
            "now_ms": now_ms,
            "decision": decision
        }),
    };
    if let Some(exec) = executor {
        if let Some(obj) = row.as_object_mut() {
            obj.insert("executor".to_string(), json!(exec));
        }
    }
    row
}

pub(super) fn build_position_locked_target(
    market_type: &str,
    position_state: &LivePositionState,
) -> Option<LiveMarketTarget> {
    let market_id = position_state
        .entry_market_id
        .as_deref()?
        .trim()
        .to_string();
    let token_id = position_state.entry_token_id.as_deref()?.trim().to_string();
    if market_id.is_empty() || token_id.is_empty() {
        return None;
    }
    let side = position_state.side.as_deref()?.trim().to_ascii_uppercase();
    let (token_id_yes, token_id_no) = match side.as_str() {
        "UP" => (token_id.clone(), String::new()),
        "DOWN" => (String::new(), token_id.clone()),
        _ => return None,
    };
    let symbol = position_state.symbol.trim().to_ascii_uppercase();
    let symbol = if !symbol.is_empty() {
        symbol
    } else {
        position_state
            .entry_round_id
            .as_deref()
            .and_then(|rid| rid.split('_').next())
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.to_ascii_uppercase())
            .unwrap_or_else(default_runtime_symbol)
    };
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
