pub(super) fn pm_dec_to_f64(v: &PmDecimal) -> f64 {
    v.to_string().parse::<f64>().unwrap_or(0.0)
}

pub(super) fn pm_order_type_from_tif(tif: &str) -> PmOrderType {
    match tif.to_ascii_uppercase().as_str() {
        "FOK" => PmOrderType::FOK,
        "GTD" => PmOrderType::GTD,
        "FAK" => PmOrderType::FAK,
        _ => PmOrderType::GTC,
    }
}

pub(super) fn pm_is_terminal_fill(status: &PmOrderStatusType) -> bool {
    matches!(status, PmOrderStatusType::Matched)
}

pub(super) fn pm_is_terminal_reject(status: &PmOrderStatusType) -> bool {
    matches!(
        status,
        PmOrderStatusType::Canceled | PmOrderStatusType::Unmatched
    )
}

pub(super) fn fill_event_from_open_order(
    order: &polymarket_client_sdk::clob::types::response::OpenOrderResponse,
) -> Value {
    let fill_price = pm_dec_to_f64(&order.price).max(0.0001);
    let fill_size = pm_dec_to_f64(&order.size_matched).max(0.0);
    let fill_quote = (fill_price * fill_size).max(0.0);
    json!({
        "event": "order_terminal",
        "state": "filled",
        "order_id": order.id,
        "fill_price_cents": fill_price * 100.0,
        "fill_quote_usdc": fill_quote,
        "fill_size_shares": fill_size,
        "size_matched": fill_size,
        "matched_size": fill_size,
        "associate_trades": order.associate_trades,
    })
}

fn is_exit_like_action(action: &str) -> bool {
    let a = action.trim().to_ascii_lowercase();
    a == "exit" || a == "reduce"
}

fn cancel_failure_pause_threshold(action: &str) -> u8 {
    if is_exit_like_action(action) {
        LIVE_CANCEL_FAILURE_FORCE_PAUSE_EXIT_THRESHOLD
    } else {
        LIVE_CANCEL_FAILURE_FORCE_PAUSE_OTHER_THRESHOLD
    }
}

pub(super) async fn handle_cancel_failure_with_escalation(
    state: &ApiState,
    pending: &LivePendingOrder,
    reason: &str,
    detail: Value,
) {
    let mut retry = pending.clone();
    retry.retry_count = retry.retry_count.saturating_add(1);
    state.upsert_pending_order(retry.clone()).await;
    state
        .append_live_event(
            &retry.market_type,
            json!({
                "accepted": false,
                "action": retry.action,
                "side": retry.side,
                "round_id": retry.round_id,
                "reason": format!("{reason}_requeued"),
                "order_id": retry.order_id,
                "retry_count": retry.retry_count,
                "detail": detail
            }),
        )
        .await;

    if retry.retry_count < cancel_failure_pause_threshold(&retry.action) {
        return;
    }

    let now_ms = Utc::now().timestamp_millis();
    let mut control = state.get_live_runtime_control(&retry.market_type).await;
    if control.mode != LiveRuntimeControlMode::ForcePause {
        control.mode = LiveRuntimeControlMode::ForcePause;
        control.requested_at_ms = now_ms;
        control.updated_at_ms = now_ms;
        control.completed_at_ms = None;
        control.note = Some(format!(
            "auto_force_pause:{}:order_id={}:retry={}",
            reason, retry.order_id, retry.retry_count
        ));
        state
            .put_live_runtime_control(&retry.market_type, control.clone())
            .await;
    }
    state
        .append_live_event(
            &retry.market_type,
            json!({
                "accepted": false,
                "action": retry.action,
                "side": retry.side,
                "round_id": retry.round_id,
                "reason": "auto_force_pause_cancel_failure",
                "order_id": retry.order_id,
                "retry_count": retry.retry_count,
                "runtime_mode": control.mode,
                "runtime_note": control.note,
            }),
        )
        .await;
}

pub(super) async fn get_or_init_rust_executor(
    state: &ApiState,
) -> Result<Arc<RustExecutorContext>, String> {
    if let Some(ctx) = state.live_rust_executor.read().await.clone() {
        return Ok(ctx);
    }
    let cfg = RustExecutorConfig::from_env()?;
    let signer = PmLocalSigner::from_str(cfg.private_key.trim())
        .map_err(|e| format!("invalid private key: {e}"))?
        .with_chain_id(Some(cfg.chain_id));
    let signer_for_runtime = signer.clone();
    let mut auth = PmClient::new(&cfg.host, PmConfig::default())
        .map_err(|e| format!("rust sdk client init failed: {e}"))?
        .authentication_builder(&signer)
        .signature_type(cfg.signature_type);
    if let Some(nonce) = cfg.nonce {
        auth = auth.nonce(nonce);
    }
    if let Some(creds) = cfg.credentials.clone() {
        auth = auth.credentials(creds);
    }
    if let Some(funder) = cfg.funder {
        auth = auth.funder(funder);
    }
    let client = auth
        .authenticate()
        .await
        .map_err(|e| format!("rust sdk auth failed: {e}"))?;
    let ctx = Arc::new(RustExecutorContext {
        client,
        signer: Box::new(signer_for_runtime),
    });
    let mut slot = state.live_rust_executor.write().await;
    if slot.is_none() {
        *slot = Some(ctx.clone());
    }
    Ok(slot.as_ref().cloned().unwrap_or(ctx))
}

pub(super) async fn fetch_rust_book_snapshot(
    ctx: &RustExecutorContext,
    token_id: &str,
) -> Option<GatewayBookSnapshot> {
    let token = PmU256::from_str(token_id).ok()?;
    let req = PmOrderBookSummaryRequest::builder().token_id(token).build();
    let book = ctx.client.order_book(&req).await.ok()?;
    let best_bid = book.bids.first().map(|l| pm_dec_to_f64(&l.price));
    let best_ask = book.asks.first().map(|l| pm_dec_to_f64(&l.price));
    let best_bid_size = book.bids.first().map(|l| pm_dec_to_f64(&l.size));
    let best_ask_size = book.asks.first().map(|l| pm_dec_to_f64(&l.size));
    let bid_depth_top3 = Some(
        book.bids
            .iter()
            .take(3)
            .map(|l| pm_dec_to_f64(&l.size))
            .sum::<f64>(),
    );
    let ask_depth_top3 = Some(
        book.asks
            .iter()
            .take(3)
            .map(|l| pm_dec_to_f64(&l.size))
            .sum::<f64>(),
    );
    Some(GatewayBookSnapshot {
        token_id: token_id.to_string(),
        min_order_size: pm_dec_to_f64(&book.min_order_size).max(0.0001),
        tick_size: pm_dec_to_f64(&book.tick_size.as_decimal()).max(0.0001),
        best_bid,
        best_ask,
        best_bid_size,
        best_ask_size,
        bid_depth_top3,
        ask_depth_top3,
    })
}

pub(super) async fn submit_rust_order(
    ctx: &RustExecutorContext,
    payload: &Value,
) -> Result<Value, String> {
    let token_id = payload
        .get("token_id")
        .and_then(Value::as_str)
        .ok_or_else(|| "missing token_id".to_string())?;
    let token = PmU256::from_str(token_id).map_err(|e| format!("invalid token_id: {e}"))?;
    let side = payload
        .get("side")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let pm_side = match side.as_str() {
        "buy_yes" | "buy_no" => PmSide::Buy,
        "sell_yes" | "sell_no" => PmSide::Sell,
        _ => return Err(format!("unsupported side: {side}")),
    };
    let price = payload
        .get("price")
        .and_then(Value::as_f64)
        .unwrap_or(0.5)
        .clamp(0.01, 0.99);
    let size = payload
        .get("size")
        .and_then(Value::as_f64)
        .unwrap_or(1.0)
        .max(0.01);
    let tif = payload
        .get("tif")
        .and_then(Value::as_str)
        .unwrap_or("FAK")
        .to_ascii_uppercase();
    let style = payload
        .get("style")
        .and_then(Value::as_str)
        .unwrap_or("taker")
        .to_ascii_lowercase();
    let ttl_ms = payload
        .get("ttl_ms")
        .and_then(Value::as_i64)
        .unwrap_or(1_200)
        .clamp(500, 30_000);
    let tick_size = payload
        .get("book_meta")
        .and_then(|v| v.get("tick_size"))
        .and_then(Value::as_f64)
        .unwrap_or(0.01);
    let price_decimals = decimal_places_from_step(tick_size, 2, 6);
    let price_str = format_decimal_compact(price, price_decimals.max(2));
    let size_str = format_decimal_compact(round_lot_size(size), 2);
    let price_dec = PmDecimal::from_str(&price_str).map_err(|e| format!("bad price: {e}"))?;
    let size_dec = PmDecimal::from_str(&size_str).map_err(|e| format!("bad size: {e}"))?;
    let order_type = pm_order_type_from_tif(&tif);
    let amount_mode_buy_usdc = payload
        .get("amount_mode")
        .and_then(Value::as_str)
        .map(|v| v.eq_ignore_ascii_case("buy_usdc"))
        .unwrap_or(false)
        || payload
            .get("buy_amount_usdc")
            .and_then(Value::as_f64)
            .filter(|v| v.is_finite() && *v > 0.0)
            .is_some();
    let requested_notional = payload
        .get("buy_amount_usdc")
        .and_then(Value::as_f64)
        .or_else(|| {
            if amount_mode_buy_usdc {
                None
            } else {
                payload
                    .get("requested_notional_usdc")
                    .and_then(Value::as_f64)
            }
        })
        .filter(|v| v.is_finite() && *v > 0.0)
        .unwrap_or(size * price);
    let use_market_buy_amount = amount_mode_buy_usdc
        && matches!(pm_side, PmSide::Buy)
        && matches!(order_type, PmOrderType::FAK | PmOrderType::FOK)
        && requested_notional > 0.0;

    let resp = if use_market_buy_amount {
        let amount_dec = PmDecimal::from_str(&format_decimal_compact(requested_notional, 6))
            .map_err(|e| format!("bad buy_amount_usdc: {e}"))?;
        let amount =
            PmAmount::usdc(amount_dec).map_err(|e| format!("invalid buy amount (usdc): {e}"))?;
        let signable = ctx
            .client
            .market_order()
            .token_id(token)
            .side(pm_side)
            .order_type(order_type.clone())
            .price(price_dec)
            .amount(amount)
            .build()
            .await
            .map_err(|e| format!("build market order failed: {e}"))?;
        let signed = ctx
            .client
            .sign(&ctx.signer, signable)
            .await
            .map_err(|e| format!("sign failed: {e}"))?;
        ctx.client
            .post_order(signed)
            .await
            .map_err(|e| format!("post_order failed: {e}"))?
    } else {
        let mut builder = ctx
            .client
            .limit_order()
            .token_id(token)
            .side(pm_side)
            .price(price_dec)
            .size(size_dec)
            .order_type(order_type.clone());
        let post_only =
            style == "maker" && matches!(order_type, PmOrderType::GTC | PmOrderType::GTD);
        builder = builder.post_only(post_only);
        if matches!(order_type, PmOrderType::GTD) {
            // Polymarket GTD requires expiration to be sufficiently in the future
            // (server-side security threshold, typically >= 60s).
            let ttl_sec = ((ttl_ms + 999) / 1000).max(1);
            let expiration = Utc::now()
                + chrono::Duration::seconds(
                    live_gtd_min_future_guard_sec()
                        + live_gtd_expiration_safety_sec()
                        + ttl_sec,
                );
            builder = builder.expiration(expiration);
        }
        let signable = builder
            .build()
            .await
            .map_err(|e| format!("build order failed: {e}"))?;
        let signed = ctx
            .client
            .sign(&ctx.signer, signable)
            .await
            .map_err(|e| format!("sign failed: {e}"))?;
        ctx.client
            .post_order(signed)
            .await
            .map_err(|e| format!("post_order failed: {e}"))?
    };
    let accepted = resp.success && resp.error_msg.as_deref().unwrap_or_default().is_empty();
    let making_amount = pm_dec_to_f64(&resp.making_amount);
    let taking_amount = pm_dec_to_f64(&resp.taking_amount);
    let accepted_size = if use_market_buy_amount {
        if taking_amount > 0.0 {
            taking_amount
        } else {
            size
        }
    } else if making_amount > 0.0 {
        making_amount
    } else {
        size
    };
    let effective_notional = if use_market_buy_amount {
        if making_amount > 0.0 {
            making_amount
        } else {
            requested_notional
        }
    } else if taking_amount > 0.0 {
        taking_amount
    } else {
        size * price
    };
    Ok(json!({
        "accepted": accepted,
        "order_id": resp.order_id,
        "status": format!("{}", resp.status),
        "error_msg": resp.error_msg,
        "accepted_size": accepted_size,
        "effective_notional": effective_notional,
        "making_amount": making_amount,
        "taking_amount": taking_amount,
        "requested_notional_usdc": requested_notional,
        "market_order_amount_mode": use_market_buy_amount,
        "trade_ids": resp.trade_ids,
        "transaction_hashes": resp.transaction_hashes
    }))
}

async fn try_resubmit_after_terminal_reject_rust(
    state: &ApiState,
    gateway_cfg: &LiveGatewayConfig,
    ctx: &Arc<RustExecutorContext>,
    row: &LivePendingOrder,
    terminal_status: &str,
) -> bool {
    let action = row.action.to_ascii_lowercase();
    let exit_like = is_live_exit_action(&action);
    let emergency_exit = exit_like && is_emergency_exit_reason(&row.submit_reason);
    let max_retry = if emergency_exit { 2 } else { 1 };
    if row.retry_count >= max_retry {
        return false;
    }
    let target = match resolve_live_market_target_with_state(state, &row.market_type).await {
        Ok(v) => v,
        Err(err) => {
            state
                .append_live_event(
                    &row.market_type,
                    json!({
                        "accepted": false,
                        "action": row.action,
                        "side": row.side,
                        "round_id": row.round_id,
                        "reason": "rust_terminal_rejected_resubmit_no_target",
                        "order_id": row.order_id,
                        "status": terminal_status,
                        "error": err.message
                    }),
                )
                .await;
            return false;
        }
    };
    let mut effective_target = target.clone();
    apply_pending_target_override(&mut effective_target, row);
    let round_check = json!({ "round_id": row.round_id });
    let bypass_round_guard = is_live_exit_action(&action)
        && !row.market_id.trim().is_empty()
        && !row.token_id.trim().is_empty();
    if !decision_round_matches_target(&round_check, &effective_target) && !bypass_round_guard {
        state
            .append_live_event(
                &row.market_type,
                json!({
                    "accepted": false,
                    "action": row.action,
                        "side": row.side,
                        "round_id": row.round_id,
                        "reason": "rust_terminal_rejected_resubmit_round_target_mismatch",
                        "order_id": row.order_id,
                        "status": terminal_status,
                        "target_market_id": effective_target.market_id,
                        "target_end_date": effective_target.end_date
                    }),
                )
                .await;
        return false;
    }
    let mut decision = json!({
        "action": row.action,
        "side": row.side,
        "round_id": row.round_id,
        "price_cents": row.price_cents,
        "quote_size_usdc": row.quote_size_usdc,
        "reason": format!("{}_terminal_retry_fak", row.submit_reason),
        "tif": "FAK",
        "style": "taker",
        "ttl_ms": if emergency_exit { 650 } else { 900 },
        "max_slippage_bps": if exit_like {
            (gateway_cfg.exit_slippage_bps + if emergency_exit { 22.0 } else { 14.0 }).max(34.0)
        } else {
            (gateway_cfg.entry_slippage_bps + 12.0).max(28.0)
        }
    });
    if exit_like {
        let ps = state.get_live_position_state(&row.market_type).await;
        if ps.position_size_shares > 0.0 {
            if let Some(obj) = decision.as_object_mut() {
                obj.insert(
                    "position_size_shares".to_string(),
                    json!(ps.position_size_shares),
                );
            }
        }
    }
    let token_id = token_id_for_decision(&decision, &effective_target).map(str::to_string);
    let book_snapshot = if let Some(token_id) = token_id {
        if let Some(cached) = state.get_rust_book_cache(&token_id).await {
            Some(cached)
        } else if let Some(v) = fetch_rust_book_snapshot(ctx, &token_id).await {
            state.put_rust_book_cache(&token_id, v.clone()).await;
            Some(v)
        } else {
            None
        }
    } else {
        None
    };
    let Some(payload) = decision_to_live_payload(
        &decision,
        &effective_target,
        gateway_cfg,
        book_snapshot.as_ref(),
        Some(row.quote_size_usdc),
    ) else {
        state
            .append_live_event(
                &row.market_type,
                json!({
                    "accepted": false,
                    "action": row.action,
                    "side": row.side,
                    "round_id": row.round_id,
                    "reason": "rust_terminal_rejected_resubmit_payload_failed",
                    "order_id": row.order_id,
                    "status": terminal_status
                }),
            )
            .await;
        return false;
    };
    let now_ms = Utc::now().timestamp_millis();
    match submit_rust_order(ctx, &payload).await {
        Ok(submit_resp) => {
            let accepted = submit_resp
                .get("accepted")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let new_order_id = submit_resp
                .get("order_id")
                .and_then(Value::as_str)
                .filter(|v| !v.trim().is_empty())
                .map(str::to_string);
            if accepted {
                if let Some(new_order_id) = new_order_id {
                    let mut pending = row.clone();
                    pending.order_id = new_order_id.clone();
                    pending.retry_count = pending.retry_count.saturating_add(1);
                    pending.tif = "FAK".to_string();
                    pending.style = "taker".to_string();
                    pending.submitted_ts_ms = now_ms;
                    pending.cancel_after_ms = if emergency_exit { 700 } else { 1500 };
                    state.upsert_pending_order(pending).await;
                    state
                        .append_live_event(
                            &row.market_type,
                            json!({
                                "accepted": true,
                                "action": row.action,
                                "side": row.side,
                                "round_id": row.round_id,
                                "reason": "rust_terminal_rejected_resubmitted_fak",
                                "order_id": row.order_id,
                                "new_order_id": new_order_id,
                                "status": terminal_status,
                                "submit_response": submit_resp,
                                "book_snapshot": book_snapshot,
                                "emergency_exit": emergency_exit
                            }),
                        )
                        .await;
                    return true;
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
                        "reason": "rust_terminal_rejected_resubmit_rejected",
                        "order_id": row.order_id,
                        "status": terminal_status,
                        "submit_response": submit_resp
                    }),
                )
                .await;
            false
        }
        Err(err) => {
            state
                .append_live_event(
                    &row.market_type,
                    json!({
                        "accepted": false,
                        "action": row.action,
                        "side": row.side,
                        "round_id": row.round_id,
                        "reason": "rust_terminal_rejected_resubmit_error",
                        "order_id": row.order_id,
                        "status": terminal_status,
                        "error": err
                    }),
                )
                .await;
            false
        }
    }
}

pub(super) async fn execute_live_orders_via_rust_sdk(
    state: &ApiState,
    gateway_cfg: &LiveGatewayConfig,
    target: &LiveMarketTarget,
    position_state: &LivePositionState,
    decisions: &[LiveGatedDecision],
) -> Vec<Value> {
    let ctx = match get_or_init_rust_executor(state).await {
        Ok(v) => v,
        Err(err) => {
            return decisions
                .iter()
                .map(|g| {
                    json!({
                        "ok": false,
                        "accepted": false,
                        "decision_key": g.decision_key,
                        "decision": g.decision,
                        "error": err,
                        "executor": "rust_sdk"
                    })
                })
                .collect();
        }
    };

    let mut out = Vec::<Value>::with_capacity(decisions.len());
    let fallback_client = state.gateway_http_client.as_ref();
    let token_ids = collect_decision_token_ids(target, decisions);
    let mut book_cache =
        prefetch_rust_books_for_tokens(state, &ctx, fallback_client, gateway_cfg, &token_ids).await;
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
        let effective_target = build_effective_target_for_decision(target, &decision, position_state);
        let bypass_round_guard = should_bypass_round_match_for_locked_exit(&decision, position_state);
        if !decision_round_matches_target(&decision, &effective_target) && !bypass_round_guard {
            out.push(json!({
                "ok": false,
                "accepted": false,
                "reason": "round_target_mismatch",
                "decision_key": gated.decision_key,
                "decision_round_id": decision.get("round_id").and_then(Value::as_str),
                "target_market_id": effective_target.market_id,
                "target_end_date": effective_target.end_date,
                "decision": decision
            }));
            continue;
        }
        if decision
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .eq_ignore_ascii_case("exit")
        {
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
                    let fetched = if let Some(cached) = state.get_rust_book_cache(&token_id).await {
                        Some(cached)
                    } else if let Some(v) = fetch_rust_book_snapshot(&ctx, &token_id).await {
                        state.put_rust_book_cache(&token_id, v.clone()).await;
                        Some(v)
                    } else {
                        let fetched =
                            fetch_gateway_book_snapshot(fallback_client, gateway_cfg, &token_id)
                                .await;
                        if let Some(v) = fetched.as_ref() {
                            state.put_rust_book_cache(&token_id, v.clone()).await;
                        }
                        fetched
                    };
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
                "accepted": false,
                "decision_key": gated.decision_key,
                "decision": decision,
                "reason": "decision_to_payload_failed",
                "executor": "rust_sdk"
            }));
            continue;
        };
        let mut attempt_payload = payload.clone();
        let mut attempts = Vec::<Value>::with_capacity(3);
        let mut accepted = false;
        let mut final_response: Option<Value> = None;
        let mut final_error: Option<String> = None;
        let mut final_reject_reason: Option<String> = None;
        let mut executed_via = "rust_sdk";
        let order_started = Instant::now();
        for attempt in 0..3usize {
            let attempt_started = Instant::now();
            let submit = submit_rust_order(&ctx, &attempt_payload).await;
            match submit {
                Ok(resp) => {
                    let accept = resp
                        .get("accepted")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    let reject_reason = if accept {
                        String::new()
                    } else {
                        extract_rust_reject_reason(&resp, None)
                    };
                    attempts.push(json!({
                        "attempt": attempt + 1,
                        "executor": "rust_sdk",
                        "latency_ms": attempt_started.elapsed().as_millis() as u64,
                        "request": attempt_payload.clone(),
                        "accepted": accept,
                        "reject_reason": if reject_reason.is_empty() { Value::Null } else { json!(reject_reason.clone()) },
                        "response": resp
                    }));
                    final_response = Some(resp.clone());
                    if accept {
                        accepted = true;
                        break;
                    }
                    final_reject_reason = Some(reject_reason.clone());
                    if let Some(next_payload) =
                        build_retry_payload(&attempt_payload, &reject_reason, attempt)
                    {
                        attempt_payload = next_payload;
                        continue;
                    }
                    break;
                }
                Err(err) => {
                    let mut fallback_response: Option<Value> = None;
                    let mut fallback_accept = false;
                    let mut fallback_used = false;
                    if gateway_cfg.rust_submit_fallback_gateway {
                        let fallback_started = Instant::now();
                        let (fallback_submit, fallback_endpoint) =
                            submit_gateway_order(fallback_client, gateway_cfg, &attempt_payload)
                                .await;
                        if let Ok(resp) = fallback_submit {
                            fallback_used = true;
                            fallback_accept = resp
                                .get("accepted")
                                .and_then(Value::as_bool)
                                .unwrap_or(false);
                            executed_via = "gateway_fallback";
                            fallback_response = Some(json!({
                                "endpoint": fallback_endpoint,
                                "latency_ms": fallback_started.elapsed().as_millis() as u64,
                                "response": resp
                            }));
                            if fallback_accept {
                                accepted = true;
                                final_response = fallback_response
                                    .as_ref()
                                    .and_then(|v| v.get("response"))
                                    .cloned();
                                attempts.push(json!({
                                    "attempt": attempt + 1,
                                    "executor": "rust_sdk",
                                    "latency_ms": attempt_started.elapsed().as_millis() as u64,
                                    "request": attempt_payload.clone(),
                                    "accepted": false,
                                    "error": err,
                                    "fallback": fallback_response
                                }));
                                break;
                            }
                        }
                    }
                    attempts.push(json!({
                        "attempt": attempt + 1,
                        "executor": "rust_sdk",
                        "latency_ms": attempt_started.elapsed().as_millis() as u64,
                        "request": attempt_payload.clone(),
                        "accepted": false,
                        "error": err,
                        "fallback_used": fallback_used,
                        "fallback_accepted": fallback_accept,
                        "fallback": fallback_response
                    }));
                    final_error = Some(err.clone());
                    final_reject_reason = Some(err.clone());
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
            "request": payload,
            "final_request": attempt_payload,
            "response": final_response,
            "error": final_error,
            "reject_reason": final_reject_reason,
            "attempts": attempts,
            "executor": executed_via,
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

pub(super) async fn reconcile_rust_reports(state: &ApiState, gateway_cfg: &LiveGatewayConfig) {
    let ctx = match get_or_init_rust_executor(state).await {
        Ok(v) => v,
        Err(err) => {
            tracing::warn!(error = %err, "rust sdk reconcile unavailable, fallback to gateway reports");
            reconcile_gateway_reports(state, gateway_cfg).await;
            return;
        }
    };
    let pending = state.list_pending_orders().await;
    if pending.is_empty() {
        return;
    }
    for row in pending {
        let status = ctx.client.order(&row.order_id).await;
        let Ok(order) = status else {
            continue;
        };
        if pm_is_terminal_fill(&order.status) {
            let _ = state.remove_pending_order(&row.order_id).await;
            let fill_event = fill_event_from_open_order(&order);
            apply_pending_confirmation(
                state,
                &row,
                "rust_order_terminal_filled",
                Some(&fill_event),
            )
            .await;
            state
                .append_live_event(
                    &row.market_type,
                    json!({
                        "accepted": true,
                        "action": row.action,
                        "side": row.side,
                        "round_id": row.round_id,
                        "reason": "rust_order_terminal_filled",
                        "order_id": row.order_id
                    }),
                )
                .await;
        } else if pm_is_terminal_reject(&order.status) {
            let _ = state.remove_pending_order(&row.order_id).await;
            let terminal_status = format!("{}", order.status);
            let retried = try_resubmit_after_terminal_reject_rust(
                state,
                gateway_cfg,
                &ctx,
                &row,
                &terminal_status,
            )
            .await;
            if retried {
                continue;
            }
            apply_pending_revert(state, &row, "rust_order_terminal_rejected").await;
            state
                .append_live_event(
                    &row.market_type,
                    json!({
                        "accepted": false,
                        "action": row.action,
                        "side": row.side,
                        "round_id": row.round_id,
                        "reason": "rust_order_terminal_rejected",
                        "order_id": row.order_id,
                        "status": terminal_status
                    }),
                )
                .await;
        }
    }
}

pub(super) async fn handle_pending_timeouts_rust(
    state: &ApiState,
    gateway_cfg: &LiveGatewayConfig,
) {
    let ctx = match get_or_init_rust_executor(state).await {
        Ok(v) => v,
        Err(err) => {
            tracing::warn!(error = %err, "rust sdk cancel unavailable, fallback to gateway cancels");
            handle_pending_timeouts(state, gateway_cfg).await;
            return;
        }
    };
    let now_ms = Utc::now().timestamp_millis();
    let pending = state.list_pending_orders().await;
    for row in pending {
        if now_ms.saturating_sub(row.submitted_ts_ms) < pending_cancel_due_ms(&row) {
            continue;
        }
        match ctx.client.cancel_order(&row.order_id).await {
            Ok(resp) => {
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
                                        "reason": "rust_timeout_cancelled_round_target_mismatch",
                                        "order_id": row.order_id,
                                        "target_market_id": effective_target.market_id,
                                        "target_end_date": effective_target.end_date
                                    }),
                                )
                                .await;
                            apply_pending_revert(state, &row, "rust_local_timeout_cancel").await;
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
                            if let Some(cached) = state.get_rust_book_cache(&token_id).await {
                                Some(cached)
                            } else if let Some(v) = fetch_rust_book_snapshot(&ctx, &token_id).await
                            {
                                state.put_rust_book_cache(&token_id, v.clone()).await;
                                Some(v)
                            } else {
                                None
                            }
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
                            let submit = submit_rust_order(&ctx, &payload).await;
                            if let Ok(submit_resp) = submit {
                                let accepted = submit_resp
                                    .get("accepted")
                                    .and_then(Value::as_bool)
                                    .unwrap_or(false);
                                if accepted {
                                    if let Some(order_id) = submit_resp
                                        .get("order_id")
                                        .and_then(Value::as_str)
                                        .filter(|v| !v.trim().is_empty())
                                    {
                                        let mut pending = row.clone();
                                        pending.order_id = order_id.to_string();
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
                                                    "reason": "rust_timeout_cancelled_resubmitted_fak",
                                                    "order_id": row.order_id,
                                                    "cancelled": resp.canceled,
                                                    "not_cancelled": resp.not_canceled,
                                                    "submit_response": submit_resp,
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
                                            "reason": "rust_timeout_cancelled_fak_resubmit_rejected",
                                            "order_id": row.order_id,
                                            "cancelled": resp.canceled,
                                            "not_cancelled": resp.not_canceled,
                                            "submit_response": submit_resp
                                        }),
                                    )
                                    .await;
                            }
                        }
                    }
                }
                apply_pending_revert(state, &row, "rust_timeout_cancelled").await;
                state
                    .append_live_event(
                        &row.market_type,
                        json!({
                            "accepted": false,
                            "action": row.action,
                            "side": row.side,
                            "round_id": row.round_id,
                            "reason": "rust_timeout_cancelled",
                            "order_id": row.order_id,
                            "cancelled": resp.canceled,
                            "not_cancelled": resp.not_canceled
                        }),
                    )
                    .await;
            }
            Err(err) => {
                handle_cancel_failure_with_escalation(
                    state,
                    &row,
                    "rust_timeout_cancel_failed",
                    json!({
                        "error": err.to_string()
                    }),
                )
                .await;
            }
        }
    }
}

