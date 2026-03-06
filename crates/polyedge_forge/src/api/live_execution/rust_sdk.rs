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

fn cancel_response_contains_order(resp: &polymarket_client_sdk::clob::types::response::CancelOrdersResponse, order_id: &str) -> bool {
    resp.canceled.iter().any(|id| id == order_id)
}

fn initial_pending_cancel_after_ms(action: &str, tif: &str) -> i64 {
    let maker_tif = matches!(tif.to_ascii_uppercase().as_str(), "GTD" | "GTC" | "POST_ONLY");
    if is_entry_action(action) && maker_tif {
        live_entry_maker_max_wait_ms()
    } else if is_live_exit_action(action) {
        900
    } else {
        1_200
    }
}

fn initial_pending_terminal_after_ms(action: &str, tif: &str) -> i64 {
    let maker_tif = matches!(tif.to_ascii_uppercase().as_str(), "GTD" | "GTC" | "POST_ONLY");
    if maker_tif && is_entry_action(action) {
        live_entry_maker_max_wait_ms().saturating_add(2_000)
    } else if is_live_exit_action(action) {
        5_000
    } else {
        4_000
    }
}

fn should_retry_submit_error(err: &str) -> bool {
    let e = err.to_ascii_lowercase();
    e.contains(" 425")
        || e.contains("status 425")
        || e.contains("too early")
        || e.contains("engine")
        || e.contains("restart")
}

fn build_pending_from_accepted_submission(
    symbol: &str,
    market_type: &str,
    decision_key: &str,
    decision_id: &str,
    decision: &Value,
    effective_target: &LiveMarketTarget,
    payload: &Value,
    order_id: &str,
    submitted_ts_ms: i64,
    ack_ts_ms: i64,
) -> LivePendingOrder {
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
    let quote_size_usdc = payload
        .get("quote_size_usdc")
        .and_then(Value::as_f64)
        .or_else(|| decision.get("quote_size_usdc").and_then(Value::as_f64))
        .unwrap_or(0.0)
        .max(0.0);
    let order_size_shares = payload
        .get("size")
        .and_then(Value::as_f64)
        .unwrap_or(0.0)
        .max(0.0);
    let price_cents = payload
        .get("price")
        .and_then(Value::as_f64)
        .map(|v| v * 100.0)
        .or_else(|| decision.get("price_cents").and_then(Value::as_f64))
        .unwrap_or(0.0)
        .clamp(0.01, 99.99);
    let token_id = payload
        .get("token_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string();
    let cancel_after_ms = initial_pending_cancel_after_ms(&action, &tif);
    let terminal_after_ms = initial_pending_terminal_after_ms(&action, &tif);
    let maker_tif = matches!(tif.as_str(), "GTD" | "GTC" | "POST_ONLY");
    LivePendingOrder {
        symbol: symbol.trim().to_ascii_uppercase(),
        market_type: market_type.to_string(),
        order_id: order_id.to_string(),
        market_id: effective_target.market_id.clone(),
        token_id,
        action: action.clone(),
        side,
        round_id: decision
            .get("round_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        decision_key: decision_key.to_string(),
        decision_id: decision_id.to_string(),
        price_cents,
        quote_size_usdc,
        order_size_shares,
        tif: tif.clone(),
        style,
        submit_reason: decision
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or("live_submit")
            .to_string(),
        submitted_ts_ms,
        ack_ts_ms: ack_ts_ms.max(submitted_ts_ms),
        cancel_after_ms,
        cancel_due_at_ms: if maker_tif {
            ack_ts_ms.max(submitted_ts_ms).saturating_add(cancel_after_ms)
        } else {
            0
        },
        terminal_due_at_ms: ack_ts_ms
            .max(submitted_ts_ms)
            .saturating_add(terminal_after_ms),
        retry_count: 0,
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
            &retry.symbol, &retry.market_type,
            json!({
                "accepted": false,
                "action": retry.action,
                "side": retry.side,
                "round_id": retry.round_id,
                "decision_id": retry.decision_id,
                "decision_key": retry.decision_key,
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
    let mut control = state.get_live_runtime_control(&retry.symbol, &retry.market_type).await;
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
            .put_live_runtime_control(&retry.symbol, &retry.market_type, control.clone())
            .await;
    }
    state
        .append_live_event(
            &retry.symbol, &retry.market_type,
            json!({
                "accepted": false,
                "action": retry.action,
                "side": retry.side,
                "round_id": retry.round_id,
                "decision_id": retry.decision_id,
                "decision_key": retry.decision_key,
                "reason": "auto_force_pause_cancel_failure",
                "order_id": retry.order_id,
                "retry_count": retry.retry_count,
                "runtime_mode": control.mode,
                "runtime_note": control.note,
            }),
        )
        .await;
}

async fn force_pause_for_uncertain_pending(
    state: &ApiState,
    row: &LivePendingOrder,
    reason: &str,
    detail: Value,
) {
    let now_ms = Utc::now().timestamp_millis();
    let mut control = state.get_live_runtime_control(&row.symbol, &row.market_type).await;
    if control.mode != LiveRuntimeControlMode::ForcePause {
        control.mode = LiveRuntimeControlMode::ForcePause;
        control.requested_at_ms = now_ms;
        control.updated_at_ms = now_ms;
        control.completed_at_ms = None;
        control.note = Some(format!("uncertain_pending:{reason}:order_id={}", row.order_id));
        state
            .put_live_runtime_control(&row.symbol, &row.market_type, control.clone())
            .await;
    }
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
                "reason": reason,
                "order_id": row.order_id,
                "runtime_mode": control.mode,
                "runtime_note": control.note,
                "detail": detail
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

async fn get_or_fetch_book_snapshot_cached(
    state: &ApiState,
    ctx: &Arc<RustExecutorContext>,
    token_id: &str,
    local_cache: &mut HashMap<String, Option<GatewayBookSnapshot>>,
) -> Option<GatewayBookSnapshot> {
    if let Some(cached) = local_cache.get(token_id) {
        return cached.clone();
    }
    let snapshot = if let Some(cached) = state.get_rust_book_cache(token_id).await {
        Some(cached)
    } else if let Some(v) = fetch_rust_book_snapshot(ctx, token_id).await {
        state.put_rust_book_cache(token_id, v.clone()).await;
        Some(v)
    } else {
        None
    };
    local_cache.insert(token_id.to_string(), snapshot.clone());
    snapshot
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
    let requested_notional = if amount_mode_buy_usdc {
        ceil_market_buy_amount_usdc(requested_notional)
    } else {
        ceil_quote_amount_usdc(requested_notional)
    };
    let use_market_buy_amount = amount_mode_buy_usdc
        && matches!(pm_side, PmSide::Buy)
        && matches!(order_type, PmOrderType::FAK | PmOrderType::FOK)
        && requested_notional > 0.0;

    let resp = if use_market_buy_amount {
        let amount_decimals = live_market_buy_amount_decimals() as usize;
        let amount_dec =
            PmDecimal::from_str(&format_decimal_compact(requested_notional, amount_decimals))
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
    exec_cfg: &LiveExecutionConfig,
    ctx: &Arc<RustExecutorContext>,
    row: &LivePendingOrder,
    terminal_status: &str,
    retry_book_cache: &mut HashMap<String, Option<GatewayBookSnapshot>>,
) -> bool {
    let action = row.action.to_ascii_lowercase();
    let exit_like = is_live_exit_action(&action);
    if !exit_like {
        return false;
    }
    let emergency_exit = exit_like && is_emergency_exit_reason(&row.submit_reason);
    let max_retry = if emergency_exit { 2 } else { 1 };
    if row.retry_count >= max_retry {
        return false;
    }
    let now_ms = Utc::now().timestamp_millis();
    let target = match resolve_live_market_target_with_state(state, &row.symbol, &row.market_type).await {
        Ok(v) => v,
        Err(err) => {
            state
                .append_live_event(
                    &row.symbol, &row.market_type,
                    json!({
                        "accepted": false,
                        "action": row.action,
                        "side": row.side,
                        "round_id": row.round_id,
                        "decision_id": row.decision_id,
                        "decision_key": row.decision_key,
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
                &row.symbol, &row.market_type,
                json!({
                    "accepted": false,
                    "action": row.action,
                        "side": row.side,
                        "round_id": row.round_id,
                        "decision_id": row.decision_id,
                        "decision_key": row.decision_key,
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
    if let Err(reason) = validate_entry_target_readiness(&json!({
        "action": row.action,
        "side": row.side,
        "round_id": row.round_id
    }), &effective_target, now_ms) {
        state
            .append_live_event(
                &row.symbol, &row.market_type,
                json!({
                    "accepted": false,
                    "action": row.action,
                    "side": row.side,
                    "round_id": row.round_id,
                    "decision_id": row.decision_id,
                    "decision_key": row.decision_key,
                    "reason": format!("rust_terminal_rejected_resubmit_{reason}"),
                    "order_id": row.order_id,
                    "status": terminal_status,
                    "target_market_id": effective_target.market_id,
                    "target_end_date": effective_target.end_date,
                    "now_ms": now_ms
                }),
            )
            .await;
        return false;
    }
    let mut decision = json!({
        "decision_id": row.decision_id,
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
            (exec_cfg.exit_slippage_bps + if emergency_exit { 22.0 } else { 14.0 }).max(34.0)
        } else {
            (exec_cfg.entry_slippage_bps + 12.0).max(28.0)
        }
    });
    if exit_like {
        let ps = state.get_live_position_state(&row.symbol, &row.market_type).await;
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
        get_or_fetch_book_snapshot_cached(state, ctx, &token_id, retry_book_cache).await
    } else {
        None
    };
    let Some(payload) = decision_to_live_payload(
        &decision,
        &effective_target,
        exec_cfg,
        book_snapshot.as_ref(),
        Some(row.quote_size_usdc),
    ) else {
        state
            .append_live_event(
                &row.symbol, &row.market_type,
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
                    pending.ack_ts_ms = now_ms;
                    pending.cancel_after_ms = if emergency_exit { 700 } else { 1500 };
                    pending.cancel_due_at_ms = 0;
                    pending.terminal_due_at_ms =
                        now_ms.saturating_add(initial_pending_terminal_after_ms(&row.action, "FAK"));
                    state.upsert_pending_order(pending).await;
                    state
                        .append_live_event(
                            &row.symbol, &row.market_type,
                            json!({
                                "accepted": true,
                                "action": row.action,
                                "side": row.side,
                                "round_id": row.round_id,
                                "decision_id": row.decision_id,
                                "decision_key": row.decision_key,
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
                    &row.symbol, &row.market_type,
                    json!({
                        "accepted": false,
                        "action": row.action,
                        "side": row.side,
                        "round_id": row.round_id,
                        "decision_id": row.decision_id,
                        "decision_key": row.decision_key,
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
                    &row.symbol, &row.market_type,
                    json!({
                        "accepted": false,
                        "action": row.action,
                        "side": row.side,
                        "round_id": row.round_id,
                        "decision_id": row.decision_id,
                        "decision_key": row.decision_key,
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
    exec_cfg: &LiveExecutionConfig,
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
    let token_ids = collect_decision_token_ids(target, decisions);
    let mut book_cache = prefetch_rust_books_for_tokens(state, &ctx, &token_ids).await;
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
        let action = gated
            .decision
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        let freshness_ms = if action == "enter" || action == "add" {
            live_signal_entry_freshness_ms()
        } else {
            live_signal_exit_freshness_ms()
        };
        let now_ms = Utc::now().timestamp_millis();
        let decision_ts_ms_raw = gated
            .decision
            .get("ts_ms")
            .and_then(Value::as_i64)
            .filter(|v| *v > 0);
        if let Some(decision_ts_ms_raw) = decision_ts_ms_raw {
            let stale_ms = now_ms.saturating_sub(decision_ts_ms_raw);
            if stale_ms > freshness_ms {
                let skipped = json!({
                    "ok": false,
                    "accepted": false,
                    "decision_key": gated.decision_key,
                    "decision_id": gated.decision.get("decision_id").and_then(Value::as_str),
                    "decision": gated.decision.clone(),
                    "reason": "decision_stale_pre_submit",
                    "stale_ms": stale_ms,
                    "freshness_ms": freshness_ms,
                    "executor": "rust_sdk"
                });
                state
                    .append_live_event(
                        &position_state.symbol,
                        &position_state.market_type,
                        json!({
                            "accepted": false,
                            "action": action,
                            "side": gated.decision.get("side").and_then(Value::as_str).unwrap_or("unknown"),
                            "round_id": gated.decision.get("round_id").and_then(Value::as_str),
                            "decision_id": gated.decision.get("decision_id").and_then(Value::as_str),
                            "decision_key": gated.decision_key,
                            "reason": "decision_stale_pre_submit",
                            "stale_ms": stale_ms,
                            "freshness_ms": freshness_ms
                        }),
                    )
                    .await;
                out.push(skipped);
                continue;
            }
        }

        let prepared = match prepare_live_decision_for_submission(
            gated,
            target,
            position_state,
            exec_cfg,
            exit_quote_override,
            exit_size_override,
        ) {
            Ok(v) => v,
            Err(err) => {
                out.push(prepare_live_decision_error_json(gated, err, Some("rust_sdk")));
                continue;
            }
        };
        let decision_id = prepared
            .decision
            .get("decision_id")
            .and_then(Value::as_str)
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| gated.decision_key.clone());
        let token_id =
            token_id_for_decision(&prepared.decision, &prepared.effective_target).map(str::to_string);
        let book_snapshot = if let Some(token_id) = token_id.clone() {
            get_or_fetch_book_snapshot_cached(state, &ctx, &token_id, &mut book_cache).await
        } else {
            None
        };
        let payload = match try_decision_to_live_payload(
            &prepared.decision,
            &prepared.effective_target,
            exec_cfg,
            book_snapshot.as_ref(),
            None,
        ) {
            Ok(payload) => payload,
            Err(reason) => {
                out.push(json!({
                    "ok": false,
                    "accepted": false,
                    "decision_key": gated.decision_key,
                    "decision": prepared.decision,
                    "reason": reason,
                    "executor": "rust_sdk"
                }));
                continue;
            }
        };
        let attempt_payload = payload.clone();
        let mut attempts = Vec::<Value>::with_capacity(2);
        let mut accepted = false;
        let mut final_response: Option<Value> = None;
        let mut final_error: Option<String> = None;
        let mut final_reject_reason: Option<String> = None;
        let executed_via = "rust_sdk";
        let decision_ts_ms = prepared
            .decision
            .get("ts_ms")
            .and_then(Value::as_i64)
            .filter(|v| *v > 0);
        let submit_start_ts_ms = Utc::now().timestamp_millis();
        let mut ack_ts_ms = submit_start_ts_ms;
        let order_started = Instant::now();
        for attempt in 0..2usize {
            let attempt_started_ts_ms = Utc::now().timestamp_millis();
            let attempt_started = Instant::now();
            let submit = submit_rust_order(&ctx, &attempt_payload).await;
            match submit {
                Ok(resp) => {
                    let attempt_completed_ts_ms = Utc::now().timestamp_millis();
                    ack_ts_ms = ack_ts_ms.max(attempt_completed_ts_ms);
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
                        "started_ts_ms": attempt_started_ts_ms,
                        "completed_ts_ms": attempt_completed_ts_ms,
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
                    break;
                }
                Err(err) => {
                    let attempt_completed_ts_ms = Utc::now().timestamp_millis();
                    ack_ts_ms = ack_ts_ms.max(attempt_completed_ts_ms);
                    attempts.push(json!({
                        "attempt": attempt + 1,
                        "executor": "rust_sdk",
                        "started_ts_ms": attempt_started_ts_ms,
                        "completed_ts_ms": attempt_completed_ts_ms,
                        "latency_ms": attempt_started.elapsed().as_millis() as u64,
                        "request": attempt_payload.clone(),
                        "accepted": false,
                        "error": err
                    }));
                    final_error = Some(err.clone());
                    final_reject_reason = Some(err.clone());
                    if attempt == 0 && should_retry_submit_error(&err) {
                        tokio::time::sleep(Duration::from_millis(350)).await;
                        continue;
                    }
                    break;
                }
            }
        }
        let final_order_id = final_response.as_ref().and_then(|resp| {
            resp.get("order_id")
                .or_else(|| resp.get("id"))
                .and_then(Value::as_str)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        });
        let request_price = payload.get("price").and_then(Value::as_f64);
        let request_size = payload.get("size").and_then(Value::as_f64);
        let request_quote = payload.get("quote_size_usdc").and_then(Value::as_f64);
        let final_price = attempt_payload.get("price").and_then(Value::as_f64);
        let final_size = attempt_payload.get("size").and_then(Value::as_f64);
        let final_quote = attempt_payload.get("quote_size_usdc").and_then(Value::as_f64);
        let signal_to_submit_ms = decision_ts_ms.map(|ts| submit_start_ts_ms.saturating_sub(ts));
        let signal_to_ack_ms = decision_ts_ms.map(|ts| ack_ts_ms.saturating_sub(ts));
        let submit_to_ack_ms = ack_ts_ms.saturating_sub(submit_start_ts_ms);
        let order_latency_ms = order_started.elapsed().as_millis() as u64;
        let row = json!({
            "ok": accepted,
            "accepted": accepted,
            "submitted_ts_ms": submit_start_ts_ms,
            "submit_start_ts_ms": submit_start_ts_ms,
            "ack_ts_ms": ack_ts_ms,
            "decision_ts_ms": decision_ts_ms,
            "signal_to_submit_ms": signal_to_submit_ms,
            "signal_to_ack_ms": signal_to_ack_ms,
            "submit_to_ack_ms": submit_to_ack_ms,
            "symbol": position_state.symbol,
            "market_type": position_state.market_type,
            "decision_key": gated.decision_key,
            "decision_id": decision_id,
            "decision": prepared.decision,
            "order_id": final_order_id,
            "request": payload.clone(),
            "final_request": attempt_payload.clone(),
            "response": final_response,
            "error": final_error,
            "reject_reason": final_reject_reason,
            "attempts": attempts,
            "executor": executed_via,
            "order_latency_ms": order_latency_ms,
            "book_snapshot": book_snapshot,
            "target_market_id": prepared.effective_target.market_id,
            "target_token_id": token_id,
            "round_guard_bypassed": prepared.bypass_round_guard,
            "request_price": request_price,
            "request_size_shares": request_size,
            "request_quote_usdc": request_quote,
            "final_price": final_price,
            "final_size_shares": final_size,
            "final_quote_usdc": final_quote,
            "price_trace": build_execution_price_trace(
                &prepared.decision,
                &payload,
                &attempt_payload,
                final_response.as_ref()
            )
        });
        if accepted {
            if let Some(order_id) = row.get("order_id").and_then(Value::as_str) {
                if !order_id.trim().is_empty() {
                    let pending = build_pending_from_accepted_submission(
                        &position_state.symbol,
                        &position_state.market_type,
                        &gated.decision_key,
                        &decision_id,
                        &prepared.decision,
                        &prepared.effective_target,
                        &attempt_payload,
                        order_id,
                        submit_start_ts_ms,
                        ack_ts_ms,
                    );
                    state.upsert_pending_order(pending).await;
                }
            }
        }
        let submit_event_reason = if accepted {
            "rust_order_submit_accepted"
        } else if final_error.is_some() {
            "rust_order_submit_error"
        } else {
            "rust_order_submit_rejected"
        };
        state
            .append_live_event(
                &position_state.symbol,
                &position_state.market_type,
                json!({
                    "accepted": accepted,
                    "action": prepared.decision.get("action").and_then(Value::as_str).unwrap_or("unknown"),
                    "side": prepared.decision.get("side").and_then(Value::as_str).unwrap_or("unknown"),
                    "round_id": prepared.decision.get("round_id").and_then(Value::as_str),
                    "decision_id": decision_id,
                    "decision_key": gated.decision_key,
                    "reason": submit_event_reason,
                    "order_id": row.get("order_id").cloned().unwrap_or(Value::Null),
                    "executor": executed_via,
                    "attempt_count": row.get("attempts").and_then(Value::as_array).map(|v| v.len()).unwrap_or(0),
                    "order_latency_ms": order_latency_ms,
                    "signal_to_submit_ms": signal_to_submit_ms,
                    "signal_to_ack_ms": signal_to_ack_ms,
                    "submit_to_ack_ms": submit_to_ack_ms,
                    "submit_start_ts_ms": submit_start_ts_ms,
                    "ack_ts_ms": ack_ts_ms,
                    "reject_reason": row.get("reject_reason").cloned().unwrap_or(Value::Null),
                    "error": row.get("error").cloned().unwrap_or(Value::Null),
                }),
            )
            .await;
        out.push(row);
    }
    out
}

pub(super) async fn reconcile_rust_reports(state: &ApiState, exec_cfg: &LiveExecutionConfig) {
    let ctx = match get_or_init_rust_executor(state).await {
        Ok(v) => v,
        Err(err) => {
            tracing::warn!(error = %err, "rust sdk reconcile unavailable");
            return;
        }
    };
    let pending = state.list_pending_orders().await;
    if pending.is_empty() {
        return;
    }
    let status_rows = join_all(pending.into_iter().map(|row| {
        let ctx = Arc::clone(&ctx);
        async move {
            let status = ctx.client.order(&row.order_id).await;
            (row, status)
        }
    }))
    .await;
    let mut retry_book_cache = HashMap::<String, Option<GatewayBookSnapshot>>::new();
    for (row, status) in status_rows {
        let Ok(order) = status else {
            continue;
        };
        if pm_is_terminal_fill(&order.status) {
            let _ = state.remove_pending_order(&row.order_id).await;
            let fill_event = fill_event_from_open_order(&order);
            let fill_meta = extract_pending_fill_meta(Some(&fill_event), &row);
            apply_pending_confirmation(
                state,
                &row,
                "rust_order_terminal_filled",
                Some(&fill_event),
            )
            .await;
            let ps_after = state
                .get_live_position_state(&row.symbol, &row.market_type)
                .await;
            let fill_price_cents = fill_meta
                .fill_price_cents
                .unwrap_or(row.price_cents)
                .max(0.01);
            let fill_quote_usdc = fill_meta
                .fill_quote_usdc
                .unwrap_or(row.quote_size_usdc.max(0.0))
                .max(0.0);
            let fill_size_shares = fill_meta
                .fill_size_shares
                .unwrap_or_else(|| {
                    if fill_quote_usdc > 0.0 {
                        fill_quote_usdc / (fill_price_cents / 100.0).max(0.0001)
                    } else {
                        row.order_size_shares.max(0.0)
                    }
                })
                .max(0.0);
            let fill_fee_cents = (fill_meta.fee_usdc.max(0.0)) * 100.0;
            let fill_slippage_cents = (fill_meta.slippage_usdc.max(0.0)) * 100.0;
            let fill_cost_cents = fill_fee_cents + fill_slippage_cents;
            state
                .append_live_event(
                    &row.symbol, &row.market_type,
                    json!({
                        "accepted": true,
                        "action": row.action,
                        "side": row.side,
                        "round_id": row.round_id,
                        "decision_id": row.decision_id,
                        "decision_key": row.decision_key,
                        "reason": "rust_order_terminal_filled",
                        "order_id": row.order_id,
                        "fill_price_cents": fill_price_cents,
                        "fill_quote_usdc": fill_quote_usdc,
                        "fill_size_shares": fill_size_shares,
                        "fill_fee_cents": fill_fee_cents,
                        "fill_slippage_cents": fill_slippage_cents,
                        "fill_cost_cents": fill_cost_cents,
                        "fill_pnl_cents_net": ps_after.last_fill_pnl_usdc * 100.0,
                        "position_realized_pnl_cents": ps_after.realized_pnl_usdc * 100.0
                    }),
                )
                .await;
        } else if pm_is_terminal_reject(&order.status) {
            let _ = state.remove_pending_order(&row.order_id).await;
            let terminal_status = format!("{}", order.status);
            let retried = try_resubmit_after_terminal_reject_rust(
                state,
                exec_cfg,
                &ctx,
                &row,
                &terminal_status,
                &mut retry_book_cache,
            )
            .await;
            if retried {
                continue;
            }
            apply_pending_revert(state, &row, "rust_order_terminal_rejected").await;
            state
                .append_live_event(
                    &row.symbol, &row.market_type,
                    json!({
                        "accepted": false,
                        "action": row.action,
                        "side": row.side,
                        "round_id": row.round_id,
                        "decision_id": row.decision_id,
                        "decision_key": row.decision_key,
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
    exec_cfg: &LiveExecutionConfig,
) {
    let ctx = match get_or_init_rust_executor(state).await {
        Ok(v) => v,
        Err(err) => {
            tracing::warn!(error = %err, "rust sdk cancel unavailable");
            return;
        }
    };
    let now_ms = Utc::now().timestamp_millis();
    let pending = state.list_pending_orders().await;
    let due_rows = pending
        .into_iter()
        .filter(|row| {
            let maker_tif = matches!(
                row.tif.to_ascii_uppercase().as_str(),
                "GTD" | "GTC" | "POST_ONLY"
            );
            if maker_tif {
                now_ms >= pending_cancel_due_ms(row)
            } else {
                now_ms >= pending_terminal_due_ms(row)
            }
        })
        .collect::<Vec<_>>();
    if due_rows.is_empty() {
        return;
    }
    let mut timeout_book_cache = HashMap::<String, Option<GatewayBookSnapshot>>::new();
    for row in due_rows {
        let maker_tif = matches!(
            row.tif.to_ascii_uppercase().as_str(),
            "GTD" | "GTC" | "POST_ONLY"
        );
        if !maker_tif {
            match ctx.client.order(&row.order_id).await {
                Ok(order) if pm_is_terminal_fill(&order.status) => {
                    let _ = state.remove_pending_order(&row.order_id).await;
                    let fill_event = fill_event_from_open_order(&order);
                    apply_pending_confirmation(
                        state,
                        &row,
                        "rust_terminal_timeout_reconciled_fill",
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
                                "reason": "rust_terminal_timeout_reconciled_fill",
                                "order_id": row.order_id,
                                "status": format!("{}", order.status)
                            }),
                        )
                        .await;
                }
                Ok(order) if pm_is_terminal_reject(&order.status) => {
                    let _ = state.remove_pending_order(&row.order_id).await;
                    let terminal_status = format!("{}", order.status);
                    let retried = try_resubmit_after_terminal_reject_rust(
                        state,
                        exec_cfg,
                        &ctx,
                        &row,
                        &terminal_status,
                        &mut timeout_book_cache,
                    )
                    .await;
                    if !retried {
                        apply_pending_revert(state, &row, "rust_terminal_timeout_rejected").await;
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
                                    "reason": "rust_terminal_timeout_rejected",
                                    "order_id": row.order_id,
                                    "status": terminal_status
                                }),
                            )
                            .await;
                    }
                }
                Ok(order) => {
                    let mut bumped = row.clone();
                    bumped.terminal_due_at_ms = now_ms.saturating_add(30_000);
                    state.upsert_pending_order(bumped).await;
                    force_pause_for_uncertain_pending(
                        state,
                        &row,
                        "rust_terminal_timeout_unresolved",
                        json!({
                            "status": format!("{}", order.status),
                            "terminal_due_at_ms": pending_terminal_due_ms(&row)
                        }),
                    )
                    .await;
                }
                Err(err) => {
                    let mut bumped = row.clone();
                    bumped.terminal_due_at_ms = now_ms.saturating_add(30_000);
                    state.upsert_pending_order(bumped).await;
                    force_pause_for_uncertain_pending(
                        state,
                        &row,
                        "rust_terminal_timeout_status_error",
                        json!({
                            "error": err.to_string(),
                            "terminal_due_at_ms": pending_terminal_due_ms(&row)
                        }),
                    )
                    .await;
                }
            }
            continue;
        }

        match ctx.client.cancel_order(&row.order_id).await {
            Ok(resp) => {
                match ctx.client.order(&row.order_id).await {
                    Ok(order) if pm_is_terminal_fill(&order.status) => {
                        let _ = state.remove_pending_order(&row.order_id).await;
                        let fill_event = fill_event_from_open_order(&order);
                        apply_pending_confirmation(
                            state,
                            &row,
                            "rust_cancel_post_reconcile_filled",
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
                                    "reason": "rust_cancel_post_reconcile_filled",
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
                        apply_pending_revert(state, &row, "rust_timeout_cancelled").await;
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
                                    "reason": "rust_timeout_cancelled",
                                    "order_id": row.order_id,
                                    "cancelled": resp.canceled,
                                    "not_cancelled": resp.not_canceled,
                                    "status": format!("{}", order.status)
                                }),
                            )
                            .await;
                    }
                    Ok(order) => {
                        let mut bumped = row.clone();
                        bumped.cancel_due_at_ms = now_ms.saturating_add(30_000);
                        bumped.terminal_due_at_ms = now_ms.saturating_add(30_000);
                        state.upsert_pending_order(bumped).await;
                        force_pause_for_uncertain_pending(
                            state,
                            &row,
                            "rust_cancel_post_reconcile_unresolved",
                            json!({
                                "cancelled": resp.canceled,
                                "not_cancelled": resp.not_canceled,
                                "status": format!("{}", order.status)
                            }),
                        )
                        .await;
                    }
                    Err(err) => {
                        if cancel_response_contains_order(&resp, &row.order_id) {
                            let _ = state.remove_pending_order(&row.order_id).await;
                            apply_pending_revert(state, &row, "rust_timeout_cancelled").await;
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
                                        "reason": "rust_timeout_cancelled",
                                        "order_id": row.order_id,
                                        "cancelled": resp.canceled,
                                        "not_cancelled": resp.not_canceled,
                                        "status_error": err.to_string()
                                    }),
                                )
                                .await;
                        } else {
                            let mut bumped = row.clone();
                            bumped.cancel_due_at_ms = now_ms.saturating_add(30_000);
                            bumped.terminal_due_at_ms = now_ms.saturating_add(30_000);
                            state.upsert_pending_order(bumped).await;
                            force_pause_for_uncertain_pending(
                                state,
                                &row,
                                "rust_cancel_post_status_error",
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
