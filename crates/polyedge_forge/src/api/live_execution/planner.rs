pub(super) fn decision_to_live_payload(
    decision: &Value,
    target: &LiveMarketTarget,
    exec_cfg: &LiveExecutionConfig,
    book: Option<&GatewayBookSnapshot>,
    quote_size_override: Option<f64>,
) -> Option<Value> {
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
    let is_exit_like = action == "exit" || action == "reduce";
    let (gateway_side, token_id, mut slippage_bps) = match (action.as_str(), side.as_str()) {
        ("enter", "UP") | ("add", "UP") => {
            ("buy_yes", target.token_id_yes.as_str(), exec_cfg.entry_slippage_bps)
        }
        ("exit", "UP") | ("reduce", "UP") => {
            ("sell_yes", target.token_id_yes.as_str(), exec_cfg.exit_slippage_bps)
        }
        ("enter", "DOWN") | ("add", "DOWN") => {
            ("buy_no", target.token_id_no.as_str(), exec_cfg.entry_slippage_bps)
        }
        ("exit", "DOWN") | ("reduce", "DOWN") => {
            ("sell_no", target.token_id_no.as_str(), exec_cfg.exit_slippage_bps)
        }
        _ => return None,
    };
    let mut price_cents = decision
        .get("price_cents")
        .and_then(Value::as_f64)
        .unwrap_or(50.0);
    if !price_cents.is_finite() {
        price_cents = 50.0;
    }
    price_cents = price_cents.clamp(1.0, 99.0);
    let mut price = (price_cents / 100.0).clamp(0.01, 0.99);
    let mut tif = decision
        .get("tif")
        .and_then(Value::as_str)
        .map(|v| v.trim().to_ascii_uppercase())
        .filter(|v| matches!(v.as_str(), "FAK" | "FOK" | "GTC" | "GTD" | "POST_ONLY"))
        .unwrap_or_else(|| "FAK".to_string());
    let mut style = decision
        .get("style")
        .and_then(Value::as_str)
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| matches!(v.as_str(), "maker" | "taker"))
        .unwrap_or_else(|| {
            if tif == "GTC" || tif == "GTD" || tif == "POST_ONLY" {
                "maker".to_string()
            } else {
                "taker".to_string()
            }
        });
    let mut quote_size = quote_size_override.unwrap_or_else(|| {
        decision
            .get("quote_size_usdc")
            .and_then(Value::as_f64)
            .unwrap_or(exec_cfg.min_quote_usdc)
    });
    quote_size = quote_size.max(exec_cfg.min_quote_usdc);
    let mut notes: Vec<String> = Vec::with_capacity(3);
    let is_buy = gateway_side.starts_with("buy_");
    let fixed_entry_size_shares = if !is_exit_like {
        live_fixed_entry_size_shares()
    } else {
        None
    };
    let forced_exit_size_shares = if is_exit_like {
        decision
            .get("position_size_shares")
            .and_then(Value::as_f64)
            .filter(|v| v.is_finite() && *v > 0.0)
    } else {
        None
    };
    let forced_size_shares = forced_exit_size_shares.or(fixed_entry_size_shares);
    let mut min_order_size = 0.01_f64;
    let mut size_floor = min_order_size;
    let mut size = forced_size_shares.unwrap_or_else(|| (quote_size / price).max(size_floor));
    if let Some(book) = book {
        min_order_size = book.min_order_size.max(0.0001);
        let taker_like = style == "taker" || matches!(tif.as_str(), "FAK" | "FOK");
        size_floor = min_order_size;
        let tick_size = book.tick_size.max(0.0001);
        price = round_to_tick(price, tick_size, is_buy);
        size = forced_size_shares.unwrap_or_else(|| (quote_size / price).max(size_floor));

        let depth_top1 = if is_buy {
            book.best_ask_size.unwrap_or(0.0)
        } else {
            book.best_bid_size.unwrap_or(0.0)
        };
        let depth_top3 = if is_buy {
            book.ask_depth_top3.unwrap_or(0.0)
        } else {
            book.bid_depth_top3.unwrap_or(0.0)
        };
        if forced_size_shares.is_none()
            && !(is_buy && taker_like)
            && depth_top1.is_finite()
            && depth_top1 > 0.0
            && size > depth_top1 * 1.12
        {
            let capped = (depth_top1 * 0.95).max(size_floor);
            if capped < size {
                size = capped;
                notes.push("depth_cap_top1".to_string());
            }
        }
        if depth_top3.is_finite() && depth_top3 > 0.0 && size > depth_top3 * 1.25 {
            if is_exit_like {
                slippage_bps = (slippage_bps + 12.0).min(500.0);
                notes.push("depth_thin_exit_force_taker".to_string());
            } else {
                slippage_bps = (slippage_bps + 8.0).min(500.0);
                notes.push("depth_thin_entry_boost_slippage".to_string());
            }
        }
    }
    size = round_lot_size(size.max(size_floor));
    if is_exit_like && notes.iter().any(|n| n == "depth_thin_exit_force_taker") {
        tif = "FAK".to_string();
        style = "taker".to_string();
    }
    let ttl_ms = decision
        .get("ttl_ms")
        .and_then(Value::as_i64)
        .unwrap_or(if is_exit_like { 900 } else { 1400 })
        .clamp(300, 30_000);
    slippage_bps = decision
        .get("max_slippage_bps")
        .and_then(Value::as_f64)
        .unwrap_or(slippage_bps)
        .clamp(0.0, 500.0);
    if let Some(force) = exec_cfg.force_slippage_bps {
        slippage_bps = force.clamp(0.0, 500.0);
    }
    if let Some(book) = book {
        let taker_like = style == "taker" || matches!(tif.as_str(), "FAK" | "FOK");
        if taker_like {
            if is_buy {
                if let Some(best_ask) = book.best_ask.filter(|v| v.is_finite() && *v > 0.0) {
                    let anchored = best_ask.clamp(0.01, 0.99);
                    if anchored > price {
                        price = anchored;
                        notes.push("anchor_taker_to_best_ask".to_string());
                    }
                }
            } else if let Some(best_bid) = book.best_bid.filter(|v| v.is_finite() && *v > 0.0) {
                let anchored = best_bid.clamp(0.01, 0.99);
                if anchored < price {
                    price = anchored;
                    notes.push("anchor_taker_to_best_bid".to_string());
                }
            }
            let depth_top3 = if is_buy {
                book.ask_depth_top3.unwrap_or(0.0)
            } else {
                book.bid_depth_top3.unwrap_or(0.0)
            };
            if depth_top3.is_finite() && depth_top3 > 0.0 {
                let depth_util = (size / depth_top3).max(0.0);
                let mut extra_bps = if depth_util <= 0.25 {
                    2.0
                } else if depth_util <= 0.5 {
                    4.0
                } else if depth_util <= 0.85 {
                    8.0
                } else if depth_util <= 1.0 {
                    14.0
                } else if depth_util <= 1.3 {
                    22.0
                } else {
                    34.0
                };
                if let (Some(best_bid), Some(best_ask)) = (book.best_bid, book.best_ask) {
                    if best_bid.is_finite()
                        && best_ask.is_finite()
                        && best_bid > 0.0
                        && best_ask > 0.0
                    {
                        let mid = ((best_bid + best_ask) * 0.5).max(0.0001);
                        let spread_bps = ((best_ask - best_bid) / mid * 10_000.0).max(0.0);
                        if spread_bps >= 100.0 {
                            extra_bps += 4.0;
                        }
                    }
                }
                if is_exit_like {
                    extra_bps += 4.0;
                }
                if extra_bps > 0.0 {
                    let adjusted = if is_buy {
                        (price * (1.0 + extra_bps / 10_000.0)).clamp(0.01, 0.99)
                    } else {
                        (price * (1.0 - extra_bps / 10_000.0)).clamp(0.01, 0.99)
                    };
                    if (adjusted - price).abs() >= 0.00001 {
                        price = adjusted;
                        notes.push(format!("adaptive_taker_price_bps_{extra_bps:.1}"));
                    }
                    let min_slippage =
                        (extra_bps + if is_exit_like { 18.0 } else { 12.0 }).min(500.0);
                    if min_slippage > slippage_bps {
                        slippage_bps = min_slippage;
                        notes.push("adaptive_slippage_floor".to_string());
                    }
                }
            }
            let tick_size = book.tick_size.max(0.0001);
            price = round_to_tick(price, tick_size, is_buy);
            size_floor = min_order_size;
            size = round_lot_size(
                forced_size_shares
                    .unwrap_or_else(|| (quote_size / price).max(size_floor))
                    .max(0.01),
            );
        }
    }
    if let Some(forced) = forced_size_shares {
        size = if is_exit_like {
            floor_lot_size(forced.max(size_floor).max(0.01))
        } else {
            round_lot_size(forced.max(size_floor).max(0.01))
        };
        if is_exit_like {
            notes.push("force_exit_full_size".to_string());
        } else {
            notes.push("force_entry_fixed_size".to_string());
        }
    }
    let taker_like = style == "taker" || matches!(tif.as_str(), "FAK" | "FOK");
    let floor_notional_usdc = (size * price).max(0.0);
    let requested_notional_usdc = ceil_quote_amount_usdc(quote_size.max(floor_notional_usdc));
    let buy_amount_usdc = if is_buy && taker_like && fixed_entry_size_shares.is_none() {
        Some(requested_notional_usdc)
    } else {
        None
    };
    let cache_key = if let Some(amount) = buy_amount_usdc {
        format!(
            "fev1:{}:{}:{}:{:.4}:{:.4}:a{:.4}",
            target.market_id, gateway_side, action, price, size, amount
        )
    } else {
        format!(
            "fev1:{}:{}:{}:{:.4}:{:.4}",
            target.market_id, gateway_side, action, price, size
        )
    };
    let mut payload = json!({
        "market_id": target.market_id,
        "token_id": token_id,
        "side": gateway_side,
        "price": price,
        "size": size,
        "quote_size_usdc": requested_notional_usdc,
        "requested_notional_usdc": requested_notional_usdc,
        "tif": tif,
        "style": style,
        "ttl_ms": ttl_ms,
        "max_slippage_bps": slippage_bps,
        "execution_notes": notes,
        "cache_key": cache_key,
        "action": action,
        "signal_side": side,
        "book_meta": if let Some(book) = book {
            json!({
                "token_id": book.token_id,
                "min_order_size": book.min_order_size,
                "tick_size": book.tick_size,
                "best_bid": book.best_bid,
                "best_ask": book.best_ask,
                "best_bid_size": book.best_bid_size,
                "best_ask_size": book.best_ask_size,
                "bid_depth_top3": book.bid_depth_top3,
                "ask_depth_top3": book.ask_depth_top3
            })
        } else {
            Value::Null
        }
    });
    if let Some(amount) = buy_amount_usdc {
        if let Some(obj) = payload.as_object_mut() {
            obj.insert("buy_amount_usdc".to_string(), json!(amount));
            obj.insert("amount_mode".to_string(), json!("buy_usdc"));
        }
    }
    Some(payload)
}

pub(super) fn extract_rust_reject_reason(payload: &Value, fallback_error: Option<&str>) -> String {
    payload
        .get("error_msg")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .or_else(|| payload.get("status").and_then(Value::as_str))
        .or_else(|| payload.get("error").and_then(Value::as_str))
        .or(fallback_error)
        .unwrap_or("unknown_reject")
        .to_string()
}

pub(super) fn can_retry_on_liquidity(reason: &str) -> bool {
    let r = reason.to_ascii_lowercase();
    let non_retryable = r.contains("unauthorized")
        || r.contains("forbidden")
        || r.contains("signature")
        || r.contains("invalid api key")
        || r.contains("invalid key")
        || r.contains("nonce")
        || r.contains("insufficient balance")
        || r.contains("not enough balance")
        || r.contains("allowance")
        || r.contains("bad request")
        || r.contains("invalid order")
        || r.contains("tick size")
        || r.contains("min order size")
        || r.contains("market closed")
        || r.contains("not tradable")
        || r.contains("round_target_mismatch")
        || r.contains("no_live_market_target")
        || r.contains("side_mismatch");
    if non_retryable {
        return false;
    }
    let explicit_liquidity = r.contains("no orders found to match")
        || r.contains("insufficient liquidity")
        || r.contains("cannot be matched")
        || r.contains("would not fill")
        || r.contains("unmatched")
        || r.contains("no match")
        || r.contains("empty book");
    let timeout_like_liquidity =
        r.contains("timeout") && (r.contains("match") || r.contains("liquidity") || r.contains("maker"));
    explicit_liquidity || timeout_like_liquidity
}

pub(super) fn is_live_exit_action(action: &str) -> bool {
    let a = action.to_ascii_lowercase();
    a == "exit" || a == "reduce"
}

pub(super) fn is_emergency_exit_reason(reason: &str) -> bool {
    let r = reason.to_ascii_lowercase();
    r.contains("stop_loss")
        || r.contains("signal_reverse")
        || r.contains("trail_drawdown")
        || r.contains("liquidity_widen")
        || r.contains("round_rollover")
        || r.contains("blocked_exit_escalation")
        || r.contains("panic_exit")
        || r.contains("emergency")
}

pub(super) fn decision_is_emergency_exit(decision: &Value) -> bool {
    let action = decision
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !is_live_exit_action(action) {
        return false;
    }
    decision
        .get("reason")
        .and_then(Value::as_str)
        .map(is_emergency_exit_reason)
        .unwrap_or(false)
}

pub(super) fn apply_emergency_exit_overrides(decision: &mut Value, exec_cfg: &LiveExecutionConfig) {
    if !decision_is_emergency_exit(decision) {
        return;
    }
    if let Some(obj) = decision.as_object_mut() {
        obj.insert("tif".to_string(), json!("FAK"));
        obj.insert("style".to_string(), json!("taker"));
        let ttl_ms = obj
            .get("ttl_ms")
            .and_then(Value::as_i64)
            .unwrap_or(900)
            .clamp(350, 700);
        obj.insert("ttl_ms".to_string(), json!(ttl_ms));
        let slippage = obj
            .get("max_slippage_bps")
            .and_then(Value::as_f64)
            .unwrap_or(exec_cfg.exit_slippage_bps)
            .max(exec_cfg.exit_slippage_bps + 14.0)
            .clamp(34.0, 140.0);
        obj.insert("max_slippage_bps".to_string(), json!(slippage));
        obj.insert("emergency_exit".to_string(), Value::Bool(true));
    }
}

pub(super) fn build_retry_payload(current: &Value, reason: &str, attempt: usize) -> Option<Value> {
    if attempt >= 2 || !can_retry_on_liquidity(reason) {
        return None;
    }
    let action = current
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let is_exit_like = is_live_exit_action(&action);
    let emergency_exit = current
        .get("reason")
        .and_then(Value::as_str)
        .map(is_emergency_exit_reason)
        .unwrap_or(false);
    let mut next = current.clone();
    let current_slippage = current
        .get("max_slippage_bps")
        .and_then(Value::as_f64)
        .unwrap_or(20.0)
        .clamp(0.0, 500.0);
    let current_ttl = current
        .get("ttl_ms")
        .and_then(Value::as_i64)
        .unwrap_or(1200)
        .clamp(300, 30_000);
    let current_tif = current
        .get("tif")
        .and_then(Value::as_str)
        .unwrap_or("FAK")
        .to_ascii_uppercase();
    let maker_mode = matches!(current_tif.as_str(), "GTD" | "GTC" | "POST_ONLY");
    let side = current
        .get("side")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let is_buy = side.starts_with("buy_");
    let is_taker_like = matches!(current_tif.as_str(), "FAK" | "FOK")
        || current
            .get("style")
            .and_then(Value::as_str)
            .map(|s| s.eq_ignore_ascii_case("taker"))
            .unwrap_or(false);
    let tick_size = current
        .get("book_meta")
        .and_then(|v| v.get("tick_size"))
        .and_then(Value::as_f64)
        .unwrap_or(0.01)
        .max(0.0001);
    let current_price = current
        .get("price")
        .and_then(Value::as_f64)
        .unwrap_or(0.5)
        .clamp(0.01, 0.99);

    if is_taker_like {
        let (slip_boost, ttl_target, tick_steps) = if attempt == 0 {
            if is_exit_like {
                if emergency_exit {
                    (24.0, 520_i64, 1_u32)
                } else {
                    (16.0, 620_i64, 1_u32)
                }
            } else {
                (12.0, 900_i64, 1_u32)
            }
        } else if is_exit_like {
            if emergency_exit {
                (36.0, 420_i64, 3_u32)
            } else {
                (28.0, 520_i64, 2_u32)
            }
        } else {
            (18.0, 760_i64, 2_u32)
        };
        let boosted_slippage =
            (current_slippage + slip_boost).min(if is_exit_like { 180.0 } else { 120.0 });
        let shifted_price = {
            let step = tick_size * (tick_steps as f64);
            let raw = if is_buy {
                current_price + step
            } else {
                current_price - step
            };
            round_to_tick(raw.clamp(0.01, 0.99), tick_size, is_buy)
        };
        let next_ttl = if is_exit_like {
            current_ttl.min(ttl_target).max(320)
        } else {
            current_ttl.min(ttl_target).max(420)
        };
        if let Some(obj) = next.as_object_mut() {
            obj.insert("tif".to_string(), json!("FAK"));
            obj.insert("style".to_string(), json!("taker"));
            obj.insert("price".to_string(), json!(shifted_price));
            obj.insert("max_slippage_bps".to_string(), json!(boosted_slippage));
            obj.insert("ttl_ms".to_string(), json!(next_ttl));
            obj.insert(
                "retry_tag".to_string(),
                json!(if is_exit_like {
                    if attempt == 0 {
                        "exit_fak_ladder_step_1"
                    } else {
                        "exit_fak_ladder_step_2"
                    }
                } else if attempt == 0 {
                    "entry_fak_ladder_step_1"
                } else {
                    "entry_fak_ladder_step_2"
                }),
            );
            let lock_retry_size = if is_exit_like
                || (matches!(action.as_str(), "enter" | "add")
                    && live_fixed_entry_size_shares().is_some())
            {
                obj.get("size")
                    .and_then(Value::as_f64)
                    .filter(|v| v.is_finite() && *v > 0.0)
                    .or_else(|| {
                        if matches!(action.as_str(), "enter" | "add") {
                            live_fixed_entry_size_shares()
                        } else {
                            None
                        }
                    })
            } else {
                None
            };
            if let Some(sz) = lock_retry_size {
                obj.insert("size".to_string(), json!(round_lot_size(sz)));
            } else if let Some(q) = obj
                .get("quote_size_usdc")
                .and_then(Value::as_f64)
                .or_else(|| obj.get("requested_notional_usdc").and_then(Value::as_f64))
                .filter(|v| v.is_finite() && *v > 0.0)
            {
                let resized = (q / shifted_price).max(0.01);
                obj.insert("size".to_string(), json!(round_lot_size(resized)));
            }
        }
        return Some(next);
    }

    if maker_mode {
        let entry_like = is_entry_action(&action);
        let ttl_ms = if entry_like {
            live_entry_fak_ttl_ms()
        } else {
            (current_ttl / 2).clamp(520, 1_400)
        };
        let slip_boost = if is_exit_like {
            20.0
        } else {
            live_entry_fak_slippage_boost_bps()
        };
        if let Some(obj) = next.as_object_mut() {
            obj.insert("tif".to_string(), json!("FAK"));
            obj.insert("style".to_string(), json!("taker"));
            obj.insert("ttl_ms".to_string(), json!(ttl_ms));
            obj.insert(
                "max_slippage_bps".to_string(),
                json!((current_slippage + slip_boost).min(160.0)),
            );
            obj.insert("retry_tag".to_string(), json!("maker_timeout_to_fak"));
        }
        return Some(next);
    }

    None
}

pub(super) fn build_execution_price_trace(
    decision: &Value,
    request: &Value,
    final_request: &Value,
    response: Option<&Value>,
) -> Value {
    let signal_price_cents = decision.get("price_cents").and_then(Value::as_f64);
    let submit_price_cents = request
        .get("price")
        .and_then(Value::as_f64)
        .map(|v| (v * 100.0).clamp(0.0, 100.0));
    let final_submit_price_cents = final_request
        .get("price")
        .and_then(Value::as_f64)
        .map(|v| (v * 100.0).clamp(0.0, 100.0));
    let accepted_price_cents = response
        .and_then(|v| v.get("price").and_then(Value::as_f64))
        .map(|v| (v * 100.0).clamp(0.0, 100.0))
        .or(final_submit_price_cents);
    let signal_vs_submit_cents = match (signal_price_cents, submit_price_cents) {
        (Some(signal), Some(submit)) => Some(submit - signal),
        _ => None,
    };
    let signal_vs_accepted_cents = match (signal_price_cents, accepted_price_cents) {
        (Some(signal), Some(accepted)) => Some(accepted - signal),
        _ => None,
    };
    json!({
        "signal_price_cents": signal_price_cents,
        "submit_price_cents": submit_price_cents,
        "final_submit_price_cents": final_submit_price_cents,
        "accepted_price_cents": accepted_price_cents,
        "fill_price_cents": Value::Null,
        "signal_vs_submit_cents": signal_vs_submit_cents,
        "signal_vs_accepted_cents": signal_vs_accepted_cents,
    })
}

#[derive(Debug, Clone, Default)]
pub(super) struct PendingFillMeta {
    fill_price_cents: Option<f64>,
    fill_quote_usdc: Option<f64>,
    fill_size_shares: Option<f64>,
    fee_usdc: f64,
    slippage_usdc: f64,
}

fn value_as_f64(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
}

fn collect_f64_by_key(node: &Value, target_key: &str, out: &mut Vec<f64>) {
    match node {
        Value::Object(map) => {
            for (k, v) in map {
                if k.eq_ignore_ascii_case(target_key) {
                    if let Some(x) = value_as_f64(v) {
                        if x.is_finite() {
                            out.push(x);
                        }
                    }
                }
                collect_f64_by_key(v, target_key, out);
            }
        }
        Value::Array(rows) => {
            for row in rows {
                collect_f64_by_key(row, target_key, out);
            }
        }
        _ => {}
    }
}

fn collect_candidates(node: &Value, keys: &[&str]) -> Vec<f64> {
    let mut out = Vec::<f64>::new();
    for key in keys {
        collect_f64_by_key(node, key, &mut out);
    }
    out
}

fn normalize_usdc_amount(v: f64) -> f64 {
    if v <= 0.0 || !v.is_finite() {
        0.0
    } else if v > 100_000.0 {
        (v / 1_000_000.0).max(0.0)
    } else {
        v
    }
}

pub(super) fn extract_pending_fill_meta(
    fill_event: Option<&Value>,
    pending: &LivePendingOrder,
) -> PendingFillMeta {
    let Some(fill) = fill_event else {
        return PendingFillMeta::default();
    };
    let price_cents_direct = collect_candidates(
        fill,
        &[
            "price_cents",
            "fill_price_cents",
            "filled_price_cents",
            "avg_price_cents",
        ],
    )
    .into_iter()
    .find(|v| *v > 0.0);
    let price_prob = collect_candidates(
        fill,
        &[
            "price",
            "avg_price",
            "fill_price",
            "filled_price",
            "execution_price",
            "executed_price",
            "matched_price",
        ],
    )
    .into_iter()
    .find(|v| *v > 0.0);
    let fill_price_cents =
        price_cents_direct.or_else(|| price_prob.map(|v| if v <= 1.5 { v * 100.0 } else { v }));

    let quote_direct = collect_candidates(
        fill,
        &[
            "fill_quote_usdc",
            "filled_quote_usdc",
            "executed_quote_usdc",
            "effective_notional",
            "filled_notional",
            "executed_notional",
            "notional",
            "quote_size_usdc",
        ],
    )
    .into_iter()
    .map(normalize_usdc_amount)
    .find(|v| *v > 0.0);

    let size_candidates = collect_candidates(
        fill,
        &[
            "fill_size_shares",
            "size_matched",
            "filled_size",
            "executed_size",
            "matched_size",
            "accepted_size",
            "size",
        ],
    );
    let fill_size_shares = size_candidates.into_iter().find(|v| *v > 0.0);
    let quote_from_size = if let (Some(px_c), Some(sz)) = (fill_price_cents, fill_size_shares) {
        Some((px_c / 100.0) * sz)
    } else {
        None
    };
    let fill_quote_usdc = quote_direct.or(quote_from_size);

    let fee_usdc = collect_candidates(
        fill,
        &["fee_usdc", "total_fee_usdc", "fee", "fees", "total_fee"],
    )
    .into_iter()
    .map(normalize_usdc_amount)
    .fold(0.0, |acc, v| acc + v);

    let explicit_slippage_usdc = collect_candidates(fill, &["slippage_usdc"])
        .into_iter()
        .map(normalize_usdc_amount)
        .fold(0.0, |acc, v| acc + v);

    let inferred_slippage_usdc = if let (Some(px_c), Some(quote)) = (fill_price_cents, fill_quote_usdc)
    {
        let expected_c = pending.price_cents.max(0.01);
        let shares = quote / (px_c / 100.0).max(0.0001);
        let action = pending.action.to_ascii_lowercase();
        let adverse_cents = if action == "enter" || action == "add" {
            (px_c - expected_c).max(0.0)
        } else {
            (expected_c - px_c).max(0.0)
        };
        ((adverse_cents / 100.0) * shares).max(0.0)
    } else {
        0.0
    };

    PendingFillMeta {
        fill_price_cents,
        fill_quote_usdc,
        fill_size_shares,
        fee_usdc,
        slippage_usdc: explicit_slippage_usdc.max(inferred_slippage_usdc),
    }
}
