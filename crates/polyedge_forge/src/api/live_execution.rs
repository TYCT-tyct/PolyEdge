use super::*;

pub(super) struct LiveGatewayConfig {
    pub(super) primary_url: String,
    pub(super) backup_url: Option<String>,
    pub(super) timeout_ms: u64,
    pub(super) min_quote_usdc: f64,
    pub(super) entry_slippage_bps: f64,
    pub(super) exit_slippage_bps: f64,
    pub(super) force_slippage_bps: Option<f64>,
    pub(super) rust_submit_fallback_gateway: bool,
}

impl LiveGatewayConfig {
    pub(super) fn from_env() -> Self {
        let primary_url = std::env::var("FORGE_FEV1_GATEWAY_PRIMARY")
            .ok()
            .map(|v| v.trim().trim_end_matches('/').to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "http://127.0.0.1:9001".to_string());
        let backup_url = std::env::var("FORGE_FEV1_GATEWAY_BACKUP")
            .ok()
            .map(|v| v.trim().trim_end_matches('/').to_string())
            .filter(|v| !v.is_empty() && v != &primary_url);
        let timeout_ms = std::env::var("FORGE_FEV1_GATEWAY_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(500)
            .clamp(100, 10_000);
        let min_quote_usdc = std::env::var("FORGE_FEV1_MIN_QUOTE_USDC")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(1.0)
            .clamp(0.5, 1000.0);
        let entry_slippage_bps = std::env::var("FORGE_FEV1_ENTRY_SLIPPAGE_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(18.0)
            .clamp(0.0, 500.0);
        let exit_slippage_bps = std::env::var("FORGE_FEV1_EXIT_SLIPPAGE_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(22.0)
            .clamp(0.0, 500.0);
        let force_slippage_bps = std::env::var("FORGE_FEV1_FORCE_SLIPPAGE_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .map(|v| v.clamp(0.0, 500.0));
        let rust_submit_fallback_gateway = std::env::var("FORGE_FEV1_RUST_SUBMIT_FALLBACK_GATEWAY")
            .ok()
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(true);
        Self {
            primary_url,
            backup_url,
            timeout_ms,
            min_quote_usdc,
            entry_slippage_bps,
            exit_slippage_bps,
            force_slippage_bps,
            rust_submit_fallback_gateway,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(super) struct GatewayBookSnapshot {
    token_id: String,
    min_order_size: f64,
    tick_size: f64,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    best_bid_size: Option<f64>,
    best_ask_size: Option<f64>,
    bid_depth_top3: Option<f64>,
    ask_depth_top3: Option<f64>,
}

impl GatewayBookSnapshot {
    fn from_value(v: &Value) -> Option<Self> {
        let token_id = v
            .get("token_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if token_id.is_empty() {
            return None;
        }
        let min_order_size = v
            .get("min_order_size")
            .and_then(Value::as_f64)
            .unwrap_or(0.01)
            .max(0.0001);
        let tick_size = v
            .get("tick_size")
            .and_then(Value::as_f64)
            .unwrap_or(0.01)
            .max(0.0001);
        Some(Self {
            token_id,
            min_order_size,
            tick_size,
            best_bid: v.get("best_bid").and_then(Value::as_f64),
            best_ask: v.get("best_ask").and_then(Value::as_f64),
            best_bid_size: v.get("best_bid_size").and_then(Value::as_f64),
            best_ask_size: v.get("best_ask_size").and_then(Value::as_f64),
            bid_depth_top3: v.get("bid_depth_top3").and_then(Value::as_f64),
            ask_depth_top3: v.get("ask_depth_top3").and_then(Value::as_f64),
        })
    }
}

pub(super) fn round_to_tick(price: f64, tick_size: f64, is_buy: bool) -> f64 {
    let tick = tick_size.max(0.0001);
    let steps = price / tick;
    let aligned = if is_buy {
        steps.ceil() * tick
    } else {
        steps.floor() * tick
    };
    aligned.clamp(0.01, 0.99)
}

#[derive(Debug, Clone)]
pub(super) struct LiveMarketTarget {
    pub(super) market_id: String,
    pub(super) symbol: String,
    pub(super) timeframe: String,
    pub(super) token_id_yes: String,
    pub(super) token_id_no: String,
    pub(super) end_date: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct LiveGatedDecision {
    pub(super) decision: Value,
    pub(super) decision_key: String,
}

pub(super) fn live_decision_key(market_type: &str, decision: &Value) -> String {
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
    let round = decision
        .get("round_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let ts = decision
        .get("ts_ms")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    let price_cents = decision
        .get("price_cents")
        .and_then(Value::as_f64)
        .unwrap_or_default();
    format!(
        "{}:{}:{}:{}:{}:{:.4}",
        market_type, action, side, round, ts, price_cents
    )
}

pub(super) async fn gate_live_decisions(
    state: &ApiState,
    market_type: &str,
    decisions: &[Value],
    mark_attempts: bool,
) -> (Vec<LiveGatedDecision>, Vec<Value>, LivePositionState) {
    let now_ms = Utc::now().timestamp_millis();
    let mut position_state = state.get_live_position_state(market_type).await;
    let mut virtual_side = position_state.side.clone();
    let (mut has_enter_pending, mut has_exit_pending) =
        state.pending_flags_for_market(market_type).await;
    let mut accepted = Vec::<LiveGatedDecision>::new();
    let mut skipped = Vec::<Value>::new();

    for decision in decisions {
        let mut normalized = decision.clone();
        let mut action = normalized
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        let side = normalized
            .get("side")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_uppercase();
        let side_match = virtual_side
            .as_deref()
            .map(|s| s.eq_ignore_ascii_case(&side))
            .unwrap_or(false);
        if action == "enter" && virtual_side.is_some() && side_match {
            action = "add".to_string();
            if let Some(obj) = normalized.as_object_mut() {
                obj.insert("action".to_string(), Value::String(action.clone()));
            }
        }
        if !matches!(action.as_str(), "enter" | "add" | "reduce" | "exit") {
            skipped.push(json!({
                "reason": "invalid_action",
                "decision": normalized
            }));
            continue;
        }
        if side != "UP" && side != "DOWN" {
            skipped.push(json!({
                "reason": "invalid_side",
                "decision": normalized
            }));
            continue;
        }
        if matches!(action.as_str(), "enter" | "add") && (has_enter_pending || has_exit_pending) {
            skipped.push(json!({
                "reason": if has_enter_pending { "enter_pending_exists" } else { "exit_pending_exists" },
                "decision": normalized
            }));
            continue;
        }
        if matches!(action.as_str(), "exit" | "reduce") && has_exit_pending {
            skipped.push(json!({
                "reason": "exit_pending_exists",
                "decision": normalized
            }));
            continue;
        }
        if matches!(action.as_str(), "exit" | "reduce") && has_enter_pending {
            skipped.push(json!({
                "reason": "entry_pending_not_filled",
                "decision": normalized
            }));
            continue;
        }
        let decision_key = live_decision_key(market_type, &normalized);
        let should_submit = if action == "enter" {
            if virtual_side.is_none() {
                true
            } else {
                skipped.push(json!({
                    "reason": "already_in_position",
                    "state_side": virtual_side,
                    "decision": normalized
                }));
                false
            }
        } else if action == "add" {
            if side_match {
                true
            } else {
                skipped.push(json!({
                    "reason": if virtual_side.is_none() { "no_open_position_for_add" } else { "side_mismatch_for_add" },
                    "state_side": virtual_side,
                    "decision": normalized
                }));
                false
            }
        } else if side_match {
            true
        } else {
            let reason = if virtual_side.is_none() {
                "no_open_position"
            } else {
                "side_mismatch"
            };
            skipped.push(json!({
                "reason": reason,
                "state_side": virtual_side,
                "decision": normalized
            }));
            false
        };
        if !should_submit {
            continue;
        }
        if state
            .check_and_mark_live_decision(&decision_key, now_ms, mark_attempts)
            .await
        {
            skipped.push(json!({
                "reason": "duplicate_recent",
                "decision_key": decision_key,
                "decision": normalized
            }));
            continue;
        }
        if matches!(action.as_str(), "enter" | "add") {
            virtual_side = Some(side.clone());
            has_enter_pending = true;
        } else if action == "exit" {
            virtual_side = None;
            has_exit_pending = true;
        } else {
            has_exit_pending = true;
        }
        accepted.push(LiveGatedDecision {
            decision: normalized,
            decision_key,
        });
    }

    position_state.updated_ts_ms = now_ms;
    (accepted, skipped, position_state)
}

pub(super) fn select_live_decisions(
    decisions: &[Value],
    latest_ts_ms: i64,
    max_orders: usize,
    drain_only: bool,
    prefer_action: Option<&str>,
) -> Vec<Value> {
    fn is_replay_only_reason(decision: &Value) -> bool {
        decision
            .get("reason")
            .and_then(Value::as_str)
            .map(|reason| {
                reason
                    .trim()
                    .eq_ignore_ascii_case("end_of_samples_force_close")
            })
            .unwrap_or(false)
    }

    if decisions.is_empty() {
        return Vec::new();
    }
    let latest_round = decisions
        .iter()
        .rev()
        .find_map(|d| d.get("round_id").and_then(Value::as_str))
        .unwrap_or_default()
        .to_string();
    let mut selected: Vec<Value> = decisions
        .iter()
        .filter(|d| {
            let action = d
                .get("action")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_ascii_lowercase();
            let round_ok = d
                .get("round_id")
                .and_then(Value::as_str)
                .map(|r| r == latest_round)
                .unwrap_or(false);
            let ts_ok = d
                .get("ts_ms")
                .and_then(Value::as_i64)
                .map(|ts| ts >= latest_ts_ms.saturating_sub(90_000))
                .unwrap_or(false);
            let scope_ok = if drain_only { round_ok || ts_ok } else { ts_ok };
            let action_ok = if drain_only {
                action == "exit" || action == "reduce"
            } else {
                true
            };
            let reason_ok = !is_replay_only_reason(d);
            scope_ok && action_ok && reason_ok
        })
        .cloned()
        .collect();
    if selected.is_empty() {
        if let Some(last) = decisions.iter().rev().find(|d| {
            let action = d
                .get("action")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_ascii_lowercase();
            let action_ok = if drain_only {
                action == "exit" || action == "reduce"
            } else {
                d.get("ts_ms")
                    .and_then(Value::as_i64)
                    .map(|ts| ts >= latest_ts_ms.saturating_sub(90_000))
                    .unwrap_or(false)
            };
            action_ok && !is_replay_only_reason(d)
        }) {
            selected.push(last.clone());
        }
    }
    selected.sort_by_key(|v| v.get("ts_ms").and_then(Value::as_i64).unwrap_or(0));
    if selected.len() > max_orders {
        if max_orders <= 1 {
            let preferred = prefer_action
                .and_then(|want| {
                    selected.iter().rev().find(|v| {
                        v.get("action")
                            .and_then(Value::as_str)
                            .map(|a| a.eq_ignore_ascii_case(want))
                            .unwrap_or(false)
                    })
                })
                .cloned();
            if let Some(v) = preferred.or_else(|| selected.last().cloned()) {
                selected = vec![v];
            }
        } else {
            selected = selected[selected.len() - max_orders..].to_vec();
        }
    }
    selected
}

pub(super) async fn resolve_live_market_target(
    market_type: &str,
) -> Result<LiveMarketTarget, ApiError> {
    let discovery = MarketDiscovery::new(DiscoveryConfig {
        symbols: vec!["BTCUSDT".to_string()],
        market_types: vec!["updown".to_string()],
        timeframes: vec![market_type.to_string()],
        ..DiscoveryConfig::default()
    });
    let now_ms = Utc::now().timestamp_millis();
    let mut markets: Vec<MarketDescriptor> = discovery
        .discover()
        .await
        .map_err(|e| ApiError::internal(format!("market discovery failed: {e}")))?
        .into_iter()
        .filter(|m| {
            m.symbol.eq_ignore_ascii_case("BTCUSDT")
                && m.timeframe
                    .as_deref()
                    .map(|tf| tf.eq_ignore_ascii_case(market_type))
                    .unwrap_or(false)
                && m.token_id_yes.is_some()
                && m.token_id_no.is_some()
        })
        .collect();
    if markets.is_empty() {
        return Err(ApiError::bad_request(format!(
            "no active BTC {market_type} market with token ids"
        )));
    }
    markets.sort_by_key(|m| {
        let end_ms = parse_end_date_ms(m.end_date.as_deref()).unwrap_or(i64::MAX / 4);
        let ended_penalty = if end_ms < now_ms { 1_i64 } else { 0_i64 };
        let distance = end_ms.saturating_sub(now_ms).abs();
        (ended_penalty, distance)
    });
    let best = markets.remove(0);
    Ok(LiveMarketTarget {
        market_id: best.market_id,
        symbol: best.symbol,
        timeframe: best.timeframe.unwrap_or_else(|| market_type.to_string()),
        token_id_yes: best.token_id_yes.unwrap_or_default(),
        token_id_no: best.token_id_no.unwrap_or_default(),
        end_date: best.end_date,
    })
}

pub(super) fn decision_to_live_payload(
    decision: &Value,
    target: &LiveMarketTarget,
    gateway_cfg: &LiveGatewayConfig,
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
        ("enter", "UP") | ("add", "UP") => (
            "buy_yes",
            target.token_id_yes.as_str(),
            gateway_cfg.entry_slippage_bps,
        ),
        ("exit", "UP") | ("reduce", "UP") => (
            "sell_yes",
            target.token_id_yes.as_str(),
            gateway_cfg.exit_slippage_bps,
        ),
        ("enter", "DOWN") | ("add", "DOWN") => (
            "buy_no",
            target.token_id_no.as_str(),
            gateway_cfg.entry_slippage_bps,
        ),
        ("exit", "DOWN") | ("reduce", "DOWN") => (
            "sell_no",
            target.token_id_no.as_str(),
            gateway_cfg.exit_slippage_bps,
        ),
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
    let mut quote_size = quote_size_override.unwrap_or_else(|| {
        decision
            .get("quote_size_usdc")
            .and_then(Value::as_f64)
            .unwrap_or(gateway_cfg.min_quote_usdc)
    });
    quote_size = quote_size.max(gateway_cfg.min_quote_usdc);
    let mut size = (quote_size / price).max(0.01);
    let mut notes: Vec<String> = Vec::with_capacity(2);
    let is_buy = gateway_side.starts_with("buy_");
    let mut min_order_size = 0.01_f64;
    if let Some(book) = book {
        min_order_size = book.min_order_size.max(0.0001);
        let tick_size = book.tick_size.max(0.0001);
        price = round_to_tick(price, tick_size, is_buy);
        size = (quote_size / price).max(min_order_size);

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
        if depth_top1.is_finite() && depth_top1 > 0.0 && size > depth_top1 * 1.12 {
            let capped = (depth_top1 * 0.95).max(min_order_size);
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
    size = size.max(min_order_size).max(0.01);
    size = (size * 10_000.0).round() / 10_000.0;

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
    if let Some(force) = gateway_cfg.force_slippage_bps {
        slippage_bps = force.clamp(0.0, 500.0);
    }
    let cache_key = format!(
        "fev1:{}:{}:{}:{:.4}:{:.4}",
        target.market_id, gateway_side, action, price, size
    );
    Some(json!({
        "market_id": target.market_id,
        "token_id": token_id,
        "side": gateway_side,
        "price": price,
        "size": size,
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
    }))
}

pub(super) async fn post_gateway(
    client: &reqwest::Client,
    timeout_ms: u64,
    endpoint: &str,
    path: &str,
    payload: &Value,
) -> Result<Value, String> {
    let url = format!(
        "{}/{}",
        endpoint.trim_end_matches('/'),
        path.trim_start_matches('/')
    );
    let resp = client
        .post(&url)
        .timeout(Duration::from_millis(timeout_ms.clamp(100, 20_000)))
        .header("content-type", "application/json")
        .json(payload)
        .send()
        .await
        .map_err(|e| format!("http_error:{e}"))?;
    let status = resp.status();
    let body = resp.text().await.map_err(|e| format!("read_error:{e}"))?;
    let parsed = serde_json::from_str::<Value>(&body).unwrap_or_else(|_| json!({"raw": body}));
    if status.is_success() {
        Ok(parsed)
    } else {
        Err(format!("status_{}:{}", status.as_u16(), parsed))
    }
}

pub(super) async fn get_gateway(
    client: &reqwest::Client,
    timeout_ms: u64,
    endpoint: &str,
    path: &str,
    query: &[(&str, &str)],
) -> Result<Value, String> {
    let url = format!(
        "{}/{}",
        endpoint.trim_end_matches('/'),
        path.trim_start_matches('/')
    );
    let resp = client
        .get(&url)
        .timeout(Duration::from_millis(timeout_ms.clamp(100, 20_000)))
        .query(query)
        .send()
        .await
        .map_err(|e| format!("http_error:{e}"))?;
    let status = resp.status();
    let body = resp.text().await.map_err(|e| format!("read_error:{e}"))?;
    let parsed = serde_json::from_str::<Value>(&body).unwrap_or_else(|_| json!({"raw": body}));
    if status.is_success() {
        Ok(parsed)
    } else {
        Err(format!("status_{}:{}", status.as_u16(), parsed))
    }
}

pub(super) async fn fetch_gateway_book_snapshot(
    client: &reqwest::Client,
    gateway_cfg: &LiveGatewayConfig,
    token_id: &str,
) -> Option<GatewayBookSnapshot> {
    let mut endpoint = gateway_cfg.primary_url.clone();
    let mut resp = get_gateway(
        client,
        gateway_cfg.timeout_ms,
        &endpoint,
        "/book",
        &[("token_id", token_id)],
    )
    .await;
    if resp.is_err() {
        if let Some(backup) = gateway_cfg.backup_url.as_deref() {
            endpoint = backup.to_string();
            resp = get_gateway(
                client,
                gateway_cfg.timeout_ms,
                &endpoint,
                "/book",
                &[("token_id", token_id)],
            )
            .await;
        }
    }
    match resp {
        Ok(v) => GatewayBookSnapshot::from_value(&v).or_else(|| {
            tracing::warn!(
                token_id = token_id,
                endpoint = %endpoint,
                "gateway /book response missing required fields"
            );
            None
        }),
        Err(err) => {
            tracing::warn!(token_id = token_id, endpoint = %endpoint, error = %err, "gateway /book fetch failed");
            None
        }
    }
}

pub(super) async fn submit_gateway_order(
    client: &reqwest::Client,
    gateway_cfg: &LiveGatewayConfig,
    payload: &Value,
) -> (Result<Value, String>, String) {
    let mut used_endpoint = gateway_cfg.primary_url.clone();
    let _ = post_gateway(
        client,
        gateway_cfg.timeout_ms,
        &gateway_cfg.primary_url,
        "/prebuild_order",
        payload,
    )
    .await;
    let mut result = post_gateway(
        client,
        gateway_cfg.timeout_ms,
        &gateway_cfg.primary_url,
        "/orders",
        payload,
    )
    .await;
    if result.is_err() {
        if let Some(backup) = gateway_cfg.backup_url.as_deref() {
            used_endpoint = backup.to_string();
            let _ = post_gateway(
                client,
                gateway_cfg.timeout_ms,
                backup,
                "/prebuild_order",
                payload,
            )
            .await;
            result = post_gateway(client, gateway_cfg.timeout_ms, backup, "/orders", payload).await;
        }
    }
    (result, used_endpoint)
}

pub(super) fn extract_gateway_reject_reason(payload: &Value) -> String {
    payload
        .get("reject_code")
        .and_then(Value::as_str)
        .or_else(|| payload.get("error").and_then(Value::as_str))
        .or_else(|| payload.get("reason").and_then(Value::as_str))
        .unwrap_or("unknown_reject")
        .to_string()
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
    r.contains("no orders found to match")
        || r.contains("insufficient liquidity")
        || r.contains("cannot be matched")
        || r.contains("would not fill")
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
    let is_exit_like = action == "exit" || action == "reduce";
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

    if attempt == 0 {
        let boosted_slippage = if is_exit_like {
            (current_slippage + 16.0).min(120.0)
        } else {
            (current_slippage + 12.0).min(90.0)
        };
        if let Some(obj) = next.as_object_mut() {
            obj.insert("max_slippage_bps".to_string(), json!(boosted_slippage));
            obj.insert("ttl_ms".to_string(), json!((current_ttl + 400).min(5_000)));
            obj.insert("retry_tag".to_string(), json!("slippage_retry"));
        }
        return Some(next);
    }

    if maker_mode {
        if let Some(obj) = next.as_object_mut() {
            obj.insert("tif".to_string(), json!("FAK"));
            obj.insert("style".to_string(), json!("taker"));
            obj.insert(
                "ttl_ms".to_string(),
                json!((current_ttl / 2).clamp(600, 2_000)),
            );
            obj.insert(
                "max_slippage_bps".to_string(),
                json!((current_slippage + if is_exit_like { 20.0 } else { 14.0 }).min(140.0)),
            );
            obj.insert("retry_tag".to_string(), json!("maker_timeout_to_fak"));
        }
        return Some(next);
    }

    if action == "enter" || action == "add" {
        if let Some(obj) = next.as_object_mut() {
            obj.insert("tif".to_string(), json!("GTD"));
            obj.insert("style".to_string(), json!("maker"));
            obj.insert(
                "ttl_ms".to_string(),
                json!((current_ttl + 700).clamp(800, 6_000)),
            );
            obj.insert(
                "max_slippage_bps".to_string(),
                json!((current_slippage + 6.0).min(90.0)),
            );
            obj.insert("retry_tag".to_string(), json!("maker_fallback"));
        }
        return Some(next);
    }

    None
}

pub(super) async fn get_gateway_reports(
    client: &reqwest::Client,
    timeout_ms: u64,
    endpoint: &str,
    since_seq: i64,
    limit: usize,
) -> Result<Value, String> {
    let url = format!(
        "{}/reports/orders?since_seq={}&limit={}",
        endpoint.trim_end_matches('/'),
        since_seq.max(0),
        limit.clamp(1, 1000)
    );
    let resp = client
        .get(&url)
        .timeout(Duration::from_millis(timeout_ms.clamp(100, 20_000)))
        .send()
        .await
        .map_err(|e| format!("http_error:{e}"))?;
    let status = resp.status();
    let body = resp.text().await.map_err(|e| format!("read_error:{e}"))?;
    let parsed = serde_json::from_str::<Value>(&body).unwrap_or_else(|_| json!({"raw": body}));
    if status.is_success() {
        Ok(parsed)
    } else {
        Err(format!("status_{}:{}", status.as_u16(), parsed))
    }
}

pub(super) async fn delete_gateway_order(
    client: &reqwest::Client,
    timeout_ms: u64,
    endpoint: &str,
    order_id: &str,
) -> Result<Value, String> {
    let url = format!(
        "{}/orders/{}",
        endpoint.trim_end_matches('/'),
        order_id.trim()
    );
    let resp = client
        .delete(&url)
        .timeout(Duration::from_millis(timeout_ms.clamp(100, 20_000)))
        .send()
        .await
        .map_err(|e| format!("http_error:{e}"))?;
    let status = resp.status();
    let body = resp.text().await.map_err(|e| format!("read_error:{e}"))?;
    let parsed = serde_json::from_str::<Value>(&body).unwrap_or_else(|_| json!({"raw": body}));
    if status.is_success() {
        Ok(parsed)
    } else {
        Err(format!("status_{}:{}", status.as_u16(), parsed))
    }
}

pub(super) fn extract_order_id_from_gateway_record(row: &Value) -> Option<String> {
    row.get("response")
        .and_then(|v| v.get("order_id").and_then(Value::as_str))
        .map(str::to_string)
        .or_else(|| {
            row.get("response")
                .and_then(|v| v.get("id").and_then(Value::as_str))
                .map(str::to_string)
        })
        .or_else(|| {
            row.get("order_id")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
}

#[derive(Debug, Clone, Default)]
struct PendingFillMeta {
    fill_price_cents: Option<f64>,
    fill_quote_usdc: Option<f64>,
    fee_usdc: f64,
    slippage_usdc: f64,
}

fn value_as_f64(v: &Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
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
        // Some gateways return 6-decimal scaled collateral integers.
        (v / 1_000_000.0).max(0.0)
    } else {
        v
    }
}

fn extract_pending_fill_meta(fill_event: Option<&Value>, pending: &LivePendingOrder) -> PendingFillMeta {
    let Some(fill) = fill_event else {
        return PendingFillMeta::default();
    };
    let price_cents_direct = collect_candidates(
        fill,
        &["price_cents", "fill_price_cents", "filled_price_cents", "avg_price_cents"],
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
    let fill_price_cents = price_cents_direct.or_else(|| {
        price_prob.map(|v| if v <= 1.5 { v * 100.0 } else { v })
    });

    let quote_direct = collect_candidates(
        fill,
        &[
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
        &["filled_size", "executed_size", "matched_size", "accepted_size", "size"],
    );
    let quote_from_size = if let (Some(px_c), Some(sz)) = (
        fill_price_cents,
        size_candidates.into_iter().find(|v| *v > 0.0),
    ) {
        Some((px_c / 100.0) * sz)
    } else {
        None
    };
    let fill_quote_usdc = quote_direct.or(quote_from_size);

    let fee_usdc = collect_candidates(fill, &["fee_usdc", "total_fee_usdc", "fee", "fees", "total_fee"])
        .into_iter()
        .map(normalize_usdc_amount)
        .fold(0.0, |acc, v| acc + v);

    let explicit_slippage_usdc = collect_candidates(fill, &["slippage_usdc"])
        .into_iter()
        .map(normalize_usdc_amount)
        .fold(0.0, |acc, v| acc + v);

    let inferred_slippage_usdc = if let (Some(px_c), Some(quote)) = (fill_price_cents, fill_quote_usdc) {
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
        fee_usdc,
        slippage_usdc: explicit_slippage_usdc.max(inferred_slippage_usdc),
    }
}

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
    let fill_price_cents = fill_meta.fill_price_cents.unwrap_or(pending.price_cents).max(0.01);
    let fill_quote_usdc = fill_meta
        .fill_quote_usdc
        .unwrap_or(pending.quote_size_usdc.max(0.0))
        .max(0.0);
    let fill_cost_usdc = (fill_meta.fee_usdc + fill_meta.slippage_usdc).max(0.0);
    let mut realized_pnl_usdc = 0.0_f64;
    if action == "enter" {
        ps.state = "in_position".to_string();
        ps.side = Some(pending.side.clone());
        ps.entry_round_id = Some(pending.round_id.clone());
        ps.entry_ts_ms = Some(pending.submitted_ts_ms);
        ps.entry_price_cents = Some(fill_price_cents);
        ps.entry_quote_usdc = Some(fill_quote_usdc);
        ps.net_quote_usdc = fill_quote_usdc;
        ps.vwap_entry_cents = Some(fill_price_cents);
        ps.open_add_layers = 0;
        ps.position_cost_usdc = fill_cost_usdc;
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
        ps.entry_ts_ms = Some(pending.submitted_ts_ms);
        ps.entry_price_cents = Some(next_price);
        ps.vwap_entry_cents = Some(next_price);
        ps.entry_quote_usdc = Some(next_quote);
        ps.net_quote_usdc = next_quote;
        ps.total_adds = ps.total_adds.saturating_add(1);
        ps.open_add_layers = ps.open_add_layers.saturating_add(1);
        ps.position_cost_usdc += fill_cost_usdc;
        ps.last_fill_pnl_usdc = 0.0;
    } else if action == "reduce" || action == "exit" {
        let entry_price_cents = ps
            .vwap_entry_cents
            .or(ps.entry_price_cents)
            .unwrap_or(0.0)
            .max(0.01);
        let prev_quote = ps
            .net_quote_usdc
            .max(ps.entry_quote_usdc.unwrap_or(0.0))
            .max(0.0);
        let close_quote = if action == "exit" {
            prev_quote
        } else {
            fill_quote_usdc.max(0.0).min(prev_quote)
        };
        if close_quote > 1e-9 {
            let shares = close_quote / (entry_price_cents / 100.0).max(0.0001);
            let gross_pnl_usdc = shares * ((fill_price_cents - entry_price_cents) / 100.0);
            let entry_cost_alloc = if prev_quote > 1e-9 {
                ps.position_cost_usdc * (close_quote / prev_quote).clamp(0.0, 1.0)
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
        let remaining_quote = (prev_quote - close_quote).max(0.0);
        if action == "exit" || remaining_quote <= 1e-6 {
            ps.state = "flat".to_string();
            ps.side = None;
            ps.entry_round_id = None;
            ps.entry_ts_ms = None;
            ps.entry_price_cents = None;
            ps.entry_quote_usdc = None;
            ps.net_quote_usdc = 0.0;
            ps.vwap_entry_cents = None;
            ps.open_add_layers = 0;
            ps.position_cost_usdc = 0.0;
            ps.total_exits = ps.total_exits.saturating_add(1);
        } else {
            ps.state = "in_position".to_string();
            ps.entry_quote_usdc = Some(remaining_quote);
            ps.net_quote_usdc = remaining_quote;
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
                        "reason": "timeout_cancel_failed_requeued",
                        "order_id": retry.order_id,
                        "gateway_endpoint": endpoint
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
        if elapsed < row.cancel_after_ms.max(500) {
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
                if maker_tif && row.retry_count < 1 {
                    let maybe_target = resolve_live_market_target(&row.market_type).await.ok();
                    if let Some(target) = maybe_target {
                        let decision = json!({
                            "action": row.action,
                            "side": row.side,
                            "round_id": row.round_id,
                            "price_cents": row.price_cents,
                            "quote_size_usdc": row.quote_size_usdc,
                            "reason": format!("{}_timeout_fallback_fak", row.submit_reason),
                            "tif": "FAK",
                            "style": "taker",
                            "ttl_ms": 900,
                            "max_slippage_bps": gateway_cfg.exit_slippage_bps.max(gateway_cfg.entry_slippage_bps) + 10.0
                        });
                        let token_id =
                            token_id_for_decision(&decision, &target).map(str::to_string);
                        let book_snapshot = if let Some(token_id) = token_id {
                            fetch_gateway_book_snapshot(client, gateway_cfg, &token_id).await
                        } else {
                            None
                        };
                        if let Some(payload) = decision_to_live_payload(
                            &decision,
                            &target,
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
                                        pending.cancel_after_ms = 1500;
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
                                                    "book_snapshot": book_snapshot
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
                state
                    .append_live_event(
                        &row.market_type,
                        json!({
                            "accepted": false,
                            "action": row.action,
                            "side": row.side,
                            "round_id": row.round_id,
                            "reason": "local_timeout_cancel_failed",
                            "order_id": row.order_id,
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

pub(super) async fn execute_live_orders_via_gateway(
    state: &ApiState,
    gateway_cfg: &LiveGatewayConfig,
    target: &LiveMarketTarget,
    position_state: &LivePositionState,
    decisions: &[LiveGatedDecision],
) -> Vec<Value> {
    let client = state.gateway_http_client.as_ref();
    let mut out = Vec::<Value>::with_capacity(decisions.len());
    let mut book_cache: HashMap<String, Option<GatewayBookSnapshot>> =
        HashMap::with_capacity(decisions.len());
    let exit_quote_override =
        position_state
            .entry_quote_usdc
            .and_then(|v| if v > 0.0 { Some(v) } else { None });
    for gated in decisions {
        let mut decision = gated.decision.clone();
        let action = decision
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        if action == "exit" {
            if let Some(q) = exit_quote_override {
                if let Some(obj) = decision.as_object_mut() {
                    obj.insert("quote_size_usdc".to_string(), json!(q));
                }
            }
        }
        let token_id = token_id_for_decision(&decision, target).map(str::to_string);
        let book_snapshot = if let Some(token_id) = token_id.clone() {
            if let Some(cached) = book_cache.get(&token_id) {
                cached.clone()
            } else {
                let fetched = fetch_gateway_book_snapshot(client, gateway_cfg, &token_id).await;
                book_cache.insert(token_id.clone(), fetched.clone());
                fetched
            }
        } else {
            None
        };
        let Some(payload) =
            decision_to_live_payload(&decision, target, gateway_cfg, book_snapshot.as_ref(), None)
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
            "book_snapshot": book_snapshot
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

    let price_dec =
        PmDecimal::from_str(&format!("{price:.6}")).map_err(|e| format!("bad price: {e}"))?;
    let size_dec =
        PmDecimal::from_str(&format!("{size:.2}")).map_err(|e| format!("bad size: {e}"))?;
    let order_type = pm_order_type_from_tif(&tif);

    let mut builder = ctx
        .client
        .limit_order()
        .token_id(token)
        .side(pm_side)
        .price(price_dec)
        .size(size_dec)
        .order_type(order_type.clone());
    let post_only = style == "maker" && matches!(order_type, PmOrderType::GTC | PmOrderType::GTD);
    builder = builder.post_only(post_only);
    if matches!(order_type, PmOrderType::GTD) {
        let expiration = Utc::now() + chrono::Duration::milliseconds(ttl_ms);
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
    let resp = ctx
        .client
        .post_order(signed)
        .await
        .map_err(|e| format!("post_order failed: {e}"))?;
    let accepted = resp.success && resp.error_msg.as_deref().unwrap_or_default().is_empty();
    Ok(json!({
        "accepted": accepted,
        "order_id": resp.order_id,
        "status": format!("{}", resp.status),
        "error_msg": resp.error_msg,
        "accepted_size": size,
        "effective_notional": size * price,
        "making_amount": pm_dec_to_f64(&resp.making_amount),
        "taking_amount": pm_dec_to_f64(&resp.taking_amount),
        "trade_ids": resp.trade_ids,
        "transaction_hashes": resp.transaction_hashes
    }))
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
    let mut book_cache: HashMap<String, Option<GatewayBookSnapshot>> =
        HashMap::with_capacity(decisions.len());
    let fallback_client = state.gateway_http_client.as_ref();
    let exit_quote_override =
        position_state
            .entry_quote_usdc
            .and_then(|v| if v > 0.0 { Some(v) } else { None });

    for gated in decisions {
        let mut decision = gated.decision.clone();
        if decision
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .eq_ignore_ascii_case("exit")
        {
            if let Some(q) = exit_quote_override {
                if let Some(obj) = decision.as_object_mut() {
                    obj.insert("quote_size_usdc".to_string(), json!(q));
                }
            }
        }
        let token_id = token_id_for_decision(&decision, target).map(str::to_string);
        let book_snapshot = if let Some(token_id) = token_id.clone() {
            if let Some(cached) = book_cache.get(&token_id) {
                cached.clone()
            } else {
                let fetched = if let Some(cached) = state.get_rust_book_cache(&token_id).await {
                    Some(cached)
                } else if let Some(v) = fetch_rust_book_snapshot(&ctx, &token_id).await {
                    state.put_rust_book_cache(&token_id, v.clone()).await;
                    Some(v)
                } else {
                    tracing::warn!(token_id = %token_id, "rust_sdk /book failed, fallback gateway /book");
                    let fetched =
                        fetch_gateway_book_snapshot(fallback_client, gateway_cfg, &token_id).await;
                    if let Some(v) = fetched.as_ref() {
                        state.put_rust_book_cache(&token_id, v.clone()).await;
                    }
                    fetched
                };
                book_cache.insert(token_id.clone(), fetched.clone());
                fetched
            }
        } else {
            None
        };
        let Some(payload) =
            decision_to_live_payload(&decision, target, gateway_cfg, book_snapshot.as_ref(), None)
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
            "book_snapshot": book_snapshot
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
            apply_pending_confirmation(state, &row, "rust_order_terminal_filled", None).await;
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
                        "status": format!("{}", order.status)
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
        if now_ms.saturating_sub(row.submitted_ts_ms) < row.cancel_after_ms.max(500) {
            continue;
        }
        match ctx.client.cancel_order(&row.order_id).await {
            Ok(resp) => {
                let _ = state.remove_pending_order(&row.order_id).await;
                let maker_tif = matches!(
                    row.tif.to_ascii_uppercase().as_str(),
                    "GTD" | "GTC" | "POST_ONLY"
                );
                if maker_tif && row.retry_count < 1 {
                    let maybe_target = resolve_live_market_target(&row.market_type).await.ok();
                    if let Some(target) = maybe_target {
                        let decision = json!({
                            "action": row.action,
                            "side": row.side,
                            "round_id": row.round_id,
                            "price_cents": row.price_cents,
                            "quote_size_usdc": row.quote_size_usdc,
                            "reason": format!("{}_timeout_fallback_fak", row.submit_reason),
                            "tif": "FAK",
                            "style": "taker",
                            "ttl_ms": 900
                        });
                        let token_id =
                            token_id_for_decision(&decision, &target).map(str::to_string);
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
                            &target,
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
                                        pending.cancel_after_ms = 1500;
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
                                                    "book_snapshot": book_snapshot
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
                state
                    .append_live_event(
                        &row.market_type,
                        json!({
                            "accepted": false,
                            "action": row.action,
                            "side": row.side,
                            "round_id": row.round_id,
                            "reason": "rust_timeout_cancel_failed",
                            "order_id": row.order_id,
                            "error": err.to_string()
                        }),
                    )
                    .await;
            }
        }
    }
}

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

pub(super) async fn execute_live_orders(
    state: &ApiState,
    gateway_cfg: &LiveGatewayConfig,
    mode: LiveExecutorMode,
    target: &LiveMarketTarget,
    position_state: &LivePositionState,
    decisions: &[LiveGatedDecision],
) -> Vec<Value> {
    match mode {
        LiveExecutorMode::Gateway => {
            execute_live_orders_via_gateway(state, gateway_cfg, target, position_state, decisions)
                .await
        }
        LiveExecutorMode::RustSdk => {
            execute_live_orders_via_rust_sdk(state, gateway_cfg, target, position_state, decisions)
                .await
        }
    }
}
