fn decision_round_matches_target(decision: &Value, target: &LiveMarketTarget) -> bool {
    let action = decision
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let is_entry_like = action == "enter";
    let round_id = decision
        .get("round_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim();
    if round_id.is_empty() {
        return !is_entry_like;
    }
    let Some(decision_end_ms) = parse_round_end_ts_ms(round_id) else {
        return !is_entry_like;
    };
    let Some(target_end_ms) = parse_end_date_ms(target.end_date.as_deref()) else {
        return !is_entry_like;
    };
    decision_end_ms
        .saturating_sub(target_end_ms)
        .abs()
        <= live_round_target_match_tolerance_ms()
}

fn is_entry_action(action: &str) -> bool {
    let a = action.to_ascii_lowercase();
    a == "enter"
}

fn pending_cancel_due_ms(row: &LivePendingOrder) -> i64 {
    let base = row.cancel_after_ms.max(500);
    let maker_tif = matches!(
        row.tif.to_ascii_uppercase().as_str(),
        "GTD" | "GTC" | "POST_ONLY"
    );
    let due_delta = if maker_tif && is_entry_action(&row.action) {
        base.min(live_entry_maker_max_wait_ms())
    } else {
        base
    };
    if row.cancel_due_at_ms > 0 {
        row.cancel_due_at_ms
    } else {
        row.ack_ts_ms
            .max(row.submitted_ts_ms)
            .saturating_add(due_delta)
    }
}

fn pending_terminal_due_ms(row: &LivePendingOrder) -> i64 {
    if row.terminal_due_at_ms > 0 {
        row.terminal_due_at_ms
    } else {
        row.ack_ts_ms
            .max(row.submitted_ts_ms)
            .saturating_add(if is_live_exit_action(&row.action) { 5_000 } else { 4_000 })
    }
}

fn end_date_iso_from_ms(end_ms: i64) -> Option<String> {
    DateTime::<Utc>::from_timestamp_millis(end_ms).map(|dt| dt.to_rfc3339())
}

async fn resolve_token_ids_from_target_cache(
    market_id: &str,
    market_type: &str,
) -> Option<(String, String)> {
    {
        let cache = live_market_token_cache().read().await;
        if let Some(v) = cache.get(market_id) {
            return Some(v.clone());
        }
    }
    let cache_path = live_target_cache_file_path();
    if let Ok(raw) = tokio::fs::read_to_string(&cache_path).await {
        if let Ok(root) = serde_json::from_str::<Value>(&raw) {
            if let Some(obj) = root.as_object() {
                for (_, bucket) in obj {
                    let Some(market_obj) = bucket
                        .as_object()
                        .and_then(|m| m.get(market_id))
                        .and_then(Value::as_object)
                    else {
                        continue;
                    };
                    let tf_ok = market_obj
                        .get("timeframe")
                        .and_then(Value::as_str)
                        .map(|tf| tf.eq_ignore_ascii_case(market_type))
                        .unwrap_or(false);
                    if !tf_ok {
                        continue;
                    }
                    let yes = market_obj
                        .get("yes_token")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .trim();
                    let no = market_obj
                        .get("no_token")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .trim();
                    if yes.is_empty() || no.is_empty() {
                        continue;
                    }
                    let pair = (yes.to_string(), no.to_string());
                    let mut cache = live_market_token_cache().write().await;
                    cache.insert(market_id.to_string(), pair.clone());
                    return Some(pair);
                }
            }
        }
    }
    let from_gamma = resolve_token_ids_from_gamma_market_detail(market_id).await;
    if from_gamma.is_some() {
        tracing::warn!(
            market_id = market_id,
            market_type = market_type,
            "resolved token ids via gamma market detail fallback"
        );
    }
    from_gamma
}

async fn resolve_live_target_from_snapshot(
    state: &ApiState,
    symbol: &str,
    market_type: &str,
    now_ms: i64,
) -> Option<LiveMarketTarget> {
    async fn try_snapshot_candidate(
        _state: &ApiState,
        symbol: &str,
        market_type: &str,
        now_ms: i64,
        snapshot: &Value,
        source: &str,
    ) -> Option<LiveMarketTarget> {
        let market_id = snapshot
            .get("market_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())?
            .to_string();
        let round_id = snapshot
            .get("round_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let sample_ms = snapshot
            .get("ts_ireland_sample_ms")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let age_ms = now_ms.saturating_sub(sample_ms);
        let fresh_max_age_ms = live_market_target_snapshot_max_age_ms();
        let stale_max_age_ms = live_market_target_snapshot_stale_max_age_ms();
        if age_ms > stale_max_age_ms {
            return None;
        }
        let end_ms = parse_round_end_ts_ms(round_id).or_else(|| {
            let remain = snapshot.get("remaining_ms").and_then(Value::as_i64)?;
            Some(sample_ms.saturating_add(remain.max(0)))
        })?;
        if end_ms < now_ms {
            return None;
        }
        let (token_id_yes, token_id_no) =
            resolve_token_ids_from_target_cache(&market_id, market_type).await?;
        if age_ms > fresh_max_age_ms {
            tracing::warn!(
                market_type = market_type,
                market_id = market_id,
                source = source,
                age_ms = age_ms,
                fresh_max_age_ms = fresh_max_age_ms,
                stale_max_age_ms = stale_max_age_ms,
                end_ms = end_ms,
                "using stale-but-active snapshot fallback for live market target"
            );
        }
        Some(LiveMarketTarget {
            market_id,
            symbol: symbol.to_ascii_uppercase(),
            timeframe: market_type.to_string(),
            token_id_yes,
            token_id_no,
            end_date: end_date_iso_from_ms(end_ms),
        })
    }

    let symbol_key = format!(
        "{}:snapshot:latest:{}:{market_type}",
        state.redis_prefix,
        symbol.trim().to_ascii_uppercase()
    );
    if let Ok(snapshot_json) = read_key_json(state, &symbol_key).await {
        let snapshot = snapshot_json.0;
        if let Some(target) =
            try_snapshot_candidate(
                state,
                symbol,
                market_type,
                now_ms,
                &snapshot,
                "symbol_snapshot",
            )
            .await
        {
            return Some(target);
        }
    }

    let tf_key = format!("{}:snapshot:latest:tf:{market_type}", state.redis_prefix);
    if let Ok(tf_snapshot_json) = read_key_json(state, &tf_key).await {
        let tf_snapshot = tf_snapshot_json.0;
        let mut symbol_rows: Vec<&Value> = match tf_snapshot.as_array() {
            Some(rows) => rows
                .iter()
                .filter(|row| {
                    row.get("symbol")
                        .and_then(Value::as_str)
                        .map(|s| s.eq_ignore_ascii_case(symbol))
                        .unwrap_or(false)
                })
                .collect(),
            None => Vec::new(),
        };
        symbol_rows.sort_by_key(|row| {
            std::cmp::Reverse(
                row.get("ts_ireland_sample_ms")
                    .and_then(Value::as_i64)
                    .unwrap_or(0),
            )
        });
        for row in symbol_rows {
            if let Some(target) =
                try_snapshot_candidate(state, symbol, market_type, now_ms, row, "tf_snapshot")
                    .await
            {
                return Some(target);
            }
        }
    }

    let pattern = format!(
        "{}:snapshot:latest:{}:{}:*",
        state.redis_prefix,
        symbol.trim().to_ascii_uppercase(),
        market_type
    );
    let mut history_keys = Vec::<String>::new();
    if let Some(mut conn) = state.redis_manager.clone() {
        let mut cursor: u64 = 0;
        loop {
            let scanned: redis::RedisResult<(u64, Vec<String>)> = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(96)
                .query_async(&mut conn)
                .await;
            let Ok((next, mut keys)) = scanned else {
                break;
            };
            history_keys.append(&mut keys);
            if next == 0 || history_keys.len() >= 320 {
                break;
            }
            cursor = next;
        }
    }
    if !history_keys.is_empty() {
        let mut best: Option<(i64, LiveMarketTarget)> = None;
        for key in history_keys {
            let Ok(snapshot_json) = read_key_json(state, &key).await else {
                continue;
            };
            let snapshot = snapshot_json.0;
            let Some(target) = try_snapshot_candidate(
                state,
                symbol,
                market_type,
                now_ms,
                &snapshot,
                "symbol_history_scan",
            )
            .await
            else {
                continue;
            };
            let sample_ms = snapshot
                .get("ts_ireland_sample_ms")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            match &best {
                Some((best_ts, _)) if *best_ts >= sample_ms => {}
                _ => best = Some((sample_ms, target)),
            }
        }
        if let Some((_, target)) = best {
            tracing::warn!(
                market_type = market_type,
                "market target resolved from symbol history scan fallback"
            );
            return Some(target);
        }
    }
    None
}

pub(super) fn live_decision_key(market_type: &str, decision: &Value) -> String {
    // NEW: Prefer intent_id if available (from ActiveIntent)
    if let Some(intent_id) = decision.get("intent_id").and_then(Value::as_str) {
        return intent_id.to_string();
    }
    
    // Fallback to legacy key generation
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
    symbol: &str,
    market_type: &str,
    decisions: &[Value],
    mark_attempts: bool,
) -> (Vec<LiveGatedDecision>, Vec<Value>, LivePositionState) {
    let now_ms = Utc::now().timestamp_millis();
    let max_open_positions = live_max_open_positions();
    let max_completed_trades = live_max_completed_trades_for_scope(symbol, market_type);
    let require_fixed_entry_size = live_require_fixed_entry_size();
    
    // OPTIMIZATION: Parallelize 4 state queries to reduce latency (~50-150ms savings)
    // Run all 4 queries concurrently instead of sequentially
    let (
        open_positions_total,
        enter_pending_total,
        position_state,
        pending_flags,
    ) = tokio::join!(
        state.count_open_positions(),
        state.count_entry_pending_orders(),
        state.get_live_position_state(symbol, market_type),
        state.pending_flags_for_market(symbol, market_type)
    );
    let mut enter_pending_total = enter_pending_total;
    let mut position_state = position_state;
    let (mut has_enter_pending, mut has_exit_pending) = pending_flags;
    let mut virtual_side = position_state.side.clone();
    let mut accepted = Vec::<LiveGatedDecision>::new();
    let mut skipped = Vec::<Value>::new();

    for decision in decisions {
        let mut normalized = decision.clone();
        
        let action = normalized
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
        
        // REMOVED: enter→add automatic rewrite
        // Per codex design, targeting should ONLY reject, not rewrite signals
        // ActiveIntent only supports Enter/Exit, no Add/Reduce at strategy layer
        // If user wants Add support, it must be added at strategy layer, not execution layer
        
        if !matches!(action.as_str(), "enter" | "exit") {
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
        if action == "enter"
            && max_completed_trades > 0
            && (position_state.total_exits as usize) >= max_completed_trades
        {
            skipped.push(json!({
                "reason": "completed_trade_limit_reached",
                "completed_trades": position_state.total_exits,
                "max_completed_trades": max_completed_trades,
                "decision": normalized
            }));
            continue;
        }
        if action == "enter" {
            let aggr = state
                .get_live_execution_aggr_state(symbol, market_type)
                .await;
            let reject_limit = live_entry_liquidity_reject_limit();
            let reject_window_ms = live_entry_liquidity_reject_window_ms();
            let decision_round_id = normalized
                .get("round_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let same_round = aggr
                .entry_liquidity_reject_round_id
                .as_deref()
                .map(|v| v == decision_round_id)
                .unwrap_or(false);
            let same_side = aggr
                .entry_liquidity_reject_side
                .as_deref()
                .map(|v| v.eq_ignore_ascii_case(&side))
                .unwrap_or(false);
            let within_window = aggr.entry_liquidity_reject_last_ts_ms > 0
                && now_ms
                    .saturating_sub(aggr.entry_liquidity_reject_last_ts_ms)
                    <= reject_window_ms;
            if same_round
                && same_side
                && within_window
                && aggr.entry_liquidity_reject_count >= reject_limit
            {
                skipped.push(json!({
                    "reason": "entry_liquidity_reject_limit_reached",
                    "entry_liquidity_reject_count": aggr.entry_liquidity_reject_count,
                    "entry_liquidity_reject_limit": reject_limit,
                    "entry_liquidity_reject_window_ms": reject_window_ms,
                    "decision": normalized
                }));
                continue;
            }
        }
        if mark_attempts
            && action == "enter"
            && require_fixed_entry_size
            && !live_has_fixed_entry_sizing()
        {
            skipped.push(json!({
                "reason": "fixed_entry_size_required",
                "required_env": "FORGE_FEV1_FIXED_ENTRY_SIZE_SHARES or FORGE_FEV1_FIXED_ENTRY_QUOTE_USDC",
                "decision": normalized
            }));
            continue;
        }
        if action == "enter" && virtual_side.is_none() {
            // Cross-market cap: count currently open positions plus in-flight enter orders.
            let projected = open_positions_total.saturating_add(enter_pending_total);
            if projected >= max_open_positions {
                skipped.push(json!({
                    "reason": "global_open_position_cap",
                    "max_open_positions": max_open_positions,
                    "open_positions": open_positions_total,
                    "enter_pending": enter_pending_total,
                    "decision": normalized
                }));
                continue;
            }
        }
        if action == "enter"
            && position_state.state.eq_ignore_ascii_case("exit_pending")
        {
            skipped.push(json!({
                "reason": "exit_state_lock",
                "state": position_state.state,
                "decision": normalized
            }));
            continue;
        }
        if action == "enter" && (has_enter_pending || has_exit_pending) {
            skipped.push(json!({
                "reason": if has_enter_pending { "enter_pending_exists" } else { "exit_pending_exists" },
                "decision": normalized
            }));
            continue;
        }
        if action == "exit" && has_exit_pending {
            skipped.push(json!({
                "reason": "exit_pending_exists",
                "decision": normalized
            }));
            continue;
        }
        if action == "exit" && has_enter_pending {
            // Exit signals must not be blocked by entry pending.
            // Execution layer will actively cancel entry/add pendings first.
            if let Some(obj) = normalized.as_object_mut() {
                obj.insert(
                    "pre_exit_cancel_entry_pending".to_string(),
                    Value::Bool(true),
                );
            }
        }
        let decision_key = live_decision_key(market_type, &normalized);
        let should_submit = if action == "enter" {
            if virtual_side.is_none() {
                true
            } else {
                skipped.push(json!({
                    "reason": "already_has_position",
                    "state_side": virtual_side,
                    "decision": normalized
                }));
                false
            }
        } else if side_match {
            true
        } else {
            let reason = if virtual_side.is_none() {
                "no_position_to_exit"
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
        let opened_new_position = action == "enter" && virtual_side.is_none();
        if action == "enter" {
            virtual_side = Some(side.clone());
            has_enter_pending = true;
        } else if action == "exit" {
            virtual_side = None;
            has_exit_pending = true;
        } else {
            has_exit_pending = true;
        }
        if opened_new_position {
            enter_pending_total = enter_pending_total.saturating_add(1);
        }
        accepted.push(LiveGatedDecision {
            decision: normalized,
            decision_key,
        });
    }

    position_state.updated_ts_ms = now_ms;
    (accepted, skipped, position_state)
}

fn live_market_target_cache_scope_key(symbol: &str, market_type: &str) -> String {
    format!(
        "{}:{}",
        symbol.trim().to_ascii_uppercase(),
        market_type.trim().to_ascii_lowercase()
    )
}

pub(super) async fn resolve_live_market_target_with_state(
    state: &ApiState,
    symbol: &str,
    market_type: &str,
) -> Result<LiveMarketTarget, ApiError> {
    resolve_live_market_target_inner(Some(state), symbol, market_type, true).await
}

pub(super) async fn resolve_live_market_target_fast_with_state(
    state: &ApiState,
    symbol: &str,
    market_type: &str,
) -> Result<LiveMarketTarget, ApiError> {
    resolve_live_market_target_inner(Some(state), symbol, market_type, false).await
}

async fn resolve_live_market_target_inner(
    state: Option<&ApiState>,
    symbol: &str,
    market_type: &str,
    allow_discovery: bool,
) -> Result<LiveMarketTarget, ApiError> {
    let now_ms = Utc::now().timestamp_millis();
    let cache_ttl_ms = live_market_target_cache_ttl_ms();
    let active_cache_max_age_ms = live_market_target_active_cache_max_age_ms();
    let resolve_attempts = live_market_target_resolve_attempts();
    let resolve_retry_ms = live_market_target_resolve_retry_ms();
    let switch_guard_ms = live_market_target_switch_guard_ms();
    let mut cached_entry: Option<CachedLiveMarketTarget> = None;
    {
        let cache = live_market_target_cache().read().await;
        let cache_key = live_market_target_cache_scope_key(symbol, market_type);
        if let Some(cached) = cache.get(&cache_key) {
            cached_entry = Some(cached.clone());
            let age_ms = now_ms.saturating_sub(cached.fetched_at_ms);
            let end_ms =
                parse_end_date_ms(cached.target.end_date.as_deref()).unwrap_or(i64::MAX / 4);
            if age_ms <= cache_ttl_ms && end_ms >= now_ms {
                return Ok(cached.target.clone());
            }
        }
    }
    if let Some(state) = state {
        if let Some(target) = resolve_live_target_from_snapshot(state, symbol, market_type, now_ms).await {
            let mut cache = live_market_target_cache().write().await;
            let cache_key = live_market_target_cache_scope_key(symbol, market_type);
            cache.insert(
                cache_key,
                CachedLiveMarketTarget {
                    fetched_at_ms: Utc::now().timestamp_millis(),
                    target: target.clone(),
                },
            );
            return Ok(target);
        }
    }

    let mut last_discovery_error: Option<String> = None;
    if allow_discovery {
        let discovery = MarketDiscovery::new(DiscoveryConfig {
            symbols: vec![symbol.trim().to_ascii_uppercase()],
            market_types: vec!["updown".to_string()],
            timeframes: vec![market_type.to_string()],
            ..DiscoveryConfig::default()
        });
        for attempt_idx in 0..resolve_attempts {
            let discovered = match discovery.discover().await {
                Ok(v) => v,
                Err(e) => {
                    last_discovery_error = Some(e.to_string());
                    if attempt_idx + 1 < resolve_attempts && resolve_retry_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(resolve_retry_ms)).await;
                    }
                    continue;
                }
            };
            let mut markets: Vec<MarketDescriptor> = discovered
                .into_iter()
                .filter(|m| {
                    m.symbol.eq_ignore_ascii_case(symbol)
                        && m.timeframe
                            .as_deref()
                            .map(|tf| tf.eq_ignore_ascii_case(market_type))
                            .unwrap_or(false)
                })
                .collect();
            if markets.is_empty() {
                if attempt_idx + 1 < resolve_attempts && resolve_retry_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(resolve_retry_ms)).await;
                }
                continue;
            }
            {
                let mut token_cache = live_market_token_cache().write().await;
                for m in &markets {
                    let yes = m
                        .token_id_yes
                        .as_deref()
                        .map(str::trim)
                        .unwrap_or_default();
                    let no = m
                        .token_id_no
                        .as_deref()
                        .map(str::trim)
                        .unwrap_or_default();
                    if yes.is_empty() || no.is_empty() || yes == no {
                        continue;
                    }
                    token_cache.insert(m.market_id.clone(), (yes.to_string(), no.to_string()));
                }
            }
            markets.sort_by_key(|m| {
                let end_ms = parse_end_date_ms(m.end_date.as_deref()).unwrap_or(i64::MAX / 4);
                let bucket = if end_ms >= now_ms.saturating_add(switch_guard_ms) {
                    0_i64
                } else if end_ms >= now_ms {
                    1_i64
                } else {
                    2_i64
                };
                let distance = if end_ms >= now_ms {
                    end_ms.saturating_sub(now_ms)
                } else {
                    now_ms.saturating_sub(end_ms)
                };
                (
                    bucket,
                    distance,
                    std::cmp::Reverse(end_ms),
                    m.market_id.clone(),
                )
            });
            for candidate in markets {
                let token_pair = match (
                    candidate
                        .token_id_yes
                        .as_deref()
                        .map(str::trim)
                        .filter(|v| !v.is_empty()),
                    candidate
                        .token_id_no
                        .as_deref()
                        .map(str::trim)
                        .filter(|v| !v.is_empty()),
                ) {
                    (Some(yes), Some(no)) if yes != no => Some((yes.to_string(), no.to_string())),
                    _ => resolve_token_ids_from_target_cache(&candidate.market_id, market_type).await,
                };
                let Some((token_id_yes, token_id_no)) = token_pair else {
                    continue;
                };
                let target = LiveMarketTarget {
                    market_id: candidate.market_id,
                    symbol: candidate.symbol,
                    timeframe: candidate.timeframe.unwrap_or_else(|| market_type.to_string()),
                    token_id_yes,
                    token_id_no,
                    end_date: candidate.end_date,
                };
                {
                    let mut cache = live_market_target_cache().write().await;
                    let cache_key = live_market_target_cache_scope_key(symbol, market_type);
                    cache.insert(
                        cache_key,
                        CachedLiveMarketTarget {
                            fetched_at_ms: Utc::now().timestamp_millis(),
                            target: target.clone(),
                        },
                    );
                }
                return Ok(target);
            }
            if attempt_idx + 1 < resolve_attempts && resolve_retry_ms > 0 {
                tokio::time::sleep(Duration::from_millis(resolve_retry_ms)).await;
            }
        }
    }
    if let Some(state) = state {
        if let Some(target) = resolve_live_target_from_snapshot(state, symbol, market_type, now_ms).await {
            tracing::warn!(
                market_type = market_type,
                "market target resolved from recorder snapshot fallback"
            );
            let mut cache = live_market_target_cache().write().await;
            let cache_key = live_market_target_cache_scope_key(symbol, market_type);
            cache.insert(
                cache_key,
                CachedLiveMarketTarget {
                    fetched_at_ms: Utc::now().timestamp_millis(),
                    target: target.clone(),
                },
            );
            return Ok(target);
        }
    }
    if let Some(cached) = cached_entry {
        let age_ms = now_ms.saturating_sub(cached.fetched_at_ms);
        let end_ms = parse_end_date_ms(cached.target.end_date.as_deref()).unwrap_or(i64::MIN / 4);
        if age_ms <= active_cache_max_age_ms && end_ms >= now_ms {
            tracing::warn!(
                market_type = market_type,
                resolve_attempts = resolve_attempts,
                cache_age_ms = age_ms,
                active_cache_max_age_ms = active_cache_max_age_ms,
                "market target resolve exhausted retries, fallback to still-active cached target"
            );
            return Ok(cached.target);
        }
    }
    if allow_discovery {
        tracing::warn!(
            market_type = market_type,
            resolve_attempts = resolve_attempts,
            last_discovery_error = last_discovery_error.as_deref().unwrap_or("none"),
            "market target resolve exhausted retries without stale fallback"
        );
        Err(ApiError::bad_request(format!(
            "no active {symbol} {market_type} market with token ids after {} attempts{}",
            resolve_attempts,
            last_discovery_error
                .as_ref()
                .map(|e| format!(", last_error={e}"))
                .unwrap_or_default()
        )))
    } else {
        Err(ApiError::bad_request(format!(
            "fast market target resolve miss for {symbol}/{market_type} (cache/snapshot only)"
        )))
    }
}

fn collect_decision_token_ids(
    target: &LiveMarketTarget,
    decisions: &[LiveGatedDecision],
) -> Vec<String> {
    let mut uniq = HashSet::<String>::new();
    for gated in decisions {
        if let Some(token) = token_id_for_decision(&gated.decision, target) {
            uniq.insert(token.to_string());
        }
    }
    uniq.into_iter().collect()
}

async fn prefetch_rust_books_for_tokens(
    state: &ApiState,
    ctx: &Arc<RustExecutorContext>,
    token_ids: &[String],
) -> HashMap<String, Option<GatewayBookSnapshot>> {
    let futures = token_ids.iter().map(|token_id| {
        let state = state.clone();
        let ctx = Arc::clone(ctx);
        let token_id = token_id.clone();
        async move {
            let snapshot = if let Some(cached) = state.get_rust_book_cache(&token_id).await {
                Some(cached)
            } else if let Some(v) = fetch_rust_book_snapshot(&ctx, &token_id).await {
                state.put_rust_book_cache(&token_id, v.clone()).await;
                Some(v)
            } else {
                None
            };
            (token_id, snapshot)
        }
    });
    let rows = join_all(futures).await;
    rows.into_iter().collect()
}

pub(super) async fn load_cached_rust_books_for_tokens(
    state: &ApiState,
    token_ids: &[String],
) -> HashMap<String, Option<GatewayBookSnapshot>> {
    let futures = token_ids.iter().map(|token_id| {
        let state = state.clone();
        let token_id = token_id.clone();
        async move {
            let snapshot = state.get_rust_book_cache(&token_id).await;
            (token_id, snapshot)
        }
    });
    let rows = join_all(futures).await;
    rows.into_iter().collect()
}
