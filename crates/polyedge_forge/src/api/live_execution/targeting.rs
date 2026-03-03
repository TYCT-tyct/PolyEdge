fn decision_round_matches_target(decision: &Value, target: &LiveMarketTarget) -> bool {
    let action = decision
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let is_entry_like = action == "enter" || action == "add";
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
    a == "enter" || a == "add"
}

fn pending_cancel_due_ms(row: &LivePendingOrder) -> i64 {
    let base = row.cancel_after_ms.max(500);
    let maker_tif = matches!(
        row.tif.to_ascii_uppercase().as_str(),
        "GTD" | "GTC" | "POST_ONLY"
    );
    if maker_tif && is_entry_action(&row.action) {
        base.min(live_entry_maker_max_wait_ms())
    } else {
        base
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
    market_type: &str,
    now_ms: i64,
) -> Option<LiveMarketTarget> {
    async fn try_snapshot_candidate(
        _state: &ApiState,
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
            symbol: "BTCUSDT".to_string(),
            timeframe: market_type.to_string(),
            token_id_yes,
            token_id_no,
            end_date: end_date_iso_from_ms(end_ms),
        })
    }

    let symbol_key = format!(
        "{}:snapshot:latest:BTCUSDT:{market_type}",
        state.redis_prefix
    );
    if let Ok(snapshot_json) = read_key_json(state, &symbol_key).await {
        let snapshot = snapshot_json.0;
        if let Some(target) =
            try_snapshot_candidate(state, market_type, now_ms, &snapshot, "symbol_snapshot").await
        {
            return Some(target);
        }
    }

    let tf_key = format!("{}:snapshot:latest:tf:{market_type}", state.redis_prefix);
    if let Ok(tf_snapshot_json) = read_key_json(state, &tf_key).await {
        let tf_snapshot = tf_snapshot_json.0;
        let mut btc_rows: Vec<&Value> = match tf_snapshot.as_array() {
            Some(rows) => rows
                .iter()
                .filter(|row| {
                    row.get("symbol")
                        .and_then(Value::as_str)
                        .map(|s| s.eq_ignore_ascii_case("BTCUSDT"))
                        .unwrap_or(false)
                })
                .collect(),
            None => Vec::new(),
        };
        btc_rows.sort_by_key(|row| {
            std::cmp::Reverse(
                row.get("ts_ireland_sample_ms")
                    .and_then(Value::as_i64)
                    .unwrap_or(0),
            )
        });
        for row in btc_rows {
            if let Some(target) =
                try_snapshot_candidate(state, market_type, now_ms, row, "tf_snapshot").await
            {
                return Some(target);
            }
        }
    }

    let pattern = format!(
        "{}:snapshot:latest:BTCUSDT:{}:*",
        state.redis_prefix, market_type
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
        if matches!(action.as_str(), "enter" | "add")
            && position_state.state.eq_ignore_ascii_case("exit_pending")
        {
            skipped.push(json!({
                "reason": "exit_state_lock",
                "state": position_state.state,
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
            let freshness_ms = if action == "enter" || action == "add" {
                20_000
            } else {
                90_000
            };
            let round_ok = d
                .get("round_id")
                .and_then(Value::as_str)
                .map(|r| r == latest_round)
                .unwrap_or(false);
            let ts_ok = d
                .get("ts_ms")
                .and_then(Value::as_i64)
                .map(|ts| ts >= latest_ts_ms.saturating_sub(freshness_ms))
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
            let freshness_ms = if action == "enter" || action == "add" {
                20_000
            } else {
                90_000
            };
            let action_ok = if drain_only {
                action == "exit" || action == "reduce"
            } else {
                d.get("ts_ms")
                    .and_then(Value::as_i64)
                    .map(|ts| ts >= latest_ts_ms.saturating_sub(freshness_ms))
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

pub(super) async fn resolve_live_market_target_with_state(
    state: &ApiState,
    market_type: &str,
) -> Result<LiveMarketTarget, ApiError> {
    resolve_live_market_target_inner(Some(state), market_type).await
}

async fn resolve_live_market_target_inner(
    state: Option<&ApiState>,
    market_type: &str,
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
        if let Some(cached) = cache.get(market_type) {
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
        if let Some(target) = resolve_live_target_from_snapshot(state, market_type, now_ms).await {
            let mut cache = live_market_target_cache().write().await;
            cache.insert(
                market_type.to_string(),
                CachedLiveMarketTarget {
                    fetched_at_ms: Utc::now().timestamp_millis(),
                    target: target.clone(),
                },
            );
            return Ok(target);
        }
    }

    let discovery = MarketDiscovery::new(DiscoveryConfig {
        symbols: vec!["BTCUSDT".to_string()],
        market_types: vec!["updown".to_string()],
        timeframes: vec![market_type.to_string()],
        ..DiscoveryConfig::default()
    });
    let mut last_discovery_error: Option<String> = None;
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
                m.symbol.eq_ignore_ascii_case("BTCUSDT")
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
                token_cache.insert(
                    m.market_id.clone(),
                    (yes.to_string(), no.to_string()),
                );
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
                cache.insert(
                    market_type.to_string(),
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
    if let Some(state) = state {
        if let Some(target) = resolve_live_target_from_snapshot(state, market_type, now_ms).await {
            tracing::warn!(
                market_type = market_type,
                "market target resolved from recorder snapshot fallback"
            );
            let mut cache = live_market_target_cache().write().await;
            cache.insert(
                market_type.to_string(),
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
    tracing::warn!(
        market_type = market_type,
        resolve_attempts = resolve_attempts,
        last_discovery_error = last_discovery_error.as_deref().unwrap_or("none"),
        "market target resolve exhausted retries without stale fallback"
    );
    Err(ApiError::bad_request(format!(
        "no active BTC {market_type} market with token ids after {} attempts{}",
        resolve_attempts,
        last_discovery_error
            .as_ref()
            .map(|e| format!(", last_error={e}"))
            .unwrap_or_default()
    )))
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

