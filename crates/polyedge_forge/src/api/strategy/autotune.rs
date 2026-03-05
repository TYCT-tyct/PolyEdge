pub(super) fn normalize_autotune_context(
    raw: Option<&str>,
    market_type: &str,
    symbol: &str,
) -> String {
    let default_ctx = format!("{}:{market_type}", symbol.to_ascii_lowercase());
    let Some(v) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return default_ctx;
    };
    let cleaned: String = v
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, ':' | '_' | '-' | '.'))
        .collect();
    if cleaned.is_empty() {
        default_ctx
    } else {
        cleaned.to_ascii_lowercase()
    }
}

pub(super) fn strategy_autotune_key(redis_prefix: &str, context: &str) -> String {
    format!("{redis_prefix}:strategy:autotune:{context}")
}

pub(super) fn strategy_autotune_history_key(redis_prefix: &str, context: &str) -> String {
    format!("{redis_prefix}:strategy:autotune:{context}:history")
}

pub(super) fn strategy_autotune_active_key(
    redis_prefix: &str,
    market_type: &str,
    symbol: &str,
) -> String {
    format!(
        "{redis_prefix}:strategy:autotune:active:{}:{market_type}",
        symbol.to_ascii_lowercase()
    )
}

pub(super) fn strategy_autotune_live_key(
    redis_prefix: &str,
    market_type: &str,
    symbol: &str,
) -> String {
    format!(
        "{redis_prefix}:strategy:autotune:live:{}:{market_type}",
        symbol.to_ascii_lowercase()
    )
}

pub(super) fn strategy_autotune_active_history_key(
    redis_prefix: &str,
    market_type: &str,
    symbol: &str,
) -> String {
    format!(
        "{redis_prefix}:strategy:autotune:active:{}:{market_type}:history",
        symbol.to_ascii_lowercase()
    )
}

pub(super) async fn resolve_autotune_doc(
    state: &ApiState,
    context: &str,
    _market_type: &str,
) -> Result<(Option<Value>, String, bool), ApiError> {
    let key = strategy_autotune_key(&state.redis_prefix, context);
    if let Some(payload) = read_key_value(state, &key).await? {
        return Ok((Some(payload), key, false));
    }
    Ok((None, key, false))
}

pub(super) async fn resolve_autotune_active_doc(
    state: &ApiState,
    market_type: &str,
    symbol: &str,
) -> Result<(Option<Value>, String), ApiError> {
    let key = strategy_autotune_active_key(&state.redis_prefix, market_type, symbol);
    let payload = read_key_value(state, &key).await?;
    Ok((payload, key))
}

pub(super) async fn resolve_autotune_live_doc(
    state: &ApiState,
    market_type: &str,
    symbol: &str,
) -> Result<(Option<Value>, String), ApiError> {
    let key = strategy_autotune_live_key(&state.redis_prefix, market_type, symbol);
    let payload = read_key_value(state, &key).await?;
    Ok((payload, key))
}

#[derive(Debug, Clone, Copy, Serialize)]
pub(super) struct ObjectiveMetrics {
    objective: f64,
    win_rate_pct: f64,
    avg_pnl_cents: f64,
    max_drawdown_cents: f64,
    trade_count: f64,
}

pub(super) fn payload_metrics(payload: &Value) -> ObjectiveMetrics {
    let summary = payload
        .get("summary_validation")
        .or_else(|| payload.get("summary_recent"))
        .or_else(|| payload.get("summary"))
        .unwrap_or(&Value::Null);
    ObjectiveMetrics {
        objective: payload
            .get("objective")
            .and_then(Value::as_f64)
            .unwrap_or(-1_000_000_000.0),
        win_rate_pct: summary
            .get("win_rate_pct")
            .and_then(Value::as_f64)
            .unwrap_or(0.0),
        avg_pnl_cents: summary
            .get("avg_pnl_cents")
            .and_then(Value::as_f64)
            .unwrap_or(0.0),
        max_drawdown_cents: summary
            .get("max_drawdown_cents")
            .and_then(Value::as_f64)
            .unwrap_or(0.0),
        trade_count: summary
            .get("trade_count")
            .and_then(Value::as_f64)
            .unwrap_or(0.0),
    }
}

pub(super) fn should_promote_candidate(
    candidate: &Value,
    incumbent: &Value,
    has_incumbent: bool,
    min_trades: f64,
    min_win_rate: f64,
    min_pnl: f64,
) -> (bool, String) {
    let c = payload_metrics(candidate);
    if !has_incumbent {
        if c.trade_count < min_trades {
            return (false, "candidate_trade_count_too_low".to_string());
        }
        if c.avg_pnl_cents <= min_pnl {
            return (false, "candidate_avg_pnl_non_positive".to_string());
        }
        if c.win_rate_pct < min_win_rate {
            return (
                false,
                "candidate_win_rate_below_bootstrap_floor".to_string(),
            );
        }
        return (true, "bootstrap_promote".to_string());
    }
    let i = payload_metrics(incumbent);
    // Incumbent comparison is slightly more relaxed on raw minimums but MUST strictly beat incumbent
    if c.trade_count < (min_trades * 0.85).max(10.0) {
        return (false, "candidate_trade_count_too_low".to_string());
    }
    if c.avg_pnl_cents <= min_pnl {
        return (false, "candidate_avg_pnl_non_positive".to_string());
    }
    if c.trade_count + 1.0 < i.trade_count {
        return (false, "trade_count_regression".to_string());
    }
    if c.win_rate_pct + 1e-9 < i.win_rate_pct {
        return (false, "win_rate_regression".to_string());
    }
    if c.avg_pnl_cents + 1e-9 < i.avg_pnl_cents {
        return (false, "avg_pnl_regression".to_string());
    }
    if c.max_drawdown_cents > i.max_drawdown_cents + 1e-9 {
        return (false, "drawdown_regression".to_string());
    }
    if c.objective + 1e-9 < i.objective {
        return (false, "objective_regression".to_string());
    }
    let strictly_better = (c.win_rate_pct > i.win_rate_pct + 1e-9)
        || (c.avg_pnl_cents > i.avg_pnl_cents + 1e-9)
        || (c.max_drawdown_cents + 1e-9 < i.max_drawdown_cents)
        || (c.objective > i.objective + 1e-9);
    if !strictly_better {
        return (false, "not_strictly_better".to_string());
    }
    (true, "promote_strictly_better_candidate".to_string())
}
