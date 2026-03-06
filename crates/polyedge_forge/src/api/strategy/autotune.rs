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
    recent_total_pnl_cents: f64,
    validation_total_pnl_cents: f64,
    positive_fold_ratio: f64,
    robust_eligible: bool,
}

pub(super) fn payload_metrics(payload: &Value) -> ObjectiveMetrics {
    let robust_summary = payload.get("summary_robust").unwrap_or(&Value::Null);
    let summary = payload
        .get("summary_robust")
        .or_else(|| payload.get("summary_validation"))
        .or_else(|| payload.get("summary_recent"))
        .or_else(|| payload.get("summary"))
        .unwrap_or(&Value::Null);
    let recent_total_pnl_cents = payload
        .get("summary_recent")
        .and_then(|v| v.get("total_pnl_cents"))
        .and_then(Value::as_f64)
        .unwrap_or_else(|| {
            summary
                .get("total_pnl_cents")
                .and_then(Value::as_f64)
                .unwrap_or(0.0)
        });
    let validation_total_pnl_cents = payload
        .get("summary_validation")
        .and_then(|v| v.get("total_pnl_cents"))
        .and_then(Value::as_f64)
        .unwrap_or_else(|| {
            summary
                .get("total_pnl_cents")
                .and_then(Value::as_f64)
                .unwrap_or(0.0)
        });
    let positive_fold_ratio = payload
        .pointer("/robustness/gates/positive_fold_ratio")
        .and_then(Value::as_f64)
        .unwrap_or_else(|| {
            payload
                .pointer("/robustness/walk_forward/aggregate/positive_total_pnl_ratio")
                .and_then(Value::as_f64)
                .unwrap_or(0.0)
        });
    let robust_eligible = payload
        .pointer("/robustness/gates/eligible_positive")
        .and_then(Value::as_bool)
        .unwrap_or_else(|| {
            recent_total_pnl_cents > 0.0
                && validation_total_pnl_cents > 0.0
                && robust_summary
                    .get("avg_pnl_cents")
                    .and_then(Value::as_f64)
                    .unwrap_or_else(|| {
                        summary
                            .get("avg_pnl_cents")
                            .and_then(Value::as_f64)
                            .unwrap_or(0.0)
                    })
                    > 0.0
        });
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
        recent_total_pnl_cents,
        validation_total_pnl_cents,
        positive_fold_ratio,
        robust_eligible,
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
    if !c.robust_eligible {
        return (false, "candidate_failed_positive_gates".to_string());
    }
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
        if c.positive_fold_ratio > 0.0 && c.positive_fold_ratio < 0.60 {
            return (false, "candidate_positive_fold_ratio_too_low".to_string());
        }
        return (true, "bootstrap_promote".to_string());
    }
    let i = payload_metrics(incumbent);
    if c.trade_count < (min_trades * 0.85).max(10.0) {
        return (false, "candidate_trade_count_too_low".to_string());
    }
    if c.avg_pnl_cents <= min_pnl {
        return (false, "candidate_avg_pnl_non_positive".to_string());
    }
    if c.recent_total_pnl_cents <= 0.0 || c.validation_total_pnl_cents <= 0.0 {
        return (false, "candidate_total_pnl_non_positive".to_string());
    }
    if c.positive_fold_ratio > 0.0 && c.positive_fold_ratio < 0.60 {
        return (false, "candidate_positive_fold_ratio_too_low".to_string());
    }
    if c.trade_count + 1.0 < i.trade_count * 0.85 {
        return (false, "trade_count_regression".to_string());
    }
    let drawdown_limit = if i.max_drawdown_cents > 0.0 {
        i.max_drawdown_cents * 1.08 + 0.25
    } else {
        c.max_drawdown_cents
    };
    if c.max_drawdown_cents > drawdown_limit {
        return (false, "drawdown_regression".to_string());
    }
    let objective_improved = c.objective > i.objective + 8.0;
    let pnl_improved = c.avg_pnl_cents > i.avg_pnl_cents + 0.08
        || c.recent_total_pnl_cents > i.recent_total_pnl_cents + 4.0;
    let win_rate_guard = c.win_rate_pct + 0.75 >= i.win_rate_pct;
    if !(objective_improved || pnl_improved) {
        return (false, "not_meaningfully_better".to_string());
    }
    if !win_rate_guard && c.avg_pnl_cents <= i.avg_pnl_cents + 0.20 {
        return (false, "win_rate_regression_without_pnl_gain".to_string());
    }
    (true, "promote_positive_robust_candidate".to_string())
}
