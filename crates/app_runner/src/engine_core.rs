use super::*;

pub(super) fn is_quote_reject_reason(reason: &str) -> bool {
    reason.starts_with("execution_") || reason.starts_with("exchange_reject")
}

pub(super) fn is_policy_block_reason(reason: &str) -> bool {
    reason == "risk_capped_zero"
        || reason.starts_with("risk:")
        || reason.starts_with("rate_budget_")
        || matches!(
            reason,
            "open_orders_pressure_precheck"
                | "taker_slippage_budget"
                | "market_rank_blocked"
                | "symbol_quality_guard"
                | "decision_backlog_guard"
                | "no_quote_policy"
                | "no_quote_edge"
        )
}

pub(super) fn classify_execution_style(
    book: &BookTop,
    intent: &core_types::QuoteIntent,
) -> ExecutionStyle {
    match intent.side {
        OrderSide::BuyYes => {
            if intent.price >= book.ask_yes {
                ExecutionStyle::Taker
            } else {
                ExecutionStyle::Maker
            }
        }
        OrderSide::SellYes => {
            if intent.price <= book.bid_yes {
                ExecutionStyle::Taker
            } else {
                ExecutionStyle::Maker
            }
        }
        OrderSide::BuyNo => {
            if intent.price >= book.ask_no {
                ExecutionStyle::Taker
            } else {
                ExecutionStyle::Maker
            }
        }
        OrderSide::SellNo => {
            if intent.price <= book.bid_no {
                ExecutionStyle::Taker
            } else {
                ExecutionStyle::Maker
            }
        }
    }
}

pub(super) fn normalize_reject_code(raw: &str) -> String {
    let normalized = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    normalized.trim_matches('_').to_string()
}

pub(super) fn classify_execution_error_reason(err: &anyhow::Error) -> &'static str {
    let msg = err.to_string().to_ascii_lowercase();
    if msg.contains("429") {
        "exchange_reject_rate_limit"
    } else if msg.contains("401") || msg.contains("403") {
        "exchange_reject_auth"
    } else if msg.contains("400") || msg.contains("422") {
        "exchange_reject_bad_request"
    } else if msg.contains("timeout") {
        "execution_timeout"
    } else if msg.contains("connection") || msg.contains("broken pipe") || msg.contains("closed") {
        "execution_network"
    } else {
        "execution_error"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_block_reason_includes_runtime_gates() {
        assert!(is_policy_block_reason("risk:exposure"));
        assert!(is_policy_block_reason("risk_capped_zero"));
        assert!(is_policy_block_reason("rate_budget_global"));
        assert!(is_policy_block_reason("open_orders_pressure_precheck"));
        assert!(is_policy_block_reason("decision_backlog_guard"));
        assert!(!is_policy_block_reason("ref_dedupe_dropped"));
    }
}
