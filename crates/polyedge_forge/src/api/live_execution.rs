use super::*;
use futures::future::join_all;

#[derive(Debug, Clone)]
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
            .clamp(0.01, 1000.0);
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

include!("live_execution/config.rs");
include!("live_execution/targeting.rs");
include!("live_execution/gateway.rs");
include!("live_execution/pending.rs");
include!("live_execution/rust_sdk.rs");
include!("live_execution/orchestrator.rs");

#[cfg(test)]
include!("live_execution/tests.rs");
