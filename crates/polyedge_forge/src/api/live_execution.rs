use super::*;
use futures::future::join_all;

#[derive(Debug, Clone)]
pub(super) struct LiveExecutionConfig {
    pub(super) min_quote_usdc: f64,
    pub(super) entry_slippage_bps: f64,
    pub(super) exit_slippage_bps: f64,
    pub(super) force_slippage_bps: Option<f64>,
}

impl LiveExecutionConfig {
    pub(super) fn from_env() -> Self {
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
        Self {
            min_quote_usdc,
            entry_slippage_bps,
            exit_slippage_bps,
            force_slippage_bps,
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
    /// Number of bid levels in the order book (for one-sided book detection)
    bid_levels: Option<usize>,
    /// Number of ask levels in the order book (for one-sided book detection)
    ask_levels: Option<usize>,
}

include!("live_execution/config.rs");
include!("live_execution/targeting.rs");
include!("live_execution/planner.rs");
include!("live_execution/pending.rs");
include!("live_execution/rust_sdk.rs");
include!("live_execution/orchestrator.rs");

#[cfg(test)]
include!("live_execution/tests.rs");
