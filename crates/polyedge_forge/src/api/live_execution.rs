use super::*;
use futures::future::join_all;

/// Kelly formula position sizing stages
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) enum KellyStage {
    Stage1, // $0-$50: aggressive (25% of capital)
    Stage2, // $50-$200: moderate (20% of capital)
    Stage3, // $200+: Kelly-based (kelly_fraction * Kelly)
}

/// Deployment stages for gradual live rollout
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) enum DeployStage {
    Shadow, // Full strategy, $0 amount (verify connectivity)
    Canary, // Limited live, 5% of capital
    Full,   // Full capital management
}

impl DeployStage {
    /// Get deployment stage from environment variable FORGE_FEV1_DEPLOY_STAGE
    #[allow(dead_code)]
    pub(super) fn from_env() -> Self {
        let stage = std::env::var("FORGE_FEV1_DEPLOY_STAGE")
            .ok()
            .map(|v| v.trim().to_ascii_lowercase())
            .unwrap_or_else(|| "shadow".to_string());

        match stage.as_str() {
            "canary" => DeployStage::Canary,
            "full" => DeployStage::Full,
            _ => DeployStage::Shadow,
        }
    }

    /// Returns the fraction of capital to use for this stage
    #[allow(dead_code)]
    pub(super) fn capital_fraction(&self) -> f64 {
        match self {
            DeployStage::Shadow => 0.0,  // $0 actual trades
            DeployStage::Canary => 0.05, // 5% of capital
            DeployStage::Full => 1.0,    // Full capital
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(super) struct LiveExecutionConfig {
    pub(super) min_quote_usdc: f64,
    pub(super) entry_slippage_bps: f64,
    pub(super) exit_slippage_bps: f64,
    pub(super) force_slippage_bps: Option<f64>,
    // Kelly formula parameters
    pub(super) kelly_fraction: f64,
    pub(super) kelly_stage1_cap: f64,
    pub(super) kelly_stage2_cap: f64,
    pub(super) kelly_stage1_fraction: f64,
    pub(super) kelly_stage2_fraction: f64,
    // Circuit breaker parameters
    pub(super) max_daily_drawdown: f64,
    pub(super) max_total_drawdown: f64,
    pub(super) consecutive_loss_limit: u8,
    pub(super) cooldown_minutes: u32,
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

        // Kelly parameters
        let kelly_fraction = std::env::var("FORGE_FEV1_KELLY_FRACTION")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.25)
            .clamp(0.05, 0.50);
        let kelly_stage1_cap = std::env::var("FORGE_FEV1_KELLY_STAGE1_CAP")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(50.0)
            .clamp(10.0, 500.0);
        let kelly_stage2_cap = std::env::var("FORGE_FEV1_KELLY_STAGE2_CAP")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(200.0)
            .clamp(50.0, 2000.0);
        let kelly_stage1_fraction = std::env::var("FORGE_FEV1_KELLY_STAGE1_FRACTION")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.25)
            .clamp(0.10, 0.50);
        let kelly_stage2_fraction = std::env::var("FORGE_FEV1_KELLY_STAGE2_FRACTION")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.20)
            .clamp(0.05, 0.40);

        // Circuit breaker parameters
        let max_daily_drawdown = std::env::var("FORGE_FEV1_MAX_DAILY_DRAWDOWN")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.20)
            .clamp(0.05, 0.50);
        let max_total_drawdown = std::env::var("FORGE_FEV1_MAX_TOTAL_DRAWDOWN")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.30)
            .clamp(0.10, 0.60);
        let consecutive_loss_limit = std::env::var("FORGE_FEV1_CONSECUTIVE_LOSS_LIMIT")
            .ok()
            .and_then(|v| v.parse::<u8>().ok())
            .unwrap_or(3)
            .clamp(1, 10);
        let cooldown_minutes = std::env::var("FORGE_FEV1_COOLDOWN_MINUTES")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(10)
            .clamp(1, 120);

        Self {
            min_quote_usdc,
            entry_slippage_bps,
            exit_slippage_bps,
            force_slippage_bps,
            kelly_fraction,
            kelly_stage1_cap,
            kelly_stage2_cap,
            kelly_stage1_fraction,
            kelly_stage2_fraction,
            max_daily_drawdown,
            max_total_drawdown,
            consecutive_loss_limit,
            cooldown_minutes,
        }
    }
}

/// Compute Kelly-based position size
/// Kelly = (p * b - q) / b where:
/// - p = win probability (win rate)
/// - b = win/loss ratio
/// - q = 1 - p
#[allow(dead_code)]
pub(super) fn compute_kelly_size(
    portfolio_value: f64,
    win_rate: f64,
    avg_win_loss_ratio: f64,
    kelly_fraction: f64,
    stage1_cap: f64,
    stage2_cap: f64,
    stage1_frac: f64,
    stage2_frac: f64,
) -> f64 {
    if portfolio_value <= 0.0 || win_rate <= 0.0 || avg_win_loss_ratio <= 0.0 {
        return 0.0;
    }

    let q = 1.0 - win_rate;
    let b = avg_win_loss_ratio;
    let kelly = (win_rate * b - q) / b;
    let fractional_kelly = kelly * kelly_fraction;

    let fraction = if portfolio_value < stage1_cap {
        stage1_frac
    } else if portfolio_value < stage2_cap {
        stage2_frac
    } else {
        fractional_kelly.clamp(0.05, 0.40)
    };

    (portfolio_value * fraction).max(0.0)
}

/// Check if circuit breaker should trigger based on drawdown
#[allow(dead_code)]
pub(super) fn should_circuit_break(
    current_drawdown: f64,
    daily_drawdown: f64,
    max_daily: f64,
    max_total: f64,
) -> bool {
    current_drawdown > max_total || daily_drawdown > max_daily
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

include!("live_execution/config.rs");
include!("live_execution/targeting.rs");
include!("live_execution/planner.rs");
include!("live_execution/pending.rs");
include!("live_execution/rust_sdk.rs");
include!("live_execution/orchestrator.rs");

#[cfg(test)]
include!("live_execution/tests.rs");
