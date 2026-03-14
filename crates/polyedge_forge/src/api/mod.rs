use std::cmp::min;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Datelike, Timelike, Utc};
use futures::StreamExt;
use market_discovery::{DiscoveryConfig, MarketDescriptor, MarketDiscovery};
use polymarket_client_sdk::auth::state::Authenticated as PmAuthenticated;
use polymarket_client_sdk::auth::Credentials as PmCredentials;
use polymarket_client_sdk::auth::{
    LocalSigner as PmLocalSigner, Normal as PmNormal, Signer as PmSigner,
};
use polymarket_client_sdk::clob::types::request::OrderBookSummaryRequest as PmOrderBookSummaryRequest;
use polymarket_client_sdk::clob::types::request::TradesRequest as PmTradesRequest;
use polymarket_client_sdk::clob::types::{
    Amount as PmAmount, Side as PmSide, SignatureType as PmSignatureType,
};
use polymarket_client_sdk::clob::types::{
    OrderStatusType as PmOrderStatusType, OrderType as PmOrderType,
};
use polymarket_client_sdk::clob::{Client as PmClient, Config as PmConfig};
use polymarket_client_sdk::types::{Address as PmAddress, Decimal as PmDecimal, U256 as PmU256};
use polymarket_client_sdk::POLYGON;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{mpsc, RwLock, Semaphore};
use tower_http::services::{ServeDir, ServeFile};
use uuid::Uuid;

use crate::fev1;

use infra::*;
use live_execution::*;
use read_api::*;
use snapshot::*;
use strategy::*;

mod infra;
mod live_execution;
mod market_utils;
mod read_api;
mod row_utils;
mod snapshot;
mod strategy;

use market_utils::{
    infer_market_type_from_round_id, is_safe_round_id, market_type_to_ms, normalize_market_type,
    parse_end_date_ms, parse_round_start_ms,
};
use row_utils::{row_f64, row_i64, rows_from_json, stride_downsample};

#[derive(Debug, Clone)]
pub struct ApiConfig {
    pub bind: String,
    pub clickhouse_url: Option<String>,
    pub redis_url: Option<String>,
    pub redis_prefix: String,
    pub dashboard_dist_dir: Option<String>,
}

#[derive(Clone)]
struct ApiState {
    ch_url: Option<String>,
    redis_prefix: String,
    redis_client: Option<redis::Client>,
    redis_manager: Option<redis::aio::ConnectionManager>,
    chart_cache: Arc<RwLock<HashMap<String, ChartCacheEntry>>>,
    live_position_states: Arc<RwLock<HashMap<String, LivePositionState>>>,
    live_decision_guard: Arc<RwLock<HashMap<String, i64>>>,
    live_events: Arc<RwLock<VecDeque<Value>>>,
    live_event_seq: Arc<RwLock<u64>>,
    live_pending_orders: Arc<RwLock<HashMap<String, LivePendingOrder>>>,
    live_runtime_snapshots: Arc<RwLock<HashMap<String, Value>>>,
    live_recent_active_intents: Arc<RwLock<HashMap<String, VecDeque<Value>>>>,
    live_runtime_controls: Arc<RwLock<HashMap<String, LiveRuntimeControl>>>,
    live_runtime_paper_reset_anchors: Arc<RwLock<HashMap<String, i64>>>,
    live_persist_inflight: Arc<RwLock<HashSet<String>>>,
    live_execution_aggr_states: Arc<RwLock<HashMap<String, LiveExecutionAggState>>>,
    live_rust_executor: Arc<RwLock<Option<Arc<RustExecutorContext>>>>,
    live_rust_book_cache: Arc<RwLock<HashMap<String, RustBookCacheEntry>>>,
    runtime_alert_throttle: Arc<RwLock<HashMap<String, i64>>>,
    runtime_daily_report_sent: Arc<RwLock<HashSet<String>>>,
    strategy_heavy_slots: Arc<Semaphore>,
    strategy_live_source_slots: Arc<Semaphore>,
    runtime_event_samples: Arc<RwLock<HashMap<String, RuntimeEventSampleBuffer>>>,
    gateway_http_client: Arc<reqwest::Client>,
    /// Three-layer kill switch state per trading scope (symbol|market_type).
    live_kill_switch: Arc<RwLock<HashMap<String, LiveKillSwitchState>>>,
}

#[derive(Clone)]
struct ChartCacheEntry {
    created_at: Instant,
    payload: Value,
}

/// Classification of entry attempt outcomes for kill-switch tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryAttemptReason {
    /// Successfully built and submitted payload
    Success,
    /// Price exceeded parity band - systematic execution quality issue (triggers halt)
    ParityExhausted,
    /// No orders found / one-sided book - market microstructure (doesn't trigger halt)
    LiquidityEmpty,
    /// Infrastructure failure: timeout, 5xx, network error (doesn't trigger halt)
    InfraError,
}

/// Three-layer automated kill switch for a trading scope.
/// Layer 1: Risk (daily loss / drawdown limits)
/// Layer 2: Execution quality (parity_band_exhaustion rate)
/// Layer 3: Signal quality (rolling win rate)
#[derive(Debug, Clone)]
struct LiveKillSwitchState {
    /// When this kill switch was activated (None = not active)
    halted_since_ms: Option<i64>,
    /// Human-readable reason for the halt
    halt_reason: Option<String>,
    /// Resume after this timestamp (None = manual resume required)
    resume_after_ms: Option<i64>,
    /// Layer 1: cumulative realised PnL since daily reset (in cents)
    daily_pnl_cents: f64,
    /// Layer 1: peak cumulative PnL since daily reset
    daily_peak_pnl_cents: f64,
    /// Layer 1: timestamp of daily reset
    daily_reset_ts_ms: i64,
    /// Layer 2: rolling count of parity_band_exhausted in exec-quality window
    parity_exhausted_count: u32,
    /// Layer 2: rolling count of total entry attempts in exec-quality window
    entry_attempt_count: u32,
    /// Layer 2: timestamp window start for execution quality
    exec_quality_window_start_ms: i64,
    /// Layer 2: separate tracking for liquidity-empty failures (no orders found, book empty)
    /// These are market microstructure issues, NOT system failures - don't trigger halt
    liquidity_empty_count: u32,
    /// Layer 2: separate tracking for infrastructure failures (timeout, 5xx, network)
    /// These are transient issues - don't trigger halt
    infra_error_count: u32,
    /// Layer 3: rolling trade outcomes (true=win, false=loss) - last 50 trades
    rolling_outcomes: VecDeque<bool>,
}

impl LiveKillSwitchState {
    fn new(now_ms: i64) -> Self {
        Self {
            halted_since_ms: None,
            halt_reason: None,
            resume_after_ms: None,
            daily_pnl_cents: 0.0,
            daily_peak_pnl_cents: 0.0,
            daily_reset_ts_ms: now_ms,
            parity_exhausted_count: 0,
            entry_attempt_count: 0,
            exec_quality_window_start_ms: now_ms,
            liquidity_empty_count: 0,
            infra_error_count: 0,
            rolling_outcomes: VecDeque::with_capacity(50),
        }
    }

    fn is_halted(&self, now_ms: i64) -> bool {
        match (self.halted_since_ms, self.resume_after_ms) {
            (Some(_), Some(resume)) => now_ms < resume,
            (Some(_), None) => true,
            _ => false,
        }
    }

    fn halt(&mut self, now_ms: i64, reason: &str, resume_after_ms: Option<i64>) {
        self.halted_since_ms = Some(now_ms);
        self.halt_reason = Some(reason.to_string());
        self.resume_after_ms = resume_after_ms;
        tracing::warn!(
            reason = reason,
            resume_after_ms = resume_after_ms,
            "live kill switch activated"
        );
    }

    fn record_trade_outcome(&mut self, net_pnl_cents: f64) {
        let win = net_pnl_cents > 0.0;
        self.rolling_outcomes.push_back(win);
        while self.rolling_outcomes.len() > 50 {
            self.rolling_outcomes.pop_front();
        }
        self.daily_pnl_cents += net_pnl_cents;
        if self.daily_pnl_cents > self.daily_peak_pnl_cents {
            self.daily_peak_pnl_cents = self.daily_pnl_cents;
        }
    }

    fn rolling_win_rate(&self, min_trades: usize) -> Option<f64> {
        if self.rolling_outcomes.len() < min_trades {
            return None;
        }
        let wins = self.rolling_outcomes.iter().filter(|&&w| w).count();
        Some(wins as f64 / self.rolling_outcomes.len() as f64)
    }

    fn daily_drawdown_cents(&self) -> f64 {
        (self.daily_peak_pnl_cents - self.daily_pnl_cents).max(0.0)
    }

    fn daily_loss_cents(&self) -> f64 {
        (-self.daily_pnl_cents).max(0.0)
    }

    fn parity_exhaustion_rate(&self) -> f64 {
        if self.entry_attempt_count == 0 {
            return 0.0;
        }
        self.parity_exhausted_count as f64 / self.entry_attempt_count as f64
    }

    /// Record an entry attempt with specific failure classification.
    /// - parity_exhausted: price exceeded parity band (systematic execution quality issue)
    /// - liquidity_empty: no orders found / one-sided book (market microstructure)
    /// - infra_error: timeout / 5xx / network (transient infrastructure)
    fn record_entry_attempt(&mut self, now_ms: i64, reason: EntryAttemptReason) {
        // Reset window after 10 minutes
        if now_ms - self.exec_quality_window_start_ms > 600_000 {
            self.parity_exhausted_count = 0;
            self.liquidity_empty_count = 0;
            self.infra_error_count = 0;
            self.entry_attempt_count = 0;
            self.exec_quality_window_start_ms = now_ms;
        }
        self.entry_attempt_count = self.entry_attempt_count.saturating_add(1);
        match reason {
            EntryAttemptReason::ParityExhausted => {
                self.parity_exhausted_count = self.parity_exhausted_count.saturating_add(1);
            }
            EntryAttemptReason::LiquidityEmpty => {
                self.liquidity_empty_count = self.liquidity_empty_count.saturating_add(1);
            }
            EntryAttemptReason::InfraError => {
                self.infra_error_count = self.infra_error_count.saturating_add(1);
            }
            EntryAttemptReason::Success => {
                // No counter increment - success is implicit in the rate calculation
            }
        }
    }

    fn reset_daily(&mut self, now_ms: i64) {
        self.daily_pnl_cents = 0.0;
        self.daily_peak_pnl_cents = 0.0;
        self.daily_reset_ts_ms = now_ms;
    }
}

fn ks_daily_loss_limit_cents() -> f64 {
    std::env::var("FORGE_KS_DAILY_LOSS_LIMIT_CENTS")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(200.0)
        .max(10.0)
}

fn ks_daily_drawdown_limit_cents() -> f64 {
    std::env::var("FORGE_KS_DAILY_DRAWDOWN_LIMIT_CENTS")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(150.0)
        .max(10.0)
}

fn ks_parity_exhaustion_rate_limit() -> f64 {
    std::env::var("FORGE_KS_PARITY_EXHAUSTION_RATE")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(0.55) // Increased from 0.40 - now only counts true parity failures
        .clamp(0.1, 0.95)
}

fn ks_min_rolling_win_rate() -> f64 {
    std::env::var("FORGE_KS_MIN_ROLLING_WIN_RATE")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(0.42)
        .clamp(0.1, 0.8)
}

fn ks_win_rate_min_trades() -> usize {
    std::env::var("FORGE_KS_WIN_RATE_MIN_TRADES")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(20)
        .clamp(5, 200)
}

fn ks_halt_resume_ms() -> i64 {
    std::env::var("FORGE_KS_HALT_RESUME_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(10 * 60_000) // 10 minutes - reduced from 4 hours (now halts are more precise)
        .clamp(60_000, 24 * 3_600_000)
}

#[derive(Clone)]
struct RuntimeEventSampleBuffer {
    updated_at_ms: i64,
    samples: VecDeque<StrategySample>,
}

const CHART_CACHE_TTL_MS: u64 = 900;
const CHART_CACHE_MAX_ENTRIES: usize = 120;
const LIVE_DECISION_GUARD_TTL_MS: i64 = 45_000;
const LIVE_EVENT_LOG_MAX_DEFAULT: usize = 4_000;
const LIVE_EVENT_LOG_MAX_MIN: usize = 200;
const LIVE_EVENT_LOG_MAX_MAX: usize = 20_000;
const LIVE_EVENT_LOG_TTL_SEC_DEFAULT: u32 = 7 * 24 * 3600;
const LIVE_EVENT_LOG_TTL_SEC_MIN: u32 = 3600;
const LIVE_EVENT_LOG_TTL_SEC_MAX: u32 = 30 * 24 * 3600;
const LIVE_RUST_BOOK_CACHE_TTL_MS: u64 = 260;
const LIVE_RECENT_INTENT_BUFFER_MAX_DEFAULT: usize = 24;
const LIVE_RECENT_INTENT_BUFFER_MAX_MIN: usize = 4;
const LIVE_RECENT_INTENT_BUFFER_MAX_MAX: usize = 256;
const LIVE_RECENT_INTENT_BUFFER_TTL_MS_DEFAULT: i64 = 4_000;
const LIVE_RECENT_INTENT_BUFFER_TTL_MS_MIN: i64 = 1_000;
const LIVE_RECENT_INTENT_BUFFER_TTL_MS_MAX: i64 = 15_000;
const LIVE_RUST_BOOK_CACHE_MAX: usize = 512;
const LIVE_ALERT_THROTTLE_DEFAULT_MS: i64 = 5 * 60_000;
const RUNTIME_EVENT_SAMPLE_BUFFER_MAX_DEFAULT: usize = 140_000;
const RUNTIME_EVENT_SAMPLE_BUFFER_MAX_MIN: usize = 10_000;
const RUNTIME_EVENT_SAMPLE_BUFFER_MAX_MAX: usize = 600_000;

fn default_runtime_symbol() -> String {
    "BTCUSDT".to_string()
}

fn normalize_runtime_symbol(raw: &str) -> Option<String> {
    match raw.trim().to_ascii_uppercase().as_str() {
        "BTC" | "BTCUSDT" | "XBT" => Some("BTCUSDT".to_string()),
        "ETH" | "ETHUSDT" | "ETHER" => Some("ETHUSDT".to_string()),
        "SOL" | "SOLUSDT" => Some("SOLUSDT".to_string()),
        "XRP" | "XRPUSDT" | "RIPPLE" => Some("XRPUSDT".to_string()),
        _ => None,
    }
}

fn runtime_scope_key(symbol: &str, market_type: &str) -> String {
    format!(
        "{}|{}",
        symbol.trim().to_ascii_uppercase(),
        market_type.trim().to_ascii_lowercase()
    )
}

fn runtime_scope_symbol(symbol: &str) -> String {
    normalize_runtime_symbol(symbol).unwrap_or_else(default_runtime_symbol)
}

fn runtime_event_sample_scope_key(symbol: &str, market_type: &str) -> String {
    runtime_scope_key(symbol, market_type)
}

fn runtime_event_sample_buffer_max() -> usize {
    std::env::var("FORGE_STRATEGY_RUNTIME_EVENT_BUFFER_MAX_POINTS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(RUNTIME_EVENT_SAMPLE_BUFFER_MAX_DEFAULT)
        .clamp(
            RUNTIME_EVENT_SAMPLE_BUFFER_MAX_MIN,
            RUNTIME_EVENT_SAMPLE_BUFFER_MAX_MAX,
        )
}

fn live_recent_intent_buffer_max() -> usize {
    std::env::var("FORGE_FEV1_RECENT_INTENT_BUFFER_MAX")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(LIVE_RECENT_INTENT_BUFFER_MAX_DEFAULT)
        .clamp(
            LIVE_RECENT_INTENT_BUFFER_MAX_MIN,
            LIVE_RECENT_INTENT_BUFFER_MAX_MAX,
        )
}

fn live_recent_intent_buffer_ttl_ms() -> i64 {
    std::env::var("FORGE_FEV1_RECENT_INTENT_BUFFER_TTL_MS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(LIVE_RECENT_INTENT_BUFFER_TTL_MS_DEFAULT)
        .clamp(
            LIVE_RECENT_INTENT_BUFFER_TTL_MS_MIN,
            LIVE_RECENT_INTENT_BUFFER_TTL_MS_MAX,
        )
}

fn live_event_log_max() -> usize {
    std::env::var("FORGE_FEV1_LIVE_EVENT_LOG_MAX")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(LIVE_EVENT_LOG_MAX_DEFAULT)
        .clamp(LIVE_EVENT_LOG_MAX_MIN, LIVE_EVENT_LOG_MAX_MAX)
}

fn live_event_log_ttl_sec() -> u32 {
    std::env::var("FORGE_FEV1_LIVE_EVENT_LOG_TTL_SEC")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(LIVE_EVENT_LOG_TTL_SEC_DEFAULT)
        .clamp(LIVE_EVENT_LOG_TTL_SEC_MIN, LIVE_EVENT_LOG_TTL_SEC_MAX)
}

fn extract_order_id_from_live_event(event: &Value) -> Option<String> {
    event
        .get("order_id")
        .and_then(Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            event.get("response").and_then(|resp| {
                resp.get("order_id")
                    .and_then(Value::as_str)
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
            })
        })
        .or_else(|| {
            event.get("event").and_then(|ev| {
                ev.get("order_id")
                    .and_then(Value::as_str)
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
            })
        })
}

fn classify_live_event_type(event: &Value) -> &'static str {
    let reason = event
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let action = event
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if reason.contains("no_live_market_target") {
        "target_miss"
    } else if reason.contains("pending_confirm")
        || reason.contains("submit")
        || reason.contains("decision_to_payload_failed")
    {
        "submit"
    } else if reason.contains("terminal")
        || reason.contains("filled")
        || reason.contains("canceled")
        || reason.contains("cancelled")
    {
        "terminal"
    } else if reason.contains("timeout") {
        "timeout"
    } else if reason.contains("resubmit") || reason.contains("retry") {
        "retry"
    } else if action == "none" {
        "state"
    } else {
        "execution"
    }
}

#[derive(Debug, Clone)]
struct RustExecutorConfig {
    host: String,
    private_key: String,
    chain_id: u64,
    signature_type: PmSignatureType,
    funder: Option<PmAddress>,
    credentials: Option<PmCredentials>,
    nonce: Option<u32>,
}

impl RustExecutorConfig {
    fn from_env() -> Result<Self, String> {
        let host = std::env::var("FORGE_FEV1_CLOB_HOST")
            .ok()
            .map(|v| v.trim().trim_end_matches('/').to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "https://clob.polymarket.com".to_string());
        let private_key = std::env::var("FORGE_FEV1_PRIVATE_KEY")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| "FORGE_FEV1_PRIVATE_KEY missing".to_string())?;
        let chain_id = std::env::var("FORGE_FEV1_CHAIN_ID")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(POLYGON);
        let signature_type = std::env::var("FORGE_FEV1_SIGNATURE_TYPE")
            .ok()
            .map(|v| match v.trim().to_ascii_lowercase().as_str() {
                "proxy" => PmSignatureType::Proxy,
                "gnosissafe" | "safe" | "gnosis_safe" => PmSignatureType::GnosisSafe,
                _ => PmSignatureType::Eoa,
            })
            .unwrap_or(PmSignatureType::Eoa);
        let funder = std::env::var("FORGE_FEV1_FUNDER").ok().and_then(|v| {
            let t = v.trim();
            if t.is_empty() {
                None
            } else {
                PmAddress::from_str(t).ok()
            }
        });
        let nonce = std::env::var("FORGE_FEV1_NONCE")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok());
        let credentials = {
            let key_raw = std::env::var("FORGE_FEV1_API_KEY").ok();
            let sec_raw = std::env::var("FORGE_FEV1_API_SECRET").ok();
            let pass_raw = std::env::var("FORGE_FEV1_API_PASSPHRASE").ok();
            match (key_raw, sec_raw, pass_raw) {
                (Some(key), Some(sec), Some(pass)) => {
                    let key = Uuid::parse_str(key.trim())
                        .map_err(|e| format!("invalid FORGE_FEV1_API_KEY: {e}"))?;
                    Some(PmCredentials::new(
                        key,
                        sec.trim().to_string(),
                        pass.trim().to_string(),
                    ))
                }
                _ => None,
            }
        };
        Ok(Self {
            host,
            private_key,
            chain_id,
            signature_type,
            funder,
            credentials,
            nonce,
        })
    }
}

struct RustExecutorContext {
    client: PmClient<PmAuthenticated<PmNormal>>,
    signer: Box<dyn PmSigner + Send + Sync>,
}

#[derive(Clone)]
struct RustBookCacheEntry {
    fetched_at: Instant,
    snapshot: GatewayBookSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LivePositionState {
    #[serde(default = "default_runtime_symbol")]
    symbol: String,
    market_type: String,
    state: String,
    side: Option<String>,
    entry_round_id: Option<String>,
    #[serde(default)]
    entry_market_id: Option<String>,
    #[serde(default)]
    entry_token_id: Option<String>,
    entry_ts_ms: Option<i64>,
    entry_price_cents: Option<f64>,
    entry_quote_usdc: Option<f64>,
    #[serde(default)]
    net_quote_usdc: f64,
    #[serde(default)]
    vwap_entry_cents: Option<f64>,
    last_action: Option<String>,
    last_reason: Option<String>,
    total_entries: u64,
    total_exits: u64,
    #[serde(default)]
    total_adds: u64,
    #[serde(default)]
    open_add_layers: u32,
    #[serde(default)]
    total_reduces: u64,
    #[serde(default)]
    realized_pnl_usdc: f64,
    #[serde(default)]
    last_fill_pnl_usdc: f64,
    #[serde(default)]
    position_cost_usdc: f64,
    #[serde(default)]
    position_size_shares: f64,
    updated_ts_ms: i64,
}

impl LivePositionState {
    fn flat(symbol: &str, market_type: &str, now_ms: i64) -> Self {
        Self {
            symbol: symbol.to_ascii_uppercase(),
            market_type: market_type.to_string(),
            state: "flat".to_string(),
            side: None,
            entry_round_id: None,
            entry_market_id: None,
            entry_token_id: None,
            entry_ts_ms: None,
            entry_price_cents: None,
            entry_quote_usdc: None,
            net_quote_usdc: 0.0,
            vwap_entry_cents: None,
            last_action: None,
            last_reason: None,
            total_entries: 0,
            total_exits: 0,
            total_adds: 0,
            open_add_layers: 0,
            total_reduces: 0,
            realized_pnl_usdc: 0.0,
            last_fill_pnl_usdc: 0.0,
            position_cost_usdc: 0.0,
            position_size_shares: 0.0,
            updated_ts_ms: now_ms,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LivePendingOrder {
    #[serde(default = "default_runtime_symbol")]
    symbol: String,
    market_type: String,
    order_id: String,
    #[serde(default)]
    market_id: String,
    #[serde(default)]
    token_id: String,
    action: String,
    side: String,
    round_id: String,
    decision_key: String,
    #[serde(default)]
    intent_id: String,
    #[serde(default)]
    attempt_id: String,
    #[serde(default)]
    decision_id: String,
    price_cents: f64,
    quote_size_usdc: f64,
    #[serde(default)]
    order_size_shares: f64,
    #[serde(default)]
    tif: String,
    #[serde(default)]
    style: String,
    #[serde(default)]
    submit_reason: String,
    submitted_ts_ms: i64,
    #[serde(default)]
    ack_ts_ms: i64,
    #[serde(default)]
    decision_ts_ms: i64,
    #[serde(default)]
    trigger_ts_ms: i64,
    cancel_after_ms: i64,
    #[serde(default)]
    cancel_due_at_ms: i64,
    #[serde(default)]
    terminal_due_at_ms: i64,
    retry_count: u8,
    #[serde(default)]
    size_locked: bool,
    #[serde(default)]
    accepted_trade_ids: Vec<String>,
}

#[derive(Debug, Clone)]
struct LiveRuntimeConfig {
    enabled: bool,
    live_execute: bool,
    loop_interval_ms: u64,
    lookback_minutes: u32,
    max_points: u32,
    max_trades: usize,
    max_orders: usize,
    drain_only: bool,
    quote_usdc: f64,
    symbols: Vec<String>,
    symbol: String,
    markets: Vec<String>,
    scoped_markets: HashMap<String, HashSet<String>>,
}

impl LiveRuntimeConfig {
    fn from_env() -> Self {
        let enabled = std::env::var("FORGE_FEV1_RUNTIME_ENABLED")
            .ok()
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(true);
        let live_execute = std::env::var("FORGE_FEV1_LIVE_EXECUTE")
            .ok()
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);
        let loop_interval_ms = std::env::var("FORGE_FEV1_RUNTIME_LOOP_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(40) // OPTIMIZED: 40ms for ultra-fast event-driven loop
            .clamp(10, 10_000);
        let lookback_minutes = std::env::var("FORGE_FEV1_RUNTIME_LOOKBACK_MINUTES")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(360)
            .clamp(30, 24 * 60 * 7);
        let max_points = std::env::var("FORGE_FEV1_RUNTIME_MAX_POINTS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(140_000)
            .clamp(20_000, 1_000_000);
        // Runtime trade cap semantics:
        // - 0 => unbounded by trade count (bounded by lookback/max_points instead)
        // - N > 0 => keep only latest N trades in simulation summary payload
        let max_trades = std::env::var("FORGE_FEV1_RUNTIME_MAX_TRADES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .map(|v| {
                if v == 0 {
                    usize::MAX
                } else {
                    v.clamp(1, 20_000)
                }
            })
            .unwrap_or(120);
        let max_orders = std::env::var("FORGE_FEV1_RUNTIME_MAX_ORDERS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1)
            .clamp(1, 6);
        let drain_only = std::env::var("FORGE_FEV1_RUNTIME_DRAIN_ONLY")
            .ok()
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);
        let quote_usdc = std::env::var("FORGE_FEV1_RUNTIME_QUOTE_USDC")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(1.0)
            .clamp(0.01, 500.0);
        let mut symbols = Vec::<String>::new();
        if let Some(symbol) = std::env::var("FORGE_FEV1_RUNTIME_SYMBOL")
            .ok()
            .and_then(|raw| normalize_runtime_symbol(&raw))
        {
            symbols.push(symbol);
        }
        if let Ok(raw) = std::env::var("FORGE_FEV1_RUNTIME_SYMBOLS") {
            for symbol in raw.split(',').filter_map(normalize_runtime_symbol) {
                if !symbols
                    .iter()
                    .any(|existing| existing.eq_ignore_ascii_case(&symbol))
                {
                    symbols.push(symbol);
                }
            }
        }
        if symbols.is_empty() {
            symbols.push(default_runtime_symbol());
        }
        let symbol = symbols
            .first()
            .cloned()
            .unwrap_or_else(default_runtime_symbol);
        let markets = std::env::var("FORGE_FEV1_RUNTIME_MARKETS")
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(|v| v.trim().to_ascii_lowercase())
                    .filter(|v| v == "5m" || v == "15m")
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| vec!["5m".to_string()]);
        let mut scoped_markets = HashMap::<String, HashSet<String>>::new();
        if let Ok(raw) = std::env::var("FORGE_FEV1_RUNTIME_SYMBOL_TIMEFRAMES") {
            for scope in raw.split(',').map(str::trim).filter(|v| !v.is_empty()) {
                let Some((raw_symbol, raw_markets)) = scope.split_once(':') else {
                    continue;
                };
                let Some(symbol) = normalize_runtime_symbol(raw_symbol) else {
                    continue;
                };
                for market in raw_markets
                    .split('|')
                    .map(|v| v.trim().to_ascii_lowercase())
                    .filter(|v| v == "5m" || v == "15m")
                {
                    scoped_markets
                        .entry(symbol.clone())
                        .or_default()
                        .insert(market);
                }
            }
        }
        Self {
            enabled,
            live_execute,
            loop_interval_ms,
            lookback_minutes,
            max_points,
            max_trades,
            max_orders,
            drain_only,
            quote_usdc,
            symbols,
            symbol,
            markets,
            scoped_markets,
        }
    }
}

fn runtime_scope_pairs(cfg: &LiveRuntimeConfig) -> Vec<(String, String)> {
    let mut scopes = Vec::new();
    for symbol in &cfg.symbols {
        let allowed_markets = cfg.scoped_markets.get(symbol);
        for market in &cfg.markets {
            if let Some(allowed) = allowed_markets {
                if !allowed.contains(market) {
                    continue;
                }
            }
            scopes.push((symbol.clone(), market.clone()));
        }
    }
    scopes
}

#[derive(Debug, Clone)]
struct LiveRuntimeStrategyEngineState {
    symbol: String,
    cfg: StrategyRuntimeConfig,
    max_trades: usize,
    samples: Arc<Vec<StrategySample>>,
    engine: fev1::IncrementalSimulationEngine,
}

impl LiveRuntimeStrategyEngineState {
    fn bootstrap(
        symbol: &str,
        cfg: StrategyRuntimeConfig,
        max_trades: usize,
        samples: Arc<Vec<StrategySample>>,
    ) -> Self {
        let mapped = map_samples_to_fev1(&samples);
        let engine = fev1::IncrementalSimulationEngine::from_samples(
            &mapped,
            map_cfg_to_fev1(&cfg),
            max_trades,
            None,
        );
        Self {
            symbol: symbol.to_ascii_uppercase(),
            cfg,
            max_trades,
            samples,
            engine,
        }
    }

    fn can_reuse(
        &self,
        symbol: &str,
        cfg: &StrategyRuntimeConfig,
        max_trades: usize,
        samples: &[StrategySample],
    ) -> bool {
        if !self.symbol.eq_ignore_ascii_case(symbol)
            || self.cfg != *cfg
            || self.max_trades != max_trades
        {
            return false;
        }
        if samples.is_empty() || self.samples.is_empty() {
            return false;
        }
        if self.samples[0].ts_ms != samples[0].ts_ms {
            return false;
        }
        if samples.len() < self.samples.len() {
            return false;
        }
        match (self.samples.last(), samples.last()) {
            (Some(prev_last), Some(last)) if samples.len() == self.samples.len() => {
                prev_last == last
            }
            (Some(prev_last), Some(_)) if self.samples.len() <= samples.len() => {
                samples.get(self.samples.len().saturating_sub(1)) == Some(prev_last)
            }
            _ => false,
        }
    }

    fn update(&mut self, samples: Arc<Vec<StrategySample>>) {
        if samples.len() > self.samples.len() {
            let new_mapped = map_samples_to_fev1(&samples[self.samples.len()..]);
            for sample in &new_mapped {
                self.engine.apply_sample(sample);
            }
        }
        self.samples = samples;
    }
}

fn runtime_fast_loop_enabled() -> bool {
    std::env::var("FORGE_FEV1_RUNTIME_FAST_ENABLED")
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(true)
}

fn runtime_fast_loop_ms(base_loop_ms: u64) -> u64 {
    std::env::var("FORGE_FEV1_RUNTIME_FAST_LOOP_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(40)
        .clamp(20, base_loop_ms.max(20))
}

fn runtime_fast_margin_threshold() -> f64 {
    std::env::var("FORGE_FEV1_RUNTIME_FAST_MARGIN")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.10)
        .clamp(0.01, 0.50)
}

fn runtime_target_prewarm_ms() -> i64 {
    std::env::var("FORGE_FEV1_RUNTIME_TARGET_PREWARM_MS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(10_000) // OPTIMIZED: Reduced from 40s to 10s for more frequent prewarming
        .clamp(5_000, 120_000)
}

fn runtime_target_keepalive_ms() -> i64 {
    std::env::var("FORGE_FEV1_RUNTIME_TARGET_KEEPALIVE_MS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(2_500)
        .clamp(500, 30_000)
}

fn runtime_event_idle_poll_ms() -> i64 {
    std::env::var("FORGE_FEV1_RUNTIME_EVENT_IDLE_POLL_MS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(1_200)
        .clamp(200, 20_000)
}

fn runtime_env_refresh_ms() -> i64 {
    std::env::var("FORGE_FEV1_RUNTIME_ENV_REFRESH_MS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(1_000)
        .clamp(100, 10_000)
}

fn live_submit_arm_required() -> bool {
    std::env::var("FORGE_FEV1_LIVE_ARM_REQUIRED")
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(true)
}

fn live_submit_armed() -> bool {
    std::env::var("FORGE_FEV1_LIVE_ARMED")
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn live_submit_effective_armed() -> bool {
    !live_submit_arm_required() || live_submit_armed()
}

fn live_hard_kill_enabled() -> bool {
    std::env::var("FORGE_FEV1_LIVE_HARD_KILL")
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn normalize_runtime_signal_decisions(
    market_type: &str,
    decisions: Vec<Value>,
    quote_usdc: f64,
) -> Vec<Value> {
    let default_quote = quote_usdc.max(0.01);
    decisions
        .into_iter()
        .filter_map(|mut decision| {
            let obj = decision.as_object_mut()?;
            if let Some(action) = obj
                .get("action")
                .and_then(Value::as_str)
                .map(|v| v.to_ascii_lowercase())
            {
                obj.insert("action".to_string(), json!(action));
            }
            if let Some(side) = obj
                .get("side")
                .and_then(Value::as_str)
                .map(|v| v.to_ascii_uppercase())
            {
                obj.insert("side".to_string(), json!(side));
            }
            if obj
                .get("price_cents")
                .and_then(Value::as_f64)
                .filter(|v| v.is_finite())
                .is_none()
            {
                if let Some(signal_price) = obj.get("signal_price_cents").cloned() {
                    obj.insert("price_cents".to_string(), signal_price);
                }
            }
            let quote_ok = obj
                .get("quote_size_usdc")
                .and_then(Value::as_f64)
                .map(|v| v > 0.0)
                .unwrap_or(false);
            if !quote_ok {
                obj.insert("quote_size_usdc".to_string(), json!(default_quote));
            }

            let has_intent_id = obj
                .get("intent_id")
                .and_then(Value::as_str)
                .map(|v| !v.trim().is_empty())
                .unwrap_or(false);
            let has_decision_id = obj
                .get("decision_id")
                .and_then(Value::as_str)
                .map(|v| !v.trim().is_empty())
                .unwrap_or(false);
            if !has_decision_id {
                if let Some(intent_id) = obj.get("intent_id").cloned() {
                    obj.insert("decision_id".to_string(), intent_id);
                } else {
                    let action = obj
                        .get("action")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                        .to_ascii_lowercase();
                    let side = obj
                        .get("side")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                        .to_ascii_uppercase();
                    let round_id = obj.get("round_id").and_then(Value::as_str).unwrap_or("na");
                    let ts_ms = obj.get("ts_ms").and_then(Value::as_i64).unwrap_or(0);
                    obj.insert(
                        "decision_id".to_string(),
                        Value::String(format!(
                            "fev1:{}:{}:{}:{}:{}",
                            market_type, action, side, round_id, ts_ms
                        )),
                    );
                }
            }
            if !has_intent_id {
                if let Some(decision_id) = obj.get("decision_id").cloned() {
                    obj.insert("intent_id".to_string(), decision_id);
                }
            }
            if obj.get("signal_source").is_none() {
                obj.insert("signal_source".to_string(), json!("signal_decision"));
            }
            let action = obj
                .get("action")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_ascii_lowercase();
            if action == "enter" && obj.get("paper_entry_exec_price_cents").is_none() {
                if let Some(v) = obj.get("paper_expected_exec_price_cents").cloned() {
                    obj.insert("paper_entry_exec_price_cents".to_string(), v);
                }
            }
            if action == "exit" && obj.get("paper_exit_exec_price_cents").is_none() {
                if let Some(v) = obj.get("paper_expected_exec_price_cents").cloned() {
                    obj.insert("paper_exit_exec_price_cents".to_string(), v);
                }
            }
            Some(decision)
        })
        .collect()
}

fn normalize_current_active_signal(
    payload: &Value,
    market_type: &str,
    quote_usdc: f64,
) -> Option<Value> {
    let decision = payload
        .get("current")
        .and_then(|v| v.get("active_signal"))
        .cloned()?;
    let mut normalized =
        normalize_runtime_signal_decisions(market_type, vec![decision], quote_usdc)
            .into_iter()
            .next()?;
    if let Some(obj) = normalized.as_object_mut() {
        obj.insert("signal_source".to_string(), json!("active_signal"));
    }
    Some(normalized)
}

fn decision_effective_expires_at_ms(decision: &Value) -> i64 {
    decision
        .get("expires_at_ms")
        .and_then(Value::as_i64)
        .unwrap_or(i64::MAX)
}

fn select_live_execution_candidates(
    active_signal: Option<Value>,
    recent_intents: Vec<Value>,
    latest_ts_ms: i64,
    max_orders: usize,
    drain_only: bool,
    prefer_action: Option<&str>,
) -> (Vec<Value>, usize, &'static str, bool) {
    let current_entry_available = active_signal
        .as_ref()
        .map(|signal| {
            let action = signal
                .get("action")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_ascii_lowercase();
            action == "enter" && decision_effective_expires_at_ms(signal) > latest_ts_ms
        })
        .unwrap_or(false);

    let mut staged = Vec::<(Value, &'static str)>::new();
    if let Some(signal) = active_signal {
        staged.push((signal, "active_signal"));
    }
    for signal in recent_intents {
        staged.push((signal, "recent_intent_buffer"));
    }

    let mut fresh_count = 0usize;
    let mut selected = Vec::<Value>::new();
    let mut candidate_source = "none";
    let mut saw_expired_active = false;
    let mut saw_drain_reject = false;
    let mut saw_prefer_mismatch = false;
    let mut seen_intents = HashSet::<String>::new();

    for (signal, source) in staged {
        let intent_id = signal
            .get("intent_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if !intent_id.is_empty() && !seen_intents.insert(intent_id) {
            continue;
        }
        let expires_at_ms = decision_effective_expires_at_ms(&signal);
        if expires_at_ms <= latest_ts_ms {
            if source == "active_signal" {
                saw_expired_active = true;
            }
            continue;
        }
        fresh_count = fresh_count.saturating_add(1);
        let action = signal
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        if drain_only && action != "exit" {
            if source == "active_signal" {
                saw_drain_reject = true;
            }
            continue;
        }
        if let Some(prefer) = prefer_action {
            if action != prefer.to_ascii_lowercase() {
                if source == "active_signal" {
                    saw_prefer_mismatch = true;
                }
                continue;
            }
        }
        selected.push(signal);
        candidate_source = source;
        break;
    }

    if !selected.is_empty() {
        return (
            selected,
            min(max_orders.max(1), 1),
            candidate_source,
            current_entry_available,
        );
    }

    let no_candidate_reason = if saw_expired_active {
        "active_signal_expired"
    } else if saw_drain_reject {
        "active_signal_drain_only"
    } else if saw_prefer_mismatch {
        "active_signal_prefer_action_mismatch"
    } else {
        "none"
    };
    (
        Vec::new(),
        fresh_count,
        no_candidate_reason,
        current_entry_available,
    )
}

fn decision_action_count(decisions: &[Value], action: &str) -> usize {
    decisions
        .iter()
        .filter(|d| {
            d.get("action")
                .and_then(Value::as_str)
                .map(|a| a.eq_ignore_ascii_case(action))
                .unwrap_or(false)
        })
        .count()
}

fn decision_action_count_from_orders(orders: &[Value], action: &str) -> usize {
    orders
        .iter()
        .filter(|row| {
            row.get("decision")
                .and_then(|d| d.get("action"))
                .and_then(Value::as_str)
                .map(|a| a.eq_ignore_ascii_case(action))
                .unwrap_or(false)
        })
        .count()
}

fn collect_latency_values_ms(rows: &[Value], key: &str) -> Vec<f64> {
    rows.iter()
        .filter_map(|row| row.get(key).and_then(Value::as_f64))
        .filter(|v| v.is_finite() && *v >= 0.0)
        .collect()
}

fn latency_percentile_ms(sorted_samples: &[f64], p: f64) -> Option<f64> {
    if sorted_samples.is_empty() {
        return None;
    }
    let p = p.clamp(0.0, 1.0);
    let rank = ((sorted_samples.len() - 1) as f64 * p).round() as usize;
    sorted_samples.get(rank).copied()
}

fn latency_stats_json(samples: &[f64]) -> Value {
    if samples.is_empty() {
        return json!({
            "count": 0,
            "avg_ms": Value::Null,
            "p50_ms": Value::Null,
            "p95_ms": Value::Null,
            "max_ms": Value::Null
        });
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let avg = sorted.iter().sum::<f64>() / sorted.len() as f64;
    json!({
        "count": sorted.len(),
        "avg_ms": avg,
        "p50_ms": latency_percentile_ms(&sorted, 0.50),
        "p95_ms": latency_percentile_ms(&sorted, 0.95),
        "max_ms": sorted.last().copied()
    })
}

fn summarize_live_order_latency(orders: &[Value]) -> Value {
    let signal_to_trigger = collect_latency_values_ms(orders, "signal_to_trigger_ms");
    let trigger_to_submit = collect_latency_values_ms(orders, "trigger_to_submit_ms");
    let signal_to_submit = collect_latency_values_ms(orders, "signal_to_submit_ms");
    let signal_to_ack = collect_latency_values_ms(orders, "signal_to_ack_ms");
    let submit_to_ack = collect_latency_values_ms(orders, "submit_to_ack_ms");
    let order_latency = collect_latency_values_ms(orders, "order_latency_ms");
    json!({
        "signal_to_trigger_ms": latency_stats_json(&signal_to_trigger),
        "trigger_to_submit_ms": latency_stats_json(&trigger_to_submit),
        "signal_to_submit_ms": latency_stats_json(&signal_to_submit),
        "signal_to_ack_ms": latency_stats_json(&signal_to_ack),
        "submit_to_ack_ms": latency_stats_json(&submit_to_ack),
        "order_latency_ms": latency_stats_json(&order_latency),
    })
}

fn collect_price_trace_delta_values_cents(orders: &[Value], key: &str) -> Vec<f64> {
    orders
        .iter()
        .filter_map(|row| row.get("price_trace"))
        .filter_map(|trace| trace.get(key).and_then(Value::as_f64))
        .filter(|v| v.is_finite())
        .collect()
}

fn price_drift_stats_json(signed_samples: &[f64]) -> Value {
    if signed_samples.is_empty() {
        return json!({
            "count": 0,
            "avg_abs_cents": Value::Null,
            "p50_abs_cents": Value::Null,
            "p95_abs_cents": Value::Null,
            "max_abs_cents": Value::Null,
            "avg_signed_cents": Value::Null
        });
    }
    let mut abs_values = signed_samples.iter().map(|v| v.abs()).collect::<Vec<_>>();
    abs_values.sort_by(|a, b| a.total_cmp(b));
    let avg_abs = abs_values.iter().sum::<f64>() / abs_values.len() as f64;
    let avg_signed = signed_samples.iter().sum::<f64>() / signed_samples.len() as f64;
    json!({
        "count": abs_values.len(),
        "avg_abs_cents": avg_abs,
        "p50_abs_cents": latency_percentile_ms(&abs_values, 0.50),
        "p95_abs_cents": latency_percentile_ms(&abs_values, 0.95),
        "max_abs_cents": abs_values.last().copied(),
        "avg_signed_cents": avg_signed
    })
}

fn summarize_live_price_parity(orders: &[Value]) -> Value {
    let signal_vs_submit = collect_price_trace_delta_values_cents(orders, "signal_vs_submit_cents");
    let signal_vs_accepted =
        collect_price_trace_delta_values_cents(orders, "signal_vs_accepted_cents");
    let parity_anchor_vs_submit =
        collect_price_trace_delta_values_cents(orders, "parity_anchor_vs_submit_cents");
    let parity_anchor_vs_accepted =
        collect_price_trace_delta_values_cents(orders, "parity_anchor_vs_accepted_cents");
    let paper_exec_vs_submit =
        collect_price_trace_delta_values_cents(orders, "paper_exec_vs_submit_cents");
    let paper_exec_vs_accepted =
        collect_price_trace_delta_values_cents(orders, "paper_exec_vs_accepted_cents");
    // MRG: Model-Reality Gap — collected from price_trace.mrg_cents per order
    let mrg_values: Vec<f64> = orders
        .iter()
        .filter_map(|order| {
            order
                .get("price_trace")
                .and_then(|pt| pt.get("mrg_cents"))
                .and_then(Value::as_f64)
                .filter(|v| v.is_finite())
        })
        .collect();
    let mrg_stats = price_drift_stats_json(&mrg_values);
    // Parity band exhaustion rate
    let total_entry_attempts = orders
        .iter()
        .filter(|o| {
            o.get("action")
                .or_else(|| o.get("decision").and_then(|d| d.get("action")))
                .and_then(Value::as_str)
                .map(|a| matches!(a.to_ascii_lowercase().as_str(), "enter" | "add"))
                .unwrap_or(false)
        })
        .count();
    let parity_exhausted_count = orders
        .iter()
        .filter(|o| {
            o.get("reason")
                .and_then(Value::as_str)
                .map(|r| r.contains("parity_band_exhausted"))
                .unwrap_or(false)
        })
        .count();
    let parity_exhaustion_rate = if total_entry_attempts > 0 {
        parity_exhausted_count as f64 / total_entry_attempts as f64
    } else {
        0.0
    };
    json!({
        "signal_vs_submit_cents": price_drift_stats_json(&signal_vs_submit),
        "signal_vs_accepted_cents": price_drift_stats_json(&signal_vs_accepted),
        "parity_anchor_vs_submit_cents": price_drift_stats_json(&parity_anchor_vs_submit),
        "parity_anchor_vs_accepted_cents": price_drift_stats_json(&parity_anchor_vs_accepted),
        "paper_exec_vs_submit_cents": price_drift_stats_json(&paper_exec_vs_submit),
        "paper_exec_vs_accepted_cents": price_drift_stats_json(&paper_exec_vs_accepted),
        // MRG summary — key canary validation metric
        "mrg_cents": mrg_stats,
        "mrg_sample_count": mrg_values.len(),
        // Execution quality
        "parity_exhaustion_rate": parity_exhaustion_rate,
        "parity_exhausted_count": parity_exhausted_count,
        "entry_attempt_count": total_entry_attempts,
    })
}

fn extract_decision_id(node: &Value) -> Option<String> {
    node.get("intent_id")
        .and_then(Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            node.get("decision").and_then(|decision| {
                decision
                    .get("intent_id")
                    .and_then(Value::as_str)
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
            })
        })
        .or_else(|| {
            node.get("decision_id")
                .and_then(Value::as_str)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .or_else(|| {
            node.get("decision").and_then(|decision| {
                decision
                    .get("decision_id")
                    .and_then(Value::as_str)
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
            })
        })
}

fn build_live_fill_decision_map(events: &[Value]) -> (HashMap<String, Value>, Value) {
    let mut by_decision = HashMap::<String, Value>::new();
    let mut fill_event_count = 0_usize;
    let mut realized_net_pnl_cents = 0.0_f64;
    let mut total_fill_cost_cents = 0.0_f64;

    for ev in events {
        let reason = ev
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        let is_fill = reason.contains("terminal_filled")
            || ev.get("fill_price_cents").and_then(Value::as_f64).is_some()
            || ev.get("fill_quote_usdc").and_then(Value::as_f64).is_some();
        if !is_fill {
            continue;
        }

        fill_event_count = fill_event_count.saturating_add(1);
        realized_net_pnl_cents += ev
            .get("fill_pnl_cents_net")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        total_fill_cost_cents += ev
            .get("fill_cost_cents")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);

        let Some(decision_id) = extract_decision_id(ev) else {
            continue;
        };
        let ts_ms = ev.get("ts_ms").and_then(Value::as_i64).unwrap_or(0);
        let fill_ts_ms = ev
            .get("fill_ts_ms")
            .and_then(Value::as_i64)
            .unwrap_or(ts_ms);
        let fill_size_shares = ev
            .get("fill_size_shares")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let fill_quote_usdc = ev
            .get("fill_quote_usdc")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let fill_fee_cents = ev
            .get("fill_fee_cents")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let actual_fee_cents = ev
            .get("actual_fee_cents")
            .and_then(Value::as_f64)
            .or_else(|| ev.get("fill_fee_cents").and_then(Value::as_f64))
            .unwrap_or(0.0);
        let fill_slippage_cents = ev
            .get("fill_slippage_cents")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let actual_slippage_cents = ev
            .get("actual_slippage_cents")
            .and_then(Value::as_f64)
            .or_else(|| ev.get("fill_slippage_cents").and_then(Value::as_f64))
            .unwrap_or(0.0);
        let fill_cost_cents = ev
            .get("fill_cost_cents")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let fill_pnl_cents_net = ev
            .get("fill_pnl_cents_net")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let fill_price_cents = ev
            .get("fill_price_cents")
            .and_then(Value::as_f64)
            .filter(|v| v.is_finite() && *v > 0.0);
        let actual_fill_price_cents = ev
            .get("actual_fill_price_cents")
            .and_then(Value::as_f64)
            .or(fill_price_cents)
            .filter(|v| v.is_finite() && *v > 0.0);
        let prev = by_decision.entry(decision_id.clone()).or_insert_with(|| {
            json!({
                "intent_id": ev.get("intent_id").cloned().unwrap_or_else(|| json!(decision_id.clone())),
                "decision_id": decision_id.clone(),
                "ts_ms": ts_ms,
                "fill_ts_ms": fill_ts_ms,
                "action": ev.get("action").cloned().unwrap_or(Value::Null),
                "side": ev.get("side").cloned().unwrap_or(Value::Null),
                "order_id": ev.get("order_id").cloned().unwrap_or(Value::Null),
                "order_ids": [],
                "fill_event_count": 0_u64,
                "fill_price_cents": Value::Null,
                "actual_fill_price_cents": Value::Null,
                "fill_quote_usdc": 0.0,
                "fill_size_shares": 0.0,
                "fill_fee_cents": 0.0,
                "actual_fee_cents": 0.0,
                "fill_slippage_cents": Value::Null,
                "actual_slippage_cents": Value::Null,
                "fill_cost_cents": 0.0,
                "fill_pnl_cents_net": 0.0,
                "ack_to_fill_ms": Value::Null,
                "submit_to_fill_ms": Value::Null,
                "signal_to_fill_ms": Value::Null,
                "trigger_to_fill_ms": Value::Null
            })
        });

        let Some(obj) = prev.as_object_mut() else {
            continue;
        };
        let prev_size = obj
            .get("fill_size_shares")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let total_size = prev_size + fill_size_shares.max(0.0);
        let prev_fill_count = obj
            .get("fill_event_count")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        obj.insert(
            "fill_event_count".to_string(),
            json!(prev_fill_count.saturating_add(1)),
        );
        obj.insert(
            "fill_quote_usdc".to_string(),
            json!(
                obj.get("fill_quote_usdc")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0)
                    + fill_quote_usdc.max(0.0)
            ),
        );
        obj.insert("fill_size_shares".to_string(), json!(total_size));
        obj.insert(
            "fill_fee_cents".to_string(),
            json!(
                obj.get("fill_fee_cents")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0)
                    + fill_fee_cents.max(0.0)
            ),
        );
        obj.insert(
            "actual_fee_cents".to_string(),
            json!(
                obj.get("actual_fee_cents")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0)
                    + actual_fee_cents.max(0.0)
            ),
        );
        obj.insert(
            "fill_cost_cents".to_string(),
            json!(
                obj.get("fill_cost_cents")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0)
                    + fill_cost_cents
            ),
        );
        obj.insert(
            "fill_pnl_cents_net".to_string(),
            json!(
                obj.get("fill_pnl_cents_net")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0)
                    + fill_pnl_cents_net
            ),
        );
        if fill_ts_ms >= obj.get("fill_ts_ms").and_then(Value::as_i64).unwrap_or(0) {
            obj.insert("ts_ms".to_string(), json!(ts_ms));
            obj.insert("fill_ts_ms".to_string(), json!(fill_ts_ms));
            obj.insert(
                "order_id".to_string(),
                ev.get("order_id").cloned().unwrap_or(Value::Null),
            );
            obj.insert(
                "ack_to_fill_ms".to_string(),
                ev.get("ack_to_fill_ms").cloned().unwrap_or(Value::Null),
            );
            obj.insert(
                "submit_to_fill_ms".to_string(),
                ev.get("submit_to_fill_ms").cloned().unwrap_or(Value::Null),
            );
            obj.insert(
                "signal_to_fill_ms".to_string(),
                ev.get("signal_to_fill_ms").cloned().unwrap_or(Value::Null),
            );
            obj.insert(
                "trigger_to_fill_ms".to_string(),
                ev.get("trigger_to_fill_ms").cloned().unwrap_or(Value::Null),
            );
        }
        if let Some(order_ids) = obj.get_mut("order_ids").and_then(Value::as_array_mut) {
            if let Some(order_id) = ev
                .get("order_id")
                .and_then(Value::as_str)
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
            {
                if !order_ids
                    .iter()
                    .any(|existing| existing.as_str() == Some(order_id))
                {
                    order_ids.push(json!(order_id));
                }
            }
        }
        if total_size > 0.0 {
            if let Some(price) = fill_price_cents {
                let prev_avg = obj
                    .get("fill_price_cents")
                    .and_then(Value::as_f64)
                    .unwrap_or(price);
                let next_avg = if prev_size > 0.0 {
                    ((prev_avg * prev_size) + (price * fill_size_shares.max(0.0))) / total_size
                } else {
                    price
                };
                obj.insert("fill_price_cents".to_string(), json!(next_avg));
            }
            if let Some(price) = actual_fill_price_cents {
                let prev_avg = obj
                    .get("actual_fill_price_cents")
                    .and_then(Value::as_f64)
                    .unwrap_or(price);
                let next_avg = if prev_size > 0.0 {
                    ((prev_avg * prev_size) + (price * fill_size_shares.max(0.0))) / total_size
                } else {
                    price
                };
                obj.insert("actual_fill_price_cents".to_string(), json!(next_avg));
            }
            if actual_slippage_cents.is_finite() {
                let prev_avg = obj
                    .get("actual_slippage_cents")
                    .and_then(Value::as_f64)
                    .unwrap_or(actual_slippage_cents);
                let next_avg = if prev_size > 0.0 {
                    ((prev_avg * prev_size) + (actual_slippage_cents * fill_size_shares.max(0.0)))
                        / total_size
                } else {
                    actual_slippage_cents
                };
                obj.insert("actual_slippage_cents".to_string(), json!(next_avg));
            }
            if fill_slippage_cents.is_finite() {
                let prev_avg = obj
                    .get("fill_slippage_cents")
                    .and_then(Value::as_f64)
                    .unwrap_or(fill_slippage_cents);
                let next_avg = if prev_size > 0.0 {
                    ((prev_avg * prev_size) + (fill_slippage_cents * fill_size_shares.max(0.0)))
                        / total_size
                } else {
                    fill_slippage_cents
                };
                obj.insert("fill_slippage_cents".to_string(), json!(next_avg));
            }
        } else {
            if let Some(price) = fill_price_cents {
                obj.insert("fill_price_cents".to_string(), json!(price));
            }
            if let Some(price) = actual_fill_price_cents {
                obj.insert("actual_fill_price_cents".to_string(), json!(price));
            }
            if actual_slippage_cents.is_finite() {
                obj.insert(
                    "actual_slippage_cents".to_string(),
                    json!(actual_slippage_cents),
                );
            }
            if fill_slippage_cents.is_finite() {
                obj.insert(
                    "fill_slippage_cents".to_string(),
                    json!(fill_slippage_cents),
                );
            }
        }
    }

    let summary = json!({
        "fill_event_count": fill_event_count,
        "fill_decision_count": by_decision.len(),
        "realized_net_pnl_cents": realized_net_pnl_cents,
        "total_fill_cost_cents": total_fill_cost_cents
    });
    (by_decision, summary)
}

fn enrich_paper_records_with_live_fills(
    paper_records: &[Value],
    fill_by_decision: &HashMap<String, Value>,
) -> (Vec<Value>, usize) {
    let mut matched = 0_usize;
    let mut out = Vec::<Value>::with_capacity(paper_records.len());
    for record in paper_records {
        let mut next = record.clone();
        let Some(decision_id) = extract_decision_id(record) else {
            out.push(next);
            continue;
        };
        let Some(fill_row) = fill_by_decision.get(&decision_id) else {
            out.push(next);
            continue;
        };
        if let Some(obj) = next.as_object_mut() {
            obj.insert(
                "intent_id".to_string(),
                fill_row
                    .get("intent_id")
                    .cloned()
                    .unwrap_or_else(|| fill_row.get("decision_id").cloned().unwrap_or(Value::Null)),
            );
            obj.insert("live_fill".to_string(), fill_row.clone());
            obj.insert(
                "live_fill_price_cents".to_string(),
                fill_row
                    .get("fill_price_cents")
                    .cloned()
                    .unwrap_or(Value::Null),
            );
            obj.insert(
                "live_fill_pnl_cents_net".to_string(),
                fill_row
                    .get("fill_pnl_cents_net")
                    .cloned()
                    .unwrap_or(Value::Null),
            );
            obj.insert(
                "actual_fill_price_cents".to_string(),
                fill_row
                    .get("actual_fill_price_cents")
                    .cloned()
                    .unwrap_or(Value::Null),
            );
            obj.insert(
                "actual_fee_cents".to_string(),
                fill_row
                    .get("actual_fee_cents")
                    .cloned()
                    .unwrap_or(Value::Null),
            );
            obj.insert(
                "actual_slippage_cents".to_string(),
                fill_row
                    .get("actual_slippage_cents")
                    .cloned()
                    .unwrap_or(Value::Null),
            );
            obj.insert(
                "fill_ts_ms".to_string(),
                fill_row.get("fill_ts_ms").cloned().unwrap_or(Value::Null),
            );
            obj.insert(
                "ack_to_fill_ms".to_string(),
                fill_row
                    .get("ack_to_fill_ms")
                    .cloned()
                    .unwrap_or(Value::Null),
            );
            obj.insert(
                "submit_to_fill_ms".to_string(),
                fill_row
                    .get("submit_to_fill_ms")
                    .cloned()
                    .unwrap_or(Value::Null),
            );
            obj.insert(
                "signal_to_fill_ms".to_string(),
                fill_row
                    .get("signal_to_fill_ms")
                    .cloned()
                    .unwrap_or(Value::Null),
            );
            obj.insert(
                "trigger_to_fill_ms".to_string(),
                fill_row
                    .get("trigger_to_fill_ms")
                    .cloned()
                    .unwrap_or(Value::Null),
            );
            obj.insert(
                "live_fill_event_count".to_string(),
                fill_row
                    .get("fill_event_count")
                    .cloned()
                    .unwrap_or_else(|| json!(1_u64)),
            );
        }
        matched = matched.saturating_add(1);
        out.push(next);
    }
    (out, matched)
}

#[allow(dead_code)]
fn merge_current_summary_paper_records(
    paper_records: &[Value],
    selected_decisions: &[Value],
) -> Vec<Value> {
    let mut seen = HashSet::<String>::new();
    let mut out = Vec::<Value>::with_capacity(paper_records.len() + 1);
    for record in paper_records {
        if let Some(decision_id) = extract_decision_id(record) {
            seen.insert(decision_id);
        }
        out.push(record.clone());
    }
    for decision in selected_decisions {
        let signal_source = decision
            .get("signal_source")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let reason = decision
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let is_current_summary = signal_source.eq_ignore_ascii_case("current_summary")
            || reason.eq_ignore_ascii_case("fev1_current_summary_entry");
        if !is_current_summary {
            continue;
        }
        let Some(decision_id) = extract_decision_id(decision) else {
            continue;
        };
        if !seen.insert(decision_id) {
            continue;
        }
        let mut row = decision.clone();
        if let Some(obj) = row.as_object_mut() {
            if obj.get("intent_id").is_none() {
                if let Some(decision_id) = obj.get("decision_id").cloned() {
                    obj.insert("intent_id".to_string(), decision_id);
                }
            }
            obj.insert("paper_record_source".to_string(), json!("current_summary"));
            obj.insert("paper_record_virtual".to_string(), Value::Bool(true));
        }
        out.push(row);
    }
    out.sort_by_key(|row| row.get("ts_ms").and_then(Value::as_i64).unwrap_or(0));
    out
}

fn build_live_order_lineage(events: &[Value], pending_rows: &[LivePendingOrder]) -> Vec<Value> {
    #[derive(Default)]
    struct LineageAcc {
        intent_id: String,
        attempt_ids: BTreeSet<String>,
        order_ids: BTreeSet<String>,
        pending_order_ids: BTreeSet<String>,
        last_reason: String,
        last_event_type: String,
        last_status: String,
        last_ts_ms: i64,
    }

    let mut acc = HashMap::<String, LineageAcc>::new();
    for ev in events {
        let Some(decision_id) = extract_decision_id(ev) else {
            continue;
        };
        let ts_ms = ev.get("ts_ms").and_then(Value::as_i64).unwrap_or(0);
        let row = acc.entry(decision_id).or_default();
        if row.intent_id.trim().is_empty() {
            row.intent_id = ev
                .get("intent_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
        }
        if let Some(attempt_id) = ev
            .get("attempt_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            row.attempt_ids.insert(attempt_id.to_string());
        }
        if let Some(order_id) = extract_order_id_from_live_event(ev) {
            row.order_ids.insert(order_id);
        }
        if ts_ms >= row.last_ts_ms {
            row.last_ts_ms = ts_ms;
            row.last_reason = ev
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            row.last_event_type = ev
                .get("event_type")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            row.last_status = if ev.get("accepted").and_then(Value::as_bool).unwrap_or(false) {
                "accepted".to_string()
            } else if row.last_reason.contains("filled") {
                "filled".to_string()
            } else if row.last_reason.contains("pending") {
                "pending".to_string()
            } else if row.last_reason.contains("reject") {
                "rejected".to_string()
            } else {
                "unknown".to_string()
            };
        }
    }

    for row in pending_rows {
        if row.decision_id.trim().is_empty() {
            continue;
        }
        let entry = acc.entry(row.decision_id.clone()).or_default();
        if entry.intent_id.trim().is_empty() {
            entry.intent_id = row.intent_id.clone();
        }
        if !row.attempt_id.trim().is_empty() {
            entry.attempt_ids.insert(row.attempt_id.clone());
        }
        if !row.order_id.trim().is_empty() {
            entry.order_ids.insert(row.order_id.clone());
            entry.pending_order_ids.insert(row.order_id.clone());
        }
        entry.last_status = "pending".to_string();
        entry.last_ts_ms = entry.last_ts_ms.max(row.submitted_ts_ms);
    }

    let mut rows = acc
        .into_iter()
        .map(|(decision_id, row)| {
            let pending_count = row.pending_order_ids.len();
            json!({
                "intent_id": if row.intent_id.trim().is_empty() { decision_id.clone() } else { row.intent_id },
                "decision_id": decision_id,
                "attempt_ids": row.attempt_ids.into_iter().collect::<Vec<_>>(),
                "order_ids": row.order_ids.into_iter().collect::<Vec<_>>(),
                "pending_order_ids": row.pending_order_ids.into_iter().collect::<Vec<_>>(),
                "pending_count": pending_count,
                "last_reason": row.last_reason,
                "last_event_type": row.last_event_type,
                "last_status": row.last_status,
                "last_ts_ms": row.last_ts_ms
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| {
        std::cmp::Reverse(row.get("last_ts_ms").and_then(Value::as_i64).unwrap_or(0))
    });
    rows
}

fn build_live_completed_records(
    paper_records: &[Value],
    fill_by_decision: &HashMap<String, Value>,
    live_order_lineage: &[Value],
) -> Vec<Value> {
    let paper_by_decision = paper_records
        .iter()
        .filter_map(|row| extract_decision_id(row).map(|decision_id| (decision_id, row.clone())))
        .collect::<HashMap<_, _>>();
    let lineage_by_decision = live_order_lineage
        .iter()
        .filter_map(|row| extract_decision_id(row).map(|decision_id| (decision_id, row.clone())))
        .collect::<HashMap<_, _>>();
    let mut rows = fill_by_decision
        .iter()
        .map(|(decision_id, fill_row)| {
            let mut next = paper_by_decision
                .get(decision_id)
                .cloned()
                .unwrap_or_else(|| {
                    json!({
                        "intent_id": fill_row
                            .get("intent_id")
                            .cloned()
                            .unwrap_or_else(|| json!(decision_id)),
                        "decision_id": decision_id,
                    })
                });
            if let Some(obj) = next.as_object_mut() {
                obj.insert(
                    "intent_id".to_string(),
                    fill_row
                        .get("intent_id")
                        .cloned()
                        .unwrap_or_else(|| json!(decision_id)),
                );
                obj.insert("decision_id".to_string(), json!(decision_id));
                obj.insert("live_fill".to_string(), fill_row.clone());
                obj.insert(
                    "fill_ts_ms".to_string(),
                    fill_row.get("fill_ts_ms").cloned().unwrap_or(Value::Null),
                );
                obj.insert(
                    "live_fill_price_cents".to_string(),
                    fill_row
                        .get("fill_price_cents")
                        .cloned()
                        .unwrap_or(Value::Null),
                );
                obj.insert(
                    "actual_fill_price_cents".to_string(),
                    fill_row
                        .get("actual_fill_price_cents")
                        .cloned()
                        .unwrap_or(Value::Null),
                );
                obj.insert(
                    "actual_fee_cents".to_string(),
                    fill_row
                        .get("actual_fee_cents")
                        .cloned()
                        .unwrap_or(Value::Null),
                );
                obj.insert(
                    "actual_slippage_cents".to_string(),
                    fill_row
                        .get("actual_slippage_cents")
                        .cloned()
                        .unwrap_or(Value::Null),
                );
                obj.insert(
                    "live_fill_pnl_cents_net".to_string(),
                    fill_row
                        .get("fill_pnl_cents_net")
                        .cloned()
                        .unwrap_or(Value::Null),
                );
                obj.insert(
                    "fill_quote_usdc".to_string(),
                    fill_row
                        .get("fill_quote_usdc")
                        .cloned()
                        .unwrap_or(Value::Null),
                );
                obj.insert(
                    "fill_size_shares".to_string(),
                    fill_row
                        .get("fill_size_shares")
                        .cloned()
                        .unwrap_or(Value::Null),
                );
                obj.insert(
                    "live_fill_event_count".to_string(),
                    fill_row
                        .get("fill_event_count")
                        .cloned()
                        .unwrap_or_else(|| json!(1_u64)),
                );
                obj.insert("live_status".to_string(), json!("filled"));
                if let Some(lineage_row) = lineage_by_decision.get(decision_id) {
                    obj.insert(
                        "order_ids".to_string(),
                        lineage_row.get("order_ids").cloned().unwrap_or(Value::Null),
                    );
                    obj.insert(
                        "pending_order_ids".to_string(),
                        lineage_row
                            .get("pending_order_ids")
                            .cloned()
                            .unwrap_or(Value::Null),
                    );
                    obj.insert(
                        "pending_count".to_string(),
                        lineage_row
                            .get("pending_count")
                            .cloned()
                            .unwrap_or_else(|| json!(0_u64)),
                    );
                    obj.insert(
                        "last_reason".to_string(),
                        lineage_row
                            .get("last_reason")
                            .cloned()
                            .unwrap_or(Value::Null),
                    );
                    obj.insert(
                        "last_status".to_string(),
                        lineage_row
                            .get("last_status")
                            .cloned()
                            .unwrap_or_else(|| json!("filled")),
                    );
                    obj.insert(
                        "last_ts_ms".to_string(),
                        lineage_row.get("last_ts_ms").cloned().unwrap_or_else(|| {
                            fill_row.get("fill_ts_ms").cloned().unwrap_or(Value::Null)
                        }),
                    );
                } else {
                    obj.insert(
                        "order_ids".to_string(),
                        fill_row
                            .get("order_ids")
                            .cloned()
                            .unwrap_or_else(|| json!([])),
                    );
                    obj.insert("pending_order_ids".to_string(), json!([]));
                    obj.insert("pending_count".to_string(), json!(0_u64));
                    obj.insert(
                        "last_reason".to_string(),
                        json!("rust_order_terminal_filled"),
                    );
                    obj.insert("last_status".to_string(), json!("filled"));
                    obj.insert(
                        "last_ts_ms".to_string(),
                        fill_row.get("fill_ts_ms").cloned().unwrap_or(Value::Null),
                    );
                }
            }
            next
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| {
        std::cmp::Reverse(
            row.get("fill_ts_ms")
                .and_then(Value::as_i64)
                .unwrap_or_else(|| row.get("last_ts_ms").and_then(Value::as_i64).unwrap_or(0)),
        )
    });
    rows
}

fn live_execution_policy_meta() -> Value {
    json!({
        "mode": "paper_parity",
        "description": "live execution follows paper decisions and only applies safety, freshness, duplicate-position and price-parity checks"
    })
}

#[derive(Debug, Clone)]
struct LiveRuntimeWakeEvent {
    symbol: String,
    market_type: String,
    source: String,
    ts_ms: i64,
    sample_ts_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LiveExecutionAggState {
    market_type: String,
    entry_slippage_mult: f64,
    exit_slippage_mult: f64,
    reject_ema: f64,
    submit_delta_ema_cents: f64,
    accepted_delta_ema_cents: f64,
    #[serde(default)]
    latency_ema_ms: f64,
    #[serde(default)]
    attempts_ema: f64,
    sample_count: u64,
    last_error: Option<String>,
    #[serde(default)]
    entry_liquidity_reject_round_id: Option<String>,
    #[serde(default)]
    entry_liquidity_reject_side: Option<String>,
    #[serde(default)]
    entry_liquidity_reject_count: u32,
    #[serde(default)]
    entry_liquidity_reject_first_ts_ms: i64,
    #[serde(default)]
    entry_liquidity_reject_last_ts_ms: i64,
    updated_ts_ms: i64,
}

impl LiveExecutionAggState {
    fn new(market_type: &str) -> Self {
        Self {
            market_type: market_type.to_string(),
            entry_slippage_mult: 1.0,
            exit_slippage_mult: 1.0,
            reject_ema: 0.0,
            submit_delta_ema_cents: 0.0,
            accepted_delta_ema_cents: 0.0,
            latency_ema_ms: 0.0,
            attempts_ema: 0.0,
            sample_count: 0,
            last_error: None,
            entry_liquidity_reject_round_id: None,
            entry_liquidity_reject_side: None,
            entry_liquidity_reject_count: 0,
            entry_liquidity_reject_first_ts_ms: 0,
            entry_liquidity_reject_last_ts_ms: 0,
            updated_ts_ms: Utc::now().timestamp_millis(),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test_liquidity_rejects(
        market_type: &str,
        round_id: &str,
        side: &str,
        reject_count: u32,
        now_ms: i64,
    ) -> Self {
        Self {
            market_type: market_type.to_string(),
            entry_slippage_mult: 1.0,
            exit_slippage_mult: 1.0,
            reject_ema: 0.5,
            submit_delta_ema_cents: 0.0,
            accepted_delta_ema_cents: 0.0,
            latency_ema_ms: 0.0,
            attempts_ema: 1.0,
            sample_count: reject_count as u64,
            last_error: Some("no orders found to match with FAK order".to_string()),
            entry_liquidity_reject_round_id: Some(round_id.to_string()),
            entry_liquidity_reject_side: Some(side.to_ascii_uppercase()),
            entry_liquidity_reject_count: reject_count,
            entry_liquidity_reject_first_ts_ms: now_ms.saturating_sub(1_500),
            entry_liquidity_reject_last_ts_ms: now_ms,
            updated_ts_ms: now_ms,
        }
    }
}

fn parse_snapshot_event(v: &Value) -> Option<(String, String, Option<i64>)> {
    let symbol = v
        .get("symbol")
        .and_then(Value::as_str)
        .and_then(normalize_runtime_symbol)?;
    let tf = v
        .get("timeframe")
        .and_then(Value::as_str)
        .map(|s| s.trim().to_ascii_lowercase())?;
    let sample_ts_ms = v
        .get("ts_ireland_sample_ms")
        .and_then(Value::as_i64)
        .or_else(|| v.get("ts_ms").and_then(Value::as_i64))
        .filter(|v| *v > 0);
    if tf == "5m" || tf == "15m" {
        Some((symbol, tf, sample_ts_ms))
    } else {
        None
    }
}

fn live_snapshot_event_channel(prefix: &str) -> String {
    format!("{prefix}:snapshot:events")
}

fn is_liquidity_reject_reason(reason: &str) -> bool {
    let r = reason.trim().to_ascii_lowercase();
    r.contains("unmatched")
        || r.contains("insufficient liquidity")
        || r.contains("no orders found")
        || r.contains("rejected")
        || r.contains("terminal_rejected")
        || r.contains("timeout")
        || r.contains("canceled")
        || r.contains("cancelled")
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
enum LiveRuntimeControlMode {
    #[default]
    Normal,
    GracefulStop,
    ForcePause,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LiveRuntimeControl {
    mode: LiveRuntimeControlMode,
    requested_at_ms: i64,
    updated_at_ms: i64,
    completed_at_ms: Option<i64>,
    note: Option<String>,
}

impl LiveRuntimeControl {
    fn normal(now_ms: i64) -> Self {
        Self {
            mode: LiveRuntimeControlMode::Normal,
            requested_at_ms: now_ms,
            updated_at_ms: now_ms,
            completed_at_ms: None,
            note: None,
        }
    }
}

#[derive(Debug, Clone)]
struct ServerChanConfig {
    enabled: bool,
    api_url: String,
    request_timeout_ms: u64,
    throttle_ms: i64,
    daily_report_hour_utc: u32,
}

impl ServerChanConfig {
    fn from_env() -> Self {
        let api_url = std::env::var("FORGE_SC3_API_URL")
            .ok()
            .or_else(|| std::env::var("SC3_API_URL").ok())
            .unwrap_or_default()
            .trim()
            .to_string();
        let enabled = std::env::var("FORGE_SC3_ENABLED")
            .ok()
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(!api_url.is_empty());
        let request_timeout_ms = std::env::var("FORGE_SC3_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(4_000)
            .clamp(800, 15_000);
        let throttle_ms = std::env::var("FORGE_SC3_THROTTLE_MS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(LIVE_ALERT_THROTTLE_DEFAULT_MS)
            .clamp(2_000, 3_600_000);
        let daily_report_hour_utc = std::env::var("FORGE_SC3_DAILY_REPORT_HOUR_UTC")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0)
            .min(23);
        Self {
            enabled: enabled && !api_url.is_empty(),
            api_url,
            request_timeout_ms,
            throttle_ms,
            daily_report_hour_utc,
        }
    }
}

impl ApiState {
    async fn chart_cache_get(&self, key: &str) -> Option<Value> {
        let now = Instant::now();
        let cache = self.chart_cache.read().await;
        let entry = cache.get(key)?;
        if now.duration_since(entry.created_at) > Duration::from_millis(CHART_CACHE_TTL_MS) {
            return None;
        }
        Some(entry.payload.clone())
    }

    async fn chart_cache_put(&self, key: String, payload: Value) {
        let now = Instant::now();
        let mut cache = self.chart_cache.write().await;
        if cache.len() >= CHART_CACHE_MAX_ENTRIES {
            cache.retain(|_, v| now.duration_since(v.created_at) <= Duration::from_secs(15));
            if cache.len() >= CHART_CACHE_MAX_ENTRIES {
                let mut oldest: Vec<(String, Instant)> = cache
                    .iter()
                    .map(|(k, v)| (k.clone(), v.created_at))
                    .collect();
                oldest.sort_by_key(|(_, ts)| *ts);
                let remove_n = cache.len().saturating_sub(CHART_CACHE_MAX_ENTRIES / 2);
                for (k, _) in oldest.into_iter().take(remove_n) {
                    cache.remove(&k);
                }
            }
        }
        cache.insert(
            key,
            ChartCacheEntry {
                created_at: now,
                payload,
            },
        );
    }

    async fn get_live_position_state(&self, symbol: &str, market_type: &str) -> LivePositionState {
        let now_ms = Utc::now().timestamp_millis();
        let scope_key = runtime_scope_key(symbol, market_type);
        {
            let states = self.live_position_states.read().await;
            if let Some(v) = states.get(&scope_key) {
                return v.clone();
            }
        }
        let mut states = self.live_position_states.write().await;
        let entry = states
            .entry(scope_key)
            .or_insert_with(|| LivePositionState::flat(symbol, market_type, now_ms));
        entry.clone()
    }

    async fn put_live_position_state(
        &self,
        symbol: &str,
        market_type: &str,
        next: LivePositionState,
    ) {
        let mut states = self.live_position_states.write().await;
        let mut normalized = next;
        if normalized.symbol.trim().is_empty() {
            normalized.symbol = runtime_scope_symbol(symbol);
        }
        let scope_key = runtime_scope_key(&normalized.symbol, market_type);
        states.insert(scope_key, normalized);
    }

    async fn get_live_runtime_control(
        &self,
        symbol: &str,
        market_type: &str,
    ) -> LiveRuntimeControl {
        let now_ms = Utc::now().timestamp_millis();
        let scope_key = runtime_scope_key(symbol, market_type);
        {
            let controls = self.live_runtime_controls.read().await;
            if let Some(v) = controls.get(&scope_key) {
                return v.clone();
            }
        }
        let mut controls = self.live_runtime_controls.write().await;
        let entry = controls
            .entry(scope_key)
            .or_insert_with(|| LiveRuntimeControl::normal(now_ms));
        entry.clone()
    }

    async fn put_live_runtime_control(
        &self,
        symbol: &str,
        market_type: &str,
        next: LiveRuntimeControl,
    ) {
        let mut controls = self.live_runtime_controls.write().await;
        let scope_key = runtime_scope_key(symbol, market_type);
        controls.insert(scope_key, next);
    }

    async fn get_runtime_paper_reset_anchor(&self, symbol: &str, market_type: &str) -> Option<i64> {
        let scope_key = runtime_scope_key(symbol, market_type);
        let anchors = self.live_runtime_paper_reset_anchors.read().await;
        anchors.get(&scope_key).copied().filter(|v| *v > 0)
    }

    async fn put_runtime_paper_reset_anchor(
        &self,
        symbol: &str,
        market_type: &str,
        reset_after_ms: i64,
    ) {
        let scope_key = runtime_scope_key(symbol, market_type);
        let mut anchors = self.live_runtime_paper_reset_anchors.write().await;
        anchors.insert(scope_key, reset_after_ms.max(0));
    }

    #[allow(dead_code)]
    async fn check_and_mark_live_decision(
        &self,
        key: &str,
        now_ms: i64,
        mark_attempt: bool,
    ) -> bool {
        let mut guard = self.live_decision_guard.write().await;
        guard.retain(|_, ts| now_ms.saturating_sub(*ts) <= LIVE_DECISION_GUARD_TTL_MS);
        if guard.contains_key(key) {
            return true;
        }
        if mark_attempt {
            guard.insert(key.to_string(), now_ms);
        }
        false
    }

    async fn append_live_event(&self, symbol: &str, market_type: &str, mut event: Value) {
        let now_ms = Utc::now().timestamp_millis();
        if event.get("symbol").is_none() {
            event["symbol"] = Value::String(runtime_scope_symbol(symbol));
        }
        if event.get("market_type").is_none() {
            event["market_type"] = Value::String(market_type.to_string());
        }
        if event.get("ts_ms").is_none() {
            event["ts_ms"] = Value::from(now_ms);
        }
        let ts_ms = event.get("ts_ms").and_then(Value::as_i64).unwrap_or(now_ms);
        if event.get("ts_iso").is_none() {
            event["ts_iso"] = Value::String(
                DateTime::<Utc>::from_timestamp_millis(ts_ms)
                    .map(|v| v.to_rfc3339())
                    .unwrap_or_default(),
            );
        }
        if event.get("event_id").is_none() {
            event["event_id"] = Value::String(Uuid::new_v4().to_string());
        }
        if event.get("order_id").is_none() {
            if let Some(order_id) = extract_order_id_from_live_event(&event) {
                event["order_id"] = Value::String(order_id);
            }
        }
        if event.get("event_type").is_none() {
            event["event_type"] = Value::String(classify_live_event_type(&event).to_string());
        }
        {
            let mut seq = self.live_event_seq.write().await;
            *seq = seq.saturating_add(1);
            event["event_seq"] = Value::from(*seq);
        }
        let max_len = live_event_log_max();
        let mut events = self.live_events.write().await;
        events.push_back(event);
        while events.len() > max_len {
            events.pop_front();
        }
        drop(events);
        self.persist_live_runtime_state_async(symbol, market_type)
            .await;
    }

    async fn list_live_events(&self, symbol: &str, market_type: &str, limit: usize) -> Vec<Value> {
        let symbol = runtime_scope_symbol(symbol);
        let events = self.live_events.read().await;
        events
            .iter()
            .rev()
            .filter(|v| {
                let symbol_ok = v
                    .get("symbol")
                    .and_then(Value::as_str)
                    .map(|s| s.eq_ignore_ascii_case(&symbol))
                    .unwrap_or(false);
                let market_ok = v
                    .get("market_type")
                    .and_then(Value::as_str)
                    .map(|s| s == market_type)
                    .unwrap_or(false);
                symbol_ok && market_ok
            })
            .take(limit.max(1))
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    async fn get_runtime_snapshot(&self, symbol: &str, market_type: &str) -> Option<Value> {
        let store = self.live_runtime_snapshots.read().await;
        let scope_key = runtime_scope_key(symbol, market_type);
        store.get(&scope_key).cloned()
    }

    async fn set_runtime_snapshot(&self, symbol: &str, market_type: &str, payload: Value) {
        let mut store = self.live_runtime_snapshots.write().await;
        let scope_key = runtime_scope_key(symbol, market_type);
        store.insert(scope_key, payload);
    }

    async fn remember_recent_active_intent(&self, symbol: &str, market_type: &str, signal: &Value) {
        let Some(intent_id) = signal
            .get("intent_id")
            .and_then(Value::as_str)
            .map(str::trim)
        else {
            return;
        };
        if intent_id.is_empty() {
            return;
        }
        let now_ms = Utc::now().timestamp_millis();
        let ts_ms = signal
            .get("ts_ms")
            .and_then(Value::as_i64)
            .unwrap_or(now_ms);
        let buffer_ttl_ms = live_recent_intent_buffer_ttl_ms();
        let buffer_expires_at_ms = signal
            .get("expires_at_ms")
            .and_then(Value::as_i64)
            .map(|v| v.max(ts_ms.saturating_add(buffer_ttl_ms)))
            .unwrap_or_else(|| ts_ms.saturating_add(buffer_ttl_ms));
        if buffer_expires_at_ms <= now_ms {
            return;
        }
        let mut buffered = signal.clone();
        if let Some(obj) = buffered.as_object_mut() {
            obj.insert("expires_at_ms".to_string(), json!(buffer_expires_at_ms));
            obj.insert("signal_source".to_string(), json!("recent_intent_buffer"));
        }
        let scope_key = runtime_scope_key(symbol, market_type);
        let mut store = self.live_recent_active_intents.write().await;
        let queue = store.entry(scope_key).or_insert_with(VecDeque::new);
        queue.retain(|row| {
            let expires_at_ms = decision_effective_expires_at_ms(row);
            let row_intent_id = row
                .get("intent_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            expires_at_ms > now_ms && row_intent_id != intent_id
        });
        queue.push_back(buffered);
        let max_rows = live_recent_intent_buffer_max();
        while queue.len() > max_rows {
            queue.pop_front();
        }
    }

    async fn recent_active_intents(
        &self,
        symbol: &str,
        market_type: &str,
        latest_ts_ms: i64,
    ) -> Vec<Value> {
        let scope_key = runtime_scope_key(symbol, market_type);
        let mut store = self.live_recent_active_intents.write().await;
        let Some(queue) = store.get_mut(&scope_key) else {
            return Vec::new();
        };
        queue.retain(|row| decision_effective_expires_at_ms(row) > latest_ts_ms);
        queue.iter().rev().cloned().collect()
    }

    async fn upsert_runtime_event_sample(
        &self,
        symbol: &str,
        market_type: &str,
        sample: StrategySample,
    ) {
        self.upsert_runtime_event_samples_batch(symbol, market_type, vec![sample])
            .await;
    }

    async fn upsert_runtime_event_samples_batch(
        &self,
        symbol: &str,
        market_type: &str,
        mut samples: Vec<StrategySample>,
    ) {
        if samples.is_empty() {
            return;
        }
        samples.sort_by_key(|s| s.ts_ms);
        let mut store = self.runtime_event_samples.write().await;
        let scope_key = runtime_event_sample_scope_key(symbol, market_type);
        let entry = store
            .entry(scope_key)
            .or_insert_with(|| RuntimeEventSampleBuffer {
                updated_at_ms: 0,
                samples: VecDeque::new(),
            });
        for sample in samples {
            entry.updated_at_ms = sample.ts_ms;
            entry.samples.push_back(sample);
        }
        let max_points = runtime_event_sample_buffer_max();
        while entry.samples.len() > max_points {
            entry.samples.pop_front();
        }
    }

    async fn get_runtime_event_samples(
        &self,
        symbol: &str,
        market_type: &str,
        lookback_minutes: u32,
        max_points: u32,
    ) -> Option<Arc<Vec<StrategySample>>> {
        let store = self.runtime_event_samples.read().await;
        let scope_key = runtime_event_sample_scope_key(symbol, market_type);
        let rows = store.get(&scope_key)?;
        if rows.samples.is_empty() {
            return None;
        }
        let now_ms = Utc::now().timestamp_millis();
        let from_ms = now_ms.saturating_sub(i64::from(lookback_minutes) * 60_000);
        let mut out = rows
            .samples
            .iter()
            .filter(|row| row.ts_ms >= from_ms)
            .cloned()
            .collect::<Vec<_>>();
        if out.is_empty() {
            return None;
        }
        let max_points = max_points as usize;
        if out.len() > max_points {
            let trim = out.len().saturating_sub(max_points);
            out.drain(0..trim);
        }
        Some(Arc::new(out))
    }

    async fn preload_runtime_event_samples_from_redis(&self, symbol: &str, markets: &[String]) {
        let Some(mut conn) = self.redis_manager.clone() else {
            return;
        };
        let ring_take = runtime_event_sample_buffer_max().min(40_000) as isize;
        for market in markets {
            let market = market.trim().to_ascii_lowercase();
            if market != "5m" && market != "15m" {
                continue;
            }
            let key = format!(
                "{}:snapshot:ring:{}:{market}",
                self.redis_prefix,
                symbol.trim().to_ascii_uppercase()
            );
            let rows: Vec<String> = match conn.lrange(&key, -ring_take, -1_isize).await {
                Ok(v) => v,
                Err(err) => {
                    tracing::debug!(?err, key = %key, "runtime preload ring lrange failed");
                    continue;
                }
            };
            if rows.is_empty() {
                continue;
            }
            let mut samples = Vec::<StrategySample>::new();
            for row in rows {
                let Ok(v) = serde_json::from_str::<Value>(&row) else {
                    continue;
                };
                let Some((ev_symbol, tf, _, sample)) = strategy_sample_from_snapshot_event(&v)
                else {
                    continue;
                };
                if tf.eq_ignore_ascii_case(&market) && ev_symbol.eq_ignore_ascii_case(symbol) {
                    samples.push(sample);
                }
            }
            if !samples.is_empty() {
                self.upsert_runtime_event_samples_batch(symbol, &market, samples)
                    .await;
            }
        }
    }

    async fn persist_live_runtime_state_async(&self, symbol: &str, market_type: &str) {
        let scope_key = runtime_scope_key(symbol, market_type);
        {
            let mut inflight = self.live_persist_inflight.write().await;
            if !inflight.insert(scope_key.clone()) {
                return;
            }
        }
        let state = self.clone();
        let symbol = runtime_scope_symbol(symbol);
        let market = market_type.to_string();
        tokio::spawn(async move {
            persist_live_runtime_state(&state, &symbol, &market).await;
            let mut inflight = state.live_persist_inflight.write().await;
            inflight.remove(&scope_key);
        });
    }

    pub(super) async fn get_live_execution_aggr_state(
        &self,
        symbol: &str,
        market_type: &str,
    ) -> LiveExecutionAggState {
        let mut store = self.live_execution_aggr_states.write().await;
        let scope_key = runtime_scope_key(symbol, market_type);
        store
            .entry(scope_key)
            .or_insert_with(|| LiveExecutionAggState::new(market_type))
            .clone()
    }

    #[cfg(test)]
    pub(super) async fn set_live_execution_aggr_state(
        &self,
        symbol: &str,
        market_type: &str,
        state: LiveExecutionAggState,
    ) {
        let mut store = self.live_execution_aggr_states.write().await;
        let scope_key = runtime_scope_key(symbol, market_type);
        store.insert(scope_key, state);
    }

    // --- Kill Switch API ---

    pub(super) async fn ks_is_halted(&self, symbol: &str, market_type: &str) -> bool {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let store = self.live_kill_switch.read().await;
        let key = runtime_scope_key(symbol, market_type);
        store
            .get(&key)
            .map(|s| s.is_halted(now_ms))
            .unwrap_or(false)
    }

    pub(super) async fn ks_halt_reason(&self, symbol: &str, market_type: &str) -> Option<String> {
        let store = self.live_kill_switch.read().await;
        let key = runtime_scope_key(symbol, market_type);
        store.get(&key).and_then(|s| s.halt_reason.clone())
    }

    /// Classify an error reason into EntryAttemptReason.
    fn classify_entry_failure(reason: &str) -> EntryAttemptReason {
        let r = reason.trim().to_ascii_lowercase();
        // Parity exhaustion - price exceeded band
        if r.contains("parity_band_exhausted") {
            return EntryAttemptReason::ParityExhausted;
        }
        // Infrastructure errors
        if r.contains("timeout")
            || r.contains("5")
            || r.contains("network")
            || r.contains("connection")
            || r.contains("503")
            || r.contains("502")
        {
            return EntryAttemptReason::InfraError;
        }
        // Liquidity empty - market microstructure issues (not a system failure)
        if r.contains("no orders found")
            || r.contains("one_sided_book")
            || r.contains("liquidity_empty")
            || r.contains("insufficient_book_depth")
            || r.contains("no top1")
            || r.contains("liquidity no top1")
        {
            return EntryAttemptReason::LiquidityEmpty;
        }
        // Unknown - treat as liquidity empty for safety (don't trigger halt)
        EntryAttemptReason::LiquidityEmpty
    }

    /// Record entry attempt from an error string. Automatically classifies the failure type.
    pub(super) async fn ks_record_entry_attempt_from_error(
        &self,
        symbol: &str,
        market_type: &str,
        error_reason: Option<&str>,
    ) {
        let reason = error_reason
            .map(Self::classify_entry_failure)
            .unwrap_or(EntryAttemptReason::Success);

        let now_ms = chrono::Utc::now().timestamp_millis();
        let mut store = self.live_kill_switch.write().await;
        let key = runtime_scope_key(symbol, market_type);
        let ks = store
            .entry(key)
            .or_insert_with(|| LiveKillSwitchState::new(now_ms));
        ks.record_entry_attempt(now_ms, reason);

        // Only parity exhaustion triggers Layer 2 halt - not liquidity_empty or infra_error
        if reason == EntryAttemptReason::ParityExhausted {
            // Check Layer 2: execution quality (only for parity failures)
            let rate = ks.parity_exhaustion_rate();
            let limit = ks_parity_exhaustion_rate_limit();
            let min_samples = 12; // Increased from 10 for more stable rate
            if !ks.is_halted(now_ms) && ks.entry_attempt_count >= min_samples && rate > limit {
                let resume_ms = now_ms + ks_halt_resume_ms();
                ks.halt(
                    now_ms,
                    &format!(
                        "layer2_parity_exhaustion_rate:{:.1}%_over_limit:{:.1}%_samples:{}",
                        rate * 100.0,
                        limit * 100.0,
                        ks.entry_attempt_count
                    ),
                    Some(resume_ms),
                );
            }
        }
    }

    /// Record successful entry attempt.
    pub(super) async fn ks_record_entry_success(&self, symbol: &str, market_type: &str) {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let mut store = self.live_kill_switch.write().await;
        let key = runtime_scope_key(symbol, market_type);
        let ks = store
            .entry(key)
            .or_insert_with(|| LiveKillSwitchState::new(now_ms));
        ks.record_entry_attempt(now_ms, EntryAttemptReason::Success);
    }

    pub(super) async fn ks_record_trade_outcome(
        &self,
        symbol: &str,
        market_type: &str,
        net_pnl_cents: f64,
    ) {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let mut store = self.live_kill_switch.write().await;
        let key = runtime_scope_key(symbol, market_type);
        let ks = store
            .entry(key)
            .or_insert_with(|| LiveKillSwitchState::new(now_ms));
        // Reset daily if >24h since last reset
        if now_ms - ks.daily_reset_ts_ms > 86_400_000 {
            ks.reset_daily(now_ms);
        }
        ks.record_trade_outcome(net_pnl_cents);
        if ks.is_halted(now_ms) {
            return;
        }
        let resume_ms = now_ms + ks_halt_resume_ms();
        // Layer 1a: daily loss limit
        let loss = ks.daily_loss_cents();
        if loss > ks_daily_loss_limit_cents() {
            ks.halt(
                now_ms,
                &format!(
                    "layer1_daily_loss:{:.1}c_limit:{:.1}c",
                    loss,
                    ks_daily_loss_limit_cents()
                ),
                Some(resume_ms),
            );
            return;
        }
        // Layer 1b: daily drawdown limit
        let dd = ks.daily_drawdown_cents();
        if dd > ks_daily_drawdown_limit_cents() {
            ks.halt(
                now_ms,
                &format!(
                    "layer1_daily_drawdown:{:.1}c_limit:{:.1}c",
                    dd,
                    ks_daily_drawdown_limit_cents()
                ),
                Some(resume_ms),
            );
            return;
        }
        // Layer 3: rolling win rate
        let min_trades = ks_win_rate_min_trades();
        if let Some(wr) = ks.rolling_win_rate(min_trades) {
            if wr < ks_min_rolling_win_rate() {
                ks.halt(
                    now_ms,
                    &format!(
                        "layer3_rolling_win_rate:{:.1}%_below_min:{:.1}%_over_{}_trades",
                        wr * 100.0,
                        ks_min_rolling_win_rate() * 100.0,
                        ks.rolling_outcomes.len()
                    ),
                    Some(resume_ms),
                );
            }
        }
    }

    pub(super) async fn ks_snapshot(&self, symbol: &str, market_type: &str) -> Value {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let store = self.live_kill_switch.read().await;
        let key = runtime_scope_key(symbol, market_type);
        if let Some(ks) = store.get(&key) {
            json!({
                "halted": ks.is_halted(now_ms),
                "halt_reason": ks.halt_reason,
                "halted_since_ms": ks.halted_since_ms,
                "resume_after_ms": ks.resume_after_ms,
                "daily_pnl_cents": ks.daily_pnl_cents,
                "daily_loss_cents": ks.daily_loss_cents(),
                "daily_drawdown_cents": ks.daily_drawdown_cents(),
                "daily_reset_ts_ms": ks.daily_reset_ts_ms,
                "parity_exhaustion_rate": ks.parity_exhaustion_rate(),
                "entry_attempt_count": ks.entry_attempt_count,
                "parity_exhausted_count": ks.parity_exhausted_count,
                "rolling_trade_count": ks.rolling_outcomes.len(),
                "rolling_win_rate": ks.rolling_win_rate(1),
                "thresholds": {
                    "daily_loss_limit_cents": ks_daily_loss_limit_cents(),
                    "daily_drawdown_limit_cents": ks_daily_drawdown_limit_cents(),
                    "parity_exhaustion_rate_limit": ks_parity_exhaustion_rate_limit(),
                    "min_rolling_win_rate": ks_min_rolling_win_rate(),
                    "win_rate_min_trades": ks_win_rate_min_trades(),
                }
            })
        } else {
            json!({ "halted": false, "initialized": false })
        }
    }

    #[allow(dead_code)]
    async fn apply_aggressiveness_to_execution_cfg(
        &self,
        symbol: &str,
        market_type: &str,
        cfg: &LiveExecutionConfig,
    ) -> (LiveExecutionConfig, LiveExecutionAggState) {
        let s = self
            .get_live_execution_aggr_state(symbol, market_type)
            .await;
        let mut tuned = cfg.clone();
        tuned.entry_slippage_bps =
            (cfg.entry_slippage_bps * s.entry_slippage_mult).clamp(0.0, 500.0);
        tuned.exit_slippage_bps = (cfg.exit_slippage_bps * s.exit_slippage_mult).clamp(0.0, 500.0);
        (tuned, s)
    }

    #[allow(dead_code)]
    async fn update_live_execution_aggr_from_orders(
        &self,
        symbol: &str,
        market_type: &str,
        orders: &[Value],
    ) -> LiveExecutionAggState {
        let mut store = self.live_execution_aggr_states.write().await;
        let scope_key = runtime_scope_key(symbol, market_type);
        let st = store
            .entry(scope_key)
            .or_insert_with(|| LiveExecutionAggState::new(market_type));
        if orders.is_empty() {
            // No recent executions: slowly relax back to baseline to avoid stale high aggressiveness.
            st.entry_slippage_mult =
                (st.entry_slippage_mult * 0.992 + 1.0 * 0.008).clamp(0.85, 2.40);
            st.exit_slippage_mult = (st.exit_slippage_mult * 0.990 + 1.0 * 0.010).clamp(0.85, 2.40);
            st.reject_ema *= 0.92;
            st.submit_delta_ema_cents *= 0.96;
            st.accepted_delta_ema_cents *= 0.96;
            st.latency_ema_ms *= 0.95;
            st.attempts_ema = (st.attempts_ema * 0.95).max(1.0);
            st.updated_ts_ms = Utc::now().timestamp_millis();
            return st.clone();
        }
        for row in orders {
            let action = row
                .get("decision")
                .and_then(|v| v.get("action"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_ascii_lowercase();
            let is_exit_like = is_live_exit_action(&action);
            let round_id = row
                .get("decision")
                .and_then(|v| v.get("round_id"))
                .and_then(Value::as_str)
                .map(|v| v.to_string());
            let side = row
                .get("decision")
                .and_then(|v| v.get("side"))
                .and_then(Value::as_str)
                .map(|v| v.to_ascii_uppercase());
            let accepted = row
                .get("accepted")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let err = row
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let trace = row.get("price_trace").cloned().unwrap_or(Value::Null);
            let submit_delta = trace
                .get("submit_delta_cents_abs")
                .and_then(Value::as_f64)
                .unwrap_or(0.0)
                .abs();
            let accepted_delta = trace
                .get("accepted_delta_cents_abs")
                .and_then(Value::as_f64)
                .unwrap_or(0.0)
                .abs();
            let order_latency_ms = row
                .get("order_latency_ms")
                .and_then(Value::as_f64)
                .unwrap_or(0.0)
                .max(0.0);
            let event_ts_ms = row
                .get("ack_ts_ms")
                .and_then(Value::as_i64)
                .or_else(|| row.get("submitted_ts_ms").and_then(Value::as_i64))
                .unwrap_or_else(|| Utc::now().timestamp_millis());
            let attempts = row
                .get("attempts")
                .and_then(Value::as_array)
                .map(|v| v.len() as f64)
                .unwrap_or(1.0)
                .max(1.0);

            st.sample_count = st.sample_count.saturating_add(1);
            st.reject_ema = st.reject_ema * 0.86 + if accepted { 0.0 } else { 0.14 };
            st.submit_delta_ema_cents = if st.submit_delta_ema_cents <= 1e-9 {
                submit_delta
            } else {
                st.submit_delta_ema_cents + 0.20 * (submit_delta - st.submit_delta_ema_cents)
            };
            if accepted {
                st.accepted_delta_ema_cents = if st.accepted_delta_ema_cents <= 1e-9 {
                    accepted_delta
                } else {
                    st.accepted_delta_ema_cents
                        + 0.20 * (accepted_delta - st.accepted_delta_ema_cents)
                };
            }
            st.latency_ema_ms = if st.latency_ema_ms <= 1e-9 {
                order_latency_ms
            } else {
                st.latency_ema_ms + 0.16 * (order_latency_ms - st.latency_ema_ms)
            };
            st.attempts_ema = if st.attempts_ema <= 1e-9 {
                attempts
            } else {
                st.attempts_ema + 0.20 * (attempts - st.attempts_ema)
            };

            let latency_boost = ((st.latency_ema_ms - 220.0) / 340.0).clamp(0.0, 1.0) * 0.42;
            let attempts_boost = ((st.attempts_ema - 1.0) / 1.8).clamp(0.0, 1.0) * 0.35;

            let mut target_mult = 1.0
                + (st.submit_delta_ema_cents / 3.2).clamp(0.0, 0.85)
                + (st.reject_ema * 0.65).clamp(0.0, 0.8)
                + latency_boost
                + attempts_boost;
            if accepted {
                target_mult -= 0.04;
            } else if is_liquidity_reject_reason(&err) {
                target_mult += if is_exit_like { 0.16 } else { 0.10 };
            }
            target_mult = target_mult.clamp(0.85, 2.40);

            let mult = if is_exit_like {
                &mut st.exit_slippage_mult
            } else {
                &mut st.entry_slippage_mult
            };
            *mult = (*mult * 0.84 + target_mult * 0.16).clamp(0.85, 2.40);
            if accepted
                && st.reject_ema <= 0.16
                && st.accepted_delta_ema_cents <= 0.9
                && st.latency_ema_ms <= 260.0
                && st.attempts_ema <= 1.2
            {
                *mult = (*mult * 0.96 + 1.0 * 0.04).clamp(0.85, 2.40);
            }
            if !accepted && !err.is_empty() {
                st.last_error = Some(err);
            }
            if !is_exit_like {
                let window_ms = live_entry_liquidity_reject_window_ms();
                if !accepted
                    && st
                        .last_error
                        .as_deref()
                        .map(is_liquidity_reject_reason)
                        .unwrap_or(false)
                {
                    let same_round = st
                        .entry_liquidity_reject_round_id
                        .as_deref()
                        .zip(round_id.as_deref())
                        .map(|(a, b)| a == b)
                        .unwrap_or(false);
                    let same_side = st
                        .entry_liquidity_reject_side
                        .as_deref()
                        .zip(side.as_deref())
                        .map(|(a, b)| a.eq_ignore_ascii_case(b))
                        .unwrap_or(false);
                    let within_window = st.entry_liquidity_reject_last_ts_ms > 0
                        && event_ts_ms.saturating_sub(st.entry_liquidity_reject_last_ts_ms)
                            <= window_ms;
                    if same_round && same_side && within_window {
                        st.entry_liquidity_reject_count =
                            st.entry_liquidity_reject_count.saturating_add(1);
                    } else {
                        st.entry_liquidity_reject_round_id = round_id.clone();
                        st.entry_liquidity_reject_side = side.clone();
                        st.entry_liquidity_reject_count = 1;
                        st.entry_liquidity_reject_first_ts_ms = event_ts_ms;
                    }
                    st.entry_liquidity_reject_last_ts_ms = event_ts_ms;
                } else if accepted {
                    st.entry_liquidity_reject_round_id = None;
                    st.entry_liquidity_reject_side = None;
                    st.entry_liquidity_reject_count = 0;
                    st.entry_liquidity_reject_first_ts_ms = 0;
                    st.entry_liquidity_reject_last_ts_ms = 0;
                }
            }
        }
        st.updated_ts_ms = Utc::now().timestamp_millis();
        st.clone()
    }

    #[allow(dead_code)]
    async fn upsert_pending_order(&self, row: LivePendingOrder) {
        let mut pending = self.live_pending_orders.write().await;
        pending.insert(row.order_id.clone(), row);
    }

    #[allow(dead_code)]
    async fn remove_pending_order(&self, order_id: &str) -> Option<LivePendingOrder> {
        let mut pending = self.live_pending_orders.write().await;
        pending.remove(order_id)
    }

    async fn list_pending_orders(&self) -> Vec<LivePendingOrder> {
        let pending = self.live_pending_orders.read().await;
        pending.values().cloned().collect()
    }

    async fn count_entry_pending_orders(&self) -> usize {
        let pending = self.live_pending_orders.read().await;
        pending
            .values()
            .filter(|row| row.action.eq_ignore_ascii_case("enter"))
            .count()
    }

    async fn list_pending_orders_for_market(
        &self,
        symbol: &str,
        market_type: &str,
    ) -> Vec<LivePendingOrder> {
        let symbol = runtime_scope_symbol(symbol);
        let pending = self.live_pending_orders.read().await;
        pending
            .values()
            .filter(|p| {
                p.market_type.eq_ignore_ascii_case(market_type)
                    && p.symbol.eq_ignore_ascii_case(&symbol)
            })
            .cloned()
            .collect()
    }

    async fn count_open_positions(&self) -> usize {
        let rows = self.live_position_states.read().await;
        rows.values()
            .filter(|row| {
                row.position_size_shares > 0.0
                    || row
                        .side
                        .as_deref()
                        .map(|v| !v.trim().is_empty())
                        .unwrap_or(false)
            })
            .count()
    }

    #[allow(dead_code)]
    async fn pending_flags_for_market(&self, symbol: &str, market_type: &str) -> (bool, bool) {
        let symbol = runtime_scope_symbol(symbol);
        let pending = self.live_pending_orders.read().await;
        let mut has_enter_pending = false;
        let mut has_exit_pending = false;
        for row in pending.values() {
            if !row.market_type.eq_ignore_ascii_case(market_type)
                || !row.symbol.eq_ignore_ascii_case(&symbol)
            {
                continue;
            }
            if row.action.eq_ignore_ascii_case("enter") || row.action.eq_ignore_ascii_case("add") {
                has_enter_pending = true;
            }
            if row.action.eq_ignore_ascii_case("exit") || row.action.eq_ignore_ascii_case("reduce")
            {
                has_exit_pending = true;
            }
            if has_enter_pending && has_exit_pending {
                break;
            }
        }
        (has_enter_pending, has_exit_pending)
    }

    #[allow(dead_code)]
    async fn get_rust_book_cache(&self, token_id: &str) -> Option<GatewayBookSnapshot> {
        let cache = self.live_rust_book_cache.read().await;
        let row = cache.get(token_id)?;
        if row.fetched_at.elapsed() > Duration::from_millis(LIVE_RUST_BOOK_CACHE_TTL_MS) {
            return None;
        }
        Some(row.snapshot.clone())
    }

    #[allow(dead_code)]
    async fn put_rust_book_cache(&self, token_id: &str, snapshot: GatewayBookSnapshot) {
        let mut cache = self.live_rust_book_cache.write().await;
        if cache.len() >= LIVE_RUST_BOOK_CACHE_MAX {
            let mut stale_keys = Vec::new();
            for (k, v) in cache.iter() {
                if v.fetched_at.elapsed() > Duration::from_millis(LIVE_RUST_BOOK_CACHE_TTL_MS) {
                    stale_keys.push(k.clone());
                }
                if stale_keys.len() >= 64 {
                    break;
                }
            }
            for key in stale_keys {
                cache.remove(&key);
            }
            if cache.len() >= LIVE_RUST_BOOK_CACHE_MAX {
                if let Some(first_key) = cache.keys().next().cloned() {
                    cache.remove(&first_key);
                }
            }
        }
        cache.insert(
            token_id.to_string(),
            RustBookCacheEntry {
                fetched_at: Instant::now(),
                snapshot,
            },
        );
    }

    async fn should_emit_alert(&self, key: &str, now_ms: i64, throttle_ms: i64) -> bool {
        let mut slots = self.runtime_alert_throttle.write().await;
        let last = slots.get(key).copied().unwrap_or(0);
        if now_ms.saturating_sub(last) < throttle_ms {
            return false;
        }
        slots.insert(key.to_string(), now_ms);
        if slots.len() > 1024 {
            let cutoff = now_ms.saturating_sub(24 * 3_600_000);
            slots.retain(|_, ts| *ts >= cutoff);
        }
        true
    }

    async fn should_emit_daily_report(&self, market_type: &str, day_key: &str) -> bool {
        let key = format!("{market_type}:{day_key}");
        let mut sent = self.runtime_daily_report_sent.write().await;
        if sent.contains(&key) {
            return false;
        }
        sent.insert(key);
        if sent.len() > 256 {
            let keep_prefix = format!("{}-", Utc::now().year());
            sent.retain(|k| k.contains(&keep_prefix));
        }
        true
    }

    async fn emit_serverchan_markdown(
        &self,
        cfg: &ServerChanConfig,
        title: &str,
        markdown: &str,
    ) -> Result<()> {
        if !cfg.enabled {
            return Ok(());
        }
        let res = self
            .gateway_http_client
            .post(&cfg.api_url)
            .timeout(Duration::from_millis(cfg.request_timeout_ms))
            .form(&[("title", title), ("desp", markdown)])
            .send()
            .await
            .with_context(|| "serverchan request failed")?;
        if !res.status().is_success() {
            let status = res.status();
            let body = res.text().await.unwrap_or_default();
            bail!("serverchan status={} body={}", status, body);
        }
        Ok(())
    }
}

const LIVE_SNAPSHOT_MAX_AGE_MS: i64 = 4_000;
const LIVE_SNAPSHOT_FALLBACK_MAX_AGE_MS: i64 = 12_000;
const LIVE_ROUND_END_GRACE_MS: i64 = 1_500;
const RESOLVED_UP_PRICE_CENTS: f64 = 99.0;
const RESOLVED_DOWN_PRICE_CENTS: f64 = 0.0;

#[derive(Debug, Deserialize)]
struct StrategyOptimizeQueryParams {
    market_type: Option<String>,
    symbol: Option<String>,
    autotune_context: Option<String>,
    lookback_minutes: Option<u32>,
    max_points: Option<u32>,
    max_samples: Option<u32>,
    max_trades: Option<u32>,
    full_history: Option<bool>,
    max_arms: Option<u32>,
    window_trades: Option<u32>,
    target_win_rate: Option<f64>,
    iterations: Option<u32>,
    seed: Option<u64>,
    recent_lookback_minutes: Option<u32>,
    recent_weight: Option<f64>,
    persist_best: Option<bool>,
    persist_ttl_sec: Option<u32>,
    promote_min_trades: Option<f64>,
    promote_min_win_rate: Option<f64>,
    promote_min_pnl: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct StrategyAutotuneLatestQueryParams {
    market_type: Option<String>,
    symbol: Option<String>,
    context: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StrategyAutotuneHistoryQueryParams {
    market_type: Option<String>,
    symbol: Option<String>,
    context: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct StrategyAutotuneSetBody {
    market_type: Option<String>,
    symbol: Option<String>,
    context: Option<String>,
    config: Value,
    ttl_sec: Option<u32>,
    source: Option<String>,
    note: Option<String>,
}

#[derive(Debug, Serialize)]
struct ServiceHealth {
    enabled: bool,
    ok: bool,
    latency_ms: Option<u128>,
    detail: String,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    ok: bool,
    ts_ms: i64,
    clickhouse: ServiceHealth,
    redis: ServiceHealth,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: msg.into(),
        }
    }

    fn not_found(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: msg.into(),
        }
    }

    fn internal(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: msg.into(),
        }
    }

    fn too_many_requests(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            message: msg.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(ErrorBody {
            error: self.message,
        });
        (self.status, body).into_response()
    }
}

pub async fn run_api_server(cfg: ApiConfig) -> Result<()> {
    let bind: SocketAddr = cfg
        .bind
        .parse()
        .map_err(|e| anyhow!("invalid api bind {}: {}", cfg.bind, e))?;
    let redis_client = match cfg.redis_url {
        Some(url) => {
            Some(redis::Client::open(url).map_err(|e| anyhow!("invalid redis url: {}", e))?)
        }
        None => None,
    };
    let redis_manager = if let Some(client) = redis_client.as_ref() {
        match client.get_connection_manager().await {
            Ok(manager) => Some(manager),
            Err(err) => {
                tracing::warn!(
                    ?err,
                    "redis connection manager init failed; redis kv ops degraded"
                );
                None
            }
        }
    } else {
        None
    };
    let gateway_http_client = reqwest::Client::builder()
        .pool_max_idle_per_host(128)
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_nodelay(true)
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());
    let strategy_heavy_slots = std::env::var("FORGE_STRATEGY_HEAVY_MAX_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1)
        .clamp(1, 8);
    let strategy_live_source_slots = std::env::var("FORGE_STRATEGY_LIVE_SOURCE_MAX_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(64)
        .clamp(4, 512);
    let state = ApiState {
        ch_url: cfg.clickhouse_url,
        redis_prefix: cfg.redis_prefix,
        redis_client,
        redis_manager,
        chart_cache: Arc::new(RwLock::new(HashMap::new())),
        live_position_states: Arc::new(RwLock::new(HashMap::new())),
        live_decision_guard: Arc::new(RwLock::new(HashMap::new())),
        live_events: Arc::new(RwLock::new(VecDeque::new())),
        live_event_seq: Arc::new(RwLock::new(0)),
        live_pending_orders: Arc::new(RwLock::new(HashMap::new())),
        live_runtime_snapshots: Arc::new(RwLock::new(HashMap::new())),
        live_recent_active_intents: Arc::new(RwLock::new(HashMap::new())),
        live_runtime_controls: Arc::new(RwLock::new(HashMap::new())),
        live_runtime_paper_reset_anchors: Arc::new(RwLock::new(HashMap::new())),
        live_persist_inflight: Arc::new(RwLock::new(HashSet::new())),
        live_execution_aggr_states: Arc::new(RwLock::new(HashMap::new())),
        live_rust_executor: Arc::new(RwLock::new(None)),
        live_rust_book_cache: Arc::new(RwLock::new(HashMap::new())),
        runtime_alert_throttle: Arc::new(RwLock::new(HashMap::new())),
        runtime_daily_report_sent: Arc::new(RwLock::new(HashSet::new())),
        strategy_heavy_slots: Arc::new(Semaphore::new(strategy_heavy_slots)),
        strategy_live_source_slots: Arc::new(Semaphore::new(strategy_live_source_slots)),
        runtime_event_samples: Arc::new(RwLock::new(HashMap::new())),
        gateway_http_client: Arc::new(gateway_http_client),
        live_kill_switch: Arc::new(RwLock::new(HashMap::new())),
    };

    let mut app = Router::new()
        .route("/", get(root_redirect))
        .route("/dashboard", get(dashboard_redirect))
        .route("/ws/live", get(ws_live))
        .route("/health/live", get(health_live))
        .route("/health/db", get(health_db))
        .route("/api/latest/all", get(latest_all))
        .route(
            "/api/history/{symbol}/{timeframe}",
            get(history_symbol_timeframe),
        )
        .route("/api/stats", get(stats))
        .route("/api/collector/status", get(collector_status))
        .route("/api/collector/metrics", get(collector_metrics))
        .route("/api/chart", get(chart))
        .route("/api/chart/round", get(chart_round))
        .route("/api/rounds", get(rounds))
        .route("/api/rounds/available", get(rounds_available))
        .route("/api/heatmap", get(heatmap))
        .route("/api/accuracy_series", get(accuracy_series))
        .route("/api/strategy/paper", get(strategy_paper))
        .route("/api/strategy/history_paper", get(strategy_history_paper))
        .route("/api/strategy/runtime", get(strategy_runtime))
        .route("/api/strategy/replay", get(strategy_replay))
        .route("/api/strategy/paper_ledger", get(strategy_paper_ledger))
        .route(
            "/api/strategy/execution_attribution",
            get(strategy_execution_attribution),
        )
        .route("/api/strategy/optimize", get(strategy_optimize))
        .route(
            "/api/strategy/autotune/latest",
            get(strategy_autotune_latest),
        )
        .route(
            "/api/strategy/autotune/history",
            get(strategy_autotune_history),
        )
        .route("/api/strategy/autotune/set", post(strategy_autotune_set))
        .route("/api/strategy/live/reset", post(strategy_live_reset))
        .route(
            "/api/strategy/live/reconcile_resolved",
            post(strategy_live_reconcile_resolved),
        )
        .route("/api/strategy/live/control", post(strategy_live_control))
        .route("/api/strategy/live/events", get(strategy_live_events))
        .with_state(state.clone());

    if let Some(dist) = cfg.dashboard_dist_dir {
        let index_path = format!("{}/index.html", dist);
        if Path::new(&index_path).exists() {
            let svc =
                ServeDir::new(dist.clone()).not_found_service(ServeFile::new(index_path.clone()));
            app = app.nest_service("/dashboard/", svc);
            let assets_dir = format!("{}/assets", dist);
            if Path::new(&assets_dir).exists() {
                app = app.nest_service("/assets", ServeDir::new(assets_dir));
            }
            tracing::info!(dist = %dist, "dashboard static enabled");
        } else {
            tracing::warn!(dist = %dist, "dashboard dist missing; skip static");
        }
    }

    start_live_runtime(state.clone());

    tracing::info!(bind = %bind, "forge api server started");
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

fn live_position_state_key(redis_prefix: &str, symbol: &str, market_type: &str) -> String {
    format!(
        "{redis_prefix}:fev1:live:position:{}:{market_type}",
        symbol.trim().to_ascii_uppercase()
    )
}

fn live_pending_orders_key(redis_prefix: &str) -> String {
    format!("{redis_prefix}:fev1:live:pending")
}

fn live_events_key(redis_prefix: &str, symbol: &str, market_type: &str) -> String {
    format!(
        "{redis_prefix}:fev1:live:events:{}:{market_type}",
        symbol.trim().to_ascii_uppercase()
    )
}

fn live_runtime_control_key(redis_prefix: &str, symbol: &str, market_type: &str) -> String {
    format!(
        "{redis_prefix}:fev1:live:runtime_control:{}:{market_type}",
        symbol.trim().to_ascii_uppercase()
    )
}

fn paper_ledger_key(redis_prefix: &str, symbol: &str, market_type: &str) -> String {
    format!(
        "{redis_prefix}:fev1:paper:ledger:{}:{market_type}",
        symbol.trim().to_ascii_uppercase()
    )
}

fn paper_ledger_clickhouse_database() -> String {
    std::env::var("FORGE_CH_DATABASE")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "polyedge_forge".to_string())
}

fn paper_ledger_clickhouse_table() -> String {
    std::env::var("FORGE_CH_PAPER_LEDGER_TABLE")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "paper_ledger_trades".to_string())
}

fn paper_ledger_clickhouse_ttl_days() -> u16 {
    std::env::var("FORGE_CH_PAPER_LEDGER_TTL_DAYS")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(365)
        .clamp(14, 3650)
}

fn live_runtime_paper_reset_anchor_key(
    redis_prefix: &str,
    symbol: &str,
    market_type: &str,
) -> String {
    format!(
        "{redis_prefix}:fev1:paper:reset_anchor:{}:{market_type}",
        symbol.trim().to_ascii_uppercase()
    )
}

fn paper_ledger_max_trades() -> usize {
    std::env::var("FORGE_FEV1_PAPER_LEDGER_MAX_TRADES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(5_000)
        .clamp(100, 50_000)
}

fn extract_trade_row_id(row: &Value) -> Option<i64> {
    row.get("id").and_then(Value::as_i64)
}

fn extract_trade_row_key(row: &Value) -> Option<String> {
    let entry_ts_ms = row.get("entry_ts_ms").and_then(Value::as_i64).unwrap_or(0);
    let side = row
        .get("side")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or("unknown");
    let round_id = row
        .get("entry_round_id")
        .and_then(Value::as_str)
        .or_else(|| row.get("round_id").and_then(Value::as_str))
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or("unassigned");
    let entry_price_raw_cents = row
        .get("entry_price_raw_cents")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let entry_price_cents = row
        .get("entry_price_cents")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    if entry_ts_ms <= 0 {
        return None;
    }
    Some(format!(
        "{}:{}:{}:{entry_price_raw_cents:.4}:{entry_price_cents:.4}",
        round_id, side, entry_ts_ms
    ))
}

fn trade_row_ts_ms(row: &Value) -> i64 {
    row.get("entry_ts_ms")
        .and_then(Value::as_i64)
        .or_else(|| row.get("exit_ts_ms").and_then(Value::as_i64))
        .or_else(|| row.get("ts_ms").and_then(Value::as_i64))
        .unwrap_or(0)
}

fn decision_row_ts_ms(row: &Value) -> i64 {
    row.get("ts_ms")
        .and_then(Value::as_i64)
        .or_else(|| row.get("timestamp_ms").and_then(Value::as_i64))
        .unwrap_or(0)
}

fn filter_trade_rows_after_reset(rows: &[Value], reset_after_ms: Option<i64>) -> Vec<Value> {
    match reset_after_ms.filter(|v| *v > 0) {
        Some(reset_after_ms) => rows
            .iter()
            .filter(|row| trade_row_ts_ms(row) >= reset_after_ms)
            .cloned()
            .collect(),
        None => rows.to_vec(),
    }
}

fn filter_decision_rows_after_reset(rows: &[Value], reset_after_ms: Option<i64>) -> Vec<Value> {
    match reset_after_ms.filter(|v| *v > 0) {
        Some(reset_after_ms) => rows
            .iter()
            .filter(|row| decision_row_ts_ms(row) >= reset_after_ms)
            .cloned()
            .collect(),
        None => rows.to_vec(),
    }
}

fn merge_unique_trade_rows(existing: &[Value], fresh: &[Value], max_rows: usize) -> Vec<Value> {
    let mut merged = existing
        .iter()
        .filter_map(|row| extract_trade_row_key(row).map(|id| (id, row.clone())))
        .collect::<HashMap<_, _>>();
    for row in fresh {
        if let Some(id) = extract_trade_row_key(row) {
            merged.insert(id, row.clone());
        }
    }
    let mut rows = merged.into_iter().collect::<Vec<_>>();
    rows.sort_by_key(|(_, row)| {
        (
            trade_row_ts_ms(row),
            extract_trade_row_id(row).unwrap_or_default(),
        )
    });
    let trim = rows.len().saturating_sub(max_rows);
    rows.into_iter().skip(trim).map(|(_, row)| row).collect()
}

fn diff_trade_rows(existing: &[Value], merged: &[Value]) -> Vec<Value> {
    let existing_keys = existing
        .iter()
        .filter_map(extract_trade_row_key)
        .collect::<HashSet<_>>();
    merged
        .iter()
        .filter(|row| {
            extract_trade_row_key(row)
                .map(|key| !existing_keys.contains(&key))
                .unwrap_or(false)
        })
        .cloned()
        .collect()
}

fn merge_unique_decision_rows(existing: &[Value], fresh: &[Value], max_rows: usize) -> Vec<Value> {
    let mut merged = existing
        .iter()
        .filter_map(|row| extract_decision_id(row).map(|id| (id, row.clone())))
        .collect::<HashMap<_, _>>();
    for row in fresh {
        if let Some(id) = extract_decision_id(row) {
            merged.insert(id, row.clone());
        }
    }
    let mut rows = merged.into_iter().map(|(_, row)| row).collect::<Vec<_>>();
    rows.sort_by_key(|row| row.get("ts_ms").and_then(Value::as_i64).unwrap_or(0));
    let trim = rows.len().saturating_sub(max_rows);
    rows.into_iter().skip(trim).collect()
}

fn summarize_trade_rows(trades: &[Value]) -> Value {
    if trades.is_empty() {
        return json!({
            "trade_count": 0,
            "win_rate_pct": 0.0,
            "avg_pnl_cents": 0.0,
            "avg_duration_s": 0.0,
            "total_pnl_cents": 0.0,
            "net_pnl_cents": 0.0,
            "gross_pnl_cents": 0.0,
            "total_cost_cents": 0.0,
            "total_entry_fee_cents": 0.0,
            "total_exit_fee_cents": 0.0,
            "total_slippage_cents": 0.0,
            "total_impact_cents": 0.0,
            "net_margin_pct": 0.0,
            "max_drawdown_cents": 0.0,
        });
    }
    let mut trade_count = 0usize;
    let mut win_count = 0usize;
    let mut total_net = 0.0_f64;
    let mut total_gross = 0.0_f64;
    let mut total_cost = 0.0_f64;
    let mut total_entry_fee = 0.0_f64;
    let mut total_exit_fee = 0.0_f64;
    let mut total_slippage = 0.0_f64;
    let mut total_impact = 0.0_f64;
    let mut total_duration_s = 0.0_f64;
    let mut equity = 0.0_f64;
    let mut peak = 0.0_f64;
    let mut max_drawdown = 0.0_f64;

    for row in trades {
        let pnl_net = row
            .get("pnl_net_cents")
            .and_then(Value::as_f64)
            .or_else(|| row.get("pnl_cents").and_then(Value::as_f64))
            .or_else(|| row.get("net_pnl_cents").and_then(Value::as_f64))
            .unwrap_or(0.0);
        let pnl_gross = row
            .get("pnl_gross_cents")
            .and_then(Value::as_f64)
            .unwrap_or(pnl_net);
        let cost = row
            .get("total_cost_cents")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let entry_fee = row
            .get("entry_fee_cents")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let exit_fee = row
            .get("exit_fee_cents")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let slip = row
            .get("entry_slippage_cents")
            .and_then(Value::as_f64)
            .unwrap_or(0.0)
            + row
                .get("exit_slippage_cents")
                .and_then(Value::as_f64)
                .unwrap_or(0.0)
            + row
                .get("exit_emergency_penalty_cents")
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
        let impact = row
            .get("entry_impact_cents")
            .and_then(Value::as_f64)
            .unwrap_or(0.0)
            + row
                .get("exit_impact_cents")
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
        let duration_s = row.get("duration_s").and_then(Value::as_f64).unwrap_or(0.0);

        trade_count = trade_count.saturating_add(1);
        if pnl_net >= 0.0 {
            win_count = win_count.saturating_add(1);
        }
        total_net += pnl_net;
        total_gross += pnl_gross;
        total_cost += cost;
        total_entry_fee += entry_fee;
        total_exit_fee += exit_fee;
        total_slippage += slip;
        total_impact += impact;
        total_duration_s += duration_s.max(0.0);

        equity += pnl_net;
        peak = peak.max(equity);
        max_drawdown = max_drawdown.max((peak - equity).max(0.0));
    }

    let trade_count_f = trade_count as f64;
    let win_rate_pct = if trade_count > 0 {
        (win_count as f64 / trade_count_f) * 100.0
    } else {
        0.0
    };
    let avg_pnl_cents = if trade_count > 0 {
        total_net / trade_count_f
    } else {
        0.0
    };
    let avg_duration_s = if trade_count > 0 {
        total_duration_s / trade_count_f
    } else {
        0.0
    };
    let net_margin_pct = if total_gross.abs() > f64::EPSILON {
        (total_net / total_gross) * 100.0
    } else {
        0.0
    };
    json!({
        "trade_count": trade_count,
        "win_rate_pct": win_rate_pct,
        "avg_pnl_cents": avg_pnl_cents,
        "avg_duration_s": avg_duration_s,
        "total_pnl_cents": total_net,
        "net_pnl_cents": total_net,
        "gross_pnl_cents": total_gross,
        "total_cost_cents": total_cost,
        "total_entry_fee_cents": total_entry_fee,
        "total_exit_fee_cents": total_exit_fee,
        "total_slippage_cents": total_slippage,
        "total_impact_cents": total_impact,
        "net_margin_pct": net_margin_pct,
        "max_drawdown_cents": max_drawdown,
    })
}

fn build_paper_ledger_payload(
    existing: Option<&Value>,
    runtime_payload: &Value,
    symbol: &str,
    market_type: &str,
    reset_after_ms: Option<i64>,
) -> Value {
    build_paper_ledger_payload_from_sources(
        existing,
        Some(runtime_payload),
        &[],
        symbol,
        market_type,
        reset_after_ms,
    )
}

fn build_paper_ledger_payload_from_sources(
    existing: Option<&Value>,
    runtime_payload: Option<&Value>,
    historical_trades: &[Value],
    symbol: &str,
    market_type: &str,
    reset_after_ms: Option<i64>,
) -> Value {
    let max_trades = paper_ledger_max_trades();
    let base_payload = runtime_payload.or(existing);
    let existing_trades = if historical_trades.is_empty() {
        existing
            .and_then(|v| v.get("trades"))
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    } else {
        historical_trades
    };
    let fresh_trades = runtime_payload
        .and_then(|v| v.get("trades"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let filtered_existing_trades = filter_trade_rows_after_reset(existing_trades, reset_after_ms);
    let filtered_fresh_trades = filter_trade_rows_after_reset(fresh_trades, reset_after_ms);
    let merged_trades = merge_unique_trade_rows(
        &filtered_existing_trades,
        &filtered_fresh_trades,
        max_trades,
    );

    let existing_records = existing
        .and_then(|v| v.get("paper_records"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let fresh_records = runtime_payload
        .and_then(|v| v.get("live_execution"))
        .and_then(|v| v.get("paper_records"))
        .and_then(Value::as_array)
        .or_else(|| {
            runtime_payload
                .and_then(|v| v.get("signal_decisions"))
                .and_then(Value::as_array)
        })
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let filtered_existing_records =
        filter_decision_rows_after_reset(existing_records, reset_after_ms);
    let filtered_fresh_records = filter_decision_rows_after_reset(fresh_records, reset_after_ms);
    let merged_records = merge_unique_decision_rows(
        &filtered_existing_records,
        &filtered_fresh_records,
        max_trades * 4,
    );

    let existing_live_records = existing
        .and_then(|v| v.get("live_records"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let fresh_live_records = runtime_payload
        .and_then(|v| v.get("live_execution"))
        .and_then(|v| v.get("live_records"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let filtered_existing_live_records =
        filter_decision_rows_after_reset(existing_live_records, reset_after_ms);
    let filtered_fresh_live_records =
        filter_decision_rows_after_reset(fresh_live_records, reset_after_ms);
    let merged_live_records = merge_unique_decision_rows(
        &filtered_existing_live_records,
        &filtered_fresh_live_records,
        max_trades * 4,
    );

    json!({
        "source": "ledger",
        "history_source": if historical_trades.is_empty() { "runtime_redis" } else { "clickhouse_plus_runtime" },
        "view": {
            "family": "ledger",
            "label": "Paper Ledger",
        },
        "symbol": base_payload
            .and_then(|v| v.get("symbol"))
            .cloned()
            .unwrap_or_else(|| json!(runtime_scope_symbol(symbol))),
        "market_type": base_payload
            .and_then(|v| v.get("market_type"))
            .cloned()
            .unwrap_or_else(|| json!(market_type)),
        "updated_at_ms": Utc::now().timestamp_millis(),
        "runtime_defaults": base_payload.and_then(|v| v.get("runtime_defaults")).cloned().unwrap_or(Value::Null),
        "runtime_control": base_payload.and_then(|v| v.get("runtime_control")).cloned().unwrap_or(Value::Null),
        "execution_aggressiveness": base_payload.and_then(|v| v.get("execution_aggressiveness")).cloned().unwrap_or(Value::Null),
        "lookback_minutes": base_payload.and_then(|v| v.get("lookback_minutes")).cloned().unwrap_or_else(|| json!(0)),
        "lookback": base_payload.and_then(|v| v.get("lookback")).cloned().unwrap_or(Value::Null),
        "samples": base_payload.and_then(|v| v.get("samples")).cloned().unwrap_or_else(|| json!(0)),
        "paper_reset_after_ms": reset_after_ms,
        "historical_trade_count": merged_trades.len(),
        "config_source": base_payload.and_then(|v| v.get("config_source")).cloned().unwrap_or(Value::Null),
        "baseline_profile": base_payload.and_then(|v| v.get("baseline_profile")).cloned().unwrap_or(Value::Null),
        "config": base_payload.and_then(|v| v.get("config")).cloned().unwrap_or(Value::Null),
        "current": base_payload.and_then(|v| v.get("current")).cloned().unwrap_or(Value::Null),
        "summary": summarize_trade_rows(&merged_trades),
        "trades": merged_trades,
        "paper_records": merged_records,
        "live_records": merged_live_records,
    })
}

#[derive(Debug, Clone, Serialize)]
struct PaperLedgerTradeInsertRow {
    symbol: String,
    market_type: String,
    trade_key: String,
    trade_id: i64,
    side: String,
    entry_round_id: String,
    entry_ts_ms: i64,
    exit_ts_ms: i64,
    pnl_net_cents: f64,
    trade_json: String,
    persisted_at_ms: i64,
}

async fn ensure_paper_ledger_clickhouse_table(ch_url: &str) -> Result<(), ApiError> {
    let db = paper_ledger_clickhouse_database();
    let table = paper_ledger_clickhouse_table();
    let ttl_days = paper_ledger_clickhouse_ttl_days();
    execute_clickhouse_command(ch_url, &format!("CREATE DATABASE IF NOT EXISTS {}", db)).await?;
    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (
            symbol LowCardinality(String),
            market_type LowCardinality(String),
            trade_key String,
            trade_id Int64,
            side LowCardinality(String),
            entry_round_id String,
            entry_ts_ms Int64,
            exit_ts_ms Int64,
            pnl_net_cents Float64,
            trade_json String,
            persisted_at_ms Int64,
            ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
        )
        ENGINE = ReplacingMergeTree(persisted_at_ms)
        PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(exit_ts_ms))
        ORDER BY (symbol, market_type, trade_key)
        TTL fromUnixTimestamp64Milli(exit_ts_ms) + INTERVAL {} DAY DELETE",
        db, table, ttl_days
    );
    execute_clickhouse_command(ch_url, &create_sql).await
}

async fn persist_paper_ledger_trades_to_clickhouse(
    state: &ApiState,
    symbol: &str,
    market_type: &str,
    rows: &[Value],
) -> Result<(), ApiError> {
    let Some(ch_url) = state.ch_url.as_deref() else {
        return Ok(());
    };
    if rows.is_empty() {
        return Ok(());
    }
    ensure_paper_ledger_clickhouse_table(ch_url).await?;
    let db = paper_ledger_clickhouse_database();
    let table = paper_ledger_clickhouse_table();
    let query = format!("INSERT INTO {}.{} FORMAT JSONEachRow", db, table);
    let persisted_at_ms = Utc::now().timestamp_millis();
    let mut body = String::with_capacity(rows.len().saturating_mul(800));
    for row in rows {
        let Some(trade_key) = extract_trade_row_key(row) else {
            continue;
        };
        let line = serde_json::to_string(&PaperLedgerTradeInsertRow {
            symbol: runtime_scope_symbol(symbol),
            market_type: market_type.to_ascii_lowercase(),
            trade_key,
            trade_id: extract_trade_row_id(row).unwrap_or_default(),
            side: row
                .get("side")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            entry_round_id: row
                .get("entry_round_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            entry_ts_ms: row
                .get("entry_ts_ms")
                .and_then(Value::as_i64)
                .unwrap_or_default(),
            exit_ts_ms: row
                .get("exit_ts_ms")
                .and_then(Value::as_i64)
                .unwrap_or_default(),
            pnl_net_cents: row
                .get("pnl_net_cents")
                .and_then(Value::as_f64)
                .or_else(|| row.get("pnl_cents").and_then(Value::as_f64))
                .unwrap_or_default(),
            trade_json: serde_json::to_string(row).unwrap_or_else(|_| "{}".to_string()),
            persisted_at_ms,
        })
        .map_err(|e| ApiError::internal(format!("paper ledger row serialize failed: {}", e)))?;
        body.push_str(&line);
        body.push('\n');
    }
    if body.is_empty() {
        return Ok(());
    }
    insert_clickhouse_json_each_row(ch_url, &query, body).await
}

async fn load_clickhouse_paper_ledger_trades(
    state: &ApiState,
    symbol: &str,
    market_type: &str,
    lookback_minutes: u32,
    reset_after_ms: Option<i64>,
) -> Result<Vec<Value>, ApiError> {
    let Some(ch_url) = state.ch_url.as_deref() else {
        return Ok(Vec::new());
    };
    let db = paper_ledger_clickhouse_database();
    let table = paper_ledger_clickhouse_table();
    let safe_symbol = runtime_scope_symbol(symbol).replace('\'', "''");
    let safe_market = market_type.trim().to_ascii_lowercase().replace('\'', "''");
    let lookback_cutoff_ms = Utc::now()
        .timestamp_millis()
        .saturating_sub((lookback_minutes.max(1) as i64) * 60_000);
    let cutoff_ms = reset_after_ms
        .filter(|v| *v > 0)
        .map(|v| v.max(lookback_cutoff_ms))
        .unwrap_or(lookback_cutoff_ms);
    let query = format!(
        "SELECT trade_json
         FROM {}.{} FINAL
         WHERE symbol = '{}' AND market_type = '{}' AND exit_ts_ms >= {}
         ORDER BY exit_ts_ms ASC
         LIMIT {}
         FORMAT JSON",
        db,
        table,
        safe_symbol,
        safe_market,
        cutoff_ms,
        paper_ledger_max_trades()
    );
    let json = match query_clickhouse_json(ch_url, &query).await {
        Ok(v) => v,
        Err(err) => {
            let message = err.message.to_ascii_uppercase();
            if message.contains("UNKNOWN_TABLE") || message.contains("UNKNOWN_DATABASE") {
                return Ok(Vec::new());
            }
            return Err(err);
        }
    };
    Ok(rows_from_json(json)
        .into_iter()
        .filter_map(|row| {
            row.get("trade_json")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .filter_map(|raw| serde_json::from_str::<Value>(&raw).ok())
        .collect())
}

async fn load_live_runtime_samples(
    state: &ApiState,
    symbol: &str,
    market_type: &str,
    lookback_minutes: u32,
    max_points: u32,
) -> Result<Arc<Vec<StrategySample>>, ApiError> {
    let reset_after_ms = state
        .get_runtime_paper_reset_anchor(symbol, market_type)
        .await;
    let filtered = |samples: Arc<Vec<StrategySample>>| -> Arc<Vec<StrategySample>> {
        let Some(reset_after_ms) = reset_after_ms.filter(|v| *v > 0) else {
            return samples;
        };
        Arc::new(
            samples
                .iter()
                .filter(|row| row.ts_ms >= reset_after_ms)
                .cloned()
                .collect::<Vec<_>>(),
        )
    };
    if let Some(samples) = state
        .get_runtime_event_samples(symbol, market_type, lookback_minutes, max_points)
        .await
    {
        return Ok(build_decision_samples_1s(&filtered(samples)));
    }
    let samples = load_strategy_samples_runtime_stream(
        state,
        symbol,
        market_type,
        lookback_minutes,
        max_points,
    )
    .await?;
    Ok(filtered(samples))
}

async fn prewarm_live_runtime_target_and_books(state: &ApiState, symbol: &str, market_type: &str) {
    let Ok(target) = resolve_live_market_target_with_state(state, symbol, market_type).await else {
        return;
    };
    let _ = prewarm_rust_books_for_target(state, &target).await;
}

async fn live_runtime_background_maintenance(state: ApiState, bootstrap: LiveRuntimeConfig) {
    let mut cfg = bootstrap;
    let mut target_keepalive_at = HashMap::<String, i64>::new();
    let mut book_prewarm_at = HashMap::<String, i64>::new();
    let mut last_env_refresh_ms = 0_i64;
    loop {
        let now_ms = Utc::now().timestamp_millis();
        if now_ms.saturating_sub(last_env_refresh_ms) >= 1_000 {
            cfg = LiveRuntimeConfig::from_env();
            let scope_keys = runtime_scope_pairs(&cfg)
                .into_iter()
                .map(|(symbol, market)| runtime_scope_key(&symbol, &market))
                .collect::<HashSet<_>>();
            target_keepalive_at.retain(|k, _| scope_keys.contains(k));
            book_prewarm_at.retain(|k, _| scope_keys.contains(k));
            last_env_refresh_ms = now_ms;
        }
        if !cfg.enabled {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }
        let target_keepalive_ms = runtime_target_keepalive_ms().max(400);
        let book_prewarm_ms = (LIVE_RUST_BOOK_CACHE_TTL_MS as i64)
            .saturating_sub(60)
            .max(120);
        for (symbol, market) in runtime_scope_pairs(&cfg) {
            let scope_key = runtime_scope_key(&symbol, &market);
            let last_target = target_keepalive_at.get(&scope_key).copied().unwrap_or(0);
            let last_book = book_prewarm_at.get(&scope_key).copied().unwrap_or(0);
            let due_target = now_ms.saturating_sub(last_target) >= target_keepalive_ms;
            let due_book = now_ms.saturating_sub(last_book) >= book_prewarm_ms;
            if !(due_target || due_book) {
                continue;
            }
            prewarm_live_runtime_target_and_books(&state, &symbol, &market).await;
            if due_target {
                target_keepalive_at.insert(scope_key.clone(), now_ms);
            }
            if due_book {
                book_prewarm_at.insert(scope_key, now_ms);
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await; // OPTIMIZED: 120ms -> 50ms for faster prewarming
    }
}

fn start_live_runtime(state: ApiState) {
    let cfg = LiveRuntimeConfig::from_env();
    if !cfg.enabled {
        tracing::info!(
            "fev1 live runtime starts in paused mode (FORGE_FEV1_RUNTIME_ENABLED=false)"
        );
    }
    let (wake_tx, wake_rx) = mpsc::unbounded_channel::<LiveRuntimeWakeEvent>();
    if state.redis_client.is_some() {
        let state_for_listener = state.clone();
        let tx_for_listener = wake_tx.clone();
        tokio::spawn(async move {
            live_runtime_snapshot_event_listener(state_for_listener, tx_for_listener).await;
        });
    } else {
        tracing::warn!(
            "redis unavailable: runtime event-driven wakeup disabled, fallback to polling"
        );
    }
    let runtime_state = state.clone();
    let runtime_cfg = cfg.clone();
    tokio::spawn(async move {
        if let Err(err) = restore_live_runtime_state(&runtime_state, &runtime_cfg).await {
            tracing::warn!(?err, "restore live runtime state failed");
        }
        live_runtime_loop(runtime_state, runtime_cfg, wake_rx).await;
    });
    let maintenance_state = state.clone();
    let maintenance_cfg = cfg.clone();
    tokio::spawn(async move {
        live_runtime_background_maintenance(maintenance_state, maintenance_cfg).await;
    });
}

async fn live_runtime_snapshot_event_listener(
    state: ApiState,
    wake_tx: mpsc::UnboundedSender<LiveRuntimeWakeEvent>,
) {
    let Some(client) = state.redis_client.clone() else {
        return;
    };
    let channel = live_snapshot_event_channel(&state.redis_prefix);
    loop {
        match client.get_async_pubsub().await {
            Ok(mut pubsub) => {
                if let Err(err) = pubsub.subscribe(&channel).await {
                    tracing::warn!(?err, channel = %channel, "runtime pubsub subscribe failed");
                    tokio::time::sleep(Duration::from_millis(900)).await;
                    continue;
                }
                tracing::info!(channel = %channel, "runtime pubsub subscribed");
                let mut stream = pubsub.on_message();
                while let Some(msg) = stream.next().await {
                    let payload: String = match msg.get_payload() {
                        Ok(v) => v,
                        Err(err) => {
                            tracing::warn!(?err, "runtime pubsub payload decode failed");
                            continue;
                        }
                    };
                    let payload_value: Value = match serde_json::from_str(&payload) {
                        Ok(v) => v,
                        Err(err) => {
                            tracing::warn!(?err, "runtime pubsub json decode failed");
                            continue;
                        }
                    };
                    let Some((symbol, market_type, sample_ts_ms)) =
                        parse_snapshot_event(&payload_value)
                    else {
                        continue;
                    };
                    if let Some((sample_symbol, sample_tf, _, sample)) =
                        strategy_sample_from_snapshot_event(&payload_value)
                    {
                        state
                            .upsert_runtime_event_sample(&sample_symbol, &sample_tf, sample)
                            .await;
                    }
                    let _ = wake_tx.send(LiveRuntimeWakeEvent {
                        symbol,
                        market_type,
                        source: "redis_snapshot_event".to_string(),
                        ts_ms: Utc::now().timestamp_millis(),
                        sample_ts_ms,
                    });
                }
                tracing::warn!(channel = %channel, "runtime pubsub stream closed, reconnecting");
            }
            Err(err) => {
                tracing::warn!(?err, "runtime pubsub connect failed");
            }
        }
        tokio::time::sleep(Duration::from_millis(900)).await;
    }
}

async fn restore_live_runtime_state(
    state: &ApiState,
    cfg: &LiveRuntimeConfig,
) -> Result<(), ApiError> {
    let mut restored_events = Vec::<Value>::new();
    let mut max_event_seq: u64 = 0;
    for (symbol, market) in runtime_scope_pairs(cfg) {
        let key = live_position_state_key(&state.redis_prefix, &symbol, &market);
        if let Some(v) = read_key_value(state, &key).await? {
            if let Ok(parsed) = serde_json::from_value::<LivePositionState>(v) {
                if parsed.symbol.eq_ignore_ascii_case(&symbol) {
                    state
                        .put_live_position_state(&symbol, &market, parsed)
                        .await;
                } else {
                    tracing::warn!(
                        market_type = %market,
                        persisted_symbol = %parsed.symbol,
                        runtime_symbol = %symbol,
                        "skip restoring live position state due to symbol mismatch"
                    );
                }
            }
        }
        let control_key = live_runtime_control_key(&state.redis_prefix, &symbol, &market);
        if let Some(v) = read_key_value(state, &control_key).await? {
            if let Ok(parsed) = serde_json::from_value::<LiveRuntimeControl>(v) {
                state
                    .put_live_runtime_control(&symbol, &market, parsed)
                    .await;
            }
        }
        let reset_anchor_key =
            live_runtime_paper_reset_anchor_key(&state.redis_prefix, &symbol, &market);
        if let Some(v) = read_key_value(state, &reset_anchor_key).await? {
            if let Some(reset_after_ms) = v.as_i64().filter(|v| *v > 0) {
                state
                    .put_runtime_paper_reset_anchor(&symbol, &market, reset_after_ms)
                    .await;
            }
        }
        let events_key = live_events_key(&state.redis_prefix, &symbol, &market);
        if let Some(v) = read_key_value(state, &events_key).await? {
            if let Ok(rows) = serde_json::from_value::<Vec<Value>>(v) {
                for ev in rows {
                    if let Some(seq) = ev.get("event_seq").and_then(Value::as_u64) {
                        max_event_seq = max_event_seq.max(seq);
                    }
                    restored_events.push(ev);
                }
            }
        }
    }
    let pending_key = live_pending_orders_key(&state.redis_prefix);
    if let Some(v) = read_key_value(state, &pending_key).await? {
        if let Ok(rows) = serde_json::from_value::<Vec<LivePendingOrder>>(v) {
            let mut map = state.live_pending_orders.write().await;
            map.clear();
            for row in rows {
                map.insert(row.order_id.clone(), row);
            }
        }
    }
    if !restored_events.is_empty() {
        restored_events.sort_by_key(|ev| ev.get("ts_ms").and_then(Value::as_i64).unwrap_or(0));
        let max_len = live_event_log_max();
        let mut events = state.live_events.write().await;
        events.clear();
        for ev in restored_events {
            events.push_back(ev);
            while events.len() > max_len {
                events.pop_front();
            }
        }
    }
    {
        let mut seq = state.live_event_seq.write().await;
        *seq = max_event_seq;
    }
    for symbol in &cfg.symbols {
        state
            .preload_runtime_event_samples_from_redis(symbol, &cfg.markets)
            .await;
    }
    Ok(())
}

async fn persist_live_runtime_state(state: &ApiState, symbol: &str, market_type: &str) {
    let position = state.get_live_position_state(symbol, market_type).await;
    let pos_key = live_position_state_key(&state.redis_prefix, symbol, market_type);
    let _ = write_key_value(state, &pos_key, &json!(position), Some(2 * 24 * 3600)).await;
    let runtime_control = state.get_live_runtime_control(symbol, market_type).await;
    let control_key = live_runtime_control_key(&state.redis_prefix, symbol, market_type);
    let _ = write_key_value(
        state,
        &control_key,
        &json!(runtime_control),
        Some(2 * 24 * 3600),
    )
    .await;

    let pending_rows = state.list_pending_orders().await;
    let pending_key = live_pending_orders_key(&state.redis_prefix);
    let _ = write_key_value(
        state,
        &pending_key,
        &json!(pending_rows),
        Some(2 * 24 * 3600),
    )
    .await;

    let events_key = live_events_key(&state.redis_prefix, symbol, market_type);
    let events_rows = state
        .list_live_events(symbol, market_type, live_event_log_max())
        .await;
    let _ = write_key_value(
        state,
        &events_key,
        &json!(events_rows),
        Some(live_event_log_ttl_sec()),
    )
    .await;

    if let Some(runtime_payload) = state.get_runtime_snapshot(symbol, market_type).await {
        let ledger_key = paper_ledger_key(&state.redis_prefix, symbol, market_type);
        let existing = read_key_value(state, &ledger_key).await.ok().flatten();
        let reset_after_ms = state
            .get_runtime_paper_reset_anchor(symbol, market_type)
            .await;
        let ledger_payload = build_paper_ledger_payload(
            existing.as_ref(),
            &runtime_payload,
            symbol,
            market_type,
            reset_after_ms,
        );
        let existing_trades = existing
            .as_ref()
            .and_then(|v| v.get("trades"))
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let merged_trades = ledger_payload
            .get("trades")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let new_trades = diff_trade_rows(existing_trades, merged_trades);
        if let Err(err) =
            persist_paper_ledger_trades_to_clickhouse(state, symbol, market_type, &new_trades).await
        {
            tracing::warn!(
                symbol = symbol,
                market_type = market_type,
                error = %err.message,
                "paper ledger clickhouse persist failed"
            );
        }
        let _ = write_key_value(state, &ledger_key, &ledger_payload, Some(7 * 24 * 3600)).await;
    }
}

async fn live_runtime_loop(
    state: ApiState,
    bootstrap: LiveRuntimeConfig,
    mut wake_rx: mpsc::UnboundedReceiver<LiveRuntimeWakeEvent>,
) {
    tracing::info!(
        markets = ?bootstrap.markets,
        symbols = ?bootstrap.symbols,
        live_execute = bootstrap.live_execute,
        loop_ms = bootstrap.loop_interval_ms,
        "fev1 live runtime started"
    );
    let mut last_round_seen: HashMap<String, String> = HashMap::new();
    let mut strategy_engines: HashMap<String, LiveRuntimeStrategyEngineState> = HashMap::new();
    let mut market_next_due_ms: HashMap<String, i64> = HashMap::new();
    let mut market_last_strategy_eval_ms: HashMap<String, i64> = HashMap::new();
    let mut market_last_strategy_sample_ts_ms: HashMap<String, i64> = HashMap::new();
    let snapshot_event_available = state.redis_client.is_some();
    let mut cached_cfg = LiveRuntimeConfig::from_env();
    let mut cached_push_cfg = ServerChanConfig::from_env();
    let mut cached_exec_cfg = LiveExecutionConfig::from_env();
    let mut cached_fast_enabled = runtime_fast_loop_enabled();
    let mut cached_fast_margin = runtime_fast_margin_threshold();
    let mut cached_target_prewarm_ms = runtime_target_prewarm_ms();
    let mut cached_idle_force_poll_ms = runtime_event_idle_poll_ms();
    let mut cached_live_arm_required = live_submit_arm_required();
    let mut cached_live_armed = live_submit_armed();
    let mut cached_live_submit_allowed = live_submit_effective_armed();
    let mut cached_live_hard_kill = live_hard_kill_enabled();
    let mut env_refresh_ms = runtime_env_refresh_ms();
    let mut last_env_refresh_ms = Utc::now().timestamp_millis();
    loop {
        let now_for_wait = Utc::now().timestamp_millis();
        if now_for_wait.saturating_sub(last_env_refresh_ms) >= env_refresh_ms {
            cached_cfg = LiveRuntimeConfig::from_env();
            cached_push_cfg = ServerChanConfig::from_env();
            cached_exec_cfg = LiveExecutionConfig::from_env();
            cached_fast_enabled = runtime_fast_loop_enabled();
            cached_fast_margin = runtime_fast_margin_threshold();
            cached_target_prewarm_ms = runtime_target_prewarm_ms();
            cached_idle_force_poll_ms = runtime_event_idle_poll_ms();
            cached_live_arm_required = live_submit_arm_required();
            cached_live_armed = live_submit_armed();
            cached_live_submit_allowed = live_submit_effective_armed();
            cached_live_hard_kill = live_hard_kill_enabled();
            env_refresh_ms = runtime_env_refresh_ms();
            last_env_refresh_ms = now_for_wait;
        }
        let cfg = cached_cfg.clone();
        let push_cfg = cached_push_cfg.clone();
        let base_exec_cfg = cached_exec_cfg.clone();
        let fast_enabled = cached_fast_enabled;
        let fast_loop_ms = runtime_fast_loop_ms(cfg.loop_interval_ms);
        let fast_margin = cached_fast_margin;
        let target_prewarm_ms = cached_target_prewarm_ms;
        let idle_force_poll_ms = cached_idle_force_poll_ms;
        let scope_keys = runtime_scope_pairs(&cfg)
            .into_iter()
            .map(|(symbol, market)| runtime_scope_key(&symbol, &market))
            .collect::<HashSet<_>>();
        market_next_due_ms.retain(|k, _| scope_keys.contains(k));
        strategy_engines.retain(|k, _| scope_keys.contains(k));
        for (symbol, market) in runtime_scope_pairs(&cfg) {
            market_next_due_ms
                .entry(runtime_scope_key(&symbol, &market))
                .or_insert(now_for_wait);
        }
        let earliest_due_ms = market_next_due_ms
            .values()
            .copied()
            .min()
            .unwrap_or(now_for_wait);
        let wait_budget_ms = if cfg.enabled {
            earliest_due_ms.saturating_sub(now_for_wait).max(0) as u64
        } else {
            cfg.loop_interval_ms
        };
        let wait_ms = wait_budget_ms.clamp(1, cfg.loop_interval_ms);
        let first_wake_event = tokio::select! {
            maybe = wake_rx.recv() => maybe,
            _ = tokio::time::sleep(Duration::from_millis(wait_ms)) => None,
        };
        let mut wake_events = Vec::<LiveRuntimeWakeEvent>::new();
        if let Some(first) = first_wake_event {
            wake_events.push(first);
            while let Ok(ev) = wake_rx.try_recv() {
                wake_events.push(ev);
                if wake_events.len() >= 48 {
                    break;
                }
            }
        }
        let wake_scopes: HashSet<String> = wake_events
            .iter()
            .filter_map(|ev| {
                if cfg
                    .symbols
                    .iter()
                    .any(|symbol| ev.symbol.eq_ignore_ascii_case(symbol))
                    && cfg
                        .markets
                        .iter()
                        .any(|m| m.eq_ignore_ascii_case(&ev.market_type))
                {
                    Some(runtime_scope_key(&ev.symbol, &ev.market_type))
                } else {
                    None
                }
            })
            .collect();
        let wake_event_scope_enabled = !wake_scopes.is_empty();
        if !cfg.enabled {
            let next_due = Utc::now()
                .timestamp_millis()
                .saturating_add(cfg.loop_interval_ms as i64);
            for (symbol, market) in runtime_scope_pairs(&cfg) {
                market_next_due_ms.insert(runtime_scope_key(&symbol, &market), next_due);
            }
            continue;
        }

        for (runtime_symbol, market) in runtime_scope_pairs(&cfg) {
            let market_type = market.as_str();
            let runtime_symbol = runtime_symbol.as_str();
            let scope_key = runtime_scope_key(runtime_symbol, market_type);
            let trigger_for_market = wake_events.iter().rev().find(|ev| {
                ev.market_type.eq_ignore_ascii_case(market_type)
                    && ev.symbol.eq_ignore_ascii_case(runtime_symbol)
            });
            let now_ms = Utc::now().timestamp_millis();
            let is_wake_hit = wake_scopes.contains(&scope_key);
            let due_at_ms = market_next_due_ms
                .get(&scope_key)
                .copied()
                .unwrap_or(now_ms);
            let due_cycle = now_ms >= due_at_ms;
            if wake_event_scope_enabled && !is_wake_hit && !due_cycle {
                continue;
            }
            if !wake_event_scope_enabled && !due_cycle && trigger_for_market.is_none() {
                continue;
            }
            let mut market_sleep_ms = cfg.loop_interval_ms;
            let position_before_cycle = state
                .get_live_position_state(runtime_symbol, market_type)
                .await;
            let pending_before_cycle = state
                .list_pending_orders_for_market(runtime_symbol, market_type)
                .await;
            let pending_before_count = pending_before_cycle.len();
            let mut runtime_control = state
                .get_live_runtime_control(runtime_symbol, market_type)
                .await;
            let exec_aggr = state
                .get_live_execution_aggr_state(runtime_symbol, market_type)
                .await;
            let mut effective_live_execute = cfg.live_execute;
            let mut effective_drain_only = cfg.drain_only;
            let live_arm_required = cached_live_arm_required;
            let live_armed = cached_live_armed;
            let live_submit_allowed = cached_live_submit_allowed;
            let live_hard_kill = cached_live_hard_kill;
            match runtime_control.mode {
                LiveRuntimeControlMode::Normal => {}
                LiveRuntimeControlMode::ForcePause => {
                    effective_live_execute = false;
                    effective_drain_only = true;
                }
                LiveRuntimeControlMode::GracefulStop => {
                    effective_drain_only = true;
                    if position_before_cycle.side.is_none() && pending_before_count == 0 {
                        effective_live_execute = false;
                        if runtime_control.completed_at_ms.is_none() {
                            runtime_control.completed_at_ms = Some(now_ms);
                            runtime_control.updated_at_ms = now_ms;
                            state
                                .put_live_runtime_control(
                                    runtime_symbol,
                                    market_type,
                                    runtime_control.clone(),
                                )
                                .await;
                        }
                    } else {
                        // Keep live execution enabled in drain mode until existing position is closed.
                        effective_live_execute = true;
                    }
                }
            }
            if live_hard_kill {
                effective_live_execute = false;
                effective_drain_only = true;
            }
            // --- Three-layer kill switch check ---
            // Only blocks NEW entries; exits are always allowed (drain_only = true).
            if effective_live_execute {
                let ks_halted = state.ks_is_halted(runtime_symbol, market_type).await;
                if ks_halted {
                    let reason = state.ks_halt_reason(runtime_symbol, market_type).await;
                    effective_drain_only = true;
                    tracing::warn!(
                        symbol = runtime_symbol,
                        market_type = market_type,
                        reason = ?reason,
                        "live kill switch active: new entries blocked, exits still allowed"
                    );
                }
            }
            if effective_live_execute && !live_submit_allowed {
                effective_live_execute = false;
            }
            let latest_event_sample_ts = trigger_for_market
                .and_then(|ev| ev.sample_ts_ms)
                .filter(|v| *v > 0);
            let last_eval_sample_ts = market_last_strategy_sample_ts_ms
                .get(&scope_key)
                .copied()
                .unwrap_or(0);
            let duplicate_wake_sample = latest_event_sample_ts
                .map(|ts| ts <= last_eval_sample_ts)
                .unwrap_or(false);
            let last_eval_ms = market_last_strategy_eval_ms
                .get(&scope_key)
                .copied()
                .unwrap_or(0);
            let force_poll_due = now_ms.saturating_sub(last_eval_ms) >= idle_force_poll_ms;
            let must_run_without_new_sample = effective_drain_only
                || position_before_cycle.side.is_some()
                || pending_before_count > 0;
            let event_idle_skip = snapshot_event_available
                && !must_run_without_new_sample
                && !force_poll_due
                && ((!is_wake_hit) || duplicate_wake_sample);
            if event_idle_skip {
                let next_due = now_ms.saturating_add(idle_force_poll_ms);
                market_next_due_ms.insert(scope_key.clone(), next_due);
                continue;
            }
            let resolved_cfg = match strategy_resolve_effective_config(
                &state,
                runtime_symbol,
                market_type,
                None,
                true,
            )
            .await
            {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(error = %err.message, symbol = runtime_symbol, market_type = market_type, "strategy config resolve failed, fallback to baseline");
                    StrategyResolvedConfig {
                        cfg: strategy_current_default_config_for_scope(runtime_symbol, market_type),
                        baseline_profile: strategy_current_default_profile_name_for_scope(
                            runtime_symbol,
                            market_type,
                        ),
                        baseline_layer: "fallback",
                        config_source: strategy_current_default_profile_name_for_scope(
                            runtime_symbol,
                            market_type,
                        )
                        .to_string(),
                        config_layer: "fallback",
                        source_key: None,
                    }
                }
            };
            let runtime_cfg_source = resolved_cfg.baseline_profile;
            let runtime_config_resolution = strategy_config_resolution_json(&resolved_cfg);
            let strategy_cfg = resolved_cfg.cfg;
            let runtime_req = StrategyPaperLiveReq {
                state: &state,
                symbol: runtime_symbol,
                market_type,
                baseline_profile: runtime_cfg_source,
                full_history: false,
                lookback_minutes: cfg.lookback_minutes,
                max_points: cfg.max_points,
                max_trades: cfg.max_trades,
                cfg: &strategy_cfg,
                config_source: resolved_cfg.config_source.as_str(),
                config_resolution: runtime_config_resolution,
                live_execute: effective_live_execute,
                live_quote_usdc: cfg.quote_usdc,
                live_max_orders: cfg.max_orders,
                live_drain_only: effective_drain_only,
            };
            let payload_result = match load_live_runtime_samples(
                &state,
                runtime_symbol,
                market_type,
                cfg.lookback_minutes,
                cfg.max_points,
            )
            .await
            {
                Ok(samples) => {
                    let run = if samples.len() >= 20 {
                        let rebuild = match strategy_engines.get(&scope_key) {
                            Some(engine_state) => !engine_state.can_reuse(
                                runtime_symbol,
                                &strategy_cfg,
                                cfg.max_trades,
                                &samples,
                            ),
                            None => true,
                        };
                        if rebuild {
                            strategy_engines.insert(
                                scope_key.clone(),
                                LiveRuntimeStrategyEngineState::bootstrap(
                                    runtime_symbol,
                                    strategy_cfg,
                                    cfg.max_trades,
                                    samples.clone(),
                                ),
                            );
                        } else if let Some(engine_state) = strategy_engines.get_mut(&scope_key) {
                            engine_state.update(samples.clone());
                        }
                        strategy_engines.get(&scope_key).map(|engine_state| {
                            map_simulation_result(engine_state.engine.snapshot())
                        })
                    } else {
                        None
                    };
                    strategy_paper_live_from_samples(
                        runtime_req,
                        "runtime_bucket_1s_from_snapshot_100ms",
                        1000,
                        samples,
                        run,
                    )
                    .await
                }
                Err(err) => Err(err),
            };
            match payload_result {
                Ok(mut payload) => {
                    let now = Utc::now();
                    let now_ms = now.timestamp_millis();
                    market_last_strategy_eval_ms.insert(scope_key.clone(), now_ms);
                    let current_round = payload
                        .get("current")
                        .and_then(|v| v.get("round_id"))
                        .and_then(Value::as_str)
                        .map(|v| v.to_string());
                    let current_remaining_ms = payload
                        .get("current")
                        .and_then(|v| v.get("remaining_ms"))
                        .and_then(Value::as_i64);
                    if let Some(current_sample_ts) = payload
                        .get("current")
                        .and_then(|v| v.get("timestamp_ms"))
                        .and_then(Value::as_i64)
                        .filter(|v| *v > 0)
                    {
                        market_last_strategy_sample_ts_ms
                            .insert(scope_key.clone(), current_sample_ts);
                    }
                    let round_switched = if let Some(round_id) = current_round.clone() {
                        if let Some(prev) =
                            last_round_seen.insert(scope_key.clone(), round_id.clone())
                        {
                            prev != round_id
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if let Some(obj) = payload.as_object_mut() {
                        obj.insert(
                            "runtime_control".to_string(),
                            json!({
                                "mode": runtime_control.mode,
                                "requested_at_ms": runtime_control.requested_at_ms,
                                "updated_at_ms": runtime_control.updated_at_ms,
                                "completed_at_ms": runtime_control.completed_at_ms,
                                "note": runtime_control.note,
                                "effective_live_execute": effective_live_execute,
                                "effective_drain_only": effective_drain_only,
                                "position_side_before_cycle": position_before_cycle.side,
                                "pending_orders_before_cycle": pending_before_count,
                                "fast_loop_enabled": fast_enabled,
                                "fast_loop_ms": fast_loop_ms,
                                "base_loop_ms": cfg.loop_interval_ms,
                                "round_switched": round_switched,
                                "target_prewarm_ms": target_prewarm_ms,
                                "trigger_source": trigger_for_market.map(|v| v.source.clone()),
                                "trigger_symbol": trigger_for_market.map(|v| v.symbol.clone()),
                                "trigger_market": trigger_for_market.map(|v| v.market_type.clone()),
                                "trigger_ts_ms": trigger_for_market.map(|v| v.ts_ms),
                                "live_arm_required": live_arm_required,
                                "live_armed": live_armed,
                                "live_submit_allowed": live_submit_allowed,
                                "live_hard_kill": live_hard_kill,
                            }),
                        );
                        obj.insert("execution_aggressiveness".to_string(), json!(exec_aggr));
                    }

                    if fast_enabled {
                        let mut fast_path = effective_drain_only
                            || position_before_cycle.side.is_some()
                            || pending_before_count > 0;
                        if !fast_path {
                            let suggested_enter = payload
                                .get("current")
                                .and_then(|v| v.get("suggested_action"))
                                .and_then(Value::as_str)
                                .map(|s| s.starts_with("ENTER_"))
                                .unwrap_or(false);
                            if suggested_enter {
                                fast_path = true;
                            } else {
                                let score = payload
                                    .get("current")
                                    .and_then(|v| v.get("score"))
                                    .and_then(Value::as_f64)
                                    .map(f64::abs);
                                let threshold = payload
                                    .get("current")
                                    .and_then(|v| v.get("entry_threshold"))
                                    .and_then(Value::as_f64)
                                    .map(f64::abs);
                                if let (Some(s), Some(t)) = (score, threshold) {
                                    let margin = s - t;
                                    if margin >= -fast_margin {
                                        fast_path = true;
                                    }
                                }
                            }
                        }
                        if fast_path {
                            market_sleep_ms = market_sleep_ms.min(fast_loop_ms);
                        }
                        if round_switched {
                            // Event-driven assist: immediately tighten next cycle on round rollover.
                            market_sleep_ms = market_sleep_ms.min(fast_loop_ms);
                        }
                    }

                    if current_remaining_ms
                        .map(|v| v >= 0 && v <= target_prewarm_ms)
                        .unwrap_or(false)
                    {
                        // Keep round-switch prewarm off the hot path. Background maintenance handles
                        // the actual target/book refresh without stalling submit latency.
                        let prewarm_state = state.clone();
                        let prewarm_symbol = runtime_symbol.to_string();
                        let prewarm_market = market_type.to_string();
                        tokio::spawn(async move {
                            prewarm_live_runtime_target_and_books(
                                &prewarm_state,
                                &prewarm_symbol,
                                &prewarm_market,
                            )
                            .await;
                        });
                        market_sleep_ms = market_sleep_ms.min(fast_loop_ms);
                    }

                    if wake_event_scope_enabled {
                        market_sleep_ms = market_sleep_ms.min(fast_loop_ms);
                    }

                    let summary = payload.get("summary").cloned().unwrap_or(Value::Null);
                    let signal_decisions = payload
                        .get("signal_decisions")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    let paper_decisions = normalize_runtime_signal_decisions(
                        market_type,
                        signal_decisions,
                        cfg.quote_usdc,
                    );
                    let latest_ts_ms = payload
                        .get("current")
                        .and_then(|v| v.get("timestamp_ms"))
                        .and_then(Value::as_i64)
                        .unwrap_or(now_ms);
                    let current_suggested_action = payload
                        .get("current")
                        .and_then(|v| v.get("suggested_action"))
                        .and_then(Value::as_str)
                        .unwrap_or("UNKNOWN")
                        .to_string();
                    let current_confirmed = payload
                        .get("current")
                        .and_then(|v| v.get("confirmed"))
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    let current_score = payload
                        .get("current")
                        .and_then(|v| v.get("score"))
                        .and_then(Value::as_f64);
                    let current_entry_threshold = payload
                        .get("current")
                        .and_then(|v| v.get("entry_threshold"))
                        .and_then(Value::as_f64);
                    let prefer_action =
                        if effective_drain_only || position_before_cycle.side.is_some() {
                            Some("exit")
                        } else {
                            None
                        };
                    let active_signal =
                        normalize_current_active_signal(&payload, market_type, cfg.quote_usdc);
                    if let Some(signal) = active_signal.as_ref() {
                        state
                            .remember_recent_active_intent(runtime_symbol, market_type, signal)
                            .await;
                    }
                    let recent_intents = state
                        .recent_active_intents(runtime_symbol, market_type, latest_ts_ms)
                        .await;
                    let (
                        mut selected_decisions,
                        fresh_signal_count,
                        candidate_source,
                        current_entry_available,
                    ) = select_live_execution_candidates(
                        active_signal,
                        recent_intents.clone(),
                        latest_ts_ms,
                        cfg.max_orders.max(1),
                        effective_drain_only,
                        prefer_action,
                    );
                    let trigger_ts_ms = trigger_for_market.map(|v| v.ts_ms);
                    if let Some(trigger_ts_ms) = trigger_ts_ms {
                        for decision in &mut selected_decisions {
                            if let Some(obj) = decision.as_object_mut() {
                                obj.insert("trigger_ts_ms".to_string(), json!(trigger_ts_ms));
                            }
                        }
                    }
                    let (exec_cfg_tuned, _) = state
                        .apply_aggressiveness_to_execution_cfg(
                            runtime_symbol,
                            market_type,
                            &base_exec_cfg,
                        )
                        .await;

                    if effective_live_execute || pending_before_count > 0 {
                        reconcile_live_reports(&state, &exec_cfg_tuned).await;
                        handle_live_pending_timeouts(&state, &exec_cfg_tuned).await;
                    }

                    let ledger_signal_count = paper_decisions.len();
                    let candidate_count = selected_decisions.len();

                    // OPTIMIZATION: Aggressive book prewarming when candidates exist
                    // This ensures books are fresh for fast execution
                    if candidate_count > 0 {
                        if let Ok(target) = resolve_live_market_target_fast_with_state(
                            &state,
                            runtime_symbol,
                            market_type,
                        )
                        .await
                        {
                            let _ = prewarm_rust_books_for_target(&state, &target).await;
                        }
                    }

                    let (gated, mut state_skipped, position_for_submit) = gate_live_decisions(
                        &state,
                        runtime_symbol,
                        market_type,
                        &selected_decisions,
                        effective_live_execute,
                    )
                    .await;
                    let state_skipped_count = state_skipped.len();
                    let skipped_decisions = std::mem::take(&mut state_skipped);
                    let submitted_decisions: Vec<Value> =
                        gated.iter().map(|g| g.decision.clone()).collect();
                    let mut execution_orders = Vec::<Value>::new();
                    let execution_mode = if effective_live_execute {
                        "live"
                    } else {
                        "dry_run"
                    }
                    .to_string();
                    let execution_status: String;
                    let mut execution_target = Value::Null;
                    let resolved_target = if gated.is_empty() {
                        None
                    } else if let Some(locked_target) =
                        build_position_locked_target(market_type, &position_for_submit)
                    {
                        Some(locked_target)
                    } else {
                        resolve_live_market_target_fast_with_state(
                            &state,
                            runtime_symbol,
                            market_type,
                        )
                        .await
                        .ok()
                    };
                    if let Some(target) = resolved_target {
                        execution_target = json!({
                            "market_id": target.market_id.clone(),
                            "symbol": target.symbol.clone(),
                            "timeframe": target.timeframe.clone(),
                            "token_id_yes": target.token_id_yes.clone(),
                            "token_id_no": target.token_id_no.clone(),
                            "end_date": target.end_date.clone()
                        });
                        if effective_live_execute {
                            execution_orders = execute_live_orders(
                                &state,
                                &exec_cfg_tuned,
                                runtime_symbol,
                                market_type,
                                &target,
                                &position_for_submit,
                                &gated,
                            )
                            .await;
                            let accepted_count = execution_orders
                                .iter()
                                .filter(|row| {
                                    row.get("accepted")
                                        .and_then(Value::as_bool)
                                        .unwrap_or(false)
                                })
                                .count();
                            execution_status = if accepted_count > 0
                                || execution_orders
                                    .iter()
                                    .any(|row| row.get("decision").is_some())
                            {
                                "submitted".to_string()
                            } else {
                                "all_rejected".to_string()
                            };
                        } else if candidate_count == 0 {
                            execution_status = "dry_run_no_candidate".to_string();
                        } else if submitted_decisions.is_empty() {
                            execution_status = "dry_run_state_blocked".to_string();
                        } else {
                            execution_status = "dry_run_ready".to_string();
                        }
                    } else if effective_live_execute {
                        execution_status = if gated.is_empty() {
                            "all_rejected".to_string()
                        } else {
                            "target_missing".to_string()
                        };
                    } else {
                        execution_status = if candidate_count == 0 {
                            "dry_run_no_candidate".to_string()
                        } else if submitted_decisions.is_empty() {
                            "dry_run_state_blocked".to_string()
                        } else {
                            "target_missing".to_string()
                        };
                    }
                    let shadow_target_ready = !execution_target.is_null();
                    let shadow_target_missing =
                        !submitted_decisions.is_empty() && !shadow_target_ready;
                    let no_candidate_reason = if candidate_count > 0 {
                        Value::Null
                    } else if fresh_signal_count == 0 {
                        let reason = if current_suggested_action.eq_ignore_ascii_case("hold")
                            || current_suggested_action.eq_ignore_ascii_case("wait")
                        {
                            match (current_confirmed, current_score, current_entry_threshold) {
                                (false, _, _) => "signal_not_confirmed",
                                (true, Some(score), Some(threshold)) if score.abs() < threshold => {
                                    "score_below_threshold"
                                }
                                _ => "current_hold",
                            }
                        } else {
                            "no_fresh_signal"
                        };
                        Value::String(reason.to_string())
                    } else {
                        Value::String("selection_empty".to_string())
                    };

                    let aggr_orders = if effective_live_execute {
                        execution_orders
                            .iter()
                            .filter(|row| row.get("decision").is_some())
                            .cloned()
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    };
                    let exec_aggr = state
                        .update_live_execution_aggr_from_orders(
                            runtime_symbol,
                            market_type,
                            &aggr_orders,
                        )
                        .await;
                    let state_machine = state
                        .get_live_position_state(runtime_symbol, market_type)
                        .await;
                    let live_events_all = state
                        .list_live_events(runtime_symbol, market_type, live_event_log_max())
                        .await;
                    let pending_rows = state
                        .list_pending_orders_for_market(runtime_symbol, market_type)
                        .await;
                    let (live_fill_by_decision, live_fill_summary) =
                        build_live_fill_decision_map(&live_events_all);
                    let paper_records = paper_decisions.clone();
                    let (paper_records_enriched, paper_live_fill_count) =
                        enrich_paper_records_with_live_fills(
                            &paper_records,
                            &live_fill_by_decision,
                        );
                    let live_order_lineage =
                        build_live_order_lineage(&live_events_all, &pending_rows);
                    let live_completed_records = build_live_completed_records(
                        &paper_records_enriched,
                        &live_fill_by_decision,
                        &live_order_lineage,
                    );
                    // Feed completed trade outcomes to kill-switch (Layer 1 + Layer 3)
                    // We track newly completed trades each cycle to avoid double-counting.
                    for record in &live_completed_records {
                        // Only process completed (exit-filled) records
                        let has_exit_fill = record
                            .get("live_fill")
                            .and_then(|f| f.get("action"))
                            .and_then(Value::as_str)
                            .map(|a| {
                                a.eq_ignore_ascii_case("exit") || a.eq_ignore_ascii_case("reduce")
                            })
                            .unwrap_or(false);
                        if !has_exit_fill {
                            continue;
                        }
                        // Use live fill net_pnl if available, fall back to paper net_pnl
                        let net_pnl = record
                            .get("live_fill")
                            .and_then(|f| f.get("net_pnl_cents"))
                            .and_then(Value::as_f64)
                            .or_else(|| record.get("net_pnl_cents").and_then(Value::as_f64))
                            .unwrap_or(0.0);
                        state
                            .ks_record_trade_outcome(runtime_symbol, market_type, net_pnl)
                            .await;
                    }
                    let live_realized_net_pnl_cents = state_machine.realized_pnl_usdc * 100.0;
                    let live_events = live_events_all
                        .iter()
                        .rev()
                        .take(60)
                        .cloned()
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .collect::<Vec<_>>();

                    let live_submitted_count = execution_orders
                        .iter()
                        .filter(|row| row.get("decision").is_some())
                        .count();
                    let live_accepted_count = execution_orders
                        .iter()
                        .filter(|row| {
                            row.get("accepted")
                                .and_then(Value::as_bool)
                                .unwrap_or(false)
                        })
                        .count();
                    let live_rejected_count =
                        live_submitted_count.saturating_sub(live_accepted_count);
                    let live_latency = summarize_live_order_latency(&aggr_orders);
                    let live_price_parity = summarize_live_price_parity(&aggr_orders);
                    let live_execution_payload = json!({
                        "summary": {
                            "decision_count": selected_decisions.len(),
                            "mode": if effective_live_execute { execution_mode.clone() } else { "paper_only".to_string() },
                            "latency": live_latency,
                            "price_parity": live_price_parity,
                            "realized_net_pnl_cents": live_realized_net_pnl_cents,
                            "paper_live_fill_count": paper_live_fill_count,
                            "fills": live_fill_summary,
                            "shadow_eval": {
                                "enabled": !effective_live_execute,
                                "status": execution_status.clone(),
                                "trigger_ts_ms": trigger_for_market.map(|v| v.ts_ms),
                                "ledger_signal_count": ledger_signal_count,
                                "decision_pool_count": ledger_signal_count,
                                "raw_signal_count": ledger_signal_count,
                                "fresh_signal_count": fresh_signal_count,
                                "candidate_count": candidate_count,
                                "candidate_source": candidate_source,
                                "current_entry_available": current_entry_available,
                                "state_selected_count": submitted_decisions.len(),
                                "state_skipped_count": state_skipped_count,
                                "target_ready": shadow_target_ready,
                                "target_missing": shadow_target_missing,
                                "no_candidate_reason": no_candidate_reason.clone(),
                                "current_suggested_action": current_suggested_action,
                                "current_confirmed": current_confirmed,
                                "current_score": current_score,
                                "current_entry_threshold": current_entry_threshold
                            },
                        },
                        "decisions": selected_decisions,
                        "paper_records": paper_records_enriched,
                        "live_records": live_completed_records,
                        "order_lineage": live_order_lineage,
                        "parity_check": {
                            "status": execution_status.clone(),
                            "level": if effective_live_execute { "warn" } else { "ok" },
                            "paper": {
                                "decision_count": submitted_decisions.len(),
                                "entry_count": decision_action_count(&submitted_decisions, "enter"),
                                "add_count": decision_action_count(&submitted_decisions, "add"),
                                "reduce_count": decision_action_count(&submitted_decisions, "reduce"),
                                "exit_count": decision_action_count(&submitted_decisions, "exit")
                            },
                            "live": {
                                "submitted_count": live_submitted_count,
                                "submitted_entry_count": decision_action_count_from_orders(&aggr_orders, "enter"),
                                "submitted_add_count": decision_action_count_from_orders(&aggr_orders, "add"),
                                "submitted_reduce_count": decision_action_count_from_orders(&aggr_orders, "reduce"),
                                "submitted_exit_count": decision_action_count_from_orders(&aggr_orders, "exit"),
                                "accepted_count": live_accepted_count,
                                "rejected_count": live_rejected_count,
                                "skipped_count": skipped_decisions.len(),
                                "no_live_market_target": shadow_target_missing
                            }
                        },
                        "execution_policy": live_execution_policy_meta(),
                            "gated": {
                                "ledger_signal_count": ledger_signal_count,
                                "decision_pool_count": ledger_signal_count,
                                "raw_signal_count": ledger_signal_count,
                                "fresh_signal_count": fresh_signal_count,
                                "recent_intent_buffer_count": recent_intents.len(),
                                "candidate_count": candidate_count,
                                "selected_count": submitted_decisions.len(),
                            "submitted_count": live_submitted_count,
                            "state_skipped_count": state_skipped_count,
                            "skipped_count": skipped_decisions.len(),
                            "target_ready": shadow_target_ready,
                            "target_missing": shadow_target_missing,
                            "no_candidate_reason": no_candidate_reason,
                            "submitted_decisions": submitted_decisions,
                            "skipped_decisions": skipped_decisions
                        },
                        "execution_target": execution_target,
                        "execution": {
                            "mode": execution_mode,
                            "orders": aggr_orders
                        },
                        "kill_switch": state.ks_snapshot(runtime_symbol, market_type).await,
                        "state_machine": state_machine,
                        "events": live_events
                    });

                    let status = if effective_live_execute {
                        match execution_status.as_str() {
                            "submitted" => "ok",
                            "target_missing" => "target_missing",
                            "all_rejected" => "all_rejected",
                            _ => "blocked_by_gate_or_state",
                        }
                    } else {
                        payload
                            .get("status")
                            .and_then(Value::as_str)
                            .unwrap_or("ok")
                    }
                    .to_string();
                    if let Some(obj) = payload.as_object_mut() {
                        obj.insert("execution_aggressiveness".to_string(), json!(exec_aggr));
                        obj.insert("live_execution".to_string(), live_execution_payload);
                        obj.insert("status".to_string(), Value::String(status.clone()));
                    }
                    state
                        .set_runtime_snapshot(runtime_symbol, market_type, payload)
                        .await;
                    // Persist runtime state off the hot path to avoid Redis latency coupling.
                    state
                        .persist_live_runtime_state_async(runtime_symbol, market_type)
                        .await;

                    if push_cfg.enabled && effective_live_execute && status != "ok" {
                        let alert_key = format!("live-status:{market_type}:{status}");
                        if state
                            .should_emit_alert(&alert_key, now_ms, push_cfg.throttle_ms)
                            .await
                        {
                            let trade_count = summary
                                .get("trade_count")
                                .and_then(Value::as_i64)
                                .unwrap_or(0);
                            let win_rate = summary
                                .get("win_rate_pct")
                                .and_then(Value::as_f64)
                                .unwrap_or(0.0);
                            let net = summary
                                .get("net_pnl_cents")
                                .and_then(Value::as_f64)
                                .or_else(|| summary.get("total_pnl_cents").and_then(Value::as_f64))
                                .unwrap_or(0.0);
                            let md = format!(
                                "### FEV1 Live状态异常\n\n- 市场: `{}`\n- 状态: `{}`\n- 时间(UTC): `{}`\n- 胜率: `{:.2}%`\n- 净收益: `{:.2}¢`\n- 交易数: `{}`\n",
                                market_type,
                                status,
                                now.to_rfc3339(),
                                win_rate,
                                net,
                                trade_count
                            );
                            if let Err(err) = state
                                .emit_serverchan_markdown(
                                    &push_cfg,
                                    &format!("FEV1异常/{market_type}/{status}"),
                                    &md,
                                )
                                .await
                            {
                                tracing::warn!(?err, "serverchan status alert failed");
                            }
                        }
                    }

                    if push_cfg.enabled && now.hour() >= push_cfg.daily_report_hour_utc {
                        let day_key =
                            format!("{:04}-{:02}-{:02}", now.year(), now.month(), now.day());
                        if state.should_emit_daily_report(market_type, &day_key).await {
                            let trade_count = summary
                                .get("trade_count")
                                .and_then(Value::as_i64)
                                .unwrap_or(0);
                            let win_rate = summary
                                .get("win_rate_pct")
                                .and_then(Value::as_f64)
                                .unwrap_or(0.0);
                            let net = summary
                                .get("net_pnl_cents")
                                .and_then(Value::as_f64)
                                .or_else(|| summary.get("total_pnl_cents").and_then(Value::as_f64))
                                .unwrap_or(0.0);
                            let max_dd = summary
                                .get("max_drawdown_cents")
                                .and_then(Value::as_f64)
                                .unwrap_or(0.0);
                            let md = format!(
                                "### FEV1 每日报告\n\n- 日期(UTC): `{}`\n- 市场: `{}`\n- 模式: `{}`\n- 胜率: `{:.2}%`\n- 净收益: `{:.2}¢`\n- 最大回撤: `{:.2}¢`\n- 交易数: `{}`\n",
                                day_key,
                                market_type,
                                if effective_live_execute {
                                    "live_execute"
                                } else {
                                    "paper_replay"
                                },
                                win_rate,
                                net,
                                max_dd,
                                trade_count
                            );
                            if let Err(err) = state
                                .emit_serverchan_markdown(
                                    &push_cfg,
                                    &format!("FEV1日报/{market_type}/{day_key}"),
                                    &md,
                                )
                                .await
                            {
                                tracing::warn!(?err, "serverchan daily report failed");
                            }
                        }
                    }
                }
                Err(err) => {
                    let now = Utc::now();
                    let now_ms = now.timestamp_millis();
                    market_last_strategy_eval_ms.insert(scope_key.clone(), now_ms);
                    let msg = format!("runtime_cycle_error:{}", err.message);
                    state
                        .append_live_event(
                            runtime_symbol,
                            market_type,
                            json!({
                                "accepted": false,
                                "action": "runtime",
                                "side": "NONE",
                                "reason": msg,
                                "ts_ms": now_ms
                            }),
                        )
                        .await;
                    if push_cfg.enabled {
                        let alert_key = format!("runtime-error:{market_type}");
                        if state
                            .should_emit_alert(&alert_key, now_ms, push_cfg.throttle_ms)
                            .await
                        {
                            let md = format!(
                                "### FEV1 Runtime错误\n\n- 市场: `{}`\n- 时间(UTC): `{}`\n- 错误: `{}`\n",
                                market_type,
                                now.to_rfc3339(),
                                err.message
                            );
                            if let Err(send_err) = state
                                .emit_serverchan_markdown(
                                    &push_cfg,
                                    &format!("FEV1错误/{market_type}"),
                                    &md,
                                )
                                .await
                            {
                                tracing::warn!(?send_err, "serverchan runtime error alert failed");
                            }
                        }
                    }
                }
            }
            let pending_after_count = if effective_live_execute || pending_before_count > 0 {
                state
                    .list_pending_orders_for_market(runtime_symbol, market_type)
                    .await
                    .len()
            } else {
                0
            };
            if pending_after_count > 0 {
                market_sleep_ms = market_sleep_ms.min(fast_loop_ms.min(80));
            }
            let next_due = Utc::now()
                .timestamp_millis()
                .saturating_add(market_sleep_ms as i64);
            market_next_due_ms.insert(scope_key, next_due);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn live_snapshot_rejects_old_timestamp() {
        let snap = json!({
            "ts_ireland_sample_ms": 1_000_000_i64,
            "remaining_ms": 120_000_i64,
            "round_id": "BTCUSDT_5m_1000000",
        });
        assert!(!is_live_snapshot_fresh(
            &snap,
            "5m",
            1_000_000 + LIVE_SNAPSHOT_MAX_AGE_MS + 1
        ));
    }

    #[test]
    fn live_snapshot_rejects_ended_round_after_grace() {
        let start = 1_000_000_i64;
        let end = start + market_type_to_ms("5m");
        let snap = json!({
            "ts_ireland_sample_ms": end,
            "remaining_ms": 0_i64,
            "round_id": format!("BTCUSDT_5m_{}", start),
        });
        assert!(!is_live_snapshot_fresh(
            &snap,
            "5m",
            end + LIVE_ROUND_END_GRACE_MS + 1
        ));
    }

    #[test]
    fn live_snapshot_accepts_fresh_active_round() {
        let start = 1_000_000_i64;
        let snap = json!({
            "ts_ireland_sample_ms": start + 100_000,
            "remaining_ms": 200_000_i64,
            "round_id": format!("BTCUSDT_5m_{}", start),
        });
        assert!(is_live_snapshot_fresh(&snap, "5m", start + 100_500));
    }

    #[test]
    fn parse_snapshot_event_extracts_timeframe_and_sample_ts() {
        let payload = json!({
            "event": "snapshot_updated",
            "symbol": "BTCUSDT",
            "timeframe": "5m",
            "ts_ireland_sample_ms": 123_456_i64
        });
        let parsed = parse_snapshot_event(&payload).expect("parse snapshot event");
        assert_eq!(parsed.0, "BTCUSDT");
        assert_eq!(parsed.1, "5m");
        assert_eq!(parsed.2, Some(123_456_i64));
    }

    #[test]
    fn parse_snapshot_event_rejects_unsupported_timeframe() {
        let payload = json!({
            "event": "snapshot_updated",
            "symbol": "BTCUSDT",
            "timeframe": "30m",
            "ts_ireland_sample_ms": 123_456_i64
        });
        assert!(parse_snapshot_event(&payload).is_none());
    }

    #[test]
    fn strategy_sample_from_snapshot_event_builds_runtime_sample() {
        let payload = json!({
            "symbol": "BTCUSDT",
            "timeframe": "5m",
            "round_id": "BTCUSDT_5m_123000",
            "ts_ireland_sample_ms": 123_456_i64,
            "remaining_ms": 40_000_i64,
            "mid_yes_smooth": 0.62,
            "mid_yes": 0.61,
            "mid_no_smooth": 0.38,
            "bid_yes": 0.615,
            "ask_yes": 0.625,
            "bid_no": 0.375,
            "ask_no": 0.385,
            "delta_pct_smooth": 0.12,
            "velocity_bps_per_sec": 8.0,
            "acceleration": 0.6
        });
        let parsed = strategy_sample_from_snapshot_event(&payload).expect("runtime sample");
        assert_eq!(parsed.0, "BTCUSDT");
        assert_eq!(parsed.1, "5m");
        assert_eq!(parsed.2, 123_456_i64);
    }

    #[test]
    fn build_decision_samples_1s_suppresses_single_spike_inside_second() {
        let raw = vec![
            StrategySample {
                ts_ms: 1_100,
                round_id: "BTCUSDT_5m_r1".to_string(),
                remaining_ms: 150_000,
                p_up: 0.60,
                delta_pct: 0.2,
                velocity: 1.0,
                acceleration: 0.1,
                bid_yes: 0.59,
                ask_yes: 0.61,
                bid_no: 0.39,
                ask_no: 0.41,
                spread_up: 0.02,
                spread_down: 0.02,
                spread_mid: 0.02,
            },
            StrategySample {
                ts_ms: 1_500,
                round_id: "BTCUSDT_5m_r1".to_string(),
                remaining_ms: 149_600,
                p_up: 0.60,
                delta_pct: 0.2,
                velocity: 1.0,
                acceleration: 0.1,
                bid_yes: 0.01,
                ask_yes: 0.99,
                bid_no: 0.01,
                ask_no: 0.99,
                spread_up: 0.04,
                spread_down: 0.04,
                spread_mid: 0.04,
            },
            StrategySample {
                ts_ms: 1_900,
                round_id: "BTCUSDT_5m_r1".to_string(),
                remaining_ms: 149_200,
                p_up: 0.61,
                delta_pct: 0.22,
                velocity: 1.1,
                acceleration: 0.1,
                bid_yes: 0.60,
                ask_yes: 0.62,
                bid_no: 0.38,
                ask_no: 0.40,
                spread_up: 0.02,
                spread_down: 0.02,
                spread_mid: 0.02,
            },
            StrategySample {
                ts_ms: 2_100,
                round_id: "BTCUSDT_5m_r1".to_string(),
                remaining_ms: 148_900,
                p_up: 0.61,
                delta_pct: 0.23,
                velocity: 1.2,
                acceleration: 0.2,
                bid_yes: 0.60,
                ask_yes: 0.62,
                bid_no: 0.38,
                ask_no: 0.40,
                spread_up: 0.02,
                spread_down: 0.02,
                spread_mid: 0.02,
            },
        ];
        let decision = build_decision_samples_1s(&raw);
        assert_eq!(decision.len(), 1);
        let sample = &decision[0];
        assert!(sample.bid_yes > 0.55, "bid_yes={}", sample.bid_yes);
        assert!(sample.ask_yes < 0.65, "ask_yes={}", sample.ask_yes);
        assert!(sample.spread_up <= 0.03, "spread_up={}", sample.spread_up);
    }

    #[test]
    fn decision_payload_uses_decision_execution_fields() {
        let decision = json!({
            "action": "enter",
            "side": "UP",
            "price_cents": 63.2,
            "quote_size_usdc": 1.0,
            "tif": "GTD",
            "style": "maker",
            "ttl_ms": 2300,
            "max_slippage_bps": 17.0
        });
        let target = LiveMarketTarget {
            market_id: "mkt1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "5m".to_string(),
            token_id_yes: "yes_token".to_string(),
            token_id_no: "no_token".to_string(),
            end_date: None,
        };
        let cfg = LiveExecutionConfig {
            min_quote_usdc: 1.0,
            entry_slippage_bps: 18.0,
            exit_slippage_bps: 22.0,
            force_slippage_bps: None,
        };
        let payload =
            decision_to_live_payload(&decision, &target, &cfg, None, None).expect("payload");
        assert_eq!(payload.get("tif").and_then(Value::as_str), Some("FAK"));
        assert_eq!(payload.get("style").and_then(Value::as_str), Some("taker"));
        assert_eq!(payload.get("ttl_ms").and_then(Value::as_i64), Some(2300));
        assert_eq!(payload.get("action").and_then(Value::as_str), Some("enter"));
        assert_eq!(
            payload.get("signal_side").and_then(Value::as_str),
            Some("UP")
        );
        assert_eq!(
            payload.get("token_id").and_then(Value::as_str),
            Some("yes_token")
        );
    }

    #[test]
    fn retry_payload_escalates_fak_ladder_for_entry() {
        let payload = json!({
            "action": "enter",
            "side": "buy_yes",
            "price": 0.50,
            "tif": "FAK",
            "style": "taker",
            "ttl_ms": 1200,
            "max_slippage_bps": 18.0,
            "book_meta": { "tick_size": 0.01 }
        });
        let reason = "no orders found to match with FAK order";
        let first = build_retry_payload(&payload, reason, 0).expect("first retry");
        assert_eq!(
            first.get("retry_tag").and_then(Value::as_str),
            Some("entry_fak_ladder_step_1")
        );
        assert_eq!(first.get("tif").and_then(Value::as_str), Some("FAK"));
        assert_eq!(first.get("style").and_then(Value::as_str), Some("taker"));
        let second = build_retry_payload(&first, reason, 1).expect("second retry");
        assert_eq!(second.get("tif").and_then(Value::as_str), Some("FAK"));
        assert_eq!(second.get("style").and_then(Value::as_str), Some("taker"));
        assert_eq!(
            second.get("retry_tag").and_then(Value::as_str),
            Some("entry_fak_ladder_step_2")
        );
    }

    #[test]
    fn select_live_execution_candidates_uses_active_signal_only() {
        let active_signal = Some(json!({
            "action": "enter",
            "side": "DOWN",
            "round_id": "r-1",
            "ts_ms": 250_000_i64,
            "expires_at_ms": 250_300_i64,
            "reason": "fev1_signal_entry",
            "intent_id": "intent-1",
            "quote_size_usdc": 5.0,
            "price_cents": 51.2
        }));
        let (selected, fresh_count, candidate_source, current_entry_available) =
            select_live_execution_candidates(
                active_signal,
                Vec::new(),
                250_000_i64,
                1,
                false,
                None,
            );
        assert_eq!(selected.len(), 1);
        assert_eq!(candidate_source, "active_signal");
        assert_eq!(fresh_count, 1);
        assert!(current_entry_available);
    }

    #[test]
    fn select_live_execution_candidates_returns_empty_when_active_signal_missing() {
        let (selected, fresh_count, candidate_source, current_entry_available) =
            select_live_execution_candidates(None, Vec::new(), 250_000_i64, 1, false, None);
        assert_eq!(selected.len(), 0);
        assert_eq!(candidate_source, "none");
        assert_eq!(fresh_count, 0);
        assert!(!current_entry_available);
    }

    #[test]
    fn select_live_execution_candidates_rejects_expired_active_signal() {
        let active_signal = Some(json!({
            "action": "enter",
            "side": "UP",
            "round_id": "r-1",
            "ts_ms": 100_i64,
            "expires_at_ms": 150_i64,
            "intent_id": "intent-expired"
        }));
        let (selected, fresh_count, candidate_source, current_entry_available) =
            select_live_execution_candidates(active_signal, Vec::new(), 250_i64, 1, false, None);
        assert!(selected.is_empty());
        assert_eq!(fresh_count, 0);
        assert_eq!(candidate_source, "active_signal_expired");
        assert!(!current_entry_available);
    }

    #[test]
    fn select_live_execution_candidates_falls_back_to_recent_intent_buffer() {
        let buffered = vec![json!({
            "action": "enter",
            "side": "UP",
            "round_id": "r-1",
            "ts_ms": 10_000_i64,
            "expires_at_ms": 14_000_i64,
            "intent_id": "intent-buffered",
            "quote_size_usdc": 5.0,
            "price_cents": 48.5
        })];
        let (selected, fresh_count, candidate_source, current_entry_available) =
            select_live_execution_candidates(None, buffered, 12_000_i64, 1, false, None);
        assert_eq!(selected.len(), 1);
        assert_eq!(fresh_count, 1);
        assert_eq!(candidate_source, "recent_intent_buffer");
        assert!(!current_entry_available);
    }

    #[test]
    fn normalize_runtime_signal_decisions_backfills_quote_and_decision_id() {
        let decisions = vec![json!({
            "action": "enter",
            "side": "UP",
            "round_id": "r-1",
            "ts_ms": 12_345_i64
        })];
        let normalized = normalize_runtime_signal_decisions("5m", decisions, 2.5);
        assert_eq!(normalized.len(), 1);
        assert_eq!(
            normalized[0].get("quote_size_usdc").and_then(Value::as_f64),
            Some(2.5)
        );
        assert_eq!(
            normalized[0].get("decision_id").and_then(Value::as_str),
            Some("fev1:5m:enter:UP:r-1:12345")
        );
    }

    #[test]
    fn normalize_runtime_signal_decisions_preserves_existing_decision_id() {
        let decisions = vec![json!({
            "decision_id": "custom-id",
            "action": "exit",
            "side": "DOWN",
            "round_id": "r-2",
            "ts_ms": 9_999_i64,
            "quote_size_usdc": 1.2
        })];
        let normalized = normalize_runtime_signal_decisions("15m", decisions, 3.0);
        assert_eq!(normalized.len(), 1);
        assert_eq!(
            normalized[0].get("decision_id").and_then(Value::as_str),
            Some("custom-id")
        );
        assert_eq!(
            normalized[0].get("quote_size_usdc").and_then(Value::as_f64),
            Some(1.2)
        );
    }

    #[test]
    fn merge_current_summary_paper_records_appends_virtual_record() {
        let paper_records = vec![json!({
            "decision_id": "paper-1",
            "action": "enter",
            "side": "UP",
            "round_id": "r-1",
            "ts_ms": 100_i64
        })];
        let selected_decisions = vec![
            json!({
                "decision_id": "paper-1",
                "action": "enter",
                "side": "UP",
                "signal_source": "signal_decision",
                "round_id": "r-1",
                "ts_ms": 100_i64
            }),
            json!({
                "decision_id": "summary-1",
                "action": "enter",
                "side": "DOWN",
                "signal_source": "current_summary",
                "round_id": "r-2",
                "ts_ms": 200_i64
            }),
        ];

        let merged = merge_current_summary_paper_records(&paper_records, &selected_decisions);
        assert_eq!(merged.len(), 2);
        assert_eq!(
            merged[1].get("decision_id").and_then(Value::as_str),
            Some("summary-1")
        );
        assert_eq!(
            merged[1].get("paper_record_source").and_then(Value::as_str),
            Some("current_summary")
        );
        assert_eq!(
            merged[1]
                .get("paper_record_virtual")
                .and_then(Value::as_bool),
            Some(true)
        );
    }

    #[test]
    fn build_paper_ledger_payload_merges_trades_and_decision_rows() {
        let existing = json!({
            "trades": [
                {
                    "id": 1,
                    "side": "UP",
                    "entry_round_id": "SOLUSDT_5m_round_1",
                    "entry_ts_ms": 10_000_i64,
                    "exit_ts_ms": 20_000_i64,
                    "entry_price_raw_cents": 48.0,
                    "entry_price_cents": 48.7,
                    "pnl_net_cents": 5.0,
                    "pnl_gross_cents": 6.0,
                    "total_cost_cents": 1.0,
                    "entry_fee_cents": 0.2,
                    "exit_fee_cents": 0.3,
                    "entry_slippage_cents": 0.1,
                    "exit_slippage_cents": 0.1,
                    "entry_impact_cents": 0.0,
                    "exit_impact_cents": 0.0,
                    "duration_s": 10.0
                }
            ],
            "paper_records": [
                {
                    "decision_id": "decision-1",
                    "action": "enter",
                    "side": "UP",
                    "ts_ms": 100_i64
                }
            ],
            "live_records": [
                {
                    "decision_id": "decision-1",
                    "ts_ms": 200_i64
                }
            ]
        });
        let runtime_payload = json!({
            "symbol": "SOLUSDT",
            "market_type": "5m",
            "lookback_minutes": 1440,
            "samples": 321,
            "current": {
                "timestamp_ms": 123_456_i64,
                "round_id": "SOLUSDT_5m_123000"
            },
            "trades": [
                {
                    "id": 1,
                    "side": "UP",
                    "entry_round_id": "SOLUSDT_5m_round_1",
                    "entry_ts_ms": 10_000_i64,
                    "exit_ts_ms": 20_000_i64,
                    "entry_price_raw_cents": 48.0,
                    "entry_price_cents": 48.7,
                    "pnl_net_cents": 5.0,
                    "pnl_gross_cents": 6.0,
                    "total_cost_cents": 1.0,
                    "entry_fee_cents": 0.2,
                    "exit_fee_cents": 0.3,
                    "entry_slippage_cents": 0.1,
                    "exit_slippage_cents": 0.1,
                    "entry_impact_cents": 0.0,
                    "exit_impact_cents": 0.0,
                    "duration_s": 10.0
                },
                {
                    "id": 2,
                    "side": "DOWN",
                    "entry_round_id": "SOLUSDT_5m_round_2",
                    "entry_ts_ms": 30_000_i64,
                    "exit_ts_ms": 38_000_i64,
                    "entry_price_raw_cents": 52.0,
                    "entry_price_cents": 52.8,
                    "pnl_net_cents": -3.0,
                    "pnl_gross_cents": -2.5,
                    "total_cost_cents": 0.5,
                    "entry_fee_cents": 0.1,
                    "exit_fee_cents": 0.2,
                    "entry_slippage_cents": 0.1,
                    "exit_slippage_cents": 0.1,
                    "entry_impact_cents": 0.05,
                    "exit_impact_cents": 0.05,
                    "duration_s": 8.0
                }
            ],
            "live_execution": {
                "paper_records": [
                    {
                        "decision_id": "decision-1",
                        "action": "enter",
                        "side": "UP",
                        "ts_ms": 100_i64
                    },
                    {
                        "decision_id": "decision-2",
                        "action": "exit",
                        "side": "UP",
                        "ts_ms": 220_i64
                    }
                ],
                "live_records": [
                    {
                        "decision_id": "decision-2",
                        "ts_ms": 300_i64
                    }
                ]
            }
        });

        let payload =
            build_paper_ledger_payload(Some(&existing), &runtime_payload, "SOLUSDT", "5m", None);
        assert_eq!(
            payload.get("source").and_then(Value::as_str),
            Some("ledger")
        );
        assert_eq!(
            payload
                .get("summary")
                .and_then(|v| v.get("trade_count"))
                .and_then(Value::as_u64),
            Some(2)
        );
        assert_eq!(
            payload
                .get("summary")
                .and_then(|v| v.get("net_pnl_cents"))
                .and_then(Value::as_f64),
            Some(2.0)
        );
        assert_eq!(
            payload
                .get("paper_records")
                .and_then(Value::as_array)
                .map(|rows| rows.len()),
            Some(2)
        );
        assert_eq!(
            payload
                .get("live_records")
                .and_then(Value::as_array)
                .map(|rows| rows.len()),
            Some(2)
        );
        assert_eq!(
            payload.get("symbol").and_then(Value::as_str),
            Some("SOLUSDT")
        );
    }

    #[test]
    fn merge_unique_trade_rows_uses_stable_trade_identity() {
        let existing = vec![json!({
            "id": 1,
            "side": "UP",
            "entry_round_id": "BTCUSDT_5m_1",
            "entry_ts_ms": 1_000_i64,
            "entry_price_raw_cents": 50.0,
            "entry_price_cents": 50.8,
            "exit_ts_ms": 2_000_i64,
            "pnl_net_cents": 1.0
        })];
        let fresh = vec![
            json!({
                "id": 2,
                "side": "UP",
                "entry_round_id": "BTCUSDT_5m_1",
                "entry_ts_ms": 1_000_i64,
                "entry_price_raw_cents": 50.0,
                "entry_price_cents": 50.8,
                "exit_ts_ms": 2_500_i64,
                "pnl_net_cents": 1.5
            }),
            json!({
                "id": 1,
                "side": "DOWN",
                "entry_round_id": "BTCUSDT_5m_2",
                "entry_ts_ms": 3_000_i64,
                "entry_price_raw_cents": 49.0,
                "entry_price_cents": 49.7,
                "exit_ts_ms": 4_000_i64,
                "pnl_net_cents": -2.0
            }),
        ];
        let merged = merge_unique_trade_rows(&existing, &fresh, 10);
        assert_eq!(merged.len(), 2);
        assert_eq!(
            merged[0].get("exit_ts_ms").and_then(Value::as_i64),
            Some(2_500_i64)
        );
    }

    #[test]
    fn build_paper_ledger_payload_from_sources_merges_historical_and_runtime_tail() {
        let existing = json!({
            "paper_records": [
                { "decision_id": "paper-1", "ts_ms": 111_i64 }
            ],
            "live_records": [
                { "decision_id": "live-1", "ts_ms": 222_i64 }
            ]
        });
        let historical_trades = vec![json!({
            "id": 11,
            "side": "UP",
            "entry_round_id": "ETHUSDT_5m_hist",
            "entry_ts_ms": 10_000_i64,
            "exit_ts_ms": 12_000_i64,
            "pnl_net_cents": 3.5,
            "pnl_gross_cents": 4.0,
            "total_cost_cents": 0.5,
            "duration_s": 2.0
        })];
        let runtime_payload = json!({
            "symbol": "ETHUSDT",
            "market_type": "5m",
            "lookback_minutes": 1440,
            "samples": 1200,
            "current": { "timestamp_ms": 123_000_i64, "round_id": "ETHUSDT_5m_now" },
            "trades": [
                {
                    "id": 12,
                    "side": "DOWN",
                    "entry_round_id": "ETHUSDT_5m_now",
                    "entry_ts_ms": 20_000_i64,
                    "exit_ts_ms": 22_000_i64,
                    "pnl_net_cents": -1.0,
                    "pnl_gross_cents": -0.8,
                    "total_cost_cents": 0.2,
                    "duration_s": 2.0
                }
            ],
            "live_execution": {
                "paper_records": [
                    { "decision_id": "paper-2", "ts_ms": 333_i64 }
                ],
                "live_records": [
                    { "decision_id": "live-2", "ts_ms": 444_i64 }
                ]
            }
        });
        let payload = build_paper_ledger_payload_from_sources(
            Some(&existing),
            Some(&runtime_payload),
            &historical_trades,
            "ETHUSDT",
            "5m",
            None,
        );
        assert_eq!(
            payload.get("history_source").and_then(Value::as_str),
            Some("clickhouse_plus_runtime")
        );
        assert_eq!(
            payload
                .get("summary")
                .and_then(|v| v.get("trade_count"))
                .and_then(Value::as_u64),
            Some(2)
        );
        assert_eq!(
            payload
                .get("trades")
                .and_then(Value::as_array)
                .map(|rows| rows.len()),
            Some(2)
        );
    }

    #[test]
    fn build_live_fill_decision_map_aggregates_same_intent() {
        let events = vec![
            json!({
                "intent_id": "intent-1",
                "decision_id": "decision-a",
                "order_id": "order-1",
                "reason": "rust_order_terminal_filled",
                "ts_ms": 100_i64,
                "fill_ts_ms": 100_i64,
                "action": "enter",
                "side": "UP",
                "fill_price_cents": 60.0,
                "actual_fill_price_cents": 60.0,
                "fill_quote_usdc": 1.2,
                "fill_size_shares": 2.0,
                "fill_fee_cents": 1.0,
                "actual_fee_cents": 1.0,
                "fill_slippage_cents": 0.2,
                "actual_slippage_cents": 0.2,
                "fill_cost_cents": 121.0,
                "fill_pnl_cents_net": 3.0,
                "ack_to_fill_ms": 40_i64,
                "submit_to_fill_ms": 60_i64,
                "signal_to_fill_ms": 90_i64,
                "trigger_to_fill_ms": 70_i64
            }),
            json!({
                "intent_id": "intent-1",
                "decision_id": "decision-b",
                "order_id": "order-2",
                "reason": "rust_cancel_post_reconcile_filled",
                "ts_ms": 200_i64,
                "fill_ts_ms": 200_i64,
                "action": "enter",
                "side": "UP",
                "fill_price_cents": 61.0,
                "actual_fill_price_cents": 61.0,
                "fill_quote_usdc": 1.83,
                "fill_size_shares": 3.0,
                "fill_fee_cents": 1.5,
                "actual_fee_cents": 1.5,
                "fill_slippage_cents": 0.4,
                "actual_slippage_cents": 0.4,
                "fill_cost_cents": 184.5,
                "fill_pnl_cents_net": 4.0,
                "ack_to_fill_ms": 55_i64,
                "submit_to_fill_ms": 80_i64,
                "signal_to_fill_ms": 120_i64,
                "trigger_to_fill_ms": 95_i64
            }),
        ];

        let (by_decision, summary) = build_live_fill_decision_map(&events);
        assert_eq!(
            summary.get("fill_event_count").and_then(Value::as_u64),
            Some(2)
        );
        assert_eq!(
            summary.get("fill_decision_count").and_then(Value::as_u64),
            Some(1)
        );
        let row = by_decision.get("intent-1").expect("aggregated fill row");
        assert_eq!(row.get("order_id").and_then(Value::as_str), Some("order-2"));
        assert_eq!(row.get("fill_event_count").and_then(Value::as_u64), Some(2));
        assert_eq!(
            row.get("fill_size_shares")
                .and_then(Value::as_f64)
                .map(|v| (v * 100.0).round() / 100.0),
            Some(5.0)
        );
        assert_eq!(
            row.get("fill_quote_usdc")
                .and_then(Value::as_f64)
                .map(|v| (v * 100.0).round() / 100.0),
            Some(3.03)
        );
        assert_eq!(
            row.get("actual_fill_price_cents")
                .and_then(Value::as_f64)
                .map(|v| (v * 100.0).round() / 100.0),
            Some(60.6)
        );
        assert_eq!(
            row.get("actual_fee_cents")
                .and_then(Value::as_f64)
                .map(|v| (v * 100.0).round() / 100.0),
            Some(2.5)
        );
        assert_eq!(row.get("ack_to_fill_ms").and_then(Value::as_i64), Some(55));
        assert_eq!(
            row.get("order_ids")
                .and_then(Value::as_array)
                .map(|rows| rows.len()),
            Some(2)
        );
    }

    #[test]
    fn build_live_completed_records_materializes_filled_trade_rows() {
        let paper_records = vec![json!({
            "intent_id": "intent-1",
            "decision_id": "intent-1",
            "action": "exit",
            "side": "UP",
            "round_id": "SOLUSDT_5m_1",
            "paper_record_source": "current_summary",
            "paper_record_virtual": true,
            "paper_exit_exec_price_cents": 63.0
        })];
        let fill_by_decision = HashMap::from([(
            "intent-1".to_string(),
            json!({
                "intent_id": "intent-1",
                "decision_id": "intent-1",
                "action": "exit",
                "side": "UP",
                "fill_ts_ms": 222_i64,
                "fill_price_cents": 63.0,
                "actual_fill_price_cents": 63.0,
                "fill_quote_usdc": 3.15,
                "fill_size_shares": 5.0,
                "actual_fee_cents": 0.0,
                "actual_slippage_cents": 0.0,
                "fill_pnl_cents_net": -45.0,
                "fill_event_count": 1_u64,
                "order_ids": ["order-1"]
            }),
        )]);
        let live_order_lineage = vec![json!({
            "intent_id": "intent-1",
            "decision_id": "intent-1",
            "order_ids": ["order-1"],
            "pending_order_ids": [],
            "pending_count": 0,
            "last_reason": "rust_order_terminal_filled",
            "last_status": "filled",
            "last_ts_ms": 222_i64
        })];

        let rows =
            build_live_completed_records(&paper_records, &fill_by_decision, &live_order_lineage);
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(
            row.get("live_status").and_then(Value::as_str),
            Some("filled")
        );
        assert_eq!(
            row.get("actual_fill_price_cents").and_then(Value::as_f64),
            Some(63.0)
        );
        assert_eq!(
            row.get("live_fill_pnl_cents_net").and_then(Value::as_f64),
            Some(-45.0)
        );
        assert_eq!(
            row.get("order_ids")
                .and_then(Value::as_array)
                .map(|rows| rows.len()),
            Some(1)
        );
        assert_eq!(
            row.get("paper_record_source").and_then(Value::as_str),
            Some("current_summary")
        );
    }

    #[test]
    fn summarize_live_order_latency_reports_percentiles() {
        let orders = vec![
            json!({
                "signal_to_trigger_ms": 3.0,
                "trigger_to_submit_ms": 5.0,
                "signal_to_submit_ms": 8.0,
                "signal_to_ack_ms": 74.0,
                "submit_to_ack_ms": 66.0,
                "order_latency_ms": 68.0
            }),
            json!({
                "signal_to_trigger_ms": 4.0,
                "trigger_to_submit_ms": 10.0,
                "signal_to_submit_ms": 14.0,
                "signal_to_ack_ms": 88.0,
                "submit_to_ack_ms": 74.0,
                "order_latency_ms": 75.0
            }),
            json!({
                "signal_to_trigger_ms": 6.0,
                "trigger_to_submit_ms": 16.0,
                "signal_to_submit_ms": 22.0,
                "signal_to_ack_ms": 130.0,
                "submit_to_ack_ms": 108.0,
                "order_latency_ms": 111.0
            }),
        ];
        let summary = summarize_live_order_latency(&orders);
        assert_eq!(
            summary
                .get("signal_to_trigger_ms")
                .and_then(|v| v.get("p50_ms"))
                .and_then(Value::as_f64),
            Some(4.0)
        );
        assert_eq!(
            summary
                .get("trigger_to_submit_ms")
                .and_then(|v| v.get("p95_ms"))
                .and_then(Value::as_f64),
            Some(16.0)
        );
        assert_eq!(
            summary
                .get("signal_to_submit_ms")
                .and_then(|v| v.get("count"))
                .and_then(Value::as_u64),
            Some(3)
        );
        assert_eq!(
            summary
                .get("signal_to_submit_ms")
                .and_then(|v| v.get("p50_ms"))
                .and_then(Value::as_f64),
            Some(14.0)
        );
        assert_eq!(
            summary
                .get("signal_to_ack_ms")
                .and_then(|v| v.get("p95_ms"))
                .and_then(Value::as_f64),
            Some(130.0)
        );
    }

    #[test]
    fn summarize_live_price_parity_reports_abs_percentiles() {
        let orders = vec![
            json!({
                "price_trace": {
                    "signal_vs_submit_cents": 1.2,
                    "signal_vs_accepted_cents": -0.8
                }
            }),
            json!({
                "price_trace": {
                    "signal_vs_submit_cents": -2.0,
                    "signal_vs_accepted_cents": -1.4
                }
            }),
            json!({
                "price_trace": {
                    "signal_vs_submit_cents": 0.6,
                    "signal_vs_accepted_cents": 0.4
                }
            }),
        ];
        let parity = summarize_live_price_parity(&orders);
        assert_eq!(
            parity
                .get("signal_vs_submit_cents")
                .and_then(|v| v.get("count"))
                .and_then(Value::as_u64),
            Some(3)
        );
        assert_eq!(
            parity
                .get("signal_vs_submit_cents")
                .and_then(|v| v.get("p50_abs_cents"))
                .and_then(Value::as_f64),
            Some(1.2)
        );
        assert_eq!(
            parity
                .get("signal_vs_accepted_cents")
                .and_then(|v| v.get("p95_abs_cents"))
                .and_then(Value::as_f64),
            Some(1.4)
        );
    }
}
