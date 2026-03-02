use std::cmp::min;
use std::collections::{HashMap, HashSet, VecDeque};
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
    live_gateway_report_seq: Arc<RwLock<i64>>,
    live_runtime_snapshots: Arc<RwLock<HashMap<String, Value>>>,
    live_runtime_controls: Arc<RwLock<HashMap<String, LiveRuntimeControl>>>,
    live_persist_inflight: Arc<RwLock<HashSet<String>>>,
    live_execution_aggr_states: Arc<RwLock<HashMap<String, LiveExecutionAggState>>>,
    live_rust_executor: Arc<RwLock<Option<Arc<RustExecutorContext>>>>,
    live_rust_book_cache: Arc<RwLock<HashMap<String, RustBookCacheEntry>>>,
    runtime_alert_throttle: Arc<RwLock<HashMap<String, i64>>>,
    runtime_daily_report_sent: Arc<RwLock<HashSet<String>>>,
    strategy_heavy_slots: Arc<Semaphore>,
    strategy_live_source_slots: Arc<Semaphore>,
    gateway_http_client: Arc<reqwest::Client>,
}

#[derive(Clone)]
struct ChartCacheEntry {
    created_at: Instant,
    payload: Value,
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
const LIVE_RUST_BOOK_CACHE_MAX: usize = 512;
const LIVE_ALERT_THROTTLE_DEFAULT_MS: i64 = 5 * 60_000;

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

fn live_min_order_size_shares() -> f64 {
    std::env::var("FORGE_FEV1_MIN_ORDER_SIZE_SHARES")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(5.0)
        .clamp(0.01, 1_000.0)
}

fn enforce_min_order_quote_usdc(quote_usdc: f64, price_cents: f64) -> f64 {
    let px = (price_cents / 100.0).clamp(0.01, 0.99);
    let min_quote = live_min_order_size_shares() * px;
    quote_usdc.max(min_quote)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LiveExecutorMode {
    Gateway,
    RustSdk,
}

impl LiveExecutorMode {
    fn from_env() -> Self {
        let raw = std::env::var("FORGE_FEV1_EXECUTOR")
            .ok()
            .map(|v| v.trim().to_ascii_lowercase())
            .unwrap_or_else(|| "gateway".to_string());
        match raw.as_str() {
            "rust" | "rust_sdk" | "rust-native" | "rust_native" => Self::RustSdk,
            _ => Self::Gateway,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Gateway => "clob_gateway",
            Self::RustSdk => "rust_sdk",
        }
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
    fn flat(market_type: &str, now_ms: i64) -> Self {
        Self {
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
    cancel_after_ms: i64,
    retry_count: u8,
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
    markets: Vec<String>,
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
            .unwrap_or(450)
            .clamp(200, 10_000);
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
            markets,
        }
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
        .unwrap_or(120)
        .clamp(80, base_loop_ms.max(80))
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
        .unwrap_or(40_000)
        .clamp(5_000, 120_000)
}

fn runtime_target_keepalive_ms() -> i64 {
    std::env::var("FORGE_FEV1_RUNTIME_TARGET_KEEPALIVE_MS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(2_500)
        .clamp(500, 30_000)
}

#[derive(Debug, Clone)]
struct LiveRuntimeWakeEvent {
    market_type: String,
    source: String,
    ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LiveExecutionAggState {
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
            updated_ts_ms: Utc::now().timestamp_millis(),
        }
    }
}

fn parse_snapshot_event_market_type(payload: &str) -> Option<String> {
    let v: Value = serde_json::from_str(payload).ok()?;
    let tf = v
        .get("timeframe")
        .and_then(Value::as_str)
        .map(|s| s.trim().to_ascii_lowercase())?;
    if tf == "5m" || tf == "15m" {
        Some(tf)
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

    async fn get_live_position_state(&self, market_type: &str) -> LivePositionState {
        let now_ms = Utc::now().timestamp_millis();
        {
            let states = self.live_position_states.read().await;
            if let Some(v) = states.get(market_type) {
                return v.clone();
            }
        }
        let mut states = self.live_position_states.write().await;
        let entry = states
            .entry(market_type.to_string())
            .or_insert_with(|| LivePositionState::flat(market_type, now_ms));
        entry.clone()
    }

    async fn put_live_position_state(&self, market_type: &str, next: LivePositionState) {
        let mut states = self.live_position_states.write().await;
        states.insert(market_type.to_string(), next);
    }

    async fn get_live_runtime_control(&self, market_type: &str) -> LiveRuntimeControl {
        let now_ms = Utc::now().timestamp_millis();
        {
            let controls = self.live_runtime_controls.read().await;
            if let Some(v) = controls.get(market_type) {
                return v.clone();
            }
        }
        let mut controls = self.live_runtime_controls.write().await;
        let entry = controls
            .entry(market_type.to_string())
            .or_insert_with(|| LiveRuntimeControl::normal(now_ms));
        entry.clone()
    }

    async fn put_live_runtime_control(&self, market_type: &str, next: LiveRuntimeControl) {
        let mut controls = self.live_runtime_controls.write().await;
        controls.insert(market_type.to_string(), next);
    }

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

    async fn append_live_event(&self, market_type: &str, mut event: Value) {
        let now_ms = Utc::now().timestamp_millis();
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
        self.persist_live_runtime_state_async(market_type).await;
    }

    async fn list_live_events(&self, market_type: &str, limit: usize) -> Vec<Value> {
        let events = self.live_events.read().await;
        events
            .iter()
            .rev()
            .filter(|v| {
                v.get("market_type")
                    .and_then(Value::as_str)
                    .map(|s| s == market_type)
                    .unwrap_or(false)
            })
            .take(limit.max(1))
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    async fn get_runtime_snapshot(&self, market_type: &str) -> Option<Value> {
        let store = self.live_runtime_snapshots.read().await;
        store.get(market_type).cloned()
    }

    async fn set_runtime_snapshot(&self, market_type: &str, payload: Value) {
        let mut store = self.live_runtime_snapshots.write().await;
        store.insert(market_type.to_string(), payload);
    }

    async fn persist_live_runtime_state_async(&self, market_type: &str) {
        {
            let mut inflight = self.live_persist_inflight.write().await;
            if !inflight.insert(market_type.to_string()) {
                return;
            }
        }
        let state = self.clone();
        let market = market_type.to_string();
        tokio::spawn(async move {
            persist_live_runtime_state(&state, &market).await;
            let mut inflight = state.live_persist_inflight.write().await;
            inflight.remove(&market);
        });
    }

    async fn get_live_execution_aggr_state(&self, market_type: &str) -> LiveExecutionAggState {
        let mut store = self.live_execution_aggr_states.write().await;
        store
            .entry(market_type.to_string())
            .or_insert_with(|| LiveExecutionAggState::new(market_type))
            .clone()
    }

    async fn apply_aggressiveness_to_gateway_cfg(
        &self,
        market_type: &str,
        cfg: &LiveGatewayConfig,
    ) -> (LiveGatewayConfig, LiveExecutionAggState) {
        let s = self.get_live_execution_aggr_state(market_type).await;
        let mut tuned = cfg.clone();
        tuned.entry_slippage_bps =
            (cfg.entry_slippage_bps * s.entry_slippage_mult).clamp(0.0, 500.0);
        tuned.exit_slippage_bps = (cfg.exit_slippage_bps * s.exit_slippage_mult).clamp(0.0, 500.0);
        (tuned, s)
    }

    async fn update_live_execution_aggr_from_orders(
        &self,
        market_type: &str,
        orders: &[Value],
    ) -> LiveExecutionAggState {
        let mut store = self.live_execution_aggr_states.write().await;
        let st = store
            .entry(market_type.to_string())
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
        }
        st.updated_ts_ms = Utc::now().timestamp_millis();
        st.clone()
    }

    async fn upsert_pending_order(&self, row: LivePendingOrder) {
        let mut pending = self.live_pending_orders.write().await;
        pending.insert(row.order_id.clone(), row);
    }

    async fn remove_pending_order(&self, order_id: &str) -> Option<LivePendingOrder> {
        let mut pending = self.live_pending_orders.write().await;
        pending.remove(order_id)
    }

    async fn list_pending_orders(&self) -> Vec<LivePendingOrder> {
        let pending = self.live_pending_orders.read().await;
        pending.values().cloned().collect()
    }

    async fn list_pending_orders_for_market(&self, market_type: &str) -> Vec<LivePendingOrder> {
        let pending = self.live_pending_orders.read().await;
        pending
            .values()
            .filter(|p| p.market_type.eq_ignore_ascii_case(market_type))
            .cloned()
            .collect()
    }

    async fn pending_flags_for_market(&self, market_type: &str) -> (bool, bool) {
        let pending = self.live_pending_orders.read().await;
        let mut has_enter_pending = false;
        let mut has_exit_pending = false;
        for row in pending.values() {
            if !row.market_type.eq_ignore_ascii_case(market_type) {
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

    async fn get_gateway_report_seq(&self) -> i64 {
        *self.live_gateway_report_seq.read().await
    }

    async fn set_gateway_report_seq(&self, seq: i64) {
        let mut current = self.live_gateway_report_seq.write().await;
        *current = seq;
    }

    async fn get_rust_book_cache(&self, token_id: &str) -> Option<GatewayBookSnapshot> {
        let cache = self.live_rust_book_cache.read().await;
        let row = cache.get(token_id)?;
        if row.fetched_at.elapsed() > Duration::from_millis(LIVE_RUST_BOOK_CACHE_TTL_MS) {
            return None;
        }
        Some(row.snapshot.clone())
    }

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
struct HistoryQueryParams {
    lookback_minutes: Option<u32>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct ChartQueryParams {
    market_type: String,
    minutes: Option<u32>,
    max_points: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct CollectorMetricsQueryParams {
    symbol: Option<String>,
    window_sec: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct RoundQueryParams {
    market_type: String,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct AvailableRoundsQueryParams {
    market_type: String,
}

#[derive(Debug, Deserialize)]
struct RoundChartQueryParams {
    round_id: String,
    market_type: Option<String>,
    max_points: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct HeatmapQueryParams {
    market_type: String,
    lookback_hours: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct AccuracyQueryParams {
    market_type: String,
    window: Option<u32>,
    lookback_hours: Option<u32>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct StrategyPaperQueryParams {
    source: Option<String>,
    profile: Option<String>,
    market_type: Option<String>,
    symbol: Option<String>,
    lookback_minutes: Option<u32>,
    max_points: Option<u32>,
    max_samples: Option<u32>,
    max_trades: Option<u32>,
    full_history: Option<bool>,
    entry_threshold_base: Option<f64>,
    entry_threshold_cap: Option<f64>,
    spread_limit_prob: Option<f64>,
    entry_edge_prob: Option<f64>,
    entry_min_potential_cents: Option<f64>,
    entry_max_price_cents: Option<f64>,
    min_hold_ms: Option<i64>,
    stop_loss_cents: Option<f64>,
    reverse_signal_threshold: Option<f64>,
    reverse_signal_ticks: Option<u32>,
    trail_activate_profit_cents: Option<f64>,
    trail_drawdown_cents: Option<f64>,
    take_profit_near_max_cents: Option<f64>,
    endgame_take_profit_cents: Option<f64>,
    endgame_remaining_ms: Option<i64>,
    liquidity_widen_prob: Option<f64>,
    cooldown_ms: Option<i64>,
    max_entries_per_round: Option<u32>,
    max_exec_spread_cents: Option<f64>,
    slippage_cents_per_side: Option<f64>,
    fee_cents_per_side: Option<f64>,
    emergency_wide_spread_penalty_ratio: Option<f64>,
    stop_loss_grace_ticks: Option<u32>,
    stop_loss_hard_mult: Option<f64>,
    stop_loss_reverse_extra_ticks: Option<u32>,
    loss_cluster_limit: Option<u32>,
    loss_cluster_cooldown_ms: Option<i64>,
    noise_gate_enabled: Option<bool>,
    noise_gate_threshold_add: Option<f64>,
    noise_gate_edge_add: Option<f64>,
    noise_gate_spread_scale: Option<f64>,
    vic_enabled: Option<bool>,
    vic_target_entries_per_hour: Option<f64>,
    vic_deadband_ratio: Option<f64>,
    vic_threshold_relax_max: Option<f64>,
    vic_edge_relax_max: Option<f64>,
    vic_spread_relax_max: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StrategyPaperSource {
    Replay,
    Live,
    Auto,
}

fn parse_strategy_paper_source(raw: Option<&str>) -> StrategyPaperSource {
    match raw
        .map(|v| v.trim().to_ascii_lowercase())
        .unwrap_or_else(|| "replay".to_string())
        .as_str()
    {
        "live" => StrategyPaperSource::Live,
        "auto" => StrategyPaperSource::Auto,
        _ => StrategyPaperSource::Replay,
    }
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
        live_gateway_report_seq: Arc::new(RwLock::new(0)),
        live_runtime_snapshots: Arc::new(RwLock::new(HashMap::new())),
        live_runtime_controls: Arc::new(RwLock::new(HashMap::new())),
        live_persist_inflight: Arc::new(RwLock::new(HashSet::new())),
        live_execution_aggr_states: Arc::new(RwLock::new(HashMap::new())),
        live_rust_executor: Arc::new(RwLock::new(None)),
        live_rust_book_cache: Arc::new(RwLock::new(HashMap::new())),
        runtime_alert_throttle: Arc::new(RwLock::new(HashMap::new())),
        runtime_daily_report_sent: Arc::new(RwLock::new(HashSet::new())),
        strategy_heavy_slots: Arc::new(Semaphore::new(strategy_heavy_slots)),
        strategy_live_source_slots: Arc::new(Semaphore::new(strategy_live_source_slots)),
        gateway_http_client: Arc::new(gateway_http_client),
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
        .route("/api/strategy/live/reset", post(strategy_live_reset))
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

fn live_position_state_key(redis_prefix: &str, market_type: &str) -> String {
    format!("{redis_prefix}:fev1:live:position:{market_type}")
}

fn live_pending_orders_key(redis_prefix: &str) -> String {
    format!("{redis_prefix}:fev1:live:pending")
}

fn live_events_key(redis_prefix: &str, market_type: &str) -> String {
    format!("{redis_prefix}:fev1:live:events:{market_type}")
}

fn live_runtime_control_key(redis_prefix: &str, market_type: &str) -> String {
    format!("{redis_prefix}:fev1:live:runtime_control:{market_type}")
}

fn live_gateway_seq_key(redis_prefix: &str) -> String {
    format!("{redis_prefix}:fev1:live:gateway_seq")
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
    tokio::spawn(async move {
        if let Err(err) = restore_live_runtime_state(&state, &cfg).await {
            tracing::warn!(?err, "restore live runtime state failed");
        }
        live_runtime_loop(state, cfg, wake_rx).await;
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
                    let Some(market_type) = parse_snapshot_event_market_type(&payload) else {
                        continue;
                    };
                    let _ = wake_tx.send(LiveRuntimeWakeEvent {
                        market_type,
                        source: "redis_snapshot_event".to_string(),
                        ts_ms: Utc::now().timestamp_millis(),
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
    for market in &cfg.markets {
        let key = live_position_state_key(&state.redis_prefix, market);
        if let Some(v) = read_key_value(state, &key).await? {
            if let Ok(parsed) = serde_json::from_value::<LivePositionState>(v) {
                state.put_live_position_state(market, parsed).await;
            }
        }
        let control_key = live_runtime_control_key(&state.redis_prefix, market);
        if let Some(v) = read_key_value(state, &control_key).await? {
            if let Ok(parsed) = serde_json::from_value::<LiveRuntimeControl>(v) {
                state.put_live_runtime_control(market, parsed).await;
            }
        }
        let events_key = live_events_key(&state.redis_prefix, market);
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
    let seq_key = live_gateway_seq_key(&state.redis_prefix);
    if let Some(v) = read_key_value(state, &seq_key).await? {
        let seq = v.get("seq").and_then(Value::as_i64).unwrap_or(0);
        state.set_gateway_report_seq(seq).await;
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
    Ok(())
}

async fn persist_live_runtime_state(state: &ApiState, market_type: &str) {
    let position = state.get_live_position_state(market_type).await;
    let pos_key = live_position_state_key(&state.redis_prefix, market_type);
    let _ = write_key_value(state, &pos_key, &json!(position), Some(2 * 24 * 3600)).await;
    let runtime_control = state.get_live_runtime_control(market_type).await;
    let control_key = live_runtime_control_key(&state.redis_prefix, market_type);
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

    let seq = state.get_gateway_report_seq().await;
    let seq_key = live_gateway_seq_key(&state.redis_prefix);
    let _ = write_key_value(state, &seq_key, &json!({ "seq": seq }), Some(2 * 24 * 3600)).await;

    let events_key = live_events_key(&state.redis_prefix, market_type);
    let events_rows = state
        .list_live_events(market_type, live_event_log_max())
        .await;
    let _ = write_key_value(
        state,
        &events_key,
        &json!(events_rows),
        Some(live_event_log_ttl_sec()),
    )
    .await;
}

async fn live_runtime_loop(
    state: ApiState,
    bootstrap: LiveRuntimeConfig,
    mut wake_rx: mpsc::UnboundedReceiver<LiveRuntimeWakeEvent>,
) {
    tracing::info!(
        markets = ?bootstrap.markets,
        live_execute = bootstrap.live_execute,
        loop_ms = bootstrap.loop_interval_ms,
        "fev1 live runtime started"
    );
    let mut last_round_seen: HashMap<String, String> = HashMap::new();
    let mut target_keepalive_at: HashMap<String, i64> = HashMap::new();
    let mut next_wait_ms = bootstrap.loop_interval_ms;
    loop {
        let cfg = LiveRuntimeConfig::from_env();
        let push_cfg = ServerChanConfig::from_env();
        let fast_enabled = runtime_fast_loop_enabled();
        let fast_loop_ms = runtime_fast_loop_ms(cfg.loop_interval_ms);
        let fast_margin = runtime_fast_margin_threshold();
        let target_prewarm_ms = runtime_target_prewarm_ms();
        let target_keepalive_ms = runtime_target_keepalive_ms();
        let mut cycle_sleep_ms = cfg.loop_interval_ms;
        let wait_ms = next_wait_ms.clamp(20, cfg.loop_interval_ms.max(20));
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
        let wake_markets: HashSet<String> = wake_events
            .iter()
            .filter_map(|ev| {
                if cfg
                    .markets
                    .iter()
                    .any(|m| m.eq_ignore_ascii_case(&ev.market_type))
                {
                    Some(ev.market_type.to_ascii_lowercase())
                } else {
                    None
                }
            })
            .collect();
        let wake_event_scope_enabled = !wake_markets.is_empty();
        if !cfg.enabled {
            next_wait_ms = cfg.loop_interval_ms;
            continue;
        }

        for market in &cfg.markets {
            let market_type = market.as_str();
            if wake_event_scope_enabled && !wake_markets.contains(&market_type.to_ascii_lowercase())
            {
                continue;
            }
            let trigger_for_market = wake_events
                .iter()
                .rev()
                .find(|ev| ev.market_type.eq_ignore_ascii_case(market_type));
            let now_ms = Utc::now().timestamp_millis();
            let last_keepalive = target_keepalive_at.get(market_type).copied().unwrap_or(0);
            let due_keepalive = now_ms.saturating_sub(last_keepalive) >= target_keepalive_ms;
            if due_keepalive {
                // Keep target discovery warm during steady-state to reduce market target misses.
                if matches!(
                    tokio::time::timeout(
                        Duration::from_millis(360),
                        resolve_live_market_target_with_state(&state, market_type),
                    )
                    .await,
                    Ok(Ok(_))
                ) {
                    target_keepalive_at.insert(market_type.to_string(), now_ms);
                    cycle_sleep_ms = cycle_sleep_ms.min(fast_loop_ms);
                }
            }
            let position_before_cycle = state.get_live_position_state(market_type).await;
            let pending_before_cycle = state.list_pending_orders_for_market(market_type).await;
            let pending_before_count = pending_before_cycle.len();
            let mut runtime_control = state.get_live_runtime_control(market_type).await;
            let exec_aggr = state.get_live_execution_aggr_state(market_type).await;
            let mut effective_live_execute = cfg.live_execute;
            let mut effective_drain_only = cfg.drain_only;
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
                                .put_live_runtime_control(market_type, runtime_control.clone())
                                .await;
                        }
                    } else {
                        // Keep live execution enabled in drain mode until existing position is closed.
                        effective_live_execute = true;
                    }
                }
            }
            let strategy_cfg = StrategyRuntimeConfig::default();
            let runtime_cfg_source = "live_default";
            match strategy_paper_live(StrategyPaperLiveReq {
                state: &state,
                market_type,
                full_history: false,
                lookback_minutes: cfg.lookback_minutes,
                max_points: cfg.max_points,
                max_trades: cfg.max_trades,
                cfg: &strategy_cfg,
                config_source: runtime_cfg_source,
                live_execute: effective_live_execute,
                live_quote_usdc: cfg.quote_usdc,
                live_max_orders: cfg.max_orders,
                live_drain_only: effective_drain_only,
            })
            .await
            {
                Ok(mut payload) => {
                    let now = Utc::now();
                    let now_ms = now.timestamp_millis();
                    let current_round = payload
                        .get("current")
                        .and_then(|v| v.get("round_id"))
                        .and_then(Value::as_str)
                        .map(|v| v.to_string());
                    let current_remaining_ms = payload
                        .get("current")
                        .and_then(|v| v.get("remaining_ms"))
                        .and_then(Value::as_i64);
                    let round_switched = if let Some(round_id) = current_round.clone() {
                        if let Some(prev) =
                            last_round_seen.insert(market_type.to_string(), round_id.clone())
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
                                "trigger_market": trigger_for_market.map(|v| v.market_type.clone()),
                                "trigger_ts_ms": trigger_for_market.map(|v| v.ts_ms),
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
                            cycle_sleep_ms = cycle_sleep_ms.min(fast_loop_ms);
                        }
                        if round_switched {
                            // Event-driven assist: immediately tighten next cycle on round rollover.
                            cycle_sleep_ms = cycle_sleep_ms.min(fast_loop_ms);
                        }
                    }

                    if current_remaining_ms
                        .map(|v| v >= 0 && v <= target_prewarm_ms)
                        .unwrap_or(false)
                    {
                        // Round-switch prewarm is always on, even in paused mode, to eliminate no_live_market_target gaps.
                        let _ = tokio::time::timeout(
                            Duration::from_millis(420),
                            resolve_live_market_target_with_state(&state, market_type),
                        )
                        .await;
                        cycle_sleep_ms = cycle_sleep_ms.min(fast_loop_ms);
                    }

                    if wake_event_scope_enabled {
                        cycle_sleep_ms = cycle_sleep_ms.min(fast_loop_ms);
                    }

                    let status = payload
                        .get("status")
                        .and_then(Value::as_str)
                        .unwrap_or("ok")
                        .to_string();
                    let summary = payload.get("summary").cloned().unwrap_or(Value::Null);
                    state.set_runtime_snapshot(market_type, payload).await;
                    // Persist runtime state off the hot path to avoid Redis latency coupling.
                    state.persist_live_runtime_state_async(market_type).await;

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
                    let msg = format!("runtime_cycle_error:{}", err.message);
                    state
                        .append_live_event(
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
        }
        next_wait_ms = cycle_sleep_ms.clamp(20, cfg.loop_interval_ms.max(20));
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
        let cfg = LiveGatewayConfig {
            primary_url: "http://127.0.0.1:9001".to_string(),
            backup_url: None,
            timeout_ms: 500,
            min_quote_usdc: 1.0,
            entry_slippage_bps: 18.0,
            exit_slippage_bps: 22.0,
            force_slippage_bps: None,
            rust_submit_fallback_gateway: true,
        };
        let payload =
            decision_to_live_payload(&decision, &target, &cfg, None, None).expect("payload");
        assert_eq!(payload.get("tif").and_then(Value::as_str), Some("GTD"));
        assert_eq!(payload.get("style").and_then(Value::as_str), Some("maker"));
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
    fn select_live_decisions_skips_replay_force_close_reasons() {
        let decisions = vec![json!({
            "action": "exit",
            "side": "UP",
            "round_id": "r-1",
            "ts_ms": 1_000_i64,
            "reason": "end_of_samples_force_close"
        })];
        let selected = select_live_decisions(&decisions, 1_100, 1, false, Some("exit"));
        assert!(selected.is_empty());
    }

    #[test]
    fn select_live_decisions_does_not_fallback_to_stale_non_drain_decision() {
        let decisions = vec![json!({
            "action": "enter",
            "side": "UP",
            "round_id": "old-round",
            "ts_ms": 1_000_i64,
            "reason": "fev1_signal_entry"
        })];
        let latest_ts_ms = 250_000_i64;
        let selected = select_live_decisions(&decisions, latest_ts_ms, 1, false, Some("enter"));
        assert!(selected.is_empty());
    }
}
