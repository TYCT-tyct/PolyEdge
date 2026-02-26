use std::cmp::min;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use market_discovery::{DiscoveryConfig, MarketDescriptor, MarketDiscovery};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::RwLock;
use tower_http::services::{ServeDir, ServeFile};

use crate::fev1;

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
    chart_cache: Arc<RwLock<HashMap<String, ChartCacheEntry>>>,
    live_position_states: Arc<RwLock<HashMap<String, LivePositionState>>>,
    live_decision_guard: Arc<RwLock<HashMap<String, i64>>>,
    live_events: Arc<RwLock<VecDeque<Value>>>,
    live_pending_orders: Arc<RwLock<HashMap<String, LivePendingOrder>>>,
    live_gateway_report_seq: Arc<RwLock<i64>>,
    live_runtime_snapshots: Arc<RwLock<HashMap<String, Value>>>,
}

#[derive(Clone)]
struct ChartCacheEntry {
    created_at: Instant,
    payload: Value,
}

const CHART_CACHE_TTL_MS: u64 = 900;
const CHART_CACHE_MAX_ENTRIES: usize = 120;
const LIVE_DECISION_GUARD_TTL_MS: i64 = 45_000;
const LIVE_EVENT_LOG_MAX: usize = 240;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LivePositionState {
    market_type: String,
    state: String,
    side: Option<String>,
    entry_round_id: Option<String>,
    entry_ts_ms: Option<i64>,
    entry_price_cents: Option<f64>,
    entry_quote_usdc: Option<f64>,
    last_action: Option<String>,
    last_reason: Option<String>,
    total_entries: u64,
    total_exits: u64,
    updated_ts_ms: i64,
}

impl LivePositionState {
    fn flat(market_type: &str, now_ms: i64) -> Self {
        Self {
            market_type: market_type.to_string(),
            state: "flat".to_string(),
            side: None,
            entry_round_id: None,
            entry_ts_ms: None,
            entry_price_cents: None,
            entry_quote_usdc: None,
            last_action: None,
            last_reason: None,
            total_entries: 0,
            total_exits: 0,
            updated_ts_ms: now_ms,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LivePendingOrder {
    market_type: String,
    order_id: String,
    action: String,
    side: String,
    round_id: String,
    decision_key: String,
    price_cents: f64,
    quote_size_usdc: f64,
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
    entry_only: bool,
    quote_usdc: f64,
    markets: Vec<String>,
}

impl LiveRuntimeConfig {
    fn from_env() -> Self {
        let enabled = std::env::var("FORGE_FEV1_RUNTIME_ENABLED")
            .ok()
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(true);
        let live_execute = std::env::var("FORGE_FEV1_LIVE_EXECUTE")
            .ok()
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        let loop_interval_ms = std::env::var("FORGE_FEV1_RUNTIME_LOOP_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1200)
            .clamp(300, 10_000);
        let lookback_minutes = std::env::var("FORGE_FEV1_RUNTIME_LOOKBACK_MINUTES")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(360)
            .clamp(30, 24 * 60 * 7);
        let max_points = std::env::var("FORGE_FEV1_RUNTIME_MAX_POINTS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(220_000)
            .clamp(20_000, 1_000_000);
        let max_trades = std::env::var("FORGE_FEV1_RUNTIME_MAX_TRADES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(160)
            .clamp(20, 500);
        let max_orders = std::env::var("FORGE_FEV1_RUNTIME_MAX_ORDERS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1)
            .clamp(1, 6);
        let entry_only = std::env::var("FORGE_FEV1_RUNTIME_ENTRY_ONLY")
            .ok()
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(true);
        let quote_usdc = std::env::var("FORGE_FEV1_RUNTIME_QUOTE_USDC")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(1.0)
            .clamp(0.5, 500.0);
        let markets = std::env::var("FORGE_FEV1_RUNTIME_MARKETS")
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(|v| v.trim().to_ascii_lowercase())
                    .filter(|v| v == "5m" || v == "15m")
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| vec!["5m".to_string(), "15m".to_string()]);
        Self {
            enabled,
            live_execute,
            loop_interval_ms,
            lookback_minutes,
            max_points,
            max_trades,
            max_orders,
            entry_only,
            quote_usdc,
            markets,
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

    async fn should_skip_live_decision(&self, key: &str, now_ms: i64) -> bool {
        let mut guard = self.live_decision_guard.write().await;
        guard.retain(|_, ts| now_ms.saturating_sub(*ts) <= LIVE_DECISION_GUARD_TTL_MS);
        guard.contains_key(key)
    }

    async fn mark_live_decision_seen(&self, key: String, now_ms: i64) {
        let mut guard = self.live_decision_guard.write().await;
        guard.insert(key, now_ms);
    }

    async fn append_live_event(&self, market_type: &str, mut event: Value) {
        let now_ms = Utc::now().timestamp_millis();
        if event.get("market_type").is_none() {
            event["market_type"] = Value::String(market_type.to_string());
        }
        if event.get("ts_ms").is_none() {
            event["ts_ms"] = Value::from(now_ms);
        }
        let mut events = self.live_events.write().await;
        events.push_back(event);
        while events.len() > LIVE_EVENT_LOG_MAX {
            events.pop_front();
        }
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

    async fn get_gateway_report_seq(&self) -> i64 {
        *self.live_gateway_report_seq.read().await
    }

    async fn set_gateway_report_seq(&self, seq: i64) {
        let mut current = self.live_gateway_report_seq.write().await;
        *current = seq;
    }
}

const LIVE_SNAPSHOT_MAX_AGE_MS: i64 = 4_000;
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
    market_type: Option<String>,
    lookback_minutes: Option<u32>,
    max_points: Option<u32>,
    max_trades: Option<u32>,
    full_history: Option<bool>,
    use_autotune: Option<bool>,
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
    live_execute: Option<bool>,
    live_quote_usdc: Option<f64>,
    live_max_orders: Option<u32>,
    live_entry_only: Option<bool>,
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

#[derive(Debug, Deserialize)]
struct StrategyFullQueryParams {
    market_type: Option<String>,
    lookback_minutes: Option<u32>,
    max_points: Option<u32>,
    max_trades: Option<u32>,
    full_history: Option<bool>,
    max_arms: Option<u32>,
    window_trades: Option<u32>,
    target_win_rate: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct StrategyOptimizeQueryParams {
    market_type: Option<String>,
    lookback_minutes: Option<u32>,
    max_points: Option<u32>,
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
}

#[derive(Debug, Deserialize)]
struct StrategyAutotuneLatestQueryParams {
    market_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StrategyAutotuneHistoryQueryParams {
    market_type: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct StrategyAutotuneSetBody {
    market_type: Option<String>,
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
    let state = ApiState {
        ch_url: cfg.clickhouse_url,
        redis_prefix: cfg.redis_prefix,
        redis_client,
        chart_cache: Arc::new(RwLock::new(HashMap::new())),
        live_position_states: Arc::new(RwLock::new(HashMap::new())),
        live_decision_guard: Arc::new(RwLock::new(HashMap::new())),
        live_events: Arc::new(RwLock::new(VecDeque::new())),
        live_pending_orders: Arc::new(RwLock::new(HashMap::new())),
        live_gateway_report_seq: Arc::new(RwLock::new(0)),
        live_runtime_snapshots: Arc::new(RwLock::new(HashMap::new())),
    };

    let mut app = Router::new()
        .route("/", get(root_redirect))
        .route("/dashboard", get(dashboard_redirect))
        .route("/ws/live", get(ws_live))
        .route("/health/live", get(health_live))
        .route("/health/db", get(health_db))
        .route("/api/latest/all", get(latest_all))
        .route("/api/latest/tf/{timeframe}", get(latest_timeframe))
        .route("/api/latest/{symbol}/{timeframe}", get(latest_symbol_tf))
        .route(
            "/api/latest/{symbol}/{timeframe}/{market_id}",
            get(latest_market),
        )
        .route(
            "/api/history/{symbol}/{timeframe}",
            get(history_symbol_timeframe),
        )
        .route("/api/stats", get(stats))
        .route("/api/collector/status", get(collector_status))
        .route("/api/chart", get(chart))
        .route("/api/chart/round", get(chart_round))
        .route("/api/rounds", get(rounds))
        .route("/api/rounds/available", get(rounds_available))
        .route("/api/heatmap", get(heatmap))
        .route("/api/accuracy_series", get(accuracy_series))
        .route("/api/strategy/paper", get(strategy_paper))
        .route("/api/strategy/full", get(strategy_full))
        .route("/api/strategy/optimize", get(strategy_optimize))
        .route("/api/strategy/autotune/latest", get(strategy_autotune_latest))
        .route("/api/strategy/autotune/history", get(strategy_autotune_history))
        .route("/api/strategy/autotune/set", post(strategy_autotune_set))
        .route("/api/strategy/live/reset", post(strategy_live_reset))
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

fn live_gateway_seq_key(redis_prefix: &str) -> String {
    format!("{redis_prefix}:fev1:live:gateway_seq")
}

fn start_live_runtime(state: ApiState) {
    let cfg = LiveRuntimeConfig::from_env();
    if !cfg.enabled {
        tracing::info!("fev1 live runtime disabled by FORGE_FEV1_RUNTIME_ENABLED=false");
        return;
    }
    tokio::spawn(async move {
        if let Err(err) = restore_live_runtime_state(&state, &cfg).await {
            tracing::warn!(?err, "restore live runtime state failed");
        }
        live_runtime_loop(state, cfg).await;
    });
}

async fn restore_live_runtime_state(state: &ApiState, cfg: &LiveRuntimeConfig) -> Result<(), ApiError> {
    for market in &cfg.markets {
        let key = live_position_state_key(&state.redis_prefix, market);
        if let Some(v) = read_key_value(state, &key).await? {
            if let Ok(parsed) = serde_json::from_value::<LivePositionState>(v) {
                state.put_live_position_state(market, parsed).await;
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
    Ok(())
}

async fn persist_live_runtime_state(state: &ApiState, market_type: &str) {
    let position = state.get_live_position_state(market_type).await;
    let pos_key = live_position_state_key(&state.redis_prefix, market_type);
    let _ = write_key_value(state, &pos_key, &json!(position), Some(2 * 24 * 3600)).await;

    let pending_rows = state.list_pending_orders().await;
    let pending_key = live_pending_orders_key(&state.redis_prefix);
    let _ = write_key_value(state, &pending_key, &json!(pending_rows), Some(2 * 24 * 3600)).await;

    let seq = state.get_gateway_report_seq().await;
    let seq_key = live_gateway_seq_key(&state.redis_prefix);
    let _ = write_key_value(state, &seq_key, &json!({ "seq": seq }), Some(2 * 24 * 3600)).await;
}

async fn live_runtime_loop(state: ApiState, cfg: LiveRuntimeConfig) {
    tracing::info!(
        markets = ?cfg.markets,
        live_execute = cfg.live_execute,
        loop_ms = cfg.loop_interval_ms,
        "fev1 live runtime started"
    );
    loop {
        for market in &cfg.markets {
            let market_type = market.as_str();
            let strategy_cfg = StrategyRuntimeConfig::default();
            match strategy_paper_live(
                &state,
                market_type,
                false,
                cfg.lookback_minutes,
                cfg.max_points,
                cfg.max_trades,
                &strategy_cfg,
                cfg.live_execute,
                cfg.quote_usdc,
                cfg.max_orders,
                cfg.entry_only,
            )
            .await
            {
                Ok(payload) => {
                    state.set_runtime_snapshot(market_type, payload).await;
                    persist_live_runtime_state(&state, market_type).await;
                }
                Err(err) => {
                    let now_ms = Utc::now().timestamp_millis();
                    state
                        .append_live_event(
                            market_type,
                            json!({
                                "accepted": false,
                                "action": "runtime",
                                "side": "NONE",
                                "reason": format!("runtime_cycle_error:{}", err.message),
                                "ts_ms": now_ms
                            }),
                        )
                        .await;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(cfg.loop_interval_ms)).await;
    }
}

async fn root_redirect() -> Redirect {
    Redirect::temporary("/dashboard/")
}

async fn dashboard_redirect() -> Redirect {
    Redirect::temporary("/dashboard/")
}

async fn health_live() -> Json<Value> {
    Json(json!({
        "ok": true,
        "ts_ms": Utc::now().timestamp_millis(),
    }))
}

async fn health_db(State(state): State<ApiState>) -> Json<HealthResponse> {
    let ch = check_clickhouse(state.ch_url.as_deref()).await;
    let redis = check_redis(state.redis_client.as_ref()).await;
    Json(HealthResponse {
        ok: ch.ok && redis.ok,
        ts_ms: Utc::now().timestamp_millis(),
        clickhouse: ch,
        redis,
    })
}

async fn ws_live(ws: WebSocketUpgrade, State(state): State<ApiState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_live_loop(socket, state))
}

async fn ws_live_loop(mut socket: WebSocket, state: ApiState) {
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_payload = String::new();

    loop {
        interval.tick().await;
        let payload = match build_ws_live_payload(&state).await {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(?err, "ws live payload build failed");
                continue;
            }
        };
        let text = match serde_json::to_string(&payload) {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(?err, "ws live payload serialize failed");
                continue;
            }
        };

        if text == last_payload {
            continue;
        }
        if socket
            .send(Message::Text(text.clone().into()))
            .await
            .is_err()
        {
            break;
        }
        last_payload = text;
    }
}

async fn build_ws_live_payload(state: &ApiState) -> Result<Value, ApiError> {
    let five = fetch_latest_snapshot(state, "BTCUSDT", "5m").await?;
    let fifteen = fetch_latest_snapshot(state, "BTCUSDT", "15m").await?;
    Ok(json!({
        "5m": five.as_ref().map(|v| compact_live_snapshot(v, "5m")),
        "15m": fifteen.as_ref().map(|v| compact_live_snapshot(v, "15m")),
    }))
}

async fn fetch_latest_snapshot(
    state: &ApiState,
    symbol: &str,
    timeframe: &str,
) -> Result<Option<Value>, ApiError> {
    let now_ms = Utc::now().timestamp_millis();
    if state.redis_client.is_none() {
        let row = fetch_latest_snapshot_from_clickhouse(state, symbol, timeframe).await?;
        return Ok(row.filter(|v| is_live_snapshot_fresh(v, timeframe, now_ms)));
    }

    let mut fallback_direct: Option<Value> = None;
    let key_direct = format!(
        "{}:snapshot:latest:{}:{}",
        state.redis_prefix,
        symbol.to_ascii_uppercase(),
        timeframe
    );
    if let Some(v) = read_key_value(state, &key_direct).await? {
        if is_live_snapshot_fresh(&v, timeframe, now_ms) {
            if snapshot_remaining_ms(&v) > 0 {
                return Ok(Some(v));
            }
            fallback_direct = Some(v);
        }
    }

    let tf_key = format!("{}:snapshot:latest:tf:{}", state.redis_prefix, timeframe);
    let Some(arr_val) = read_key_value(state, &tf_key).await? else {
        if fallback_direct.is_some() {
            return Ok(fallback_direct);
        }
        return Ok(None);
    };
    let Some(arr) = arr_val.as_array() else {
        if fallback_direct.is_some() {
            return Ok(fallback_direct);
        }
        return Ok(None);
    };

    let mut best: Option<Value> = None;
    let mut best_priority: Option<(i64, i64, i64)> = None;
    for row in arr {
        let row_symbol = row
            .get("symbol")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_uppercase();
        let row_tf = row
            .get("timeframe")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        if row_symbol != symbol.to_ascii_uppercase() || row_tf != timeframe {
            continue;
        }
        if !is_live_snapshot_fresh(row, timeframe, now_ms) {
            continue;
        }
        let pri = snapshot_priority(row, timeframe, now_ms);
        if best_priority.map(|v| pri > v).unwrap_or(true) {
            best_priority = Some(pri);
            best = Some(row.clone());
        }
    }

    if best.is_some() {
        return Ok(best);
    }

    if fallback_direct.is_some() {
        return Ok(fallback_direct);
    }

    let row = fetch_latest_snapshot_from_clickhouse(state, symbol, timeframe).await?;
    Ok(row.filter(|v| is_live_snapshot_fresh(v, timeframe, now_ms)))
}

async fn fetch_latest_snapshot_from_clickhouse(
    state: &ApiState,
    symbol: &str,
    timeframe: &str,
) -> Result<Option<Value>, ApiError> {
    let Some(ch_url) = state.ch_url.as_deref() else {
        return Ok(None);
    };
    let query = format!(
        "SELECT
            schema_version,
            ts_ireland_sample_ms,
            symbol,
            timeframe,
            market_id,
            round_id,
            title,
            target_price,
            binance_price,
            pm_live_btc_price,
            chainlink_price,
            mid_yes,
            mid_no,
            mid_yes_smooth,
            mid_no_smooth,
            bid_yes,
            ask_yes,
            bid_no,
            ask_no,
            delta_price,
            delta_pct,
            delta_pct_smooth,
            remaining_ms,
            velocity_bps_per_sec,
            acceleration
        FROM polyedge_forge.snapshot_100ms
        WHERE symbol='{}' AND timeframe='{}'
        ORDER BY (remaining_ms > 0) DESC, ts_ireland_sample_ms DESC
        LIMIT 1
        FORMAT JSON",
        symbol.to_ascii_uppercase(),
        timeframe
    );
    let rows = rows_from_json(query_clickhouse_json(ch_url, &query).await?);
    Ok(rows.into_iter().next())
}

fn compact_live_snapshot(snapshot: &Value, market_type: &str) -> Value {
    let round_id = snapshot
        .get("round_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let start_time_ms = parse_round_start_ms(round_id).unwrap_or(0);
    let end_time_ms = if start_time_ms > 0 {
        start_time_ms.saturating_add(market_type_to_ms(market_type))
    } else {
        0
    };
    let slug = if start_time_ms > 0 {
        format!("btc-updown-{market_type}-{}", start_time_ms / 1000)
    } else {
        String::new()
    };

    let mid_yes_raw = snapshot.get("mid_yes").and_then(Value::as_f64);
    let mid_no_raw = snapshot.get("mid_no").and_then(Value::as_f64);
    let mid_yes_smooth = snapshot.get("mid_yes_smooth").and_then(Value::as_f64);
    let mid_no_smooth = snapshot.get("mid_no_smooth").and_then(Value::as_f64);
    // Live display should prefer smoothed mids to avoid transient quote noise.
    let mid_yes = mid_yes_smooth.or(mid_yes_raw);
    let mid_no = mid_no_smooth.or(mid_no_raw);
    let raw_bid_yes = snapshot.get("bid_yes").and_then(Value::as_f64);
    let raw_ask_yes = snapshot.get("ask_yes").and_then(Value::as_f64);
    let raw_bid_no = snapshot.get("bid_no").and_then(Value::as_f64);
    let raw_ask_no = snapshot.get("ask_no").and_then(Value::as_f64);
    let yes_spread = match (raw_bid_yes, raw_ask_yes) {
        (Some(b), Some(a)) if a.is_finite() && b.is_finite() => (a - b).abs().clamp(0.001, 0.06),
        _ => 0.01,
    };
    let no_spread = match (raw_bid_no, raw_ask_no) {
        (Some(b), Some(a)) if a.is_finite() && b.is_finite() => (a - b).abs().clamp(0.001, 0.06),
        _ => 0.01,
    };
    let best_bid_up =
        raw_bid_yes.or_else(|| mid_yes.map(|m| (m - yes_spread * 0.5).clamp(0.0, 1.0)));
    let best_ask_up =
        raw_ask_yes.or_else(|| mid_yes.map(|m| (m + yes_spread * 0.5).clamp(0.0, 1.0)));
    let best_bid_down =
        raw_bid_no.or_else(|| mid_no.map(|m| (m - no_spread * 0.5).clamp(0.0, 1.0)));
    let best_ask_down =
        raw_ask_no.or_else(|| mid_no.map(|m| (m + no_spread * 0.5).clamp(0.0, 1.0)));

    json!({
        "timestamp_ms": snapshot.get("ts_ireland_sample_ms").and_then(Value::as_i64),
        "round_id": round_id,
        "market_type": market_type,
        "btc_price": snapshot.get("binance_price").and_then(Value::as_f64),
        "target_price": snapshot.get("target_price").and_then(Value::as_f64),
        "mid_yes": mid_yes_raw,
        "mid_no": mid_no_raw,
        "mid_yes_smooth": mid_yes_smooth,
        "mid_no_smooth": mid_no_smooth,
        "delta_pct": snapshot.get("delta_pct_smooth").and_then(Value::as_f64).or_else(|| snapshot.get("delta_pct").and_then(Value::as_f64)),
        "delta_pct_smooth": snapshot.get("delta_pct_smooth").and_then(Value::as_f64),
        "best_bid_up": best_bid_up,
        "best_ask_up": best_ask_up,
        "best_bid_down": best_bid_down,
        "best_ask_down": best_ask_down,
        "time_remaining_s": snapshot.get("remaining_ms").and_then(Value::as_i64).map(|v| v as f64 / 1000.0),
        "velocity_bps_per_sec": snapshot.get("velocity_bps_per_sec").and_then(Value::as_f64),
        "acceleration": snapshot.get("acceleration").and_then(Value::as_f64),
        "outcome": Value::Null,
        "slug": slug,
        "start_time_ms": start_time_ms,
        "end_time_ms": end_time_ms,
        "round_outcome": Value::Null,
    })
}

async fn latest_all(State(state): State<ApiState>) -> Result<Json<Value>, ApiError> {
    let now_ms = Utc::now().timestamp_millis();
    if let Some(v) = read_key_value(
        &state,
        &format!("{}:snapshot:latest:all", state.redis_prefix),
    )
    .await?
    {
        if let Some(arr) = v.as_array() {
            let filtered = arr
                .iter()
                .filter(|row| {
                    row.get("symbol")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .eq_ignore_ascii_case("BTCUSDT")
                })
                .filter_map(|row| {
                    let tf = row.get("timeframe").and_then(Value::as_str)?;
                    if is_live_snapshot_fresh(row, tf, now_ms) {
                        Some(row.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            return Ok(Json(Value::Array(filtered)));
        }
    }

    let mut rows = Vec::<Value>::new();
    if let Some(v) = fetch_latest_snapshot(&state, "BTCUSDT", "5m").await? {
        rows.push(v);
    }
    if let Some(v) = fetch_latest_snapshot(&state, "BTCUSDT", "15m").await? {
        rows.push(v);
    }
    Ok(Json(Value::Array(rows)))
}

async fn latest_timeframe(
    State(state): State<ApiState>,
    AxumPath(timeframe): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    if timeframe.trim().is_empty() {
        return Err(ApiError::bad_request("empty timeframe"));
    }
    let key = format!("{}:snapshot:latest:tf:{}", state.redis_prefix, timeframe);
    read_key_json(&state, &key).await
}

async fn latest_symbol_tf(
    State(state): State<ApiState>,
    AxumPath((symbol, timeframe)): AxumPath<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    if symbol.trim().is_empty() || timeframe.trim().is_empty() {
        return Err(ApiError::bad_request("empty symbol/timeframe"));
    }
    let key = format!(
        "{}:snapshot:latest:{}:{}",
        state.redis_prefix,
        symbol.to_ascii_uppercase(),
        timeframe
    );
    read_key_json(&state, &key).await
}

async fn latest_market(
    State(state): State<ApiState>,
    AxumPath((symbol, timeframe, market_id)): AxumPath<(String, String, String)>,
) -> Result<Json<Value>, ApiError> {
    if symbol.trim().is_empty() || timeframe.trim().is_empty() || market_id.trim().is_empty() {
        return Err(ApiError::bad_request("empty symbol/timeframe/market_id"));
    }
    let key = format!(
        "{}:snapshot:latest:{}:{}:{}",
        state.redis_prefix,
        symbol.to_ascii_uppercase(),
        timeframe,
        market_id
    );
    read_key_json(&state, &key).await
}

async fn history_symbol_timeframe(
    State(state): State<ApiState>,
    AxumPath((symbol_raw, timeframe_raw)): AxumPath<(String, String)>,
    Query(params): Query<HistoryQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let symbol = symbol_raw.to_ascii_uppercase();
    if !is_safe_identifier(&symbol) {
        return Err(ApiError::bad_request("invalid symbol"));
    }

    let timeframe = timeframe_raw.to_ascii_lowercase();
    if !is_valid_timeframe(&timeframe) {
        return Err(ApiError::bad_request("invalid timeframe"));
    }

    let lookback_minutes = params
        .lookback_minutes
        .unwrap_or_else(|| default_lookback_minutes(&timeframe))
        .clamp(1, 24 * 60);
    let limit = params.limit.unwrap_or(30_000).clamp(100, 200_000);

    let to_ms = Utc::now().timestamp_millis();
    let from_ms = to_ms.saturating_sub(i64::from(lookback_minutes) * 60_000);

    let Some(ch_url) = state.ch_url.as_deref() else {
        return Err(ApiError::internal("clickhouse not configured"));
    };

    let timeframe_predicate = if timeframe == "all" {
        "timeframe IN ('5m','15m','30m','1h','2h','4h')".to_string()
    } else {
        format!("timeframe='{}'", timeframe)
    };

    let sample_query = format!(
        "SELECT
            round_id,
            market_id,
            symbol,
            timeframe,
            title,
            ts_ireland_sample_ms,
            target_price,
            binance_price,
            pm_live_btc_price,
            mid_yes,
            mid_no,
            mid_yes_smooth,
            mid_no_smooth,
            bid_yes,
            ask_yes,
            bid_no,
            ask_no,
            delta_price,
            delta_pct,
            delta_pct_smooth,
            remaining_ms,
            velocity_bps_per_sec,
            acceleration
        FROM polyedge_forge.snapshot_100ms
        WHERE symbol='{symbol}'
          AND {timeframe_predicate}
          AND ts_ireland_sample_ms >= {from_ms}
        ORDER BY ts_ireland_sample_ms ASC
        LIMIT {limit}
        FORMAT JSON"
    );
    let round_query = format!(
        "SELECT
            round_id,
            market_id,
            symbol,
            timeframe,
            title,
            start_ts_ms,
            end_ts_ms,
            target_price,
            settle_price,
            toInt8(settle_price > target_price) AS label_up,
            ts_recorded_ms
        FROM polyedge_forge.rounds
        WHERE symbol='{symbol}'
          AND {timeframe_predicate}
          AND end_ts_ms >= {from_ms}
        ORDER BY end_ts_ms ASC
        LIMIT 1000
        FORMAT JSON"
    );

    let sample_json = query_clickhouse_json(ch_url, &sample_query).await?;
    let round_json = query_clickhouse_json(ch_url, &round_query).await?;

    let sample_rows = sample_json
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let round_rows = round_json
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    Ok(Json(json!({
        "symbol": symbol,
        "timeframe": timeframe,
        "from_ms": from_ms,
        "to_ms": to_ms,
        "lookback_minutes": lookback_minutes,
        "limit": limit,
        "sample_count": sample_rows.len(),
        "round_count": round_rows.len(),
        "samples": sample_rows,
        "rounds": round_rows,
    })))
}

async fn stats(State(state): State<ApiState>) -> Result<Json<Value>, ApiError> {
    let Some(ch_url) = state.ch_url.as_deref() else {
        return Err(ApiError::internal("clickhouse not configured"));
    };

    let sample_query = "SELECT
            count() AS total_samples,
            min(ts_ireland_sample_ms) AS first_sample_ms,
            max(ts_ireland_sample_ms) AS last_sample_ms
        FROM polyedge_forge.snapshot_100ms
        WHERE symbol='BTCUSDT' AND timeframe IN ('5m','15m')
        FORMAT JSON";
    let round_query = "SELECT
            count() AS total_rounds,
            sum(toUInt8(settle_price > target_price)) AS up_count,
            count() - sum(toUInt8(settle_price > target_price)) AS down_count
        FROM polyedge_forge.rounds
        WHERE symbol='BTCUSDT' AND timeframe IN ('5m','15m')
        FORMAT JSON";
    let accuracy_query = "SELECT
            countIf(isNotNull(s.eval_mid_up)) AS market_accuracy_n,
            avgIf(
                toFloat64((s.eval_mid_up >= 0.5) = (r.settle_price > r.target_price)),
                isNotNull(s.eval_mid_up)
            ) AS market_accuracy
        FROM polyedge_forge.rounds r
        LEFT JOIN (
            SELECT
                round_id,
                argMinIf(coalesce(mid_yes, mid_yes_smooth), abs(remaining_ms - if(timeframe='5m', 60000, 180000)), remaining_ms >= 0) AS eval_mid_up
            FROM polyedge_forge.snapshot_100ms
            WHERE symbol='BTCUSDT' AND timeframe IN ('5m','15m')
            GROUP BY round_id
        ) s ON r.round_id = s.round_id
        WHERE r.symbol='BTCUSDT' AND r.timeframe IN ('5m','15m')
        FORMAT JSON";

    let sample_row = rows_from_json(query_clickhouse_json(ch_url, sample_query).await?)
        .into_iter()
        .next()
        .unwrap_or_else(|| json!({}));
    let round_row = rows_from_json(query_clickhouse_json(ch_url, round_query).await?)
        .into_iter()
        .next()
        .unwrap_or_else(|| json!({}));
    let accuracy_row = rows_from_json(query_clickhouse_json(ch_url, accuracy_query).await?)
        .into_iter()
        .next()
        .unwrap_or_else(|| json!({}));

    let total_samples = row_i64(&sample_row, "total_samples").unwrap_or(0).max(0);
    let first_sample_ms = row_i64(&sample_row, "first_sample_ms");
    let last_sample_ms = row_i64(&sample_row, "last_sample_ms");
    let uptime_hours = match (first_sample_ms, last_sample_ms) {
        (Some(first), Some(last)) if first > 0 && last >= first => {
            (last - first) as f64 / 3_600_000.0
        }
        _ => 0.0,
    };

    Ok(Json(json!({
        "total_rounds": row_i64(&round_row, "total_rounds").unwrap_or(0).max(0),
        "total_samples": total_samples,
        "up_count": row_i64(&round_row, "up_count").unwrap_or(0).max(0),
        "down_count": row_i64(&round_row, "down_count").unwrap_or(0).max(0),
        "first_sample_ms": first_sample_ms,
        "last_sample_ms": last_sample_ms,
        "uptime_hours": uptime_hours,
        "market_accuracy": row_f64(&accuracy_row, "market_accuracy"),
        "market_accuracy_n": row_i64(&accuracy_row, "market_accuracy_n").unwrap_or(0).max(0),
    })))
}

async fn collector_status(State(state): State<ApiState>) -> Result<Json<Value>, ApiError> {
    let now_ms = Utc::now().timestamp_millis();
    let mut tf_map = serde_json::Map::new();
    let mut overall_ok = true;

    for tf in ["5m", "15m"] {
        let snapshot = match fetch_latest_snapshot(&state, "BTCUSDT", tf).await? {
            Some(v) => Some(v),
            None => fetch_latest_snapshot_from_clickhouse(&state, "BTCUSDT", tf).await?,
        };

        let status_value = if let Some(row) = snapshot {
            let ts_ms = row
                .get("ts_ireland_sample_ms")
                .or_else(|| row.get("timestamp_ms"))
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let age_ms = if ts_ms > 0 {
                now_ms.saturating_sub(ts_ms)
            } else {
                i64::MAX
            };
            let remaining_ms = snapshot_remaining_ms(&row);
            let status = if ts_ms <= 0 {
                overall_ok = false;
                "missing"
            } else if age_ms <= LIVE_SNAPSHOT_MAX_AGE_MS {
                "ok"
            } else if age_ms <= LIVE_SNAPSHOT_MAX_AGE_MS * 3 {
                overall_ok = false;
                "lagging"
            } else {
                overall_ok = false;
                "stalled"
            };
            json!({
                "status": status,
                "timestamp_ms": if ts_ms > 0 { Some(ts_ms) } else { None::<i64> },
                "age_ms": if ts_ms > 0 { Some(age_ms) } else { None::<i64> },
                "remaining_ms": remaining_ms,
                "round_id": row.get("round_id").and_then(Value::as_str).unwrap_or_default(),
            })
        } else {
            overall_ok = false;
            json!({
                "status": "missing",
                "timestamp_ms": Value::Null,
                "age_ms": Value::Null,
                "remaining_ms": Value::Null,
                "round_id": "",
            })
        };
        tf_map.insert(tf.to_string(), status_value);
    }

    Ok(Json(json!({
        "ok": overall_ok,
        "ts_ms": now_ms,
        "timeframes": Value::Object(tf_map),
    })))
}

async fn chart(
    State(state): State<ApiState>,
    Query(params): Query<ChartQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let market_type = normalize_market_type(&params.market_type)
        .ok_or_else(|| ApiError::bad_request("invalid market_type"))?;
    let minutes = params.minutes.unwrap_or(30).min(7 * 24 * 60);
    let max_points = params.max_points.unwrap_or(1500).clamp(200, 20_000) as usize;
    let cache_key = format!("{}:{}:{}", market_type, minutes, max_points);
    if let Some(cached) = state.chart_cache_get(&cache_key).await {
        return Ok(Json(cached));
    }
    let raw_limit = (max_points.saturating_mul(12)).clamp(6_000, 120_000);
    let round_limit = {
        let tf_minutes = if market_type == "5m" { 5usize } else { 15usize };
        let round_span_minutes = if minutes == 0 {
            2 * 24 * 60
        } else {
            minutes as usize
        };
        (round_span_minutes / tf_minutes + 128).clamp(300, 4000)
    };

    let Some(ch_url) = state.ch_url.as_deref() else {
        return Err(ApiError::internal("clickhouse not configured"));
    };

    let now_ms = Utc::now().timestamp_millis();
    let from_clause = if minutes == 0 {
        String::new()
    } else {
        let from_ms = now_ms.saturating_sub(i64::from(minutes) * 60_000);
        format!("AND ts_ireland_sample_ms >= {}", from_ms)
    };

    let point_query = format!(
        "SELECT *
        FROM (
            SELECT
            ts_ireland_sample_ms AS timestamp_ms,
            delta_pct,
            delta_pct_smooth,
            mid_yes,
            mid_no,
            mid_yes_smooth,
            mid_no_smooth,
            bid_yes AS best_bid_up,
            ask_yes AS best_ask_up,
            bid_no AS best_bid_down,
            ask_no AS best_ask_down,
            round_id,
            remaining_ms / 1000.0 AS time_remaining_s,
            binance_price AS btc_price,
            target_price
        FROM polyedge_forge.snapshot_100ms
        WHERE symbol='BTCUSDT'
          AND timeframe='{market_type}'
          {from_clause}
        ORDER BY ts_ireland_sample_ms DESC
        LIMIT {raw_limit}
        )
        ORDER BY timestamp_ms ASC
        FORMAT JSON"
    );

    let round_query = format!(
        "SELECT *
        FROM (
            SELECT
            round_id,
            start_ts_ms,
            end_ts_ms,
            target_price,
            toInt8(settle_price > target_price) AS outcome
        FROM polyedge_forge.rounds
        WHERE symbol='BTCUSDT'
          AND timeframe='{market_type}'
          {}
        ORDER BY end_ts_ms DESC
        LIMIT {round_limit}
        )
        ORDER BY end_ts_ms ASC
        FORMAT JSON",
        if minutes == 0 {
            "".to_string()
        } else {
            format!(
                "AND end_ts_ms >= {}",
                now_ms.saturating_sub(i64::from(minutes) * 60_000)
            )
        }
    );

    let mut points = rows_from_json(query_clickhouse_json(ch_url, &point_query).await?);
    let rounds = rows_from_json(query_clickhouse_json(ch_url, &round_query).await?);

    let total_samples = points.len();
    let step = if total_samples <= max_points {
        1usize
    } else {
        ((total_samples as f64) / (max_points as f64)).ceil() as usize
    };

    if step > 1 {
        points = stride_downsample(points, step);
    }

    let payload = json!({
        "points": points,
        "rounds": rounds,
        "total_samples": total_samples,
        "downsampled": step > 1,
        "step": step,
    });
    state.chart_cache_put(cache_key, payload.clone()).await;
    Ok(Json(payload))
}

async fn chart_round(
    State(state): State<ApiState>,
    Query(params): Query<RoundChartQueryParams>,
) -> Result<Json<Value>, ApiError> {
    if !is_safe_round_id(&params.round_id) {
        return Err(ApiError::bad_request("invalid round_id"));
    }

    let market_type = if let Some(mt) = params.market_type.as_deref() {
        normalize_market_type(mt).ok_or_else(|| ApiError::bad_request("invalid market_type"))?
    } else {
        infer_market_type_from_round_id(&params.round_id)
            .ok_or_else(|| ApiError::bad_request("market_type required"))?
    };

    let max_points = params.max_points.unwrap_or(100_000).clamp(200, 300_000) as usize;
    let Some(ch_url) = state.ch_url.as_deref() else {
        return Err(ApiError::internal("clickhouse not configured"));
    };

    let point_query = format!(
        "SELECT
            ts_ireland_sample_ms AS timestamp_ms,
            delta_pct,
            delta_pct_smooth,
            mid_yes,
            mid_no,
            mid_yes_smooth,
            mid_no_smooth,
            bid_yes AS best_bid_up,
            ask_yes AS best_ask_up,
            bid_no AS best_bid_down,
            ask_no AS best_ask_down,
            round_id,
            remaining_ms / 1000.0 AS time_remaining_s,
            binance_price AS btc_price,
            target_price
        FROM polyedge_forge.snapshot_100ms
        WHERE symbol='BTCUSDT'
          AND timeframe='{market_type}'
          AND round_id='{}'
        ORDER BY ts_ireland_sample_ms ASC
        LIMIT 500000
        FORMAT JSON",
        params.round_id
    );

    let round_query = format!(
        "SELECT
            round_id,
            timeframe AS market_type,
            market_id,
            start_ts_ms AS start_time_ms,
            end_ts_ms AS end_time_ms,
            target_price,
            toInt8(settle_price > target_price) AS outcome
        FROM polyedge_forge.rounds
        WHERE symbol='BTCUSDT'
          AND timeframe='{market_type}'
          AND round_id='{}'
        ORDER BY end_ts_ms DESC
        LIMIT 1
        FORMAT JSON",
        params.round_id
    );

    let mut points = rows_from_json(query_clickhouse_json(ch_url, &point_query).await?);
    let total_samples = points.len();
    if total_samples == 0 {
        return Ok(Json(
            json!({"points":[],"round":Value::Null,"total_samples":0,"step":1}),
        ));
    }

    let step = if total_samples <= max_points {
        1usize
    } else {
        ((total_samples as f64) / (max_points as f64)).ceil() as usize
    };
    if step > 1 {
        points = stride_downsample(points, step);
    }

    let round_row_opt = rows_from_json(query_clickhouse_json(ch_url, &round_query).await?)
        .into_iter()
        .next();
    let round_obj = if let Some(mut r) = round_row_opt {
        let start_ms = row_i64(&r, "start_time_ms").unwrap_or(0);
        let slug = if start_ms > 0 {
            format!("btc-updown-{market_type}-{}", start_ms / 1000)
        } else {
            String::new()
        };
        if let Some(map) = r.as_object_mut() {
            map.insert("slug".to_string(), Value::String(slug));
            map.insert("up_token_id".to_string(), Value::Null);
            map.insert("down_token_id".to_string(), Value::Null);
        }
        r
    } else {
        let start_ms = parse_round_start_ms(&params.round_id).unwrap_or(0);
        let end_ms = if start_ms > 0 {
            start_ms.saturating_add(market_type_to_ms(market_type))
        } else {
            0
        };
        json!({
            "round_id": params.round_id,
            "market_type": market_type,
            "slug": if start_ms > 0 { format!("btc-updown-{market_type}-{}", start_ms / 1000) } else { String::new() },
            "start_time_ms": start_ms,
            "end_time_ms": end_ms,
            "target_price": points.first().and_then(|v| v.get("target_price")).cloned().unwrap_or(Value::Null),
            "outcome": Value::Null,
            "up_token_id": Value::Null,
            "down_token_id": Value::Null,
        })
    };

    Ok(Json(json!({
        "points": points,
        "round": round_obj,
        "total_samples": total_samples,
        "step": step,
    })))
}

async fn read_key_value(state: &ApiState, key: &str) -> Result<Option<Value>, ApiError> {
    let Some(client) = state.redis_client.as_ref() else {
        return Ok(None);
    };

    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| ApiError::internal(format!("redis connect failed: {}", e)))?;

    let payload: Option<String> = conn
        .get(key)
        .await
        .map_err(|e| ApiError::internal(format!("redis get failed: {}", e)))?;

    let Some(payload) = payload else {
        return Ok(None);
    };

    let parsed: Value = serde_json::from_str(&payload)
        .map_err(|e| ApiError::internal(format!("redis payload json parse failed: {}", e)))?;
    Ok(Some(parsed))
}

async fn write_key_value(
    state: &ApiState,
    key: &str,
    value: &Value,
    ttl_sec: Option<u32>,
) -> Result<(), ApiError> {
    let Some(client) = state.redis_client.as_ref() else {
        return Err(ApiError::internal("redis not configured"));
    };
    let payload = serde_json::to_string(value)
        .map_err(|e| ApiError::internal(format!("redis payload serialize failed: {}", e)))?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| ApiError::internal(format!("redis connect failed: {}", e)))?;
    if let Some(ttl) = ttl_sec {
        let _: () = conn
            .set_ex(key, payload, ttl as u64)
            .await
            .map_err(|e| ApiError::internal(format!("redis set_ex failed: {}", e)))?;
    } else {
        let _: () = conn
            .set(key, payload)
            .await
            .map_err(|e| ApiError::internal(format!("redis set failed: {}", e)))?;
    }
    Ok(())
}

async fn delete_key(state: &ApiState, key: &str) -> Result<(), ApiError> {
    let Some(client) = state.redis_client.as_ref() else {
        return Ok(());
    };
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| ApiError::internal(format!("redis connect failed: {}", e)))?;
    let _: usize = conn
        .del(key)
        .await
        .map_err(|e| ApiError::internal(format!("redis del failed: {}", e)))?;
    Ok(())
}

fn strategy_autotune_key(redis_prefix: &str, market_type: &str) -> String {
    format!("{redis_prefix}:strategy:autotune:{market_type}")
}

fn strategy_autotune_history_key(redis_prefix: &str, market_type: &str) -> String {
    format!("{redis_prefix}:strategy:autotune:{market_type}:history")
}

async fn read_key_json(state: &ApiState, key: &str) -> Result<Json<Value>, ApiError> {
    if state.redis_client.is_none() {
        return Err(ApiError::internal("redis not configured"));
    }
    let parsed = read_key_value(state, key).await?;
    let Some(parsed) = parsed else {
        return Err(ApiError::not_found(format!("key not found: {}", key)));
    };
    Ok(Json(parsed))
}

async fn query_clickhouse_json(ch_url: &str, query: &str) -> Result<Value, ApiError> {
    let resp = reqwest::Client::new()
        .post(ch_url)
        .header(reqwest::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(query.to_string())
        .send()
        .await
        .map_err(|e| ApiError::internal(format!("clickhouse request failed: {}", e)))?;

    let status = resp.status();
    let body = resp
        .text()
        .await
        .map_err(|e| ApiError::internal(format!("clickhouse body read failed: {}", e)))?;
    if !status.is_success() {
        return Err(ApiError::internal(format!(
            "clickhouse query failed status={} body={}",
            status, body
        )));
    }

    serde_json::from_str::<Value>(&body)
        .map_err(|e| ApiError::internal(format!("clickhouse json parse failed: {}", e)))
}

fn is_live_snapshot_fresh(snapshot: &Value, market_type: &str, now_ms: i64) -> bool {
    let ts_ms = snapshot
        .get("ts_ireland_sample_ms")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    if ts_ms <= 0 {
        return false;
    }
    if now_ms.saturating_sub(ts_ms) > LIVE_SNAPSHOT_MAX_AGE_MS {
        return false;
    }

    let remaining_ms = snapshot
        .get("remaining_ms")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    if remaining_ms <= 0 && now_ms.saturating_sub(ts_ms) > LIVE_ROUND_END_GRACE_MS {
        return false;
    }

    let round_id = snapshot
        .get("round_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let start_ms = parse_round_start_ms(round_id).unwrap_or(0);
    if start_ms > 0 {
        let end_ms = start_ms.saturating_add(market_type_to_ms(market_type));
        if now_ms > end_ms.saturating_add(LIVE_ROUND_END_GRACE_MS) {
            return false;
        }
    }

    true
}

fn snapshot_remaining_ms(snapshot: &Value) -> i64 {
    snapshot
        .get("remaining_ms")
        .and_then(Value::as_i64)
        .or_else(|| {
            snapshot
                .get("time_remaining_s")
                .and_then(Value::as_f64)
                .map(|v| (v * 1000.0).round() as i64)
        })
        .unwrap_or(0)
}

fn snapshot_priority(snapshot: &Value, market_type: &str, now_ms: i64) -> (i64, i64, i64) {
    let rem_ms = snapshot_remaining_ms(snapshot);
    let active = if rem_ms > 0 {
        1
    } else {
        let rid = snapshot
            .get("round_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let start_ms = parse_round_start_ms(rid).unwrap_or(0);
        if start_ms > 0 {
            let end_ms = start_ms.saturating_add(market_type_to_ms(market_type));
            if now_ms <= end_ms.saturating_add(LIVE_ROUND_END_GRACE_MS) {
                1
            } else {
                0
            }
        } else {
            0
        }
    };
    let ts = snapshot
        .get("ts_ireland_sample_ms")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let start_ms = snapshot
        .get("round_id")
        .and_then(Value::as_str)
        .and_then(parse_round_start_ms)
        .unwrap_or(0);
    (active, ts, start_ms)
}

fn is_safe_identifier(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 32
        && v.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn is_valid_timeframe(tf: &str) -> bool {
    matches!(tf, "5m" | "15m" | "30m" | "1h" | "2h" | "4h" | "all")
}

fn default_lookback_minutes(tf: &str) -> u32 {
    match tf {
        "5m" => 120,
        "15m" => 240,
        "30m" => 360,
        "1h" => 720,
        "2h" => 720,
        "4h" => 1440,
        _ => 360,
    }
}

async fn check_clickhouse(ch_url: Option<&str>) -> ServiceHealth {
    let Some(url) = ch_url else {
        return ServiceHealth {
            enabled: false,
            ok: true,
            latency_ms: None,
            detail: "disabled".to_string(),
        };
    };

    let st = Instant::now();
    let resp = reqwest::Client::new()
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body("SELECT 1")
        .send()
        .await;

    match resp {
        Ok(r) if r.status().is_success() => ServiceHealth {
            enabled: true,
            ok: true,
            latency_ms: Some(st.elapsed().as_millis()),
            detail: "ok".to_string(),
        },
        Ok(r) => ServiceHealth {
            enabled: true,
            ok: false,
            latency_ms: Some(st.elapsed().as_millis()),
            detail: format!("status {}", r.status()),
        },
        Err(e) => ServiceHealth {
            enabled: true,
            ok: false,
            latency_ms: Some(st.elapsed().as_millis()),
            detail: format!("error {}", e),
        },
    }
}

async fn check_redis(client: Option<&redis::Client>) -> ServiceHealth {
    let Some(client) = client else {
        return ServiceHealth {
            enabled: false,
            ok: true,
            latency_ms: None,
            detail: "disabled".to_string(),
        };
    };

    let st = Instant::now();
    let ping = async {
        let mut conn = client.get_multiplexed_async_connection().await?;
        let pong: String = redis::cmd("PING").query_async(&mut conn).await?;
        Result::<String>::Ok(pong)
    }
    .await;

    match ping {
        Ok(v) if v.eq_ignore_ascii_case("PONG") => ServiceHealth {
            enabled: true,
            ok: true,
            latency_ms: Some(st.elapsed().as_millis()),
            detail: "ok".to_string(),
        },
        Ok(v) => ServiceHealth {
            enabled: true,
            ok: false,
            latency_ms: Some(st.elapsed().as_millis()),
            detail: format!("unexpected ping response {}", v),
        },
        Err(e) => ServiceHealth {
            enabled: true,
            ok: false,
            latency_ms: Some(st.elapsed().as_millis()),
            detail: format!("error {}", e),
        },
    }
}

async fn rounds(
    State(state): State<ApiState>,
    Query(params): Query<RoundQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let market_type = normalize_market_type(&params.market_type)
        .ok_or_else(|| ApiError::bad_request("invalid market_type"))?;
    let limit = min(params.limit.unwrap_or(500), 5_000).max(1);

    let Some(ch_url) = state.ch_url.as_deref() else {
        return Err(ApiError::internal("clickhouse not configured"));
    };

    let query = format!(
        "SELECT
            r.round_id,
            r.market_id,
            r.timeframe AS market_type,
            r.start_ts_ms AS start_time_ms,
            r.end_ts_ms AS end_time_ms,
            r.target_price,
            r.settle_price AS final_btc_price,
            toInt8(r.settle_price > r.target_price) AS outcome,
            if(r.target_price > 0, (r.settle_price - r.target_price) / r.target_price * 100.0, NULL) AS delta_pct,
            if(toInt8(r.settle_price > r.target_price) = 1, {resolved_up_cents}, {resolved_down_cents}) AS mkt_price_cents
        FROM polyedge_forge.rounds r
        WHERE r.symbol='BTCUSDT'
          AND r.timeframe='{market_type}'
        ORDER BY end_ts_ms DESC
        LIMIT {limit}
        FORMAT JSON",
        resolved_up_cents = RESOLVED_UP_PRICE_CENTS,
        resolved_down_cents = RESOLVED_DOWN_PRICE_CENTS
    );

    let rows = rows_from_json(query_clickhouse_json(ch_url, &query).await?);
    Ok(Json(json!({
        "market_type": market_type,
        "count": rows.len(),
        "rounds": rows,
    })))
}

async fn rounds_available(
    State(state): State<ApiState>,
    Query(params): Query<AvailableRoundsQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let market_type = normalize_market_type(&params.market_type)
        .ok_or_else(|| ApiError::bad_request("invalid market_type"))?;

    let Some(ch_url) = state.ch_url.as_deref() else {
        return Err(ApiError::internal("clickhouse not configured"));
    };

    let query = format!(
        "SELECT
            round_id,
            market_id,
            start_ts_ms AS start_time_ms,
            end_ts_ms AS end_time_ms
        FROM polyedge_forge.rounds
        WHERE symbol='BTCUSDT'
          AND timeframe='{market_type}'
        ORDER BY start_ts_ms DESC
        LIMIT 5000
        FORMAT JSON"
    );

    let mut rows = rows_from_json(query_clickhouse_json(ch_url, &query).await?);
    rows.sort_by(|a, b| {
        row_i64(b, "start_time_ms")
            .unwrap_or(0)
            .cmp(&row_i64(a, "start_time_ms").unwrap_or(0))
    });

    let mut days_count: HashMap<String, i64> = HashMap::new();
    for row in &mut rows {
        let start_ms = row_i64(row, "start_time_ms").unwrap_or(0);
        let day = DateTime::<Utc>::from_timestamp_millis(start_ms)
            .map(|dt| dt.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| "unknown".to_string());
        if let Some(map) = row.as_object_mut() {
            map.insert("date".to_string(), Value::String(day.clone()));
        }
        *days_count.entry(day).or_insert(0) += 1;
    }

    let mut days = days_count
        .into_iter()
        .map(|(date, round_count)| json!({"date": date, "round_count": round_count}))
        .collect::<Vec<_>>();
    days.sort_by(|a, b| {
        b.get("date")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .cmp(a.get("date").and_then(Value::as_str).unwrap_or_default())
    });

    Ok(Json(json!({
        "market_type": market_type,
        "days": days,
        "rounds": rows,
    })))
}

async fn heatmap(
    State(state): State<ApiState>,
    Query(params): Query<HeatmapQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let market_type = normalize_market_type(&params.market_type)
        .ok_or_else(|| ApiError::bad_request("invalid market_type"))?;
    let lookback_hours = params.lookback_hours.unwrap_or(72).clamp(1, 24 * 30);
    let duration_ms = market_type_to_ms(market_type);
    let from_ms = Utc::now()
        .timestamp_millis()
        .saturating_sub(i64::from(lookback_hours) * 3_600_000);

    let Some(ch_url) = state.ch_url.as_deref() else {
        return Err(ApiError::internal("clickhouse not configured"));
    };

    let query = format!(
        "SELECT
            if(abs(delta_bin_raw) < 0.005, 0.0, delta_bin_raw) AS delta_bin_pct,
            time_left_s_bin,
            avg(avg_up_price_cents_raw) AS avg_up_price_cents,
            sum(sample_count_raw) AS sample_count
        FROM (
            SELECT
                round(round(delta_pct / 0.05) * 0.05, 2) AS delta_bin_raw,
                intDiv(greatest(remaining_ms, 0), 30000) * 30 AS time_left_s_bin,
                coalesce(mid_yes, mid_yes_smooth) * 100.0 AS avg_up_price_cents_raw,
                1 AS sample_count_raw
            FROM polyedge_forge.snapshot_100ms
            WHERE symbol='BTCUSDT'
              AND timeframe='{market_type}'
              AND ts_ireland_sample_ms >= {from_ms}
              AND delta_pct IS NOT NULL
              AND delta_pct >= -5
              AND delta_pct <= 5
              AND remaining_ms >= 0
              AND remaining_ms <= {duration_ms}
        )
        GROUP BY delta_bin_pct, time_left_s_bin
        ORDER BY time_left_s_bin DESC, delta_bin_pct ASC
        FORMAT JSON"
    );

    let mut rows = rows_from_json(query_clickhouse_json(ch_url, &query).await?);
    let max_count = rows
        .iter()
        .filter_map(|v| row_i64(v, "sample_count"))
        .max()
        .unwrap_or(0);
    for row in &mut rows {
        let c = row_i64(row, "sample_count").unwrap_or(0);
        let opacity = if max_count > 0 {
            c as f64 / max_count as f64
        } else {
            0.0
        };
        if let Some(map) = row.as_object_mut() {
            map.insert("opacity".to_string(), json!(opacity));
        }
    }

    Ok(Json(json!({
        "market_type": market_type,
        "lookback_hours": lookback_hours,
        "max_sample_count": max_count,
        "cells": rows,
    })))
}

async fn accuracy_series(
    State(state): State<ApiState>,
    Query(params): Query<AccuracyQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let market_type = normalize_market_type(&params.market_type)
        .ok_or_else(|| ApiError::bad_request("invalid market_type"))?;
    let rolling_window = if market_type == "5m" {
        40usize
    } else {
        20usize
    };
    let lookback_hours = 24u32;
    let now_ms = Utc::now().timestamp_millis();
    let from_ms = now_ms.saturating_sub(i64::from(lookback_hours) * 3_600_000);
    let bucket_ms: i64 = 1_800_000; // 30 minutes
    let _window_override = params.window.unwrap_or(rolling_window as u32);
    let _limit_override = params.limit.unwrap_or(5_000);
    let _lookback_override = params.lookback_hours.unwrap_or(24);
    let eval_remaining_ms = if market_type == "5m" { 60_000 } else { 180_000 };
    let duration_ms = market_type_to_ms(market_type);

    let Some(ch_url) = state.ch_url.as_deref() else {
        return Err(ApiError::internal("clickhouse not configured"));
    };

    let query = format!(
        "SELECT
            r.round_id,
            r.end_ts_ms AS timestamp_ms,
            toInt8(r.settle_price > r.target_price) AS outcome,
            s.eval_mid_up AS eval_mid_up
        FROM polyedge_forge.rounds r
        INNER JOIN (
            SELECT
                round_id,
                argMinIf(coalesce(mid_yes, mid_yes_smooth), abs(remaining_ms - {eval_remaining_ms}), remaining_ms >= 0) AS eval_mid_up
            FROM polyedge_forge.snapshot_100ms
            WHERE symbol='BTCUSDT'
              AND timeframe='{market_type}'
              AND remaining_ms >= 0
              AND remaining_ms <= {duration_ms}
            GROUP BY round_id
        ) s ON r.round_id = s.round_id
        WHERE r.symbol='BTCUSDT'
          AND r.timeframe='{market_type}'
          AND isFinite(s.eval_mid_up)
          AND r.end_ts_ms >= ({from_ms} - {duration_ms} * 4)
        ORDER BY r.end_ts_ms ASC
        FORMAT JSON"
    );

    let rows = rows_from_json(query_clickhouse_json(ch_url, &query).await?);
    let mut correctness_window: std::collections::VecDeque<i64> =
        std::collections::VecDeque::with_capacity(rolling_window + 4);
    let mut correctness_sum: i64 = 0;
    let mut by_bucket: HashMap<i64, (f64, i64, String)> = HashMap::new();
    let mut processed_rounds = 0usize;

    for row in rows {
        let ts = row_i64(&row, "timestamp_ms").unwrap_or(0);
        let outcome = row_i64(&row, "outcome").unwrap_or(0).clamp(0, 1);
        let eval_mid_up = row_f64(&row, "eval_mid_up");
        let Some(eval_mid_up) = eval_mid_up else {
            continue;
        };
        if !eval_mid_up.is_finite() || ts <= 0 {
            continue;
        }
        let correct = if (eval_mid_up >= 0.5) == (outcome == 1) {
            1
        } else {
            0
        };
        correctness_window.push_back(correct);
        correctness_sum += correct;
        while correctness_window.len() > rolling_window {
            if let Some(v) = correctness_window.pop_front() {
                correctness_sum -= v;
            }
        }

        processed_rounds = processed_rounds.saturating_add(1);
        if correctness_window.len() < rolling_window {
            continue;
        }
        let acc = (correctness_sum as f64 / rolling_window as f64) * 100.0;
        if ts < from_ms {
            continue;
        }
        let bucket_ts = (ts / bucket_ms) * bucket_ms;
        let n = rolling_window as i64;
        let rid = row
            .get("round_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        by_bucket.insert(bucket_ts, (acc.clamp(0.0, 100.0), n, rid));
    }

    let start_aligned = (from_ms / bucket_ms) * bucket_ms;
    let end_aligned = ((now_ms + bucket_ms - 1) / bucket_ms) * bucket_ms;
    let mut series: Vec<Value> =
        Vec::with_capacity(((end_aligned - start_aligned) / bucket_ms + 1).max(0) as usize);
    let mut last_acc: Option<f64> = None;
    for ts in (start_aligned..=end_aligned).step_by(bucket_ms as usize) {
        if let Some((acc, n, rid)) = by_bucket.get(&ts) {
            last_acc = Some(*acc);
            series.push(json!({
                "timestamp_ms": ts,
                "round_id": rid,
                "accuracy_pct": *acc,
                "sample_count": *n,
            }));
            continue;
        }
        if let Some(acc) = last_acc {
            series.push(json!({
                "timestamp_ms": ts,
                "round_id": "",
                "accuracy_pct": acc,
                "sample_count": 0,
            }));
        }
    }

    let latest_accuracy = series
        .last()
        .and_then(|v| v.get("accuracy_pct"))
        .and_then(Value::as_f64);

    Ok(Json(json!({
        "market_type": market_type,
        "window": rolling_window,
        "lookback_hours": lookback_hours,
        "bucket_minutes": 30,
        "processed_rounds": processed_rounds,
        "series": series,
        "latest_accuracy_pct": latest_accuracy,
    })))
}

#[derive(Debug, Clone)]
struct StrategySample {
    ts_ms: i64,
    round_id: String,
    remaining_ms: i64,
    p_up: f64,
    delta_pct: f64,
    velocity: f64,
    acceleration: f64,
    bid_yes: f64,
    ask_yes: f64,
    bid_no: f64,
    ask_no: f64,
    spread_up: f64,
    spread_down: f64,
    spread_mid: f64,
}

#[derive(Debug, Clone, Copy)]
struct StrategyRuntimeConfig {
    entry_threshold_base: f64,
    entry_threshold_cap: f64,
    spread_limit_prob: f64,
    entry_edge_prob: f64,
    entry_min_potential_cents: f64,
    entry_max_price_cents: f64,
    min_hold_ms: i64,
    stop_loss_cents: f64,
    reverse_signal_threshold: f64,
    reverse_signal_ticks: usize,
    trail_activate_profit_cents: f64,
    trail_drawdown_cents: f64,
    take_profit_near_max_cents: f64,
    endgame_take_profit_cents: f64,
    endgame_remaining_ms: i64,
    liquidity_widen_prob: f64,
    cooldown_ms: i64,
    max_entries_per_round: usize,
    max_exec_spread_cents: f64,
    slippage_cents_per_side: f64,
    fee_cents_per_side: f64,
    emergency_wide_spread_penalty_ratio: f64,
}

const STRATEGY_BASELINE_PROFILE: &str = "strict_no_autotune_2026_02_26";

#[allow(dead_code)]
fn strategy_backup_baseline_config() -> StrategyRuntimeConfig {
    StrategyRuntimeConfig {
        entry_threshold_base: 0.599103,
        entry_threshold_cap: 0.828918,
        spread_limit_prob: 0.022381,
        entry_edge_prob: 0.03,
        entry_min_potential_cents: 10.32355,
        entry_max_price_cents: 72.915692,
        min_hold_ms: 3_702,
        stop_loss_cents: 4.369163,
        reverse_signal_threshold: -0.6368297565445915,
        reverse_signal_ticks: 1,
        trail_activate_profit_cents: 2.2,
        trail_drawdown_cents: 2.265089,
        take_profit_near_max_cents: 87.74805411830562,
        endgame_take_profit_cents: 86.18788390856207,
        endgame_remaining_ms: 13_670,
        liquidity_widen_prob: 0.08860501062699792,
        cooldown_ms: 0,
        max_entries_per_round: 10,
        max_exec_spread_cents: 1.48597,
        slippage_cents_per_side: 0.06918614011422781,
        fee_cents_per_side: 0.03270800007174326,
        emergency_wide_spread_penalty_ratio: 0.27217322622042583,
    }
}

fn strategy_current_default_config() -> StrategyRuntimeConfig {
    // Revert to previous stable baseline (strict_no_autotune_2026_02_26).
    StrategyRuntimeConfig {
        entry_threshold_base: 0.5974897596832403,
        entry_threshold_cap: 0.8445281582196084,
        spread_limit_prob: 0.023219988756074554,
        entry_edge_prob: 0.03139843811607734,
        entry_min_potential_cents: 10.14705380694935,
        entry_max_price_cents: 75.93741099259525,
        min_hold_ms: 3_475,
        stop_loss_cents: 4.094248266919752,
        reverse_signal_threshold: -0.68824813079899,
        reverse_signal_ticks: 1,
        trail_activate_profit_cents: 2.693031061201522,
        trail_drawdown_cents: 2.791041275329463,
        take_profit_near_max_cents: 89.6868393303587,
        endgame_take_profit_cents: 88.69503839734718,
        endgame_remaining_ms: 17_270,
        liquidity_widen_prob: 0.1108363526508022,
        cooldown_ms: 0,
        max_entries_per_round: 10,
        max_exec_spread_cents: 1.6193923581847693,
        slippage_cents_per_side: 0.07264891978079893,
        fee_cents_per_side: 0.03502784311015152,
        emergency_wide_spread_penalty_ratio: 0.25277914857787637,
    }
}

impl Default for StrategyRuntimeConfig {
    fn default() -> Self {
        strategy_current_default_config()
    }
}


fn parse_strategy_rows(rows: Vec<Value>) -> Vec<StrategySample> {
    let mut out = Vec::<StrategySample>::new();
    for row in rows {
        let ts_ms = row_i64(&row, "ts_ms").unwrap_or(0);
        if ts_ms <= 0 {
            continue;
        }
        let round_id = row
            .get("round_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if round_id.is_empty() {
            continue;
        }

        let raw_bid_yes = row_f64(&row, "bid_yes");
        let raw_ask_yes = row_f64(&row, "ask_yes");
        let raw_bid_no = row_f64(&row, "bid_no");
        let raw_ask_no = row_f64(&row, "ask_no");

        let p_mid_yes = row_f64(&row, "mid_yes_smooth")
            .or_else(|| row_f64(&row, "mid_yes"))
            .or_else(|| match (raw_bid_yes, raw_ask_yes) {
                (Some(b), Some(a)) => Some((a + b) * 0.5),
                (Some(v), None) | (None, Some(v)) => Some(v),
                _ => None,
            });

        let mut p_up = p_mid_yes.unwrap_or(0.5);
        if !p_up.is_finite() {
            p_up = 0.5;
        }
        p_up = p_up.clamp(0.0, 1.0);

        let p_no_mid = row_f64(&row, "mid_no_smooth")
            .or_else(|| row_f64(&row, "mid_no"))
            .unwrap_or((1.0 - p_up).clamp(0.0, 1.0));

        let raw_spread_up = match (raw_bid_yes, raw_ask_yes) {
            (Some(b), Some(a)) if a.is_finite() && b.is_finite() => (a - b).abs(),
            _ => 0.012,
        };
        let raw_spread_down = match (raw_bid_no, raw_ask_no) {
            (Some(b), Some(a)) if a.is_finite() && b.is_finite() => (a - b).abs(),
            _ => 0.012,
        };
        let spread_up = raw_spread_up.clamp(0.003, 0.04);
        let spread_down = raw_spread_down.clamp(0.003, 0.04);
        // Use a synthetic tradable band around smoothed mids to suppress occasional bad top-of-book spikes.
        let mut by = (p_up - spread_up * 0.5).clamp(0.0, 1.0);
        let mut ay = (p_up + spread_up * 0.5).clamp(0.0, 1.0);
        if by > ay {
            std::mem::swap(&mut by, &mut ay);
        }
        let mut bn = (p_no_mid - spread_down * 0.5).clamp(0.0, 1.0);
        let mut an = (p_no_mid + spread_down * 0.5).clamp(0.0, 1.0);
        if bn > an {
            std::mem::swap(&mut bn, &mut an);
        }

        let spread_mid = ((spread_up + spread_down) * 0.5).clamp(0.001, 0.08);

        let delta_pct = row_f64(&row, "delta_pct_smooth")
            .or_else(|| row_f64(&row, "delta_pct"))
            .or_else(|| {
                let px = row_f64(&row, "binance_price")?;
                let tp = row_f64(&row, "target_price")?;
                if tp > 0.0 {
                    Some(((px - tp) / tp) * 100.0)
                } else {
                    None
                }
            })
            .unwrap_or(0.0);
        let velocity = row_f64(&row, "velocity_bps_per_sec").unwrap_or(0.0);
        let acceleration = row_f64(&row, "acceleration").unwrap_or(0.0);
        let remaining_ms = row_i64(&row, "remaining_ms").unwrap_or(0).max(0);

        if let Some(last) = out.last_mut() {
            if last.round_id == round_id && last.ts_ms / 1000 == ts_ms / 1000 {
                *last = StrategySample {
                    ts_ms,
                    round_id,
                    remaining_ms,
                    p_up,
                    delta_pct,
                    velocity,
                    acceleration,
                    bid_yes: by,
                    ask_yes: ay,
                    bid_no: bn,
                    ask_no: an,
                    spread_up,
                    spread_down,
                    spread_mid,
                };
                continue;
            }
        }

        out.push(StrategySample {
            ts_ms,
            round_id,
            remaining_ms,
            p_up,
            delta_pct,
            velocity,
            acceleration,
            bid_yes: by,
            ask_yes: ay,
            bid_no: bn,
            ask_no: an,
            spread_up,
            spread_down,
            spread_mid,
        });
    }
    out
}

#[derive(Debug, Clone)]
struct StrategySimulationResult {
    current: Value,
    trades: Vec<Value>,
    all_trade_pnls: Vec<f64>,
    trade_count: usize,
    win_rate_pct: f64,
    avg_pnl_cents: f64,
    avg_duration_s: f64,
    total_pnl_cents: f64,
    max_drawdown_cents: f64,
    max_profit_trade_cents: f64,
    blocked_exits: usize,
    emergency_wide_exit_count: usize,
    execution_penalty_cents_total: f64,
    gross_pnl_cents: f64,
    net_pnl_cents: f64,
    total_entry_fee_cents: f64,
    total_exit_fee_cents: f64,
    total_slippage_cents: f64,
    total_cost_cents: f64,
    net_margin_pct: f64,
}

#[derive(Debug, Clone, Copy)]
struct RollingStats {
    window_trades: usize,
    windows: usize,
    latest_win_rate_pct: f64,
    avg_win_rate_pct: f64,
    min_win_rate_pct: f64,
    latest_avg_pnl_cents: f64,
}

fn compute_rolling_stats(pnls: &[f64], window_trades: usize) -> RollingStats {
    let w = window_trades.max(1);
    if pnls.len() < w {
        return RollingStats {
            window_trades: w,
            windows: 0,
            latest_win_rate_pct: 0.0,
            avg_win_rate_pct: 0.0,
            min_win_rate_pct: 0.0,
            latest_avg_pnl_cents: 0.0,
        };
    }

    let mut win_sum = 0usize;
    let mut pnl_sum = 0.0_f64;
    for v in &pnls[..w] {
        if *v > 0.0 {
            win_sum += 1;
        }
        pnl_sum += *v;
    }

    let mut windows = 0usize;
    let mut acc_win_rate = 0.0_f64;
    let mut min_win_rate = 100.0_f64;
    let mut latest_win_rate = 0.0_f64;
    let mut latest_avg_pnl = 0.0_f64;
    for end in (w - 1)..pnls.len() {
        if end >= w {
            let out = pnls[end - w];
            if out > 0.0 {
                win_sum = win_sum.saturating_sub(1);
            }
            pnl_sum -= out;
            let incoming = pnls[end];
            if incoming > 0.0 {
                win_sum += 1;
            }
            pnl_sum += incoming;
        }
        let wr = (win_sum as f64) * 100.0 / w as f64;
        let ap = pnl_sum / w as f64;
        windows += 1;
        acc_win_rate += wr;
        if wr < min_win_rate {
            min_win_rate = wr;
        }
        latest_win_rate = wr;
        latest_avg_pnl = ap;
    }

    RollingStats {
        window_trades: w,
        windows,
        latest_win_rate_pct: latest_win_rate,
        avg_win_rate_pct: if windows > 0 {
            acc_win_rate / windows as f64
        } else {
            0.0
        },
        min_win_rate_pct: if windows > 0 { min_win_rate } else { 0.0 },
        latest_avg_pnl_cents: latest_avg_pnl,
    }
}

fn rolling_stats_json(rs: RollingStats) -> Value {
    json!({
        "window_trades": rs.window_trades,
        "windows": rs.windows,
        "latest_win_rate_pct": rs.latest_win_rate_pct,
        "avg_win_rate_pct": rs.avg_win_rate_pct,
        "min_win_rate_pct": rs.min_win_rate_pct,
        "latest_avg_pnl_cents": rs.latest_avg_pnl_cents,
    })
}

fn run_summary_json(run: &StrategySimulationResult) -> Value {
    json!({
        "trade_count": run.trade_count,
        "win_rate_pct": run.win_rate_pct,
        "avg_pnl_cents": run.avg_pnl_cents,
        "avg_duration_s": run.avg_duration_s,
        "total_pnl_cents": run.total_pnl_cents,
        "net_pnl_cents": run.net_pnl_cents,
        "gross_pnl_cents": run.gross_pnl_cents,
        "total_cost_cents": run.total_cost_cents,
        "total_entry_fee_cents": run.total_entry_fee_cents,
        "total_exit_fee_cents": run.total_exit_fee_cents,
        "total_slippage_cents": run.total_slippage_cents,
        "net_margin_pct": run.net_margin_pct,
        "max_drawdown_cents": run.max_drawdown_cents,
        "max_profit_trade_cents": run.max_profit_trade_cents,
        "blocked_exits": run.blocked_exits,
        "emergency_wide_exit_count": run.emergency_wide_exit_count,
        "execution_penalty_cents_total": run.execution_penalty_cents_total,
    })
}

fn strategy_cfg_json(cfg: &StrategyRuntimeConfig) -> Value {
    json!({
        "entry_threshold_base": cfg.entry_threshold_base,
        "entry_threshold_cap": cfg.entry_threshold_cap,
        "spread_limit_prob": cfg.spread_limit_prob,
        "entry_edge_prob": cfg.entry_edge_prob,
        "entry_min_potential_cents": cfg.entry_min_potential_cents,
        "entry_max_price_cents": cfg.entry_max_price_cents,
        "min_hold_ms": cfg.min_hold_ms,
        "stop_loss_cents": cfg.stop_loss_cents,
        "reverse_signal_threshold": cfg.reverse_signal_threshold,
        "reverse_signal_ticks": cfg.reverse_signal_ticks,
        "trail_activate_profit_cents": cfg.trail_activate_profit_cents,
        "trail_drawdown_cents": cfg.trail_drawdown_cents,
        "take_profit_near_max_cents": cfg.take_profit_near_max_cents,
        "endgame_take_profit_cents": cfg.endgame_take_profit_cents,
        "endgame_remaining_ms": cfg.endgame_remaining_ms,
        "liquidity_widen_prob": cfg.liquidity_widen_prob,
        "cooldown_ms": cfg.cooldown_ms,
        "max_entries_per_round": cfg.max_entries_per_round,
        "max_exec_spread_cents": cfg.max_exec_spread_cents,
        "slippage_cents_per_side": cfg.slippage_cents_per_side,
        "fee_cents_per_side": cfg.fee_cents_per_side,
        "emergency_wide_spread_penalty_ratio": cfg.emergency_wide_spread_penalty_ratio,
    })
}

fn strategy_cfg_from_payload(base: StrategyRuntimeConfig, payload: &Value) -> StrategyRuntimeConfig {
    let mut c = base;
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("entry_threshold_base"))
        .and_then(Value::as_f64)
    {
        c.entry_threshold_base = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("entry_threshold_cap"))
        .and_then(Value::as_f64)
    {
        c.entry_threshold_cap = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("spread_limit_prob"))
        .and_then(Value::as_f64)
    {
        c.spread_limit_prob = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("entry_edge_prob"))
        .and_then(Value::as_f64)
    {
        c.entry_edge_prob = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("entry_min_potential_cents"))
        .and_then(Value::as_f64)
    {
        c.entry_min_potential_cents = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("entry_max_price_cents"))
        .and_then(Value::as_f64)
    {
        c.entry_max_price_cents = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("min_hold_ms"))
        .and_then(Value::as_i64)
    {
        c.min_hold_ms = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("stop_loss_cents"))
        .and_then(Value::as_f64)
    {
        c.stop_loss_cents = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("reverse_signal_threshold"))
        .and_then(Value::as_f64)
    {
        c.reverse_signal_threshold = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("reverse_signal_ticks"))
        .and_then(Value::as_u64)
    {
        c.reverse_signal_ticks = v as usize;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("trail_activate_profit_cents"))
        .and_then(Value::as_f64)
    {
        c.trail_activate_profit_cents = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("trail_drawdown_cents"))
        .and_then(Value::as_f64)
    {
        c.trail_drawdown_cents = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("take_profit_near_max_cents"))
        .and_then(Value::as_f64)
    {
        c.take_profit_near_max_cents = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("endgame_take_profit_cents"))
        .and_then(Value::as_f64)
    {
        c.endgame_take_profit_cents = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("endgame_remaining_ms"))
        .and_then(Value::as_i64)
    {
        c.endgame_remaining_ms = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("liquidity_widen_prob"))
        .and_then(Value::as_f64)
    {
        c.liquidity_widen_prob = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("cooldown_ms"))
        .and_then(Value::as_i64)
    {
        c.cooldown_ms = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("max_entries_per_round"))
        .and_then(Value::as_u64)
    {
        c.max_entries_per_round = v as usize;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("max_exec_spread_cents"))
        .and_then(Value::as_f64)
    {
        c.max_exec_spread_cents = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("slippage_cents_per_side"))
        .and_then(Value::as_f64)
    {
        c.slippage_cents_per_side = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("fee_cents_per_side"))
        .and_then(Value::as_f64)
    {
        c.fee_cents_per_side = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("emergency_wide_spread_penalty_ratio"))
        .and_then(Value::as_f64)
    {
        c.emergency_wide_spread_penalty_ratio = v;
    }
    c
}

async fn strategy_paper_live(
    state: &ApiState,
    market_type: &str,
    full_history: bool,
    lookback_minutes: u32,
    max_points: u32,
    max_trades: usize,
    cfg: &StrategyRuntimeConfig,
    live_execute: bool,
    live_quote_usdc: f64,
    live_max_orders: usize,
    live_entry_only: bool,
) -> Result<Value, ApiError> {
    let exec_gate = fev1::ExecutionGate::from_env();
    if live_execute && !exec_gate.live_enabled {
        return Err(ApiError::bad_request(
            "live execution disabled (set FORGE_FEV1_LIVE_ENABLED=true to enable)",
        ));
    }

    let samples = load_strategy_samples(
        state,
        market_type,
        full_history,
        lookback_minutes,
        max_points,
    )
    .await?;
    if samples.len() < 20 {
        return Err(ApiError::bad_request(
            "not enough samples for live execution",
        ));
    }

    let mapped_samples = map_samples_to_fev1(&samples);
    let mapped_cfg = map_cfg_to_fev1(cfg);
    let run = fev1::simulate(&mapped_samples, &mapped_cfg, max_trades);
    let gateway = fev1::ArmedSimulatedGateway;
    let dual = fev1::simulate_dual(
        &mapped_samples,
        &mapped_cfg,
        max_trades,
        live_quote_usdc.max(0.01),
        &gateway,
    );
    let live_gateway_cfg = LiveGatewayConfig::from_env();
    reconcile_gateway_reports(state, &live_gateway_cfg).await;
    handle_pending_timeouts(state, &live_gateway_cfg).await;
    let live_market = resolve_live_market_target(market_type).await.ok();
    let selected_decisions = select_live_decisions(
        &dual.decisions,
        samples.last().map(|s| s.ts_ms).unwrap_or_default(),
        live_max_orders.max(1),
        live_entry_only,
    );
    let (gated_decisions, skipped_decisions, mut live_position_state) = gate_live_decisions(
        state,
        market_type,
        &selected_decisions,
        live_execute,
    )
    .await;
    let submitted_decisions: Vec<Value> = gated_decisions
        .iter()
        .map(|v| v.decision.clone())
        .collect();

    let live_exec_payload = if live_execute {
        match live_market.as_ref() {
            Some(target) => {
                let execution_orders =
                    execute_live_orders_via_gateway(&live_gateway_cfg, target, &gated_decisions)
                        .await;
                for record in &execution_orders {
                    let decision = record.get("decision").cloned().unwrap_or(Value::Null);
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
                    let round_id = decision
                        .get("round_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let ts_ms = decision
                        .get("ts_ms")
                        .and_then(Value::as_i64)
                        .unwrap_or_else(|| Utc::now().timestamp_millis());
                    let accepted = record
                        .get("accepted")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    let event_reason = if accepted {
                        "submit_accepted_pending_confirm".to_string()
                    } else {
                        record
                            .get("error")
                            .and_then(Value::as_str)
                            .unwrap_or("rejected")
                            .to_string()
                    };
                    if accepted {
                        if action == "enter" {
                            live_position_state.state = "enter_pending".to_string();
                        } else if action == "exit" {
                            live_position_state.state = "exit_pending".to_string();
                        }
                        let order_id = extract_order_id_from_gateway_record(record);
                        if let Some(order_id) = order_id {
                            let pending = LivePendingOrder {
                                market_type: market_type.to_string(),
                                order_id,
                                action: action.clone(),
                                side: side.clone(),
                                round_id: round_id.to_string(),
                                decision_key: record
                                    .get("decision_key")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default()
                                    .to_string(),
                                price_cents: decision
                                    .get("price_cents")
                                    .and_then(Value::as_f64)
                                    .unwrap_or(0.0),
                                quote_size_usdc: decision
                                    .get("quote_size_usdc")
                                    .and_then(Value::as_f64)
                                    .unwrap_or(0.0),
                                submitted_ts_ms: ts_ms,
                                cancel_after_ms: if action == "exit" { 2200 } else { 3600 },
                                retry_count: 0,
                            };
                            state.upsert_pending_order(pending).await;
                        }
                    }
                    live_position_state.last_action = Some(format!("{}_{}", action, side));
                    live_position_state.last_reason = Some(event_reason.clone());
                    live_position_state.updated_ts_ms = Utc::now().timestamp_millis();
                    state
                        .append_live_event(
                            market_type,
                            json!({
                                "decision_key": record.get("decision_key"),
                                "accepted": accepted,
                                "action": action,
                                "side": side,
                                "round_id": round_id,
                                "price_cents": decision.get("price_cents").and_then(Value::as_f64),
                                "quote_size_usdc": decision.get("quote_size_usdc").and_then(Value::as_f64),
                                "reason": event_reason,
                                "gateway_endpoint": record.get("endpoint"),
                                "response": record.get("response"),
                                "error": record.get("error")
                            }),
                        )
                        .await;
                }
                state
                    .put_live_position_state(market_type, live_position_state.clone())
                    .await;
                reconcile_gateway_reports(state, &live_gateway_cfg).await;
                json!({
                    "mode": "real_gateway",
                    "target": {
                        "market_id": target.market_id,
                        "symbol": target.symbol,
                        "timeframe": target.timeframe,
                        "token_id_yes": target.token_id_yes,
                        "token_id_no": target.token_id_no,
                        "end_date": target.end_date
                    },
                    "orders": execution_orders
                })
            }
            None => {
                state
                    .append_live_event(
                        market_type,
                        json!({
                            "accepted": false,
                            "action": "none",
                            "side": "NONE",
                            "reason": "no_live_market_target"
                        }),
                    )
                    .await;
                json!({
                    "mode": "real_gateway",
                    "error": "no_live_market_target",
                    "orders": []
                })
            }
        }
    } else {
        json!({
            "mode": "dry_run",
            "target": live_market.as_ref().map(|target| json!({
                "market_id": target.market_id,
                "symbol": target.symbol,
                "timeframe": target.timeframe,
                "token_id_yes": target.token_id_yes,
                "token_id_no": target.token_id_no,
                "end_date": target.end_date
            })),
            "orders": submitted_decisions.clone()
        })
    };
    let paper_entry_count = dual
        .decisions
        .iter()
        .filter(|d| d.get("action").and_then(Value::as_str).map(|v| v.eq_ignore_ascii_case("enter")).unwrap_or(false))
        .count();
    let paper_exit_count = dual
        .decisions
        .iter()
        .filter(|d| d.get("action").and_then(Value::as_str).map(|v| v.eq_ignore_ascii_case("exit")).unwrap_or(false))
        .count();
    let submitted_entry_count = submitted_decisions
        .iter()
        .filter(|d| d.get("action").and_then(Value::as_str).map(|v| v.eq_ignore_ascii_case("enter")).unwrap_or(false))
        .count();
    let submitted_exit_count = submitted_decisions
        .iter()
        .filter(|d| d.get("action").and_then(Value::as_str).map(|v| v.eq_ignore_ascii_case("exit")).unwrap_or(false))
        .count();
    let (gateway_accept_count_raw, gateway_reject_count_raw) = live_exec_payload
        .get("orders")
        .and_then(Value::as_array)
        .map(|orders| {
            let accepted = orders
                .iter()
                .filter(|o| o.get("accepted").and_then(Value::as_bool).unwrap_or(false))
                .count();
            let rejected = orders.len().saturating_sub(accepted);
            (accepted, rejected)
        })
        .unwrap_or((0, 0));
    let gateway_accept_count = if live_execute { gateway_accept_count_raw } else { 0 };
    let gateway_reject_count = if live_execute { gateway_reject_count_raw } else { 0 };
    let no_live_market_target = live_exec_payload
        .get("error")
        .and_then(Value::as_str)
        .map(|v| v == "no_live_market_target")
        .unwrap_or(false);
    let paper_has_signal = paper_entry_count > 0 || paper_exit_count > 0;
    let submitted_has_signal = submitted_entry_count > 0 || submitted_exit_count > 0;
    let status = if !live_execute {
        "dry_run"
    } else if no_live_market_target {
        "blocked_no_market_target"
    } else if paper_has_signal && !submitted_has_signal {
        "blocked_by_gate_or_state"
    } else if submitted_has_signal && gateway_accept_count == 0 {
        "gateway_rejected_all"
    } else {
        "ok"
    };
    let level = match status {
        "ok" | "dry_run" => "ok",
        "blocked_by_gate_or_state" | "blocked_no_market_target" => "warn",
        _ => "critical",
    };
    if live_execute && !matches!(status, "ok") {
        tracing::warn!(
            market_type = market_type,
            status = status,
            paper_entry_count = paper_entry_count,
            paper_exit_count = paper_exit_count,
            submitted_entry_count = submitted_entry_count,
            submitted_exit_count = submitted_exit_count,
            gateway_accept_count = gateway_accept_count,
            gateway_reject_count = gateway_reject_count,
            skipped_count = skipped_decisions.len(),
            "fev1 paper-live parity mismatch"
        );
    }
    let live_submitted_count = if live_execute {
        submitted_decisions.len()
    } else {
        0
    };
    let live_submitted_entry_count = if live_execute { submitted_entry_count } else { 0 };
    let live_submitted_exit_count = if live_execute { submitted_exit_count } else { 0 };
    let simulated_submitted_count = if live_execute {
        0
    } else {
        submitted_decisions.len()
    };
    let simulated_submitted_entry_count = if live_execute { 0 } else { submitted_entry_count };
    let simulated_submitted_exit_count = if live_execute { 0 } else { submitted_exit_count };

    let parity_check = json!({
        "status": status,
        "level": level,
        "paper": {
            "decision_count": dual.decisions.len(),
            "entry_count": paper_entry_count,
            "exit_count": paper_exit_count
        },
        "live": {
            "submitted_count": live_submitted_count,
            "submitted_entry_count": live_submitted_entry_count,
            "submitted_exit_count": live_submitted_exit_count,
            "accepted_count": gateway_accept_count,
            "rejected_count": gateway_reject_count,
            "skipped_count": skipped_decisions.len(),
            "no_live_market_target": no_live_market_target,
            "simulated_submitted_count": simulated_submitted_count,
            "simulated_submitted_entry_count": simulated_submitted_entry_count,
            "simulated_submitted_exit_count": simulated_submitted_exit_count
        }
    });
    let live_events = state.list_live_events(market_type, 60).await;
    let live_position_state = state.get_live_position_state(market_type).await;
    let decisions_for_response = tail_slice(&dual.decisions, 120);
    let paper_records_for_response = tail_slice(&dual.paper_records, 120);
    let live_records_for_response = tail_slice(&dual.live_records, 120);
    let submitted_for_response = tail_slice(&submitted_decisions, 120);
    let skipped_for_response = tail_slice(&skipped_decisions, 120);
    let trades_for_response = tail_slice(&run.trades, 80);

    Ok(json!({
        "source": "live",
        "execution_target": "live",
        "live_enabled": exec_gate.live_enabled,
        "strategy_engine": "forge_fev1",
        "strategy_alias": "FEV1",
        "engine_version": "v1",
        "executor_mode": "same_signal_dual_executor",
        "live_executor": if live_execute { "clob_gateway" } else { "dry_run" },
        "live_execute": live_execute,
        "market_type": market_type,
        "lookback_minutes": lookback_minutes,
        "full_history": full_history,
        "samples": samples.len(),
        "config_source": "live_gate",
        "baseline_profile": STRATEGY_BASELINE_PROFILE,
        "autotune": Value::Null,
        "config": strategy_cfg_json(cfg),
        "current": run.current,
        "summary": run_summary_json(&map_simulation_result(run.clone())),
        "trades": trades_for_response,
        "live_execution": {
            "summary": dual.summary,
            "decisions": decisions_for_response,
            "paper_records": paper_records_for_response,
            "live_records": live_records_for_response,
            "parity_check": parity_check,
            "gated": {
                "selected_count": selected_decisions.len(),
                "submitted_count": submitted_decisions.len(),
                "skipped_count": skipped_decisions.len(),
                "submitted_decisions": submitted_for_response,
                "skipped_decisions": skipped_for_response
            },
            "gateway": {
                "primary_url": live_gateway_cfg.primary_url,
                "backup_url": live_gateway_cfg.backup_url,
                "timeout_ms": live_gateway_cfg.timeout_ms,
                "entry_slippage_bps": live_gateway_cfg.entry_slippage_bps,
                "exit_slippage_bps": live_gateway_cfg.exit_slippage_bps,
                "min_quote_usdc": live_gateway_cfg.min_quote_usdc
            },
            "execution": live_exec_payload,
            "state_machine": live_position_state,
            "events": live_events
        }
    }))
}

fn tail_slice<T: Clone>(items: &[T], limit: usize) -> Vec<T> {
    let n = limit.max(1);
    if items.len() <= n {
        items.to_vec()
    } else {
        items[items.len() - n..].to_vec()
    }
}

#[derive(Debug, Clone)]
struct LiveGatewayConfig {
    primary_url: String,
    backup_url: Option<String>,
    timeout_ms: u64,
    min_quote_usdc: f64,
    entry_slippage_bps: f64,
    exit_slippage_bps: f64,
}

impl LiveGatewayConfig {
    fn from_env() -> Self {
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
            .clamp(0.5, 1000.0);
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
        Self {
            primary_url,
            backup_url,
            timeout_ms,
            min_quote_usdc,
            entry_slippage_bps,
            exit_slippage_bps,
        }
    }
}

#[derive(Debug, Clone)]
struct LiveMarketTarget {
    market_id: String,
    symbol: String,
    timeframe: String,
    token_id_yes: String,
    token_id_no: String,
    end_date: Option<String>,
}

#[derive(Debug, Clone)]
struct LiveGatedDecision {
    decision: Value,
    decision_key: String,
}

fn live_decision_key(market_type: &str, decision: &Value) -> String {
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
    let ts = decision.get("ts_ms").and_then(Value::as_i64).unwrap_or_default();
    let price_cents = decision
        .get("price_cents")
        .and_then(Value::as_f64)
        .unwrap_or_default();
    format!(
        "{}:{}:{}:{}:{}:{:.4}",
        market_type, action, side, round, ts, price_cents
    )
}

async fn gate_live_decisions(
    state: &ApiState,
    market_type: &str,
    decisions: &[Value],
    mark_attempts: bool,
) -> (Vec<LiveGatedDecision>, Vec<Value>, LivePositionState) {
    let now_ms = Utc::now().timestamp_millis();
    let mut position_state = state.get_live_position_state(market_type).await;
    let mut virtual_side = position_state.side.clone();
    let mut accepted = Vec::<LiveGatedDecision>::new();
    let mut skipped = Vec::<Value>::new();

    for decision in decisions {
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
        if action != "enter" && action != "exit" {
            skipped.push(json!({
                "reason": "invalid_action",
                "decision": decision
            }));
            continue;
        }
        if side != "UP" && side != "DOWN" {
            skipped.push(json!({
                "reason": "invalid_side",
                "decision": decision
            }));
            continue;
        }
        let decision_key = live_decision_key(market_type, decision);
        if mark_attempts && state.should_skip_live_decision(&decision_key, now_ms).await {
            skipped.push(json!({
                "reason": "duplicate_recent",
                "decision_key": decision_key,
                "decision": decision
            }));
            continue;
        }
        let side_match = virtual_side
            .as_deref()
            .map(|s| s.eq_ignore_ascii_case(&side))
            .unwrap_or(false);
        let should_submit = if action == "enter" {
            if virtual_side.is_none() {
                true
            } else {
                skipped.push(json!({
                    "reason": "already_in_position",
                    "state_side": virtual_side,
                    "decision": decision
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
                "decision": decision
            }));
            false
        };
        if !should_submit {
            continue;
        }
        if mark_attempts {
            state
                .mark_live_decision_seen(decision_key.clone(), now_ms)
                .await;
        }
        if action == "enter" {
            virtual_side = Some(side.clone());
        } else {
            virtual_side = None;
        }
        accepted.push(LiveGatedDecision {
            decision: decision.clone(),
            decision_key,
        });
    }

    position_state.updated_ts_ms = now_ms;
    (accepted, skipped, position_state)
}

fn select_live_decisions(
    decisions: &[Value],
    latest_ts_ms: i64,
    max_orders: usize,
    entry_only: bool,
) -> Vec<Value> {
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
            let round_ok = d
                .get("round_id")
                .and_then(Value::as_str)
                .map(|r| r == latest_round)
                .unwrap_or(false);
            let ts_ok = d
                .get("ts_ms")
                .and_then(Value::as_i64)
                .map(|ts| ts >= latest_ts_ms.saturating_sub(90_000))
                .unwrap_or(false);
            let action_ok = if entry_only { action == "enter" } else { true };
            (round_ok || ts_ok) && action_ok
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
            !entry_only || action == "enter"
        }) {
            selected.push(last.clone());
        }
    }
    selected.sort_by_key(|v| v.get("ts_ms").and_then(Value::as_i64).unwrap_or(0));
    if selected.len() > max_orders {
        selected = selected[selected.len() - max_orders..].to_vec();
    }
    selected
}

async fn resolve_live_market_target(market_type: &str) -> Result<LiveMarketTarget, ApiError> {
    let discovery = MarketDiscovery::new(DiscoveryConfig {
        symbols: vec!["BTCUSDT".to_string()],
        market_types: vec!["updown".to_string()],
        timeframes: vec![market_type.to_string()],
        ..DiscoveryConfig::default()
    });
    let now_ms = Utc::now().timestamp_millis();
    let mut markets: Vec<MarketDescriptor> = discovery
        .discover()
        .await
        .map_err(|e| ApiError::internal(format!("market discovery failed: {e}")))?
        .into_iter()
        .filter(|m| {
            m.symbol.eq_ignore_ascii_case("BTCUSDT")
                && m.timeframe
                    .as_deref()
                    .map(|tf| tf.eq_ignore_ascii_case(market_type))
                    .unwrap_or(false)
                && m.token_id_yes.is_some()
                && m.token_id_no.is_some()
        })
        .collect();
    if markets.is_empty() {
        return Err(ApiError::bad_request(format!(
            "no active BTC {market_type} market with token ids"
        )));
    }
    markets.sort_by_key(|m| {
        let end_ms = parse_end_date_ms(m.end_date.as_deref()).unwrap_or(i64::MAX / 4);
        let ended_penalty = if end_ms < now_ms { 1_i64 } else { 0_i64 };
        let distance = end_ms.saturating_sub(now_ms).abs();
        (ended_penalty, distance)
    });
    let best = markets.remove(0);
    Ok(LiveMarketTarget {
        market_id: best.market_id,
        symbol: best.symbol,
        timeframe: best.timeframe.unwrap_or_else(|| market_type.to_string()),
        token_id_yes: best.token_id_yes.unwrap_or_default(),
        token_id_no: best.token_id_no.unwrap_or_default(),
        end_date: best.end_date,
    })
}

fn decision_to_live_payload(
    decision: &Value,
    target: &LiveMarketTarget,
    gateway_cfg: &LiveGatewayConfig,
) -> Option<Value> {
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
    let (gateway_side, token_id, max_slippage_bps) = match (action.as_str(), side.as_str()) {
        ("enter", "UP") => ("buy_yes", target.token_id_yes.as_str(), gateway_cfg.entry_slippage_bps),
        ("exit", "UP") => ("sell_yes", target.token_id_yes.as_str(), gateway_cfg.exit_slippage_bps),
        ("enter", "DOWN") => ("buy_no", target.token_id_no.as_str(), gateway_cfg.entry_slippage_bps),
        ("exit", "DOWN") => ("sell_no", target.token_id_no.as_str(), gateway_cfg.exit_slippage_bps),
        _ => return None,
    };
    let mut price_cents = decision
        .get("price_cents")
        .and_then(Value::as_f64)
        .unwrap_or(50.0);
    if !price_cents.is_finite() {
        price_cents = 50.0;
    }
    price_cents = price_cents.clamp(1.0, 99.0);
    let price = (price_cents / 100.0).clamp(0.01, 0.99);
    let quote_size = decision
        .get("quote_size_usdc")
        .and_then(Value::as_f64)
        .unwrap_or(gateway_cfg.min_quote_usdc)
        .max(gateway_cfg.min_quote_usdc);
    let size = (quote_size / price).max(5.0);
    let ttl_ms = if action == "exit" { 900 } else { 1400 };
    let cache_key = format!(
        "fev1:{}:{}:{}:{:.4}:{:.4}",
        target.market_id, gateway_side, action, price, size
    );
    Some(json!({
        "market_id": target.market_id,
        "token_id": token_id,
        "side": gateway_side,
        "price": price,
        "size": size,
        "tif": "FAK",
        "style": "taker",
        "ttl_ms": ttl_ms,
        "max_slippage_bps": max_slippage_bps,
        "cache_key": cache_key
    }))
}

async fn post_gateway(
    client: &reqwest::Client,
    endpoint: &str,
    path: &str,
    payload: &Value,
) -> Result<Value, String> {
    let url = format!("{}/{}", endpoint.trim_end_matches('/'), path.trim_start_matches('/'));
    let resp = client
        .post(&url)
        .header("content-type", "application/json")
        .json(payload)
        .send()
        .await
        .map_err(|e| format!("http_error:{e}"))?;
    let status = resp.status();
    let body = resp.text().await.map_err(|e| format!("read_error:{e}"))?;
    let parsed = serde_json::from_str::<Value>(&body).unwrap_or_else(|_| json!({"raw": body}));
    if status.is_success() {
        Ok(parsed)
    } else {
        Err(format!("status_{}:{}", status.as_u16(), parsed))
    }
}

async fn get_gateway_reports(
    client: &reqwest::Client,
    endpoint: &str,
    since_seq: i64,
    limit: usize,
) -> Result<Value, String> {
    let url = format!(
        "{}/reports/orders?since_seq={}&limit={}",
        endpoint.trim_end_matches('/'),
        since_seq.max(0),
        limit.clamp(1, 1000)
    );
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("http_error:{e}"))?;
    let status = resp.status();
    let body = resp.text().await.map_err(|e| format!("read_error:{e}"))?;
    let parsed = serde_json::from_str::<Value>(&body).unwrap_or_else(|_| json!({"raw": body}));
    if status.is_success() {
        Ok(parsed)
    } else {
        Err(format!("status_{}:{}", status.as_u16(), parsed))
    }
}

async fn delete_gateway_order(
    client: &reqwest::Client,
    endpoint: &str,
    order_id: &str,
) -> Result<Value, String> {
    let url = format!(
        "{}/orders/{}",
        endpoint.trim_end_matches('/'),
        order_id.trim()
    );
    let resp = client
        .delete(&url)
        .send()
        .await
        .map_err(|e| format!("http_error:{e}"))?;
    let status = resp.status();
    let body = resp.text().await.map_err(|e| format!("read_error:{e}"))?;
    let parsed = serde_json::from_str::<Value>(&body).unwrap_or_else(|_| json!({"raw": body}));
    if status.is_success() {
        Ok(parsed)
    } else {
        Err(format!("status_{}:{}", status.as_u16(), parsed))
    }
}

fn extract_order_id_from_gateway_record(row: &Value) -> Option<String> {
    row.get("response")
        .and_then(|v| v.get("order_id").and_then(Value::as_str))
        .map(str::to_string)
        .or_else(|| {
            row.get("response")
                .and_then(|v| v.get("id").and_then(Value::as_str))
                .map(str::to_string)
        })
        .or_else(|| row.get("order_id").and_then(Value::as_str).map(str::to_string))
}

async fn apply_pending_confirmation(
    state: &ApiState,
    pending: &LivePendingOrder,
    reason: &str,
) {
    let now_ms = Utc::now().timestamp_millis();
    let mut ps = state.get_live_position_state(&pending.market_type).await;
    if pending.action == "enter" {
        ps.state = "in_position".to_string();
        ps.side = Some(pending.side.clone());
        ps.entry_round_id = Some(pending.round_id.clone());
        ps.entry_ts_ms = Some(pending.submitted_ts_ms);
        ps.entry_price_cents = Some(pending.price_cents);
        ps.entry_quote_usdc = Some(pending.quote_size_usdc);
        ps.total_entries = ps.total_entries.saturating_add(1);
    } else if pending.action == "exit" {
        ps.state = "flat".to_string();
        ps.side = None;
        ps.entry_round_id = None;
        ps.entry_ts_ms = None;
        ps.entry_price_cents = None;
        ps.entry_quote_usdc = None;
        ps.total_exits = ps.total_exits.saturating_add(1);
    }
    ps.last_action = Some(format!("{}_{}", pending.action, pending.side));
    ps.last_reason = Some(reason.to_string());
    ps.updated_ts_ms = now_ms;
    state.put_live_position_state(&pending.market_type, ps).await;
}

async fn apply_pending_revert(
    state: &ApiState,
    pending: &LivePendingOrder,
    reason: &str,
) {
    let now_ms = Utc::now().timestamp_millis();
    let mut ps = state.get_live_position_state(&pending.market_type).await;
    if pending.action == "enter" {
        ps.state = "flat".to_string();
        ps.side = None;
        ps.entry_round_id = None;
        ps.entry_ts_ms = None;
        ps.entry_price_cents = None;
        ps.entry_quote_usdc = None;
    } else if pending.action == "exit" {
        ps.state = "in_position".to_string();
        if ps.side.is_none() {
            ps.side = Some(pending.side.clone());
        }
    }
    ps.last_action = Some(format!("{}_{}", pending.action, pending.side));
    ps.last_reason = Some(reason.to_string());
    ps.updated_ts_ms = now_ms;
    state.put_live_position_state(&pending.market_type, ps).await;
}

async fn reconcile_gateway_reports(state: &ApiState, gateway_cfg: &LiveGatewayConfig) {
    let since_seq = state.get_gateway_report_seq().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(gateway_cfg.timeout_ms))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());
    let mut endpoint = gateway_cfg.primary_url.clone();
    let mut response = get_gateway_reports(&client, &endpoint, since_seq, 240).await;
    if response.is_err() {
        if let Some(backup) = gateway_cfg.backup_url.as_deref() {
            endpoint = backup.to_string();
            response = get_gateway_reports(&client, &endpoint, since_seq, 240).await;
        }
    }
    let Ok(payload) = response else {
        return;
    };
    let latest_seq = payload
        .get("latest_seq")
        .and_then(Value::as_i64)
        .unwrap_or(since_seq);
    let events = payload
        .get("events")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for ev in events {
        let event_name = ev
            .get("event")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        let order_id = ev
            .get("order_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if order_id.is_empty() {
            continue;
        }
        let pending = state.remove_pending_order(&order_id).await;
        let Some(pending) = pending else {
            continue;
        };
        let state_text = ev
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        let filled_terminal = matches!(state_text.as_str(), "filled" | "executed" | "matched");
        let canceled_terminal = matches!(
            state_text.as_str(),
            "cancelled" | "canceled" | "rejected" | "expired"
        );
        if event_name == "order_terminal" && filled_terminal {
            apply_pending_confirmation(state, &pending, "order_terminal_filled").await;
        } else if event_name == "order_terminal" && canceled_terminal {
            apply_pending_revert(state, &pending, "order_terminal_canceled").await;
        } else if event_name == "order_timeout_cancelled" {
            apply_pending_revert(state, &pending, "timeout_cancelled").await;
        } else if event_name == "order_timeout_cancel_failed" {
            let mut retry = pending.clone();
            retry.retry_count = retry.retry_count.saturating_add(1);
            state.upsert_pending_order(retry.clone()).await;
            state
                .append_live_event(
                    &retry.market_type,
                    json!({
                        "accepted": false,
                        "action": retry.action,
                        "side": retry.side,
                        "round_id": retry.round_id,
                        "reason": "timeout_cancel_failed_requeued",
                        "order_id": retry.order_id,
                        "gateway_endpoint": endpoint
                    }),
                )
                .await;
            continue;
        } else {
            let retry = pending.clone();
            state.upsert_pending_order(retry.clone()).await;
            state
                .append_live_event(
                    &retry.market_type,
                    json!({
                        "accepted": false,
                        "action": retry.action,
                        "side": retry.side,
                        "round_id": retry.round_id,
                        "reason": format!("gateway_event_ignored:{event_name}"),
                        "order_id": retry.order_id,
                        "gateway_endpoint": endpoint
                    }),
                )
                .await;
            continue;
        }
        state
            .append_live_event(
                &pending.market_type,
                json!({
                    "accepted": true,
                    "action": pending.action,
                    "side": pending.side,
                    "round_id": pending.round_id,
                    "reason": format!("gateway_event:{event_name}"),
                    "order_id": pending.order_id,
                    "gateway_endpoint": endpoint,
                    "event": ev
                }),
            )
            .await;
    }
    state.set_gateway_report_seq(latest_seq).await;
}

async fn handle_pending_timeouts(state: &ApiState, gateway_cfg: &LiveGatewayConfig) {
    let now_ms = Utc::now().timestamp_millis();
    let pending = state.list_pending_orders().await;
    if pending.is_empty() {
        return;
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(gateway_cfg.timeout_ms))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());
    for row in pending {
        let elapsed = now_ms.saturating_sub(row.submitted_ts_ms);
        if elapsed < row.cancel_after_ms.max(500) {
            continue;
        }
        let mut endpoint = gateway_cfg.primary_url.clone();
        let mut cancel_res = delete_gateway_order(&client, &endpoint, &row.order_id).await;
        if cancel_res.is_err() {
            if let Some(backup) = gateway_cfg.backup_url.as_deref() {
                endpoint = backup.to_string();
                cancel_res = delete_gateway_order(&client, &endpoint, &row.order_id).await;
            }
        }
        match cancel_res {
            Ok(v) => {
                let _ = state.remove_pending_order(&row.order_id).await;
                apply_pending_revert(state, &row, "local_timeout_cancel").await;
                state
                    .append_live_event(
                        &row.market_type,
                        json!({
                            "accepted": false,
                            "action": row.action,
                            "side": row.side,
                            "round_id": row.round_id,
                            "reason": "local_timeout_cancelled",
                            "order_id": row.order_id,
                            "gateway_endpoint": endpoint,
                            "response": v
                        }),
                    )
                    .await;
            }
            Err(err) => {
                state
                    .append_live_event(
                        &row.market_type,
                        json!({
                            "accepted": false,
                            "action": row.action,
                            "side": row.side,
                            "round_id": row.round_id,
                            "reason": "local_timeout_cancel_failed",
                            "order_id": row.order_id,
                            "gateway_endpoint": endpoint,
                            "error": err
                        }),
                    )
                    .await;
            }
        }
    }
}

async fn execute_live_orders_via_gateway(
    gateway_cfg: &LiveGatewayConfig,
    target: &LiveMarketTarget,
    decisions: &[LiveGatedDecision],
) -> Vec<Value> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(gateway_cfg.timeout_ms))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());
    let mut out = Vec::<Value>::new();
    for gated in decisions {
        let decision = &gated.decision;
        let Some(payload) = decision_to_live_payload(decision, target, gateway_cfg) else {
            out.push(json!({
                "ok": false,
                "reason": "decision_to_payload_failed",
                "decision_key": gated.decision_key,
                "decision": decision
            }));
            continue;
        };
        let _ = post_gateway(&client, &gateway_cfg.primary_url, "/prebuild_order", &payload).await;
        let mut result = post_gateway(&client, &gateway_cfg.primary_url, "/orders", &payload).await;
        let mut used_endpoint = gateway_cfg.primary_url.clone();
        if result.is_err() {
            if let Some(backup) = gateway_cfg.backup_url.as_deref() {
                let _ = post_gateway(&client, backup, "/prebuild_order", &payload).await;
                result = post_gateway(&client, backup, "/orders", &payload).await;
                used_endpoint = backup.to_string();
            }
        }
        match result {
            Ok(v) => {
                let accepted = v.get("accepted").and_then(Value::as_bool).unwrap_or(false);
                out.push(json!({
                    "ok": accepted,
                    "accepted": accepted,
                    "decision_key": gated.decision_key,
                    "decision": decision,
                    "endpoint": used_endpoint,
                    "request": payload,
                    "response": v
                }));
            }
            Err(err) => {
                out.push(json!({
                    "ok": false,
                    "decision_key": gated.decision_key,
                    "decision": decision,
                    "endpoint": used_endpoint,
                    "request": payload,
                    "error": err
                }));
            }
        }
    }
    out
}

fn profit_factor(pnls: &[f64]) -> f64 {
    let mut gross_win = 0.0_f64;
    let mut gross_loss = 0.0_f64;
    for v in pnls {
        if *v > 0.0 {
            gross_win += *v;
        } else if *v < 0.0 {
            gross_loss += v.abs();
        }
    }
    if gross_loss <= 1e-9 {
        if gross_win > 0.0 {
            return 9.0;
        }
        return 0.0;
    }
    gross_win / gross_loss
}

fn loss_tail_penalty(pnls: &[f64]) -> f64 {
    if pnls.len() < 20 {
        return 0.0;
    }
    let mut sorted = pnls.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let tail_n = (sorted.len() / 10).max(3);
    let tail = &sorted[..tail_n];
    let tail_avg = tail.iter().copied().sum::<f64>() / tail_n as f64;
    tail_avg.abs() * 2.4
}

fn side_loss_penalty(trades: &[Value]) -> f64 {
    let mut up_n = 0usize;
    let mut up_w = 0usize;
    let mut up_p = 0.0_f64;
    let mut dn_n = 0usize;
    let mut dn_w = 0usize;
    for t in trades {
        let side = t.get("side").and_then(Value::as_str).unwrap_or_default();
        let pnl = row_f64(t, "pnl_cents").unwrap_or(0.0);
        if side == "UP" {
            up_n = up_n.saturating_add(1);
            up_p += pnl;
            if pnl > 0.0 {
                up_w = up_w.saturating_add(1);
            }
        } else if side == "DOWN" {
            dn_n = dn_n.saturating_add(1);
            if pnl > 0.0 {
                dn_w = dn_w.saturating_add(1);
            }
        }
    }
    if up_n < 12 || dn_n < 12 {
        return 0.0;
    }
    let up_wr = up_w as f64 * 100.0 / up_n as f64;
    let dn_wr = dn_w as f64 * 100.0 / dn_n as f64;
    let up_avg = up_p / up_n as f64;
    let mut penalty = 0.0_f64;
    if dn_wr > up_wr + 8.0 {
        penalty += (dn_wr - up_wr - 8.0) * 2.1;
    }
    if up_wr < 55.0 {
        penalty += (55.0 - up_wr) * 1.4;
    }
    if up_avg < 0.0 {
        penalty += up_avg.abs() * 22.0;
    }
    penalty
}

fn split_samples_by_round(
    samples: &[StrategySample],
    valid_ratio: f64,
) -> (Vec<StrategySample>, Vec<StrategySample>) {
    if samples.len() < 200 {
        return (samples.to_vec(), Vec::new());
    }
    let mut rounds: Vec<String> = Vec::new();
    let mut seen = HashSet::<String>::new();
    for s in samples {
        if seen.insert(s.round_id.clone()) {
            rounds.push(s.round_id.clone());
        }
    }
    if rounds.len() < 8 {
        return (samples.to_vec(), Vec::new());
    }
    let valid_rounds = ((rounds.len() as f64) * valid_ratio.clamp(0.1, 0.5)).round() as usize;
    let valid_rounds = valid_rounds.clamp(2, rounds.len().saturating_sub(2));
    let split_idx = rounds.len().saturating_sub(valid_rounds);
    let valid_set: HashSet<String> = rounds[split_idx..].iter().cloned().collect();

    let mut train = Vec::<StrategySample>::with_capacity(samples.len());
    let mut valid = Vec::<StrategySample>::with_capacity(samples.len() / 3);
    for s in samples {
        if valid_set.contains(&s.round_id) {
            valid.push(s.clone());
        } else {
            train.push(s.clone());
        }
    }
    if train.len() < 200 || valid.len() < 120 {
        return (samples.to_vec(), Vec::new());
    }
    (train, valid)
}

fn map_sample_to_fev1(s: &StrategySample) -> fev1::Sample {
    fev1::Sample {
        ts_ms: s.ts_ms,
        round_id: s.round_id.clone(),
        remaining_ms: s.remaining_ms,
        p_up: s.p_up,
        delta_pct: s.delta_pct,
        velocity: s.velocity,
        acceleration: s.acceleration,
        bid_yes: s.bid_yes,
        ask_yes: s.ask_yes,
        bid_no: s.bid_no,
        ask_no: s.ask_no,
        spread_up: s.spread_up,
        spread_down: s.spread_down,
        spread_mid: s.spread_mid,
    }
}

fn map_samples_to_fev1(samples: &[StrategySample]) -> Vec<fev1::Sample> {
    samples.iter().map(map_sample_to_fev1).collect()
}

fn map_cfg_to_fev1(cfg: &StrategyRuntimeConfig) -> fev1::RuntimeConfig {
    fev1::RuntimeConfig {
        entry_threshold_base: cfg.entry_threshold_base,
        entry_threshold_cap: cfg.entry_threshold_cap,
        spread_limit_prob: cfg.spread_limit_prob,
        entry_edge_prob: cfg.entry_edge_prob,
        entry_min_potential_cents: cfg.entry_min_potential_cents,
        entry_max_price_cents: cfg.entry_max_price_cents,
        min_hold_ms: cfg.min_hold_ms,
        stop_loss_cents: cfg.stop_loss_cents,
        reverse_signal_threshold: cfg.reverse_signal_threshold,
        reverse_signal_ticks: cfg.reverse_signal_ticks,
        trail_activate_profit_cents: cfg.trail_activate_profit_cents,
        trail_drawdown_cents: cfg.trail_drawdown_cents,
        take_profit_near_max_cents: cfg.take_profit_near_max_cents,
        endgame_take_profit_cents: cfg.endgame_take_profit_cents,
        endgame_remaining_ms: cfg.endgame_remaining_ms,
        liquidity_widen_prob: cfg.liquidity_widen_prob,
        cooldown_ms: cfg.cooldown_ms,
        max_entries_per_round: cfg.max_entries_per_round,
        max_exec_spread_cents: cfg.max_exec_spread_cents,
        slippage_cents_per_side: cfg.slippage_cents_per_side,
        fee_cents_per_side: cfg.fee_cents_per_side,
        emergency_wide_spread_penalty_ratio: cfg.emergency_wide_spread_penalty_ratio,
    }
}

fn map_simulation_result(run: fev1::SimulationResult) -> StrategySimulationResult {
    StrategySimulationResult {
        current: run.current,
        trades: run.trades,
        all_trade_pnls: run.all_trade_pnls,
        trade_count: run.trade_count,
        win_rate_pct: run.win_rate_pct,
        avg_pnl_cents: run.avg_pnl_cents,
        avg_duration_s: run.avg_duration_s,
        total_pnl_cents: run.total_pnl_cents,
        max_drawdown_cents: run.max_drawdown_cents,
        max_profit_trade_cents: run.max_profit_trade_cents,
        blocked_exits: run.blocked_exits,
        emergency_wide_exit_count: run.emergency_wide_exit_count,
        execution_penalty_cents_total: run.execution_penalty_cents_total,
        gross_pnl_cents: run.gross_pnl_cents,
        net_pnl_cents: run.net_pnl_cents,
        total_entry_fee_cents: run.total_entry_fee_cents,
        total_exit_fee_cents: run.total_exit_fee_cents,
        total_slippage_cents: run.total_slippage_cents,
        total_cost_cents: run.total_cost_cents,
        net_margin_pct: run.net_margin_pct,
    }
}

fn run_strategy_simulation(
    samples: &[StrategySample],
    cfg: &StrategyRuntimeConfig,
    max_trades: usize,
) -> StrategySimulationResult {
    let mapped_samples = map_samples_to_fev1(samples);
    let mapped_cfg = map_cfg_to_fev1(cfg);
    map_simulation_result(fev1::simulate(&mapped_samples, &mapped_cfg, max_trades))
}


async fn load_strategy_samples(
    state: &ApiState,
    market_type: &str,
    full_history: bool,
    lookback_minutes: u32,
    max_points: u32,
) -> Result<Vec<StrategySample>, ApiError> {
    let Some(ch_url) = state.ch_url.as_deref() else {
        return Err(ApiError::internal("clickhouse not configured"));
    };
    let from_ms = Utc::now()
        .timestamp_millis()
        .saturating_sub(i64::from(lookback_minutes) * 60_000);
    let ts_filter = if full_history {
        String::new()
    } else {
        format!("AND ts_ireland_sample_ms >= {from_ms}")
    };
    let q = format!(
        "SELECT
            intDiv(ts_ireland_sample_ms, 1000) * 1000 AS ts_ms,
            argMax(round_id, ts_ireland_sample_ms) AS round_id,
            argMax(remaining_ms, ts_ireland_sample_ms) AS remaining_ms,
            argMax(mid_yes, ts_ireland_sample_ms) AS mid_yes,
            argMax(mid_yes_smooth, ts_ireland_sample_ms) AS mid_yes_smooth,
            argMax(mid_no, ts_ireland_sample_ms) AS mid_no,
            argMax(mid_no_smooth, ts_ireland_sample_ms) AS mid_no_smooth,
            argMax(bid_yes, ts_ireland_sample_ms) AS bid_yes,
            argMax(ask_yes, ts_ireland_sample_ms) AS ask_yes,
            argMax(bid_no, ts_ireland_sample_ms) AS bid_no,
            argMax(ask_no, ts_ireland_sample_ms) AS ask_no,
            argMax(delta_pct, ts_ireland_sample_ms) AS delta_pct,
            argMax(delta_pct_smooth, ts_ireland_sample_ms) AS delta_pct_smooth,
            argMax(velocity_bps_per_sec, ts_ireland_sample_ms) AS velocity_bps_per_sec,
            argMax(acceleration, ts_ireland_sample_ms) AS acceleration,
            argMax(binance_price, ts_ireland_sample_ms) AS binance_price,
            argMax(target_price, ts_ireland_sample_ms) AS target_price
        FROM polyedge_forge.snapshot_100ms
        WHERE symbol='BTCUSDT'
          AND timeframe='{market_type}'
          {ts_filter}
        GROUP BY ts_ms
        ORDER BY ts_ms ASC
        LIMIT {max_points}
        FORMAT JSON"
    );
    let rows = rows_from_json(query_clickhouse_json(ch_url, &q).await?);
    Ok(parse_strategy_rows(rows))
}

async fn strategy_paper(
    State(state): State<ApiState>,
    Query(params): Query<StrategyPaperQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let source_mode = parse_strategy_paper_source(params.source.as_deref());
    let market_type = if let Some(mt) = params.market_type.as_deref() {
        normalize_market_type(mt).ok_or_else(|| ApiError::bad_request("invalid market_type"))?
    } else {
        "5m"
    };
    let use_autotune = params.use_autotune.unwrap_or(false);
    let mut cfg = StrategyRuntimeConfig::default();
    let mut config_source = "default";
    let mut autotune_info = Value::Null;
    if use_autotune && !matches!(source_mode, StrategyPaperSource::Live) {
        let key = strategy_autotune_key(&state.redis_prefix, market_type);
        if let Some(saved) = read_key_value(&state, &key).await? {
            let tuned_cfg = strategy_cfg_from_payload(cfg, &saved);
            cfg = tuned_cfg;
            config_source = "autotune";
            autotune_info = saved;
        }
    }
    if !matches!(source_mode, StrategyPaperSource::Live) {
        if let Some(v) = params.entry_threshold_base {
            cfg.entry_threshold_base = v.clamp(0.40, 0.95);
        }
        if let Some(v) = params.entry_threshold_cap {
            cfg.entry_threshold_cap = v.clamp(cfg.entry_threshold_base, 0.99);
        }
        if let Some(v) = params.spread_limit_prob {
            cfg.spread_limit_prob = v.clamp(0.005, 0.12);
        }
        if let Some(v) = params.entry_edge_prob {
            cfg.entry_edge_prob = v.clamp(0.002, 0.25);
        }
        if let Some(v) = params.entry_min_potential_cents {
            cfg.entry_min_potential_cents = v.clamp(1.0, 70.0);
        }
        if let Some(v) = params.entry_max_price_cents {
            cfg.entry_max_price_cents = v.clamp(45.0, 98.5);
        }
        if let Some(v) = params.min_hold_ms {
            cfg.min_hold_ms = v.clamp(0, 240_000);
        }
        if let Some(v) = params.stop_loss_cents {
            cfg.stop_loss_cents = v.clamp(2.0, 60.0);
        }
        if let Some(v) = params.reverse_signal_threshold {
            cfg.reverse_signal_threshold = v.clamp(-0.95, -0.02);
        }
        if let Some(v) = params.reverse_signal_ticks {
            cfg.reverse_signal_ticks = v.clamp(1, 12) as usize;
        }
        if let Some(v) = params.trail_activate_profit_cents {
            cfg.trail_activate_profit_cents = v.clamp(2.0, 80.0);
        }
        if let Some(v) = params.trail_drawdown_cents {
            cfg.trail_drawdown_cents = v.clamp(1.0, 50.0);
        }
        if let Some(v) = params.take_profit_near_max_cents {
            cfg.take_profit_near_max_cents = v.clamp(70.0, 99.5);
        }
        if let Some(v) = params.endgame_take_profit_cents {
            cfg.endgame_take_profit_cents = v.clamp(50.0, 99.0);
        }
        if let Some(v) = params.endgame_remaining_ms {
            cfg.endgame_remaining_ms = v.clamp(1_000, 180_000);
        }
        if let Some(v) = params.liquidity_widen_prob {
            cfg.liquidity_widen_prob = v.clamp(0.01, 0.2);
        }
        if let Some(v) = params.cooldown_ms {
            cfg.cooldown_ms = v.clamp(0, 120_000);
        }
        if let Some(v) = params.max_entries_per_round {
            cfg.max_entries_per_round = v.clamp(1, 16) as usize;
        }
        if let Some(v) = params.max_exec_spread_cents {
            cfg.max_exec_spread_cents = v.clamp(0.2, 30.0);
        }
        if let Some(v) = params.slippage_cents_per_side {
            cfg.slippage_cents_per_side = v.clamp(0.0, 10.0);
        }
        if let Some(v) = params.fee_cents_per_side {
            cfg.fee_cents_per_side = v.clamp(0.0, 12.0);
        }
        if let Some(v) = params.emergency_wide_spread_penalty_ratio {
            cfg.emergency_wide_spread_penalty_ratio = v.clamp(0.2, 3.0);
        }
        if cfg.entry_threshold_cap < cfg.entry_threshold_base {
            cfg.entry_threshold_cap = cfg.entry_threshold_base;
        }
    }
    let full_history = params.full_history.unwrap_or(false);
    let lookback_minutes = params
        .lookback_minutes
        .unwrap_or(if full_history { 30 * 24 * 60 } else { 6 * 60 })
        .clamp(30, 365 * 24 * 60);
    let max_points = params
        .max_points
        .unwrap_or(if full_history { 2_400_000 } else { 220_000 })
        .clamp(20_000, 5_000_000);
    let max_trades = params.max_trades.unwrap_or(200).clamp(20, 1000) as usize;
    let _live_execute = params.live_execute.unwrap_or(false);
    let _live_quote_usdc = params.live_quote_usdc.unwrap_or(1.0).clamp(0.5, 1000.0);
    let _live_max_orders = params.live_max_orders.unwrap_or(1).clamp(1, 8) as usize;
    let _live_entry_only = params.live_entry_only.unwrap_or(true);

    let mut source_fallback_error: Option<String> = None;
    if matches!(source_mode, StrategyPaperSource::Live | StrategyPaperSource::Auto) {
        if let Some(payload) = state.get_runtime_snapshot(market_type).await {
            return Ok(Json(payload));
        }
        let warmup_msg = "live runtime warming up (no snapshot yet)";
        if matches!(source_mode, StrategyPaperSource::Live) {
            return Err(ApiError::bad_request(warmup_msg));
        }
        source_fallback_error = Some(warmup_msg.to_string());
    }

    let samples = load_strategy_samples(
        &state,
        market_type,
        full_history,
        lookback_minutes,
        max_points,
    )
    .await?;
    if samples.len() < 20 {
        return Ok(Json(json!({
            "source": "replay",
            "execution_target": "paper",
            "live_enabled": fev1::ExecutionGate::from_env().live_enabled,
            "strategy_engine": "forge_fev1",
            "strategy_alias": "FEV1",
            "source_fallback_error": source_fallback_error,
            "market_type": market_type,
            "lookback_minutes": lookback_minutes,
            "samples": samples.len(),
            "current": Value::Null,
            "summary": {
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
                "net_margin_pct": 0.0,
                "max_drawdown_cents": 0.0,
            },
            "trades": [],
        })));
    }

    let run = run_strategy_simulation(&samples, &cfg, max_trades);

    Ok(Json(json!({
        "source": "replay",
        "execution_target": "paper",
        "live_enabled": fev1::ExecutionGate::from_env().live_enabled,
        "strategy_engine": "forge_fev1",
        "strategy_alias": "FEV1",
        "engine_version": "v1",
        "source_fallback_error": source_fallback_error,
        "market_type": market_type,
        "lookback_minutes": lookback_minutes,
        "full_history": full_history,
        "samples": samples.len(),
        "config_source": config_source,
        "baseline_profile": STRATEGY_BASELINE_PROFILE,
        "autotune": autotune_info,
        "config": {
            "entry_threshold_base": cfg.entry_threshold_base,
            "entry_threshold_cap": cfg.entry_threshold_cap,
            "spread_limit_prob": cfg.spread_limit_prob,
            "entry_edge_prob": cfg.entry_edge_prob,
            "entry_min_potential_cents": cfg.entry_min_potential_cents,
            "entry_max_price_cents": cfg.entry_max_price_cents,
            "min_hold_ms": cfg.min_hold_ms,
            "stop_loss_cents": cfg.stop_loss_cents,
            "trail_activate_profit_cents": cfg.trail_activate_profit_cents,
            "trail_drawdown_cents": cfg.trail_drawdown_cents,
            "take_profit_near_max_cents": cfg.take_profit_near_max_cents,
            "endgame_take_profit_cents": cfg.endgame_take_profit_cents,
            "endgame_remaining_ms": cfg.endgame_remaining_ms,
            "liquidity_widen_prob": cfg.liquidity_widen_prob,
            "reverse_signal_threshold": cfg.reverse_signal_threshold,
            "reverse_signal_ticks": cfg.reverse_signal_ticks,
            "max_exec_spread_cents": cfg.max_exec_spread_cents,
            "slippage_cents_per_side": cfg.slippage_cents_per_side,
            "fee_cents_per_side": cfg.fee_cents_per_side,
            "emergency_wide_spread_penalty_ratio": cfg.emergency_wide_spread_penalty_ratio
        },
        "current": run.current,
        "summary": {
            "trade_count": run.trade_count,
            "win_rate_pct": run.win_rate_pct,
            "avg_pnl_cents": run.avg_pnl_cents,
            "avg_duration_s": run.avg_duration_s,
            "total_pnl_cents": run.total_pnl_cents,
            "net_pnl_cents": run.net_pnl_cents,
            "gross_pnl_cents": run.gross_pnl_cents,
            "total_cost_cents": run.total_cost_cents,
            "total_entry_fee_cents": run.total_entry_fee_cents,
            "total_exit_fee_cents": run.total_exit_fee_cents,
            "total_slippage_cents": run.total_slippage_cents,
            "net_margin_pct": run.net_margin_pct,
            "max_drawdown_cents": run.max_drawdown_cents,
            "max_profit_trade_cents": run.max_profit_trade_cents,
            "blocked_exits": run.blocked_exits,
            "emergency_wide_exit_count": run.emergency_wide_exit_count,
            "execution_penalty_cents_total": run.execution_penalty_cents_total,
        },
        "trades": run.trades,
    })))
}

async fn strategy_live_reset(State(state): State<ApiState>) -> Result<Json<Value>, ApiError> {
    let now_ms = Utc::now().timestamp_millis();
    {
        let mut events = state.live_events.write().await;
        events.clear();
    }
    {
        let mut pending = state.live_pending_orders.write().await;
        pending.clear();
    }
    {
        let mut guard = state.live_decision_guard.write().await;
        guard.clear();
    }
    {
        let mut snapshots = state.live_runtime_snapshots.write().await;
        snapshots.clear();
    }
    state.set_gateway_report_seq(0).await;
    for market in ["5m", "15m"] {
        state
            .put_live_position_state(market, LivePositionState::flat(market, now_ms))
            .await;
    }

    let mut deleted_keys = Vec::<String>::new();
    for market in ["5m", "15m"] {
        let key = live_position_state_key(&state.redis_prefix, market);
        if delete_key(&state, &key).await.is_ok() {
            deleted_keys.push(key);
        }
    }
    let pending_key = live_pending_orders_key(&state.redis_prefix);
    if delete_key(&state, &pending_key).await.is_ok() {
        deleted_keys.push(pending_key);
    }
    let seq_key = live_gateway_seq_key(&state.redis_prefix);
    if delete_key(&state, &seq_key).await.is_ok() {
        deleted_keys.push(seq_key);
    }

    Ok(Json(json!({
        "ok": true,
        "reset_at_ms": now_ms,
        "deleted_redis_keys": deleted_keys
    })))
}

async fn strategy_autotune_latest(
    State(state): State<ApiState>,
    Query(params): Query<StrategyAutotuneLatestQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let market_type = if let Some(mt) = params.market_type.as_deref() {
        normalize_market_type(mt).ok_or_else(|| ApiError::bad_request("invalid market_type"))?
    } else {
        "5m"
    };
    let key = strategy_autotune_key(&state.redis_prefix, market_type);
    let payload = read_key_value(&state, &key).await?;
    Ok(Json(json!({
        "market_type": market_type,
        "key": key,
        "found": payload.is_some(),
        "data": payload,
    })))
}

async fn strategy_autotune_history(
    State(state): State<ApiState>,
    Query(params): Query<StrategyAutotuneHistoryQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let market_type = if let Some(mt) = params.market_type.as_deref() {
        normalize_market_type(mt).ok_or_else(|| ApiError::bad_request("invalid market_type"))?
    } else {
        "5m"
    };
    let limit = params.limit.unwrap_or(20).clamp(1, 200) as usize;
    let key = strategy_autotune_history_key(&state.redis_prefix, market_type);
    let mut items = read_key_value(&state, &key)
        .await?
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default();
    if items.len() > limit {
        items.truncate(limit);
    }
    Ok(Json(json!({
        "market_type": market_type,
        "key": key,
        "limit": limit,
        "count": items.len(),
        "items": items,
    })))
}

async fn strategy_autotune_set(
    State(state): State<ApiState>,
    Json(body): Json<StrategyAutotuneSetBody>,
) -> Result<Json<Value>, ApiError> {
    let market_type = if let Some(mt) = body.market_type.as_deref() {
        normalize_market_type(mt).ok_or_else(|| ApiError::bad_request("invalid market_type"))?
    } else {
        "5m"
    };
    let key = strategy_autotune_key(&state.redis_prefix, market_type);
    let history_key = strategy_autotune_history_key(&state.redis_prefix, market_type);
    let previous = read_key_value(&state, &key).await?;

    let wrapped = json!({ "config": body.config });
    let cfg = strategy_cfg_from_payload(StrategyRuntimeConfig::default(), &wrapped);
    let saved_doc = json!({
        "saved_at_ms": Utc::now().timestamp_millis(),
        "market_type": market_type,
        "source": body.source.unwrap_or_else(|| "manual".to_string()),
        "note": body.note.unwrap_or_default(),
        "config": strategy_cfg_json(&cfg),
    });

    write_key_value(&state, &key, &saved_doc, body.ttl_sec.filter(|v| *v > 0)).await?;

    // Keep a small local history for rollback decisions.
    let mut history: Vec<Value> = read_key_value(&state, &history_key)
        .await?
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default();
    if let Some(prev) = previous.clone() {
        history.insert(0, prev);
    }
    if history.len() > 40 {
        history.truncate(40);
    }
    let _ = write_key_value(&state, &history_key, &Value::Array(history), None).await;

    Ok(Json(json!({
        "ok": true,
        "market_type": market_type,
        "key": key,
        "history_key": history_key,
        "previous_found": previous.is_some(),
        "saved": saved_doc,
    })))
}

fn build_strategy_arms(
    base: StrategyRuntimeConfig,
    max_arms: usize,
) -> Vec<(String, StrategyRuntimeConfig)> {
    let mut arms = Vec::<(String, StrategyRuntimeConfig)>::new();
    let presets = [
        (
            0.64, 0.030, 0.030, 10.0, 92.0, 20_000, 10.0, 4usize, 20.0, 8.0, 95.0, 90.0, 0.07,
            1usize,
        ),
        (
            0.68, 0.024, 0.040, 8.0, 90.0, 25_000, 10.0, 5usize, 24.0, 11.0, 93.0, 88.0, 0.07,
            2usize,
        ),
        (
            0.72, 0.024, 0.050, 12.0, 86.0, 35_000, 12.0, 5usize, 26.0, 10.0, 94.0, 90.0, 0.08,
            1usize,
        ),
        (
            0.76, 0.020, 0.060, 14.0, 84.0, 45_000, 14.0, 6usize, 30.0, 12.0, 96.0, 92.0, 0.08,
            1usize,
        ),
        (
            0.62, 0.038, 0.025, 6.0, 94.0, 15_000, 8.0, 3usize, 18.0, 6.0, 92.0, 86.0, 0.09, 2usize,
        ),
        (
            0.70, 0.030, 0.045, 10.0, 88.0, 30_000, 12.0, 4usize, 22.0, 9.0, 94.0, 90.0, 0.07,
            2usize,
        ),
        (
            0.74, 0.024, 0.055, 14.0, 82.0, 55_000, 16.0, 6usize, 28.0, 12.0, 97.0, 94.0, 0.08,
            1usize,
        ),
        (
            0.66, 0.024, 0.035, 9.0, 90.0, 22_000, 10.0, 4usize, 20.0, 8.0, 93.0, 88.0, 0.06,
            2usize,
        ),
        (
            0.78, 0.020, 0.075, 16.0, 82.0, 40_000, 8.0, 5usize, 22.0, 8.0, 96.0, 94.0, 0.05,
            1usize,
        ),
        (
            0.80, 0.018, 0.070, 18.0, 80.0, 50_000, 10.0, 6usize, 26.0, 10.0, 97.0, 95.0, 0.06,
            1usize,
        ),
        (
            0.84, 0.016, 0.085, 20.0, 78.0, 65_000, 9.0, 6usize, 24.0, 9.0, 98.0, 96.0, 0.06,
            1usize,
        ),
        (
            0.82, 0.014, 0.090, 24.0, 75.0, 70_000, 8.0, 7usize, 28.0, 11.0, 98.0, 97.0, 0.05,
            1usize,
        ),
        (
            0.86, 0.010, 0.095, 18.0, 78.0, 20_000, 6.0, 2usize, 10.0, 3.8, 90.0, 88.0, 0.045,
            1usize,
        ),
        (
            0.88, 0.009, 0.105, 20.0, 76.0, 24_000, 5.5, 1usize, 9.0, 3.2, 89.0, 87.0, 0.042,
            1usize,
        ),
        (
            0.90, 0.008, 0.115, 22.0, 74.0, 28_000, 5.0, 1usize, 8.0, 3.0, 88.5, 86.5, 0.040,
            1usize,
        ),
        (
            0.84, 0.011, 0.085, 16.0, 80.0, 18_000, 6.5, 2usize, 11.0, 4.2, 91.0, 89.0, 0.046,
            1usize,
        ),
    ];
    for (idx, p) in presets.iter().enumerate() {
        let mut cfg = base;
        cfg.entry_threshold_base = p.0;
        cfg.entry_threshold_cap = (p.0 + 0.22).clamp(p.0, 0.99);
        cfg.spread_limit_prob = p.1;
        cfg.entry_edge_prob = p.2;
        cfg.entry_min_potential_cents = p.3;
        cfg.entry_max_price_cents = p.4;
        cfg.min_hold_ms = p.5;
        cfg.stop_loss_cents = p.6;
        cfg.reverse_signal_ticks = p.7;
        cfg.trail_activate_profit_cents = p.8;
        cfg.trail_drawdown_cents = p.9;
        cfg.take_profit_near_max_cents = p.10;
        cfg.endgame_take_profit_cents = p.11;
        cfg.liquidity_widen_prob = p.12;
        cfg.max_entries_per_round = p.13;
        arms.push((format!("arm_{:02}", idx + 1), cfg));
    }
    if arms.len() > max_arms {
        arms.truncate(max_arms);
    }
    arms
}

fn group_round_samples(samples: &[StrategySample]) -> Vec<Vec<StrategySample>> {
    let mut rounds = Vec::<Vec<StrategySample>>::new();
    let mut cur: Vec<StrategySample> = Vec::new();
    let mut cur_id = String::new();
    for s in samples {
        if cur_id.is_empty() || cur_id == s.round_id {
            cur_id = s.round_id.clone();
            cur.push(s.clone());
        } else {
            rounds.push(cur);
            cur = vec![s.clone()];
            cur_id = s.round_id.clone();
        }
    }
    if !cur.is_empty() {
        rounds.push(cur);
    }
    rounds
}

async fn strategy_full(
    State(state): State<ApiState>,
    Query(params): Query<StrategyFullQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let market_type = if let Some(mt) = params.market_type.as_deref() {
        normalize_market_type(mt).ok_or_else(|| ApiError::bad_request("invalid market_type"))?
    } else {
        "5m"
    };
    let full_history = params.full_history.unwrap_or(true);
    let lookback_minutes = params
        .lookback_minutes
        .unwrap_or(if full_history { 30 * 24 * 60 } else { 12 * 60 })
        .clamp(30, 365 * 24 * 60);
    let max_points = params
        .max_points
        .unwrap_or(if full_history { 2_400_000 } else { 220_000 })
        .clamp(20_000, 5_000_000);
    let max_trades = params.max_trades.unwrap_or(300).clamp(20, 1000) as usize;
    let max_arms = params.max_arms.unwrap_or(8).clamp(2, 24) as usize;
    let window_trades = params.window_trades.unwrap_or(50).clamp(10, 200) as usize;
    let target_win_rate = params.target_win_rate.unwrap_or(90.0).clamp(40.0, 99.9);

    let samples = load_strategy_samples(
        &state,
        market_type,
        full_history,
        lookback_minutes,
        max_points,
    )
    .await?;
    if samples.len() < 20 {
        return Ok(Json(json!({
            "market_type": market_type,
            "full_history": full_history,
            "samples": samples.len(),
            "error": "not enough samples",
        })));
    }

    let base_cfg = StrategyRuntimeConfig::default();
    let arms = build_strategy_arms(base_cfg, max_arms);
    let mut evaluations: Vec<Value> = Vec::new();
    for (name, cfg) in &arms {
        let run = run_strategy_simulation(&samples, cfg, max_trades);
        let rs = compute_rolling_stats(&run.all_trade_pnls, window_trades);
        let objective = if rs.windows > 0 {
            rs.latest_win_rate_pct * 2.8
                + rs.avg_win_rate_pct * 1.7
                + rs.min_win_rate_pct * 0.6
                + run.avg_pnl_cents * 2.0
                + run.total_pnl_cents.max(0.0) * 0.02
                - run.max_drawdown_cents * 0.02
                + if rs.latest_win_rate_pct >= target_win_rate {
                    120.0
                } else {
                    0.0
                }
        } else {
            run.win_rate_pct + run.avg_pnl_cents + run.total_pnl_cents.max(0.0) * 0.01 - 120.0
        };
        evaluations.push(json!({
            "name": name,
            "objective": objective,
            "summary": {
                "trade_count": run.trade_count,
                "win_rate_pct": run.win_rate_pct,
                "avg_pnl_cents": run.avg_pnl_cents,
                "avg_duration_s": run.avg_duration_s,
                "total_pnl_cents": run.total_pnl_cents,
                "max_drawdown_cents": run.max_drawdown_cents,
                "max_profit_trade_cents": run.max_profit_trade_cents,
                "blocked_exits": run.blocked_exits,
                "emergency_wide_exit_count": run.emergency_wide_exit_count,
                "execution_penalty_cents_total": run.execution_penalty_cents_total,
            },
            "rolling_window": rolling_stats_json(rs),
            "config": {
                "entry_threshold_base": cfg.entry_threshold_base,
                "entry_threshold_cap": cfg.entry_threshold_cap,
                "spread_limit_prob": cfg.spread_limit_prob,
                "entry_edge_prob": cfg.entry_edge_prob,
                "entry_min_potential_cents": cfg.entry_min_potential_cents,
                "entry_max_price_cents": cfg.entry_max_price_cents,
                "min_hold_ms": cfg.min_hold_ms,
                "stop_loss_cents": cfg.stop_loss_cents,
                "reverse_signal_threshold": cfg.reverse_signal_threshold,
                "reverse_signal_ticks": cfg.reverse_signal_ticks,
                "trail_activate_profit_cents": cfg.trail_activate_profit_cents,
                "trail_drawdown_cents": cfg.trail_drawdown_cents,
                "take_profit_near_max_cents": cfg.take_profit_near_max_cents,
                "endgame_take_profit_cents": cfg.endgame_take_profit_cents,
                "endgame_remaining_ms": cfg.endgame_remaining_ms,
                "liquidity_widen_prob": cfg.liquidity_widen_prob,
                "cooldown_ms": cfg.cooldown_ms,
                "max_entries_per_round": cfg.max_entries_per_round,
                "max_exec_spread_cents": cfg.max_exec_spread_cents,
                "slippage_cents_per_side": cfg.slippage_cents_per_side,
                "fee_cents_per_side": cfg.fee_cents_per_side,
                "emergency_wide_spread_penalty_ratio": cfg.emergency_wide_spread_penalty_ratio,
            },
            "trades": run.trades,
            "current": run.current,
        }));
    }
    evaluations.sort_by(|a, b| {
        let av = row_f64(a, "objective").unwrap_or(f64::NEG_INFINITY);
        let bv = row_f64(b, "objective").unwrap_or(f64::NEG_INFINITY);
        bv.partial_cmp(&av).unwrap_or(std::cmp::Ordering::Equal)
    });

    let best = evaluations.first().cloned().unwrap_or(Value::Null);
    let max_win_rate = evaluations
        .iter()
        .map(|v| {
            v.get("rolling_window")
                .and_then(|s| s.get("latest_win_rate_pct"))
                .and_then(Value::as_f64)
                .unwrap_or(0.0)
        })
        .fold(0.0_f64, f64::max);
    let max_profit_per_trade_cents = evaluations
        .iter()
        .map(|v| {
            v.get("summary")
                .and_then(|s| s.get("max_profit_trade_cents"))
                .and_then(Value::as_f64)
                .unwrap_or(0.0)
        })
        .fold(f64::NEG_INFINITY, f64::max);

    // Lightweight online learner (UCB1) replay by round for arm adaptation preview.
    let rounds = group_round_samples(&samples);
    let mut plays = vec![0usize; arms.len()];
    let mut reward_sum = vec![0.0_f64; arms.len()];
    let mut wins = vec![0usize; arms.len()];
    for r in &rounds {
        let total_played: usize = plays.iter().sum();
        let mut best_idx = 0usize;
        let mut best_ucb = f64::NEG_INFINITY;
        for idx in 0..arms.len() {
            if plays[idx] == 0 {
                best_idx = idx;
                break;
            }
            let mean = reward_sum[idx] / plays[idx] as f64;
            let explore = (2.0 * ((total_played + 1) as f64).ln() / plays[idx] as f64).sqrt();
            let ucb = mean + explore;
            if ucb > best_ucb {
                best_ucb = ucb;
                best_idx = idx;
            }
        }
        let run = run_strategy_simulation(r, &arms[best_idx].1, 40);
        plays[best_idx] += 1;
        reward_sum[best_idx] += run.total_pnl_cents;
        if run.total_pnl_cents > 0.0 {
            wins[best_idx] += 1;
        }
    }
    let learner_arms: Vec<Value> = arms
        .iter()
        .enumerate()
        .map(|(i, (name, _))| {
            let p = plays[i];
            let mean = if p > 0 { reward_sum[i] / p as f64 } else { 0.0 };
            let wr = if p > 0 {
                (wins[i] as f64) * 100.0 / p as f64
            } else {
                0.0
            };
            json!({
                "name": name,
                "plays": p,
                "win_rate_pct": wr,
                "mean_round_pnl_cents": mean,
                "total_round_pnl_cents": reward_sum[i]
            })
        })
        .collect();

    Ok(Json(json!({
        "market_type": market_type,
        "full_history": full_history,
        "lookback_minutes": lookback_minutes,
        "target_win_rate_pct": target_win_rate,
        "window_trades": window_trades,
        "samples": samples.len(),
        "rounds": rounds.len(),
        "arms_tested": evaluations.len(),
        "best": best,
        "top_arms": evaluations.iter().take(5).cloned().collect::<Vec<_>>(),
        "metrics": {
            "estimated_max_win_rate_pct": max_win_rate,
            "estimated_max_profit_per_trade_cents": if max_profit_per_trade_cents.is_finite() { max_profit_per_trade_cents } else { 0.0 }
        },
        "learner_ucb": {
            "arms": learner_arms
        }
    })))
}

fn clamp_runtime_cfg(cfg: &mut StrategyRuntimeConfig) {
    cfg.entry_threshold_base = cfg.entry_threshold_base.clamp(0.40, 0.95);
    cfg.entry_threshold_cap = cfg
        .entry_threshold_cap
        .clamp(cfg.entry_threshold_base, 0.99);
    cfg.spread_limit_prob = cfg.spread_limit_prob.clamp(0.005, 0.12);
    cfg.entry_edge_prob = cfg.entry_edge_prob.clamp(0.002, 0.25);
    cfg.entry_min_potential_cents = cfg.entry_min_potential_cents.clamp(1.0, 70.0);
    cfg.entry_max_price_cents = cfg.entry_max_price_cents.clamp(45.0, 98.5);
    cfg.min_hold_ms = cfg.min_hold_ms.clamp(0, 240_000);
    cfg.stop_loss_cents = cfg.stop_loss_cents.clamp(2.0, 60.0);
    cfg.reverse_signal_threshold = cfg.reverse_signal_threshold.clamp(-0.95, -0.02);
    cfg.reverse_signal_ticks = cfg.reverse_signal_ticks.clamp(1, 12);
    cfg.trail_activate_profit_cents = cfg.trail_activate_profit_cents.clamp(2.0, 80.0);
    cfg.trail_drawdown_cents = cfg.trail_drawdown_cents.clamp(1.0, 50.0);
    cfg.take_profit_near_max_cents = cfg.take_profit_near_max_cents.clamp(70.0, 99.5);
    cfg.endgame_take_profit_cents = cfg.endgame_take_profit_cents.clamp(50.0, 99.0);
    cfg.endgame_remaining_ms = cfg.endgame_remaining_ms.clamp(1_000, 180_000);
    cfg.liquidity_widen_prob = cfg.liquidity_widen_prob.clamp(0.01, 0.2);
    cfg.cooldown_ms = cfg.cooldown_ms.clamp(0, 120_000);
    cfg.max_entries_per_round = cfg.max_entries_per_round.clamp(1, 16);
    cfg.max_exec_spread_cents = cfg.max_exec_spread_cents.clamp(0.8, 12.0);
    cfg.slippage_cents_per_side = cfg.slippage_cents_per_side.clamp(0.03, 4.0);
    cfg.fee_cents_per_side = cfg.fee_cents_per_side.clamp(0.03, 4.0);
    cfg.emergency_wide_spread_penalty_ratio =
        cfg.emergency_wide_spread_penalty_ratio.clamp(0.0, 2.0);
}

fn lcg_next(seed: &mut u64) -> f64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    let x = (*seed >> 33) as u32;
    (x as f64) / (u32::MAX as f64)
}

fn mutate_cfg(
    parent: &StrategyRuntimeConfig,
    seed: &mut u64,
    iter: usize,
    max_iter: usize,
) -> StrategyRuntimeConfig {
    let mut c = *parent;
    let progress = if max_iter > 0 {
        (iter as f64 / max_iter as f64).clamp(0.0, 1.0)
    } else {
        1.0
    };
    let scale = (1.0 - progress * 0.7).clamp(0.25, 1.0);
    let r = |seed: &mut u64| -> f64 { lcg_next(seed) * 2.0 - 1.0 };

    c.entry_threshold_base += r(seed) * 0.08 * scale;
    c.entry_threshold_cap += r(seed) * 0.08 * scale;
    c.spread_limit_prob += r(seed) * 0.012 * scale;
    c.entry_edge_prob += r(seed) * 0.02 * scale;
    c.entry_min_potential_cents += r(seed) * 8.0 * scale;
    c.entry_max_price_cents += r(seed) * 8.0 * scale;
    c.min_hold_ms += (r(seed) * 30_000.0 * scale) as i64;
    c.stop_loss_cents += r(seed) * 8.0 * scale;
    c.reverse_signal_threshold += r(seed) * 0.18 * scale;
    c.reverse_signal_ticks =
        ((c.reverse_signal_ticks as f64) + r(seed) * 2.5 * scale).round() as usize;
    c.trail_activate_profit_cents += r(seed) * 10.0 * scale;
    c.trail_drawdown_cents += r(seed) * 6.0 * scale;
    c.take_profit_near_max_cents += r(seed) * 4.0 * scale;
    c.endgame_take_profit_cents += r(seed) * 5.0 * scale;
    c.endgame_remaining_ms += (r(seed) * 30_000.0 * scale) as i64;
    c.liquidity_widen_prob += r(seed) * 0.03 * scale;
    c.cooldown_ms += (r(seed) * 8_000.0 * scale) as i64;
    c.max_entries_per_round =
        ((c.max_entries_per_round as f64) + r(seed) * 1.5 * scale).round() as usize;
    c.max_exec_spread_cents += r(seed) * 1.5 * scale;
    c.slippage_cents_per_side += r(seed) * 0.4 * scale;
    c.fee_cents_per_side += r(seed) * 0.2 * scale;
    c.emergency_wide_spread_penalty_ratio += r(seed) * 0.25 * scale;
    clamp_runtime_cfg(&mut c);
    c
}

fn score_with_rolling(
    run: &StrategySimulationResult,
    window_trades: usize,
    target_win_rate: f64,
) -> (RollingStats, f64, bool) {
    let rs = compute_rolling_stats(&run.all_trade_pnls, window_trades);
    let pf = profit_factor(&run.all_trade_pnls);
    let tail_penalty = loss_tail_penalty(&run.all_trade_pnls);
    let side_penalty = side_loss_penalty(&run.trades);
    let trade_shortfall = window_trades.saturating_sub(run.trade_count) as f64;
    let coverage_ratio = (run.trade_count as f64 / window_trades as f64).clamp(0.0, 1.0);
    if run.trade_count < window_trades {
        let objective = -5000.0
            - trade_shortfall * 60.0
            - run.max_drawdown_cents * 0.05
            - run.execution_penalty_cents_total
            - run.blocked_exits as f64 * 30.0
            - tail_penalty * 1.2
            - side_penalty * 0.9
            - (1.0 - pf.min(1.0)) * 220.0;
        return (rs, objective, false);
    }
    let shortfall = (target_win_rate - rs.latest_win_rate_pct).max(0.0);
    let target_hit = rs.windows > 0
        && rs.latest_win_rate_pct >= target_win_rate
        && run.avg_pnl_cents > 0.0
        && run.trade_count >= window_trades;
    let non_positive_pnl_penalty = if run.avg_pnl_cents <= 0.0 || run.total_pnl_cents <= 0.0 {
        280.0 + run.avg_pnl_cents.abs() * 240.0 + run.total_pnl_cents.abs() * 0.02
    } else {
        0.0
    };
    let pf_bonus = ((pf.min(4.0) - 1.0).max(0.0)) * 22.0;
    let pf_penalty = ((1.0 - pf).max(0.0)) * 180.0;
    let churn_penalty = if run.avg_duration_s < 6.0 {
        (6.0 - run.avg_duration_s) * 5.0
    } else {
        0.0
    };
    let objective = if rs.windows > 0 {
        (rs.latest_win_rate_pct * 3.2
            + rs.avg_win_rate_pct * 1.8
            + rs.min_win_rate_pct * 0.8
            + run.avg_pnl_cents * 2.2
            + run.total_pnl_cents.max(0.0) * 0.02
            + run.max_profit_trade_cents.max(0.0) * 0.6
            - run.max_drawdown_cents * 0.02
            - run.execution_penalty_cents_total * 0.4
            - run.blocked_exits as f64 * 18.0
            - tail_penalty
            - side_penalty
            - shortfall * shortfall * 2.4
            - non_positive_pnl_penalty
            + pf_bonus
            - pf_penalty
            - churn_penalty
            + if target_hit { 160.0 } else { 0.0 })
            * (0.60 + 0.40 * coverage_ratio)
            - trade_shortfall * 25.0
    } else {
        run.win_rate_pct * 0.3
            + run.avg_pnl_cents * 0.2
            - run.max_drawdown_cents * 0.03
            - run.execution_penalty_cents_total * 0.6
            - run.blocked_exits as f64 * 25.0
            - tail_penalty * 1.1
            - side_penalty * 0.8
            - 320.0
            - trade_shortfall * 35.0
            - pf_penalty
    };
    (rs, objective, target_hit)
}

async fn strategy_optimize(
    State(state): State<ApiState>,
    Query(params): Query<StrategyOptimizeQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let market_type = if let Some(mt) = params.market_type.as_deref() {
        normalize_market_type(mt).ok_or_else(|| ApiError::bad_request("invalid market_type"))?
    } else {
        "5m"
    };
    let full_history = params.full_history.unwrap_or(true);
    let lookback_minutes = params
        .lookback_minutes
        .unwrap_or(if full_history { 30 * 24 * 60 } else { 12 * 60 })
        .clamp(30, 365 * 24 * 60);
    let max_points = params
        .max_points
        .unwrap_or(if full_history { 2_400_000 } else { 220_000 })
        .clamp(20_000, 5_000_000);
    let max_trades = params.max_trades.unwrap_or(400).clamp(20, 2000) as usize;
    let max_arms = params.max_arms.unwrap_or(8).clamp(2, 24) as usize;
    let window_trades = params.window_trades.unwrap_or(50).clamp(10, 200) as usize;
    let target_win_rate = params.target_win_rate.unwrap_or(90.0).clamp(40.0, 99.9);
    let iterations = params.iterations.unwrap_or(600).clamp(50, 5000) as usize;
    let recent_lookback_minutes = params
        .recent_lookback_minutes
        .unwrap_or(12 * 60)
        .clamp(60, 30 * 24 * 60);
    let recent_weight = params.recent_weight.unwrap_or(0.62).clamp(0.25, 0.90);

    let samples = load_strategy_samples(
        &state,
        market_type,
        full_history,
        lookback_minutes,
        max_points,
    )
    .await?;
    if samples.len() < 20 {
        return Ok(Json(json!({
            "market_type": market_type,
            "full_history": full_history,
            "samples": samples.len(),
            "error": "not enough samples",
        })));
    }

    let (train_samples_buf, valid_samples_buf) = split_samples_by_round(&samples, 0.30);
    let using_validation = !valid_samples_buf.is_empty();
    let train_samples: &[StrategySample] = if using_validation {
        train_samples_buf.as_slice()
    } else {
        samples.as_slice()
    };
    let valid_samples: &[StrategySample] = if using_validation {
        valid_samples_buf.as_slice()
    } else {
        samples.as_slice()
    };
    let valid_window_trades = window_trades;
    let valid_target_win_rate = (target_win_rate - 6.0).clamp(40.0, 99.9);
    let valid_max_trades = std::cmp::max(std::cmp::max(160, window_trades * 3), max_trades);
    let latest_ts_ms = samples.last().map(|s| s.ts_ms).unwrap_or(0);
    let recent_from_ms =
        latest_ts_ms.saturating_sub(i64::from(recent_lookback_minutes) * 60_000);
    let mut recent_samples_buf: Vec<StrategySample> = samples
        .iter()
        .filter(|s| s.ts_ms >= recent_from_ms)
        .cloned()
        .collect();
    if recent_samples_buf.len() < 200 {
        recent_samples_buf = valid_samples.to_vec();
    }
    let recent_samples: &[StrategySample] = recent_samples_buf.as_slice();
    let recent_target_win_rate = (target_win_rate - 2.0).clamp(40.0, 99.9);
    let recent_max_trades = std::cmp::max(valid_max_trades, window_trades * 4);
    let valid_weight = (1.0 - recent_weight).max(0.1);

    let base_cfg = StrategyRuntimeConfig::default();
    let pool: Vec<(String, StrategyRuntimeConfig)> = build_strategy_arms(base_cfg, max_arms);
    let mut seed = params.seed.unwrap_or(0xC0DEC0DE1234ABCDu64);

    let mut leaderboard: Vec<Value> = Vec::new();
    let mut best_objective = f64::NEG_INFINITY;
    let mut best_payload = Value::Null;
    let mut reached_target = false;

    let evaluate_candidate =
        |name: String, cfg: &StrategyRuntimeConfig| -> (Value, f64, bool) {
            let train_run = run_strategy_simulation(train_samples, cfg, max_trades);
            let valid_run = run_strategy_simulation(valid_samples, cfg, valid_max_trades);
            let recent_run = run_strategy_simulation(recent_samples, cfg, recent_max_trades);

            let (train_rs, train_obj, _train_hit) =
                score_with_rolling(&train_run, window_trades, target_win_rate);
            let (valid_rs, valid_obj, valid_hit) =
                score_with_rolling(&valid_run, valid_window_trades, valid_target_win_rate);
            let (recent_rs, recent_obj, recent_hit) =
                score_with_rolling(&recent_run, valid_window_trades, recent_target_win_rate);

            let pf_train = profit_factor(&train_run.all_trade_pnls);
            let pf_valid = profit_factor(&valid_run.all_trade_pnls);
            let pf_recent = profit_factor(&recent_run.all_trade_pnls);
            let wr_gap = (train_rs.latest_win_rate_pct - valid_rs.latest_win_rate_pct).abs();
            let pnl_gap = (train_run.avg_pnl_cents - valid_run.avg_pnl_cents).abs();
            let pf_gap = (pf_train - pf_valid).abs();
            let recent_wr_gap = (recent_rs.latest_win_rate_pct - valid_rs.latest_win_rate_pct).abs();
            let consistency_penalty =
                wr_gap * 1.1 + pnl_gap * 1.6 + pf_gap * 8.0 + recent_wr_gap * 1.8;
            let validation_fail_penalty =
                if valid_run.avg_pnl_cents <= 0.0 || valid_run.total_pnl_cents <= 0.0 {
                    300.0
                        + valid_run.avg_pnl_cents.abs() * 260.0
                        + valid_run.total_pnl_cents.abs() * 0.03
                } else {
                    0.0
                };
            let train_fail_penalty = if train_run.avg_pnl_cents <= 0.0
                || train_run.total_pnl_cents <= 0.0
                || pf_train < 0.90
            {
                80.0
                    + train_run.avg_pnl_cents.abs() * 80.0
                    + train_run.total_pnl_cents.abs() * 0.004
                    + (0.90 - pf_train).max(0.0) * 160.0
            } else {
                0.0
            };
            let recent_fail_penalty = if recent_run.avg_pnl_cents <= 0.0
                || recent_run.total_pnl_cents <= 0.0
                || pf_recent < 1.0
            {
                260.0
                    + recent_run.avg_pnl_cents.abs() * 240.0
                    + recent_run.total_pnl_cents.abs() * 0.02
                    + (1.0 - pf_recent).max(0.0) * 220.0
            } else {
                0.0
            };
            let fill_risk_penalty = valid_run.blocked_exits as f64 * 18.0
                + valid_run.execution_penalty_cents_total * 0.9
                + valid_run.emergency_wide_exit_count as f64 * 3.0;
            let train_trade_shortfall = window_trades.saturating_sub(train_run.trade_count) as f64;
            let valid_trade_shortfall = window_trades.saturating_sub(valid_run.trade_count) as f64;
            let recent_trade_shortfall =
                window_trades.saturating_sub(recent_run.trade_count) as f64;
            let coverage_gate_penalty =
                train_trade_shortfall * 800.0
                    + valid_trade_shortfall * 900.0
                    + recent_trade_shortfall * 1100.0;

            let objective = train_obj * 0.14 + valid_obj * valid_weight + recent_obj * recent_weight * 1.15
                - consistency_penalty
                - validation_fail_penalty
                - train_fail_penalty
                - recent_fail_penalty
                - fill_risk_penalty
                - coverage_gate_penalty;
            let hit = (valid_hit || valid_rs.latest_win_rate_pct >= (target_win_rate - 3.0))
                && (recent_hit || recent_rs.latest_win_rate_pct >= (target_win_rate - 2.0))
                && valid_run.avg_pnl_cents > 0.0
                && recent_run.avg_pnl_cents > 0.0
                && pf_valid >= 1.05
                && pf_recent >= 1.02;

            let payload = json!({
                "name": name,
                "objective": objective,
                "rolling_window": rolling_stats_json(valid_rs),
                "rolling_window_train": rolling_stats_json(train_rs),
                "rolling_window_validation": rolling_stats_json(valid_rs),
                "rolling_window_recent": rolling_stats_json(recent_rs),
                "summary": run_summary_json(&valid_run),
                "summary_train": run_summary_json(&train_run),
                "summary_validation": run_summary_json(&valid_run),
                "summary_recent": run_summary_json(&recent_run),
                "profit_factor_train": pf_train,
                "profit_factor_validation": pf_valid,
                "profit_factor_recent": pf_recent,
                "consistency": {
                    "win_rate_gap_pct": wr_gap,
                    "avg_pnl_gap_cents": pnl_gap,
                    "profit_factor_gap": pf_gap,
                    "recent_win_rate_gap_pct": recent_wr_gap
                },
                "config": strategy_cfg_json(cfg),
            });
            (payload, objective, hit)
        };

    // warmup evaluate initial arms
    for (name, cfg) in &pool {
        let (payload, objective, hit) = evaluate_candidate(name.clone(), cfg);
        leaderboard.push(payload.clone());
        if objective > best_objective {
            best_objective = objective;
            best_payload = payload;
        }
        if hit {
            reached_target = true;
        }
    }
    leaderboard.sort_by(|a, b| {
        let av = a
            .get("objective")
            .and_then(Value::as_f64)
            .unwrap_or(f64::NEG_INFINITY);
        let bv = b
            .get("objective")
            .and_then(Value::as_f64)
            .unwrap_or(f64::NEG_INFINITY);
        bv.partial_cmp(&av).unwrap_or(std::cmp::Ordering::Equal)
    });
    leaderboard.truncate(16);

    if !reached_target {
        for iter in 0..iterations {
            let elite_count = min(leaderboard.len(), 4).max(1);
            let pick = (lcg_next(&mut seed) * elite_count as f64).floor() as usize;
            let parent_cfg = if let Some(parent) = leaderboard.get(pick) {
                strategy_cfg_from_payload(base_cfg, parent)
            } else {
                base_cfg
            };
            let candidate_cfg = mutate_cfg(&parent_cfg, &mut seed, iter, iterations);
            let (payload, objective, hit) =
                evaluate_candidate(format!("iter_{iter}"), &candidate_cfg);
            leaderboard.push(payload.clone());
            leaderboard.sort_by(|a, b| {
                let av = a
                    .get("objective")
                    .and_then(Value::as_f64)
                    .unwrap_or(f64::NEG_INFINITY);
                let bv = b
                    .get("objective")
                    .and_then(Value::as_f64)
                    .unwrap_or(f64::NEG_INFINITY);
                bv.partial_cmp(&av).unwrap_or(std::cmp::Ordering::Equal)
            });
            leaderboard.truncate(16);
            if objective > best_objective {
                best_objective = objective;
                best_payload = payload;
            }
            if hit {
                reached_target = true;
                break;
            }
        }
    }

    let persist_requested = params.persist_best.unwrap_or(false);
    let persist_ttl_sec = params.persist_ttl_sec.filter(|v| *v > 0);
    let persist_key = strategy_autotune_key(&state.redis_prefix, market_type);
    let mut persist_error: Option<String> = None;
    let mut persisted = false;
    if persist_requested {
        if best_payload.is_null() {
            persist_error = Some("best payload is null".to_string());
        } else {
            let save_doc = json!({
                "saved_at_ms": Utc::now().timestamp_millis(),
                "market_type": market_type,
                "target_win_rate_pct": target_win_rate,
                "window_trades": window_trades,
                "recent_lookback_minutes": recent_lookback_minutes,
                "recent_weight": recent_weight,
                "config": best_payload.get("config").cloned().unwrap_or(Value::Null),
                "best": best_payload.clone(),
                "leaderboard_top": leaderboard.iter().take(5).cloned().collect::<Vec<Value>>(),
            });
            if let Err(e) = write_key_value(&state, &persist_key, &save_doc, persist_ttl_sec).await {
                persist_error = Some(e.message);
            } else {
                persisted = true;
            }
        }
    }

    Ok(Json(json!({
        "market_type": market_type,
        "full_history": full_history,
        "lookback_minutes": lookback_minutes,
        "samples": samples.len(),
        "samples_train": train_samples.len(),
        "samples_validation": valid_samples.len(),
        "samples_recent": recent_samples.len(),
        "recent_lookback_minutes": recent_lookback_minutes,
        "recent_weight": recent_weight,
        "using_validation_split": using_validation,
        "target_win_rate_pct": target_win_rate,
        "window_trades": window_trades,
        "window_trades_validation": valid_window_trades,
        "window_trades_recent": valid_window_trades,
        "iterations_requested": iterations,
        "target_reached": reached_target,
        "persist": {
            "requested": persist_requested,
            "saved": persisted,
            "key": persist_key,
            "ttl_sec": persist_ttl_sec,
            "error": persist_error,
        },
        "best": best_payload,
        "leaderboard": leaderboard,
    })))
}

fn rows_from_json(v: Value) -> Vec<Value> {
    v.get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn row_i64(v: &Value, key: &str) -> Option<i64> {
    let val = v.get(key)?;
    if let Some(n) = val.as_i64() {
        return Some(n);
    }
    if let Some(u) = val.as_u64() {
        return i64::try_from(u).ok();
    }
    if let Some(f) = val.as_f64() {
        if f.is_finite() {
            return Some(f.round() as i64);
        }
    }
    val.as_str()?.trim().parse::<i64>().ok()
}

fn row_f64(v: &Value, key: &str) -> Option<f64> {
    let val = v.get(key)?;
    if let Some(f) = val.as_f64() {
        return Some(f);
    }
    if let Some(i) = val.as_i64() {
        return Some(i as f64);
    }
    if let Some(u) = val.as_u64() {
        return Some(u as f64);
    }
    val.as_str()?.trim().parse::<f64>().ok()
}

fn stride_downsample(rows: Vec<Value>, step: usize) -> Vec<Value> {
    if step <= 1 || rows.len() <= 2 {
        return rows;
    }
    let mut out = Vec::with_capacity(rows.len() / step + 2);
    for (idx, row) in rows.iter().enumerate() {
        if idx == 0 || idx + 1 == rows.len() || idx % step == 0 {
            out.push(row.clone());
        }
    }
    out
}

fn normalize_market_type(raw: &str) -> Option<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "5m" | "5min" | "5" => Some("5m"),
        "15m" | "15min" | "15" => Some("15m"),
        "30m" | "30min" | "30" => Some("30m"),
        "1h" | "60m" | "60min" => Some("1h"),
        "2h" | "120m" | "120min" => Some("2h"),
        "4h" | "240m" | "240min" => Some("4h"),
        _ => None,
    }
}

fn market_type_to_ms(market_type: &str) -> i64 {
    match market_type {
        "5m" => 5 * 60 * 1000,
        "15m" => 15 * 60 * 1000,
        "30m" => 30 * 60 * 1000,
        "1h" => 60 * 60 * 1000,
        "2h" => 2 * 60 * 60 * 1000,
        "4h" => 4 * 60 * 60 * 1000,
        _ => 0,
    }
}

fn infer_market_type_from_round_id(round_id: &str) -> Option<&'static str> {
    let mut parts = round_id.split('_');
    let _symbol = parts.next()?;
    let market_type = parts.next()?;
    normalize_market_type(market_type)
}

fn parse_round_start_ms(round_id: &str) -> Option<i64> {
    let raw = round_id.rsplit('_').next()?;
    raw.parse::<i64>().ok().filter(|v| *v > 0)
}

fn parse_end_date_ms(raw: Option<&str>) -> Option<i64> {
    let value = raw?.trim();
    if value.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

fn is_safe_round_id(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 128
        && v.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
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
}

