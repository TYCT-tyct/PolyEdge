use std::cmp::min;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tower_http::services::{ServeDir, ServeFile};

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
    market_type: Option<String>,
    lookback_minutes: Option<u32>,
    max_points: Option<u32>,
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
        .route("/api/chart", get(chart))
        .route("/api/chart/round", get(chart_round))
        .route("/api/rounds", get(rounds))
        .route("/api/rounds/available", get(rounds_available))
        .route("/api/heatmap", get(heatmap))
        .route("/api/accuracy_series", get(accuracy_series))
        .route("/api/strategy/paper", get(strategy_paper))
        .route("/api/strategy/full", get(strategy_full))
        .route("/api/strategy/optimize", get(strategy_optimize))
        .with_state(state);

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

    tracing::info!(bind = %bind, "forge api server started");
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
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

async fn chart(
    State(state): State<ApiState>,
    Query(params): Query<ChartQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let market_type = normalize_market_type(&params.market_type)
        .ok_or_else(|| ApiError::bad_request("invalid market_type"))?;
    let minutes = params.minutes.unwrap_or(30).min(7 * 24 * 60);
    let max_points = params.max_points.unwrap_or(1500).clamp(200, 20_000) as usize;

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
          {from_clause}
        ORDER BY ts_ireland_sample_ms ASC
        LIMIT 300000
        FORMAT JSON"
    );

    let round_query = format!(
        "SELECT
            round_id,
            start_ts_ms,
            end_ts_ms,
            target_price,
            toInt8(settle_price > target_price) AS outcome
        FROM polyedge_forge.rounds
        WHERE symbol='BTCUSDT'
          AND timeframe='{market_type}'
          {}
        ORDER BY end_ts_ms ASC
        LIMIT 4000
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

    Ok(Json(json!({
        "points": points,
        "rounds": rounds,
        "total_samples": total_samples,
        "downsampled": step > 1,
        "step": step,
    })))
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StrategySide {
    Up,
    Down,
}

impl StrategySide {
    fn as_str(self) -> &'static str {
        match self {
            StrategySide::Up => "UP",
            StrategySide::Down => "DOWN",
        }
    }

    fn dir(self) -> f64 {
        match self {
            StrategySide::Up => 1.0,
            StrategySide::Down => -1.0,
        }
    }
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

#[derive(Debug, Clone)]
struct StrategyPosition {
    side: StrategySide,
    entry_price_cents: f64,
    entry_ts_ms: i64,
    entry_round_id: String,
    entry_score: f64,
    entry_remaining_ms: i64,
    peak_pnl_cents: f64,
    reverse_streak: usize,
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

impl Default for StrategyRuntimeConfig {
    fn default() -> Self {
        Self {
            entry_threshold_base: 0.68,
            entry_threshold_cap: 0.88,
            spread_limit_prob: 0.024,
            entry_edge_prob: 0.040,
            entry_min_potential_cents: 8.0,
            entry_max_price_cents: 90.0,
            min_hold_ms: 25_000,
            stop_loss_cents: 10.0,
            reverse_signal_threshold: -0.30,
            reverse_signal_ticks: 5,
            trail_activate_profit_cents: 24.0,
            trail_drawdown_cents: 11.0,
            take_profit_near_max_cents: 93.0,
            endgame_take_profit_cents: 88.0,
            endgame_remaining_ms: 30_000,
            liquidity_widen_prob: 0.070,
            cooldown_ms: 8_000,
            max_entries_per_round: 2,
            max_exec_spread_cents: 3.4,
            slippage_cents_per_side: 0.25,
            fee_cents_per_side: 0.35,
            emergency_wide_spread_penalty_ratio: 0.50,
        }
    }
}

fn tanh_norm(v: f64, scale: f64) -> f64 {
    if !v.is_finite() || !scale.is_finite() || scale <= 0.0 {
        return 0.0;
    }
    (v / scale).tanh()
}

fn side_bid(sample: &StrategySample, side: StrategySide) -> f64 {
    match side {
        StrategySide::Up => sample.bid_yes,
        StrategySide::Down => sample.bid_no,
    }
}

fn side_ask(sample: &StrategySample, side: StrategySide) -> f64 {
    match side {
        StrategySide::Up => sample.ask_yes,
        StrategySide::Down => sample.ask_no,
    }
}

fn side_spread(sample: &StrategySample, side: StrategySide) -> f64 {
    match side {
        StrategySide::Up => sample.spread_up,
        StrategySide::Down => sample.spread_down,
    }
}

fn confirm_direction(score_hist: &VecDeque<f64>, current_score: f64) -> bool {
    if score_hist.len() < 2 || !current_score.is_finite() {
        return false;
    }
    let sgn = if current_score > 0.0 {
        1
    } else if current_score < 0.0 {
        -1
    } else {
        0
    };
    if sgn == 0 {
        return false;
    }
    let mut same = 0usize;
    for v in score_hist.iter().rev().take(2) {
        let vsgn = if *v > 0.0 {
            1
        } else if *v < 0.0 {
            -1
        } else {
            0
        };
        if vsgn == sgn {
            same += 1;
        }
    }
    same == 2
}

fn rolling_vol_absdiff(p_hist: &VecDeque<f64>) -> f64 {
    if p_hist.len() < 2 {
        return 0.0;
    }
    let mut sum = 0.0;
    let mut n = 0usize;
    let mut prev: Option<f64> = None;
    for p in p_hist {
        if let Some(v) = prev {
            sum += (p - v).abs();
            n += 1;
        }
        prev = Some(*p);
    }
    if n == 0 {
        return 0.0;
    }
    sum / n as f64
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

fn compute_score(
    current: &StrategySample,
    p_hist: &VecDeque<f64>,
    score_hist: &VecDeque<f64>,
    cfg: &StrategyRuntimeConfig,
) -> (f64, f64, bool) {
    let p_now = current.p_up;
    let p_mom3 = if p_hist.len() >= 4 {
        p_now - p_hist[p_hist.len() - 4]
    } else {
        0.0
    };
    let p_mom8 = if p_hist.len() >= 9 {
        p_now - p_hist[p_hist.len() - 9]
    } else {
        0.0
    };
    let dlt = current.delta_pct;
    let vel = current.velocity;
    let acc = current.acceleration;

    let score = 0.34 * tanh_norm(p_mom3, 0.028)
        + 0.27 * tanh_norm(dlt, 0.07)
        + 0.21 * tanh_norm(p_mom8, 0.045)
        + 0.12 * tanh_norm(vel, 3.6)
        + 0.06 * tanh_norm(acc, 16.0);

    let vol = rolling_vol_absdiff(p_hist);
    let entry_threshold = (cfg.entry_threshold_base + vol * 3.6 + current.spread_mid * 1.3)
        .clamp(cfg.entry_threshold_base, cfg.entry_threshold_cap);
    let confirmed = confirm_direction(score_hist, score);
    (score.clamp(-1.0, 1.0), entry_threshold, confirmed)
}

#[derive(Debug, Clone, Copy)]
struct StrategySignal {
    score: f64,
    entry_threshold: f64,
    confirmed: bool,
    p_fair_up: f64,
    edge_prob: f64,
}

fn compute_signal(
    current: &StrategySample,
    p_hist: &VecDeque<f64>,
    score_hist: &VecDeque<f64>,
    cfg: &StrategyRuntimeConfig,
) -> StrategySignal {
    let (base_score, entry_threshold, confirmed) = compute_score(current, p_hist, score_hist, cfg);
    let micro_bias = 0.08 * tanh_norm(current.delta_pct, 0.09)
        + 0.04 * tanh_norm(current.velocity, 5.0)
        - 0.03 * (current.spread_mid / 0.03).clamp(0.0, 2.0);
    let p_fair_up = (0.5 + 0.43 * base_score + micro_bias).clamp(0.005, 0.995);
    let edge_prob = p_fair_up - current.p_up.clamp(0.0, 1.0);
    let edge_score = tanh_norm(edge_prob, (cfg.entry_edge_prob * 1.4).max(0.01));
    let score = (0.62 * base_score + 0.38 * edge_score).clamp(-1.0, 1.0);
    StrategySignal {
        score,
        entry_threshold,
        confirmed,
        p_fair_up,
        edge_prob,
    }
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

fn run_strategy_simulation(
    samples: &[StrategySample],
    cfg: &StrategyRuntimeConfig,
    max_trades: usize,
) -> StrategySimulationResult {
    let mut p_hist = VecDeque::<f64>::with_capacity(24);
    let mut score_hist = VecDeque::<f64>::with_capacity(24);
    let mut position: Option<StrategyPosition> = None;
    let mut trades: Vec<Value> = Vec::new();
    let mut entries_by_round: HashMap<String, usize> = HashMap::new();
    let mut last_exit_ts_ms: i64 = 0;

    let mut last_round = String::new();
    let mut trade_id = 1_i64;
    let mut last_score = 0.0_f64;
    let mut last_entry_threshold = cfg.entry_threshold_base;
    let mut last_confirmed = false;
    let mut last_side = "WAIT";
    let mut last_p_fair_up = 0.5_f64;
    let mut last_edge_prob = 0.0_f64;
    let mut blocked_exits = 0usize;
    let mut emergency_wide_exit_count = 0usize;
    let mut execution_penalty_cents_total = 0.0_f64;

    for sample in samples {
        if !last_round.is_empty() && sample.round_id != last_round {
            p_hist.clear();
            score_hist.clear();
        }
        let signal = compute_signal(sample, &p_hist, &score_hist, cfg);
        let score = signal.score;
        let entry_thr = signal.entry_threshold;
        last_score = score;
        last_entry_threshold = entry_thr;
        last_confirmed = signal.confirmed;
        last_p_fair_up = signal.p_fair_up;
        last_edge_prob = signal.edge_prob;
        last_round = sample.round_id.clone();

        let side = if signal.edge_prob >= 0.0 {
            StrategySide::Up
        } else {
            StrategySide::Down
        };
        last_side = side.as_str();

        if let Some(pos) = position.as_mut() {
            let bid_cents_raw = side_bid(sample, pos.side) * 100.0;
            let spread_side_cents = side_spread(sample, pos.side) * 100.0;
            let bid_cents_exec =
                bid_cents_raw - cfg.slippage_cents_per_side - cfg.fee_cents_per_side;
            let pnl = bid_cents_exec - pos.entry_price_cents;
            if pnl > pos.peak_pnl_cents {
                pos.peak_pnl_cents = pnl;
            }
            let drawdown = pos.peak_pnl_cents - pnl;
            let held_ms = (sample.ts_ms - pos.entry_ts_ms).max(0);
            let trend_signed = score * pos.side.dir();
            if trend_signed <= cfg.reverse_signal_threshold {
                pos.reverse_streak = pos.reverse_streak.saturating_add(1);
            } else {
                pos.reverse_streak = 0;
            }
            let can_exit_now = held_ms >= cfg.min_hold_ms;
            let force_reverse_exit = pos.reverse_streak >= cfg.reverse_signal_ticks
                && trend_signed <= cfg.reverse_signal_threshold * 1.25;
            let mut exit_reason: Option<&str> = None;
            if sample.round_id != pos.entry_round_id {
                exit_reason = Some("round_rollover");
            } else if bid_cents_exec >= cfg.take_profit_near_max_cents {
                exit_reason = Some("near_max_take_profit");
            } else if sample.remaining_ms <= cfg.endgame_remaining_ms
                && bid_cents_exec >= cfg.endgame_take_profit_cents
            {
                exit_reason = Some("endgame_take_profit");
            } else if can_exit_now && pnl <= -cfg.stop_loss_cents {
                exit_reason = Some("stop_loss");
            } else if force_reverse_exit
                || (can_exit_now && pos.reverse_streak >= cfg.reverse_signal_ticks)
            {
                exit_reason = Some("signal_reverse");
            } else if can_exit_now
                && pos.peak_pnl_cents >= cfg.trail_activate_profit_cents
                && drawdown >= cfg.trail_drawdown_cents
            {
                exit_reason = Some("trail_drawdown");
            } else if can_exit_now && side_spread(sample, pos.side) >= cfg.liquidity_widen_prob {
                exit_reason = Some("liquidity_widen");
            }

            if let Some(reason) = exit_reason {
                let emergency = matches!(
                    reason,
                    "round_rollover" | "stop_loss" | "endgame_take_profit"
                );
                let can_fill = spread_side_cents <= cfg.max_exec_spread_cents;
                if !can_fill && !emergency {
                    blocked_exits = blocked_exits.saturating_add(1);
                    continue;
                }
                let mut exit_price_cents = bid_cents_exec;
                if !can_fill {
                    let extra_penalty = (spread_side_cents - cfg.max_exec_spread_cents).max(0.0)
                        * cfg.emergency_wide_spread_penalty_ratio;
                    exit_price_cents -= extra_penalty;
                    execution_penalty_cents_total += extra_penalty;
                    emergency_wide_exit_count = emergency_wide_exit_count.saturating_add(1);
                }
                let net_pnl = exit_price_cents - pos.entry_price_cents;
                let duration_s = ((sample.ts_ms - pos.entry_ts_ms).max(0) as f64) / 1000.0;
                trades.push(json!({
                    "id": trade_id,
                    "side": pos.side.as_str(),
                    "entry_ts_ms": pos.entry_ts_ms,
                    "exit_ts_ms": sample.ts_ms,
                    "entry_remaining_ms": pos.entry_remaining_ms,
                    "exit_remaining_ms": sample.remaining_ms,
                    "entry_price_cents": pos.entry_price_cents,
                    "exit_price_cents": exit_price_cents,
                    "pnl_cents": net_pnl,
                    "peak_pnl_cents": pos.peak_pnl_cents,
                    "duration_s": duration_s,
                    "entry_score": pos.entry_score,
                    "exit_score": score,
                    "entry_reason": "p_fair_edge",
                    "exit_reason": reason,
                    "exec_fill_ok": can_fill,
                    "exit_spread_cents": spread_side_cents,
                }));
                trade_id += 1;
                position = None;
                last_exit_ts_ms = sample.ts_ms;
                if reason == "signal_reverse" {
                    let entry_price = side_ask(sample, side) * 100.0;
                    let entry_potential = (99.0 - entry_price).max(0.0);
                    let round_entries =
                        entries_by_round.get(&sample.round_id).copied().unwrap_or(0);
                    let can_flip = score.abs() >= entry_thr
                        && sample.spread_mid <= cfg.spread_limit_prob
                        && entry_price <= cfg.entry_max_price_cents
                        && entry_potential >= cfg.entry_min_potential_cents
                        && round_entries < cfg.max_entries_per_round;
                    if can_flip {
                        position = Some(StrategyPosition {
                            side,
                            entry_price_cents: entry_price,
                            entry_ts_ms: sample.ts_ms,
                            entry_round_id: sample.round_id.clone(),
                            entry_score: score,
                            entry_remaining_ms: sample.remaining_ms,
                            peak_pnl_cents: 0.0,
                            reverse_streak: 0,
                        });
                        entries_by_round.insert(sample.round_id.clone(), round_entries + 1);
                    }
                }
            }
        } else {
            let entry_price_raw = side_ask(sample, side) * 100.0;
            let entry_spread_cents = side_spread(sample, side) * 100.0;
            let entry_price =
                entry_price_raw + cfg.slippage_cents_per_side + cfg.fee_cents_per_side;
            let entry_potential = (99.0 - entry_price).max(0.0);
            let can_enter = signal.confirmed
                && score.abs() >= entry_thr
                && signal.edge_prob.abs() >= cfg.entry_edge_prob
                && sample.spread_mid <= cfg.spread_limit_prob
                && entry_spread_cents <= cfg.max_exec_spread_cents
                && entry_price <= cfg.entry_max_price_cents
                && entry_potential >= cfg.entry_min_potential_cents;
            let round_entries = entries_by_round.get(&sample.round_id).copied().unwrap_or(0);
            let cooled_down = last_exit_ts_ms <= 0
                || sample.ts_ms.saturating_sub(last_exit_ts_ms) >= cfg.cooldown_ms;
            if can_enter && cooled_down && round_entries < cfg.max_entries_per_round {
                position = Some(StrategyPosition {
                    side,
                    entry_price_cents: entry_price,
                    entry_ts_ms: sample.ts_ms,
                    entry_round_id: sample.round_id.clone(),
                    entry_score: score,
                    entry_remaining_ms: sample.remaining_ms,
                    peak_pnl_cents: 0.0,
                    reverse_streak: 0,
                });
                entries_by_round.insert(sample.round_id.clone(), round_entries + 1);
            }
        }

        p_hist.push_back(sample.p_up);
        score_hist.push_back(score);
        while p_hist.len() > 24 {
            p_hist.pop_front();
        }
        while score_hist.len() > 24 {
            score_hist.pop_front();
        }
    }

    if let (Some(pos), Some(last)) = (position.as_ref(), samples.last()) {
        let bid_cents_raw = side_bid(last, pos.side) * 100.0;
        let spread_side_cents = side_spread(last, pos.side) * 100.0;
        let mut bid_cents = bid_cents_raw - cfg.slippage_cents_per_side - cfg.fee_cents_per_side;
        let can_fill = spread_side_cents <= cfg.max_exec_spread_cents;
        if !can_fill {
            let extra_penalty = (spread_side_cents - cfg.max_exec_spread_cents).max(0.0)
                * cfg.emergency_wide_spread_penalty_ratio;
            bid_cents -= extra_penalty;
            execution_penalty_cents_total += extra_penalty;
            emergency_wide_exit_count = emergency_wide_exit_count.saturating_add(1);
        }
        let pnl = bid_cents - pos.entry_price_cents;
        trades.push(json!({
            "id": trade_id,
            "side": pos.side.as_str(),
            "entry_ts_ms": pos.entry_ts_ms,
            "exit_ts_ms": last.ts_ms,
            "entry_remaining_ms": pos.entry_remaining_ms,
            "exit_remaining_ms": last.remaining_ms,
            "entry_price_cents": pos.entry_price_cents,
            "exit_price_cents": bid_cents,
            "pnl_cents": pnl,
            "peak_pnl_cents": pos.peak_pnl_cents.max(pnl),
            "duration_s": ((last.ts_ms - pos.entry_ts_ms).max(0) as f64) / 1000.0,
            "entry_score": pos.entry_score,
            "exit_score": last_score,
            "entry_reason": "p_fair_edge",
            "exit_reason": "window_end",
            "exec_fill_ok": can_fill,
            "exit_spread_cents": spread_side_cents,
        }));
    }

    let mut total_pnl = 0.0_f64;
    let mut wins = 0usize;
    let mut equity = 0.0_f64;
    let mut peak_equity = 0.0_f64;
    let mut max_drawdown = 0.0_f64;
    let mut total_duration_s = 0.0_f64;
    for t in &trades {
        let pnl = row_f64(t, "pnl_cents").unwrap_or(0.0);
        total_duration_s += row_f64(t, "duration_s").unwrap_or(0.0);
        total_pnl += pnl;
        if pnl > 0.0 {
            wins += 1;
        }
        equity += pnl;
        if equity > peak_equity {
            peak_equity = equity;
        }
        let dd = peak_equity - equity;
        if dd > max_drawdown {
            max_drawdown = dd;
        }
    }
    let trade_count = trades.len();
    let win_rate_pct = if trade_count > 0 {
        (wins as f64) * 100.0 / trade_count as f64
    } else {
        0.0
    };
    let avg_pnl = if trade_count > 0 {
        total_pnl / trade_count as f64
    } else {
        0.0
    };
    let avg_duration_s = if trade_count > 0 {
        total_duration_s / trade_count as f64
    } else {
        0.0
    };
    let all_trade_pnls: Vec<f64> = trades
        .iter()
        .map(|t| row_f64(t, "pnl_cents").unwrap_or(0.0))
        .collect();
    let max_profit_trade_cents = all_trade_pnls
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);

    let trades_view = if trades.len() > max_trades {
        trades[trades.len() - max_trades..].to_vec()
    } else {
        trades
    };

    let current = samples
        .last()
        .map(|last| {
            let action = if position.is_some() {
                "HOLD"
            } else if last_confirmed
                && last_score.abs() >= last_entry_threshold
                && last.spread_mid <= cfg.spread_limit_prob
            {
                if last_score >= 0.0 {
                    "ENTER_UP"
                } else {
                    "ENTER_DOWN"
                }
            } else if last.spread_mid > 0.05 {
                "RISK_OFF"
            } else {
                "WAIT"
            };
            json!({
                "timestamp_ms": last.ts_ms,
                "round_id": last.round_id,
                "remaining_s": (last.remaining_ms as f64 / 1000.0),
                "score": last_score,
                "entry_threshold": last_entry_threshold,
                "confirmed": last_confirmed,
                "suggested_side": last_side,
                "suggested_action": action,
                "confidence": (last_score.abs() / last_entry_threshold.max(1e-6)).clamp(0.0, 2.0) * 0.5,
                "p_up_pct": last.p_up * 100.0,
                "p_fair_up_pct": last_p_fair_up * 100.0,
                "edge_prob_pct": last_edge_prob * 100.0,
                "delta_pct": last.delta_pct,
                "spread_up_cents": last.spread_up * 100.0,
                "spread_down_cents": last.spread_down * 100.0,
            })
        })
        .unwrap_or(Value::Null);

    StrategySimulationResult {
        current,
        trades: trades_view,
        all_trade_pnls,
        trade_count,
        win_rate_pct,
        avg_pnl_cents: avg_pnl,
        avg_duration_s,
        total_pnl_cents: total_pnl,
        max_drawdown_cents: max_drawdown,
        max_profit_trade_cents: if max_profit_trade_cents.is_finite() {
            max_profit_trade_cents
        } else {
            0.0
        },
        blocked_exits,
        emergency_wide_exit_count,
        execution_penalty_cents_total,
    }
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
    let mut cfg = StrategyRuntimeConfig::default();
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
        cfg.max_entries_per_round = v.clamp(1, 8) as usize;
    }
    if cfg.entry_threshold_cap < cfg.entry_threshold_base {
        cfg.entry_threshold_cap = cfg.entry_threshold_base;
    }
    let market_type = if let Some(mt) = params.market_type.as_deref() {
        normalize_market_type(mt).ok_or_else(|| ApiError::bad_request("invalid market_type"))?
    } else {
        "5m"
    };
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
            "lookback_minutes": lookback_minutes,
            "samples": samples.len(),
            "current": Value::Null,
            "summary": {
                "trade_count": 0,
                "win_rate_pct": 0.0,
                "avg_pnl_cents": 0.0,
                "avg_duration_s": 0.0,
                "total_pnl_cents": 0.0,
                "max_drawdown_cents": 0.0,
            },
            "trades": [],
        })));
    }

    let run = run_strategy_simulation(&samples, &cfg, max_trades);

    Ok(Json(json!({
        "market_type": market_type,
        "lookback_minutes": lookback_minutes,
        "full_history": full_history,
        "samples": samples.len(),
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
            "max_drawdown_cents": run.max_drawdown_cents,
            "max_profit_trade_cents": run.max_profit_trade_cents,
            "blocked_exits": run.blocked_exits,
            "emergency_wide_exit_count": run.emergency_wide_exit_count,
            "execution_penalty_cents_total": run.execution_penalty_cents_total,
        },
        "trades": run.trades,
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
    cfg.max_entries_per_round = cfg.max_entries_per_round.clamp(1, 8);
    cfg.max_exec_spread_cents = cfg.max_exec_spread_cents.clamp(0.5, 12.0);
    cfg.slippage_cents_per_side = cfg.slippage_cents_per_side.clamp(0.0, 4.0);
    cfg.fee_cents_per_side = cfg.fee_cents_per_side.clamp(0.0, 4.0);
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
    let shortfall = (target_win_rate - rs.latest_win_rate_pct).max(0.0);
    let target_hit = rs.windows > 0
        && rs.latest_win_rate_pct >= target_win_rate
        && run.avg_pnl_cents > 0.0
        && run.trade_count >= window_trades;
    let objective = if rs.windows > 0 {
        rs.latest_win_rate_pct * 3.2
            + rs.avg_win_rate_pct * 1.8
            + rs.min_win_rate_pct * 0.8
            + run.avg_pnl_cents * 2.2
            + run.total_pnl_cents.max(0.0) * 0.02
            + run.max_profit_trade_cents.max(0.0) * 0.6
            - run.max_drawdown_cents * 0.02
            - run.execution_penalty_cents_total * 0.4
            - run.blocked_exits as f64 * 18.0
            - shortfall * shortfall * 1.6
            + if target_hit { 160.0 } else { 0.0 }
    } else {
        run.win_rate_pct + run.avg_pnl_cents - 180.0
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
    let pool: Vec<(String, StrategyRuntimeConfig)> = build_strategy_arms(base_cfg, max_arms);
    let mut seed = 0xC0DEC0DE1234ABCDu64;

    let mut leaderboard: Vec<Value> = Vec::new();
    let mut best_objective = f64::NEG_INFINITY;
    let mut best_payload = Value::Null;
    let mut reached_target = false;

    // warmup evaluate initial arms
    for (name, cfg) in &pool {
        let run = run_strategy_simulation(&samples, cfg, max_trades);
        let (rs, objective, hit) = score_with_rolling(&run, window_trades, target_win_rate);
        let payload = json!({
            "name": name,
            "objective": objective,
            "rolling_window": rolling_stats_json(rs),
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
        });
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
                let mut c = base_cfg;
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("entry_threshold_base"))
                    .and_then(Value::as_f64)
                {
                    c.entry_threshold_base = v;
                }
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("entry_threshold_cap"))
                    .and_then(Value::as_f64)
                {
                    c.entry_threshold_cap = v;
                }
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("spread_limit_prob"))
                    .and_then(Value::as_f64)
                {
                    c.spread_limit_prob = v;
                }
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("entry_edge_prob"))
                    .and_then(Value::as_f64)
                {
                    c.entry_edge_prob = v;
                }
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("entry_min_potential_cents"))
                    .and_then(Value::as_f64)
                {
                    c.entry_min_potential_cents = v;
                }
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("entry_max_price_cents"))
                    .and_then(Value::as_f64)
                {
                    c.entry_max_price_cents = v;
                }
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("min_hold_ms"))
                    .and_then(Value::as_i64)
                {
                    c.min_hold_ms = v;
                }
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("stop_loss_cents"))
                    .and_then(Value::as_f64)
                {
                    c.stop_loss_cents = v;
                }
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("reverse_signal_threshold"))
                    .and_then(Value::as_f64)
                {
                    c.reverse_signal_threshold = v;
                }
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("reverse_signal_ticks"))
                    .and_then(Value::as_u64)
                {
                    c.reverse_signal_ticks = v as usize;
                }
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("trail_activate_profit_cents"))
                    .and_then(Value::as_f64)
                {
                    c.trail_activate_profit_cents = v;
                }
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("trail_drawdown_cents"))
                    .and_then(Value::as_f64)
                {
                    c.trail_drawdown_cents = v;
                }
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("take_profit_near_max_cents"))
                    .and_then(Value::as_f64)
                {
                    c.take_profit_near_max_cents = v;
                }
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("endgame_take_profit_cents"))
                    .and_then(Value::as_f64)
                {
                    c.endgame_take_profit_cents = v;
                }
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("endgame_remaining_ms"))
                    .and_then(Value::as_i64)
                {
                    c.endgame_remaining_ms = v;
                }
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("liquidity_widen_prob"))
                    .and_then(Value::as_f64)
                {
                    c.liquidity_widen_prob = v;
                }
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("cooldown_ms"))
                    .and_then(Value::as_i64)
                {
                    c.cooldown_ms = v;
                }
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("max_entries_per_round"))
                    .and_then(Value::as_u64)
                {
                    c.max_entries_per_round = v as usize;
                }
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("max_exec_spread_cents"))
                    .and_then(Value::as_f64)
                {
                    c.max_exec_spread_cents = v;
                }
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("slippage_cents_per_side"))
                    .and_then(Value::as_f64)
                {
                    c.slippage_cents_per_side = v;
                }
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("fee_cents_per_side"))
                    .and_then(Value::as_f64)
                {
                    c.fee_cents_per_side = v;
                }
                if let Some(v) = parent
                    .get("config")
                    .and_then(|v| v.get("emergency_wide_spread_penalty_ratio"))
                    .and_then(Value::as_f64)
                {
                    c.emergency_wide_spread_penalty_ratio = v;
                }
                c
            } else {
                base_cfg
            };
            let candidate_cfg = mutate_cfg(&parent_cfg, &mut seed, iter, iterations);
            let run = run_strategy_simulation(&samples, &candidate_cfg, max_trades);
            let (rs, objective, hit) = score_with_rolling(&run, window_trades, target_win_rate);
            let payload = json!({
                "name": format!("iter_{iter}"),
                "objective": objective,
                "rolling_window": rolling_stats_json(rs),
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
                "config": {
                    "entry_threshold_base": candidate_cfg.entry_threshold_base,
                    "entry_threshold_cap": candidate_cfg.entry_threshold_cap,
                    "spread_limit_prob": candidate_cfg.spread_limit_prob,
                    "entry_edge_prob": candidate_cfg.entry_edge_prob,
                    "entry_min_potential_cents": candidate_cfg.entry_min_potential_cents,
                    "entry_max_price_cents": candidate_cfg.entry_max_price_cents,
                    "min_hold_ms": candidate_cfg.min_hold_ms,
                    "stop_loss_cents": candidate_cfg.stop_loss_cents,
                    "reverse_signal_threshold": candidate_cfg.reverse_signal_threshold,
                    "reverse_signal_ticks": candidate_cfg.reverse_signal_ticks,
                    "trail_activate_profit_cents": candidate_cfg.trail_activate_profit_cents,
                    "trail_drawdown_cents": candidate_cfg.trail_drawdown_cents,
                    "take_profit_near_max_cents": candidate_cfg.take_profit_near_max_cents,
                    "endgame_take_profit_cents": candidate_cfg.endgame_take_profit_cents,
                    "endgame_remaining_ms": candidate_cfg.endgame_remaining_ms,
                    "liquidity_widen_prob": candidate_cfg.liquidity_widen_prob,
                    "cooldown_ms": candidate_cfg.cooldown_ms,
                    "max_entries_per_round": candidate_cfg.max_entries_per_round,
                    "max_exec_spread_cents": candidate_cfg.max_exec_spread_cents,
                    "slippage_cents_per_side": candidate_cfg.slippage_cents_per_side,
                    "fee_cents_per_side": candidate_cfg.fee_cents_per_side,
                    "emergency_wide_spread_penalty_ratio": candidate_cfg.emergency_wide_spread_penalty_ratio,
                },
            });
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

    Ok(Json(json!({
        "market_type": market_type,
        "full_history": full_history,
        "lookback_minutes": lookback_minutes,
        "samples": samples.len(),
        "target_win_rate_pct": target_win_rate,
        "window_trades": window_trades,
        "iterations_requested": iterations,
        "target_reached": reached_target,
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
