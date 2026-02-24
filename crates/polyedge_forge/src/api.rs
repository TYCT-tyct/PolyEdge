use std::cmp::min;
use std::collections::HashMap;
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
        .route("/api/history/{symbol}/{timeframe}", get(history_symbol_timeframe))
        .route("/api/stats", get(stats))
        .route("/api/chart", get(chart))
        .route("/api/chart/round", get(chart_round))
        .route("/api/rounds", get(rounds))
        .route("/api/rounds/available", get(rounds_available))
        .route("/api/heatmap", get(heatmap))
        .route("/api/accuracy_series", get(accuracy_series))
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
        if socket.send(Message::Text(text.clone().into())).await.is_err() {
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
        return Ok(
            row.filter(|v| is_live_snapshot_fresh(v, timeframe, now_ms))
        );
    }

    let key_direct = format!(
        "{}:snapshot:latest:{}:{}",
        state.redis_prefix,
        symbol.to_ascii_uppercase(),
        timeframe
    );
    if let Some(v) = read_key_value(state, &key_direct).await? {
        if is_live_snapshot_fresh(&v, timeframe, now_ms) {
            return Ok(Some(v));
        }
    }

    let tf_key = format!("{}:snapshot:latest:tf:{}", state.redis_prefix, timeframe);
    let Some(arr_val) = read_key_value(state, &tf_key).await? else {
        return Ok(None);
    };
    let Some(arr) = arr_val.as_array() else {
        return Ok(None);
    };

    let mut best: Option<Value> = None;
    let mut best_ts = i64::MIN;
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
        let ts = row
            .get("ts_ireland_sample_ms")
            .and_then(Value::as_i64)
            .unwrap_or(i64::MIN);
        if ts > best_ts && is_live_snapshot_fresh(row, timeframe, now_ms) {
            best_ts = ts;
            best = Some(row.clone());
        }
    }

    if best.is_some() {
        return Ok(best);
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
        ORDER BY ts_ireland_sample_ms DESC
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

    let mid_yes = snapshot
        .get("mid_yes")
        .and_then(Value::as_f64)
        .or_else(|| snapshot.get("mid_yes_smooth").and_then(Value::as_f64));
    let mid_no = snapshot
        .get("mid_no")
        .and_then(Value::as_f64)
        .or_else(|| snapshot.get("mid_no_smooth").and_then(Value::as_f64));
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
    let best_bid_up = raw_bid_yes.or_else(|| mid_yes.map(|m| (m - yes_spread * 0.5).clamp(0.0, 1.0)));
    let best_ask_up = raw_ask_yes.or_else(|| mid_yes.map(|m| (m + yes_spread * 0.5).clamp(0.0, 1.0)));
    let best_bid_down = raw_bid_no.or_else(|| mid_no.map(|m| (m - no_spread * 0.5).clamp(0.0, 1.0)));
    let best_ask_down = raw_ask_no.or_else(|| mid_no.map(|m| (m + no_spread * 0.5).clamp(0.0, 1.0)));

    json!({
        "timestamp_ms": snapshot.get("ts_ireland_sample_ms").and_then(Value::as_i64),
        "round_id": round_id,
        "market_type": market_type,
        "btc_price": snapshot.get("binance_price").and_then(Value::as_f64),
        "target_price": snapshot.get("target_price").and_then(Value::as_f64),
        "delta_pct": snapshot.get("delta_pct_smooth").and_then(Value::as_f64).or_else(|| snapshot.get("delta_pct").and_then(Value::as_f64)),
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
            label_up,
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
            sum(label_up) AS up_count,
            count() - sum(label_up) AS down_count
        FROM polyedge_forge.rounds
        WHERE symbol='BTCUSDT' AND timeframe IN ('5m','15m')
        FORMAT JSON";
    let accuracy_query = "SELECT
            countIf(isNotNull(s.eval_mid_up)) AS market_accuracy_n,
            avgIf(toFloat64((s.eval_mid_up >= 0.5) = (r.label_up = 1)), isNotNull(s.eval_mid_up)) AS market_accuracy
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
            toInt8(label_up) AS outcome
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
            toInt8(label_up) AS outcome
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
        return Ok(Json(json!({"points":[],"round":Value::Null,"total_samples":0,"step":1})));
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
            toInt8(r.label_up) AS outcome,
            if(r.target_price > 0, (r.settle_price - r.target_price) / r.target_price * 100.0, NULL) AS delta_pct,
            ifNull(s.close_mid_up, 0.5) * 100.0 AS mkt_price_cents
        FROM polyedge_forge.rounds r
        LEFT JOIN (
            SELECT
                round_id,
                argMinIf(coalesce(mid_yes, mid_yes_smooth), remaining_ms, remaining_ms >= 0) AS close_mid_up
            FROM polyedge_forge.snapshot_100ms
            WHERE symbol='BTCUSDT'
              AND timeframe='{market_type}'
            GROUP BY round_id
        ) s ON r.round_id = s.round_id
        WHERE r.symbol='BTCUSDT'
          AND r.timeframe='{market_type}'
        ORDER BY end_ts_ms DESC
        LIMIT {limit}
        FORMAT JSON"
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
    let rolling_window = if market_type == "5m" { 40usize } else { 20usize };
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
            toInt8(r.label_up) AS outcome,
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
    let mut series: Vec<Value> = Vec::with_capacity(((end_aligned - start_aligned) / bucket_ms + 1).max(0) as usize);
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
