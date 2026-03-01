use super::*;
use std::sync::OnceLock;

const CLICKHOUSE_CONNECT_TIMEOUT_MS: u64 = 1_500;
const CLICKHOUSE_REQUEST_TIMEOUT_MS: u64 = 8_000;
const CLICKHOUSE_QUERY_RETRY_ATTEMPTS_DEFAULT: usize = 3;
const CLICKHOUSE_QUERY_RETRY_ATTEMPTS_MIN: usize = 1;
const CLICKHOUSE_QUERY_RETRY_ATTEMPTS_MAX: usize = 6;
const CLICKHOUSE_QUERY_RETRY_BACKOFF_MS_DEFAULT: u64 = 180;
const CLICKHOUSE_QUERY_RETRY_BACKOFF_MS_MIN: u64 = 50;
const CLICKHOUSE_QUERY_RETRY_BACKOFF_MS_MAX: u64 = 2_000;

fn clickhouse_http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(CLICKHOUSE_CONNECT_TIMEOUT_MS))
            .timeout(Duration::from_millis(CLICKHOUSE_REQUEST_TIMEOUT_MS))
            .pool_max_idle_per_host(8)
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .expect("build clickhouse reqwest client")
    })
}

fn clickhouse_query_retry_attempts() -> usize {
    std::env::var("FORGE_CLICKHOUSE_QUERY_RETRY_ATTEMPTS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(CLICKHOUSE_QUERY_RETRY_ATTEMPTS_DEFAULT)
        .clamp(
            CLICKHOUSE_QUERY_RETRY_ATTEMPTS_MIN,
            CLICKHOUSE_QUERY_RETRY_ATTEMPTS_MAX,
        )
}

fn clickhouse_query_retry_backoff_ms() -> u64 {
    std::env::var("FORGE_CLICKHOUSE_QUERY_RETRY_BACKOFF_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(CLICKHOUSE_QUERY_RETRY_BACKOFF_MS_DEFAULT)
        .clamp(
            CLICKHOUSE_QUERY_RETRY_BACKOFF_MS_MIN,
            CLICKHOUSE_QUERY_RETRY_BACKOFF_MS_MAX,
        )
}

fn redis_manager_optional(state: &ApiState) -> Option<redis::aio::ConnectionManager> {
    state.redis_manager.clone()
}

fn redis_manager_required(state: &ApiState) -> Result<redis::aio::ConnectionManager, ApiError> {
    redis_manager_optional(state).ok_or_else(|| ApiError::internal("redis not configured"))
}

pub(super) async fn read_key_value(state: &ApiState, key: &str) -> Result<Option<Value>, ApiError> {
    let Some(mut conn) = redis_manager_optional(state) else {
        return Ok(None);
    };

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

pub(super) async fn write_key_value(
    state: &ApiState,
    key: &str,
    value: &Value,
    ttl_sec: Option<u32>,
) -> Result<(), ApiError> {
    let mut conn = redis_manager_required(state)?;
    let payload = serde_json::to_string(value)
        .map_err(|e| ApiError::internal(format!("redis payload serialize failed: {}", e)))?;
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

pub(super) async fn delete_key(state: &ApiState, key: &str) -> Result<(), ApiError> {
    let Some(mut conn) = redis_manager_optional(state) else {
        return Ok(());
    };
    let _: usize = conn
        .del(key)
        .await
        .map_err(|e| ApiError::internal(format!("redis del failed: {}", e)))?;
    Ok(())
}

pub(super) async fn read_key_json(state: &ApiState, key: &str) -> Result<Json<Value>, ApiError> {
    if state.redis_client.is_none() {
        return Err(ApiError::internal("redis not configured"));
    }
    let parsed = read_key_value(state, key).await?;
    let Some(parsed) = parsed else {
        return Err(ApiError::not_found(format!("key not found: {}", key)));
    };
    Ok(Json(parsed))
}

pub(super) async fn query_clickhouse_json(ch_url: &str, query: &str) -> Result<Value, ApiError> {
    let attempts = clickhouse_query_retry_attempts();
    let base_backoff_ms = clickhouse_query_retry_backoff_ms();
    for attempt in 1..=attempts {
        let resp = clickhouse_http_client()
            .post(ch_url)
            .header(reqwest::header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(query.to_string())
            .send()
            .await;
        match resp {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.map_err(|e| {
                    ApiError::internal(format!("clickhouse body read failed: {}", e))
                })?;
                if status.is_success() {
                    return serde_json::from_str::<Value>(&body).map_err(|e| {
                        ApiError::internal(format!("clickhouse json parse failed: {}", e))
                    });
                }
                let retriable = status.is_server_error()
                    || status == reqwest::StatusCode::TOO_MANY_REQUESTS
                    || status == reqwest::StatusCode::REQUEST_TIMEOUT;
                if retriable && attempt < attempts {
                    let backoff = base_backoff_ms
                        .saturating_mul(2_u64.saturating_pow((attempt - 1) as u32))
                        .min(2_500);
                    tokio::time::sleep(Duration::from_millis(backoff)).await;
                    continue;
                }
                return Err(ApiError::internal(format!(
                    "clickhouse query failed status={} body={} (attempt {}/{})",
                    status, body, attempt, attempts
                )));
            }
            Err(e) => {
                if attempt < attempts {
                    let backoff = base_backoff_ms
                        .saturating_mul(2_u64.saturating_pow((attempt - 1) as u32))
                        .min(2_500);
                    tokio::time::sleep(Duration::from_millis(backoff)).await;
                    continue;
                }
                return Err(ApiError::internal(format!(
                    "clickhouse request failed after {} attempts: {}",
                    attempts, e
                )));
            }
        }
    }
    Err(ApiError::internal("clickhouse request failed unexpectedly"))
}

pub(super) fn is_safe_identifier(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 32
        && v.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

pub(super) fn is_valid_timeframe(tf: &str) -> bool {
    matches!(tf, "5m" | "15m" | "30m" | "1h" | "2h" | "4h" | "all")
}

pub(super) fn default_lookback_minutes(tf: &str) -> u32 {
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

pub(super) async fn check_clickhouse(ch_url: Option<&str>) -> ServiceHealth {
    let Some(url) = ch_url else {
        return ServiceHealth {
            enabled: false,
            ok: true,
            latency_ms: None,
            detail: "disabled".to_string(),
        };
    };

    let st = Instant::now();
    let resp = clickhouse_http_client()
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

pub(super) async fn check_redis(manager: Option<redis::aio::ConnectionManager>) -> ServiceHealth {
    let Some(mut conn) = manager else {
        return ServiceHealth {
            enabled: false,
            ok: true,
            latency_ms: None,
            detail: "disabled".to_string(),
        };
    };

    let st = Instant::now();
    let ping = redis::cmd("PING").query_async::<String>(&mut conn).await;

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
