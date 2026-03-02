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

pub(super) fn round_to_tick(price: f64, tick_size: f64, is_buy: bool) -> f64 {
    let tick = tick_size.max(0.0001);
    let steps = price / tick;
    let aligned = if is_buy {
        steps.ceil() * tick
    } else {
        steps.floor() * tick
    };
    aligned.clamp(0.01, 0.99)
}

fn decimal_places_from_step(step: f64, fallback: usize, max_decimals: usize) -> usize {
    if !step.is_finite() || step <= 0.0 {
        return fallback.min(max_decimals);
    }
    let mut s = format!("{step:.12}");
    while s.ends_with('0') {
        s.pop();
    }
    if s.ends_with('.') {
        s.pop();
    }
    let dp = s
        .split_once('.')
        .map(|(_, frac)| frac.len())
        .unwrap_or(0usize);
    dp.min(max_decimals)
}

fn format_decimal_compact(value: f64, max_decimals: usize) -> String {
    let mut s = format!("{value:.prec$}", prec = max_decimals);
    while s.contains('.') && s.ends_with('0') {
        s.pop();
    }
    if s.ends_with('.') {
        s.pop();
    }
    if s.is_empty() {
        "0".to_string()
    } else {
        s
    }
}

#[derive(Debug, Clone)]
pub(super) struct LiveMarketTarget {
    pub(super) market_id: String,
    pub(super) symbol: String,
    pub(super) timeframe: String,
    pub(super) token_id_yes: String,
    pub(super) token_id_no: String,
    pub(super) end_date: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct LiveGatedDecision {
    pub(super) decision: Value,
    pub(super) decision_key: String,
}

const LIVE_MARKET_TARGET_CACHE_TTL_MS_DEFAULT: i64 = 8_000;
const LIVE_MARKET_TARGET_CACHE_TTL_MS_MIN: i64 = 2_000;
const LIVE_MARKET_TARGET_CACHE_TTL_MS_MAX: i64 = 120_000;
const LIVE_MARKET_TARGET_ACTIVE_CACHE_MAX_AGE_MS_DEFAULT: i64 = 90_000;
const LIVE_MARKET_TARGET_ACTIVE_CACHE_MAX_AGE_MS_MIN: i64 = 5_000;
const LIVE_MARKET_TARGET_ACTIVE_CACHE_MAX_AGE_MS_MAX: i64 = 600_000;
const LIVE_MARKET_TARGET_RESOLVE_ATTEMPTS_DEFAULT: i64 = 5;
const LIVE_MARKET_TARGET_RESOLVE_ATTEMPTS_MIN: i64 = 1;
const LIVE_MARKET_TARGET_RESOLVE_ATTEMPTS_MAX: i64 = 8;
const LIVE_MARKET_TARGET_RESOLVE_RETRY_MS_DEFAULT: i64 = 220;
const LIVE_MARKET_TARGET_RESOLVE_RETRY_MS_MIN: i64 = 0;
const LIVE_MARKET_TARGET_RESOLVE_RETRY_MS_MAX: i64 = 2_000;
const LIVE_MARKET_TARGET_SWITCH_GUARD_MS_DEFAULT: i64 = 2_500;
const LIVE_MARKET_TARGET_SWITCH_GUARD_MS_MIN: i64 = 0;
const LIVE_MARKET_TARGET_SWITCH_GUARD_MS_MAX: i64 = 30_000;
const LIVE_MARKET_TARGET_SNAPSHOT_MAX_AGE_MS_DEFAULT: i64 = 8_000;
const LIVE_MARKET_TARGET_SNAPSHOT_MAX_AGE_MS_MIN: i64 = 1_000;
const LIVE_MARKET_TARGET_SNAPSHOT_MAX_AGE_MS_MAX: i64 = 120_000;
const LIVE_MARKET_TARGET_SNAPSHOT_STALE_MAX_AGE_MS_DEFAULT: i64 = 45_000;
const LIVE_MARKET_TARGET_SNAPSHOT_STALE_MAX_AGE_MS_MIN: i64 = 5_000;
const LIVE_MARKET_TARGET_SNAPSHOT_STALE_MAX_AGE_MS_MAX: i64 = 300_000;
const LIVE_TARGET_CACHE_FILE_DEFAULT: &str = "/data/polyedge-forge/cache/target_market_cache.json";
const LIVE_ENTRY_MAKER_MAX_WAIT_MS_DEFAULT: i64 = 850;
const LIVE_ENTRY_MAKER_MAX_WAIT_MS_MIN: i64 = 500;
const LIVE_ENTRY_MAKER_MAX_WAIT_MS_MAX: i64 = 3_000;
const LIVE_ENTRY_FAK_TTL_MS_DEFAULT: i64 = 650;
const LIVE_ENTRY_FAK_TTL_MS_MIN: i64 = 400;
const LIVE_ENTRY_FAK_TTL_MS_MAX: i64 = 2_000;
const LIVE_ENTRY_FAK_CANCEL_AFTER_MS_DEFAULT: i64 = 900;
const LIVE_ENTRY_FAK_CANCEL_AFTER_MS_MIN: i64 = 500;
const LIVE_ENTRY_FAK_CANCEL_AFTER_MS_MAX: i64 = 3_000;
const LIVE_ENTRY_FAK_SLIPPAGE_BOOST_BPS_DEFAULT: f64 = 16.0;
const LIVE_ENTRY_FAK_SLIPPAGE_BOOST_BPS_MIN: f64 = 0.0;
const LIVE_ENTRY_FAK_SLIPPAGE_BOOST_BPS_MAX: f64 = 120.0;
const LIVE_CANCEL_FAILURE_FORCE_PAUSE_EXIT_THRESHOLD: u8 = 2;
const LIVE_CANCEL_FAILURE_FORCE_PAUSE_OTHER_THRESHOLD: u8 = 3;
const LIVE_GTD_MIN_FUTURE_GUARD_SEC_DEFAULT: i64 = 60;
const LIVE_GTD_MIN_FUTURE_GUARD_SEC_MIN: i64 = 30;
const LIVE_GTD_MIN_FUTURE_GUARD_SEC_MAX: i64 = 240;
const LIVE_ROUND_TARGET_MATCH_TOLERANCE_MS_DEFAULT: i64 = 5_000;
const LIVE_ROUND_TARGET_MATCH_TOLERANCE_MS_MIN: i64 = 0;
const LIVE_ROUND_TARGET_MATCH_TOLERANCE_MS_MAX: i64 = 120_000;
const LIVE_FIXED_ENTRY_SIZE_SHARES_MIN: f64 = 0.0;
const LIVE_FIXED_ENTRY_SIZE_SHARES_MAX: f64 = 1_000.0;

#[derive(Debug, Clone)]
struct CachedLiveMarketTarget {
    fetched_at_ms: i64,
    target: LiveMarketTarget,
}

fn live_market_target_cache(
) -> &'static tokio::sync::RwLock<HashMap<String, CachedLiveMarketTarget>> {
    static CACHE: std::sync::OnceLock<
        tokio::sync::RwLock<HashMap<String, CachedLiveMarketTarget>>,
    > = std::sync::OnceLock::new();
    CACHE.get_or_init(|| tokio::sync::RwLock::new(HashMap::new()))
}

fn live_market_target_cache_ttl_ms() -> i64 {
    std::env::var("FORGE_FEV1_LIVE_TARGET_CACHE_TTL_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_MARKET_TARGET_CACHE_TTL_MS_DEFAULT)
        .clamp(
            LIVE_MARKET_TARGET_CACHE_TTL_MS_MIN,
            LIVE_MARKET_TARGET_CACHE_TTL_MS_MAX,
        )
}

fn live_market_target_active_cache_max_age_ms() -> i64 {
    std::env::var("FORGE_FEV1_LIVE_TARGET_ACTIVE_CACHE_MAX_AGE_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_MARKET_TARGET_ACTIVE_CACHE_MAX_AGE_MS_DEFAULT)
        .clamp(
            LIVE_MARKET_TARGET_ACTIVE_CACHE_MAX_AGE_MS_MIN,
            LIVE_MARKET_TARGET_ACTIVE_CACHE_MAX_AGE_MS_MAX,
        )
}

fn live_market_target_resolve_attempts() -> usize {
    std::env::var("FORGE_FEV1_LIVE_TARGET_RESOLVE_ATTEMPTS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_MARKET_TARGET_RESOLVE_ATTEMPTS_DEFAULT)
        .clamp(
            LIVE_MARKET_TARGET_RESOLVE_ATTEMPTS_MIN,
            LIVE_MARKET_TARGET_RESOLVE_ATTEMPTS_MAX,
        ) as usize
}

fn live_market_target_resolve_retry_ms() -> u64 {
    std::env::var("FORGE_FEV1_LIVE_TARGET_RESOLVE_RETRY_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_MARKET_TARGET_RESOLVE_RETRY_MS_DEFAULT)
        .clamp(
            LIVE_MARKET_TARGET_RESOLVE_RETRY_MS_MIN,
            LIVE_MARKET_TARGET_RESOLVE_RETRY_MS_MAX,
        ) as u64
}

fn live_market_target_switch_guard_ms() -> i64 {
    std::env::var("FORGE_FEV1_LIVE_TARGET_SWITCH_GUARD_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_MARKET_TARGET_SWITCH_GUARD_MS_DEFAULT)
        .clamp(
            LIVE_MARKET_TARGET_SWITCH_GUARD_MS_MIN,
            LIVE_MARKET_TARGET_SWITCH_GUARD_MS_MAX,
        )
}

fn live_market_target_snapshot_max_age_ms() -> i64 {
    std::env::var("FORGE_FEV1_LIVE_TARGET_SNAPSHOT_MAX_AGE_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_MARKET_TARGET_SNAPSHOT_MAX_AGE_MS_DEFAULT)
        .clamp(
            LIVE_MARKET_TARGET_SNAPSHOT_MAX_AGE_MS_MIN,
            LIVE_MARKET_TARGET_SNAPSHOT_MAX_AGE_MS_MAX,
        )
}

fn live_market_target_snapshot_stale_max_age_ms() -> i64 {
    std::env::var("FORGE_FEV1_LIVE_TARGET_SNAPSHOT_STALE_MAX_AGE_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_MARKET_TARGET_SNAPSHOT_STALE_MAX_AGE_MS_DEFAULT)
        .clamp(
            LIVE_MARKET_TARGET_SNAPSHOT_STALE_MAX_AGE_MS_MIN,
            LIVE_MARKET_TARGET_SNAPSHOT_STALE_MAX_AGE_MS_MAX,
        )
}

fn live_entry_maker_max_wait_ms() -> i64 {
    std::env::var("FORGE_FEV1_ENTRY_MAKER_MAX_WAIT_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_ENTRY_MAKER_MAX_WAIT_MS_DEFAULT)
        .clamp(
            LIVE_ENTRY_MAKER_MAX_WAIT_MS_MIN,
            LIVE_ENTRY_MAKER_MAX_WAIT_MS_MAX,
        )
}

fn live_entry_fak_ttl_ms() -> i64 {
    std::env::var("FORGE_FEV1_ENTRY_FAK_TTL_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_ENTRY_FAK_TTL_MS_DEFAULT)
        .clamp(LIVE_ENTRY_FAK_TTL_MS_MIN, LIVE_ENTRY_FAK_TTL_MS_MAX)
}

fn live_entry_fak_cancel_after_ms() -> i64 {
    std::env::var("FORGE_FEV1_ENTRY_FAK_CANCEL_AFTER_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_ENTRY_FAK_CANCEL_AFTER_MS_DEFAULT)
        .clamp(
            LIVE_ENTRY_FAK_CANCEL_AFTER_MS_MIN,
            LIVE_ENTRY_FAK_CANCEL_AFTER_MS_MAX,
        )
}

fn live_entry_fak_slippage_boost_bps() -> f64 {
    std::env::var("FORGE_FEV1_ENTRY_FAK_SLIPPAGE_BOOST_BPS")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(LIVE_ENTRY_FAK_SLIPPAGE_BOOST_BPS_DEFAULT)
        .clamp(
            LIVE_ENTRY_FAK_SLIPPAGE_BOOST_BPS_MIN,
            LIVE_ENTRY_FAK_SLIPPAGE_BOOST_BPS_MAX,
        )
}

fn live_target_cache_file_path() -> String {
    std::env::var("FORGE_FEV1_LIVE_TARGET_CACHE_FILE")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| LIVE_TARGET_CACHE_FILE_DEFAULT.to_string())
}

fn live_gtd_min_future_guard_sec() -> i64 {
    std::env::var("FORGE_FEV1_GTD_MIN_FUTURE_GUARD_SEC")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_GTD_MIN_FUTURE_GUARD_SEC_DEFAULT)
        .clamp(
            LIVE_GTD_MIN_FUTURE_GUARD_SEC_MIN,
            LIVE_GTD_MIN_FUTURE_GUARD_SEC_MAX,
        )
}

fn live_round_target_match_tolerance_ms() -> i64 {
    std::env::var("FORGE_FEV1_ROUND_TARGET_MATCH_TOLERANCE_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_ROUND_TARGET_MATCH_TOLERANCE_MS_DEFAULT)
        .clamp(
            LIVE_ROUND_TARGET_MATCH_TOLERANCE_MS_MIN,
            LIVE_ROUND_TARGET_MATCH_TOLERANCE_MS_MAX,
        )
}

fn live_fixed_entry_size_shares() -> Option<f64> {
    std::env::var("FORGE_FEV1_FIXED_ENTRY_SIZE_SHARES")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .map(|v| v.clamp(LIVE_FIXED_ENTRY_SIZE_SHARES_MIN, LIVE_FIXED_ENTRY_SIZE_SHARES_MAX))
        .filter(|v| *v > 0.0)
}

fn timeframe_duration_ms(tf: &str) -> Option<i64> {
    match tf.trim().to_ascii_lowercase().as_str() {
        "1m" => Some(60_000),
        "5m" => Some(300_000),
        "15m" => Some(900_000),
        "30m" => Some(1_800_000),
        "1h" => Some(3_600_000),
        "2h" => Some(7_200_000),
        "4h" => Some(14_400_000),
        _ => None,
    }
}

fn parse_round_end_ts_ms(round_id: &str) -> Option<i64> {
    let mut parts = round_id.rsplit('_');
    let start_ms = parts.next()?.parse::<i64>().ok()?;
    let timeframe = parts.next()?;
    let dur_ms = timeframe_duration_ms(timeframe)?;
    Some(start_ms.saturating_add(dur_ms))
}

fn live_market_token_cache() -> &'static tokio::sync::RwLock<HashMap<String, (String, String)>> {
    static CACHE: std::sync::OnceLock<tokio::sync::RwLock<HashMap<String, (String, String)>>> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(|| tokio::sync::RwLock::new(HashMap::new()))
}

fn live_gamma_market_url_base() -> String {
    std::env::var("FORGE_FEV1_GAMMA_MARKET_URL_BASE")
        .ok()
        .map(|v| v.trim().trim_end_matches('/').to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "https://gamma-api.polymarket.com/markets".to_string())
}

fn live_gamma_market_http_client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(500))
            .timeout(Duration::from_millis(1_200))
            .pool_max_idle_per_host(4)
            .build()
            .expect("build gamma market http client")
    })
}

fn parse_token_array_field(v: &Value, key: &str) -> Vec<String> {
    if let Some(arr) = v.get(key).and_then(Value::as_array) {
        return arr
            .iter()
            .filter_map(Value::as_str)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
    }
    if let Some(raw) = v.get(key).and_then(Value::as_str) {
        if let Ok(arr) = serde_json::from_str::<Vec<String>>(raw) {
            return arr
                .into_iter()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
    }
    Vec::new()
}

async fn resolve_token_ids_from_gamma_market_detail(
    market_id: &str,
) -> Option<(String, String)> {
    {
        let cache = live_market_token_cache().read().await;
        if let Some(v) = cache.get(market_id) {
            return Some(v.clone());
        }
    }

    let base = live_gamma_market_url_base();
    let url = format!("{base}/{market_id}");
    let client = live_gamma_market_http_client();
    let resp = client.get(url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let detail = resp.json::<Value>().await.ok()?;
    let token_ids = parse_token_array_field(&detail, "clobTokenIds");
    if token_ids.len() < 2 {
        return None;
    }
    let outcomes = parse_token_array_field(&detail, "outcomes");

    let mut up_idx = None;
    let mut down_idx = None;
    for (idx, o) in outcomes.iter().enumerate() {
        let key = o.to_ascii_lowercase();
        if up_idx.is_none() && (key.contains("up") || key.contains("yes")) {
            up_idx = Some(idx);
        }
        if down_idx.is_none() && (key.contains("down") || key.contains("no")) {
            down_idx = Some(idx);
        }
    }
    let yes_token = up_idx
        .and_then(|i| token_ids.get(i))
        .cloned()
        .unwrap_or_else(|| token_ids[0].clone());
    let no_token = down_idx
        .and_then(|i| token_ids.get(i))
        .cloned()
        .unwrap_or_else(|| token_ids[1].clone());
    if yes_token.is_empty() || no_token.is_empty() || yes_token == no_token {
        return None;
    }

    {
        let mut cache = live_market_token_cache().write().await;
        cache.insert(market_id.to_string(), (yes_token.clone(), no_token.clone()));
    }
    Some((yes_token, no_token))
}

fn decision_round_matches_target(decision: &Value, target: &LiveMarketTarget) -> bool {
    let round_id = decision
        .get("round_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim();
    if round_id.is_empty() {
        return true;
    }
    let Some(decision_end_ms) = parse_round_end_ts_ms(round_id) else {
        return true;
    };
    let Some(target_end_ms) = parse_end_date_ms(target.end_date.as_deref()) else {
        return true;
    };
    decision_end_ms
        .saturating_sub(target_end_ms)
        .abs()
        <= live_round_target_match_tolerance_ms()
}

fn is_entry_action(action: &str) -> bool {
    let a = action.to_ascii_lowercase();
    a == "enter" || a == "add"
}

fn pending_cancel_due_ms(row: &LivePendingOrder) -> i64 {
    let base = row.cancel_after_ms.max(500);
    let maker_tif = matches!(
        row.tif.to_ascii_uppercase().as_str(),
        "GTD" | "GTC" | "POST_ONLY"
    );
    if maker_tif && is_entry_action(&row.action) {
        base.min(live_entry_maker_max_wait_ms())
    } else {
        base
    }
}

fn end_date_iso_from_ms(end_ms: i64) -> Option<String> {
    DateTime::<Utc>::from_timestamp_millis(end_ms).map(|dt| dt.to_rfc3339())
}

async fn resolve_token_ids_from_target_cache(
    market_id: &str,
    market_type: &str,
) -> Option<(String, String)> {
    {
        let cache = live_market_token_cache().read().await;
        if let Some(v) = cache.get(market_id) {
            return Some(v.clone());
        }
    }
    let cache_path = live_target_cache_file_path();
    if let Ok(raw) = tokio::fs::read_to_string(&cache_path).await {
        if let Ok(root) = serde_json::from_str::<Value>(&raw) {
            if let Some(obj) = root.as_object() {
                for (_, bucket) in obj {
                    let Some(market_obj) = bucket
                        .as_object()
                        .and_then(|m| m.get(market_id))
                        .and_then(Value::as_object)
                    else {
                        continue;
                    };
                    let tf_ok = market_obj
                        .get("timeframe")
                        .and_then(Value::as_str)
                        .map(|tf| tf.eq_ignore_ascii_case(market_type))
                        .unwrap_or(false);
                    if !tf_ok {
                        continue;
                    }
                    let yes = market_obj
                        .get("yes_token")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .trim();
                    let no = market_obj
                        .get("no_token")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .trim();
                    if yes.is_empty() || no.is_empty() {
                        continue;
                    }
                    let pair = (yes.to_string(), no.to_string());
                    let mut cache = live_market_token_cache().write().await;
                    cache.insert(market_id.to_string(), pair.clone());
                    return Some(pair);
                }
            }
        }
    }
    let from_gamma = resolve_token_ids_from_gamma_market_detail(market_id).await;
    if from_gamma.is_some() {
        tracing::warn!(
            market_id = market_id,
            market_type = market_type,
            "resolved token ids via gamma market detail fallback"
        );
    }
    from_gamma
}

async fn resolve_live_target_from_snapshot(
    state: &ApiState,
    market_type: &str,
    now_ms: i64,
) -> Option<LiveMarketTarget> {
    async fn try_snapshot_candidate(
        _state: &ApiState,
        market_type: &str,
        now_ms: i64,
        snapshot: &Value,
        source: &str,
    ) -> Option<LiveMarketTarget> {
        let market_id = snapshot
            .get("market_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())?
            .to_string();
        let round_id = snapshot
            .get("round_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let sample_ms = snapshot
            .get("ts_ireland_sample_ms")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let age_ms = now_ms.saturating_sub(sample_ms);
        let fresh_max_age_ms = live_market_target_snapshot_max_age_ms();
        let stale_max_age_ms = live_market_target_snapshot_stale_max_age_ms();
        if age_ms > stale_max_age_ms {
            return None;
        }
        let end_ms = parse_round_end_ts_ms(round_id).or_else(|| {
            let remain = snapshot.get("remaining_ms").and_then(Value::as_i64)?;
            Some(sample_ms.saturating_add(remain.max(0)))
        })?;
        if end_ms < now_ms {
            return None;
        }
        let (token_id_yes, token_id_no) =
            resolve_token_ids_from_target_cache(&market_id, market_type).await?;
        if age_ms > fresh_max_age_ms {
            tracing::warn!(
                market_type = market_type,
                market_id = market_id,
                source = source,
                age_ms = age_ms,
                fresh_max_age_ms = fresh_max_age_ms,
                stale_max_age_ms = stale_max_age_ms,
                end_ms = end_ms,
                "using stale-but-active snapshot fallback for live market target"
            );
        }
        Some(LiveMarketTarget {
            market_id,
            symbol: "BTCUSDT".to_string(),
            timeframe: market_type.to_string(),
            token_id_yes,
            token_id_no,
            end_date: end_date_iso_from_ms(end_ms),
        })
    }

    let symbol_key = format!(
        "{}:snapshot:latest:BTCUSDT:{market_type}",
        state.redis_prefix
    );
    if let Ok(snapshot_json) = read_key_json(state, &symbol_key).await {
        let snapshot = snapshot_json.0;
        if let Some(target) =
            try_snapshot_candidate(state, market_type, now_ms, &snapshot, "symbol_snapshot").await
        {
            return Some(target);
        }
    }

    let tf_key = format!("{}:snapshot:latest:tf:{market_type}", state.redis_prefix);
    if let Ok(tf_snapshot_json) = read_key_json(state, &tf_key).await {
        let tf_snapshot = tf_snapshot_json.0;
        let mut btc_rows: Vec<&Value> = match tf_snapshot.as_array() {
            Some(rows) => rows
                .iter()
                .filter(|row| {
                    row.get("symbol")
                        .and_then(Value::as_str)
                        .map(|s| s.eq_ignore_ascii_case("BTCUSDT"))
                        .unwrap_or(false)
                })
                .collect(),
            None => Vec::new(),
        };
        btc_rows.sort_by_key(|row| {
            std::cmp::Reverse(
                row.get("ts_ireland_sample_ms")
                    .and_then(Value::as_i64)
                    .unwrap_or(0),
            )
        });
        for row in btc_rows {
            if let Some(target) =
                try_snapshot_candidate(state, market_type, now_ms, row, "tf_snapshot").await
            {
                return Some(target);
            }
        }
    }

    let pattern = format!(
        "{}:snapshot:latest:BTCUSDT:{}:*",
        state.redis_prefix, market_type
    );
    let mut history_keys = Vec::<String>::new();
    if let Some(mut conn) = state.redis_manager.clone() {
        let mut cursor: u64 = 0;
        loop {
            let scanned: redis::RedisResult<(u64, Vec<String>)> = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(96)
                .query_async(&mut conn)
                .await;
            let Ok((next, mut keys)) = scanned else {
                break;
            };
            history_keys.append(&mut keys);
            if next == 0 || history_keys.len() >= 320 {
                break;
            }
            cursor = next;
        }
    }
    if !history_keys.is_empty() {
        let mut best: Option<(i64, LiveMarketTarget)> = None;
        for key in history_keys {
            let Ok(snapshot_json) = read_key_json(state, &key).await else {
                continue;
            };
            let snapshot = snapshot_json.0;
            let Some(target) = try_snapshot_candidate(
                state,
                market_type,
                now_ms,
                &snapshot,
                "symbol_history_scan",
            )
            .await
            else {
                continue;
            };
            let sample_ms = snapshot
                .get("ts_ireland_sample_ms")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            match &best {
                Some((best_ts, _)) if *best_ts >= sample_ms => {}
                _ => best = Some((sample_ms, target)),
            }
        }
        if let Some((_, target)) = best {
            tracing::warn!(
                market_type = market_type,
                "market target resolved from symbol history scan fallback"
            );
            return Some(target);
        }
    }
    None
}

pub(super) fn live_decision_key(market_type: &str, decision: &Value) -> String {
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
    let ts = decision
        .get("ts_ms")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    let price_cents = decision
        .get("price_cents")
        .and_then(Value::as_f64)
        .unwrap_or_default();
    format!(
        "{}:{}:{}:{}:{}:{:.4}",
        market_type, action, side, round, ts, price_cents
    )
}

pub(super) async fn gate_live_decisions(
    state: &ApiState,
    market_type: &str,
    decisions: &[Value],
    mark_attempts: bool,
) -> (Vec<LiveGatedDecision>, Vec<Value>, LivePositionState) {
    let now_ms = Utc::now().timestamp_millis();
    let mut position_state = state.get_live_position_state(market_type).await;
    let mut virtual_side = position_state.side.clone();
    let (mut has_enter_pending, mut has_exit_pending) =
        state.pending_flags_for_market(market_type).await;
    let mut accepted = Vec::<LiveGatedDecision>::new();
    let mut skipped = Vec::<Value>::new();

    for decision in decisions {
        let mut normalized = decision.clone();
        let mut action = normalized
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        let side = normalized
            .get("side")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_uppercase();
        let side_match = virtual_side
            .as_deref()
            .map(|s| s.eq_ignore_ascii_case(&side))
            .unwrap_or(false);
        if action == "enter" && virtual_side.is_some() && side_match {
            action = "add".to_string();
            if let Some(obj) = normalized.as_object_mut() {
                obj.insert("action".to_string(), Value::String(action.clone()));
            }
        }
        if !matches!(action.as_str(), "enter" | "add" | "reduce" | "exit") {
            skipped.push(json!({
                "reason": "invalid_action",
                "decision": normalized
            }));
            continue;
        }
        if side != "UP" && side != "DOWN" {
            skipped.push(json!({
                "reason": "invalid_side",
                "decision": normalized
            }));
            continue;
        }
        if matches!(action.as_str(), "enter" | "add")
            && position_state.state.eq_ignore_ascii_case("exit_pending")
        {
            skipped.push(json!({
                "reason": "exit_state_lock",
                "state": position_state.state,
                "decision": normalized
            }));
            continue;
        }
        if matches!(action.as_str(), "enter" | "add") && (has_enter_pending || has_exit_pending) {
            skipped.push(json!({
                "reason": if has_enter_pending { "enter_pending_exists" } else { "exit_pending_exists" },
                "decision": normalized
            }));
            continue;
        }
        if matches!(action.as_str(), "exit" | "reduce") && has_exit_pending {
            skipped.push(json!({
                "reason": "exit_pending_exists",
                "decision": normalized
            }));
            continue;
        }
        if matches!(action.as_str(), "exit" | "reduce") && has_enter_pending {
            // Exit signals must not be blocked by entry pending.
            // Execution layer will actively cancel entry/add pendings first.
            if let Some(obj) = normalized.as_object_mut() {
                obj.insert(
                    "pre_exit_cancel_entry_pending".to_string(),
                    Value::Bool(true),
                );
            }
        }
        let decision_key = live_decision_key(market_type, &normalized);
        let should_submit = if action == "enter" {
            if virtual_side.is_none() {
                true
            } else {
                skipped.push(json!({
                    "reason": "already_in_position",
                    "state_side": virtual_side,
                    "decision": normalized
                }));
                false
            }
        } else if action == "add" {
            if side_match {
                true
            } else {
                skipped.push(json!({
                    "reason": if virtual_side.is_none() { "no_open_position_for_add" } else { "side_mismatch_for_add" },
                    "state_side": virtual_side,
                    "decision": normalized
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
                "decision": normalized
            }));
            false
        };
        if !should_submit {
            continue;
        }
        if state
            .check_and_mark_live_decision(&decision_key, now_ms, mark_attempts)
            .await
        {
            skipped.push(json!({
                "reason": "duplicate_recent",
                "decision_key": decision_key,
                "decision": normalized
            }));
            continue;
        }
        if matches!(action.as_str(), "enter" | "add") {
            virtual_side = Some(side.clone());
            has_enter_pending = true;
        } else if action == "exit" {
            virtual_side = None;
            has_exit_pending = true;
        } else {
            has_exit_pending = true;
        }
        accepted.push(LiveGatedDecision {
            decision: normalized,
            decision_key,
        });
    }

    position_state.updated_ts_ms = now_ms;
    (accepted, skipped, position_state)
}

pub(super) fn select_live_decisions(
    decisions: &[Value],
    latest_ts_ms: i64,
    max_orders: usize,
    drain_only: bool,
    prefer_action: Option<&str>,
) -> Vec<Value> {
    fn is_replay_only_reason(decision: &Value) -> bool {
        decision
            .get("reason")
            .and_then(Value::as_str)
            .map(|reason| {
                reason
                    .trim()
                    .eq_ignore_ascii_case("end_of_samples_force_close")
            })
            .unwrap_or(false)
    }

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
            let freshness_ms = if action == "enter" || action == "add" {
                20_000
            } else {
                90_000
            };
            let round_ok = d
                .get("round_id")
                .and_then(Value::as_str)
                .map(|r| r == latest_round)
                .unwrap_or(false);
            let ts_ok = d
                .get("ts_ms")
                .and_then(Value::as_i64)
                .map(|ts| ts >= latest_ts_ms.saturating_sub(freshness_ms))
                .unwrap_or(false);
            let scope_ok = if drain_only { round_ok || ts_ok } else { ts_ok };
            let action_ok = if drain_only {
                action == "exit" || action == "reduce"
            } else {
                true
            };
            let reason_ok = !is_replay_only_reason(d);
            scope_ok && action_ok && reason_ok
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
            let freshness_ms = if action == "enter" || action == "add" {
                20_000
            } else {
                90_000
            };
            let action_ok = if drain_only {
                action == "exit" || action == "reduce"
            } else {
                d.get("ts_ms")
                    .and_then(Value::as_i64)
                    .map(|ts| ts >= latest_ts_ms.saturating_sub(freshness_ms))
                    .unwrap_or(false)
            };
            action_ok && !is_replay_only_reason(d)
        }) {
            selected.push(last.clone());
        }
    }
    selected.sort_by_key(|v| v.get("ts_ms").and_then(Value::as_i64).unwrap_or(0));
    if selected.len() > max_orders {
        if max_orders <= 1 {
            let preferred = prefer_action
                .and_then(|want| {
                    selected.iter().rev().find(|v| {
                        v.get("action")
                            .and_then(Value::as_str)
                            .map(|a| a.eq_ignore_ascii_case(want))
                            .unwrap_or(false)
                    })
                })
                .cloned();
            if let Some(v) = preferred.or_else(|| selected.last().cloned()) {
                selected = vec![v];
            }
        } else {
            selected = selected[selected.len() - max_orders..].to_vec();
        }
    }
    selected
}

pub(super) async fn resolve_live_market_target_with_state(
    state: &ApiState,
    market_type: &str,
) -> Result<LiveMarketTarget, ApiError> {
    resolve_live_market_target_inner(Some(state), market_type).await
}

async fn resolve_live_market_target_inner(
    state: Option<&ApiState>,
    market_type: &str,
) -> Result<LiveMarketTarget, ApiError> {
    let now_ms = Utc::now().timestamp_millis();
    let cache_ttl_ms = live_market_target_cache_ttl_ms();
    let active_cache_max_age_ms = live_market_target_active_cache_max_age_ms();
    let resolve_attempts = live_market_target_resolve_attempts();
    let resolve_retry_ms = live_market_target_resolve_retry_ms();
    let switch_guard_ms = live_market_target_switch_guard_ms();
    let mut cached_entry: Option<CachedLiveMarketTarget> = None;
    {
        let cache = live_market_target_cache().read().await;
        if let Some(cached) = cache.get(market_type) {
            cached_entry = Some(cached.clone());
            let age_ms = now_ms.saturating_sub(cached.fetched_at_ms);
            let end_ms =
                parse_end_date_ms(cached.target.end_date.as_deref()).unwrap_or(i64::MAX / 4);
            if age_ms <= cache_ttl_ms && end_ms >= now_ms {
                return Ok(cached.target.clone());
            }
        }
    }
    if let Some(state) = state {
        if let Some(target) = resolve_live_target_from_snapshot(state, market_type, now_ms).await {
            let mut cache = live_market_target_cache().write().await;
            cache.insert(
                market_type.to_string(),
                CachedLiveMarketTarget {
                    fetched_at_ms: Utc::now().timestamp_millis(),
                    target: target.clone(),
                },
            );
            return Ok(target);
        }
    }

    let discovery = MarketDiscovery::new(DiscoveryConfig {
        symbols: vec!["BTCUSDT".to_string()],
        market_types: vec!["updown".to_string()],
        timeframes: vec![market_type.to_string()],
        ..DiscoveryConfig::default()
    });
    let mut last_discovery_error: Option<String> = None;
    for attempt_idx in 0..resolve_attempts {
        let discovered = match discovery.discover().await {
            Ok(v) => v,
            Err(e) => {
                last_discovery_error = Some(e.to_string());
                if attempt_idx + 1 < resolve_attempts && resolve_retry_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(resolve_retry_ms)).await;
                }
                continue;
            }
        };
        let mut markets: Vec<MarketDescriptor> = discovered
            .into_iter()
            .filter(|m| {
                m.symbol.eq_ignore_ascii_case("BTCUSDT")
                    && m.timeframe
                        .as_deref()
                        .map(|tf| tf.eq_ignore_ascii_case(market_type))
                        .unwrap_or(false)
            })
            .collect();
        if markets.is_empty() {
            if attempt_idx + 1 < resolve_attempts && resolve_retry_ms > 0 {
                tokio::time::sleep(Duration::from_millis(resolve_retry_ms)).await;
            }
            continue;
        }
        {
            let mut token_cache = live_market_token_cache().write().await;
            for m in &markets {
                let yes = m
                    .token_id_yes
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or_default();
                let no = m
                    .token_id_no
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or_default();
                if yes.is_empty() || no.is_empty() || yes == no {
                    continue;
                }
                token_cache.insert(
                    m.market_id.clone(),
                    (yes.to_string(), no.to_string()),
                );
            }
        }
        markets.sort_by_key(|m| {
            let end_ms = parse_end_date_ms(m.end_date.as_deref()).unwrap_or(i64::MAX / 4);
            let bucket = if end_ms >= now_ms.saturating_add(switch_guard_ms) {
                0_i64
            } else if end_ms >= now_ms {
                1_i64
            } else {
                2_i64
            };
            let distance = if end_ms >= now_ms {
                end_ms.saturating_sub(now_ms)
            } else {
                now_ms.saturating_sub(end_ms)
            };
            (
                bucket,
                distance,
                std::cmp::Reverse(end_ms),
                m.market_id.clone(),
            )
        });
        for candidate in markets {
            let token_pair = match (
                candidate
                    .token_id_yes
                    .as_deref()
                    .map(str::trim)
                    .filter(|v| !v.is_empty()),
                candidate
                    .token_id_no
                    .as_deref()
                    .map(str::trim)
                    .filter(|v| !v.is_empty()),
            ) {
                (Some(yes), Some(no)) if yes != no => Some((yes.to_string(), no.to_string())),
                _ => resolve_token_ids_from_target_cache(&candidate.market_id, market_type).await,
            };
            let Some((token_id_yes, token_id_no)) = token_pair else {
                continue;
            };
            let target = LiveMarketTarget {
                market_id: candidate.market_id,
                symbol: candidate.symbol,
                timeframe: candidate.timeframe.unwrap_or_else(|| market_type.to_string()),
                token_id_yes,
                token_id_no,
                end_date: candidate.end_date,
            };
            {
                let mut cache = live_market_target_cache().write().await;
                cache.insert(
                    market_type.to_string(),
                    CachedLiveMarketTarget {
                        fetched_at_ms: Utc::now().timestamp_millis(),
                        target: target.clone(),
                    },
                );
            }
            return Ok(target);
        }
        if attempt_idx + 1 < resolve_attempts && resolve_retry_ms > 0 {
            tokio::time::sleep(Duration::from_millis(resolve_retry_ms)).await;
        }
    }
    if let Some(state) = state {
        if let Some(target) = resolve_live_target_from_snapshot(state, market_type, now_ms).await {
            tracing::warn!(
                market_type = market_type,
                "market target resolved from recorder snapshot fallback"
            );
            let mut cache = live_market_target_cache().write().await;
            cache.insert(
                market_type.to_string(),
                CachedLiveMarketTarget {
                    fetched_at_ms: Utc::now().timestamp_millis(),
                    target: target.clone(),
                },
            );
            return Ok(target);
        }
    }
    if let Some(cached) = cached_entry {
        let age_ms = now_ms.saturating_sub(cached.fetched_at_ms);
        let end_ms = parse_end_date_ms(cached.target.end_date.as_deref()).unwrap_or(i64::MIN / 4);
        if age_ms <= active_cache_max_age_ms && end_ms >= now_ms {
            tracing::warn!(
                market_type = market_type,
                resolve_attempts = resolve_attempts,
                cache_age_ms = age_ms,
                active_cache_max_age_ms = active_cache_max_age_ms,
                "market target resolve exhausted retries, fallback to still-active cached target"
            );
            return Ok(cached.target);
        }
    }
    tracing::warn!(
        market_type = market_type,
        resolve_attempts = resolve_attempts,
        last_discovery_error = last_discovery_error.as_deref().unwrap_or("none"),
        "market target resolve exhausted retries without stale fallback"
    );
    Err(ApiError::bad_request(format!(
        "no active BTC {market_type} market with token ids after {} attempts{}",
        resolve_attempts,
        last_discovery_error
            .as_ref()
            .map(|e| format!(", last_error={e}"))
            .unwrap_or_default()
    )))
}

fn collect_decision_token_ids(
    target: &LiveMarketTarget,
    decisions: &[LiveGatedDecision],
) -> Vec<String> {
    let mut uniq = HashSet::<String>::new();
    for gated in decisions {
        if let Some(token) = token_id_for_decision(&gated.decision, target) {
            uniq.insert(token.to_string());
        }
    }
    uniq.into_iter().collect()
}

async fn prefetch_gateway_books_for_tokens(
    client: &reqwest::Client,
    gateway_cfg: &LiveGatewayConfig,
    token_ids: &[String],
) -> HashMap<String, Option<GatewayBookSnapshot>> {
    let futures = token_ids.iter().cloned().map(|token_id| async move {
        let snap = fetch_gateway_book_snapshot(client, gateway_cfg, &token_id).await;
        (token_id, snap)
    });
    let rows = join_all(futures).await;
    rows.into_iter().collect()
}

async fn prefetch_rust_books_for_tokens(
    state: &ApiState,
    ctx: &Arc<RustExecutorContext>,
    fallback_client: &reqwest::Client,
    gateway_cfg: &LiveGatewayConfig,
    token_ids: &[String],
) -> HashMap<String, Option<GatewayBookSnapshot>> {
    let futures = token_ids.iter().cloned().map(|token_id| {
        let state = state.clone();
        let ctx = Arc::clone(ctx);
        let gateway_cfg = gateway_cfg.clone();
        async move {
            let snapshot = if let Some(cached) = state.get_rust_book_cache(&token_id).await {
                Some(cached)
            } else if let Some(v) = fetch_rust_book_snapshot(&ctx, &token_id).await {
                state.put_rust_book_cache(&token_id, v.clone()).await;
                Some(v)
            } else {
                let fetched =
                    fetch_gateway_book_snapshot(fallback_client, &gateway_cfg, &token_id).await;
                if let Some(v) = fetched.as_ref() {
                    state.put_rust_book_cache(&token_id, v.clone()).await;
                }
                fetched
            };
            (token_id, snapshot)
        }
    });
    let rows = join_all(futures).await;
    rows.into_iter().collect()
}

pub(super) fn decision_to_live_payload(
    decision: &Value,
    target: &LiveMarketTarget,
    gateway_cfg: &LiveGatewayConfig,
    book: Option<&GatewayBookSnapshot>,
    quote_size_override: Option<f64>,
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
    let is_exit_like = action == "exit" || action == "reduce";
    let (gateway_side, token_id, mut slippage_bps) = match (action.as_str(), side.as_str()) {
        ("enter", "UP") | ("add", "UP") => (
            "buy_yes",
            target.token_id_yes.as_str(),
            gateway_cfg.entry_slippage_bps,
        ),
        ("exit", "UP") | ("reduce", "UP") => (
            "sell_yes",
            target.token_id_yes.as_str(),
            gateway_cfg.exit_slippage_bps,
        ),
        ("enter", "DOWN") | ("add", "DOWN") => (
            "buy_no",
            target.token_id_no.as_str(),
            gateway_cfg.entry_slippage_bps,
        ),
        ("exit", "DOWN") | ("reduce", "DOWN") => (
            "sell_no",
            target.token_id_no.as_str(),
            gateway_cfg.exit_slippage_bps,
        ),
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
    let mut price = (price_cents / 100.0).clamp(0.01, 0.99);
    let mut tif = decision
        .get("tif")
        .and_then(Value::as_str)
        .map(|v| v.trim().to_ascii_uppercase())
        .filter(|v| matches!(v.as_str(), "FAK" | "FOK" | "GTC" | "GTD" | "POST_ONLY"))
        .unwrap_or_else(|| "FAK".to_string());
    let mut style = decision
        .get("style")
        .and_then(Value::as_str)
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| matches!(v.as_str(), "maker" | "taker"))
        .unwrap_or_else(|| {
            if tif == "GTC" || tif == "GTD" || tif == "POST_ONLY" {
                "maker".to_string()
            } else {
                "taker".to_string()
            }
        });
    let mut quote_size = quote_size_override.unwrap_or_else(|| {
        decision
            .get("quote_size_usdc")
            .and_then(Value::as_f64)
            .unwrap_or(gateway_cfg.min_quote_usdc)
    });
    quote_size = quote_size.max(gateway_cfg.min_quote_usdc);
    let mut notes: Vec<String> = Vec::with_capacity(3);
    let is_buy = gateway_side.starts_with("buy_");
    let fixed_entry_size_shares = if !is_exit_like {
        live_fixed_entry_size_shares()
    } else {
        None
    };
    let forced_exit_size_shares = if is_exit_like {
        decision
            .get("position_size_shares")
            .and_then(Value::as_f64)
            .filter(|v| v.is_finite() && *v > 0.0)
    } else {
        None
    };
    let forced_size_shares = forced_exit_size_shares.or(fixed_entry_size_shares);
    let mut min_order_size = 0.01_f64;
    let mut size_floor = min_order_size;
    let mut size = forced_size_shares.unwrap_or_else(|| (quote_size / price).max(size_floor));
    if let Some(book) = book {
        min_order_size = book.min_order_size.max(0.0001);
        let taker_like = style == "taker" || matches!(tif.as_str(), "FAK" | "FOK");
        size_floor = min_order_size;
        let tick_size = book.tick_size.max(0.0001);
        price = round_to_tick(price, tick_size, is_buy);
        size = forced_size_shares.unwrap_or_else(|| (quote_size / price).max(size_floor));

        let depth_top1 = if is_buy {
            book.best_ask_size.unwrap_or(0.0)
        } else {
            book.best_bid_size.unwrap_or(0.0)
        };
        let depth_top3 = if is_buy {
            book.ask_depth_top3.unwrap_or(0.0)
        } else {
            book.bid_depth_top3.unwrap_or(0.0)
        };
        if forced_size_shares.is_none()
            && !(is_buy && taker_like)
            && depth_top1.is_finite()
            && depth_top1 > 0.0
            && size > depth_top1 * 1.12
        {
            let capped = (depth_top1 * 0.95).max(size_floor);
            if capped < size {
                size = capped;
                notes.push("depth_cap_top1".to_string());
            }
        }
        if depth_top3.is_finite() && depth_top3 > 0.0 && size > depth_top3 * 1.25 {
            if is_exit_like {
                slippage_bps = (slippage_bps + 12.0).min(500.0);
                notes.push("depth_thin_exit_force_taker".to_string());
            } else {
                slippage_bps = (slippage_bps + 8.0).min(500.0);
                notes.push("depth_thin_entry_boost_slippage".to_string());
            }
        }
    }
    size = size.max(size_floor).max(0.01);
    size = (size * 10_000.0).round() / 10_000.0;
    if is_exit_like && notes.iter().any(|n| n == "depth_thin_exit_force_taker") {
        tif = "FAK".to_string();
        style = "taker".to_string();
    }
    let ttl_ms = decision
        .get("ttl_ms")
        .and_then(Value::as_i64)
        .unwrap_or(if is_exit_like { 900 } else { 1400 })
        .clamp(300, 30_000);
    slippage_bps = decision
        .get("max_slippage_bps")
        .and_then(Value::as_f64)
        .unwrap_or(slippage_bps)
        .clamp(0.0, 500.0);
    if let Some(force) = gateway_cfg.force_slippage_bps {
        slippage_bps = force.clamp(0.0, 500.0);
    }
    if let Some(book) = book {
        let taker_like = style == "taker" || matches!(tif.as_str(), "FAK" | "FOK");
        if taker_like {
            if is_buy {
                if let Some(best_ask) = book.best_ask.filter(|v| v.is_finite() && *v > 0.0) {
                    let anchored = best_ask.clamp(0.01, 0.99);
                    if anchored > price {
                        price = anchored;
                        notes.push("anchor_taker_to_best_ask".to_string());
                    }
                }
            } else if let Some(best_bid) = book.best_bid.filter(|v| v.is_finite() && *v > 0.0) {
                let anchored = best_bid.clamp(0.01, 0.99);
                if anchored < price {
                    price = anchored;
                    notes.push("anchor_taker_to_best_bid".to_string());
                }
            }
            let depth_top3 = if is_buy {
                book.ask_depth_top3.unwrap_or(0.0)
            } else {
                book.bid_depth_top3.unwrap_or(0.0)
            };
            if depth_top3.is_finite() && depth_top3 > 0.0 {
                let depth_util = (size / depth_top3).max(0.0);
                let mut extra_bps = if depth_util <= 0.25 {
                    2.0
                } else if depth_util <= 0.5 {
                    4.0
                } else if depth_util <= 0.85 {
                    8.0
                } else if depth_util <= 1.0 {
                    14.0
                } else if depth_util <= 1.3 {
                    22.0
                } else {
                    34.0
                };
                if let (Some(best_bid), Some(best_ask)) = (book.best_bid, book.best_ask) {
                    if best_bid.is_finite()
                        && best_ask.is_finite()
                        && best_bid > 0.0
                        && best_ask > 0.0
                    {
                        let mid = ((best_bid + best_ask) * 0.5).max(0.0001);
                        let spread_bps = ((best_ask - best_bid) / mid * 10_000.0).max(0.0);
                        if spread_bps >= 100.0 {
                            extra_bps += 4.0;
                        }
                    }
                }
                if is_exit_like {
                    extra_bps += 4.0;
                }
                if extra_bps > 0.0 {
                    let adjusted = if is_buy {
                        (price * (1.0 + extra_bps / 10_000.0)).clamp(0.01, 0.99)
                    } else {
                        (price * (1.0 - extra_bps / 10_000.0)).clamp(0.01, 0.99)
                    };
                    if (adjusted - price).abs() >= 0.00001 {
                        price = adjusted;
                        notes.push(format!("adaptive_taker_price_bps_{extra_bps:.1}"));
                    }
                    let min_slippage =
                        (extra_bps + if is_exit_like { 18.0 } else { 12.0 }).min(500.0);
                    if min_slippage > slippage_bps {
                        slippage_bps = min_slippage;
                        notes.push("adaptive_slippage_floor".to_string());
                    }
                }
            }
            let tick_size = book.tick_size.max(0.0001);
            price = round_to_tick(price, tick_size, is_buy);
            size_floor = min_order_size;
            size = forced_size_shares
                .unwrap_or_else(|| (quote_size / price).max(size_floor))
                .max(0.01);
            size = (size * 10_000.0).round() / 10_000.0;
        }
    }
    if let Some(forced) = forced_size_shares {
        size = forced.max(size_floor).max(0.01);
        if is_exit_like {
            notes.push("force_exit_full_size".to_string());
        } else {
            notes.push("force_entry_fixed_size".to_string());
        }
    }
    let taker_like = style == "taker" || matches!(tif.as_str(), "FAK" | "FOK");
    let floor_notional_usdc = (size * price).max(0.0);
    let requested_notional_usdc =
        (quote_size.max(floor_notional_usdc) * 1_000_000.0).round() / 1_000_000.0;
    let buy_amount_usdc = if is_buy && taker_like && fixed_entry_size_shares.is_none() {
        Some(requested_notional_usdc)
    } else {
        None
    };
    let cache_key = if let Some(amount) = buy_amount_usdc {
        format!(
            "fev1:{}:{}:{}:{:.4}:{:.4}:a{:.4}",
            target.market_id, gateway_side, action, price, size, amount
        )
    } else {
        format!(
            "fev1:{}:{}:{}:{:.4}:{:.4}",
            target.market_id, gateway_side, action, price, size
        )
    };
    let mut payload = json!({
        "market_id": target.market_id,
        "token_id": token_id,
        "side": gateway_side,
        "price": price,
        "size": size,
        "quote_size_usdc": requested_notional_usdc,
        "requested_notional_usdc": requested_notional_usdc,
        "tif": tif,
        "style": style,
        "ttl_ms": ttl_ms,
        "max_slippage_bps": slippage_bps,
        "execution_notes": notes,
        "cache_key": cache_key,
        "action": action,
        "signal_side": side,
        "book_meta": if let Some(book) = book {
            json!({
                "token_id": book.token_id,
                "min_order_size": book.min_order_size,
                "tick_size": book.tick_size,
                "best_bid": book.best_bid,
                "best_ask": book.best_ask,
                "best_bid_size": book.best_bid_size,
                "best_ask_size": book.best_ask_size,
                "bid_depth_top3": book.bid_depth_top3,
                "ask_depth_top3": book.ask_depth_top3
            })
        } else {
            Value::Null
        }
    });
    if let Some(amount) = buy_amount_usdc {
        if let Some(obj) = payload.as_object_mut() {
            obj.insert("buy_amount_usdc".to_string(), json!(amount));
            obj.insert("amount_mode".to_string(), json!("buy_usdc"));
        }
    }
    Some(payload)
}

pub(super) async fn post_gateway(
    client: &reqwest::Client,
    timeout_ms: u64,
    endpoint: &str,
    path: &str,
    payload: &Value,
) -> Result<Value, String> {
    let url = format!(
        "{}/{}",
        endpoint.trim_end_matches('/'),
        path.trim_start_matches('/')
    );
    let resp = client
        .post(&url)
        .timeout(Duration::from_millis(timeout_ms.clamp(100, 20_000)))
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

pub(super) async fn get_gateway(
    client: &reqwest::Client,
    timeout_ms: u64,
    endpoint: &str,
    path: &str,
    query: &[(&str, &str)],
) -> Result<Value, String> {
    let url = format!(
        "{}/{}",
        endpoint.trim_end_matches('/'),
        path.trim_start_matches('/')
    );
    let resp = client
        .get(&url)
        .timeout(Duration::from_millis(timeout_ms.clamp(100, 20_000)))
        .query(query)
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

pub(super) async fn fetch_gateway_book_snapshot(
    client: &reqwest::Client,
    gateway_cfg: &LiveGatewayConfig,
    token_id: &str,
) -> Option<GatewayBookSnapshot> {
    let mut endpoint = gateway_cfg.primary_url.clone();
    let mut resp = get_gateway(
        client,
        gateway_cfg.timeout_ms,
        &endpoint,
        "/book",
        &[("token_id", token_id)],
    )
    .await;
    if resp.is_err() {
        if let Some(backup) = gateway_cfg.backup_url.as_deref() {
            endpoint = backup.to_string();
            resp = get_gateway(
                client,
                gateway_cfg.timeout_ms,
                &endpoint,
                "/book",
                &[("token_id", token_id)],
            )
            .await;
        }
    }
    match resp {
        Ok(v) => GatewayBookSnapshot::from_value(&v).or_else(|| {
            tracing::warn!(
                token_id = token_id,
                endpoint = %endpoint,
                "gateway /book response missing required fields"
            );
            None
        }),
        Err(err) => {
            tracing::warn!(token_id = token_id, endpoint = %endpoint, error = %err, "gateway /book fetch failed");
            None
        }
    }
}

pub(super) async fn submit_gateway_order(
    client: &reqwest::Client,
    gateway_cfg: &LiveGatewayConfig,
    payload: &Value,
) -> (Result<Value, String>, String) {
    let mut used_endpoint = gateway_cfg.primary_url.clone();
    let _ = post_gateway(
        client,
        gateway_cfg.timeout_ms,
        &gateway_cfg.primary_url,
        "/prebuild_order",
        payload,
    )
    .await;
    let mut result = post_gateway(
        client,
        gateway_cfg.timeout_ms,
        &gateway_cfg.primary_url,
        "/orders",
        payload,
    )
    .await;
    if result.is_err() {
        if let Some(backup) = gateway_cfg.backup_url.as_deref() {
            used_endpoint = backup.to_string();
            let _ = post_gateway(
                client,
                gateway_cfg.timeout_ms,
                backup,
                "/prebuild_order",
                payload,
            )
            .await;
            result = post_gateway(client, gateway_cfg.timeout_ms, backup, "/orders", payload).await;
        }
    }
    (result, used_endpoint)
}

pub(super) fn extract_gateway_reject_reason(payload: &Value) -> String {
    payload
        .get("reject_code")
        .and_then(Value::as_str)
        .or_else(|| payload.get("error").and_then(Value::as_str))
        .or_else(|| payload.get("reason").and_then(Value::as_str))
        .unwrap_or("unknown_reject")
        .to_string()
}

pub(super) fn extract_rust_reject_reason(payload: &Value, fallback_error: Option<&str>) -> String {
    payload
        .get("error_msg")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .or_else(|| payload.get("status").and_then(Value::as_str))
        .or_else(|| payload.get("error").and_then(Value::as_str))
        .or(fallback_error)
        .unwrap_or("unknown_reject")
        .to_string()
}

pub(super) fn can_retry_on_liquidity(reason: &str) -> bool {
    let r = reason.to_ascii_lowercase();
    r.contains("no orders found to match")
        || r.contains("insufficient liquidity")
        || r.contains("cannot be matched")
        || r.contains("would not fill")
        || r.contains("unmatched")
        || r.contains("canceled")
        || r.contains("cancelled")
        || r.contains("rejected")
        || r.contains("timeout")
}

pub(super) fn is_live_exit_action(action: &str) -> bool {
    let a = action.to_ascii_lowercase();
    a == "exit" || a == "reduce"
}

pub(super) fn is_emergency_exit_reason(reason: &str) -> bool {
    let r = reason.to_ascii_lowercase();
    r.contains("stop_loss")
        || r.contains("signal_reverse")
        || r.contains("trail_drawdown")
        || r.contains("liquidity_widen")
        || r.contains("round_rollover")
        || r.contains("blocked_exit_escalation")
        || r.contains("panic_exit")
        || r.contains("emergency")
}

pub(super) fn decision_is_emergency_exit(decision: &Value) -> bool {
    let action = decision
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !is_live_exit_action(action) {
        return false;
    }
    decision
        .get("reason")
        .and_then(Value::as_str)
        .map(is_emergency_exit_reason)
        .unwrap_or(false)
}

pub(super) fn apply_emergency_exit_overrides(
    decision: &mut Value,
    gateway_cfg: &LiveGatewayConfig,
) {
    if !decision_is_emergency_exit(decision) {
        return;
    }
    if let Some(obj) = decision.as_object_mut() {
        obj.insert("tif".to_string(), json!("FAK"));
        obj.insert("style".to_string(), json!("taker"));
        let ttl_ms = obj
            .get("ttl_ms")
            .and_then(Value::as_i64)
            .unwrap_or(900)
            .min(700)
            .max(350);
        obj.insert("ttl_ms".to_string(), json!(ttl_ms));
        let slippage = obj
            .get("max_slippage_bps")
            .and_then(Value::as_f64)
            .unwrap_or(gateway_cfg.exit_slippage_bps)
            .max(gateway_cfg.exit_slippage_bps + 14.0)
            .max(34.0)
            .min(140.0);
        obj.insert("max_slippage_bps".to_string(), json!(slippage));
        obj.insert("emergency_exit".to_string(), Value::Bool(true));
    }
}

pub(super) fn build_retry_payload(current: &Value, reason: &str, attempt: usize) -> Option<Value> {
    if attempt >= 2 || !can_retry_on_liquidity(reason) {
        return None;
    }
    let action = current
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let is_exit_like = is_live_exit_action(&action);
    let emergency_exit = current
        .get("reason")
        .and_then(Value::as_str)
        .map(is_emergency_exit_reason)
        .unwrap_or(false);
    let mut next = current.clone();
    let current_slippage = current
        .get("max_slippage_bps")
        .and_then(Value::as_f64)
        .unwrap_or(20.0)
        .clamp(0.0, 500.0);
    let current_ttl = current
        .get("ttl_ms")
        .and_then(Value::as_i64)
        .unwrap_or(1200)
        .clamp(300, 30_000);
    let current_tif = current
        .get("tif")
        .and_then(Value::as_str)
        .unwrap_or("FAK")
        .to_ascii_uppercase();
    let maker_mode = matches!(current_tif.as_str(), "GTD" | "GTC" | "POST_ONLY");
    let side = current
        .get("side")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let is_buy = side.starts_with("buy_");
    let is_taker_like = matches!(current_tif.as_str(), "FAK" | "FOK")
        || current
            .get("style")
            .and_then(Value::as_str)
            .map(|s| s.eq_ignore_ascii_case("taker"))
            .unwrap_or(false);
    let tick_size = current
        .get("book_meta")
        .and_then(|v| v.get("tick_size"))
        .and_then(Value::as_f64)
        .unwrap_or(0.01)
        .max(0.0001);
    let current_price = current
        .get("price")
        .and_then(Value::as_f64)
        .unwrap_or(0.5)
        .clamp(0.01, 0.99);

    if is_taker_like {
        let (slip_boost, ttl_target, tick_steps) = if attempt == 0 {
            if is_exit_like {
                if emergency_exit {
                    (24.0, 520_i64, 1_u32)
                } else {
                    (16.0, 620_i64, 1_u32)
                }
            } else {
                (12.0, 900_i64, 1_u32)
            }
        } else {
            // Final taker step: favor certainty of exit/fill over price.
            if is_exit_like {
                if emergency_exit {
                    (36.0, 420_i64, 3_u32)
                } else {
                    (28.0, 520_i64, 2_u32)
                }
            } else {
                (18.0, 760_i64, 2_u32)
            }
        };
        let boosted_slippage =
            (current_slippage + slip_boost).min(if is_exit_like { 180.0 } else { 120.0 });
        let shifted_price = {
            let step = tick_size * (tick_steps as f64);
            let raw = if is_buy {
                current_price + step
            } else {
                current_price - step
            };
            round_to_tick(raw.clamp(0.01, 0.99), tick_size, is_buy)
        };
        let next_ttl = if is_exit_like {
            current_ttl.min(ttl_target).max(320)
        } else {
            current_ttl.min(ttl_target).max(420)
        };
        if let Some(obj) = next.as_object_mut() {
            obj.insert("tif".to_string(), json!("FAK"));
            obj.insert("style".to_string(), json!("taker"));
            obj.insert("price".to_string(), json!(shifted_price));
            obj.insert("max_slippage_bps".to_string(), json!(boosted_slippage));
            obj.insert("ttl_ms".to_string(), json!(next_ttl));
            obj.insert(
                "retry_tag".to_string(),
                json!(if is_exit_like {
                    if attempt == 0 {
                        "exit_fak_ladder_step_1"
                    } else {
                        "exit_fak_ladder_step_2"
                    }
                } else {
                    if attempt == 0 {
                        "entry_fak_ladder_step_1"
                    } else {
                        "entry_fak_ladder_step_2"
                    }
                }),
            );
            let lock_retry_size = if is_exit_like
                || (matches!(action.as_str(), "enter" | "add")
                    && live_fixed_entry_size_shares().is_some())
            {
                obj.get("size")
                    .and_then(Value::as_f64)
                    .filter(|v| v.is_finite() && *v > 0.0)
                    .or_else(|| {
                        if matches!(action.as_str(), "enter" | "add") {
                            live_fixed_entry_size_shares()
                        } else {
                            None
                        }
                    })
            } else {
                None
            };
            if let Some(sz) = lock_retry_size {
                obj.insert(
                    "size".to_string(),
                    json!((sz * 10_000.0).round() / 10_000.0),
                );
            } else if let Some(q) = obj
                .get("quote_size_usdc")
                .and_then(Value::as_f64)
                .or_else(|| obj.get("requested_notional_usdc").and_then(Value::as_f64))
                .filter(|v| v.is_finite() && *v > 0.0)
            {
                let resized = (q / shifted_price).max(0.01);
                obj.insert(
                    "size".to_string(),
                    json!((resized * 10_000.0).round() / 10_000.0),
                );
            }
        }
        return Some(next);
    }

    if maker_mode {
        let entry_like = is_entry_action(&action);
        let ttl_ms = if entry_like {
            live_entry_fak_ttl_ms()
        } else {
            (current_ttl / 2).clamp(520, 1_400)
        };
        let slip_boost = if is_exit_like {
            20.0
        } else {
            live_entry_fak_slippage_boost_bps()
        };
        if let Some(obj) = next.as_object_mut() {
            obj.insert("tif".to_string(), json!("FAK"));
            obj.insert("style".to_string(), json!("taker"));
            obj.insert("ttl_ms".to_string(), json!(ttl_ms));
            obj.insert(
                "max_slippage_bps".to_string(),
                json!((current_slippage + slip_boost).min(160.0)),
            );
            obj.insert("retry_tag".to_string(), json!("maker_timeout_to_fak"));
        }
        return Some(next);
    }

    None
}

fn build_execution_price_trace(
    decision: &Value,
    request: &Value,
    final_request: &Value,
    response: Option<&Value>,
) -> Value {
    let signal_price_cents = decision.get("price_cents").and_then(Value::as_f64);
    let submit_price_cents = request
        .get("price")
        .and_then(Value::as_f64)
        .map(|v| (v * 100.0).clamp(0.0, 100.0));
    let final_submit_price_cents = final_request
        .get("price")
        .and_then(Value::as_f64)
        .map(|v| (v * 100.0).clamp(0.0, 100.0));
    let accepted_price_cents = response
        .and_then(|v| v.get("price").and_then(Value::as_f64))
        .map(|v| (v * 100.0).clamp(0.0, 100.0))
        .or(final_submit_price_cents);
    let signal_vs_submit_cents = match (signal_price_cents, submit_price_cents) {
        (Some(signal), Some(submit)) => Some(submit - signal),
        _ => None,
    };
    let signal_vs_accepted_cents = match (signal_price_cents, accepted_price_cents) {
        (Some(signal), Some(accepted)) => Some(accepted - signal),
        _ => None,
    };
    json!({
        "signal_price_cents": signal_price_cents,
        "submit_price_cents": submit_price_cents,
        "final_submit_price_cents": final_submit_price_cents,
        "accepted_price_cents": accepted_price_cents,
        "fill_price_cents": Value::Null,
        "signal_vs_submit_cents": signal_vs_submit_cents,
        "signal_vs_accepted_cents": signal_vs_accepted_cents,
    })
}

pub(super) async fn get_gateway_reports(
    client: &reqwest::Client,
    timeout_ms: u64,
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
        .timeout(Duration::from_millis(timeout_ms.clamp(100, 20_000)))
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

pub(super) async fn delete_gateway_order(
    client: &reqwest::Client,
    timeout_ms: u64,
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
        .timeout(Duration::from_millis(timeout_ms.clamp(100, 20_000)))
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

pub(super) fn extract_order_id_from_gateway_record(row: &Value) -> Option<String> {
    row.get("response")
        .and_then(|v| v.get("order_id").and_then(Value::as_str))
        .map(str::to_string)
        .or_else(|| {
            row.get("response")
                .and_then(|v| v.get("id").and_then(Value::as_str))
                .map(str::to_string)
        })
        .or_else(|| {
            row.get("order_id")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
}

#[derive(Debug, Clone, Default)]
struct PendingFillMeta {
    fill_price_cents: Option<f64>,
    fill_quote_usdc: Option<f64>,
    fill_size_shares: Option<f64>,
    fee_usdc: f64,
    slippage_usdc: f64,
}

fn value_as_f64(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
}

fn collect_f64_by_key(node: &Value, target_key: &str, out: &mut Vec<f64>) {
    match node {
        Value::Object(map) => {
            for (k, v) in map {
                if k.eq_ignore_ascii_case(target_key) {
                    if let Some(x) = value_as_f64(v) {
                        if x.is_finite() {
                            out.push(x);
                        }
                    }
                }
                collect_f64_by_key(v, target_key, out);
            }
        }
        Value::Array(rows) => {
            for row in rows {
                collect_f64_by_key(row, target_key, out);
            }
        }
        _ => {}
    }
}

fn collect_candidates(node: &Value, keys: &[&str]) -> Vec<f64> {
    let mut out = Vec::<f64>::new();
    for key in keys {
        collect_f64_by_key(node, key, &mut out);
    }
    out
}

fn normalize_usdc_amount(v: f64) -> f64 {
    if v <= 0.0 || !v.is_finite() {
        0.0
    } else if v > 100_000.0 {
        // Some gateways return 6-decimal scaled collateral integers.
        (v / 1_000_000.0).max(0.0)
    } else {
        v
    }
}

fn extract_pending_fill_meta(
    fill_event: Option<&Value>,
    pending: &LivePendingOrder,
) -> PendingFillMeta {
    let Some(fill) = fill_event else {
        return PendingFillMeta::default();
    };
    let price_cents_direct = collect_candidates(
        fill,
        &[
            "price_cents",
            "fill_price_cents",
            "filled_price_cents",
            "avg_price_cents",
        ],
    )
    .into_iter()
    .find(|v| *v > 0.0);
    let price_prob = collect_candidates(
        fill,
        &[
            "price",
            "avg_price",
            "fill_price",
            "filled_price",
            "execution_price",
            "executed_price",
            "matched_price",
        ],
    )
    .into_iter()
    .find(|v| *v > 0.0);
    let fill_price_cents =
        price_cents_direct.or_else(|| price_prob.map(|v| if v <= 1.5 { v * 100.0 } else { v }));

    let quote_direct = collect_candidates(
        fill,
        &[
            "fill_quote_usdc",
            "filled_quote_usdc",
            "executed_quote_usdc",
            "effective_notional",
            "filled_notional",
            "executed_notional",
            "notional",
            "quote_size_usdc",
        ],
    )
    .into_iter()
    .map(normalize_usdc_amount)
    .find(|v| *v > 0.0);

    let size_candidates = collect_candidates(
        fill,
        &[
            "fill_size_shares",
            "size_matched",
            "filled_size",
            "executed_size",
            "matched_size",
            "accepted_size",
            "size",
        ],
    );
    let fill_size_shares = size_candidates.into_iter().find(|v| *v > 0.0);
    let quote_from_size = if let (Some(px_c), Some(sz)) = (
        fill_price_cents,
        fill_size_shares,
    ) {
        Some((px_c / 100.0) * sz)
    } else {
        None
    };
    let fill_quote_usdc = quote_direct.or(quote_from_size);

    let fee_usdc = collect_candidates(
        fill,
        &["fee_usdc", "total_fee_usdc", "fee", "fees", "total_fee"],
    )
    .into_iter()
    .map(normalize_usdc_amount)
    .fold(0.0, |acc, v| acc + v);

    let explicit_slippage_usdc = collect_candidates(fill, &["slippage_usdc"])
        .into_iter()
        .map(normalize_usdc_amount)
        .fold(0.0, |acc, v| acc + v);

    let inferred_slippage_usdc =
        if let (Some(px_c), Some(quote)) = (fill_price_cents, fill_quote_usdc) {
            let expected_c = pending.price_cents.max(0.01);
            let shares = quote / (px_c / 100.0).max(0.0001);
            let action = pending.action.to_ascii_lowercase();
            let adverse_cents = if action == "enter" || action == "add" {
                (px_c - expected_c).max(0.0)
            } else {
                (expected_c - px_c).max(0.0)
            };
            ((adverse_cents / 100.0) * shares).max(0.0)
        } else {
            0.0
        };

    PendingFillMeta {
        fill_price_cents,
        fill_quote_usdc,
        fill_size_shares,
        fee_usdc,
        slippage_usdc: explicit_slippage_usdc.max(inferred_slippage_usdc),
    }
}

pub(super) async fn apply_pending_confirmation(
    state: &ApiState,
    pending: &LivePendingOrder,
    reason: &str,
    fill_event: Option<&Value>,
) {
    let now_ms = Utc::now().timestamp_millis();
    let mut ps = state.get_live_position_state(&pending.market_type).await;
    let capital_cfg = LiveCapitalConfig::from_env();
    let action = pending.action.to_ascii_lowercase();
    let fill_meta = extract_pending_fill_meta(fill_event, pending);
    let fill_price_cents = fill_meta
        .fill_price_cents
        .unwrap_or(pending.price_cents)
        .max(0.01);
    let fill_size_shares = fill_meta
        .fill_size_shares
        .filter(|v| v.is_finite() && *v > 0.0);
    let fill_quote_usdc = fill_meta
        .fill_quote_usdc
        .or_else(|| fill_size_shares.map(|sz| sz * (fill_price_cents / 100.0)))
        .unwrap_or(pending.quote_size_usdc.max(0.0))
        .max(0.0);
    let fill_shares = if let Some(sz) = fill_size_shares {
        sz.max(0.0)
    } else if fill_meta.fill_quote_usdc.is_some() {
        (fill_quote_usdc / (fill_price_cents / 100.0).max(0.0001)).max(0.0)
    } else if pending.order_size_shares > 0.0 {
        pending.order_size_shares.max(0.0)
    } else {
        (fill_quote_usdc / (fill_price_cents / 100.0).max(0.0001)).max(0.0)
    };
    let fill_cost_usdc = (fill_meta.fee_usdc + fill_meta.slippage_usdc).max(0.0);
    let mut realized_pnl_usdc = 0.0_f64;
    if action == "enter" {
        ps.state = "in_position".to_string();
        ps.side = Some(pending.side.clone());
        ps.entry_round_id = Some(pending.round_id.clone());
        ps.entry_market_id = if pending.market_id.trim().is_empty() {
            None
        } else {
            Some(pending.market_id.trim().to_string())
        };
        ps.entry_token_id = if pending.token_id.trim().is_empty() {
            None
        } else {
            Some(pending.token_id.trim().to_string())
        };
        ps.entry_ts_ms = Some(pending.submitted_ts_ms);
        ps.entry_price_cents = Some(fill_price_cents);
        ps.entry_quote_usdc = Some(fill_quote_usdc);
        ps.net_quote_usdc = fill_quote_usdc;
        ps.vwap_entry_cents = Some(fill_price_cents);
        ps.open_add_layers = 0;
        ps.position_cost_usdc = fill_cost_usdc;
        ps.position_size_shares = fill_shares.max(0.0);
        ps.total_entries = ps.total_entries.saturating_add(1);
        ps.last_fill_pnl_usdc = 0.0;
    } else if action == "add" {
        let prev_quote = ps
            .net_quote_usdc
            .max(ps.entry_quote_usdc.unwrap_or(0.0))
            .max(0.0);
        let add_quote = fill_quote_usdc;
        let prev_price = ps
            .vwap_entry_cents
            .or(ps.entry_price_cents)
            .unwrap_or(fill_price_cents);
        let next_quote = prev_quote + add_quote;
        let next_price = if next_quote > 1e-9 {
            (prev_price * prev_quote + fill_price_cents * add_quote) / next_quote
        } else {
            fill_price_cents
        };
        ps.state = "in_position".to_string();
        ps.side = Some(pending.side.clone());
        ps.entry_round_id = Some(pending.round_id.clone());
        if ps
            .entry_market_id
            .as_deref()
            .map(|v| v.trim().is_empty())
            .unwrap_or(true)
            && !pending.market_id.trim().is_empty()
        {
            ps.entry_market_id = Some(pending.market_id.trim().to_string());
        }
        if ps
            .entry_token_id
            .as_deref()
            .map(|v| v.trim().is_empty())
            .unwrap_or(true)
            && !pending.token_id.trim().is_empty()
        {
            ps.entry_token_id = Some(pending.token_id.trim().to_string());
        }
        ps.entry_ts_ms = Some(pending.submitted_ts_ms);
        ps.entry_price_cents = Some(next_price);
        ps.vwap_entry_cents = Some(next_price);
        ps.entry_quote_usdc = Some(next_quote);
        ps.net_quote_usdc = next_quote;
        ps.total_adds = ps.total_adds.saturating_add(1);
        ps.open_add_layers = ps.open_add_layers.saturating_add(1);
        ps.position_cost_usdc += fill_cost_usdc;
        ps.position_size_shares = (ps.position_size_shares + fill_shares).max(0.0);
        ps.last_fill_pnl_usdc = 0.0;
    } else if action == "reduce" || action == "exit" {
        let entry_price_cents = ps
            .vwap_entry_cents
            .or(ps.entry_price_cents)
            .unwrap_or(0.0)
            .max(0.01);
        let prev_shares = ps.position_size_shares.max(0.0);
        let close_shares = if action == "exit" {
            prev_shares
        } else {
            fill_shares.max(0.0).min(prev_shares)
        };
        if close_shares > 1e-9 {
            let gross_pnl_usdc = close_shares * ((fill_price_cents - entry_price_cents) / 100.0);
            let entry_cost_alloc = if prev_shares > 1e-9 {
                ps.position_cost_usdc * (close_shares / prev_shares).clamp(0.0, 1.0)
            } else {
                0.0
            };
            realized_pnl_usdc = gross_pnl_usdc - entry_cost_alloc - fill_cost_usdc;
            ps.position_cost_usdc = (ps.position_cost_usdc - entry_cost_alloc).max(0.0);
            ps.realized_pnl_usdc += realized_pnl_usdc;
            ps.last_fill_pnl_usdc = realized_pnl_usdc;
        } else {
            ps.last_fill_pnl_usdc = 0.0;
        }
        let remaining_shares = (prev_shares - close_shares).max(0.0);
        let remaining_quote = (remaining_shares * (entry_price_cents / 100.0)).max(0.0);
        if action == "exit" || remaining_shares <= 1e-6 {
            ps.state = "flat".to_string();
            ps.side = None;
            ps.entry_round_id = None;
            ps.entry_market_id = None;
            ps.entry_token_id = None;
            ps.entry_ts_ms = None;
            ps.entry_price_cents = None;
            ps.entry_quote_usdc = None;
            ps.net_quote_usdc = 0.0;
            ps.vwap_entry_cents = None;
            ps.open_add_layers = 0;
            ps.position_cost_usdc = 0.0;
            ps.position_size_shares = 0.0;
            ps.total_exits = ps.total_exits.saturating_add(1);
        } else {
            ps.state = "in_position".to_string();
            ps.entry_quote_usdc = Some(remaining_quote);
            ps.net_quote_usdc = remaining_quote;
            ps.position_size_shares = remaining_shares;
            ps.open_add_layers = ps.open_add_layers.saturating_sub(1);
            ps.total_reduces = ps.total_reduces.saturating_add(1);
        }
    }
    ps.last_action = Some(format!("{}_{}", pending.action, pending.side));
    ps.last_reason = Some(reason.to_string());
    ps.updated_ts_ms = now_ms;
    state
        .put_live_position_state(&pending.market_type, ps)
        .await;
    if realized_pnl_usdc.is_finite() && realized_pnl_usdc.abs() > 1e-9 {
        let _ = state
            .update_capital_after_realized_pnl(
                &pending.market_type,
                &capital_cfg,
                realized_pnl_usdc,
            )
            .await;
    } else if action == "enter" || action == "add" {
        let _ = state
            .update_capital_after_realized_pnl(&pending.market_type, &capital_cfg, 0.0)
            .await;
    }
}

pub(super) async fn apply_pending_revert(
    state: &ApiState,
    pending: &LivePendingOrder,
    reason: &str,
) {
    let now_ms = Utc::now().timestamp_millis();
    let mut ps = state.get_live_position_state(&pending.market_type).await;
    let action = pending.action.to_ascii_lowercase();
    if action == "enter" {
        ps.state = "flat".to_string();
        ps.side = None;
        ps.entry_round_id = None;
        ps.entry_market_id = None;
        ps.entry_token_id = None;
        ps.entry_ts_ms = None;
        ps.entry_price_cents = None;
        ps.entry_quote_usdc = None;
        ps.net_quote_usdc = 0.0;
        ps.vwap_entry_cents = None;
        ps.open_add_layers = 0;
    } else if action == "exit" || action == "reduce" {
        ps.state = "in_position".to_string();
        if ps.side.is_none() {
            ps.side = Some(pending.side.clone());
        }
    }
    ps.last_action = Some(format!("{}_{}", pending.action, pending.side));
    ps.last_reason = Some(reason.to_string());
    ps.updated_ts_ms = now_ms;
    state
        .put_live_position_state(&pending.market_type, ps)
        .await;
}

pub(super) async fn reconcile_gateway_reports(state: &ApiState, gateway_cfg: &LiveGatewayConfig) {
    let since_seq = state.get_gateway_report_seq().await;
    let client = state.gateway_http_client.as_ref();
    let mut endpoint = gateway_cfg.primary_url.clone();
    let mut response =
        get_gateway_reports(client, gateway_cfg.timeout_ms, &endpoint, since_seq, 240).await;
    if response.is_err() {
        if let Some(backup) = gateway_cfg.backup_url.as_deref() {
            endpoint = backup.to_string();
            response =
                get_gateway_reports(client, gateway_cfg.timeout_ms, &endpoint, since_seq, 240)
                    .await;
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
            apply_pending_confirmation(state, &pending, "order_terminal_filled", Some(&ev)).await;
        } else if event_name == "order_terminal" && canceled_terminal {
            apply_pending_revert(state, &pending, "order_terminal_canceled").await;
        } else if event_name == "order_timeout_cancelled" {
            apply_pending_revert(state, &pending, "timeout_cancelled").await;
        } else if event_name == "order_timeout_cancel_failed" {
            handle_cancel_failure_with_escalation(
                state,
                &pending,
                "timeout_cancel_failed",
                json!({
                    "gateway_endpoint": endpoint,
                    "event": ev
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

pub(super) async fn handle_pending_timeouts(state: &ApiState, gateway_cfg: &LiveGatewayConfig) {
    let now_ms = Utc::now().timestamp_millis();
    let pending = state.list_pending_orders().await;
    if pending.is_empty() {
        return;
    }
    let client = state.gateway_http_client.as_ref();
    for row in pending {
        let elapsed = now_ms.saturating_sub(row.submitted_ts_ms);
        if elapsed < pending_cancel_due_ms(&row) {
            continue;
        }
        let mut endpoint = gateway_cfg.primary_url.clone();
        let mut cancel_res =
            delete_gateway_order(client, gateway_cfg.timeout_ms, &endpoint, &row.order_id).await;
        if cancel_res.is_err() {
            if let Some(backup) = gateway_cfg.backup_url.as_deref() {
                endpoint = backup.to_string();
                cancel_res =
                    delete_gateway_order(client, gateway_cfg.timeout_ms, &endpoint, &row.order_id)
                        .await;
            }
        }
        match cancel_res {
            Ok(v) => {
                let _ = state.remove_pending_order(&row.order_id).await;
                let maker_tif = matches!(
                    row.tif.to_ascii_uppercase().as_str(),
                    "GTD" | "GTC" | "POST_ONLY"
                );
                let exit_like = is_live_exit_action(&row.action);
                let emergency_exit = exit_like && is_emergency_exit_reason(&row.submit_reason);
                let can_retry = if emergency_exit {
                    row.retry_count < 2
                } else {
                    row.retry_count < 1
                };
                if (maker_tif || emergency_exit) && can_retry {
                    let entry_like = is_entry_action(&row.action);
                    let fallback_ttl_ms = if emergency_exit {
                        700
                    } else if entry_like {
                        live_entry_fak_ttl_ms()
                    } else {
                        900
                    };
                    let fallback_slippage_bps = if emergency_exit {
                        (gateway_cfg.exit_slippage_bps + 18.0).max(38.0)
                    } else if entry_like {
                        gateway_cfg
                            .exit_slippage_bps
                            .max(gateway_cfg.entry_slippage_bps)
                            + live_entry_fak_slippage_boost_bps()
                    } else {
                        gateway_cfg
                            .exit_slippage_bps
                            .max(gateway_cfg.entry_slippage_bps)
                            + 10.0
                    };
                    let maybe_target =
                        resolve_live_market_target_with_state(state, &row.market_type)
                            .await
                            .ok();
                    if let Some(target) = maybe_target {
                        let mut effective_target = target.clone();
                        apply_pending_target_override(&mut effective_target, &row);
                        let round_check = json!({ "round_id": row.round_id });
                        let bypass_round_guard = is_live_exit_action(&row.action)
                            && !row.market_id.trim().is_empty()
                            && !row.token_id.trim().is_empty();
                        if !decision_round_matches_target(&round_check, &effective_target)
                            && !bypass_round_guard
                        {
                            state
                                .append_live_event(
                                    &row.market_type,
                                    json!({
                                        "accepted": false,
                                        "action": row.action,
                                        "side": row.side,
                                        "round_id": row.round_id,
                                        "reason": "local_timeout_cancelled_round_target_mismatch",
                                        "order_id": row.order_id,
                                        "target_market_id": effective_target.market_id,
                                        "target_end_date": effective_target.end_date
                                    }),
                                )
                                .await;
                            apply_pending_revert(state, &row, "local_timeout_cancel").await;
                            continue;
                        }
                        let decision = json!({
                            "action": row.action,
                            "side": row.side,
                            "round_id": row.round_id,
                            "price_cents": row.price_cents,
                            "quote_size_usdc": row.quote_size_usdc,
                            "reason": format!("{}_timeout_fallback_fak", row.submit_reason),
                            "tif": "FAK",
                            "style": "taker",
                            "ttl_ms": fallback_ttl_ms,
                            "max_slippage_bps": fallback_slippage_bps
                        });
                        let token_id =
                            token_id_for_decision(&decision, &effective_target).map(str::to_string);
                        let book_snapshot = if let Some(token_id) = token_id {
                            fetch_gateway_book_snapshot(client, gateway_cfg, &token_id).await
                        } else {
                            None
                        };
                        if let Some(payload) = decision_to_live_payload(
                            &decision,
                            &effective_target,
                            gateway_cfg,
                            book_snapshot.as_ref(),
                            Some(row.quote_size_usdc),
                        ) {
                            let (submit_res, submit_endpoint) =
                                submit_gateway_order(client, gateway_cfg, &payload).await;
                            if let Ok(resp) = submit_res {
                                let accepted = resp
                                    .get("accepted")
                                    .and_then(Value::as_bool)
                                    .unwrap_or(false);
                                if accepted {
                                    let order_id = extract_order_id_from_gateway_record(&resp)
                                        .or_else(|| {
                                            extract_order_id_from_gateway_record(
                                                &json!({ "response": resp.clone() }),
                                            )
                                        });
                                    if let Some(order_id) = order_id {
                                        let mut pending = row.clone();
                                        pending.order_id = order_id;
                                        pending.retry_count = pending.retry_count.saturating_add(1);
                                        pending.tif = "FAK".to_string();
                                        pending.style = "taker".to_string();
                                        pending.submitted_ts_ms = now_ms;
                                        pending.cancel_after_ms = if emergency_exit {
                                            700
                                        } else if entry_like {
                                            live_entry_fak_cancel_after_ms()
                                        } else {
                                            1500
                                        };
                                        state.upsert_pending_order(pending).await;
                                        state
                                            .append_live_event(
                                                &row.market_type,
                                                json!({
                                                    "accepted": true,
                                                    "action": row.action,
                                                    "side": row.side,
                                                    "round_id": row.round_id,
                                                    "reason": "local_timeout_cancelled_resubmitted_fak",
                                                    "order_id": row.order_id,
                                                    "gateway_endpoint": submit_endpoint,
                                                    "cancel_response": v.clone(),
                                                    "submit_response": resp,
                                                    "book_snapshot": book_snapshot,
                                                    "emergency_exit": emergency_exit
                                                }),
                                            )
                                            .await;
                                        continue;
                                    }
                                }
                                state
                                    .append_live_event(
                                        &row.market_type,
                                        json!({
                                            "accepted": false,
                                            "action": row.action,
                                            "side": row.side,
                                            "round_id": row.round_id,
                                            "reason": "local_timeout_cancelled_fak_resubmit_rejected",
                                            "order_id": row.order_id,
                                            "gateway_endpoint": submit_endpoint,
                                            "cancel_response": v.clone(),
                                            "submit_response": resp
                                        }),
                                    )
                                    .await;
                            }
                        }
                    }
                }
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
                handle_cancel_failure_with_escalation(
                    state,
                    &row,
                    "local_timeout_cancel_failed",
                    json!({
                        "gateway_endpoint": endpoint,
                        "error": err
                    }),
                )
                .await;
            }
        }
    }
}

pub(super) fn token_id_for_decision<'a>(
    decision: &Value,
    target: &'a LiveMarketTarget,
) -> Option<&'a str> {
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
    match (action.as_str(), side.as_str()) {
        ("enter", "UP") | ("add", "UP") | ("reduce", "UP") | ("exit", "UP") => {
            Some(target.token_id_yes.as_str())
        }
        ("enter", "DOWN") | ("add", "DOWN") | ("reduce", "DOWN") | ("exit", "DOWN") => {
            Some(target.token_id_no.as_str())
        }
        _ => None,
    }
}

fn align_exit_decision_side_with_position(
    decision: &mut Value,
    position_state: &LivePositionState,
) -> bool {
    let action = decision
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !is_live_exit_action(&action) {
        return false;
    }
    let Some(locked_side) = position_state.side.as_deref() else {
        return false;
    };
    let locked_side = locked_side.trim().to_ascii_uppercase();
    if locked_side.is_empty() {
        return false;
    }
    let current_side = decision
        .get("side")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase();
    if current_side == locked_side {
        return false;
    }
    if let Some(obj) = decision.as_object_mut() {
        obj.insert("side".to_string(), json!(locked_side));
        obj.insert(
            "execution_notes_override".to_string(),
            json!("exit_side_aligned_to_locked_position"),
        );
        return true;
    }
    false
}

fn apply_pending_target_override(target: &mut LiveMarketTarget, row: &LivePendingOrder) {
    if !row.market_id.trim().is_empty() {
        target.market_id = row.market_id.trim().to_string();
    }
    if !row.token_id.trim().is_empty() {
        let token = row.token_id.trim().to_string();
        if row.side.eq_ignore_ascii_case("UP") {
            target.token_id_yes = token.clone();
            if target.token_id_no.trim().is_empty() {
                target.token_id_no = token;
            }
        } else if row.side.eq_ignore_ascii_case("DOWN") {
            target.token_id_no = token.clone();
            if target.token_id_yes.trim().is_empty() {
                target.token_id_yes = token;
            }
        } else {
            target.token_id_yes = token.clone();
            target.token_id_no = token;
        }
    }
    if let Some(end_ms) = parse_round_end_ts_ms(&row.round_id) {
        target.end_date = end_date_iso_from_ms(end_ms);
    }
}

fn build_effective_target_for_decision(
    target: &LiveMarketTarget,
    decision: &Value,
    position_state: &LivePositionState,
) -> LiveMarketTarget {
    let mut effective = target.clone();
    let action = decision
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !is_live_exit_action(&action) {
        return effective;
    }
    if let Some(mid) = position_state.entry_market_id.as_deref() {
        let mid = mid.trim();
        if !mid.is_empty() {
            effective.market_id = mid.to_string();
        }
    }
    if let Some(token) = position_state.entry_token_id.as_deref() {
        let token = token.trim();
        if !token.is_empty() {
            let pos_side = position_state
                .side
                .as_deref()
                .unwrap_or_default()
                .trim()
                .to_ascii_uppercase();
            if pos_side == "DOWN" {
                effective.token_id_no = token.to_string();
                if effective.token_id_yes.trim().is_empty() {
                    effective.token_id_yes = token.to_string();
                }
            } else {
                effective.token_id_yes = token.to_string();
                if effective.token_id_no.trim().is_empty() {
                    effective.token_id_no = token.to_string();
                }
            }
        }
    }
    if let Some(end_ms) = position_state
        .entry_round_id
        .as_deref()
        .and_then(parse_round_end_ts_ms)
    {
        effective.end_date = end_date_iso_from_ms(end_ms);
    }
    effective
}

fn should_bypass_round_match_for_locked_exit(
    decision: &Value,
    position_state: &LivePositionState,
) -> bool {
    let action = decision
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    is_live_exit_action(&action)
        && position_state.position_size_shares > 0.0
        && position_state
            .entry_market_id
            .as_deref()
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false)
        && position_state
            .entry_token_id
            .as_deref()
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false)
}

pub(super) fn build_position_locked_target(
    market_type: &str,
    position_state: &LivePositionState,
) -> Option<LiveMarketTarget> {
    let market_id = position_state.entry_market_id.as_deref()?.trim().to_string();
    let token_id = position_state.entry_token_id.as_deref()?.trim().to_string();
    if market_id.is_empty() || token_id.is_empty() {
        return None;
    }
    let side = position_state
        .side
        .as_deref()
        .unwrap_or("UP")
        .trim()
        .to_ascii_uppercase();
    let (token_id_yes, token_id_no) = if side == "DOWN" {
        (token_id.clone(), token_id.clone())
    } else {
        (token_id.clone(), token_id.clone())
    };
    let symbol = position_state
        .entry_round_id
        .as_deref()
        .and_then(|rid| rid.split('_').next())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("BTCUSDT")
        .to_string();
    let end_date = position_state
        .entry_round_id
        .as_deref()
        .and_then(parse_round_end_ts_ms)
        .and_then(end_date_iso_from_ms);
    Some(LiveMarketTarget {
        market_id,
        symbol,
        timeframe: market_type.to_string(),
        token_id_yes,
        token_id_no,
        end_date,
    })
}

pub(super) async fn execute_live_orders_via_gateway(
    state: &ApiState,
    gateway_cfg: &LiveGatewayConfig,
    target: &LiveMarketTarget,
    position_state: &LivePositionState,
    decisions: &[LiveGatedDecision],
) -> Vec<Value> {
    let client = state.gateway_http_client.as_ref();
    let mut out = Vec::<Value>::with_capacity(decisions.len());
    let token_ids = collect_decision_token_ids(target, decisions);
    let mut book_cache = prefetch_gateway_books_for_tokens(client, gateway_cfg, &token_ids).await;
    let exit_quote_override =
        position_state
            .entry_quote_usdc
            .and_then(|v| if v > 0.0 { Some(v) } else { None });
    let exit_size_override = if position_state.position_size_shares > 0.0 {
        Some(position_state.position_size_shares)
    } else {
        None
    };
    for gated in decisions {
        let mut decision = gated.decision.clone();
        let _side_aligned = align_exit_decision_side_with_position(&mut decision, position_state);
        let action = decision
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        let effective_target = build_effective_target_for_decision(target, &decision, position_state);
        let bypass_round_guard = should_bypass_round_match_for_locked_exit(&decision, position_state);
        if !decision_round_matches_target(&decision, &effective_target) && !bypass_round_guard {
            out.push(json!({
                "ok": false,
                "reason": "round_target_mismatch",
                "decision_key": gated.decision_key,
                "decision_round_id": decision.get("round_id").and_then(Value::as_str),
                "target_market_id": effective_target.market_id,
                "target_end_date": effective_target.end_date,
                "decision": decision
            }));
            continue;
        }
        if action == "exit" {
            if let Some(obj) = decision.as_object_mut() {
                if let Some(q) = exit_quote_override {
                    obj.insert("quote_size_usdc".to_string(), json!(q));
                }
                if let Some(sz) = exit_size_override {
                    obj.insert("position_size_shares".to_string(), json!(sz));
                }
            }
        }
        apply_emergency_exit_overrides(&mut decision, gateway_cfg);
        let token_id = token_id_for_decision(&decision, &effective_target).map(str::to_string);
        let book_snapshot = if let Some(token_id) = token_id.clone() {
            match book_cache.get(&token_id) {
                Some(cached) => cached.clone(),
                None => {
                    let fetched = fetch_gateway_book_snapshot(client, gateway_cfg, &token_id).await;
                    book_cache.insert(token_id.clone(), fetched.clone());
                    fetched
                }
            }
        } else {
            None
        };
        let Some(payload) =
            decision_to_live_payload(
                &decision,
                &effective_target,
                gateway_cfg,
                book_snapshot.as_ref(),
                None,
            )
        else {
            out.push(json!({
                "ok": false,
                "reason": "decision_to_payload_failed",
                "decision_key": gated.decision_key,
                "decision": decision,
                "book_snapshot": book_snapshot
            }));
            continue;
        };
        let mut attempt_payload = payload.clone();
        let mut attempts = Vec::<Value>::with_capacity(3);
        let mut accepted = false;
        let mut final_endpoint = gateway_cfg.primary_url.clone();
        let mut final_response: Option<Value> = None;
        let mut final_error: Option<String> = None;

        let order_started = Instant::now();
        for attempt in 0..3usize {
            let attempt_started = Instant::now();
            let (result, used_endpoint) =
                submit_gateway_order(client, gateway_cfg, &attempt_payload).await;
            final_endpoint = used_endpoint.clone();
            let latency_ms = attempt_started.elapsed().as_millis() as u64;
            match result {
                Ok(v) => {
                    let accept = v.get("accepted").and_then(Value::as_bool).unwrap_or(false);
                    let reject_reason = if accept {
                        String::new()
                    } else {
                        extract_gateway_reject_reason(&v)
                    };
                    attempts.push(json!({
                        "attempt": attempt + 1,
                        "endpoint": used_endpoint,
                        "latency_ms": latency_ms,
                        "request": attempt_payload.clone(),
                        "accepted": accept,
                        "reject_reason": if reject_reason.is_empty() { Value::Null } else { json!(reject_reason.clone()) },
                        "response": v
                    }));
                    final_response = Some(v.clone());
                    if accept {
                        accepted = true;
                        break;
                    }
                    if let Some(next_payload) =
                        build_retry_payload(&attempt_payload, &reject_reason, attempt)
                    {
                        attempt_payload = next_payload;
                        continue;
                    }
                    break;
                }
                Err(err) => {
                    attempts.push(json!({
                        "attempt": attempt + 1,
                        "endpoint": used_endpoint,
                        "latency_ms": latency_ms,
                        "request": attempt_payload.clone(),
                        "accepted": false,
                        "error": err
                    }));
                    final_error = Some(err.clone());
                    if let Some(next_payload) = build_retry_payload(&attempt_payload, &err, attempt)
                    {
                        attempt_payload = next_payload;
                        continue;
                    }
                    break;
                }
            }
        }
        out.push(json!({
            "ok": accepted,
            "accepted": accepted,
            "decision_key": gated.decision_key,
            "decision": decision,
            "endpoint": final_endpoint,
            "request": payload,
            "final_request": attempt_payload.clone(),
            "response": final_response,
            "error": final_error,
            "attempts": attempts,
            "order_latency_ms": order_started.elapsed().as_millis() as u64,
            "book_snapshot": book_snapshot,
            "target_market_id": effective_target.market_id,
            "round_guard_bypassed": bypass_round_guard,
            "price_trace": build_execution_price_trace(
                &decision,
                &payload,
                &attempt_payload,
                final_response.as_ref()
            )
        }));
    }
    out
}

pub(super) async fn fetch_live_balance_snapshot(
    state: &ApiState,
) -> Result<LiveBalanceSnapshot, String> {
    const USDC_SCALE: f64 = 1_000_000.0;
    let ctx = get_or_init_rust_executor(state).await?;
    let req = PmBalanceAllowanceRequest::builder()
        .asset_type(PmAssetType::Collateral)
        .build();
    let response = ctx
        .client
        .balance_allowance(req)
        .await
        .map_err(|e| format!("balance_allowance failed: {e}"))?;
    let account = ctx.client.address().to_string();
    let allowance_raw = response
        .allowances
        .iter()
        .find_map(|(addr, amount)| {
            if addr.to_string().eq_ignore_ascii_case(&account) {
                amount.parse::<f64>().ok()
            } else {
                None
            }
        })
        .or_else(|| {
            response
                .allowances
                .values()
                .filter_map(|v| v.parse::<f64>().ok())
                .fold(None, |acc, v| match acc {
                    Some(prev) if prev >= v => Some(prev),
                    _ => Some(v),
                })
        })
        .unwrap_or(0.0);
    let now_ms = Utc::now().timestamp_millis();
    let balance_raw = pm_dec_to_f64(&response.balance).max(0.0);
    let balance = (balance_raw / USDC_SCALE).max(0.0);
    let allowance = (allowance_raw / USDC_SCALE).max(0.0);
    let spendable = balance.min(allowance).max(0.0);
    let raw_allowances = response
        .allowances
        .iter()
        .map(|(addr, amount)| (addr.to_string(), amount.clone()))
        .collect::<HashMap<String, String>>();
    let raw = json!({
        "balance_raw": balance_raw,
        "balance": balance,
        "allowance_raw": allowance_raw,
        "allowances": raw_allowances,
        "account": account,
        "asset_type": "COLLATERAL"
    });
    Ok(LiveBalanceSnapshot {
        fetched_at_ms: now_ms,
        source: "clob_balance_allowance".to_string(),
        balance_usdc: balance,
        allowance_usdc: allowance,
        spendable_usdc: spendable,
        raw,
    })
}

pub(super) fn pm_dec_to_f64(v: &PmDecimal) -> f64 {
    v.to_string().parse::<f64>().unwrap_or(0.0)
}

pub(super) fn pm_order_type_from_tif(tif: &str) -> PmOrderType {
    match tif.to_ascii_uppercase().as_str() {
        "FOK" => PmOrderType::FOK,
        "GTD" => PmOrderType::GTD,
        "FAK" => PmOrderType::FAK,
        _ => PmOrderType::GTC,
    }
}

pub(super) fn pm_is_terminal_fill(status: &PmOrderStatusType) -> bool {
    matches!(status, PmOrderStatusType::Matched)
}

pub(super) fn pm_is_terminal_reject(status: &PmOrderStatusType) -> bool {
    matches!(
        status,
        PmOrderStatusType::Canceled | PmOrderStatusType::Unmatched
    )
}

pub(super) fn fill_event_from_open_order(
    order: &polymarket_client_sdk::clob::types::response::OpenOrderResponse,
) -> Value {
    let fill_price = pm_dec_to_f64(&order.price).max(0.0001);
    let fill_size = pm_dec_to_f64(&order.size_matched).max(0.0);
    let fill_quote = (fill_price * fill_size).max(0.0);
    json!({
        "event": "order_terminal",
        "state": "filled",
        "order_id": order.id,
        "fill_price_cents": fill_price * 100.0,
        "fill_quote_usdc": fill_quote,
        "fill_size_shares": fill_size,
        "size_matched": fill_size,
        "matched_size": fill_size,
        "associate_trades": order.associate_trades,
    })
}

fn is_exit_like_action(action: &str) -> bool {
    let a = action.trim().to_ascii_lowercase();
    a == "exit" || a == "reduce"
}

fn cancel_failure_pause_threshold(action: &str) -> u8 {
    if is_exit_like_action(action) {
        LIVE_CANCEL_FAILURE_FORCE_PAUSE_EXIT_THRESHOLD
    } else {
        LIVE_CANCEL_FAILURE_FORCE_PAUSE_OTHER_THRESHOLD
    }
}

pub(super) async fn handle_cancel_failure_with_escalation(
    state: &ApiState,
    pending: &LivePendingOrder,
    reason: &str,
    detail: Value,
) {
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
                "reason": format!("{reason}_requeued"),
                "order_id": retry.order_id,
                "retry_count": retry.retry_count,
                "detail": detail
            }),
        )
        .await;

    if retry.retry_count < cancel_failure_pause_threshold(&retry.action) {
        return;
    }

    let now_ms = Utc::now().timestamp_millis();
    let mut control = state.get_live_runtime_control(&retry.market_type).await;
    if control.mode != LiveRuntimeControlMode::ForcePause {
        control.mode = LiveRuntimeControlMode::ForcePause;
        control.requested_at_ms = now_ms;
        control.updated_at_ms = now_ms;
        control.completed_at_ms = None;
        control.note = Some(format!(
            "auto_force_pause:{}:order_id={}:retry={}",
            reason, retry.order_id, retry.retry_count
        ));
        state
            .put_live_runtime_control(&retry.market_type, control.clone())
            .await;
    }
    state
        .append_live_event(
            &retry.market_type,
            json!({
                "accepted": false,
                "action": retry.action,
                "side": retry.side,
                "round_id": retry.round_id,
                "reason": "auto_force_pause_cancel_failure",
                "order_id": retry.order_id,
                "retry_count": retry.retry_count,
                "runtime_mode": control.mode,
                "runtime_note": control.note,
            }),
        )
        .await;
}

pub(super) async fn get_or_init_rust_executor(
    state: &ApiState,
) -> Result<Arc<RustExecutorContext>, String> {
    if let Some(ctx) = state.live_rust_executor.read().await.clone() {
        return Ok(ctx);
    }
    let cfg = RustExecutorConfig::from_env()?;
    let signer = PmLocalSigner::from_str(cfg.private_key.trim())
        .map_err(|e| format!("invalid private key: {e}"))?
        .with_chain_id(Some(cfg.chain_id));
    let signer_for_runtime = signer.clone();
    let mut auth = PmClient::new(&cfg.host, PmConfig::default())
        .map_err(|e| format!("rust sdk client init failed: {e}"))?
        .authentication_builder(&signer)
        .signature_type(cfg.signature_type);
    if let Some(nonce) = cfg.nonce {
        auth = auth.nonce(nonce);
    }
    if let Some(creds) = cfg.credentials.clone() {
        auth = auth.credentials(creds);
    }
    if let Some(funder) = cfg.funder {
        auth = auth.funder(funder);
    }
    let client = auth
        .authenticate()
        .await
        .map_err(|e| format!("rust sdk auth failed: {e}"))?;
    let ctx = Arc::new(RustExecutorContext {
        client,
        signer: Box::new(signer_for_runtime),
    });
    let mut slot = state.live_rust_executor.write().await;
    if slot.is_none() {
        *slot = Some(ctx.clone());
    }
    Ok(slot.as_ref().cloned().unwrap_or(ctx))
}

pub(super) async fn fetch_rust_book_snapshot(
    ctx: &RustExecutorContext,
    token_id: &str,
) -> Option<GatewayBookSnapshot> {
    let token = PmU256::from_str(token_id).ok()?;
    let req = PmOrderBookSummaryRequest::builder().token_id(token).build();
    let book = ctx.client.order_book(&req).await.ok()?;
    let best_bid = book.bids.first().map(|l| pm_dec_to_f64(&l.price));
    let best_ask = book.asks.first().map(|l| pm_dec_to_f64(&l.price));
    let best_bid_size = book.bids.first().map(|l| pm_dec_to_f64(&l.size));
    let best_ask_size = book.asks.first().map(|l| pm_dec_to_f64(&l.size));
    let bid_depth_top3 = Some(
        book.bids
            .iter()
            .take(3)
            .map(|l| pm_dec_to_f64(&l.size))
            .sum::<f64>(),
    );
    let ask_depth_top3 = Some(
        book.asks
            .iter()
            .take(3)
            .map(|l| pm_dec_to_f64(&l.size))
            .sum::<f64>(),
    );
    Some(GatewayBookSnapshot {
        token_id: token_id.to_string(),
        min_order_size: pm_dec_to_f64(&book.min_order_size).max(0.0001),
        tick_size: pm_dec_to_f64(&book.tick_size.as_decimal()).max(0.0001),
        best_bid,
        best_ask,
        best_bid_size,
        best_ask_size,
        bid_depth_top3,
        ask_depth_top3,
    })
}

pub(super) async fn submit_rust_order(
    ctx: &RustExecutorContext,
    payload: &Value,
) -> Result<Value, String> {
    let token_id = payload
        .get("token_id")
        .and_then(Value::as_str)
        .ok_or_else(|| "missing token_id".to_string())?;
    let token = PmU256::from_str(token_id).map_err(|e| format!("invalid token_id: {e}"))?;
    let side = payload
        .get("side")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let pm_side = match side.as_str() {
        "buy_yes" | "buy_no" => PmSide::Buy,
        "sell_yes" | "sell_no" => PmSide::Sell,
        _ => return Err(format!("unsupported side: {side}")),
    };
    let price = payload
        .get("price")
        .and_then(Value::as_f64)
        .unwrap_or(0.5)
        .clamp(0.01, 0.99);
    let size = payload
        .get("size")
        .and_then(Value::as_f64)
        .unwrap_or(1.0)
        .max(0.01);
    let tif = payload
        .get("tif")
        .and_then(Value::as_str)
        .unwrap_or("FAK")
        .to_ascii_uppercase();
    let style = payload
        .get("style")
        .and_then(Value::as_str)
        .unwrap_or("taker")
        .to_ascii_lowercase();
    let ttl_ms = payload
        .get("ttl_ms")
        .and_then(Value::as_i64)
        .unwrap_or(1_200)
        .clamp(500, 30_000);
    let tick_size = payload
        .get("book_meta")
        .and_then(|v| v.get("tick_size"))
        .and_then(Value::as_f64)
        .unwrap_or(0.01);
    let price_decimals = decimal_places_from_step(tick_size, 2, 6);
    let price_str = format_decimal_compact(price, price_decimals.max(2));
    let size_str = format_decimal_compact(size, 5);
    let price_dec = PmDecimal::from_str(&price_str).map_err(|e| format!("bad price: {e}"))?;
    let size_dec = PmDecimal::from_str(&size_str).map_err(|e| format!("bad size: {e}"))?;
    let order_type = pm_order_type_from_tif(&tif);
    let requested_notional = payload
        .get("buy_amount_usdc")
        .and_then(Value::as_f64)
        .or_else(|| {
            payload
                .get("requested_notional_usdc")
                .and_then(Value::as_f64)
        })
        .filter(|v| v.is_finite() && *v > 0.0)
        .unwrap_or(size * price);
    let use_market_buy_amount = matches!(pm_side, PmSide::Buy)
        && matches!(order_type, PmOrderType::FAK | PmOrderType::FOK)
        && requested_notional > 0.0;

    let resp = if use_market_buy_amount {
        let amount_dec = PmDecimal::from_str(&format_decimal_compact(requested_notional, 6))
            .map_err(|e| format!("bad buy_amount_usdc: {e}"))?;
        let amount =
            PmAmount::usdc(amount_dec).map_err(|e| format!("invalid buy amount (usdc): {e}"))?;
        let signable = ctx
            .client
            .market_order()
            .token_id(token)
            .side(pm_side.clone())
            .order_type(order_type.clone())
            .price(price_dec)
            .amount(amount)
            .build()
            .await
            .map_err(|e| format!("build market order failed: {e}"))?;
        let signed = ctx
            .client
            .sign(&ctx.signer, signable)
            .await
            .map_err(|e| format!("sign failed: {e}"))?;
        ctx.client
            .post_order(signed)
            .await
            .map_err(|e| format!("post_order failed: {e}"))?
    } else {
        let mut builder = ctx
            .client
            .limit_order()
            .token_id(token)
            .side(pm_side)
            .price(price_dec)
            .size(size_dec)
            .order_type(order_type.clone());
        let post_only =
            style == "maker" && matches!(order_type, PmOrderType::GTC | PmOrderType::GTD);
        builder = builder.post_only(post_only);
        if matches!(order_type, PmOrderType::GTD) {
            // Polymarket GTD requires expiration to be sufficiently in the future
            // (server-side security threshold, typically >= 60s).
            let ttl_sec = ((ttl_ms + 999) / 1000).max(1);
            let expiration =
                Utc::now() + chrono::Duration::seconds(live_gtd_min_future_guard_sec() + ttl_sec);
            builder = builder.expiration(expiration);
        }
        let signable = builder
            .build()
            .await
            .map_err(|e| format!("build order failed: {e}"))?;
        let signed = ctx
            .client
            .sign(&ctx.signer, signable)
            .await
            .map_err(|e| format!("sign failed: {e}"))?;
        ctx.client
            .post_order(signed)
            .await
            .map_err(|e| format!("post_order failed: {e}"))?
    };
    let accepted = resp.success && resp.error_msg.as_deref().unwrap_or_default().is_empty();
    let making_amount = pm_dec_to_f64(&resp.making_amount);
    let taking_amount = pm_dec_to_f64(&resp.taking_amount);
    let accepted_size = if use_market_buy_amount {
        if taking_amount > 0.0 {
            taking_amount
        } else {
            size
        }
    } else if making_amount > 0.0 {
        making_amount
    } else {
        size
    };
    let effective_notional = if use_market_buy_amount {
        if making_amount > 0.0 {
            making_amount
        } else {
            requested_notional
        }
    } else if taking_amount > 0.0 {
        taking_amount
    } else {
        size * price
    };
    Ok(json!({
        "accepted": accepted,
        "order_id": resp.order_id,
        "status": format!("{}", resp.status),
        "error_msg": resp.error_msg,
        "accepted_size": accepted_size,
        "effective_notional": effective_notional,
        "making_amount": making_amount,
        "taking_amount": taking_amount,
        "requested_notional_usdc": requested_notional,
        "market_order_amount_mode": use_market_buy_amount,
        "trade_ids": resp.trade_ids,
        "transaction_hashes": resp.transaction_hashes
    }))
}

async fn try_resubmit_after_terminal_reject_rust(
    state: &ApiState,
    gateway_cfg: &LiveGatewayConfig,
    ctx: &Arc<RustExecutorContext>,
    row: &LivePendingOrder,
    terminal_status: &str,
) -> bool {
    let action = row.action.to_ascii_lowercase();
    let exit_like = is_live_exit_action(&action);
    let emergency_exit = exit_like && is_emergency_exit_reason(&row.submit_reason);
    let max_retry = if emergency_exit { 2 } else { 1 };
    if row.retry_count >= max_retry {
        return false;
    }
    let target = match resolve_live_market_target_with_state(state, &row.market_type).await {
        Ok(v) => v,
        Err(err) => {
            state
                .append_live_event(
                    &row.market_type,
                    json!({
                        "accepted": false,
                        "action": row.action,
                        "side": row.side,
                        "round_id": row.round_id,
                        "reason": "rust_terminal_rejected_resubmit_no_target",
                        "order_id": row.order_id,
                        "status": terminal_status,
                        "error": err.message
                    }),
                )
                .await;
            return false;
        }
    };
    let mut effective_target = target.clone();
    apply_pending_target_override(&mut effective_target, row);
    let round_check = json!({ "round_id": row.round_id });
    let bypass_round_guard = is_live_exit_action(&action)
        && !row.market_id.trim().is_empty()
        && !row.token_id.trim().is_empty();
    if !decision_round_matches_target(&round_check, &effective_target) && !bypass_round_guard {
        state
            .append_live_event(
                &row.market_type,
                json!({
                    "accepted": false,
                    "action": row.action,
                        "side": row.side,
                        "round_id": row.round_id,
                        "reason": "rust_terminal_rejected_resubmit_round_target_mismatch",
                        "order_id": row.order_id,
                        "status": terminal_status,
                        "target_market_id": effective_target.market_id,
                        "target_end_date": effective_target.end_date
                    }),
                )
                .await;
        return false;
    }
    let mut decision = json!({
        "action": row.action,
        "side": row.side,
        "round_id": row.round_id,
        "price_cents": row.price_cents,
        "quote_size_usdc": row.quote_size_usdc,
        "reason": format!("{}_terminal_retry_fak", row.submit_reason),
        "tif": "FAK",
        "style": "taker",
        "ttl_ms": if emergency_exit { 650 } else { 900 },
        "max_slippage_bps": if exit_like {
            (gateway_cfg.exit_slippage_bps + if emergency_exit { 22.0 } else { 14.0 }).max(34.0)
        } else {
            (gateway_cfg.entry_slippage_bps + 12.0).max(28.0)
        }
    });
    if exit_like {
        let ps = state.get_live_position_state(&row.market_type).await;
        if ps.position_size_shares > 0.0 {
            if let Some(obj) = decision.as_object_mut() {
                obj.insert(
                    "position_size_shares".to_string(),
                    json!(ps.position_size_shares),
                );
            }
        }
    }
    let token_id = token_id_for_decision(&decision, &effective_target).map(str::to_string);
    let book_snapshot = if let Some(token_id) = token_id {
        if let Some(cached) = state.get_rust_book_cache(&token_id).await {
            Some(cached)
        } else if let Some(v) = fetch_rust_book_snapshot(ctx, &token_id).await {
            state.put_rust_book_cache(&token_id, v.clone()).await;
            Some(v)
        } else {
            None
        }
    } else {
        None
    };
    let Some(payload) = decision_to_live_payload(
        &decision,
        &effective_target,
        gateway_cfg,
        book_snapshot.as_ref(),
        Some(row.quote_size_usdc),
    ) else {
        state
            .append_live_event(
                &row.market_type,
                json!({
                    "accepted": false,
                    "action": row.action,
                    "side": row.side,
                    "round_id": row.round_id,
                    "reason": "rust_terminal_rejected_resubmit_payload_failed",
                    "order_id": row.order_id,
                    "status": terminal_status
                }),
            )
            .await;
        return false;
    };
    let now_ms = Utc::now().timestamp_millis();
    match submit_rust_order(ctx, &payload).await {
        Ok(submit_resp) => {
            let accepted = submit_resp
                .get("accepted")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let new_order_id = submit_resp
                .get("order_id")
                .and_then(Value::as_str)
                .filter(|v| !v.trim().is_empty())
                .map(str::to_string);
            if accepted {
                if let Some(new_order_id) = new_order_id {
                    let mut pending = row.clone();
                    pending.order_id = new_order_id.clone();
                    pending.retry_count = pending.retry_count.saturating_add(1);
                    pending.tif = "FAK".to_string();
                    pending.style = "taker".to_string();
                    pending.submitted_ts_ms = now_ms;
                    pending.cancel_after_ms = if emergency_exit { 700 } else { 1500 };
                    state.upsert_pending_order(pending).await;
                    state
                        .append_live_event(
                            &row.market_type,
                            json!({
                                "accepted": true,
                                "action": row.action,
                                "side": row.side,
                                "round_id": row.round_id,
                                "reason": "rust_terminal_rejected_resubmitted_fak",
                                "order_id": row.order_id,
                                "new_order_id": new_order_id,
                                "status": terminal_status,
                                "submit_response": submit_resp,
                                "book_snapshot": book_snapshot,
                                "emergency_exit": emergency_exit
                            }),
                        )
                        .await;
                    return true;
                }
            }
            state
                .append_live_event(
                    &row.market_type,
                    json!({
                        "accepted": false,
                        "action": row.action,
                        "side": row.side,
                        "round_id": row.round_id,
                        "reason": "rust_terminal_rejected_resubmit_rejected",
                        "order_id": row.order_id,
                        "status": terminal_status,
                        "submit_response": submit_resp
                    }),
                )
                .await;
            false
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
                        "reason": "rust_terminal_rejected_resubmit_error",
                        "order_id": row.order_id,
                        "status": terminal_status,
                        "error": err
                    }),
                )
                .await;
            false
        }
    }
}

pub(super) async fn execute_live_orders_via_rust_sdk(
    state: &ApiState,
    gateway_cfg: &LiveGatewayConfig,
    target: &LiveMarketTarget,
    position_state: &LivePositionState,
    decisions: &[LiveGatedDecision],
) -> Vec<Value> {
    let ctx = match get_or_init_rust_executor(state).await {
        Ok(v) => v,
        Err(err) => {
            return decisions
                .iter()
                .map(|g| {
                    json!({
                        "ok": false,
                        "accepted": false,
                        "decision_key": g.decision_key,
                        "decision": g.decision,
                        "error": err,
                        "executor": "rust_sdk"
                    })
                })
                .collect();
        }
    };

    let mut out = Vec::<Value>::with_capacity(decisions.len());
    let fallback_client = state.gateway_http_client.as_ref();
    let token_ids = collect_decision_token_ids(target, decisions);
    let mut book_cache =
        prefetch_rust_books_for_tokens(state, &ctx, fallback_client, gateway_cfg, &token_ids).await;
    let exit_quote_override =
        position_state
            .entry_quote_usdc
            .and_then(|v| if v > 0.0 { Some(v) } else { None });
    let exit_size_override = if position_state.position_size_shares > 0.0 {
        Some(position_state.position_size_shares)
    } else {
        None
    };

    for gated in decisions {
        let mut decision = gated.decision.clone();
        let _side_aligned = align_exit_decision_side_with_position(&mut decision, position_state);
        let effective_target = build_effective_target_for_decision(target, &decision, position_state);
        let bypass_round_guard = should_bypass_round_match_for_locked_exit(&decision, position_state);
        if !decision_round_matches_target(&decision, &effective_target) && !bypass_round_guard {
            out.push(json!({
                "ok": false,
                "accepted": false,
                "reason": "round_target_mismatch",
                "decision_key": gated.decision_key,
                "decision_round_id": decision.get("round_id").and_then(Value::as_str),
                "target_market_id": effective_target.market_id,
                "target_end_date": effective_target.end_date,
                "decision": decision
            }));
            continue;
        }
        if decision
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .eq_ignore_ascii_case("exit")
        {
            if let Some(obj) = decision.as_object_mut() {
                if let Some(q) = exit_quote_override {
                    obj.insert("quote_size_usdc".to_string(), json!(q));
                }
                if let Some(sz) = exit_size_override {
                    obj.insert("position_size_shares".to_string(), json!(sz));
                }
            }
        }
        apply_emergency_exit_overrides(&mut decision, gateway_cfg);
        let token_id = token_id_for_decision(&decision, &effective_target).map(str::to_string);
        let book_snapshot = if let Some(token_id) = token_id.clone() {
            match book_cache.get(&token_id) {
                Some(cached) => cached.clone(),
                None => {
                    let fetched = if let Some(cached) = state.get_rust_book_cache(&token_id).await {
                        Some(cached)
                    } else if let Some(v) = fetch_rust_book_snapshot(&ctx, &token_id).await {
                        state.put_rust_book_cache(&token_id, v.clone()).await;
                        Some(v)
                    } else {
                        let fetched =
                            fetch_gateway_book_snapshot(fallback_client, gateway_cfg, &token_id)
                                .await;
                        if let Some(v) = fetched.as_ref() {
                            state.put_rust_book_cache(&token_id, v.clone()).await;
                        }
                        fetched
                    };
                    book_cache.insert(token_id.clone(), fetched.clone());
                    fetched
                }
            }
        } else {
            None
        };
        let Some(payload) =
            decision_to_live_payload(
                &decision,
                &effective_target,
                gateway_cfg,
                book_snapshot.as_ref(),
                None,
            )
        else {
            out.push(json!({
                "ok": false,
                "accepted": false,
                "decision_key": gated.decision_key,
                "decision": decision,
                "reason": "decision_to_payload_failed",
                "executor": "rust_sdk"
            }));
            continue;
        };
        let mut attempt_payload = payload.clone();
        let mut attempts = Vec::<Value>::with_capacity(3);
        let mut accepted = false;
        let mut final_response: Option<Value> = None;
        let mut final_error: Option<String> = None;
        let mut final_reject_reason: Option<String> = None;
        let mut executed_via = "rust_sdk";
        let order_started = Instant::now();
        for attempt in 0..3usize {
            let attempt_started = Instant::now();
            let submit = submit_rust_order(&ctx, &attempt_payload).await;
            match submit {
                Ok(resp) => {
                    let accept = resp
                        .get("accepted")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    let reject_reason = if accept {
                        String::new()
                    } else {
                        extract_rust_reject_reason(&resp, None)
                    };
                    attempts.push(json!({
                        "attempt": attempt + 1,
                        "executor": "rust_sdk",
                        "latency_ms": attempt_started.elapsed().as_millis() as u64,
                        "request": attempt_payload.clone(),
                        "accepted": accept,
                        "reject_reason": if reject_reason.is_empty() { Value::Null } else { json!(reject_reason.clone()) },
                        "response": resp
                    }));
                    final_response = Some(resp.clone());
                    if accept {
                        accepted = true;
                        break;
                    }
                    final_reject_reason = Some(reject_reason.clone());
                    if let Some(next_payload) =
                        build_retry_payload(&attempt_payload, &reject_reason, attempt)
                    {
                        attempt_payload = next_payload;
                        continue;
                    }
                    break;
                }
                Err(err) => {
                    let mut fallback_response: Option<Value> = None;
                    let mut fallback_accept = false;
                    let mut fallback_used = false;
                    if gateway_cfg.rust_submit_fallback_gateway {
                        let fallback_started = Instant::now();
                        let (fallback_submit, fallback_endpoint) =
                            submit_gateway_order(fallback_client, gateway_cfg, &attempt_payload)
                                .await;
                        if let Ok(resp) = fallback_submit {
                            fallback_used = true;
                            fallback_accept = resp
                                .get("accepted")
                                .and_then(Value::as_bool)
                                .unwrap_or(false);
                            executed_via = "gateway_fallback";
                            fallback_response = Some(json!({
                                "endpoint": fallback_endpoint,
                                "latency_ms": fallback_started.elapsed().as_millis() as u64,
                                "response": resp
                            }));
                            if fallback_accept {
                                accepted = true;
                                final_response = fallback_response
                                    .as_ref()
                                    .and_then(|v| v.get("response"))
                                    .cloned();
                                attempts.push(json!({
                                    "attempt": attempt + 1,
                                    "executor": "rust_sdk",
                                    "latency_ms": attempt_started.elapsed().as_millis() as u64,
                                    "request": attempt_payload.clone(),
                                    "accepted": false,
                                    "error": err,
                                    "fallback": fallback_response
                                }));
                                break;
                            }
                        }
                    }
                    attempts.push(json!({
                        "attempt": attempt + 1,
                        "executor": "rust_sdk",
                        "latency_ms": attempt_started.elapsed().as_millis() as u64,
                        "request": attempt_payload.clone(),
                        "accepted": false,
                        "error": err,
                        "fallback_used": fallback_used,
                        "fallback_accepted": fallback_accept,
                        "fallback": fallback_response
                    }));
                    final_error = Some(err.clone());
                    final_reject_reason = Some(err.clone());
                    if let Some(next_payload) = build_retry_payload(&attempt_payload, &err, attempt)
                    {
                        attempt_payload = next_payload;
                        continue;
                    }
                    break;
                }
            }
        }
        out.push(json!({
            "ok": accepted,
            "accepted": accepted,
            "decision_key": gated.decision_key,
            "decision": decision,
            "request": payload,
            "final_request": attempt_payload,
            "response": final_response,
            "error": final_error,
            "reject_reason": final_reject_reason,
            "attempts": attempts,
            "executor": executed_via,
            "order_latency_ms": order_started.elapsed().as_millis() as u64,
            "book_snapshot": book_snapshot,
            "target_market_id": effective_target.market_id,
            "round_guard_bypassed": bypass_round_guard,
            "price_trace": build_execution_price_trace(
                &decision,
                &payload,
                &attempt_payload,
                final_response.as_ref()
            )
        }));
    }
    out
}

pub(super) async fn reconcile_rust_reports(state: &ApiState, gateway_cfg: &LiveGatewayConfig) {
    let ctx = match get_or_init_rust_executor(state).await {
        Ok(v) => v,
        Err(err) => {
            tracing::warn!(error = %err, "rust sdk reconcile unavailable, fallback to gateway reports");
            reconcile_gateway_reports(state, gateway_cfg).await;
            return;
        }
    };
    let pending = state.list_pending_orders().await;
    if pending.is_empty() {
        return;
    }
    for row in pending {
        let status = ctx.client.order(&row.order_id).await;
        let Ok(order) = status else {
            continue;
        };
        if pm_is_terminal_fill(&order.status) {
            let _ = state.remove_pending_order(&row.order_id).await;
            let fill_event = fill_event_from_open_order(&order);
            apply_pending_confirmation(
                state,
                &row,
                "rust_order_terminal_filled",
                Some(&fill_event),
            )
            .await;
            state
                .append_live_event(
                    &row.market_type,
                    json!({
                        "accepted": true,
                        "action": row.action,
                        "side": row.side,
                        "round_id": row.round_id,
                        "reason": "rust_order_terminal_filled",
                        "order_id": row.order_id
                    }),
                )
                .await;
        } else if pm_is_terminal_reject(&order.status) {
            let _ = state.remove_pending_order(&row.order_id).await;
            let terminal_status = format!("{}", order.status);
            let retried = try_resubmit_after_terminal_reject_rust(
                state,
                gateway_cfg,
                &ctx,
                &row,
                &terminal_status,
            )
            .await;
            if retried {
                continue;
            }
            apply_pending_revert(state, &row, "rust_order_terminal_rejected").await;
            state
                .append_live_event(
                    &row.market_type,
                    json!({
                        "accepted": false,
                        "action": row.action,
                        "side": row.side,
                        "round_id": row.round_id,
                        "reason": "rust_order_terminal_rejected",
                        "order_id": row.order_id,
                        "status": terminal_status
                    }),
                )
                .await;
        }
    }
}

pub(super) async fn handle_pending_timeouts_rust(
    state: &ApiState,
    gateway_cfg: &LiveGatewayConfig,
) {
    let ctx = match get_or_init_rust_executor(state).await {
        Ok(v) => v,
        Err(err) => {
            tracing::warn!(error = %err, "rust sdk cancel unavailable, fallback to gateway cancels");
            handle_pending_timeouts(state, gateway_cfg).await;
            return;
        }
    };
    let now_ms = Utc::now().timestamp_millis();
    let pending = state.list_pending_orders().await;
    for row in pending {
        if now_ms.saturating_sub(row.submitted_ts_ms) < pending_cancel_due_ms(&row) {
            continue;
        }
        match ctx.client.cancel_order(&row.order_id).await {
            Ok(resp) => {
                let _ = state.remove_pending_order(&row.order_id).await;
                let maker_tif = matches!(
                    row.tif.to_ascii_uppercase().as_str(),
                    "GTD" | "GTC" | "POST_ONLY"
                );
                let exit_like = is_live_exit_action(&row.action);
                let emergency_exit = exit_like && is_emergency_exit_reason(&row.submit_reason);
                let can_retry = if emergency_exit {
                    row.retry_count < 2
                } else {
                    row.retry_count < 1
                };
                if (maker_tif || emergency_exit) && can_retry {
                    let entry_like = is_entry_action(&row.action);
                    let fallback_ttl_ms = if emergency_exit {
                        700
                    } else if entry_like {
                        live_entry_fak_ttl_ms()
                    } else {
                        900
                    };
                    let fallback_slippage_bps = if emergency_exit {
                        (gateway_cfg.exit_slippage_bps + 18.0).max(38.0)
                    } else if entry_like {
                        gateway_cfg
                            .exit_slippage_bps
                            .max(gateway_cfg.entry_slippage_bps)
                            + live_entry_fak_slippage_boost_bps()
                    } else {
                        gateway_cfg
                            .exit_slippage_bps
                            .max(gateway_cfg.entry_slippage_bps)
                            + 10.0
                    };
                    let maybe_target =
                        resolve_live_market_target_with_state(state, &row.market_type)
                            .await
                            .ok();
                    if let Some(target) = maybe_target {
                        let mut effective_target = target.clone();
                        apply_pending_target_override(&mut effective_target, &row);
                        let round_check = json!({ "round_id": row.round_id });
                        let bypass_round_guard = is_live_exit_action(&row.action)
                            && !row.market_id.trim().is_empty()
                            && !row.token_id.trim().is_empty();
                        if !decision_round_matches_target(&round_check, &effective_target)
                            && !bypass_round_guard
                        {
                            state
                                .append_live_event(
                                    &row.market_type,
                                    json!({
                                        "accepted": false,
                                        "action": row.action,
                                        "side": row.side,
                                        "round_id": row.round_id,
                                        "reason": "rust_timeout_cancelled_round_target_mismatch",
                                        "order_id": row.order_id,
                                        "target_market_id": effective_target.market_id,
                                        "target_end_date": effective_target.end_date
                                    }),
                                )
                                .await;
                            apply_pending_revert(state, &row, "rust_local_timeout_cancel").await;
                            continue;
                        }
                        let decision = json!({
                            "action": row.action,
                            "side": row.side,
                            "round_id": row.round_id,
                            "price_cents": row.price_cents,
                            "quote_size_usdc": row.quote_size_usdc,
                            "reason": format!("{}_timeout_fallback_fak", row.submit_reason),
                            "tif": "FAK",
                            "style": "taker",
                            "ttl_ms": fallback_ttl_ms,
                            "max_slippage_bps": fallback_slippage_bps
                        });
                        let token_id =
                            token_id_for_decision(&decision, &effective_target).map(str::to_string);
                        let book_snapshot = if let Some(token_id) = token_id {
                            if let Some(cached) = state.get_rust_book_cache(&token_id).await {
                                Some(cached)
                            } else if let Some(v) = fetch_rust_book_snapshot(&ctx, &token_id).await
                            {
                                state.put_rust_book_cache(&token_id, v.clone()).await;
                                Some(v)
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        if let Some(payload) = decision_to_live_payload(
                            &decision,
                            &effective_target,
                            gateway_cfg,
                            book_snapshot.as_ref(),
                            Some(row.quote_size_usdc),
                        ) {
                            let submit = submit_rust_order(&ctx, &payload).await;
                            if let Ok(submit_resp) = submit {
                                let accepted = submit_resp
                                    .get("accepted")
                                    .and_then(Value::as_bool)
                                    .unwrap_or(false);
                                if accepted {
                                    if let Some(order_id) = submit_resp
                                        .get("order_id")
                                        .and_then(Value::as_str)
                                        .filter(|v| !v.trim().is_empty())
                                    {
                                        let mut pending = row.clone();
                                        pending.order_id = order_id.to_string();
                                        pending.retry_count = pending.retry_count.saturating_add(1);
                                        pending.tif = "FAK".to_string();
                                        pending.style = "taker".to_string();
                                        pending.submitted_ts_ms = now_ms;
                                        pending.cancel_after_ms = if emergency_exit {
                                            700
                                        } else if entry_like {
                                            live_entry_fak_cancel_after_ms()
                                        } else {
                                            1500
                                        };
                                        state.upsert_pending_order(pending).await;
                                        state
                                            .append_live_event(
                                                &row.market_type,
                                                json!({
                                                    "accepted": true,
                                                    "action": row.action,
                                                    "side": row.side,
                                                    "round_id": row.round_id,
                                                    "reason": "rust_timeout_cancelled_resubmitted_fak",
                                                    "order_id": row.order_id,
                                                    "cancelled": resp.canceled,
                                                    "not_cancelled": resp.not_canceled,
                                                    "submit_response": submit_resp,
                                                    "book_snapshot": book_snapshot,
                                                    "emergency_exit": emergency_exit
                                                }),
                                            )
                                            .await;
                                        continue;
                                    }
                                }
                                state
                                    .append_live_event(
                                        &row.market_type,
                                        json!({
                                            "accepted": false,
                                            "action": row.action,
                                            "side": row.side,
                                            "round_id": row.round_id,
                                            "reason": "rust_timeout_cancelled_fak_resubmit_rejected",
                                            "order_id": row.order_id,
                                            "cancelled": resp.canceled,
                                            "not_cancelled": resp.not_canceled,
                                            "submit_response": submit_resp
                                        }),
                                    )
                                    .await;
                            }
                        }
                    }
                }
                apply_pending_revert(state, &row, "rust_timeout_cancelled").await;
                state
                    .append_live_event(
                        &row.market_type,
                        json!({
                            "accepted": false,
                            "action": row.action,
                            "side": row.side,
                            "round_id": row.round_id,
                            "reason": "rust_timeout_cancelled",
                            "order_id": row.order_id,
                            "cancelled": resp.canceled,
                            "not_cancelled": resp.not_canceled
                        }),
                    )
                    .await;
            }
            Err(err) => {
                handle_cancel_failure_with_escalation(
                    state,
                    &row,
                    "rust_timeout_cancel_failed",
                    json!({
                        "error": err.to_string()
                    }),
                )
                .await;
            }
        }
    }
}

pub(super) async fn reconcile_live_reports(
    state: &ApiState,
    gateway_cfg: &LiveGatewayConfig,
    mode: LiveExecutorMode,
) {
    match mode {
        LiveExecutorMode::Gateway => reconcile_gateway_reports(state, gateway_cfg).await,
        LiveExecutorMode::RustSdk => reconcile_rust_reports(state, gateway_cfg).await,
    }
}

pub(super) async fn handle_live_pending_timeouts(
    state: &ApiState,
    gateway_cfg: &LiveGatewayConfig,
    mode: LiveExecutorMode,
) {
    match mode {
        LiveExecutorMode::Gateway => handle_pending_timeouts(state, gateway_cfg).await,
        LiveExecutorMode::RustSdk => handle_pending_timeouts_rust(state, gateway_cfg).await,
    }
}

pub(super) async fn flush_entry_pending_before_exit(
    state: &ApiState,
    gateway_cfg: &LiveGatewayConfig,
    mode: LiveExecutorMode,
    market_type: &str,
) -> usize {
    let pending_rows = state
        .list_pending_orders_for_market(market_type)
        .await
        .into_iter()
        .filter(|row| {
            let action = row.action.to_ascii_lowercase();
            action == "enter" || action == "add"
        })
        .collect::<Vec<_>>();
    if pending_rows.is_empty() {
        return 0;
    }

    let mut cancelled = 0usize;
    let client = state.gateway_http_client.as_ref();
    let rust_ctx = if matches!(mode, LiveExecutorMode::RustSdk) {
        get_or_init_rust_executor(state).await.ok()
    } else {
        None
    };

    for row in pending_rows {
        let mut cancel_ok = false;
        let mut cancel_detail = Value::Null;
        let mut cancel_path = "gateway".to_string();

        if let Some(ctx) = rust_ctx.as_ref() {
            cancel_path = "rust_sdk".to_string();
            match ctx.client.cancel_order(&row.order_id).await {
                Ok(resp) => {
                    cancel_ok = true;
                    cancel_detail = json!({
                        "cancelled": resp.canceled,
                        "not_cancelled": resp.not_canceled
                    });
                }
                Err(err) => {
                    cancel_detail = json!({ "rust_error": err.to_string() });
                }
            }
        }

        if !cancel_ok {
            let mut endpoint = gateway_cfg.primary_url.clone();
            let mut cancel_res =
                delete_gateway_order(client, gateway_cfg.timeout_ms, &endpoint, &row.order_id)
                    .await;
            if cancel_res.is_err() {
                if let Some(backup) = gateway_cfg.backup_url.as_deref() {
                    endpoint = backup.to_string();
                    cancel_res = delete_gateway_order(
                        client,
                        gateway_cfg.timeout_ms,
                        &endpoint,
                        &row.order_id,
                    )
                    .await;
                }
            }
            match cancel_res {
                Ok(v) => {
                    cancel_ok = true;
                    cancel_path = format!("gateway:{endpoint}");
                    cancel_detail = v;
                }
                Err(err) => {
                    if cancel_detail.is_null() {
                        cancel_detail = json!({ "gateway_error": err });
                    }
                }
            }
        }

        if cancel_ok {
            let _ = state.remove_pending_order(&row.order_id).await;
            apply_pending_revert(state, &row, "pre_exit_cancelled").await;
            cancelled = cancelled.saturating_add(1);
            state
                .append_live_event(
                    &row.market_type,
                    json!({
                        "accepted": true,
                        "action": row.action,
                        "side": row.side,
                        "round_id": row.round_id,
                        "reason": "pre_exit_cancelled",
                        "order_id": row.order_id,
                        "cancel_path": cancel_path,
                        "cancel_response": cancel_detail
                    }),
                )
                .await;
        } else {
            state
                .append_live_event(
                    &row.market_type,
                    json!({
                        "accepted": false,
                        "action": row.action,
                        "side": row.side,
                        "round_id": row.round_id,
                        "reason": "pre_exit_cancel_failed",
                        "order_id": row.order_id,
                        "cancel_path": cancel_path,
                        "cancel_response": cancel_detail
                    }),
                )
                .await;
        }
    }

    cancelled
}

pub(super) async fn execute_live_orders(
    state: &ApiState,
    gateway_cfg: &LiveGatewayConfig,
    mode: LiveExecutorMode,
    market_type: &str,
    target: &LiveMarketTarget,
    position_state: &LivePositionState,
    decisions: &[LiveGatedDecision],
) -> Vec<Value> {
    let mut prioritized = decisions.to_vec();
    prioritized.sort_by_key(|d| {
        let action = d
            .decision
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        if is_live_exit_action(&action) {
            0_u8
        } else if action == "enter" || action == "add" {
            2_u8
        } else {
            1_u8
        }
    });
    let has_exit_like = prioritized.iter().any(|d| {
        d.decision
            .get("action")
            .and_then(Value::as_str)
            .map(|a| {
                let a = a.to_ascii_lowercase();
                is_live_exit_action(&a)
            })
            .unwrap_or(false)
    });
    let mut deferred = Vec::<Value>::new();
    if has_exit_like {
        prioritized.retain(|d| {
            let action = d
                .decision
                .get("action")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_ascii_lowercase();
            if action == "enter" || action == "add" {
                deferred.push(json!({
                    "ok": false,
                    "accepted": false,
                    "decision_key": d.decision_key,
                    "decision": d.decision,
                    "reason": "deferred_due_exit_priority"
                }));
                false
            } else {
                true
            }
        });
    }
    if has_exit_like {
        let _ = flush_entry_pending_before_exit(state, gateway_cfg, mode, market_type).await;
    }
    let mut out = match mode {
        LiveExecutorMode::Gateway => {
            execute_live_orders_via_gateway(
                state,
                gateway_cfg,
                target,
                position_state,
                &prioritized,
            )
            .await
        }
        LiveExecutorMode::RustSdk => {
            execute_live_orders_via_rust_sdk(
                state,
                gateway_cfg,
                target,
                position_state,
                &prioritized,
            )
            .await
        }
    };
    if !deferred.is_empty() {
        out.extend(deferred);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_target() -> LiveMarketTarget {
        LiveMarketTarget {
            market_id: "mkt".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "5m".to_string(),
            token_id_yes: "yes".to_string(),
            token_id_no: "no".to_string(),
            end_date: None,
        }
    }

    fn test_gateway_cfg() -> LiveGatewayConfig {
        LiveGatewayConfig {
            primary_url: "http://127.0.0.1:9001".to_string(),
            backup_url: None,
            timeout_ms: 500,
            min_quote_usdc: 1.0,
            entry_slippage_bps: 18.0,
            exit_slippage_bps: 22.0,
            force_slippage_bps: None,
            rust_submit_fallback_gateway: true,
        }
    }

    fn test_book(min_order_size: f64) -> GatewayBookSnapshot {
        GatewayBookSnapshot {
            token_id: "yes".to_string(),
            min_order_size,
            tick_size: 0.01,
            best_bid: Some(0.49),
            best_ask: Some(0.50),
            best_bid_size: Some(100.0),
            best_ask_size: Some(100.0),
            bid_depth_top3: Some(300.0),
            ask_depth_top3: Some(300.0),
        }
    }

    #[test]
    fn taker_buy_uses_buy_amount_mode_with_min_order_size_floor() {
        let decision = json!({
            "action": "enter",
            "side": "UP",
            "price_cents": 50.0,
            "quote_size_usdc": 1.0,
            "tif": "FAK",
            "style": "taker"
        });
        let payload = decision_to_live_payload(
            &decision,
            &test_target(),
            &test_gateway_cfg(),
            Some(&test_book(5.0)),
            None,
        )
        .expect("payload");
        let size = payload
            .get("size")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        let price = payload
            .get("price")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        let amount = payload
            .get("buy_amount_usdc")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        let mode = payload
            .get("amount_mode")
            .and_then(Value::as_str)
            .unwrap_or_default();

        assert!(
            size >= 5.0,
            "taker buy must keep min_order_size floor, got {size}"
        );
        assert!(
            amount + 1e-9 >= size * price,
            "buy amount should cover min-order notional, got amount={amount}, size={size}, price={price}"
        );
        assert_eq!(mode, "buy_usdc");
    }

    #[test]
    fn maker_buy_still_respects_min_order_size_floor() {
        let decision = json!({
            "action": "enter",
            "side": "UP",
            "price_cents": 50.0,
            "quote_size_usdc": 1.0,
            "tif": "GTD",
            "style": "maker"
        });
        let payload = decision_to_live_payload(
            &decision,
            &test_target(),
            &test_gateway_cfg(),
            Some(&test_book(5.0)),
            None,
        )
        .expect("payload");
        let size = payload
            .get("size")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        assert!(
            size >= 5.0,
            "maker buy must keep min_order_size floor, got {size}"
        );
        assert!(
            payload.get("buy_amount_usdc").is_none(),
            "maker path should stay in share-size mode"
        );
    }

    #[test]
    fn retry_payload_exit_ladder_moves_price_and_slippage() {
        let cur = json!({
            "action": "exit",
            "reason": "signal_reverse",
            "side": "sell_yes",
            "price": 0.60,
            "size": 5.0,
            "quote_size_usdc": 3.0,
            "tif": "FAK",
            "style": "taker",
            "ttl_ms": 900,
            "max_slippage_bps": 22.0,
            "book_meta": {
                "tick_size": 0.01
            }
        });
        let nxt = build_retry_payload(&cur, "UNMATCHED", 0).expect("retry payload");
        let px = nxt.get("price").and_then(Value::as_f64).unwrap_or_default();
        let slip = nxt
            .get("max_slippage_bps")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        let ttl = nxt
            .get("ttl_ms")
            .and_then(Value::as_i64)
            .unwrap_or_default();
        assert!(
            px <= 0.59 + 1e-9,
            "exit retry should move sell price down by >=1 tick, got {px}"
        );
        assert!(slip > 22.0, "exit retry should raise slippage, got {slip}");
        assert!(ttl <= 900, "exit retry should not increase ttl, got {ttl}");
    }

    #[test]
    fn retry_payload_entry_ladder_keeps_taker_mode() {
        let cur = json!({
            "action": "enter",
            "reason": "fev1_signal_entry",
            "side": "buy_yes",
            "price": 0.51,
            "size": 5.0,
            "quote_size_usdc": 3.0,
            "tif": "FAK",
            "style": "taker",
            "ttl_ms": 1400,
            "max_slippage_bps": 18.0,
            "book_meta": {
                "tick_size": 0.01
            }
        });
        let nxt = build_retry_payload(&cur, "insufficient liquidity", 0).expect("retry payload");
        let tif = nxt.get("tif").and_then(Value::as_str).unwrap_or_default();
        let style = nxt.get("style").and_then(Value::as_str).unwrap_or_default();
        let px = nxt.get("price").and_then(Value::as_f64).unwrap_or_default();
        assert_eq!(tif, "FAK");
        assert_eq!(style, "taker");
        assert!(
            px >= 0.52 - 1e-9,
            "entry retry should move buy price up by >=1 tick, got {px}"
        );
    }

    #[test]
    fn retry_payload_exit_keeps_full_size() {
        let cur = json!({
            "action": "exit",
            "reason": "signal_reverse",
            "side": "sell_yes",
            "price": 0.60,
            "size": 6.1,
            "quote_size_usdc": 3.0,
            "tif": "FAK",
            "style": "taker",
            "ttl_ms": 900,
            "max_slippage_bps": 22.0,
            "book_meta": {
                "tick_size": 0.01
            }
        });
        let nxt = build_retry_payload(&cur, "UNMATCHED", 0).expect("retry payload");
        let sz = nxt.get("size").and_then(Value::as_f64).unwrap_or_default();
        assert!(
            (sz - 6.1).abs() < 1e-9,
            "exit retry must keep full size, got {sz}"
        );
    }

    #[test]
    fn decision_round_target_match_rejects_cross_round_target() {
        let start_ms = 1_700_000_000_000_i64;
        let mut target = test_target();
        target.end_date = end_date_iso_from_ms(start_ms + 300_000);
        let ok = json!({"round_id": format!("BTCUSDT_5m_{start_ms}")});
        let bad = json!({"round_id": format!("BTCUSDT_5m_{}", start_ms + 300_000)});
        assert!(decision_round_matches_target(&ok, &target));
        assert!(!decision_round_matches_target(&bad, &target));
    }

    #[test]
    fn parse_round_end_ts_uses_timeframe_duration() {
        let start = 1_700_000_000_000_i64;
        assert_eq!(
            parse_round_end_ts_ms(&format!("BTCUSDT_5m_{start}")),
            Some(start + 300_000)
        );
        assert_eq!(
            parse_round_end_ts_ms(&format!("BTCUSDT_15m_{start}")),
            Some(start + 900_000)
        );
    }

    #[test]
    fn extract_fill_meta_prefers_size_matched_over_quote_backsolve() {
        let pending = LivePendingOrder {
            market_type: "5m".to_string(),
            order_id: "oid".to_string(),
            market_id: "mkt".to_string(),
            token_id: "yes".to_string(),
            action: "enter".to_string(),
            side: "UP".to_string(),
            round_id: "r".to_string(),
            decision_key: "k".to_string(),
            price_cents: 99.0,
            quote_size_usdc: 4.95,
            order_size_shares: 5.0,
            tif: "FAK".to_string(),
            style: "taker".to_string(),
            submit_reason: "test".to_string(),
            submitted_ts_ms: 0,
            cancel_after_ms: 1000,
            retry_count: 0,
        };
        let fill = json!({
            "event": "order_terminal",
            "state": "filled",
            "fill_price_cents": 81.0,
            "fill_quote_usdc": 4.95,
            "size_matched": 6.111111
        });
        let meta = extract_pending_fill_meta(Some(&fill), &pending);
        let shares = meta.fill_size_shares.unwrap_or(0.0);
        assert!(
            (shares - 6.111111).abs() < 1e-6,
            "should keep exact matched shares, got {shares}"
        );
    }
}
