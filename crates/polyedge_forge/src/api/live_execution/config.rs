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

fn round_lot_size(size: f64) -> f64 {
    ((size.max(0.0) * 100.0).round() / 100.0).max(0.01)
}

fn floor_lot_size(size: f64) -> f64 {
    ((size.max(0.0) * 100.0).floor() / 100.0).max(0.01)
}

const USDC_MICRO_SCALE_F64: f64 = 1_000_000.0;

pub(super) fn usdc_to_micros(value: f64) -> i64 {
    let clamped = if value.is_finite() { value.max(0.0) } else { 0.0 };
    (clamped * USDC_MICRO_SCALE_F64).round() as i64
}

pub(super) fn micros_to_usdc(value: i64) -> f64 {
    (value as f64) / USDC_MICRO_SCALE_F64
}

pub(super) fn quantize_usdc_micros(value: f64) -> f64 {
    micros_to_usdc(usdc_to_micros(value))
}

fn live_quote_amount_decimals() -> u32 {
    std::env::var("FORGE_FEV1_QUOTE_AMOUNT_DECIMALS")
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .unwrap_or(2)
        .clamp(0, 6)
}

pub(super) fn ceil_quote_amount_usdc(value: f64) -> f64 {
    ceil_usdc_to_decimals(value, live_quote_amount_decimals())
}

fn live_market_buy_amount_decimals() -> u32 {
    std::env::var("FORGE_FEV1_MARKET_BUY_AMOUNT_DECIMALS")
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .unwrap_or(2)
        .clamp(0, 2)
}

pub(super) fn ceil_market_buy_amount_usdc(value: f64) -> f64 {
    ceil_usdc_to_decimals(value, live_market_buy_amount_decimals())
}

fn ceil_usdc_to_decimals(value: f64, decimals: u32) -> f64 {
    let decimals = decimals.clamp(0, 6);
    let micros = usdc_to_micros(value).max(0);
    let step = 10_i64.pow(6_u32.saturating_sub(decimals));
    let aligned = if step <= 1 {
        micros
    } else {
        ((micros + (step - 1)) / step) * step
    };
    micros_to_usdc(aligned)
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
const LIVE_GTD_EXPIRATION_SAFETY_SEC_DEFAULT: i64 = 8;
const LIVE_GTD_EXPIRATION_SAFETY_SEC_MIN: i64 = 0;
const LIVE_GTD_EXPIRATION_SAFETY_SEC_MAX: i64 = 30;
const LIVE_ROUND_TARGET_MATCH_TOLERANCE_MS_DEFAULT: i64 = 5_000;
const LIVE_ROUND_TARGET_MATCH_TOLERANCE_MS_MIN: i64 = 0;
const LIVE_ROUND_TARGET_MATCH_TOLERANCE_MS_MAX: i64 = 120_000;
const LIVE_REQUIRE_FIXED_ENTRY_SIZE_DEFAULT: bool = true;
const LIVE_FIXED_ENTRY_SIZE_SHARES_MIN: f64 = 0.0;
const LIVE_FIXED_ENTRY_SIZE_SHARES_MAX: f64 = 1_000.0;
const LIVE_MAX_OPEN_POSITIONS_DEFAULT: usize = 1;
const LIVE_MAX_OPEN_POSITIONS_MIN: usize = 1;
const LIVE_MAX_OPEN_POSITIONS_MAX: usize = 4;
const LIVE_MAX_COMPLETED_TRADES_DEFAULT: usize = 0;
const LIVE_MAX_COMPLETED_TRADES_MIN: usize = 0;
const LIVE_MAX_COMPLETED_TRADES_MAX: usize = 1_000;
const LIVE_ALLOW_ADDS_DEFAULT: bool = true;
const LIVE_SIGNAL_ENTRY_FRESHNESS_MS_DEFAULT: i64 = 2_000;
const LIVE_SIGNAL_ENTRY_FRESHNESS_MS_MIN: i64 = 200;
const LIVE_SIGNAL_ENTRY_FRESHNESS_MS_MAX: i64 = 60_000;
const LIVE_SIGNAL_EXIT_FRESHNESS_MS_DEFAULT: i64 = 5_000;
const LIVE_SIGNAL_EXIT_FRESHNESS_MS_MIN: i64 = 500;
const LIVE_SIGNAL_EXIT_FRESHNESS_MS_MAX: i64 = 120_000;

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

fn live_gtd_expiration_safety_sec() -> i64 {
    std::env::var("FORGE_FEV1_GTD_EXPIRATION_SAFETY_SEC")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_GTD_EXPIRATION_SAFETY_SEC_DEFAULT)
        .clamp(
            LIVE_GTD_EXPIRATION_SAFETY_SEC_MIN,
            LIVE_GTD_EXPIRATION_SAFETY_SEC_MAX,
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

fn live_require_fixed_entry_size() -> bool {
    std::env::var("FORGE_FEV1_REQUIRE_FIXED_ENTRY_SIZE")
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(LIVE_REQUIRE_FIXED_ENTRY_SIZE_DEFAULT)
}

fn live_max_open_positions() -> usize {
    std::env::var("FORGE_FEV1_LIVE_MAX_OPEN_POSITIONS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(LIVE_MAX_OPEN_POSITIONS_DEFAULT)
        .clamp(LIVE_MAX_OPEN_POSITIONS_MIN, LIVE_MAX_OPEN_POSITIONS_MAX)
}

fn live_max_completed_trades() -> usize {
    std::env::var("FORGE_FEV1_LIVE_MAX_COMPLETED_TRADES")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(LIVE_MAX_COMPLETED_TRADES_DEFAULT)
        .clamp(
            LIVE_MAX_COMPLETED_TRADES_MIN,
            LIVE_MAX_COMPLETED_TRADES_MAX,
        )
}

fn live_max_completed_trades_for_scope(symbol: &str, market_type: &str) -> usize {
    let default_limit = live_max_completed_trades();
    let symbol = symbol.trim().to_ascii_uppercase();
    let market_type = market_type.trim().to_ascii_lowercase();
    let raw = match std::env::var("FORGE_FEV1_LIVE_MAX_COMPLETED_TRADES_BY_SCOPE") {
        Ok(v) => v,
        Err(_) => return default_limit,
    };
    for entry in raw.split(',') {
        let item = entry.trim();
        if item.is_empty() {
            continue;
        }
        let Some((scope, limit_raw)) = item.split_once('=') else {
            continue;
        };
        let Ok(limit) = limit_raw.trim().parse::<usize>() else {
            continue;
        };
        let limit = limit.clamp(
            LIVE_MAX_COMPLETED_TRADES_MIN,
            LIVE_MAX_COMPLETED_TRADES_MAX,
        );
        let scope = scope.trim();
        if scope.is_empty() {
            continue;
        }
        if let Some((scope_symbol, scope_market)) = scope.split_once(':') {
            if scope_symbol.trim().eq_ignore_ascii_case(&symbol)
                && scope_market.trim().eq_ignore_ascii_case(&market_type)
            {
                return limit;
            }
            continue;
        }
        if scope.eq_ignore_ascii_case(&symbol) {
            return limit;
        }
    }
    default_limit
}

fn live_allow_add_orders() -> bool {
    std::env::var("FORGE_FEV1_LIVE_ALLOW_ADDS")
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(LIVE_ALLOW_ADDS_DEFAULT)
}

pub(super) fn live_signal_entry_freshness_ms() -> i64 {
    std::env::var("FORGE_FEV1_LIVE_SIGNAL_ENTRY_FRESHNESS_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_SIGNAL_ENTRY_FRESHNESS_MS_DEFAULT)
        .clamp(
            LIVE_SIGNAL_ENTRY_FRESHNESS_MS_MIN,
            LIVE_SIGNAL_ENTRY_FRESHNESS_MS_MAX,
        )
}

pub(super) fn live_signal_exit_freshness_ms() -> i64 {
    std::env::var("FORGE_FEV1_LIVE_SIGNAL_EXIT_FRESHNESS_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_SIGNAL_EXIT_FRESHNESS_MS_DEFAULT)
        .clamp(
            LIVE_SIGNAL_EXIT_FRESHNESS_MS_MIN,
            LIVE_SIGNAL_EXIT_FRESHNESS_MS_MAX,
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

