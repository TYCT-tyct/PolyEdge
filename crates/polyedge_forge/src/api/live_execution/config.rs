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
    let normalized = if value.is_finite() { value } else { 0.0 };
    (normalized * USDC_MICRO_SCALE_F64).round() as i64
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
const LIVE_MARKET_TARGET_RESOLVE_ATTEMPTS_DEFAULT: i64 = 3; // OPTIMIZED: Reduced from 5 to 3 for faster resolution
const LIVE_MARKET_TARGET_RESOLVE_ATTEMPTS_MIN: i64 = 1;
const LIVE_MARKET_TARGET_RESOLVE_ATTEMPTS_MAX: i64 = 8;
const LIVE_MARKET_TARGET_RESOLVE_RETRY_MS_DEFAULT: i64 = 50; // OPTIMIZED: Reduced from 220ms to 50ms for faster resolution
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
const LIVE_SIGNAL_ENTRY_FRESHNESS_MS_DEFAULT: i64 = 5_000; // OPTIMIZED: Increased from 2s to 5s for short-window markets
const LIVE_SIGNAL_ENTRY_FRESHNESS_MS_MIN: i64 = 200;
const LIVE_SIGNAL_ENTRY_FRESHNESS_MS_MAX: i64 = 60_000;
const LIVE_ENTRY_LIQUIDITY_REJECT_WINDOW_MS_DEFAULT: i64 = 5_000;
const LIVE_ENTRY_LIQUIDITY_REJECT_WINDOW_MS_MIN: i64 = 500;
const LIVE_ENTRY_LIQUIDITY_REJECT_WINDOW_MS_MAX: i64 = 60_000;
const LIVE_ENTRY_LIQUIDITY_REJECT_LIMIT_DEFAULT: u32 = 4;
const LIVE_ENTRY_LIQUIDITY_REJECT_LIMIT_MIN: u32 = 1;
const LIVE_ENTRY_LIQUIDITY_REJECT_LIMIT_MAX: u32 = 100;
const LIVE_SIGNAL_EXIT_FRESHNESS_MS_DEFAULT: i64 = 30_000;
const LIVE_SIGNAL_EXIT_FRESHNESS_MS_MIN: i64 = 500;
const LIVE_SIGNAL_EXIT_FRESHNESS_MS_MAX: i64 = 120_000;
const LIVE_LOCKED_EXIT_MARKET_GRACE_MS_DEFAULT: i64 = 60_000;
const LIVE_LOCKED_EXIT_MARKET_GRACE_MS_MIN: i64 = 0;
const LIVE_LOCKED_EXIT_MARKET_GRACE_MS_MAX: i64 = 10 * 60_000;
const LIVE_GAMMA_MARKET_DETAIL_CACHE_TTL_MS_DEFAULT: i64 = 3_000;
const LIVE_GAMMA_MARKET_DETAIL_CACHE_TTL_MS_MIN: i64 = 250;
const LIVE_GAMMA_MARKET_DETAIL_CACHE_TTL_MS_MAX: i64 = 60_000;

#[derive(Debug, Clone)]
struct CachedLiveMarketTarget {
    fetched_at_ms: i64,
    target: LiveMarketTarget,
}

#[derive(Debug, Clone)]
pub(super) struct GammaMarketDetail {
    pub(super) market_id: String,
    pub(super) condition_id: Option<String>,
    pub(super) active: bool,
    pub(super) closed: bool,
    pub(super) enable_order_book: bool,
    pub(super) end_date: Option<String>,
    pub(super) token_ids: Vec<String>,
    pub(super) outcomes: Vec<String>,
    pub(super) outcome_prices: Vec<f64>,
    pub(super) uma_resolution_status: Option<String>,
    pub(super) slug: Option<String>,
    pub(super) question: Option<String>,
}

#[derive(Debug, Clone)]
struct CachedGammaMarketDetail {
    fetched_at_ms: i64,
    detail: GammaMarketDetail,
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

pub(super) fn live_entry_liquidity_reject_window_ms() -> i64 {
    std::env::var("FORGE_FEV1_LIVE_ENTRY_LIQUIDITY_REJECT_WINDOW_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_ENTRY_LIQUIDITY_REJECT_WINDOW_MS_DEFAULT)
        .clamp(
            LIVE_ENTRY_LIQUIDITY_REJECT_WINDOW_MS_MIN,
            LIVE_ENTRY_LIQUIDITY_REJECT_WINDOW_MS_MAX,
        )
}

pub(super) fn live_entry_liquidity_reject_limit() -> u32 {
    std::env::var("FORGE_FEV1_LIVE_ENTRY_LIQUIDITY_REJECT_LIMIT")
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .unwrap_or(LIVE_ENTRY_LIQUIDITY_REJECT_LIMIT_DEFAULT)
        .clamp(
            LIVE_ENTRY_LIQUIDITY_REJECT_LIMIT_MIN,
            LIVE_ENTRY_LIQUIDITY_REJECT_LIMIT_MAX,
        )
}

// ============================================================================
// FAST PATH OPTIMIZATION: For short-window current_summary signals
// Reduces checks for high-confidence signals to improve latency
// ============================================================================
const LIVE_FAST_PATH_ENABLED_DEFAULT: bool = true;
const LIVE_FAST_PATH_MIN_SCORE_DEFAULT: f64 = 0.70; // Skip extra checks if score >= 0.7
const LIVE_FAST_PATH_MIN_REMAINING_MS_DEFAULT: i64 = 30_000; // Skip extra checks if > 30s remaining

pub(super) fn live_fast_path_enabled() -> bool {
    std::env::var("FORGE_FEV1_LIVE_FAST_PATH_ENABLED")
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(LIVE_FAST_PATH_ENABLED_DEFAULT)
}

pub(super) fn live_fast_path_min_score() -> f64 {
    std::env::var("FORGE_FEV1_LIVE_FAST_PATH_MIN_SCORE")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(LIVE_FAST_PATH_MIN_SCORE_DEFAULT)
        .clamp(0.5, 0.95)
}

pub(super) fn live_fast_path_min_remaining_ms() -> i64 {
    std::env::var("FORGE_FEV1_LIVE_FAST_PATH_MIN_REMAINING_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_FAST_PATH_MIN_REMAINING_MS_DEFAULT)
        .clamp(10_000, 120_000)
}

/// Check if a decision qualifies for fast path optimization
pub(super) fn decision_qualifies_for_fast_path(decision: &Value, remaining_ms: i64) -> bool {
    if !live_fast_path_enabled() {
        return false;
    }
    
    // Must be a current_summary signal
    let signal_source = decision
        .get("signal_source")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if !signal_source.eq_ignore_ascii_case("current_summary") {
        return false;
    }
    
    // Must have high confidence score
    let score = decision
        .get("score")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    if score.abs() < live_fast_path_min_score() {
        return false;
    }
    
    // Must have enough remaining time
    if remaining_ms < live_fast_path_min_remaining_ms() {
        return false;
    }
    
    true
}

// =========================================================================
// P3: Dual parity band config
// Normal: 1.1c (default) - standard parity protection
// Aggressive: 1.6c - for high confidence signals (score >= 0.8) or near round close (remaining <= 120s)
// =========================================================================
const LIVE_ENTRY_PARITY_BAND_NORMAL_CENTS_DEFAULT: f64 = 1.1;
const LIVE_ENTRY_PARITY_BAND_AGGRESSIVE_CENTS_DEFAULT: f64 = 1.6;
const LIVE_ENTRY_PARITY_BAND_AGGRESSIVE_REMAINING_MS: i64 = 120_000;
const LIVE_ENTRY_PARITY_BAND_AGGRESSIVE_SCORE: f64 = 0.80;

pub(super) fn live_entry_parity_band_normal_cents() -> f64 {
    std::env::var("FORGE_FEV1_LIVE_ENTRY_PARITY_BAND_NORMAL_CENTS")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(LIVE_ENTRY_PARITY_BAND_NORMAL_CENTS_DEFAULT)
        .clamp(0.5, 5.0)
}

pub(super) fn live_entry_parity_band_aggressive_cents() -> f64 {
    std::env::var("FORGE_FEV1_LIVE_ENTRY_PARITY_BAND_AGGRESSIVE_CENTS")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(LIVE_ENTRY_PARITY_BAND_AGGRESSIVE_CENTS_DEFAULT)
        .clamp(0.5, 5.0)
}

pub(super) fn live_entry_parity_band_aggressive_remaining_ms() -> i64 {
    std::env::var("FORGE_FEV1_LIVE_ENTRY_PARITY_BAND_AGGRESSIVE_REMAINING_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_ENTRY_PARITY_BAND_AGGRESSIVE_REMAINING_MS)
        .clamp(30_000, 300_000)
}

pub(super) fn live_entry_parity_band_aggressive_score() -> f64 {
    std::env::var("FORGE_FEV1_LIVE_ENTRY_PARITY_BAND_AGGRESSIVE_SCORE")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(LIVE_ENTRY_PARITY_BAND_AGGRESSIVE_SCORE)
        .clamp(0.5, 0.99)
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

fn live_locked_exit_market_grace_ms() -> i64 {
    std::env::var("FORGE_FEV1_LOCKED_EXIT_MARKET_GRACE_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_LOCKED_EXIT_MARKET_GRACE_MS_DEFAULT)
        .clamp(
            LIVE_LOCKED_EXIT_MARKET_GRACE_MS_MIN,
            LIVE_LOCKED_EXIT_MARKET_GRACE_MS_MAX,
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

fn live_gamma_market_detail_cache(
) -> &'static tokio::sync::RwLock<HashMap<String, CachedGammaMarketDetail>> {
    static CACHE: std::sync::OnceLock<
        tokio::sync::RwLock<HashMap<String, CachedGammaMarketDetail>>,
    > = std::sync::OnceLock::new();
    CACHE.get_or_init(|| tokio::sync::RwLock::new(HashMap::new()))
}

fn live_gamma_market_detail_cache_ttl_ms() -> i64 {
    std::env::var("FORGE_FEV1_GAMMA_MARKET_DETAIL_CACHE_TTL_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_GAMMA_MARKET_DETAIL_CACHE_TTL_MS_DEFAULT)
        .clamp(
            LIVE_GAMMA_MARKET_DETAIL_CACHE_TTL_MS_MIN,
            LIVE_GAMMA_MARKET_DETAIL_CACHE_TTL_MS_MAX,
        )
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
            .user_agent("PolyEdge-Forge/0.1 (+resolved-reconcile)")
            .build()
            .expect("build gamma market http client")
    })
}

fn live_polymarket_data_api_url_base() -> String {
    std::env::var("FORGE_FEV1_DATA_API_URL_BASE")
        .ok()
        .map(|v| v.trim().trim_end_matches('/').to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "https://data-api.polymarket.com".to_string())
}

fn live_polymarket_data_api_http_client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(800))
            .timeout(Duration::from_millis(2_500))
            .pool_max_idle_per_host(2)
            .user_agent("Mozilla/5.0 PolyEdge-Forge/0.1")
            .build()
            .expect("build polymarket data api http client")
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

fn parse_f64_array_field(v: &Value, key: &str) -> Vec<f64> {
    if let Some(arr) = v.get(key).and_then(Value::as_array) {
        return arr
            .iter()
            .filter_map(parse_json_number_like)
            .collect::<Vec<_>>();
    }
    if let Some(raw) = v.get(key).and_then(Value::as_str) {
        if let Ok(arr) = serde_json::from_str::<Vec<Value>>(raw) {
            return arr
                .iter()
                .filter_map(parse_json_number_like)
                .collect::<Vec<_>>();
        }
        if let Ok(arr) = serde_json::from_str::<Vec<String>>(raw) {
            return arr
                .into_iter()
                .filter_map(|s| s.trim().parse::<f64>().ok())
                .collect::<Vec<_>>();
        }
    }
    Vec::new()
}

fn parse_json_number_like(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn parse_json_bool_like(v: &Value) -> Option<bool> {
    match v {
        Value::Bool(v) => Some(*v),
        Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Some(true),
            "false" | "0" | "no" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct DataApiUserPosition {
    pub(super) asset: String,
    pub(super) condition_id: Option<String>,
    pub(super) size: Option<f64>,
    pub(super) avg_price: Option<f64>,
    pub(super) current_value: Option<f64>,
    pub(super) cash_pnl: Option<f64>,
    pub(super) realized_pnl: Option<f64>,
    pub(super) cur_price: Option<f64>,
    pub(super) redeemable: Option<bool>,
    pub(super) mergeable: Option<bool>,
    pub(super) title: Option<String>,
    pub(super) slug: Option<String>,
    pub(super) outcome: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(super) struct ResolvedPositionOutcome {
    pub(super) market_closed: bool,
    pub(super) market_resolved: bool,
    pub(super) local_token_result: String,
    pub(super) local_outcome_label: Option<String>,
    pub(super) winning_outcome_label: Option<String>,
    pub(super) winning_token_id: Option<String>,
    pub(super) local_expected_claim_value_usdc: Option<f64>,
}

fn parse_data_api_user_position(v: &Value) -> Option<DataApiUserPosition> {
    let asset = v
        .get("asset")
        .and_then(Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())?;
    Some(DataApiUserPosition {
        asset,
        condition_id: v
            .get("conditionId")
            .and_then(Value::as_str)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        size: v.get("size").and_then(parse_json_number_like),
        avg_price: v.get("avgPrice").and_then(parse_json_number_like),
        current_value: v.get("currentValue").and_then(parse_json_number_like),
        cash_pnl: v.get("cashPnl").and_then(parse_json_number_like),
        realized_pnl: v.get("realizedPnl").and_then(parse_json_number_like),
        cur_price: v.get("curPrice").and_then(parse_json_number_like),
        redeemable: v.get("redeemable").and_then(parse_json_bool_like),
        mergeable: v.get("mergeable").and_then(parse_json_bool_like),
        title: v
            .get("title")
            .and_then(Value::as_str)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        slug: v
            .get("slug")
            .and_then(Value::as_str)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        outcome: v
            .get("outcome")
            .and_then(Value::as_str)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
    })
}

pub(super) async fn fetch_data_api_user_positions(
    user: &str,
) -> Result<Vec<DataApiUserPosition>, String> {
    let user = user.trim();
    if user.is_empty() {
        return Err("missing user wallet".to_string());
    }
    let base = live_polymarket_data_api_url_base();
    let url = format!("{base}/positions");
    let client = live_polymarket_data_api_http_client();
    let resp = client
        .get(url)
        .query(&[("user", user), ("sizeThreshold", "0.0001")])
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await
        .map_err(|e| format!("positions request failed: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("positions request failed: {}", status));
    }
    let payload = resp
        .json::<Value>()
        .await
        .map_err(|e| format!("positions json parse failed: {e}"))?;
    let arr = payload
        .as_array()
        .ok_or_else(|| "positions payload was not an array".to_string())?;
    Ok(arr
        .iter()
        .filter_map(parse_data_api_user_position)
        .collect::<Vec<_>>())
}

pub(super) fn analyze_resolved_position_outcome(
    detail: Option<&GammaMarketDetail>,
    position_state: &LivePositionState,
) -> ResolvedPositionOutcome {
    let mut market_resolved = false;
    let mut market_closed = false;
    let mut local_token_result = "unknown".to_string();
    let mut local_outcome_label = None;
    let mut winning_outcome_label = None;
    let mut winning_token_id = None;
    let mut local_expected_claim_value_usdc = None;

    if let Some(detail) = detail {
        market_closed = detail.closed;
        market_resolved = detail
            .uma_resolution_status
            .as_deref()
            .map(|s| s.eq_ignore_ascii_case("resolved"))
            .unwrap_or(false);
        let winning_index = detail.outcome_prices.iter().position(|price| *price >= 0.999);
        if let Some(idx) = winning_index {
            winning_outcome_label = detail.outcomes.get(idx).cloned();
            winning_token_id = detail.token_ids.get(idx).cloned();
        }
        if let Some(local_token_id) = position_state.entry_token_id.as_deref() {
            if let Some(local_idx) = detail
                .token_ids
                .iter()
                .position(|token| token == local_token_id)
            {
                local_outcome_label = detail.outcomes.get(local_idx).cloned();
                local_token_result = match winning_index {
                    Some(win_idx) if win_idx == local_idx => {
                        local_expected_claim_value_usdc =
                            Some(position_state.position_size_shares.max(0.0));
                        "winning".to_string()
                    }
                    Some(_) => "losing".to_string(),
                    None if market_resolved => "resolved_unknown".to_string(),
                    None => "unknown".to_string(),
                };
            }
        }
    }

    ResolvedPositionOutcome {
        market_closed,
        market_resolved,
        local_token_result,
        local_outcome_label,
        winning_outcome_label,
        winning_token_id,
        local_expected_claim_value_usdc,
    }
}

pub(super) fn can_clear_resolved_position_local_state(
    market_resolved: bool,
    pending_count: usize,
    control_mode: LiveRuntimeControlMode,
    venue_check_available: bool,
    force: bool,
) -> bool {
    if !market_resolved || pending_count > 0 {
        return false;
    }
    if matches!(control_mode, LiveRuntimeControlMode::Normal) {
        return false;
    }
    venue_check_available || force
}

pub(super) fn live_runtime_wallet_address() -> Option<String> {
    std::env::var("FORGE_FEV1_FUNDER")
        .ok()
        .or_else(|| std::env::var("FORGE_FEV1_PROXY_WALLET").ok())
        .or_else(|| std::env::var("FORGE_FEV1_ADDRESS").ok())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

pub(super) fn mask_wallet_address(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() <= 10 {
        return trimmed.to_string();
    }
    format!("{}...{}", &trimmed[..6], &trimmed[trimmed.len() - 4..])
}

pub(super) async fn fetch_gamma_market_detail(market_id: &str) -> Option<GammaMarketDetail> {
    let now_ms = Utc::now().timestamp_millis();
    let ttl_ms = live_gamma_market_detail_cache_ttl_ms();
    {
        let cache = live_gamma_market_detail_cache().read().await;
        if let Some(cached) = cache.get(market_id) {
            if now_ms.saturating_sub(cached.fetched_at_ms) <= ttl_ms {
                return Some(cached.detail.clone());
            }
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
    let parsed = GammaMarketDetail {
        market_id: market_id.to_string(),
        condition_id: detail
            .get("conditionId")
            .and_then(Value::as_str)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        active: detail.get("active").and_then(Value::as_bool).unwrap_or(false),
        closed: detail.get("closed").and_then(Value::as_bool).unwrap_or(false),
        enable_order_book: detail
            .get("enableOrderBook")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        end_date: detail
            .get("endDate")
            .and_then(Value::as_str)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        token_ids: parse_token_array_field(&detail, "clobTokenIds"),
        outcomes: parse_token_array_field(&detail, "outcomes"),
        outcome_prices: parse_f64_array_field(&detail, "outcomePrices"),
        uma_resolution_status: detail
            .get("umaResolutionStatus")
            .and_then(Value::as_str)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        slug: detail
            .get("slug")
            .and_then(Value::as_str)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        question: detail
            .get("question")
            .and_then(Value::as_str)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
    };
    {
        let mut cache = live_gamma_market_detail_cache().write().await;
        cache.insert(
            market_id.to_string(),
            CachedGammaMarketDetail {
                fetched_at_ms: now_ms,
                detail: parsed.clone(),
            },
        );
    }
    Some(parsed)
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

    let detail = fetch_gamma_market_detail(market_id).await?;
    let token_ids = detail.token_ids.clone();
    if token_ids.len() < 2 {
        return None;
    }
    let outcomes = detail.outcomes.clone();

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

// ============================================================================
// MAKER STRATEGY: For better prices when spread is tight
// ============================================================================
const LIVE_MAKER_ENABLED_DEFAULT: bool = false; // Disabled by default, enable for better prices
const LIVE_MAKER_SPREAD_THRESHOLD_CENTS_DEFAULT: f64 = 1.0; // Use maker when spread < 1c
const LIVE_MAKER_MIN_REMAINING_MS_DEFAULT: i64 = 60_000; // Need at least 60s remaining
const LIVE_MAKER_TTL_MS_DEFAULT: i64 = 2000; // 2s TTL for maker orders
const LIVE_MAKER_MAX_SLIPPAGE_BPS_DEFAULT: f64 = 10.0; // Max slippage for maker

pub(super) fn live_maker_enabled() -> bool {
    std::env::var("FORGE_FEV1_LIVE_MAKER_ENABLED")
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(LIVE_MAKER_ENABLED_DEFAULT)
}

pub(super) fn live_maker_spread_threshold_cents() -> f64 {
    std::env::var("FORGE_FEV1_LIVE_MAKER_SPREAD_THRESHOLD_CENTS")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(LIVE_MAKER_SPREAD_THRESHOLD_CENTS_DEFAULT)
        .clamp(0.5, 5.0)
}

pub(super) fn live_maker_min_remaining_ms() -> i64 {
    std::env::var("FORGE_FEV1_LIVE_MAKER_MIN_REMAINING_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_MAKER_MIN_REMAINING_MS_DEFAULT)
        .clamp(30_000, 300_000)
}

pub(super) fn live_maker_ttl_ms() -> i64 {
    std::env::var("FORGE_FEV1_LIVE_MAKER_TTL_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_MAKER_TTL_MS_DEFAULT)
        .clamp(500, 10_000)
}

pub(super) fn live_maker_max_slippage_bps() -> f64 {
    std::env::var("FORGE_FEV1_LIVE_MAKER_MAX_SLIPPAGE_BPS")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(LIVE_MAKER_MAX_SLIPPAGE_BPS_DEFAULT)
        .clamp(1.0, 50.0)
}

// ============================================================================
// PRE-COMPUTED ORDER TEMPLATE: For ultra-fast execution
// ============================================================================
const LIVE_PRECOMPUTE_ENABLED_DEFAULT: bool = true;
const LIVE_PRECOMPUTE_TTL_MS_DEFAULT: i64 = 5_000; // 5s TTL for pre-computed templates

pub(super) fn live_precompute_enabled() -> bool {
    std::env::var("FORGE_FEV1_LIVE_PRECOMPUTE_ENABLED")
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(LIVE_PRECOMPUTE_ENABLED_DEFAULT)
}

pub(super) fn live_precompute_ttl_ms() -> i64 {
    std::env::var("FORGE_FEV1_LIVE_PRECOMPUTE_TTL_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(LIVE_PRECOMPUTE_TTL_MS_DEFAULT)
        .clamp(1_000, 30_000)
}

// Check if we should use maker strategy for a given spread and remaining time
pub(super) fn should_use_maker_strategy(spread_cents: f64, remaining_ms: i64) -> bool {
    if !live_maker_enabled() {
        return false;
    }
    spread_cents <= live_maker_spread_threshold_cents() 
        && remaining_ms >= live_maker_min_remaining_ms()
}
