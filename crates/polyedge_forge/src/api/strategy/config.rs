fn strategy_sample_cache_enabled() -> bool {
    strategy_env_bool("FORGE_STRATEGY_SAMPLE_CACHE_ENABLED", true)
}

fn strategy_sample_cache_ttl_ms() -> u64 {
    strategy_env_u32("FORGE_STRATEGY_SAMPLE_CACHE_TTL_MS", 300, 0, 5_000) as u64
}

fn strategy_sample_cache_max_entries() -> usize {
    strategy_env_u32("FORGE_STRATEGY_SAMPLE_CACHE_MAX_ENTRIES", 24, 4, 256) as usize
}

fn strategy_sample_cache() -> &'static tokio::sync::RwLock<HashMap<String, StrategySampleCacheEntry>>
{
    static CACHE: OnceLock<tokio::sync::RwLock<HashMap<String, StrategySampleCacheEntry>>> =
        OnceLock::new();
    CACHE.get_or_init(|| tokio::sync::RwLock::new(HashMap::new()))
}

fn strategy_sample_cache_key(
    symbol: &str,
    market_type: &str,
    full_history: bool,
    lookback_minutes: u32,
    max_points: u32,
) -> String {
    format!("{symbol}|{market_type}|{full_history}|{lookback_minutes}|{max_points}")
}

fn strategy_runtime_stream_enabled() -> bool {
    strategy_env_bool("FORGE_STRATEGY_RUNTIME_STREAM_ENABLED", true)
}

fn strategy_runtime_event_cache_enabled() -> bool {
    strategy_env_bool("FORGE_STRATEGY_RUNTIME_EVENT_CACHE_ENABLED", true)
}

fn strategy_runtime_stream_reload_sec() -> u64 {
    strategy_env_u32("FORGE_STRATEGY_RUNTIME_STREAM_RELOAD_SEC", 180, 10, 3_600) as u64
}

fn strategy_runtime_stream_delta_limit() -> u32 {
    strategy_env_u32(
        "FORGE_STRATEGY_RUNTIME_STREAM_DELTA_LIMIT",
        6_000,
        200,
        50_000,
    )
}

fn strategy_runtime_stream_cache(
) -> &'static tokio::sync::RwLock<HashMap<String, StrategyRuntimeStreamState>> {
    static CACHE: OnceLock<tokio::sync::RwLock<HashMap<String, StrategyRuntimeStreamState>>> =
        OnceLock::new();
    CACHE.get_or_init(|| tokio::sync::RwLock::new(HashMap::new()))
}

fn strategy_enabled_markets() -> &'static Vec<String> {
    static ENABLED: OnceLock<Vec<String>> = OnceLock::new();
    ENABLED.get_or_init(|| {
        std::env::var("FORGE_STRATEGY_MARKETS")
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(|v| v.trim().to_ascii_lowercase())
                    .filter(|v| v == "5m" || v == "15m")
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| vec!["5m".to_string()])
    })
}

fn normalize_strategy_symbol(raw: &str) -> Option<&'static str> {
    match raw.trim().to_ascii_uppercase().as_str() {
        "BTC" | "BTCUSDT" | "XBT" => Some("BTCUSDT"),
        "ETH" | "ETHUSDT" | "ETHER" => Some("ETHUSDT"),
        "SOL" | "SOLUSDT" => Some("SOLUSDT"),
        "XRP" | "XRPUSDT" | "RIPPLE" => Some("XRPUSDT"),
        _ => None,
    }
}

fn strategy_enabled_symbols() -> &'static Vec<String> {
    static ENABLED: OnceLock<Vec<String>> = OnceLock::new();
    ENABLED.get_or_init(|| {
        std::env::var("FORGE_STRATEGY_SYMBOLS")
            .ok()
            .map(|raw| {
                raw.split(',')
                    .filter_map(normalize_strategy_symbol)
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| {
                vec![
                    "BTCUSDT".to_string(),
                    "ETHUSDT".to_string(),
                    "SOLUSDT".to_string(),
                    "XRPUSDT".to_string(),
                ]
            })
    })
}

fn live_execution_enabled_markets() -> &'static Vec<String> {
    static ENABLED: OnceLock<Vec<String>> = OnceLock::new();
    ENABLED.get_or_init(|| {
        std::env::var("FORGE_FEV1_RUNTIME_MARKETS")
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(|v| v.trim().to_ascii_lowercase())
                    .filter(|v| v == "5m" || v == "15m")
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| vec!["5m".to_string()])
    })
}

fn live_execution_market_allowed(market_type: &str) -> bool {
    live_execution_enabled_markets()
        .iter()
        .any(|v| v.as_str() == market_type)
}

fn resolve_strategy_market_type(raw: Option<&str>) -> Result<&'static str, ApiError> {
    let market_type = if let Some(mt) = raw {
        normalize_market_type(mt).ok_or_else(|| ApiError::bad_request("invalid market_type"))?
    } else {
        "5m"
    };
    if strategy_enabled_markets()
        .iter()
        .any(|v| v.as_str() == market_type)
    {
        return Ok(market_type);
    }
    Err(ApiError::bad_request(format!(
        "market_type '{}' disabled by FORGE_STRATEGY_MARKETS={}",
        market_type,
        strategy_enabled_markets().join(",")
    )))
}

fn resolve_strategy_symbol(raw: Option<&str>) -> Result<&'static str, ApiError> {
    let symbol = if let Some(v) = raw {
        normalize_strategy_symbol(v).ok_or_else(|| ApiError::bad_request("invalid symbol"))?
    } else {
        "BTCUSDT"
    };
    if strategy_enabled_symbols().iter().any(|v| v.as_str() == symbol) {
        return Ok(symbol);
    }
    Err(ApiError::bad_request(format!(
        "symbol '{}' disabled by FORGE_STRATEGY_SYMBOLS={}",
        symbol,
        strategy_enabled_symbols().join(",")
    )))
}

fn strategy_env_u32(name: &str, default: u32, min_v: u32, max_v: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(default)
        .clamp(min_v, max_v)
}

fn strategy_default_max_points(full_history: bool) -> u32 {
    if full_history {
        strategy_env_u32("FORGE_STRATEGY_MAX_POINTS_FULL", 160_000, 5_000, 2_000_000)
    } else {
        strategy_env_u32("FORGE_STRATEGY_MAX_POINTS_SHORT", 90_000, 5_000, 600_000)
    }
}

fn strategy_max_points_hard_cap() -> u32 {
    strategy_env_u32(
        "FORGE_STRATEGY_MAX_POINTS_HARD_CAP",
        600_000,
        5_000,
        5_000_000,
    )
}

fn strategy_min_points() -> u32 {
    strategy_env_u32("FORGE_STRATEGY_MIN_POINTS", 5_000, 1_000, 50_000)
}

fn strategy_guard_max_points(full_history: bool) -> u32 {
    if full_history {
        strategy_env_u32(
            "FORGE_STRATEGY_GUARD_MAX_POINTS_FULL",
            120_000,
            10_000,
            2_000_000,
        )
    } else {
        strategy_env_u32(
            "FORGE_STRATEGY_GUARD_MAX_POINTS_SHORT",
            80_000,
            10_000,
            1_000_000,
        )
    }
}

fn strategy_heavy_points_threshold() -> u32 {
    strategy_env_u32(
        "FORGE_STRATEGY_HEAVY_POINTS_THRESHOLD",
        70_000,
        5_000,
        2_000_000,
    )
}

fn strategy_heavy_trim_enabled() -> bool {
    strategy_env_bool("FORGE_STRATEGY_HEAVY_TRIM", true)
}

fn strategy_ensure_lookback_coverage_enabled() -> bool {
    strategy_env_bool("FORGE_STRATEGY_ENSURE_LOOKBACK_COVERAGE", true)
}

fn strategy_required_points_1s(lookback_minutes: u32) -> u32 {
    lookback_minutes.saturating_mul(60)
}

fn strategy_required_points_for_resolution(
    lookback_minutes: u32,
    sample_resolution_ms: u32,
) -> u32 {
    let resolution_ms = sample_resolution_ms.max(1);
    let points_per_minute = 60_000u32 / resolution_ms;
    lookback_minutes.saturating_mul(points_per_minute.max(1))
}

fn strategy_samples_coverage_minutes(samples: &[StrategySample]) -> f64 {
    if samples.len() < 2 {
        return 0.0;
    }
    let start_ms = samples.first().map(|v| v.ts_ms).unwrap_or(0);
    let end_ms = samples.last().map(|v| v.ts_ms).unwrap_or(start_ms);
    (end_ms.saturating_sub(start_ms).max(0) as f64) / 60_000.0
}

fn strategy_lookback_meta_json(
    samples: &[StrategySample],
    full_history: bool,
    lookback_minutes: u32,
    max_points: u32,
    sample_resolution_ms: u32,
) -> Value {
    let required_points_1s = strategy_required_points_1s(lookback_minutes);
    let required_points_effective =
        strategy_required_points_for_resolution(lookback_minutes, sample_resolution_ms);
    let coverage_minutes_by_points = if required_points_effective == 0 {
        0.0
    } else {
        ((lookback_minutes as f64)
            * (max_points as f64 / required_points_effective as f64).min(1.0))
        .max(0.0)
    };
    json!({
        "requested_lookback_minutes": lookback_minutes,
        "full_history": full_history,
        "sample_resolution_ms": sample_resolution_ms,
        "required_points_1s": required_points_1s,
        "required_points_effective": required_points_effective,
        "max_points_effective": max_points,
        "truncated_by_points": !full_history && max_points < required_points_effective,
        "coverage_minutes_by_points": coverage_minutes_by_points,
        "coverage_minutes_by_samples": strategy_samples_coverage_minutes(samples)
    })
}

fn strategy_resolve_max_points(
    full_history: bool,
    lookback_minutes: u32,
    requested_max_points: Option<u32>,
    requested_max_samples: Option<u32>,
) -> u32 {
    let min_points = strategy_min_points();
    let hard_cap = strategy_max_points_hard_cap().max(min_points);
    let guard_cap = strategy_guard_max_points(full_history).clamp(min_points, hard_cap);
    let ensure_lookback_coverage = strategy_ensure_lookback_coverage_enabled() && !full_history;
    let mut max_points = requested_max_points.unwrap_or(strategy_default_max_points(full_history));
    if let Some(max_samples) = requested_max_samples {
        max_points = max_points.min(max_samples);
    }
    if ensure_lookback_coverage {
        let required = strategy_required_points_1s(lookback_minutes).clamp(min_points, hard_cap);
        max_points = max_points.max(required);
    }
    let resolved = max_points.clamp(min_points, hard_cap);
    if ensure_lookback_coverage {
        resolved
    } else {
        resolved.min(guard_cap)
    }
}

async fn strategy_acquire_heavy_permit(
    state: &ApiState,
    full_history: bool,
    max_points: u32,
) -> StrategyHeavyScope {
    let heavy = full_history || max_points >= strategy_heavy_points_threshold();
    if !heavy {
        return StrategyHeavyScope {
            _permit: None,
            trim_on_drop: false,
        };
    }
    let permit = state
        .strategy_heavy_slots
        .clone()
        .acquire_owned()
        .await
        .ok();
    StrategyHeavyScope {
        _permit: permit,
        trim_on_drop: strategy_heavy_trim_enabled(),
    }
}

#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn malloc_trim(pad: usize) -> i32;
}

fn strategy_try_trim_allocator() {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: `malloc_trim` is thread-safe in glibc and can be called to return free heap pages.
        unsafe {
            let _ = malloc_trim(0);
        }
    }
}

struct StrategyHeavyScope {
    _permit: Option<OwnedSemaphorePermit>,
    trim_on_drop: bool,
}

impl Drop for StrategyHeavyScope {
    fn drop(&mut self) {
        if self.trim_on_drop {
            strategy_try_trim_allocator();
        }
    }
}

fn strategy_select_profile_name() -> &'static str {
    if let Ok(raw) = std::env::var("FORGE_STRATEGY_BASE_PROFILE") {
        if let Some(profile) = strategy_profile_name_from_alias(raw.trim()) {
            return profile;
        }
    }
    STRATEGY_PROFILE_HI_WIN
}

fn strategy_profile_name_from_alias(raw: &str) -> Option<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "profit_max" | "manual_profit_max" | "max" | "fev1_manual_profit_max_2026_02_27" => {
            Some(STRATEGY_PROFILE_PROFIT_MAX)
        }
        "hi_win" | "manual_hi_win" | "safe" | "safety" | "fev1_manual_hi_win_2026_02_27" => {
            Some(STRATEGY_PROFILE_HI_WIN)
        }
        "hi_freq" | "manual_hi_freq" | "freq" | "fev1_manual_hi_freq_2026_02_27" => {
            Some(STRATEGY_PROFILE_HI_FREQ)
        }
        "balanced" | "manual_balanced" | "fev1_manual_balanced_2026_02_28" => {
            Some(STRATEGY_PROFILE_BALANCED)
        }
        "cand_growth_mix" | "growth_mix" | "growth" | "fev1_cand_growth_mix_2026_02_28" => {
            Some(STRATEGY_PROFILE_CAND_GROWTH_MIX)
        }
        _ => None,
    }
}

fn strategy_scope_profile_overrides() -> &'static HashMap<String, &'static str> {
    static OVERRIDES: OnceLock<HashMap<String, &'static str>> = OnceLock::new();
    OVERRIDES.get_or_init(|| {
        let mut out = HashMap::<String, &'static str>::new();
        let Ok(raw) = std::env::var("FORGE_STRATEGY_BASE_PROFILE_BY_SCOPE") else {
            return out;
        };
        for entry in raw.split(',').map(str::trim).filter(|v| !v.is_empty()) {
            let Some((scope_raw, profile_raw)) = entry.split_once('=') else {
                continue;
            };
            let scope = scope_raw.trim();
            let Some(profile) = strategy_profile_name_from_alias(profile_raw.trim()) else {
                continue;
            };
            if let Some((symbol_raw, market_raw)) = scope.split_once(':') {
                let symbol = normalize_strategy_symbol(symbol_raw);
                let market = normalize_market_type(market_raw);
                match (symbol, market) {
                    (Some(sym), Some(mt)) => {
                        out.insert(format!("{sym}|{mt}"), profile);
                    }
                    (Some(sym), None) => {
                        out.insert(format!("{sym}|*"), profile);
                    }
                    (None, Some(mt)) => {
                        out.insert(format!("*|{mt}"), profile);
                    }
                    (None, None) => {}
                }
            } else if let Some(sym) = normalize_strategy_symbol(scope) {
                out.insert(format!("{sym}|*"), profile);
            } else if let Some(mt) = normalize_market_type(scope) {
                out.insert(format!("*|{mt}"), profile);
            }
        }
        out
    })
}

fn strategy_profile_name_for_scope(symbol: &str, market_type: &str) -> &'static str {
    let symbol = normalize_strategy_symbol(symbol).unwrap_or("BTCUSDT");
    let market_type = normalize_market_type(market_type).unwrap_or("5m");
    let overrides = strategy_scope_profile_overrides();
    if let Some(profile) = overrides.get(&format!("{symbol}|{market_type}")) {
        return profile;
    }
    if let Some(profile) = overrides.get(&format!("{symbol}|*")) {
        return profile;
    }
    if let Some(profile) = overrides.get(&format!("*|{market_type}")) {
        return profile;
    }
    strategy_select_profile_name()
}

pub(super) fn strategy_current_default_profile_name_for_scope(
    symbol: &str,
    market_type: &str,
) -> &'static str {
    strategy_profile_name_for_scope(symbol, market_type)
}

fn strategy_profile_from_alias(raw: &str) -> Option<(&'static str, StrategyRuntimeConfig)> {
    let profile = strategy_profile_name_from_alias(raw)?;
    let cfg = match profile {
        STRATEGY_PROFILE_HI_WIN => strategy_hi_win_config(),
        STRATEGY_PROFILE_HI_FREQ => strategy_hi_freq_config(),
        STRATEGY_PROFILE_BALANCED => strategy_balanced_config(),
        STRATEGY_PROFILE_CAND_GROWTH_MIX => strategy_cand_growth_mix_config(),
        _ => strategy_profit_max_config(),
    };
    Some((profile, cfg))
}

pub(super) fn strategy_current_default_config_for_scope(
    symbol: &str,
    market_type: &str,
) -> StrategyRuntimeConfig {
    match strategy_current_default_profile_name_for_scope(symbol, market_type) {
        STRATEGY_PROFILE_HI_WIN => strategy_hi_win_config(),
        STRATEGY_PROFILE_HI_FREQ => strategy_hi_freq_config(),
        STRATEGY_PROFILE_BALANCED => strategy_balanced_config(),
        STRATEGY_PROFILE_CAND_GROWTH_MIX => strategy_cand_growth_mix_config(),
        _ => strategy_profit_max_config(),
    }
}

fn strategy_env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

fn strategy_cfg_hash(cfg: &StrategyRuntimeConfig, profile_name: &str) -> String {
    let mut hasher = Sha256::new();
    let canonical = serde_json::to_string(&strategy_cfg_json(cfg)).unwrap_or_default();
    hasher.update(profile_name.as_bytes());
    hasher.update(b"|");
    hasher.update(canonical.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn strategy_fixed_guard_payload(cfg: &StrategyRuntimeConfig, profile_name: &str) -> Value {
    let enabled = strategy_env_bool("FORGE_STRATEGY_FIXED_GUARD_ENABLED", true);
    let enforce_live = strategy_env_bool("FORGE_STRATEGY_FIXED_GUARD_ENFORCE_LIVE", true);
    let allow_mismatch = strategy_env_bool("FORGE_STRATEGY_FIXED_GUARD_ALLOW_MISMATCH", false);
    let current_hash = strategy_cfg_hash(cfg, profile_name);
    let expected_hash = std::env::var("FORGE_STRATEGY_FIXED_GUARD_EXPECTED_HASH")
        .ok()
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty());
    let hash_match = match expected_hash.as_deref() {
        Some(expected) if expected.len() < current_hash.len() => current_hash.starts_with(expected),
        Some(expected) => expected == current_hash,
        None => true,
    };
    let mismatch = expected_hash.is_some() && !hash_match;
    let live_blocked = enabled && enforce_live && mismatch && !allow_mismatch;
    let mode = if !enabled {
        "disabled"
    } else if expected_hash.is_none() {
        "observe"
    } else if mismatch && allow_mismatch {
        "mismatch_allowed"
    } else if mismatch {
        "mismatch_blocking"
    } else {
        "locked_ok"
    };
    json!({
        "enabled": enabled,
        "enforce_live": enforce_live,
        "allow_mismatch": allow_mismatch,
        "mode": mode,
        "profile": profile_name,
        "current_hash": current_hash,
        "expected_hash": expected_hash,
        "mismatch": mismatch,
        "live_blocked": live_blocked
    })
}

#[allow(dead_code)]
pub(super) fn strategy_backup_baseline_config() -> StrategyRuntimeConfig {
    StrategyRuntimeConfig {
        entry_threshold_base: 0.30,
        entry_threshold_cap: 0.70,
        spread_limit_prob: 0.022381,
        entry_edge_prob: 0.005,
        entry_min_potential_cents: 10.32355,
        entry_max_price_cents: 72.915692,
        min_hold_ms: 3_702,
        stop_loss_cents: 5.0,
        reverse_signal_threshold: -0.6368297565445915,
        reverse_signal_ticks: 1,
        trail_activate_profit_cents: 10.0,
        trail_drawdown_cents: 3.0,
        take_profit_near_max_cents: 87.74805411830562,
        endgame_take_profit_cents: 86.18788390856207,
        endgame_remaining_ms: 13_670,
        liquidity_widen_prob: 0.05,
        cooldown_ms: 0,
        max_entries_per_round: 10,
        max_exec_spread_cents: 5.0,
        slippage_cents_per_side: 0.06918614011422781,
        fee_cents_per_side: 0.03270800007174326,
        emergency_wide_spread_penalty_ratio: 0.27217322622042583,
        stop_loss_grace_ticks: 2,
        stop_loss_hard_mult: 1.45,
        stop_loss_reverse_extra_ticks: 1,
        loss_cluster_limit: 3,
        loss_cluster_cooldown_ms: 25_000,
        market_quality_enabled: true,
        market_quality_min: 0.35,
        staleness_entry_threshold: 1.5,
        staleness_exit_threshold: 1.2,
        velocity_min_bps: 2.0,
        accel_reverse_exit_ticks: 2,
        vic_enabled: true,
        vic_target_entries_per_hour: 14.0,
        vic_deadband_ratio: 0.08,
        vic_threshold_relax_max: 0.02,
        vic_edge_relax_max: 0.008,
        vic_spread_relax_max: 0.12,
        paper_slippage_mult: 1.5,
        paper_latency_penalty_cents: 0.3,
        paper_fill_rate_discount: 0.05,
    }
}

fn strategy_profit_max_config() -> StrategyRuntimeConfig {
    StrategyRuntimeConfig {
        entry_threshold_base: 0.30,
        entry_threshold_cap: 0.80,
        spread_limit_prob: 0.023675384305404043,
        entry_edge_prob: 0.005,
        entry_min_potential_cents: 10.0,
        entry_max_price_cents: 70.62354055896994,
        min_hold_ms: 2_277,
        stop_loss_cents: 5.0,
        reverse_signal_threshold: -0.15512720801233063,
        reverse_signal_ticks: 2,
        trail_activate_profit_cents: 10.0,
        trail_drawdown_cents: 3.0,
        take_profit_near_max_cents: 97.02669954722674,
        endgame_take_profit_cents: 95.62661300231477,
        endgame_remaining_ms: 23_453,
        liquidity_widen_prob: 0.05,
        cooldown_ms: 2_268,
        max_entries_per_round: 3,
        max_exec_spread_cents: 5.0,
        slippage_cents_per_side: 0.13017362950853426,
        fee_cents_per_side: 0.04024842164853446,
        emergency_wide_spread_penalty_ratio: 0.29593221663217434,
        stop_loss_grace_ticks: 2,
        stop_loss_hard_mult: 1.45,
        stop_loss_reverse_extra_ticks: 1,
        loss_cluster_limit: 3,
        loss_cluster_cooldown_ms: 25_000,
        market_quality_enabled: true,
        market_quality_min: 0.35,
        staleness_entry_threshold: 1.5,
        staleness_exit_threshold: 1.2,
        velocity_min_bps: 2.0,
        accel_reverse_exit_ticks: 2,
        vic_enabled: true,
        vic_target_entries_per_hour: 14.0,
        vic_deadband_ratio: 0.08,
        vic_threshold_relax_max: 0.02,
        vic_edge_relax_max: 0.008,
        vic_spread_relax_max: 0.12,
        paper_slippage_mult: 1.5,
        paper_latency_penalty_cents: 0.3,
        paper_fill_rate_discount: 0.05,
    }
}

fn strategy_hi_freq_config() -> StrategyRuntimeConfig {
    StrategyRuntimeConfig {
        entry_threshold_base: 0.30,
        entry_threshold_cap: 0.80,
        spread_limit_prob: 0.031050931460392672,
        entry_edge_prob: 0.005,
        entry_min_potential_cents: 8.0,
        entry_max_price_cents: 74.19641304603604,
        min_hold_ms: 4_271,
        stop_loss_cents: 5.0,
        reverse_signal_threshold: -0.24517460190661405,
        reverse_signal_ticks: 2,
        trail_activate_profit_cents: 10.0,
        trail_drawdown_cents: 3.0,
        take_profit_near_max_cents: 97.32996482850581,
        endgame_take_profit_cents: 97.27772783424335,
        endgame_remaining_ms: 23_313,
        liquidity_widen_prob: 0.05,
        cooldown_ms: 7_196,
        max_entries_per_round: 3,
        max_exec_spread_cents: 5.0,
        slippage_cents_per_side: 0.12346479836309485,
        fee_cents_per_side: 0.03,
        emergency_wide_spread_penalty_ratio: 0.197306675940024,
        stop_loss_grace_ticks: 2,
        stop_loss_hard_mult: 1.45,
        stop_loss_reverse_extra_ticks: 1,
        loss_cluster_limit: 3,
        loss_cluster_cooldown_ms: 25_000,
        market_quality_enabled: true,
        market_quality_min: 0.35,
        staleness_entry_threshold: 1.5,
        staleness_exit_threshold: 1.2,
        velocity_min_bps: 2.0,
        accel_reverse_exit_ticks: 2,
        vic_enabled: true,
        vic_target_entries_per_hour: 14.0,
        vic_deadband_ratio: 0.08,
        vic_threshold_relax_max: 0.02,
        vic_edge_relax_max: 0.008,
        vic_spread_relax_max: 0.12,
        paper_slippage_mult: 1.5,
        paper_latency_penalty_cents: 0.3,
        paper_fill_rate_discount: 0.05,
    }
}

fn strategy_hi_win_config() -> StrategyRuntimeConfig {
    StrategyRuntimeConfig {
        entry_threshold_base: 0.30,
        entry_threshold_cap: 0.75,
        spread_limit_prob: 0.0288942174328033,
        entry_edge_prob: 0.005,
        entry_min_potential_cents: 12.0,
        entry_max_price_cents: 65.17784700310176,
        min_hold_ms: 3_235,
        stop_loss_cents: 5.0,
        reverse_signal_threshold: -0.10037499713079152,
        reverse_signal_ticks: 5,
        trail_activate_profit_cents: 10.0,
        trail_drawdown_cents: 3.0,
        take_profit_near_max_cents: 97.73898243530077,
        endgame_take_profit_cents: 93.1420351179262,
        endgame_remaining_ms: 24_778,
        liquidity_widen_prob: 0.05,
        cooldown_ms: 5_482,
        max_entries_per_round: 2,
        max_exec_spread_cents: 5.0,
        slippage_cents_per_side: 0.14515338668372577,
        fee_cents_per_side: 0.05331355583758494,
        emergency_wide_spread_penalty_ratio: 0.20954404654691547,
        stop_loss_grace_ticks: 2,
        stop_loss_hard_mult: 1.45,
        stop_loss_reverse_extra_ticks: 1,
        loss_cluster_limit: 3,
        loss_cluster_cooldown_ms: 25_000,
        market_quality_enabled: true,
        market_quality_min: 0.35,
        staleness_entry_threshold: 1.5,
        staleness_exit_threshold: 1.2,
        velocity_min_bps: 2.0,
        accel_reverse_exit_ticks: 2,
        vic_enabled: true,
        vic_target_entries_per_hour: 14.0,
        vic_deadband_ratio: 0.08,
        vic_threshold_relax_max: 0.02,
        vic_edge_relax_max: 0.008,
        vic_spread_relax_max: 0.12,
        paper_slippage_mult: 1.5,
        paper_latency_penalty_cents: 0.3,
        paper_fill_rate_discount: 0.05,
    }
}

fn strategy_balanced_config() -> StrategyRuntimeConfig {
    StrategyRuntimeConfig {
        entry_threshold_base: 0.30,
        entry_threshold_cap: 0.75,
        spread_limit_prob: 0.02439836783513124,
        entry_edge_prob: 0.005,
        entry_min_potential_cents: 10.0,
        entry_max_price_cents: 75.52831419817902,
        min_hold_ms: 0,
        stop_loss_cents: 5.0,
        reverse_signal_threshold: -0.1770513110145612,
        reverse_signal_ticks: 2,
        trail_activate_profit_cents: 10.0,
        trail_drawdown_cents: 3.0,
        take_profit_near_max_cents: 99.5,
        endgame_take_profit_cents: 93.55513599829052,
        endgame_remaining_ms: 20_518,
        liquidity_widen_prob: 0.05,
        cooldown_ms: 4_654,
        max_entries_per_round: 3,
        max_exec_spread_cents: 5.0,
        slippage_cents_per_side: 0.10036573476058915,
        fee_cents_per_side: 0.03,
        emergency_wide_spread_penalty_ratio: 0.13810130927202827,
        stop_loss_grace_ticks: 2,
        stop_loss_hard_mult: 1.45,
        stop_loss_reverse_extra_ticks: 1,
        loss_cluster_limit: 3,
        loss_cluster_cooldown_ms: 25_000,
        market_quality_enabled: true,
        market_quality_min: 0.35,
        staleness_entry_threshold: 1.5,
        staleness_exit_threshold: 1.2,
        velocity_min_bps: 2.0,
        accel_reverse_exit_ticks: 2,
        vic_enabled: true,
        vic_target_entries_per_hour: 14.0,
        vic_deadband_ratio: 0.08,
        vic_threshold_relax_max: 0.02,
        vic_edge_relax_max: 0.008,
        vic_spread_relax_max: 0.12,
        paper_slippage_mult: 1.5,
        paper_latency_penalty_cents: 0.3,
        paper_fill_rate_discount: 0.05,
    }
}

fn strategy_cand_growth_mix_config() -> StrategyRuntimeConfig {
    StrategyRuntimeConfig {
        entry_threshold_base: 0.30,
        entry_threshold_cap: 0.80,
        spread_limit_prob: 0.031847184690691074,
        entry_edge_prob: 0.005,
        entry_min_potential_cents: 2.5648265524713785,
        entry_max_price_cents: 72.91716340298034,
        min_hold_ms: 4_957,
        stop_loss_cents: 5.0,
        reverse_signal_threshold: -0.7707977919813044,
        reverse_signal_ticks: 3,
        trail_activate_profit_cents: 2.0,
        trail_drawdown_cents: 1.5742668933343105,
        take_profit_near_max_cents: 92.93113282745682,
        endgame_take_profit_cents: 91.79762168224036,
        endgame_remaining_ms: 27_380,
        liquidity_widen_prob: 0.05,
        cooldown_ms: 14_776,
        max_entries_per_round: 2,
        max_exec_spread_cents: 5.0,
        slippage_cents_per_side: 0.052941519427761694,
        fee_cents_per_side: 0.03725807613665984,
        emergency_wide_spread_penalty_ratio: 0.04172825825876407,
        stop_loss_grace_ticks: 7,
        stop_loss_hard_mult: 1.670270804386013,
        stop_loss_reverse_extra_ticks: 0,
        loss_cluster_limit: 3,
        loss_cluster_cooldown_ms: 25_000,
        market_quality_enabled: true,
        market_quality_min: 0.35,
        staleness_entry_threshold: 1.5,
        staleness_exit_threshold: 1.2,
        velocity_min_bps: 2.0,
        accel_reverse_exit_ticks: 2,
        vic_enabled: true,
        vic_target_entries_per_hour: 14.0,
        vic_deadband_ratio: 0.08,
        vic_threshold_relax_max: 0.02,
        vic_edge_relax_max: 0.008,
        vic_spread_relax_max: 0.12,
        paper_slippage_mult: 1.5,
        paper_latency_penalty_cents: 0.3,
        paper_fill_rate_discount: 0.05,
    }
}

pub(super) fn strategy_current_default_config() -> StrategyRuntimeConfig {
    strategy_current_default_config_for_scope("BTCUSDT", "5m")
}

impl Default for StrategyRuntimeConfig {
    fn default() -> Self {
        strategy_current_default_config()
    }
}

