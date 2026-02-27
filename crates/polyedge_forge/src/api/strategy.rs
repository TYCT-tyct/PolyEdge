use super::*;
use sha2::{Digest, Sha256};
use std::sync::OnceLock;

#[derive(Debug, Clone)]
pub(super) struct StrategySample {
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
pub(super) struct StrategyRuntimeConfig {
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

const STRATEGY_PROFILE_PROFIT_MAX: &str = "fev1_manual_profit_max_2026_02_27";
const STRATEGY_PROFILE_HI_FREQ: &str = "fev1_manual_hi_freq_2026_02_27";
const STRATEGY_PROFILE_HI_WIN: &str = "fev1_manual_hi_win_2026_02_27";

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

fn strategy_env_u32(name: &str, default: u32, min_v: u32, max_v: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(default)
        .clamp(min_v, max_v)
}

fn strategy_default_max_points(full_history: bool) -> u32 {
    if full_history {
        strategy_env_u32("FORGE_STRATEGY_MAX_POINTS_FULL", 320_000, 20_000, 2_000_000)
    } else {
        strategy_env_u32("FORGE_STRATEGY_MAX_POINTS_SHORT", 180_000, 20_000, 600_000)
    }
}

fn strategy_max_points_hard_cap() -> u32 {
    strategy_env_u32(
        "FORGE_STRATEGY_MAX_POINTS_HARD_CAP",
        600_000,
        20_000,
        5_000_000,
    )
}

fn strategy_select_profile_name() -> &'static str {
    if let Ok(raw) = std::env::var("FORGE_STRATEGY_BASE_PROFILE") {
        match raw.trim().to_ascii_lowercase().as_str() {
            "profit_max" | "manual_profit_max" | "max" | "fev1_manual_profit_max_2026_02_27" => {
                return STRATEGY_PROFILE_PROFIT_MAX;
            }
            "hi_win" | "manual_hi_win" | "safe" | "fev1_manual_hi_win_2026_02_27" => {
                return STRATEGY_PROFILE_HI_WIN;
            }
            "hi_freq" | "manual_hi_freq" | "freq" | "fev1_manual_hi_freq_2026_02_27" => {
                return STRATEGY_PROFILE_HI_FREQ;
            }
            _ => {}
        }
    }
    let equity_base = std::env::var("FORGE_FEV1_CAPITAL_BASE_USDC")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(50.0);
    if equity_base <= 60.0 {
        STRATEGY_PROFILE_HI_WIN
    } else {
        STRATEGY_PROFILE_HI_FREQ
    }
}

fn strategy_current_default_profile_name() -> &'static str {
    strategy_select_profile_name()
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

fn strategy_cfg_hash(cfg: &StrategyRuntimeConfig) -> String {
    let mut hasher = Sha256::new();
    let profile = strategy_current_default_profile_name();
    let canonical = serde_json::to_string(&strategy_cfg_json(cfg)).unwrap_or_default();
    hasher.update(profile.as_bytes());
    hasher.update(b"|");
    hasher.update(canonical.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn strategy_fixed_guard_payload(cfg: &StrategyRuntimeConfig) -> Value {
    let enabled = strategy_env_bool("FORGE_STRATEGY_FIXED_GUARD_ENABLED", true);
    let enforce_live = strategy_env_bool("FORGE_STRATEGY_FIXED_GUARD_ENFORCE_LIVE", true);
    let allow_mismatch = strategy_env_bool("FORGE_STRATEGY_FIXED_GUARD_ALLOW_MISMATCH", false);
    let current_hash = strategy_cfg_hash(cfg);
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
        "profile": strategy_current_default_profile_name(),
        "current_hash": current_hash,
        "expected_hash": expected_hash,
        "mismatch": mismatch,
        "live_blocked": live_blocked
    })
}

#[allow(dead_code)]
pub(super) fn strategy_backup_baseline_config() -> StrategyRuntimeConfig {
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

fn strategy_profit_max_config() -> StrategyRuntimeConfig {
    StrategyRuntimeConfig {
        entry_threshold_base: 0.7369682327402423,
        entry_threshold_cap: 0.9497259999257743,
        spread_limit_prob: 0.023675384305404043,
        entry_edge_prob: 0.07006913988697057,
        entry_min_potential_cents: 18.135042971301328,
        entry_max_price_cents: 70.62354055896994,
        min_hold_ms: 2_277,
        stop_loss_cents: 11.992982528968525,
        reverse_signal_threshold: -0.15512720801233063,
        reverse_signal_ticks: 2,
        trail_activate_profit_cents: 26.093956734426936,
        trail_drawdown_cents: 16.820471088753425,
        take_profit_near_max_cents: 97.02669954722674,
        endgame_take_profit_cents: 95.62661300231477,
        endgame_remaining_ms: 23_453,
        liquidity_widen_prob: 0.06300478937274487,
        cooldown_ms: 2_268,
        max_entries_per_round: 3,
        max_exec_spread_cents: 1.2249300342458038,
        slippage_cents_per_side: 0.13017362950853426,
        fee_cents_per_side: 0.04024842164853446,
        emergency_wide_spread_penalty_ratio: 0.29593221663217434,
    }
}

fn strategy_hi_freq_config() -> StrategyRuntimeConfig {
    StrategyRuntimeConfig {
        entry_threshold_base: 0.7686523821483723,
        entry_threshold_cap: 0.99,
        spread_limit_prob: 0.031050931460392672,
        entry_edge_prob: 0.04397227060534279,
        entry_min_potential_cents: 11.713295063203187,
        entry_max_price_cents: 74.19641304603604,
        min_hold_ms: 4_271,
        stop_loss_cents: 13.44825602224101,
        reverse_signal_threshold: -0.24517460190661405,
        reverse_signal_ticks: 2,
        trail_activate_profit_cents: 23.872099447769717,
        trail_drawdown_cents: 15.457789629059327,
        take_profit_near_max_cents: 97.32996482850581,
        endgame_take_profit_cents: 97.27772783424335,
        endgame_remaining_ms: 23_313,
        liquidity_widen_prob: 0.0671475985081152,
        cooldown_ms: 7_196,
        max_entries_per_round: 3,
        max_exec_spread_cents: 1.2418651648940238,
        slippage_cents_per_side: 0.12346479836309485,
        fee_cents_per_side: 0.03,
        emergency_wide_spread_penalty_ratio: 0.197306675940024,
    }
}

fn strategy_hi_win_config() -> StrategyRuntimeConfig {
    StrategyRuntimeConfig {
        entry_threshold_base: 0.7928044963810135,
        entry_threshold_cap: 0.9849918027705858,
        spread_limit_prob: 0.028629416086831005,
        entry_edge_prob: 0.06259309494575992,
        entry_min_potential_cents: 17.245351179461537,
        entry_max_price_cents: 65.17784700310176,
        min_hold_ms: 3_235,
        stop_loss_cents: 13.14946542892194,
        reverse_signal_threshold: -0.10037499713079152,
        reverse_signal_ticks: 4,
        trail_activate_profit_cents: 24.393265002393424,
        trail_drawdown_cents: 17.632807444464234,
        take_profit_near_max_cents: 97.73898243530077,
        endgame_take_profit_cents: 93.1420351179262,
        endgame_remaining_ms: 24_778,
        liquidity_widen_prob: 0.0620961928172456,
        cooldown_ms: 2_671,
        max_entries_per_round: 3,
        max_exec_spread_cents: 1.1661721107208205,
        slippage_cents_per_side: 0.14515338668372577,
        fee_cents_per_side: 0.05331355583758494,
        emergency_wide_spread_penalty_ratio: 0.20954404654691547,
    }
}

pub(super) fn strategy_current_default_config() -> StrategyRuntimeConfig {
    match strategy_select_profile_name() {
        STRATEGY_PROFILE_HI_WIN => strategy_hi_win_config(),
        STRATEGY_PROFILE_HI_FREQ => strategy_hi_freq_config(),
        _ => strategy_profit_max_config(),
    }
}

impl Default for StrategyRuntimeConfig {
    fn default() -> Self {
        strategy_current_default_config()
    }
}

pub(super) fn parse_strategy_rows(rows: Vec<Value>) -> Vec<StrategySample> {
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
            .or(match (raw_bid_yes, raw_ask_yes) {
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
pub(super) struct StrategySimulationResult {
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
pub(super) struct RollingStats {
    window_trades: usize,
    windows: usize,
    latest_win_rate_pct: f64,
    avg_win_rate_pct: f64,
    min_win_rate_pct: f64,
    latest_avg_pnl_cents: f64,
}

pub(super) fn compute_rolling_stats(pnls: &[f64], window_trades: usize) -> RollingStats {
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

pub(super) fn rolling_stats_json(rs: RollingStats) -> Value {
    json!({
        "window_trades": rs.window_trades,
        "windows": rs.windows,
        "latest_win_rate_pct": rs.latest_win_rate_pct,
        "avg_win_rate_pct": rs.avg_win_rate_pct,
        "min_win_rate_pct": rs.min_win_rate_pct,
        "latest_avg_pnl_cents": rs.latest_avg_pnl_cents,
    })
}

pub(super) fn run_summary_json(run: &StrategySimulationResult) -> Value {
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

pub(super) fn strategy_cfg_json(cfg: &StrategyRuntimeConfig) -> Value {
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

pub(super) fn strategy_cfg_from_payload(
    base: StrategyRuntimeConfig,
    payload: &Value,
) -> StrategyRuntimeConfig {
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

pub(super) struct StrategyPaperLiveReq<'a> {
    pub(super) state: &'a ApiState,
    pub(super) market_type: &'a str,
    pub(super) full_history: bool,
    pub(super) lookback_minutes: u32,
    pub(super) max_points: u32,
    pub(super) max_trades: usize,
    pub(super) cfg: &'a StrategyRuntimeConfig,
    pub(super) config_source: &'a str,
    pub(super) autotune_doc: Value,
    pub(super) autotune_live_key: Value,
    pub(super) live_execute: bool,
    pub(super) live_quote_usdc: f64,
    pub(super) live_max_orders: usize,
    pub(super) live_drain_only: bool,
}

pub(super) async fn strategy_paper_live(req: StrategyPaperLiveReq<'_>) -> Result<Value, ApiError> {
    let StrategyPaperLiveReq {
        state,
        market_type,
        full_history,
        lookback_minutes,
        max_points,
        max_trades,
        cfg,
        config_source,
        autotune_doc,
        autotune_live_key,
        live_execute,
        live_quote_usdc,
        live_max_orders,
        live_drain_only,
    } = req;
    let live_execute_requested = live_execute;
    let mut live_execute = live_execute_requested && live_execution_market_allowed(market_type);
    let mut live_execute_block_reason = if live_execute_requested && !live_execute {
        Some("market_not_enabled_for_live_execution".to_string())
    } else {
        None
    };
    let fixed_guard = strategy_fixed_guard_payload(cfg);
    if live_execute
        && fixed_guard
            .get("live_blocked")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    {
        live_execute = false;
        if live_execute_block_reason.is_none() {
            live_execute_block_reason = Some("fixed_config_hash_mismatch".to_string());
        }
    }
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

    let live_gateway_cfg = LiveGatewayConfig::from_env();
    let capital_cfg = LiveCapitalConfig::from_env();
    let capital_scope = if capital_cfg.portfolio_shared {
        LIVE_CAPITAL_PORTFOLIO_SCOPE
    } else {
        market_type
    };
    let (dynamic_quote_usdc, capital_before, balance_sync_ok, balance_sync_error) = state
        .compute_live_quote_usdc(
            market_type,
            live_quote_usdc.max(live_gateway_cfg.min_quote_usdc),
            live_gateway_cfg.min_quote_usdc,
            &capital_cfg,
        )
        .await;

    let mapped_samples = map_samples_to_fev1(&samples);
    let mapped_cfg = map_cfg_to_fev1(cfg);
    let run = fev1::simulate(&mapped_samples, &mapped_cfg, max_trades);
    let gateway = fev1::ArmedSimulatedGateway;
    let dual = fev1::build_gateway_execution(&run, dynamic_quote_usdc.max(0.01), &gateway);
    let live_executor_mode = LiveExecutorMode::from_env();
    reconcile_live_reports(state, &live_gateway_cfg, live_executor_mode).await;
    handle_live_pending_timeouts(state, &live_gateway_cfg, live_executor_mode).await;
    let live_market = resolve_live_market_target(market_type).await.ok();
    let position_before_gate = state.get_live_position_state(market_type).await;
    let prefer_action = if live_drain_only {
        if position_before_gate.side.is_some() {
            Some("exit")
        } else {
            None
        }
    } else if position_before_gate.side.is_some() {
        Some("exit")
    } else {
        Some("enter")
    };
    let selected_decisions_raw = select_live_decisions(
        &dual.decisions,
        samples.last().map(|s| s.ts_ms).unwrap_or_default(),
        live_max_orders.max(1),
        live_drain_only,
        prefer_action,
    );
    let mut selected_decisions = Vec::<Value>::new();
    let mut capital_skipped = Vec::<Value>::new();
    let mut capital_remaining = capital_before.available_to_trade_usdc.max(0.0);
    let base_add_scale = if capital_before.equity_estimate_usdc <= capital_cfg.small_threshold_usdc
    {
        capital_cfg.add_scale_small
    } else {
        capital_cfg.add_scale_large
    };
    let max_add_layers = max_add_layers_by_equity(
        capital_cfg.max_add_layers,
        capital_before.equity_estimate_usdc,
    );
    let mut current_add_layers = if position_before_gate.side.is_some() {
        position_before_gate.open_add_layers
    } else {
        0
    };
    let in_loss_phase = capital_before.last_realized_pnl_usdc < 0.0
        || capital_before.consecutive_losses > 0
        || matches!(
            capital_before.risk_state.as_str(),
            "caution" | "defensive" | "lockdown"
        );
    let has_explicit_exit_candidate = selected_decisions_raw.iter().any(|v| {
        v.get("action")
            .and_then(Value::as_str)
            .map(|a| a.eq_ignore_ascii_case("exit") || a.eq_ignore_ascii_case("reduce"))
            .unwrap_or(false)
    });
    let mut reverse_exit_injected = false;
    for mut d in selected_decisions_raw {
        let mut action = d
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        let side = d
            .get("side")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_uppercase();
        let entry_like_action = action == "enter" || action == "add";
        if live_execute
            && entry_like_action
            && capital_cfg.use_real_balance
            && capital_cfg.hard_require_real_balance
            && !balance_sync_ok
        {
            capital_skipped.push(json!({
                "reason": "balance_sync_required",
                "balance_sync_ok": balance_sync_ok,
                "balance_sync_error": balance_sync_error,
                "decision": d
            }));
            continue;
        }
        if action == "enter" {
            if let Some(state_side) = position_before_gate.side.as_deref() {
                if state_side.eq_ignore_ascii_case(&side) {
                    if capital_cfg.disable_add_on_loss && in_loss_phase {
                        capital_skipped.push(json!({
                            "reason": "add_disabled_on_loss_or_risk",
                            "risk_state": capital_before.risk_state,
                            "consecutive_losses": capital_before.consecutive_losses,
                            "last_realized_pnl_usdc": capital_before.last_realized_pnl_usdc,
                            "decision": d
                        }));
                        continue;
                    }
                    if current_add_layers >= max_add_layers {
                        capital_skipped.push(json!({
                            "reason": "max_add_layers_reached",
                            "decision": d
                        }));
                        continue;
                    }
                    current_add_layers = current_add_layers.saturating_add(1);
                    action = "add".to_string();
                    let base_q = d
                        .get("quote_size_usdc")
                        .and_then(Value::as_f64)
                        .unwrap_or(dynamic_quote_usdc);
                    let add_scale =
                        dynamic_add_scale(&d, base_add_scale, &capital_before, &capital_cfg);
                    let scaled_q = (base_q * add_scale).max(live_gateway_cfg.min_quote_usdc);
                    let reason = d
                        .get("reason")
                        .and_then(Value::as_str)
                        .unwrap_or("fev1_signal_entry")
                        .to_string();
                    if let Some(obj) = d.as_object_mut() {
                        obj.insert("action".to_string(), Value::String("add".to_string()));
                        obj.insert("quote_size_usdc".to_string(), json!(scaled_q));
                        obj.insert(
                            "reason".to_string(),
                            Value::String(format!("{}_auto_add", reason)),
                        );
                    }
                } else if capital_cfg.reverse_two_phase {
                    if !has_explicit_exit_candidate && !reverse_exit_injected {
                        let close_side = state_side.to_ascii_uppercase();
                        let close_quote = position_before_gate
                            .net_quote_usdc
                            .max(position_before_gate.entry_quote_usdc.unwrap_or(0.0))
                            .max(dynamic_quote_usdc);
                        let reverse_ts = d
                            .get("ts_ms")
                            .and_then(Value::as_i64)
                            .unwrap_or_else(|| Utc::now().timestamp_millis())
                            .saturating_sub(1);
                        let reverse_round = d
                            .get("round_id")
                            .and_then(Value::as_str)
                            .or(position_before_gate.entry_round_id.as_deref())
                            .unwrap_or_default()
                            .to_string();
                        let reverse_price_cents = d
                            .get("price_cents")
                            .and_then(Value::as_f64)
                            .or(position_before_gate.vwap_entry_cents)
                            .or(position_before_gate.entry_price_cents)
                            .unwrap_or(50.0);
                        selected_decisions.push(json!({
                            "action": "exit",
                            "side": close_side,
                            "round_id": reverse_round,
                            "ts_ms": reverse_ts,
                            "reason": "auto_reverse_exit_first",
                            "quote_size_usdc": close_quote,
                            "price_cents": reverse_price_cents,
                            "tif": "FAK",
                            "style": "taker",
                            "ttl_ms": 900,
                            "max_slippage_bps": live_gateway_cfg.exit_slippage_bps
                        }));
                        reverse_exit_injected = true;
                    }
                    capital_skipped.push(json!({
                        "reason": "reverse_two_phase_wait_exit",
                        "state_side": state_side,
                        "decision": d
                    }));
                    continue;
                }
            } else {
                let q = d
                    .get("quote_size_usdc")
                    .and_then(Value::as_f64)
                    .unwrap_or(dynamic_quote_usdc)
                    .max(live_gateway_cfg.min_quote_usdc);
                let tuned_q = dynamic_entry_quote(
                    &d,
                    q,
                    live_gateway_cfg.min_quote_usdc,
                    &capital_before,
                    &capital_cfg,
                );
                if let Some(obj) = d.as_object_mut() {
                    obj.insert("quote_size_usdc".to_string(), json!(tuned_q));
                }
            }
        } else if action == "add" {
            let Some(state_side) = position_before_gate.side.as_deref() else {
                capital_skipped.push(json!({
                    "reason": "add_without_open_position",
                    "decision": d
                }));
                continue;
            };
            if !state_side.eq_ignore_ascii_case(&side) {
                capital_skipped.push(json!({
                    "reason": "add_side_mismatch",
                    "state_side": state_side,
                    "decision": d
                }));
                continue;
            }
            if capital_cfg.disable_add_on_loss && in_loss_phase {
                capital_skipped.push(json!({
                    "reason": "add_disabled_on_loss_or_risk",
                    "risk_state": capital_before.risk_state,
                    "consecutive_losses": capital_before.consecutive_losses,
                    "last_realized_pnl_usdc": capital_before.last_realized_pnl_usdc,
                    "decision": d
                }));
                continue;
            }
            if current_add_layers >= max_add_layers {
                capital_skipped.push(json!({
                    "reason": "max_add_layers_reached",
                    "decision": d
                }));
                continue;
            }
            current_add_layers = current_add_layers.saturating_add(1);
            let base_q = d
                .get("quote_size_usdc")
                .and_then(Value::as_f64)
                .unwrap_or(dynamic_quote_usdc);
            let add_scale = dynamic_add_scale(&d, base_add_scale, &capital_before, &capital_cfg);
            let scaled_q = (base_q * add_scale).max(live_gateway_cfg.min_quote_usdc);
            if let Some(obj) = d.as_object_mut() {
                obj.insert("quote_size_usdc".to_string(), json!(scaled_q));
            }
        }
        let entry_like = d
            .get("action")
            .and_then(Value::as_str)
            .map(|v| v.eq_ignore_ascii_case("enter") || v.eq_ignore_ascii_case("add"))
            .unwrap_or(false);
        if entry_like {
            let quote = d
                .get("quote_size_usdc")
                .and_then(Value::as_f64)
                .unwrap_or(dynamic_quote_usdc)
                .max(live_gateway_cfg.min_quote_usdc);
            if quote > capital_remaining.max(0.0) {
                capital_skipped.push(json!({
                    "reason": "insufficient_available_capital",
                    "required_quote_usdc": quote,
                    "available_to_trade_usdc": capital_remaining.max(0.0),
                    "decision": d
                }));
                continue;
            }
            capital_remaining = (capital_remaining - quote).max(0.0);
        }
        selected_decisions.push(d);
    }
    let (gated_decisions, skipped_decisions, mut live_position_state) =
        gate_live_decisions(state, market_type, &selected_decisions, live_execute).await;
    let mut skipped_decisions_all = skipped_decisions;
    skipped_decisions_all.extend(capital_skipped.clone());
    let submitted_decisions: Vec<Value> =
        gated_decisions.iter().map(|v| v.decision.clone()).collect();

    let live_exec_payload = if live_execute {
        match live_market.as_ref() {
            Some(target) => {
                let execution_orders = execute_live_orders(
                    state,
                    &live_gateway_cfg,
                    live_executor_mode,
                    target,
                    &live_position_state,
                    &gated_decisions,
                )
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
                        if action == "enter" || action == "add" {
                            live_position_state.state = "enter_pending".to_string();
                        } else if action == "exit" || action == "reduce" {
                            live_position_state.state = "exit_pending".to_string();
                        }
                        let order_id = extract_order_id_from_gateway_record(record);
                        if let Some(order_id) = order_id {
                            let filled_size = record
                                .get("response")
                                .and_then(|v| v.get("accepted_size"))
                                .and_then(Value::as_f64)
                                .unwrap_or(0.0)
                                .max(0.0);
                            let exec_price = record
                                .get("final_request")
                                .and_then(|v| v.get("price"))
                                .and_then(Value::as_f64)
                                .or_else(|| {
                                    record
                                        .get("request")
                                        .and_then(|v| v.get("price"))
                                        .and_then(Value::as_f64)
                                })
                                .unwrap_or_else(|| {
                                    decision
                                        .get("price_cents")
                                        .and_then(Value::as_f64)
                                        .unwrap_or(0.0)
                                        / 100.0
                                })
                                .max(0.0001);
                            let effective_notional = record
                                .get("response")
                                .and_then(|v| v.get("effective_notional"))
                                .and_then(Value::as_f64)
                                .unwrap_or(filled_size * exec_price)
                                .max(0.0);
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
                                quote_size_usdc: if effective_notional > 0.0 {
                                    effective_notional
                                } else {
                                    decision
                                        .get("quote_size_usdc")
                                        .and_then(Value::as_f64)
                                        .unwrap_or(0.0)
                                },
                                tif: decision
                                    .get("tif")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default()
                                    .to_string(),
                                style: decision
                                    .get("style")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default()
                                    .to_string(),
                                submit_reason: decision
                                    .get("reason")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default()
                                    .to_string(),
                                submitted_ts_ms: ts_ms,
                                cancel_after_ms: if action == "exit" || action == "reduce" {
                                    2200
                                } else {
                                    3600
                                },
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
    let count_action = |rows: &[Value], target: &str| -> usize {
        rows.iter()
            .filter(|d| {
                d.get("action")
                    .and_then(Value::as_str)
                    .map(|v| v.eq_ignore_ascii_case(target))
                    .unwrap_or(false)
            })
            .count()
    };
    let paper_entry_count = count_action(&dual.decisions, "enter");
    let paper_add_count = count_action(&dual.decisions, "add");
    let paper_reduce_count = count_action(&dual.decisions, "reduce");
    let paper_exit_count = count_action(&dual.decisions, "exit");
    let submitted_entry_count = count_action(&submitted_decisions, "enter");
    let submitted_add_count = count_action(&submitted_decisions, "add");
    let submitted_reduce_count = count_action(&submitted_decisions, "reduce");
    let submitted_exit_count = count_action(&submitted_decisions, "exit");
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
    let gateway_accept_count = if live_execute {
        gateway_accept_count_raw
    } else {
        0
    };
    let gateway_reject_count = if live_execute {
        gateway_reject_count_raw
    } else {
        0
    };
    let no_live_market_target = live_exec_payload
        .get("error")
        .and_then(Value::as_str)
        .map(|v| v == "no_live_market_target")
        .unwrap_or(false);
    let balance_blocked_entries = skipped_decisions_all.iter().any(|row| {
        row.get("reason")
            .and_then(Value::as_str)
            .map(|v| v == "balance_sync_required")
            .unwrap_or(false)
    });
    let paper_has_signal = paper_entry_count > 0
        || paper_add_count > 0
        || paper_reduce_count > 0
        || paper_exit_count > 0;
    let submitted_has_signal = submitted_entry_count > 0
        || submitted_add_count > 0
        || submitted_reduce_count > 0
        || submitted_exit_count > 0;
    let status = if !live_execute {
        "dry_run"
    } else if no_live_market_target {
        "blocked_no_market_target"
    } else if balance_blocked_entries
        && (paper_entry_count + paper_add_count > 0)
        && (submitted_entry_count + submitted_add_count == 0)
    {
        "blocked_balance_sync"
    } else if paper_has_signal && !submitted_has_signal {
        "blocked_by_gate_or_state"
    } else if submitted_has_signal && gateway_accept_count == 0 {
        "gateway_rejected_all"
    } else {
        "ok"
    };
    let level = match status {
        "ok" | "dry_run" => "ok",
        "blocked_by_gate_or_state" | "blocked_no_market_target" | "blocked_balance_sync" => "warn",
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
            skipped_count = skipped_decisions_all.len(),
            "fev1 paper-live parity mismatch"
        );
    }
    let live_submitted_count = if live_execute {
        submitted_decisions.len()
    } else {
        0
    };
    let live_submitted_entry_count = if live_execute {
        submitted_entry_count
    } else {
        0
    };
    let live_submitted_exit_count = if live_execute {
        submitted_exit_count
    } else {
        0
    };
    let simulated_submitted_count = if live_execute {
        0
    } else {
        submitted_decisions.len()
    };
    let simulated_submitted_entry_count = if live_execute {
        0
    } else {
        submitted_entry_count
    };
    let simulated_submitted_exit_count = if live_execute {
        0
    } else {
        submitted_exit_count
    };

    let parity_check = json!({
        "status": status,
        "level": level,
        "paper": {
            "decision_count": dual.decisions.len(),
            "entry_count": paper_entry_count,
            "add_count": paper_add_count,
            "reduce_count": paper_reduce_count,
            "exit_count": paper_exit_count
        },
        "live": {
            "submitted_count": live_submitted_count,
            "submitted_entry_count": live_submitted_entry_count,
            "submitted_add_count": if live_execute { submitted_add_count } else { 0 },
            "submitted_reduce_count": if live_execute { submitted_reduce_count } else { 0 },
            "submitted_exit_count": live_submitted_exit_count,
            "accepted_count": gateway_accept_count,
            "rejected_count": gateway_reject_count,
            "skipped_count": skipped_decisions_all.len(),
            "no_live_market_target": no_live_market_target,
            "simulated_submitted_count": simulated_submitted_count,
            "simulated_submitted_entry_count": simulated_submitted_entry_count,
            "simulated_submitted_add_count": if live_execute { 0 } else { submitted_add_count },
            "simulated_submitted_reduce_count": if live_execute { 0 } else { submitted_reduce_count },
            "simulated_submitted_exit_count": simulated_submitted_exit_count
        }
    });
    let live_events = state.list_live_events(market_type, 60).await;
    let live_position_state = state.get_live_position_state(market_type).await;
    let capital_state = state
        .get_live_capital_state(market_type, &capital_cfg)
        .await;
    let live_balance_snapshot = if capital_cfg.use_real_balance {
        state
            .get_live_balance_snapshot(capital_cfg.balance_refresh_ms)
            .await
            .ok()
            .flatten()
    } else {
        None
    };
    let market_pending_orders = state.list_pending_orders_for_market(market_type).await;
    let decisions_for_response = tail_slice(&dual.decisions, 120);
    let paper_records_for_response = tail_slice(&dual.paper_records, 120);
    let live_records_for_response = tail_slice(&dual.live_records, 120);
    let submitted_for_response = tail_slice(&submitted_decisions, 120);
    let skipped_for_response = tail_slice(&skipped_decisions_all, 120);
    let trades_for_response = tail_slice(&run.trades, 80);

    Ok(json!({
        "source": "live",
        "execution_target": "live",
        "live_enabled": exec_gate.live_enabled,
        "strategy_engine": "forge_fev1",
        "strategy_alias": "FEV1",
        "engine_version": "v1",
        "executor_mode": "same_signal_dual_executor",
        "live_executor": if live_execute { live_executor_mode.as_str() } else { "dry_run" },
        "live_execute_requested": live_execute_requested,
        "live_execute": live_execute,
        "live_execute_block_reason": live_execute_block_reason,
        "live_execute_markets": live_execution_enabled_markets(),
        "runtime_mode": if live_drain_only { "drain" } else { "normal" },
        "market_type": market_type,
        "lookback_minutes": lookback_minutes,
        "full_history": full_history,
        "samples": samples.len(),
        "config_source": config_source,
        "baseline_profile": strategy_current_default_profile_name(),
        "fixed_guard": fixed_guard,
        "autotune": autotune_doc,
        "autotune_live_key": autotune_live_key,
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
                "skipped_count": skipped_decisions_all.len(),
                "submitted_decisions": submitted_for_response,
                "skipped_decisions": skipped_for_response
            },
            "gateway": {
                "primary_url": live_gateway_cfg.primary_url,
                "backup_url": live_gateway_cfg.backup_url,
                "timeout_ms": live_gateway_cfg.timeout_ms,
                "entry_slippage_bps": live_gateway_cfg.entry_slippage_bps,
                "exit_slippage_bps": live_gateway_cfg.exit_slippage_bps,
                "min_quote_usdc": live_gateway_cfg.min_quote_usdc,
                "rust_submit_fallback_gateway": live_gateway_cfg.rust_submit_fallback_gateway
            },
            "capital": {
                "auto_enabled": capital_cfg.enabled,
                "scope": capital_scope,
                "portfolio_shared": capital_cfg.portfolio_shared,
                "use_real_balance": capital_cfg.use_real_balance,
                "hard_require_real_balance": capital_cfg.hard_require_real_balance,
                "dynamic_quote_usdc": dynamic_quote_usdc,
                "base_quote_usdc": live_quote_usdc,
                "target_utilization": capital_cfg.target_utilization,
                "reserve_usdc": capital_cfg.reserve_usdc,
                "reserve_ratio": capital_cfg.reserve_ratio,
                "reserve_min_usdc": capital_cfg.reserve_min_usdc,
                "small_threshold_usdc": capital_cfg.small_threshold_usdc,
                "max_add_layers": capital_cfg.max_add_layers,
                "disable_add_on_loss": capital_cfg.disable_add_on_loss,
                "balance_refresh_ms": capital_cfg.balance_refresh_ms,
                "balance_sync_ok": balance_sync_ok,
                "balance_sync_error": balance_sync_error,
                "real_balance": live_balance_snapshot,
                "state": capital_state,
                "available_after_capital_guard_usdc": capital_remaining.max(0.0),
                "capital_skipped_count": capital_skipped.len(),
                "open_pending_orders": market_pending_orders.len()
            },
            "execution": live_exec_payload,
            "state_machine": live_position_state,
            "events": live_events
        }
    }))
}

pub(super) fn tail_slice<T: Clone>(items: &[T], limit: usize) -> Vec<T> {
    let n = limit.max(1);
    if items.len() <= n {
        items.to_vec()
    } else {
        items[items.len() - n..].to_vec()
    }
}

pub(super) fn profit_factor(pnls: &[f64]) -> f64 {
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

pub(super) fn loss_tail_penalty(pnls: &[f64]) -> f64 {
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

pub(super) fn side_loss_penalty(trades: &[Value]) -> f64 {
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

pub(super) fn split_samples_by_round(
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

pub(super) fn map_sample_to_fev1(s: &StrategySample) -> fev1::Sample {
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

pub(super) fn map_samples_to_fev1(samples: &[StrategySample]) -> Vec<fev1::Sample> {
    samples.iter().map(map_sample_to_fev1).collect()
}

pub(super) fn map_cfg_to_fev1(cfg: &StrategyRuntimeConfig) -> fev1::RuntimeConfig {
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

pub(super) fn map_simulation_result(run: fev1::SimulationResult) -> StrategySimulationResult {
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

pub(super) fn run_strategy_simulation(
    samples: &[StrategySample],
    cfg: &StrategyRuntimeConfig,
    max_trades: usize,
) -> StrategySimulationResult {
    let mapped_samples = map_samples_to_fev1(samples);
    let mapped_cfg = map_cfg_to_fev1(cfg);
    map_simulation_result(fev1::simulate(&mapped_samples, &mapped_cfg, max_trades))
}

pub(super) async fn load_strategy_samples(
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

pub(super) async fn strategy_paper(
    State(state): State<ApiState>,
    Query(params): Query<StrategyPaperQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let source_mode = parse_strategy_paper_source(params.source.as_deref());
    let market_type = resolve_strategy_market_type(params.market_type.as_deref())?;
    let autotune_context =
        normalize_autotune_context(params.autotune_context.as_deref(), market_type);
    let use_autotune = params.use_autotune.unwrap_or(false);
    let mut cfg = StrategyRuntimeConfig::default();
    let mut config_source = "default";
    let mut autotune_info = Value::Null;
    let mut autotune_key_used = Value::Null;
    let (active_opt, active_key_used) = resolve_autotune_active_doc(&state, market_type).await?;
    let (live_opt, live_key_used) = resolve_autotune_live_doc(&state, market_type).await?;
    if use_autotune {
        if let Some(active) = active_opt {
            cfg = strategy_cfg_from_payload(cfg, &active);
            config_source = "autotune_active";
            autotune_info = active;
            autotune_key_used = Value::String(active_key_used.clone());
        } else {
            let (saved_opt, key_used, _) =
                resolve_autotune_doc(&state, &autotune_context, market_type).await?;
            if let Some(saved) = saved_opt {
                cfg = strategy_cfg_from_payload(cfg, &saved);
                config_source = "autotune_candidate";
                autotune_info = saved;
                autotune_key_used = Value::String(key_used);
            }
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
    let fixed_guard = strategy_fixed_guard_payload(&cfg);
    let full_history = params.full_history.unwrap_or(false);
    let lookback_minutes = params
        .lookback_minutes
        .unwrap_or(if full_history { 30 * 24 * 60 } else { 24 * 60 })
        .clamp(30, 365 * 24 * 60);
    let max_points = params
        .max_points
        .unwrap_or(strategy_default_max_points(full_history))
        .clamp(20_000, strategy_max_points_hard_cap());
    let max_trades = params.max_trades.unwrap_or(200).clamp(20, 1000) as usize;
    let _live_execute = params.live_execute.unwrap_or(false);
    let _live_quote_usdc = params.live_quote_usdc.unwrap_or(1.0).clamp(0.5, 1000.0);
    let _live_max_orders = params.live_max_orders.unwrap_or(1).clamp(1, 8) as usize;
    let _live_entry_only = params.live_entry_only.unwrap_or(true);

    let mut source_fallback_error: Option<String> = None;
    if matches!(
        source_mode,
        StrategyPaperSource::Live | StrategyPaperSource::Auto
    ) {
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
            "autotune_context": autotune_context,
            "autotune_active_key": active_key_used,
            "autotune_live_key": live_key_used,
            "autotune_live_found": live_opt.is_some(),
            "fixed_guard": fixed_guard,
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
        "autotune_context": autotune_context,
        "autotune_active_key": active_key_used,
        "autotune_live_key": live_key_used,
        "autotune_live_found": live_opt.is_some(),
        "lookback_minutes": lookback_minutes,
        "full_history": full_history,
        "samples": samples.len(),
        "config_source": config_source,
        "baseline_profile": strategy_current_default_profile_name(),
        "fixed_guard": fixed_guard,
        "autotune": autotune_info,
        "autotune_key_used": autotune_key_used,
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

pub(super) async fn strategy_live_reset(
    State(state): State<ApiState>,
) -> Result<Json<Value>, ApiError> {
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
    {
        let mut cache = state.live_rust_book_cache.write().await;
        cache.clear();
    }
    {
        let mut cache = state.live_balance_cache.write().await;
        cache.take();
    }
    {
        let mut exec = state.live_rust_executor.write().await;
        exec.take();
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

#[derive(Debug, Deserialize)]
pub(super) struct StrategyLiveBalanceQueryParams {
    market_type: Option<String>,
    refresh_ms: Option<i64>,
}

pub(super) async fn strategy_live_balance(
    State(state): State<ApiState>,
    Query(params): Query<StrategyLiveBalanceQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let market_type = resolve_strategy_market_type(params.market_type.as_deref())?;
    let mut capital_cfg = LiveCapitalConfig::from_env();
    let refresh_ms = params
        .refresh_ms
        .unwrap_or(capital_cfg.balance_refresh_ms)
        .clamp(500, 120_000);
    capital_cfg.balance_refresh_ms = refresh_ms;
    let live_gateway_cfg = LiveGatewayConfig::from_env();
    let runtime_cfg = LiveRuntimeConfig::from_env();
    let base_quote_usdc = runtime_cfg.quote_usdc.max(live_gateway_cfg.min_quote_usdc);
    let (dynamic_quote_usdc, capital_state, balance_sync_ok, balance_sync_error) = state
        .compute_live_quote_usdc(
            market_type,
            base_quote_usdc,
            live_gateway_cfg.min_quote_usdc,
            &capital_cfg,
        )
        .await;
    let snapshot = state
        .get_live_balance_snapshot(refresh_ms)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::internal("live balance unavailable"))?;
    let capital_scope = if capital_cfg.portfolio_shared {
        LIVE_CAPITAL_PORTFOLIO_SCOPE
    } else {
        market_type
    };
    let fixed_guard = strategy_fixed_guard_payload(&StrategyRuntimeConfig::default());
    Ok(Json(json!({
        "ok": true,
        "market_type": market_type,
        "capital_scope": capital_scope,
        "fetched_at_ms": snapshot.fetched_at_ms,
        "fetched_at_iso": DateTime::<Utc>::from_timestamp_millis(snapshot.fetched_at_ms).map(|v| v.to_rfc3339()).unwrap_or_default(),
        "source": snapshot.source,
        "balance_usdc": snapshot.balance_usdc,
        "allowance_usdc": snapshot.allowance_usdc,
        "spendable_usdc": snapshot.spendable_usdc,
        "capital_auto_enabled": capital_cfg.enabled,
        "capital_use_real_balance": capital_cfg.use_real_balance,
        "base_quote_usdc": base_quote_usdc,
        "dynamic_quote_usdc": dynamic_quote_usdc,
        "balance_sync_ok": balance_sync_ok,
        "balance_sync_error": balance_sync_error,
        "capital_state_equity_estimate_usdc": capital_state.equity_estimate_usdc,
        "capital_state_available_to_trade_usdc": capital_state.available_to_trade_usdc,
        "raw": snapshot.raw,
        "fixed_guard": fixed_guard
    })))
}

pub(super) async fn strategy_autotune_latest(
    State(state): State<ApiState>,
    Query(params): Query<StrategyAutotuneLatestQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let market_type = resolve_strategy_market_type(params.market_type.as_deref())?;
    let context = normalize_autotune_context(params.context.as_deref(), market_type);
    let (payload, key, from_legacy) = resolve_autotune_doc(&state, &context, market_type).await?;
    let (active_payload, active_key) = resolve_autotune_active_doc(&state, market_type).await?;
    let (live_payload, live_key) = resolve_autotune_live_doc(&state, market_type).await?;
    Ok(Json(json!({
        "market_type": market_type,
        "context": context,
        "key": key,
        "from_legacy": from_legacy,
        "found": payload.is_some(),
        "data": payload,
        "active_key": active_key,
        "active_found": active_payload.is_some(),
        "active_data": active_payload,
        "live_key": live_key,
        "live_found": live_payload.is_some(),
        "live_data": live_payload,
    })))
}

pub(super) async fn strategy_autotune_history(
    State(state): State<ApiState>,
    Query(params): Query<StrategyAutotuneHistoryQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let market_type = resolve_strategy_market_type(params.market_type.as_deref())?;
    let context = normalize_autotune_context(params.context.as_deref(), market_type);
    let limit = params.limit.unwrap_or(20).clamp(1, 200) as usize;
    let active_key = strategy_autotune_active_history_key(&state.redis_prefix, market_type);
    let mut items = read_key_value(&state, &active_key)
        .await?
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default();
    let candidate_key = strategy_autotune_history_key(&state.redis_prefix, &context);
    let (key_used, source_used) = if items.is_empty() {
        items = read_key_value(&state, &candidate_key)
            .await?
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default();
        (candidate_key, "candidate_context")
    } else {
        (active_key, "active_promotions")
    };
    if items.len() > limit {
        items.truncate(limit);
    }
    Ok(Json(json!({
        "market_type": market_type,
        "context": context,
        "key": key_used,
        "source": source_used,
        "from_legacy": false,
        "limit": limit,
        "count": items.len(),
        "items": items,
    })))
}

pub(super) async fn strategy_autotune_set(
    State(state): State<ApiState>,
    Json(body): Json<StrategyAutotuneSetBody>,
) -> Result<Json<Value>, ApiError> {
    let market_type = resolve_strategy_market_type(body.market_type.as_deref())?;
    let context = normalize_autotune_context(body.context.as_deref(), market_type);
    let key = strategy_autotune_key(&state.redis_prefix, &context);
    let history_key = strategy_autotune_history_key(&state.redis_prefix, &context);
    let (previous, previous_key, previous_from_legacy) =
        resolve_autotune_doc(&state, &context, market_type).await?;

    let wrapped = json!({ "config": body.config });
    let cfg = strategy_cfg_from_payload(StrategyRuntimeConfig::default(), &wrapped);
    let saved_doc = json!({
        "saved_at_ms": Utc::now().timestamp_millis(),
        "market_type": market_type,
        "context": context.clone(),
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
        "context": context,
        "key": key,
        "history_key": history_key,
        "previous_found": previous.is_some(),
        "previous_key": previous_key,
        "previous_from_legacy": previous_from_legacy,
        "saved": saved_doc,
    })))
}

pub(super) fn build_strategy_arms(
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

pub(super) fn group_round_samples(samples: &[StrategySample]) -> Vec<Vec<StrategySample>> {
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

pub(super) async fn strategy_full(
    State(state): State<ApiState>,
    Query(params): Query<StrategyFullQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let market_type = resolve_strategy_market_type(params.market_type.as_deref())?;
    let full_history = params.full_history.unwrap_or(true);
    let lookback_minutes = params
        .lookback_minutes
        .unwrap_or(if full_history { 30 * 24 * 60 } else { 12 * 60 })
        .clamp(30, 365 * 24 * 60);
    let max_points = params
        .max_points
        .unwrap_or(strategy_default_max_points(full_history))
        .clamp(20_000, strategy_max_points_hard_cap());
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

pub(super) fn clamp_runtime_cfg(cfg: &mut StrategyRuntimeConfig) {
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

pub(super) fn lcg_next(seed: &mut u64) -> f64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    let x = (*seed >> 33) as u32;
    (x as f64) / (u32::MAX as f64)
}

pub(super) fn mutate_cfg(
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

pub(super) fn score_with_rolling(
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
        run.win_rate_pct * 0.3 + run.avg_pnl_cents * 0.2
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

pub(super) async fn strategy_optimize(
    State(state): State<ApiState>,
    Query(params): Query<StrategyOptimizeQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let market_type = resolve_strategy_market_type(params.market_type.as_deref())?;
    let autotune_context =
        normalize_autotune_context(params.autotune_context.as_deref(), market_type);
    let full_history = params.full_history.unwrap_or(true);
    let lookback_minutes = params
        .lookback_minutes
        .unwrap_or(if full_history { 30 * 24 * 60 } else { 12 * 60 })
        .clamp(30, 365 * 24 * 60);
    let max_points = params
        .max_points
        .unwrap_or(strategy_default_max_points(full_history))
        .clamp(20_000, strategy_max_points_hard_cap());
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
    let recent_from_ms = latest_ts_ms.saturating_sub(i64::from(recent_lookback_minutes) * 60_000);
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

    let evaluate_candidate = |name: String, cfg: &StrategyRuntimeConfig| -> (Value, f64, bool) {
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
        let consistency_penalty = wr_gap * 1.1 + pnl_gap * 1.6 + pf_gap * 8.0 + recent_wr_gap * 1.8;
        let validation_fail_penalty = if valid_run.avg_pnl_cents <= 0.0
            || valid_run.total_pnl_cents <= 0.0
        {
            300.0 + valid_run.avg_pnl_cents.abs() * 260.0 + valid_run.total_pnl_cents.abs() * 0.03
        } else {
            0.0
        };
        let train_fail_penalty = if train_run.avg_pnl_cents <= 0.0
            || train_run.total_pnl_cents <= 0.0
            || pf_train < 0.90
        {
            80.0 + train_run.avg_pnl_cents.abs() * 80.0
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
        let recent_trade_shortfall = window_trades.saturating_sub(recent_run.trade_count) as f64;
        let coverage_gate_penalty = train_trade_shortfall * 800.0
            + valid_trade_shortfall * 900.0
            + recent_trade_shortfall * 1100.0;

        let objective =
            train_obj * 0.14 + valid_obj * valid_weight + recent_obj * recent_weight * 1.15
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

    let persist_requested = params.persist_best.unwrap_or(true);
    let persist_ttl_sec = params.persist_ttl_sec.filter(|v| *v > 0);
    let persist_key = strategy_autotune_key(&state.redis_prefix, &autotune_context);
    let active_key = strategy_autotune_active_key(&state.redis_prefix, market_type);
    let live_key = strategy_autotune_live_key(&state.redis_prefix, market_type);
    let active_history_key = strategy_autotune_active_history_key(&state.redis_prefix, market_type);
    let mut persist_error: Option<String> = None;
    let mut persisted = false;
    let (active_before_opt, _) = resolve_autotune_active_doc(&state, market_type).await?;
    let incumbent_payload = if let Some(active_before) = active_before_opt.as_ref() {
        if let Some(best) = active_before.get("best") {
            best.clone()
        } else {
            let cfg = strategy_cfg_from_payload(base_cfg, active_before);
            let (payload, _, _) = evaluate_candidate("incumbent_active".to_string(), &cfg);
            payload
        }
    } else {
        let (payload, _, _) = evaluate_candidate("incumbent_default".to_string(), &base_cfg);
        payload
    };
    let promote_min_trades = params.promote_min_trades.unwrap_or(40.0);
    let promote_min_win_rate = params.promote_min_win_rate.unwrap_or(70.0);
    let promote_min_pnl = params.promote_min_pnl.unwrap_or(0.0);

    let (should_promote, promotion_reason) = should_promote_candidate(
        &best_payload,
        &incumbent_payload,
        active_before_opt.is_some(),
        promote_min_trades,
        promote_min_win_rate,
        promote_min_pnl,
    );
    let mut promoted = false;
    let mut promotion_error: Option<String> = None;
    let mut active_saved_doc = Value::Null;

    if persist_requested {
        if best_payload.is_null() {
            persist_error = Some("best payload is null".to_string());
        } else {
            let save_doc = json!({
                "saved_at_ms": Utc::now().timestamp_millis(),
                "market_type": market_type,
                "context": autotune_context.clone(),
                "target_win_rate_pct": target_win_rate,
                "window_trades": window_trades,
                "recent_lookback_minutes": recent_lookback_minutes,
                "recent_weight": recent_weight,
                "config": best_payload.get("config").cloned().unwrap_or(Value::Null),
                "best": best_payload.clone(),
                "leaderboard_top": leaderboard.iter().take(5).cloned().collect::<Vec<Value>>(),
            });
            if let Err(e) = write_key_value(&state, &persist_key, &save_doc, persist_ttl_sec).await
            {
                persist_error = Some(e.message);
            } else {
                persisted = true;
                if should_promote {
                    let promote_doc = json!({
                        "saved_at_ms": Utc::now().timestamp_millis(),
                        "market_type": market_type,
                        "context": autotune_context.clone(),
                        "source": "autotune_pipeline",
                        "note": format!("promoted:{promotion_reason}"),
                        "target_win_rate_pct": target_win_rate,
                        "window_trades": window_trades,
                        "config": best_payload.get("config").cloned().unwrap_or(Value::Null),
                        "best": best_payload.clone(),
                        "incumbent": incumbent_payload.clone(),
                    });
                    if let Err(e) =
                        write_key_value(&state, &active_key, &promote_doc, persist_ttl_sec).await
                    {
                        promotion_error = Some(e.message);
                    } else if let Err(e) =
                        write_key_value(&state, &live_key, &promote_doc, persist_ttl_sec).await
                    {
                        if let Some(active_before) = active_before_opt.as_ref() {
                            let _ = write_key_value(
                                &state,
                                &active_key,
                                active_before,
                                persist_ttl_sec,
                            )
                            .await;
                        } else {
                            let _ = delete_key(&state, &active_key).await;
                        }
                        promotion_error = Some(format!("live_pinned_write_failed: {}", e.message));
                    } else {
                        promoted = true;
                        active_saved_doc = promote_doc.clone();
                        let mut history: Vec<Value> = read_key_value(&state, &active_history_key)
                            .await?
                            .and_then(|v| v.as_array().cloned())
                            .unwrap_or_default();
                        history.insert(0, promote_doc);
                        if history.len() > 40 {
                            history.truncate(40);
                        }
                        let _ = write_key_value(
                            &state,
                            &active_history_key,
                            &Value::Array(history),
                            None,
                        )
                        .await;
                    }
                }
            }
        }
    }

    Ok(Json(json!({
        "market_type": market_type,
        "autotune_context": autotune_context,
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
        "promotion": {
            "checked": true,
            "should_promote": should_promote,
            "promoted": promoted,
            "reason": promotion_reason,
            "error": promotion_error,
            "active_key": active_key,
            "live_key": live_key,
            "active_history_key": active_history_key,
            "active_before_found": active_before_opt.is_some(),
            "active_saved": active_saved_doc,
            "candidate_metrics": payload_metrics(&best_payload),
            "incumbent_metrics": payload_metrics(&incumbent_payload),
        },
        "best": best_payload,
        "incumbent": incumbent_payload,
        "leaderboard": leaderboard,
    })))
}

pub(super) fn normalize_autotune_context(raw: Option<&str>, market_type: &str) -> String {
    let default_ctx = format!("btcusdt:{market_type}");
    let Some(v) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return default_ctx;
    };
    let cleaned: String = v
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, ':' | '_' | '-' | '.'))
        .collect();
    if cleaned.is_empty() {
        default_ctx
    } else {
        cleaned.to_ascii_lowercase()
    }
}

pub(super) fn strategy_autotune_key(redis_prefix: &str, context: &str) -> String {
    format!("{redis_prefix}:strategy:autotune:{context}")
}

pub(super) fn strategy_autotune_history_key(redis_prefix: &str, context: &str) -> String {
    format!("{redis_prefix}:strategy:autotune:{context}:history")
}

pub(super) fn strategy_autotune_active_key(redis_prefix: &str, market_type: &str) -> String {
    format!("{redis_prefix}:strategy:autotune:active:{market_type}")
}

pub(super) fn strategy_autotune_live_key(redis_prefix: &str, market_type: &str) -> String {
    format!("{redis_prefix}:strategy:autotune:live:{market_type}")
}

pub(super) fn strategy_autotune_active_history_key(
    redis_prefix: &str,
    market_type: &str,
) -> String {
    format!("{redis_prefix}:strategy:autotune:active:{market_type}:history")
}

pub(super) async fn resolve_autotune_doc(
    state: &ApiState,
    context: &str,
    _market_type: &str,
) -> Result<(Option<Value>, String, bool), ApiError> {
    let key = strategy_autotune_key(&state.redis_prefix, context);
    if let Some(payload) = read_key_value(state, &key).await? {
        return Ok((Some(payload), key, false));
    }
    Ok((None, key, false))
}

pub(super) async fn resolve_autotune_active_doc(
    state: &ApiState,
    market_type: &str,
) -> Result<(Option<Value>, String), ApiError> {
    let key = strategy_autotune_active_key(&state.redis_prefix, market_type);
    let payload = read_key_value(state, &key).await?;
    Ok((payload, key))
}

pub(super) async fn resolve_autotune_live_doc(
    state: &ApiState,
    market_type: &str,
) -> Result<(Option<Value>, String), ApiError> {
    let key = strategy_autotune_live_key(&state.redis_prefix, market_type);
    let payload = read_key_value(state, &key).await?;
    Ok((payload, key))
}

#[derive(Debug, Clone, Copy, Serialize)]
pub(super) struct ObjectiveMetrics {
    objective: f64,
    win_rate_pct: f64,
    avg_pnl_cents: f64,
    max_drawdown_cents: f64,
    trade_count: f64,
}

pub(super) fn payload_metrics(payload: &Value) -> ObjectiveMetrics {
    let summary = payload
        .get("summary_validation")
        .or_else(|| payload.get("summary_recent"))
        .or_else(|| payload.get("summary"))
        .unwrap_or(&Value::Null);
    ObjectiveMetrics {
        objective: payload
            .get("objective")
            .and_then(Value::as_f64)
            .unwrap_or(-1_000_000_000.0),
        win_rate_pct: summary
            .get("win_rate_pct")
            .and_then(Value::as_f64)
            .unwrap_or(0.0),
        avg_pnl_cents: summary
            .get("avg_pnl_cents")
            .and_then(Value::as_f64)
            .unwrap_or(0.0),
        max_drawdown_cents: summary
            .get("max_drawdown_cents")
            .and_then(Value::as_f64)
            .unwrap_or(0.0),
        trade_count: summary
            .get("trade_count")
            .and_then(Value::as_f64)
            .unwrap_or(0.0),
    }
}

pub(super) fn should_promote_candidate(
    candidate: &Value,
    incumbent: &Value,
    has_incumbent: bool,
    min_trades: f64,
    min_win_rate: f64,
    min_pnl: f64,
) -> (bool, String) {
    let c = payload_metrics(candidate);
    if !has_incumbent {
        if c.trade_count < min_trades {
            return (false, "candidate_trade_count_too_low".to_string());
        }
        if c.avg_pnl_cents <= min_pnl {
            return (false, "candidate_avg_pnl_non_positive".to_string());
        }
        if c.win_rate_pct < min_win_rate {
            return (
                false,
                "candidate_win_rate_below_bootstrap_floor".to_string(),
            );
        }
        return (true, "bootstrap_promote".to_string());
    }
    let i = payload_metrics(incumbent);
    // Incumbent comparison is slightly more relaxed on raw minimums but MUST strictly beat incumbent
    if c.trade_count < (min_trades * 0.85).max(10.0) {
        return (false, "candidate_trade_count_too_low".to_string());
    }
    if c.avg_pnl_cents <= min_pnl {
        return (false, "candidate_avg_pnl_non_positive".to_string());
    }
    if c.trade_count + 1.0 < i.trade_count {
        return (false, "trade_count_regression".to_string());
    }
    if c.win_rate_pct + 1e-9 < i.win_rate_pct {
        return (false, "win_rate_regression".to_string());
    }
    if c.avg_pnl_cents + 1e-9 < i.avg_pnl_cents {
        return (false, "avg_pnl_regression".to_string());
    }
    if c.max_drawdown_cents > i.max_drawdown_cents + 1e-9 {
        return (false, "drawdown_regression".to_string());
    }
    if c.objective + 1e-9 < i.objective {
        return (false, "objective_regression".to_string());
    }
    let strictly_better = (c.win_rate_pct > i.win_rate_pct + 1e-9)
        || (c.avg_pnl_cents > i.avg_pnl_cents + 1e-9)
        || (c.max_drawdown_cents + 1e-9 < i.max_drawdown_cents)
        || (c.objective > i.objective + 1e-9);
    if !strictly_better {
        return (false, "not_strictly_better".to_string());
    }
    (true, "promote_strictly_better_candidate".to_string())
}
