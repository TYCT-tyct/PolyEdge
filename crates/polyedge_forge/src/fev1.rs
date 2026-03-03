use std::collections::{HashMap, VecDeque};

use serde_json::{json, Value};

const OFFICIAL_TAKER_FEE_MAX_RATE: f64 = 0.25;
const OFFICIAL_TAKER_FEE_EXPONENT: f64 = 2.0;
const MAX_BLOCKED_EXIT_STREAK: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Up,
    Down,
}

impl Side {
    fn as_str(self) -> &'static str {
        match self {
            Side::Up => "UP",
            Side::Down => "DOWN",
        }
    }

    fn dir(self) -> f64 {
        match self {
            Side::Up => 1.0,
            Side::Down => -1.0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Sample {
    pub ts_ms: i64,
    pub round_id: String,
    pub remaining_ms: i64,
    pub p_up: f64,
    pub delta_pct: f64,
    pub velocity: f64,
    pub acceleration: f64,
    pub bid_yes: f64,
    pub ask_yes: f64,
    pub bid_no: f64,
    pub ask_no: f64,
    pub spread_up: f64,
    pub spread_down: f64,
    pub spread_mid: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct RuntimeConfig {
    pub entry_threshold_base: f64,
    pub entry_threshold_cap: f64,
    pub spread_limit_prob: f64,
    pub entry_edge_prob: f64,
    pub entry_min_potential_cents: f64,
    pub entry_max_price_cents: f64,
    pub min_hold_ms: i64,
    pub stop_loss_cents: f64,
    pub reverse_signal_threshold: f64,
    pub reverse_signal_ticks: usize,
    pub trail_activate_profit_cents: f64,
    pub trail_drawdown_cents: f64,
    pub take_profit_near_max_cents: f64,
    pub endgame_take_profit_cents: f64,
    pub endgame_remaining_ms: i64,
    pub liquidity_widen_prob: f64,
    pub cooldown_ms: i64,
    pub max_entries_per_round: usize,
    pub max_exec_spread_cents: f64,
    pub slippage_cents_per_side: f64,
    pub fee_cents_per_side: f64,
    pub emergency_wide_spread_penalty_ratio: f64,
    pub stop_loss_grace_ticks: usize,
    pub stop_loss_hard_mult: f64,
    pub stop_loss_reverse_extra_ticks: usize,
    pub loss_cluster_limit: usize,
    pub loss_cluster_cooldown_ms: i64,
    pub noise_gate_enabled: bool,
    pub noise_gate_threshold_add: f64,
    pub noise_gate_edge_add: f64,
    pub noise_gate_spread_scale: f64,
    pub vic_enabled: bool,
    pub vic_target_entries_per_hour: f64,
    pub vic_deadband_ratio: f64,
    pub vic_threshold_relax_max: f64,
    pub vic_edge_relax_max: f64,
    pub vic_spread_relax_max: f64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct FeeModelContext {
    pub up_taker_fee_bps: Option<u32>,
    pub down_taker_fee_bps: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct SimulationResult {
    pub current: Value,
    pub trades: Vec<Value>,
    pub signal_decisions: Vec<Value>,
    pub trade_count: usize,
    pub win_rate_pct: f64,
    pub avg_pnl_cents: f64,
    pub avg_duration_s: f64,
    pub total_pnl_cents: f64,
    pub max_drawdown_cents: f64,
    pub max_profit_trade_cents: f64,
    pub blocked_exits: usize,
    pub emergency_wide_exit_count: usize,
    pub execution_penalty_cents_total: f64,
    pub gross_pnl_cents: f64,
    pub net_pnl_cents: f64,
    pub total_entry_fee_cents: f64,
    pub total_exit_fee_cents: f64,
    pub total_slippage_cents: f64,
    pub total_impact_cents: f64,
    pub total_cost_cents: f64,
    pub net_margin_pct: f64,
}

fn decision_id(action: &str, side: Side, round_id: &str, ts_ms: i64) -> String {
    format!(
        "fev1:{}:{}:{}:{}",
        action.to_ascii_lowercase(),
        side.as_str().to_ascii_uppercase(),
        round_id,
        ts_ms
    )
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionTarget {
    Paper,
    Live,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderAction {
    Enter,
    Exit,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct OrderIntent {
    pub ts_ms: i64,
    pub round_id: String,
    pub side: Side,
    pub action: OrderAction,
    pub price_cents: f64,
    pub quote_size_usdc: f64,
    pub reason: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct OrderResult {
    pub accepted: bool,
    pub request_id: String,
    pub reason: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub struct ExecutionGate {
    pub live_enabled: bool,
}

#[allow(dead_code)]
impl ExecutionGate {
    pub fn from_env() -> Self {
        let live_enabled = std::env::var("FORGE_FEV1_LIVE_ENABLED")
            .ok()
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);
        Self { live_enabled }
    }
}

#[allow(dead_code)]
pub trait LiveOrderGateway {
    fn submit(&self, intent: &OrderIntent) -> OrderResult;
}

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct DisabledLiveGateway;

impl LiveOrderGateway for DisabledLiveGateway {
    fn submit(&self, _intent: &OrderIntent) -> OrderResult {
        OrderResult {
            accepted: false,
            request_id: "live-disabled".to_string(),
            reason: "FORGE_FEV1_LIVE_ENABLED=false".to_string(),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct ArmedSimulatedGateway;

impl LiveOrderGateway for ArmedSimulatedGateway {
    fn submit(&self, intent: &OrderIntent) -> OrderResult {
        OrderResult {
            accepted: true,
            request_id: format!("sim-{}-{}", intent.action as u8, intent.ts_ms),
            reason: "accepted".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
struct Position {
    side: Side,
    entry_ts_ms: i64,
    entry_round_id: String,
    entry_price_raw_cents: f64,
    entry_price_cents: f64,
    entry_fee_cents: f64,
    entry_slippage_cents: f64,
    entry_impact_cents: f64,
    entry_score: f64,
    entry_remaining_ms: i64,
    peak_pnl_cents: f64,
    reverse_streak: usize,
    blocked_exit_streak: usize,
    soft_stop_pending_ticks: usize,
}

#[derive(Debug, Clone, Copy)]
struct Signal {
    score: f64,
    entry_threshold: f64,
    confirmed: bool,
    p_fair_up: f64,
    edge_prob: f64,
}

#[derive(Debug, Clone, Copy)]
struct EntryAdjustments {
    threshold_add: f64,
    edge_add: f64,
    spread_scale: f64,
}

fn tanh_norm(v: f64, scale: f64) -> f64 {
    if !v.is_finite() || !scale.is_finite() || scale <= 0.0 {
        return 0.0;
    }
    (v / scale).tanh()
}

fn milli_cents(v: f64) -> i64 {
    if !v.is_finite() {
        return 0;
    }
    (v * 1_000.0).round() as i64
}

fn dynamic_taker_fee_cents(price_cents: f64) -> f64 {
    if !price_cents.is_finite() || price_cents <= 0.0 {
        return 0.0;
    }
    let p = (price_cents / 100.0).clamp(0.0001, 0.9999);
    let fee_rate = OFFICIAL_TAKER_FEE_MAX_RATE * (p * (1.0 - p)).powf(OFFICIAL_TAKER_FEE_EXPONENT);
    let fee_usdc = (price_cents / 100.0) * fee_rate;
    let fee_usdc_rounded_4dp = (fee_usdc * 10_000.0).round() / 10_000.0;
    (fee_usdc_rounded_4dp * 100.0).clamp(0.0, 12.0)
}

fn fee_cents_from_bps(price_cents: f64, fee_bps: u32) -> f64 {
    if !price_cents.is_finite() || price_cents <= 0.0 {
        return 0.0;
    }
    let rate = (fee_bps as f64 / 10_000.0).clamp(0.0, 0.99);
    let fee_usdc = (price_cents / 100.0) * rate;
    let fee_usdc_rounded_4dp = (fee_usdc * 10_000.0).round() / 10_000.0;
    (fee_usdc_rounded_4dp * 100.0).clamp(0.0, 12.0)
}

fn taker_fee_cents(price_cents: f64, side: Side, fee_ctx: Option<&FeeModelContext>) -> f64 {
    let fee_bps = fee_ctx.and_then(|ctx| match side {
        Side::Up => ctx.up_taker_fee_bps,
        Side::Down => ctx.down_taker_fee_bps,
    });
    if let Some(bps) = fee_bps {
        fee_cents_from_bps(price_cents, bps)
    } else {
        dynamic_taker_fee_cents(price_cents)
    }
}

fn side_bid(sample: &Sample, side: Side) -> f64 {
    match side {
        Side::Up => sample.bid_yes,
        Side::Down => sample.bid_no,
    }
}

fn side_ask(sample: &Sample, side: Side) -> f64 {
    match side {
        Side::Up => sample.ask_yes,
        Side::Down => sample.ask_no,
    }
}

fn execution_impact_cents(
    sample: &Sample,
    side: Side,
    cfg: &RuntimeConfig,
    remaining_ms: i64,
    emergency: bool,
) -> f64 {
    let spread_cents = side_spread(sample, side) * 100.0;
    let spread_pressure = (spread_cents / cfg.max_exec_spread_cents.max(0.5)).clamp(0.0, 4.0);
    let velocity_pressure = tanh_norm(sample.velocity.abs(), 4.0).abs();
    let accel_pressure = tanh_norm(sample.acceleration.abs(), 16.0).abs();
    let endgame_ms = cfg.endgame_remaining_ms.max(1) as f64;
    let remaining_ms = remaining_ms.max(0) as f64;
    let time_pressure = ((2.0 * endgame_ms - remaining_ms) / (2.0 * endgame_ms)).clamp(0.0, 1.0);
    let mut impact = cfg.slippage_cents_per_side * 0.35
        + spread_cents * (0.16 + 0.14 * spread_pressure)
        + velocity_pressure * 0.55
        + accel_pressure * 0.40
        + time_pressure * 0.50;
    if emergency {
        impact *= 1.35;
    }
    impact.clamp(0.0, 18.0)
}

fn side_spread(sample: &Sample, side: Side) -> f64 {
    match side {
        Side::Up => sample.spread_up,
        Side::Down => sample.spread_down,
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
    score_hist
        .iter()
        .rev()
        .take(2)
        .all(|v| (*v > 0.0 && sgn > 0) || (*v < 0.0 && sgn < 0))
}

fn hour_cn_from_ts_ms(ts_ms: i64) -> u8 {
    ((ts_ms.div_euclid(3_600_000) + 8).rem_euclid(24)) as u8
}

fn is_noise_hour_cn(hour: u8) -> bool {
    matches!(hour, 5 | 10 | 13 | 18 | 20)
}

fn entry_adjustments(
    ts_ms: i64,
    cfg: &RuntimeConfig,
    session_start_ts_ms: i64,
    total_entries: usize,
) -> EntryAdjustments {
    let mut out = EntryAdjustments {
        threshold_add: 0.0,
        edge_add: 0.0,
        spread_scale: 1.0,
    };
    if cfg.noise_gate_enabled && is_noise_hour_cn(hour_cn_from_ts_ms(ts_ms)) {
        out.threshold_add += cfg.noise_gate_threshold_add.max(0.0);
        out.edge_add += cfg.noise_gate_edge_add.max(0.0);
        out.spread_scale *= cfg.noise_gate_spread_scale.clamp(0.5, 1.2);
    }
    if cfg.vic_enabled
        && cfg.vic_target_entries_per_hour > 0.0
        && cfg.vic_deadband_ratio < 1.0
        && ts_ms > session_start_ts_ms
    {
        let elapsed_hours = (ts_ms - session_start_ts_ms) as f64 / 3_600_000.0;
        let target_entries = (cfg.vic_target_entries_per_hour * elapsed_hours).max(1.0);
        let actual_entries = total_entries as f64;
        let deficit_ratio = ((target_entries - actual_entries) / target_entries).clamp(0.0, 1.0);
        let deadband = cfg.vic_deadband_ratio.clamp(0.0, 0.8);
        if deficit_ratio > deadband {
            let norm = ((deficit_ratio - deadband) / (1.0 - deadband)).clamp(0.0, 1.0);
            out.threshold_add -= cfg.vic_threshold_relax_max.max(0.0) * norm;
            out.edge_add -= cfg.vic_edge_relax_max.max(0.0) * norm;
            out.spread_scale *= 1.0 + cfg.vic_spread_relax_max.max(0.0) * norm;
        }
    }
    out
}

fn compute_signal(
    sample: &Sample,
    p_hist: &VecDeque<f64>,
    score_hist: &VecDeque<f64>,
    cfg: &RuntimeConfig,
) -> Signal {
    let local_vol = if p_hist.len() >= 2 {
        let mut sum = 0.0;
        let mut cnt = 0usize;
        let mut prev: Option<f64> = None;
        for p in p_hist {
            if let Some(v) = prev {
                sum += (p - v).abs();
                cnt += 1;
            }
            prev = Some(*p);
        }
        if cnt > 0 {
            sum / cnt as f64
        } else {
            0.0
        }
    } else {
        0.0
    };

    let trend_component = 0.55 * tanh_norm(sample.delta_pct, 0.09)
        + 0.30 * tanh_norm(sample.velocity, 4.5)
        + 0.15 * tanh_norm(sample.acceleration, 16.0);
    let p_fair_up = (0.5 + 0.45 * trend_component).clamp(0.005, 0.995);
    let edge_prob = p_fair_up - sample.p_up.clamp(0.0, 1.0);
    let edge_component = tanh_norm(edge_prob, cfg.entry_edge_prob.max(0.008));
    let spread_penalty = (sample.spread_mid / cfg.spread_limit_prob.max(0.008)).clamp(0.0, 3.0);
    let score =
        (0.72 * edge_component + 0.28 * trend_component - 0.08 * spread_penalty).clamp(-1.0, 1.0);
    let entry_threshold = (cfg.entry_threshold_base + local_vol * 1.2 + sample.spread_mid * 0.6)
        .clamp(cfg.entry_threshold_base, cfg.entry_threshold_cap);
    let confirmed = confirm_direction(score_hist, score);

    Signal {
        score,
        entry_threshold,
        confirmed,
        p_fair_up,
        edge_prob,
    }
}

fn max_drawdown_from_pnls(pnls: &[f64]) -> f64 {
    let mut equity = 0.0;
    let mut peak = 0.0;
    let mut max_dd = 0.0;
    for p in pnls {
        equity += *p;
        if equity > peak {
            peak = equity;
        }
        let dd = peak - equity;
        if dd > max_dd {
            max_dd = dd;
        }
    }
    max_dd.max(0.0)
}

#[allow(dead_code)]
pub fn simulate(samples: &[Sample], cfg: &RuntimeConfig, max_trades: usize) -> SimulationResult {
    simulate_with_fee_context(samples, cfg, max_trades, None)
}

pub fn simulate_with_fee_context(
    samples: &[Sample],
    cfg: &RuntimeConfig,
    max_trades: usize,
    fee_ctx: Option<&FeeModelContext>,
) -> SimulationResult {
    let mut p_hist = VecDeque::<f64>::with_capacity(24);
    let mut score_hist = VecDeque::<f64>::with_capacity(24);
    let mut position: Option<Position> = None;
    let mut trades = Vec::<Value>::new();
    let mut signal_decisions = Vec::<Value>::new();
    let mut entries_by_round: HashMap<String, usize> = HashMap::new();
    let mut last_exit_ts_ms = 0_i64;
    let mut blocked_exits = 0usize;
    let mut emergency_wide_exit_count = 0usize;
    let mut execution_penalty_cents_total = 0.0;

    let mut trade_id = 1_i64;
    let mut last_score = 0.0;
    let mut last_entry_threshold = cfg.entry_threshold_base;
    let mut last_confirmed = false;
    let mut last_side = "WAIT";
    let mut last_p_fair_up = 0.5;
    let mut last_edge_prob = 0.0;
    let mut last_ts_ms = 0_i64;
    let mut last_round_id = String::new();
    let mut last_remaining_s = 0.0_f64;
    let mut last_p_up = 0.5_f64;
    let mut last_delta_pct = 0.0_f64;
    let mut last_spread_up_cents = 0.0_f64;
    let mut last_spread_down_cents = 0.0_f64;
    let mut total_entries = 0usize;
    let mut loss_cluster_streak = 0usize;
    let mut loss_cluster_cooldown_until_ts_ms = 0_i64;
    let session_start_ts_ms = samples.first().map(|v| v.ts_ms).unwrap_or(0);

    for sample in samples {
        last_ts_ms = sample.ts_ms;
        last_round_id = sample.round_id.clone();
        last_remaining_s = (sample.remaining_ms.max(0) as f64) / 1000.0;
        last_p_up = sample.p_up.clamp(0.0, 1.0);
        last_delta_pct = sample.delta_pct;
        last_spread_up_cents = sample.spread_up * 100.0;
        last_spread_down_cents = sample.spread_down * 100.0;

        let signal = compute_signal(sample, &p_hist, &score_hist, cfg);
        let score = signal.score;
        let side = if score >= 0.0 { Side::Up } else { Side::Down };
        last_score = score;
        last_entry_threshold = signal.entry_threshold;
        last_confirmed = signal.confirmed;
        last_p_fair_up = signal.p_fair_up;
        last_edge_prob = signal.edge_prob;
        last_side = side.as_str();

        if let Some(pos) = position.as_mut() {
            let bid_raw = side_bid(sample, pos.side) * 100.0;
            let spread_side_cents = side_spread(sample, pos.side) * 100.0;
            let exit_fee = cfg.fee_cents_per_side + taker_fee_cents(bid_raw, pos.side, fee_ctx);
            let mut exit_impact_cents =
                execution_impact_cents(sample, pos.side, cfg, sample.remaining_ms, false);
            let mut exit_exec =
                bid_raw - cfg.slippage_cents_per_side - exit_fee - exit_impact_cents;
            let pnl = exit_exec - pos.entry_price_cents;
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
            let mut exit_reason: Option<&str> = None;
            if sample.round_id != pos.entry_round_id {
                exit_reason = Some("round_rollover");
            } else if bid_raw >= cfg.take_profit_near_max_cents {
                exit_reason = Some("near_max_take_profit");
            } else if sample.remaining_ms <= cfg.endgame_remaining_ms
                && bid_raw >= cfg.endgame_take_profit_cents
            {
                exit_reason = Some("endgame_take_profit");
            } else if can_exit_now && milli_cents(pnl) <= -milli_cents(cfg.stop_loss_cents) {
                let can_fill_now = spread_side_cents <= cfg.max_exec_spread_cents;
                if can_fill_now || cfg.stop_loss_grace_ticks == 0 {
                    pos.soft_stop_pending_ticks = 0;
                    exit_reason = Some("stop_loss");
                } else {
                    pos.soft_stop_pending_ticks = pos.soft_stop_pending_ticks.saturating_add(1);
                    let hard_stop = -cfg.stop_loss_cents * cfg.stop_loss_hard_mult.max(1.0);
                    let reverse_trigger = pos.reverse_streak
                        >= cfg
                            .reverse_signal_ticks
                            .saturating_add(cfg.stop_loss_reverse_extra_ticks);
                    let should_force = milli_cents(pnl) <= milli_cents(hard_stop)
                        || reverse_trigger
                        || sample.remaining_ms <= cfg.endgame_remaining_ms
                        || pos.soft_stop_pending_ticks >= cfg.stop_loss_grace_ticks;
                    if should_force {
                        exit_reason = Some("stop_loss_wide_grace");
                    }
                }
            } else if can_exit_now && pos.reverse_streak >= cfg.reverse_signal_ticks {
                pos.soft_stop_pending_ticks = 0;
                exit_reason = Some("signal_reverse");
            } else if can_exit_now
                && pos.peak_pnl_cents >= cfg.trail_activate_profit_cents
                && drawdown >= cfg.trail_drawdown_cents
            {
                pos.soft_stop_pending_ticks = 0;
                exit_reason = Some("trail_drawdown");
            } else if can_exit_now && side_spread(sample, pos.side) >= cfg.liquidity_widen_prob {
                pos.soft_stop_pending_ticks = 0;
                exit_reason = Some("liquidity_widen");
            } else {
                pos.soft_stop_pending_ticks = 0;
            }
            if exit_reason.is_none() {
                pos.blocked_exit_streak = 0;
            }

            if let Some(reason) = exit_reason {
                let emergency = matches!(
                    reason,
                    "round_rollover"
                        | "stop_loss"
                        | "stop_loss_wide_grace"
                        | "endgame_take_profit"
                        | "signal_reverse"
                );
                let can_fill = spread_side_cents <= cfg.max_exec_spread_cents;
                let mut escalated_for_blocked_exit = false;
                if !can_fill && !emergency {
                    blocked_exits = blocked_exits.saturating_add(1);
                    pos.blocked_exit_streak = pos.blocked_exit_streak.saturating_add(1);
                    if pos.blocked_exit_streak < MAX_BLOCKED_EXIT_STREAK {
                        continue;
                    }
                    escalated_for_blocked_exit = true;
                }
                pos.blocked_exit_streak = 0;
                let emergency_penalty_cents = if !can_fill {
                    emergency_wide_exit_count = emergency_wide_exit_count.saturating_add(1);
                    (spread_side_cents - cfg.max_exec_spread_cents).max(0.0)
                        * cfg.emergency_wide_spread_penalty_ratio
                } else {
                    0.0
                };
                if emergency || escalated_for_blocked_exit {
                    exit_impact_cents =
                        execution_impact_cents(sample, pos.side, cfg, sample.remaining_ms, true);
                    exit_exec =
                        bid_raw - cfg.slippage_cents_per_side - exit_fee - exit_impact_cents;
                }
                exit_exec -= emergency_penalty_cents;
                execution_penalty_cents_total += emergency_penalty_cents;

                let net_pnl = exit_exec - pos.entry_price_cents;
                let gross_pnl = bid_raw - pos.entry_price_raw_cents;
                let total_cost = (gross_pnl - net_pnl).max(0.0);
                let duration_s = ((sample.ts_ms - pos.entry_ts_ms).max(0) as f64) / 1000.0;
                if net_pnl < 0.0 {
                    loss_cluster_streak = loss_cluster_streak.saturating_add(1);
                    if cfg.loss_cluster_limit > 0 && loss_cluster_streak >= cfg.loss_cluster_limit {
                        loss_cluster_cooldown_until_ts_ms = sample
                            .ts_ms
                            .saturating_add(cfg.loss_cluster_cooldown_ms.max(0));
                    }
                } else {
                    loss_cluster_streak = 0;
                }
                trades.push(json!({
                    "id": trade_id,
                    "side": pos.side.as_str(),
                    "entry_round_id": pos.entry_round_id,
                    "entry_ts_ms": pos.entry_ts_ms,
                    "exit_ts_ms": sample.ts_ms,
                    "entry_remaining_ms": pos.entry_remaining_ms,
                    "exit_remaining_ms": sample.remaining_ms,
                    "entry_price_cents": pos.entry_price_cents,
                    "entry_price_raw_cents": pos.entry_price_raw_cents,
                    "entry_fee_cents": pos.entry_fee_cents,
                    "entry_slippage_cents": pos.entry_slippage_cents,
                    "entry_impact_cents": pos.entry_impact_cents,
                    "exit_price_cents": exit_exec,
                    "exit_price_raw_cents": bid_raw,
                    "exit_slippage_cents": cfg.slippage_cents_per_side,
                    "exit_impact_cents": exit_impact_cents,
                    "exit_emergency_penalty_cents": emergency_penalty_cents,
                    "exit_fee_cents": exit_fee,
                    "pnl_gross_cents": gross_pnl,
                    "total_cost_cents": total_cost,
                    "pnl_net_cents": net_pnl,
                    "pnl_cents": net_pnl,
                    "peak_pnl_cents": pos.peak_pnl_cents,
                    "duration_s": duration_s,
                    "entry_score": pos.entry_score,
                    "exit_score": score,
                    "entry_reason": "fev1_signal_entry",
                    "exit_reason": if escalated_for_blocked_exit { "blocked_exit_escalation" } else { reason },
                    "exec_fill_ok": can_fill,
                    "exit_spread_cents": spread_side_cents,
                    "loss_cluster_streak_after_exit": loss_cluster_streak,
                }));
                let final_reason = if escalated_for_blocked_exit {
                    "blocked_exit_escalation"
                } else {
                    reason
                };
                let final_reason_lc = final_reason.to_ascii_lowercase();
                let take_profit_exit = final_reason_lc.contains("take_profit");
                let emergency_exit = matches!(
                    final_reason_lc.as_str(),
                    "stop_loss"
                        | "signal_reverse"
                        | "trail_drawdown"
                        | "liquidity_widen"
                        | "round_rollover"
                );
                let (exit_tif, exit_style, exit_ttl_ms, exit_slippage_bps) =
                    if take_profit_exit && !emergency_exit && sample.remaining_ms > 30_000 {
                        ("GTD", "maker", 1_100_i64, 10.0_f64)
                    } else if sample.remaining_ms <= 30_000 {
                        ("FAK", "taker", 900_i64, 30.0_f64)
                    } else {
                        ("FAK", "taker", 1_000_i64, 22.0_f64)
                    };
                signal_decisions.push(json!({
                    "decision_id": decision_id("exit", pos.side, &sample.round_id, sample.ts_ms),
                    "action": "exit",
                    "side": pos.side.as_str(),
                    "round_id": sample.round_id,
                    "ts_ms": sample.ts_ms,
                    "reason": final_reason,
                    "price_cents": bid_raw.clamp(1.0, 99.0),
                    "remaining_ms": sample.remaining_ms,
                    "tif": exit_tif,
                    "style": exit_style,
                    "ttl_ms": exit_ttl_ms,
                    "max_slippage_bps": exit_slippage_bps
                }));
                trade_id += 1;
                position = None;
                last_exit_ts_ms = sample.ts_ms;
            }
        } else {
            let ask_raw = side_ask(sample, side) * 100.0;
            let spread_side_cents = side_spread(sample, side) * 100.0;
            let dynamic = entry_adjustments(sample.ts_ms, cfg, session_start_ts_ms, total_entries);
            let dynamic_entry_threshold = (signal.entry_threshold + dynamic.threshold_add).clamp(
                cfg.entry_threshold_base * 0.65,
                cfg.entry_threshold_cap + 0.20,
            );
            last_entry_threshold = dynamic_entry_threshold;
            let dynamic_edge_req =
                (cfg.entry_edge_prob + dynamic.edge_add).clamp(0.001, cfg.entry_edge_prob + 0.2);
            let dynamic_spread_limit_prob =
                (cfg.spread_limit_prob * dynamic.spread_scale).clamp(0.003, 0.25);
            let dynamic_max_exec_spread =
                (cfg.max_exec_spread_cents * dynamic.spread_scale).clamp(0.5, 12.0);
            let cooldown_ok = last_exit_ts_ms <= 0
                || sample.ts_ms.saturating_sub(last_exit_ts_ms) >= cfg.cooldown_ms;
            let cluster_cooldown_ok = sample.ts_ms >= loss_cluster_cooldown_until_ts_ms;
            let round_entries = entries_by_round.get(&sample.round_id).copied().unwrap_or(0);
            let within_limit = round_entries < cfg.max_entries_per_round;
            let directional_edge = signal.edge_prob * side.dir();
            let edge_ok = directional_edge >= dynamic_edge_req;
            let score_ok = score.abs() >= dynamic_entry_threshold;
            let fair_conf = if side == Side::Up {
                signal.p_fair_up
            } else {
                1.0 - signal.p_fair_up
            };
            let confidence_ok = fair_conf >= 0.56;
            let spread_ok = sample.spread_mid <= dynamic_spread_limit_prob
                && spread_side_cents <= dynamic_max_exec_spread;
            let fee = cfg.fee_cents_per_side + taker_fee_cents(ask_raw, side, fee_ctx);
            let entry_impact_cents =
                execution_impact_cents(sample, side, cfg, sample.remaining_ms, false);
            let entry_exec = ask_raw + cfg.slippage_cents_per_side + fee + entry_impact_cents;
            let expected_potential = (99.0 - entry_exec).max(0.0);
            let potential_ok = expected_potential >= cfg.entry_min_potential_cents;
            let price_ok = entry_exec <= cfg.entry_max_price_cents;
            if signal.confirmed
                && edge_ok
                && score_ok
                && confidence_ok
                && spread_ok
                && price_ok
                && potential_ok
                && cooldown_ok
                && cluster_cooldown_ok
                && within_limit
            {
                position = Some(Position {
                    side,
                    entry_ts_ms: sample.ts_ms,
                    entry_round_id: sample.round_id.clone(),
                    entry_price_raw_cents: ask_raw,
                    entry_price_cents: entry_exec,
                    entry_fee_cents: fee,
                    entry_slippage_cents: cfg.slippage_cents_per_side,
                    entry_impact_cents,
                    entry_score: score,
                    entry_remaining_ms: sample.remaining_ms,
                    peak_pnl_cents: 0.0,
                    reverse_streak: 0,
                    blocked_exit_streak: 0,
                    soft_stop_pending_ticks: 0,
                });
                let entry_reason = "fev1_signal_entry";
                let entry_score = score.abs();
                let (entry_tif, entry_style, entry_ttl_ms, entry_slippage_bps) =
                    if sample.remaining_ms <= 120_000 || entry_score >= 0.80 {
                        ("FAK", "taker", 900_i64, 24.0_f64)
                    } else {
                        ("GTD", "maker", 900_i64, 10.0_f64)
                    };
                signal_decisions.push(json!({
                    "decision_id": decision_id("enter", side, &sample.round_id, sample.ts_ms),
                    "action": "enter",
                    "side": side.as_str(),
                    "round_id": sample.round_id,
                    "ts_ms": sample.ts_ms,
                    "reason": entry_reason,
                    "price_cents": ask_raw.clamp(1.0, 99.0),
                    "entry_score": entry_score,
                    "edge_score": entry_score,
                    "entry_remaining_ms": sample.remaining_ms,
                    "remaining_ms": sample.remaining_ms,
                    "tif": entry_tif,
                    "style": entry_style,
                    "ttl_ms": entry_ttl_ms,
                    "max_slippage_bps": entry_slippage_bps
                }));
                *entries_by_round.entry(sample.round_id.clone()).or_insert(0) += 1;
                total_entries = total_entries.saturating_add(1);
            }
        }

        if p_hist.len() >= 24 {
            p_hist.pop_front();
        }
        if score_hist.len() >= 24 {
            score_hist.pop_front();
        }
        p_hist.push_back(sample.p_up);
        score_hist.push_back(score);
    }

    if let (Some(pos), Some(last)) = (position.take(), samples.last()) {
        let bid_raw = side_bid(last, pos.side) * 100.0;
        let exit_fee = cfg.fee_cents_per_side + taker_fee_cents(bid_raw, pos.side, fee_ctx);
        let exit_impact_cents =
            execution_impact_cents(last, pos.side, cfg, last.remaining_ms, false);
        let exit_exec = bid_raw - cfg.slippage_cents_per_side - exit_fee - exit_impact_cents;
        let net_pnl = exit_exec - pos.entry_price_cents;
        let gross_pnl = bid_raw - pos.entry_price_raw_cents;
        let total_cost = (gross_pnl - net_pnl).max(0.0);
        trades.push(json!({
            "id": trade_id,
            "side": pos.side.as_str(),
            "entry_round_id": pos.entry_round_id,
            "entry_ts_ms": pos.entry_ts_ms,
            "exit_ts_ms": last.ts_ms,
            "entry_remaining_ms": pos.entry_remaining_ms,
            "exit_remaining_ms": last.remaining_ms,
            "entry_price_cents": pos.entry_price_cents,
            "entry_price_raw_cents": pos.entry_price_raw_cents,
            "entry_fee_cents": pos.entry_fee_cents,
            "entry_slippage_cents": pos.entry_slippage_cents,
            "entry_impact_cents": pos.entry_impact_cents,
            "exit_price_cents": exit_exec,
            "exit_price_raw_cents": bid_raw,
            "exit_slippage_cents": cfg.slippage_cents_per_side,
            "exit_impact_cents": exit_impact_cents,
            "exit_emergency_penalty_cents": 0.0,
            "exit_fee_cents": exit_fee,
            "pnl_gross_cents": gross_pnl,
            "total_cost_cents": total_cost,
            "pnl_net_cents": net_pnl,
            "pnl_cents": net_pnl,
            "peak_pnl_cents": pos.peak_pnl_cents.max(net_pnl),
            "duration_s": ((last.ts_ms - pos.entry_ts_ms).max(0) as f64) / 1000.0,
            "entry_score": pos.entry_score,
            "exit_score": last_score,
            "entry_reason": "fev1_signal_entry",
            "exit_reason": "end_of_samples_force_close",
            "exec_fill_ok": true,
            "exit_spread_cents": side_spread(last, pos.side) * 100.0
        }));
    }

    let trade_count = trades.len();
    let all_trade_pnls: Vec<f64> = trades
        .iter()
        .filter_map(|t| t.get("pnl_cents").and_then(Value::as_f64))
        .collect();
    let wins = all_trade_pnls.iter().filter(|v| **v > 0.0).count();
    let win_rate_pct = if trade_count > 0 {
        wins as f64 * 100.0 / trade_count as f64
    } else {
        0.0
    };
    let total_pnl_cents: f64 = all_trade_pnls.iter().sum();
    let gross_pnl_cents: f64 = trades
        .iter()
        .filter_map(|t| t.get("pnl_gross_cents").and_then(Value::as_f64))
        .sum();
    let total_entry_fee_cents: f64 = trades
        .iter()
        .filter_map(|t| t.get("entry_fee_cents").and_then(Value::as_f64))
        .sum();
    let total_exit_fee_cents: f64 = trades
        .iter()
        .filter_map(|t| t.get("exit_fee_cents").and_then(Value::as_f64))
        .sum();
    let total_slippage_cents: f64 = trades
        .iter()
        .map(|t| {
            let a = t
                .get("entry_slippage_cents")
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
            let b = t
                .get("exit_slippage_cents")
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
            a + b
        })
        .sum();
    let total_impact_cents: f64 = trades
        .iter()
        .map(|t| {
            let a = t
                .get("entry_impact_cents")
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
            let b = t
                .get("exit_impact_cents")
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
            a + b
        })
        .sum();
    let total_cost_cents = total_entry_fee_cents
        + total_exit_fee_cents
        + total_slippage_cents
        + total_impact_cents
        + execution_penalty_cents_total;
    let avg_pnl_cents = if trade_count > 0 {
        total_pnl_cents / trade_count as f64
    } else {
        0.0
    };
    let avg_duration_s = if trade_count > 0 {
        trades
            .iter()
            .filter_map(|t| t.get("duration_s").and_then(Value::as_f64))
            .sum::<f64>()
            / trade_count as f64
    } else {
        0.0
    };
    let max_drawdown_cents = max_drawdown_from_pnls(&all_trade_pnls);
    let max_profit_trade_cents = all_trade_pnls.iter().copied().fold(0.0_f64, f64::max);
    let net_margin_pct = if gross_pnl_cents.abs() > 1e-9 {
        total_pnl_cents / gross_pnl_cents.abs() * 100.0
    } else {
        0.0
    };
    let net_pnl_cents = total_pnl_cents;
    let confidence = if last_entry_threshold > 1e-9 {
        (last_score.abs() / last_entry_threshold).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let suggested_side = if last_confirmed { last_side } else { "WAIT" };

    let current = json!({
        "suggested_action": if last_confirmed && last_score.abs() >= last_entry_threshold {
            if last_side == "UP" { "ENTER_UP" } else { "ENTER_DOWN" }
        } else if trade_count > 0 {
            "HOLD"
        } else {
            "WAIT"
        },
        "suggested_side": suggested_side,
        "score": last_score,
        "entry_threshold": last_entry_threshold,
        "side": last_side,
        "confirmed": last_confirmed,
        "confidence": confidence,
        "p_fair_up": last_p_fair_up,
        "edge_prob": last_edge_prob,
        "timestamp_ms": last_ts_ms,
        "round_id": last_round_id,
        "remaining_s": last_remaining_s,
        "p_up_pct": last_p_up,
        "delta_pct": last_delta_pct,
        "spread_up_cents": last_spread_up_cents,
        "spread_down_cents": last_spread_down_cents,
        "loss_cluster_streak": loss_cluster_streak,
        "loss_cluster_cooldown_until_ts_ms": loss_cluster_cooldown_until_ts_ms,
    });

    let trades_view = if trades.len() > max_trades {
        trades[trades.len() - max_trades..].to_vec()
    } else {
        trades
    };

    SimulationResult {
        current,
        trades: trades_view,
        signal_decisions,
        trade_count,
        win_rate_pct,
        avg_pnl_cents,
        avg_duration_s,
        total_pnl_cents,
        max_drawdown_cents,
        max_profit_trade_cents,
        blocked_exits,
        emergency_wide_exit_count,
        execution_penalty_cents_total,
        gross_pnl_cents,
        net_pnl_cents,
        total_entry_fee_cents,
        total_exit_fee_cents,
        total_slippage_cents,
        total_impact_cents,
        total_cost_cents,
        net_margin_pct,
    }
}

#[cfg(test)]
mod tests {
    use super::{simulate, RuntimeConfig, Sample};

    #[test]
    fn blocked_non_emergency_exits_are_escalated() {
        let cfg = RuntimeConfig {
            entry_threshold_base: 0.40,
            entry_threshold_cap: 0.55,
            spread_limit_prob: 0.02,
            entry_edge_prob: 0.01,
            entry_min_potential_cents: 1.0,
            entry_max_price_cents: 95.0,
            min_hold_ms: 0,
            stop_loss_cents: 99.0,
            reverse_signal_threshold: -0.95,
            reverse_signal_ticks: 12,
            trail_activate_profit_cents: 99.0,
            trail_drawdown_cents: 99.0,
            take_profit_near_max_cents: 99.9,
            endgame_take_profit_cents: 99.9,
            endgame_remaining_ms: 1_000,
            liquidity_widen_prob: 0.02,
            cooldown_ms: 0,
            max_entries_per_round: 1,
            max_exec_spread_cents: 1.0,
            slippage_cents_per_side: 0.0,
            fee_cents_per_side: 0.0,
            emergency_wide_spread_penalty_ratio: 0.5,
            stop_loss_grace_ticks: 0,
            stop_loss_hard_mult: 1.4,
            stop_loss_reverse_extra_ticks: 1,
            loss_cluster_limit: 0,
            loss_cluster_cooldown_ms: 0,
            noise_gate_enabled: false,
            noise_gate_threshold_add: 0.0,
            noise_gate_edge_add: 0.0,
            noise_gate_spread_scale: 1.0,
            vic_enabled: false,
            vic_target_entries_per_hour: 0.0,
            vic_deadband_ratio: 0.08,
            vic_threshold_relax_max: 0.0,
            vic_edge_relax_max: 0.0,
            vic_spread_relax_max: 0.0,
        };
        let mut samples = vec![
            // Build confirmation history and open one UP position.
            Sample {
                ts_ms: 1_000,
                round_id: "r-1".to_string(),
                remaining_ms: 200_000,
                p_up: 0.18,
                delta_pct: 0.8,
                velocity: 6.0,
                acceleration: 2.0,
                bid_yes: 0.49,
                ask_yes: 0.50,
                bid_no: 0.50,
                ask_no: 0.51,
                spread_up: 0.005,
                spread_down: 0.005,
                spread_mid: 0.005,
            },
            Sample {
                ts_ms: 2_000,
                round_id: "r-1".to_string(),
                remaining_ms: 199_000,
                p_up: 0.18,
                delta_pct: 0.8,
                velocity: 6.0,
                acceleration: 2.0,
                bid_yes: 0.49,
                ask_yes: 0.50,
                bid_no: 0.50,
                ask_no: 0.51,
                spread_up: 0.005,
                spread_down: 0.005,
                spread_mid: 0.005,
            },
            Sample {
                ts_ms: 3_000,
                round_id: "r-1".to_string(),
                remaining_ms: 198_000,
                p_up: 0.18,
                delta_pct: 0.8,
                velocity: 6.0,
                acceleration: 2.0,
                bid_yes: 0.50,
                ask_yes: 0.51,
                bid_no: 0.49,
                ask_no: 0.50,
                spread_up: 0.005,
                spread_down: 0.005,
                spread_mid: 0.005,
            },
        ];
        for ts in [4_000_i64, 5_000_i64, 6_000_i64] {
            samples.push(Sample {
                ts_ms: ts,
                round_id: "r-1".to_string(),
                remaining_ms: 197_000 - (ts - 4_000),
                p_up: 0.35,
                delta_pct: 0.4,
                velocity: 2.0,
                acceleration: 0.5,
                bid_yes: 0.53,
                ask_yes: 0.56,
                bid_no: 0.44,
                ask_no: 0.47,
                spread_up: 0.03,
                spread_down: 0.03,
                spread_mid: 0.03,
            });
        }

        let run = simulate(&samples, &cfg, 16);
        assert!(
            run.blocked_exits >= 2,
            "expected blocked exits before escalation"
        );
        let has_escalation = run.trades.iter().any(|t| {
            t.get("exit_reason")
                .and_then(|v| v.as_str())
                .map(|v| v == "blocked_exit_escalation")
                .unwrap_or(false)
        });
        assert!(has_escalation, "expected blocked_exit_escalation trade");
        assert!(
            !run.signal_decisions.is_empty(),
            "expected signal decisions from simulation"
        );
        assert!(
            run.signal_decisions.iter().all(|d| {
                d.get("decision_id")
                    .and_then(|v| v.as_str())
                    .map(|v| !v.trim().is_empty())
                    .unwrap_or(false)
            }),
            "every signal decision should carry a non-empty decision_id"
        );
        assert!(
            run.signal_decisions.iter().any(|d| {
                d.get("action")
                    .and_then(|v| v.as_str())
                    .map(|v| v.eq_ignore_ascii_case("enter"))
                    .unwrap_or(false)
            }),
            "expected at least one enter decision"
        );
        assert!(
            run.signal_decisions.iter().any(|d| {
                d.get("action")
                    .and_then(|v| v.as_str())
                    .map(|v| v.eq_ignore_ascii_case("exit"))
                    .unwrap_or(false)
            }),
            "expected at least one exit decision"
        );
    }

    #[test]
    fn wide_spread_stop_loss_uses_grace_then_forces_exit() {
        let cfg = RuntimeConfig {
            entry_threshold_base: 0.40,
            entry_threshold_cap: 0.55,
            spread_limit_prob: 0.05,
            entry_edge_prob: 0.01,
            entry_min_potential_cents: 1.0,
            entry_max_price_cents: 95.0,
            min_hold_ms: 0,
            stop_loss_cents: 2.0,
            reverse_signal_threshold: -0.95,
            reverse_signal_ticks: 12,
            trail_activate_profit_cents: 99.0,
            trail_drawdown_cents: 99.0,
            take_profit_near_max_cents: 99.9,
            endgame_take_profit_cents: 99.9,
            endgame_remaining_ms: 1_000,
            liquidity_widen_prob: 0.2,
            cooldown_ms: 0,
            max_entries_per_round: 2,
            max_exec_spread_cents: 1.0,
            slippage_cents_per_side: 0.0,
            fee_cents_per_side: 0.0,
            emergency_wide_spread_penalty_ratio: 0.1,
            stop_loss_grace_ticks: 2,
            stop_loss_hard_mult: 1.8,
            stop_loss_reverse_extra_ticks: 1,
            loss_cluster_limit: 0,
            loss_cluster_cooldown_ms: 0,
            noise_gate_enabled: false,
            noise_gate_threshold_add: 0.0,
            noise_gate_edge_add: 0.0,
            noise_gate_spread_scale: 1.0,
            vic_enabled: false,
            vic_target_entries_per_hour: 0.0,
            vic_deadband_ratio: 0.08,
            vic_threshold_relax_max: 0.0,
            vic_edge_relax_max: 0.0,
            vic_spread_relax_max: 0.0,
        };

        let mut samples = vec![
            Sample {
                ts_ms: 1_000,
                round_id: "r-1".to_string(),
                remaining_ms: 200_000,
                p_up: 0.18,
                delta_pct: 0.8,
                velocity: 6.0,
                acceleration: 2.0,
                bid_yes: 0.49,
                ask_yes: 0.50,
                bid_no: 0.50,
                ask_no: 0.51,
                spread_up: 0.005,
                spread_down: 0.005,
                spread_mid: 0.005,
            },
            Sample {
                ts_ms: 2_000,
                round_id: "r-1".to_string(),
                remaining_ms: 199_000,
                p_up: 0.18,
                delta_pct: 0.8,
                velocity: 6.0,
                acceleration: 2.0,
                bid_yes: 0.49,
                ask_yes: 0.50,
                bid_no: 0.50,
                ask_no: 0.51,
                spread_up: 0.005,
                spread_down: 0.005,
                spread_mid: 0.005,
            },
            Sample {
                ts_ms: 3_000,
                round_id: "r-1".to_string(),
                remaining_ms: 198_000,
                p_up: 0.18,
                delta_pct: 0.8,
                velocity: 6.0,
                acceleration: 2.0,
                bid_yes: 0.50,
                ask_yes: 0.51,
                bid_no: 0.49,
                ask_no: 0.50,
                spread_up: 0.005,
                spread_down: 0.005,
                spread_mid: 0.005,
            },
        ];
        for ts in [4_000_i64, 5_000_i64] {
            samples.push(Sample {
                ts_ms: ts,
                round_id: "r-1".to_string(),
                remaining_ms: 197_000 - (ts - 4_000),
                p_up: 0.40,
                delta_pct: -0.3,
                velocity: -2.0,
                acceleration: -0.5,
                bid_yes: 0.47,
                ask_yes: 0.50,
                bid_no: 0.50,
                ask_no: 0.53,
                spread_up: 0.03,
                spread_down: 0.03,
                spread_mid: 0.03,
            });
        }

        let run = simulate(&samples, &cfg, 100);
        assert_eq!(run.trade_count, 1);
        let last_reason = run
            .trades
            .last()
            .and_then(|v| v.get("exit_reason"))
            .and_then(|v| v.as_str());
        assert_eq!(last_reason, Some("stop_loss_wide_grace"));
    }

    #[test]
    fn loss_cluster_cooldown_blocks_reentry_after_loss() {
        let cfg = RuntimeConfig {
            entry_threshold_base: 0.40,
            entry_threshold_cap: 0.55,
            spread_limit_prob: 0.05,
            entry_edge_prob: 0.01,
            entry_min_potential_cents: 1.0,
            entry_max_price_cents: 95.0,
            min_hold_ms: 0,
            stop_loss_cents: 2.0,
            reverse_signal_threshold: -0.95,
            reverse_signal_ticks: 12,
            trail_activate_profit_cents: 99.0,
            trail_drawdown_cents: 99.0,
            take_profit_near_max_cents: 99.9,
            endgame_take_profit_cents: 99.9,
            endgame_remaining_ms: 1_000,
            liquidity_widen_prob: 0.2,
            cooldown_ms: 0,
            max_entries_per_round: 10,
            max_exec_spread_cents: 2.0,
            slippage_cents_per_side: 0.0,
            fee_cents_per_side: 0.0,
            emergency_wide_spread_penalty_ratio: 0.1,
            stop_loss_grace_ticks: 0,
            stop_loss_hard_mult: 1.4,
            stop_loss_reverse_extra_ticks: 1,
            loss_cluster_limit: 1,
            loss_cluster_cooldown_ms: 30_000,
            noise_gate_enabled: false,
            noise_gate_threshold_add: 0.0,
            noise_gate_edge_add: 0.0,
            noise_gate_spread_scale: 1.0,
            vic_enabled: false,
            vic_target_entries_per_hour: 0.0,
            vic_deadband_ratio: 0.08,
            vic_threshold_relax_max: 0.0,
            vic_edge_relax_max: 0.0,
            vic_spread_relax_max: 0.0,
        };

        let mut samples = vec![
            Sample {
                ts_ms: 1_000,
                round_id: "r-1".to_string(),
                remaining_ms: 200_000,
                p_up: 0.18,
                delta_pct: 0.8,
                velocity: 6.0,
                acceleration: 2.0,
                bid_yes: 0.49,
                ask_yes: 0.50,
                bid_no: 0.50,
                ask_no: 0.51,
                spread_up: 0.005,
                spread_down: 0.005,
                spread_mid: 0.005,
            },
            Sample {
                ts_ms: 2_000,
                round_id: "r-1".to_string(),
                remaining_ms: 199_000,
                p_up: 0.18,
                delta_pct: 0.8,
                velocity: 6.0,
                acceleration: 2.0,
                bid_yes: 0.49,
                ask_yes: 0.50,
                bid_no: 0.50,
                ask_no: 0.51,
                spread_up: 0.005,
                spread_down: 0.005,
                spread_mid: 0.005,
            },
            Sample {
                ts_ms: 3_000,
                round_id: "r-1".to_string(),
                remaining_ms: 198_000,
                p_up: 0.18,
                delta_pct: 0.8,
                velocity: 6.0,
                acceleration: 2.0,
                bid_yes: 0.50,
                ask_yes: 0.51,
                bid_no: 0.49,
                ask_no: 0.50,
                spread_up: 0.005,
                spread_down: 0.005,
                spread_mid: 0.005,
            },
            Sample {
                ts_ms: 4_000,
                round_id: "r-1".to_string(),
                remaining_ms: 197_000,
                p_up: 0.35,
                delta_pct: -0.6,
                velocity: -4.0,
                acceleration: -1.0,
                bid_yes: 0.47,
                ask_yes: 0.48,
                bid_no: 0.52,
                ask_no: 0.53,
                spread_up: 0.01,
                spread_down: 0.01,
                spread_mid: 0.01,
            },
        ];
        for ts in [5_000_i64, 6_000_i64, 7_000_i64] {
            samples.push(Sample {
                ts_ms: ts,
                round_id: "r-1".to_string(),
                remaining_ms: 196_000 - (ts - 5_000),
                p_up: 0.18,
                delta_pct: 0.8,
                velocity: 6.0,
                acceleration: 2.0,
                bid_yes: 0.49,
                ask_yes: 0.50,
                bid_no: 0.50,
                ask_no: 0.51,
                spread_up: 0.005,
                spread_down: 0.005,
                spread_mid: 0.005,
            });
        }

        let run = simulate(&samples, &cfg, 100);
        assert_eq!(run.trade_count, 1);
        let last_reason = run
            .trades
            .last()
            .and_then(|v| v.get("exit_reason"))
            .and_then(|v| v.as_str());
        assert_eq!(last_reason, Some("stop_loss"));
    }

    #[test]
    fn directional_edge_guard_blocks_entry_when_edge_opposes_side() {
        let cfg = RuntimeConfig {
            entry_threshold_base: 0.10,
            entry_threshold_cap: 0.25,
            spread_limit_prob: 0.05,
            entry_edge_prob: 0.01,
            entry_min_potential_cents: 1.0,
            entry_max_price_cents: 95.0,
            min_hold_ms: 0,
            stop_loss_cents: 50.0,
            reverse_signal_threshold: -0.9,
            reverse_signal_ticks: 12,
            trail_activate_profit_cents: 99.0,
            trail_drawdown_cents: 99.0,
            take_profit_near_max_cents: 99.0,
            endgame_take_profit_cents: 99.0,
            endgame_remaining_ms: 1_000,
            liquidity_widen_prob: 0.2,
            cooldown_ms: 0,
            max_entries_per_round: 10,
            max_exec_spread_cents: 3.0,
            slippage_cents_per_side: 0.0,
            fee_cents_per_side: 0.0,
            emergency_wide_spread_penalty_ratio: 1.0,
            stop_loss_grace_ticks: 0,
            stop_loss_hard_mult: 1.5,
            stop_loss_reverse_extra_ticks: 1,
            loss_cluster_limit: 0,
            loss_cluster_cooldown_ms: 0,
            noise_gate_enabled: false,
            noise_gate_threshold_add: 0.0,
            noise_gate_edge_add: 0.0,
            noise_gate_spread_scale: 1.0,
            vic_enabled: true,
            vic_target_entries_per_hour: 120.0,
            vic_deadband_ratio: 0.0,
            vic_threshold_relax_max: 0.0,
            vic_edge_relax_max: 0.02,
            vic_spread_relax_max: 0.0,
        };

        // Keep trend strongly UP, but make p_up slightly above fair value:
        // score stays positive while edge_prob is negative.
        let samples = vec![
            Sample {
                ts_ms: 1_000,
                round_id: "r-1".to_string(),
                remaining_ms: 200_000,
                p_up: 0.9340,
                delta_pct: 0.20,
                velocity: 10.0,
                acceleration: 20.0,
                bid_yes: 0.53,
                ask_yes: 0.54,
                bid_no: 0.46,
                ask_no: 0.47,
                spread_up: 0.01,
                spread_down: 0.01,
                spread_mid: 0.01,
            },
            Sample {
                ts_ms: 2_000,
                round_id: "r-1".to_string(),
                remaining_ms: 199_000,
                p_up: 0.9340,
                delta_pct: 0.20,
                velocity: 10.0,
                acceleration: 20.0,
                bid_yes: 0.53,
                ask_yes: 0.54,
                bid_no: 0.46,
                ask_no: 0.47,
                spread_up: 0.01,
                spread_down: 0.01,
                spread_mid: 0.01,
            },
            Sample {
                ts_ms: 3_000,
                round_id: "r-1".to_string(),
                remaining_ms: 198_000,
                p_up: 0.9340,
                delta_pct: 0.20,
                velocity: 10.0,
                acceleration: 20.0,
                bid_yes: 0.53,
                ask_yes: 0.54,
                bid_no: 0.46,
                ask_no: 0.47,
                spread_up: 0.01,
                spread_down: 0.01,
                spread_mid: 0.01,
            },
        ];

        let run = simulate(&samples, &cfg, 32);
        let score = run
            .current
            .get("score")
            .and_then(|v| v.as_f64())
            .unwrap_or_default();
        let edge = run
            .current
            .get("edge_prob")
            .and_then(|v| v.as_f64())
            .unwrap_or_default();
        assert!(score > 0.0, "precondition failed: expected positive score");
        assert!(edge < 0.0, "precondition failed: expected negative edge");
        assert_eq!(
            run.trade_count, 0,
            "entry should be blocked when edge direction opposes trade side"
        );
    }
}
