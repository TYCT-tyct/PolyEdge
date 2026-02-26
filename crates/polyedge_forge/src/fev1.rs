use std::collections::{HashMap, VecDeque};

use serde_json::{json, Value};

const OFFICIAL_TAKER_FEE_MAX_RATE: f64 = 0.25;
const OFFICIAL_TAKER_FEE_EXPONENT: f64 = 2.0;

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
}

#[derive(Debug, Clone)]
pub struct SimulationResult {
    pub current: Value,
    pub trades: Vec<Value>,
    pub all_trade_pnls: Vec<f64>,
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
    pub total_cost_cents: f64,
    pub net_margin_pct: f64,
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
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
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

#[derive(Debug, Clone)]
struct Position {
    side: Side,
    entry_ts_ms: i64,
    entry_round_id: String,
    entry_price_raw_cents: f64,
    entry_price_cents: f64,
    entry_fee_cents: f64,
    entry_slippage_cents: f64,
    entry_score: f64,
    entry_remaining_ms: i64,
    peak_pnl_cents: f64,
    reverse_streak: usize,
}

#[derive(Debug, Clone, Copy)]
struct Signal {
    score: f64,
    entry_threshold: f64,
    confirmed: bool,
    p_fair_up: f64,
    edge_prob: f64,
}

fn tanh_norm(v: f64, scale: f64) -> f64 {
    if !v.is_finite() || !scale.is_finite() || scale <= 0.0 {
        return 0.0;
    }
    (v / scale).tanh()
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
    let score = (0.72 * edge_component + 0.28 * trend_component - 0.08 * spread_penalty).clamp(-1.0, 1.0);
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

pub fn simulate(samples: &[Sample], cfg: &RuntimeConfig, max_trades: usize) -> SimulationResult {
    let mut p_hist = VecDeque::<f64>::with_capacity(24);
    let mut score_hist = VecDeque::<f64>::with_capacity(24);
    let mut position: Option<Position> = None;
    let mut trades = Vec::<Value>::new();
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

    for sample in samples {
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
            let exit_fee = cfg.fee_cents_per_side + dynamic_taker_fee_cents(bid_raw);
            let mut exit_exec = bid_raw - cfg.slippage_cents_per_side - exit_fee;
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
            } else if sample.remaining_ms <= cfg.endgame_remaining_ms && bid_raw >= cfg.endgame_take_profit_cents {
                exit_reason = Some("endgame_take_profit");
            } else if can_exit_now && pnl <= -cfg.stop_loss_cents {
                exit_reason = Some("stop_loss");
            } else if can_exit_now && pos.reverse_streak >= cfg.reverse_signal_ticks {
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
                    "round_rollover" | "stop_loss" | "endgame_take_profit" | "signal_reverse"
                );
                let can_fill = spread_side_cents <= cfg.max_exec_spread_cents;
                if !can_fill && !emergency {
                    blocked_exits = blocked_exits.saturating_add(1);
                    continue;
                }
                let emergency_penalty_cents = if !can_fill {
                    emergency_wide_exit_count = emergency_wide_exit_count.saturating_add(1);
                    (spread_side_cents - cfg.max_exec_spread_cents).max(0.0)
                        * cfg.emergency_wide_spread_penalty_ratio
                } else {
                    0.0
                };
                exit_exec -= emergency_penalty_cents;
                execution_penalty_cents_total += emergency_penalty_cents;

                let net_pnl = exit_exec - pos.entry_price_cents;
                let gross_pnl = bid_raw - pos.entry_price_raw_cents;
                let total_cost = (gross_pnl - net_pnl).max(0.0);
                let duration_s = ((sample.ts_ms - pos.entry_ts_ms).max(0) as f64) / 1000.0;
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
                    "exit_price_cents": exit_exec,
                    "exit_price_raw_cents": bid_raw,
                    "exit_slippage_cents": cfg.slippage_cents_per_side,
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
                    "entry_reason": "fev1_edgeflow_entry",
                    "exit_reason": reason,
                    "exec_fill_ok": can_fill,
                    "exit_spread_cents": spread_side_cents
                }));
                trade_id += 1;
                position = None;
                last_exit_ts_ms = sample.ts_ms;
            }
        } else {
            let ask_raw = side_ask(sample, side) * 100.0;
            let spread_side_cents = side_spread(sample, side) * 100.0;
            let cooldown_ok = last_exit_ts_ms <= 0 || sample.ts_ms.saturating_sub(last_exit_ts_ms) >= cfg.cooldown_ms;
            let round_entries = entries_by_round.get(&sample.round_id).copied().unwrap_or(0);
            let within_limit = round_entries < cfg.max_entries_per_round;
            let edge_ok = signal.edge_prob.abs() >= cfg.entry_edge_prob;
            let score_ok = score.abs() >= signal.entry_threshold;
            let fair_conf = if side == Side::Up {
                signal.p_fair_up
            } else {
                1.0 - signal.p_fair_up
            };
            let confidence_ok = fair_conf >= 0.56;
            let spread_ok = sample.spread_mid <= cfg.spread_limit_prob
                && spread_side_cents <= cfg.max_exec_spread_cents;
            let fee = cfg.fee_cents_per_side + dynamic_taker_fee_cents(ask_raw);
            let entry_exec = ask_raw + cfg.slippage_cents_per_side + fee;
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
                    entry_score: score,
                    entry_remaining_ms: sample.remaining_ms,
                    peak_pnl_cents: 0.0,
                    reverse_streak: 0,
                });
                *entries_by_round.entry(sample.round_id.clone()).or_insert(0) += 1;
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
        let exit_fee = cfg.fee_cents_per_side + dynamic_taker_fee_cents(bid_raw);
        let exit_exec = bid_raw - cfg.slippage_cents_per_side - exit_fee;
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
            "exit_price_cents": exit_exec,
            "exit_price_raw_cents": bid_raw,
            "exit_slippage_cents": cfg.slippage_cents_per_side,
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
            "entry_reason": "fev1_edgeflow_entry",
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
        .filter_map(|t| {
            let a = t.get("entry_slippage_cents").and_then(Value::as_f64).unwrap_or(0.0);
            let b = t.get("exit_slippage_cents").and_then(Value::as_f64).unwrap_or(0.0);
            Some(a + b)
        })
        .sum();
    let total_cost_cents = total_entry_fee_cents + total_exit_fee_cents + total_slippage_cents + execution_penalty_cents_total;
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
    let max_profit_trade_cents = all_trade_pnls
        .iter()
        .copied()
        .fold(0.0_f64, f64::max);
    let net_margin_pct = if gross_pnl_cents.abs() > 1e-9 {
        total_pnl_cents / gross_pnl_cents.abs() * 100.0
    } else {
        0.0
    };
    let net_pnl_cents = total_pnl_cents;

    let current = json!({
        "suggested_action": if last_confirmed && last_score.abs() >= last_entry_threshold {
            if last_side == "UP" { "ENTER_UP" } else { "ENTER_DOWN" }
        } else if trade_count > 0 {
            "HOLD"
        } else {
            "WAIT"
        },
        "score": last_score,
        "entry_threshold": last_entry_threshold,
        "side": last_side,
        "confirmed": last_confirmed,
        "p_fair_up": last_p_fair_up,
        "edge_prob": last_edge_prob,
    });

    let trades_view = if trades.len() > max_trades {
        trades[trades.len() - max_trades..].to_vec()
    } else {
        trades
    };

    SimulationResult {
        current,
        trades: trades_view,
        all_trade_pnls,
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
        total_cost_cents,
        net_margin_pct,
    }
}
