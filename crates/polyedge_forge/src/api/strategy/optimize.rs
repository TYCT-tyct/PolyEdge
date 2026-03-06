const SEARCH_ENGINE_VERSION: &str = "forge_fev1_search_v3";
const SEARCH_OBJECTIVE_VERSION: &str = "comprehensive_frontier_v3";
const SEARCH_WALK_FORWARD_MAX_FOLDS: usize = 5;
const SEARCH_WALK_FORWARD_MIN_ROUNDS_PER_FOLD: usize = 2;
const SEARCH_WALK_FORWARD_MIN_SAMPLES_PER_FOLD: usize = 120;
const SEARCH_LEADERBOARD_MAX: usize = 18;
const SEARCH_MIN_POSITIVE_FOLD_RATIO: f64 = 0.67;
const SEARCH_MIN_VALIDATION_PROFIT_FACTOR: f64 = 1.01;
const SEARCH_MIN_RECENT_PROFIT_FACTOR: f64 = 1.02;
const SEARCH_MAX_EMERGENCY_EXIT_RATE: f64 = 0.38;
const SEARCH_MAX_BLOCKED_EXIT_RATE: f64 = 0.45;
const HALTON_BASES: [u32; 28] = [
    2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83,
    89, 97, 101, 103, 107,
];

#[derive(Debug, Clone, Copy, Serialize)]
struct StrategySearchBudget {
    seed_trials: usize,
    global_trials: usize,
    guided_trials: usize,
    local_trials: usize,
    leaderboard_size: usize,
}

#[derive(Debug, Clone)]
struct WalkForwardFoldInput {
    name: String,
    mapped_samples: Vec<fev1::Sample>,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct WalkForwardFoldMetrics {
    trade_count: usize,
    win_rate_pct: f64,
    avg_pnl_cents: f64,
    total_pnl_cents: f64,
    max_drawdown_cents: f64,
    profit_factor: f64,
    blocked_exit_rate: f64,
    emergency_exit_rate: f64,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct WalkForwardAggregate {
    fold_count: usize,
    positive_total_pnl_ratio: f64,
    positive_avg_pnl_ratio: f64,
    median_total_pnl_cents: f64,
    min_total_pnl_cents: f64,
    last_total_pnl_cents: f64,
    median_avg_pnl_cents: f64,
    median_win_rate_pct: f64,
    median_trade_count: f64,
    min_profit_factor: f64,
    median_profit_factor: f64,
    worst_drawdown_cents: f64,
    max_blocked_exit_rate: f64,
    max_emergency_exit_rate: f64,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct PositiveGateSummary {
    eligible_positive: bool,
    validation_positive: bool,
    recent_positive: bool,
    validation_profit_factor_ok: bool,
    recent_profit_factor_ok: bool,
    positive_fold_ratio_ok: bool,
    median_fold_positive: bool,
    recent_fold_positive: bool,
    emergency_exit_rate_ok: bool,
    blocked_exit_rate_ok: bool,
    positive_fold_ratio: f64,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct SelectionScores {
    profit: f64,
    robust: f64,
    balanced: f64,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct SelectionImprovement {
    beats_incumbent: bool,
    balanced_gain: f64,
    profit_gain: f64,
    robust_gain: f64,
    recent_net_pnl_gain_cents: f64,
    validation_net_pnl_gain_cents: f64,
    robust_net_pnl_gain_cents: f64,
    mean_pnl_gain_cents: f64,
    win_rate_gain_pct: f64,
    drawdown_delta_cents: f64,
    trade_count_ratio: f64,
}

fn build_strategy_arms(
    base: StrategyRuntimeConfig,
    incumbent: Option<StrategyRuntimeConfig>,
    max_arms: usize,
) -> Vec<(String, StrategyRuntimeConfig)> {
    let mut pool = Vec::<(String, StrategyRuntimeConfig)>::new();
    pool.push(("baseline_scope".to_string(), base));
    if let Some(cfg) = incumbent {
        pool.push(("active_incumbent".to_string(), cfg));
    }
    pool.push(("profile_profit_max".to_string(), strategy_profit_max_config()));
    pool.push(("profile_hi_freq".to_string(), strategy_hi_freq_config()));
    pool.push(("profile_hi_win".to_string(), strategy_hi_win_config()));
    pool.push(("profile_balanced".to_string(), strategy_balanced_config()));
    pool.push((
        "profile_cand_growth_mix".to_string(),
        strategy_cand_growth_mix_config(),
    ));
    pool.push(("profile_backup_baseline".to_string(), strategy_backup_baseline_config()));

    let mut noise_off = base;
    noise_off.noise_gate_enabled = false;
    noise_off.noise_gate_threshold_add = 0.0;
    noise_off.noise_gate_edge_add = 0.0;
    noise_off.noise_gate_spread_scale = 1.0;
    pool.push(("baseline_noise_off".to_string(), noise_off));

    let mut vic_off = base;
    vic_off.vic_enabled = false;
    vic_off.vic_target_entries_per_hour = 0.0;
    vic_off.vic_deadband_ratio = 0.0;
    vic_off.vic_threshold_relax_max = 0.0;
    vic_off.vic_edge_relax_max = 0.0;
    vic_off.vic_spread_relax_max = 0.0;
    pool.push(("baseline_vic_off".to_string(), vic_off));

    let mut both_off = noise_off;
    both_off.vic_enabled = false;
    both_off.vic_target_entries_per_hour = 0.0;
    both_off.vic_deadband_ratio = 0.0;
    both_off.vic_threshold_relax_max = 0.0;
    both_off.vic_edge_relax_max = 0.0;
    both_off.vic_spread_relax_max = 0.0;
    pool.push(("baseline_noise_vic_off".to_string(), both_off));

    let mut dedup = Vec::<(String, StrategyRuntimeConfig)>::new();
    let mut seen = HashSet::<String>::new();
    for (name, mut cfg) in pool {
        canonicalize_search_cfg(&mut cfg, &base);
        let key = strategy_cfg_key(&cfg);
        if seen.insert(key) {
            dedup.push((name, cfg));
        }
    }
    if dedup.len() > max_arms {
        dedup.truncate(max_arms);
    }
    dedup
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
    cfg.stop_loss_grace_ticks = cfg.stop_loss_grace_ticks.clamp(0, 8);
    cfg.stop_loss_hard_mult = cfg.stop_loss_hard_mult.clamp(1.0, 3.0);
    cfg.stop_loss_reverse_extra_ticks = cfg.stop_loss_reverse_extra_ticks.clamp(0, 6);
    cfg.loss_cluster_limit = cfg.loss_cluster_limit.clamp(0, 8);
    cfg.loss_cluster_cooldown_ms = cfg.loss_cluster_cooldown_ms.clamp(0, 120_000);
    cfg.noise_gate_threshold_add = cfg.noise_gate_threshold_add.clamp(0.0, 0.20);
    cfg.noise_gate_edge_add = cfg.noise_gate_edge_add.clamp(0.0, 0.12);
    cfg.noise_gate_spread_scale = cfg.noise_gate_spread_scale.clamp(0.5, 1.2);
    cfg.vic_target_entries_per_hour = cfg.vic_target_entries_per_hour.clamp(0.0, 120.0);
    cfg.vic_deadband_ratio = cfg.vic_deadband_ratio.clamp(0.0, 0.8);
    cfg.vic_threshold_relax_max = cfg.vic_threshold_relax_max.clamp(0.0, 0.2);
    cfg.vic_edge_relax_max = cfg.vic_edge_relax_max.clamp(0.0, 0.1);
    cfg.vic_spread_relax_max = cfg.vic_spread_relax_max.clamp(0.0, 0.8);
}

pub(super) fn lcg_next(seed: &mut u64) -> f64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    let x = (*seed >> 33) as u32;
    (x as f64) / (u32::MAX as f64)
}

fn signed_unit(seed: &mut u64) -> f64 {
    lcg_next(seed) * 2.0 - 1.0
}

fn radical_inverse(base: u32, mut index: u64) -> f64 {
    let base_f = base as f64;
    let mut reversed = 0.0_f64;
    let mut factor = 1.0_f64 / base_f;
    while index > 0 {
        reversed += factor * (index % base as u64) as f64;
        index /= base as u64;
        factor /= base_f;
    }
    reversed
}

fn halton_value(index: u64, dim: usize) -> f64 {
    radical_inverse(HALTON_BASES[dim % HALTON_BASES.len()], index.saturating_add(1))
}

fn lerp(min_v: f64, max_v: f64, t: f64) -> f64 {
    min_v + (max_v - min_v) * t.clamp(0.0, 1.0)
}

fn lerp_i64(min_v: i64, max_v: i64, t: f64) -> i64 {
    lerp(min_v as f64, max_v as f64, t).round() as i64
}

fn lerp_usize(min_v: usize, max_v: usize, t: f64) -> usize {
    lerp(min_v as f64, max_v as f64, t).round() as usize
}

fn freeze_cost_model_fields(cfg: &mut StrategyRuntimeConfig, reference: &StrategyRuntimeConfig) {
    cfg.slippage_cents_per_side = reference.slippage_cents_per_side;
    cfg.fee_cents_per_side = reference.fee_cents_per_side;
    cfg.emergency_wide_spread_penalty_ratio = reference.emergency_wide_spread_penalty_ratio;
}

fn canonicalize_search_cfg(cfg: &mut StrategyRuntimeConfig, reference: &StrategyRuntimeConfig) {
    clamp_runtime_cfg(cfg);
    cfg.entry_threshold_cap = cfg
        .entry_threshold_cap
        .max((cfg.entry_threshold_base + 0.02).min(0.99));
    cfg.trail_activate_profit_cents = cfg
        .trail_activate_profit_cents
        .max(cfg.trail_drawdown_cents + 1.0);
    cfg.take_profit_near_max_cents = cfg
        .take_profit_near_max_cents
        .max(cfg.endgame_take_profit_cents);
    if !cfg.noise_gate_enabled {
        cfg.noise_gate_threshold_add = 0.0;
        cfg.noise_gate_edge_add = 0.0;
        cfg.noise_gate_spread_scale = 1.0;
    }
    if !cfg.vic_enabled {
        cfg.vic_target_entries_per_hour = 0.0;
        cfg.vic_deadband_ratio = 0.0;
        cfg.vic_threshold_relax_max = 0.0;
        cfg.vic_edge_relax_max = 0.0;
        cfg.vic_spread_relax_max = 0.0;
    }
    freeze_cost_model_fields(cfg, reference);
    clamp_runtime_cfg(cfg);
}

fn strategy_cfg_key(cfg: &StrategyRuntimeConfig) -> String {
    serde_json::to_string(&strategy_cfg_json(cfg)).unwrap_or_default()
}

fn search_budget(iterations: usize, max_arms: usize) -> StrategySearchBudget {
    let global_trials = ((iterations as f64) * 0.34).round() as usize;
    let guided_trials = ((iterations as f64) * 0.42).round() as usize;
    let mut local_trials = iterations.saturating_sub(global_trials + guided_trials);
    if local_trials < 24 {
        local_trials = 24.min(iterations);
    }
    StrategySearchBudget {
        seed_trials: max_arms.clamp(6, 16),
        global_trials: global_trials.clamp(20, 240),
        guided_trials: guided_trials.clamp(20, 320),
        local_trials,
        leaderboard_size: SEARCH_LEADERBOARD_MAX,
    }
}

fn sample_global_cfg(reference: &StrategyRuntimeConfig, index: u64) -> StrategyRuntimeConfig {
    let mut dim = 0usize;
    let mut next = || {
        let v = halton_value(index, dim);
        dim = dim.saturating_add(1);
        v
    };
    let mut cfg = *reference;
    let structure = (index % 4) as usize;
    cfg.noise_gate_enabled = matches!(structure, 0 | 1);
    cfg.vic_enabled = matches!(structure, 0 | 2);

    cfg.entry_threshold_base = lerp(0.48, 0.90, next());
    let cap_delta = lerp(0.06, 0.24, next());
    cfg.entry_threshold_cap = (cfg.entry_threshold_base + cap_delta).min(0.99);
    cfg.spread_limit_prob = lerp(0.012, 0.065, next());
    cfg.entry_edge_prob = lerp(0.010, 0.095, next());
    cfg.entry_min_potential_cents = lerp(4.0, 26.0, next());
    cfg.entry_max_price_cents = lerp(60.0, 86.0, next());
    cfg.min_hold_ms = lerp_i64(0, 45_000, next());
    cfg.stop_loss_cents = lerp(4.0, 24.0, next());
    cfg.reverse_signal_threshold = lerp(-0.82, -0.05, next());
    cfg.reverse_signal_ticks = lerp_usize(1, 6, next());
    cfg.trail_activate_profit_cents = lerp(4.0, 34.0, next());
    cfg.trail_drawdown_cents = lerp(2.0, 22.0, next());
    cfg.take_profit_near_max_cents = lerp(88.0, 99.2, next());
    cfg.endgame_take_profit_cents = lerp(84.0, 97.8, next());
    cfg.endgame_remaining_ms = lerp_i64(6_000, 45_000, next());
    cfg.liquidity_widen_prob = lerp(0.04, 0.14, next());
    cfg.cooldown_ms = lerp_i64(0, 15_000, next());
    cfg.max_entries_per_round = lerp_usize(1, 5, next());
    cfg.max_exec_spread_cents = lerp(0.90, 2.80, next());
    cfg.stop_loss_grace_ticks = lerp_usize(0, 4, next());
    cfg.stop_loss_hard_mult = lerp(1.10, 2.05, next());
    cfg.stop_loss_reverse_extra_ticks = lerp_usize(0, 3, next());
    cfg.loss_cluster_limit = lerp_usize(1, 5, next());
    cfg.loss_cluster_cooldown_ms = lerp_i64(10_000, 45_000, next());

    if cfg.noise_gate_enabled {
        cfg.noise_gate_threshold_add = lerp(0.0, 0.07, next());
        cfg.noise_gate_edge_add = lerp(0.0, 0.03, next());
        cfg.noise_gate_spread_scale = lerp(0.72, 1.02, next());
    } else {
        cfg.noise_gate_threshold_add = 0.0;
        cfg.noise_gate_edge_add = 0.0;
        cfg.noise_gate_spread_scale = 1.0;
        let _ = next();
        let _ = next();
        let _ = next();
    }

    if cfg.vic_enabled {
        cfg.vic_target_entries_per_hour = lerp(6.0, 26.0, next());
        cfg.vic_deadband_ratio = lerp(0.03, 0.18, next());
        cfg.vic_threshold_relax_max = lerp(0.0, 0.05, next());
        cfg.vic_edge_relax_max = lerp(0.0, 0.02, next());
        cfg.vic_spread_relax_max = lerp(0.02, 0.22, next());
    } else {
        cfg.vic_target_entries_per_hour = 0.0;
        cfg.vic_deadband_ratio = 0.0;
        cfg.vic_threshold_relax_max = 0.0;
        cfg.vic_edge_relax_max = 0.0;
        cfg.vic_spread_relax_max = 0.0;
        let _ = next();
        let _ = next();
        let _ = next();
        let _ = next();
        let _ = next();
    }

    canonicalize_search_cfg(&mut cfg, reference);
    cfg
}

fn mix_bool(a: bool, b: bool, seed: &mut u64, progress: f64) -> bool {
    if lcg_next(seed) < (0.12 * (1.0 - progress)).clamp(0.02, 0.12) {
        return lcg_next(seed) >= 0.5;
    }
    if lcg_next(seed) >= 0.5 { a } else { b }
}

fn mix_f64(a: f64, b: f64, seed: &mut u64, radius: f64) -> f64 {
    let t = lcg_next(seed);
    let min_v = a.min(b);
    let max_v = a.max(b);
    let span = (max_v - min_v).abs().max(0.001);
    lerp(min_v, max_v, t) + signed_unit(seed) * span * radius.clamp(0.0, 1.0)
}

fn mix_i64(a: i64, b: i64, seed: &mut u64, radius: f64) -> i64 {
    mix_f64(a as f64, b as f64, seed, radius).round() as i64
}

fn mix_usize(a: usize, b: usize, seed: &mut u64, radius: f64) -> usize {
    mix_f64(a as f64, b as f64, seed, radius).round() as usize
}

fn crossover_cfg(
    left: &StrategyRuntimeConfig,
    right: &StrategyRuntimeConfig,
    reference: &StrategyRuntimeConfig,
    seed: &mut u64,
    progress: f64,
) -> StrategyRuntimeConfig {
    let mut cfg = *left;
    let radius = (0.24 * (1.0 - progress) + 0.04).clamp(0.04, 0.28);
    cfg.noise_gate_enabled = mix_bool(left.noise_gate_enabled, right.noise_gate_enabled, seed, progress);
    cfg.vic_enabled = mix_bool(left.vic_enabled, right.vic_enabled, seed, progress);
    cfg.entry_threshold_base = mix_f64(left.entry_threshold_base, right.entry_threshold_base, seed, radius);
    cfg.entry_threshold_cap = mix_f64(left.entry_threshold_cap, right.entry_threshold_cap, seed, radius);
    cfg.spread_limit_prob = mix_f64(left.spread_limit_prob, right.spread_limit_prob, seed, radius);
    cfg.entry_edge_prob = mix_f64(left.entry_edge_prob, right.entry_edge_prob, seed, radius);
    cfg.entry_min_potential_cents =
        mix_f64(left.entry_min_potential_cents, right.entry_min_potential_cents, seed, radius);
    cfg.entry_max_price_cents =
        mix_f64(left.entry_max_price_cents, right.entry_max_price_cents, seed, radius);
    cfg.min_hold_ms = mix_i64(left.min_hold_ms, right.min_hold_ms, seed, radius);
    cfg.stop_loss_cents = mix_f64(left.stop_loss_cents, right.stop_loss_cents, seed, radius);
    cfg.reverse_signal_threshold = mix_f64(
        left.reverse_signal_threshold,
        right.reverse_signal_threshold,
        seed,
        radius,
    );
    cfg.reverse_signal_ticks =
        mix_usize(left.reverse_signal_ticks, right.reverse_signal_ticks, seed, radius);
    cfg.trail_activate_profit_cents = mix_f64(
        left.trail_activate_profit_cents,
        right.trail_activate_profit_cents,
        seed,
        radius,
    );
    cfg.trail_drawdown_cents = mix_f64(
        left.trail_drawdown_cents,
        right.trail_drawdown_cents,
        seed,
        radius,
    );
    cfg.take_profit_near_max_cents = mix_f64(
        left.take_profit_near_max_cents,
        right.take_profit_near_max_cents,
        seed,
        radius,
    );
    cfg.endgame_take_profit_cents = mix_f64(
        left.endgame_take_profit_cents,
        right.endgame_take_profit_cents,
        seed,
        radius,
    );
    cfg.endgame_remaining_ms = mix_i64(
        left.endgame_remaining_ms,
        right.endgame_remaining_ms,
        seed,
        radius,
    );
    cfg.liquidity_widen_prob = mix_f64(
        left.liquidity_widen_prob,
        right.liquidity_widen_prob,
        seed,
        radius,
    );
    cfg.cooldown_ms = mix_i64(left.cooldown_ms, right.cooldown_ms, seed, radius);
    cfg.max_entries_per_round =
        mix_usize(left.max_entries_per_round, right.max_entries_per_round, seed, radius);
    cfg.max_exec_spread_cents = mix_f64(
        left.max_exec_spread_cents,
        right.max_exec_spread_cents,
        seed,
        radius,
    );
    cfg.stop_loss_grace_ticks =
        mix_usize(left.stop_loss_grace_ticks, right.stop_loss_grace_ticks, seed, radius);
    cfg.stop_loss_hard_mult = mix_f64(
        left.stop_loss_hard_mult,
        right.stop_loss_hard_mult,
        seed,
        radius,
    );
    cfg.stop_loss_reverse_extra_ticks = mix_usize(
        left.stop_loss_reverse_extra_ticks,
        right.stop_loss_reverse_extra_ticks,
        seed,
        radius,
    );
    cfg.loss_cluster_limit =
        mix_usize(left.loss_cluster_limit, right.loss_cluster_limit, seed, radius);
    cfg.loss_cluster_cooldown_ms = mix_i64(
        left.loss_cluster_cooldown_ms,
        right.loss_cluster_cooldown_ms,
        seed,
        radius,
    );
    cfg.noise_gate_threshold_add = mix_f64(
        left.noise_gate_threshold_add,
        right.noise_gate_threshold_add,
        seed,
        radius,
    );
    cfg.noise_gate_edge_add = mix_f64(
        left.noise_gate_edge_add,
        right.noise_gate_edge_add,
        seed,
        radius,
    );
    cfg.noise_gate_spread_scale = mix_f64(
        left.noise_gate_spread_scale,
        right.noise_gate_spread_scale,
        seed,
        radius,
    );
    cfg.vic_target_entries_per_hour = mix_f64(
        left.vic_target_entries_per_hour,
        right.vic_target_entries_per_hour,
        seed,
        radius,
    );
    cfg.vic_deadband_ratio = mix_f64(
        left.vic_deadband_ratio,
        right.vic_deadband_ratio,
        seed,
        radius,
    );
    cfg.vic_threshold_relax_max = mix_f64(
        left.vic_threshold_relax_max,
        right.vic_threshold_relax_max,
        seed,
        radius,
    );
    cfg.vic_edge_relax_max = mix_f64(
        left.vic_edge_relax_max,
        right.vic_edge_relax_max,
        seed,
        radius,
    );
    cfg.vic_spread_relax_max = mix_f64(
        left.vic_spread_relax_max,
        right.vic_spread_relax_max,
        seed,
        radius,
    );
    canonicalize_search_cfg(&mut cfg, reference);
    cfg
}

pub(super) fn mutate_cfg(
    parent: &StrategyRuntimeConfig,
    reference: &StrategyRuntimeConfig,
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
    let scale = (1.0 - progress * 0.75).clamp(0.18, 1.0);

    if lcg_next(seed) < (0.10 * scale).clamp(0.02, 0.10) {
        c.noise_gate_enabled = !c.noise_gate_enabled;
    }
    if lcg_next(seed) < (0.10 * scale).clamp(0.02, 0.10) {
        c.vic_enabled = !c.vic_enabled;
    }

    c.entry_threshold_base += signed_unit(seed) * 0.07 * scale;
    c.entry_threshold_cap += signed_unit(seed) * 0.06 * scale;
    c.spread_limit_prob += signed_unit(seed) * 0.010 * scale;
    c.entry_edge_prob += signed_unit(seed) * 0.018 * scale;
    c.entry_min_potential_cents += signed_unit(seed) * 6.0 * scale;
    c.entry_max_price_cents += signed_unit(seed) * 6.0 * scale;
    c.min_hold_ms += (signed_unit(seed) * 22_000.0 * scale) as i64;
    c.stop_loss_cents += signed_unit(seed) * 6.5 * scale;
    c.reverse_signal_threshold += signed_unit(seed) * 0.16 * scale;
    c.reverse_signal_ticks =
        ((c.reverse_signal_ticks as f64) + signed_unit(seed) * 2.0 * scale).round() as usize;
    c.trail_activate_profit_cents += signed_unit(seed) * 7.0 * scale;
    c.trail_drawdown_cents += signed_unit(seed) * 4.0 * scale;
    c.take_profit_near_max_cents += signed_unit(seed) * 3.0 * scale;
    c.endgame_take_profit_cents += signed_unit(seed) * 4.0 * scale;
    c.endgame_remaining_ms += (signed_unit(seed) * 20_000.0 * scale) as i64;
    c.liquidity_widen_prob += signed_unit(seed) * 0.024 * scale;
    c.cooldown_ms += (signed_unit(seed) * 6_000.0 * scale) as i64;
    c.max_entries_per_round =
        ((c.max_entries_per_round as f64) + signed_unit(seed) * 1.4 * scale).round() as usize;
    c.max_exec_spread_cents += signed_unit(seed) * 1.0 * scale;
    c.stop_loss_grace_ticks =
        ((c.stop_loss_grace_ticks as f64) + signed_unit(seed) * 1.6 * scale).round() as usize;
    c.stop_loss_hard_mult += signed_unit(seed) * 0.26 * scale;
    c.stop_loss_reverse_extra_ticks = ((c.stop_loss_reverse_extra_ticks as f64)
        + signed_unit(seed) * 1.4 * scale)
        .round() as usize;
    c.loss_cluster_limit =
        ((c.loss_cluster_limit as f64) + signed_unit(seed) * 1.8 * scale).round() as usize;
    c.loss_cluster_cooldown_ms += (signed_unit(seed) * 18_000.0 * scale) as i64;
    c.noise_gate_threshold_add += signed_unit(seed) * 0.025 * scale;
    c.noise_gate_edge_add += signed_unit(seed) * 0.016 * scale;
    c.noise_gate_spread_scale += signed_unit(seed) * 0.14 * scale;
    c.vic_target_entries_per_hour += signed_unit(seed) * 6.0 * scale;
    c.vic_deadband_ratio += signed_unit(seed) * 0.06 * scale;
    c.vic_threshold_relax_max += signed_unit(seed) * 0.022 * scale;
    c.vic_edge_relax_max += signed_unit(seed) * 0.015 * scale;
    c.vic_spread_relax_max += signed_unit(seed) * 0.08 * scale;
    canonicalize_search_cfg(&mut c, reference);
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

fn median_f64(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        (sorted[mid - 1] + sorted[mid]) * 0.5
    } else {
        sorted[mid]
    }
}

fn build_walk_forward_folds(samples: &[StrategySample]) -> Vec<WalkForwardFoldInput> {
    if samples.len() < SEARCH_WALK_FORWARD_MIN_SAMPLES_PER_FOLD * 3 {
        return Vec::new();
    }
    let mut round_spans = Vec::<(usize, usize)>::new();
    let mut start_idx = 0usize;
    while start_idx < samples.len() {
        let round_id = &samples[start_idx].round_id;
        let mut end_idx = start_idx + 1;
        while end_idx < samples.len() && samples[end_idx].round_id == *round_id {
            end_idx += 1;
        }
        round_spans.push((start_idx, end_idx));
        start_idx = end_idx;
    }
    if round_spans.len() < SEARCH_WALK_FORWARD_MIN_ROUNDS_PER_FOLD * 3 {
        return Vec::new();
    }

    let mut fold_count = (round_spans.len() / 4).clamp(3, SEARCH_WALK_FORWARD_MAX_FOLDS);
    while fold_count > 1 && round_spans.len() / fold_count < SEARCH_WALK_FORWARD_MIN_ROUNDS_PER_FOLD
    {
        fold_count -= 1;
    }
    if fold_count < 2 {
        return Vec::new();
    }

    let mut folds = Vec::<WalkForwardFoldInput>::new();
    for idx in 0..fold_count {
        let start_round = idx * round_spans.len() / fold_count;
        let end_round = ((idx + 1) * round_spans.len() / fold_count).max(start_round + 1);
        let Some((start_sample, _)) = round_spans.get(start_round).copied() else {
            continue;
        };
        let Some((_, end_sample)) = round_spans
            .get(end_round.saturating_sub(1))
            .copied()
        else {
            continue;
        };
        if end_round.saturating_sub(start_round) < SEARCH_WALK_FORWARD_MIN_ROUNDS_PER_FOLD {
            continue;
        }
        let slice = &samples[start_sample..end_sample];
        if slice.len() < SEARCH_WALK_FORWARD_MIN_SAMPLES_PER_FOLD {
            continue;
        }
        folds.push(WalkForwardFoldInput {
            name: format!("fold_{:02}", idx + 1),
            mapped_samples: map_samples_to_fev1(slice),
        });
    }
    folds
}

fn walk_forward_fold_metrics(run: &StrategySimulationResult) -> WalkForwardFoldMetrics {
    let trade_count = run.trade_count.max(1);
    WalkForwardFoldMetrics {
        trade_count: run.trade_count,
        win_rate_pct: run.win_rate_pct,
        avg_pnl_cents: run.avg_pnl_cents,
        total_pnl_cents: run.total_pnl_cents,
        max_drawdown_cents: run.max_drawdown_cents,
        profit_factor: profit_factor(&run.all_trade_pnls),
        blocked_exit_rate: run.blocked_exits as f64 / trade_count as f64,
        emergency_exit_rate: run.emergency_wide_exit_count as f64 / trade_count as f64,
    }
}

fn walk_forward_aggregate(folds: &[WalkForwardFoldMetrics]) -> WalkForwardAggregate {
    if folds.is_empty() {
        return WalkForwardAggregate {
            fold_count: 0,
            positive_total_pnl_ratio: 0.0,
            positive_avg_pnl_ratio: 0.0,
            median_total_pnl_cents: 0.0,
            min_total_pnl_cents: 0.0,
            last_total_pnl_cents: 0.0,
            median_avg_pnl_cents: 0.0,
            median_win_rate_pct: 0.0,
            median_trade_count: 0.0,
            min_profit_factor: 0.0,
            median_profit_factor: 0.0,
            worst_drawdown_cents: 0.0,
            max_blocked_exit_rate: 0.0,
            max_emergency_exit_rate: 0.0,
        };
    }
    let total_pnls: Vec<f64> = folds.iter().map(|f| f.total_pnl_cents).collect();
    let avg_pnls: Vec<f64> = folds.iter().map(|f| f.avg_pnl_cents).collect();
    let win_rates: Vec<f64> = folds.iter().map(|f| f.win_rate_pct).collect();
    let trade_counts: Vec<f64> = folds.iter().map(|f| f.trade_count as f64).collect();
    let profit_factors: Vec<f64> = folds.iter().map(|f| f.profit_factor).collect();
    let positive_total = total_pnls.iter().filter(|v| **v > 0.0).count();
    let positive_avg = avg_pnls.iter().filter(|v| **v > 0.0).count();
    WalkForwardAggregate {
        fold_count: folds.len(),
        positive_total_pnl_ratio: positive_total as f64 / folds.len() as f64,
        positive_avg_pnl_ratio: positive_avg as f64 / folds.len() as f64,
        median_total_pnl_cents: median_f64(&total_pnls),
        min_total_pnl_cents: total_pnls.iter().copied().fold(f64::INFINITY, f64::min),
        last_total_pnl_cents: total_pnls.last().copied().unwrap_or(0.0),
        median_avg_pnl_cents: median_f64(&avg_pnls),
        median_win_rate_pct: median_f64(&win_rates),
        median_trade_count: median_f64(&trade_counts),
        min_profit_factor: profit_factors
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min),
        median_profit_factor: median_f64(&profit_factors),
        worst_drawdown_cents: folds
            .iter()
            .map(|f| f.max_drawdown_cents)
            .fold(0.0_f64, f64::max),
        max_blocked_exit_rate: folds
            .iter()
            .map(|f| f.blocked_exit_rate)
            .fold(0.0_f64, f64::max),
        max_emergency_exit_rate: folds
            .iter()
            .map(|f| f.emergency_exit_rate)
            .fold(0.0_f64, f64::max),
    }
}

fn positive_gate_summary(
    valid_run: &StrategySimulationResult,
    recent_run: &StrategySimulationResult,
    pf_valid: f64,
    pf_recent: f64,
    walk: WalkForwardAggregate,
    window_trades: usize,
) -> PositiveGateSummary {
    let min_trade_gate = window_trades.saturating_sub(4).max(12);
    let validation_positive = valid_run.trade_count >= min_trade_gate
        && valid_run.avg_pnl_cents > 0.0
        && valid_run.total_pnl_cents > 0.0;
    let recent_positive = recent_run.trade_count >= min_trade_gate
        && recent_run.avg_pnl_cents > 0.0
        && recent_run.total_pnl_cents > 0.0;
    let validation_profit_factor_ok = pf_valid >= SEARCH_MIN_VALIDATION_PROFIT_FACTOR;
    let recent_profit_factor_ok = pf_recent >= SEARCH_MIN_RECENT_PROFIT_FACTOR;
    let positive_fold_ratio_ok = walk.fold_count == 0
        || walk.positive_total_pnl_ratio >= SEARCH_MIN_POSITIVE_FOLD_RATIO;
    let median_fold_positive = walk.fold_count == 0 || walk.median_total_pnl_cents > 0.0;
    let recent_fold_positive = walk.fold_count == 0 || walk.last_total_pnl_cents > 0.0;
    let emergency_exit_rate_ok =
        walk.fold_count == 0 || walk.max_emergency_exit_rate <= SEARCH_MAX_EMERGENCY_EXIT_RATE;
    let blocked_exit_rate_ok =
        walk.fold_count == 0 || walk.max_blocked_exit_rate <= SEARCH_MAX_BLOCKED_EXIT_RATE;
    PositiveGateSummary {
        eligible_positive: validation_positive
            && recent_positive
            && validation_profit_factor_ok
            && recent_profit_factor_ok
            && positive_fold_ratio_ok
            && median_fold_positive
            && recent_fold_positive
            && emergency_exit_rate_ok
            && blocked_exit_rate_ok,
        validation_positive,
        recent_positive,
        validation_profit_factor_ok,
        recent_profit_factor_ok,
        positive_fold_ratio_ok,
        median_fold_positive,
        recent_fold_positive,
        emergency_exit_rate_ok,
        blocked_exit_rate_ok,
        positive_fold_ratio: walk.positive_total_pnl_ratio,
    }
}

fn strategy_samples_hash(samples: &[StrategySample]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(samples.len().to_string().as_bytes());
    if let Some(first) = samples.first() {
        hasher.update(first.round_id.as_bytes());
        hasher.update(first.ts_ms.to_le_bytes());
    }
    if let Some(last) = samples.last() {
        hasher.update(last.round_id.as_bytes());
        hasher.update(last.ts_ms.to_le_bytes());
    }
    let step = (samples.len() / 64).max(1);
    for sample in samples.iter().step_by(step).take(128) {
        hasher.update(sample.round_id.as_bytes());
        hasher.update(sample.ts_ms.to_le_bytes());
        hasher.update(sample.remaining_ms.to_le_bytes());
        hasher.update(sample.p_up.to_le_bytes());
        hasher.update(sample.delta_pct.to_le_bytes());
        hasher.update(sample.spread_mid.to_le_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn payload_f64(payload: &Value, path: &str) -> f64 {
    payload
        .pointer(path)
        .and_then(Value::as_f64)
        .unwrap_or(0.0)
}

fn payload_selection_score(payload: &Value, score_name: &str) -> f64 {
    payload
        .pointer(&format!("/selection_scores/{score_name}"))
        .and_then(Value::as_f64)
        .unwrap_or_else(|| payload_objective(payload))
}

fn selection_improvement(payload: &Value, incumbent: &Value) -> SelectionImprovement {
    let candidate_trade_count = payload_f64(payload, "/summary_robust/trade_count");
    let incumbent_trade_count = payload_f64(incumbent, "/summary_robust/trade_count");
    SelectionImprovement {
        beats_incumbent: false,
        balanced_gain: payload_selection_score(payload, "balanced")
            - payload_selection_score(incumbent, "balanced"),
        profit_gain: payload_selection_score(payload, "profit")
            - payload_selection_score(incumbent, "profit"),
        robust_gain: payload_selection_score(payload, "robust")
            - payload_selection_score(incumbent, "robust"),
        recent_net_pnl_gain_cents: payload_f64(payload, "/summary_recent/total_pnl_cents")
            - payload_f64(incumbent, "/summary_recent/total_pnl_cents"),
        validation_net_pnl_gain_cents: payload_f64(payload, "/summary_validation/total_pnl_cents")
            - payload_f64(incumbent, "/summary_validation/total_pnl_cents"),
        robust_net_pnl_gain_cents: payload_f64(payload, "/summary_robust/total_pnl_cents")
            - payload_f64(incumbent, "/summary_robust/total_pnl_cents"),
        mean_pnl_gain_cents: payload_f64(payload, "/summary_robust/avg_pnl_cents")
            - payload_f64(incumbent, "/summary_robust/avg_pnl_cents"),
        win_rate_gain_pct: payload_f64(payload, "/summary_robust/win_rate_pct")
            - payload_f64(incumbent, "/summary_robust/win_rate_pct"),
        drawdown_delta_cents: payload_f64(payload, "/summary_robust/max_drawdown_cents")
            - payload_f64(incumbent, "/summary_robust/max_drawdown_cents"),
        trade_count_ratio: if incumbent_trade_count > 0.0 {
            candidate_trade_count / incumbent_trade_count
        } else {
            1.0
        },
    }
}

fn candidate_beats_incumbent_balanced(payload: &Value, incumbent: &Value) -> bool {
    if !payload_is_positive_eligible(payload) {
        return false;
    }
    let mut improvement = selection_improvement(payload, incumbent);
    let incumbent_positive = payload_is_positive_eligible(incumbent);
    let drawdown_limit = payload_f64(incumbent, "/summary_robust/max_drawdown_cents") * 0.10 + 4.0;
    let drawdown_ok = improvement.drawdown_delta_cents <= drawdown_limit;
    let trade_count_ok = improvement.trade_count_ratio >= 0.80 || improvement.trade_count_ratio >= 1.0;
    let profitability_ok = improvement.recent_net_pnl_gain_cents >= -4.0
        && improvement.validation_net_pnl_gain_cents >= -4.0
        && improvement.robust_net_pnl_gain_cents >= -8.0
        && (improvement.recent_net_pnl_gain_cents > 8.0
            || improvement.validation_net_pnl_gain_cents > 6.0
            || improvement.robust_net_pnl_gain_cents > 8.0);
    let quality_ok = improvement.mean_pnl_gain_cents >= -0.15
        && (improvement.win_rate_gain_pct >= -1.0
            || improvement.recent_net_pnl_gain_cents > 25.0
            || improvement.validation_net_pnl_gain_cents > 18.0);
    let beats = if !incumbent_positive {
        improvement.balanced_gain > 16.0
            && improvement.recent_net_pnl_gain_cents > 0.0
            && improvement.validation_net_pnl_gain_cents > 0.0
    } else {
        improvement.balanced_gain > 12.0
            && profitability_ok
            && quality_ok
            && drawdown_ok
            && trade_count_ok
    };
    improvement.beats_incumbent = beats;
    improvement.beats_incumbent
}

fn payload_is_positive_eligible(payload: &Value) -> bool {
    payload
        .pointer("/robustness/gates/eligible_positive")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn payload_objective(payload: &Value) -> f64 {
    payload
        .get("objective")
        .and_then(Value::as_f64)
        .unwrap_or(f64::NEG_INFINITY)
}

fn sort_leaderboard(leaderboard: &mut Vec<Value>, limit: usize, incumbent: &Value) {
    leaderboard.sort_by(|a, b| {
        let ai = candidate_beats_incumbent_balanced(a, incumbent);
        let bi = candidate_beats_incumbent_balanced(b, incumbent);
        let ae = payload_is_positive_eligible(a);
        let be = payload_is_positive_eligible(b);
        match bi.cmp(&ai) {
            std::cmp::Ordering::Equal => match be.cmp(&ae) {
                std::cmp::Ordering::Equal => payload_selection_score(b, "balanced")
                    .partial_cmp(&payload_selection_score(a, "balanced"))
                    .unwrap_or(std::cmp::Ordering::Equal),
                other => other,
            },
            other => other,
        }
    });
    leaderboard.truncate(limit);
}

fn candidate_names(leaderboard: &[Value]) -> Vec<String> {
    leaderboard
        .iter()
        .filter_map(|payload| payload.get("name").and_then(Value::as_str))
        .map(|v| v.to_string())
        .collect()
}

pub(super) async fn strategy_optimize(
    State(state): State<ApiState>,
    Query(params): Query<StrategyOptimizeQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let market_type = resolve_strategy_market_type(params.market_type.as_deref())?;
    let symbol = resolve_strategy_symbol(params.symbol.as_deref())?;
    let autotune_context =
        normalize_autotune_context(params.autotune_context.as_deref(), market_type, symbol);
    let full_history = params.full_history.unwrap_or(true);
    let lookback_minutes = params
        .lookback_minutes
        .unwrap_or(if full_history { 30 * 24 * 60 } else { 12 * 60 })
        .clamp(30, 365 * 24 * 60);
    let max_points = {
        let resolved = strategy_resolve_max_points(
            full_history,
            lookback_minutes,
            params.max_points,
            params.max_samples,
        );
        let optimize_cap = if !full_history && strategy_ensure_lookback_coverage_enabled() {
            strategy_optimize_guard_max_points().max(strategy_required_points_1s(lookback_minutes))
        } else {
            strategy_optimize_guard_max_points()
        };
        resolved.min(optimize_cap)
    };
    let _heavy_permit = strategy_acquire_heavy_permit(&state, full_history, max_points).await;
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
    let budget = search_budget(iterations, max_arms);

    let baseline_profile = strategy_current_default_profile_name_for_scope(symbol, market_type);
    let base_cfg = strategy_current_default_config_for_scope(symbol, market_type);

    let samples = load_strategy_samples(
        &state,
        symbol,
        market_type,
        full_history,
        lookback_minutes,
        max_points,
    )
    .await?;
    if samples.len() < 20 {
        return Ok(Json(json!({
            "search_engine_version": SEARCH_ENGINE_VERSION,
            "objective_version": SEARCH_OBJECTIVE_VERSION,
            "market_type": market_type,
            "symbol": symbol,
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
    let mapped_train_samples = map_samples_to_fev1(train_samples);
    let mapped_valid_samples = map_samples_to_fev1(valid_samples);
    let mapped_recent_samples = map_samples_to_fev1(recent_samples);
    let recent_target_win_rate = (target_win_rate - 2.0).clamp(40.0, 99.9);
    let recent_max_trades = std::cmp::max(valid_max_trades, window_trades * 4);
    let walk_forward_folds = build_walk_forward_folds(train_samples);
    let dataset_hash = strategy_samples_hash(&samples);
    let study_id = format!(
        "{}:{}:{}:{}",
        SEARCH_ENGINE_VERSION,
        symbol.to_ascii_lowercase(),
        market_type,
        latest_ts_ms
    );

    let (active_before_opt, _) = resolve_autotune_active_doc(&state, market_type, symbol).await?;
    let incumbent_cfg = active_before_opt.as_ref().map(|active_before| {
        if let Some(best) = active_before.get("best") {
            let mut cfg = strategy_cfg_from_payload(base_cfg, best);
            canonicalize_search_cfg(&mut cfg, &base_cfg);
            cfg
        } else {
            let mut cfg = strategy_cfg_from_payload(base_cfg, active_before);
            canonicalize_search_cfg(&mut cfg, &base_cfg);
            cfg
        }
    });

    let seed_pool = build_strategy_arms(base_cfg, incumbent_cfg, budget.seed_trials);
    let mut seed = params.seed.unwrap_or(0xC0DEC0DE1234ABCDu64);
    let mut leaderboard: Vec<Value> = Vec::new();
    let mut reached_target = false;
    let mut seen_configs = HashSet::<String>::new();
    let mut evaluated_candidates = 0usize;
    let mut positive_candidates = 0usize;

    let evaluate_candidate = |name: String, cfg: &StrategyRuntimeConfig| -> (Value, f64, bool, bool) {
        let train_run = run_strategy_simulation_on_mapped(&mapped_train_samples, cfg, max_trades);
        let valid_run =
            run_strategy_simulation_on_mapped(&mapped_valid_samples, cfg, valid_max_trades);
        let recent_run =
            run_strategy_simulation_on_mapped(&mapped_recent_samples, cfg, recent_max_trades);

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
        let consistency_penalty =
            wr_gap * 1.2 + pnl_gap * 1.8 + pf_gap * 10.0 + recent_wr_gap * 2.0;
        let fill_risk_penalty = valid_run.blocked_exits as f64 * 16.0
            + valid_run.execution_penalty_cents_total * 0.8
            + valid_run.emergency_wide_exit_count as f64 * 3.4
            + recent_run.blocked_exits as f64 * 20.0
            + recent_run.execution_penalty_cents_total * 0.9;
        let train_trade_shortfall = window_trades.saturating_sub(train_run.trade_count) as f64;
        let valid_trade_shortfall = window_trades.saturating_sub(valid_run.trade_count) as f64;
        let recent_trade_shortfall = window_trades.saturating_sub(recent_run.trade_count) as f64;
        let coverage_gate_penalty = train_trade_shortfall * 520.0
            + valid_trade_shortfall * 850.0
            + recent_trade_shortfall * 1_050.0;

        let fold_pairs: Vec<(String, WalkForwardFoldMetrics, f64)> = walk_forward_folds
            .iter()
            .map(|fold| {
                let run =
                    run_strategy_simulation_on_mapped(&fold.mapped_samples, cfg, valid_max_trades);
                let metrics = walk_forward_fold_metrics(&run);
                let (_, fold_objective, _) = score_with_rolling(
                    &run,
                    valid_window_trades.min(40),
                    (target_win_rate - 8.0).clamp(40.0, 99.9),
                );
                (fold.name.clone(), metrics, fold_objective)
            })
            .collect();
        let fold_metrics: Vec<WalkForwardFoldMetrics> =
            fold_pairs.iter().map(|(_, metrics, _)| *metrics).collect();
        let walk = walk_forward_aggregate(&fold_metrics);
        let gates =
            positive_gate_summary(&valid_run, &recent_run, pf_valid, pf_recent, walk, window_trades);
        let walk_score = if walk.fold_count > 0 {
            walk.median_total_pnl_cents.max(0.0) * 0.08
                + walk.median_avg_pnl_cents.max(0.0) * 18.0
                + walk.median_win_rate_pct * 1.4
                + walk.positive_total_pnl_ratio * 160.0
                + (walk.median_profit_factor - 1.0).max(0.0) * 45.0
                - (1.0 - walk.min_profit_factor).max(0.0) * 90.0
                - walk.worst_drawdown_cents * 0.03
                - walk.max_blocked_exit_rate * 120.0
                - walk.max_emergency_exit_rate * 140.0
        } else {
            0.0
        };
        let positive_gate_penalty = if gates.eligible_positive {
            -240.0
        } else {
            let fold_penalty = if walk.fold_count > 0 {
                (SEARCH_MIN_POSITIVE_FOLD_RATIO - walk.positive_total_pnl_ratio).max(0.0) * 1800.0
                    + walk.median_total_pnl_cents.min(0.0).abs() * 0.7
                    + walk.last_total_pnl_cents.min(0.0).abs() * 0.9
                    + (SEARCH_MIN_VALIDATION_PROFIT_FACTOR - walk.min_profit_factor).max(0.0) * 280.0
            } else {
                0.0
            };
            3200.0
                + valid_run.total_pnl_cents.min(0.0).abs() * 0.08
                + recent_run.total_pnl_cents.min(0.0).abs() * 0.12
                + (SEARCH_MIN_VALIDATION_PROFIT_FACTOR - pf_valid).max(0.0) * 320.0
                + (SEARCH_MIN_RECENT_PROFIT_FACTOR - pf_recent).max(0.0) * 380.0
                + fold_penalty
        };

        let trade_support = recent_run.trade_count.min(160) as f64 * 0.8
            + valid_run.trade_count.min(120) as f64 * 0.9
            + walk.median_trade_count.min(90.0) * 1.8;
        let profit_score = recent_run.total_pnl_cents.max(0.0) * 0.42
            + valid_run.total_pnl_cents.max(0.0) * 0.30
            + walk.median_total_pnl_cents.max(0.0) * 0.24
            + recent_run.avg_pnl_cents.max(0.0) * 92.0
            + valid_run.avg_pnl_cents.max(0.0) * 74.0
            + walk.median_avg_pnl_cents.max(0.0) * 68.0
            + recent_run.win_rate_pct * 5.5
            + valid_run.win_rate_pct * 4.0
            + ((pf_recent.min(8.0) - 1.0).max(0.0)) * 140.0
            + ((pf_valid.min(6.0) - 1.0).max(0.0)) * 100.0
            + trade_support
            - recent_run.max_drawdown_cents * 1.25
            - valid_run.max_drawdown_cents * 0.95
            - walk.worst_drawdown_cents * 1.10
            - consistency_penalty * 2.0
            - fill_risk_penalty * 1.15
            - coverage_gate_penalty * 0.08
            - if gates.eligible_positive { 0.0 } else { 2200.0 };
        let robust_score = walk.median_total_pnl_cents.max(0.0) * 0.28
            + valid_run.total_pnl_cents.max(0.0) * 0.22
            + recent_run.total_pnl_cents.max(0.0) * 0.18
            + walk.median_avg_pnl_cents.max(0.0) * 88.0
            + valid_run.avg_pnl_cents.max(0.0) * 70.0
            + recent_run.avg_pnl_cents.max(0.0) * 60.0
            + walk.median_win_rate_pct * 7.0
            + valid_run.win_rate_pct * 5.0
            + recent_run.win_rate_pct * 4.5
            + walk.positive_total_pnl_ratio * 280.0
            + ((walk.median_profit_factor.min(8.0) - 1.0).max(0.0)) * 160.0
            + ((walk.min_profit_factor.min(4.0) - 1.0).max(0.0)) * 110.0
            + trade_support * 0.8
            - walk.worst_drawdown_cents * 2.9
            - valid_run.max_drawdown_cents * 1.8
            - recent_run.max_drawdown_cents * 1.4
            - walk.max_blocked_exit_rate * 320.0
            - walk.max_emergency_exit_rate * 360.0
            - consistency_penalty * 2.3
            - fill_risk_penalty
            - coverage_gate_penalty * 0.10
            - if gates.eligible_positive { 0.0 } else { 2600.0 };
        let balanced_score = profit_score * 0.52
            + robust_score * 0.68
            + train_obj * 0.04
            + valid_obj * 0.18
            + recent_obj * 0.24
            + walk_score * 0.55
            - (recent_rs.latest_win_rate_pct - valid_rs.latest_win_rate_pct).abs() * 3.5;
        let objective = balanced_score;

        let hit = gates.eligible_positive
            && (valid_hit || valid_rs.latest_win_rate_pct >= (target_win_rate - 3.0))
            && (recent_hit || recent_rs.latest_win_rate_pct >= (target_win_rate - 2.0))
            && walk.positive_total_pnl_ratio >= SEARCH_MIN_POSITIVE_FOLD_RATIO
            && pf_valid >= SEARCH_MIN_VALIDATION_PROFIT_FACTOR
            && pf_recent >= SEARCH_MIN_RECENT_PROFIT_FACTOR;

        let payload = json!({
            "name": name,
            "search_engine_version": SEARCH_ENGINE_VERSION,
            "objective_version": SEARCH_OBJECTIVE_VERSION,
            "objective": objective,
            "rolling_window": rolling_stats_json(valid_rs),
            "rolling_window_train": rolling_stats_json(train_rs),
            "rolling_window_validation": rolling_stats_json(valid_rs),
            "rolling_window_recent": rolling_stats_json(recent_rs),
            "summary": run_summary_json(&valid_run),
            "summary_train": run_summary_json(&train_run),
            "summary_validation": run_summary_json(&valid_run),
            "summary_recent": run_summary_json(&recent_run),
            "summary_robust": {
                "trade_count": walk.median_trade_count,
                "win_rate_pct": walk.median_win_rate_pct,
                "avg_pnl_cents": walk.median_avg_pnl_cents,
                "total_pnl_cents": walk.median_total_pnl_cents,
                "net_pnl_cents": walk.median_total_pnl_cents,
                "max_drawdown_cents": walk.worst_drawdown_cents,
            },
            "profit_factor_train": pf_train,
            "profit_factor_validation": pf_valid,
            "profit_factor_recent": pf_recent,
            "consistency": {
                "win_rate_gap_pct": wr_gap,
                "avg_pnl_gap_cents": pnl_gap,
                "profit_factor_gap": pf_gap,
                "recent_win_rate_gap_pct": recent_wr_gap,
            },
            "robustness": {
                "walk_forward": {
                    "fold_count": walk.fold_count,
                    "aggregate": walk,
                    "folds": fold_pairs.iter().map(|(fold_name, metrics, fold_objective)| {
                        json!({
                            "name": fold_name,
                            "objective": fold_objective,
                            "summary": metrics,
                        })
                    }).collect::<Vec<Value>>(),
                },
                "gates": gates,
            },
            "objective_components": {
                "train_objective": train_obj,
                "validation_objective": valid_obj,
                "recent_objective": recent_obj,
                "walk_forward_score": walk_score,
                "profit_score": profit_score,
                "robust_score": robust_score,
                "balanced_score": balanced_score,
                "consistency_penalty": consistency_penalty,
                "fill_risk_penalty": fill_risk_penalty,
                "coverage_penalty": coverage_gate_penalty,
                "positive_gate_penalty": positive_gate_penalty,
            },
            "selection_scores": SelectionScores {
                profit: profit_score,
                robust: robust_score,
                balanced: balanced_score,
            },
            "config": strategy_cfg_json(cfg),
        });
        (payload, objective, hit, gates.eligible_positive)
    };

    let incumbent_payload = if let Some(cfg) = incumbent_cfg {
        let (payload, _, _, _) = evaluate_candidate("incumbent_active".to_string(), &cfg);
        payload
    } else {
        let (payload, _, _, _) = evaluate_candidate("incumbent_default".to_string(), &base_cfg);
        payload
    };

    let mut best_profit_score = f64::NEG_INFINITY;
    let mut best_profit_payload = Value::Null;
    let mut best_robust_score = f64::NEG_INFINITY;
    let mut best_robust_payload = Value::Null;
    let mut best_balanced_score = f64::NEG_INFINITY;
    let mut best_balanced_payload = Value::Null;
    let mut best_improved_score = f64::NEG_INFINITY;
    let mut best_improved_payload = Value::Null;

    let register_candidate =
        |name: String,
         mut cfg: StrategyRuntimeConfig,
         leaderboard: &mut Vec<Value>,
         seen_configs: &mut HashSet<String>,
         best_profit_score: &mut f64,
         best_profit_payload: &mut Value,
         best_robust_score: &mut f64,
         best_robust_payload: &mut Value,
         best_balanced_score: &mut f64,
         best_balanced_payload: &mut Value,
         best_improved_score: &mut f64,
         best_improved_payload: &mut Value,
         reached_target: &mut bool,
         evaluated_candidates: &mut usize,
         positive_candidates: &mut usize| {
            canonicalize_search_cfg(&mut cfg, &base_cfg);
            let key = strategy_cfg_key(&cfg);
            if !seen_configs.insert(key) {
                return;
            }
            let (payload, _objective, hit, eligible_positive) = evaluate_candidate(name, &cfg);
            *evaluated_candidates = evaluated_candidates.saturating_add(1);
            if eligible_positive {
                *positive_candidates = positive_candidates.saturating_add(1);
                let profit_score = payload_selection_score(&payload, "profit");
                let robust_score = payload_selection_score(&payload, "robust");
                let balanced_score = payload_selection_score(&payload, "balanced");
                if profit_score > *best_profit_score {
                    *best_profit_score = profit_score;
                    *best_profit_payload = payload.clone();
                }
                if robust_score > *best_robust_score {
                    *best_robust_score = robust_score;
                    *best_robust_payload = payload.clone();
                }
                if balanced_score > *best_balanced_score {
                    *best_balanced_score = balanced_score;
                    *best_balanced_payload = payload.clone();
                }
                if candidate_beats_incumbent_balanced(&payload, &incumbent_payload)
                    && balanced_score > *best_improved_score
                {
                    *best_improved_score = balanced_score;
                    *best_improved_payload = payload.clone();
                }
            }
            if hit {
                *reached_target = true;
            }
            leaderboard.push(payload);
            sort_leaderboard(leaderboard, budget.leaderboard_size, &incumbent_payload);
        };

    for (name, cfg) in seed_pool {
        register_candidate(
            name,
            cfg,
            &mut leaderboard,
            &mut seen_configs,
            &mut best_profit_score,
            &mut best_profit_payload,
            &mut best_robust_score,
            &mut best_robust_payload,
            &mut best_balanced_score,
            &mut best_balanced_payload,
            &mut best_improved_score,
            &mut best_improved_payload,
            &mut reached_target,
            &mut evaluated_candidates,
            &mut positive_candidates,
        );
    }

    for idx in 0..budget.global_trials {
        let cfg = sample_global_cfg(&base_cfg, idx as u64);
        register_candidate(
            format!("global_{idx:03}"),
            cfg,
            &mut leaderboard,
            &mut seen_configs,
            &mut best_profit_score,
            &mut best_profit_payload,
            &mut best_robust_score,
            &mut best_robust_payload,
            &mut best_balanced_score,
            &mut best_balanced_payload,
            &mut best_improved_score,
            &mut best_improved_payload,
            &mut reached_target,
            &mut evaluated_candidates,
            &mut positive_candidates,
        );
    }

    for iter in 0..budget.guided_trials {
        let positives: Vec<&Value> = leaderboard
            .iter()
            .filter(|payload| payload_is_positive_eligible(payload))
            .collect();
        let source = if positives.len() >= 2 {
            positives
        } else {
            leaderboard.iter().collect()
        };
        if source.is_empty() {
            break;
        }
        let left_idx = ((lcg_next(&mut seed) * source.len() as f64).floor() as usize)
            .min(source.len().saturating_sub(1));
        let right_idx = ((lcg_next(&mut seed) * source.len() as f64).floor() as usize)
            .min(source.len().saturating_sub(1));
        let left = strategy_cfg_from_payload(base_cfg, source[left_idx]);
        let right = strategy_cfg_from_payload(base_cfg, source[right_idx]);
        let cfg = crossover_cfg(
            &left,
            &right,
            &base_cfg,
            &mut seed,
            iter as f64 / budget.guided_trials.max(1) as f64,
        );
        register_candidate(
            format!("guided_{iter:03}"),
            cfg,
            &mut leaderboard,
            &mut seen_configs,
            &mut best_profit_score,
            &mut best_profit_payload,
            &mut best_robust_score,
            &mut best_robust_payload,
            &mut best_balanced_score,
            &mut best_balanced_payload,
            &mut best_improved_score,
            &mut best_improved_payload,
            &mut reached_target,
            &mut evaluated_candidates,
            &mut positive_candidates,
        );
    }

    for iter in 0..budget.local_trials {
        let positives: Vec<&Value> = leaderboard
            .iter()
            .filter(|payload| payload_is_positive_eligible(payload))
            .collect();
        if !positives.is_empty() {
            let idx = ((lcg_next(&mut seed) * positives.len() as f64).floor() as usize)
                .min(positives.len().saturating_sub(1));
            let parent = strategy_cfg_from_payload(base_cfg, positives[idx]);
            let cfg = mutate_cfg(&parent, &base_cfg, &mut seed, iter, budget.local_trials);
            register_candidate(
                format!("local_{iter:03}"),
                cfg,
                &mut leaderboard,
                &mut seen_configs,
                &mut best_profit_score,
                &mut best_profit_payload,
                &mut best_robust_score,
                &mut best_robust_payload,
                &mut best_balanced_score,
                &mut best_balanced_payload,
                &mut best_improved_score,
                &mut best_improved_payload,
                &mut reached_target,
                &mut evaluated_candidates,
                &mut positive_candidates,
            );
            continue;
        }

        if leaderboard.is_empty() {
            break;
        }
        let idx = ((lcg_next(&mut seed) * leaderboard.len() as f64).floor() as usize)
            .min(leaderboard.len().saturating_sub(1));
        let parent = strategy_cfg_from_payload(base_cfg, &leaderboard[idx]);
        let cfg = mutate_cfg(&parent, &base_cfg, &mut seed, iter, budget.local_trials);
        register_candidate(
            format!("local_{iter:03}"),
            cfg,
            &mut leaderboard,
            &mut seen_configs,
            &mut best_profit_score,
            &mut best_profit_payload,
            &mut best_robust_score,
            &mut best_robust_payload,
            &mut best_balanced_score,
            &mut best_balanced_payload,
            &mut best_improved_score,
            &mut best_improved_payload,
            &mut reached_target,
            &mut evaluated_candidates,
            &mut positive_candidates,
        );
    }

    let best_discovered_payload = if !best_balanced_payload.is_null() {
        best_balanced_payload.clone()
    } else {
        incumbent_payload.clone()
    };
    let best_payload = if !best_improved_payload.is_null() {
        best_improved_payload.clone()
    } else {
        incumbent_payload.clone()
    };
    let positive_candidate_found = !best_balanced_payload.is_null();
    let selected_beats_incumbent = candidate_beats_incumbent_balanced(&best_payload, &incumbent_payload);
    let selected_improvement = selection_improvement(&best_payload, &incumbent_payload);

    let persist_requested = params.persist_best.unwrap_or(true);
    let persist_ttl_sec = params.persist_ttl_sec.filter(|v| *v > 0);
    let persist_key = strategy_autotune_key(&state.redis_prefix, &autotune_context);
    let active_key = strategy_autotune_active_key(&state.redis_prefix, market_type, symbol);
    let live_key = strategy_autotune_live_key(&state.redis_prefix, market_type, symbol);
    let active_history_key =
        strategy_autotune_active_history_key(&state.redis_prefix, market_type, symbol);
    let mut persist_error: Option<String> = None;
    let mut persisted = false;
    let promote_min_trades = params.promote_min_trades.unwrap_or(40.0);
    let promote_min_win_rate = params.promote_min_win_rate.unwrap_or(70.0);
    let promote_min_pnl = params.promote_min_pnl.unwrap_or(0.0);

    let (should_promote, promotion_reason) = should_promote_candidate(
        &best_payload,
        &incumbent_payload,
        true,
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
                "search_engine_version": SEARCH_ENGINE_VERSION,
                "objective_version": SEARCH_OBJECTIVE_VERSION,
                "study_id": study_id,
                "dataset_hash": dataset_hash,
                "market_type": market_type,
                "symbol": symbol,
                "context": autotune_context.clone(),
                "baseline_profile": baseline_profile,
                "target_win_rate_pct": target_win_rate,
                "window_trades": window_trades,
                "recent_lookback_minutes": recent_lookback_minutes,
                "recent_weight": recent_weight,
                "seed": seed,
                "budget": budget,
                "positive_candidate_found": positive_candidate_found,
                "selected_beats_incumbent": selected_beats_incumbent,
                "config": best_payload.get("config").cloned().unwrap_or(Value::Null),
                "best": best_payload.clone(),
                "best_discovered": best_discovered_payload.clone(),
                "best_profit": best_profit_payload.clone(),
                "best_robust": best_robust_payload.clone(),
                "best_balanced": best_balanced_payload.clone(),
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
                        "search_engine_version": SEARCH_ENGINE_VERSION,
                        "objective_version": SEARCH_OBJECTIVE_VERSION,
                        "study_id": study_id,
                        "dataset_hash": dataset_hash,
                        "market_type": market_type,
                        "symbol": symbol,
                        "context": autotune_context.clone(),
                        "source": "autotune_pipeline",
                        "note": format!("promoted:{promotion_reason}"),
                        "baseline_profile": baseline_profile,
                        "target_win_rate_pct": target_win_rate,
                        "window_trades": window_trades,
                        "selected_beats_incumbent": selected_beats_incumbent,
                        "config": best_payload.get("config").cloned().unwrap_or(Value::Null),
                        "best": best_payload.clone(),
                        "best_discovered": best_discovered_payload.clone(),
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
        "search_engine_version": SEARCH_ENGINE_VERSION,
        "objective_version": SEARCH_OBJECTIVE_VERSION,
        "study_id": study_id,
        "dataset_hash": dataset_hash,
        "market_type": market_type,
        "symbol": symbol,
        "baseline_profile": baseline_profile,
        "autotune_context": autotune_context,
        "full_history": full_history,
        "lookback_minutes": lookback_minutes,
        "lookback": strategy_lookback_meta_json(&samples, full_history, lookback_minutes, max_points, 1000),
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
        "search_budget": budget,
        "candidates_evaluated": evaluated_candidates,
        "positive_candidates": positive_candidates,
        "positive_candidate_found": positive_candidate_found,
        "target_reached": reached_target,
        "leaderboard_names": candidate_names(&leaderboard),
        "selection": {
            "selected_beats_incumbent": selected_beats_incumbent,
            "selected_improvement": selected_improvement,
            "selected_source": if selected_beats_incumbent { "best_improved_balanced" } else { "hold_current_reference" },
        },
        "walk_forward": {
            "fold_count": walk_forward_folds.len(),
            "fold_names": walk_forward_folds.iter().map(|fold| fold.name.clone()).collect::<Vec<String>>(),
        },
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
        "best_discovered": best_discovered_payload,
        "best_profit": best_profit_payload,
        "best_robust": best_robust_payload,
        "best_balanced": best_balanced_payload,
        "incumbent": incumbent_payload,
        "leaderboard": leaderboard,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_cfg() -> StrategyRuntimeConfig {
        strategy_hi_freq_config()
    }

    #[test]
    fn canonicalize_search_cfg_freezes_cost_model_fields() {
        let reference = sample_cfg();
        let mut cfg = sample_cfg();
        cfg.slippage_cents_per_side = 3.7;
        cfg.fee_cents_per_side = 1.2;
        cfg.emergency_wide_spread_penalty_ratio = 1.7;
        cfg.noise_gate_enabled = false;
        cfg.vic_enabled = false;
        canonicalize_search_cfg(&mut cfg, &reference);
        assert_eq!(cfg.slippage_cents_per_side, reference.slippage_cents_per_side);
        assert_eq!(cfg.fee_cents_per_side, reference.fee_cents_per_side);
        assert_eq!(
            cfg.emergency_wide_spread_penalty_ratio,
            reference.emergency_wide_spread_penalty_ratio
        );
        assert_eq!(cfg.noise_gate_threshold_add, 0.0);
        assert_eq!(cfg.vic_target_entries_per_hour, 0.0);
    }

    #[test]
    fn global_sampler_stays_inside_search_bounds() {
        let reference = sample_cfg();
        let cfg = sample_global_cfg(&reference, 17);
        assert!((0.40..=0.95).contains(&cfg.entry_threshold_base));
        assert!(cfg.entry_threshold_cap >= cfg.entry_threshold_base);
        assert!((0.005..=0.12).contains(&cfg.spread_limit_prob));
        assert_eq!(cfg.slippage_cents_per_side, reference.slippage_cents_per_side);
        assert_eq!(cfg.fee_cents_per_side, reference.fee_cents_per_side);
    }

    #[test]
    fn build_walk_forward_folds_keeps_multiple_contiguous_segments() {
        let mut samples = Vec::<StrategySample>::new();
        let mut ts_ms = 1_000_i64;
        for round in 0..12 {
            for _ in 0..80 {
                samples.push(StrategySample {
                    ts_ms,
                    round_id: format!("round_{round:02}"),
                    remaining_ms: 120_000,
                    p_up: 0.5,
                    delta_pct: 0.0,
                    velocity: 0.0,
                    acceleration: 0.0,
                    bid_yes: 0.49,
                    ask_yes: 0.51,
                    bid_no: 0.49,
                    ask_no: 0.51,
                    spread_up: 0.02,
                    spread_down: 0.02,
                    spread_mid: 0.02,
                });
                ts_ms += 1_000;
            }
        }
        let folds = build_walk_forward_folds(&samples);
        assert!(folds.len() >= 3);
        assert!(folds.iter().all(|fold| fold.mapped_samples.len() >= 120));
    }

    #[test]
    fn candidate_beats_incumbent_requires_real_comprehensive_improvement() {
        let incumbent = json!({
            "selection_scores": { "profit": 100.0, "robust": 120.0, "balanced": 150.0 },
            "summary_recent": { "total_pnl_cents": 120.0 },
            "summary_validation": { "total_pnl_cents": 80.0 },
            "summary_robust": {
                "total_pnl_cents": 90.0,
                "avg_pnl_cents": 4.0,
                "win_rate_pct": 70.0,
                "max_drawdown_cents": 30.0,
                "trade_count": 40.0
            },
            "robustness": { "gates": { "eligible_positive": true } }
        });
        let better = json!({
            "selection_scores": { "profit": 145.0, "robust": 154.0, "balanced": 176.0 },
            "summary_recent": { "total_pnl_cents": 168.0 },
            "summary_validation": { "total_pnl_cents": 108.0 },
            "summary_robust": {
                "total_pnl_cents": 112.0,
                "avg_pnl_cents": 4.2,
                "win_rate_pct": 70.4,
                "max_drawdown_cents": 31.0,
                "trade_count": 38.0
            },
            "robustness": { "gates": { "eligible_positive": true } }
        });
        let worse = json!({
            "selection_scores": { "profit": 146.0, "robust": 158.0, "balanced": 170.0 },
            "summary_recent": { "total_pnl_cents": 115.0 },
            "summary_validation": { "total_pnl_cents": 75.0 },
            "summary_robust": {
                "total_pnl_cents": 88.0,
                "avg_pnl_cents": 3.7,
                "win_rate_pct": 68.5,
                "max_drawdown_cents": 38.0,
                "trade_count": 32.0
            },
            "robustness": { "gates": { "eligible_positive": true } }
        });
        assert!(candidate_beats_incumbent_balanced(&better, &incumbent));
        assert!(!candidate_beats_incumbent_balanced(&worse, &incumbent));
    }
}
