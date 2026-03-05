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
    c.stop_loss_grace_ticks =
        ((c.stop_loss_grace_ticks as f64) + r(seed) * 2.0 * scale).round() as usize;
    c.stop_loss_hard_mult += r(seed) * 0.35 * scale;
    c.stop_loss_reverse_extra_ticks =
        ((c.stop_loss_reverse_extra_ticks as f64) + r(seed) * 2.0 * scale).round() as usize;
    c.loss_cluster_limit = ((c.loss_cluster_limit as f64) + r(seed) * 2.0 * scale).round() as usize;
    c.loss_cluster_cooldown_ms += (r(seed) * 25_000.0 * scale) as i64;
    c.noise_gate_threshold_add += r(seed) * 0.03 * scale;
    c.noise_gate_edge_add += r(seed) * 0.02 * scale;
    c.noise_gate_spread_scale += r(seed) * 0.20 * scale;
    c.vic_target_entries_per_hour += r(seed) * 8.0 * scale;
    c.vic_deadband_ratio += r(seed) * 0.08 * scale;
    c.vic_threshold_relax_max += r(seed) * 0.03 * scale;
    c.vic_edge_relax_max += r(seed) * 0.02 * scale;
    c.vic_spread_relax_max += r(seed) * 0.12 * scale;
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
    let valid_weight = (1.0_f64 - recent_weight).max(0.1);

    let base_cfg = StrategyRuntimeConfig::default();
    let pool: Vec<(String, StrategyRuntimeConfig)> = build_strategy_arms(base_cfg, max_arms);
    let mut seed = params.seed.unwrap_or(0xC0DEC0DE1234ABCDu64);

    let mut leaderboard: Vec<Value> = Vec::new();
    let mut best_objective = f64::NEG_INFINITY;
    let mut best_payload = Value::Null;
    let mut reached_target = false;

    let evaluate_candidate = |name: String, cfg: &StrategyRuntimeConfig| -> (Value, f64, bool) {
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
    let active_key = strategy_autotune_active_key(&state.redis_prefix, market_type, symbol);
    let live_key = strategy_autotune_live_key(&state.redis_prefix, market_type, symbol);
    let active_history_key =
        strategy_autotune_active_history_key(&state.redis_prefix, market_type, symbol);
    let mut persist_error: Option<String> = None;
    let mut persisted = false;
    let (active_before_opt, _) = resolve_autotune_active_doc(&state, market_type, symbol).await?;
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
                "symbol": symbol,
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
                        "symbol": symbol,
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
        "symbol": symbol,
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

