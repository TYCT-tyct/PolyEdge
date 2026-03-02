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
    total_impact_cents: f64,
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
        "total_impact_cents": run.total_impact_cents,
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
        "stop_loss_grace_ticks": cfg.stop_loss_grace_ticks,
        "stop_loss_hard_mult": cfg.stop_loss_hard_mult,
        "stop_loss_reverse_extra_ticks": cfg.stop_loss_reverse_extra_ticks,
        "loss_cluster_limit": cfg.loss_cluster_limit,
        "loss_cluster_cooldown_ms": cfg.loss_cluster_cooldown_ms,
        "noise_gate_enabled": cfg.noise_gate_enabled,
        "noise_gate_threshold_add": cfg.noise_gate_threshold_add,
        "noise_gate_edge_add": cfg.noise_gate_edge_add,
        "noise_gate_spread_scale": cfg.noise_gate_spread_scale,
        "vic_enabled": cfg.vic_enabled,
        "vic_target_entries_per_hour": cfg.vic_target_entries_per_hour,
        "vic_deadband_ratio": cfg.vic_deadband_ratio,
        "vic_threshold_relax_max": cfg.vic_threshold_relax_max,
        "vic_edge_relax_max": cfg.vic_edge_relax_max,
        "vic_spread_relax_max": cfg.vic_spread_relax_max,
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
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("stop_loss_grace_ticks"))
        .and_then(Value::as_u64)
    {
        c.stop_loss_grace_ticks = v as usize;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("stop_loss_hard_mult"))
        .and_then(Value::as_f64)
    {
        c.stop_loss_hard_mult = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("stop_loss_reverse_extra_ticks"))
        .and_then(Value::as_u64)
    {
        c.stop_loss_reverse_extra_ticks = v as usize;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("loss_cluster_limit"))
        .and_then(Value::as_u64)
    {
        c.loss_cluster_limit = v as usize;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("loss_cluster_cooldown_ms"))
        .and_then(Value::as_i64)
    {
        c.loss_cluster_cooldown_ms = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("noise_gate_enabled"))
        .and_then(Value::as_bool)
    {
        c.noise_gate_enabled = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("noise_gate_threshold_add"))
        .and_then(Value::as_f64)
    {
        c.noise_gate_threshold_add = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("noise_gate_edge_add"))
        .and_then(Value::as_f64)
    {
        c.noise_gate_edge_add = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("noise_gate_spread_scale"))
        .and_then(Value::as_f64)
    {
        c.noise_gate_spread_scale = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("vic_enabled"))
        .and_then(Value::as_bool)
    {
        c.vic_enabled = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("vic_target_entries_per_hour"))
        .and_then(Value::as_f64)
    {
        c.vic_target_entries_per_hour = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("vic_deadband_ratio"))
        .and_then(Value::as_f64)
    {
        c.vic_deadband_ratio = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("vic_threshold_relax_max"))
        .and_then(Value::as_f64)
    {
        c.vic_threshold_relax_max = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("vic_edge_relax_max"))
        .and_then(Value::as_f64)
    {
        c.vic_edge_relax_max = v;
    }
    if let Some(v) = payload
        .get("config")
        .and_then(|v| v.get("vic_spread_relax_max"))
        .and_then(Value::as_f64)
    {
        c.vic_spread_relax_max = v;
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
    let max_points = if !full_history && strategy_ensure_lookback_coverage_enabled() {
        max_points
            .max(strategy_required_points_1s(lookback_minutes))
            .min(strategy_max_points_hard_cap())
    } else {
        max_points
    };
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

    let sample_symbol = "BTCUSDT";
    let (samples, sample_source_mode, sample_resolution_ms) = if !full_history {
        (
            load_strategy_samples_runtime_stream(
                state,
                sample_symbol,
                market_type,
                lookback_minutes,
                max_points,
            )
            .await?,
            "runtime_stream_100ms",
            100,
        )
    } else {
        (
            load_strategy_samples(
                state,
                sample_symbol,
                market_type,
                full_history,
                lookback_minutes,
                max_points,
            )
            .await?,
            "replay_bucket_1s",
            1000,
        )
    };
    if samples.len() < 20 {
        return Err(ApiError::bad_request(
            "not enough samples for live execution",
        ));
    }

    let live_gateway_cfg = LiveGatewayConfig::from_env();
    let (live_gateway_cfg_effective, live_aggr_state_before) = state
        .apply_aggressiveness_to_gateway_cfg(market_type, &live_gateway_cfg)
        .await;
    let capital_cfg = LiveCapitalConfig::from_env();
    let quote_from_price = live_quote_from_price_enabled();
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
    let maintenance_fut = async {
        reconcile_live_reports(state, &live_gateway_cfg_effective, live_executor_mode).await;
        handle_live_pending_timeouts(state, &live_gateway_cfg_effective, live_executor_mode).await;
    };
    let (_, live_market_res, position_before_gate) = tokio::join!(
        maintenance_fut,
        resolve_live_market_target_with_state(state, market_type),
        state.get_live_position_state(market_type)
    );
    let live_market_target_error = live_market_res.as_ref().err().map(|e| e.message.clone());
    let live_market = live_market_res.ok();
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
                    if capital_cfg.bankroll_policy_enabled {
                        capital_skipped.push(json!({
                            "reason": "bankroll_policy_no_add",
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
                    action = "add".to_string();
                    let base_q = d
                        .get("quote_size_usdc")
                        .and_then(Value::as_f64)
                        .unwrap_or(dynamic_quote_usdc);
                    let add_scale =
                        dynamic_add_scale(&d, base_add_scale, &capital_before, &capital_cfg);
                    let px = d.get("price_cents").and_then(Value::as_f64).unwrap_or(50.0);
                    let scaled_q = enforce_min_order_quote_usdc(
                        (base_q * add_scale).max(live_gateway_cfg.min_quote_usdc),
                        px,
                    );
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
                let tuned_q = if quote_from_price {
                    let px = d
                        .get("price_cents")
                        .and_then(Value::as_f64)
                        .unwrap_or(50.0)
                        .clamp(1.0, 99.0);
                    enforce_min_order_quote_usdc(
                        (px / 100.0).max(live_gateway_cfg.min_quote_usdc),
                        px,
                    )
                } else {
                    let q = dynamic_entry_quote(
                        &d,
                        q,
                        live_gateway_cfg.min_quote_usdc,
                        &capital_before,
                        &capital_cfg,
                    );
                    let px = d.get("price_cents").and_then(Value::as_f64).unwrap_or(50.0);
                    enforce_min_order_quote_usdc(q, px)
                };
                if let Some(obj) = d.as_object_mut() {
                    obj.insert("quote_size_usdc".to_string(), json!(tuned_q));
                    if quote_from_price {
                        obj.insert(
                            "quote_source".to_string(),
                            Value::String("price_prob_usdc".to_string()),
                        );
                    }
                }
            }
        } else if action == "add" {
            if capital_cfg.bankroll_policy_enabled {
                capital_skipped.push(json!({
                    "reason": "bankroll_policy_no_add",
                    "decision": d
                }));
                continue;
            }
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
            let px = d.get("price_cents").and_then(Value::as_f64).unwrap_or(50.0);
            let scaled_q = enforce_min_order_quote_usdc(
                (base_q * add_scale).max(live_gateway_cfg.min_quote_usdc),
                px,
            );
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

    let mut live_aggr_state_after = live_aggr_state_before.clone();
    let live_exec_payload = if live_execute {
        let has_exit_like_decision = gated_decisions.iter().any(|d| {
            d.decision
                .get("action")
                .and_then(Value::as_str)
                .map(|v| {
                    let v = v.to_ascii_lowercase();
                    v == "exit" || v == "reduce"
                })
                .unwrap_or(false)
        });
        let locked_exit_target = if live_market.is_none() && has_exit_like_decision {
            build_position_locked_target(market_type, &live_position_state)
        } else {
            None
        };
        let target_from_locked_position = live_market.is_none() && locked_exit_target.is_some();
        match live_market.as_ref().or(locked_exit_target.as_ref()) {
            Some(target) => {
                let execution_orders = execute_live_orders(
                    state,
                    &live_gateway_cfg_effective,
                    live_executor_mode,
                    market_type,
                    target,
                    &live_position_state,
                    &gated_decisions,
                )
                .await;
                live_aggr_state_after = state
                    .update_live_execution_aggr_from_orders(market_type, &execution_orders)
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
                    let accepted_raw = record
                        .get("accepted")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    let order_id = if accepted_raw {
                        extract_order_id_from_gateway_record(record)
                    } else {
                        None
                    };
                    let accepted = accepted_raw && order_id.is_some();
                    let event_reason = if accepted {
                        "submit_accepted_pending_confirm".to_string()
                    } else if accepted_raw && order_id.is_none() {
                        "submit_accepted_missing_order_id".to_string()
                    } else {
                        record
                            .get("reject_reason")
                            .and_then(Value::as_str)
                            .filter(|v| !v.trim().is_empty())
                            .or_else(|| {
                                record
                                    .get("response")
                                    .and_then(|v| v.get("error_msg"))
                                    .and_then(Value::as_str)
                                    .filter(|v| !v.trim().is_empty())
                            })
                            .or_else(|| {
                                record
                                    .get("response")
                                    .and_then(|v| v.get("status"))
                                    .and_then(Value::as_str)
                                    .filter(|v| !v.trim().is_empty())
                            })
                            .or_else(|| {
                                record
                                    .get("error")
                                    .and_then(Value::as_str)
                                    .filter(|v| !v.trim().is_empty())
                            })
                            .or_else(|| {
                                record
                                    .get("reason")
                                    .and_then(Value::as_str)
                                    .filter(|v| !v.trim().is_empty())
                            })
                            .unwrap_or("rejected")
                            .to_string()
                    };
                    if !accepted && event_reason.eq_ignore_ascii_case("rejected") {
                        tracing::warn!(
                            market_type = market_type,
                            action = action,
                            side = side,
                            "live order rejected without detailed reason in execution record"
                        );
                    }
                    let event_reason = if accepted {
                        "submit_accepted_pending_confirm".to_string()
                    } else if accepted_raw && order_id.is_none() {
                        "submit_accepted_missing_order_id".to_string()
                    } else {
                        event_reason
                    };
                    if accepted {
                        if action == "enter" || action == "add" {
                            live_position_state.state = "enter_pending".to_string();
                        } else if action == "exit" || action == "reduce" {
                            live_position_state.state = "exit_pending".to_string();
                        }
                        let order_id = order_id.unwrap_or_default();
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
                        let request_view = record
                            .get("final_request")
                            .or_else(|| record.get("request"));
                        let request_market_id = request_view
                            .and_then(|v| v.get("market_id"))
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .trim()
                            .to_string();
                        let request_token_id = request_view
                            .and_then(|v| v.get("token_id"))
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .trim()
                            .to_string();
                        let request_size_shares = request_view
                            .and_then(|v| v.get("size"))
                            .and_then(Value::as_f64)
                            .unwrap_or(filled_size)
                            .max(0.0);
                        let submit_reason = decision
                            .get("reason")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        let is_exit_like = is_live_exit_action(&action);
                        let emergency_exit =
                            is_exit_like && is_emergency_exit_reason(&submit_reason);
                        let pending = LivePendingOrder {
                            market_type: market_type.to_string(),
                            order_id,
                            market_id: request_market_id,
                            token_id: request_token_id,
                            action: action.clone(),
                            side: side.clone(),
                            round_id: round_id.to_string(),
                            decision_key: record
                                .get("decision_key")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                            price_cents: (exec_price * 100.0).max(0.0),
                            quote_size_usdc: if effective_notional > 0.0 {
                                effective_notional
                            } else {
                                decision
                                    .get("quote_size_usdc")
                                    .and_then(Value::as_f64)
                                    .unwrap_or(0.0)
                            },
                            order_size_shares: request_size_shares,
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
                            submit_reason,
                            // Use real submit time, not signal timestamp, to avoid premature timeout cancellation.
                            submitted_ts_ms: Utc::now().timestamp_millis(),
                            cancel_after_ms: if is_exit_like {
                                if emergency_exit {
                                    650
                                } else {
                                    900
                                }
                            } else {
                                1500
                            },
                            retry_count: 0,
                        };
                        state.upsert_pending_order(pending).await;
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
                                "order_latency_ms": record.get("order_latency_ms"),
                                "attempt_count": record.get("attempt_count"),
                                "attempts": record.get("attempts"),
                                "request": record.get("request"),
                                "final_request": record.get("final_request"),
                                "gateway_endpoint": record.get("endpoint"),
                                "response": record.get("response"),
                                "error": record.get("error"),
                                "price_trace": record.get("price_trace")
                            }),
                        )
                        .await;
                }
                state
                    .put_live_position_state(market_type, live_position_state.clone())
                    .await;
                reconcile_live_reports(state, &live_gateway_cfg_effective, live_executor_mode)
                    .await;
                json!({
                    "mode": "real_gateway",
                    "target_source": if target_from_locked_position { "locked_position_fallback" } else { "resolved" },
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
                if !gated_decisions.is_empty() {
                    state
                        .append_live_event(
                            market_type,
                            json!({
                                "accepted": false,
                                "action": "none",
                                "side": "NONE",
                                "reason": "no_live_market_target",
                                "detail": live_market_target_error.clone()
                            }),
                        )
                        .await;
                }
                json!({
                    "mode": "real_gateway",
                    "error": if gated_decisions.is_empty() { "no_live_decision_to_execute" } else { "no_live_market_target" },
                    "error_detail": live_market_target_error.clone(),
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
                .filter(|o| {
                    o.get("accepted").and_then(Value::as_bool).unwrap_or(false)
                        && extract_order_id_from_gateway_record(o).is_some()
                })
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
        .get_live_capital_state(capital_scope, &capital_cfg)
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
        "sample_source_mode": sample_source_mode,
        "sample_resolution_ms": sample_resolution_ms,
        "lookback": strategy_lookback_meta_json(&samples, full_history, lookback_minutes, max_points, sample_resolution_ms as u32),
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
                "effective_entry_slippage_bps": live_gateway_cfg_effective.entry_slippage_bps,
                "effective_exit_slippage_bps": live_gateway_cfg_effective.exit_slippage_bps,
                "min_quote_usdc": live_gateway_cfg.min_quote_usdc,
                "rust_submit_fallback_gateway": live_gateway_cfg.rust_submit_fallback_gateway,
                "adaptive_state_before": live_aggr_state_before,
                "adaptive_state_after": live_aggr_state_after
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
    let mut mapped = Vec::with_capacity(samples.len());
    mapped.extend(samples.iter().map(map_sample_to_fev1));
    mapped
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
        stop_loss_grace_ticks: cfg.stop_loss_grace_ticks,
        stop_loss_hard_mult: cfg.stop_loss_hard_mult,
        stop_loss_reverse_extra_ticks: cfg.stop_loss_reverse_extra_ticks,
        loss_cluster_limit: cfg.loss_cluster_limit,
        loss_cluster_cooldown_ms: cfg.loss_cluster_cooldown_ms,
        noise_gate_enabled: cfg.noise_gate_enabled,
        noise_gate_threshold_add: cfg.noise_gate_threshold_add,
        noise_gate_edge_add: cfg.noise_gate_edge_add,
        noise_gate_spread_scale: cfg.noise_gate_spread_scale,
        vic_enabled: cfg.vic_enabled,
        vic_target_entries_per_hour: cfg.vic_target_entries_per_hour,
        vic_deadband_ratio: cfg.vic_deadband_ratio,
        vic_threshold_relax_max: cfg.vic_threshold_relax_max,
        vic_edge_relax_max: cfg.vic_edge_relax_max,
        vic_spread_relax_max: cfg.vic_spread_relax_max,
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
        total_impact_cents: run.total_impact_cents,
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
    run_strategy_simulation_on_mapped(&mapped_samples, cfg, max_trades)
}

pub(super) fn run_strategy_simulation_on_mapped(
    mapped_samples: &[fev1::Sample],
    cfg: &StrategyRuntimeConfig,
    max_trades: usize,
) -> StrategySimulationResult {
    let mapped_cfg = map_cfg_to_fev1(cfg);
    map_simulation_result(fev1::simulate(mapped_samples, &mapped_cfg, max_trades))
}

pub(super) async fn load_strategy_samples(
    state: &ApiState,
    symbol: &str,
    market_type: &str,
    full_history: bool,
    lookback_minutes: u32,
    max_points: u32,
) -> Result<Arc<Vec<StrategySample>>, ApiError> {
    let cache_enabled = strategy_sample_cache_enabled() && !full_history;
    let cache_ttl = Duration::from_millis(strategy_sample_cache_ttl_ms());
    let cache_key = strategy_sample_cache_key(
        symbol,
        market_type,
        full_history,
        lookback_minutes,
        max_points,
    );
    if cache_enabled && !cache_ttl.is_zero() {
        let cache = strategy_sample_cache().read().await;
        if let Some(entry) = cache.get(&cache_key) {
            if entry.created_at.elapsed() <= cache_ttl {
                return Ok(entry.samples.clone());
            }
        }
    }

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
        "SELECT *
         FROM (
            SELECT
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
            WHERE symbol='{symbol}'
              AND timeframe='{market_type}'
              {ts_filter}
            GROUP BY ts_ms
            ORDER BY ts_ms DESC
            LIMIT {max_points}
         )
         ORDER BY ts_ms ASC
         FORMAT JSON"
    );
    let rows = rows_from_json(query_clickhouse_json(ch_url, &q).await?);
    let samples = Arc::new(parse_strategy_rows(rows));

    if cache_enabled {
        let mut cache = strategy_sample_cache().write().await;
        let now = Instant::now();
        cache.retain(|_, v| now.duration_since(v.created_at) <= cache_ttl.saturating_mul(3));
        let max_entries = strategy_sample_cache_max_entries();
        if cache.len() >= max_entries {
            let mut oldest: Vec<(String, Instant)> = cache
                .iter()
                .map(|(k, v)| (k.clone(), v.created_at))
                .collect();
            oldest.sort_by_key(|(_, ts)| *ts);
            let remove_n = cache.len().saturating_sub(max_entries.saturating_sub(1));
            for (k, _) in oldest.into_iter().take(remove_n) {
                cache.remove(&k);
            }
        }
        cache.insert(
            cache_key,
            StrategySampleCacheEntry {
                created_at: now,
                samples: samples.clone(),
            },
        );
    }

    Ok(samples)
}

pub(super) async fn load_strategy_samples_runtime_stream(
    state: &ApiState,
    symbol: &str,
    market_type: &str,
    lookback_minutes: u32,
    max_points: u32,
) -> Result<Arc<Vec<StrategySample>>, ApiError> {
    if !strategy_runtime_stream_enabled() {
        return load_strategy_samples(
            state,
            symbol,
            market_type,
            false,
            lookback_minutes,
            max_points,
        )
        .await;
    }
    let Some(ch_url) = state.ch_url.as_deref() else {
        return Err(ApiError::internal("clickhouse not configured"));
    };

    let now_ms = Utc::now().timestamp_millis();
    let from_ms = now_ms.saturating_sub(i64::from(lookback_minutes) * 60_000);
    let key = format!("{symbol}|{market_type}|{lookback_minutes}|{max_points}");
    let reload_after = Duration::from_secs(strategy_runtime_stream_reload_sec());
    let delta_limit = strategy_runtime_stream_delta_limit();

    let cached = {
        let cache = strategy_runtime_stream_cache().read().await;
        cache.get(&key).cloned()
    };

    let bootstrap = cached
        .as_ref()
        .map(|v| v.updated_at.elapsed() > reload_after)
        .unwrap_or(true);

    if bootstrap {
        let q = format!(
            "SELECT *
             FROM (
                SELECT
                    ts_ireland_sample_ms AS ts_ms,
                    round_id,
                    remaining_ms,
                    mid_yes,
                    mid_yes_smooth,
                    mid_no,
                    mid_no_smooth,
                    bid_yes,
                    ask_yes,
                    bid_no,
                    ask_no,
                    delta_pct,
                    delta_pct_smooth,
                    velocity_bps_per_sec,
                    acceleration,
                    binance_price,
                    target_price
                FROM polyedge_forge.snapshot_100ms
                WHERE symbol='{symbol}'
                  AND timeframe='{market_type}'
                  AND ts_ireland_sample_ms >= {from_ms}
                ORDER BY ts_ireland_sample_ms DESC
                LIMIT {max_points}
             )
             ORDER BY ts_ms ASC
             FORMAT JSON"
        );
        let rows = rows_from_json(query_clickhouse_json(ch_url, &q).await?);
        let samples = Arc::new(parse_strategy_rows(rows));
        let last_ts_ms = samples.last().map(|v| v.ts_ms).unwrap_or(from_ms);
        let mut cache = strategy_runtime_stream_cache().write().await;
        cache.insert(
            key,
            StrategyRuntimeStreamState {
                updated_at: Instant::now(),
                last_ts_ms,
                samples: samples.clone(),
            },
        );
        return Ok(samples);
    }

    let state_before = cached.expect("cached checked above");
    let q = format!(
        "SELECT
            ts_ireland_sample_ms AS ts_ms,
            round_id,
            remaining_ms,
            mid_yes,
            mid_yes_smooth,
            mid_no,
            mid_no_smooth,
            bid_yes,
            ask_yes,
            bid_no,
            ask_no,
            delta_pct,
            delta_pct_smooth,
            velocity_bps_per_sec,
            acceleration,
            binance_price,
            target_price
         FROM polyedge_forge.snapshot_100ms
         WHERE symbol='{symbol}'
           AND timeframe='{market_type}'
           AND ts_ireland_sample_ms > {}
           AND ts_ireland_sample_ms >= {from_ms}
         ORDER BY ts_ireland_sample_ms ASC
         LIMIT {delta_limit}
         FORMAT JSON",
        state_before.last_ts_ms
    );
    let rows = rows_from_json(query_clickhouse_json(ch_url, &q).await?);
    let delta = parse_strategy_rows(rows);

    let mut merged = (*state_before.samples).clone();
    let mut last_ts_ms = state_before.last_ts_ms;
    for s in delta {
        if s.ts_ms > last_ts_ms {
            last_ts_ms = s.ts_ms;
            merged.push(s);
        }
    }
    merged.retain(|v| v.ts_ms >= from_ms);
    let max_points_usize = max_points as usize;
    if merged.len() > max_points_usize {
        let remove_n = merged.len().saturating_sub(max_points_usize);
        merged.drain(0..remove_n);
    }
    if let Some(v) = merged.last() {
        last_ts_ms = v.ts_ms;
    }
    let samples = Arc::new(merged);
    let mut cache = strategy_runtime_stream_cache().write().await;
    cache.insert(
        key,
        StrategyRuntimeStreamState {
            updated_at: Instant::now(),
            last_ts_ms,
            samples: samples.clone(),
        },
    );
    Ok(samples)
}

