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

pub(super) fn strategy_sample_from_snapshot_event(
    event: &Value,
) -> Option<(String, String, i64, StrategySample)> {
    let symbol = event
        .get("symbol")
        .and_then(Value::as_str)
        .map(|v| v.trim().to_ascii_uppercase())
        .unwrap_or_default();
    if symbol.is_empty() {
        return None;
    }
    let timeframe = event
        .get("timeframe")
        .and_then(Value::as_str)
        .map(|v| v.trim().to_ascii_lowercase())?;
    if timeframe != "5m" && timeframe != "15m" {
        return None;
    }
    let ts_ms = event.get("ts_ireland_sample_ms").and_then(Value::as_i64)?;
    if ts_ms <= 0 {
        return None;
    }
    let round_id = event
        .get("round_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())?
        .to_string();

    let mut p_up = event
        .get("mid_yes_smooth")
        .and_then(Value::as_f64)
        .or_else(|| event.get("mid_yes").and_then(Value::as_f64))
        .unwrap_or(0.5);
    if !p_up.is_finite() {
        p_up = 0.5;
    }
    p_up = p_up.clamp(0.0, 1.0);
    let p_no_mid = event
        .get("mid_no_smooth")
        .and_then(Value::as_f64)
        .or_else(|| event.get("mid_no").and_then(Value::as_f64))
        .unwrap_or((1.0 - p_up).clamp(0.0, 1.0));
    let bid_yes = event
        .get("bid_yes")
        .and_then(Value::as_f64)
        .unwrap_or((p_up - 0.006).clamp(0.0, 1.0));
    let ask_yes = event
        .get("ask_yes")
        .and_then(Value::as_f64)
        .unwrap_or((p_up + 0.006).clamp(0.0, 1.0));
    let bid_no = event
        .get("bid_no")
        .and_then(Value::as_f64)
        .unwrap_or((p_no_mid - 0.006).clamp(0.0, 1.0));
    let ask_no = event
        .get("ask_no")
        .and_then(Value::as_f64)
        .unwrap_or((p_no_mid + 0.006).clamp(0.0, 1.0));
    let spread_up = (ask_yes - bid_yes).abs().clamp(0.003, 0.04);
    let spread_down = (ask_no - bid_no).abs().clamp(0.003, 0.04);
    let spread_mid = ((spread_up + spread_down) * 0.5).clamp(0.001, 0.08);
    let delta_pct = event
        .get("delta_pct_smooth")
        .and_then(Value::as_f64)
        .or_else(|| event.get("delta_pct").and_then(Value::as_f64))
        .unwrap_or(0.0);
    let velocity = event
        .get("velocity_bps_per_sec")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let acceleration = event
        .get("acceleration")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let remaining_ms = event
        .get("remaining_ms")
        .and_then(Value::as_i64)
        .unwrap_or(0)
        .max(0);

    Some((
        symbol,
        timeframe,
        ts_ms,
        StrategySample {
            ts_ms,
            round_id,
            remaining_ms,
            p_up,
            delta_pct,
            velocity,
            acceleration,
            bid_yes,
            ask_yes,
            bid_no,
            ask_no,
            spread_up,
            spread_down,
            spread_mid,
        },
    ))
}

#[derive(Debug, Clone)]
pub(super) struct StrategySimulationResult {
    current: Value,
    trades: Vec<Value>,
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

pub(super) struct StrategyPaperLiveReq<'a> {
    pub(super) state: &'a ApiState,
    pub(super) symbol: &'a str,
    pub(super) market_type: &'a str,
    pub(super) full_history: bool,
    pub(super) lookback_minutes: u32,
    pub(super) max_points: u32,
    pub(super) max_trades: usize,
    pub(super) cfg: &'a StrategyRuntimeConfig,
    pub(super) config_source: &'a str,
    pub(super) live_execute: bool,
    pub(super) live_quote_usdc: f64,
    pub(super) live_max_orders: usize,
    pub(super) live_drain_only: bool,
}

pub(super) async fn strategy_paper_live(req: StrategyPaperLiveReq<'_>) -> Result<Value, ApiError> {
    let StrategyPaperLiveReq {
        state,
        symbol,
        market_type,
        full_history,
        lookback_minutes,
        max_points,
        max_trades,
        cfg,
        config_source,
        live_execute,
        live_quote_usdc,
        live_max_orders,
        live_drain_only,
    } = req;

    let fixed_guard = strategy_fixed_guard_payload(cfg);
    let runtime_defaults = LiveRuntimeConfig::from_env();
    let live_enabled = fev1::ExecutionGate::from_env().live_enabled;
    let live_allowed = live_execution_market_allowed(market_type);
    let live_submit_allowed = live_submit_effective_armed();
    let live_execute_effective =
        live_execute && live_allowed && live_enabled && !live_drain_only && live_submit_allowed;
    let live_execute_block_reason = if live_execute && !live_allowed {
        Some("market_not_enabled_for_live_execution")
    } else if live_execute && !live_enabled {
        Some("live_execution_disabled_by_env")
    } else if live_execute && !live_submit_allowed {
        Some("live_submit_not_armed")
    } else if live_execute && live_drain_only {
        Some("drain_mode_no_new_orders")
    } else {
        None
    };

    let sample_symbol = symbol;
    let sample_resolution_ms = if full_history { 1000 } else { 100 };
    let sample_source_mode = if full_history {
        "replay_bucket_1s"
    } else {
        "runtime_stream_100ms"
    };
    let samples = if full_history {
        load_strategy_samples(
            state,
            sample_symbol,
            market_type,
            true,
            lookback_minutes,
            max_points,
        )
        .await?
    } else {
        load_strategy_samples_runtime_stream(
            state,
            sample_symbol,
            market_type,
            lookback_minutes,
            max_points,
        )
        .await?
    };

    if samples.len() < 20 {
        return Ok(json!({
            "source": "live",
            "execution_target": "live",
            "live_enabled": live_enabled,
            "strategy_engine": "forge_fev1",
            "strategy_alias": "FEV1",
            "engine_version": "v1",
            "status": "warmup",
            "market_type": market_type,
            "symbol": sample_symbol,
            "lookback_minutes": lookback_minutes,
            "sample_source_mode": sample_source_mode,
            "sample_resolution_ms": sample_resolution_ms,
            "lookback": strategy_lookback_meta_json(&samples, full_history, lookback_minutes, max_points, sample_resolution_ms as u32),
            "runtime_defaults": {
                "lookback_minutes": runtime_defaults.lookback_minutes,
                "max_points": runtime_defaults.max_points,
                "max_trades": runtime_defaults.max_trades,
            },
            "live_execute_requested": live_execute,
            "live_execute": false,
            "live_execute_block_reason": live_execute_block_reason,
            "live_quote_usdc": live_quote_usdc,
            "live_max_orders": live_max_orders,
            "samples": samples.len(),
            "config_source": config_source,
            "baseline_profile": strategy_current_default_profile_name(),
            "fixed_guard": fixed_guard,
            "config": strategy_cfg_json(cfg),
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
                "total_impact_cents": 0.0,
                "net_margin_pct": 0.0,
                "max_drawdown_cents": 0.0,
                "max_profit_trade_cents": 0.0,
                "blocked_exits": 0,
                "emergency_wide_exit_count": 0,
                "execution_penalty_cents_total": 0.0,
            },
            "trades": [],
            "live_execution": {
                "summary": {
                    "decision_count": 0,
                    "mode": if live_execute_effective { "live_ready" } else { "paper_only" },
                },
                "decisions": [],
                "paper_records": [],
                "live_records": [],
                "parity_check": {
                    "status": if live_execute_effective { "blocked_by_gate_or_state" } else { "dry_run" },
                    "level": "warn",
                    "paper": {
                        "decision_count": 0,
                        "entry_count": 0,
                        "add_count": 0,
                        "reduce_count": 0,
                        "exit_count": 0
                    },
                    "live": {
                        "submitted_count": 0,
                        "submitted_entry_count": 0,
                        "submitted_add_count": 0,
                        "submitted_reduce_count": 0,
                        "submitted_exit_count": 0,
                        "accepted_count": 0,
                        "rejected_count": 0,
                        "skipped_count": 0,
                        "no_live_market_target": false
                    }
                },
                "gated": {
                    "selected_count": 0,
                    "submitted_count": 0,
                    "skipped_count": 0,
                    "submitted_decisions": [],
                    "skipped_decisions": []
                },
                "execution": {
                    "mode": "dry_run",
                    "orders": []
                },
                "state_machine": state.get_live_position_state(symbol, market_type).await,
                "events": state.list_live_events(symbol, market_type, 60).await
            }
        }));
    }

    let run = run_strategy_simulation(&samples, cfg, max_trades);
    Ok(json!({
        "source": "live",
        "execution_target": "live",
        "live_enabled": live_enabled,
        "strategy_engine": "forge_fev1",
        "strategy_alias": "FEV1",
        "engine_version": "v1",
        "status": if live_execute_effective { "blocked_by_gate_or_state" } else { "ok" },
        "market_type": market_type,
        "symbol": sample_symbol,
        "lookback_minutes": lookback_minutes,
        "sample_source_mode": sample_source_mode,
        "sample_resolution_ms": sample_resolution_ms,
        "lookback": strategy_lookback_meta_json(&samples, full_history, lookback_minutes, max_points, sample_resolution_ms as u32),
        "runtime_defaults": {
            "lookback_minutes": runtime_defaults.lookback_minutes,
            "max_points": runtime_defaults.max_points,
            "max_trades": runtime_defaults.max_trades,
        },
        "live_execute_requested": live_execute,
        "live_execute": live_execute_effective,
        "live_execute_block_reason": live_execute_block_reason,
        "live_quote_usdc": live_quote_usdc,
        "live_max_orders": live_max_orders,
        "samples": samples.len(),
        "config_source": config_source,
        "baseline_profile": strategy_current_default_profile_name(),
        "fixed_guard": fixed_guard,
        "config": strategy_cfg_json(cfg),
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
            "total_impact_cents": run.total_impact_cents,
            "net_margin_pct": run.net_margin_pct,
            "max_drawdown_cents": run.max_drawdown_cents,
            "max_profit_trade_cents": run.max_profit_trade_cents,
            "blocked_exits": run.blocked_exits,
            "emergency_wide_exit_count": run.emergency_wide_exit_count,
            "execution_penalty_cents_total": run.execution_penalty_cents_total,
        },
        "trades": run.trades,
        "live_execution": {
            "summary": {
                "decision_count": 0,
                "mode": if live_execute_effective { "signal_only" } else { "dry_run" },
            },
            "decisions": [],
            "paper_records": [],
            "live_records": [],
            "parity_check": {
                "status": if live_execute_effective { "blocked_by_gate_or_state" } else { "dry_run" },
                "level": if live_execute_effective { "warn" } else { "ok" },
                "paper": {
                    "decision_count": 0,
                    "entry_count": 0,
                    "add_count": 0,
                    "reduce_count": 0,
                    "exit_count": 0
                },
                "live": {
                    "submitted_count": 0,
                    "submitted_entry_count": 0,
                    "submitted_add_count": 0,
                    "submitted_reduce_count": 0,
                    "submitted_exit_count": 0,
                    "accepted_count": 0,
                    "rejected_count": 0,
                    "skipped_count": 0,
                    "no_live_market_target": false
                }
            },
            "gated": {
                "selected_count": 0,
                "submitted_count": 0,
                "skipped_count": 0,
                "submitted_decisions": [],
                "skipped_decisions": []
            },
            "execution": {
                "mode": "dry_run",
                "orders": []
            },
            "state_machine": state.get_live_position_state(symbol, market_type).await,
            "events": state.list_live_events(symbol, market_type, 60).await
        }
    }))
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
    if strategy_runtime_event_cache_enabled() {
        if let Some(samples) = state
            .get_runtime_event_samples(symbol, market_type, lookback_minutes, max_points)
            .await
        {
            if samples.len() >= 20 {
                return Ok(samples);
            }
        }
    }
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


