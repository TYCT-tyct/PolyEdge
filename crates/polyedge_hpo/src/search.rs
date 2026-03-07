use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use rand::{rngs::StdRng, Rng, SeedableRng};
use reqwest::Client;
use serde_json::Value;
use tokio::sync::Semaphore;

use crate::model::{
    CandidateResult, Constraints, IterationSummary, ResolvedSearchRequest, SearchResult,
    StrategyParams, WindowMetrics,
};

#[derive(Debug, Clone, Copy)]
struct ParamBounds {
    min: f64,
    max: f64,
}

pub struct SearchHooks {
    pub on_iteration: Option<Box<dyn Fn(usize, usize, usize, f64) + Send + Sync>>,
    pub on_candidate: Option<Box<dyn Fn(usize, usize, usize, Option<f64>) + Send + Sync>>,
}

impl SearchHooks {
    pub fn no_op() -> Self {
        Self {
            on_iteration: None,
            on_candidate: None,
        }
    }
}

pub async fn run_search(
    request: ResolvedSearchRequest,
    hooks: SearchHooks,
) -> anyhow::Result<SearchResult> {
    let started_at = crate::model::now_utc();
    let client = Client::builder()
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_nodelay(true)
        .build()
        .context("build reqwest client failed")?;
    let seed = request.search.seed.unwrap_or(0xA5A5_2026_0303);
    let mut rng = StdRng::seed_from_u64(seed);

    let mut population = request
        .seed_params
        .iter()
        .cloned()
        .map(normalize_params)
        .collect::<Vec<_>>();
    while population.len() < request.search.population {
        population.push(random_params(&mut rng));
    }
    population.truncate(request.search.population);
    let mut elite_bank = Vec::<CandidateResult>::new();
    let mut iteration_summaries = Vec::<IterationSummary>::new();
    let total_iterations = request.search.iterations;
    let mut evaluated_total = 0usize;

    for iteration in 1..=total_iterations {
        let mut merged = population.clone();
        merged.extend(
            elite_bank
                .iter()
                .take(request.search.elite)
                .map(|v| v.params.clone()),
        );
        dedup_params(&mut merged);

        let mut evaluated = evaluate_population(
            &client,
            &request,
            merged,
            iteration,
            total_iterations,
            evaluated_total,
            &hooks,
        )
        .await?;
        evaluated_total += evaluated.len();
        evaluated.sort_by(|a, b| b.robust_score.total_cmp(&a.robust_score));
        let best = evaluated
            .first()
            .cloned()
            .context("no candidate evaluated in iteration")?;

        iteration_summaries.push(IterationSummary {
            iteration,
            best_score: best.robust_score,
            best_avg_net_pnl_cents: best.avg_net_pnl_cents,
            best_win_rate_pct: best.avg_win_rate_pct,
            best_drawdown_cents: best.max_drawdown_cents,
        });

        if let Some(cb) = hooks.on_iteration.as_ref() {
            cb(
                iteration,
                total_iterations,
                evaluated_total,
                best.robust_score,
            );
        }

        let mut next_elite = evaluated
            .iter()
            .take(request.search.elite)
            .cloned()
            .collect::<Vec<_>>();
        elite_bank.append(&mut next_elite);
        elite_bank.sort_by(|a, b| b.robust_score.total_cmp(&a.robust_score));
        elite_bank.truncate(request.search.max_top_candidates.saturating_mul(3).max(16));

        let elite_params = elite_bank
            .iter()
            .take(request.search.elite)
            .map(|v| v.params.clone())
            .collect::<Vec<_>>();
        population = next_generation(
            &mut rng,
            &elite_params,
            request.search.population,
            request
                .search
                .random_injection
                .min(request.search.population / 3),
            request.search.mutation_ratio,
        );
    }

    elite_bank.sort_by(|a, b| b.robust_score.total_cmp(&a.robust_score));
    elite_bank.truncate(request.search.max_top_candidates.max(1));
    let best = elite_bank
        .first()
        .cloned()
        .context("search finished with no result")?;

    Ok(SearchResult {
        started_at,
        finished_at: crate::model::now_utc(),
        symbol: request.symbol.clone(),
        market_type: request.market_type.clone(),
        lookbacks: request.lookbacks.clone(),
        total_iterations,
        population: request.search.population,
        best,
        top_candidates: elite_bank,
        iteration_summaries,
    })
}

fn next_generation(
    rng: &mut StdRng,
    elites: &[StrategyParams],
    population_size: usize,
    random_injection: usize,
    mutation_ratio: f64,
) -> Vec<StrategyParams> {
    let mut next = Vec::with_capacity(population_size);
    let keep = elites.len().min(population_size);
    next.extend(elites.iter().take(keep).cloned());

    for _ in 0..random_injection {
        if next.len() >= population_size {
            break;
        }
        next.push(random_params(rng));
    }

    while next.len() < population_size {
        let parent = if elites.is_empty() {
            random_params(rng)
        } else {
            elites[rng.random_range(0..elites.len())].clone()
        };
        next.push(mutate_params(rng, &parent, mutation_ratio));
    }
    next
}

fn dedup_params(params: &mut Vec<StrategyParams>) {
    let mut seen = HashSet::new();
    params.retain(|p| {
        let key = serde_json::to_string(p).unwrap_or_default();
        seen.insert(key)
    });
}

async fn evaluate_population(
    client: &Client,
    request: &ResolvedSearchRequest,
    params: Vec<StrategyParams>,
    iteration: usize,
    total_iterations: usize,
    evaluated_total_before: usize,
    hooks: &SearchHooks,
) -> anyhow::Result<Vec<CandidateResult>> {
    let sem = Arc::new(Semaphore::new(request.search.worker_concurrency.max(1)));
    let mut joinset = tokio::task::JoinSet::new();
    for p in params {
        let sem_cloned = sem.clone();
        let req = request.clone();
        let client_cloned = client.clone();
        joinset.spawn(async move {
            let _permit = sem_cloned.acquire_owned().await.ok()?;
            evaluate_candidate(&client_cloned, &req, p).await.ok()
        });
    }

    let mut out = Vec::new();
    let mut evaluated_in_iteration = 0usize;
    let mut best_score_so_far: Option<f64> = None;
    while let Some(joined) = joinset.join_next().await {
        if let Ok(Some(item)) = joined {
            evaluated_in_iteration += 1;
            let score = item.robust_score;
            best_score_so_far = Some(match best_score_so_far {
                Some(v) => v.max(score),
                None => score,
            });
            out.push(item);
            if let Some(cb) = hooks.on_candidate.as_ref() {
                cb(
                    iteration,
                    total_iterations,
                    evaluated_total_before + evaluated_in_iteration,
                    best_score_so_far,
                );
            }
        }
    }
    Ok(out)
}

async fn evaluate_candidate(
    client: &Client,
    request: &ResolvedSearchRequest,
    params: StrategyParams,
) -> anyhow::Result<CandidateResult> {
    let mut windows = Vec::with_capacity(request.lookbacks.len());
    for lookback in &request.lookbacks {
        windows.push(
            fetch_window_metrics(client, request, &params, *lookback)
                .await
                .with_context(|| format!("fetch lookback={} failed", lookback))?,
        );
    }
    Ok(score_candidate(windows, &params, &request.constraints))
}

async fn fetch_window_metrics(
    client: &Client,
    req: &ResolvedSearchRequest,
    params: &StrategyParams,
    lookback: u32,
) -> anyhow::Result<WindowMetrics> {
    let base = req.forge_base_url.trim_end_matches('/');
    let url = format!("{base}/api/strategy/paper");
    let query = vec![
        ("symbol".to_string(), req.symbol.clone()),
        ("market_type".to_string(), req.market_type.clone()),
        ("full_history".to_string(), req.full_history.to_string()),
        ("lookback_minutes".to_string(), lookback.to_string()),
        ("max_trades".to_string(), req.max_trades.to_string()),
        ("max_samples".to_string(), req.max_samples.to_string()),
        (
            "entry_threshold_base".to_string(),
            fmtf(params.entry_threshold_base),
        ),
        (
            "entry_threshold_cap".to_string(),
            fmtf(params.entry_threshold_cap),
        ),
        (
            "spread_limit_prob".to_string(),
            fmtf(params.spread_limit_prob),
        ),
        ("entry_edge_prob".to_string(), fmtf(params.entry_edge_prob)),
        (
            "entry_min_potential_cents".to_string(),
            fmtf(params.entry_min_potential_cents),
        ),
        (
            "entry_max_price_cents".to_string(),
            fmtf(params.entry_max_price_cents),
        ),
        ("min_hold_ms".to_string(), params.min_hold_ms.to_string()),
        ("stop_loss_cents".to_string(), fmtf(params.stop_loss_cents)),
        (
            "reverse_signal_threshold".to_string(),
            fmtf(params.reverse_signal_threshold),
        ),
        (
            "reverse_signal_ticks".to_string(),
            params.reverse_signal_ticks.to_string(),
        ),
        (
            "trail_activate_profit_cents".to_string(),
            fmtf(params.trail_activate_profit_cents),
        ),
        (
            "trail_drawdown_cents".to_string(),
            fmtf(params.trail_drawdown_cents),
        ),
        (
            "take_profit_near_max_cents".to_string(),
            fmtf(params.take_profit_near_max_cents),
        ),
        (
            "endgame_take_profit_cents".to_string(),
            fmtf(params.endgame_take_profit_cents),
        ),
        (
            "endgame_remaining_ms".to_string(),
            params.endgame_remaining_ms.to_string(),
        ),
        (
            "liquidity_widen_prob".to_string(),
            fmtf(params.liquidity_widen_prob),
        ),
        ("cooldown_ms".to_string(), params.cooldown_ms.to_string()),
        (
            "max_entries_per_round".to_string(),
            params.max_entries_per_round.to_string(),
        ),
        (
            "max_exec_spread_cents".to_string(),
            fmtf(params.max_exec_spread_cents),
        ),
        (
            "slippage_cents_per_side".to_string(),
            fmtf(params.slippage_cents_per_side),
        ),
        (
            "fee_cents_per_side".to_string(),
            fmtf(params.fee_cents_per_side),
        ),
        (
            "emergency_wide_spread_penalty_ratio".to_string(),
            fmtf(params.emergency_wide_spread_penalty_ratio),
        ),
        (
            "stop_loss_grace_ticks".to_string(),
            params.stop_loss_grace_ticks.to_string(),
        ),
        (
            "stop_loss_hard_mult".to_string(),
            fmtf(params.stop_loss_hard_mult),
        ),
        (
            "stop_loss_reverse_extra_ticks".to_string(),
            params.stop_loss_reverse_extra_ticks.to_string(),
        ),
        (
            "loss_cluster_limit".to_string(),
            params.loss_cluster_limit.to_string(),
        ),
        (
            "loss_cluster_cooldown_ms".to_string(),
            params.loss_cluster_cooldown_ms.to_string(),
        ),
        (
            "noise_gate_enabled".to_string(),
            params.noise_gate_enabled.to_string(),
        ),
        (
            "noise_gate_threshold_add".to_string(),
            fmtf(params.noise_gate_threshold_add),
        ),
        (
            "noise_gate_edge_add".to_string(),
            fmtf(params.noise_gate_edge_add),
        ),
        (
            "noise_gate_spread_scale".to_string(),
            fmtf(params.noise_gate_spread_scale),
        ),
        ("vic_enabled".to_string(), params.vic_enabled.to_string()),
        (
            "vic_target_entries_per_hour".to_string(),
            fmtf(params.vic_target_entries_per_hour),
        ),
        (
            "vic_deadband_ratio".to_string(),
            fmtf(params.vic_deadband_ratio),
        ),
        (
            "vic_threshold_relax_max".to_string(),
            fmtf(params.vic_threshold_relax_max),
        ),
        (
            "vic_edge_relax_max".to_string(),
            fmtf(params.vic_edge_relax_max),
        ),
        (
            "vic_spread_relax_max".to_string(),
            fmtf(params.vic_spread_relax_max),
        ),
    ];
    let value = client
        .get(url)
        .query(&query)
        .timeout(Duration::from_secs(req.timeout_secs))
        .send()
        .await
        .context("send request failed")?
        .error_for_status()
        .context("strategy paper http error")?
        .json::<Value>()
        .await
        .context("decode json failed")?;
    parse_window_metrics(value, lookback)
}

fn parse_window_metrics(payload: Value, lookback: u32) -> anyhow::Result<WindowMetrics> {
    let summary = payload
        .get("summary")
        .and_then(Value::as_object)
        .context("missing summary object in paper response")?;
    let trades = payload
        .get("trades")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let trade_count = summary
        .get("trade_count")
        .and_then(Value::as_u64)
        .unwrap_or(trades.len() as u64) as u32;
    let win_rate_pct = summary
        .get("win_rate_pct")
        .and_then(Value::as_f64)
        .unwrap_or_else(|| {
            if trades.is_empty() {
                0.0
            } else {
                let wins = trades
                    .iter()
                    .filter(|v| v.get("pnl_cents").and_then(Value::as_f64).unwrap_or(0.0) > 0.0)
                    .count() as f64;
                wins * 100.0 / trades.len() as f64
            }
        });
    let avg_pnl_cents = summary
        .get("avg_pnl_cents")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let net_pnl_cents = summary
        .get("net_pnl_cents")
        .and_then(Value::as_f64)
        .or_else(|| summary.get("total_pnl_cents").and_then(Value::as_f64))
        .unwrap_or_else(|| {
            trades
                .iter()
                .map(|v| v.get("pnl_cents").and_then(Value::as_f64).unwrap_or(0.0))
                .sum::<f64>()
        });
    let max_drawdown_cents = summary
        .get("max_drawdown_cents")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);

    let tail_len = trades.len().min(80);
    let tail = &trades[trades.len().saturating_sub(tail_len)..];
    let last80_win_rate_pct = if tail.is_empty() {
        0.0
    } else {
        let wins = tail
            .iter()
            .filter(|v| v.get("pnl_cents").and_then(Value::as_f64).unwrap_or(0.0) > 0.0)
            .count() as f64;
        wins * 100.0 / tail.len() as f64
    };
    let last80_avg_pnl_cents = if tail.is_empty() {
        0.0
    } else {
        tail.iter()
            .map(|v| v.get("pnl_cents").and_then(Value::as_f64).unwrap_or(0.0))
            .sum::<f64>()
            / tail.len() as f64
    };

    Ok(WindowMetrics {
        lookback_minutes: lookback,
        trade_count,
        win_rate_pct,
        avg_pnl_cents,
        net_pnl_cents,
        max_drawdown_cents,
        last80_win_rate_pct,
        last80_avg_pnl_cents,
    })
}

fn score_candidate(
    windows: Vec<WindowMetrics>,
    params: &StrategyParams,
    constraints: &Constraints,
) -> CandidateResult {
    let mut hard_fail_reasons = Vec::new();
    let n = windows.len().max(1) as f64;
    let avg_net = windows.iter().map(|w| w.net_pnl_cents).sum::<f64>() / n;
    let avg_win = windows.iter().map(|w| w.win_rate_pct).sum::<f64>() / n;
    let avg_trades = windows.iter().map(|w| w.trade_count as f64).sum::<f64>() / n;
    let avg_drawdown = windows.iter().map(|w| w.max_drawdown_cents).sum::<f64>() / n;
    let worst_win = windows
        .iter()
        .map(|w| w.win_rate_pct)
        .fold(f64::INFINITY, f64::min);
    let worst_net = windows
        .iter()
        .map(|w| w.net_pnl_cents)
        .fold(f64::INFINITY, f64::min);
    let worst_score = windows
        .iter()
        .map(|w| window_score(w, constraints))
        .fold(f64::INFINITY, f64::min);
    let mean_score = windows
        .iter()
        .map(|w| window_score(w, constraints))
        .sum::<f64>()
        / n;

    if avg_trades < constraints.min_trades as f64 {
        hard_fail_reasons.push(format!(
            "avg_trade_count {:.1} < min_trades {}",
            avg_trades, constraints.min_trades
        ));
    }
    if worst_win < constraints.min_win_rate_pct {
        hard_fail_reasons.push(format!(
            "worst_window_win_rate {:.2}% < floor {:.2}%",
            worst_win, constraints.min_win_rate_pct
        ));
    }
    if avg_drawdown > constraints.max_drawdown_cents {
        hard_fail_reasons.push(format!(
            "avg_drawdown {:.2} > max_drawdown {:.2}",
            avg_drawdown, constraints.max_drawdown_cents
        ));
    }
    if constraints.require_non_negative_worst_window_net && worst_net < 0.0 {
        hard_fail_reasons.push(format!("worst_window_net {:.2} < 0", worst_net));
    }

    let mut robust_score = mean_score * 0.62 + worst_score * 0.38;
    if !hard_fail_reasons.is_empty() {
        robust_score -= 10_000.0 + hard_fail_reasons.len() as f64 * 500.0;
    }

    CandidateResult {
        score: mean_score,
        robust_score,
        avg_net_pnl_cents: avg_net,
        worst_window_net_pnl_cents: worst_net,
        avg_win_rate_pct: avg_win,
        worst_window_win_rate_pct: worst_win,
        max_drawdown_cents: avg_drawdown,
        avg_trade_count: avg_trades,
        windows,
        hard_fail_reasons,
        params: params.clone(),
    }
}

fn window_score(window: &WindowMetrics, constraints: &Constraints) -> f64 {
    let participation =
        (window.trade_count as f64 / constraints.min_trades.max(1) as f64).min(1.35);
    let wr_penalty = (constraints.min_win_rate_pct - window.win_rate_pct).max(0.0) * 18.0;
    let drawdown_penalty = window.max_drawdown_cents * 0.82;
    let risk_reward_bonus = if window.max_drawdown_cents <= 0.0 {
        0.0
    } else {
        (window.net_pnl_cents / window.max_drawdown_cents) * 55.0
    };
    window.win_rate_pct * 4.4
        + window.last80_win_rate_pct * 2.8
        + window.avg_pnl_cents * 3.4
        + window.last80_avg_pnl_cents * 2.1
        + window.net_pnl_cents * 0.03
        + participation * 140.0
        + risk_reward_bonus
        - drawdown_penalty
        - wr_penalty
}

pub fn random_params(rng: &mut StdRng) -> StrategyParams {
    normalize_params(StrategyParams {
        entry_threshold_base: sample(rng, b(0.40, 0.95)),
        entry_threshold_cap: sample(rng, b(0.45, 0.99)),
        spread_limit_prob: sample(rng, b(0.005, 0.12)),
        entry_edge_prob: sample(rng, b(0.002, 0.25)),
        entry_min_potential_cents: sample(rng, b(1.0, 70.0)),
        entry_max_price_cents: sample(rng, b(45.0, 98.5)),
        min_hold_ms: sample(rng, b(0.0, 240_000.0)).round() as i64,
        stop_loss_cents: sample(rng, b(2.0, 60.0)),
        reverse_signal_threshold: sample(rng, b(-0.95, -0.02)),
        reverse_signal_ticks: sample(rng, b(1.0, 12.0)).round() as i64,
        trail_activate_profit_cents: sample(rng, b(2.0, 80.0)),
        trail_drawdown_cents: sample(rng, b(1.0, 50.0)),
        take_profit_near_max_cents: sample(rng, b(70.0, 99.5)),
        endgame_take_profit_cents: sample(rng, b(50.0, 99.0)),
        endgame_remaining_ms: sample(rng, b(1_000.0, 180_000.0)).round() as i64,
        liquidity_widen_prob: sample(rng, b(0.01, 0.2)),
        cooldown_ms: sample(rng, b(0.0, 120_000.0)).round() as i64,
        max_entries_per_round: sample(rng, b(1.0, 16.0)).round() as i64,
        max_exec_spread_cents: sample(rng, b(0.2, 30.0)),
        slippage_cents_per_side: sample(rng, b(0.0, 10.0)),
        // Forge paper currently forces fee_cents_per_side to 0.0, so do not waste search budget on it.
        fee_cents_per_side: 0.0,
        emergency_wide_spread_penalty_ratio: sample(rng, b(0.2, 3.0)),
        stop_loss_grace_ticks: sample(rng, b(0.0, 8.0)).round() as i64,
        stop_loss_hard_mult: sample(rng, b(1.0, 3.0)),
        stop_loss_reverse_extra_ticks: sample(rng, b(0.0, 6.0)).round() as i64,
        loss_cluster_limit: sample(rng, b(0.0, 8.0)).round() as i64,
        loss_cluster_cooldown_ms: sample(rng, b(0.0, 120_000.0)).round() as i64,
        noise_gate_enabled: rng.random::<f64>() < 0.8,
        noise_gate_threshold_add: sample(rng, b(0.0, 0.20)),
        noise_gate_edge_add: sample(rng, b(0.0, 0.12)),
        noise_gate_spread_scale: sample(rng, b(0.5, 1.2)),
        vic_enabled: rng.random::<f64>() < 0.8,
        vic_target_entries_per_hour: sample(rng, b(0.0, 120.0)),
        vic_deadband_ratio: sample(rng, b(0.0, 0.8)),
        vic_threshold_relax_max: sample(rng, b(0.0, 0.2)),
        vic_edge_relax_max: sample(rng, b(0.0, 0.1)),
        vic_spread_relax_max: sample(rng, b(0.0, 0.8)),
    })
}

pub fn mutate_params(rng: &mut StdRng, parent: &StrategyParams, ratio: f64) -> StrategyParams {
    let jitter = |rng: &mut StdRng, v: f64, bound: ParamBounds| -> f64 {
        let span = (bound.max - bound.min).abs();
        let delta = (rng.random::<f64>() * 2.0 - 1.0) * span * ratio;
        (v + delta).clamp(bound.min, bound.max)
    };
    let flip_bool = |rng: &mut StdRng, v: bool| -> bool {
        if rng.random::<f64>() < ratio * 0.8 {
            !v
        } else {
            v
        }
    };
    normalize_params(StrategyParams {
        entry_threshold_base: jitter(rng, parent.entry_threshold_base, b(0.40, 0.95)),
        entry_threshold_cap: jitter(rng, parent.entry_threshold_cap, b(0.45, 0.99)),
        spread_limit_prob: jitter(rng, parent.spread_limit_prob, b(0.005, 0.12)),
        entry_edge_prob: jitter(rng, parent.entry_edge_prob, b(0.002, 0.25)),
        entry_min_potential_cents: jitter(rng, parent.entry_min_potential_cents, b(1.0, 70.0)),
        entry_max_price_cents: jitter(rng, parent.entry_max_price_cents, b(45.0, 98.5)),
        min_hold_ms: jitter(rng, parent.min_hold_ms as f64, b(0.0, 240_000.0)).round() as i64,
        stop_loss_cents: jitter(rng, parent.stop_loss_cents, b(2.0, 60.0)),
        reverse_signal_threshold: jitter(rng, parent.reverse_signal_threshold, b(-0.95, -0.02)),
        reverse_signal_ticks: jitter(rng, parent.reverse_signal_ticks as f64, b(1.0, 12.0))
            .round() as i64,
        trail_activate_profit_cents: jitter(rng, parent.trail_activate_profit_cents, b(2.0, 80.0)),
        trail_drawdown_cents: jitter(rng, parent.trail_drawdown_cents, b(1.0, 50.0)),
        take_profit_near_max_cents: jitter(rng, parent.take_profit_near_max_cents, b(70.0, 99.5)),
        endgame_take_profit_cents: jitter(rng, parent.endgame_take_profit_cents, b(50.0, 99.0)),
        endgame_remaining_ms: jitter(
            rng,
            parent.endgame_remaining_ms as f64,
            b(1_000.0, 180_000.0),
        )
        .round() as i64,
        liquidity_widen_prob: jitter(rng, parent.liquidity_widen_prob, b(0.01, 0.2)),
        cooldown_ms: jitter(rng, parent.cooldown_ms as f64, b(0.0, 120_000.0)).round() as i64,
        max_entries_per_round: jitter(rng, parent.max_entries_per_round as f64, b(1.0, 16.0))
            .round() as i64,
        max_exec_spread_cents: jitter(rng, parent.max_exec_spread_cents, b(0.2, 30.0)),
        slippage_cents_per_side: jitter(rng, parent.slippage_cents_per_side, b(0.0, 10.0)),
        fee_cents_per_side: 0.0,
        emergency_wide_spread_penalty_ratio: jitter(
            rng,
            parent.emergency_wide_spread_penalty_ratio,
            b(0.2, 3.0),
        ),
        stop_loss_grace_ticks: jitter(rng, parent.stop_loss_grace_ticks as f64, b(0.0, 8.0))
            .round() as i64,
        stop_loss_hard_mult: jitter(rng, parent.stop_loss_hard_mult, b(1.0, 3.0)),
        stop_loss_reverse_extra_ticks: jitter(
            rng,
            parent.stop_loss_reverse_extra_ticks as f64,
            b(0.0, 6.0),
        )
        .round() as i64,
        loss_cluster_limit: jitter(rng, parent.loss_cluster_limit as f64, b(0.0, 8.0)).round()
            as i64,
        loss_cluster_cooldown_ms: jitter(
            rng,
            parent.loss_cluster_cooldown_ms as f64,
            b(0.0, 120_000.0),
        )
        .round() as i64,
        noise_gate_enabled: flip_bool(rng, parent.noise_gate_enabled),
        noise_gate_threshold_add: jitter(rng, parent.noise_gate_threshold_add, b(0.0, 0.20)),
        noise_gate_edge_add: jitter(rng, parent.noise_gate_edge_add, b(0.0, 0.12)),
        noise_gate_spread_scale: jitter(rng, parent.noise_gate_spread_scale, b(0.5, 1.2)),
        vic_enabled: flip_bool(rng, parent.vic_enabled),
        vic_target_entries_per_hour: jitter(
            rng,
            parent.vic_target_entries_per_hour,
            b(0.0, 120.0),
        ),
        vic_deadband_ratio: jitter(rng, parent.vic_deadband_ratio, b(0.0, 0.8)),
        vic_threshold_relax_max: jitter(rng, parent.vic_threshold_relax_max, b(0.0, 0.2)),
        vic_edge_relax_max: jitter(rng, parent.vic_edge_relax_max, b(0.0, 0.1)),
        vic_spread_relax_max: jitter(rng, parent.vic_spread_relax_max, b(0.0, 0.8)),
    })
}

#[inline]
fn sample(rng: &mut StdRng, bound: ParamBounds) -> f64 {
    rng.random_range(bound.min..=bound.max)
}

#[inline]
fn b(min: f64, max: f64) -> ParamBounds {
    ParamBounds { min, max }
}

#[inline]
fn fmtf(v: f64) -> String {
    format!("{v:.8}")
}

fn normalize_params(mut p: StrategyParams) -> StrategyParams {
    p.entry_threshold_base = p.entry_threshold_base.clamp(0.40, 0.95);
    p.entry_threshold_cap = p
        .entry_threshold_cap
        .clamp((p.entry_threshold_base + 0.01).min(0.99), 0.99);
    p.spread_limit_prob = p.spread_limit_prob.clamp(0.005, 0.12);
    p.entry_edge_prob = p.entry_edge_prob.clamp(0.002, 0.25);
    p.entry_min_potential_cents = p.entry_min_potential_cents.clamp(1.0, 70.0);
    p.entry_max_price_cents = p.entry_max_price_cents.clamp(45.0, 98.5);
    p.min_hold_ms = p.min_hold_ms.clamp(0, 240_000);
    p.stop_loss_cents = p.stop_loss_cents.clamp(2.0, 60.0);
    p.reverse_signal_threshold = p.reverse_signal_threshold.clamp(-0.95, -0.02);
    p.reverse_signal_ticks = p.reverse_signal_ticks.clamp(1, 12);
    p.trail_activate_profit_cents = p.trail_activate_profit_cents.clamp(2.0, 80.0);
    p.trail_drawdown_cents = p.trail_drawdown_cents.clamp(1.0, 50.0);
    p.take_profit_near_max_cents = p.take_profit_near_max_cents.clamp(70.0, 99.5);
    p.endgame_take_profit_cents = p.endgame_take_profit_cents.clamp(50.0, 99.0);
    p.endgame_remaining_ms = p.endgame_remaining_ms.clamp(1_000, 180_000);
    p.liquidity_widen_prob = p.liquidity_widen_prob.clamp(0.01, 0.2);
    p.cooldown_ms = p.cooldown_ms.clamp(0, 120_000);
    p.max_entries_per_round = p.max_entries_per_round.clamp(1, 16);
    p.max_exec_spread_cents = p.max_exec_spread_cents.clamp(0.2, 30.0);
    p.slippage_cents_per_side = p.slippage_cents_per_side.clamp(0.0, 10.0);
    p.fee_cents_per_side = 0.0;
    p.emergency_wide_spread_penalty_ratio = p.emergency_wide_spread_penalty_ratio.clamp(0.2, 3.0);
    p.stop_loss_grace_ticks = p.stop_loss_grace_ticks.clamp(0, 8);
    p.stop_loss_hard_mult = p.stop_loss_hard_mult.clamp(1.0, 3.0);
    p.stop_loss_reverse_extra_ticks = p.stop_loss_reverse_extra_ticks.clamp(0, 6);
    p.loss_cluster_limit = p.loss_cluster_limit.clamp(0, 8);
    p.loss_cluster_cooldown_ms = p.loss_cluster_cooldown_ms.clamp(0, 120_000);
    p.noise_gate_threshold_add = p.noise_gate_threshold_add.clamp(0.0, 0.20);
    p.noise_gate_edge_add = p.noise_gate_edge_add.clamp(0.0, 0.12);
    p.noise_gate_spread_scale = p.noise_gate_spread_scale.clamp(0.5, 1.2);
    p.vic_target_entries_per_hour = p.vic_target_entries_per_hour.clamp(0.0, 120.0);
    p.vic_deadband_ratio = p.vic_deadband_ratio.clamp(0.0, 0.8);
    p.vic_threshold_relax_max = p.vic_threshold_relax_max.clamp(0.0, 0.2);
    p.vic_edge_relax_max = p.vic_edge_relax_max.clamp(0.0, 0.1);
    p.vic_spread_relax_max = p.vic_spread_relax_max.clamp(0.0, 0.8);
    p
}
