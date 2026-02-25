#!/usr/bin/env python3
import argparse
import json
import math
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib import error, parse, request


def fetch_json(url: str, timeout_sec: int, retries: int = 2) -> dict:
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            with request.urlopen(url, timeout=timeout_sec) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            # Server-side failures can be transient under load.
            if exc.code >= 500 and attempt < retries:
                time.sleep(0.4 * (attempt + 1))
                last_exc = RuntimeError(
                    f"GET {url} failed: status={exc.code} body={body}"
                )
                continue
            raise RuntimeError(
                f"GET {url} failed: status={exc.code} body={body}"
            ) from exc
        except Exception as exc:  # network reset / remote close / timeout
            last_exc = exc
            if attempt < retries:
                time.sleep(0.4 * (attempt + 1))
                continue
            raise RuntimeError(f"GET {url} failed: {exc}") from exc
    if last_exc is None:
        raise RuntimeError(f"GET {url} failed")
    raise RuntimeError(f"GET {url} failed: {last_exc}") from last_exc


def post_json(url: str, payload: dict, timeout_sec: int, retries: int = 2) -> dict:
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        req = request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=timeout_sec) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            if exc.code >= 500 and attempt < retries:
                time.sleep(0.4 * (attempt + 1))
                last_exc = RuntimeError(
                    f"POST {url} failed: status={exc.code} body={body}"
                )
                continue
            raise RuntimeError(
                f"POST {url} failed: status={exc.code} body={body}"
            ) from exc
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(0.4 * (attempt + 1))
                continue
            raise RuntimeError(f"POST {url} failed: {exc}") from exc
    if last_exc is None:
        raise RuntimeError(f"POST {url} failed")
    raise RuntimeError(f"POST {url} failed: {last_exc}") from last_exc


def build_url(base_url: str, path: str, query: dict | None = None) -> str:
    if query:
        return f"{base_url}{path}?{parse.urlencode(query)}"
    return f"{base_url}{path}"


def score_row(row: dict) -> tuple:
    # Primary: latest win rate, then avg pnl, then total pnl.
    return (
        float(row.get("latest_win", 0.0)),
        float(row.get("avg_pnl", 0.0)),
        float(row.get("total_pnl", 0.0)),
    )


def to_float(value: object, fallback: float = 0.0) -> float:
    try:
        if value is None:
            return fallback
        return float(value)
    except (TypeError, ValueError):
        return fallback


def to_int(value: object, fallback: int = 0) -> int:
    try:
        if value is None:
            return fallback
        return int(value)
    except (TypeError, ValueError):
        return fallback


CONFIG_KEYS = [
    "entry_threshold_base",
    "entry_threshold_cap",
    "spread_limit_prob",
    "entry_edge_prob",
    "entry_min_potential_cents",
    "entry_max_price_cents",
    "min_hold_ms",
    "stop_loss_cents",
    "reverse_signal_threshold",
    "reverse_signal_ticks",
    "trail_activate_profit_cents",
    "trail_drawdown_cents",
    "take_profit_near_max_cents",
    "endgame_take_profit_cents",
    "endgame_remaining_ms",
    "liquidity_widen_prob",
    "cooldown_ms",
    "max_entries_per_round",
    "max_exec_spread_cents",
    "slippage_cents_per_side",
    "fee_cents_per_side",
    "emergency_wide_spread_penalty_ratio",
]


def normalize_config(raw: dict | None) -> dict:
    if not isinstance(raw, dict):
        return {}
    out: dict = {}
    for key in CONFIG_KEYS:
        if key not in raw:
            continue
        if key in {"reverse_signal_ticks", "max_entries_per_round"}:
            out[key] = to_int(raw.get(key))
        elif key in {"min_hold_ms", "endgame_remaining_ms", "cooldown_ms"}:
            out[key] = to_int(raw.get(key))
        else:
            out[key] = to_float(raw.get(key))
    return out


def compute_window_metrics(trades: list[dict], window: int) -> dict:
    tail = trades[-window:] if window > 0 else trades
    pnls = [to_float(t.get("pnl_cents")) for t in tail]
    trade_count = len(pnls)
    wins = sum(1 for p in pnls if p > 0.0)
    total_pnl = sum(pnls)
    avg_pnl = (total_pnl / trade_count) if trade_count > 0 else 0.0
    win_rate = (wins * 100.0 / trade_count) if trade_count > 0 else 0.0

    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for p in pnls:
        equity += p
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_drawdown:
            max_drawdown = dd
    gains = sum(p for p in pnls if p > 0.0)
    losses = -sum(p for p in pnls if p < 0.0)
    if losses > 0.0:
        profit_factor = gains / losses
    else:
        profit_factor = 99.0 if gains > 0.0 else 1.0

    return {
        "latest_trade_count": trade_count,
        "latest_win": win_rate,
        "latest_avg_pnl": avg_pnl,
        "latest_total_pnl": total_pnl,
        "latest_max_drawdown": max_drawdown,
        "latest_profit_factor": profit_factor,
    }


def score_eval(metrics: dict, target_win_rate: float, window_trades: int) -> float:
    latest_count = to_int(metrics.get("latest_trade_count"))
    coverage = min(1.0, latest_count / max(1.0, float(window_trades)))
    latest_win = to_float(metrics.get("latest_win"))
    latest_avg = to_float(metrics.get("latest_avg_pnl"))
    latest_total = to_float(metrics.get("latest_total_pnl"))
    full_win = to_float(metrics.get("full_win"))
    full_avg = to_float(metrics.get("full_avg_pnl"))
    latest_dd = to_float(metrics.get("latest_max_drawdown"))
    latest_pf = to_float(metrics.get("latest_profit_factor"), 1.0)
    blocked = to_float(metrics.get("blocked_exits"))
    emergency = to_float(metrics.get("emergency_wide_exit_count"))
    exec_penalty = to_float(metrics.get("execution_penalty_cents_total"))

    win_gap_penalty = max(0.0, target_win_rate - latest_win) * 18.0
    neg_pnl_penalty = max(0.0, -latest_avg) * 140.0 + max(0.0, -latest_total) * 1.0
    drawdown_penalty = latest_dd * 0.85
    fill_penalty = blocked * 4.0 + emergency * 6.0 + exec_penalty * 1.1
    pf_penalty = max(0.0, 1.0 - latest_pf) * 120.0
    pf_bonus = max(0.0, min(latest_pf, 3.0) - 1.0) * 18.0

    base = (
        latest_win * 2.0
        + latest_avg * 85.0
        + latest_total * 0.10
        + full_win * 0.40
        + full_avg * 30.0
    )
    score = (
        base * (0.55 + 0.45 * coverage)
        - win_gap_penalty
        - neg_pnl_penalty
        - drawdown_penalty
        - fill_penalty
        - pf_penalty
        + pf_bonus
    )
    if latest_count < max(8, int(window_trades * 0.5)):
        score -= 120.0
    return score


def evaluate_config(
    base_url: str,
    market_type: str,
    timeout_sec: int,
    target_win_rate: float,
    window_trades: int,
    lookback_minutes: int,
    max_trades: int,
    use_autotune: bool,
    config: dict | None,
    label: str,
) -> dict:
    query = {
        "market_type": market_type,
        "full_history": "true",
        "lookback_minutes": lookback_minutes,
        "max_trades": max_trades,
        "use_autotune": "true" if use_autotune else "false",
    }
    if config:
        query.update(config)
    payload = fetch_json(build_url(base_url, "/api/strategy/paper", query), timeout_sec)
    summary = payload.get("summary") or {}
    trades = payload.get("trades") or []
    metrics = compute_window_metrics(trades, window_trades)
    metrics.update(
        {
            "label": label,
            "market_type": market_type,
            "config_source": payload.get("config_source"),
            "full_trade_count": to_int(summary.get("trade_count")),
            "full_win": to_float(summary.get("win_rate_pct")),
            "full_avg_pnl": to_float(summary.get("avg_pnl_cents")),
            "full_total_pnl": to_float(summary.get("total_pnl_cents")),
            "blocked_exits": to_int(summary.get("blocked_exits")),
            "emergency_wide_exit_count": to_int(summary.get("emergency_wide_exit_count")),
            "execution_penalty_cents_total": to_float(summary.get("execution_penalty_cents_total")),
            "full_max_drawdown": to_float(summary.get("max_drawdown_cents")),
            "full_net_pnl": to_float(summary.get("net_pnl_cents")),
            "full_total_cost": to_float(summary.get("total_cost_cents")),
            "full_net_margin_pct": to_float(summary.get("net_margin_pct")),
        }
    )
    metrics["score"] = score_eval(metrics, target_win_rate, window_trades)
    return metrics


def config_shift_ratio(current_cfg: dict | None, challenger_cfg: dict | None) -> tuple[float, float]:
    current_cfg = current_cfg or {}
    challenger_cfg = challenger_cfg or {}
    keys = [k for k in CONFIG_KEYS if k in current_cfg and k in challenger_cfg]
    if not keys:
        return (0.0, 0.0)
    diffs: list[float] = []
    for k in keys:
        a = current_cfg.get(k)
        b = challenger_cfg.get(k)
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            denom = max(1.0, abs(float(a)), abs(float(b)))
            diffs.append(abs(float(a) - float(b)) / denom)
    if not diffs:
        return (0.0, 0.0)
    return (sum(diffs) / len(diffs), max(diffs))


def should_promote(
    challenger: dict,
    baseline: dict,
    args: argparse.Namespace,
    current_cfg: dict | None,
    challenger_cfg: dict | None,
    cooldown_ok: bool,
) -> tuple[bool, list[str], dict]:
    reasons: list[str] = []
    min_trade_count = max(10, int(args.window_trades * args.promote_min_coverage))
    if to_int(challenger.get("latest_trade_count")) < min_trade_count:
        reasons.append(f"challenger latest_trade_count<{min_trade_count}")
    if to_float(challenger.get("latest_win")) < args.promote_min_absolute_win:
        reasons.append("challenger latest_win below absolute floor")
    if (
        to_float(challenger.get("latest_win"))
        < to_float(baseline.get("latest_win")) + args.promote_min_win_lift
    ):
        reasons.append("challenger latest_win lift too small")
    if (
        to_float(challenger.get("latest_avg_pnl"))
        < to_float(baseline.get("latest_avg_pnl")) + args.promote_min_avg_pnl_lift
    ):
        reasons.append("challenger latest_avg_pnl lift too small")
    if (
        to_float(challenger.get("latest_total_pnl"))
        < to_float(baseline.get("latest_total_pnl")) + args.promote_min_total_pnl_lift
    ):
        reasons.append("challenger latest_total_pnl lift too small")
    if (
        to_float(challenger.get("latest_max_drawdown"))
        > to_float(baseline.get("latest_max_drawdown")) + args.promote_max_drawdown_lift
    ):
        reasons.append("challenger drawdown deterioration too large")
    if to_float(challenger.get("latest_profit_factor")) < args.promote_min_profit_factor:
        reasons.append("challenger latest_profit_factor below floor")
    if (
        to_float(challenger.get("latest_profit_factor"))
        < to_float(baseline.get("latest_profit_factor")) + args.promote_min_profit_factor_lift
    ):
        reasons.append("challenger latest_profit_factor lift too small")
    if abs(to_float(challenger.get("latest_win")) - to_float(challenger.get("full_win"))) > args.promote_max_win_divergence:
        reasons.append("challenger win divergence too large (overfit risk)")
    if abs(to_float(challenger.get("latest_avg_pnl")) - to_float(challenger.get("full_avg_pnl"))) > args.promote_max_avg_pnl_divergence:
        reasons.append("challenger avg_pnl divergence too large (overfit risk)")
    if not cooldown_ok:
        reasons.append("promote cooldown not elapsed")
    if normalize_config(current_cfg or {}) == normalize_config(challenger_cfg or {}):
        reasons.append("challenger config equals current config")
    avg_shift, max_shift = config_shift_ratio(current_cfg, challenger_cfg)
    if avg_shift > args.promote_max_param_shift:
        reasons.append("challenger avg param shift too large")
    if max_shift > args.promote_max_param_shift * 2.0:
        reasons.append("challenger max param shift too large")
    if to_float(challenger.get("score")) < to_float(baseline.get("score")) + args.promote_score_margin:
        reasons.append("challenger score margin too small")
    detail = {
        "avg_param_shift": avg_shift,
        "max_param_shift": max_shift,
        "cooldown_ok": cooldown_ok,
    }
    return (len(reasons) == 0, reasons, detail)


def extract_best(payload: dict) -> dict:
    best_payload = payload.get("best") or {}
    rolling = best_payload.get("rolling_window_recent") or best_payload.get("rolling_window") or {}
    summary = best_payload.get("summary_recent") or best_payload.get("summary") or {}
    return {
        "name": best_payload.get("name"),
        "objective": to_float(best_payload.get("objective")),
        "latest_win": to_float(rolling.get("latest_win_rate_pct")),
        "avg_pnl": to_float(summary.get("avg_pnl_cents")),
        "total_pnl": to_float(summary.get("total_pnl_cents")),
        "trade_count": to_int(summary.get("trade_count")),
        "pf_recent": to_float(
            best_payload.get("profit_factor_recent") or best_payload.get("profit_factor_validation")
        ),
        "config": normalize_config(best_payload.get("config") or {}),
        "raw_best": best_payload,
    }


def run_seed_optimize(
    base_url: str,
    base_query: dict,
    seed: int,
    iterations: int,
    timeout_sec: int,
) -> dict:
    q = dict(base_query)
    q["seed"] = seed
    q["iterations"] = iterations
    payload = fetch_json(build_url(base_url, "/api/strategy/optimize", q), timeout_sec)
    row = extract_best(payload)
    row["seed"] = seed
    return row


def run_seed_batch_parallel(
    base_url: str,
    base_query: dict,
    seeds: list[int],
    iterations: int,
    timeout_sec: int,
    workers: int,
) -> tuple[list[dict], list[dict]]:
    rows: list[dict] = []
    errors: list[dict] = []
    max_workers = max(1, min(workers, len(seeds)))
    if max_workers <= 1:
        for seed in seeds:
            try:
                rows.append(run_seed_optimize(base_url, base_query, seed, iterations, timeout_sec))
            except Exception as exc:
                errors.append({"seed": seed, "error": str(exc)})
        return rows, errors

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        fut_map = {
            ex.submit(run_seed_optimize, base_url, base_query, seed, iterations, timeout_sec): seed
            for seed in seeds
        }
        for fut in as_completed(fut_map):
            seed = fut_map[fut]
            try:
                rows.append(fut.result())
            except Exception as exc:
                errors.append({"seed": seed, "error": str(exc)})
    return rows, errors


def main() -> None:
    ap = argparse.ArgumentParser(description="Continuous strategy autotune loop for polyedge_forge")
    ap.add_argument("--base-url", default="http://127.0.0.1:9810")
    ap.add_argument("--market-type", default="5m")
    ap.add_argument("--target-win-rate", type=float, default=90.0)
    ap.add_argument("--window-trades", type=int, default=50)
    ap.add_argument("--recent-lookback-minutes", type=int, default=720)
    ap.add_argument("--recent-weight", type=float, default=0.68)
    ap.add_argument("--max-arms", type=int, default=16)
    ap.add_argument("--max-trades", type=int, default=700)
    ap.add_argument("--evaluation-lookback-minutes", type=int, default=720)
    ap.add_argument("--evaluation-max-trades", type=int, default=280)
    ap.add_argument("--seed-from", type=int, default=1)
    ap.add_argument("--seed-to", type=int, default=24)
    ap.add_argument("--iterations-sweep", type=int, default=1200)
    ap.add_argument("--iterations-persist", type=int, default=2200)
    ap.add_argument("--top-k-seeds", type=int, default=6)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--persist-ttl-sec", type=int, default=86400)
    ap.add_argument("--promote-min-coverage", type=float, default=0.70)
    ap.add_argument("--promote-min-absolute-win", type=float, default=78.0)
    ap.add_argument("--promote-min-win-lift", type=float, default=2.0)
    ap.add_argument("--promote-min-avg-pnl-lift", type=float, default=0.35)
    ap.add_argument("--promote-min-total-pnl-lift", type=float, default=10.0)
    ap.add_argument("--promote-max-drawdown-lift", type=float, default=2.5)
    ap.add_argument("--promote-min-profit-factor", type=float, default=1.03)
    ap.add_argument("--promote-min-profit-factor-lift", type=float, default=0.02)
    ap.add_argument("--promote-max-win-divergence", type=float, default=12.0)
    ap.add_argument("--promote-max-avg-pnl-divergence", type=float, default=2.5)
    ap.add_argument("--promote-max-param-shift", type=float, default=0.40)
    ap.add_argument("--promote-score-margin", type=float, default=8.0)
    ap.add_argument("--promote-confirm-cycles", type=int, default=2)
    ap.add_argument("--promote-cooldown-sec", type=int, default=1800)
    ap.add_argument("--history-limit", type=int, default=8)
    ap.add_argument("--rollback-consecutive-cycles", type=int, default=2)
    ap.add_argument("--rollback-floor-win", type=float, default=74.0)
    ap.add_argument("--rollback-floor-avg-pnl", type=float, default=0.0)
    ap.add_argument("--rollback-floor-total-pnl", type=float, default=0.0)
    ap.add_argument("--rollback-min-win-lift", type=float, default=2.0)
    ap.add_argument("--rollback-min-score-lift", type=float, default=6.0)
    ap.add_argument("--cycle-interval-sec", type=int, default=300)
    ap.add_argument("--timeout-sec", type=int, default=900)
    ap.add_argument("--max-cycles", type=int, default=0, help="0 means run forever")
    ap.add_argument("--report-file", default="tmp/autotune_loop/report.jsonl")
    args = ap.parse_args()

    report_path = Path(args.report_file)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    base_query = {
        "market_type": args.market_type,
        "target_win_rate": args.target_win_rate,
        "window_trades": args.window_trades,
        "max_arms": args.max_arms,
        "max_trades": args.max_trades,
        "full_history": "true",
        "recent_lookback_minutes": args.recent_lookback_minutes,
        "recent_weight": args.recent_weight,
    }

    cycle = 0
    underperform_streak = 0
    promote_streak = 0
    while True:
        cycle += 1
        started = int(time.time() * 1000)
        try:
            best = None
            rows = []
            optimize_errors = []
            seeds = list(range(args.seed_from, args.seed_to + 1))

            sweep_rows, sweep_errors = run_seed_batch_parallel(
                base_url=args.base_url,
                base_query=base_query,
                seeds=seeds,
                iterations=args.iterations_sweep,
                timeout_sec=args.timeout_sec,
                workers=args.workers,
            )
            rows.extend(sweep_rows)
            optimize_errors.extend(sweep_errors)
            for row in rows:
                if best is None or score_row(row) > score_row(best):
                    best = row

            if best is None:
                retry_seeds = seeds[: min(8, len(seeds))]
                retry_rows, retry_errors = run_seed_batch_parallel(
                    base_url=args.base_url,
                    base_query=base_query,
                    seeds=retry_seeds,
                    iterations=max(80, args.iterations_sweep // 4),
                    timeout_sec=args.timeout_sec,
                    workers=1,
                )
                rows.extend(retry_rows)
                optimize_errors.extend(retry_errors)
                for row in rows:
                    if best is None or score_row(row) > score_row(best):
                        best = row
                if best is None:
                    sample_errors = ", ".join(
                        str((e or {}).get("error", "")) for e in optimize_errors[:3]
                    )
                    raise RuntimeError(
                        f"all optimize sweeps failed (errors={len(optimize_errors)}; sample={sample_errors})"
                    )

            rows_sorted = sorted(rows, key=score_row, reverse=True)
            top_k = max(1, min(args.top_k_seeds, len(rows_sorted)))
            top_seed_rows = rows_sorted[:top_k]
            deep_seeds = sorted({to_int(r.get("seed")) for r in top_seed_rows if to_int(r.get("seed")) > 0})

            deep_rows, deep_errors = run_seed_batch_parallel(
                base_url=args.base_url,
                base_query=base_query,
                seeds=deep_seeds,
                iterations=args.iterations_persist,
                timeout_sec=args.timeout_sec,
                workers=args.workers,
            )
            optimize_errors.extend(deep_errors)
            if not deep_rows:
                deep_rows = [best]
            deep_rows_sorted = sorted(deep_rows, key=score_row, reverse=True)

            baseline_eval = evaluate_config(
                base_url=args.base_url,
                market_type=args.market_type,
                timeout_sec=args.timeout_sec,
                target_win_rate=args.target_win_rate,
                window_trades=args.window_trades,
                lookback_minutes=args.evaluation_lookback_minutes,
                max_trades=args.evaluation_max_trades,
                use_autotune=True,
                config=None,
                label="baseline_autotune",
            )
            autotune_latest = fetch_json(
                build_url(
                    args.base_url,
                    "/api/strategy/autotune/latest",
                    {"market_type": args.market_type},
                ),
                args.timeout_sec,
            )
            autotune_doc = ((autotune_latest or {}).get("data") or {}) if isinstance(autotune_latest, dict) else {}
            current_autotune_cfg = normalize_config((autotune_doc or {}).get("config") or {})
            current_saved_at_ms = to_int((autotune_doc or {}).get("saved_at_ms"))
            now_ms = int(time.time() * 1000)
            cooldown_remaining_ms = max(
                0,
                current_saved_at_ms + args.promote_cooldown_sec * 1000 - now_ms,
            )
            cooldown_ok = cooldown_remaining_ms <= 0

            challenger_candidates = []
            for idx, cand in enumerate(deep_rows_sorted):
                cfg = cand.get("config") or {}
                if not cfg:
                    continue
                eval_row = evaluate_config(
                    base_url=args.base_url,
                    market_type=args.market_type,
                    timeout_sec=args.timeout_sec,
                    target_win_rate=args.target_win_rate,
                    window_trades=args.window_trades,
                    lookback_minutes=args.evaluation_lookback_minutes,
                    max_trades=args.evaluation_max_trades,
                    use_autotune=False,
                    config=cfg,
                    label=f"challenger_{idx+1}",
                )
                challenger_candidates.append(
                    {
                        "seed": cand.get("seed"),
                        "opt": cand,
                        "eval": eval_row,
                    }
                )
            if not challenger_candidates:
                raise RuntimeError("no challenger candidates after deep search")

            challenger_candidates.sort(
                key=lambda x: (
                    to_float((x.get("eval") or {}).get("score")),
                    to_float((x.get("eval") or {}).get("latest_win")),
                    to_float((x.get("eval") or {}).get("latest_avg_pnl")),
                ),
                reverse=True,
            )
            chosen = challenger_candidates[0]
            chosen_seed = to_int(chosen.get("seed"))
            chosen_opt = chosen.get("opt") or {}
            challenger_eval = chosen.get("eval") or {}
            challenger_config = (chosen_opt.get("config") or {})

            promote_candidate_ok, promote_reasons, promote_detail = should_promote(
                challenger_eval,
                baseline_eval,
                args,
                current_autotune_cfg,
                challenger_config,
                cooldown_ok,
            )
            promote_streak = (promote_streak + 1) if promote_candidate_ok else 0
            promote_ok = promote_candidate_ok and promote_streak >= args.promote_confirm_cycles
            if promote_candidate_ok and not promote_ok:
                promote_reasons = [
                    *promote_reasons,
                    f"promote confirmation not reached ({promote_streak}/{args.promote_confirm_cycles})",
                ]
            promote_resp = {"ok": False}
            promoted = False
            if promote_ok:
                promote_payload = {
                    "market_type": args.market_type,
                    "config": challenger_config,
                    "ttl_sec": args.persist_ttl_sec,
                    "source": "autotune_loop_challenger",
                    "note": (
                        f"cycle={cycle}; seed={chosen_seed}; "
                        f"objective={to_float(chosen_opt.get('objective')):.4f}"
                    ),
                }
                promote_resp = post_json(
                    build_url(args.base_url, "/api/strategy/autotune/set"),
                    promote_payload,
                    args.timeout_sec,
                )
                promoted = bool(promote_resp.get("ok"))
                if promoted:
                    promote_streak = 0

            live_eval = evaluate_config(
                base_url=args.base_url,
                market_type=args.market_type,
                timeout_sec=args.timeout_sec,
                target_win_rate=args.target_win_rate,
                window_trades=args.window_trades,
                lookback_minutes=args.evaluation_lookback_minutes,
                max_trades=args.evaluation_max_trades,
                use_autotune=True,
                config=None,
                label="live_after_promote" if promoted else "live",
            )

            underperform = (
                to_float(live_eval.get("latest_win")) < args.rollback_floor_win
                or to_float(live_eval.get("latest_avg_pnl")) <= args.rollback_floor_avg_pnl
                or to_float(live_eval.get("latest_total_pnl")) <= args.rollback_floor_total_pnl
            )
            underperform_streak = (underperform_streak + 1) if underperform else 0

            rollback_resp = {"ok": False}
            rollback_candidate = None
            rollback_done = False
            history_items: list = []

            if underperform_streak >= args.rollback_consecutive_cycles:
                history_payload = fetch_json(
                    build_url(
                        args.base_url,
                        "/api/strategy/autotune/history",
                        {"market_type": args.market_type, "limit": args.history_limit},
                    ),
                    args.timeout_sec,
                )
                history_items = history_payload.get("items") or []
                best_hist_eval = None
                best_hist_item = None
                for item in history_items:
                    hist_cfg = normalize_config((item or {}).get("config") or {})
                    if not hist_cfg:
                        continue
                    hist_eval = evaluate_config(
                        base_url=args.base_url,
                        market_type=args.market_type,
                        timeout_sec=args.timeout_sec,
                        target_win_rate=args.target_win_rate,
                        window_trades=args.window_trades,
                        lookback_minutes=args.evaluation_lookback_minutes,
                        max_trades=args.evaluation_max_trades,
                        use_autotune=False,
                        config=hist_cfg,
                        label="history_candidate",
                    )
                    if best_hist_eval is None or to_float(hist_eval.get("score")) > to_float(best_hist_eval.get("score")):
                        best_hist_eval = hist_eval
                        best_hist_item = item

                if best_hist_eval is not None and best_hist_item is not None:
                    rollback_candidate = {
                        "eval": best_hist_eval,
                        "item": best_hist_item,
                    }
                    rollback_ok = (
                        to_float(best_hist_eval.get("latest_win"))
                        >= to_float(live_eval.get("latest_win")) + args.rollback_min_win_lift
                        and to_float(best_hist_eval.get("score"))
                        >= to_float(live_eval.get("score")) + args.rollback_min_score_lift
                    )
                    if rollback_ok:
                        rollback_payload = {
                            "market_type": args.market_type,
                            "config": normalize_config(best_hist_item.get("config") or {}),
                            "ttl_sec": args.persist_ttl_sec,
                            "source": "autotune_loop_rollback",
                            "note": f"cycle={cycle}; rollback_from_underperform_streak={underperform_streak}",
                        }
                        rollback_resp = post_json(
                            build_url(args.base_url, "/api/strategy/autotune/set"),
                            rollback_payload,
                            args.timeout_sec,
                        )
                        rollback_done = bool(rollback_resp.get("ok"))
                        if rollback_done:
                            underperform_streak = 0

            finished = int(time.time() * 1000)
            report = {
                "cycle": cycle,
                "started_ms": started,
                "finished_ms": finished,
                "duration_ms": finished - started,
                "best_seed_sweep": best,
                "top_seed_rows": top_seed_rows,
                "deep_rows": deep_rows_sorted,
                "chosen_challenger_seed": chosen_seed,
                "chosen_challenger_opt": chosen_opt,
                "baseline_eval": baseline_eval,
                "challenger_eval": challenger_eval,
                "challenger_candidates": challenger_candidates,
                "live_eval": live_eval,
                "promote": {
                    "candidate_ok": promote_candidate_ok,
                    "ok": promote_ok,
                    "reasons": promote_reasons,
                    "promoted": promoted,
                    "streak": promote_streak,
                    "confirm_cycles": args.promote_confirm_cycles,
                    "cooldown_sec": args.promote_cooldown_sec,
                    "cooldown_remaining_ms": cooldown_remaining_ms,
                    "detail": promote_detail,
                    "response": promote_resp,
                },
                "current_autotune": {
                    "found": bool((autotune_latest or {}).get("found")),
                    "saved_at_ms": current_saved_at_ms,
                    "config": current_autotune_cfg,
                },
                "underperform": underperform,
                "underperform_streak": underperform_streak,
                "rollback": {
                    "done": rollback_done,
                    "response": rollback_resp,
                    "candidate": rollback_candidate,
                },
                "errors": optimize_errors,
                "rows": rows,
            }
            with report_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(report, ensure_ascii=False) + "\n")

            print(
                f"[cycle={cycle}] best_seed={best['seed']} chosen={chosen_seed} "
                f"base_win={to_float(baseline_eval.get('latest_win')):.2f} "
                f"chal_win={to_float(challenger_eval.get('latest_win')):.2f} "
                f"promote_ok={promote_ok} promoted={promoted} "
                f"cooldown_ms={cooldown_remaining_ms} confirm={promote_streak}/{args.promote_confirm_cycles} "
                f"rollback={rollback_done} "
                f"live_win={to_float(live_eval.get('latest_win')):.2f} "
                f"streak={underperform_streak}",
                flush=True,
            )
        except Exception as exc:
            finished = int(time.time() * 1000)
            report = {
                "cycle": cycle,
                "started_ms": started,
                "finished_ms": finished,
                "duration_ms": finished - started,
                "error": str(exc),
                "traceback": traceback.format_exc(limit=3),
            }
            with report_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(report, ensure_ascii=False) + "\n")
            print(f"[cycle={cycle}] error={exc}", flush=True)

        if args.max_cycles > 0 and cycle >= args.max_cycles:
            break
        time.sleep(max(5, args.cycle_interval_sec))


if __name__ == "__main__":
    main()
