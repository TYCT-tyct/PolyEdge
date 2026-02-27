#!/usr/bin/env python3
import argparse
import concurrent.futures
import json
import random
import time
import urllib.parse
import urllib.request
from pathlib import Path

PARAM_KEYS = [
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

BOUNDS = {
    "entry_threshold_base": (0.40, 0.95),
    "entry_threshold_cap": (0.45, 0.99),
    "spread_limit_prob": (0.005, 0.05),
    "entry_edge_prob": (0.002, 0.25),
    "entry_min_potential_cents": (1.0, 70.0),
    "entry_max_price_cents": (45.0, 98.5),
    "min_hold_ms": (0, 25_000),
    "stop_loss_cents": (2.0, 60.0),
    "reverse_signal_threshold": (-0.95, -0.02),
    "reverse_signal_ticks": (1, 12),
    "trail_activate_profit_cents": (2.0, 80.0),
    "trail_drawdown_cents": (1.0, 50.0),
    "take_profit_near_max_cents": (70.0, 99.5),
    "endgame_take_profit_cents": (50.0, 99.0),
    "endgame_remaining_ms": (1_000, 45_000),
    "liquidity_widen_prob": (0.02, 0.20),
    "cooldown_ms": (0, 20_000),
    "max_entries_per_round": (1, 3),
    "max_exec_spread_cents": (0.8, 3.4),
    "slippage_cents_per_side": (0.03, 0.35),
    "fee_cents_per_side": (0.03, 0.45),
    "emergency_wide_spread_penalty_ratio": (0.0, 0.8),
}

INT_KEYS = {
    "min_hold_ms",
    "reverse_signal_ticks",
    "endgame_remaining_ms",
    "cooldown_ms",
    "max_entries_per_round",
}

STEPS = {
    "entry_threshold_base": 0.05,
    "entry_threshold_cap": 0.05,
    "spread_limit_prob": 0.005,
    "entry_edge_prob": 0.012,
    "entry_min_potential_cents": 3.0,
    "entry_max_price_cents": 4.0,
    "min_hold_ms": 2600,
    "stop_loss_cents": 2.0,
    "reverse_signal_threshold": 0.09,
    "trail_activate_profit_cents": 2.5,
    "trail_drawdown_cents": 1.2,
    "take_profit_near_max_cents": 2.5,
    "endgame_take_profit_cents": 2.8,
    "endgame_remaining_ms": 3200,
    "liquidity_widen_prob": 0.015,
    "cooldown_ms": 2000,
    "max_exec_spread_cents": 0.3,
    "slippage_cents_per_side": 0.02,
    "fee_cents_per_side": 0.02,
    "emergency_wide_spread_penalty_ratio": 0.08,
}


def clamp_param(k: str, v: float | int) -> float | int:
    lo, hi = BOUNDS[k]
    if k in INT_KEYS:
        return int(max(lo, min(hi, round(float(v)))))
    return max(lo, min(hi, float(v)))


def only_params(cfg: dict) -> dict:
    out: dict = {}
    for k in PARAM_KEYS:
        if k in cfg:
            out[k] = cfg[k]
    return out


def fetch_payload(base_url: str, market_type: str, lookback_minutes: int, max_trades: int, cfg: dict, timeout: int, retries: int) -> dict:
    q: dict[str, str] = {
        "market_type": market_type,
        "full_history": "true",
        "lookback_minutes": str(lookback_minutes),
        "max_trades": str(max_trades),
        "use_autotune": "false",
    }
    for k, v in cfg.items():
        q[k] = str(v)
    url = f"{base_url}/api/strategy/paper?{urllib.parse.urlencode(q)}"
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(0.35 * (attempt + 1))
                continue
            raise
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("payload fetch failed")


def summarize_payload(payload: dict) -> dict:
    summary = payload.get("summary") or {}
    trades = payload.get("trades") or []
    pnls = [float(t.get("pnl_cents") or 0.0) for t in trades]
    n = len(pnls)
    win_rate = (sum(1 for p in pnls if p > 0.0) * 100.0 / n) if n else 0.0
    avg = (sum(pnls) / n) if n else 0.0
    tail = pnls[-80:] if n >= 80 else pnls
    tail_win = (sum(1 for p in tail if p > 0.0) * 100.0 / len(tail)) if tail else 0.0
    tail_avg = (sum(tail) / len(tail)) if tail else 0.0
    return {
        "trade_count": n,
        "win_rate_pct": float(summary.get("win_rate_pct") or win_rate),
        "avg_pnl_cents": float(summary.get("avg_pnl_cents") or avg),
        "net_pnl_cents": float(summary.get("net_pnl_cents") or summary.get("total_pnl_cents") or sum(pnls)),
        "max_drawdown_cents": float(summary.get("max_drawdown_cents") or 0.0),
        "last80_win_rate_pct": tail_win,
        "last80_avg_pnl_cents": tail_avg,
    }


def score_window(metrics: dict, min_trades: int, trade_target: int, win_floor: float) -> float:
    n = metrics["trade_count"]
    wr = metrics["win_rate_pct"]
    tw = metrics["last80_win_rate_pct"]
    avg = metrics["avg_pnl_cents"]
    tavg = metrics["last80_avg_pnl_cents"]
    net = metrics["net_pnl_cents"]
    dd = metrics["max_drawdown_cents"]
    participation = min(max(n, 0) / max(trade_target, 1), 1.2)
    sample_penalty = max(0, min_trades - n) * 6.0
    wr_penalty = max(0.0, win_floor - wr) * 22.0 + max(0.0, (win_floor - 2.0) - tw) * 16.0
    return (
        wr * 5.2
        + tw * 3.5
        + avg * 2.2
        + tavg * 1.8
        + max(0.0, net) * 0.015
        + participation * 150.0
        - dd * 0.9
        - sample_penalty
        - wr_penalty
    )


def evaluate_cfg(base_url: str, market_type: str, lookbacks: list[int], max_trades: int, cfg: dict, min_trades: int, trade_target: int, win_floor: float, timeout: int, retries: int) -> dict:
    windows = []
    total_score = 0.0
    for lb in lookbacks:
        payload = fetch_payload(base_url, market_type, lb, max_trades, cfg, timeout, retries)
        metrics = summarize_payload(payload)
        metrics["lookback_minutes"] = lb
        w_score = score_window(metrics, min_trades=min_trades, trade_target=trade_target, win_floor=win_floor)
        metrics["window_score"] = w_score
        windows.append(metrics)
        total_score += w_score
    avg_net = sum(w["net_pnl_cents"] for w in windows) / len(windows)
    avg_wr = sum(w["win_rate_pct"] for w in windows) / len(windows)
    avg_dd = sum(w["max_drawdown_cents"] for w in windows) / len(windows)
    avg_n = sum(w["trade_count"] for w in windows) / len(windows)
    return {
        "score": total_score / max(1, len(windows)),
        "avg_net_pnl_cents": avg_net,
        "avg_win_rate_pct": avg_wr,
        "avg_max_drawdown_cents": avg_dd,
        "avg_trade_count": avg_n,
        "windows": windows,
    }


def mutate(cfg: dict, scale: float) -> dict:
    c = dict(cfg)
    for k in PARAM_KEYS:
        if k not in c:
            continue
        if k in {"reverse_signal_ticks", "max_entries_per_round"}:
            if random.random() < 0.25:
                c[k] = clamp_param(k, float(c[k]) + random.choice([-1, 1]))
            continue
        step = STEPS.get(k, 0.1)
        c[k] = clamp_param(k, float(c[k]) + random.uniform(-step, step) * scale)
    c["entry_threshold_cap"] = clamp_param(
        "entry_threshold_cap",
        max(float(c["entry_threshold_cap"]), float(c["entry_threshold_base"]) + 0.03),
    )
    return c


def build_candidates(seed: dict, best_cfg: dict, elites: list[dict], population: int, scale: float) -> list[dict]:
    candidates = [dict(best_cfg), dict(seed)]
    while len(candidates) < population:
        r = random.random()
        if elites and r < 0.55:
            base = random.choice(elites)
        elif r < 0.85:
            base = best_cfg
        else:
            base = seed
        candidates.append(mutate(base, scale))
    return candidates[:population]


def save_checkpoint(path: Path, generation: int, best_cfg: dict, best_metrics: dict, seed_metrics: dict) -> None:
    payload = {
        "generation": generation,
        "best_cfg": best_cfg,
        "best_metrics": best_metrics,
        "seed_metrics": seed_metrics,
        "saved_at_ms": int(time.time() * 1000),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Parallel guarded parameter search for /api/strategy/paper (autotune off).")
    ap.add_argument("--base-url", default="http://127.0.0.1:9810")
    ap.add_argument("--market-type", default="5m")
    ap.add_argument("--seed-config", required=True)
    ap.add_argument("--lookbacks", default="1440,2880")
    ap.add_argument("--max-trades", type=int, default=900)
    ap.add_argument("--generations", type=int, default=36)
    ap.add_argument("--population", type=int, default=16)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--min-trades", type=int, default=100)
    ap.add_argument("--trade-target", type=int, default=180)
    ap.add_argument("--win-floor", type=float, default=85.0)
    ap.add_argument("--timeout", type=int, default=240)
    ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--checkpoint", default="/tmp/parallel_search_checkpoint.json")
    ap.add_argument("--out", default="/tmp/parallel_search_result.json")
    ap.add_argument("--seed", type=int, default=20260227)
    args = ap.parse_args()

    random.seed(args.seed)
    lookbacks = [int(x.strip()) for x in args.lookbacks.split(",") if x.strip()]
    if not lookbacks:
        raise ValueError("lookbacks cannot be empty")

    raw_seed = json.loads(Path(args.seed_config).read_text(encoding="utf-8"))
    seed_cfg = only_params(raw_seed)
    if not seed_cfg:
        raise ValueError("seed config contains no strategy params")

    seed_metrics = evaluate_cfg(
        args.base_url,
        args.market_type,
        lookbacks,
        args.max_trades,
        seed_cfg,
        args.min_trades,
        args.trade_target,
        args.win_floor,
        args.timeout,
        args.retries,
    )
    best_cfg = dict(seed_cfg)
    best_metrics = seed_metrics
    elites = [dict(best_cfg)]
    save_checkpoint(Path(args.checkpoint), 0, best_cfg, best_metrics, seed_metrics)
    print("BASE", json.dumps(seed_metrics, ensure_ascii=False))

    for gen in range(1, args.generations + 1):
        if gen < int(args.generations * 0.45):
            scale = 1.0
        elif gen < int(args.generations * 0.80):
            scale = 0.6
        else:
            scale = 0.3

        candidates = build_candidates(seed_cfg, best_cfg, elites, args.population, scale)
        evaluated: list[tuple[dict, dict]] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
            fut_map = {
                ex.submit(
                    evaluate_cfg,
                    args.base_url,
                    args.market_type,
                    lookbacks,
                    args.max_trades,
                    cfg,
                    args.min_trades,
                    args.trade_target,
                    args.win_floor,
                    args.timeout,
                    args.retries,
                ): cfg
                for cfg in candidates
            }
            for fut in concurrent.futures.as_completed(fut_map):
                cfg = fut_map[fut]
                try:
                    m = fut.result()
                except Exception:
                    continue
                evaluated.append((cfg, m))
        if not evaluated:
            continue
        evaluated.sort(
            key=lambda x: (
                x[1]["score"],
                x[1]["avg_win_rate_pct"],
                x[1]["avg_net_pnl_cents"],
                -x[1]["avg_max_drawdown_cents"],
                x[1]["avg_trade_count"],
            ),
            reverse=True,
        )
        elites = [cfg for cfg, _ in evaluated[: max(2, min(6, len(evaluated)))]]
        gen_best_cfg, gen_best_metrics = evaluated[0]
        if (
            gen_best_metrics["score"],
            gen_best_metrics["avg_win_rate_pct"],
            gen_best_metrics["avg_net_pnl_cents"],
            -gen_best_metrics["avg_max_drawdown_cents"],
            gen_best_metrics["avg_trade_count"],
        ) > (
            best_metrics["score"],
            best_metrics["avg_win_rate_pct"],
            best_metrics["avg_net_pnl_cents"],
            -best_metrics["avg_max_drawdown_cents"],
            best_metrics["avg_trade_count"],
        ):
            best_cfg = dict(gen_best_cfg)
            best_metrics = gen_best_metrics
            save_checkpoint(Path(args.checkpoint), gen, best_cfg, best_metrics, seed_metrics)
            print(
                "NEW",
                gen,
                json.dumps(
                    {
                        "score": round(best_metrics["score"], 2),
                        "avg_wr": round(best_metrics["avg_win_rate_pct"], 2),
                        "avg_net": round(best_metrics["avg_net_pnl_cents"], 2),
                        "avg_dd": round(best_metrics["avg_max_drawdown_cents"], 2),
                        "avg_n": round(best_metrics["avg_trade_count"], 2),
                    },
                    ensure_ascii=False,
                ),
            )

    result = {
        "search": {
            "market_type": args.market_type,
            "lookbacks": lookbacks,
            "max_trades": args.max_trades,
            "generations": args.generations,
            "population": args.population,
            "workers": args.workers,
            "min_trades": args.min_trades,
            "trade_target": args.trade_target,
            "win_floor": args.win_floor,
            "seed": args.seed,
        },
        "seed_cfg": seed_cfg,
        "seed_metrics": seed_metrics,
        "best_cfg": best_cfg,
        "best_metrics": best_metrics,
        "saved_at_ms": int(time.time() * 1000),
    }
    Path(args.out).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print("FINAL", json.dumps(best_metrics, ensure_ascii=False))
    print("CFG", json.dumps(best_cfg, ensure_ascii=False))


if __name__ == "__main__":
    main()
