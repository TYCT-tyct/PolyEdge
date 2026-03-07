#!/usr/bin/env python3
import argparse
import concurrent.futures
import datetime as dt
import glob
import json
import math
import random
import statistics
import time
import urllib.parse
import urllib.request
from pathlib import Path

PARAM_SPECS: dict[str, dict] = {
    "entry_threshold_base": {"type": "float", "lo": 0.40, "hi": 0.95, "step": 0.04},
    "entry_threshold_cap": {"type": "float", "lo": 0.45, "hi": 0.99, "step": 0.04},
    "spread_limit_prob": {"type": "float", "lo": 0.005, "hi": 0.12, "step": 0.006},
    "entry_edge_prob": {"type": "float", "lo": 0.002, "hi": 0.25, "step": 0.012},
    "entry_min_potential_cents": {"type": "float", "lo": 1.0, "hi": 70.0, "step": 3.2},
    "entry_max_price_cents": {"type": "float", "lo": 45.0, "hi": 98.5, "step": 3.5},
    "min_hold_ms": {"type": "int", "lo": 0, "hi": 240_000, "step": 8_000},
    "stop_loss_cents": {"type": "float", "lo": 2.0, "hi": 60.0, "step": 2.2},
    "reverse_signal_threshold": {"type": "float", "lo": -0.95, "hi": -0.02, "step": 0.08},
    "reverse_signal_ticks": {"type": "int", "lo": 1, "hi": 12, "step": 1},
    "trail_activate_profit_cents": {"type": "float", "lo": 2.0, "hi": 80.0, "step": 3.0},
    "trail_drawdown_cents": {"type": "float", "lo": 1.0, "hi": 50.0, "step": 1.6},
    "take_profit_near_max_cents": {"type": "float", "lo": 70.0, "hi": 99.5, "step": 1.8},
    "endgame_take_profit_cents": {"type": "float", "lo": 50.0, "hi": 99.0, "step": 2.4},
    "endgame_remaining_ms": {"type": "int", "lo": 1_000, "hi": 180_000, "step": 8_000},
    "liquidity_widen_prob": {"type": "float", "lo": 0.01, "hi": 0.2, "step": 0.015},
    "cooldown_ms": {"type": "int", "lo": 0, "hi": 120_000, "step": 7_000},
    "max_entries_per_round": {"type": "int", "lo": 1, "hi": 16, "step": 1},
    "max_exec_spread_cents": {"type": "float", "lo": 0.2, "hi": 30.0, "step": 0.9},
    "slippage_cents_per_side": {"type": "float", "lo": 0.0, "hi": 10.0, "step": 0.15},
    "emergency_wide_spread_penalty_ratio": {"type": "float", "lo": 0.2, "hi": 3.0, "step": 0.15},
    "stop_loss_grace_ticks": {"type": "int", "lo": 0, "hi": 8, "step": 1},
    "stop_loss_hard_mult": {"type": "float", "lo": 1.0, "hi": 3.0, "step": 0.1},
    "stop_loss_reverse_extra_ticks": {"type": "int", "lo": 0, "hi": 6, "step": 1},
    "loss_cluster_limit": {"type": "int", "lo": 0, "hi": 8, "step": 1},
    "loss_cluster_cooldown_ms": {"type": "int", "lo": 0, "hi": 120_000, "step": 7_000},
    "noise_gate_enabled": {"type": "bool"},
    "noise_gate_threshold_add": {"type": "float", "lo": 0.0, "hi": 0.20, "step": 0.012},
    "noise_gate_edge_add": {"type": "float", "lo": 0.0, "hi": 0.12, "step": 0.008},
    "noise_gate_spread_scale": {"type": "float", "lo": 0.5, "hi": 1.2, "step": 0.05},
    "vic_enabled": {"type": "bool"},
    "vic_target_entries_per_hour": {"type": "float", "lo": 0.0, "hi": 120.0, "step": 5.0},
    "vic_deadband_ratio": {"type": "float", "lo": 0.0, "hi": 0.8, "step": 0.04},
    "vic_threshold_relax_max": {"type": "float", "lo": 0.0, "hi": 0.2, "step": 0.01},
    "vic_edge_relax_max": {"type": "float", "lo": 0.0, "hi": 0.1, "step": 0.006},
    "vic_spread_relax_max": {"type": "float", "lo": 0.0, "hi": 0.8, "step": 0.04},
}


def clamp_value(k: str, v):
    spec = PARAM_SPECS[k]
    t = spec["type"]
    if t == "bool":
        return bool(v)
    lo, hi = spec["lo"], spec["hi"]
    if t == "int":
        return int(max(lo, min(hi, round(float(v)))))
    return max(lo, min(hi, float(v)))


def sanitize_cfg(cfg: dict) -> dict:
    out = {}
    for k in PARAM_SPECS:
        if k in cfg:
            out[k] = clamp_value(k, cfg[k])
    # Forge paper currently ignores request fee_cents_per_side and forces its own fee model.
    out["fee_cents_per_side"] = 0.0
    if "entry_threshold_base" in out and "entry_threshold_cap" in out:
        min_cap = float(out["entry_threshold_base"]) + 0.01
        out["entry_threshold_cap"] = max(float(out["entry_threshold_cap"]), min_cap)
        out["entry_threshold_cap"] = clamp_value("entry_threshold_cap", out["entry_threshold_cap"])
    return out


def load_cfg(path: str) -> dict:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return sanitize_cfg(raw)


def fetch_payload(
    base_url: str,
    symbol: str,
    market_type: str,
    lookback_minutes: int,
    max_trades: int,
    max_samples: int,
    cfg: dict,
    timeout: int,
    retries: int,
) -> dict:
    q: dict[str, str] = {
        "symbol": symbol,
        "market_type": market_type,
        "lookback_minutes": str(lookback_minutes),
        "max_trades": str(max_trades),
        "max_samples": str(max_samples),
        "full_history": "false",
    }
    for k, v in cfg.items():
        q[k] = str(v).lower() if isinstance(v, bool) else str(v)
    url = f"{base_url}/api/strategy/paper?{urllib.parse.urlencode(q)}"
    last_exc: Exception | None = None
    for i in range(retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            last_exc = exc
            if i < retries:
                time.sleep(0.30 * (i + 1))
                continue
            raise
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("fetch failed")


def summarize_payload(payload: dict) -> dict:
    summary = payload.get("summary") or {}
    trades = payload.get("trades") or []
    pnls = [float(t.get("pnl_cents") or 0.0) for t in trades]
    n_list = len(pnls)
    n = int(summary.get("trade_count") or n_list)
    win_rate = float(summary.get("win_rate_pct") or 0.0)
    avg_pnl = float(summary.get("avg_pnl_cents") or 0.0)
    net = float(summary.get("net_pnl_cents") or summary.get("total_pnl_cents") or sum(pnls))
    dd = float(summary.get("max_drawdown_cents") or 0.0)
    margin = float(summary.get("net_margin_pct") or 0.0)

    tail50 = pnls[-50:] if len(pnls) >= 50 else pnls
    tail80 = pnls[-80:] if len(pnls) >= 80 else pnls
    t50_win = (sum(1 for x in tail50 if x > 0.0) * 100.0 / len(tail50)) if tail50 else win_rate
    t80_win = (sum(1 for x in tail80 if x > 0.0) * 100.0 / len(tail80)) if tail80 else win_rate
    t50_avg = (sum(tail50) / len(tail50)) if tail50 else avg_pnl
    t80_avg = (sum(tail80) / len(tail80)) if tail80 else avg_pnl
    return {
        "trade_count": n,
        "win_rate_pct": win_rate,
        "avg_pnl_cents": avg_pnl,
        "net_pnl_cents": net,
        "max_drawdown_cents": dd,
        "net_margin_pct": margin,
        "last50_win_rate_pct": t50_win,
        "last80_win_rate_pct": t80_win,
        "last50_avg_pnl_cents": t50_avg,
        "last80_avg_pnl_cents": t80_avg,
    }


def score_window(
    m: dict,
    min_trades: int,
    trade_target: int,
    win_target: float,
    dd_target: float,
) -> float:
    n = m["trade_count"]
    wr = m["win_rate_pct"]
    t50w = m["last50_win_rate_pct"]
    t80w = m["last80_win_rate_pct"]
    avg = m["avg_pnl_cents"]
    t50avg = m["last50_avg_pnl_cents"]
    t80avg = m["last80_avg_pnl_cents"]
    net = m["net_pnl_cents"]
    dd = m["max_drawdown_cents"]
    margin = m["net_margin_pct"]

    participation = min(max(n, 0) / max(trade_target, 1), 1.25)
    wr_penalty = max(0.0, win_target - wr) * 22.0 + max(0.0, (win_target - 1.5) - t80w) * 14.0
    sample_penalty = max(0, min_trades - n) * 6.5
    dd_penalty = max(0.0, dd - dd_target) * 2.8 + dd * 0.8
    net_bonus = max(0.0, net) * 0.018
    return (
        wr * 4.8
        + t50w * 2.4
        + t80w * 3.5
        + avg * 2.1
        + t50avg * 1.2
        + t80avg * 1.8
        + margin * 0.8
        + participation * 170.0
        + net_bonus
        - wr_penalty
        - sample_penalty
        - dd_penalty
    )


def evaluate_cfg(
    base_url: str,
    symbol: str,
    market_type: str,
    lookbacks: list[int],
    max_trades: int,
    max_samples: int,
    cfg: dict,
    min_trades: int,
    trade_target: int,
    win_target: float,
    dd_target: float,
    timeout: int,
    retries: int,
) -> dict:
    windows = []
    scores = []
    for lb in lookbacks:
        payload = fetch_payload(
            base_url=base_url,
            symbol=symbol,
            market_type=market_type,
            lookback_minutes=lb,
            max_trades=max_trades,
            max_samples=max_samples,
            cfg=cfg,
            timeout=timeout,
            retries=retries,
        )
        m = summarize_payload(payload)
        m["lookback_minutes"] = lb
        ws = score_window(
            m=m,
            min_trades=min_trades,
            trade_target=trade_target,
            win_target=win_target,
            dd_target=dd_target,
        )
        m["window_score"] = ws
        windows.append(m)
        scores.append(ws)

    avg = {
        "avg_trade_count": sum(x["trade_count"] for x in windows) / len(windows),
        "avg_win_rate_pct": sum(x["win_rate_pct"] for x in windows) / len(windows),
        "avg_net_pnl_cents": sum(x["net_pnl_cents"] for x in windows) / len(windows),
        "avg_max_drawdown_cents": sum(x["max_drawdown_cents"] for x in windows) / len(windows),
        "avg_net_margin_pct": sum(x["net_margin_pct"] for x in windows) / len(windows),
    }

    # Robustness penalty: avoid overfitting to one window.
    wr_std = statistics.pstdev([x["win_rate_pct"] for x in windows]) if len(windows) > 1 else 0.0
    net_std = statistics.pstdev([x["net_pnl_cents"] for x in windows]) if len(windows) > 1 else 0.0
    dd_std = statistics.pstdev([x["max_drawdown_cents"] for x in windows]) if len(windows) > 1 else 0.0
    min_wr = min(x["win_rate_pct"] for x in windows)
    min_net = min(x["net_pnl_cents"] for x in windows)
    max_dd = max(x["max_drawdown_cents"] for x in windows)

    robust_score = (
        (sum(scores) / len(scores))
        - wr_std * 2.0
        - dd_std * 1.0
        - max(0.0, net_std) * 0.025
        + min_wr * 0.7
        + max(0.0, min_net) * 0.008
        - max_dd * 0.45
    )
    return {
        "score": robust_score,
        "wr_std": wr_std,
        "net_std": net_std,
        "dd_std": dd_std,
        "min_win_rate_pct": min_wr,
        "min_net_pnl_cents": min_net,
        "max_drawdown_cents_max_window": max_dd,
        **avg,
        "windows": windows,
    }


def mutate_value(k: str, cur, scale: float):
    spec = PARAM_SPECS[k]
    t = spec["type"]
    if t == "bool":
        if random.random() < 0.12 * scale:
            return not bool(cur)
        return bool(cur)
    if t == "int":
        step = max(1, int(round(spec["step"] * scale)))
        return clamp_value(k, int(cur) + random.randint(-step, step))
    step = spec["step"] * scale
    return clamp_value(k, float(cur) + random.uniform(-step, step))


def mutate_cfg(base: dict, scale: float) -> dict:
    c = dict(base)
    for k in PARAM_SPECS:
        if k not in c:
            continue
        # Keep fee/slippage relatively stable to reduce fake alpha.
        if k == "slippage_cents_per_side" and random.random() < 0.75:
            continue
        if random.random() < (0.55 if PARAM_SPECS[k]["type"] != "bool" else 0.20):
            c[k] = mutate_value(k, c[k], scale)
    return sanitize_cfg(c)


def crossover_cfg(a: dict, b: dict) -> dict:
    out = {}
    for k in PARAM_SPECS:
        if k in a and k in b:
            out[k] = a[k] if random.random() < 0.5 else b[k]
        elif k in a:
            out[k] = a[k]
        elif k in b:
            out[k] = b[k]
    return sanitize_cfg(out)


def rank_key(m: dict) -> tuple:
    return (
        m["score"],
        m["avg_win_rate_pct"],
        m["avg_net_pnl_cents"],
        -m["avg_max_drawdown_cents"],
        m["avg_trade_count"],
        -m["wr_std"],
    )


def evaluate_population(
    population: list[dict],
    args,
) -> list[tuple[dict, dict]]:
    out: list[tuple[dict, dict]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        fut_map = {
            ex.submit(
                evaluate_cfg,
                args.base_url,
                args.symbol,
                args.market_type,
                args.lookbacks,
                args.max_trades,
                args.max_samples,
                cfg,
                args.min_trades,
                args.trade_target,
                args.win_target,
                args.dd_target,
                args.timeout,
                args.retries,
            ): cfg
            for cfg in population
        }
        for fut in concurrent.futures.as_completed(fut_map):
            cfg = fut_map[fut]
            try:
                m = fut.result()
                out.append((cfg, m))
            except Exception:
                continue
    out.sort(key=lambda x: rank_key(x[1]), reverse=True)
    return out


def load_seed_pool(pattern: str, extra_seed: str | None) -> list[dict]:
    seed_cfgs: list[dict] = []
    for path in sorted(glob.glob(pattern)):
        try:
            cfg = load_cfg(path)
            if cfg:
                seed_cfgs.append(cfg)
        except Exception:
            continue
    if extra_seed:
        seed_cfgs.append(load_cfg(extra_seed))
    if not seed_cfgs:
        raise RuntimeError("no seed configs loaded")
    return seed_cfgs


def fetch_default_cfg_seed(base_url: str, symbol: str, market_type: str, timeout: int) -> dict | None:
    q = {
        "symbol": symbol,
        "market_type": market_type,
        "lookback_minutes": "720",
        "max_trades": "400",
        "max_samples": "120000",
        "full_history": "false",
    }
    url = f"{base_url}/api/strategy/paper?{urllib.parse.urlencode(q)}"
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    cfg = payload.get("config") or {}
    cfg = sanitize_cfg(cfg)
    return cfg if cfg else None


def save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Full paper-only hyperparameter search for FEV1 strategy.")
    ap.add_argument("--base-url", default="http://127.0.0.1:9810")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--market-type", default="5m")
    ap.add_argument("--seed-glob", default="configs/strategy-profiles/*.full.json")
    ap.add_argument("--extra-seed", default="")
    ap.add_argument("--lookbacks", default="720,1440,2880,4320")
    ap.add_argument("--max-trades", type=int, default=900)
    ap.add_argument("--max-samples", type=int, default=220000)
    ap.add_argument("--generations", type=int, default=24)
    ap.add_argument("--population", type=int, default=14)
    ap.add_argument("--elitism", type=int, default=4)
    ap.add_argument("--phase2-iters", type=int, default=120)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--min-trades", type=int, default=180)
    ap.add_argument("--trade-target", type=int, default=420)
    ap.add_argument("--win-target", type=float, default=90.0)
    ap.add_argument("--dd-target", type=float, default=36.0)
    ap.add_argument("--timeout", type=int, default=220)
    ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--seed", type=int, default=20260305)
    ap.add_argument("--checkpoint", default="outputs/search_btc5m_full/checkpoint.json")
    ap.add_argument("--out", default="outputs/search_btc5m_full/result.json")
    ap.add_argument("--save-profile", default="")
    args = ap.parse_args()

    random.seed(args.seed)
    args.lookbacks = [int(x.strip()) for x in args.lookbacks.split(",") if x.strip()]
    if not args.lookbacks:
        raise RuntimeError("lookbacks cannot be empty")

    seed_pool = load_seed_pool(args.seed_glob, args.extra_seed or None)
    try:
        default_seed = fetch_default_cfg_seed(
            base_url=args.base_url,
            symbol=args.symbol,
            market_type=args.market_type,
            timeout=args.timeout,
        )
        if default_seed:
            seed_pool.append(default_seed)
            print("INFO", "added default config seed from /api/strategy/paper")
    except Exception as exc:
        print("WARN", f"default config seed load failed: {str(exc)[:180]}")
    seed_eval = evaluate_population(seed_pool, args)
    if not seed_eval:
        raise RuntimeError("failed to evaluate seed configs")
    best_cfg, best_metrics = seed_eval[0]
    history = [
        {
            "stage": "seed",
            "idx": i,
            "score": m["score"],
            "avg_win_rate_pct": m["avg_win_rate_pct"],
            "avg_net_pnl_cents": m["avg_net_pnl_cents"],
            "avg_max_drawdown_cents": m["avg_max_drawdown_cents"],
            "avg_trade_count": m["avg_trade_count"],
        }
        for i, (_, m) in enumerate(seed_eval[:10], 1)
    ]
    print(
        "SEED_BEST",
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

    for g in range(1, args.generations + 1):
        if g <= int(args.generations * 0.45):
            scale = 1.0
        elif g <= int(args.generations * 0.80):
            scale = 0.60
        else:
            scale = 0.35

        elites = [cfg for cfg, _ in seed_eval[: max(2, min(args.elitism, len(seed_eval)))]]
        population = []
        population.extend(elites)
        population.append(best_cfg)
        while len(population) < args.population:
            r = random.random()
            if r < 0.45 and len(elites) >= 2:
                a = random.choice(elites)
                b = random.choice(elites)
                c = crossover_cfg(a, b)
            elif r < 0.85:
                c = mutate_cfg(random.choice(elites), scale)
            else:
                c = mutate_cfg(random.choice(seed_pool), scale * 1.2)
            population.append(c)

        eval_res = evaluate_population(population, args)
        if not eval_res:
            continue
        seed_eval = eval_res
        gen_best_cfg, gen_best_metrics = eval_res[0]
        if rank_key(gen_best_metrics) > rank_key(best_metrics):
            best_cfg = gen_best_cfg
            best_metrics = gen_best_metrics
            print(
                "NEW",
                g,
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
        history.append(
            {
                "stage": "gen",
                "idx": g,
                "score": gen_best_metrics["score"],
                "avg_win_rate_pct": gen_best_metrics["avg_win_rate_pct"],
                "avg_net_pnl_cents": gen_best_metrics["avg_net_pnl_cents"],
                "avg_max_drawdown_cents": gen_best_metrics["avg_max_drawdown_cents"],
                "avg_trade_count": gen_best_metrics["avg_trade_count"],
            }
        )
        save_json(
            Path(args.checkpoint),
            {
                "args": vars(args),
                "best_cfg": best_cfg,
                "best_metrics": best_metrics,
                "history": history[-60:],
                "saved_at_ms": int(time.time() * 1000),
            },
        )

    # Phase2: local refinement around final best
    current_cfg = dict(best_cfg)
    current_metrics = dict(best_metrics)
    for i in range(1, args.phase2_iters + 1):
        scale = 0.50 if i < int(args.phase2_iters * 0.7) else 0.28
        cand = mutate_cfg(current_cfg, scale)
        eval_res = evaluate_population([cand], args)
        if not eval_res:
            continue
        cand_cfg, cand_metrics = eval_res[0]
        if rank_key(cand_metrics) > rank_key(current_metrics):
            current_cfg, current_metrics = cand_cfg, cand_metrics
            print(
                "REFINE",
                i,
                json.dumps(
                    {
                        "score": round(current_metrics["score"], 2),
                        "avg_wr": round(current_metrics["avg_win_rate_pct"], 2),
                        "avg_net": round(current_metrics["avg_net_pnl_cents"], 2),
                        "avg_dd": round(current_metrics["avg_max_drawdown_cents"], 2),
                        "avg_n": round(current_metrics["avg_trade_count"], 2),
                    },
                    ensure_ascii=False,
                ),
            )

    if rank_key(current_metrics) > rank_key(best_metrics):
        best_cfg, best_metrics = current_cfg, current_metrics

    now = dt.datetime.utcnow().strftime("%Y_%m_%d")
    profile_name = f"{args.symbol.lower()}_{args.market_type}_paper_search_full_{now}"
    profile_payload = {"profile": profile_name, **best_cfg}
    profile_path = (
        Path(args.save_profile)
        if args.save_profile
        else Path(f"configs/strategy-profiles/{profile_name}.full.json")
    )
    save_json(profile_path, profile_payload)

    result = {
        "search": {
            "base_url": args.base_url,
            "symbol": args.symbol,
            "market_type": args.market_type,
            "lookbacks": args.lookbacks,
            "max_trades": args.max_trades,
            "max_samples": args.max_samples,
            "generations": args.generations,
            "population": args.population,
            "phase2_iters": args.phase2_iters,
            "min_trades": args.min_trades,
            "trade_target": args.trade_target,
            "win_target": args.win_target,
            "dd_target": args.dd_target,
            "workers": args.workers,
            "seed": args.seed,
        },
        "best_cfg": best_cfg,
        "best_metrics": best_metrics,
        "profile_path": str(profile_path),
        "history": history,
        "saved_at_ms": int(time.time() * 1000),
    }
    save_json(Path(args.out), result)
    print("FINAL", json.dumps(best_metrics, ensure_ascii=False))
    print("PROFILE", str(profile_path))
    print("RESULT", str(args.out))


if __name__ == "__main__":
    main()
