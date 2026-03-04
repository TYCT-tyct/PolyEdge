#!/usr/bin/env python3
import argparse
import json
import random
import time
import urllib.parse
import urllib.request

BOUNDS = {
    "entry_threshold_base": (0.40, 0.95),
    "entry_threshold_cap": (0.45, 0.99),
    "spread_limit_prob": (0.005, 0.05),
    "entry_edge_prob": (0.002, 0.25),
    "entry_min_potential_cents": (1.0, 70.0),
    "entry_max_price_cents": (45.0, 98.5),
    "min_hold_ms": (0, 25000),
    "stop_loss_cents": (2.0, 60.0),
    "reverse_signal_threshold": (-0.95, -0.02),
    "reverse_signal_ticks": (1, 12),
    "trail_activate_profit_cents": (2.0, 80.0),
    "trail_drawdown_cents": (1.0, 50.0),
    "take_profit_near_max_cents": (70.0, 99.5),
    "endgame_take_profit_cents": (50.0, 99.0),
    "endgame_remaining_ms": (1000, 45000),
    "liquidity_widen_prob": (0.02, 0.20),
    "cooldown_ms": (0, 20000),
    "max_entries_per_round": (1, 2),
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


def get_json(base_url: str, path: str, query: dict, timeout: int, retries: int) -> dict:
    url = f"{base_url}{path}?{urllib.parse.urlencode(query)}"
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(0.4 * (attempt + 1))
                continue
            raise
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"failed to fetch {url}")


def eval_cfg(
    base_url: str,
    symbol: str,
    market_type: str,
    full_history: bool,
    lookback_minutes: int,
    max_trades: int,
    max_samples: int,
    min_trades: int,
    win_floor: float,
    cfg: dict,
    retries: int,
) -> dict:
    q = {
        "symbol": symbol,
        "market_type": market_type,
        "full_history": "true" if full_history else "false",
        "lookback_minutes": str(lookback_minutes),
        "max_trades": str(max_trades),
        "max_samples": str(max_samples),
        "use_autotune": "false",
    }
    for k, v in cfg.items():
        q[k] = str(v)
    p = get_json(base_url, "/api/strategy/paper", q, timeout=180, retries=retries)
    t = p.get("trades") or []
    s = p.get("summary") or {}

    def stat(arr: list[dict]) -> tuple[float, float, float]:
        n = len(arr)
        if n == 0:
            return 0.0, 0.0, 0.0
        pnls = [float(x.get("pnl_cents") or 0.0) for x in arr]
        wins = sum(1 for p in pnls if p > 0.0)
        total = sum(pnls)
        return wins * 100.0 / n, total / n, total

    w50, a50, t50 = stat(t[-50:])
    w80, a80, t80 = stat(t[-80:])
    n = len(t)
    max_drawdown = float(s.get("max_drawdown_cents") or 0.0)
    sample_penalty = max(0, min_trades - n) * 8.0
    win_penalty = max(0.0, win_floor - w50) * 20.0 + max(0.0, (win_floor - 2.0) - w80) * 12.0
    score = (
        w50 * 6.5
        + w80 * 4.5
        + a50 * 2.8
        + a80 * 1.8
        + max(0.0, t50) * 0.03
        + max(0.0, float(s.get("total_pnl_cents") or 0.0)) * 0.004
        - max_drawdown * 0.9
        - sample_penalty
        - win_penalty
    )
    return {
        "w50": w50,
        "a50": a50,
        "t50": t50,
        "w80": w80,
        "a80": a80,
        "t80": t80,
        "score": score,
        "n": n,
        "full": float(s.get("win_rate_pct") or 0.0),
        "total": float(s.get("total_pnl_cents") or 0.0),
        "max_drawdown": max_drawdown,
    }


def clamp_param(k: str, v: float | int) -> float | int:
    lo, hi = BOUNDS[k]
    if k in INT_KEYS:
        return int(max(lo, min(hi, round(float(v)))))
    return max(lo, min(hi, float(v)))


def mutate(cfg: dict, scale: float) -> dict:
    c = dict(cfg)
    steps = {
        "entry_threshold_base": 0.06,
        "entry_threshold_cap": 0.06,
        "spread_limit_prob": 0.006,
        "entry_edge_prob": 0.015,
        "entry_min_potential_cents": 3.5,
        "entry_max_price_cents": 4.5,
        "min_hold_ms": 3000,
        "stop_loss_cents": 2.0,
        "reverse_signal_threshold": 0.12,
        "trail_activate_profit_cents": 3.0,
        "trail_drawdown_cents": 1.5,
        "take_profit_near_max_cents": 3.0,
        "endgame_take_profit_cents": 3.0,
        "endgame_remaining_ms": 3500,
        "liquidity_widen_prob": 0.02,
        "cooldown_ms": 2500,
        "max_exec_spread_cents": 0.4,
        "slippage_cents_per_side": 0.03,
        "fee_cents_per_side": 0.03,
        "emergency_wide_spread_penalty_ratio": 0.12,
    }

    for k in list(c.keys()):
        if k not in BOUNDS:
            continue
        if k in {"max_entries_per_round", "reverse_signal_ticks"}:
            if random.random() < 0.20:
                c[k] = clamp_param(k, float(c[k]) + random.choice([-1, 1]))
            continue
        if k in {"fee_cents_per_side", "slippage_cents_per_side"} and random.random() >= 0.18:
            continue
        step = steps.get(k, 0.10)
        c[k] = clamp_param(k, float(c[k]) + random.uniform(-step, step) * scale)

    c["entry_threshold_cap"] = max(float(c["entry_threshold_cap"]), float(c["entry_threshold_base"]) + 0.03)
    c["entry_threshold_cap"] = clamp_param("entry_threshold_cap", c["entry_threshold_cap"])
    return c


def main() -> None:
    ap = argparse.ArgumentParser(description="Refine local neighborhood around a seed config.")
    ap.add_argument("--base-url", default="http://127.0.0.1:9810")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--market-type", default="5m")
    ap.add_argument(
        "--full-history",
        action="store_true",
        help="Request full history from API (default false to honor lookback window).",
    )
    ap.add_argument("--lookback-minutes", type=int, default=1440)
    ap.add_argument("--max-trades", type=int, default=900)
    ap.add_argument("--min-trades", type=int, default=120)
    ap.add_argument("--win-floor", type=float, default=84.0)
    ap.add_argument("--input", required=True, help="Path to JSON containing candidate config object.")
    ap.add_argument("--iters", type=int, default=280)
    ap.add_argument(
        "--max-samples",
        type=int,
        default=260000,
        help="Upper bound for /api/strategy/paper max_samples; lower value reduces API memory pressure.",
    )
    ap.add_argument("--out", default="/tmp/refine_result.json")
    ap.add_argument("--retries", type=int, default=3)
    args = ap.parse_args()

    with open(args.input, "r", encoding="utf-8") as f:
        base_cfg = json.load(f)

    best_cfg = dict(base_cfg)
    best = eval_cfg(
        args.base_url,
        args.symbol,
        args.market_type,
        args.full_history,
        args.lookback_minutes,
        args.max_trades,
        args.max_samples,
        args.min_trades,
        args.win_floor,
        best_cfg,
        args.retries,
    )
    print("BASE", json.dumps(best, ensure_ascii=False))

    for i in range(1, args.iters + 1):
        scale = 1.0 if i < int(args.iters * 0.45) else (0.6 if i < int(args.iters * 0.8) else 0.35)
        cand = mutate(best_cfg if random.random() < 0.70 else base_cfg, scale)
        res = eval_cfg(
            args.base_url,
            args.symbol,
            args.market_type,
            args.full_history,
            args.lookback_minutes,
            args.max_trades,
            args.max_samples,
            args.min_trades,
            args.win_floor,
            cand,
            args.retries,
        )
        if (res["score"], res["w50"], res["a50"], res["w80"]) > (
            best["score"],
            best["w50"],
            best["a50"],
            best["w80"],
        ):
            best = res
            best_cfg = cand
            print(
                "NEW",
                i,
                json.dumps(
                    {
                        "w50": round(best["w50"], 2),
                        "a50": round(best["a50"], 3),
                        "w80": round(best["w80"], 2),
                        "a80": round(best["a80"], 3),
                        "score": round(best["score"], 2),
                        "n": best["n"],
                        "full": round(best["full"], 2),
                        "total": round(best["total"], 2),
                    },
                    ensure_ascii=False,
                ),
            )

    out = {"best_metrics": best, "best_cfg": best_cfg}
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("FINAL", json.dumps(best, ensure_ascii=False))
    print("CFG", json.dumps(best_cfg, ensure_ascii=False))


if __name__ == "__main__":
    main()
