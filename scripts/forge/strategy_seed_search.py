#!/usr/bin/env python3
import argparse
import json
import urllib.parse
import urllib.request


def get_json(base_url: str, path: str, query: dict, timeout: int) -> dict:
    url = f"{base_url}{path}?{urllib.parse.urlencode(query)}"
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def trade_stats(trades: list[dict]) -> tuple[float, float, float]:
    n = len(trades)
    if n <= 0:
        return 0.0, 0.0, 0.0
    pnls = [float(t.get("pnl_cents") or 0.0) for t in trades]
    wins = sum(1 for p in pnls if p > 0.0)
    total = sum(pnls)
    return wins * 100.0 / n, total / n, total


def eval_config(base_url: str, market_type: str, cfg: dict, lookback_minutes: int, max_trades: int) -> dict:
    query = {
        "market_type": market_type,
        "full_history": "true",
        "lookback_minutes": str(lookback_minutes),
        "max_trades": str(max_trades),
        "use_autotune": "false",
    }
    for key, value in cfg.items():
        query[key] = str(value)

    payload = get_json(base_url, "/api/strategy/paper", query, timeout=180)
    summary = payload.get("summary") or {}
    trades = payload.get("trades") or []

    w50, a50, t50 = trade_stats(trades[-50:])
    w80, a80, t80 = trade_stats(trades[-80:])
    w120, a120, t120 = trade_stats(trades[-120:])

    trade_count = int(summary.get("trade_count") or 0)
    full_win = float(summary.get("win_rate_pct") or 0.0)
    full_avg = float(summary.get("avg_pnl_cents") or 0.0)
    full_total = float(summary.get("total_pnl_cents") or 0.0)
    max_drawdown = float(summary.get("max_drawdown_cents") or 0.0)

    # Prefer high recent win-rate and keep pnl positive/stable.
    score = (
        w50 * 4.8
        + w80 * 2.2
        + a50 * 9.0
        + a80 * 5.0
        + max(0.0, t50) * 0.08
        + max(0.0, t80) * 0.04
        - max(0.0, 90.0 - w50) * 20.0
        - max(0.0, 88.0 - w80) * 10.0
        - max(0.0, -a50) * 80.0
        - max(0.0, -a80) * 60.0
        - max_drawdown * 0.02
    )
    if trade_count < 50:
        score -= 500.0

    return {
        "score": score,
        "trade_count": trade_count,
        "full_win": full_win,
        "full_avg": full_avg,
        "full_total": full_total,
        "max_drawdown": max_drawdown,
        "w50": w50,
        "a50": a50,
        "t50": t50,
        "w80": w80,
        "a80": a80,
        "t80": t80,
        "w120": w120,
        "a120": a120,
        "t120": t120,
        "cfg": cfg,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Seed sweep for fixed (non-autotune) strategy config.")
    ap.add_argument("--base-url", default="http://127.0.0.1:9810")
    ap.add_argument("--market-type", default="5m")
    ap.add_argument("--seed-from", type=int, default=1)
    ap.add_argument("--seed-to", type=int, default=40)
    ap.add_argument("--iterations", type=int, default=1600)
    ap.add_argument("--lookback-minutes", type=int, default=1440)
    ap.add_argument("--max-trades", type=int, default=900)
    ap.add_argument("--max-arms", type=int, default=22)
    ap.add_argument("--recent-lookback-minutes", type=int, default=1440)
    ap.add_argument("--recent-weight", type=float, default=0.78)
    ap.add_argument("--window-trades", type=int, default=50)
    ap.add_argument("--target-win-rate", type=float, default=90.0)
    ap.add_argument("--out", default="/tmp/strategy_search_result.json")
    args = ap.parse_args()

    baseline = eval_config(
        args.base_url, args.market_type, {}, args.lookback_minutes, args.max_trades
    )
    print(
        "BASELINE",
        json.dumps(
            {
                k: baseline[k]
                for k in [
                    "w50",
                    "a50",
                    "t50",
                    "w80",
                    "a80",
                    "t80",
                    "full_win",
                    "full_avg",
                    "full_total",
                    "trade_count",
                    "max_drawdown",
                    "score",
                ]
            },
            ensure_ascii=False,
        ),
    )

    rows: list[dict] = []
    for seed in range(args.seed_from, args.seed_to + 1):
        try:
            optimize_payload = get_json(
                args.base_url,
                "/api/strategy/optimize",
                {
                    "market_type": args.market_type,
                    "target_win_rate": str(args.target_win_rate),
                    "window_trades": str(args.window_trades),
                    "max_arms": str(args.max_arms),
                    "max_trades": str(args.max_trades),
                    "full_history": "true",
                    "recent_lookback_minutes": str(args.recent_lookback_minutes),
                    "recent_weight": str(args.recent_weight),
                    "iterations": str(args.iterations),
                    "seed": str(seed),
                },
                timeout=300,
            )
            best = optimize_payload.get("best") or {}
            cfg = best.get("config") or {}
            if not cfg:
                continue
            result = eval_config(
                args.base_url,
                args.market_type,
                cfg,
                args.lookback_minutes,
                args.max_trades,
            )
            result["seed"] = seed
            result["objective"] = best.get("objective")
            rows.append(result)
            print(
                f"seed={seed:02d} w50={result['w50']:.1f}% a50={result['a50']:.2f} "
                f"w80={result['w80']:.1f}% full={result['full_win']:.1f}% "
                f"trades={result['trade_count']} score={result['score']:.1f}"
            )
        except Exception as exc:
            print(f"seed={seed:02d} ERR {str(exc)[:180]}")

    rows.sort(
        key=lambda r: (r["score"], r["w50"], r["a50"], r["w80"], r["a80"], r["full_win"]),
        reverse=True,
    )
    print("\nTOP10")
    for row in rows[:10]:
        print(
            json.dumps(
                {
                    k: row[k]
                    for k in [
                        "seed",
                        "score",
                        "w50",
                        "a50",
                        "w80",
                        "a80",
                        "w120",
                        "a120",
                        "full_win",
                        "full_avg",
                        "full_total",
                        "trade_count",
                        "max_drawdown",
                    ]
                },
                ensure_ascii=False,
            )
        )

    out = {
        "baseline": baseline,
        "top10": rows[:10],
        "best": rows[0] if rows else None,
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    if rows:
        print("\nBEST_CFG_JSON")
        print(json.dumps(rows[0]["cfg"], ensure_ascii=False))


if __name__ == "__main__":
    main()
