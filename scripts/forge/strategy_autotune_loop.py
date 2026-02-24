#!/usr/bin/env python3
import argparse
import json
import time
from pathlib import Path
from urllib import parse, request


def fetch_json(url: str, timeout_sec: int) -> dict:
    with request.urlopen(url, timeout=timeout_sec) as resp:
        return json.loads(resp.read().decode("utf-8"))


def build_opt_url(base_url: str, query: dict) -> str:
    return f"{base_url}/api/strategy/optimize?{parse.urlencode(query)}"


def score_row(row: dict) -> tuple:
    # Primary: latest win rate, secondary: avg pnl, then total pnl.
    return (
        float(row.get("latest_win", 0.0)),
        float(row.get("avg_pnl", 0.0)),
        float(row.get("total_pnl", 0.0)),
    )


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
    ap.add_argument("--seed-from", type=int, default=1)
    ap.add_argument("--seed-to", type=int, default=24)
    ap.add_argument("--iterations-sweep", type=int, default=1200)
    ap.add_argument("--iterations-persist", type=int, default=2200)
    ap.add_argument("--persist-ttl-sec", type=int, default=86400)
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
    while True:
        cycle += 1
        started = int(time.time() * 1000)
        best = None
        rows = []

        for seed in range(args.seed_from, args.seed_to + 1):
            q = dict(base_query)
            q["seed"] = seed
            q["iterations"] = args.iterations_sweep
            payload = fetch_json(build_opt_url(args.base_url, q), args.timeout_sec)
            best_payload = payload.get("best") or {}
            rolling = best_payload.get("rolling_window_recent") or best_payload.get("rolling_window") or {}
            summary = best_payload.get("summary_recent") or best_payload.get("summary") or {}
            row = {
                "seed": seed,
                "latest_win": float(rolling.get("latest_win_rate_pct") or 0.0),
                "avg_pnl": float(summary.get("avg_pnl_cents") or 0.0),
                "total_pnl": float(summary.get("total_pnl_cents") or 0.0),
                "trade_count": int(summary.get("trade_count") or 0),
                "pf_recent": float(best_payload.get("profit_factor_recent") or best_payload.get("profit_factor_validation") or 0.0),
                "name": best_payload.get("name"),
            }
            rows.append(row)
            if best is None or score_row(row) > score_row(best):
                best = row

        persist_info = {"saved": False}
        if best is not None:
            q = dict(base_query)
            q["seed"] = best["seed"]
            q["iterations"] = args.iterations_persist
            q["persist_best"] = "true"
            q["persist_ttl_sec"] = args.persist_ttl_sec
            persisted = fetch_json(build_opt_url(args.base_url, q), args.timeout_sec)
            persist_info = persisted.get("persist") or {"saved": False}

        finished = int(time.time() * 1000)
        report = {
            "cycle": cycle,
            "started_ms": started,
            "finished_ms": finished,
            "duration_ms": finished - started,
            "best": best,
            "persist": persist_info,
            "rows": rows,
        }
        with report_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(report, ensure_ascii=False) + "\n")

        print(
            f"[cycle={cycle}] best_seed={best['seed'] if best else 'NA'} "
            f"win={best['latest_win'] if best else 0:.2f} "
            f"avg_pnl={best['avg_pnl'] if best else 0:.3f} "
            f"persist_saved={persist_info.get('saved', False)}",
            flush=True,
        )

        if args.max_cycles > 0 and cycle >= args.max_cycles:
            break
        time.sleep(max(5, args.cycle_interval_sec))


if __name__ == "__main__":
    main()
