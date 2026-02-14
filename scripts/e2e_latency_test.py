#!/usr/bin/env python3
"""WS-first latency benchmark for PolyEdge runtime and market feeds."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path
from typing import Any, Dict, List

import requests

from latency_probe.models import SYMBOL_TO_NAME
from latency_probe.stats import print_stat_block, summarize
from latency_probe.ws_probe import has_delta, run_ws_latency


def collect_engine_metrics(base_url: str, seconds: int, poll_interval: float) -> Dict[str, Any]:
    session = requests.Session()
    deadline = time.time() + max(1, seconds)
    samples = 0
    failures = 0

    series: Dict[str, List[float]] = {
        "tick_to_decision_p99_ms": [],
        "tick_to_ack_p99_ms": [],
        "ack_only_p99_ms": [],
        "feed_in_p50_ms": [],
        "quote_block_ratio": [],
        "policy_block_ratio": [],
        "queue_depth_p99": [],
        "event_backlog_p99": [],
        "pnl_10s_p50_bps_raw": [],
        "pnl_10s_p50_bps_robust": [],
        "pnl_10s_outlier_ratio": [],
    }

    while time.time() < deadline:
        try:
            resp = session.get(f"{base_url.rstrip('/')}/report/shadow/live", timeout=5)
            resp.raise_for_status()
            live = resp.json()
            latency = live.get("latency") or {}
            for key in (
                "tick_to_decision_p99_ms",
                "tick_to_ack_p99_ms",
                "ack_only_p99_ms",
                "quote_block_ratio",
                "policy_block_ratio",
                "queue_depth_p99",
                "event_backlog_p99",
                "pnl_10s_p50_bps_raw",
                "pnl_10s_p50_bps_robust",
                "pnl_10s_outlier_ratio",
            ):
                value = live.get(key)
                if isinstance(value, (int, float)):
                    series[key].append(float(value))
            feed_p50 = latency.get("feed_in_p50_ms")
            if isinstance(feed_p50, (int, float)):
                series["feed_in_p50_ms"].append(float(feed_p50))
            samples += 1
        except Exception:
            failures += 1
        time.sleep(max(0.2, poll_interval))

    return {
        "samples": samples,
        "failures": failures,
        "stats": {k: summarize(v) for k, v in series.items()},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="WS-first latency benchmark")
    parser.add_argument("--symbol", default="BTCUSDT", choices=list(SYMBOL_TO_NAME.keys()))
    parser.add_argument("--mode", default="ws-first", choices=["ws-first"])
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--seconds", type=int, default=120)
    parser.add_argument("--poll-interval", type=float, default=2.0)
    parser.add_argument("--json-out", default="")
    args = parser.parse_args()

    print("=== ENGINE WS-FIRST METRICS ===")
    engine = collect_engine_metrics(args.base_url, args.seconds, args.poll_interval)
    print(f"base_url={args.base_url} symbol={args.symbol} mode={args.mode}")
    print(f"samples={engine['samples']} failures={engine['failures']}")
    print_stat_block("tick_to_decision_p99", engine["stats"]["tick_to_decision_p99_ms"], "ms")
    print_stat_block("tick_to_ack_p99", engine["stats"]["tick_to_ack_p99_ms"], "ms")
    print_stat_block("ack_only_p99", engine["stats"]["ack_only_p99_ms"], "ms")
    print_stat_block("feed_in_p50", engine["stats"]["feed_in_p50_ms"], "ms")
    print_stat_block("quote_block_ratio", engine["stats"]["quote_block_ratio"], "")
    print_stat_block("policy_block_ratio", engine["stats"]["policy_block_ratio"], "")
    print_stat_block("queue_depth_p99", engine["stats"]["queue_depth_p99"], "")
    print_stat_block("event_backlog_p99", engine["stats"]["event_backlog_p99"], "")
    print_stat_block("pnl_10s_p50_bps_raw", engine["stats"]["pnl_10s_p50_bps_raw"], "bps")
    print_stat_block(
        "pnl_10s_p50_bps_robust", engine["stats"]["pnl_10s_p50_bps_robust"], "bps"
    )
    print_stat_block("pnl_10s_outlier_ratio", engine["stats"]["pnl_10s_outlier_ratio"], "")

    print("\n=== WS FEED LATENCY (RECV - SOURCE TS) ===")
    ws = asyncio.run(run_ws_latency(args.seconds, args.symbol))
    print_stat_block("polymarket_ws", ws["pm_lag_ms"], "ms")
    print_stat_block("binance_ws", ws["bin_lag_ms"], "ms")
    if has_delta(ws):
        print(f"delta_pm_minus_bin_p50={ws['delta_pm_minus_bin_p50_ms']:.3f}ms")

    payload = {
        "meta": {
            "mode": args.mode,
            "symbol": args.symbol,
            "base_url": args.base_url,
            "seconds": args.seconds,
            "poll_interval": args.poll_interval,
            "ts_ms": int(time.time() * 1000),
        },
        "engine": engine,
        "ws": ws,
    }
    if args.json_out:
        out_path = Path(args.json_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"wrote_json={out_path}")


if __name__ == "__main__":
    main()
