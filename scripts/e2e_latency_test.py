#!/usr/bin/env python3
"""WS-first latency benchmark for PolyEdge runtime and market feeds."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import time
from pathlib import Path
from typing import Any, Dict, List

import requests

from latency_probe.models import SYMBOL_TO_NAME
from latency_probe.stats import print_stat_block, summarize
from latency_probe.ws_probe import has_delta, run_ws_latency

PROFILE_DEFAULTS: Dict[str, Dict[str, float]] = {
    "quick": {"seconds": 60, "poll_interval": 2.0},
    "standard": {"seconds": 120, "poll_interval": 2.0},
    "deep": {"seconds": 300, "poll_interval": 2.0},
}


def collect_engine_metrics(base_url: str, seconds: int, poll_interval: float) -> Dict[str, Any]:
    session = requests.Session()
    deadline = time.time() + max(1, seconds)
    samples = 0
    failures = 0

    series: Dict[str, List[float]] = {
        "tick_to_decision_p99_ms": [],
        "decision_queue_wait_p99_ms": [],
        "decision_compute_p99_ms": [],
        "tick_to_ack_p99_ms": [],
        "ack_only_p99_ms": [],
        "feed_in_p50_ms": [],
        "source_latency_p99_ms": [],
        "local_backlog_p99_ms": [],
        "data_valid_ratio": [],
        "seq_gap_rate": [],
        "ts_inversion_rate": [],
        "stale_tick_drop_ratio": [],
        "quote_block_ratio": [],
        "policy_block_ratio": [],
        "queue_depth_p99": [],
        "event_backlog_p99": [],
        "pnl_10s_p50_bps_raw": [],
        "pnl_10s_p50_bps_robust": [],
        "ev_net_usdc_p50": [],
        "ev_net_usdc_p10": [],
        "ev_positive_ratio": [],
        "eligible_count": [],
        "executed_count": [],
        "executed_over_eligible": [],
        "net_markout_10s_usdc_p50": [],
        "roi_notional_10s_bps_p50": [],
        "pnl_10s_outlier_ratio": [],
        "gate_ready_ratio": [],
        "window_outcomes": [],
    }

    while time.time() < deadline:
        try:
            resp = session.get(f"{base_url.rstrip('/')}/report/shadow/live", timeout=5)
            resp.raise_for_status()
            live = resp.json()
            latency = live.get("latency") or {}
            for key in (
                "tick_to_decision_p99_ms",
                "decision_queue_wait_p99_ms",
                "decision_compute_p99_ms",
                "tick_to_ack_p99_ms",
                "ack_only_p99_ms",
                "source_latency_p99_ms",
                "local_backlog_p99_ms",
                "data_valid_ratio",
                "seq_gap_rate",
                "ts_inversion_rate",
                "stale_tick_drop_ratio",
                "quote_block_ratio",
                "policy_block_ratio",
                "queue_depth_p99",
                "event_backlog_p99",
                "pnl_10s_p50_bps_raw",
                "pnl_10s_p50_bps_robust",
                "ev_net_usdc_p50",
                "ev_net_usdc_p10",
                "ev_positive_ratio",
                "eligible_count",
                "executed_count",
                "executed_over_eligible",
                "net_markout_10s_usdc_p50",
                "roi_notional_10s_bps_p50",
                "pnl_10s_outlier_ratio",
                "window_outcomes",
            ):
                value = live.get(key)
                if isinstance(value, (int, float)):
                    series[key].append(float(value))
            series["gate_ready_ratio"].append(1.0 if bool(live.get("gate_ready", False)) else 0.0)
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
    parser.add_argument("--profile", default="quick", choices=["quick", "standard", "deep"])
    parser.add_argument("--symbol", default="BTCUSDT", choices=list(SYMBOL_TO_NAME.keys()))
    parser.add_argument("--mode", default="ws-first", choices=["ws-first"])
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--seconds", type=int, default=None)
    parser.add_argument("--poll-interval", type=float, default=None)
    parser.add_argument("--json-out", default="")
    args = parser.parse_args()
    profile_defaults = PROFILE_DEFAULTS[args.profile]
    if args.seconds is None:
        args.seconds = int(profile_defaults["seconds"])
    if args.poll_interval is None:
        args.poll_interval = float(profile_defaults["poll_interval"])
    print(
        f"[profile] {args.profile} seconds={args.seconds} poll_interval={args.poll_interval}"
    )

    print("=== ENGINE WS-FIRST METRICS ===")
    engine = collect_engine_metrics(args.base_url, args.seconds, args.poll_interval)
    print(f"base_url={args.base_url} symbol={args.symbol} mode={args.mode}")
    print(f"samples={engine['samples']} failures={engine['failures']}")
    print_stat_block("tick_to_decision_p99", engine["stats"]["tick_to_decision_p99_ms"], "ms")
    print_stat_block(
        "decision_queue_wait_p99", engine["stats"]["decision_queue_wait_p99_ms"], "ms"
    )
    print_stat_block("decision_compute_p99", engine["stats"]["decision_compute_p99_ms"], "ms")
    print_stat_block("tick_to_ack_p99", engine["stats"]["tick_to_ack_p99_ms"], "ms")
    print_stat_block("ack_only_p99", engine["stats"]["ack_only_p99_ms"], "ms")
    print_stat_block("feed_in_p50", engine["stats"]["feed_in_p50_ms"], "ms")
    print_stat_block("source_latency_p99", engine["stats"]["source_latency_p99_ms"], "ms")
    print_stat_block("local_backlog_p99", engine["stats"]["local_backlog_p99_ms"], "ms")
    print_stat_block("data_valid_ratio", engine["stats"]["data_valid_ratio"], "")
    print_stat_block("seq_gap_rate", engine["stats"]["seq_gap_rate"], "")
    print_stat_block("ts_inversion_rate", engine["stats"]["ts_inversion_rate"], "")
    print_stat_block("stale_tick_drop_ratio", engine["stats"]["stale_tick_drop_ratio"], "")
    print_stat_block("quote_block_ratio", engine["stats"]["quote_block_ratio"], "")
    print_stat_block("policy_block_ratio", engine["stats"]["policy_block_ratio"], "")
    print_stat_block("queue_depth_p99", engine["stats"]["queue_depth_p99"], "")
    print_stat_block("event_backlog_p99", engine["stats"]["event_backlog_p99"], "")
    print_stat_block("pnl_10s_p50_bps_raw", engine["stats"]["pnl_10s_p50_bps_raw"], "bps")
    print_stat_block(
        "pnl_10s_p50_bps_robust", engine["stats"]["pnl_10s_p50_bps_robust"], "bps"
    )
    print_stat_block("ev_net_usdc_p50", engine["stats"]["ev_net_usdc_p50"], "usdc")
    print_stat_block("ev_net_usdc_p10", engine["stats"]["ev_net_usdc_p10"], "usdc")
    print_stat_block("ev_positive_ratio", engine["stats"]["ev_positive_ratio"], "")
    print_stat_block("eligible_count", engine["stats"]["eligible_count"], "")
    print_stat_block("executed_count", engine["stats"]["executed_count"], "")
    print_stat_block(
        "executed_over_eligible", engine["stats"]["executed_over_eligible"], ""
    )
    print_stat_block(
        "net_markout_10s_usdc_p50", engine["stats"]["net_markout_10s_usdc_p50"], "usdc"
    )
    print_stat_block(
        "roi_notional_10s_bps_p50", engine["stats"]["roi_notional_10s_bps_p50"], "bps"
    )
    print_stat_block("pnl_10s_outlier_ratio", engine["stats"]["pnl_10s_outlier_ratio"], "")
    print_stat_block("gate_ready_ratio", engine["stats"]["gate_ready_ratio"], "")
    print_stat_block("window_outcomes", engine["stats"]["window_outcomes"], "")

    print("\n=== WS FEED LATENCY (RECV - SOURCE TS) ===")
    ws = asyncio.run(run_ws_latency(args.seconds, args.symbol))
    print_stat_block("rtds_crypto_prices", ws["pm_lag_ms"], "ms")
    print_stat_block("rtds_chainlink", ws["chainlink_lag_ms"], "ms")
    print_stat_block("binance_ws", ws["bin_lag_ms"], "ms")
    if has_delta(ws):
        print(f"delta_pm_minus_bin_p50={ws['delta_pm_minus_bin_p50_ms']:.3f}ms")
    delta_chainlink = ws.get("delta_chainlink_minus_bin_p50_ms", float("nan"))
    if not math.isnan(delta_chainlink):
        print(f"delta_chainlink_minus_bin_p50={delta_chainlink:.3f}ms")
    delta_pm_minus_chainlink = ws.get("delta_pm_minus_chainlink_p50_ms", float("nan"))
    if not math.isnan(delta_pm_minus_chainlink):
        print(f"delta_pm_minus_chainlink_p50={delta_pm_minus_chainlink:.3f}ms")

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
