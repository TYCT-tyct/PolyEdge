#!/usr/bin/env python3
"""Bounded full-latency sweep (engine metrics + WS lag) across all symbols.

This is meant to be fast and repeatable: one command produces a single JSON artifact
under datasets/reports/<day>/, suitable for comparing runs over time.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests

from latency_probe.models import SYMBOL_TO_NAME
from latency_probe.stats import summarize
from latency_probe.ws_probe import run_ws_latency

PROFILE_DEFAULTS: Dict[str, Dict[str, float]] = {
    "quick": {"seconds": 60, "poll_interval": 2.0},
    "standard": {"seconds": 120, "poll_interval": 2.0},
    "deep": {"seconds": 300, "poll_interval": 2.0},
}


def utc_day(ts: float | None = None) -> str:
    dt = datetime.fromtimestamp(ts or time.time(), tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def collect_engine_series(base_url: str, seconds: int, poll_interval: float) -> Dict[str, Any]:
    session = requests.Session()
    deadline = time.time() + max(1, seconds)
    samples = 0
    failures = 0
    last_live: Dict[str, Any] | None = None

    series: Dict[str, List[float]] = {
        "tick_to_ack_p99_ms": [],
        "decision_queue_wait_p99_ms": [],
        "decision_compute_p99_ms": [],
        "feed_in_p99_ms": [],
        "source_latency_p99_ms": [],
        "local_backlog_p99_ms": [],
        "book_top_lag_p50_ms": [],
        "book_top_lag_p90_ms": [],
        "book_top_lag_p99_ms": [],
        "quote_block_ratio": [],
        "policy_block_ratio": [],
        "executed_over_eligible": [],
        "ev_net_usdc_p50": [],
        "ev_positive_ratio": [],
        "survival_10ms": [],
        "fillability_10ms": [],
        "window_outcomes": [],
    }

    while time.time() < deadline:
        try:
            resp = session.get(f"{base_url.rstrip('/')}/report/shadow/live", timeout=5)
            resp.raise_for_status()
            live = resp.json()
            last_live = live
            latency = live.get("latency") or {}

            def push(key: str, value: Any) -> None:
                if isinstance(value, (int, float)):
                    series[key].append(float(value))

            push("tick_to_ack_p99_ms", live.get("tick_to_ack_p99_ms"))
            push("decision_queue_wait_p99_ms", live.get("decision_queue_wait_p99_ms"))
            push("decision_compute_p99_ms", live.get("decision_compute_p99_ms"))
            push("quote_block_ratio", live.get("quote_block_ratio"))
            push("policy_block_ratio", live.get("policy_block_ratio"))
            push("executed_over_eligible", live.get("executed_over_eligible"))
            push("ev_net_usdc_p50", live.get("ev_net_usdc_p50"))
            push("ev_positive_ratio", live.get("ev_positive_ratio"))
            push("window_outcomes", live.get("window_outcomes"))
            push("survival_10ms", live.get("survival_10ms"))
            push("fillability_10ms", live.get("fillability_10ms"))
            push("book_top_lag_p50_ms", live.get("book_top_lag_p50_ms"))
            push("book_top_lag_p90_ms", live.get("book_top_lag_p90_ms"))
            push("book_top_lag_p99_ms", live.get("book_top_lag_p99_ms"))

            push("feed_in_p99_ms", latency.get("feed_in_p99_ms"))
            push("source_latency_p99_ms", live.get("source_latency_p99_ms"))
            push("local_backlog_p99_ms", live.get("local_backlog_p99_ms"))

            samples += 1
        except Exception:
            failures += 1
        time.sleep(max(0.2, poll_interval))

    return {
        "samples": samples,
        "failures": failures,
        "stats": {k: summarize(v) for k, v in series.items()},
        "last": {
            "window_id": (last_live or {}).get("window_id"),
            "gate_ready": (last_live or {}).get("gate_ready"),
            "window_outcomes": (last_live or {}).get("window_outcomes"),
            "book_top_lag_by_symbol_p50_ms": (last_live or {}).get("book_top_lag_by_symbol_p50_ms"),
            "survival_10ms_by_symbol": (last_live or {}).get("survival_10ms_by_symbol"),
        },
    }


async def collect_ws_all(seconds: int, symbols: List[str]) -> Dict[str, Any]:
    results = await asyncio.gather(*(run_ws_latency(seconds, s) for s in symbols))
    return {sym: res for sym, res in zip(symbols, results)}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PolyEdge full latency sweep (bounded)")
    p.add_argument("--profile", default="quick", choices=["quick", "standard", "deep"])
    p.add_argument("--base-url", default="http://127.0.0.1:8080")
    p.add_argument("--seconds", type=int, default=None)
    p.add_argument("--poll-interval", type=float, default=None)
    p.add_argument("--symbols", default=",".join(SYMBOL_TO_NAME.keys()))
    p.add_argument("--out-root", default="datasets/reports")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    profile = PROFILE_DEFAULTS[args.profile]
    seconds = int(args.seconds if args.seconds is not None else profile["seconds"])
    poll_interval = float(args.poll_interval if args.poll_interval is not None else profile["poll_interval"])
    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]

    started_ms = int(time.time() * 1000)
    async def run_all() -> tuple[Dict[str, Any], Dict[str, Any]]:
        engine_task = asyncio.to_thread(collect_engine_series, args.base_url, seconds, poll_interval)
        ws_task = collect_ws_all(seconds, symbols)
        engine_res, ws_res = await asyncio.gather(engine_task, ws_task)
        return engine_res, ws_res

    engine, ws = asyncio.run(run_all())

    payload: Dict[str, Any] = {
        "meta": {
            "profile": args.profile,
            "base_url": args.base_url,
            "seconds": seconds,
            "poll_interval": poll_interval,
            "symbols": symbols,
            "ts_ms": started_ms,
        },
        "engine": engine,
        "ws": ws,
    }

    out_dir = Path(args.out_root) / utc_day()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"full_latency_sweep_{started_ms}.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    print(f"wrote_json={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
