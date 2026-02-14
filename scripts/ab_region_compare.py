#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests


def utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    arr = sorted(values)
    idx = int(round((len(arr) - 1) * p))
    idx = max(0, min(len(arr) - 1, idx))
    return arr[idx]


@dataclass
class RegionStats:
    name: str
    samples: int
    tick_to_ack_p99_ms_p50: float
    tick_to_decision_p99_ms_p50: float
    quote_block_ratio_p50: float
    pnl_10s_p50_bps_robust_p50: float
    fillability_10ms_p50: float
    feed_in_p50_ms_p50: float


def collect(base_url: str, name: str, seconds: int, poll_interval: float) -> RegionStats:
    session = requests.Session()
    deadline = time.time() + max(1, seconds)
    tick_ack: List[float] = []
    tick_decision: List[float] = []
    block_ratio: List[float] = []
    pnl_robust: List[float] = []
    fillability: List[float] = []
    feed_in: List[float] = []

    while time.time() < deadline:
        resp = session.get(f"{base_url.rstrip('/')}/report/shadow/live", timeout=5)
        resp.raise_for_status()
        live = resp.json()
        latency = live.get("latency") or {}
        tick_ack.append(float(live.get("tick_to_ack_p99_ms", 0.0)))
        tick_decision.append(float(live.get("tick_to_decision_p99_ms", 0.0)))
        block_ratio.append(float(live.get("quote_block_ratio", 0.0)))
        pnl_robust.append(float(live.get("pnl_10s_p50_bps_robust", live.get("pnl_10s_p50_bps", 0.0))))
        fillability.append(float(live.get("fillability_10ms", 0.0)))
        feed_in.append(float(latency.get("feed_in_p50_ms", 0.0)))
        time.sleep(max(1.0, poll_interval))

    return RegionStats(
        name=name,
        samples=len(tick_ack),
        tick_to_ack_p99_ms_p50=percentile(tick_ack, 0.50),
        tick_to_decision_p99_ms_p50=percentile(tick_decision, 0.50),
        quote_block_ratio_p50=percentile(block_ratio, 0.50),
        pnl_10s_p50_bps_robust_p50=percentile(pnl_robust, 0.50),
        fillability_10ms_p50=percentile(fillability, 0.50),
        feed_in_p50_ms_p50=percentile(feed_in, 0.50),
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compare two region deployments with live shadow metrics")
    p.add_argument("--base-a", required=True, help="Region A base url, e.g. http://eu-host:8080")
    p.add_argument("--base-b", required=True, help="Region B base url, e.g. http://us-host:8080")
    p.add_argument("--name-a", default="eu-west-2")
    p.add_argument("--name-b", default="us-east-1")
    p.add_argument("--seconds", type=int, default=600)
    p.add_argument("--poll-interval", type=float, default=10.0)
    p.add_argument("--out-root", default="datasets/reports")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    a = collect(args.base_a, args.name_a, args.seconds, args.poll_interval)
    b = collect(args.base_b, args.name_b, args.seconds, args.poll_interval)

    winner = a if a.tick_to_ack_p99_ms_p50 < b.tick_to_ack_p99_ms_p50 else b
    day_dir = Path(args.out_root) / utc_day()
    day_dir.mkdir(parents=True, exist_ok=True)
    out_json = day_dir / "region_ab_compare.json"
    out_md = day_dir / "region_ab_compare.md"

    payload: Dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "region_a": a.__dict__,
        "region_b": b.__dict__,
        "winner_by_tick_to_ack_p99": winner.name,
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

    md = [
        "# Region A/B Compare",
        "",
        f"- winner_by_tick_to_ack_p99: {winner.name}",
        "",
        "## Region A",
        f"- name: {a.name}",
        f"- samples: {a.samples}",
        f"- tick_to_ack_p99_ms_p50: {a.tick_to_ack_p99_ms_p50:.3f}",
        f"- tick_to_decision_p99_ms_p50: {a.tick_to_decision_p99_ms_p50:.3f}",
        f"- quote_block_ratio_p50: {a.quote_block_ratio_p50:.4f}",
        f"- pnl_10s_p50_bps_robust_p50: {a.pnl_10s_p50_bps_robust_p50:.4f}",
        f"- fillability_10ms_p50: {a.fillability_10ms_p50:.4f}",
        f"- feed_in_p50_ms_p50: {a.feed_in_p50_ms_p50:.3f}",
        "",
        "## Region B",
        f"- name: {b.name}",
        f"- samples: {b.samples}",
        f"- tick_to_ack_p99_ms_p50: {b.tick_to_ack_p99_ms_p50:.3f}",
        f"- tick_to_decision_p99_ms_p50: {b.tick_to_decision_p99_ms_p50:.3f}",
        f"- quote_block_ratio_p50: {b.quote_block_ratio_p50:.4f}",
        f"- pnl_10s_p50_bps_robust_p50: {b.pnl_10s_p50_bps_robust_p50:.4f}",
        f"- fillability_10ms_p50: {b.fillability_10ms_p50:.4f}",
        f"- feed_in_p50_ms_p50: {b.feed_in_p50_ms_p50:.3f}",
        "",
    ]
    out_md.write_text("\n".join(md), encoding="utf-8")
    print(f"wrote={out_json}")
    print(f"wrote={out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
