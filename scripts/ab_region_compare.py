#!/usr/bin/env python3
from __future__ import annotations

import atexit
import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import requests


def utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def default_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"ab-{ts}-{os.getpid()}"


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
    net_markout_10s_usdc_p50: float
    roi_notional_10s_bps_p50: float
    fillability_10ms_p50: float
    feed_in_p50_ms_p50: float
    feed_in_p99_ms_p50: float
    decision_compute_p99_ms_p50: float
    data_valid_ratio_p50: float
    gate_ready_ratio_p50: float
    window_outcomes_p50: float


def collect(
    base_url: str,
    name: str,
    seconds: int,
    poll_interval: float,
    heartbeat_sec: float,
    fail_fast_threshold: int,
) -> RegionStats:
    session = requests.Session()
    atexit.register(session.close)
    deadline = time.time() + max(1, seconds)
    tick_ack: List[float] = []
    tick_decision: List[float] = []
    block_ratio: List[float] = []
    net_markout_usdc: List[float] = []
    roi_notional_bps: List[float] = []
    fillability: List[float] = []
    feed_in: List[float] = []
    feed_in_p99: List[float] = []
    decision_compute_p99: List[float] = []
    data_valid_ratio: List[float] = []
    gate_ready_ratio: List[float] = []
    window_outcomes: List[float] = []
    next_heartbeat = time.time() + max(1.0, heartbeat_sec)
    consecutive_errors = 0

    while time.time() < deadline:
        try:
            resp = session.get(f"{base_url.rstrip('/')}/report/shadow/live", timeout=5)
            resp.raise_for_status()
            live = resp.json()
            latency = live.get("latency") or {}
            tick_ack.append(float(live.get("tick_to_ack_p99_ms", 0.0)))
            tick_decision.append(float(live.get("tick_to_decision_p99_ms", 0.0)))
            block_ratio.append(float(live.get("quote_block_ratio", 0.0)))
            net_markout_usdc.append(float(live.get("net_markout_10s_usdc_p50", 0.0)))
            roi_notional_bps.append(float(live.get("roi_notional_10s_bps_p50", 0.0)))
            fillability.append(float(live.get("fillability_10ms", 0.0)))
            feed_in.append(float(latency.get("feed_in_p50_ms", 0.0)))
            feed_in_p99.append(float(latency.get("feed_in_p99_ms", 0.0)))
            decision_compute_p99.append(float(live.get("decision_compute_p99_ms", 0.0)))
            data_valid_ratio.append(float(live.get("data_valid_ratio", 1.0)))
            gate_ready_ratio.append(1.0 if bool(live.get("gate_ready", False)) else 0.0)
            window_outcomes.append(
                float(live.get("window_outcomes", live.get("total_outcomes", 0.0)))
            )
            consecutive_errors = 0
        except Exception:
            raise  # Linus: Fail loudly and explicitly
        now = time.time()
        if now >= next_heartbeat:
            print(
                f"[heartbeat] region={name} samples={len(tick_ack)} "
                f"errors={consecutive_errors}"
            )
            next_heartbeat = now + max(1.0, heartbeat_sec)
        time.sleep(max(1.0, poll_interval))

    return RegionStats(
        name=name,
        samples=len(tick_ack),
        tick_to_ack_p99_ms_p50=percentile(tick_ack, 0.50),
        tick_to_decision_p99_ms_p50=percentile(tick_decision, 0.50),
        quote_block_ratio_p50=percentile(block_ratio, 0.50),
        net_markout_10s_usdc_p50=percentile(net_markout_usdc, 0.50),
        roi_notional_10s_bps_p50=percentile(roi_notional_bps, 0.50),
        fillability_10ms_p50=percentile(fillability, 0.50),
        feed_in_p50_ms_p50=percentile(feed_in, 0.50),
        feed_in_p99_ms_p50=percentile(feed_in_p99, 0.50),
        decision_compute_p99_ms_p50=percentile(decision_compute_p99, 0.50),
        data_valid_ratio_p50=percentile(data_valid_ratio, 0.50),
        gate_ready_ratio_p50=percentile(gate_ready_ratio, 0.50),
        window_outcomes_p50=percentile(window_outcomes, 0.50),
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compare two region deployments with live shadow metrics")
    p.add_argument("--base-a", required=True, help="Region A base url, e.g. http://eu-host:8080")
    p.add_argument("--base-b", required=True, help="Region B base url, e.g. http://us-host:8080")
    p.add_argument("--name-a", default="eu-west-2")
    p.add_argument("--name-b", default="us-east-1")
    p.add_argument("--run-id", default=default_run_id())
    p.add_argument("--seconds", type=int, default=600)
    p.add_argument("--max-runtime-sec", type=int, default=0)
    p.add_argument("--poll-interval", type=float, default=10.0)
    p.add_argument("--heartbeat-sec", type=float, default=30.0)
    p.add_argument("--fail-fast-threshold", type=int, default=3)
    p.add_argument("--out-root", default="datasets/reports")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    seconds = args.seconds
    if args.max_runtime_sec > 0:
        seconds = min(seconds, args.max_runtime_sec)
    a = collect(
        args.base_a,
        args.name_a,
        seconds,
        args.poll_interval,
        args.heartbeat_sec,
        args.fail_fast_threshold,
    )
    b = collect(
        args.base_b,
        args.name_b,
        seconds,
        args.poll_interval,
        args.heartbeat_sec,
        args.fail_fast_threshold,
    )

    winner = a if a.tick_to_ack_p99_ms_p50 < b.tick_to_ack_p99_ms_p50 else b
    day_dir = Path(args.out_root) / utc_day()
    day_dir.mkdir(parents=True, exist_ok=True)
    out_json = day_dir / "region_ab_compare.json"
    out_md = day_dir / "region_ab_compare.md"

    payload: dict = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_id": args.run_id,
        "seconds": seconds,
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
        f"- net_markout_10s_usdc_p50: {a.net_markout_10s_usdc_p50:.6f}",
        f"- roi_notional_10s_bps_p50: {a.roi_notional_10s_bps_p50:.4f}",
        f"- fillability_10ms_p50: {a.fillability_10ms_p50:.4f}",
        f"- feed_in_p50_ms_p50: {a.feed_in_p50_ms_p50:.3f}",
        f"- feed_in_p99_ms_p50: {a.feed_in_p99_ms_p50:.3f}",
        f"- decision_compute_p99_ms_p50: {a.decision_compute_p99_ms_p50:.3f}",
        f"- data_valid_ratio_p50: {a.data_valid_ratio_p50:.5f}",
        f"- gate_ready_ratio_p50: {a.gate_ready_ratio_p50:.4f}",
        f"- window_outcomes_p50: {a.window_outcomes_p50:.1f}",
        "",
        "## Region B",
        f"- name: {b.name}",
        f"- samples: {b.samples}",
        f"- tick_to_ack_p99_ms_p50: {b.tick_to_ack_p99_ms_p50:.3f}",
        f"- tick_to_decision_p99_ms_p50: {b.tick_to_decision_p99_ms_p50:.3f}",
        f"- quote_block_ratio_p50: {b.quote_block_ratio_p50:.4f}",
        f"- net_markout_10s_usdc_p50: {b.net_markout_10s_usdc_p50:.6f}",
        f"- roi_notional_10s_bps_p50: {b.roi_notional_10s_bps_p50:.4f}",
        f"- fillability_10ms_p50: {b.fillability_10ms_p50:.4f}",
        f"- feed_in_p50_ms_p50: {b.feed_in_p50_ms_p50:.3f}",
        f"- feed_in_p99_ms_p50: {b.feed_in_p99_ms_p50:.3f}",
        f"- decision_compute_p99_ms_p50: {b.decision_compute_p99_ms_p50:.3f}",
        f"- data_valid_ratio_p50: {b.data_valid_ratio_p50:.5f}",
        f"- gate_ready_ratio_p50: {b.gate_ready_ratio_p50:.4f}",
        f"- window_outcomes_p50: {b.window_outcomes_p50:.1f}",
        "",
    ]
    out_md.write_text("\n".join(md), encoding="utf-8")
    print(f"wrote={out_json}")
    print(f"wrote={out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
