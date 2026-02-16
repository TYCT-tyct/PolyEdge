#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def fmt(x: Any, digits: int = 3) -> str:
    try:
        if x is None:
            return "NA"
        if isinstance(x, bool):
            return "true" if x else "false"
        if isinstance(x, (int, float)):
            return f"{float(x):.{digits}f}"
        return str(x)
    except Exception:
        return "NA"


@dataclass
class SweepRow:
    ts_ms: int
    path: Path
    stats: Dict[str, Dict[str, float]]
    last: Dict[str, Any]


def collect_sweeps(run_dir: Path) -> List[SweepRow]:
    rows: List[SweepRow] = []
    for p in sorted(run_dir.glob("full_latency_sweep_*.json")):
        try:
            d = read_json(p)
            meta = (d.get("meta") or {}) if isinstance(d, dict) else {}
            ts_ms = int((meta.get("ts_ms") or 0) or 0)
            engine = d.get("engine") or {}
            stats = engine.get("stats") or {}
            last = engine.get("last") or {}
            if isinstance(stats, dict) and isinstance(last, dict):
                rows.append(SweepRow(ts_ms=ts_ms, path=p, stats=stats, last=last))
        except Exception:
            continue
    return rows


def pick_stat(stats: Dict[str, Dict[str, float]], key: str, which: str) -> Optional[float]:
    try:
        st = stats.get(key) or {}
        if not isinstance(st, dict):
            return None
        v = st.get(which)
        return float(v) if v is not None else None
    except Exception:
        return None


def print_sweep_table(rows: List[SweepRow]) -> None:
    if not rows:
        print("no full_latency_sweep_*.json found")
        return

    cols = [
        ("tick_to_ack_p99_ms", "p99"),
        ("decision_queue_wait_p99_ms", "p99"),
        ("decision_compute_p99_ms", "p99"),
        ("feed_in_p99_ms", "p99"),
        ("local_backlog_p99_ms", "p99"),
        ("quote_block_ratio", "p99"),
        ("policy_block_ratio", "p99"),
        ("executed_over_eligible", "p50"),
        ("ev_net_usdc_p50", "p50"),
        ("window_outcomes", "p99"),
    ]

    header = ["ts_ms", "window_id"] + [f"{k}.{w}" for k, w in cols] + ["file"]
    print("\t".join(header))
    for r in rows:
        window_id = r.last.get("window_id")
        vals = [str(r.ts_ms), str(window_id)]
        for k, w in cols:
            vals.append(fmt(pick_stat(r.stats, k, w)))
        vals.append(r.path.name)
        print("\t".join(vals))


def print_storm(run_dir: Path) -> None:
    p = run_dir / "storm_test_summary.json"
    if not p.exists():
        print("no storm_test_summary.json found")
        return
    d = read_json(p)
    lat = d.get("latency_ms") or {}
    print("storm_test_summary")
    print(f"- error_count: {d.get('error_count')}")
    print(f"- consecutive_failures: {d.get('consecutive_failures')}")
    print(f"- latency_p50_ms: {fmt(lat.get('p50'))}")
    print(f"- latency_p99_ms: {fmt(lat.get('p99'))}")
    print(f"- latency_max_ms: {fmt(lat.get('max'))}")


def print_snapshot_diffs(run_dir: Path) -> None:
    snap = run_dir / "snapshots"
    if not snap.exists():
        return

    def get_live(name: str) -> Optional[Dict[str, Any]]:
        p = snap / name
        if not p.exists():
            return None
        try:
            d = read_json(p)
            return d if isinstance(d, dict) else None
        except Exception:
            return None

    pre = get_live("shadow_live_pre.json")
    post = get_live("shadow_live_after_storm.json") or get_live("shadow_live_after_sweeps.json")
    if not pre or not post:
        return

    keys = [
        "tick_to_ack_p99_ms",
        "decision_queue_wait_p99_ms",
        "decision_compute_p99_ms",
        "window_outcomes",
        "quote_block_ratio",
        "policy_block_ratio",
        "executed_over_eligible",
        "ev_net_usdc_p50",
        "ev_positive_ratio",
        "data_valid_ratio",
    ]
    print("shadow_live_pre_vs_post")
    for k in keys:
        print(f"- {k}: {fmt(pre.get(k))} -> {fmt(post.get(k))}")

    # Nested latency block (may be absent)
    pre_lat = pre.get("latency") or {}
    post_lat = post.get("latency") or {}
    for k in ["feed_in_p99_ms", "source_latency_p99_ms", "local_backlog_p99_ms"]:
        if k in pre_lat or k in post_lat:
            print(f"- latency.{k}: {fmt(pre_lat.get(k))} -> {fmt(post_lat.get(k))}")


def print_missing_endpoints(run_dir: Path) -> None:
    snap = run_dir / "snapshots"
    if not snap.exists():
        return
    errs = sorted(snap.glob("*.err.txt"))
    if not errs:
        return
    print("snapshot_errors")
    for p in errs:
        msg = p.read_text(encoding="utf-8", errors="replace").strip()
        print(f"- {p.name}: {msg}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze a fullchain benchmark run directory.")
    p.add_argument("--run-dir", required=True, help="Path to datasets/reports/<day>/runs/<run_id>")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    run_dir = Path(args.run_dir)
    if not run_dir.exists():
        print(f"run dir not found: {run_dir}")
        return 2

    rows = collect_sweeps(run_dir)
    print_sweep_table(rows)
    print()
    print_storm(run_dir)
    print()
    print_snapshot_diffs(run_dir)
    print()
    print_missing_endpoints(run_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

