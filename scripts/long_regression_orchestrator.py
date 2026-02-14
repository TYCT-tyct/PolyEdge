#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests


def utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def post_json(session: requests.Session, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    resp = session.post(url, json=payload, timeout=8)
    resp.raise_for_status()
    return resp.json()


def get_json(session: requests.Session, url: str) -> Dict[str, Any]:
    resp = session.get(url, timeout=8)
    resp.raise_for_status()
    return resp.json()


def gate_pass(live: Dict[str, Any]) -> bool:
    return (
        float(live.get("pnl_10s_p50_bps_robust", live.get("pnl_10s_p50_bps", 0.0))) > 0.0
        and float(live.get("fillability_10ms", 0.0)) >= 0.60
        and float(live.get("quote_block_ratio", 1.0)) < 0.10
        and float(live.get("tick_to_ack_p99_ms", 9999.0)) < 450.0
    )


def run_param_regression(args: argparse.Namespace) -> None:
    cmd = [
        "python",
        "scripts/param_regression.py",
        "--base-url",
        args.base_url,
        "--window-sec",
        str(args.window_sec),
        "--poll-interval-sec",
        str(args.poll_interval_sec),
        "--max-trials",
        str(args.max_trials),
    ]
    subprocess.run(cmd, check=True)


def read_best_trial(day_dir: Path) -> Dict[str, Any] | None:
    summary_json = day_dir / "regression_summary.json"
    if not summary_json.exists():
        return None
    data = json.loads(summary_json.read_text(encoding="utf-8"))
    trials = data.get("trials") or []
    return trials[0] if trials else None


def apply_trial(session: requests.Session, base_url: str, trial: Dict[str, Any]) -> None:
    base = base_url.rstrip("/")
    post_json(
        session,
        f"{base}/control/reload_strategy",
        {
            "min_edge_bps": trial["min_edge_bps"],
            "ttl_ms": trial["ttl_ms"],
            "basis_k_revert": trial["basis_k_revert"],
            "basis_z_cap": trial["basis_z_cap"],
        },
    )
    post_json(
        session,
        f"{base}/control/reload_toxicity",
        {
            "safe_threshold": trial["safe_threshold"],
            "caution_threshold": trial["caution_threshold"],
        },
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Long-run shadow regression orchestrator")
    p.add_argument("--base-url", default="http://127.0.0.1:8080")
    p.add_argument("--cycles", type=int, default=4)
    p.add_argument("--window-sec", type=int, default=1800)
    p.add_argument("--poll-interval-sec", type=float, default=10.0)
    p.add_argument("--max-trials", type=int, default=12)
    p.add_argument("--out-root", default="datasets/reports")
    p.add_argument("--cooldown-sec", type=int, default=120)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    session = requests.Session()
    day_dir = Path(args.out_root) / utc_day()
    day_dir.mkdir(parents=True, exist_ok=True)
    audit_file = day_dir / "long_regression_audit.jsonl"

    best_trial: Dict[str, Any] | None = None
    for cycle in range(1, args.cycles + 1):
        run_param_regression(args)
        trial = read_best_trial(day_dir)
        if trial is None:
            raise RuntimeError("No best trial found after param_regression")

        apply_trial(session, args.base_url, trial)
        time.sleep(max(10, args.cooldown_sec))
        live = get_json(session, f"{args.base_url.rstrip('/')}/report/shadow/live")
        passed = gate_pass(live)

        if passed:
            best_trial = trial
        elif best_trial is not None:
            apply_trial(session, args.base_url, best_trial)

        row = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "cycle": cycle,
            "applied_trial": trial,
            "gate_pass": passed,
            "live": {
                "fillability_10ms": float(live.get("fillability_10ms", 0.0)),
                "quote_block_ratio": float(live.get("quote_block_ratio", 0.0)),
                "tick_to_ack_p99_ms": float(live.get("tick_to_ack_p99_ms", 0.0)),
                "pnl_10s_p50_bps_robust": float(
                    live.get("pnl_10s_p50_bps_robust", live.get("pnl_10s_p50_bps", 0.0))
                ),
            },
            "rollback_applied": (not passed and best_trial is not None),
        }
        with audit_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")
        print(
            f"[cycle {cycle}/{args.cycles}] gate_pass={passed} "
            f"pnl10_robust={row['live']['pnl_10s_p50_bps_robust']:.3f}"
        )

    print(f"wrote={audit_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
