#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests


def utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def default_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"long-reg-{ts}-{os.getpid()}"

PROFILE_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "quick": {
        "cycles": 2,
        "max_cycles": 2,
        "max_runtime_sec": 1200,
        "window_sec": 180,
        "eval_window_sec": 90,
        "poll_interval_sec": 3.0,
        "heartbeat_sec": 15.0,
        "max_trials": 2,
        "fail_fast_threshold": 1,
        "cooldown_sec": 30,
        "min_outcomes": 10,
        "max_estimated_sec": 1800,
    },
    "standard": {
        "cycles": 4,
        "max_cycles": 0,
        "max_runtime_sec": 0,
        "window_sec": 1800,
        "eval_window_sec": 120,
        "poll_interval_sec": 10.0,
        "heartbeat_sec": 30.0,
        "max_trials": 12,
        "fail_fast_threshold": 0,
        "cooldown_sec": 120,
        "min_outcomes": 30,
        "max_estimated_sec": 0,
    },
    "deep": {
        "cycles": 8,
        "max_cycles": 0,
        "max_runtime_sec": 0,
        "window_sec": 2400,
        "eval_window_sec": 180,
        "poll_interval_sec": 10.0,
        "heartbeat_sec": 30.0,
        "max_trials": 16,
        "fail_fast_threshold": 0,
        "cooldown_sec": 120,
        "min_outcomes": 40,
        "max_estimated_sec": 0,
    },
}


def apply_profile_defaults(args: argparse.Namespace) -> argparse.Namespace:
    profile = PROFILE_DEFAULTS[args.profile]
    for key, value in profile.items():
        if getattr(args, key) is None:
            setattr(args, key, value)
    return args


def post_json(session: requests.Session, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    resp = session.post(url, json=payload, timeout=8)
    resp.raise_for_status()
    return resp.json()


def post_json_optional(session: requests.Session, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        return post_json(session, url, payload)
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        if status in (404, 405):
            return {}
        raise


def get_json(session: requests.Session, url: str) -> Dict[str, Any]:
    resp = session.get(url, timeout=8)
    resp.raise_for_status()
    return resp.json()


def gate_pass(live: Dict[str, Any], min_outcomes: int) -> bool:
    if "gate_fail_reasons" in live:
        reasons = live.get("gate_fail_reasons") or []
        return isinstance(reasons, list) and len(reasons) == 0 and bool(live.get("gate_ready", False))
    outcomes = int(live.get("window_outcomes", live.get("total_outcomes", 0)) or 0)
    return (
        outcomes >= min_outcomes
        and float(live.get("net_markout_10s_usdc_p50", 0.0)) > 0.0
        and float(live.get("roi_notional_10s_bps_p50", 0.0)) > 0.0
        and float(live.get("fillability_10ms", 0.0)) >= 0.60
        and float(live.get("quote_block_ratio", 1.0)) < 0.10
        and float(live.get("policy_block_ratio", 1.0)) < 0.10
        and float(live.get("tick_to_ack_p99_ms", 9999.0)) < 450.0
        and float(live.get("decision_compute_p99_ms", 9999.0)) < 2.0
        and float((live.get("latency") or {}).get("feed_in_p99_ms", 9999.0)) < 800.0
        and float(live.get("data_valid_ratio", 0.0)) >= 0.999
    )


def run_param_regression(args: argparse.Namespace, cycle: int, budget_sec: int) -> None:
    script_path = Path(__file__).resolve().parent / "param_regression.py"
    trial_run_id = f"{args.run_id}-c{cycle:02d}"
    cmd = [
        sys.executable,
        str(script_path),
        "--base-url",
        args.base_url,
        "--profile",
        args.profile,
        "--run-id",
        trial_run_id,
        "--window-sec",
        str(args.window_sec),
        "--eval-window-sec",
        str(args.eval_window_sec),
        "--poll-interval-sec",
        str(args.poll_interval_sec),
        "--max-trials",
        str(args.max_trials),
        "--min-outcomes",
        str(args.min_outcomes),
        "--heartbeat-sec",
        str(args.heartbeat_sec),
        "--fail-fast-threshold",
        str(args.fail_fast_threshold),
    ]
    if budget_sec > 0:
        cmd.extend(["--max-runtime-sec", str(budget_sec)])
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
    post_json_optional(
        session,
        f"{base}/control/reload_taker",
        {
            "trigger_bps": max(1.0, float(trial["min_edge_bps"])),
            "max_slippage_bps": 25.0,
            "stale_tick_filter_ms": 450.0,
            "market_tier_profile": "balanced",
        },
    )
    post_json_optional(
        session,
        f"{base}/control/reload_allocator",
        {
            "capital_fraction_kelly": 0.35,
            "variance_penalty_lambda": 0.25,
            "active_top_n_markets": 8,
            "taker_weight": 0.7,
            "maker_weight": 0.2,
            "arb_weight": 0.1,
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


def reset_shadow(session: requests.Session, base_url: str) -> None:
    base = base_url.rstrip("/")
    post_json(session, f"{base}/control/reset_shadow", {})


def collect_live_window(
    session: requests.Session,
    base_url: str,
    eval_window_sec: int,
    poll_interval_sec: float,
) -> List[Dict[str, Any]]:
    base = base_url.rstrip("/")
    samples: List[Dict[str, Any]] = []
    deadline = time.monotonic() + max(1, eval_window_sec)
    while time.monotonic() < deadline:
        samples.append(get_json(session, f"{base}/report/shadow/live"))
        time.sleep(max(0.2, poll_interval_sec))
    if not samples:
        samples.append(get_json(session, f"{base}/report/shadow/live"))
    return samples


def pick_gate_snapshot(samples: List[Dict[str, Any]]) -> Dict[str, Any]:
    # Prefer the richest sample in-window to reduce small-sample artifacts.
    return max(
        samples,
        key=lambda item: int(item.get("window_outcomes", item.get("total_outcomes", 0)) or 0),
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Long-run shadow regression orchestrator")
    p.add_argument("--profile", choices=["quick", "standard", "deep"], default="quick")
    p.add_argument("--run-id", default=default_run_id())
    p.add_argument("--base-url", default="http://127.0.0.1:8080")
    p.add_argument("--cycles", type=int, default=None)
    p.add_argument("--max-cycles", type=int, default=None)
    p.add_argument("--max-runtime-sec", type=int, default=None)
    p.add_argument("--window-sec", type=int, default=None)
    p.add_argument("--eval-window-sec", type=int, default=None)
    p.add_argument("--poll-interval-sec", type=float, default=None)
    p.add_argument("--heartbeat-sec", type=float, default=None)
    p.add_argument("--max-trials", type=int, default=None)
    p.add_argument("--fail-fast-threshold", type=int, default=None)
    p.add_argument("--out-root", default="datasets/reports")
    p.add_argument("--cooldown-sec", type=int, default=None)
    p.add_argument("--min-outcomes", type=int, default=None)
    p.add_argument(
        "--max-estimated-sec",
        type=int,
        default=None,
        help="Hard cap for estimated total runtime; trims cycle count automatically when exceeded",
    )
    return p.parse_args()


def main() -> int:
    args = apply_profile_defaults(parse_args())
    started = time.monotonic()
    next_heartbeat = started + max(1.0, args.heartbeat_sec)
    session = requests.Session()
    day_dir = Path(args.out_root) / utc_day()
    day_dir.mkdir(parents=True, exist_ok=True)
    audit_file = day_dir / "long_regression_audit.jsonl"

    best_trial: Dict[str, Any] | None = None
    cycle_cap = args.max_cycles if args.max_cycles > 0 else args.cycles
    per_cycle_trial_sec = max(1, int(min(args.window_sec, args.eval_window_sec)))
    per_cycle_sec = max(1, int(args.cooldown_sec + args.max_trials * per_cycle_trial_sec + args.eval_window_sec))
    estimated_total_sec = per_cycle_sec * cycle_cap
    if args.max_estimated_sec and args.max_estimated_sec > 0 and estimated_total_sec > args.max_estimated_sec:
        allowed_cycles = max(1, int(args.max_estimated_sec // per_cycle_sec))
        if allowed_cycles < cycle_cap:
            print(
                f"[budget] estimated={estimated_total_sec}s exceeds cap={args.max_estimated_sec}s; "
                f"trimming cycles {cycle_cap} -> {allowed_cycles}"
            )
            cycle_cap = allowed_cycles
            estimated_total_sec = per_cycle_sec * cycle_cap
    print(
        f"[profile] {args.profile} cycles={cycle_cap} per_cycle~{per_cycle_sec}s "
        f"estimated~{estimated_total_sec}s max_runtime={args.max_runtime_sec}s"
    )
    consecutive_failures = 0
    for cycle in range(1, cycle_cap + 1):
        if args.max_runtime_sec > 0 and (time.monotonic() - started) >= args.max_runtime_sec:
            print("[stop] max-runtime-sec reached; ending orchestrator")
            break
        budget_remaining = 0
        if args.max_runtime_sec > 0:
            budget_remaining = max(1, int(args.max_runtime_sec - (time.monotonic() - started)))
        run_param_regression(args, cycle, budget_remaining)
        trial = read_best_trial(day_dir)
        if trial is None:
            raise RuntimeError("No best trial found after param_regression")

        apply_trial(session, args.base_url, trial)
        # Use cycle-local statistics for gate decision; avoid cumulative contamination.
        reset_shadow(session, args.base_url)
        time.sleep(max(10, args.cooldown_sec))
        samples = collect_live_window(
            session,
            args.base_url,
            args.eval_window_sec,
            args.poll_interval_sec,
        )
        live = pick_gate_snapshot(samples)
        passed = gate_pass(live, args.min_outcomes)

        if passed:
            best_trial = trial
            consecutive_failures = 0
        elif best_trial is not None:
            apply_trial(session, args.base_url, best_trial)
            consecutive_failures += 1
        else:
            consecutive_failures += 1

        row = {
            "run_id": args.run_id,
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "cycle": cycle,
            "applied_trial": trial,
            "gate_ready": bool(live.get("gate_ready", False)),
            "gate_pass": passed,
            "live": {
                "window_id": int(live.get("window_id", 0) or 0),
                "window_outcomes": int(live.get("window_outcomes", live.get("total_outcomes", 0)) or 0),
                "fillability_10ms": float(live.get("fillability_10ms", 0.0)),
                "quote_block_ratio": float(live.get("quote_block_ratio", 0.0)),
                "policy_block_ratio": float(live.get("policy_block_ratio", 0.0)),
                "tick_to_ack_p99_ms": float(live.get("tick_to_ack_p99_ms", 0.0)),
                "decision_compute_p99_ms": float(live.get("decision_compute_p99_ms", 0.0)),
                "feed_in_p99_ms": float((live.get("latency") or {}).get("feed_in_p99_ms", 0.0)),
                "data_valid_ratio": float(live.get("data_valid_ratio", 1.0)),
                "net_markout_10s_usdc_p50": float(live.get("net_markout_10s_usdc_p50", 0.0)),
                "ev_net_usdc_p50": float(live.get("ev_net_usdc_p50", live.get("net_markout_10s_usdc_p50", 0.0))),
                "ev_net_usdc_p10": float(live.get("ev_net_usdc_p10", 0.0)),
                "ev_positive_ratio": float(live.get("ev_positive_ratio", 0.0)),
                "eligible_count": int(live.get("eligible_count", 0)),
                "executed_count": int(live.get("executed_count", 0)),
                "executed_over_eligible": float(live.get("executed_over_eligible", 0.0)),
                "roi_notional_10s_bps_p50": float(live.get("roi_notional_10s_bps_p50", 0.0)),
                "gate_fail_reasons": live.get("gate_fail_reasons") or [],
            },
            "min_outcomes": args.min_outcomes,
            "eval_window_sec": args.eval_window_sec,
            "rollback_applied": (not passed and best_trial is not None),
            "eval_samples": len(samples),
        }
        with audit_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")
        print(
            f"[cycle {cycle}/{cycle_cap}] gate_pass={passed} "
            f"net_usdc={row['live']['net_markout_10s_usdc_p50']:.6f}"
        )
        now = time.monotonic()
        if now >= next_heartbeat:
            elapsed = int(time.monotonic() - started)
            print(
                f"[heartbeat] run_id={args.run_id} cycle={cycle} elapsed={elapsed}s "
                f"fails={consecutive_failures}"
            )
            next_heartbeat = now + max(1.0, args.heartbeat_sec)
        if args.fail_fast_threshold > 0 and consecutive_failures >= args.fail_fast_threshold:
            print(
                f"[stop] fail-fast-threshold reached with {consecutive_failures} consecutive failures"
            )
            break

    print(f"wrote={audit_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
