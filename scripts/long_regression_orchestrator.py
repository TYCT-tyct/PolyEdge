#!/usr/bin/env python3
from __future__ import annotations

import atexit
import argparse
import json
import math
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import requests


def utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def default_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"long-reg-{ts}-{os.getpid()}"


PROFILE_DEFAULTS: Dict[str, dict] = {
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
        "selection_mode": "robust",
        "walkforward_windows": 2,
        "walkforward_cooldown_sec": 2.0,
        "top_k_consensus": 3,
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
        "selection_mode": "robust",
        "walkforward_windows": 2,
        "walkforward_cooldown_sec": 3.0,
        "top_k_consensus": 3,
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
        "selection_mode": "robust",
        "walkforward_windows": 3,
        "walkforward_cooldown_sec": 5.0,
        "top_k_consensus": 5,
    },
}


def apply_profile_defaults(args: argparse.Namespace) -> argparse.Namespace:
    profile = PROFILE_DEFAULTS[args.profile]
    for key, value in profile.items():
        if getattr(args, key) is None:
            setattr(args, key, value)
    return args


def post_json(
    session: requests.Session,
    url: str,
    payload: dict,
    timeout_sec: float = 8.0,
) -> dict:
    resp = session.post(url, json=payload, timeout=timeout_sec)
    resp.raise_for_status()
    return resp.json()


def post_json_optional(
    session: requests.Session,
    url: str,
    payload: dict,
    timeout_sec: float = 8.0,
) -> dict:
    try:
        return post_json(session, url, payload, timeout_sec)
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        if status in (404, 405):
            return {}
        raise


def get_json(session: requests.Session, url: str, timeout_sec: float = 8.0) -> dict:
    resp = session.get(url, timeout=timeout_sec)
    resp.raise_for_status()
    return resp.json()


def gate_pass(live: dict, min_outcomes: int) -> bool:
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


def run_param_regression(args: argparse.Namespace, trial_run_id: str, budget_sec: int) -> None:
    script_path = Path(__file__).resolve().parent / "param_regression.py"
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
        "--selection-mode",
        str(args.selection_mode),
        "--walkforward-windows",
        str(args.walkforward_windows),
        "--walkforward-cooldown-sec",
        str(args.walkforward_cooldown_sec),
        "--top-k-consensus",
        str(args.top_k_consensus),
    ]
    timeout_sec: float | None = None
    if budget_sec > 0:
        cmd.extend(["--max-runtime-sec", str(budget_sec)])
        timeout_sec = float(max(60, budget_sec + 90))
    subprocess.run(cmd, check=True, timeout=timeout_sec)


def read_best_trial(out_root: Path, trial_run_id: str) -> dict | None:
    candidates = sorted(
        out_root.glob(f"*/runs/{trial_run_id}/regression_summary.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return None
    data = json.loads(candidates[0].read_text(encoding="utf-8"))
    auto_tuned = ((data.get("auto_tuned_thresholds") or {}).get("consensus") or {})
    if isinstance(auto_tuned, dict) and auto_tuned.get("reload_strategy"):
        return auto_tuned
    trials = data.get("trials") or []
    return trials[0] if trials else None


def load_champion_state(state_file: Path) -> Tuple[dict | None, dict | None]:
    if not state_file.exists():
        return None, None
    data = json.loads(state_file.read_text(encoding="utf-8"))
    bundle = data.get("bundle")
    snapshot = data.get("snapshot")
    if not isinstance(bundle, dict):
        return None, None
    return bundle, snapshot if isinstance(snapshot, dict) else None


def save_champion_state(
    state_file: Path,
    run_id: str,
    cycle: int,
    decision: str,
    bundle: dict,
    snapshot: dict,
) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(
        json.dumps(
            {
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "run_id": run_id,
                "cycle": cycle,
                "decision": decision,
                "bundle": bundle,
                "snapshot": snapshot,
            },
            ensure_ascii=True,
            indent=2,
        ),
        encoding="utf-8",
    )


def to_bundle(trial: dict) -> dict:
    if "reload_strategy" in trial:
        return {
            "reload_strategy": dict(trial.get("reload_strategy") or {}),
            "reload_taker": dict(trial.get("reload_taker") or {}),
            "reload_allocator": dict(trial.get("reload_allocator") or {}),
            "reload_toxicity": dict(trial.get("reload_toxicity") or {}),
        }
    return {
        "reload_strategy": {
            "min_edge_bps": trial["min_edge_bps"],
            "ttl_ms": trial["ttl_ms"],
            "max_spread": trial["max_spread"],
            "base_quote_size": trial["base_quote_size"],
            "min_eval_notional_usdc": trial["min_eval_notional_usdc"],
            "min_expected_edge_usdc": trial["min_expected_edge_usdc"],
            "basis_k_revert": trial["basis_k_revert"],
            "basis_z_cap": trial["basis_z_cap"],
        },
        "reload_taker": {
            "trigger_bps": trial["taker_trigger_bps"],
            "max_slippage_bps": trial["taker_max_slippage_bps"],
            "stale_tick_filter_ms": trial["stale_tick_filter_ms"],
            "market_tier_profile": trial["market_tier_profile"],
        },
        "reload_allocator": {
            "capital_fraction_kelly": 0.35,
            "variance_penalty_lambda": 0.25,
            "active_top_n_markets": trial["active_top_n_markets"],
            "taker_weight": 0.7,
            "maker_weight": 0.2,
            "arb_weight": 0.1,
        },
        "reload_toxicity": {
            "safe_threshold": trial["safe_threshold"],
            "caution_threshold": trial["caution_threshold"],
        },
    }


def apply_bundle(session: requests.Session, base_url: str, bundle: dict) -> None:
    base = base_url.rstrip("/")
    post_json(session, f"{base}/control/reload_strategy", bundle.get("reload_strategy") or {})
    post_json_optional(session, f"{base}/control/reload_taker", bundle.get("reload_taker") or {})
    post_json_optional(session, f"{base}/control/reload_allocator", bundle.get("reload_allocator") or {})
    post_json(session, f"{base}/control/reload_toxicity", bundle.get("reload_toxicity") or {})


def reset_shadow(session: requests.Session, base_url: str) -> None:
    base = base_url.rstrip("/")
    post_json(session, f"{base}/control/reset_shadow", {})


def collect_live_window(
    session: requests.Session,
    base_url: str,
    eval_window_sec: int,
    poll_interval_sec: float,
    timeout_sec: float,
    max_read_failures: int,
) -> List[dict]:
    base = base_url.rstrip("/")
    samples: List[dict] = []
    consecutive_failures = 0
    deadline = time.monotonic() + max(1, eval_window_sec)
    while time.monotonic() < deadline:
        try:
            samples.append(get_json(session, f"{base}/report/shadow/live", timeout_sec))
            consecutive_failures = 0
        except requests.RequestException as exc:
            consecutive_failures += 1
            if consecutive_failures >= max(1, max_read_failures) and samples:
                print(
                    f"[warn] shadow/live read failure x{consecutive_failures}; "
                    "keeping collected samples and ending evaluation window early"
                )
                break
            print(f"[warn] shadow/live read failed ({type(exc).__name__}); retrying")
        time.sleep(max(0.2, poll_interval_sec))
    if not samples:
        try:
            samples.append(get_json(session, f"{base}/report/shadow/live", timeout_sec))
        except requests.RequestException:
            samples.append(
                {
                    "gate_ready": False,
                    "gate_fail_reasons": ["shadow_live_unreachable"],
                    "window_outcomes": 0,
                    "total_outcomes": 0,
                    "net_markout_10s_usdc_p50": 0.0,
                    "ev_net_usdc_p50": 0.0,
                    "roi_notional_10s_bps_p50": 0.0,
                    "fillability_10ms": 0.0,
                    "quote_block_ratio": 1.0,
                    "policy_block_ratio": 1.0,
                    "tick_to_ack_p99_ms": 9999.0,
                    "decision_compute_p99_ms": 9999.0,
                    "data_valid_ratio": 0.0,
                    "latency": {"feed_in_p99_ms": 9999.0},
                }
            )
    return samples


def pick_gate_snapshot(samples: List[dict]) -> dict:
    return max(
        samples,
        key=lambda item: int(item.get("window_outcomes", item.get("total_outcomes", 0)) or 0),
    )


def stddev(values: List[float]) -> float:
    if len(values) <= 1:
        return 0.0
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(max(0.0, var))


def ev_penalty_objective(
    live: dict,
    samples: List[dict],
    min_outcomes: int,
    args: argparse.Namespace,
) -> Tuple[float, float, Dict[str, float]]:
    ev_net = float(live.get("ev_net_usdc_p50", live.get("net_markout_10s_usdc_p50", 0.0)))
    tick_to_ack = float(live.get("tick_to_ack_p99_ms", 0.0))
    decision_compute = float(live.get("decision_compute_p99_ms", 0.0))
    feed_in = float((live.get("latency") or {}).get("feed_in_p99_ms", 0.0))
    quote_block = float(live.get("quote_block_ratio", 0.0))
    policy_block = float(live.get("policy_block_ratio", 0.0))
    executed_over_eligible = float(live.get("executed_over_eligible", 0.0))
    data_valid = float(live.get("data_valid_ratio", 1.0))

    ev_series = [
        float(s.get("ev_net_usdc_p50", s.get("net_markout_10s_usdc_p50", 0.0)))
        for s in samples
    ]
    latency_series = [float(s.get("tick_to_ack_p99_ms", 0.0)) for s in samples]

    penalties = {
        "latency": max(0.0, tick_to_ack - 450.0) * args.penalty_latency_ms,
        "decision": max(0.0, decision_compute - 2.0) * args.penalty_decision_ms,
        "feed": max(0.0, feed_in - 800.0) * args.penalty_feed_ms,
        "quote_block": max(0.0, quote_block - 0.10) * args.penalty_block_ratio,
        "policy_block": max(0.0, policy_block - 0.10) * args.penalty_policy_block_ratio,
        "execution": max(0.0, 0.60 - executed_over_eligible) * args.penalty_exec_shortfall,
        "data_valid": max(0.0, 0.999 - data_valid) * args.penalty_data_valid,
        "ev_instability": stddev(ev_series) * args.penalty_instability_ev,
        "latency_instability": stddev(latency_series) * args.penalty_instability_latency,
    }
    passed = gate_pass(live, min_outcomes)
    penalties["gate_fail"] = 0.0 if passed else args.penalty_gate_fail
    penalty_total = sum(penalties.values())
    objective = ev_net - penalty_total
    return objective, penalty_total, penalties


def _step_towards(current: float, target: float, max_step: float) -> float:
    max_step = abs(max_step)
    if max_step <= 0.0:
        return target
    delta = target - current
    if delta > max_step:
        delta = max_step
    if delta < -max_step:
        delta = -max_step
    return current + delta


def drift_bundle(
    champion: dict,
    challenger: dict,
    args: argparse.Namespace,
) -> dict:
    out = json.loads(json.dumps(challenger))
    cst = champion.get("reload_strategy") or {}
    dst = out.get("reload_strategy") or {}
    if cst and dst:
        if "min_edge_bps" in cst and "min_edge_bps" in dst:
            dst["min_edge_bps"] = _step_towards(
                float(cst["min_edge_bps"]), float(dst["min_edge_bps"]), args.drift_edge_step_bps
            )
        if "ttl_ms" in cst and "ttl_ms" in dst:
            dst["ttl_ms"] = int(
                round(
                    _step_towards(
                        float(cst["ttl_ms"]),
                        float(dst["ttl_ms"]),
                        float(args.drift_ttl_step_ms),
                    )
                )
            )
        if "max_spread" in cst and "max_spread" in dst:
            dst["max_spread"] = _step_towards(
                float(cst["max_spread"]),
                float(dst["max_spread"]),
                args.drift_max_spread_step,
            )
        if "base_quote_size" in cst and "base_quote_size" in dst:
            dst["base_quote_size"] = _step_towards(
                float(cst["base_quote_size"]),
                float(dst["base_quote_size"]),
                args.drift_base_quote_step,
            )
        out["reload_strategy"] = dst

    ctk = champion.get("reload_taker") or {}
    dtk = out.get("reload_taker") or {}
    if ctk and dtk and "trigger_bps" in ctk and "trigger_bps" in dtk:
        dtk["trigger_bps"] = _step_towards(
            float(ctk["trigger_bps"]), float(dtk["trigger_bps"]), args.drift_trigger_step_bps
        )
    if ctk and dtk and "max_slippage_bps" in ctk and "max_slippage_bps" in dtk:
        dtk["max_slippage_bps"] = _step_towards(
            float(ctk["max_slippage_bps"]),
            float(dtk["max_slippage_bps"]),
            args.drift_slippage_step_bps,
        )
    out["reload_taker"] = dtk

    ctox = champion.get("reload_toxicity") or {}
    dtox = out.get("reload_toxicity") or {}
    if ctox and dtox and "safe_threshold" in ctox and "safe_threshold" in dtox:
        dtox["safe_threshold"] = _step_towards(
            float(ctox["safe_threshold"]),
            float(dtox["safe_threshold"]),
            args.drift_safe_step,
        )
    if ctox and dtox and "caution_threshold" in ctox and "caution_threshold" in dtox:
        dtox["caution_threshold"] = _step_towards(
            float(ctox["caution_threshold"]),
            float(dtox["caution_threshold"]),
            args.drift_caution_step,
        )
    out["reload_toxicity"] = dtox

    return out


def evaluate_bundle(
    session: requests.Session,
    base_url: str,
    bundle: dict,
    args: argparse.Namespace,
    role: str,
) -> dict:
    apply_bundle(session, base_url, bundle)
    reset_shadow(session, base_url)
    time.sleep(max(5, args.cooldown_sec))
    samples = collect_live_window(
        session,
        base_url,
        args.eval_window_sec,
        args.poll_interval_sec,
        args.http_timeout_sec,
        args.max_sample_read_failures,
    )
    live = pick_gate_snapshot(samples)
    objective, penalty, breakdown = ev_penalty_objective(live, samples, args.min_outcomes, args)
    return {
        "role": role,
        "bundle": bundle,
        "gate_pass": gate_pass(live, args.min_outcomes),
        "objective": objective,
        "ev_net_usdc_p50": float(
            live.get("ev_net_usdc_p50", live.get("net_markout_10s_usdc_p50", 0.0))
        ),
        "penalty_total": penalty,
        "penalty_breakdown": breakdown,
        "samples": len(samples),
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
            "ev_net_usdc_p50": float(
                live.get("ev_net_usdc_p50", live.get("net_markout_10s_usdc_p50", 0.0))
            ),
            "ev_net_usdc_p10": float(live.get("ev_net_usdc_p10", 0.0)),
            "ev_positive_ratio": float(live.get("ev_positive_ratio", 0.0)),
            "eligible_count": int(live.get("eligible_count", 0)),
            "executed_count": int(live.get("executed_count", 0)),
            "executed_over_eligible": float(live.get("executed_over_eligible", 0.0)),
            "roi_notional_10s_bps_p50": float(live.get("roi_notional_10s_bps_p50", 0.0)),
            "gate_fail_reasons": live.get("gate_fail_reasons") or [],
        },
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Long-run shadow regression orchestrator")
    p.add_argument("--profile", choices=["quick", "standard", "deep"], default="quick")
    p.add_argument("--run-id", default=default_run_id())
    p.add_argument("--base-url", default="http://127.0.0.1:8080")
    p.add_argument(
        "--continuous",
        action="store_true",
        help="Run continuous optimization cycles until max-runtime or stop-file is reached",
    )
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
    p.add_argument("--selection-mode", choices=["score", "robust"], default=None)
    p.add_argument("--walkforward-windows", type=int, default=None)
    p.add_argument("--walkforward-cooldown-sec", type=float, default=None)
    p.add_argument("--top-k-consensus", type=int, default=None)
    p.add_argument(
        "--cycle-sleep-sec",
        type=float,
        default=2.0,
        help="Sleep between cycles to avoid tight loop on remote APIs",
    )
    p.add_argument(
        "--http-timeout-sec",
        type=float,
        default=8.0,
        help="Per-request HTTP timeout for control/report endpoints",
    )
    p.add_argument(
        "--max-sample-read-failures",
        type=int,
        default=4,
        help="Consecutive /report/shadow/live failures allowed before ending window",
    )
    p.add_argument(
        "--state-file",
        default="",
        help="Champion state path; default: <out-root>/champion_state.json",
    )
    p.add_argument(
        "--stop-file",
        default="",
        help="If this file exists, orchestrator exits cleanly at cycle boundary",
    )
    p.add_argument(
        "--max-estimated-sec",
        type=int,
        default=None,
        help="Hard cap for estimated total runtime; trims cycle count automatically when exceeded",
    )
    p.add_argument(
        "--min-objective-delta",
        type=float,
        default=0.0005,
        help="Required objective improvement for challenger promotion",
    )
    p.add_argument(
        "--disable-drift",
        action="store_true",
        help="Disable champion->challenger small-step drift; apply raw challenger directly",
    )
    p.add_argument("--drift-edge-step-bps", type=float, default=0.25)
    p.add_argument("--drift-ttl-step-ms", type=int, default=50)
    p.add_argument("--drift-max-spread-step", type=float, default=0.005)
    p.add_argument("--drift-base-quote-step", type=float, default=0.50)
    p.add_argument("--drift-trigger-step-bps", type=float, default=0.50)
    p.add_argument("--drift-slippage-step-bps", type=float, default=2.0)
    p.add_argument("--drift-safe-step", type=float, default=0.02)
    p.add_argument("--drift-caution-step", type=float, default=0.02)
    p.add_argument("--penalty-latency-ms", type=float, default=0.00002)
    p.add_argument("--penalty-decision-ms", type=float, default=0.05)
    p.add_argument("--penalty-feed-ms", type=float, default=0.000005)
    p.add_argument("--penalty-block-ratio", type=float, default=0.20)
    p.add_argument("--penalty-policy-block-ratio", type=float, default=0.20)
    p.add_argument("--penalty-exec-shortfall", type=float, default=0.50)
    p.add_argument("--penalty-data-valid", type=float, default=80.0)
    p.add_argument("--penalty-instability-ev", type=float, default=1.0)
    p.add_argument("--penalty-instability-latency", type=float, default=0.0005)
    p.add_argument("--penalty-gate-fail", type=float, default=0.20)
    return p.parse_args()


def main() -> int:
    args = apply_profile_defaults(parse_args())
    started = time.monotonic()
    next_heartbeat = started + max(1.0, args.heartbeat_sec)
    session = requests.Session()
    atexit.register(session.close)
    out_root = Path(args.out_root)
    out_root.mkdir(parents=True, exist_ok=True)

    state_file = Path(args.state_file) if args.state_file else (out_root / "champion_state.json")
    stop_file = Path(args.stop_file) if args.stop_file else None

    champion_bundle, champion_snapshot = load_champion_state(state_file)
    if champion_bundle is not None:
        print(f"[resume] loaded champion state from {state_file}")

    cycle_cap = args.max_cycles if args.max_cycles > 0 else args.cycles
    per_cycle_trial_sec = max(
        1,
        int(
            min(args.window_sec, args.eval_window_sec) * max(1, int(args.walkforward_windows))
            + max(0.0, float(args.walkforward_cooldown_sec))
            * max(0, int(args.walkforward_windows) - 1)
        ),
    )
    # Per cycle includes both champion and challenger online windows.
    per_cycle_sec = max(
        1,
        int(
            args.cooldown_sec
            + args.max_trials * per_cycle_trial_sec
            + (2 * (args.cooldown_sec + args.eval_window_sec))
        ),
    )
    if args.continuous:
        print(
            f"[profile] {args.profile} cycles=continuous per_cycle~{per_cycle_sec}s "
            f"max_runtime={args.max_runtime_sec}s mode={args.selection_mode} "
            f"wf_windows={args.walkforward_windows}"
        )
    else:
        estimated_total_sec = per_cycle_sec * cycle_cap
        if (
            args.max_estimated_sec
            and args.max_estimated_sec > 0
            and estimated_total_sec > args.max_estimated_sec
        ):
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
            f"estimated~{estimated_total_sec}s max_runtime={args.max_runtime_sec}s "
            f"mode={args.selection_mode} wf_windows={args.walkforward_windows}"
        )

    consecutive_failures = 0
    cycle = 0
    last_audit_file: Path | None = None
    last_daily_state_file: Path | None = None
    while True:
        cycle += 1
        if not args.continuous and cycle > cycle_cap:
            break
        if stop_file is not None and stop_file.exists():
            print(f"[stop] stop-file detected: {stop_file}")
            break
        if args.max_runtime_sec > 0 and (time.monotonic() - started) >= args.max_runtime_sec:
            print("[stop] max-runtime-sec reached; ending orchestrator")
            break

        budget_remaining = 0
        if args.max_runtime_sec > 0:
            budget_remaining = max(1, int(args.max_runtime_sec - (time.monotonic() - started)))
        trial_run_id = f"{args.run_id}-c{cycle:04d}"
        run_param_regression(args, trial_run_id, budget_remaining)
        trial = read_best_trial(out_root, trial_run_id)
        if trial is None:
            raise RuntimeError("No best trial found after param_regression")

        challenger_bundle = to_bundle(trial)
        if champion_bundle is not None and not args.disable_drift:
            challenger_bundle = drift_bundle(champion_bundle, challenger_bundle, args)

        champion_eval: dict | None = None
        if champion_bundle is not None:
            champion_eval = evaluate_bundle(session, args.base_url, champion_bundle, args, "champion")

        challenger_eval = evaluate_bundle(session, args.base_url, challenger_bundle, args, "challenger")

        decision = "bootstrap_reject"
        rollback_applied = False
        promoted = False
        if champion_bundle is None:
            if challenger_eval["gate_pass"]:
                champion_bundle = challenger_bundle
                champion_snapshot = challenger_eval
                promoted = True
                decision = "bootstrap_promote"
            else:
                decision = "bootstrap_reject"
        else:
            champ_obj = float(champion_eval["objective"] if champion_eval else -1e9)
            chall_obj = float(challenger_eval["objective"])
            improve = chall_obj - champ_obj
            if challenger_eval["gate_pass"] and improve >= args.min_objective_delta:
                champion_bundle = challenger_bundle
                champion_snapshot = challenger_eval
                promoted = True
                decision = "promote_challenger"
            else:
                apply_bundle(session, args.base_url, champion_bundle)
                rollback_applied = True
                if champion_eval is not None:
                    champion_snapshot = champion_eval
                decision = "rollback_to_champion"

        final_eval = champion_snapshot if champion_snapshot is not None else challenger_eval
        final_pass = bool(final_eval["gate_pass"])
        if final_pass:
            consecutive_failures = 0
        else:
            consecutive_failures += 1

        day_dir = out_root / utc_day()
        day_dir.mkdir(parents=True, exist_ok=True)
        audit_file = day_dir / "long_regression_audit.jsonl"
        daily_state_file = day_dir / "champion_state.json"

        row = {
            "run_id": args.run_id,
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "cycle": cycle,
            "trial_run_id": trial_run_id,
            "decision": decision,
            "promoted": promoted,
            "rollback_applied": rollback_applied,
            "min_objective_delta": args.min_objective_delta,
            "champion_eval": champion_eval,
            "challenger_eval": challenger_eval,
            "final_eval": final_eval,
            "final_gate_pass": final_pass,
            "eval_window_sec": args.eval_window_sec,
        }
        with audit_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")
        last_audit_file = audit_file

        if champion_bundle is not None:
            save_champion_state(
                daily_state_file,
                args.run_id,
                cycle,
                decision,
                champion_bundle,
                final_eval,
            )
            if state_file.resolve() != daily_state_file.resolve():
                save_champion_state(
                    state_file,
                    args.run_id,
                    cycle,
                    decision,
                    champion_bundle,
                    final_eval,
                )
            last_daily_state_file = daily_state_file

        cycle_text = f"{cycle}/{cycle_cap}" if not args.continuous else f"{cycle}/inf"
        print(
            f"[cycle {cycle_text}] decision={decision} final_pass={final_pass} "
            f"final_obj={float(final_eval['objective']):.6f} ev={float(final_eval['ev_net_usdc_p50']):.6f}"
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
        if args.cycle_sleep_sec > 0:
            time.sleep(args.cycle_sleep_sec)

    if last_audit_file is not None:
        print(f"wrote={last_audit_file}")
    if last_daily_state_file is not None:
        print(f"wrote={last_daily_state_file}")
    if state_file.exists():
        print(f"wrote={state_file}")
    else:
        print(f"state_not_written={state_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
