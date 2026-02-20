#!/usr/bin/env python3
"""SEAT transport guard: A/B validate candidate transport mode and auto-rollback on regression."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests


def utc_day(ts: float | None = None) -> str:
    dt = datetime.fromtimestamp(ts or datetime.now(timezone.utc).timestamp(), tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[guard][{ts}] {msg}", flush=True)


def run(cmd: List[str]) -> None:
    started = time.time()
    log(f"run: {' '.join(cmd)}")
    proc = subprocess.run(cmd, check=False)
    elapsed = time.time() - started
    log(f"exit={proc.returncode} elapsed={elapsed:.1f}s")
    if proc.returncode != 0:
        raise RuntimeError(f"command failed (exit={proc.returncode}): {' '.join(cmd)}")


def latest_sweep(run_dir: Path) -> dict:
    files = sorted(run_dir.glob("full_latency_sweep_*.json"))
    if not files:
        raise FileNotFoundError(f"no full_latency_sweep_*.json under {run_dir}")
    return json.loads(files[-1].read_text(encoding="utf-8"))


def storm_summary(run_dir: Path) -> dict:
    path = run_dir / "storm_test_summary.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def metric(sweep: dict, key: str, stat: str) -> Optional[float]:
    try:
        v = sweep["engine"]["stats"][key][stat]
        return float(v)
    except Exception:
        raise  # Linus: Fail loudly and explicitly
def udp_share(sweep: dict) -> float:
    try:
        mix = sweep["engine"]["last"]["source_mix_ratio"]
        return float(mix.get("binance_udp", 0.0))
    except Exception:
        raise  # Linus: Fail loudly and explicitly
def storm_p99(summary: dict) -> Optional[float]:
    try:
        return float(summary["latency_ms"]["p99"])
    except Exception:
        raise  # Linus: Fail loudly and explicitly
@dataclass
class Check:
    key: str
    stat: str
    direction: str  # lower|higher
    abs_tol: float
    rel_tol: float


def check_regression(
    baseline: dict,
    candidate: dict,
    checks: List[Check],
) -> dict:
    rows: List[dict] = []
    regressed = False
    for spec in checks:
        b = metric(baseline, spec.key, spec.stat)
        c = metric(candidate, spec.key, spec.stat)
        if b is None or c is None:
            rows.append(
                {
                    "key": spec.key,
                    "stat": spec.stat,
                    "status": "missing",
                    "baseline": b,
                    "candidate": c,
                }
            )
            continue

        delta = c - b
        threshold = max(spec.abs_tol, abs(b) * spec.rel_tol)
        if spec.direction == "lower":
            is_regressed = delta > threshold
            is_improved = delta < -threshold
        else:
            is_regressed = -delta > threshold
            is_improved = delta > threshold

        status = "flat"
        if is_regressed:
            status = "regressed"
            regressed = True
        elif is_improved:
            status = "improved"

        rows.append(
            {
                "key": spec.key,
                "stat": spec.stat,
                "status": status,
                "baseline": b,
                "candidate": c,
                "delta": delta,
                "threshold": threshold,
                "direction": spec.direction,
            }
        )
    return {"rows": rows, "regressed": regressed}


def build_fusion_payload(
    mode: str,
    dedupe_window_ms: int,
    udp_share_cap: Optional[float],
    jitter_threshold_ms: Optional[float],
    fallback_arm_duration_ms: Optional[int],
    fallback_cooldown_sec: Optional[int],
    udp_local_only: Optional[bool],
) -> dict:
    if mode == "direct_only":
        payload: dict = {
            "enable_udp": False,
            "mode": "direct_only",
            "dedupe_window_ms": int(dedupe_window_ms),
        }
    else:
        payload = {
            "enable_udp": True,
            "mode": mode,
            "dedupe_window_ms": int(dedupe_window_ms),
        }
    if udp_share_cap is not None:
        payload["udp_share_cap"] = float(udp_share_cap)
    if jitter_threshold_ms is not None:
        payload["jitter_threshold_ms"] = float(jitter_threshold_ms)
    if fallback_arm_duration_ms is not None:
        payload["fallback_arm_duration_ms"] = int(fallback_arm_duration_ms)
    if fallback_cooldown_sec is not None:
        payload["fallback_cooldown_sec"] = int(fallback_cooldown_sec)
    if udp_local_only is not None:
        payload["udp_local_only"] = bool(udp_local_only)
    return payload


def post_json(base_url: str, path: str, payload: dict) -> dict:
    resp = requests.post(f"{base_url.rstrip('/')}{path}", json=payload, timeout=8)
    resp.raise_for_status()
    return resp.json()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SEAT transport A/B guard with automatic rollback")
    p.add_argument("--base-url", default="http://127.0.0.1:8080")
    p.add_argument("--out-root", default="datasets/reports")
    p.add_argument("--run-id", required=True)
    p.add_argument(
        "--profile",
        default="quick_60s",
        choices=["quick_60s", "quick", "standard", "deep"],
    )
    p.add_argument("--baseline-mode", default="direct_only", choices=["direct_only"])
    p.add_argument(
        "--candidate-mode",
        default="websocket_primary",
        choices=["websocket_primary", "active_active", "udp_only"],
    )
    p.add_argument("--dedupe-window-ms", type=int, default=8)
    p.add_argument("--storm-duration-sec", type=int, default=60)
    p.add_argument("--storm-concurrency", type=int, default=8)
    p.add_argument("--storm-burst-rps", type=int, default=20)
    p.add_argument("--warmup-sec", type=int, default=5)
    p.add_argument("--udp-share-cap", type=float, default=0.35)
    p.add_argument("--jitter-threshold-ms", type=float, default=25.0)
    p.add_argument("--fallback-arm-duration-ms", type=int, default=8000)
    p.add_argument("--fallback-cooldown-sec", type=int, default=300)
    p.add_argument("--udp-local-only", choices=["true", "false"], default="true")
    p.add_argument(
        "--skip-storm",
        action="store_true",
        help="skip storm_test.py phase for quick transport-only validation",
    )
    p.add_argument(
        "--no-rollback",
        action="store_true",
        help="do not apply rollback when regression is detected",
    )
    return p.parse_args()


def run_phase(
    mode: str,
    run_id: str,
    args: argparse.Namespace,
    include_candidate_params: bool,
) -> Path:
    sweep_cmd = [
        sys.executable,
        "scripts/full_latency_sweep.py",
        "--profile",
        args.profile,
        "--base-url",
        args.base_url,
        "--fusion-mode",
        mode,
        "--dedupe-window-ms",
        str(args.dedupe_window_ms),
        "--skip-ws",
        "--out-root",
        args.out_root,
        "--run-id",
        run_id,
        "--reset-shadow",
        "--warmup-sec",
        str(args.warmup_sec),
        "--progress-sec",
        "5",
    ]
    if include_candidate_params:
        sweep_cmd.extend(
            [
                "--udp-share-cap",
                str(args.udp_share_cap),
                "--jitter-threshold-ms",
                str(args.jitter_threshold_ms),
                "--fallback-arm-duration-ms",
                str(args.fallback_arm_duration_ms),
                "--fallback-cooldown-sec",
                str(args.fallback_cooldown_sec),
                "--udp-local-only",
                str(args.udp_local_only),
            ]
        )
    run(sweep_cmd)

    if not args.skip_storm:
        storm_cmd = [
            sys.executable,
            "scripts/storm_test.py",
            "--base-url",
            args.base_url,
            "--duration-sec",
            str(args.storm_duration_sec),
            "--concurrency",
            str(args.storm_concurrency),
            "--burst-rps",
            str(args.storm_burst_rps),
            "--out-root",
            args.out_root,
            "--run-id",
            run_id,
            "--use-run-dir",
        ]
        run(storm_cmd)
    return Path(args.out_root) / utc_day() / "runs" / run_id


def main() -> int:
    args = parse_args()
    baseline_run_id = f"{args.run_id}-baseline"
    candidate_run_id = f"{args.run_id}-candidate"

    log(
        f"start run_id={args.run_id} baseline={args.baseline_mode} "
        f"candidate={args.candidate_mode} profile={args.profile} skip_storm={args.skip_storm}"
    )
    log("phase=baseline")
    baseline_dir = run_phase(args.baseline_mode, baseline_run_id, args, False)
    log("phase=candidate")
    candidate_dir = run_phase(args.candidate_mode, candidate_run_id, args, True)

    base_sweep = latest_sweep(baseline_dir)
    cand_sweep = latest_sweep(candidate_dir)
    base_storm = storm_summary(baseline_dir)
    cand_storm = storm_summary(candidate_dir)

    checks = [
        Check("tick_to_decision_p99_ms", "p99", "lower", 0.08, 0.20),
        Check("tick_to_ack_p99_ms", "p99", "lower", 0.08, 0.20),
        Check("decision_queue_wait_p99_ms", "p99", "lower", 0.08, 0.20),
        Check("source_latency_p99_ms", "p99", "lower", 5.0, 0.12),
        Check("policy_block_ratio", "p50", "lower", 0.03, 0.20),
        Check("survival_probe_10ms", "p50", "higher", 0.02, 0.05),
        Check("fillability_10ms", "p50", "higher", 0.02, 0.05),
        Check("ev_net_usdc_p50", "p50", "higher", 0.00015, 0.12),
    ]

    regression = check_regression(base_sweep, cand_sweep, checks)
    base_udp_share = udp_share(base_sweep)
    cand_udp_share = udp_share(cand_sweep)
    cap_regressed = cand_udp_share > (args.udp_share_cap + 0.05)

    base_storm_p99 = storm_p99(base_storm)
    cand_storm_p99 = storm_p99(cand_storm)
    storm_regressed = False
    if base_storm_p99 is not None and cand_storm_p99 is not None:
        storm_threshold = max(5.0, abs(base_storm_p99) * 0.25)
        storm_regressed = (cand_storm_p99 - base_storm_p99) > storm_threshold

    regressed = regression["regressed"] or cap_regressed or storm_regressed
    rollback_applied = False
    rollback_resp: dict | None = None

    if regressed and not args.no_rollback:
        log("regression detected; applying rollback to direct_only")
        rollback_payload = build_fusion_payload(
            mode="direct_only",
            dedupe_window_ms=args.dedupe_window_ms,
            udp_share_cap=None,
            jitter_threshold_ms=None,
            fallback_arm_duration_ms=None,
            fallback_cooldown_sec=None,
            udp_local_only=None,
        )
        rollback_resp = post_json(args.base_url, "/control/reload_fusion", rollback_payload)
        post_json(args.base_url, "/control/reset_shadow", {})
        post_json(args.base_url, "/control/resume", {})
        rollback_applied = True
    elif regressed:
        log("regression detected; rollback skipped by --no-rollback")
    else:
        log("no regression detected")

    summary = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "run_id": args.run_id,
        "baseline_run_id": baseline_run_id,
        "candidate_run_id": candidate_run_id,
        "baseline_dir": str(baseline_dir),
        "candidate_dir": str(candidate_dir),
        "baseline_mode": args.baseline_mode,
        "candidate_mode": args.candidate_mode,
        "regressed": regressed,
        "rollback_applied": rollback_applied,
        "rollback_response": rollback_resp,
        "udp_share_cap_target": args.udp_share_cap,
        "baseline_udp_share": base_udp_share,
        "candidate_udp_share": cand_udp_share,
        "udp_share_cap_regressed": cap_regressed,
        "baseline_storm_p99_ms": base_storm_p99,
        "candidate_storm_p99_ms": cand_storm_p99,
        "storm_regressed": storm_regressed,
        "metric_checks": regression["rows"],
    }

    out_dir = Path(args.out_root) / utc_day() / "runs" / args.run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "seat_transport_guard_summary.json"
    out_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")

    print(f"baseline_dir={baseline_dir}")
    print(f"candidate_dir={candidate_dir}")
    print(f"regressed={regressed}")
    print(f"rollback_applied={rollback_applied}")
    print(f"summary={out_path}")
    return 10 if regressed else 0


if __name__ == "__main__":
    raise SystemExit(main())
