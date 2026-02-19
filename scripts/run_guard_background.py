#!/usr/bin/env python3
"""Run seat_transport_guard.py as a background child with heartbeat logging."""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


def utc_day(ts: float | None = None) -> str:
    dt = datetime.fromtimestamp(ts or time.time(), tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Background wrapper for seat_transport_guard.py")
    p.add_argument("--run-id", required=True)
    p.add_argument("--base-url", default="http://127.0.0.1:8080")
    p.add_argument("--out-root", default="datasets/reports")
    p.add_argument("--profile", default="quick_60s", choices=["quick_60s", "quick", "standard", "deep"])
    p.add_argument("--dedupe-window-ms", type=int, default=8)
    p.add_argument("--udp-share-cap", type=float, default=0.35)
    p.add_argument("--jitter-threshold-ms", type=float, default=25.0)
    p.add_argument("--fallback-arm-duration-ms", type=int, default=8000)
    p.add_argument("--fallback-cooldown-sec", type=int, default=300)
    p.add_argument("--udp-local-only", choices=["true", "false"], default="true")
    p.add_argument("--warmup-sec", type=int, default=5)
    p.add_argument("--storm-duration-sec", type=int, default=60)
    p.add_argument("--storm-concurrency", type=int, default=8)
    p.add_argument("--storm-burst-rps", type=int, default=20)
    p.add_argument("--skip-storm", action="store_true")
    p.add_argument("--heartbeat-sec", type=int, default=5)
    p.add_argument("--timeout-sec", type=int, default=3600)
    return p.parse_args()


def tail_last_line(path: Path) -> str:
    if not path.exists():
        return ""
    data = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    for line in reversed(data):
        if line.strip():
            return line.strip()
    return ""


def main() -> int:
    args = parse_args()
    run_dir = Path(args.out_root) / utc_day() / "runs" / args.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "seat_transport_guard_live.log"

    cmd = [
        sys.executable,
        "scripts/seat_transport_guard.py",
        "--run-id",
        args.run_id,
        "--base-url",
        args.base_url,
        "--out-root",
        args.out_root,
        "--profile",
        args.profile,
        "--dedupe-window-ms",
        str(args.dedupe_window_ms),
        "--udp-share-cap",
        str(args.udp_share_cap),
        "--jitter-threshold-ms",
        str(args.jitter_threshold_ms),
        "--fallback-arm-duration-ms",
        str(args.fallback_arm_duration_ms),
        "--fallback-cooldown-sec",
        str(args.fallback_cooldown_sec),
        "--udp-local-only",
        args.udp_local_only,
        "--warmup-sec",
        str(args.warmup_sec),
        "--storm-duration-sec",
        str(args.storm_duration_sec),
        "--storm-concurrency",
        str(args.storm_concurrency),
        "--storm-burst-rps",
        str(args.storm_burst_rps),
    ]
    if args.skip_storm:
        cmd.append("--skip-storm")

    started = time.time()
    with log_path.open("w", encoding="utf-8") as log_file:
        proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
        print(f"guard_pid={proc.pid}")
        print(f"log={log_path}")
        while True:
            rc = proc.poll()
            elapsed = int(time.time() - started)
            if rc is not None:
                print(f"exit={rc} elapsed={elapsed}s")
                summary = run_dir / "seat_transport_guard_summary.json"
                print(f"summary={summary}")
                return rc
            if elapsed >= args.timeout_sec:
                proc.kill()
                print(f"timeout after {elapsed}s")
                return 124
            line = tail_last_line(log_path)
            if line:
                print(f"[hb] elapsed={elapsed}s last={line}")
            else:
                print(f"[hb] elapsed={elapsed}s waiting-for-first-log-line")
            time.sleep(max(1, args.heartbeat_sec))


if __name__ == "__main__":
    raise SystemExit(main())
