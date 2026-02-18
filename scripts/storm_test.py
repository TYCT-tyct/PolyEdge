#!/usr/bin/env python3
from __future__ import annotations

import atexit
import argparse
import concurrent.futures
import json
import random
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


def utc_day(ts: Optional[float] = None) -> str:
    dt = datetime.fromtimestamp(ts or time.time(), tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def now_ms() -> int:
    return int(time.time() * 1000)


def percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    k = (len(values) - 1) * q
    lo = int(k)
    hi = min(lo + 1, len(values) - 1)
    w = k - lo
    s = sorted(values)
    return s[lo] * (1.0 - w) + s[hi] * w


@dataclass
class ProbeResult:
    ok: bool
    latency_ms: float
    status: int
    endpoint: str
    error: str = ""


def request_json(
    session: requests.Session,
    method: str,
    url: str,
    timeout_sec: float,
    payload: Optional[Dict[str, Any]] = None,
) -> ProbeResult:
    started = time.perf_counter()
    try:
        if method == "GET":
            resp = session.get(url, timeout=timeout_sec)
        else:
            resp = session.post(url, timeout=timeout_sec, json=payload or {})
        latency_ms = (time.perf_counter() - started) * 1000.0
        if resp.status_code >= 400:
            return ProbeResult(False, latency_ms, resp.status_code, url, resp.text[:240])
        _ = resp.json()
        return ProbeResult(True, latency_ms, resp.status_code, url)
    except Exception as exc:  # noqa: BLE001
        latency_ms = (time.perf_counter() - started) * 1000.0
        return ProbeResult(False, latency_ms, 0, url, str(exc))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PolyEdge storm test harness (bounded, non-recursive)")
    p.add_argument("--base-url", default="http://127.0.0.1:8080")
    p.add_argument("--duration-sec", type=int, default=300)
    p.add_argument("--burst-rps", type=int, default=20)
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--control-interval-sec", type=float, default=20.0)
    p.add_argument("--timeout-sec", type=float, default=2.5)
    p.add_argument("--max-errors", type=int, default=50)
    p.add_argument("--max-runtime-sec", type=int, default=600)
    p.add_argument("--heartbeat-sec", type=float, default=30.0)
    p.add_argument("--run-id", default=f"storm-{int(time.time())}")
    p.add_argument("--out-root", default="datasets/reports")
    # When enabled, write artifacts under datasets/reports/<day>/runs/<run_id>/ to avoid overwriting
    # day-level legacy files (storm_test_summary.json / storm_test_trace.jsonl).
    p.add_argument("--use-run-dir", action="store_true")
    p.add_argument("--fail-fast-threshold", type=int, default=20)
    # Default is safe: do NOT leave the engine paused after a test. Enable only when
    # explicitly testing pause/resume semantics.
    p.add_argument("--churn-pause-resume", action="store_true")
    p.add_argument("--once", action="store_true")
    # P4: 轻量级探测端点，使用 /health/latency 代替 /report/shadow/live
    p.add_argument("--lightweight", action="store_true", help="使用轻量级 /health/latency 端点而非 /report/shadow/live")
    return p.parse_args()


def maybe_control_churn(
    session: requests.Session, base_url: str, timeout_sec: float, churn_pause_resume: bool
) -> List[ProbeResult]:
    jitter = random.uniform(-2.5, 2.5)
    maker_payload = {
        "min_edge_bps": max(1.0, 6.0 + jitter),
        "taker_trigger_bps": max(1.0, 8.0 + jitter),
        "taker_max_slippage_bps": max(0.5, 4.0 + jitter * 0.2),
        "stale_tick_filter_ms": 500.0,
        "capital_fraction_kelly": 0.35,
        "variance_penalty_lambda": 0.25,
    }
    alloc_payload = {
        "capital_fraction_kelly": 0.35,
        "variance_penalty_lambda": 0.25,
        "active_top_n_markets": 12,
        "taker_weight": 0.7,
        "maker_weight": 0.2,
        "arb_weight": 0.1,
    }
    risk_payload = {
        "daily_drawdown_cap_pct": 0.015,
        "max_loss_streak": 4,
        "cooldown_sec": 120,
    }
    ops: List[tuple[str, str, Dict[str, Any]]] = [
        ("POST", f"{base_url}/control/reload_strategy", maker_payload),
        ("POST", f"{base_url}/control/reload_taker", maker_payload),
        ("POST", f"{base_url}/control/reload_allocator", alloc_payload),
        ("POST", f"{base_url}/control/reload_risk", risk_payload),
    ]
    if churn_pause_resume:
        ops.extend(
            [
                ("POST", f"{base_url}/control/pause", {}),
                ("POST", f"{base_url}/control/resume", {}),
            ]
        )
    out: List[ProbeResult] = []
    for method, url, payload in ops:
        out.append(request_json(session, method, url, timeout_sec, payload))
    return out


def write_summary(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    started = time.monotonic()
    deadline = started + max(1, args.duration_sec)
    hard_deadline = started + max(1, args.max_runtime_sec)
    heartbeat_at = started + max(1.0, args.heartbeat_sec)
    next_control = started + max(1.0, args.control_interval_sec)

    day = utc_day()
    if args.use_run_dir:
        out_dir = Path(args.out_root) / day / "runs" / str(args.run_id)
    else:
        out_dir = Path(args.out_root) / day
    summary_path = out_dir / "storm_test_summary.json"
    trace_path = out_dir / "storm_test_trace.jsonl"
    out_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    atexit.register(session.close)
    errors = 0
    consecutive_failures = 0
    samples: List[ProbeResult] = []

    # P4: 选择端点 - 轻量级或完整报告
    endpoint = "/health/latency" if args.lightweight else "/report/shadow/live"

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as pool:
            if args.once:
                probe = request_json(session, "GET", f"{args.base_url}{endpoint}", args.timeout_sec)
                samples.append(probe)
            else:
                while True:
                    now = time.monotonic()
                    if now >= deadline:
                        break
                    if now >= hard_deadline:
                        break

                    burst = max(1, args.burst_rps)
                    futures = [
                        pool.submit(
                            request_json,
                            session,
                            "GET",
                            f"{args.base_url}{endpoint}",
                            args.timeout_sec,
                            None,
                        )
                        for _ in range(burst)
                    ]
                    for f in futures:
                        r = f.result()
                        samples.append(r)
                        if not r.ok:
                            errors += 1
                            consecutive_failures += 1
                        else:
                            consecutive_failures = 0
                        with trace_path.open("a", encoding="utf-8") as fp:
                            fp.write(
                                json.dumps(
                                    {
                                        "ts_ms": now_ms(),
                                        "ok": r.ok,
                                        "latency_ms": round(r.latency_ms, 3),
                                        "status": r.status,
                                        "endpoint": r.endpoint,
                                        "error": r.error,
                                    },
                                    ensure_ascii=True,
                                    separators=(",", ":"),
                                )
                                + "\n"
                            )

                    if now >= next_control:
                        for ctrl in maybe_control_churn(
                            session, args.base_url, args.timeout_sec, args.churn_pause_resume
                        ):
                            samples.append(ctrl)
                            if not ctrl.ok:
                                errors += 1
                                consecutive_failures += 1
                        next_control = now + max(1.0, args.control_interval_sec)

                    if consecutive_failures >= max(1, args.fail_fast_threshold):
                        break
                    if errors >= max(1, args.max_errors):
                        break
                    if now >= heartbeat_at:
                        elapsed = int(now - started)
                        print(
                            f"[heartbeat] elapsed={elapsed}s samples={len(samples)} errors={errors} "
                            f"consecutive_failures={consecutive_failures}"
                        )
                        heartbeat_at = now + max(1.0, args.heartbeat_sec)
                    time.sleep(1.0)
    finally:
        # Safety: never leave the engine paused after a stress run.
        _ = request_json(session, "POST", f"{args.base_url}/control/resume", 2.0, {})

    lat_ok = [s.latency_ms for s in samples if s.ok]
    lat_fail = [s.latency_ms for s in samples if not s.ok]
    summary: Dict[str, Any] = {
        "ts_ms": now_ms(),
        "run_id": args.run_id,
        "use_run_dir": bool(args.use_run_dir),
        "out_dir": str(out_dir),
        "base_url": args.base_url,
        "duration_sec": int(time.monotonic() - started),
        "sample_count": len(samples),
        "ok_count": sum(1 for s in samples if s.ok),
        "error_count": errors,
        "consecutive_failures": consecutive_failures,
        "latency_ms": {
            "p50": percentile(lat_ok, 0.50),
            "p90": percentile(lat_ok, 0.90),
            "p99": percentile(lat_ok, 0.99),
            "mean": statistics.mean(lat_ok) if lat_ok else 0.0,
            "max": max(lat_ok) if lat_ok else 0.0,
            "fail_mean": statistics.mean(lat_fail) if lat_fail else 0.0,
        },
        "status_histogram": {
            str(code): sum(1 for s in samples if s.status == code)
            for code in sorted({s.status for s in samples})
        },
    }
    write_summary(summary_path, summary)
    print(f"storm_test summary: {summary_path}")

    failed = errors >= max(1, args.max_errors) or consecutive_failures >= max(1, args.fail_fast_threshold)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
