#!/usr/bin/env python3
"""Probe transport paths (GA/PrivateLink/direct) with lightweight HTTP latency checks."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Dict, List

import requests


def utc_day(ts: float | None = None) -> str:
    dt = datetime.fromtimestamp(ts or time.time(), tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    data = sorted(values)
    i = int(round((len(data) - 1) * p))
    i = max(0, min(i, len(data) - 1))
    return float(data[i])


def probe_url(url: str, samples: int, timeout_sec: float, interval_ms: int) -> Dict[str, float]:
    latencies: List[float] = []
    errors = 0
    session = requests.Session()
    for _ in range(max(1, samples)):
        t0 = time.perf_counter()
        try:
            r = session.get(url, timeout=timeout_sec)
            r.raise_for_status()
            latencies.append((time.perf_counter() - t0) * 1000.0)
        except Exception:
            raise  # Linus: Fail loudly and explicitly
        if interval_ms > 0:
            time.sleep(interval_ms / 1000.0)
    return {
        "samples": len(latencies),
        "errors": errors,
        "p50_ms": percentile(latencies, 0.50),
        "p90_ms": percentile(latencies, 0.90),
        "p99_ms": percentile(latencies, 0.99),
        "mean_ms": mean(latencies) if latencies else 0.0,
        "max_ms": max(latencies) if latencies else 0.0,
    }


def parse_targets(raw_targets: str, path: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for token in raw_targets.split(","):
        token = token.strip()
        if not token:
            continue
        if "=" not in token:
            continue
        name, base = token.split("=", 1)
        name = name.strip()
        base = base.strip().rstrip("/")
        if not name or not base:
            continue
        if base.startswith("http://") or base.startswith("https://"):
            out[name] = f"{base}{path}"
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Probe transport paths via HTTP latency")
    p.add_argument(
        "--targets",
        required=True,
        help=(
            "comma list: name=url. "
            "example: direct=http://127.0.0.1:8080,ga=http://10.0.3.10:8080,pl=http://10.0.3.20:8080"
        ),
    )
    p.add_argument("--path", default="/health/latency")
    p.add_argument("--samples", type=int, default=120)
    p.add_argument("--timeout-sec", type=float, default=2.0)
    p.add_argument("--interval-ms", type=int, default=50)
    p.add_argument("--run-id", default=None)
    p.add_argument("--out-root", default="datasets/reports")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    targets = parse_targets(args.targets, args.path)
    if not targets:
        raise SystemExit("no valid targets")
    started_ms = int(time.time() * 1000)
    rows = {}
    for name, url in targets.items():
        print(f"[probe] target={name} url={url}", flush=True)
        rows[name] = probe_url(url, args.samples, args.timeout_sec, args.interval_ms)
        print(f"[probe] {name} p50={rows[name]['p50_ms']:.3f} p99={rows[name]['p99_ms']:.3f} errors={rows[name]['errors']}", flush=True)
    payload = {
        "meta": {
            "ts_ms": started_ms,
            "run_id": args.run_id,
            "path": args.path,
            "samples": args.samples,
            "timeout_sec": args.timeout_sec,
            "interval_ms": args.interval_ms,
        },
        "targets": targets,
        "results": rows,
    }
    day_dir = Path(args.out_root) / utc_day()
    out_dir = day_dir / "runs" / str(args.run_id) if args.run_id else day_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"transport_path_probe_{started_ms}.json"
    out.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    print(f"wrote_json={out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
