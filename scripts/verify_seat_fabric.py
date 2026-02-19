#!/usr/bin/env python3
"""Verify SEAT Latency Fabric implementation status and runtime readiness.

Checks:
- Runtime control plane + live report fields
- Transport path reachability (direct / GA / optional PrivateLink)
- Static config expectations in configs/strategy.toml and configs/execution.toml
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Optional

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
            errors += 1
        if interval_ms > 0:
            time.sleep(interval_ms / 1000.0)
    return {
        "samples": len(latencies),
        "errors": errors,
        "p50_ms": percentile(latencies, 0.50),
        "p90_ms": percentile(latencies, 0.90),
        "p99_ms": percentile(latencies, 0.99),
        "mean_ms": mean(latencies) if latencies else 0.0,
    }


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str
    extra: Optional[Dict[str, Any]] = None


def parse_simple_toml(path: Path) -> Dict[str, Dict[str, str]]:
    raw = path.read_text(encoding="utf-8")
    out: Dict[str, Dict[str, str]] = {}
    section = ""
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1].strip()
            out.setdefault(section, {})
            continue
        if "=" not in line or not section:
            continue
        k, v = line.split("=", 1)
        out[section][k.strip()] = v.strip().strip('"').strip("'")
    return out


def check_runtime(base_url: str) -> List[CheckResult]:
    checks: List[CheckResult] = []
    try:
        health = requests.get(f"{base_url.rstrip('/')}/health", timeout=4)
        health.raise_for_status()
        payload = health.json()
        checks.append(
            CheckResult(
                name="runtime_health",
                ok=payload.get("status") == "ok",
                detail=f"status={payload.get('status')}",
                extra=payload,
            )
        )
    except Exception as err:
        checks.append(
            CheckResult(
                name="runtime_health",
                ok=False,
                detail=f"request_failed: {err}",
            )
        )
        return checks

    try:
        live = requests.get(f"{base_url.rstrip('/')}/report/shadow/live", timeout=6)
        live.raise_for_status()
        row = live.json()
        required = [
            "udp_share_effective",
            "udp_local_drop_count",
            "share_cap_drop_count",
            "fallback_state",
            "fallback_trigger_reason_distribution",
            "policy_block_ratio",
            "source_latency_p99_ms",
        ]
        missing = [k for k in required if k not in row]
        checks.append(
            CheckResult(
                name="live_report_fields",
                ok=not missing,
                detail="missing=" + ",".join(missing) if missing else "all_present",
                extra={k: row.get(k) for k in required},
            )
        )
    except Exception as err:
        checks.append(
            CheckResult(
                name="live_report_fields",
                ok=False,
                detail=f"request_failed: {err}",
            )
        )
    return checks


def check_static_configs(strategy_toml: Path, execution_toml: Path) -> List[CheckResult]:
    checks: List[CheckResult] = []
    try:
        strat = parse_simple_toml(strategy_toml)
        transport = strat.get("transport", {})
        mode = transport.get("mode")
        udp_local_only = transport.get("udp_local_only", "").lower() == "true"
        cap = float(transport.get("udp_share_cap", "0") or 0)
        checks.append(
            CheckResult(
                name="strategy_transport_section",
                ok=bool(transport),
                detail="present" if transport else "missing [transport] section",
                extra=transport,
            )
        )
        checks.append(
            CheckResult(
                name="transport_websocket_primary_profile",
                ok=(mode == "websocket_primary" and udp_local_only and cap <= 0.35 and cap > 0),
                detail=f"mode={mode},udp_local_only={udp_local_only},udp_share_cap={cap}",
            )
        )
    except Exception as err:
        checks.append(
            CheckResult(
                name="strategy_transport_section",
                ok=False,
                detail=f"parse_failed: {err}",
            )
        )

    try:
        exe = parse_simple_toml(execution_toml)
        ecfg = exe.get("execution", {})
        order_endpoint = ecfg.get("order_endpoint", "")
        backup_endpoint = ecfg.get("order_backup_endpoint", "")
        clob_endpoint = ecfg.get("clob_endpoint", "")
        dual_ok = bool(order_endpoint) and bool(backup_endpoint) and order_endpoint != backup_endpoint
        checks.append(
            CheckResult(
                name="execution_dual_order_endpoint",
                ok=dual_ok,
                detail=f"order_endpoint={order_endpoint},order_backup_endpoint={backup_endpoint}",
                extra={"clob_endpoint": clob_endpoint},
            )
        )
    except Exception as err:
        checks.append(
            CheckResult(
                name="execution_dual_order_endpoint",
                ok=False,
                detail=f"parse_failed: {err}",
            )
        )
    return checks


def check_paths(
    base_url: str,
    ga_url: Optional[str],
    pl_url: Optional[str],
    samples: int,
    timeout_sec: float,
    interval_ms: int,
) -> List[CheckResult]:
    checks: List[CheckResult] = []
    targets = {"direct": base_url.rstrip("/") + "/health/latency"}
    if ga_url:
        targets["ga"] = ga_url.rstrip("/") + "/health/latency"
    if pl_url:
        targets["privatelink"] = pl_url.rstrip("/") + "/health/latency"

    for name, url in targets.items():
        result = probe_url(url, samples=samples, timeout_sec=timeout_sec, interval_ms=interval_ms)
        ok = result["errors"] == 0 and result["samples"] > 0
        checks.append(
            CheckResult(
                name=f"path_{name}",
                ok=ok,
                detail=f"url={url}, errors={int(result['errors'])}, p99_ms={result['p99_ms']:.3f}",
                extra=result,
            )
        )
    return checks


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Verify SEAT latency fabric implementation/readiness")
    p.add_argument("--base-url", default="http://127.0.0.1:8080")
    p.add_argument("--ga-url", default=None)
    p.add_argument("--privatelink-url", default=None)
    p.add_argument("--strategy-toml", default="configs/strategy.toml")
    p.add_argument("--execution-toml", default="configs/execution.toml")
    p.add_argument("--samples", type=int, default=30)
    p.add_argument("--timeout-sec", type=float, default=2.0)
    p.add_argument("--interval-ms", type=int, default=50)
    p.add_argument("--run-id", default=None)
    p.add_argument("--out-root", default="datasets/reports")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    checks: List[CheckResult] = []
    checks.extend(check_runtime(args.base_url))
    checks.extend(
        check_static_configs(
            strategy_toml=Path(args.strategy_toml),
            execution_toml=Path(args.execution_toml),
        )
    )
    checks.extend(
        check_paths(
            base_url=args.base_url,
            ga_url=args.ga_url,
            pl_url=args.privatelink_url,
            samples=args.samples,
            timeout_sec=args.timeout_sec,
            interval_ms=args.interval_ms,
        )
    )

    all_ok = all(c.ok for c in checks)
    payload = {
        "meta": {
            "ts_ms": int(time.time() * 1000),
            "run_id": args.run_id,
            "base_url": args.base_url,
            "ga_url": args.ga_url,
            "privatelink_url": args.privatelink_url,
            "samples": args.samples,
            "timeout_sec": args.timeout_sec,
            "interval_ms": args.interval_ms,
        },
        "all_ok": all_ok,
        "checks": [asdict(c) for c in checks],
    }

    day_dir = Path(args.out_root) / utc_day()
    out_dir = day_dir / "runs" / str(args.run_id) if args.run_id else day_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"seat_fabric_verify_{payload['meta']['ts_ms']}.json"
    out.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

    for c in checks:
        status = "OK" if c.ok else "FAIL"
        print(f"[{status}] {c.name}: {c.detail}")
    print(f"all_ok={all_ok}")
    print(f"wrote_json={out}")
    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
