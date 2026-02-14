#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests


def now_ms() -> int:
    return int(time.time() * 1000)


def utc_day(ts_ms: int | None = None) -> str:
    dt = datetime.fromtimestamp((ts_ms or now_ms()) / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def write_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=True, separators=(",", ":")) + "\n")


def fetch_json(session: requests.Session, url: str, timeout_sec: float) -> Dict[str, Any]:
    resp = session.get(url, timeout=timeout_sec)
    resp.raise_for_status()
    return resp.json()


def evaluate_alerts(
    health: Dict[str, Any],
    live: Dict[str, Any],
    toxicity: Dict[str, Any],
    max_ref_freshness_ms: int,
    max_book_freshness_ms: int,
    max_tick_to_ack_p99_ms: float,
    max_block_ratio: float,
    min_attempt_for_block_ratio: int,
) -> List[Dict[str, Any]]:
    alerts: List[Dict[str, Any]] = []

    if health.get("status") != "ok":
        alerts.append({"type": "service_unhealthy", "detail": f"health={health}"})
    if health.get("paused") is True:
        alerts.append({"type": "service_paused", "detail": "trading engine paused"})

    ref_freshness = int(live.get("ref_freshness_ms", 10**9))
    book_freshness = int(live.get("book_freshness_ms", 10**9))
    if ref_freshness > max_ref_freshness_ms:
        alerts.append(
            {
                "type": "ref_feed_stale",
                "detail": f"ref_freshness_ms={ref_freshness} > {max_ref_freshness_ms}",
            }
        )
    if book_freshness > max_book_freshness_ms:
        alerts.append(
            {
                "type": "book_feed_stale",
                "detail": f"book_freshness_ms={book_freshness} > {max_book_freshness_ms}",
            }
        )

    tick_to_ack_p99 = float(live.get("tick_to_ack_p99_ms", 0.0))
    if tick_to_ack_p99 > max_tick_to_ack_p99_ms:
        alerts.append(
            {
                "type": "e2e_latency_high",
                "detail": (
                    f"tick_to_ack_p99_ms={tick_to_ack_p99:.3f} > "
                    f"{max_tick_to_ack_p99_ms:.3f}"
                ),
            }
        )

    attempted = int(live.get("quote_attempted", 0))
    blocked = int(live.get("quote_blocked", 0))
    block_ratio = float(live.get("quote_block_ratio", 0.0))
    policy_block_ratio = float(live.get("policy_block_ratio", 0.0))
    if attempted >= min_attempt_for_block_ratio and block_ratio > max_block_ratio:
        alerts.append(
            {
                "type": "quote_reject_ratio_high",
                "detail": (
                    f"quote_block_ratio={block_ratio:.4f} > {max_block_ratio:.4f} "
                    f"(attempted={attempted}, blocked={blocked})"
                ),
            }
        )
    if attempted >= min_attempt_for_block_ratio and policy_block_ratio > max_block_ratio:
        alerts.append(
            {
                "type": "policy_block_ratio_high",
                "detail": (
                    f"policy_block_ratio={policy_block_ratio:.4f} > {max_block_ratio:.4f} "
                    f"(attempted={attempted}, blocked={blocked})"
                ),
            }
        )

    total_shots = int(live.get("total_shots", 0))
    if attempted >= min_attempt_for_block_ratio and total_shots == 0:
        alerts.append(
            {
                "type": "no_shadow_shots",
                "detail": f"quote_attempted={attempted}, total_shots=0",
            }
        )

    gate_fail_reasons = []
    if float(live.get("fillability_10ms", 0.0)) < 0.60:
        gate_fail_reasons.append("fillability_10ms<0.60")
    if float(live.get("net_edge_p50_bps", 0.0)) <= 0.0:
        gate_fail_reasons.append("net_edge_p50_bps<=0")
    pnl_robust = float(
        live.get("pnl_10s_p50_bps_robust", live.get("pnl_10s_p50_bps", 0.0))
    )
    if pnl_robust <= 0.0:
        gate_fail_reasons.append("pnl_10s_p50_bps_robust<=0")
    if gate_fail_reasons:
        alerts.append(
            {
                "type": "gate_pressure",
                "detail": ",".join(gate_fail_reasons),
            }
        )

    avg_tox = float(toxicity.get("average_tox_score", 0.0)) if toxicity else 0.0
    danger_count = int(toxicity.get("danger_count", 0)) if toxicity else 0
    active_count = 0
    if toxicity:
        rows = toxicity.get("rows", []) or []
        active_count = sum(1 for r in rows if bool(r.get("active_for_quoting", False)))
    if avg_tox > 0.65:
        alerts.append(
            {
                "type": "toxicity_high",
                "detail": f"average_tox_score={avg_tox:.4f} > 0.6500",
            }
        )
    if danger_count > 0:
        alerts.append(
            {
                "type": "toxicity_danger_markets",
                "detail": f"danger_count={danger_count}",
            }
        )
    if toxicity and active_count == 0:
        alerts.append(
            {
                "type": "no_active_markets",
                "detail": "active_for_quoting_count=0",
            }
        )

    return alerts


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PolyEdge runtime watchdog")
    p.add_argument("--base-url", default="http://127.0.0.1:8080")
    p.add_argument("--out-root", default="datasets/reports")
    p.add_argument("--interval-sec", type=float, default=30.0)
    p.add_argument("--timeout-sec", type=float, default=3.0)
    p.add_argument("--max-ref-freshness-ms", type=int, default=5000)
    p.add_argument("--max-book-freshness-ms", type=int, default=5000)
    p.add_argument("--max-tick-to-ack-p99-ms", type=float, default=450.0)
    p.add_argument("--max-block-ratio", type=float, default=0.3)
    p.add_argument("--min-attempt-for-block-ratio", type=int, default=100)
    p.add_argument("--once", action="store_true")
    return p.parse_args()


def run_once(session: requests.Session, args: argparse.Namespace) -> int:
    ts = now_ms()
    day = utc_day(ts)
    out_dir = Path(args.out_root) / day
    monitor_file = out_dir / "monitor_live.jsonl"
    alert_file = out_dir / "monitor_alerts.jsonl"

    record: Dict[str, Any] = {"ts_ms": ts}
    status_code = 0
    try:
        health = fetch_json(session, f"{args.base_url}/health", args.timeout_sec)
        live = fetch_json(session, f"{args.base_url}/report/shadow/live", args.timeout_sec)
        toxicity = fetch_json(session, f"{args.base_url}/report/toxicity/live", args.timeout_sec)
        record["health"] = health
        record["live"] = live
        record["toxicity"] = toxicity
        alerts = evaluate_alerts(
            health=health,
            live=live,
            toxicity=toxicity,
            max_ref_freshness_ms=args.max_ref_freshness_ms,
            max_book_freshness_ms=args.max_book_freshness_ms,
            max_tick_to_ack_p99_ms=args.max_tick_to_ack_p99_ms,
            max_block_ratio=args.max_block_ratio,
            min_attempt_for_block_ratio=args.min_attempt_for_block_ratio,
        )
        write_jsonl(monitor_file, record)
        for a in alerts:
            alert_payload = {"ts_ms": ts, **a}
            write_jsonl(alert_file, alert_payload)
            print(f"[ALERT] {a['type']}: {a['detail']}")
    except Exception as exc:
        status_code = 1
        record["error"] = str(exc)
        write_jsonl(monitor_file, record)
        write_jsonl(
            alert_file,
            {"ts_ms": ts, "type": "watchdog_exception", "detail": str(exc)},
        )
        print(f"[ALERT] watchdog_exception: {exc}")
    return status_code


def main() -> int:
    args = parse_args()
    session = requests.Session()

    if args.once:
        return run_once(session, args)

    print("runtime_watchdog started")
    while True:
        run_once(session, args)
        time.sleep(max(1.0, args.interval_sec))


if __name__ == "__main__":
    sys.exit(main())
