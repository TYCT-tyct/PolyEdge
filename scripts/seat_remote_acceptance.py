#!/usr/bin/env python3
"""Remote SEAT acceptance checker for Ireland runtime.

This script validates the production acceptance goals without modifying remote state:
1) Runtime reached Layer2+ after >=72h and >=800 trades.
2) At least one challenger cycle decision is present.
3) Reports and decision trail are retrievable remotely.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


def utc_day(ts: float | None = None) -> str:
    dt = datetime.fromtimestamp(ts or time.time(), tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def run_ssh_json(host: str, user: str, key: str, remote_cmd: str, timeout: int = 12) -> Dict[str, Any]:
    cmd = [
        "ssh",
        "-i",
        key,
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "BatchMode=yes",
        f"{user}@{host}",
        remote_cmd,
    ]
    try:
        proc = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as err:
        raise RuntimeError(f"ssh timeout after {timeout}s: {err}") from err
    if proc.returncode != 0:
        raise RuntimeError(f"ssh failed ({proc.returncode}): {proc.stderr.strip()}")
    raw = proc.stdout.strip()
    if not raw:
        raise RuntimeError("ssh returned empty output")
    return json.loads(raw)


def fetch_status(host: str, user: str, key: str, base_url: str) -> Dict[str, Any]:
    return run_ssh_json(
        host,
        user,
        key,
        f"curl -fsSL '{base_url.rstrip('/')}/report/seat/status'",
    )


def fetch_history(host: str, user: str, key: str, base_url: str, limit: int) -> List[Dict[str, Any]]:
    payload = run_ssh_json(
        host,
        user,
        key,
        f"curl -fsSL '{base_url.rstrip('/')}/report/seat/history?limit={int(limit)}'",
    )
    if isinstance(payload, list):
        return payload
    return []


@dataclass
class AcceptanceState:
    ready_layer2: bool
    challenger_seen: bool
    last_layer: str
    trade_count: int
    uptime_hours: float
    reasons: List[str]


def evaluate(status: Dict[str, Any], history: List[Dict[str, Any]]) -> AcceptanceState:
    now_ms = int(time.time() * 1000)
    started_ms = int(status.get("started_ms", 0) or 0)
    uptime_hours = max(0.0, (now_ms - started_ms) / 1000.0 / 3600.0)

    layer = str(status.get("current_layer", "unknown"))
    trade_count = int(status.get("trade_count", 0) or 0)

    ready_layer2 = layer in {"layer2", "layer3"} and trade_count >= 800 and uptime_hours >= 72.0

    decisions = [str(row.get("decision", "")) for row in history if isinstance(row, dict)]
    challenger_seen = any(dec.startswith("shadow_started") for dec in decisions) and any(
        dec.startswith("monitor_pass") or dec.startswith("shadow_reject") for dec in decisions
    )

    reasons: List[str] = []
    if not ready_layer2:
        reasons.append(
            f"layer/trade/uptime not ready: layer={layer}, trades={trade_count}, uptime_h={uptime_hours:.2f}"
        )
    if not challenger_seen:
        reasons.append("challenger cycle not seen in decision history")

    return AcceptanceState(
        ready_layer2=ready_layer2,
        challenger_seen=challenger_seen,
        last_layer=layer,
        trade_count=trade_count,
        uptime_hours=uptime_hours,
        reasons=reasons,
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate remote SEAT Layer2 acceptance on Ireland node")
    p.add_argument("--host", default="54.77.232.166")
    p.add_argument("--user", default="ubuntu")
    p.add_argument("--key", default=r"C:\Users\Shini\Documents\PolyEdge.pem")
    p.add_argument("--base-url", default="http://127.0.0.1:8080")
    p.add_argument("--history-limit", type=int, default=400)
    p.add_argument("--watch-sec", type=int, default=0, help="poll until timeout; 0 = single shot")
    p.add_argument("--poll-sec", type=int, default=30)
    p.add_argument("--run-id", default=f"seat-remote-accept-{int(time.time())}")
    p.add_argument("--out-root", default="datasets/reports")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    deadline = time.time() + max(0, args.watch_sec)

    snapshots: List[Dict[str, Any]] = []
    final_state: AcceptanceState | None = None

    while True:
        try:
            status = fetch_status(args.host, args.user, args.key, args.base_url)
            history = fetch_history(args.host, args.user, args.key, args.base_url, args.history_limit)
            state = evaluate(status, history)
            final_state = state

            snapshots.append(
                {
                    "ts_ms": int(time.time() * 1000),
                    "status": status,
                    "history_size": len(history),
                    "evaluation": {
                        "ready_layer2": state.ready_layer2,
                        "challenger_seen": state.challenger_seen,
                        "layer": state.last_layer,
                        "trade_count": state.trade_count,
                        "uptime_hours": state.uptime_hours,
                        "reasons": state.reasons,
                    },
                }
            )
        except Exception as err:  # noqa: BLE001
            state = AcceptanceState(
                ready_layer2=False,
                challenger_seen=False,
                last_layer="unreachable",
                trade_count=0,
                uptime_hours=0.0,
                reasons=[f"remote_probe_failed: {err}"],
            )
            final_state = state
            snapshots.append(
                {
                    "ts_ms": int(time.time() * 1000),
                    "error": str(err),
                    "evaluation": {
                        "ready_layer2": False,
                        "challenger_seen": False,
                        "layer": "unreachable",
                        "trade_count": 0,
                        "uptime_hours": 0.0,
                        "reasons": state.reasons,
                    },
                }
            )

        if state.ready_layer2 and state.challenger_seen:
            break
        if args.watch_sec <= 0 or time.time() >= deadline:
            break
        time.sleep(max(5, args.poll_sec))

    assert final_state is not None
    accepted = final_state.ready_layer2 and final_state.challenger_seen

    out_dir = Path(args.out_root) / utc_day() / "runs" / args.run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "seat_remote_acceptance.json"
    out = {
        "meta": {
            "ts_ms": int(time.time() * 1000),
            "host": args.host,
            "base_url": args.base_url,
            "history_limit": args.history_limit,
            "watch_sec": args.watch_sec,
            "poll_sec": args.poll_sec,
            "run_id": args.run_id,
        },
        "accepted": accepted,
        "final": {
            "ready_layer2": final_state.ready_layer2,
            "challenger_seen": final_state.challenger_seen,
            "layer": final_state.last_layer,
            "trade_count": final_state.trade_count,
            "uptime_hours": final_state.uptime_hours,
            "reasons": final_state.reasons,
        },
        "snapshots": snapshots,
    }
    out_path.write_text(json.dumps(out, ensure_ascii=True, indent=2), encoding="utf-8")

    print(f"accepted={accepted}")
    print(f"layer={final_state.last_layer}")
    print(f"trade_count={final_state.trade_count}")
    print(f"uptime_hours={final_state.uptime_hours:.2f}")
    print(f"report={out_path}")

    return 0 if accepted else 3


if __name__ == "__main__":
    raise SystemExit(main())
