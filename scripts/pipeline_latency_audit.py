#!/usr/bin/env python3
from __future__ import annotations

import atexit
import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import requests


def utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def default_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"pipeline-latency-{ts}-{os.getpid()}"


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    arr = sorted(values)
    idx = int(round((len(arr) - 1) * p))
    idx = max(0, min(len(arr) - 1, idx))
    return arr[idx]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Full pipeline latency audit for direct/udp/active-active modes")
    p.add_argument("--base-url", default="http://127.0.0.1:18080")
    p.add_argument("--run-id", default=default_run_id())
    p.add_argument("--out-root", default="datasets/reports")
    p.add_argument("--profile", choices=["quick", "standard", "deep"], default="quick")
    p.add_argument("--sample-seconds", type=int, default=90)
    p.add_argument("--poll-interval-sec", type=float, default=2.0)
    p.add_argument("--warmup-sec", type=int, default=8)
    p.add_argument("--skip-sweep", action="store_true")
    p.add_argument("--skip-e2e", action="store_true")
    return p.parse_args()


def post_json(session: requests.Session, base_url: str, path: str, payload: dict) -> dict:
    resp = session.post(f"{base_url.rstrip('/')}{path}", json=payload, timeout=8)
    resp.raise_for_status()
    return resp.json()


def get_json(session: requests.Session, base_url: str, path: str) -> dict:
    resp = session.get(f"{base_url.rstrip('/')}{path}", timeout=8)
    resp.raise_for_status()
    return resp.json()


def run_cmd(cmd: List[str]) -> None:
    subprocess.run(cmd, check=True)


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def e2e_summary(e2e_payload: dict) -> dict:
    stats = (e2e_payload.get("engine") or {}).get("stats") or {}

    def pick(name: str) -> Dict[str, float]:
        obj = stats.get(name) or {}
        if isinstance(obj, dict):
            return {
                "p50": float(obj.get("p50", 0.0) or 0.0),
                "p90": float(obj.get("p90", 0.0) or 0.0),
                "p99": float(obj.get("p99", 0.0) or 0.0),
            }
        return {"p50": 0.0, "p90": 0.0, "p99": 0.0}

    return {
        "tick_to_ack_p99_ms": pick("tick_to_ack_p99_ms"),
        "tick_to_decision_p99_ms": pick("tick_to_decision_p99_ms"),
        "decision_compute_p99_ms": pick("decision_compute_p99_ms"),
        "source_latency_p99_ms": pick("source_latency_p99_ms"),
        "local_backlog_p99_ms": pick("local_backlog_p99_ms"),
        "samples": int((e2e_payload.get("engine") or {}).get("samples") or 0),
        "failures": int((e2e_payload.get("engine") or {}).get("failures") or 0),
    }


def collect_live_samples(
    session: requests.Session,
    base_url: str,
    seconds: int,
    poll_interval: float,
) -> List[dict]:
    samples: List[dict] = []
    deadline = time.time() + max(1, seconds)
    while time.time() < deadline:
        try:
            samples.append(get_json(session, base_url, "/report/shadow/live"))
        except requests.RequestException:
            pass
        time.sleep(max(0.5, poll_interval))
    if not samples:
        samples.append(get_json(session, base_url, "/report/shadow/live"))
    return samples


def summary_from_samples(samples: List[dict]) -> dict:
    def col(name: str) -> List[float]:
        out: List[float] = []
        for row in samples:
            if name in row:
                try:
                    out.append(float(row[name]))
                except Exception:
                    raise  # Linus: Fail loudly and explicitly
        return out

    def col_lat(name: str) -> List[float]:
        out: List[float] = []
        for row in samples:
            lat = row.get("latency") or {}
            if name in lat:
                try:
                    out.append(float(lat[name]))
                except Exception:
                    raise  # Linus: Fail loudly and explicitly
        return out

    def p(vals: List[float], q: float) -> float:
        return percentile(vals, q)

    tick_to_ack = col("tick_to_ack_p99_ms")
    tick_to_decision = col("tick_to_decision_p99_ms")
    decision_compute = col("decision_compute_p99_ms")
    quote_block = col("quote_block_ratio")
    policy_block = col("policy_block_ratio")
    executed_over_eligible = col("executed_over_eligible")
    eligible_count = col("eligible_count")
    executed_count = col("executed_count")
    fillability = col("fillability_10ms")
    ev_net = col("ev_net_usdc_p50")
    ev_positive_ratio = col("ev_positive_ratio")
    data_valid = col("data_valid_ratio")
    lag_half_life = col("lag_half_life_ms")
    outcomes = col("window_outcomes")
    source_latency = col("source_latency_p99_ms")
    local_backlog = col("local_backlog_p99_ms")
    feed_in_p99 = col_lat("feed_in_p99_ms")
    queue_wait_p99 = col_lat("decision_queue_wait_p99_ms")
    compute_p99 = col_lat("decision_compute_p99_ms")

    source_mix: Dict[str, float] = {}
    exit_reason_top: Dict[str, int] = {}
    gate_ready_series_strict: List[float] = []
    gate_ready_series_reported: List[float] = []
    gate_ready_series_effective: List[float] = []
    gate_ready_mismatch_count = 0
    for row in samples:
        strict = bool(row.get("gate_ready_strict", row.get("gate_ready", False)))
        reported = bool(row.get("gate_ready_effective", strict))
        eligible = float(row.get("eligible_count", 0.0) or 0.0)
        executed = float(row.get("executed_count", 0.0) or 0.0)
        effective = reported or eligible > 0.0 or executed > 0.0
        gate_ready_series_strict.append(1.0 if strict else 0.0)
        gate_ready_series_reported.append(1.0 if reported else 0.0)
        gate_ready_series_effective.append(1.0 if effective else 0.0)
        if executed > 0.0 and not reported:
            gate_ready_mismatch_count += 1

        source_mix_row = row.get("source_mix_ratio") or row.get("source_mix_ratio_peak") or {}
        for k, v in source_mix_row.items():
            source_mix[k] = max(source_mix.get(k, 0.0), float(v))
        exit_reason_row = row.get("exit_reason_top") or {}
        if isinstance(exit_reason_row, dict):
            for k, v in exit_reason_row.items():
                try:
                    exit_reason_top[str(k)] = int(v)
                except Exception:
                    raise  # Linus: Fail loudly and explicitly
        elif isinstance(exit_reason_row, list):
            for item in exit_reason_row:
                if isinstance(item, list) and len(item) == 2:
                    exit_reason_top[str(item[0])] = int(item[1])

    return {
        "samples": len(samples),
        "tick_to_ack_p99_ms": {"p50": p(tick_to_ack, 0.50), "p90": p(tick_to_ack, 0.90), "p99": p(tick_to_ack, 0.99)},
        "tick_to_decision_p99_ms": {"p50": p(tick_to_decision, 0.50), "p90": p(tick_to_decision, 0.90), "p99": p(tick_to_decision, 0.99)},
        "decision_compute_p99_ms": {"p50": p(decision_compute, 0.50), "p90": p(decision_compute, 0.90), "p99": p(decision_compute, 0.99)},
        "quote_block_ratio": {"p50": p(quote_block, 0.50), "p90": p(quote_block, 0.90)},
        "policy_block_ratio": {"p50": p(policy_block, 0.50), "p90": p(policy_block, 0.90)},
        "eligible_count": {"p50": p(eligible_count, 0.50), "p90": p(eligible_count, 0.90)},
        "executed_count": {"p50": p(executed_count, 0.50), "p90": p(executed_count, 0.90)},
        "executed_over_eligible": {"p50": p(executed_over_eligible, 0.50)},
        "fillability_10ms": {"p50": p(fillability, 0.50)},
        "ev_net_usdc_p50": {"p50": p(ev_net, 0.50), "p90": p(ev_net, 0.90)},
        "ev_positive_ratio": {"p50": p(ev_positive_ratio, 0.50)},
        "data_valid_ratio": {"p50": p(data_valid, 0.50), "p10": p(data_valid, 0.10)},
        "lag_half_life_ms": {"p50": p(lag_half_life, 0.50), "p90": p(lag_half_life, 0.90)},
        "window_outcomes": {"p50": p(outcomes, 0.50), "max": max(outcomes) if outcomes else 0.0},
        "source_latency_p99_ms": {"p50": p(source_latency, 0.50), "p90": p(source_latency, 0.90)},
        "local_backlog_p99_ms": {"p50": p(local_backlog, 0.50), "p90": p(local_backlog, 0.90)},
        "feed_in_p99_ms": {"p50": p(feed_in_p99, 0.50), "p90": p(feed_in_p99, 0.90)},
        "decision_queue_wait_p99_ms": {"p50": p(queue_wait_p99, 0.50), "p90": p(queue_wait_p99, 0.90)},
        "latency_decision_compute_p99_ms": {"p50": p(compute_p99, 0.50), "p90": p(compute_p99, 0.90)},
        "gate_ready_ratio": p(gate_ready_series_effective, 0.50),
        "gate_ready_ratio_strict": p(gate_ready_series_strict, 0.50),
        "gate_ready_ratio_reported": p(gate_ready_series_reported, 0.50),
        "gate_ready_ratio_effective": p(gate_ready_series_effective, 0.50),
        "gate_ready_mismatch_count": gate_ready_mismatch_count,
        "source_mix_ratio_peak": source_mix,
        "exit_reason_top_last": dict(sorted(exit_reason_top.items(), key=lambda kv: kv[1], reverse=True)[:8]),
        "tick_to_ack_stddev": statistics.pstdev(tick_to_ack) if len(tick_to_ack) > 1 else 0.0,
    }


def mode_payload(mode: str) -> dict:
    if mode == "direct_only":
        return {"enable_udp": False, "mode": "direct_only", "dedupe_window_ms": 30}
    if mode == "udp_only":
        return {"enable_udp": True, "mode": "udp_only", "dedupe_window_ms": 30}
    if mode == "active_active":
        return {"enable_udp": True, "mode": "active_active", "dedupe_window_ms": 30}
    if mode == "websocket_primary":
        return {
            "enable_udp": True,
            "mode": "websocket_primary",
            "dedupe_window_ms": 30,
        }
    raise ValueError(f"unsupported mode: {mode}")


def write_markdown(out_md: Path, payload: dict) -> None:
    lines: List[str] = []
    lines.append("# Full Pipeline Latency Audit")
    lines.append("")
    lines.append(f"- run_id: {payload['run_id']}")
    lines.append(f"- generated_at_utc: {payload['generated_at_utc']}")
    lines.append(f"- base_url: {payload['base_url']}")
    lines.append(f"- profile: {payload['profile']}")
    lines.append("")
    lines.append("## Mode Summary")
    lines.append("")
    lines.append("| mode | tick_to_ack_p99 p50 (ms) | tick_to_decision_p99 p50 (ms) | feed_in_p99 p50 (ms) | decision_compute_p99 p50 (ms) | data_valid p50 | gate_ready_effective |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for mode in payload["modes"]:
        s = payload["modes"][mode]["summary"]
        lines.append(
            "| "
            + f"{mode} | {s['tick_to_ack_p99_ms']['p50']:.6f} | {s['tick_to_decision_p99_ms']['p50']:.6f} | "
            + f"{s['feed_in_p99_ms']['p50']:.6f} | {s['decision_compute_p99_ms']['p50']:.6f} | "
            + f"{s['data_valid_ratio']['p50']:.5f} | {s['gate_ready_ratio_effective']:.6f} |"
        )
    lines.append("")
    lines.append("## End-to-End Flow")
    lines.append("")
    lines.append("```mermaid")
    lines.append("flowchart LR")
    lines.append("    A[Binance/Chainlink ticks] --> B[Tokyo Sender parse + 24-byte pack]")
    lines.append("    B --> C[AWS Backbone UDP]")
    lines.append("    C --> D[Ireland app_runner UDP ingest]")
    lines.append("    A --> E[Ireland direct WS ingest]")
    lines.append("    D --> F[Fusion dedupe + source selection]")
    lines.append("    E --> F")
    lines.append("    F --> G[Time alignment + fair value]")
    lines.append("    G --> H[Edge engine EV_net - penalty]")
    lines.append("    H --> I[Execution router maker/taker]")
    lines.append("    I --> J[Order ACK / fill updates]")
    lines.append("    J --> K[Shadow stats + gate metrics]")
    lines.append("    K --> L[Optimizer champion/challenger]")
    lines.append("```")
    lines.append("")
    lines.append("## Per-Mode Details")
    lines.append("")
    for mode in payload["modes"]:
        m = payload["modes"][mode]
        s = m["summary"]
        lines.append(f"### {mode}")
        lines.append("")
        lines.append(f"- samples: {s['samples']}")
        lines.append(f"- tick_to_ack_p99_ms p50/p90/p99: {s['tick_to_ack_p99_ms']['p50']:.6f}/{s['tick_to_ack_p99_ms']['p90']:.6f}/{s['tick_to_ack_p99_ms']['p99']:.6f}")
        lines.append(f"- tick_to_decision_p99_ms p50/p90/p99: {s['tick_to_decision_p99_ms']['p50']:.6f}/{s['tick_to_decision_p99_ms']['p90']:.6f}/{s['tick_to_decision_p99_ms']['p99']:.6f}")
        lines.append(f"- source_latency_p99_ms p50/p90: {s['source_latency_p99_ms']['p50']:.6f}/{s['source_latency_p99_ms']['p90']:.6f}")
        lines.append(f"- local_backlog_p99_ms p50/p90: {s['local_backlog_p99_ms']['p50']:.6f}/{s['local_backlog_p99_ms']['p90']:.6f}")
        lines.append(f"- feed_in_p99_ms p50/p90: {s['feed_in_p99_ms']['p50']:.6f}/{s['feed_in_p99_ms']['p90']:.6f}")
        lines.append(f"- decision_queue_wait_p99_ms p50/p90: {s['decision_queue_wait_p99_ms']['p50']:.6f}/{s['decision_queue_wait_p99_ms']['p90']:.6f}")
        lines.append(f"- decision_compute_p99_ms p50/p90/p99: {s['decision_compute_p99_ms']['p50']:.6f}/{s['decision_compute_p99_ms']['p90']:.6f}/{s['decision_compute_p99_ms']['p99']:.6f}")
        lines.append(f"- lag_half_life_ms p50/p90: {s['lag_half_life_ms']['p50']:.6f}/{s['lag_half_life_ms']['p90']:.6f}")
        lines.append(f"- quote_block_ratio p50/p90: {s['quote_block_ratio']['p50']:.4f}/{s['quote_block_ratio']['p90']:.4f}")
        lines.append(f"- policy_block_ratio p50/p90: {s['policy_block_ratio']['p50']:.4f}/{s['policy_block_ratio']['p90']:.4f}")
        lines.append(f"- eligible_count p50/p90: {s['eligible_count']['p50']:.3f}/{s['eligible_count']['p90']:.3f}")
        lines.append(f"- executed_count p50/p90: {s['executed_count']['p50']:.3f}/{s['executed_count']['p90']:.3f}")
        lines.append(f"- data_valid_ratio p10/p50: {s['data_valid_ratio']['p10']:.5f}/{s['data_valid_ratio']['p50']:.5f}")
        lines.append(f"- executed_over_eligible p50: {s['executed_over_eligible']['p50']:.4f}")
        lines.append(
            f"- gate_ready_ratio strict/reported/effective: {s['gate_ready_ratio_strict']:.3f}/{s['gate_ready_ratio_reported']:.3f}/{s['gate_ready_ratio_effective']:.3f}"
        )
        lines.append(f"- gate_ready_mismatch_count: {s['gate_ready_mismatch_count']}")
        lines.append(f"- ev_net_usdc_p50 p50/p90: {s['ev_net_usdc_p50']['p50']:.6f}/{s['ev_net_usdc_p50']['p90']:.6f}")
        lines.append(f"- source_mix_ratio_peak: {json.dumps(s['source_mix_ratio_peak'], ensure_ascii=True)}")
        lines.append(f"- exit_reason_top_last: {json.dumps(s['exit_reason_top_last'], ensure_ascii=True)}")
        if m.get("e2e_json"):
            lines.append(f"- e2e_json: `{m['e2e_json']}`")
        if m.get("sweep_jsons"):
            lines.append(f"- sweep_jsons: {', '.join(m['sweep_jsons'])}")
        lines.append("")
    out_md.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_root) / utc_day() / "runs" / args.run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    modes = ["direct_only", "udp_only", "active_active", "websocket_primary"]

    session = requests.Session()
    atexit.register(session.close)
    payload: dict = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_id": args.run_id,
        "base_url": args.base_url,
        "profile": args.profile,
        "sample_seconds": args.sample_seconds,
        "modes": {},
    }

    for mode in modes:
        mode_dir = out_dir / mode
        mode_dir.mkdir(parents=True, exist_ok=True)
        print(f"[mode] {mode} start")
        fusion_resp = post_json(session, args.base_url, "/control/reload_fusion", mode_payload(mode))
        post_json(session, args.base_url, "/control/reset_shadow", {})
        post_json(session, args.base_url, "/control/resume", {})
        time.sleep(max(1, args.warmup_sec))

        mode_result: dict = {
            "fusion_applied": fusion_resp,
            "live_samples_file": str(mode_dir / "live_samples.json"),
        }

        if not args.skip_e2e:
            e2e_json = mode_dir / "e2e_latency.json"
            cmd = [
                sys.executable,
                "scripts/e2e_latency_test.py",
                "--profile",
                args.profile,
                "--mode",
                "ws-first",
                "--base-url",
                args.base_url,
                "--seconds",
                str(max(30, args.sample_seconds)),
                "--json-out",
                str(e2e_json),
            ]
            try:
                run_cmd(cmd)
                mode_result["e2e_json"] = str(e2e_json)
                mode_result["summary_e2e"] = e2e_summary(read_json(e2e_json))
            except subprocess.CalledProcessError as err:
                mode_result["e2e_error"] = {
                    "returncode": int(err.returncode),
                    "command": cmd,
                }

        if not args.skip_sweep:
            sweep_cmd = [
                sys.executable,
                "scripts/full_latency_sweep.py",
                "--profile",
                args.profile,
                "--base-url",
                args.base_url,
                "--out-root",
                args.out_root,
                "--run-id",
                f"{args.run_id}-{mode}",
                "--fusion-mode",
                mode,
                "--reset-shadow",
                "--warmup-sec",
                str(max(1, args.warmup_sec)),
                "--dedupe-window-ms",
                "30",
            ]
            try:
                run_cmd(sweep_cmd)
                sweep_files = sorted(
                    str(p)
                    for p in (Path(args.out_root) / utc_day() / "runs" / f"{args.run_id}-{mode}").glob("full_latency_sweep_*.json")
                )
                mode_result["sweep_jsons"] = sweep_files
            except subprocess.CalledProcessError as err:
                mode_result["sweep_error"] = {
                    "returncode": int(err.returncode),
                    "command": sweep_cmd,
                }

        # Re-baseline live summary after sweep to prevent cross-window contamination.
        post_json(session, args.base_url, "/control/reload_fusion", mode_payload(mode))
        post_json(session, args.base_url, "/control/reset_shadow", {})
        post_json(session, args.base_url, "/control/resume", {})
        time.sleep(max(1, args.warmup_sec))
        samples = collect_live_samples(session, args.base_url, args.sample_seconds, args.poll_interval_sec)
        (mode_dir / "live_samples.json").write_text(json.dumps(samples, ensure_ascii=True, indent=2), encoding="utf-8")
        mode_result["summary"] = summary_from_samples(samples)
        if mode_result.get("summary_e2e"):
            live_ack = mode_result["summary"]["tick_to_ack_p99_ms"]["p50"]
            e2e_ack = mode_result["summary_e2e"]["tick_to_ack_p99_ms"]["p50"]
            ratio = 0.0 if e2e_ack <= 0 else live_ack / e2e_ack
            mode_result["consistency"] = {
                "tick_to_ack_p50_ratio_live_over_e2e": ratio,
                "consistent": bool(0.5 <= ratio <= 2.0) if e2e_ack > 0 else (live_ack == 0.0),
            }
        payload["modes"][mode] = mode_result
        print(f"[mode] {mode} done samples={len(samples)}")

    out_json = out_dir / "pipeline_latency_audit.json"
    out_md = out_dir / "pipeline_latency_audit.md"
    out_json.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    write_markdown(out_md, payload)
    print(f"wrote={out_json}")
    print(f"wrote={out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
