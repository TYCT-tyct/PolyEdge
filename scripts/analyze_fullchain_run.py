#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def fmt(x: Any, digits: int = 3) -> str:
    try:
        if x is None:
            return "NA"
        if isinstance(x, bool):
            return "true" if x else "false"
        if isinstance(x, (int, float)):
            return f"{float(x):.{digits}f}"
        return str(x)
    except Exception:
        return "NA"


@dataclass
class SweepRow:
    ts_ms: int
    path: Path
    stats: Dict[str, Dict[str, float]]
    last: Dict[str, Any]


@dataclass
class MetricSpec:
    key: str
    stat: str
    direction: str  # lower|higher
    abs_tolerance: float
    rel_tolerance: float


def collect_sweeps(run_dir: Path) -> List[SweepRow]:
    rows: List[SweepRow] = []
    for p in sorted(run_dir.glob("full_latency_sweep_*.json")):
        try:
            d = read_json(p)
            meta = d.get("meta") or {}
            ts_ms = int(meta.get("ts_ms") or 0)
            engine = d.get("engine") or {}
            stats = engine.get("stats") or {}
            last = engine.get("last") or {}
            if isinstance(stats, dict) and isinstance(last, dict):
                rows.append(SweepRow(ts_ms=ts_ms, path=p, stats=stats, last=last))
        except Exception:
            continue
    return rows


def pick_stat(stats: Dict[str, Dict[str, float]], key: str, which: str) -> Optional[float]:
    try:
        st = stats.get(key) or {}
        if not isinstance(st, dict):
            return None
        val = st.get(which)
        return float(val) if val is not None else None
    except Exception:
        return None


def print_sweep_table(rows: List[SweepRow]) -> None:
    if not rows:
        print("no full_latency_sweep_*.json found")
        return

    cols = [
        ("tick_to_ack_p99_ms", "p99"),
        ("tick_to_decision_p99_ms", "p99"),
        ("decision_queue_wait_p99_ms", "p99"),
        ("decision_compute_p99_ms", "p99"),
        ("source_latency_p99_ms", "p99"),
        ("exchange_lag_p99_ms", "p99"),
        ("path_lag_p99_ms", "p99"),
        ("local_backlog_p99_ms", "p99"),
        ("quote_block_ratio", "p99"),
        ("policy_block_ratio", "p99"),
        ("gate_block_ratio", "p99"),
        ("executed_over_eligible", "p50"),
        ("ev_net_usdc_p50", "p50"),
        ("window_outcomes", "p99"),
    ]
    header = ["ts_ms", "window_id"] + [f"{k}.{w}" for k, w in cols] + ["file"]
    print("\t".join(header))
    for row in rows:
        values = [str(row.ts_ms), str(row.last.get("window_id"))]
        for key, which in cols:
            values.append(fmt(pick_stat(row.stats, key, which)))
        values.append(row.path.name)
        print("\t".join(values))


def print_storm(run_dir: Path) -> None:
    p = run_dir / "storm_test_summary.json"
    if not p.exists():
        print("no storm_test_summary.json found")
        return
    d = read_json(p)
    lat = d.get("latency_ms") or {}
    print("storm_test_summary")
    print(f"- error_count: {d.get('error_count')}")
    print(f"- consecutive_failures: {d.get('consecutive_failures')}")
    print(f"- latency_p50_ms: {fmt(lat.get('p50'))}")
    print(f"- latency_p99_ms: {fmt(lat.get('p99'))}")
    print(f"- latency_max_ms: {fmt(lat.get('max'))}")


def _snapshot_live(run_dir: Path, name: str) -> Optional[Dict[str, Any]]:
    path = run_dir / "snapshots" / name
    if not path.exists():
        return None
    try:
        data = read_json(path)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def print_snapshot_diffs(run_dir: Path) -> None:
    pre = _snapshot_live(run_dir, "shadow_live_pre.json")
    post = _snapshot_live(run_dir, "shadow_live_after_storm.json") or _snapshot_live(
        run_dir, "shadow_live_after_sweeps.json"
    )
    if not pre or not post:
        return

    keys = [
        "tick_to_ack_p99_ms",
        "tick_to_decision_p99_ms",
        "decision_queue_wait_p99_ms",
        "decision_compute_p99_ms",
        "source_latency_p99_ms",
        "exchange_lag_p99_ms",
        "path_lag_p99_ms",
        "local_backlog_p99_ms",
        "window_outcomes",
        "quote_block_ratio",
        "policy_block_ratio",
        "gate_block_ratio",
        "executed_over_eligible",
        "ev_net_usdc_p50",
        "ev_positive_ratio",
        "data_valid_ratio",
    ]
    print("shadow_live_pre_vs_post")
    for key in keys:
        print(f"- {key}: {fmt(pre.get(key))} -> {fmt(post.get(key))}")


def print_missing_endpoints(run_dir: Path) -> None:
    snap = run_dir / "snapshots"
    if not snap.exists():
        return
    errs = sorted(snap.glob("*.err.txt"))
    if not errs:
        return
    print("snapshot_errors")
    for p in errs:
        msg = p.read_text(encoding="utf-8", errors="replace").strip()
        print(f"- {p.name}: {msg}")


def latest_metrics(rows: List[SweepRow]) -> Dict[str, float]:
    if not rows:
        return {}
    row = rows[-1]
    metrics: Dict[str, float] = {}
    for key, stat in (
        ("tick_to_ack_p99_ms", "p99"),
        ("tick_to_decision_p99_ms", "p99"),
        ("decision_queue_wait_p99_ms", "p99"),
        ("decision_compute_p99_ms", "p99"),
        ("source_latency_p99_ms", "p99"),
        ("exchange_lag_p99_ms", "p99"),
        ("path_lag_p99_ms", "p99"),
        ("local_backlog_p99_ms", "p99"),
        ("feed_in_p99_ms", "p99"),
        ("quote_block_ratio", "p99"),
        ("policy_block_ratio", "p99"),
        ("gate_block_ratio", "p99"),
        ("executed_over_eligible", "p50"),
        ("ev_positive_ratio", "p50"),
        ("ev_net_usdc_p50", "p50"),
    ):
        val = pick_stat(row.stats, key, stat)
        if val is not None:
            metrics[key] = val
    return metrics


def regression_specs() -> List[MetricSpec]:
    return [
        MetricSpec("tick_to_ack_p99_ms", "p99", "lower", 0.10, 0.08),
        MetricSpec("tick_to_decision_p99_ms", "p99", "lower", 0.10, 0.08),
        MetricSpec("decision_queue_wait_p99_ms", "p99", "lower", 0.08, 0.10),
        MetricSpec("decision_compute_p99_ms", "p99", "lower", 0.08, 0.10),
        MetricSpec("source_latency_p99_ms", "p99", "lower", 5.00, 0.12),
        MetricSpec("exchange_lag_p99_ms", "p99", "lower", 5.00, 0.12),
        MetricSpec("path_lag_p99_ms", "p99", "lower", 0.25, 0.20),
        MetricSpec("local_backlog_p99_ms", "p99", "lower", 0.15, 0.15),
        MetricSpec("feed_in_p99_ms", "p99", "lower", 0.15, 0.15),
        MetricSpec("quote_block_ratio", "p99", "lower", 0.01, 0.20),
        MetricSpec("policy_block_ratio", "p99", "lower", 0.01, 0.20),
        MetricSpec("gate_block_ratio", "p99", "lower", 0.01, 0.20),
        MetricSpec("executed_over_eligible", "p50", "higher", 0.02, 0.10),
        MetricSpec("ev_positive_ratio", "p50", "higher", 0.02, 0.10),
        MetricSpec("ev_net_usdc_p50", "p50", "higher", 0.0001, 0.15),
    ]


def compare_metrics(
    candidate_rows: List[SweepRow], baseline_rows: List[SweepRow]
) -> Dict[str, Any]:
    candidate = latest_metrics(candidate_rows)
    baseline = latest_metrics(baseline_rows)

    metrics: Dict[str, Dict[str, Any]] = {}
    regressed: List[str] = []
    improved: List[str] = []
    missing: List[str] = []

    for spec in regression_specs():
        c = candidate.get(spec.key)
        b = baseline.get(spec.key)
        if c is None or b is None:
            missing.append(spec.key)
            continue
        delta = c - b
        rel = 0.0 if abs(b) < 1e-9 else delta / abs(b)
        if spec.direction == "lower":
            threshold = max(spec.abs_tolerance, abs(b) * spec.rel_tolerance)
            is_regressed = delta > threshold
            is_improved = delta < -threshold
        else:
            threshold = max(spec.abs_tolerance, abs(b) * spec.rel_tolerance)
            is_regressed = -delta > threshold
            is_improved = delta > threshold

        status = "flat"
        if is_regressed:
            status = "regressed"
            regressed.append(spec.key)
        elif is_improved:
            status = "improved"
            improved.append(spec.key)

        metrics[spec.key] = {
            "baseline": b,
            "candidate": c,
            "delta": delta,
            "rel_delta": rel,
            "status": status,
            "direction": spec.direction,
        }

    attributions = infer_regression_attributions(metrics)
    return {
        "candidate": candidate,
        "baseline": baseline,
        "metrics": metrics,
        "regressed_metrics": regressed,
        "improved_metrics": improved,
        "missing_metrics": missing,
        "pass": len(regressed) == 0,
        "attributions": attributions,
    }


def infer_regression_attributions(metrics: Dict[str, Dict[str, Any]]) -> List[str]:
    reasons: List[str] = []

    def is_regressed(key: str) -> bool:
        return (metrics.get(key) or {}).get("status") == "regressed"

    if is_regressed("source_latency_p99_ms") or is_regressed("exchange_lag_p99_ms"):
        reasons.append(
            "upstream source latency regressed (exchange/ingest); prioritize exchange feed stability and timestamp alignment"
        )
    if is_regressed("path_lag_p99_ms") and not is_regressed("exchange_lag_p99_ms"):
        reasons.append(
            "Tokyo->Ireland relay path regressed independently; inspect UDP relay host/network path and irq/core pinning"
        )
    if is_regressed("decision_queue_wait_p99_ms"):
        if is_regressed("local_backlog_p99_ms") or is_regressed("feed_in_p99_ms"):
            reasons.append(
                "ingress queue pressure increased (queue wait + backlog/feed-in moved together); tune merge capacity/coalescing"
            )
        elif is_regressed("decision_compute_p99_ms"):
            reasons.append(
                "compute-bound decision loop (queue wait grew with compute p99); reduce hot-path work or increase consumer parallelism"
            )
        else:
            reasons.append(
                "scheduler/dispatch jitter likely (queue wait regressed without matching backlog/compute growth)"
            )
    if is_regressed("quote_block_ratio") or is_regressed("policy_block_ratio"):
        reasons.append(
            "execution gate blocking rose; inspect policy thresholds and eligibility drift"
        )
    if is_regressed("executed_over_eligible") and not (
        is_regressed("quote_block_ratio") or is_regressed("policy_block_ratio")
    ):
        reasons.append(
            "execution conversion fell without explicit gate rise; check order path, ack handling, and venue responsiveness"
        )

    if not reasons:
        reasons.append("no dominant bottleneck inferred; run targeted stage-by-stage profiling for next iteration")
    return reasons


def print_regression_report(result: Dict[str, Any], baseline_dir: Path) -> None:
    print(f"baseline_run_dir={baseline_dir}")
    print(f"regression_gate_pass={'true' if result['pass'] else 'false'}")
    if result["missing_metrics"]:
        print(f"missing_metrics={','.join(result['missing_metrics'])}")

    print("regression_scorecard")
    ordered = sorted(
        result["metrics"].items(),
        key=lambda kv: (0 if kv[1]["status"] == "regressed" else 1, kv[0]),
    )
    for key, row in ordered:
        print(
            f"- {key}: {fmt(row['baseline'])} -> {fmt(row['candidate'])} "
            f"(delta={fmt(row['delta'])}, rel={fmt(row['rel_delta'], 4)}, status={row['status']})"
        )

    print("likely_causes")
    for item in result["attributions"]:
        print(f"- {item}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze a fullchain benchmark run directory.")
    p.add_argument("--run-dir", required=True, help="path to datasets/reports/<day>/runs/<run_id>")
    p.add_argument(
        "--baseline-run-dir",
        default="",
        help="optional baseline run directory for regression comparison",
    )
    p.add_argument("--json-out", default="", help="optional path to write analysis json")
    p.add_argument(
        "--fail-on-regression",
        action="store_true",
        help="exit non-zero when regression gate does not pass",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    run_dir = Path(args.run_dir)
    if not run_dir.exists():
        print(f"run dir not found: {run_dir}")
        return 2

    rows = collect_sweeps(run_dir)
    print_sweep_table(rows)
    print()
    print_storm(run_dir)
    print()
    print_snapshot_diffs(run_dir)
    print()
    print_missing_endpoints(run_dir)

    regression_result: Optional[Dict[str, Any]] = None
    if args.baseline_run_dir:
        baseline_dir = Path(args.baseline_run_dir)
        baseline_rows = collect_sweeps(baseline_dir)
        print()
        if not baseline_rows:
            print(f"baseline has no full_latency_sweep_*.json: {baseline_dir}")
        elif not rows:
            print("candidate has no full_latency_sweep_*.json; regression analysis skipped")
        else:
            regression_result = compare_metrics(rows, baseline_rows)
            print_regression_report(regression_result, baseline_dir)

    if args.json_out:
        output = {
            "run_dir": str(run_dir),
            "sweep_files": [str(r.path) for r in rows],
            "regression": regression_result,
        }
        out_path = Path(args.json_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(output, ensure_ascii=True, indent=2), encoding="utf-8")
        print(f"wrote_json={out_path}")

    if args.fail_on_regression and regression_result is not None and not regression_result["pass"]:
        return 10
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
