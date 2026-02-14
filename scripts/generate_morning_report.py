#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List


def default_day() -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=1)
    return dt.strftime("%Y-%m-%d")


def read_json_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    out = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                continue
    return out


def read_latency_csv(path: Path) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        return {}
    out: Dict[str, Dict[str, str]] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            stage = row.get("stage", "")
            if stage:
                out[stage] = row
    return out


def top_blocked_reasons(live: Dict[str, Any], limit: int = 8) -> List[tuple[str, int]]:
    raw = live.get("blocked_reason_counts", {}) or {}
    rows = [(str(k), int(v)) for k, v in raw.items()]
    rows.sort(key=lambda x: x[1], reverse=True)
    return rows[:limit]


def summarize_alerts(alerts: List[Dict[str, Any]]) -> Counter[str]:
    c: Counter[str] = Counter()
    for a in alerts:
        c[str(a.get("type", "unknown"))] += 1
    return c


def build_recommendations(live: Dict[str, Any], alert_count: Counter[str]) -> List[str]:
    recs: List[str] = []
    if not live:
        return ["No live snapshot found. Check service and watchdog process."]

    if int(live.get("ref_freshness_ms", 10**9)) > 5000:
        recs.append("Reference feed stale. Check exchange WS subscriptions and region routing.")
    if int(live.get("book_freshness_ms", 10**9)) > 5000:
        recs.append("Polymarket book feed stale. Check CLOB WS health and reconnect loop.")
    if not bool(live.get("gate_ready", False)):
        recs.append(
            "Gate sample not ready yet. Extend shadow window before concluding strategy quality."
        )
    if float(live.get("tick_to_ack_p99_ms", 0.0)) > 450.0:
        recs.append("E2E p99 too high. Investigate tail latency and network jitter.")
    if float((live.get("latency") or {}).get("feed_in_p99_ms", 0.0)) > 800.0:
        recs.append("feed_in p99 too high. Enable strict stale tick filter and inspect source/local latency split.")
    if float(live.get("decision_compute_p99_ms", 0.0)) > 2.0:
        recs.append("decision_compute p99 too high. Reduce lock contention and hot-path logging.")
    if float(live.get("executed_over_eligible", 0.0)) < 0.60:
        recs.append(
            "Execution funnel below threshold. Add market-level rate budget and revisit taker trigger."
        )
    if float(live.get("data_valid_ratio", 1.0)) < 0.999:
        recs.append("Data validity below 99.9%. Pause quoting and fix raw->normalized reconciliation.")
    net_markout_usdc = float(live.get("net_markout_10s_usdc_p50", 0.0))
    if net_markout_usdc <= 0.0:
        recs.append("Median net markout (USDC) is non-positive. Keep Shadow mode and retune edge/fees.")
    if int(live.get("quote_attempted", 0)) > 100 and int(live.get("total_shots", 0)) == 0:
        recs.append("Many quote attempts but zero shadow shots. Re-check edge model vs fee/rebate.")

    if not recs and not alert_count:
        recs.append("No critical alerts. Continue shadow observation and keep conservative risk.")
    return recs


def build_toxicity_recommendations(tox: Dict[str, Any]) -> List[str]:
    recs: List[str] = []
    if not tox:
        return recs
    avg = float(tox.get("average_tox_score", 0.0))
    danger = int(tox.get("danger_count", 0))
    rows = tox.get("rows", []) or []
    active_count = sum(1 for r in rows if bool(r.get("active_for_quoting", False)))
    if avg > 0.65:
        recs.append("Average toxicity is high. Raise min_edge_bps and tighten TTL before next cycle.")
    if danger > 0:
        recs.append("Danger regime markets detected. Keep them observe-only until markout recovers.")
    if rows and active_count < 2:
        recs.append("Too few active markets after rank gating. Expand top-N or relax market score temporarily.")
    return recs


def write_report(
    out_md: Path,
    out_json: Path,
    day: str,
    live: Dict[str, Any],
    toxicity: Dict[str, Any],
    latency: Dict[str, Dict[str, str]],
    alert_count: Counter[str],
    recommendations: List[str],
) -> None:
    out_md.parent.mkdir(parents=True, exist_ok=True)

    attempted = int(live.get("quote_attempted", 0))
    blocked = int(live.get("quote_blocked", 0))
    block_ratio = float(live.get("quote_block_ratio", 0.0))
    policy_block_ratio = float(live.get("policy_block_ratio", 0.0))
    gate_ready = bool(live.get("gate_ready", False))
    gate_fail_reasons = live.get("gate_fail_reasons", []) or []
    window_id = int(live.get("window_id", 0))
    window_shots = int(live.get("window_shots", 0))
    window_outcomes = int(live.get("window_outcomes", 0))
    pnl_raw = float(live.get("pnl_10s_p50_bps_raw", live.get("pnl_10s_p50_bps", 0.0)))
    pnl_robust = float(live.get("pnl_10s_p50_bps_robust", pnl_raw))
    net_markout_usdc = float(live.get("net_markout_10s_usdc_p50", 0.0))
    ev_net_usdc_p50 = float(live.get("ev_net_usdc_p50", net_markout_usdc))
    ev_net_usdc_p10 = float(live.get("ev_net_usdc_p10", 0.0))
    ev_positive_ratio = float(live.get("ev_positive_ratio", 0.0))
    eligible_count = int(live.get("eligible_count", 0))
    executed_count = int(live.get("executed_count", 0))
    executed_over_eligible = float(live.get("executed_over_eligible", 0.0))
    roi_notional = float(live.get("roi_notional_10s_bps_p50", 0.0))
    pnl_outlier_ratio = float(live.get("pnl_10s_outlier_ratio", 0.0))
    queue_depth_p99 = float(live.get("queue_depth_p99", 0.0))
    event_backlog_p99 = float(live.get("event_backlog_p99", 0.0))

    lines = []
    lines.append(f"# PolyEdge Morning Report ({day}, UTC)")
    lines.append("")
    lines.append("## Core Snapshot")
    lines.append(f"- window_id: {window_id}")
    lines.append(f"- window_shots: {window_shots}")
    lines.append(f"- window_outcomes: {window_outcomes}")
    lines.append(f"- gate_ready: {str(gate_ready).lower()}")
    lines.append(f"- observe_only: {str(bool(live.get('observe_only', False))).lower()}")
    lines.append(f"- data_valid_ratio: {float(live.get('data_valid_ratio', 1.0)):.5f}")
    lines.append(f"- seq_gap_rate: {float(live.get('seq_gap_rate', 0.0)):.5f}")
    lines.append(f"- ts_inversion_rate: {float(live.get('ts_inversion_rate', 0.0)):.5f}")
    lines.append(f"- stale_tick_drop_ratio: {float(live.get('stale_tick_drop_ratio', 0.0)):.5f}")
    lines.append(f"- quote_attempted: {attempted}")
    lines.append(f"- quote_blocked: {blocked}")
    lines.append(f"- quote_block_ratio: {block_ratio:.4f}")
    lines.append(f"- policy_block_ratio: {policy_block_ratio:.4f}")
    lines.append(f"- total_shots: {int(live.get('total_shots', 0))}")
    lines.append(f"- total_outcomes: {int(live.get('total_outcomes', 0))}")
    lines.append(f"- fillability_10ms: {float(live.get('fillability_10ms', 0.0)):.4f}")
    lines.append(f"- net_edge_p50_bps: {float(live.get('net_edge_p50_bps', 0.0)):.4f}")
    lines.append(f"- net_edge_p10_bps: {float(live.get('net_edge_p10_bps', 0.0)):.4f}")
    lines.append(f"- pnl_1s_p50_bps: {float(live.get('pnl_1s_p50_bps', 0.0)):.4f}")
    lines.append(f"- pnl_5s_p50_bps: {float(live.get('pnl_5s_p50_bps', 0.0)):.4f}")
    lines.append(f"- pnl_10s_p50_bps_raw: {pnl_raw:.4f}")
    lines.append(f"- pnl_10s_p50_bps_robust: {pnl_robust:.4f}")
    lines.append(f"- net_markout_10s_usdc_p50: {net_markout_usdc:.6f}")
    lines.append(f"- ev_net_usdc_p50: {ev_net_usdc_p50:.6f}")
    lines.append(f"- ev_net_usdc_p10: {ev_net_usdc_p10:.6f}")
    lines.append(f"- ev_positive_ratio: {ev_positive_ratio:.4f}")
    lines.append(f"- eligible_count: {eligible_count}")
    lines.append(f"- executed_count: {executed_count}")
    lines.append(f"- executed_over_eligible: {executed_over_eligible:.4f}")
    lines.append(f"- roi_notional_10s_bps_p50: {roi_notional:.6f}")
    lines.append(f"- pnl_10s_outlier_ratio: {pnl_outlier_ratio:.4f}")
    lines.append(f"- tick_to_ack_p99_ms: {float(live.get('tick_to_ack_p99_ms', 0.0)):.3f}")
    lines.append(f"- tick_to_decision_p50_ms: {float(live.get('tick_to_decision_p50_ms', 0.0)):.3f}")
    lines.append(
        f"- decision_queue_wait_p99_ms: {float(live.get('decision_queue_wait_p99_ms', 0.0)):.3f}"
    )
    lines.append(
        f"- decision_compute_p99_ms: {float(live.get('decision_compute_p99_ms', 0.0)):.3f}"
    )
    lines.append(f"- source_latency_p99_ms: {float(live.get('source_latency_p99_ms', 0.0)):.3f}")
    lines.append(f"- local_backlog_p99_ms: {float(live.get('local_backlog_p99_ms', 0.0)):.3f}")
    lines.append(f"- ack_only_p50_ms: {float(live.get('ack_only_p50_ms', 0.0)):.3f}")
    lines.append(f"- queue_depth_p99: {queue_depth_p99:.3f}")
    lines.append(f"- event_backlog_p99: {event_backlog_p99:.3f}")
    lines.append(f"- ref_freshness_ms: {int(live.get('ref_freshness_ms', -1))}")
    lines.append(f"- book_freshness_ms: {int(live.get('book_freshness_ms', -1))}")
    if gate_fail_reasons:
        lines.append(f"- gate_fail_reasons: {','.join(str(v) for v in gate_fail_reasons)}")
    lines.append("")

    lines.append("## Latency Breakdown")
    if latency:
        for stage in [
            "feed_in",
            "signal",
            "quote",
            "risk",
            "tick_to_decision",
            "ack_only",
            "tick_to_ack",
            "shadow_fill",
        ]:
            row = latency.get(stage)
            if row:
                lines.append(
                    f"- {stage}: p50={row.get('p50')} p90={row.get('p90')} p99={row.get('p99')} {row.get('unit')}"
                )
    else:
        lines.append("- no latency csv found")
    lines.append("")

    lines.append("## Blocked Reasons")
    reasons = top_blocked_reasons(live)
    if reasons:
        for reason, count in reasons:
            lines.append(f"- {reason}: {count}")
    else:
        lines.append("- none")
    lines.append("")

    lines.append("## Alert Counts")
    if alert_count:
        for k, v in alert_count.most_common():
            lines.append(f"- {k}: {v}")
    else:
        lines.append("- none")
    lines.append("")

    lines.append("## Toxicity Snapshot")
    if toxicity:
        lines.append(f"- average_tox_score: {float(toxicity.get('average_tox_score', 0.0)):.4f}")
        lines.append(f"- safe_count: {int(toxicity.get('safe_count', 0))}")
        lines.append(f"- caution_count: {int(toxicity.get('caution_count', 0))}")
        lines.append(f"- danger_count: {int(toxicity.get('danger_count', 0))}")
        rows = toxicity.get("rows", []) or []
        active_rows = [r for r in rows if bool(r.get("active_for_quoting", False))]
        lines.append(f"- active_for_quoting_count: {len(active_rows)}")
        for row in active_rows[:5]:
            lines.append(
                "- active_market: "
                f"rank={int(row.get('market_rank', 0))} "
                f"{row.get('symbol', 'UNKNOWN')} "
                f"score={float(row.get('market_score', 0.0)):.2f} "
                f"markout10={float(row.get('markout_10s_bps', 0.0)):.2f}"
            )
        for row in rows[:5]:
            lines.append(
                "- top_market: "
                f"{row.get('symbol', 'UNKNOWN')} "
                f"score={float(row.get('market_score', 0.0)):.2f} "
                f"tox={float(row.get('tox_score', 0.0)):.3f} "
                f"regime={row.get('regime', 'UNKNOWN')} "
                f"markout10={float(row.get('markout_10s_bps', 0.0)):.2f}"
            )
    else:
        lines.append("- no toxicity snapshot found")
    lines.append("")

    lines.append("## Recommendations")
    for rec in recommendations:
        lines.append(f"- {rec}")
    lines.append("")

    out_md.write_text("\n".join(lines), encoding="utf-8")
    out_json.write_text(
        json.dumps(
            {
                "day": day,
                "live": live,
                "toxicity": toxicity,
                "latency": latency,
                "alert_count": dict(alert_count),
                "recommendations": recommendations,
            },
            ensure_ascii=True,
            indent=2,
        ),
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate PolyEdge morning report")
    p.add_argument("--day", default=default_day(), help="UTC day, e.g. 2026-02-13")
    p.add_argument("--root", default="datasets/reports", help="reports root path")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    day_dir = Path(args.root) / args.day
    live = read_json_file(day_dir / "shadow_live_latest.json")
    toxicity = read_json_file(day_dir / "toxicity_live_latest.json")
    latency = read_latency_csv(day_dir / "latency_breakdown_12h.csv")
    alerts = read_jsonl(day_dir / "monitor_alerts.jsonl")
    alert_count = summarize_alerts(alerts)
    recommendations = build_recommendations(live, alert_count)
    recommendations.extend(build_toxicity_recommendations(toxicity))

    write_report(
        out_md=day_dir / "morning_summary.md",
        out_json=day_dir / "morning_summary.json",
        day=args.day,
        live=live,
        toxicity=toxicity,
        latency=latency,
        alert_count=alert_count,
        recommendations=recommendations,
    )
    print(f"generated: {day_dir / 'morning_summary.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
