#!/usr/bin/env python3
"""Three-window verification harness (bounded, fail-fast).

Supports baseline vs Predator C+ modes and can optionally run A/B in one invocation.

Artifacts are written under:
  datasets/reports/<YYYY-MM-DD>/runs/<run_id>/<mode>/
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


def utc_day(ts: Optional[float] = None) -> str:
    dt = datetime.fromtimestamp(ts or time.time(), tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def now_ms() -> int:
    return int(time.time() * 1000)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def top_items(d: Any, n: int = 5) -> List[Tuple[str, int]]:
    if not isinstance(d, dict):
        return []
    items: List[Tuple[str, int]] = []
    for k, v in d.items():
        try:
            items.append((str(k), int(v)))
        except Exception:
            continue
    items.sort(key=lambda kv: kv[1], reverse=True)
    return items[:n]


def request_json(
    session: requests.Session,
    method: str,
    url: str,
    timeout_sec: float,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if method.upper() == "GET":
        resp = session.get(url, timeout=timeout_sec)
    else:
        resp = session.post(url, timeout=timeout_sec, json=payload or {})
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        return {"_raw": data}
    return data


def set_predator_mode(
    session: requests.Session,
    base_url: str,
    timeout_sec: float,
    enabled: bool,
    priority: str,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"enabled": enabled}
    if enabled:
        payload["priority"] = priority
    return request_json(session, "POST", f"{base_url.rstrip('/')}/control/reload_predator_c", timeout_sec, payload)


def resume_and_reset(session: requests.Session, base_url: str, timeout_sec: float) -> int:
    _ = request_json(session, "POST", f"{base_url.rstrip('/')}/control/resume", timeout_sec, {})
    res = request_json(session, "POST", f"{base_url.rstrip('/')}/control/reset_shadow", timeout_sec, {})
    try:
        return int(res.get("window_id") or 0)
    except Exception:
        return 0


def poll_until_outcomes(
    session: requests.Session,
    base_url: str,
    timeout_sec: float,
    min_outcomes: int,
    window_timeout_sec: int,
    poll_interval: float,
) -> Dict[str, Any]:
    deadline = time.monotonic() + max(1, window_timeout_sec)
    last: Dict[str, Any] = {}
    while time.monotonic() < deadline:
        try:
            last = request_json(
                session,
                "GET",
                f"{base_url.rstrip('/')}/report/shadow/live",
                timeout_sec,
            )
            outcomes = int(last.get("window_outcomes") or 0)
            if outcomes >= min_outcomes:
                return last
        except Exception:
            # keep polling; the runtime might be restarting
            pass
        time.sleep(max(0.2, poll_interval))
    return last


def compute_fixlist(live: Dict[str, Any]) -> List[str]:
    reasons = live.get("blocked_reason_counts") or {}
    top = [k for k, _ in top_items(reasons, n=10)]
    out: List[str] = []

    observe_only = bool(live.get("observe_only"))
    if observe_only:
        out.append("observe_only=true: check market tier profile / SOL guard / universe scope; reduce scope to BTC/ETH first.")

    if "paused" in top:
        out.append("blocked_reason=paused: POST /control/resume and ensure storm_test doesn't leave engine paused.")

    if any("tiny_notional" in k for k in top):
        out.append("blocked_reason=tiny_notional: lower min_eval_notional_usdc or increase base_quote_size to generate outcomes faster.")

    if any("market_untracked" in k for k in top):
        out.append("blocked_reason=market_untracked: check market_discovery mappings and active_top_n_markets scope.")

    if any("edge_below" in k for k in top) or any("no_quote_spread" in k for k in top):
        out.append("edge/no_quote blocks: thresholds too strict; relax min_edge (maker) or min_edge_net_bps (predator) and/or max_spread.")

    if any("rate_budget" in k for k in top):
        out.append("rate_budget blocks: raise rate_limit_rps and/or per-market budgets; confirm gateway + endpoint latency is stable.")

    # Strongly bounded: top 3 only.
    return out[:3] if out else ["No obvious fixlist from blocked reasons; inspect /report/shadow/live blocked_reason_counts and latency tails."]


@dataclass
class WindowResult:
    idx: int
    window_id: int
    live: Dict[str, Any]
    pnl_by_engine: Dict[str, Any]


def summarize_windows(results: List[WindowResult], min_outcomes: int) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    for r in results:
        live = r.live
        rows.append(
            {
                "idx": r.idx,
                "window_id": r.window_id,
                "window_outcomes": int(live.get("window_outcomes") or 0),
                "timed_out": bool(live.get("_timed_out", False)),
                "gate_ready": bool(live.get("gate_ready")),
                "gate_fail_reasons": live.get("gate_fail_reasons") or [],
                "ev_net_usdc_p50": float(live.get("ev_net_usdc_p50") or 0.0),
                "roi_notional_10s_bps_p50": float(live.get("roi_notional_10s_bps_p50") or 0.0),
                "executed_over_eligible": float(live.get("executed_over_eligible") or 0.0),
                "fillability_10ms": float(live.get("fillability_10ms") or 0.0),
                "quote_block_ratio": float(live.get("quote_block_ratio") or 0.0),
                "policy_block_ratio": float(live.get("policy_block_ratio") or 0.0),
                "data_valid_ratio": float(live.get("data_valid_ratio") or 0.0),
                "tick_to_ack_p99_ms": float(live.get("tick_to_ack_p99_ms") or 0.0),
                "decision_compute_p99_ms": float(live.get("decision_compute_p99_ms") or 0.0),
                "feed_in_p99_ms": float((live.get("latency") or {}).get("feed_in_p99_ms") or 0.0),
                "blocked_reason_top5": top_items(live.get("blocked_reason_counts") or {}, n=5),
                "predator": {
                    "enabled": bool(live.get("predator_c_enabled")),
                    "dir_up": int(live.get("direction_signals_up") or 0),
                    "dir_down": int(live.get("direction_signals_down") or 0),
                    "dir_neutral": int(live.get("direction_signals_neutral") or 0),
                    "sniper_fired": int(live.get("taker_sniper_fired") or 0),
                    "sniper_skipped": int(live.get("taker_sniper_skipped") or 0),
                },
            }
        )

    def avg(key: str) -> float:
        vals = [float(row.get(key) or 0.0) for row in rows]
        return sum(vals) / len(vals) if vals else 0.0

    all_have_samples = all(int(row["window_outcomes"]) >= min_outcomes for row in rows)
    all_gate_ok = all_have_samples and all((row.get("gate_fail_reasons") or []) == [] for row in rows)
    timed_out_count = sum(1 for row in rows if bool(row.get("timed_out")))

    return {
        "ts_ms": now_ms(),
        "windows": rows,
        "avg": {
            "ev_net_usdc_p50": avg("ev_net_usdc_p50"),
            "roi_notional_10s_bps_p50": avg("roi_notional_10s_bps_p50"),
            "executed_over_eligible": avg("executed_over_eligible"),
            "fillability_10ms": avg("fillability_10ms"),
            "quote_block_ratio": avg("quote_block_ratio"),
            "policy_block_ratio": avg("policy_block_ratio"),
            "data_valid_ratio": avg("data_valid_ratio"),
            "tick_to_ack_p99_ms": avg("tick_to_ack_p99_ms"),
            "decision_compute_p99_ms": avg("decision_compute_p99_ms"),
            "feed_in_p99_ms": avg("feed_in_p99_ms"),
        },
        "pass": bool(all_gate_ok),
        "timed_out_windows": timed_out_count,
        "notes": [],
    }


def render_md(mode_name: str, summary: Dict[str, Any]) -> str:
    avg = summary.get("avg") or {}
    lines: List[str] = []
    lines.append(f"# Three-Window Summary ({mode_name})")
    lines.append("")
    lines.append(f"- pass: `{summary.get('pass')}`")
    lines.append(f"- timed_out_windows: `{int(summary.get('timed_out_windows') or 0)}`")
    lines.append("")
    lines.append("## Averages")
    lines.append("")
    lines.append("| metric | value |")
    lines.append("|---|---:|")
    for k in [
        "ev_net_usdc_p50",
        "roi_notional_10s_bps_p50",
        "executed_over_eligible",
        "fillability_10ms",
        "quote_block_ratio",
        "policy_block_ratio",
        "data_valid_ratio",
        "tick_to_ack_p99_ms",
        "decision_compute_p99_ms",
        "feed_in_p99_ms",
    ]:
        try:
            v = float(avg.get(k) or 0.0)
        except Exception:
            v = 0.0
        lines.append(f"| `{k}` | `{v:.6g}` |")

    lines.append("")
    lines.append("## Windows")
    lines.append("")
    lines.append("| idx | window_id | outcomes | timed_out | gate_ready | ev_p50 | roi_p50_bps | exec/eligible | quote_block | policy_block |")
    lines.append("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in summary.get("windows") or []:
        lines.append(
            "| {idx} | {window_id} | {out} | {timed_out} | {gate_ready} | {ev:.6g} | {roi:.6g} | {eoe:.4f} | {qbr:.4f} | {pbr:.4f} |".format(
                idx=int(row.get("idx") or 0),
                window_id=int(row.get("window_id") or 0),
                out=int(row.get("window_outcomes") or 0),
                timed_out=bool(row.get("timed_out")),
                gate_ready=bool(row.get("gate_ready")),
                ev=float(row.get("ev_net_usdc_p50") or 0.0),
                roi=float(row.get("roi_notional_10s_bps_p50") or 0.0),
                eoe=float(row.get("executed_over_eligible") or 0.0),
                qbr=float(row.get("quote_block_ratio") or 0.0),
                pbr=float(row.get("policy_block_ratio") or 0.0),
            )
        )

    lines.append("")
    lines.append("## Top Block Reasons (per window)")
    lines.append("")
    for row in summary.get("windows") or []:
        lines.append(f"- window {int(row.get('idx') or 0)}: `{row.get('blocked_reason_top5')}`")
    lines.append("")
    return "\n".join(lines)


def run_one_mode(
    mode_name: str,
    session: requests.Session,
    base_url: str,
    timeout_sec: float,
    predator_enabled: bool,
    predator_priority: str,
    windows: int,
    min_outcomes: int,
    window_timeout_sec: int,
    poll_interval: float,
    out_dir: Path,
    continue_on_timeout: bool,
) -> Dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)

    _ = set_predator_mode(session, base_url, timeout_sec, predator_enabled, predator_priority)

    results: List[WindowResult] = []
    for i in range(1, windows + 1):
        window_id = resume_and_reset(session, base_url, timeout_sec)
        live = poll_until_outcomes(
            session,
            base_url,
            timeout_sec,
            min_outcomes=min_outcomes,
            window_timeout_sec=window_timeout_sec,
            poll_interval=poll_interval,
        )

        outcomes = int(live.get("window_outcomes") or 0)
        if outcomes < min_outcomes:
            # Persist whatever we have for debugging.
            try:
                pnl_by_engine = request_json(
                    session, "GET", f"{base_url.rstrip('/')}/report/pnl/by_engine", timeout_sec
                )
            except Exception:
                pnl_by_engine = {}
            write_json(out_dir / f"window{i:02d}_shadow_live.json", live)
            write_json(out_dir / f"window{i:02d}_pnl_by_engine.json", pnl_by_engine)

            fixlist = compute_fixlist(live)
            write_text(out_dir / "next_fixlist.md", "\n".join(f"- {x}" for x in fixlist) + "\n")
            if not continue_on_timeout:
                raise RuntimeError(
                    f"window {i} timed out: outcomes={outcomes} < min_outcomes={min_outcomes}"
                )
            live = dict(live)
            live["_timed_out"] = True
            live["_min_outcomes"] = min_outcomes
            results.append(WindowResult(idx=i, window_id=window_id, live=live, pnl_by_engine=pnl_by_engine))
            continue

        pnl_by_engine = request_json(
            session, "GET", f"{base_url.rstrip('/')}/report/pnl/by_engine", timeout_sec
        )

        write_json(out_dir / f"window{i:02d}_shadow_live.json", live)
        write_json(out_dir / f"window{i:02d}_pnl_by_engine.json", pnl_by_engine)
        results.append(WindowResult(idx=i, window_id=window_id, live=live, pnl_by_engine=pnl_by_engine))

    summary = summarize_windows(results, min_outcomes=min_outcomes)
    write_json(out_dir / "three_window_summary.json", summary)
    write_text(out_dir / "three_window_summary.md", render_md(mode_name, summary))
    return summary


def render_compare_md(base: Dict[str, Any], pred: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# Three-Window A/B Compare")
    lines.append("")
    lines.append("| metric | baseline | predator_c | delta |")
    lines.append("|---|---:|---:|---:|")
    for k in [
        "ev_net_usdc_p50",
        "roi_notional_10s_bps_p50",
        "executed_over_eligible",
        "fillability_10ms",
        "quote_block_ratio",
        "policy_block_ratio",
        "data_valid_ratio",
        "tick_to_ack_p99_ms",
        "decision_compute_p99_ms",
        "feed_in_p99_ms",
    ]:
        b = float((base.get("avg") or {}).get(k) or 0.0)
        p = float((pred.get("avg") or {}).get(k) or 0.0)
        lines.append(f"| `{k}` | `{b:.6g}` | `{p:.6g}` | `{(p - b):.6g}` |")
    lines.append("")
    lines.append(f"- baseline pass: `{base.get('pass')}`")
    lines.append(f"- predator_c pass: `{pred.get('pass')}`")
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PolyEdge three-window verify (bounded, fail-fast)")
    p.add_argument("--base-url", default="http://127.0.0.1:8080")
    p.add_argument("--mode", default="ab", choices=["baseline", "predator_c", "ab"])
    p.add_argument("--predator-priority", default="taker_first", choices=["maker_first", "taker_first", "taker_only"])
    p.add_argument("--windows", type=int, default=3)
    p.add_argument("--min-outcomes", type=int, default=30)
    p.add_argument("--window-timeout-sec", type=int, default=20 * 60)
    p.add_argument("--poll-interval", type=float, default=2.0)
    p.add_argument("--timeout-sec", type=float, default=5.0)
    p.add_argument("--continue-on-timeout", action="store_true")
    p.add_argument("--run-id", default=f"threew-{int(time.time())}")
    p.add_argument("--out-root", default="datasets/reports")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    day = utc_day()
    root = Path(args.out_root) / day / "runs" / args.run_id
    session = requests.Session()

    try:
        if args.mode == "baseline":
            out_dir = root / "baseline"
            _ = run_one_mode(
                "baseline",
                session,
                args.base_url,
                args.timeout_sec,
                predator_enabled=False,
                predator_priority=args.predator_priority,
                windows=args.windows,
                min_outcomes=args.min_outcomes,
                window_timeout_sec=args.window_timeout_sec,
                poll_interval=args.poll_interval,
                out_dir=out_dir,
                continue_on_timeout=args.continue_on_timeout,
            )
            print(f"wrote_dir={out_dir}")
            return 0

        if args.mode == "predator_c":
            out_dir = root / "predator_c"
            _ = run_one_mode(
                "predator_c",
                session,
                args.base_url,
                args.timeout_sec,
                predator_enabled=True,
                predator_priority=args.predator_priority,
                windows=args.windows,
                min_outcomes=args.min_outcomes,
                window_timeout_sec=args.window_timeout_sec,
                poll_interval=args.poll_interval,
                out_dir=out_dir,
                continue_on_timeout=args.continue_on_timeout,
            )
            print(f"wrote_dir={out_dir}")
            return 0

        # A/B
        baseline_dir = root / "baseline"
        predator_dir = root / "predator_c"
        base = run_one_mode(
            "baseline",
            session,
            args.base_url,
            args.timeout_sec,
            predator_enabled=False,
            predator_priority=args.predator_priority,
            windows=args.windows,
            min_outcomes=args.min_outcomes,
            window_timeout_sec=args.window_timeout_sec,
            poll_interval=args.poll_interval,
            out_dir=baseline_dir,
            continue_on_timeout=args.continue_on_timeout,
        )
        pred = run_one_mode(
            "predator_c",
            session,
            args.base_url,
            args.timeout_sec,
            predator_enabled=True,
            predator_priority=args.predator_priority,
            windows=args.windows,
            min_outcomes=args.min_outcomes,
            window_timeout_sec=args.window_timeout_sec,
            poll_interval=args.poll_interval,
            out_dir=predator_dir,
            continue_on_timeout=args.continue_on_timeout,
        )
        compare_md = render_compare_md(base, pred)
        write_text(root / "three_window_compare.md", compare_md)
        print(f"wrote_dir={root}")
        return 0
    except Exception as exc:  # noqa: BLE001
        # Fail-fast: persist a short error marker and exit non-zero.
        write_text(root / "FAILED.txt", f"{exc}\n")
        print(f"failed: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
