#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import itertools
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

import requests


def utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def parse_float_grid(raw: str) -> List[float]:
    out: List[float] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        out.append(float(token))
    return out


def parse_int_grid(raw: str) -> List[int]:
    out: List[int] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        out.append(int(token))
    return out


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    idx = int(round((len(sorted_values) - 1) * p))
    idx = max(0, min(len(sorted_values) - 1, idx))
    return sorted_values[idx]


@dataclass
class TrialResult:
    min_edge_bps: float
    ttl_ms: int
    basis_k_revert: float
    basis_z_cap: float
    safe_threshold: float
    caution_threshold: float
    samples: int
    fillability_10ms: float
    pnl_10s_p50_bps: float
    pnl_10s_p25_bps: float
    quote_block_ratio: float
    tick_to_ack_p99_ms: float
    net_edge_p50_bps: float

    def gate_pass(self) -> bool:
        return (
            self.pnl_10s_p50_bps > 0.0
            and self.pnl_10s_p25_bps > -20.0
            and self.fillability_10ms >= 0.60
            and self.quote_block_ratio < 0.10
            and self.tick_to_ack_p99_ms < 450.0
        )


def fetch_json(session: requests.Session, url: str, timeout: float = 5.0) -> Dict[str, Any]:
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def post_json(session: requests.Session, url: str, payload: Dict[str, Any], timeout: float = 5.0) -> Dict[str, Any]:
    resp = session.post(url, json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def run_trial(
    session: requests.Session,
    base_url: str,
    window_sec: int,
    poll_interval_sec: float,
    min_edge_bps: float,
    ttl_ms: int,
    basis_k_revert: float,
    basis_z_cap: float,
    safe_threshold: float,
    caution_threshold: float,
    warmup_sec: int,
) -> TrialResult:
    base = base_url.rstrip("/")
    post_json(session, f"{base}/control/reset_shadow", {})
    post_json(
        session,
        f"{base}/control/reload_strategy",
        {
            "min_edge_bps": min_edge_bps,
            "ttl_ms": ttl_ms,
            "basis_k_revert": basis_k_revert,
            "basis_z_cap": basis_z_cap,
        },
    )
    post_json(
        session,
        f"{base}/control/reload_toxicity",
        {
            "safe_threshold": safe_threshold,
            "caution_threshold": caution_threshold,
        },
    )
    if warmup_sec > 0:
        time.sleep(warmup_sec)

    fillability: List[float] = []
    pnl10: List[float] = []
    block_ratio: List[float] = []
    tick_ack: List[float] = []
    net_edge: List[float] = []

    deadline = time.time() + window_sec
    while time.time() < deadline:
        live = fetch_json(session, f"{base}/report/shadow/live")
        fillability.append(float(live.get("fillability_10ms", 0.0)))
        pnl10.append(float(live.get("pnl_10s_p50_bps", 0.0)))
        block_ratio.append(float(live.get("quote_block_ratio", 0.0)))
        tick_ack.append(float(live.get("tick_to_ack_p99_ms", 0.0)))
        net_edge.append(float(live.get("net_edge_p50_bps", 0.0)))
        time.sleep(max(1.0, poll_interval_sec))

    return TrialResult(
        min_edge_bps=min_edge_bps,
        ttl_ms=ttl_ms,
        basis_k_revert=basis_k_revert,
        basis_z_cap=basis_z_cap,
        safe_threshold=safe_threshold,
        caution_threshold=caution_threshold,
        samples=len(fillability),
        fillability_10ms=percentile(fillability, 0.50),
        pnl_10s_p50_bps=percentile(pnl10, 0.50),
        pnl_10s_p25_bps=percentile(pnl10, 0.25),
        quote_block_ratio=percentile(block_ratio, 0.50),
        tick_to_ack_p99_ms=percentile(tick_ack, 0.50),
        net_edge_p50_bps=percentile(net_edge, 0.50),
    )


def write_ablation_csv(path: Path, rows: Iterable[TrialResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "min_edge_bps",
                "ttl_ms",
                "basis_k_revert",
                "basis_z_cap",
                "safe_threshold",
                "caution_threshold",
                "samples",
                "fillability_10ms",
                "pnl_10s_p50_bps",
                "pnl_10s_p25_bps",
                "quote_block_ratio",
                "tick_to_ack_p99_ms",
                "net_edge_p50_bps",
                "gate_pass",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.min_edge_bps,
                    row.ttl_ms,
                    row.basis_k_revert,
                    row.basis_z_cap,
                    row.safe_threshold,
                    row.caution_threshold,
                    row.samples,
                    f"{row.fillability_10ms:.6f}",
                    f"{row.pnl_10s_p50_bps:.6f}",
                    f"{row.pnl_10s_p25_bps:.6f}",
                    f"{row.quote_block_ratio:.6f}",
                    f"{row.tick_to_ack_p99_ms:.6f}",
                    f"{row.net_edge_p50_bps:.6f}",
                    row.gate_pass(),
                ]
            )


def write_summary_md(path: Path, rows: List[TrialResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: List[str] = []
    lines.append("# Regression Summary")
    lines.append("")
    lines.append(f"- generated_at_utc: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"- trials: {len(rows)}")
    lines.append("")
    top = rows[:5]
    lines.append("## Top Trials")
    for i, row in enumerate(top, start=1):
        lines.append(
            f"- #{i} edge={row.min_edge_bps}, ttl={row.ttl_ms}, "
            f"k={row.basis_k_revert}, z={row.basis_z_cap}, "
            f"pnl10_p50={row.pnl_10s_p50_bps:.3f}, "
            f"fill10={row.fillability_10ms:.3f}, block={row.quote_block_ratio:.3f}, "
            f"tick_to_ack_p99={row.tick_to_ack_p99_ms:.3f}, gate={row.gate_pass()}"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_fixlist(path: Path, best: TrialResult | None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: List[str] = ["# Next Fixlist", ""]
    if best is None:
        lines.append("- No trial results. Check app health and API connectivity.")
    elif best.gate_pass():
        lines.append("- Conservative gate passed on best trial. Keep Shadow mode and extend soak run.")
    else:
        if best.pnl_10s_p50_bps <= 0.0:
            lines.append("- pnl_10s_p50_bps still <= 0. Increase edge threshold and reduce toxic markets.")
        if best.pnl_10s_p25_bps <= -20.0:
            lines.append("- pnl_10s_p25_bps <= -20. Tighten toxicity danger threshold and shorter TTL.")
        if best.fillability_10ms < 0.60:
            lines.append("- fillability_10ms below 0.60. Revisit spread/size and queue proxy assumptions.")
        if best.quote_block_ratio >= 0.10:
            lines.append("- quote_block_ratio too high. Relax min_edge or widen active market top-N.")
        if best.tick_to_ack_p99_ms >= 450.0:
            lines.append("- tick_to_ack_p99_ms too high. Investigate WS lag spikes and execution path.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PolyEdge parameter regression")
    p.add_argument("--base-url", default="http://127.0.0.1:8080")
    p.add_argument("--out-root", default="datasets/reports")
    p.add_argument("--window-sec", type=int, default=300)
    p.add_argument("--poll-interval-sec", type=float, default=10.0)
    p.add_argument("--max-trials", type=int, default=12)
    p.add_argument("--min-edge-grid", default="5,7,9")
    p.add_argument("--ttl-grid", default="250,400,700")
    p.add_argument("--basis-k-grid", default="0.70,0.85,1.00")
    p.add_argument("--basis-z-grid", default="2.0,3.0")
    p.add_argument("--safe-threshold-grid", default="0.35")
    p.add_argument("--caution-threshold-grid", default="0.65")
    p.add_argument("--warmup-sec", type=int, default=15)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    day_dir = Path(args.out_root) / utc_day()
    session = requests.Session()

    edge_grid = parse_float_grid(args.min_edge_grid)
    ttl_grid = parse_int_grid(args.ttl_grid)
    k_grid = parse_float_grid(args.basis_k_grid)
    z_grid = parse_float_grid(args.basis_z_grid)
    safe_grid = parse_float_grid(args.safe_threshold_grid)
    caution_grid = parse_float_grid(args.caution_threshold_grid)

    combos = list(itertools.product(edge_grid, ttl_grid, k_grid, z_grid, safe_grid, caution_grid))
    combos = combos[: max(1, args.max_trials)]

    rows: List[TrialResult] = []
    for idx, (edge, ttl, k, z, safe, caution) in enumerate(combos, start=1):
        print(
            f"[trial {idx}/{len(combos)}] edge={edge} ttl={ttl} "
            f"k={k} z={z} safe={safe} caution={caution}"
        )
        row = run_trial(
            session=session,
            base_url=args.base_url,
            window_sec=args.window_sec,
            poll_interval_sec=args.poll_interval_sec,
            min_edge_bps=edge,
            ttl_ms=ttl,
            basis_k_revert=k,
            basis_z_cap=z,
            safe_threshold=safe,
            caution_threshold=caution,
            warmup_sec=args.warmup_sec,
        )
        rows.append(row)

    rows.sort(
        key=lambda r: (
            r.gate_pass(),
            r.pnl_10s_p50_bps,
            r.fillability_10ms,
            -r.quote_block_ratio,
            -r.tick_to_ack_p99_ms,
        ),
        reverse=True,
    )

    ablation_path = day_dir / "ablation_toxicity.csv"
    summary_path = day_dir / "regression_summary.md"
    fixlist_path = day_dir / "next_fixlist.md"
    write_ablation_csv(ablation_path, rows)
    write_summary_md(summary_path, rows)
    write_fixlist(fixlist_path, rows[0] if rows else None)

    out_json = day_dir / "regression_summary.json"
    out_json.write_text(
        json.dumps(
            {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "trials": [row.__dict__ | {"gate_pass": row.gate_pass()} for row in rows],
            },
            ensure_ascii=True,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"wrote={ablation_path}")
    print(f"wrote={summary_path}")
    print(f"wrote={fixlist_path}")
    print(f"wrote={out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
