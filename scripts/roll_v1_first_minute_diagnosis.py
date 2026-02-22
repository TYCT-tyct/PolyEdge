#!/usr/bin/env python3
"""
Diagnose "early move but no trade" for roll_v1 runs.

Inputs:
  --run-dir <.../inst1> or <.../roll5m_run_root>
Outputs:
  datasets/reports/roll5m_diagnosis/<run_name>_first_minute.csv
  datasets/reports/roll5m_diagnosis/<run_name>_first_minute.md
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


@dataclass
class MarketStats:
    market_id: str
    symbol: str
    timeframe: str
    title: str
    first_ts_ms: int
    points_total: int
    points_first60: int
    mid_yes_start: float
    mid_yes_end_60: float
    mid_yes_max_60: float
    mid_yes_min_60: float
    rise_to_max_60_pct: float
    drop_to_min_60_pct: float
    shots_first60: int
    fills_first60: int
    first_shot_sec: Optional[float]
    first_fill_sec: Optional[float]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--run-dir", required=True, help="run root or inst dir")
    p.add_argument(
        "--timeframe",
        default="5m",
        choices=["5m", "15m", "all"],
        help="restrict output timeframe",
    )
    return p.parse_args()


def normalize_inst_dir(run_dir: Path) -> Path:
    if (run_dir / "normalized").exists() and (run_dir / "reports").exists():
        return run_dir
    inst1 = run_dir / "inst1"
    if (inst1 / "normalized").exists() and (inst1 / "reports").exists():
        return inst1
    raise FileNotFoundError(f"cannot resolve inst dir from: {run_dir}")


def latest_date_dir(parent: Path) -> Path:
    dates = sorted([p for p in parent.iterdir() if p.is_dir()])
    if not dates:
        raise FileNotFoundError(f"no date partition under: {parent}")
    return dates[-1]


def iter_jsonl(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def load_price_tape(path: Path) -> Dict[str, List[dict]]:
    by_market: Dict[str, List[dict]] = defaultdict(list)
    for row in iter_jsonl(path):
        market_id = str(row.get("market_id", "")).strip()
        if not market_id:
            continue
        by_market[market_id].append(row)
    for rows in by_market.values():
        rows.sort(key=lambda r: int(r.get("ts_ms", 0)))
    return by_market


def load_shots(path: Path) -> Dict[str, List[Tuple[int, dict]]]:
    out: Dict[str, List[Tuple[int, dict]]] = defaultdict(list)
    for row in iter_jsonl(path):
        shot = row.get("shot") or {}
        market_id = str(shot.get("market_id", "")).strip()
        ts_ms = int(row.get("ts_ms", 0))
        if not market_id or ts_ms <= 0:
            continue
        out[market_id].append((ts_ms, shot))
    for arr in out.values():
        arr.sort(key=lambda x: x[0])
    return out


def load_fills(path: Path) -> Dict[str, List[Tuple[int, dict]]]:
    out: Dict[str, List[Tuple[int, dict]]] = defaultdict(list)
    for row in iter_jsonl(path):
        fill = row.get("fill") or {}
        market_id = str(fill.get("market_id", "")).strip()
        ts_ms = int(row.get("ts_ms", 0))
        if not market_id or ts_ms <= 0:
            continue
        out[market_id].append((ts_ms, fill))
    for arr in out.values():
        arr.sort(key=lambda x: x[0])
    return out


def compute_market_stats(
    market_id: str,
    rows: List[dict],
    shots: Dict[str, List[Tuple[int, dict]]],
    fills: Dict[str, List[Tuple[int, dict]]],
) -> Optional[MarketStats]:
    if not rows:
        return None
    first_ts = int(rows[0].get("ts_ms", 0))
    if first_ts <= 0:
        return None
    end_60 = first_ts + 60_000
    first60 = [r for r in rows if int(r.get("ts_ms", 0)) <= end_60]
    if not first60:
        first60 = rows[:1]

    def mid_yes(r: dict) -> float:
        return float(r.get("mid_yes", 0.0))

    start_yes = mid_yes(first60[0])
    end_yes = mid_yes(first60[-1])
    max_yes = max(mid_yes(r) for r in first60)
    min_yes = min(mid_yes(r) for r in first60)
    rise_to_max = max_yes - start_yes
    drop_to_min = start_yes - min_yes

    market_shots = shots.get(market_id, [])
    market_fills = fills.get(market_id, [])
    shots_60 = [x for x in market_shots if x[0] <= end_60]
    fills_60 = [x for x in market_fills if x[0] <= end_60]
    first_shot_sec = (
        (shots_60[0][0] - first_ts) / 1000.0 if shots_60 else None
    )
    first_fill_sec = (
        (fills_60[0][0] - first_ts) / 1000.0 if fills_60 else None
    )

    head = rows[0]
    return MarketStats(
        market_id=market_id,
        symbol=str(head.get("symbol", "")),
        timeframe=str(head.get("timeframe", "unknown")),
        title=str(head.get("title", "")),
        first_ts_ms=first_ts,
        points_total=len(rows),
        points_first60=len(first60),
        mid_yes_start=start_yes,
        mid_yes_end_60=end_yes,
        mid_yes_max_60=max_yes,
        mid_yes_min_60=min_yes,
        rise_to_max_60_pct=rise_to_max * 100.0,
        drop_to_min_60_pct=drop_to_min * 100.0,
        shots_first60=len(shots_60),
        fills_first60=len(fills_60),
        first_shot_sec=first_shot_sec,
        first_fill_sec=first_fill_sec,
    )


def main() -> None:
    args = parse_args()
    run_dir = Path(args.run_dir).resolve()
    inst_dir = normalize_inst_dir(run_dir)
    run_name = inst_dir.parent.name if inst_dir.name == "inst1" else inst_dir.name

    normalized_date = latest_date_dir(inst_dir / "normalized")
    report_date = latest_date_dir(inst_dir / "reports")
    price_tape_path = normalized_date / "market_price_tape.jsonl"
    shots_path = normalized_date / "shadow_shots.jsonl"
    fills_path = normalized_date / "fills.jsonl"

    if not price_tape_path.exists():
        raise FileNotFoundError(f"missing price tape: {price_tape_path}")

    by_market = load_price_tape(price_tape_path)
    shots = load_shots(shots_path) if shots_path.exists() else {}
    fills = load_fills(fills_path) if fills_path.exists() else {}

    stats: List[MarketStats] = []
    for market_id, rows in by_market.items():
        s = compute_market_stats(market_id, rows, shots, fills)
        if not s:
            continue
        if args.timeframe != "all" and s.timeframe != args.timeframe:
            continue
        stats.append(s)

    stats.sort(key=lambda x: (x.symbol, x.timeframe, x.first_ts_ms))
    out_dir = Path("datasets/reports/roll5m_diagnosis")
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"{run_name}_first_minute.csv"
    md_path = out_dir / f"{run_name}_first_minute.md"

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "market_id",
                "symbol",
                "timeframe",
                "title",
                "first_ts_ms",
                "points_total",
                "points_first60",
                "mid_yes_start_pct",
                "mid_yes_end_60_pct",
                "mid_yes_max_60_pct",
                "mid_yes_min_60_pct",
                "rise_to_max_60_pct",
                "drop_to_min_60_pct",
                "shots_first60",
                "fills_first60",
                "first_shot_sec",
                "first_fill_sec",
            ]
        )
        for s in stats:
            w.writerow(
                [
                    s.market_id,
                    s.symbol,
                    s.timeframe,
                    s.title,
                    s.first_ts_ms,
                    s.points_total,
                    s.points_first60,
                    round(s.mid_yes_start * 100.0, 3),
                    round(s.mid_yes_end_60 * 100.0, 3),
                    round(s.mid_yes_max_60 * 100.0, 3),
                    round(s.mid_yes_min_60 * 100.0, 3),
                    round(s.rise_to_max_60_pct, 3),
                    round(s.drop_to_min_60_pct, 3),
                    s.shots_first60,
                    s.fills_first60,
                    None if s.first_shot_sec is None else round(s.first_shot_sec, 3),
                    None if s.first_fill_sec is None else round(s.first_fill_sec, 3),
                ]
            )

    missed_early = sorted(
        [s for s in stats if s.rise_to_max_60_pct >= 8.0 and s.shots_first60 == 0],
        key=lambda x: x.rise_to_max_60_pct,
        reverse=True,
    )
    early_hit = sorted(
        [s for s in stats if s.shots_first60 > 0],
        key=lambda x: (x.first_shot_sec if x.first_shot_sec is not None else 1e9),
    )

    with md_path.open("w", encoding="utf-8") as f:
        f.write(f"# roll_v1 首分钟诊断: {run_name}\n\n")
        f.write(f"- run_dir: `{inst_dir}`\n")
        f.write(f"- normalized_date: `{normalized_date.name}`\n")
        f.write(f"- report_date: `{report_date.name}`\n")
        f.write(f"- markets_analyzed: {len(stats)}\n\n")

        f.write("## 首分钟大涨但0下单（优先排查）\n\n")
        if not missed_early:
            f.write("- 无\n\n")
        else:
            for s in missed_early[:20]:
                f.write(
                    f"- {s.symbol} {s.timeframe} {s.market_id}: "
                    f"start={s.mid_yes_start*100:.2f}% "
                    f"max60={s.mid_yes_max_60*100:.2f}% "
                    f"rise={s.rise_to_max_60_pct:.2f}pp, "
                    f"shots60={s.shots_first60}\n"
                )
            f.write("\n")

        f.write("## 首分钟已下单样本（按首单时间排序）\n\n")
        if not early_hit:
            f.write("- 无\n")
        else:
            for s in early_hit[:20]:
                f.write(
                    f"- {s.symbol} {s.timeframe} {s.market_id}: "
                    f"first_shot={s.first_shot_sec:.2f}s, "
                    f"shots60={s.shots_first60}, fills60={s.fills_first60}, "
                    f"rise={s.rise_to_max_60_pct:.2f}pp\n"
                )

    print(csv_path)
    print(md_path)


if __name__ == "__main__":
    main()
