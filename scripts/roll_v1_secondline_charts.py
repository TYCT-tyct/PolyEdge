#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import matplotlib.pyplot as plt
import pandas as pd


@dataclass
class Marker:
    t_ms: int
    yes_prob: float
    action: str
    side: str
    style: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build 1-second point charts with roll_v1 shot/fill markers from a paper run."
    )
    p.add_argument("--run-dir", required=True, help="run root or inst1 directory")
    p.add_argument(
        "--out-dir",
        default="datasets/reports/roll5m_secondline",
        help="output root directory",
    )
    p.add_argument("--resample-sec", type=int, default=1, help="resample interval seconds")
    p.add_argument("--timeframe", default="5m", choices=["5m", "15m", "all"])
    return p.parse_args()


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


def side_to_yes_prob(side: str, price: float) -> Optional[float]:
    side = side.strip()
    if not (0.0 < price < 1.0):
        return None
    if side in ("BuyYes", "SellYes"):
        return price
    if side in ("BuyNo", "SellNo"):
        return 1.0 - price
    return None


def load_price_tape(path: Path, timeframe_filter: str) -> Dict[str, List[dict]]:
    out: Dict[str, List[dict]] = defaultdict(list)
    for row in iter_jsonl(path):
        market_id = str(row.get("market_id", "")).strip()
        ts_ms = int(row.get("ts_ms", 0))
        mid_yes = float(row.get("mid_yes", 0.0))
        tf = str(row.get("timeframe", ""))
        if timeframe_filter != "all" and tf != timeframe_filter:
            continue
        if not market_id or ts_ms <= 0 or not (0.0 <= mid_yes <= 1.0):
            continue
        out[market_id].append(
            {
                "ts_ms": ts_ms,
                "mid_yes": mid_yes,
                "symbol": str(row.get("symbol", "")),
                "timeframe": tf,
                "title": str(row.get("title", "")),
            }
        )
    for rows in out.values():
        rows.sort(key=lambda r: r["ts_ms"])
    return out


def load_markers(shots_path: Path, fills_path: Path) -> Dict[str, List[Marker]]:
    out: Dict[str, List[Marker]] = defaultdict(list)
    if shots_path.exists():
        for row in iter_jsonl(shots_path):
            shot = row.get("shot") or {}
            market_id = str(shot.get("market_id", "")).strip()
            ts_ms = int(row.get("ts_ms", 0))
            side = str(shot.get("side", ""))
            style = str(shot.get("execution_style", ""))
            px = float(shot.get("intended_price", 0.0))
            yp = side_to_yes_prob(side, px)
            if market_id and ts_ms > 0 and yp is not None:
                out[market_id].append(
                    Marker(
                        t_ms=ts_ms,
                        yes_prob=yp,
                        action="shot",
                        side=side,
                        style=style,
                    )
                )
    if fills_path.exists():
        for row in iter_jsonl(fills_path):
            fill = row.get("fill") or {}
            market_id = str(fill.get("market_id", "")).strip()
            ts_ms = int(row.get("ts_ms", 0))
            side = str(fill.get("side", ""))
            style = str(fill.get("style", ""))
            px = float(fill.get("price", 0.0))
            yp = side_to_yes_prob(side, px)
            if market_id and ts_ms > 0 and yp is not None:
                out[market_id].append(
                    Marker(
                        t_ms=ts_ms,
                        yes_prob=yp,
                        action="fill",
                        side=side,
                        style=style,
                    )
                )
    for arr in out.values():
        arr.sort(key=lambda x: x.t_ms)
    return out


def resample_to_seconds(rows: List[dict], resample_sec: int) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df = df.sort_values("ts_ms").drop_duplicates(subset=["ts_ms"], keep="last")
    t0 = int(df["ts_ms"].iloc[0])
    t1 = int(df["ts_ms"].iloc[-1])
    freq_ms = max(1, int(resample_sec)) * 1000
    # Bucket by second window; do not require exact millisecond alignment.
    df["ts_bucket"] = (((df["ts_ms"] - t0) // freq_ms) * freq_ms) + t0
    bucket_last = (
        df.sort_values("ts_ms")
        .groupby("ts_bucket", as_index=False)["mid_yes"]
        .last()
        .rename(columns={"ts_bucket": "ts_ms"})
    )
    idx = pd.Index(range(t0, t1 + 1, freq_ms), name="ts_ms")
    s = bucket_last.set_index("ts_ms")["mid_yes"].reindex(idx).ffill().bfill()
    out = s.reset_index()
    out["elapsed_s"] = (out["ts_ms"] - t0) / 1000.0
    return out


def draw_chart(
    market_id: str,
    meta: dict,
    sec_df: pd.DataFrame,
    markers: List[Marker],
    out_png: Path,
) -> dict:
    plt.figure(figsize=(13.5, 7.0), dpi=140)
    y = sec_df["mid_yes"] * 100.0
    x = sec_df["elapsed_s"]
    plt.plot(x, y, color="#1f77b4", linewidth=1.6, alpha=0.95, label="YES prob (1s)")
    plt.scatter(x, y, s=10, color="#5aa0ff", alpha=0.65)

    t0 = int(sec_df["ts_ms"].iloc[0])
    styles = {
        ("shot", "BuyYes"): ("^", "#2ca02c", "Shot BuyYes"),
        ("shot", "BuyNo"): ("v", "#9467bd", "Shot BuyNo"),
        ("shot", "SellYes"): ("x", "#d62728", "Shot SellYes"),
        ("shot", "SellNo"): ("x", "#ff7f0e", "Shot SellNo"),
        ("fill", "BuyYes"): ("^", "#22c55e", "Fill BuyYes"),
        ("fill", "BuyNo"): ("v", "#a855f7", "Fill BuyNo"),
        ("fill", "SellYes"): ("x", "#ef4444", "Fill SellYes"),
        ("fill", "SellNo"): ("x", "#f97316", "Fill SellNo"),
    }
    legend_seen = set()
    for m in markers:
        key = (m.action, m.side)
        marker, color, label = styles.get(key, ("o", "#aaaaaa", f"{m.action} {m.side}"))
        lx = (m.t_ms - t0) / 1000.0
        ly = m.yes_prob * 100.0
        show_label = label if label not in legend_seen else None
        legend_seen.add(label)
        plt.scatter(
            [lx],
            [ly],
            s=86 if marker != "x" else 72,
            marker=marker,
            color=color,
            linewidths=1.9 if marker == "x" else 0.0,
            label=show_label,
            zorder=5,
        )

    symbol = meta.get("symbol", "")
    timeframe = meta.get("timeframe", "")
    title = meta.get("title", "")
    plt.title(f"{symbol} {timeframe} | {market_id}\n{title}")
    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("YES Probability (%)")
    plt.ylim(0, 100)
    plt.grid(True, linestyle="--", alpha=0.3)
    plt.legend(loc="best", fontsize=8)
    plt.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png)
    plt.close()

    return {
        "market_id": market_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "title": title,
        "points_1s": int(len(sec_df)),
        "shots": int(sum(1 for m in markers if m.action == "shot")),
        "fills": int(sum(1 for m in markers if m.action == "fill")),
        "start_ts_ms": int(sec_df["ts_ms"].iloc[0]),
        "end_ts_ms": int(sec_df["ts_ms"].iloc[-1]),
        "span_s": float((int(sec_df["ts_ms"].iloc[-1]) - int(sec_df["ts_ms"].iloc[0])) / 1000.0),
        "yes_min_pct": float((sec_df["mid_yes"].min()) * 100.0),
        "yes_max_pct": float((sec_df["mid_yes"].max()) * 100.0),
        "png": str(out_png),
    }


def main() -> None:
    args = parse_args()
    run_dir = Path(args.run_dir).resolve()
    inst_dir = normalize_inst_dir(run_dir)
    run_name = inst_dir.parent.name if inst_dir.name == "inst1" else inst_dir.name
    normalized_date = latest_date_dir(inst_dir / "normalized")

    price_path = normalized_date / "market_price_tape.jsonl"
    shots_path = normalized_date / "shadow_shots.jsonl"
    fills_path = normalized_date / "fills.jsonl"
    if not price_path.exists():
        raise FileNotFoundError(f"missing price tape: {price_path}")

    price_by_market = load_price_tape(price_path, args.timeframe)
    markers_by_market = load_markers(shots_path, fills_path)
    out_root = Path(args.out_dir) / run_name
    out_root.mkdir(parents=True, exist_ok=True)

    summaries = []
    for market_id, rows in sorted(price_by_market.items()):
        if len(rows) < 2:
            continue
        sec_df = resample_to_seconds(rows, args.resample_sec)
        meta = rows[0]
        out_png = out_root / f"{market_id}_{meta['symbol']}_{meta['timeframe']}_1s.png"
        summary = draw_chart(
            market_id=market_id,
            meta=meta,
            sec_df=sec_df,
            markers=markers_by_market.get(market_id, []),
            out_png=out_png,
        )
        sec_csv = out_root / f"{market_id}_{meta['symbol']}_{meta['timeframe']}_1s.csv"
        sec_df.assign(yes_prob_pct=sec_df["mid_yes"] * 100.0).to_csv(sec_csv, index=False)
        summary["csv"] = str(sec_csv)
        summaries.append(summary)

    payload = {
        "run_name": run_name,
        "inst_dir": str(inst_dir),
        "normalized_date": str(normalized_date),
        "timeframe_filter": args.timeframe,
        "resample_sec": args.resample_sec,
        "markets": summaries,
    }
    (out_root / "summary.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
