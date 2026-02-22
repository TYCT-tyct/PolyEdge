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
        description="Build 1-second price charts from recorder raw market_tape, optionally overlaying strategy shot/fill markers."
    )
    p.add_argument("--recorder-root", required=True, help="datasets/recorder* root")
    p.add_argument("--out-dir", default="datasets/reports/recorder_secondline")
    p.add_argument("--timeframe", default="5m", choices=["5m", "15m", "all"])
    p.add_argument("--symbol", default="", help="optional symbol filter, e.g. BTCUSDT")
    p.add_argument("--market-id", default="", help="optional exact market id")
    p.add_argument("--resample-sec", type=int, default=1)
    p.add_argument("--max-markets", type=int, default=16)
    p.add_argument(
        "--run-dir",
        default="",
        help="optional paper run dir to overlay shot/fill markers",
    )
    p.add_argument("--simulate-roll", action="store_true", help="add simulated roll buy/sell markers")
    p.add_argument("--sim-entry-vel-bps", type=float, default=8.0)
    p.add_argument("--sim-entry-persist-sec", type=int, default=2)
    p.add_argument("--sim-reverse-vel-bps", type=float, default=-80.0)
    p.add_argument("--sim-reverse-persist-sec", type=int, default=2)
    p.add_argument("--sim-reverse-drop", type=float, default=0.03)
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


def latest_date_dir(parent: Path) -> Path:
    dates = sorted([p for p in parent.iterdir() if p.is_dir()])
    if not dates:
        raise FileNotFoundError(f"no date partition under: {parent}")
    return dates[-1]


def normalize_inst_dir(run_dir: Path) -> Path:
    if (run_dir / "normalized").exists():
        return run_dir
    inst1 = run_dir / "inst1"
    if (inst1 / "normalized").exists():
        return inst1
    raise FileNotFoundError(f"cannot resolve inst dir from: {run_dir}")


def side_to_yes_prob(side: str, price: float) -> Optional[float]:
    side = side.strip()
    if not (0.0 < price < 1.0):
        return None
    if side in ("BuyYes", "SellYes"):
        return price
    if side in ("BuyNo", "SellNo"):
        return 1.0 - price
    return None


def load_markers(run_dir: Path) -> Dict[str, List[Marker]]:
    inst = normalize_inst_dir(run_dir)
    date_dir = latest_date_dir(inst / "normalized")
    shots = date_dir / "shadow_shots.jsonl"
    fills = date_dir / "fills.jsonl"
    out: Dict[str, List[Marker]] = defaultdict(list)

    if shots.exists():
        for row in iter_jsonl(shots):
            shot = row.get("shot") or {}
            mid = str(shot.get("market_id", "")).strip()
            ts = int(row.get("ts_ms", 0))
            side = str(shot.get("side", ""))
            style = str(shot.get("execution_style", ""))
            px = float(shot.get("intended_price", 0.0))
            yp = side_to_yes_prob(side, px)
            if mid and ts > 0 and yp is not None:
                out[mid].append(Marker(ts, yp, "shot", side, style))

    if fills.exists():
        for row in iter_jsonl(fills):
            fill = row.get("fill") or {}
            mid = str(fill.get("market_id", "")).strip()
            ts = int(row.get("ts_ms", 0))
            side = str(fill.get("side", ""))
            style = str(fill.get("style", ""))
            px = float(fill.get("price", 0.0))
            yp = side_to_yes_prob(side, px)
            if mid and ts > 0 and yp is not None:
                out[mid].append(Marker(ts, yp, "fill", side, style))

    for arr in out.values():
        arr.sort(key=lambda x: x.t_ms)
    return out


def load_recorder_tape(
    raw_file: Path,
    timeframe_filter: str,
    symbol_filter: str,
    market_filter: str,
) -> Dict[str, List[dict]]:
    out: Dict[str, List[dict]] = defaultdict(list)
    for row in iter_jsonl(raw_file):
        mid = str(row.get("market_id", "")).strip()
        ts = int(row.get("ts_ms", 0))
        tf = str(row.get("timeframe", ""))
        symbol = str(row.get("symbol", ""))
        if not mid or ts <= 0:
            continue
        if timeframe_filter != "all" and tf != timeframe_filter:
            continue
        if symbol_filter and symbol_filter.upper() != symbol.upper():
            continue
        if market_filter and market_filter != mid:
            continue
        by = float(row.get("bid_yes", 0.0))
        ay = float(row.get("ask_yes", 0.0))
        bn = float(row.get("bid_no", 0.0))
        an = float(row.get("ask_no", 0.0))
        if by <= 0.0 or ay <= 0.0:
            continue
        mid_yes = ((by + ay) * 0.5)
        out[mid].append(
            {
                "ts_ms": ts,
                "mid_yes": mid_yes,
                "symbol": symbol,
                "timeframe": tf,
                "title": str(row.get("title", "")),
                "bid_yes": by,
                "ask_yes": ay,
                "bid_no": bn,
                "ask_no": an,
            }
        )
    for rows in out.values():
        rows.sort(key=lambda x: x["ts_ms"])
    return out


def resample_seconds(rows: List[dict], sec: int) -> pd.DataFrame:
    df = pd.DataFrame(rows).sort_values("ts_ms")
    df = df.drop_duplicates(subset=["ts_ms"], keep="last")
    t0 = int(df["ts_ms"].iloc[0])
    t1 = int(df["ts_ms"].iloc[-1])
    freq = max(1, sec) * 1000
    idx = pd.Index(range(t0, t1 + 1, freq), name="ts_ms")
    s = df.set_index("ts_ms")["mid_yes"].reindex(idx).ffill().bfill()
    out = s.reset_index()
    out["elapsed_s"] = (out["ts_ms"] - t0) / 1000.0
    return out


def draw_plot(
    market_id: str,
    meta: dict,
    sec_df: pd.DataFrame,
    markers: List[Marker],
    out_png: Path,
) -> dict:
    plt.figure(figsize=(13.5, 7), dpi=140)
    x = sec_df["elapsed_s"]
    y = sec_df["mid_yes"] * 100.0
    plt.plot(x, y, color="#1f77b4", linewidth=1.5, label="YES prob (1s)")
    plt.scatter(x, y, s=9, color="#5aa0ff", alpha=0.65)

    t0 = int(sec_df["ts_ms"].iloc[0])
    style_map = {
        ("shot", "BuyYes"): ("^", "#2ca02c", "Shot BuyYes"),
        ("shot", "BuyNo"): ("v", "#9467bd", "Shot BuyNo"),
        ("shot", "SellYes"): ("x", "#d62728", "Shot SellYes"),
        ("shot", "SellNo"): ("x", "#ff7f0e", "Shot SellNo"),
        ("fill", "BuyYes"): ("^", "#16a34a", "Fill BuyYes"),
        ("fill", "BuyNo"): ("v", "#9333ea", "Fill BuyNo"),
        ("fill", "SellYes"): ("x", "#ef4444", "Fill SellYes"),
        ("fill", "SellNo"): ("x", "#f97316", "Fill SellNo"),
        ("sim", "BuyYes"): ("^", "#10b981", "Sim BuyYes"),
        ("sim", "BuyNo"): ("v", "#8b5cf6", "Sim BuyNo"),
        ("sim", "SellYes"): ("x", "#ef4444", "Sim SellYes"),
        ("sim", "SellNo"): ("x", "#f97316", "Sim SellNo"),
    }
    seen = set()
    for m in markers:
        mk, c, label = style_map.get((m.action, m.side), ("o", "#9ca3af", f"{m.action} {m.side}"))
        lx = (m.t_ms - t0) / 1000.0
        ly = m.yes_prob * 100.0
        show = label if label not in seen else None
        seen.add(label)
        plt.scatter(
            [lx],
            [ly],
            s=84 if mk != "x" else 72,
            marker=mk,
            color=c,
            linewidths=1.8 if mk == "x" else 0.0,
            label=show,
            zorder=5,
        )

    plt.ylim(0, 100)
    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("YES Probability (%)")
    plt.title(f"{meta['symbol']} {meta['timeframe']} | {market_id}\n{meta['title']}")
    plt.grid(True, linestyle="--", alpha=0.3)
    plt.legend(loc="best", fontsize=8)
    plt.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png)
    plt.close()

    return {
        "market_id": market_id,
        "symbol": meta["symbol"],
        "timeframe": meta["timeframe"],
        "title": meta["title"],
        "points_1s": int(len(sec_df)),
        "span_s": float((int(sec_df["ts_ms"].iloc[-1]) - int(sec_df["ts_ms"].iloc[0])) / 1000.0),
        "yes_min_pct": float(sec_df["mid_yes"].min() * 100.0),
        "yes_max_pct": float(sec_df["mid_yes"].max() * 100.0),
        "shots": int(sum(1 for m in markers if m.action == "shot")),
        "fills": int(sum(1 for m in markers if m.action == "fill")),
        "sim_actions": int(sum(1 for m in markers if m.action == "sim")),
        "png": str(out_png),
    }


def simulate_markers(
    sec_df: pd.DataFrame,
    entry_vel_bps: float,
    entry_persist_sec: int,
    reverse_vel_bps: float,
    reverse_persist_sec: int,
    reverse_drop: float,
) -> List[Marker]:
    if len(sec_df) < 3:
        return []
    df = sec_df.copy()
    dt = df["elapsed_s"].diff().fillna(1.0).clip(lower=1e-6)
    prev = df["mid_yes"].shift(1).fillna(df["mid_yes"].iloc[0]).clip(lower=1e-9)
    df["vel_bps_s"] = ((df["mid_yes"] - prev) / prev) * 10000.0 / dt
    df.loc[df.index[0], "vel_bps_s"] = 0.0

    out: List[Marker] = []
    pos = "flat"
    up = 0
    down = 0
    rev = 0
    peak_side = 0.0

    for r in df.itertuples(index=False):
        ts = int(r.ts_ms)
        p_yes = float(r.mid_yes)
        vel = float(r.vel_bps_s)

        if vel >= entry_vel_bps:
            up += 1
            down = 0
        elif vel <= -entry_vel_bps:
            down += 1
            up = 0
        else:
            up = max(0, up - 1)
            down = max(0, down - 1)

        if pos == "flat":
            if up >= entry_persist_sec:
                pos = "yes"
                peak_side = p_yes
                rev = 0
                out.append(Marker(ts, p_yes, "sim", "BuyYes", "sim"))
                continue
            if down >= entry_persist_sec:
                pos = "no"
                peak_side = 1.0 - p_yes
                rev = 0
                out.append(Marker(ts, 1.0 - p_yes, "sim", "BuyNo", "sim"))
                continue

        if pos == "yes":
            side_prob = p_yes
            peak_side = max(peak_side, side_prob)
            drop = (peak_side - side_prob) / max(peak_side, 1e-9)
            cond = vel <= reverse_vel_bps and drop >= reverse_drop
            rev = rev + 1 if cond else 0
            if rev >= reverse_persist_sec:
                out.append(Marker(ts, p_yes, "sim", "SellYes", "sim"))
                out.append(Marker(ts, 1.0 - p_yes, "sim", "BuyNo", "sim"))
                pos = "no"
                peak_side = 1.0 - p_yes
                rev = 0

        elif pos == "no":
            side_prob = 1.0 - p_yes
            peak_side = max(peak_side, side_prob)
            # For NO side, reverse condition uses opposite velocity sign.
            side_vel = -vel
            drop = (peak_side - side_prob) / max(peak_side, 1e-9)
            cond = side_vel <= reverse_vel_bps and drop >= reverse_drop
            rev = rev + 1 if cond else 0
            if rev >= reverse_persist_sec:
                out.append(Marker(ts, 1.0 - p_yes, "sim", "SellNo", "sim"))
                out.append(Marker(ts, p_yes, "sim", "BuyYes", "sim"))
                pos = "yes"
                peak_side = p_yes
                rev = 0
    return out


def main() -> None:
    args = parse_args()
    root = Path(args.recorder_root).resolve()
    raw_file = latest_date_dir(root / "raw") / "market_tape.jsonl"
    if not raw_file.exists():
        raise FileNotFoundError(f"missing raw tape: {raw_file}")

    marker_map: Dict[str, List[Marker]] = {}
    if args.run_dir:
        marker_map = load_markers(Path(args.run_dir).resolve())

    by_market = load_recorder_tape(
        raw_file=raw_file,
        timeframe_filter=args.timeframe,
        symbol_filter=args.symbol,
        market_filter=args.market_id,
    )
    # Keep most data-rich markets first.
    items = sorted(by_market.items(), key=lambda kv: len(kv[1]), reverse=True)[: args.max_markets]

    out_root = Path(args.out_dir)
    out_root.mkdir(parents=True, exist_ok=True)
    summaries = []
    for market_id, rows in items:
        if len(rows) < 2:
            continue
        sec_df = resample_seconds(rows, args.resample_sec)
        meta = rows[0]
        sim_markers: List[Marker] = []
        if args.simulate_roll:
            sim_markers = simulate_markers(
                sec_df,
                entry_vel_bps=args.sim_entry_vel_bps,
                entry_persist_sec=max(1, args.sim_entry_persist_sec),
                reverse_vel_bps=args.sim_reverse_vel_bps,
                reverse_persist_sec=max(1, args.sim_reverse_persist_sec),
                reverse_drop=max(0.0, args.sim_reverse_drop),
            )
        merged_markers = list(marker_map.get(market_id, [])) + sim_markers
        png = out_root / f"{market_id}_{meta['symbol']}_{meta['timeframe']}_1s.png"
        summary = draw_plot(market_id, meta, sec_df, merged_markers, png)
        csv_path = out_root / f"{market_id}_{meta['symbol']}_{meta['timeframe']}_1s.csv"
        sec_df.assign(yes_prob_pct=sec_df["mid_yes"] * 100.0).to_csv(csv_path, index=False)
        summary["csv"] = str(csv_path)
        summary["raw_points"] = len(rows)
        if sim_markers:
            sim_csv = out_root / f"{market_id}_{meta['symbol']}_{meta['timeframe']}_sim_actions.csv"
            pd.DataFrame([{
                "ts_ms": m.t_ms,
                "side": m.side,
                "yes_prob_pct": m.yes_prob * 100.0,
            } for m in sim_markers]).to_csv(sim_csv, index=False)
            summary["sim_csv"] = str(sim_csv)
        summaries.append(summary)

    payload = {
        "recorder_root": str(root),
        "raw_file": str(raw_file),
        "timeframe_filter": args.timeframe,
        "symbol_filter": args.symbol,
        "market_filter": args.market_id,
        "resample_sec": args.resample_sec,
        "markets": summaries,
    }
    (out_root / "summary.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
