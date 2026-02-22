#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


@dataclass(frozen=True)
class SimConfig:
    # Entry confirm (synthetic simplified)
    entry_velocity_bps_per_sec: float = 30.0
    entry_persist_ms: int = 200
    # Reverse confirm (align with roll_v1.5m defaults)
    reverse_velocity_bps_per_sec: float = -180.0
    reverse_persist_ms: int = 800
    reverse_drop_pct: float = 0.06


def load_case(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path).copy()
    df = df.sort_values("elapsed_s").reset_index(drop=True)
    if "yes_prob" not in df.columns:
        df["yes_prob"] = df["yes_prob_pct"] / 100.0
    # estimate local dt from elapsed_s
    dt = df["elapsed_s"].diff().fillna(0.0).clip(lower=1e-9)
    prev = df["yes_prob"].shift(1).fillna(df["yes_prob"].iloc[0]).clip(lower=1e-9)
    ret = (df["yes_prob"] - prev) / prev
    df["velocity_bps_per_sec"] = (ret * 10_000.0) / dt
    df.loc[df.index[0], "velocity_bps_per_sec"] = 0.0
    return df


def simulate(df: pd.DataFrame, cfg: SimConfig) -> pd.DataFrame:
    rows: list[dict] = []
    pos = "flat"  # flat | yes | no
    up_persist_ms = 0
    down_persist_ms = 0
    reverse_persist_ms = 0
    peak_side_prob = 0.0
    prev_elapsed_s = df["elapsed_s"].iloc[0]

    for _, r in df.iterrows():
        t = float(r["elapsed_s"])
        p_yes = float(r["yes_prob"])
        v = float(r["velocity_bps_per_sec"])
        dt_ms = int(max((t - prev_elapsed_s) * 1000.0, 0.0))
        prev_elapsed_s = t

        if v >= cfg.entry_velocity_bps_per_sec:
            up_persist_ms += dt_ms
            down_persist_ms = 0
        elif v <= -cfg.entry_velocity_bps_per_sec:
            down_persist_ms += dt_ms
            up_persist_ms = 0
        else:
            up_persist_ms = max(0, up_persist_ms - dt_ms)
            down_persist_ms = max(0, down_persist_ms - dt_ms)

        if pos == "flat":
            if up_persist_ms >= cfg.entry_persist_ms:
                pos = "yes"
                peak_side_prob = p_yes
                reverse_persist_ms = 0
                rows.append(
                    {
                        "elapsed_s": t,
                        "action": "buy_yes",
                        "yes_prob_pct": p_yes * 100.0,
                        "velocity_bps_per_sec": v,
                        "note": "trend_up_confirmed",
                    }
                )
                continue
            if down_persist_ms >= cfg.entry_persist_ms:
                pos = "no"
                peak_side_prob = 1.0 - p_yes
                reverse_persist_ms = 0
                rows.append(
                    {
                        "elapsed_s": t,
                        "action": "buy_no",
                        "yes_prob_pct": p_yes * 100.0,
                        "velocity_bps_per_sec": v,
                        "note": "trend_down_confirmed",
                    }
                )
                continue

        elif pos == "yes":
            side_prob = p_yes
            peak_side_prob = max(peak_side_prob, side_prob)
            drop_pct = (peak_side_prob - side_prob) / max(peak_side_prob, 1e-9)
            aligned_v = v
            reverse_cond = (
                aligned_v <= cfg.reverse_velocity_bps_per_sec
                and drop_pct >= cfg.reverse_drop_pct
            )
            reverse_persist_ms = reverse_persist_ms + dt_ms if reverse_cond else 0
            if reverse_persist_ms >= cfg.reverse_persist_ms:
                rows.append(
                    {
                        "elapsed_s": t,
                        "action": "sell_yes",
                        "yes_prob_pct": p_yes * 100.0,
                        "velocity_bps_per_sec": v,
                        "note": "reverse_confirmed",
                    }
                )
                rows.append(
                    {
                        "elapsed_s": t,
                        "action": "buy_no",
                        "yes_prob_pct": p_yes * 100.0,
                        "velocity_bps_per_sec": v,
                        "note": "reverse_flip",
                    }
                )
                pos = "no"
                peak_side_prob = 1.0 - p_yes
                reverse_persist_ms = 0

        elif pos == "no":
            side_prob = 1.0 - p_yes
            peak_side_prob = max(peak_side_prob, side_prob)
            drop_pct = (peak_side_prob - side_prob) / max(peak_side_prob, 1e-9)
            aligned_v = -v
            reverse_cond = (
                aligned_v <= cfg.reverse_velocity_bps_per_sec
                and drop_pct >= cfg.reverse_drop_pct
            )
            reverse_persist_ms = reverse_persist_ms + dt_ms if reverse_cond else 0
            if reverse_persist_ms >= cfg.reverse_persist_ms:
                rows.append(
                    {
                        "elapsed_s": t,
                        "action": "sell_no",
                        "yes_prob_pct": p_yes * 100.0,
                        "velocity_bps_per_sec": v,
                        "note": "reverse_confirmed",
                    }
                )
                rows.append(
                    {
                        "elapsed_s": t,
                        "action": "buy_yes",
                        "yes_prob_pct": p_yes * 100.0,
                        "velocity_bps_per_sec": v,
                        "note": "reverse_flip",
                    }
                )
                pos = "yes"
                peak_side_prob = p_yes
                reverse_persist_ms = 0

    return pd.DataFrame(rows)


def draw_overlay(df: pd.DataFrame, actions: pd.DataFrame, out_png: Path, title: str) -> None:
    plt.figure(figsize=(13, 7), dpi=150)
    plt.plot(df["elapsed_s"], df["yes_prob"] * 100.0, color="#1f77b4", linewidth=2, label="YES prob")
    style = {
        "buy_yes": ("^", "#2ca02c", "Buy YES"),
        "sell_yes": ("x", "#d62728", "Sell YES"),
        "buy_no": ("v", "#9467bd", "Buy NO"),
        "sell_no": ("x", "#ff7f0e", "Sell NO"),
    }
    for act, (marker, color, label) in style.items():
        s = actions[actions["action"] == act]
        if len(s) == 0:
            continue
        plt.scatter(
            s["elapsed_s"],
            s["yes_prob_pct"],
            s=85,
            marker=marker,
            color=color,
            linewidths=2 if marker == "x" else 0.0,
            label=label,
            zorder=5,
        )
    plt.xlim(0, 300)
    plt.ylim(0, 100)
    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("YES Probability (%)")
    plt.title(title)
    plt.grid(True, linestyle="--", alpha=0.35)
    plt.legend(loc="best")
    plt.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png)
    plt.close()


def run_one(case_csv: Path, out_root: Path, cfg: SimConfig) -> None:
    df = load_case(case_csv)
    actions = simulate(df, cfg)
    stem = case_csv.stem.replace("_40ms", "")
    out_actions = out_root / f"{stem}_actions.csv"
    out_png = out_root / f"{stem}_with_trades.png"
    actions.to_csv(out_actions, index=False)
    draw_overlay(df, actions, out_png, f"{stem} | roll_v1 synthetic trade overlay")
    print(f"{out_png}")
    print(f"{out_actions}")
    if len(actions):
        print(actions[["elapsed_s", "action", "yes_prob_pct", "note"]].to_string(index=False))
    else:
        print("No actions generated")


def main() -> None:
    cfg = SimConfig()
    root = Path("datasets/reports/synthetic_roll_tests")
    out_root = root / "trade_overlay"
    out_root.mkdir(parents=True, exist_ok=True)

    run_one(root / "scenario1_monotonic_rise_BTCUSDT_40ms.csv", out_root, cfg)
    run_one(root / "scenario2_drawdown_rebound_ETHUSDT_40ms.csv", out_root, cfg)


if __name__ == "__main__":
    main()
