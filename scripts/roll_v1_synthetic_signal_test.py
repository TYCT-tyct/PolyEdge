#!/usr/bin/env python3
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


@dataclass(frozen=True)
class Scenario:
    name: str
    symbol: str
    anchor_points: list[tuple[str, float]]  # ("HH.MM", yes_prob_pct)


SCENARIOS = [
    Scenario(
        name="scenario1_monotonic_rise",
        symbol="BTCUSDT",
        anchor_points=[
            ("12.35", 54.0),
            ("12.36", 59.6),
            ("12.37", 90.5),
            ("12.38", 97.5),
            ("12.39", 99.5),
            ("12.40", 100.0),
        ],
    ),
    Scenario(
        name="scenario2_drawdown_rebound",
        symbol="ETHUSDT",
        anchor_points=[
            ("11.25", 58.0),
            ("11.26", 38.0),
            ("11.27", 51.0),
            ("11.28", 80.0),
            ("11.29", 85.0),
            ("11.30", 100.0),
        ],
    ),
]


def hhmm_to_dt(v: str) -> datetime:
    hour, minute = v.split(".")
    # Same day placeholder; only elapsed-time matters in synthetic tests.
    return datetime(2026, 2, 22, int(hour), int(minute), 0)


def interpolate(points: list[tuple[str, float]], step_ms: int) -> pd.DataFrame:
    anchors = [(hhmm_to_dt(t), p / 100.0) for t, p in points]
    t0 = anchors[0][0]
    rows: list[dict] = []
    for i in range(len(anchors) - 1):
        start_t, start_p = anchors[i]
        end_t, end_p = anchors[i + 1]
        total_ms = int((end_t - start_t).total_seconds() * 1000)
        for dt_ms in range(0, total_ms, step_ms):
            frac = dt_ms / total_ms if total_ms else 0.0
            p = start_p + (end_p - start_p) * frac
            cur_t = start_t + pd.Timedelta(milliseconds=dt_ms)
            elapsed_s = (cur_t - t0).total_seconds()
            rows.append(
                {
                    "ts": cur_t.isoformat(),
                    "elapsed_s": elapsed_s,
                    "yes_prob": p,
                    "yes_prob_pct": p * 100.0,
                }
            )
    last_t, last_p = anchors[-1]
    rows.append(
        {
            "ts": last_t.isoformat(),
            "elapsed_s": (last_t - t0).total_seconds(),
            "yes_prob": last_p,
            "yes_prob_pct": last_p * 100.0,
        }
    )
    return pd.DataFrame(rows)


def compute_kinematics(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    dt = out["elapsed_s"].diff().fillna(0.0)
    prev = out["yes_prob"].shift(1).fillna(out["yes_prob"].iloc[0]).clip(lower=1e-9)
    ret = (out["yes_prob"] - prev) / prev
    out["velocity_bps_per_sec"] = (ret * 10000.0) / dt.replace(0.0, 1e-9)
    out.loc[dt <= 0.0, "velocity_bps_per_sec"] = 0.0
    dv = out["velocity_bps_per_sec"].diff().fillna(0.0)
    out["acceleration"] = dv / dt.replace(0.0, 1e-9)
    out.loc[dt <= 0.0, "acceleration"] = 0.0
    return out


def plot_scenario(
    scenario: Scenario,
    df_1s: pd.DataFrame,
    out_png: Path,
) -> None:
    plt.figure(figsize=(12, 6), dpi=150)
    plt.plot(
        df_1s["elapsed_s"],
        df_1s["yes_prob_pct"],
        color="#1f77b4",
        linewidth=2.0,
        label="YES probability (1s interpolated)",
    )
    anchor_elapsed = [i * 60 for i in range(len(scenario.anchor_points))]
    anchor_values = [v for _, v in scenario.anchor_points]
    anchor_labels = [t for t, _ in scenario.anchor_points]
    plt.scatter(anchor_elapsed, anchor_values, color="#d62728", s=40, zorder=5, label="anchors")
    for x, y, txt in zip(anchor_elapsed, anchor_values, anchor_labels):
        plt.annotate(
            f"{txt} {y:.1f}%",
            (x, y),
            textcoords="offset points",
            xytext=(5, 6),
            fontsize=8,
        )
    plt.title(f"Roll v1 Synthetic 5m - {scenario.name} ({scenario.symbol})")
    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("YES Probability (%)")
    plt.xlim(0, 300)
    plt.ylim(0, 100)
    plt.grid(True, linestyle="--", alpha=0.35)
    plt.legend(loc="best")
    plt.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png)
    plt.close()


def main() -> None:
    out_root = Path("datasets/reports/synthetic_roll_tests")
    out_root.mkdir(parents=True, exist_ok=True)

    summary: dict[str, dict] = {}
    for sc in SCENARIOS:
        df_1s = compute_kinematics(interpolate(sc.anchor_points, step_ms=1000))
        df_40ms = compute_kinematics(interpolate(sc.anchor_points, step_ms=40))

        csv_1s = out_root / f"{sc.name}_{sc.symbol}_1s.csv"
        csv_40 = out_root / f"{sc.name}_{sc.symbol}_40ms.csv"
        png = out_root / f"{sc.name}_{sc.symbol}.png"
        df_1s.to_csv(csv_1s, index=False)
        df_40ms.to_csv(csv_40, index=False)
        plot_scenario(sc, df_1s, png)

        summary[sc.name] = {
            "symbol": sc.symbol,
            "rows_1s": int(len(df_1s)),
            "rows_40ms": int(len(df_40ms)),
            "yes_prob_min_pct": round(float(df_1s["yes_prob_pct"].min()), 4),
            "yes_prob_max_pct": round(float(df_1s["yes_prob_pct"].max()), 4),
            "velocity_p50_bps_per_sec_40ms": round(
                float(df_40ms["velocity_bps_per_sec"].quantile(0.5)), 4
            ),
            "velocity_p90_bps_per_sec_40ms": round(
                float(df_40ms["velocity_bps_per_sec"].quantile(0.9)), 4
            ),
            "accel_p90_40ms": round(float(df_40ms["acceleration"].quantile(0.9)), 4),
            "files": {
                "csv_1s": str(csv_1s),
                "csv_40ms": str(csv_40),
                "png": str(png),
            },
        }

    summary_path = out_root / "synthetic_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(summary_path)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
