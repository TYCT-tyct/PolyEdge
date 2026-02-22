#!/usr/bin/env python3
from __future__ import annotations

import itertools
import json
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


@dataclass(frozen=True)
class FitParams:
    entry_vel_bps_s: float
    entry_persist_ms: int
    reverse_vel_bps_s: float
    reverse_persist_ms: int
    reverse_drop: float
    min_edge_net_bps: float
    prob_vel_gain: float
    prob_acc_gain: float
    fail_cost_bps: float
    maker_rebate_bps: float


@dataclass
class Action:
    market_id: str
    symbol: str
    timeframe: str
    ts_ms: int
    elapsed_s: float
    action: str
    side: str
    price_yes: float
    size_shares: float
    pnl: float = 0.0


def estimate_prob_settle(mid_yes: float, vel_bps_s: float, acc_bps_s2: float, p: FitParams) -> float:
    prob = mid_yes + p.prob_vel_gain * (vel_bps_s / 10000.0) + p.prob_acc_gain * (acc_bps_s2 / 10000.0)
    return min(max(prob, 0.001), 0.999)


def edge_bps(prob: float, side: str, price: float) -> float:
    if side == "yes":
        return ((prob - price) / max(price, 1e-6)) * 10000.0
    return (((1.0 - prob) - price) / max(price, 1e-6)) * 10000.0


def load_payload(path: Path, timeframe: str = "5m") -> list[tuple[dict, pd.DataFrame]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    selected = []
    by_id = payload.get("series", {})
    for m in payload.get("markets", []):
        if m.get("timeframe") != timeframe:
            continue
        mid = by_id.get(str(m["market_id"]), [])
        if not mid:
            continue
        df = pd.DataFrame(mid).sort_values("ts_ms").reset_index(drop=True)
        if "mid_yes" not in df.columns:
            continue
        t0 = int(df["ts_ms"].iloc[0])
        df["elapsed_s"] = (df["ts_ms"] - t0) / 1000.0
        dt = df["elapsed_s"].diff().fillna(0.0).clip(lower=1e-6)
        prev = df["mid_yes"].shift(1).fillna(df["mid_yes"].iloc[0]).clip(lower=1e-9)
        ret = (df["mid_yes"] - prev) / prev
        df["vel_bps_s"] = (ret * 10000.0) / dt
        df.loc[df.index[0], "vel_bps_s"] = 0.0
        dv = df["vel_bps_s"].diff().fillna(0.0)
        df["acc_bps_s2"] = dv / dt
        df.loc[df.index[0], "acc_bps_s2"] = 0.0
        selected.append((m, df))
    return selected


def run_market(meta: dict, df: pd.DataFrame, p: FitParams, notional_usdc: float = 1.0) -> tuple[list[Action], float]:
    symbol = meta["symbol"]
    timeframe = meta["timeframe"]
    market_id = str(meta["market_id"])

    side = "flat"  # flat/yes/no
    shares = 0.0
    avg_price = 0.0
    peak_prob = 0.0
    up_ms = 0
    down_ms = 0
    rev_ms = 0
    actions: list[Action] = []
    realized = 0.0
    prev_ts = int(df["ts_ms"].iloc[0])

    for r in df.itertuples(index=False):
        ts_ms = int(r.ts_ms)
        dt_ms = max(ts_ms - prev_ts, 1)
        prev_ts = ts_ms
        mid_yes = float(r.mid_yes)
        vel = float(r.vel_bps_s)
        acc = float(r.acc_bps_s2)
        bid_yes = float(r.bid_yes)
        bid_no = float(r.bid_no)
        p_est = estimate_prob_settle(mid_yes, vel, acc, p)

        if vel >= p.entry_vel_bps_s:
            up_ms += dt_ms
            down_ms = 0
        elif vel <= -p.entry_vel_bps_s:
            down_ms += dt_ms
            up_ms = 0
        else:
            up_ms = max(0, up_ms - dt_ms)
            down_ms = max(0, down_ms - dt_ms)

        if side == "flat":
            want_yes = up_ms >= p.entry_persist_ms
            want_no = down_ms >= p.entry_persist_ms
            if want_yes or want_no:
                pick = "yes" if want_yes else "no"
                px = bid_yes if pick == "yes" else bid_no
                e = edge_bps(p_est, pick, px) + p.maker_rebate_bps - p.fail_cost_bps
                if e > p.min_edge_net_bps:
                    q = notional_usdc / max(px, 1e-6)
                    actions.append(
                        Action(market_id, symbol, timeframe, ts_ms, float(r.elapsed_s), f"buy_{pick}", pick, mid_yes, q)
                    )
                    side = pick
                    shares = q
                    avg_price = px
                    peak_prob = mid_yes if pick == "yes" else 1.0 - mid_yes
                    rev_ms = 0
            continue

        side_prob = mid_yes if side == "yes" else 1.0 - mid_yes
        side_vel = vel if side == "yes" else -vel
        peak_prob = max(peak_prob, side_prob)
        drop = (peak_prob - side_prob) / max(peak_prob, 1e-9)
        rev_cond = side_vel <= p.reverse_vel_bps_s and drop >= p.reverse_drop
        rev_ms = rev_ms + dt_ms if rev_cond else 0

        if rev_ms >= p.reverse_persist_ms:
            exit_px = bid_yes if side == "yes" else bid_no
            pnl = shares * (exit_px - avg_price)
            actions.append(
                Action(market_id, symbol, timeframe, ts_ms, float(r.elapsed_s), f"sell_{side}", side, mid_yes, shares, pnl)
            )
            realized += pnl
            # flip
            side = "no" if side == "yes" else "yes"
            enter_px = bid_yes if side == "yes" else bid_no
            shares = notional_usdc / max(enter_px, 1e-6)
            avg_price = enter_px
            actions.append(
                Action(market_id, symbol, timeframe, ts_ms, float(r.elapsed_s), f"buy_{side}_flip", side, mid_yes, shares)
            )
            peak_prob = mid_yes if side == "yes" else 1.0 - mid_yes
            rev_ms = 0

    # settlement
    if side != "flat" and shares > 0:
        final_yes = float(df["mid_yes"].iloc[-1])
        payout = 1.0 if ((side == "yes" and final_yes >= 0.5) or (side == "no" and final_yes < 0.5)) else 0.0
        pnl = shares * (payout - avg_price)
        actions.append(
            Action(market_id, symbol, timeframe, int(df["ts_ms"].iloc[-1]), float(df["elapsed_s"].iloc[-1]), f"settle_{side}", side, final_yes, shares, pnl)
        )
        realized += pnl
    return actions, realized


def summarize(actions: list[Action], pnl: float) -> dict:
    closes = [a for a in actions if a.action.startswith("sell_") or a.action.startswith("settle_")]
    wins = sum(1 for a in closes if a.pnl > 0)
    losses = sum(1 for a in closes if a.pnl < 0)
    return {
        "actions": len(actions),
        "closed_trades": len(closes),
        "wins": wins,
        "losses": losses,
        "win_rate": (wins / len(closes)) if closes else 0.0,
        "pnl_total": pnl,
    }


def plot_actions(meta: dict, df: pd.DataFrame, actions: list[Action], out_png: Path) -> None:
    plt.figure(figsize=(12, 6), dpi=140)
    plt.plot(df["elapsed_s"], df["mid_yes"] * 100.0, color="#1f77b4", linewidth=2, label="YES mid")
    adf = pd.DataFrame([a.__dict__ for a in actions])
    if not adf.empty:
        for name, marker, color in [
            ("buy_yes", "^", "#2ca02c"),
            ("buy_yes_flip", "^", "#66bb6a"),
            ("sell_yes", "x", "#d62728"),
            ("settle_yes", "o", "#d62728"),
            ("buy_no", "v", "#9467bd"),
            ("buy_no_flip", "v", "#ab47bc"),
            ("sell_no", "x", "#ff7f0e"),
            ("settle_no", "o", "#ff7f0e"),
        ]:
            s = adf[adf["action"] == name]
            if s.empty:
                continue
            plt.scatter(s["elapsed_s"], s["price_yes"] * 100.0, s=65, marker=marker, color=color, linewidths=1.8 if marker == "x" else 0.0, label=name)
    plt.title(f"{meta['symbol']} {meta['timeframe']} {meta['market_id']}")
    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("YES Probability (%)")
    plt.grid(True, linestyle="--", alpha=0.35)
    plt.legend(loc="best", fontsize=8)
    plt.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png)
    plt.close()


def main() -> None:
    src = Path("datasets/recorder/reports/kline_recent/kline_recent_payload.json")
    out = Path("datasets/reports/kline_recent_fit")
    out.mkdir(parents=True, exist_ok=True)
    (out / "plots").mkdir(parents=True, exist_ok=True)

    markets = load_payload(src, timeframe="5m")
    grid = [
        FitParams(*vals)
        for vals in itertools.product(
            [8.0, 12.0, 20.0],         # entry vel
            [120, 240, 400],           # entry persist ms
            [-60.0, -90.0, -120.0],    # reverse vel
            [200, 400, 800],           # reverse persist ms
            [0.02, 0.03, 0.05],        # reverse drop
            [0.0, 0.5, 1.0],           # min edge
            [0.15, 0.25, 0.35],        # prob vel gain
            [0.0, 0.03, 0.06],         # prob acc gain
            [0.2, 0.5, 1.0],           # fail cost
            [0.0, 0.3, 0.6],           # maker rebate
        )
    ]

    rows = []
    best = None
    best_key = None
    for p in grid:
        all_actions: list[Action] = []
        total_pnl = 0.0
        for meta, df in markets:
            acts, pnl = run_market(meta, df, p)
            all_actions.extend(acts)
            total_pnl += pnl
        s = summarize(all_actions, total_pnl)
        row = {**p.__dict__, **s}
        rows.append(row)
        key = (s["pnl_total"], s["win_rate"], s["closed_trades"])
        if best is None or key > best_key:
            best = p
            best_key = key

    res = pd.DataFrame(rows).sort_values(["pnl_total", "win_rate", "closed_trades"], ascending=False)
    res.to_csv(out / "fit_grid_results.csv", index=False)
    res.head(30).to_csv(out / "fit_grid_top30.csv", index=False)

    market_reports = []
    for meta, df in markets:
        acts, pnl = run_market(meta, df, best)
        s = summarize(acts, pnl)
        plot_path = out / "plots" / f"{meta['market_id']}_{meta['symbol']}_{meta['timeframe']}_fit_best.png"
        plot_actions(meta, df, acts, plot_path)
        pd.DataFrame([a.__dict__ for a in acts]).to_csv(
            out / "plots" / f"{meta['market_id']}_{meta['symbol']}_{meta['timeframe']}_actions_best.csv",
            index=False,
        )
        market_reports.append({**meta, **s, "plot": str(plot_path)})

    summary = {
        "markets": len(markets),
        "grid_size": len(grid),
        "best_params": best.__dict__ if best else {},
        "best_row": res.iloc[0].to_dict() if not res.empty else {},
        "market_reports": market_reports,
    }
    (out / "fit_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
