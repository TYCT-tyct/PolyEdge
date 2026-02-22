#!/usr/bin/env python3
from __future__ import annotations

import itertools
import json
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


@dataclass(frozen=True)
class Params:
    entry_vel_bps: float
    entry_persist_s: int
    reverse_vel_bps: float
    reverse_persist_s: int
    reverse_drop: float
    add_interval_s: int
    add_vel_bps: float
    entry_prob_floor: float


@dataclass
class Trade:
    market_id: str
    side: str
    ts_s: int
    elapsed_s: float
    action: str
    price_yes: float
    shares: float
    cashflow: float
    fee: float
    pnl: float = 0.0


def official_fee_usdc(shares: float, price_yes: float) -> float:
    p = min(max(price_yes, 0.0), 1.0)
    return shares * 0.25 * (p * (1.0 - p)) ** 2


def load_market(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path).copy()
    df = df.sort_values("ts_s").reset_index(drop=True)
    dt = df["elapsed_s"].diff().fillna(0.0).clip(lower=1e-9)
    prev = df["yes_prob"].shift(1).fillna(df["yes_prob"].iloc[0]).clip(lower=1e-9)
    ret = (df["yes_prob"] - prev) / prev
    df["vel_bps_s"] = (ret * 10000.0) / dt
    df.loc[df.index[0], "vel_bps_s"] = 0.0
    return df


def run_market(df: pd.DataFrame, params: Params, notional_usdc: float = 1.0) -> tuple[list[Trade], float]:
    market_id = str(df["market_id"].iloc[0])
    pos_side = "flat"  # flat/yes/no
    pos_shares = 0.0
    avg_cost = 0.0  # weighted avg yes-price for current side
    peak_side_prob = 0.0
    up_persist = 0
    down_persist = 0
    rev_persist = 0
    last_add_ts: int | None = None
    trades: list[Trade] = []
    realized_pnl = 0.0

    for r in df.itertuples(index=False):
        ts_s = int(r.ts_s)
        elapsed_s = float(r.elapsed_s)
        p_yes = float(r.yes_prob)
        vel = float(r.vel_bps_s)
        dt_s = 1 if trades else 1

        if vel >= params.entry_vel_bps:
            up_persist += dt_s
            down_persist = 0
        elif vel <= -params.entry_vel_bps:
            down_persist += dt_s
            up_persist = 0
        else:
            up_persist = max(0, up_persist - 1)
            down_persist = max(0, down_persist - 1)

        if pos_side == "flat":
            want_yes = up_persist >= params.entry_persist_s and p_yes >= params.entry_prob_floor
            want_no = down_persist >= params.entry_persist_s and p_yes <= 1.0 - params.entry_prob_floor
            if want_yes or want_no:
                side = "yes" if want_yes else "no"
                price = p_yes if side == "yes" else (1.0 - p_yes)
                shares = notional_usdc / max(price, 1e-6)
                fee = official_fee_usdc(shares, price)
                cashflow = -shares * price - fee
                trades.append(
                    Trade(market_id, side, ts_s, elapsed_s, f"buy_{side}", p_yes, shares, cashflow, fee)
                )
                pos_side = side
                pos_shares = shares
                avg_cost = price
                peak_side_prob = price
                rev_persist = 0
                last_add_ts = ts_s
            continue

        # Position management
        side_prob = p_yes if pos_side == "yes" else 1.0 - p_yes
        side_vel = vel if pos_side == "yes" else -vel
        peak_side_prob = max(peak_side_prob, side_prob)
        drop = (peak_side_prob - side_prob) / max(peak_side_prob, 1e-9)
        rev_cond = side_vel <= params.reverse_vel_bps and drop >= params.reverse_drop
        rev_persist = rev_persist + 1 if rev_cond else 0

        # Scale-in
        can_add = (
            last_add_ts is None
            or ts_s - last_add_ts >= params.add_interval_s
        ) and side_vel >= params.add_vel_bps and side_prob >= params.entry_prob_floor
        if can_add:
            price = p_yes if pos_side == "yes" else (1.0 - p_yes)
            add_shares = notional_usdc / max(price, 1e-6)
            fee = official_fee_usdc(add_shares, price)
            cashflow = -add_shares * price - fee
            trades.append(
                Trade(market_id, pos_side, ts_s, elapsed_s, f"buy_{pos_side}_add", p_yes, add_shares, cashflow, fee)
            )
            total_cost = avg_cost * pos_shares + price * add_shares
            pos_shares += add_shares
            avg_cost = total_cost / max(pos_shares, 1e-9)
            last_add_ts = ts_s

        # Reverse
        if rev_persist >= params.reverse_persist_s:
            price_exit = p_yes if pos_side == "yes" else (1.0 - p_yes)
            fee_exit = official_fee_usdc(pos_shares, price_exit)
            sell_cash = pos_shares * price_exit - fee_exit
            pnl = sell_cash - pos_shares * avg_cost
            trades.append(
                Trade(
                    market_id,
                    pos_side,
                    ts_s,
                    elapsed_s,
                    f"sell_{pos_side}",
                    p_yes,
                    pos_shares,
                    sell_cash,
                    fee_exit,
                    pnl,
                )
            )
            realized_pnl += pnl
            # Flip side
            new_side = "no" if pos_side == "yes" else "yes"
            price_new = p_yes if new_side == "yes" else (1.0 - p_yes)
            shares_new = notional_usdc / max(price_new, 1e-6)
            fee_new = official_fee_usdc(shares_new, price_new)
            cashflow_new = -shares_new * price_new - fee_new
            trades.append(
                Trade(market_id, new_side, ts_s, elapsed_s, f"buy_{new_side}_flip", p_yes, shares_new, cashflow_new, fee_new)
            )
            pos_side = new_side
            pos_shares = shares_new
            avg_cost = price_new
            peak_side_prob = price_new
            rev_persist = 0
            last_add_ts = ts_s

    # Settlement
    if pos_side != "flat" and pos_shares > 0:
        final_yes = float(df["yes_prob"].iloc[-1])
        payout = 1.0 if ((pos_side == "yes" and final_yes >= 0.5) or (pos_side == "no" and final_yes < 0.5)) else 0.0
        settle_cash = pos_shares * payout
        pnl = settle_cash - pos_shares * avg_cost
        trades.append(
            Trade(
                market_id,
                pos_side,
                int(df["ts_s"].iloc[-1]),
                float(df["elapsed_s"].iloc[-1]),
                f"settle_{pos_side}",
                final_yes,
                pos_shares,
                settle_cash,
                0.0,
                pnl,
            )
        )
        realized_pnl += pnl
    return trades, realized_pnl


def summarize(trades: list[Trade], pnl: float) -> dict:
    closes = [t for t in trades if t.action.startswith("sell_") or t.action.startswith("settle_")]
    wins = sum(1 for t in closes if t.pnl > 0)
    losses = sum(1 for t in closes if t.pnl < 0)
    win_rate = wins / len(closes) if closes else 0.0
    fees = sum(t.fee for t in trades)
    return {
        "trades_total": len(trades),
        "closed_trades": len(closes),
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "pnl_total": pnl,
        "fee_total": fees,
    }


def plot_market(df: pd.DataFrame, trades: list[Trade], out_png: Path, title: str) -> None:
    plt.figure(figsize=(12, 6), dpi=140)
    plt.plot(df["elapsed_s"], df["yes_prob"] * 100.0, color="#1f77b4", linewidth=2, label="YES prob")
    styles = {
        "buy_yes": ("^", "#2ca02c", "Buy YES"),
        "buy_yes_add": ("^", "#66bb6a", "Add YES"),
        "buy_yes_flip": ("^", "#2ca02c", "Flip->YES"),
        "sell_yes": ("x", "#d62728", "Sell YES"),
        "settle_yes": ("o", "#d62728", "Settle YES"),
        "buy_no": ("v", "#9467bd", "Buy NO"),
        "buy_no_add": ("v", "#ab47bc", "Add NO"),
        "buy_no_flip": ("v", "#9467bd", "Flip->NO"),
        "sell_no": ("x", "#ff7f0e", "Sell NO"),
        "settle_no": ("o", "#ff7f0e", "Settle NO"),
    }
    tdf = pd.DataFrame([t.__dict__ for t in trades])
    if not tdf.empty:
        for action, (marker, color, label) in styles.items():
            s = tdf[tdf["action"] == action]
            if s.empty:
                continue
            plt.scatter(
                s["elapsed_s"],
                s["price_yes"] * 100.0,
                s=70,
                marker=marker,
                color=color,
                linewidths=1.8 if marker == "x" else 0.0,
                label=label,
                zorder=5,
            )
    plt.xlim(0, 300)
    plt.ylim(0, 100)
    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("YES Probability (%)")
    plt.title(title)
    plt.grid(True, linestyle="--", alpha=0.35)
    plt.legend(loc="best", fontsize=8)
    plt.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png)
    plt.close()


def main() -> None:
    data_dir = Path("datasets/reports/official_kline_5m_1s_constructed")
    out_dir = Path("datasets/reports/official_fit_iter")
    plot_dir = out_dir / "plots"
    out_dir.mkdir(parents=True, exist_ok=True)
    plot_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(data_dir.glob("*.csv"))
    markets = [(f, load_market(f)) for f in files]

    grid = [
        Params(*vals)
        for vals in itertools.product(
            [8.0, 12.0, 20.0, 30.0],  # entry_vel_bps
            [1, 2, 3],                # entry_persist_s
            [-60.0, -80.0, -120.0, -160.0],  # reverse_vel_bps
            [1, 2, 3],                # reverse_persist_s
            [0.03, 0.05, 0.07],      # reverse_drop
            [1, 2],                  # add_interval_s
            [8.0, 12.0, 16.0],       # add_vel_bps
            [0.48, 0.50, 0.52],      # entry_prob_floor
        )
    ]

    rows = []
    best = None
    best_key = None
    for p in grid:
        all_trades: list[Trade] = []
        total_pnl = 0.0
        for _, df in markets:
            t, pnl = run_market(df, p)
            all_trades.extend(t)
            total_pnl += pnl
        s = summarize(all_trades, total_pnl)
        row = {
            **p.__dict__,
            **s,
        }
        rows.append(row)
        key = (s["pnl_total"], s["win_rate"], s["closed_trades"])
        if best is None or key > best_key:
            best = p
            best_key = key

    res_df = pd.DataFrame(rows).sort_values(
        ["pnl_total", "win_rate", "closed_trades"], ascending=False
    )
    res_df.to_csv(out_dir / "fit_grid_results.csv", index=False)
    top = res_df.head(20)
    top.to_csv(out_dir / "fit_grid_top20.csv", index=False)

    # Render per-market plots for best params
    market_summaries = []
    for f, df in markets:
        trades, pnl = run_market(df, best)
        title = f"{f.stem} | pnl={pnl:.4f} | params={best}"
        out_png = plot_dir / f"{f.stem}_fit_best.png"
        plot_market(df, trades, out_png, title)
        tdf = pd.DataFrame([t.__dict__ for t in trades])
        if not tdf.empty:
            tdf.to_csv(plot_dir / f"{f.stem}_actions_fit_best.csv", index=False)
        market_summaries.append(
            {
                "market_file": f.name,
                "pnl": pnl,
                "actions": int(len(trades)),
                "plot": str(out_png),
            }
        )

    summary = {
        "files": len(files),
        "grid_size": len(grid),
        "best_params": best.__dict__ if best else {},
        "best_row": res_df.iloc[0].to_dict() if not res_df.empty else {},
        "market_summaries": market_summaries,
    }
    (out_dir / "fit_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
