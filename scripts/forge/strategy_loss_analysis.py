#!/usr/bin/env python3
import argparse
import json
import urllib.parse
import urllib.request
from pathlib import Path

PARAM_KEYS = {
    "entry_threshold_base",
    "entry_threshold_cap",
    "spread_limit_prob",
    "entry_edge_prob",
    "entry_min_potential_cents",
    "entry_max_price_cents",
    "min_hold_ms",
    "stop_loss_cents",
    "reverse_signal_threshold",
    "reverse_signal_ticks",
    "trail_activate_profit_cents",
    "trail_drawdown_cents",
    "take_profit_near_max_cents",
    "endgame_take_profit_cents",
    "endgame_remaining_ms",
    "liquidity_widen_prob",
    "cooldown_ms",
    "max_entries_per_round",
    "max_exec_spread_cents",
    "slippage_cents_per_side",
    "fee_cents_per_side",
    "emergency_wide_spread_penalty_ratio",
}


def load_cfg(path: str | None) -> dict:
    if not path:
        return {}
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return {k: v for k, v in raw.items() if k in PARAM_KEYS}


def fetch_payload(
    base_url: str,
    symbol: str,
    market_type: str,
    full_history: bool,
    lookback_minutes: int,
    max_trades: int,
    cfg: dict,
    timeout: int,
) -> dict:
    q = {
        "symbol": symbol,
        "market_type": market_type,
        "full_history": "true" if full_history else "false",
        "lookback_minutes": str(lookback_minutes),
        "max_trades": str(max_trades),
        "use_autotune": "false",
    }
    for k, v in cfg.items():
        q[k] = str(v)
    url = f"{base_url}/api/strategy/paper?{urllib.parse.urlencode(q)}"
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def analyze(payload: dict) -> dict:
    summary = payload.get("summary") or {}
    trades = payload.get("trades") or []
    pnls = [float(t.get("pnl_cents") or 0.0) for t in trades]

    reason_map: dict[str, dict] = {}
    for t in trades:
        reason = str(t.get("exit_reason") or "unknown")
        pnl = float(t.get("pnl_cents") or 0.0)
        row = reason_map.setdefault(
            reason,
            {"count": 0, "win_count": 0, "net_pnl_cents": 0.0, "loss_pnl_cents": 0.0},
        )
        row["count"] += 1
        row["net_pnl_cents"] += pnl
        if pnl > 0:
            row["win_count"] += 1
        else:
            row["loss_pnl_cents"] += pnl
    for row in reason_map.values():
        c = max(1, row["count"])
        row["win_rate_pct"] = row["win_count"] * 100.0 / c
        row["avg_pnl_cents"] = row["net_pnl_cents"] / c

    # cumulative pnl and drawdown curve
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    dd_idx = -1
    curve = []
    for i, pnl in enumerate(pnls):
        equity += pnl
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd
            dd_idx = i
        curve.append({"idx": i + 1, "equity_cents": equity, "drawdown_cents": dd})

    worst_trades = sorted(
        (
            {
                "id": int(t.get("id") or 0),
                "side": t.get("side"),
                "entry_ts_ms": t.get("entry_ts_ms"),
                "exit_ts_ms": t.get("exit_ts_ms"),
                "entry_remaining_ms": t.get("entry_remaining_ms"),
                "exit_remaining_ms": t.get("exit_remaining_ms"),
                "entry_price_cents": t.get("entry_price_cents"),
                "exit_price_cents": t.get("exit_price_cents"),
                "pnl_cents": float(t.get("pnl_cents") or 0.0),
                "exit_reason": t.get("exit_reason"),
                "entry_score": t.get("entry_score"),
                "exit_score": t.get("exit_score"),
            }
            for t in trades
        ),
        key=lambda x: x["pnl_cents"],
    )[:15]

    n = len(pnls)
    win_rate = (sum(1 for x in pnls if x > 0.0) * 100.0 / n) if n else 0.0
    avg_pnl = (sum(pnls) / n) if n else 0.0
    tail = pnls[-80:] if n >= 80 else pnls
    tail_win = (sum(1 for x in tail if x > 0.0) * 100.0 / len(tail)) if tail else 0.0
    tail_avg = (sum(tail) / len(tail)) if tail else 0.0

    top_loss_reasons = sorted(
        (
            {"reason": k, **v}
            for k, v in reason_map.items()
            if float(v.get("loss_pnl_cents") or 0.0) < 0.0
        ),
        key=lambda x: x["loss_pnl_cents"],
    )[:8]

    return {
        "summary": {
            "trade_count": int(summary.get("trade_count") or n),
            "win_rate_pct": float(summary.get("win_rate_pct") or win_rate),
            "avg_pnl_cents": float(summary.get("avg_pnl_cents") or avg_pnl),
            "net_pnl_cents": float(summary.get("net_pnl_cents") or summary.get("total_pnl_cents") or sum(pnls)),
            "max_drawdown_cents": float(summary.get("max_drawdown_cents") or max_dd),
            "last80_win_rate_pct": tail_win,
            "last80_avg_pnl_cents": tail_avg,
            "recomputed_max_drawdown_cents": max_dd,
            "max_drawdown_trade_idx": dd_idx + 1 if dd_idx >= 0 else 0,
        },
        "exit_reason_breakdown": sorted(
            ({"reason": k, **v} for k, v in reason_map.items()),
            key=lambda x: (x["count"], x["net_pnl_cents"]),
            reverse=True,
        ),
        "top_loss_reasons": top_loss_reasons,
        "worst_trades": worst_trades,
        "equity_curve_tail": curve[-80:],
    }


def delta(a: dict, b: dict) -> dict:
    sa = a["summary"]
    sb = b["summary"]
    keys = [
        "trade_count",
        "win_rate_pct",
        "avg_pnl_cents",
        "net_pnl_cents",
        "max_drawdown_cents",
        "last80_win_rate_pct",
        "last80_avg_pnl_cents",
    ]
    return {k: float(sb[k]) - float(sa[k]) for k in keys}


def main() -> None:
    ap = argparse.ArgumentParser(description="Analyze why pnl/drawdown happen under unified baseline.")
    ap.add_argument("--base-url", default="http://127.0.0.1:9810")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--market-type", default="5m")
    ap.add_argument(
        "--full-history",
        action="store_true",
        help="Request full history from API (default false to honor lookback window).",
    )
    ap.add_argument("--lookback-minutes", type=int, default=1440)
    ap.add_argument("--max-trades", type=int, default=900)
    ap.add_argument("--config-a", help="baseline config json")
    ap.add_argument("--config-b", help="candidate config json")
    ap.add_argument("--timeout", type=int, default=240)
    ap.add_argument("--out", default="/tmp/strategy_loss_analysis.json")
    args = ap.parse_args()

    cfg_a = load_cfg(args.config_a)
    cfg_b = load_cfg(args.config_b) if args.config_b else cfg_a

    pa = fetch_payload(
        args.base_url,
        args.symbol,
        args.market_type,
        args.full_history,
        args.lookback_minutes,
        args.max_trades,
        cfg_a,
        args.timeout,
    )
    pb = fetch_payload(
        args.base_url,
        args.symbol,
        args.market_type,
        args.full_history,
        args.lookback_minutes,
        args.max_trades,
        cfg_b,
        args.timeout,
    )
    aa = analyze(pa)
    bb = analyze(pb)

    report = {
        "query": {
            "symbol": args.symbol,
            "market_type": args.market_type,
            "full_history": args.full_history,
            "lookback_minutes": args.lookback_minutes,
            "max_trades": args.max_trades,
            "use_autotune": False,
        },
        "a": aa,
        "b": bb,
        "delta_b_minus_a": delta(aa, bb),
    }
    Path(args.out).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
