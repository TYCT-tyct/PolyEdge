#!/usr/bin/env python3
import argparse
import json
import urllib.parse
import urllib.request
from pathlib import Path

PARAM_KEYS = [
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
    "emergency_wide_spread_penalty_ratio",
    "stop_loss_grace_ticks",
    "stop_loss_hard_mult",
    "stop_loss_reverse_extra_ticks",
    "loss_cluster_limit",
    "loss_cluster_cooldown_ms",
    "noise_gate_enabled",
    "noise_gate_threshold_add",
    "noise_gate_edge_add",
    "noise_gate_spread_scale",
    "vic_enabled",
    "vic_target_entries_per_hour",
    "vic_deadband_ratio",
    "vic_threshold_relax_max",
    "vic_edge_relax_max",
    "vic_spread_relax_max",
]


def fetch_payload(
    base_url: str,
    symbol: str,
    market_type: str,
    full_history: bool,
    lookback_minutes: int,
    max_trades: int,
    max_samples: int,
    cfg: dict | None,
) -> dict:
    q: dict[str, str] = {
        "symbol": symbol,
        "market_type": market_type,
        "full_history": "true" if full_history else "false",
        "lookback_minutes": str(lookback_minutes),
        "max_trades": str(max_trades),
        "max_samples": str(max_samples),
    }
    if cfg:
        for k, v in cfg.items():
            q[k] = str(v)
    url = f"{base_url}/api/strategy/paper?{urllib.parse.urlencode(q)}"
    with urllib.request.urlopen(url, timeout=180) as resp:
        return json.loads(resp.read().decode("utf-8"))


def summarize(payload: dict) -> dict:
    summary = payload.get("summary") or {}
    trades = payload.get("trades") or []
    last50 = trades[-50:]
    last50_pnls = [float(t.get("pnl_cents") or 0.0) for t in last50]
    last50_count = len(last50_pnls)
    last50_wins = sum(1 for p in last50_pnls if p > 0.0)
    last50_win = (last50_wins * 100.0 / last50_count) if last50_count else 0.0
    last50_avg = (sum(last50_pnls) / last50_count) if last50_count else 0.0
    last50_total = sum(last50_pnls)

    neg = [t for t in trades if float(t.get("pnl_cents") or 0.0) <= 0.0]
    stop_losses = [t for t in neg if t.get("exit_reason") == "stop_loss"]
    stop_loss_avg = (
        sum(float(t.get("pnl_cents") or 0.0) for t in stop_losses) / len(stop_losses)
        if stop_losses
        else 0.0
    )
    cover_fail = [
        t
        for t in trades
        if float(t.get("pnl_gross_cents") or 0.0)
        <= float(t.get("total_cost_cents") or 0.0)
    ]
    n = max(1, len(trades))
    return {
        "trade_count": int(summary.get("trade_count") or len(trades)),
        "win_rate_pct": float(summary.get("win_rate_pct") or 0.0),
        "avg_pnl_cents": float(summary.get("avg_pnl_cents") or 0.0),
        "net_pnl_cents": float(summary.get("net_pnl_cents") or summary.get("total_pnl_cents") or 0.0),
        "max_drawdown_cents": float(summary.get("max_drawdown_cents") or 0.0),
        "total_cost_cents": float(summary.get("total_cost_cents") or 0.0),
        "net_margin_pct": float(summary.get("net_margin_pct") or 0.0),
        "last50_win_rate_pct": last50_win,
        "last50_avg_pnl_cents": last50_avg,
        "last50_total_pnl_cents": last50_total,
        "negative_ratio": len(neg) / n,
        "cover_fail_ratio": len(cover_fail) / n,
        "stop_loss_count": len(stop_losses),
        "stop_loss_avg_pnl_cents": stop_loss_avg,
    }


def compare(a: dict, b: dict) -> dict:
    out: dict[str, float] = {}
    keys = sorted(set(a.keys()) & set(b.keys()))
    for k in keys:
        if isinstance(a[k], (int, float)) and isinstance(b[k], (int, float)):
            out[k] = float(b[k]) - float(a[k])
    return out


def load_cfg(path: str | None) -> dict | None:
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    raw = json.loads(p.read_text(encoding="utf-8"))
    out = {k: raw[k] for k in PARAM_KEYS if k in raw}
    out["fee_cents_per_side"] = 0.0
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="A/B compare strategy paper metrics under same query window.")
    ap.add_argument("--url-a", default="http://127.0.0.1:9830")
    ap.add_argument("--url-b", default="http://127.0.0.1:9830")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--market-type", default="5m")
    ap.add_argument(
        "--full-history",
        action="store_true",
        help="Request full history from API (default false to honor lookback window).",
    )
    ap.add_argument("--lookback-minutes", type=int, default=2880)
    ap.add_argument("--max-trades", type=int, default=1500)
    ap.add_argument(
        "--max-samples",
        type=int,
        default=260000,
        help="Upper bound for /api/strategy/paper max_samples; lower value reduces API memory pressure.",
    )
    ap.add_argument("--config-a", help="JSON file for query A override params")
    ap.add_argument("--config-b", help="JSON file for query B override params")
    ap.add_argument("--out", help="Optional output json path")
    args = ap.parse_args()

    cfg_a = load_cfg(args.config_a)
    cfg_b = load_cfg(args.config_b)

    payload_a = fetch_payload(
        args.url_a,
        args.symbol,
        args.market_type,
        args.full_history,
        args.lookback_minutes,
        args.max_trades,
        args.max_samples,
        cfg_a,
    )
    payload_b = fetch_payload(
        args.url_b,
        args.symbol,
        args.market_type,
        args.full_history,
        args.lookback_minutes,
        args.max_trades,
        args.max_samples,
        cfg_b,
    )
    metrics_a = summarize(payload_a)
    metrics_b = summarize(payload_b)
    delta = compare(metrics_a, metrics_b)

    report = {
        "a": metrics_a,
        "b": metrics_b,
        "delta_b_minus_a": delta,
        "query": {
            "symbol": args.symbol,
            "market_type": args.market_type,
            "full_history": args.full_history,
            "lookback_minutes": args.lookback_minutes,
            "max_trades": args.max_trades,
            "max_samples": args.max_samples,
        },
    }
    if args.out:
        Path(args.out).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
