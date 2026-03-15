#!/usr/bin/env python3
import argparse
import glob
import json
import time
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


def load_cfg(path: str) -> dict:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    out = {k: raw[k] for k in PARAM_KEYS if k in raw}
    out["fee_cents_per_side"] = 0.0
    return out


def fetch_payload(
    base_url: str,
    symbol: str,
    market_type: str,
    lookback_minutes: int,
    max_trades: int,
    max_samples: int,
    cfg: dict,
    timeout: int,
    retries: int,
) -> dict:
    q = {
        "symbol": symbol,
        "market_type": market_type,
        "full_history": "false",
        "lookback_minutes": str(lookback_minutes),
        "max_trades": str(max_trades),
        "max_samples": str(max_samples),
        "use_autotune": "false",
    }
    for k, v in cfg.items():
        q[k] = str(v)
    url = f"{base_url}/api/strategy/paper?{urllib.parse.urlencode(q)}"
    last_exc: Exception | None = None
    for i in range(retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            last_exc = exc
            if i < retries:
                time.sleep(0.3 * (i + 1))
                continue
            raise
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("fetch failed")


def summarize(payload: dict) -> dict:
    summary = payload.get("summary") or {}
    trades = payload.get("trades") or []
    pnls = [float(t.get("pnl_cents") or 0.0) for t in trades]
    n = len(pnls)
    win_rate = (sum(1 for p in pnls if p > 0.0) * 100.0 / n) if n else 0.0
    avg_pnl = (sum(pnls) / n) if n else 0.0
    tail = pnls[-80:] if n >= 80 else pnls
    tail_win = (sum(1 for p in tail if p > 0.0) * 100.0 / len(tail)) if tail else 0.0
    tail_avg = (sum(tail) / len(tail)) if tail else 0.0
    return {
        "trade_count": int(summary.get("trade_count") or n),
        "win_rate_pct": float(summary.get("win_rate_pct") or win_rate),
        "avg_pnl_cents": float(summary.get("avg_pnl_cents") or avg_pnl),
        "net_pnl_cents": float(summary.get("net_pnl_cents") or summary.get("total_pnl_cents") or sum(pnls)),
        "max_drawdown_cents": float(summary.get("max_drawdown_cents") or 0.0),
        "last80_win_rate_pct": tail_win,
        "last80_avg_pnl_cents": tail_avg,
    }


def score_window(m: dict, min_trades: int, trade_target: int, win_floor: float) -> float:
    n = m["trade_count"]
    wr = m["win_rate_pct"]
    tw = m["last80_win_rate_pct"]
    avg = m["avg_pnl_cents"]
    tavg = m["last80_avg_pnl_cents"]
    net = m["net_pnl_cents"]
    dd = m["max_drawdown_cents"]
    participation = min(max(n, 0) / max(trade_target, 1), 1.2)
    sample_penalty = max(0, min_trades - n) * 6.0
    wr_penalty = max(0.0, win_floor - wr) * 22.0 + max(0.0, (win_floor - 2.0) - tw) * 16.0
    return (
        wr * 5.2
        + tw * 3.5
        + avg * 2.2
        + tavg * 1.8
        + max(0.0, net) * 0.015
        + participation * 150.0
        - dd * 0.9
        - sample_penalty
        - wr_penalty
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Compare cand_v2 against all profiles under unified baseline.")
    ap.add_argument("--base-url", default="http://127.0.0.1:9830")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--market-type", default="5m")
    ap.add_argument("--lookbacks", default="720,1440,2880")
    ap.add_argument("--max-trades", type=int, default=1400)
    ap.add_argument("--max-samples", type=int, default=220000)
    ap.add_argument("--min-trades", type=int, default=180)
    ap.add_argument("--trade-target", type=int, default=500)
    ap.add_argument("--win-floor", type=float, default=86.0)
    ap.add_argument("--profiles-glob", default="configs/strategy-profiles/*.full.json")
    ap.add_argument("--cand-v2", default="/tmp/polyedge_eval/cand_bg_v2.json")
    ap.add_argument("--refine-1440", default="/tmp/polyedge_bg/bg_refine_candv2_5m_1440_20260227T175157Z.result.json")
    ap.add_argument("--refine-2880", default="/tmp/polyedge_bg/bg_refine_candv2_5m_2880_20260227T175157Z.result.json")
    ap.add_argument("--timeout", type=int, default=240)
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--out", default="/tmp/polyedge_eval/cand_v2_vs_all_full_compare.json")
    args = ap.parse_args()

    lookbacks = [int(x.strip()) for x in args.lookbacks.split(",") if x.strip()]
    candidates: list[dict] = []

    for p in sorted(glob.glob(args.profiles_glob)):
        candidates.append({"name": Path(p).name, "source": "profile", "cfg": load_cfg(p)})
    if Path(args.cand_v2).exists():
        candidates.append({"name": "cand_v2_bg_parallel_best.json", "source": "cand", "cfg": load_cfg(args.cand_v2)})
    if Path(args.refine_1440).exists():
        data = json.loads(Path(args.refine_1440).read_text(encoding="utf-8"))
        candidates.append({"name": "cand_refine_1440_best.json", "source": "refine", "cfg": {k: v for k, v in (data.get("best_cfg") or {}).items() if k in PARAM_KEYS}})
    if Path(args.refine_2880).exists():
        data = json.loads(Path(args.refine_2880).read_text(encoding="utf-8"))
        candidates.append({"name": "cand_refine_2880_best.json", "source": "refine", "cfg": {k: v for k, v in (data.get("best_cfg") or {}).items() if k in PARAM_KEYS}})

    ranked = []
    for c in candidates:
        windows = []
        for lb in lookbacks:
            payload = fetch_payload(
                args.base_url,
                args.symbol,
                args.market_type,
                lb,
                args.max_trades,
                args.max_samples,
                c["cfg"],
                args.timeout,
                args.retries,
            )
            m = summarize(payload)
            m["lookback_minutes"] = lb
            m["window_score"] = score_window(m, args.min_trades, args.trade_target, args.win_floor)
            windows.append(m)
        ranked.append(
            {
                "name": c["name"],
                "source": c["source"],
                "avg_trade_count": sum(w["trade_count"] for w in windows) / len(windows),
                "avg_win_rate_pct": sum(w["win_rate_pct"] for w in windows) / len(windows),
                "avg_net_pnl_cents": sum(w["net_pnl_cents"] for w in windows) / len(windows),
                "avg_max_drawdown_cents": sum(w["max_drawdown_cents"] for w in windows) / len(windows),
                "unified_score": sum(w["window_score"] for w in windows) / len(windows),
                "windows": windows,
                "cfg": c["cfg"],
            }
        )

    ranked.sort(
        key=lambda x: (
            x["unified_score"],
            x["avg_win_rate_pct"],
            x["avg_net_pnl_cents"],
            -x["avg_max_drawdown_cents"],
            x["avg_trade_count"],
        ),
        reverse=True,
    )

    cand = next((r for r in ranked if r["name"] == "cand_v2_bg_parallel_best.json"), None)
    cand_vs_all = []
    if cand is not None:
        for r in ranked:
            if r["name"] == cand["name"]:
                continue
            cand_vs_all.append(
                {
                    "name": r["name"],
                    "source": r["source"],
                    "delta_vs_cand_v2": {
                        "score": cand["unified_score"] - r["unified_score"],
                        "win_rate_pct": cand["avg_win_rate_pct"] - r["avg_win_rate_pct"],
                        "net_pnl_cents": cand["avg_net_pnl_cents"] - r["avg_net_pnl_cents"],
                        "max_drawdown_cents": cand["avg_max_drawdown_cents"] - r["avg_max_drawdown_cents"],
                        "trade_count": cand["avg_trade_count"] - r["avg_trade_count"],
                    },
                }
            )

    out = {
        "query": {
            "symbol": args.symbol,
            "market_type": args.market_type,
            "lookbacks": lookbacks,
            "full_history": False,
            "max_trades": args.max_trades,
            "max_samples": args.max_samples,
            "score_model": {
                "min_trades": args.min_trades,
                "trade_target": args.trade_target,
                "win_floor": args.win_floor,
            },
        },
        "ranked": ranked,
        "cand_v2_vs_all": cand_vs_all,
        "generated_at_ms": int(time.time() * 1000),
    }

    Path(args.out).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(args.out)
    for i, r in enumerate(ranked[:12], 1):
        print(
            "{}. {} | score={:.2f} win={:.2f} net={:.1f} dd={:.2f} tr={:.1f}".format(
                i,
                r["name"],
                r["unified_score"],
                r["avg_win_rate_pct"],
                r["avg_net_pnl_cents"],
                r["avg_max_drawdown_cents"],
                r["avg_trade_count"],
            )
        )


if __name__ == "__main__":
    main()
