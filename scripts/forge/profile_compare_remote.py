#!/usr/bin/env python3
import json
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

BASE = "http://127.0.0.1:19810/api/strategy/paper"
LOOKBACKS = [180, 360, 720, 1440, 2880]
PROFILES = ["hi_win", "hi_freq", "balanced", "cand_growth_mix", "profit_max"]
SYMS = ["BTCUSDT", "SOLUSDT"]


def fetch(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=180) as resp:
        return json.loads(resp.read().decode())


def main() -> None:
    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "base": BASE,
        "lookbacks": LOOKBACKS,
        "profiles": {},
    }

    for profile in PROFILES:
        result["profiles"][profile] = {"symbols": {}, "profile_config": None}
        for sym in SYMS:
            windows = []
            profile_config = None
            for lb in LOOKBACKS:
                query = (
                    f"?symbol={sym}&market_type=5m&profile={profile}"
                    f"&lookback_minutes={lb}&max_samples=180000"
                )
                payload = fetch(BASE + query)
                if "error" in payload:
                    raise RuntimeError(
                        f"profile={profile} sym={sym} lb={lb} error={payload['error']}"
                    )
                if profile_config is None:
                    profile_config = payload.get("config")
                summary = payload.get("summary") or {}
                windows.append(
                    {
                        "lookback": lb,
                        "trade_count": int(summary.get("trade_count") or 0),
                        "win_rate_pct": float(summary.get("win_rate_pct") or 0.0),
                        "avg_pnl_cents": float(summary.get("avg_pnl_cents") or 0.0),
                        "net_pnl_cents": float(
                            summary.get("net_pnl_cents")
                            or summary.get("total_pnl_cents")
                            or 0.0
                        ),
                        "max_drawdown_cents": float(
                            summary.get("max_drawdown_cents") or 0.0
                        ),
                        "total_cost_cents": float(summary.get("total_cost_cents") or 0.0),
                        "net_margin_pct": float(summary.get("net_margin_pct") or 0.0),
                    }
                )

            stats = {
                "avg_win_rate_pct": mean(w["win_rate_pct"] for w in windows),
                "min_win_rate_pct": min(w["win_rate_pct"] for w in windows),
                "avg_net_pnl_cents": mean(w["net_pnl_cents"] for w in windows),
                "min_net_pnl_cents": min(w["net_pnl_cents"] for w in windows),
                "avg_max_drawdown_cents": mean(w["max_drawdown_cents"] for w in windows),
                "avg_trade_count": mean(w["trade_count"] for w in windows),
                "avg_net_margin_pct": mean(w["net_margin_pct"] for w in windows),
            }
            result["profiles"][profile]["symbols"][sym] = {
                "stats": stats,
                "windows": windows,
            }
            if result["profiles"][profile]["profile_config"] is None:
                result["profiles"][profile]["profile_config"] = profile_config

    ranking = {}
    for sym in SYMS:
        rows = []
        for profile in PROFILES:
            stats = result["profiles"][profile]["symbols"][sym]["stats"]
            score = (
                stats["avg_net_pnl_cents"] * 0.015
                + stats["avg_win_rate_pct"] * 4.0
                + stats["min_win_rate_pct"] * 2.5
                + stats["avg_net_margin_pct"] * 1.8
                - stats["avg_max_drawdown_cents"] * 0.85
            )
            rows.append({"profile": profile, "score": score, **stats})
        rows.sort(key=lambda x: x["score"], reverse=True)
        ranking[sym] = rows

    combined = []
    for profile in PROFILES:
        btc = next(x for x in ranking["BTCUSDT"] if x["profile"] == profile)
        sol = next(x for x in ranking["SOLUSDT"] if x["profile"] == profile)
        combined.append(
            {
                "profile": profile,
                "combined_score": (btc["score"] + sol["score"]) / 2.0,
                "btc_score": btc["score"],
                "sol_score": sol["score"],
                "btc_avg_win": btc["avg_win_rate_pct"],
                "sol_avg_win": sol["avg_win_rate_pct"],
                "btc_avg_net": btc["avg_net_pnl_cents"],
                "sol_avg_net": sol["avg_net_pnl_cents"],
            }
        )
    combined.sort(key=lambda x: x["combined_score"], reverse=True)
    result["ranking"] = {"by_symbol": ranking, "combined": combined}

    out = Path("/home/ubuntu/PolyEdge/outputs/hpo/profile_5config_windows_remote19810.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(out))
    print(json.dumps(combined, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

