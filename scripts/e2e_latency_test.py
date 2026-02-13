#!/usr/bin/env python3
"""
Modular entrypoint for end-to-end latency benchmarking.
"""

from __future__ import annotations

import argparse
import asyncio

from latency_probe.models import SYMBOL_TO_NAME
from latency_probe.pipeline import run_rest_pipeline
from latency_probe.stats import print_stat_block
from latency_probe.ws_probe import has_delta, run_ws_latency


def main() -> None:
    parser = argparse.ArgumentParser(description="Full pipeline latency benchmark")
    parser.add_argument("--symbol", default="BTCUSDT", choices=list(SYMBOL_TO_NAME.keys()))
    parser.add_argument("--iterations", type=int, default=80)
    parser.add_argument("--ws-seconds", type=int, default=20)
    args = parser.parse_args()

    print("=== REST E2E PIPELINE LATENCY ===")
    rest = run_rest_pipeline(args.iterations, args.symbol)
    print(f"symbol={args.symbol}")
    print(
        "market=",
        rest["market"]["question"],
        f"(market_id={rest['market']['market_id']}, yes_token={rest['market']['yes_token'][:8]}...)",
    )
    print(f"samples={rest['n']} failures={rest['failures']} intents_avg={rest['intents_avg']:.3f}")
    if rest.get("failure_reasons"):
        print("failure_reasons=", rest["failure_reasons"])
    print_stat_block("ref_fetch", rest["ref_fetch_ms"], "ms")
    print_stat_block("book_fetch", rest["book_fetch_ms"], "ms")
    print_stat_block("signal", rest["signal_us"], "us")
    print_stat_block("quote", rest["quote_us"], "us")
    print_stat_block("risk", rest["risk_us"], "us")
    print_stat_block("exec", rest["exec_us"], "us")
    print_stat_block("total_e2e", rest["total_ms"], "ms")

    print("\n=== WS FEED LATENCY (RECV - SOURCE TS) ===")
    ws = asyncio.run(run_ws_latency(args.ws_seconds, args.symbol))
    print_stat_block("polymarket_ws", ws["pm_lag_ms"], "ms")
    print_stat_block("binance_ws", ws["bin_lag_ms"], "ms")
    if has_delta(ws):
        print(f"delta_pm_minus_bin_p50={ws['delta_pm_minus_bin_p50_ms']:.3f}ms")


if __name__ == "__main__":
    main()
