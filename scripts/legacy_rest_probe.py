#!/usr/bin/env python3
"""Legacy REST pipeline benchmark kept only for historical comparison."""

from __future__ import annotations

import argparse

from latency_probe.models import SYMBOL_TO_NAME
from latency_probe.pipeline import run_rest_pipeline
from latency_probe.stats import print_stat_block


def main() -> None:
    parser = argparse.ArgumentParser(description="Legacy REST latency probe")
    parser.add_argument("--symbol", default="BTCUSDT", choices=list(SYMBOL_TO_NAME.keys()))
    parser.add_argument("--iterations", type=int, default=80)
    args = parser.parse_args()

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


if __name__ == "__main__":
    main()
