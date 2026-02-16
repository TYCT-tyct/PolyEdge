#!/usr/bin/env python3
import argparse
import json
import sys
import urllib.request


def _get(d, path):
    cur = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur


def main() -> int:
    ap = argparse.ArgumentParser(description="Print key Shadow KPIs for quick analysis.")
    ap.add_argument("--base-url", default="http://127.0.0.1:8080")
    args = ap.parse_args()

    url = args.base_url.rstrip("/") + "/report/shadow/live"
    with urllib.request.urlopen(url, timeout=5) as r:
        d = json.load(r)

    fields = [
        ("data_valid_ratio", ["data_valid_ratio"]),
        ("seq_gap_rate", ["seq_gap_rate"]),
        ("ts_inversion_rate", ["ts_inversion_rate"]),
        ("executed_over_eligible", ["executed_over_eligible"]),
        ("net_edge_p50_bps", ["net_edge_p50_bps"]),
        ("net_edge_p10_bps", ["net_edge_p10_bps"]),
        ("ev_positive_ratio", ["ev_positive_ratio"]),
        ("feed_in_p50_ms", ["latency", "feed_in_p50_ms"]),
        ("feed_in_p90_ms", ["latency", "feed_in_p90_ms"]),
        ("feed_in_p99_ms", ["latency", "feed_in_p99_ms"]),
        ("decision_compute_p50_ms", ["latency", "decision_compute_p50_ms"]),
        ("decision_compute_p90_ms", ["latency", "decision_compute_p90_ms"]),
        ("decision_compute_p99_ms", ["latency", "decision_compute_p99_ms"]),
        ("ack_only_p50_ms", ["latency", "ack_only_p50_ms"]),
        ("ack_only_p90_ms", ["latency", "ack_only_p90_ms"]),
        ("ack_only_p99_ms", ["latency", "ack_only_p99_ms"]),
        ("ack_only_n", ["latency", "ack_only_n"]),
        ("tick_to_ack_p50_ms", ["latency", "tick_to_ack_p50_ms"]),
        ("tick_to_ack_p90_ms", ["latency", "tick_to_ack_p90_ms"]),
        ("tick_to_ack_p99_ms", ["latency", "tick_to_ack_p99_ms"]),
        ("tick_to_ack_n", ["latency", "tick_to_ack_n"]),
        ("book_top_lag_p50_ms", ["book_top_lag_p50_ms"]),
        ("book_top_lag_p90_ms", ["book_top_lag_p90_ms"]),
        ("book_top_lag_p99_ms", ["book_top_lag_p99_ms"]),
        ("book_top_lag_at_ack_p50_ms", ["latency", "book_top_lag_at_ack_p50_ms"]),
        ("book_top_lag_at_ack_p90_ms", ["latency", "book_top_lag_at_ack_p90_ms"]),
        ("book_top_lag_at_ack_p99_ms", ["latency", "book_top_lag_at_ack_p99_ms"]),
        ("book_top_lag_at_ack_n", ["latency", "book_top_lag_at_ack_n"]),
        ("capturable_window_p50_ms", ["latency", "capturable_window_p50_ms"]),
        ("capturable_window_p75_ms", ["latency", "capturable_window_p75_ms"]),
        ("capturable_window_p90_ms", ["latency", "capturable_window_p90_ms"]),
        ("capturable_window_p99_ms", ["latency", "capturable_window_p99_ms"]),
        ("capturable_window_n", ["latency", "capturable_window_n"]),
        ("profitable_window_ratio", ["latency", "profitable_window_ratio"]),
    ]

    for name, path in fields:
        v = _get(d, path)
        print(f"{name}={v}")

    # Sanity check: capturable should match (book_top_lag_at_ack - tick_to_ack) on the same sample set.
    try:
        lag_at_ack_p50 = float(_get(d, ["latency", "book_top_lag_at_ack_p50_ms"]) or 0.0)
        tta_p50 = float(_get(d, ["latency", "tick_to_ack_p50_ms"]) or 0.0)
        capt_est = lag_at_ack_p50 - tta_p50
        print(f"capturable_est_p50_ms={capt_est}")
    except Exception:
        pass

    br = d.get("blocked_reason_counts", {})
    if isinstance(br, dict):
        top10 = sorted(br.items(), key=lambda kv: kv[1], reverse=True)[:10]
        print("blocked_reason_top10=" + json.dumps(top10, separators=(",", ":")))

    # Quick hint for operators.
    ack_n = _get(d, ["latency", "ack_only_n"]) or 0
    if ack_n == 0:
        print("note=ack_only_ms not measured (likely paper mode); switch execution to live to measure real RTT")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
