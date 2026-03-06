#!/usr/bin/env python3
from __future__ import annotations

import argparse
import collections
import json
import math
import pathlib
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]


def fetch_json(url: str, timeout: float) -> Any:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.load(resp)


def paper_url(base_url: str, market_type: str, symbol: str) -> str:
    params = urllib.parse.urlencode(
        {
            "market_type": market_type,
            "source": "live",
            "symbol": symbol,
        }
    )
    return f"{base_url.rstrip('/')}/api/strategy/paper?{params}"


def latest_all_url(base_url: str) -> str:
    return f"{base_url.rstrip('/')}/api/latest/all"


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    idx = round((len(values) - 1) * max(0.0, min(1.0, p)))
    return round(values[idx], 4)


def avg(values: list[float | int]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 4)


def safe_len(value: Any) -> int:
    return len(value) if isinstance(value, list) else 0


def snapshot_map(rows: Any) -> dict[tuple[str, str], dict[str, Any]]:
    out: dict[tuple[str, str], dict[str, Any]] = {}
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        tf = str(row.get("market_type") or row.get("timeframe") or "").strip().lower()
        if symbol and tf:
            out[(symbol, tf)] = row
    return out


def best_book_price_cents(decision: dict[str, Any], snapshot: dict[str, Any]) -> float | None:
    action = str(decision.get("action") or "").strip().lower()
    side = str(decision.get("side") or "").strip().upper()
    if action in ("enter", "add"):
        key = "best_ask_up" if side == "UP" else "best_ask_down"
    elif action in ("exit", "reduce"):
        key = "best_bid_up" if side == "UP" else "best_bid_down"
    else:
        return None
    raw = snapshot.get(key)
    if raw is None:
        return None
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(val):
        return None
    return round(val * 100.0, 4)


def adverse_gap_cents(action: str, signal_price: float, book_price: float) -> float:
    if action in ("enter", "add"):
        return round(book_price - signal_price, 4)
    return round(signal_price - book_price, 4)


def decision_gap_rows(decisions: list[Any], snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in decisions:
        if not isinstance(row, dict):
            continue
        try:
            signal_price = float(row.get("price_cents"))
        except (TypeError, ValueError):
            continue
        if not math.isfinite(signal_price):
            continue
        action = str(row.get("action") or "").strip().lower()
        if action not in ("enter", "add", "exit", "reduce"):
            continue
        book_price = best_book_price_cents(row, snapshot)
        if book_price is None:
            continue
        adverse = adverse_gap_cents(action, signal_price, book_price)
        out.append(
            {
                "decision_id": row.get("decision_id"),
                "action": action,
                "side": row.get("side"),
                "signal_price_cents": round(signal_price, 4),
                "book_price_cents": round(book_price, 4),
                "abs_gap_cents": round(abs(book_price - signal_price), 4),
                "adverse_gap_cents": adverse,
                "remaining_ms": row.get("remaining_ms"),
                "reason": row.get("reason"),
            }
        )
    return out


def counter_to_dict(counter: collections.Counter[str]) -> dict[str, int]:
    return {k: int(v) for k, v in counter.most_common()}


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Observe 5m live shadow coverage and approximate signal-vs-book price gaps "
            "for multiple symbols without sending real orders."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:9810")
    parser.add_argument("--market-type", default="5m")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--duration-sec", type=int, default=14_400)
    parser.add_argument("--poll-sec", type=float, default=5.0)
    parser.add_argument("--timeout-sec", type=float, default=15.0)
    parser.add_argument("--jsonl-dir", default="")
    parser.add_argument("--summary-out", default="")
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        print("no symbols provided", file=sys.stderr)
        return 2

    jsonl_dir = pathlib.Path(args.jsonl_dir).expanduser() if args.jsonl_dir else None
    if jsonl_dir is not None:
        jsonl_dir.mkdir(parents=True, exist_ok=True)

    symbol_state: dict[str, dict[str, Any]] = {}
    jsonl_fp: dict[str, Any] = {}
    for symbol in symbols:
        symbol_state[symbol] = {
            "poll_count": 0,
            "ok_count": 0,
            "error_count": 0,
            "status_counter": collections.Counter(),
            "suggested_action_counter": collections.Counter(),
            "no_candidate_reason_counter": collections.Counter(),
            "candidate_source_counter": collections.Counter(),
            "trade_count_series": [],
            "win_rate_series": [],
            "net_pnl_series": [],
            "raw_signal_series": [],
            "decision_pool_series": [],
            "fresh_signal_series": [],
            "candidate_series": [],
            "parity_ready_series": [],
            "state_selected_series": [],
            "target_ready_series": [],
            "state_skipped_series": [],
            "skip_reason_counter": collections.Counter(),
            "gap_abs_values": [],
            "gap_adverse_values": [],
            "gap_positive_values": [],
            "gap_rows_seen": 0,
            "target_missing_count": 0,
            "last_snapshot_ts_ms": None,
            "last_status": None,
            "last_shadow_eval": None,
            "last_current": None,
        }
        if jsonl_dir is not None:
            fp = open(jsonl_dir / f"{symbol}_{args.market_type}_shadow.jsonl", "a", encoding="utf-8")
            jsonl_fp[symbol] = fp

    deadline = time.time() + max(1, args.duration_sec)
    try:
        while time.time() < deadline:
            poll_ts_ms = int(time.time() * 1000)
            try:
                latest_rows = fetch_json(latest_all_url(args.base_url), args.timeout_sec)
                latest = snapshot_map(latest_rows)
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as err:
                latest = {}
                for symbol in symbols:
                    st = symbol_state[symbol]
                    st["poll_count"] += 1
                    st["error_count"] += 1
                    st["status_counter"]["latest_all_fetch_error"] += 1
                    if symbol in jsonl_fp:
                        jsonl_fp[symbol].write(
                            json.dumps(
                                {
                                    "ts_ms": poll_ts_ms,
                                    "ok": False,
                                    "error": str(err),
                                    "source": "latest_all",
                                },
                                ensure_ascii=False,
                            )
                            + "\n"
                        )
                        jsonl_fp[symbol].flush()
                time.sleep(args.poll_sec)
                continue

            for symbol in symbols:
                st = symbol_state[symbol]
                st["poll_count"] += 1
                try:
                    payload = fetch_json(
                        paper_url(args.base_url, args.market_type, symbol),
                        args.timeout_sec,
                    )
                    st["ok_count"] += 1
                except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as err:
                    st["error_count"] += 1
                    st["status_counter"]["paper_fetch_error"] += 1
                    if symbol in jsonl_fp:
                        jsonl_fp[symbol].write(
                            json.dumps(
                                {
                                    "ts_ms": poll_ts_ms,
                                    "ok": False,
                                    "error": str(err),
                                    "source": "paper",
                                },
                                ensure_ascii=False,
                            )
                            + "\n"
                        )
                        jsonl_fp[symbol].flush()
                    continue

                snapshot = latest.get((symbol, args.market_type), {})
                if isinstance(snapshot, dict):
                    st["last_snapshot_ts_ms"] = snapshot.get("timestamp_ms")

                summary = payload.get("summary") or {}
                current = payload.get("current") or {}
                live_execution = payload.get("live_execution") or {}
                live_summary = live_execution.get("summary") or {}
                shadow_eval = live_summary.get("shadow_eval") or {}
                gated = live_execution.get("gated") or {}
                decisions = live_execution.get("decisions") or []
                status = (
                    shadow_eval.get("status")
                    or (live_execution.get("parity_check") or {}).get("status")
                    or payload.get("status")
                    or "unknown"
                )
                suggested_action = str(current.get("suggested_action") or "UNKNOWN")
                raw_signal_count = int(
                    shadow_eval.get("raw_signal_count")
                    or gated.get("raw_signal_count")
                    or safe_len(payload.get("signal_decisions"))
                )
                decision_pool_count = int(
                    shadow_eval.get("decision_pool_count")
                    or gated.get("decision_pool_count")
                    or raw_signal_count
                )
                fresh_signal_count = int(
                    shadow_eval.get("fresh_signal_count")
                    or gated.get("fresh_signal_count")
                    or 0
                )
                candidate_count = int(
                    shadow_eval.get("candidate_count")
                    or gated.get("candidate_count")
                    or safe_len(decisions)
                )
                parity_ready_count = int(
                    shadow_eval.get("state_selected_count")
                    or gated.get("selected_count")
                    or 0
                )
                state_selected_count = int(
                    shadow_eval.get("state_selected_count")
                    or gated.get("selected_count")
                    or 0
                )
                target_ready = bool(
                    shadow_eval.get("target_ready")
                    if "target_ready" in shadow_eval
                    else gated.get("target_ready")
                )
                state_skipped_count = int(
                    shadow_eval.get("state_skipped_count")
                    or gated.get("state_skipped_count")
                    or 0
                )
                target_missing = bool(
                    shadow_eval.get("target_missing")
                    if "target_missing" in shadow_eval
                    else gated.get("target_missing")
                )
                no_candidate_reason = str(
                    shadow_eval.get("no_candidate_reason")
                    or gated.get("no_candidate_reason")
                    or ""
                ).strip()
                candidate_source = str(shadow_eval.get("candidate_source") or "").strip()
                current_entry_available = bool(shadow_eval.get("current_entry_available"))

                st["last_status"] = status
                st["last_shadow_eval"] = shadow_eval
                st["last_current"] = current
                st["status_counter"][str(status)] += 1
                st["suggested_action_counter"][suggested_action] += 1
                if no_candidate_reason:
                    st["no_candidate_reason_counter"][no_candidate_reason] += 1
                if candidate_source:
                    st["candidate_source_counter"][candidate_source] += 1
                st["trade_count_series"].append(int(summary.get("trade_count") or 0))
                st["win_rate_series"].append(float(summary.get("win_rate_pct") or 0.0))
                st["net_pnl_series"].append(float(summary.get("net_pnl_cents") or 0.0))
                st["raw_signal_series"].append(raw_signal_count)
                st["decision_pool_series"].append(decision_pool_count)
                st["fresh_signal_series"].append(fresh_signal_count)
                st["candidate_series"].append(candidate_count)
                st["parity_ready_series"].append(parity_ready_count)
                st["state_selected_series"].append(state_selected_count)
                st["target_ready_series"].append(state_selected_count if target_ready else 0)
                st["state_skipped_series"].append(state_skipped_count)
                if target_missing:
                    st["target_missing_count"] += 1

                for row in gated.get("skipped_decisions") or []:
                    reason = str(row.get("reason") or "unknown")
                    st["skip_reason_counter"][reason] += 1

                gap_rows = decision_gap_rows(decisions if isinstance(decisions, list) else [], snapshot)
                st["gap_rows_seen"] += len(gap_rows)
                for row in gap_rows:
                    abs_gap = float(row["abs_gap_cents"])
                    adverse_gap = float(row["adverse_gap_cents"])
                    st["gap_abs_values"].append(abs_gap)
                    st["gap_adverse_values"].append(adverse_gap)
                    if adverse_gap > 0:
                        st["gap_positive_values"].append(adverse_gap)

                if symbol in jsonl_fp:
                    jsonl_fp[symbol].write(
                        json.dumps(
                            {
                                "ts_ms": poll_ts_ms,
                                "ok": True,
                                "symbol": symbol,
                                "status": status,
                                "suggested_action": suggested_action,
                                "trade_count": summary.get("trade_count"),
                                "win_rate_pct": summary.get("win_rate_pct"),
                                "net_pnl_cents": summary.get("net_pnl_cents"),
                                "raw_signal_count": raw_signal_count,
                                "decision_pool_count": decision_pool_count,
                                "fresh_signal_count": fresh_signal_count,
                                "candidate_count": candidate_count,
                                "parity_ready_count": parity_ready_count,
                                "state_selected_count": state_selected_count,
                                "target_ready": target_ready,
                                "state_skipped_count": state_skipped_count,
                                "target_missing": target_missing,
                                "no_candidate_reason": no_candidate_reason or None,
                                "candidate_source": candidate_source or None,
                                "current_entry_available": current_entry_available,
                                "current": current,
                                "gap_rows": gap_rows[:8],
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                    jsonl_fp[symbol].flush()

            time.sleep(args.poll_sec)
    finally:
        for fp in jsonl_fp.values():
            fp.close()

    result: dict[str, Any] = {
        "base_url": args.base_url,
        "market_type": args.market_type,
        "duration_sec": args.duration_sec,
        "poll_sec": args.poll_sec,
        "symbols": {},
    }

    for symbol, st in symbol_state.items():
        candidate_total = sum(st["candidate_series"])
        decision_pool_total = sum(st["decision_pool_series"])
        fresh_signal_total = sum(st["fresh_signal_series"])
        parity_ready_total = sum(st["parity_ready_series"])
        state_selected_total = sum(st["state_selected_series"])
        target_ready_total = sum(st["target_ready_series"])
        raw_signal_total = sum(st["raw_signal_series"])
        result["symbols"][symbol] = {
            "poll_count": st["poll_count"],
            "ok_count": st["ok_count"],
            "error_count": st["error_count"],
            "last_status": st["last_status"],
            "last_current": st["last_current"],
            "last_shadow_eval": st["last_shadow_eval"],
            "last_snapshot_ts_ms": st["last_snapshot_ts_ms"],
            "paper_summary": {
                "trade_count_avg": avg(st["trade_count_series"]),
                "trade_count_last": st["trade_count_series"][-1] if st["trade_count_series"] else None,
                "win_rate_pct_avg": avg(st["win_rate_series"]),
                "net_pnl_cents_avg": avg(st["net_pnl_series"]),
                "net_pnl_cents_last": st["net_pnl_series"][-1] if st["net_pnl_series"] else None,
            },
            "shadow_coverage": {
                "raw_signal_sum": raw_signal_total,
                "decision_pool_sum": decision_pool_total,
                "fresh_signal_sum": fresh_signal_total,
                "candidate_sum": candidate_total,
                "parity_ready_sum": parity_ready_total,
                "state_selected_sum": state_selected_total,
                "target_ready_sum": target_ready_total,
                "candidate_vs_signal": round(candidate_total / raw_signal_total, 4) if raw_signal_total else None,
                "candidate_vs_fresh_signal": round(candidate_total / fresh_signal_total, 4) if fresh_signal_total else None,
                "parity_vs_candidate": round(parity_ready_total / candidate_total, 4) if candidate_total else None,
                "state_vs_parity": round(state_selected_total / parity_ready_total, 4) if parity_ready_total else None,
                "target_vs_state": round(target_ready_total / state_selected_total, 4) if state_selected_total else None,
                "state_skipped_sum": sum(st["state_skipped_series"]),
                "target_missing_count": st["target_missing_count"],
            },
            "approx_price_gap": {
                "sample_count": st["gap_rows_seen"],
                "abs_gap_avg_cents": avg(st["gap_abs_values"]),
                "abs_gap_p50_cents": percentile(st["gap_abs_values"], 0.50),
                "abs_gap_p95_cents": percentile(st["gap_abs_values"], 0.95),
                "adverse_gap_avg_cents": avg(st["gap_adverse_values"]),
                "adverse_gap_p50_cents": percentile(st["gap_adverse_values"], 0.50),
                "adverse_gap_p95_cents": percentile(st["gap_adverse_values"], 0.95),
                "positive_adverse_avg_cents": avg(st["gap_positive_values"]),
            },
            "status_counter": counter_to_dict(st["status_counter"]),
            "suggested_action_counter": counter_to_dict(st["suggested_action_counter"]),
            "no_candidate_reason_counter": counter_to_dict(st["no_candidate_reason_counter"]),
            "candidate_source_counter": counter_to_dict(st["candidate_source_counter"]),
            "skip_reason_counter": counter_to_dict(st["skip_reason_counter"]),
        }

    if args.summary_out:
        out_path = pathlib.Path(args.summary_out).expanduser()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
