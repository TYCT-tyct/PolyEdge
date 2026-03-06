#!/usr/bin/env python3
from __future__ import annotations

import argparse
import collections
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


def fetch_json(url: str, timeout: float) -> Any:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.load(resp)


def market_url(base_url: str, market_type: str, symbol: str) -> str:
    params = urllib.parse.urlencode(
        {
            "market_type": market_type,
            "source": "live",
            "symbol": symbol,
        }
    )
    return f"{base_url.rstrip('/')}/api/strategy/paper?{params}"


def events_url(base_url: str, market_type: str, symbol: str) -> str:
    params = urllib.parse.urlencode(
        {
            "market_type": market_type,
            "symbol": symbol,
        }
    )
    return f"{base_url.rstrip('/')}/api/strategy/live/events?{params}"


def safe_len(value: Any) -> int:
    return len(value) if isinstance(value, list) else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Poll Forge live shadow snapshot and summarize decision coverage / skip reasons."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:9810")
    parser.add_argument("--market-type", default="5m")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--duration-sec", type=int, default=180)
    parser.add_argument("--poll-sec", type=float, default=5.0)
    parser.add_argument("--timeout-sec", type=float, default=15.0)
    parser.add_argument("--jsonl", default="")
    args = parser.parse_args()

    snapshot_endpoint = market_url(args.base_url, args.market_type, args.symbol)
    events_endpoint = events_url(args.base_url, args.market_type, args.symbol)
    deadline = time.time() + max(args.duration_sec, 1)

    poll_count = 0
    ok_count = 0
    error_count = 0
    last_status = None
    last_mode = None
    last_runtime_control = None
    last_event_seq = None
    signal_decision_count_series: list[int] = []
    candidate_count_series: list[int] = []
    parity_ready_count_series: list[int] = []
    state_selected_count_series: list[int] = []
    submitted_count_series: list[int] = []
    skipped_count_series: list[int] = []
    target_ready_count_series: list[int] = []
    paper_trade_count_series: list[int] = []
    live_fill_count_series: list[int] = []
    no_live_target_count = 0
    skip_reason_counter: collections.Counter[str] = collections.Counter()
    submit_reason_counter: collections.Counter[str] = collections.Counter()
    event_type_counter: collections.Counter[str] = collections.Counter()
    status_counter: collections.Counter[str] = collections.Counter()
    mode_counter: collections.Counter[str] = collections.Counter()
    accepted_submit_count = 0
    rejected_submit_count = 0
    recent_events: list[dict[str, Any]] = []

    jsonl_fp = open(args.jsonl, "a", encoding="utf-8") if args.jsonl else None
    try:
        while time.time() < deadline:
            poll_ts = int(time.time() * 1000)
            poll_count += 1
            try:
                payload = fetch_json(snapshot_endpoint, args.timeout_sec)
                events_payload = fetch_json(events_endpoint, args.timeout_sec)
                ok_count += 1
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as err:
                error_count += 1
                status_counter["fetch_error"] += 1
                record = {
                    "ts_ms": poll_ts,
                    "ok": False,
                    "error": str(err),
                }
                if jsonl_fp is not None:
                    jsonl_fp.write(json.dumps(record, ensure_ascii=False) + "\n")
                    jsonl_fp.flush()
                time.sleep(args.poll_sec)
                continue

            live_execution = payload.get("live_execution") or {}
            summary = live_execution.get("summary") or {}
            shadow_eval = summary.get("shadow_eval") or {}
            parity_check = live_execution.get("parity_check") or {}
            gated = live_execution.get("gated") or {}
            events = events_payload.get("events") or []

            status = (
                shadow_eval.get("status")
                or parity_check.get("status")
                or payload.get("status")
                or "unknown"
            )
            mode = summary.get("mode") or "unknown"
            runtime_control = payload.get("runtime_control") or {}
            last_runtime_control = runtime_control
            last_status = status
            last_mode = mode
            status_counter[str(status)] += 1
            mode_counter[str(mode)] += 1

            signal_decision_count = int(
                shadow_eval.get("raw_signal_count")
                or gated.get("raw_signal_count")
                or safe_len(payload.get("signal_decisions"))
            )
            candidate_count = int(
                shadow_eval.get("candidate_count")
                or gated.get("candidate_count")
                or (((parity_check.get("paper") or {}).get("decision_count")) or 0)
            )
            state_skipped_count = int(
                shadow_eval.get("state_skipped_count")
                or gated.get("state_skipped_count")
                or 0
            )
            gated_selected_count = int(
                shadow_eval.get("state_selected_count")
                or gated.get("selected_count")
                or 0
            )
            parity_ready_count = gated_selected_count
            submitted_count = int(gated.get("submitted_count") or 0)
            skipped_count = int(gated.get("skipped_count") or 0)
            target_ready = bool(
                shadow_eval.get("target_ready")
                if "target_ready" in shadow_eval
                else gated.get("target_ready")
            )
            no_live_target = bool(
                shadow_eval.get("target_missing")
                if "target_missing" in shadow_eval
                else (((parity_check.get("live") or {}).get("no_live_market_target")))
            )
            paper_trade_count = safe_len(payload.get("trades"))
            fill_count = int((((summary.get("fills") or {}).get("fill_decision_count")) or 0))

            signal_decision_count_series.append(int(signal_decision_count))
            candidate_count_series.append(int(candidate_count))
            parity_ready_count_series.append(parity_ready_count)
            state_selected_count_series.append(gated_selected_count)
            submitted_count_series.append(submitted_count)
            skipped_count_series.append(skipped_count)
            target_ready_count_series.append(gated_selected_count if target_ready else 0)
            paper_trade_count_series.append(paper_trade_count)
            live_fill_count_series.append(fill_count)
            if no_live_target:
                no_live_target_count += 1

            for row in gated.get("skipped_decisions") or []:
                reason = row.get("reason") or "unknown"
                skip_reason_counter[str(reason)] += 1

            newest_seq = last_event_seq
            if last_event_seq is None:
                seqs = [event.get("event_seq") for event in events if isinstance(event.get("event_seq"), int)]
                last_event_seq = max(seqs) if seqs else 0
                newest_seq = last_event_seq
            for event in events:
                seq = event.get("event_seq")
                if isinstance(seq, int):
                    newest_seq = max(seq, newest_seq or seq)
                if last_event_seq is not None and isinstance(seq, int) and seq <= last_event_seq:
                    continue
                reason = event.get("reason")
                event_type = event.get("event_type")
                accepted = bool(event.get("accepted"))
                if reason:
                    submit_reason_counter[str(reason)] += 1
                if event_type:
                    event_type_counter[str(event_type)] += 1
                if event_type == "submit":
                    if accepted:
                        accepted_submit_count += 1
                    else:
                        rejected_submit_count += 1
                recent_events.append(
                    {
                        "ts_ms": event.get("ts_ms"),
                        "event_seq": seq,
                        "event_type": event_type,
                        "reason": reason,
                        "accepted": accepted,
                        "action": event.get("action"),
                        "side": event.get("side"),
                        "reject_reason": event.get("reject_reason"),
                    }
                )
            last_event_seq = newest_seq
            recent_events = recent_events[-20:]

            record = {
                "ts_ms": poll_ts,
                "ok": True,
                "status": status,
                "mode": mode,
                "signal_decision_count": signal_decision_count,
                "candidate_count": candidate_count,
                "gated_selected_count": gated_selected_count,
                "parity_ready_count": parity_ready_count,
                "state_skipped_count": state_skipped_count,
                "submitted_count": submitted_count,
                "skipped_count": skipped_count,
                "paper_trade_count": paper_trade_count,
                "fill_decision_count": fill_count,
                "no_live_market_target": no_live_target,
                "target_ready": target_ready,
                "runtime_control": runtime_control,
            }
            if jsonl_fp is not None:
                jsonl_fp.write(json.dumps(record, ensure_ascii=False) + "\n")
                jsonl_fp.flush()
            time.sleep(args.poll_sec)
    finally:
        if jsonl_fp is not None:
            jsonl_fp.close()

    def avg(values: list[int]) -> float | None:
        return round(sum(values) / len(values), 3) if values else None

    def ratio(num: int, den: int) -> float | None:
        if den <= 0:
            return None
        return round(num / den, 4)

    signal_decision_total = sum(signal_decision_count_series)
    candidate_total = sum(candidate_count_series)
    parity_ready_total = sum(parity_ready_count_series)
    state_selected_total = sum(state_selected_count_series)
    submitted_total = sum(submitted_count_series)
    skipped_total = sum(skipped_count_series)
    target_ready_total = sum(target_ready_count_series)

    summary = {
        "base_url": args.base_url,
        "symbol": args.symbol,
        "market_type": args.market_type,
        "duration_sec": args.duration_sec,
        "poll_sec": args.poll_sec,
        "poll_count": poll_count,
        "ok_count": ok_count,
        "error_count": error_count,
        "last_status": last_status,
        "last_mode": last_mode,
        "last_runtime_control": last_runtime_control,
        "signal_decision_count": {
            "avg": avg(signal_decision_count_series),
            "sum": signal_decision_total,
        },
        "candidate_count": {
            "avg": avg(candidate_count_series),
            "sum": candidate_total,
        },
        "parity_ready_count": {
            "avg": avg(parity_ready_count_series),
            "sum": parity_ready_total,
            "coverage_vs_candidate": ratio(parity_ready_total, candidate_total),
            "coverage_vs_signal": ratio(parity_ready_total, signal_decision_total),
        },
        "gated_selected_count": {
            "avg": avg(state_selected_count_series),
            "sum": state_selected_total,
            "coverage_vs_candidate": ratio(state_selected_total, candidate_total),
            "coverage_vs_parity_ready": ratio(state_selected_total, parity_ready_total),
            "coverage_vs_signal": ratio(state_selected_total, signal_decision_total),
        },
        "target_ready_count": {
            "avg": avg(target_ready_count_series),
            "sum": target_ready_total,
            "coverage_vs_gated": ratio(target_ready_total, state_selected_total),
        },
        "submitted_count": {
            "avg": avg(submitted_count_series),
            "sum": submitted_total,
            "coverage_vs_signal": ratio(submitted_total, signal_decision_total),
            "coverage_vs_candidate": ratio(submitted_total, candidate_total),
            "coverage_vs_selected": ratio(submitted_total, state_selected_total),
        },
        "skipped_count": {
            "avg": avg(skipped_count_series),
            "sum": skipped_total,
        },
        "paper_trade_count": {
            "avg": avg(paper_trade_count_series),
            "sum": sum(paper_trade_count_series),
        },
        "fill_decision_count": {
            "avg": avg(live_fill_count_series),
            "sum": sum(live_fill_count_series),
        },
        "no_live_market_target_polls": no_live_target_count,
        "status_counter": dict(status_counter),
        "mode_counter": dict(mode_counter),
        "skip_reason_counter": dict(skip_reason_counter.most_common()),
        "event_type_counter": dict(event_type_counter),
        "submit_reason_counter": dict(submit_reason_counter.most_common()),
        "submit_acceptance": {
            "accepted_submit_count": accepted_submit_count,
            "rejected_submit_count": rejected_submit_count,
        },
        "recent_events": recent_events,
    }
    json.dump(summary, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
