import json
import pathlib
import time
import urllib.request


URL = "http://127.0.0.1:9810/api/strategy/paper?market_type=5m&source=live&symbol=BTCUSDT"
OUT_DIR = pathlib.Path(r"C:\Users\Shini\AppData\Local\Temp\paper_live_stage123_watch")
OUT_DIR.mkdir(parents=True, exist_ok=True)
WATCH_PATH = OUT_DIR / "watch.jsonl"
SUMMARY_PATH = OUT_DIR / "summary.json"


def main() -> None:
    start = time.time()
    deadline = start + 900
    samples = 0
    errors = 0
    enter_rows = []
    action_counts = {}
    reason_counts = {}

    with WATCH_PATH.open("w", encoding="utf-8") as watch_file:
        while time.time() < deadline:
            poll_ts_ms = int(time.time() * 1000)
            row = {"poll_ts_ms": poll_ts_ms}
            try:
                with urllib.request.urlopen(URL, timeout=4) as response:
                    payload = json.load(response)
                current = payload.get("current") or {}
                shadow = ((payload.get("live_execution") or {}).get("summary") or {}).get("shadow_eval") or {}
                decision = current.get("active_signal") or current.get("live_entry_decision") or {}
                action = current.get("suggested_action") or "UNKNOWN"
                row.update(
                    {
                        "ok": True,
                        "action": action,
                        "confirmed": current.get("confirmed"),
                        "score": current.get("score"),
                        "entry_threshold": current.get("entry_threshold"),
                        "current_ts_ms": current.get("timestamp_ms"),
                        "live_entry_available": current.get("live_entry_available"),
                        "active_signal_ts_ms": decision.get("ts_ms"),
                        "trigger_ts_ms": shadow.get("trigger_ts_ms"),
                        "signal_price_cents": current.get("signal_price_cents"),
                        "paper_entry_exec_price_cents": current.get("paper_entry_exec_price_cents"),
                        "paper_entry_fee_cents": current.get("paper_entry_fee_cents"),
                        "paper_entry_slippage_cents": current.get("paper_entry_slippage_cents"),
                        "paper_entry_impact_cents": current.get("paper_entry_impact_cents"),
                        "candidate_count": shadow.get("candidate_count"),
                        "candidate_source": shadow.get("candidate_source"),
                        "no_candidate_reason": shadow.get("no_candidate_reason"),
                        "current_entry_available": shadow.get("current_entry_available"),
                        "current_suggested_action": shadow.get("current_suggested_action"),
                        "current_score": shadow.get("current_score"),
                        "current_entry_threshold": shadow.get("current_entry_threshold"),
                    }
                )
                samples += 1
                action_counts[action] = action_counts.get(action, 0) + 1
                reason = row.get("no_candidate_reason")
                if reason is not None:
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1
                if action in ("ENTER_UP", "ENTER_DOWN"):
                    current_ts_ms = row.get("current_ts_ms")
                    decision_ts_ms = row.get("active_signal_ts_ms")
                    row["delta_current_vs_decision_ms"] = (
                        None
                        if current_ts_ms is None or decision_ts_ms is None
                        else int(current_ts_ms) - int(decision_ts_ms)
                    )
                    row["delta_poll_vs_current_ms"] = (
                        None if current_ts_ms is None else poll_ts_ms - int(current_ts_ms)
                    )
                    trigger_ts_ms = row.get("trigger_ts_ms")
                    row["delta_trigger_vs_current_ms"] = (
                        None
                        if trigger_ts_ms is None or current_ts_ms is None
                        else int(trigger_ts_ms) - int(current_ts_ms)
                    )
                    row["mismatch"] = not bool(row.get("live_entry_available")) or int(row.get("candidate_count") or 0) <= 0
                    enter_rows.append(dict(row))
            except Exception as exc:  # noqa: BLE001
                errors += 1
                row.update({"ok": False, "error": str(exc)})

            watch_file.write(json.dumps(row, ensure_ascii=False) + "\n")
            watch_file.flush()
            time.sleep(2)

    summary = {
        "status": "done",
        "samples": samples,
        "errors": errors,
        "action_counts": action_counts,
        "reason_counts": reason_counts,
        "enter_count": len(enter_rows),
        "mismatch_count": sum(1 for row in enter_rows if row.get("mismatch")),
        "first_enter": enter_rows[0] if enter_rows else None,
        "last_enter": enter_rows[-1] if enter_rows else None,
    }
    SUMMARY_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
