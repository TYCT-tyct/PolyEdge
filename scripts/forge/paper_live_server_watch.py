import json
import pathlib
import time
import traceback
import urllib.request
from datetime import datetime

PAPER_URL = "http://127.0.0.1:9830/api/strategy/paper?market_type=5m&source=live&symbol=BTCUSDT"
LATEST_URL = "http://127.0.0.1:9830/api/latest/all"


def fetch_json(url: str):
    with urllib.request.urlopen(url, timeout=3) as response:
        return json.load(response)


def latest_btc_5m_snapshot(rows):
    if not isinstance(rows, list):
        return {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        timeframe = str(row.get("market_type") or row.get("timeframe") or "").strip().lower()
        if symbol == "BTCUSDT" and timeframe in ("5m", ""):
            return row
    return {}


def book_price_cents(side, snapshot):
    key = "ask_yes" if str(side).upper() == "UP" else "ask_no"
    value = snapshot.get(key)
    if value is None:
        return None
    try:
        return round(float(value) * 100.0, 4)
    except Exception:
        return None


def main():
    out_dir = (
        pathlib.Path.home()
        / "polyedge_runs"
        / ("paper_live_server_watch_" + datetime.utcnow().strftime("%Y%m%d_%H%M%S"))
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    watch_path = out_dir / "watch.jsonl"
    summary_path = out_dir / "summary.json"
    status_path = out_dir / "status.json"

    start = time.time()
    deadline = start + 3 * 3600
    samples = 0
    errors = 0
    hold_count = 0
    enter_hits = []

    with watch_path.open("w", encoding="utf-8") as watch_file:
        while time.time() < deadline:
            poll_ts_ms = int(time.time() * 1000)
            row = {"poll_ts_ms": poll_ts_ms}
            try:
                paper = fetch_json(PAPER_URL)
                latest = fetch_json(LATEST_URL)
                current = (paper if isinstance(paper, dict) else {}).get("current") or {}
                summary = (paper if isinstance(paper, dict) else {}).get("summary") or {}
                live_exec = (
                    (paper if isinstance(paper, dict) else {}).get("live_execution") or {}
                ).get("summary") or {}
                shadow = live_exec.get("shadow_eval") or {}
                decision = current.get("live_entry_decision") or {}
                snapshot = latest_btc_5m_snapshot(latest)
                action = current.get("suggested_action") or "UNKNOWN"
                trade_count = summary.get("trade_count")
                row.update(
                    {
                        "ok": True,
                        "action": action,
                        "trade_count": trade_count,
                        "current_ts_ms": current.get("timestamp_ms"),
                        "runtime_trigger_ts_ms": shadow.get("trigger_ts_ms"),
                        "live_entry_available": current.get("live_entry_available"),
                        "decision": decision,
                        "candidate_count": shadow.get("candidate_count"),
                        "candidate_source": shadow.get("candidate_source"),
                        "no_candidate_reason": shadow.get("no_candidate_reason"),
                        "signal_price_cents": current.get("signal_price_cents"),
                        "paper_entry_exec_price_cents": current.get(
                            "paper_entry_exec_price_cents"
                        ),
                        "paper_entry_fee_cents": current.get("paper_entry_fee_cents"),
                        "paper_entry_slippage_cents": current.get(
                            "paper_entry_slippage_cents"
                        ),
                        "paper_entry_impact_cents": current.get("paper_entry_impact_cents"),
                        "ask_yes": snapshot.get("ask_yes"),
                        "ask_no": snapshot.get("ask_no"),
                        "bid_yes": snapshot.get("bid_yes"),
                        "bid_no": snapshot.get("bid_no"),
                        "snapshot_ts_ms": snapshot.get("ts_ms"),
                    }
                )
                if action == "HOLD":
                    hold_count += 1
                if action in ("ENTER_UP", "ENTER_DOWN"):
                    submit_shadow = book_price_cents(decision.get("side"), snapshot)
                    signal_price = current.get("signal_price_cents")
                    paper_exec = current.get("paper_entry_exec_price_cents")
                    try:
                        signal_price = (
                            None if signal_price is None else round(float(signal_price), 4)
                        )
                    except Exception:
                        signal_price = None
                    try:
                        paper_exec = None if paper_exec is None else round(float(paper_exec), 4)
                    except Exception:
                        paper_exec = None
                    drift_signal = (
                        None
                        if submit_shadow is None or signal_price is None
                        else round(submit_shadow - signal_price, 4)
                    )
                    drift_exec = (
                        None
                        if submit_shadow is None or paper_exec is None
                        else round(submit_shadow - paper_exec, 4)
                    )
                    row.update(
                        {
                            "shadow_submit_price_cents": submit_shadow,
                            "entry_price_drift_vs_signal_cents": drift_signal,
                            "entry_price_drift_vs_exec_cents": drift_exec,
                            "delta_current_vs_decision_ms": None
                            if current.get("timestamp_ms") is None
                            or decision.get("ts_ms") is None
                            else int(current.get("timestamp_ms")) - int(decision.get("ts_ms")),
                            "delta_trigger_vs_current_ms": None
                            if shadow.get("trigger_ts_ms") is None
                            or current.get("timestamp_ms") is None
                            else int(shadow.get("trigger_ts_ms"))
                            - int(current.get("timestamp_ms")),
                            "delta_poll_vs_current_ms": None
                            if current.get("timestamp_ms") is None
                            else poll_ts_ms - int(current.get("timestamp_ms")),
                        }
                    )
                    enter_hits.append(dict(row))
                samples += 1
                status = {
                    "samples": samples,
                    "errors": errors,
                    "hold_count": hold_count,
                    "enter_hits": len(enter_hits),
                    "last_action": action,
                    "last_trade_count": trade_count,
                    "out_dir": str(out_dir),
                }
                status_path.write_text(
                    json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8"
                )
            except Exception as exc:
                errors += 1
                row.update(
                    {
                        "ok": False,
                        "error": str(exc),
                        "trace": traceback.format_exc(limit=1),
                    }
                )

            watch_file.write(json.dumps(row, ensure_ascii=False) + "\n")
            watch_file.flush()
            time.sleep(0.25)

    summary = {
        "status": "done",
        "samples": samples,
        "errors": errors,
        "hold_count": hold_count,
        "enter_hit_count": len(enter_hits),
        "first_enter": enter_hits[0] if enter_hits else None,
        "last_enter": enter_hits[-1] if enter_hits else None,
        "out_dir": str(out_dir),
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(out_dir))


if __name__ == "__main__":
    main()
