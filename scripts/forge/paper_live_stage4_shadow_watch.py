import json
import pathlib
import time
import urllib.request


PAPER_URL = "http://127.0.0.1:9810/api/strategy/paper?market_type=5m&source=live&symbol=BTCUSDT"
LATEST_URL = "http://127.0.0.1:9810/api/latest/all"
OUT_DIR = pathlib.Path(r"C:\Users\Shini\AppData\Local\Temp\paper_live_stage4_shadow_watch")
OUT_DIR.mkdir(parents=True, exist_ok=True)
WATCH_PATH = OUT_DIR / "watch.jsonl"
SUMMARY_PATH = OUT_DIR / "summary.json"


def fetch_json(url: str) -> object:
    with urllib.request.urlopen(url, timeout=4) as response:
        return json.load(response)


def latest_btc_5m_snapshot(rows: object) -> dict:
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


def book_price_cents(side: str, snapshot: dict) -> float | None:
    side = side.upper()
    if side == "UP":
        value = snapshot.get("ask_yes")
    else:
        value = snapshot.get("ask_no")
    if value is None:
        return None
    try:
        return round(float(value) * 100.0, 4)
    except (TypeError, ValueError):
        return None


def main() -> None:
    deadline = time.time() + 1800
    samples = 0
    errors = 0
    hit = None

    with WATCH_PATH.open("w", encoding="utf-8") as watch_file:
        while time.time() < deadline and hit is None:
            row = {"poll_ts_ms": int(time.time() * 1000)}
            try:
                paper = fetch_json(PAPER_URL)
                latest = fetch_json(LATEST_URL)
                current = (paper if isinstance(paper, dict) else {}).get("current") or {}
                shadow = ((paper if isinstance(paper, dict) else {}).get("live_execution") or {}).get("summary", {}).get("shadow_eval") or {}
                decision = current.get("live_entry_decision") or {}
                snapshot = latest_btc_5m_snapshot(latest)

                row.update(
                    {
                        "ok": True,
                        "action": current.get("suggested_action"),
                        "current_ts_ms": current.get("timestamp_ms"),
                        "live_entry_available": current.get("live_entry_available"),
                        "trigger_ts_ms": shadow.get("trigger_ts_ms"),
                        "decision": decision,
                        "snapshot_ts_ms": snapshot.get("ts_ms"),
                        "ask_yes": snapshot.get("ask_yes"),
                        "ask_no": snapshot.get("ask_no"),
                        "bid_yes": snapshot.get("bid_yes"),
                        "bid_no": snapshot.get("bid_no"),
                    }
                )

                if current.get("suggested_action") in ("ENTER_UP", "ENTER_DOWN") and decision:
                    side = str(decision.get("side") or "")
                    signal_price = decision.get("signal_price_cents", decision.get("price_cents"))
                    paper_exec_price = decision.get("paper_entry_exec_price_cents")
                    paper_fee_cents = decision.get("paper_entry_fee_cents")
                    paper_slippage_cents = decision.get("paper_entry_slippage_cents")
                    paper_impact_cents = decision.get("paper_entry_impact_cents")
                    submit_shadow = book_price_cents(side, snapshot)
                    try:
                        signal_price = round(float(signal_price), 4)
                    except (TypeError, ValueError):
                        signal_price = None
                    try:
                        paper_exec_price = round(float(paper_exec_price), 4)
                    except (TypeError, ValueError):
                        paper_exec_price = None
                    drift_vs_signal = None
                    drift_vs_exec = None
                    if signal_price is not None and submit_shadow is not None:
                        drift_vs_signal = round(submit_shadow - signal_price, 4)
                    if paper_exec_price is not None and submit_shadow is not None:
                        drift_vs_exec = round(submit_shadow - paper_exec_price, 4)
                    row.update(
                        {
                            "paper_signal_price_cents": signal_price,
                            "paper_entry_exec_price_cents": paper_exec_price,
                            "paper_entry_fee_cents": paper_fee_cents,
                            "paper_entry_slippage_cents": paper_slippage_cents,
                            "paper_entry_impact_cents": paper_impact_cents,
                            "shadow_submit_price_cents": submit_shadow,
                            "entry_price_drift_vs_signal_cents": drift_vs_signal,
                            "entry_price_drift_vs_exec_cents": drift_vs_exec,
                            "delta_current_vs_decision_ms": None
                            if current.get("timestamp_ms") is None or decision.get("ts_ms") is None
                            else int(current.get("timestamp_ms")) - int(decision.get("ts_ms")),
                            "delta_trigger_vs_current_ms": None
                            if current.get("timestamp_ms") is None or shadow.get("trigger_ts_ms") is None
                            else int(shadow.get("trigger_ts_ms")) - int(current.get("timestamp_ms")),
                            "delta_snapshot_vs_current_ms": None
                            if current.get("timestamp_ms") is None or snapshot.get("ts_ms") is None
                            else int(snapshot.get("ts_ms")) - int(current.get("timestamp_ms")),
                        }
                    )
                    hit = dict(row)
                samples += 1
            except Exception as exc:  # noqa: BLE001
                errors += 1
                row.update({"ok": False, "error": str(exc)})

            watch_file.write(json.dumps(row, ensure_ascii=False) + "\n")
            watch_file.flush()
            time.sleep(2)

    summary = {
        "status": "enter_detected" if hit else "timeout",
        "samples": samples,
        "errors": errors,
        "first_entry_shadow": hit,
    }
    SUMMARY_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
