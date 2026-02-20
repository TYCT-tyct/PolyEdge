#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import math
import time
from dataclasses import dataclass
from collections import deque
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import websockets


def percentile(values: List[float], q: float) -> Optional[float]:
    if not values:
        return None
    arr = sorted(values)
    if len(arr) == 1:
        return float(arr[0])
    idx = (len(arr) - 1) * q
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return float(arr[lo])
    w = idx - lo
    return float(arr[lo] * (1.0 - w) + arr[hi] * w)


def to_bps(delta: float, base: float) -> Optional[float]:
    if not math.isfinite(delta) or not math.isfinite(base) or base == 0:
        return None
    return (delta / base) * 10_000.0


@dataclass
class PricePoint:
    ts_ms: int
    px: float


class SharedState:
    def __init__(self, symbols: List[str]) -> None:
        self.symbols = symbols
        self.binance: Dict[str, PricePoint] = {}
        self.pm: Dict[str, PricePoint] = {}
        self.last_trigger_ms: Dict[str, int] = {s: 0 for s in symbols}
        self.events: List[dict] = []
        self.lock = asyncio.Lock()
        self.trigger_count: int = 0
        self.trigger_skip_no_pm: int = 0


def normalize_symbol(s: str) -> str:
    s = s.strip().upper()
    return s if s.endswith("USDT") else f"{s}USDT"


def symbol_to_pm(s: str) -> str:
    return normalize_symbol(s).replace("USDT", "").lower()


async def run_binance_feed(state: SharedState, end_ts: float) -> None:
    streams = "/".join([f"{normalize_symbol(s).lower()}@bookTicker" for s in state.symbols])
    url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    while time.time() < end_ts:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**22) as ws:
                while time.time() < end_ts:
                    raw = await asyncio.wait_for(ws.recv(), timeout=3.0)
                    recv_ms = int(time.time() * 1000)
                    msg = json.loads(raw)
                    payload = msg.get("data", msg)
                    symbol = normalize_symbol(str(payload.get("s", "")))
                    bid = float(payload.get("b", 0.0))
                    ask = float(payload.get("a", 0.0))
                    if symbol not in state.symbols or bid <= 0 or ask <= 0:
                        continue
                    px = (bid + ask) * 0.5
                    async with state.lock:
                        prev = state.binance.get(symbol)
                        state.binance[symbol] = PricePoint(ts_ms=recv_ms, px=px)
                    if prev is None:
                        continue
                    dt_ms = max(1, recv_ms - prev.ts_ms)
                    ret = (px - prev.px) / prev.px
                    velocity_bps_per_sec = (ret * 10_000.0) / (dt_ms / 1000.0)
                    if abs(velocity_bps_per_sec) > 5.0:
                        await maybe_trigger_event(
                            state,
                            symbol,
                            recv_ms,
                            velocity_bps_per_sec,
                            binance_change_bps=ret * 10_000.0,
                        )
        except Exception:
            raise  # Linus: Fail loudly and explicitly
async def run_pm_feed(state: SharedState, end_ts: float) -> None:
    url = "wss://ws-live-data.polymarket.com"
    subscriptions = [
        {
            "topic": "crypto_prices",
            "type": "update",
            "filters": json.dumps({"symbol": symbol_to_pm(s)}),
        }
        for s in state.symbols
    ]
    sub_payload = {"action": "subscribe", "subscriptions": subscriptions}
    while time.time() < end_ts:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**22) as ws:
                await ws.send(json.dumps(sub_payload))
                while time.time() < end_ts:
                    raw = await asyncio.wait_for(ws.recv(), timeout=3.0)
                    recv_ms = int(time.time() * 1000)
                    msg = json.loads(raw)
                    if msg.get("topic") != "crypto_prices" or msg.get("type") != "update":
                        continue
                    payload = msg.get("payload") or {}
                    symbol = normalize_symbol(f"{str(payload.get('symbol', '')).upper()}USDT")
                    price = payload.get("price")
                    if symbol not in state.symbols or price is None:
                        continue
                    px = float(price)
                    if not math.isfinite(px) or px <= 0:
                        continue
                    async with state.lock:
                        state.pm[symbol] = PricePoint(ts_ms=recv_ms, px=px)
        except Exception:
            raise  # Linus: Fail loudly and explicitly
def fetch_token_symbol_map(symbols: List[str]) -> Dict[Tuple[str, str], str]:
    symbols = [normalize_symbol(s) for s in symbols]
    allowed = set(symbols)
    out: Dict[Tuple[str, str], str] = {}
    endpoint = "https://gamma-api.polymarket.com/markets"
    client = requests.Session()
    for offset in (0, 1000, 2000, 3000):
        try:
            resp = client.get(
                endpoint,
                params={
                    "closed": "false",
                    "archived": "false",
                    "active": "true",
                    "limit": 1000,
                    "offset": offset,
                    "order": "volume",
                    "ascending": "false",
                },
                timeout=10,
            )
            resp.raise_for_status()
            rows = resp.json()
        except Exception:
            raise  # Linus: Fail loudly and explicitly
        if not isinstance(rows, list) or not rows:
            break
        for m in rows:
            question = str(m.get("question") or "").upper()
            slug = str(m.get("slug") or "").upper()
            text = f"{question} {slug}"
            symbol = None
            for s in allowed:
                base = s.replace("USDT", "")
                if base in text:
                    symbol = s
                    break
            if symbol is None:
                continue
            token_raw = m.get("clobTokenIds")
            if not token_raw:
                continue
            try:
                token_ids = json.loads(token_raw)
            except Exception:
                raise  # Linus: Fail loudly and explicitly
            if not isinstance(token_ids, list) or len(token_ids) < 2:
                continue
            yes = str(token_ids[0])
            no = str(token_ids[1])
            out[(yes, no)] = symbol
    return out


async def run_pm_file_feed(
    state: SharedState,
    end_ts: float,
    raw_root: str,
) -> None:
    token_map = fetch_token_symbol_map(state.symbols)
    if not token_map:
        return
    book_file = Path(raw_root) / time.strftime("%Y-%m-%d", time.gmtime()) / "book_tops.jsonl"
    # wait up to 15s for file creation
    for _ in range(150):
        if book_file.exists():
            break
        await asyncio.sleep(0.1)
    if not book_file.exists():
        return

    # Bootstrap latest PM mid by symbol from recent file tail so triggers don't wait for fresh lines.
    bootstrap_latest_pm(state, book_file, token_map)

    with book_file.open("r", encoding="utf-8") as f:
        f.seek(0, 2)
        while time.time() < end_ts:
            line = f.readline()
            if not line:
                await asyncio.sleep(0.01)
                continue
            try:
                msg = json.loads(line)
                book = msg.get("book") or {}
                yes = str(book.get("token_id_yes") or "")
                no = str(book.get("token_id_no") or "")
                symbol = token_map.get((yes, no))
                if symbol is None:
                    continue
                bid_yes = float(book.get("bid_yes"))
                ask_yes = float(book.get("ask_yes"))
                if bid_yes <= 0 or ask_yes <= 0:
                    continue
                px = (bid_yes + ask_yes) * 0.5
                recv_ns = int(book.get("recv_ts_local_ns") or 0)
                recv_ms = recv_ns // 1_000_000 if recv_ns > 0 else int(time.time() * 1000)
                async with state.lock:
                    state.pm[symbol] = PricePoint(ts_ms=recv_ms, px=px)
            except Exception:
                raise  # Linus: Fail loudly and explicitly
def bootstrap_latest_pm(state: SharedState, book_file: Path, token_map: Dict[Tuple[str, str], str]) -> None:
    latest: Dict[str, PricePoint] = {}
    try:
        with book_file.open("r", encoding="utf-8") as f:
            tail_lines = deque(f, maxlen=30_000)
        for line in tail_lines:
            try:
                msg = json.loads(line)
                book = msg.get("book") or {}
                yes = str(book.get("token_id_yes") or "")
                no = str(book.get("token_id_no") or "")
                symbol = token_map.get((yes, no))
                if symbol is None:
                    continue
                bid_yes = float(book.get("bid_yes"))
                ask_yes = float(book.get("ask_yes"))
                if bid_yes <= 0 or ask_yes <= 0:
                    continue
                px = (bid_yes + ask_yes) * 0.5
                recv_ns = int(book.get("recv_ts_local_ns") or 0)
                recv_ms = recv_ns // 1_000_000 if recv_ns > 0 else int(time.time() * 1000)
                latest[symbol] = PricePoint(ts_ms=recv_ms, px=px)
            except Exception:
                raise  # Linus: Fail loudly and explicitly
    except Exception:
        raise  # Linus: Fail loudly and explicitly
    if latest:
        state.pm.update(latest)


async def maybe_trigger_event(
    state: SharedState,
    symbol: str,
    t0_ms: int,
    velocity_bps_per_sec: float,
    binance_change_bps: float,
) -> None:
    async with state.lock:
        state.trigger_count += 1
        last = state.last_trigger_ms.get(symbol, 0)
        # Keep events independent but avoid over-trigger flooding.
        if t0_ms - last < 200:
            return
        pm0 = state.pm.get(symbol)
        if pm0 is None:
            state.trigger_skip_no_pm += 1
            return
        state.last_trigger_ms[symbol] = t0_ms
    event: dict = {
        "symbol": symbol,
        "t0_ms": t0_ms,
        "velocity_bps_per_sec": velocity_bps_per_sec,
        "binance_price_change_bps": binance_change_bps,
        "pm_book_mid_at_t0": pm0.px,
    }
    await capture_event_samples(state, event)
    async with state.lock:
        state.events.append(event)


async def read_pm_price(state: SharedState, symbol: str) -> Optional[float]:
    async with state.lock:
        point = state.pm.get(symbol)
        return None if point is None else point.px


async def capture_event_samples(state: SharedState, event: dict) -> None:
    symbol = str(event["symbol"])
    t_start = time.time()
    schedule_ms = {
        "pm_book_mid_at_t0_plus_20ms": 20,
        "pm_book_mid_at_t0_plus_50ms": 50,
        "pm_book_mid_at_t0_plus_100ms": 100,
        "pm_book_mid_at_t0_plus_200ms": 200,
        "pm_book_mid_at_t0_plus_500ms": 500,
        "pm_book_mid_at_t0_plus_1000ms": 1000,
        "pm_price_at_t1_plus_1s": 1020,
        "pm_price_at_t1_plus_3s": 3020,
        "pm_price_at_t1_plus_5s": 5020,
        "pm_price_at_t1_plus_10s": 10020,
    }
    for key, delay_ms in schedule_ms.items():
        now_elapsed_ms = int((time.time() - t_start) * 1000)
        sleep_ms = max(0, delay_ms - now_elapsed_ms)
        await asyncio.sleep(sleep_ms / 1000.0)
        event[key] = await read_pm_price(state, symbol)
    finalize_event_metrics(event)


def finalize_event_metrics(event: dict) -> None:
    t0 = float(event.get("pm_book_mid_at_t0") or 0.0)
    p20 = event.get("pm_book_mid_at_t0_plus_20ms")
    p50 = event.get("pm_book_mid_at_t0_plus_50ms")
    p100 = event.get("pm_book_mid_at_t0_plus_100ms")
    p200 = event.get("pm_book_mid_at_t0_plus_200ms")
    p500 = event.get("pm_book_mid_at_t0_plus_500ms")
    p1000 = event.get("pm_book_mid_at_t0_plus_1000ms")

    if not all(isinstance(v, (int, float)) for v in [p20, p50, p100, p200, p500, p1000]) or t0 <= 0:
        event["convergence_time_ms"] = None
        event["price_gap_at_t1_bps"] = None
        event["max_profit_time_ms"] = None
        event["max_profit_bps"] = None
        event["profit_at_3s_bps"] = None
        event["entry_price"] = p20
        return

    sign = 1.0 if float(event.get("binance_price_change_bps", 0.0)) >= 0 else -1.0
    delta_target = sign * (float(p1000) - t0)
    event["entry_price"] = float(p20)

    if abs(delta_target) < 1e-12:
        event["convergence_time_ms"] = None
    else:
        checkpoints = [(50, p50), (100, p100), (200, p200), (500, p500), (1000, p1000)]
        conv = None
        for ms, val in checkpoints:
            progress = sign * (float(val) - t0)
            if progress >= 0.9 * delta_target:
                conv = ms
                break
        event["convergence_time_ms"] = conv

    gap = sign * (float(p1000) - float(p20))
    event["price_gap_at_t1_bps"] = to_bps(gap, float(p20))

    future_points = [
        (1000, event.get("pm_price_at_t1_plus_1s")),
        (3000, event.get("pm_price_at_t1_plus_3s")),
        (5000, event.get("pm_price_at_t1_plus_5s")),
        (10000, event.get("pm_price_at_t1_plus_10s")),
    ]
    profits: List[tuple[int, float]] = []
    for ms, px in future_points:
        if isinstance(px, (int, float)):
            pnl = sign * (float(px) - float(p20))
            bps = to_bps(pnl, float(p20))
            if bps is not None:
                profits.append((ms, bps))
    if not profits:
        event["max_profit_time_ms"] = None
        event["max_profit_bps"] = None
        event["profit_at_3s_bps"] = None
        return
    best = max(profits, key=lambda x: x[1])
    event["max_profit_time_ms"] = best[0]
    event["max_profit_bps"] = best[1]
    profit_3s = next((bps for ms, bps in profits if ms == 3000), None)
    event["profit_at_3s_bps"] = profit_3s


def build_summary(events: List[dict]) -> dict:
    convergence_vals = [float(e["convergence_time_ms"]) for e in events if isinstance(e.get("convergence_time_ms"), (int, float))]
    gap_vals = [float(e["price_gap_at_t1_bps"]) for e in events if isinstance(e.get("price_gap_at_t1_bps"), (int, float))]
    max_profit_vals = [float(e["max_profit_bps"]) for e in events if isinstance(e.get("max_profit_bps"), (int, float))]
    profit_3s_vals = [float(e["profit_at_3s_bps"]) for e in events if isinstance(e.get("profit_at_3s_bps"), (int, float))]
    best_time_vals = [float(e["max_profit_time_ms"]) for e in events if isinstance(e.get("max_profit_time_ms"), (int, float))]

    by_symbol: Dict[str, dict] = {}
    for symbol in sorted(set(str(e["symbol"]) for e in events)):
        rows = [e for e in events if e["symbol"] == symbol]
        conv = [float(e["convergence_time_ms"]) for e in rows if isinstance(e.get("convergence_time_ms"), (int, float))]
        gap = [float(e["price_gap_at_t1_bps"]) for e in rows if isinstance(e.get("price_gap_at_t1_bps"), (int, float))]
        by_symbol[symbol] = {
            "events": len(rows),
            "convergence_time_ms": {
                "p50": percentile(conv, 0.5),
                "p90": percentile(conv, 0.9),
                "p99": percentile(conv, 0.99),
            },
            "price_gap_at_t1_bps": {
                "p50": percentile(gap, 0.5),
                "p90": percentile(gap, 0.9),
                "p99": percentile(gap, 0.99),
            },
        }

    return {
        "event_count": len(events),
        "convergence_time_ms": {
            "p50": percentile(convergence_vals, 0.5),
            "p90": percentile(convergence_vals, 0.9),
            "p99": percentile(convergence_vals, 0.99),
        },
        "price_gap_at_t1_bps": {
            "p50": percentile(gap_vals, 0.5),
            "p90": percentile(gap_vals, 0.9),
            "p99": percentile(gap_vals, 0.99),
        },
        "max_profit_bps": {
            "p50": percentile(max_profit_vals, 0.5),
            "p90": percentile(max_profit_vals, 0.9),
            "p99": percentile(max_profit_vals, 0.99),
        },
        "profit_at_3s_bps": {
            "p50": percentile(profit_3s_vals, 0.5),
            "p90": percentile(profit_3s_vals, 0.9),
            "p99": percentile(profit_3s_vals, 0.99),
        },
        "best_exit_time_ms": {
            "p50": percentile(best_time_vals, 0.5),
            "p90": percentile(best_time_vals, 0.9),
            "p99": percentile(best_time_vals, 0.99),
        },
        "by_symbol": by_symbol,
    }


async def run_measurement(symbols: List[str], duration_sec: int, pm_source: str, raw_root: str) -> dict:
    symbols = [normalize_symbol(s) for s in symbols]
    state = SharedState(symbols)
    end_ts = time.time() + duration_sec
    pm_task = run_pm_feed(state, end_ts) if pm_source == "ws_live_data" else run_pm_file_feed(state, end_ts, raw_root)
    await asyncio.gather(run_binance_feed(state, end_ts), pm_task)
    events = state.events
    return {
        "meta": {
            "started_at_ms": int((end_ts - duration_sec) * 1000),
            "finished_at_ms": int(time.time() * 1000),
            "duration_sec": duration_sec,
            "symbols": symbols,
            "velocity_trigger_bps_per_sec": 5.0,
            "t1_assumed_ms": 20,
            "pm_mid_source": pm_source,
            "raw_root": raw_root,
            "trigger_count": state.trigger_count,
            "trigger_skip_no_pm": state.trigger_skip_no_pm,
        },
        "summary": build_summary(events),
        "events": events,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Convergence and exit-timing analysis")
    parser.add_argument("--duration-sec", type=int, default=120)
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT")
    parser.add_argument("--pm-source", choices=["raw_file", "ws_live_data"], default="raw_file")
    parser.add_argument("--raw-root", default="datasets/raw")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    result = asyncio.run(run_measurement(symbols, args.duration_sec, args.pm_source, args.raw_root))
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(out_path.as_posix())
    print(json.dumps(result["summary"], ensure_ascii=False))


if __name__ == "__main__":
    main()
