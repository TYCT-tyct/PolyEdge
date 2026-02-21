#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import websockets


def utc_day(ts: Optional[float] = None) -> str:
    dt = datetime.fromtimestamp(ts or time.time(), tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def percentile(values: List[float], p: float) -> float:
    if not values:
        return float("nan")
    data = sorted(values)
    idx = int(round((len(data) - 1) * p))
    idx = max(0, min(idx, len(data) - 1))
    return float(data[idx])


def summary(values: List[float]) -> Dict[str, float]:
    if not values:
        return {"count": 0, "p50_ms": float("nan"), "p90_ms": float("nan"), "p99_ms": float("nan")}
    return {
        "count": len(values),
        "p50_ms": percentile(values, 0.50),
        "p90_ms": percentile(values, 0.90),
        "p99_ms": percentile(values, 0.99),
    }


def first_timestamp_ms(payload: Any) -> Optional[float]:
    # Traverse nested ws payloads; return the first plausible millisecond timestamp.
    stack = [payload]
    now_ms = time.time() * 1000.0
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            for k, v in cur.items():
                if k in {"timestamp", "ts", "time", "t"}:
                    try:
                        x = float(v)
                    except Exception:
                        continue
                    # Heuristic: valid unix ms in a reasonable range.
                    if 1_500_000_000_000 <= x <= now_ms + 120_000:
                        return x
                stack.append(v)
        elif isinstance(cur, list):
            stack.extend(cur)
    return None


async def measure_binance_ws_lag(symbol: str, seconds: int) -> List[float]:
    url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@trade"
    end_at = time.time() + seconds
    lags: List[float] = []
    while time.time() < end_at:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**22) as ws:
                while time.time() < end_at:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5)
                    except TimeoutError:
                        continue
                    recv_ms = time.time() * 1000.0
                    msg = json.loads(raw)
                    event_ts = msg.get("E")
                    if event_ts is None:
                        continue
                    lags.append(recv_ms - float(event_ts))
        except Exception:
            await asyncio.sleep(0.3)
    return lags


async def measure_pm_clob_ws_lag(
    asset_ids: List[str],
    seconds: int,
    latest_pm_recv_ms_holder: Optional[Dict[str, float]] = None,
) -> List[float]:
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    end_at = time.time() + seconds
    lags: List[float] = []
    sub = {"type": "market", "assets_ids": asset_ids}
    while time.time() < end_at:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**22) as ws:
                await ws.send(json.dumps(sub))
                while time.time() < end_at:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5)
                    except TimeoutError:
                        continue
                    recv_ms = time.time() * 1000.0
                    msg = json.loads(raw)
                    ts = first_timestamp_ms(msg)
                    if ts is None:
                        continue
                    if latest_pm_recv_ms_holder is not None:
                        latest_pm_recv_ms_holder["recv_ms"] = recv_ms
                    lags.append(recv_ms - ts)
        except Exception:
            await asyncio.sleep(0.3)
    return lags


async def measure_midpoint_delta(symbol: str, token_id: str, seconds: int, poll_interval_ms: int) -> Dict[str, Any]:
    """
    Recommended delta proxy:
    latest Binance tick event time -> /midpoint receive time.
    """
    latest_binance_event_ms: float = float("nan")
    latest_binance_recv_ms: float = float("nan")
    deltas_event: List[float] = []
    deltas_recv: List[float] = []
    deltas_recv_fresh_500ms: List[float] = []
    done = False

    async def binance_task() -> None:
        nonlocal latest_binance_event_ms, latest_binance_recv_ms, done
        url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@trade"
        while not done:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**22) as ws:
                    while not done:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5)
                        except TimeoutError:
                            continue
                        msg = json.loads(raw)
                        event_ts = msg.get("E")
                        if event_ts is not None:
                            latest_binance_event_ms = float(event_ts)
                        latest_binance_recv_ms = time.time() * 1000.0
            except Exception:
                await asyncio.sleep(0.3)

    async def midpoint_task() -> None:
        nonlocal done
        end_at = time.time() + seconds
        session = requests.Session()
        while time.time() < end_at:
            t0 = time.perf_counter()
            r = session.get("https://clob.polymarket.com/midpoint", params={"token_id": token_id}, timeout=3.0)
            r.raise_for_status()
            _ = r.json()
            recv_ms = time.time() * 1000.0
            if not math.isnan(latest_binance_event_ms):
                deltas_event.append(recv_ms - latest_binance_event_ms)
            if not math.isnan(latest_binance_recv_ms):
                # Same-clock delta: both are local receive timestamps on the same host/process.
                d = recv_ms - latest_binance_recv_ms
                deltas_recv.append(d)
                if d <= 500.0:
                    deltas_recv_fresh_500ms.append(d)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            sleep_ms = max(0.0, poll_interval_ms - elapsed_ms)
            await asyncio.sleep(sleep_ms / 1000.0)
        done = True

    await asyncio.gather(binance_task(), midpoint_task())
    return {
        "delta_midpoint_recv_minus_latest_binance_event_ms": summary(deltas_event),
        "delta_midpoint_recv_minus_latest_binance_recv_ms_same_clock": summary(deltas_recv),
        "delta_midpoint_recv_minus_latest_binance_recv_ms_same_clock_fresh_le_500ms": summary(
            deltas_recv_fresh_500ms
        ),
    }


def fetch_token_pair(symbol: str) -> Dict[str, str]:
    """
    Get a live token pair for symbol from Gamma.
    """
    url = "https://gamma-api.polymarket.com/markets"
    session = requests.Session()
    symbol_u = symbol.upper().replace("USDT", "")
    offset = 0
    while offset <= 4000:
        r = session.get(
            url,
            params={
                "closed": "false",
                "archived": "false",
                "active": "true",
                "limit": "500",
                "offset": str(offset),
                "order": "volume",
                "ascending": "false",
            },
            timeout=5.0,
        )
        r.raise_for_status()
        markets = r.json()
        if not markets:
            break
        for m in markets:
            clob_ids_raw = m.get("clobTokenIds")
            if not clob_ids_raw:
                continue
            if symbol_u not in (m.get("question", "") + m.get("description", "") + m.get("slug", "")).upper():
                continue
            try:
                clob_ids = json.loads(clob_ids_raw)
            except Exception:
                continue
            if isinstance(clob_ids, list) and len(clob_ids) >= 2:
                return {
                    "market_id": str(m.get("id", "")),
                    "token_yes": str(clob_ids[0]),
                    "token_no": str(clob_ids[1]),
                    "question": str(m.get("question", "")),
                }
        offset += 500
    raise RuntimeError(f"no active token pair found for {symbol}")


def measure_http_rtt(token_id: str, samples: int) -> Dict[str, Dict[str, float]]:
    session = requests.Session()
    book_rtt: List[float] = []
    midpoint_rtt: List[float] = []
    for _ in range(samples):
        t0 = time.perf_counter()
        rb = session.get("https://clob.polymarket.com/book", params={"token_id": token_id}, timeout=3.0)
        rb.raise_for_status()
        _ = rb.json()
        book_rtt.append((time.perf_counter() - t0) * 1000.0)

        t1 = time.perf_counter()
        rm = session.get("https://clob.polymarket.com/midpoint", params={"token_id": token_id}, timeout=3.0)
        rm.raise_for_status()
        _ = rm.json()
        midpoint_rtt.append((time.perf_counter() - t1) * 1000.0)
    return {
        "book_http_rtt_ms": summary(book_rtt),
        "midpoint_http_rtt_ms": summary(midpoint_rtt),
    }


async def main() -> int:
    p = argparse.ArgumentParser(description="Measure Binance/Polymarket latency for hyper_mesh diagnostics")
    p.add_argument("--symbol", default="BTCUSDT")
    p.add_argument("--seconds", type=int, default=180)
    p.add_argument("--http-samples", type=int, default=120)
    p.add_argument("--midpoint-poll-interval-ms", type=int, default=100)
    p.add_argument("--run-id", default=None)
    p.add_argument("--out-root", default="datasets/reports")
    args = p.parse_args()

    run_id = args.run_id.strip() if isinstance(args.run_id, str) else args.run_id

    token_info = fetch_token_pair(args.symbol)
    token_yes = token_info["token_yes"]
    token_no = token_info["token_no"]

    latest_pm_ws_recv_ms: Dict[str, float] = {}
    latest_bin_ws_recv_ms: Dict[str, float] = {}

    async def measure_binance_with_recv() -> List[float]:
        url = f"wss://stream.binance.com:9443/ws/{args.symbol.lower()}@trade"
        end_at = time.time() + args.seconds
        lags: List[float] = []
        while time.time() < end_at:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**22) as ws:
                    while time.time() < end_at:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5)
                        except TimeoutError:
                            continue
                        recv_ms = time.time() * 1000.0
                        latest_bin_ws_recv_ms["recv_ms"] = recv_ms
                        msg = json.loads(raw)
                        event_ts = msg.get("E")
                        if event_ts is None:
                            continue
                        lags.append(recv_ms - float(event_ts))
            except Exception:
                await asyncio.sleep(0.3)
        return lags

    bin_task = asyncio.create_task(measure_binance_with_recv())
    pm_task = asyncio.create_task(
        measure_pm_clob_ws_lag([token_yes, token_no], args.seconds, latest_pm_ws_recv_ms)
    )
    midpoint_delta_task = asyncio.create_task(
        measure_midpoint_delta(
            args.symbol,
            token_yes,
            args.seconds,
            args.midpoint_poll_interval_ms,
        )
    )

    bin_lags, pm_lags, midpoint_delta = await asyncio.gather(bin_task, pm_task, midpoint_delta_task)
    http_rtt = measure_http_rtt(token_yes, args.http_samples)

    payload: Dict[str, Any] = {
        "meta": {
            "ts_ms": int(time.time() * 1000),
            "symbol": args.symbol,
            "mode": "hyper_mesh",
            "seconds": args.seconds,
            "http_samples": args.http_samples,
            "run_id": run_id,
        },
        "market": token_info,
        "binance_ws_lag_ms": summary(bin_lags),
        "polymarket_clob_ws_lag_ms": summary(pm_lags),
        "clob_http": http_rtt,
        "delta": midpoint_delta,
    }
    # Proxy metric: pm ws event lag minus binance ws event lag.
    if bin_lags and pm_lags:
        payload["delta"]["pm_ws_recv_minus_binance_event_ms"] = {
            "p50_ms": percentile(pm_lags, 0.50) - percentile(bin_lags, 0.50),
            "p90_ms": percentile(pm_lags, 0.90) - percentile(bin_lags, 0.90),
            "p99_ms": percentile(pm_lags, 0.99) - percentile(bin_lags, 0.99),
        }
    if latest_pm_ws_recv_ms.get("recv_ms") and latest_bin_ws_recv_ms.get("recv_ms"):
        payload["delta"]["latest_pm_ws_recv_minus_latest_binance_ws_recv_ms_same_clock"] = (
            latest_pm_ws_recv_ms["recv_ms"] - latest_bin_ws_recv_ms["recv_ms"]
        )

    out_dir = Path(args.out_root) / utc_day()
    if run_id:
        out_dir = out_dir / "runs" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"hyper_mesh_latency_{int(time.time() * 1000)}.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"wrote_json={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
