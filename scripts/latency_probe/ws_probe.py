from __future__ import annotations

import asyncio
import json
import math
import time
from typing import Dict, List

import websockets

from .stats import percentile, summarize


async def run_ws_latency(seconds: int, symbol: str) -> Dict[str, float]:
    pm_symbol = symbol.lower()
    chainlink_symbol = internal_to_chainlink_symbol(symbol)
    bin_symbol = symbol.lower()
    pm_lags: List[float] = []
    chainlink_lags: List[float] = []
    bin_lags: List[float] = []

    async def pm_task() -> None:
        url = "wss://ws-live-data.polymarket.com"
        async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**22) as ws:
            sub = {
                "action": "subscribe",
                "subscriptions": [
                    {
                        "topic": "crypto_prices",
                        "type": "update",
                        "filters": f'{{"symbol":"{symbol}"}}',
                    },
                    {
                        # Chainlink RTDS does not currently support server-side symbol filters.
                        # Subscribe to the topic and filter client-side to keep this probe simple.
                        "topic": "crypto_prices_chainlink",
                        "type": "*",
                        "filters": "",
                    }
                ],
            }
            await ws.send(json.dumps(sub))
            end = time.time() + seconds
            while time.time() < end:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=3)
                except asyncio.TimeoutError:
                    continue
                recv_ms = time.time() * 1000.0
                if not raw:
                    continue
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                topic = msg.get("topic")
                if topic == "crypto_prices":
                    if msg.get("type") != "update":
                        continue
                    payload = msg.get("payload") or {}
                    sym = str(payload.get("symbol", "")).lower()
                    ts = payload.get("timestamp")
                    if sym != pm_symbol or ts is None:
                        continue
                    pm_lags.append(recv_ms - float(ts))
                elif topic == "crypto_prices_chainlink":
                    payload = msg.get("payload") or {}
                    sym = str(payload.get("symbol", "")).lower()
                    ts = payload.get("timestamp")
                    if sym != chainlink_symbol or ts is None:
                        continue
                    chainlink_lags.append(recv_ms - float(ts))

    async def bin_task() -> None:
        # Keep this consistent with the Rust ref feed which uses `@trade`.
        url = f"wss://stream.binance.com:9443/ws/{bin_symbol}@trade"
        async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**22) as ws:
            end = time.time() + seconds
            while time.time() < end:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=3)
                except asyncio.TimeoutError:
                    continue
                recv_ms = time.time() * 1000.0
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                event_ts = msg.get("E")
                if event_ts is None:
                    continue
                bin_lags.append(recv_ms - float(event_ts))

    await asyncio.gather(pm_task(), bin_task())
    pm_stats = summarize(pm_lags)
    chainlink_stats = summarize(chainlink_lags)
    bin_stats = summarize(bin_lags)
    delta_p50 = float("nan")
    delta_chainlink_minus_bin_p50 = float("nan")
    delta_pm_minus_chainlink_p50 = float("nan")
    if pm_lags and bin_lags:
        delta_p50 = percentile(pm_lags, 0.5) - percentile(bin_lags, 0.5)
    if chainlink_lags and bin_lags:
        delta_chainlink_minus_bin_p50 = percentile(chainlink_lags, 0.5) - percentile(bin_lags, 0.5)
    if pm_lags and chainlink_lags:
        delta_pm_minus_chainlink_p50 = percentile(pm_lags, 0.5) - percentile(chainlink_lags, 0.5)

    return {
        "pm_lag_ms": pm_stats,
        "chainlink_lag_ms": chainlink_stats,
        "bin_lag_ms": bin_stats,
        "delta_pm_minus_bin_p50_ms": delta_p50,
        "delta_chainlink_minus_bin_p50_ms": delta_chainlink_minus_bin_p50,
        "delta_pm_minus_chainlink_p50_ms": delta_pm_minus_chainlink_p50,
    }


def has_delta(ws_result: Dict[str, float]) -> bool:
    delta = ws_result.get("delta_pm_minus_bin_p50_ms", float("nan"))
    return not math.isnan(delta)


def internal_to_chainlink_symbol(symbol: str) -> str:
    """Map internal symbols like BTCUSDT to Chainlink RTDS symbols like btc/usd."""
    s = symbol.strip().upper()
    if s.endswith("USDT"):
        base = s[:-4].lower()
        return f"{base}/usd"
    return s.lower()
