#!/usr/bin/env python3
"""
PolyEdge 实时研究仪表板（独立服务）

目标：
- 以 100ms 级别采集 BTC 价格 + Polymarket BTC Up/Down 市场盘口
- 实时输出可视化数据、策略建议、交易日志、PNL、热力图
- 与现有执行器解耦，避免历史策略链路影响研究验证

运行示例：
  python scripts/live_research_dashboard.py --host 0.0.0.0 --port 8098

依赖：
  pip install aiohttp websockets requests
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import aiohttp
import websockets
from aiohttp import web


def utc_ms() -> int:
    return int(time.time() * 1000)


def utc_iso(ms: Optional[int] = None) -> str:
    dt = datetime.fromtimestamp((ms or utc_ms()) / 1000.0, tz=timezone.utc)
    return dt.isoformat()


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def parse_rfc3339_to_ms(value: str) -> Optional[int]:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return int(datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return None


def classify_timeframe(text: str) -> Optional[str]:
    t = (text or "").upper()
    if "15 MIN" in t or "15M" in t or "15 MINUTE" in t:
        return "15m"
    if "5 MIN" in t or "5M" in t or "5 MINUTE" in t:
        return "5m"
    return None


def official_fee_per_share(price: float, fee_rate: float = 0.25, exponent: float = 2.0) -> float:
    p = clamp(price, 0.0, 1.0)
    return fee_rate * ((p * (1.0 - p)) ** exponent)


def parse_top_of_book(book_payload: dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    bids = book_payload.get("bids") or []
    asks = book_payload.get("asks") or []
    bid = None
    ask = None
    if bids:
        try:
            bid = float(bids[0].get("price"))
        except (TypeError, ValueError, AttributeError):
            bid = None
    if asks:
        try:
            ask = float(asks[0].get("price"))
        except (TypeError, ValueError, AttributeError):
            ask = None
    return bid, ask


def mid_from_bid_ask(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    if bid is not None and ask is not None and ask >= bid:
        return (bid + ask) * 0.5
    if bid is not None:
        return bid
    if ask is not None:
        return ask
    return None


def json_loads_array(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            arr = json.loads(value)
            if isinstance(arr, list):
                return arr
        except json.JSONDecodeError:
            return []
    return []


class LocalJsonlSink:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def append(self, name: str, payload: dict[str, Any]) -> None:
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        p = self.root / day / f"{name}.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")


class ModelAdapter:
    async def predict(self, features: dict[str, Any]) -> dict[str, float]:
        raise NotImplementedError


class HeuristicModel(ModelAdapter):
    async def predict(self, features: dict[str, Any]) -> dict[str, float]:
        delta_pct = float(features.get("delta_pct", 0.0))
        velocity = float(features.get("velocity_bps_per_sec", 0.0))
        accel = float(features.get("acceleration_bps_per_sec2", 0.0))
        rem = float(features.get("remaining_sec", 0.0))
        total = float(features.get("round_total_sec", 300.0))
        phase = 1.0 - clamp(rem / max(1.0, total), 0.0, 1.0)
        z = 1.25 * delta_pct + 0.012 * velocity + 0.003 * accel + 0.35 * phase
        p_up = clamp(sigmoid(z), 0.01, 0.99)
        return {"p_up": p_up, "p_down": 1.0 - p_up}


class HttpModelAdapter(ModelAdapter):
    def __init__(self, endpoint: str, timeout_sec: float = 0.5) -> None:
        self.endpoint = endpoint
        self.timeout_sec = timeout_sec

    async def predict(self, features: dict[str, Any]) -> dict[str, float]:
        timeout = aiohttp.ClientTimeout(total=self.timeout_sec)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(self.endpoint, json=features) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"model endpoint status={resp.status}")
                data = await resp.json()
                p_up = float(data.get("p_up", 0.5))
                p_up = clamp(p_up, 0.01, 0.99)
                return {"p_up": p_up, "p_down": 1.0 - p_up}


@dataclass
class SimPosition:
    side: str  # YES / NO
    qty: float
    entry_price: float
    opened_ts_ms: int
    market_id: str


@dataclass
class MarketState:
    market_id: str
    title: str
    timeframe: str
    end_ts_ms: int
    yes_token: str
    no_token: str
    target_price: float
    created_ts_ms: int
    bid_yes: Optional[float] = None
    ask_yes: Optional[float] = None
    bid_no: Optional[float] = None
    ask_no: Optional[float] = None
    mid_yes: Optional[float] = None
    mid_no: Optional[float] = None
    remaining_sec: float = 0.0
    delta_pct: float = 0.0
    velocity_bps_per_sec: float = 0.0
    acceleration_bps_per_sec2: float = 0.0
    p_up: float = 0.5
    p_down: float = 0.5
    ev_yes: float = 0.0
    ev_no: float = 0.0
    recommendation: str = "观望"
    reason: str = "初始化"
    last_update_ts_ms: int = 0
    yes_hist: deque[tuple[int, float]] = field(default_factory=lambda: deque(maxlen=3000))
    btc_hist: deque[tuple[int, float]] = field(default_factory=lambda: deque(maxlen=3000))
    sampled_features: list[tuple[float, float]] = field(default_factory=list)
    last_feature_sample_ts_ms: int = 0
    position: Optional[SimPosition] = None
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    closed: bool = False


class LiveResearchService:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.sink = LocalJsonlSink(Path(args.data_root))
        self.model: ModelAdapter = (
            HttpModelAdapter(args.model_endpoint, args.model_timeout_sec)
            if args.model_endpoint
            else HeuristicModel()
        )
        self.started_ms = utc_ms()

        self._session: Optional[aiohttp.ClientSession] = None
        self._ws_clients: set[web.WebSocketResponse] = set()
        self._tasks: list[asyncio.Task[Any]] = []

        self.btc_price: float = 0.0
        self.btc_source_ts_ms: int = 0

        self.markets: dict[str, MarketState] = {}
        self.trade_log: deque[dict[str, Any]] = deque(maxlen=2000)
        self.daily_realized_pnl: float = 0.0
        self.daily_unrealized_pnl: float = 0.0
        self.initial_capital: float = float(args.initial_capital)
        self.total_equity: float = self.initial_capital

        self.heatmap_up: dict[tuple[int, int], int] = defaultdict(int)
        self.heatmap_total: dict[tuple[int, int], int] = defaultdict(int)
        self.delta_bins = [-2.0, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0, 3.5, 5.0]
        self.remaining_bins = [0, 15, 30, 45, 60, 90, 120, 180, 240, 300, 420, 600, 900]

    async def start(self) -> None:
        timeout = aiohttp.ClientTimeout(total=5)
        self._session = aiohttp.ClientSession(timeout=timeout)
        self._tasks.extend(
            [
                asyncio.create_task(self._binance_ws_loop(), name="binance_ws"),
                asyncio.create_task(self._market_discovery_loop(), name="market_discovery"),
                asyncio.create_task(self._sampling_loop(), name="sampling_100ms"),
                asyncio.create_task(self._push_loop(), name="ws_push"),
            ]
        )

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _binance_ws_loop(self) -> None:
        symbol = self.args.binance_symbol.lower()
        ws_url = f"wss://stream.binance.com:9443/ws/{symbol}@trade"
        while True:
            try:
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as ws:
                    async for raw in ws:
                        now_ms = utc_ms()
                        try:
                            msg = json.loads(raw)
                            px = float(msg.get("p"))
                        except (ValueError, TypeError, json.JSONDecodeError):
                            continue
                        self.btc_price = px
                        self.btc_source_ts_ms = now_ms
                        self.sink.append(
                            "binance_ticks",
                            {
                                "ts_ms": now_ms,
                                "source": "binance_ws",
                                "symbol": self.args.binance_symbol.upper(),
                                "price": px,
                            },
                        )
            except Exception as e:
                self.sink.append("collector_errors", {"ts_ms": utc_ms(), "loop": "binance_ws", "error": str(e)})
                await asyncio.sleep(1.5)

    async def _market_discovery_loop(self) -> None:
        while True:
            try:
                discovered = await self._discover_active_markets()
                self._merge_markets(discovered)
            except Exception as e:
                self.sink.append("collector_errors", {"ts_ms": utc_ms(), "loop": "market_discovery", "error": str(e)})
            await asyncio.sleep(max(2.0, float(self.args.discovery_sec)))

    async def _discover_active_markets(self) -> list[dict[str, Any]]:
        assert self._session is not None
        endpoint = "https://gamma-api.polymarket.com/markets"
        symbol = self.args.symbol.upper()
        wanted_tfs = {s.strip().lower() for s in self.args.timeframes.split(",") if s.strip()}
        now = utc_ms()
        out: list[dict[str, Any]] = []
        seen: set[str] = set()

        for page in range(0, 8):
            limit = 200
            offset = page * limit
            params = {
                "closed": "false",
                "archived": "false",
                "active": "true",
                "limit": str(limit),
                "offset": str(offset),
                "order": "endDate",
                "ascending": "true",
            }
            async with self._session.get(endpoint, params=params) as resp:
                resp.raise_for_status()
                arr = await resp.json()
            if not arr:
                break
            for m in arr:
                market_id = str(m.get("id", "")).strip()
                if not market_id or market_id in seen:
                    continue
                seen.add(market_id)

                question = str(m.get("question", ""))
                text = f"{question} {m.get('slug', '')}".upper()
                if "UP OR DOWN" not in text and "UPDOWN" not in text:
                    continue
                if symbol == "BTCUSDT" and not any(k in text for k in ("BITCOIN", "BTC", "XBT")):
                    continue
                tf = classify_timeframe(text)
                if tf is None or tf not in wanted_tfs:
                    continue
                if not bool(m.get("active", False)):
                    continue
                if bool(m.get("closed", False)):
                    continue
                if not bool(m.get("acceptingOrders", False)):
                    continue

                end_ms = parse_rfc3339_to_ms(str(m.get("endDate", "")))
                if end_ms is None:
                    continue
                max_future_ms = 25 * 60 * 1000 if tf == "5m" else 70 * 60 * 1000
                max_past_ms = int(self.args.max_past_ms)
                delta = end_ms - now
                if delta < -max_past_ms or delta > max_future_ms:
                    continue

                token_ids = json_loads_array(m.get("clobTokenIds"))
                outcomes = [str(x).strip().lower() for x in json_loads_array(m.get("outcomes"))]
                if len(token_ids) < 2:
                    continue
                yes_idx = 0
                no_idx = 1
                if len(outcomes) >= 2:
                    for idx, oc in enumerate(outcomes[:2]):
                        if oc in ("up", "yes"):
                            yes_idx = idx
                        if oc in ("down", "no"):
                            no_idx = idx
                out.append(
                    {
                        "market_id": market_id,
                        "title": question,
                        "timeframe": tf,
                        "end_ts_ms": end_ms,
                        "yes_token": str(token_ids[yes_idx]),
                        "no_token": str(token_ids[no_idx]),
                    }
                )

        best_by_tf: dict[str, dict[str, Any]] = {}
        for m in out:
            tf = m["timeframe"]
            prev = best_by_tf.get(tf)
            if prev is None or m["end_ts_ms"] < prev["end_ts_ms"]:
                best_by_tf[tf] = m
        return list(best_by_tf.values())

    def _merge_markets(self, discovered: list[dict[str, Any]]) -> None:
        now = utc_ms()
        alive: set[str] = set()
        for m in discovered:
            market_id = m["market_id"]
            alive.add(market_id)
            if market_id in self.markets:
                st = self.markets[market_id]
                st.end_ts_ms = int(m["end_ts_ms"])
                st.closed = False
                continue
            target = self.btc_price if self.btc_price > 0 else 0.0
            self.markets[market_id] = MarketState(
                market_id=market_id,
                title=m["title"],
                timeframe=m["timeframe"],
                end_ts_ms=int(m["end_ts_ms"]),
                yes_token=m["yes_token"],
                no_token=m["no_token"],
                target_price=target,
                created_ts_ms=now,
            )
            self.sink.append(
                "market_events",
                {
                    "ts_ms": now,
                    "event": "market_open",
                    "market_id": market_id,
                    "timeframe": m["timeframe"],
                    "title": m["title"],
                    "target_price": target,
                },
            )

        for market_id, st in list(self.markets.items()):
            if market_id not in alive and not st.closed and utc_ms() > st.end_ts_ms + 30_000:
                self._finalize_market(st, forced=True)
                del self.markets[market_id]

    async def _sampling_loop(self) -> None:
        interval = max(20, int(self.args.sample_ms)) / 1000.0
        while True:
            t0 = time.time()
            try:
                await self._sample_once()
            except Exception as e:
                self.sink.append("collector_errors", {"ts_ms": utc_ms(), "loop": "sampling", "error": str(e)})
            elapsed = time.time() - t0
            await asyncio.sleep(max(0.0, interval - elapsed))

    async def _sample_once(self) -> None:
        if not self.markets or self.btc_price <= 0:
            return
        now = utc_ms()
        await asyncio.gather(*[self._update_market(st, now) for st in self.markets.values()], return_exceptions=True)

        realized = 0.0
        unrealized = 0.0
        for st in self.markets.values():
            realized += st.realized_pnl
            unrealized += st.unrealized_pnl
        self.daily_realized_pnl = realized
        self.daily_unrealized_pnl = unrealized
        self.total_equity = self.initial_capital + realized + unrealized

    async def _fetch_book(self, token_id: str) -> Optional[dict[str, Any]]:
        assert self._session is not None
        endpoint = "https://clob.polymarket.com/book"
        try:
            async with self._session.get(endpoint, params={"token_id": token_id}) as resp:
                if resp.status != 200:
                    return None
                return await resp.json()
        except Exception:
            return None

    def _compute_velocity_accel(self, hist: deque[tuple[int, float]]) -> tuple[float, float]:
        if len(hist) < 4:
            return 0.0, 0.0
        now_ts, now_p = hist[-1]

        def price_at(ts_cut: int) -> Optional[tuple[int, float]]:
            for ts, p in reversed(hist):
                if ts <= ts_cut:
                    return ts, p
            return None

        p1 = price_at(now_ts - 1000)
        p2 = price_at(now_ts - 2000)
        if p1 is None:
            return 0.0, 0.0

        dt1 = max(1.0, (now_ts - p1[0]) / 1000.0)
        v_now = ((now_p - p1[1]) * 10000.0) / dt1
        if p2 is None:
            return v_now, 0.0
        dt_prev = max(1.0, (p1[0] - p2[0]) / 1000.0)
        v_prev = ((p1[1] - p2[1]) * 10000.0) / dt_prev
        accel = (v_now - v_prev) / max(1.0, dt1)
        return v_now, accel

    def _infer_action(self, st: MarketState) -> tuple[str, str]:
        daily_loss_pct = 0.0
        if self.initial_capital > 0:
            daily_loss_pct = max(0.0, -(self.daily_realized_pnl / self.initial_capital) * 100.0)
        if daily_loss_pct >= float(self.args.daily_loss_stop_pct):
            return "观望", "日亏损熔断，停止新仓"

        if st.mid_yes is None or st.mid_no is None:
            return "观望", "盘口缺失"
        if st.remaining_sec <= 2:
            return "观望", "接近结算，避免新开仓"

        entry_ev = float(self.args.entry_ev_threshold)
        switch_ev = float(self.args.switch_ev_threshold)
        reverse_velocity = float(self.args.reverse_velocity_bps)
        reverse_drop = float(self.args.reverse_drop_pct)
        yes_now = st.mid_yes
        no_now = st.mid_no

        if st.position is None:
            if st.ev_yes > entry_ev and st.p_up >= 0.55:
                return "小仓买入YES", "趋势向上且EV为正"
            if st.ev_no > entry_ev and st.p_up <= 0.45:
                return "小仓买入NO", "趋势向下且EV为正"
            return "观望", "信号不足或EV不足"

        pos = st.position
        if pos.side == "YES":
            drawdown = yes_now - pos.entry_price
            strong_reverse = st.velocity_bps_per_sec <= reverse_velocity and st.delta_pct <= -abs(reverse_drop * 100.0)
            if strong_reverse and st.ev_no > switch_ev:
                return "卖出YES并切换NO", "反向确认，执行快速切换"
            if drawdown < -0.03 and st.remaining_sec > 20:
                return "卖出YES锁损", "回撤过大"
            return "持有YES", "趋势未反转"

        drawdown = no_now - pos.entry_price
        strong_reverse = st.velocity_bps_per_sec >= abs(reverse_velocity) and st.delta_pct >= abs(reverse_drop * 100.0)
        if strong_reverse and st.ev_yes > switch_ev:
            return "卖出NO并切换YES", "反向确认，执行快速切换"
        if drawdown < -0.03 and st.remaining_sec > 20:
            return "卖出NO锁损", "回撤过大"
        return "持有NO", "趋势未反转"

    def _apply_simulated_action(self, st: MarketState, action: str, reason: str, now: int) -> None:
        if st.mid_yes is None or st.mid_no is None:
            return
        qty_notional = float(self.args.base_notional_usdc)

        def log(action_name: str, side: str, price: float, qty: float, pnl: float) -> None:
            row = {
                "ts_ms": now,
                "ts_iso": utc_iso(now),
                "market_id": st.market_id,
                "timeframe": st.timeframe,
                "title": st.title,
                "action": action_name,
                "side": side,
                "price": price,
                "qty_shares": qty,
                "reason": reason,
                "realized_pnl": pnl,
                "round_realized_pnl": st.realized_pnl,
            }
            self.trade_log.appendleft(row)
            self.sink.append("trade_log", row)

        if st.position is None:
            if action == "小仓买入YES":
                px = st.mid_yes
                shares = qty_notional / max(px, 1e-6)
                fee = official_fee_per_share(px) * shares
                st.realized_pnl -= fee
                st.position = SimPosition("YES", shares, px, now, st.market_id)
                log("BUY", "YES", px, shares, -fee)
            elif action == "小仓买入NO":
                px = st.mid_no
                shares = qty_notional / max(px, 1e-6)
                fee = official_fee_per_share(px) * shares
                st.realized_pnl -= fee
                st.position = SimPosition("NO", shares, px, now, st.market_id)
                log("BUY", "NO", px, shares, -fee)
            return

        pos = st.position

        def close_position(close_price: float, side: str) -> None:
            fee = official_fee_per_share(close_price) * pos.qty
            pnl = (close_price - pos.entry_price) * pos.qty - fee
            st.realized_pnl += pnl
            st.position = None
            log("SELL", side, close_price, pos.qty, pnl)

        if pos.side == "YES":
            if action == "卖出YES锁损":
                close_position(st.mid_yes, "YES")
            elif action == "卖出YES并切换NO":
                close_position(st.mid_yes, "YES")
                px = st.mid_no
                shares = qty_notional / max(px, 1e-6)
                fee = official_fee_per_share(px) * shares
                st.realized_pnl -= fee
                st.position = SimPosition("NO", shares, px, now, st.market_id)
                log("BUY", "NO", px, shares, -fee)
        else:
            if action == "卖出NO锁损":
                close_position(st.mid_no, "NO")
            elif action == "卖出NO并切换YES":
                close_position(st.mid_no, "NO")
                px = st.mid_yes
                shares = qty_notional / max(px, 1e-6)
                fee = official_fee_per_share(px) * shares
                st.realized_pnl -= fee
                st.position = SimPosition("YES", shares, px, now, st.market_id)
                log("BUY", "YES", px, shares, -fee)

    def _update_unrealized(self, st: MarketState) -> None:
        st.unrealized_pnl = 0.0
        if st.position is None:
            return
        if st.position.side == "YES" and st.mid_yes is not None:
            st.unrealized_pnl = (st.mid_yes - st.position.entry_price) * st.position.qty
        elif st.position.side == "NO" and st.mid_no is not None:
            st.unrealized_pnl = (st.mid_no - st.position.entry_price) * st.position.qty

    def _heatmap_bin(self, delta_pct: float, remaining_sec: float) -> tuple[int, int]:
        bx = len(self.delta_bins) - 2
        by = len(self.remaining_bins) - 2
        for i in range(len(self.delta_bins) - 1):
            if self.delta_bins[i] <= delta_pct < self.delta_bins[i + 1]:
                bx = i
                break
        for i in range(len(self.remaining_bins) - 1):
            if self.remaining_bins[i] <= remaining_sec < self.remaining_bins[i + 1]:
                by = i
                break
        return bx, by

    def _finalize_market(self, st: MarketState, forced: bool = False) -> None:
        if st.closed:
            return
        st.closed = True
        outcome_up = 1 if self.btc_price >= st.target_price else 0
        if st.position is not None:
            pos = st.position
            payout = 1.0 if ((pos.side == "YES" and outcome_up == 1) or (pos.side == "NO" and outcome_up == 0)) else 0.0
            pnl = (payout - pos.entry_price) * pos.qty
            st.realized_pnl += pnl
            self.trade_log.appendleft(
                {
                    "ts_ms": utc_ms(),
                    "ts_iso": utc_iso(),
                    "market_id": st.market_id,
                    "timeframe": st.timeframe,
                    "title": st.title,
                    "action": "SETTLE",
                    "side": pos.side,
                    "price": payout,
                    "qty_shares": pos.qty,
                    "reason": "到期结算",
                    "realized_pnl": pnl,
                    "round_realized_pnl": st.realized_pnl,
                }
            )
            st.position = None
            st.unrealized_pnl = 0.0

        for delta_pct, rem in st.sampled_features:
            bx, by = self._heatmap_bin(delta_pct, rem)
            self.heatmap_total[(bx, by)] += 1
            self.heatmap_up[(bx, by)] += outcome_up

        self.sink.append(
            "round_summary",
            {
                "ts_ms": utc_ms(),
                "event": "market_close" if not forced else "market_cleanup",
                "market_id": st.market_id,
                "title": st.title,
                "timeframe": st.timeframe,
                "target_price": st.target_price,
                "btc_end": self.btc_price,
                "outcome_up": outcome_up,
                "round_realized_pnl": st.realized_pnl,
                "samples": len(st.sampled_features),
            },
        )

    async def _update_market(self, st: MarketState, now: int) -> None:
        yes_book, no_book = await asyncio.gather(
            self._fetch_book(st.yes_token),
            self._fetch_book(st.no_token),
        )
        if yes_book is None or no_book is None:
            return

        bid_yes, ask_yes = parse_top_of_book(yes_book)
        bid_no, ask_no = parse_top_of_book(no_book)
        mid_yes = mid_from_bid_ask(bid_yes, ask_yes)
        mid_no = mid_from_bid_ask(bid_no, ask_no)
        if mid_yes is None or mid_no is None:
            return

        st.bid_yes, st.ask_yes, st.mid_yes = bid_yes, ask_yes, mid_yes
        st.bid_no, st.ask_no, st.mid_no = bid_no, ask_no, mid_no
        st.last_update_ts_ms = now
        st.remaining_sec = max(0.0, (st.end_ts_ms - now) / 1000.0)
        st.delta_pct = ((self.btc_price - st.target_price) / st.target_price) * 100.0 if st.target_price > 0 else 0.0
        st.yes_hist.append((now, mid_yes))
        st.btc_hist.append((now, self.btc_price))
        st.velocity_bps_per_sec, st.acceleration_bps_per_sec2 = self._compute_velocity_accel(st.yes_hist)

        round_total_sec = 300.0 if st.timeframe == "5m" else 900.0
        pred = await self.model.predict(
            {
                "delta_pct": st.delta_pct,
                "velocity_bps_per_sec": st.velocity_bps_per_sec,
                "acceleration_bps_per_sec2": st.acceleration_bps_per_sec2,
                "remaining_sec": st.remaining_sec,
                "round_total_sec": round_total_sec,
                "mid_yes": mid_yes,
                "mid_no": mid_no,
            }
        )
        st.p_up = float(pred.get("p_up", 0.5))
        st.p_down = float(pred.get("p_down", 0.5))
        st.ev_yes = st.p_up - mid_yes - official_fee_per_share(mid_yes)
        st.ev_no = st.p_down - mid_no - official_fee_per_share(mid_no)

        action, reason = self._infer_action(st)
        st.recommendation = action
        st.reason = reason
        self._apply_simulated_action(st, action, reason, now)
        self._update_unrealized(st)

        if now - st.last_feature_sample_ts_ms >= 1000:
            st.sampled_features.append((st.delta_pct, st.remaining_sec))
            st.last_feature_sample_ts_ms = now

        self.sink.append(
            "market_snapshots",
            {
                "ts_ms": now,
                "ts_iso": utc_iso(now),
                "market_id": st.market_id,
                "timeframe": st.timeframe,
                "title": st.title,
                "btc_price": self.btc_price,
                "target_price": st.target_price,
                "delta_pct": st.delta_pct,
                "remaining_sec": st.remaining_sec,
                "bid_yes": bid_yes,
                "ask_yes": ask_yes,
                "mid_yes": mid_yes,
                "bid_no": bid_no,
                "ask_no": ask_no,
                "mid_no": mid_no,
                "velocity_bps_per_sec": st.velocity_bps_per_sec,
                "acceleration_bps_per_sec2": st.acceleration_bps_per_sec2,
                "p_up": st.p_up,
                "ev_yes": st.ev_yes,
                "ev_no": st.ev_no,
                "recommendation": action,
                "reason": reason,
            },
        )

        if st.remaining_sec <= 0:
            self._finalize_market(st)

    async def _push_loop(self) -> None:
        while True:
            await asyncio.sleep(max(0.2, float(self.args.ui_push_sec)))
            if not self._ws_clients:
                continue
            payload = self.snapshot()
            raw = json.dumps(payload, ensure_ascii=False)
            stale: list[web.WebSocketResponse] = []
            for ws in self._ws_clients:
                try:
                    await ws.send_str(raw)
                except Exception:
                    stale.append(ws)
            for ws in stale:
                self._ws_clients.discard(ws)

    def _round_series(self, st: MarketState, limit: int = 1200) -> list[dict[str, Any]]:
        yes = list(st.yes_hist)[-limit:]
        btc = list(st.btc_hist)[-limit:]
        by_ts: dict[int, dict[str, Any]] = {}
        for ts, p in yes:
            by_ts.setdefault(ts, {})["yes"] = p
        for ts, p in btc:
            by_ts.setdefault(ts, {})["btc"] = p
        out = []
        for ts in sorted(by_ts.keys()):
            row = by_ts[ts]
            out.append({"ts_ms": ts, "yes": row.get("yes"), "btc": row.get("btc"), "target": st.target_price})
        return out

    def _heatmap_payload(self) -> dict[str, Any]:
        rows = len(self.remaining_bins) - 1
        cols = len(self.delta_bins) - 1
        matrix = [[None for _ in range(cols)] for _ in range(rows)]
        for by in range(rows):
            for bx in range(cols):
                total = self.heatmap_total.get((bx, by), 0)
                up = self.heatmap_up.get((bx, by), 0)
                matrix[by][bx] = None if total == 0 else (up / total)
        return {
            "delta_bins": self.delta_bins,
            "remaining_bins": self.remaining_bins,
            "matrix": matrix,
        }

    def snapshot(self) -> dict[str, Any]:
        markets = []
        exposure_yes = 0.0
        exposure_no = 0.0
        for st in sorted(self.markets.values(), key=lambda x: (x.timeframe, x.end_ts_ms)):
            if st.position is not None:
                notional = st.position.qty * st.position.entry_price
                if st.position.side == "YES":
                    exposure_yes += notional
                else:
                    exposure_no += notional

            markets.append(
                {
                    "market_id": st.market_id,
                    "title": st.title,
                    "timeframe": st.timeframe,
                    "end_ts_ms": st.end_ts_ms,
                    "remaining_sec": round(st.remaining_sec, 3),
                    "target_price": st.target_price,
                    "btc_price": self.btc_price,
                    "delta_pct": round(st.delta_pct, 6),
                    "bid_yes": st.bid_yes,
                    "ask_yes": st.ask_yes,
                    "mid_yes": st.mid_yes,
                    "bid_no": st.bid_no,
                    "ask_no": st.ask_no,
                    "mid_no": st.mid_no,
                    "velocity_bps_per_sec": round(st.velocity_bps_per_sec, 4),
                    "acceleration_bps_per_sec2": round(st.acceleration_bps_per_sec2, 4),
                    "p_up": round(st.p_up, 6),
                    "ev_yes": round(st.ev_yes, 8),
                    "ev_no": round(st.ev_no, 8),
                    "recommendation": st.recommendation,
                    "reason": st.reason,
                    "position_side": None if st.position is None else st.position.side,
                    "position_qty": 0.0 if st.position is None else st.position.qty,
                    "realized_pnl": st.realized_pnl,
                    "unrealized_pnl": st.unrealized_pnl,
                    "series": self._round_series(st),
                }
            )

        daily_loss_pct = max(
            0.0,
            (self.initial_capital - (self.initial_capital + self.daily_realized_pnl)) / self.initial_capital * 100.0,
        )
        return {
            "ts_ms": utc_ms(),
            "ts_iso": utc_iso(),
            "uptime_sec": round((utc_ms() - self.started_ms) / 1000.0, 2),
            "collector": {
                "sample_ms": self.args.sample_ms,
                "binance_symbol": self.args.binance_symbol.upper(),
                "active_markets": len(self.markets),
            },
            "summary": {
                "btc_price": self.btc_price,
                "equity": self.total_equity,
                "realized_pnl": self.daily_realized_pnl,
                "unrealized_pnl": self.daily_unrealized_pnl,
                "daily_loss_pct": daily_loss_pct,
                "daily_loss_stop_pct": float(self.args.daily_loss_stop_pct),
                "exposure_yes": exposure_yes,
                "exposure_no": exposure_no,
            },
            "markets": markets,
            "trades": list(self.trade_log)[:200],
            "heatmap": self._heatmap_payload(),
        }


HTML_PATH = Path(__file__).resolve().parent / "live_dashboard" / "index.html"


def build_app(service: LiveResearchService) -> web.Application:
    app = web.Application()

    async def on_startup(_: web.Application) -> None:
        await service.start()

    async def on_cleanup(_: web.Application) -> None:
        await service.stop()

    async def index(_: web.Request) -> web.Response:
        if not HTML_PATH.exists():
            return web.Response(text="dashboard html not found", status=500)
        return web.Response(text=HTML_PATH.read_text(encoding="utf-8"), content_type="text/html")

    async def api_health(_: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "ts_ms": utc_ms()})

    async def api_snapshot(_: web.Request) -> web.Response:
        return web.json_response(service.snapshot())

    async def ws_handler(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=20)
        await ws.prepare(request)
        service._ws_clients.add(ws)
        await ws.send_str(json.dumps(service.snapshot(), ensure_ascii=False))
        async for msg in ws:
            if msg.type == web.WSMsgType.ERROR:
                break
        service._ws_clients.discard(ws)
        return ws

    app.router.add_get("/", index)
    app.router.add_get("/api/health", api_health)
    app.router.add_get("/api/snapshot", api_snapshot)
    app.router.add_get("/ws", ws_handler)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PolyEdge BTC Up/Down 实时研究仪表板")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=8098)
    p.add_argument("--sample-ms", type=int, default=100, help="采样周期（毫秒）")
    p.add_argument("--discovery-sec", type=float, default=5.0, help="市场发现周期（秒）")
    p.add_argument("--ui-push-sec", type=float, default=0.5, help="WebSocket 推送间隔（秒）")
    p.add_argument("--max-past-ms", type=int, default=20_000, help="发现窗口允许过去毫秒")
    p.add_argument("--symbol", default="BTCUSDT", help="研究标的（当前建议 BTCUSDT）")
    p.add_argument("--timeframes", default="5m,15m", help="时间周期，逗号分隔")
    p.add_argument("--binance-symbol", default="BTCUSDT")
    p.add_argument("--entry-ev-threshold", type=float, default=0.0025, help="开仓最小EV（每股）")
    p.add_argument("--switch-ev-threshold", type=float, default=0.0030, help="切换最小EV（每股）")
    p.add_argument("--reverse-velocity-bps", type=float, default=-80.0, help="反向速度阈值(bps/s)")
    p.add_argument("--reverse-drop-pct", type=float, default=0.03, help="反向跌幅阈值（相对目标）")
    p.add_argument("--base-notional-usdc", type=float, default=10.0, help="模拟每次基础下单USDC")
    p.add_argument("--initial-capital", type=float, default=1000.0, help="模拟初始资金")
    p.add_argument("--daily-loss-stop-pct", type=float, default=2.0, help="日亏损熔断阈值")
    p.add_argument("--data-root", default="datasets/research_dashboard", help="本地JSONL存储根目录")
    p.add_argument("--model-endpoint", default="", help="可选HTTP模型接口，POST features -> {p_up}")
    p.add_argument("--model-timeout-sec", type=float, default=0.5, help="模型接口超时（秒）")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    service = LiveResearchService(args)
    app = build_app(service)
    web.run_app(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
