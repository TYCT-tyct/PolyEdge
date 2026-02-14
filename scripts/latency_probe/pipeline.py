from __future__ import annotations

# NOTE: This module is kept for legacy REST comparison only.
# The primary benchmark path is scripts/e2e_latency_test.py in ws-first mode.

import json
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from .models import SYMBOL_TO_NAME, StageSample
from .stats import summarize


def discover_market(session: requests.Session, symbol: str) -> Dict[str, Any]:
    name = SYMBOL_TO_NAME.get(symbol, "Bitcoin")
    aliases = {
        "BTCUSDT": ["bitcoin", "btc"],
        "ETHUSDT": ["ethereum", "eth"],
        "SOLUSDT": ["solana", "sol"],
        "XRPUSDT": ["xrp"],
    }
    keys = aliases.get(symbol, [name.lower()])
    markets = _fetch_markets_pages(session)
    now = datetime.now(timezone.utc)

    def parse_ids(raw: Optional[str]) -> Optional[List[str]]:
        if not raw:
            return None
        try:
            parsed = json.loads(raw)
        except Exception:
            return None
        if not isinstance(parsed, list) or len(parsed) < 2:
            return None
        return [str(parsed[0]), str(parsed[1])]

    # Candidate priority:
    # 1) up/down style
    # 2) any market for the same asset
    candidate_groups: List[List[Dict[str, Any]]] = [[], []]
    for market in markets:
        if not _is_market_tradeable(market, now):
            continue
        question = market.get("question", "")
        slug = market.get("slug", "")
        text = f"{question} {slug}".lower()
        if not any(k in text for k in keys):
            continue
        ids = parse_ids(market.get("clobTokenIds"))
        if ids is None or not market.get("active", True):
            continue
        candidate = {
            "market_id": market.get("id"),
            "question": question,
            "yes_token": ids[0],
            "no_token": ids[1],
            "fallback_bid_yes": market.get("bestBid"),
            "fallback_ask_yes": market.get("bestAsk"),
        }
        if "Up or Down" in question:
            candidate_groups[0].append(candidate)
        else:
            candidate_groups[1].append(candidate)

    # Validate that chosen market has reachable live book.
    for group in candidate_groups:
        for candidate in group[:20]:
            try:
                fetch_polymarket_book(
                    session,
                    candidate["yes_token"],
                    candidate.get("fallback_bid_yes"),
                    candidate.get("fallback_ask_yes"),
                )
                return candidate
            except Exception:
                continue

    # Fallback: pick any active market with a reachable orderbook so the
    # end-to-end latency harness can still run when asset-specific windows are closed.
    for market in markets:
        if not _is_market_tradeable(market, now):
            continue
        ids = parse_ids(market.get("clobTokenIds"))
        if ids is None:
            continue
        candidate = {
            "market_id": market.get("id"),
            "question": market.get("question", ""),
            "yes_token": ids[0],
            "no_token": ids[1],
            "fallback_bid_yes": market.get("bestBid"),
            "fallback_ask_yes": market.get("bestAsk"),
        }
        try:
            fetch_polymarket_book(
                session,
                candidate["yes_token"],
                candidate.get("fallback_bid_yes"),
                candidate.get("fallback_ask_yes"),
            )
            return candidate
        except Exception:
            continue

    raise RuntimeError(f"No active market found for {symbol}")


def _is_market_tradeable(market: Dict[str, Any], now_utc: datetime) -> bool:
    if not market.get("active", True):
        return False
    if market.get("closed", False):
        return False
    if not market.get("acceptingOrders", False):
        return False
    if not market.get("enableOrderBook", False):
        return False
    end_date = _parse_iso8601_utc(market.get("endDate"))
    if end_date is not None and end_date <= now_utc:
        return False
    return True


def _parse_iso8601_utc(raw: Any) -> Optional[datetime]:
    if raw is None:
        return None
    try:
        text = str(raw).replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _fetch_markets_pages(session: requests.Session) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    limit = 1000
    for offset in (0, 1000, 2000, 3000):
        resp = session.get(
            "https://gamma-api.polymarket.com/markets",
            params={
                "closed": "false",
                "archived": "false",
                "active": "true",
                "limit": limit,
                "offset": offset,
                "order": "volume",
                "ascending": "false",
            },
            timeout=15,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        out.extend(batch)
        if len(batch) < limit:
            break
    return out


def fetch_reference_price(session: requests.Session, symbol: str) -> float:
    resp = session.get(
        "https://api.binance.com/api/v3/ticker/price",
        params={"symbol": symbol},
        timeout=6,
    )
    resp.raise_for_status()
    return float(resp.json()["price"])


def fetch_polymarket_book(
    session: requests.Session,
    token_id: str,
    fallback_bid_yes: Optional[float] = None,
    fallback_ask_yes: Optional[float] = None,
) -> Dict[str, float]:
    resp = session.get(
        "https://clob.polymarket.com/book",
        params={"token_id": token_id},
        timeout=6,
    )
    if resp.status_code == 200:
        payload = resp.json()
        bids = payload.get("bids", [])
        asks = payload.get("asks", [])
    else:
        bids = []
        asks = []

    if bids and asks:
        bid_yes = float(bids[0]["price"])
        ask_yes = float(asks[0]["price"])
    else:
        if fallback_bid_yes is None and fallback_ask_yes is None:
            raise RuntimeError("Empty orderbook from CLOB and no fallback best bid/ask")
        bid_yes = float(fallback_bid_yes) if fallback_bid_yes is not None else 0.0
        ask_yes = float(fallback_ask_yes) if fallback_ask_yes is not None else bid_yes
        if ask_yes < bid_yes:
            ask_yes = bid_yes
    return {
        "bid_yes": bid_yes,
        "ask_yes": ask_yes,
        "bid_no": max(0.0, 1.0 - ask_yes),
        "ask_no": min(1.0, 1.0 - bid_yes),
    }


def evaluate_signal(ref_price: float, book: Dict[str, float]) -> Dict[str, float]:
    mid_yes = (book["bid_yes"] + book["ask_yes"]) * 0.5
    spread = max(book["ask_yes"] - book["bid_yes"], 1e-4)
    normalized = max(-0.5, min(0.5, (ref_price / 100_000.0) - 0.5))
    jump = max(-0.02, min(0.02, normalized * 35.0 / 10_000.0))
    fair_yes = max(0.001, min(0.999, mid_yes + jump))
    edge_bps_bid = ((fair_yes - book["ask_yes"]) / max(book["ask_yes"], 1e-4)) * 10_000.0
    edge_bps_ask = ((book["bid_yes"] - fair_yes) / max(book["bid_yes"], 1e-4)) * 10_000.0
    confidence = max(0.0, min(1.0, 1.0 - spread * 10.0))
    return {
        "fair_yes": fair_yes,
        "edge_bps_bid": edge_bps_bid,
        "edge_bps_ask": edge_bps_ask,
        "confidence": confidence,
    }


def build_quotes(market_id: str, signal: Dict[str, float], base_size: float = 2.0) -> List[Dict[str, Any]]:
    if signal["confidence"] <= 0.05:
        return []
    intents: List[Dict[str, Any]] = []
    bid_price = max(0.001, min(0.999, signal["fair_yes"] - 0.01))
    ask_price = max(0.001, min(0.999, signal["fair_yes"] + 0.01))
    if signal["edge_bps_bid"] >= 3.0:
        intents.append(
            {
                "market_id": market_id,
                "side": "buy_yes",
                "price": bid_price,
                "size": base_size,
                "ttl_ms": 1500,
            }
        )
    if signal["edge_bps_ask"] >= 3.0:
        intents.append(
            {
                "market_id": market_id,
                "side": "sell_yes",
                "price": ask_price,
                "size": base_size,
                "ttl_ms": 1500,
            }
        )
    return intents


def risk_check(intents: List[Dict[str, Any]], max_market_notional: float = 50.0) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for intent in intents:
        capped = min(intent["size"], max_market_notional)
        if capped <= 0:
            continue
        intent2 = dict(intent)
        intent2["size"] = capped
        out.append(intent2)
    return out


def simulate_execution(intents: List[Dict[str, Any]]) -> List[str]:
    out = []
    for intent in intents:
        _ = json.dumps(intent, separators=(",", ":"))
        out.append(str(uuid.uuid4()))
    return out


def run_rest_pipeline(iterations: int, symbol: str) -> Dict[str, Any]:
    session = requests.Session()
    market = discover_market(session, symbol)
    samples: List[StageSample] = []
    failures = 0
    failure_reasons: Counter[str] = Counter()

    for _ in range(iterations):
        try:
            t0 = time.perf_counter_ns()

            t_ref_0 = time.perf_counter_ns()
            ref_px = fetch_reference_price(session, symbol)
            t_ref_1 = time.perf_counter_ns()

            t_book_0 = time.perf_counter_ns()
            book = fetch_polymarket_book(
                session,
                market["yes_token"],
                market.get("fallback_bid_yes"),
                market.get("fallback_ask_yes"),
            )
            t_book_1 = time.perf_counter_ns()

            t_sig_0 = time.perf_counter_ns()
            signal = evaluate_signal(ref_px, book)
            t_sig_1 = time.perf_counter_ns()

            t_quote_0 = time.perf_counter_ns()
            intents = build_quotes(market["market_id"], signal)
            t_quote_1 = time.perf_counter_ns()

            t_risk_0 = time.perf_counter_ns()
            safe_intents = risk_check(intents)
            t_risk_1 = time.perf_counter_ns()

            t_exec_0 = time.perf_counter_ns()
            _ = simulate_execution(safe_intents)
            t_exec_1 = time.perf_counter_ns()

            t1 = time.perf_counter_ns()

            samples.append(
                StageSample(
                    ref_fetch_ms=(t_ref_1 - t_ref_0) / 1e6,
                    book_fetch_ms=(t_book_1 - t_book_0) / 1e6,
                    signal_us=(t_sig_1 - t_sig_0) / 1e3,
                    quote_us=(t_quote_1 - t_quote_0) / 1e3,
                    risk_us=(t_risk_1 - t_risk_0) / 1e3,
                    exec_us=(t_exec_1 - t_exec_0) / 1e3,
                    total_ms=(t1 - t0) / 1e6,
                    intents=len(safe_intents),
                )
            )
        except Exception as exc:
            failures += 1
            failure_reasons[_classify_error(exc)] += 1

    return {
        "market": market,
        "n": len(samples),
        "failures": failures,
        "failure_reasons": dict(failure_reasons),
        "ref_fetch_ms": summarize([s.ref_fetch_ms for s in samples]),
        "book_fetch_ms": summarize([s.book_fetch_ms for s in samples]),
        "signal_us": summarize([s.signal_us for s in samples]),
        "quote_us": summarize([s.quote_us for s in samples]),
        "risk_us": summarize([s.risk_us for s in samples]),
        "exec_us": summarize([s.exec_us for s in samples]),
        "total_ms": summarize([s.total_ms for s in samples]),
        "intents_avg": sum([s.intents for s in samples]) / len(samples) if samples else 0.0,
    }


def _classify_error(exc: Exception) -> str:
    name = type(exc).__name__
    text = str(exc)
    if isinstance(exc, requests.exceptions.Timeout):
        return "timeout"
    if isinstance(exc, requests.exceptions.ConnectionError):
        return "connection_error"
    if isinstance(exc, requests.exceptions.HTTPError):
        return f"http_error:{text[:80]}"
    if "Empty orderbook" in text:
        return "empty_book_no_fallback"
    return f"{name}:{text[:80]}"
