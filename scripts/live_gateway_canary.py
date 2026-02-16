#!/usr/bin/env python3
"""Canary for the local CLOB gateway.

Goal: prove "I can place a real order via gateway and flatten" with tiny notional.

Writes:
  datasets/reports/<day>/runs/<run_id>/live_gateway_canary.json
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


def utc_day(ts: Optional[float] = None) -> str:
    dt = datetime.fromtimestamp(ts or time.time(), tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def now_ms() -> int:
    return int(time.time() * 1000)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def fetch_book_top(clob_host: str, token_id: str, timeout_sec: float) -> Tuple[float, float]:
    """Returns (best_bid, best_ask). 0.0 when missing."""
    url = f"{clob_host.rstrip('/')}/book?token_id={token_id}"
    resp = requests.get(url, timeout=timeout_sec)
    resp.raise_for_status()
    data = resp.json()
    bids = data.get("bids") or []
    asks = data.get("asks") or []

    def px(level: Any) -> Optional[float]:
        if not isinstance(level, dict):
            return None
        v = level.get("price")
        if v is None:
            return None
        try:
            return float(v)
        except Exception:
            return None

    best_bid = 0.0
    best_ask = 0.0
    if bids:
        p0 = px(bids[0])
        best_bid = float(p0 or 0.0)
    if asks:
        p0 = px(asks[0])
        best_ask = float(p0 or 0.0)
    return best_bid, best_ask


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PolyEdge live gateway canary (tiny notional)")
    p.add_argument("--gateway-url", default="http://127.0.0.1:9001")
    p.add_argument("--clob-host", default="https://clob.polymarket.com")
    p.add_argument("--token-id", required=True, help="Polymarket asset_id / token_id (YES or NO token)")
    p.add_argument(
        "--price",
        type=float,
        default=None,
        help="Optional fixed price to skip fetching orderbook. Useful when token has no /book or access is restricted.",
    )
    p.add_argument("--market-id", default="canary")
    p.add_argument("--side", default="buy_yes", choices=["buy_yes", "buy_no", "sell_yes", "sell_no"])
    p.add_argument("--notional-usdc", type=float, default=0.5)
    p.add_argument("--max-slippage-bps", type=float, default=10.0)
    p.add_argument("--attempts", type=int, default=5)
    p.add_argument("--timeout-sec", type=float, default=5.0)
    p.add_argument("--run-id", default=f"gateway-canary-{int(time.time())}")
    p.add_argument("--out-root", default="datasets/reports")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    day = utc_day()
    out_dir = Path(args.out_root) / day / "runs" / args.run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    health = session.get(f"{args.gateway_url.rstrip('/')}/health", timeout=args.timeout_sec).json()

    best_bid = 0.0
    best_ask = 0.0
    if args.price is not None:
        ref_px = float(args.price)
    else:
        best_bid, best_ask = fetch_book_top(args.clob_host, args.token_id, args.timeout_sec)
        if args.side.startswith("buy"):
            ref_px = best_ask
            if ref_px <= 0:
                raise RuntimeError("no asks in book; cannot form crossable buy (pass --price to skip book fetch)")
        else:
            ref_px = best_bid
            if ref_px <= 0:
                raise RuntimeError("no bids in book; cannot form crossable sell (pass --price to skip book fetch)")

    if not (0.0 < ref_px < 1.0):
        raise RuntimeError(f"invalid ref_px={ref_px} (must be in (0,1))")

    notional = max(0.0, float(args.notional_usdc))
    if notional <= 0.0:
        raise RuntimeError("notional_usdc must be > 0")

    size = notional / ref_px

    records: List[Dict[str, Any]] = []
    ok = 0
    for i in range(1, max(1, args.attempts) + 1):
        client_order_id = f"{args.run_id}-{i:02d}"
        payload: Dict[str, Any] = {
            "market_id": args.market_id,
            "token_id": args.token_id,
            "side": args.side,
            "price": ref_px,
            "size": size,
            "ttl_ms": 5_000,
            "style": "taker",
            "tif": "FAK",
            "client_order_id": client_order_id,
            "max_slippage_bps": float(args.max_slippage_bps),
            "fee_rate_bps": 0.0,
            "expected_edge_net_bps": 0.0,
            "hold_to_resolution": False,
        }
        started = time.perf_counter()
        try:
            resp = session.post(
                f"{args.gateway_url.rstrip('/')}/orders",
                json=payload,
                timeout=args.timeout_sec,
            )
            latency_ms = (time.perf_counter() - started) * 1000.0
            rec: Dict[str, Any] = {
                "i": i,
                "latency_ms": latency_ms,
                "http_status": resp.status_code,
                "resp": resp.json() if resp.headers.get("content-type", "").startswith("application/json") else resp.text[:240],
            }
        except Exception as exc:  # noqa: BLE001
            rec = {"i": i, "error": str(exc)[:240]}

        records.append(rec)
        accepted = False
        try:
            accepted = bool((rec.get("resp") or {}).get("accepted"))
        except Exception:
            accepted = False
        if accepted:
            ok += 1

        # Always flatten after each attempt: ultra-safe.
        try:
            _ = session.post(f"{args.gateway_url.rstrip('/')}/flatten", timeout=args.timeout_sec)
        except Exception:
            pass
        time.sleep(0.5)

    summary: Dict[str, Any] = {
        "ts_ms": now_ms(),
        "gateway_url": args.gateway_url,
        "clob_host": args.clob_host,
        "token_id": args.token_id,
        "side": args.side,
        "book_top": {"best_bid": best_bid, "best_ask": best_ask, "ref_px": ref_px, "used_fixed_price": (args.price is not None)},
        "notional_usdc": notional,
        "size": size,
        "attempts": int(args.attempts),
        "accepted": ok,
        "accept_rate": (ok / max(1, int(args.attempts))),
        "health": health,
        "records": records,
    }
    write_json(out_dir / "live_gateway_canary.json", summary)
    print(f"wrote_json={out_dir / 'live_gateway_canary.json'} accept_rate={summary['accept_rate']:.3f}")
    return 0 if ok > 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
