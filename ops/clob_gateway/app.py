#!/usr/bin/env python3
"""Local CLOB order gateway for PolyEdge (py-clob-client).

Why this exists:
- Keep Rust free of EIP-712 signing / API key auth details.
- Provide a small, auditable localhost surface: /orders, /flatten, /health.

This service is meant to run on the *remote* box, bound to 127.0.0.1.
Secrets MUST be provided via environment variables (e.g. systemd EnvironmentFile).
"""

from __future__ import annotations

import math
import os
import threading
import time
from typing import Any, Dict, Optional, Tuple

from fastapi import Body, FastAPI
from fastapi.responses import JSONResponse

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL


ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# Polymarket CLOB enforces probability bounds for order prices.
# Using a strict clamp here makes the gateway resilient to small slippage widening and
# prevents avoidable "price out of range" rejects during canaries / micro-live.
MIN_PRICE = 0.01
MAX_PRICE = 0.99


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    if v is None:
        return default
    v = str(v).strip()
    return v if v else default


def _env_required(name: str) -> str:
    v = _env(name)
    if not v:
        raise RuntimeError(f"missing required env: {name}")
    return v


def _parse_bool(v: Optional[str]) -> bool:
    if v is None:
        return False
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _ms_now() -> int:
    return int(time.time() * 1000)


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _map_side(side: str) -> Tuple[str, Optional[str]]:
    s = str(side or "").strip().lower()
    if s in ("buy_yes", "buy_no"):
        return BUY, None
    if s in ("sell_yes", "sell_no"):
        return SELL, None
    return BUY, f"invalid_side:{side}"


def _map_tif_to_order_type(tif: str, ttl_ms: int) -> Tuple[OrderType, Optional[str]]:
    t = str(tif or "").strip().upper()
    if t == "FAK":
        return OrderType.FAK, None
    if t == "FOK":
        return OrderType.FOK, None
    if t == "GTC":
        return OrderType.GTC, None
    if t == "POST_ONLY":
        # Polymarket CLOB does not expose a strict "post-only" on this client surface.
        # We approximate with a resting order; use GTD to respect (coarse) ttl.
        if ttl_ms > 0:
            return OrderType.GTD, None
        return OrderType.GTC, None
    # Unknown -> be conservative.
    return OrderType.FAK, f"invalid_tif:{tif}"


def _coarse_expiration_s(ttl_ms: int) -> int:
    # CLOB expiration is second-granularity (sub-second TTLs become 1s).
    ttl_s = max(1, int(math.ceil(max(0.0, float(ttl_ms)) / 1000.0)))
    return int(time.time()) + ttl_s


def _safe_reject_code(exc: BaseException) -> str:
    msg = str(exc).replace("\n", " ").strip()
    msg = msg[:160]
    return f"exception:{exc.__class__.__name__}:{msg}"


class GatewayState:
    def __init__(self) -> None:
        self.client: Optional[ClobClient] = None
        self.client_lock = threading.Lock()
        self.tracked_orders: set[str] = set()
        self.tracked_lock = threading.Lock()

        # For debugging readiness without leaking secrets.
        self.ready: bool = False
        self.ready_error: str = ""


STATE = GatewayState()

app = FastAPI(title="PolyEdge CLOB Gateway", version="0.1.0")


@app.on_event("startup")
def _startup() -> None:
    host = _env("CLOB_HOST", "https://clob.polymarket.com")
    chain_id = int(_env("CLOB_CHAIN_ID", "137") or "137")
    private_key = _env_required("CLOB_PRIVATE_KEY")

    api_key = _env("CLOB_API_KEY")
    api_secret = _env("CLOB_API_SECRET")
    api_passphrase = _env("CLOB_API_PASSPHRASE")

    try:
        creds: Optional[ApiCreds] = None
        if api_key and api_secret and api_passphrase:
            creds = ApiCreds(
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_passphrase,
            )

        client = ClobClient(host=host, chain_id=chain_id, key=private_key, creds=creds)
        if creds is None:
            # Prefer deriving first to avoid creating a new API key on every restart.
            # If none exists yet, fall back to creating one.
            derived = None
            try:
                derived = client.derive_api_key()
            except Exception:
                derived = None
            if derived is None:
                derived = client.create_api_key()
            if derived is None:
                raise RuntimeError("failed to derive/create api creds (got None)")
            client.set_api_creds(derived)

        STATE.client = client
        STATE.ready = True
        STATE.ready_error = ""
    except Exception as exc:  # noqa: BLE001
        # Keep the process up so /health is useful, but mark as not-ready.
        # (systemd can still restart-loop if you prefer; set Restart=always)
        STATE.client = None
        STATE.ready = False
        STATE.ready_error = _safe_reject_code(exc)


@app.get("/health")
def health() -> Dict[str, Any]:
    client = STATE.client
    addr = None
    if client is not None:
        try:
            addr = client.get_address()
        except Exception:
            addr = None
    return {
        "status": "ok",
        "ready": bool(STATE.ready and client is not None),
        "ready_error": STATE.ready_error,
        "ts_ms": _ms_now(),
        "address": addr,
    }


@app.post("/orders")
def post_order(payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    started = time.perf_counter()

    def respond(
        accepted: bool,
        order_id: str = "",
        accepted_size: float = 0.0,
        reject_code: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> JSONResponse:
        exchange_latency_ms = (time.perf_counter() - started) * 1000.0
        body: Dict[str, Any] = {
            "accepted": bool(accepted),
            "order_id": order_id,
            "accepted_size": float(accepted_size),
            "reject_code": reject_code,
            "exchange_latency_ms": float(exchange_latency_ms),
            "ts_ms": _ms_now(),
        }
        if extra:
            body.update(extra)
        return JSONResponse(status_code=200, content=body)

    client = STATE.client
    if client is None or not STATE.ready:
        return respond(False, reject_code="gateway_not_ready")

    token_id = str(payload.get("token_id") or "").strip()
    if not token_id:
        return respond(False, reject_code="missing_token_id")

    side_raw = payload.get("side")
    side, side_err = _map_side(str(side_raw or ""))
    if side_err:
        return respond(False, reject_code=side_err)

    try:
        price = float(payload.get("price"))
        size = float(payload.get("size"))
    except Exception:
        return respond(False, reject_code="invalid_price_or_size")

    if not (MIN_PRICE <= price <= MAX_PRICE):
        return respond(False, reject_code="price_out_of_range")
    if not (size > 0.0):
        return respond(False, reject_code="size_non_positive")

    ttl_ms = int(payload.get("ttl_ms") or 0)
    tif_raw = str(payload.get("tif") or "").strip()
    order_type, tif_err = _map_tif_to_order_type(tif_raw, ttl_ms)
    if tif_err:
        return respond(False, reject_code=tif_err)

    # Optional slippage widening (kept conservative).
    try:
        max_slippage_bps = float(payload.get("max_slippage_bps") or 0.0)
    except Exception:
        max_slippage_bps = 0.0
    max_slippage_bps = max(0.0, max_slippage_bps)
    if max_slippage_bps > 0:
        slip = max_slippage_bps / 10_000.0
        if side == BUY:
            price = _clamp(price * (1.0 + slip), MIN_PRICE, MAX_PRICE)
        else:
            price = _clamp(price * (1.0 - slip), MIN_PRICE, MAX_PRICE)

    try:
        fee_rate_bps = float(payload.get("fee_rate_bps") or 0.0)
    except Exception:
        fee_rate_bps = 0.0
    fee_rate_bps_i = int(round(max(0.0, fee_rate_bps)))

    expiration_s = _coarse_expiration_s(ttl_ms)

    # Use a high-entropy nonce to prevent accidental duplicates.
    nonce = time.time_ns()

    try:
        with STATE.client_lock:
            signed = client.create_order(
                OrderArgs(
                    token_id=token_id,
                    price=price,
                    size=size,
                    side=side,
                    fee_rate_bps=fee_rate_bps_i,
                    nonce=nonce,
                    expiration=expiration_s,
                    taker=ZERO_ADDRESS,
                )
            )
            res = client.post_order(signed, orderType=order_type)
    except Exception as exc:  # noqa: BLE001
        return respond(False, reject_code=_safe_reject_code(exc))

    # Best-effort parsing; different gateways/clients sometimes vary the field names.
    order_id = ""
    try:
        if isinstance(res, dict):
            order_id = (
                (res.get("order_id") or res.get("id") or res.get("orderID") or res.get("orderId") or "")
            )
    except Exception:
        order_id = ""

    # Some responses include a filled size; if absent assume requested (engine will cap later anyway).
    accepted_size = None
    if isinstance(res, dict):
        for k in ("accepted_size", "size", "sizeMatched", "matched_size", "filled_size", "filledSize"):
            v = res.get(k)
            try:
                if v is not None:
                    accepted_size = float(v)
                    break
            except Exception:
                continue
        # Some responses include explicit error/success.
        if res.get("error") or res.get("errors"):
            return respond(False, order_id=order_id or "", accepted_size=0.0, reject_code=str(res.get("error") or "exchange_error")[:160])

    if accepted_size is None:
        accepted_size = float(size)

    if order_id:
        with STATE.tracked_lock:
            STATE.tracked_orders.add(str(order_id))

    # If the venue returns 0 fill for taker orders, treat as reject so the engine can account for it.
    accepted = accepted_size > 0.0
    reject_code = None if accepted else "zero_fill"
    return respond(accepted, order_id=order_id or "", accepted_size=accepted_size, reject_code=reject_code)


@app.delete("/orders/{order_id}")
def cancel_order(order_id: str) -> JSONResponse:
    client = STATE.client
    if client is None or not STATE.ready:
        return JSONResponse(status_code=503, content={"ok": False, "error": "gateway_not_ready"})
    try:
        with STATE.client_lock:
            _ = client.cancel(order_id)
        with STATE.tracked_lock:
            STATE.tracked_orders.discard(order_id)
        return JSONResponse(status_code=200, content={"ok": True})
    except Exception as exc:  # noqa: BLE001
        return JSONResponse(status_code=500, content={"ok": False, "error": _safe_reject_code(exc)})


@app.post("/flatten")
def flatten() -> JSONResponse:
    client = STATE.client
    if client is None or not STATE.ready:
        return JSONResponse(status_code=503, content={"ok": False, "error": "gateway_not_ready"})
    try:
        # Hard safety: cancel everything for the API key.
        with STATE.client_lock:
            _ = client.cancel_all()
        with STATE.tracked_lock:
            STATE.tracked_orders.clear()
        return JSONResponse(status_code=200, content={"ok": True})
    except Exception as exc:  # noqa: BLE001
        return JSONResponse(status_code=500, content={"ok": False, "error": _safe_reject_code(exc)})


def _main() -> int:
    import argparse

    p = argparse.ArgumentParser(description="PolyEdge local CLOB gateway (FastAPI)")
    p.add_argument("--host", default=_env("GATEWAY_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(_env("GATEWAY_PORT", "9001") or "9001"))
    p.add_argument("--log-level", default=_env("GATEWAY_LOG_LEVEL", "info"))
    args = p.parse_args()

    import uvicorn

    uvicorn.run("app:app", host=args.host, port=args.port, log_level=args.log_level, workers=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
