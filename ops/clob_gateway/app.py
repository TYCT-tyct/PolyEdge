#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import math
import os
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, Optional, Tuple

from fastapi import Body, FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from py_clob_client.client import ClobClient, POST_ORDER
from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType
from py_clob_client.exceptions import PolyApiException
from py_clob_client.headers.headers import build_hmac_signature
from py_clob_client.http_helpers.helpers import post as http_post
from py_clob_client.order_builder.constants import BUY, SELL

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
MIN_PRICE = 0.01
MAX_PRICE = 0.99
TERMINAL_STATES = {"filled", "cancelled", "canceled", "rejected", "expired", "executed", "matched"}
DEFAULT_FEE_RATE_BPS = int((os.environ.get("CLOB_FEE_RATE_BPS") or "1000").strip() or "1000")
MIN_MARKETABLE_BUY_USDC = float((os.environ.get("CLOB_MIN_MARKETABLE_BUY_USDC") or "1.0").strip() or "1.0")


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    if v is None:
        return default
    v = str(v).strip()
    return v if v else default


def _env_bool(name: str, default: bool = False) -> bool:
    v = _env(name)
    if v is None:
        return default
    return v.lower() in {"1", "true", "yes", "on", "y"}


def _env_required(name: str) -> str:
    v = _env(name)
    if not v:
        raise RuntimeError(f"missing required env: {name}")
    return v


def _ms_now() -> int:
    return int(time.time() * 1000)


def _safe_error(exc: BaseException) -> str:
    msg = str(exc).replace("\n", " ").strip()[:180]
    return f"{exc.__class__.__name__}:{msg}"


def _normalize_price(v: float) -> float:
    return round(max(MIN_PRICE, min(MAX_PRICE, v)), 6)


def _map_side(side: str) -> Tuple[str, Optional[str]]:
    s = str(side or "").strip().lower()
    if s in {"buy_yes", "buy_no"}:
        return BUY, None
    if s in {"sell_yes", "sell_no"}:
        return SELL, None
    return BUY, f"invalid_side:{side}"


def _map_tif(tif: str, ttl_ms: int) -> Tuple[OrderType, Optional[str]]:
    t = str(tif or "").strip().upper()
    if t == "FAK":
        return OrderType.FAK, None
    if t == "FOK":
        return OrderType.FOK, None
    if t == "GTC":
        return OrderType.GTC, None
    if t == "POST_ONLY":
        return (OrderType.GTD if ttl_ms > 0 else OrderType.GTC), None
    return OrderType.FAK, f"invalid_tif:{tif}"


def _expiration_s(ttl_ms: int) -> int:
    # Polymarket enforces a safety threshold: GTD expiration must be sufficiently in the future.
    # Keep a >=61s floor to avoid immediate exchange rejects on short maker windows.
    ttl_s = max(61, int(math.ceil(max(0.0, float(ttl_ms)) / 1000.0)))
    return int(time.time()) + ttl_s


@dataclass
class PrebuiltEntry:
    key: str
    body: str
    api_key: str
    passphrase: str
    address: str
    hmac: Dict[str, str]
    expires_ms: int


@dataclass
class TrackedOrder:
    order_id: str
    created_ms: int
    timeout_ms: int
    next_poll_ms: int
    token_id: str
    market_id: str
    side: str
    source: str


app = FastAPI(title="PolyEdge CLOB Gateway", version="0.2.0")

STATE: Dict[str, Any] = {
    "client": None,
    "host": "https://clob.polymarket.com",
    "signature_type": None,
    "funder": None,
    "ready": False,
    "ready_error": "",
    "client_lock": threading.Lock(),
    "cache_lock": threading.Lock(),
    "cache": defaultdict(deque),
    "cache_enabled": _env_bool("CLOB_PREBUILD_CACHE_ENABLED", True),
    "cache_pool": int(_env("CLOB_PREBUILD_CACHE_POOL", "4") or "4"),
    "cache_ttl_ms": int(_env("CLOB_PREBUILD_CACHE_TTL_MS", "90000") or "90000"),
    "tracked_lock": threading.Lock(),
    "tracked": {},
    "order_timeout_ms": int(_env("CLOB_ORDER_TIMEOUT_MS", "6500") or "6500"),
    "order_poll_interval_ms": int(_env("CLOB_ORDER_POLL_INTERVAL_MS", "700") or "700"),
    "watch_interval_ms": int(_env("CLOB_ORDER_WATCH_INTERVAL_MS", "220") or "220"),
    "report_lock": threading.Lock(),
    "reports": deque(maxlen=int(_env("CLOB_REPORT_MAX_EVENTS", "2000") or "2000")),
    "report_seq": 0,
    "stop_event": threading.Event(),
    "watcher": None,
}


def _emit(event: str, **fields: Any) -> None:
    with STATE["report_lock"]:
        STATE["report_seq"] += 1
        row = {"seq": STATE["report_seq"], "event": event, "ts_ms": _ms_now()}
        row.update(fields)
        STATE["reports"].append(row)


def _reports_since(seq: int, limit: int) -> list[dict]:
    with STATE["report_lock"]:
        items = [x for x in STATE["reports"] if int(x.get("seq", 0)) > seq]
    return items[-limit:] if limit > 0 else items


def _extract_order_id(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    for k in ("order_id", "id", "orderID", "orderId", "hash"):
        if payload.get(k):
            return str(payload[k])
    order = payload.get("order")
    if isinstance(order, dict):
        for k in ("id", "order_id", "hash"):
            if order.get(k):
                return str(order[k])
    return ""


def _extract_size(payload: Any, fallback: float) -> float:
    if not isinstance(payload, dict):
        return max(0.0, fallback)
    for k in ("accepted_size", "size", "sizeMatched", "matched_size", "filled_size", "filledSize"):
        try:
            if payload.get(k) is not None:
                return max(0.0, float(payload[k]))
        except Exception:
            pass
    return max(0.0, fallback)


def _extract_reject(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    for k in ("reject_code", "reason", "error", "message", "detail"):
        if payload.get(k):
            return str(payload[k])[:180]
    if payload.get("errors"):
        return str(payload["errors"])[:180]
    return None


def _extract_state(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    row = payload.get("order") if isinstance(payload.get("order"), dict) else payload
    for k in ("status", "state", "order_status", "orderState"):
        if row.get(k) is not None:
            return str(row[k]).strip().lower()
    return ""


def _is_terminal(state: str) -> bool:
    return any(t in state for t in TERMINAL_STATES) if state else False


def _cache_put(entry: PrebuiltEntry) -> int:
    if not STATE["cache_enabled"]:
        return 0
    with STATE["cache_lock"]:
        q: Deque[PrebuiltEntry] = STATE["cache"][entry.key]
        q.append(entry)
        while len(q) > max(1, STATE["cache_pool"]):
            q.popleft()
        return len(q)


def _cache_take(key: str) -> Optional[PrebuiltEntry]:
    if not STATE["cache_enabled"]:
        return None
    now = _ms_now()
    with STATE["cache_lock"]:
        q: Optional[Deque[PrebuiltEntry]] = STATE["cache"].get(key)
        if not q:
            return None
        while q and q[0].expires_ms <= now:
            q.popleft()
        if not q:
            return None
        return q.popleft()


def _cache_stats() -> dict:
    now = _ms_now()
    with STATE["cache_lock"]:
        total = 0
        keys = list(STATE["cache"].keys())
        for key in keys:
            q: Deque[PrebuiltEntry] = STATE["cache"][key]
            while q and q[0].expires_ms <= now:
                q.popleft()
            if not q:
                del STATE["cache"][key]
                continue
            total += len(q)
        return {
            "enabled": STATE["cache_enabled"],
            "keys": len(STATE["cache"]),
            "entries": total,
            "pool": STATE["cache_pool"],
            "ttl_ms": STATE["cache_ttl_ms"],
        }


def _build_hmac_window(secret: str, body: str, sec: int) -> Dict[str, str]:
    now_s = int(time.time())
    out: Dict[str, str] = {}
    for ts in range(now_s, now_s + sec + 1):
        out[str(ts)] = build_hmac_signature(secret, ts, "POST", POST_ORDER, body)
    return out


def _submit_raw(body_text: str, headers: dict) -> dict:
    endpoint = f"{STATE['host']}{POST_ORDER}"
    try:
        res = http_post(endpoint, headers=headers, data=body_text)
    except PolyApiException as exc:
        raise RuntimeError(f"exchange_http:{exc.status_code}:{exc.error_msg}") from exc
    except Exception as exc:
        raise RuntimeError(_safe_error(exc)) from exc
    if isinstance(res, dict):
        return res
    if isinstance(res, str):
        try:
            return json.loads(res)
        except Exception:
            return {"raw": res}
    return {"raw": str(res)}


def _headers_from_request(headers: Any) -> Optional[dict]:
    vals = {
        "address": headers.get("poly-address") if headers else None,
        "sig": headers.get("poly-signature") if headers else None,
        "ts": headers.get("poly-timestamp") if headers else None,
        "api_key": headers.get("poly-api-key") if headers else None,
        "passphrase": headers.get("poly-passphrase") if headers else None,
    }
    if not all(vals.values()):
        return None
    return {
        "POLY_ADDRESS": vals["address"],
        "POLY_SIGNATURE": vals["sig"],
        "POLY_TIMESTAMP": str(vals["ts"]),
        "POLY_API_KEY": vals["api_key"],
        "POLY_PASSPHRASE": vals["passphrase"],
        "Content-Type": "application/json",
    }


def _build_order(payload: dict) -> Tuple[dict, Optional[str]]:
    token_id = str(payload.get("token_id") or "").strip()
    if not token_id:
        return {}, "missing_token_id"
    side_raw = str(payload.get("side") or "")
    side, err = _map_side(side_raw)
    if err:
        return {}, err
    try:
        price = float(payload.get("price"))
        size = float(payload.get("size"))
    except Exception:
        return {}, "invalid_price_or_size"
    if not (MIN_PRICE <= price <= MAX_PRICE):
        return {}, "price_out_of_range"
    if size <= 0.0:
        return {}, "size_non_positive"
    ttl_ms = int(payload.get("ttl_ms") or 0)
    tif_raw = str(payload.get("tif") or "FAK")
    order_type, tif_err = _map_tif(tif_raw, ttl_ms)
    if tif_err:
        return {}, tif_err
    slippage_bps = max(0.0, float(payload.get("max_slippage_bps") or 0.0))
    if slippage_bps > 0:
        slip = slippage_bps / 10_000.0
        price = _normalize_price(price * (1.0 + slip if side == BUY else 1.0 - slip))
    else:
        price = _normalize_price(price)
    # For marketable BUY paths, Polymarket enforces a minimum notional amount.
    if side == BUY and size * price < MIN_MARKETABLE_BUY_USDC:
        size = round((MIN_MARKETABLE_BUY_USDC / max(price, MIN_PRICE)) + 1e-6, 6)
    # Polymarket currently expects a market-specific fee rate (1000 for our target flow).
    # We keep this fixed to avoid exchange-side hard rejects from stale client payload values.
    fee_bps = DEFAULT_FEE_RATE_BPS
    nonce = int(payload.get("nonce") or time.time_ns())
    expiration = _expiration_s(ttl_ms) if order_type == OrderType.GTD else 0
    client: Optional[ClobClient] = STATE["client"]
    if client is None:
        return {}, "gateway_not_ready"
    try:
        with STATE["client_lock"]:
            signed = client.create_order(
                OrderArgs(
                    token_id=token_id,
                    price=price,
                    size=size,
                    side=side,
                    fee_rate_bps=fee_bps,
                    nonce=nonce,
                    expiration=expiration,
                    taker=ZERO_ADDRESS,
                )
            )
    except Exception as exc:
        return {}, _safe_error(exc)
    order_type_value = order_type.value if hasattr(order_type, "value") else str(order_type)
    body = {"order": signed.dict(), "owner": client.creds.api_key, "orderType": order_type_value}
    return {
        "token_id": token_id,
        "market_id": str(payload.get("market_id") or ""),
        "side": side_raw.strip().lower(),
        "size": size,
        "ttl_ms": ttl_ms,
        "timeout_ms": int(payload.get("cancel_after_ms") or 0),
        "cache_key": str(payload.get("cache_key") or "").strip(),
        "price": price,
        "fee_bps": fee_bps,
        "tif": tif_raw,
        "signed_order": signed,
        "order_type": order_type,
        "body": body,
    }, None


def _track(order_id: str, token_id: str, market_id: str, side: str, source: str, timeout_ms: int) -> None:
    timeout = max(500, timeout_ms if timeout_ms > 0 else STATE["order_timeout_ms"])
    now = _ms_now()
    row = TrackedOrder(
        order_id=order_id,
        created_ms=now,
        timeout_ms=timeout,
        next_poll_ms=now + STATE["order_poll_interval_ms"],
        token_id=token_id,
        market_id=market_id,
        side=side,
        source=source,
    )
    with STATE["tracked_lock"]:
        STATE["tracked"][order_id] = row


def _watch_loop() -> None:
    while not STATE["stop_event"].is_set():
        now = _ms_now()
        timeout_list: list[TrackedOrder] = []
        poll_list: list[TrackedOrder] = []
        with STATE["tracked_lock"]:
            for row in list(STATE["tracked"].values()):
                if now - row.created_ms >= row.timeout_ms:
                    timeout_list.append(row)
                elif now >= row.next_poll_ms:
                    row.next_poll_ms = now + STATE["order_poll_interval_ms"]
                    poll_list.append(row)

        client: Optional[ClobClient] = STATE["client"]
        for row in timeout_list:
            ok = False
            reason = ""
            if client is not None:
                try:
                    with STATE["client_lock"]:
                        client.cancel(row.order_id)
                    ok = True
                except Exception as exc:
                    reason = _safe_error(exc)
            with STATE["tracked_lock"]:
                STATE["tracked"].pop(row.order_id, None)
            _emit("order_timeout_cancelled" if ok else "order_timeout_cancel_failed", order_id=row.order_id, reason=reason)

        for row in poll_list:
            if client is None:
                continue
            info: Any = {}
            try:
                with STATE["client_lock"]:
                    info = client.get_order(row.order_id)
            except Exception as exc:
                _emit("order_poll_error", order_id=row.order_id, reason=_safe_error(exc))
                continue
            state = _extract_state(info)
            if not _is_terminal(state):
                continue
            with STATE["tracked_lock"]:
                STATE["tracked"].pop(row.order_id, None)
            _emit("order_terminal", order_id=row.order_id, state=state, filled_size=_extract_size(info, 0.0))

        STATE["stop_event"].wait(max(0.05, STATE["watch_interval_ms"] / 1000.0))


def _ensure_watcher() -> None:
    t = STATE["watcher"]
    if t is not None and t.is_alive():
        return
    STATE["stop_event"].clear()
    t = threading.Thread(target=_watch_loop, name="clob_gateway_watch", daemon=True)
    t.start()
    STATE["watcher"] = t


def _order_response(started: float, payload_resp: dict, fallback_size: float, meta: dict, source: str) -> JSONResponse:
    order_id = _extract_order_id(payload_resp)
    size = _extract_size(payload_resp, fallback_size)
    reject = _extract_reject(payload_resp)
    accepted = size > 0 and reject is None
    if order_id:
        _track(order_id, meta.get("token_id", ""), meta.get("market_id", ""), meta.get("side", ""), source, int(meta.get("timeout_ms") or 0))
    _emit("order_submit", accepted=accepted, order_id=order_id, accepted_size=size, reject=reject, source=source)
    return JSONResponse(
        status_code=200,
        content={
            "accepted": bool(accepted),
            "order_id": order_id,
            "accepted_size": float(size),
            "reject_code": reject,
            "exchange_latency_ms": float((time.perf_counter() - started) * 1000.0),
            "ts_ms": _ms_now(),
            "source": source,
        },
    )


def _is_invalid_nonce_error(exc: BaseException) -> bool:
    return "invalid_nonce" in _safe_error(exc).lower()


@app.on_event("startup")
def _startup() -> None:
    host = _env("CLOB_HOST", "https://clob.polymarket.com")
    chain_id = int(_env("CLOB_CHAIN_ID", "137") or "137")
    private_key = _env_required("CLOB_PRIVATE_KEY")
    signature_type = int(_env("CLOB_SIGNATURE_TYPE", "0") or "0")
    funder = _env("CLOB_FUNDER")
    api_key = _env("CLOB_API_KEY")
    api_secret = _env("CLOB_API_SECRET")
    api_passphrase = _env("CLOB_API_PASSPHRASE")
    creds = None
    if api_key and api_secret and api_passphrase:
        creds = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
    kwargs = {"host": host, "chain_id": chain_id, "key": private_key, "creds": creds, "signature_type": signature_type}
    if funder:
        kwargs["funder"] = funder
    client = ClobClient(**kwargs)
    if creds is None:
        derived = client.derive_api_key() or client.create_api_key()
        if derived is None:
            raise RuntimeError("failed to derive/create api creds")
        client.set_api_creds(derived)
    STATE["client"] = client
    STATE["host"] = host
    STATE["signature_type"] = signature_type
    STATE["funder"] = funder
    STATE["ready"] = True
    STATE["ready_error"] = ""
    _ensure_watcher()


@app.on_event("shutdown")
def _shutdown() -> None:
    STATE["stop_event"].set()
    t = STATE["watcher"]
    if t is not None and t.is_alive():
        t.join(timeout=1.0)


@app.get("/health")
def health() -> dict:
    client = STATE["client"]
    addr = None
    if client is not None:
        try:
            addr = client.get_address()
        except Exception:
            addr = None
    with STATE["tracked_lock"]:
        tracked_count = len(STATE["tracked"])
    return {
        "status": "ok",
        "ready": bool(STATE["ready"] and client is not None),
        "ready_error": STATE["ready_error"],
        "ts_ms": _ms_now(),
        "address": addr,
        "signature_type": STATE["signature_type"],
        "funder": STATE["funder"],
        "tracked_orders": tracked_count,
        "watcher_running": bool(STATE["watcher"] and STATE["watcher"].is_alive()),
        "prebuild_cache": _cache_stats(),
    }


@app.get("/reports/orders")
def reports_orders(since_seq: int = 0, limit: int = 200) -> dict:
    events = _reports_since(max(0, since_seq), max(1, min(limit, 1000)))
    with STATE["tracked_lock"]:
        rows = list(STATE["tracked"].values())
    return {
        "ok": True,
        "events": events,
        "latest_seq": int(events[-1]["seq"]) if events else int(since_seq),
        "tracked_orders": [{"order_id": x.order_id, "age_ms": max(0, _ms_now() - x.created_ms), "timeout_ms": x.timeout_ms, "source": x.source} for x in rows],
    }


@app.websocket("/ws/reports/orders")
async def ws_reports_orders(websocket: WebSocket) -> None:
    await websocket.accept()
    since = 0
    try:
        while True:
            events = _reports_since(since, 200)
            if events:
                since = int(events[-1]["seq"])
                await websocket.send_json({"type": "events", "events": events})
            else:
                await websocket.send_json({"type": "heartbeat", "ts_ms": _ms_now()})
            await asyncio.sleep(0.5)
    except (WebSocketDisconnect, Exception):
        return


@app.get("/cache/prebuild")
def prebuild_cache_status() -> dict:
    return {"ok": True, "stats": _cache_stats()}


@app.post("/prebuild_order")
def prebuild_order(payload: dict = Body(...)) -> JSONResponse:
    if STATE["client"] is None or not STATE["ready"]:
        return JSONResponse(status_code=503, content={"ok": False, "error": "gateway_not_ready"})
    built, err = _build_order(payload)
    if err:
        return JSONResponse(status_code=400, content={"ok": False, "error": err})
    client: ClobClient = STATE["client"]
    body_text = json.dumps(built["body"], separators=(",", ":"), ensure_ascii=False)
    window_s = max(5, min(int(payload.get("time_window_sec") or 60), 300))
    hmac_map = _build_hmac_window(client.creds.api_secret, body_text, window_s)
    key = built["cache_key"] or f"{built['token_id']}|{built['side']}|{built['price']:.6f}|{built['size']:.6f}|{str(built['tif']).upper()}|{int(built['ttl_ms'])}|{int(built['fee_bps'])}"
    entry = PrebuiltEntry(
        key=key,
        body=body_text,
        api_key=client.creds.api_key,
        passphrase=client.creds.api_passphrase,
        address=client.signer.address(),
        hmac=hmac_map,
        expires_ms=_ms_now() + STATE["cache_ttl_ms"],
    )
    cache_size = _cache_put(entry)
    return JSONResponse(status_code=200, content={"ok": True, "cache_key": key, "cache_size": cache_size, "body": body_text, "api_key": entry.api_key, "api_passphrase": entry.passphrase, "address": entry.address, "hmac_signatures": hmac_map})


@app.post("/orders")
async def post_order(request: Request) -> JSONResponse:
    started = time.perf_counter()
    body_bytes = await request.body()
    raw = body_bytes.decode("utf-8") if body_bytes else ""
    if STATE["client"] is None or not STATE["ready"]:
        return JSONResponse(status_code=200, content={"accepted": False, "order_id": "", "accepted_size": 0.0, "reject_code": "gateway_not_ready", "exchange_latency_ms": float((time.perf_counter() - started) * 1000.0), "ts_ms": _ms_now()})

    headers = _headers_from_request(request.headers)
    if headers and raw:
        try:
            res = _submit_raw(raw, headers)
        except Exception as exc:
            return JSONResponse(status_code=200, content={"accepted": False, "order_id": "", "accepted_size": 0.0, "reject_code": _safe_error(exc), "exchange_latency_ms": float((time.perf_counter() - started) * 1000.0), "ts_ms": _ms_now(), "source": "header_prebuilt"})
        return _order_response(started, res, 0.0, {}, "header_prebuilt")

    try:
        payload = json.loads(raw) if raw else {}
    except Exception:
        return JSONResponse(status_code=200, content={"accepted": False, "order_id": "", "accepted_size": 0.0, "reject_code": "invalid_json_body", "exchange_latency_ms": float((time.perf_counter() - started) * 1000.0), "ts_ms": _ms_now()})

    cache_key = str(payload.get("cache_key") or "").strip()
    if cache_key:
        hit = _cache_take(cache_key)
        if hit is not None:
            now_s = int(time.time())
            sig = hit.hmac.get(str(now_s))
            if not sig:
                client: ClobClient = STATE["client"]
                sig = build_hmac_signature(client.creds.api_secret, now_s, "POST", POST_ORDER, hit.body)
            try:
                res = _submit_raw(hit.body, {"POLY_ADDRESS": hit.address, "POLY_SIGNATURE": sig, "POLY_TIMESTAMP": str(now_s), "POLY_API_KEY": hit.api_key, "POLY_PASSPHRASE": hit.passphrase, "Content-Type": "application/json"})
            except Exception as exc:
                # Cached pre-signed payload can occasionally hit nonce reuse under concurrent retries.
                # Fallback once to a freshly signed order from payload.
                if _is_invalid_nonce_error(exc):
                    built_retry, err_retry = _build_order(payload)
                    if not err_retry:
                        client_retry: ClobClient = STATE["client"]
                        try:
                            with STATE["client_lock"]:
                                res_retry = client_retry.post_order(built_retry["signed_order"], orderType=built_retry["order_type"])
                            return _order_response(started, res_retry if isinstance(res_retry, dict) else {"raw": str(res_retry)}, built_retry["size"], built_retry, "cache_fallback_resign")
                        except Exception as exc_retry:
                            return JSONResponse(status_code=200, content={"accepted": False, "order_id": "", "accepted_size": 0.0, "reject_code": _safe_error(exc_retry), "exchange_latency_ms": float((time.perf_counter() - started) * 1000.0), "ts_ms": _ms_now(), "source": "cache_fallback_resign"})
                return JSONResponse(status_code=200, content={"accepted": False, "order_id": "", "accepted_size": 0.0, "reject_code": _safe_error(exc), "exchange_latency_ms": float((time.perf_counter() - started) * 1000.0), "ts_ms": _ms_now(), "source": "cache_prebuilt"})
            return _order_response(started, res, float(payload.get("size") or 0.0), payload, "cache_prebuilt")

    built, err = _build_order(payload)
    if err:
        return JSONResponse(status_code=200, content={"accepted": False, "order_id": "", "accepted_size": 0.0, "reject_code": err, "exchange_latency_ms": float((time.perf_counter() - started) * 1000.0), "ts_ms": _ms_now(), "source": "slow_sign"})
    client: ClobClient = STATE["client"]
    try:
        with STATE["client_lock"]:
            res = client.post_order(built["signed_order"], orderType=built["order_type"])
    except Exception as exc:
        return JSONResponse(status_code=200, content={"accepted": False, "order_id": "", "accepted_size": 0.0, "reject_code": _safe_error(exc), "exchange_latency_ms": float((time.perf_counter() - started) * 1000.0), "ts_ms": _ms_now(), "source": "slow_sign"})
    return _order_response(started, res if isinstance(res, dict) else {"raw": str(res)}, built["size"], built, "slow_sign")


@app.delete("/orders/{order_id}")
def cancel_order(order_id: str) -> JSONResponse:
    client: Optional[ClobClient] = STATE["client"]
    if client is None or not STATE["ready"]:
        return JSONResponse(status_code=503, content={"ok": False, "error": "gateway_not_ready"})
    try:
        with STATE["client_lock"]:
            client.cancel(order_id)
        with STATE["tracked_lock"]:
            STATE["tracked"].pop(order_id, None)
        _emit("order_cancelled_manual", order_id=order_id)
        return JSONResponse(status_code=200, content={"ok": True})
    except Exception as exc:
        return JSONResponse(status_code=200, content={"ok": False, "error": _safe_error(exc)})


@app.post("/flatten")
def flatten() -> JSONResponse:
    client: Optional[ClobClient] = STATE["client"]
    if client is None or not STATE["ready"]:
        return JSONResponse(status_code=503, content={"ok": False, "error": "gateway_not_ready"})
    try:
        with STATE["client_lock"]:
            client.cancel_all()
        with STATE["tracked_lock"]:
            STATE["tracked"].clear()
        _emit("flatten_all")
        return JSONResponse(status_code=200, content={"ok": True})
    except Exception as exc:
        return JSONResponse(status_code=200, content={"ok": False, "error": _safe_error(exc)})


def _main() -> int:
    import argparse
    import uvicorn

    p = argparse.ArgumentParser(description="PolyEdge local CLOB gateway")
    p.add_argument("--host", default=_env("GATEWAY_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(_env("GATEWAY_PORT", "9001") or "9001"))
    p.add_argument("--log-level", default=_env("GATEWAY_LOG_LEVEL", "info"))
    args = p.parse_args()
    uvicorn.run("app:app", host=args.host, port=args.port, log_level=args.log_level, workers=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
