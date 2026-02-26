# clob_gateway

Local (127.0.0.1) HTTP gateway that signs/orders via `py-clob-client`, so the Rust
engine does not need to implement EIP-712 signing + CLOB auth.

## Endpoints

- `GET /health`
- `GET /book?token_id=...`
- `POST /orders`
- `POST /prebuild_order`
- `DELETE /orders/{order_id}`
- `POST /flatten`
- `GET /reports/orders`
- `GET /cache/prebuild`
- `WS /ws/reports/orders`

## Environment Variables (required)

- `CLOB_HOST` (default: `https://clob.polymarket.com`)
- `CLOB_CHAIN_ID` (default: `137`)
- `CLOB_PRIVATE_KEY`
- `CLOB_SIGNATURE_TYPE` (default: `0`)
  - `0`: standard EOA wallet flow
  - `1`: proxy/Magic wallet flow (commonly used by email/Google login)
- `CLOB_FUNDER` (recommended when `CLOB_SIGNATURE_TYPE=1`)
  - For proxy wallets, set this to the funder address used by Polymarket.

API creds (recommended; if missing we try to derive on startup):

- `CLOB_API_KEY`
- `CLOB_API_SECRET`
- `CLOB_API_PASSPHRASE`

Optional tuning:

- `CLOB_PREBUILD_CACHE_ENABLED=true|false` (default `true`)
- `CLOB_PREBUILD_CACHE_POOL` (default `4`)
- `CLOB_PREBUILD_CACHE_TTL_MS` (default `90000`)
- `CLOB_ORDER_TIMEOUT_MS` (default `6500`)
- `CLOB_ORDER_POLL_INTERVAL_MS` (default `700`)
- `CLOB_ORDER_WATCH_INTERVAL_MS` (default `220`)
- `CLOB_MARKET_META_TTL_MS` (default `1500`) market rules/orderbook cache TTL
- `CLOB_MIN_ORDER_SIZE_SHARES` (default `1.0`) fallback only when market meta unavailable
- `CLOB_ENFORCE_MARKETABLE_BUY_MIN=true|false` (default `false`) enforce notional floor for buy orders
- `CLOB_FEE_RATE_BPS` (default `1000`) fallback fee rate, overridden by market fee lookup when available

## Run (dev)

```bash
python3 -m pip install -r ops/clob_gateway/requirements.txt

# export env vars first...
python3 ops/clob_gateway/app.py --host 127.0.0.1 --port 9001
```

## systemd

Use `ops/systemd/clob_gateway.service` and provide an env file at:

- `/etc/polyedge/clob_gateway.env`

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now clob_gateway.service
curl -fsS http://127.0.0.1:9001/health
```
