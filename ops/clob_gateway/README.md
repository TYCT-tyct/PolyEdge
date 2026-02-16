# clob_gateway

Local (127.0.0.1) HTTP gateway that signs/orders via `py-clob-client`, so the Rust
engine does not need to implement EIP-712 signing + CLOB auth.

## Endpoints

- `GET /health`
- `POST /orders`
- `DELETE /orders/{order_id}`
- `POST /flatten`

## Environment Variables (required)

- `CLOB_HOST` (default: `https://clob.polymarket.com`)
- `CLOB_CHAIN_ID` (default: `137`)
- `CLOB_PRIVATE_KEY`

API creds (recommended; if missing we try to derive on startup):

- `CLOB_API_KEY`
- `CLOB_API_SECRET`
- `CLOB_API_PASSPHRASE`

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

