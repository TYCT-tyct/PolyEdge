#!/usr/bin/env bash
set -euo pipefail

API_BASE="${API_BASE:-http://127.0.0.1:9810}"
RECORDER_SERVICE="${RECORDER_SERVICE:-polyedge-forge-ireland-recorder.service}"
API_SERVICE="${API_SERVICE:-polyedge-forge-ireland-api.service}"
WINDOW_SEC="${WINDOW_SEC:-180}"
BRONZE_ROOT="${BRONZE_ROOT:-/data/polyedge-forge/bronze}"

echo "[verify-ireland] recorder=$(systemctl is-active "$RECORDER_SERVICE") api=$(systemctl is-active "$API_SERVICE")"

curl -fsS "${API_BASE}/health/live" >/dev/null

python3 - <<'PY'
import json, os, sys, urllib.request

base = os.environ.get("API_BASE", "http://127.0.0.1:9810")
window_sec = os.environ.get("WINDOW_SEC", "180")

collector = json.load(urllib.request.urlopen(f"{base}/api/collector/status?symbol=BTCUSDT", timeout=10))
if not collector.get("ok"):
    raise SystemExit("collector status is not ok for BTCUSDT")

source = json.load(urllib.request.urlopen(f"{base}/api/source_health?window_sec={window_sec}", timeout=10))
if not source.get("ok"):
    raise SystemExit("source health overall is not ok")

print("[verify-ireland] collector BTCUSDT ok")
for row in source.get("symbols", []):
    tf5 = row["timeframes"]["5m"]["status"]
    tf15 = row["timeframes"]["15m"]["status"]
    print(f"[verify-ireland] {row['symbol']} ok={row['ok']} 5m={tf5} 15m={tf15}")
PY

if [ -d "$BRONZE_ROOT" ]; then
  bronze_count="$(find "$BRONZE_ROOT" -type f | wc -l | xargs)"
  echo "[verify-ireland] bronze_files=${bronze_count}"
fi

echo "[verify-ireland] done"
