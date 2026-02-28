#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-/home/ubuntu/PolyEdge/.env}"
SERVICE_NAME="${SERVICE_NAME:-polyedge-forge-ireland-api.service}"
POSITION_KEY="${POSITION_KEY:-forge:fev1:live:position:5m}"
LIVE_URL="${LIVE_URL:-http://127.0.0.1:9810/api/strategy/paper?market_type=5m&source=live}"
ENTRY_TIMEOUT_SEC="${ENTRY_TIMEOUT_SEC:-420}"
EXIT_TIMEOUT_SEC="${EXIT_TIMEOUT_SEC:-420}"
POLL_SEC="${POLL_SEC:-2}"

upsert_env() {
  local key="$1"
  local value="$2"
  local file="$3"
  if grep -q "^${key}=" "$file" 2>/dev/null; then
    sed -i "s#^${key}=.*#${key}=${value}#g" "$file"
  else
    printf '%s=%s\n' "$key" "$value" >>"$file"
  fi
}

restart_api() {
  sudo systemctl restart "$SERVICE_NAME"
  sleep 2
}

fetch_position_field() {
  local field="$1"
  local raw
  raw="$(redis-cli --raw GET "$POSITION_KEY" || true)"
  if [[ -z "$raw" ]]; then
    echo ""
    return 0
  fi
  python3 -c "import json,sys; d=json.loads(sys.stdin.read()); v=d.get('$field'); print('' if v is None else v)" <<<"$raw"
}

echo "[roundtrip] ensure live mode enabled with balanced profile"
upsert_env "FORGE_STRATEGY_BASE_PROFILE" "fev1_manual_balanced_2026_02_28" "$ENV_FILE"
upsert_env "FORGE_FEV1_RUNTIME_LOOKBACK_MINUTES" "1440" "$ENV_FILE"
upsert_env "FORGE_FEV1_LIVE_ENABLED" "true" "$ENV_FILE"
upsert_env "FORGE_FEV1_LIVE_EXECUTE" "true" "$ENV_FILE"
upsert_env "FORGE_FEV1_RUNTIME_DRAIN_ONLY" "false" "$ENV_FILE"
upsert_env "FORGE_FEV1_RUNTIME_MAX_ORDERS" "1" "$ENV_FILE"
upsert_env "FORGE_FEV1_RUNTIME_MARKETS" "5m" "$ENV_FILE"
upsert_env "FORGE_FEV1_MIN_QUOTE_USDC" "0.01" "$ENV_FILE"
upsert_env "FORGE_FEV1_RUNTIME_QUOTE_USDC" "0.01" "$ENV_FILE"
upsert_env "FORGE_FEV1_LIVE_QUOTE_FROM_PRICE" "true" "$ENV_FILE"
upsert_env "FORGE_FEV1_CAPITAL_MAX_ADD_LAYERS" "0" "$ENV_FILE"
restart_api

echo "[roundtrip] wait for entry..."
entry_deadline=$((SECONDS + ENTRY_TIMEOUT_SEC))
entry_seen=0
while ((SECONDS < entry_deadline)); do
  side="$(fetch_position_field side)"
  state="$(fetch_position_field state)"
  last_action="$(fetch_position_field last_action)"
  printf '[entry-check] t=%ss state=%s side=%s last_action=%s\n' "$SECONDS" "${state:-null}" "${side:-null}" "${last_action:-null}"
  if [[ -n "$side" && "$side" != "None" && "$side" != "null" ]]; then
    entry_seen=1
    break
  fi
  sleep "$POLL_SEC"
done

if [[ "$entry_seen" -ne 1 ]]; then
  echo "[roundtrip] entry timeout; disable live execute and abort"
  upsert_env "FORGE_FEV1_LIVE_EXECUTE" "false" "$ENV_FILE"
  upsert_env "FORGE_FEV1_RUNTIME_DRAIN_ONLY" "false" "$ENV_FILE"
  restart_api
  exit 1
fi

entry_side="$(fetch_position_field side)"
entry_ts="$(fetch_position_field entry_ts_ms)"
echo "[roundtrip] entry seen side=$entry_side entry_ts_ms=$entry_ts"

echo "[roundtrip] switch to drain mode to force exit"
upsert_env "FORGE_FEV1_RUNTIME_DRAIN_ONLY" "true" "$ENV_FILE"
restart_api

echo "[roundtrip] wait for exit..."
exit_deadline=$((SECONDS + EXIT_TIMEOUT_SEC))
exit_seen=0
while ((SECONDS < exit_deadline)); do
  side="$(fetch_position_field side)"
  state="$(fetch_position_field state)"
  last_action="$(fetch_position_field last_action)"
  last_reason="$(fetch_position_field last_reason)"
  last_exit_ts="$(fetch_position_field last_exit_ts_ms)"
  printf '[exit-check] t=%ss state=%s side=%s last_action=%s reason=%s exit_ts=%s\n' \
    "$SECONDS" "${state:-null}" "${side:-null}" "${last_action:-null}" "${last_reason:-null}" "${last_exit_ts:-null}"
  if [[ -z "$side" || "$side" == "None" || "$side" == "null" ]]; then
    if [[ "$last_action" == exit* || "$last_action" == reduce* ]]; then
      exit_seen=1
      break
    fi
    # fallback: if exit timestamp exists, treat as completed round-trip
    if [[ -n "$last_exit_ts" && "$last_exit_ts" != "null" ]]; then
      exit_seen=1
      break
    fi
  fi
  sleep "$POLL_SEC"
done

echo "[roundtrip] disable live execute and restore normal mode"
upsert_env "FORGE_FEV1_LIVE_EXECUTE" "false" "$ENV_FILE"
upsert_env "FORGE_FEV1_RUNTIME_DRAIN_ONLY" "false" "$ENV_FILE"
restart_api

if [[ "$exit_seen" -ne 1 ]]; then
  echo "[roundtrip] exit timeout; live execution disabled for safety"
  exit 2
fi

echo "[roundtrip] completed one full entry/exit cycle"
curl -s "$LIVE_URL" | python3 -m json.tool | sed -n '1,220p'
