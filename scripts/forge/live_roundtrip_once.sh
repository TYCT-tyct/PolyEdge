#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-/home/ubuntu/PolyEdge/.env}"
SERVICE_NAME="${SERVICE_NAME:-polyedge-forge-ireland-api.service}"
POSITION_KEY="${POSITION_KEY:-forge:fev1:live:position:5m}"
LIVE_URL="${LIVE_URL:-http://127.0.0.1:9810/api/strategy/paper?market_type=5m&source=live}"
ENTRY_TIMEOUT_SEC="${ENTRY_TIMEOUT_SEC:-420}"
EXIT_TIMEOUT_SEC="${EXIT_TIMEOUT_SEC:-420}"
POLL_SEC="${POLL_SEC:-2}"
PROFILE_OVERRIDE="${PROFILE_OVERRIDE:-}"
BACKUP_FILE="${BACKUP_FILE:-/tmp/polyedge-live-roundtrip-env-backup.txt}"
RESTORE_SERVICE_ON_EXIT=0

remember_env() {
  local key="$1"
  local file="$2"
  if [[ -f "$BACKUP_FILE" ]] && grep -q "^${key}|" "$BACKUP_FILE" 2>/dev/null; then
    return 0
  fi
  if grep -q "^${key}=" "$file" 2>/dev/null; then
    local value
    value="$(grep "^${key}=" "$file" | head -n1 | cut -d= -f2-)"
    printf '%s|set|%s\n' "$key" "$value" >>"$BACKUP_FILE"
  else
    printf '%s|unset|\n' "$key" >>"$BACKUP_FILE"
  fi
}

upsert_env() {
  local key="$1"
  local value="$2"
  local file="$3"
  remember_env "$key" "$file"
  if grep -q "^${key}=" "$file" 2>/dev/null; then
    sed -i "s#^${key}=.*#${key}=${value}#g" "$file"
  else
    printf '%s=%s\n' "$key" "$value" >>"$file"
  fi
}

remove_env() {
  local key="$1"
  local file="$2"
  remember_env "$key" "$file"
  if grep -q "^${key}=" "$file" 2>/dev/null; then
    sed -i "/^${key}=/d" "$file"
  fi
}

restore_env() {
  local file="$1"
  [[ -f "$BACKUP_FILE" ]] || return 0
  tac "$BACKUP_FILE" | while IFS='|' read -r key mode value; do
    sed -i "/^${key}=/d" "$file"
    if [[ "$mode" == "set" ]]; then
      printf '%s=%s\n' "$key" "$value" >>"$file"
    fi
  done
}

restart_api() {
  sudo systemctl restart "$SERVICE_NAME"
  sleep 2
}

cleanup() {
  restore_env "$ENV_FILE"
  if [[ "$RESTORE_SERVICE_ON_EXIT" -eq 1 ]]; then
    restart_api || true
  fi
}
trap cleanup EXIT

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

echo "[roundtrip] ensure live mode enabled (profile is not overridden by default)"
rm -f "$BACKUP_FILE"
if [[ -n "$PROFILE_OVERRIDE" ]]; then
  echo "[roundtrip] override profile -> $PROFILE_OVERRIDE"
  upsert_env "FORGE_STRATEGY_BASE_PROFILE" "$PROFILE_OVERRIDE" "$ENV_FILE"
else
  echo "[roundtrip] keep existing FORGE_STRATEGY_BASE_PROFILE from $ENV_FILE"
fi
upsert_env "FORGE_FEV1_RUNTIME_LOOKBACK_MINUTES" "1440" "$ENV_FILE"
upsert_env "FORGE_FEV1_LIVE_ENABLED" "true" "$ENV_FILE"
upsert_env "FORGE_FEV1_LIVE_EXECUTE" "true" "$ENV_FILE"
upsert_env "FORGE_FEV1_RUNTIME_DRAIN_ONLY" "false" "$ENV_FILE"
upsert_env "FORGE_FEV1_RUNTIME_MAX_ORDERS" "1" "$ENV_FILE"
remove_env "FORGE_FEV1_LIVE_MAX_COMPLETED_TRADES" "$ENV_FILE"
remove_env "FORGE_FEV1_LIVE_MAX_COMPLETED_TRADES_BY_SCOPE" "$ENV_FILE"
upsert_env "FORGE_FEV1_RUNTIME_MARKETS" "5m" "$ENV_FILE"
upsert_env "FORGE_FEV1_MIN_QUOTE_USDC" "0.01" "$ENV_FILE"
upsert_env "FORGE_FEV1_RUNTIME_QUOTE_USDC" "0.01" "$ENV_FILE"
upsert_env "FORGE_FEV1_LIVE_QUOTE_FROM_PRICE" "true" "$ENV_FILE"
RESTORE_SERVICE_ON_EXIT=1
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
