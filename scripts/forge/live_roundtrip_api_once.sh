#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:?ENV_FILE is required}"
SERVICE_NAME="${SERVICE_NAME:-polyedge-forge-ireland-api.service}"
BASE_URL="${BASE_URL:-http://127.0.0.1:9810}"
REDIS_PREFIX="${REDIS_PREFIX:-forge}"
SYMBOL="${SYMBOL:-SOLUSDT}"
MARKET_TYPE="${MARKET_TYPE:-5m}"
ENTRY_TIMEOUT_SEC="${ENTRY_TIMEOUT_SEC:-900}"
EXIT_TIMEOUT_SEC="${EXIT_TIMEOUT_SEC:-900}"
POLL_SEC="${POLL_SEC:-2}"
CANARY_DIR="${CANARY_DIR:-/tmp/polyedge-live-roundtrip}"

POSITION_KEY="${REDIS_PREFIX}:fev1:live:position:${SYMBOL}:${MARKET_TYPE}"
mkdir -p "$CANARY_DIR"
BACKUP_FILE="$CANARY_DIR/env_backup.txt"
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

fetch_field() {
  local field="$1"
  local raw
  raw="$(redis-cli --raw GET "$POSITION_KEY" || true)"
  if [[ -z "$raw" ]]; then
    echo ""
    return 0
  fi
  python3 - <<PY
import json
try:
    d=json.loads('''$raw''')
    v=d.get('$field')
    print('' if v is None else v)
except Exception:
    print('')
PY
}

api_control() {
  local action="$1"
  local note="$2"
  python3 - <<PY
import json
import urllib.request
req = urllib.request.Request(
    "$BASE_URL/api/strategy/live/control",
    data=json.dumps({
        "action": "$action",
        "market_type": "$MARKET_TYPE",
        "note": "$note",
    }).encode(),
    headers={"content-type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(req, timeout=20) as resp:
    print(resp.read().decode())
PY
}

log() {
  printf '[live-roundtrip-api] %s\n' "$*" | tee -a "$CANARY_DIR/roundtrip.log"
}

cleanup() {
  api_control "force_pause" "codex_${SYMBOL}_${MARKET_TYPE}_cleanup_pause" >>"$CANARY_DIR/control.log" 2>&1 || true
  restore_env "$ENV_FILE"
  if [[ "$RESTORE_SERVICE_ON_EXIT" -eq 1 ]]; then
    sudo systemctl restart "$SERVICE_NAME" >>"$CANARY_DIR/control.log" 2>&1 || true
  fi
}
trap cleanup EXIT

log "prepare symbol=$SYMBOL market_type=$MARKET_TYPE env=$ENV_FILE"
rm -f "$BACKUP_FILE"
upsert_env "FORGE_FEV1_RUNTIME_SYMBOL" "$SYMBOL" "$ENV_FILE"
upsert_env "FORGE_FEV1_RUNTIME_MARKETS" "$MARKET_TYPE" "$ENV_FILE"
upsert_env "FORGE_FEV1_RUNTIME_LOOKBACK_MINUTES" "1440" "$ENV_FILE"
upsert_env "FORGE_FEV1_LIVE_EXECUTE" "true" "$ENV_FILE"
upsert_env "FORGE_FEV1_RUNTIME_DRAIN_ONLY" "false" "$ENV_FILE"
upsert_env "FORGE_FEV1_RUNTIME_MAX_ORDERS" "1" "$ENV_FILE"
remove_env "FORGE_FEV1_LIVE_MAX_COMPLETED_TRADES" "$ENV_FILE"
remove_env "FORGE_FEV1_LIVE_MAX_COMPLETED_TRADES_BY_SCOPE" "$ENV_FILE"
upsert_env "FORGE_FEV1_MIN_QUOTE_USDC" "0.01" "$ENV_FILE"
upsert_env "FORGE_FEV1_RUNTIME_QUOTE_USDC" "0.01" "$ENV_FILE"
upsert_env "FORGE_FEV1_LIVE_QUOTE_FROM_PRICE" "true" "$ENV_FILE"
RESTORE_SERVICE_ON_EXIT=1
sudo systemctl restart "$SERVICE_NAME"
sleep 3
api_control "resume_live" "codex_${SYMBOL}_${MARKET_TYPE}_canary_start" | tee -a "$CANARY_DIR/control.log"

entry_deadline=$((SECONDS + ENTRY_TIMEOUT_SEC))
entry_seen=0
while ((SECONDS < entry_deadline)); do
  side="$(fetch_field side)"
  state="$(fetch_field state)"
  last_action="$(fetch_field last_action)"
  printf '[entry-check] t=%ss state=%s side=%s last_action=%s\n' \
    "$SECONDS" "${state:-null}" "${side:-null}" "${last_action:-null}" | tee -a "$CANARY_DIR/roundtrip.log"
  if [[ -n "$side" && "$side" != "None" && "$side" != "null" ]]; then
    entry_seen=1
    break
  fi
  sleep "$POLL_SEC"
done

if [[ "$entry_seen" -ne 1 ]]; then
  log "entry timeout"
  upsert_env "FORGE_FEV1_LIVE_EXECUTE" "false" "$ENV_FILE"
  upsert_env "FORGE_FEV1_RUNTIME_DRAIN_ONLY" "false" "$ENV_FILE"
  sudo systemctl restart "$SERVICE_NAME"
  api_control "force_pause" "codex_${SYMBOL}_${MARKET_TYPE}_entry_timeout" | tee -a "$CANARY_DIR/control.log" || true
  exit 1
fi

entry_side="$(fetch_field side)"
entry_ts="$(fetch_field entry_ts_ms)"
log "entry seen side=$entry_side entry_ts_ms=$entry_ts"

upsert_env "FORGE_FEV1_RUNTIME_DRAIN_ONLY" "true" "$ENV_FILE"
sudo systemctl restart "$SERVICE_NAME"
sleep 3
api_control "resume_live" "codex_${SYMBOL}_${MARKET_TYPE}_canary_drain" | tee -a "$CANARY_DIR/control.log"

exit_deadline=$((SECONDS + EXIT_TIMEOUT_SEC))
exit_seen=0
while ((SECONDS < exit_deadline)); do
  side="$(fetch_field side)"
  state="$(fetch_field state)"
  last_action="$(fetch_field last_action)"
  last_reason="$(fetch_field last_reason)"
  last_exit_ts="$(fetch_field last_exit_ts_ms)"
  printf '[exit-check] t=%ss state=%s side=%s last_action=%s reason=%s exit_ts=%s\n' \
    "$SECONDS" "${state:-null}" "${side:-null}" "${last_action:-null}" "${last_reason:-null}" "${last_exit_ts:-null}" | tee -a "$CANARY_DIR/roundtrip.log"
  if [[ ( -z "$side" || "$side" == "None" || "$side" == "null" ) && ( "$last_action" == exit* || "$last_action" == reduce* || -n "$last_exit_ts" ) ]]; then
    exit_seen=1
    break
  fi
  sleep "$POLL_SEC"
done

upsert_env "FORGE_FEV1_LIVE_EXECUTE" "false" "$ENV_FILE"
upsert_env "FORGE_FEV1_RUNTIME_DRAIN_ONLY" "false" "$ENV_FILE"
sudo systemctl restart "$SERVICE_NAME"
sleep 3
api_control "force_pause" "codex_${SYMBOL}_${MARKET_TYPE}_post_trade_pause" | tee -a "$CANARY_DIR/control.log"

if [[ "$exit_seen" -ne 1 ]]; then
  log "exit timeout"
  exit 2
fi

log "completed roundtrip"
