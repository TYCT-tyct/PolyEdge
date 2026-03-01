#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${REPO_DIR:-/home/ubuntu/PolyEdge}"
DATA_ROOT="${DATA_ROOT:-/data/polyedge-forge}"
USER_NAME="${USER_NAME:-ubuntu}"
ACTIVE_SYMBOLS="${ACTIVE_SYMBOLS:-BTCUSDT}"
ACTIVE_TIMEFRAMES="${ACTIVE_TIMEFRAMES:-5m,15m}"
ACTIVE_SYMBOL_TIMEFRAMES="${ACTIVE_SYMBOL_TIMEFRAMES:-BTCUSDT:5m|15m}"
RUNTIME_MARKETS="${RUNTIME_MARKETS:-5m}"
STRATEGY_MARKETS="${STRATEGY_MARKETS:-5m,15m}"
STRATEGY_BASE_PROFILE="${STRATEGY_BASE_PROFILE:-fev1_cand_growth_mix_2026_02_28}"
RUNTIME_LOOKBACK_MINUTES="${RUNTIME_LOOKBACK_MINUTES:-1440}"
RUNTIME_MAX_POINTS="${RUNTIME_MAX_POINTS:-140000}"
RUNTIME_MAX_TRADES="${RUNTIME_MAX_TRADES:-0}"
RUNTIME_LIVE_EXECUTE="${RUNTIME_LIVE_EXECUTE:-false}"
DISCOVERY_REFRESH_SEC="${DISCOVERY_REFRESH_SEC:-5}"
DISCOVERY_MARKETS_PER_TEMPLATE="${DISCOVERY_MARKETS_PER_TEMPLATE:-2}"
MARKET_WS_REFRESH_SEC="${MARKET_WS_REFRESH_SEC:-30}"
TOKYO_INPUT_STALE_GUARD_MS="${TOKYO_INPUT_STALE_GUARD_MS:-2500}"
MARKET_SAMPLE_END_GRACE_MS="${MARKET_SAMPLE_END_GRACE_MS:-300}"

echo "[forge-ireland] repo=$REPO_DIR data_root=$DATA_ROOT user=$USER_NAME"

sudo systemctl stop \
  polyedge-forge-ireland.service \
  polyedge-forge-ireland-recorder.service \
  polyedge-forge-ireland-api.service \
  polyedge-data-backend-ireland.service \
  polyedge-data-backend-api.service \
  polyedge-recorder.service \
  polyedge.service 2>/dev/null || true

sudo systemctl disable \
  polyedge-forge-ireland.service \
  polyedge-forge-ireland-recorder.service \
  polyedge-forge-ireland-api.service \
  polyedge-data-backend-ireland.service \
  polyedge-data-backend-api.service \
  polyedge-recorder.service \
  polyedge.service 2>/dev/null || true

# Legacy dataset cleanup requested by operator.
if [ -d /data/polyedge-data ]; then
  sudo rm -rf /data/polyedge-data/*
fi

sudo mkdir -p "$DATA_ROOT"
sudo chown -R "$USER_NAME:$USER_NAME" "$DATA_ROOT"

cd "$REPO_DIR"
~/.cargo/bin/cargo build -p polyedge_forge --release

# Keep service-level defaults in sync even when .env overrides are present.
ENV_FILE="$REPO_DIR/.env"
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
if [ -f "$ENV_FILE" ]; then
  upsert_env "FORGE_STRATEGY_BASE_PROFILE" "$STRATEGY_BASE_PROFILE" "$ENV_FILE"
  upsert_env "FORGE_FEV1_RUNTIME_MARKETS" "$RUNTIME_MARKETS" "$ENV_FILE"
  upsert_env "FORGE_FEV1_RUNTIME_LOOKBACK_MINUTES" "$RUNTIME_LOOKBACK_MINUTES" "$ENV_FILE"
  upsert_env "FORGE_FEV1_RUNTIME_MAX_POINTS" "$RUNTIME_MAX_POINTS" "$ENV_FILE"
  upsert_env "FORGE_FEV1_RUNTIME_MAX_TRADES" "$RUNTIME_MAX_TRADES" "$ENV_FILE"
  upsert_env "FORGE_FEV1_LIVE_EXECUTE" "$RUNTIME_LIVE_EXECUTE" "$ENV_FILE"
  upsert_env "FORGE_FEV1_RUNTIME_DRAIN_ONLY" "false" "$ENV_FILE"
fi

# Always rebuild dashboard static assets before restarting API service.
# This prevents stale /dashboard bundles after backend-only deploys.
if command -v npm >/dev/null 2>&1; then
  pushd "$REPO_DIR/heatmap_dashboard" >/dev/null
  npm install --no-audit --no-fund
  npm run build
  popd >/dev/null
else
  echo "[forge-ireland] warning: npm not found; dashboard dist may be stale"
fi

# Remove stale systemd drop-in overrides so deploy script remains source of truth.
sudo rm -rf /etc/systemd/system/polyedge-forge-ireland-recorder.service.d
sudo rm -rf /etc/systemd/system/polyedge-forge-ireland-api.service.d

sudo tee /etc/systemd/system/polyedge-forge-ireland-recorder.service >/dev/null <<UNIT
[Unit]
Description=PolyEdge Forge Ireland Recorder
After=network-online.target
Wants=network-online.target
After=clickhouse-server.service redis-server.service
Wants=clickhouse-server.service redis-server.service

[Service]
Type=simple
User=$USER_NAME
WorkingDirectory=$REPO_DIR
Environment=RUST_LOG=info,polyedge_forge=debug
Environment=POLYEDGE_MARKET_REFRESH_SEC=$MARKET_WS_REFRESH_SEC
Environment=POLYEDGE_TARGET_MARKET_CACHE_FILE=/data/polyedge-forge/cache/target_market_cache.json
Environment=POLYEDGE_DISCOVERY_NEAR_EXPIRY_ONLY=true
Environment=POLYEDGE_DISCOVERY_MARKETS_PER_TEMPLATE=$DISCOVERY_MARKETS_PER_TEMPLATE
Environment=POLYEDGE_DISCOVERY_MAX_FUTURE_MS_5M=900000
Environment=POLYEDGE_DISCOVERY_MAX_FUTURE_MS_15M=2700000
Environment=POLYEDGE_DISCOVERY_ENDDATE_LIMIT=500
Environment=POLYEDGE_DISCOVERY_ENDDATE_MAX_PAGES=16
Environment=POLYEDGE_DISCOVERY_ENDDATE_DESC_LIMIT=400
Environment=POLYEDGE_DISCOVERY_ENDDATE_DESC_MAX_PAGES=12
Environment=POLYEDGE_DISCOVERY_VOLUME_MAX_PAGES=6
Environment=POLYEDGE_DISCOVERY_VOLUME_LIMIT=300
Environment=POLYEDGE_DISCOVERY_VOLUME_FALLBACK_PAGES=0
Environment=POLYEDGE_DISCOVERY_MAX_PAST_MS=60000
Environment="POLYEDGE_DISCOVERY_USER_AGENT=Mozilla/5.0 (compatible; PolyEdgeBot/1.0; +https://github.com/TYCT-tyct/PolyEdge)"
Environment=FORGE_TOKYO_INPUT_STALE_GUARD_MS=$TOKYO_INPUT_STALE_GUARD_MS
Environment=FORGE_MARKET_FUTURE_GUARD_MS=3600000
Environment=FORGE_MARKET_PRESTART_ALLOW_MS=30000
Environment=FORGE_MARKET_SAMPLE_END_GRACE_MS=$MARKET_SAMPLE_END_GRACE_MS
Environment=FORGE_CHAINLINK_ENABLED=false
Environment=POLYEDGE_ENABLE_CHAINLINK_ANCHOR=false
Environment=POLYEDGE_MARKET_STALE_RECONNECT_RATIO=0.75
ExecStart=$REPO_DIR/target/release/polyedge_forge ireland-recorder --data-root $DATA_ROOT --udp-bind 0.0.0.0:9801 --sample-ms 100 --supported-symbols BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT --active-symbols $ACTIVE_SYMBOLS --active-timeframes $ACTIVE_TIMEFRAMES --active-symbol-timeframes $ACTIVE_SYMBOL_TIMEFRAMES --discovery-refresh-sec $DISCOVERY_REFRESH_SEC --clickhouse-url http://127.0.0.1:8123 --clickhouse-database polyedge_forge --clickhouse-snapshot-table snapshot_100ms --clickhouse-round-table rounds --redis-url redis://127.0.0.1:6379/0 --redis-prefix forge --redis-ttl-sec 7200 --sink-batch-size 200 --sink-flush-ms 1000 --sink-queue-cap 20000 --disable-api --dashboard-dist /home/ubuntu/PolyEdge/heatmap_dashboard/dist
Restart=always
RestartSec=2
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT

sudo tee /etc/systemd/system/polyedge-forge-ireland-api.service >/dev/null <<UNIT
[Unit]
Description=PolyEdge Forge Ireland API + Dashboard
After=network-online.target
Wants=network-online.target
After=clickhouse-server.service redis-server.service polyedge-forge-ireland-recorder.service
Wants=clickhouse-server.service redis-server.service

[Service]
Type=simple
User=$USER_NAME
WorkingDirectory=$REPO_DIR
EnvironmentFile=-$REPO_DIR/.env
Environment=RUST_LOG=info,polyedge_forge=debug
Environment=FORGE_FEV1_RUNTIME_MARKETS=$RUNTIME_MARKETS
Environment=FORGE_FEV1_RUNTIME_LOOKBACK_MINUTES=$RUNTIME_LOOKBACK_MINUTES
Environment=FORGE_FEV1_RUNTIME_LOOP_MS=450
Environment=FORGE_FEV1_RUNTIME_MAX_POINTS=$RUNTIME_MAX_POINTS
Environment=FORGE_FEV1_RUNTIME_MAX_TRADES=$RUNTIME_MAX_TRADES
Environment=FORGE_STRATEGY_MARKETS=$STRATEGY_MARKETS
Environment=FORGE_STRATEGY_BASE_PROFILE=$STRATEGY_BASE_PROFILE
Environment=FORGE_FEV1_EXECUTOR=rust_sdk
Environment=FORGE_FEV1_CAPITAL_USE_REAL_BALANCE=true
Environment=FORGE_FEV1_CAPITAL_HARD_REQUIRE_REAL_BALANCE=true
Environment=FORGE_FEV1_MIN_QUOTE_USDC=0.01
Environment=FORGE_FEV1_RUNTIME_QUOTE_USDC=0.01
Environment=FORGE_FEV1_LIVE_QUOTE_FROM_PRICE=true
Environment=FORGE_FEV1_CAPITAL_MAX_ADD_LAYERS=0
Environment=FORGE_STRATEGY_MAX_POINTS_FULL=320000
Environment=FORGE_STRATEGY_MAX_POINTS_SHORT=180000
Environment=FORGE_STRATEGY_MAX_POINTS_HARD_CAP=600000
Environment=MALLOC_TRIM_THRESHOLD_=131072
ExecStart=$REPO_DIR/target/release/polyedge_forge ireland-api --bind 0.0.0.0:9810 --clickhouse-url http://127.0.0.1:8123 --redis-url redis://127.0.0.1:6379/0 --redis-prefix forge --dashboard-dist /home/ubuntu/PolyEdge/heatmap_dashboard/dist
Restart=always
RestartSec=2
LimitNOFILE=1048576
MemoryHigh=6G
MemoryMax=7G

[Install]
WantedBy=multi-user.target
UNIT

# Self-healing healthcheck: restart recorder when API stalls or snapshots stop updating.
sudo tee /usr/local/bin/polyedge_forge_healthcheck.sh >/dev/null <<'HC'
#!/usr/bin/env bash
set -euo pipefail

RECORDER_SERVICE="polyedge-forge-ireland-recorder.service"
API_SERVICE="polyedge-forge-ireland-api.service"
API_URL="http://127.0.0.1:9810/health/live"
MAX_API_RSS_MB=4500
API_MAX_TIME_SEC="${HEALTHCHECK_API_MAX_TIME_SEC:-6}"
API_FAIL_THRESHOLD="${HEALTHCHECK_API_FAIL_THRESHOLD:-3}"
API_FAIL_COUNT_FILE="/run/polyedge_forge_api_health_fail.count"
STALE_MAX_AGE_MS="${HEALTHCHECK_STALE_MAX_AGE_MS:-30000}"
STALE_FAIL_THRESHOLD="${HEALTHCHECK_STALE_FAIL_THRESHOLD:-3}"
STALE_FAIL_COUNT_FILE="/run/polyedge_forge_stale_fail.count"
STALE_RESTART_ENABLED="${HEALTHCHECK_STALE_RESTART_ENABLED:-true}"
STALE_TIMEFRAMES="${HEALTHCHECK_STALE_TIMEFRAMES:-5m,15m}"

if ! systemctl is-active --quiet "$RECORDER_SERVICE"; then
  systemctl restart "$RECORDER_SERVICE" || true
fi
if ! systemctl is-active --quiet "$API_SERVICE"; then
  systemctl restart "$API_SERVICE" || true
  exit 0
fi

if ! curl -fsS --max-time "$API_MAX_TIME_SEC" "$API_URL" >/dev/null; then
  fail_count=0
  if [[ -r "$API_FAIL_COUNT_FILE" ]]; then
    fail_count="$(cat "$API_FAIL_COUNT_FILE" 2>/dev/null || echo 0)"
  fi
  if ! [[ "$fail_count" =~ ^[0-9]+$ ]]; then
    fail_count=0
  fi
  fail_count=$((fail_count + 1))
  echo "$fail_count" > "$API_FAIL_COUNT_FILE"
  if [[ "$fail_count" -ge "$API_FAIL_THRESHOLD" ]]; then
    systemctl restart "$API_SERVICE" || true
    echo 0 > "$API_FAIL_COUNT_FILE"
  fi
  exit 0
fi
echo 0 > "$API_FAIL_COUNT_FILE"

api_pid="$(systemctl show -p MainPID --value "$API_SERVICE" 2>/dev/null || echo 0)"
if [[ "${api_pid}" =~ ^[0-9]+$ ]] && [[ "${api_pid}" -gt 1 ]] && [[ -r "/proc/${api_pid}/status" ]]; then
  api_rss_kb="$(awk '/VmRSS:/ {print $2}' "/proc/${api_pid}/status" 2>/dev/null || echo 0)"
  if [[ "${api_rss_kb}" =~ ^[0-9]+$ ]] && [[ "${api_rss_kb}" -gt 0 ]]; then
    api_rss_mb=$((api_rss_kb / 1024))
    if [[ "${api_rss_mb}" -gt "${MAX_API_RSS_MB}" ]]; then
      systemctl restart "$API_SERVICE" || true
      exit 0
    fi
  fi
fi

now_ms="$(date +%s%3N)"
stale_hit=0
stale_details=""

IFS=',' read -r -a stale_timeframe_arr <<<"${STALE_TIMEFRAMES}"
for tf in "${stale_timeframe_arr[@]}"; do
  tf="$(echo "${tf}" | tr '[:upper:]' '[:lower:]' | xargs)"
  if [[ -z "${tf}" ]]; then
    continue
  fi
  latest_ms="$(clickhouse-client -q "SELECT max(ts_ireland_sample_ms) FROM polyedge_forge.snapshot_100ms WHERE symbol='BTCUSDT' AND timeframe='${tf}'" 2>/dev/null || echo 0)"
  if [[ -z "${latest_ms}" || "${latest_ms}" == "0" ]]; then
    # Recorder may just be starting or this timeframe has not emitted yet.
    continue
  fi
  age_ms=$((now_ms - latest_ms))
  if [[ "${age_ms}" -gt "${STALE_MAX_AGE_MS}" ]]; then
    stale_hit=1
    stale_details="${stale_details}${tf}:${age_ms}ms "
  fi
done

if [[ "${stale_hit}" -eq 1 ]]; then
  stale_count=0
  if [[ -r "$STALE_FAIL_COUNT_FILE" ]]; then
    stale_count="$(cat "$STALE_FAIL_COUNT_FILE" 2>/dev/null || echo 0)"
  fi
  if ! [[ "$stale_count" =~ ^[0-9]+$ ]]; then
    stale_count=0
  fi
  stale_count=$((stale_count + 1))
  echo "$stale_count" > "$STALE_FAIL_COUNT_FILE"
  if [[ "$stale_count" -ge "$STALE_FAIL_THRESHOLD" ]]; then
    logger -t polyedge_forge_healthcheck "stale data detected (${stale_details}) count=${stale_count}, restarting=${STALE_RESTART_ENABLED}"
    if [[ "${STALE_RESTART_ENABLED,,}" == "1" || "${STALE_RESTART_ENABLED,,}" == "true" || "${STALE_RESTART_ENABLED,,}" == "yes" || "${STALE_RESTART_ENABLED,,}" == "on" ]]; then
      systemctl restart "$RECORDER_SERVICE" || true
    fi
    echo 0 > "$STALE_FAIL_COUNT_FILE"
  fi
  exit 0
fi
echo 0 > "$STALE_FAIL_COUNT_FILE"
HC
sudo chmod +x /usr/local/bin/polyedge_forge_healthcheck.sh

sudo tee /etc/systemd/system/polyedge-forge-healthcheck.service >/dev/null <<UNIT
[Unit]
Description=PolyEdge Forge Ireland Healthcheck
After=network-online.target clickhouse-server.service redis-server.service
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=/usr/local/bin/polyedge_forge_healthcheck.sh
UNIT

sudo tee /etc/systemd/system/polyedge-forge-healthcheck.timer >/dev/null <<UNIT
[Unit]
Description=Run PolyEdge Forge healthcheck every 30 seconds

[Timer]
OnBootSec=45
OnUnitActiveSec=30
AccuracySec=5
Unit=polyedge-forge-healthcheck.service

[Install]
WantedBy=timers.target
UNIT

sudo systemctl daemon-reload
sudo systemctl enable polyedge-forge-ireland-recorder.service
sudo systemctl enable polyedge-forge-ireland-api.service
sudo systemctl restart polyedge-forge-ireland-recorder.service
sudo systemctl restart polyedge-forge-ireland-api.service
sudo systemctl enable --now polyedge-forge-healthcheck.timer
sudo systemctl --no-pager --full status polyedge-forge-ireland-recorder.service | sed -n '1,25p'
sudo systemctl --no-pager --full status polyedge-forge-ireland-api.service | sed -n '1,25p'
sudo systemctl --no-pager --full status polyedge-forge-healthcheck.timer | sed -n '1,20p'

echo "[forge-ireland] done"
