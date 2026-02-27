#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${REPO_DIR:-/home/ubuntu/PolyEdge}"
DATA_ROOT="${DATA_ROOT:-/data/polyedge-forge}"
USER_NAME="${USER_NAME:-ubuntu}"
ACTIVE_TIMEFRAMES="${ACTIVE_TIMEFRAMES:-5m}"
STRATEGY_MARKETS="${STRATEGY_MARKETS:-5m}"
# Small-capital default: prioritize drawdown control and execution stability.
STRATEGY_BASE_PROFILE="${STRATEGY_BASE_PROFILE:-fev1_manual_hi_win_2026_02_27}"
CAPITAL_BASE_USDC="${CAPITAL_BASE_USDC:-10}"

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
ExecStart=$REPO_DIR/target/release/polyedge_forge ireland-recorder --data-root $DATA_ROOT --udp-bind 0.0.0.0:9801 --sample-ms 100 --supported-symbols BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT --active-symbols BTCUSDT --active-timeframes $ACTIVE_TIMEFRAMES --discovery-refresh-sec 5 --clickhouse-url http://127.0.0.1:8123 --clickhouse-database polyedge_forge --clickhouse-snapshot-table snapshot_100ms --clickhouse-round-table rounds --redis-url redis://127.0.0.1:6379/0 --redis-prefix forge --redis-ttl-sec 7200 --sink-batch-size 200 --sink-flush-ms 1000 --sink-queue-cap 20000 --disable-api --dashboard-dist /home/ubuntu/PolyEdge/heatmap_dashboard/dist
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
Environment=RUST_LOG=info,polyedge_forge=debug
Environment=FORGE_FEV1_RUNTIME_MARKETS=$STRATEGY_MARKETS
Environment=FORGE_STRATEGY_MARKETS=$STRATEGY_MARKETS
Environment=FORGE_STRATEGY_BASE_PROFILE=$STRATEGY_BASE_PROFILE
Environment=FORGE_FEV1_CAPITAL_BASE_USDC=$CAPITAL_BASE_USDC
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
MAX_STALE_MS=30000
MAX_API_RSS_MB=4500

if ! systemctl is-active --quiet "$RECORDER_SERVICE"; then
  systemctl restart "$RECORDER_SERVICE" || true
fi
if ! systemctl is-active --quiet "$API_SERVICE"; then
  systemctl restart "$API_SERVICE" || true
  exit 0
fi

if ! curl -fsS --max-time 2 "$API_URL" >/dev/null; then
  systemctl restart "$API_SERVICE" || true
  exit 0
fi

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

latest_ms="$(clickhouse-client -q "SELECT max(ts_ireland_sample_ms) FROM polyedge_forge.snapshot_100ms WHERE symbol='BTCUSDT' AND timeframe='5m'" 2>/dev/null || echo 0)"
if [[ -z "${latest_ms}" || "${latest_ms}" == "0" ]]; then
  # Recorder may just be starting; skip hard restart.
  exit 0
fi

now_ms="$(date +%s%3N)"
if [[ $((now_ms - latest_ms)) -gt ${MAX_STALE_MS} ]]; then
  systemctl restart "$RECORDER_SERVICE" || true
fi
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
