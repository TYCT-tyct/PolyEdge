#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${POLYEDGE_REPO_DIR:-$HOME/PolyEdge}"
BIN_PATH="${POLYEDGE_BIN_PATH:-$REPO_DIR/target/release/polyedge}"
RUN_USER="${POLYEDGE_USER:-$(id -un)}"
ENV_FILE="/etc/polyedge/recorder.env"
SERVICE_FILE="/etc/systemd/system/polyedge-recorder.service"

if [[ ! -x "$BIN_PATH" ]]; then
  echo "[setup] missing binary: $BIN_PATH" >&2
  exit 1
fi

sudo mkdir -p /etc/polyedge
sudo mkdir -p /var/log/polyedge
sudo chown "$RUN_USER":"$RUN_USER" /var/log/polyedge

if [[ ! -f "$ENV_FILE" ]]; then
  sudo tee "$ENV_FILE" >/dev/null <<'EOF'
RUST_LOG=info
POLYEDGE_RECORDER_ROOT=/home/ubuntu/PolyEdge/datasets/recorder
POLYEDGE_RECORDER_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT
POLYEDGE_RECORDER_MARKET_TYPES=updown
POLYEDGE_RECORDER_TIMEFRAMES=5m,15m
POLYEDGE_RECORDER_WRITE_THROTTLE_MS=40
POLYEDGE_RECORDER_META_REFRESH_SEC=20
POLYEDGE_RECORDER_STATUS_INTERVAL_SEC=5
POLYEDGE_STORAGE_MAX_USED_PCT=90
POLYEDGE_STORAGE_KEEP_RAW_DAYS=2
POLYEDGE_STORAGE_KEEP_REPORTS_DAYS=14
POLYEDGE_STORAGE_GC_INTERVAL_SEC=300
EOF
fi

sudo tee "$SERVICE_FILE" >/dev/null <<EOF
[Unit]
Description=PolyEdge Market Tape Recorder
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$RUN_USER
WorkingDirectory=$REPO_DIR
EnvironmentFile=-$ENV_FILE
ExecStart=$BIN_PATH recorder run --dataset-root $REPO_DIR/datasets/recorder
Restart=always
RestartSec=3
LimitNOFILE=1048576
StandardOutput=append:/var/log/polyedge/recorder.log
StandardError=append:/var/log/polyedge/recorder.err.log

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now polyedge-recorder.service
sleep 2
sudo systemctl --no-pager --full status polyedge-recorder.service || true
