#!/usr/bin/env bash
set -euo pipefail

# Install and enable a stable app_runner service so reboot does not leave 8080 down.
# Usage:
#   bash scripts/setup_app_runner_systemd.sh
# Optional env:
#   POLYEDGE_REPO_DIR=/home/ubuntu/PolyEdge
#   POLYEDGE_BIN_PATH=/home/ubuntu/PolyEdge/target/release/app_runner
#   POLYEDGE_USER=ubuntu

REPO_DIR="${POLYEDGE_REPO_DIR:-$HOME/PolyEdge}"
BIN_PATH="${POLYEDGE_BIN_PATH:-$REPO_DIR/target/release/app_runner}"
RUN_USER="${POLYEDGE_USER:-$(id -un)}"
ENV_FILE="/etc/polyedge/app_runner.env"
SERVICE_FILE="/etc/systemd/system/polyedge.service"

echo "[setup] repo=$REPO_DIR"
echo "[setup] bin=$BIN_PATH"
echo "[setup] user=$RUN_USER"

if [[ ! -x "$BIN_PATH" ]]; then
  echo "[setup] build release binary first: cargo build -p app_runner --release" >&2
  exit 1
fi

sudo mkdir -p /etc/polyedge
if [[ ! -f "$ENV_FILE" ]]; then
  sudo tee "$ENV_FILE" >/dev/null <<'EOF'
# PolyEdge app_runner runtime env
RUST_LOG=info
POLYEDGE_ENABLE_BINANCE_WS=true
POLYEDGE_ENABLE_BYBIT_WS=false
POLYEDGE_ENABLE_COINBASE_WS=false
POLYEDGE_ENABLE_CHAINLINK_ANCHOR=true
POLYEDGE_ACK_ONLY_PROBE_ENABLED=false
POLYEDGE_ACK_ONLY_PROBE_EVERY=20
POLYEDGE_UDP_USER_SPIN=true
POLYEDGE_UDP_BUSY_POLL_US=50
POLYEDGE_UDP_RCVBUF_BYTES=33554432
POLYEDGE_UDP_PIN_CORES=6666:0,6667:0,6668:0,6669:0
TOKIO_WORKER_THREADS=3
EOF
  echo "[setup] wrote default env: $ENV_FILE"
else
  echo "[setup] keep existing env: $ENV_FILE"
fi

ensure_env_line() {
  local key="$1"
  local value="$2"
  if ! sudo grep -qE "^${key}=" "$ENV_FILE"; then
    echo "[setup] append default: ${key}=${value}"
    echo "${key}=${value}" | sudo tee -a "$ENV_FILE" >/dev/null
  fi
}

ensure_env_line "POLYEDGE_ACK_ONLY_PROBE_ENABLED" "false"
ensure_env_line "POLYEDGE_ACK_ONLY_PROBE_EVERY" "20"
ensure_env_line "POLYEDGE_UDP_USER_SPIN" "true"
ensure_env_line "POLYEDGE_UDP_BUSY_POLL_US" "50"
ensure_env_line "POLYEDGE_UDP_RCVBUF_BYTES" "33554432"
ensure_env_line "POLYEDGE_UDP_PIN_CORES" "6666:0,6667:0,6668:0,6669:0"
ensure_env_line "TOKIO_WORKER_THREADS" "3"

sudo tee "$SERVICE_FILE" >/dev/null <<EOF
[Unit]
Description=PolyEdge app_runner
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$RUN_USER
WorkingDirectory=$REPO_DIR
EnvironmentFile=$ENV_FILE
ExecStart=$BIN_PATH
Restart=always
RestartSec=2
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl stop polyedge.service || true
pkill -x app_runner || true
sleep 1
sudo systemctl enable --now polyedge.service
sleep 2
sudo systemctl --no-pager --full status polyedge.service || true
curl -fsS http://127.0.0.1:8080/health || {
  echo "[setup] health check failed after service start" >&2
  exit 2
}
echo
echo "[setup] polyedge.service installed and healthy"
