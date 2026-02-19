#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="polyedge-seat-optimizer.service"
REPO_DIR="${REPO_DIR:-$HOME/PolyEdge}"
PYTHON_BIN="${PYTHON_BIN:-/usr/bin/python3}"
HOST="${POLYEDGE_SEAT_OPTIMIZER_HOST:-127.0.0.1}"
PORT="${POLYEDGE_SEAT_OPTIMIZER_PORT:-8091}"

SERVICE_PATH="/etc/systemd/system/${SERVICE_NAME}"

cat <<EOF | sudo tee "${SERVICE_PATH}" >/dev/null
[Unit]
Description=PolyEdge SEAT Optimizer Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${USER}
WorkingDirectory=${REPO_DIR}
Environment=POLYEDGE_SEAT_OPTIMIZER_HOST=${HOST}
Environment=POLYEDGE_SEAT_OPTIMIZER_PORT=${PORT}
ExecStart=${PYTHON_BIN} ${REPO_DIR}/scripts/seat_optimizer_service.py
Restart=always
RestartSec=2
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=false

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable "${SERVICE_NAME}"
sudo systemctl restart "${SERVICE_NAME}"
sudo systemctl --no-pager --full status "${SERVICE_NAME}" || true
