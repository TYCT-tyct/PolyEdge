#!/usr/bin/env bash
set -euo pipefail

ROLE="${1:-ireland}"
REPO_DIR="${POLYEDGE_REPO_DIR:-$HOME/PolyEdge}"
BIN_PATH="${POLYEDGE_BIN_PATH:-$REPO_DIR/target/release/polyedge_data_backend}"
RUN_USER="${POLYEDGE_USER:-$(id -un)}"

if [[ ! -x "$BIN_PATH" ]]; then
  echo "[setup] missing binary: $BIN_PATH" >&2
  exit 1
fi

sudo mkdir -p /etc/polyedge /var/log/polyedge
sudo chown "$RUN_USER":"$RUN_USER" /var/log/polyedge

# stop old recorder to avoid interference
sudo systemctl disable --now polyedge-recorder.service 2>/dev/null || true

if [[ "$ROLE" == "ireland" ]]; then
  sudo mkdir -p /dev/xvdbb/polyedge-data || true
  sudo chown -R "$RUN_USER":"$RUN_USER" /dev/xvdbb/polyedge-data || true

  sudo cp "$REPO_DIR/ops/systemd/polyedge-data-backend-ireland.service" /etc/systemd/system/
  sudo cp "$REPO_DIR/ops/systemd/polyedge-data-backend-api.service" /etc/systemd/system/

  sudo tee /etc/polyedge/data-backend-ireland.env >/dev/null <<'EOF'
RUST_LOG=info
POLYEDGE_DATA_ROOT=/dev/xvdbb/polyedge-data
POLYEDGE_SYMBOLS=BTCUSDT
POLYEDGE_TIMEFRAMES=5m,15m
POLYEDGE_IRELAND_UDP_BIND=0.0.0.0:9801
POLYEDGE_SNAPSHOT_MS=100
POLYEDGE_DISCOVERY_REFRESH_SEC=10
EOF

  sudo tee /etc/polyedge/data-backend-api.env >/dev/null <<'EOF'
RUST_LOG=info
POLYEDGE_DATA_ROOT=/dev/xvdbb/polyedge-data
POLYEDGE_API_BIND=0.0.0.0:8095
EOF

  sudo systemctl daemon-reload
  sudo systemctl enable --now polyedge-data-backend-ireland.service
  sudo systemctl enable --now polyedge-data-backend-api.service
  sleep 2
  sudo systemctl --no-pager --full status polyedge-data-backend-ireland.service || true
  sudo systemctl --no-pager --full status polyedge-data-backend-api.service || true
else
  sudo cp "$REPO_DIR/ops/systemd/polyedge-data-backend-tokyo.service" /etc/systemd/system/

  sudo tee /etc/polyedge/data-backend-tokyo.env >/dev/null <<'EOF'
RUST_LOG=info
POLYEDGE_DATA_ROOT=/var/lib/polyedge-data
POLYEDGE_SYMBOLS=BTCUSDT
POLYEDGE_TOKYO_BIND=0.0.0.0:0
POLYEDGE_IRELAND_UDP=10.0.3.123:9801
EOF

  sudo mkdir -p /var/lib/polyedge-data
  sudo chown -R "$RUN_USER":"$RUN_USER" /var/lib/polyedge-data

  sudo systemctl daemon-reload
  sudo systemctl enable --now polyedge-data-backend-tokyo.service
  sleep 2
  sudo systemctl --no-pager --full status polyedge-data-backend-tokyo.service || true
fi
