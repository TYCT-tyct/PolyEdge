#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${REPO_DIR:-/home/ubuntu/PolyEdge}"
USER_NAME="${USER_NAME:-ubuntu}"
IRELAND_PRIVATE_UDP="${IRELAND_PRIVATE_UDP:-10.0.3.123:9801}"
TOKYO_SYMBOLS="${TOKYO_SYMBOLS:-BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT}"
POLYEDGE_RELEASE_COMMIT="${POLYEDGE_RELEASE_COMMIT:-local-dev}"
RELEASE_ROOT="${RELEASE_ROOT:-/home/${USER_NAME}/polyedge-releases}"

echo "[forge-tokyo] repo=$REPO_DIR user=$USER_NAME ireland_udp=$IRELAND_PRIVATE_UDP release_commit=$POLYEDGE_RELEASE_COMMIT"

if [ -d /var/lib/polyedge-data ]; then
  sudo rm -rf /var/lib/polyedge-data/*
fi

cd "$REPO_DIR"
~/.cargo/bin/cargo build -p polyedge_forge --release

sudo systemctl stop polyedge-feeder.service polyedge-data-backend-tokyo.service 2>/dev/null || true
sudo systemctl disable polyedge-feeder.service polyedge-data-backend-tokyo.service 2>/dev/null || true

sudo tee /etc/systemd/system/polyedge-forge-tokyo.service >/dev/null <<UNIT
[Unit]
Description=PolyEdge Forge Tokyo Relay
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$USER_NAME
WorkingDirectory=$REPO_DIR
Environment=RUST_LOG=info,polyedge_forge=debug
Environment=POLYEDGE_RELEASE_COMMIT=$POLYEDGE_RELEASE_COMMIT
ExecStart=$REPO_DIR/target/release/polyedge_forge tokyo-relay --symbols $TOKYO_SYMBOLS --bind 0.0.0.0:0 --ireland-udp $IRELAND_PRIVATE_UDP --redundancy 2
Restart=always
RestartSec=2
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT

sudo systemctl daemon-reload
sudo systemctl enable polyedge-forge-tokyo.service
sudo systemctl restart polyedge-forge-tokyo.service
sudo systemctl --no-pager --full status polyedge-forge-tokyo.service | sed -n '1,25p'

sudo install -m 0755 "$REPO_DIR/scripts/forge/tokyo_disk_guard.sh" /usr/local/bin/polyedge-tokyo-disk-guard.sh
sudo cp "$REPO_DIR/ops/systemd/polyedge-tokyo-disk-guard.service" /etc/systemd/system/
sudo cp "$REPO_DIR/ops/systemd/polyedge-tokyo-disk-guard.timer" /etc/systemd/system/
sudo sed -i "s#^Environment=REPO_DIR=.*#Environment=REPO_DIR=$REPO_DIR#" /etc/systemd/system/polyedge-tokyo-disk-guard.service
sudo sed -i "s#^Environment=RELEASE_ROOT=.*#Environment=RELEASE_ROOT=$RELEASE_ROOT#" /etc/systemd/system/polyedge-tokyo-disk-guard.service
sudo systemctl daemon-reload
sudo systemctl enable --now polyedge-tokyo-disk-guard.timer
sudo systemctl --no-pager --full status polyedge-tokyo-disk-guard.timer | sed -n '1,15p'

echo "[forge-tokyo] done"
