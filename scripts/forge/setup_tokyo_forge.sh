#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${REPO_DIR:-/home/ubuntu/PolyEdge}"
USER_NAME="${USER_NAME:-ubuntu}"
IRELAND_PRIVATE_UDP="${IRELAND_PRIVATE_UDP:-10.0.3.123:9801}"
TOKYO_SYMBOLS="${TOKYO_SYMBOLS:-BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT}"

echo "[forge-tokyo] repo=$REPO_DIR user=$USER_NAME ireland_udp=$IRELAND_PRIVATE_UDP"

sudo systemctl stop polyedge-feeder.service polyedge-data-backend-tokyo.service 2>/dev/null || true
sudo systemctl disable polyedge-feeder.service polyedge-data-backend-tokyo.service 2>/dev/null || true

if [ -d /var/lib/polyedge-data ]; then
  sudo rm -rf /var/lib/polyedge-data/*
fi

cd "$REPO_DIR"
~/.cargo/bin/cargo build -p polyedge_forge --release
chmod +x "$REPO_DIR/scripts/forge/tokyo_disk_guard.sh"

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
Environment=FORGE_TOKYO_REPLAY_CACHE_SIZE=65536
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

sudo cp "$REPO_DIR/ops/systemd/polyedge-tokyo-disk-guard.service" /etc/systemd/system/
sudo cp "$REPO_DIR/ops/systemd/polyedge-tokyo-disk-guard.timer" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now polyedge-tokyo-disk-guard.timer
sudo systemctl --no-pager --full status polyedge-tokyo-disk-guard.timer | sed -n '1,15p'

echo "[forge-tokyo] done"
