#!/usr/bin/env bash
set -euo pipefail

# PolyEdge HFT kernel tuning for UDP relay path.
# Usage:
#   sudo bash scripts/hft_kernel_tune.sh

if [[ "${EUID}" -ne 0 ]]; then
  echo "run as root: sudo bash scripts/hft_kernel_tune.sh" >&2
  exit 1
fi

SYSCTL_FILE="/etc/sysctl.d/99-polyedge-hft.conf"
CHRONY_FILE="/etc/chrony/sources.d/aws-time.sources"

cat > "${SYSCTL_FILE}" <<'EOF'
# PolyEdge HFT UDP relay tuning
net.core.rmem_max=26214400
net.core.rmem_default=26214400
net.core.wmem_max=26214400
net.core.wmem_default=26214400
net.core.default_qdisc=fq
net.ipv4.tcp_congestion_control=bbr
EOF

sysctl --system >/dev/null
echo "applied sysctl profile: ${SYSCTL_FILE}"

mkdir -p "$(dirname "${CHRONY_FILE}")"
cat > "${CHRONY_FILE}" <<'EOF'
# AWS Time Sync Service
server 169.254.169.123 prefer iburst minpoll 4 maxpoll 4
EOF

if systemctl is-active --quiet chronyd; then
  systemctl restart chronyd
elif systemctl is-active --quiet chrony; then
  systemctl restart chrony
fi

echo "updated chrony source: ${CHRONY_FILE}"
chronyc sources -v || true

echo "done"
