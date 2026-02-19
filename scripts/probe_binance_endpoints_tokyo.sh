#!/usr/bin/env bash
set -euo pipefail

# Quick host RTT probe for Tokyo feeder. Run directly on Tokyo EC2.
# Example:
#   bash scripts/probe_binance_endpoints_tokyo.sh
#   PING_COUNT=60 bash scripts/probe_binance_endpoints_tokyo.sh stream.binance.com stream1.binance.com ws-api.binance.com

COUNT="${PING_COUNT:-100}"
if [[ $# -gt 0 ]]; then
  HOSTS=("$@")
else
  HOSTS=("stream.binance.com" "stream1.binance.com" "ws-api.binance.com")
fi

echo "probe_count=$COUNT"
echo "ts_utc=$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

for host in "${HOSTS[@]}"; do
  echo "=== $host ==="
  # Keep script alive even if one host has intermittent packet loss.
  if ! out="$(ping -c "$COUNT" "$host" 2>/dev/null || true)"; then
    echo "status=error"
    continue
  fi
  if [[ -z "$out" ]]; then
    echo "status=no_output"
    continue
  fi

  # Parse icmp_seq lines to extract all RTT samples.
  samples="$(echo "$out" | awk -F'time=' '/time=/{print $2}' | awk '{print $1}')"
  if [[ -z "$samples" ]]; then
    echo "status=no_samples"
    echo "$out" | tail -n 3
    continue
  fi

  python - "$host" <<'PY'
import sys
from statistics import median

host = sys.argv[1]
vals = [float(x.strip()) for x in sys.stdin if x.strip()]
vals.sort()
n = len(vals)
def pct(p: float) -> float:
    if not vals:
        return 0.0
    idx = round((n - 1) * p)
    idx = max(0, min(idx, n - 1))
    return vals[idx]

print(f"host={host} samples={n} p50_ms={pct(0.50):.3f} p90_ms={pct(0.90):.3f} p99_ms={pct(0.99):.3f} min_ms={vals[0]:.3f} max_ms={vals[-1]:.3f}")
PY
  <<<"$samples"

  # Also print packet loss summary from ping output.
  echo "$out" | awk '/packet loss|rtt min\/avg\/max/{print}'
done

