#!/usr/bin/env bash
set -euo pipefail

pkill -f strategy_refine_search.py || true
nohup python3 -u /tmp/strategy_refine_search.py \
  --base-url http://127.0.0.1:9810 \
  --market-type 5m \
  --input /tmp/candidate_cfg.json \
  --iters 320 \
  --out /tmp/refine_result.json \
  > /tmp/refine_search.log 2>&1 &
echo "PID:$!"
sleep 1
pgrep -af strategy_refine_search.py || true
