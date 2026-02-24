#!/usr/bin/env bash
set -euo pipefail

pkill -f strategy_seed_search.py || true
nohup python3 -u /tmp/strategy_seed_search.py \
  --base-url http://127.0.0.1:9810 \
  --market-type 5m \
  --seed-from 1 \
  --seed-to 40 \
  --iterations 900 \
  --lookback-minutes 1440 \
  --max-trades 900 \
  --window-trades 50 \
  --target-win-rate 90 \
  --recent-lookback-minutes 1440 \
  --recent-weight 0.82 \
  --out /tmp/seed_search_single.json \
  > /tmp/seed_search_single.log 2>&1 &
echo "PID:$!"
sleep 1
pgrep -af strategy_seed_search.py || true
