#!/usr/bin/env bash
set -euo pipefail

pkill -f strategy_seed_search.py || true
mkdir -p /tmp/seed_search_logs
rm -f /tmp/seed_search_logs/run_*.log /tmp/seed_search_logs/res_*.json

nohup python3 -u /tmp/strategy_seed_search.py \
  --base-url http://127.0.0.1:9810 \
  --market-type 5m \
  --seed-from 1 --seed-to 12 \
  --iterations 1500 \
  --lookback-minutes 1440 \
  --max-trades 900 \
  --window-trades 50 \
  --target-win-rate 90 \
  --recent-lookback-minutes 1440 \
  --recent-weight 0.80 \
  --out /tmp/seed_search_logs/res_1_12.json \
  > /tmp/seed_search_logs/run_1_12.log 2>&1 &
echo "PID1:$!"

nohup python3 -u /tmp/strategy_seed_search.py \
  --base-url http://127.0.0.1:9810 \
  --market-type 5m \
  --seed-from 13 --seed-to 24 \
  --iterations 1500 \
  --lookback-minutes 1440 \
  --max-trades 900 \
  --window-trades 50 \
  --target-win-rate 90 \
  --recent-lookback-minutes 1440 \
  --recent-weight 0.80 \
  --out /tmp/seed_search_logs/res_13_24.json \
  > /tmp/seed_search_logs/run_13_24.log 2>&1 &
echo "PID2:$!"

nohup python3 -u /tmp/strategy_seed_search.py \
  --base-url http://127.0.0.1:9810 \
  --market-type 5m \
  --seed-from 25 --seed-to 36 \
  --iterations 1500 \
  --lookback-minutes 1440 \
  --max-trades 900 \
  --window-trades 50 \
  --target-win-rate 90 \
  --recent-lookback-minutes 1440 \
  --recent-weight 0.80 \
  --out /tmp/seed_search_logs/res_25_36.json \
  > /tmp/seed_search_logs/run_25_36.log 2>&1 &
echo "PID3:$!"

sleep 1
pgrep -af strategy_seed_search.py || true
