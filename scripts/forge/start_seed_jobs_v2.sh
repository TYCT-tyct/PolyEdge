#!/usr/bin/env bash
set -euo pipefail

pkill -f strategy_seed_search.py || true
mkdir -p /tmp/seed_search_logs_v2
rm -f /tmp/seed_search_logs_v2/run_*.log /tmp/seed_search_logs_v2/res_*.json

run_job() {
  local from="$1"
  local to="$2"
  local tag="$3"
  nohup python3 -u /tmp/strategy_seed_search.py \
    --base-url http://127.0.0.1:9810 \
    --market-type 5m \
    --seed-from "$from" \
    --seed-to "$to" \
    --iterations 1700 \
    --lookback-minutes 1440 \
    --max-trades 900 \
    --window-trades 50 \
    --target-win-rate 90 \
    --recent-lookback-minutes 1440 \
    --recent-weight 0.82 \
    --out "/tmp/seed_search_logs_v2/res_${tag}.json" \
    > "/tmp/seed_search_logs_v2/run_${tag}.log" 2>&1 &
  echo "PID(${tag}):$!"
}

run_job 1 24 1_24
run_job 25 48 25_48
run_job 49 72 49_72

sleep 1
pgrep -af strategy_seed_search.py || true
