# SEAT Transport Guard Runbook

## Scope
- Guard rollback is runtime config rollback (`/control/reload_fusion`), not git rollback.
- A/B baseline is `direct_only`, candidate is `websocket_primary`.

## Quick Validation (60s)
```bash
python scripts/seat_transport_guard.py \
  --run-id seat-final-ab-20260219-quick \
  --profile quick_60s \
  --base-url http://127.0.0.1:8080 \
  --dedupe-window-ms 8 \
  --udp-share-cap 0.35 \
  --jitter-threshold-ms 25 \
  --fallback-arm-duration-ms 8000 \
  --fallback-cooldown-sec 300 \
  --udp-local-only true \
  --warmup-sec 5
```

## Background Mode (heartbeat every 5s)
```bash
python scripts/run_guard_background.py \
  --run-id seat-final-ab-20260219-quick-bg \
  --profile quick_60s \
  --base-url http://127.0.0.1:8080 \
  --udp-share-cap 0.35 \
  --jitter-threshold-ms 25 \
  --fallback-arm-duration-ms 8000 \
  --fallback-cooldown-sec 300 \
  --udp-local-only true
```

## Deep + Storm (120s storm)
```bash
python scripts/seat_transport_guard.py \
  --run-id seat-final-ab-20260219-deep \
  --profile deep \
  --storm-duration-sec 120 \
  --base-url http://127.0.0.1:8080 \
  --dedupe-window-ms 8 \
  --udp-share-cap 0.35 \
  --jitter-threshold-ms 25 \
  --fallback-arm-duration-ms 8000 \
  --fallback-cooldown-sec 300 \
  --udp-local-only true \
  --warmup-sec 5
```

## Output Files
- `datasets/reports/<utc-day>/runs/<run-id>-baseline/full_latency_sweep_*.json`
- `datasets/reports/<utc-day>/runs/<run-id>-candidate/full_latency_sweep_*.json`
- `datasets/reports/<utc-day>/runs/<run-id>/seat_transport_guard_summary.json`
- Background wrapper log: `datasets/reports/<utc-day>/runs/<run-id>/seat_transport_guard_live.log`

## Passing Gates
- `candidate_udp_share <= 0.35`
- `policy_block_ratio <= 0.20`
- `tick_to_decision_p99_ms <= 0.45`
- `source_latency_p99_ms <= 100`
- Guard did not rollback, or rollback restored stable baseline in next window
