# SEAT v2.3 Runtime + Acceptance Runbook

## Local Service Layout

- `app_runner` control API: `http://127.0.0.1:${POLYEDGE_CONTROL_PORT:-8080}`
- optimizer service: `http://127.0.0.1:${POLYEDGE_SEAT_OPTIMIZER_PORT:-8091}`
- seat config: `configs/seat.toml`
- seat state files:
  - `datasets/reports/seat/seat_state.json`
  - `datasets/reports/seat/seat_decisions.jsonl`
  - `datasets/reports/seat/reports/seat_tune_*.json`

## Runtime Controls

```bash
curl -X POST http://127.0.0.1:8080/control/seat/pause
curl -X POST http://127.0.0.1:8080/control/seat/resume
curl -X POST http://127.0.0.1:8080/control/seat/force_layer -H 'content-type: application/json' -d '{"layer":"layer2"}'
curl -X POST http://127.0.0.1:8080/control/seat/manual_override -H 'content-type: application/json' -d '{"params":{"position_fraction":0.12}}'
curl -X POST http://127.0.0.1:8080/control/seat/clear_override
```

## Runtime Reports

```bash
curl -fsSL http://127.0.0.1:8080/report/seat/status | jq
curl -fsSL 'http://127.0.0.1:8080/report/seat/history?limit=200' | jq
```

## Optimizer Service

Install and start on target host:

```bash
bash scripts/setup_seat_optimizer_systemd.sh
systemctl status polyedge-seat-optimizer.service --no-pager
```

Health check:

```bash
curl -fsSL http://127.0.0.1:8091/health | jq
```

## Ireland 72h Acceptance Validation

Single-shot check:

```bash
python scripts/seat_remote_acceptance.py \
  --host 54.77.232.166 \
  --user ubuntu \
  --key C:\\Users\\Shini\\Documents\\PolyEdge.pem \
  --run-id seat-ireland-check
```

Watch mode (poll until accepted or timeout):

```bash
python scripts/seat_remote_acceptance.py \
  --host 54.77.232.166 \
  --user ubuntu \
  --key C:\\Users\\Shini\\Documents\\PolyEdge.pem \
  --watch-sec 28800 \
  --poll-sec 60 \
  --run-id seat-ireland-watch
```

Acceptance criteria:

1. `current_layer` is `layer2` or `layer3`
2. `trade_count >= 800`
3. uptime `>= 72h`
4. decision history includes challenger cycle (`shadow_started` + `monitor_pass` or `shadow_reject`)

Output JSON: `datasets/reports/<day>/runs/<run-id>/seat_remote_acceptance.json`
