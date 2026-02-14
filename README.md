# Polymarket Trading Workspace

This workspace implements a no-mock, event-driven system for paper-to-live trading:

- Real feeds only (Polymarket + external exchanges)
- Maker strategy with inventory controls
- Risk gates, replay engine, and paper executor
- Control plane via HTTP

## Run

```bash
cargo run -p app_runner
```

On Windows, install prerequisites first:

```powershell
pwsh -File .\scripts\setup_windows.ps1
```

## Control API

- `GET /health`
- `GET /metrics`
- `GET /state/positions`
- `GET /state/pnl`
- `GET /report/shadow/live`
- `GET /report/shadow/final`
- `POST /control/pause`
- `POST /control/resume`
- `POST /control/flatten`
- `POST /control/reset_shadow`
- `POST /control/reload_strategy`
- `POST /control/reload_toxicity`
- `POST /control/reload_perf_profile`

## Benchmarks

Primary benchmark (WS-first, uses live runtime metrics + market WS lag):

```bash
python scripts/e2e_latency_test.py --mode ws-first --base-url http://127.0.0.1:8080 --symbol BTCUSDT --seconds 120
```

Legacy REST comparison:

```bash
python scripts/legacy_rest_probe.py --symbol BTCUSDT --iterations 80
```

Conservative parameter regression:

```bash
python scripts/param_regression.py --base-url http://127.0.0.1:8080 --window-sec 300 --max-trials 12
```

## Key Live Metrics

- `quote_block_ratio`
- `policy_block_ratio`
- `tick_to_decision_p50_ms/p90_ms/p99_ms`
- `ack_only_p50_ms/p90_ms/p99_ms`
- `tick_to_ack_p99_ms`
- `pnl_10s_p50_bps_raw/pnl_10s_p50_bps_robust`
- `pnl_10s_sample_count/pnl_10s_outlier_ratio`
- `queue_depth_p99/event_backlog_p99`

## Report Artifacts

Runtime report files are written to `datasets/reports/<utc-date>/`:

- `report_shadow_12h.md`
- `latency_breakdown_12h.csv`
- `market_scorecard.csv`
- `next_fixlist.md`
