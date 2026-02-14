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

If `cargo test` fails with `link.exe not found`, run cargo via VS Build Tools environment:

```powershell
pwsh -File .\scripts\cargo_msvc.ps1 test -q
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
python scripts/param_regression.py --base-url http://127.0.0.1:8080 --window-sec 300 --max-trials 12 --run-id r1 --max-runtime-sec 3600 --heartbeat-sec 30 --fail-fast-threshold 3
```

Long-run orchestrator with rollback:

```bash
python scripts/long_regression_orchestrator.py --base-url http://127.0.0.1:8080 --cycles 4 --run-id long1 --max-runtime-sec 14400 --heartbeat-sec 60 --fail-fast-threshold 2
```

Cross-region A/B comparison:

```bash
python scripts/ab_region_compare.py --base-a http://<eu-host>:8080 --base-b http://<us-host>:8080 --seconds 600 --run-id ab1 --heartbeat-sec 30 --fail-fast-threshold 3
```

## Key Live Metrics

- `quote_block_ratio`
- `policy_block_ratio`
- `window_id/window_shots/window_outcomes`
- `gate_ready/gate_fail_reasons`
- `tick_to_decision_p50_ms/p90_ms/p99_ms`
- `ack_only_p50_ms/p90_ms/p99_ms`
- `tick_to_ack_p99_ms`
- `pnl_10s_p50_bps_raw/pnl_10s_p50_bps_robust`
- `pnl_10s_sample_count/pnl_10s_outlier_ratio`
- `queue_depth_p99/event_backlog_p99`

## Runtime Perf Profile

`configs/latency.toml` runtime section supports:

- `tail_guard`
- `io_flush_batch`
- `io_queue_capacity`
- `json_mode`
- `io_drop_on_full` (when `true`, JSONL queue overflow drops records instead of blocking hot path I/O)

## Report Artifacts

Runtime report files are written to `datasets/reports/<utc-date>/`:

- `report_shadow_12h.md`
- `latency_breakdown_12h.csv`
- `market_scorecard.csv`
- `next_fixlist.md`
- `region_ab_compare.md` / `region_ab_compare.json`
- `long_regression_audit.jsonl`
