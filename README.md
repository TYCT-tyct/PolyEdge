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
- `GET /report/pnl/by_engine`
- `GET /report/toxicity/live`
- `GET /report/toxicity/final`
- `POST /control/pause`
- `POST /control/resume`
- `POST /control/flatten`
- `POST /control/reset_shadow`
- `POST /control/reload_strategy`
- `POST /control/reload_taker`
- `POST /control/reload_allocator`
- `POST /control/reload_risk`
- `POST /control/reload_toxicity`
- `POST /control/reload_fusion`
- `POST /control/reload_edge_model`
- `POST /control/reload_exit`
- `POST /control/reload_perf_profile`

## Benchmarks

Primary benchmark (WS-first, uses live runtime metrics + market WS lag):

```bash
python scripts/e2e_latency_test.py --profile quick --mode ws-first --base-url http://127.0.0.1:8080 --symbol BTCUSDT
```

The benchmark now supports runtime profiles to avoid oversized test windows:

```bash
python scripts/e2e_latency_test.py --profile quick    # default, ~60s
python scripts/e2e_latency_test.py --profile standard # ~120s
python scripts/e2e_latency_test.py --profile deep     # ~300s
```

Conservative parameter regression:

```bash
python scripts/param_regression.py --profile quick --base-url http://127.0.0.1:8080 --run-id r1
```

`param_regression.py` supports `quick/standard/deep` and a hard runtime budget:

```bash
python scripts/param_regression.py --profile quick --max-estimated-sec 900 --max-runtime-sec 1200
```

Walk-forward robust auto-tuning (recommended):

```bash
python scripts/param_regression.py --profile quick --selection-mode robust --walkforward-windows 2 --top-k-consensus 3
```

This now writes `auto_tuned_thresholds.json` (consensus payloads for `reload_strategy/reload_taker/reload_allocator/reload_toxicity`) under `datasets/reports/<utc-date>/runs/<run-id>/`.

Long-run orchestrator with rollback:

```bash
python scripts/long_regression_orchestrator.py --profile quick --base-url http://127.0.0.1:8080 --run-id long1
```

`long_regression_orchestrator.py` supports `quick/standard/deep`, champion/challenger drift, automatic rollback, and objective-driven promotion:

```bash
python scripts/long_regression_orchestrator.py --profile quick --max-estimated-sec 1800 --max-runtime-sec 1800 --min-objective-delta 0.0005
```

Objective is optimized online as:

```text
EV_net - penalty(latency, decision_compute, feed, block_ratio, execution_shortfall, data_valid, instability, gate_fail)
```

Continuous optimization mode (recommended for remote shadow runs):

```bash
python scripts/long_regression_orchestrator.py --profile standard --continuous --base-url http://127.0.0.1:8080 --state-file datasets/reports/champion_state.json --stop-file datasets/reports/STOP_OPTIMIZER
```

Windows one-command wrapper (opens SSH tunnel + runs continuous optimizer):

```powershell
pwsh -File .\scripts\run_continuous_optimizer.ps1 -SshHost 54.77.232.166 -KeyPath "C:\Users\Shini\Documents\PolyEdge.pem" -Profile standard
```

The orchestrator forwards walk-forward tuning flags to `param_regression.py`, prefers consensus payloads from `auto_tuned_thresholds.json`, and persists champion state for restart-safe operation.

Storm / fault-tolerance test (bounded runtime, no infinite loop):

```bash
python scripts/storm_test.py --base-url http://127.0.0.1:8080 --duration-sec 300 --burst-rps 20 --concurrency 8 --max-runtime-sec 600 --fail-fast-threshold 20
```

Cross-region A/B comparison:

```bash
python scripts/ab_region_compare.py --base-a http://<eu-host>:8080 --base-b http://<us-host>:8080 --seconds 600 --run-id ab1 --heartbeat-sec 30 --fail-fast-threshold 3
```

Remote deploy + validate (from Windows host):

```powershell
pwsh -File .\scripts\remote_deploy_validate.ps1 -RemoteHost 13.43.23.190 -KeyPath "C:\Users\Shini\Documents\test.pem" -BenchSeconds 180 -RegressionSeconds 1200 -Symbol BTCUSDT
```

## UDP Relay Binaries

Tokyo sender (`tokio-tungstenite` + 24-byte bincode UDP packet):

```bash
TARGET=10.0.3.123:6666 SYMBOL=btcusdt cargo run -p feeder_tokyo --bin sender --release
```

Ireland receiver (busy-spin UDP read loop + latency print):

```bash
BIND_ADDR=0.0.0.0:6666 PRINT_EVERY=1 cargo run -p feeder_tokyo --bin receiver --release
```

Pin receiver to dedicated core (recommended on Linux):

```bash
taskset -c 2 BIND_ADDR=0.0.0.0:6666 PRINT_EVERY=1 cargo run -p feeder_tokyo --bin receiver --release
```

Kernel tuning helper (run as root on Linux):

```bash
sudo bash scripts/hft_kernel_tune.sh
```

## Key Live Metrics

- `quote_block_ratio`
- `policy_block_ratio`
- `window_id/window_shots/window_outcomes`
- `gate_ready/gate_fail_reasons`
- `data_valid_ratio/seq_gap_rate/ts_inversion_rate/stale_tick_drop_ratio`
- `tick_to_decision_p50_ms/p90_ms/p99_ms`
- `ack_only_p50_ms/p90_ms/p99_ms`
- `tick_to_ack_p99_ms`
- `decision_queue_wait_p99_ms/decision_compute_p99_ms`
- `source_latency_p99_ms/local_backlog_p99_ms`
- `pnl_10s_p50_bps_raw/pnl_10s_p50_bps_robust`
- `net_markout_10s_usdc_p50/roi_notional_10s_bps_p50`
- `pnl_10s_sample_count/pnl_10s_outlier_ratio`
- `queue_depth_p99/event_backlog_p99`

Metric formulas, units, and gate thresholds are defined in `docs/metrics_contract.md`.
CI validates contract drift with:

```bash
python scripts/validate_metrics_contract.py
```

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
- `engine_pnl_breakdown.csv`
- `next_fixlist.md`
- `truth_manifest.json`
- `region_ab_compare.md` / `region_ab_compare.json`
- `long_regression_audit.jsonl`
