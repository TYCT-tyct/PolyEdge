# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PolyEdge is a high-frequency statistical arbitrage trading system for Polymarket. It implements an event-driven, no-mock architecture with maker strategy, inventory controls, risk gates, replay engine, and paper executor.

## Commands

### Running the Application

```bash
# Main application (requires clob_gateway running on port 9001)
cargo run -p app_runner
```

On Windows, install prerequisites first:
```powershell
pwsh -File .\scripts\setup_windows.ps1
```

If `cargo test` fails with `link.exe not found`, run cargo via VS Build Tools:
```powershell
pwsh -File .\scripts\cargo_msvc.ps1 test -q
```

### Testing

```bash
cargo test
```

Run a single test:
```bash
cargo test -p <crate_name> <test_name>
```

### Benchmarks

```bash
# Primary benchmark (WS-first, ~60s default)
python scripts/e2e_latency_test.py --profile quick --mode ws-first --base-url http://127.0.0.1:8080 --symbol BTCUSDT

# Parameter regression
python scripts/param_regression.py --profile quick --base-url http://127.0.0.1:8080 --run-id r1

# Long-run orchestrator with rollback
python scripts/long_regression_orchestrator.py --profile quick --base-url http://127.0.0.1:8080 --run-id long1

# Storm/fault-tolerance test
python scripts/storm_test.py --base-url http://127.0.0.1:8080 --duration-sec 300 --burst-rps 20 --concurrency 8

# Cross-region A/B comparison
python scripts/ab_region_compare.py --base-a http://<eu-host>:8080 --base-b http://<us-host>:8080 --seconds 600
```

### Control API

The app exposes HTTP control endpoints on port 8080:
- `GET /health` - Health check
- `GET /metrics` - Prometheus metrics
- `GET /state/positions` - Current positions
- `GET /state/pnl` - P&L state
- `POST /control/pause` / `POST /control/resume` - Trading control
- `POST /control/flatten` - Flatten all positions
- `POST /control/reload_strategy` - Hot-reload strategy config

### clob_gateway (Required Dependency)

The Rust engine requires the Python clob_gateway to be running for order signing:

```bash
# Install dependencies
python3 -m pip install -r ops/clob_gateway/requirements.txt

# Run gateway (set CLOB_PRIVATE_KEY, CLOB_API_KEY, etc. env vars first)
python3 ops/clob_gateway/app.py --host 127.0.0.1 --port 9001
```

## Architecture

### Crate Organization

The workspace contains 21 crates in `crates/`:

**Data Feeds:**
- `feed_polymarket` - Polymarket WebSocket feed for orders/books
- `feed_reference` - External reference price feeds
- `feed_udp` - Low-latency UDP receiver for Binance BookTicker
- `feeder_tokyo` - UDP sender service (Binance -> UDP)
- `poly_wire` - Binary protocol definitions for UDP feed

**Strategy Layer:**
- `strategy_maker` - Maker quote generation with inventory skew
- `fair_value` - Basis MR fair value model
- `direction_detector` - Market direction signal detection
- `timeframe_router` - Multi-timeframe opportunity routing
- `taker_sniper` - Taker order execution logic
- `settlement_compounder` - Position compounding across settlements

**Risk & Execution:**
- `risk_engine` - Risk limits and position checks
- `execution_clob` - CLOB order execution (communicates with clob_gateway)
- `portfolio` - Position and inventory management

**Infrastructure:**
- `infra_bus` - RingBus event bus for inter-component communication
- `infra_clock` - Time synchronization
- `core_types` - Shared types and traits
- `observability` - Metrics and tracing setup

**Backtesting & Replay:**
- `paper_executor` - Shadow executor for paper trading
- `replay_engine` - Historical market replay for backtesting
- `market_discovery` - Market scanning and discovery

**Application:**
- `app_runner` - Main entry point orchestrating all components

### Data Flow

```
Feed Layer          Strategy Layer         Risk Layer        Execution
   │                     │                     │                 │
   ▼                     ▼                     ▼                 ▼
Polymarket WS  ──►  Fair Value  ──►  Risk Engine  ──►  clob_gateway
Reference Feed       Maker Quotes      Position Limits       (HTTP)
                     Direction           Hard Gates
```

Events flow through `RingBus<EngineEvent>` - the central event bus connecting all components.

### Configuration

- `configs/strategy.toml` - Maker strategy parameters
- `configs/strategy.toml` (`[risk_controls]`) - Risk limits
- `configs/execution.toml` - Execution settings
- `configs/latency.toml` - Runtime performance tuning
- `configs/universe.toml` - Market universe (symbols, timeframes)

### Hard Gates (from docs/metrics_contract.md)

The system enforces these live trading thresholds:
- `data_valid_ratio >= 0.999`
- `seq_gap_rate <= 0.001`
- `tick_to_ack_p99_ms < 450`
- `executed_over_eligible >= 0.60`
- `quote_block_ratio < 0.10`
- `ev_net_usdc_p50 > 0`
