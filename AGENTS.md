# POLYEDGE — PROJECT KNOWLEDGE BASE

**Generated:** 2026-03-04
**Branch:** main

## OVERVIEW
Forge-first Rust trading runtime for Polymarket BTC/ETH/SOL/XRP 5m/15m binary prediction markets. Paper always-on; Live gated by env. Stack: Rust (tokio/axum) + React (uPlot) + ClickHouse + Redis.

## STRUCTURE
```
polyedge/
├── crates/
│   ├── polyedge_forge/     # Main runtime: recorder, API, paper/live strategy
│   ├── polyedge_hpo/       # Standalone HPO service (port 9820)
│   ├── core_types/         # Shared domain types (traits, signals, intents)
│   ├── feed_polymarket/    # Polymarket CLOB WS feed adapter
│   ├── feed_reference/     # Binance/Chainlink reference price feeds
│   └── market_discovery/   # Gamma API market discovery
├── heatmap_dashboard/      # React frontend (served from Ireland dist/)
├── configs/
│   ├── strategy-profiles/  # Tuned FEV1 param sets (.full.json)
│   └── optimized.env.example
├── scripts/forge/          # Ops scripts (setup, live profiles, guards)
└── ops/
    ├── systemd/            # Tokyo disk-guard timer units
    └── clob_gateway/       # Python CLOB proxy (legacy, standalone)
```

## WHERE TO LOOK
| Task | Location |
|------|----------|
| FEV1 strategy logic | `crates/polyedge_forge/src/fev1.rs` |
| API server wiring + routes | `crates/polyedge_forge/src/api/mod.rs` |
| Live execution (order flow) | `crates/polyedge_forge/src/api/live_execution/` |
| Paper strategy handlers | `crates/polyedge_forge/src/api/strategy/handlers.rs` |
| Ireland recorder/relay core | `crates/polyedge_forge/src/ireland.rs` |
| Tokyo relay | `crates/polyedge_forge/src/tokyo.rs` |
| ClickHouse/Redis persistence | `crates/polyedge_forge/src/db_sink.rs` |
| Data models (rows) | `crates/polyedge_forge/src/models.rs` |
| Market round/switch logic | `crates/polyedge_forge/src/market_switch/` |
| Market data sampling window | `crates/polyedge_forge/src/market_data_exchange/` |
| Shared domain types/traits | `crates/core_types/src/lib.rs` |
| Polymarket WS feed | `crates/feed_polymarket/src/lib.rs` |
| Reference feed (Binance+Chainlink) | `crates/feed_reference/src/lib.rs` |
| Frontend dashboard | `heatmap_dashboard/src/App.tsx` |
| Frontend API client | `heatmap_dashboard/src/api.ts` |
| Strategy param profiles | `configs/strategy-profiles/*.full.json` |
| Live execution env profiles | `scripts/forge/live_execution_profiles.env` |
| All env config defaults | `configs/optimized.env.example` |

## RUNTIME MODES (polyedge_forge)
Three exclusive binary modes via CLI:
- `ireland-recorder` — UDP ingestion → ClickHouse/Redis, no API
- `ireland-api` — API server + paper/live strategy runtime
- `tokyo-relay` — Price relay to Ireland over AWS backbone

## KEY ENV VARS
| Variable | Default | Purpose |
|----------|---------|---------|
| `FORGE_API_BIND` | `0.0.0.0:9810` | API bind |
| `FORGE_CH_URL` | `http://127.0.0.1:8123` | ClickHouse |
| `FORGE_REDIS_URL` | `redis://127.0.0.1:6379/0` | Redis |
| `FORGE_FEV1_RUNTIME_ENABLED` | `true` | Paper strategy on/off |
| `FORGE_FEV1_LIVE_EXECUTE` | `false` | **Live trading master switch** |
| `FORGE_FEV1_LIVE_ARM_REQUIRED` | `true` | Must arm before live submit |
| `FORGE_FEV1_LIVE_HARD_KILL` | `false` | Force-disable live (no restart needed) |
| `FORGE_FEV1_RUNTIME_LOOP_MS` | `260` | Main strategy loop |
| `FORGE_FEV1_RUNTIME_FAST_LOOP_MS` | `90` | Fast path loop |
| `FORGE_FEV1_PRIVATE_KEY` | (required) | Polymarket signing key |
| `FORGE_ACTIVE_SYMBOL_TIMEFRAMES` | `BTCUSDT:5m\|15m,...` | Active symbol/TF matrix |

## LIVE ROLLOUT PROFILES
Load: `COMMON` + one of:
- `SHADOW` — Full strategy, no real submit (dry-run verification)
- `CANARY` — Limited live, 1 position max
- `FULL` — Full production

## API ENDPOINTS (ireland-api, port 9810)
| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/chart` | Historical chart (downsampled) |
| GET | `/api/rounds` | Round history with outcomes |
| GET | `/api/heatmap` | Pricing heatmap |
| GET | `/api/accuracy_series` | Rolling direction accuracy |
| GET | `/api/collector/status` | Feed health |
| GET | `/api/strategy/paper` | Paper sim results |
| POST | `/api/strategy/live/control` | Pause/resume live |
| GET | `/api/strategy/live/events` | Live execution log |
| GET | `/ws/live` | Real-time market WS |
| GET | `/health/live` | Liveness |
| GET | `/health/db` | CH + Redis health |

## COMMANDS
```bash
# Primary commands
cargo check -p polyedge_forge
cargo test --workspace --all-targets
npm --prefix heatmap_dashboard ci
npm --prefix heatmap_dashboard run build

# Run modes
cargo run -p polyedge_forge -- ireland-recorder
cargo run -p polyedge_forge -- ireland-api
cargo run -p polyedge_forge -- tokyo-relay
cargo run -p polyedge_hpo
```

## INFRASTRUCTURE
- Ireland (AWS eu-west-1 c7i.xlarge, 54.77.232.166) — Primary runtime
- Tokyo (AWS ap-northeast-1 c6i.large, 57.180.89.145) — Price relay (lower WS latency)
- Tokyo → Ireland over AWS backbone

## CONVENTIONS
- `include!` pattern: `live_execution.rs` and `strategy.rs` compose subfiles via `include!()`, NOT Rust `mod`. All files compile into one module scope.
- Live safety defaults are strict by design — arm/kill env vars require no restart.
- No legacy latency-arb code. If something looks like leftover arbitrage infra, it's removable.
- `anyhow::Result` everywhere, `thiserror` for domain errors.
- Tokio async throughout; channel patterns: `tokio::sync::mpsc`, `crossbeam-queue`.

## ANTI-PATTERNS (THIS PROJECT)
- Never add `as any` / `@ts-ignore` / `#[allow(unused)]` silencing
- Never add new "infra" in `mod.rs` — isolate to `infra.rs`
- Never bypass `FORGE_FEV1_LIVE_ARM_REQUIRED` in code
- Never dual-write to both old and new structures (clean migration only)
- Never store state in the clob_gateway Python process — it's stateless

## NOTES
- HPO (`polyedge_hpo`) runs independently on port 9820; it fetches history from Ireland API
- `market_switch` handles round-phase transitions (Active/Prewarm/FreezeEnter/CutoverReady/Degraded)
- `market_data_exchange` manages candidate pool trimming and sampling windows
- Tokyo disk guard: systemd timer at `ops/systemd/` prevents disk-full crashes
