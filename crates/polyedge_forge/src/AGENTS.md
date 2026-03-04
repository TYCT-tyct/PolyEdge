# polyedge_forge/src — MODULE MAP

## OVERVIEW
Main Forge runtime. Three CLI modes: `ireland-recorder`, `ireland-api`, `tokyo-relay`.

## MODULE ROLES
| File | Purpose |
|------|---------|
| `main.rs` | Entry: spawns runtime via `cli.rs` dispatch |
| `cli.rs` | Clap CLI — three subcommands, env-driven config |
| `ireland.rs` | Ireland core: UDP ingest + ClickHouse/Redis sink (recorder) + API runtime init |
| `tokyo.rs` | Tokyo relay: WS price relay → Ireland UDP |
| `fev1.rs` | FEV1 strategy: signal gen, entry/exit logic, fee model |
| `db_sink.rs` | ClickHouse batch writer + Redis snapshot cache |
| `models.rs` | DB row types: `RoundRow`, `SnapshotRow100ms`, `SnapshotRow100msProcessed` |
| `persist.rs` | Event persistence helpers — wraps db_sink for domain events |
| `common.rs` | Shared utilities (time, math, symbol helpers) |
| `api/` | REST + WS server — see `api/AGENTS.md` |
| `market_switch/` | Round-phase FSM: Active→Prewarm→FreezeEnter→CutoverReady→Degraded |
| `market_data_exchange/` | Candidate pool trimming + sampling window management |

## KEY TYPES (fev1.rs)
- `Fev1Config` — 25+ runtime params (all from env/profile)
- `Fev1State` — per-market mutable state (position, P&L, cooldown)
- `Fev1Signal` — computed signal: direction, edge, score, P_fair
- `EntryDecision`, `ExitDecision` — typed outcome from entry/exit evaluators

## FEV1 SIGNAL FLOW
```
RefTick + BookSnapshot
  → compute_signal()  [delta_pct, velocity, acceleration → P_fair_up → edge]
  → evaluate_entry()  [threshold gate, spread, bounds, cooldown, noise gate]
  → evaluate_exit()   [stop loss, reverse, trail, take profit, liquidity]
  → ExecutionIntent / PaperIntent
```

## CONVENTIONS
- `include!` in `strategy.rs` / `live_execution.rs` — subfiles share parent module scope
- All env vars parsed once at startup into typed config structs
- `anyhow::Result` for fallible ops; `thiserror` for typed domain errors
- No `unwrap()` in hot path — propagate with `?`

## ANTI-PATTERNS
- Don't add logic to `main.rs` — keep dispatch only
- Don't put infra helpers in `ireland.rs` — use `db_sink.rs` / `persist.rs`
- Don't read env vars outside config structs
