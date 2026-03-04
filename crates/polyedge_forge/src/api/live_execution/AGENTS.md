# live_execution/ — ORDER SUBMISSION PIPELINE

## OVERVIEW
All files compile into `live_execution.rs` module scope via `include!`. Do not add `mod` declarations.

## FILE ROLES
| File | Owns |
|------|------|
| `config.rs` | `LiveExecConfig` from env, cache tuning params, fee/slippage estimates |
| `targeting.rs` | Target market resolution, gating checks, decision candidate selection |
| `planner.rs` | Order payload building, retry ladder policy, fill-meta parsing |
| `pending.rs` | Pending order lifecycle (create, expire, reconcile), shared validation helpers |
| `rust_sdk.rs` | Polymarket Rust SDK: submit, cancel, reconcile (single execution backend) |
| `orchestrator.rs` | Top-level loop: targeting → planning → submit → pending → reconcile |
| `tests.rs` | Focused behavioral tests for execution logic |

## EXECUTION FLOW
```
orchestrator.rs
  → targeting.rs   [resolve market, gate checks, pick decision]
  → planner.rs     [build payload, compute retry ladder]
  → rust_sdk.rs    [submit to CLOB]
  → pending.rs     [track order, wait for fill/expire]
  → rust_sdk.rs    [reconcile / cancel on timeout]
```

## KEY INVARIANTS
- Only ONE open position per (symbol, market_type) — enforced by `targeting.rs`
- `FORGE_FEV1_LIVE_HARD_KILL` checked atomically before every submit in `rust_sdk.rs`
- `FORGE_FEV1_MARKET_BUY_AMOUNT_DECIMALS=2` — notional always cents-aligned
- Retry ladder defined in `planner.rs` — don't replicate retry logic elsewhere
- Pending order expiry tracked in `pending.rs` — single source of truth for in-flight state

## CONVENTIONS
- All functions in this directory are `pub(super)` or `pub(crate)` — no public API surface
- Error propagation: `anyhow::Result` + `?` — no silent swallows
- Env vars parsed via `LiveExecConfig::from_env()` once at startup
- Fill metadata parsed in `planner.rs::parse_fill_meta()` — called post-reconcile

## ANTI-PATTERNS
- Don't add route handlers here — orchestrator is called from strategy runtime, not HTTP
- Don't duplicate HARD_KILL check — it belongs in `rust_sdk.rs` submit path only
- Don't store order state outside `pending.rs`
- Don't split `rust_sdk.rs` into multiple backends — single execution path by design
