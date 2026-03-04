# api/ — SERVER WIRING & HANDLERS

## OVERVIEW
Axum HTTP/WS server. `mod.rs` owns state + router. Submodules own logic. See `ARCHITECTURE.md` for full module map.

## MODULE ROLES
| File | Owns |
|------|------|
| `mod.rs` | `ApiState`, `ApiConfig`, router registration, live runtime loop |
| `read_api.rs` | All read-only GET handlers (chart, rounds, heatmap, accuracy, latest, collector) |
| `snapshot.rs` | Snapshot freshness scoring + market ranking |
| `infra.rs` | Redis helpers, ClickHouse query helper, health checks |
| `row_utils.rs` | Row parsing + downsampling (no HTTP concern) |
| `market_utils.rs` | Symbol normalization, timeframe conversion |
| `strategy.rs` + `strategy/` | Paper simulation types + `include!` composition |
| `live_execution.rs` + `live_execution/` | Order submission types + `include!` composition |

## KEY STATE TYPES
- `ApiState` — Arc-shared: caches, live position states, pending orders, exec metrics
- `LivePositionState` — Per (symbol, market_type): entry price, entry ts, side, fill metadata
- `LivePendingOrder` — In-flight order: order_id, side, size, submitted_at, retry_count
- `LiveRuntimeConfig` — Parsed once at startup from all `FORGE_FEV1_LIVE_*` env vars
- `LiveExecutionAggState` — Rolling exec metrics: slippage_avg, reject_rate, latency_p95

## LIVE SAFETY GATES (in priority order)
1. `FORGE_FEV1_LIVE_HARD_KILL=true` → blocks all submit, no restart needed
2. `FORGE_FEV1_LIVE_EXECUTE=false` → master switch
3. `FORGE_FEV1_LIVE_ARM_REQUIRED=true` + `FORGE_FEV1_LIVE_ARMED=false` → requires arming
4. `FORGE_FEV1_LIVE_MAX_OPEN_POSITIONS=1` → position cap
5. `FORGE_FEV1_REQUIRE_FIXED_ENTRY_SIZE=true` → must set `FORGE_FEV1_FIXED_ENTRY_SIZE_SHARES`

## ROUTING OWNERSHIP
- Router registered in `mod.rs` only
- Handlers live in `read_api.rs` and `strategy/handlers.rs`
- Live execution not directly route-exposed — orchestrated from strategy runtime

## CONVENTIONS
- `include!` compiles `strategy/*` and `live_execution/*` into parent module scope — don't use `mod`
- `Arc<RwLock<HashMap<...>>>` for shared live state
- Redis pub/sub drives WS push to frontend
- Child modules must not import each other directly

## ANTI-PATTERNS
- Don't add new routes in child modules — route registration belongs in `mod.rs`
- Don't add infra helpers to `mod.rs` — use `infra.rs`
- Don't duplicate snapshot freshness logic — `snapshot.rs` is the single source
- Don't bypass live safety gates in code — they're intentional hard stops
