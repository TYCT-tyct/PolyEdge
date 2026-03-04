# polyedge_hpo — HYPERPARAMETER OPTIMIZATION SERVICE

## OVERVIEW
Standalone async HTTP service (port 9820). Fetches history from Ireland API, runs genetic algorithm search over FEV1 params, returns best candidate. Runs independently — zero coupling to Forge runtime.

## API
| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Liveness |
| POST | `/api/hpo/search` | Synchronous one-shot search |
| POST | `/api/hpo/jobs` | Submit async job |
| GET | `/api/hpo/jobs` | List all jobs |
| GET | `/api/hpo/jobs/{job_id}` | Job status + result |

## SEARCH ALGORITHM
- Genetic / evolutionary: population → fitness eval → elite retention → mutation → repeat
- Fitness = `SimulationResult` metrics from `polyedge_forge::fev1::simulate()`
- Robust score weights: win_rate × net_pnl × drawdown_penalty
- Job input: `{ symbol, market_type, lookbacks: [720, 1440, 2880] }`
- Job output: best `RuntimeConfig` candidate + score breakdown

## CONVENTIONS
- History fetched from Ireland API (`FORGE_API_BASE` env var) — not direct DB access
- Jobs stored in-memory (no persistence) — restart clears job list
- `anyhow::Result` + axum error types

## ANTI-PATTERNS
- Don't couple HPO to Forge runtime state — it must run independently
- Don't persist jobs to disk — stateless by design
- Don't add Forge-side changes to trigger HPO — HPO pulls, never pushed

## COMMANDS
```bash
cargo run -p polyedge_hpo
# Submit job:
curl -X POST http://127.0.0.1:9820/api/hpo/jobs \
  -H "content-type: application/json" \
  -d '{"symbol":"BTCUSDT","market_type":"5m","lookbacks":[720,1440,2880]}'
```
