# PolyEdge (Forge-Only)

PolyEdge is now a Forge-first repository. Legacy latency-arbitrage stacks and side systems were removed.

## Kept Components

- `crates/polyedge_forge` - recorder, API, paper/live strategy runtime
- `crates/polyedge_hpo` - dedicated hyperparameter optimization service (independent from Forge runtime)
- `crates/core_types` - shared domain types
- `crates/feed_polymarket` - Polymarket feed adapter
- `crates/feed_reference` - reference feed adapter
- `crates/market_discovery` - market discovery logic
- `heatmap_dashboard` - frontend dashboard
- `scripts/forge` - Forge-only ops and deployment scripts
- `ops/systemd/polyedge-tokyo-disk-guard.*` - Tokyo disk protection timer

## Run Forge

```bash
cargo run -p polyedge_forge -- ireland-recorder
```

```bash
cargo run -p polyedge_forge -- ireland-api
```

```bash
cargo run -p polyedge_forge -- tokyo-relay
```

## Build Dashboard

```bash
npm --prefix heatmap_dashboard ci
npm --prefix heatmap_dashboard run build
```

## Run HPO Service

```bash
cargo run -p polyedge_hpo
```

Submit one search job:

```bash
curl -X POST http://127.0.0.1:9820/api/hpo/jobs \
  -H "content-type: application/json" \
  -d '{"symbol":"BTCUSDT","market_type":"5m","lookbacks":[720,1440,2880]}'
```

Read job result:

```bash
curl http://127.0.0.1:9820/api/hpo/jobs/<job_id>
```

## Live Execution Profiles

Runtime/live parameter presets are in:

- `scripts/forge/live_execution_profiles.env`

Use `COMMON` + one profile block (`SHADOW`, `CANARY`, or `FULL`) to keep rollout reproducible.
