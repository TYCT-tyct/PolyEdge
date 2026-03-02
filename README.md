# PolyEdge (Forge-Only)

PolyEdge is now a Forge-first repository. Legacy latency-arbitrage stacks and side systems were removed.

## Kept Components

- `crates/polyedge_forge` - recorder, API, paper/live strategy runtime
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
