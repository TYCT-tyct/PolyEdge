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

## Search Params

Only the Forge paper-only search toolchain is supported now.

Search locally or on Ireland against the real Forge paper endpoint:

```bash
python scripts/forge/strategy_search_paper_full.py --base-url http://127.0.0.1:9810 --symbol BTCUSDT --market-type 5m
```

Compare candidates under one unified scoring baseline:

```bash
python scripts/forge/compare_profiles.py --base-url http://127.0.0.1:9810 --symbol BTCUSDT --market-type 5m
```

A/B compare two configs against the same paper window:

```bash
python scripts/forge/strategy_ab_compare.py --symbol BTCUSDT --market-type 5m
```

## Live Execution Profiles

Runtime/live parameter presets are in:

- `scripts/forge/live_execution_profiles.env`

Use `COMMON` + one profile block (`SHADOW`, `CANARY`, or `FULL`) to keep rollout reproducible.

## Deploy Forge

Use the exact-commit deploy wrapper from Windows after pushing your release commit to GitHub:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\forge\deploy_forge_release.ps1 -Target both
```

Deployment details are documented in:

- `scripts/forge/DEPLOY_FORGE_RELEASE.md`
