# PolyEdge (FEV1-Only Workspace)

This repository now keeps only the active FEV1 runtime chain:

- `crates/polyedge_forge` - dashboard API, paper simulation, market data aggregation
- `heatmap_dashboard` - frontend dashboard

All legacy strategy stacks and their dependent crates/configs have been removed from the workspace.

## Run

```bash
cargo run -p polyedge_forge
```

## Frontend

```bash
npm --prefix heatmap_dashboard install
npm --prefix heatmap_dashboard run build
```

## Notes

- Paper simulation remains available in `polyedge_forge`.
- Live execution should only be enabled explicitly by runtime config/flag.
