# CLAUDE.md

## Project Scope

This repository is Forge-only:

- `polyedge_forge` runtime + its direct dependency crates
- `heatmap_dashboard`
- Forge deployment/maintenance scripts under `scripts/forge`

All legacy latency-arbitrage side stacks are intentionally removed.

## Primary Commands

```bash
cargo check -p polyedge_forge
cargo test --workspace --all-targets
npm --prefix heatmap_dashboard ci
npm --prefix heatmap_dashboard run build
```

## Runtime Rule

- Paper is always available in Forge.
- Live execution must be explicitly enabled by env/config gate.
