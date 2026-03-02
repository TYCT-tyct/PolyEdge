# CLAUDE.md

## Current Project Scope

The workspace is intentionally reduced to FEV1-active services only:

- `polyedge_forge`
- shared runtime crates they directly depend on

Legacy strategy modules are no longer part of active code paths.

## Primary Commands

```bash
cargo check -p polyedge_forge
cargo run -p polyedge_forge
npm --prefix heatmap_dashboard run build
```

## Operational Rule

- Paper simulation runs by default through `polyedge_forge`.
- Live execution must be explicitly enabled by runtime switch/config.
