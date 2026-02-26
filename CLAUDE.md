# CLAUDE.md

## Current Project Scope

The workspace is intentionally reduced to FEV1-active services only:

- `polyedge_forge`
- `feeder_tokyo`
- `polyedge_data_backend`
- shared runtime crates they directly depend on

Legacy strategy modules are no longer part of active code paths.

## Primary Commands

```bash
cargo check -p polyedge_forge -p feeder_tokyo -p polyedge_data_backend
cargo run -p polyedge_forge
npm --prefix heatmap_dashboard run build
```

## Operational Rule

- Paper simulation runs by default through `polyedge_forge`.
- Live execution must be explicitly enabled by runtime switch/config.
