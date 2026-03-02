# PolyEdge Forge API Architecture

This document describes the current API module layout after the large-file split.

## Goals

- Keep `mod.rs` focused on server wiring and shared runtime state.
- Isolate strategy, live execution, read endpoints, and infra helpers.
- Make refactors safer by reducing cross-cutting edits in one giant file.

## Module Map

- `mod.rs`
  - API state, config, runtime orchestration, router registration.
  - Imports child modules and re-exports internal functions via `use ...::*`.

- `read_api.rs`
  - Read-only HTTP handlers and websocket payload endpoints.
  - Includes chart/history/latest/round/heatmap/accuracy endpoints.

- `strategy.rs` + `strategy/`
  - `strategy.rs` keeps shared strategy types/constants and composes subfiles via `include!`.
  - `strategy/config.rs`: env/profile/default config and guardrails.
  - `strategy/runtime.rs`: sample parsing/loading and runtime simulation plumbing.
  - `strategy/handlers.rs`: HTTP handlers for paper/live/control/events endpoints.

- `live_execution.rs` + `live_execution/`
  - `live_execution.rs` keeps shared execution types/constants and composes subfiles via `include!`.
  - `live_execution/config.rs`: gateway/runtime env + cache tuning helpers.
  - `live_execution/targeting.rs`: target resolution, gating and decision selection helpers.
  - `live_execution/gateway.rs`: payload building, gateway submit/query and retry shaping.
  - `live_execution/pending.rs`: pending-order lifecycle and gateway reconciliation.
  - `live_execution/rust_sdk.rs`: Rust SDK submit path and reconciliation.
  - `live_execution/orchestrator.rs`: executor mode dispatch and top-level orchestration.
  - `live_execution/tests.rs`: focused execution behavior tests.

- `infra.rs`
  - Redis read/write helpers.
  - ClickHouse JSON query helper.
  - Generic validation and service health checks.

- `snapshot.rs`
  - Snapshot freshness and ranking helpers.
  - Used by read handlers and live runtime decisions.

- `row_utils.rs`
  - Common row parsing and downsampling helpers.

- `market_utils.rs`
  - Market type normalization/time conversion helpers.

## Dependency Direction

- `mod.rs` depends on all child modules.
- Child modules do not depend on each other directly by path import.
- `strategy/*` and `live_execution/*` are compiled into their parent module via `include!`,
  so behavior remains in a single Rust module scope while files are physically split.
- Shared helper modules (`infra`, `snapshot`, `row_utils`, `market_utils`) should stay logic-only and side-effect-light.

## Routing Ownership

- Router registration remains in `mod.rs`.
- Handler implementations live in `read_api.rs` and `strategy.rs`.
- Live execution internals are not directly exposed as route handlers.

## Operational Notes

- Changes to live order flow should stay inside `live_execution/*`.
- Snapshot freshness rules should stay in `snapshot.rs` to avoid duplicated time-window logic.

## Next Refactor Targets

- Split runtime state mutation helpers from `mod.rs` into a dedicated `runtime_state.rs`.
- Introduce typed request objects for remaining high-arity internal functions.
- Continue converting `include!` groups into explicit Rust submodules where visibility boundaries are beneficial.
