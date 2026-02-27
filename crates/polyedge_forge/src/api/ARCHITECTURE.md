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

- `strategy.rs`
  - Strategy paper/live pipeline.
  - Optimization and autotune promotion flow.
  - Strategy data parsing/simulation mapping helpers.

- `live_execution.rs`
  - Live order decision gating.
  - Gateway and Rust SDK execution paths.
  - Pending-order reconciliation and retry behavior.

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
- Child modules use `use super::*;` and consume shared types from `mod.rs`.
- Shared helper modules (`infra`, `snapshot`, `row_utils`, `market_utils`) should stay logic-only and side-effect-light.

## Routing Ownership

- Router registration remains in `mod.rs`.
- Handler implementations live in `read_api.rs` and `strategy.rs`.
- Live execution internals are not directly exposed as route handlers.

## Operational Notes

- Changes to live order flow should stay inside `live_execution.rs` whenever possible.
- Autotune key schema and promotion checks should stay inside `strategy.rs`.
- Snapshot freshness rules should stay in `snapshot.rs` to avoid duplicated time-window logic.

## Next Refactor Targets

- Split runtime state mutation helpers from `mod.rs` into a dedicated `runtime_state.rs`.
- Introduce typed request objects for remaining high-arity internal functions.
- Gradually reduce `use super::*;` breadth by importing only required items per module.
