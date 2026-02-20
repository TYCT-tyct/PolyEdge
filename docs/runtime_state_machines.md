# Runtime State Machines

This document captures the runtime control-state transitions that matter for remote operations and transport A/B testing.

## 1) Execution Arming State

The runtime decides between `paper` and `live` in `crates/app_runner/src/bootstrap.rs`.

### Inputs

- `configs/execution.toml` -> `execution.mode`
- env: `POLYEDGE_LIVE_ARMED`
- `configs/settlement.toml` -> settlement gate (`required_for_live`, `endpoint`)

### Transition Rules

1. If `execution.mode != "live"` -> run in `paper`.
2. If `execution.mode == "live"` but `POLYEDGE_LIVE_ARMED` is false/missing -> force `paper`.
3. If `execution.mode == "live"` and armed, but settlement gate is not ready -> force `paper`.
4. Only when all three are satisfied (`live` mode, armed, settlement gate ready) -> run `live`.

### Operational Implications

- `/control/arm_live` changes arming intent but does not hot-swap execution mode if process booted in `paper`.
- `restart_required: true` in arm response means process restart is needed to actually enter `live`.

## 2) Fusion Mode State

Managed by `/control/reload_fusion` and evaluated in `crates/app_runner/src/feed_runtime.rs`.

### Modes

- `direct_only`
- `active_active`
- `hyper_mesh`

### Common Guards

- `udp_share_cap`
- `jitter_threshold_ms`
- `fallback_arm_duration_ms`
- `fallback_cooldown_sec`
- `udp_local_only`

### Hyper-Mesh Fallback Lifecycle

This applies when `mode == hyper_mesh`.

1. **ws_primary**: default state while WS freshness is healthy.
2. **armed**: WS freshness breach started but persistence window not met yet.
3. **udp_fallback**: breach persisted for `fallback_arm_duration_ms`; UDP fallback is activated.
4. **cooldown**: WS recovered but still inside cooldown window (`fallback_cooldown_sec`).
5. Back to **ws_primary** after cooldown expiry.

Reported in `/report/shadow/live` via:

- `fallback_state`
- `fallback_trigger_reason_distribution`
- `udp_share_effective`
- `source_mix_ratio`

## 3) Shadow Metrics Window State

Managed by `/control/reset_shadow`.

### Reset Effects

- Shadow stats window is reset (`window_id` increments).
- Toxicity state is cleared.
- Router/sniper/compounder runtime objects are reinitialized from current configs.
- Fallback reason counters and short-horizon tallies restart from the new window baseline.

### Why This Matters

- Any regression comparison across runs must treat `window_id` reset boundaries as new baselines.
- Scripts that compare deltas should use run-local deltas, not absolute totals across resets.

## 4) Recommended Remote Test Order

1. `POST /control/reload_fusion` with target mode and guard values.
2. `POST /control/reset_shadow`.
3. Warmup interval (5s+).
4. Run `scripts/full_latency_sweep.py`.
5. Run `scripts/storm_test.py` (or `scripts/seat_transport_guard.py` for A/B and rollback).
6. Read `/report/shadow/live` and verify mode-specific counters and fallback state.
