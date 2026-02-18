# PolyEdge v5.2 Atomic Commit Audit Map

This file maps the plan template names to concrete commits on `feat/p1-wire-dual`.
It is intended for audit and traceability when commit history spans multiple refinement rounds.

## Template -> Commit Mapping

| Plan template name | Implemented commit(s) | Notes |
|---|---|---|
| `chore/baseline-snapshot` | `cedf2c2`, `3acb902` | Remote deploy/tuning baseline hardening and sync robustness. |
| `feat/v52-single-flow-core` | `f9c9ad8`, `ce68b68` | Single v5.2 route path and convergence/fee gate consolidation. |
| `feat/v52-exit-and-fallback` | `30e93d5`, `a499802`, `ce68b68` | Exit lifecycle, reversal handling, and fallback behavior. |
| `feat/v52-live-gates` | `f9c9ad8`, `6f07242` | Settlement/live hard gate and degraded-source guard. |
| `feat/v52-reporting-fallback-metric` | `f9c9ad8`, `2ac06ea` | Live report continuity, fallback counter, and queue-focused reporting. |
| `feat/v52-reversal-switch-integration` | `TBD (this change set)` | End-to-end reversal flatten + opposite maker re-entry integration test. |
| `feat/v52-queue-opt` | `53d612a`, `24b8bfe`, `6282bc8`, `2ac06ea` | Queue wait tail optimization and hot-loop stall removal. |

## Working Tree Hygiene

- `scripts/convergence_analysis.py` is intentionally tracked as a reproducible convergence analysis utility used in latency-window studies.
