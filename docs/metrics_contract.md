# PolyEdge Metrics Contract

Version: `2026-02-14.v1`

This file is the single source of truth for live gate/report metrics.

## Units

- `ms`: milliseconds
- `us`: microseconds
- `bps`: basis points
- `usdc`: USDC notional
- `ratio`: `[0, 1]`

## Data Validity Chain

- `raw` records must include: `sha256`, `source_seq`, `ingest_seq`, source timestamp, local receive timestamp.
- `normalized` records must include: `source_seq`, `ingest_seq`, market/symbol, timing fields, and outcome fields.
- Any invalid sample (`stale`, `seq_gap`, `ts_inversion`) is excluded from gate EV calculations.

## Execution Funnel Metrics

- `eligible_count`: count of quote intents that pass pre-trade quality/risk checks.
- `executed_count`: count of eligible intents that receive accepted ack.
- `executed_over_eligible = executed_count / eligible_count` (`0` when `eligible_count == 0`).

## Blocking Metrics

- `quote_block_ratio = quote_blocked / (quote_attempted + quote_blocked)` (`0` when denominator is `0`).
- `policy_block_ratio = policy_blocked / (quote_attempted + policy_blocked)` (`0` when denominator is `0`).

## EV / PnL Metrics

- `ev_net_usdc_p50`: p50 of `net_markout_10s_usdc` on valid outcomes.
- `ev_net_usdc_p10`: p10 of `net_markout_10s_usdc` on valid outcomes.
- `ev_positive_ratio`: `count(net_markout_10s_usdc > 0) / count(valid outcomes)`.
- `roi_notional_10s_bps_p50`: p50 of 10s markout return in bps, scaled by entry notional.
- `pnl_10s_p50_bps_raw`: p50 over all valid outcomes.
- `pnl_10s_p50_bps_robust`: p50 after outlier trimming.

## Latency Metrics

- `tick_to_decision_*_ms`: from accepted tick/book pair to strategy+risk decision completion.
- `ack_only_*_ms`: execution venue request/ack duration only.
- `tick_to_ack_*_ms`: end-to-end decision+execution path.
- `decision_queue_wait_p99_ms`: queue waiting before compute stage.
- `decision_compute_p99_ms`: compute stage duration.
- `source_latency_p99_ms`: source event timestamp to local receive delay.
- `local_backlog_p99_ms`: local queue/backlog-induced delay.

## Hard Gates

- `data_valid_ratio >= 0.999`
- `seq_gap_rate <= 0.001`
- `ts_inversion_rate <= 0.0005`
- `tick_to_ack_p99_ms < 450`
- `decision_compute_p99_ms < 2`
- `feed_in_p99_ms < 800`
- `executed_over_eligible >= 0.60`
- `quote_block_ratio < 0.10`
- `policy_block_ratio < 0.10`
- `ev_net_usdc_p50 > 0`
- `roi_notional_10s_bps_p50 > 0`

