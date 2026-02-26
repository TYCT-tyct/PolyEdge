# EdgeFlow V1 Isolated Config Set

This directory is the dedicated runtime config set for strategy one (`edgeflow_v1`).

Use it by exporting:

- `POLYEDGE_STRATEGY_CONFIG_PATH=configs/edgeflow_v1/strategy.toml`
- `POLYEDGE_UNIVERSE_CONFIG_PATH=configs/edgeflow_v1/universe.toml`
- `POLYEDGE_EXECUTION_CONFIG_PATH=configs/edgeflow_v1/execution.toml`
- `POLYEDGE_SETTLEMENT_CONFIG_PATH=configs/edgeflow_v1/settlement.toml`
- `POLYEDGE_LATENCY_CONFIG_PATH=configs/edgeflow_v1/latency.toml`
- `POLYEDGE_SEAT_CONFIG_PATH=configs/edgeflow_v1/seat.toml`

Compatibility:

- `engine_mode = "edgeflow_v1"` is the only supported value for this isolated config set.
