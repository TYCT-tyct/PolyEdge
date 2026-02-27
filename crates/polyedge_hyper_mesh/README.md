# polyedge_hyper_mesh

独立的 Hyper-Mesh 传输重构项目（不修改既有 Forge 主链路），用于先把协议层能力落地并可独立压测：

- 带 `seq` 的 UDP 帧协议（用于 gap/乱序/重复检测）
- 接收端实时统计 `lost_frames/gap_rate/mean_latency/stale_age`
- 健康状态机：`Normal / Caution / Lockdown`
- 便于后续接入 Forge 的独立控制面与回归测试

## 快速运行

```bash
cargo run -p polyedge_hyper_mesh -- recv --bind 0.0.0.0:9901 --report-sec 5
```

另开一个终端发送测试流：

```bash
cargo run -p polyedge_hyper_mesh -- send-test --target 127.0.0.1:9901 --stream-id tokyo-primary --symbol BTCUSDT --interval-ms 50 --count 2000
```

## 设计目标

1. 先把协议与可靠性能力独立实现，避免污染既有交易主链路。
2. 把“是否可交易”的信号从主观判断改为可量化的健康指标。
3. 作为后续 Forge 接入的稳定基座（以最小耦合方式对接）。
