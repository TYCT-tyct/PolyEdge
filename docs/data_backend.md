# PolyEdge Data Backend (Independent)

独立数据后端，不依赖旧交易循环。

## 组件

1. `tokyo-collector`
   - 默认只采集 `BTCUSDT` Binance tick。
   - 打时间戳并通过 UDP 转发到爱尔兰。
2. `ireland-ingest`
   - 接收东京 tick + Polymarket CLOB + Chainlink。
   - 统一输出 100ms `snapshot_100ms.jsonl`。
3. `api`
   - 为前端提供 REST：
     - `/api/live/markets`
     - `/api/live/snapshot`
     - `/api/live/latest`
     - `/api/live/trades`
     - `/api/live/heatmap`

## 默认字段

- 东京：`symbol, binance_price, ts_exchange_ms, ts_tokyo_recv_ms, source_seq`
- 爱尔兰 PM：`market_id, symbol, timeframe, title, bid_yes, ask_yes, mid_yes, bid_no, ask_no, mid_no, ts_pm_recv_ms`
- Chainlink：`symbol, chainlink_price, ts_chainlink_recv_ms`
- 快照：`delta_price, delta_pct, velocity, acceleration, remaining_ms, net_ev_bps, time_edge_ms`

## 默认目录

- 爱尔兰：`/dev/xvdbb/polyedge-data`
- 东京：`/var/lib/polyedge-data`

## 启动

```bash
# Tokyo
cargo run -p polyedge_data_backend -- tokyo-collector \
  --symbols BTCUSDT --ireland-udp 10.0.3.123:9801

# Ireland ingest
cargo run -p polyedge_data_backend -- ireland-ingest \
  --dataset-root /dev/xvdbb/polyedge-data --symbols BTCUSDT --timeframes 5m

# Ireland API
cargo run -p polyedge_data_backend -- api \
  --dataset-root /dev/xvdbb/polyedge-data --bind 0.0.0.0:8095
```

## systemd 一键安装

```bash
# ireland
bash scripts/setup_data_backend_systemd.sh ireland

# tokyo
bash scripts/setup_data_backend_systemd.sh tokyo
```

脚本会自动关闭旧的 `polyedge-recorder.service`，避免旧链路干扰。
