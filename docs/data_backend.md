# PolyEdge Data Backend (Independent)

独立数据后端，不依赖旧交易循环。交易热路径与分析路径解耦：

- 热路径：东京价 + PM 盘口 + Chainlink 融合，持续生成 `snapshot_100ms.jsonl`
- 分析路径（可选）：异步批量写入 ClickHouse
- 热缓存（可选）：将最新快照写入 Redis，前端低延迟读取

## 组件

1. `tokyo-collector`
   - 默认采集 `BTCUSDT` Binance tick
   - 打时间戳并通过 UDP 转发到爱尔兰
2. `ireland-ingest`
   - 接收东京 tick + Polymarket CLOB + Chainlink
   - 统一输出 `snapshot_100ms.jsonl`（100ms 原始快照，非聚合）
   - 可选异步落 ClickHouse、Redis
3. `api`
   - `/api/live/markets`
   - `/api/live/snapshot`
   - `/api/live/latest`
   - `/api/live/latest_cached`（Redis）
   - `/api/live/trades`
   - `/api/live/heatmap`
   - `/api/history/dates`
   - `/api/history/markets`
   - `/api/history/snapshot`

## 默认目录

- 爱尔兰：`/data/polyedge-data`
- 东京：`/var/lib/polyedge-data`

## 启动

```bash
# Tokyo
cargo run -p polyedge_data_backend -- tokyo-collector \
  --dataset-root /var/lib/polyedge-data \
  --symbols BTCUSDT \
  --ireland-udp 10.0.3.123:9801

# Ireland ingest
cargo run -p polyedge_data_backend -- ireland-ingest \
  --dataset-root /data/polyedge-data \
  --symbols BTCUSDT \
  --timeframes 5m,15m \
  --snapshot-ms 100

# Ireland API
cargo run -p polyedge_data_backend -- api \
  --dataset-root /data/polyedge-data \
  --bind 0.0.0.0:8095
```

## ClickHouse + Redis（可选）

`ireland-ingest` 支持以下环境变量：

- `POLYEDGE_CH_URL`（示例：`http://127.0.0.1:8123`）
- `POLYEDGE_CH_DATABASE`（默认 `polyedge`）
- `POLYEDGE_CH_SNAPSHOT_TABLE`（默认 `snapshot_100ms`）
- `POLYEDGE_CH_TTL_DAYS`（默认 `30`）
- `POLYEDGE_REDIS_URL`（示例：`redis://127.0.0.1:6379/0`）
- `POLYEDGE_REDIS_PREFIX`（默认 `polyedge`）
- `POLYEDGE_REDIS_TTL_SEC`（默认 `7200`）
- `POLYEDGE_SINK_BATCH_SIZE`（默认 `200`）
- `POLYEDGE_SINK_FLUSH_MS`（默认 `1000`）
- `POLYEDGE_SINK_QUEUE_CAP`（默认 `20000`）

`api` 支持 Redis 读取：

- `POLYEDGE_REDIS_URL`
- `POLYEDGE_REDIS_PREFIX`

## systemd 一键安装

```bash
# ireland
bash scripts/setup_data_backend_systemd.sh ireland

# tokyo
bash scripts/setup_data_backend_systemd.sh tokyo
```

脚本会自动关闭旧的 `polyedge-recorder.service`，避免旧链路干扰。
