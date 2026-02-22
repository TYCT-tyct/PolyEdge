# PolyEdge 实时研究仪表板（独立版）

## 目标
- 100ms 级采集并展示 BTC 与 Polymarket 5m/15m Up/Down 市场
- 中文可视化：实时曲线、决策建议、交易日志、热力图、风险面板
- 与旧策略链路解耦，避免历史逻辑污染

## 功能
1. **实时采集**
- Binance WS: `btcusdt@trade`
- Polymarket CLOB: `/book?token_id=...`
- 市场发现：Gamma `/markets`（近到期窗口）

2. **衍生指标**
- `delta_pct = (btc - target)/target`
- `velocity_bps_per_sec`
- `acceleration_bps_per_sec2`
- `remaining_sec`
- `ev_yes / ev_no`（含官方费用公式）

3. **策略建议（研究模式）**
- 小仓入场 / 持仓 / 锁损 / 反向切换
- 交易日志与 PNL 实时记录
- 热力图：Delta × 剩余时间 -> 历史 UP 胜率

4. **模型接口**
- `ModelAdapter` 抽象接口
- 默认 `HeuristicModel`
- 可替换为未来 ML 预测器（胜率、反向概率、加仓优化）

## 运行
```bash
python scripts/live_research_dashboard.py --host 0.0.0.0 --port 8098
```

浏览器访问：
```text
http://127.0.0.1:8098
```

## 关键参数
- `--sample-ms`：采样周期（默认 100ms）
- `--discovery-sec`：市场发现周期
- `--entry-ev-threshold`：开仓 EV 阈值
- `--switch-ev-threshold`：切换 EV 阈值
- `--daily-loss-stop-pct`：日亏损熔断
- `--data-root`：JSONL 数据存储根目录

## 数据落盘
`<data-root>/<utc-date>/`
- `binance_ticks.jsonl`
- `market_snapshots.jsonl`
- `trade_log.jsonl`
- `round_summary.jsonl`
- `collector_errors.jsonl`

## 说明
- 当前是“研究执行模式”（模拟下单 + 实时决策），用于快速验证策略闭环与可解释性。
- 迁移到真实执行时，可复用该看板作为观测层，不改前端结构。
