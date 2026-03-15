# CLAUDE.md

## 目录结构

```text
crates/
  core_types/            基础领域类型
  feed_polymarket/       Polymarket 实时数据接入
  feed_reference/        参考价格源接入
  forge_tokyo_relay/     东京采集与 UDP 转发独立产物
  forge_wire/            东京 <-> 爱尔兰稳定 wire schema
  market_discovery/      市场发现与筛选
  polyedge_forge/        爱尔兰 recorder + API + strategy/live runtime
heatmap_dashboard/       前端页面
scripts/forge/           Forge 运维与部署脚本
```

## 文件职责

- `crates/forge_wire/src/lib.rs`
  只定义跨区域传输对象，禁止混入运行时逻辑。
- `crates/forge_tokyo_relay/src/main.rs`
  东京独立入口，只负责参考价格采集与 UDP 转发。
- `crates/polyedge_forge/src/ireland.rs`
  爱尔兰运行时入口，负责 recorder 与 API 两条服务线。
- `scripts/forge/setup_tokyo_forge.sh`
  东京主机安装独立 relay 服务。
- `scripts/forge/setup_ireland_forge.sh`
  爱尔兰主机安装 recorder 与 API 服务。

## 模块边界

- 东京只允许依赖 `forge_wire`、`core_types`、`feed_reference` 和必要基础库。
- 爱尔兰 recorder 不允许再携带 API 启动语义。
- API 端口、dashboard 资源、live 执行状态只属于 `ireland-api`。
- 东京与爱尔兰之间只通过 `forge_wire` 交换数据，不共享运行时状态。

## 架构决策

- 把东京从 `polyedge_forge` 中拆出为独立 crate。
  原因：东京只做采集转发，不该跟着爱尔兰交易/API 代码一起构建和部署。
- 抽出 `forge_wire`。
  原因：把跨区域协议固定成清晰边界，避免东京和爱尔兰通过大包体隐式耦合。
- 移除 recorder 的内嵌 API 分支。
  原因：采集服务不该顺手起 API，这会污染职责和部署粒度。

## 开发规范

- 改东京 relay，只构建和部署东京。
- 改爱尔兰 API 或 strategy，不部署东京。
- 改 wire schema 时，先确认东京和爱尔兰兼容窗口，再分别发布。
- 所有部署必须能追溯到明确提交，不接受服务器本地脏改动。

## 变更日志

- 2026-03-15：API 默认端口统一从 `9810` 切到 `9830`。
- 2026-03-15：部署脚本发送远端 bash 前统一去除 BOM/CRLF。
- 2026-03-15：新增 `forge_tokyo_relay` 与 `forge_wire`，东京从 `polyedge_forge` 中独立。
- 2026-03-15：移除 ireland recorder 的内嵌 API 启动参数与分支。
