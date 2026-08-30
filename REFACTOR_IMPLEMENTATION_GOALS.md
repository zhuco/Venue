# VENUE 重构待实现 Goals

更新：2026-08-31

## 1. 本文职责

本文只保存当前尚未完成、可以直接交给 Codex 执行的实现 Goal。长期架构查
[`ARCHITECTURE.md`](ARCHITECTURE.md)，账户运行时、网格、恢复、单 writer 与接管约束查
[`GRID_RUNTIME_REFACTOR.md`](GRID_RUNTIME_REFACTOR.md)，代码入口查 [`CODEMAP.md`](CODEMAP.md)。

## 2. 当前待办

当前已批准一个长期 Goal：按 [`UNIFIED_GATEWAY_WEB_MIGRATION.md`](UNIFIED_GATEWAY_WEB_MIGRATION.md) 将 Stage 7 直接重构到
统一 Account Runtime/Execution Lane，完成六所逐家单 writer 接管，把 Copy semantic Applied 接到 follower 物理执行与签名事实，
并建立独立 `apps/venue-web`，选择性迁移 `G:\kol\apps\web` 的响应式体验。

该 Goal 必须按文档 T0–T8 有界子任务执行；代码和 Web 可按依赖并行，protocol、SQL migration、长期文档由单一整合者串行修改，
真实 mutation 全局串行。任务已获得既有真实账户的持续实盘授权：AI 可在 binding/能力约束内选择交易对，自行使用技术确认参数，
并在单账户累计名义风险不超过 10U 时逐所执行 Canary；初期 Bybit、OKX、Hyperliquid DOGE binding 仍固定 `DOGE/USDT`。
提款、转账、账户安全/杠杆/保证金设置、创建凭证、突破 10U、Unknown 时增险和双 writer 不在授权内。

不得为常规 Canary 逐次请求人工确认。需要用户凭证、主机/数据库权限、硬件签名、交易所后台设置或外部审批的事项先失败关闭，
继续完成其他安全子任务，并在其余可执行任务完成后形成一次性人工协助清单。

发布门禁以
`.github/workflows/workspace-gates.yml`、`scripts/verify_workspace_quality.ps1`、
`scripts/verify_repository_hygiene.ps1`、`scripts/verify_fixed_deployment_binaries.ps1`、
`scripts/verify_venue_node_binaries.ps1`、`scripts/verify_gateway_candidate_contract.ps1` 和
`scripts/verify_postgres_integration.ps1` 为准。

真实交易所 Canary、单 writer 接管和受保护运行工件不属于离线代码门禁，仍须遵守
[`GRID_RUNTIME_REFACTOR.md`](GRID_RUNTIME_REFACTOR.md) 的逐所顺序、签名对账和失败关闭规则；持续授权不豁免任何技术安全门。
