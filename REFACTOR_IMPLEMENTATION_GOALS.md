# VENUE 重构待实现 Goals

更新：2026-08-30

## 1. 本文职责

本文只保存当前尚未完成、可以直接交给 Codex 执行的实现 Goal。长期架构查
[`ARCHITECTURE.md`](ARCHITECTURE.md)，账户运行时、网格、恢复、单 writer 与接管约束查
[`GRID_RUNTIME_REFACTOR.md`](GRID_RUNTIME_REFACTOR.md)，代码入口查 [`CODEMAP.md`](CODEMAP.md)。

## 2. 当前待办

当前没有已批准、尚未实现的 Goal。最近获准的 Copy 交付门禁已关闭：Node 以恢复后的真实 WAL head 持久应用规范 Copy Actor；Control/PostgreSQL 以 LIVE-only、幂等 revision 保存关系配置与投递闭环；VenueFlow 只经 Control API 查询和编辑关系，且不持有凭证、writer、WAL 或交易客户端。任何真实账户 Canary 仍须依照 `GRID_RUNTIME_REFACTOR.md` 逐账户单独授权。

新增任务必须先写成边界明确、可独立验收的 Goal，再进入活动 workspace。发布门禁以
`.github/workflows/workspace-gates.yml`、`scripts/verify_workspace_quality.ps1`、
`scripts/verify_repository_hygiene.ps1`、`scripts/verify_fixed_deployment_binaries.ps1`、
`scripts/verify_venue_node_binaries.ps1`、`scripts/verify_gateway_candidate_contract.ps1` 和
`scripts/verify_postgres_integration.ps1` 为准。

真实交易所 Canary、单 writer 接管和受保护运行工件不属于离线代码门禁，必须继续遵守
[`GRID_RUNTIME_REFACTOR.md`](GRID_RUNTIME_REFACTOR.md) 的逐所顺序和失败关闭规则。
