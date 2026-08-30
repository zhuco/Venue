# VENUE 重构待实现 Goals

更新：2026-08-30

## 1. 本文职责

本文只保存当前尚未完成、可以直接交给 Codex 执行的实现 Goal。长期架构查
[`ARCHITECTURE.md`](ARCHITECTURE.md)，账户运行时、网格、恢复、单 writer 与接管约束查
[`GRID_RUNTIME_REFACTOR.md`](GRID_RUNTIME_REFACTOR.md)，代码入口查 [`CODEMAP.md`](CODEMAP.md)。

## 2. 当前实现边界

Goal 1–8 的代码和离线门禁已并入活动 workspace：

- Runtime 以规范 Actor identity、当前 WAL head、generation、turn 与 replay state 持久化 Actor Applied；
- Binance、Gate.io、Bitget 输出完整同 attempt 恢复证据，保持现有 Stage 7 writer、订单编码和接管基线；
- Copy 的 LIVE-only PostgreSQL 工作流、PostgreSQL 强制 CI job、指标只读 Control 投影已接线；
- Bybit、OKX、Hyperliquid 使用账户级进程锁、串行 mutation、同一分段 `commands` WAL、WAL 内 Owner、Unknown 禁重投和一次性 dispatch permit；DOGE 账户已有未撤入场或签名非零持仓时拒绝继续增险，累计名义上限固定 10U；
- 六个 fixed binary 只接受精确 `LIVE`。新三所只开放显式 `preflight / canary-place / canary-cancel`，错误确认在凭证和工件 I/O 前拒绝；
- 旧 Stage 7 的多层 root/lease/receipt/handoff 只作已有三所迁移兼容，不复制到新路径。

真实交易所 Canary 不属于离线代码门禁。它必须在凭证、交易所状态和人工确认可用时逐所执行，且不能并行 mutation。

## 3. Goal 9：最终整合与发布门禁

**目标**：独占完成共享 Cargo/lock、长期文档、全仓质量门禁、提交与推送。

**必须通过**：

```text
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo test --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
scripts/verify_workspace_policy.ps1
scripts/verify_repository_hygiene.ps1
scripts/verify_fixed_deployment_binaries.ps1
scripts/verify_venue_node_binaries.ps1
scripts/verify_gateway_candidate_contract.ps1
git diff --check
```

PostgreSQL CI 必须实际连接临时 PostgreSQL，migration 或测试跳过即失败。构建和离线门禁不得读取真实凭证、连接交易所或发送 mutation。
真实 Canary 收据单独保存为受保护运行工件，不进入普通测试日志或 Git。

## 4. 实盘接管顺序

每所独立执行：停止旧 writer（如存在）、确认账户锁释放、收敛全部 `Submitted/Unknown`、签名读取订单/成交/持仓、运行只读
`preflight`、人工确认一笔不超过 10U 的 Canary、立即签名复核并撤单或确认终态。完成并观察一所后才开始下一所；失败时保持
Paused、保留 WAL/Unknown 工件并人工检查。
