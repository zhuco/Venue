# Venue Node：六个固定账户节点

版本与范围见 [项目 README](../README.md)；本页对应 `src/lib.rs` 和 `src/runtime_config.rs` 的实际 CLI，
替代旧 Stage 7 透传教程。六所 binary 各自只链接一个 adapter，所有交易都经过统一账户链。

当前旧三所按 Binance、Gate.io、Bitget 接管；终端和真实跟单先完善 Binance。后续三所与 Scalping 暂缓，本页保留的配置字段不是启用指令。

## 构建

从项目根目录执行；本机所有 Cargo 必须受控：

```powershell
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('build','--locked','-p','venue-node','--no-default-features','--features','binance','--bin','venue-node-binance')
```

六个 feature 为 `binance / gate / bitget / bybit / okx / hyperliquid`，对应 `venue-node-<feature>`。
只给所需 feature，不用全 feature 构建发布 binary。六所产物隔离专项
`scripts/verify_venue_node_binaries.ps1` 自带 guard；Ubuntu 用
[Build-VenueUbuntu](DEVELOPMENT.md#build-policy)，不另开 target。

## 启动契约

外层必填：`--mode LIVE`、`--trading-account-id <内部UUID>`、`--symbol BASE/QUOTE`、
`--artifacts-base <绝对路径>`。Node 派生 `<base>/<venue>/LIVE/<account>`，不接受 `--artifacts-root`。
本机 base 固定 `G:\Venue\artifacts`；非 LIVE 在凭证/endpoint/工件设置前拒绝。

Binance/Gate/Bitget 还必须在分隔符前提供 `--legacy-v1-handoff <绝对JSON路径>`，
由 `LegacyV1WriterPredecessor` 校验精确旧 registry/绑定及目标 `trading_account_id`。启动同时持有旧 writer
锁和新账户 `writer.lock`，不能伪造记录或以另一账户复用前驱来跳过真实 writer/WAL 接管。
v2 handoff 必须哈希绑定旧 WAL 的 `strategy_instance_id` 与 `run_id`。既有 v1 只有在旧锁仍受持有且冻结 WAL
的全部物理 Owner 唯一、可由导入精确推导时才兼容；配置值本身不被信任。导入的每条 physical mutation 都须匹配
该冻结 Owner。它们只用于把精确旧 Owner 限定为后续 cancellation-only custody 候选，绝不替换成新 UUID 或授予新开仓权限。
前驱锁取得后，Node 仅在旧 `commands.jsonl` 完整、终态且无 `Unknown` 时把它按 5 MiB 段复制到新的
账户 root，并以来源路径、字节数与 SHA-256、每个导入段的 SHA-256 写入一次性导入标记；后续新段可以正常
轮转，但任一已导入段篡改均拒绝启动。旧文件绝不改写。旧 Grid checkpoint、
路由和 Actor Applied 不会由此推测或伪造，缺少它们仍保持失败关闭，不能据此宣称 Grid 已接管。
Bybit/OKX/Hyperliquid 不接受该前驱参数。初期 Bybit/OKX 的 DOGE 为 `DOGE/USDT`，
Hyperliquid 为 `DOGE/USDC`，风险必须换算 USDT。

`--` 后四类子命令：

| 子命令 | 参数与含义 |
|---|---|
| `run` | `--runtime-config <绝对JSON路径>`；启动统一 resident 和 Control 循环，不要求 Canary 的 `--confirm-live` 参数 |
| `preflight` | `--confirm-live <小写venue>`；签名账户读取、取得账户锁和本地 WAL 恢复；不发送交易，但可能创建/更新恢复工件 |
| `canary-place` | 同上确认；另需 `--command-id`、`--client-order-id`、`--position-side long或short`、`--quantity`、`--limit-price` |
| `canary-cancel` | 同上确认；另需 `--command-id`、`--target-client-order-id`；只撤精确耐久身份 |

Canary place 为受 10U 单笔及账户累计门约束的 post-only 入场，不是任意订单 API。
existing position、未撤增险单、Unknown 或不完整签名证据会拒绝新增风险。
CLI 没有旧 `grid-*` 子命令；Stop/Flatten/手动 GTC/reduce-only 等走常驻 Control 语义路径，
且由已接线的策略/账户能力决定是否接受。不得将未支持的 scope 描述为已能执行。

## Runtime JSON

`NodeRuntimeConfig` 当前 `version=1`，配置不含凭证。必填内容：

- `mode / venue / trading_account_id / node_id`，必须与外层启动 binding 一致；
- `control.loopback_origin / poll_interval_ms / projection_interval_ms / lease_duration_ms / claim_limit`；
- 非空 `strategies`：`strategy_kind / instance_id / run_id / config_digest / config_epoch / symbol`；`manual` 是终端专用 dormant actor，不产生自动策略意图，且不得携带 Grid/Scalping 配置；
- Grid 还需 `grid.params`、`grid.recovery` 及可选库存恢复开关；
- Scalping 还需 `scalping.parameter_release_id / owner_scope / risk_budget`；
- `copy_leader_capital` 仅在非 Manual actor 显式启用时发布 leader 资本事实，不用账户总权益代替。

精确 enum 和界限以 `src/runtime_config.rs` 为准；当前 loader 要求首次配置 `config_epoch=1`。
不要把旧 `venue.grid.toml` 当作 runtime JSON。为避免误启实盘，本页不提供可直接交易的示例账户/config。
启动凭证来自进程环境或当前工作目录的根 `.env`，不输出其值。Control token 配置与 loopback 限制见 Control 入口。

## 安全和真实完成度

`AccountRuntimeHost` 组合唯一 Lane/Host；同一账户锁、WAL 与 Submitted 后 permit 是唯一物理链。
Unknown 只签名对账，不重投。preflight 也不能与同账户现有 writer 同时运行。
WAL 分段 5 MiB、单文件 10 MiB，根预算 256 MiB；未决状态和当前 checkpoint 永不作普通缓存清理。

六所公共盘口/成交已接入，Bitget/Hyperliquid 权威闭合 bar 尚缺；Scalping 自动入场保护仍未闭合。
Copy、手动交易与 Grid 已有桥接代码；终端必须绑定 `manual` actor，不能借用会自动运行的 Grid/Scalping 或拒绝通用手动 turn 的 Copy。完整生产验收和旧服务器接管不能由编译/单测推断。
后续工作见 [迁移契约](UNIFIED_GATEWAY_WEB_MIGRATION.md)，停用方法见 [清单](ARCHITECTURE.md#deprecated)。
