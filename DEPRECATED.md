# 停用、冻结与替代入口

更新：2026-09-01

本页管理项目自己的旧方法和功能，不宣称同名上游库已经弃用。
“入口已删除”不等于历史源码全删，“源码冻结”不等于远端旧进程已停止。
本轮只清理失效文档和标记状态；不删除代码、数据库、`bak/` 或运行恢复工件。

## 1. 已停用的生产入口

| 旧入口/方案 | 状态与替代 |
|---|---|
| 根 `hedged-grid-binance/gate/bitget` binary、对应 feature 和旧发布入口 | 根 manifest 已移除；使用 `apps/venue-node` 六个固定 binary。不得按旧教程恢复 |
| Node 后透传 Stage 7 的 `grid-stop`、`grid-external-algo-cancel`、`grid-legacy-binance-stop` 等 | 当前 Node 不接受这些子命令；常驻模式使用 `run --runtime-config`，实例动作通过 Control 语义命令，范围见 Node README |
| 旧 Scalping 手造 candidate 直送 Host 的组合路径 | 生产不再从旧壳进入；Node 消费真实 FeatureSource/engine/Actor checkpoint，保护缺失仍禁止自动入场 |
| KOL 重复网关/后端、旧 `/v1` DTO、UI 模拟交易/旧 mutation gate | 不迁移为新系统运行依赖；Web 重写为 schema v2/BFF，UI 不直接交易 |
| PID/会话专用 Cargo target、随手指定 `--target-dir` | 停用；仅 main/slot-1/slot-2 与受控脚本 |
| 弱服务器上的日常 Cargo 编译 | 不作默认方案；本机 `Build-VenueUbuntu.ps1` 编译后受控上传，Linux 打包脚本只是备用 |

根 package 仍有两个离线 verifier：`verify-grid-inventory-recovery` 与 `verify-grid-exposure-shadow`。
其中 Shadow 是旧证据名称，不是可选择的交易所运行模式。

## 2. 冻结保留，不作新入口

| 方法/源码范围 | 当前用途 | 删除前提 |
|---|---|---|
| `src/runtime/grid/stage7_*`、`src/runtime/legacy/hedged_grid_live.rs::run_binance_stage7_grid` | 历史行为、工件与恢复证据；无根生产 binary | 逐所新链等价与旧 writer/WAL 接管证明齐全 |
| `src/runtime/scalping/*live*` 与旧控制/自动选币组合 | 恢复和策略行为参考，不重新接旧 writer | 对应 Node 安全输入、保护/退出及恢复契约验收 |
| `src/execution/external_algo_cleanup.rs`、旧 recovery_writer/Canary 壳 | 历史精确身份与证据契约，不能作为新链旁路 | 调用和恢复依赖清零，保留必要只读解析 |
| `crates/venue-runtime/src/account/physical_recovery*`、各 adapter 旧 recovery collector | 迁移兼容，不发行当前账户 writer 权限 | 对应恢复数据与契约有替代并验证 |
| `crates/venue-gateway-api/src/capability_promotion.rs` 的普通 `promote/authorize` | 保持 `AuthorityUnavailable`；probe 不能提升成交易权限 | 旧契约引用清零，不破坏失败关闭测试 |
| 根 package 直接 `tungstenite` 与现有阻塞 transport | 本项目冻结例外；不增加旧调用点 | 行为等价、延迟和断线恢复验证后逐步替换 |
| `bak/`、`G:\kol` | 冻结迁移参考，不构建/运行 | 不在当前授权清理范围 |

## 3. 仍在使用，不能误删

- `preflight`、`canary-place`、`canary-cancel` 与 `run` 都是当前 Node 子命令；Canary 不是已删除功能，
  它仍须经 Runtime/Host/Lane/WAL，不能作为旁路。
- `--legacy-v1-handoff`：当前 Binance/Gate/Bitget 的外层启动参数必须提供已验证前驱记录；
  Bybit/OKX/Hyperliquid 不接受该记录。它是接管保护，不是旧生产命令。
- `venue-execution` 中的账户 canonical root/锁、writer lease 及前驱记录，仍可能被当前 Host 引用；
  不能因名字含 legacy/lease 就整体删除。
- Actor durable-applied、Control delivery/inbox/outbox 与存储版本字段仍有当前调用；
  它们不是另一套物理 writer，也不能不做兼容就更名或删除。
- `PublicTradeOrdering::Unsequenced` 是旧记录安全读取状态，不是可直接喂策略的“默认连续流”。
- VenueFlow WASM 仍是内部客户端；独立 Next.js Web 并不意味着 WASM 应删除。

## 4. 技术栈纠偏

当前实际版本和官方支持来源集中在 [架构技术栈](ARCHITECTURE.md)。
Node.js 24 是 CI/默认 Web 基线；不要沿用已 EOL 的 Node 20/23/25。
Next.js 16 不再提供 `next lint`，也不在 `next build` 中自动 lint；当前仓库未安装专用 JS linter。
Axum、FastAPI、SQLite ORM、MQTT、Condor/Hummingbot 核心都不是现行实现，不能写成已依赖服务。
本轮没有批量升级依赖，也没有执行完整 CVE/供应链审计。
