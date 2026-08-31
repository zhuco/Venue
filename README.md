# VENUE

当前版本：**v0.1.0-alpha.2 · 开发预览**（[版本定义](VERSION) / [更新说明](docs/CHANGELOG.md)）。

多交易所共用 Rust 账户执行链，承载 Grid、Scalping、Copy 与手动语义交易；用户 Web 与内部桌面客户端共用
Control schema v2。当前先完善 Binance；其余五所进入第二批验证与实盘。Scalping 暂缓，不作为当前收工门槛。六所有实现不等于已验收，本版本不是可无人值守上线的稳定版。

## 当前功能

| 功能 | 已提交范围 | 尚未完成的验收 |
|---|---|---|
| 六所网关/Node | Binance、Gate.io、Bitget、Bybit、OKX、Hyperliquid；统一账户 Host/Lane/WAL，公共盘口与成交接线 | 各账户模式/订单族能力、策略驱动、旧 writer/WAL 逐所接管 |
| Copy | 纯目标规划、关系/任务、跨零物理桥、签名结果与 ledger/drift 实现 | 持续真实 leader 来源、异常恢复及产品端到端 |
| Grid | 共享 reducer、Actor checkpoint、行情 FeatureSource | 第一批 Binance 私流/多 symbol 协同；其他五所第二批 |
| Scalping | 保留既有实现与失败关闭 | 暂缓处理，不进入当前两批开发与实盘验收 |
| 手动交易/账户 | 显式限价语义、自有手动单撤单；Control 登录和 Binance 凭证加密验证 | 全 scope 撤单、策略协同、其他交易所自助绑定 |
| UI | VenueFlow 原生/WASM、历史 K 线/EMA-ADX、执行事实视图、Windows 登录凭证库；独立响应式 Web/BFF | Web 手动下单闭环、真实连通、五视口截图、易用性与分段速度验收 |
| Ubuntu 构建 | Windows 本机交叉编译六个 Linux Node，manifest/哈希和固定缓存 | 每次发布仍须动态库核验与独立实盘接管 |

## 文档入口

长期文档统一存放在 [docs/ 文档目录](docs/README.md)。

- [架构与实际技术栈](docs/ARCHITECTURE.md)：进程、依赖边界、已实现能力。
- [CODEMAP](docs/CODEMAP.md)：按功能定位代码。
- [开发指南](docs/DEVELOPMENT.md)：验证范围、构建、合并与版本规则。
- [运行时/网格契约](docs/GRID_RUNTIME_REFACTOR.md)：单 writer、风险、WAL、Unknown 和接管。
- [迁移开发契约](docs/UNIFIED_GATEWAY_WEB_MIGRATION.md) / [剩余工作](docs/REFACTOR_IMPLEMENTATION_GOALS.md)。
- [停用与冻结入口](docs/DEPRECATED.md)：哪些已删除、哪些仍需恢复兼容、使用什么替代入口。
- [Node 使用说明](docs/NODE.md)、[Web 使用说明](docs/WEB.md)、[账户管理](docs/ACCOUNT_MANAGEMENT.md)。
- [本机及 Ubuntu 构建规则](docs/BUILD_POLICY.md)。

## 最短开发路径

主工作区为 `G:\Venue`；worktree 是隔离开发目录，合并后文档与源码都在主工作区。
Rust 固定 1.98.0；Web 使用 Node.js 24（与 CI 对齐），精确前端依赖由 lockfile 固定。

```powershell
# 在项目根目录；不连接交易所。
./scripts/Invoke-VenueBuild.ps1 -CheckOnly
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('check','--locked','-p','venue-runtime')
```

Web 在 `apps/venue-web` 执行 `npm ci` 后按其 README 验证。Ubuntu 编译入口为
`scripts/Build-VenueUbuntu.ps1`，专用目录 `G:\Build\Venue\ubuntu`；不在弱服务器日常编译。

## 安全与版本边界

网关模式只有精确 `LIVE`；没有 demo/testnet/Shadow 运行模式。离线 fixture 是测试，不是实盘准入。
UI/Control 只提交语义命令，Node 独占账户 writer；Unknown 不重投。
根 `.env`、数据库、运行工件和构建产物不进 Git，不得删除已有持仓相关 WAL/checkpoint。

本版本包含一并提交的桌面 UI、共享指标、账户凭证库和文档整理；服务器历史运行包不自动升级。
Web 允许提交手动下单，仍由 Node 执行；不能把“允许开发”写成已完成实盘验收。旧 KOL 后端不参与构建，停用入口不恢复运行。
