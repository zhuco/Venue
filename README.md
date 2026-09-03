# VENUE

当前版本：**v0.1.0-alpha.2 · 开发预览**（[版本定义](VERSION) / [更新说明](docs/CHANGELOG.md)）。

仓库包含既有多交易所交易内核、用户 Web 和内部桌面客户端。当前产品目标是 Binance KOL 跟单 MVP 与事实驱动 Binance 对冲网格共用单例 Executor；旧 Node/Actor/WAL 网格接管路线停止。Gate.io、Bitget 及其余交易所迁移仍暂停。本版本仍是开发预览，不是可无人值守上线的稳定版。

## 当前功能

| 功能 | 已提交范围 | 尚未完成的验收 |
|---|---|---|
| Binance KOL MVP | Control 已有注册/登录、API 密文与 Portfolio Margin 只读验证；Web、Copy 与手动交易已有可复用部件 | 邀请归属、公开登录/绑定页面、KOL 文案与终端、轻量多账户 Executor、真实快速跟单闭环 |
| 六所网关/旧 Node | Binance、Gate.io、Bitget、Bybit、OKX、Hyperliquid adapter 及旧 Host/Lane/WAL 代码 | 当前冻结；不继续逐所接管，不作为 KOL MVP 执行路径 |
| Copy/Grid | Copy 已进入统一 Executor；Binance Grid 正重写为配置+签名事实→Planner→Reconciler | 旧 Copy/Actor/Grid 接管链退出；新 Grid 需通过故障、重启与实盘 Canary |
| Scalping | 保留既有实现与失败关闭 | 暂缓处理，不进入当前 MVP 验收 |
| 手动交易/账户 | 显式限价语义；Control 登录和 Binance 凭证加密验证 | KOL Web 终端、跟随者自助绑定、权限与真实连通验收 |
| UI | VenueFlow 原生/WASM 与独立响应式 Web/BFF | 面向用户的邀请落地页、注册登录、API绑定、跟单状态和KOL可编辑说明 |
| Ubuntu 构建 | Windows 本机交叉编译六个 Linux Node，manifest/哈希和固定缓存 | 每次发布仍须动态库核验与独立实盘接管 |

## 文档入口

长期文档统一存放在 [docs/ 文档目录](docs/README.md)。

- [架构与实际技术栈](docs/ARCHITECTURE.md)：进程、依赖边界、已实现能力。
- [CODEMAP](docs/CODEMAP.md)：按功能定位代码。
- [开发指南](docs/DEVELOPMENT.md)：验证范围、构建、合并与版本规则。
- [Binance KOL 跟单 MVP](docs/KOL_COPY_MVP.md)：当前任务、目标架构、开发顺序和验收的唯一入口。
- [旧运行时/网格兼容](docs/GRID_RUNTIME_REFACTOR.md) 与 [旧迁移导航](docs/UNIFIED_GATEWAY_WEB_MIGRATION.md)：仅供维护冻结代码和已有工件。
- [停用与冻结入口](docs/ARCHITECTURE.md#deprecated)：哪些已删除、哪些仍需恢复兼容、使用什么替代入口。
- [Node 使用说明](docs/NODE.md)、[Web 使用说明](docs/WEB.md)、[账户管理](docs/ACCOUNT_MANAGEMENT.md)。
- [本机及 Ubuntu 构建规则](docs/DEVELOPMENT.md#build-policy)。

## 最短开发路径

主工作区为 `G:\Venue`；worktree 是隔离开发目录，合并后文档与源码都在主工作区。
Rust 固定 1.98.0；Web 使用 Node.js 24（与 CI 对齐），精确前端依赖由 lockfile 固定。

```powershell
# 在项目根目录；不连接交易所。
./scripts/Invoke-VenueBuild.ps1 -CheckOnly
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('check','--locked','-p','venue-runtime')
```

Web 在 `apps/ui/web` 执行 `npm ci` 后按其 README 验证；桌面端位于同级 `apps/ui/desktop`。Ubuntu 编译入口为
`scripts/Build-VenueUbuntu.ps1`，专用目录 `G:\Build\Venue\ubuntu`；不在弱服务器日常编译。

## 安全与版本边界

网关模式只有精确 `LIVE`；没有 demo/testnet/Shadow 运行模式。离线 fixture 是测试，不是实盘准入。
新链由单个 Binance 多账户 Executor 消费 PostgreSQL 命令账本；账户内顺序执行，稳定 `clientOrderId` 超时后先查单、不盲目重投。
根 `.env`、数据库、运行工件和构建产物不进 Git，不得删除冻结旧账户已有持仓相关 WAL/checkpoint。

本版本包含一并提交的桌面 UI、共享指标、账户凭证库和文档整理；服务器历史运行包不自动升级。
Web 不接触交易所密钥或直连 Binance；不能把“允许开发”写成已完成实盘验收。旧 KOL 后端不参与构建，停用入口不恢复运行。
