# VENUE

当前产品版本：**v0.1.0**（[VERSION](VERSION) / [更新说明](docs/CHANGELOG.md)）。

VENUE 提供 Binance 交易终端、KOL 人工带单和对冲网格。VenueFlow 是原生桌面客户端，Web 提供邀请注册、账户管理和跟单设置；交易统一由服务端单个多账户 `venue-executor-binance` 执行。Binance KOL、终端和 Binance Grid 使用 Portfolio Margin UM 双向持仓账户；另外五所通过独立策略入口执行，支撑分批做多首个闭环使用 Bybit LIVE。

本页按当前代码入口说明能力。代码存在、版本推送、服务器部署和实盘验收是独立状态；实际运行版本与跟单速度须通过对应部署及测量记录确认。

## 当前功能与边界

| 功能 | 当前实现 | 使用边界 |
|---|---|---|
| 交易机器人列表 | 桌面统一展示内置 Grid 与 KOL 带单机器人，支持新建、编辑及启停 | 私有列表，无策略广场；同一 KOL 最多保存 20 条带单配置，同时最多一条非停止实例 |
| KOL 人工带单 | 同步新普通限价单、市价单和 STOP_MARKET 止损单；每账户独立选择定比或定额 | 需带单授权、机器人运行及跟随关系各自启用；不复制其他 Algo 类型，不追补仓位差异 |
| 账户与邀请 | 登录、免费桌面注册、Web 邀请注册、API 加密保存与只读验证、KOL 托管账户管理 | 注册、保存和验证不自动启用；跟随账户启用须通过签名空仓、无挂单和未决命令检查 |
| 桌面终端 | 图表、盘口、持仓、委托、成交、仓位变更及资产；Post Only 开平仓、精确撤单、市价平仓和反开 | 私有数据来自用户作用域投影；反开先确认平仓，失败或未知结果不继续增仓 |
| Binance 对冲网格 | 配置和签名事实驱动 Planner、目标订单与补撤收敛，共用命令账本 | 不依赖旧 Actor/WAL；旧账户迁入、恢复及性能须独立验收 |
| Web | 用户登录、邀请页、API、托管跟随账户、跟单状态和人工带单；独立 `/ops` 运营入口 | 使用兼容单实例带单接口；完整浏览器交易终端尚非桌面功能的等价实现 |
| 五所独立策略与网格 | Bitget、Bybit、Gate.io、OKX、Hyperliquid 的独立命令、条件单及网格生命周期 | 共用 Executor；逐所验收，不扩展跨所 KOL 或原子成交 |
| 支撑分批做多 | Binance 参考行情、Bybit 执行；桌面配置、签名预检及生命周期 | 独立账户、多币共享预算；精确资金费及其他执行所仍有增强门 |
| 冻结旧 Node | 保留旧账户执行与恢复兼容代码 | 不作为新用户入口；Scalping 暂缓 |

主账户可从币安原生客户端或 VENUE 下单；同步依据是认证账户订单事实，与下单界面无关。停用带单会撤销程序子单剩余量，不自动平仓；不确定请求按原 `clientOrderId` 查单。桌面切换执行账户不代表带单源已完成切换，须遵守[带单源交接契约](docs/LEADER_ORDER_MIRROR.md)。

初期准入上限为 5 个启用 KOL、200 个启用跟随账户。完整可用性还需隔离 PostgreSQL、目标主机容量及一组 KOL 与两个真实跟随账户的 Canary 验收；UI 可见或离线 fixture 不替代这些门。

## 代码与进程

| 目录 | 职责 |
|---|---|
| `apps/ui/desktop` | VenueFlow：Rust、eframe/egui 桌面终端 |
| `apps/ui/web` | Next.js、React、TypeScript 用户 Web 与同源 BFF |
| `apps/venue-control` | Control API、迁移、共享 Executor、带单授权与独立策略管理员工具 |
| `crates/venue-control-protocol` | UI 与 Control 的版本化协议 |
| `crates/venue-domain`、`venue-strategies`、`venue-indicators` | 规范类型、纯策略规划和共享指标 |
| `crates/venue-gateway-*` | 交易所签名、原生协议和规范事实转换 |
| `apps/venue-node`、根 `src/` 及旧 Runtime/Storage | 冻结执行链和恢复兼容；共享边界见架构文档 |

Control 负责认证、配置和命令入账；Executor 负责物理交易。PostgreSQL 保存凭证密文、配置、投影与耐久命令。账户内串行、账户间有界并发；同一真实账户不能同时由旧 Node 和新 Executor 下单。

## 开发入口

先读 [UI 入口](apps/ui/README.md) 和 [代码地图](docs/CODEMAP.md)。Rust 工具链固定为 1.98.0；Web 使用与 CI 一致的 Node.js 24，依赖以 lockfile 为准。

```powershell
# 在仓库根目录：检查缓存准入，再验证受影响包。
./scripts/Invoke-VenueBuild.ps1 -CheckOnly
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('check','--locked','-p','venue-control')

# 构建桌面客户端，完成后从 guard 输出的固定目录启动二进制。
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('build','--locked','--release','-p','venueflow','--bin','venueflow')
./scripts/Start-VenueFlow.ps1
```

Web 在 `apps/ui/web` 执行 `npm ci`、`npm run typecheck`、`npm test`、`npm run build` 和 `npm run verify:boundary`。运行配置与浏览器验证见 [Web 指南](docs/WEB.md)；Control/Executor 的数据库、主密钥和角色配置见 [账户管理](docs/ACCOUNT_MANAGEMENT.md) 与 [发布及回滚](docs/DEVELOPMENT.md#executor-release)。Ubuntu 构建使用本机 `scripts/Build-VenueUbuntu.ps1 -Component Control`。

主工作区 `G:\Venue` 保持主线，独立任务在 `codex/*` 分支和工作树开发；验证后合入 `master`，标签固定到发布提交。分支合并或推送不会自动更新运行中的桌面或服务器。详见 [开发、构建与合并规则](docs/DEVELOPMENT.md)。

## 文档

- [文档目录](docs/README.md)：按使用、开发和运维查找说明。
- [架构](docs/ARCHITECTURE.md) / [代码地图](docs/CODEMAP.md)：进程职责、调用链与源码定位。
- [KOL 产品契约](docs/KOL_COPY_MVP.md) / [带单机器人](docs/LEADER_ORDER_MIRROR.md)：授权、数量、生命周期和验收。
- [Grid 契约与旧运行时保护](docs/GRID_RUNTIME_REFACTOR.md)、[多交易所执行](docs/MULTI_VENUE_EXECUTOR.md)、[支撑分批做多](docs/SUPPORT_MARTINGALE.md)：各策略的准入、生命周期和验收。

Git 只保存源码、非秘密配置、脚本、长期文档和小型 fixture。`.env`、凭证、数据库、构建发布产物及 `artifacts/` 不进入 Git；旧未决 WAL、Unknown 和 checkpoint 是恢复事实，不属于文档或缓存清理对象。
