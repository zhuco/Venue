# VENUE 工作规则

反馈结果简短，不超过 500 字。只处理当前获准任务，保留已有未提交改动；不预建未来模块、公共 SDK、插件系统或多租户控制面。

## 读取入口

先读 [CODEMAP.md](CODEMAP.md)，按任务打开入口和直接依赖；文档索引见 [docs/README.md](docs/README.md)。

| 任务 | 必读契约 |
|---|---|
| 桌面或 Web | [UI 入口](apps/ui/README.md)及对应目录 AGENTS.md |
| KOL 产品、邀请、账户与容量 | [KOL 契约](docs/KOL_COPY_MVP.md) |
| 带单授权、数量、订单同步与停止 | [订单同步](docs/LEADER_ORDER_MIRROR.md) |
| Binance Grid、旧 Node 或账户迁入 | 完整阅读 [Grid 与冻结运行时边界](docs/GRID_RUNTIME_REFACTOR.md) |
| 另外五所独立命令或策略 | [多交易所执行](docs/MULTI_VENUE_EXECUTOR.md)及具体策略文档 |
| 构建、验证、发布与回滚 | [开发指南](docs/DEVELOPMENT.md)；构建前完整阅读其中构建规则 |

## 架构与交易安全

- 活动代码以根 Cargo workspace 和独立 `apps/ui/web` npm 应用为准；桌面在 `apps/ui/desktop`。职责与兼容调用以 [架构](docs/ARCHITECTURE.md)为准，不因名称含 legacy 删除仍被调用的实现。
- Control 负责认证、配置与命令入账；单例 `venue-executor-binance` 承载 KOL、终端、Binance Grid 和五所独立策略。账户内串行、账户间有界并发，PostgreSQL advisory lock 保证单实例；不新增每账户进程、Actor、writer lease、本地 WAL 或通用策略 runtime。
- PostgreSQL 命令账本、稳定 `clientOrderId` 和签名查单是新链耐久边界。超时、崩溃或响应不完整保留原身份进入 `ReconcileRequired`，确认前不重发；不得清账、伪造终态或用 HTTP ACK 冒充成交。
- 新 Executor 与冻结旧 Node 按真实账户互斥。未决 WAL、Unknown、checkpoint、成交游标、订单和仓位未安全收敛前不得迁入、重写或删除恢复事实。新网格从配置与签名订单/持仓收敛，不导入旧 Actor/WAL 状态。
- KOL 仅支持 Binance Portfolio Margin UM 的精确 `LIVE`，初期最多 5 个启用 KOL、200 个启用跟随账户；不扩展跨所 KOL。五所独立策略通过 `multi_venue_*` 与 `DurableAccountGateway`，逐所验证身份、权限和持仓模式；Hyperliquid Net 须明确策略方向。Scalping 暂缓。
- 不新增测试网、demo、Shadow 或隐式布尔运行模式；fixture/mock 是离线验证手段。源码、离线测试、部署、签名回读和真实成交分别报告，不能互相替代。部署、实盘和账户迁移须在当前任务授权范围内。
- KOL 同步普通限价、确认原生身份的市价和 `STOP_MARKET`；限价保留价格与 GTC/PostOnly，市价按原生源单只复制一次，不追补仓位差异。改止损先确认旧子单撤销终态；停止/撤权撤程序子单剩余量，不自动平仓。权限与授权版本发送前重验，初始授权和明确撤权遵循订单同步契约。
- 开仓按持久化数量策略满足步长和最低名义额；新 KOL 命令采用 `exchange_account`，旧未决命令保留 `stored_limits` 等原规则。数量、保证金和交易所最大量仍校验，不机械抬高源价格。平仓按同代新鲜签名持仓扣除预留量向下裁剪；PM UM Hedge 原生订单不得发送被禁止的 `reduceOnly`。
- API 密钥由 Control 在现有 AES-256-GCM 边界加密存 PostgreSQL，仅验证与共享 Executor 可短时解密。明文不得进入响应、页面、TOML、日志、错误或工件；KOL 不得读取跟随者密钥。Binance 准入须验证读取/UM 交易权限、提现关闭、PM 账户与双向持仓。

## 代码与依赖

- 手写源文件最多 2000 个物理行；入口只声明、组合和重导出，新增行为超限前按职责拆分。
- `domain` 不依赖业务模块；原生协议只存在于交易所 adapter。策略不得依赖具体交易所、凭证、原生字段或物理订单客户端。规范交易对使用大写 `BASE/QUOTE` 的 `domain::Symbol`。
- 复用规范类型、指标、归一化和订单事实，不复制实现；不以 `unsafe`、`unwrap`、`expect`、`panic!` 处理运行时外部输入。注释只解释边界、不变量、失败语义和非显然原因。
- 新依赖先查 workspace 与 Cargo.lock，说明现有依赖的具体缺口，同时加入实际调用与专项测试。同用途第二套依赖须有可验证缺口、限定边界与退出条件，不因偏好替换或提前安装。
- 基线：`tokio`、`reqwest`、`tokio-tungstenite`、`serde/serde_json`、`rust_decimal`、`thiserror`、`tracing/tracing-subscriber`、`bytes`。
- 状态与通信：`arc-swap`、`parking_lot`、`tokio::sync`；UI/同步线程边界用 `crossbeam-channel`。凭证用 `secrecy/zeroize`，能力用 `bitflags`。
- PostgreSQL 用 `sqlx`；仅明确需要本地 SQLite 时使用 `rusqlite`，不引入其他 ORM。冻结 Stage 7 的直接 `tungstenite` 不新增调用，也不为统一依赖破坏现有恢复。

## 构建与验证

- Rust 操作必须走所在工作树 `scripts/Invoke-VenueBuild.ps1`；桌面使用 `scripts/Build-Run-VenueFlow.ps1`，Ubuntu 使用 `scripts/Build-VenueUbuntu.ps1` 本机交叉编译后上传，不在生产服务器日常编译。专项脚本已持 guard 时不得再次套锁。
- 本机所有工作树只复用 `G:\Build\Venue\main`、`slot-1`、`slot-2`，最多两项受控构建。禁止原始 Cargo、临时脚本、环境变量、`--target-dir` 或嵌套目录绕过入口；锁忙最多等 60 秒后报告，不抢锁、不终止他人进程。
- 准入：`G:\Build\Venue` 总量不超过 500 GiB，物理宿主 F 空闲至少 100 GiB，G 至少 20 GiB。入口检查不是操作系统硬配额；超限报告，不自动清理。Ubuntu 源码/工具/版本化产物根为 `G:\Build\Venue\ubuntu`，Cargo 仍复用 slot-2。
- 保留有价值的增量缓存，不例行 cargo clean。清理仅限已核准、登记、无占用的缓存，须校验绝对路径和重解析点并取得对应锁；不得删除整个 Build 或项目目录。旧会话下一次构建前重读全局及当前工作树 AGENTS.md。
- 文档/注释只验证路径、引用、命令一致性、`git diff --check` 和仓库卫生，不启动编译。局部代码验证受影响包及直接契约；交易安全另覆盖幂等、账户队列、签名查单、数量/权限和超时对账。
- 公共契约、依赖、架构变更或正式发布前，经统一入口集中执行 fmt/check/test workspace 基线及 `scripts/verify_repository_hygiene.ps1`；具体命令见开发指南。之后局部增量不重复无关全量测试；记录源码范围与跳过项，相关验证失败不得宣称完成。

## 文档与文件维护

- README 介绍项目，CODEMAP 定位代码，docs/README 导航；长期说明集中 docs，组件 README 只留入口。AGENTS 只保留工作规则，业务细节链接到唯一契约。
- 已实现开发计划删除步骤和完成流水，保留当前功能/代码入口；未完成能力和验收门明确标注，不因源码存在宣称验收通过。重复说明合并后修复引用，不为删除正文另建历史归档。
- 目录、模块、binary、CLI 或主要入口变化同步 docs/CODEMAP.md；KOL 范围或语义变化同步对应 KOL/订单同步契约，Grid/旧运行时兼容变化同步 Grid 契约。发布摘要维护 CHANGELOG，详细历史由 Git 保存。
- Git 只跟踪源码、非秘密配置、脚本、长期文档和小型 fixture；不跟踪 bak、构建/发布产物、工具链、凭证、日志、数据库或 artifacts。`G:\Venue\artifacts` 是冻结恢复工件，文档整理不授权清理它。
- 已合并工作树删除前，确认改动已提交或等价整合；不丢弃未审查内容。历史对 `G:\Venue\bak` 的删除授权不扩大到数据库、凭证、恢复工件或其他项目。
