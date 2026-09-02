# VENUE Workspace Rules

活动代码由根 `Cargo.toml` 声明的 Rust workspace（根 package、`apps/`、`crates/`）及独立 `apps/ui/web` npm 应用组成；两套 UI 统一位于 `apps/ui/`：`desktop` 是 VenueFlow/egui 桌面终端，`web` 是 Next.js/BFF 用户 Web，先读 `apps/ui/README.md` 判定入口。长期说明统一在 `docs/`。当前唯一产品目标是 Binance KOL 跟单 MVP：邀请注册、登录、API Key 绑定与验证、KOL 交易终端、快速跟单和 KOL 落地页。初期全站不超过 5 个启用 KOL、200 个启用跟单账户；Grid、Gate.io、Bitget 及其余交易所迁移与 Scalping 暂缓。

## 最短读取路径

1. 先读 `CODEMAP.md`，按任务只打开对应入口和直接依赖。
2. 当前开发范围、目标架构和验收统一查 `docs/KOL_COPY_MVP.md`。
3. 只有维护冻结 Grid/旧 Node、处理其已有仓位或工件时，才完整阅读 `docs/GRID_RUNTIME_REFACTOR.md`；该旧运行时不得成为 KOL MVP 的依赖模板。

## 目录与旧实现

- 用户已明确授权删除 `G:\Venue\bak`，不需要备份。该授权不扩大到数据库、凭证、运行恢复工件或其他项目。
- 活动兼容代码以 `docs/ARCHITECTURE.md` 为准，不因名字含 legacy 就删除仍被调用的代码。
- 已合并工作树清理前确认所有改动已提交或等价整合；禁止丢弃未审查内容。

## 实施与实盘

- 只实现当前获准任务，不创建未被当前需求使用的未来模块、公共 SDK、插件系统或多租户控制面。
- 默认按影响面验证：文档/注释只做静态检查，单模块修改只检查该 package 及直接契约，交易安全修改覆盖受影响的命令幂等、账户顺序队列、签名查单、数量/权限与超时待对账路径。跨模块公共契约、依赖或架构变更及正式发布前集中执行 `cargo fmt --all --check`、`cargo check --workspace --all-targets`、`cargo test --workspace`、`scripts/verify_repository_hygiene.ps1` 建立基线；基线后的局部增量不重复全工作区测试。记录验证对应的源码范围，相关验证失败时不得宣称完成或删除被替代实现。
- KOL MVP 只允许一个 `venue-executor-binance` 部署实例管理新链账户；账户在进程内使用独立串行队列和有界全局并发，不为每个账户启动进程、Actor、writer lease 或本地 WAL。PostgreSQL 命令账本、稳定 `clientOrderId` 和超时后的签名查单是唯一新链耐久边界。
- 新链与冻结旧 Node 必须按 `trading_account_id` 互斥分配；同一真实账户不得同时由两条路径下单。旧账户的未决 WAL、Unknown、仓位和工件未安全收敛前不得迁入新链，也不得为推进 MVP 删除或伪造旧状态。
- 请求结果不确定时把新链命令置为 `ReconcileRequired`，按同一 `clientOrderId` 查询订单/成交；确认前不重发。该状态只暂停对应账户的后续增险，不建立 authority、root、receipt、manifest 或事件回放体系。
- KOL 成交复制只按 Binance 认证账户流的真实成交增量触发，并以签名 REST 补查恢复，不按页面点击或 NEW 订单触发；双向持仓必须保留 `positionSide`。平仓在领域层是只减仓意图，发送前按跟随账户同代新鲜签名持仓向下裁剪；Portfolio Margin UM Hedge Mode 的原生订单不得发送 Binance 禁止的 `reduceOnly` 参数。
- `G:\Venue\artifacts` 仅保留冻结旧运行时的恢复工件；其未决 WAL、checkpoint、成交游标及 Unknown 事实不得删除。KOL MVP 的用户、关系、成交去重、命令和对账状态进入 PostgreSQL，不新增本地恢复 journal。

## 代码与配置

- 手写源文件最多 2000 个物理行；入口文件只声明、组合和重导出，新增行为超限前按职责拆分。
- `domain` 不依赖业务模块；交易所原始协议只存在于 `exchange`；策略不得依赖具体交易所、凭证、原生字段或物理订单客户端。
- 当前 KOL MVP 不建立通用策略 runtime；交易所原始差异仍只进入 adapter。未来 Grid 应由配置与签名订单/双向持仓事实计算目标订单并收敛，不恢复 Actor/checkpoint 作为新架构前置。
- 规范交易对使用大写 `BASE/QUOTE` 的 `domain::Symbol`；native symbol 不越过 adapter。
- KOL MVP 只准入 Binance Portfolio Margin UM 的精确 `LIVE`；其余 adapter 代码冻结且不扩大验收范围。不得新增测试网、demo、Shadow 或隐式布尔模式。离线 fixture、mock 和集成测试是验证手段，不是运行模式。
- 用户 API Key 由 Control 使用现有 AES-256-GCM 边界加密后存 PostgreSQL；只有验证流程和 Binance Executor 可短时解密。明文不得进入浏览器响应、KOL 页面、TOML、日志、错误或工件，也不得由 KOL 查看。API 必须具备读取与 UM 交易权限、关闭提现，并验证 Portfolio Margin 统一账户与双向持仓。
- 禁止复制规范类型、指标算法、归一化、订单事实或 journal；禁止用 `unsafe`、`unwrap`、`expect`、`panic!` 处理运行时外部输入。
- 注释只解释边界、不变量、失败语义和非显然原因，不复述代码。
- Git 只跟踪源码、配置、长期文档、脚本与小型协议 fixture；禁止跟踪 `bak/`、构建/发布目录、工具链、凭证、运行日志、数据库和 `artifacts/`。清理 `artifacts` 必须按上述活跃恢复集与历史归档边界执行，禁止删除未决 WAL、Unknown 关联事实或当前 checkpoint。

## 依赖治理

- 下列依赖是优先基线，不是禁止扩展的封闭清单。当前功能确有需要时可以新增依赖，但必须先查 workspace 与 `Cargo.lock`，
  说明现有依赖为何不能满足，并在同一修改中加入实际调用和专项测试；不得因个人偏好替换或为未来模块预装。
- 同一用途原则上复用既有实现；只有当前任务存在可验证的技术缺口时才允许增加第二套同类直接依赖，并必须限制调用边界和退出条件。
- 用途固定为：异步运行时 `tokio`；HTTP client `reqwest`；WebSocket `tokio-tungstenite`；JSON `serde` + `serde_json`；交易 Decimal `rust_decimal`；library 错误 `thiserror`；日志 `tracing` + `tracing-subscriber`；网络 Buffer `bytes`。
- 状态与通信固定为：读多写少快照 `arc-swap`；同步 Mutex/RwLock `parking_lot`；异步通道 `tokio::sync`；UI/同步线程边界 `crossbeam-channel`。
- 安全与能力固定为：凭证 `secrecy` + `zeroize`；adapter capability `bitflags`。
- PostgreSQL 固定使用 `sqlx`；只有当前功能明确需要本地 SQLite 时才可使用 `rusqlite`，不引入其他 ORM 或数据库封装。
- 现有 Stage 7 直接 `tungstenite` 只是冻结迁移例外，不得增加新调用点；转换前保留，不为满足白名单直接破坏式删除。

## 本机 Rust 构建与磁盘预算

- Windows 本机只允许三个固定缓存：`G:\Build\Venue\main`、`slot-1`、`slot-2`；主工作区使用 main，其余工作树按规范路径稳定映射到两个槽。禁止按会话、PID、时间戳、任务名新建或嵌套 target，不得改写 CARGO_TARGET_DIR/--target-dir 绕过入口。
- Cargo 构建/检查/测试统一使用 `scripts/Invoke-VenueBuild.ps1 -CargoArguments @('check','--locked','-p','venue-runtime')`；专项验证脚本已有同一 guard，直接运行，不要二次套锁。只读空间检查用 `-CheckOnly`。原始 cargo 编译、临时脚本、IDE 或子进程也不得用于绕过限制。
- Ubuntu 产物默认在本机用 `scripts/Build-VenueUbuntu.ps1` 交叉编译后上传，不在 `45.77.253.180` 日常编译。专用根 `G:\Build\Venue\ubuntu` 保存源码快照、Zig 工具缓存和版本化产物；Cargo 复用已有 slot-2 与同一 guard。目标、工具版本、预检和上传边界见 `docs/DEVELOPMENT.md`，不另开 Cargo target。
- 所有工作树合计最多两个受控构建；同槽锁覆盖构建、二进制核验/测试和产物复制。槽满等待最多60秒后报告，不新建目录、不抢锁、不终止其他会话进程；不允许嵌套 guard。
- 准入预算：`G:\Build\Venue` 普通文件合计150 GiB（含旧目录及临时文件），F宿主空闲至少100 GiB，G至少20 GiB；超限拒绝新构建并报告。检查不跟随重解析点。这是入口准入检查，不是持续运行监控或系统硬配额；单次构建仍可能跨过阈值。
- main 保留增量，并在 guard 内临时以空 RUSTC_WRAPPER 禁用外层编译 wrapper，避免全局 sccache 拒绝增量；finally 精确恢复原值，不改全局配置。隔离槽关闭增量并保留 wrapper；dev/test 使用精简调试信息。保持工具链/参数稳定；局部修改只验证受影响包，不反复全量测试、不常规 cargo clean。
- 本阶段不自动清理。清理须另行核准精确目录并取得对应槽锁，只能处理已登记且无占用的冷缓存；不得删除整个 Build/项目目录、源码、bak、Git、数据库、发布产物、备份或运行恢复工件。G内删除不保证F的VHDX立即缩小。
- 旧会话下一次构建前重读本段及 `docs/DEVELOPMENT.md`。脚本用finally释放锁并恢复环境。GitHub托管CI复用其RUNNER_TEMP下既有target，不套用本机F/G容量阈值。

## 文档同步

- 目录、模块、package、binary、CLI 或主要功能入口变化时，同一修改更新 `docs/CODEMAP.md`。
- KOL 产品范围、邀请、账户验证、复制语义、性能门或执行架构变化时，同一修改更新 `docs/KOL_COPY_MVP.md`。
- 只有冻结 Grid/旧 Node 的兼容边界变化时更新 `docs/GRID_RUNTIME_REFACTOR.md`。
- 长期文档只保留入口、当前约束、契约与验收；不积累已完成阶段记录、临时计划和事故流水。
