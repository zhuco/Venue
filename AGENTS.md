# VENUE Workspace Rules

本工作区只有根目录 Rust package 是活动代码；`bak/` 是冻结迁移来源。

## 最短读取路径

1. 先读 `CODEMAP.md`，按任务只打开对应入口和直接依赖。
2. 对冲网格架构、库存恢复重心、共享运行时、部署或实盘接管任务必须完整阅读 `GRID_RUNTIME_REFACTOR.md`。
3. 只有用户明确要求从旧实现提取行为时，才读取 `bak` 中被点名的入口；定位失败前不得递归扫描 `bak`。

## Legacy 冻结

- `bak` 不参与根 package 的构建、测试或运行，未经用户明确授权不得修改、格式化、清理、提交或删除。
- `bak/VenueCore` 的内部 `.git`、未提交和未跟踪内容必须原样保留。
- 新代码不得通过 path dependency、include、symlink 或运行时路径依赖 `bak`。
- 提取行为后在根 package 建立独立测试，不修改旧实现迎合新测试。

## 实施与实盘

- 只实现当前获准任务，不创建未被当前需求使用的未来模块、公共 SDK、插件系统或多租户控制面。
- 默认按影响面验证：文档/注释只做静态检查，单模块修改只检查该 package 及直接契约，交易安全修改覆盖受影响的 risk/WAL/Unknown/恢复路径。跨模块公共契约、依赖或架构变更及正式发布前集中执行 `cargo fmt --all --check`、`cargo check --workspace --all-targets`、`cargo test --workspace`、`scripts/verify_repository_hygiene.ps1` 建立基线；基线后的局部增量不重复全工作区测试。记录验证对应的源码范围，相关验证失败时不得宣称完成或删除被替代实现。
- 实盘 mutation 一次只允许一个精确 writer；先验证，再按交易所逐家 Canary/接管，禁止两个版本同时写同一 binding。
- 策略只输出语义意图；所有 mutation 必须经过 execution、risk、owner、WAL/journal 和 reconciliation。
- 数据、私有事实、订单身份、规则或恢复状态无法证明时失败关闭，只允许降险和对账。
- 不得把 `artifacts` 中的 checkpoint、writer、WAL、JSONL、admission、capability 或 handoff 收据当普通日志删除。

## 代码与配置

- 手写源文件最多 2000 个物理行；入口文件只声明、组合和重导出，新增行为超限前按职责拆分。
- `domain` 不依赖业务模块；交易所原始协议只存在于 `exchange`；策略不得依赖具体交易所、凭证、原生字段或物理订单客户端。
- 同一策略族跨交易所复用 reducer 和 runtime；交易所差异只进入 adapter、能力证据、execution profile 或 deployment binding。
- 规范交易对使用大写 `BASE/QUOTE` 的 `domain::Symbol`；native symbol 不越过 adapter。
- 配置必须恰好选择 Binance、Gate.io、Bitget、Bybit、OKX、Hyperliquid 之一，网关模式只允许精确 `TEST` 或 `LIVE`。凭证只来自进程环境或根 `.env`，不得写进 TOML、日志、错误或工件。
- 禁止复制规范类型、指标算法、归一化、订单事实或 journal；禁止用 `unsafe`、`unwrap`、`expect`、`panic!` 处理运行时外部输入。
- 注释只解释边界、不变量、失败语义和非显然原因，不复述代码。
- Git 只跟踪源码、配置、长期文档、脚本与小型协议 fixture；禁止跟踪 `bak/`、构建/发布目录、工具链、凭证、运行日志、数据库和 `artifacts/`。受保护工件只保留在本地或部署存储，不以清理仓库为由删除。

## 依赖白名单

- 同一用途已有白名单实现时，未经用户明确批准不得增加第二套同类直接依赖；先查 workspace 和 `Cargo.lock`，不得因个人偏好替换。
- 依赖只在当前功能实际使用时加入，不为未来模块预装。
- 用途固定为：异步运行时 `tokio`；HTTP client `reqwest`；WebSocket `tokio-tungstenite`；JSON `serde` + `serde_json`；交易 Decimal `rust_decimal`；library 错误 `thiserror`；日志 `tracing` + `tracing-subscriber`；网络 Buffer `bytes`。
- 状态与通信固定为：读多写少快照 `arc-swap`；同步 Mutex/RwLock `parking_lot`；异步通道 `tokio::sync`；UI/同步线程边界 `crossbeam-channel`。
- 安全与能力固定为：凭证 `secrecy` + `zeroize`；adapter capability `bitflags`。
- PostgreSQL 固定使用 `sqlx`；只有当前功能明确需要本地 SQLite 时才可使用 `rusqlite`，不引入其他 ORM 或数据库封装。
- 现有 Stage 7 直接 `tungstenite` 只是冻结迁移例外，不得增加新调用点；转换前保留，不为满足白名单直接破坏式删除。

## 本机 Rust 构建与磁盘预算

- Windows 本机只允许三个固定缓存：`G:\Build\Venue\main`、`slot-1`、`slot-2`；主工作区使用 main，其余工作树按规范路径稳定映射到两个槽。禁止按会话、PID、时间戳、任务名新建或嵌套 target，不得改写 CARGO_TARGET_DIR/--target-dir 绕过入口。
- Cargo 构建/检查/测试统一使用 `scripts/Invoke-VenueBuild.ps1 -CargoArguments @('check','--locked','-p','venue-runtime')`；专项验证脚本已有同一 guard，直接运行，不要二次套锁。只读空间检查用 `-CheckOnly`。原始 cargo 编译、临时脚本、IDE 或子进程也不得用于绕过限制。
- 所有工作树合计最多两个受控构建；同槽锁覆盖构建、二进制核验/测试和产物复制。槽满等待最多60秒后报告，不新建目录、不抢锁、不终止其他会话进程；不允许嵌套 guard。
- 准入预算：`G:\Build\Venue` 普通文件合计150 GiB（含旧目录及临时文件），F宿主空闲至少100 GiB，G至少20 GiB；超限拒绝新构建并报告。检查不跟随重解析点。这是入口准入检查，不是持续运行监控或系统硬配额；单次构建仍可能跨过阈值。
- main 保留增量，隔离槽关闭增量；dev/test 使用精简调试信息。保持工具链/参数稳定；局部修改只验证受影响包，不反复全量测试、不常规 cargo clean。
- 本阶段不自动清理。清理须另行核准精确目录并取得对应槽锁，只能处理已登记且无占用的冷缓存；不得删除整个 Build/项目目录、源码、bak、Git、数据库、发布产物、备份或运行恢复工件。G内删除不保证F的VHDX立即缩小。
- 旧会话下一次构建前重读本段及 `scripts/BUILD_POLICY.md`。脚本用finally释放锁并恢复环境。GitHub托管CI复用其RUNNER_TEMP下既有target，不套用本机F/G容量阈值。

## 文档同步

- 目录、模块、package、binary、CLI 或主要功能入口变化时，同一修改更新 `CODEMAP.md`。
- 本轮网格架构、状态机、参数、验收、迁移顺序或接管流程变化时，同一修改更新 `GRID_RUNTIME_REFACTOR.md`。
- 长期文档只保留入口、当前约束、契约与验收；不积累已完成阶段记录、临时计划和事故流水。
