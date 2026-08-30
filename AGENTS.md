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
- 完成修改必须执行 `cargo fmt --all --check`、`cargo check --workspace --all-targets`、`cargo test --workspace`、`scripts/verify_repository_hygiene.ps1` 和任务专项验证；失败时不得宣称完成或删除被替代实现。
- 实盘 mutation 一次只允许一个精确 writer；先验证，再按交易所逐家 Canary/接管，禁止两个版本同时写同一 binding。
- 策略只输出语义意图；所有 mutation 必须经过 execution、risk、owner、WAL/journal 和 reconciliation。
- 数据、私有事实、订单身份、规则或恢复状态无法证明时失败关闭，只允许降险和对账。
- 不得把 `artifacts` 中的 checkpoint、writer、WAL、JSONL、admission、capability 或 handoff 收据当普通日志删除。

## 代码与配置

- 手写源文件最多 2000 个物理行；入口文件只声明、组合和重导出，新增行为超限前按职责拆分。
- `domain` 不依赖业务模块；交易所原始协议只存在于 `exchange`；策略不得依赖具体交易所、凭证、原生字段或物理订单客户端。
- 同一策略族跨交易所复用 reducer 和 runtime；交易所差异只进入 adapter、能力证据、execution profile 或 deployment binding。
- 规范交易对使用大写 `BASE/QUOTE` 的 `domain::Symbol`；native symbol 不越过 adapter。
- 配置必须恰好选择 Binance、Gate.io、Bitget、Bybit、OKX、Hyperliquid 之一，网关运行模式只允许精确 `LIVE`；不得新增测试网、demo、Shadow 或隐式布尔模式。离线 fixture、mock 和集成测试是验证手段，不是运行模式。凭证只来自进程环境或根 `.env`，不得写进 TOML、日志、错误或工件。
- 禁止复制规范类型、指标算法、归一化、订单事实或 journal；禁止用 `unsafe`、`unwrap`、`expect`、`panic!` 处理运行时外部输入。
- 注释只解释边界、不变量、失败语义和非显然原因，不复述代码。
- Git 只跟踪源码、配置、长期文档、脚本与小型协议 fixture；禁止跟踪 `bak/`、构建/发布目录、工具链、凭证、运行日志、数据库和 `artifacts/`。受保护工件只保留在本地或部署存储，不以清理仓库为由删除。

## 依赖治理

- 下列依赖是优先基线，不是禁止扩展的封闭清单。当前功能确有需要时可以新增依赖，但必须先查 workspace 与 `Cargo.lock`，
  说明现有依赖为何不能满足，并在同一修改中加入实际调用和专项测试；不得因个人偏好替换或为未来模块预装。
- 同一用途原则上复用既有实现；只有当前任务存在可验证的技术缺口时才允许增加第二套同类直接依赖，并必须限制调用边界和退出条件。
- 用途固定为：异步运行时 `tokio`；HTTP client `reqwest`；WebSocket `tokio-tungstenite`；JSON `serde` + `serde_json`；交易 Decimal `rust_decimal`；library 错误 `thiserror`；日志 `tracing` + `tracing-subscriber`；网络 Buffer `bytes`。
- 状态与通信固定为：读多写少快照 `arc-swap`；同步 Mutex/RwLock `parking_lot`；异步通道 `tokio::sync`；UI/同步线程边界 `crossbeam-channel`。
- 安全与能力固定为：凭证 `secrecy` + `zeroize`；adapter capability `bitflags`。
- PostgreSQL 固定使用 `sqlx`；只有当前功能明确需要本地 SQLite 时才可使用 `rusqlite`，不引入其他 ORM 或数据库封装。
- 现有 Stage 7 直接 `tungstenite` 只是冻结迁移例外，不得增加新调用点；转换前保留，不为满足白名单直接破坏式删除。

本地 Cargo 构建产物统一写入 `G:\Build\Venue`，由根 `.cargo/config.toml` 固定；验证脚本可在其下创建按 PID 隔离的子目录，
不得重新把大体积 `target/` 写入仓库。

## 文档同步

- 目录、模块、package、binary、CLI 或主要功能入口变化时，同一修改更新 `CODEMAP.md`。
- 本轮网格架构、状态机、参数、验收、迁移顺序或接管流程变化时，同一修改更新 `GRID_RUNTIME_REFACTOR.md`。
- 长期文档只保留入口、当前约束、契约与验收；不积累已完成阶段记录、临时计划和事故流水。
