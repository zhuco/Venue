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
- 实盘 mutation 每个 `(exchange, trading_account_id)` 只允许一个账户级 writer；单机优先使用一个进程锁和一个串行 Execution Lane，多机部署实际出现前不得增加租约选举、分布式 fencing 或可执行文件 handoff 链。先验证，再按交易所逐家 Canary，禁止两个版本同时写同一账户。
- 策略只输出语义意图；所有 mutation 必须依次经过 risk、同一命令 WAL 和账户级 writer。Owner 只是 WAL 中的 `strategy_id/user_id` 归属字段，不单独建立 authority、journal、root 或 receipt 体系。
- 初期 Bybit、OKX、Hyperliquid 的 DOGE 账户累计名义仓位硬上限为 10U；已有未撤入场命令或交易所签名读到非零持仓时禁止继续增险。OKX 数量必须以实时 `ctVal × ctMult × contracts` 换算并遵守 `lotSz/minSz`，不得把基础币数量直接当张数。
- WAL 状态保持最小集合 `Prepared / Submitted / Accepted / Rejected / Unknown`；请求结果不确定时持久化 `Unknown`、冻结该账户新增风险并以签名订单/成交查询收敛，禁止自动重投。撤单和 reduce-only 降险仍可继续。
- 本地运行工件根固定为 `G:\Venue\artifacts`。追加文件在 5 MiB 轮转，任何单文件不得超过 10 MiB；原始私流默认不落盘，诊断时最多两个 5 MiB 滚动段；整个根默认预算 256 MiB。必须保留未决 WAL、当前 checkpoint、成交游标及 Unknown 关联事实；已对账覆盖的历史段可压缩或删除，原始 wire payload 不得作为永久恢复前提。

## 代码与配置

- 手写源文件最多 2000 个物理行；入口文件只声明、组合和重导出，新增行为超限前按职责拆分。
- `domain` 不依赖业务模块；交易所原始协议只存在于 `exchange`；策略不得依赖具体交易所、凭证、原生字段或物理订单客户端。
- 同一策略族跨交易所复用 reducer 和 runtime；交易所差异只进入 adapter、能力证据、execution profile 或 deployment binding。
- 规范交易对使用大写 `BASE/QUOTE` 的 `domain::Symbol`；native symbol 不越过 adapter。
- 配置必须恰好选择 Binance、Gate.io、Bitget、Bybit、OKX、Hyperliquid 之一，网关运行模式只允许精确 `LIVE`；不得新增测试网、demo、Shadow 或隐式布尔模式。离线 fixture、mock 和集成测试是验证手段，不是运行模式。凭证只来自进程环境或根 `.env`，不得写进 TOML、日志、错误或工件。
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

本地 Cargo 构建产物统一写入 `G:\Build\Venue`，由根 `.cargo/config.toml` 固定；验证脚本可在其下创建按 PID 隔离的子目录，
不得重新把大体积 `target/` 写入仓库。

## 文档同步

- 目录、模块、package、binary、CLI 或主要功能入口变化时，同一修改更新 `CODEMAP.md`。
- 本轮网格架构、状态机、参数、验收、迁移顺序或接管流程变化时，同一修改更新 `GRID_RUNTIME_REFACTOR.md`。
- 长期文档只保留入口、当前约束、契约与验收；不积累已完成阶段记录、临时计划和事故流水。
