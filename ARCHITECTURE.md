# VENUE 目标架构与技术栈

更新：2026-08-29

## 1. 文档职责

本文定义 Venue 合并跟单、指标、桌面 UI 与六交易所网关后的目标架构、依赖边界、技术栈和迁移顺序。
[`GRID_RUNTIME_REFACTOR.md`](GRID_RUNTIME_REFACTOR.md) 继续约束当前三所 Stage 7 网格热路径、恢复、接管和实盘准入；
在目标架构尚未逐项验收前，不得用本文替代现有安全门。

迁移来源只提供行为和测试证据：

- `G:\kol`：跟单语义、账本、事务 outbox/inbox、租约栅栏和六所协议 fixture；
- `bak/VenueCore`：VenuePulse 指标、VenueFlow 桌面框架及交易所网关缺口；
- Condor 会话：Agent、指标计算和确定性执行分层思想。

迁移必须在本工作区建立独立实现和测试，不得通过 path dependency、include、symlink 或运行时路径依赖上述来源。

## 2. 固定架构决策

1. 采用一个 Rust workspace 的模块化单体，不复制 KOL 的二十四包多进程拓扑，也不引入 Condor/Hummingbot 作为执行核心。
2. 每个 `(venue, trading_account_id)` 仍由一个账户节点进程拥有；`trading_account_id` 是凭证映射到真实交易账户的稳定规范 UUID，不是 `portfolio_margin_um` 等产品类型，也不含 symbol；同一真实账户任意时刻只有一个精确 writer。
3. 网格、Scalping 和跟单都只输出语义意图；所有交易 mutation 统一经过账户 Runtime、Execution、Risk、Owner、WAL 和 Reconciliation。
4. 跟单是独立 Copy Engine，不直接持有交易所客户端、凭证、writer lease 或 native mutation 权限。
5. 桌面 UI、Control API 和 Agent 都属于慢速控制面，不进入成交热路径，不直接读取或改写 WAL。
6. 规范交易所集合固定为 Binance、Bitget、Bybit、Gate.io、Hyperliquid、OKX；策略是否可在某所运行仍由产品、账户和订单族能力证据决定。
7. 网关运行模式只有 `TEST` 和 `LIVE`；不设计只读、Shadow 或隐式 `live=false` 模式。
8. 旧策略、Condor 策略、VenuePulse 中的策略特征、KOL 重复网关和 VenueFlow 模拟交易全部不迁移。

所有 `TEST`/`LIVE` 配置必须显式提供规范 `trading_account_id`。`account_binding` 仅描述 API 产品与账户模式能力；同一真实账户的不同交易对和策略必须复用同一 UUID。缺失、格式错误或与持久工件不一致时拒绝启动，禁止由 API Key、产品类型或 symbol 临时推导账户身份。

## 3. 目标进程拓扑

```text
VenueFlow Desktop / optional Agent
                 |
                 v
          venue-control
   copy planning / query / control
 PostgreSQL command outbox + ledger
                 |
 durable semantic command + wakeup
                 v
 venue-node-<venue>  (one process per account binding)
 ├─ Account Runtime / Market Hub / Private Router
 ├─ Grid Actors
 ├─ Scalping Actors
 ├─ Copy Follower Actors
 ├─ Execution Lane / Risk / Owner / WAL
 ├─ Reconciliation
 └─ exactly one linked venue adapter
                 |
                 v
              Exchange
```

`venue-control` 可以规划和持久化跟单目标，但不能直接提交订单。Control 到节点的权威传输使用 PostgreSQL durable
command outbox/inbox；LISTEN/NOTIFY、SSE、WebSocket 或 HTTP 只能唤醒，不能代替耐久记录。每条命令具有确定性 ID，
绑定 account、instance、symbol、config epoch、capability epoch 和 payload digest。节点必须先把命令持久化到本地 Actor
inbox，再确认数据库 delivery；重复投递只返回同一 applied/拒绝回执。节点回执写入独立 receipt outbox，Control 只能在
消费持久回执后推进 copy ledger。账户节点仍须重新执行风险、Owner、WAL、writer 和私有事实校验。

## 4. 目标 workspace

```text
apps/
├─ venue-node/
│  └─ src/bin/venue-node-{binance,bitget,bybit,gate,hyperliquid,okx}.rs
├─ venue-control/
└─ venueflow/

crates/
├─ venue-domain/
├─ venue-indicators/
├─ venue-gateway-api/
├─ venue-gateway-binance/
├─ venue-gateway-bitget/
├─ venue-gateway-bybit/
├─ venue-gateway-gate/
├─ venue-gateway-hyperliquid/
├─ venue-gateway-okx/
├─ venue-strategies/
│  └─ src/{grid,scalping}/
├─ venue-copy/
├─ venue-execution/
├─ venue-storage/
├─ venue-runtime/
│  └─ src/{account,grid,scalping,copy,shared,legacy}/
└─ venue-control-protocol/
```

六个节点 binary 必须各自只链接一个交易所 adapter。构建验收继续扫描生产 endpoint，拒绝把其他 adapter 链接进
固定节点。`legacy` 只是有退出条件的迁移隔离区，不允许新增功能。

## 5. 依赖方向

以下 `A -> B` 表示 A 依赖 B：

```text
venue-indicators -> venue-domain
venue-gateway-api -> venue-domain
venue-gateway-* -> venue-gateway-api
venue-strategies -> venue-domain
venue-copy -> venue-domain
venue-execution -> venue-domain + venue-storage + venue-gateway-api
venue-runtime -> venue-execution + venue-strategies + venue-copy
venue-node-<venue> -> venue-runtime + exactly one venue-gateway-*
venue-control -> venue-copy + venue-storage + venue-control-protocol
venueflow / optional Agent -> venue-control-protocol
```

硬边界：

- `venue-domain` 不依赖业务、数据库、网络、UI 或交易所；
- crate 拆分期间，带 crate-private 构造器的 runtime turn/capability authority identity 暂留根 `domain` facade；不得为了物理迁移把其发行构造器公开，待 `venue-runtime` 提取时再整体移动；
- adapter 不依赖 runtime、strategy、copy 或 UI；原生协议不得越过 adapter；
- strategy 和 copy 不依赖具体交易所、凭证、native symbol 或物理客户端；
- `venue-control-protocol` 只含版本化 DTO、错误码和序列化契约，不含 Axum handler、数据库、runtime 或 application service；
- UI 不依赖 execution、adapter 或数据库，只依赖版本化 Control protocol，并通过 HTTP/SSE/WebSocket client 访问服务；
- Control API DTO 不作为交易权威事实，节点必须重新验证；
- 不复制 Symbol、Money、Order、Position、Fill、InstrumentRule、Capability 或 journal 类型。

## 6. Runtime 内部结构

`venue-runtime` 按责任分组，禁止重新回到根目录平铺：

```text
account/   身份、Registry、Market Hub、Private Router、Reconciler、Actor host
grid/      三所现有 Stage 7 的共享网格运行时及以后验收通过的交易所组合；根 facade 保持既有 Stage 7 API
scalping/  test、live、evidence、scheduling；迁移期间由根 runtime facade 保持既有模块名和公开 API
copy/      follower Actor、目标应用、漂移修复、跟单回执
shared/    supervisor、恢复、私有事实、bounded mailbox 与生命周期公共组件
legacy/    旧 Binance 网格壳与兼容桥；只减不增，Stage 7 共享实现归入 grid
```

同一账户、同一规范交易对仍只允许一个 Owner。Copy Follower 注册前必须拒绝已有 Grid、Scalping 或 Copy Owner。
任何策略切换必须依次完成 Stop、请求后新代全订单族签名零自有订单、残仓归零或显式 custody handoff、旧 Owner 释放、
新 Owner 安装；不同 instance ID 不构成交接。若要同时操作同一 symbol，必须使用不同账户 binding。

Tokio task、HTTP handler、数据库 worker 或 Agent 决策都不构成 mutation authority。只有账户 Actor turn 经持久 applied
receipt 后生成的语义意图，才可进入唯一 Execution Lane。

## 7. Copy Engine

从 KOL 提取并收敛为下列固定数据流：

```text
Leader Intent
-> transactional outbox
-> immutable leader snapshot
-> target exposure planner
-> follower job with deterministic identity
-> account-node admission
-> Execution Lane
-> private facts / signed REST reconciliation
-> copy ledger and drift repair
```

当前 `venue-copy` 已落地纯资本/目标敞口 reducer 与确定性身份内核：只使用同一 generation、同一估值币种和有效期内的冻结事实，
按 configured、allocated、扣安全储备后的 available margin 三者最小值确定 follower 有效资本，复用 leader 已包含杠杆的
exposure ratio，且跨零反向必须先平仓并等待新私有事实。它不依赖 storage、network、runtime 或 writer；完整数量/价格 sizing
要等规范 `Instrument` 补齐跨所稳定身份和合约换算语义后再实现，禁止在 Copy 内复制一套 metadata。job、planning snapshot、
child order 和 idempotency key 已按 KOL 的 length-delimited SHA-256 与 domain separation 固化，重放不读取时间或随机源。

必须保留：

- leader、follower、account binding、instrument 和 policy 的稳定身份；
- 目标敞口与倍率算法、数量/名义值对齐和 follower 独立硬限额；
- deterministic child identity 与幂等提交；
- UNKNOWN 先对账、不得直接重发；
- venue/account lease fencing，但最终 mutation writer 仍由账户节点持有；
- 托管成交、外部成交和人工仓位变化的明确归因；
- outbox/inbox、observer、账本和漂移修复的事务边界。

不得迁移：

- KOL 的重复签名、HTTP、WebSocket、市场和 instrument-sync 实现；
- 空 execution crate、未被生产引用的 catalog/ledger 壳；
- 为每一步拆独立常驻进程的部署拓扑；
- 任何绕过 Venue Execution Lane 的 leader 或 child executor。

KOL 的 PostgreSQL migrations 只按当前模型逐表提取；不得整体复制历史表和未使用角色。

Copy planner/job-consumer lease 只允许竞争数据库 job 的规划或投递权，不授予、刷新、撤销或 fence 账户 mutation writer。
唯一 mutation authority 始终是账户节点本地 writer lease、命令 WAL 和一次性 dispatch permit。即使数据库 lease 有效，
节点 writer 或能力证据无法证明时也必须拒绝 mutation。

## 8. 六交易所网关

当前已把六所规范身份、仅含 `TEST | LIVE` 的模式、账户/交易对 binding 与版本化 capability mutation 门禁落入 `venue-gateway-api`；根 package 复用同一 `VenueId`，不再复制交易所枚举。门禁要求完整读取、私流、交易和具体 mutation 能力且拒绝提现权限。Binance、Bitget、Gate.io 沿用现有账户订单族能力证据；Bybit、OKX、Hyperliquid 在 adapter 最小闭环完成前能力为空并失败关闭。

| Venue | 目标 adapter | 当前权威来源 | 初始准入 |
|---|---|---|---|
| Binance | `venue-gateway-binance` | 已迁入 Portfolio Margin 产品身份、原生 symbol、私有 payload、账户/仓位/订单/成交/风险纯规范化及有界成交 cursor 分页；根路径兼容 re-export，现有签名 readback、transport 与 Stage 7 capability/WAL/writer 保持生产权威，GatewayBinding 不授予旧 writer 能力 | `TEST | LIVE`；保留现有已验收路径，新 Copy 路径重新 Canary |
| Bitget | `venue-gateway-bitget` | 已迁入 UTA 产品身份、当前官方 TEST Demo/LIVE 端点、secrecy 凭证、REST/WS 签名、公共市场、账户模式/余额/持仓及账户/腿风险纯协议；根路径兼容 re-export，HTTP/WS transport、私有 readback、订单/成交、Stage 7 capability/WAL/writer 仍由根 package 保持生产权威 | `TEST | LIVE`；保留现有已验收路径，新 Copy 路径重新 Canary |
| Gate.io | `venue-gateway-gate` | 已迁入 USDT perpetual scope、当前官方 TEST/LIVE 端点、secrecy 凭证、REST/WS 签名、公共市场、账户/持仓、合约数量及账户/腿风险纯协议；根路径兼容 re-export，HTTP/WS transport、私有 readback、订单/成交、Stage 7 capability/WAL/writer 仍由根 package 保持生产权威 | `TEST | LIVE`；保留现有已验收路径，新 Copy 路径重新 Canary |
| Bybit | `venue-gateway-bybit` | 已迁入完整 gateway binding、由 binding 派生的 V5 TEST/LIVE endpoint、secrecy 凭证/签名 header、HMAC 固定向量与签名成交 fixture；capability 为空，transport/private stream/writer 待接 | `TEST | LIVE`；最小 LIVE 安全闭环后即可小额实盘调试 |
| OKX | `venue-gateway-okx` | 已迁入完整 gateway binding、由 binding 派生的 V5 TEST 模拟盘/LIVE 端点与请求头、三项 secret、HMAC 固定向量与合约成交规范化 fixture；capability 为空，transport/private stream/writer 待接 | `TEST | LIVE`；最小 LIVE 安全闭环后即可小额实盘调试 |
| Hyperliquid | `venue-gateway-hyperliquid` | 已迁入完整 gateway binding、由 binding 派生的 TEST/LIVE 端点、命名 Agent secret 边界、按 Agent 绑定的持久 nonce 契约与私有成交 fixture；EIP-712 所需 `k256`/MessagePack/Keccak 依赖尚未获准，因此 signing/transport/writer 未接且 capability 为空 | `TEST | LIVE`；最小 LIVE 安全闭环后即可小额实盘调试 |

KOL 网关只是协议 fixture 和差异对照来源，不继承其运行开关或实盘准入状态。前三所的生产权威继续来自 Venue 已验收实现。

`TEST` 使用明确配置的测试网、demo 或测试 endpoint，允许在该环境发送 mutation；`LIVE` 只使用生产 endpoint 和实盘账户。
两种模式的 endpoint、凭证、account binding、WAL、checkpoint、evidence 和 receipt root 必须完全隔离，不得自动回退或混用。
离线 fixture 和 parser 测试属于测试工具，不是第三种网关运行模式。
现有 Stage 7 中名为 Shadow/verify 的命令是冻结的策略证据验收流程，不是 gateway mode；新 adapter 不得沿用该命名建立第三种运行状态。

`TEST` 不是进入 `LIVE` 的强制前置阶段。单所 adapter 一旦完成精确账户/交易对绑定、唯一 writer、Owner、WAL、独立小额限额、
签名私有 readback、确定性订单身份、UNKNOWN 先对账、紧急 Stop/Flatten 和显式人工确认，就可对一个 binding 开始小额 `LIVE` Canary 调试。
完整 Copy、Grid 或 Scalping 产品准入仍需各自的行为、恢复和接管验收，不得因网关能下单而自动获得策略准入。

每个 adapter 必须提供：

- native symbol 与规范 `BASE/QUOTE` 的双向映射；
- instrument rules、账户模式和带时效能力快照；
- 公共行情及交易所事件时间；
- 私有订单、成交、仓位、余额的完整快照和增量；
- canonical order family 的完整签名页或明确不支持证据；
- client/venue order identity、分页闭合、时间同步、限频和断线代际；
- place/cancel/reduce-only 的请求编码与可审计标量回报；
- 原始 payload evidence 和离线重放 fixture。

任一产品类型、订单族、账户模式或原生字段无法规范化时，必须保留原生证据并拒绝相应 mutation，不能用最低公共字段
伪造“六所统一支持”。网格支持范围与网关数量分离；新增 adapter 不自动获得 Grid 或 Scalping 准入。

## 9. 指标

VenuePulse 迁入单一 `venue-indicators` crate，并按五个模块组织：

```text
core / series / ta / orderflow / registry
```

迁移增量 `update/reset/warmup` 契约及算法测试；通过适配器消费 `venue-domain` 的 Bar、Trade、OrderBook，禁止复制权威
行情类型。指标内部允许使用 `f64`，但任何价格、数量或风险 mutation 必须回到 Decimal 领域类型并重新校验。

`mas_frame`、`market_maker_*` 和其他策略特征不迁移。指标 crate 不读取凭证、数据库、WAL 或交易所客户端。

## 10. VenueFlow 桌面端

桌面技术栈固定为 Rust 2024、`eframe/egui`、`egui_tiles` 和 WGPU；异步网络使用 Tokio 与 WebSocket/SSE client。

迁移范围：

- 窗口、dock 布局、主题、图表、工作区持久化和指标面板；
- KOL 的 leader/follower、binding、目标敞口、漂移、执行状态和账本查询交互；
- Grid、Scalping、Copy 实例的查询、Pause/Stop/Flatten 与人工确认流程。

删除 Alpha DTO、旧策略 analytics、模拟交易和 UI 内 mutation gate。VenueFlow 不持有交易所凭证，不直接连接交易所，
不读取 PostgreSQL 或 artifacts，只调用版本化 Control protocol。高风险操作必须显示精确 account/symbol/action 并经服务端再次确认。

## 11. Agent 边界

Condor 的 Agent/Routine/Memory 思想仅作为可选控制层：

- Agent 可读取指标、PnL、健康和账本投影；
- Agent 可提出策略选择、参数或 Pause/Resume 建议；
- 建议必须转换为版本化 Control protocol 命令并接受与人工命令相同的权限和审计；
- Agent 不持有凭证、writer、WAL、native client ID 或订单客户端；
- Agent 不参与逐成交补撤、风险减仓或 UNKNOWN 收敛。

Python、FastAPI、MQTT、TypeScript DEX Gateway 和 Hummingbot 均不是核心运行依赖；将来若引入 Agent，只能作为可替换
sidecar 通过 Control protocol 接入。

## 12. 数据与存储

PostgreSQL + SQLx 用于：

- leader intent、snapshot、copy policy、follower target；
- transactional outbox/inbox、copy job、observer cursor；
- node command outbox、delivery inbox、receipt outbox 和幂等 applied 状态；
- copy ledger、drift、UI/query projection 和角色审计；
- schema migration。

文件型耐久存储继续用于交易权威事实：

- writer lease、command WAL、Owner route；
- private/public evidence；
- strategy/account checkpoint；
- admission、capability、Canary 和 handoff receipt。

PostgreSQL 不得成为已发物理订单的第二权威 writer。Copy ledger 只消费账户节点的持久执行/私有事实收据，并保存其摘要
和稳定身份。凭证只来自进程环境或根 `.env`，不得写入 PostgreSQL、TOML、日志、UI 或工件。

## 13. 技术栈

| 层 | 选择 |
|---|---|
| 语言 | Rust 2024；工具链和 workspace `rust-version` 精确锁定 Rust 1.98.0 |
| 交易 Actor | 顺序状态机、专用 OS thread 或受控 executor、有界 mailbox |
| 异步 I/O | Tokio；不授予执行顺序或 mutation authority |
| HTTP/控制 | Axum、Serde JSON、SSE/WebSocket |
| 交易 HTTP/WS | reqwest、tokio-tungstenite 或经等价验收的现有阻塞 transport |
| 数值 | rust_decimal 处理交易金额；指标内部可用 f64 |
| 数据库 | PostgreSQL、SQLx、显式 migrations |
| 交易耐久化 | append-only JSONL、hash chain、fsync checkpoint/receipt |
| 桌面 | eframe、egui、egui_tiles、WGPU |
| 日志与追踪 | tracing；凭证和私有 payload 默认脱敏 |
| 配置 | TOML + process env/root `.env` secrets |

依赖白名单按用途执行：

| 用途 | 唯一直接依赖 | Venue 定位 |
|---|---|---|
| Async Runtime | `tokio` | 唯一异步运行时 |
| HTTP client | `reqwest` | 交易所与外部 HTTP |
| WebSocket | `tokio-tungstenite` | 无官方 Rust SDK 的交易所 |
| JSON | `serde` + `serde_json` | 唯一 JSON 体系 |
| 金融 Decimal | `rust_decimal` | API 边界、账户、金额 |
| library 错误 | `thiserror` | 结构化 library error |
| 日志/Tracing | `tracing` + `tracing-subscriber` | 全项目统一 |
| Buffer | `bytes` | 网络数据 |
| Read-mostly 状态 | `arc-swap` | 行情/账户快照 |
| Mutex/RwLock | `parking_lot` | 同步线程状态 |
| Async channel | `tokio::sync` | async 内部通信 |
| Sync channel | `crossbeam-channel` | UI/同步线程边界 |
| Secret | `secrecy` + `zeroize` | API Key/Secret |
| Capability flags | `bitflags` | adapter capabilities |
| PostgreSQL | `sqlx` | Control/Copy 服务端数据 |
| 本地 SQLite | `rusqlite` | 仅当当前功能确实需要 SQLite |

白名单解决同一用途时，未经用户明确批准不得引入第二套同类直接依赖；不得为未来模块提前增加未使用依赖。
现有 Stage 7 直接 `tungstenite` 是行为等价迁移前的唯一冻结例外，不允许增加新调用点；它与 `tokio-tungstenite` 的底层传递性关系不构成新的第三套 WebSocket 实现。

不得在同一阶段同时重写网络 transport、交易状态机和持久化布局。当前 Stage 7 可继续使用既有阻塞 I/O，先完成 crate
和目录边界迁移；异步 transport 只能在行为等价测试和逐所 Canary 后替换。

## 14. 迁移顺序

1. **工具链与结构基线**：全 workspace、CI、rustfmt 和 clippy 锁定 Rust 1.98.0；建立 workspace、依赖检查和现有 Binance/Gate/Bitget 固定节点 binary；只移动代码和改引用，不改变行为。
2. **Runtime 收拢**：把现有 account/grid/scalping/shared/legacy 分目录，缩小根 facade；保持全部测试通过。
3. **Gateway API**：提取统一契约，先接回 Venue 的 Binance/Gate/Bitget；逐项导入 KOL fixture 做差异测试。
4. **新增三所最小闭环**：Bybit、OKX、Hyperliquid 各自增加固定 binary 和显式 `TEST | LIVE` 配置；完成该所最小 LIVE 安全闭环后即可对单账户、单交易对小额调试，无需等待 Copy 或策略全量迁移；同一修改同步配置枚举、`AGENTS.md` 和 `CODEMAP.md`。
5. **Copy TEST**：迁移 KOL 目标敞口、outbox/inbox、observer 和账本，使用离线 fixture 或网关 `TEST` 验证；不引入 Shadow 网关模式。
6. **指标与 UI**：迁入 VenuePulse 五层算法和查询/控制型 VenueFlow，再开放有审计的高风险控制命令。
7. **逐所扩大 LIVE**：在第 4 步小额实盘调试基础上，按一个精确账户、一个精确 writer、一个交易所依次完成 Copy 产品 Canary 和接管；新增三所不得并行扩大。
8. **Legacy 退休**：只有调用点清零、恢复工件兼容、行为等价和接管验收全部成立后，才能删除兼容层。

## 15. 验收门槛

每次结构或行为迁移至少满足：

- `cargo fmt --all --check`、`cargo check --workspace --all-targets`、`cargo test --workspace`；
- `rustc --version` 为 1.98.0，所有 workspace package 的 `rust-version` 必须显式设为或继承 1.98.0；
- workspace 依赖图无反向依赖或循环；
- 新增依赖已证明当前必需，且未引入白名单同用途的第二套直接依赖；
- 六个节点 binary 只接受 `TEST | LIVE`，交易所及模式之间 endpoint/凭证/binding/artifact isolation；
- 同账户同 symbol 的第二 Owner 被拒绝；
- Grid、Scalping、Copy 都不能绕过唯一 Execution Lane；
- Copy UNKNOWN 重启后先查事实且不重复下单；
- PostgreSQL outbox、账户 WAL 和 copy ledger 的崩溃边界可重放且不产生双 writer；
- Control 命令在 outbox 写入、节点 inbox 持久化、Actor applied 和 receipt outbox 任一边界崩溃后均不漏单、不双发；
- UI/Agent 无凭证、无物理订单客户端、无 artifacts 写权限；
- `LIVE` 调试只能在最小安全闭环成立后对一个精确 binding 小额执行，不因调试准入扩大策略产品权限。

## 16. 明确不做

- 不迁移 `bak` 或 KOL 的策略实现；
- 不复制 Hummingbot、Condor runtime、KOL 多服务部署或 Next.js PWA；
- 不让 PostgreSQL、Control API、UI 或 Agent 直接写交易所；
- 不为六所强造相同能力；
- 不同时运行两个版本写同一 binding；
- 不在迁移期间清理、覆盖或重解释现有交易 artifacts。
