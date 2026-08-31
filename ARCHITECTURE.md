# VENUE 目标架构与技术栈

更新：2026-08-31

## 1. 文档职责

本文定义 Venue 合并跟单、指标、桌面/Web UI 与六交易所网关后的目标架构、依赖边界和技术栈。
[`GRID_RUNTIME_REFACTOR.md`](GRID_RUNTIME_REFACTOR.md) 约束共享账户运行时、网格热路径、旧 Stage 7 恢复接管和实盘准入；
在目标架构尚未逐项验收前，不得用本文替代现有安全门。

当前统一执行链、旧三所接管和 Web 迁移的实施阶段与完成定义统一查
[`UNIFIED_GATEWAY_WEB_MIGRATION.md`](UNIFIED_GATEWAY_WEB_MIGRATION.md)。

当前获准的待实现 Goal 见 [`REFACTOR_IMPLEMENTATION_GOALS.md`](REFACTOR_IMPLEMENTATION_GOALS.md)。

迁移来源只提供行为和测试证据：

- `G:\kol`：跟单语义、账本、事务 outbox/inbox、六所协议 fixture，以及 `apps/web` 的响应式 UI 行为和测试；
- `bak/VenueCore`：VenuePulse 指标、VenueFlow 桌面框架及交易所网关缺口；
- Condor 会话：Agent、指标计算和确定性执行分层思想。

迁移必须在本工作区建立独立实现和测试，不得通过 path dependency、include、symlink 或运行时路径依赖上述来源。

## 2. 固定架构决策

1. 采用一个 Rust workspace 的模块化单体，不复制 KOL 的二十四包多进程拓扑，也不引入 Condor/Hummingbot 作为执行核心。
2. 每个 `(venue, trading_account_id)` 仍由一个账户节点进程拥有；`trading_account_id` 是凭证映射到真实交易账户的稳定规范 UUID，不是 `portfolio_margin_um` 等产品类型，也不含 symbol；同一真实账户任意时刻只有一个精确 writer。
3. 网格、Scalping 和跟单都只输出语义意图；所有交易 mutation 统一经过账户 Runtime、Execution、Risk、Owner、WAL 和 Reconciliation。
4. 跟单是独立 Copy Engine，不直接持有交易所客户端、凭证、writer lease 或 native mutation 权限。
5. Web、桌面 UI、Control API 和 Agent 都属于慢速控制面，不进入成交热路径，不直接读取或改写 WAL；桌面端可使用严格无凭证、
   无账户身份、无 mutation 的本地公共行情通道，但所有账户查询、控制和交易权威仍只来自 Control/账户节点。
6. 规范交易所集合固定为 Binance、Bitget、Bybit、Gate.io、Hyperliquid、OKX；策略是否可在某所运行仍由产品、账户和订单族能力证据决定。
7. 网关运行模式只接受精确 `LIVE`；不设计测试网、demo、只读、Shadow 或隐式 `live=false` 模式。
8. KOL 的响应式 Web 体验选择性迁移到独立 `apps/venue-web`；旧策略、重复网关/后端、Condor 策略、VenuePulse 中未采用的
   策略特征和 VenueFlow 模拟交易不迁移。

所有 `LIVE` 配置必须显式提供规范 `trading_account_id`。`account_binding` 仅描述 API 产品与账户模式能力；同一真实账户的不同交易对和策略必须复用同一 UUID。缺失、格式错误或与持久工件不一致时拒绝启动，禁止由 API Key、产品类型或 symbol 临时推导账户身份。

## 3. 目标进程拓扑

```text
Venue Web -> same-origin BFF --+
VenueFlow Desktop / Agent -----+-> venue-control
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

`venue-control` 校验 schema v2 scope，以 PostgreSQL durable inbox/outbox、fencing delivery lease、幂等 claim 和终态 receipt 保存命令，并提供
仅本地 HTTP/SSE `/v2`。LIVE-only Copy worker 可在事务内锁定 leader 事件并持久化纯规划结果、delivery、ledger 与恢复状态；
节点 ACK 只证明本地 inbox 已耐久，Unknown 只能进入下一序号只读对账。Node 已接入单一 opaque-journal adapter 与 bounded loopback HTTP polling client；每次 await 后都以当前时钟重验 lease/session/epoch，过期 outbox 不确认或重放。Copy delivery 严格校验 immutable manifest 并派生规范 Actor/Owner；runtime 只从恢复后的真实 WAL head 写 anchored Actor journal/checkpoint，receipt 必须精确匹配 delivery/inbox commitment 后才可 Applied。Copy 不能直接提交订单且始终不授予 mutation authority；唤醒通道不能
代替耐久记录；节点仍须先持久化本地 Actor inbox，再独立重验 risk、Owner、WAL、writer 和私有事实。

## 4. 目标 workspace

```text
apps/
├─ venue-node/
│  └─ src/bin/venue-node-{binance,bitget,bybit,gate,hyperliquid,okx}.rs
├─ venue-control/
├─ venue-web/                 # 目标响应式用户 Web；建立前以迁移文档为入口
└─ venueflow/                 # 内部运维/诊断桌面与 WASM 工具

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
│  └─ src/{hedged_grid,scalping}/
├─ venue-copy/
├─ venue-execution/
│  └─ src/{journal.rs,writer_lease.rs,canonical_root.rs,owner_routes.rs}
├─ venue-storage/
│  └─ src/{journal.rs,control_delivery.rs,actor_applied.rs,...}
├─ venue-runtime/
│  └─ src/{authority.rs,account_lane.rs,account,strategy,grid,scalping,copy,shared,legacy}/
└─ venue-control-protocol/
```

六个节点 binary 必须各自只链接一个交易所 adapter。构建验收继续扫描生产 endpoint，拒绝把其他 adapter 链接进
固定节点。`legacy` 只是有退出条件的迁移隔离区，不允许新增功能。

当前 `apps/venue-node` 已建立六个固定产物和逐 feature 二进制隔离门禁。Binance、Gate.io、Bitget 的实盘 mutation 仍由
Stage 7 兼容链掌握；Bybit、OKX、Hyperliquid 已通过 `AccountMutationHost` 提供显式 preflight/place/cancel Canary。
六所尚未共同进入一个常驻 Account Runtime，Copy semantic Applied 也尚未进入物理 Execution Lane。迁移期不得扩展 Stage 7
authority；统一和逐所接管按 `UNIFIED_GATEWAY_WEB_MIGRATION.md` 执行。

## 5. 依赖方向

以下 `A -> B` 表示 A 依赖 B：

```text
venue-indicators -> venue-domain
venue-gateway-api -> venue-domain
venue-gateway-* -> venue-domain + venue-gateway-api
venue-strategies -> venue-domain
venue-copy -> venue-domain
venue-storage -> venue-domain
venue-control-protocol -> venue-domain + venue-gateway-api
venue-execution -> venue-domain + venue-gateway-api
venue-runtime -> venue-domain + venue-execution + venue-storage + venue-gateway-api
venue-node-<venue> -> venue-runtime + exactly one venue-gateway-*
venue-control -> venue-control-protocol + sqlx + credential-admin-only adapter probe
venue-web BFF/client -> versioned venue-control HTTP/SSE contract
venueflow / optional Agent -> venue-control-protocol
venueflow local public market -> venue-gateway-api + public-only adapter surface
```

硬边界：

- `venue-domain` 不依赖业务、数据库、网络、UI 或交易所；
- runtime turn/capability authority identity 唯一实现位于 `venue-runtime`，发行构造器保持 crate-private；根 `domain` 只兼容重导出类型，不得为了物理迁移公开授权构造器；
- adapter 不依赖 runtime、strategy、copy 或 UI；原生协议不得越过 adapter；
- strategy 和 copy 不依赖具体交易所、凭证、native symbol 或物理客户端；
- `venue-control-protocol` 只含版本化 DTO、错误码和序列化契约，不含 Axum handler、数据库、runtime 或 application service；
- UI 不依赖 execution、私有/交易 adapter 或数据库；账户查询和命令只依赖版本化 Control protocol。原生桌面端可另依赖
  `venue-gateway-api` 的 secret-free public binding 和明确 public-only 的 adapter surface，不得调用账户私流或 mutation；账户表单仅允许下述窄凭证管理传输；
- Web 浏览器只访问同源 BFF；BFF 只代理 allow-list 内的版本化 Control 查询和语义命令，不提供任意透传或交易所客户端；
- Control API DTO 不作为交易权威事实，节点必须重新验证；
- 不复制 Symbol、Money、Order、Position、Fill、InstrumentRule、Capability 或 journal 类型。

## 6. Runtime 内部结构

`venue-runtime` 按责任分组，禁止重新回到根目录平铺；未实际迁入的依赖不得预装：

```text
authority.rs   账户/实例 identity、订单族 capability 与 opaque turn/applied authority
account_lane.rs 账户公平调度、Unknown fence、WAL 前/后授权分态
account/   Registry、Market Hub、Private Router、Reconciler、恢复与 Actor host
strategy/  顺序邮箱与 Strategy turn
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

当前 `venue-copy` 已落地纯资本/目标敞口 reducer、确定性身份、数量 sizing、跨所 LIMIT 转换、delivery/receipt 状态、幂等 ledger 与 drift repair。它只使用同 generation、同估值币种和
有效期内的冻结资本事实，并按 configured、allocated、扣安全储备后的 available margin 三者最小值确定 follower 有效资本；
跨零反向必须先平仓并等待新私有事实。规范 `InstrumentIdentity`、Precision、ContractSpec、metadata 与 snapshot 只存在于
`venue-domain`：sizing 精确绑定 instrument/reference generation 与半开时效窗口，把 quote delta 向下归一为 base quantity 或
contract lots，显式返回 reduce-only 与残差；LIMIT 只保留同一稳定 instrument 的相对偏移，BUY 向下、SELL 向上对齐且不得跨越
风险边界。job、planning snapshot、child order 和 idempotency key 继续按 length-delimited SHA-256 与 domain separation 固化。
delivery manifest 把 job/snapshot/child/idempotency、leader/follower/account/instrument/policy、plan digest、generation 和短时效窗口绑定为不可变提交；
账户节点持久回执只允许 Applied/Unknown/Reconciled/Rejected，Unknown 封存旧授权且只能由下一序号精确对账收敛。ledger 对精确重复
no-op，拒绝冲突、跳序和 generation 回退，并显式区分 Copy、External、Manual 归因；drift repair 只从不超过 60 秒的新鲜权威持仓和
可重算目标生成全新 job 的语义请求，跨零仍须先平仓再等待新私有事实。这些 reducer 均不依赖 storage、network、runtime、native symbol
或 writer。Control/PostgreSQL relation、delivery、claim/ACK/receipt 与 Node semantic Applied 已接入；当前缺口是从 Copy Actor
生成 follower 账户执行意图，并把签名执行事实回写 ledger/drift。

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

六所规范身份、精确 `LIVE` 模式和账户/交易对 binding 统一由 `venue-gateway-api` 提供。面向当前 10–20 用户的 MVP，Bybit、OKX、Hyperliquid 已采用较小安全闭环：账户进程锁、单一命令 WAL、Owner 字段、10U 风险上限、Unknown 禁重投及签名对账；不复制旧 Stage 7 的分布式 lease、handoff 或多层 receipt。

| Venue | adapter | 协议与账户接线 | 准入边界 |
|---|---|---|---|
| Binance | `venue-gateway-binance` | Portfolio Margin HTTP/私流、账户级事实及 fills cursor、policy-exact 限价/撤单/减仓回读；`account_gateway.rs` 接入统一 Node | 仅 `LIVE`；同一账户 Host/WAL/Lane，旧服务器 release 接管另行验收 |
| Bitget | `venue-gateway-bitget` | UTA 账户五面同 attempt、订单族完整性、policy-exact 限价/撤单/减仓与 Unknown 回读；`account_gateway.rs` 接入统一 Node | 仅 `LIVE`；同一账户 Host/WAL/Lane，不继承旧 candidate authority |
| Gate.io | `venue-gateway-gate` | Hedge 双腿、完整订单族、fills cursor、policy-exact 限价/撤单/减仓回读；`account_gateway.rs` 接入统一 Node | 仅 `LIVE`；同一账户 Host/WAL/Lane，真实单笔规则不作为存量仓位上限 |
| Bybit | `venue-gateway-bybit` | UTA2/双向持仓和权限预检；PostOnly/Gtc 限价、精确撤单与签名回读 | 仅 `LIVE`；经账户 Host，初期账户累计名义风险上限 10U；已有持仓或未撤入场时拒绝增险 |
| OKX | `venue-gateway-okx` | Long/Short Cross 预检；SWAP `ctVal × ctMult × contracts` 换算；PostOnly/Gtc 限价、精确撤单与签名回读 | 仅 `LIVE`；经账户 Host，按张数向下取整，初期账户累计名义风险上限 10U |
| Hyperliquid | `venue-gateway-hyperliquid` | API Wallet 绑定、账户预检、持久 nonce；ALO/GTC 限价、cloid 精确撤单与签名回读 | 仅 `LIVE`；经账户 Host，非 USDT 报价真实估值，初期累计风险上限 10U；已有风险时拒绝增险 |

KOL 网关只是协议 fixture 和差异对照来源，不继承其运行开关或实盘准入状态。六所当前源码只允许固定 Node 通过统一账户链执行；旧服务器进程是否退休、策略驱动及产品是否验收，须分别提供证据，不能从 adapter 或 fixture 通过推断。

网关只使用生产 endpoint、实盘账户和按 venue/account 隔离的 LIVE 工件。离线 fixture、mock transport 和 parser 测试属于
测试工具，不是运行模式，也不得连接真实交易所或发送 mutation。
现有 Stage 7 中名为 Shadow/verify 的命令是冻结的策略证据验收流程，不是 gateway mode；新 adapter 不得沿用该命名建立第三种运行状态。

单所 adapter 一旦完成精确账户/交易对绑定、唯一 writer、Owner、WAL、独立小额限额、
签名私有 readback、确定性订单身份、UNKNOWN 先对账和紧急 Stop/Flatten，就可对一个 binding 开始小额 `LIVE` Canary 调试。
本迁移任务的 AI 持续授权、10U 上限和无需逐次人工确认规则见 `UNIFIED_GATEWAY_WEB_MIGRATION.md` 第 2.1 节；产品 UI 的高风险确认仍保留。
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

VenuePulse 当前迁入单一 `venue-indicators` crate，并按实际职责组织：

```text
feature_frame / public_book / public_market_source / scalping_features
```

crate 通过窄 `PublicBook` 适配器消费 `venue-domain` 的 Bar、Trade 与盘口，禁止复制权威
行情类型。规范 closed `PublicBar` 显式保留 base/quote volume、trade count 与 taker-buy base/quote volume 的 Known/Unavailable
状态；Missing、Null、NotApplicable、负量、taker 超总量或可证明的 quote/price 边界矛盾均失败关闭。现有 feature builder 已真实消费
bar base volume，并继续绑定 generation/provenance。指标内部允许使用 `f64`，但任何价格、数量或风险 mutation 必须回到 Decimal 领域类型并重新校验。

`mas_frame`、`market_maker_*` 和其他策略特征不迁移。指标 crate 不读取凭证、数据库、WAL 或交易所客户端。

## 10. 客户端

### 10.1 VenueFlow 内部工具

桌面技术栈固定为 Rust 2024、`eframe/egui`、`egui_tiles` 和 WGPU；异步网络使用 Tokio 与 WebSocket/SSE client。

当前第一版 `apps/venueflow` 已用同一套 eframe/egui_tiles/WGPU 视图提供原生窗口与 WebAssembly canvas；native client 使用
Tokio/reqwest/SSE，Web client 使用 reqwest/EventSource。`venue-control-protocol` schema v2 固定 `/v2/ui/snapshot`、
`/v2/ui/events` 和 `/v2/control/commands` 的 DTO、递归校验、receipt 与错误边界。策略和命令都显式携带精确 `LIVE`，两端只显示查询投影并提交语义控制请求；Stop/Flatten
必须携带精确 mode、account、symbol、instance、config epoch、action 与人工确认。`apps/venue-control` 已提供本地 HTTP/SSE server、schema scope 重验、PostgreSQL durable inbox/outbox 及 bounded 节点 claim/ACK/receipt 路由；Node polling、storage adapter 和绑定真实 WAL head 的 Actor Applied 已接入，但 Copy 尚未生成账户执行意图，Control 仍无物理交易执行权限。

保留范围：

- 窗口、dock 布局、主题、图表、工作区持久化和指标面板；
- KOL 的 leader/follower、binding、目标敞口、漂移、执行状态和账本查询交互；
- Grid、Scalping、Copy 实例的查询、Pause/Stop/Flatten 与人工确认流程。

删除 Alpha DTO、旧策略 analytics、模拟交易和 UI 内 mutation gate。VenueFlow 不持久化交易所凭证，不读取 PostgreSQL 或 artifacts；
账户、策略、账本和控制只调用版本化 Control protocol。原生桌面端允许为行情/K 线直连生产公共 REST/WS，但 binding 必须无账户、
无 secret、无私流、无下单能力，且 public-only 依赖边界须由构建测试证明。高风险操作必须显示并确认精确
mode/account/symbol/instance/config epoch/action，服务端和账户 runtime 仍须独立重验。

登录、注册、API 加密绑定、验证和执行账户选择见 [`ACCOUNT_MANAGEMENT.md`](ACCOUNT_MANAGEMENT.md)。这是当前获准的窄账户管理功能，
不引入付费权限、独立认证服务或多租户控制面。行情选择无需登录；账户表单可短暂持有输入并经版本化账户 API 提交，
Control 仅作加密存储和 adapter 签名只读探测。API 可访问、Node 状态和交易准入不得混为一谈。

### 10.2 响应式 Web

面向用户的主 Web 独立建立在 `apps/venue-web`，技术栈固定为 Next.js 16、React 19、TypeScript 严格模式和同源 BFF。
从 `G:\kol\apps\web` 迁移响应式 shell、移动端交互、恢复失败关闭、十进制和页面信息架构；全部 `/v1` DTO、认证、数据访问和
命令层按 Venue schema v2 重写。浏览器不直连 loopback Control、数据库或交易所，不接触交易所 secret。详细边界和页面顺序查
[`UNIFIED_GATEWAY_WEB_MIGRATION.md`](UNIFIED_GATEWAY_WEB_MIGRATION.md) 第 6 节。

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
和稳定身份。Node 凭证仍只来自进程环境或根 `.env`。当前获准的 UI 绑定例外只允许账户管理模块在 PostgreSQL 保存
经过用户/凭证 ID 认证绑定的密文；主密钥仅来自进程环境，UI 不持久化输入，明文不得写入 TOML、日志或工件。
具体加密、认证、删除和操作边界见 [`ACCOUNT_MANAGEMENT.md`](ACCOUNT_MANAGEMENT.md)。

## 13. 技术栈

| 层 | 选择 |
|---|---|
| 语言 | Rust 2024；工具链和 workspace `rust-version` 精确锁定 Rust 1.98.0 |
| 交易 Actor | 顺序状态机、专用 OS thread 或受控 executor、有界 mailbox |
| 异步 I/O | Tokio；不授予执行顺序或 mutation authority |
| HTTP/控制 | schema v2、PostgreSQL repository、仅本地 HTTP/SSE `/v2`、bounded Node polling 路由、LIVE-only Copy worker 和耐久 Actor Applied |
| 交易 HTTP/WS | reqwest、tokio-tungstenite 或经等价验收的现有阻塞 transport |
| 数值 | rust_decimal 处理交易金额；指标内部可用 f64 |
| 数据库 | PostgreSQL、SQLx、显式 migrations |
| 交易耐久化 | append-only JSONL、hash chain、fsync checkpoint/receipt |
| 桌面 | eframe、egui、egui_tiles、WGPU |
| Web | Next.js 16、React 19、TypeScript 严格模式、同源 BFF、响应式桌面/移动端 |
| 日志与追踪 | tracing；凭证和私有 payload 默认脱敏 |
| 配置 | TOML + process env/root `.env` secrets |

依赖基线按用途优先执行：

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

当前功能确有需要时可以增加基线外依赖，但必须先检查 workspace 与 `Cargo.lock`，证明现有依赖不能满足，并在同一修改中
加入真实调用和专项测试。同一用途原则上仍复用既有实现；只有可验证的技术缺口才允许第二套同类直接依赖，不得为未来模块预装。
本地 Cargo 构建目录固定为 `G:\Build\Venue`；验证脚本可在其下使用按 PID 隔离的子目录。
现有 Stage 7 直接 `tungstenite` 是行为等价迁移前的唯一冻结例外，不允许增加新调用点；它与 `tokio-tungstenite` 的底层传递性关系不构成新的第三套 WebSocket 实现。

不得在同一阶段同时重写网络 transport、交易状态机和持久化布局。当前 Stage 7 可继续使用既有阻塞 I/O，先完成 crate
和目录边界迁移；异步 transport 只能在行为等价测试和逐所 Canary 后替换。

## 14. 迁移顺序

当前实施顺序以 [`UNIFIED_GATEWAY_WEB_MIGRATION.md`](UNIFIED_GATEWAY_WEB_MIGRATION.md) 的 M0–M7 为准：先完成统一账户执行脊柱
和 Copy 物理闭环，再按 Binance、Gate.io、Bitget、Bybit、OKX、Hyperliquid 的顺序完成单 writer 接管；Web 可在稳定的
Control schema 上并行开发只读页面，但高风险写操作必须等待对应后端事实和失败关闭门禁。旧入口只有在调用点清零、恢复兼容、
行为等价和实盘接管全部成立后才能删除。

## 15. 验收门槛

每次结构或行为迁移至少满足：

- `cargo fmt --all --check`、`cargo check --workspace --all-targets`、`cargo test --workspace`；
- `rustc --version` 为 1.98.0，所有 workspace package 的 `rust-version` 必须显式设为或继承 1.98.0；
- workspace 依赖图无反向依赖或循环；
- 新增依赖已证明当前必需，且未引入白名单同用途的第二套直接依赖；
- 六个节点 binary 只接受精确 `LIVE`，拒绝任何测试网/demo 标记，并保持 endpoint/凭证/binding/artifact 按交易所和账户隔离；
- `scripts/verify_gateway_candidate_contract.ps1` 的逐 binary 构建、隔离和零工件失败关闭探针通过；其矩阵中未接线项保持 `not_reached`、`writer_enabled=false`，不能替代实盘准入；
- 同账户同 symbol 的第二 Owner 被拒绝；
- Grid、Scalping、Copy 都不能绕过唯一 Execution Lane；
- Copy UNKNOWN 重启后先查事实且不重复下单；
- PostgreSQL outbox、账户 WAL 和 copy ledger 的崩溃边界可重放且不产生双 writer；
- Control 命令在 outbox 写入、节点 inbox 持久化、Actor applied 和 receipt outbox 任一边界崩溃后均不漏单、不双发；
- UI/Agent 无凭证、无物理订单客户端、无 artifacts 写权限；
- `LIVE` 调试只能在最小安全闭环成立后对一个精确 binding 小额执行，不因调试准入扩大策略产品权限。

## 16. 明确不做

- 不迁移 `bak` 或 KOL 的策略实现；
- 不复制 Hummingbot、Condor runtime、KOL Rust 后端、重复网关或多服务部署；KOL Web 只选择性迁移 UI 行为和测试；
- 不让 PostgreSQL、Control API、UI 或 Agent 直接写交易所；
- 不为六所强造相同能力；
- 不同时运行两个版本写同一 binding；
- 不在迁移期间清理、覆盖或重解释现有交易 artifacts。
