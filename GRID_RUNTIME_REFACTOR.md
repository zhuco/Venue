# 多策略、多交易所账户运行时与对冲网格开发规范

更新：2026-08-30

## 1. 文档职责

本文是多策略、多交易所实盘和对冲网格的唯一长期开发契约，说明当前采用的架构、边界、成交热路径、恢复规则、迁移顺序与验收要求。

[`ARCHITECTURE.md`](ARCHITECTURE.md) 定义合并跟单、六交易所网关、指标和桌面 UI 后的目标 workspace。六所是网关目标覆盖，
网关只接受精确 `LIVE` 并只使用生产 endpoint；这不自动扩大当前 Stage 7 网格的三所实盘准入，新增交易所仍必须独立完成策略、恢复和接管验收。
本文现有 Shadow/verify 是策略与工件证据流程，不是第三种网关运行模式。

`CODEMAP.md` 只负责定位代码。事故时间线、已完成阶段、临时发布号和一次性排障记录不得继续写入长期文档。

离线 fixture、mock 与数据库集成测试保留，但不得成为交易所运行模式或连接真实交易所。

当前选择的架构不是功能最多的方案，而是最适合“主要由一个人使用、需要多个交易所和同账户多个交易对、不要反复重构”的方案：

- 每个交易所账户一个常驻进程；
- 账户进程内运行多个 Strategy Actor（策略运行单元）；
- 一个规范交易对只允许一个策略实例拥有；
- 行情和账户私有连接共享，策略状态、订单归属和持久化隔离；
- 所有下单、撤单和市价减仓经过账户内唯一 Execution Lane（执行通道）；
- Web 面板或 Windows UI 以后只接控制与查询接口，不进入交易热路径。

现有 Stage 7 共享网格运行时是迁移中的可运行实现。新增修复必须符合本文边界，不再扩展成另一套交易所专用运行时。

账户级纯内核固定在 `crates/venue-runtime/src/account/`，策略顺序邮箱固定在 `crates/venue-runtime/src/strategy/`，账户执行调度契约固定在
`crates/venue-runtime/src/account_lane.rs`；根 `src/runtime/account/`、`src/runtime/strategy/` 与 `src/execution/account_lane.rs` 只保留兼容 facade。这些模块只建立身份、注册、路由、调度和对账边界，不持有连接、凭证、WAL、
writer 或物理交易客户端。`legacy_stage7_strategy_binding` 只转换单策略身份，不授予 mutation 能力。配置中的
`trading_account_id` 是真实账户的稳定规范 UUID，跨 symbol/策略复用；交易所 `account_binding` 只表示产品/模式能力。
迁移完成前，现有 Stage 7 仍是唯一实盘 writer；不得同时为同一账户启动账户内核的新物理执行路径。
通用 `CommandJournal`、writer lease 与账户级 canonical-root fence 固定在 `venue-execution`；`DurableOwnerRoutes` 独占并重放同一 CommandJournal，以 family + native identity 生成精确 cancel route，不增加第二本 journal 或 mutation authority。原 JSONL serde/hash、writer schema、fsync、调用方工件路径及机器级 `stage7_writer_roots/v2` 路径保持不变。`venue-storage` 的单一 `DurableJsonl` 同时支撑 facts 与 opaque Control journal，append 在排他锁内核对耐久长度并同步文件/父目录；旧进程、坏尾、空行、hash/状态迁移分叉均失败关闭。Stage 7 继续经根 facade 使用原实现。
六所 adapter 均已补齐绑定型 async HTTP/私有 WS、订单族/持仓/成交证据、单次 mutation 候选及 ACK 后只读收敛；Bybit、OKX、Hyperliquid 另有可持久化 probe 候选。部分固定 `run()` 只实例化 inert candidate 做隔离/失败关闭预检；没有 fixed binary 构造 `TokioPhysicalGateway` 或把候选接入 `NodeSafetyHost`/dispatch，静态 capability、Node writer 与 WAL 均未因此开启。
`apps/venue-node` 已提供六个逐 adapter 固定产物：Binance、Gate.io、Bitget 在精确 `LIVE` 下委托现有 Stage 7
部署入口并继续使用原 Owner/WAL/writer/reconciliation/Canary 契约；非 LIVE 输入在 endpoint、凭证和工件初始化前拒绝。Bybit、OKX、Hyperliquid 节点只验证 secret-free binding、adapter endpoint、
凭证环境变量命名空间与隔离 artifact root。Hyperliquid 固定使用 `HYPERLIQUID_ACCOUNT_ADDRESS`、`HYPERLIQUID_API_WALLET_ADDRESS`、`HYPERLIQUID_API_WALLET_PRIVATE_KEY` 三项必填及 `HYPERLIQUID_VAULT_ADDRESS` 可选；普通账户的读取/交易地址由 account 得出，Vault/Subaccount 模式由可选地址存在得出。API Wallet 地址必须由私钥推导一致，禁止额外 `is_vault` 开关、Agent 名称或重复 user/master 地址造成矛盾配置。`safe_host` 与 `supervision` 仍维护 root/WAL/Owner/writer metadata、独立 control log、一次性 permit 与 UNKNOWN 禁重投。共享层已有耐久 Owner/native identity、六面物理恢复值类型、`ProvenAbsent`、bounded loopback Control polling、单 runtime async 与 host capability 候选契约。Binance、Gate.io、Bitget 已新增生产可达但非 authority 的 authenticated 只读 collection session：冻结凭证、endpoint、generation、完整 symbol/cursor/订单族请求面、deadline 与全局 pages/bytes，逐 HTTP await 重验并拒绝 caller Owner/root；Bitget 最终六面 fold 仍关闭，三所 candidate 均不携带 capability、writer、WAL 或 permit。普通调用方的 capability promotion/authorize 已固定失败关闭，不能再用自组 receipt 升级；runtime 也已用私有 issuer seal 签发绑定完整账户 universe 与五类耐久 root 的恢复 session，并在 await/refresh/install 重验。生产仍缺三所 collector 到 runtime durable universe/root 的桥接、Bitget final fold、Bybit/OKX/Hyperliquid authenticated collector、耐久 replay refresh adapter 与真实 host promotion verifier，因此 physical installer、Ready、Actor turn、host admission 和 async mutation统一失败关闭；Prepared 在 admission 失败时耐久 Rejected，物理调用为零。Bybit、OKX、Hyperliquid 的 mutation builder、签名、POST 与 dispatch 仅在 `cfg(test)`，不能由 feature 在生产构建复活。
Stage 7 成交热路径不得遍历历史命令 WAL。命令 journal 在启动重放时建立未决命令、撤单目标和交易所订单 ID 的派生内存索引；滚动补撤批次以原 JSONL 格式一次 fsync 持久化 Prepared/Submitted 状态，再并行提交物理请求。接管只可在签名全订单族为空、零未决且零本地事务后显式按源 SHA 封存已解析 WAL；原件留在同 root，活动 WAL 从空文件继续，禁止运行中轮转或删除审计源。

纯内核启动必须先安装覆盖 lifecycle、config epoch、Stop/Flatten fence、连接代际、已应用私有游标、完整批次
delivery manifest、Actor inbox、WAL/Owner 与 Unknown 的完整恢复收据，空新账户也不能省略。恢复 manifest 必须以
稳定字段编码绑定五类 journal 的 root、tail、record count 与完整 replay 投影；截断任一 UNKNOWN、Owner route 或批次均失败关闭。连接代际只能在已恢复下限上 checked+1。
Actor 每次取件由运行时签发绑定 connection/private/config/turn 的不可伪造 token；只有 inbox/checkpoint 已持久化的
applied receipt 才能更新 Desired Orders、进入 Running 或生成执行意图。命令/native identity 还必须由账户 journal
分配收据绑定订单族，策略调用方不得自报这些权限。重连、参数变更或更新一代签名对账不会复活未入 WAL 的旧意图；
已进入 WAL 的 Unknown 必须按原 command、native client identity、订单族和更新连接的完整回读证明收敛，不能直接重发。
物理恢复的内存值类型/验证器还要求同一 attempt、严格更新的 private generation 完整交付 Account、Positions、三个订单族与
FillsCursor 六面，并绑定 mode/account/native binding、完整 registry universe/config epoch、position mode、profile version、connection/private generation 与 Owner/WAL/structured Unknown roots；任一 WAL、Unknown、root、config、profile、Pause/Freeze、私有路由或 Actor 状态漂移立即撤销 session/Ready/turn。runtime-issued opaque session 已把 attempt/session epoch、完整 universe、五类 journal/checkpoint head 与运行时 authority-state commitment 纳入同一不可构造 authority；install 强制至少一次 post-await refresh，生产 refresh 构造在可信耐久桥接入前保持封闭。六所测试 fixture 覆盖 endpoint/session/freshness、raw replay、exact Owner 与结构化 Unknown，但生产入口不得从普通 candidate、持久 probe、caller generation 或 caller digest 构造准入；在六所 collector 消费 session、每次网络 await 后复核且耐久 adapter 签发 refresh 前，账户 startup 与固定 binary 统一返回 integration unavailable，也不生成持久 manifest 工件。

## 2. 术语

- Account Runtime（账户运行时）：一个交易所账户对应的常驻进程。
- Market Hub（行情分发器）：每个交易所共享公共行情连接并分发规范行情。
- Private Router（私有事件路由器）：共享账户用户流，按订单身份把事件路由到唯一策略。
- Strategy Actor（策略运行单元）：一个策略实例的单线程顺序状态机。
- Execution Lane（执行通道）：账户内唯一物理交易写通道。
- Reconciler（对账器）：用完整签名账户事实检查并修复本地投影。
- WAL（预写日志）：发送交易请求前先持久化命令，供崩溃恢复。
- Owner（订单归属）：能唯一确定订单属于哪个策略实例的身份。
- GTX / Post-only（只做挂单）：订单不得立即吃单。
- Checkpoint（检查点）：策略可恢复状态快照。
- Desired Orders（目标订单）：策略状态机当前明确要求存在的订单集合。

规范交易对统一使用 `BASE/QUOTE`，例如 `DOGE/USDT`。交易所原生 symbol 只能存在于 adapter（适配器）内部。

## 3. 固定架构决策

### 3.1 进程与实例

进程键为：

```text
(exchange, account)
```

策略实例键为：

```text
(exchange, account, strategy_kind, instance_id, symbol)
```

固定约束：

1. Binance、Gate.io、Bitget 各账户独立进程，不能共用一个跨交易所进程。
2. 同一账户可运行多个不同交易对的网格或剥头皮实例。
3. 当前不支持同一账户、同一交易对运行多个策略。
4. 同一交易对的所有订单和仓位只归一个 Strategy Actor。
5. 一个策略故障只暂停该实例；账户连接故障才影响该账户进程；一个交易所故障不拖累其他交易所。

### 3.2 账户进程内部结构

```text
Exchange Account Process
├─ Market Hub（行情分发器）
├─ Private Router（私有事件路由器）
├─ Strategy Actor A：grid / DOGE-USDT
├─ Strategy Actor B：grid / BTC-USDT
├─ Strategy Actor C：scalping / ETH-USDT
├─ Execution Lane（唯一执行通道）
├─ Reconciler（账户对账器）
└─ Control API 边界（`apps/venue-control` 已提供本地 HTTP/SSE，账户节点 adapter 未实现）
```

数据流必须单向：

```text
交易所事件 -> 归一化 -> 路由 -> 策略 reducer -> 语义意图 -> 执行通道 -> 交易所
```

策略不得直接持有凭证、创建交易所客户端、写原生协议或绕过执行通道。

### 3.3 代码边界

- `domain`：规范类型，不依赖策略或交易所。
- `exchange`：原生协议、签名、分页、限频、原生 symbol 与规范事实转换。
- `strategy`：纯 reducer（状态归约器），输入事实并输出语义意图。
- `venue-runtime/account`：账户级身份注册、规范路由、对账和生命周期纯内核；物理连接由后续 runtime 组合层持有。
- `venue-runtime/strategy`：目标策略实例宿主、Checkpoint 和恢复；opaque turn/applied authority 的发行构造器保持 crate-private。
- `venue-runtime/account_lane`：账户级公平调度、Unknown fence 与 WAL 前/后授权分态，不持有 writer、WAL 或客户端。
- `venue-execution`：通用命令 WAL、唯一 writer lease、账户 canonical-root fence 与命令哈希；根 execution 其余物理门禁保持现状。
- `risk`：少量账户级硬上限与策略自身风险逻辑。

同一策略族跨交易所必须复用一个 reducer。交易所差异只进入 adapter、能力证据、Execution Profile（执行配置）或 Deployment Binding（部署绑定）。

## 4. 共享组件职责

### 4.1 Market Hub（行情分发器）

- 每个账户进程对同一交易所尽量共享公共连接；
- 维护持续更新的 BBO（最优买卖价）和必要深度；
- 每个事件带交易所时间、接收时间和单调序号；
- 慢策略不能阻塞行情读取；仅 Snapshot、Ticker、MarkFunding 可保留最新值，Delta、Trade、Bar 必须进入有界无损队列；
- 私有或连续行情邮箱满载必须显式失败并封锁相关新增风险，不能静默丢事件；BBO 新鲜度只用连接代、交易所事件时间及同事件族序号，不用本机接收时间，也不比较不同事件族的序号；任一事件族进入新 symbol generation 时必须清空该 symbol 全部旧 watermark、BBO 和 Actor 行情队列。BBO 只参与初装、整网重建及显式再中心化，不参与成交滚动；这些非滚动 mutation turn 的完整签名私有 readback、风险或规则核验若可能超过 BBO 新鲜窗口，必须在任意 WAL/mutation 前再次有界排空并持久化期间已到达的公共帧，再按新的当前时钟复核 BBO；closing wave 的签名确认也可能跨越该窗口，因此在 opening wave 尚未 dispatch 前必须再次持久排空并重采 BBO，只有全量 opening 仍为 post-only 才可发出；刷新只更新数据，不授予 writer、risk 或 dispatch authority。
- WebSocket 一次建连的 DNS、全部解析地址、TCP、代理 CONNECT、TLS 与 upgrade 共用 10 秒总期限，禁止每个地址重新获得完整超时；失败后的公共、私有及启动连接按有上限指数退避，并用账户/进程/失败代际错峰，禁止固定间隔同步重连风暴。

### 4.2 Private Router（私有事件路由器）

- 每个账户共享一套用户流；
- 原始事件先落持久证据，规范事实只能消费持久收据并通过完整领域校验；一条原始证据产生多条事实时必须按 index/count 整批缓存、整批路由，禁止先投递 fact 0；
- 路由计划的全部 Actor delivery 必须先写 durable inbox，整批事实全部可归属且持久收据提交后才能推进 Router cursor；任一事实失败时保留整批供原证据重试，禁止只推进前缀；applied cursor 只在连续序列的全部目标 Actor applied receipt 完成后推进，后序 fully-applied 或零 delivery 批次必须跨重启保留为 completed；
- Client Order ID 与 venue order ID 均以 canonical order family 为命名空间；二者同时存在时必须完整命中同一持久 Owner 映射，半匹配也按身份冲突处理；
- 完整、可归属成交直接进入对应策略的最高优先级邮箱；
- 无法归属必须冻结相关 symbol 并触发对账；身份冲突、symbol 不符或 Owner 已失效必须账户级失败关闭；
- 不允许广播给所有策略后猜归属。

### 4.3 Strategy Actor（策略运行单元）

- 单实例内部严格顺序执行，不允许两个线程同时修改状态；
- 只消费规范行情、私有事实、控制命令和对账结果；
- 只输出 Place（挂单）、Cancel（撤单）、Replace（补撤）、Reduce（减仓）、Pause（暂停）等语义意图；
- 维护自己的目标订单、成交游标、恢复状态和 Checkpoint；
- 不读取其他实例文件，不修改其他 symbol。
- 私有事实有界优先，但连续处理 64 条后必须让行一次对账或控制；控制和对账之间轮转，避免任何一类永久饥饿。
- 一个实例同时最多一个未确认 turn；仅把对账 notice 放入邮箱不得切换 Running，必须等对应 applied receipt。

### 4.4 Execution Lane（执行通道）

- 一个账户只有一个精确 writer；
- 机器级 canonical root 与进程锁由 `venue-execution` 按 `(exchange, trading_account_id)` 建键，不含 symbol、Owner 或策略；Stage 7、旧网格、Scalping Live、Canary 和可写恢复必须先取得同一账户 fence，Shadow 不占用；
- 负责 Owner 校验、WAL、Client Order ID、基础数量/价格精度、账户硬上限和交易所限频；创建订单在入队时原子保留 `(family, client id, Owner)`，Cancel 必须精确命中同 Owner、同 family；
- 高优先级：成交后的补撤、止损、减仓、紧急撤单；
- 中优先级：策略正常挂撤；
- 低优先级：周期查询、统计和报表；
- 同一 Client Order ID 重试必须幂等，Unknown（结果未知）命令先查事实，不能直接重发。
- 队列必须有界；Critical 连续服务也必须周期让行 FillRepair/Normal，不能让兄弟实例或普通工作永久饥饿。
- 调度候选不构成 mutation authority，也不得克隆出可执行请求。命令只有在精确 WAL 已持久、现有唯一 writer lease 已证明，且运行时二次核对 connection/private/config/turn、lifecycle、能力与 dispatch revision 后，才能得到一次性 dispatch permit；WAL 已落而二次核对失败时必须保留为 fenced，并持久写入 NotDispatched 或 Unknown 后才能收敛。
- outcome、WAL 未准备证明和 Unknown readback 都必须消费绑定完整命令、WAL/lease/readback 摘要与序号的不可伪造持久收据；Transient、NotDispatched、ProvenAbsent 或 WAL abort 只要求 Actor 重新规划，不返还或自动重试旧授权请求。

账户执行调度器只决定优先级、实例公平性和单 in-flight。请求离开调度器后，仍必须依次经过现有 Owner 校验、
WAL 和精确 writer guard 才能 dispatch；调度结果本身从不构成 mutation authority。当前兼容壳已共用账户级
机器 registry；不同 symbol 或策略即使使用不同 root 也不能并行成为 writer。registry schema 升级或旧 executable
迁移时必须先正常 Stop/handoff，禁止让仍使用旧 scope 键的进程与新版本重叠运行，也不得删除旧 registry 或运行工件。

### 4.5 Reconciler（对账器）

完整签名账户回读只做三件事：

1. 更新账户权威余额、仓位、订单和成交事实；
2. 按 Owner 分配到唯一策略实例；
3. 比较每个策略的 Desired Orders 与交易所实际订单。

Desired Orders 必须来自恢复 checkpoint 或 Actor applied receipt，绑定当前配置摘要、config epoch 与同一账户持仓模式，并按 family、方向、
position side、purpose、数量、价格、reduce-only 和条件/Algo 原生语义摘要全量比较，不能只比 Client ID。首次签名对账后，每一代 notice 都撤销上一 applied authority，只有 Actor 应用 notice 后产生的新 turn 才能提交下一代 Desired。完整签名快照必须逐一证明 regular、
conditional、algo 三个 canonical family 完整或明确不支持，同时声明账户持仓模式，并精确覆盖每个注册 symbol 的
全部 Net 腿或 Long/Short 腿；订单 position side 必须与该模式一致。不支持订单族的同一能力证据同时约束 execution admission、恢复与对账，不能先接受 mutation 再用 unsupported 空快照证明零单。缺 family、缺腿都不能解释为空或零。Stop/Flatten 都需要请求后更新一代的全 family
签名零自有订单；Flatten 还需要同一代完整签名零持仓。Stop 不主动平仓，但残仓期间继续持有 symbol custody，不能释放给新实例。

对账器不得替策略决定价格、层数或开平逻辑。对可证明属于该实例的缺单，策略必须立即进入可恢复重建；不能等周期健康检查。

## 5. 成交热路径

### 5.1 用户流最高优先级

用户流收到完整、可归属成交后，允许在补撤前执行的动作只有：

1. 验证本地 Owner 和订单身份；
2. 追加原始私有证据；
3. 持久化最小成交/状态边界；
4. 检查唯一 writer 与已有 WAL 是否允许提交；
5. reducer 固定语义动作后直接写补撤 WAL；成交滚动不得读取 BBO、公共 socket/journal、风险快照或逐单 REST；
6. 取得唯一 writer guard 并立即 dispatch（发送）；替代单始终使用交易所原生 post-only。

以下动作不得位于补撤前：

- 新发多组 REST 账户查询；
- 风险全量快照；
- 周期健康检查；
- 仓位、余额、订单历史的批量重读；
- 报表、日志聚合、压缩或 UI 推送。

这些工作在补撤之后异步完成。若身份、writer 或 WAL 无法证明，则不猜测交易，转完整签名对账。滚动不以本地盘口作为授权事实。

### 5.2 部分成交不得阻塞其他订单

部分成交按订单身份独立累计，只有累计达到该订单完整数量时才驱动一次网格滚动。

硬约束：

- 一笔尚未完整的部分成交只能挂起它自己的动作；
- 同一签名页或用户流中其他订单的完整成交必须继续按交易所执行序号处理；
- 禁止因较早的部分成交而反复重读、饿死后续完整成交；
- 单独的部分成交等待用户流后续分片或正常周期回读，不得触发账户历史 REST 忙循环；
- 每个完整成交最多生成一次滚动动作，重放只返回 Noop（无动作）。

### 5.3 每代签名回读的订单完整性

每次完整签名回读依次执行：

1. 消费这一代中可证明的完整成交；
2. 提交或恢复对应补撤；
3. 比较策略目标订单键与实际可见订单键；
4. 若仍有无法由成交或在途 WAL 解释的缺单，立即进入 `ResettingGrid` 并按该账户事实重建；
5. 周期健康检查只做审计和报警，不承担首次发现缺单的正确性责任。

### 5.4 急行情穿价恢复

owned maker fill 的滚动价格来自既有 epoch。急涨成交后快速回落时，替代 Buy 可能已高于新 Ask；反向行情同理。
运行时不得把它钳到盘口边缘、改为 taker 或用旧 BBO 写 WAL，因为这会改变 reducer 的网格间距和订单语义。

用户流与签名成交两条入口共用同一最终门：reducer 先固定语义动作和最小 Checkpoint，随后直接进入 WAL 和唯一 writer dispatch，不查询 BBO 或逐单订单详情。Binance `GTX`、Gate `poc`、Bitget `post_only` 是最终防吃单栅栏；快速反转导致穿价时，交易所只能明确拒绝替代单，运行时持久记录精确结果并转更新一代全订单族签名对账。禁止把价格钳到盘口、转换 taker、重新使用旧命令或在拒绝后自动重试。

公共 socket/journal 只在 resident 公共 turn 推进，供初装、整网重建和再中心化使用；它的缺失或过期不阻塞完整成交的滚动 dispatch。`mutation_dispatched`、`private_reconcile_required` 与 `recenter_required` 继续区分已发批次、后续私有对账和 reducer 明确要求的整网重心迁移；BBO 不再产生成交滚动的等待状态。

最小 Checkpoint 与最终公共/WAL 门之间的崩溃也按整批恢复：重启先把 `Prepared` 证明为未 dispatch、把
`Submitted` 围栏为 `Unknown`；若所有替代单身份均不在 WAL，才可恢复原 cancel target 并 recenter。任一替代单
已进入 WAL 时，整个 pending transaction 必须进入 `BlockedUnknown`，禁止单条复活；只有全部 WAL 身份收敛且
更新一代全订单族签名回读重建出实际 owned 集合后，才清除 pending 并继续 reset/running。

重建 place 收到 Accepted 后不得先把本地 4×N 张订单记为 `Running`；若安装窗口继续成交或订单消失，
Checkpoint 与健康报告都必须保持过渡态，直到更新一代签名 open-orders 精确收敛。

Accepted WAL 只证明请求被接纳。新 epoch 必须保持 `Reconciling`，直到严格更新的签名订单集合精确等于
Desired Orders，期间的新成交须先消费并重新计算目标集合。`Running` 的每代既有签名回读都要比较集合漂移，
缺单或多单立即重建；健康报告必须携带签名 generation 和年龄，过期报告不得继续表示当前 `healthy`。

专项回归必须在周期读回为 10 分钟时，仍证明用户流和签名成交两条入口在下一调度 turn 推进恢复；完整成交测试必须令 BBO 与逐单订单详情不可用，仍只产生精确两补一撤。另注入 exchange post-only 明确拒绝、“40 张 place 已 Accepted、首次签名核对只剩 26 张”、撤旧期间继续成交及各持久边界崩溃，证明不会吃单、改价、重试旧命令、提前进入 `Running/healthy` 或等待健康周期。

## 6. 对冲网格语义

### 6.1 初始与滚动订单

- 网格层数来自配置 `grid_count`；
- 每个层级包含 long-open、long-close、short-open、short-close 四种语义订单，但平仓单可受真实库存限制；
- closing wave（平仓批次）先于 opening wave（开仓批次）；
- maker（挂单成交）驱动滚动，taker（吃单成交）只进入库存和对账，不伪造成网格动作；
- 滚动补撤保持固定目标层数；补单会穿价、批次拒绝或订单集合无法证明时，转签名重建。

### 6.2 最小名义价值

每笔开仓挂单的名义价值不得低于交易所当前最小值。Binance 当前策略约束至少为 5 USDT；实际校验以 adapter 的实时规则为准。

若 `价格 × 数量` 不足：

1. 按数量精度向上取整；
2. 再次验证名义价值；
3. 不得向下取整或提交必然拒绝的订单。

平仓数量不得超过签名库存和已承诺平仓量。

显式 `--skip-inventory-replenishment-until-recovered` 是耐久的无市价补仓模式：低库存时仍可按当前签名库存重建，closing 数量必须由库存裁剪且不得超额，两腿 opening 必须各自保持完整。Stage 7 不得在 reducer 已接受该模式后用重复的无条件低库存门拒绝安装；未显式进入该模式时，低于单格名义的任一腿仍必须先走 WAL 绑定的库存补充。

### 6.3 库存恢复后的下一成交重心

库存不足时，状态机进入恢复流程。补足库存后不立刻用旧中心重建，而是：

```text
Deficient（不足）
-> AwaitingInventoryRecovery（等待库存恢复）
-> AwaitingNextOwnedFill（等待下一笔本策略完整 maker 成交）
-> ReanchorPending（新重心待持久化）
-> Rebuilding（重建中）
-> Settled / Deficient（完成或仍不足）
```

触发成交必须属于当前实例、当前有效 epoch、完整、maker 且晚于 armed generation（武装代）。新 anchor（中心价）优先使用成交价；若完整 post-only 网格会穿价，才允许使用持久化的最新 BBO 中点回退，并记录原成交价与穿价证明。

### 6.4 高暴露浮盈减仓

风险判断属于策略慢路径，不得阻塞成交补撤。周期首轮风险 tuple 只由单 in-flight request-only worker 采集；worker 不持有 writer、WAL、checkpoint、evidence 或 mutation client，resident 只用 `try_recv` 接收。采集期间私有 generation 已推进的结果直接丢弃，不得重编号、入证据或触发动作；风险触发后的更新全族复核与 reduce-only 仍由唯一 writer 串行收敛。启用时必须同时满足：

- 当前方向库存至少达到 `inventory_multiple × 单网格数量`；
- 该方向未实现盈利率达到 `profit_threshold`；
- 本次最多减少 `reduce_fraction`；
- 数量按交易所精度向下取整，且不得超过签名库存；
- 只使用 reduce-only 市价单，不先撤正常网格。

默认参考参数为 `3 / 0.05 / 0.30`，风险采集固定每 120 秒一次；采集失败或因私有 generation 推进而丢弃的结果同样等待 120 秒后再采集，禁止异常期间缩短周期。配置、准入摘要和运行 Checkpoint 必须完全一致。失败快照只退避风险 lane，不阻塞成交热路径。Binance 私有 WS 握手后使用 1ms readiness timeout；空闲 socket 不得沿用 10 秒连接超时占用 resident turn，真实成交会立即唤醒读取。

reduce-only 已终态并持久锁存后，同 anchor 网格修复若暂时拿不到新鲜 BBO，必须保留待修复 envelope、禁止准备任何挂单 mutation 并等待公共行情恢复；该可恢复条件不得退出 resident。行情恢复后必须从更新一代完整签名 readback 重新证明订单面，再继续修复。

## 7. 最小实盘保护

保留以下必要保护：

1. 唯一 writer；
2. Owner 身份与同 symbol 唯一策略；
3. WAL 与幂等 Client Order ID；
4. 价格、数量、最小名义价值和 reduce-only 校验；
5. 账户/策略最大仓位、最大单笔和最大挂单数硬上限；
6. 完整签名对账与 Unknown 命令恢复；
7. 明确 Stop（停止）、Pause（暂停）和 Flatten（清仓）语义；
8. 逐交易所 Canary（小流量验证）和单 writer 接管。

以下功能不进入实时补撤路径：全账户风险报表、周期签名快照、健康统计、完整历史分页、复杂准入证明生成、日志归档和 UI 推送。

保护原则是“无法证明时停止新增风险，但仍允许撤单和 reduce-only 降险”，不是遇到任何非关键数据缺失就停止整个账户。

## 8. 身份、持久化与恢复

### 8.1 订单身份

Client Order ID 必须可稳定恢复：

```text
venue + account + instance_id + symbol + epoch + semantic_role + sequence
```

受交易所长度限制时允许编码或哈希，但本地必须保存完整映射，且同一账户内绝不冲突。

### 8.2 文件布局

目标布局：

```text
artifacts/<exchange>/<account>/
├─ account/
│  ├─ writer.json
│  ├─ commands.jsonl
│  ├─ private_evidence.jsonl
│  ├─ account_checkpoint.json
│  └─ reconciliation.json
└─ strategies/<instance_id>/
   ├─ config.toml
   ├─ checkpoint.json
   ├─ control.json
   └─ evidence.jsonl
```

现有 Stage 7 root 在迁移完成前继续使用原文件名。checkpoint、writer、WAL、JSONL、admission、capability 和 handoff 收据均为恢复工件，不得当普通日志删除。Binance 人工授权的外部 Algo 精确清理另写 `external_algo_cleanup.jsonl`：它与 `commands.jsonl` 分离，避免伪造策略 ownership，但同样是受保护的 hash-chain mutation WAL。

通用 facts Journal 在 open 与每次 append 前都必须权威重放；检测到崩溃留下的不完整尾部时，先截断到最后一条完整换行记录并 `fsync`，再重放并核对磁盘 next sequence 与内存一致，才允许追加。完整坏 JSON、空完整行、非 1 起始、断裂序号或外部完整推进均失败关闭，不得修复、跳过或覆盖。Checkpoint 和可重建 Projection 的原子替换必须使用同目录唯一临时文件，先同步文件再 rename，Unix 上 rename 后继续同步父目录；失败时不得将临时内容解释为权威状态。

Stage 7 控制记录不存在时不得推断为 Running。只有同时不存在 checkpoint 与 control 的全新工件根，才可在取得该根唯一 writer guard 后一次性持久化 Running；已有 checkpoint 缺失 control 一律按 Stop 失败关闭。持久 Stopping 不得被普通启动或 Running 记录恢复，只有显式 Reset 且已满足停止清理不变量时才能推进到恢复态。

### 8.3 重启顺序

1. 读取账户/策略 Checkpoint、Actor inbox、私有 applied cursor、WAL 和 Owner 映射，形成绑定五类 journal root/tail/count 及完整投影的不可省略恢复收据；
2. 恢复 config epoch、Paused/Stopping、Stop/Flatten fence、连接代际下限、Owner 路由、未应用 inbox 和所有未决 Unknown，安装失败不得进入 Ready；
3. 恢复 pending 与后序 completed 私有批次，先应用恢复 inbox 并推进可证明的连续 cursor，再由内核递增 connection generation，连接私有流并获取全订单族完整签名账户订单与持仓快照；
4. 用精确 native identity 回读收敛 Accepted / Rejected / Unknown 命令结果；
5. 把实际订单分配给唯一策略实例并比较带 config epoch 的 Desired Orders；
6. 各策略进入 Running、Paused、Rebuilding 或 NeedsAttention；
7. 全部身份、订单与持仓腿确定后才开放新增风险。

实盘 resident 由进程监督器以 `on-failure` 语义托管：异常非零退出可从相同受准入发布和恢复工件重新启动；应用内显式 Stop 完成撤单后正常退出，监督器不得将其重新拉起。监督器不替代唯一 writer、WAL、签名对账或准入校验。

恢复不要求把整个进程做成分布式系统。单机顺序恢复、一个账户一个 writer 足够当前规模。

## 9. 动态新增、停止与参数修改

新增实例：验证交易对未被占用，分配 `instance_id`，创建独立目录，订阅共享行情，执行签名对账后启动。无需重启其他策略。

停止实例：停止生成新开仓，撤销该实例自有订单并确认全订单族签名回读中自有订单为零。Stop 默认不主动平仓；若仍有仓位则保存 Checkpoint 并保留 symbol custody，只有持仓已由外部安全交接/归零或显式 Flatten 后才能释放 symbol。

修改参数：必须原子更新 Registry 与 Actor binding，生成新配置摘要并递增 config epoch；丢弃未入 WAL 的旧配置意图，
存在 in-flight/Unknown 时拒绝改参。影响层数、数量、间距或风险参数时，走该实例的可恢复重建，不能偷偷原地改状态。

## 10. 限频与性能

账户级调度器统一管理交易所权重和频率：

- 私有成交与补撤优先；
- 用户流已经提供的成交、订单变化不立即重复 REST；
- 完整 REST 对账按需或低频运行；
- 多策略共享账户查询，禁止每个策略各自轮询同一接口；
- 遇到 429 或交易所限频信号自动退避，但撤单和 reduce-only 保留最高可用优先级。

性能目标：

- 私有事件路由到正确 Strategy Actor：常态小于 5 ms；
- 完整成交收到后开始补撤 dispatch：常态小于 20 ms，不含交易所网络耗时；
- 同一账户多个策略不能因慢查询互相阻塞；
- 断线后恢复不重复下单、不跨策略撤单；
- 性能数据必须分别记录交易所事件时间、接收时间、WAL 完成时间和 dispatch 时间。

## 11. 迁移顺序

### A. 共享语义与热路径

- 保持三个交易所共用 reducer；
- 用户流成交优先于 REST、风险和健康动作；
- 部分成交不阻塞其他完整成交；
- 每代完整签名回读即时发现缺单并重建。

### B. 单策略 Account Runtime（账户运行时）

- 把现有单交易对 Stage 7 接入账户级 Market Hub、Private Router 和 Execution Lane；
- 行为与当前实盘等价后再迁移持久化布局。

### C. 同账户多交易对

- 引入 Strategy Registry（策略注册表）和 symbol 唯一占用；
- 验证两个网格或多个剥头皮交易对共享连接、独立状态和公平执行。

### D. 逐交易所迁移

顺序固定为 Gate.io -> Bitget -> Binance。每家开始前须明确分工、依赖与验收门槛：

1. 只读 Shadow 只打开既有 WAL、checkpoint、private evidence、control、writer 与公开 journal，在内存中重放；不得创建、续租、消费控制、写 checkpoint/evidence 或发 mutation。private/public evidence 的任一序列、哈希、编码、binding 或尾部完整性失败均阻止 Shadow、Canary、Stop、Flatten 与 handoff；不得通过重编号、截断、替换或以新证据旁路既有工件。唯一例外是人工授权的取证恢复：旧 writer 已停且 canonical root 锁、clean Stop、零 owned/pending、WAL 收敛，以及原文件 SHA、规范选择 root、quarantine 选择 root、全覆盖 root、规范尾序和冲突数全部精确成立时，只允许按固定“已经先持久化的每序列首个物理记录保持规范、一个后续连续 fork 全量 quarantine”规则生成派生日记；public 恢复还必须证明其余记录的完整 binding 与 payload hash。immutable manifest 必须绑定原文件全 SHA/字节/记录数、上述三个选择 root、逐冲突映射、binding、canonical root 及 control/checkpoint/WAL/writer 摘要；原文件永久只读保留，派生初始前缀由 manifest 固定，private 后续边界由 executable handoff 回执继续固定。任何残件、歧义、篡改或原文件后续变化继续失败关闭。恢复不授予 mutation、lease 或接管资格，后续仍须更新一代全订单族签名 readback 与正常 handoff。固定部署只在 Binance 组合开放这两个本地工件恢复命令，Gate/Bitget 组合拒绝；命令不连接交易所、不发订单 mutation。
   无论 Shadow 或接管，WS 直连/HTTP CONNECT 的 TCP 与 TLS 建连均限制为 10 秒；三所公共流及 Binance Stage 7 私有流在握手后单次 socket readiness 等待上限为 1ms，公开流每回合再受帧数和 5ms drain 时限约束，避免空闲 socket 或行情突发阻塞成交热路径、私有 custody 或 Stop。
2. 适配器必须把 `UmOrder`、`UmConditional`、`UmAlgo` 分别交付完整签名页，或以同一 execution profile 的明确不支持证据交付；normal projection 必须等于 `UmOrder`。缺任一族、原始签名页或两视图不一致，Shadow、Canary、Stop、Flatten 和 handoff 全部失败关闭。
   库存补充 round 分配前必须按精确 binding 扫描既有 WAL 的 `hgm_r{round}_{leg}` native identity，并先把 `max(checkpoint round, WAL round) + 1` 持久化；不得仅凭重建后的 checkpoint 从 round 1 重用历史身份。物理数量仍可按当轮规则/BBO 对齐，但任何 malformed owned replenishment identity 都失败关闭，且既有短 ID 保持 Gate.io、Bitget、Binance 的共同长度约束。
3. Canary 仅在上述证据、现有 WAL/checkpoint/private evidence 与唯一 Stage 7 writer-root 全部成立时执行；每次实盘 mutation 之前必须另行获得人工确认，交易所之间绝不并行 mutation。
4. 停旧 writer 后，必须以请求后更新一代的全订单族签名零自有订单、WAL 已收敛和签名 long/short 残仓 custody 生成 immutable handoff receipt。若 Stop 的首个签名页期间 predecessor lease 到期，只能以更新一代的同 scope 签名读回恢复该精确 session；这不是新 writer 选举，且发生在任意取消前。恢复后 writer readback 水位可等于该精确签名页；最终 handoff lease/readback 也仅可与这个已刷新的精确 session 相等，绝不可回退、跨 session 复用或领先。receipt 的 private generation 亦可与该水位相等。只有未恢复的有效 predecessor lease 才拒绝 handoff。receipt 先 fence 精确 predecessor lease；只有 receipt 指定且本机哈希匹配的 successor executable 能激活下一 writer generation。pre-fence 中断 receipt 不得自动重放，仍须人工 custody 复核。
5. 只有新 writer 已激活、successor admission 精确绑定 receipt/executable/configuration，且 Canary 与复核均通过，才能进入下一交易所。Binance legacy bridge 也必须先取得相同 canonical writer-root guard，不能绕过该序列。

Gate、Bitget 当前 profile 只允许常规订单族，条件/Algo 由同一 profile 明确拒绝；Bitget 常规页只接纳 `delegateType=normal`，账户/设置/持仓/订单/成交五面任一失败即作废整轮并由 resident 重做完整 turn，禁止复用其他尝试的成功面拼成一个 private generation。Binance 以 normal 与当前 Algo 的独立 PAPI 已签名页交付完整 live collection。Binance 已退役的 UM `conditional/*` 端点不构成空集证据：adapter 将该 canonical 族显式标为不支持，只在 normal 与当前 Algo 页都经签名读取后放行；不得用同一 Algo payload 填充两个族，也不得把 HTTP 404 当空页。Stage 7 对 Binance 的非空 Algo 行没有 WAL owner，故拒绝常规 writer，必须先完成旧 writer 的签名 custody/清理。风险减仓收据还须以精确的已签名终态订单/成交及 canonical client-to-side 归因生成；Bitget 的 `close_long`、`close_short`、`open_long`、`open_short` 仅在与 `posSide` 和买卖方向共同推导的开平语义一致时接纳，否则失败关闭；handoff 检测到尚有风险减仓待结算时必须保留既有 Bitget 成交历史起点，直到 successor 用精确历史成交完成结算后才允许推进窗口；Gate 仅接受 `t-ord-etp-{l|s}-<16 小写 hex>`，其余标识或 side 不明一律失败关闭。

外部 Binance Algo 清理不是常规恢复的 ownership 推断。只有 operator 明确给出并确认 `clientAlgoId + algoId`，完整当前 Algo 签名页只含该行、regular 页逐单仍由 checkpoint/命令 WAL 证明、命令 WAL 无未决、canonical root 与 `writer.json.lock` 都已独占时，才允许生成短时 permit。执行必须先 fsync 独立 hash-chain WAL 的 Prepared/Submitted，再调用精确 client Algo ID 撤单；HTTP 响应只记录摘要，最终仅由后续完整签名 Algo 空页结算。进程中断后若签名页已空，只补 Settled；若仍为同一唯一 custody，先持久化 StillOpen 才可重新预写，任何新行、字段不全、ID 变化或 regular ownership 失败都继续关闭。

逐阶段职责与门槛固定如下：

| 阶段 | 分工与依赖 | 验收门槛 |
|---|---|---|
| Gate.io | adapter 审计签名常规页/profile；runtime 只读 Shadow；接管链复核 WAL/checkpoint/evidence/lease | Shadow 零写；profile 与 normal 投影一致；旧 writer 的 Stop、全族零单与残仓 custody receipt 齐备后才可申请 Canary 确认 |
| Bitget | 同上，并核对 UTA 签名常规页与 execution profile | 与 Gate 相同；Gate successor admission、Canary 和单 writer 复核通过，才开始本阶段 |
| Binance | adapter 解析 normal 与当前 Algo；已退役 UM conditional 仅可显式不支持，runtime 拒绝未托管 Algo 行 | normal 与 Algo 均有独立签名原文且 normal 投影一致；已退役族无 mutation surface、当前两族零旧单、WAL 收敛和残仓 custody receipt 后才可申请 Canary 确认 |

### E. 控制端

本地 Control API 使用 `venue-control-protocol` schema v2；原生 VenueFlow 与 WebAssembly canvas 共用
`/v2/ui/snapshot`、`/v2/ui/events`、`/v2/control/commands`。策略投影和命令必须携带精确 `LIVE`，两端只调用 API，不读取数据库、WAL 或 artifacts，
不持有凭证，不直连交易所，不直接下单。Stop/Flatten 必须显示并提交精确 mode、account、symbol、instance、config epoch、action 与人工确认；
`apps/venue-control` 提供 transport-neutral schema 重验、幂等 repository、PostgreSQL durable inbox/outbox/claim/terminal receipt，
以及仅限本地的 HTTP/SSE `/v2` 适配层；bounded Node claim/ACK/receipt 路由和 PostgreSQL fencing delivery lease 均绑定精确 instance/config epoch，ACK 只表示节点 inbox 已耐久，Unknown 只能由下一序号只读 reconciliation claim 收敛，重复 receipt 必须幂等且冲突失败关闭。Node 已接入唯一 `OpaqueJournal` storage adapter 与 bounded loopback HTTP polling client；claim、ACK、receipt 每次 await 后都以当前时钟重验 lease/session/epoch，ACK 绑定完整 inbox replay root/sequence/node，过期 outbox 不确认或重放。`venue-storage::ActorAppliedStore` 已提供 anchored journal/checkpoint durability receipt，能拒绝缺件、旧副本与回退；该 receipt 仍只证明调用方声明已耐久，尚未由 runtime 以规范 Actor/Owner 与真实 WAL head 接线，因此生产 Applied 继续返回 `ActorAppliedUnavailable`。LIVE-only Copy worker 可原子持久化冻结资本、语义 job、delivery/receipt、ledger 与崩溃恢复，但数据库 lease、claim 或 Control receipt 均不授予 mutation authority。

## 12. 验收标准

代码提交前必须通过：

```text
cargo fmt --all -- --check
cargo check --all-targets
cargo test
```

还必须覆盖：

- 完整用户流成交零 BBO、零逐单 REST，并在周期 risk worker 未完成时仍进入补撤；
- 较早部分成交不会饿死后续完整成交；
- 签名回读缺单无需等待周期健康检查即进入重建；
- 同账户两个不同 symbol 的策略互不读写状态、互不撤单；
- 同 symbol 第二策略被拒绝；
- Unknown 命令重启后先查事实、不重复下单；
- 点查 404、部分页或普通空页不能证明查无单；只有 adapter 在新代重验完整签名订单族并收集到终端 cursor 才能生成 `ProvenAbsent`。`SignedOrderReadback` 与权威 outcome 不可反序列化，重启必须重验原始签名页；
- WAL 前候选在 Pause、Stop、私有事实或重连后不能取得旧 permit；已写 WAL 的候选只能由持久 NotDispatched/Unknown/outcome 收敛；
- 连接断开只暂停受影响账户；
- Stop 只撤目标实例订单且默认不主动平仓；残仓时保留 symbol custody；
- 开仓订单全部满足实时最小名义价值；
- 三个交易所 adapter 通过相同 runtime/reducer 契约测试。
- `scripts/verify_gateway_candidate_contract.ps1` 通过六个隔离 LIVE binary、非生产模式前置拒绝和缺证据零工件探针；矩阵中的 `not_reached` 与 `writer_enabled=false` 必须原样保留，不能当作 Canary 或实盘准入。
- 取证恢复证明原文件不改写、规范序列不重编号、quarantine 无遗漏/重叠、派生前缀 append-only；源文件、manifest、派生前缀、binding/root 或 crash residue 任一变化使全部 Stage 7 入口失败关闭，Shadow 仍为零写。

账户内核自身还必须覆盖：跨策略族的同 symbol 注册失败；已持久私有 Order/Fill 只能按 family + Client Order ID 或
family + venue order ID 精确路由；多事实证据整批投递、durable Actor inbox 与 applied cursor 原子推进，双身份必须完整一致；
身份冲突时零投递并账户级失败关闭；私有及连续行情有界无损，只有显式状态行情可合并，symbol 换代清空全部旧行情；
私有 burst 与执行优先级均有让行；Unknown 重启恢复后只封锁所属实例新增风险而不阻塞兄弟实例、撤单或 reduce-only，
且证明绑定 command/native family/新连接代；恢复 manifest 漏任一 UNKNOWN/Owner route/批次或 journal 边界不一致时拒绝启动；跨 Actor 乱序 applied 的后序 completed 批次重启后仍能在前序完成时推进 cursor；重连和改参不会复活未入 WAL 的旧开仓；Actor 未 applied 不得 Running；
Pause 不被重连清除，NeedsAttention 不被普通控制绕过；Stop 只凭请求后新代全订单族签名零自有订单完成撤单，
残仓 custody 不释放 symbol；Flatten 还必须同代完整覆盖并证明所有账户模式持仓腿为零。

实盘接管必须满足：旧 writer 已停止、旧自有订单已签名确认为零、仓位已记录、新可执行文件与配置摘要明确、Canary 成功且只有一个 writer。

## 13. 明确不做

当前不实现：

- 同一账户同一交易对多策略；
- 多用户、多租户和付费权限系统；
- 跨机器分布式撮合或复杂服务网格；
- 策略插件市场和公共 SDK；
- 在网格/账户 Runtime 任务内实现 Web 面板或 Windows UI；独立 VenueFlow 任务可开发无凭证公共行情和 Control 客户端，
  但不得进入成交热路径或持有账户、私流、writer、WAL 与 mutation；
- 为未来功能预建未被当前任务使用的模块；
- 让 `bak/` 参与构建、运行或持久化。

只有当单机账户进程的 CPU、网络或交易所连接上限被实测证明不足时，才讨论拆分独立网关。当前规模下不提前增加这类复杂度。

内容寻址可执行文件接管链允许长期累积发布；真实循环按 admission 摘要去重检测。深度保护只防损坏工件耗尽资源，不得把正常的几十次升级误报为循环。
