# 多策略、多交易所账户运行时与对冲网格开发规范

更新：2026-09-01

## 1. 文档职责

本文是多策略、多交易所实盘和对冲网格的唯一长期开发契约，说明当前采用的架构、边界、成交热路径、恢复规则、迁移顺序与验收要求。

当前实施范围以三目标迁移契约为准：先 Binance 交易终端和真实跟单，再完成 Gate.io、Bitget 接管；Bybit、OKX、Hyperliquid 与 Scalping 暂缓。相关段落保留已有安全约束，不代表恢复其开发。Web 可以提交手动交易语义，仍由 Node 的唯一账户执行链下单。目录整理不变更风险、WAL 或已有仓位。

[`ARCHITECTURE.md`](ARCHITECTURE.md) 说明合并跟单、六交易所网关、指标和桌面 UI 后的当前 workspace。六所是网关目标覆盖，
网关只接受精确 `LIVE` 并只使用生产 endpoint；这不自动扩大当前 Stage 7 网格的三所实盘准入，新增交易所仍必须独立完成策略、恢复和接管验收。
本文现有 Shadow/verify 是策略与工件证据流程，不是第三种网关运行模式。

Stage 7 直接重构到统一账户执行链、六所逐家接管和响应式 Web 迁移的阶段统一查
[`UNIFIED_GATEWAY_WEB_MIGRATION.md`](UNIFIED_GATEWAY_WEB_MIGRATION.md)。本文继续拥有成交热路径、安全不变量和实盘接管最终解释权。

该迁移任务已获得持续实盘授权：AI 可在现有账户 binding 和能力约束内选择交易对，对单账户累计不超过 10U 的名义风险执行
逐所真实 Canary，并自行使用 CLI 的 `--confirm-live` 等技术确认参数，无需逐次请求人工确认。初期 Bybit、OKX
既有 DOGE binding 使用 `DOGE/USDT`，Hyperliquid 永续使用其实际报价资产 `DOGE/USDC`；不得把 USDC 数值直接当作 USDT。
授权不包含提款、转账、修改账户安全/杠杆/保证金设置、创建凭证、突破 10U、
Unknown 时增险或双 writer；完整范围和人工协助延后规则以迁移文档第 2.1–2.2 节为准。

`CODEMAP.md` 只负责定位代码。事故时间线、已完成阶段、临时发布号和一次性排障记录不得继续写入长期文档。

离线 fixture、mock 与数据库集成测试保留，但不得成为交易所运行模式或连接真实交易所。

当前选择的架构不是功能最多的方案，而是最适合“主要由一个人使用、需要多个交易所和同账户多个交易对、不要反复重构”的方案：

- 每个交易所账户一个常驻进程；
- 账户进程内运行多个 Strategy Actor（策略运行单元）；
- 一个规范交易对只允许一个策略实例拥有；
- 行情和账户私有连接共享，策略状态、订单归属和持久化隔离；
- 所有下单、撤单和市价减仓经过账户内唯一 Execution Lane（执行通道）；
- Web 面板与 Windows UI 已有实现，只接控制与查询接口，不进入交易热路径；原生端无凭证公共行情例外见架构文档。

Stage 7 网格实现仅保留为工件兼容读取和行为证据源；根 package 的三家 `hedged-grid-*` 生产 binary、feature、部署 re-export 与发布脚本已移除。新的生产编排只能经六个固定 Node binary、统一 Account Runtime、Execution Lane 和 AccountMutationHost，绝不保留第二套 writer/WAL/authority 链。

账户级纯内核固定在 `crates/venue-runtime/src/account/`，策略顺序邮箱固定在 `crates/venue-runtime/src/strategy/`，账户执行调度固定在 `crates/venue-runtime/src/account_lane.rs`。配置中的 `trading_account_id` 是本系统生成并稳定保存的内部账户 ID，不要求交易所提供 UUID；同一真实账户跨 symbol/策略复用。

当前 10–20 用户阶段采用最小实盘安全模型：

1. 每个 `(exchange, trading_account_id)` 只有一个常驻账户进程、一个进程锁和一个串行 Execution Lane；当前不设计跨机器 writer 选举。
2. 所有 mutation 共用一本 `commands.jsonl`，状态只保留 `Prepared / Submitted / Accepted / Rejected / Unknown`。发送前 fsync `Prepared`，dispatch 前持久化 `Submitted`，再写请求结果；连接中断或超时写 `Unknown`。
3. Owner 只是同一 WAL 记录中的 `strategy_id/user_id` 和订单 family/native identity，不建立独立 authority、journal、root、seal 或 receipt。
4. `Unknown` 是恢复状态而不是权限体系：账户暂停新增风险，以原 Client Order ID、交易所订单 ID、订单族、成交和当前持仓的签名查询收敛；未收敛前禁止自动重发，允许撤单和 reduce-only。ACK 外层与订单行矛盾、身份或时间校验失败但订单行显示成功时也必须记为 `Unknown`，不得误记 `Rejected` 后释放风险。
5. 启动准入只检查账户锁、配置/凭证、实时规则、当前 checkpoint、未决 WAL 和更新的签名订单/仓位/成交事实。检查通过后由同一账户进程直接持有 writer；当前实现由同一 Host 在 Submitted 后签发一次性 dispatch permit，Actor applied 只证明本地耐久应用；不得在外部另拼 capability、admission、五类 root 或第二套 writer 权限链。

现有 Stage 7 的 lease、canonical root、hash-chain receipt、handoff 和恢复 manifest 只作为迁移兼容实现，不得继续扩展，也不得成为 Bybit、OKX、Hyperliquid 接入的前置模板。迁移时优先收敛到上述单账户模型；旧 writer 与新 writer 不得重叠，旧 WAL 中的 Unknown 必须先对账。历史预检或 Canary 只证明其对应版本、账户与观测时点，不能作为当前版本已经完成生产接管的声明；后续 mutation 仍必须经本文的账户 host，并重新核验实时安全条件。

Hyperliquid 使用 `HYPERLIQUID_ACCOUNT_ADDRESS`、`HYPERLIQUID_API_WALLET_ADDRESS`、`HYPERLIQUID_API_WALLET_PRIVATE_KEY` 三项必填及 `HYPERLIQUID_VAULT_ADDRESS` 可选；API Wallet 地址必须由私钥推导一致。其 USDC 风险估值经无凭证、只读的公开稳定币基准读取保守 USDC/USD bid 与 USDT/USD ask 后换算，绝不按 1:1 视为 USDT；基准异常、格式变更或读取失败均冻结新增风险。该窄估值读取不链接任何其他六所 adapter。Stage 7 成交热路径不得遍历历史 WAL；启动时只为未决命令、Client Order ID、Owner 字段和交易所订单 ID 建内存索引。

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

1. 六个交易所各账户独立进程，不能共用一个跨交易所进程；每个固定 Node binary 只链接一个 adapter。
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
└─ Control API 边界（本地 HTTP/SSE；Copy inbox 只接收耐久语义输入并由 Actor Applied 再验证）
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
- `venue-runtime/account_lane`：账户级公平调度、单 in-flight 与 Unknown fence，不重复实现 mutation authority。
- `venue-execution`：单一命令 WAL、账户进程锁、幂等 Client Order ID 和精确订单状态转换；旧 lease/canonical-root/handoff 只保留迁移兼容，不新增调用点。
- `risk`：少量账户级硬上限与策略自身风险逻辑。

同一策略族跨交易所必须复用一个 reducer。交易所差异只进入 adapter、能力证据、Execution Profile（执行配置）或 Deployment Binding（部署绑定）。

## 4. 共享组件职责

### 4.1 Market Hub（行情分发器）

- 每个账户进程对同一交易所尽量共享公共连接；
- 维护持续更新的 BBO（最优买卖价）和必要深度；
- 每个事件带交易所时间、接收时间和单调序号；
- 公共源按真实协议区分完整 WS 图像与 snapshot/delta 桥：OKX 的增量前序必须命中上一 `seqId`，Bybit 在 adapter 内按单调 `u/seq` 重建整簿再输出 Snapshot，Hyperliquid 以完整 L2 的源时间作为快照水位，均不得虚构原生连续编号。完整图像由 Node 的显式只读 `FullSnapshotBook` view 提供就绪语义，不把普通 REST 快照或一次性 BBO 提升为策略输入；FeatureSource capture cursor 按实例隔离，其他 symbol 的事件不能制造假断层。
- 公共成交身份与连续游标分离：`PublicTradeId` 保留数值或最多 256 字节的无空白 opaque 原生身份；`PublicTradeOrdering` 显式选择 NativeAggregateId、Unsequenced 或 Session。旧记录缺少 ordering 时只视为 Unsequenced，不能从字段名推断连续性。Bybit UUID、Bitget execution id（非 L correlation）、Gate/OKX 数值 id、Hyperliquid `(time,coin,tid)` 均不得伪装成逐笔加一序号。
- 非原生连续成交在 Node 同代盘口就绪后，由单实例有界去重窗口赋予 Session cursor；同身份同事实重放不推进，冲突、时间回退、无法证明为新事实的已淘汰重放失败关闭。新盘口同步代重建窗口，Bitget 同一物理连接的同步代只由其 book sequencer 推进。FeatureSource/Runtime 拒绝未排序成交，同代原生/session 混用及 cursor 断层失败关闭；成交 freshness 使用交易所成交/发布时间中较早者，盘口使用自身 exchange time，不使用接收时间。就绪盘口缺少源时间则围栏，未来水位不得 Ready；盘口断层在进入 Runtime/Actor 之前拒绝，同代快照不能解除围栏。
- `pulse-orderflow-v1-session-observed` 的 Ready 只证明本机无损接收窗口满足盘口、64 笔独立成交和 21 根连续闭合 bar，不证明交易所全市场成交完整，也不授予增险权限。单 receiver 最多排队 1024 条规范事实并逐条轮转，Node 成交去重最多 4096 身份；raw batch 不按每笔成交复制。
- 闭合 bar 必须具备协议确认：Bybit `confirm`、OKX `confirm=1`、Gate `w=true`；形成窗口、后续行情时间越界、缺确认字段均不能使缓存的 forming bar 自动成为完整闭合事实。Bitget/Hyperliquid 当前仍缺权威闭合读取，保持 Warmup，不把只有盘口/成交的接线描述为完整策略。
- Control poll/重试只推进自己的 deadline，退避期间仍运行同一账户的私有/公共 pump；公共空闲等待最多 5ms，有积压则继续有界排空。该值不是端到端延迟保证：同步签名读取、HTTP、持久化及 reducer 耗时仍须单独测量和消除瓶颈。
- 慢策略不能阻塞行情读取；仅 Snapshot、Ticker、MarkFunding 可保留最新值，Delta、Trade、Bar 必须进入有界无损队列；
- 私有或连续行情邮箱满载必须显式失败并封锁相关新增风险，不能静默丢事件；BBO 新鲜度只用连接代、交易所事件时间及同事件族序号，不用本机接收时间，也不比较不同事件族的序号；任一事件族进入新 symbol generation 时必须清空该 symbol 全部旧 watermark、BBO 和 Actor 行情队列。BBO 只参与初装、整网重建及显式再中心化，不参与成交滚动；这些非滚动 mutation turn 的完整签名私有 readback、风险或规则核验若可能超过 BBO 新鲜窗口，必须在任意 WAL/mutation 前再次有界排空并持久化期间已到达的公共帧，再按新的当前时钟复核 BBO；closing wave 的签名确认也可能跨越该窗口，因此在 opening wave 尚未 dispatch 前必须再次持久排空并重采 BBO，只有全量 opening 仍为 post-only 才可发出；刷新只更新数据，不授予 writer、risk 或 dispatch authority。
- WebSocket 一次建连的 DNS、全部解析地址、TCP、代理 CONNECT、TLS 与 upgrade 共用 10 秒总期限，禁止每个地址重新获得完整超时；失败后的公共、私有及启动连接按有上限指数退避，并用账户/进程/失败代际错峰，禁止固定间隔同步重连风暴。

### 4.2 Private Router（私有事件路由器）

- 每个账户共享一套用户流；
- 原始事件先完成领域校验，再把规范订单、成交与连续游标写入当前 checkpoint/facts；原始 wire payload 仅用于短期排障，不是永久恢复依赖；
- 一条事件产生多条事实时整批校验、整批路由；失败时不推进该连接游标。当前规模不建立独立 Actor inbox/root/receipt 证明链；
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
- 账户进程锁由 `venue-execution` 按 `(exchange, trading_account_id)` 建键，不含 symbol、Owner 或策略；Stage 7、账户 runtime、Canary 和恢复必须共用该锁，禁止再叠加 symbol/Owner 级 writer lease；
- 负责 Owner 校验、WAL、Client Order ID、基础数量/价格精度、账户硬上限和交易所限频；创建订单在入队时原子保留 `(family, client id, Owner)`，Cancel 必须精确命中同 Owner、同 family；
- 高优先级：成交后的补撤、止损、减仓、紧急撤单；
- 中优先级：策略正常挂撤；
- 低优先级：周期查询、统计和报表；
- 同一 Client Order ID 重试必须幂等，Unknown（结果未知）命令先查事实，不能直接重发。
- 队列必须有界；Critical 连续服务也必须周期让行 FillRepair/Normal，不能让兄弟实例或普通工作永久饥饿。
- 命令只有在账户进程锁仍持有、风险/生命周期仍允许且 `Prepared` 已 fsync 后才能发送；发送前校验失败写 `Rejected`，发送后结果不确定写 `Unknown`。复用同一 Host 现有的 Submitted 后 dispatch permit，不另增加独立 permit/receipt 权限层。
- outcome 与 Unknown 回读直接按同一本 WAL 的 command、Client Order ID、family 和交易所订单 ID 核对；Transient 或 Unknown 只触发冻结与对账，不自动重试旧命令。
- 限价政策是命令身份的一部分：`LimitTimeInForce::PostOnly/Gtc` 必须随 wire、签名事实、Unknown 回读、Owner 和 Desired Orders 精确比较。历史命令缺字段只按原 PostOnly 契约恢复且保持 WAL 字节；签名事实缺字段保持未知，不能默认只挂单。Grid 与 BBO 自动归一化仍为 PostOnly，手动 Gtc 不得改变策略防吃单边界。
- 手动选价归一化必须保持原始价格、政策、Owner 和方向；数量只能按报价预算、签名平仓上限及实时 native lot 向下裁剪。适配器不得改用 BBO，归一化不写 WAL 或发送请求，物理准入仍经同一账户链。订单创建时间缺失保持未知，不能用更新或接收时间推导“最近挂单”。
- 签名订单的 quantity 表示原始委托量，filled_quantity 单独保留；风险估值仍使用剩余未成交量。归属校验必须同时匹配 adapter 的精确 client ID 编码、native ID 和完整命令语义，不能仅凭交易所订单 ID 认领。Unknown 可恢复归属，但不得因此改成 Accepted 或自动重投。

账户执行调度器负责优先级、实例公平性和单 in-flight；请求离开调度器后只经过风险复核、同一 WAL 和账户进程锁。不同 symbol 或策略共用同一账户 writer。版本迁移时先 Stop 旧进程、确认锁已释放并对账未决命令，再启动新版本；当前阶段不要求内容寻址 executable、跨主机 handoff receipt 或多代 lease 协议。

### 4.5 Reconciler（对账器）

完整签名账户回读只做三件事：

1. 更新账户权威余额、仓位、订单和成交事实；
2. 按 Owner 分配到唯一策略实例；
3. 比较每个策略的 Desired Orders 与交易所实际订单。

账户快照必须保留每种资产的余额，缺失可用金额或订单累计成交量保持未知，不能填零。账户级采集不得只过滤到启动交易对；
Net 模式仓位以有符号数量表达，完整列表中不存在的注册交易对才能证明零腿。成交水位与当前 checkpoint 原子提交，恢复读取必须
携带此前水位并重叠去重；交易所历史保留窗口已丢失恢复锚时失败关闭，不得用终端空页伪造连续性。所有非 USDT 报价风险需携带
新鲜、同一观测代的真实换算率；汇总风险与每个 WAL 保留额均按各自报价资产换算，不做稳定币 1:1 假设。
汇率有效期保留来源时间，不能以 HTTP 接收时间重新计时；多请求采集按最早账户事实复核新鲜度，后续分页或 FX 响应不得刷新先前账户事实的有效期。

Net reduce 的 Accepted 不能永久占用已完成减仓量，也不能仅因订单消失而释放：须由同一本 WAL 的 native identity、精确完整成交合量、更新完整仓位和零开放原单证明结算，并与原签名 checkpoint 原子持久化。持久化失败不改变内存预留；Unknown 始终保留最坏情况预留。该 Net 专用判定不得拒绝正常 Hedge 账户快照。

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
2. 持久化规范成交/订单事实与连接游标；原始 wire payload 只在显式诊断时短期记录；
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

Grid bridge 对同一已接受 native order 的部分成交必须在策略 checkpoint 按精确 native id、不可变 fill id 和累计数量持久化；累计恰好等于原委托量时才一次调用 reducer，并在同一 checkpoint 退休已成交 route。partial 的重复 id 内容不一致、数量超原单、价格偏离原 PostOnly 委托或 checkpoint 路由无法完整反序列化都失败关闭。复杂 Grid key 以已校验 route 记录列表持久化，不以 JSON map key 或价格/方向推导；只兼容旧空 route checkpoint，非空旧对象拒绝恢复。

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

显式 Node Grid 配置 `skip_inventory_replenishment_until_recovered`（部署入口可映射为
`--skip-inventory-replenishment-until-recovered`）是耐久的无市价补仓模式：低库存时仍可按当前签名库存重建，closing 数量必须由库存裁剪且不得超额，两腿 opening 必须各自保持完整。该配置只在缺少本账户 Actor checkpoint 的首次 bootstrap 生效；恢复 checkpoint 或首次尝试后的重启不会再由新 BBO 重建 epoch。Stage 7 不得在 reducer 已接受该模式后用重复的无条件低库存门拒绝安装；未显式进入该模式时，低于单格名义的任一腿仍必须先走 WAL 绑定的库存补充。

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
2. WAL 内 Owner 字段与同 symbol 唯一策略；
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

### 8.2 文件布局与容量

本地根固定为 `G:\Venue\artifacts`，并保持 Git ignore。目标布局：

```text
G:\Venue\artifacts\<exchange>\LIVE\<trading_account_id>\
├─ writer.lock
├─ commands.jsonl
├─ checkpoint.json
├─ facts.jsonl
├─ reconciliation.json
├─ strategies\<instance_id>\checkpoint.json
└─ raw-private\                 # 可选、短期诊断数据
```

恢复所需的活跃集合只有：账户锁、当前 checkpoint、当前/未决命令 WAL、成交游标、未决 Unknown 涉及的订单/成交事实。它们在 Unknown 收敛和更新一代签名对账完成前不得删除。Owner、admission、capability、handoff 不再各自产生日记；需要的 Owner 直接在命令记录中保存。

空间策略固定为：

- 所有追加文件达到 5 MiB 即关闭并轮转，任何单文件硬上限为 10 MiB；新实现不得创建无上限单体 JSONL；
- 原始私流默认不落盘；显式诊断时只保留两个 5 MiB 滚动段，总量不超过 10 MiB。它不是恢复依据，旧段可直接覆盖；
- `commands` 与 `facts` 使用 5 MiB 小分段。已终态且被更新一代签名对账覆盖的段可压缩或删除；含未决 Unknown 的段保持只读，后续写入新段，因此单文件仍不增长；
- checkpoint 使用同目录临时文件、fsync、rename 原子替换，写入后不得超过 5 MiB；超过说明状态设计错误并冻结新增风险；
- 整个 `G:\Venue\artifacts` 默认预算为 256 MiB；达到 200 MiB 告警并清理已覆盖历史段，达到 240 MiB 冻结新增风险并要求人工归档，保留余量只供撤单、reduce-only、Unknown 对账和 checkpoint；
- 清理只针对已关闭、已对账覆盖且不被 Unknown 引用的历史段。迁移旧工件前先生成清单并确认当前 writer 已停止，禁止直接递归删除整个目录。

现有 Stage 7 文件在完成迁移前保持只读兼容；新路径不得继续产生无限增长的 `private_evidence.jsonl` 或复制相同事实到多本 journal。

旧三所切换时，`LegacyV1WriterPredecessor` 必须同时绑定目标规范 `trading_account_id`；新 Host 同时持有精确旧锁
和新账户 writer 锁之后，才可一次性导入冻结且全部终态的
`commands.jsonl`：先按原 journal 的序号、哈希和状态转换验证，再复制为新 root 中不超过 5 MiB 的历史段，
并持久化来源绝对路径、字节数和 SHA-256 及每个导入段的摘要。源文件始终只读；已有导入标记必须精确匹配，
后续新 WAL 段可追加轮转，但导入前缀不可变；任何中断、篡改、
空文件或 `Prepared/Submitted/Unknown` 均失败关闭，不清理后重试。此兼容导入只保留既有命令/Owner/路由事实，
不从旧 checkpoint 推断 Grid 路由、库存或 Actor Applied，后者缺失时仍不得启动 Grid 或新增风险。前驱记录使用
不可变 v2 handoff，除旧 scope/root 外还必须哈希绑定旧 WAL 的策略实例与 run；两者任一不一致的 handoff 均拒绝。
既有 v1 handoff 只能在旧锁持有、冻结 WAL 的全部物理 Owner 唯一且由导入核验精确推导时兼容，不能信任配置自由填写。
导入时每一条物理 mutation 的 Owner 都必须匹配这份冻结身份，混合 Owner WAL 不复制。它们仅和
状态为 `New`/`PartiallyFilled`、剩余量为正的签名订单的 native/client/full-shape 共同组成 cancellation-only custody
候选，不可把旧 Owner 重写为新 UUID，也不可成为 Place/Reduce 的授权。
候选只可在 Host 已把该份签名快照安装为 Runtime 当前 private generation 后，由当前策略的耐久 Actor turn
提交同一 lane 的精确 Cancel；Host 在写入 `Prepared` 前再次从该已持久快照逐字段比对 route。任何较新的签名
generation 在 dispatch 前到达，都必须拒绝该 Prepared，而不是用旧 Owner、旧 route 或新的快照自动重组/重投。

### 8.3 重启顺序

1. 取得 `(exchange, trading_account_id)` 账户进程锁；失败则退出，不参与选举或抢占；
2. 读取当前 checkpoint、命令 WAL、成交游标和策略配置，恢复全部未决 `Prepared/Submitted/Unknown`；
3. 连接私有流并签名查询当前订单、持仓和必要成交历史；用 Client Order ID、交易所订单 ID 与 family 收敛未决命令；
4. 把实际订单按 WAL 中的 Owner 字段分配到策略，比较 Desired Orders；无法归属的订单进入 `NeedsAttention`，不开放新增风险；
5. 记录新的 reconciliation 水位；无未决 Unknown、风险与规则校验通过后进入 Running，否则保持 Paused/NeedsAttention；
6. 恢复全程不自动重发旧命令，撤单和 reduce-only 必须继续经过同一本 WAL。

实盘 resident 由进程监督器以 `on-failure` 语义托管：异常非零退出可从相同受准入发布和恢复工件重新启动；应用内显式 Stop 完成撤单后正常退出，监督器不得将其重新拉起。监督器不替代唯一 writer、WAL、签名对账或准入校验。

Ubuntu Node 发布默认先在本机通过 `scripts/Build-VenueUbuntu.ps1` 交叉编译；独立版本化产物位于 `G:\Build\Venue\ubuntu\releases`，复用现有 slot-2 构建锁。弱集成服务器只接收产物并核验哈希、架构和动态库，不承担日常 Cargo 编译。编译/上传不授权启动 writer，也不替代原 WAL、旧 writer 停止及签名接管证据；构建入口与资源边界见 `docs/DEVELOPMENT.md`。

恢复不要求把整个进程做成分布式系统。单机顺序恢复、一个账户一个 writer 足够当前规模。

Actor Applied 早于其后物理命令的 WAL 是正常顺序。重启安装 Actor 时，应由同一账户 Host 校验其 WAL head 是当前已恢复 WAL 的真实历史前缀，
不能要求两者尾部完全相等，也不能只比较序号大小而放过伪前缀。该核验属于冷启动，不得让逐成交 dispatch 重新扫描或序列化历史 WAL。
当前 WAL head 使用带版本的 v2 增量摘要，首个 Prepared 的序号、命令摘要和上一摘要共同推进，状态转换仍落在同一本 WAL。
无版本字段的既有 head 按 v1 精确验证，冷启动支持 v1/v2 历史前缀；不得重写旧命令或要求逐成交重新序列化全部历史。

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

### A. 冻结并提取共享行为

- 保持三个交易所共用 reducer；
- 用户流成交优先于 REST、风险和健康动作；
- 部分成交不阻塞其他完整成交；
- 每代完整签名回读即时发现缺单并重建。
- 将上述行为固化为不依赖 Stage 7 authority/root 的 runtime contract test；迁移期不得改变策略语义迎合新外壳。

### B. 统一 Account Runtime（账户运行时）

- 组合 Strategy Registry、Market Hub、Private Router、Execution Lane、AccountMutationHost 和 Reconciler；
- Grid、Scalping、Copy 只输出共同的规范账户意图；
- 账户进程锁、命令 WAL、Owner、风险和 Unknown 只实现一次；
- 先保持现有 adapter transport 和网格 reducer 行为等价，再迁移持久化布局或异步 transport。

Scalping Node 配置必须显式给出 `scalping.parameter_release_id/owner_scope/risk_budget`，按纯策略参数生成同一 FeatureSource profile，并从既有 Actor checkpoint 恢复真实引擎。只有产出 frame 才进行 evaluate 和耐久 checkpoint；原手造 Scalping candidate 到 Host 的入口已移除。签名私流安全、成交确认和服务端保护尚未闭合时，引擎禁止自动入场，原本 Running 的对外投影显示 NeedsAttention；Paused/Stopping 不被覆盖。只有盘口、缺少 trades/bars 的接收路径不等于完整策略输入。

### C. 同账户多交易对

- 引入 Strategy Registry（策略注册表）和 symbol 唯一占用；
- 验证两个网格或多个剥头皮交易对共享连接、独立状态和公平执行。
- Copy Follower 与 Grid/Scalping 使用相同 Owner 冲突规则，同一账户同一 symbol 只能有一个实例。

### D. 逐交易所迁移

本次旧三所按 Binance、Gate.io、Bitget 逐家接管；其余三所后续再安排。新增实盘验收 mutation 由同一协调者串行调度，不同真实账户可以分别运行其唯一账户进程；不得由多个子任务同时操作：

1. 停止旧 writer，确认账户进程锁已释放；读取当前 checkpoint 和命令 WAL，所有 `Submitted/Unknown` 先完成签名对账。
2. 只读获取账户模式、实时规则、余额、全部持仓腿、支持的订单族、开放订单和必要成交历史；不支持的订单族由 adapter 明确拒绝，不要求伪造空页或永久保存原始报文。
3. 验证 DOGE 账户累计名义仓位硬上限 10U、数量步长和最小下单量；已有未撤入场命令或非零持仓时不得继续增险。OKX 必须按 `ctVal × ctMult × contracts` 换算基础数量，并按 `lotSz/minSz` 约束可执行张数。
4. 取得唯一账户进程锁，并在旧 root 仍冻结时按上述只读兼容规则导入可验证的终态 WAL；随后写入最小 checkpoint
   并启动同一本 WAL。发送前落 `Prepared`，结果不确定落 `Unknown`。
5. 每轮 Canary 的单笔和账户累计名义风险均不得超过 10U，并立即签名回读订单、成交和持仓。AI 可使用持续授权直接执行，
   无需逐次请求人工确认；同一账户可重复多轮，但前一轮必须终态、无 Unknown/未撤增险订单且签名仓位归零后才能再次增险。
   已有持仓账户仍可做只读、撤单、reduce-only、Stop/Flatten。该所全部必需轮次完成并停止后才推进下一家；失败则释放 writer、
   保持 Paused，把确需外部权限的事项留到其他可执行任务完成后汇总。
6. 该所全部生产调用进入统一账户链且旧调用点清零后，才允许删除其 Stage 7/旁路 Canary writer；读取旧工件所需兼容代码继续保留并测试。

旧 Stage 7 的 evidence recovery、immutable manifest、lease generation 和 executable handoff 仅服务已有旧 root 的兼容恢复，不复制到新交易所。迁移旧 root 时只需证明旧进程停止、账户锁释放、未决 WAL 已收敛及当前订单/仓位已签名读回；新版本随后取得同一账户锁。

### E. 控制端

本地 Control API 使用 `venue-control-protocol` schema v2；原生 VenueFlow 与 WebAssembly canvas 共用
`/v2/ui/snapshot`、`/v2/ui/events`、`/v2/control/commands`。策略投影和命令必须携带精确 `LIVE`，两端只调用 API，不读取数据库、WAL 或 artifacts，
不持有账户私流/交易凭证或物理订单客户端；原生端可用无凭证公共行情，账户表单的短暂输入与加密提交见 `ACCOUNT_MANAGEMENT.md`。Stop/Flatten 必须显示并提交精确 mode、account、symbol、instance、config epoch、action 与人工确认；
`apps/venue-control` 提供幂等命令和本地 HTTP/SSE `/v2`。Control 只提交语义命令及 `instance/config epoch`，Node 接收后写入自身命令 WAL；Control 的数据库状态、delivery ACK 或 receipt 都不授予交易权限。当前阶段只需“命令已接收/已拒绝/已完成”三类终态，不建立第二套 Actor-applied root、跨层 durability receipt 或 delivery lease 证明链。重复命令按稳定 command ID 幂等处理，冲突输入拒绝。

VenueFlow 的 Trade Dock 同样只提交语义 `TradeIntent`：按钮和热键必须先统一为 `TradingAction`；开平仓固定 LIMIT/GTC 并要求选价，平多/平空显式 reduce-only，UI 的 `min(quote preset / price, projected position)` 只作为数量上限，账户 Node 必须再按更新的签名仓位与 adapter 实时规则向下裁剪。撤当前未带显式订单 ID 时只能在同一 `(account, symbol)` 选择最近 Working order，撤全部也只能作用于该 scope。Control 接收不等于物理执行；生产 Actor durable-applied authority、风险、同一 WAL 和账户唯一 writer 全部满足前不得产生 mutation。
统一 Node 的 `production_resident/manual.rs` 已接入非 Copy Actor 的显式限价及自有手动挂单撤单；稳定 request ID、原始计划和数量上限存于同一 Actor replay 的可选 manual 字段，后续策略 checkpoint 必须保留该字段。恢复验证原绑定和配置，Reconcile 只读原 WAL 与签名事实，不重投；只有精确挂单或完整成交与仓位变化得到签名证明才返回完成。Copy 绑定及会影响 Grid desired 的撤单继续明确 Rejected，不能把部分 scope 撤单称为撤全部。此桥接不代表自动策略协同、生产接管或所有账户能力已验收，也不得恢复旧 writer 旁路。

响应式 `apps/venue-web` 通过同源 BFF 访问相同 Control 契约；浏览器不直连 loopback Control，不读取数据库、WAL、artifacts 或
交易所 secret。`G:\kol\apps\web` 只提供响应式布局、恢复失败关闭和交互测试参考，全部数据 DTO 和命令按 schema v2 重写。

### F. Copy 物理闭环

Copy delivery 的 semantic Applied 只能表示 Actor 已耐久接收目标。Node 必须继续生成 follower 账户意图，经风险、Owner、同一 WAL、
唯一 writer 和 adapter dispatch，再以更新签名私有事实写回执行状态、ledger 和 drift。跨零反向先 reduce-only 到零并等待新事实；
任一 Unknown 禁止重投旧 child。具体阶段和 Web 页面依赖查 `UNIFIED_GATEWAY_WEB_MIGRATION.md`。
Copy Install 的领取租期精确截断于 immutable job 截止时间，不改变原执行有效期；已领取任务过期后仍可获得完整只读对账窗口。ReconcileOnly 只能用同一 delivery、原 WAL/request、精确成交与更新签名仓位回传最终 Adjust 的 Reconciled，不能把中间归零当作完整目标完成，也不能借对账继续反向增险。Control 同时核验原始执行投影与 delivery receipt 后才进入 ledger。

从未领取即过期的 Copy job 只能在双边新观察均晚于旧截止时间、follower 私有 generation 更新且当前关系仍获准时重新规划。Control 在同一事务锁定两条 delivery 及原 job，证明 lease/claim、receipt、execution、ledger 全部不存在，才把两条投递记录标为 `expired_unclaimed` 并建立独立新 job。原 job/payload/截止时间不变，不伪造 Rejected 或 ledger；任一已领取/Unknown 继续阻断。历史退休记录不占未决扫描额度，晚到执行结果不得重新挂回退休 job；该状态不是 WAL 或 Node receipt 状态。

## 12. 验收标准

局部代码按 package 与直接契约验证，文档/注释只做静态检查；已通过基线后的增量不重复全工作区回归。
跨模块公共契约、依赖或架构变更及正式发布前集中通过：

```powershell
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('fmt','--all','--check')
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('check','--locked','--workspace','--all-targets')
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('test','--locked','--workspace')
./scripts/verify_repository_hygiene.ps1
```

本机缓存与 Ubuntu 本地交叉编译按 `docs/DEVELOPMENT.md`；文档更新只做静态检查，不能因本文列出全量门禁就每次重跑。

还必须覆盖：

- 完整用户流成交零 BBO、零逐单 REST，并在周期 risk worker 未完成时仍进入补撤；
- 较早部分成交不会饿死后续完整成交；
- 签名回读缺单无需等待周期健康检查即进入重建；
- 同账户两个不同 symbol 的策略互不读写状态、互不撤单；
- 同 symbol 第二策略被拒绝；
- Unknown 命令重启后先查事实、不重复下单；
- 点查 404 不能单独收敛 Unknown；必须结合对应订单族列表、Client Order ID、必要成交历史和当前持仓完成签名查询；
- Pause、Stop、重连或规则变化后必须重新做风险校验；已写 `Submitted/Unknown` 的旧命令只能对账，不能重投；
- 连接断开只暂停受影响账户；
- Stop 只撤目标实例订单且默认不主动平仓；残仓时保留 symbol custody；
- 开仓订单全部满足实时最小名义价值；
- 六个交易所 adapter 通过相同 Account Runtime/Execution Lane 契约测试；能力差异由 adapter 明确证明或拒绝。
- Copy semantic Applied 必须继续进入 follower risk/Owner/WAL/writer，并由签名订单、成交和持仓事实收敛；只到达 delivery/ACK 不算物理闭环。
- Web 在 SSE 断线、cursor 断层、schema 错误、会话过期或陈旧 snapshot 下必须关闭全部 mutation，并在桌面与 390×844 移动端通过关键流程。
- `scripts/verify_gateway_candidate_contract.ps1` 必须证明非 LIVE 前置拒绝、未取得账户锁时物理 mutation 为零，以及 Unknown 不重投。
- 工件专项测试证明 5 MiB 轮转、10 MiB 单文件硬上限、256 MiB 根预算、含 Unknown 段不删除，以及已对账历史段可压缩/清理。

账户内核自身还必须覆盖：同 symbol 第二策略注册失败；订单按 family + Client Order ID 或交易所订单 ID 精确路由；身份冲突时冻结新增风险；Unknown 重启后不重投；重连和改参不复活旧意图；Pause 不被重连清除；Stop 只撤本策略订单；Flatten 以更新的签名持仓证明归零。无需为这些断言分别生成 root、seal、manifest 或不可伪造 receipt。

实盘接管必须满足：旧 writer 已停止且账户锁已释放、旧 WAL 的 Unknown 已收敛、当前订单和仓位已签名读取、10U 风险上限生效、Canary 成功且只有一个账户进程持锁。

## 13. 明确不做

当前不实现：

- 同一账户同一交易对多策略；
- 超出已获准窄账户注册/登录/凭证管理范围的多租户与付费权限系统；
- 跨机器 writer 选举、分布式 fencing、内容寻址 executable handoff 或复杂服务网格；
- 策略插件市场和公共 SDK；
- 在网格/账户 Runtime 内嵌 Web 面板或 Windows UI；获准的独立 `apps/venue-web` 和 VenueFlow 可开发无凭证 Control 客户端，
  但不得进入成交热路径或持有账户私流、writer、WAL 与 mutation 客户端；
- 为未来功能预建未被当前任务使用的模块；

只有当单机账户进程的 CPU、网络或交易所连接上限被实测证明不足时，才讨论拆分独立网关。当前规模下不提前增加这类复杂度。

当前迁移优先使用已获持续授权的 AI 执行 Stop、签名对账、释放账户锁和逐家 Canary；确需外部权限的事项在其余任务完成后汇总。
只有真实出现多机高可用需求后才重新评估 lease/handoff 体系。
