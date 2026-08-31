# VENUE 统一执行链与 Web 迁移开发契约

更新：2026-09-01

## 1. 文档职责

本文是“将 Binance、Gate.io、Bitget 的 Stage 7 生产链重构到统一账户执行链，并迁移 `G:\kol\apps\web`
用户体验”的当前开发契约。它固定目标、边界、阶段、退出条件和验收标准，不记录临时进度或发布流水。

- 总体模块和依赖边界查 [`ARCHITECTURE.md`](ARCHITECTURE.md)。
- 账户运行时、网格热路径、恢复和实盘接管查 [`GRID_RUNTIME_REFACTOR.md`](GRID_RUNTIME_REFACTOR.md)。
- 当前代码位置查 [`CODEMAP.md`](CODEMAP.md)。
- 当前获准任务查 [`REFACTOR_IMPLEMENTATION_GOALS.md`](REFACTOR_IMPLEMENTATION_GOALS.md)。

本文件的阶段是最终验收契约，不代表全部仍待编码，也不自动恢复已停止的长任务。
当前源码状态以第 3 节为准；开发方式见 [`DEVELOPMENT.md`](DEVELOPMENT.md)，停用状态见 [`DEPRECATED.md`](DEPRECATED.md)，版本范围见 [`CHANGELOG.md`](CHANGELOG.md)。

发生冲突时，交易安全与实盘接管以 `GRID_RUNTIME_REFACTOR.md` 为准；本文负责统一执行链和 Web 产品的实施顺序。

## 2. 已批准结果

本轮按两批收口：第一批只完善 Binance 的统一网关、Grid/Copy、手动交易与 Web；Gate.io、Bitget、Bybit、OKX、Hyperliquid 全部列入第二批验证与实盘，保留现有实现，不作为第一批完成的阻塞项。
Scalping 暂缓处理，既有实现保持失败关闭，不开发、不开放自动交易，也不作为两批收工门槛。文中涉及 Scalping 的内容仅记录既有架构与后续恢复工作时的约束。
Web 可以发起下单；它提交 `TradeIntent`，由 BFF→Control→Node→同一 risk/WAL/writer 执行，不在浏览器或 BFF 内另建交易所网关。当前已有后端与桌面手动交易桥，Web 表单/BFF 下单完整闭环仍待实现和验收。

最终只保留一条生产 mutation 链：

```text
Market / Private / Control Fact
-> Strategy Actor（Grid / Scalping / Copy）
-> semantic intent
-> Account Execution Lane
-> account risk + Owner validation
-> one account WAL
-> one account writer
-> one venue adapter
-> signed private/readback facts
-> Reconciler + strategy/copy ledger
```

固定结果：

1. 六个交易所复用同一 Account Runtime、Execution Lane、风险门、WAL 状态机、Owner 路由和恢复流程。
2. adapter 只保留原生签名、symbol、instrument rule、账户模式、订单族、限频、私流和请求/回报转换差异。
3. Stage 7 不再作为长期第二套执行架构；完成逐所接管后删除其生产调用点和仅为旧 authority 服务的外壳。
4. Copy Actor 必须把耐久语义目标转换为账户意图，并经过与 Grid、Scalping 相同的物理执行链；Control、PostgreSQL 和 Copy planner
   永不直接持有 writer 或交易所客户端。
5. 新建独立响应式 Web 应用；`VenueFlow` 保留为内部运维、诊断和桌面工具，不再承担面向用户的主 Web 产品。
6. `G:\kol` 只作为 UI 行为、样式和测试参考，不能成为 path dependency、运行时依赖或第二套网关/后端。

“统一”不等于伪造六所能力相同。某交易所不支持的产品、订单族或账户模式必须以能力证据明确拒绝。

### 2.1 持续实盘授权

本任务已获得对现有配置中真实交易账户进行重构验证和小额实盘操作的持续授权。执行本任务的 AI 可以：

用户所说的 10U 是单笔名义价值上限，允许同一账户多次测试，也可能存在已有持仓。下列账户累计 10U、非零仓位拒绝新增风险等是当前实现的更严格技术门，不是把用户授权重新解释为累计测试额度；本次目录与文档整理不修改这些执行门。真实验证按批次推进，第二批不自动在第一批期间启动。

- 在 Binance、Gate.io、Bitget、Bybit、OKX、Hyperliquid 的既有可用账户中选择符合账户 binding、能力契约和项目硬约束的交易对；
  初期既有 DOGE binding 保持该基础币：Bybit、OKX 使用 `DOGE/USDT`，Hyperliquid 使用实际永续报价 `DOGE/USDC`；
  后者必须以真实新鲜汇率验证 USDT 等值风险，不得借报价名称修正改变账户或策略基础币；
- 在离线门禁、签名预检、唯一 writer 和恢复检查通过后执行真实 place、cancel、必要的 reduce-only、Stop 和 Flatten 验证；
- 自行传入 CLI 所要求的 `--confirm-live` 或等价技术确认参数，无需为每个 Canary 再请求人工确认；
- 根据实时 instrument rule 选择价格和数量，但单个账户的现有绝对持仓名义价值、未撤增险订单名义价值与本次新增风险之和
  必须始终不超过 10 USDT 等值；
- 同一账户允许执行多轮串行测试，不限制累计测试次数；前一轮必须已终态、完成签名收敛且没有 Unknown/未撤增险订单，
  才能进入下一轮。若要再次新增风险，更新签名持仓必须已归零；
- 在一个账户/交易所失败关闭后继续完成其他不依赖该账户的代码、测试、文档和 Web 子任务。

持续授权是操作授权，不是绕过技术门。单笔 `max_entry_notional=10` 不能替代账户累计门；因此在 M1
完成下列汇总风险、每所真实估值接线及故障测试前，AI 不得发送真实新增风险命令，只能执行签名只读预检、撤单、reduce-only、Stop/Flatten 和
其他离线任务：

```text
sum(abs(all signed position notionals))
+ all open entry-order notionals
+ full risk reservations for Submitted / Accepted / Unknown entry commands
+ candidate entry notional
<= 10 USDT equivalent
```

汇总必须跨该账户全部策略和交易对，使用新鲜签名事实、实时 contract/instrument rule 和保守估值；Unknown 按最坏已成交预留，
不能因超时释放。单笔 10U 检查继续保留，但不能替代账户累计检查。

持续授权不包含：

- 提款、链上转账、资金划转、充值、购买资产或修改提现白名单；
- 修改 API 权限、账户安全设置、保证金模式、杠杆或会扩大既有风险的账户配置；
- 创建新账户、新 API Key/Wallet，读取未提供的 secret，或把 secret 写入命令输出、日志、数据库、UI、文档和工件；
- 突破 10U 累计名义上限、在未决 Unknown 时增险、同时启动两个 writer，或自动重投结果不确定的命令；
- 把某所网关 Canary 授权解释为该所 Copy/Grid 产品已完成准入。

若账户已有非零持仓、未撤增险订单、未决 WAL、Unknown、规则/账户模式不一致或签名事实不完整，AI 必须冻结该账户新增风险，
只允许撤单、reduce-only、Stop、Flatten 和只读对账。不得为了完成测试而清除、覆盖或猜测真实持仓。

AI 只发起经授权的 operator/control 语义动作，不能直接调用 adapter mutation。自选 symbol 必须先以新的持久 binding/config revision
保存，并通过 adapter capability、实时规则/标记、账户模式和 Owner 冲突校验；恢复 Pause/Stop、首次启用或换 symbol 仍须产生明确
且可审计的确认记录，但本节持续授权允许 AI 自行提交该记录，无需再次询问用户。Unknown 只能由签名事实收敛，任何确认记录都不能替代。

### 2.2 人工协助延后原则

重构期间不因常规实盘确认暂停工作。只要仍有安全、独立、在授权范围内的任务可执行，就继续推进。只有确实需要用户掌握的
凭证、主机/数据库权限、硬件签名、交易所后台设置、外部审批或超出上述授权的动作，才记入“人工协助清单”。

人工协助清单在其他可执行子任务全部完成后一次性提交，每项必须包含：阻塞对象、已完成证据、失败关闭状态、用户需要执行的
最小动作和完成后的复验命令。某账户出现 Unknown 或异常暴露时必须立即停止该账户新增风险并保存证据，但仍可继续其他账户的
只读工作和不相关子任务；不得等待用户期间维持不受控 mutation。

## 3. 当前基线与真实缺口

### 3.1 可复用基线

- 六个 `venue-gateway-*` adapter 已具备不同程度的签名 HTTP、私流、账户事实、place/cancel、规则和签名回读能力。
- 规范限价命令以 `LimitTimeInForce::{PostOnly, Gtc}` 区分只挂单和普通限价；六所 wire、签名订单与 WAL 对账必须保持同一政策，不能只按订单 ID 判定成功。
- Binance、Gate.io、Bitget 的 Stage 7 已保存成熟的网格热路径、成交滚动、完整订单族回读、Unknown 和接管行为。
- 六所固定 Node 已组合 `AccountRuntimeHost`、Execution Lane 与 `AccountMutationHost`；preflight/Canary 也不绕过统一链。旧三家的 `--legacy-v1-handoff` 前驱保护仍是当前启动前置条件，服务器接管另行证明。
- `venue-copy` 已包含资本、目标敞口、数量、限价、确定性身份、delivery、ledger 和 drift 纯语义。
- Control/PostgreSQL 已保存 Copy relation、job delivery、claim/ACK/receipt、ledger/drift 及 0016 未领取过期恢复；Node 已有 Copy 语义到同一账户 WAL 的物理桥和签名执行结果回传，完整产品闭环仍需验收。
- 六所固定 Node 已接入 adapter-owned 公共盘口及公共成交；Bybit 重建完整簿、Hyperliquid 使用原生完整 L2 图像，OKX 按 `prevSeqId` 连续桥接。成交原生身份与本地 Session cursor 分离，批量事实有界无损轮转；Binance 保留显式原生聚合序列。Bybit/OKX/Gate 仅发布协议确认闭合的 K 线，Bitget/Hyperliquid 形成线不自动提升。行情只进入共享 MarketHub/FeatureSource；公共接收器和 fixture 通过不代表策略自动出单、私流热路径或逐所接管完成。
- 当前 `apps/venue-web` 已采用 Next.js 16、React 19、TypeScript 和同源 BFF，包含响应式页面、会话、恢复、SSE、十进制及隔离交互测试；旧 KOL Web 只是行为参考，不是运行依赖。

### 3.2 已知缺口（按批次安排，Scalping 暂缓）

- Copy 已有 semantic Applied 到同一账户 WAL、签名成交/仓位的物理桥；leader 权威事实自动接入、持续 ledger/drift repair 和完整产品端到端仍须验收。
- 六所 Node 已组合 Account Runtime、Execution Lane 与 `AccountMutationHost`；Grid/Scalping 的生产私流驱动和物理接线、控制撤单/清仓及多 symbol 常驻调度仍须闭合，不能以纯 reducer 测试替代。
- Scalping 的 FeatureSource frame 已接入真实 engine evaluate 与同一 Actor checkpoint 恢复，配置须显式固定 release、Owner scope 和风险预算；手造候选到 Host 的旧入口已移除。Bitget/Hyperliquid 权威闭合 bars、六所持续行情实测、签名安全投影、入场确认与退出保护仍未闭合；保护缺失时阻止自动入场，对外不得显示已可运行。Session-observed Ready 仅表示本机窗口完整，详见运行时契约第 4.1 节。
- 根 package 的旧三所生产 binary 已移除，但服务器上的旧 release、未决 WAL 与运行进程仍须逐所核验和接管；本地删除入口不构成生产退休证明。
- 六所尚未共同满足同一套启动恢复、Owner 路由、Stop/Flatten、Unknown 收敛和产品级 Canary 契约。
- 手动 `TradeIntent` 已有 Node 显式选价、同一 Actor replay 的原始计划、精确自有手动单撤单及只读 delivery 对账；仍须完成与 Grid desired/库存的协同、Copy 绑定支持和全 scope 撤单后才能认定完整闭环。当前不支持的 scope 明确拒绝；BBO 自动选价不能替代用户选价，离线通过不替代生产接管验收。
- Control 的 loopback API 与 Web 会话、鉴权、同源 BFF 已建立；隔离 fixture 的浏览器验收不替代指定主机的真实 Node/Control/Web 连通与分段性能验收。
- 已保留主线的用户会话、账户中心与 Binance 凭证加密托管；这些属于 Control 应用层，不进入账户 writer。
  其他交易所的自助凭证接入、面向公众的多用户 Web 登录和付费能力仍未完成，不能由前端页面假装已实现。

因此，在物理跟单闭环完成前，不得把项目状态描述为“后端完成、只剩 UI”。

## 4. 统一执行链设计

### 4.1 唯一写入边界

每个 `(exchange, trading_account_id)` 恰好一个账户进程、一个进程锁、一本命令 WAL 和一个串行 writer。

```text
Account Runtime
├─ Market Hub
├─ Private Router
├─ Strategy Registry
│  ├─ Grid Actor(s)
│  ├─ Scalping Actor(s)
│  └─ Copy Actor(s)
├─ Execution Lane
├─ AccountMutationHost
├─ Reconciler
└─ exactly one venue adapter
```

Execution Lane 负责调度、Owner、优先级和单 in-flight；`AccountMutationHost` 负责账户锁、风险复核、同一 WAL、Unknown fence
和 adapter dispatch。实现可以在行为等价后合并两层，但不得让两层各自拥有 writer、WAL 或订单状态机。

### 4.2 通用命令契约

账户层只接收规范命令：

- `Place`：规范 instrument、side、position side、purpose、order family、quantity、price、time-in-force 和 client identity；
- `Cancel`：必须精确匹配 Owner、family、client/venue order identity；
- `Reduce`：只允许 reduce-only，数量不得超过更新签名持仓和已承诺减仓量；
- `Stop`：停止新增意图并撤销目标实例自有订单，默认不平仓；
- `Flatten`：撤自有订单后按更新签名持仓降至零；
- `Reconcile`：只读收敛，不授予重投旧命令的权限。

WAL 状态只保留 `Prepared / Submitted / Accepted / Rejected / Unknown`。HTTP ACK 不等于最终交易事实；`Accepted` 后仍须由
更新一代签名订单、成交和持仓事实完成策略状态收敛。

`OrderCommand.time_in_force` 属于不可变命令和 WAL 摘要：历史省略字段的命令按原 PostOnly 契约恢复并保持原序列化字节，Gtc 必须显式编码，禁止用相同 command ID 改政策。签名订单缺失或不支持该字段时保持未知，不得套用命令默认值；即时回读、重启 Unknown 收敛、Owner 归属与 Desired Orders 比较都必须检验政策。旧实单与旧命令契约不符时失败关闭，不改写历史证据。

Grid 和当前 BBO 自动归一化仍固定 PostOnly；只有显式手动限价意图才可选择 Gtc，不能因 adapter 支持普通限价而改变策略的防吃单边界。

显式手动归一化使用 `AccountPricedLimitIntent`：用户价格及政策不可改写，数量按报价预算、平仓基础数量上限和实时 native lot 向下取整；不满足真实规则则拒绝，不向上补足名义价值。该只读转换不授予 mutation，结果仍须由同一 Actor、风险、WAL 和账户 writer 准入。撤最近订单只使用原生创建时间，缺失保持未知，禁止用最后更新时间或本机接收时间替代。

### 4.3 adapter 契约

六所 adapter 共同实现窄能力，不依赖 runtime、copy、strategy 或 UI：

1. 规范/native symbol 双向转换；
2. 实时 instrument rule 与时效；
3. 账户产品、持仓模式、权限和订单族能力；
4. 余额、全部持仓腿、全部开放订单族和成交游标的有界签名快照；
5. 私有订单、成交、仓位事实及连接 generation；
6. place、cancel、reduce-only 请求编码；
7. client/venue order identity 的精确签名回读；
8. 时间同步、分页闭合、限频和 Unknown 所需事实。

原始响应只在 adapter 内解析；跨边界只传规范事实及必要的原生证据摘要。

### 4.4 Copy 接入

Copy 的目标链固定为：

```text
Leader authoritative fact
-> immutable snapshot
-> target exposure plan
-> durable follower job
-> Node Copy Actor
-> follower AccountExecutionIntent
-> risk / Owner / WAL / writer
-> adapter
-> signed private facts
-> Copy ledger / drift repair
```

Copy Actor 输出意图前必须重验：relation revision、leader/follower binding、instrument generation、目标时效、follower 当前签名持仓、
可用资本、倍率、准备金、单笔/累计限额和账户生命周期。跨零反向分两轮：先 reduce-only 到零，等新私有事实后再开反向风险。

自动规划输入复用 Node 的耐久 projection outbox：外层 `copy_planning_facts` 携带精确 relation/revision/policy、实例 epoch、
规范 instrument、私有/规则 generation、原始报价敞口和有效窗口，不进入浏览器 DTO。Leader 策略资本必须显式配置，不能取账户权益代替；
Follower 可用保证金必须来自同轮签名事实。Control 仅配对同报价资产、当前 Running 实例的新鲜双边事实，不按稳定币 1:1 换算。
worker 在同一数据库事务中冻结输入、规划任务并推进观察游标；倍率作为冻结字段进入纯计算，不改写 Leader 原始敞口。
同一经济输入的重复上传不产生新任务；旧 revision 任务未签名收敛时仍阻止该关系叠加新任务。新节点的空/暂停投影不得回退使用旧节点事实。
已经签名收敛并写入 ledger 的任务若仍有漂移，必须等待不早于该收敛仓位的新鲜双边事实，重新验证当前 Active 关系、资本、规则与目标，
才可产生带 `supersedes_job_id` 的独立修复语义任务。修复身份、窗口和持仓代际来自新观察；历史 projection 只证明来源，不续期旧任务、
不重投旧 child，也不把历史 `repair` 候选直接作为新风险授权。没有 ledger 的 Accepted、Unknown、Rejected 不因重复目标进入自动修复。
上游资本规划应保留原始跨零目标和当前敞口，不能在生成语义 job 前拒绝反向目标，也不能把完整跨零 delta 当成一笔可执行订单。
数据库 observer/job 的账户 scope 是实际接收 job 的 follower；leader 的 venue/account 不得与 follower scope 混为一谈。

原始执行 request 必须早于 Actor Applied 和账户 WAL 耐久保存。每个 immutable job 只允许一个 ReduceToZero child 和一个 Adjust child，
child 身份不得包含不断推进的签名快照 generation；重启或重复 delivery 不得用新持仓重算已提交 child 的价格、数量或原 request。
尚未过期的 Copy Install 领取窗口必须精确为 `min(领取时间 + 请求租期, 原 job 截止时间)`；Control 数据行、immutable claim 和 Node 校验保持一致，不能因剩余窗口不足完整租期而漏领，也不得续期 job。已领取任务过期后只允许 ReconcileOnly 使用完整对账租期；从未领取即过期的任务不能重新执行。`copy_planning_expiry.rs`/0016 在同一事务证明双 delivery 从未 claim、无执行或记账，并取得截止时间之后更新的双边事实，才能退休旧投递并建立独立新 job；保留旧 payload/期限，不伪造 Rejected 或 ledger。
恢复读取同一本 WAL 的原命令，按精确 native order identity 累积规范成交并检查更新的完整仓位腿与开放订单；仅 ACK 或较新仓位不能证明成交。
过期、暂停或旧 revision job 仍可只读收敛原 child，但不能产生新风险。第二 phase 必须保留第一 phase 的 request/签名零仓证据，重新检查
当前 relation、有效期和账户风险，并在新 WAL 前单独持久化 Adjust request；不得覆盖第一 phase 的恢复事实。
第二 phase 的最后一次签名读取仍须证明零仓；若减仓回读与新 request 之间仓位发生变化，应停止续行，不能重新解释已存在的 ReduceToZero child。
执行结果保留原 request 的仓位代际，另携带完成对账的更新签名仓位；ledger 按该实际仓位匹配，不能用新仓位代际查找旧 request，
也不能把包含命令和成交的执行摘要当作单独仓位摘要。同一 job/phase 不得借新代际重新绑定命令。关系暂停或改参不阻断既有 child 的只读结果记录。
Node 通过既有 projection outbox 传输有界、固定编码的原始执行结果，保留 ReduceToZero 与 Adjust 的各自历史；它们不放进浏览器的 UI facts。
Control 在提交投影游标的同一事务内校验外层 binding、SHA256、内层结果和原始 delivery，记录结果；批次任一项冲突则全部回滚。
只收到完全相同的回显后，Node 才在既有 Copy journal 标记该结果已投影。结果投影可跳过尚未上传的中间状态，但 Reconciled 必须携带更新签名仓位。
回传 request 的目标、资产、phase 与 delta 必须由原 immutable job 和统一纯规划语义校验；已有 ReduceToZero 时，Adjust 只能引用其已签名归零后不早于该代的零仓事实。过期结果仍可只读记录，但不能借投影刷新授权。
相同规范成交在不同签名快照代重复出现时，成交身份与全部交易字段、摘要必须一致，只允许观测 generation 不同并保留真实较新代；不能因重复回读阻断投影，也不能吞掉数量、方向、价格或订单身份冲突。
Copy 的 ReconcileOnly 回传须匹配原 delivery ID、账户/实例/epoch binding、完整 immutable payload、当前 request 与 manifest 摘要；只有最终 Adjust 已由原 WAL、精确成交和更新签名仓位收敛，且执行结果携带的仓位与耐久 journal 一致，才能回传 Reconciled。缺失、Pending、Unknown 或中间 ReduceToZero 不得伪造终态；对账后的跨零续行禁用标记保存在原 Copy journal，后续 tick 和重启均不得据此增险。执行投影和 delivery receipt 均保留原身份，由 Control 交叉核验后记账。
Control 已确认的 Unknown 可在原租期结束前领取精确下一 epoch 的 ReconcileOnly；Node 必须先有同一 Unknown 的耐久回显确认，且新领取时间不早于该事实。未确认 Unknown、普通 Install、Applied 或 Rejected 不适用此提前对账例外；该例外只缩短只读恢复等待，不授予执行权限。

每个 immutable snapshot、job、manifest、outbox row 和 Actor inbox 都必须耐久绑定精确 relation revision 与 policy digest。关系改参、
Pause、Stop 或删除时，Control 必须在同一 PostgreSQL 事务内递增 revision 并产生配置变更事件；Planner 和 Node 通过耐久事件/投递消费，
不得由 BFF 或 PostgreSQL 写入直接修改 Actor 内存。旧 revision job 只能完成只读对账或以稳定原因拒绝，不能套用新配置继续执行；
配置事件重复消费必须幂等，跳 revision 或配置摘要冲突时失败关闭。

`Applied` 必须区分：

- `SemanticApplied`：目标已被 Actor 耐久接收；
- `ExecutionPrepared/Submitted`：物理命令已进入账户 WAL；
- `ExecutionAccepted/Rejected/Unknown`：请求结果；
- `Reconciled`：签名事实已收敛；

UI 不得把 `SemanticApplied` 显示为“已成交”或“跟单成功”。若协议暂不新增这些枚举，投影必须用已有 receipt、WAL 和
reconciliation 字段组合出等价且不误导的状态。

## 5. Stage 7 直接重构规则

### 5.1 重构而非重写

保留并复用：

- `venue-strategies` 的共享 reducer 和网格语义；
- 三所 adapter 中已验证的签名、私流、订单族、规则和回读；
- Stage 7 的成交优先级、部分成交隔离、post-only 防吃单、完整签名缺单重建行为；
- 现有 fixture、行为测试和实盘恢复事实。

替换并最终删除：

- 独立于统一账户 WAL 的 Stage 7 mutation 编排；
- 仅服务旧链的 capability promotion、lease generation、root/manifest/receipt 组合；
- 交易所专用 runtime 对 writer、WAL 或 Owner 的持有；
- 固定 `run_*_stage7_*` 生产入口及其 CLI 组合；
- 新链已覆盖后的兼容重导出、桥接和重复恢复投影。

不得在同一阶段同时重写 adapter transport、策略 reducer 和持久化格式。先把既有行为接到统一账户链，再逐项替换 transport 或布局。

### 5.2 两批顺序

第一批：Binance。完成该所网关、Grid/Copy、Web 手动下单和真实 UI 验收后独立收工，不等待其他交易所。

第二批：Gate.io、Bitget、Bybit、OKX、Hyperliquid。沿用第一批共享链，逐所补齐差异、验证和实盘接管；真实 mutation 串行，不同时接管多个账户。第二批开始时再核验账户与能力，不把已有 adapter 代码视为验收通过。

Scalping 不在两批任务内。某所未完成退出条件时，不得以“adapter 已支持下单”为理由推进该所的 Copy 产品准入。

### 5.3 单所实施模板

每所必须按相同顺序完成：

1. 建立 adapter contract fixture，覆盖账户模式、规则、订单族、分页、私流和错误映射。
2. 将签名账户快照接入 Reconciler，证明完整 symbol universe、全部腿和全部订单族。
3. 将私有事实接入 Private Router，证明 Owner 精确路由、连接代际和成交游标恢复。
4. 将 Grid/Copy 语义意图接入 Execution Lane，证明只能经账户 host dispatch。
5. 覆盖 place、cancel、reduce-only、Stop、Flatten 和 post-only 明确拒绝。
6. 注入发送前崩溃、发送后超时、ACK 矛盾、404、部分空页和重启，证明 Unknown 不重投。
7. 验证同账户不同 symbol 公平执行，同 symbol 第二 Owner 拒绝。
8. 通过离线门禁后，停止旧 writer、收敛旧 WAL、签名读取订单/仓位并做单账户小额 Canary；同一账户可按缺陷修复需要重复多轮。
9. 每轮 Canary 均须停止并完成签名收敛；该所全部必需轮次通过后，才标为统一链生产准入。
10. 调用点清零后才删除该所 Stage 7 生产入口；恢复读取兼容按实际工件需要保留。

删除前还必须审计该所全部旧 root、checkpoint、WAL、Unknown、服务配置和运行进程：未决状态已签名收敛、旧入口调用为零、
同账户锁唯一、仍需恢复读取的工件已有新链兼容测试。不得按日期、代码合并或一次 Canary 成功直接删除旧实现。

### 5.4 接管和回退

接管前必须同时满足：

- 旧 writer 已正常停止，账户锁已释放；
- 旧 `Prepared/Submitted/Unknown` 已根据签名事实收敛；
- 当前开放订单、成交游标和全部持仓腿已签名读取；
- 新链恢复演练能重建 Owner、WAL、checkpoint 和 reconciliation 水位；
- 10U 初始硬上限和不增险规则生效；
- 只有一个候选进程能取得账户锁。

回退不是同时运行旧版本。新链必须先 Stop、收敛 Unknown、签名回读并释放账户锁；随后旧版本只能从兼容读取或人工确认的
干净状态启动。任何一方存在未决 mutation 时禁止切换。

## 6. Web 迁移

### 6.1 目标和技术栈

在本工作区新增 `apps/venue-web`，采用：

- Next.js 16 App Router；
- React 19；
- TypeScript 7 严格模式；
- 原生 CSS/Design Tokens，先复用旧 Web 已验证的响应式布局，不为迁移额外引入 UI 框架；
- Node test + TypeScript typecheck；
- Playwright 做桌面和 390×844 移动端关键流程；
- 同源 BFF 代理 Control，浏览器不直接访问 loopback Control 或 PostgreSQL。

版本以首次建立 `apps/venue-web/package.json` 时 workspace 能稳定安装和构建的精确锁定值为准；不得使用宽松版本范围。

### 6.2 复用、重写和不迁移

从 `G:\kol\apps\web` 选择性迁移：

- `app-shell` 的桌面侧栏、移动抽屉、焦点恢复和滚动保护；
- `globals.css` 的 design token、断点和触控尺寸；
- `i18n`、decimal 字符串、连接恢复和只读失败关闭行为；
- dashboard、follower、KOL、Admin/OPS 页面的信息架构；
- SSE 断线、游标断层、会话失效和移动交互测试思路；
- manifest/offline shell，但离线或陈旧快照下所有 mutation 必须禁用。

上述文件只提供实现思路和可验证行为，不能整文件复制。尤其 `control-api.ts`、`realtime.ts`、`sw.js`、`next.config.ts` 含旧 `/v1`、
Bearer/session、mock 或旧环境假设，必须重写并接受负向扫描；可复用重点限于 `app-shell`、`globals.css`、decimal、recovery 的
布局/算法思路和独立测试用例。

必须重写：

- `control-api.ts` 和所有 DTO，目标只认 Venue schema v2/BFF 契约；
- 认证、会话和角色映射；
- relation、账户、订单、风险、receipt 和 reconciliation 的数据查询；
- 所有高风险命令确认和幂等键；
- 旧页面中把 planner/ACK 当作执行完成的状态文案。

不得迁移：

- KOL 后端、数据库 schema、网关、observer、executor 或 instrument sync；
- 浏览器中的 API Key/Secret/Passphrase/私钥输入和持久化；
- mock trading、demo mode、旧 `/v1` DTO、旧 mutation gate；
- 与 Venue 当前产品无对应后端事实的空壳按钮。

### 6.3 Web/BFF 安全边界

```text
Browser
-> same-origin Next.js BFF/session
-> authenticated, allow-listed Venue request
-> loopback venue-control /v2
-> durable Control command/outbox
-> Account Node
```

- BFF 与 loopback `venue-control` 部署在同一主机；Control 不因 Web 上线改为公网监听。
- 浏览器只持有短时会话，不持有交易所凭证、数据库连接或账户 writer token。
- M5 必须先建立最小受控会话认证、SameSite/CSRF 防护和角色 allow-list；在它们完成前 Web 只允许本机只读，不得开放公网写操作。
- 会话使用 `HttpOnly + Secure + SameSite` cookie，所有 mutation 校验 CSRF token、Origin/Host、角色和账户 scope；BFF 在服务端生成或
  校验 idempotency key，并把 session subject、role、account scope、command id 和时间写入审计投影，禁止记录 secret。
- BFF 只代理明确列出的查询和语义命令，不提供任意 URL、任意 JSON 或透明管理隧道。
- 金额、价格、数量均使用十进制字符串；时间使用 `*_ms` Unix 毫秒整数。
- Snapshot 必须 `no-store`；SSE 断线、cursor 断层、schema 错误或会话过期后立刻关闭写入门。
- Pause/Stop/Flatten 必须显示精确 venue、mode、account、symbol、instance、config epoch 和动作，并由服务端再次校验。
- 当前凭证仍只来自账户节点进程环境或根 `.env`。自助账户接入只有在独立凭证托管与审核契约获批后才能开放。

### 6.4 页面交付顺序

1. Shell、主题、响应式导航、错误/离线/恢复状态；
2. 只读总览：账户节点、策略、网关健康、Copy relation 和执行状态；
3. Copy relation 创建/编辑、目标/实际/漂移、job/receipt/ledger；
4. 订单、持仓、成交和 reconciliation；
5. Binance 手动限价/撤单表单与签名执行状态、Pause/Resume/Stop/Flatten；手动下单必须复用 `TradeIntent` 和同一账户执行链，不能把 Control 已接收显示为已成交；
6. KOL/follower 产品页和必要角色视图；
7. 经另行批准的用户自助注册、邀请、账户接入和凭证验证；最小受控会话认证已属于 Web 基础，不得延后到本步骤。

前六步不能依赖第七步的多租户能力；本地单用户/受控运维会话必须先完成端到端验收。

### 6.5 UI 截图、易用性和性能验收

页面功能完成后必须使用真实浏览器和实际 Control 投影逐页截图、复核并迭代，不能只凭组件测试判断布局合理。至少覆盖：

- `390×844` 手机竖屏、`844×390` 手机横屏、`768×1024` 平板、`1440×900` 和 `1920×1080` 桌面；
- 总览、Copy relation 列表/详情/编辑、账户、手动下单/撤单、订单、持仓、成交、ledger/drift、网关健康、Pause/Stop/Flatten 确认；
- loading、empty、error、offline、stale snapshot、SSE reconnect、Rejected、Unknown 和 Reconciled；
- 中英文长文本、极大/极小十进制、长交易对/账户 ID、多行错误和大量列表。

每轮截图由 AI 检查并调整：信息层级、对齐、留白、折行、对比度、表格溢出、固定导航遮挡、安全区、横向滚动、触控目标、
焦点顺序、键盘操作和高风险动作辨识。常用任务应在不超过三次主要操作内到达；触控目标最小 `44×44` CSS px；手机主流程
不得依赖 hover，关键按钮不得被软键盘或底部安全区遮挡。

截图、trace 和性能报告写到 `G:\Build\Venue\venue-web-qa\<run-id>` 或等价隔离构建目录，不提交 Git，不写入交易恢复
`G:\Venue\artifacts`，并在保存前检查不含 token、secret、完整账户敏感信息或私有 payload。

性能必须拆分测量并报告 p50/p95/p99：

1. 浏览器导航到首个可用 snapshot；
2. BFF 代理开销与 Control snapshot 响应；
3. Node 投影更新到 SSE 到达浏览器并完成渲染；
4. 用户命令到 Control durable receipt；
5. Node claim/Actor applied/WAL/dispatch；
6. 交易所网络 ACK；
7. ACK 到更新签名订单/成交/持仓收敛并回显 UI。

交易所网络耗时必须与本地处理分开，不能用 UI 总耗时掩盖网关慢点。账户热路径仍执行 `GRID_RUNTIME_REFACTOR.md` 的路由 `<5ms`、
dispatch 启动 `<20ms`（均不含交易所网络）目标。Web 首次可用 snapshot 的 p95 目标为 2.5 秒以内，loopback BFF 额外 p95
目标为 100ms 以内，Node 投影到已连接页面渲染的 p95 目标为 500ms 以内；未达标必须定位网络、数据库、序列化、SSE 或渲染阶段并调整。

### 6.6 指定集成主机

指定集成/实盘主机为 `45.77.253.180`，当前预期可通过既有密钥免交互 SSH 连接。持续授权允许 AI 在该主机部署、启动、停止和
重启本任务相关的 Venue gateway/Node、Control 和 Web 服务，无需逐次人工确认，但必须遵守：

1. 首先只读检查主机身份、工作目录、Git revision、现有进程/服务、监听端口、账户 binding、writer lock、WAL/Unknown 和磁盘预算；
2. 不停止或修改无关服务，不覆盖活动 release、凭证、数据库和交易 artifacts；
3. 使用版本化 release 或已验证部署目录，先完成二进制隔离和离线门禁；
   Ubuntu binary 默认经本机 `scripts/Build-VenueUbuntu.ps1` 编译至 `G:\Build\Venue\ubuntu\releases/<版本号>` 后上传；
   弱服务器不承担日常 Cargo 编译，上传后核对 SHA256、ELF 架构及动态库，不能仅以文件上传成功认定能运行；
4. 任何账户启动前证明旧 writer 已停止且锁已释放，真实 mutation 仍全局串行；
5. secret 只从主机既有受控环境读取，不下载、不回显、不写入测试报告；
6. Gateway 启动后先做签名只读 preflight，再验证 Control/Node/BFF/UI 连通，账户累计 10U 门未完成前不得新增风险；
7. 异常时停止相关新增风险、保留证据和恢复工件，不能通过删除锁/WAL 或强行双启动恢复。

若免密连接、目录权限、服务管理权限或数据库访问后来不可用，记录到最终人工协助清单，同时继续其他本地和只读子任务。

## 7. 开发阶段与门禁

### 7.1 多子任务执行方式

以下是获准实现任务的有界拆分，不是文档/目录整理时自动启动全部开发的指令。T3 第一批只处理 Binance，T6 包含 Web 手动下单；其余五所 T3/T7/T8 均后移至第二批。Scalping 子任务暂停。

本任务必须按有界子任务执行，由一个协调任务维护依赖、集成、全仓门禁和最终人工协助清单。各子任务只修改明确负责的目录；
共享 DTO、WAL/Owner 契约和 migration 先由契约子任务落地并通过测试，消费者再接入。推荐拆分如下：

| 子任务 | 责任范围 | 依赖 | 主要交付 |
|---|---|---|---|
| T0 契约与入口 | `venue-domain`、gateway/control protocol、长期文档 | 无 | 规范 intent、状态语义、能力矩阵、CODEMAP |
| T1 账户执行脊柱 | `venue-runtime`、`venue-execution`、Node host | T0 | Registry/Router/Lane/Host/Reconciler 单链 |
| T2 Copy 物理桥 | `venue-copy`、Node Copy Actor、执行回执 | T0、T1 | semantic target 到 follower WAL/签名事实 |
| T3 交易所接入 | 每次只负责一个 `venue-gateway-*` 与对应 Node binding | T1 | adapter contract、恢复、Canary、接管证据 |
| T4 Control/投影 | `apps/venue-control`、SQLx migration、schema v2 | T0、T2 | 查询、命令、ledger/drift、Web BFF 契约 |
| T5 Web 基础 | `apps/venue-web` shell、BFF、session、SSE、CSS | T0、T4 契约 | 响应式壳、恢复门、只读总览 |
| T6 Web 产品页 | Copy/账户/手动下单/订单/风险/控制页面 | T4、T5 | Binance 桌面/移动端关键流程 |
| T7 质量与发布 | scripts、CI、二进制隔离、PG/Web E2E | 各实现子任务 | 全仓和产品门禁报告 |
| T8 旧入口清理 | Stage 7/旁路入口、兼容读取、CODEMAP | T1–T7 与逐所接管 | 调用点清零、兼容范围、删除证明 |

可并行的是相互无写入冲突的代码、fixture、Control 和 Web 子任务。真实账户 mutation 全局串行：任意时刻最多一个实盘子任务、
一个交易所、一个账户 binding 持有 writer；即使多个子任务已完成离线门禁，也必须按第 5.2 节顺序排队 Canary。

固定依赖为 `T0 -> T1 -> T2 -> T3/T4 -> T6 -> T7 -> T8`；T5 的 shell、CSS 和本机只读状态可在 T1/T2 期间并行，
但 BFF 写操作必须等待 T4 契约、最小受控会话认证、T2 物理闭环和目标交易所准入。protocol、SQL migration 和长期文档各自只允许
一个整合者串行修改，其他子任务先提交契约需求，避免并行产生不兼容 schema。

每个子任务交接必须报告：修改文件、契约变化、测试命令及结果、未完成项、是否涉及实盘、账户是否已 Stop、是否存在 Unknown，
以及是否新增人工协助项。协调任务不得只凭子任务结论宣称完成，必须复核共享差异和相应门禁。

### 7.2 阶段

#### M0：契约和入口

- 本文、`ARCHITECTURE.md`、`GRID_RUNTIME_REFACTOR.md`、`CODEMAP.md` 和 Goals 无冲突；
- 旧链只允许修复阻断接管的缺陷，不再增加功能；
- 生产入口、迁移入口、兼容读取入口在代码和文档中有明确标签。

#### M1：统一账户执行脊柱

- Account Runtime 组合 Registry、Private Router、Execution Lane、AccountMutationHost 和 Reconciler；
- Grid、Copy、手动交易使用同一个规范 intent；Scalping 保留接口但暂停接入验收；
- 六个固定 Node binary 只链接一个 adapter；
- 账户级 writer/WAL/Unknown 契约测试不含交易所分支。

#### M2：Copy 物理闭环

- Copy semantic Applied 可生成受控账户 intent；
- follower 执行结果、私有事实和 drift 回写 Control/ledger；
- 重复 delivery 幂等，Unknown 不重投，跨零分两轮；
- 在 mock/fixture 环境完成崩溃边界和端到端测试。

#### M3：第一批 Binance 接管

- Binance 按第 5 节模板完成接管；Gate.io、Bitget 后移第二批；
- 每家先完成行为等价测试，再做单账户串行 Canary；可重复多轮，但任意时刻只有一轮持有 writer；
- 当前一所完成 Stop 和签名收敛后才推进下一所。

#### M4：第二批五所验证与接管

- Gate.io、Bitget、Bybit、OKX、Hyperliquid 沿用现有常驻 Node/host，补齐能力差异并逐所验证，不重建网关；
- 六所共用启动、恢复、控制和观测投影；
- Node→Control 观测上传固定为 loopback `/v2/account-node/projection`：Node 先将 envelope 写入耐久 outbox，按 node generation/sequence/digest 串行重放，Control 在同一 PostgreSQL 事务中提交 cursor、snapshot 与 signed execution facts；重复 envelope 幂等，冲突、跳序、跨账户内容或错误 rollover 一律失败关闭。该回显不是 writer、WAL、capability 或 dispatch authority；
- 每个实例的 outbox 游标按账户、node_id、instance_id 分开保存；不同实例不可共用一个 sequence。上传仅替换该实例的策略/订单/持仓等投影，账户汇总只接受较新事实，不能删除兄弟实例或其他账户；旧 cursor 的实例键由原 envelope 迁移，缺失或冲突拒绝迁移；
- adapter capability matrix 决定功能，不在 runtime 复制交易所分支。

#### M5：Web 基础与只读产品

- 建立 `apps/venue-web`、锁文件、BFF、最小受控会话认证、schema client、shell、恢复状态和移动端门禁；
- 先交付只读投影，再开放语义控制；
- VenueFlow 同期收敛为内部运维工具，避免重复开发用户页面。
- VenueFlow 继续提供内部 Control 运维和原生无凭证公共行情，不作为用户 Web 页面或 BFF；`apps/venue-web` 不复制其交易所客户端能力。

#### M6：受控写操作与产品验收

- 第一批完成 Binance Web 下单/撤单与高风险控制的幂等、审计和失败关闭验收；
- Copy 单账户、单关系、小额 Canary；
- Binance 第一批独立准入；其余五所第二批逐家准入，不因网关准入自动扩大 Copy 准入。M4 不阻塞第一批 M5/M6。

#### M7：旧入口退休

- 根 package 的 `hedged-grid-{binance,gate,bitget}` binary、Cargo feature、部署 re-export 与发布脚本已清零；其余 Stage 7 代码只可作既有工件读取兼容，不能重新接通 mutation；
- 旧工件读取兼容有明确保留期限和测试；
- `CODEMAP.md` 删除已不存在入口，`legacy` 目录只剩仍被真实工件需要的读取代码；
- 删除前执行全仓引用、发布脚本、服务配置和实盘进程审计。

## 8. 验收矩阵

### 8.1 Rust 分层验证

默认只检查修改的 package 与直接契约；交易安全修改另覆盖受影响的 risk/WAL/Unknown/恢复专项。文档、注释或 lint 标注不触发业务回归。
跨模块公共契约、依赖或架构变更及正式发布前集中建立以下基线；基线通过后的局部增量不重复全工作区测试，记录源码范围与专项结果：

```text
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('fmt','--all','--check')
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('check','--locked','--workspace','--all-targets')
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('test','--locked','--workspace')
scripts/verify_repository_hygiene.ps1
```

按影响范围追加：

- `scripts/verify_workspace_quality.ps1`；
- `scripts/verify_venue_node_binaries.ps1`；
- `scripts/verify_venue_node_binary_isolation.ps1`；
- 默认 `scripts/Build-VenueUbuntu.ps1 -ExpectedRevision <40位commit> -ReleaseId <id> -CheckOnly`，可显式指定干净 `-SourceRoot`；预检后去掉 `-CheckOnly`，在本机受控 slot-2 编译六所 Linux ELF 产物至 `G:\Build\Venue\ubuntu\releases/<id>`。固定 GNU/Linux glibc 2.35 target、精确工具版本、源码 revision、SHA256 与不可覆盖的 manifest，专项为 `test_venue_ubuntu_build.ps1`。不在弱集成服务器日常编译；Linux 构建机仍可使用 `package_venue_node_linux_release.sh` 备用，不启动服务，完整约束见 `docs/BUILD_POLICY.md`；
- `scripts/verify_venue_node_linux_release.ps1`；
- `scripts/verify_gateway_candidate_contract.ps1`；
- `scripts/verify_postgres_integration.ps1`；
- 交易所 adapter contract 和故障注入测试。

### 8.2 Web 分层验证

局部 UI 修改只验证客户端、对应交互和受影响视口；认证/BFF/协议变更追加边界与恢复专项。
正式 Web 发布前集中通过以下完整基线，不在每次样式或文案调整后重复全部 E2E：

```text
npm run typecheck
npm test
npm run build
npm run verify:boundary
npm run test:e2e
```

Playwright 是 M5 的硬退出门禁；`apps/venue-web` 必须提供可重复的 `test:e2e` 命令和固定浏览器安装说明，并覆盖：

- 桌面和 390×844 移动端关键页；
- 移动抽屉焦点、滚动、触控目标和横竖屏；
- SSE 断线、cursor 断层、过期 snapshot 和重新登录；
- 离线/只读状态禁止所有 mutation；
- Stop/Flatten 精确确认和重复提交幂等；
- 十进制大值、小值和本地化显示不改变提交字符串。
- 第 6.5 节全部视口、关键状态截图和分段性能报告；旧 KOL 的源码正则测试不能替代真实浏览器 E2E。

CI 还必须负向扫描 Web 产物和源码，拒绝旧 `/v1` endpoint、`TESTNET/DEMO`、交易所 secret 字段、任意 Control 代理、URL token、
浏览器端交易客户端及被直接复制的旧 service worker/session 假设。

### 8.3 每所实盘接管

- 非 `LIVE` 在网络、凭证和工件前拒绝；
- 当前账户只有一个 writer；
- 启动签名事实完整且新鲜；
- 累计名义仓位不超过 10U 初始上限；
- 同一账户多轮测试不叠加未决风险；有非零签名持仓时只执行只读、撤单、reduce-only、Stop/Flatten，不发新增风险命令；
- Canary place/cancel 或明确的产品动作完整签名收敛；
- 注入或真实 Unknown 后不重投；
- Stop 后自有订单为零，Flatten 后同代签名持仓为零；
- 旧版本不能在新账户锁持有期间启动。

## 9. 完成定义

第一批独立完成定义如下；第二批是单独排期，不再把全部六所或 Scalping 作为第一批收工门槛：

1. 六个固定 Node binary 均通过同一账户 Runtime 和 Execution Lane 进行所有生产 mutation。
2. Binance、Gate.io、Bitget 不再有 Stage 7 生产 writer 调用点；Bybit、OKX、Hyperliquid 不再有旁路 Canary writer。
3. 第一批 Grid、Copy 和手动交易只能输出规范语义意图，无法直接访问 adapter mutation；Scalping 暂缓。
4. Copy 从 leader 权威事实到 follower 签名成交/持仓、ledger 和 drift repair 形成耐久闭环。
5. Binance 通过能力契约、恢复、Unknown、Stop/Flatten、单 writer 和小额 Canary；其他五所列入第二批，不标记通过。
6. `apps/venue-web` 完成 Binance 响应式主要页面、手动下单/撤单、BFF、恢复门、关键控制和桌面/移动端验收。
7. 指定主机上的 Gateway、Control、BFF 和 Web 已完成真实连通、分段响应速度评估、关键页面截图复核和至少一轮布局/交互调整。
8. VenueFlow 明确定位为内部工具，不与 Web 维护两套用户产品逻辑。
9. 旧生产入口、重复文档入口和无调用兼容代码已按安全退出条件清理。
10. 全仓质量、二进制隔离、PostgreSQL、Web 构建和仓库卫生门禁全部通过。

第二批沿用以上共享成果，对其余五所分别完成能力差异、恢复、产品流程、真实 Canary 与 UI 连通验收。Scalping 只有另行恢复范围后才安排。

## 10. 明确不做

- 不重写已经满足契约的交易所签名和协议实现；
- 不把 `G:\kol` 的 Rust 后端、网关、数据库或多服务拓扑搬入 Venue；
- Web 允许发起下单，但 BFF、Control、PostgreSQL 不持有物理 writer，执行统一交给 Node；
- 不在迁移中同时运行两个 writer；
- 不为“六所统一”伪造交易所不支持的能力；
- 不在凭证托管方案获批前开放用户自助提交交易所 secret；
