# VENUE 三目标交付与统一迁移契约

更新：2026-09-01。基线 v0.1.0-alpha.2；实施时重新核对 HEAD、部署版本与账户事实。

## 1. 唯一任务范围

本文合并原“剩余开发与验收”，定义下一次长期任务的范围、验收和提示词。阅读或整理文档不自动开始实施、部署或交易；用户明确启动 Goal 后才执行。

| 目标 | 本次交付 | 不自动扩大到 |
|---|---|---|
| A 交易终端可用 | Binance：VenueFlow 桌面交易主流程，Web 基础下单/撤单，真实账户及订单事实回显 | 将整个专业桌面终端复制到 Web、额外订单类型、全部六所终端 |
| B 跟单业务可用 | Binance 优先；至少一组真实、不同账户的 leader/follower；Web 配置、自动规划、执行、账本和漂移闭环 | 公众多租户、收费、邀请、全六所跨所组合矩阵 |
| C 旧三所接管 | Binance → Gate.io → Bitget，逐家统一 Node/Runtime/Lane/WAL 接管与实际验收 | Bybit、OKX、Hyperliquid 新一轮实盘和 Scalping |

最新“三目标”取代此前“只有 Binance、其余五所不进入本轮”的排期：Binance 仍先行，Gate.io、Bitget 接管也进入本次目标。后续三所保留代码；Scalping 暂缓，不开发、不开放自动入场，不作为收工门槛。

只修阻碍三目标的缺口。复用 Rust workspace、既有网关/指标与 Next.js/React/TypeScript、egui，不另造网关、数据库、服务拓扑、公共 SDK，不批量升级依赖。

- 当前实现和停用状态：[架构](ARCHITECTURE.md)；代码定位：[CODEMAP](CODEMAP.md)。
- 风险、热路径、Owner、WAL 与恢复：[运行时契约](GRID_RUNTIME_REFACTOR.md)，涉及此类代码时完整阅读。
- 构建、测试和合并：[开发指南](DEVELOPMENT.md)；应用配置：[Node](NODE.md)、[Web](WEB.md)、[账户管理](ACCOUNT_MANAGEMENT.md)。

## 2. 操作授权与安全边界

### 2.1 持续实盘授权

用户已有授权：使用已提供的真实账户，在 binding、实时规则与能力允许范围选择交易对，单笔名义价值不超过 10 USDT 等值；允许同一账户多轮测试，也可能有已有持仓。授权明确包括 A/B/C 所需的受控实盘验收：终端下单/撤单/回读、不同账户的跟单闭环，以及 Binance、Gate.io、Bitget 的逐家 Canary、重启后签名收敛。用户启动实施后，无需为这些常规已授权动作反复人工确认，可自行提交 CLI 技术确认参数。

当前实现仍有更严格的账户累计 10U、已有签名仓位/增险挂单时拒绝继续增险等门禁。它不是累计测试次数额度，也不能被“单笔授权”自动覆盖。启动时核实实际代码与配置，本次文档整理不修改风险政策。若网格多笔最小名义、已有持仓或跨零测试与这些门冲突，保留门禁，完成可行的离线/只读/降险部分，列明所需政策决定；不能把全部拒绝交易当作“终端可用”。

账户估值包含全部 symbol 的绝对签名持仓、未撤增险订单、Submitted/Accepted/Unknown 最坏预留和候选命令；使用实时规则、新鲜保守汇率，不按稳定币 1:1 换算。单笔上限不能替代账户风险门。

- 所有语义动作统一经 Node 的 Runtime → Execution Lane → risk/Owner → 同一 WAL → 唯一账户 writer → adapter。
- 每个真实账户只有一个版本持有 writer。leader/follower 可分别运行独立账户进程，leader 事实采集只读；新增测试 mutation 由主会话串行安排，子任务不得自行交易。不是禁止不同账户存在各自的进程锁。
- Unknown 持久化并冻结该账户新增风险，签名订单/成交/仓位收敛前不重投；撤单和有签名依据的 reduce-only 降险仍走原链。
- 先识别已有仓位归属与恢复事实，不为通过测试清掉无关仓位。每轮核对订单/仓位/WAL，重复测试前重新验证风险，不叠加未决风险。
- 不创建账户/API Key，不修改 API 权限、安全、杠杆/保证金模式，不提款/划转，不输出 secret，不伪造持仓、签名事实、前驱记录或 capability。
- 凭证仅用已有受控环境/根 .env；已批准的 Control 密文托管和 Windows 登录凭证库遵循账户文档，不向浏览器下发交易所秘密。
- 保留未决 WAL、Unknown 事实、成交游标和当前 checkpoint；5 MiB 轮转、10 MiB 单文件、256 MiB 工件根预算不变。

### 2.2 无人值守与阻塞处理

一个账户阻塞不阻断独立的代码、离线和只读工作。权限、凭证、资金、账户模式或外部审批不足时，记录证据、账户状态和最小人工动作，完成其他可执行项后统一汇总。工具/环境安全拒绝不得换路径绕过。

用户离开不等于无限重试或无限交易。故障先用离线 fixture 注入，不在有仓位的真实账户上主动制造断网/崩溃来碰运气，不用额外订单凑性能样本。实测未覆盖项保持未完成。

收工前停止本任务新增测试入场、撤销测试自有未成交入场单并签名核对。残仓需要保留 custody/恢复/降险能力时，按运行时契约处理，不直接杀进程或恢复旧 writer；列出仍运行服务和账户状态，不让测试策略无限交易。

Goal 是会话的持久目标，不是无限后台服务；预算、权限、网络或机器中断可能停止续行。不能承诺“一晚必成”，不能因预算用完就把目标标完成。

<a id="goal-acceptance"></a>

## 3. 三目标验收

### A. 交易终端可用

已有 Trade Dock、账户中心、图表/指标、执行事实视图、Windows 凭证库及 Node 手动限价桥。当前 Copy binding 拒绝通用手动 turn，Grid desired 外撤单受限，Web 缺完整基础下单流程；先补真实缺口，不重写整套终端。

1. 实际登录、账户验证、选择执行目标可用；区分 API 可达、Node 在线与交易准入。存在可用的手动执行 binding，不要求用户先启动会自行交易的自动策略。
2. 桌面完成选币/选价、基础币或报价金额输入、LIMIT/GTC 下单、精确撤单、reduce-only 平仓；订单/成交/仓位由更新签名事实回显。
3. Web 基础下单/撤单复用 TradeIntent 和 BFF/Control 命令入口，价格、Decimal 字符串、幂等身份、账户/实例/config epoch 与桌面一致。
4. “撤全部”须证明准确 scope；外部单或自动策略订单不在支持范围时明确拒绝或缩小按钮语义，不能假报全部成功。手动与自动策略不争同 symbol，不绕过 Owner。
5. 断线、陈旧投影、过期会话、Unknown、重复点击/热键和切换账户不会误发/重发，恢复后重新验证绑定和事实新鲜度。
6. 至少完成获准真实 place、签名回读、cancel/成交、更新仓位回显；需要成交的证据不能用 ACK 代替。桌面实际启动检查，Web 按第 6 节截图和测量。

### B. 跟单业务可用

已有纯规划、relation/job、双边事实投影、Copy Actor 物理桥、execution evidence、ledger/drift 和未领取过期任务处理。目标是实际部署连续闭环，不是手造 fixture 或往数据库塞任务。

1. Web 创建/查看/暂停/恢复/修改一组 relation；leader/follower 是不同真实账户，资本和倍率显式配置，不自跟单、不拿总权益冒充策略资本。
2. 已运行 Node 的新鲜签名观察自动进入规划和耐久 job，经 follower 同一 risk/WAL/writer 执行，签名成交/仓位进入 ledger/drift 和 Web；全链能按同一 job/request 身份追踪。
3. 加仓、减仓、跨零目标与当前持仓一致；跨零先 reduce-only 到零，再依新鲜签名事实和仍有效任务考虑反向，原 child 不因重启重算。
4. revision/暂停、过期、重复 delivery、Unknown、重连/重启、漂移修复不造成重复跟单或旧意图复活。故障边界离线覆盖，正常链以真实账户验证。
5. SemanticApplied、Accepted、Reconciled 分开显示；中间归零不是最终跟单成功，目标/实际/方向/漂移对应同一签名事实。
6. 至少一组真实关系完成受控周期并对账；缺第二个可用账户、资本或 leader 权限则目标未完成，不能用同账户双 Key 代替。

Copy 持久化契约在附录 A。旧三所接管不自动代表全部跨所 Copy 组合已通过；当前不追加该矩阵。

### C. 旧三所接管

六个固定 Node 和旧三所根生产 binary 退休已有源码基础；Binance/Gate/Bitget 的合法 legacy-v1-handoff 仍是现有启动条件。每所分别取得：

1. 主机、源码/release hash、真实旧进程/服务、账户 binding、锁、旧恢复根和未决 WAL 清单；签名事实完整、新鲜。
2. 旧 writer 安全停止、锁释放，Prepared/Submitted/Unknown 收敛；真实开放单/仓位归属准确，新链恢复兼容通过，前驱记录不伪造。
3. 唯一 Host/Lane/WAL 承担生产 place/cancel/reduce；订单族完整读取或明确不支持，账户模式与原生数量单位正确。
4. Grid 成交优先、部分成交隔离、post-only 拒绝、缺单重建、Stop/Flatten、Owner 与恢复满足运行时契约，不只测纯 reducer。
5. 当前 release 的小额 Canary、签名收敛、受控重启检查及 Node/Control/UI 连通证据；历史测试不代替当前接管。
6. 唯一明确的启动/停止/恢复入口及当前服务状态，旧入口不可误启；必要兼容读取继续保留，不以删净旧代码为硬目标。

C 只有三所分别通过才完成。一所阻塞时继续其他独立工作，但不把 C 改名成“币安接管完成”。

## 4. 执行顺序

| 阶段 | 交付 | 并行边界与放行 |
|---|---|---|
| P0 启动核对 | 实际缺口、两账户可用性、风险门、SSH/PG/构建资源、唯一执行者 | 只读定位；先确定哪些实盘证据可取得 |
| P1 Binance 基线 | 合法接管及最小手动执行闭环 | UI、Copy 可做不重叠文件的离线实现；公共协议先定稿 |
| P2 A/B 收口 | 桌面/Web 基础交易、至少一组真实跟单 | 主会话整合后串行实际验收，子任务不自行实盘 |
| P3 C 收口 | Gate.io、Bitget 依次接管 | 下一所可只读/fixture 准备；前一所测试入场停止且签名核对后再继续 |
| P4 交付 | 源码、截图/性能/验收、干净主线、服务状态和协助清单 | A/B/C 分别有真实证据，不用跳过代替通过 |

缺陷按影响面验证，不为文案/样式重复全业务回归。只修当前三目标的阻断问题，其他发现记简短后续项，不升级成新架构任务。

## 5. 会话与模型

建议一个新的主会话，选择 G:\Venue 项目，建立一个 Goal。不是三个独立会话各自修改共享链、合并和交易；子代理由主会话调度并返回结果，用户不用往返传话。

| 职责 | 建议模型/推理 | 修改与操作边界 |
|---|---|---|
| 主协调、执行链、集成和接管 | gpt-5.6-sol / high | 共享协议、risk/WAL/Owner、合并、所有实盘操作 |
| UI 子任务 | gpt-5.6-terra / high | 分配的桌面/Web 文件、交互与测试 |
| Copy 子任务 | gpt-5.6-terra / high | 分配的 Copy planner/Control/Node Copy 文件及离线验证 |
| 关键安全差异复核 | gpt-5.6-sol / xhigh | 按需替换已完成子任务，只读审查 Unknown/恢复/归属 |

默认一主两子，最多同时四个代理；构建仍最多两个，不另开缓存。一个文件只有一个编辑者，DTO、migration、Cargo.lock、文档统一由主会话整合。无子代理能力时主会话串行执行，不擅自另建三个用户会话。

terra-high 能承担明确边界的 UI/业务/测试工作；把三目标交给它独跑并不能省掉安全复核。以上是工程建议，不是成功率、价格或一晚完成保证；不常态使用 max/ultra。关键差异必要时 xhigh，常规实现 high。

依据：[官方子代理说明](https://learn.chatgpt.com/docs/agent-configuration/subagents) 明确独立任务并行、写入冲突和额外 token 成本，terra 偏速度/效率。本机工具目录提供 sol/terra high 与 sol xhigh；[官方安全扫描说明](https://learn.chatgpt.com/docs/security/plugin/workbench#start-a-scan) 推荐 sol/xhigh 用于安全扫描，不是对本项目的性能保证。

## 6. 验证、部署和收工

- Rust 按 [开发指南](DEVELOPMENT.md) 的 guard 与影响面验证；公共契约/依赖或最终发布集中建立完整基线，后续增量不重跑无关测试。
- PostgreSQL 使用隔离测试库/schema，确认真正执行而非缺配置返回；不要把生产 .env 当可清空测试库。应用可用已授权数据库配置，但不覆盖业务数据。
- Web 发布基线在 apps/venue-web 执行 typecheck、unit、build、boundary、E2E；Next build 不等于 lint。桌面必须真实启动，不用浏览器截图冒充桌面验收。
- Web 视口：390×844、844×390、768×1024、1440×900、1920×1080；检查关键页正常/空/加载/错误/离线/陈旧/重连和确认状态。桌面检查常用窗口及缩放，修复遮挡、表格溢出、焦点和误触。
- 常用任务不超过三次主要导航，触控目标至少 44×44 CSS px，手机不依赖 hover；账户/交易对/方向/数量/高风险动作可辨认。保留现有设计系统，不做无关换肤。
- 分段测量 snapshot 首次可用、BFF/Control、Node→SSE→渲染、命令入队、WAL/dispatch、交易所 ACK、签名收敛回显；报告环境、样本数和 p50/p95/p99。少量真实订单不能给出可靠尾延迟，不为凑样本频繁实盘。
- 既定目标：Web 首次 snapshot p95 ≤2.5s，loopback BFF 额外 p95 ≤100ms，已连接页面投影到渲染 p95 ≤500ms；热路径路由 <5ms、dispatch 启动 <20ms（不含交易所网络）。未达标定位修复本地问题，外部限制保留证据，不偷偷改门槛。
- 主机 45.77.253.180：先只读核验身份、目录、服务、端口、binding、writer/WAL 和容量，只管理本任务 Venue 服务。免密 SSH 不是永久可用保证。
- Ubuntu 默认本机 scripts/Build-VenueUbuntu.ps1 编译，G:\Build\Venue\ubuntu 专用根并复用 slot-2。版本化上传、核对 SHA256/ELF/动态库，不在弱服务器日常编译，不覆盖活动 release。
- Node/Control/BFF 的 loopback 限制保留。先打通服务器同机链路，再经受控 HTTPS 或已有安全通道供本地 UI 使用，不把 Control 改成公网裸监听。
- 截图、trace、脱敏性能/验收报告放 G:\Build\Venue\venue-web-qa 的本次固定子目录，不进 Git、不写交易恢复 artifacts；一个可更新的验收索引即可，不再累计长期进度文件。

最终按 A/B/C 分别报告：已验收/未完成/阻塞，附代码 commit、实际应用入口、验证范围、截图与报告位置、账户及服务状态。只有真实必需证据齐全才标“可用/接管完成”。完成获准改动后提交并安全合并到 G:\Venue，不自动 push 或发布远端版本标签。

<a id="start-prompt"></a>

## 7. 可复制的启动提示词

在新会话选择 G:\Venue 项目，主模型 gpt-5.6-sol / high；以下提示词供用户启动时复制，不在文档整理阶段执行：

~~~text
请在本会话建立并执行一个长期 Goal：完成 G:\Venue 的“交易终端可用、跟单业务可用、旧三所接管”，以 docs/UNIFIED_GATEWAY_WEB_MIGRATION.md 的 A/B/C 验收为完成标准，不要只交付计划。

先读 AGENTS.md、CODEMAP.md、迁移契约和完整运行时契约，核对当前代码/服务/账户，复用已有成果。先 Binance 终端与真实跟单，再 Gate.io、Bitget 串行接管。Scalping、Bybit、OKX、Hyperliquid 及收费/多租户不在本轮。

明确允许子代理：UI、Copy 有界实现用 gpt-5.6-terra/high；主会话负责共享契约、合并和全部实盘操作，关键安全差异可用 gpt-5.6-sol/xhigh 只读复核。分配文件所有权，最多四个代理、两个受控构建，不另开三个独立用户会话。

按文档既有授权直接进行 A/B/C 所需的小额受控实盘验收，单笔不超过10U；不绕过现有更严格风险门，不清掉无关已有仓位，不双 writer，Unknown 不重投。先离线验证和签名预检，再串行实盘；Ubuntu 本机编译上传，不在弱服务器日常编译。

完成实际 UI 启动、截图检查及必要布局修复，测量真实分段延迟。fixture、ACK、跳过的数据库测试不能冒充生产通过。每个增量只跑受影响测试，最终发布集中验证，提交并安全合并主线。

持续完成仍可安全执行的任务，不为常规已授权确认等待我；真正外部权限或风险政策阻塞则保持失败关闭和恢复事实，完成其他独立任务后汇总最小人工协助。不要绕过工具拒绝、扩大范围或无限制造测试订单。收工前停止新增测试风险并记录残仓/custody和服务状态。三个目标分别给证据，未满足验收不能标完成。
~~~

Goal 应显示为已建立的持久目标，而不只是普通回复里的“我会继续”。[官方 Goals 指南](https://developers.openai.com/cookbook/examples/codex/using_goals_in_codex) 说明跨回合续行与预算/权限阻塞边界，不需要再用周期自动化重复发“继续”。

离开前确认机器供电/唤醒、网络/SSH、额度、浏览器和必要权限就绪；本轮不更改系统电源或权限。额度用完、机器休眠或工具拒绝都可能中断。[官方提示词建议](https://developers.openai.com/api/docs/guides/model-guidance?model=gpt-5.6#prompting-best-practices) 强调精简重复指令、定义授权和完成证据，本提示词引用共同契约而不再复制安全规则。

## 附录 A：Copy 持久化与恢复契约

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
