# Binance 对冲网格重建与冻结旧运行时边界

更新：2026-09-03

## 1. 文档职责

本文定义 Binance 对冲网格迁入单例 `venue-executor-binance` 的目标契约，并继续说明冻结旧 Grid/Node 的兼容边界。
本轮只恢复 Binance Grid；Gate.io、Bitget、其余交易所和 Scalping 仍不进入新执行链。

仓库现有 Account Runtime、Strategy Actor、Actor Applied、checkpoint、JSONL WAL、writer lease、canonical root、receipt、manifest、handoff
和 Stage 7 恢复代码属于已提交旧实现，不因文档调整自动获得新架构地位，也不得在没有替代和现存账户核验时删除。

## 2. 冻结旧账户的保护

- 先通过进程、配置、交易所签名订单/持仓及 `G:\Venue\artifacts` 判断账户是否仍由旧链管理，不从文件名猜测。
- 旧账户的未决 `Prepared / Submitted / Unknown`、开放订单、仓位、当前 checkpoint 和成交游标必须保留；不得为接入新 MVP 清空或伪造终态。
- 同一真实账户不得同时由旧 Node 和新 Binance Executor 下单。只有旧进程停止、未决请求完成签名对账、开放订单与仓位归属明确后，才能另行批准迁移。
- 当前 KOL MVP 优先使用没有 Venue 旧运行工件的新绑定账户，因此旧三所接管和复杂 handoff 不再阻塞产品开发。
- 冻结代码只做必要的只读核验、降险或缺陷修复，不继续扩展 authority、journal、root、receipt、lease 或恢复证明层。

## 3. 新网格的目标模型

对冲网格应是交易所事实驱动的收敛循环，不依赖 Actor 历史状态：

```text
GridConfig + Instrument 规则/参考价
+ 签名 Long/Short 持仓 + 本实例开放订单
-> 纯 GridPlanner 计算 DesiredOrders
-> Reconciler 比较实际与目标
-> 统一 Executor 串行撤单/挂单
-> 更新签名事实后再次收敛
```

认证成交私流负责去重并立即唤醒计算，但不单独授予下单权。策略启动先订阅用户流，再取得签名 REST 基线；连续用户流维护订单、成交和双腿库存，正常运行不定时拉全账户快照。热路径使用认证成交价和缓存规则，不依赖 BBO；冷路径的标记价用于初装、库存阈值和盈利候选判断。明确断流先禁止热派发，重连缺口、持续缺失的配套事件或不确定请求才做签名恢复。PM 权益不随心跳续期，仅盈利减仓候选单独签名核验，失败不阻塞普通滚动。签名订单和双向持仓仍是重启及纠偏事实，不要求回放全部历史事件。

新实现只持久化：

- `instance_id`、配置及 `revision`；
- 无法从当前订单面推导时才保存一个最小 `rolling_anchor`；
- PostgreSQL 命令账本中的稳定 `clientOrderId`、请求状态、交易所订单 ID，以及 Grid 原子批次的 ID 与派发顺序。

不为网格重新建立 Actor checkpoint、Applied journal、facts journal、hash chain 或恢复 manifest。旧 WAL、checkpoint
和 Actor 状态不导入新网格，也不决定新网格的初始订单面。

### 3.1 重布网络，不接管旧网络

迁移按“清除旧网络、保留真实仓位、重新规划”执行：

1. 旧 writer 停止产生新命令；
2. 使用旧订单归属或签名订单身份精确撤销旧实例开放订单；
3. 签名确认旧实例订单已不存在，且没有可能已发送但结果未知的 mutation；
4. 读取最新双腿仓位、账户权益、规则和参考价；
5. 新实例以新 `instance_id / revision / rolling_anchor` 计算完整目标并通过统一 Executor 挂单。

旧订单不被新实例采用或改名；旧仓位是交易所事实，继续作为新规划器的库存输入。旧 WAL 只在旧 writer 尚未完成
第 2–3 步时用于确定其未决请求，绝不迁入 PostgreSQL 或新运行时。签名事实不能排除旧请求仍可能生效时，迁移失败关闭。

## 4. 双向持仓与订单归属

Hedge 网格的主要业务复杂度只有双向持仓和滚动更新。每张订单必须显式属于以下一种语义：

- Long Open：买入开多；
- Long Close：卖出平多，领域语义必须只减多腿；
- Short Open：卖出开空；
- Short Close：买入平空，领域语义必须只减空腿。

`clientOrderId` 使用交易所长度允许的紧凑编码绑定 `instance_id / revision / position_side / open_or_close / level / sequence`；
完整映射保存在 PostgreSQL。系统只撤改可证明属于本实例的订单，外部订单只展示或告警，不按价格猜归属。

平仓数量按最新签名持仓和已挂平仓量向下裁剪；开仓数量继续遵守实时数量步长、最小名义价值及产品风险上限。

## 5. 滚动和重启

- maker 成交或签名订单面变化只触发一次重新计算；新目标与实际订单做差异，不固定为某种历史批次结构。
- 每一张 Maker 订单完整成交都立即、独立推进一次滚动，不等待第二张成交。普通不变拓扑下，一笔完整成交自然得到 2 Place + 1 Cancel；两笔在首笔后最多 1 ms 的同代微批中自然得到 4 Place + 2 Cancel。该数量是目标面差异的结果，不是跳过重算的硬编码模板。
- 部分成交按交易所成交 ID 去重，只累计成交量、剩余量与库存；订单未完整成交时不得提前滚动。规划器依据更新后的订单剩余量与 Long/Short 仓位重新求解。
- 重启读取配置、实时规则/参考价、完整双向持仓和本实例开放订单，随后直接收敛。仍符合目标的订单保留，不无条件撤净。
- 参数修改递增 revision；旧 revision 的自有订单先按明确策略撤销或保留到安全终态，不套用新参数解释。
- Stop 停止新增风险并撤销本实例开放订单；是否平仓必须由独立明确动作决定。

### 5.1 网格运行基石

以下行为必须在同一迁移中闭合，不允许以“后续补充”替代：

- 初装同时计算 Long Open、Long Close、Short Open、Short Close 四类 Maker 订单；平仓总量扣除本实例及其他来源已预留的平仓量；
- 每批新成交先按 `(account, native_symbol, native_trade_id)` 去重，再更新以新鲜签名基线为锚的认证增量投影并只重算一次；同一轮同时成交两单不得重复补单、漏撤单或按到达顺序产生不同结果；签名 REST 在热路径外补漏与验证；
- 滚动不再硬编码“补二撤一”网络批次，而是对 `DesiredOrders` 与本实例实际开放订单求差，输出 `Keep / CancelExact / PlacePostOnly`；
- 库存低于配置阈值时进入 `Replenishing`，先停止普通增险并撤本实例冲突订单，再通过统一命令账本补充明确的 Long/Short 库存；签名仓位确认后以补充成交价或新鲜标记价重新锚定；
- 库存名义价值超过配置倍数且该腿浮盈达到阈值时产生只减仓命令；发送前按最新签名腿及全部未成交平仓预留量向下裁剪，成交确认后重新布网；
- 行情或私有事实过期时只进入 `Blocked` 且不产生 mutation；规则变化、订单归属冲突、目标重复、价格穿越、长期不收敛或连续失败达到配置阈值时才进入 `ResetRequired`；Reset 只撤本实例订单，取得新鲜签名事实后递增 revision 并重新布网；
- 私流增量只在连续认证覆盖内推进投影，不改变签名事实的重启权威性；真实断流、乱序和持续缺口关闭热路径并按同 ID 恢复，同时成交正常合批。无成交的空闲不视为故障，连接覆盖时间只能来自真实帧/Pong；
- 明确交易所拒单以当前配置代命令账本中首次拒单的终态时间为起点，30 秒后开始 Reset；后续拒单、计划变化、短暂恢复与进程重启不得刷新或取消该期限。等待期间正常处理成交与补撤，不因拒单次数提前重置；事实缺失、归属冲突和未知请求仍遵守各自安全门。实际 Reset 从进入撤单阶段重新计算收敛超时，签名确认撤净且无未决命令后清除该计时再布网，不把已完成撤净误判为超时。历史拒单保留在旧配置代，不触发新网络反复重置；
- Pause 清空目标面，只撤本实例自有开放订单且绝不挂新单；签名确认订单面为空且命令终态后保持 Paused。Resume 递增计划并重新布网，不沿用暂停前 clientOrderId。Stop 撤本实例订单且不平仓；Flatten 必须是独立、显式且二次确认的动作。

补库存与盈利减仓都是会产生物理交易的显式策略配置，不使用旧实现硬编码的 5/15 USDC、3 倍、5% 或 30%。默认值、
启用状态和上限进入版本化配置；任何缺失、非正数、过期事实或规则不一致均失败关闭。

## 6. 最小执行安全

网格、终端和跟单未来共用轻量 Executor，只保留不可省略的网络交易语义：

1. 同一账户命令在进程内顺序执行，并使用有界全局并发与交易所限频；
2. 发送前把命令写入 PostgreSQL，`clientOrderId` 全局稳定且有唯一约束；
3. 明确拒绝可直接终结；请求超时或响应不完整进入 `ReconcileRequired`；
4. `ReconcileRequired` 以同一 ID 查询订单和成交，确认前不重发；
5. 价格、数量、`positionSide`、只减仓语义和实时产品规则在 adapter 边界校验；Binance Portfolio Margin UM Hedge Mode 不发送其禁止的 `reduceOnly` 原生参数；
6. 原始私流 payload、API Key 和签名材料不写日志或长期工件。

Grid 成交热路径必须把成交分配、rolling anchor、完整 desired surface、订单归属及本轮全部命令放入同一个 PostgreSQL 事务，并在提交成功后唤醒账户队列；同账户的 KOL 规划只能在 Grid 返回该事务的成功确认后开始，确认失败、关闭或超时都必须退役私流 worker 并走签名恢复。即使计划变化但差异为零，也写入 0 命令批次收据，使 anchor/desired/fill 的提交仍可去重审计。一笔完整成交通常产生 2 Place + 1 Cancel；两笔同代成交若落入首笔后最多 1 ms、最多 5 笔的有界微批，通常产生 4 Place + 2 Cancel。两者都使用 1–16 的 `dispatch_sequence` 保证全部 Place 先于 Cancel；事务失败不得留下半批，也不得为凑成两笔而等待。预热态从认证事件收到到事务提交并完成 Executor 唤醒的 p95 必须不超过 10 ms，交易所发送和 ACK 不计入该指标。

执行出网是独立验收，不能用上述提交与唤醒指标替代。Executor 必须记录认证事件接收至首个/末个 transport send-entry 的时延；同一微批先预备并预签全部子命令，所有 Place 并发越过 send-entry 后才并发启动 Cancel，随后逐命令处理 ACK 与签名回读。若批次尚未耐久保存触发事件接收时间，或热路径仍同步执行时钟、exchangeInfo 与完整账户 REST 快照，则该出网 p95 不得宣称满足 10 ms；应复用提交事务已 CAS 验证的同代私有投影与规则热缓存，缓存缺失或重启只走保守签名恢复冷路径。

`0023_binance_grid_hot_batch.sql` 提供最小批次收据、Grid 必填批次字段、历史 Grid 单命令 legacy 批次回填、非 Grid `NULL/NULL` 约束和索引；`0024_binance_grid_batch_chain.sql` 记录输入 desired 摘要、唯一前驱和实例批次尾。跨微批第二笔成交按首批投影后的目标面独立重算并先持久化，后批只有在前批命令全部 `Reconciled` 后才能领取；未确认 Place 只参与规划占位，不能充当签名事实或撤单目标。认证私流成交的 socket 连接代、签名 baseline 代与订单进度上下文只能全空或全有效，两种 generation 不得互相替代，历史签名成交保持全空。当前代码已接通 Store 原子批写、提交唤醒、同批领取、一次性热令牌、Place send-entry 屏障和事件至 send-entry 指标；没有 PostgreSQL 实库压力结果、真实 Binance 小额 Canary 与预热 p95 样本前，仍不构成 10 ms 验收通过。

这里的“统一执行入口”不是每账户进程锁、writer lease 或分布式选举。单个交易所 Executor 部署实例自然形成内部唯一入口；真实出现多实例扩容需求后再设计账户静态分片，当前不预建高可用体系。

## 7. 不再作为目标架构的机制

以下机制只可能因冻结旧代码仍被读取，不得复制到 KOL MVP 或未来网格：

- 每个账户一个进程；
- Strategy Actor、Actor turn、Applied receipt 和 checkpoint 权限；
- 本地 `commands.jsonl` 与多本 facts/delivery journal；
- canonical root、writer lease、capability promotion、不可伪造 permit；
- executable handoff、hash-chain receipt、recovery manifest；
- 单独用私流 generation 或历史游标作为下单权限；
- 因 UI/Control ACK 构造“已成交”或自动重投。

## 8. Binance Grid 迁移验收门

完整迁移至少验证：

- 根据同一签名订单/双向持仓快照重复计算得到相同目标；
- Long/Short 四类开平订单和只减仓数量正确，且各交易所原生参数合法；
- 私流断线、进程重启和重复成交不会重复滚动或误撤外部订单；
- 请求超时按同一 `clientOrderId` 查单且不盲目重发；
- 交易所规则变化、部分成交和急行情穿价均通过重新计算收敛；
- 不读取冻结 Actor/checkpoint/WAL 也能从当前交易所事实启动。
- 同一快照内两笔成交、部分成交、重复成交、成交与撤单交叉到达均只产生一组确定性差异；
- 单笔完整成交不等待配对即产生一次确定性滚动，普通拓扑为 2 Place + 1 Cancel；部分成交不提前滚动；
- 同轮两笔成交的典型 4 Place + 2 Cancel 在一个事务中全有或全无，持久顺序和实际领取都保持 Place 在 Cancel 之前，预热 p95 热路径满足 10 ms；
- 补充 Long、补充 Short、单腿过量盈利减仓、减仓后重布和补充失败后的恢复均由签名事实闭环；
- 网格运行时手动开多、平多、开空、平空与精确撤单可用；网格不撤外部、手动或其他实例订单；
- 同账户多个实例共享单一账户串行执行入口，全部平仓预留合计不超过签名实际仓位；
- Stop、Reset 和进程重启后只靠 PostgreSQL 配置/归属/命令与交易所事实恢复；
- 切换演练证明旧网络已撤、新网络重新生成，中间不存在双 writer 或重叠网络。

### 8.1 旧迁移代码删除门

失败的 `apps/venue-node` Grid/Actor/GridBridge 迁移不再保留为候选入口。只有同时满足以下条件才删除其运行代码：

- 新 planner、reconciler、网格存储、统一 Executor 和 Control/UI 生命周期入口已通过上述专项；
- 所有生产账户均不运行 `venue-node`，部署清单、服务单元和发布脚本没有 Node Grid 入口；
- 旧实例已停止，旧自有订单签名确认为零，`Prepared / Submitted / Unknown` 已明确终结；
- 新网格经过离线回归、重启测试、故障注入和真实小额 Canary，且保留可执行回滚发布包；
- 旧工件的只读核验器已与 mutation 代码解耦。

删除包括 Node 内 Grid/Manual/Copy mutation bridge、Actor/checkpoint/WAL 运行组合及对应发布入口；不改写或删除已应用的 SQL
migration。`G:\Venue\artifacts`、旧 WAL/checkpoint/成交游标只能在旧账户全部收敛、留存边界另行核准后清理。

文档调整本身不迁移任何实盘账户、不删除工件、不停止旧进程，也不授权新的真实交易。
