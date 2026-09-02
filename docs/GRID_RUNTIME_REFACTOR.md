# 冻结旧运行时与未来对冲网格边界

更新：2026-09-02

## 1. 文档职责

本文只说明冻结 Grid/旧 Node 的兼容边界，以及未来若恢复网格开发时应采用的简化模型。当前唯一开发目标是
[`KOL_COPY_MVP.md`](KOL_COPY_MVP.md)；Grid、Gate.io、Bitget、其余交易所和 Scalping 均不进入本轮。

仓库现有 Account Runtime、Strategy Actor、Actor Applied、checkpoint、JSONL WAL、writer lease、canonical root、receipt、manifest、handoff
和 Stage 7 恢复代码属于已提交旧实现，不因文档调整自动获得新架构地位，也不得在没有替代和现存账户核验时删除。

## 2. 冻结旧账户的保护

- 先通过进程、配置、交易所签名订单/持仓及 `G:\Venue\artifacts` 判断账户是否仍由旧链管理，不从文件名猜测。
- 旧账户的未决 `Prepared / Submitted / Unknown`、开放订单、仓位、当前 checkpoint 和成交游标必须保留；不得为接入新 MVP 清空或伪造终态。
- 同一真实账户不得同时由旧 Node 和新 Binance Executor 下单。只有旧进程停止、未决请求完成签名对账、开放订单与仓位归属明确后，才能另行批准迁移。
- 当前 KOL MVP 优先使用没有 Venue 旧运行工件的新绑定账户，因此旧三所接管和复杂 handoff 不再阻塞产品开发。
- 冻结代码只做必要的只读核验、降险或缺陷修复，不继续扩展 authority、journal、root、receipt、lease 或恢复证明层。

## 3. 未来网格的目标模型

对冲网格应是交易所事实驱动的收敛循环，不依赖 Actor 历史状态：

```text
GridConfig + 实时 Instrument/BBO
+ 签名 Long/Short 持仓 + 本实例开放订单
-> 纯 GridPlanner 计算 DesiredOrders
-> Reconciler 比较实际与目标
-> 统一 Executor 串行撤单/挂单
-> 更新签名事实后再次收敛
```

成交私流只负责尽快唤醒计算；签名订单和双向持仓是重启与纠偏事实。断线后重新获取签名快照，不能要求回放全部历史事件。

未来实现只持久化：

- `instance_id`、配置及 `revision`；
- 无法从当前订单面推导时才保存一个最小 `rolling_anchor`；
- PostgreSQL 命令账本中的稳定 `clientOrderId`、请求状态和交易所订单 ID。

不为网格重新建立 Actor checkpoint、Applied journal、facts journal、hash chain 或恢复 manifest。

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
- 部分成交按交易所成交 ID 去重并更新当前事实；规划器依据更新后的订单剩余量与 Long/Short 仓位重新求解。
- 重启读取配置、实时规则/BBO、完整双向持仓和本实例开放订单，随后直接收敛。仍符合目标的订单保留，不无条件撤净。
- 参数修改递增 revision；旧 revision 的自有订单先按明确策略撤销或保留到安全终态，不套用新参数解释。
- Stop 停止新增风险并撤销本实例开放订单；是否平仓必须由独立明确动作决定。

## 6. 最小执行安全

网格、终端和跟单未来共用轻量 Executor，只保留不可省略的网络交易语义：

1. 同一账户命令在进程内顺序执行，并使用有界全局并发与交易所限频；
2. 发送前把命令写入 PostgreSQL，`clientOrderId` 全局稳定且有唯一约束；
3. 明确拒绝可直接终结；请求超时或响应不完整进入 `ReconcileRequired`；
4. `ReconcileRequired` 以同一 ID 查询订单和成交，确认前不重发；
5. 价格、数量、`positionSide`、只减仓语义和实时产品规则在 adapter 边界校验；Binance Portfolio Margin UM Hedge Mode 不发送其禁止的 `reduceOnly` 原生参数；
6. 原始私流 payload、API Key 和签名材料不写日志或长期工件。

这里的“统一执行入口”不是每账户进程锁、writer lease 或分布式选举。单个交易所 Executor 部署实例自然形成内部唯一入口；真实出现多实例扩容需求后再设计账户静态分片，当前不预建高可用体系。

## 7. 不再作为目标架构的机制

以下机制只可能因冻结旧代码仍被读取，不得复制到 KOL MVP 或未来网格：

- 每个账户一个进程；
- Strategy Actor、Actor turn、Applied receipt 和 checkpoint 权限；
- 本地 `commands.jsonl` 与多本 facts/delivery journal；
- canonical root、writer lease、capability promotion、不可伪造 permit；
- executable handoff、hash-chain receipt、recovery manifest；
- 用私流 generation 或历史游标作为下单权限；
- 因 UI/Control ACK 构造“已成交”或自动重投。

## 8. 将来恢复 Grid 的验收门

Grid 只有在 KOL MVP 完成后另行立项。届时至少验证：

- 根据同一签名订单/双向持仓快照重复计算得到相同目标；
- Long/Short 四类开平订单和只减仓数量正确，且各交易所原生参数合法；
- 私流断线、进程重启和重复成交不会重复滚动或误撤外部订单；
- 请求超时按同一 `clientOrderId` 查单且不盲目重发；
- 交易所规则变化、部分成交和急行情穿价均通过重新计算收敛；
- 不读取冻结 Actor/checkpoint/WAL 也能从当前交易所事实启动。

文档调整本身不迁移任何实盘账户、不删除工件、不停止旧进程，也不授权新的真实交易。
