# 多交易所策略与网格统一入口

同一策略可分别运行于 Binance、Bitget、Bybit、Gate.io、OKX、Hyperliquid 的独立账户。六所共用现有 `venue-executor-binance` 单例；binary 保留历史名称，不为交易所或策略另启动进程。Binance KOL 的复制范围不变，其他五所提供独立策略命令和网格入口，不提供跨所原子成交。

## 代码与持久化边界

| 职责 | 入口 |
|---|---|
| 规范命令入账、幂等、账户顺序、nonce、原身份对账 | `apps/venue-control/src/multi_venue_store.rs` |
| 同进程账户调度，复用最多 32 个网络任务许可 | `multi_venue_runtime.rs` |
| 凭证 AES-256-GCM、真实账户身份、签名快照 | `multi_venue_credentials.rs` |
| 五所物理网关组合，协议保留在各 adapter | `multi_venue_exchange.rs`、各 `venue-gateway-*/src/durable_execution.rs` |
| 新鲜行情、精度、单笔与交易对名义金额上限 | `multi_venue_risk.rs` |
| 网格配置、生命周期、订单归属、累计成交游标与目标面 | `multi_venue_grid/{store,runtime,planner}.rs` |
| 共用网格数学、滚动、库存补齐、盈利减仓 | `crates/venue-strategies/src/hedged_grid/planner.rs` |
| 发送边界、签名减仓预检、撤单原命令上下文 | `crates/venue-execution/src/durable_gateway.rs` |
| 数据库增量迁移 | `0035_multi_venue_executor.sql`、`0036_strategy_grid.sql` |

表内省略目录的 Control 文件相对 `apps/venue-control/src/`；迁移位于 `apps/venue-control/migrations/`。新链以 PostgreSQL 为耐久边界，不依赖旧 Host、Actor、writer lease、WAL 或 checkpoint。配置、目标面、成交消费游标与本轮命令在同一事务提交；崩溃后不靠删除本地工件恢复。

## 命令与账户语义

| 功能 | 规范命令 | 关键约束 |
|---|---|---|
| 普通限价、仅限 maker | `PlaceLimit`，GTC / PostOnly | 精度与名义金额校验；PostOnly 不降级为 taker |
| 市价增仓 | `PlaceMarket` | 明确方向、单笔及总敞口上限、新鲜签名持仓和行情 |
| 市价减仓 | `MarketReduce` | 原腿位、代次和数量；发送前扣除普通平仓挂单预留 |
| 止损、止盈 | `StopMarketFullPosition` | `owner.purpose` 分别为 `protection`、`take_profit`；标记价触发、冻结数量、只减仓 |
| 精确撤单 | `Cancel` | 账本加载原命令、原 client ID 与原生 ID；禁止跨策略、账户或 run 撤单 |

四家中心化交易所要求各 adapter 支持的永续合约双向持仓账户；Bitget 使用 UTA v3。Hyperliquid 使用主账户/API Wallet 的 Net 持仓：网格必须显式指定 `net_direction` 为 `long` 或 `short`，只生成选定方向；已有反向净仓会阻止增仓，不用另一方向下单冒充双向持仓。

Bitget 原生带单凭证在耐久密文中使用显式 `bitget_copy` 类型，普通 `bitget` 保留原 UTA 契约；两者均复用 UTA v3 物理执行。带单绑定和增仓发送读取该 Key 的签名 `copy/futures/trading-pairs`，不以全球合约目录代替带单范围。带单凭证要求 `uta_trade`、`copy_futures_order`、`copy_futures_position` 读写且无提现权限，仍读取真实账户设置与完整签名快照；缺少设置读取能力时拒绝准入，不默认假设 Hedge。普通账户与带单账户沿用签名 UID 唯一身份约束，不能用 Key 哈希或人工 scope 后缀伪造独立账户。

桌面通过本人认证的 `POST /v2/account/bitget-copy-credentials` 提交备注、Key、Secret 和 Passphrase，保存即完成签名准入；不创建 Binance KOL 关系。绑定使用 BTC/USDT 作为只读快照锚点并验证带单范围非空，不要求实际交易 BTC。马丁启动前验证全部配置交易对的带单资格，增仓发送前再次验证当前交易对。此接入须取得目标专用 Key 的签名验证及策略真实订单事实后才可视为实盘可用。

市价语义遵循原生市场规则；Hyperliquid 以带价格保护的 IOC 执行，可能部分成交或未成交。精确查单的累计成交量与终态才是结果，入账、HTTP ACK、订单存在都不等于足额成交。数量统一为基础币数量，合约张数转换仅在 adapter 内；不隐式扩大最小数量或把拒绝的 maker 单改成市价。

条件单同时保留 SL/TP 意图、触发方向、mark 触发类型和原条件单身份。恢复和撤单使用原条件订单端点；已触发时查其子单，不能因条件挂单列表里消失而重发。未证明终态的请求保持 `ReconcileRequired`，暂停该账户后续新命令。

金额上限按交易对自身报价资产计价，发送前将持仓及现有增仓挂单计入；行情计价不等于成交价格保证。策略须显式设置上限，不假设 USDC、USDT 或其他资产等值。SL/TP 可分别保护同一库存，但都必须具备原生减仓保证；普通平仓挂单仍占用可减仓数量。

Bitget 普通 Key 与带单 Key 可能返回相同 UID。当前账户排他按交易所签名身份执行；Key 类型、权限列表或 Key 指纹不能单独证明独立资金账户，也不能用于绕过已有策略占用。同一身份已有运行策略时，带单绑定与马丁启动不得自动停旧策略或替换其凭证。

## 操作入口

`venue-strategy-admin` 与策略代码调用相同的 Store。数据库地址来自 `VENUE_STRATEGY_ADMIN_DATABASE_URL`，密钥来自 `VENUE_ACCOUNT_MASTER_KEY`。`migrate` 使用迁移角色；其他命令使用相应最小权限角色。凭证只经 stdin 进入，不能放在命令行、仓库配置、日志或浏览器响应中。

```text
venue-strategy-admin migrate
venue-strategy-admin probe EXISTING_ACCOUNT_UUID BTC/USDT
venue-strategy-admin bind USER_ID EXISTING_ACCOUNT_UUID BTC/USDT LABEL
venue-strategy-admin bind-released USER_ID EXISTING_ACCOUNT_UUID BTC/USDT LABEL
venue-strategy-admin limits USER_ID CREDENTIAL_ID
venue-strategy-admin snapshot USER_ID CREDENTIAL_ID BTC/USDT
venue-strategy-admin submit USER_ID CREDENTIAL_ID
venue-strategy-admin observe USER_ID CREDENTIAL_ID
venue-strategy-admin funding USER_ID CREDENTIAL_ID BTC/USDT
venue-strategy-admin status USER_ID COMMAND_ID
venue-strategy-admin grid-create USER_ID CREDENTIAL_ID
venue-strategy-admin grid-status USER_ID INSTANCE_ID
venue-strategy-admin grid-lifecycle USER_ID INSTANCE_ID start|pause|resume|stop|reset
```

`probe` 使用同一 stdin 凭证结构和正式 adapter，只返回脱敏身份摘要、权限核验后的持仓/挂单/余额及成交条数，不连接数据库、不入账也不授予发送权。`bind` 的 stdin 是带 `venue` 标签的凭证结构：Bitget/OKX 为 `api_key/api_secret/passphrase`；Bybit/Gate 为 `api_key/api_secret`；Hyperliquid 为 `account_address`、可选 `vault_address`、`api_wallet_address/private_key`，存在 vault 时所有读取、签名和唯一身份均绑定该 vault。绑定只执行签名读取与权限、模式、身份核验，保存密文；默认要求账户无持仓、无普通或条件挂单。

`bind-released` 仅用于运营清单中的既有账户 UUID 已通过旧运行时生命周期释放后的迁入，可保留经签名确认的持仓，仍要求无开放订单、无旧 scope、无未决命令。首次迁入在真实身份、权限和上述排他条件同时通过后才原子建立账户库存行；真实身份唯一约束仍禁止换 UUID 重复接管。它不停止旧 writer、不释放旧 scope、不导入或修改旧 WAL。

`limits` 的 stdin 是 `StrategyRiskLimits` JSON，必填字符串字段 `max_order_notional` 与 `max_symbol_notional`，均为正数，后者不得低于前者。上限使用交易对报价资产单位。`submit` 的 stdin 是 `domain::ExecutionCommand` JSON：相同 ID 与内容幂等，改变内容须使用新 ID。`snapshot` 带观察时间；过期或读取失败不等于空仓，并保留交易所已提供的成交费用、maker/taker 角色和成交时间。`observe` 读取 stdin 中既有的限价、市价增仓或市价减仓命令，只按其原 `clientOrderId` 查询订单与成交，不认领、不发送也不重试；动态价格保护的 IOC 不冒充规范限价，订单与成交均精确为空才返回未发现，身份冲突或孤立成交失败关闭。`funding` 的 stdin 是不超过七日的 Bybit `start_ms/end_ms/cursor` 窗口，只接受该账户、交易对及报价资产的 `SETTLEMENT` 游标闭包，保留交易所签名正负号且不产生风险命令。

`grid-create` 的 stdin 是 `StrategyGridConfig`：`planner` 采用既有 `GridPlannerConfig`，`net_direction` 对四所 Hedge 账户为 `null`，对 Hyperliquid 为明确方向。新建 `revision=1`，金额资产必须与 symbol 的 quote 一致；须先设置账户金额上限。实例创建后为 `paused`，创建本身不发送命令。当前 Hedge 每侧最多 4 层、Net 最多 8 层，完整首轮最多 16 单；独立策略队列上限 32 条，以容纳一次完整撤挂；配置超限明确拒绝。配置结构示例见 [离线 fixture](../apps/venue-control/tests/fixtures/multi_venue_grid.json)，其中金额仅用于测试，不代表实际账户的交易参数。

`start/resume` 启动或恢复收敛；`pause/stop` 先取消未发送的非撤单命令，再按原身份对账并撤销自有挂单剩余量，保留持仓。两者只有在自有订单终态确认后才进入 `paused/stopped`。`reset` 先收敛旧订单，再增加配置 revision 重新规划。连续拒单和读取/规划失败达到 `failure_threshold`，或收敛超过 `convergence_timeout_ms` 时，持久化暂停意图；未发送命令取消，已发送和未知命令继续按原身份对账。拒单按账本顺序去重计数，明确恢复时清除门。每账户最多 20 个网格，创建在账户锁内执行并发准入检查。状态中的 `blocked_reason` 保留暂停原因；不能靠改库清空错误推动发送。

网格使用逐单签名累计成交与当前签名持仓驱动滚动；部分成交消耗量与计划原子提交，重启不会重复消费。撤单与补挂在账户队列内按提交顺序执行；撤单结果不确定时后续命令不发送。每笔补挂前再次核对原单累计成交、预期撤销终态与当前快照；发生撤单/成交竞态时取消该轮未发送的补挂并重新规划。新网格不要求导入旧订单网络；迁入时基于当前持仓和新配置建立目标面。

## 协议依据与验收

新网格命令 ID 控制在共同支持的 28 字节内；旧账本中的 ID 不改写。滚动锚点按规则内容识别精度和上下界变化，不把每次重连产生的会话代次当作规则变更。Bitget Hedge 普通订单沿用 adapter 的持仓腿位与买卖方向语义核验平仓，不能使用只适用于单向模式的原生 `reduceOnly` 值覆盖它。

适配器必须保持与官方协议一致：[Bybit 下单](https://bybit-exchange.github.io/docs/v5/order/create-order)、[Bitget UTA](https://www.bitget.com/api-doc/uta/intro)、[Gate Futures](https://www.gate.com/docs/developers/apiv4/en/#futures)、[OKX Algo](https://www.okx.com/docs-v5/en/#order-book-trading-algo-trading)、[Hyperliquid Exchange](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint)。Bitget 准入校验 UID、可写、`uta_trade` 且无提现权限；Gate 使用签名 `/account/main_keys` 校验 UID、有效状态、期货写权限和无提现权限，不能读取该权限接口的 Key 不准入。

本地验证使用统一构建入口、五所协议 fixture、命令幂等与恢复契约、隔离 PostgreSQL 迁移/顺序/生命周期测试；不得把生产数据库当作 QA。真实验收另逐所记录普通挂单、精确撤单、市价增减仓、SL/TP 创建与触发、PostOnly 参数及 maker 成交角色，并核对手续费与持仓差额。离线通过不代表这些真实交易验收已执行。

旧网格替换与代码删除遵守 [迁移删除门](GRID_RUNTIME_REFACTOR.md#81-旧迁移代码删除门)：逐账户确认旧 writer 无在途不确定请求、旧自有订单已核清、新路径重启恢复已验证并保留回滚包后才能移除对应运行入口。未决 WAL、Unknown、checkpoint 和恢复工件始终保留；未完成真实切换前，旧 Node 只标注兼容入口，不宣称已经替换或重启。
