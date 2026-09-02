# Binance KOL 跟单 MVP 长期契约

更新：2026-09-02

## 1. 文档职责

本文定义 VENUE 下一阶段唯一获准的产品目标：完成可供真实用户使用的 Binance KOL 跟单 MVP。本文拥有该 MVP 的产品流程、进程拓扑、成交语义、安全边界、开发顺序和验收标准；旧网格迁移文档只继续约束仍在运行的旧实现，不得反向要求新跟单链引入 Actor、Checkpoint、handoff 或每账户进程。

本阶段只做 Binance。旧 Grid、Gate.io、Bitget 的迁移、接管和功能开发全部暂停；Bybit、OKX、Hyperliquid、Scalping 同样不进入本阶段。暂停不等于删除其源码、运行工件或恢复事实。

## 2. 目标与非目标

### 2.1 必须交付

1. 用户可从 KOL 专属链接进入页面、注册、登录，并在注册事务中默认归属该 KOL。
2. 用户可绑定自己的 Binance API Key，系统可验证真实账户身份、API 权限、Portfolio Margin 统一账户、UM 交易能力和双向持仓模式。
3. KOL 可在 VenueFlow 桌面交易终端查看自己的双向持仓、活动订单、真实成交、仓位历史和资产，并执行默认 Post Only 限价开多、平多、开空、平空及二次确认的市价平仓；精确撤单只在服务端确认订单归属后开放。
4. 用户明确确认风险参数并启用后，KOL 的真实成交可尽可能快地复制到跟随账户。
5. 初期全站最多 5 个启用 KOL、200 个启用跟单账户；容量测试必须覆盖单个 KOL 一次成交扇出全部 200 个账户。
6. KOL 可修改自己的公开页面名称、标题和说明；固定平台风险提示不可修改。
7. 用户可查看 API、跟单和执行的真实状态，可暂停/恢复跟单；任何状态不得把“已入队”显示成“已成交”。

### 2.2 明确不做

- Binance 以外交易所、跨交易所跟单、现货和 COIN-M；MVP 只支持 Binance Portfolio Margin UM。
- 收费、分佣、结算、返佣、排行榜、社交、站内信、公开策略市场和任意多层代理关系。
- 一个用户同时绑定多个 KOL、任何用户或管理员换绑 KOL、KOL 代用户创建 API Key 或修改交易所账户模式。
- 抄送 KOL 的未成交委托、条件单、止损单或撤单；MVP 只复制权威成交造成的仓位变化。
- 以旧 Grid/Runtime 迁移完成作为跟单上线前置，也不在本阶段重构或清理旧三所代码。
- 多 Executor 分片、跨机器选举、分布式 lease、复杂服务网格和高可用切换。
- 自动平掉用户启用跟单前的外部仓位，或在暂停跟单时自动清仓。

KOL 可从 Binance、VENUE 基础终端或其他客户端下单。跟单源始终是 Binance 账户成交事实，与下单入口无关。基础终端是 MVP 必交付项，但不扩展为完整专业终端：不做图表策略、条件单、止盈止损编排、批量算法单或自定义工作台。

## 3. 角色与产品流程

角色只有四类：普通用户、跟随者、KOL、平台管理员。KOL 是管理员显式授予的有限角色，不允许普通用户自行升级；启用 KOL 总数硬限制为 5。

每个启用 KOL 在 MVP 中恰好绑定一个已验证的主交易账户、一份策略资本配置和一个公开页面；每个普通用户只能归属一个 KOL，并且同一时间最多启用一个 Binance 跟随交易账户。用户可保存替换用凭证，但切换活动账户前必须暂停关系并重新通过启用门。所谓“绑定 KOL”是 Venue 中的关系，不把用户账户变成 KOL 可登录、可切换或可代操作的 Binance 子账户。

标准流程固定为：

```text
访问 /join/<invite_code>
-> 查看 KOL 公开页
-> 注册或登录
-> 注册事务保存唯一 KOL 归属
-> 绑定并验证 Binance API
-> 设置跟单资金、倍率和风险上限
-> 明确确认并启用
-> Executor 建立基线并开始跟随
-> 页面显示目标、提交、成交、偏差和暂停状态
```

邀请归属只决定默认 KOL，不创建交易权限、不自动启用跟单、不代表用户接受风险。只有 API 验证通过、关系参数有效且用户再次明确点击“启用跟单”后，Executor 才能为该账户产生新交易。

启用或恢复时必须读取 KOL 和跟随账户的签名仓位作为当前基线。跟随者已有非零仓位、开放普通/Algo 订单、未决旧命令或账户仍被旧 Node 管理时拒绝启用并给出明确原因；KOL 可有已有仓位，但只记录为基线，不向新跟随者追补。MVP 不猜测这些仓位的归属。

## 4. 邀请绑定与 KOL 页面

### 4.1 邀请绑定

- 每个 KOL 有一个当前有效、可轮换的 URL-safe 随机邀请码；公开链接为 `/join/<invite_code>`。
- 邀请码是归属标识，不是登录凭证或交易授权。服务端只保存其哈希或等价不可直接还原的索引。
- 匿名访问时可写入带签名、`HttpOnly`、`Secure`、`SameSite=Lax` 的短期邀请 Cookie；注册提交时仍须由服务端重新验证邀请码、KOL 状态和有效期。
- 用户、KOL 归属和邀请证据在同一 PostgreSQL 事务中写入。注册成功后，一个用户只能有一个 KOL 归属；再次访问其他链接不得静默覆盖。
- 已有账号点击邀请链接不会换绑。MVP 不提供用户端、KOL 端或管理员端纠错换绑；归属一经注册事务提交即保持不变，未来如确需换绑必须另立需求和审计方案。
- KOL 用户与跟随用户不得相同；KOL 和跟随者的 Binance 真实账户身份也不得相同，不能用同账户的两把 Key 绕过。
- 邀请码被停用只阻止新的归属，不自动停止已有关系；停用 KOL 时由管理员显式暂停其全部关系。

建议最小数据关系为 `kol_profiles`、`kol_invites`、`user_kol_bindings`，其中 `user_kol_bindings.user_id` 唯一，启用 KOL 的 `leader_trading_account_id` 唯一且非空，活动跟单关系的 `follower_trading_account_id` 对用户唯一。数据库外键指向稳定的 KOL 用户 ID；客户端提交的 KOL ID 不作为可信事实。

### 4.2 KOL 页面

KOL 可编辑字段限定为：公开名称（1–40 字）、页面标题（1–80 字）和纯文本说明（0–2000 字）。输出统一转义，不接受 HTML、脚本、样式或可执行嵌入；是否允许普通外链须由服务器白名单决定。

每次修改携带期望 revision，使用乐观并发并保留修改时间和操作者审计。KOL 只能修改自己的页面，管理员可以停用页面；页面修改不得改变邀请码归属、跟单参数或 Executor 状态。

页面必须固定展示平台风险提示、KOL 内容为其自行提供的说明、历史表现不代表未来结果。KOL 最多看到聚合跟随人数和聚合运行状态；不得读取跟随者密码、API Key、API Secret、完整账户身份、持仓明细或订单明细。任何 UI（包括管理员 UI）均不提供 API 明文查看功能。

## 5. 目标架构与进程边界

目标拓扑固定为：

```text
浏览器
  -> Venue Web / 同源 BFF
  -> Venue Control + PostgreSQL
  -> 单个 Binance 多账户 Copy Executor
  -> Binance
```

### 5.1 进程职责

- `venue-web`：公开 HTTPS 页面、邀请注册、登录、API 绑定、跟单设置和 KOL 页面；浏览器不直连 Control、PostgreSQL 或 Binance。
- `apps/ui/desktop`：Binance 风格 VenueFlow 桌面终端；只消费 Control 的用户作用域私有投影和命令状态，不持有 API Secret，也不直连 Binance 私流。
- `venue-control`：认证、邀请归属、KOL 页面、API 密文管理、只读验证、关系配置、终端命令授权、查询投影和任务持久化；不直接执行物理订单。
- PostgreSQL：保存用户、归属、页面、加密凭证、关系、KOL 成交游标、目标版本、终端/跟单轻量命令账本和执行投影。
- `venue-executor-binance`：初期新链唯一物理交易进程。它可抽取现有 `venue-copy-worker` 的纯逻辑，但后者及旧 Node 不得继续作为第二个生产跟单入口。

Executor 使用 Tokio 异步任务管理所有账户：

- 最多 5 个 KOL 账户各持有一条认证私流，用于接收成交；另可共享公共价格和规则连接。
- 跟随账户不创建独立进程，不运行 Strategy Actor，不保存策略 Checkpoint，也不要求常驻私流。
- 跟随账户通过共享 HTTP keep-alive client、按需精确订单回读和低频签名仓位对账工作。
- 每个跟随账户只有一个有界串行队列；不同账户并发，同一账户的订单不得并发乱序。
- 每个账户只保留“当前目标、最近已对账仓位、一个在途命令和一个待重算标记”，连续 KOL 成交合并到最新目标，不形成无界任务队列。

Executor 以部署副本数 1 运行，并在启动时取得一个 PostgreSQL 全局 advisory lock；取锁失败立即退出。它不是每账户 lease、选举或 handoff。数据库连接或全局锁丢失后停止产生新订单，只允许对已提交命令做只读收敛。

基础终端与自动跟单共用这个 Executor、Binance adapter、账户串行队列和命令账本，不建立第二个手动交易 writer。Control 只验证 KOL 对本人账户的命令并写入账本；Executor 再按签名账户事实执行。终端不得接受客户端自造账户归属、数量归一化结果或 `clientOrderId`。

桌面每次打开或切换账户时只向 Control 续订短期投影需求。Executor 有界接入认证私流，并以签名 REST 形成账户级完整快照；仓位、活动订单、成交和资产先进入 PostgreSQL，再由 Control 按登录用户与 credential 双重作用域返回。仓位历史只从上线后真实快照变化累积，不伪造历史回填；历史委托当前只展示新 Executor 命令账本，不冒充 Binance 全量历史订单。

### 5.2 新旧互斥

任何 `(binance, trading_account_id)` 不得同时由旧 Node/Stage 7 和新 Executor 管理。账户加入新链前必须：

1. 停止并禁用旧服务及自动重启；
2. 确认旧 writer 已释放且没有 `Prepared / Submitted / Unknown`；
3. 签名读取当前仓位、普通订单和 Algo 订单；
4. 从旧部署配置移除该账户，再在新 Executor 中启用。

本 MVP 不迁移旧 Grid Actor、Checkpoint 或本地 WAL。存在旧未决事实的账户保持拒绝，不能通过清文件或换 API Key 绕过。KOL 与跟随账户都遵守新旧互斥。

## 6. 跟单成交语义

### 6.1 权威触发

唯一实时触发是 KOL 认证账户流中的 `ORDER_TRADE_UPDATE` 且执行类型为 `TRADE` 的增量成交。账户流不是逐消息签名；其断线补偿、游标恢复和最终核对使用签名 REST。`NEW`、ACK、未成交委托、撤单和本地 UI 状态都不是跟单事实。

每条 KOL 成交先以 `(kol_trading_account_id, native_symbol, native_trade_id)` 唯一写入 PostgreSQL，再更新该 KOL 在规范交易对上的 `LONG` 和 `SHORT` 两条仓位目标。私流重连后用签名成交查询从已提交游标重叠补齐；同 ID 同内容是重复，同 ID 不同内容为冲突并暂停该 KOL。

MVP 只支持 Hedge Mode，`LONG` 与 `SHORT` 独立计算，不把两腿合并成净仓：

```text
BUY  + LONG  -> 增加多腿
SELL + LONG  -> 减少多腿
SELL + SHORT -> 增加空腿
BUY  + SHORT -> 减少空腿
```

减少量不得超过 Executor 在 dispatch 前取得、且仍属同一账户代际的新鲜签名对应腿；永远不能因平仓事件穿过零点并反向开仓。快照过期、代际改变或读取失败就暂停该账户本次发送。多腿与空腿可以同时存在。对同一账户、交易对，先处理降险目标，再处理增险目标。

“平仓”是领域层只减仓语义，不等同于原生请求必须携带 `reduceOnly=true`。Binance Portfolio Margin UM Hedge Mode 要求显式 `positionSide=LONG/SHORT`，且普通订单禁止发送 `reduceOnly` 参数；adapter 必须省略该原生参数，并以相反方向、持仓上限裁剪和成交后签名仓位证明不跨零。

### 6.2 目标和数量

KOL 必须配置正数的策略资本；跟随者配置分配资金、倍率、单笔最大名义、总名义上限和允许交易对。启用关系时只保存 KOL 当时的 Long/Short 签名基线和成交游标，不追补历史仓位；该关系的“可复制腿数量”从零开始，只按启用后的认证成交增减且下限为零。每条腿的目标按以下输入确定计算，并持久化 Decimal 字符串：

```text
跟随目标数量
= 该关系启用后的 KOL 可复制腿数量
  × (跟随分配资金 / KOL 策略资本)
  × 跟随倍率
```

随后按实时 Binance 数量步长向下归一化，并同时受可用保证金、最小名义、单笔上限和总上限约束。低于最小名义时保留最新目标但不下单；后续成交使差额达到最小值后再收敛，不为每个小成交制造碎单。

每个部分成交都按原生 trade ID 持久化并更新最新目标，但不机械地为每个 WebSocket 帧发一张订单：第一笔新目标立即调度；若同腿已有在途命令，后续成交只更新 `dirty target`，在原命令签名收敛后重新计算一次差额。

### 6.3 订单政策

为优先满足快速复制，MVP 的增仓使用 Binance UM `MARKET`，减仓使用绑定明确 `positionSide` 且不超过 dispatch 前同代新鲜签名腿数量的市场减仓。实现 P3 必须补齐并专项验证现有 Binance adapter 尚未开放的 `PlaceMarket` 路径；不得从旧旁路直接拼 HTTP 请求。

KOL 终端开仓和平仓默认只使用 `LIMIT + GTX(Post Only)`；Maker-only 设置默认开启，关闭时禁止交易而不是退化成 taker 限价。市价只开放双向持仓腿的平仓，必须二次确认，并在 Executor 发送前按同代新鲜签名仓位再次向下裁剪。撤单是独立耐久命令，只能以服务端确认属于该 KOL 账户的原生订单身份精确撤销；该链路未完成前桌面按钮保持禁用。终端订单与 Copy 一样，只有签名订单/成交/仓位回读完成后才显示最终状态。

增仓发送前必须使用共享的新鲜价格进行名义价值和价格偏离检查；超过用户配置的最大偏离、价格过期、规则缺失或最小/最大数量不满足时拒绝该次执行。市场单无法保证最终成交价，页面必须在启用前明确提示滑点风险。

关系启用、暂停、恢复或修改风险参数均递增 revision。旧 revision 尚未发送的任务直接取消；已经进入 `Sending / ReconcileRequired` 的命令只做原身份对账，不按新参数重解释。

暂停只阻止新订单并取消尚未提交的计划，不自动平仓。KOL 撤销未成交委托无需对跟随者做任何操作；KOL 后续真实平仓成交会生成对应降仓目标。自动“停止并清仓”不属于 MVP。

## 7. 幂等与轻量命令账本

跟单路径不使用每账户 JSONL WAL、Actor Applied、hash-chain receipt、manifest 或 Checkpoint。PostgreSQL 轻量命令账本是该路径唯一的发送前持久记录。

最小记录包括：关系/revision、KOL 成交或目标版本、跟随账户、symbol、position side、目标与差额、规则版本、确定性 `clientOrderId`、状态、时间和脱敏错误。唯一约束至少覆盖：

```text
(relation_id, relation_revision, target_revision, follower_account_id, symbol, position_side, phase)
client_order_id
```

状态只保留：

```text
Pending -> Sending -> Accepted -> Reconciled
   |            |--> Rejected
   |            \--> ReconcileRequired -> Reconciled
   \--> Cancelled（仅发送前取消）
```

- `Pending` 必须已提交事务后才允许发送。
- 发送前原子切换为 `Sending`；崩溃恢复看到 `Sending` 时按同一 `clientOrderId` 查询，禁止直接再次 POST。
- HTTP 超时、连接中断、响应无法解析或 ACK 身份不一致记为 `ReconcileRequired`。该跟随账户暂停新订单；其他账户继续。
- `Accepted` 只代表交易所接受，不代表成交。只有精确订单/成交以及更新签名仓位相互一致才记 `Reconciled`，UI 才能显示“跟单完成”。
- `Rejected` 不自动重发原命令；后续新的 KOL 目标可产生新的确定性命令。
- 重复 KOL 帧、重复数据库轮询、进程重启和重复页面操作只能返回已有记录，不得产生第二个物理订单。
- `ReconcileRequired` 在有界精确回读后仍无法收敛时保持该状态并标记账户 `NeedsAttention`；不得用一次 404 证明未下单。

账本是订单幂等和故障收敛的必要最小机制，不承担策略历史回放、权限证明链或多进程选举。

## 8. 凭证、认证与权限

### 8.1 用户会话

- 复用 Control 现有 Argon2id 密码、随机 256-bit 会话 token、数据库仅存 token 摘要及登录/验证限频。
- Web 从当前环境注入的运维会话改为真实用户会话；Control token 只保存在 BFF 设置的 `HttpOnly / Secure / SameSite` Cookie，不进入浏览器 `localStorage`、URL 或前端日志。
- 所有写操作校验同源、CSRF、服务端用户身份和资源归属。浏览器传入的 `user_id`、`kol_id`、`trading_account_id` 都不能替代服务端归属查询。
- Node/Executor 内部令牌与普通用户会话完全分开，内部路由不得由浏览器调用。

### 8.2 API Key 保存

- 复用现有 AES-256-GCM 随机 nonce 加密，AAD 绑定 `user_id + credential_id`；PostgreSQL 只存密文、Key 指纹、掩码和非秘密验证结果。
- 主密钥只来自进程环境或后续 KMS/Vault，不进入数据库、仓库、配置、日志或错误。MVP 上线前必须有仓库外备份和恢复演练；主密钥轮换不在本阶段临时实现。
- API Key/Secret 只在绑定请求、Control 验证和 Executor 内存中短时出现，使用 `secrecy/zeroize` 清理；不得写入 URL、trace、panic、运行工件或浏览器回包。
- 只有受信任的 Control 凭证验证代码和 Binance Executor 可以解密。KOL、跟随者、普通后台页面及运维查询均只能看到掩码；KOL绝不能看到跟随账户 API 明文。
- Executor 数据库角色只读取已启用关系所需的加密凭证和配置，只写成交游标、命令账本与执行投影；不授予用户/密码管理权限。

### 8.3 Binance 验证门

一次验证必须通过全部条件才可标记 Ready：

1. Key 有读取权限和 Portfolio Margin 交易权限；提现权限必须关闭。
2. 真实账户 `accountStatus=NORMAL`，Portfolio Margin 统一账户及 UM 交易可用。
3. `dualSidePosition=true`，即双向持仓模式；MVP 拒绝单向净持仓。
4. 账户身份 UID 可稳定哈希；同一真实账户不能归属于不同用户，也不能同时作为同一关系的 KOL 和跟随账户。
5. 持仓、普通开放订单和 Algo 开放订单三个签名面均完整可读；任一面缺失、解析失败或网络失败都不能显示验证通过。

绑定验证只执行签名 GET，不下单、不撤单、不修改账户模式。页面应显示服务器出口 IP，并强烈要求用户在 Binance 对 Key 设置精确 IP 白名单；系统不能验证的交易所侧设置不得伪装成已自动证明。

启用跟单要求最近一次完整验证仍新鲜。运行中出现权限/身份/模式变化、认证拒绝或解密失败时只暂停该账户，不删除密文、不自动换 Key、不影响其他账户。

## 9. 现有代码复用与冻结边界

### 9.1 优先复用

- `apps/venue-control/src/accounts/`：用户、会话、Argon2id、AES-GCM、凭证归属和限频。
- `crates/venue-control-protocol/src/accounts.rs`：注册、登录、凭证绑定/验证的秘密传输边界；按本契约增量扩展邀请、KOL 和启用状态 DTO。
- `crates/venue-gateway-binance`：签名、校时、凭证 probe、账户身份、Portfolio Margin UM 规则、KOL 私流、订单/成交/仓位解析和精确回读。
- `crates/venue-domain`：Symbol、Decimal、OrderSide、PositionSide、订单和成交规范类型。
- `crates/venue-copy`：仅复用能直接证明正确且不依赖 Actor/delivery/manifest 的纯数量、资本和价格计算；不要求保留旧 Copy orchestration。
- `apps/ui/web`：邀请、账户与 KOL 公共页面；`apps/ui/desktop`：桌面交易终端。两个 UI 保持同级目录并由 `apps/ui/README.md` 明确入口，不能混作同一客户端。
- SQLx/PostgreSQL、Tokio、reqwest、tokio-tungstenite、secrecy、zeroize、tracing 等现有依赖，不为本 MVP 引入第二套框架。

### 9.2 冻结而不迁移

以下路径在本阶段不继续扩展，也不得成为新跟单的前置：旧 Grid、Stage 7、Gate/Bitget、Strategy Actor、Actor Applied、Account Runtime 的复杂恢复、旧 Copy delivery/lease/manifest/outbox 链、每账户 Node 启动和本地 JSONL WAL。

它们保持可编译、可读取既有恢复事实，除非阻挡本 MVP 的共享类型编译或构建；不得借本任务删除旧恢复工件、改写旧 WAL、宣称旧三所接管完成或恢复旧生产入口。

新 Executor 上线后，所有新 Binance 跟单订单只允许从该入口产生。已有旧 Copy 关系先暂停并完成签名核对，再以干净账户和新 revision 重新启用；不迁移旧 Actor/manifest 的中间状态。

### 9.3 目标代码落点

本轮不新建通用 runtime crate，也不搬迁整个 workspace。改动限定为：

- 在现有 `apps/venue-control` package 内新增薄 binary `src/bin/venue-executor-binance.rs` 和按职责拆分的 `binance_executor/` 模块；旧 `venue-copy-worker` 在开发期只作冻结参考，MVP 发布清单中不得与新 binary 并存。
- 在 `apps/venue-control/migrations/` 增量增加 KOL、邀请、唯一归属、跟单设置、源成交/目标版本、命令账本和执行投影表；不改写旧 migration 或旧恢复记录。
- 在 `apps/venue-control/src/accounts/`、HTTP/service/repository 现有边界内扩展用户会话、KOL 权限和凭证授权；不建立第二个认证服务。
- 在 `crates/venue-control-protocol` 增量加入邀请、KOL 页面、终端和跟单状态 DTO；不复用旧 Node delivery DTO 作为新 Executor 协议。
- 在 `crates/venue-gateway-binance` 补齐 Portfolio Margin UM 认证账户流、Post Only 限价、市价平仓、精确撤单和同代签名回读；不复制签名 HTTP client。
- 在现有 `apps/ui/web` 增加 `/join/<invite_code>`、注册/登录、API 管理、跟单状态和 KOL 页面编辑；桌面终端只位于同级的 `apps/ui/desktop`。

这样只新增一个 binary 和一组局部模块，旧 Grid/六所 Node 不迁移、不改数据格式，也不成为交付依赖。

当前已落地 `0017`–`0020` 数据契约、邀请/KOL/跟单 HTTP、唯一 `venue-executor-binance`、私流与签名 REST 投影、Post Only/市价平仓命令账本及桌面消费链。真实凭证联调、旧账户迁移、精确撤单和 2 核 4 GiB/真实 Canary 仍须按验收门执行；存在旧 `venue_control_strategy_scopes` 的账户在 Control 入账与 Executor 抢占两处均保持拒绝。

## 10. P0–P5 开发计划

### P0：冻结范围与当前基线

- 更新长期架构/代码地图，使 Binance KOL 跟单 MVP 成为下一目标，并明确旧 Grid/Gate/Bitget 暂停。
- 清点现有 Web 会话、Control 账户、Copy planner、Binance adapter、部署服务和真实账户；列出可复用项与必须停用的旧入口。
- 固定 `venue-executor-binance` 的数据库表、状态枚举、API DTO 和性能时间戳；不得先写第二套网关。
- 只读核对真实 KOL/跟随账户、旧 writer、持仓/订单和权限。P0 不进行交易。

完成门：形成精确代码改动清单；目标账户无新旧并行计划；所有未决旧状态有处置结论。

### P1：公开注册、邀请与 KOL 页面

- 新增 KOL/profile/invite/user-binding migration 和 Control API。
- 把 Web 的环境注入会话替换为真实注册/登录/退出及安全 Cookie BFF。
- 完成邀请落地页、注册后不可静默换绑、KOL 自助编辑和固定风险提示。
- 增加跨用户、跨 KOL、CSRF、邀请码重放/停用、revision 冲突和 XSS 测试。

完成门：新用户从专属链接注册后服务端归属正确；未绑定 API 时明确显示“未启用交易”。

### P2：Web API 绑定和启用门

- 将现有 Control 凭证接口安全接到 Web；完成添加、掩码、验证、复验、选择、删除和错误说明。
- 按第 8.3 节完善 Binance probe 及真实账户冲突校验。
- 新增跟单资金、倍率、交易对和风险参数；创建关系默认为 Paused，显式确认后才申请 Active。
- 建立 Executor 的最小权限凭证读取边界，不通过环境变量为每个账户复制明文 Key。

完成门：正确账户可 Ready；普通合约/单向持仓/提现开启/权限不足/跨用户绑定均失败；数据库、日志和浏览器均无 API 明文。

### P3：单进程多账户 Executor

- 在现有 `venue-control` package 内建立唯一 `venue-executor-binance`，只抽取旧 copy worker 可证明正确的纯逻辑，并接入全局单实例锁、最多 5 个 KOL 私流、共享规则/价格和每账户有界队列。
- 实现 KOL 成交去重、断线补查、双腿目标计算、目标合并、实时规则归一化和轻量命令账本。
- 在统一 Binance adapter 内补齐 `PlaceMarket`，复用现有市场减仓、签名回读和错误解析；不创建旁路 HTTP client。
- 把 KOL 基础终端接到同一命令账本和账户队列，支持显式 `positionSide` 的市价/限价开平仓、精确撤单及签名订单/成交/仓位回显。

当前预备边界：Binance adapter 的 Hedge Mode 市价开仓预备路径显式携带 `positionSide` 且从不序列化 Binance 禁止的 `reduceOnly`；`kol_executor.rs` 已持久化领取 Pending、同账户 Sending/ReconcileRequired 栅栏和 `Accepted/Rejected/ReconcileRequired/Reconciled/Cancelled` 的单向状态迁移。它不含私流、目标计算、解密、传输或 binary，尚不构成 P3 完成。

离线调度层进一步固定为中心化有界 round-robin：每个账户最多一个 in-flight，单账户积压最多 16，全局最多 32 个 in-flight；该层不创建每账户 task、Actor 或恢复 journal。`source_fill_from_private` 只接收 Binance adapter 已认证且带明确 Long/Short 腿的成交，`scaled_copy_quantity` 只做已持久化源成交后的比例计算。私流连接、密文解密、签名发送、精确回读、重启对账与 `venue-executor-binance` binary 尚未接通，不能将这些纯离线边界视为 P3 完成。

P3-A 已接通受限依赖边界：`PgExecutorStore` 在 PostgreSQL 中按 native trade identity 去重源成交、读取未终态命令，并只在 Pending activation 的 relation revision 匹配时提升为 Active；`ExecutorSecretProvider` 只用 `(credential_id, owner_user_id)` 查询密文并复用 AES-256-GCM AAD 解密为不可序列化的 adapter 密钥容器。

P3-B 将这些边界组合成离线可验证的 `BinanceExecutorRuntime`：同一事务内对源成交去重、推进 relation/腿目标并生成不超过 36 字符的确定性命令 ID；重启先对 `Sending/Accepted/ReconcileRequired` 做同 ID readback，超时和损坏响应只进入 `ReconcileRequired`，绝不回到 Pending 或重发；每个账户只领取最旧 Pending。启用请求只在 KOL 与 follower 两次签名基线都干净时提升，任一基线失败则拒绝。PostgreSQL+mock 集成 fixture 覆盖重复成交、重启、超时、拒单和启用成功。

P3-C 已闭合 production adapter 与 binary：`BinanceHttpExecution` 使用固定 `BinanceHttpTransport` 的签名、`exchangeInfo` 规则、`prepare_place_market` 与一次 POST 后精确回读。数量按 stepSize 向下归一，低于 minQty/minNotional 或缺签名价格、规则、Hedge 腿即拒绝；Hedge MARKET 开仓不传 `reduceOnly`。ACK、accountTradeList 和 position risk 必须共同收敛才进入 Reconciled，部分成交、订单缺失、仓位不符或响应不明保持 Accepted/Unknown；超时、断连和损坏 ACK 只按原 clientOrderId 查询，绝不再次 POST。

`venue-executor-binance` 只接受 `VENUE_EXECUTOR_MODE=LIVE`、`VENUE_EXECUTOR_DATABASE_URL` 的 PostgreSQL URL 与既有 credential master key；无 mock、dry-run 或 testnet 配置。它持有 advisory singleton，启动先完成 Pending activation 的双账户签名基线并恢复未终态命令，随后对已激活 KOL 账户建立 listenKey 私流。只把 `ORDER_TRADE_UPDATE/TRADE` 映射为规范成交；重复 WS/REST 成交由 `(account,native symbol,native trade id)` 去重，断线、过期和 gap 触发签名 REST 补读与状态对账，原始帧不持久化。SIGINT/SIGTERM 令循环停止、私流析构并释放锁。真实凭证联调、2核4G复验和真实 Canary 仍是外部验收，不构成实盘准入。
- 实现 Pending/Sending/Accepted/Rejected/ReconcileRequired/Reconciled、重启恢复、账户隔离和 UI 投影。

完成门：离线端到端证明终端与跟单不会争抢同一账户，且重复事件、重启、超时、响应损坏、部分成交、两腿并存和关系暂停均不重复下单、不跨零、不拖垮其他账户。

### P4：安全与容量验证

- 使用可控 Binance fixture/代理完成 5 KOL、200 跟随者的突发和 30 分钟稳态压测。
- 检查 BFF/Control/PG/Executor 的权限、日志脱敏、密文篡改拒绝、队列上限和单实例失败关闭。
- 在目标 2核4G 主机测量总内存、CPU、事件循环、数据库连接、队列、签名和分段延迟。
- 实际启动 Web 的手机/桌面关键流程，验证正常、错误、离线、过期会话和待对账页面。

完成门：满足第 11 节全部门槛；未满足时不得声称 2核4G 足够，先优化或升级至 4核8G，不在此阶段引入分片。

### P5：Binance 真实 Canary 与发布

- 先停止目标账户旧服务并签名证明干净，再以 1 KOL + 1 跟随账户、单笔不超过当前获准 10U 做真实闭环。
- 验证多开、多平、空开、空平、暂停、重复源事件、Executor 受控重启和签名仓位回显；真实 Canary 不得主动制造网络故障或待对账状态。
- 真实 Canary 扩至至少 2 个不同跟随账户即可；5、20、50、100、200 是 P4 的 fixture 容量门和上线后自然增长时的配置放行档位，不要求为验收制造 200 个真实账户交易。每次提高档位前确认指标、未决命令和异常仓位。
- 记录 release hash、服务入口、账户状态、分段延迟和脱敏证据；停止测试新增风险并核对剩余仓位/订单。

完成门：真实 KOL 成交可触发至少两个不同账户的正确复制和签名收敛；注册、邀请、API、KOL 页面、暂停及执行状态均可由真实 Web 完成。ACK、fixture 或同账户双 Key 均不能代替真实验收。

## 11. 验收与性能容量门

### 11.1 功能验收

- 专属链接注册后绑定正确 KOL；重复访问其他链接不换绑；邀请绑定本身不产生任何订单。
- 用户可登录、退出、恢复会话；退出后 Cookie/SSE/写操作立即失效。
- Binance API 的统一账户、交易权限、提现关闭和双向持仓均由签名事实验证；API 状态、Executor 在线和跟单 Active 分开显示。
- KOL 基础终端可显示签名持仓、活动订单和成交；市价/限价四种开平仓及精确撤单均走同一 Executor，页面区分已提交、已接受与已成交。
- KOL 只能修改自己的页面，固定风险提示不可编辑，任何 KOL 页面与接口都看不到跟随者 API 明文。
- 一次 KOL 成交可正确产生多开、多平、空开或空平目标；同事件重复 100 次仍最多一张相同跟随命令。
- 跟随账户之间故障隔离；一个账户 `Rejected/ReconcileRequired` 不停止其他账户。暂停不再产生新订单，也不自动清仓。
- Executor 重启后 `Pending` 可继续调度，`Sending/ReconcileRequired` 只查不重发；`Accepted` 必须经签名事实后才显示完成。
- 新旧实现不得同时操作同一账户，旧 Grid/Gate/Bitget 没有因本次发布被启动、迁移或宣称完成。

### 11.2 性能指标

所有本地指标从 Executor 收到已解析 KOL 成交的单调时钟开始，分别记录 PostgreSQL 提交、排队、HTTP send start、Binance ACK、精确回读和 UI 可见时间。交易所网络耗时单独报告，不混入本地调度承诺。

在 2核4G、同机 Web + Control + PostgreSQL + Executor、连接预热且 Binance 使用可控延迟 fixture 的环境，以 5 个 KOL、200 个已启用跟随账户、一个 KOL 成交同时 fan-out 为基准：

- 成交接收至源事实、关系目标版本及本次合格 `Pending` 命令的单一批量事务提交：p95 ≤ 20 ms；
- 成交接收至单个跟随请求开始发送：p50 ≤ 100 ms、p95 ≤ 500 ms、p99 ≤ 1 s；
- 200 个合格跟随请求全部开始发送：≤ 1.5 s；
- 无丢失、无重复物理发送、无跨账户串行阻塞；队列不得无界增长或静默丢弃。

30 分钟稳态与重复突发期间：整机不得持续 swap，全部 VENUE 服务与 PostgreSQL RSS 合计峰值 ≤ 3 GiB，CPU p95 < 80%，数据库连接、文件描述符和队列均低于配置上限的 80%。超载必须记录并暂停受影响关系，不能用丢事件保持低延迟。

真实 Binance Canary 只报告实测 p50/p95 和样本量，不用少量真实订单伪造可靠 p99。若本地容量门通过而外部 ACK 变慢，应单独归因为网络或交易所；若本地门未通过，2核4G即判定不足，升级规格后重新验收。只有单机 200 账户实测失败且优化无效，后续阶段才讨论 Executor 分片。

## 12. 上线退出条件

只有同时满足以下条件才可称为“Binance KOL 跟单 MVP 可用”：产品功能、安全验收、2核4G容量门、至少一组 KOL 与两个不同真实跟随账户的真实成交闭环全部通过；运行中无未决 `ReconcileRequired`；目标账户无旧 writer；密钥恢复材料已在仓库外备份；部署入口、停止方式和当前账户状态已有记录。

任何一项缺失均应标记为“未完成”或“受限试运行”，不得以代码已提交、HTTP ACK、模拟测试、旧历史证据或页面可见代替。
