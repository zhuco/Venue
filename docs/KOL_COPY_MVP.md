# Binance KOL 跟单 MVP 长期契约

## KOL 托管凭证保存

冻结 Web 曾使用同名 `venue_kol_managed_followers` 且字段不同。新接口明确使用 `venue_managed_credentials`；迁移 `0031` 不改写或删除旧表，旧表有记录时拒绝安装，须先审查凭证归属并制订数据迁移。已应用的 `0030` 校验和保持不变。

启用的 KOL 可添加已获账户所有者委托的 Binance API，填写标签、Key 和 Secret，单次对话框最多 10 个、每 KOL 最多 200 个保存账户。此功能与邀请注册分别管理：每个托管账户建立不可登录的内部用户，在 `venue_managed_credentials` 固定归属；凭证保存不创建跟单关系或执行权限。原有跟随用户仍不能由 KOL 查看密钥或代为切换账户。

凭证复用现有 AES-256-GCM、AAD 和唯一 Key 指纹；内部用户、凭证和管理映射同一事务提交。保存请求按 KOL 与 request_id 幂等，同 ID 不同内容拒绝；重复 Key 整笔回滚。页面和接口只返回管理编号、标签、掩码与验证状态，不返回凭证编号、内部用户、完整账户身份或密钥。手动验证复用 Binance 只读 probe，拒绝跨 KOL 访问及非启用 KOL 操作。保存/验证不会授予带单权限、选中终端账户、创建机器人或发送订单；真实跟单启用另需明确风险授权及空仓等门禁。

托管账户各自通过 `/v2/kol/managed-followers/follow/{status,settings,lifecycle}` 读取状态、保存参数和显式启停。保存参数在同一事务中建立托管来源绑定及暂停关系；迁移 `0033` 约束邀请来源和托管来源恰选其一，保持归属不可变。归属、请求摘要、参数及幂等结果同事务校验和提交；内部用户不能借普通跟单接口调用。停用 KOL 仍可暂停原关系，不能激活。

每账户独立选择定比或定额跟单，迁移 `0032` 的旧记录默认定比。定比保留原数量比例公式；定额使用正数报价币名义金额除以源限价，不再乘资金比例或倍数，且不得超过单笔风险上限。两种模式都遵守步长向下取整、最低名义额、总风险上限、已有子单成交扣减和只减仓裁剪；改变参数前必须暂停并结清未决同步订单。保存设置不自动启用。

更新：2026-09-05

## 1. 文档职责

本文定义可供真实用户使用的 Binance KOL 跟单 MVP，拥有其产品流程、挂单同步语义、安全边界和验收标准。Binance Grid 的当前重建契约由 [`GRID_RUNTIME_REFACTOR.md`](GRID_RUNTIME_REFACTOR.md) 管理；两者共用单例 Executor，但都不得引入 Actor、Checkpoint、handoff 或每账户进程。

本阶段只做 Binance。旧 Grid 接管路线停止，新的事实驱动 Binance Grid 与 KOL 共用执行链；Gate.io、Bitget、Bybit、OKX、Hyperliquid、Scalping 仍暂停。暂停不等于删除其运行工件或恢复事实。

Grid 明确交易所拒单采用首次拒单后 30 秒开始重置的恢复语义，期间继续正常补撤；后续拒单不刷新期限，实际撤单重布使用独立收敛计时。超时或响应未知仍由统一命令账本按原 clientOrderId 对账，不能作为明确拒单重发；完整边界见 Grid 契约第 5.1 节。

## 2. 目标与非目标

### 2.1 必须交付

1. 用户可从 KOL 专属链接进入页面、注册、登录，并在注册事务中默认归属该 KOL。
2. 用户可绑定自己的 Binance API Key，系统可验证真实账户身份、API 权限、Portfolio Margin 统一账户、UM 交易能力和双向持仓模式。
3. KOL 可在 VenueFlow 桌面交易终端查看自己的双向持仓、活动订单、真实成交、仓位历史和资产，并执行默认 Post Only 限价开多、平多、开空、平空及二次确认的市价平仓；精确撤单只在服务端确认订单归属后开放。
4. 用户明确确认风险参数并启用后，同步已授权带单机器人主账户的新限价挂单；子账户独立成交。
5. 初期全站最多 5 个启用 KOL、200 个启用跟单账户；容量测试必须覆盖单个 KOL 一次挂单变化扇出全部 200 个账户。
6. KOL 可修改自己的公开页面名称、标题和说明；固定平台风险提示不可修改。
7. 用户可查看 API、跟单和执行的真实状态，可暂停/恢复跟单；任何状态不得把“已入队”显示成“已成交”。

### 2.2 明确不做

- Binance 以外交易所、跨交易所跟单、现货和 COIN-M；MVP 只支持 Binance Portfolio Margin UM。
- 收费、分佣、结算、返佣、排行榜、社交、站内信、公开策略市场和任意多层代理关系。
- 一个用户同时绑定多个 KOL、任何用户或管理员换绑 KOL、KOL 代用户创建 API Key 或修改交易所账户模式。
- 市价成交追补、Algo/条件单/止损单复制；本阶段同步普通 UM 限价挂单及其撤单。
- 以旧 Grid/Runtime 接管完成作为跟单上线前置；Binance Grid 重建独立验收，不能阻塞 KOL 跟单账户。
- 多 Executor 分片、跨机器选举、分布式 lease、复杂服务网格和高可用切换。
- 自动平掉用户启用跟单前的外部仓位，或在暂停跟单时自动清仓。

KOL 可从 Binance、VENUE 基础终端或其他客户端下单。跟单源始终是 Binance 账户的认证订单事实，与下单入口无关。基础终端是 MVP 必交付项，但不扩展为完整专业终端：不做图表策略、条件单、止盈止损编排、批量算法单或自定义工作台。

## 3. 角色与产品流程

角色只有四类：普通用户、跟随者、KOL、平台管理员。KOL 是管理员显式授予的有限角色，不允许普通用户自行升级；启用 KOL 总数硬限制为 5。

每个启用 KOL 在 MVP 中恰好绑定一个已验证的主交易账户、一份策略资本配置和一个公开页面；每个跟随用户只能归属一个 KOL，并且同一时间最多启用一个 Binance 跟随交易账户。用户可保存替换用凭证，但切换活动账户前必须暂停关系并重新通过启用门。所谓“绑定 KOL”是 Venue 中的关系，不把用户账户变成 KOL 可登录、可切换或可代操作的 Binance 子账户。

普通 Windows 终端免费开放：使用 `POST /v2/account/terminal/register` 注册，不要求邀请码、不创建 KOL 角色或跟单关系；登录后可绑定验证自己的 API、查询账户及管理 Binance Grid。Web 注册专用于跟随者，使用 `POST /v2/account/register`，必须提供有效邀请码，并在同一事务中绑定 KOL。两端共享用户登录和凭证服务，但注册入口不可混用；客户端不得通过注册字段自行指定 KOL 权限。付费 KOL 由管理员开通，自动收费与结算暂不实现。

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

启用使用当次权限/UID probe 与完整账户签名快照，不把连接成功或以前的 API 验证结果当作空仓证明。完成事务重验 request ID、relation revision、KOL 状态、凭证归属、新旧执行占用及未决命令；过期回调不得完成或拒绝用户后来提交的新请求。快照超过 30 秒或不是 Hedge 模式即拒绝启用。

恢复也是一个新的跟单起点：签名证明跟随账户为空后，当前可复制数量、目标数量与已核对数量归零；目标版本继续递增，历史成交和命令不删除。KOL 基线仓位和成交游标保留在关系的 `baseline_json`。旧版本缺少明确目标模型的活动关系不自动解释存量数量，必须暂停、核对并重新启用。

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
- `venue-executor-binance`：初期新链唯一物理交易进程，承载终端、跟单与 Binance Grid；旧 Copy worker、旧 Grid writer 及旧 Node 不得继续作为第二个生产入口。

Executor 使用 Tokio 异步任务管理所有账户：

- 最多 5 个 KOL 账户各持有一条认证私流，用于接收订单、成交和仓位事实；另可共享公共价格和规则连接。
- 跟随账户不创建独立进程，不运行 Strategy Actor，不保存策略 Checkpoint；活动跟单或未决镜像会维持认证私有投影。
- 跟随账户通过共享 HTTP keep-alive client、按需精确订单回读和低频签名仓位对账工作。
- 每个跟随账户只有一个有界串行队列；不同账户并发，同一账户的订单不得并发乱序。
- 每个关系保存有界订单映射，每个账户共享上限 16 的命令队列；同一原生来源订单的替代子单保留独立身份，不形成无界内存队列。

Executor 以部署副本数 1 运行，并在启动时取得一个 PostgreSQL 全局 advisory lock；取锁失败立即退出。它不是每账户 lease、选举或 handoff。数据库连接或全局锁丢失后停止产生新订单，只允许对已提交命令做只读收敛。

基础终端与自动跟单共用这个 Executor、Binance adapter、账户串行队列和命令账本，不建立第二个手动交易 writer。Control 验证登录用户对本人账户的命令并写入账本，不要求普通终端用户拥有 KOL 角色。终端不得接受客户端自造账户归属、数量归一化结果或 `clientOrderId`。

手动 Post Only 开仓是明确的 `terminal` 来源分支，不以持仓快照新鲜度、已有仓位 mark price 或同步完整私有 REST 作为前置条件。Control 和发送前只校验本人已验证凭证及运行归属；Executor 复用校时时钟、连接池与共享公开规则缓存（60 秒），按数量步长向下取整，非零后直接向 Binance 提交。冷连接/规则首次需要公开数据预热；最低名义、保证金、价格过滤与 Maker 拒绝由 Binance 判定。完整且身份匹配的认证 RESULT 可确认挂单，不冒充成交；缺失或不确定结果保持同 ID 签名对账，禁止自动重发。数值拒绝码以 `binance_<code>` 持久化，原始消息、签名和密钥不回显。平仓、撤单、自动 Copy 风险约束和 Grid 的签名事实路径不因该分支放宽。

桌面每次打开或切换账户时只向 Control 续订短期投影需求。Executor 在策略活动期间独立订阅认证私流，不依赖桌面登录。先建立用户流并以签名 REST 形成启动基线，此后 NEW、TRADE、撤单与 ACCOUNT_UPDATE 维护活动订单和双腿仓位，不定时轮询全账户 REST。成交和仓位事件可交叉到达，两者尚未覆盖同一成交时不得发布混合时点投影；连接空闲不算异常，真实帧/Pong 才更新连接覆盖时间。断流先暂停热路径，重连缺口、持续事件不完整或结果不确定时才做签名恢复。账户余额保持独立的原始观察时间，不因心跳伪装成新 PM 权益。数据先进入 PostgreSQL，再由 Control 按登录用户与 credential 双重作用域返回；历史委托仍只展示新 Executor 命令账本。

活动下单连接在策略私流基线启用前完成 Binance 校时，随后每小时后台刷新；网络刷新不持有账户下单锁，不清除仍可用的已验证偏差，失败或忙连接一分钟后重试。正常成交直接使用缓存时间，不触发校时。私流发布同时核验规范成交增量推导的双腿数量与实际 ACCOUNT_UPDATE 数量及成交覆盖时间；不把两类消息时间戳不完全相等单独判为缺口。重复成交精确去重，数量冲突或缺失覆盖仍暂停发布。

桌面持仓、挂单、成交、仓位历史与资产统一使用 `TerminalAccountProjection`，不回退到冻结 Node 的 `ExecutionFacts`；交易机器人区显示新 Grid 实例及按用户授权的带单实例。切换凭证和空响应均清除原账户数据与撤单选择，迟到的旧凭证响应不得覆盖当前账户；签名时间过期或查询失败须明确提示。命令历史全量响应替换列表，单个提交回执只更新对应记录。仓位历史为最近 500 条已观察的数量/均价变更，零数量表示对应持仓腿关闭，不冒充绑定前完整历史、独立交易周期或已实现盈亏。

桌面持仓表的 PnL 固定为 4 位小数，不显示更新时间。每行的“平仓 / 反开”使用所点行的 credential、规范交易对、LONG/SHORT 和数量上限，不从当前图表重新取值。确认框说明市价滑点、反开两笔交易非原子、已有反向持仓会追加，以及不会撤挂单或停止策略。

手动持仓动作由 `POST /v2/kol/terminal/positions/action` 写入同一账户命令队列。发送前仅读取 Control/Executor 已有的本人已验证账户和新鲜签名投影，在本地按当前腿数量向下裁剪，不额外向 Binance 查询账户/仓位；公开规则和签名时钟继续共享缓存。正常路径为市价 POST/RESULT 后读取该交易对的签名仓位。仓位刷新只覆盖该交易对，不把订单、资产或连接时间伪装成更新。常规私流更新追上后继续使用统一投影。

反开使用 migration `0027` 关联两条普通命令；不是新进程或新恢复框架。只有原腿平仓完整成交且签名仓位为零才释放反向开仓，数量取实际平掉的数量。拒绝、部分成交、剩余仓位或未确认结果不会继续反开；第二笔失败保留第一笔的平仓事实并显示原因。重启后按原 `clientOrderId` 查单，不重发；重复请求复用同一命令，同交易对已有持仓动作未完成时拒绝新动作。仅注册的手动动作进入此路径，Copy/Grid 不放宽其原有约束。

验收须覆盖双腿按钮与 4 位 PnL、切图/切账户、重复点击与请求重放、越权/失效凭证/过期投影、数量裁剪、平仓失败/部分成交/超时/重启、反开第二笔失败和提交后的仓位刷新。离线用 HTTP 请求计数证明 POST 前无签名仓位 GET；真实成交与延迟另经用户授权的 Canary 验证。

### 5.2 新旧互斥

任何 `(binance, trading_account_id)` 不得同时由旧 Node/Stage 7 和新 Executor 管理。账户加入新链前必须：

1. 停止并禁用旧服务及自动重启；
2. 确认旧 writer 已释放且没有 `Prepared / Submitted / Unknown`；
3. 签名读取当前仓位、普通订单和 Algo 订单；
4. 从旧部署配置移除该账户，再在新 Executor 中启用。

本 MVP 不迁移旧 Grid Actor、Checkpoint 或本地 WAL。存在旧未决事实的账户保持拒绝，不能通过清文件或换 API Key 绕过。KOL 与跟随账户都遵守新旧互斥。

## 6. 挂单同步与带单机器人

当前复制语义为挂单同步，完整授权、生命周期、数量、改单/撤单、恢复及 Web 边界见 [带单机器人与挂单同步](LEADER_ORDER_MIRROR.md)。

管理员按用户显式授权，默认不可见、不可创建、不可启动。KOL 用本人的已验证主账户创建一个带单实例；用户邀请注册、API 权限和跟单激活仍独立验证。撤权禁止新增同步，并保留已有实例状态与停止操作。

主账户启用后的普通 UM GTC/PostOnly 限价单按资金倍率同步，保留价格、买卖方向和 positionSide。部分成交不重复挂单，改单先确认旧子单终态及自身累计成交再替换，主单结束撤销子单剩余量；市价单与 Algo/条件单不复制，不补追主从仓位差异。

平仓为领域只减仓意图，按新鲜签名持仓扣除已有平仓预留后限制数量。PM UM Hedge 原生普通订单省略 reduceOnly 参数。暂停/撤权不自动平掉已有仓位；未决请求保持原 clientOrderId 查单。

旧成交目标模型只用于既有历史与未决命令恢复。新激活关系使用 target_model=2，迁移暂停旧活动关系并取消其未发送命令，重新启用必须通过空仓/无挂单门。

## 7. 幂等与轻量命令账本

跟单路径不使用每账户 JSONL WAL、Actor Applied、hash-chain receipt、manifest 或 Checkpoint。PostgreSQL 轻量命令账本是该路径唯一的发送前持久记录。

最小记录包括：关系/revision、源订单身份、机器人与授权版本、子单序号、跟随账户、symbol、position side、委托与累计成交数量、规则版本、确定性 `clientOrderId`、状态、时间和脱敏错误。唯一约束至少覆盖：

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
- `Accepted` 只代表交易所接受，不代表成交。Post Only 放单命令在精确签名回读逐字段证明活动订单存在后即记 `Reconciled`，订单后续成交/撤销由私有投影跟踪，不能以一张长期挂单占住账户队列；市价与旧成交复制恢复仍须订单、成交及更新签名仓位相互一致才记 `Reconciled`。
- `Rejected` 不自动重发原命令；后续新的 KOL 目标可产生新的确定性命令。
- Grid 普通 Maker 补撤使用提交事务验证的热令牌，不逐笔同步查询时钟、规则、BBO 或账户。完整、逐字段匹配的 Binance mutation RESULT 可直接确认活动挂单或终态撤单；身份型 ACK、成交竞态、缺字段和超时仍进入同 ID 对账，市价补库存/减仓仍走签名确认。正常补撤不逐单同步 REST 回读，也不定时拉全账户快照。先完成成交滚动，冷路径再评估附属风险动作：只有达到盈利条件的减仓候选才读取显式汇率与 PM 账户权益；核验失败只推迟减仓，不阻塞普通补撤。认证成交接收至首/末 send-entry 与全部响应完成分别计时，不以完成时间冒充出网时间。
- 重复 KOL 帧、重复数据库轮询、进程重启和重复页面操作只能返回已有记录，不得产生第二个物理订单。
- `Accepted / ReconcileRequired` 的精确签名回读按 PostgreSQL 中的尝试次数和下次时间执行 500 ms 起、8 s 封顶的确定性指数退避；未到期仍保持账户栅栏但不访问 Binance。无法收敛时保持 `ReconcileRequired` 并标记账户 `NeedsAttention`；不得用一次 404 证明未下单。

旧成交模型的市价命令在现有账本中额外保存不可改写的 `market_baseline` 与 `signed_settlement`：前者是发送前签名腿数量和本次实际归一/裁剪数量，后者是同订单真实成交量与更新后的签名腿数量。只有成交和仓位变化一致时，才在一个事务中完成命令并回写跟单实际数量。部分终态成交只按实际量记账；后续 dirty target 从该实际量重新计算，不重复发送整份目标。这是单笔订单的对账数据，不是策略 checkpoint 或本地 WAL。

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

验证成功的 API 绑定长期保留并可跨 UI 登录会话继续选择，不按固定分钟数失效；启用跟单和每次增险仍要求当次签名基线或新鲜私有投影。运行中出现权限/身份/模式变化、认证拒绝、投影过期或解密失败时只暂停该账户，不删除密文、不自动换 Key、不影响其他账户。

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

- 在现有 `apps/venue-control` package 内新增薄 binary `src/bin/venue-executor-binance.rs` 和按职责拆分的 `executor_runtime/`、`executor_store/`、`executor_exchange/`、`order_mirror/` 模块，管理员授权入口为 `src/bin/venue-leader-bot-admin.rs`；旧 `venue-copy-worker` 在开发期只作冻结参考，MVP 发布清单中不得与新 binary 并存。
- 在 `apps/venue-control/migrations/` 增量增加 KOL、邀请、唯一归属、跟单设置、源成交/目标版本、命令账本和执行投影表；不改写旧 migration 或旧恢复记录。
- 在 `apps/venue-control/src/accounts/`、HTTP/service/repository 现有边界内扩展用户会话、KOL 权限和凭证授权；不建立第二个认证服务。
- 在 `crates/venue-control-protocol` 增量加入邀请、KOL 页面、终端和跟单状态 DTO；不复用旧 Node delivery DTO 作为新 Executor 协议。
- 在 `crates/venue-gateway-binance` 补齐 Portfolio Margin UM 认证账户流、Post Only 限价、市价平仓、精确撤单和同代签名回读；不复制签名 HTTP client。
- 在现有 `apps/ui/web` 增加 `/join/<invite_code>`、注册/登录、API 管理、跟单状态和 KOL 页面编辑；桌面终端只位于同级的 `apps/ui/desktop`。

挂单同步复用唯一 Executor，管理员工具只修改授权与审计。冻结旧 Node 的恢复数据格式不变，不作为新链依赖。

当前代码包含 `0017`–`0029` 数据契约与 `schema.rs` 版本化安装、邀请/KOL/跟单/Grid HTTP、唯一 `venue-executor-binance`、私流与签名 REST 投影、Post Only/市价平仓/精确撤单命令账本、耐久签名回读退避及桌面消费链。真实凭证联调、旧账户迁移和 2 核 4 GiB/真实 Canary 仍须按验收门执行；存在旧 `venue_control_strategy_scopes` 的账户在 Control 入账与 Executor 抢占两处均保持拒绝。

## 10. 改造与发布边界

实现入口与管理员授权操作见 [带单机器人与挂单同步](LEADER_ORDER_MIRROR.md)。源码、离线 fixture、隔离 PostgreSQL、浏览器、目标主机容量及真实 Canary 分别验收；前者通过不等于后者完成。真实部署、旧账户停止或迁入、真实交易必须另获当前任务明确授权，不沿用历史计划中的金额或停机许可。

## 11. 验收与性能容量门

### 11.1 功能验收

- 专属链接注册后绑定正确 KOL；重复访问其他链接不换绑；邀请绑定本身不产生任何订单。
- 用户可登录、退出、恢复会话；退出后 Cookie/SSE/写操作立即失效。
- Binance API 的统一账户、交易权限、提现关闭和双向持仓均由签名事实验证；API 状态、Executor 在线和跟单 Active 分开显示。
- KOL 基础终端可显示签名持仓、活动订单和成交；市价/限价四种开平仓及精确撤单均走同一 Executor，页面区分已提交、已接受与已成交。
- KOL 只能修改自己的页面，固定风险提示不可编辑，任何 KOL 页面与接口都看不到跟随者 API 明文。
- 一次 KOL 新限价挂单可正确产生对应多/空腿委托；同事实重复 100 次仍只有一个对应子单，改单和撤单保留精确身份。
- 跟随账户之间故障隔离；一个账户 `Rejected/ReconcileRequired` 不停止其他账户。暂停停止新挂单并撤销程序创建的子单剩余量，不自动清仓。
- Executor 重启后 `Pending` 可继续调度，`Sending/ReconcileRequired` 只查不重发；`Accepted` 必须经签名事实后才显示完成。
- 新旧实现不得同时操作同一账户，旧 Grid/Gate/Bitget 没有因本次发布被启动、迁移或宣称完成。

### 11.2 性能指标

所有本地指标从 Executor 收到已解析 KOL 订单变化的单调时钟开始，分别记录 PostgreSQL 提交、排队、HTTP send start、Binance ACK、精确回读和 UI 可见时间。交易所网络耗时单独报告，不混入本地调度承诺。

在 2核4G、同机 Web + Control + PostgreSQL + Executor、连接预热且 Binance 使用可控延迟 fixture 的环境，以 5 个 KOL、200 个已启用跟随账户、一个 KOL 挂单同时 fan-out 为基准：

- 订单接收至源投影、关系映射及本次合格 `Pending` 命令提交：p95 ≤ 20 ms；
- 订单接收至单个跟随请求开始发送：p50 ≤ 100 ms、p95 ≤ 500 ms、p99 ≤ 1 s；
- 200 个合格跟随请求全部开始发送：≤ 1.5 s；
- 无丢失、无重复物理发送、无跨账户串行阻塞；队列不得无界增长或静默丢弃。

30 分钟稳态与重复突发期间：整机不得持续 swap，全部 VENUE 服务与 PostgreSQL RSS 合计峰值 ≤ 3 GiB，CPU p95 < 80%，数据库连接、文件描述符和队列均低于配置上限的 80%。超载必须记录并暂停受影响关系，不能用丢事件保持低延迟。

真实 Binance Canary 只报告实测 p50/p95 和样本量，不用少量真实订单伪造可靠 p99。若本地容量门通过而外部 ACK 变慢，应单独归因为网络或交易所；若本地门未通过，2核4G即判定不足，升级规格后重新验收。只有单机 200 账户实测失败且优化无效，后续阶段才讨论 Executor 分片。

## 12. 上线退出条件

只有同时满足以下条件才可称为“Binance KOL 跟单 MVP 可用”：产品功能、安全验收、2核4G容量门、至少一组 KOL 与两个不同真实跟随账户的真实成交闭环全部通过；运行中无未决 `ReconcileRequired`；目标账户无旧 writer；密钥恢复材料已在仓库外备份；部署入口、停止方式和当前账户状态已有记录。

任何一项缺失均应标记为“未完成”或“受限试运行”，不得以代码已提交、HTTP ACK、模拟测试、旧历史证据或页面可见代替。
