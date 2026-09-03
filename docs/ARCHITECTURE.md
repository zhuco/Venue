# VENUE 当前代码与 Binance KOL MVP 目标架构

更新：2026-09-03

本文同时说明“仓库已经有什么”和“下一步要变成什么”。已提交代码、离线测试和真实业务可用是三个不同结论；目标架构尚未实现的部分不得描述为已上线。

KOL 产品契约见 [`KOL_COPY_MVP.md`](KOL_COPY_MVP.md)，Binance Grid 重建契约见
[`GRID_RUNTIME_REFACTOR.md`](GRID_RUNTIME_REFACTOR.md)，代码入口见 [`CODEMAP.md`](CODEMAP.md)，开发与构建见 [`DEVELOPMENT.md`](DEVELOPMENT.md)。

## 1. 当前产品范围

- 只支持 Binance Portfolio Margin UM；必须验证统一账户、UM 交易权限和双向持仓。
- 初期最多 5 个由管理员开通的 KOL、200 个启用跟单账户；容量必须覆盖单个 KOL 对全部 200 个账户的突发扇出。
- 用户通过某个 KOL 的邀请链接进入，注册事务默认建立该 KOL 的唯一归属。
- 用户可登录、绑定/验证自己的 Binance API Key、设置并暂停跟单。
- KOL 可使用基础交易终端操作自己的主账户，并编辑自己的公开页面标题和说明。
- Copy 只由 KOL 的 Binance 认证账户流真实成交增量触发，并以签名 REST 补查恢复，目标是尽快开始跟随下单。
- Binance Grid 正迁入统一 Executor；Scalping、Gate.io、Bitget、Bybit、OKX、Hyperliquid、收费、公开市场和高可用均不在当前范围。

## 2. 目标进程拓扑

```text
Browser -> Venue Web / same-origin BFF（邀请、账户、KOL 页面） -> venue-control
VenueFlow Desktop（用户作用域终端投影与命令） -------------> venue-control
                                                                  -> PostgreSQL
                                                                  -> venue-executor-binance
                                                                       -> 最多 5 条 KOL 私流
                                                                       -> 账户内顺序队列
                                                                       -> Binance Portfolio Margin API
```

目标部署不为每个账户启动进程，不为终端或 Copy 创建 Strategy Actor。跟随账户默认不保持完整私流；发送后签名查单并周期对账。若实测 REST 限频或状态时效不足，再按证据增加有界私流，不预先复制旧 Private Router。

## 3. 组件职责

| 组件 | 目标职责 | 明确不做 |
|---|---|---|
| Venue Web/BFF | KOL落地页、邀请注册、登录、API绑定、跟单设置和KOL文案编辑 | 不接收或返回解密后的API Secret，不直连Binance |
| VenueFlow Desktop | Binance风格行情、可增删交易对、私有账户多标签、Post Only四动作和市价平仓 | 不持有API Secret，不直连私流，不自行判定成交 |
| venue-control | 认证、授权、KOL/邀请/关系、凭证加密与只读验证、查询投影 | 不直接发送交易所mutation |
| PostgreSQL | 业务事实、唯一耐久命令账本和 Grid 批内派发顺序 | 不保存明文凭证或原始私流payload |
| venue-executor-binance | KOL私流、成交归一化、快速fan-out、事实驱动Grid、账户顺序队列、规则/数量校验、下单与签名查单 | 不运行Scalping，不建立Actor/checkpoint/local WAL |
| Binance adapter | 签名、原生symbol/数量/持仓模式、订单与成交协议转换 | 不包含用户、KOL、邀请或页面逻辑 |

Control、Web 和 Executor 复用现有 Tokio、reqwest、tokio-tungstenite、serde、rust_decimal、SQLx、secrecy、zeroize 和 ring；不为本轮引入第二套 ORM、数据库或消息队列。

## 4. 关键数据关系

```text
KOL 1 ── N Invite
KOL 1 ── 1 LeaderTradingAccount
KOL 1 ── N FollowerBinding ── 1 VenueUser
VenueUser 1 ── N ApiCredential ── 1 TradingAccount
FollowerBinding 1 ── 1 CopySetting ── 1 ActiveFollowerTradingAccount
KOL Fills ── TargetRevision ── N CopyCommand（按跟随账户最新目标）
```

- 一个用户在 MVP 中最多绑定一个 KOL；邀请码由服务端解析，客户端不能提交任意 `kol_id`。
- 一个启用 KOL 只有一个主交易账户和策略资本配置；一个普通用户同时最多启用一个跟随账户。Venue 关系不授予 KOL 登录或切换跟随者 Binance 账户的能力。
- 邀请归属只建立业务关系，不自动启用实盘跟单。用户完成 API 验证、设置额度并明确确认后才启用。
- KOL 只能编辑自己的公开内容并查看脱敏汇总；不能查看跟随者 API Key/Secret，也不能以跟随者身份下单。
- 用户可暂停或撤销后续跟单；暂停不自动平仓，页面必须明确提示当前仓位仍由用户承担。

## 5. 快速跟单数据流

```text
KOL Binance TRADE 成交
-> Executor 从认证账户流解析、按 exchange trade id 去重
-> PostgreSQL 同一批量事务保存 LeaderFill、关系目标版本及可发送的 CopyCommand
-> 有界并发投递到各账户顺序队列
-> 按 multiplier、限额、实时步长与 positionSide 生成跟随订单
-> Binance
-> ACK 后精确查单；超时进入 ReconcileRequired 并按同一 clientOrderId 查询
-> 订单/成交/失败投影返回用户页面
```

不按按钮点击、`NEW` 订单或本地推测状态触发 Copy。每个部分成交都持久化并更新关系目标，但同一腿已有在途命令时只合并为最新目标，不强制每个成交发送一张订单。源成交唯一键为 `(leader_account, symbol, exchange_trade_id)`；物理命令唯一键绑定关系 revision、目标 revision、跟随账户、symbol、position side 与 phase。平仓在领域层必须是只减仓意图，发送前按跟随账户同代新鲜签名持仓裁剪；Portfolio Margin UM Hedge Mode 的原生请求使用明确 `positionSide`，不发送该模式禁止的 `reduceOnly` 参数。

## 6. 最小执行与故障语义

- 目标部署中的单个 Binance Executor 是新链唯一交易出口；同一账户命令在进程内严格顺序执行。
- 命令发送前写 PostgreSQL；稳定 `clientOrderId` 有唯一约束。
- Grid 热路径目标是从认证成交到去重/单次规划，再以一个事务提交成交分配、anchor/desired surface、订单归属和完整命令批次，提交成功后立即唤醒账户队列。预热态“事件收到→事务提交并唤醒”p95 不超过 10 ms；交易所发送和 ACK 不计入该指标。
- Grid 批次使用 `grid_batch_id` 与 1 起始的 `dispatch_sequence` 持久排序，单批最多 16 个 mutation；计划变化但无需 mutation 时仍提交 0 命令收据。非空批次先并发启动全部 Place；只有全部 Place 越过本地 transport send-entry 后才启动 Cancel，不等待 HTTP ACK。任一 Place 未进入发送即失败时，本批 Cancel 不发送；发送后的不确定结果只按原 ID 对账，绝不重发。
- 连续认证私流可在新鲜签名基线、连续序列和新鲜 BBO 门内推进成交增量，不为每笔成交同步等待完整签名 REST；任一门失效立即停止 mutation，并以签名 REST 异步补漏/恢复。
- 状态只保留 `Pending / Sending / Accepted / Rejected / ReconcileRequired / Reconciled`，以及仅发送前可用的 `Cancelled`。
- Post Only 放单在精确签名回读证明不可变订单语义及原生身份后完成命令并释放账户队列；活动订单的后续成交/撤销由私有投影承接。市价命令仍须成交与同代签名仓位收敛。
- `Accepted / ReconcileRequired` 的精确签名回读使用 PostgreSQL 持久化的 500 ms 起、8 s 封顶指数退避；未到期仍封锁该账户但不发起网络读取。`ReconcileRequired` 表示请求可能已被交易所接收；确认前不重发，并暂停该账户后续增险。
- 一个账户认证、限频、余额或查单失败只暂停该账户，不退出整个 Executor。
- 全局并发和每账户队列必须有界；429 按 Binance 响应退避，不能通过无限任务或线程提高速度。
- 当前不做多 Executor 选举、分布式 fencing、每账户进程锁、writer lease、JSONL WAL、Actor receipt 或恢复 manifest。

迁移 `0023_binance_grid_hot_batch.sql` 提供最小批次收据、Grid 必填批次 ID、1–16 顺序约束、历史 Grid 单命令 legacy 批次回填及索引；`0024_binance_grid_batch_chain.sql` 再绑定输入 desired 摘要、唯一前驱和实例批次尾，使跨微批成交能连续规划而不能越过前批签名确认出网。Copy/终端批次字段保持 `NULL/NULL`。认证私流成交的 socket 代、签名 baseline 代和订单进度上下文只能全空或全有效。当前代码已接通原子 Store、提交唤醒、严格领取、一次性热令牌和事件至 send-entry 计时；未完成 PostgreSQL 实库压力、真实 Binance Canary 与预热 p95 验收前，不得描述为已经上线或已满足 10 ms。

## 7. 凭证与权限

- 密码继续使用 Argon2id；服务端会话使用随机 token 摘要，浏览器使用 Secure/HttpOnly/SameSite Cookie 和 CSRF 防护。
- API Key/Secret 在 Control 使用 AES-256-GCM 随机 nonce 加密，AAD 绑定用户与 credential ID；PostgreSQL 只保存密文、指纹、掩码和验证结果。
- 加密主密钥来自部署环境中的 `VENUE_ACCOUNT_MASTER_KEY`，不得入库、入 Git 或进入日志。MVP 先复用现有实现，密钥轮换/KMS 在真实需要前不扩展。
- 只有 Control 验证路径和 Binance Executor 可短时解密；KOL、浏览器、Copy配置和投影永远不获得明文。
- API 必须开启读取和 UM 交易、关闭提现；部署要求用户将 Key 限定到 Executor 出口 IP。权限、Portfolio Margin、UM 与双向持仓任一不满足即不可启用。

## 8. 当前代码复用与冻结

直接复用：

- `apps/venue-control/src/accounts/` 的注册、登录、会话和凭证加密；
- `crates/venue-gateway-binance/src/credential_probe.rs` 的只读验证；
- Binance adapter 的签名、规则、订单、成交和双向持仓协议；
- `venue-domain` 的规范类型、Decimal 和 Symbol；
- `apps/ui/web` 的 Next.js/BFF、安全响应头与响应式基础；
- 现有 Copy 纯数量计算中经过专项验证且不依赖 Actor/WAL 的部分。

冻结、不作为新链依赖：

- `apps/venue-node` 的每账户 resident、AccountRuntimeHost、Execution Lane 和 Actor 组合；
- 旧 Copy delivery/Actor Applied/ledger recovery 链；
- 本地 JSONL WAL、facts/checkpoint、writer lease、canonical root 与 handoff；
- 旧 Grid/Scalping 和 Gate.io、Bitget、Bybit、OKX、Hyperliquid 接管路径。

冻结代码可继续编译或接受必要维护，但不得为了“复用”把其复杂恢复契约带入 MVP。新旧链按 `trading_account_id` 严格互斥；旧工件不得自动删除。

## 9. 容量与演进

初始形态只部署一个 Binance Executor。最多 5 个 KOL 常驻私流，跟随账户共享进程和连接池；KOL 成交突发以有界异步 fan-out 处理。
2 核 4 GiB 是否可承载目标数量必须由 5 KOL/200 模拟跟随账户压测决定，文档不预先承诺。

只有单实例资源或 Binance 限频被测量证明不足时，才把账户按稳定哈希静态分配到少量 Executor 分片。分片仍由数据库保证一个账户只有一个活动归属；本轮不实现自动迁移、选主或热备。

## 10. 当前完成度

仓库已有邀请/KOL/跟单/Grid 入口、唯一生产 `venue-executor-binance`、账户级私流+签名 REST 投影、Post Only/市价平仓/精确撤单命令账本和 VenueFlow 桌面消费链。Executor 取得 PostgreSQL advisory singleton 后执行 activation、未终态同 ID 回读和有界私流；投影先持久化再按用户返回。旧 Binance LIVE scope 同时在 Control 入账与 Executor 抢占处拒绝，旧状态不会按时间自动释放。真实凭证 Canary 和 2核4G容量门仍未完成。

完成标准只以 [`KOL_COPY_MVP.md`](KOL_COPY_MVP.md) 为准。真实凭证联调、2 核 4 GiB 压测和隔离账户 Canary 仍是外部验收；文档更新本身不启动服务、不迁移账户，也不表示实盘已可用。

<a id="deprecated"></a>

## 11. 停用与兼容入口

- 根 `hedged-grid-*` 旧生产 binary、旧 `/v1` KOL 后端、模拟交易 DTO 和 PID target 不得恢复为新入口。
- `apps/venue-node` 六个 binary、Stage 7、旧 recovery collector 和 Actor Applied 仍可能被冻结代码或工件读取；删除前必须证明没有调用、运行账户或恢复依赖。
- `G:\kol` 是外部 UI 参考，不是本 workspace 的构建或运行依赖。
- Node.js 24、Rust 1.98.0、Next.js 16.3.3、React 19.2.8、TypeScript 7.0.2 等当前锁定版本不因架构文档更新而批量升级。
