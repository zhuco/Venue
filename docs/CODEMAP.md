# VENUE 功能入口

更新：2026-09-03

当前目标是 Binance KOL 跟单 MVP 与事实驱动 Binance 对冲网格共用单例 `venue-executor-binance`；初期全站最多 5 个启用 KOL、200 个启用跟单账户。Scalping 和其余交易所迁移暂停。下面多数 Runtime/Node 入口是已提交代码或冻结兼容位置，不等于新链调用链。长期文档统一在本目录，仓库根 CODEMAP 仅作导航。

当前可复用入口：`apps/venue-control/src/accounts/`（注册、登录、会话、凭证密文、邀请码归属、KOL、跟单、终端和 Grid 生命周期）、`crates/venue-gateway-binance/src/{credential_probe,account_gateway,execution,grid_market}.rs`（Portfolio Margin UM 验证、私流/签名 REST、Post Only、市价平仓与 Grid 公开事实）及同级 `apps/ui/{web,desktop}`。两套 UI 的入口判断先看 `apps/ui/README.md`。新链由 `0017`–`0024`、版本化终端/Grid/投影 DTO、用户作用域私有投影、旧 writer 门禁和唯一 `venue-executor-binance` 组装；启动仅在 explicit LIVE、PostgreSQL 和既有主密钥均有效时进行。

项目/版本总览见 [`README.md`](../README.md)，开发与合并方式见 [`DEVELOPMENT.md`](DEVELOPMENT.md)，旧方法状态见 [停用入口](ARCHITECTURE.md#deprecated)，产品版本和变更范围见 [`VERSION`](../VERSION) / [`CHANGELOG.md`](CHANGELOG.md)。

本文件只回答“当前功能代码在哪里”。当前目标、目标架构与验收查 [`KOL_COPY_MVP.md`](KOL_COPY_MVP.md)，
当前/目标差异查 [`ARCHITECTURE.md`](ARCHITECTURE.md)。只有维护冻结 Grid/旧 Node 或已有恢复工件时才查
[`GRID_RUNTIME_REFACTOR.md`](GRID_RUNTIME_REFACTOR.md)；旧三目标文件只保留路径导航。

## 进程、配置与通用领域

合并后 Git 源码总预算为 12 MiB，单文件仍为 2 MiB；`content-releases/`、`handoff-staging/` 与既有运行工件均排除跟踪。
依赖门禁按当前用途允许 Control 的 `argon2/ring` 密码与加密、VenueFlow 的 `time` 本地时区；`zeroize` 遵循凭证清理基线。
Node 投影摘要辅助位于 `apps/venue-node/src/control_loop/projection_digest.rs`；手动交易协议位于 `crates/venue-control-protocol/src/trade.rs`。
当前 Web 的 Control 用户会话只在 BFF 环境注入，这是待替换的运维入口；MVP 必须改为真实用户安全 Cookie 会话。HTTP/SSE 继续保留 Control 账户归属校验，不复用 Node token；部署配置见 `docs/WEB.md`。

下表中只有明确写为“KOL MVP 契约/目标复用”的条目可进入新链。名称含“冻结/旧/Node/Actor/WAL/Stage 7”或 Binance 以外交易所的条目都只描述仓库现状与恢复兼容；即使正文使用现在时，也不表示当前开发或部署目标。

| 功能 | 首要入口 | 已提交边界/目标用途 |
|---|---|---|
| Binance KOL MVP 契约 | `docs/KOL_COPY_MVP.md` | 邀请注册、登录、API绑定验证、KOL页面与终端、快速成交复制、轻量命令账本、P0–P5和验收的唯一入口 |
| 构建、依赖与仓库体积门禁 | `Cargo.toml` | workspace 当前包含根 package、`venue-copy`、`venue-control-protocol`、`venue-domain`、`venue-execution`、`venue-indicators`、`venue-runtime`、`venue-storage`、`venue-strategies`、`venue-gateway-api`、六个 `venue-gateway-*` adapter、`apps/venue-node`、`apps/venue-control` 与 `apps/ui/desktop`，resolver 固定为 3；独立 Next.js 应用位于 `apps/ui/web`。workspace 与 `rust-toolchain.toml` 共同锁定 Rust 1.98.0；`.cargo/config.toml` 固定主缓存为 `G:\Build\Venue\main`；`Cargo.lock`；`scripts/verify_repository_hygiene.ps1` 执行体积和运行态文件门禁 |
| 本机构建资源约束 | `docs/DEVELOPMENT.md` | `Invoke-VenueBuild.ps1` 与 `venue_build_guard.ps1` 管理 main/slot-1/slot-2、最多2个受控构建、150 GiB缓存准入及F/G空闲检查；main 临时关闭外层 wrapper 以保留增量，退出恢复原环境，隔离槽保留 wrapper；专项脚本持锁到验证完成，不自动删除缓存 |
| 冻结旧账户执行链 | `crates/venue-execution/src/account_host.rs`、`crates/venue-runtime/src/account_lane.rs` | 现有进程锁、WAL、Owner、Unknown 与 handoff 仅用于旧 Node/工件兼容；KOL MVP 不继续扩展或复用。旧账户未安全收敛前不得分配给新 Executor |
| 冻结旧签名快照与成交恢复 | `crates/venue-execution/src/{account_snapshot,account_recovery_request}.rs`、`account_cursor_tests.rs` | 余额保留原资产、未知可用金额不填零；订单状态与已成交量只保留来源明确的值。`AccountRecoveryRequest::read_only` 为新投影建立无未决 mutation 的账户级签名读取，旧 checkpoint 恢复仍从 `previous_fills_cursor` 单调推进。HL 的账户级协议覆盖非所选币种事实与保留窗口缺口；单交易对旧 parser 不可冒充账户完整性 |
| 冻结六所网关能力门禁 | `crates/venue-gateway-api/src/lib.rs` | 规范 venue 固定 Binance、Bitget、Bybit、Gate.io、Hyperliquid、OKX；运行模式只接受精确 `LIVE`，`PublicMarketBinding` 另提供无账户、无凭证、无 mutation 的 Binance USD-M 公共行情 scope；旧 `capability_promotion.rs` 的普通 `promote_capability/authorize` 入口继续固定 `AuthorityUnavailable`，序列化 probe 不能升级能力；先前 Bybit、OKX、Hyperliquid 计划使用的 Host/WAL/permit 链现已冻结 |
| Binance Portfolio Margin adapter | `crates/venue-gateway-binance/src/{account_gateway,account_gateway_projection,transport,private_ws,readback}.rs` | `credential_probe.rs` 提供账户/API、Portfolio Margin UM、交易权限与双向持仓只读验证；认证私流与规范 TRADE 映射由 `account_gateway.rs` 拥有，`account_gateway_projection.rs` 只组合完整签名账户快照。断线、过期和语义 gap 触发签名 REST 补读；`execution.rs` 的 Hedge Market 不序列化原生 `reduceOnly`。|
| Bitget adapter | `crates/venue-gateway-bitget/src/{account_gateway,public_ws}.rs` | UTA v3 账户级未成交订单分页读取全部 delegateType；风险汇总只接纳可精确归一化的 normal 行，任何尚未支持的条件/策略单均拒绝准入，不能因 mutation profile 不支持就当作账户不存在该订单。认证 `UTA/fill` 只在最后一个完整签名 snapshot attempt 内有界规范化，刷新、漂移、断线、坏帧或超限即清流关闭；它不是 Runtime generation。Grid 初装只用 books-only WS 的同连接 snapshot 与覆盖 update，经规范 `OrderBook` 重建 BBO；规则必须仍匹配该签名 snapshot generation，缺 `maxOrderQty`、断层、旧/未来时间或任何流错误均暂停，绝不以 REST/ticker 替代。`private.rs`、`order_families.rs` 解析官方 orderStatus/reduceOnly 与旧 fixture 兼容形状；`transport.rs`、`private_ws.rs`、`execution.rs` 保留已验证签名/私流/精确回读。`recovery.rs` 与 `runtime_recovery.rs` 的旧证据链仅供迁移兼容 |
| Gate.io adapter | `crates/venue-gateway-gate/src/account_gateway.rs` | 账户 adapter 负责规则、Hedge 双腿、完整订单族、fills cursor、policy-exact place/cancel/reduce 回读，经固定 Node 的统一 Host/WAL/Lane 执行；认证 `futures.usertrades` 仅在同一已签名快照 attempt 内有界规范化、按序交付，漂移/断线/坏帧关闭，Grid 初装读取新鲜规则+BBO。`account_gateway_priced.rs` 保留用户价格和数量上限，专项位于 `account_gateway_tests.rs`。`risk.rs` 区分存量数量转换与真实 `order_size_max` 新单检查，旧风险回放不伪造最大下单量。`transport.rs`、`private_surface.rs`、`execution.rs` 为协议实现；旧 `recovery_session.rs` 与 `recovery.rs` 不授予新链 writer；生产接管仍须单独验收 |
| Bybit V5 adapter | `crates/venue-gateway-bybit/src/account_gateway.rs`、`account_gateway_tests.rs` | 固定 UTA2/双向持仓预检、实时 linear 规则、policy-exact PostOnly/Gtc limit place/readback、精确 cancel 和签名仓位约束的 reduce-only；MarketReduce 只减已有腿，不准入市价增险或止损单。原始 mutation transport 为 crate-private，只消费统一 Host permit；账户专项在同 crate 测试文件，生产接管单独验收 |
| OKX V5 adapter | `crates/venue-gateway-okx/src/account_gateway.rs` | 固定 Long/Short + Cross、实时 SWAP 规则、policy-exact PostOnly/Gtc limit place、精确 cancel 与 `clOrdId` signed readback；`public.rs` 按 `ctVal × ctMult × contracts` 在 base 数量与张数间换算并向下取整，不突破 10U；ACK 正常解析失败但订单行 `sCode=0` 时必须持久化 `Unknown` 并签名回读，禁止误记 `Rejected` 后继续增险；原始 POST 为 crate-private，生产接管单独验收 |
| Hyperliquid adapter | `crates/venue-gateway-hyperliquid/src/account_gateway.rs` | 固定主账户/API Wallet 绑定、meta/clearinghouse/open-orders 预检、ALO/GTC limit place、精确 cancel 与 cloid readback；nonce 在签名前原子持久化且单文件上限 4 KiB，`/exchange` 为 crate-private 并只消费 host permit；生产接管单独验收 |
| 根 package 离线验证 | `src/bin/verify-grid-{inventory-recovery,exposure-shadow}.rs` | 根 package 不再有通用 CLI 或生产 mutation binary；两个 verifier 只读取既有工件 |
| 冻结六所账户 Node | `apps/venue-node/src/lib.rs` | 当前代码仍包含每账户 resident、Runtime/Lane/Host、Grid/Scalping/旧 Copy 组合；本轮不继续接管或把该组合扩展给新用户。轻量 `venue-executor-binance` 的实际入口创建后必须在本表登记 |
| Control→Node delivery 与 Node→Control 投影 | `apps/venue-node/src/{control_delivery,projection_outbox,control_loop}.rs`、`apps/venue-control/src/node_projection_postgres.rs` | delivery 的 claim、ACK、receipt 每次 await 后重验 lease/session/epoch。projection 按账户/node/instance 的独立游标写入耐久 outbox，精确回显才确认；PG 在同一事务合并本实例投影、保留其他实例/账户并更新 scopes/events，0012 迁移从旧 envelope 恢复实例游标。外层有界 Copy execution evidence 复用原始结果，`copy_execution_postgres.rs` 校验原 delivery 并与游标原子记录，不进入浏览器 facts；两条路径均不授予交易权限。停止/清仓端到端测试位于 `control_loop/control_loop_tests.rs` |
| Actor Applied 持久化证据 | `crates/venue-storage/src/actor_applied.rs` | `create_new` 与 `open_existing(anchor)` 耦合 hash-chain journal 和原子 checkpoint；anchor 精确绑定 root/tail/count/checkpoint，缺件、截断、scope/generation/turn/replay/WAL 回退失败关闭。Node 经 `venue-runtime::account::AccountRuntimeHost` 校验 Actor 所引用 head 确属同一 WAL 的真实历史前缀，允许 Actor Applied 后正常追加命令再重启，但拒绝伪前缀；`account::copy_actor::recover_copy_actor_applied` 仅恢复原语义回执，不签发当前执行 turn。receipt 不是身份、WAL 或 mutation authority |
| 冻结多策略账户运行时内核 | `crates/venue-runtime/src/account/mod.rs` | `registry.rs`、`private_router.rs`、`market_hub.rs`、`reconciler.rs` 与 `recovery.rs` 固定账户恢复/路由边界，`runtime_error.rs` 集中运行时失败语义；旧 `physical_recovery*` 多层证据链保持 Stage 7 迁移兼容且不再扩展。先前 Bybit、OKX、Hyperliquid 计划使用的 `AccountMutationHost`、账户锁、分段 WAL、Unknown 与 permit 链均已冻结，不进入当前 MVP |
| 策略 Actor 宿主与邮箱 | `crates/venue-runtime/src/strategy/mod.rs` | 私有事实与 Delta/Trade/Bar 有界无损；仅 Snapshot/Ticker/MarkFunding 合并；私有 burst 64 后让行对账/控制；一个实例一个 runtime-issued turn，durable applied receipt 后才可 Running/输出授权意图；`src/runtime/strategy/mod.rs` 只兼容重导出 |
| 共享私有事实调度 | `crates/venue-runtime/src/shared/private_facts.rs` | effect 调度、session generation/ticket、周期刷新、退避与 readiness/snapshot；单 in-flight 且证据尾绑定，跨 generation 结果拒绝；根 worker 仅保留兼容 facade 与 Binance 协议/REST/WS 组合，不持有 writer/WAL/mutation |
| 旧 Copy 规划与 Node 桥 | `crates/venue-copy/src/lib.rs`、`apps/venue-node/src/production_resident/copy.rs` | 仅复用不依赖 Actor/WAL 的纯 Decimal 数量计算；delivery、Actor、journal、跨层 receipt 与恢复链冻结，不是新 MVP 的执行入口 |
| 版本化 Control 协议 | `crates/venue-control-protocol/src/{lib,accounts,kol,grid}.rs` | 既有 schema v2 Node/Control DTO 保持冻结；KOL schema v1 固定邀请/跟单，终端命令 schema v2 明确 Post Only、确认后的 close-only Market 与精确撤单；Grid schema v1 明确配置、状态、生命周期确认和四类语义订单键；不复用旧 delivery/Actor 协议，也不授予物理交易权限 |
| Control 与统一 Executor | `apps/venue-control/src/{lib,accounts/{kol,terminal,grid},private_projection,kol_executor,grid_store,grid_runtime,grid_hot_dispatch,executor_config,executor_store,executor_secret,executor_exchange{,/grid_batch},executor_runtime,bin/venue-executor-binance,http/accounts}.rs` | `0017`–`0024` 提供容量、归属、Grid 配置/目标面/归属/成交分配、命令账本、用户作用域私有投影、签名回读退避、Grid 热批次及跨微批前驱链。Executor 以私流唤醒并持久化账户投影；终端、Copy 与 Grid 共用账户串行队列和稳定 clientOrderId。热批次用一次性进程内令牌精确匹配数据库摘要、账户、symbol、私有代与规则代，命中时不做同步完整 REST 预检；缺失、过期、重启或不匹配只走签名冷路径。所有 Place 并发越过 send-entry 后才启动 Cancel，ACK/不确定结果随后逐单签名回读。新链只拒绝仍有旧 Binance LIVE writer scope 的账户。|
| Binance Grid 热批次迁移 | `apps/venue-control/migrations/{0023_binance_grid_hot_batch,0024_binance_grid_batch_chain}.sql`、`apps/venue-control/tests/binance_grid_hot_batch_migration.rs` | 0023 建立 0–16 条命令的原子收据与批内 Place-before-Cancel 顺序；0024 把每批绑定到所消费的 desired 摘要和唯一前驱。相邻私流成交可继续规划为独立后继批，但领取后批前必须确认前批全部 Reconciled；未确认 Place 不冒充签名订单，也不得被后批选作撤单目标。PostgreSQL 实库压力、真实 Canary 与预热 p95 尚未验收，不能宣称 10 ms 已达成。|
| Binance Grid 纯规划 | `crates/venue-strategies/src/hedged_grid/{planner,planner_tests}.rs` | 配置、实时规则/BBO、同代签名双腿仓位、本实例订单面、外部平仓预留与批量成交确定性地产生四类 Maker 目标、补库存、盈利减仓、Blocked/Reset/Stop；不持有 Actor、checkpoint、WAL 或交易所客户端 |
| Binance Grid 行情与规则证据 | `crates/venue-gateway-binance/src/grid_market.rs` | 每次读取新鲜 BBO，按有界周期刷新完整规则并在变化时提升 generation；盈利风险使用显式 quote→USD 证据，不假定 USDT/USDC 恒等于 USD |
| Copy 执行结果与耐久记账 | `apps/venue-control/src/{copy_execution_postgres,copy_ledger_postgres,copy_ledger_worker}.rs` | 结果以原 immutable job 校验目标/资产/phase/delta；跨零 Adjust 必须接在已签名归零的 Reduce 后。worker tick 从真实 Node receipt 与最终 Adjust 的 Reconciled 仓位原子生成 canonical receipt、ledger 和 drift；Rejected 只关闭原 delivery，不造 ledger。0013 允许经原 Node receipt 交叉核验的无 Copy consumer claim 终态；暂停/过期任务仍可记账但不生成新增风险授权，缺 Node receipt 暂不处理 |
| Copy 自动规划事实与冻结输入 | `apps/venue-node/src/control_loop/copy_planning.rs`、`apps/venue-control/src/{copy_planning_postgres,copy_planning_input,copy_planning_repair,copy_leader_postgres}.rs` | Node 用签名仓位/保证金、实时 Instrument 和显式 `copy_leader_capital` 产生外层有界事实；worker 配对当前关系的双边新鲜事实，与不可变 envelope/job/游标同事务提交。原始 leader 敞口与冻结倍率分别保留；未收敛旧任务不生成新风险任务。已记账漂移只能由新鲜事实生成独立修复任务，不能续期旧 child。逐所实际来源及产品端到端仍需验收 |
| VenueFlow 桌面运维客户端 | `apps/ui/desktop/src/main.rs` | `trading.rs`、`trade_dock.rs` 统一按钮、交易设置与非重复热键为同一 `TradingAction`；动作按“开多、平多、开空、平空”显示，默认 `A/S/D/F`，Maker-only 默认开启。`workspace.rs` 固定币安式图表/盘口/下单/底部账户区，交易对标签可关闭并新增。`execution_view.rs` 的仓位、当前委托、历史成交、仓位历史和资产只读新私有投影，历史委托只读新命令账本；`grid_view.rs` 与 `client/grid.rs` 管理新 Grid 配置和生命周期，并保留冻结旧策略只读区。Post Only 四动作、二次确认市价平仓与选中委托精确撤单进入唯一 Executor。|
| 响应式用户 Web | `apps/ui/web/components/control-console.tsx` | Next.js 16 + React 19 + TypeScript、同源 BFF/session、五视口响应式界面；总览/关系/账户/订单/持仓/成交/签名对账/ledger/drift/风险/语义控制复用 schema v2。`lib/projection-scope.ts` 过滤账户与关系、拒绝模糊回执归属，`lib/decimal.ts` 只作精确显示汇总，`lib/realtime.ts` 与 `app/api/events/route.ts` 管理连续事件、超时和恢复门；`e2e/` 仅为隔离 QA。构建与受控部署见 `docs/WEB.md`；页面已建立不等于服务器/实盘验收完成 |
| Web/CI 发布门禁 | `.github/workflows/workspace-gates.yml`、`apps/ui/web/scripts/verify-boundary.mjs` | CI 使用六 Node 发布契约，另运行 Web typecheck/unit/build/产物负向扫描和五视口 Playwright。BFF 在运行时同时限制路径与 HTTP method，角色只接受显式自有键；扫描只报告文件和规则名，不回显可疑 secret 内容 |
| KOL 离线容量与发布演练 | `apps/venue-control/tests/{kol_executor_capacity,kol_mvp_postgres_integration}.rs`、`scripts/Invoke-KolCanaryDrill.ps1`、`docs/KOL_EXECUTOR_RELEASE.md` | 确定性 5 KOL / 200 follower 中心调度 fixture 与 PostgreSQL+mock 端到端场景覆盖有界队列、源成交去重、重启回读、超时栅栏及拒单；脚本必须显式 `-OfflineFixture`，拒绝进程内 Binance 凭证并只调用受控 Rust 构建入口。它不是 2核4G 实机容量证明或真实 Canary |
| 账户 Execution Lane 调度 | `crates/venue-runtime/src/account_lane.rs` | 纯调度负责 Owner、优先级、单 in-flight 和 Unknown fence，不持有 writer/WAL/client。六个 Node binary 由 Account Runtime 组合它与 `AccountMutationHost`；各策略私流驱动、多 symbol 常驻及逐所生产接管仍须分别验收，不能把本地入口统一等同于实盘准入 |
| 旧 Grid 工件兼容读取 | `src/runtime/grid/`、`src/runtime/legacy/` | 根 package 已移除 `hedged-grid-{binance,gate,bitget}` binary、feature、部署 re-export 和发布脚本。仅保留已有工件的离线解析/验证代码；不得从根 package 启动 Stage 7 mutation |
| 配置、交易所选择、账户身份、网格层数 | `src/config.rs` | `venue.toml`、`venue.grid.toml`、`venue.gate.example.toml`、`venue.bitget.example.toml`；`trading_account_id` 是系统稳定内部 ID，不要求交易所提供 UUID，同一真实账户跨 symbol 复用；`account_binding` 只表示交易所产品/模式能力 |
| 凭证环境读取 | `src/credential_env.rs` | 根 `.env` 仅作本地输入，禁止读取到文档/日志 |
| 规范交易账户、Instrument、交易对、金额、订单、仓位、成交 | `crates/venue-domain/src/domain/mod.rs` | `identity.rs`、`instrument.rs`、`market.rs::PublicBar` 与 `risk_value.rs` 提供规范领域类型；`order_outcome.rs` 规定 adapter 必须先验签，且仅以新代、完整订单族页面收集到终端 cursor 的证据证明 `ProvenAbsent`；point 404/部分空页保持 `Unresolved`，不能收敛原 UNKNOWN fence；仅 `SignedOrderReadback` 与 `AuthoritativeOrderOutcome` 不可反序列化 |
| 错误汇总 | `src/error.rs` | 各领域本地错误枚举 |
| 未领取即过期的 Copy 恢复 | `apps/venue-control/src/copy_planning_expiry.rs` | `0016_copy_expired_unclaimed.sql` 仅扩展双 delivery 数据库状态；锁定并证明从未 claim/执行/记账后，依据更新双边事实原子生成独立 job，保留原 job。`copy_execution_postgres.rs` 拒绝退休后的晚到执行证据；`tests/copy_postgres/unclaimed_expiry.rs` 覆盖重跑迁移、恢复、幂等与负例 |
| Scalping 真实引擎输入与恢复 | `apps/venue-node/src/production_resident/scalping.rs`、`runtime_config.rs` | 显式 `scalping.parameter_release_id/owner_scope/risk_budget` 绑定纯引擎及 FeatureSource；frame 驱动 evaluate 和既有 Actor checkpoint，原手造 candidate 到 Host 入口已移除。签名安全/保护未接好时禁止自动入场，Control 的 Running 投影降为 NeedsAttention；Bitget/Hyperliquid 闭合 bars、持续行情实测、成交确认及退出保护仍待闭合 |
| 公共成交身份、批量调度与就绪证据 | `crates/venue-domain/src/domain/market.rs`、`apps/venue-node/src/production_resident/scalping/trade_window.rs` | `PublicTradeId` 数值/opaque 身份与显式 NativeAggregateId/Unsequenced/Session cursor 分离；原生 ID 不伪造连续，Node 同代就绪盘口后有界去重。`control_loop/public_stream/pending.rs` 最多 1024 事实逐条轮转；`control_loop/pump.rs` 把 Control 退避与行情 cadence 分开，同步 HTTP/签名读取延迟仍另行评估。指标 `public_market_source.rs/scalping_features.rs` 区分 native/session-observed Ready；Bybit/OKX/Gate 仅接协议确认闭合 K 线，Bitget/Hyperliquid forming 不提升为闭合事实 |

## 对冲网格与冻结兼容定位

新 Binance Grid 的活动入口是 `venue-control` 的 Grid Store/Runtime、共享 Executor、Binance Grid Market Reader 与纯 Planner；它根据签名事实重布网络，不采用旧订单。冻结旧 Grid 的最后提交组合仍在 `apps/venue-node/src/production_resident/grid.rs`，签名成交补账与多成交收敛在同目录 `grid_recovery.rs`。下列 Stage 7 路径只作旧工件恢复与行为核对参考；删除门统一见 [`GRID_RUNTIME_REFACTOR.md`](GRID_RUNTIME_REFACTOR.md#81-旧迁移代码删除门)。

| 功能 | 首要入口 | 边界 |
|---|---|---|
| 新 Grid 配置、持久目标与收敛 | `crates/venue-control-protocol/src/grid.rs`、`apps/venue-control/src/grid_store.rs`、`apps/venue-control/src/grid_store/{surface,reads,types}.rs`、`apps/venue-control/src/grid_runtime.rs`、`apps/venue-control/migrations/{0021_binance_grid,0023_binance_grid_hot_batch,0024_binance_grid_batch_chain}.sql` | `surface.rs` 原子提交 rolling anchor、完整 desired surface 与 plan CAS；其余边界保存版本化配置、订单归属、成交分配、生命周期栅栏和分支唯一批次尾，重启只依赖 PostgreSQL 与新鲜交易所事实。|
| Grid 参数、状态机、库存与 desired orders | `crates/venue-strategies/src/hedged_grid/{planner,model,reducer}.rs` | `planner.rs` 是新链无状态入口；旧 reducer/model 仍为共享类型和冻结行为参考；根 `src/strategy/hedged_grid` 仅重导出 |
| 高暴露浮盈减仓 | `crates/venue-strategies/src/hedged_grid/exposure_guard.rs` | 只输出语义结果；冻结旧 dispatch 位于 Node 账户链，当前 MVP 不调用 |
| 库存/暴露离线 verifier | `src/bin/verify-grid-{inventory-recovery,exposure-shadow}.rs` | 读取旧工件，Shadow 不构成 gateway mode |
| 冻结 Grid/Canary/成交热路径 | `src/runtime/grid/stage7_grid.rs`、`stage7_resident.rs`、`stage7_grid_canary.rs` | 保留行为与恢复证据；根旧三所 binary 已删除，不作为新 writer 入口 |
| 冻结停止/外部 Algo/保仓桥 | `src/runtime/grid/{stage7_executable_handoff,stage7_external_algo_cleanup,binance_legacy_stage7_bridge}.rs` | 旧 CLI 不在当前 Node；不复制独立 authority/journal 到新链 |
| 冻结公开/私有证据与安装 | `src/runtime/grid/{stage7_public_runtime,stage7_private_evidence_recovery,stage7_epoch_install}.rs` | 当前 checkpoint、Unknown 与未决 WAL 必须保留 |
| 冻结共享行为内核 | `src/runtime/hedged_grid/mod.rs`、`src/runtime/legacy/hedged_grid_live.rs` | 不再称为现行三所生产入口 |
| 历史配置 | `venue.grid.toml`、`venue.gate.example.toml`、`venue.bitget.example.toml` | 不是 Node runtime JSON；远端运行状态必须实测，不由文档路径推断 |
| 六所 Node 二进制验证 | `scripts/verify_venue_node_binaries.ps1` | 固定 feature 隔离；不等于实盘接管 |
| 本地 Ubuntu Node/Control 编译 | `scripts/Build-VenueUbuntu.ps1` | `G:\Build\Venue\ubuntu`，Cargo 复用 slot-2；默认 Nodes 为六 ELF，`-Component Control` 为 `venue-control-server` 与 `venue-executor-binance`，均绑定固定 commit 与 manifest/SHA256，`-CheckOnly` 零写；详见 [构建政策](DEVELOPMENT.md#build-policy) |
| 远程 Control 桌面启动 | `scripts/Start-VenueFlow.ps1` | 为默认访问 `127.0.0.1:39180` 的桌面端建立到服务器回环 Control 的 SSH 转发，再启动 `venueflow.exe`；不接触账户凭证 |

## 交易所 adapter

| 功能 | 首要入口 | 已提交边界/目标用途 |
|---|---|---|
| 网格所需窄 contract | `src/exchange/grid/mod.rs` | `src/exchange/grid/public_market.rs` 拆分三所公共订单簿桥，`adapter_tests.rs` 与 `event_time_tests.rs` 覆盖规则/协议时间；规范命令与私有 readback 类型仍由入口声明；`UmOrder`、`UmConditional`、`UmAlgo` 必须各自完整签名或绑定当前 execution profile 的显式不支持，正常订单投影须逐项等于 `UmOrder` 快照；Stage 7 没有条件/Algo WAL owner，已签名非空行拒绝常规族 writer；Binance mutation 只返回标量 `orderId` |
| Binance 公共/私有/PAPI | `src/exchange/binance/mod.rs` | `src/exchange/binance/{private,portfolio,risk_readback,binance_fill_pagination,signer,clock,public_stream,market_scan,order_parameters}.rs`；`order_parameters.rs` 封装 PAPI hedge-mode 的 GTC/GTX/IOC、market/reduce 与 conditional/algo stop 参数编码；网格 readback 保存 PAPI normal 与当前 Algo 的独立已签名页；已退役的 UM conditional 族显式不支持，字段不全一律拒绝；历史 `doctor --private` 不再是当前 Node CLI |
| Gate 旧协议兼容 | `src/exchange/gate/mod.rs` | 公共、账户/持仓与风险纯协议位于 `crates/venue-gateway-gate/src/{public,private,risk}.rs`；根路径保留历史协议、重导出和错误映射，不是新的生产启动入口。旧风险减仓回报只有严格 `t-ord-etp-{l|s}-<16 小写 hex>` 才可归因 hedge side，其他不透明 text 保持未知；旧规则缺最大下单量时仅供兼容读取，不能准入新链订单 |
| Bitget 旧协议兼容 | `src/exchange/bitget/mod.rs` | 公共、账户/持仓与风险纯协议位于 `crates/venue-gateway-bitget/src/{public,account,risk}.rs`；根路径保留历史协议、重导出和错误映射，不是新的生产启动入口。旧账户/设置/持仓/订单/成交五面任一失败即作废整轮，不跨尝试拼成一代；终态订单必须保留 `tradeSide` 与腿/方向一致性，成交时间优先 `execTime`；尚有风险减仓待结算的旧工件不推进成交历史窗口 |
| 三所风险原始证据离线重放 | `src/exchange/shared/risk_replay.rs` | 严格消费 Binance、Bitget、Gate 各自固定数量与顺序的原始 payload tuple，复用 adapter parser 输出规范 account/legs；缺失、冗余、乱序或篡改失败关闭 |
| 统一 WS/HTTP CONNECT | `src/exchange/shared/websocket.rs` | adapter 的连接调用点；DNS、全部解析地址、TCP、HTTP CONNECT 与 TLS/upgrade 共享一次 10 秒总期限，单个地址不得重置预算；超时工作线程不能阻塞账户 resident；握手后改用 1ms readiness poll，公共流再以帧数和 5ms 公平时间片让位私有成交；`src/backoff.rs` 为公共/私有连接与启动重试提供有上限、按账户/进程错峰的指数退避 |
| 私有 session 与 generation | `src/exchange/shared/private_session.rs` | `src/exchange/shared/private_session_state.rs` |

## 执行、安全与存储

| 功能 | 首要入口 | 已提交边界/目标用途 |
|---|---|---|
| 命令 WAL/journal | `crates/venue-execution/src/journal.rs` | 原 JSONL serde/hash/状态迁移与路径调用约定不变；append 持有排他文件锁并核对恢复时的耐久长度，旧进程、坏尾、空行、hash 或状态迁移分叉均失败关闭；Unix rename/创建同步父目录；`src/execution/journal.rs` 只兼容重导出，通用事实 journal 仍位于 `crates/venue-storage/src/journal.rs` |
| 限价执行政策 | `crates/venue-domain/src/domain/command.rs` | `LimitTimeInForce::{PostOnly, Gtc}` 为不可变命令字段；PostOnly 保留历史省略编码和 WAL hash，Gtc 显式编码。六 adapter 负责原生映射及即时/重启签名回读，`account_snapshot.rs`、`account_host.rs`、Runtime `reconciler.rs` 与 Node Grid/Copy 保留并精确比较政策；签名字段缺失保持未知。自动归一化仍只挂单，手动意图显式选择价格与政策；不支持的账户或策略能力继续拒绝 |
| 显式选价归一化 | `crates/venue-execution/src/account_normalization.rs` | `AccountPricedLimitIntent` 保留用户价格、政策与基础数量上限；Host 拒绝 adapter 改价、改 Owner、改方向或突破预算，归一化本身不写 WAL、不发送订单。六 adapter 只刷新规则并向下取量，不以 BBO 替换用户价格；已有自动 `AccountLimitNormalizationIntent` 继续只挂单。`SignedAccountOrderFact.created_at_ms` 只保存原生订单创建时间，旧快照缺字段保持未知，不用更新时间或本机时间伪造最近订单 |
| 手动交易 Node 桥 | `apps/venue-node/src/production_resident/manual.rs`、`control_loop.rs`、`runtime_config.rs` | `StrategyKind::Manual` 是不产生自动策略意图的终端 actor；Control、VenueFlow 和 Node 三层均要求 Trade 精确命中该 kind。显式限价与自有手动单撤单复用 Runtime/Host/Lane/WAL，稳定 request ID 与原始计划写入同一 Actor replay 的可选 manual 字段。Accepted 后从同一 WAL 回填 Runtime Owner 路由；私流分别核验 private generation 与 connection generation。`crates/venue-runtime/src/account/resident_manual_ack.rs` 在原 Runtime 私有边界内确认手动成交；`control_loop/manual_trade_e2e_tests.rs` 验证实际 Runtime/WAL 的离线交互。重投请求先查原 WAL，Reconcile 只查原命令和签名事实，不重发。Grid/Scalping/Copy 绑定仍拒绝；全 scope 撤单、自动策略协同与真实接管尚未完成 |
| 签名订单身份归一化 | `crates/venue-execution/src/account_order_identity.rs` | adapter 仅比较规范 client ID 与签名 wire ID 的精确编码，Host 再核对 WAL 状态、native ID、family 和完整订单语义才恢复 Owner。HL 使用真实 cloid 编码，Gate 在 adapter 内还原严格 `t-` 编码；Unknown 归属不等于收敛。签名订单 quantity 是原始委托量，filled_quantity 独立保存累计成交；账户风险仍按剩余未成交量计量 |
| 耐久 Owner/native identity | `crates/venue-runtime/src/account/host_route_hydration.rs`、`owner_route_install.rs` | Node 注册 Actor 时从同一本账户 WAL 恢复 Accepted 命令的 Owner/client/native/family 路由，已成交关闭单的 fills 仍能归属。`crates/venue-execution/src/owner_routes.rs` 保留独立契约测试；不能创建第二 writer/journal。恢复后的 Stop/Flatten 由统一 Runtime/Lane/Host 撤精确自有单并等待更新签名事实 |
| Net 减仓预留与恢复 | `crates/venue-execution/src/account_net_reduce.rs` | Accepted 仅在精确 native fills 合量、无开放原单且更新完整仓位后结算；Unknown 保留预留。结算事实与签名 bootstrap 共用原 checkpoint，5MiB 上限；文件和父目录持久化成功后才释放内存预留，Hedge 快照不经过 Net 专用拒绝门。专项在 `account_host_tests.rs` |
| 旧 lease/前驱兼容保护 | `crates/venue-execution/src/writer_lease.rs` | `canonical_root.rs` 提供 `(exchange, trading_account_id)` 机器级账户 fence，保留既有 schema-2、hash 与 `stage7_writer_roots/v2` 路径；恢复拒绝同 revision 的主备分叉，并按 scope/generation/handoff 不变量验证可选版本；根 `src/execution/writer_lease.rs`、`src/runtime/grid/stage7_writer_registry.rs` 只作兼容 facade；symbol/Owner 级 lease 之外，不同 symbol 不能选择不同 canonical root |
| 冻结执行门禁参考 | `src/execution/gate.rs` | `src/execution/engine.rs`、`src/risk.rs` |
| 冻结私有事实恢复参考 | `src/execution/reconcile.rs` | `src/execution/{private_projection,fill_recovery,recovery_writer}.rs`、`src/runtime/shared/private_facts_worker.rs`；私有 session 与 fill cursor 使用稳定 `trading_account_id`，不得使用产品类型代替账户身份 |
| 冻结外部 Algo 清理审计 | `src/execution/external_algo_cleanup.rs` | 独立 custody/permit/hash-chain WAL，只经 `recovery_writer` 的同一 writer 锁 dispatch；中断后先签名回读，仍在场才允许新一轮预写，已消失只结算、不重复撤单 |
| checkpoint 与权威事实 | `crates/venue-storage/src/lib.rs` | `journal.rs` 的单一 crate-private `DurableJsonl` 负责锁、完整行读取、经调用方 replay 验证后的 incomplete-tail 修复、append、文件与父目录同步；facts 与 `control_delivery.rs::OpaqueJournal` 分别验证自身 sequence/hash/格式并复用该 I/O 边界，保持既有 JSONL 字节/相对路径契约；根 `src/storage/*` 只保留 facade |
| 冻结 Canary/保护行为参考 | `src/execution/canary_sequence.rs` | `src/execution/{canary_evidence,canary_preflight,emergency_flatten,protection_custody}.rs` |

## Scalping、行情与自动选币

| 功能 | 首要入口 | 已提交边界/目标用途 |
|---|---|---|
| Scalping 策略 | `crates/venue-strategies/src/scalping/mod.rs` | 纯 model/candidate memory/risk、engine 和 checkpoint 位于 crate；根 `src/strategy/scalping/{mod,checkpoint}.rs` 仅保留兼容重导出 |
| Scalping Node 行情接线 | `apps/venue-node/src/production_resident/scalping.rs` | 公共簿经 MarketHub/FeatureSource 进入统一 resident；持续 evaluate、账户意图及退出保护闭环尚需验收，不能把行情 Ready 当作自动交易完成 |
| Scalping 冻结行为与恢复参考 | `src/runtime/scalping/scalping_resident_process.rs` | `scalping_resident/live_driver/live_gateway/live_exit` 及 `scalping_live_gateway_recovery.rs` 保留既有行为；根 facade 仍可定位兼容类型，但不进入当前生产目标，也不能重接旧 writer |
| 冻结控制/自动编排行为参考 | `src/runtime/scalping/scalping_control.rs` | `src/runtime/scalping/binance_auto_shadow.rs` |
| 公共行情、订单簿、记录与回放 | `src/market/mod.rs` | `src/market/{session,orderbook,recorder,replay}.rs`；`orderbook.rs` 是 `venue-indicators` 的兼容重导出 |
| 指标与 FeatureFrame | `crates/venue-indicators/src/feature_frame.rs` | `catalog/` 将 VenuePulse 72 项行为迁入只接受规范 `PublicBar`/`PublicTrade`/`PublicBook` 的共享核心，并加入 AVL/TRIX/SAR/SUPER 扩展；`chart/` 提供 22 项商用图表指标注册、forming 克隆预览与参数化引擎；VenueFlow 的 `chart_settings.rs`、`settings_panel.rs`、`chart_view.rs` 分别负责配置、实时重算和渲染；`public_book.rs`、`public_market_source.rs`、`scalping_features.rs`；`orderbook.rs` 提供根 OrderBook 的共享实现，根 `src/indicator/mod.rs` 只重导出 |
| Binance 候选扫描 | `src/market/scanner.rs` | `src/exchange/binance/market_scan.rs`、`src/runtime/scalping/binance_market_scan.rs` |

## 测试定位

Grid 穿价边界：`hedged_grid/planner.rs` 不用最新 BBO 推断已有签名挂单已失效；仅最终新增 Maker 目标穿价时返回 `MakerPriceWouldCrossBook`，运行时显示 `maker_price_wait` 并保留锚点与未分配成交，等待后续事实重试，不进入整网重置。`planner_tests.rs` 覆盖旧挂单与 BBO 错位、配对成交分批到达、穿价等待后继续补撤。

账户成交观察时间回归：`apps/venue-control/tests/support/projection_fill_observation.rs` 由 `kol_mvp_postgres_integration` 组合；覆盖 REST 拉取期间成交、签名重读修复与重复私流的稳定身份。`private_projection.rs` 保留订单/持仓的快照起始时间，成交使用接收后的持久化时间；历史倒置时间通过 `account_gateway_projection.rs::replay_projection_fills_from` 定向签名补查。`grid_runtime/fills.rs` 在任何成交尚未被后续签名基线覆盖时推迟整轮冷规划，不允许过滤后形成假缺单；不完整订单面或成交单仍在快照中的情况只等待私有事实收敛，不触发撤单重置。对应边界测试位于 `grid_runtime/support.rs`。

默认按影响面分层验证，不在每次局部修改后重复全工作区回归。UI 局部改动验证客户端及相应交互；单模块修改验证该模块及直接契约；KOL 新链交易安全修改覆盖命令幂等、账户串行、`ReconcileRequired`、数量/权限和 Binance adapter；冻结旧链维护才覆盖风险、WAL、Unknown 与恢复路径。
跨模块契约、依赖变化、架构合并或发布前集中建立全工作区通过基线。基线通过后的增量只重跑受影响专项；纯文档、注释或 lint 标注只做对应静态检查，不使既有业务测试结果失效。记录验证对应的提交/源码范围，构建缓存疑似串用时使用两个固定隔离槽并持锁核验，不新建目录、不清空共享缓存。

- VenueFlow 图表实时锚点：`apps/ui/desktop/src/chart.rs` 与 `chart_view.rs`；左键拖动保持固定 K 线宽度、右侧留白占用视窗槽位，新 K 线在拖定位置继续更新。内联测试覆盖连续拖动/松手不回弹、图表与时间轴、缩放及新 K 线到达；“跟随”/“适配”显式恢复右端位置。
- 新 Grid 规划/持久化/运行时测试：`crates/venue-strategies/src/hedged_grid/planner_tests.rs`、`apps/venue-control/src/grid_runtime.rs` 内联测试、`apps/venue-control/tests/{grid_store_postgres_integration,binance_grid_hot_batch_migration}.rs`；旧 reducer/风险状态回归仍位于 `crates/venue-strategies/src/hedged_grid/{reducer_tests,exposure_guard,recovery_tests}.rs`。
- 交易所 adapter 测试：`src/exchange/{binance,bitget,gate,grid}/` 内的测试文件及各交易所直接测试模块。
- 共享 resident/runtime 测试：`src/runtime/grid/stage7_grid_tests.rs` 统一组合 `stage7_grid_{core,recovery}_tests.rs`，其余专项位于 `stage7_grid_reconciliation_tests.rs`、`stage7_fill_sequence_tests.rs`、`stage7_install_recovery_tests.rs`、`stage7_inventory_recovery_evidence_tests.rs`、`stage7_exposure_composition_tests.rs`、`hedged_grid_runtime_equivalence_tests.rs`、`exposure_runtime_tests.rs` 及各 `stage7_*` 模块内测试。
- 账户运行时架构契约：`crates/venue-runtime/src/account/{tests,recovery_tests}.rs`、`account/tests/runtime_safety_tests.rs` 及 `account/{private_router,market_hub,reconciler}.rs`、`strategy/mod.rs`、`account_lane.rs` 内测试；运行时错误集中在 `account/runtime_error.rs`；覆盖多 symbol/family 隔离、durable inbox/applied cursor、Actor turn ack、Owner/Cancel 路由、邮箱与执行公平性、WAL 五状态/Unknown/恢复清单、配置 epoch、Pause/Stop 残仓 custody/Flatten、三订单族能力与签名订单全语义、Net/Hedge 完整持仓腿；通用 command journal、writer lease 与账户 canonical root 测试位于 `crates/venue-execution/src/`。
- 六所候选准入审计：`scripts/verify_gateway_candidate_contract.ps1` 构建/测试六个隔离 LIVE binary，验证非生产模式前置拒绝、非生产 endpoint/header 标记缺失及缺证据时零工件失败关闭；矩阵中的 `not_reached` 和 `writer_enabled=false` 是尚未接线的真实结论，不构成实盘准入。
- Binance 旧网格测试：`src/runtime/legacy/hedged_grid_live_tests.rs`、`hedged_grid_hot_path.rs` 内测试；共享行为测试位于 `src/runtime/hedged_grid/` 与 `src/runtime/grid/`。
- Node CLI、配置：`apps/venue-node/src/lib.rs`、`apps/venue-node/src/runtime_config.rs` 和 `src/config.rs` 内测试。
- Copy 领取窗口：`apps/venue-node/src/control_delivery_claim_window_tests.rs` 与 `apps/venue-control/tests/copy_postgres/claim_window.rs` 验证临期 Install 精确截断、不续期原 job、并发抢领及过期已领取任务仅对账。`apps/venue-node/src/control_loop/copy_reconciliation.rs` 回传 Copy ReconcileOnly 的最终签名结果，`copy_delivery_journal.rs` 持久化只读对账后的跨零续行禁用标记；`control_loop/control_loop_tests.rs` 覆盖 Unknown 原 WAL 重启收敛、不重复 dispatch、原 delivery 精确回传及首次任务前的规划事实发布。
- KOL MVP 数据契约：`apps/venue-control/tests/kol_mvp_postgres_integration.rs` 重跑 0017 并验证 5/200 容量槽、邀请码唯一、注册归属不可更新/删除、跨用户引用和命令状态/幂等约束；真实 PostgreSQL 门由 `scripts/verify_postgres_integration.ps1` 执行。
- 执行、恢复、writer：`tests/*recovery*`、`tests/*writer*`、`tests/*canary*`。
- 行情与存储：`tests/market.rs`、`tests/storage.rs`。
- Scalping：`crates/venue-strategies/src/scalping/engine_tests.rs`、`src/runtime/scalping/*tests.rs`、`tests/scalping_*` 与 `tests/legacy_scalping_*`。
