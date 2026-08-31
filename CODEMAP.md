# VENUE 功能入口

更新：2026-08-31

本文件只回答“当前功能代码在哪里”。合并跟单、六交易所、指标、桌面/Web UI 后的目标 workspace、依赖边界和技术栈查
[`ARCHITECTURE.md`](ARCHITECTURE.md)；多策略账户运行时、网格、成交热路径、库存恢复、验收和接管统一查
[`GRID_RUNTIME_REFACTOR.md`](GRID_RUNTIME_REFACTOR.md)；统一执行链、Stage 7 退休、持续实盘授权、多子任务和 Web 迁移查
[`UNIFIED_GATEWAY_WEB_MIGRATION.md`](UNIFIED_GATEWAY_WEB_MIGRATION.md)；当前获准的待实现 Goal 查
[`REFACTOR_IMPLEMENTATION_GOALS.md`](REFACTOR_IMPLEMENTATION_GOALS.md)。不要从 `bak/` 或历史提交寻找当前约束。

## 进程、配置与通用领域

合并后 Git 源码总预算为 12 MiB，单文件仍为 2 MiB；`content-releases/`、`handoff-staging/` 与既有运行工件均排除跟踪。
依赖门禁按当前用途允许 Control 的 `argon2/ring` 密码与加密、VenueFlow 的 `time` 本地时区；`zeroize` 遵循凭证清理基线。
Node 投影摘要辅助位于 `apps/venue-node/src/control_loop/projection_digest.rs`；手动交易协议位于 `crates/venue-control-protocol/src/trade.rs`。
Web 的 Control 用户会话只在 BFF 环境注入，HTTP/SSE 同时保留 Control 账户归属校验，不复用 Node token；部署配置见 `apps/venue-web/README.md`。

| 功能 | 首要入口 | 直接继续 |
|---|---|---|
| 当前统一迁移 Goal | `UNIFIED_GATEWAY_WEB_MIGRATION.md` | T0–T8 子任务把 Stage 7 直接重构到六所统一 Account Runtime/Execution Lane，闭合 Copy 物理执行并建立响应式 `apps/venue-web`；真实 mutation 全局串行，AI 持续授权和 10U 技术门见第 2.1 节 |
| 构建、依赖与仓库体积门禁 | `Cargo.toml` | workspace 当前包含根 package、`venue-copy`、`venue-control-protocol`、`venue-domain`、`venue-execution`、`venue-indicators`、`venue-runtime`、`venue-storage`、`venue-strategies`、`venue-gateway-api`、六个 `venue-gateway-*` adapter、`apps/venue-node`、`apps/venue-control` 与 `apps/venueflow`，resolver 固定为 3；workspace 与 `rust-toolchain.toml` 共同锁定 Rust 1.98.0；`.cargo/config.toml` 固定主缓存为 `G:\Build\Venue\main`；`Cargo.lock`；`scripts/verify_repository_hygiene.ps1` 执行体积和运行态文件门禁 |
| 本机构建资源约束 | `scripts/BUILD_POLICY.md` | `Invoke-VenueBuild.ps1` 与 `venue_build_guard.ps1` 管理 main/slot-1/slot-2、最多2个受控构建、150 GiB缓存准入及F/G空闲检查；main 临时关闭外层 wrapper 以保留增量，退出恢复原环境，隔离槽保留 wrapper；专项脚本持锁到验证完成，不自动删除缓存 |
| 目标账户实盘安全与工件预算 | `crates/venue-execution/src/account_host.rs`、`crates/venue-runtime/src/account_lane.rs`、`GRID_RUNTIME_REFACTOR.md` 第 4.4、7、8、11 节 | 账户级进程锁、单一分段命令 WAL、WAL 内 Owner 和 Unknown 签名对账；host 在同一 WAL 持久化 `Submitted` 后才签发一次性 dispatch permit。账户汇总门包含签名仓位、未撤入场单、未决 WAL 风险保留与候选命令；跨报价资产必须由 `account_snapshot.rs` 的新鲜换算事实估值到 USDT，缺证据禁止增险。适配器接线和实盘验收仍须逐所证明。工件根固定 `G:\Venue\artifacts`，轮转、单文件和根预算分别为 5 MiB、10 MiB、256 MiB |
| 规范签名账户快照与成交恢复 | `crates/venue-execution/src/account_snapshot.rs`、`account_cursor_tests.rs` | 余额保留原资产、未知可用金额不填零；订单状态与已成交量只保留来源明确的值。`AccountRecoveryRequest::previous_fills_cursor` 从当前 checkpoint 恢复，整轮签名快照成功才原子推进游标。HL 的账户级协议在 `crates/venue-gateway-hyperliquid/src/protocol/account.rs`，覆盖非所选币种的仓位/订单/成交、Net 正负数量与保留窗口缺口；单交易对旧 parser 不可冒充账户完整性 |
| 六所网关身份、模式与能力门禁 | `crates/venue-gateway-api/src/lib.rs` | 规范 venue 固定 Binance、Bitget、Bybit、Gate.io、Hyperliquid、OKX；运行模式只接受精确 `LIVE`，`PublicMarketBinding` 另提供无账户、无凭证、无 mutation 的 Binance USD-M 公共行情 scope；旧 `capability_promotion.rs` 的普通 `promote/authorize` 入口继续固定 `AuthorityUnavailable`，序列化 probe 不能升级能力；Bybit、OKX、Hyperliquid MVP 不复用该旧 authority 链，只消费 `AccountMutationHost` 在 WAL `Submitted` 后签发的一次性不可构造 permit |
| Binance Portfolio Margin adapter | `crates/venue-gateway-binance/src/account_gateway.rs` | `credential_probe.rs` 提供固定签名 GET 的账户/API 只读验证，不创建 writer；账户 adapter 负责完整签名事实、规则及 policy-exact place/cancel/reduce 回读，经固定 Node 的统一 Host/WAL/Lane 执行。`transport.rs`、`private_ws.rs`、`readback.rs`、`execution.rs` 保留协议实现；`recovery.rs` 的旧 collector 仅为迁移兼容，不授予新链 capability/WAL/writer；服务器旧 release 接管须另行验收 |
| Bitget adapter | `crates/venue-gateway-bitget/src/account_gateway.rs` | UTA v3 账户级未成交订单分页读取全部 delegateType；风险汇总只接纳可精确归一化的 normal 行，任何尚未支持的条件/策略单均拒绝准入，不能因 mutation profile 不支持就当作账户不存在该订单。`private.rs`、`order_families.rs` 解析官方 orderStatus/reduceOnly 与旧 fixture 兼容形状；`transport.rs`、`private_ws.rs`、`execution.rs` 保留已验证签名/私流/精确回读，`recovery.rs` 与 `runtime_recovery.rs` 的旧证据链仅供迁移兼容 |
| Gate.io adapter | `crates/venue-gateway-gate/src/account_gateway.rs` | 账户 adapter 负责规则、Hedge 双腿、完整订单族、fills cursor、policy-exact place/cancel/reduce 回读，经固定 Node 的统一 Host/WAL/Lane 执行；`account_gateway_priced.rs` 保留用户价格和数量上限，专项位于 `account_gateway_tests.rs`。`risk.rs` 区分存量数量转换与真实 `order_size_max` 新单检查，旧风险回放不伪造最大下单量。`transport.rs`、`private_surface.rs`、`execution.rs` 为协议实现；旧 `recovery_session.rs` 与 `recovery.rs` 不授予新链 writer |
| Bybit V5 adapter | `crates/venue-gateway-bybit/src/account_gateway.rs`、`account_gateway_tests.rs` | 固定 UTA2/双向持仓预检、实时 linear 规则、policy-exact PostOnly/Gtc limit place/readback、精确 cancel 和签名仓位约束的 reduce-only；MarketReduce 只减已有腿，不准入市价增险或止损单。原始 mutation transport 为 crate-private，只消费统一 Host permit；账户专项在同 crate 测试文件，生产接管单独验收 |
| OKX V5 adapter | `crates/venue-gateway-okx/src/account_gateway.rs` | 固定 Long/Short + Cross、实时 SWAP 规则、policy-exact PostOnly/Gtc limit place、精确 cancel 与 `clOrdId` signed readback；`public.rs` 按 `ctVal × ctMult × contracts` 在 base 数量与张数间换算并向下取整，不突破 10U；ACK 正常解析失败但订单行 `sCode=0` 时必须持久化 `Unknown` 并签名回读，禁止误记 `Rejected` 后继续增险；原始 POST 为 crate-private，生产接管单独验收 |
| Hyperliquid adapter | `crates/venue-gateway-hyperliquid/src/account_gateway.rs` | 固定主账户/API Wallet 绑定、meta/clearinghouse/open-orders 预检、ALO/GTC limit place、精确 cancel 与 cloid readback；nonce 在签名前原子持久化且单文件上限 4 KiB，`/exchange` 为 crate-private 并只消费 host permit；生产接管单独验收 |
| 根 package 离线验证 | `src/bin/verify-grid-{inventory-recovery,exposure-shadow}.rs` | 根 package 不再有通用 CLI 或生产 mutation binary；两个 verifier 只读取既有工件 |
| 六所固定账户节点产物 | `apps/venue-node/src/lib.rs` | 六 binary 逐 adapter 隔离，是唯一生产启动入口；`resident.rs` 仅编排 Grid/Scalping/Copy 的规范事实与语义 intent。物理 mutation 必须经常驻 Account Runtime、Execution Lane 与 `AccountRuntimeHost`；签名 bootstrap 或 Actor/WAL 准入缺失时失败关闭，绝无旧 Grid/Canary writer 旁路。Binance 读取其专属公共流；Gate 由 adapter-owned WS 先订阅、再取 REST snapshot，经连续 delta 到 Ready；Bitget 由 adapter-owned `books` WS snapshot 与首个覆盖 update 接桥；两者都只进入 resident 的 sequenced ingress。Bybit、OKX、Hyperliquid 的无凭证 receiver 位于各 adapter `public_ws.rs`，固定 Node 接线集中于 `control_loop/public_stream.rs`；OKX 按真实 prevSeqId 接桥，Bybit 重建完整簿、Hyperliquid 消费完整 L2 快照并走显式完整图像入口，不伪造 Delta。通用 signed-only Run 仍拒绝 Scalping，一次性 BBO 不进入策略输入；完整策略驱动与实盘接管另行验收 |
| Control→Node delivery 与 Node→Control 投影 | `apps/venue-node/src/{control_delivery,projection_outbox,control_loop}.rs`、`apps/venue-control/src/node_projection_postgres.rs` | delivery 的 claim、ACK、receipt 每次 await 后重验 lease/session/epoch。projection 按账户/node/instance 的独立游标写入耐久 outbox，精确回显才确认；PG 在同一事务合并本实例投影、保留其他实例/账户并更新 scopes/events，0012 迁移从旧 envelope 恢复实例游标。外层有界 Copy execution evidence 复用原始结果，`copy_execution_postgres.rs` 校验原 delivery 并与游标原子记录，不进入浏览器 facts；两条路径均不授予交易权限。停止/清仓端到端测试位于 `control_loop/control_loop_tests.rs` |
| Actor Applied 持久化证据 | `crates/venue-storage/src/actor_applied.rs` | `create_new` 与 `open_existing(anchor)` 耦合 hash-chain journal 和原子 checkpoint；anchor 精确绑定 root/tail/count/checkpoint，缺件、截断、scope/generation/turn/replay/WAL 回退失败关闭。Node 经 `venue-runtime::account::AccountRuntimeHost` 校验 Actor 所引用 head 确属同一 WAL 的真实历史前缀，允许 Actor Applied 后正常追加命令再重启，但拒绝伪前缀；`account::copy_actor::recover_copy_actor_applied` 仅恢复原语义回执，不签发当前执行 turn。receipt 不是身份、WAL 或 mutation authority |
| 多策略账户运行时内核 | `crates/venue-runtime/src/account/mod.rs` | `registry.rs`、`private_router.rs`、`market_hub.rs`、`reconciler.rs` 与 `recovery.rs` 固定账户恢复/路由边界，`runtime_error.rs` 集中运行时失败语义；旧 `physical_recovery*` 多层证据链保持 Stage 7 迁移兼容且不再扩展。Bybit、OKX、Hyperliquid 的 MVP 生产 mutation 直接使用 `venue-execution::AccountMutationHost` 的账户锁、分段 WAL、Unknown 对账与一次性 permit，不从旧 Runtime fixture 或 capability 链取得 authority |
| 策略 Actor 宿主与邮箱 | `crates/venue-runtime/src/strategy/mod.rs` | 私有事实与 Delta/Trade/Bar 有界无损；仅 Snapshot/Ticker/MarkFunding 合并；私有 burst 64 后让行对账/控制；一个实例一个 runtime-issued turn，durable applied receipt 后才可 Running/输出授权意图；`src/runtime/strategy/mod.rs` 只兼容重导出 |
| 共享私有事实调度 | `crates/venue-runtime/src/shared/private_facts.rs` | effect 调度、session generation/ticket、周期刷新、退避与 readiness/snapshot；单 in-flight 且证据尾绑定，跨 generation 结果拒绝；根 worker 仅保留兼容 facade 与 Binance 协议/REST/WS 组合，不持有 writer/WAL/mutation |
| 跟单纯规划与 Node 物理桥 | `crates/venue-copy/src/lib.rs`、`apps/venue-node/src/production_resident/copy.rs` | Copy crate 只提供资本、目标、身份、delivery、ledger/drift 纯语义，不持有 writer/client。跨零目标由资本规划保留、Node 分 ReduceToZero/Adjust 两轮处理；在 Actor/WAL 前耐久保存原始 request，经统一 Runtime/Lane/Host 准入。`copy_semantic/{signed_position,reconciliation}.rs` 校验完整仓位腿、精确 WAL 原命令和累计规范成交，ACK 不作 Reconciled；结果另外保留更新签名仓位供 ledger 匹配。`copy_delivery_journal.rs` 保存跨重启的 job/phase；leader 自动接入和 ledger/drift 产品闭环仍需对应端到端验收 |
| 版本化 Control 协议 | `crates/venue-control-protocol/src/lib.rs` | schema v2 DTO 固定 snapshot、event、command、receipt 与账户节点 delivery/claim/ACK/Unknown/Reconciled 的精确 LIVE binding、instance/config epoch 校验；`CopyRelationConfig` 另固定 relation UUID/revision、Leader/Follower 精确账户/实例/交易对绑定、资本、倍率、准备金、风险和 lifecycle；`trade.rs` 保留固定 LIMIT/GTC、显式 reduce-only 的 `TradeIntent`，未接线 Node 必须明确拒绝；协议不授予 capability、writer、WAL 或交易权限 |
| Control 服务核心 | `apps/venue-control/src/lib.rs` | Control 命令、本地 HTTP/SSE `/v2`、PostgreSQL fencing delivery lease 及 `account_node_poll.rs` 的 bounded claim/ACK/receipt HTTP 路由已落地；repository 始终重验 LIVE scope/lease/sequence，重复 receipt 幂等且冲突关闭；`/v2/copy/relations` 用 PostgreSQL revision、唯一 follower binding 和 JSON/索引列一致性保存配置；`0014_manual_trade_intent.sql` 与 `0015_accounts.sql` 分别提供手动语义命令及窄账户管理，`accounts/` 和 `http/accounts.rs` 负责密码/会话、API 密文、只读验证与归属校验，见 `ACCOUNT_MANAGEMENT.md`；`venue-copy-worker` 只生成语义 job，mutation authority 恒为 false |
| Copy 执行结果与耐久记账 | `apps/venue-control/src/{copy_execution_postgres,copy_ledger_postgres,copy_ledger_worker}.rs` | 结果以原 immutable job 校验目标/资产/phase/delta；跨零 Adjust 必须接在已签名归零的 Reduce 后。worker tick 从真实 Node receipt 与最终 Adjust 的 Reconciled 仓位原子生成 canonical receipt、ledger 和 drift；Rejected 只关闭原 delivery，不造 ledger。0013 允许经原 Node receipt 交叉核验的无 Copy consumer claim 终态；暂停/过期任务仍可记账但不生成新增风险授权，缺 Node receipt 暂不处理 |
| Copy 自动规划事实与冻结输入 | `apps/venue-node/src/control_loop/copy_planning.rs`、`apps/venue-control/src/{copy_planning_postgres,copy_planning_input,copy_planning_repair,copy_leader_postgres}.rs` | Node 用签名仓位/保证金、实时 Instrument 和显式 `copy_leader_capital` 产生外层有界事实；worker 配对当前关系的双边新鲜事实，与不可变 envelope/job/游标同事务提交。原始 leader 敞口与冻结倍率分别保留；未收敛旧任务不生成新风险任务。已记账漂移只能由新鲜事实生成独立修复任务，不能续期旧 child。逐所实际来源及产品端到端仍需验收 |
| VenueFlow 内部运维客户端 | `apps/venueflow/src/main.rs` | `trading.rs`、`trade_dock.rs` 统一按钮、交易设置与非重复热键为同一 `TradingAction`，设置面板按订单参数/数量预设/快捷键双栏呈现，保存 5 档 quote preset/真实映射与选价有效期（默认 10 秒，1–300 秒）；交易面板移除清除/回到市场按钮及对应快捷键入口；图表或 DOM 选价到期自动清空，构造意图前再次检查有效期，修改期限立即清空当前价格，不影响已提交的 GTC 订单；有效选价经 Control 提交精确 LIVE `TradeIntent`；交易对切换统一通过 `model.rs::select_symbol` 立即清除选价、订单选择与待确认操作；无可用交易账户时仍按显示交易对清理，取价及提交动作前再次同步作用域；切换账户清除选价，文字输入与非 Trading workspace 屏蔽交易热键。顶部全局作用域栏提供与账户登录无关的行情服务器选择和执行账户入口；`account_center.rs` 与 `account_client.rs` 提供注册/登录、临时凭证输入、绑定/验证/删除及会话级执行账户选择；API 验证与 Node 投影分别展示，详见 `ACCOUNT_MANAGEMENT.md`；`copy_relation_view.rs` 编辑跟单关系；native 用 `Last-Event-ID`、Web 用 `after=` 恢复带 scope 的失效通知 SSE，随后重新查询授权快照；原生端以 `market_client.rs` 提供 Binance LIVE 公共行情、`symbol_picker.rs` 提供整行可点击搜索器，`ui.rs`、`chart.rs` 与 `chart_view.rs` 绘制图表并由 `venue-indicators::chart` 计算指标；Web 保持 Control-only；两端均无账户私流、物理交易客户端、WAL、writer 或 artifacts 写权限，表单凭证不持久化 |
| 响应式用户 Web | `apps/venue-web/components/control-console.tsx` | Next.js 16 + React 19 + TypeScript、同源 BFF/session、五视口响应式界面；总览/关系/账户/订单/持仓/成交/签名对账/ledger/drift/风险/语义控制复用 schema v2。`lib/projection-scope.ts` 过滤账户与关系、拒绝模糊回执归属，`lib/decimal.ts` 只作精确显示汇总，`lib/realtime.ts` 与 `app/api/events/route.ts` 管理连续事件、超时和恢复门；`e2e/` 仅为隔离 QA。构建与受控部署见 `apps/venue-web/README.md`；页面已建立不等于服务器/实盘验收完成 |
| Web/CI 发布门禁 | `.github/workflows/workspace-gates.yml`、`apps/venue-web/scripts/verify-boundary.mjs` | CI 使用六 Node 发布契约，另运行 Web typecheck/unit/build/产物负向扫描和五视口 Playwright。BFF 在运行时同时限制路径与 HTTP method，角色只接受显式自有键；扫描只报告文件和规则名，不回显可疑 secret 内容 |
| 账户 Execution Lane 调度 | `crates/venue-runtime/src/account_lane.rs` | 纯调度负责 Owner、优先级、单 in-flight 和 Unknown fence，不持有 writer/WAL/client。六个 Node binary 由 Account Runtime 组合它与 `AccountMutationHost`；各策略私流驱动、多 symbol 常驻及逐所生产接管仍须分别验收，不能把本地入口统一等同于实盘准入 |
| 旧 Grid 工件兼容读取 | `src/runtime/grid/`、`src/runtime/legacy/` | 根 package 已移除 `hedged-grid-{binance,gate,bitget}` binary、feature、部署 re-export 和发布脚本。仅保留已有工件的离线解析/验证代码；不得从根 package 启动 Stage 7 mutation |
| 配置、交易所选择、账户身份、网格层数 | `src/config.rs` | `venue.toml`、`venue.grid.toml`、`venue.gate.example.toml`、`venue.bitget.example.toml`；`trading_account_id` 是系统稳定内部 ID，不要求交易所提供 UUID，同一真实账户跨 symbol 复用；`account_binding` 只表示交易所产品/模式能力 |
| 凭证环境读取 | `src/credential_env.rs` | 根 `.env` 仅作本地输入，禁止读取到文档/日志 |
| 规范交易账户、Instrument、交易对、金额、订单、仓位、成交 | `crates/venue-domain/src/domain/mod.rs` | `identity.rs`、`instrument.rs`、`market.rs::PublicBar` 与 `risk_value.rs` 提供规范领域类型；`order_outcome.rs` 规定 adapter 必须先验签，且仅以新代、完整订单族页面收集到终端 cursor 的证据证明 `ProvenAbsent`；point 404/部分空页保持 `Unresolved`，不能收敛原 UNKNOWN fence；仅 `SignedOrderReadback` 与 `AuthoritativeOrderOutcome` 不可反序列化 |
| 错误汇总 | `src/error.rs` | 各领域本地错误枚举 |

## 对冲网格与旧工件兼容定位

本节的 `stage7_*`、legacy、handoff 和旧 root 条目用于定位既有行为及恢复兼容代码，不是当前根 package 可启动的生产 CLI。
服务器既有旧 release 必须独立核验，不能从本地入口删除推断其已停止。目标调用、逐所退出和删除门见
`UNIFIED_GATEWAY_WEB_MIGRATION.md` 第 5、7、9 节。

| 功能 | 首要入口 | 直接继续 |
|---|---|---|
| 网格参数、epoch、订单意图模型 | `crates/venue-strategies/src/hedged_grid/model.rs` | 根 `src/strategy/hedged_grid/mod.rs` 仅兼容重导出 |
| 网格纯状态机、库存、desired ladder、maker fill/滚动 | `crates/venue-strategies/src/hedged_grid/reducer.rs` | `model.rs` 持久化 fill anchor 与 passive-book fallback 穿价证明；只有 maker 成交驱动，taker 只更新库存与对账 |
| 高暴露浮盈市价减仓 | `crates/venue-strategies/src/hedged_grid/exposure_guard.rs` | `crates/venue-domain/src/domain/risk_snapshot.rs`、`src/runtime/hedged_grid/{risk_snapshot,shadow_evidence}.rs`、`src/runtime/grid/stage7_exposure.rs`；策略 crate 只产出语义结果，持久化和 mutation 仍由根 runtime 承担 |
| 风险 Shadow 只读验收 | `src/runtime/grid/stage7_exposure_shadow_verifier.rs` | `src/bin/verify-grid-exposure-shadow.rs`；要求 risk-bound admission，逐条交叉核对同 root 原始引用，再按三所固定 raw tuple 语义重放并精确比较 account/目标 leg；全程无凭证、网络和写入，且不等于风险 Live 准入 |
| Binance Stage 7 冻结行为参考 | `src/runtime/legacy/hedged_grid_live.rs` | checkpoint/stop 兼容在 `src/runtime/legacy/{hedged_grid_hot_path,hedged_grid_recovery,hedged_grid_support}.rs`；`run_binance_stage7_grid` 是历史组合函数，根生产 binary 已移除。服务器旧 release 的实际退休仍须独立完成 admission/handoff 与逐所接管，不能由源码入口删除推断 |
| 三家 Stage 7 冻结 runtime | `src/runtime/grid/stage7_grid.rs` | `stage7_resident/fill_drive/exposure/risk_lane/readback/mutation/retry` 保存当前三所网格热路径和行为证据；完整 maker fill、post-only、完整订单族签名回读、Unknown 与重建语义必须提取为统一 runtime 契约，禁止在该入口新增第二套功能 |
| Stage 7 冻结 Canary/准入 | `src/runtime/grid/stage7_grid_canary.rs` | `stage7_canary_*` 继续保护迁移期实盘并绑定现有配置摘要；新链完成同等门禁和逐所接管前不得删除 |
| Stage 7 停止接管、清仓、健康、writer root | `src/runtime/grid/stage7_executable_handoff.rs` | schema-1 同 root 升级及 schema-2 跨主机/root 保仓迁移；`crates/venue-execution/src/canonical_root.rs` 以 `(exchange, trading_account_id)` 而非 symbol/Owner 建立机器级 canonical root 与进程锁，`src/runtime/grid/stage7_writer_registry.rs` 保持原 Stage 7 facade，Stage7、旧网格、Scalping Live、Canary 和可写恢复共用；Stop 与到期 `BlockedUnknown` 共用全订单族签名回读的 WAL 收敛与残仓 custody；最终 handoff 的 lease/readback 不得回退或跨 session；immutable receipt 后精确 fence 旧 lease，只有 receipt 绑定的 executable 可激活下一 writer generation；显式 WAL 封存只允许在签名全族为空、零未决和零本地事务边界执行 |
| Binance 外部 Algo 精确清理 | `src/runtime/grid/stage7_external_algo_cleanup.rs` | CLI `grid-external-algo-cancel` 仅在完整签名 Algo 页证明“唯一一行且两个 operator 锚定 ID 精确匹配”、regular 页仍与 grid checkpoint/WAL 绑定且旧 WAL 零未决时开放；`src/execution/{external_algo_cleanup,recovery_writer}.rs` 复用 canonical root 与 `writer.json.lock`，先 fsync hash-chain `external_algo_cleanup.jsonl` 再发一次精确撤单，HTTP 回报不作终态，必须以新签名 Algo 空页结算；不把外部单伪造成 owned order，不撤 regular 网格单 |
| Binance 旧运行时保仓桥接 | `src/runtime/grid/binance_legacy_stage7_bridge.rs` | CLI `grid-legacy-binance-stop` 只请求旧进程正常停止；bridge 在任何工件写入前取得相同 canonical Stage 7 writer-root guard，只接受全订单族签名零单、零未决与同 binding writer；全程无交易 mutation |
| Stage 7 公开/私有证据 | `src/runtime/grid/stage7_public_runtime.rs` | `src/runtime/grid/stage7_public_journal.rs`、`src/runtime/grid/stage7_private_evidence_recovery.rs`、`src/runtime/grid/stage7_public_evidence_recovery.rs`、`src/exchange/grid/mod.rs`、各 adapter 私有 readback/WS；Stage 7 runtime、库存/风险证据验收均经 resolver 打开活跃证据。仅强确认的 recovery CLI 可在 clean Stop、零 pending/WAL、唯一 canonical root 锁和人工源 SHA/规范选择 root/quarantine root/全覆盖 root/尾序/冲突数全部锚定下，隔离一个已证明的连续重复 fork 并建立派生日记；原件永不改写，派生初始前缀不可变，private 后续边界由 handoff 回执继续承诺。Shadow 只打开既有工件并在内存中重放，绝不创建/更新 journal、checkpoint、lease、control 或 evidence；三所盘口 mutation 新鲜度只认 payload 交易所事件时间，本机接收时间仅作原始取证 |
| 库存恢复安装与被动回退 | `src/runtime/grid/stage7_epoch_install.rs` | 成交价优先；完整网格会穿价时才把 BBO 中点、原 fill 与 crossing proof 在订单 WAL 前写入 epoch，closing wave 仍先于 opening wave；显式“跳过市价补仓”重启允许按签名库存裁剪 closing，但两腿 opening 仍必须各自完整 |
| 库存恢复实证与只读验收 | `src/runtime/grid/stage7_inventory_recovery_evidence.rs` | `src/bin/verify-grid-inventory-recovery.rs`；hash-chain JSONL 绑定 admission/executable/config、签名私有 generation、owned maker fill、最终 anchor 与可选 passive-book fallback；settlement 以网格库存 generation 为准，并兼容验证风险 lane 已把 checkpoint 水位推进一代的既有实盘记录 |
| 共享 Grid 行为内核 | `src/runtime/hedged_grid/mod.rs` | `fill_driver.rs`、`rebuild.rs`、`risk_snapshot.rs`、`exposure_repair.rs`；三家实盘均由 `stage7_resident.rs` 组合，旧壳仅保留迁移兼容，须待完整验收后删除 |
| 当前 Binance 配置/root | `venue.grid.toml` | 服务器 canonical root `/home/cta/venue/artifacts/hedged_grid_sol_usdc`；运行中的 immutable release 必须由进程、admission 与 executable SHA 共同核验，禁止以本文固定发布号替代实测；风险为 Live `3 / 0.05 / 0.30`；Shell 加载 CRLF `.env` 时必须去除行尾 CR，禁止改变凭证内容 |
| 当前 Gate 配置/root | `venue.gate.example.toml` | 服务器 `/home/cta/venue/artifacts/gate_doge_usdt`；风险为 Live `3 / 0.05 / 0.30`；发布号和当前订单数以服务器签名 doctor 与 handoff 收据为准，不在长期文档固化 |
| 当前 Bitget 配置/root | `venue.bitget.example.toml` | 服务器 canonical root 为 `/home/cta/venue/artifacts/bitget_doge_usdt`，风险为 Live `3 / 0.05 / 0.30`；活动 release、运行状态、订单数、pending/WAL 与 custody 只以服务器进程、新鲜签名证据及 writer/handoff 收据为准，不在长期文档固化瞬时值 |
| 固定 Node 二进制验证 | `scripts/verify_venue_node_binaries.ps1` | 验证六个 `venue-node-*` 是唯一固定 venue 启动产物；旧 hedged-grid 发布准备脚本与生产 binary 已删除 |
| Linux Node 发布目录 | `scripts/package_venue_node_linux_release.sh` | 只构建并放入版本化目录的六个 `venue-node-*`，附 SHA256SUMS/manifest；`--preflight-only` 只检查发布条件，绝不操作实盘进程、凭证或账户 artifacts；`verify_venue_node_linux_release.ps1` 静态验收该 allow-list |

## 交易所 adapter

| 功能 | 首要入口 | 直接继续 |
|---|---|---|
| 网格所需窄 contract | `src/exchange/grid/mod.rs` | `src/exchange/grid/public_market.rs` 拆分三所公共订单簿桥，`adapter_tests.rs` 与 `event_time_tests.rs` 覆盖规则/协议时间；规范命令与私有 readback 类型仍由入口声明；`UmOrder`、`UmConditional`、`UmAlgo` 必须各自完整签名或绑定当前 execution profile 的显式不支持，正常订单投影须逐项等于 `UmOrder` 快照；Stage 7 没有条件/Algo WAL owner，已签名非空行拒绝常规族 writer；Binance mutation 只返回标量 `orderId` |
| Binance 公共/私有/PAPI | `src/exchange/binance/mod.rs` | `src/exchange/binance/{private,portfolio,risk_readback,binance_fill_pagination,signer,clock,public_stream,market_scan,order_parameters}.rs`；`order_parameters.rs` 封装 PAPI hedge-mode 的 GTC/GTX/IOC、market/reduce 与 conditional/algo stop 参数编码；网格 readback 保存 PAPI normal 与当前 Algo 的独立已签名页；已退役的 UM conditional 族显式不支持，字段不全一律拒绝；`doctor --private` 同时核对风险快照 |
| Gate 旧协议兼容 | `src/exchange/gate/mod.rs` | 公共、账户/持仓与风险纯协议位于 `crates/venue-gateway-gate/src/{public,private,risk}.rs`；根路径保留历史协议、重导出和错误映射，不是新的生产启动入口。旧风险减仓回报只有严格 `t-ord-etp-{l|s}-<16 小写 hex>` 才可归因 hedge side，其他不透明 text 保持未知；旧规则缺最大下单量时仅供兼容读取，不能准入新链订单 |
| Bitget 旧协议兼容 | `src/exchange/bitget/mod.rs` | 公共、账户/持仓与风险纯协议位于 `crates/venue-gateway-bitget/src/{public,account,risk}.rs`；根路径保留历史协议、重导出和错误映射，不是新的生产启动入口。旧账户/设置/持仓/订单/成交五面任一失败即作废整轮，不跨尝试拼成一代；终态订单必须保留 `tradeSide` 与腿/方向一致性，成交时间优先 `execTime`；尚有风险减仓待结算的旧工件不推进成交历史窗口 |
| 三所风险原始证据离线重放 | `src/exchange/shared/risk_replay.rs` | 严格消费 Binance、Bitget、Gate 各自固定数量与顺序的原始 payload tuple，复用 adapter parser 输出规范 account/legs；缺失、冗余、乱序或篡改失败关闭 |
| 统一 WS/HTTP CONNECT | `src/exchange/shared/websocket.rs` | adapter 的连接调用点；DNS、全部解析地址、TCP、HTTP CONNECT 与 TLS/upgrade 共享一次 10 秒总期限，单个地址不得重置预算；超时工作线程不能阻塞账户 resident；握手后改用 1ms readiness poll，公共流再以帧数和 5ms 公平时间片让位私有成交；`src/backoff.rs` 为公共/私有连接与启动重试提供有上限、按账户/进程错峰的指数退避 |
| 私有 session 与 generation | `src/exchange/shared/private_session.rs` | `src/exchange/shared/private_session_state.rs` |

## 执行、安全与存储

| 功能 | 首要入口 | 直接继续 |
|---|---|---|
| 命令 WAL/journal | `crates/venue-execution/src/journal.rs` | 原 JSONL serde/hash/状态迁移与路径调用约定不变；append 持有排他文件锁并核对恢复时的耐久长度，旧进程、坏尾、空行、hash 或状态迁移分叉均失败关闭；Unix rename/创建同步父目录；`src/execution/journal.rs` 只兼容重导出，通用事实 journal 仍位于 `crates/venue-storage/src/journal.rs` |
| 限价执行政策 | `crates/venue-domain/src/domain/command.rs` | `LimitTimeInForce::{PostOnly, Gtc}` 为不可变命令字段；PostOnly 保留历史省略编码和 WAL hash，Gtc 显式编码。六 adapter 负责原生映射及即时/重启签名回读，`account_snapshot.rs`、`account_host.rs`、Runtime `reconciler.rs` 与 Node Grid/Copy 保留并精确比较政策；签名字段缺失保持未知。自动归一化仍只挂单，手动意图显式选择价格与政策；不支持的账户或策略能力继续拒绝 |
| 显式选价归一化 | `crates/venue-execution/src/account_normalization.rs` | `AccountPricedLimitIntent` 保留用户价格、政策与基础数量上限；Host 拒绝 adapter 改价、改 Owner、改方向或突破预算，归一化本身不写 WAL、不发送订单。六 adapter 只刷新规则并向下取量，不以 BBO 替换用户价格；已有自动 `AccountLimitNormalizationIntent` 继续只挂单。`SignedAccountOrderFact.created_at_ms` 只保存原生订单创建时间，旧快照缺字段保持未知，不用更新时间或本机时间伪造最近订单 |
| 手动交易 Node 桥 | `apps/venue-node/src/production_resident/manual.rs`、`control_loop.rs` | 非 Copy Actor 的显式限价与自有手动单撤单复用 Runtime/Host/Lane/WAL；稳定 request ID 与原始计划写入同一 Actor replay 的可选 manual 字段，普通策略 checkpoint 保留它。`crates/venue-runtime/src/account/resident_manual_ack.rs` 在原 Runtime 私有边界内确认手动成交；`control_loop/manual_trade_e2e_tests.rs` 验证实际 Runtime/WAL 的离线交互。重投请求先查原 WAL，Reconcile 只查原命令和签名事实，不重发；即时成交另核对精确成交和仓位变化。Copy 绑定及会改变 Grid desired 的撤单仍拒绝；全 scope 撤单、自动策略协同与真实接管尚未完成 |
| 签名订单身份归一化 | `crates/venue-execution/src/account_order_identity.rs` | adapter 仅比较规范 client ID 与签名 wire ID 的精确编码，Host 再核对 WAL 状态、native ID、family 和完整订单语义才恢复 Owner。HL 使用真实 cloid 编码，Gate 在 adapter 内还原严格 `t-` 编码；Unknown 归属不等于收敛。签名订单 quantity 是原始委托量，filled_quantity 独立保存累计成交；账户风险仍按剩余未成交量计量 |
| 耐久 Owner/native identity | `crates/venue-runtime/src/account/host_route_hydration.rs`、`owner_route_install.rs` | Node 注册 Actor 时从同一本账户 WAL 恢复 Accepted 命令的 Owner/client/native/family 路由，已成交关闭单的 fills 仍能归属。`crates/venue-execution/src/owner_routes.rs` 保留独立契约测试；不能创建第二 writer/journal。恢复后的 Stop/Flatten 由统一 Runtime/Lane/Host 撤精确自有单并等待更新签名事实 |
| Net 减仓预留与恢复 | `crates/venue-execution/src/account_net_reduce.rs` | Accepted 仅在精确 native fills 合量、无开放原单且更新完整仓位后结算；Unknown 保留预留。结算事实与签名 bootstrap 共用原 checkpoint，5MiB 上限；文件和父目录持久化成功后才释放内存预留，Hedge 快照不经过 Net 专用拒绝门。专项在 `account_host_tests.rs` |
| 单 writer 与 dispatch guard | `crates/venue-execution/src/writer_lease.rs` | `canonical_root.rs` 提供 `(exchange, trading_account_id)` 机器级账户 fence，保留既有 schema-2、hash 与 `stage7_writer_roots/v2` 路径；恢复拒绝同 revision 的主备分叉，并按 scope/generation/handoff 不变量验证可选版本；根 `src/execution/writer_lease.rs`、`src/runtime/grid/stage7_writer_registry.rs` 只作兼容 facade；symbol/Owner 级 lease 之外，不同 symbol 不能选择不同 canonical root |
| 执行门禁与物理归一化 | `src/execution/gate.rs` | `src/execution/engine.rs`、`src/risk.rs` |
| 私有事实、对账和恢复 | `src/execution/reconcile.rs` | `src/execution/{private_projection,fill_recovery,recovery_writer}.rs`、`src/runtime/shared/private_facts_worker.rs`；私有 session 与 fill cursor 使用稳定 `trading_account_id`，不得使用产品类型代替账户身份 |
| 外部 Algo 清理审计 | `src/execution/external_algo_cleanup.rs` | 独立 custody/permit/hash-chain WAL，只经 `recovery_writer` 的同一 writer 锁 dispatch；中断后先签名回读，仍在场才允许新一轮预写，已消失只结算、不重复撤单 |
| checkpoint 与权威事实 | `crates/venue-storage/src/lib.rs` | `journal.rs` 的单一 crate-private `DurableJsonl` 负责锁、完整行读取、经调用方 replay 验证后的 incomplete-tail 修复、append、文件与父目录同步；facts 与 `control_delivery.rs::OpaqueJournal` 分别验证自身 sequence/hash/格式并复用该 I/O 边界，保持既有 JSONL 字节/相对路径契约；根 `src/storage/*` 只保留 facade |
| Canary、保护、紧急降险 | `src/execution/canary_sequence.rs` | `src/execution/{canary_evidence,canary_preflight,emergency_flatten,protection_custody}.rs` |

## Scalping、行情与自动选币

| 功能 | 首要入口 | 直接继续 |
|---|---|---|
| Scalping 策略 | `crates/venue-strategies/src/scalping/mod.rs` | 纯 model/candidate memory/risk、engine 和 checkpoint 位于 crate；根 `src/strategy/scalping/{mod,checkpoint}.rs` 仅保留兼容重导出 |
| Scalping Node 行情接线 | `apps/venue-node/src/production_resident/scalping.rs` | 公共簿经 MarketHub/FeatureSource 进入统一 resident；持续 evaluate、账户意图及退出保护闭环尚需验收，不能把行情 Ready 当作自动交易完成 |
| Scalping 冻结行为与恢复参考 | `src/runtime/scalping/scalping_resident_process.rs` | `scalping_resident/live_driver/live_gateway/live_exit` 及 `scalping_live_gateway_recovery.rs` 保留既有行为；根 facade 仍可定位兼容类型，但生产只能从固定 Node 经同一账户链进入，不能重接旧 writer |
| 控制目标与自动编排 | `src/runtime/scalping/scalping_control.rs` | `src/runtime/scalping/binance_auto_shadow.rs` |
| 公共行情、订单簿、记录与回放 | `src/market/mod.rs` | `src/market/{session,orderbook,recorder,replay}.rs`；`orderbook.rs` 是 `venue-indicators` 的兼容重导出 |
| 指标与 FeatureFrame | `crates/venue-indicators/src/feature_frame.rs` | `catalog/` 将 VenuePulse 72 项行为迁入只接受规范 `PublicBar`/`PublicTrade`/`PublicBook` 的共享核心，并加入 AVL/TRIX/SAR/SUPER 扩展；`chart/` 提供 22 项商用图表指标注册、forming 克隆预览与参数化引擎；VenueFlow 的 `chart_settings.rs`、`settings_panel.rs`、`chart_view.rs` 分别负责配置、实时重算和渲染；`public_book.rs`、`public_market_source.rs`、`scalping_features.rs`；`orderbook.rs` 提供根 OrderBook 的共享实现，根 `src/indicator/mod.rs` 只重导出 |
| Binance 候选扫描 | `src/market/scanner.rs` | `src/exchange/binance/market_scan.rs`、`src/runtime/scalping/binance_market_scan.rs` |

## 测试定位

默认按影响面分层验证，不在每次局部修改后重复全工作区回归。UI 局部改动验证客户端及相应交互；单模块修改验证该模块及直接契约；交易安全修改覆盖受影响的风险、WAL、Unknown、恢复和 adapter 路径。
跨模块契约、依赖变化、架构合并或发布前集中建立全工作区通过基线。基线通过后的增量只重跑受影响专项；纯文档、注释或 lint 标注只做对应静态检查，不使既有业务测试结果失效。记录验证对应的提交/源码范围，构建缓存疑似串用时使用两个固定隔离槽并持锁核验，不新建目录、不清空共享缓存。

- 网格 reducer/风险状态测试：`crates/venue-strategies/src/hedged_grid/{reducer_tests,exposure_guard,recovery_tests}.rs`。
- 交易所 adapter 测试：`src/exchange/{binance,bitget,gate,grid}/` 内的测试文件及各交易所直接测试模块。
- 共享 resident/runtime 测试：`src/runtime/grid/stage7_grid_tests.rs` 统一组合 `stage7_grid_{core,recovery}_tests.rs`，其余专项位于 `stage7_grid_reconciliation_tests.rs`、`stage7_fill_sequence_tests.rs`、`stage7_install_recovery_tests.rs`、`stage7_inventory_recovery_evidence_tests.rs`、`stage7_exposure_composition_tests.rs`、`hedged_grid_runtime_equivalence_tests.rs`、`exposure_runtime_tests.rs` 及各 `stage7_*` 模块内测试。
- 账户运行时架构契约：`crates/venue-runtime/src/account/{tests,recovery_tests}.rs`、`account/tests/runtime_safety_tests.rs` 及 `account/{private_router,market_hub,reconciler}.rs`、`strategy/mod.rs`、`account_lane.rs` 内测试；运行时错误集中在 `account/runtime_error.rs`；覆盖多 symbol/family 隔离、durable inbox/applied cursor、Actor turn ack、Owner/Cancel 路由、邮箱与执行公平性、WAL 三态/Unknown/恢复清单、配置 epoch、Pause/Stop 残仓 custody/Flatten、三订单族能力与签名订单全语义、Net/Hedge 完整持仓腿；通用 command journal、writer lease 与账户 canonical root 测试位于 `crates/venue-execution/src/`。
- 六所候选准入审计：`scripts/verify_gateway_candidate_contract.ps1` 构建/测试六个隔离 LIVE binary，验证非生产模式前置拒绝、非生产 endpoint/header 标记缺失及缺证据时零工件失败关闭；矩阵中的 `not_reached` 和 `writer_enabled=false` 是尚未接线的真实结论，不构成实盘准入。
- Binance 旧网格测试：`src/runtime/legacy/hedged_grid_live_tests.rs`、`hedged_grid_hot_path.rs` 内测试；共享行为测试位于 `src/runtime/hedged_grid/` 与 `src/runtime/grid/`。
- Node CLI、配置：`apps/venue-node/src/lib.rs`、`apps/venue-node/src/runtime_config.rs` 和 `src/config.rs` 内测试。
- 执行、恢复、writer：`tests/*recovery*`、`tests/*writer*`、`tests/*canary*`。
- 行情与存储：`tests/market.rs`、`tests/storage.rs`。
- Scalping：`crates/venue-strategies/src/scalping/engine_tests.rs`、`src/runtime/scalping/*tests.rs`、`tests/scalping_*` 与 `tests/legacy_scalping_*`。

`bak/` 仅在用户点名迁移旧行为时按具体入口只读；不得递归扫描、修改或作为运行时依赖。
