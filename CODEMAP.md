# VENUE 功能入口

更新：2026-08-30

本文件只回答“功能代码在哪里”。合并跟单、六交易所、指标、桌面 UI 后的目标 workspace、依赖边界、技术栈和迁移顺序查
[`ARCHITECTURE.md`](ARCHITECTURE.md)；多策略账户运行时、网格、成交热路径、库存恢复、验收和接管统一查
[`GRID_RUNTIME_REFACTOR.md`](GRID_RUNTIME_REFACTOR.md)。不要从 `bak/` 或历史提交寻找当前约束。

## 进程、配置与通用领域

| 功能 | 首要入口 | 直接继续 |
|---|---|---|
| 构建、依赖与仓库体积门禁 | `Cargo.toml` | workspace 当前包含根 package、`venue-copy`、`venue-control-protocol`、`venue-domain`、`venue-execution`、`venue-indicators`、`venue-runtime`、`venue-storage`、`venue-strategies`、`venue-gateway-api`、六个 `venue-gateway-*` adapter、`apps/venue-node`、`apps/venue-control` 与 `apps/venueflow`，resolver 固定为 3；workspace 与 `rust-toolchain.toml` 共同锁定 Rust 1.98.0；`Cargo.lock`；`scripts/verify_repository_hygiene.ps1` 执行体积和运行态文件门禁 |
| 六所网关身份、模式与能力门禁 | `crates/venue-gateway-api/src/lib.rs` | 规范 venue 固定 Binance、Bitget、Bybit、Gate.io、Hyperliquid、OKX；运行模式只接受精确 `TEST`、`LIVE`；`GatewayBinding` 固定规范账户 UUID 与 symbol，版本化 capability 必须 scope 精确、未过期、具备完整读取/私流/交易及具体 mutation 且无提现权限；未接 adapter 的 venue 在账户能力证据中保持空集、失败关闭 |
| Binance Portfolio Margin adapter | `crates/venue-gateway-binance/src/lib.rs` | `binding.rs` 固定 Portfolio Margin UM 身份；`config.rs`、`credentials.rs`、`sign.rs` 只从精确 TEST/LIVE binding 派生端点与 PAPI 签名；`instrument.rs` 严格解析 exchangeInfo 双向 symbol/rules；`public.rs` 保留 BBO/深度/成交/闭合 bar 的原始证据、事件时间、base/quote volume、trade count 与 taker-buy volume；`private.rs`、`portfolio.rs`、`fill_pagination.rs` 负责账户/仓位/订单/成交/风险和七日有界分页；根路径现有签名 readback、transport、Stage 7 capability/WAL/writer 仍是生产权威 |
| Bitget adapter | `crates/venue-gateway-bitget/src/lib.rs` | `config.rs` 固定 UTA v3 TEST Demo/LIVE，凭证与 REST/WS 签名使用 `secrecy`；`instrument.rs` 以 UTA payload 构造带时效 `InstrumentSnapshot`，不猜 contract value；`public.rs` 负责公共市场；`private.rs` 把账户、设置、持仓、normal 订单与成交绑定为同 attempt 五面候选并重放 raw hash；`account.rs`、`risk.rs` 保持账户/腿规范化；签名 transport/WS、Stage 7 capability/WAL/writer 仍留根 |
| Gate.io adapter | `crates/venue-gateway-gate/src/lib.rs` | `config.rs`、`credentials.rs`、`sign.rs` 固定 TEST/LIVE USDT futures；`public.rs`、`private.rs`、`risk.rs` 负责市场、账户、持仓与合约规则；`orders.rs` 严格规范化 regular 订单/成交与闭合分页；`order_families.rs` 重放 regular raw pages，并只以 profile v1 显式声明 Conditional/Algo unsupported；transport、签名采集、Stage 7 capability/WAL/writer 仍留根 |
| Bybit V5 adapter | `crates/venue-gateway-bybit/src/lib.rs` | 公共/私有协议与 execution 保持分页、ACK 和闭合读回边界；`transport.rs` 提供绑定型 async HTTP/私有 WS、限时限长、ACK 前有界缓存、received-at、应用层心跳与 generation；仍无 writer/WAL/capability |
| OKX V5 adapter | `crates/venue-gateway-okx/src/lib.rs` | linear SWAP 公私协议和 execution 读回均绑定账户/symbol/mode；`private_ws.rs` 解析 login、orders/account/positions，`transport.rs` 提供限时限长的 async HTTP/私有 WS、ACK 前有界缓存、received-at、心跳与 generation；`tdMode` 只接受 Cross/Isolated，仍无 writer/WAL/capability |
| Hyperliquid adapter | `crates/venue-gateway-hyperliquid/src/lib.rs` | `protocol.rs` 与 `private_stream.rs` 提供绑定型 `/info`、恢复查询和私流 generation；`transport.rs` 提供限时限长的 async HTTP/双私有 WS、ACK 前有界缓存、received-at、心跳、generation 与跨频道 fill 去重；`action.rs` 提供 MessagePack action hash、EIP-712 Agent 签名、持久 nonce 与限价下单/撤单 wire；`tid` 不是 sequence；writer/WAL/capability 仍为空 |
| CLI 定义与命令分派 | `src/cli.rs` | `src/app.rs`、`src/main.rs` |
| 六所固定账户节点产物 | `apps/venue-node/src/lib.rs` | `src/bin/venue-node-{binance,bitget,bybit,gate,hyperliquid,okx}.rs`；每个 binary 由同名、无默认 feature 单独构建，只引用一个 adapter；启动 scope 只接受精确 `TEST | LIVE`，artifact root 固定派生为 `<base>/<venue>/<mode>/<trading_account_id>` 且调用方不能覆盖；Binance/Gate/Bitget 的 `LIVE` 委托现有 Stage 7 固定部署入口，`TEST` 因旧物理 client 仅支持生产而失败关闭；Bybit/OKX/Hyperliquid 在共享 Owner/WAL/账户 fence/签名 readback/UNKNOWN/Stop/Flatten/人工 Canary 未接入前无条件失败关闭且不读凭证、不联网、不创建工件；`scripts/verify_venue_node_binaries.ps1` 与 `verify_venue_node_binary_isolation.ps1` 做逐 feature 构建及 endpoint/凭证命名空间/binding 字节级隔离 |
| 多策略账户运行时内核 | `crates/venue-runtime/src/account/mod.rs` | `authority.rs` 固定账户/实例、订单族能力及 opaque turn authority；`registry.rs` 强制 symbol 独占/config epoch/残仓 custody；`private_router.rs` 按 family + 双 ID 路由且整批成功后才推进 cursor；`market_hub.rs` 做 symbol generation 栅栏；`reconciler.rs` 校验三订单族、Net/Hedge 完整持仓腿及订单全语义；`recovery.rs` 用规范 manifest 绑定 lifecycle/fence/连接代/Actor inbox/WAL/Owner 和 journal root/tail；`runtime.rs` 原子组合；`src/runtime/account/mod.rs` 与 `src/domain/runtime_identity.rs` 只保留兼容 facade；当前无网络、writer 和物理 mutation |
| 策略 Actor 宿主与邮箱 | `crates/venue-runtime/src/strategy/mod.rs` | 私有事实与 Delta/Trade/Bar 有界无损；仅 Snapshot/Ticker/MarkFunding 合并；私有 burst 64 后让行对账/控制；一个实例一个 runtime-issued turn，durable applied receipt 后才可 Running/输出授权意图；`src/runtime/strategy/mod.rs` 只兼容重导出 |
| 跟单纯规划内核 | `crates/venue-copy/src/lib.rs` | `capital.rs` 冻结资本并计算目标敞口，跨零反向必须分两轮；`identity.rs` 固化 job/snapshot/child/idempotency 身份；`sizing.rs` 与 `limit.rs` 完成数量和跨所 LIMIT 规范化；`delivery.rs` 绑定 immutable manifest 与 Applied/Unknown/Reconciled/Rejected 持久回执，Unknown 禁止重投；`ledger.rs` 幂等投影 Copy/External/Manual 归因；`drift.rs` 只从新鲜权威持仓生成新 job 的语义修复；需落 PostgreSQL 的值类型提供 serde，但全 crate 仍无 storage/network/runtime/writer/mutation authority |
| 版本化 Control 协议 | `crates/venue-control-protocol/src/lib.rs` | schema v2 DTO 固定 snapshot、event、command、receipt 与递归校验；HTTP 路径为 `/v2/ui/snapshot`、`/v2/ui/events`、`/v2/control/commands`；策略与命令显式携带 `TEST | LIVE`，Stop/Flatten 人工确认精确绑定 mode/account/symbol/instance/config epoch；不含 handler、数据库、凭证、WAL 或交易客户端 |
| Control 服务核心 | `apps/venue-control/src/lib.rs` | transport-neutral `ControlService` 重新校验 schema v2 scope；Control 命令由 `repository.rs`、`postgres.rs` 与 `migrations/0001_control_core.sql` 持久化；Copy TEST 由 `copy_{model,repository,postgres}.rs` 与 `migrations/0002_copy_core.sql` 原子保存 leader intent/snapshot、fenced observer cursor、deterministic job、delivery outbox/inbox/receipt、ledger/drift projection 和崩溃重放；`http.rs` 提供限长、限时、可恢复 cursor 的本地 HTTP/SSE `/v2`，`venue-control-server` 负责启动；所有数据库 lease/claim 与 HTTP receipt 均不授予 mutation，当前仍无账户节点 adapter、交易客户端、writer/WAL 或 mutation authority |
| VenueFlow 原生/Web 客户端 | `apps/venueflow/src/main.rs` | `src/lib.rs` 是 WASM canvas 入口；`app.rs`、`client.rs`、`model.rs`、`workspace.rs` 共用 eframe/egui_tiles/WGPU UI 与 Control v2 DTO；native 用 Tokio/reqwest/SSE，Web 用 reqwest/EventSource；两端只访问版本化 Control API 并保留命令的精确 mode，不直连交易所、数据库或 artifacts |
| 账户 Execution Lane 调度 | `crates/venue-runtime/src/account_lane.rs` | Applied turn + journal identity receipt 绑定 connection/private/config/turn、命令摘要、native ID/family；创建先保留 Owner 路由，Cancel 精确核对 owner/family；有界公平队列、实例 Unknown fence；候选、WAL-prepared、一次性 dispatch permit 分态，WAL 后 fence 必须持久收敛；outcome/abort/readback 只收精确持久收据；不持有 writer/WAL/client，`src/execution/account_lane.rs` 只兼容重导出，Stage 7 实盘 mutation 路径不变 |
| Hedged Grid 固定部署组合 | `src/deployment.rs` | `src/bin/hedged-grid-{binance,gate,bitget}.rs`；Cargo `hedged-grid-*` feature 固定组合，只允许只读 doctor/Grid 生命周期命令，且配置交易所必须匹配；Binance 组合另允许强锚定、零交易所 mutation 的 private/public evidence 恢复命令，Gate/Bitget 明确拒绝 |
| 配置、交易所选择、账户身份、网格层数 | `src/config.rs` | `venue.toml`、`venue.grid.toml`、`venue.gate.example.toml`、`venue.bitget.example.toml`；`trading_account_id` 必须是稳定规范 UUID，同一真实账户跨 symbol 复用；`account_binding` 只表示交易所产品/模式能力 |
| 凭证环境读取 | `src/credential_env.rs` | 根 `.env` 仅作本地输入，禁止读取到文档/日志 |
| 规范交易账户、Instrument、交易对、金额、订单、仓位、成交 | `crates/venue-domain/src/domain/mod.rs` | `identity.rs` 唯一定义规范账户 UUID；`instrument.rs` 唯一定义 Instrument 与换算；`market.rs::PublicBar` 保留闭合行情 Known/Unavailable 语义；`risk_value.rs` 为策略风险事实到耐久存储提供无反向依赖的规范值接口；runtime identity/authority 唯一实现位于 `crates/venue-runtime/src/authority.rs`，根 `src/domain/mod.rs` 只保持既有 facade |
| 日志初始化 | `src/log.rs` | 日志级别在 `src/config.rs` |
| 错误汇总 | `src/error.rs` | 各领域本地错误枚举 |

## 对冲网格

| 功能 | 首要入口 | 直接继续 |
|---|---|---|
| 网格参数、epoch、订单意图模型 | `crates/venue-strategies/src/hedged_grid/model.rs` | 根 `src/strategy/hedged_grid/mod.rs` 仅兼容重导出 |
| 网格纯状态机、库存、desired ladder、maker fill/滚动 | `crates/venue-strategies/src/hedged_grid/reducer.rs` | `model.rs` 持久化 fill anchor 与 passive-book fallback 穿价证明；只有 maker 成交驱动，taker 只更新库存与对账 |
| 高暴露浮盈市价减仓 | `crates/venue-strategies/src/hedged_grid/exposure_guard.rs` | `crates/venue-domain/src/domain/risk_snapshot.rs`、`src/runtime/hedged_grid/{risk_snapshot,shadow_evidence}.rs`、`src/runtime/grid/stage7_exposure.rs`；策略 crate 只产出语义结果，持久化和 mutation 仍由根 runtime 承担 |
| 风险 Shadow 只读验收 | `src/runtime/grid/stage7_exposure_shadow_verifier.rs` | `src/bin/verify-grid-exposure-shadow.rs`；要求 risk-bound admission，逐条交叉核对同 root 原始引用，再按三所固定 raw tuple 语义重放并精确比较 account/目标 leg；全程无凭证、网络和写入，且不等于风险 Live 准入 |
| Binance 旧运行时兼容与当前共享实盘 | `src/runtime/legacy/hedged_grid_live.rs` | 旧 checkpoint/stop 兼容隔离在 `src/runtime/legacy/{hedged_grid_hot_path,hedged_grid_recovery,hedged_grid_support}.rs`，共用私有事实入口为 `src/runtime/shared/private_facts_worker.rs`；签名缺单与终态成交确认位于 legacy 的 `hedged_grid_fill_readback.rs`；当前实盘入口是共享 `run_binance_stage7_grid`，仍受 admission/handoff 门禁约束 |
| 三家共享 Stage 7 runtime | `src/runtime/grid/stage7_grid.rs` | `src/runtime/grid/{stage7_grid_binding,stage7_resident,stage7_fill_drive,stage7_exposure,stage7_risk_lane,stage7_readback,stage7_grid_model,stage7_grid_error,stage7_mutation,stage7_retry}.rs`；新空根在取得唯一 root guard 后一次性持久化 Running，已有 checkpoint 缺失 control 时按 Stop 失败关闭，Stopping 只接受显式 Reset；所有 Shadow/Live/Canary/Stop/Flatten/交接均要求三订单族签名覆盖，且 regular 投影必须与该族快照完全一致；完整 owned maker fill 经私有证据、reducer 与最小 checkpoint 后直接进入滚动 WAL/唯一 writer，不读 BBO、公共流、风险或逐单 REST；三所 exchange-native post-only 是穿价竞态的物理栅栏，明确拒绝只按精确 WAL 结果转签名对账，禁止改价、吃单或自动重试旧命令；周期风险首轮采集由单 in-flight request-only lane 执行，不持有 writer/WAL/mutation，过时代结果不入证据；初装/整网重建仍在 closing 签名确认后再次排空并重采 BBO，opening 必须全量 post-only；缺失、矛盾或非托管条件/Algo 行一律失败关闭 |
| Stage 7 Canary 与准入 | `src/runtime/grid/stage7_grid_canary.rs` | `src/runtime/grid/{stage7_canary_runtime,stage7_canary_support,stage7_canary_safety,stage7_canary_contract,stage7_canary_limit}.rs`；准入 configuration digest 显式绑定 `grid_count` 与完整风险发布参数，Live 启动严格匹配当前配置 |
| Stage 7 停止接管、清仓、健康、writer root | `src/runtime/grid/stage7_executable_handoff.rs` | schema-1 同 root 升级及 schema-2 跨主机/root 保仓迁移；`crates/venue-execution/src/canonical_root.rs` 以 `(exchange, trading_account_id)` 而非 symbol/Owner 建立机器级 canonical root 与进程锁，`src/runtime/grid/stage7_writer_registry.rs` 保持原 Stage 7 facade，Stage7、旧网格、Scalping Live、Canary 和可写恢复共用；Stop 与到期 `BlockedUnknown` 共用全订单族签名回读的 WAL 收敛与残仓 custody；最终 handoff 的 lease/readback 不得回退或跨 session；immutable receipt 后精确 fence 旧 lease，只有 receipt 绑定的 executable 可激活下一 writer generation；显式 WAL 封存只允许在签名全族为空、零未决和零本地事务边界执行 |
| Binance 外部 Algo 精确清理 | `src/runtime/grid/stage7_external_algo_cleanup.rs` | CLI `grid-external-algo-cancel` 仅在完整签名 Algo 页证明“唯一一行且两个 operator 锚定 ID 精确匹配”、regular 页仍与 grid checkpoint/WAL 绑定且旧 WAL 零未决时开放；`src/execution/{external_algo_cleanup,recovery_writer}.rs` 复用 canonical root 与 `writer.json.lock`，先 fsync hash-chain `external_algo_cleanup.jsonl` 再发一次精确撤单，HTTP 回报不作终态，必须以新签名 Algo 空页结算；不把外部单伪造成 owned order，不撤 regular 网格单 |
| Binance 旧运行时保仓桥接 | `src/runtime/grid/binance_legacy_stage7_bridge.rs` | CLI `grid-legacy-binance-stop` 只请求旧进程正常停止；bridge 在任何工件写入前取得相同 canonical Stage 7 writer-root guard，只接受全订单族签名零单、零未决与同 binding writer；全程无交易 mutation |
| Stage 7 公开/私有证据 | `src/runtime/grid/stage7_public_runtime.rs` | `src/runtime/grid/stage7_public_journal.rs`、`src/runtime/grid/stage7_private_evidence_recovery.rs`、`src/runtime/grid/stage7_public_evidence_recovery.rs`、`src/exchange/grid/mod.rs`、各 adapter 私有 readback/WS；Stage 7 runtime、库存/风险证据验收均经 resolver 打开活跃证据。仅强确认的 recovery CLI 可在 clean Stop、零 pending/WAL、唯一 canonical root 锁和人工源 SHA/规范选择 root/quarantine root/全覆盖 root/尾序/冲突数全部锚定下，隔离一个已证明的连续重复 fork 并建立派生日记；原件永不改写，派生初始前缀不可变，private 后续边界由 handoff 回执继续承诺。Shadow 只打开既有工件并在内存中重放，绝不创建/更新 journal、checkpoint、lease、control 或 evidence；三所盘口 mutation 新鲜度只认 payload 交易所事件时间，本机接收时间仅作原始取证 |
| 库存恢复安装与被动回退 | `src/runtime/grid/stage7_epoch_install.rs` | 成交价优先；完整网格会穿价时才把 BBO 中点、原 fill 与 crossing proof 在订单 WAL 前写入 epoch，closing wave 仍先于 opening wave；显式“跳过市价补仓”重启允许按签名库存裁剪 closing，但两腿 opening 仍必须各自完整 |
| 库存恢复实证与只读验收 | `src/runtime/grid/stage7_inventory_recovery_evidence.rs` | `src/bin/verify-grid-inventory-recovery.rs`；hash-chain JSONL 绑定 admission/executable/config、签名私有 generation、owned maker fill、最终 anchor 与可选 passive-book fallback；settlement 以网格库存 generation 为准，并兼容验证风险 lane 已把 checkpoint 水位推进一代的既有实盘记录 |
| 共享 Grid 行为内核 | `src/runtime/hedged_grid/mod.rs` | `fill_driver.rs`、`rebuild.rs`、`risk_snapshot.rs`、`exposure_repair.rs`；三家实盘均由 `stage7_resident.rs` 组合，旧壳仅保留迁移兼容，须待完整验收后删除 |
| 当前 Binance 配置/root | `venue.grid.toml` | 服务器 canonical root `/home/cta/venue/artifacts/hedged_grid_sol_usdc`；运行中的 immutable release 必须由进程、admission 与 executable SHA 共同核验，禁止以本文固定发布号替代实测；风险为 Live `3 / 0.05 / 0.30`；Shell 加载 CRLF `.env` 时必须去除行尾 CR，禁止改变凭证内容 |
| 当前 Gate 配置/root | `venue.gate.example.toml` | 服务器 `/home/cta/venue/artifacts/gate_doge_usdt`；风险为 Live `3 / 0.05 / 0.30`；发布号和当前订单数以服务器签名 doctor 与 handoff 收据为准，不在长期文档固化 |
| 当前 Bitget 配置/root | `venue.bitget.example.toml` | 服务器 canonical root 为 `/home/cta/venue/artifacts/bitget_doge_usdt`，当前发布 `releases/bitget-doge-usdt-77664b808a1b93b6`，风险为 Live `3 / 0.05 / 0.30`；运行状态、订单数、pending/WAL 与 custody 只以该 root 的新鲜签名证据及 writer/handoff 收据为准，不在长期文档固化瞬时值 |
| 内容寻址发布准备 | `scripts/prepare_hedged_grid_release.ps1` | 按 Exchange 以单一 feature 构建；`verify_hedged_grid_binary_isolation.ps1` 扫描生产 endpoint，拒绝链接其它 adapter；不停止进程、不复制凭证或 artifacts |

## 交易所 adapter

| 功能 | 首要入口 | 直接继续 |
|---|---|---|
| 网格所需窄 contract | `src/exchange/grid/mod.rs` | `src/exchange/grid/public_market.rs` 拆分三所公共订单簿桥，`adapter_tests.rs` 与 `event_time_tests.rs` 覆盖规则/协议时间；规范命令与私有 readback 类型仍由入口声明；`UmOrder`、`UmConditional`、`UmAlgo` 必须各自完整签名或绑定当前 execution profile 的显式不支持，正常订单投影须逐项等于 `UmOrder` 快照；Stage 7 没有条件/Algo WAL owner，已签名非空行拒绝常规族 writer；Binance mutation 只返回标量 `orderId` |
| Binance 公共/私有/PAPI | `src/exchange/binance/mod.rs` | `src/exchange/binance/{private,portfolio,risk_readback,binance_fill_pagination,signer,clock,public_stream,market_scan}.rs`；网格 readback 保存 PAPI normal 与当前 Algo 的独立已签名页；已退役的 UM conditional 族显式不支持，字段不全一律拒绝；`doctor --private` 同时核对风险快照 |
| Gate USDT 永续 | `src/exchange/gate/mod.rs` | 公共、账户/持仓与风险纯协议分别位于 `crates/venue-gateway-gate/src/{public,private,risk}.rs`，根路径只做兼容重导出/错误映射；transport/private readback、订单/成交与 Stage 7 保持生产权威；支持当前 Unified Single-Currency 权益；当前 Stage 7 profile 对常规族保存签名页，条件/Algo 明确不支持，因而无该两族 mutation 准入；风险减仓回报只有严格 `t-ord-etp-{l|s}-<16 小写 hex>` 才可归因 hedge side，其他不透明 text 保持未知 |
| Bitget UTA 永续 | `src/exchange/bitget/mod.rs` | 公共、账户/持仓与风险纯协议分别位于 `crates/venue-gateway-bitget/src/{public,account,risk}.rs`，根路径只做兼容重导出/错误映射；transport/private readback、订单/成交与 Stage 7 保持生产权威；当前 Stage 7 profile 保存 `delegateType=normal` 的常规族签名页，条件/Algo 明确不支持；账户/设置/持仓/订单/成交五面任一失败即作废整轮，禁止跨尝试拼成一代；精确终态订单只在方向化 `tradeSide` 与 position side/side 一致时接纳，成交时间优先 `execTime`；handoff 尚有风险减仓待结算时不得推进 Bitget 成交历史窗口 |
| 三所风险原始证据离线重放 | `src/exchange/shared/risk_replay.rs` | 严格消费 Binance、Bitget、Gate 各自固定数量与顺序的原始 payload tuple，复用 adapter parser 输出规范 account/legs；缺失、冗余、乱序或篡改失败关闭 |
| 统一 WS/HTTP CONNECT | `src/exchange/shared/websocket.rs` | adapter 的连接调用点；DNS、全部解析地址、TCP、HTTP CONNECT 与 TLS/upgrade 共享一次 10 秒总期限，单个地址不得重置预算；超时工作线程不能阻塞账户 resident；握手后改用 1ms readiness poll，公共流再以帧数和 5ms 公平时间片让位私有成交；`src/backoff.rs` 为公共/私有连接与启动重试提供有上限、按账户/进程错峰的指数退避 |
| 私有 session 与 generation | `src/exchange/shared/private_session.rs` | `src/exchange/shared/private_session_state.rs` |

## 执行、安全与存储

| 功能 | 首要入口 | 直接继续 |
|---|---|---|
| 命令 WAL/journal | `crates/venue-execution/src/journal.rs` | 原 JSONL serde/hash/状态迁移与路径调用约定不变；append 持有排他文件锁并核对恢复时的耐久长度，旧进程、坏尾、空行、hash 或状态迁移分叉均失败关闭；Unix rename/创建同步父目录；`src/execution/journal.rs` 只兼容重导出，通用事实 journal 仍位于 `crates/venue-storage/src/journal.rs` |
| 单 writer 与 dispatch guard | `crates/venue-execution/src/writer_lease.rs` | `canonical_root.rs` 提供 `(exchange, trading_account_id)` 机器级账户 fence，保留既有 schema-2、hash 与 `stage7_writer_roots/v2` 路径；恢复拒绝同 revision 的主备分叉，并按 scope/generation/handoff 不变量验证可选版本；根 `src/execution/writer_lease.rs`、`src/runtime/grid/stage7_writer_registry.rs` 只作兼容 facade；symbol/Owner 级 lease 之外，不同 symbol 不能选择不同 canonical root |
| 执行门禁与物理归一化 | `src/execution/gate.rs` | `src/execution/engine.rs`、`src/risk.rs` |
| 私有事实、对账和恢复 | `src/execution/reconcile.rs` | `src/execution/{private_projection,fill_recovery,recovery_writer}.rs`、`src/runtime/shared/private_facts_worker.rs`；私有 session 与 fill cursor 使用稳定 `trading_account_id`，不得使用产品类型代替账户身份 |
| 外部 Algo 清理审计 | `src/execution/external_algo_cleanup.rs` | 独立 custody/permit/hash-chain WAL，只经 `recovery_writer` 的同一 writer 锁 dispatch；中断后先签名回读，仍在场才允许新一轮预写，已消失只结算、不重复撤单 |
| checkpoint 与权威事实 | `crates/venue-storage/src/lib.rs` | `checkpoint.rs`、`journal.rs`、`facts.rs`、`private_evidence.rs`、`fill_cursor.rs`、`scalping_{evidence,risk}.rs` 集中耐久文件 I/O；根 `src/storage/*` 只保留兼容 facade/宿主扩展；原子 rename、fsync、hash/replay 与坏尾失败关闭语义不变 |
| Canary、保护、紧急降险 | `src/execution/canary_sequence.rs` | `src/execution/{canary_evidence,canary_preflight,emergency_flatten,protection_custody}.rs` |

## Scalping、行情与自动选币

| 功能 | 首要入口 | 直接继续 |
|---|---|---|
| Scalping 策略 | `crates/venue-strategies/src/scalping/mod.rs` | 纯 model/candidate memory/risk 位于 crate；根 `src/strategy/scalping/{engine,checkpoint}.rs` 保留 runtime/storage 宿主组合 |
| Scalping Shadow/Live resident | `src/runtime/scalping/scalping_resident_process.rs` | `src/runtime/scalping/{scalping_resident,scalping_live_driver,scalping_live_gateway,scalping_live_exit}.rs`；Live gateway 的未知命令恢复与单元测试分别在同目录的 `scalping_live_gateway_recovery.rs`、`scalping_live_gateway_tests.rs`，公开 API 仍由 `runtime` facade 统一导出 |
| 控制目标与自动编排 | `src/runtime/scalping/scalping_control.rs` | `src/runtime/scalping/binance_auto_shadow.rs` |
| 公共行情、订单簿、记录与回放 | `src/market/mod.rs` | `src/market/{session,orderbook,recorder,replay}.rs` |
| 指标与 FeatureFrame | `crates/venue-indicators/src/feature_frame.rs` | `public_book.rs`、`public_market_source.rs`、`scalping_features.rs`；根 `src/indicator/mod.rs` 仅适配根 OrderBook 并重导出 |
| Binance 候选扫描 | `src/market/scanner.rs` | `src/exchange/binance/market_scan.rs`、`src/runtime/scalping/binance_market_scan.rs` |

## 测试定位

- 网格 reducer/风险状态测试：`crates/venue-strategies/src/hedged_grid/{reducer_tests,exposure_guard,recovery_tests}.rs`。
- 交易所 adapter 测试：`src/exchange/{binance,bitget,gate,grid}/` 内的测试文件及各交易所直接测试模块。
- 共享 resident/runtime 测试：`src/runtime/grid/stage7_grid_tests.rs` 统一组合 `stage7_grid_{core,recovery}_tests.rs`，其余专项位于 `stage7_grid_reconciliation_tests.rs`、`stage7_fill_sequence_tests.rs`、`stage7_install_recovery_tests.rs`、`stage7_inventory_recovery_evidence_tests.rs`、`stage7_exposure_composition_tests.rs`、`hedged_grid_runtime_equivalence_tests.rs`、`exposure_runtime_tests.rs` 及各 `stage7_*` 模块内测试。
- 账户运行时架构契约：`crates/venue-runtime/src/account/{tests,recovery_tests}.rs`、`account/tests/runtime_safety_tests.rs` 及 `account/{private_router,market_hub,reconciler}.rs`、`strategy/mod.rs`、`account_lane.rs` 内测试；覆盖多 symbol/family 隔离、durable inbox/applied cursor、Actor turn ack、Owner/Cancel 路由、邮箱与执行公平性、WAL 三态/Unknown/恢复清单、配置 epoch、Pause/Stop 残仓 custody/Flatten、三订单族能力与签名订单全语义、Net/Hedge 完整持仓腿；通用 command journal、writer lease 与账户 canonical root 测试位于 `crates/venue-execution/src/`。
- Binance 旧网格测试：`src/runtime/legacy/hedged_grid_live_tests.rs`、`hedged_grid_hot_path.rs` 内测试；共享行为测试位于 `src/runtime/hedged_grid/` 与 `src/runtime/grid/`。
- CLI、配置：`tests/cli.rs`、`src/config.rs` 内测试。
- 执行、恢复、writer：`tests/*recovery*`、`tests/*writer*`、`tests/*canary*`。
- 行情与存储：`tests/market.rs`、`tests/storage.rs`。
- Scalping：`src/strategy/scalping/engine_tests.rs`、`src/runtime/scalping/*tests.rs`、`tests/scalping_*` 与 `tests/legacy_scalping_*`。

`bak/` 仅在用户点名迁移旧行为时按具体入口只读；不得递归扫描、修改或作为运行时依赖。
