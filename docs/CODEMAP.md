# VENUE 功能代码地图

终端低延迟链：`apps/ui/desktop/src/market_client/native/delivery.rs` 从行情线程唤醒绘制，Binance WS 在 `market_client.rs` 优先直连、失败回退 HTTPS 中继；`client/execution/stream.rs` 消费认证账户快照 SSE。Control `http/accounts/terminal_stream.rs` 逐次校验会话及账户归属；`database_wake.rs` 与迁移 `0044` 提供提交后通知，原轮询负责断线恢复。

行情状态中文提示：`apps/ui/desktop/src/i18n/market.rs` 区分更新延迟、自动校时、限流、访问受限、合约不可用与校验失败；`market.rs` 在新的有效行情到达后恢复延迟状态，连接状态消息不刷新行情接收时间。

六所公共行情的自动校时：`crates/venue-gateway-api/src/display/clock.rs` 维护仅供显示的单调时钟；Bybit `display::synchronize_display_clock` 读取精确只读 `/quotes/bybit/v5/market/time`，桌面 `market_client/native/multi.rs` 自动刷新。它不参与签名、账户事实或系统校时。

Bybit 桌面实时行情：`apps/ui/desktop/src/market_client/native/multi/bybit_stream.rs` 独立维护当前图表的 WS、历史与统计任务；`crates/venue-gateway-bybit/src/display/stream.rs` 复用原盘口序列与逐笔解析，公开范围不携带账户身份。

桌面多交易所公共行情：`apps/ui/desktop/src/market_client/native/multi.rs` 负责显示订阅及代次；`crates/venue-gateway-{bybit,bitget,gate,okx,hyperliquid}/src/display.rs` 解析各所公开协议；`crates/venue-gateway-api/src/display.rs` 提供有界显示数据与已有领域类型转换；`scripts/configure_desktop_https.py` 管理固定只读代理。语言入口为顶部 `ui.rs` → `settings_panel.rs` 通用页。

本页只定位当前入口与直接依赖。产品范围见 [README](../README.md)，职责见 [架构](ARCHITECTURE.md)，行为与验收见 [KOL MVP](KOL_COPY_MVP.md)、[人工带单](LEADER_ORDER_MIRROR.md) 和 [Grid 契约](GRID_RUNTIME_REFACTOR.md)。路径均相对仓库根。

当前发布链是 Control + PostgreSQL + 单例 `venue-executor-binance`，由桌面与 Web 消费。先按下面的功能进入代码；旧 Node/Actor/WAL 单列为冻结兼容，不能作为新链模板。

## 进程与构建

| 入口 | 位置 |
|---|---|
| Rust workspace、依赖及固定工具链 | `Cargo.toml`、`Cargo.lock`、`rust-toolchain.toml` |
| 产品版本与发布范围 | `VERSION`、`docs/CHANGELOG.md` |
| Control HTTP 服务 | `apps/venue-control/src/bin/venue-control-server.rs` |
| 共享 Executor：私有投影、人工带单、Grid 与独立策略调度 | `apps/venue-control/src/bin/venue-executor-binance.rs` |
| Executor 只读账户及未决命令检查 | `apps/venue-control/src/executor_runtime/inspection.rs`；`venue-executor-binance inspect-account CREDENTIAL SYMBOL`，只读数据库与签名查询，不启动调度 |
| 带单授权、撤权及仅迁移命令 | `apps/venue-control/src/bin/venue-leader-bot-admin.rs`、`apps/venue-control/src/leader_bot_admin.rs` |
| 版本化迁移及校验 | `apps/venue-control/src/schema.rs`、`apps/venue-control/migrations/`；源码迁移至 0045，部署版本须读取目标数据库确认 |
| 本地受控构建与缓存准入 | `scripts/Invoke-VenueBuild.ps1`、`scripts/venue_build_guard.ps1` |
| Ubuntu Control/Executor/管理员工具打包 | `scripts/Build-VenueUbuntu.ps1`；`-Component Control` |
| CI、源码卫生及依赖边界 | `.github/workflows/workspace-gates.yml`、`scripts/verify_repository_hygiene.ps1`、`scripts/verify_workspace_policy.ps1` |

所有 Cargo 操作和固定缓存规则见 [DEVELOPMENT](DEVELOPMENT.md#build-policy)，发布及回滚见 [发布清单](DEVELOPMENT.md#executor-release)。

## 账户、KOL 与人工带单

| 功能 | 首要入口与直接契约 |
|---|---|
| 用户、会话、密码、凭证密文及归属 | `apps/venue-control/src/accounts/`、`crates/venue-control-protocol/src/accounts.rs` |
| KOL、邀请、跟随关系和终端投影协议 | `crates/venue-control-protocol/src/kol.rs`；Control 的 accounts、HTTP、repository 边界 |
| 带单多配置目录与创建/编辑/启停 | `apps/venue-control/src/accounts/leader_bot.rs`、`crates/venue-control-protocol/src/leader_bot.rs`；迁移 0028/0029/0034 |
| 跟单 API 授权与托管账户 | `apps/venue-control/src/accounts/{credentials,managed_followers}.rs`、`crates/venue-gateway-binance/src/credential_probe.rs`；迁移 0030/0031/0038/0041，保存定比/定额后验证并自动申请激活，删除免密码并在撤单对账后擦除凭证 |
| 托管账户逐账户参数及生命周期 | `apps/venue-control/src/accounts/follow_requests.rs`；迁移 0033；`/v2/kol/managed-followers/follow/{status,settings,lifecycle}`，暂停态仅作为执行安全闸门 |
| 定比/定额数量 | `crates/venue-control-protocol/src/follow_sizing.rs`、`apps/venue-control/src/order_mirror/planner.rs`；开仓向上取整/最小合规额与回读共用 `apps/venue-control/src/executor_exchange/copy_risk.rs`，回归 `apps/venue-control/src/executor_exchange/copy_rounding_tests.rs`；迁移 0032 |
| 限价/市价/止损源单、子单映射、替代单及对账 | `apps/venue-control/src/order_mirror/{mod,planner,extended,store,settlement}.rs`、`executor_exchange/algo.rs`；迁移 0040 |
| 启用签名基线、空仓与过期请求保护 | `apps/venue-control/src/executor_store/activation.rs` |
| 关系领取、暂停与 revision 事务顺序 | `apps/venue-control/src/kol_executor/copy_gate.rs` |
| 旧成交目标模型的历史及未决市价命令恢复 | `apps/venue-control/src/executor_store/{market,copy_targets,copy_drain}.rs`、`executor_exchange/market.rs`；迁移 0025，不是新关系的挂单规划入口 |

## 执行、私有事实与终端命令

| 功能 | 首要入口与直接契约 |
|---|---|
| 有界持续发现、账户串行和失败隔离 | `apps/venue-control/src/executor_runtime/dispatch.rs` |
| 命令账本、领取、签名结算和耐久退避 | `apps/venue-control/src/executor_store/`、`executor_runtime/`、`executor_exchange/` |
| 停止带单时的有限历史查无核对 | `apps/venue-control/src/executor_store/mirror_drain.rs`、`apps/venue-control/src/executor_exchange/drain.rs`、`crates/venue-gateway-binance/src/account_gateway_absence.rs`；迁移 0042；只结束已确认无订单和敞口的停止目标 |
| 认证投影、私流成交与 REST 去重 | `apps/venue-control/src/private_projection.rs`、`private_projection/` |
| 非 Binance 桌面签名账户投影 | `apps/venue-control/src/accounts/strategy_projection.rs`，复用 `StrategyCredentialStore` 与独立策略 Gateway |
| Binance 权限、统一账户及双向模式 probe | `crates/venue-gateway-binance/src/credential_probe.rs` |
| 认证私流、签名基线与异常恢复 | `crates/venue-gateway-binance/src/{account_gateway,account_gateway_market_recovery,account_gateway_conditional,account_stream_projection,account_gateway_projection,private_ws,readback}.rs` |
| 原生下单、签名时钟和安全错误码 | `crates/venue-gateway-binance/src/{execution,transport}.rs` |
| 共享规则目录与后台校时 | `apps/venue-control/src/executor_exchange/catalogue.rs`、`executor_exchange/` |
| 手动 Post Only 开仓快速路径 | `apps/venue-control/src/executor_exchange/terminal_open.rs`、`crates/venue-gateway-binance/src/execution/terminal_open.rs` |
| 发送前拒绝分类与参数绑定 | `apps/venue-control/src/executor_exchange/{rejection,validation}.rs`；沿命令账本安全错误码到 `apps/ui/desktop/src/terminal_feedback.rs`，未知旧码不推测原因 |
| 逐行市价平仓与反开 | `crates/venue-control-protocol/src/terminal_position.rs`、`apps/venue-control/src/accounts/terminal/position_actions.rs`、`executor_runtime/terminal_positions.rs`、`executor_store/terminal_positions.rs`；迁移 0027 |
| 市价操作的精简持仓读取及发送后单交易对刷新 | `apps/venue-control/src/executor_exchange/terminal_market.rs`、`private_projection/terminal_positions.rs`；执行读取合并凭证/流状态，跳过成交与仓位历史查询 |

## Binance Grid

| 功能 | 首要入口与直接契约 |
|---|---|
| 纯规划：目标价位、数量、滚动、库存和盈利减仓 | `crates/venue-strategies/src/hedged_grid/planner.rs`、`planner_tests.rs` |
| 配置与生命周期协议 | `crates/venue-control-protocol/src/grid.rs` |
| 持久目标、版本 CAS、订单归属与批次尾 | `apps/venue-control/src/grid_store.rs`、`grid_store/{surface,reads,types}.rs`；迁移 0021–0024 |
| 私流驱动、冷恢复、热路径及风险协调 | `apps/venue-control/src/grid_runtime.rs`、`grid_runtime/{driver,fast_path,reconcile,stream_overlay,risk}.rs` |
| 自有订单改价后的安全撤净与运维重置 | `grid_runtime.rs` 的运行参数检查与生命周期撤单身份检查分离；`venue-strategy-admin binance-grid-reset USER` 从 stdin 接收完整 `GridLifecycleRequest`，复用 owner/revision/幂等边界，只保存 Reset 意图 |
| 批次组装与成交分配 | `apps/venue-control/src/grid_runtime/{batch,fills}.rs` |
| 首次明确拒单后 30 秒重置 | `apps/venue-control/src/grid_store/{rejection,convergence}.rs`、`grid_runtime/driver.rs` |
| 批内 Place-before-Cancel、RESULT 确认及计时 | `apps/venue-control/src/executor_exchange/grid_batch.rs` |
| 公开规则、标记价及必要汇率事实 | `crates/venue-gateway-binance/src/grid_market.rs` |

调用链见 [架构中的 Grid 流程](ARCHITECTURE.md#grid-flow)。旧恢复工件与迁入删除门见 [GRID_RUNTIME_REFACTOR](GRID_RUNTIME_REFACTOR.md#81-旧迁移代码删除门)。性能目标不以代码中的周期或历史测试结果代替实测。

## Binance 库存做市

独立于 Grid，行为与实盘准入见 [库存做市契约](INVENTORY_MM.md)。

| 功能 | 首要入口 |
|---|---|
| 双报价、库存偏移、波动与风险退出 | `crates/venue-strategies/src/inventory_mm/` |
| 独立配置及启动/停止协议 | `crates/venue-control-protocol/src/inventory_mm.rs` |
| 实例、命令归属、取消确认及单例协调 | `apps/venue-control/src/inventory_mm/`；迁移 `0045_inventory_mm.sql` |
| 本人 API 与签名预检 | `apps/venue-control/src/accounts/inventory_mm.rs` |
| 桌面独立创建与管理窗口 | `apps/ui/desktop/src/inventory_mm_view.rs`、`client/inventory_mm.rs` |

## 桌面与 Web

先读 [UI 入口](../apps/ui/README.md)。桌面截图、图表、盘口和机器人列表属于 desktop；URL、Cookie、邀请和 BFF 属于 web。

| 功能 | 首要入口 |
|---|---|
| VenueFlow 启动与布局 | `apps/ui/desktop/src/main.rs`、`workspace.rs` |
| 统一机器人列表、带单编辑及启停 | `apps/ui/desktop/src/leader_bot_view.rs` |
| Grid 模态配置和生命周期 | `apps/ui/desktop/src/grid_view.rs`、`client/grid.rs` |
| 账户/API/系统登录凭据库 | `apps/ui/desktop/src/account_client.rs`、`account_center/` |
| 桌面账户切换代次与迟到结果过滤 | `apps/ui/desktop/src/account_scope.rs`、`account_scope/tests.rs`、`client/execution/race_tests.rs` |
| 桌面行情切换应用边界 | `apps/ui/desktop/src/app/market_events.rs`、`app/market_events/tests.rs` |
| 私有持仓、委托、成交、资产与历史 | `apps/ui/desktop/src/execution_view.rs`、`client/execution.rs` |
| 逐行平仓/反开、下单及反馈 | `apps/ui/desktop/src/execution_view/position_actions.rs`、`trade_dock.rs`、`terminal_feedback.rs` |
| 当前账户 SSE 与写入状态门 | `apps/ui/desktop/src/client/stream_gates.rs`、`ui/status_bar.rs` |
| 图表拖动撤单并新挂 | `apps/ui/desktop/src/chart_trading/order_tags.rs`、`apps/venue-control/src/accounts/terminal/replace.rs`、`apps/venue-control/src/executor_store/terminal_replace.rs`（终态剩余量与新单释放）、`apps/venue-control/migrations/0043_terminal_replace.sql`；数据库夹具 `apps/venue-control/tests/support/kol_fixture.rs` |
| 图表与共享指标 | `apps/ui/desktop/src/{chart_view,chart_settings,settings_panel}.rs`、`chart_trading/{overlays,order_tags}.rs`（委托/持仓标签与价格线）、`crates/venue-indicators/src/chart/` |
| 服务器配置、公共行情代理、启动和 UI 日志 | `apps/ui/desktop/src/{server_connection,market_client,diagnostics}.rs`、`scripts/Start-VenueFlow.ps1`、`scripts/configure_desktop_https.py` |
| 用户首页和邀请注册 | `apps/ui/web/app/`、`components/customer-console.tsx`、`lib/customer-server.ts` |
| 普通与托管跟单账户的定比/定额授权表单 | `apps/ui/web/components/{customer-console,managed-followers-panel,managed-follow-settings,follow-sizing-fields}.tsx` |
| 独立运营控制台 `/ops` | `apps/ui/web/components/control-console.tsx`、`lib/projection-scope.ts` |
| Web 命令、边界扫描与浏览器验证 | `apps/ui/web/package.json`、`apps/ui/web/scripts/verify-boundary.mjs`、`apps/ui/web/e2e/`；见 [WEB](WEB.md) |

表中续写的短文件名相对同格首个文件的目录；终端执行相关缩写目录相对 `apps/venue-control/src/`。

## 独立多交易所策略与支撑分批做多

支撑分批做多（马丁）的当前模块、桌面/API、多币预算和市价/限价执行契约见 [SUPPORT_MARTINGALE](SUPPORT_MARTINGALE.md)。当前首个闭环只准入 Bybit LIVE；其他执行所仍按逐所真实验收放行。

以下功能纳入 alpha.28 源码，真实部署与逐所验收另行核验；完整契约见 [MULTI_VENUE_EXECUTOR](MULTI_VENUE_EXECUTOR.md)。它不扩大 Binance KOL 的复制范围，也不重新启用旧 Node 执行链。

| 功能 | 入口 |
|---|---|
| 独立策略命令与账户调度 | `apps/venue-control/src/{multi_venue_store,multi_venue_runtime}.rs` |
| 凭证、只读原命令观察及五所 adapter 组装 | `apps/venue-control/src/{multi_venue_credentials,multi_venue_exchange}.rs` |
| 已持久化命令物理边界 | `crates/venue-execution/src/durable_gateway.rs` |
| 行情精度、名义金额和条件触发风险检查 | `apps/venue-control/src/multi_venue_risk.rs` |
| 网格生命周期、签名累计成交与原子目标面 | `apps/venue-control/src/multi_venue_grid/{store,runtime,planner,tests}.rs` |
| 迁移、绑定、命令、原命令观察、Bybit 资金费及网格操作工具 | `apps/venue-control/migrations/{0035_multi_venue_executor,0036_strategy_grid}.sql`、`apps/venue-control/src/bin/venue-strategy-admin.rs`；Bybit 资金费协议在 `crates/venue-gateway-bybit/src/funding.rs` |
| 支撑分批做多规则、持久化、执行与参考行情 | `crates/venue-strategies/src/support_martingale/`、`apps/venue-control/src/support_martingale/`、migration `0037_support_martingale.sql` |
| 支撑分批协议、用户 API 与桌面闭环 | `crates/venue-control-protocol/src/support_martingale.rs`、`apps/venue-control/src/accounts/support_martingale.rs`、`apps/ui/desktop/src/{support_martingale_view.rs,client/support_martingale.rs}` |
| 马丁固定价格入场与可选止损扩展 | `support_martingale/planner.rs`、`support_martingale/stop_loss.rs`、`support_martingale/runtime.rs`、migration `0039_martingale_stop_loss.sql`；当前源码扩展，未代表 alpha.28 已部署范围 |

## 冻结兼容与共享类型

| 范围 | 入口与边界 |
|---|---|
| 六所旧账户 Node | `apps/venue-node/src/lib.rs`、`runtime_config.rs`、`production_resident/`、`control_loop/`；CLI 见 [NODE](NODE.md) |
| 旧账户 Host、Lane、Actor 和恢复 | `crates/venue-execution/src/account_host.rs`、`crates/venue-runtime/src/account_lane.rs`、`account/`、`strategy/` |
| 旧 JSONL、checkpoint 与 Actor Applied | `crates/venue-execution/src/journal.rs`、`crates/venue-storage/src/{journal,actor_applied}.rs` |
| 旧 Copy delivery 与记账 | `apps/venue-control/src/{copy_planning_postgres,copy_execution_postgres,copy_ledger_postgres}.rs` |
| 旧 Grid/Stage 7 与三所协议兼容 | `src/runtime/grid/`、`src/runtime/legacy/`、`src/exchange/` |
| 非 Binance adapter 的旧调用与协议 | `crates/venue-gateway-{bitget,bybit,gate,okx,hyperliquid}/src/`；当前独立策略调用见上节，旧运行链不因此迁入 |
| 冻结 Scalping | `crates/venue-strategies/src/scalping/`、`apps/venue-node/src/production_resident/scalping.rs` |
| 根离线工件 verifier | `src/bin/verify-grid-inventory-recovery.rs`、`src/bin/verify-grid-exposure-shadow.rs` |
| 共享规范事实及数量归一化 | `crates/venue-domain/src/domain/`、`crates/venue-execution/src/{account_snapshot,account_recovery_request,account_normalization}.rs` |
| 共享指标、订单簿与行情类型 | `crates/venue-indicators/src/`、`crates/venue-domain/src/domain/market.rs` |

冻结不等于可删除。处理旧 Node/Stage 7、WAL、Unknown 或账户迁入前完整阅读 [Grid 兼容契约](GRID_RUNTIME_REFACTOR.md)；不要从源码名称推断服务器是否仍在运行。

## 测试定位

| 验证面 | 入口 |
|---|---|
| 账户、激活、跟单、终端及 Grid 的 PostgreSQL 契约 | `apps/venue-control/tests/kol_mvp_postgres_integration.rs`、`tests/support/` |
| 带单授权、映射及生命周期 | `apps/venue-control/tests/support/leader_order_mirror.rs` |
| 账户隔离与持续调度 | `apps/venue-control/tests/support/kol_continuous_dispatch.rs` |
| Grid 批次与拒单恢复 | `apps/venue-control/tests/binance_grid_hot_batch_migration.rs`、`tests/support/grid_rejection_recovery.rs` |
| Binance adapter | `crates/venue-gateway-binance/src/` 内对应单元/契约测试 |
| 旧成交模型容量 fixture | `apps/venue-control/tests/kol_executor_capacity.rs`、`scripts/Invoke-KolCanaryDrill.ps1 -OfflineFixture`；不能代替新挂单模型容量验收 |
| 隔离数据库门 | `scripts/verify_postgres_integration.ps1`；缺少测试库标记跳过 |
| Web 与桌面 | Web `lib/*.test.ts`、`e2e/`；VenueFlow 包内测试及布局验证 |

只验证改动影响面；文档修改不启动 Cargo 编译。全量发布门、命令和报告要求统一见 [DEVELOPMENT](DEVELOPMENT.md)。
