# VENUE 文档目录

本目录维护当前使用方式、运行契约和验收要求。功能与版本见 [项目 README](../README.md)、[VERSION](../VERSION) 和 [发布摘要](CHANGELOG.md)；部署与真实交易状态须单独核验。

| 要解决的问题 | 说明入口 |
|---|---|
| 当前版本有哪些功能 | [README](../README.md)、[CHANGELOG](CHANGELOG.md) |
| 桌面与 Web 应该改哪一端 | [UI 入口](../apps/ui/README.md) |
| 桌面图表、账户显示、连接与本机预览 | [VenueFlow 契约](VENUEFLOW.md) |
| 裸 K 指标、六周期热力图及 OI/Funding 尚未通过的验收门 | [指标验收门](VENUEFLOW_INDICATOR_UPGRADE.md) |
| 桌面延迟如何采集和判读 | [延迟证据](VENUEFLOW_LATENCY_EVIDENCE.md) |
| 如何注册、绑定和验证账户 | [账户与 API 管理](ACCOUNT_MANAGEMENT.md) |
| KOL 产品流程和验收要求 | [KOL MVP](KOL_COPY_MVP.md) |
| 如何授权、配置和启停带单机器人 | [人工带单与订单同步](LEADER_ORDER_MIRROR.md) |
| 进程如何协作、功能代码在哪里 | [架构](ARCHITECTURE.md)、[CODEMAP](CODEMAP.md) |
| Grid 如何规划和运行 | [Grid 调用链](ARCHITECTURE.md#grid-flow) / [Grid 行为契约](GRID_RUNTIME_REFACTOR.md) |
| 独立库存做市、净头寸与取消确认 | [Binance 库存做市](INVENTORY_MM.md)，不是对冲网格参数组合 |
| 独立多交易所命令、网格与账户准入 | [多交易所执行](MULTI_VENUE_EXECUTOR.md) |
| 支撑分批做多的当前能力与增强门 | [策略与桌面契约](SUPPORT_MARTINGALE.md)；Binance 参考行情、Bybit LIVE 执行 |
| 如何处理旧 Grid/Node 账户和恢复事实 | [Grid 契约与旧运行时保护](GRID_RUNTIME_REFACTOR.md)、[冻结 Node CLI](NODE.md) |
| 如何构建、验证、合并及管理版本 | [开发指南](DEVELOPMENT.md) |
| 如何发布或回滚 Control/Executor | [发布及回滚清单](DEVELOPMENT.md#executor-release) |
| 如何运行和验证 Web、配置 HTTPS | [Web 指南](WEB.md) |

每类说明只保留一个主要维护位置：README 介绍项目，CODEMAP 定位代码，架构解释职责，产品契约定义行为和验收，开发指南管理构建与发布。组件 README 和根 CODEMAP 保留必要导航；AGENTS 是工作规则，CHANGELOG 保留发布历史。

已实现计划只保留当前功能入口，删除实施步骤和完成流水；历史版本详单从 Git 查阅。未完成能力与验收门保留在对应契约中。文中源码路径相对仓库根，Markdown 链接相对所在文档。
