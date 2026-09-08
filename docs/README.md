# VENUE 文档目录

当前 v0.1.0 源码包含 Binance KOL/终端/Grid、五所独立策略与网格，以及 Bybit 支撑分批做多桌面闭环；共用单例多账户 Executor 与 PostgreSQL 命令账本。版本与功能摘要以 [项目 README](../README.md)、[VERSION](../VERSION) 和 [CHANGELOG](CHANGELOG.md) 为准；部署或实盘验收状态须单独核验。

| 要解决的问题 | 说明入口 |
|---|---|
| 当前版本有哪些功能 | [README](../README.md)、[CHANGELOG](CHANGELOG.md) |
| 桌面与 Web 应该改哪一端 | [UI 入口](../apps/ui/README.md) |
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

旧统一迁移计划已退出当前范围，其冻结边界统一保留在架构和 Grid 契约；不再单独维护旧计划导航。被合并或删除的正文可从 Git 历史恢复。文中的源码路径相对仓库根，Markdown 链接相对所在文档。
