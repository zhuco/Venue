# VENUE 文档目录

当前发布范围是 Binance KOL 挂单同步、VenueFlow 终端和事实驱动对冲网格，共用单例多账户 Executor 与 PostgreSQL 命令账本。版本与功能摘要见 [项目 README](../README.md)。

| 要解决的问题 | 说明入口 |
|---|---|
| 当前版本有哪些功能 | [README](../README.md)、[CHANGELOG](CHANGELOG.md) |
| 桌面与 Web 应该改哪一端 | [UI 入口](../apps/ui/README.md) |
| 如何注册、绑定和验证账户 | [账户与 API 管理](ACCOUNT_MANAGEMENT.md) |
| KOL 产品流程和验收要求 | [KOL MVP](KOL_COPY_MVP.md) |
| 如何授权、配置和启停带单机器人 | [带单机器人与挂单同步](LEADER_ORDER_MIRROR.md) |
| 进程如何协作、功能代码在哪里 | [架构](ARCHITECTURE.md)、[CODEMAP](CODEMAP.md) |
| Grid 如何规划和运行 | [Grid 开发结构](GRID_STRATEGY_ARCHITECTURE.md) |
| 当前开发中的独立多交易所策略入口 | [多交易所执行](MULTI_VENUE_EXECUTOR.md)；与 alpha.27 发布基线分开验证 |
| 如何处理旧 Grid/Node 账户和恢复事实 | [Grid 契约与旧运行时保护](GRID_RUNTIME_REFACTOR.md)、[冻结 Node CLI](NODE.md) |
| 如何构建、验证、合并及管理版本 | [开发指南](DEVELOPMENT.md) |
| 如何发布或回滚 Control/Executor | [发布及回滚清单](DEVELOPMENT.md#executor-release) |
| 如何运行和验证 Web、配置 HTTPS | [Web 指南](WEB.md) |

每类说明只保留一个主要维护位置：README 介绍项目，CODEMAP 定位代码，架构解释职责，产品契约定义行为和验收，开发指南管理构建与发布。组件 README 和根 CODEMAP 保留必要导航；AGENTS 是工作规则，CHANGELOG 保留发布历史。

旧统一迁移计划已退出当前范围，其冻结边界统一保留在架构和 Grid 契约；不再单独维护旧计划导航。被合并或删除的正文可从 Git 历史恢复。文中的源码路径相对仓库根，Markdown 链接相对所在文档。
