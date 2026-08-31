# VENUE 文档目录

当前优先完成 Binance；Gate.io、Bitget、Bybit、OKX、Hyperliquid 进入第二批验证与实盘。Scalping 暂缓，不作为两批收工门槛。

Web 可以提供下单界面，所有订单仍由 Node 的统一风险、WAL 和账户 writer 执行；Web 下单闭环尚待完成验收。

## 阅读入口

| 文档 | 用途 |
|---|---|
| [项目说明](../README.md) / [更新说明](CHANGELOG.md) | 当前功能、版本与未完成边界 |
| [架构](ARCHITECTURE.md) / [代码地图](CODEMAP.md) | 技术栈、模块职责和代码入口 |
| [迁移开发契约](UNIFIED_GATEWAY_WEB_MIGRATION.md) / [剩余工作](REFACTOR_IMPLEMENTATION_GOALS.md) | 两批范围、子任务、退出条件 |
| [运行时与网格](GRID_RUNTIME_REFACTOR.md) | 唯一 writer、WAL、风险、恢复与接管 |
| [开发指南](DEVELOPMENT.md) / [构建规则](BUILD_POLICY.md) | 影响面验证、工作树合并、本地 Ubuntu 编译 |
| [Node](NODE.md) / [Web](WEB.md) / [账户管理](ACCOUNT_MANAGEMENT.md) | 应用入口、配置与凭证边界 |
| [停用清单](DEPRECATED.md) | 已移除入口、冻结兼容及替代方法 |

长期说明只在本目录维护。根 README、AGENTS、CODEMAP 和组件 README 保留必要入口，不重复正文。文中的源码及命令路径均相对仓库根，另有说明除外。
