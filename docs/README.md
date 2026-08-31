# VENUE 文档目录

当前只围绕三个结果：交易终端可用、真实跟单业务可用、旧三所接管。先 Binance，再 Gate.io、Bitget；Bybit、OKX、Hyperliquid 与 Scalping 暂缓。

Web 可以提供下单界面，所有订单仍由 Node 的统一风险、WAL 和账户 writer 执行；Web 下单闭环尚待完成验收。

## 阅读入口

| 文档 | 用途 |
|---|---|
| [项目说明](../README.md) / [更新说明](CHANGELOG.md) | 当前功能、版本与未完成边界 |
| [架构](ARCHITECTURE.md) / [代码地图](CODEMAP.md) | 技术栈、模块职责和代码入口 |
| [三目标执行契约](UNIFIED_GATEWAY_WEB_MIGRATION.md) / [启动提示词](UNIFIED_GATEWAY_WEB_MIGRATION.md#start-prompt) | A/B/C 验收、单会话协作、模型、实盘范围及收工 |
| [运行时与网格](GRID_RUNTIME_REFACTOR.md) | 唯一 writer、WAL、风险、恢复与接管 |
| [开发指南及构建规则](DEVELOPMENT.md) | 影响面验证、工作树合并、本地 Ubuntu 编译 |
| [Node](NODE.md) / [Web](WEB.md) / [账户管理](ACCOUNT_MANAGEMENT.md) | 应用入口、配置与凭证边界 |

长期说明只在本目录维护。根 README、AGENTS、CODEMAP 和组件 README 保留必要入口，不重复正文。文中的源码及命令路径均相对仓库根，另有说明除外。

原“剩余工作”“停用清单”“构建规则”独立文件已分别并入执行契约、架构、开发指南，不再维护重复版本；已删正文可从 Git 历史恢复。停用入口见[架构第 8 节](ARCHITECTURE.md#deprecated)。
