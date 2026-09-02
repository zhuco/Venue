# VENUE 文档目录

当前只围绕一个结果：完成 Binance KOL 跟单 MVP。新用户通过 KOL 邀请链接注册，登录并绑定 API Key；系统验证 Portfolio Margin UM 统一账户、UM 交易权限与双向持仓，随后由 Binance 多账户 Executor 快速复制 KOL 认证账户流中的真实成交。初期全站不超过 5 个启用 KOL、200 个启用跟单账户。

Grid、Gate.io、Bitget、其余交易所和 Scalping 均暂停，不作为当前验收或新执行链前置。冻结旧 Node 仍按原工件处理，但不得与新链写入同一真实账户。

## 阅读入口

| 文档 | 用途 |
|---|---|
| [项目说明](../README.md) / [更新说明](CHANGELOG.md) | 当前功能、版本与未完成边界 |
| [架构](ARCHITECTURE.md) / [代码地图](CODEMAP.md) | 技术栈、模块职责和代码入口 |
| [Binance KOL 跟单 MVP](KOL_COPY_MVP.md) | 当前唯一目标、轻量架构、产品流程、阶段计划与验收 |
| [旧迁移导航](UNIFIED_GATEWAY_WEB_MIGRATION.md) / [旧运行时兼容](GRID_RUNTIME_REFACTOR.md) | 冻结 Grid/Node 的代码与工件边界；不作为 MVP 模板 |
| [开发指南及构建规则](DEVELOPMENT.md) | 影响面验证、工作树合并、本地 Ubuntu 编译 |
| [Node](NODE.md) / [Web](WEB.md) / [账户管理](ACCOUNT_MANAGEMENT.md) | 应用入口、配置与凭证边界 |

长期说明只在本目录维护。根 README、AGENTS、CODEMAP 和组件 README 保留必要入口，不重复正文。文中的源码及命令路径均相对仓库根，另有说明除外。

原“剩余工作”“停用清单”“构建规则”独立文件已分别并入执行契约、架构、开发指南，不再维护重复版本；已删正文可从 Git 历史恢复。停用入口见[架构第 11 节](ARCHITECTURE.md#deprecated)。
