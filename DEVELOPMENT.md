# 开发、验证与合并

本指南只说明当前工作方式，不自动启动 [剩余工作](REFACTOR_IMPLEMENTATION_GOALS.md) 中的任务。
先读 [CODEMAP](CODEMAP.md) 定位；涉及账户运行时、网格或部署时完整读 [运行时契约](GRID_RUNTIME_REFACTOR.md)。
业务边界见 [ARCHITECTURE](ARCHITECTURE.md)，旧方法状态见 [DEPRECATED](DEPRECATED.md)。

## 1. 工作区与环境

主工作区 `G:\Venue`，隔离工作树用于开发；只修改当前获准范围，不顺手清理他人的改动。
`bak/` 冻结；禁止扫描/修改迁移来源来代替当前代码。秘密只通过规定的环境/根 `.env` 注入，日志中不输出值。

Rust/Cargo 1.98.0，Web 默认 Node.js 24 + npm lockfile。数据库为 PostgreSQL + SQLx，
集成测试使用独立测试数据库/随机 schema；不要把主线 `.env` 的生产连接直接当作测试库。
数据库真实地址配置在工作区环境中，不固化到示例、版本文件或提交。

## 2. 按影响面验证

| 变更 | 必要验证 |
|---|---|
| 文档/注释/产品预览版本 | 引用路径、命令与 manifest 一致性、`git diff --check`、仓库卫生 |
| 单一 Rust 模块 | 受影响 package 和直接契约；不重复全 workspace |
| 交易安全 | 对应 risk/WAL/Unknown/恢复/adapter 故障路径 |
| Web/UI | 对应 typecheck/单测/构建与交互；布局变更再截图 |
| 公共契约、依赖、架构代码或正式发布 | 集中建立 fmt/check/test workspace 基线及所需专项；后续局部增量不重跑无关测试 |

Windows 所有 Cargo 命令都经过 guard。下面是需要全量基线时的命令，不是每次改文档都执行：

```powershell
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('fmt','--all','--check')
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('check','--locked','--workspace','--all-targets')
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('test','--locked','--workspace')
./scripts/verify_repository_hygiene.ps1
```

专项脚本如 `verify_venue_node_binaries.ps1`、`verify_gateway_candidate_contract.ps1`、
`verify_postgres_integration.ps1`、`verify_workspace_quality.ps1` 自带 guard，直接执行，不二次套锁。
记录验证的 commit/源码范围、被测目标及跳过项；解析 fixture 通过不等于真实连接，数据库跳过不等于集成测试通过。

## 3. 构建缓存与 Ubuntu

[BUILD_POLICY](scripts/BUILD_POLICY.md) 是完整构建约束。仅有
`G:\Build\Venue\main`、`slot-1`、`slot-2` 三个 Cargo 缓存；两个受控构建并发，槽等待最长 60 秒。
禁止按 PID/任务建 target、绕过 CARGO_TARGET_DIR、常规 cargo clean 或终止别人的构建。
总预算 150 GiB、F 空闲 100 GiB、G 空闲 20 GiB；不足时报告，不自行删缓存。

Ubuntu 默认通过 `scripts/Build-VenueUbuntu.ps1` 本机编译；先对干净、固定完整 commit 的源码运行 `-CheckOnly`。
`G:\Build\Venue\ubuntu` 存源码快照、工具缓存及版本化产物，Cargo 复用 slot-2。
脚本不自动安装工具、上传或启动服务；上传后还需核对 ELF/架构、动态库、manifest/SHA256。
本指南不提供绕过旧 writer/WAL 接管的运行捷径。

## 4. 本地应用

- [Node README](apps/venue-node/README.md) 说明真实 CLI、runtime JSON 和旧三家前驱记录要求。
- [账户管理](ACCOUNT_MANAGEMENT.md) 说明 Control/桌面启动环境；运行前先受控 build，
  再从 guard 选择的固定缓存启动已构建 binary，不用长期 `cargo run` 占用构建槽。
- [Web README](apps/venue-web/README.md) 说明 BFF 会话、同源 HTTPS、npm 验证与五视口测试。
- 所有真实交易须重新满足单 writer、风险、WAL、签名事实和获准任务范围；文档更新不触发实盘测试。

UI 完成标准包括移动/桌面布局截图、空/错误/离线状态、作用域与确认交互、网关联通和分段延迟。
本地 fixture/BFF 性能报告不能代替 Node→交易所的实际延迟；未完成项目保持待验收。

## 5. 旧代码与技术选型

先查停用清单再改调用点。已删除 CLI 不恢复；冻结兼容只修正必需恢复问题，不增加新策略或新授权层。
旧持久化字段不能为符合新类型而回写。只有生产调用点清零、行为等价、恢复兼容和实盘接管证据齐全，
才可删除被替代的执行壳。`bak/` 不在本轮清理范围。

依赖审计先查 workspace/lockfile，再看官方兼容与支持政策；“版本较旧”“本项目不再新增调用”
与“上游已弃用”分别记录。不批量更新依赖来凑最新版，不预装新的 ORM/HTTP/runtime。
当前 Web 没有 ESLint/Biome；Next build、typecheck 和边界扫描不能被写成 lint 全通过。

## 6. 合并与版本

1. 核对主线分支/HEAD、工作树 diff、暂存区和未跟踪文件，明确本批路径。
2. 文档可安全整合，但 UI/源码/lockfile 的其他未提交工作原样保留；重叠不明时停止合并。
3. 在隔离树验证并提交本批改动；建立主线回退分支，优先 `merge --ff-only`，不做 hard reset。
4. 必要时仅暂存精确重叠文件，按具体 stash OID 恢复并核对内容，不全工作区盲目 stash/pop。
5. 合并后核对提交及原本地文件哈希；不把未验证的他人改动算进当前版本。
6. 产品号在 [VERSION](VERSION)，变更范围在 [CHANGELOG](CHANGELOG.md)；本地 tag 指向准确提交，
   不自动 push、发布安装包或改变服务器服务。
