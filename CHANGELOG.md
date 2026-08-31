# 版本与更新说明

## v0.1.0-alpha.1 — 2026-09-01

首次定义统一网关迁移后的产品开发预览基线。功能源码基线为 `ecafc5f098f8e60af562b9dbb24ca46b6d466a3c`；
本版本再整理架构、README、开发/停用入口与说明，不改变交易行为或依赖。
以本地 `v0.1.0-alpha.1` Git tag 指向的提交确定完整版本；不代表已经推送或部署。

### 已纳入

- 六所固定 Node 与账户 Runtime/Execution Lane/Host 的统一写入边界，旧三所根生产 binary 退休。
- Copy 跨零计划、原 job/WAL 身份、签名执行结果、ledger/drift 与未领取过期任务恢复。
- 显式手动限价和自有手动单撤单桥，账户登录与 Binance 凭证密文管理。
- 公共成交原生身份与连续游标分离、有界排队去重、盘口先验连续性及源时间 freshness。
- Bybit/OKX/Gate 协议确认 closed bar；Bitget/Hyperliquid 形成线保持非闭合状态。
- VenueFlow 原生/WASM 与独立响应式 Web/BFF；Ubuntu 本机交叉编译脚本和固定缓存政策。

### 已修正文档

- 根 workspace 不再被描述为只有根 package；增加 README 与统一开发入口。
- Node 文档补齐 `run --runtime-config`；保留现有 preflight/Canary，删除无效 Stage 7 命令教程。
- 修正“Copy 没有物理桥”“Web 尚未创建”“可按 PID 新建 target”等过期说明。
- 将已删除入口、冻结兼容、仍活动方法分开列出；不删除恢复代码或历史工件。
- 技术栈按实际 manifest/lockfile 描述，不把未来选型或未安装 lint 工具说成已使用。

### 已知限制与验证范围

六所生产策略闭环、旧 writer/WAL 接管、真实 UI 连通/截图/性能验收仍未全部完成；
Scalping 自动入场安全链、Bitget/Hyperliquid 权威闭合 bar、手动交易完整 scope 协同仍有限制。
细项见 [剩余工作](REFACTOR_IMPLEMENTATION_GOALS.md)。保留失败关闭，不以版本号扩大实盘授权。

源码基线已有 workspace 编译检查、分段回归、Node 112 项单测与 PostgreSQL 25 项集成验证；
当时发现的 fixture 失败已定向修复。本次仅对文档和版本文件做静态检查，
不声称重跑全量测试、完成整站 lint、安全审计或构建新服务器版本。

## 版本规则

- `VERSION` 是产品源码预览标识；alpha 序号用于尚未完整验收的基线，只有相应验收完成后才推进 beta/stable。
- Rust package 和 Web package 当前均为 `0.1.0`，是内部包元数据；本轮不批量改 manifest/lockfile。
  产品预览号不冒充这些包的 `--version` 输出，后续自动显示接线须另行实现。
- Control schema v2、Node runtime JSON v1 与存储版本独立演进；产品改号不触发数据库迁移或重写 WAL。
- 构建 release ID 可用产品号加 commit 短哈希；真实产物始终以 manifest 的完整 commit/SHA256 为准。
- 本地 tag 不包含未提交/未跟踪文件，不代表服务器正在运行该版本；本轮不上传、不启动服务、不创建远端发布。
