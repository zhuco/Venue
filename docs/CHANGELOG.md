# 版本与更新说明

## v0.1.0-alpha.18 — 2026-09-04

- Grid 补库存/盈利减仓配置不再无条件禁用成交热路径；正常滚动使用有效缓存，实际风险动作和失效事实仍转签名恢复。
- 完整 Binance RESULT 逐字段确认 Maker 补撤，移除正常批次的串行 REST 查单；身份型 ACK、成交竞态与超时继续同 ID 对账。
- 成交投影维护加权开仓均价；私流空闲轮询降为 1 ms，启用限定范围的规划及发送时延日志。10 ms 为出网验收目标，不表示该版本已通过实盘性能验收。

## v0.1.0-alpha.3 — 2026-09-03

- Binance KOL、桌面手动交易与新对冲 Grid 统一进入单个多账户 Executor；Grid 使用 PostgreSQL 命令账本、签名私有事实、稳定 `clientOrderId` 和超时签名查单，不恢复 Actor/checkpoint/WAL。
- 新增 Grid 生命周期、配置、目标订单面、订单归属、成交分配、热批次和跨微批前驱链。每笔完整 Maker 成交独立重算，通常形成 2 Place + 1 Cancel；相邻成交可连续持久化，但后批出网等待前批全部签名确认。
- 桌面终端接入真实私有持仓、当前/历史委托、成交、仓位历史、Grid 与资产，多标签交易对、账户备注、Maker 默认、快捷键和精确撤单/市价平仓均走同一新链。
- 旧 Binance Grid 未完成安全收敛前仍按账户互斥，不能与新 Executor 并行写同一账户。真实 PostgreSQL、Binance Canary 和 10 ms p95 以发布验收记录为准。

## 先前未发布：Binance KOL 跟单 MVP 架构收敛

- 当前唯一下一步目标调整为 Binance KOL 跟单 MVP：邀请注册、登录、API Key 绑定验证、基础 KOL 交易终端、快速真实跟单和 KOL 自助页面说明。
- 目标架构收敛为 Web/BFF、Control、PostgreSQL 和单个多账户 Binance Executor；最多 5 个 KOL、200 个启用跟单账户，不为每个账户建立进程、Actor、Checkpoint 或本地 WAL。
- 旧 Grid、六所 Node、Gate.io/Bitget 接管及复杂迁移链冻结；保留既有源码和恢复事实，但不作为 MVP 前置，新旧执行链不得同时写同一账户。
- 新增 [`KOL_COPY_MVP.md`](KOL_COPY_MVP.md) 作为产品、架构、P0–P5 开发顺序、安全和 2核4G 容量验收的唯一长期契约；P0 同步加入 0017 数据约束、KOL/终端协议及单 Executor 数据库锁边界，但 HTTP、页面、物理执行与实盘闭环仍未完成。

## 先前未发布记录：三目标文档收口（范围已停止）

- “剩余开发与验收”并入迁移契约，明确交易终端、真实跟单、旧三所接管的各自完成证据、依赖与可复制 Goal 提示词。
- “停用清单”并入当时的架构文档，“构建规则”并入开发指南；删除三份重复文档，保留必要导航。原内容可从 Git 历史恢复。
- 当时范围为 Binance 优先，并包含 Gate.io、Bitget 接管；该范围现已停止。下列 alpha.2 的原排期仅描述历史版本，不是当前任务指令。
- 本次只修改文档和入口，不改代码、依赖、风险门、数据库或部署，不创建/启动长期 Goal；产品版本保持 alpha.2。

## v0.1.0-alpha.2 — 2026-09-01

工作区整合预览，不代表部署或生产接管完成。

- 一并纳入 VenueFlow 桌面 UI、行情历史补载、图表拖动/缩放、手动金额输入与执行订单/持仓/成交视图。
- 纳入共享 EMA/ADX 图表指标、参数/显示设置和专项测试，桌面/WASM 复用指标核心。
- 纳入 Windows `keyring` 3.6.3 登录凭证库及 mock 测试；只保存 Venue 登录资料/会话，不保存交易所 API Key。
- 整合旧工作树；历史构建门禁快照保留在 Git 中，运行代码使用主线较新实现，不恢复停用发布脚本。
- 长期文档集中到 `docs/`，同步 README、功能地图、架构、开发与停用说明。
- 明确 Binance 第一批，其他五所第二批验证与实盘；Scalping 暂缓。Web 可发起下单但必须复用 Node 统一执行链，完整 Web 下单仍待验收。
- `bak/` 不再作为维护来源，用户授权直接删除、不备份；该授权不包含数据库、凭证和运行恢复工件。目录是否删除以实际清理结果为准。

本版没有交易所 mutation、远端部署或新增服务器编译。离线构建/测试不替代 PostgreSQL 实际执行、UI 截图或真实网关联通验收。

本批源码已通过 fmt、workspace/all-targets check、workspace test 与 VenueFlow WASM check；数据库测试未配置时的提前返回不作为真实 PostgreSQL 集成通过。文档与范围说明另做链接、依赖/源码行数、仓库卫生及 diff 检查，不重复无关业务回归。

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
这些是 alpha.1 的历史限制；当前开发和验收只以 [KOL MVP 契约](KOL_COPY_MVP.md) 为准。保留失败关闭，不以版本号扩大实盘授权。

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
