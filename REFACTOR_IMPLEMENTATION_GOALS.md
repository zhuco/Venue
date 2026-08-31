# VENUE 剩余开发与验收

更新：2026-09-01

本文只列当前尚未闭合的工作；不是自动继续执行全部任务的指令。每轮先明确获准范围，
尤其文档整理不自动触发业务开发、部署或实盘。完整契约见
[统一迁移](UNIFIED_GATEWAY_WEB_MIGRATION.md)，安全与接管见 [运行时契约](GRID_RUNTIME_REFACTOR.md)，
当前已提交能力见 [README](README.md)，开发入口见 [DEVELOPMENT](DEVELOPMENT.md)。

## 已有基础，不重复开发

六个固定 Node 已组合账户 Runtime/Execution Lane/Host，Copy 已有物理桥和签名结果到 ledger 的实现，
响应式 `apps/venue-web` 已建立，Stage 7 根生产 binary 已移除，Ubuntu 本机编译入口已落实。
不要再以“复制一套网关”“新建整个 Web”“重写 Copy 纯规划”作为默认起点。

## 仍需闭合

| 工作范围 | 当前缺口 | 完成证据 |
|---|---|---|
| 六所公共行情 | Bitget/Hyperliquid 权威闭合 bar；持续连接/重连的实际完整性 | 真实确认语义、断线/缺口失败关闭、来源时间及有界排队；forming 不冒充 closed |
| Scalping | 签名安全投影、入场确认、服务端保护及退出流程 | 同一 Actor/WAL/Host 故障恢复与逐所验收；未完成时禁止自动入场 |
| Grid/账户运行时 | 私流成交驱动、库存恢复、控制与多 symbol 常驻协同 | 共享 reducer 热路径、Stop/Flatten/Owner/Unknown 与公平性契约，不能只测纯 reducer |
| Copy 产品闭环 | 实际 leader 权威输入、连续 ledger/drift 与过期/Unknown/跨零恢复 | 真实 Node 签名最终目标与 Control 账本一致；中间归零不是最终完成 |
| 手动交易 | Grid desired/库存协同、Copy binding、全 scope 撤单 | 精确归属与更新签名订单/仓位；部分完成不回报“全部成功” |
| 旧三家生产接管 | 服务器旧 release、writer、WAL 与未决订单事实 | 旧 writer 停止、前驱记录有效、Unknown 收敛、新链唯一锁及逐所 Canary |
| UI 集成验收 | 实际 Node/Control/BFF/Web 连通、布局/易用性/速度 | 五视口逐页截图、恢复失败关闭、交互确认及分段延迟；fixture 不能代替实测 |
| 自助账户扩展 | 当前已提交托管/验证仅 Binance；其他所与公众 Web 产品未完成 | 独立账户能力、凭证管理与归属验收；不靠展示按钮宣称支持 |

上述内容在迁移契约 T0–T8 中按依赖切分；只有具体实现任务获准时才启动相应子任务。
不把全部待验收项无限加入一次文档或小修复任务。

## 验证与执行边界

采用 [开发指南](DEVELOPMENT.md) 的影响面验证；公共契约/依赖或正式发布才集中全量。
Web 的 typecheck/unit/build/边界扫描与隔离浏览器通过不代表真实交易完成；
PostgreSQL 测试必须确认实际执行而非缺配置跳过。

既有实盘授权的范围与技术约束保留在迁移契约第 2.1–2.2 节，但不扩大当前请求：
单笔和更严格账户累计 10U 门、逐所串行、唯一 writer、Unknown 不重投始终保留。
真正需要凭证/后台权限等外部协助时列明缺少的证据和最小操作，不伪造通过或改写真实持仓。
