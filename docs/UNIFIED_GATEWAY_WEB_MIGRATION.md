# 旧统一迁移计划导航

更新：2026-09-02

原“交易终端、真实跟单、Binance/Gate.io/Bitget 接管”三目标已停止作为当前开发计划。当前唯一产品目标、目标架构、开发顺序和验收统一见
[`KOL_COPY_MVP.md`](KOL_COPY_MVP.md)。

## 当前决定

- 当前只开发 Binance KOL 跟单 MVP；Gate.io、Bitget、Bybit、OKX、Hyperliquid 与 Scalping 均暂停。
- 新链服务于新注册、经验证且明确分配给 Binance Executor 的账户，不要求先完成旧 Grid/Node 接管。
- 旧 Grid、旧 Copy Actor、账户 Node、WAL、checkpoint 与恢复工件保持冻结；它们不是新 MVP 的模板，也不得因改计划而删除。
- 同一 `trading_account_id` 只能分配给旧 Node 或新 Binance Executor 之一。旧链存在未决命令、Unknown、开放订单或仓位时不得直接切换。
- 后续若恢复旧交易所或 Grid 工作，先重新确定范围并阅读
  [`GRID_RUNTIME_REFACTOR.md`](GRID_RUNTIME_REFACTOR.md) 的冻结兼容边界，不沿用已撤销的三目标顺序。

本文件只保留旧路径导航，避免历史链接误导开发；不再维护 A/B/C 验收、旧启动提示词或逐所接管计划。
