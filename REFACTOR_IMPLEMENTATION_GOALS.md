# VENUE 重构待实现 Goals

更新：2026-08-30

## 1. 本文职责

本文只保存当前尚未完成、可以直接交给 Codex 执行的实现 Goal。完成的 Goal 必须从本文删除或改写为仍未闭合的边界，
不得把本文积累成阶段日志。长期架构继续以 [`ARCHITECTURE.md`](ARCHITECTURE.md) 为准，账户运行时、网格、恢复、
单 writer 与接管约束继续以 [`GRID_RUNTIME_REFACTOR.md`](GRID_RUNTIME_REFACTOR.md) 为准，代码入口查
[`CODEMAP.md`](CODEMAP.md)。

## 2. 已确认的生产基线

下列能力不是待重写模块：

- Binance、Gate.io、Bitget 的 Stage 7 网格已经长期运行，现有限价单、市价减仓、撤单、仓位读取、数量/价格精度、
  最小名义价值、Owner、WAL、Unknown 对账、唯一 writer、Stop/Flatten 与 handoff 是受保护的生产基线；
- 网格 reducer 与三所共享 resident 已有行为和恢复测试；
- `venue-storage::ActorAppliedStore` 已实现 journal/checkpoint 双工件、anchor、回退和崩溃边界校验；
- Control PostgreSQL inbox/outbox、delivery lease、Node polling client 和本地 opaque inbox 已存在；
- Copy 的资本、目标敞口、身份、sizing、LIMIT、delivery、ledger 与 drift reducer 已存在；
- `venue-indicators` 已有规范行情输入、FeatureFrame 和 scalping feature builder；
- Binance、Gate.io、Bitget 已有生产认证只读 collection session；Bybit、OKX、Hyperliquid 的生产 mutation 由独立任务负责。

任何 Goal 都不得另建第二套下单引擎、名义价值算法、仓位模型、Owner、WAL、journal、writer 或对账器。

## 3. 本轮范围

### 3.1 固定目标

- 交易所运行配置最终只允许精确 `LIVE`，并只连接生产 endpoint、生产凭证和生产 artifact root；
- 不再保留 `TEST` 网关、测试网/demo endpoint、TEST Node 或 TEST Copy 运行模式；
- `cfg(test)`、离线协议 fixture、mock transport、单元测试和 PostgreSQL 集成测试必须保留，但它们不是运行模式，
  且门禁不得连接真实交易所或发送真实 mutation；
- 新账户 Runtime 只桥接现有生产执行闭环，不改变订单语义和 Stage 7 工件格式；
- Copy、Control 和指标只能产生语义意图或查询投影，实际 mutation 仍由账户节点唯一 Execution Lane 完成。

### 3.2 主重构 Goals 明确不做

- Goal 1–9 不修改 `apps/venueflow/**`，不承担交易终端、桌面/Web UI、图表、交互或 UI E2E；这些路径由独立桌面终端任务
  在明确租约内并行开发；
- 不在本计划内实现 Bybit、OKX、Hyperliquid 的生产签名、POST、writer、Canary 或 mutation；
- 不迁移 `bak/`，不迁移旧策略，不新增策略，不重写 Stage 7 热路径；
- 不并行对多个交易所发送 mutation，不以数据库 lease、Control receipt 或 UI 命令替代本地 writer authority；
- 不在代码门禁中执行真实 LIVE 网络请求；真实 Canary 只能在离线门禁全绿后由独立接管步骤执行。

## 4. 并行执行规则

1. Goal 1–7 可并行；当前四并发槽建议分两批，每批最多三个子任务，主任务保留一个整合槽。
2. Goal 8 依赖 Goal 1–5，涉及生产组合与接管，三个交易所内部固定 Gate.io -> Bitget -> Binance，禁止并行 mutation。
3. Goal 9 最后独占共享 Cargo、lock、长期文档、全仓门禁、提交和推送。
4. 推荐复杂实现使用 `gpt-5.6-sol`、`high`；每个 Goal 使用独立分支/worktree 或明确文件租约。
5. 子任务只显式暂存自有路径，禁止 `git add -A`；发现共享文件需求先停下协调。
6. 依赖表是优先基线；当前 Goal 存在可验证缺口时可新增依赖，但必须先查 workspace/lock、说明理由、加入真实调用与专项测试，
   不得为未来功能预装或无理由复制同类栈。
7. Cargo 构建产物统一位于 `G:\Build\Venue`；并行任务在其下使用按 Goal/PID 隔离的子目录，禁止写入仓库 `target/`。

共享文件租约：

| 路径 | 独占 Goal |
|---|---|
| 根 `Cargo.toml`、`Cargo.lock` | Goal 9 |
| `CODEMAP.md`、`ARCHITECTURE.md`、`GRID_RUNTIME_REFACTOR.md`、本文 | Goal 9 |
| `.github/workflows/**` | Goal 6 |
| `apps/venueflow/**` | 独立桌面终端任务独占；Goal 1–9 全程冻结 |
| `bak/**` | 全程冻结 |

## 5. Goal 1：Actor Applied 与 Runtime 权威桥接

**目标**：让 Runtime 用规范 Actor identity、Owner commitment、真实 command WAL durable head、config/connection/private
generation、turn sequence 与 replay state 构造 `ActorAppliedCommit`；只有当前 durable receipt 才能让 Actor 进入 Running 或输出语义意图。

**独占路径**：

- `crates/venue-runtime/src/strategy/**`
- `crates/venue-runtime/src/account/runtime.rs` 中 Actor turn/applied 边界
- `crates/venue-runtime/src/account/tests/**` 中对应测试

**必须证明**：

- missing/stale/rollback WAL、Owner、generation、turn、inbox 或 checkpoint 全部失败关闭；
- receipt 不可由策略、Control、数据库或普通调用方伪造；
- receipt 只授权当前 Actor 语义 turn，不直接授予 capability、writer 或物理 dispatch；
- 崩溃发生在 inbox、Actor checkpoint、applied journal/checkpoint 任一边界后，重启不漏 apply、不双 apply。

**依赖**：无。**可并行**：Goal 2–7。

## 6. Goal 2：Binance 生产恢复证据桥

**目标**：把现有 Binance 认证只读六面 collection 收敛为可被共享 Runtime 验证的完整 recovery bundle；绑定完整
registry universe、账户/持仓模式、订单族、cursor、Owner/WAL/Unknown commitments、attempt、generation、deadline 和原始摘要。

**独占路径**：`crates/venue-gateway-binance/**`。

**必须证明**：完整/分页终结的 Account、Positions、UmOrder、UmConditional-unsupported、UmAlgo、FillsCursor 同 attempt；
任一 await 后 scope 漂移、分页预算耗尽、额外 symbol、未知订单或非托管 Algo 均失败关闭。不得修改现有 mutation 语义。

**依赖**：固定 LIVE recovery DTO。**可并行**：Goal 1、3–7。

## 7. Goal 3：Gate.io 生产恢复证据桥

**目标**：把现有 Gate.io 四 ACK 私流和认证只读六面 collection 收敛为共享 Runtime recovery bundle，保持 Hedge 双腿、
regular family 与 conditional/algo 明确不支持语义。

**独占路径**：`crates/venue-gateway-gate/**`。

**必须证明**：唯一 nonce Pong、四频道 ACK、完整 universe/cursor、全局 pages/bytes/deadline、原生订单身份和 structured
Unknown 同 attempt；任何 side/profile/Owner/root 漂移失败关闭。不得另建 Gate writer 或改 `poc` 下单路径。

**依赖**：无。**可并行**：Goal 1–2、4–7。

## 8. Goal 4：Bitget 最终六面 Fold 与恢复证据桥

**目标**：完成 Bitget 当前缺失的最终六面 fold，并输出共享 Runtime recovery bundle；保持 login + 三频道 ACK、UTA
账户面、normal family、unsupported family、fills cursor 与方向化 `tradeSide` 约束。

**独占路径**：`crates/venue-gateway-bitget/**`。

**必须证明**：五面任一失败整轮作废，禁止跨 attempt 拼接；完整 symbol/cursor universe 与全局预算生效；Unknown、持仓腿、
订单族或成交方向无法证明时失败关闭。不得改现有 post-only/mutation 语义。

**依赖**：无。**可并行**：Goal 1–3、5–7。

## 9. Goal 5：Copy LIVE-only 耐久工作流闭合

**目标**：复用现有 LIVE-only Copy reducer 和 PostgreSQL 模型，闭合 leader intent ->
snapshot -> planning -> delivery -> durable node receipt -> ledger/drift 投影链。此链只产生语义 job，不持有交易所客户端或 mutation authority。

**独占路径**：

- `apps/venue-control/src/copy_*`
- `apps/venue-control/migrations/0002_copy_core.sql` 及确有必要的新 Copy migration
- `apps/venue-control/tests/copy_postgres_integration.rs`
- `crates/venue-copy/**` 仅在现有 reducer 契约确有缺口时修改

**必须证明**：并发 observer 单规划、事务 outbox/inbox、lease fencing、Unknown 禁重投、receipt 幂等、账本归因、drift 新 job、
重启恢复和所有模拟崩溃窗口；数据库租约不授予本地 writer，Node 拒绝时数据库不能绕过。

**依赖**：Node 终态 Applied 联调依赖 Goal 1/8。**可并行**：Goal 1–4、6–7。

## 10. Goal 6：PostgreSQL 非跳过 CI 门禁

**目标**：为 PostgreSQL 集成测试建立独立 Linux CI job 和临时 PostgreSQL service，使 CI 中缺数据库、migration 失败或测试跳过
都直接失败；本地无数据库时可保留明确的开发提示，但不得在 CI 伪装成功。

**独占路径**：

- `.github/workflows/workspace-gates.yml`
- 新增或修改的 PostgreSQL gate 脚本
- `apps/venue-control/tests/account_delivery_postgres_integration.rs` 中仅强制门禁入口

**必须证明**：migrations 可重复执行；Control delivery 与 Copy 的并发、lease、Unknown、receipt、崩溃恢复测试实际运行；日志明确
打印测试数据库已连接且不得包含凭证。

**依赖**：无。**可并行**：Goal 1–5、7。

## 11. Goal 7：指标到 Control 的只读生产投影

**目标**：从规范 `PublicBar`、Trade、Book 输入持续生成 binding-scoped `FeatureFrame`，通过 Control snapshot/SSE 暴露只读
投影；本轮不实现 VenueFlow 或其他交易终端消费者。

**独占路径**：

- `crates/venue-indicators/**`
- `crates/venue-control-protocol/**` 中指标只读 DTO（基于当前 LIVE-only 契约）
- `apps/venue-control/src/indicator_*` 新文件及对应非 UI 测试

**必须证明**：venue/account/symbol/generation/provenance/age 精确绑定；Missing/Null/NotApplicable、负量、taker 超总量、跨 generation
或过期 frame 失败关闭；SSE 断线可从 cursor 恢复且 bounded；投影不含凭证、WAL、artifact 路径或 mutation authority。

**依赖**：无。**可并行**：Goal 1–6。

## 12. Goal 8：账户 Node 组合与三所 LIVE 单 writer 接管

**目标**：在 Goal 1–5 的证据闭合后，把 Binance、Gate.io、Bitget recovery bundle、Actor Applied、Control delivery、
DurableOwnerRoutes、CommandJournal、canonical root、writer lease、Execution Lane 和现有 Stage 7 physical adapter 组合成唯一账户节点路径。

**独占路径**：

- `apps/venue-node/**`
- `crates/venue-runtime/src/account/physical_recovery*`
- `crates/venue-runtime/src/account/recovery_session.rs`
- 必要的现有 Stage 7 组合入口；不得改 reducer、价格/数量/名义价值或订单编码算法

**必须证明**：

- 离线门禁下 Prepared 只有在 durable WAL、Owner、fresh recovery、Actor applied、risk、writer 和一次性 permit 全部成立后可达 dispatch；
- Unknown 重启只做签名 readback，不重复下单；Stop/Flatten 继续使用更新一代完整订单族与持仓事实；
- 旧 Stage 7 与新 Node 绝不同时写同一 `(venue, trading_account_id)`；
- 实盘接管固定 Gate.io -> Bitget -> Binance，每所分别 Stop 旧 writer、生成 immutable handoff、人工确认、小额 Canary、复核唯一 writer，
  完成一所后才开始下一所；
- Bybit、OKX、Hyperliquid 继续失败关闭，不得顺手接入。

**依赖**：Goal 1–5；Goal 6 必须在真实接管前通过。**不可与其他 production mutation 并行**。

## 13. Goal 9：最终整合、文档与发布门禁

**目标**：独占处理共享 Cargo/lock、exports、长期文档和最终提交；删除已经完成的 Goal，只保留真实未完成项。

**独占路径**：根 `Cargo.toml`、`Cargo.lock`、三个长期文档、本文、workspace scripts，以及各 Goal 明确交给整合者的 module export。

**必须通过**：

```text
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo test --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
scripts/verify_workspace_policy.ps1
scripts/verify_repository_hygiene.ps1
scripts/verify_fixed_deployment_binaries.ps1
scripts/verify_venue_node_binaries.ps1
git diff --check
```

另须证明：PostgreSQL CI 实际运行、全部 fixed binary 只接受 LIVE、构建和测试不连接真实交易所、Goal 1–9 未越界修改
`apps/venueflow/**`、`bak/**` 无越界改动、独立 UI 提交已明确归属、工作树 clean、HEAD 与目标远端一致。真实 Canary 收据单独保存为
受保护 artifact，不作为普通测试日志处理。

## 14. 建议执行批次与工期

| 批次 | Goals | 可并行度 | 代码工期估算 |
|---|---|---:|---:|
| 1A | Goal 1、2、3 | 3 | 3–6 小时 |
| 1B | Goal 4、5、6、7 | 3，分两轮 | 4–8 小时 |
| 2 | Goal 8 | 1；三所顺序接管 | 6–12 小时，不含交易所观察窗口 |
| 3 | Goal 9 | 1 | 2–4 小时 |

在接口稳定、无外部凭证/交易所阻塞时，剩余纯代码约 15–30 小时；真实三所 Canary 与稳定性观察另按每所独立窗口安排，
不得为了压缩墙钟时间并行发送 mutation。
