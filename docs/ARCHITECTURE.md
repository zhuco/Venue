# VENUE 当前架构与技术栈

更新：2026-09-01

本文说明已提交源码的架构；“有实现”“已通过离线测试”“已完成生产接管”是三个不同结论。
版本和产品概览见 [README](../README.md)，代码定位见 [CODEMAP](CODEMAP.md)，开发方式见
[DEVELOPMENT](DEVELOPMENT.md)。交易安全、网格语义和接管以 [运行时契约](GRID_RUNTIME_REFACTOR.md) 为准；
剩余验收见 [迁移契约](UNIFIED_GATEWAY_WEB_MIGRATION.md)，旧入口见 [停用清单](DEPRECATED.md)。

实施分批：Binance 第一批；Gate.io、Bitget、Bybit、OKX、Hyperliquid 第二批验证与实盘。Scalping 暂缓，保留已有接口及保护，不阻塞当前收工。

## 1. 当前组成与边界

根 `Cargo.toml` 是一个包含 19 个 Rust package 的 workspace；根 `venue` package 只保留共享 facade、
历史行为兼容与两个离线 verifier。活动应用在 `apps/`，领域和网关在 `crates/`。`apps/venue-web`
是独立 npm 应用，不是 Rust package。`bak/` 与 `G:\kol` 均非构建或运行依赖。

```text
Venue Web（Next.js）→ 同源 BFF ──────────┐
VenueFlow（native / WASM）─────────────┤
                                      ↓
                    venue-control（loopback HTTP/SSE /v2）
                    PostgreSQL：账户、delivery、投影、ledger
                    venue-copy-worker：纯语义规划，不交易
                                      ↓
                    venue-node-<venue>（每个账户一个进程）
                    AccountRuntimeHost / Strategy Actor
                    → Execution Lane → risk / Owner
                    → 同一命令 WAL → 唯一账户 writer
                    → 唯一链接的交易所 adapter
                                      ↓
                                   交易所
```

原生 VenueFlow 另有无凭证 Binance 公共行情通道；WASM 保持 Control-only。公共行情不授予交易权限。
Node 的 Control polling、BFF 到 Control 的当前连接均限制为 loopback，不应把拓扑解释为已支持任意远程直连。
远程访问须有受控 HTTPS 边界；Web/UI 可以发起手动交易语义，BFF、Control、数据库和 Copy worker 不直接调用交易所 mutation；账户 Node 统一执行。

## 2. 源码职责

| 层 | 当前入口与职责 |
|---|---|
| 规范领域 | `crates/venue-domain`：Symbol、Decimal 金额、订单、仓位、成交、Instrument、行情规范类型 |
| 六所适配器 | `crates/venue-gateway-*`：签名、原生协议/身份、账户模式、数量单位、规则、行情、私流及签名回读 |
| 适配器契约 | `crates/venue-gateway-api`：精确 LIVE binding、规范能力；旧 probe promotion 不授予新链权限 |
| 策略与指标 | `venue-strategies` 的 Grid/Scalping reducer；`venue-copy` 的纯资本/目标规划；`venue-indicators` 的规范行情指标与图表算法 |
| 账户运行时 | `venue-runtime/src/account/`、`strategy/`、`account_lane.rs`：注册、路由、Actor、生命周期与公平执行 |
| 交易执行 | `venue-execution/src/account_host.rs`：账户锁、风险、WAL、Unknown、dispatch；`account_normalization.rs` 保持用户限价与数量上限 |
| 耐久 I/O | `venue-storage`：JSONL、Actor checkpoint、Control delivery 等存储；不额外拥有 writer |
| Node 组合 | `apps/venue-node/src/production_resident/` 与 `control_loop/`：Grid/Scalping/Copy/手动意图接线、行情、投递及投影 |
| Control | `apps/venue-control`：schema v2 HTTP/SSE、SQLx repository、账户管理、Copy planner/ledger；无物理交易权 |
| UI | `apps/venueflow`：Rust 运维桌面/WASM；`apps/venue-web`：响应式用户 Web 与同源 BFF |
| 冻结兼容 | 根 `src/runtime/grid`、`src/runtime/scalping`、`src/runtime/legacy`：历史工件与行为参考，不是 Node 生产入口 |

依赖方向：domain 不依赖业务；strategy/copy 不依赖 adapter；adapter 不依赖 runtime 或 UI；
Node 组合 runtime 和一个 adapter；Control/UI 只使用所需规范契约。Control 的 Binance 凭证管理探测是
明确的签名只读例外，不能将其扩展为订单客户端。不得复制领域类型、指标算法、归一化或 WAL。

`venue-runtime` 当前公开组合为 `account / account_lane / strategy / shared`；
不要按旧架构图新建不存在的 `runtime/grid、runtime/scalping、runtime/copy` package 子树。
策略组合已经在 Node 中，历史 facade 与纯策略库各自保留现有职责。

## 3. 账户与执行不变量

- 账户键为 `(exchange, trading_account_id)`；后者是稳定内部 UUID，不是交易所产品名、API Key 或 symbol。
- 一个账户一个进程锁和 Execution Lane；同一 symbol 只能归属一个策略实例。同账户多 symbol 的产品验收仍需单独证明。
- Actor 只输出语义意图；Runtime 校验后进入风险/Owner、同一命令 WAL、账户 writer、adapter。
- WAL 状态只有 `Prepared / Submitted / Accepted / Rejected / Unknown`。Host 持久化 Submitted 后提供当前实现的
  一次性 dispatch permit；它不是另一套 writer、journal 或可持久化的授权服务。
- Unknown 冻结新增风险并签名对账，禁止自动重投。ACK、Applied、Accepted 均不等于成交或目标完成。
- 所有交易数量使用 Decimal，OKX 依实时 `ctVal × ctMult × contracts` 转换；不能把基础币数量当张数。
- 当前 10U 约束同时保留单笔门和更严格的账户累计门；已有仓位/增险挂单时不得通过测试继续增险。
  修改该政策不是文档整理的副作用，细则见运行时契约。
- 本地恢复工件固定 `G:\Venue\artifacts`；轮转 5 MiB、单文件 10 MiB、根预算 256 MiB。未决 WAL、
  Unknown 事实和当前 checkpoint 不得删除。原始私流不作永久恢复依据。

## 4. 六所能力的真实边界

六个固定 binary 均经统一账户链。它们复用执行逻辑，不代表所有账户产品、订单族和策略都通用。

| 所 | 当前 adapter 产品边界 | 公共行情/闭合 K 线 |
|---|---|---|
| Binance | Portfolio Margin UM；桌面无凭证行情为 USD-M 公共源 | 盘口、聚合成交及闭合 bar |
| Gate.io | USDT 永续、账户与 Hedge 能力按实时预检 | 盘口、成交；bar 要求 `w=true` |
| Bitget | UTA v3；不支持的条件/策略订单拒绝准入 | 盘口、成交；权威闭合 bar 未闭合 |
| Bybit | V5 UTA2 linear、双向持仓 | 重建完整簿、UUID 成交；bar 要求 `confirm` |
| OKX | V5 SWAP、Long/Short + Cross | `prevSeqId` 接桥、成交；业务 WS bar 要求 `confirm=1` |
| Hyperliquid | 主账户/API Wallet、原生 Net 持仓 | 完整 L2、原生成交；权威闭合 bar 未闭合 |

成交身份 `PublicTradeId` 与连续性 `PublicTradeOrdering` 分开。非连续原生 ID 只有在同代盘口就绪后才由
Node 有界去重并分配 Session cursor；不能伪造成交易所连续序号。FeatureSource 拒绝未知连续性、断层、
冲突或源时间过期；接收时间不能刷新数据有效期。forming bar 不自动变成 closed bar。

Scalping 的 Session-observed Ready 只证明本机输入窗口完整，不证明全市场成交完整，更不授予增险权限。
保护投影、入场确认和退出链尚未闭合时禁止自动入场。5ms 公共空闲 poll 不是端到端响应速度承诺。

## 5. Copy、手动交易与控制面

Copy 的纯规划、确定性 job、跨零 ReduceToZero/Adjust、Node 到统一 WAL 的物理桥、签名成交/仓位回读、
Control ledger/drift 代码及从未领取即过期任务的重新规划已经存在。中间归零不等于最终反向目标完成；
Unknown、过期与跨重启必须绑定原 job/WAL，不能靠 ACK 造 ledger。逐所 leader 事实自动来源与持续产品闭环尚需验收。

手动 `TradeIntent` 已有显式 LIMIT/GTC 选价、同一 Actor replay 和自有手动单撤单桥。
Copy 绑定、影响 Grid desired 的撤单及完整 scope 协同仍有限制，不支持时明确拒绝。
Stop 默认撤自有单且不主动平仓；Flatten 必须以更新签名零持仓证明完成。

Control 使用自有 Tokio HTTP/SSE 实现，不是 Axum/FastAPI 服务。PostgreSQL 保存业务任务、会话、投递与账本，
不是交易 WAL 或第二个 writer。数据库 delivery lease 只控制任务领取，不控制账户 writer 选举。
schema v2、Node runtime JSON 配置 v1、WAL 的版本与产品版本彼此独立。

账户注册/登录、Binance API 密文托管与只读验证的已提交边界见 [账户文档](ACCOUNT_MANAGEMENT.md)。
Node 仍从环境/根 `.env` 读凭证，Control 不自动把密文安装到 Node。API 可访问、Node 在线与交易准入不能混为一谈。

## 6. UI 与技术栈

| 层 | 仓库采用的技术（不是“全为最新版”声明） |
|---|---|
| Rust | 2024 edition、Rust/Cargo 1.98.0；`rust-toolchain.toml` 与 workspace 对齐 |
| 网络 | Tokio、reqwest 0.12、tokio-tungstenite 0.26；既有 blocking transport 逐步等价迁移 |
| 领域/存储 | rust_decimal、serde/serde_json、SQLx 0.8 + PostgreSQL；无第二套 ORM |
| 安全/日志 | secrecy + zeroize；Control 密码 argon2、加密 ring；tracing |
| 桌面/WASM | eframe/egui 0.36.1、egui_tiles 0.17.1、WGPU；native Tokio/reqwest，WASM EventSource；Windows 专用 keyring 3.6.3 |
| 用户 Web | Next.js 16.3.3、React/React DOM 19.2.8、TypeScript 7.0.2；同源 BFF，standalone 发布 |
| Web 验证 | TypeScript 检查、Node 单测、边界扫描、Playwright 1.58.2；当前没有 ESLint/Biome 门禁 |
| Ubuntu 编译 | 本机 Rust + Zig 0.16.0 + cargo-zigbuild 0.23.0，x86-64 GNU/Linux glibc 2.35 基线 |

精确依赖以各 manifest 和 lockfile 为准，不因文档审计批量升级。
当前 `@types/node` 为 26.4.0，而 CI 运行 Node 24；类型包主版本不代表实际 runtime 已升级，
新增 Node API 必须在 Node 24 验证，不能只凭类型检查判断兼容。本轮保留依赖，不把这一差异自动认定为运行故障。
`arc-swap / parking_lot / crossbeam-channel / bytes / bitflags` 等按既有用途复用，不为未来功能预装；
SQLite/rusqlite、Axum、Python/FastAPI、MQTT、Hummingbot、Condor runtime 均不是当前核心运行依赖。
直接 `tungstenite` 是项目冻结例外，不是“该上游库已弃用”；不得新增旧调用点。

Web 已有总览、关系、账户、订单、持仓、成交、对账、ledger/drift 与控制界面；它是独立 DOM 响应式产品，
不是 VenueFlow WASM 的改名。WASM 是内部 canvas 客户端。真实服务器连通、五视口截图、易用性和分段性能仍需部署验收。
BFF 当前使用受控部署会话，不等同于已完成面向公众的多用户自助平台。

VenueFlow 已纳入历史 K 线补载、图表留白/缩放、执行事实视图与手动金额输入；EMA/ADX 算法在 `venue-indicators`，UI 只负责配置与渲染。Windows 的 Venue 登录资料/会话可存系统凭证库，不进入界面普通持久化；交易所 API Key 不在该记录中。Web 下单页面/BFF 闭环仍待完成，不因桌面已有 Trade Dock 而标记 Web 已支持。

技术栈外部支持核对（2026-09-01）：Next.js 16 仍在官方 Active LTS，React 文档当前为 19.2；
Web 默认沿用 CI 的 Node.js 24 LTS，项目最低要求 22.18 不等于任意更高主版本都获验收。
Node 20/23/25 已 EOL，不作为新部署基线。Next 16 的 `next build` 不自动执行 lint；
不能把构建通过写成 ESLint 通过。详见 [Next 支持政策](https://nextjs.org/support-policy)、
[React 版本](https://react.dev/versions)、[Node 发布表](https://nodejs.org/en/about/previous-releases)、
[Next 16 变更](https://nextjs.org/blog/next-16)。本次不是完整漏洞扫描或依赖升级验收。

## 7. 构建、版本与退出条件

本机 Cargo 统一走 `scripts/Invoke-VenueBuild.ps1`，只复用 `G:\Build\Venue\main、slot-1、slot-2`；
禁止旧文档中的 PID target。两个并发构建、150 GiB 总预算和 F/G 空间准入见
[BUILD_POLICY](BUILD_POLICY.md)。文档变更仅做静态检查，不重复业务全量测试。

Ubuntu 默认本机 `Build-VenueUbuntu.ps1` 交叉编译后上传，专用根为 `G:\Build\Venue\ubuntu`，
Cargo 仍复用 slot-2。服务器不承担日常编译；产物有 manifest、源码 commit 与 SHA256，
编译/上传均不等于启动 writer 或完成接管。

产品预览版本以 [VERSION](../VERSION) 为准，版本范围见 [CHANGELOG](CHANGELOG.md)。
旧三所的 `--legacy-v1-handoff` 前驱记录仍是当前启动前置条件，不能因 Stage 7 binary 已删除而绕过。
当前第一批 Binance 策略、接管和真实 UI 验收未完成，保持 alpha；其他五所列第二批，Scalping 暂缓，不宣称“后端已全部完成”。
