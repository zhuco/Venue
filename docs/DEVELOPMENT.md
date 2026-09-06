# 开发、验证与合并

本页统一维护构建缓存、验证、合并、版本化发布和回滚。先从 [CODEMAP](CODEMAP.md) 定位源码，再从 [文档目录](README.md) 进入对应业务契约；各策略的准入与验收范围分别维护。

## 1. 工作区与环境

主工作区 `G:\Venue`，隔离工作树用于开发；只修改当前获准范围，不顺手清理他人的改动。
长期文档集中在 `docs/`，源码按 `apps/`、`crates/`、根兼容 `src/` 分工；`bak/` 已退出项目维护范围，用户已授权删除且不备份。部署主密钥、数据库地址和旧 Node 凭证只从规定环境/根 `.env` 注入；用户 Binance API Key/Secret 由 Control 加密后存 PostgreSQL，任何明文都不写日志、配置或工件。

Rust/Cargo 1.98.0，Web 默认 Node.js 24 + npm lockfile。数据库为 PostgreSQL + SQLx，
集成测试使用独立测试数据库/随机 schema；不要把主线 `.env` 的生产连接直接当作测试库。
数据库真实地址配置在工作区环境中，不固化到示例、版本文件或提交。

## 2. 按影响面验证

| 变更 | 必要验证 |
|---|---|
| 文档/注释/产品预览版本 | 引用路径、命令与 manifest 一致性、`git diff --check`、仓库卫生 |
| 单一 Rust 模块 | 受影响 package 和直接契约；不重复全 workspace |
| 交易安全 | 新链覆盖命令幂等、账户串行、`ReconcileRequired` 与 adapter 故障路径；冻结旧链维护另覆盖 risk/WAL/Unknown/恢复 |
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

## 3. 构建入口

所有 Cargo 操作统一经过 [构建规则与固定缓存](#build-policy)；Ubuntu Control 包使用同节的 `Build-VenueUbuntu.ps1 -Component Control`。

## 4. 本地应用

- [Node README](NODE.md) 只说明冻结旧 CLI、runtime JSON 和旧三家前驱记录要求。
- [账户管理](ACCOUNT_MANAGEMENT.md) 说明 Control/桌面启动环境；运行前先受控 build，
  再从 guard 选择的固定缓存启动已构建 binary，不用长期 `cargo run` 占用构建槽。
- [Web README](WEB.md) 说明 BFF 会话、同源 HTTPS、npm 验证与五视口测试。
- 新 KOL 链只由 `venue-executor-binance` 执行：它要求精确 `VENUE_EXECUTOR_MODE=LIVE`、`VENUE_EXECUTOR_DATABASE_URL` 的 PostgreSQL URL 和现有 credential master key，取得 advisory singleton 后启动私有投影、Grid 与持续命令发现；未决命令按账户只查不重发，启用基线在独立低优先级任务中核验，单个失败账户不阻止进程启动。缺配置、权限、规则或基线失败一律 fail-closed；没有 mock/dry-run/testnet 运行模式。SIGINT/SIGTERM 停止循环、关闭私流并释放锁。冻结旧 Node 维护仍遵守原 writer/WAL 契约。文档更新不触发实盘测试。

UI 完成标准包括移动/桌面布局截图、空/错误/离线状态、作用域与确认交互、网关联通和分段延迟。
本地 fixture/BFF 性能报告不能代替 Executor→Binance 的实际延迟；冻结旧 Node 的历史结果也不能代替新链验收。

## 5. 旧代码与技术选型

先查停用清单再改调用点。已删除 CLI 不恢复；冻结兼容只修正必需恢复问题，不增加新策略或新授权层。
旧持久化字段不能为符合新类型而回写。只有生产调用点清零、行为等价、恢复兼容和实盘接管证据齐全，
才可删除被替代的执行壳。删除迁移来源目录不改变当前执行代码的安全约束。

依赖审计先查 workspace/lockfile，再看官方兼容与支持政策；“版本较旧”“本项目不再新增调用”
与“上游已弃用”分别记录。不批量更新依赖来凑最新版，不预装新的 ORM/HTTP/runtime。
当前 Web 没有 ESLint/Biome；Next build、typecheck 和边界扫描不能被写成 lint 全通过。

## 6. 合并与版本

`master` 是集成主线，独立任务使用 `codex/*` 分支和隔离工作树。一个工作树只处理一个明确任务，避免多个任务同时改写相同文件；合并前检查未提交改动和其他工作树占用。分支是提交指针，工作树是实际文件目录，版本标签固定发布提交；Git 推送不会编译或更新运行中的 UI/服务。

1. 核对主线分支/HEAD、工作树 diff、暂存区和未跟踪文件，明确本批路径。
2. 文档可安全整合，但 UI/源码/lockfile 的其他未提交工作原样保留；重叠不明时停止合并。
3. 在隔离树验证并提交本批改动；建立主线回退分支，优先 `merge --ff-only`，不做 hard reset。
4. 必要时仅暂存精确重叠文件，按具体 stash OID 恢复并核对内容，不全工作区盲目 stash/pop。
5. 合并后核对提交及原本地文件哈希；不把未验证的他人改动算进当前版本。
6. 产品号在 [VERSION](../VERSION)，变更范围在 [CHANGELOG](CHANGELOG.md)；本地 tag 指向准确提交，
   不自动 push、发布安装包或改变服务器服务。

<a id="build-policy"></a>

## 7. 本机与 Ubuntu 构建规则

适用 Windows 本机及其全部 Venue 工作树。保留现有源码、Git、数据库、发布包和历史目录，本阶段不执行自动清理。

### 入口

```powershell
# 只检查路径、容量和预算；不编译，不创建缓存，不删除文件。
./scripts/Invoke-VenueBuild.ps1 -CheckOnly

# 按改动影响面验证；参数使用数组，不传 --target-dir / --config。
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('check','--locked','-p','venue-runtime')
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('test','--locked','-p','venue-runtime')

# 以下专项入口自带锁，直接执行，不再套一层 Invoke-VenueBuild。
./scripts/verify_venue_node_binaries.ps1
```

### 固定缓存与锁

- 只允许 `G:\Build\Venue\main`、`slot-1`、`slot-2`。主工作区默认 main，其他工作树按规范路径的 SHA256 稳定映射到两个槽；槽可能共享，因此必须串行持锁。
- 网关专项固定 slot-1，Node binary 专项固定 slot-2。`-CargoTargetDir` 只接受三个白名单路径，不能指定 PID、时间戳、任意新目录或嵌套 target。
- `.guard` 保存排他文件锁：所有入口共用两个并发许可，每槽另有独占锁。等待最多60秒后报忙；不得抢锁、另开目录或终止其他任务。
- 专项脚本持锁直到构建、测试、二进制扫描及产物复制完成；`finally` 释放锁并还原 CARGO_TARGET_DIR、CARGO_BUILD_TARGET_DIR、CARGO_BUILD_BUILD_DIR、CARGO_INCREMENTAL、RUSTC_WRAPPER、调试配置、TEMP/TMP 和工作目录；环境变量未设置与空字符串须分别恢复。
- 临时文件固定放在 `.tmp/<slot>`；不按会话创建新的构建缓存。测试自身的小型 fixture 可以使用唯一名称，不得把编译产物放入 fixture 目录。

### 空间与编译策略

- 准入时检查 `G:\Build\Venue` 下普通文件合计不超过150 GiB（包含旧目录和临时文件）；跳过重解析点，拒绝受控路径上的重解析点。
- 同时要求物理宿主 F 至少100 GiB空闲、虚拟盘G至少20 GiB空闲。排队后再次检查；超限拒绝新任务，不自动清理。
- 这是入口准入阈值，不是系统硬配额，也不是编译期间的连续监控。已运行的单次构建可能跨过阈值；直接绕过入口的进程不受脚本锁约束。磁盘情况不明时停下报告，不声称绝对限额。
- main 开启增量，隔离槽关闭增量；dev/test 保留行号级调试信息，第三方依赖不生成完整调试符号。release 优化配置不变。需要完整调试信息时明确申请临时调整，而不是复制另一份 target。
- main 使用直接增量编译，guard 内临时设置 `RUSTC_WRAPPER=''`，覆盖 Cargo 全局配置中的外层 wrapper，避免 sccache 拒绝 `CARGO_INCREMENTAL=1`（包括编译器版本探测）。退出或失败后精确恢复原值，不修改全局配置；隔离槽与 hosted CI 保留原 wrapper，`RUSTC_WORKSPACE_WRAPPER` 不变。显式非 sccache 的 `RUSTC_WRAPPER` 或非空 `CARGO_BUILD_RUSTC_WRAPPER` 在准入前拒绝，不静默关闭；主线若需要自定义外层 wrapper，须先明确调整此政策。
- 局部改动只测试受影响包和直接契约；依赖、公共契约或发布前集中全量验证。保持工具链、features 和构建参数稳定，不常规执行 cargo clean。

### 清理边界与生效范围

- 不自动删除旧目录，也不安装后台清理任务。后续清理须先登记精确目录、确认无活动使用并取得对应锁；不能因为目录叫 target/Build 就认定全部可删除。
- 构建缓存清理不得删除业务源码、Git、数据库、发布产物、恢复备份、未决WAL和checkpoint。项目目录整理须另行明确授权；清理G内缓存不保证F上的动态VHDX立即缩小。
- 全局和项目 AGENTS.md 提供会话规则；旧会话开始下一次构建前需重新读取，必要时重启会话。这不是强制安全沙箱。
- GitHub托管CI继续使用既有 RUNNER_TEMP 内的 job-owned target，不使用本机F/G盘阈值；保留同目录锁、两项并发和环境恢复，CI空闲下限2 GiB。

### Ubuntu Control 发布与冻结 Node 编译

`45.77.253.180` 只接收本机编译好的产物并执行运行核验，不承担日常 Cargo 编译。Windows 本机使用既有
Rust/Cargo 1.98.0、cargo-zigbuild 0.23.0、Zig 0.16.0 和 `x86_64-unknown-linux-gnu` 标准库，不依赖 WSL/Docker。
交叉目标固定为 `x86_64-unknown-linux-gnu.2.35`，即 x86-64 GNU/Linux、glibc 2.35 基线；上传后仍需检查服务器动态库兼容性。
版本后缀和缓存环境使用 [cargo-zigbuild 官方契约](https://github.com/rust-cross/cargo-zigbuild#specify-glibc-version)。

```powershell
# SourceRoot 必须为指定 commit 的干净 checkout，不包含工作区未提交的开发改动。
./scripts/Build-VenueUbuntu.ps1 -SourceRoot G:\Build\Venue\ubuntu\source -ExpectedRevision <完整40位commit> -ReleaseId <版本号> -CheckOnly
# 预检后去掉 -CheckOnly 编译六所；脚本不自动上传或启动服务。
./scripts/test_venue_ubuntu_build.ps1

# KOL MVP Control 发布入口：打包 Control Server、Binance Executor 与带单授权管理工具。
./scripts/Build-VenueUbuntu.ps1 -SourceRoot G:\Build\Venue\ubuntu\source -ExpectedRevision <完整40位commit> -ReleaseId <版本号> -Component Control -CheckOnly
```

- 专用根为 `G:\Build\Venue\ubuntu`：`source` 可存固定 revision 的独立 checkout；Nodes release 包含六个冻结 Node binary。alpha.28 的 Control release 包含 `venue-control-server`、`venue-executor-binance`、`venue-leader-bot-admin`、`venue-strategy-admin`，实际清单以所选干净 revision 的脚本与 manifest 为准。两类包均另含 SHA256SUMS 与 manifest。旧 `venue-copy-worker` 不进入 KOL 发布包；离线发布和回滚清单见[第 8 节](#executor-release)。工具缓存为 `zig-cache/zig-local-cache/zigbuild-cache`。源码只用干净 Git clone/bundle，不复制 `.env`、账户工件或未提交文件；已有 checkout 不自动 reset。
- Cargo 仍使用既有 `slot-2` 锁和两个全局并发许可，其自动目标子目录 `slot-2/x86_64-unknown-linux-gnu/release` 不是另设 target root。六所按顺序、每次两个 Cargo jobs；全部目录计入 150 GiB 总预算。不清理 Windows 缓存，不安装工具、不改全局配置。
- `-CheckOnly` 不新建输出、锁或缓存；正式构建前后均校验 HEAD 和干净状态，manifest 另记录构建入口/辅助/guard 脚本哈希，运行期间脚本变动则拒绝发布。源码 checkout 必须由构建独占，其他任务不得在构建期间同步或编辑；前后 Git 检查不是文件系统只读沙箱。输出要求 ELF64/x86-64，拒绝误复制 Windows exe；目录原子转为新 release，已有 release 不覆盖。失败保留缓存和本次 `.stage.*` 目录，不把不完整目录当发布包。
- 入口持锁覆盖构建、ELF/哈希核验和复制，finally 还原 Cargo/Zig 环境并释放锁。版本化产物仅表示编译完成；KOL MVP 仍须完成 API/双向持仓验证、Executor/Binance、UI、容量和真实 Canary 验收。冻结旧链的签名 preflight 与 writer/WAL 接管另行处理。
- 脚本专项只跑静态/离线 fixture 和受影响编译，不因构建入口修改重跑全业务测试；`test_venue_ubuntu_build.ps1` 的小型验证工件保留在专用根，不执行交易或服务操作。

### Linux 主机本地打包（备用，不在弱服务器日常使用）

`package_venue_node_linux_release.sh` 只生成发布目录，不启动 Node、操作账户或部署服务。必须显式指定
`--release-id`、`--output-root`、`--build-root` 和完整 40 位 `--expected-revision`；源码、发布根和构建根互不包含。

- 源码必须是指定 revision 的干净 Git checkout，工具链为 Rust/Cargo 1.98.0。传输源码时保留可验证的 Git revision，不以缺少 `.git` 的压缩包冒充可发布 checkout。
- `--preflight-only` 不创建缓存、锁或发布目录；构建/发布所在文件系统均须至少有 20 GiB 空闲。预检通过不代表已经完成 Linux 编译或服务器接管。
- 同一构建根复用 `cargo-target` 和 `tmp`，持有 `venue-node-build.lock` 后才构建，排队最多 60 秒并重新预检；六所逐个构建，每次一个 Cargo job。不另开时间戳 target，不抢锁或终止其他构建。
- 不自动删除构建缓存。失败仅可删除本次创建且规范路径校验通过的发布暂存目录；版本化 release 不覆盖，只包含六个固定 Node binary、`SHA256SUMS` 和 `manifest.json`。
- 20 GiB 是准入阈值，不是持续磁盘配额。正式发布仍需对应源码的验证基线；构建成功不等于 writer、旧 WAL、真实网关或 UI 验收完成。
- Windows 的 `test_venue_node_linux_release.ps1` 用 Git Bash 和假 Cargo/Rust/flock 检查脚本编排、缓存复用、零写预检、revision 变化及发布竞争；不执行真实 Cargo。真实 Linux 锁和符号链接边界必须在目标主机另外验证，不能用该 fixture 代替。

<a id="executor-release"></a>

## 8. Control / Executor 发布与回滚

本清单只覆盖离线构建、fixture 和可恢复的部署准备；它不授权连接 Binance、读取用户密钥或下单。真实 Canary 仍须遵守 [KOL MVP 契约](KOL_COPY_MVP.md) 的单独授权与隔离账户条件。

### 发布前离线门

1. 在干净、指定 revision 的 checkout 中运行 `scripts/Invoke-KolCanaryDrill.ps1 -OfflineFixture`。脚本拒绝进程内 `BINANCE_API_KEY`、`BINANCE_API_SECRET`，只通过受控 Cargo 入口运行 5 KOL / 200 follower fixture。
2. 若配置了独立测试 PostgreSQL，运行 `scripts/verify_postgres_integration.ps1`；它必须不出现 `SKIP:`。该门覆盖源成交去重、稳定命令 ID、owner-scoped 密文、重启 readback、超时栅栏与拒单 fixture。
3. 按第 2 节建立与发布源码一致的全量验证基线后，使用 `scripts/Build-VenueUbuntu.ps1 -Component Control` 打包；核对所选 revision 的二进制清单（见第 7 节），不得含 `venue-copy-worker`。Executor 只接受 `VENUE_EXECUTOR_MODE=LIVE`、PostgreSQL `VENUE_EXECUTOR_DATABASE_URL` 和既有 credential master key；不提供 mock、dry-run 或 testnet 配置。
4. Grid 发布必须确认 `0021`–`0024` 已按序落库；`0024` 的输入 desired 摘要、唯一前驱和实例批次尾列缺一不可。记录 release hash、控制服务和 Executor 二进制哈希、迁移版本、脱敏账户数量及待对账计数。不要记录连接串、API key、secret、listen key 或原始私流帧。

### 真实部署与回滚边界

当前 Control 升级须由 `schema.rs` 安装并验证截至 `0040` 的迁移（包括 `0028`/`0029` 授权与挂单映射、`0030`–`0033` 托管和数量设置、`0034` 多机器人目录、`0035` 独立执行、`0036` 策略网格、`0037` 支撑分批策略、`0038` 跟单授权、`0039` 马丁止损和 `0040` 市价/止损同步），同步核验 [授权及数据库角色](LEADER_ORDER_MIRROR.md)。旧 Canary 脚本的成交模型 fixture 不能代替新挂单模型的 5/200 容量与延迟验收。数据库含真实 GTC 镜像后，不得直接回退到每次启动重跑旧迁移的 Control binary：旧 `0020` 会重解释 GTC，须先单独核验回滚版本的迁移兼容性。

管理员可先运行包内 `venue-leader-bot-admin migrate`，然后以迁移所有者向运行角色授予实际需要的表与序列权限；授权表和授权审计表只授予运行角色 SELECT。生产 Control 设置 `VENUE_CONTROL_RUNTIME_DATABASE_URL`，Executor 使用其独立 `VENUE_EXECUTOR_DATABASE_URL`，二者不得拥有授权表或持有能改回管理员身份的角色成员关系。迁移 URL 不进入启动参数或日志。

上线前由获授权操作者确认目标账户未被旧 Node/Copy writer 管理，且旧 `Prepared/Submitted/Unknown`、仓位和订单均已按签名事实收敛。先启动并确认 PostgreSQL advisory lock 仅由一个 `venue-executor-binance` 持有；它并行组装私有投影、Grid、人工带单与持续命令调度；账户栅栏保证各账户完成自身启用基线及未终态 readback 前不能继续增险。锁、数据库、凭证、规则或基线失败均不得发送新订单；私流断线/过期只经有界退避与签名 REST 补读恢复，原始帧和 listenKey 不记录。

回滚只暂停关系并以 SIGINT/SIGTERM 停止新 Executor；它关闭私流并释放锁。不得删除 PostgreSQL 命令、源成交、目标、凭证或旧恢复工件。`Sending`、`Accepted`、`ReconcileRequired` 先由同一 `clientOrderId` 查单和签名仓位收敛；精确回读遵守数据库中 500 ms 起、8 s 封顶的耐久退避，未到期仍保留账户栅栏。未终态时不得恢复旧 writer 或盲目重新发送。确认所有账户无未决命令且旧链仍未接管后，才允许按单独变更恢复此前部署。
