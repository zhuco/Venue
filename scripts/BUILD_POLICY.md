# 本机 Rust 构建约束

适用 Windows 本机及其全部 Venue 工作树。保留现有源码、Git、数据库、发布包和历史目录，本阶段不执行自动清理。

## 入口

```powershell
# 只检查路径、容量和预算；不编译，不创建缓存，不删除文件。
./scripts/Invoke-VenueBuild.ps1 -CheckOnly

# 按改动影响面验证；参数使用数组，不传 --target-dir / --config。
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('check','--locked','-p','venue-runtime')
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('test','--locked','-p','venue-runtime')

# 以下专项入口自带锁，直接执行，不再套一层 Invoke-VenueBuild。
./scripts/verify_venue_node_binaries.ps1
```

## 固定缓存与锁

- 只允许 `G:\Build\Venue\main`、`slot-1`、`slot-2`。主工作区默认 main，其他工作树按规范路径的 SHA256 稳定映射到两个槽；槽可能共享，因此必须串行持锁。
- 网关专项固定 slot-1，Node binary 专项固定 slot-2。`-CargoTargetDir` 只接受三个白名单路径，不能指定 PID、时间戳、任意新目录或嵌套 target。
- `.guard` 保存排他文件锁：所有入口共用两个并发许可，每槽另有独占锁。等待最多60秒后报忙；不得抢锁、另开目录或终止其他任务。
- 专项脚本持锁直到构建、测试、二进制扫描及产物复制完成；`finally` 释放锁并还原 CARGO_TARGET_DIR、CARGO_BUILD_TARGET_DIR、CARGO_BUILD_BUILD_DIR、CARGO_INCREMENTAL、RUSTC_WRAPPER、调试配置、TEMP/TMP 和工作目录；环境变量未设置与空字符串须分别恢复。
- 临时文件固定放在 `.tmp/<slot>`；不按会话创建新的构建缓存。测试自身的小型 fixture 可以使用唯一名称，不得把编译产物放入 fixture 目录。

## 空间与编译策略

- 准入时检查 `G:\Build\Venue` 下普通文件合计不超过150 GiB（包含旧目录和临时文件）；跳过重解析点，拒绝受控路径上的重解析点。
- 同时要求物理宿主 F 至少100 GiB空闲、虚拟盘G至少20 GiB空闲。排队后再次检查；超限拒绝新任务，不自动清理。
- 这是入口准入阈值，不是系统硬配额，也不是编译期间的连续监控。已运行的单次构建可能跨过阈值；直接绕过入口的进程不受脚本锁约束。磁盘情况不明时停下报告，不声称绝对限额。
- main 开启增量，隔离槽关闭增量；dev/test 保留行号级调试信息，第三方依赖不生成完整调试符号。release 优化配置不变。需要完整调试信息时明确申请临时调整，而不是复制另一份 target。
- main 使用直接增量编译，guard 内临时设置 `RUSTC_WRAPPER=''`，覆盖 Cargo 全局配置中的外层 wrapper，避免 sccache 拒绝 `CARGO_INCREMENTAL=1`（包括编译器版本探测）。退出或失败后精确恢复原值，不修改全局配置；隔离槽与 hosted CI 保留原 wrapper，`RUSTC_WORKSPACE_WRAPPER` 不变。显式非 sccache 的 `RUSTC_WRAPPER` 或非空 `CARGO_BUILD_RUSTC_WRAPPER` 在准入前拒绝，不静默关闭；主线若需要自定义外层 wrapper，须先明确调整此政策。
- 局部改动只测试受影响包和直接契约；依赖、公共契约或发布前集中全量验证。保持工具链、features 和构建参数稳定，不常规执行 cargo clean。

## 清理边界与生效范围

- 不自动删除旧目录，也不安装后台清理任务。后续清理须先登记精确目录、确认无活动使用并取得对应锁；不能因为目录叫 target/Build 就认定全部可删除。
- 永久保留业务源码、bak、Git、数据库、发布产物、恢复备份、未决WAL和checkpoint。清理G内缓存不保证F上的动态VHDX立即缩小。
- 全局和项目 AGENTS.md 提供会话规则；旧会话开始下一次构建前需重新读取，必要时重启会话。这不是强制安全沙箱。
- GitHub托管CI继续使用既有 RUNNER_TEMP 内的 job-owned target，不使用本机F/G盘阈值；保留同目录锁、两项并发和环境恢复，CI空闲下限2 GiB。

## Linux 六所 Node 发布构建

`package_venue_node_linux_release.sh` 只生成发布目录，不启动 Node、操作账户或部署服务。必须显式指定
`--release-id`、`--output-root`、`--build-root` 和完整 40 位 `--expected-revision`；源码、发布根和构建根互不包含。

- 源码必须是指定 revision 的干净 Git checkout，工具链为 Rust/Cargo 1.98.0。传输源码时保留可验证的 Git revision，不以缺少 `.git` 的压缩包冒充可发布 checkout。
- `--preflight-only` 不创建缓存、锁或发布目录；构建/发布所在文件系统均须至少有 20 GiB 空闲。预检通过不代表已经完成 Linux 编译或服务器接管。
- 同一构建根复用 `cargo-target` 和 `tmp`，持有 `venue-node-build.lock` 后才构建，排队最多 60 秒并重新预检；六所逐个构建，每次一个 Cargo job。不另开时间戳 target，不抢锁或终止其他构建。
- 不自动删除构建缓存。失败仅可删除本次创建且规范路径校验通过的发布暂存目录；版本化 release 不覆盖，只包含六个固定 Node binary、`SHA256SUMS` 和 `manifest.json`。
- 20 GiB 是准入阈值，不是持续磁盘配额。正式发布仍需对应源码的验证基线；构建成功不等于 writer、旧 WAL、真实网关或 UI 验收完成。
- Windows 的 `test_venue_node_linux_release.ps1` 用 Git Bash 和假 Cargo/Rust/flock 检查脚本编排、缓存复用、零写预检、revision 变化及发布竞争；不执行真实 Cargo。真实 Linux 锁和符号链接边界必须在目标主机另外验证，不能用该 fixture 代替。
