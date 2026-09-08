# Binance KOL MVP 账户、邀请与 API 管理

入口：[MVP 契约](KOL_COPY_MVP.md) / [README](../README.md) / [开发指南](DEVELOPMENT.md)。本页说明当前 Control 账户、邀请、会话和凭证能力；实际部署与实盘验收须核验对应环境，不能由页面或保存的验证状态推断。

## 产品边界

- 行情服务器选择独立于登录；当前 UI 仅提供 Binance。没有账号或 Control 不可用时，原生端仍可使用公共行情。
- Venue 用户账号与真实交易所账户分开。注册/登录后才能加载、添加、验证、选择和删除自己的 API 绑定。
- 新用户从 `/join/<invite_code>` 注册时由服务端在同一事务绑定唯一 KOL；邀请绑定不自动启用交易。初期全站最多 5 个启用 KOL、200 个启用跟单账户；KOL 由管理员授予，普通用户不能自行升级。
- “添加 API”绑定用户已在币安创建的 Key，不代用户在交易所创建密钥。“删除绑定”只删除 Venue 中的加密凭证，不撤销交易所 Key。
- API 验证只发签名 GET：读取权限、真实账户身份、Portfolio Margin 状态与 USD 权益/可用保证金、UM 交易权限、双向持仓、持仓、普通挂单及 Algo 挂单；不下单、不撤单、不切换账户模式。本页用户 API 流程仅接受 Binance Portfolio Margin UM，不声称支持普通合约账户。
- 同一真实账户的多把 Key 复用同一稳定 `trading_account_id`，不同用户不能认领同一真实账户。系统账号或 Key 数量不是 writer 数量。
- 用户保存自己的 API 时选择定比或定额；KOL 通过专用托管接口添加经委托的 API 时使用同一选择，并自动建立不可变 KOL 归属。KOL 可查看掩码与余额验证结果，并在密码复验和零风险门通过后删除 Venue 托管绑定。托管边界见 [KOL MVP](KOL_COPY_MVP.md)，不授予读取已保存密钥的能力，也不删除交易所子账户。
- 保存 API 即完成跟单授权。默认定比 1 倍；验证成功后以账户权益作为分配资金、5 倍权益作为总名义上限自动申请激活。激活闸门通过前不产生订单。

非 Binance 独立策略凭证与账户准入使用 [多交易所管理员入口](MULTI_VENUE_EXECUTOR.md#操作入口)，不套用本页 Binance 用户绑定流程。

## 进程与状态

Control 负责认证、邀请归属、KOL 页面、绑定管理、只读验证和查询投影。Binance Executor 是一个多账户进程，按活动账户需求维护有界私有投影与签名恢复，复用进程内顺序队列。新关系同步普通限价单、已确认原生身份的市价单和 `STOP_MARKET` 止损单；不为每个账户启动 Node、Actor 或本地 WAL。

UI 分开显示登录状态、邀请归属、API 验证、跟单启用和 Executor 最近报告。`api_reachable` 只说明验证期内签名读取成功，不表示 Executor 在线或跟单已启用。公共行情默认经同一 HTTPS 主机的精确只读 Binance 反代获取；私有仓位、活动委托、成交和资产必须由服务端 Binance gateway 解析，再经唯一 Executor/Control 返回用户作用域投影，桌面不得持有 API Secret 或自行解析私流。无新鲜私有投影时必须显示未知/未连接，旧 Node 当前快照不得冒充完整委托或仓位历史。

已通过的 API 验证结果保存在 PostgreSQL，不因桌面重启或 UI 计时器自动失效；Executor 的新鲜签名账户事实仍独立决定是否可交易。主动复验开始即撤销旧结果，并通过 revision 拒绝较旧并发探测覆盖较新结果。账户列表每 30 秒从 Control 刷新。

Windows 将有效登录会话保存至系统凭据库，启动后向服务端验证身份；API 验证结果及会话内的执行账户选择从数据库恢复，不用本地缓存伪造授权。会话有效期 12 小时，主动退出或过期后需重新登录；执行账户选择目前按会话保存，新登录会话需重新选择。API 表格按备注/掩码、交易所、账户类型、账户身份、验证、选择和删除排列。顶部选择器和底部状态栏使用同一选中凭证备注；账户连接状态统一读取 Executor 私有投影，不回退旧 Node。

冻结旧 Node 仍受 `GRID_RUNTIME_REFACTOR.md` 约束，但不再作为新用户接入路径。新 Executor 只读取明确启用且未分配给旧 Node 的账户；Control 接收配置或命令不等于交易所已成交。

## 凭证边界

用户绑定与部署凭证分别遵守以下边界：

- 交易所 API Key 仅在 UI 表单和请求期间持有，使用掩码、清理编辑历史，不写入界面配置、系统登录凭证库、日志、URL、错误信息或 artifacts。
- Windows 桌面可通过 `account_center/vault.rs` 保存 Venue 登录资料及会话至系统凭证库，按 Control endpoint 隔离，过期会话丢弃；记住密码开关控制登录资料保存。非 Windows 无本地凭证库回退。记录不经过 eframe 普通配置，不包含交易所 API Key 或账户投影。
- `keyring =3.6.3` 仅启用 Windows native 后端，限定在 VenueFlow；现有 `secrecy/zeroize` 只负责内存清理，`ring` 只提供密码学原语，均不能替代系统凭证库。复用 lockfile 依赖，专项以 mock store 验证保存/恢复/退出/过期，不访问用户真实凭证库。
- 仅向显式配置的 HTTPS 或本机 HTTP 地址提交。更改 Control 地址时，先丢弃旧会话及异步回复；匿名行情不受影响。
- Control 的账户管理模块使用 AES-256-GCM 随机 nonce 加密凭证，认证附加数据绑定用户和凭证 ID；PostgreSQL 仅保存密文、Key 指纹、掩码和非秘密验证结果。
- 加密主密钥只来自 `VENUE_ACCOUNT_MASTER_KEY` 进程环境变量，为 Base64 编码的 32 字节随机值。缺失、格式错误或解密认证失败均拒绝；不得把主密钥存入数据库、TOML、日志或仓库。重启必须使用同一主密钥，应由运维在仓库外安全备份。
- 当前旧 Node 的环境凭证方式仅供冻结旧账户。KOL MVP 的 Executor 从 PostgreSQL 读取已启用账户的密文并使用部署主密钥短时解密，不为每个账户生成 `.env`。Control 仍只做签名只读探测，不发送物理订单。
- 密码接受 8–128 个字符并使用 Argon2id（19 MiB、2 次迭代、并行度 1、随机盐）；会话使用随机 256-bit token，数据库仅存 SHA-256 摘要。密码计算并发、注册/登录/验证频率、绑定数与会话数均有上限。
- 新增 `argon2` 是为密码哈希，现有 SHA/HMAC 不能替代慢密码哈希；直接复用锁文件中已有 `ring` 实现认证加密和随机数，不引入第二套 ORM、HTTP 或密码体系。

所有账户快照、命令和 SSE 按服务端会话和真实账户归属校验，不信任客户端传入的 user/KOL/account ID。未登录只可读取启用的 KOL 公开页。Executor 内部接口使用独立服务身份，普通用户会话不能调用。

删除需要再次验证登录密码。曾验证绑定的账户，还需本次完整签名零持仓、零挂单、无运行节点/策略托管，并且新链无 `Accepted/ReconcileRequired`、冻结旧链无 `Accepted/Unknown` 命令；证据不足拒绝删除。凭证/会话锁及真实账户锁将删除、切换和命令入队串行化，避免同账户不同 Key 绕过检查。删除密文不删除真实账户身份和历史业务记录；数据库备份中的旧密文须按运维备份策略处理。

## 启动

Control 使用现有 PostgreSQL，通过 `DATABASE_URL` 指定连接。本地启动前按 [发布指南](DEVELOPMENT.md#executor-release) 配置数据库角色与 `VENUE_ACCOUNT_MASTER_KEY`，先构建：

```powershell
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('build','--locked','-p','venue-control','--bin','venue-control-server')
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('build','--locked','-p','venueflow','--bin','venueflow')
```

编译结束后，从 guard 实际选择的固定缓存 `debug` 目录分别启动 `venue-control-server.exe` 和 `venueflow.exe`；
主工作区默认为 `G:\Build\Venue\main\debug`。不要以长期 `cargo run` 占用构建锁。启动与部署按当前任务的授权范围执行。

Control 默认监听 `127.0.0.1:39180`，桌面默认连接 `https://clawdbotweb.site`，通过 Caddy 转发到服务器本机；地址设置与旧默认迁移见 [UI 说明](../apps/ui/README.md)。当前版本化迁移及运行角色要求统一见 [发布指南](DEVELOPMENT.md#executor-release)。生产 Control 迁移后使用 `VENUE_CONTROL_RUNTIME_DATABASE_URL` 的受限角色；Executor 使用独立 `VENUE_EXECUTOR_DATABASE_URL`，完整配置及回滚见 [发布指南](DEVELOPMENT.md#executor-release)。`VENUE_CONTROL_BIND` 继续只允许 loopback；公网浏览器经同源 HTTPS BFF 访问。

不要把数据库 URL、主密钥或实际 API Key 粘贴到诊断输出。主密钥丢失无法恢复绑定密文；轮换必须另行设计迁移，不可直接换值后假定旧绑定仍可用。

## 验收

- `accounts/crypto`：随机盐、正确/错误密码、随机 nonce、篡改/用户/Key 替换拒绝。
- `accounts/credentials/tests`：真实 PostgreSQL 注册、登录、重启加载、归属隔离、稳定真实账户、复验失败与并发结果栅栏、删除风险门、命令/退出/删除并发保护。
- `http/account_tests`：真实 HTTP + PostgreSQL 会话、JSON 约束、匿名/跨用户投影与命令拒绝、SSE 数据过滤及退出后关闭。
- Binance `credential_probe`：完整签名请求面、权限/双向模式不匹配、任一面失败/不完整不通过、普通/Algo/持仓任一非零不允许安全删除。
- 邀请/KOL：服务端邀请码解析、注册事务绑定唯一 KOL、任何后续换绑拒绝、页面 revision、XSS 与跨 KOL 修改拒绝。
- Web UI：真实注册/登录 Cookie、API 掩码、保存 API 时选择定比或定额授权；默认定比 1 倍，验证成功自动申请激活，浏览器响应和构建产物无 API 明文。

数据库测试使用 `VENUE_CONTROL_TEST_DATABASE_URL`，并设置 `VENUE_CONTROL_POSTGRES_REQUIRED=1`，避免未配置数据库时的跳过被误认为验收。每个测试创建独立随机 schema；不接真实交易所或使用实盘凭证。
