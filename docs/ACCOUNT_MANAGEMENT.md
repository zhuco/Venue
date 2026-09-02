# Binance KOL MVP 账户、邀请与 API 管理

入口：[MVP 契约](KOL_COPY_MVP.md) / [README](../README.md) / [开发指南](DEVELOPMENT.md)。本页描述已提交的 Control 账户能力及下一步公开用户接入边界；尚未实现的邀请、Web会话和Executor凭证读取不得描述为已上线。

## 产品边界

- 行情服务器选择独立于登录；当前 UI 仅提供 Binance。没有账号或 Control 不可用时，原生端仍可使用公共行情。
- Venue 用户账号与真实交易所账户分开。注册/登录后才能加载、添加、验证、选择和删除自己的 API 绑定。
- 新用户从 `/join/<invite_code>` 注册时由服务端在同一事务绑定唯一 KOL；邀请绑定不自动启用交易。初期全站最多 5 个启用 KOL、200 个启用跟单账户；KOL 由管理员授予，普通用户不能自行升级。
- “添加 API”绑定用户已在币安创建的 Key，不代用户在交易所创建密钥。“删除绑定”只删除 Venue 中的加密凭证，不撤销交易所 Key。
- API 验证只发签名 GET：读取权限、真实账户身份、Portfolio Margin 状态、UM 交易权限、双向持仓、持仓、普通挂单及 Algo 挂单；不下单、不撤单、不切换账户模式。当前仅接受 Binance Portfolio Margin UM，不声称支持普通合约账户。
- 同一真实账户的多把 Key 复用同一稳定 `trading_account_id`，不同用户不能认领同一真实账户。系统账号或 Key 数量不是 writer 数量。
- 用户只能管理自己的 API 和跟单设置；KOL 只能管理自己的公开页面和主账户终端，不能查看跟随者 API 明文或账户明细。
- 启用跟单还要求明确配置资金、倍率与风险上限并再次确认；选择或验证账户本身不产生订单。

## 进程与状态

Control 负责认证、邀请归属、KOL 页面、绑定管理、只读验证和查询投影。目标 Binance Executor 是一个多账户进程：最多 5 个 KOL 保持成交私流，跟随账户只使用进程内顺序队列、按需查单和周期签名对账，不为每个账户启动 Node、Actor 或本地 WAL。

UI 分开显示登录状态、邀请归属、API 验证、跟单启用和 Executor 最近报告。`api_reachable` 只说明验证期内签名读取成功，不表示 Executor 在线或跟单已启用。公共行情可由桌面直连 Binance；私有仓位、活动委托、成交和资产必须由服务端 Binance gateway 解析，再经唯一 Executor/Control 返回用户作用域投影，桌面不得持有 API Secret 或自行解析私流。无新鲜私有投影时必须显示未知/未连接，旧 Node 当前快照不得冒充完整委托或仓位历史。

用户发起绑定、首次启用或更换 API 时，交互式验证结果有效期为 5 分钟；过期后必须复验才能完成该次变更。已经 Active 的长期跟单不会每 5 分钟要求用户手动验证，改由 Executor 的持续认证结果、账户身份、持仓模式和周期签名对账维持；任一关键事实失败即自动暂停该账户。复验开始即撤销旧结果，并通过 revision 拒绝较旧并发探测覆盖较新结果。账户列表每 30 秒刷新。会话有效期 12 小时，退出登录立即撤销服务端会话并关闭关联事件流。

冻结旧 Node 仍受 `GRID_RUNTIME_REFACTOR.md` 约束，但不再作为新用户接入路径。新 Executor 只读取明确启用且未分配给旧 Node 的账户；Control 接收配置或命令不等于交易所已成交。

## 凭证边界

本次获准的 UI 绑定对原“凭证仅来自环境”的限制增加一个窄例外：

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

Control 使用现有 PostgreSQL，通过 `DATABASE_URL` 指定连接。以下命令只启动当前已提交组件，不代表 KOL MVP 已完成；设置 `VENUE_ACCOUNT_MASTER_KEY` 后运行：

```powershell
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('build','--locked','-p','venue-control','--bin','venue-control-server')
./scripts/Invoke-VenueBuild.ps1 -CargoArguments @('build','--locked','-p','venueflow','--bin','venueflow')
```

编译结束后，从 guard 实际选择的固定缓存 `debug` 目录分别启动 `venue-control-server.exe` 和 `venueflow.exe`；
主工作区默认为 `G:\Build\Venue\main\debug`。不要以长期 `cargo run` 占用构建锁。本次文档核对不自动启动任何服务。

默认监听和连接 `127.0.0.1:39180`。当前 Control 幂等安装 `0001`–`0017` migrations；0017 已固定 KOL、邀请、永久唯一用户归属、关系容量槽、源成交/目标和轻量命令账本，HTTP repository 与 Executor 消费仍按 `KOL_COPY_MVP.md` 的 P1–P3 实现。`VENUE_CONTROL_BIND` 继续只允许 loopback；公网浏览器经同源 HTTPS BFF 访问。

不要把数据库 URL、主密钥或实际 API Key 粘贴到诊断输出。主密钥丢失无法恢复绑定密文；轮换必须另行设计迁移，不可直接换值后假定旧绑定仍可用。

## 验收

- `accounts/crypto`：随机盐、正确/错误密码、随机 nonce、篡改/用户/Key 替换拒绝。
- `accounts/credentials/tests`：真实 PostgreSQL 注册、登录、重启加载、归属隔离、稳定真实账户、复验失败与并发结果栅栏、删除风险门、命令/退出/删除并发保护。
- `http/account_tests`：真实 HTTP + PostgreSQL 会话、JSON 约束、匿名/跨用户投影与命令拒绝、SSE 数据过滤及退出后关闭。
- Binance `credential_probe`：完整签名请求面、权限/双向模式不匹配、任一面失败/不完整不通过、普通/Algo/持仓任一非零不允许安全删除。
- 邀请/KOL：服务端邀请码解析、注册事务绑定唯一 KOL、任何后续换绑拒绝、页面 revision、XSS 与跨 KOL 修改拒绝。
- Web UI：真实注册/登录 Cookie、API 掩码、验证与 Executor 状态分离、跟单显式启用/暂停；浏览器响应和构建产物无 API 明文。

数据库测试使用 `VENUE_CONTROL_TEST_DATABASE_URL`，并设置 `VENUE_CONTROL_POSTGRES_REQUIRED=1`，避免未配置数据库时的跳过被误认为验收。每个测试创建独立随机 schema；不接真实交易所或使用实盘凭证。
