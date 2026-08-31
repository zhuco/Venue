# 账户与 API 管理

## 产品边界

- 行情服务器选择独立于登录；当前 UI 仅提供 Binance。没有账号或 Control 不可用时，原生端仍可使用公共行情。
- Venue 用户账号与真实交易所账户分开。注册/登录后才能加载、添加、验证、选择和删除自己的 API 绑定。
- “添加 API”绑定用户已在币安创建的 Key，不代用户在交易所创建密钥。“删除绑定”只删除 Venue 中的加密凭证，不撤销交易所 Key。
- API 验证只发签名 GET：读取权限、真实账户身份、Portfolio Margin 状态、UM 交易权限、双向持仓、持仓、普通挂单及 Algo 挂单；不下单、不撤单、不切换账户模式。当前仅接受 Binance Portfolio Margin UM，不声称支持普通合约账户。
- 同一真实账户的多把 Key 复用同一稳定 `trading_account_id`，不同用户不能认领同一真实账户。系统账号或 Key 数量不是 writer 数量。
- 选择执行账户只切换当前登录会话的查询/命令目标，清除旧选价与待确认动作，不启动、停止或迁移策略。

## 进程与状态

Control 负责认证、绑定管理、查询投影和语义命令。每个运行交易账户对应一个 Node；策略运行时、网关、风险、同一本交易 WAL 和唯一 writer 都在 Node 内。增加账户中心不增加独立认证服务，也不把网关再拆一个进程。

UI 分开显示登录状态、API 验证结果和执行节点最近报告。`api_reachable` 只说明验证期内签名读取成功，不表示 Node、私流或 writer 已就绪。无新鲜 Node 投影时必须显示未知/未连接，不用 API 成功或 Control 可访问冒充在线。

验证结果有效期为 5 分钟；失效或复验失败后必须重新验证才能选择/提交命令。复验开始即撤销旧结果，并通过 revision 拒绝较旧并发探测覆盖较新结果。账户列表每 30 秒刷新。会话有效期 12 小时，退出登录立即撤销服务端会话并关闭关联事件流。

Node 的生产执行接线和实盘准入仍受 `GRID_RUNTIME_REFACTOR.md` 约束。账户管理不自动部署 Node，不自动把数据库密钥注入已有实盘进程，也不因 UI 选择成功获得交易权限；Control 返回 Accepted 仅表示语义命令已排队。

## 凭证边界

本次获准的 UI 绑定对原“凭证仅来自环境”的限制增加一个窄例外：

- UI 仅在表单和请求期间持有输入，使用掩码、清理密码编辑历史，不写入界面持久化配置、日志、URL、错误信息或 artifacts。
- 仅向显式配置的 HTTPS 或本机 HTTP 地址提交。更改 Control 地址时，先丢弃旧会话及异步回复；匿名行情不受影响。
- Control 的账户管理模块使用 AES-256-GCM 随机 nonce 加密凭证，认证附加数据绑定用户和凭证 ID；PostgreSQL 仅保存密文、Key 指纹、掩码和非秘密验证结果。
- 加密主密钥只来自 `VENUE_ACCOUNT_MASTER_KEY` 进程环境变量，为 Base64 编码的 32 字节随机值。缺失、格式错误或解密认证失败均拒绝；不得把主密钥存入数据库、TOML、日志或仓库。重启必须使用同一主密钥，应由运维在仓库外安全备份。
- 现有 Node 启动凭证来源保持进程环境/根 `.env` 不变。Control 只调用 adapter 的只读探测入口，没有物理交易写入权。
- 密码使用 Argon2id（19 MiB、2 次迭代、并行度 1、随机盐）；会话使用随机 256-bit token，数据库仅存 SHA-256 摘要。密码计算并发、注册/登录/验证频率、绑定数与会话数均有上限。
- 新增 `argon2` 是为密码哈希，现有 SHA/HMAC 不能替代慢密码哈希；直接复用锁文件中已有 `ring` 实现认证加密和随机数，不引入第二套 ORM、HTTP 或密码体系。

所有账户快照、命令和 SSE 按服务端会话和真实账户归属校验，不信任客户端传入的用户 ID。未登录只返回公共部分；无 scope 的内部 notice 不对外广播。Node 投递接口使用独立的 `VENUE_CONTROL_NODE_TOKEN`（至少 32 字符），普通用户会话不能调用；未配置时该接口关闭。

删除需要再次验证登录密码。曾验证绑定的账户，还需本次完整签名零持仓、零挂单、无运行节点/策略托管和无 Accepted/Unknown 命令；证据不足拒绝删除。凭证/会话锁及真实账户锁将删除、切换和命令入队串行化，避免同账户不同 Key 绕过检查。删除密文不删除真实账户身份和历史业务记录；数据库备份中的旧密文须按运维备份策略处理。

## 启动

Control 使用现有 PostgreSQL，通过 `DATABASE_URL` 指定连接。设置 `VENUE_ACCOUNT_MASTER_KEY` 后运行：

```text
cargo run -p venue-control --bin venue-control-server
cargo run -p venueflow --bin venueflow
```

默认监听和连接 `127.0.0.1:39180`。Control 启动时幂等安装 `0001`–`0007` migrations。`VENUE_CONTROL_BIND` 可覆盖监听地址，但服务只允许 loopback；远程 UI 应通过受控 HTTPS 入口转发，Web 使用同源部署。`VENUE_CONTROL_URL` 提供首次 UI 默认地址，已有 UI 配置通过设置修改。

不要把数据库 URL、主密钥或实际 API Key 粘贴到诊断输出。主密钥丢失无法恢复绑定密文；轮换必须另行设计迁移，不可直接换值后假定旧绑定仍可用。

## 验收

- `accounts/crypto`：随机盐、正确/错误密码、随机 nonce、篡改/用户/Key 替换拒绝。
- `accounts/credentials/tests`：真实 PostgreSQL 注册、登录、重启加载、归属隔离、稳定真实账户、复验失败与并发结果栅栏、删除风险门、命令/退出/删除并发保护。
- `http/account_tests`：真实 HTTP + PostgreSQL 会话、JSON 约束、匿名/跨用户投影与命令拒绝、SSE 数据过滤及退出后关闭。
- Binance `credential_probe`：完整签名请求面、权限/双向模式不匹配、任一面失败/不完整不通过、普通/Algo/持仓任一非零不允许安全删除。
- 原生/Web UI：账户切换、会话清理、掩码与表单布局、验证与 Node 状态分离；匿名行情与整行交易对选择继续有效。

数据库测试使用 `VENUE_CONTROL_TEST_DATABASE_URL`，并设置 `VENUE_CONTROL_POSTGRES_REQUIRED=1`，避免未配置数据库时的跳过被误认为验收。每个测试创建独立随机 schema；不接真实交易所或使用实盘凭证。
