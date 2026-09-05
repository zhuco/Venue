# Binance KOL Executor 离线发布与回滚清单

本清单只覆盖离线构建、fixture 和可恢复的部署准备；它不授权连接 Binance、读取用户密钥或下单。真实 Canary 仍须遵守 [KOL MVP 契约](KOL_COPY_MVP.md) 的单独授权与隔离账户条件。

## 发布前离线门

1. 在干净、指定 revision 的 checkout 中运行 `scripts/Invoke-KolCanaryDrill.ps1 -OfflineFixture`。脚本拒绝进程内 `BINANCE_API_KEY`、`BINANCE_API_SECRET`，只通过受控 Cargo 入口运行 5 KOL / 200 follower fixture。
2. 若配置了独立测试 PostgreSQL，运行 `scripts/verify_postgres_integration.ps1`；它必须不出现 `SKIP:`。该门覆盖源成交去重、稳定命令 ID、owner-scoped 密文、重启 readback、超时栅栏与拒单 fixture。
3. 受控 Ubuntu 入口建立全量基线后，`scripts/Build-VenueUbuntu.ps1 -Component Control` 的版本化包包含 `venue-control-server`、`venue-executor-binance` 与离线授权工具 `venue-leader-bot-admin`，不得含 `venue-copy-worker`。binary 只接受 `VENUE_EXECUTOR_MODE=LIVE`、PostgreSQL `VENUE_EXECUTOR_DATABASE_URL` 和既有 credential master key；不提供 mock、dry-run 或 testnet 配置。
4. Grid 发布必须确认 `0021`–`0024` 已按序落库；`0024` 的输入 desired 摘要、唯一前驱和实例批次尾列缺一不可。记录 release hash、控制服务和 Executor 二进制哈希、迁移版本、脱敏账户数量及待对账计数。不要记录连接串、API key、secret、listen key 或原始私流帧。

## 真实部署与回滚边界

带单升级须由 `schema.rs` 安装并验证 `0028`/`0029`，同步核验 [授权及数据库角色](LEADER_ORDER_MIRROR.md)。旧 Canary 脚本的成交模型 fixture 不能代替新挂单模型的 5/200 容量与延迟验收。数据库含真实 GTC 镜像后，不得直接回退到每次启动重跑旧迁移的 Control binary：旧 `0020` 会重解释 GTC，须先单独核验回滚版本的迁移兼容性。

管理员可先运行包内 `venue-leader-bot-admin migrate`，然后以迁移所有者向运行角色授予实际需要的表与序列权限；授权表和授权审计表只授予运行角色 SELECT。生产 Control 设置 `VENUE_CONTROL_RUNTIME_DATABASE_URL`，Executor 使用其独立 `VENUE_EXECUTOR_DATABASE_URL`，二者不得拥有授权表或持有能改回管理员身份的角色成员关系。迁移 URL 不进入启动参数或日志。

上线前由获授权操作者确认目标账户未被旧 Node/Copy writer 管理，且旧 `Prepared/Submitted/Unknown`、仓位和订单均已按签名事实收敛。先启动并确认 PostgreSQL advisory lock 仅由一个 `venue-executor-binance` 持有；它会先完成 activation baseline 与未终态 readback，再建立 KOL listenKey 私流。锁、数据库、凭证、规则或基线失败均不得发送新订单；私流断线/过期只经有界退避与签名 REST 补读恢复，原始帧和 listenKey 不记录。

回滚只暂停关系并以 SIGINT/SIGTERM 停止新 Executor；它关闭私流并释放锁。不得删除 PostgreSQL 命令、源成交、目标、凭证或旧恢复工件。`Sending`、`Accepted`、`ReconcileRequired` 先由同一 `clientOrderId` 查单和签名仓位收敛；精确回读遵守数据库中 500 ms 起、8 s 封顶的耐久退避，未到期仍保留账户栅栏。未终态时不得恢复旧 writer 或盲目重新发送。确认所有账户无未决命令且旧链仍未接管后，才允许按单独变更恢复此前部署。
