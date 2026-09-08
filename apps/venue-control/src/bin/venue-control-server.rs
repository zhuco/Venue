//! Local PostgreSQL-backed control HTTP/SSE process.
//!
//! ## 请求调用流程总览
//!
//! 第一步 (main): 启动入口 — 解析环境变量、建立数据库连接池、创建 service 实例、绑定 TCP 监听器，
//!               然后调用 `serve_local_with_accounts` 进入 HTTP 主循环。
//! 第二步 (http::serve_inner): TCP 主循环 — accept 连接，限制并发数，每个连接 spawn 到独立 task。
//! 第三步 (http::handle_connection): 读取 HTTP 请求报文（header + body），带超时控制。
//! 第四步 (http::dispatch): 路由入口 — 非测试模式统一转发到 `accounts::dispatch_authenticated`。
//! 第五步 (accounts::dispatch_authenticated): 认证与业务路由分发 — 解析 Bearer token、鉴权、
//!               按 (Method, path) 匹配到具体 handler，调用 AccountService/ControlService 方法，
//!               序列化响应并写回 TCP 流。

use std::{env, net::SocketAddr, sync::Arc};

use sqlx::postgres::PgPoolOptions;
use venue_control::accounts::{AccountService, CredentialCipher, run_managed_deletion_cleanup};
use venue_control::{
    ControlHttpConfig, ControlService, PgControlRepository, control_shutdown_channel,
    install_control_schema, serve_local_with_accounts,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── 第一步：解析配置 ──────────────────────────────────────────────
    // 从环境变量读取数据库连接串和 TCP 绑定地址（默认 127.0.0.1:39180）
    let database_url = env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL must contain the local PostgreSQL connection string")?;
    let bind = env::var("VENUE_CONTROL_BIND")
        .unwrap_or_else(|_| "127.0.0.1:39180".to_owned())
        .parse::<SocketAddr>()
        .map_err(|_| "VENUE_CONTROL_BIND must be a socket address such as 127.0.0.1:39180")?;

    // ── 第一步：初始化凭证加解密（AES-256-GCM） ──────────────────────
    let cipher = CredentialCipher::from_environment()?;

    // ── 第一步：建立 PostgreSQL 连接池并安装 schema ───────────────────
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await?;
    install_control_schema(&pool).await?;
    // 如果指定了运行时数据库 URL，切换到运行时连接池
    let pool = match env::var("VENUE_CONTROL_RUNTIME_DATABASE_URL") {
        Ok(runtime_url) => {
            pool.close().await;
            PgPoolOptions::new()
                .max_connections(8)
                .connect(&runtime_url)
                .await?
        }
        Err(env::VarError::NotPresent) => pool,
        Err(_) => return Err("Control runtime database URL is invalid".into()),
    };

    // ── 第一步：创建核心 service 实例并启动 HTTP 主循环 ──────────────
    // listener:  TCP 监听器，接收 HTTP 连接
    // accounts:  账户服务（注册/登录/鉴权/业务操作）
    // service:   控制服务（快照/命令/事件/跟单关系）
    // 最终调用 serve_local_with_accounts → 进入 http::serve_inner
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let accounts = Arc::new(AccountService::new(pool.clone(), cipher)?);
    let service = Arc::new(ControlService::new(PgControlRepository::new(pool.clone())));
    let (shutdown_tx, shutdown_rx) = control_shutdown_channel();
    let cleanup_task = tokio::spawn(run_managed_deletion_cleanup(pool, shutdown_rx.clone()));
    let server = serve_local_with_accounts(
        listener,
        service,
        accounts,
        ControlHttpConfig::default(),
        shutdown_rx,
    );
    tokio::pin!(server);

    // ── 优雅关闭：等待 Ctrl+C 或 server 自行退出 ─────────────────────
    let result = tokio::select! {
        result = &mut server => result.map_err(Into::into),
        signal = tokio::signal::ctrl_c() => {
            signal?;
            let _ = shutdown_tx.send(true);
            server.await.map_err(Into::into)
        }
    };
    let _ = shutdown_tx.send(true);
    cleanup_task.await?;
    result
}
