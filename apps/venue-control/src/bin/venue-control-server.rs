//! Local PostgreSQL-backed control HTTP/SSE process.

use std::{env, net::SocketAddr, sync::Arc};

use sqlx::postgres::PgPoolOptions;
use venue_control::accounts::{AccountService, CredentialCipher};
use venue_control::{
    ControlHttpConfig, ControlService, PgControlRepository, control_shutdown_channel,
    install_control_schema, serve_local_with_accounts,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL must contain the local PostgreSQL connection string")?;
    let bind = env::var("VENUE_CONTROL_BIND")
        .unwrap_or_else(|_| "127.0.0.1:39180".to_owned())
        .parse::<SocketAddr>()
        .map_err(|_| "VENUE_CONTROL_BIND must be a socket address such as 127.0.0.1:39180")?;

    let cipher = CredentialCipher::from_environment()?;

    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await?;
    install_control_schema(&pool).await?;
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
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let accounts = Arc::new(AccountService::new(pool.clone(), cipher)?);
    let service = Arc::new(ControlService::new(PgControlRepository::new(pool)));
    let (shutdown_tx, shutdown_rx) = control_shutdown_channel();
    let server = serve_local_with_accounts(
        listener,
        service,
        accounts,
        ControlHttpConfig::default(),
        shutdown_rx,
    );
    tokio::pin!(server);

    tokio::select! {
        result = &mut server => result.map_err(Into::into),
        signal = tokio::signal::ctrl_c() => {
            signal?;
            let _ = shutdown_tx.send(true);
            server.await.map_err(Into::into)
        }
    }
}
