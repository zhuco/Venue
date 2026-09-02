//! Local PostgreSQL-backed control HTTP/SSE process.

use std::{env, net::SocketAddr, sync::Arc};

use sqlx::{PgPool, postgres::PgPoolOptions};
use venue_control::accounts::{AccountService, CredentialCipher, MIGRATION_0015};
use venue_control::{
    ControlHttpConfig, ControlService, MIGRATION_0001, MIGRATION_0002, MIGRATION_0003,
    MIGRATION_0004, MIGRATION_0005, MIGRATION_0006, MIGRATION_0007, MIGRATION_0008, MIGRATION_0009,
    MIGRATION_0010, MIGRATION_0011, MIGRATION_0012, MIGRATION_0013, MIGRATION_0014, MIGRATION_0016,
    MIGRATION_0017, PgControlRepository, control_shutdown_channel, serve_local_with_accounts,
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
    install_schema(&pool).await?;
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

async fn install_schema(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::raw_sql(MIGRATION_0001).execute(pool).await?;
    sqlx::raw_sql(MIGRATION_0002).execute(pool).await?;
    sqlx::raw_sql(MIGRATION_0003).execute(pool).await?;
    sqlx::raw_sql(MIGRATION_0004).execute(pool).await?;
    sqlx::raw_sql(MIGRATION_0005).execute(pool).await?;
    sqlx::raw_sql(MIGRATION_0006).execute(pool).await?;
    sqlx::raw_sql(MIGRATION_0007).execute(pool).await?;
    sqlx::raw_sql(MIGRATION_0008).execute(pool).await?;
    sqlx::raw_sql(MIGRATION_0009).execute(pool).await?;
    sqlx::raw_sql(MIGRATION_0010).execute(pool).await?;
    sqlx::raw_sql(MIGRATION_0011).execute(pool).await?;
    sqlx::raw_sql(MIGRATION_0012).execute(pool).await?;
    sqlx::raw_sql(MIGRATION_0013).execute(pool).await?;
    sqlx::raw_sql(MIGRATION_0014).execute(pool).await?;
    sqlx::raw_sql(MIGRATION_0015).execute(pool).await?;
    sqlx::raw_sql(MIGRATION_0016).execute(pool).await?;
    sqlx::raw_sql(MIGRATION_0017).execute(pool).await?;
    Ok(())
}
