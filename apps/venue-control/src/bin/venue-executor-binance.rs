//! Singleton process entrypoint for the Binance KOL executor.
//!
//! This binary assembles the singleton's restricted PostgreSQL and master-key boundaries. It
//! never reads Binance API secrets from environment variables.

use sqlx::postgres::PgPoolOptions;
use venue_control::{
    BinanceExecutorSingleton, accounts::CredentialCipher, executor_secret::ExecutorSecretProvider,
    executor_store::PgExecutorStore,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("VENUE_EXECUTOR_DATABASE_URL")
        .map_err(|_| "VENUE_EXECUTOR_DATABASE_URL is required")?;
    let singleton = BinanceExecutorSingleton::acquire(&database_url).await?;
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await?;
    let store = PgExecutorStore::new(pool.clone());
    let secrets = ExecutorSecretProvider::new(pool, CredentialCipher::from_environment()?);
    // Keep construction type-checked without converting any encrypted record until a command is
    // claimed by the later runtime loop. No API key has an environment fallback.
    std::mem::drop((store, secrets));
    singleton.release().await?;
    Err("Binance executor event loop is not configured".into())
}
