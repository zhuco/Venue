//! Singleton process entrypoint for the Binance KOL executor.
//!
//! This binary assembles the singleton's restricted PostgreSQL and master-key boundaries. It
//! never reads Binance API secrets from environment variables. Network transport is deliberately
//! not enabled by configuration here: production deployment must inject the reviewed signed
//! adapter, while the built-in offline mode provides a no-network convergence smoke path.

use sqlx::postgres::PgPoolOptions;
use venue_control::{
    BinanceExecutorRuntime, BinanceExecutorSingleton, accounts::CredentialCipher,
    executor_exchange::MockBinanceExecution, executor_secret::ExecutorSecretProvider,
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
    if std::env::var_os("VENUE_EXECUTOR_OFFLINE_FIXTURE").is_none() {
        singleton.release().await?;
        return Err("a reviewed Binance signed adapter is required; set VENUE_EXECUTOR_OFFLINE_FIXTURE only for an isolated fixture database".into());
    }
    let mut runtime = BinanceExecutorRuntime::new(store, secrets, MockBinanceExecution::default());
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| "system clock is before Unix epoch")?
        .as_millis();
    let now_ms = u64::try_from(now_ms).map_err(|_| "system clock is outside executor range")?;
    let report = runtime.sweep(now_ms).await?;
    eprintln!(
        "offline executor sweep submitted={} read_back={} reconciled={} rejected={} reconcile_required={}",
        report.submitted,
        report.read_back,
        report.reconciled,
        report.rejected,
        report.reconcile_required
    );
    singleton.release().await?;
    Ok(())
}
