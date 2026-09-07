use sqlx::{
    Executor,
    postgres::{PgListener, PgPoolOptions},
};
use std::time::Duration;

#[tokio::test]
async fn notifications_follow_commit_and_listener_closes() -> Result<(), Box<dyn std::error::Error>>
{
    let url = match std::env::var("VENUE_CONTROL_TEST_DATABASE_URL") {
        Ok(url) => url,
        Err(_) if std::env::var("VENUE_CONTROL_POSTGRES_REQUIRED").as_deref() == Ok("1") => {
            return Err("PostgreSQL URL required".into());
        }
        Err(_) => {
            eprintln!("SKIP: PostgreSQL URL is not configured");
            return Ok(());
        }
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await?;
    let schema = format!(
        "venue_realtime_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    );
    let mut connection = pool.acquire().await?;
    connection.execute(format!("CREATE SCHEMA {schema}; SET search_path TO {schema}; CREATE TABLE venue_binance_commands(id int); CREATE TABLE venue_binance_account_projections(id int)").as_str()).await?;
    sqlx::raw_sql(venue_control::MIGRATION_0044)
        .execute(&mut *connection)
        .await?;
    let mut listener = PgListener::connect(&url).await?;
    listener.listen("venue_executor_commands").await?;
    listener.listen("venue_terminal_projection").await?;
    connection
        .execute("BEGIN; INSERT INTO venue_binance_commands VALUES (1)")
        .await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(80), listener.recv())
            .await
            .is_err()
    );
    connection.execute("ROLLBACK").await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(80), listener.recv())
            .await
            .is_err()
    );
    connection
        .execute("BEGIN; INSERT INTO venue_binance_commands VALUES (2); COMMIT")
        .await?;
    let notification = tokio::time::timeout(Duration::from_secs(1), listener.recv()).await??;
    assert_eq!(notification.channel(), "venue_executor_commands");
    assert_eq!(notification.payload(), "");
    connection
        .execute("INSERT INTO venue_binance_account_projections VALUES (1)")
        .await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), listener.recv())
            .await??
            .channel(),
        "venue_terminal_projection"
    );
    let mut wake = venue_control::database_wake::listen(pool.clone(), "venue_realtime_test");
    tokio::time::timeout(Duration::from_secs(2), wake.changed()).await??;
    connection.execute("NOTIFY venue_realtime_test").await?;
    tokio::time::timeout(Duration::from_secs(1), wake.changed()).await??;
    drop(wake);
    connection
        .execute(format!("DROP SCHEMA {schema} CASCADE").as_str())
        .await?;
    drop(connection);
    tokio::time::timeout(Duration::from_secs(2), pool.close()).await?;
    Ok(())
}
