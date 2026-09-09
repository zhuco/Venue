use super::*;

impl BinanceGridStore {
    pub(super) async fn begin_transaction(
        &self,
    ) -> Result<Transaction<'static, Postgres>, GridStoreError> {
        finish_begin(self.pool.clone(), None).await
    }
}

async fn finish_begin(
    pool: PgPool,
    statement: Option<std::borrow::Cow<'static, str>>,
) -> Result<Transaction<'static, Postgres>, GridStoreError> {
    // SQLx 0.8 increments transaction depth only after BEGIN's ReadyForQuery. Cancelling
    // earlier can return a server-side open transaction with depth zero to the pool.
    // Complete just the handshake; if the caller left, the returned Transaction rolls back.
    // No planning, command insertion or commit is allowed inside this detached task.
    let connection = pool.acquire().await.map_err(database_error)?;
    tokio::spawn(async move { Transaction::begin(connection, statement).await })
        .await
        .map_err(|_| GridStoreError::Unavailable)?
        .map_err(database_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn interrupted_begin_rolls_back_before_connection_reuse()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(url) = std::env::var("VENUE_CONTROL_TEST_DATABASE_URL") else {
            eprintln!("SKIP: VENUE_CONTROL_TEST_DATABASE_URL is not configured");
            return Ok(());
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        sqlx::query("CREATE TEMP TABLE grid_begin_cancellation_probe(value INTEGER)")
            .execute(&pool)
            .await?;
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(25),
            finish_begin(pool.clone(), Some("BEGIN; INSERT INTO grid_begin_cancellation_probe VALUES(1); SELECT pg_sleep(0.2)".into())),
        ).await;
        assert!(result.is_err());
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM grid_begin_cancellation_probe")
            .fetch_one(&pool)
            .await?;
        assert_eq!(
            count, 0,
            "an abandoned BEGIN must never leak its transaction into the next borrower"
        );
        let mut transaction = finish_begin(pool.clone(), None).await?;
        sqlx::query("INSERT INTO grid_begin_cancellation_probe VALUES(2)")
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM grid_begin_cancellation_probe")
            .fetch_one(&pool)
            .await?;
        assert_eq!(count, 1);
        pool.close().await;
        Ok(())
    }
}
