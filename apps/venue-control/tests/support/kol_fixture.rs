use super::*;

impl Fixture {
    pub(super) async fn create(database_url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(database_url)
            .await?;
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        // Windows clock resolution can give parallel fixtures the same timestamp.
        static NEXT_SCHEMA: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sequence = NEXT_SCHEMA.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let schema = format!(
            "venue_kol_mvp_{}_{}_{}",
            std::process::id(),
            nonce,
            sequence
        );
        admin
            .execute(format!("CREATE SCHEMA {schema}").as_str())
            .await?;
        let search_path = schema.clone();
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .after_connect(move |connection, _| {
                let sql = format!("SET search_path TO {search_path}");
                Box::pin(async move {
                    connection.execute(sql.as_str()).await?;
                    Ok(())
                })
            })
            .connect(database_url)
            .await?;
        Ok(Self {
            pool,
            admin,
            schema,
        })
    }

    pub(super) async fn migrate_twice(&self) -> Result<(), sqlx::Error> {
        for _ in 0..2 {
            self.migrate_through_0021().await?;
            sqlx::raw_sql(MIGRATION_0022).execute(&self.pool).await?;
            sqlx::raw_sql(MIGRATION_0023).execute(&self.pool).await?;
            sqlx::raw_sql(MIGRATION_0024).execute(&self.pool).await?;
            sqlx::raw_sql(venue_control::MIGRATION_0025)
                .execute(&self.pool)
                .await?;
            sqlx::raw_sql(venue_control::MIGRATION_0026)
                .execute(&self.pool)
                .await?;
            sqlx::raw_sql(venue_control::MIGRATION_0027)
                .execute(&self.pool)
                .await?;
            sqlx::raw_sql(venue_control::MIGRATION_0028)
                .execute(&self.pool)
                .await?;
        }
        sqlx::raw_sql(venue_control::MIGRATION_0029)
            .execute(&self.pool)
            .await?;
        sqlx::raw_sql(venue_control::MIGRATION_0030)
            .execute(&self.pool)
            .await?;
        sqlx::raw_sql(venue_control::MIGRATION_0031)
            .execute(&self.pool)
            .await?;
        sqlx::raw_sql(venue_control::MIGRATION_0032)
            .execute(&self.pool)
            .await?;
        sqlx::raw_sql(venue_control::MIGRATION_0033)
            .execute(&self.pool)
            .await?;
        sqlx::raw_sql(venue_control::MIGRATION_0034)
            .execute(&self.pool)
            .await?;
        sqlx::raw_sql(venue_control::MIGRATION_0035)
            .execute(&self.pool)
            .await?;
        sqlx::raw_sql(venue_control::MIGRATION_0036)
            .execute(&self.pool)
            .await?;
        sqlx::raw_sql(venue_control::MIGRATION_0037)
            .execute(&self.pool)
            .await?;
        sqlx::raw_sql(venue_control::MIGRATION_0038)
            .execute(&self.pool)
            .await?;
        sqlx::raw_sql(venue_control::MIGRATION_0039)
            .execute(&self.pool)
            .await?;
        sqlx::raw_sql(venue_control::MIGRATION_0040)
            .execute(&self.pool)
            .await?;
        sqlx::raw_sql(venue_control::MIGRATION_0043)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub(super) async fn migrate_through_0021(&self) -> Result<(), sqlx::Error> {
        for migration in [
            MIGRATION_0001,
            MIGRATION_0015,
            MIGRATION_0017,
            MIGRATION_0018,
            MIGRATION_0019,
            MIGRATION_0020,
            MIGRATION_0021,
        ] {
            sqlx::raw_sql(migration).execute(&self.pool).await?;
        }
        Ok(())
    }

    pub(super) async fn cleanup(self) -> Result<(), sqlx::Error> {
        self.pool.close().await;
        self.admin
            .execute(format!("DROP SCHEMA {} CASCADE", self.schema).as_str())
            .await?;
        self.admin.close().await;
        Ok(())
    }
}
