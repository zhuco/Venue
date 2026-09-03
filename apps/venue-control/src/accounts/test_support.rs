use super::{AccountService, CredentialCipher, crypto};
use sqlx::{Executor, PgPool, postgres::PgPoolOptions};

pub type TestResult = Result<(), Box<dyn std::error::Error>>;
pub struct Fixture {
    pub pool: PgPool,
    pub service: AccountService,
    admin: PgPool,
    schema: String,
}
impl Fixture {
    pub async fn create() -> Result<Option<Self>, Box<dyn std::error::Error>> {
        let Some(url) = std::env::var("VENUE_CONTROL_TEST_DATABASE_URL").ok() else {
            if std::env::var("VENUE_CONTROL_POSTGRES_REQUIRED")
                .ok()
                .as_deref()
                == Some("1")
            {
                return Err("account PostgreSQL test database is required".into());
            }
            eprintln!("SKIP: account PostgreSQL test database is not configured");
            return Ok(None);
        };
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        let schema = format!("venue_accounts_{}", crypto::opaque_id()?.replace('-', "_"));
        admin
            .execute(format!("CREATE SCHEMA {schema}").as_str())
            .await?;
        let search_path = schema.clone();
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .after_connect(move |connection, _| {
                let sql = format!("SET search_path TO {search_path}");
                Box::pin(async move {
                    connection.execute(sql.as_str()).await?;
                    Ok(())
                })
            })
            .connect(&url)
            .await?;
        for _ in 0..2 {
            for migration in [
                crate::MIGRATION_0001,
                crate::MIGRATION_0002,
                crate::MIGRATION_0003,
                crate::MIGRATION_0004,
                crate::MIGRATION_0005,
                crate::MIGRATION_0006,
                crate::MIGRATION_0007,
                crate::MIGRATION_0008,
                crate::MIGRATION_0009,
                crate::MIGRATION_0010,
                crate::MIGRATION_0011,
                crate::MIGRATION_0012,
                crate::MIGRATION_0013,
                crate::MIGRATION_0014,
                super::MIGRATION_0015,
                crate::MIGRATION_0016,
                crate::MIGRATION_0017,
                crate::MIGRATION_0018,
                crate::MIGRATION_0019,
                crate::MIGRATION_0020,
                crate::MIGRATION_0021,
                crate::MIGRATION_0022,
                crate::MIGRATION_0023,
                crate::MIGRATION_0024,
            ] {
                sqlx::raw_sql(migration).execute(&pool).await?;
            }
        }
        let service = AccountService::new(pool.clone(), CredentialCipher::from_key(&[17; 32])?)?;
        Ok(Some(Self {
            pool,
            service,
            admin,
            schema,
        }))
    }
    pub async fn cleanup(self) -> TestResult {
        self.pool.close().await;
        self.admin
            .execute(format!("DROP SCHEMA {} CASCADE", self.schema).as_str())
            .await?;
        self.admin.close().await;
        Ok(())
    }
}
pub fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|v| v.as_millis() as u64)
        .unwrap_or(1)
}
pub fn login(username: &str) -> venue_control_protocol::accounts::LoginRequest {
    venue_control_protocol::accounts::LoginRequest {
        username: username.into(),
        password: venue_control_protocol::accounts::SecretValue::new(
            "integration passphrase only".into(),
        ),
    }
}
