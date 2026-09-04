//! Applied migrations never rerun data rewrites. In particular, the historic GTC scaffold
//! rename must not reinterpret new mirror GTC commands after a Control restart.
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};

#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("Control schema is unavailable")]
    Unavailable,
    #[error("An applied Control migration changed")]
    Changed,
}

pub async fn install_control_schema(pool: &PgPool) -> Result<(), SchemaError> {
    use crate::*;
    let migrations = [
        MIGRATION_0001,
        MIGRATION_0002,
        MIGRATION_0003,
        MIGRATION_0004,
        MIGRATION_0005,
        MIGRATION_0006,
        MIGRATION_0007,
        MIGRATION_0008,
        MIGRATION_0009,
        MIGRATION_0010,
        MIGRATION_0011,
        MIGRATION_0012,
        MIGRATION_0013,
        MIGRATION_0014,
        accounts::MIGRATION_0015,
        MIGRATION_0016,
        MIGRATION_0017,
        MIGRATION_0018,
        MIGRATION_0019,
        MIGRATION_0020,
        MIGRATION_0021,
        MIGRATION_0022,
        MIGRATION_0023,
        MIGRATION_0024,
        MIGRATION_0025,
        MIGRATION_0026,
        MIGRATION_0027,
        MIGRATION_0028,
        MIGRATION_0029,
        MIGRATION_0030,
    ];
    let mut tx = pool.begin().await.map_err(|_| SchemaError::Unavailable)?;
    // Serializes schema installation only; never participates in trading-account ownership.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || ':venue-control-schema',0))")
        .execute(&mut *tx).await.map_err(|_|SchemaError::Unavailable)?;
    sqlx::query("CREATE TABLE IF NOT EXISTS venue_control_schema_migrations(version INTEGER PRIMARY KEY,checksum BYTEA NOT NULL CHECK(octet_length(checksum)=32))")
        .execute(&mut *tx).await.map_err(|_|SchemaError::Unavailable)?;
    for (index, migration) in migrations.iter().enumerate() {
        let version = i32::try_from(index + 1).map_err(|_| SchemaError::Unavailable)?;
        let checksum = Sha256::digest(migration.replace("\r\n", "\n").as_bytes()).to_vec();
        let prior =
            sqlx::query("SELECT checksum FROM venue_control_schema_migrations WHERE version=$1")
                .bind(version)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|_| SchemaError::Unavailable)?;
        if let Some(prior) = prior {
            if prior
                .try_get::<Vec<u8>, _>("checksum")
                .map_err(|_| SchemaError::Unavailable)?
                != checksum
            {
                return Err(SchemaError::Changed);
            }
            continue;
        }
        sqlx::raw_sql(migration)
            .execute(&mut *tx)
            .await
            .map_err(|_| SchemaError::Unavailable)?;
        sqlx::query("INSERT INTO venue_control_schema_migrations(version,checksum) VALUES($1,$2)")
            .bind(version)
            .bind(checksum)
            .execute(&mut *tx)
            .await
            .map_err(|_| SchemaError::Unavailable)?;
    }
    tx.commit().await.map_err(|_| SchemaError::Unavailable)
}
