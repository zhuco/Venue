//! Minimal shared database boundary for the Binance KOL executor.

use sqlx::{Connection, PgConnection};

pub const MIGRATION_0017: &str = include_str!("../migrations/0017_kol_copy_mvp.sql");
pub const MIGRATION_0018: &str = include_str!("../migrations/0018_kol_follow_activation.sql");

/// Stable process-wide identity for the single Binance executor. The session that owns this
/// advisory lock is dedicated and never returned to a connection pool.
pub const BINANCE_EXECUTOR_ADVISORY_LOCK: i64 = 0x5645_4E55_454B_4F4C_i64;

pub struct BinanceExecutorSingleton {
    connection: PgConnection,
}

impl BinanceExecutorSingleton {
    pub async fn acquire(database_url: &str) -> Result<Self, ExecutorSingletonError> {
        let mut connection = PgConnection::connect(database_url).await?;
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(BINANCE_EXECUTOR_ADVISORY_LOCK)
            .fetch_one(&mut connection)
            .await?;
        if !acquired {
            let _ = connection.close().await;
            return Err(ExecutorSingletonError::AlreadyRunning);
        }
        Ok(Self { connection })
    }

    pub async fn release(mut self) -> Result<(), ExecutorSingletonError> {
        let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
            .bind(BINANCE_EXECUTOR_ADVISORY_LOCK)
            .fetch_one(&mut self.connection)
            .await?;
        self.connection.close().await?;
        if !released {
            return Err(ExecutorSingletonError::LockOwnershipLost);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutorSingletonError {
    #[error("another Binance executor already owns the singleton lock")]
    AlreadyRunning,
    #[error("the Binance executor singleton lock was not owned during release")]
    LockOwnershipLost,
    #[error("Binance executor singleton database operation failed")]
    Database(#[from] sqlx::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advisory_lock_identity_is_stable_and_nonzero() {
        assert_eq!(BINANCE_EXECUTOR_ADVISORY_LOCK, 0x5645_4E55_454B_4F4C);
        assert_ne!(BINANCE_EXECUTOR_ADVISORY_LOCK, 0);
    }
}
