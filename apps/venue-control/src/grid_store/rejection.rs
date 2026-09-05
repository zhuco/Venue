use sqlx::{Executor, Postgres};

use super::{BinanceGridStore, GridStoreError, database_error, integer, unsigned, validate_ids};

pub(crate) const EXCHANGE_REJECTION_RESET_DELAY_MS: u64 = 30_000;

pub(crate) fn rejection_reset_due(first_rejected_ms: Option<u64>, now_ms: u64) -> bool {
    first_rejected_ms
        .is_some_and(|first| now_ms.saturating_sub(first) >= EXCHANGE_REJECTION_RESET_DELAY_MS)
}

// Terminal command timestamps survive plan retries and process restarts. A new config revision
// starts a new network, so old rejections cannot repeatedly reset its replacement.
pub(super) async fn first_exchange_rejection_ms<'e>(
    executor: impl Executor<'e, Database = Postgres>,
    instance_id: &str,
    config_revision: u64,
) -> Result<Option<u64>, GridStoreError> {
    let value: Option<i64> = sqlx::query_scalar(
        "SELECT MIN(terminal_ms) FROM venue_binance_commands \
         WHERE command_origin='grid' AND grid_instance_id=$1 AND grid_config_revision=$2 \
          AND command_state='rejected' AND terminal_ms IS NOT NULL \
          AND (sanitized_error_code='binance_rejected' OR sanitized_error_code ~ '^binance_-[0-9]+$')",
    )
    .bind(instance_id)
    .bind(integer(config_revision)?)
    .fetch_one(executor)
    .await
    .map_err(database_error)?;
    value.map(unsigned).transpose()
}

impl BinanceGridStore {
    pub(crate) async fn exchange_rejection_started_ms(
        &self,
        instance_id: &str,
        config_revision: u64,
    ) -> Result<Option<u64>, GridStoreError> {
        validate_ids(&[instance_id])?;
        first_exchange_rejection_ms(&self.pool, instance_id, config_revision).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejection_deadline_includes_exactly_thirty_seconds_and_never_wraps() {
        assert!(!rejection_reset_due(None, u64::MAX));
        assert!(!rejection_reset_due(Some(100_000), 99_999));
        assert!(!rejection_reset_due(Some(100_000), 129_999));
        assert!(rejection_reset_due(Some(100_000), 130_000));
        assert!(rejection_reset_due(Some(100_000), 140_000));
        assert!(!rejection_reset_due(Some(u64::MAX - 1), u64::MAX));
    }
}
