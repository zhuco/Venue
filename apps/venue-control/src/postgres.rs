use sqlx::{PgPool, Postgres, Row, Transaction};
use venue_control_protocol::{
    CONTROL_SCHEMA_VERSION, CommandReceipt, CommandState, ControlCommandRequest, ControlSnapshot,
    ExecutionFactBinding, ExecutionFactsSnapshot, GatewayMode, NodeProjectionEnvelope,
    UiAccountScope, UiEventEnvelope, UiEventKind, UiEventNotification, VenueId,
};

use crate::{
    AccountNodeBinding, ClaimedCommand, CommandEnqueueResult, CommandSettleResult,
    ControlRepository, RepositoryError, ScopedCommandReceipt, SnapshotStoreResult, StoredEvent,
    account_delivery_postgres::insert_control_account_delivery,
};

pub const MIGRATION_0001: &str = include_str!("../migrations/0001_control_core.sql");
pub const MIGRATION_0005: &str = include_str!("../migrations/0005_live_only.sql");
pub const MIGRATION_0009: &str = include_str!("../migrations/0009_control_execution_facts.sql");
pub const MIGRATION_0011: &str = include_str!("../migrations/0011_node_projection_inbox.sql");
pub const MIGRATION_0012: &str =
    include_str!("../migrations/0012_node_projection_instance_cursor.sql");
pub const MIGRATION_0014: &str = include_str!("../migrations/0014_manual_trade_intent.sql");

#[derive(Clone, Debug)]
pub struct PgControlRepository {
    pool: PgPool,
}

impl PgControlRepository {
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

impl ControlRepository for PgControlRepository {
    async fn load_snapshot(&self) -> Result<Option<ControlSnapshot>, RepositoryError> {
        let row =
            sqlx::query("SELECT snapshot_json FROM venue_control_snapshots WHERE singleton = TRUE")
                .fetch_optional(&self.pool)
                .await
                .map_err(database_error)?;
        match row {
            Some(row) => {
                let encoded = row.try_get("snapshot_json").map_err(database_error)?;
                Ok(Some(decode(encoded)?))
            }
            None => Ok(None),
        }
    }

    async fn load_execution_facts(
        &self,
    ) -> Result<Option<ExecutionFactsSnapshot>, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
        let row = sqlx::query(
            "SELECT facts_json FROM venue_control_execution_facts WHERE singleton = TRUE",
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?;
        let mut facts = row
            .map(|row| decode(row.try_get("facts_json").map_err(database_error)?))
            .transpose()?;
        if let Some(facts) = facts.as_mut() {
            crate::copy_ledger_read_model::overlay_durable_copy_facts(&mut transaction, facts)
                .await?;
        }
        transaction.commit().await.map_err(database_error)?;
        Ok(facts)
    }

    async fn store_snapshot(
        &self,
        snapshot: &ControlSnapshot,
    ) -> Result<SnapshotStoreResult, RepositoryError> {
        snapshot
            .validate()
            .map_err(|_| RepositoryError::CorruptData)?;
        let generated_ms = to_i64(snapshot.generated_ms)?;
        let snapshot_json = encode(snapshot)?;
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        sqlx::query("SELECT pg_advisory_xact_lock(834766813558209236)")
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;

        if let Some(row) = sqlx::query(
            "SELECT generated_ms, snapshot_json FROM venue_control_snapshots \
             WHERE singleton = TRUE FOR UPDATE",
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?
        {
            let current_ms: i64 = row.try_get("generated_ms").map_err(database_error)?;
            let current: ControlSnapshot =
                decode(row.try_get("snapshot_json").map_err(database_error)?)?;
            if current_ms == generated_ms && current == *snapshot {
                transaction.rollback().await.map_err(database_error)?;
                return Ok(SnapshotStoreResult::Unchanged);
            }
            if generated_ms <= current_ms {
                transaction.rollback().await.map_err(database_error)?;
                return Err(RepositoryError::SnapshotConflict);
            }
        }

        sqlx::query(
            "INSERT INTO venue_control_snapshots (singleton, generated_ms, snapshot_json) \
             VALUES (TRUE, $1, $2) \
             ON CONFLICT (singleton) DO UPDATE \
             SET generated_ms = EXCLUDED.generated_ms, snapshot_json = EXCLUDED.snapshot_json",
        )
        .bind(generated_ms)
        .bind(snapshot_json)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;

        sqlx::query("DELETE FROM venue_control_strategy_scopes")
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
        for strategy in &snapshot.strategies {
            sqlx::query(
                "INSERT INTO venue_control_strategy_scopes \
                 (instance_id, venue, mode, trading_account_id, symbol, config_epoch, snapshot_generated_ms) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(&strategy.instance_id)
            .bind(strategy.venue.as_str())
            .bind(strategy.mode.as_str())
            .bind(&strategy.trading_account_id)
            .bind(strategy.symbol.to_string())
            .bind(to_i64(strategy.config_epoch)?)
            .bind(generated_ms)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
        }

        let mut event_sequence = None;
        for account in &snapshot.accounts {
            event_sequence = Some(
                insert_ui_event(
                    &mut transaction,
                    snapshot.generated_ms,
                    UiEventKind::Snapshot,
                    UiAccountScope {
                        venue: account.venue,
                        mode: account.mode,
                        trading_account_id: account.trading_account_id.clone(),
                    },
                )
                .await?,
            );
        }
        transaction.commit().await.map_err(database_error)?;
        Ok(SnapshotStoreResult::Inserted {
            event_sequence: event_sequence.unwrap_or(0),
        })
    }

    async fn store_execution_facts(
        &self,
        facts: &ExecutionFactsSnapshot,
    ) -> Result<SnapshotStoreResult, RepositoryError> {
        facts.validate().map_err(|_| RepositoryError::CorruptData)?;
        let generated_ms = to_i64(facts.generated_ms)?;
        let facts_json = encode(facts)?;
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        sqlx::query("SELECT pg_advisory_xact_lock(834766813558209237)")
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
        if let Some(row) = sqlx::query(
            "SELECT generated_ms, facts_json FROM venue_control_execution_facts \
             WHERE singleton = TRUE FOR UPDATE",
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?
        {
            let current_ms: i64 = row.try_get("generated_ms").map_err(database_error)?;
            let current: ExecutionFactsSnapshot =
                decode(row.try_get("facts_json").map_err(database_error)?)?;
            if current_ms == generated_ms && current == *facts {
                transaction.rollback().await.map_err(database_error)?;
                return Ok(SnapshotStoreResult::Unchanged);
            }
            if generated_ms <= current_ms {
                transaction.rollback().await.map_err(database_error)?;
                return Err(RepositoryError::SnapshotConflict);
            }
        }
        sqlx::query(
            "INSERT INTO venue_control_execution_facts (singleton, generated_ms, facts_json) \
             VALUES (TRUE, $1, $2) ON CONFLICT (singleton) DO UPDATE \
             SET generated_ms = EXCLUDED.generated_ms, facts_json = EXCLUDED.facts_json",
        )
        .bind(generated_ms)
        .bind(facts_json)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        let mut event_sequence = None;
        for scope in execution_fact_scopes(facts) {
            event_sequence = Some(
                insert_ui_event(
                    &mut transaction,
                    facts.generated_ms,
                    UiEventKind::ExecutionFacts,
                    scope,
                )
                .await?,
            );
        }
        transaction.commit().await.map_err(database_error)?;
        Ok(SnapshotStoreResult::Inserted {
            event_sequence: event_sequence.unwrap_or(0),
        })
    }

    async fn merge_node_projection(
        &self,
        projection: &NodeProjectionEnvelope,
    ) -> Result<SnapshotStoreResult, RepositoryError> {
        crate::node_projection_postgres::merge(self.pool(), projection).await
    }

    async fn enqueue_command(
        &self,
        command: &ControlCommandRequest,
        accepted: &CommandReceipt,
    ) -> Result<CommandEnqueueResult, RepositoryError> {
        command
            .validate()
            .map_err(|_| RepositoryError::CorruptData)?;
        accepted
            .validate()
            .map_err(|_| RepositoryError::CorruptData)?;
        let command_json = encode(command)?;
        let receipt_json = encode(accepted)?;
        let created_ms = to_i64(accepted.observed_ms)?;
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(&command.request_id)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;

        let current_scope = sqlx::query(
            "SELECT 1 FROM venue_control_strategy_scopes \
             WHERE instance_id = $1 AND venue = $2 AND mode = $3 \
               AND trading_account_id = $4 AND symbol = $5 AND config_epoch = $6 \
             FOR SHARE",
        )
        .bind(&command.instance_id)
        .bind(command.venue.as_str())
        .bind(command.mode.as_str())
        .bind(&command.trading_account_id)
        .bind(command.symbol.to_string())
        .bind(to_i64(command.expected_config_epoch)?)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?;
        if current_scope.is_none() {
            transaction.rollback().await.map_err(database_error)?;
            return Err(RepositoryError::StaleScope);
        }

        if let Some(row) = sqlx::query(
            "SELECT command_json, receipt_json FROM venue_control_command_inbox \
             WHERE request_id = $1 FOR UPDATE",
        )
        .bind(&command.request_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?
        {
            let durable_command: ControlCommandRequest =
                decode(row.try_get("command_json").map_err(database_error)?)?;
            if durable_command != *command {
                transaction.rollback().await.map_err(database_error)?;
                return Err(RepositoryError::ReplayConflict);
            }
            let durable_receipt = decode(row.try_get("receipt_json").map_err(database_error)?)?;
            transaction.rollback().await.map_err(database_error)?;
            return Ok(CommandEnqueueResult::Existing(durable_receipt));
        }

        sqlx::query(
            "INSERT INTO venue_control_command_inbox \
             (request_id, venue, mode, trading_account_id, symbol, instance_id, config_epoch, \
              action, command_state, command_json, receipt_json, created_ms, updated_ms) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'accepted', $9, $10, $11, $11)",
        )
        .bind(&command.request_id)
        .bind(command.venue.as_str())
        .bind(command.mode.as_str())
        .bind(&command.trading_account_id)
        .bind(command.symbol.to_string())
        .bind(&command.instance_id)
        .bind(to_i64(command.expected_config_epoch)?)
        .bind(command.action.as_str())
        .bind(command_json)
        .bind(receipt_json)
        .bind(created_ms)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        sqlx::query(
            "INSERT INTO venue_control_command_outbox \
             (request_id, delivery_state, claimed_by, claimed_ms) \
             VALUES ($1, 'pending', NULL, NULL)",
        )
        .bind(&command.request_id)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        insert_control_account_delivery(&mut transaction, command, accepted.observed_ms)
            .await
            .map_err(|error| match error {
                crate::AccountDeliveryRepositoryError::BindingConflict => {
                    RepositoryError::StaleScope
                }
                crate::AccountDeliveryRepositoryError::NumericRange => {
                    RepositoryError::NumericRange
                }
                crate::AccountDeliveryRepositoryError::CorruptData => RepositoryError::CorruptData,
                _ => RepositoryError::Database,
            })?;
        insert_ui_event(
            &mut transaction,
            accepted.observed_ms,
            UiEventKind::Command,
            command_scope(command),
        )
        .await?;
        transaction.commit().await.map_err(database_error)?;
        Ok(CommandEnqueueResult::Inserted(accepted.clone()))
    }

    async fn claim_commands(
        &self,
        binding: &AccountNodeBinding,
        consumer_id: &str,
        claimed_ms: u64,
        limit: u32,
    ) -> Result<Vec<ClaimedCommand>, RepositoryError> {
        binding
            .validate()
            .map_err(|_| RepositoryError::CorruptData)?;
        let claimed_ms_i64 = to_i64(claimed_ms)?;
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        let rows = sqlx::query(
            "SELECT i.request_id, i.command_json \
             FROM venue_control_command_inbox i \
             JOIN venue_control_command_outbox o USING (request_id) \
             JOIN venue_control_strategy_scopes s \
               ON s.instance_id = i.instance_id \
              AND s.venue = i.venue \
              AND s.mode = i.mode \
              AND s.trading_account_id = i.trading_account_id \
              AND s.symbol = i.symbol \
              AND s.config_epoch = i.config_epoch \
             WHERE i.venue = $1 AND i.mode = $2 AND i.trading_account_id = $3 \
               AND i.command_state = 'accepted' AND o.delivery_state = 'pending' \
             ORDER BY i.created_ms, i.request_id \
             FOR UPDATE OF o SKIP LOCKED LIMIT $4",
        )
        .bind(binding.venue.as_str())
        .bind(binding.mode.as_str())
        .bind(&binding.trading_account_id)
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(database_error)?;

        let mut claimed = Vec::with_capacity(rows.len());
        for row in rows {
            let request_id: String = row.try_get("request_id").map_err(database_error)?;
            let command: ControlCommandRequest =
                decode(row.try_get("command_json").map_err(database_error)?)?;
            let updated = sqlx::query(
                "UPDATE venue_control_command_outbox \
                 SET delivery_state = 'claimed', claimed_by = $2, claimed_ms = $3 \
                 WHERE request_id = $1 AND delivery_state = 'pending'",
            )
            .bind(&request_id)
            .bind(consumer_id)
            .bind(claimed_ms_i64)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
            if updated.rows_affected() != 1 {
                transaction.rollback().await.map_err(database_error)?;
                return Err(RepositoryError::DeliveryConflict);
            }
            claimed.push(ClaimedCommand {
                command,
                consumer_id: consumer_id.to_owned(),
                claimed_ms,
            });
        }
        transaction.commit().await.map_err(database_error)?;
        Ok(claimed)
    }

    async fn settle_command(
        &self,
        scoped: &ScopedCommandReceipt,
    ) -> Result<CommandSettleResult, RepositoryError> {
        scoped
            .validate()
            .map_err(|_| RepositoryError::CorruptData)?;
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        let row = sqlx::query(
            "SELECT i.command_json, i.receipt_json, o.delivery_state, o.claimed_by, o.claimed_ms \
             FROM venue_control_command_inbox i \
             JOIN venue_control_command_outbox o USING (request_id) \
             WHERE i.request_id = $1 FOR UPDATE OF i, o",
        )
        .bind(&scoped.command.request_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?
        .ok_or(RepositoryError::DeliveryConflict)?;

        let durable_command: ControlCommandRequest =
            decode(row.try_get("command_json").map_err(database_error)?)?;
        if durable_command != scoped.command {
            transaction.rollback().await.map_err(database_error)?;
            return Err(RepositoryError::DeliveryConflict);
        }
        let durable_receipt: CommandReceipt =
            decode(row.try_get("receipt_json").map_err(database_error)?)?;
        if durable_receipt.state != CommandState::Accepted {
            transaction.rollback().await.map_err(database_error)?;
            return if durable_receipt == scoped.receipt {
                Ok(CommandSettleResult::Existing(durable_receipt))
            } else {
                Err(RepositoryError::DeliveryConflict)
            };
        }

        let delivery_state: String = row.try_get("delivery_state").map_err(database_error)?;
        let claimed_by: Option<String> = row.try_get("claimed_by").map_err(database_error)?;
        let claimed_ms: Option<i64> = row.try_get("claimed_ms").map_err(database_error)?;
        if delivery_state != "claimed"
            || claimed_by.as_deref() != Some(scoped.consumer_id.as_str())
            || claimed_ms.is_none_or(|time| scoped.receipt.observed_ms < time as u64)
            || scoped.receipt.observed_ms < durable_receipt.observed_ms
        {
            transaction.rollback().await.map_err(database_error)?;
            return Err(RepositoryError::DeliveryConflict);
        }

        let receipt_json = encode(&scoped.receipt)?;
        let observed_ms = to_i64(scoped.receipt.observed_ms)?;
        let inbox_updated = sqlx::query(
            "UPDATE venue_control_command_inbox \
             SET command_state = $2, receipt_json = $3, updated_ms = $4 WHERE request_id = $1",
        )
        .bind(&scoped.command.request_id)
        .bind(command_state(scoped.receipt.state))
        .bind(receipt_json)
        .bind(observed_ms)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        if inbox_updated.rows_affected() != 1 {
            transaction.rollback().await.map_err(database_error)?;
            return Err(RepositoryError::DeliveryConflict);
        }
        let outbox_updated = sqlx::query(
            "UPDATE venue_control_command_outbox SET delivery_state = 'settled' \
             WHERE request_id = $1 AND delivery_state = 'claimed'",
        )
        .bind(&scoped.command.request_id)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        if outbox_updated.rows_affected() != 1 {
            transaction.rollback().await.map_err(database_error)?;
            return Err(RepositoryError::DeliveryConflict);
        }
        insert_ui_event(
            &mut transaction,
            scoped.receipt.observed_ms,
            UiEventKind::Command,
            command_scope(&scoped.command),
        )
        .await?;
        transaction.commit().await.map_err(database_error)?;
        Ok(CommandSettleResult::Stored(scoped.receipt.clone()))
    }

    async fn list_events(
        &self,
        scope: &UiAccountScope,
        after_sequence: i64,
        limit: u32,
    ) -> Result<Vec<StoredEvent>, RepositoryError> {
        scope.validate().map_err(|_| RepositoryError::CorruptData)?;
        let rows = sqlx::query(
            "WITH scoped AS ( \
                 SELECT event_sequence, event_json, \
                        COALESCE(LAG(event_sequence) OVER (ORDER BY event_sequence), 0) \
                          AS previous_cursor \
                 FROM venue_control_events \
                 WHERE event_json->'scope'->>'venue' = $2 \
                   AND event_json->'scope'->>'mode' = $3 \
                   AND event_json->'scope'->>'trading_account_id' = $4 \
             ) SELECT event_sequence, event_json, previous_cursor FROM scoped \
               WHERE event_sequence > $1 ORDER BY event_sequence LIMIT $5",
        )
        .bind(after_sequence)
        .bind(scope.venue.as_str())
        .bind(scope.mode.as_str())
        .bind(&scope.trading_account_id)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        rows.into_iter()
            .map(|row| {
                let sequence: i64 = row.try_get("event_sequence").map_err(database_error)?;
                let previous_cursor: i64 =
                    row.try_get("previous_cursor").map_err(database_error)?;
                let notification: UiEventNotification =
                    decode(row.try_get("event_json").map_err(database_error)?)?;
                let event = UiEventEnvelope::from_notification(
                    notification,
                    u64::try_from(sequence).map_err(|_| RepositoryError::CorruptData)?,
                    u64::try_from(previous_cursor).map_err(|_| RepositoryError::CorruptData)?,
                )
                .map_err(|_| RepositoryError::CorruptData)?;
                Ok(StoredEvent { sequence, event })
            })
            .collect()
    }

    async fn has_current_strategy_scope(
        &self,
        command: &ControlCommandRequest,
    ) -> Result<bool, RepositoryError> {
        command
            .validate()
            .map_err(|_| RepositoryError::CorruptData)?;
        let row = sqlx::query(
            "SELECT 1 FROM venue_control_strategy_scopes \
             WHERE instance_id = $1 AND venue = $2 AND mode = $3 \
               AND trading_account_id = $4 AND symbol = $5 AND config_epoch = $6",
        )
        .bind(&command.instance_id)
        .bind(command.venue.as_str())
        .bind(command.mode.as_str())
        .bind(&command.trading_account_id)
        .bind(command.symbol.to_string())
        .bind(to_i64(command.expected_config_epoch)?)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?;
        Ok(row.is_some())
    }

    async fn has_current_account_scope(
        &self,
        venue: VenueId,
        mode: GatewayMode,
        trading_account_id: &str,
    ) -> Result<bool, RepositoryError> {
        if mode != GatewayMode::Live
            || !venue_domain::is_canonical_trading_account_id(trading_account_id)
        {
            return Err(RepositoryError::CorruptData);
        }
        let row = sqlx::query(
            "SELECT 1 FROM venue_control_strategy_scopes \
             WHERE venue = $1 AND mode = $2 AND trading_account_id = $3 LIMIT 1",
        )
        .bind(venue.as_str())
        .bind(mode.as_str())
        .bind(trading_account_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?;
        Ok(row.is_some())
    }
}

pub(crate) async fn insert_ui_event(
    transaction: &mut Transaction<'_, Postgres>,
    observed_ms: u64,
    event_type: UiEventKind,
    scope: UiAccountScope,
) -> Result<i64, RepositoryError> {
    let event = UiEventNotification {
        schema_version: CONTROL_SCHEMA_VERSION,
        event_type,
        scope,
        observed_ms,
    };
    event.validate().map_err(|_| RepositoryError::CorruptData)?;
    let row = sqlx::query(
        "INSERT INTO venue_control_events (observed_ms, event_json) VALUES ($1, $2) \
         RETURNING event_sequence",
    )
    .bind(to_i64(observed_ms)?)
    .bind(encode(&event)?)
    .fetch_one(&mut **transaction)
    .await
    .map_err(database_error)?;
    row.try_get("event_sequence").map_err(database_error)
}

fn command_scope(command: &ControlCommandRequest) -> UiAccountScope {
    UiAccountScope {
        venue: command.venue,
        mode: command.mode,
        trading_account_id: command.trading_account_id.clone(),
    }
}

fn fact_scope(binding: &ExecutionFactBinding) -> UiAccountScope {
    UiAccountScope {
        venue: binding.venue,
        mode: binding.mode,
        trading_account_id: binding.trading_account_id.clone(),
    }
}

fn push_unique_scope(scopes: &mut Vec<UiAccountScope>, scope: UiAccountScope) {
    if !scopes.contains(&scope) {
        scopes.push(scope);
    }
}

fn execution_fact_scopes(facts: &ExecutionFactsSnapshot) -> Vec<UiAccountScope> {
    let mut scopes = Vec::new();
    for binding in facts
        .orders
        .iter()
        .map(|fact| &fact.binding)
        .chain(facts.positions.iter().map(|fact| &fact.binding))
        .chain(facts.fills.iter().map(|fact| &fact.binding))
        .chain(facts.reconciliation.iter().map(|fact| &fact.binding))
        .chain(facts.copy_ledger.iter().map(|fact| &fact.binding))
        .chain(facts.drift.iter().map(|fact| &fact.binding))
        .chain(facts.execution.iter().map(|fact| &fact.binding))
    {
        push_unique_scope(&mut scopes, fact_scope(binding));
    }
    for fact in &facts.risk {
        push_unique_scope(
            &mut scopes,
            UiAccountScope {
                venue: fact.venue,
                mode: fact.mode,
                trading_account_id: fact.trading_account_id.clone(),
            },
        );
    }
    for fact in &facts.health {
        push_unique_scope(
            &mut scopes,
            UiAccountScope {
                venue: fact.venue,
                mode: fact.mode,
                trading_account_id: fact.trading_account_id.clone(),
            },
        );
    }
    scopes
}

fn encode<T: serde::Serialize>(value: &T) -> Result<serde_json::Value, RepositoryError> {
    serde_json::to_value(value).map_err(|_| RepositoryError::CorruptData)
}

fn decode<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> Result<T, RepositoryError> {
    serde_json::from_value(value).map_err(|_| RepositoryError::CorruptData)
}

fn to_i64(value: u64) -> Result<i64, RepositoryError> {
    i64::try_from(value).map_err(|_| RepositoryError::NumericRange)
}

const fn command_state(state: CommandState) -> &'static str {
    match state {
        CommandState::Accepted => "accepted",
        CommandState::Applied => "applied",
        CommandState::Rejected => "rejected",
        CommandState::Unknown => "unknown",
    }
}

fn database_error(_: sqlx::Error) -> RepositoryError {
    RepositoryError::Database
}

#[cfg(test)]
mod migration_tests {
    use super::MIGRATION_0014;

    #[test]
    fn manual_trade_migration_only_expands_the_command_action_constraint() {
        assert!(MIGRATION_0014.contains("'TRADE'"));
        assert!(MIGRATION_0014.contains("venue_control_command_action_v2"));
        for forbidden in ["execution_writer", "risk_permit", "command_wal"] {
            assert!(!MIGRATION_0014.contains(forbidden));
        }
    }
}
