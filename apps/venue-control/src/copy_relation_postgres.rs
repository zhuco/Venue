use sqlx::Row;
use venue_control_protocol::{
    CONTROL_SCHEMA_VERSION, ControlSnapshot, CopyRelationBinding, CopyRelationCandidate,
    CopyRelationConfig, CopyRelationReceipt, CopyRelationReceiptState, CopyRelationRecord,
    CopyRelationUpsertRequest, UiAccountScope, UiEventKind,
};

use crate::{
    CopyRelationRepository, CopyRelationRepositoryError, PgControlRepository,
    postgres::insert_ui_event,
};

pub const MIGRATION_0006: &str = include_str!("../migrations/0006_copy_relation_configs.sql");
pub const MIGRATION_0010: &str = include_str!("../migrations/0010_copy_relation_request_id.sql");

impl CopyRelationRepository for PgControlRepository {
    async fn upsert_copy_relation(
        &self,
        request: &CopyRelationUpsertRequest,
        observed_ms: u64,
    ) -> Result<CopyRelationReceipt, CopyRelationRepositoryError> {
        request
            .validate()
            .map_err(|_| CopyRelationRepositoryError::InvalidData)?;
        if observed_ms == 0 {
            return Err(CopyRelationRepositoryError::InvalidData);
        }
        let mut transaction = self.pool().begin().await.map_err(database_error)?;
        let relation_id = &request.relation.relation_id;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(&request.request_id)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
        let replay = sqlx::query(
            "SELECT relation_id, revision, action, config_json, observed_at_ms \
             FROM venue_copy_relation_audit WHERE request_id = $1 FOR SHARE",
        )
        .bind(&request.request_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?;
        if let Some(row) = replay {
            let durable: CopyRelationConfig =
                decode(row.try_get("config_json").map_err(database_error)?)?;
            if row
                .try_get::<String, _>("relation_id")
                .map_err(database_error)?
                != *relation_id
                || durable != request.relation
            {
                return Err(CopyRelationRepositoryError::Conflict);
            }
            let action: String = row.try_get("action").map_err(database_error)?;
            if !matches!(
                action.as_str(),
                "created" | "updated" | "paused" | "resumed"
            ) {
                return Err(CopyRelationRepositoryError::CorruptData);
            }
            let receipt = CopyRelationReceipt {
                schema_version: CONTROL_SCHEMA_VERSION,
                relation_id: relation_id.clone(),
                revision: from_i64(row.try_get("revision").map_err(database_error)?)?,
                state: CopyRelationReceiptState::Existing,
                observed_ms: from_i64(row.try_get("observed_at_ms").map_err(database_error)?)?,
            };
            receipt
                .validate()
                .map_err(|_| CopyRelationRepositoryError::CorruptData)?;
            transaction.commit().await.map_err(database_error)?;
            return Ok(receipt);
        }
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(relation_id)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
        for endpoint in [&request.relation.leader, &request.relation.follower] {
            let scope = sqlx::query(
                "SELECT 1 FROM venue_control_strategy_scopes \
                 WHERE venue = $1 AND mode = 'LIVE' AND trading_account_id = $2 AND symbol = $3 \
                   AND instance_id = $4 FOR SHARE",
            )
            .bind(endpoint.venue.as_str())
            .bind(&endpoint.trading_account_id)
            .bind(endpoint.symbol.to_string())
            .bind(&endpoint.instance_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(database_error)?;
            if scope.is_none() {
                return Err(CopyRelationRepositoryError::Conflict);
            }
        }
        let follower = &request.relation.follower;
        let follower_conflict = sqlx::query(
            "SELECT relation_id FROM venue_copy_relation_configs \
             WHERE follower_venue = $1 AND follower_account_id = $2 \
               AND follower_instance_id = $3 AND follower_symbol = $4 \
               AND relation_id <> $5 FOR SHARE",
        )
        .bind(follower.venue.as_str())
        .bind(&follower.trading_account_id)
        .bind(&follower.instance_id)
        .bind(follower.symbol.to_string())
        .bind(relation_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?;
        if follower_conflict.is_some() {
            return Err(CopyRelationRepositoryError::Conflict);
        }
        let current = sqlx::query(
            "SELECT revision, config_json FROM venue_copy_relation_configs \
             WHERE relation_id = $1 FOR UPDATE",
        )
        .bind(relation_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?;
        let config_json = encode(&request.relation)?;
        let (revision, state) = if let Some(row) = current {
            let revision = from_i64(row.try_get("revision").map_err(database_error)?)?;
            let durable: CopyRelationConfig =
                decode(row.try_get("config_json").map_err(database_error)?)?;
            if durable == request.relation {
                (revision, CopyRelationReceiptState::Existing)
            } else if request.expected_revision != Some(revision) {
                return Err(CopyRelationRepositoryError::Conflict);
            } else {
                let next = revision
                    .checked_add(1)
                    .ok_or(CopyRelationRepositoryError::Conflict)?;
                sqlx::query(
                    "UPDATE venue_copy_relation_configs SET revision = $2, leader_venue = $3, \
                     leader_mode = 'LIVE', leader_account_id = $4, leader_instance_id = $5, \
                     leader_symbol = $6, follower_venue = $7, follower_mode = 'LIVE', \
                     follower_account_id = $8, follower_instance_id = $9, follower_symbol = $10, \
                     lifecycle = $11, config_json = $12, updated_at_ms = $13 WHERE relation_id = $1",
                )
                .bind(relation_id)
                .bind(to_i64(next)?)
                .bind(request.relation.leader.venue.as_str())
                .bind(&request.relation.leader.trading_account_id)
                .bind(&request.relation.leader.instance_id)
                .bind(request.relation.leader.symbol.to_string())
                .bind(request.relation.follower.venue.as_str())
                .bind(&request.relation.follower.trading_account_id)
                .bind(&request.relation.follower.instance_id)
                .bind(request.relation.follower.symbol.to_string())
                .bind(lifecycle(&request.relation))
                .bind(config_json)
                .bind(to_i64(observed_ms)?)
                .execute(&mut *transaction)
                .await
                .map_err(database_error)?;
                (next, CopyRelationReceiptState::Updated)
            }
        } else {
            if request
                .expected_revision
                .is_some_and(|revision| revision != 0)
            {
                return Err(CopyRelationRepositoryError::Conflict);
            }
            sqlx::query(
                "INSERT INTO venue_copy_relation_configs \
                 (relation_id, revision, leader_venue, leader_mode, leader_account_id, \
                  leader_instance_id, leader_symbol, follower_venue, follower_mode, \
                  follower_account_id, follower_instance_id, follower_symbol, lifecycle, \
                  config_json, created_at_ms, updated_at_ms) \
                 VALUES ($1, 1, $2, 'LIVE', $3, $4, $5, $6, 'LIVE', $7, $8, $9, $10, $11, $12, $12)",
            )
            .bind(relation_id)
            .bind(request.relation.leader.venue.as_str())
            .bind(&request.relation.leader.trading_account_id)
            .bind(&request.relation.leader.instance_id)
            .bind(request.relation.leader.symbol.to_string())
            .bind(request.relation.follower.venue.as_str())
            .bind(&request.relation.follower.trading_account_id)
            .bind(&request.relation.follower.instance_id)
            .bind(request.relation.follower.symbol.to_string())
            .bind(lifecycle(&request.relation))
            .bind(config_json)
            .bind(to_i64(observed_ms)?)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
            (1, CopyRelationReceiptState::Created)
        };
        let receipt = CopyRelationReceipt {
            schema_version: CONTROL_SCHEMA_VERSION,
            relation_id: relation_id.clone(),
            revision,
            state,
            observed_ms,
        };
        receipt
            .validate()
            .map_err(|_| CopyRelationRepositoryError::CorruptData)?;
        if receipt.state != CopyRelationReceiptState::Existing {
            let action = match (receipt.state, request.relation.lifecycle) {
                (CopyRelationReceiptState::Created, _) => "created",
                (_, venue_control_protocol::CopyLifecyclePolicy::Paused) => "paused",
                (_, venue_control_protocol::CopyLifecyclePolicy::Active) => "resumed",
            };
            let policy_digest = request
                .relation
                .policy_digest()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            sqlx::query(
                "INSERT INTO venue_copy_relation_audit \
                 (relation_id, revision, action, policy_digest, config_json, observed_at_ms, request_id) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(&receipt.relation_id)
            .bind(to_i64(receipt.revision)?)
            .bind(action)
            .bind(policy_digest)
            .bind(encode(&request.relation)?)
            .bind(to_i64(observed_ms)?)
            .bind(&request.request_id)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
            for binding in [&request.relation.leader, &request.relation.follower] {
                insert_ui_event(
                    &mut transaction,
                    observed_ms,
                    UiEventKind::CopyRelation,
                    UiAccountScope {
                        venue: binding.venue,
                        mode: binding.mode,
                        trading_account_id: binding.trading_account_id.clone(),
                    },
                )
                .await
                .map_err(|_| CopyRelationRepositoryError::Database)?;
            }
        }
        transaction.commit().await.map_err(database_error)?;
        Ok(receipt)
    }

    async fn list_copy_relations(
        &self,
    ) -> Result<Vec<CopyRelationRecord>, CopyRelationRepositoryError> {
        let rows = sqlx::query(
            "SELECT relation_id, config_json, revision, leader_venue, leader_mode, leader_account_id, \
             leader_instance_id, leader_symbol, follower_venue, follower_mode, follower_account_id, \
             follower_instance_id, follower_symbol, lifecycle \
             FROM venue_copy_relation_configs ORDER BY relation_id",
        )
        .fetch_all(self.pool())
        .await
        .map_err(database_error)?;
        rows.into_iter()
            .map(|row| {
                let config: CopyRelationConfig =
                    decode(row.try_get("config_json").map_err(database_error)?)?;
                config
                    .validate()
                    .map_err(|_| CopyRelationRepositoryError::CorruptData)?;
                if row
                    .try_get::<String, _>("relation_id")
                    .map_err(database_error)?
                    != config.relation_id
                    || row
                        .try_get::<String, _>("leader_venue")
                        .map_err(database_error)?
                        != config.leader.venue.as_str()
                    || row
                        .try_get::<String, _>("leader_mode")
                        .map_err(database_error)?
                        != "LIVE"
                    || row
                        .try_get::<String, _>("leader_account_id")
                        .map_err(database_error)?
                        != config.leader.trading_account_id
                    || row
                        .try_get::<String, _>("leader_instance_id")
                        .map_err(database_error)?
                        != config.leader.instance_id
                    || row
                        .try_get::<String, _>("leader_symbol")
                        .map_err(database_error)?
                        != config.leader.symbol.to_string()
                    || row
                        .try_get::<String, _>("follower_venue")
                        .map_err(database_error)?
                        != config.follower.venue.as_str()
                    || row
                        .try_get::<String, _>("follower_mode")
                        .map_err(database_error)?
                        != "LIVE"
                    || row
                        .try_get::<String, _>("follower_account_id")
                        .map_err(database_error)?
                        != config.follower.trading_account_id
                    || row
                        .try_get::<String, _>("follower_instance_id")
                        .map_err(database_error)?
                        != config.follower.instance_id
                    || row
                        .try_get::<String, _>("follower_symbol")
                        .map_err(database_error)?
                        != config.follower.symbol.to_string()
                    || row
                        .try_get::<String, _>("lifecycle")
                        .map_err(database_error)?
                        != lifecycle(&config)
                {
                    return Err(CopyRelationRepositoryError::CorruptData);
                }
                let record = CopyRelationRecord {
                    relation: config,
                    revision: from_i64(row.try_get("revision").map_err(database_error)?)?,
                };
                record
                    .validate()
                    .map_err(|_| CopyRelationRepositoryError::CorruptData)?;
                Ok(record)
            })
            .collect()
    }

    async fn list_copy_relation_candidates(
        &self,
    ) -> Result<Vec<CopyRelationCandidate>, CopyRelationRepositoryError> {
        let row =
            sqlx::query("SELECT snapshot_json FROM venue_control_snapshots WHERE singleton = TRUE")
                .fetch_optional(self.pool())
                .await
                .map_err(database_error)?;
        let Some(row) = row else {
            return Ok(Vec::new());
        };
        let snapshot: ControlSnapshot =
            decode(row.try_get("snapshot_json").map_err(database_error)?)?;
        snapshot
            .validate()
            .map_err(|_| CopyRelationRepositoryError::CorruptData)?;
        snapshot
            .strategies
            .into_iter()
            .filter(|strategy| copy_candidate_kind(strategy.kind))
            .map(|strategy| {
                let candidate = CopyRelationCandidate {
                    binding: CopyRelationBinding {
                        venue: strategy.venue,
                        mode: strategy.mode,
                        trading_account_id: strategy.trading_account_id,
                        instance_id: strategy.instance_id,
                        symbol: strategy.symbol,
                    },
                    lifecycle: strategy.lifecycle,
                    config_epoch: strategy.config_epoch,
                };
                candidate
                    .validate()
                    .map_err(|_| CopyRelationRepositoryError::CorruptData)?;
                Ok(candidate)
            })
            .collect()
    }
}

fn copy_candidate_kind(kind: venue_control_protocol::StrategyKind) -> bool {
    kind != venue_control_protocol::StrategyKind::Manual
}

#[cfg(test)]
mod candidate_tests {
    use venue_control_protocol::StrategyKind;

    #[test]
    fn manual_terminal_actor_is_not_a_copy_relation_candidate() {
        assert!(!super::copy_candidate_kind(StrategyKind::Manual));
        for kind in [
            StrategyKind::Grid,
            StrategyKind::Scalping,
            StrategyKind::Copy,
        ] {
            assert!(super::copy_candidate_kind(kind));
        }
    }
}

fn lifecycle(config: &CopyRelationConfig) -> &'static str {
    match config.lifecycle {
        venue_control_protocol::CopyLifecyclePolicy::Active => "active",
        venue_control_protocol::CopyLifecyclePolicy::Paused => "paused",
    }
}

fn encode<T: serde::Serialize>(
    value: &T,
) -> Result<serde_json::Value, CopyRelationRepositoryError> {
    serde_json::to_value(value).map_err(|_| CopyRelationRepositoryError::CorruptData)
}

fn decode<T: serde::de::DeserializeOwned>(
    value: serde_json::Value,
) -> Result<T, CopyRelationRepositoryError> {
    serde_json::from_value(value).map_err(|_| CopyRelationRepositoryError::CorruptData)
}

fn to_i64(value: u64) -> Result<i64, CopyRelationRepositoryError> {
    i64::try_from(value).map_err(|_| CopyRelationRepositoryError::Conflict)
}

fn from_i64(value: i64) -> Result<u64, CopyRelationRepositoryError> {
    u64::try_from(value).map_err(|_| CopyRelationRepositoryError::CorruptData)
}

fn database_error(_: sqlx::Error) -> CopyRelationRepositoryError {
    CopyRelationRepositoryError::Database
}
