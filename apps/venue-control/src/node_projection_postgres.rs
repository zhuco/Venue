//! Account-scoped projection merge. Cursor, read models, scopes and UI events commit together.

use serde::{Serialize, de::DeserializeOwned};
use sqlx::{PgPool, Row};
use venue_control_protocol::{
    AccountDeliveryBinding, CONTROL_SCHEMA_VERSION, ConnectionState, ControlSnapshot,
    ExecutionFactsSnapshot, GatewayMode, HealthState, NodeProjectionEnvelope, UiAccountScope,
    UiEventKind, UiEventNotification, VenueId,
};

use crate::{RepositoryError, SnapshotStoreResult};

pub(crate) async fn merge(
    pool: &PgPool,
    projection: &NodeProjectionEnvelope,
) -> Result<SnapshotStoreResult, RepositoryError> {
    projection
        .validate()
        .map_err(|_| RepositoryError::CorruptData)?;
    // Product relations, public markets and the legacy ledger are not account-node-owned.
    if !projection.snapshot.copy_relations.is_empty()
        || !projection.snapshot.markets.is_empty()
        || !projection.snapshot.ledger.is_empty()
    {
        return Err(RepositoryError::CorruptData);
    }
    let binding = &projection.binding;
    let mut transaction = pool.begin().await.map_err(database_error)?;
    // Same order as standalone writers; one network round trip, no change to lock semantics.
    sqlx::query("WITH snapshot_fence AS MATERIALIZED (SELECT pg_advisory_xact_lock(834766813558209236)) SELECT pg_advisory_xact_lock(834766813558209237) FROM snapshot_fence")
        .execute(&mut *transaction).await.map_err(database_error)?;
    let row = sqlx::query(
        "WITH prior AS MATERIALIZED (SELECT * FROM venue_account_node_projection_inbox \
          WHERE venue=$1 AND mode='LIVE' AND trading_account_id=$2) \
         SELECT (SELECT snapshot_json FROM venue_control_snapshots WHERE singleton) AS snapshot, \
         (SELECT facts_json FROM venue_control_execution_facts WHERE singleton) AS facts, \
         (SELECT jsonb_agg(envelope_json) FROM prior) AS prior, \
         (SELECT bool_and(COALESCE( \
          to_jsonb(venue)=envelope_json#>'{binding,venue}' \
          AND to_jsonb(mode)=envelope_json#>'{binding,mode}' \
          AND to_jsonb(trading_account_id)=envelope_json#>'{binding,trading_account_id}' \
          AND to_jsonb(instance_id)=envelope_json#>'{binding,instance_id}' \
          AND to_jsonb(node_id)=envelope_json->'node_id' \
          AND to_jsonb(node_generation)=envelope_json->'node_generation' \
          AND to_jsonb(projection_sequence)=envelope_json->'sequence' \
          AND to_jsonb(ARRAY(SELECT get_byte(projection_digest,i) \
             FROM generate_series(0,octet_length(projection_digest)-1) i))=envelope_json->'digest', \
          FALSE)) FROM prior) AS prior_consistent",
    )
    .bind(binding.venue.as_str())
    .bind(&binding.trading_account_id)
    .fetch_one(&mut *transaction)
    .await
    .map_err(database_error)?;
    if row
        .try_get::<Option<bool>, _>("prior_consistent")
        .map_err(database_error)?
        == Some(false)
    {
        return Err(RepositoryError::CorruptData);
    }
    let prior: Vec<NodeProjectionEnvelope> = row
        .try_get::<Option<serde_json::Value>, _>("prior")
        .map_err(database_error)?
        .map(decode)
        .transpose()?
        .unwrap_or_default();
    let same_node = prior.iter().find(|old| {
        old.node_id == projection.node_id && old.binding.instance_id == binding.instance_id
    });
    if validate_cursor(same_node, projection)? {
        transaction.rollback().await.map_err(database_error)?;
        return Ok(SnapshotStoreResult::Unchanged);
    }
    // A fresh node name must not replace newer evidence from the account's previous process.
    for old in &prior {
        old.validate().map_err(|_| RepositoryError::CorruptData)?;
        let old_account = old
            .snapshot
            .accounts
            .first()
            .ok_or(RepositoryError::CorruptData)?;
        let new_account = projection
            .snapshot
            .accounts
            .first()
            .ok_or(RepositoryError::CorruptData)?;
        if old.binding.instance_id == binding.instance_id
            && (old.snapshot.generated_ms > projection.snapshot.generated_ms
                || old_account.private_generation > new_account.private_generation
                || old.binding.config_epoch > binding.config_epoch
                || (old.binding.config_epoch == binding.config_epoch
                    && old.binding.symbol != binding.symbol))
        {
            return Err(RepositoryError::SnapshotConflict);
        }
    }
    let current_snapshot = row
        .try_get::<Option<serde_json::Value>, _>("snapshot")
        .map_err(database_error)?
        .map(decode)
        .transpose()?;
    let current_facts = row
        .try_get::<Option<serde_json::Value>, _>("facts")
        .map_err(database_error)?
        .map(decode)
        .transpose()?;
    let snapshot = merge_snapshot(current_snapshot, projection)?;
    let facts = merge_facts(current_facts, projection)?;
    for evidence in &projection.copy_execution_evidence {
        crate::copy_postgres::execution_projection::record_evidence_in_transaction(
            &mut transaction,
            evidence,
        )
        .await
        .map_err(|error| match error {
            crate::CopyRepositoryError::Database => RepositoryError::Database,
            crate::CopyRepositoryError::NumericRange => RepositoryError::NumericRange,
            crate::CopyRepositoryError::CorruptData => RepositoryError::CorruptData,
            _ => RepositoryError::ReplayConflict,
        })?;
    }
    let scope = UiAccountScope {
        venue: binding.venue,
        mode: binding.mode,
        trading_account_id: binding.trading_account_id.clone(),
    };
    let events = [UiEventKind::Snapshot, UiEventKind::ExecutionFacts].map(|event_type| {
        UiEventNotification {
            schema_version: CONTROL_SCHEMA_VERSION,
            event_type,
            scope: scope.clone(),
            observed_ms: projection.snapshot.generated_ms,
        }
    });
    for event in &events {
        event.validate().map_err(|_| RepositoryError::CorruptData)?;
    }
    // Batch independent writes inside the same transaction. Scope deletion is limited to
    // removed instances of this account; retained instance IDs are upserted, never delete/reinserted.
    let event_sequence: i64 = sqlx::query_scalar(
        "WITH saved_snapshot AS ( \
           INSERT INTO venue_control_snapshots VALUES (TRUE,$1,$2) \
           ON CONFLICT(singleton) DO UPDATE SET generated_ms=EXCLUDED.generated_ms,snapshot_json=EXCLUDED.snapshot_json \
         ), saved_facts AS ( \
           INSERT INTO venue_control_execution_facts VALUES (TRUE,$3,$4) \
           ON CONFLICT(singleton) DO UPDATE SET generated_ms=EXCLUDED.generated_ms,facts_json=EXCLUDED.facts_json \
         ), removed_scopes AS ( \
           DELETE FROM venue_control_strategy_scopes s WHERE venue=$5 AND mode='LIVE' AND trading_account_id=$6 AND instance_id=$15 \
           AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements($7) x WHERE x->>'instance_id'=s.instance_id) \
         ), saved_scopes AS ( \
           INSERT INTO venue_control_strategy_scopes \
           SELECT instance_id,venue,mode,trading_account_id,symbol,config_epoch,$8 \
           FROM jsonb_to_recordset($7) AS x(instance_id text,venue text,mode text,trading_account_id text,symbol text,config_epoch bigint) \
           ON CONFLICT(instance_id) DO UPDATE SET venue=EXCLUDED.venue,mode=EXCLUDED.mode, \
           trading_account_id=EXCLUDED.trading_account_id,symbol=EXCLUDED.symbol,config_epoch=EXCLUDED.config_epoch,snapshot_generated_ms=EXCLUDED.snapshot_generated_ms \
         ), saved_cursor AS ( \
           INSERT INTO venue_account_node_projection_inbox (venue,mode,trading_account_id,node_id,node_generation,projection_sequence,projection_digest,envelope_json,instance_id) \
           VALUES ($5,'LIVE',$6,$9,$10,$11,$12,$13,$15) \
           ON CONFLICT(venue,mode,trading_account_id,node_id,instance_id) DO UPDATE SET node_generation=EXCLUDED.node_generation, \
           projection_sequence=EXCLUDED.projection_sequence,projection_digest=EXCLUDED.projection_digest,envelope_json=EXCLUDED.envelope_json \
         ), saved_events AS ( \
           INSERT INTO venue_control_events(observed_ms,event_json) SELECT $8,value FROM jsonb_array_elements($14) \
           RETURNING event_sequence \
         ) SELECT max(event_sequence) FROM saved_events",
    )
    .bind(to_i64(snapshot.generated_ms)?).bind(encode(&snapshot)?)
    .bind(to_i64(facts.generated_ms)?).bind(encode(&facts)?)
    .bind(binding.venue.as_str()).bind(&binding.trading_account_id)
    .bind(encode(&projection.snapshot.strategies)?).bind(to_i64(projection.snapshot.generated_ms)?)
    .bind(&projection.node_id).bind(to_i64(projection.node_generation)?)
    .bind(to_i64(projection.sequence)?).bind(projection.digest.to_vec()).bind(encode(projection)?)
    .bind(encode(&events)?)
    .bind(&binding.instance_id)
    .fetch_one(&mut *transaction).await.map_err(database_error)?;
    transaction.commit().await.map_err(database_error)?;
    Ok(SnapshotStoreResult::Inserted { event_sequence })
}

fn validate_cursor(
    old: Option<&NodeProjectionEnvelope>,
    new: &NodeProjectionEnvelope,
) -> Result<bool, RepositoryError> {
    let Some(old) = old else {
        return if new.sequence == 1 && new.previous_digest == [0; 32] {
            Ok(false)
        } else {
            Err(RepositoryError::ReplayConflict)
        };
    };
    old.validate().map_err(|_| RepositoryError::CorruptData)?;
    if old.node_generation > new.node_generation
        || (old.node_generation == new.node_generation && old.sequence > new.sequence)
    {
        return Err(RepositoryError::SnapshotConflict);
    }
    if old.node_generation == new.node_generation && old.sequence == new.sequence {
        // Caller-supplied digest equality alone does not prove identical content.
        return if old == new {
            Ok(true)
        } else {
            Err(RepositoryError::ReplayConflict)
        };
    }
    if old.node_generation == new.node_generation {
        if old.sequence.checked_add(1) != Some(new.sequence) || new.previous_digest != old.digest {
            return Err(RepositoryError::ReplayConflict);
        }
    } else if new.sequence != 1 || new.previous_digest != [0; 32] {
        return Err(RepositoryError::ReplayConflict);
    }
    Ok(false)
}

fn owns(
    binding: &AccountDeliveryBinding,
    venue: VenueId,
    mode: GatewayMode,
    account: &str,
) -> bool {
    binding.venue == venue && binding.mode == mode && binding.trading_account_id == account
}

fn merge_snapshot(
    current: Option<ControlSnapshot>,
    projection: &NodeProjectionEnvelope,
) -> Result<ControlSnapshot, RepositoryError> {
    let incoming = &projection.snapshot;
    let Some(mut merged) = current else {
        return Ok(incoming.clone());
    };
    let binding = &projection.binding;
    if merged.strategies.iter().any(|old| {
        old.instance_id == binding.instance_id
            && owns(binding, old.venue, old.mode, &old.trading_account_id)
            && (old.config_epoch > binding.config_epoch
                || (old.config_epoch == binding.config_epoch && old.symbol != binding.symbol))
    }) {
        return Err(RepositoryError::SnapshotConflict);
    }
    let new = incoming
        .accounts
        .first()
        .ok_or(RepositoryError::CorruptData)?;
    let keep_newer_account = merged.accounts.iter().any(|old| {
        owns(binding, old.venue, old.mode, &old.trading_account_id)
            && (old.private_generation > new.private_generation
                || old.last_reconciled_ms > new.last_reconciled_ms)
    });
    if !keep_newer_account {
        merged
            .accounts
            .retain(|x| !owns(binding, x.venue, x.mode, &x.trading_account_id));
        merged.accounts.extend(incoming.accounts.clone());
    }
    merged.strategies.retain(|x| {
        !owns(binding, x.venue, x.mode, &x.trading_account_id)
            || x.instance_id != binding.instance_id
    });
    merged.strategies.extend(incoming.strategies.clone());
    merged.generated_ms = merged.generated_ms.max(incoming.generated_ms);
    merged.connection = if merged
        .accounts
        .iter()
        .all(|x| x.health == HealthState::Healthy)
    {
        ConnectionState::Live
    } else if merged
        .accounts
        .iter()
        .all(|x| x.health == HealthState::Recovering)
    {
        ConnectionState::Connecting
    } else {
        ConnectionState::Degraded
    };
    merged
        .validate()
        .map_err(|_| RepositoryError::SnapshotConflict)?;
    Ok(merged)
}

fn merge_facts(
    current: Option<ExecutionFactsSnapshot>,
    projection: &NodeProjectionEnvelope,
) -> Result<ExecutionFactsSnapshot, RepositoryError> {
    let Some(mut merged) = current else {
        return Ok(projection.facts.clone());
    };
    let binding = &projection.binding;
    macro_rules! merge_bound {
        ($($field:ident),+ $(,)?) => { $(
            merged.$field.retain(|x| !owns(binding, x.binding.venue, x.binding.mode, &x.binding.trading_account_id)
                || x.binding.instance_id != binding.instance_id);
            merged.$field.extend(projection.facts.$field.clone());
        )+ };
    }
    merge_bound!(
        orders,
        positions,
        fills,
        reconciliation,
        copy_ledger,
        drift,
        execution
    );
    let generation = projection
        .snapshot
        .accounts
        .first()
        .ok_or(RepositoryError::CorruptData)?
        .private_generation;
    let keep_risk = merged.risk.iter().any(|x| {
        owns(binding, x.venue, x.mode, &x.trading_account_id)
            && (x.signed_generation > generation || x.observed_ms > projection.facts.generated_ms)
    });
    if !keep_risk && !projection.facts.risk.is_empty() {
        merged
            .risk
            .retain(|x| !owns(binding, x.venue, x.mode, &x.trading_account_id));
        merged.risk.extend(projection.facts.risk.clone());
    }
    let keep_health = merged.health.iter().any(|x| {
        owns(binding, x.venue, x.mode, &x.trading_account_id)
            && (x.private_generation > generation || x.observed_ms > projection.facts.generated_ms)
    });
    if !keep_health && !projection.facts.health.is_empty() {
        merged
            .health
            .retain(|x| !owns(binding, x.venue, x.mode, &x.trading_account_id));
        merged.health.extend(projection.facts.health.clone());
    }
    merged.generated_ms = merged.generated_ms.max(projection.facts.generated_ms);
    merged
        .validate()
        .map_err(|_| RepositoryError::SnapshotConflict)?;
    Ok(merged)
}

fn encode(value: &impl Serialize) -> Result<serde_json::Value, RepositoryError> {
    serde_json::to_value(value).map_err(|_| RepositoryError::CorruptData)
}
fn decode<T: DeserializeOwned>(value: serde_json::Value) -> Result<T, RepositoryError> {
    serde_json::from_value(value).map_err(|_| RepositoryError::CorruptData)
}
fn to_i64(value: u64) -> Result<i64, RepositoryError> {
    i64::try_from(value).map_err(|_| RepositoryError::NumericRange)
}
fn database_error(_: sqlx::Error) -> RepositoryError {
    RepositoryError::Database
}
