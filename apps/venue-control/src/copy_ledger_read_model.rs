//! Durable Copy ledger/drift rows overlay transient Node facts at the existing read-model edge.

use sqlx::{Postgres, Row, Transaction};
use venue_control_protocol::{
    CopyDriftFact, CopyLedgerFact, ExecutionFactBinding, ExecutionFactsSnapshot, GatewayMode,
    VenueId,
};
use venue_copy::LedgerEntry;

use crate::{CopyDriftProjection, CopyJob, CopyRepositoryError, RepositoryError};

/// Repair delivery state is part of the execution read model, not the historical drift row.
/// Publish the existing refetch event in the same transaction as its actual durable change.
pub(crate) async fn notify_repair_change(
    transaction: &mut Transaction<'_, Postgres>,
    job: &CopyJob,
    observed_ms: u64,
) -> Result<(), CopyRepositoryError> {
    if job
        .semantic_job
        .pointer("/leader_intent/drift_repair")
        .is_none()
    {
        return Ok(());
    }
    crate::postgres::insert_ui_event(
        transaction,
        observed_ms,
        venue_control_protocol::UiEventKind::ExecutionFacts,
        venue_control_protocol::UiAccountScope {
            venue: job.scope.venue,
            mode: GatewayMode::Live,
            trading_account_id: job.scope.trading_account_id.clone(),
        },
    )
    .await
    .map_err(|error| match error {
        RepositoryError::Database => CopyRepositoryError::Database,
        RepositoryError::NumericRange => CopyRepositoryError::NumericRange,
        _ => CopyRepositoryError::CorruptData,
    })?;
    Ok(())
}

pub(crate) async fn overlay_durable_copy_facts(
    transaction: &mut Transaction<'_, Postgres>,
    facts: &mut ExecutionFactsSnapshot,
) -> Result<(), RepositoryError> {
    let ledger = load_latest_ledger(transaction).await?;
    let drift = load_latest_drift(transaction).await?;
    for binding in ledger
        .iter()
        .map(|fact| &fact.binding)
        .chain(drift.iter().map(|fact| &fact.binding))
    {
        facts
            .copy_ledger
            .retain(|existing| !same_binding(&existing.binding, binding));
        facts
            .drift
            .retain(|existing| !same_binding(&existing.binding, binding));
    }
    facts.copy_ledger.extend(ledger);
    facts.drift.extend(drift);
    facts.generated_ms = facts.generated_ms.max(
        facts
            .copy_ledger
            .iter()
            .map(|fact| fact.observed_ms)
            .chain(facts.drift.iter().map(|fact| fact.observed_ms))
            .max()
            .unwrap_or(facts.generated_ms),
    );
    facts.validate().map_err(|_| RepositoryError::CorruptData)
}

async fn load_latest_ledger(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Vec<CopyLedgerFact>, RepositoryError> {
    let rows = sqlx::query(
        "WITH latest AS ( \
           SELECT DISTINCT ON (venue, trading_account_id, follower_binding_id) \
             venue, trading_account_id, follower_binding_id, ledger_sequence, entry_json, job_id, projected_at_ms \
           FROM venue_copy_ledger ORDER BY venue, trading_account_id, follower_binding_id, ledger_sequence DESC \
         ) \
         SELECT l.venue, l.trading_account_id, l.follower_binding_id, l.ledger_sequence, l.entry_json, \
                l.job_id, l.projected_at_ms, j.job_json, d.symbol, d.instance_id, d.config_epoch \
         FROM latest l JOIN venue_copy_jobs j ON j.job_id = l.job_id \
         JOIN venue_account_deliveries d ON d.delivery_id = ('copy:' || l.job_id) \
           AND d.source_kind = 'copy_semantic_job' AND d.source_id = l.job_id \
           AND d.venue = l.venue AND d.mode = 'LIVE' AND d.trading_account_id = l.trading_account_id \
         ORDER BY l.venue, l.trading_account_id, l.follower_binding_id",
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(database_error)?;
    rows.into_iter()
        .map(|row| {
            let entry: LedgerEntry = decode(row.try_get("entry_json").map_err(database_error)?)?;
            let job: CopyJob = decode(row.try_get("job_json").map_err(database_error)?)?;
            let binding = binding_for_row(&row, &job, &entry.binding)?;
            let sequence = from_i64(row.try_get("ledger_sequence").map_err(database_error)?)?;
            let projected_at_ms =
                from_i64(row.try_get("projected_at_ms").map_err(database_error)?)?;
            let job_id: String = row.try_get("job_id").map_err(database_error)?;
            if entry.binding.follower_binding_id.to_string()
                != row
                    .try_get::<String, _>("follower_binding_id")
                    .map_err(database_error)?
                || entry.source_id.to_string() != job_id
                || entry.source_id != job.identities.job_id
                || entry.sequence != sequence
                || projected_at_ms == 0
            {
                return Err(RepositoryError::CorruptData);
            }
            Ok(CopyLedgerFact {
                relation_id: entry.binding.relation.relation_id.to_string(),
                relation_revision: entry.binding.relation.revision,
                job_id,
                binding,
                ledger_sequence: Some(sequence),
                managed_exposure: entry.managed_exposure.value,
                signed_generation: entry.generation,
                observed_ms: projected_at_ms,
                fact_digest: entry.fact_digest,
            })
        })
        .collect()
}

async fn load_latest_drift(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Vec<CopyDriftFact>, RepositoryError> {
    let rows = sqlx::query(
        "SELECT p.venue, p.trading_account_id, p.follower_binding_id, p.projection_json, p.projected_at_ms, \
                EXISTS(SELECT 1 FROM venue_copy_jobs repair \
                  JOIN venue_copy_delivery_outbox repair_outbox USING(job_id) \
                  WHERE repair.venue=p.venue AND repair.mode=p.mode \
                   AND repair.trading_account_id=p.trading_account_id \
                   AND repair.job_json#>'{semantic_job,leader_intent,drift_repair,request,supersedes_job_id}' \
                       =j.job_json#>'{identities,job_id}' \
                   AND repair_outbox.delivery_state IN ('pending','claimed','reconciliation_required') \
                   AND NOT EXISTS(SELECT 1 FROM venue_copy_ledger done WHERE done.job_id=repair.job_id) \
                   AND NOT EXISTS(SELECT 1 FROM venue_copy_delivery_receipts rejected \
                       WHERE rejected.job_id=repair.job_id AND rejected.status='rejected')) AS repair_pending, \
                j.job_json, d.symbol, d.instance_id, d.config_epoch \
         FROM venue_copy_drift_projections p JOIN venue_copy_jobs j ON j.job_id = p.source_job_id \
         JOIN venue_account_deliveries d ON d.delivery_id = ('copy:' || p.source_job_id) \
           AND d.source_kind = 'copy_semantic_job' AND d.source_id = p.source_job_id \
           AND d.venue = p.venue AND d.mode = 'LIVE' AND d.trading_account_id = p.trading_account_id \
         ORDER BY p.venue, p.trading_account_id, p.follower_binding_id",
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(database_error)?;
    rows.into_iter()
        .map(|row| {
            let projection: CopyDriftProjection =
                decode(row.try_get("projection_json").map_err(database_error)?)?;
            let job: CopyJob = decode(row.try_get("job_json").map_err(database_error)?)?;
            let binding = binding_for_row(&row, &job, &projection.position.binding)?;
            let projected_at_ms =
                from_i64(row.try_get("projected_at_ms").map_err(database_error)?)?;
            if projection.source_job_id != job.identities.job_id
                || projection.position.binding.follower_binding_id.to_string()
                    != row
                        .try_get::<String, _>("follower_binding_id")
                        .map_err(database_error)?
                || projection.projected_at_ms != projected_at_ms
            {
                return Err(RepositoryError::CorruptData);
            }
            Ok(CopyDriftFact {
                relation_id: projection.position.binding.relation.relation_id.to_string(),
                relation_revision: projection.position.binding.relation.revision,
                job_id: projection.source_job_id.to_string(),
                binding,
                target_exposure: projection.target.target_exposure.value,
                actual_exposure: projection.position.exposure.value,
                // A historical candidate is not a delivered repair. Only a separately durable,
                // still-unsettled semantic job can appear as pending work in the UI.
                repair_pending: row.try_get("repair_pending").map_err(database_error)?,
                signed_generation: projection.position.generation,
                observed_ms: projected_at_ms,
                fact_digest: projection.position.fact_digest,
            })
        })
        .collect()
}

fn binding_for_row(
    row: &sqlx::postgres::PgRow,
    job: &CopyJob,
    binding: &venue_copy::DeliveryBinding,
) -> Result<ExecutionFactBinding, RepositoryError> {
    let venue: String = row.try_get("venue").map_err(database_error)?;
    let account: String = row.try_get("trading_account_id").map_err(database_error)?;
    let symbol: String = row.try_get("symbol").map_err(database_error)?;
    let instance_id: String = row.try_get("instance_id").map_err(database_error)?;
    let config_epoch = from_i64(row.try_get("config_epoch").map_err(database_error)?)?;
    let venue = venue
        .parse::<VenueId>()
        .map_err(|_| RepositoryError::CorruptData)?;
    if venue != job.scope.venue
        || account != job.scope.trading_account_id
        || binding != &job.manifest.binding
        || account != binding.account_id
        || symbol != binding.instrument.symbol.to_string()
        || instance_id != binding.follower_instance_id
        || config_epoch == 0
    {
        return Err(RepositoryError::CorruptData);
    }
    Ok(ExecutionFactBinding {
        venue,
        mode: GatewayMode::Live,
        trading_account_id: account,
        symbol: binding.instrument.symbol.clone(),
        instance_id,
        config_epoch,
    })
}

fn same_binding(left: &ExecutionFactBinding, right: &ExecutionFactBinding) -> bool {
    left.venue == right.venue
        && left.mode == right.mode
        && left.trading_account_id == right.trading_account_id
        && left.symbol == right.symbol
        && left.instance_id == right.instance_id
        && left.config_epoch == right.config_epoch
}

fn decode<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> Result<T, RepositoryError> {
    serde_json::from_value(value).map_err(|_| RepositoryError::CorruptData)
}

fn from_i64(value: i64) -> Result<u64, RepositoryError> {
    u64::try_from(value).map_err(|_| RepositoryError::NumericRange)
}

fn database_error(_: sqlx::Error) -> RepositoryError {
    RepositoryError::Database
}
