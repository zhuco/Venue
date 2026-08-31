//! A settled job may leave drift. Only a fresh paired observation can authorize a new semantic
//! target; the historical ledger is provenance, never a renewal of the old job or its child.

use super::*;
use crate::{CopyPlanningSnapshot, CopySemanticJob};
use venue_copy::{
    AuthoritativePositionSnapshot, DriftRepairPlanRequest, LedgerEntry, derive_copy_identities,
    plan_drift_repair,
};

pub(super) async fn from_reconciled_source(
    transaction: &mut Transaction<'_, Postgres>,
    prior: &CopyLeaderIntent,
    envelope: CopyLeaderEnvelope,
    follower: &CopyPlanningFact,
    now_ms: u64,
) -> Result<Option<CopyLeaderEnvelope>, CopyRepositoryError> {
    let identities = derive_copy_identities(&prior.identity_input)
        .map_err(|_| CopyRepositoryError::CorruptData)?;
    // Only the latest signed ledger for this exact job can be a repair predecessor. Rejected
    // or merely Accepted jobs have no ledger and cannot become automatic retries.
    let row = sqlx::query(
        "SELECT j.job_json, j.relation_id, j.relation_revision, j.policy_digest, \
         l.entry_json, l.ledger_sequence, l.generation, l.fact_digest, l.receipt_sequence, \
         d.projection_json, d.position_generation, d.projected_at_ms \
         FROM venue_copy_jobs j JOIN venue_copy_ledger l USING(job_id) \
         JOIN venue_copy_drift_projections d ON d.source_job_id=j.job_id \
          AND d.receipt_sequence=l.receipt_sequence AND d.venue=l.venue AND d.mode=l.mode \
          AND d.trading_account_id=l.trading_account_id \
          AND d.follower_binding_id=l.follower_binding_id \
         WHERE j.job_id=$1 AND j.intent_id=$2 AND j.venue=$3 AND j.mode='LIVE' \
          AND j.trading_account_id=$4 FOR SHARE OF j,l,d",
    )
    .bind(identities.job_id.to_string())
    .bind(prior.intent_id.to_string())
    .bind(envelope.scope.venue.as_str())
    .bind(&envelope.scope.trading_account_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database_error)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let job: CopyJob = decode(row.try_get("job_json").map_err(database_error)?)?;
    validate_job_relation_columns(&row, &job)?;
    let ledger: LedgerEntry = decode(row.try_get("entry_json").map_err(database_error)?)?;
    let projection: CopyDriftProjection =
        decode(row.try_get("projection_json").map_err(database_error)?)?;
    if job.identities != identities
        || job.intent_id != prior.intent_id
        || ledger.source_id != job.identities.job_id
        || ledger.binding != job.manifest.binding
        || ledger.sequence != from_i64(row.try_get("ledger_sequence").map_err(database_error)?)?
        || ledger.generation != from_i64(row.try_get("generation").map_err(database_error)?)?
        || ledger.fact_digest != digest(row.try_get("fact_digest").map_err(database_error)?)?
        || projection.receipt_sequence
            != from_i64(row.try_get("receipt_sequence").map_err(database_error)?)?
        || projection.position.generation
            != from_i64(row.try_get("position_generation").map_err(database_error)?)?
        || projection.projected_at_ms
            != from_i64(row.try_get("projected_at_ms").map_err(database_error)?)?
        || ledger.generation != projection.position.generation
        || ledger.fact_digest != projection.position.fact_digest
        || ledger.managed_exposure != projection.position.exposure
    {
        return Err(CopyRepositoryError::CorruptData);
    }
    assemble_repair(envelope, follower, &job, &projection, now_ms)
}

pub(super) fn assemble_repair(
    mut envelope: CopyLeaderEnvelope,
    follower: &CopyPlanningFact,
    source: &CopyJob,
    projection: &CopyDriftProjection,
    now_ms: u64,
) -> Result<Option<CopyLeaderEnvelope>, CopyRepositoryError> {
    let semantic: CopySemanticJob = decode(source.semantic_job.clone())?;
    let snapshot: CopyPlanningSnapshot = decode(envelope.snapshot.snapshot_payload.clone())?;
    if projection.source_job_id != source.identities.job_id
        || projection.receipt_sequence == 0
        || projection.position.binding != source.manifest.binding
        || projection.target != semantic.target
        || projection.position.fact_digest == [0; 32]
        || projection.position.generation <= source.manifest.snapshot_generation
    {
        return Err(CopyRepositoryError::CorruptData);
    }
    if source.scope.venue != envelope.scope.venue
        || source.scope.mode != envelope.scope.mode
        || source.scope.trading_account_id != envelope.scope.trading_account_id
        || snapshot.binding != source.manifest.binding
        || follower.private_generation < projection.position.generation
        || follower.observed_ms < projection.position.observed_at_ms
        || now_ms < projection.projected_at_ms
    {
        return Ok(None);
    }
    let planned = plan_observed_copy_job(
        ObservedCopyIntent {
            envelope: envelope.clone(),
            event_sequence: 1,
            event_digest: envelope.outbox_digest,
        },
        now_ms,
    )
    .map_err(|_| CopyRepositoryError::InvalidData)?;
    if planned.target.target_exposure != projection.target.target_exposure
        || planned.job.identities.job_id == source.identities.job_id
    {
        return Ok(None);
    }
    let request = DriftRepairPlanRequest {
        source_job_id: source.identities.job_id,
        repair_identities: planned.job.identities,
        binding: snapshot.binding.clone(),
        expected_position_generation: follower.private_generation,
        expected_target_generation: planned.target.snapshot_generation,
        position: AuthoritativePositionSnapshot {
            binding: snapshot.binding,
            generation: follower.private_generation,
            observed_at_ms: follower.observed_ms,
            expires_at_ms: follower.expires_ms,
            exposure: follower.quote_net_exposure.clone(),
            fact_digest: follower.fact_digest,
        },
        target: planned.target,
        now_ms,
        repair_expires_at_ms: envelope.snapshot.expires_at_ms,
    };
    let Some(repair) = plan_drift_repair(&request).map_err(|_| CopyRepositoryError::InvalidData)?
    else {
        return Ok(None);
    };
    envelope.intent.intent_payload["semantic_action"] = "REPAIR_TARGET".into();
    envelope.intent.intent_payload["drift_repair"] = serde_json::json!({
        "source_receipt_sequence": projection.receipt_sequence,
        "source_position_digest": projection.position.fact_digest,
        "request": repair,
    });
    envelope.intent.intent_digest = input::hash(b"target-intent", &envelope.intent.intent_payload)?;
    envelope.outbox_digest = input::hash(
        b"repair-target-event",
        &(&envelope.scope, &envelope.intent, &envelope.snapshot),
    )?;
    Ok(Some(envelope))
}
