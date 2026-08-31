use super::*;
use venue_control_protocol::AccountDeliveryPayload;

/// Locks and proves eligibility without updating anything. The caller may still encounter a
/// claimed sibling, so retirement must wait until a fresh successor is known to be admissible.
pub(super) async fn lock_unclaimed_expired(
    transaction: &mut Transaction<'_, Postgres>,
    job: &CopyJob,
    leader: &CopyPlanningFact,
    follower: &CopyPlanningFact,
    now_ms: u64,
) -> Result<bool, CopyRepositoryError> {
    let job_id = job.identities.job_id.to_string();
    job.manifest
        .validate(job.created_at_ms)
        .map_err(|_| CopyRepositoryError::CorruptData)?;
    if now_ms <= job.manifest.expires_at_ms
        || leader.observed_ms <= job.manifest.expires_at_ms
        || follower.observed_ms <= job.manifest.expires_at_ms
        || follower.private_generation <= job.manifest.snapshot_generation
        || follower.binding.venue != job.scope.venue
        || follower.binding.mode != job.scope.mode
        || follower.binding.trading_account_id != job.scope.trading_account_id
        || follower.binding.instance_id != job.manifest.binding.follower_instance_id
        || follower.binding.symbol != job.manifest.binding.instrument.symbol
    {
        return Ok(false);
    }
    // Match the Account claim path before locking the job.  The job lock then fences execution
    // evidence, while the legacy outbox lock fences its independent Copy consumer claim.
    // NO KEY UPDATE still conflicts with execution's UPDATE lock but lets a legacy claimant
    // finish its inbox foreign-key KEY SHARE check before we wait on its outbox lock.
    let delivery = sqlx::query(
        "SELECT source_kind, source_id, venue, mode, trading_account_id, symbol, instance_id, \
                config_epoch, payload_json, delivery_state, lease_epoch, leased_by, lease_purpose, \
                leased_at_ms, lease_expires_at_ms \
         FROM venue_account_deliveries WHERE delivery_id=$1 FOR UPDATE",
    )
    .bind(format!("copy:{job_id}"))
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database_error)?
    .ok_or(CopyRepositoryError::CorruptData)?;
    let durable_row =
        sqlx::query("SELECT * FROM venue_copy_jobs WHERE job_id=$1 FOR NO KEY UPDATE")
            .bind(&job_id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(database_error)?
            .ok_or(CopyRepositoryError::CorruptData)?;
    let durable: CopyJob = decode(durable_row.try_get("job_json").map_err(database_error)?)?;
    validate_job_relation_columns(&durable_row, &durable)?;
    if durable != *job
        || job.identities != job.manifest.identities
        || job.job_digest != job.manifest.plan_digest
        || digest(durable_row.try_get("job_digest").map_err(database_error)?)? != job.job_digest
        || decode::<venue_copy::FollowerDeliveryManifest>(
            durable_row
                .try_get("manifest_json")
                .map_err(database_error)?,
        )? != job.manifest
        || from_i64(
            durable_row
                .try_get("expires_at_ms")
                .map_err(database_error)?,
        )? != job.manifest.expires_at_ms
        || from_i64(
            durable_row
                .try_get("created_at_ms")
                .map_err(database_error)?,
        )? != job.created_at_ms
        || durable_row
            .try_get::<String, _>("intent_id")
            .map_err(database_error)?
            != job.intent_id.to_string()
        || durable_row
            .try_get::<i64, _>("source_event_sequence")
            .map_err(database_error)?
            != job.source_event_sequence
        || durable_row
            .try_get::<String, _>("observer_id")
            .map_err(database_error)?
            != job.scope.observer_id
        || durable_row
            .try_get::<String, _>("venue")
            .map_err(database_error)?
            != job.scope.venue.as_str()
        || durable_row
            .try_get::<String, _>("mode")
            .map_err(database_error)?
            != "LIVE"
        || durable_row
            .try_get::<String, _>("trading_account_id")
            .map_err(database_error)?
            != job.scope.trading_account_id
        || durable_row
            .try_get::<String, _>("idempotency_key")
            .map_err(database_error)?
            != job.identities.idempotency_key.to_string()
        || durable_row
            .try_get::<String, _>("follower_binding_id")
            .map_err(database_error)?
            != job.manifest.binding.follower_binding_id.to_string()
    {
        return Err(CopyRepositoryError::CorruptData);
    }
    let outbox = sqlx::query(
        "SELECT delivery_state, claimed_by, claim_epoch, claimed_at_ms, claim_expires_at_ms \
         FROM venue_copy_delivery_outbox WHERE job_id=$1 FOR UPDATE",
    )
    .bind(&job_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database_error)?
    .ok_or(CopyRepositoryError::CorruptData)?;
    let expected_payload =
        AccountDeliveryPayload::CopySemanticJob(venue_control_protocol::CopySemanticJobDelivery {
            job_id: job_id.clone(),
            job_digest: job.job_digest,
            symbol: job.manifest.binding.instrument.symbol.clone(),
            manifest: serde_json::to_value(&job.manifest)
                .map_err(|_| CopyRepositoryError::CorruptData)?,
            semantic_job: job.semantic_job.clone(),
            created_at_ms: job.created_at_ms,
            expires_at_ms: job.manifest.expires_at_ms,
        });
    let pending_delivery = delivery
        .try_get::<String, _>("source_kind")
        .map_err(database_error)?
        == "copy_semantic_job"
        && delivery
            .try_get::<String, _>("source_id")
            .map_err(database_error)?
            == job_id
        && delivery
            .try_get::<String, _>("venue")
            .map_err(database_error)?
            == job.scope.venue.as_str()
        && delivery
            .try_get::<String, _>("mode")
            .map_err(database_error)?
            == "LIVE"
        && delivery
            .try_get::<String, _>("trading_account_id")
            .map_err(database_error)?
            == job.scope.trading_account_id
        && delivery
            .try_get::<String, _>("symbol")
            .map_err(database_error)?
            == job.manifest.binding.instrument.symbol.to_string()
        && delivery
            .try_get::<String, _>("instance_id")
            .map_err(database_error)?
            == job.manifest.binding.follower_instance_id
        && from_i64(delivery.try_get("config_epoch").map_err(database_error)?)?
            == follower.binding.config_epoch
        && decode::<AccountDeliveryPayload>(
            delivery.try_get("payload_json").map_err(database_error)?,
        )? == expected_payload
        && delivery
            .try_get::<String, _>("delivery_state")
            .map_err(database_error)?
            == "pending"
        && delivery
            .try_get::<i64, _>("lease_epoch")
            .map_err(database_error)?
            == 0
        && delivery
            .try_get::<Option<String>, _>("leased_by")
            .map_err(database_error)?
            .is_none()
        && delivery
            .try_get::<Option<String>, _>("lease_purpose")
            .map_err(database_error)?
            .is_none()
        && delivery
            .try_get::<Option<i64>, _>("leased_at_ms")
            .map_err(database_error)?
            .is_none()
        && delivery
            .try_get::<Option<i64>, _>("lease_expires_at_ms")
            .map_err(database_error)?
            .is_none();
    let pending_outbox = outbox
        .try_get::<String, _>("delivery_state")
        .map_err(database_error)?
        == "pending"
        && outbox
            .try_get::<Option<String>, _>("claimed_by")
            .map_err(database_error)?
            .is_none()
        && outbox
            .try_get::<i64, _>("claim_epoch")
            .map_err(database_error)?
            == 0
        && outbox
            .try_get::<Option<i64>, _>("claimed_at_ms")
            .map_err(database_error)?
            .is_none()
        && outbox
            .try_get::<Option<i64>, _>("claim_expires_at_ms")
            .map_err(database_error)?
            .is_none();
    if !pending_delivery || !pending_outbox {
        return Ok(false);
    }
    let evidence: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM venue_account_delivery_claims WHERE delivery_id=$1) \
         OR EXISTS(SELECT 1 FROM venue_account_delivery_acks WHERE delivery_id=$1) \
         OR EXISTS(SELECT 1 FROM venue_account_delivery_receipts WHERE delivery_id=$1) \
         OR EXISTS(SELECT 1 FROM venue_copy_delivery_inbox WHERE job_id=$2) \
         OR EXISTS(SELECT 1 FROM venue_copy_delivery_receipts WHERE job_id=$2) \
         OR EXISTS(SELECT 1 FROM venue_copy_receipt_outbox WHERE job_id=$2) \
         OR EXISTS(SELECT 1 FROM venue_copy_projection_inbox WHERE job_id=$2) \
         OR EXISTS(SELECT 1 FROM venue_copy_execution_results WHERE job_id=$2) \
         OR EXISTS(SELECT 1 FROM venue_copy_ledger WHERE job_id=$2) \
         OR EXISTS(SELECT 1 FROM venue_copy_drift_projections WHERE source_job_id=$2)",
    )
    .bind(format!("copy:{job_id}"))
    .bind(&job_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(database_error)?;
    if evidence {
        return Ok(false);
    }
    Ok(true)
}

pub(super) fn bind_successor(
    envelope: &mut CopyLeaderEnvelope,
    expired: &[CopyJob],
    now_ms: u64,
) -> Result<(), CopyRepositoryError> {
    let identities = venue_copy::derive_copy_identities(&envelope.intent.identity_input)
        .map_err(|_| CopyRepositoryError::InvalidData)?;
    if expired.iter().any(|job| {
        identities.job_id == job.identities.job_id
            || identities.child_order_id == job.identities.child_order_id
            || identities.planning_snapshot_id == job.identities.planning_snapshot_id
            || job.scope.venue != envelope.scope.venue
            || job.scope.mode != envelope.scope.mode
            || job.scope.trading_account_id != envelope.scope.trading_account_id
    }) {
        return Err(CopyRepositoryError::ReplayConflict);
    }
    // These immutable references explain replacement, not execution or retry authority.
    envelope.intent.intent_payload["supersedes_unclaimed_expired_jobs"] = serde_json::json!(
        expired
            .iter()
            .map(|job| serde_json::json!({
                "job_id": job.identities.job_id.to_string(),
                "job_digest": job.job_digest,
                "expires_at_ms": job.manifest.expires_at_ms,
            }))
            .collect::<Vec<_>>()
    );
    envelope.intent.intent_digest = input::hash(b"target-intent", &envelope.intent.intent_payload)?;
    envelope.outbox_digest = input::hash(
        b"fresh-after-unclaimed-expiry",
        &(&envelope.scope, &envelope.intent, &envelope.snapshot),
    )?;
    plan_observed_copy_job(
        ObservedCopyIntent {
            envelope: envelope.clone(),
            event_digest: envelope.outbox_digest,
            event_sequence: 1,
        },
        now_ms,
    )
    .map_err(|_| CopyRepositoryError::InvalidData)?;
    Ok(())
}

pub(super) async fn mark_expired(
    transaction: &mut Transaction<'_, Postgres>,
    expired: &[CopyJob],
    now_ms: u64,
) -> Result<(), CopyRepositoryError> {
    let now = to_i64(now_ms)?;
    for job in expired {
        let job_id = job.identities.job_id.to_string();
        let account = sqlx::query(
            "UPDATE venue_account_deliveries SET delivery_state='expired_unclaimed', updated_at_ms=$2 \
             WHERE delivery_id=$1 AND delivery_state='pending' AND lease_epoch=0 \
              AND leased_by IS NULL AND lease_purpose IS NULL \
              AND leased_at_ms IS NULL AND lease_expires_at_ms IS NULL",
        ).bind(format!("copy:{job_id}")).bind(now).execute(&mut **transaction).await.map_err(database_error)?;
        let legacy = sqlx::query(
            "UPDATE venue_copy_delivery_outbox SET delivery_state='expired_unclaimed', updated_at_ms=$2 \
             WHERE job_id=$1 AND delivery_state='pending' AND claim_epoch=0 \
              AND claimed_by IS NULL AND claimed_at_ms IS NULL AND claim_expires_at_ms IS NULL",
        ).bind(&job_id).bind(now).execute(&mut **transaction).await.map_err(database_error)?;
        if account.rows_affected() != 1 || legacy.rows_affected() != 1 {
            return Err(CopyRepositoryError::DeliveryConflict);
        }
    }
    Ok(())
}
