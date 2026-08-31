use super::*;

use venue_control_protocol::{AccountDeliveryReceipt, AccountDeliveryReceiptState};
use venue_copy::{
    CopyExecutionPhase, CopyExecutionState, DeliveryReceiptStatus, DeliveryState, DeliveryTracker,
    DriftRepairError, LedgerAttribution, LedgerEntry, MAX_REPAIR_TTL_MS, derive_repair_identities,
};

use crate::CopySemanticJob;

/// Writes one already-derived ledger and drift projection in the caller's transaction. The caller
/// must have obtained every source fact from durable Control/Node records; this function never
/// creates an execution or a delivery authorization.
pub(crate) async fn project_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    input: &CopyLedgerProjectionInput,
) -> Result<CopyApplyResult, CopyRepositoryError> {
    if input.projected_at_ms == 0 {
        return Err(CopyRepositoryError::InvalidData);
    }
    input
        .validate_historical_fact()
        .map_err(|_| CopyRepositoryError::ProjectionConflict)?;
    if !input.has_valid_or_signed_expired_repair_window() {
        return Err(CopyRepositoryError::ProjectionConflict);
    }
    let repair_candidate = match input.plan_repair() {
        Ok(repair) => repair,
        // A durable signed outcome remains accounting evidence after expiry. It may update the
        // ledger but must not mint a fresh drift repair/new-risk semantic job.
        Err(DriftRepairError::PositionFreshness | DriftRepairError::RepairWindow) => None,
        Err(_) => return Err(CopyRepositoryError::ProjectionConflict),
    };
    let job_id = input.job_id.to_string();
    let receipt_sequence = to_i64(input.receipt_sequence)?;
    let binding_id = input.ledger_entry.binding.follower_binding_id.to_string();
    let account_id = &input.ledger_entry.binding.account_id;
    let projected_at = to_i64(input.projected_at_ms)?;

    if let Some(row) = sqlx::query(
        "SELECT projection_digest FROM venue_copy_projection_inbox \
         WHERE job_id = $1 AND receipt_sequence = $2 FOR SHARE",
    )
    .bind(&job_id)
    .bind(receipt_sequence)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database_error)?
    {
        let durable = digest(row.try_get("projection_digest").map_err(database_error)?)?;
        return if durable == input.projection_digest {
            Ok(CopyApplyResult::Existing)
        } else {
            Err(CopyRepositoryError::ProjectionConflict)
        };
    }

    let row = sqlx::query(
        "SELECT j.job_json, r.status, o.projected FROM venue_copy_jobs j \
         JOIN venue_copy_delivery_receipts r USING (job_id) \
         JOIN venue_copy_receipt_outbox o USING (job_id, receipt_sequence) \
         WHERE j.job_id = $1 AND r.receipt_sequence = $2 FOR UPDATE OF o",
    )
    .bind(&job_id)
    .bind(receipt_sequence)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database_error)?
    .ok_or(CopyRepositoryError::ProjectionConflict)?;
    let job: CopyJob = decode(row.try_get("job_json").map_err(database_error)?)?;
    let semantic: CopySemanticJob = serde_json::from_value(job.semantic_job.clone())
        .map_err(|_| CopyRepositoryError::CorruptData)?;
    let status: String = row.try_get("status").map_err(database_error)?;
    let already_projected: bool = row.try_get("projected").map_err(database_error)?;
    if already_projected
        || !matches!(status.as_str(), "applied" | "reconciled")
        || job.identities.job_id != input.job_id
        || job.manifest.binding != input.ledger_entry.binding
        || semantic.target != input.target
    {
        return Err(CopyRepositoryError::ProjectionConflict);
    }
    // The command keeps its original position generation. Reconciliation necessarily observes a
    // later generation, so match the exact closing position, not that old input.
    let executions = sqlx::query(
        "SELECT result_json FROM venue_copy_execution_results \
         WHERE job_id = $1 AND delivery_digest = $2 AND execution_state = 'reconciled' FOR SHARE",
    )
    .bind(&job_id)
    .bind(job.manifest.delivery_digest().to_vec())
    .fetch_all(&mut **transaction)
    .await
    .map_err(database_error)?;
    let mut matching = Vec::new();
    for row in executions {
        let result: CopyExecutionResult =
            decode(row.try_get("result_json").map_err(database_error)?)?;
        if result.reconciled_position.as_ref() == Some(&input.position) {
            matching.push(result);
        }
    }
    if matching.len() != 1 {
        return Err(CopyRepositoryError::ProjectionConflict);
    }
    let execution = &matching[0];
    if execution.state != CopyExecutionState::Reconciled
        || execution.request.job_id != input.job_id
        || execution.request.binding != job.manifest.binding
        || execution.reconciled_position.as_ref() != Some(&input.position)
        || execution.request.position_generation >= input.position.generation
    {
        return Err(CopyRepositoryError::ProjectionConflict);
    }
    // A historical child remains valid accounting evidence after its relation is paused or
    // revised. It must not, however, turn that evidence into a new semantic risk request.
    let repair = if job.manifest.expires_at_ms <= input.projected_at_ms
        || !repair_authority_is_current_and_active(transaction, &job).await?
    {
        None
    } else {
        repair_candidate
    };
    let projection_lock = format!("{}|{}|{}", job.scope.venue.as_str(), account_id, binding_id);
    advisory_lock(transaction, &projection_lock, 20_005).await?;

    let rows = sqlx::query(
        "SELECT entry_json FROM venue_copy_ledger \
         WHERE venue = $1 AND mode = 'LIVE' AND trading_account_id = $2 \
           AND follower_binding_id = $3 ORDER BY ledger_sequence FOR SHARE",
    )
    .bind(job.scope.venue.as_str())
    .bind(account_id)
    .bind(&binding_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(database_error)?;
    let mut ledger = CopyLedger::new(input.ledger_entry.binding.clone());
    for row in rows {
        let entry: venue_copy::LedgerEntry =
            decode(row.try_get("entry_json").map_err(database_error)?)?;
        ledger
            .apply(entry)
            .map_err(|_| CopyRepositoryError::CorruptData)?;
    }
    if ledger
        .apply(input.ledger_entry.clone())
        .map_err(|_| CopyRepositoryError::ProjectionConflict)?
        != LedgerApply::Advanced
    {
        return Err(CopyRepositoryError::ProjectionConflict);
    }

    let projection = CopyDriftProjection {
        source_job_id: input.job_id,
        receipt_sequence: input.receipt_sequence,
        position: input.position.clone(),
        target: input.target.clone(),
        repair,
        projected_at_ms: input.projected_at_ms,
    };
    if let Some(row) = sqlx::query(
        "SELECT position_generation FROM venue_copy_drift_projections \
         WHERE venue = $1 AND mode = 'LIVE' AND trading_account_id = $2 \
           AND follower_binding_id = $3 FOR UPDATE",
    )
    .bind(job.scope.venue.as_str())
    .bind(account_id)
    .bind(&binding_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database_error)?
    {
        let generation: i64 = row.try_get("position_generation").map_err(database_error)?;
        if from_i64(generation)? > input.position.generation {
            return Err(CopyRepositoryError::ProjectionConflict);
        }
    }

    sqlx::query(
        "INSERT INTO venue_copy_ledger \
         (venue, mode, trading_account_id, follower_binding_id, ledger_sequence, generation, \
          job_id, receipt_sequence, fact_digest, entry_json, projected_at_ms) \
         VALUES ($1, 'LIVE', $2, $3, $4, $5, $6, $7, $8, $9, $10)",
    )
    .bind(job.scope.venue.as_str())
    .bind(account_id)
    .bind(&binding_id)
    .bind(to_i64(input.ledger_entry.sequence)?)
    .bind(to_i64(input.ledger_entry.generation)?)
    .bind(&job_id)
    .bind(receipt_sequence)
    .bind(input.ledger_entry.fact_digest.to_vec())
    .bind(encode(&input.ledger_entry)?)
    .bind(projected_at)
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    sqlx::query(
        "INSERT INTO venue_copy_drift_projections \
         (venue, mode, trading_account_id, follower_binding_id, position_generation, \
          source_job_id, receipt_sequence, projection_json, projected_at_ms) \
         VALUES ($1, 'LIVE', $2, $3, $4, $5, $6, $7, $8) \
         ON CONFLICT (venue, mode, trading_account_id, follower_binding_id) DO UPDATE SET \
           position_generation = EXCLUDED.position_generation, \
           source_job_id = EXCLUDED.source_job_id, receipt_sequence = EXCLUDED.receipt_sequence, \
           projection_json = EXCLUDED.projection_json, projected_at_ms = EXCLUDED.projected_at_ms",
    )
    .bind(job.scope.venue.as_str())
    .bind(account_id)
    .bind(&binding_id)
    .bind(to_i64(input.position.generation)?)
    .bind(&job_id)
    .bind(receipt_sequence)
    .bind(encode(&projection)?)
    .bind(projected_at)
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    sqlx::query(
        "INSERT INTO venue_copy_projection_inbox \
         (job_id, receipt_sequence, projection_digest, projected_at_ms) VALUES ($1, $2, $3, $4)",
    )
    .bind(&job_id)
    .bind(receipt_sequence)
    .bind(input.projection_digest.to_vec())
    .bind(projected_at)
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    let updated = sqlx::query(
        "UPDATE venue_copy_receipt_outbox SET projected = TRUE \
         WHERE job_id = $1 AND receipt_sequence = $2 AND projected = FALSE",
    )
    .bind(&job_id)
    .bind(receipt_sequence)
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    if updated.rows_affected() != 1 {
        return Err(CopyRepositoryError::ProjectionConflict);
    }
    crate::postgres::insert_ui_event(
        transaction,
        input.projected_at_ms,
        venue_control_protocol::UiEventKind::ExecutionFacts,
        venue_control_protocol::UiAccountScope {
            venue: job.scope.venue,
            mode: venue_control_protocol::GatewayMode::Live,
            trading_account_id: job.scope.trading_account_id.clone(),
        },
    )
    .await
    .map_err(|error| match error {
        crate::RepositoryError::Database => CopyRepositoryError::Database,
        crate::RepositoryError::NumericRange => CopyRepositoryError::NumericRange,
        crate::RepositoryError::CorruptData => CopyRepositoryError::CorruptData,
        _ => CopyRepositoryError::ProjectionConflict,
    })?;
    Ok(CopyApplyResult::Stored)
}

/// New repair work is permitted only by the unchanged, active relation that issued this exact
/// immutable job. A later resume/revision cannot reauthorize an older child.
async fn repair_authority_is_current_and_active(
    transaction: &mut Transaction<'_, Postgres>,
    job: &CopyJob,
) -> Result<bool, CopyRepositoryError> {
    let active: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM venue_copy_relation_configs \
         WHERE relation_id = $1 AND revision = $2 AND lifecycle = 'active')",
    )
    .bind(job.manifest.binding.relation.relation_id.to_string())
    .bind(to_i64(job.manifest.binding.relation.revision)?)
    .fetch_one(&mut **transaction)
    .await
    .map_err(database_error)?;
    Ok(active)
}

impl PgControlRepository {
    /// Bounded, read-only-to-execution projection pump. It consumes only the final `Adjust`
    /// phase after both a persisted terminal Node result and immutable delivery receipt exist;
    /// the intermediate cross-zero reduction remains pending position evidence, not a ledger
    /// settlement. It never makes a new delivery or execution request.
    pub(crate) async fn project_next_reconciled_copy_ledger(
        &self,
        scope: &CopyObserverScope,
        projected_at_ms: u64,
    ) -> Result<Option<CopyApplyResult>, CopyRepositoryError> {
        scope
            .validate()
            .map_err(|_| CopyRepositoryError::InvalidData)?;
        if projected_at_ms == 0 {
            return Err(CopyRepositoryError::InvalidData);
        }
        let mut transaction = self.pool().begin().await.map_err(database_error)?;
        let candidate = sqlx::query(
            "SELECT j.job_json, p.target_exposure_json, e.result_json \
             FROM venue_copy_execution_results e \
             JOIN venue_copy_jobs j USING (job_id) \
             JOIN venue_copy_plans p USING (job_id) \
             WHERE j.observer_id = $1 AND j.venue = $2 AND j.mode = 'LIVE' \
               AND j.trading_account_id = $3 AND e.execution_state = 'reconciled' \
               AND e.result_json->'request'->>'phase' = 'adjust' \
               AND EXISTS (SELECT 1 FROM venue_account_deliveries d \
                           JOIN venue_account_delivery_receipts r USING (delivery_id) \
                           WHERE d.delivery_id = ('copy:' || e.job_id) \
                             AND d.source_kind = 'copy_semantic_job' AND d.source_id = e.job_id \
                             AND r.receipt_state IN ('applied', 'reconciled')) \
               AND NOT EXISTS (SELECT 1 FROM venue_copy_projection_inbox x \
                               WHERE x.job_id = e.job_id) \
             ORDER BY e.observed_at_ms, e.job_id LIMIT 1 FOR UPDATE OF e SKIP LOCKED",
        )
        .bind(&scope.observer_id)
        .bind(scope.venue.as_str())
        .bind(&scope.trading_account_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?;
        let Some(row) = candidate else {
            transaction.commit().await.map_err(database_error)?;
            return Ok(None);
        };
        let job: CopyJob = decode(row.try_get("job_json").map_err(database_error)?)?;
        let target = decode(
            row.try_get("target_exposure_json")
                .map_err(database_error)?,
        )?;
        let execution: CopyExecutionResult =
            decode(row.try_get("result_json").map_err(database_error)?)?;
        let semantic: CopySemanticJob = serde_json::from_value(job.semantic_job.clone())
            .map_err(|_| CopyRepositoryError::CorruptData)?;
        if job.scope != *scope
            || semantic.target != target
            || execution.state != CopyExecutionState::Reconciled
            || execution.request.phase != CopyExecutionPhase::Adjust
            || execution.request.job_id != job.identities.job_id
            || execution.request.binding != job.manifest.binding
            || execution.request.delivery_digest != job.manifest.delivery_digest()
        {
            return Err(CopyRepositoryError::CorruptData);
        }
        let position = execution
            .reconciled_position
            .clone()
            .ok_or(CopyRepositoryError::CorruptData)?;
        let receipt_sequence = materialize_node_copy_receipts(&mut transaction, &job)
            .await?
            .ok_or(CopyRepositoryError::DeliveryConflict)?;
        let binding_id = job.manifest.binding.follower_binding_id.to_string();
        let projection_lock = format!(
            "{}|{}|{}",
            job.scope.venue.as_str(),
            job.manifest.binding.account_id,
            binding_id
        );
        advisory_lock(&mut transaction, &projection_lock, 20_005).await?;
        let sequence: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(ledger_sequence), 0) FROM venue_copy_ledger \
             WHERE venue = $1 AND mode = 'LIVE' AND trading_account_id = $2 \
               AND follower_binding_id = $3",
        )
        .bind(job.scope.venue.as_str())
        .bind(&job.manifest.binding.account_id)
        .bind(&binding_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(database_error)?;
        let ledger_sequence = from_i64(sequence)?
            .checked_add(1)
            .ok_or(CopyRepositoryError::NumericRange)?;
        let repair_expires_at_ms = projected_at_ms
            .checked_add(MAX_REPAIR_TTL_MS)
            .ok_or(CopyRepositoryError::NumericRange)?
            .min(position.expires_at_ms);
        let input = CopyLedgerProjectionInput {
            job_id: job.identities.job_id,
            receipt_sequence,
            // The signed closing fact is the durable projection commitment; the inbox primary
            // key additionally binds it to this exact job and canonical receipt sequence.
            projection_digest: position.fact_digest,
            ledger_entry: LedgerEntry {
                sequence: ledger_sequence,
                generation: position.generation,
                binding: position.binding.clone(),
                attribution: LedgerAttribution::Copy,
                source_id: job.identities.job_id,
                fact_digest: position.fact_digest,
                managed_exposure: position.exposure.clone(),
            },
            position: position.clone(),
            target,
            repair_identities: derive_repair_identities(
                &job.identities.job_id,
                receipt_sequence,
                &position.fact_digest,
            )
            .map_err(|_| CopyRepositoryError::CorruptData)?,
            projected_at_ms,
            repair_expires_at_ms,
        };
        let result = project_in_transaction(&mut transaction, &input).await?;
        transaction.commit().await.map_err(database_error)?;
        Ok(Some(result))
    }

    /// Projects one durable account-node rejection into the legacy Copy receipt state so an
    /// immutable child cannot remain busy forever. It only records the rejection already
    /// persisted by the node; it never creates execution, retry, ledger, or repair work.
    pub(crate) async fn project_next_rejected_copy_delivery(
        &self,
        scope: &CopyObserverScope,
    ) -> Result<Option<CopyApplyResult>, CopyRepositoryError> {
        scope
            .validate()
            .map_err(|_| CopyRepositoryError::InvalidData)?;
        let mut transaction = self.pool().begin().await.map_err(database_error)?;
        let candidate = sqlx::query(
            "SELECT j.job_json FROM venue_account_deliveries d \
             JOIN venue_copy_jobs j ON j.job_id = d.source_id \
             JOIN venue_account_delivery_receipts r USING (delivery_id) \
             WHERE d.delivery_id = ('copy:' || j.job_id) \
               AND d.source_kind = 'copy_semantic_job' AND d.source_id = j.job_id \
               AND d.delivery_state = 'settled' AND r.receipt_state = 'rejected' \
               AND j.observer_id = $1 AND j.venue = $2 AND j.mode = 'LIVE' \
               AND j.trading_account_id = $3 \
               AND NOT EXISTS (SELECT 1 FROM venue_copy_delivery_receipts c \
                               WHERE c.job_id = j.job_id AND c.receipt_sequence = 1) \
             ORDER BY r.observed_ms, j.job_id LIMIT 1 FOR UPDATE OF d SKIP LOCKED",
        )
        .bind(&scope.observer_id)
        .bind(scope.venue.as_str())
        .bind(&scope.trading_account_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?;
        let Some(row) = candidate else {
            transaction.commit().await.map_err(database_error)?;
            return Ok(None);
        };
        let job: CopyJob = decode(row.try_get("job_json").map_err(database_error)?)?;
        if job.scope != *scope {
            return Err(CopyRepositoryError::CorruptData);
        }
        if materialize_node_copy_receipts(&mut transaction, &job)
            .await?
            .is_some()
        {
            return Err(CopyRepositoryError::DeliveryConflict);
        }
        transaction.commit().await.map_err(database_error)?;
        Ok(Some(CopyApplyResult::Stored))
    }
}

/// Converts only an existing account-node receipt for this immutable Copy delivery into the
/// legacy Copy receipt/outbox rows that ledger replay already consumes. No consumer claim or
/// caller-supplied state participates in the conversion.
async fn materialize_node_copy_receipts(
    transaction: &mut Transaction<'_, Postgres>,
    job: &CopyJob,
) -> Result<Option<u64>, CopyRepositoryError> {
    let job_id = job.identities.job_id.to_string();
    let delivery_id = format!("copy:{job_id}");
    let rows = sqlx::query(
        "SELECT d.venue, d.mode, d.trading_account_id, d.symbol, d.instance_id, d.config_epoch, \
                r.receipt_state, r.receipt_json \
         FROM venue_account_deliveries d \
         JOIN venue_account_delivery_receipts r USING (delivery_id) \
         WHERE d.delivery_id = $1 AND d.source_kind = 'copy_semantic_job' AND d.source_id = $2 \
         ORDER BY CASE r.receipt_state WHEN 'unknown' THEN 1 WHEN 'reconciled' THEN 2 ELSE 1 END, \
                  r.observed_ms FOR SHARE",
    )
    .bind(&delivery_id)
    .bind(&job_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(database_error)?;
    if rows.is_empty() {
        return Err(CopyRepositoryError::DeliveryConflict);
    }
    let mut tracker = DeliveryTracker::new(job.manifest.clone(), job.created_at_ms)
        .map_err(|_| CopyRepositoryError::CorruptData)?;
    let existing = sqlx::query(
        "SELECT receipt_json FROM venue_copy_delivery_receipts WHERE job_id = $1 \
         ORDER BY receipt_sequence FOR SHARE",
    )
    .bind(&job_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(database_error)?;
    for row in existing {
        let receipt: PersistedDeliveryReceipt =
            decode(row.try_get("receipt_json").map_err(database_error)?)?;
        tracker
            .apply_persisted_receipt(receipt)
            .map_err(|_| CopyRepositoryError::CorruptData)?;
    }

    let mut projectable_sequence = None;
    let mut latest_receipt_ms = 0_u64;
    for row in rows {
        let venue: String = row.try_get("venue").map_err(database_error)?;
        let mode: String = row.try_get("mode").map_err(database_error)?;
        let account: String = row.try_get("trading_account_id").map_err(database_error)?;
        let symbol: String = row.try_get("symbol").map_err(database_error)?;
        let instance: String = row.try_get("instance_id").map_err(database_error)?;
        let config_epoch = from_i64(row.try_get("config_epoch").map_err(database_error)?)?;
        let node: AccountDeliveryReceipt =
            decode(row.try_get("receipt_json").map_err(database_error)?)?;
        node.validate()
            .map_err(|_| CopyRepositoryError::CorruptData)?;
        if venue != job.scope.venue.as_str()
            || mode != "LIVE"
            || account != job.manifest.binding.account_id
            || symbol != job.manifest.binding.instrument.symbol.to_string()
            || instance != job.manifest.binding.follower_instance_id
            || node.lease.delivery_id != delivery_id
            || node.lease.binding.venue.as_str() != venue
            || node.lease.binding.mode != venue_control_protocol::GatewayMode::Live
            || node.lease.binding.trading_account_id != account
            || node.lease.binding.symbol.to_string() != symbol
            || node.lease.binding.instance_id != instance
            || node.lease.binding.config_epoch != config_epoch
        {
            return Err(CopyRepositoryError::DeliveryConflict);
        }
        latest_receipt_ms = latest_receipt_ms.max(node.observed_ms);
        let (status, receipt_sequence) = match node.state {
            AccountDeliveryReceiptState::Applied => (DeliveryReceiptStatus::Applied, 1),
            AccountDeliveryReceiptState::Rejected => (DeliveryReceiptStatus::Rejected, 1),
            AccountDeliveryReceiptState::Unknown => (DeliveryReceiptStatus::Unknown, 1),
            AccountDeliveryReceiptState::Reconciled => (DeliveryReceiptStatus::Reconciled, 2),
        };
        let canonical = PersistedDeliveryReceipt {
            delivery_digest: job.manifest.delivery_digest(),
            binding: job.manifest.binding.clone(),
            plan_digest: job.manifest.plan_digest,
            snapshot_generation: job.manifest.snapshot_generation,
            instrument_generation: job.manifest.instrument_generation,
            receipt_sequence,
            status,
            persisted_at_ms: node.observed_ms,
        };
        match tracker.apply_persisted_receipt(canonical.clone()) {
            Ok(_) => {}
            Err(_)
                if matches!(
                    tracker.state(),
                    DeliveryState::Applied(_) | DeliveryState::Rejected(_)
                ) =>
            {
                return Err(CopyRepositoryError::DeliveryConflict);
            }
            Err(_) => return Err(CopyRepositoryError::CorruptData),
        }
        let existing = sqlx::query(
            "SELECT receipt_json FROM venue_copy_delivery_receipts \
             WHERE job_id = $1 AND receipt_sequence = $2 FOR SHARE",
        )
        .bind(&job_id)
        .bind(to_i64(receipt_sequence)?)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(database_error)?;
        if let Some(existing) = existing {
            let durable: PersistedDeliveryReceipt =
                decode(existing.try_get("receipt_json").map_err(database_error)?)?;
            if durable != canonical {
                return Err(CopyRepositoryError::ReplayConflict);
            }
        } else {
            sqlx::query(
                "INSERT INTO venue_copy_delivery_receipts \
                 (job_id, receipt_sequence, status, receipt_json, persisted_at_ms) \
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(&job_id)
            .bind(to_i64(receipt_sequence)?)
            .bind(receipt_status(status))
            .bind(encode(&canonical)?)
            .bind(to_i64(node.observed_ms)?)
            .execute(&mut **transaction)
            .await
            .map_err(database_error)?;
        }
        if matches!(
            status,
            DeliveryReceiptStatus::Applied | DeliveryReceiptStatus::Reconciled
        ) {
            sqlx::query(
                "INSERT INTO venue_copy_receipt_outbox \
                 (job_id, receipt_sequence, projected, created_at_ms) VALUES ($1, $2, FALSE, $3) \
                 ON CONFLICT (job_id, receipt_sequence) DO NOTHING",
            )
            .bind(&job_id)
            .bind(to_i64(receipt_sequence)?)
            .bind(to_i64(node.observed_ms)?)
            .execute(&mut **transaction)
            .await
            .map_err(database_error)?;
            projectable_sequence = Some(receipt_sequence);
        }
    }
    let expected_outbox_state = match tracker.state() {
        DeliveryState::Unknown(_) => "reconciliation_required",
        DeliveryState::Applied(_) | DeliveryState::Reconciled(_) | DeliveryState::Rejected(_) => {
            "settled"
        }
        DeliveryState::Pending => return Err(CopyRepositoryError::DeliveryConflict),
    };
    let current_outbox_state: String = sqlx::query_scalar(
        "SELECT delivery_state FROM venue_copy_delivery_outbox WHERE job_id = $1 FOR UPDATE",
    )
    .bind(&job_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database_error)?
    .ok_or(CopyRepositoryError::DeliveryConflict)?;
    let transition_is_valid = matches!(
        (current_outbox_state.as_str(), expected_outbox_state),
        ("pending" | "claimed", "reconciliation_required" | "settled")
            | (
                "reconciliation_required",
                "reconciliation_required" | "settled"
            )
            | ("settled", "settled")
    );
    if !transition_is_valid {
        return Err(CopyRepositoryError::DeliveryConflict);
    }
    if current_outbox_state != expected_outbox_state {
        sqlx::query(
            "UPDATE venue_copy_delivery_outbox SET delivery_state = $2, updated_at_ms = $3 \
             WHERE job_id = $1",
        )
        .bind(&job_id)
        .bind(expected_outbox_state)
        .bind(to_i64(latest_receipt_ms)?)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
        crate::copy_ledger_read_model::notify_repair_change(transaction, job, latest_receipt_ms)
            .await?;
    }
    Ok(projectable_sequence)
}
