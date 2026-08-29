use std::collections::BTreeMap;

use sqlx::{Postgres, Row, Transaction};
use venue_copy::{
    CopyLedger, DeliveryReceiptStatus, DeliveryState, DeliveryTracker, LedgerApply,
    PersistedDeliveryReceipt,
};

use crate::copy_worker::plan_observed_copy_job;

use crate::{
    CopyApplyResult, CopyCrashReplay, CopyDeliveryClaim, CopyDriftProjection, CopyLeaderEnvelope,
    CopyLeaderIntent, CopyLeaderSnapshot, CopyLedgerProjectionInput, CopyObserverLease,
    CopyObserverScope, CopyReplayDeliveryState, CopyReplayJob, CopyRepository, CopyRepositoryError,
    CopyStoreResult, CopyTestJob, MAX_COPY_DELIVERY_CLAIM_MS, MAX_COPY_OBSERVER_LEASE_MS,
    ObservedCopyIntent, PgControlRepository, PlannedCopyJob, ScopedCopyDeliveryReceipt,
};

pub const MIGRATION_0002: &str = include_str!("../migrations/0002_copy_core.sql");

impl PgControlRepository {
    pub async fn load_copy_worker_replay(
        &self,
        scope: &CopyObserverScope,
        replayed_at_ms: u64,
    ) -> Result<CopyCrashReplay, CopyRepositoryError> {
        let replay = self.load_copy_replay(scope, replayed_at_ms).await?;
        let rows = sqlx::query(
            "SELECT j.job_json, p.venue AS plan_venue, p.mode AS plan_mode, \
                    p.trading_account_id AS plan_account_id, p.source_event_sequence, \
                    p.capital_snapshot_json, p.target_exposure_json, p.plan_digest, \
                    o.event_sequence, o.event_digest, i.intent_json, i.intent_digest, \
                    s.snapshot_json, s.snapshot_digest \
             FROM venue_copy_jobs j JOIN venue_copy_plans p USING (job_id) \
             JOIN venue_copy_observer_outbox o \
               ON o.event_sequence = j.source_event_sequence AND o.observer_id = j.observer_id \
             JOIN venue_copy_leader_intents i USING (intent_id) \
             JOIN venue_copy_leader_snapshots s USING (snapshot_id) \
             WHERE j.observer_id = $1 AND j.venue = $2 AND j.mode = 'TEST' \
               AND j.trading_account_id = $3 ORDER BY j.source_event_sequence",
        )
        .bind(&scope.observer_id)
        .bind(scope.venue.as_str())
        .bind(&scope.trading_account_id)
        .fetch_all(self.pool())
        .await
        .map_err(database_error)?;
        if rows.len() != replay.jobs.len() {
            return Err(CopyRepositoryError::CorruptData);
        }
        for row in rows {
            let job: CopyTestJob = decode(row.try_get("job_json").map_err(database_error)?)?;
            let plan_venue: String = row.try_get("plan_venue").map_err(database_error)?;
            let plan_mode: String = row.try_get("plan_mode").map_err(database_error)?;
            let plan_account_id: String = row.try_get("plan_account_id").map_err(database_error)?;
            let source_event_sequence: i64 = row
                .try_get("source_event_sequence")
                .map_err(database_error)?;
            let capital_json: serde_json::Value = row
                .try_get("capital_snapshot_json")
                .map_err(database_error)?;
            let target_json: serde_json::Value = row
                .try_get("target_exposure_json")
                .map_err(database_error)?;
            let plan_digest: Vec<u8> = row.try_get("plan_digest").map_err(database_error)?;
            let capital: crate::FrozenCapitalSnapshot = decode(capital_json)?;
            let target: venue_copy::TargetExposurePlan = decode(target_json)?;
            let observed = observed_from_row(row, scope)?;
            let recomputed = plan_observed_copy_job(observed, job.created_at_ms)
                .map_err(|_| CopyRepositoryError::CorruptData)?;
            if plan_venue != scope.venue.as_str()
                || plan_mode != "TEST"
                || plan_account_id != scope.trading_account_id
                || source_event_sequence != job.source_event_sequence
                || digest(plan_digest)? != job.job_digest
                || capital != recomputed.frozen_capital
                || target != recomputed.target
                || job != recomputed.job
            {
                return Err(CopyRepositoryError::CorruptData);
            }
        }
        Ok(replay)
    }

    /// Atomically fences the observer cursor, locks the next immutable leader event, runs the pure
    /// planner, and persists the frozen capital, target, manifest, delivery job, and new cursor.
    /// The transaction never touches writer, WAL, capability, gateway, or mutation state.
    pub async fn plan_next_copy_job_atomic(
        &self,
        lease: &CopyObserverLease,
        planned_at_ms: u64,
    ) -> Result<Option<PlannedCopyJob>, CopyRepositoryError> {
        lease
            .validate(planned_at_ms)
            .map_err(|_| CopyRepositoryError::LeaseConflict)?;
        let planned_at = to_i64(planned_at_ms)?;
        let mut transaction = self.pool().begin().await.map_err(database_error)?;
        lock_and_validate_lease(&mut transaction, lease, planned_at).await?;
        let cursor: i64 = sqlx::query(
            "SELECT last_event_sequence FROM venue_copy_observer_cursors \
             WHERE observer_id = $1 FOR UPDATE",
        )
        .bind(&lease.scope.observer_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(database_error)?
        .try_get("last_event_sequence")
        .map_err(database_error)?;
        let Some(observed) =
            load_next_observed_for_update(&mut transaction, &lease.scope, cursor).await?
        else {
            transaction.commit().await.map_err(database_error)?;
            return Ok(None);
        };
        let planned = plan_observed_copy_job(observed, planned_at_ms)
            .map_err(|_| CopyRepositoryError::InvalidData)?;
        persist_planned_copy_job(&mut transaction, lease, &planned, cursor, planned_at).await?;
        transaction.commit().await.map_err(database_error)?;
        Ok(Some(planned))
    }
}

impl CopyRepository for PgControlRepository {
    async fn store_leader_envelope(
        &self,
        envelope: &CopyLeaderEnvelope,
        stored_at_ms: u64,
    ) -> Result<CopyStoreResult, CopyRepositoryError> {
        envelope
            .validate(stored_at_ms)
            .map_err(|_| CopyRepositoryError::InvalidData)?;
        if stored_at_ms < envelope.intent.observed_at_ms {
            return Err(CopyRepositoryError::InvalidData);
        }
        let stored_at = to_i64(stored_at_ms)?;
        let snapshot_json = encode(&envelope.snapshot)?;
        let intent_json = encode(&envelope.intent)?;
        let intent_id = envelope.intent.intent_id.to_string();
        let snapshot_id = envelope.snapshot.snapshot_id.to_string();
        let mut transaction = self.pool().begin().await.map_err(database_error)?;
        advisory_lock(&mut transaction, &envelope.scope.observer_id, 20_003).await?;
        ensure_observer_scope(&mut transaction, &envelope.scope).await?;
        advisory_lock(&mut transaction, &intent_id, 20_001).await?;
        advisory_lock(&mut transaction, &snapshot_id, 20_002).await?;

        if let Some(row) = sqlx::query(
            "SELECT i.observer_id, i.venue, i.trading_account_id, i.intent_json, i.intent_digest, \
                    s.snapshot_json, s.snapshot_digest, o.event_digest, o.event_sequence \
             FROM venue_copy_leader_intents i \
             JOIN venue_copy_leader_snapshots s USING (snapshot_id) \
             JOIN venue_copy_observer_outbox o USING (intent_id) \
             WHERE i.intent_id = $1 FOR SHARE OF i, s, o",
        )
        .bind(&intent_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?
        {
            let existing_scope = scope_from_row(&row)?;
            let existing_intent: CopyLeaderIntent =
                decode(row.try_get("intent_json").map_err(database_error)?)?;
            let existing_snapshot: CopyLeaderSnapshot =
                decode(row.try_get("snapshot_json").map_err(database_error)?)?;
            let intent_digest = digest(row.try_get("intent_digest").map_err(database_error)?)?;
            let snapshot_digest = digest(row.try_get("snapshot_digest").map_err(database_error)?)?;
            let event_digest = digest(row.try_get("event_digest").map_err(database_error)?)?;
            let sequence = row.try_get("event_sequence").map_err(database_error)?;
            if existing_scope == envelope.scope
                && existing_intent == envelope.intent
                && existing_snapshot == envelope.snapshot
                && intent_digest == envelope.intent.intent_digest
                && snapshot_digest == envelope.snapshot.snapshot_digest
                && event_digest == envelope.outbox_digest
            {
                transaction.commit().await.map_err(database_error)?;
                return Ok(CopyStoreResult::Existing { sequence });
            }
            return Err(CopyRepositoryError::ReplayConflict);
        }

        if let Some(row) = sqlx::query(
            "SELECT observer_id, venue, trading_account_id, snapshot_json, snapshot_digest \
             FROM venue_copy_leader_snapshots WHERE snapshot_id = $1 FOR SHARE",
        )
        .bind(&snapshot_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?
        {
            let existing_scope = scope_from_row(&row)?;
            let existing_snapshot: CopyLeaderSnapshot =
                decode(row.try_get("snapshot_json").map_err(database_error)?)?;
            let existing_digest = digest(row.try_get("snapshot_digest").map_err(database_error)?)?;
            if existing_scope != envelope.scope
                || existing_snapshot != envelope.snapshot
                || existing_digest != envelope.snapshot.snapshot_digest
            {
                return Err(CopyRepositoryError::ReplayConflict);
            }
        } else {
            sqlx::query(
                "INSERT INTO venue_copy_leader_snapshots \
                 (snapshot_id, observer_id, venue, mode, trading_account_id, generation, \
                  observed_at_ms, expires_at_ms, snapshot_digest, snapshot_json) \
                 VALUES ($1, $2, $3, 'TEST', $4, $5, $6, $7, $8, $9)",
            )
            .bind(&snapshot_id)
            .bind(&envelope.scope.observer_id)
            .bind(envelope.scope.venue.as_str())
            .bind(&envelope.scope.trading_account_id)
            .bind(to_i64(envelope.snapshot.generation)?)
            .bind(to_i64(envelope.snapshot.observed_at_ms)?)
            .bind(to_i64(envelope.snapshot.expires_at_ms)?)
            .bind(envelope.snapshot.snapshot_digest.to_vec())
            .bind(snapshot_json)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
        }

        sqlx::query(
            "INSERT INTO venue_copy_leader_intents \
             (intent_id, observer_id, venue, mode, trading_account_id, snapshot_id, intent_digest, \
              intent_json, observed_at_ms, stored_at_ms) \
             VALUES ($1, $2, $3, 'TEST', $4, $5, $6, $7, $8, $9)",
        )
        .bind(&intent_id)
        .bind(&envelope.scope.observer_id)
        .bind(envelope.scope.venue.as_str())
        .bind(&envelope.scope.trading_account_id)
        .bind(&snapshot_id)
        .bind(envelope.intent.intent_digest.to_vec())
        .bind(intent_json)
        .bind(to_i64(envelope.intent.observed_at_ms)?)
        .bind(stored_at)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        let row = sqlx::query(
            "INSERT INTO venue_copy_observer_outbox \
             (observer_id, intent_id, event_digest, created_at_ms) VALUES ($1, $2, $3, $4) \
             RETURNING event_sequence",
        )
        .bind(&envelope.scope.observer_id)
        .bind(&intent_id)
        .bind(envelope.outbox_digest.to_vec())
        .bind(stored_at)
        .fetch_one(&mut *transaction)
        .await
        .map_err(database_error)?;
        let sequence = row.try_get("event_sequence").map_err(database_error)?;
        sqlx::query(
            "INSERT INTO venue_copy_observer_cursors \
             (observer_id, last_event_sequence, updated_at_ms) VALUES ($1, 0, $2) \
             ON CONFLICT (observer_id) DO NOTHING",
        )
        .bind(&envelope.scope.observer_id)
        .bind(stored_at)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        transaction.commit().await.map_err(database_error)?;
        Ok(CopyStoreResult::Inserted { sequence })
    }

    async fn acquire_observer_lease(
        &self,
        scope: &CopyObserverScope,
        holder_id: &str,
        acquired_at_ms: u64,
        expires_at_ms: u64,
    ) -> Result<CopyObserverLease, CopyRepositoryError> {
        scope
            .validate()
            .map_err(|_| CopyRepositoryError::InvalidData)?;
        validate_window(
            holder_id,
            acquired_at_ms,
            expires_at_ms,
            MAX_COPY_OBSERVER_LEASE_MS,
        )?;
        let acquired_at = to_i64(acquired_at_ms)?;
        let expires_at = to_i64(expires_at_ms)?;
        let mut transaction = self.pool().begin().await.map_err(database_error)?;
        advisory_lock(&mut transaction, &scope.observer_id, 20_003).await?;
        ensure_observer_scope(&mut transaction, scope).await?;
        let current = sqlx::query(
            "SELECT venue, trading_account_id, holder_id, lease_epoch, acquired_at_ms, expires_at_ms \
             FROM venue_copy_observer_leases WHERE observer_id = $1 FOR UPDATE",
        )
        .bind(&scope.observer_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?;

        let lease_epoch = if let Some(row) = current {
            let durable_scope = CopyObserverScope {
                observer_id: scope.observer_id.clone(),
                venue: parse_venue(row.try_get("venue").map_err(database_error)?)?,
                trading_account_id: row.try_get("trading_account_id").map_err(database_error)?,
            };
            if durable_scope != *scope {
                return Err(CopyRepositoryError::ReplayConflict);
            }
            let current_holder: String = row.try_get("holder_id").map_err(database_error)?;
            let current_epoch: i64 = row.try_get("lease_epoch").map_err(database_error)?;
            let current_acquired: i64 = row.try_get("acquired_at_ms").map_err(database_error)?;
            let current_expiry: i64 = row.try_get("expires_at_ms").map_err(database_error)?;
            if acquired_at < current_acquired {
                return Err(CopyRepositoryError::LeaseConflict);
            }
            if current_holder != holder_id && current_expiry > acquired_at {
                return Err(CopyRepositoryError::LeaseUnavailable);
            }
            if current_holder == holder_id && current_expiry > acquired_at {
                from_i64(current_epoch)?
            } else {
                from_i64(
                    current_epoch
                        .checked_add(1)
                        .ok_or(CopyRepositoryError::NumericRange)?,
                )?
            }
        } else {
            1
        };
        sqlx::query(
            "INSERT INTO venue_copy_observer_leases \
             (observer_id, venue, mode, trading_account_id, lease_kind, mutation_authority, \
              holder_id, lease_epoch, acquired_at_ms, expires_at_ms) \
             VALUES ($1, $2, 'TEST', $3, 'COPY_TEST_OBSERVER', FALSE, $4, $5, $6, $7) \
             ON CONFLICT (observer_id) DO UPDATE SET \
               holder_id = EXCLUDED.holder_id, lease_epoch = EXCLUDED.lease_epoch, \
               acquired_at_ms = EXCLUDED.acquired_at_ms, expires_at_ms = EXCLUDED.expires_at_ms",
        )
        .bind(&scope.observer_id)
        .bind(scope.venue.as_str())
        .bind(&scope.trading_account_id)
        .bind(holder_id)
        .bind(to_i64(lease_epoch)?)
        .bind(acquired_at)
        .bind(expires_at)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        sqlx::query(
            "INSERT INTO venue_copy_observer_cursors \
             (observer_id, last_event_sequence, updated_at_ms) VALUES ($1, 0, $2) \
             ON CONFLICT (observer_id) DO NOTHING",
        )
        .bind(&scope.observer_id)
        .bind(acquired_at)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        transaction.commit().await.map_err(database_error)?;
        Ok(CopyObserverLease {
            scope: scope.clone(),
            holder_id: holder_id.to_owned(),
            lease_epoch,
            acquired_at_ms,
            expires_at_ms,
        })
    }

    async fn observe_leader_intents(
        &self,
        lease: &CopyObserverLease,
        observed_at_ms: u64,
        limit: u32,
    ) -> Result<Vec<ObservedCopyIntent>, CopyRepositoryError> {
        lease
            .validate(observed_at_ms)
            .map_err(|_| CopyRepositoryError::LeaseConflict)?;
        validate_limit(limit)?;
        let observed_at = to_i64(observed_at_ms)?;
        let mut transaction = self.pool().begin().await.map_err(database_error)?;
        lock_and_validate_lease(&mut transaction, lease, observed_at).await?;
        let cursor: i64 = sqlx::query(
            "SELECT last_event_sequence FROM venue_copy_observer_cursors \
             WHERE observer_id = $1 FOR SHARE",
        )
        .bind(&lease.scope.observer_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(database_error)?
        .try_get("last_event_sequence")
        .map_err(database_error)?;
        let observed =
            load_observed_after(&mut transaction, &lease.scope, cursor, i64::from(limit)).await?;
        transaction.commit().await.map_err(database_error)?;
        Ok(observed)
    }

    async fn commit_copy_job(
        &self,
        lease: &CopyObserverLease,
        observed: &ObservedCopyIntent,
        job: &CopyTestJob,
        committed_at_ms: u64,
    ) -> Result<CopyApplyResult, CopyRepositoryError> {
        lease
            .validate(committed_at_ms)
            .map_err(|_| CopyRepositoryError::LeaseConflict)?;
        observed
            .envelope
            .validate(committed_at_ms)
            .map_err(|_| CopyRepositoryError::InvalidData)?;
        job.validate_against(observed)
            .map_err(|_| CopyRepositoryError::InvalidData)?;
        if observed.event_sequence <= 0 || observed.envelope.scope != lease.scope {
            return Err(CopyRepositoryError::InvalidData);
        }
        let committed_at = to_i64(committed_at_ms)?;
        let mut transaction = self.pool().begin().await.map_err(database_error)?;
        lock_and_validate_lease(&mut transaction, lease, committed_at).await?;
        let cursor: i64 = sqlx::query(
            "SELECT last_event_sequence FROM venue_copy_observer_cursors \
             WHERE observer_id = $1 FOR UPDATE",
        )
        .bind(&lease.scope.observer_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(database_error)?
        .try_get("last_event_sequence")
        .map_err(database_error)?;

        if cursor >= observed.event_sequence {
            let row = sqlx::query(
                "SELECT i.event_digest, j.job_json FROM venue_copy_observer_inbox i \
                 JOIN venue_copy_jobs j USING (job_id) \
                 WHERE i.observer_id = $1 AND i.event_sequence = $2 FOR SHARE OF i, j",
            )
            .bind(&lease.scope.observer_id)
            .bind(observed.event_sequence)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(database_error)?
            .ok_or(CopyRepositoryError::CursorConflict)?;
            let durable_digest = digest(row.try_get("event_digest").map_err(database_error)?)?;
            let durable_job: CopyTestJob =
                decode(row.try_get("job_json").map_err(database_error)?)?;
            if durable_digest == observed.event_digest && durable_job == *job {
                transaction.commit().await.map_err(database_error)?;
                return Ok(CopyApplyResult::Existing);
            }
            return Err(CopyRepositoryError::ReplayConflict);
        }

        let next = load_observed_after(&mut transaction, &lease.scope, cursor, 1)
            .await?
            .into_iter()
            .next()
            .ok_or(CopyRepositoryError::CursorConflict)?;
        if next != *observed {
            return Err(CopyRepositoryError::CursorConflict);
        }

        let job_id = job.identities.job_id.to_string();
        advisory_lock(&mut transaction, &job_id, 20_004).await?;
        let identity_conflict = sqlx::query(
            "SELECT 1 FROM venue_copy_jobs WHERE job_id = $1 OR idempotency_key = $2 LIMIT 1 FOR SHARE",
        )
        .bind(&job_id)
        .bind(job.identities.idempotency_key.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?;
        if identity_conflict.is_some() {
            return Err(CopyRepositoryError::ReplayConflict);
        }
        sqlx::query(
            "INSERT INTO venue_copy_jobs \
             (job_id, observer_id, source_event_sequence, intent_id, venue, mode, \
              trading_account_id, idempotency_key, follower_binding_id, manifest_json, job_json, \
              job_digest, created_at_ms, expires_at_ms) \
             VALUES ($1, $2, $3, $4, $5, 'TEST', $6, $7, $8, $9, $10, $11, $12, $13)",
        )
        .bind(&job_id)
        .bind(&job.scope.observer_id)
        .bind(job.source_event_sequence)
        .bind(job.intent_id.to_string())
        .bind(job.scope.venue.as_str())
        .bind(&job.scope.trading_account_id)
        .bind(job.identities.idempotency_key.to_string())
        .bind(job.manifest.binding.follower_binding_id.to_string())
        .bind(encode(&job.manifest)?)
        .bind(encode(job)?)
        .bind(job.job_digest.to_vec())
        .bind(to_i64(job.created_at_ms)?)
        .bind(to_i64(job.manifest.expires_at_ms)?)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        sqlx::query(
            "INSERT INTO venue_copy_observer_inbox \
             (observer_id, event_sequence, event_digest, job_id, consumed_at_ms) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&lease.scope.observer_id)
        .bind(observed.event_sequence)
        .bind(observed.event_digest.to_vec())
        .bind(&job_id)
        .bind(committed_at)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        sqlx::query(
            "INSERT INTO venue_copy_delivery_outbox \
             (job_id, delivery_state, claimed_by, claim_epoch, claimed_at_ms, \
              claim_expires_at_ms, updated_at_ms) \
             VALUES ($1, 'pending', NULL, 0, NULL, NULL, $2)",
        )
        .bind(&job_id)
        .bind(committed_at)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        let updated = sqlx::query(
            "UPDATE venue_copy_observer_cursors \
             SET last_event_sequence = $2, updated_at_ms = $3 \
             WHERE observer_id = $1 AND last_event_sequence = $4",
        )
        .bind(&lease.scope.observer_id)
        .bind(observed.event_sequence)
        .bind(committed_at)
        .bind(cursor)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        if updated.rows_affected() != 1 {
            return Err(CopyRepositoryError::CursorConflict);
        }
        transaction.commit().await.map_err(database_error)?;
        Ok(CopyApplyResult::Stored)
    }

    async fn claim_copy_jobs(
        &self,
        scope: &CopyObserverScope,
        consumer_id: &str,
        claimed_at_ms: u64,
        expires_at_ms: u64,
        limit: u32,
    ) -> Result<Vec<CopyDeliveryClaim>, CopyRepositoryError> {
        scope
            .validate()
            .map_err(|_| CopyRepositoryError::InvalidData)?;
        validate_window(
            consumer_id,
            claimed_at_ms,
            expires_at_ms,
            MAX_COPY_DELIVERY_CLAIM_MS,
        )?;
        validate_limit(limit)?;
        let claimed_at = to_i64(claimed_at_ms)?;
        let expires_at = to_i64(expires_at_ms)?;
        let mut transaction = self.pool().begin().await.map_err(database_error)?;
        let rows = sqlx::query(
            "SELECT j.job_id, j.job_json, j.job_digest, o.claim_epoch \
             FROM venue_copy_jobs j JOIN venue_copy_delivery_outbox o USING (job_id) \
             WHERE j.observer_id = $1 AND j.venue = $2 AND j.mode = 'TEST' \
               AND j.trading_account_id = $3 AND j.expires_at_ms > $4 \
               AND (o.delivery_state = 'pending' \
                    OR (o.delivery_state = 'claimed' AND o.claim_expires_at_ms <= $4)) \
             ORDER BY j.created_at_ms, j.job_id LIMIT $5 FOR UPDATE OF o SKIP LOCKED",
        )
        .bind(&scope.observer_id)
        .bind(scope.venue.as_str())
        .bind(&scope.trading_account_id)
        .bind(claimed_at)
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(database_error)?;
        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let job_id: String = row.try_get("job_id").map_err(database_error)?;
            let job: CopyTestJob = decode(row.try_get("job_json").map_err(database_error)?)?;
            let durable_job_digest = digest(row.try_get("job_digest").map_err(database_error)?)?;
            if expires_at_ms > job.manifest.expires_at_ms {
                return Err(CopyRepositoryError::InvalidData);
            }
            if job.scope != *scope
                || job.manifest.validate(claimed_at_ms).is_err()
                || durable_job_digest != job.job_digest
            {
                return Err(CopyRepositoryError::CorruptData);
            }
            let current_epoch: i64 = row.try_get("claim_epoch").map_err(database_error)?;
            let claim_epoch = current_epoch
                .checked_add(1)
                .ok_or(CopyRepositoryError::NumericRange)?;
            let updated = sqlx::query(
                "UPDATE venue_copy_delivery_outbox SET delivery_state = 'claimed', \
                   claimed_by = $2, claim_epoch = $3, claimed_at_ms = $4, \
                   claim_expires_at_ms = $5, updated_at_ms = $4 WHERE job_id = $1",
            )
            .bind(&job_id)
            .bind(consumer_id)
            .bind(claim_epoch)
            .bind(claimed_at)
            .bind(expires_at)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
            if updated.rows_affected() != 1 {
                return Err(CopyRepositoryError::DeliveryConflict);
            }
            sqlx::query(
                "INSERT INTO venue_copy_delivery_inbox \
                 (job_id, consumer_id, claim_epoch, job_digest, inbox_state, claimed_at_ms, updated_at_ms) \
                 VALUES ($1, $2, $3, $4, 'claimed', $5, $5) \
                 ON CONFLICT (job_id) DO UPDATE SET consumer_id = EXCLUDED.consumer_id, \
                   claim_epoch = EXCLUDED.claim_epoch, job_digest = EXCLUDED.job_digest, \
                   inbox_state = 'claimed', claimed_at_ms = EXCLUDED.claimed_at_ms, \
                   updated_at_ms = EXCLUDED.updated_at_ms",
            )
            .bind(&job_id)
            .bind(consumer_id)
            .bind(claim_epoch)
            .bind(row.try_get::<Vec<u8>, _>("job_digest").map_err(database_error)?)
            .bind(claimed_at)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
            claims.push(CopyDeliveryClaim {
                job,
                consumer_id: consumer_id.to_owned(),
                claim_epoch: from_i64(claim_epoch)?,
                claimed_at_ms,
                expires_at_ms,
            });
        }
        transaction.commit().await.map_err(database_error)?;
        Ok(claims)
    }

    async fn record_copy_receipt(
        &self,
        scoped: &ScopedCopyDeliveryReceipt,
    ) -> Result<CopyApplyResult, CopyRepositoryError> {
        if scoped.claim.consumer_id.trim().is_empty()
            || scoped.claim.claim_epoch == 0
            || scoped.receipt.persisted_at_ms < scoped.claim.claimed_at_ms
        {
            return Err(CopyRepositoryError::InvalidData);
        }
        let job_id = scoped.claim.job.identities.job_id.to_string();
        let mut transaction = self.pool().begin().await.map_err(database_error)?;
        let row = sqlx::query(
            "SELECT j.job_json, o.delivery_state, o.claimed_by, o.claim_epoch, \
                    d.consumer_id AS inbox_consumer, d.claim_epoch AS inbox_epoch \
             FROM venue_copy_jobs j JOIN venue_copy_delivery_outbox o USING (job_id) \
             JOIN venue_copy_delivery_inbox d USING (job_id) \
             WHERE j.job_id = $1 FOR UPDATE OF o, d",
        )
        .bind(&job_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?
        .ok_or(CopyRepositoryError::DeliveryConflict)?;
        let durable_job: CopyTestJob = decode(row.try_get("job_json").map_err(database_error)?)?;
        if durable_job != scoped.claim.job
            || row
                .try_get::<Option<String>, _>("claimed_by")
                .map_err(database_error)?
                .as_deref()
                != Some(scoped.claim.consumer_id.as_str())
            || from_i64(row.try_get("claim_epoch").map_err(database_error)?)?
                != scoped.claim.claim_epoch
            || row
                .try_get::<String, _>("inbox_consumer")
                .map_err(database_error)?
                != scoped.claim.consumer_id
            || from_i64(row.try_get("inbox_epoch").map_err(database_error)?)?
                != scoped.claim.claim_epoch
        {
            return Err(CopyRepositoryError::DeliveryConflict);
        }

        if let Some(existing) = sqlx::query(
            "SELECT receipt_json FROM venue_copy_delivery_receipts \
             WHERE job_id = $1 AND receipt_sequence = $2 FOR SHARE",
        )
        .bind(&job_id)
        .bind(to_i64(scoped.receipt.receipt_sequence)?)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?
        {
            let durable: PersistedDeliveryReceipt =
                decode(existing.try_get("receipt_json").map_err(database_error)?)?;
            if durable == scoped.receipt {
                transaction.commit().await.map_err(database_error)?;
                return Ok(CopyApplyResult::Existing);
            }
            return Err(CopyRepositoryError::ReplayConflict);
        }

        let receipts = load_receipts(&mut transaction, &job_id).await?;
        let mut tracker =
            DeliveryTracker::new(durable_job.manifest.clone(), durable_job.created_at_ms)
                .map_err(|_| CopyRepositoryError::CorruptData)?;
        for receipt in receipts {
            tracker
                .apply_persisted_receipt(receipt)
                .map_err(|_| CopyRepositoryError::CorruptData)?;
        }
        tracker
            .apply_persisted_receipt(scoped.receipt.clone())
            .map_err(|_| CopyRepositoryError::DeliveryConflict)?;

        let delivery_state: String = row.try_get("delivery_state").map_err(database_error)?;
        let next_state = match (delivery_state.as_str(), scoped.receipt.status) {
            ("claimed", DeliveryReceiptStatus::Unknown) => "reconciliation_required",
            ("claimed", DeliveryReceiptStatus::Applied | DeliveryReceiptStatus::Rejected)
            | ("reconciliation_required", DeliveryReceiptStatus::Reconciled) => "settled",
            _ => return Err(CopyRepositoryError::DeliveryConflict),
        };
        let persisted_at = to_i64(scoped.receipt.persisted_at_ms)?;
        sqlx::query(
            "INSERT INTO venue_copy_delivery_receipts \
             (job_id, receipt_sequence, status, receipt_json, persisted_at_ms) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&job_id)
        .bind(to_i64(scoped.receipt.receipt_sequence)?)
        .bind(receipt_status(scoped.receipt.status))
        .bind(encode(&scoped.receipt)?)
        .bind(persisted_at)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        if matches!(
            scoped.receipt.status,
            DeliveryReceiptStatus::Applied | DeliveryReceiptStatus::Reconciled
        ) {
            sqlx::query(
                "INSERT INTO venue_copy_receipt_outbox \
                 (job_id, receipt_sequence, projected, created_at_ms) VALUES ($1, $2, FALSE, $3)",
            )
            .bind(&job_id)
            .bind(to_i64(scoped.receipt.receipt_sequence)?)
            .bind(persisted_at)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
        }
        sqlx::query(
            "UPDATE venue_copy_delivery_inbox SET inbox_state = 'receipt_recorded', updated_at_ms = $2 \
             WHERE job_id = $1",
        )
        .bind(&job_id)
        .bind(persisted_at)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        sqlx::query(
            "UPDATE venue_copy_delivery_outbox SET delivery_state = $2, updated_at_ms = $3 \
             WHERE job_id = $1",
        )
        .bind(&job_id)
        .bind(next_state)
        .bind(persisted_at)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        transaction.commit().await.map_err(database_error)?;
        Ok(CopyApplyResult::Stored)
    }

    async fn project_copy_ledger(
        &self,
        input: &CopyLedgerProjectionInput,
    ) -> Result<CopyApplyResult, CopyRepositoryError> {
        if input.projected_at_ms == 0 {
            return Err(CopyRepositoryError::InvalidData);
        }
        let repair = input
            .plan_repair()
            .map_err(|_| CopyRepositoryError::ProjectionConflict)?;
        let job_id = input.job_id.to_string();
        let receipt_sequence = to_i64(input.receipt_sequence)?;
        let binding_id = input.ledger_entry.binding.follower_binding_id.to_string();
        let account_id = &input.ledger_entry.binding.account_id;
        let projected_at = to_i64(input.projected_at_ms)?;
        let mut transaction = self.pool().begin().await.map_err(database_error)?;

        if let Some(row) = sqlx::query(
            "SELECT projection_digest FROM venue_copy_projection_inbox \
             WHERE job_id = $1 AND receipt_sequence = $2 FOR SHARE",
        )
        .bind(&job_id)
        .bind(receipt_sequence)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?
        {
            let durable = digest(row.try_get("projection_digest").map_err(database_error)?)?;
            if durable == input.projection_digest {
                transaction.commit().await.map_err(database_error)?;
                return Ok(CopyApplyResult::Existing);
            }
            return Err(CopyRepositoryError::ProjectionConflict);
        }

        let row = sqlx::query(
            "SELECT j.job_json, r.status, o.projected FROM venue_copy_jobs j \
             JOIN venue_copy_delivery_receipts r USING (job_id) \
             JOIN venue_copy_receipt_outbox o USING (job_id, receipt_sequence) \
             WHERE j.job_id = $1 AND r.receipt_sequence = $2 FOR UPDATE OF o",
        )
        .bind(&job_id)
        .bind(receipt_sequence)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?
        .ok_or(CopyRepositoryError::ProjectionConflict)?;
        let job: CopyTestJob = decode(row.try_get("job_json").map_err(database_error)?)?;
        let status: String = row.try_get("status").map_err(database_error)?;
        let already_projected: bool = row.try_get("projected").map_err(database_error)?;
        if already_projected
            || !matches!(status.as_str(), "applied" | "reconciled")
            || job.identities.job_id != input.job_id
            || job.manifest.binding != input.ledger_entry.binding
        {
            return Err(CopyRepositoryError::ProjectionConflict);
        }
        let projection_lock = format!("{}|{}|{}", job.scope.venue.as_str(), account_id, binding_id);
        advisory_lock(&mut transaction, &projection_lock, 20_005).await?;

        let rows = sqlx::query(
            "SELECT entry_json FROM venue_copy_ledger \
             WHERE venue = $1 AND mode = 'TEST' AND trading_account_id = $2 \
               AND follower_binding_id = $3 ORDER BY ledger_sequence FOR SHARE",
        )
        .bind(job.scope.venue.as_str())
        .bind(account_id)
        .bind(&binding_id)
        .fetch_all(&mut *transaction)
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
             WHERE venue = $1 AND mode = 'TEST' AND trading_account_id = $2 \
               AND follower_binding_id = $3 FOR UPDATE",
        )
        .bind(job.scope.venue.as_str())
        .bind(account_id)
        .bind(&binding_id)
        .fetch_optional(&mut *transaction)
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
             VALUES ($1, 'TEST', $2, $3, $4, $5, $6, $7, $8, $9, $10)",
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
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        sqlx::query(
            "INSERT INTO venue_copy_drift_projections \
             (venue, mode, trading_account_id, follower_binding_id, position_generation, \
              source_job_id, receipt_sequence, projection_json, projected_at_ms) \
             VALUES ($1, 'TEST', $2, $3, $4, $5, $6, $7, $8) \
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
        .execute(&mut *transaction)
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
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        let updated = sqlx::query(
            "UPDATE venue_copy_receipt_outbox SET projected = TRUE \
             WHERE job_id = $1 AND receipt_sequence = $2 AND projected = FALSE",
        )
        .bind(&job_id)
        .bind(receipt_sequence)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        if updated.rows_affected() != 1 {
            return Err(CopyRepositoryError::ProjectionConflict);
        }
        transaction.commit().await.map_err(database_error)?;
        Ok(CopyApplyResult::Stored)
    }

    async fn load_copy_replay(
        &self,
        scope: &CopyObserverScope,
        replayed_at_ms: u64,
    ) -> Result<CopyCrashReplay, CopyRepositoryError> {
        scope
            .validate()
            .map_err(|_| CopyRepositoryError::InvalidData)?;
        if replayed_at_ms == 0 {
            return Err(CopyRepositoryError::InvalidData);
        }
        let replayed_at = to_i64(replayed_at_ms)?;
        let mut transaction = self.pool().begin().await.map_err(database_error)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
        let observer_cursor = sqlx::query(
            "SELECT last_event_sequence FROM venue_copy_observer_cursors WHERE observer_id = $1",
        )
        .bind(&scope.observer_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?
        .map(|row| row.try_get("last_event_sequence"))
        .transpose()
        .map_err(database_error)?
        .unwrap_or(0);
        let cursor_conflict: bool = sqlx::query(
            "SELECT \
               EXISTS (SELECT 1 FROM venue_copy_observer_outbox o \
                       LEFT JOIN venue_copy_observer_inbox i \
                         ON i.observer_id = o.observer_id \
                        AND i.event_sequence = o.event_sequence \
                       WHERE o.observer_id = $1 AND o.event_sequence <= $2 \
                         AND i.event_sequence IS NULL) \
               OR EXISTS (SELECT 1 FROM venue_copy_observer_inbox i \
                          WHERE i.observer_id = $1 AND i.event_sequence > $2) \
               OR EXISTS (SELECT 1 FROM venue_copy_jobs j \
                          LEFT JOIN venue_copy_observer_inbox i ON i.job_id = j.job_id \
                          WHERE j.observer_id = $1 AND i.job_id IS NULL) AS conflict",
        )
        .bind(&scope.observer_id)
        .bind(observer_cursor)
        .fetch_one(&mut *transaction)
        .await
        .map_err(database_error)?
        .try_get("conflict")
        .map_err(database_error)?;
        if cursor_conflict {
            return Err(CopyRepositoryError::CorruptData);
        }

        let receipt_rows = sqlx::query(
            "SELECT r.job_id, r.receipt_json FROM venue_copy_delivery_receipts r \
             JOIN venue_copy_jobs j USING (job_id) \
             WHERE j.observer_id = $1 AND j.venue = $2 AND j.mode = 'TEST' \
               AND j.trading_account_id = $3 ORDER BY r.job_id, r.receipt_sequence",
        )
        .bind(&scope.observer_id)
        .bind(scope.venue.as_str())
        .bind(&scope.trading_account_id)
        .fetch_all(&mut *transaction)
        .await
        .map_err(database_error)?;
        let mut receipts_by_job = BTreeMap::<String, Vec<PersistedDeliveryReceipt>>::new();
        for row in receipt_rows {
            let job_id: String = row.try_get("job_id").map_err(database_error)?;
            receipts_by_job.entry(job_id).or_default().push(decode(
                row.try_get("receipt_json").map_err(database_error)?,
            )?);
        }

        let rows = sqlx::query(
            "SELECT j.job_id, j.job_json, j.job_digest, o.delivery_state, o.claim_expires_at_ms, \
                    EXISTS (SELECT 1 FROM venue_copy_receipt_outbox ro \
                            WHERE ro.job_id = j.job_id AND ro.projected = FALSE) AS projection_pending \
             FROM venue_copy_jobs j JOIN venue_copy_delivery_outbox o USING (job_id) \
             WHERE j.observer_id = $1 AND j.venue = $2 AND j.mode = 'TEST' \
               AND j.trading_account_id = $3 ORDER BY j.created_at_ms, j.job_id",
        )
        .bind(&scope.observer_id)
        .bind(scope.venue.as_str())
        .bind(&scope.trading_account_id)
        .fetch_all(&mut *transaction)
        .await
        .map_err(database_error)?;
        let mut jobs = Vec::with_capacity(rows.len());
        for row in rows {
            let job_id: String = row.try_get("job_id").map_err(database_error)?;
            let job: CopyTestJob = decode(row.try_get("job_json").map_err(database_error)?)?;
            let durable_job_digest = digest(row.try_get("job_digest").map_err(database_error)?)?;
            if job.scope != *scope
                || job.identities.job_id.to_string() != job_id
                || job.job_digest != durable_job_digest
            {
                return Err(CopyRepositoryError::CorruptData);
            }
            let observed =
                load_observed_at(&mut transaction, scope, job.source_event_sequence).await?;
            job.validate_against(&observed)
                .map_err(|_| CopyRepositoryError::CorruptData)?;
            let receipts = receipts_by_job.remove(&job_id).unwrap_or_default();
            let mut tracker = DeliveryTracker::new(job.manifest.clone(), job.created_at_ms)
                .map_err(|_| CopyRepositoryError::CorruptData)?;
            for receipt in &receipts {
                tracker
                    .apply_persisted_receipt(receipt.clone())
                    .map_err(|_| CopyRepositoryError::CorruptData)?;
            }
            let state: String = row.try_get("delivery_state").map_err(database_error)?;
            let tracker_matches_outbox = matches!(
                (state.as_str(), tracker.state()),
                ("pending" | "claimed", DeliveryState::Pending)
                    | ("reconciliation_required", DeliveryState::Unknown(_))
                    | (
                        "settled",
                        DeliveryState::Applied(_)
                            | DeliveryState::Reconciled(_)
                            | DeliveryState::Rejected(_)
                    )
            );
            if !tracker_matches_outbox {
                return Err(CopyRepositoryError::CorruptData);
            }
            let delivery_state = match state.as_str() {
                "pending" | "claimed" if job.manifest.expires_at_ms <= replayed_at_ms => {
                    CopyReplayDeliveryState::Expired
                }
                "pending" => CopyReplayDeliveryState::Redeliverable,
                "claimed"
                    if row
                        .try_get::<Option<i64>, _>("claim_expires_at_ms")
                        .map_err(database_error)?
                        .is_some_and(|expiry| expiry <= replayed_at) =>
                {
                    CopyReplayDeliveryState::Redeliverable
                }
                "claimed" => CopyReplayDeliveryState::Claimed,
                "reconciliation_required" => CopyReplayDeliveryState::ReconciliationRequired,
                "settled" => CopyReplayDeliveryState::Settled,
                _ => return Err(CopyRepositoryError::CorruptData),
            };
            jobs.push(CopyReplayJob {
                job,
                delivery_state,
                receipts,
                projection_pending: row.try_get("projection_pending").map_err(database_error)?,
            });
        }
        if !receipts_by_job.is_empty() {
            return Err(CopyRepositoryError::CorruptData);
        }

        let ledger_rows = sqlx::query(
            "SELECT follower_binding_id, entry_json FROM venue_copy_ledger \
             WHERE venue = $1 AND mode = 'TEST' AND trading_account_id = $2 \
             ORDER BY follower_binding_id, ledger_sequence",
        )
        .bind(scope.venue.as_str())
        .bind(&scope.trading_account_id)
        .fetch_all(&mut *transaction)
        .await
        .map_err(database_error)?;
        let mut ledgers = BTreeMap::new();
        let mut ledger_entries = Vec::with_capacity(ledger_rows.len());
        for row in ledger_rows {
            let binding_id: String = row.try_get("follower_binding_id").map_err(database_error)?;
            let entry: venue_copy::LedgerEntry =
                decode(row.try_get("entry_json").map_err(database_error)?)?;
            let ledger = ledgers
                .entry(binding_id)
                .or_insert_with(|| CopyLedger::new(entry.binding.clone()));
            ledger
                .apply(entry.clone())
                .map_err(|_| CopyRepositoryError::CorruptData)?;
            ledger_entries.push(entry);
        }
        let drift_rows = sqlx::query(
            "SELECT projection_json FROM venue_copy_drift_projections \
             WHERE venue = $1 AND mode = 'TEST' AND trading_account_id = $2 \
             ORDER BY follower_binding_id",
        )
        .bind(scope.venue.as_str())
        .bind(&scope.trading_account_id)
        .fetch_all(&mut *transaction)
        .await
        .map_err(database_error)?;
        let drift_projections = drift_rows
            .into_iter()
            .map(|row| decode(row.try_get("projection_json").map_err(database_error)?))
            .collect::<Result<Vec<_>, _>>()?;
        transaction.commit().await.map_err(database_error)?;
        Ok(CopyCrashReplay {
            observer_cursor,
            jobs,
            ledger_entries,
            drift_projections,
        })
    }
}

async fn load_next_observed_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    scope: &CopyObserverScope,
    cursor: i64,
) -> Result<Option<ObservedCopyIntent>, CopyRepositoryError> {
    let row = sqlx::query(
        "SELECT o.event_sequence, o.event_digest, i.intent_json, i.intent_digest, \
                s.snapshot_json, s.snapshot_digest \
         FROM venue_copy_observer_outbox o \
         JOIN venue_copy_leader_intents i USING (intent_id) \
         JOIN venue_copy_leader_snapshots s USING (snapshot_id) \
         WHERE o.observer_id = $1 AND i.venue = $2 AND i.mode = 'TEST' \
           AND i.trading_account_id = $3 AND o.event_sequence > $4 \
         ORDER BY o.event_sequence LIMIT 1 FOR UPDATE OF o",
    )
    .bind(&scope.observer_id)
    .bind(scope.venue.as_str())
    .bind(&scope.trading_account_id)
    .bind(cursor)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database_error)?;
    row.map(|row| observed_from_row(row, scope)).transpose()
}

async fn persist_planned_copy_job(
    transaction: &mut Transaction<'_, Postgres>,
    lease: &CopyObserverLease,
    planned: &PlannedCopyJob,
    previous_cursor: i64,
    planned_at: i64,
) -> Result<(), CopyRepositoryError> {
    let observed = &planned.observed;
    let job = &planned.job;
    if observed.envelope.scope != lease.scope
        || observed.event_sequence <= previous_cursor
        || job.validate_against(observed).is_err()
    {
        return Err(CopyRepositoryError::InvalidData);
    }
    let job_id = job.identities.job_id.to_string();
    advisory_lock(transaction, &job_id, 20_004).await?;
    let identity_conflict = sqlx::query(
        "SELECT 1 FROM venue_copy_jobs WHERE job_id = $1 OR idempotency_key = $2 LIMIT 1 FOR SHARE",
    )
    .bind(&job_id)
    .bind(job.identities.idempotency_key.to_string())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database_error)?;
    if identity_conflict.is_some() {
        return Err(CopyRepositoryError::ReplayConflict);
    }
    sqlx::query(
        "INSERT INTO venue_copy_jobs \
         (job_id, observer_id, source_event_sequence, intent_id, venue, mode, \
          trading_account_id, idempotency_key, follower_binding_id, manifest_json, job_json, \
          job_digest, created_at_ms, expires_at_ms) \
         VALUES ($1, $2, $3, $4, $5, 'TEST', $6, $7, $8, $9, $10, $11, $12, $13)",
    )
    .bind(&job_id)
    .bind(&job.scope.observer_id)
    .bind(job.source_event_sequence)
    .bind(job.intent_id.to_string())
    .bind(job.scope.venue.as_str())
    .bind(&job.scope.trading_account_id)
    .bind(job.identities.idempotency_key.to_string())
    .bind(job.manifest.binding.follower_binding_id.to_string())
    .bind(encode(&job.manifest)?)
    .bind(encode(job)?)
    .bind(job.job_digest.to_vec())
    .bind(to_i64(job.created_at_ms)?)
    .bind(to_i64(job.manifest.expires_at_ms)?)
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    sqlx::query(
        "INSERT INTO venue_copy_plans \
         (job_id, venue, mode, trading_account_id, source_event_sequence, \
          capital_snapshot_json, target_exposure_json, plan_digest, planned_at_ms) \
         VALUES ($1, $2, 'TEST', $3, $4, $5, $6, $7, $8)",
    )
    .bind(&job_id)
    .bind(job.scope.venue.as_str())
    .bind(&job.scope.trading_account_id)
    .bind(job.source_event_sequence)
    .bind(encode(&planned.frozen_capital)?)
    .bind(encode(&planned.target)?)
    .bind(job.job_digest.to_vec())
    .bind(planned_at)
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    sqlx::query(
        "INSERT INTO venue_copy_observer_inbox \
         (observer_id, event_sequence, event_digest, job_id, consumed_at_ms) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(&lease.scope.observer_id)
    .bind(observed.event_sequence)
    .bind(observed.event_digest.to_vec())
    .bind(&job_id)
    .bind(planned_at)
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    sqlx::query(
        "INSERT INTO venue_copy_delivery_outbox \
         (job_id, delivery_state, claimed_by, claim_epoch, claimed_at_ms, \
          claim_expires_at_ms, updated_at_ms) \
         VALUES ($1, 'pending', NULL, 0, NULL, NULL, $2)",
    )
    .bind(&job_id)
    .bind(planned_at)
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    let updated = sqlx::query(
        "UPDATE venue_copy_observer_cursors \
         SET last_event_sequence = $2, updated_at_ms = $3 \
         WHERE observer_id = $1 AND last_event_sequence = $4",
    )
    .bind(&lease.scope.observer_id)
    .bind(observed.event_sequence)
    .bind(planned_at)
    .bind(previous_cursor)
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    if updated.rows_affected() != 1 {
        return Err(CopyRepositoryError::CursorConflict);
    }
    Ok(())
}

async fn advisory_lock(
    transaction: &mut Transaction<'_, Postgres>,
    value: &str,
    seed: i64,
) -> Result<(), CopyRepositoryError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, $2))")
        .bind(value)
        .bind(seed)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    Ok(())
}

async fn ensure_observer_scope(
    transaction: &mut Transaction<'_, Postgres>,
    scope: &CopyObserverScope,
) -> Result<(), CopyRepositoryError> {
    sqlx::query(
        "INSERT INTO venue_copy_observer_scopes \
         (observer_id, venue, mode, trading_account_id) VALUES ($1, $2, 'TEST', $3) \
         ON CONFLICT (observer_id) DO NOTHING",
    )
    .bind(&scope.observer_id)
    .bind(scope.venue.as_str())
    .bind(&scope.trading_account_id)
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    let row = sqlx::query(
        "SELECT venue, trading_account_id FROM venue_copy_observer_scopes \
         WHERE observer_id = $1 FOR SHARE",
    )
    .bind(&scope.observer_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(database_error)?;
    let durable = CopyObserverScope {
        observer_id: scope.observer_id.clone(),
        venue: parse_venue(row.try_get("venue").map_err(database_error)?)?,
        trading_account_id: row.try_get("trading_account_id").map_err(database_error)?,
    };
    if durable != *scope {
        return Err(CopyRepositoryError::ReplayConflict);
    }
    Ok(())
}

async fn lock_and_validate_lease(
    transaction: &mut Transaction<'_, Postgres>,
    lease: &CopyObserverLease,
    now_ms: i64,
) -> Result<(), CopyRepositoryError> {
    let row = sqlx::query(
        "SELECT venue, trading_account_id, holder_id, lease_epoch, acquired_at_ms, expires_at_ms, \
                lease_kind, mutation_authority \
         FROM venue_copy_observer_leases WHERE observer_id = $1 FOR SHARE",
    )
    .bind(&lease.scope.observer_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database_error)?
    .ok_or(CopyRepositoryError::LeaseConflict)?;
    let durable_scope = CopyObserverScope {
        observer_id: lease.scope.observer_id.clone(),
        venue: parse_venue(row.try_get("venue").map_err(database_error)?)?,
        trading_account_id: row.try_get("trading_account_id").map_err(database_error)?,
    };
    let mutation_authority: bool = row.try_get("mutation_authority").map_err(database_error)?;
    let kind: String = row.try_get("lease_kind").map_err(database_error)?;
    if durable_scope != lease.scope
        || row
            .try_get::<String, _>("holder_id")
            .map_err(database_error)?
            != lease.holder_id
        || from_i64(row.try_get("lease_epoch").map_err(database_error)?)? != lease.lease_epoch
        || from_i64(row.try_get("acquired_at_ms").map_err(database_error)?)? != lease.acquired_at_ms
        || from_i64(row.try_get("expires_at_ms").map_err(database_error)?)? != lease.expires_at_ms
        || row
            .try_get::<i64, _>("expires_at_ms")
            .map_err(database_error)?
            <= now_ms
        || mutation_authority
        || kind != "COPY_TEST_OBSERVER"
    {
        return Err(CopyRepositoryError::LeaseConflict);
    }
    Ok(())
}

async fn load_observed_after(
    transaction: &mut Transaction<'_, Postgres>,
    scope: &CopyObserverScope,
    cursor: i64,
    limit: i64,
) -> Result<Vec<ObservedCopyIntent>, CopyRepositoryError> {
    let rows = sqlx::query(
        "SELECT o.event_sequence, o.event_digest, i.intent_json, i.intent_digest, \
                s.snapshot_json, s.snapshot_digest \
         FROM venue_copy_observer_outbox o \
         JOIN venue_copy_leader_intents i USING (intent_id) \
         JOIN venue_copy_leader_snapshots s USING (snapshot_id) \
         WHERE o.observer_id = $1 AND i.venue = $2 AND i.mode = 'TEST' \
           AND i.trading_account_id = $3 AND o.event_sequence > $4 \
         ORDER BY o.event_sequence LIMIT $5",
    )
    .bind(&scope.observer_id)
    .bind(scope.venue.as_str())
    .bind(&scope.trading_account_id)
    .bind(cursor)
    .bind(limit)
    .fetch_all(&mut **transaction)
    .await
    .map_err(database_error)?;
    rows.into_iter()
        .map(|row| observed_from_row(row, scope))
        .collect()
}

async fn load_observed_at(
    transaction: &mut Transaction<'_, Postgres>,
    scope: &CopyObserverScope,
    event_sequence: i64,
) -> Result<ObservedCopyIntent, CopyRepositoryError> {
    let row = sqlx::query(
        "SELECT o.event_sequence, o.event_digest, i.intent_json, i.intent_digest, \
                s.snapshot_json, s.snapshot_digest \
         FROM venue_copy_observer_outbox o \
         JOIN venue_copy_leader_intents i USING (intent_id) \
         JOIN venue_copy_leader_snapshots s USING (snapshot_id) \
         WHERE o.observer_id = $1 AND i.venue = $2 AND i.mode = 'TEST' \
           AND i.trading_account_id = $3 AND o.event_sequence = $4",
    )
    .bind(&scope.observer_id)
    .bind(scope.venue.as_str())
    .bind(&scope.trading_account_id)
    .bind(event_sequence)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database_error)?
    .ok_or(CopyRepositoryError::CorruptData)?;
    observed_from_row(row, scope)
}

fn observed_from_row(
    row: sqlx::postgres::PgRow,
    scope: &CopyObserverScope,
) -> Result<ObservedCopyIntent, CopyRepositoryError> {
    let intent: CopyLeaderIntent = decode(row.try_get("intent_json").map_err(database_error)?)?;
    let snapshot: CopyLeaderSnapshot =
        decode(row.try_get("snapshot_json").map_err(database_error)?)?;
    let intent_digest = digest(row.try_get("intent_digest").map_err(database_error)?)?;
    let snapshot_digest = digest(row.try_get("snapshot_digest").map_err(database_error)?)?;
    if intent.intent_digest != intent_digest || snapshot.snapshot_digest != snapshot_digest {
        return Err(CopyRepositoryError::CorruptData);
    }
    Ok(ObservedCopyIntent {
        event_sequence: row.try_get("event_sequence").map_err(database_error)?,
        event_digest: digest(row.try_get("event_digest").map_err(database_error)?)?,
        envelope: CopyLeaderEnvelope {
            scope: scope.clone(),
            intent,
            snapshot,
            outbox_digest: digest(row.try_get("event_digest").map_err(database_error)?)?,
        },
    })
}

async fn load_receipts(
    transaction: &mut Transaction<'_, Postgres>,
    job_id: &str,
) -> Result<Vec<PersistedDeliveryReceipt>, CopyRepositoryError> {
    let rows = sqlx::query(
        "SELECT receipt_json FROM venue_copy_delivery_receipts \
         WHERE job_id = $1 ORDER BY receipt_sequence FOR SHARE",
    )
    .bind(job_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(database_error)?;
    rows.into_iter()
        .map(|row| decode(row.try_get("receipt_json").map_err(database_error)?))
        .collect()
}

fn scope_from_row(row: &sqlx::postgres::PgRow) -> Result<CopyObserverScope, CopyRepositoryError> {
    Ok(CopyObserverScope {
        observer_id: row.try_get("observer_id").map_err(database_error)?,
        venue: parse_venue(row.try_get("venue").map_err(database_error)?)?,
        trading_account_id: row.try_get("trading_account_id").map_err(database_error)?,
    })
}

fn parse_venue(value: String) -> Result<venue_control_protocol::VenueId, CopyRepositoryError> {
    value.parse().map_err(|_| CopyRepositoryError::CorruptData)
}

fn validate_window(
    holder_id: &str,
    starts_at_ms: u64,
    expires_at_ms: u64,
    maximum_ms: u64,
) -> Result<(), CopyRepositoryError> {
    let ttl = expires_at_ms
        .checked_sub(starts_at_ms)
        .ok_or(CopyRepositoryError::InvalidData)?;
    if holder_id.trim().is_empty() || starts_at_ms == 0 || ttl == 0 || ttl > maximum_ms {
        return Err(CopyRepositoryError::InvalidData);
    }
    Ok(())
}

fn validate_limit(limit: u32) -> Result<(), CopyRepositoryError> {
    if limit == 0 || limit > 1_000 {
        return Err(CopyRepositoryError::InvalidData);
    }
    Ok(())
}

fn encode<T: serde::Serialize>(value: &T) -> Result<serde_json::Value, CopyRepositoryError> {
    serde_json::to_value(value).map_err(|_| CopyRepositoryError::CorruptData)
}

fn decode<T: serde::de::DeserializeOwned>(
    value: serde_json::Value,
) -> Result<T, CopyRepositoryError> {
    serde_json::from_value(value).map_err(|_| CopyRepositoryError::CorruptData)
}

fn digest(value: Vec<u8>) -> Result<[u8; 32], CopyRepositoryError> {
    value
        .try_into()
        .map_err(|_| CopyRepositoryError::CorruptData)
}

fn to_i64(value: u64) -> Result<i64, CopyRepositoryError> {
    i64::try_from(value).map_err(|_| CopyRepositoryError::NumericRange)
}

fn from_i64(value: i64) -> Result<u64, CopyRepositoryError> {
    u64::try_from(value).map_err(|_| CopyRepositoryError::CorruptData)
}

const fn receipt_status(status: DeliveryReceiptStatus) -> &'static str {
    match status {
        DeliveryReceiptStatus::Applied => "applied",
        DeliveryReceiptStatus::Unknown => "unknown",
        DeliveryReceiptStatus::Reconciled => "reconciled",
        DeliveryReceiptStatus::Rejected => "rejected",
    }
}

fn database_error(_: sqlx::Error) -> CopyRepositoryError {
    CopyRepositoryError::Database
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_hard_fences_test_coordination_from_mutation_writer() {
        assert!(MIGRATION_0002.contains("CHECK (mode = 'TEST')"));
        assert!(MIGRATION_0002.contains("CHECK (mutation_authority = FALSE)"));
        assert!(MIGRATION_0002.contains("COPY_TEST_OBSERVER"));
        assert!(MIGRATION_0002.contains("venue_copy_observer_outbox"));
        assert!(MIGRATION_0002.contains("venue_copy_observer_inbox"));
        assert!(MIGRATION_0002.contains("venue_copy_delivery_outbox"));
        assert!(MIGRATION_0002.contains("venue_copy_delivery_inbox"));
        assert!(MIGRATION_0002.contains("venue_copy_projection_inbox"));
        assert!(MIGRATION_0002.contains("reconciliation_required"));
        assert!(!MIGRATION_0002.contains("writer_generation"));
        assert!(!MIGRATION_0002.contains("dispatch_permit"));
    }

    #[test]
    fn claim_and_observer_windows_are_bounded() {
        assert!(validate_window("holder", 10, 11, 1).is_ok());
        assert_eq!(
            validate_window("holder", 10, 12, 1),
            Err(CopyRepositoryError::InvalidData)
        );
        assert_eq!(validate_limit(0), Err(CopyRepositoryError::InvalidData));
    }
}
