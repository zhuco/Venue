use super::*;

pub(super) async fn store_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
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
    advisory_lock(transaction, &envelope.scope.observer_id, 20_003).await?;
    ensure_observer_scope(transaction, &envelope.scope).await?;
    advisory_lock(transaction, &intent_id, 20_001).await?;
    advisory_lock(transaction, &snapshot_id, 20_002).await?;

    if let Some(row) = sqlx::query(
            "SELECT i.observer_id, i.venue, i.mode, i.trading_account_id, i.intent_json, i.intent_digest, \
                    s.snapshot_json, s.snapshot_digest, o.event_digest, o.event_sequence \
             FROM venue_copy_leader_intents i \
             JOIN venue_copy_leader_snapshots s USING (snapshot_id) \
             JOIN venue_copy_observer_outbox o USING (intent_id) \
             WHERE i.intent_id = $1 FOR SHARE OF i, s, o",
        )
        .bind(&intent_id)
        .fetch_optional(&mut **transaction)
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
                return Ok(CopyStoreResult::Existing { sequence });
            }
            return Err(CopyRepositoryError::ReplayConflict);
        }

    if let Some(row) = sqlx::query(
        "SELECT observer_id, venue, mode, trading_account_id, snapshot_json, snapshot_digest \
             FROM venue_copy_leader_snapshots WHERE snapshot_id = $1 FOR SHARE",
    )
    .bind(&snapshot_id)
    .fetch_optional(&mut **transaction)
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
                 VALUES ($1, $2, $3, 'LIVE', $4, $5, $6, $7, $8, $9)",
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
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    }

    sqlx::query(
        "INSERT INTO venue_copy_leader_intents \
             (intent_id, observer_id, venue, mode, trading_account_id, snapshot_id, intent_digest, \
              intent_json, observed_at_ms, stored_at_ms) \
             VALUES ($1, $2, $3, 'LIVE', $4, $5, $6, $7, $8, $9)",
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
    .execute(&mut **transaction)
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
    .fetch_one(&mut **transaction)
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
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    Ok(CopyStoreResult::Inserted { sequence })
}
