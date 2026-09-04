use super::*;

impl BinanceGridStore {
    pub async fn update_convergence(
        &self,
        update: &GridConvergenceUpdate,
        now_ms: u64,
    ) -> Result<GridInstanceSummary, GridStoreError> {
        validate_ids(&[&update.instance_id])?;
        if update.expected_instance_revision == 0
            || !convergence_state_allows_update(update.expected_state)
            || (update.expected_state == GridInstanceState::Paused && update.dirty)
            || update.expected_plan_revision == 0
            || update.next_plan_revision != update.expected_plan_revision
            || update.last_facts_ms == 0
            || update.last_facts_ms > now_ms
            || update.consecutive_failures > MAX_GRID_CONSECUTIVE_FAILURES
            || (!update.dirty && update.consecutive_failures != 0)
        {
            return Err(GridStoreError::Invalid);
        }
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let row = sqlx::query(
            "SELECT i.owner_user_id,i.revision,i.current_config_revision,i.plan_revision,\
             i.instance_state,i.attention_code,i.convergence_started_ms,i.desired_digest,\
             c.config_json \
             FROM venue_binance_grid_instances i \
             JOIN venue_binance_grid_config_revisions c ON c.instance_id=i.instance_id \
              AND c.config_revision=i.current_config_revision \
             WHERE i.instance_id=$1 FOR UPDATE",
        )
        .bind(&update.instance_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?
        .ok_or(GridStoreError::Forbidden)?;
        let owner_user_id: String = row.try_get("owner_user_id").map_err(corrupt_row)?;
        let instance_revision = unsigned(row.try_get("revision").map_err(corrupt_row)?)?;
        let config_revision = unsigned(
            row.try_get("current_config_revision")
                .map_err(corrupt_row)?,
        )?;
        let plan_revision = unsigned(row.try_get("plan_revision").map_err(corrupt_row)?)?;
        let state = decode_state(row.try_get("instance_state").map_err(corrupt_row)?)?;
        let durable_desired = row
            .try_get::<Option<Vec<u8>>, _>("desired_digest")
            .map_err(corrupt_row)?;
        if !convergence_cas_matches(update, instance_revision, state)
            || plan_revision != update.expected_plan_revision
            || durable_desired.as_deref() != Some(update.desired_digest.as_slice())
        {
            return Err(GridStoreError::Conflict);
        }
        let config: GridConfig =
            serde_json::from_value(row.try_get("config_json").map_err(corrupt_row)?)
                .map_err(|_| GridStoreError::Corrupt)?;
        config.validate().map_err(|_| GridStoreError::Corrupt)?;
        let prior_started = row
            .try_get::<Option<i64>, _>("convergence_started_ms")
            .map_err(corrupt_row)?
            .map(unsigned)
            .transpose()?;
        let started = if update.dirty {
            if update.next_plan_revision != update.expected_plan_revision {
                now_ms
            } else {
                prior_started.unwrap_or(now_ms)
            }
        } else {
            0
        };
        let timeout = update.dirty
            && now_ms.saturating_sub(started) > config.reset_policy.convergence_timeout_ms;
        let failures = update.dirty
            && update.consecutive_failures >= config.reset_policy.max_consecutive_failures;
        let first_rejected =
            rejection::first_exchange_rejection_ms(&mut *tx, &update.instance_id, config_revision)
                .await?;
        let rejection_due = rejection::rejection_reset_due(first_rejected, now_ms);
        let reset_required = state != GridInstanceState::Paused
            && if first_rejected.is_some() {
                rejection_due
            } else {
                timeout || failures
            };
        let entering_reset = reset_required && state != GridInstanceState::ResetRequired;
        let next_state = if reset_required {
            GridInstanceState::ResetRequired
        } else if !update.dirty
            && matches!(
                state,
                GridInstanceState::StartPending | GridInstanceState::Blocked
            )
        {
            GridInstanceState::Running
        } else {
            state
        };
        let attention: Option<String> = if reset_required {
            Some(
                if rejection_due {
                    "exchange_rejection_delay_elapsed"
                } else if failures {
                    "consecutive_failures"
                } else {
                    "convergence_timeout"
                }
                .to_owned(),
            )
        } else if matches!(
            next_state,
            GridInstanceState::Blocked
                | GridInstanceState::ResetRequired
                | GridInstanceState::NeedsAttention
        ) {
            row.try_get::<Option<String>, _>("attention_code")
                .map_err(corrupt_row)?
        } else {
            None
        };
        if entering_reset {
            cancel_pending_risk_commands(&mut tx, &update.instance_id, now_ms).await?;
        }
        let next_config_revision = if entering_reset {
            let trigger = attention.as_deref().ok_or(GridStoreError::Corrupt)?;
            let synthetic_request_id = synthetic_config_request_id(
                "automatic-reset",
                &update.instance_id,
                trigger,
                config_revision,
            );
            clone_config_revision(
                &mut tx,
                &update.instance_id,
                config_revision,
                &synthetic_request_id,
                now_ms,
            )
            .await?
        } else {
            config_revision
        };
        let persisted_desired = if reset_required {
            sqlx::query("DELETE FROM venue_binance_grid_desired_orders WHERE instance_id=$1")
                .bind(&update.instance_id)
                .execute(&mut *tx)
                .await
                .map_err(database_error)?;
            sqlx::query("DELETE FROM venue_binance_grid_anchors WHERE instance_id=$1")
                .bind(&update.instance_id)
                .execute(&mut *tx)
                .await
                .map_err(database_error)?;
            empty_desired_digest()
        } else {
            update.desired_digest
        };
        let updated = sqlx::query(
            "UPDATE venue_binance_grid_instances SET plan_revision=$1,current_config_revision=$2,\
             desired_digest=$3,dirty=$4,convergence_started_ms=$5,consecutive_failures=$6,\
             last_facts_ms=$7,instance_state=$8,attention_code=$9,revision=revision+1,\
             updated_ms=$10 WHERE instance_id=$11 AND revision=$12 AND plan_revision=$13 \
              AND instance_state=$14",
        )
        .bind(integer(update.next_plan_revision)?)
        .bind(integer(next_config_revision)?)
        .bind(persisted_desired.as_slice())
        .bind(update.dirty || reset_required)
        .bind(if entering_reset {
            Some(ms(now_ms)?)
        } else if update.dirty {
            Some(ms(started)?)
        } else {
            None
        })
        .bind(
            i16::try_from(if reset_required {
                0
            } else {
                update.consecutive_failures
            })
            .map_err(|_| GridStoreError::Invalid)?,
        )
        .bind(ms(update.last_facts_ms)?)
        .bind(state_name(next_state))
        .bind(attention)
        .bind(ms(now_ms)?)
        .bind(&update.instance_id)
        .bind(integer(update.expected_instance_revision)?)
        .bind(integer(update.expected_plan_revision)?)
        .bind(state_name(update.expected_state))
        .execute(&mut *tx)
        .await
        .map_err(database_error)?;
        if updated.rows_affected() != 1 {
            return Err(GridStoreError::Conflict);
        }
        tx.commit().await.map_err(database_error)?;
        self.load_owned(&owner_user_id, &update.instance_id)
            .await?
            .ok_or(GridStoreError::Corrupt)
    }

    pub async fn settle_runtime_state(
        &self,
        instance_id: &str,
        expected: GridInstanceState,
        next: GridInstanceState,
        attention_code: Option<&str>,
        now_ms: u64,
    ) -> Result<GridInstanceSummary, GridStoreError> {
        self.settle_runtime_state_checked(instance_id, None, expected, next, attention_code, now_ms)
            .await
    }

    pub(crate) async fn settle_runtime_state_checked(
        &self,
        instance_id: &str,
        expected_revision: Option<u64>,
        expected: GridInstanceState,
        next: GridInstanceState,
        attention_code: Option<&str>,
        now_ms: u64,
    ) -> Result<GridInstanceSummary, GridStoreError> {
        validate_ids(&[instance_id])?;
        if now_ms == 0
            || !runtime_transition_allowed(expected, next)
            || attention_code_valid(next, attention_code).is_err()
        {
            return Err(GridStoreError::Invalid);
        }
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let locked = sqlx::query(
            "SELECT owner_user_id,revision,current_config_revision,instance_state \
             FROM venue_binance_grid_instances WHERE instance_id=$1 FOR UPDATE",
        )
        .bind(instance_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?
        .ok_or(GridStoreError::Forbidden)?;
        let owner: String = locked.try_get("owner_user_id").map_err(corrupt_row)?;
        let revision = unsigned(locked.try_get("revision").map_err(corrupt_row)?)?;
        let current_config_revision = unsigned(
            locked
                .try_get("current_config_revision")
                .map_err(corrupt_row)?,
        )?;
        let durable_state = decode_state(locked.try_get("instance_state").map_err(corrupt_row)?)?;
        if durable_state != expected || expected_revision.is_some_and(|value| value != revision) {
            return Err(GridStoreError::Conflict);
        }
        let next_config_revision = if next == GridInstanceState::ResetRequired {
            let trigger = attention_code.ok_or(GridStoreError::Invalid)?;
            let synthetic_request_id = synthetic_config_request_id(
                "runtime-reset",
                instance_id,
                trigger,
                current_config_revision,
            );
            clone_config_revision(
                &mut tx,
                instance_id,
                current_config_revision,
                &synthetic_request_id,
                now_ms,
            )
            .await?
        } else {
            current_config_revision
        };
        let clears_desired = matches!(
            next,
            GridInstanceState::ResetRequired | GridInstanceState::Stopped
        );
        if next == GridInstanceState::ResetRequired {
            cancel_pending_risk_commands(&mut tx, instance_id, now_ms).await?;
        }
        if clears_desired {
            sqlx::query("DELETE FROM venue_binance_grid_desired_orders WHERE instance_id=$1")
                .bind(instance_id)
                .execute(&mut *tx)
                .await
                .map_err(database_error)?;
        }
        if next == GridInstanceState::ResetRequired {
            sqlx::query("DELETE FROM venue_binance_grid_anchors WHERE instance_id=$1")
                .bind(instance_id)
                .execute(&mut *tx)
                .await
                .map_err(database_error)?;
        }
        let row = sqlx::query(
            "UPDATE venue_binance_grid_instances SET instance_state=$1,attention_code=$2,\
             desired_digest=CASE WHEN $3 THEN $4 ELSE desired_digest END,\
             dirty=CASE WHEN $1='stopped' OR ($1='running' AND instance_state='reset_required') \
              THEN FALSE WHEN $1='reset_required' THEN TRUE ELSE dirty END,\
             convergence_started_ms=CASE WHEN $1='stopped' OR \
              ($1='running' AND instance_state='reset_required') THEN NULL \
              WHEN $1='reset_required' THEN $5 ELSE convergence_started_ms END,\
             consecutive_failures=CASE WHEN $1 IN ('stopped','reset_required') \
              THEN 0 ELSE consecutive_failures END,current_config_revision=$6,\
             revision=revision+1,updated_ms=$5 WHERE instance_id=$7 AND revision=$8 \
              AND current_config_revision=$9 AND instance_state=$10 RETURNING owner_user_id",
        )
        .bind(state_name(next))
        .bind(attention_code)
        .bind(clears_desired)
        .bind(empty_desired_digest().as_slice())
        .bind(ms(now_ms)?)
        .bind(integer(next_config_revision)?)
        .bind(instance_id)
        .bind(integer(revision)?)
        .bind(integer(current_config_revision)?)
        .bind(state_name(expected))
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?
        .ok_or(GridStoreError::Conflict)?;
        let returned_owner: String = row.try_get("owner_user_id").map_err(corrupt_row)?;
        if returned_owner != owner {
            return Err(GridStoreError::Corrupt);
        }
        tx.commit().await.map_err(database_error)?;
        self.load_owned(&owner, instance_id)
            .await?
            .ok_or(GridStoreError::Corrupt)
    }
}
