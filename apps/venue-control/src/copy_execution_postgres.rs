use super::*;

pub(crate) async fn record_evidence_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    evidence: &venue_control_protocol::CopyExecutionEvidence,
) -> Result<CopyApplyResult, CopyRepositoryError> {
    use venue_control_protocol::{
        CopyExecutionPhaseProjection as Phase, CopyExecutionStateProjection as State,
    };
    use venue_copy::CopyExecutionPhase;
    let execution: CopyExecutionResult = serde_json::from_str(&evidence.result_bytes)
        .map_err(|_| CopyRepositoryError::InvalidData)?;
    let request = &execution.request;
    let expected_phase = match request.phase {
        CopyExecutionPhase::ReduceToZero => Phase::ReduceToZero,
        CopyExecutionPhase::Adjust => Phase::Adjust,
    };
    let expected_state = match execution.state {
        CopyExecutionState::Prepared => State::Prepared,
        CopyExecutionState::Submitted => State::Submitted,
        CopyExecutionState::Accepted => State::Accepted,
        CopyExecutionState::Rejected => State::Rejected,
        CopyExecutionState::Unknown => State::Unknown,
        CopyExecutionState::Reconciled => State::Reconciled,
    };
    if evidence.job_id != request.job_id.to_string()
        || evidence.relation_id != request.binding.relation.relation_id.to_string()
        || evidence.relation_revision != request.binding.relation.revision
        || evidence.binding.trading_account_id != request.binding.account_id
        || evidence.binding.instance_id != request.binding.follower_instance_id
        || evidence.binding.symbol != request.binding.instrument.symbol
        || evidence.phase != expected_phase
        || evidence.state != expected_state
        || evidence.command_id != execution.command_id
        || evidence.observed_ms != execution.observed_at_ms
        || evidence.result_fact_digest != execution.fact_digest
    {
        return Err(CopyRepositoryError::ProjectionConflict);
    }
    // Compare against the immutable delivery, not the current relation/config. Old children
    // must still be recorded after Pause/edit, but cannot be attributed to a different account.
    let exact: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM venue_account_deliveries WHERE delivery_id=$1 \
         AND venue=$2 AND mode='LIVE' AND trading_account_id=$3 AND symbol=$4 \
         AND instance_id=$5 AND config_epoch=$6 AND source_id=$7 AND source_kind='copy_semantic_job')",
    )
    .bind(format!("copy:{}", evidence.job_id)).bind(evidence.binding.venue.as_str())
    .bind(&evidence.binding.trading_account_id).bind(evidence.binding.symbol.to_string())
    .bind(&evidence.binding.instance_id).bind(to_i64(evidence.binding.config_epoch)?)
    .bind(&evidence.job_id).fetch_one(&mut **transaction).await.map_err(database_error)?;
    if !exact {
        return Err(CopyRepositoryError::ProjectionConflict);
    }
    let input = CopyExecutionProjectionInput {
        job_id: request.job_id,
        execution,
    };
    record_in_transaction(transaction, &input).await
}

pub(crate) async fn record_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    input: &CopyExecutionProjectionInput,
) -> Result<CopyApplyResult, CopyRepositoryError> {
    input
        .validate()
        .map_err(|_| CopyRepositoryError::InvalidData)?;
    let job_id = input.job_id.to_string();
    let request = &input.execution.request;
    let row = sqlx::query("SELECT job_json FROM venue_copy_jobs WHERE job_id = $1 FOR UPDATE")
        .bind(&job_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(database_error)?
        .ok_or(CopyRepositoryError::ProjectionConflict)?;
    let job: CopyJob = decode(row.try_get("job_json").map_err(database_error)?)?;
    // A relation edit must not prevent read-only recording of an old child's signed
    // outcome. Immutable job/delivery scope still applies; only new planning requires the
    // current active relation.
    let semantic: crate::CopySemanticJob = decode(job.semantic_job.clone())?;
    request
        .validate_against(&job.manifest, &semantic.target)
        .map_err(|_| CopyRepositoryError::ProjectionConflict)?;
    let stored_results = sqlx::query(
        "SELECT result_json FROM venue_copy_execution_results \
         WHERE job_id = $1 AND delivery_digest = $2 FOR SHARE",
    )
    .bind(&job_id)
    .bind(request.delivery_digest.to_vec())
    .fetch_all(&mut **transaction)
    .await
    .map_err(database_error)?;
    let mut same_phase = None;
    let mut other_phase = None;
    for stored in stored_results {
        let durable: CopyExecutionResult =
            decode(stored.try_get("result_json").map_err(database_error)?)?;
        CopyExecutionProjectionInput {
            job_id: input.job_id,
            execution: durable.clone(),
        }
        .validate()
        .map_err(|_| CopyRepositoryError::CorruptData)?;
        durable
            .request
            .validate_against(&job.manifest, &semantic.target)
            .map_err(|_| CopyRepositoryError::CorruptData)?;
        if durable.request.phase == request.phase {
            if same_phase.replace(durable).is_some() {
                return Err(CopyRepositoryError::CorruptData);
            }
        } else if other_phase.replace(durable).is_some() {
            return Err(CopyRepositoryError::CorruptData);
        }
    }
    validate_phase_order(&input.execution, same_phase.as_ref(), other_phase.as_ref())?;
    if let Some(durable) = same_phase {
        if durable == input.execution {
            return Ok(CopyApplyResult::Existing);
        }
        if durable.request != input.execution.request
            || durable.command_id != input.execution.command_id
            || input.execution.observed_at_ms < durable.observed_at_ms
            || !valid_execution_transition(durable.state, input.execution.state)
        {
            return Err(CopyRepositoryError::ReplayConflict);
        }
        sqlx::query(
            "UPDATE venue_copy_execution_results SET execution_state = $4, result_json = $5, \
             observed_at_ms = $6 WHERE job_id = $1 AND delivery_digest = $2 \
             AND position_generation = $3",
        )
        .bind(&job_id)
        .bind(request.delivery_digest.to_vec())
        .bind(to_i64(request.position_generation)?)
        .bind(copy_execution_state(input.execution.state))
        .bind(encode(&input.execution)?)
        .bind(to_i64(input.execution.observed_at_ms)?)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
        return Ok(CopyApplyResult::Stored);
    }
    sqlx::query(
        "INSERT INTO venue_copy_execution_results \
         (job_id, delivery_digest, position_generation, execution_state, result_json, observed_at_ms) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(&job_id)
    .bind(request.delivery_digest.to_vec())
    .bind(to_i64(request.position_generation)?)
    .bind(copy_execution_state(input.execution.state))
    .bind(encode(&input.execution)?)
    .bind(to_i64(input.execution.observed_at_ms)?)
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    Ok(CopyApplyResult::Stored)
}

fn validate_phase_order(
    incoming: &CopyExecutionResult,
    same: Option<&CopyExecutionResult>,
    other: Option<&CopyExecutionResult>,
) -> Result<(), CopyRepositoryError> {
    use venue_copy::CopyExecutionPhase;
    let Some(other) = other else {
        return Ok(());
    };
    match incoming.request.phase {
        CopyExecutionPhase::ReduceToZero => {
            // A delayed exact echo of Reduce is valid after Adjust. Inserting or changing
            // that first phase after the second phase exists is not a valid history.
            if same != Some(incoming) {
                return Err(CopyRepositoryError::ProjectionConflict);
            }
        }
        CopyExecutionPhase::Adjust => {
            let zero = other
                .reconciled_position
                .as_ref()
                .ok_or(CopyRepositoryError::ProjectionConflict)?;
            if other.state != CopyExecutionState::Reconciled
                || !zero.exposure.value.is_zero()
                || !incoming.request.current_exposure.value.is_zero()
                || incoming.request.position_generation < zero.generation
                || incoming.observed_at_ms < zero.observed_at_ms
            {
                return Err(CopyRepositoryError::ProjectionConflict);
            }
        }
    }
    Ok(())
}
