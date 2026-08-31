use super::*;
use venue_copy::CopyExecutionPhase;

#[tokio::test]
async fn immutable_target_and_cross_zero_history_survive_projection_restarts()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url() else {
        if env::var("VENUE_CONTROL_POSTGRES_REQUIRED").as_deref() == Ok("1") {
            return Err("required PostgreSQL integration database is missing".into());
        }
        return Ok(());
    };
    let fixture = PgFixture::create(&database_url, "execution_contract").await?;
    fixture.migrate_twice().await?;
    install_account_scope(&fixture.pool).await?;
    let repository = PgControlRepository::new(fixture.pool.clone());
    let scope = scope("execution-contract");
    let worker = make_worker(repository.clone(), scope.clone(), "planner")?;
    let now = 50_000;
    let planned = plan_one(&repository, &worker, &scope, 60, now).await?;
    let mut initial = AuthoritativePositionSnapshot {
        binding: planned.job.manifest.binding.clone(),
        generation: 1,
        observed_at_ms: now,
        expires_at_ms: now + 1_000,
        exposure: planned.frozen_capital.follower_managed_exposure.clone(),
        fact_digest: [100; 32],
    };
    initial.exposure.value = Decimal::from(-20);
    let mut reduce = CopyExecutionProjectionInput {
        job_id: planned.job.identities.job_id,
        execution: CopyExecutionResult {
            request: plan_copy_execution(
                &planned.job.manifest,
                &planned.target,
                &initial,
                now + 3,
            )?,
            state: CopyExecutionState::Prepared,
            command_id: Some("reduce-child".to_owned()),
            fact_digest: [0; 32],
            reconciled_position: None,
            observed_at_ms: now + 3,
        },
    };
    assert_eq!(
        reduce.execution.request.phase,
        CopyExecutionPhase::ReduceToZero
    );
    let mut changed_target = reduce.clone();
    changed_target.execution.request.target_exposure.value += Decimal::ONE;
    assert!(worker.record_execution(&changed_target).await.is_err());
    let mut wrong_delta = reduce.clone();
    wrong_delta.execution.request.requested_delta_exposure.value += Decimal::ONE;
    assert!(worker.record_execution(&wrong_delta).await.is_err());
    assert_eq!(
        scalar_i64(
            &fixture.pool,
            "SELECT count(*) FROM venue_copy_execution_results"
        )
        .await?,
        0
    );
    worker.record_execution(&reduce).await?;

    let mut zero = initial.clone();
    zero.generation = 2;
    zero.observed_at_ms = now + 4;
    zero.exposure.value = Decimal::ZERO;
    zero.fact_digest = [101; 32];
    let mut adjust = CopyExecutionProjectionInput {
        job_id: planned.job.identities.job_id,
        execution: CopyExecutionResult {
            request: plan_copy_execution(&planned.job.manifest, &planned.target, &zero, now + 5)?,
            state: CopyExecutionState::Prepared,
            command_id: Some("adjust-child".to_owned()),
            fact_digest: [0; 32],
            reconciled_position: None,
            observed_at_ms: now + 5,
        },
    };
    // A newer claimed zero position alone cannot skip the first child's signed completion.
    assert!(worker.record_execution(&adjust).await.is_err());
    reduce.execution.state = CopyExecutionState::Reconciled;
    reduce.execution.fact_digest = [102; 32];
    reduce.execution.observed_at_ms = now + 4;
    reduce.execution.reconciled_position = Some(zero.clone());
    let mut residual = reduce.clone();
    residual
        .execution
        .reconciled_position
        .as_mut()
        .ok_or("closing fact")?
        .exposure
        .value = Decimal::ONE;
    assert!(worker.record_execution(&residual).await.is_err());
    let mut foreign_asset = reduce.clone();
    foreign_asset
        .execution
        .reconciled_position
        .as_mut()
        .ok_or("closing fact")?
        .exposure
        .asset = Asset::new("USDC")?;
    assert!(worker.record_execution(&foreign_asset).await.is_err());
    worker.record_execution(&reduce).await?;

    // The repository is reconstructed, not an in-memory phase tracker.
    let restarted = make_worker(
        PgControlRepository::new(fixture.pool.clone()),
        scope,
        "restarted",
    )?;
    let mut nonzero = zero.clone();
    nonzero.generation = 3;
    nonzero.exposure.value = Decimal::ONE;
    let mut moved = adjust.clone();
    moved.execution.request =
        plan_copy_execution(&planned.job.manifest, &planned.target, &nonzero, now + 5)?;
    assert!(restarted.record_execution(&moved).await.is_err());
    let mut old_generation = adjust.clone();
    old_generation.execution.request.position_generation = 1;
    assert!(restarted.record_execution(&old_generation).await.is_err());
    assert_eq!(
        restarted.record_execution(&adjust).await?,
        CopyApplyResult::Stored
    );
    assert_eq!(
        restarted.record_execution(&reduce).await?,
        CopyApplyResult::Existing
    );
    let mut late_reduce = reduce.clone();
    late_reduce.execution.fact_digest = [103; 32];
    assert!(restarted.record_execution(&late_reduce).await.is_err());

    let mut final_position = zero;
    final_position.generation = 3;
    final_position.observed_at_ms = now + 6;
    final_position.exposure = planned.target.target_exposure.clone();
    final_position.fact_digest = [104; 32];
    adjust.execution.state = CopyExecutionState::Reconciled;
    adjust.execution.reconciled_position = Some(final_position);
    adjust.execution.fact_digest = [105; 32];
    adjust.execution.observed_at_ms = now + 6;
    assert_eq!(
        restarted.record_execution(&adjust).await?,
        CopyApplyResult::Stored
    );
    assert_eq!(
        scalar_i64(
            &fixture.pool,
            "SELECT count(*) FROM venue_copy_execution_results WHERE execution_state='reconciled'"
        )
        .await?,
        2
    );
    fixture.cleanup().await?;
    Ok(())
}
