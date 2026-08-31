use super::*;
use venue_control::{
    CopyExecutionProjectionInput, CopyObserverScope, CopyRelationRepository, CopyWorker,
    CopyWorkerConfig,
};
use venue_control_protocol::{
    CopyLifecyclePolicy, CopyPlanningFact, CopyPlanningFactRole, CopyRelationBinding,
    CopyRelationConfig, CopyRelationUpsertRequest, CopyRiskPolicy, ExecutionFactBinding,
};
use venue_copy::{
    AuthoritativePositionSnapshot, CopyExecutionResult, CopyExecutionState, DriftRepairRequest,
    plan_copy_execution,
};
use venue_domain::domain::{Amount, Asset, InstrumentIdentity, MarketKind};

#[tokio::test]
async fn fresh_node_pair_automatically_plans_once_and_fences_unsettled_jobs()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = PgFixture::create(&url).await?;
    fixture.migrate_twice().await?;
    let repository = PgControlRepository::new(fixture.pool.clone());
    let mut follower = projection(1, 1, [11; 32], [0; 32], 100)?;
    follower.snapshot.strategies[0].kind = StrategyKind::Copy;
    let mut leader = projection(1, 1, [21; 32], [0; 32], 100)?;
    leader.node_id = "leader-node".into();
    leader.binding.instance_id = "leader-grid".into();
    leader.binding.trading_account_id = "00000000-0000-4000-8000-000000000002".into();
    leader.snapshot.accounts[0].trading_account_id = leader.binding.trading_account_id.clone();
    leader.snapshot.strategies[0].trading_account_id = leader.binding.trading_account_id.clone();
    leader.snapshot.strategies[0].instance_id = leader.binding.instance_id.clone();
    repository.merge_node_projection(&follower).await?;
    repository.merge_node_projection(&leader).await?;
    let relation_binding = |binding: &AccountDeliveryBinding| CopyRelationBinding {
        venue: binding.venue,
        mode: binding.mode,
        trading_account_id: binding.trading_account_id.clone(),
        symbol: binding.symbol.clone(),
        instance_id: binding.instance_id.clone(),
    };
    let mut request = CopyRelationUpsertRequest {
        schema_version: CONTROL_SCHEMA_VERSION,
        request_id: "00000000-0000-4000-8000-000000000090".into(),
        expected_revision: Some(0),
        relation: CopyRelationConfig {
            relation_id: "00000000-0000-4000-8000-000000000099".into(),
            leader: relation_binding(&leader.binding),
            follower: relation_binding(&follower.binding),
            allocated_capital: Decimal::TEN,
            multiplier: Decimal::from(2),
            safety_reserve_rate: Decimal::ZERO,
            risk: CopyRiskPolicy {
                max_total_notional: Decimal::TEN,
                max_order_notional: Decimal::TEN,
                max_leverage: Decimal::from(2),
            },
            lifecycle: CopyLifecyclePolicy::Active,
        },
    };
    repository.upsert_copy_relation(&request, 101).await?;
    let worker_config = CopyWorkerConfig {
        mode: GatewayMode::Live,
        scope: CopyObserverScope {
            observer_id: "node-source-worker".into(),
            venue: follower.binding.venue,
            mode: GatewayMode::Live,
            trading_account_id: follower.binding.trading_account_id.clone(),
        },
        worker_id: "source-planner".into(),
        observer_lease_ms: 1000,
        delivery_claim_ms: 1000,
    };
    let worker = CopyWorker::new(repository.clone(), worker_config.clone())?;
    assert!(worker.plan_next(102).await?.is_none());
    advance(&mut follower, 110, [12; 32]);
    follower.copy_planning_facts = vec![fact(
        &follower,
        &request.relation,
        CopyPlanningFactRole::Follower,
        200,
    )?];
    repository.merge_node_projection(&follower).await?;
    assert!(worker.plan_next(111).await?.is_none());
    advance(&mut leader, 120, [22; 32]);
    leader.copy_planning_facts = vec![fact(
        &leader,
        &request.relation,
        CopyPlanningFactRole::Leader,
        200,
    )?];
    repository.merge_node_projection(&leader).await?;
    assert!(worker.plan_next(201).await?.is_none());
    advance(&mut leader, 210, [23; 32]);
    leader.copy_planning_facts = vec![fact(
        &leader,
        &request.relation,
        CopyPlanningFactRole::Leader,
        600,
    )?];
    repository.merge_node_projection(&leader).await?;
    assert!(worker.plan_next(211).await?.is_none()); // no stale follower resurrection
    advance(&mut follower, 220, [13; 32]);
    follower.copy_planning_facts = vec![fact(
        &follower,
        &request.relation,
        CopyPlanningFactRole::Follower,
        600,
    )?];
    repository.merge_node_projection(&follower).await?;
    let planned = worker
        .plan_next(221)
        .await?
        .ok_or("paired facts produced no job")?;
    assert_eq!(planned.target.target_exposure.value, Decimal::from(4));
    assert_eq!(
        planned.frozen_capital.leader_target_exposure.value,
        Decimal::from(20)
    );
    assert_eq!(planned.frozen_capital.exposure_multiplier, Decimal::from(2));
    assert_eq!(
        planned.job.scope.trading_account_id,
        follower.binding.trading_account_id
    );
    assert_eq!(
        planned.job.manifest.binding.instrument.symbol,
        follower.binding.symbol
    );
    assert!(!worker.grants_mutation_authority());
    let restarted = CopyWorker::new(
        PgControlRepository::new(fixture.pool.clone()),
        worker_config,
    )?;
    assert!(restarted.plan_next(222).await?.is_none());
    assert_eq!(restarted.recover(223).await?.jobs.len(), 1);
    sqlx::query("UPDATE venue_copy_jobs SET policy_digest=$1 WHERE job_id=$2")
        .bind(vec![0_u8; 32])
        .bind(planned.job.identities.job_id.to_string())
        .execute(&fixture.pool)
        .await?;
    assert!(matches!(
        restarted.recover(223).await,
        Err(venue_control::CopyWorkerError::Repository(
            venue_control::CopyRepositoryError::CorruptData
        ))
    ));
    sqlx::query("UPDATE venue_copy_jobs SET policy_digest=$1 WHERE job_id=$2")
        .bind(planned.job.manifest.binding.relation.policy_digest.to_vec())
        .bind(planned.job.identities.job_id.to_string())
        .execute(&fixture.pool)
        .await?;
    let claims = repository
        .claim_account_deliveries(&follower.binding, "follower-node", 224, 500, 10)
        .await?;
    assert!(
        claims
            .iter()
            .any(|claim| claim.lease.delivery_id
                == format!("copy:{}", planned.job.identities.job_id))
    );
    advance(&mut leader, 230, [24; 32]);
    let mut changed = fact(
        &leader,
        &request.relation,
        CopyPlanningFactRole::Leader,
        600,
    )?;
    changed.quote_net_exposure.value = Decimal::from(30);
    leader.copy_planning_facts = vec![changed];
    repository.merge_node_projection(&leader).await?;
    sqlx::query("UPDATE venue_copy_jobs SET relation_id=NULL WHERE job_id=$1")
        .bind(planned.job.identities.job_id.to_string())
        .execute(&fixture.pool)
        .await?;
    assert!(matches!(
        restarted.plan_next(231).await,
        Err(venue_control::CopyWorkerError::Repository(
            venue_control::CopyRepositoryError::CorruptData
        ))
    ));
    sqlx::query("UPDATE venue_copy_jobs SET relation_id=$1 WHERE job_id=$2")
        .bind(request.relation.relation_id.clone())
        .bind(planned.job.identities.job_id.to_string())
        .execute(&fixture.pool)
        .await?;
    assert!(restarted.plan_next(231).await?.is_none()); // original physical job unresolved
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM venue_copy_jobs")
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(count, 1);
    let claim = claims
        .iter()
        .find(|claim| claim.lease.delivery_id == format!("copy:{}", planned.job.identities.job_id))
        .ok_or("copy claim")?;
    repository
        .acknowledge_account_delivery(&AccountDeliveryAck {
            schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
            lease: claim.lease.clone(),
            acknowledged_ms: 225,
            durable_inbox_digest: [40; 32],
        })
        .await?;
    let original_position = AuthoritativePositionSnapshot {
        binding: planned.job.manifest.binding.clone(),
        generation: 1,
        observed_at_ms: 220,
        expires_at_ms: 600,
        exposure: planned.frozen_capital.follower_managed_exposure.clone(),
        fact_digest: [41; 32],
    };
    let mut execution = CopyExecutionResult {
        request: plan_copy_execution(
            &planned.job.manifest,
            &planned.target,
            &original_position,
            225,
        )?,
        state: CopyExecutionState::Prepared,
        command_id: Some("target-child".into()),
        fact_digest: [0; 32],
        reconciled_position: None,
        observed_at_ms: 225,
    };
    restarted
        .record_execution(&CopyExecutionProjectionInput {
            job_id: planned.job.identities.job_id,
            execution: execution.clone(),
        })
        .await?;
    repository
        .record_account_delivery_receipt(&AccountDeliveryReceipt {
            schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
            lease: claim.lease.clone(),
            receipt_id: "target-applied".into(),
            state: AccountDeliveryReceiptState::Applied,
            observed_ms: 252,
            account_fact_digest: [42; 32],
            detail: "signed child completed".into(),
        })
        .await?;
    execution.state = CopyExecutionState::Reconciled;
    execution.observed_at_ms = 252;
    execution.fact_digest = [43; 32];
    let mut closing = original_position;
    closing.generation = 2;
    closing.observed_at_ms = 252;
    closing.fact_digest = [44; 32];
    closing.exposure.value = Decimal::from(3); // target 4, signed residual drift 1
    execution.reconciled_position = Some(closing);
    restarted
        .record_execution(&CopyExecutionProjectionInput {
            job_id: planned.job.identities.job_id,
            execution,
        })
        .await?;
    assert!(
        restarted
            .project_next_reconciled_ledger(253)
            .await?
            .is_some()
    );
    assert!(
        !repository
            .load_execution_facts()
            .await?
            .ok_or("ledger UI snapshot")?
            .drift
            .iter()
            .any(|fact| fact.repair_pending)
    );
    // Historical completion alone cannot renew an expired authorization.
    assert!(restarted.plan_next(601).await?.is_none());
    advance(&mut leader, 700, [25; 32]);
    leader.copy_planning_facts = vec![fact(
        &leader,
        &request.relation,
        CopyPlanningFactRole::Leader,
        950,
    )?];
    repository.merge_node_projection(&leader).await?;
    advance(&mut follower, 701, [14; 32]);
    follower.snapshot.accounts[0].private_generation = 3;
    let mut refreshed = fact(
        &follower,
        &request.relation,
        CopyPlanningFactRole::Follower,
        900,
    )?;
    refreshed.private_generation = 3;
    refreshed.quote_net_exposure.value = Decimal::from(3);
    follower.copy_planning_facts = vec![refreshed];
    repository.merge_node_projection(&follower).await?;
    let repair = restarted
        .plan_next(702)
        .await?
        .ok_or("fresh signed drift made no repair job")?;
    let semantic: venue_control::CopySemanticJob =
        serde_json::from_value(repair.job.semantic_job.clone())?;
    let repair_request: DriftRepairRequest =
        serde_json::from_value(semantic.leader_intent["drift_repair"]["request"].clone())?;
    assert_eq!(repair_request.identities, repair.job.identities);
    assert_eq!(
        repair_request.supersedes_job_id,
        planned.job.identities.job_id
    );
    assert_eq!(repair.target.target_exposure.value, Decimal::from(4));
    assert_eq!(repair.target.delta_exposure.value, Decimal::ONE);
    assert_eq!(repair.job.manifest.expires_at_ms, 900);
    assert!(
        repository
            .load_execution_facts()
            .await?
            .ok_or("repair UI snapshot")?
            .drift
            .iter()
            .any(|fact| fact.repair_pending)
    );
    assert!(restarted.plan_next(703).await?.is_none());
    let recovered = restarted.recover(704).await?;
    assert_eq!(recovered.jobs.len(), 2);
    assert_eq!(recovered.drift_projections.len(), 1);
    assert_eq!(recovered.ledger_entries.len(), 1);
    let repair_claims = repository
        .claim_account_deliveries(&follower.binding, "follower-node", 705, 800, 10)
        .await?;
    let repair_claim = repair_claims
        .iter()
        .find(|claim| claim.lease.delivery_id == format!("copy:{}", repair.job.identities.job_id))
        .ok_or("repair delivery")?;
    repository
        .acknowledge_account_delivery(&AccountDeliveryAck {
            schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
            lease: repair_claim.lease.clone(),
            acknowledged_ms: 706,
            durable_inbox_digest: [45; 32],
        })
        .await?;
    repository
        .record_account_delivery_receipt(&AccountDeliveryReceipt {
            schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
            lease: repair_claim.lease.clone(),
            receipt_id: "repair-rejected".into(),
            state: AccountDeliveryReceiptState::Rejected,
            observed_ms: 707,
            account_fact_digest: [0; 32],
            detail: "fixture risk refusal".into(),
        })
        .await?;
    assert!(restarted.project_next_rejected_delivery().await?.is_some());
    assert!(
        !repository
            .load_execution_facts()
            .await?
            .ok_or("rejection UI snapshot")?
            .drift
            .iter()
            .any(|fact| fact.repair_pending)
    );
    assert!(restarted.plan_next(709).await?.is_none()); // no auto-retry of rejected repair
    request.request_id = "00000000-0000-4000-8000-000000000091".into();
    request.expected_revision = Some(1);
    request.relation.lifecycle = CopyLifecyclePolicy::Paused;
    repository.upsert_copy_relation(&request, 710).await?;
    assert!(restarted.plan_next(711).await?.is_none());
    fixture.cleanup().await?;
    Ok(())
}

pub(super) fn advance(envelope: &mut NodeProjectionEnvelope, now_ms: u64, digest: [u8; 32]) {
    envelope.previous_digest = envelope.digest;
    envelope.digest = digest;
    envelope.sequence += 1;
    envelope.snapshot.generated_ms = now_ms;
    envelope.facts.generated_ms = now_ms;
}

pub(super) fn fact(
    envelope: &NodeProjectionEnvelope,
    relation: &CopyRelationConfig,
    role: CopyPlanningFactRole,
    expires_ms: u64,
) -> Result<CopyPlanningFact, Box<dyn std::error::Error>> {
    let binding = &envelope.binding;
    let quote = Asset::new(binding.symbol.quote())?;
    let amount = |value| Amount::new(quote.clone(), Decimal::from(value));
    Ok(CopyPlanningFact {
        role,
        relation_id: relation.relation_id.clone(),
        relation_revision: 1,
        policy_digest: relation.policy_digest(),
        binding: ExecutionFactBinding {
            venue: binding.venue,
            mode: binding.mode,
            trading_account_id: binding.trading_account_id.clone(),
            instance_id: binding.instance_id.clone(),
            config_epoch: binding.config_epoch,
            symbol: binding.symbol.clone(),
        },
        instrument: InstrumentIdentity {
            symbol: binding.symbol.clone(),
            market: MarketKind::LinearPerpetual,
            settlement_asset: Some(quote.clone()),
        },
        private_generation: 1,
        rules_generation: 1,
        observed_ms: envelope.snapshot.generated_ms,
        expires_ms,
        quote_net_exposure: amount(if role == CopyPlanningFactRole::Leader {
            20
        } else {
            0
        }),
        follower_available_margin: (role == CopyPlanningFactRole::Follower).then(|| amount(100)),
        leader_configured_capital: (role == CopyPlanningFactRole::Leader).then(|| amount(100)),
        fact_digest: envelope.digest,
    })
}
