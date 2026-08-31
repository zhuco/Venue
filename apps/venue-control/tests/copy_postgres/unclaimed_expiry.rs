use super::node_copy_source::{advance, fact};
use super::*;
use rust_decimal::Decimal;
use sqlx::{PgPool, Row};
use std::time::Duration;
use venue_control::{
    AccountDeliveryRepository, ControlRepository, CopyApplyResult, CopyObserverScope,
    CopyRelationRepository, CopyReplayDeliveryState, CopyRepository, CopyRepositoryError,
    CopyWorker, CopyWorkerConfig, PgControlRepository, PlannedCopyJob,
};
use venue_control_protocol::{
    AccountDeliveryBinding, CopyLifecyclePolicy, CopyPlanningFactRole, CopyRelationBinding,
    CopyRelationConfig, CopyRelationUpsertRequest, CopyRiskPolicy, GatewayMode,
    NodeProjectionEnvelope,
};

#[tokio::test]
async fn postgres_unclaimed_expiry_retires_immutable_job_and_plans_fresh_successor()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = PgFixture::create(&url).await?;
    fixture.migrate_twice().await?;
    let mut scenario = Scenario::create(&fixture.pool).await?;
    let old_id = scenario.old.job.identities.job_id.to_string();
    let old_expiry = scenario.old.job.manifest.expires_at_ms;
    let old_immutable: (String, Vec<u8>) =
        sqlx::query_as("SELECT job_json::text, job_digest FROM venue_copy_jobs WHERE job_id=$1")
            .bind(&old_id)
            .fetch_one(&fixture.pool)
            .await?;

    scenario.publish_fresh_facts(2).await?;
    let successor = scenario
        .worker
        .plan_next(702)
        .await?
        .ok_or("fresh facts did not replace unclaimed expired job")?;
    let successor_id = successor.job.identities.job_id.to_string();
    assert_ne!(successor_id, old_id);
    assert_eq!(successor.job.manifest.expires_at_ms, 900);
    assert_eq!(
        successor.target.target_exposure,
        scenario.old.target.target_exposure
    );
    assert_eq!(
        sqlx::query_as::<_, (String, Vec<u8>)>(
            "SELECT job_json::text, job_digest FROM venue_copy_jobs WHERE job_id=$1",
        )
        .bind(&old_id)
        .fetch_one(&fixture.pool)
        .await?,
        old_immutable
    );
    assert_expired_without_evidence(&fixture.pool, &old_id).await?;
    assert_pending_without_lease(&fixture.pool, &successor_id).await?;

    let late_execution = historical_execution(&scenario.old)?;
    assert_eq!(
        scenario.worker.record_execution(&late_execution).await,
        Err(venue_control::CopyWorkerError::Repository(
            CopyRepositoryError::ProjectionConflict
        ))
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM venue_copy_execution_results WHERE job_id=$1",
        )
        .bind(&old_id)
        .fetch_one(&fixture.pool)
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM venue_copy_ledger WHERE job_id=$1")
            .bind(&old_id)
            .fetch_one(&fixture.pool)
            .await?,
        0
    );

    let semantic: venue_control::CopySemanticJob =
        serde_json::from_value(successor.job.semantic_job.clone())?;
    let provenance = semantic.leader_intent["supersedes_unclaimed_expired_jobs"]
        .as_array()
        .ok_or("successor omitted expired-job provenance")?;
    assert_eq!(provenance.len(), 1);
    assert_eq!(provenance[0]["job_id"], serde_json::json!(old_id));
    assert_eq!(
        provenance[0]["expires_at_ms"],
        serde_json::json!(old_expiry)
    );
    assert_eq!(
        provenance[0]["job_digest"],
        serde_json::to_value(scenario.old.job.job_digest)?
    );

    // 0016 must remain re-runnable after the retirement rows exist.
    fixture.migrate_twice().await?;
    let restarted = CopyWorker::new(
        PgControlRepository::new(fixture.pool.clone()),
        scenario.worker_config.clone(),
    )?;
    let replay = restarted.recover(703).await?;
    assert_eq!(replay.jobs.len(), 2);
    assert_eq!(
        replay
            .jobs
            .iter()
            .find(|replayed| replayed.job.identities.job_id.to_string() == old_id)
            .ok_or("expired job missing from recovery")?
            .delivery_state,
        CopyReplayDeliveryState::Expired
    );
    assert_eq!(
        replay
            .jobs
            .iter()
            .find(|replayed| replayed.job.identities.job_id.to_string() == successor_id)
            .ok_or("successor missing from recovery")?
            .delivery_state,
        CopyReplayDeliveryState::Redeliverable
    );
    assert!(restarted.plan_next(704).await?.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM venue_copy_jobs")
            .fetch_one(&fixture.pool)
            .await?,
        2
    );
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn postgres_unclaimed_expiry_requires_newer_follower_generation()
-> Result<(), Box<dyn std::error::Error>> {
    assert_expiry_remains_fenced(ExpiryBlocker::FollowerGeneration).await
}

#[tokio::test]
async fn postgres_unclaimed_expiry_never_retires_an_account_claimed_job()
-> Result<(), Box<dyn std::error::Error>> {
    assert_expiry_remains_fenced(ExpiryBlocker::AccountClaim).await
}

#[tokio::test]
async fn postgres_unclaimed_expiry_never_retires_a_legacy_claimed_job()
-> Result<(), Box<dyn std::error::Error>> {
    assert_expiry_remains_fenced(ExpiryBlocker::LegacyClaim).await
}

#[tokio::test]
async fn postgres_unclaimed_expiry_never_retires_a_job_with_execution_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    assert_expiry_remains_fenced(ExpiryBlocker::ExecutionEvidence).await
}

#[tokio::test]
async fn postgres_unclaimed_expiry_requires_both_observations_after_the_old_expiry()
-> Result<(), Box<dyn std::error::Error>> {
    assert_expiry_remains_fenced(ExpiryBlocker::ObservationNotAfterExpiry).await
}

#[tokio::test]
async fn postgres_unclaimed_expiry_legacy_claim_race_never_retires_after_claim()
-> Result<(), Box<dyn std::error::Error>> {
    for round in 0..4 {
        let Some(url) = integration_database_url()? else {
            return Ok(());
        };
        let fixture = PgFixture::create(&url).await?;
        fixture.migrate_twice().await?;
        let mut scenario = Scenario::create(&fixture.pool).await?;
        let old_id = scenario.old.job.identities.job_id.to_string();
        scenario.publish_fresh_facts(2).await?;

        // This matches the legacy consumer's outbox lock. The planner already holds the account
        // delivery and job locks before it waits here, so a conflicting claim must win cleanly.
        let mut claim_transaction = fixture.pool.begin().await?;
        let claim_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *claim_transaction)
            .await?;
        let locked_state: String = sqlx::query_scalar(
            "SELECT delivery_state FROM venue_copy_delivery_outbox WHERE job_id=$1 FOR UPDATE",
        )
        .bind(&old_id)
        .fetch_one(&mut *claim_transaction)
        .await?;
        assert_eq!(locked_state, "pending", "race round {round}");
        let old_digest = scenario.old.job.job_digest.to_vec();
        let planner = scenario.worker.plan_next(702);
        let claim = async {
            // Wait for the real lock conflict, not a scheduler yield that may run before the
            // planner has reached its job lock. This catches the FK KEY SHARE deadlock boundary.
            loop {
                let waiting: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity \
                     WHERE $1 = ANY(pg_blocking_pids(pid)))",
                )
                .bind(claim_pid)
                .fetch_one(&fixture.pool)
                .await?;
                if waiting {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            sqlx::query(
                "UPDATE venue_copy_delivery_outbox SET delivery_state='claimed', claimed_by=$2, \
                 claim_epoch=1, claimed_at_ms=300, claim_expires_at_ms=500, updated_at_ms=300 \
                 WHERE job_id=$1 AND delivery_state='pending' AND claim_epoch=0",
            )
            .bind(&old_id)
            .bind(format!("legacy-race-{round}"))
            .execute(&mut *claim_transaction)
            .await?;
            sqlx::query(
                "INSERT INTO venue_copy_delivery_inbox \
                 (job_id, consumer_id, claim_epoch, job_digest, inbox_state, claimed_at_ms, updated_at_ms) \
                 VALUES ($1, $2, 1, $3, 'claimed', 300, 300)",
            )
            .bind(&old_id)
            .bind(format!("legacy-race-{round}"))
            .bind(old_digest)
            .execute(&mut *claim_transaction)
            .await?;
            claim_transaction.commit().await
        };
        let (planned, claim_result) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(planner, claim)
        })
        .await
        .map_err(|_| "planner and legacy claim race exceeded five seconds")?;
        claim_result?;
        assert!(
            planned?.is_none(),
            "race round {round} created a successor after old claim"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT delivery_state FROM venue_copy_delivery_outbox WHERE job_id=$1",
            )
            .bind(&old_id)
            .fetch_one(&fixture.pool)
            .await?,
            "claimed",
            "race round {round} retired a legacy-claimed job"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM venue_copy_jobs")
                .fetch_one(&fixture.pool)
                .await?,
            1,
            "race round {round} produced a second job"
        );
        fixture.cleanup().await?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum ExpiryBlocker {
    FollowerGeneration,
    AccountClaim,
    LegacyClaim,
    ExecutionEvidence,
    ObservationNotAfterExpiry,
}

async fn assert_expiry_remains_fenced(
    blocker: ExpiryBlocker,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = PgFixture::create(&url).await?;
    fixture.migrate_twice().await?;
    let mut scenario = Scenario::create(&fixture.pool).await?;
    let old_id = scenario.old.job.identities.job_id.to_string();
    match blocker {
        ExpiryBlocker::FollowerGeneration => scenario.publish_fresh_facts(1).await?,
        ExpiryBlocker::AccountClaim => {
            let claims = scenario
                .repository
                .claim_account_deliveries(&scenario.follower.binding, "account-claim", 300, 500, 1)
                .await?;
            assert!(
                claims
                    .iter()
                    .any(|claim| claim.lease.delivery_id == format!("copy:{old_id}"))
            );
            scenario.publish_fresh_facts(2).await?;
        }
        ExpiryBlocker::LegacyClaim => {
            let claims = scenario
                .repository
                .claim_copy_jobs(&scenario.worker_config.scope, "legacy-claim", 300, 500, 1)
                .await?;
            assert!(
                claims
                    .iter()
                    .any(|claim| claim.job.identities.job_id.to_string() == old_id)
            );
            scenario.publish_fresh_facts(2).await?;
        }
        ExpiryBlocker::ExecutionEvidence => {
            assert_eq!(
                scenario
                    .worker
                    .record_execution(&historical_execution(&scenario.old)?)
                    .await?,
                CopyApplyResult::Stored
            );
            scenario.publish_fresh_facts(2).await?;
        }
        ExpiryBlocker::ObservationNotAfterExpiry => {
            scenario.publish_facts(600, 600, 2).await?;
        }
    }
    assert!(scenario.worker.plan_next(702).await?.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM venue_copy_jobs")
            .fetch_one(&fixture.pool)
            .await?,
        1
    );
    let account_state: String = sqlx::query_scalar(
        "SELECT delivery_state FROM venue_account_deliveries WHERE delivery_id=$1",
    )
    .bind(format!("copy:{old_id}"))
    .fetch_one(&fixture.pool)
    .await?;
    let outbox_state: String =
        sqlx::query_scalar("SELECT delivery_state FROM venue_copy_delivery_outbox WHERE job_id=$1")
            .bind(&old_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_ne!(account_state, "expired_unclaimed");
    assert_ne!(outbox_state, "expired_unclaimed");
    fixture.cleanup().await?;
    Ok(())
}

struct Scenario {
    repository: PgControlRepository,
    worker: CopyWorker,
    worker_config: CopyWorkerConfig,
    relation: CopyRelationConfig,
    leader: NodeProjectionEnvelope,
    follower: NodeProjectionEnvelope,
    old: PlannedCopyJob,
}

impl Scenario {
    async fn create(pool: &PgPool) -> Result<Self, Box<dyn std::error::Error>> {
        let repository = PgControlRepository::new(pool.clone());
        let mut follower = projection(1, 1, [11; 32], [0; 32], 100)?;
        follower.snapshot.strategies[0].kind = StrategyKind::Copy;
        let mut leader = projection(1, 1, [21; 32], [0; 32], 100)?;
        leader.node_id = "expiry-leader-node".into();
        leader.binding.instance_id = "expiry-leader-grid".into();
        leader.binding.trading_account_id = "00000000-0000-4000-8000-000000000002".into();
        leader.snapshot.accounts[0].trading_account_id = leader.binding.trading_account_id.clone();
        leader.snapshot.strategies[0].trading_account_id =
            leader.binding.trading_account_id.clone();
        leader.snapshot.strategies[0].instance_id = leader.binding.instance_id.clone();
        repository.merge_node_projection(&follower).await?;
        repository.merge_node_projection(&leader).await?;

        let relation_endpoint = |binding: &AccountDeliveryBinding| CopyRelationBinding {
            venue: binding.venue,
            mode: binding.mode,
            trading_account_id: binding.trading_account_id.clone(),
            symbol: binding.symbol.clone(),
            instance_id: binding.instance_id.clone(),
        };
        let relation = CopyRelationConfig {
            relation_id: "00000000-0000-4000-8000-000000000199".into(),
            leader: relation_endpoint(&leader.binding),
            follower: relation_endpoint(&follower.binding),
            allocated_capital: Decimal::TEN,
            multiplier: Decimal::from(2),
            safety_reserve_rate: Decimal::ZERO,
            risk: CopyRiskPolicy {
                max_total_notional: Decimal::TEN,
                max_order_notional: Decimal::TEN,
                max_leverage: Decimal::from(2),
            },
            lifecycle: CopyLifecyclePolicy::Active,
        };
        repository
            .upsert_copy_relation(
                &CopyRelationUpsertRequest {
                    schema_version: CONTROL_SCHEMA_VERSION,
                    request_id: "00000000-0000-4000-8000-000000000190".into(),
                    expected_revision: Some(0),
                    relation: relation.clone(),
                },
                101,
            )
            .await?;
        let worker_config = CopyWorkerConfig {
            mode: GatewayMode::Live,
            scope: CopyObserverScope {
                observer_id: "unclaimed-expiry-worker".into(),
                venue: follower.binding.venue,
                mode: GatewayMode::Live,
                trading_account_id: follower.binding.trading_account_id.clone(),
            },
            worker_id: "unclaimed-expiry-planner".into(),
            observer_lease_ms: 1_000,
            delivery_claim_ms: 1_000,
        };
        let worker = CopyWorker::new(repository.clone(), worker_config.clone())?;
        advance(&mut leader, 110, [22; 32]);
        leader.copy_planning_facts =
            vec![fact(&leader, &relation, CopyPlanningFactRole::Leader, 600)?];
        repository.merge_node_projection(&leader).await?;
        advance(&mut follower, 120, [12; 32]);
        follower.copy_planning_facts = vec![fact(
            &follower,
            &relation,
            CopyPlanningFactRole::Follower,
            600,
        )?];
        repository.merge_node_projection(&follower).await?;
        let old = worker
            .plan_next(121)
            .await?
            .ok_or("initial paired facts did not produce a job")?;
        Ok(Self {
            repository,
            worker,
            worker_config,
            relation,
            leader,
            follower,
            old,
        })
    }

    async fn publish_fresh_facts(
        &mut self,
        follower_private_generation: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.publish_facts(700, 701, follower_private_generation)
            .await
    }

    async fn publish_facts(
        &mut self,
        leader_observed_ms: u64,
        follower_observed_ms: u64,
        follower_private_generation: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        advance(&mut self.leader, leader_observed_ms, [23; 32]);
        self.leader.copy_planning_facts = vec![fact(
            &self.leader,
            &self.relation,
            CopyPlanningFactRole::Leader,
            900,
        )?];
        self.repository.merge_node_projection(&self.leader).await?;

        advance(&mut self.follower, follower_observed_ms, [13; 32]);
        self.follower.snapshot.accounts[0].private_generation = follower_private_generation;
        let mut follower = fact(
            &self.follower,
            &self.relation,
            CopyPlanningFactRole::Follower,
            900,
        )?;
        follower.private_generation = follower_private_generation;
        self.follower.copy_planning_facts = vec![follower];
        self.repository
            .merge_node_projection(&self.follower)
            .await?;
        Ok(())
    }
}

async fn assert_expired_without_evidence(pool: &PgPool, job_id: &str) -> Result<(), sqlx::Error> {
    let delivery = sqlx::query(
        "SELECT delivery_state, lease_epoch, leased_by, lease_purpose, leased_at_ms, lease_expires_at_ms \
         FROM venue_account_deliveries WHERE delivery_id=$1",
    )
    .bind(format!("copy:{job_id}"))
    .fetch_one(pool)
    .await?;
    assert_eq!(
        delivery.try_get::<String, _>("delivery_state")?,
        "expired_unclaimed"
    );
    assert_eq!(delivery.try_get::<i64, _>("lease_epoch")?, 0);
    assert!(
        delivery
            .try_get::<Option<String>, _>("leased_by")?
            .is_none()
    );
    assert!(
        delivery
            .try_get::<Option<String>, _>("lease_purpose")?
            .is_none()
    );
    assert!(
        delivery
            .try_get::<Option<i64>, _>("leased_at_ms")?
            .is_none()
    );
    assert!(
        delivery
            .try_get::<Option<i64>, _>("lease_expires_at_ms")?
            .is_none()
    );
    let outbox = sqlx::query(
        "SELECT delivery_state, claim_epoch, claimed_by, claimed_at_ms, claim_expires_at_ms \
         FROM venue_copy_delivery_outbox WHERE job_id=$1",
    )
    .bind(job_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        outbox.try_get::<String, _>("delivery_state")?,
        "expired_unclaimed"
    );
    assert_eq!(outbox.try_get::<i64, _>("claim_epoch")?, 0);
    assert!(outbox.try_get::<Option<String>, _>("claimed_by")?.is_none());
    assert!(outbox.try_get::<Option<i64>, _>("claimed_at_ms")?.is_none());
    assert!(
        outbox
            .try_get::<Option<i64>, _>("claim_expires_at_ms")?
            .is_none()
    );
    for sql in [
        "SELECT count(*) FROM venue_account_delivery_claims WHERE delivery_id=$1",
        "SELECT count(*) FROM venue_account_delivery_acks WHERE delivery_id=$1",
        "SELECT count(*) FROM venue_account_delivery_receipts WHERE delivery_id=$1",
        "SELECT count(*) FROM venue_copy_delivery_inbox WHERE job_id=$1",
        "SELECT count(*) FROM venue_copy_delivery_receipts WHERE job_id=$1",
        "SELECT count(*) FROM venue_copy_receipt_outbox WHERE job_id=$1",
        "SELECT count(*) FROM venue_copy_projection_inbox WHERE job_id=$1",
        "SELECT count(*) FROM venue_copy_execution_results WHERE job_id=$1",
        "SELECT count(*) FROM venue_copy_ledger WHERE job_id=$1",
        "SELECT count(*) FROM venue_copy_drift_projections WHERE source_job_id=$1",
    ] {
        let key = if sql.contains("delivery_id") {
            format!("copy:{job_id}")
        } else {
            job_id.into()
        };
        assert_eq!(
            sqlx::query_scalar::<_, i64>(sql)
                .bind(key)
                .fetch_one(pool)
                .await?,
            0
        );
    }
    Ok(())
}

async fn assert_pending_without_lease(pool: &PgPool, job_id: &str) -> Result<(), sqlx::Error> {
    let delivery: (String, i64) = sqlx::query_as(
        "SELECT delivery_state, lease_epoch FROM venue_account_deliveries WHERE delivery_id=$1",
    )
    .bind(format!("copy:{job_id}"))
    .fetch_one(pool)
    .await?;
    assert_eq!(delivery, ("pending".into(), 0));
    let outbox: (String, i64) = sqlx::query_as(
        "SELECT delivery_state, claim_epoch FROM venue_copy_delivery_outbox WHERE job_id=$1",
    )
    .bind(job_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(outbox, ("pending".into(), 0));
    Ok(())
}

fn historical_execution(
    planned: &PlannedCopyJob,
) -> Result<venue_control::CopyExecutionProjectionInput, Box<dyn std::error::Error>> {
    let position = venue_copy::AuthoritativePositionSnapshot {
        binding: planned.job.manifest.binding.clone(),
        generation: planned.job.manifest.snapshot_generation,
        observed_at_ms: planned.job.created_at_ms,
        expires_at_ms: planned.job.manifest.expires_at_ms,
        exposure: planned.frozen_capital.follower_managed_exposure.clone(),
        fact_digest: [91; 32],
    };
    Ok(venue_control::CopyExecutionProjectionInput {
        job_id: planned.job.identities.job_id,
        execution: venue_copy::CopyExecutionResult {
            request: venue_copy::plan_copy_execution(
                &planned.job.manifest,
                &planned.target,
                &position,
                planned.job.created_at_ms,
            )?,
            state: venue_copy::CopyExecutionState::Prepared,
            command_id: Some("late-retired-job-evidence".into()),
            fact_digest: [0; 32],
            reconciled_position: None,
            observed_at_ms: planned.job.created_at_ms,
        },
    })
}
