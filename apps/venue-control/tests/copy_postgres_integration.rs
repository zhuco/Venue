use std::{env, process, time::SystemTime};

use rust_decimal::Decimal;
use sqlx::{Executor, PgPool, postgres::PgPoolOptions};
use venue_control::{
    AccountDeliveryRepository, CopyApplyResult, CopyLeaderEnvelope, CopyLeaderIntent,
    CopyLeaderSnapshot, CopyLedgerProjectionInput, CopyObserverScope, CopyPlanningSnapshot,
    CopyReplayDeliveryState, CopyRepository, CopyTestWorker, CopyTestWorkerConfig,
    FrozenCapitalSnapshot, MIGRATION_0001, MIGRATION_0002, MIGRATION_0003, MIGRATION_0004,
    PgControlRepository, ScopedCopyDeliveryReceipt,
};
use venue_control_protocol::{
    AccountDeliveryBinding, AccountDeliveryPayload, GatewayMode, VenueId,
};
use venue_copy::{
    AuthoritativePositionSnapshot, CopyAction, CopyIdentityInput, DeliveryBinding,
    DeliveryReceiptStatus, LedgerAttribution, LedgerEntry, PersistedDeliveryReceipt,
    derive_copy_identities,
};
use venue_domain::domain::{Amount, Asset, InstrumentIdentity, MarketKind, Symbol};

#[tokio::test]
async fn postgres_migration_concurrency_and_crash_windows() -> Result<(), Box<dyn std::error::Error>>
{
    let Some(database_url) = integration_database_url() else {
        println!(
            "SKIP: VENUE_CONTROL_TEST_DATABASE_URL is not set; PostgreSQL integration test was not run"
        );
        return Ok(());
    };
    let fixture = PgFixture::create(&database_url, "atomic").await?;
    fixture.migrate_twice().await?;
    let repository = PgControlRepository::new(fixture.pool.clone());
    let scope = scope("atomic-observer");
    install_account_scope(&fixture.pool).await?;
    let now = 10_000;
    repository
        .store_leader_envelope(&envelope(&scope, 1, now)?, now + 1)
        .await?;
    let worker = make_worker(repository.clone(), scope.clone(), "planner-a")?;
    let (left, right) = tokio::join!(worker.plan_next(now + 2), worker.plan_next(now + 2));
    let planned_count = usize::from(left?.is_some()) + usize::from(right?.is_some());
    assert_eq!(planned_count, 1);
    assert_eq!(
        scalar_i64(&fixture.pool, "SELECT count(*) FROM venue_copy_jobs").await?,
        1
    );
    assert_eq!(
        scalar_i64(&fixture.pool, "SELECT count(*) FROM venue_copy_plans").await?,
        1
    );
    let claims = repository
        .claim_account_deliveries(
            &AccountDeliveryBinding {
                venue: VenueId::Binance,
                mode: GatewayMode::Test,
                trading_account_id: scope.trading_account_id.clone(),
                symbol: "BTC/USDT".parse()?,
                instance_id: "copy-btc".to_owned(),
                config_epoch: 7,
            },
            "account-node-a",
            now + 3,
            now + 100,
            1,
        )
        .await?;
    assert_eq!(claims.len(), 1);
    assert!(matches!(
        claims[0].payload,
        AccountDeliveryPayload::TestCopySemanticJob(_)
    ));

    repository
        .store_leader_envelope(&envelope(&scope, 2, now + 10)?, now + 11)
        .await?;
    sqlx::raw_sql(
            "CREATE FUNCTION venue_test_fail_cursor() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN RAISE EXCEPTION 'simulated planner crash'; END $$; \
             CREATE TRIGGER venue_test_fail_cursor_trigger BEFORE UPDATE \
             ON venue_copy_observer_cursors FOR EACH ROW EXECUTE FUNCTION venue_test_fail_cursor();",
        )
        .execute(&fixture.pool)
        .await?;
    assert!(worker.plan_next(now + 12).await.is_err());
    assert_eq!(
        scalar_i64(&fixture.pool, "SELECT count(*) FROM venue_copy_jobs").await?,
        1
    );
    assert_eq!(
        scalar_i64(&fixture.pool, "SELECT count(*) FROM venue_copy_plans").await?,
        1
    );
    sqlx::raw_sql(
        "DROP TRIGGER venue_test_fail_cursor_trigger ON venue_copy_observer_cursors; \
             DROP FUNCTION venue_test_fail_cursor();",
    )
    .execute(&fixture.pool)
    .await?;
    assert!(worker.plan_next(now + 13).await?.is_some());
    assert_eq!(
        scalar_i64(&fixture.pool, "SELECT count(*) FROM venue_copy_jobs").await?,
        2
    );
    assert_eq!(
        scalar_i64(&fixture.pool, "SELECT count(*) FROM venue_copy_plans").await?,
        2
    );
    let replay = worker.recover(now + 14).await?;
    assert_eq!(replay.observer_cursor, 2);
    assert_eq!(replay.jobs.len(), 2);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn postgres_receipts_unknown_fence_ledger_and_restart_recovery()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url() else {
        println!(
            "SKIP: VENUE_CONTROL_TEST_DATABASE_URL is not set; PostgreSQL receipt/recovery test was not run"
        );
        return Ok(());
    };
    let fixture = PgFixture::create(&database_url, "receipt").await?;
    fixture.migrate_twice().await?;
    let repository = PgControlRepository::new(fixture.pool.clone());
    let scope = scope("receipt-observer");
    install_account_scope(&fixture.pool).await?;
    let worker = make_worker(repository.clone(), scope.clone(), "planner-a")?;
    let now = 20_000;

    let applied = plan_one(&repository, &worker, &scope, 10, now).await?;
    let applied_claim = worker
        .claim_deliveries("account-node", now + 3, 1)
        .await?
        .remove(0);
    sqlx::raw_sql(
            "CREATE FUNCTION venue_test_fail_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN RAISE EXCEPTION 'simulated receipt crash'; END $$; \
             CREATE TRIGGER venue_test_fail_receipt_trigger BEFORE UPDATE \
             ON venue_copy_delivery_outbox FOR EACH ROW EXECUTE FUNCTION venue_test_fail_receipt();",
        )
        .execute(&fixture.pool)
        .await?;
    let applied_receipt = receipt(&applied_claim, DeliveryReceiptStatus::Applied, 1, now + 4);
    assert!(
        worker
            .record_receipt(&ScopedCopyDeliveryReceipt {
                claim: applied_claim.clone(),
                receipt: applied_receipt.clone(),
            })
            .await
            .is_err()
    );
    assert_eq!(
        scalar_i64(
            &fixture.pool,
            "SELECT count(*) FROM venue_copy_delivery_receipts"
        )
        .await?,
        0
    );
    sqlx::raw_sql(
        "DROP TRIGGER venue_test_fail_receipt_trigger ON venue_copy_delivery_outbox; \
             DROP FUNCTION venue_test_fail_receipt();",
    )
    .execute(&fixture.pool)
    .await?;
    assert_eq!(
        worker
            .record_receipt(&ScopedCopyDeliveryReceipt {
                claim: applied_claim.clone(),
                receipt: applied_receipt,
            })
            .await?,
        CopyApplyResult::Stored
    );
    project_applied_ledger(&worker, &applied_claim, &applied, now + 5).await?;

    let unknown = plan_one(&repository, &worker, &scope, 20, now + 20).await?;
    let unknown_claim = worker
        .claim_deliveries("account-node", now + 23, 1)
        .await?
        .remove(0);
    let unknown_receipt = receipt(&unknown_claim, DeliveryReceiptStatus::Unknown, 1, now + 24);
    worker
        .record_receipt(&ScopedCopyDeliveryReceipt {
            claim: unknown_claim.clone(),
            receipt: unknown_receipt,
        })
        .await?;
    assert!(
        worker
            .claim_deliveries("account-node", now + 25, 10)
            .await?
            .is_empty()
    );
    let reconciled = receipt(
        &unknown_claim,
        DeliveryReceiptStatus::Reconciled,
        2,
        now + 26,
    );
    worker
        .record_receipt(&ScopedCopyDeliveryReceipt {
            claim: unknown_claim,
            receipt: reconciled,
        })
        .await?;

    let rejected = plan_one(&repository, &worker, &scope, 30, now + 40).await?;
    let rejected_claim = worker
        .claim_deliveries("account-node", now + 43, 1)
        .await?
        .remove(0);
    worker
        .record_receipt(&ScopedCopyDeliveryReceipt {
            receipt: receipt(
                &rejected_claim,
                DeliveryReceiptStatus::Rejected,
                1,
                now + 44,
            ),
            claim: rejected_claim,
        })
        .await?;

    let restarted = make_worker(repository, scope, "planner-after-restart")?;
    let replay = restarted.recover(now + 45).await?;
    assert_eq!(replay.observer_cursor, 3);
    assert_eq!(replay.jobs.len(), 3);
    assert_eq!(replay.ledger_entries.len(), 1);
    assert!(
        replay
            .jobs
            .iter()
            .all(|job| { matches!(job.delivery_state, CopyReplayDeliveryState::Settled) })
    );
    assert_eq!(replay.jobs[0].receipts.len(), 1);
    assert_eq!(replay.jobs[1].receipts.len(), 2);
    assert_eq!(replay.jobs[2].receipts.len(), 1);
    assert_eq!(replay.jobs[0].job, applied.job);
    assert_eq!(replay.jobs[1].job, unknown.job);
    assert_eq!(replay.jobs[2].job, rejected.job);
    fixture.cleanup().await?;
    Ok(())
}

async fn plan_one(
    repository: &PgControlRepository,
    worker: &CopyTestWorker,
    scope: &CopyObserverScope,
    seed: u8,
    now: u64,
) -> Result<venue_control::PlannedCopyJob, Box<dyn std::error::Error>> {
    repository
        .store_leader_envelope(&envelope(scope, seed, now)?, now + 1)
        .await?;
    worker
        .plan_next(now + 2)
        .await?
        .ok_or_else(|| "expected one planner job".into())
}

async fn project_applied_ledger(
    worker: &CopyTestWorker,
    claim: &venue_control::CopyDeliveryClaim,
    planned: &venue_control::PlannedCopyJob,
    now: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let position = AuthoritativePositionSnapshot {
        binding: planned.job.manifest.binding.clone(),
        generation: 1,
        observed_at_ms: now - 1,
        expires_at_ms: now + 30_000,
        exposure: planned.target.target_exposure.clone(),
        fact_digest: [71; 32],
    };
    let input = CopyLedgerProjectionInput {
        job_id: planned.job.identities.job_id,
        receipt_sequence: 1,
        projection_digest: [72; 32],
        ledger_entry: LedgerEntry {
            sequence: 1,
            generation: position.generation,
            binding: position.binding.clone(),
            attribution: LedgerAttribution::Copy,
            source_id: claim.job.identities.job_id,
            fact_digest: position.fact_digest,
            managed_exposure: position.exposure.clone(),
        },
        position,
        target: planned.target.clone(),
        repair_identities: derive_copy_identities(&identity_input(90))?,
        projected_at_ms: now,
        repair_expires_at_ms: now + 30_000,
    };
    assert_eq!(
        worker.project_ledger(&input).await?,
        CopyApplyResult::Stored
    );
    Ok(())
}

fn receipt(
    claim: &venue_control::CopyDeliveryClaim,
    status: DeliveryReceiptStatus,
    sequence: u64,
    persisted_at_ms: u64,
) -> PersistedDeliveryReceipt {
    PersistedDeliveryReceipt {
        delivery_digest: claim.job.manifest.delivery_digest(),
        binding: claim.job.manifest.binding.clone(),
        plan_digest: claim.job.manifest.plan_digest,
        snapshot_generation: claim.job.manifest.snapshot_generation,
        instrument_generation: claim.job.manifest.instrument_generation,
        receipt_sequence: sequence,
        status,
        persisted_at_ms,
    }
}

fn make_worker(
    repository: PgControlRepository,
    scope: CopyObserverScope,
    worker_id: &str,
) -> Result<CopyTestWorker, venue_control::CopyWorkerError> {
    CopyTestWorker::new(
        repository,
        CopyTestWorkerConfig {
            mode: GatewayMode::Test,
            scope,
            worker_id: worker_id.to_owned(),
            observer_lease_ms: 1_000,
            delivery_claim_ms: 1_000,
        },
    )
}

fn envelope(
    scope: &CopyObserverScope,
    seed: u8,
    now: u64,
) -> Result<CopyLeaderEnvelope, Box<dyn std::error::Error>> {
    let identities = derive_copy_identities(&identity_input(seed))?;
    let binding_ids = derive_copy_identities(&identity_input(seed.saturating_add(1)))?;
    let quote = Asset::new("USDT")?;
    let amount = |value| Amount::new(quote.clone(), Decimal::from(value));
    let planning = CopyPlanningSnapshot {
        capital: FrozenCapitalSnapshot {
            generation: u64::from(seed),
            observed_ms: now,
            expires_ms: now + 40_000,
            leader_strategy_capital: amount(1_000),
            leader_target_exposure: amount(200),
            follower_configured_capital: amount(500),
            follower_allocated_capital: amount(400),
            follower_available_margin: amount(450),
            follower_managed_exposure: amount(0),
            margin_safety_reserve_rate: Decimal::new(1, 1),
        },
        binding: DeliveryBinding {
            leader_id: binding_ids.job_id,
            follower_id: binding_ids.planning_snapshot_id,
            follower_binding_id: binding_ids.child_order_id,
            account_id: scope.trading_account_id.clone(),
            instrument: InstrumentIdentity {
                symbol: "BTC/USDT".parse::<Symbol>()?,
                market: MarketKind::LinearPerpetual,
                settlement_asset: Some(quote),
            },
            policy_id: derive_copy_identities(&identity_input(seed.saturating_add(2)))?.job_id,
        },
        instrument_generation: u64::from(seed) + 100,
        delivery_expires_at_ms: now + 30_000,
    };
    Ok(CopyLeaderEnvelope {
        scope: scope.clone(),
        intent: CopyLeaderIntent {
            intent_id: identities.child_order_id,
            snapshot_id: identities.planning_snapshot_id,
            identity_input: identity_input(seed),
            intent_digest: [seed.saturating_add(20); 32],
            intent_payload: serde_json::json!({"semantic_action": "FOLLOW_TARGET"}),
            observed_at_ms: now,
        },
        snapshot: CopyLeaderSnapshot {
            snapshot_id: identities.planning_snapshot_id,
            generation: u64::from(seed),
            observed_at_ms: now,
            expires_at_ms: now + 40_000,
            snapshot_digest: [seed.saturating_add(30); 32],
            snapshot_payload: serde_json::to_value(planning)?,
        },
        outbox_digest: [seed.saturating_add(40); 32],
    })
}

fn identity_input(seed: u8) -> CopyIdentityInput {
    CopyIdentityInput {
        event_id: [seed; 16],
        source_event_id: [seed.saturating_add(1); 16],
        follower_account_id: [seed.saturating_add(2); 16],
        follower_binding_id: [seed.saturating_add(3); 16],
        leader_order_id: [seed.saturating_add(4); 16],
        revision: 1,
        action: CopyAction::New,
    }
}

fn scope(observer_id: &str) -> CopyObserverScope {
    CopyObserverScope {
        observer_id: observer_id.to_owned(),
        venue: VenueId::Binance,
        trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
    }
}

fn integration_database_url() -> Option<String> {
    env::var("VENUE_CONTROL_TEST_DATABASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

async fn scalar_i64(pool: &PgPool, sql: &str) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(sql).fetch_one(pool).await
}

async fn install_account_scope(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO venue_control_strategy_scopes \
         (instance_id, venue, mode, trading_account_id, symbol, config_epoch, snapshot_generated_ms) \
         VALUES ('copy-btc', 'binance', 'TEST', \
                 '00000000-0000-4000-8000-000000000001', 'BTC/USDT', 7, 1)",
    )
    .execute(pool)
    .await?;
    Ok(())
}

struct PgFixture {
    database_url: String,
    schema: String,
    pool: PgPool,
}

impl PgFixture {
    async fn create(database_url: &str, label: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let nonce = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_nanos();
        let schema = format!("venue_copy_test_{}_{}_{}", label, process::id(), nonce);
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(database_url)
            .await?;
        admin
            .execute(format!("CREATE SCHEMA {schema}").as_str())
            .await?;
        admin.close().await;
        let search_path = schema.clone();
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .after_connect(move |connection, _| {
                let statement = format!("SET search_path TO {search_path}");
                Box::pin(async move {
                    connection.execute(statement.as_str()).await?;
                    Ok(())
                })
            })
            .connect(database_url)
            .await?;
        Ok(Self {
            database_url: database_url.to_owned(),
            schema,
            pool,
        })
    }

    async fn migrate_twice(&self) -> Result<(), sqlx::Error> {
        for _ in 0..2 {
            sqlx::raw_sql(MIGRATION_0001).execute(&self.pool).await?;
            sqlx::raw_sql(MIGRATION_0002).execute(&self.pool).await?;
            sqlx::raw_sql(MIGRATION_0003).execute(&self.pool).await?;
            sqlx::raw_sql(MIGRATION_0004).execute(&self.pool).await?;
        }
        Ok(())
    }

    async fn cleanup(self) -> Result<(), Box<dyn std::error::Error>> {
        self.pool.close().await;
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.database_url)
            .await?;
        admin
            .execute(format!("DROP SCHEMA {} CASCADE", self.schema).as_str())
            .await?;
        admin.close().await;
        Ok(())
    }
}
