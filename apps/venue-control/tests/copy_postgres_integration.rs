use std::{env, process, time::SystemTime};

use rust_decimal::Decimal;
use sqlx::{Executor, PgPool, postgres::PgPoolOptions};
use venue_control::{
    AccountDeliveryRepository, CopyApplyResult, CopyLeaderEnvelope, CopyLeaderIntent,
    CopyLeaderSnapshot, CopyLedgerProjectionInput, CopyObserverScope, CopyPlanningSnapshot,
    CopyRelationRepository, CopyRelationRepositoryError, CopyReplayDeliveryState, CopyRepository,
    CopyWorker, CopyWorkerConfig, FrozenCapitalSnapshot, MIGRATION_0001, MIGRATION_0002,
    MIGRATION_0003, MIGRATION_0004, MIGRATION_0005, MIGRATION_0006, PgControlRepository,
    ScopedCopyDeliveryReceipt,
};
use venue_control_protocol::{
    ACCOUNT_DELIVERY_SCHEMA_VERSION, AccountDeliveryAck, AccountDeliveryBinding,
    AccountDeliveryPayload, AccountDeliveryReceipt, AccountDeliveryReceiptState,
    CONTROL_SCHEMA_VERSION, CopyLifecyclePolicy, CopyRelationBinding, CopyRelationConfig,
    CopyRelationReceiptState, CopyRelationUpsertRequest, CopyRiskPolicy, GatewayMode, VenueId,
};
use venue_copy::{
    AuthoritativePositionSnapshot, CopyAction, CopyIdentityInput, DeliveryBinding,
    DeliveryReceiptStatus, LedgerAttribution, LedgerEntry, PersistedDeliveryReceipt,
    derive_copy_identities,
};
use venue_domain::domain::{Amount, Asset, InstrumentIdentity, MarketKind, Symbol};

#[tokio::test]
async fn live_only_migration_rejects_legacy_test_rows_without_rewriting_them()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url() else {
        println!(
            "SKIP: VENUE_CONTROL_TEST_DATABASE_URL is not set; LIVE-only migration test was not run"
        );
        return Ok(());
    };
    let fixture = PgFixture::create(&database_url, "legacy_mode").await?;
    sqlx::raw_sql(MIGRATION_0001).execute(&fixture.pool).await?;
    sqlx::raw_sql(MIGRATION_0002).execute(&fixture.pool).await?;
    sqlx::raw_sql(MIGRATION_0003).execute(&fixture.pool).await?;
    sqlx::raw_sql(MIGRATION_0004).execute(&fixture.pool).await?;
    sqlx::raw_sql(
        "ALTER TABLE venue_copy_observer_scopes \
         DROP CONSTRAINT venue_copy_observer_scopes_mode_check; \
         INSERT INTO venue_copy_observer_scopes \
         (observer_id, venue, mode, trading_account_id) VALUES \
         ('legacy-observer', 'binance', 'TEST', \
          '00000000-0000-4000-8000-000000000001');",
    )
    .execute(&fixture.pool)
    .await?;

    assert!(
        sqlx::raw_sql(MIGRATION_0005)
            .execute(&fixture.pool)
            .await
            .is_err()
    );
    assert_eq!(
        scalar_i64(
            &fixture.pool,
            "SELECT count(*) FROM venue_copy_observer_scopes WHERE mode = 'TEST'"
        )
        .await?,
        1
    );
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn live_only_migration_rejects_legacy_delivery_schema_without_rewriting_it()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url() else {
        println!(
            "SKIP: VENUE_CONTROL_TEST_DATABASE_URL is not set; delivery schema migration test was not run"
        );
        return Ok(());
    };
    let fixture = PgFixture::create(&database_url, "legacy_delivery_schema").await?;
    sqlx::raw_sql(MIGRATION_0001).execute(&fixture.pool).await?;
    sqlx::raw_sql(MIGRATION_0002).execute(&fixture.pool).await?;
    sqlx::raw_sql(MIGRATION_0003).execute(&fixture.pool).await?;
    sqlx::raw_sql(MIGRATION_0004).execute(&fixture.pool).await?;
    sqlx::raw_sql(
        r#"INSERT INTO venue_account_deliveries
           (delivery_id, source_kind, source_id, venue, mode, trading_account_id, symbol,
            instance_id, config_epoch, payload_json, created_at_ms, updated_at_ms)
           VALUES
           ('legacy-schema', 'control_command', 'legacy-command', 'binance', 'LIVE',
            '00000000-0000-4000-8000-000000000001', 'BTC/USDT', 'grid-btc', 1,
            '{}'::jsonb, 1, 1);
           INSERT INTO venue_account_delivery_claims
           (delivery_id, lease_epoch, node_id, purpose, leased_at_ms, expires_at_ms, claim_json)
           VALUES
           ('legacy-schema', 1, 'legacy-node', 'install', 1, 2,
            '{"schema_version":1}'::jsonb);"#,
    )
    .execute(&fixture.pool)
    .await?;

    assert!(
        sqlx::raw_sql(MIGRATION_0005)
            .execute(&fixture.pool)
            .await
            .is_err()
    );
    assert_eq!(
        scalar_i64(
            &fixture.pool,
            "SELECT count(*) FROM venue_account_delivery_claims \
             WHERE claim_json ->> 'schema_version' = '1'"
        )
        .await?,
        1
    );
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn live_only_migration_rejects_any_non_live_json_mode_without_rewriting_it()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url() else {
        println!(
            "SKIP: VENUE_CONTROL_TEST_DATABASE_URL is not set; JSON mode migration test was not run"
        );
        return Ok(());
    };
    let fixture = PgFixture::create(&database_url, "legacy_json_mode").await?;
    sqlx::raw_sql(MIGRATION_0001).execute(&fixture.pool).await?;
    sqlx::raw_sql(MIGRATION_0002).execute(&fixture.pool).await?;
    sqlx::raw_sql(MIGRATION_0003).execute(&fixture.pool).await?;
    sqlx::raw_sql(MIGRATION_0004).execute(&fixture.pool).await?;
    sqlx::query(
        "INSERT INTO venue_control_snapshots (generated_ms, snapshot_json) \
         VALUES (1, $1::jsonb)",
    )
    .bind(r#"{"accounts":[{"mode":"SHADOW"}]}"#)
    .execute(&fixture.pool)
    .await?;

    assert!(
        sqlx::raw_sql(MIGRATION_0005)
            .execute(&fixture.pool)
            .await
            .is_err()
    );
    assert_eq!(
        scalar_i64(
            &fixture.pool,
            "SELECT count(*) FROM venue_control_snapshots \
             WHERE snapshot_json #>> '{accounts,0,mode}' = 'SHADOW'"
        )
        .await?,
        1
    );
    fixture.cleanup().await?;
    Ok(())
}

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
                mode: GatewayMode::Live,
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
        AccountDeliveryPayload::CopySemanticJob(_)
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
    persist_node_receipt(&repository, &scope, DeliveryReceiptStatus::Applied, now + 4).await?;
    sqlx::raw_sql(
            "CREATE FUNCTION venue_test_fail_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN RAISE EXCEPTION 'simulated receipt crash'; END $$; \
             CREATE TRIGGER venue_test_fail_receipt_trigger BEFORE UPDATE \
             ON venue_copy_delivery_outbox FOR EACH ROW EXECUTE FUNCTION venue_test_fail_receipt();",
        )
        .execute(&fixture.pool)
        .await?;
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
    persist_node_receipt(
        &repository,
        &scope,
        DeliveryReceiptStatus::Unknown,
        now + 24,
    )
    .await?;
    worker
        .record_receipt(&ScopedCopyDeliveryReceipt {
            claim: unknown_claim.clone(),
            receipt: unknown_receipt.clone(),
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
    persist_node_receipt(
        &repository,
        &scope,
        DeliveryReceiptStatus::Reconciled,
        now + 26,
    )
    .await?;
    worker
        .record_receipt(&ScopedCopyDeliveryReceipt {
            claim: unknown_claim.clone(),
            receipt: reconciled,
        })
        .await?;
    assert_eq!(
        worker
            .record_receipt(&ScopedCopyDeliveryReceipt {
                claim: unknown_claim,
                receipt: unknown_receipt,
            })
            .await?,
        CopyApplyResult::Existing
    );

    let rejected = plan_one(&repository, &worker, &scope, 30, now + 40).await?;
    let rejected_claim = worker
        .claim_deliveries("account-node", now + 43, 1)
        .await?
        .remove(0);
    persist_node_receipt(
        &repository,
        &scope,
        DeliveryReceiptStatus::Rejected,
        now + 44,
    )
    .await?;
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
    assert_eq!(replay.drift_projections.len(), 1);
    let repair = replay.drift_projections[0]
        .repair
        .as_ref()
        .ok_or("expected a drift repair semantic request")?;
    assert_eq!(repair.supersedes_job_id, applied.job.identities.job_id);
    assert_eq!(repair.delta_exposure.value, Decimal::ONE);
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

#[tokio::test]
async fn postgres_copy_relation_config_is_live_bound_idempotent_and_revision_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url() else {
        println!(
            "SKIP: VENUE_CONTROL_TEST_DATABASE_URL is not set; relation config test was not run"
        );
        return Ok(());
    };
    let fixture = PgFixture::create(&database_url, "relation_config").await?;
    fixture.migrate_twice().await?;
    install_account_scope(&fixture.pool).await?;
    let repository = PgControlRepository::new(fixture.pool.clone());
    let created = repository
        .upsert_copy_relation(&relation_request(None, Decimal::ONE)?, 100)
        .await?;
    assert_eq!(created.state, CopyRelationReceiptState::Created);
    assert_eq!(created.revision, 1);
    let replay = repository
        .upsert_copy_relation(&relation_request(None, Decimal::ONE)?, 101)
        .await?;
    assert_eq!(replay.state, CopyRelationReceiptState::Existing);
    assert_eq!(replay.revision, 1);
    assert_eq!(
        repository
            .upsert_copy_relation(&relation_request(Some(7), Decimal::new(2, 0))?, 102)
            .await,
        Err(CopyRelationRepositoryError::Conflict)
    );
    let updated = repository
        .upsert_copy_relation(&relation_request(Some(1), Decimal::new(2, 0))?, 103)
        .await?;
    assert_eq!(updated.state, CopyRelationReceiptState::Updated);
    assert_eq!(updated.revision, 2);
    let configs = repository.list_copy_relations().await?;
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].revision, 2);
    assert_eq!(configs[0].relation.multiplier, Decimal::new(2, 0));
    sqlx::query(
        "UPDATE venue_copy_relation_configs SET follower_symbol = 'ETH/USDT' \
         WHERE relation_id = '00000000-0000-4000-8000-000000000010'",
    )
    .execute(&fixture.pool)
    .await?;
    assert_eq!(
        repository.list_copy_relations().await,
        Err(CopyRelationRepositoryError::CorruptData)
    );
    fixture.cleanup().await?;
    Ok(())
}

fn relation_request(
    expected_revision: Option<u64>,
    multiplier: Decimal,
) -> Result<CopyRelationUpsertRequest, Box<dyn std::error::Error>> {
    let binding = |instance_id: &str| -> Result<CopyRelationBinding, Box<dyn std::error::Error>> {
        Ok(CopyRelationBinding {
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            instance_id: instance_id.to_owned(),
            symbol: "BTC/USDT".parse()?,
        })
    };
    Ok(CopyRelationUpsertRequest {
        schema_version: CONTROL_SCHEMA_VERSION,
        relation: CopyRelationConfig {
            relation_id: "00000000-0000-4000-8000-000000000010".to_owned(),
            leader: binding("leader-btc")?,
            follower: binding("copy-btc")?,
            allocated_capital: Decimal::new(500, 0),
            multiplier,
            safety_reserve_rate: Decimal::new(1, 1),
            risk: CopyRiskPolicy {
                max_total_notional: Decimal::new(1_000, 0),
                max_order_notional: Decimal::new(100, 0),
                max_leverage: Decimal::new(3, 0),
            },
            lifecycle: CopyLifecyclePolicy::Paused,
        },
        expected_revision,
    })
}

async fn plan_one(
    repository: &PgControlRepository,
    worker: &CopyWorker,
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
    worker: &CopyWorker,
    claim: &venue_control::CopyDeliveryClaim,
    planned: &venue_control::PlannedCopyJob,
    now: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let position_exposure = Amount::new(
        planned.target.target_exposure.asset.clone(),
        planned.target.target_exposure.value - Decimal::ONE,
    );
    let position = AuthoritativePositionSnapshot {
        binding: planned.job.manifest.binding.clone(),
        generation: 1,
        observed_at_ms: now - 1,
        expires_at_ms: now + 30_000,
        exposure: position_exposure,
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

async fn persist_node_receipt(
    repository: &PgControlRepository,
    scope: &CopyObserverScope,
    status: DeliveryReceiptStatus,
    observed_ms: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let binding = AccountDeliveryBinding {
        venue: scope.venue,
        mode: GatewayMode::Live,
        trading_account_id: scope.trading_account_id.clone(),
        symbol: "BTC/USDT".parse()?,
        instance_id: "copy-btc".to_owned(),
        config_epoch: 7,
    };
    let claims = repository
        .claim_account_deliveries(
            &binding,
            "account-node",
            observed_ms - 2,
            observed_ms + 100,
            1,
        )
        .await?;
    let claim = claims
        .into_iter()
        .next()
        .ok_or_else(|| "expected account-node delivery claim".to_owned())?;
    if matches!(
        status,
        DeliveryReceiptStatus::Applied | DeliveryReceiptStatus::Rejected
    ) {
        repository
            .acknowledge_account_delivery(&AccountDeliveryAck {
                schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
                lease: claim.lease.clone(),
                acknowledged_ms: observed_ms - 1,
                durable_inbox_digest: [61; 32],
            })
            .await?;
    }
    let state = match status {
        DeliveryReceiptStatus::Applied => AccountDeliveryReceiptState::Applied,
        DeliveryReceiptStatus::Unknown => AccountDeliveryReceiptState::Unknown,
        DeliveryReceiptStatus::Reconciled => AccountDeliveryReceiptState::Reconciled,
        DeliveryReceiptStatus::Rejected => AccountDeliveryReceiptState::Rejected,
    };
    repository
        .record_account_delivery_receipt(&AccountDeliveryReceipt {
            schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
            lease: claim.lease,
            receipt_id: format!("copy-node-{observed_ms}-{status:?}"),
            state,
            observed_ms,
            account_fact_digest: matches!(
                status,
                DeliveryReceiptStatus::Applied | DeliveryReceiptStatus::Reconciled
            )
            .then_some([62; 32])
            .unwrap_or([0; 32]),
            detail: match status {
                DeliveryReceiptStatus::Applied => String::new(),
                DeliveryReceiptStatus::Unknown => "node requires reconciliation".to_owned(),
                DeliveryReceiptStatus::Reconciled => {
                    "node durable reconciliation completed".to_owned()
                }
                DeliveryReceiptStatus::Rejected => "node rejected semantic job".to_owned(),
            },
        })
        .await?;
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
) -> Result<CopyWorker, venue_control::CopyWorkerError> {
    CopyWorker::new(
        repository,
        CopyWorkerConfig {
            mode: GatewayMode::Live,
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
        mode: GatewayMode::Live,
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
         VALUES ('copy-btc', 'binance', 'LIVE', \
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
            sqlx::raw_sql(MIGRATION_0005).execute(&self.pool).await?;
            sqlx::raw_sql(MIGRATION_0006).execute(&self.pool).await?;
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
