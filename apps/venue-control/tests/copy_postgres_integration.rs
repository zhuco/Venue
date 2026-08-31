use std::{env, process, time::SystemTime};

use rust_decimal::Decimal;
use sqlx::{Executor, PgPool, postgres::PgPoolOptions};
use venue_control::{
    AccountDeliveryRepository, ControlRepository, CopyApplyResult, CopyExecutionProjectionInput,
    CopyLeaderEnvelope, CopyLeaderIntent, CopyLeaderSnapshot, CopyLedgerProjectionInput,
    CopyObserverScope, CopyPlanningSnapshot, CopyRelationRepository, CopyRelationRepositoryError,
    CopyReplayDeliveryState, CopyRepository, CopyWorker, CopyWorkerConfig, FrozenCapitalSnapshot,
    MIGRATION_0001, MIGRATION_0002, MIGRATION_0003, MIGRATION_0004, MIGRATION_0005, MIGRATION_0006,
    MIGRATION_0007, MIGRATION_0008, MIGRATION_0009, MIGRATION_0010, MIGRATION_0011, MIGRATION_0012,
    MIGRATION_0013, MIGRATION_0014, PgControlRepository, ScopedCopyDeliveryReceipt,
};
use venue_control_protocol::{
    ACCOUNT_DELIVERY_SCHEMA_VERSION, AccountDeliveryAck, AccountDeliveryBinding,
    AccountDeliveryPayload, AccountDeliveryReceipt, AccountDeliveryReceiptState,
    CONTROL_SCHEMA_VERSION, CopyLedgerFact, CopyLifecyclePolicy, CopyRelationBinding,
    CopyRelationConfig, CopyRelationReceiptState, CopyRelationUpsertRequest, CopyRiskPolicy,
    ExecutionFactBinding, ExecutionFactsSnapshot, GatewayMode, VenueId,
};
use venue_copy::{
    AuthoritativePositionSnapshot, CopyAction, CopyExecutionResult, CopyExecutionState, CopyId,
    CopyIdentityInput, DeliveryBinding, DeliveryReceiptStatus, LedgerAttribution, LedgerEntry,
    PersistedDeliveryReceipt, derive_copy_identities, plan_copy_execution,
};
use venue_domain::domain::{Amount, Asset, InstrumentIdentity, MarketKind, Symbol};

#[path = "copy_postgres/execution.rs"]
mod execution_contract;

#[path = "copy_postgres/claim_window.rs"]
mod claim_window;

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
    repository
        .upsert_copy_relation(&active_relation_request()?, 9_999)
        .await?;
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
            venue: if instance_id == "leader-btc" {
                VenueId::Bybit
            } else {
                VenueId::Binance
            },
            mode: GatewayMode::Live,
            trading_account_id: if instance_id == "leader-btc" {
                "00000000-0000-4000-8000-000000000002"
            } else {
                "00000000-0000-4000-8000-000000000001"
            }
            .to_owned(),
            instance_id: instance_id.to_owned(),
            symbol: "BTC/USDT".parse()?,
        })
    };
    Ok(CopyRelationUpsertRequest {
        schema_version: CONTROL_SCHEMA_VERSION,
        request_id: match expected_revision {
            None => "00000000-0000-4000-8000-000000000011",
            Some(1) => "00000000-0000-4000-8000-000000000012",
            Some(_) => "00000000-0000-4000-8000-000000000013",
        }
        .to_owned(),
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

#[tokio::test]
async fn paused_relation_still_records_original_child_but_cannot_rebind_its_phase()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url() else {
        println!(
            "SKIP: VENUE_CONTROL_TEST_DATABASE_URL is not set; historical Copy result test was not run"
        );
        return Ok(());
    };
    let fixture = PgFixture::create(&database_url, "historical_result").await?;
    fixture.migrate_twice().await?;
    install_account_scope(&fixture.pool).await?;
    let repository = PgControlRepository::new(fixture.pool.clone());
    let scope = scope("historical-observer");
    let worker = make_worker(repository.clone(), scope.clone(), "planner")?;
    let now = 30_000;
    repository
        .store_execution_facts(&ExecutionFactsSnapshot {
            schema_version: CONTROL_SCHEMA_VERSION,
            generated_ms: now,
            orders: Vec::new(),
            positions: Vec::new(),
            fills: Vec::new(),
            reconciliation: Vec::new(),
            copy_ledger: Vec::new(),
            drift: Vec::new(),
            execution: Vec::new(),
            risk: Vec::new(),
            health: Vec::new(),
        })
        .await?;
    let planned = plan_one(&repository, &worker, &scope, 40, now).await?;
    let original = AuthoritativePositionSnapshot {
        binding: planned.job.manifest.binding.clone(),
        generation: 1,
        observed_at_ms: now,
        expires_at_ms: now + 1_000,
        exposure: planned.frozen_capital.follower_managed_exposure.clone(),
        fact_digest: [80; 32],
    };
    let mut result = CopyExecutionProjectionInput {
        job_id: planned.job.identities.job_id,
        execution: CopyExecutionResult {
            request: plan_copy_execution(
                &planned.job.manifest,
                &planned.target,
                &original,
                now + 3,
            )?,
            state: CopyExecutionState::Accepted,
            command_id: Some("copy-original-child".to_owned()),
            fact_digest: [81; 32],
            reconciled_position: None,
            observed_at_ms: now + 3,
        },
    };
    assert_eq!(
        worker.record_execution(&result).await?,
        CopyApplyResult::Stored
    );
    persist_node_receipt(&repository, &scope, DeliveryReceiptStatus::Unknown, now + 4).await?;
    let paused = relation_request(Some(1), Decimal::ONE)?;
    repository.upsert_copy_relation(&paused, now + 4).await?;
    persist_node_receipt(
        &repository,
        &scope,
        DeliveryReceiptStatus::Reconciled,
        now + 5,
    )
    .await?;
    let mut closing = original;
    closing.generation = 2;
    closing.observed_at_ms = now + 5;
    closing.exposure = planned.target.target_exposure.clone();
    closing.exposure.value -= Decimal::ONE;
    closing.fact_digest = [82; 32];
    result.execution.state = CopyExecutionState::Reconciled;
    result.execution.observed_at_ms = now + 5;
    result.execution.fact_digest = [83; 32];
    result.execution.reconciled_position = Some(closing);
    assert_eq!(
        worker.record_execution(&result).await?,
        CopyApplyResult::Stored
    );
    assert_eq!(
        worker.record_execution(&result).await?,
        CopyApplyResult::Existing
    );
    let mut conflicting = result.clone();
    conflicting.execution.request.position_generation = 3;
    conflicting
        .execution
        .reconciled_position
        .as_mut()
        .ok_or("closing fact")?
        .generation = 4;
    assert!(worker.record_execution(&conflicting).await.is_err());
    assert_eq!(
        scalar_i64(
            &fixture.pool,
            "SELECT count(*) FROM venue_copy_execution_results"
        )
        .await?,
        1
    );
    // The worker reads the immutable delivery plus the account-node Unknown/Reconciled facts;
    // it does not need a Copy consumer claim or the now-paused current relation to account for
    // the original child.
    assert_eq!(
        worker.project_next_reconciled_ledger(now + 6).await?,
        Some(CopyApplyResult::Stored)
    );
    assert_eq!(worker.project_next_reconciled_ledger(now + 6).await?, None);
    assert_eq!(
        scalar_i64(&fixture.pool, "SELECT count(*) FROM venue_copy_ledger").await?,
        1
    );
    assert_eq!(
        scalar_i64(
            &fixture.pool,
            "SELECT count(*) FROM venue_copy_delivery_outbox \
             WHERE delivery_state = 'settled' AND claimed_by IS NULL AND claim_epoch = 0 \
               AND claimed_at_ms IS NULL AND claim_expires_at_ms IS NULL",
        )
        .await?,
        1
    );
    let canonical_receipts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM venue_copy_delivery_receipts WHERE job_id = $1")
            .bind(planned.job.identities.job_id.to_string())
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(canonical_receipts, 2);
    let replay = worker.recover(now + 7).await?;
    assert_eq!(replay.drift_projections.len(), 1);
    assert!(replay.drift_projections[0].repair.is_none());
    assert_eq!(
        scalar_i64(
            &fixture.pool,
            "SELECT count(*) FROM venue_control_events WHERE event_json->>'event_type' = 'execution_facts'",
        )
        .await?,
        1
    );

    let binding = ExecutionFactBinding {
        venue: scope.venue,
        mode: GatewayMode::Live,
        trading_account_id: scope.trading_account_id.clone(),
        symbol: planned.job.manifest.binding.instrument.symbol.clone(),
        instance_id: planned.job.manifest.binding.follower_instance_id.clone(),
        config_epoch: 7,
    };
    let mut other_binding = binding.clone();
    other_binding.trading_account_id = "00000000-0000-4000-8000-000000000002".to_owned();
    other_binding.instance_id = "copy-other".to_owned();
    let node_ledger = |binding: ExecutionFactBinding, digest| CopyLedgerFact {
        relation_id: planned
            .job
            .manifest
            .binding
            .relation
            .relation_id
            .to_string(),
        relation_revision: planned.job.manifest.binding.relation.revision,
        job_id: planned.job.identities.job_id.to_string(),
        binding,
        ledger_sequence: None,
        managed_exposure: planned.target.target_exposure.value,
        signed_generation: 2,
        observed_ms: now + 8,
        fact_digest: digest,
    };
    repository
        .store_execution_facts(&ExecutionFactsSnapshot {
            schema_version: CONTROL_SCHEMA_VERSION,
            generated_ms: now + 8,
            orders: Vec::new(),
            positions: Vec::new(),
            fills: Vec::new(),
            reconciliation: Vec::new(),
            copy_ledger: vec![
                node_ledger(binding.clone(), [91; 32]),
                node_ledger(other_binding, [92; 32]),
            ],
            drift: Vec::new(),
            execution: Vec::new(),
            risk: Vec::new(),
            health: Vec::new(),
        })
        .await?;
    let facts = repository
        .load_execution_facts()
        .await?
        .ok_or("execution facts missing")?;
    let durable = facts
        .copy_ledger
        .iter()
        .find(|fact| fact.binding == binding)
        .ok_or("durable ledger fact missing")?;
    assert_eq!(durable.ledger_sequence, Some(1));
    assert_eq!(
        durable.relation_revision,
        planned.job.manifest.binding.relation.revision
    );
    assert!(facts.copy_ledger.iter().any(|fact| {
        fact.binding.trading_account_id == "00000000-0000-4000-8000-000000000002"
            && fact.ledger_sequence.is_none()
    }));
    assert_eq!(facts.drift.len(), 1);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn rejected_node_delivery_is_canonicalized_without_retry_or_ledger()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url() else {
        println!(
            "SKIP: VENUE_CONTROL_TEST_DATABASE_URL is not set; rejected Copy receipt test was not run"
        );
        return Ok(());
    };
    let fixture = PgFixture::create(&database_url, "rejected_delivery").await?;
    fixture.migrate_twice().await?;
    install_account_scope(&fixture.pool).await?;
    let repository = PgControlRepository::new(fixture.pool.clone());
    let scope = scope("rejected-observer");
    let worker = make_worker(repository.clone(), scope.clone(), "planner")?;
    let now = 35_000;
    let planned = plan_one(&repository, &worker, &scope, 41, now).await?;
    persist_node_receipt(
        &repository,
        &scope,
        DeliveryReceiptStatus::Rejected,
        now + 4,
    )
    .await?;

    assert_eq!(
        worker.project_next_rejected_delivery().await?,
        Some(CopyApplyResult::Stored)
    );
    assert_eq!(worker.project_next_rejected_delivery().await?, None);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM venue_copy_delivery_receipts \
             WHERE job_id = $1 AND status = 'rejected'",
        )
        .bind(planned.job.identities.job_id.to_string())
        .fetch_one(&fixture.pool)
        .await?,
        1
    );
    assert_eq!(
        scalar_i64(
            &fixture.pool,
            "SELECT count(*) FROM venue_copy_receipt_outbox",
        )
        .await?,
        0
    );
    assert_eq!(
        scalar_i64(&fixture.pool, "SELECT count(*) FROM venue_copy_ledger").await?,
        0
    );
    let replay = worker.recover(now + 5).await?;
    assert_eq!(replay.jobs.len(), 1);
    assert_eq!(
        replay.jobs[0].delivery_state,
        CopyReplayDeliveryState::Settled
    );
    assert_eq!(replay.jobs[0].receipts.len(), 1);
    assert_eq!(
        replay.jobs[0].receipts[0].status,
        DeliveryReceiptStatus::Rejected
    );
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn node_projection_records_copy_results_atomically_with_its_cursor()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url() else {
        return Ok(());
    };
    let fixture = PgFixture::create(&database_url, "copy_projection").await?;
    fixture.migrate_twice().await?;
    install_account_scope(&fixture.pool).await?;
    let repository = PgControlRepository::new(fixture.pool.clone());
    let scope = scope("projection-observer");
    let worker = make_worker(repository.clone(), scope.clone(), "planner")?;
    let now = 40_000;
    let planned = plan_one(&repository, &worker, &scope, 50, now).await?;
    let original = AuthoritativePositionSnapshot {
        binding: planned.job.manifest.binding.clone(),
        generation: 1,
        observed_at_ms: now,
        expires_at_ms: now + 1_000,
        exposure: planned.frozen_capital.follower_managed_exposure.clone(),
        fact_digest: [90; 32],
    };
    let mut result = CopyExecutionResult {
        request: plan_copy_execution(&planned.job.manifest, &planned.target, &original, now + 3)?,
        state: CopyExecutionState::Prepared,
        command_id: Some("copy-projection-child".to_owned()),
        fact_digest: [0; 32],
        reconciled_position: None,
        observed_at_ms: now + 3,
    };
    let first = copy_result_projection(&result, 1, [91; 32], [0; 32])?;
    repository.merge_node_projection(&first).await?;
    assert_eq!(
        scalar_i64(
            &fixture.pool,
            "SELECT count(*) FROM venue_copy_execution_results"
        )
        .await?,
        1
    );
    result.state = CopyExecutionState::Reconciled;
    result.fact_digest = [92; 32];
    result.observed_at_ms = now + 5;
    result.reconciled_position = Some(AuthoritativePositionSnapshot {
        generation: 2,
        observed_at_ms: now + 5,
        exposure: planned.target.target_exposure.clone(),
        fact_digest: [93; 32],
        ..original
    });
    let next = copy_result_projection(&result, 2, [94; 32], first.digest)?;
    let mut invalid_batch = next.clone();
    let mut unknown_job = result.clone();
    unknown_job.request.job_id = CopyId::parse("00000000-0000-4000-8000-000000000099")?;
    invalid_batch.copy_execution_evidence.extend(
        copy_result_projection(&unknown_job, 2, [95; 32], first.digest)?.copy_execution_evidence,
    );
    assert!(
        repository
            .merge_node_projection(&invalid_batch)
            .await
            .is_err()
    );
    assert_eq!(
        scalar_i64(
            &fixture.pool,
            "SELECT count(*) FROM venue_copy_execution_results WHERE execution_state='prepared'"
        )
        .await?,
        1
    );
    assert_eq!(
        scalar_i64(
            &fixture.pool,
            "SELECT projection_sequence FROM venue_account_node_projection_inbox"
        )
        .await?,
        1
    );
    // Prepared -> Reconciled may skip intermediate upload states, but not the signed proof.
    repository.merge_node_projection(&next).await?;
    assert_eq!(
        repository.merge_node_projection(&next).await?,
        venue_control::SnapshotStoreResult::Unchanged
    );
    let saved: serde_json::Value =
        sqlx::query_scalar("SELECT result_json FROM venue_copy_execution_results")
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(
        serde_json::from_value::<CopyExecutionResult>(saved)?,
        result
    );
    let mut foreign = next;
    foreign.sequence = 3;
    foreign.previous_digest = foreign.digest;
    foreign.digest = [96; 32];
    foreign.copy_execution_evidence[0].relation_revision += 1;
    assert!(repository.merge_node_projection(&foreign).await.is_err());
    assert_eq!(
        scalar_i64(
            &fixture.pool,
            "SELECT projection_sequence FROM venue_account_node_projection_inbox"
        )
        .await?,
        2
    );
    fixture.cleanup().await?;
    Ok(())
}

fn copy_result_projection(
    result: &CopyExecutionResult,
    sequence: u64,
    digest: [u8; 32],
    previous_digest: [u8; 32],
) -> Result<venue_control_protocol::NodeProjectionEnvelope, Box<dyn std::error::Error>> {
    use sha2::{Digest, Sha256};
    use venue_control_protocol::*;
    let request = &result.request;
    let binding = AccountDeliveryBinding {
        venue: VenueId::Binance,
        mode: GatewayMode::Live,
        trading_account_id: request.binding.account_id.clone(),
        symbol: request.binding.instrument.symbol.clone(),
        instance_id: request.binding.follower_instance_id.clone(),
        config_epoch: 7,
    };
    let now = result.observed_at_ms;
    let snapshot = ControlSnapshot {
        schema_version: CONTROL_SCHEMA_VERSION,
        generated_ms: now,
        connection: ConnectionState::Live,
        accounts: vec![AccountSummary {
            venue: binding.venue,
            mode: binding.mode,
            trading_account_id: binding.trading_account_id.clone(),
            health: HealthState::Healthy,
            equity: None,
            available_margin: None,
            unrealized_pnl: None,
            balances: Vec::new(),
            private_generation: 2,
            writer_generation: 1,
            last_reconciled_ms: now,
        }],
        strategies: vec![StrategySummary {
            instance_id: binding.instance_id.clone(),
            kind: StrategyKind::Copy,
            venue: binding.venue,
            mode: binding.mode,
            trading_account_id: binding.trading_account_id.clone(),
            symbol: binding.symbol.clone(),
            lifecycle: StrategyLifecycle::Running,
            config_epoch: 7,
            open_orders: 0,
            long_quantity: Decimal::ZERO,
            short_quantity: Decimal::ZERO,
            realized_pnl: None,
            unrealized_pnl: None,
            last_receipt_ms: now,
            attention: None,
        }],
        copy_relations: Vec::new(),
        markets: Vec::new(),
        ledger: Vec::new(),
    };
    let result_bytes = serde_json::to_string(result)?;
    let evidence = CopyExecutionEvidence {
        encoding: CopyExecutionEvidenceEncoding::VenueCopyExecutionResultV1,
        relation_id: request.binding.relation.relation_id.to_string(),
        relation_revision: request.binding.relation.revision,
        job_id: request.job_id.to_string(),
        binding: ExecutionFactBinding {
            venue: binding.venue,
            mode: binding.mode,
            trading_account_id: binding.trading_account_id.clone(),
            symbol: binding.symbol.clone(),
            instance_id: binding.instance_id.clone(),
            config_epoch: binding.config_epoch,
        },
        phase: match request.phase {
            venue_copy::CopyExecutionPhase::ReduceToZero => {
                CopyExecutionPhaseProjection::ReduceToZero
            }
            venue_copy::CopyExecutionPhase::Adjust => CopyExecutionPhaseProjection::Adjust,
        },
        state: match result.state {
            CopyExecutionState::Prepared => CopyExecutionStateProjection::Prepared,
            CopyExecutionState::Reconciled => CopyExecutionStateProjection::Reconciled,
            _ => return Err("fixture only uses Prepared and Reconciled".into()),
        },
        command_id: result.command_id.clone(),
        observed_ms: now,
        result_fact_digest: result.fact_digest,
        result_sha256: Sha256::digest(result_bytes.as_bytes()).into(),
        result_bytes,
    };
    Ok(NodeProjectionEnvelope {
        schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
        binding,
        node_id: "copy-node".to_owned(),
        node_generation: 1,
        sequence,
        previous_digest,
        digest,
        snapshot,
        facts: ExecutionFactsSnapshot {
            schema_version: CONTROL_SCHEMA_VERSION,
            generated_ms: now,
            orders: Vec::new(),
            positions: Vec::new(),
            fills: Vec::new(),
            reconciliation: Vec::new(),
            copy_ledger: Vec::new(),
            drift: Vec::new(),
            execution: Vec::new(),
            risk: Vec::new(),
            health: Vec::new(),
        },
        copy_execution_evidence: vec![evidence],
        copy_planning_facts: Vec::new(),
    })
}

fn active_relation_request() -> Result<CopyRelationUpsertRequest, Box<dyn std::error::Error>> {
    let mut request = relation_request(None, Decimal::ONE)?;
    request.relation.lifecycle = CopyLifecyclePolicy::Active;
    Ok(request)
}

async fn plan_one(
    repository: &PgControlRepository,
    worker: &CopyWorker,
    scope: &CopyObserverScope,
    seed: u8,
    now: u64,
) -> Result<venue_control::PlannedCopyJob, Box<dyn std::error::Error>> {
    repository
        .upsert_copy_relation(&active_relation_request()?, now)
        .await?;
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
        generation: 2,
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
    let mut original_position = input.position.clone();
    original_position.generation = 1;
    original_position.exposure = planned.frozen_capital.follower_managed_exposure.clone();
    let execution = CopyExecutionProjectionInput {
        job_id: planned.job.identities.job_id,
        execution: CopyExecutionResult {
            request: plan_copy_execution(
                &planned.job.manifest,
                &planned.target,
                &original_position,
                now,
            )?,
            state: CopyExecutionState::Reconciled,
            command_id: Some("fixture-reconciled".to_owned()),
            fact_digest: [73; 32],
            reconciled_position: Some(input.position.clone()),
            observed_at_ms: now,
        },
    };
    assert_eq!(
        worker.record_execution(&execution).await?,
        CopyApplyResult::Stored
    );
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
            exposure_multiplier: Decimal::ONE,
        },
        binding: DeliveryBinding {
            relation: venue_copy::RelationCommitment {
                relation_id: CopyId::parse("00000000-0000-4000-8000-000000000010")?,
                revision: 1,
                policy_digest: active_relation_request()?.relation.policy_digest(),
            },
            leader_id: binding_ids.job_id,
            follower_id: binding_ids.planning_snapshot_id,
            follower_binding_id: binding_ids.child_order_id,
            follower_instance_id: "copy-btc".to_owned(),
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
            sqlx::raw_sql(MIGRATION_0007).execute(&self.pool).await?;
            sqlx::raw_sql(MIGRATION_0008).execute(&self.pool).await?;
            sqlx::raw_sql(MIGRATION_0009).execute(&self.pool).await?;
            sqlx::raw_sql(MIGRATION_0010).execute(&self.pool).await?;
            sqlx::raw_sql(MIGRATION_0011).execute(&self.pool).await?;
            sqlx::raw_sql(MIGRATION_0012).execute(&self.pool).await?;
            sqlx::raw_sql(MIGRATION_0013).execute(&self.pool).await?;
            sqlx::raw_sql(MIGRATION_0014).execute(&self.pool).await?;
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
