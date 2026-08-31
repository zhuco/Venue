use super::*;
use venue_control_protocol::AccountDeliveryPurpose;

async fn window_fixture(
    database_url: &str,
    name: &str,
) -> Result<(PgFixture, PgControlRepository, AccountDeliveryBinding, u64), Box<dyn std::error::Error>>
{
    let fixture = PgFixture::create(database_url, name).await?;
    fixture.migrate_twice().await?;
    let repository = PgControlRepository::new(fixture.pool.clone());
    install_account_scope(&fixture.pool).await?;
    repository
        .upsert_copy_relation(&active_relation_request()?, 9_999)
        .await?;
    let scope = scope(name);
    let worker = make_worker(repository.clone(), scope.clone(), name)?;
    let planned = plan_one(&repository, &worker, &scope, 1, 10_000).await?;
    let binding = AccountDeliveryBinding {
        venue: scope.venue,
        mode: scope.mode,
        trading_account_id: scope.trading_account_id,
        symbol: "BTC/USDT".parse()?,
        instance_id: "copy-btc".into(),
        config_epoch: 7,
    };
    Ok((
        fixture,
        repository,
        binding,
        planned.job.manifest.expires_at_ms,
    ))
}

#[tokio::test]
async fn never_claimed_copy_at_exact_expiry_does_not_gain_a_recovery_lease()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url() else {
        println!("SKIP: isolated PostgreSQL URL not provided; expiry boundary test not run");
        return Ok(());
    };
    let (fixture, repository, binding, expiry) =
        window_fixture(&database_url, "unclaimed_expiry").await?;
    for now in [expiry, expiry + 1] {
        assert!(
            repository
                .claim_account_deliveries(&binding, "expiry-node", now, now + 1_000, 1)
                .await?
                .is_empty()
        );
    }
    assert_eq!(
        scalar_i64(
            &fixture.pool,
            "SELECT count(*) FROM venue_account_delivery_claims"
        )
        .await?,
        0
    );
    assert_eq!(scalar_i64(&fixture.pool, "SELECT count(*) FROM venue_account_deliveries WHERE delivery_state='pending' AND lease_epoch=0").await?, 1);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn short_copy_claim_is_exclusive_and_unknown_only_reconciles()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url() else {
        println!("SKIP: isolated PostgreSQL URL not provided; short lease contention test not run");
        return Ok(());
    };
    let (fixture, repository, binding, expiry) =
        window_fixture(&database_url, "short_contention").await?;
    let (first, second) = tokio::join!(
        repository.claim_account_deliveries(
            &binding,
            "window-node-a",
            expiry - 100,
            expiry + 1_000,
            1
        ),
        repository.claim_account_deliveries(
            &binding,
            "window-node-b",
            expiry - 100,
            expiry + 1_000,
            1
        ),
    );
    let mut claims = first?;
    claims.extend(second?);
    assert_eq!(claims.len(), 1);
    let claim = claims.pop().ok_or("exclusive claim missing")?;
    assert_eq!(claim.lease.lease_epoch, 1);
    assert_eq!(claim.lease.expires_at_ms, expiry);
    assert!(
        repository
            .claim_account_deliveries(&binding, "window-node-c", expiry - 95, expiry + 1_000, 1)
            .await?
            .is_empty()
    );
    repository
        .acknowledge_account_delivery(&AccountDeliveryAck {
            schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
            lease: claim.lease.clone(),
            acknowledged_ms: expiry - 90,
            durable_inbox_digest: [81; 32],
        })
        .await?;
    repository
        .record_account_delivery_receipt(&AccountDeliveryReceipt {
            schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
            lease: claim.lease.clone(),
            receipt_id: "short-window-unknown".into(),
            state: AccountDeliveryReceiptState::Unknown,
            observed_ms: expiry - 80,
            account_fact_digest: [82; 32],
            detail: "original child outcome needs signed recovery".into(),
        })
        .await?;
    let recovery = repository
        .claim_account_deliveries(&binding, "window-node-d", expiry - 70, expiry + 1_000, 1)
        .await?
        .pop()
        .ok_or("Unknown recovery missing")?;
    assert_eq!(
        recovery.lease.purpose,
        AccountDeliveryPurpose::ReconcileOnly
    );
    assert_eq!(recovery.lease.lease_epoch, 2);
    assert_eq!(recovery.lease.expires_at_ms, expiry + 1_000);
    assert_eq!(recovery.payload, claim.payload);
    assert!(!recovery.grants_mutation_authority());
    assert_eq!(
        scalar_i64(
            &fixture.pool,
            "SELECT count(*) FROM venue_account_delivery_claims"
        )
        .await?,
        2
    );
    assert_eq!(
        scalar_i64(&fixture.pool, "SELECT count(*) FROM venue_copy_ledger").await?,
        0
    );
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn copy_claim_window_caps_install_but_preserves_expired_reconciliation()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url() else {
        println!("SKIP: isolated PostgreSQL URL not provided; Copy claim-window test not run");
        return Ok(());
    };
    let fixture = PgFixture::create(&database_url, "claim_window").await?;
    fixture.migrate_twice().await?;
    let repository = PgControlRepository::new(fixture.pool.clone());
    install_account_scope(&fixture.pool).await?;
    repository
        .upsert_copy_relation(&active_relation_request()?, 9_999)
        .await?;
    let scope = scope("claim-window-observer");
    let worker = make_worker(repository.clone(), scope.clone(), "claim-window-worker")?;
    let planned = plan_one(&repository, &worker, &scope, 1, 10_000).await?;
    let binding = AccountDeliveryBinding {
        venue: scope.venue,
        mode: scope.mode,
        trading_account_id: scope.trading_account_id.clone(),
        symbol: "BTC/USDT".parse()?,
        instance_id: "copy-btc".into(),
        config_epoch: 7,
    };
    let expiry = planned.job.manifest.expires_at_ms;
    let claim = repository
        .claim_account_deliveries(
            &binding,
            "claim-window-node",
            expiry - 100,
            expiry + 1_000,
            1,
        )
        .await?
        .pop()
        .ok_or("fresh final-window Copy job was not claimable")?;
    assert_eq!(claim.lease.purpose, AccountDeliveryPurpose::Install);
    assert_eq!(claim.lease.expires_at_ms, expiry);
    let AccountDeliveryPayload::CopySemanticJob(payload) = &claim.payload else {
        return Err("expected immutable Copy payload".into());
    };
    assert_eq!(payload.expires_at_ms, expiry);
    assert_eq!(
        payload.manifest,
        serde_json::to_value(&planned.job.manifest)?
    );
    // Both row and immutable claim must commit the same shortened boundary.
    let durable_expiry: i64 = sqlx::query_scalar(
        "SELECT lease_expires_at_ms FROM venue_account_deliveries WHERE delivery_id=$1",
    )
    .bind(&claim.lease.delivery_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(durable_expiry, i64::try_from(expiry)?);
    repository
        .acknowledge_account_delivery(&AccountDeliveryAck {
            schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
            lease: claim.lease.clone(),
            acknowledged_ms: expiry - 90,
            durable_inbox_digest: [71; 32],
        })
        .await?;
    let unknown = AccountDeliveryReceipt {
        schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
        lease: claim.lease.clone(),
        receipt_id: "claim-window-unknown".into(),
        state: AccountDeliveryReceiptState::Unknown,
        observed_ms: expiry,
        account_fact_digest: [72; 32],
        detail: "original child requires signed reconciliation".into(),
    };
    assert!(
        repository
            .record_account_delivery_receipt(&unknown)
            .await
            .is_err()
    );
    let restarted = PgControlRepository::new(fixture.pool.clone());
    let recovery = restarted
        .claim_account_deliveries(
            &binding,
            "claim-window-node-restart",
            expiry,
            expiry + 1_000,
            1,
        )
        .await?
        .pop()
        .ok_or("expired acknowledged Copy job lost reconciliation")?;
    assert_eq!(
        recovery.lease.purpose,
        AccountDeliveryPurpose::ReconcileOnly
    );
    assert_eq!(recovery.lease.expires_at_ms, expiry + 1_000);
    assert_eq!(recovery.lease.lease_epoch, 2);
    assert_eq!(recovery.payload, claim.payload);
    assert!(!recovery.grants_mutation_authority());
    assert!(
        restarted
            .acknowledge_account_delivery(&AccountDeliveryAck {
                schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
                lease: recovery.lease.clone(),
                acknowledged_ms: expiry + 1,
                durable_inbox_digest: [73; 32],
            })
            .await
            .is_err()
    );
    assert_eq!(
        scalar_i64(&fixture.pool, "SELECT count(*) FROM venue_copy_jobs").await?,
        1
    );
    assert_eq!(
        scalar_i64(&fixture.pool, "SELECT count(*) FROM venue_copy_ledger").await?,
        0
    );
    fixture.cleanup().await?;
    Ok(())
}
