use std::{env, process, time::SystemTime};

use rust_decimal::Decimal;
use sqlx::{Executor, PgPool, postgres::PgPoolOptions};
use venue_control::{
    AccountDeliveryRepository, AccountDeliveryRepositoryError, ControlHttpConfig,
    ControlRepository, ControlService, DeliveryStoreResult, MIGRATION_0001, MIGRATION_0002,
    MIGRATION_0003, MIGRATION_0004, MIGRATION_0005, MIGRATION_0006, MIGRATION_0007, MIGRATION_0008,
    MIGRATION_0009, MIGRATION_0010, MIGRATION_0011, MIGRATION_0012, MIGRATION_0013, MIGRATION_0014,
    PgControlRepository,
    accounts::{AccountService, CredentialCipher},
    control_shutdown_channel, serve_local_with_accounts,
};
use venue_control_protocol::{
    ACCOUNT_DELIVERY_SCHEMA_VERSION, AccountDeliveryAck, AccountDeliveryBinding,
    AccountDeliveryPurpose, AccountDeliveryReceipt, AccountDeliveryReceiptState, AccountSummary,
    CONTROL_SCHEMA_VERSION, CommandState, ConnectionState, ControlAction, ControlCommandRequest,
    ControlSnapshot, ExecutionFactsSnapshot, GatewayMode, HealthState, NodeProjectionEnvelope,
    StrategyKind, StrategyLifecycle, StrategySummary, VenueId,
    accounts::{AccountErrorCode, AccountErrorResponse, SecretValue},
};

#[path = "copy_postgres/node_source.rs"]
mod node_copy_source;

#[tokio::test]
async fn postgres_delivery_lease_ack_unknown_reconcile_and_restart_are_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = PgFixture::create(&database_url).await?;
    fixture.migrate_twice().await?;
    let repository = PgControlRepository::new(fixture.pool.clone());
    repository.store_snapshot(&snapshot()?).await?;
    let service = ControlService::new(repository.clone());
    let command = command()?;
    let accepted = service.submit_command(&command, 101).await?;
    assert_eq!(accepted.state, CommandState::Accepted);

    let binding = binding()?;
    let claims = repository
        .claim_account_deliveries(&binding, "node-a", 110, 160, 10)
        .await?;
    assert_eq!(claims.len(), 1);
    let claim = claims[0].clone();
    assert_eq!(claim.lease.purpose, AccountDeliveryPurpose::Install);
    assert!(!claim.grants_mutation_authority());
    assert!(
        repository
            .claim_account_deliveries(&binding, "node-a", 111, 150, 10)
            .await?
            .is_empty()
    );

    let mut wrong = binding.clone();
    wrong.config_epoch += 1;
    assert_eq!(
        repository
            .claim_account_deliveries(&wrong, "wrong-node", 112, 150, 1)
            .await,
        Err(AccountDeliveryRepositoryError::BindingConflict)
    );

    let mut ack = AccountDeliveryAck {
        schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
        lease: claim.lease.clone(),
        acknowledged_ms: 160,
        durable_inbox_digest: [7; 32],
    };
    assert_eq!(
        repository.acknowledge_account_delivery(&ack).await,
        Err(AccountDeliveryRepositoryError::LeaseConflict)
    );
    ack.acknowledged_ms = 120;
    assert_eq!(
        repository.acknowledge_account_delivery(&ack).await?,
        DeliveryStoreResult::Stored
    );
    assert_eq!(
        repository.acknowledge_account_delivery(&ack).await?,
        DeliveryStoreResult::Existing
    );

    let unknown = AccountDeliveryReceipt {
        schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
        lease: claim.lease.clone(),
        receipt_id: "receipt-unknown".to_owned(),
        state: AccountDeliveryReceiptState::Unknown,
        observed_ms: 130,
        account_fact_digest: [0; 32],
        detail: "node restarted before a terminal account fact was durable".to_owned(),
    };
    assert_eq!(
        repository.record_account_delivery_receipt(&unknown).await?,
        DeliveryStoreResult::Stored
    );

    let restarted = PgControlRepository::new(fixture.pool.clone());
    let reconcile_claims = restarted
        .claim_account_deliveries(&binding, "node-b", 140, 190, 10)
        .await?;
    assert_eq!(reconcile_claims.len(), 1);
    let reconcile = &reconcile_claims[0];
    assert_eq!(
        reconcile.lease.purpose,
        AccountDeliveryPurpose::ReconcileOnly
    );
    assert_eq!(reconcile.lease.lease_epoch, claim.lease.lease_epoch + 1);
    assert!(
        restarted
            .claim_account_deliveries(&binding, "node-c", 141, 180, 10)
            .await?
            .is_empty()
    );

    let stale = AccountDeliveryReceipt {
        schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
        lease: claim.lease,
        receipt_id: "receipt-stale".to_owned(),
        state: AccountDeliveryReceiptState::Applied,
        observed_ms: 145,
        account_fact_digest: [8; 32],
        detail: String::new(),
    };
    assert_eq!(
        restarted.record_account_delivery_receipt(&stale).await,
        Err(AccountDeliveryRepositoryError::LeaseConflict)
    );

    let reconciled = AccountDeliveryReceipt {
        schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
        lease: reconcile.lease.clone(),
        receipt_id: "receipt-reconciled".to_owned(),
        state: AccountDeliveryReceiptState::Reconciled,
        observed_ms: 150,
        account_fact_digest: [9; 32],
        detail: "terminal state proved from the account node's durable reconciliation".to_owned(),
    };
    assert_eq!(
        restarted
            .record_account_delivery_receipt(&reconciled)
            .await?,
        DeliveryStoreResult::Stored
    );
    assert_eq!(
        restarted
            .record_account_delivery_receipt(&reconciled)
            .await?,
        DeliveryStoreResult::Existing
    );
    assert!(
        restarted
            .claim_account_deliveries(&binding, "node-c", 151, 180, 10)
            .await?
            .is_empty()
    );
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn postgres_projection_cursor_snapshot_and_facts_commit_as_one_unit()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = PgFixture::create(&database_url).await?;
    fixture.migrate_twice().await?;
    let repository = PgControlRepository::new(fixture.pool.clone());
    let first = projection(1, 1, [21; 32], [0; 32], 100)?;
    assert!(matches!(
        repository.merge_node_projection(&first).await?,
        venue_control::SnapshotStoreResult::Inserted { .. }
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM venue_account_node_projection_inbox")
            .fetch_one(&fixture.pool)
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT generated_ms FROM venue_control_snapshots")
            .fetch_one(&fixture.pool)
            .await?,
        100
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT generated_ms FROM venue_control_execution_facts")
            .fetch_one(&fixture.pool)
            .await?,
        100
    );
    assert_eq!(
        repository.merge_node_projection(&first).await?,
        venue_control::SnapshotStoreResult::Unchanged
    );
    // Simulate the deployed v11 cursor schema, then migrate its real envelope twice. All
    // destructive DDL is confined to this test's unique search_path schema.
    sqlx::raw_sql(
        "ALTER TABLE venue_account_node_projection_inbox DROP CONSTRAINT venue_account_node_projection_inbox_pkey; \
         ALTER TABLE venue_account_node_projection_inbox DROP COLUMN instance_id; \
         ALTER TABLE venue_account_node_projection_inbox ADD PRIMARY KEY (venue,mode,trading_account_id,node_id);",
    ).execute(&fixture.pool).await?;
    for _ in 0..2 {
        sqlx::raw_sql(MIGRATION_0012).execute(&fixture.pool).await?;
    }
    let migrated_instance: String =
        sqlx::query_scalar("SELECT instance_id FROM venue_account_node_projection_inbox")
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(migrated_instance, first.binding.instance_id);
    assert_eq!(
        repository.merge_node_projection(&first).await?,
        venue_control::SnapshotStoreResult::Unchanged
    );
    // Indexed cursor columns and their immutable JSON must agree even on the replay fast path.
    for tamper in [
        "UPDATE venue_account_node_projection_inbox SET projection_sequence=2",
        "UPDATE venue_account_node_projection_inbox SET node_id='wrong-node'",
        "UPDATE venue_account_node_projection_inbox SET instance_id='wrong-instance'",
        "UPDATE venue_account_node_projection_inbox SET projection_digest=decode(repeat('ab',32),'hex')",
    ] {
        sqlx::query(tamper).execute(&fixture.pool).await?;
        assert_eq!(
            repository.merge_node_projection(&first).await,
            Err(venue_control::RepositoryError::CorruptData)
        );
        sqlx::query("UPDATE venue_account_node_projection_inbox SET projection_sequence=$1,node_id=$2,instance_id=$3,projection_digest=$4")
            .bind(i64::try_from(first.sequence)?).bind(&first.node_id)
            .bind(&first.binding.instance_id).bind(first.digest.to_vec())
            .execute(&fixture.pool).await?;
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM venue_control_events")
            .fetch_one(&fixture.pool)
            .await?,
        2
    );
    let gap = projection(1, 3, [22; 32], [21; 32], 101)?;
    assert!(matches!(
        repository.merge_node_projection(&gap).await,
        Err(venue_control::RepositoryError::ReplayConflict)
    ));
    let rollover = projection(2, 1, [23; 32], [0; 32], 101)?;
    assert!(matches!(
        repository.merge_node_projection(&rollover).await?,
        venue_control::SnapshotStoreResult::Inserted { .. }
    ));
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn postgres_projection_round_trips_over_the_loopback_node_client()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = PgFixture::create(&database_url).await?;
    fixture.migrate_twice().await?;
    let repository = PgControlRepository::new(fixture.pool.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (stop, shutdown) = control_shutdown_channel();
    let node_token = "fixture-node-token-0123456789abcdef";
    let accounts = std::sync::Arc::new(AccountService::new_with_node_token(
        fixture.pool.clone(),
        CredentialCipher::from_key(&[19; 32])?,
        Some(SecretValue::new(node_token.to_owned())),
    )?);
    let server = tokio::spawn(serve_local_with_accounts(
        listener,
        std::sync::Arc::new(ControlService::new(repository.clone())),
        accounts,
        ControlHttpConfig::default(),
        shutdown,
    ));
    let projection = projection(1, 1, [31; 32], [0; 32], 100)?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(std::time::Duration::from_secs(3))
        .timeout(std::time::Duration::from_secs(5))
        .build()?;
    for token in [None, Some("wrong-node-token-0123456789abcdef")] {
        let mut request = client
            .post(format!("http://{address}/v2/account-node/projection"))
            .json(&projection);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await?;
        assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.json::<AccountErrorResponse>().await?.code,
            AccountErrorCode::Unauthorized
        );
    }
    let response = client
        .post(format!("http://{address}/v2/account-node/projection"))
        .bearer_auth(node_token)
        .json(&projection)
        .send()
        .await?;
    assert!(response.status().is_success());
    let echoed = response.json::<NodeProjectionEnvelope>().await?;
    assert_eq!(echoed, projection);
    assert_eq!(repository.load_snapshot().await?, Some(projection.snapshot));
    assert_eq!(
        repository.load_execution_facts().await?,
        Some(projection.facts)
    );
    let _ = stop.send(true);
    server.await??;
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn postgres_node_updates_preserve_sibling_accounts_and_reject_forged_replays()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = PgFixture::create(&database_url).await?;
    fixture.migrate_twice().await?;
    let repository = PgControlRepository::new(fixture.pool.clone());
    let mut first = projection(1, 1, [41; 32], [0; 32], 200)?;
    let mut second = projection(1, 1, [42; 32], [0; 32], 100)?;
    second.binding.venue = VenueId::Bybit;
    second.binding.trading_account_id = "00000000-0000-4000-8000-000000000002".to_owned();
    second.binding.instance_id = "grid-bybit".to_owned();
    second.node_id = "node-projection-b".to_owned();
    second.snapshot.accounts[0].venue = second.binding.venue;
    second.snapshot.accounts[0].trading_account_id = second.binding.trading_account_id.clone();
    second.snapshot.strategies[0].venue = second.binding.venue;
    second.snapshot.strategies[0].trading_account_id = second.binding.trading_account_id.clone();
    second.snapshot.strategies[0].instance_id = second.binding.instance_id.clone();
    for envelope in [&mut first, &mut second] {
        envelope
            .facts
            .health
            .push(venue_control_protocol::AccountHealthFact {
                venue: envelope.binding.venue,
                mode: envelope.binding.mode,
                trading_account_id: envelope.binding.trading_account_id.clone(),
                health: HealthState::Healthy,
                private_generation: 1,
                last_reconciled_ms: envelope.snapshot.generated_ms - 1,
                observed_ms: envelope.snapshot.generated_ms,
                fact_digest: [48; 32],
            });
    }
    repository.merge_node_projection(&first).await?;
    // Account B is older than A, but this is not a regression in B's own stream.
    repository.merge_node_projection(&second).await?;
    let merged = repository
        .load_snapshot()
        .await?
        .ok_or("missing merged snapshot")?;
    assert_eq!(merged.accounts.len(), 2);
    assert_eq!(merged.strategies.len(), 2);
    assert_eq!(merged.generated_ms, 200);
    assert_eq!(
        repository
            .load_execution_facts()
            .await?
            .ok_or("missing facts")?
            .health
            .len(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM venue_control_strategy_scopes")
            .fetch_one(&fixture.pool)
            .await?,
        2
    );

    // Reusing an existing digest must not acknowledge changed content.
    let mut forged = first.clone();
    forged.snapshot.accounts[0].equity = Some(Decimal::from(999_999));
    assert_eq!(
        repository.merge_node_projection(&forged).await,
        Err(venue_control::RepositoryError::ReplayConflict)
    );

    // One account cannot replace another account's globally unique instance ID.
    let mut collision = second.clone();
    collision.sequence = 2;
    collision.previous_digest = second.digest;
    collision.digest = [43; 32];
    collision.binding.instance_id = first.binding.instance_id.clone();
    collision.sequence = 1;
    collision.previous_digest = [0; 32];
    collision.snapshot.strategies[0].instance_id = first.binding.instance_id.clone();
    assert_eq!(
        repository.merge_node_projection(&collision).await,
        Err(venue_control::RepositoryError::SnapshotConflict)
    );

    let mut next = first.clone();
    next.sequence = 2;
    next.previous_digest = first.digest;
    next.digest = [44; 32];
    next.snapshot.generated_ms = 201;
    next.facts.generated_ms = 201;
    next.snapshot.strategies.clear();
    next.facts.health.clear();
    repository.merge_node_projection(&next).await?;
    let merged = repository
        .load_snapshot()
        .await?
        .ok_or("missing snapshot")?;
    assert_eq!(merged.accounts.len(), 2);
    assert_eq!(merged.strategies, second.snapshot.strategies);
    // Empty instance facts cannot erase account-level evidence from another projection.
    assert_eq!(
        repository
            .load_execution_facts()
            .await?
            .ok_or("missing facts")?
            .health
            .len(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM venue_control_strategy_scopes")
            .fetch_one(&fixture.pool)
            .await?,
        1
    );
    assert_eq!(sqlx::query_scalar::<_, i64>(
        "SELECT projection_sequence FROM venue_account_node_projection_inbox WHERE node_id='node-projection-b'")
        .fetch_one(&fixture.pool).await?, 1);

    // The same account process owns independent per-instance outbox cursors.
    let mut sibling = first.clone();
    sibling.binding.instance_id = "grid-eth".to_owned();
    sibling.binding.symbol = "ETH/USDT".parse()?;
    sibling.snapshot.strategies[0].instance_id = sibling.binding.instance_id.clone();
    sibling.snapshot.strategies[0].symbol = sibling.binding.symbol.clone();
    sibling.digest = [45; 32];
    sibling
        .facts
        .positions
        .push(venue_control_protocol::SignedPositionFact {
            binding: venue_control_protocol::ExecutionFactBinding {
                venue: sibling.binding.venue,
                mode: sibling.binding.mode,
                trading_account_id: sibling.binding.trading_account_id.clone(),
                symbol: sibling.binding.symbol.clone(),
                instance_id: sibling.binding.instance_id.clone(),
                config_epoch: sibling.binding.config_epoch,
            },
            position_side: venue_domain::PositionSide::Net,
            quantity: Decimal::ONE,
            entry_price: None,
            mark_price: None,
            signed_generation: 1,
            observed_ms: 199,
            fact_digest: [47; 32],
        });
    repository.merge_node_projection(&sibling).await?;
    let mut restored = next.clone();
    restored.sequence = 3;
    restored.previous_digest = next.digest;
    restored.digest = [46; 32];
    restored.snapshot.strategies = first.snapshot.strategies;
    repository.merge_node_projection(&restored).await?;
    let merged = repository
        .load_snapshot()
        .await?
        .ok_or("missing sibling snapshot")?;
    assert_eq!(merged.accounts.len(), 2);
    assert_eq!(merged.strategies.len(), 3);
    assert_eq!(
        repository
            .load_execution_facts()
            .await?
            .ok_or("missing sibling facts")?
            .positions,
        sibling.facts.positions
    );
    assert_eq!(scalar_projection_cursor_count(&fixture.pool).await?, 3);
    let mut old_epoch = sibling.clone();
    old_epoch.node_id = "different-process".to_owned();
    old_epoch.binding.config_epoch -= 1;
    old_epoch.snapshot.strategies[0].config_epoch -= 1;
    old_epoch.facts.positions[0].binding.config_epoch -= 1;
    old_epoch.snapshot.generated_ms = 300;
    old_epoch.facts.generated_ms = 300;
    old_epoch.digest = [49; 32];
    assert_eq!(
        repository.merge_node_projection(&old_epoch).await,
        Err(venue_control::RepositoryError::SnapshotConflict)
    );
    assert_eq!(scalar_projection_cursor_count(&fixture.pool).await?, 3);
    fixture.cleanup().await?;
    Ok(())
}

async fn scalar_projection_cursor_count(pool: &PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT count(*) FROM venue_account_node_projection_inbox")
        .fetch_one(pool)
        .await
}

fn projection(
    generation: u64,
    sequence: u64,
    digest: [u8; 32],
    previous_digest: [u8; 32],
    generated_ms: u64,
) -> Result<NodeProjectionEnvelope, Box<dyn std::error::Error>> {
    let mut snapshot = snapshot()?;
    snapshot.generated_ms = generated_ms;
    snapshot.accounts[0].last_reconciled_ms = generated_ms - 1;
    snapshot.strategies[0].last_receipt_ms = generated_ms - 1;
    Ok(NodeProjectionEnvelope {
        schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
        binding: binding()?,
        node_id: "node-projection-a".to_owned(),
        node_generation: generation,
        sequence,
        previous_digest,
        digest,
        copy_execution_evidence: Vec::new(),
        copy_planning_facts: Vec::new(),
        snapshot,
        facts: ExecutionFactsSnapshot {
            schema_version: CONTROL_SCHEMA_VERSION,
            generated_ms,
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
    })
}

fn binding() -> Result<AccountDeliveryBinding, Box<dyn std::error::Error>> {
    Ok(AccountDeliveryBinding {
        venue: VenueId::Binance,
        mode: GatewayMode::Live,
        trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
        symbol: "BTC/USDT".parse()?,
        instance_id: "grid-btc".to_owned(),
        config_epoch: 7,
    })
}

fn command() -> Result<ControlCommandRequest, Box<dyn std::error::Error>> {
    let binding = binding()?;
    Ok(ControlCommandRequest {
        schema_version: CONTROL_SCHEMA_VERSION,
        request_id: "request-1".to_owned(),
        venue: binding.venue,
        mode: binding.mode,
        trading_account_id: binding.trading_account_id,
        instance_id: binding.instance_id,
        symbol: binding.symbol,
        action: ControlAction::Pause,
        trade: None,
        expected_config_epoch: binding.config_epoch,
        confirmation: None,
    })
}

fn snapshot() -> Result<ControlSnapshot, Box<dyn std::error::Error>> {
    let binding = binding()?;
    Ok(ControlSnapshot {
        schema_version: CONTROL_SCHEMA_VERSION,
        generated_ms: 100,
        connection: ConnectionState::Live,
        accounts: vec![AccountSummary {
            venue: binding.venue,
            mode: binding.mode,
            trading_account_id: binding.trading_account_id.clone(),
            health: HealthState::Healthy,
            equity: Some(Decimal::from(1_000)),
            available_margin: Some(Decimal::from(900)),
            unrealized_pnl: Some(Decimal::ZERO),
            balances: Vec::new(),
            private_generation: 1,
            writer_generation: 1,
            last_reconciled_ms: 99,
        }],
        strategies: vec![StrategySummary {
            instance_id: binding.instance_id,
            kind: StrategyKind::Grid,
            venue: binding.venue,
            mode: binding.mode,
            trading_account_id: binding.trading_account_id,
            symbol: binding.symbol,
            lifecycle: StrategyLifecycle::Running,
            config_epoch: binding.config_epoch,
            open_orders: 0,
            long_quantity: Decimal::ZERO,
            short_quantity: Decimal::ZERO,
            realized_pnl: Some(Decimal::ZERO),
            unrealized_pnl: Some(Decimal::ZERO),
            last_receipt_ms: 99,
            attention: None,
        }],
        copy_relations: Vec::new(),
        markets: Vec::new(),
        ledger: Vec::new(),
    })
}

fn integration_database_url() -> Result<Option<String>, Box<dyn std::error::Error>> {
    let database_url = env::var("VENUE_CONTROL_TEST_DATABASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty());
    if database_url.is_none()
        && env::var("VENUE_CONTROL_POSTGRES_REQUIRED").ok().as_deref() == Some("1")
    {
        return Err(
            "VENUE_CONTROL_TEST_DATABASE_URL is required by the PostgreSQL integration gate".into(),
        );
    }
    if database_url.is_none() {
        println!(
            "SKIP: VENUE_CONTROL_TEST_DATABASE_URL is not set; account delivery PostgreSQL test was not run"
        );
    }
    Ok(database_url)
}

struct PgFixture {
    database_url: String,
    schema: String,
    pool: PgPool,
}

impl PgFixture {
    async fn create(database_url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let nonce = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_nanos();
        let schema = format!("venue_delivery_test_{}_{}", process::id(), nonce);
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
        println!("PostgreSQL integration database connected (connection string redacted)");
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
