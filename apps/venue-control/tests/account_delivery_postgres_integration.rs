use std::{env, process, time::SystemTime};

use rust_decimal::Decimal;
use sqlx::{Executor, PgPool, postgres::PgPoolOptions};
use venue_control::{
    AccountDeliveryRepository, AccountDeliveryRepositoryError, ControlRepository, ControlService,
    DeliveryStoreResult, MIGRATION_0001, MIGRATION_0002, MIGRATION_0003, MIGRATION_0004,
    MIGRATION_0005, MIGRATION_0006, PgControlRepository,
};
use venue_control_protocol::{
    ACCOUNT_DELIVERY_SCHEMA_VERSION, AccountDeliveryAck, AccountDeliveryBinding,
    AccountDeliveryPurpose, AccountDeliveryReceipt, AccountDeliveryReceiptState, AccountSummary,
    CONTROL_SCHEMA_VERSION, CommandState, ConnectionState, ControlAction, ControlCommandRequest,
    ControlSnapshot, GatewayMode, HealthState, StrategyKind, StrategyLifecycle, StrategySummary,
    VenueId,
};

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
            equity: Decimal::from(1_000),
            available_margin: Decimal::from(900),
            unrealized_pnl: Decimal::ZERO,
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
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
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
