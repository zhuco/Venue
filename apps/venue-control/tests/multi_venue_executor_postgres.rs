//! Isolated PostgreSQL contract coverage for the strategy ledger.  This suite never falls back
//! to the production URL: callers must provide the dedicated QA connection explicitly.

use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use sqlx::{Executor, PgPool, Row, postgres::PgPoolOptions};
use venue_control::{
    MultiVenueStore, StrategyEnqueueResult, install_control_schema,
    support_martingale::{SupportMartingaleCommandKind, SupportMartingaleStore},
};
use venue_control_protocol::{
    VenueId,
    support_martingale::{
        SUPPORT_MARTINGALE_SCHEMA_VERSION, SupportMartingaleAction, SupportMartingaleConfig,
        SupportMartingaleCreateRequest, SupportMartingaleLifecycleRequest,
    },
};
use venue_domain::{
    CommandId, ExecutionCommand, LimitTimeInForce, OrderCommand, OrderOwner, OrderPurpose,
    OrderSide, PositionSide, Price, Symbol,
};

const VENUES: [VenueId; 5] = [
    VenueId::Bitget,
    VenueId::Gate,
    VenueId::Bybit,
    VenueId::Okx,
    VenueId::Hyperliquid,
];

static NEXT_ACCOUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

struct Fixture {
    pool: PgPool,
    admin: PgPool,
    schema: String,
}

impl Fixture {
    async fn create() -> Result<Option<Self>, Box<dyn std::error::Error>> {
        let Ok(url) = std::env::var("VENUE_CONTROL_TEST_DATABASE_URL") else {
            if std::env::var("VENUE_CONTROL_POSTGRES_REQUIRED")
                .ok()
                .as_deref()
                == Some("1")
            {
                return Err("multi-venue PostgreSQL QA URL is required".into());
            }
            eprintln!("SKIP: multi-venue PostgreSQL QA URL is not configured");
            return Ok(None);
        };
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let schema = format!("venue_multi_executor_{}_{}", std::process::id(), nonce);
        admin
            .execute(format!("CREATE SCHEMA {schema}").as_str())
            .await?;
        let search_path = schema.clone();
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .after_connect(move |connection, _| {
                let sql = format!("SET search_path TO {search_path}");
                Box::pin(async move {
                    connection.execute(sql.as_str()).await?;
                    Ok(())
                })
            })
            .connect(&url)
            .await?;
        install_control_schema(&pool).await?;
        Ok(Some(Self {
            pool,
            admin,
            schema,
        }))
    }

    async fn cleanup(self) -> Result<(), sqlx::Error> {
        self.pool.close().await;
        self.admin
            .execute(format!("DROP SCHEMA {} CASCADE", self.schema).as_str())
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

async fn seed(
    pool: &PgPool,
    venue: VenueId,
    suffix: &str,
) -> Result<(String, String, String), sqlx::Error> {
    let user = format!("user_{suffix}");
    let account_number = NEXT_ACCOUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let account = format!("00000000-0000-4000-8000-{account_number:012x}");
    let credential = format!("credential_{suffix}");
    let identity = Sha256::digest(suffix.as_bytes()).to_vec();
    let fingerprint = Sha256::digest(format!("key:{suffix}").as_bytes()).to_vec();
    sqlx::query(
        "INSERT INTO venue_users(user_id,username,password_hash,created_ms) VALUES($1,$1,'fixture',1)",
    )
    .bind(&user)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO venue_user_trading_accounts(trading_account_id,user_id,venue,exchange_identity_hash) VALUES($1,$2,$3,$4)",
    )
    .bind(&account)
    .bind(&user)
    .bind(venue.as_str())
    .bind(identity)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO venue_api_credentials(credential_id,user_id,label,key_fingerprint,masked_key,encrypted_credentials,trading_account_id,venue,verification_json,created_ms) VALUES($1,$2,'fixture',$3,'***',decode('00','hex'),$4,$5,$6,1)",
    )
    .bind(&credential)
    .bind(&user)
    .bind(fingerprint)
    .bind(&account)
    .bind(venue.as_str())
    .bind(serde_json::json!({"verification":"verified","strategy_execution":true}))
    .execute(pool)
    .await?;
    Ok((user, account, credential))
}

fn command(
    id: &str,
    venue: VenueId,
    account: &str,
    instance: &str,
    run: &str,
    position_side: PositionSide,
) -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
    let symbol = if venue == VenueId::Hyperliquid {
        Symbol::from_str("BTC/USDC")?
    } else {
        Symbol::from_str("BTC/USDT")?
    };
    let owner = OrderOwner {
        strategy_instance_id: instance.into(),
        run_id: run.into(),
        exchange: venue.as_str().into(),
        account: account.into(),
        symbol,
        purpose: OrderPurpose::Entry,
    };
    Ok(ExecutionCommand::PlaceLimit(OrderCommand {
        command_id: CommandId::new(id)?,
        client_order_id: CommandId::new(format!("client_{id}"))?,
        owner,
        side: OrderSide::Buy,
        position_side,
        quantity: Decimal::ONE,
        limit_price: Price::new(Decimal::new(50_000, 0))?,
        time_in_force: LimitTimeInForce::PostOnly,
        reduce_only: false,
    }))
}

fn cancel(
    id: &str,
    venue: VenueId,
    account: &str,
    instance: &str,
    run: &str,
    target: &str,
) -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
    let symbol = if venue == VenueId::Hyperliquid {
        Symbol::from_str("BTC/USDC")?
    } else {
        Symbol::from_str("BTC/USDT")?
    };
    let owner = OrderOwner {
        strategy_instance_id: instance.into(),
        run_id: run.into(),
        exchange: venue.as_str().into(),
        account: account.into(),
        symbol,
        purpose: OrderPurpose::Entry,
    };
    Ok(ExecutionCommand::Cancel(venue_domain::CancelCommand {
        command_id: CommandId::new(id)?,
        owner,
        target_client_order_id: CommandId::new(target)?,
    }))
}

#[tokio::test]
async fn strategy_ledger_admits_five_venues_net_position_and_payload_idempotency()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let store = MultiVenueStore::new(fixture.pool.clone());
    for (index, venue) in VENUES.into_iter().enumerate() {
        let suffix = format!("{index}x");
        let (user, account, credential) = seed(&fixture.pool, venue, &suffix).await?;
        let command_id = format!("command_{index}");
        let position = if venue == VenueId::Hyperliquid {
            PositionSide::Net
        } else {
            PositionSide::Long
        };
        let inserted_command = command(&command_id, venue, &account, "grid_a", "run_a", position)?;
        assert!(matches!(
            store
                .enqueue(&user, &credential, inserted_command.clone(), 1_000)
                .await?,
            StrategyEnqueueResult::Inserted { .. }
        ));
        assert!(matches!(
            store
                .enqueue(&user, &credential, inserted_command, 1_001)
                .await?,
            StrategyEnqueueResult::Existing { .. }
        ));
        let changed = command(&command_id, venue, &account, "grid_b", "run_a", position)?;
        assert!(
            store
                .enqueue(&user, &credential, changed, 1_002)
                .await
                .is_err()
        );
    }
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn unresolved_sending_and_backoff_fence_pending_and_deleted_credentials()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let (user, account, credential) = seed(&fixture.pool, VenueId::Okx, "fencex").await?;
    let store = MultiVenueStore::new(fixture.pool.clone());
    let first = command(
        "fence_first",
        VenueId::Okx,
        &account,
        "grid_a",
        "run_a",
        PositionSide::Long,
    )?;
    let second = command(
        "fence_second",
        VenueId::Okx,
        &account,
        "grid_a",
        "run_a",
        PositionSide::Long,
    )?;
    store.enqueue(&user, &credential, first, 1_000).await?;
    store.enqueue(&user, &credential, second, 1_001).await?;
    let fresh = store
        .claim(&account, 2_000)
        .await?
        .ok_or_else(|| std::io::Error::other("missing fresh claim"))?;
    assert!(!fresh.reconcile_only);
    let crash_recovery = store
        .claim(&account, 2_001)
        .await?
        .ok_or_else(|| std::io::Error::other("missing crash recovery"))?;
    assert!(crash_recovery.reconcile_only);
    store
        .finish(
            &crash_recovery,
            venue_control_protocol::kol::ExecutorCommandState::ReconcileRequired,
            2_002,
            None,
            None,
        )
        .await?;
    store.backoff(&crash_recovery, 8_000, 2_002).await?;
    assert!(store.claim(&account, 2_003).await?.is_none());
    sqlx::query("UPDATE venue_api_credentials SET deleted_ms=3000 WHERE credential_id=$1")
        .bind(&credential)
        .execute(&fixture.pool)
        .await?;
    let after_deadline = store
        .claim(&account, 8_000)
        .await?
        .ok_or_else(|| std::io::Error::other("deleted credential readback missing"))?;
    assert!(after_deadline.reconcile_only);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn concurrent_claims_send_one_command_once_and_nonce_is_monotonic()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let (user, account, credential) = seed(&fixture.pool, VenueId::Hyperliquid, "nonce_x").await?;
    let (parallel_user, parallel_account, parallel_credential) =
        seed(&fixture.pool, VenueId::Hyperliquid, "nonce_y").await?;
    let (once_user, once_account, once_credential) =
        seed(&fixture.pool, VenueId::Hyperliquid, "nonce_z").await?;
    let store = MultiVenueStore::new(fixture.pool.clone());
    let first = command(
        "nonce_first",
        VenueId::Hyperliquid,
        &account,
        "grid_a",
        "run_a",
        PositionSide::Net,
    )?;
    store.enqueue(&user, &credential, first, 1_000).await?;
    let parallel = command(
        "nonce_parallel",
        VenueId::Hyperliquid,
        &parallel_account,
        "grid_a",
        "run_a",
        PositionSide::Net,
    )?;
    store
        .enqueue(&parallel_user, &parallel_credential, parallel, 1_000)
        .await?;
    let (parallel_claim, first_claim) = tokio::join!(
        store.claim(&parallel_account, 4_000),
        store.claim(&account, 4_000),
    );
    let parallel_claim =
        parallel_claim?.ok_or_else(|| std::io::Error::other("parallel account claim missing"))?;
    assert!(!parallel_claim.reconcile_only);
    let first_claim =
        first_claim?.ok_or_else(|| std::io::Error::other("first account claim missing"))?;
    assert!(!first_claim.reconcile_only);
    let once = command(
        "nonce_once",
        VenueId::Hyperliquid,
        &once_account,
        "grid_a",
        "run_a",
        PositionSide::Net,
    )?;
    store
        .enqueue(&once_user, &once_credential, once, 1_001)
        .await?;
    let (left, right) = tokio::join!(
        store.claim(&once_account, 4_000),
        store.claim(&once_account, 4_000)
    );
    let left = left?.ok_or_else(|| std::io::Error::other("left claim missing"))?;
    let right = right?.ok_or_else(|| std::io::Error::other("right claim missing"))?;
    assert_eq!(left.nonce, right.nonce);
    assert_eq!(
        (if !left.reconcile_only { 1 } else { 0 }) + (if !right.reconcile_only { 1 } else { 0 }),
        1
    );
    store
        .finish(
            &first_claim,
            venue_control_protocol::kol::ExecutorCommandState::Reconciled,
            4_001,
            None,
            None,
        )
        .await?;
    let second = command(
        "nonce_second",
        VenueId::Hyperliquid,
        &account,
        "grid_a",
        "run_a",
        PositionSide::Net,
    )?;
    store.enqueue(&user, &credential, second, 4_002).await?;
    let next = store
        .claim(&account, 4_003)
        .await?
        .ok_or_else(|| std::io::Error::other("second claim missing"))?;
    assert!(next.nonce > first_claim.nonce);
    assert!(next.nonce >= 4_003);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn cancel_scope_and_old_writer_fences_are_enforced() -> Result<(), Box<dyn std::error::Error>>
{
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let (user, account, credential) = seed(&fixture.pool, VenueId::Bitget, "cancelx").await?;
    let store = MultiVenueStore::new(fixture.pool.clone());
    let target = command(
        "cancel_target",
        VenueId::Bitget,
        &account,
        "grid_a",
        "run_a",
        PositionSide::Long,
    )?;
    store.enqueue(&user, &credential, target, 1_000).await?;
    let cross_strategy = cancel(
        "cancel_cross",
        VenueId::Bitget,
        &account,
        "grid_b",
        "run_a",
        "client_cancel_target",
    )?;
    assert!(
        store
            .enqueue(&user, &credential, cross_strategy, 1_001)
            .await
            .is_err()
    );
    let same_strategy = cancel(
        "cancel_same",
        VenueId::Bitget,
        &account,
        "grid_a",
        "run_a",
        "client_cancel_target",
    )?;
    assert!(matches!(
        store
            .enqueue(&user, &credential, same_strategy, 1_001)
            .await?,
        StrategyEnqueueResult::Inserted { .. }
    ));
    assert!(sqlx::query("INSERT INTO venue_control_strategy_scopes(instance_id,venue,mode,trading_account_id,symbol,config_epoch,snapshot_generated_ms) VALUES('legacy','bitget','LIVE',$1,'BTC/USDT',1,1)")
        .bind(&account)
        .execute(&fixture.pool)
        .await
        .is_err());
    let (_, free_account, free_credential) =
        seed(&fixture.pool, VenueId::Bybit, "scope_free").await?;
    sqlx::query(r#"UPDATE venue_api_credentials SET verification_json='{"verification":"verified","strategy_execution":false}'::jsonb WHERE credential_id=$1"#)
        .bind(&free_credential)
        .execute(&fixture.pool)
        .await?;
    sqlx::query("INSERT INTO venue_control_strategy_scopes(instance_id,venue,mode,trading_account_id,symbol,config_epoch,snapshot_generated_ms) VALUES('legacy-free','bybit','LIVE',$1,'BTC/USDT',1,1)")
        .bind(&free_account)
        .execute(&fixture.pool)
        .await?;
    let (blocked_user, blocked_account, blocked_credential) =
        seed(&fixture.pool, VenueId::Gate, "scope_blocked").await?;
    sqlx::query(r#"UPDATE venue_api_credentials SET verification_json='{"verification":"verified","strategy_execution":false}'::jsonb WHERE credential_id=$1"#)
        .bind(&blocked_credential)
        .execute(&fixture.pool)
        .await?;
    sqlx::query("INSERT INTO venue_control_strategy_scopes(instance_id,venue,mode,trading_account_id,symbol,config_epoch,snapshot_generated_ms) VALUES('legacy-blocked','gate','LIVE',$1,'BTC/USDT',1,1)")
        .bind(&blocked_account)
        .execute(&fixture.pool)
        .await?;
    sqlx::query(r#"UPDATE venue_api_credentials SET verification_json='{"verification":"verified","strategy_execution":true}'::jsonb WHERE credential_id=$1"#)
        .bind(&blocked_credential)
        .execute(&fixture.pool)
        .await?;
    let blocked = command(
        "blocked_command",
        VenueId::Gate,
        &blocked_account,
        "grid_a",
        "run_a",
        PositionSide::Long,
    )?;
    assert!(
        store
            .enqueue(&blocked_user, &blocked_credential, blocked, 1_002)
            .await
            .is_err()
    );
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn migration_retains_binance_grid_and_mirror_gtc_predicates()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let rows = sqlx::query(
        "SELECT conname,pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE conrelid='venue_binance_commands'::regclass",
    )
    .fetch_all(&fixture.pool)
    .await?;
    let definitions: Vec<(String, String)> = rows
        .into_iter()
        .map(|row| Ok((row.try_get("conname")?, row.try_get("definition")?)))
        .collect::<Result<_, sqlx::Error>>()?;
    assert!(definitions.iter().any(|(name, definition)| {
        name.contains("grid_fields") && definition.contains("grid_config_revision")
    }));
    assert!(definitions.iter().any(|(name, definition)| {
        name.contains("mirror_kind")
            && definition.contains("limit_gtc")
            && definition.contains("mirror_order_id")
    }));
    assert!(definitions.iter().any(|(name, definition)| {
        name.contains("mirror_shape") && definition.contains("position_side")
    }));
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn same_timestamp_commands_keep_insertion_order_and_cancel_recovers_original_identity()
-> Result<(), Box<dyn std::error::Error>> {
    use venue_control_protocol::kol::ExecutorCommandState;
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let (user, account, credential) = seed(&fixture.pool, VenueId::Bybit, "ordered").await?;
    let store = MultiVenueStore::new(fixture.pool.clone());
    let original = command(
        "z_first",
        VenueId::Bybit,
        &account,
        "grid_a",
        "r1",
        PositionSide::Long,
    )?;
    let second = command(
        "a_second",
        VenueId::Bybit,
        &account,
        "grid_a",
        "r1",
        PositionSide::Long,
    )?;
    store
        .enqueue(&user, &credential, original.clone(), 1_000)
        .await?;
    store.enqueue(&user, &credential, second, 1_000).await?;
    let first = store
        .claim(&account, 2_000)
        .await?
        .ok_or("first claim missing")?;
    assert_eq!(first.command, original);
    store
        .finish(
            &first,
            ExecutorCommandState::Reconciled,
            2_001,
            Some("native-first"),
            None,
        )
        .await?;
    let second = store
        .claim(&account, 2_002)
        .await?
        .ok_or("second claim missing")?;
    assert_eq!(second.command.command_id().as_str(), "a_second");
    store
        .finish(&second, ExecutorCommandState::Rejected, 2_003, None, None)
        .await?;
    let cancel = cancel(
        "c_cancel",
        VenueId::Bybit,
        &account,
        "grid_a",
        "r1",
        "client_z_first",
    )?;
    store.enqueue(&user, &credential, cancel, 2_004).await?;
    let cancellation = store
        .claim(&account, 2_005)
        .await?
        .ok_or("cancel claim missing")?;
    let context = store.execution_context(&cancellation).await?;
    assert_eq!(context.target_command, Some(original));
    assert_eq!(
        context.target_native_order_id.as_deref(),
        Some("native-first")
    );
    let recovered = MultiVenueStore::new(fixture.pool.clone())
        .claim(&account, 2_006)
        .await?
        .ok_or("recovery missing")?;
    assert!(recovered.reconcile_only);
    assert_eq!(store.execution_context(&recovered).await?, context);
    assert!(
        sqlx::query(
            "UPDATE venue_binance_commands SET strategy_sequence=NULL WHERE command_id='z_first'"
        )
        .execute(&fixture.pool)
        .await
        .is_err()
    );
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn five_venues_persist_market_entry_and_each_protection_purpose()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let store = MultiVenueStore::new(fixture.pool.clone());
    for (index, venue) in VENUES.into_iter().enumerate() {
        let (user, account, credential) =
            seed(&fixture.pool, venue, &format!("variants{index}")).await?;
        let position = if venue == VenueId::Hyperliquid {
            PositionSide::Net
        } else {
            PositionSide::Long
        };
        let ExecutionCommand::PlaceLimit(base) = command(
            &format!("base{index}"),
            venue,
            &account,
            "grid_a",
            "r1",
            position,
        )?
        else {
            return Err("invalid fixture".into());
        };
        let market = ExecutionCommand::PlaceMarket(venue_domain::MarketOrderCommand {
            command_id: CommandId::new(format!("market{index}"))?,
            client_order_id: CommandId::new(format!("marketclient{index}"))?,
            owner: base.owner.clone(),
            position_side: position,
            side: OrderSide::Buy,
            quantity: Decimal::ONE,
            reduce_only: false,
        });
        store.enqueue(&user, &credential, market, 1_000).await?;
        for (suffix, purpose) in [
            ("sl", OrderPurpose::Protection),
            ("tp", OrderPurpose::TakeProfit),
        ] {
            let mut owner = base.owner.clone();
            owner.purpose = purpose;
            let protection = ExecutionCommand::StopMarketFullPosition(
                venue_domain::StopMarketFullPositionCommand {
                    command_id: CommandId::new(format!("{suffix}{index}"))?,
                    client_algo_id: CommandId::new(format!("{suffix}client{index}"))?,
                    owner,
                    side: OrderSide::Sell,
                    position_side: position,
                    quantity: Decimal::ONE,
                    trigger_price: Price::new(Decimal::from(50_000))?,
                    position_generation: 1,
                },
            );
            store
                .enqueue(&user, &credential, protection.clone(), 1_001)
                .await?;
            let raw: serde_json::Value = sqlx::query_scalar(
                "SELECT strategy_command FROM venue_binance_commands WHERE command_id=$1",
            )
            .bind(protection.command_id().as_str())
            .fetch_one(&fixture.pool)
            .await?;
            assert_eq!(serde_json::from_value::<ExecutionCommand>(raw)?, protection);
        }
    }
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn support_martingale_create_lifecycle_budget_and_support_identity_are_durable()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let (user, account, credential) =
        seed(&fixture.pool, VenueId::Bybit, "support_martingale").await?;
    let store = SupportMartingaleStore::new(fixture.pool.clone());
    let symbols = vec![
        Symbol::from_str("SOL/USDT")?,
        Symbol::from_str("DOGE/USDT")?,
    ];
    let create = SupportMartingaleCreateRequest {
        schema_version: SUPPORT_MARTINGALE_SCHEMA_VERSION,
        request_id: "qa_create".into(),
        credential_id: credential,
        config: SupportMartingaleConfig {
            reference_venue: VenueId::Binance,
            execution_venue: VenueId::Bybit,
            symbols: symbols.clone(),
            total_budget: Decimal::from(20),
            first_order_notional: Decimal::from(5),
            max_entries: 3,
            size_multiplier: Decimal::ONE,
            target_profit_rate: Decimal::new(5, 3),
            minimum_profit_quote: Decimal::new(2, 2),
            max_active_positions: 2,
        },
    };
    let instance = store.create(&user, create.clone(), 1_000).await?;
    assert_eq!(store.create(&user, create, 1_001).await?, instance);
    let preflight = store.database_preflight(&user, &instance, 1).await?;
    assert!(preflight.credential_verified);
    assert!(preflight.account_exclusive);
    let revision = store
        .lifecycle(
            &user,
            SupportMartingaleLifecycleRequest {
                schema_version: SUPPORT_MARTINGALE_SCHEMA_VERSION,
                request_id: "qa_start".into(),
                instance_id: instance.clone(),
                expected_revision: 1,
                action: SupportMartingaleAction::Start,
            },
            1_002,
        )
        .await?;
    assert_eq!(revision, 2);
    store
        .sync_symbol_facts(
            &user,
            &instance,
            &symbols[0],
            Decimal::ZERO,
            None,
            Decimal::ZERO,
            None,
            Some(Decimal::ZERO),
            1_003,
        )
        .await?;
    let initial_runtime = store.load_runtime_state(&user, &instance).await?;
    assert_eq!(initial_runtime.symbols[0].cooldown_until_ms, None);
    assert_eq!(store.get(&user, &instance).await?.symbols[0].status, "idle");
    let owner = OrderOwner {
        strategy_instance_id: instance.clone(),
        run_id: "cycle_1".into(),
        exchange: VenueId::Bybit.as_str().into(),
        account,
        symbol: symbols[0].clone(),
        purpose: OrderPurpose::Entry,
    };
    let entry = ExecutionCommand::PlaceMarket(venue_domain::MarketOrderCommand {
        command_id: CommandId::new("support_entry_1")?,
        client_order_id: CommandId::new("support_entry_1")?,
        owner: owner.clone(),
        position_side: PositionSide::Long,
        side: OrderSide::Buy,
        quantity: Decimal::ONE,
        reduce_only: false,
    });
    assert!(matches!(
        store
            .enqueue_command(
                &user,
                &instance,
                &symbols[0],
                SupportMartingaleCommandKind::Entry,
                Some("cycle_1"),
                Some("support_1"),
                Some((Decimal::from(90), Decimal::from(91))),
                "support_entry_1",
                Decimal::from(5),
                entry,
                1_004,
            )
            .await?,
        StrategyEnqueueResult::Inserted { .. }
    ));
    assert_eq!(
        store.get(&user, &instance).await?.reserved_budget,
        Decimal::from(5)
    );
    store
        .settle_command(&user, "support_entry_1", Decimal::ZERO, true, 1_005)
        .await?;
    assert_eq!(
        store.get(&user, &instance).await?.reserved_budget,
        Decimal::ZERO
    );
    let retry = ExecutionCommand::PlaceMarket(venue_domain::MarketOrderCommand {
        command_id: CommandId::new("support_entry_2")?,
        client_order_id: CommandId::new("support_entry_2")?,
        owner,
        position_side: PositionSide::Long,
        side: OrderSide::Buy,
        quantity: Decimal::ONE,
        reduce_only: false,
    });
    assert!(
        store
            .enqueue_command(
                &user,
                &instance,
                &symbols[0],
                SupportMartingaleCommandKind::Entry,
                Some("cycle_1"),
                Some("support_1"),
                Some((Decimal::from(90), Decimal::from(91))),
                "support_entry_2",
                Decimal::from(5),
                retry,
                1_006,
            )
            .await
            .is_err()
    );
    fixture.cleanup().await?;
    Ok(())
}
