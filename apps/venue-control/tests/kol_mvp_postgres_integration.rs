use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::{Executor, PgPool, postgres::PgPoolOptions};
use venue_control::{
    BinanceExecutorSingleton, ExecutorSingletonError, MIGRATION_0017, accounts::MIGRATION_0015,
};
use venue_control::{
    KolSourceFill,
    executor_exchange::{ExecutionReadback, MockBinanceExecution},
    executor_runtime::BinanceExecutorRuntime,
    executor_store::PgExecutorStore,
};
use venue_control::{
    accounts::CredentialCipher,
    executor_secret::{ExecutorSecretError, ExecutorSecretProvider},
};
use venue_control_protocol::accounts::{BindCredentialRequest, SecretValue};
use venue_control_protocol::kol::ExecutorCommandState;
use venue_domain::domain::{OrderSide, PositionSide};

#[tokio::test]
async fn kol_mvp_migration_is_idempotent_and_enforces_capacity_and_ownership()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&database_url).await?;
    fixture.migrate_twice().await?;
    let singleton = BinanceExecutorSingleton::acquire(&database_url).await?;
    assert!(matches!(
        BinanceExecutorSingleton::acquire(&database_url).await,
        Err(ExecutorSingletonError::AlreadyRunning)
    ));
    singleton.release().await?;

    for slot in 1_i16..=5 {
        let user_id = id(100 + i64::from(slot));
        let account_id = id(200 + i64::from(slot));
        let credential_id = id(300 + i64::from(slot));
        seed_verified_account(
            &fixture.pool,
            &user_id,
            &account_id,
            &credential_id,
            slot as u8,
        )
        .await?;
        insert_kol_profile(&fixture.pool, &user_id, &account_id, slot).await?;
    }

    let sixth_user = id(106);
    let sixth_account = id(206);
    let sixth_credential = id(306);
    seed_verified_account(
        &fixture.pool,
        &sixth_user,
        &sixth_account,
        &sixth_credential,
        6,
    )
    .await?;
    assert!(
        insert_kol_profile(&fixture.pool, &sixth_user, &sixth_account, 6)
            .await
            .is_err()
    );

    let first_kol = id(101);
    let second_kol = id(102);
    let first_invite = id(401);
    let second_invite = id(402);
    insert_invite(&fixture.pool, &first_invite, &first_kol, 41).await?;
    insert_invite(&fixture.pool, &second_invite, &second_kol, 42).await?;
    assert!(
        insert_invite(&fixture.pool, &id(403), &first_kol, 43)
            .await
            .is_err()
    );

    let follower = id(501);
    let follower_account = id(601);
    let follower_credential = id(701);
    seed_verified_account(
        &fixture.pool,
        &follower,
        &follower_account,
        &follower_credential,
        51,
    )
    .await?;
    sqlx::query(
        "INSERT INTO venue_user_kol_bindings (user_id,kol_user_id,invite_id,bound_ms) \
         VALUES ($1,$2,$3,100)",
    )
    .bind(&follower)
    .bind(&first_kol)
    .bind(&first_invite)
    .execute(&fixture.pool)
    .await?;
    assert!(
        sqlx::query(
            "UPDATE venue_user_kol_bindings SET kol_user_id=$1,invite_id=$2 WHERE user_id=$3",
        )
        .bind(&second_kol)
        .bind(&second_invite)
        .bind(&follower)
        .execute(&fixture.pool)
        .await
        .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM venue_user_kol_bindings WHERE user_id=$1")
            .bind(&follower)
            .execute(&fixture.pool)
            .await
            .is_err()
    );

    insert_follow_relation(
        &fixture.pool,
        &id(801),
        &follower,
        &first_kol,
        &id(201),
        &follower_account,
        &follower_credential,
        1,
    )
    .await?;
    let other_follower = id(502);
    let other_account = id(602);
    let other_credential = id(702);
    seed_verified_account(
        &fixture.pool,
        &other_follower,
        &other_account,
        &other_credential,
        52,
    )
    .await?;
    sqlx::query(
        "INSERT INTO venue_user_kol_bindings (user_id,kol_user_id,invite_id,bound_ms) \
         VALUES ($1,$2,$3,100)",
    )
    .bind(&other_follower)
    .bind(&first_kol)
    .bind(&first_invite)
    .execute(&fixture.pool)
    .await?;
    assert!(
        insert_follow_relation(
            &fixture.pool,
            &id(802),
            &other_follower,
            &first_kol,
            &id(201),
            &other_account,
            &other_credential,
            201,
        )
        .await
        .is_err()
    );

    insert_terminal_command(
        &fixture.pool,
        &id(901),
        &id(902),
        &follower,
        &follower_account,
        &follower_credential,
        "pending",
    )
    .await?;
    assert!(
        insert_terminal_command(
            &fixture.pool,
            &id(903),
            &id(902),
            &follower,
            &follower_account,
            &follower_credential,
            "pending",
        )
        .await
        .is_err()
    );
    assert!(
        insert_terminal_command(
            &fixture.pool,
            &id(904),
            &id(905),
            &follower,
            &follower_account,
            &follower_credential,
            "unknown",
        )
        .await
        .is_err()
    );

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn executor_store_deduplicates_source_fills_and_recovers_only_nonterminal_commands()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&database_url).await?;
    fixture.migrate_twice().await?;
    let kol = id(110);
    let leader = id(210);
    let credential = id(310);
    seed_verified_account(&fixture.pool, &kol, &leader, &credential, 110).await?;
    insert_kol_profile(&fixture.pool, &kol, &leader, 1).await?;
    let fill = KolSourceFill {
        leader_trading_account_id: leader.clone(),
        native_symbol: "BTCUSDT".into(),
        native_trade_id: "native-1".into(),
        symbol: "BTC/USDT".into(),
        order_side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::new(1, 3),
        price: Decimal::new(100_000, 0),
        occurred_ms: 10,
        observed_ms: 11,
        payload_digest: [7; 32],
    };
    let store = PgExecutorStore::new(fixture.pool.clone());
    assert!(store.record_source_fill(&kol, &fill).await?);
    assert!(!store.record_source_fill(&kol, &fill).await?);
    insert_terminal_command(
        &fixture.pool,
        &id(910),
        &id(911),
        &kol,
        &leader,
        &credential,
        "sending",
    )
    .await?;
    let recovered = store.recover_nonterminal().await?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].client_order_id, id(910));
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn source_fill_planning_is_deduplicated_and_uses_a_stable_command_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&database_url).await?;
    fixture.migrate_twice().await?;
    let kol = id(150);
    let leader = id(250);
    let kol_credential = id(350);
    let follower = id(151);
    let follower_account = id(251);
    let follower_credential = id(351);
    let invite = id(450);
    let relation = id(850);
    seed_verified_account(&fixture.pool, &kol, &leader, &kol_credential, 150).await?;
    seed_verified_account(
        &fixture.pool,
        &follower,
        &follower_account,
        &follower_credential,
        151,
    )
    .await?;
    insert_kol_profile(&fixture.pool, &kol, &leader, 1).await?;
    insert_invite(&fixture.pool, &invite, &kol, 150).await?;
    sqlx::query("INSERT INTO venue_user_kol_bindings (user_id,kol_user_id,invite_id,bound_ms) VALUES ($1,$2,$3,1)")
        .bind(&follower).bind(&kol).bind(&invite).execute(&fixture.pool).await?;
    insert_follow_relation(
        &fixture.pool,
        &relation,
        &follower,
        &kol,
        &leader,
        &follower_account,
        &follower_credential,
        1,
    )
    .await?;
    let fill = KolSourceFill {
        leader_trading_account_id: leader,
        native_symbol: "BTCUSDT".into(),
        native_trade_id: "trade-1".into(),
        symbol: "BTC/USDT".into(),
        order_side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::new(2, 3),
        price: Decimal::new(100_000, 0),
        occurred_ms: 10,
        observed_ms: 11,
        payload_digest: [8; 32],
    };
    let store = PgExecutorStore::new(fixture.pool.clone());
    let planned = store.record_source_fill_and_plan(&kol, &fill, 12).await?;
    assert_eq!(planned.len(), 1);
    assert!(planned[0].client_order_id.starts_with('k'));
    assert!(planned[0].client_order_id.len() <= 36);
    assert!(
        store
            .record_source_fill_and_plan(&kol, &fill, 13)
            .await?
            .is_empty()
    );
    let state: String =
        sqlx::query_scalar("SELECT command_state FROM venue_binance_commands WHERE command_id=$1")
            .bind(&planned[0].command_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(state, "pending");
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn runtime_reconciles_mocked_restart_timeout_and_rejection_without_reposting()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&database_url).await?;
    fixture.migrate_twice().await?;
    let user = id(160);
    let account = id(260);
    let credential = id(360);
    let reconciled = id(960);
    let unknown = id(961);
    let rejected = id(962);
    seed_verified_account(&fixture.pool, &user, &account, &credential, 160).await?;
    for (command, request_id) in [
        (&reconciled, id(970)),
        (&unknown, id(971)),
        (&rejected, id(972)),
    ] {
        insert_terminal_command(
            &fixture.pool,
            command,
            &request_id,
            &user,
            &account,
            &credential,
            "pending",
        )
        .await?;
    }
    let cipher = CredentialCipher::from_key(&[42; 32])?;
    let payload = serde_json::to_vec(&BindCredentialRequest {
        label: "offline".into(),
        api_key: SecretValue::new("a".repeat(32)),
        api_secret: SecretValue::new("b".repeat(32)),
    })?;
    let encrypted = cipher.encrypt(&format!("venue-api-v1:{user}:{credential}"), &payload)?;
    sqlx::query("UPDATE venue_api_credentials SET encrypted_credentials=$1 WHERE credential_id=$2")
        .bind(encrypted)
        .bind(&credential)
        .execute(&fixture.pool)
        .await?;
    let mut exchange = MockBinanceExecution::default();
    exchange.set_readback(reconciled.clone(), ExecutionReadback::Reconciled);
    exchange.set_readback(unknown.clone(), ExecutionReadback::Unknown);
    exchange.set_readback(rejected.clone(), ExecutionReadback::Rejected);
    let store = PgExecutorStore::new(fixture.pool.clone());
    let secrets = ExecutorSecretProvider::new(fixture.pool.clone(), cipher);
    let mut runtime = BinanceExecutorRuntime::new(store, exchange, secrets);
    assert_eq!(runtime.recover_once().await?, 1);
    assert_eq!(
        command_state(&fixture.pool, &reconciled).await?,
        "reconciled"
    );
    assert_eq!(runtime.recover_once().await?, 1);
    assert_eq!(
        command_state(&fixture.pool, &unknown).await?,
        "reconcile_required"
    );
    assert_eq!(runtime.recover_once().await?, 1);
    assert_eq!(
        command_state(&fixture.pool, &unknown).await?,
        "reconcile_required"
    );
    PgExecutorStore::new(fixture.pool.clone())
        .transition_command(&unknown, ExecutorCommandState::Reconciled, 100, None)
        .await?;
    assert_eq!(runtime.recover_once().await?, 1);
    assert_eq!(command_state(&fixture.pool, &rejected).await?, "rejected");
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn runtime_only_activates_after_two_clean_mocked_signed_baselines()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&database_url).await?;
    fixture.migrate_twice().await?;
    let kol = id(170);
    let leader = id(270);
    let kol_credential = id(370);
    let follower = id(171);
    let follower_account = id(271);
    let follower_credential = id(371);
    let invite = id(470);
    let relation = id(870);
    seed_verified_account(&fixture.pool, &kol, &leader, &kol_credential, 170).await?;
    seed_verified_account(
        &fixture.pool,
        &follower,
        &follower_account,
        &follower_credential,
        171,
    )
    .await?;
    let cipher = CredentialCipher::from_key(&[42; 32])?;
    let payload = serde_json::to_vec(&BindCredentialRequest {
        label: "offline".into(),
        api_key: SecretValue::new("a".repeat(32)),
        api_secret: SecretValue::new("b".repeat(32)),
    })?;
    for (owner, credential) in [(&kol, &kol_credential), (&follower, &follower_credential)] {
        let encrypted = cipher.encrypt(&format!("venue-api-v1:{owner}:{credential}"), &payload)?;
        sqlx::query("UPDATE venue_api_credentials SET encrypted_credentials=$1,verification_json='{\"verification\":\"verified\",\"expires_ms\":9999999999999}'::jsonb WHERE credential_id=$2")
            .bind(encrypted).bind(credential).execute(&fixture.pool).await?;
    }
    insert_kol_profile(&fixture.pool, &kol, &leader, 1).await?;
    insert_invite(&fixture.pool, &invite, &kol, 170).await?;
    sqlx::query("INSERT INTO venue_user_kol_bindings (user_id,kol_user_id,invite_id,bound_ms) VALUES ($1,$2,$3,1)")
        .bind(&follower).bind(&kol).bind(&invite).execute(&fixture.pool).await?;
    sqlx::query("INSERT INTO venue_kol_follow_relations (relation_id,follower_user_id,kol_user_id,leader_trading_account_id,follower_trading_account_id,credential_id,relation_state,allocated_capital,multiplier,max_order_notional,max_total_notional,max_deviation_bps,allowed_symbols,revision,created_ms,updated_ms) VALUES ($1,$2,$3,$4,$5,$6,'paused','100','1','20','100',100,'[\"BTC/USDT\"]'::jsonb,1,1,1)")
        .bind(&relation).bind(&follower).bind(&kol).bind(&leader).bind(&follower_account).bind(&follower_credential).execute(&fixture.pool).await?;
    sqlx::query("INSERT INTO venue_kol_activation_requests (relation_id,request_id,relation_revision,request_state,requested_ms,updated_ms) VALUES ($1,$2,1,'pending',1,1)")
        .bind(&relation).bind(id(970)).execute(&fixture.pool).await?;
    let store = PgExecutorStore::new(fixture.pool.clone());
    let secrets = ExecutorSecretProvider::new(fixture.pool.clone(), cipher);
    let mut runtime = BinanceExecutorRuntime::new(store, MockBinanceExecution::default(), secrets);
    assert_eq!(runtime.recover_once().await?, 0);
    assert_eq!(
        command_state_value(
            &fixture.pool,
            "SELECT relation_state FROM venue_kol_follow_relations WHERE relation_id=$1",
            &relation
        )
        .await?,
        "active"
    );
    assert_eq!(
        command_state_value(
            &fixture.pool,
            "SELECT request_state FROM venue_kol_activation_requests WHERE relation_id=$1",
            &relation
        )
        .await?,
        "completed"
    );
    fixture.cleanup().await?;
    Ok(())
}

async fn command_state(pool: &PgPool, command_id: &str) -> Result<String, sqlx::Error> {
    sqlx::query_scalar("SELECT command_state FROM venue_binance_commands WHERE command_id=$1")
        .bind(command_id)
        .fetch_one(pool)
        .await
}

async fn command_state_value(pool: &PgPool, query: &str, id: &str) -> Result<String, sqlx::Error> {
    sqlx::query_scalar(query).bind(id).fetch_one(pool).await
}

#[tokio::test]
async fn executor_secret_provider_requires_the_durable_credential_owner()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&database_url).await?;
    fixture.migrate_twice().await?;
    let user = id(120);
    let account = id(220);
    let credential = id(320);
    seed_verified_account(&fixture.pool, &user, &account, &credential, 120).await?;
    let cipher = CredentialCipher::from_key(&[42; 32])?;
    let payload = serde_json::to_vec(&BindCredentialRequest {
        label: "offline".into(),
        api_key: SecretValue::new("a".repeat(32)),
        api_secret: SecretValue::new("b".repeat(32)),
    })?;
    let encrypted = cipher.encrypt(&format!("venue-api-v1:{user}:{credential}"), &payload)?;
    sqlx::query("UPDATE venue_api_credentials SET encrypted_credentials=$1 WHERE credential_id=$2")
        .bind(encrypted)
        .bind(&credential)
        .execute(&fixture.pool)
        .await?;
    let provider = ExecutorSecretProvider::new(fixture.pool.clone(), cipher);
    let credentials = provider.load(&credential, &user).await?;
    drop(credentials);
    assert_eq!(
        provider.load(&credential, &id(121)).await.err(),
        Some(ExecutorSecretError::Forbidden)
    );
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn executor_store_claim_is_atomic_and_transitions_only_forward()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&database_url).await?;
    fixture.migrate_twice().await?;
    let user = id(140);
    let account = id(240);
    let credential = id(340);
    let command = id(940);
    seed_verified_account(&fixture.pool, &user, &account, &credential, 140).await?;
    insert_terminal_command(
        &fixture.pool,
        &command,
        &id(941),
        &user,
        &account,
        &credential,
        "pending",
    )
    .await?;
    let store = PgExecutorStore::new(fixture.pool.clone());
    let left = store.clone();
    let right = store.clone();
    let (first, second) = tokio::join!(
        left.claim_next_command(&account, 2),
        right.claim_next_command(&account, 2)
    );
    assert_eq!(
        usize::from(first?.is_some()) + usize::from(second?.is_some()),
        1
    );
    assert!(
        store
            .transition_command(&command, ExecutorCommandState::Reconciled, 3, None)
            .await
            .is_err()
    );
    assert!(
        store
            .transition_command(&command, ExecutorCommandState::Pending, 3, None)
            .await
            .is_err()
    );
    store
        .transition_command(&command, ExecutorCommandState::Accepted, 3, None)
        .await?;
    store
        .transition_command(&command, ExecutorCommandState::Reconciled, 4, None)
        .await?;
    assert!(store.recover_nonterminal().await?.is_empty());
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn executor_store_promotes_only_the_matching_pending_activation()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&database_url).await?;
    fixture.migrate_twice().await?;
    let kol = id(130);
    let leader = id(230);
    let kol_credential = id(330);
    let follower = id(131);
    let follower_account = id(231);
    let follower_credential = id(331);
    let invite = id(430);
    let relation = id(830);
    seed_verified_account(&fixture.pool, &kol, &leader, &kol_credential, 130).await?;
    seed_verified_account(
        &fixture.pool,
        &follower,
        &follower_account,
        &follower_credential,
        131,
    )
    .await?;
    insert_kol_profile(&fixture.pool, &kol, &leader, 1).await?;
    insert_invite(&fixture.pool, &invite, &kol, 130).await?;
    sqlx::query("INSERT INTO venue_user_kol_bindings (user_id,kol_user_id,invite_id,bound_ms) VALUES ($1,$2,$3,1)")
        .bind(&follower).bind(&kol).bind(&invite).execute(&fixture.pool).await?;
    sqlx::query("INSERT INTO venue_kol_follow_relations (relation_id,follower_user_id,kol_user_id,leader_trading_account_id,follower_trading_account_id,credential_id,relation_state,allocated_capital,multiplier,max_order_notional,max_total_notional,max_deviation_bps,allowed_symbols,revision,created_ms,updated_ms) VALUES ($1,$2,$3,$4,$5,$6,'paused','100','1','20','100',100,'[\"BTC/USDT\"]'::jsonb,1,1,1)")
        .bind(&relation).bind(&follower).bind(&kol).bind(&leader).bind(&follower_account).bind(&follower_credential).execute(&fixture.pool).await?;
    sqlx::query("INSERT INTO venue_kol_activation_requests (relation_id,request_id,relation_revision,request_state,requested_ms,updated_ms) VALUES ($1,$2,1,'pending',1,1)")
        .bind(&relation).bind(id(930)).execute(&fixture.pool).await?;
    let store = PgExecutorStore::new(fixture.pool.clone());
    assert!(store.complete_activation(&relation, 2, 2).await.is_err());
    let before: String = sqlx::query_scalar(
        "SELECT relation_state FROM venue_kol_follow_relations WHERE relation_id=$1",
    )
    .bind(&relation)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(before, "paused");
    store.complete_activation(&relation, 1, 2).await?;
    let state: String = sqlx::query_scalar(
        "SELECT relation_state FROM venue_kol_follow_relations WHERE relation_id=$1",
    )
    .bind(&relation)
    .fetch_one(&fixture.pool)
    .await?;
    let request: String = sqlx::query_scalar(
        "SELECT request_state FROM venue_kol_activation_requests WHERE relation_id=$1",
    )
    .bind(&relation)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(state, "active");
    assert_eq!(request, "completed");
    fixture.cleanup().await?;
    Ok(())
}

fn integration_database_url() -> Result<Option<String>, Box<dyn std::error::Error>> {
    match std::env::var("VENUE_CONTROL_TEST_DATABASE_URL") {
        Ok(value) => Ok(Some(value)),
        Err(_)
            if std::env::var("VENUE_CONTROL_POSTGRES_REQUIRED")
                .ok()
                .as_deref()
                == Some("1") =>
        {
            Err("KOL MVP PostgreSQL test database is required".into())
        }
        Err(_) => {
            eprintln!("SKIP: KOL MVP PostgreSQL test database is not configured");
            Ok(None)
        }
    }
}

struct Fixture {
    pool: PgPool,
    admin: PgPool,
    schema: String,
}

impl Fixture {
    async fn create(database_url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(database_url)
            .await?;
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let schema = format!("venue_kol_mvp_{}_{}", std::process::id(), nonce);
        admin
            .execute(format!("CREATE SCHEMA {schema}").as_str())
            .await?;
        let search_path = schema.clone();
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .after_connect(move |connection, _| {
                let sql = format!("SET search_path TO {search_path}");
                Box::pin(async move {
                    connection.execute(sql.as_str()).await?;
                    Ok(())
                })
            })
            .connect(database_url)
            .await?;
        Ok(Self {
            pool,
            admin,
            schema,
        })
    }

    async fn migrate_twice(&self) -> Result<(), sqlx::Error> {
        for _ in 0..2 {
            sqlx::raw_sql(MIGRATION_0015).execute(&self.pool).await?;
            sqlx::raw_sql(MIGRATION_0017).execute(&self.pool).await?;
        }
        Ok(())
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

async fn seed_verified_account(
    pool: &PgPool,
    user_id: &str,
    account_id: &str,
    credential_id: &str,
    seed: u8,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO venue_users (user_id,username,password_hash,created_ms) VALUES ($1,$2,'test',1)")
        .bind(user_id)
        .bind(format!("user-{seed}"))
        .execute(pool)
        .await?;
    sqlx::query("INSERT INTO venue_user_trading_accounts (trading_account_id,user_id,venue,exchange_identity_hash) VALUES ($1,$2,'binance',$3)")
        .bind(account_id)
        .bind(user_id)
        .bind(vec![seed; 32])
        .execute(pool)
        .await?;
    sqlx::query("INSERT INTO venue_api_credentials (credential_id,user_id,label,key_fingerprint,masked_key,encrypted_credentials,trading_account_id,verification_json,created_ms) VALUES ($1,$2,'test',$3,'***',decode('00','hex'),$4,'{}'::jsonb,1)")
        .bind(credential_id)
        .bind(user_id)
        .bind(vec![seed.wrapping_add(100); 32])
        .bind(account_id)
        .execute(pool)
        .await?;
    Ok(())
}

async fn insert_kol_profile(
    pool: &PgPool,
    user_id: &str,
    account_id: &str,
    slot: i16,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO venue_kol_profiles (kol_user_id,leader_trading_account_id,public_name,public_title,public_description,strategy_capital,profile_state,active_slot,created_ms,updated_ms) VALUES ($1,$2,'KOL','title','','1000','enabled',$3,1,1)")
        .bind(user_id)
        .bind(account_id)
        .bind(slot)
        .execute(pool)
        .await?;
    Ok(())
}

async fn insert_invite(
    pool: &PgPool,
    invite_id: &str,
    kol_user_id: &str,
    seed: u8,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO venue_kol_invites (invite_id,kol_user_id,code_hash,invite_state,created_ms) VALUES ($1,$2,$3,'active',1)")
        .bind(invite_id)
        .bind(kol_user_id)
        .bind(vec![seed; 32])
        .execute(pool)
        .await?;
    Ok(())
}

async fn insert_follow_relation(
    pool: &PgPool,
    relation_id: &str,
    follower_user_id: &str,
    kol_user_id: &str,
    leader_account_id: &str,
    account_id: &str,
    credential_id: &str,
    active_slot: i16,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO venue_kol_follow_relations (relation_id,follower_user_id,kol_user_id,leader_trading_account_id,follower_trading_account_id,credential_id,relation_state,active_slot,allocated_capital,multiplier,max_order_notional,max_total_notional,max_deviation_bps,allowed_symbols,revision,created_ms,updated_ms) VALUES ($1,$2,$3,$4,$5,$6,'active',$7,'1000','1','100','1000',100,'[\"BTC/USDT\"]'::jsonb,1,1,1)")
        .bind(relation_id)
        .bind(follower_user_id)
        .bind(kol_user_id)
        .bind(leader_account_id)
        .bind(account_id)
        .bind(credential_id)
        .bind(active_slot)
        .execute(pool)
        .await?;
    Ok(())
}

async fn insert_terminal_command(
    pool: &PgPool,
    command_id: &str,
    request_id: &str,
    owner_user_id: &str,
    account_id: &str,
    credential_id: &str,
    state: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO venue_binance_commands (command_id,command_origin,request_id,owner_user_id,trading_account_id,credential_id,symbol,position_side,command_phase,order_kind,order_side,requested_quantity,rule_version,client_order_id,command_state,created_ms,updated_ms) VALUES ($1,'terminal',$2,$3,$4,$5,'BTC/USDT','long','open','market','buy','0.001','fixture',$6,$7,1,1)")
        .bind(command_id)
        .bind(request_id)
        .bind(owner_user_id)
        .bind(account_id)
        .bind(credential_id)
        .bind(command_id)
        .bind(state)
        .execute(pool)
        .await?;
    Ok(())
}

fn id(value: i64) -> String {
    format!("00000000-0000-4000-8000-{value:012}")
}
