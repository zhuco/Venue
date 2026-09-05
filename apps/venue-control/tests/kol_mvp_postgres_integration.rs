use std::time::{SystemTime, UNIX_EPOCH};

#[path = "support/kol_activation.rs"]
mod kol_activation;
#[path = "support/kol_continuous_dispatch.rs"]
mod kol_continuous_dispatch;
#[path = "support/kol_copy_convergence.rs"]
mod kol_copy_convergence;
#[path = "support/kol_copy_lifecycle.rs"]
mod kol_copy_lifecycle;
#[path = "support/leader_order_mirror.rs"]
mod leader_order_mirror;

use rust_decimal::Decimal;
use sqlx::{Executor, PgPool, postgres::PgPoolOptions};
use venue_control::accounts::AccountService;
use venue_control::{
    BinanceExecutorSingleton, ExecutorSingletonError, MIGRATION_0001, MIGRATION_0017,
    MIGRATION_0018, MIGRATION_0019, MIGRATION_0020, MIGRATION_0021, MIGRATION_0022, MIGRATION_0023,
    MIGRATION_0024, accounts::MIGRATION_0015,
};
use venue_control::{
    KolSourceFill,
    executor_exchange::{ExecutionReadback, MockBinanceExecution},
    executor_runtime::BinanceExecutorRuntime,
    executor_store::PgExecutorStore,
    private_projection::{
        ActiveProjectionSource, BinancePrivateProjectionStore, PrivateProjectionError,
    },
};
use venue_control::{
    accounts::CredentialCipher,
    executor_secret::{ExecutorSecretError, ExecutorSecretProvider},
};
use venue_control_protocol::accounts::{
    AccountErrorCode, BindCredentialRequest, LoginRequest, SecretValue,
};
use venue_control_protocol::kol::{
    ExecutorCommandState, ExecutorOrderKind, TERMINAL_SCHEMA_VERSION, TerminalAction,
    TerminalOrderKind, TerminalOrderRequest,
};
use venue_domain::domain::{Asset, FieldState, Fill, OrderSide, OrderState, PositionSide, Price};
use venue_execution::{
    SignedAccountBalance, SignedAccountPositionFact, SignedAccountPositionMode,
    SignedAccountSnapshot,
};
use venue_gateway_binance::{BinancePrivateFillEvent, GatewayBinding, GatewayMode, VenueId};

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
async fn reconcile_backoff_migration_safely_backfills_and_preserves_existing_progress()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&database_url).await?;
    fixture.migrate_through_0021().await?;
    let user = id(108);
    let account = id(208);
    let credential = id(308);
    let unresolved = id(908);
    let terminal = id(909);
    seed_verified_account(&fixture.pool, &user, &account, &credential, 108).await?;
    insert_terminal_command(
        &fixture.pool,
        &unresolved,
        &id(918),
        &user,
        &account,
        &credential,
        "reconcile_required",
    )
    .await?;
    insert_terminal_command(
        &fixture.pool,
        &terminal,
        &id(919),
        &user,
        &account,
        &credential,
        "reconciled",
    )
    .await?;

    sqlx::raw_sql(MIGRATION_0022).execute(&fixture.pool).await?;
    let backfilled: (i32, Option<i64>) = sqlx::query_as(
        "SELECT reconcile_attempts,next_reconcile_ms FROM venue_binance_commands WHERE command_id=$1",
    )
    .bind(&unresolved)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(backfilled.0, 0);
    assert!(backfilled.1.is_some_and(|deadline| deadline >= 501));
    let terminal_schedule: (i32, Option<i64>) = sqlx::query_as(
        "SELECT reconcile_attempts,next_reconcile_ms FROM venue_binance_commands WHERE command_id=$1",
    )
    .bind(&terminal)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(terminal_schedule, (0, None));

    sqlx::query("UPDATE venue_binance_commands SET reconcile_attempts=3,next_reconcile_ms=9000 WHERE command_id=$1")
        .bind(&unresolved)
        .execute(&fixture.pool)
        .await?;
    sqlx::raw_sql(MIGRATION_0022).execute(&fixture.pool).await?;
    let preserved: (i32, Option<i64>) = sqlx::query_as(
        "SELECT reconcile_attempts,next_reconcile_ms FROM venue_binance_commands WHERE command_id=$1",
    )
    .bind(&unresolved)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(preserved, (3, Some(9000)));
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
    let rejected_after_unknown_dispatch = id(963);
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
    sqlx::query("UPDATE venue_binance_commands SET order_kind='limit_post_only',limit_price='50000' WHERE command_id=$1")
        .bind(&reconciled)
        .execute(&fixture.pool)
        .await?;
    let cipher = CredentialCipher::from_key(&[42; 32])?;
    let payload = serde_json::to_vec(&BindCredentialRequest {
        label: "offline".into(),
        api_key: SecretValue::new("a".repeat(32)),
        api_secret: SecretValue::new("b".repeat(32)),
    })?;
    let encrypted = cipher.encrypt(&format!("venue-api-v1:{user}:{credential}"), &payload)?;
    sqlx::query("UPDATE venue_api_credentials SET encrypted_credentials=$1,verification_json='{\"verification\":\"verified\"}'::jsonb WHERE credential_id=$2")
        .bind(encrypted)
        .bind(&credential)
        .execute(&fixture.pool)
        .await?;
    let mut exchange = MockBinanceExecution::default();
    // A signed readback of a resting post-only order completes the placement command. The
    // private projection owns its later fill/cancel lifecycle, so the next account command can run.
    exchange.set_readback(reconciled.clone(), ExecutionReadback::Accepted);
    exchange.set_readback(unknown.clone(), ExecutionReadback::Unknown);
    exchange.set_rejection(rejected.clone(), -4164);
    exchange.set_readback(
        rejected_after_unknown_dispatch.clone(),
        ExecutionReadback::Rejected,
    );
    let store = PgExecutorStore::new(fixture.pool.clone());
    let secrets = ExecutorSecretProvider::new(fixture.pool.clone(), cipher);
    let mut runtime = BinanceExecutorRuntime::new(store, exchange, secrets);
    // The account drain continues after a terminal placement, then stops at the first Unknown.
    assert_eq!(runtime.recover_once().await?, 2);
    assert_eq!(
        command_state(&fixture.pool, &reconciled).await?,
        "reconciled"
    );
    assert_eq!(
        command_state(&fixture.pool, &unknown).await?,
        "reconcile_required"
    );
    assert_eq!(command_state(&fixture.pool, &rejected).await?, "pending");
    let initial_schedule: (i32, i64) = sqlx::query_as(
        "SELECT reconcile_attempts,next_reconcile_ms FROM venue_binance_commands WHERE command_id=$1",
    )
    .bind(&unknown)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(initial_schedule.0, 0);
    assert!(initial_schedule.1 > 0);
    // Discovery still sees the unresolved row and therefore fences this account, but it performs
    // no signed read before the durable deadline.
    assert_eq!(runtime.recover_once().await?, 0);
    assert_eq!(
        command_state(&fixture.pool, &unknown).await?,
        "reconcile_required"
    );
    sqlx::query("UPDATE venue_binance_commands SET next_reconcile_ms=1 WHERE command_id=$1")
        .bind(&unknown)
        .execute(&fixture.pool)
        .await?;
    assert_eq!(runtime.recover_once().await?, 1);
    let retried_schedule: (i32, i64) = sqlx::query_as(
        "SELECT reconcile_attempts,next_reconcile_ms FROM venue_binance_commands WHERE command_id=$1",
    )
    .bind(&unknown)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(retried_schedule.0, 1);
    assert!(retried_schedule.1 > initial_schedule.1);
    PgExecutorStore::new(fixture.pool.clone())
        .transition_command(&unknown, ExecutorCommandState::Reconciled, 100, None)
        .await?;
    let terminal_schedule: (i32, Option<i64>) = sqlx::query_as(
        "SELECT reconcile_attempts,next_reconcile_ms FROM venue_binance_commands WHERE command_id=$1",
    )
    .bind(&unknown)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(terminal_schedule, (0, None));
    assert_eq!(runtime.recover_once().await?, 1);
    assert_eq!(command_state(&fixture.pool, &rejected).await?, "rejected");
    let reason: Option<String> = sqlx::query_scalar(
        "SELECT sanitized_error_code FROM venue_binance_commands WHERE command_id=$1",
    )
    .bind(&rejected)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(reason.as_deref(), Some("binance_-4164"));
    insert_terminal_command(
        &fixture.pool,
        &rejected_after_unknown_dispatch,
        &id(973),
        &user,
        &account,
        &credential,
        "reconcile_required",
    )
    .await?;
    assert_eq!(runtime.recover_once().await?, 1);
    assert_eq!(
        command_state(&fixture.pool, &rejected_after_unknown_dispatch).await?,
        "rejected"
    );
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn grid_claim_carries_the_locked_durable_hot_plan_context()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&database_url).await?;
    fixture.migrate_twice().await?;
    let user = id(1_790);
    let account = id(1_791);
    let credential = id(1_792);
    let instance = id(1_793);
    let batch = "grid-context-batch";
    let now = test_now_ms()?;
    seed_verified_account(&fixture.pool, &user, &account, &credential, 219).await?;
    insert_sending_grid_batch(
        &fixture.pool,
        &user,
        &account,
        &credential,
        &instance,
        batch,
        now,
        2,
    )
    .await?;
    sqlx::query(
        "UPDATE venue_binance_commands SET command_state='pending',sending_ms=NULL,updated_ms=$1 \
         WHERE grid_batch_id=$2",
    )
    .bind(i64::try_from(now + 1)?)
    .bind(batch)
    .execute(&fixture.pool)
    .await?;

    let claimed = PgExecutorStore::new(fixture.pool.clone())
        .claim_next_command_batch(&account, now + 2)
        .await?
        .ok_or("Grid batch was not claimed")?;
    assert_eq!(claimed.grid_batch_id.as_deref(), Some(batch));
    assert_eq!(claimed.commands.len(), 2);
    let context = claimed.grid_context.ok_or("hot plan context was absent")?;
    assert_eq!(context.batch_digest, [84_u8; 32]);
    assert_eq!(context.private_generation, 7);
    assert_eq!(context.private_observed_ms, now);
    assert_eq!(context.instrument_generation, 8);
    assert_eq!(context.source_event_received_ms, Some(now));
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn runtime_restart_reads_every_unresolved_grid_batch_sibling()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&database_url).await?;
    fixture.migrate_twice().await?;
    let user = id(1_800);
    let account = id(1_801);
    let credential = id(1_802);
    let instance = id(1_803);
    let batch = "grid-restart-batch";
    let now = test_now_ms()?;
    seed_verified_account(&fixture.pool, &user, &account, &credential, 220).await?;
    let client_ids = insert_sending_grid_batch(
        &fixture.pool,
        &user,
        &account,
        &credential,
        &instance,
        batch,
        now,
        3,
    )
    .await?;

    let cipher = CredentialCipher::from_key(&[52_u8; 32])?;
    let payload = serde_json::to_vec(&BindCredentialRequest {
        label: "grid-restart".into(),
        api_key: SecretValue::new("a".repeat(32)),
        api_secret: SecretValue::new("b".repeat(32)),
    })?;
    let encrypted = cipher.encrypt(&format!("venue-api-v1:{user}:{credential}"), &payload)?;
    // This fixture must pass the credential gate before exercising dispatch uncertainty.
    sqlx::query("UPDATE venue_api_credentials SET encrypted_credentials=$1,verification_json='{\"verification\":\"verified\"}'::jsonb WHERE credential_id=$2")
        .bind(encrypted)
        .bind(&credential)
        .execute(&fixture.pool)
        .await?;

    let mut exchange = MockBinanceExecution::default();
    for client_id in &client_ids {
        exchange.set_readback(client_id.clone(), ExecutionReadback::Unknown);
    }
    let store = PgExecutorStore::new(fixture.pool.clone());
    let secrets = ExecutorSecretProvider::new(fixture.pool.clone(), cipher);
    let mut runtime = BinanceExecutorRuntime::new(store, exchange, secrets);

    assert_eq!(runtime.recover_once().await?, 1);
    let first_pass: Vec<(String, i32)> = sqlx::query_as(
        "SELECT command_state,reconcile_attempts FROM venue_binance_commands \
         WHERE grid_batch_id=$1 ORDER BY dispatch_sequence",
    )
    .bind(batch)
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(
        first_pass,
        vec![("reconcile_required".into(), 0); client_ids.len()]
    );

    sqlx::query("UPDATE venue_binance_commands SET next_reconcile_ms=1 WHERE grid_batch_id=$1")
        .bind(batch)
        .execute(&fixture.pool)
        .await?;
    assert_eq!(runtime.recover_once().await?, 1);
    let second_pass: Vec<(String, i32)> = sqlx::query_as(
        "SELECT command_state,reconcile_attempts FROM venue_binance_commands \
         WHERE grid_batch_id=$1 ORDER BY dispatch_sequence",
    )
    .bind(batch)
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(
        second_pass,
        vec![("reconcile_required".into(), 1); client_ids.len()]
    );
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn runtime_never_rejects_a_batch_after_dispatch_may_have_started()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&database_url).await?;
    fixture.migrate_twice().await?;
    let user = id(1_810);
    let account = id(1_811);
    let credential = id(1_812);
    let instance = id(1_813);
    let batch = "grid-uncertain-batch";
    let now = test_now_ms()?;
    seed_verified_account(&fixture.pool, &user, &account, &credential, 221).await?;
    insert_sending_grid_batch(
        &fixture.pool,
        &user,
        &account,
        &credential,
        &instance,
        batch,
        now,
        2,
    )
    .await?;
    sqlx::query(
        "UPDATE venue_binance_commands SET command_state='pending',sending_ms=NULL,updated_ms=$1 \
         WHERE grid_batch_id=$2",
    )
    .bind(i64::try_from(now + 1)?)
    .bind(batch)
    .execute(&fixture.pool)
    .await?;

    let cipher = CredentialCipher::from_key(&[53_u8; 32])?;
    let payload = serde_json::to_vec(&BindCredentialRequest {
        label: "grid-uncertain".into(),
        api_key: SecretValue::new("a".repeat(32)),
        api_secret: SecretValue::new("b".repeat(32)),
    })?;
    let encrypted = cipher.encrypt(&format!("venue-api-v1:{user}:{credential}"), &payload)?;
    sqlx::query("UPDATE venue_api_credentials SET encrypted_credentials=$1,verification_json='{\"verification\":\"verified\"}'::jsonb WHERE credential_id=$2")
        .bind(encrypted)
        .bind(&credential)
        .execute(&fixture.pool)
        .await?;

    let mut exchange = MockBinanceExecution::default();
    exchange.set_grid_batch_failure(
        venue_control::executor_exchange::GridBatchSubmitError::DispatchUncertain,
    );
    let dispatch_probe = exchange.clone();
    let store = PgExecutorStore::new(fixture.pool.clone());
    let secrets = ExecutorSecretProvider::new(fixture.pool.clone(), cipher);
    let mut runtime = BinanceExecutorRuntime::new(store, exchange, secrets);
    assert_eq!(runtime.recover_once().await?, 1);
    assert!(dispatch_probe.grid_batch_dispatch_started());
    let states: Vec<String> = sqlx::query_scalar(
        "SELECT command_state FROM venue_binance_commands WHERE grid_batch_id=$1 \
         ORDER BY dispatch_sequence",
    )
    .bind(batch)
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(states, vec!["reconcile_required"; 2]);
    assert!(!states.iter().any(|state| state == "rejected"));
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
        sqlx::query("UPDATE venue_api_credentials SET encrypted_credentials=$1,verification_json='{\"verification\":\"verified\"}'::jsonb WHERE credential_id=$2")
            .bind(encrypted).bind(credential).execute(&fixture.pool).await?;
    }
    insert_kol_profile(&fixture.pool, &kol, &leader, 1).await?;
    insert_invite(&fixture.pool, &invite, &kol, 170).await?;
    kol_activation::authorize_leader(&fixture.pool, &kol, &leader, &kol_credential).await?;
    sqlx::query("INSERT INTO venue_user_kol_bindings (user_id,kol_user_id,invite_id,bound_ms) VALUES ($1,$2,$3,1)")
        .bind(&follower).bind(&kol).bind(&invite).execute(&fixture.pool).await?;
    sqlx::query("INSERT INTO venue_kol_follow_relations (relation_id,follower_user_id,kol_user_id,leader_trading_account_id,follower_trading_account_id,credential_id,relation_state,allocated_capital,multiplier,max_order_notional,max_total_notional,max_deviation_bps,allowed_symbols,revision,created_ms,updated_ms) VALUES ($1,$2,$3,$4,$5,$6,'paused','100','1','20','100',100,'[\"BTC/USDT\"]'::jsonb,1,1,1)")
        .bind(&relation).bind(&follower).bind(&kol).bind(&leader).bind(&follower_account).bind(&follower_credential).execute(&fixture.pool).await?;
    sqlx::query("INSERT INTO venue_kol_activation_requests (relation_id,request_id,relation_revision,request_state,requested_ms,updated_ms) VALUES ($1,$2,1,'pending',1,1)")
        .bind(&relation).bind(id(970)).execute(&fixture.pool).await?;
    let store = PgExecutorStore::new(fixture.pool.clone());
    let secrets = ExecutorSecretProvider::new(fixture.pool.clone(), cipher);
    let baseline_ms = test_now_ms()?;
    let mut exchange = MockBinanceExecution::default();
    exchange.set_baseline(
        leader.clone(),
        kol_activation::baseline(&leader, 170, baseline_ms, Decimal::ONE)?,
    );
    exchange.set_baseline(
        follower_account.clone(),
        kol_activation::baseline(&follower_account, 171, baseline_ms, Decimal::ZERO)?,
    );
    let mut runtime = BinanceExecutorRuntime::new(store.clone(), exchange, secrets);
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
    let sources = store.active_kol_private_sources(9_999_999).await?;
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].credential_id, kol_credential);
    let projection_sources = BinancePrivateProjectionStore::new(fixture.pool.clone())
        .active_sources(9_999_999)
        .await?;
    let leader_source = projection_sources
        .iter()
        .find(|source| source.credential_id == kol_credential)
        .ok_or("enabled KOL source was not admitted to signed projection recovery")?;
    assert_eq!(leader_source.kol_user_id.as_deref(), Some(kol.as_str()));
    assert_eq!(leader_source.trading_account_id, leader);
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
async fn executor_secret_provider_requires_the_durable_verified_credential_owner()
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
    sqlx::query("UPDATE venue_api_credentials SET encrypted_credentials=$1,verification_json='{\"verification\":\"verified\"}'::jsonb WHERE credential_id=$2")
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
    sqlx::query(
        "UPDATE venue_api_credentials SET verification_json='{}'::jsonb WHERE credential_id=$1",
    )
    .bind(&credential)
    .execute(&fixture.pool)
    .await?;
    assert_eq!(
        provider.load(&credential, &user).await.err(),
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
    sqlx::query("INSERT INTO venue_control_strategy_scopes (instance_id,venue,mode,trading_account_id,symbol,config_epoch,snapshot_generated_ms) VALUES ($1,'binance','LIVE',$2,'BTC/USDT',1,1)")
        .bind(id(942)).bind(&account).execute(&fixture.pool).await?;
    assert!(store.claim_next_command(&account, 2).await?.is_none());
    sqlx::query("DELETE FROM venue_control_strategy_scopes WHERE trading_account_id=$1")
        .bind(&account)
        .execute(&fixture.pool)
        .await?;
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
        .transition_command_with_readback(
            &command,
            ExecutorCommandState::Accepted,
            3,
            None,
            Some("native-a"),
        )
        .await?;
    let accepted_schedule: (i32, Option<i64>) = sqlx::query_as(
        "SELECT reconcile_attempts,next_reconcile_ms FROM venue_binance_commands WHERE command_id=$1",
    )
    .bind(&command)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(accepted_schedule, (0, Some(503)));
    assert!(
        store
            .transition_command_with_readback(
                &command,
                ExecutorCommandState::Reconciled,
                4,
                None,
                Some("native-b"),
            )
            .await
            .is_err()
    );
    let unchanged: (String, Option<String>) = sqlx::query_as(
        "SELECT command_state,native_order_id FROM venue_binance_commands WHERE command_id=$1",
    )
    .bind(&command)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(unchanged, ("accepted".into(), Some("native-a".into())));
    store
        .transition_command_with_readback(
            &command,
            ExecutorCommandState::Reconciled,
            5,
            None,
            Some("native-a"),
        )
        .await?;
    assert!(store.recover_nonterminal().await?.is_empty());
    let cancel_command = id(943);
    sqlx::query("INSERT INTO venue_binance_commands (command_id,command_origin,request_id,owner_user_id,trading_account_id,credential_id,symbol,command_phase,order_kind,rule_version,selected_native_order_id,client_order_id,command_state,created_ms,updated_ms) VALUES ($1,'terminal',$2,$3,$4,$5,'BTC/USDT','cancel','cancel_exact','fixture','777',$1,'pending',6,6)")
        .bind(&cancel_command)
        .bind(id(944))
        .bind(&user)
        .bind(&account)
        .bind(&credential)
        .execute(&fixture.pool)
        .await?;
    let claimed_cancel = store
        .claim_next_command(&account, 7)
        .await?
        .ok_or("cancel command was not claimed")?;
    assert_eq!(
        claimed_cancel.order,
        venue_control::kol_executor::ClaimedBinanceOrder::CancelExact {
            native_order_id: Some("777".into()),
            target_client_order_id: None,
        }
    );
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
    kol_activation::authorize_leader(&fixture.pool, &kol, &leader, &kol_credential).await?;
    sqlx::query("INSERT INTO venue_user_kol_bindings (user_id,kol_user_id,invite_id,bound_ms) VALUES ($1,$2,$3,1)")
        .bind(&follower).bind(&kol).bind(&invite).execute(&fixture.pool).await?;
    sqlx::query("INSERT INTO venue_kol_follow_relations (relation_id,follower_user_id,kol_user_id,leader_trading_account_id,follower_trading_account_id,credential_id,relation_state,allocated_capital,multiplier,max_order_notional,max_total_notional,max_deviation_bps,allowed_symbols,revision,created_ms,updated_ms) VALUES ($1,$2,$3,$4,$5,$6,'paused','100','1','20','100',100,'[\"BTC/USDT\"]'::jsonb,1,1,1)")
        .bind(&relation).bind(&follower).bind(&kol).bind(&leader).bind(&follower_account).bind(&follower_credential).execute(&fixture.pool).await?;
    sqlx::query("INSERT INTO venue_kol_activation_requests (relation_id,request_id,relation_revision,request_state,requested_ms,updated_ms) VALUES ($1,$2,1,'pending',1,1)")
        .bind(&relation).bind(id(930)).execute(&fixture.pool).await?;
    let store = PgExecutorStore::new(fixture.pool.clone());
    sqlx::query("UPDATE venue_api_credentials SET verification_json='{\"verification\":\"verified\"}'::jsonb WHERE credential_id=ANY($1)")
        .bind(vec![&kol_credential, &follower_credential]).execute(&fixture.pool).await?;
    let activation = store
        .pending_activations(2)
        .await?
        .pop()
        .ok_or("missing activation")?;
    let mut wrong = activation.clone();
    wrong.revision = 2;
    let leader_baseline = kol_activation::baseline(&leader, 130, 1, Decimal::ONE)?;
    let follower_baseline = kol_activation::baseline(&follower_account, 131, 1, Decimal::ZERO)?;
    assert!(
        store
            .complete_activation(&wrong, &leader_baseline, &follower_baseline, 2)
            .await
            .is_err()
    );
    let before: String = sqlx::query_scalar(
        "SELECT relation_state FROM venue_kol_follow_relations WHERE relation_id=$1",
    )
    .bind(&relation)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(before, "paused");
    store
        .complete_activation(&activation, &leader_baseline, &follower_baseline, 2)
        .await?;
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

#[tokio::test]
async fn private_projection_is_subscribed_persisted_and_owner_scoped()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&database_url).await?;
    fixture.migrate_twice().await?;
    let user = id(960);
    let other = id(961);
    let account = id(962);
    let credential = id(963);
    seed_verified_account(&fixture.pool, &user, &account, &credential, 71).await?;
    sqlx::query("UPDATE venue_api_credentials SET verification_json='{\"verification\":\"verified\"}'::jsonb WHERE credential_id=$1")
        .bind(&credential).execute(&fixture.pool).await?;
    sqlx::query("INSERT INTO venue_users (user_id,username,password_hash,created_ms) VALUES ($1,'projection-other','test',1)")
        .bind(&other).execute(&fixture.pool).await?;
    let store = BinancePrivateProjectionStore::new(fixture.pool.clone());
    let symbol: venue_domain::domain::Symbol = "BTC/USDT".parse()?;
    store
        .subscribe(&user, &credential, std::slice::from_ref(&symbol), 100)
        .await?;
    let sources = store.active_sources(101).await?;
    assert_eq!(sources.len(), 1);
    let source = ActiveProjectionSource {
        kol_user_id: None,
        owner_user_id: user.clone(),
        credential_id: credential.clone(),
        trading_account_id: account.clone(),
        symbols: [symbol.clone()].into_iter().collect(),
        previous_fills_cursor: None,
    };
    let snapshot = projection_snapshot(
        account.clone(),
        symbol.clone(),
        110,
        1,
        "fills-1",
        Decimal::new(2, 3),
        Decimal::new(50_100, 0),
        true,
    )?;
    store.persist(&source, &snapshot, 111).await?;
    let snapshot = projection_snapshot(
        account,
        symbol,
        120,
        2,
        "fills-2",
        Decimal::new(3, 3),
        Decimal::new(50_100, 0),
        false,
    )?;
    store.persist(&source, &snapshot, 121).await?;
    let snapshot = projection_snapshot(
        source.trading_account_id.clone(),
        "BTC/USDT".parse()?,
        130,
        3,
        "fills-3",
        Decimal::new(3, 3),
        Decimal::new(50_200, 0),
        false,
    )?;
    store.persist(&source, &snapshot, 131).await?;
    let owned = store
        .load_owned(&user, &credential)
        .await?
        .ok_or("missing projection")?;
    assert_eq!(owned.private_generation, 3);
    assert_eq!(owned.fills.len(), 1);
    assert_eq!(owned.position_history.len(), 2);
    assert!(store.load_owned(&other, &credential).await?.is_none());
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn private_stream_fill_batch_is_atomic_idempotent_and_generation_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&database_url).await?;
    fixture.migrate_twice().await?;
    let user = id(964);
    let account = id(965);
    let credential = id(966);
    seed_verified_account(&fixture.pool, &user, &account, &credential, 72).await?;
    let symbol: venue_domain::domain::Symbol = "BTC/USDT".parse()?;
    let store = BinancePrivateProjectionStore::new(fixture.pool.clone());
    let source = ActiveProjectionSource {
        kol_user_id: None,
        owner_user_id: user,
        credential_id: credential,
        trading_account_id: account,
        symbols: [symbol.clone()].into_iter().collect(),
        previous_fills_cursor: None,
    };
    store
        .persist(
            &source,
            &projection_snapshot(
                source.trading_account_id.clone(),
                symbol,
                100,
                3,
                "batch-cursor",
                Decimal::new(1, 3),
                Decimal::new(50_000, 0),
                false,
            )?,
            101,
        )
        .await?;

    let partial = private_stream_fill(
        "trade-partial",
        110,
        Decimal::new(1, 3),
        OrderState::PartiallyFilled,
    )?;
    let full = private_stream_fill("trade-full", 111, Decimal::new(2, 3), OrderState::Filled)?;
    store
        .persist_stream_fills(&source, &[partial.clone(), full.clone()])
        .await?;
    store
        .persist_stream_fills(&source, &[partial.clone(), full])
        .await?;
    let persisted: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM venue_binance_account_fills WHERE trading_account_id=$1",
    )
    .bind(&source.trading_account_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(persisted, 2);

    let new_fill = private_stream_fill(
        "trade-new",
        112,
        Decimal::new(1, 3),
        OrderState::PartiallyFilled,
    )?;
    let mut conflicting = partial;
    conflicting.fill.quantity = Decimal::new(9, 3);
    assert_eq!(
        store
            .persist_stream_fills(&source, &[new_fill.clone(), conflicting])
            .await,
        Err(PrivateProjectionError::Invalid)
    );
    let rolled_back: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM venue_binance_account_fills WHERE trading_account_id=$1 AND native_trade_id='trade-new'",
    )
    .bind(&source.trading_account_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(rolled_back, 0);

    let mut mixed_generation = private_stream_fill(
        "trade-next-generation",
        113,
        Decimal::new(1, 3),
        OrderState::PartiallyFilled,
    )?;
    mixed_generation.private_generation = 4;
    assert_eq!(
        store
            .persist_stream_fills(&source, &[new_fill, mixed_generation])
            .await,
        Err(PrivateProjectionError::Invalid)
    );
    fixture.cleanup().await?;
    Ok(())
}

fn private_stream_fill(
    fill_id: &str,
    received_at_ms: u64,
    cumulative: Decimal,
    state: OrderState,
) -> Result<BinancePrivateFillEvent, Box<dyn std::error::Error>> {
    Ok(BinancePrivateFillEvent {
        stream_private_generation: 3,
        private_generation: 3,
        received_at_ms,
        fill: Fill {
            fill_id: fill_id.to_owned(),
            execution_sequence: FieldState::Known(received_at_ms),
            order_id: "native-order-batch".to_owned(),
            symbol: "BTC/USDT".parse()?,
            side: OrderSide::Buy,
            position_side: FieldState::Known(PositionSide::Long),
            quantity: Decimal::new(1, 3),
            price: Price::new(Decimal::new(50_000, 0))?,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(true),
            exchange_time_ms: Some(received_at_ms - 1),
        },
        client_order_id: FieldState::Known("client-batch".to_owned()),
        original_quantity: FieldState::Known(Decimal::new(2, 3)),
        cumulative_filled_quantity: FieldState::Known(cumulative),
        order_state: FieldState::Known(state),
    })
}

fn projection_snapshot(
    account: String,
    symbol: venue_domain::domain::Symbol,
    observed_ms: u64,
    private_generation: u64,
    cursor: &str,
    quantity: Decimal,
    mark_price: Decimal,
    include_fill: bool,
) -> Result<SignedAccountSnapshot, Box<dyn std::error::Error>> {
    let binding =
        GatewayBinding::new(VenueId::Binance, GatewayMode::Live, account, symbol.clone())?;
    let fills = if include_fill {
        vec![Fill {
            fill_id: "native-trade-1".into(),
            execution_sequence: FieldState::Known(1),
            order_id: "native-order-1".into(),
            symbol: symbol.clone(),
            side: OrderSide::Buy,
            position_side: FieldState::Known(PositionSide::Long),
            quantity: Decimal::new(1, 3),
            price: Price::new(Decimal::new(50_000, 0))?,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(true),
            exchange_time_ms: Some(observed_ms - 1),
        }]
    } else {
        Vec::new()
    };
    Ok(SignedAccountSnapshot::complete_with_fills(
        binding,
        observed_ms,
        1,
        private_generation,
        1,
        SignedAccountPositionMode::Hedge,
        Vec::new(),
        vec![SignedAccountPositionFact {
            symbol,
            position_side: PositionSide::Long,
            quantity,
            entry_price: Some(Decimal::new(50_000, 0)),
            mark_price: Some(mark_price),
        }],
        fills,
        cursor.to_owned(),
        Vec::new(),
    )?
    .with_balances(vec![SignedAccountBalance {
        asset: Asset::new("USDT")?,
        equity: Decimal::new(1_000, 0),
        available_margin: Some(Decimal::new(800, 0)),
    }])?)
}

#[tokio::test]
async fn terminal_post_only_command_is_idempotent_owner_scoped_and_durable()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&database_url).await?;
    fixture.migrate_twice().await?;
    let service = AccountService::new_with_node_token(
        fixture.pool.clone(),
        CredentialCipher::from_key(&[9_u8; 32])?,
        None,
    )?;
    let now = test_now_ms()?;
    let session = service
        .register(
            LoginRequest {
                username: "terminal-owner".into(),
                password: SecretValue::new("safe terminal password".into()),
            },
            now,
        )
        .await?;
    let principal = service
        .authenticate(session.token.expose(), now + 1)
        .await?;
    let account = id(972);
    let credential = id(973);
    sqlx::query("INSERT INTO venue_user_trading_accounts (trading_account_id,user_id,venue,exchange_identity_hash) VALUES ($1,$2,'binance',$3)")
        .bind(&account).bind(&principal.user.user_id).bind(vec![72_u8; 32]).execute(&fixture.pool).await?;
    sqlx::query("INSERT INTO venue_api_credentials (credential_id,user_id,label,key_fingerprint,masked_key,encrypted_credentials,trading_account_id,verification_json,created_ms) VALUES ($1,$2,'terminal',$3,'***',decode('00','hex'),$4,'{\"verification\":\"verified\"}'::jsonb,$5)")
        .bind(&credential).bind(&principal.user.user_id).bind(vec![73_u8; 32]).bind(&account).bind(i64::try_from(now)?).execute(&fixture.pool).await?;
    let symbol: venue_domain::domain::Symbol = "BTC/USDT".parse()?;
    let projection_store = BinancePrivateProjectionStore::new(fixture.pool.clone());
    let source = ActiveProjectionSource {
        kol_user_id: None,
        owner_user_id: principal.user.user_id.clone(),
        credential_id: credential.clone(),
        trading_account_id: account,
        symbols: [symbol.clone()].into_iter().collect(),
        previous_fills_cursor: None,
    };
    projection_store
        .persist(
            &source,
            &projection_snapshot(
                source.trading_account_id.clone(),
                symbol.clone(),
                now + 2,
                1,
                "terminal-cursor",
                Decimal::new(5, 3),
                Decimal::new(50_100, 0),
                false,
            )?,
            now + 3,
        )
        .await?;
    let request = TerminalOrderRequest {
        schema_version: TERMINAL_SCHEMA_VERSION,
        request_id: id(974),
        credential_id: credential,
        symbol,
        action: TerminalAction::OpenLong,
        order_kind: TerminalOrderKind::LimitPostOnly,
        quote_notional: Decimal::new(100, 0),
        limit_price: Some(Decimal::new(50_000, 0)),
        close_quantity_cap: None,
        market_risk_confirmed: false,
    };
    let first = service
        .enqueue_terminal_order(&principal, request.clone(), now + 4)
        .await?;
    let replay = service
        .enqueue_terminal_order(&principal, request.clone(), now + 5)
        .await?;
    assert_eq!(first.command_id, replay.command_id);
    assert_eq!(first.order_kind, ExecutorOrderKind::LimitPostOnly);
    assert_eq!(first.requested_quantity, Some(Decimal::new(2, 3)));
    assert_eq!(service.terminal_executions(&principal).await?.len(), 1);
    for offset in 0_i64..15 {
        insert_terminal_command(
            &fixture.pool,
            &id(1_100 + offset),
            &id(1_200 + offset),
            &principal.user.user_id,
            &source.trading_account_id,
            &source.credential_id,
            "pending",
        )
        .await?;
    }
    let full_queue_replay = service
        .enqueue_terminal_order(&principal, request.clone(), now + 6)
        .await?;
    assert_eq!(first.command_id, full_queue_replay.command_id);
    let mut overflow = request;
    overflow.request_id = id(1_300);
    let overflow_error = service
        .enqueue_terminal_order(&principal, overflow, now + 7)
        .await
        .err()
        .ok_or("terminal queue overflow was unexpectedly admitted")?;
    assert_eq!(overflow_error.code, AccountErrorCode::RateLimited);
    let unresolved: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM venue_binance_commands WHERE trading_account_id=$1 \
         AND command_state IN ('pending','sending','accepted','reconcile_required')",
    )
    .bind(&source.trading_account_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(unresolved, 16);
    sqlx::query("INSERT INTO venue_control_strategy_scopes (instance_id,venue,mode,trading_account_id,symbol,config_epoch,snapshot_generated_ms) VALUES ($1,'binance','LIVE',$2,'BTC/USDT',1,$3)")
        .bind(id(975)).bind(&source.trading_account_id).bind(i64::try_from(now)?).execute(&fixture.pool).await?;
    let fenced = TerminalOrderRequest {
        schema_version: TERMINAL_SCHEMA_VERSION,
        request_id: id(976),
        credential_id: source.credential_id,
        symbol: "BTC/USDT".parse()?,
        action: TerminalAction::OpenLong,
        order_kind: TerminalOrderKind::LimitPostOnly,
        quote_notional: Decimal::new(100, 0),
        limit_price: Some(Decimal::new(50_000, 0)),
        close_quantity_cap: None,
        market_risk_confirmed: false,
    };
    assert!(
        service
            .enqueue_terminal_order(&principal, fenced, now + 8)
            .await
            .is_err()
    );
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn terminal_and_copy_share_one_atomic_account_queue_limit()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&database_url).await?;
    fixture.migrate_twice().await?;
    let service = AccountService::new_with_node_token(
        fixture.pool.clone(),
        CredentialCipher::from_key(&[12_u8; 32])?,
        None,
    )?;
    let now = test_now_ms()?;
    let session = service
        .register(
            LoginRequest {
                username: "shared-account-admission".into(),
                password: SecretValue::new("safe shared queue password".into()),
            },
            now,
        )
        .await?;
    let principal = service
        .authenticate(session.token.expose(), now + 1)
        .await?;

    let follower_account = id(1_401);
    let follower_credential = id(1_402);
    sqlx::query("INSERT INTO venue_user_trading_accounts (trading_account_id,user_id,venue,exchange_identity_hash) VALUES ($1,$2,'binance',$3)")
        .bind(&follower_account).bind(&principal.user.user_id).bind(vec![141_u8; 32]).execute(&fixture.pool).await?;
    sqlx::query("INSERT INTO venue_api_credentials (credential_id,user_id,label,key_fingerprint,masked_key,encrypted_credentials,trading_account_id,verification_json,created_ms) VALUES ($1,$2,'shared-queue',$3,'***',decode('00','hex'),$4,'{\"verification\":\"verified\"}'::jsonb,$5)")
        .bind(&follower_credential).bind(&principal.user.user_id).bind(vec![142_u8; 32]).bind(&follower_account).bind(i64::try_from(now)?).execute(&fixture.pool).await?;

    let symbol: venue_domain::domain::Symbol = "BTC/USDT".parse()?;
    let projection_store = BinancePrivateProjectionStore::new(fixture.pool.clone());
    let projection_source = ActiveProjectionSource {
        kol_user_id: None,
        owner_user_id: principal.user.user_id.clone(),
        credential_id: follower_credential.clone(),
        trading_account_id: follower_account.clone(),
        symbols: [symbol.clone()].into_iter().collect(),
        previous_fills_cursor: None,
    };
    projection_store
        .persist(
            &projection_source,
            &projection_snapshot(
                follower_account.clone(),
                symbol.clone(),
                now + 2,
                1,
                "shared-queue-cursor",
                Decimal::new(5, 3),
                Decimal::new(50_100, 0),
                false,
            )?,
            now + 3,
        )
        .await?;

    let kol = id(1_410);
    let leader_account = id(1_411);
    let leader_credential = id(1_412);
    let invite = id(1_413);
    let relation = id(1_414);
    seed_verified_account(
        &fixture.pool,
        &kol,
        &leader_account,
        &leader_credential,
        210,
    )
    .await?;
    insert_kol_profile(&fixture.pool, &kol, &leader_account, 1).await?;
    insert_invite(&fixture.pool, &invite, &kol, 211).await?;
    sqlx::query("INSERT INTO venue_user_kol_bindings (user_id,kol_user_id,invite_id,bound_ms) VALUES ($1,$2,$3,1)")
        .bind(&principal.user.user_id).bind(&kol).bind(&invite).execute(&fixture.pool).await?;
    insert_follow_relation(
        &fixture.pool,
        &relation,
        &principal.user.user_id,
        &kol,
        &leader_account,
        &follower_account,
        &follower_credential,
        1,
    )
    .await?;
    for offset in 0_i64..15 {
        insert_terminal_command(
            &fixture.pool,
            &id(1_500 + offset),
            &id(1_600 + offset),
            &principal.user.user_id,
            &follower_account,
            &follower_credential,
            "pending",
        )
        .await?;
    }

    let request = TerminalOrderRequest {
        schema_version: TERMINAL_SCHEMA_VERSION,
        request_id: id(1_700),
        credential_id: follower_credential,
        symbol,
        action: TerminalAction::OpenLong,
        order_kind: TerminalOrderKind::LimitPostOnly,
        quote_notional: Decimal::new(100, 0),
        limit_price: Some(Decimal::new(50_000, 0)),
        close_quantity_cap: None,
        market_risk_confirmed: false,
    };
    let fill = KolSourceFill {
        leader_trading_account_id: leader_account,
        native_symbol: "BTCUSDT".into(),
        native_trade_id: "shared-queue-trade".into(),
        symbol: "BTC/USDT".into(),
        order_side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::new(2, 3),
        price: Decimal::new(50_000, 0),
        occurred_ms: now + 3,
        observed_ms: now + 4,
        payload_digest: [17_u8; 32],
    };
    let store = PgExecutorStore::new(fixture.pool.clone());
    let (terminal_result, copy_result) = tokio::join!(
        service.enqueue_terminal_order(&principal, request, now + 5),
        store.record_source_fill_and_plan(&kol, &fill, now + 5),
    );
    let planned = copy_result?;
    let terminal_inserted = match terminal_result {
        Ok(_) => true,
        Err(error) => {
            assert_eq!(error.code, AccountErrorCode::RateLimited);
            false
        }
    };
    let terminal_count = if terminal_inserted { 1 } else { 0 };
    assert_eq!(terminal_count + planned.len(), 1);
    let unresolved: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM venue_binance_commands WHERE trading_account_id=$1 \
         AND command_state IN ('pending','sending','accepted','reconcile_required')",
    )
    .bind(&follower_account)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(unresolved, 16);
    fixture.cleanup().await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_sending_grid_batch(
    pool: &PgPool,
    owner_user_id: &str,
    trading_account_id: &str,
    credential_id: &str,
    instance_id: &str,
    batch_id: &str,
    now_ms: u64,
    command_count: usize,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let now = i64::try_from(now_ms)?;
    let count = i16::try_from(command_count)?;
    let mut tx = pool.begin().await?;
    sqlx::query(
        "INSERT INTO venue_binance_grid_instances \
         (instance_id,owner_user_id,trading_account_id,credential_id,create_request_id,\
          create_request_digest,symbol,instance_state,revision,current_config_revision,\
          plan_revision,desired_digest,dirty,consecutive_failures,created_ms,updated_ms) \
         VALUES ($1,$2,$3,$4,$5,$6,'BTC/USDT','running',1,1,1,$7,true,0,$8,$8)",
    )
    .bind(instance_id)
    .bind(owner_user_id)
    .bind(trading_account_id)
    .bind(credential_id)
    .bind(format!("create-{instance_id}"))
    .bind(vec![81_u8; 32])
    .bind(vec![82_u8; 32])
    .bind(now)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO venue_binance_grid_config_revisions \
         (instance_id,config_revision,request_id,config_json,config_digest,created_ms) \
         VALUES ($1,1,$2,'{}'::jsonb,$3,$4)",
    )
    .bind(instance_id)
    .bind(format!("config-{instance_id}"))
    .bind(vec![83_u8; 32])
    .bind(now)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO venue_binance_grid_mutation_batches \
         (batch_id,instance_id,expected_instance_revision,config_revision,plan_revision,\
          desired_digest,batch_digest,command_count,private_generation,private_observed_ms,\
          instrument_generation,source_event_received_ms,created_ms) \
         VALUES ($1,$2,1,1,1,$3,$4,$5,7,$6,8,$6,$6)",
    )
    .bind(batch_id)
    .bind(instance_id)
    .bind(vec![82_u8; 32])
    .bind(vec![84_u8; 32])
    .bind(count)
    .bind(now)
    .execute(&mut *tx)
    .await?;

    let mut client_ids = Vec::with_capacity(command_count);
    for sequence in 1..=command_count {
        let command_id = format!("grid-restart-command-{sequence}");
        let client_id = format!("grid-restart-client-{sequence}");
        sqlx::query(
            "INSERT INTO venue_binance_commands \
             (command_id,command_origin,owner_user_id,trading_account_id,credential_id,symbol,\
              position_side,command_phase,order_kind,order_side,requested_quantity,limit_price,\
              rule_version,client_order_id,command_state,source_digest,sending_ms,created_ms,updated_ms,\
              grid_instance_id,grid_config_revision,grid_plan_revision,grid_semantic_key,\
              grid_batch_id,dispatch_sequence) VALUES \
             ($1,'grid',$2,$3,$4,'BTC/USDT','long','open','limit_post_only','buy','0.001',\
              '50000','binance-pm-um-grid-r1',$5,'sending',$6,$7,$7,$7,$8,1,1,$9,$10,$11)",
        )
        .bind(&command_id)
        .bind(owner_user_id)
        .bind(trading_account_id)
        .bind(credential_id)
        .bind(&client_id)
        .bind(vec![85_u8; 32])
        .bind(now)
        .bind(instance_id)
        .bind(format!("place:long:open:{sequence}"))
        .bind(batch_id)
        .bind(i64::try_from(sequence)?)
        .execute(&mut *tx)
        .await?;
        client_ids.push(client_id);
    }
    tx.commit().await?;
    Ok(client_ids)
}

#[path = "support/projection_fill_observation.rs"]
mod projection_fill_observation;
#[path = "support/terminal_positions.rs"]
mod terminal_positions;

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

fn test_now_ms() -> Result<u64, Box<dyn std::error::Error>> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
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
        // Windows clock resolution can give parallel fixtures the same timestamp.
        static NEXT_SCHEMA: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sequence = NEXT_SCHEMA.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let schema = format!(
            "venue_kol_mvp_{}_{}_{}",
            std::process::id(),
            nonce,
            sequence
        );
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
            self.migrate_through_0021().await?;
            sqlx::raw_sql(MIGRATION_0022).execute(&self.pool).await?;
            sqlx::raw_sql(MIGRATION_0023).execute(&self.pool).await?;
            sqlx::raw_sql(MIGRATION_0024).execute(&self.pool).await?;
            sqlx::raw_sql(venue_control::MIGRATION_0025)
                .execute(&self.pool)
                .await?;
            sqlx::raw_sql(venue_control::MIGRATION_0026)
                .execute(&self.pool)
                .await?;
            sqlx::raw_sql(venue_control::MIGRATION_0027)
                .execute(&self.pool)
                .await?;
            sqlx::raw_sql(venue_control::MIGRATION_0028)
                .execute(&self.pool)
                .await?;
        }
        sqlx::raw_sql(venue_control::MIGRATION_0029)
            .execute(&self.pool)
            .await?;
        sqlx::raw_sql(venue_control::MIGRATION_0030)
            .execute(&self.pool)
            .await?;
        sqlx::raw_sql(venue_control::MIGRATION_0031)
            .execute(&self.pool)
            .await?;
        sqlx::raw_sql(venue_control::MIGRATION_0032)
            .execute(&self.pool)
            .await?;
        sqlx::raw_sql(venue_control::MIGRATION_0033)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn migrate_through_0021(&self) -> Result<(), sqlx::Error> {
        for migration in [
            MIGRATION_0001,
            MIGRATION_0015,
            MIGRATION_0017,
            MIGRATION_0018,
            MIGRATION_0019,
            MIGRATION_0020,
            MIGRATION_0021,
        ] {
            sqlx::raw_sql(migration).execute(&self.pool).await?;
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
    sqlx::query("INSERT INTO venue_kol_follow_relations (relation_id,follower_user_id,kol_user_id,leader_trading_account_id,follower_trading_account_id,credential_id,relation_state,active_slot,allocated_capital,multiplier,max_order_notional,max_total_notional,max_deviation_bps,allowed_symbols,baseline_json,revision,created_ms,updated_ms) VALUES ($1,$2,$3,$4,$5,$6,'active',$7,'1000','1','100','1000',100,'[\"BTC/USDT\"]'::jsonb,'{\"target_model\":1,\"baseline_ms\":1}'::jsonb,1,1,1)")
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
