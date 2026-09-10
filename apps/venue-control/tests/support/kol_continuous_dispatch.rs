use super::*;
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use venue_control::{
    executor_exchange::{
        AccountBaseline, BinanceActivationBaseline, BinanceExecution, BinanceExecutionError,
        BinanceExecutionFuture, ExecutionOutcome, ExecutionRequest,
    },
    executor_runtime::ExecutorCredentials,
    executor_secret::ExecutorSecretError,
};
use venue_gateway_binance::BinanceCredentials;

struct FixtureCredentials;
impl ExecutorCredentials for FixtureCredentials {
    fn credentials<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<BinanceCredentials, ExecutorSecretError>> + Send + 'a>>
    {
        Box::pin(async {
            BinanceCredentials::from_secrets(
                secrecy::SecretString::from("a".repeat(32)),
                secrecy::SecretString::from("b".repeat(32)),
            )
            .map_err(|_| ExecutorSecretError::Unavailable)
        })
    }
}

#[tokio::test]
async fn unknown_placement_allows_only_serial_exact_cancel_and_cancel_recovery()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    fixture.migrate_twice().await?;
    let (user, account, credential) = (id(681), id(682), id(683));
    let (unknown, placement, cancel) = (id(684), id(685), id(686));
    seed_verified_account(&fixture.pool, &user, &account, &credential, 68).await?;
    for command in [&unknown, &placement, &cancel] {
        insert_terminal_command(
            &fixture.pool,
            command,
            command,
            &user,
            &account,
            &credential,
            "pending",
        )
        .await?;
    }
    sqlx::query("UPDATE venue_api_credentials SET verification_json='{\"verification\":\"verified\"}'::jsonb").execute(&fixture.pool).await?;
    sqlx::query("UPDATE venue_binance_commands SET command_state='reconcile_required',sending_ms=2,next_reconcile_ms=9223372036854775807 WHERE command_id=$1")
        .bind(&unknown).execute(&fixture.pool).await?;
    sqlx::query("UPDATE venue_binance_commands SET command_phase='cancel',order_kind='cancel_exact',position_side=NULL,order_side=NULL,requested_quantity=NULL,limit_price=NULL,selected_native_order_id='123' WHERE command_id=$1")
        .bind(&cancel).execute(&fixture.pool).await?;
    sqlx::query("UPDATE venue_binance_commands SET command_state='sending' WHERE command_id=$1")
        .bind(&unknown)
        .execute(&fixture.pool)
        .await?;
    let store = PgExecutorStore::new(fixture.pool.clone());
    assert!(store.claim_next_command(&account, 10).await?.is_none());
    assert!(
        store
            .claim_next_command_batch(&account, 10)
            .await?
            .is_none()
    );
    sqlx::query("UPDATE venue_binance_commands SET command_state='reconcile_required',next_reconcile_ms=9223372036854775807 WHERE command_id=$1")
        .bind(&unknown).execute(&fixture.pool).await?;
    sqlx::query("INSERT INTO venue_terminal_replacements(command_id,cancel_command_id,original_quantity) VALUES($1,$2,'0.001')")
        .bind(&placement).bind(&cancel).execute(&fixture.pool).await?;
    assert!(
        store
            .claim_next_command_batch(&account, 10)
            .await?
            .is_none()
    );
    sqlx::query("DELETE FROM venue_terminal_replacements WHERE command_id=$1")
        .bind(&placement)
        .execute(&fixture.pool)
        .await?;
    sqlx::query("UPDATE venue_binance_commands SET next_reconcile_ms=9223372036854775807 WHERE command_id=$1")
        .bind(&unknown).execute(&fixture.pool).await?;
    let mut exchange = MockBinanceExecution::default();
    exchange.set_readback(cancel.clone(), ExecutionReadback::Unknown);
    let mut runtime = BinanceExecutorRuntime::new(
        PgExecutorStore::new(fixture.pool.clone()),
        exchange,
        FixtureCredentials,
    );
    assert_eq!(runtime.recover_once().await?, 1);
    assert_eq!(command_state(&fixture.pool, &placement).await?, "pending");
    assert_eq!(
        command_state(&fixture.pool, &unknown).await?,
        "reconcile_required"
    );
    assert_eq!(
        command_state(&fixture.pool, &cancel).await?,
        "reconcile_required"
    );
    let duplicate = id(687);
    insert_terminal_command(
        &fixture.pool,
        &duplicate,
        &duplicate,
        &user,
        &account,
        &credential,
        "pending",
    )
    .await?;
    sqlx::query("UPDATE venue_binance_commands SET command_phase='cancel',order_kind='cancel_exact',position_side=NULL,order_side=NULL,requested_quantity=NULL,selected_native_order_id='123' WHERE command_id=$1")
        .bind(&duplicate).execute(&fixture.pool).await?;
    assert!(
        store
            .claim_next_command_batch(&account, 10)
            .await?
            .is_none()
    );
    store
        .transition_command(&duplicate, ExecutorCommandState::Cancelled, 10, None)
        .await?;
    // The original unknown is not due. A later cancel still gets its own exact recovery turn.
    sqlx::query("UPDATE venue_binance_commands SET next_reconcile_ms=1 WHERE command_id=$1")
        .bind(&cancel)
        .execute(&fixture.pool)
        .await?;
    let mut exchange = MockBinanceExecution::default();
    exchange.set_readback(cancel.clone(), ExecutionReadback::Reconciled);
    let mut restarted = BinanceExecutorRuntime::new(
        PgExecutorStore::new(fixture.pool.clone()),
        exchange,
        FixtureCredentials,
    );
    assert_eq!(restarted.recover_once().await?, 1);
    assert_eq!(command_state(&fixture.pool, &cancel).await?, "reconciled");
    assert_eq!(command_state(&fixture.pool, &placement).await?, "pending");
    assert_eq!(
        command_state(&fixture.pool, &unknown).await?,
        "reconcile_required"
    );
    fixture.cleanup().await?;
    Ok(())
}

#[derive(Clone)]
struct IsolatedExchange {
    slow_account: String,
    seen: tokio::sync::mpsc::UnboundedSender<String>,
    release: Arc<tokio::sync::Notify>,
    fail_account: bool,
}
impl BinanceExecution for IsolatedExchange {
    fn submit<'a>(
        &'a mut self,
        request: &'a ExecutionRequest,
        _: BinanceCredentials,
    ) -> BinanceExecutionFuture<'a> {
        Box::pin(async move {
            self.seen
                .send(request.command_id.clone())
                .map_err(|_| BinanceExecutionError::Unavailable)?;
            if request.trading_account_id == self.slow_account {
                assert!(
                    !self.fail_account,
                    "controlled fixture account-task failure"
                );
                self.release.notified().await;
            }
            Ok(ExecutionOutcome {
                exchange_error_code: None,
                state: ExecutionReadback::Accepted,
                native_order_id: Some(format!("fixture-{}", request.command_id)),
                market_settlement: None,
                order_fact: None,
            })
        })
    }
    fn readback<'a>(
        &'a mut self,
        request: &'a ExecutionRequest,
        credentials: BinanceCredentials,
    ) -> BinanceExecutionFuture<'a> {
        self.submit(request, credentials)
    }
    fn submit_grid_batch<'a>(
        &'a mut self,
        _: &'a venue_control::executor_exchange::GridBatchExecutionContext,
        _: &'a [ExecutionRequest],
        _: BinanceCredentials,
    ) -> venue_control::executor_exchange::BinanceGridBatchFuture<'a> {
        Box::pin(async {
            Err(
                venue_control::executor_exchange::GridBatchSubmitError::DefinitelyNotDispatched(
                    BinanceExecutionError::Invalid,
                ),
            )
        })
    }
}
impl BinanceActivationBaseline for IsolatedExchange {
    async fn activation_baseline(
        &mut self,
        _: &str,
        _: &std::collections::BTreeSet<venue_domain::Symbol>,
        _: BinanceCredentials,
    ) -> Result<AccountBaseline, BinanceExecutionError> {
        Err(BinanceExecutionError::Unavailable)
    }
}

#[tokio::test]
async fn new_fast_account_work_is_discovered_while_another_turn_is_slow_or_failed()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = integration_database_url()? else {
        return Ok(());
    };
    for fail_account in [false, true] {
        let fixture = Fixture::create(&url).await?;
        fixture.migrate_twice().await?;
        let slow = id(604);
        let fast = id(605);
        let first = id(611);
        let second = id(612);
        seed_verified_account(&fixture.pool, &id(601), &slow, &id(607), 61).await?;
        seed_verified_account(&fixture.pool, &id(602), &fast, &id(608), 62).await?;
        sqlx::query("UPDATE venue_api_credentials SET verification_json='{\"verification\":\"verified\"}'::jsonb")
            .execute(&fixture.pool).await?;
        insert_terminal_command(
            &fixture.pool,
            &id(610),
            &id(620),
            &id(601),
            &slow,
            &id(607),
            "pending",
        )
        .await?;
        insert_terminal_command(
            &fixture.pool,
            &first,
            &id(621),
            &id(602),
            &fast,
            &id(608),
            "pending",
        )
        .await?;
        sqlx::query(
            "UPDATE venue_binance_commands SET order_kind='limit_post_only',limit_price='50000'",
        )
        .execute(&fixture.pool)
        .await?;
        let (seen, mut received) = tokio::sync::mpsc::unbounded_channel();
        let exchange = IsolatedExchange {
            slow_account: slow,
            seen,
            release: Arc::new(tokio::sync::Notify::new()),
            fail_account,
        };
        let mut runtime = BinanceExecutorRuntime::new(
            PgExecutorStore::new(fixture.pool.clone()),
            exchange,
            FixtureCredentials,
        );
        let wake = runtime.command_wake();
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(async move { runtime.run_until_shutdown(stopped).await });
        let first_seen = tokio::time::timeout(Duration::from_secs(5), received.recv())
            .await?
            .ok_or("first send missing")?;
        let other_seen = tokio::time::timeout(Duration::from_secs(5), received.recv())
            .await?
            .ok_or("second account missing")?;
        assert!(first_seen == first || other_seen == first);
        insert_terminal_command(
            &fixture.pool,
            &second,
            &id(622),
            &id(602),
            &fast,
            &id(608),
            "pending",
        )
        .await?;
        sqlx::query("UPDATE venue_binance_commands SET order_kind='limit_post_only',limit_price='50000' WHERE command_id=$1")
            .bind(&second).execute(&fixture.pool).await?;
        wake.wake();
        let next = tokio::time::timeout(Duration::from_millis(800), received.recv())
            .await?
            .ok_or("new fast command starved")?;
        assert_eq!(next, second);
        stop.send(true)?;
        tokio::time::timeout(Duration::from_secs(5), task).await???;
        fixture.cleanup().await?;
    }
    Ok(())
}
