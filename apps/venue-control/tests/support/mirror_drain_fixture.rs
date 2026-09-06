use super::*;
use venue_gateway_binance::BinanceAbsentLimitOrder;

pub(super) async fn verify(
    fixture: &Fixture,
    follower: &str,
    credential: &str,
    account: &str,
    bot: &str,
    child: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let cipher = CredentialCipher::from_key(&[42; 32])?;
    let payload = serde_json::to_vec(&BindCredentialRequest {
        label: "fixture".into(),
        api_key: SecretValue::new("a".repeat(32)),
        api_secret: SecretValue::new("b".repeat(32)),
    })?;
    let encrypted = cipher.encrypt(&format!("venue-api-v1:{follower}:{credential}"), &payload)?;
    sqlx::query("UPDATE venue_api_credentials SET encrypted_credentials=$1 WHERE credential_id=$2")
        .bind(encrypted)
        .bind(credential)
        .execute(&fixture.pool)
        .await?;
    sqlx::query("UPDATE venue_binance_commands SET command_state='cancelled',terminal_ms=$1 WHERE command_id<>$2")
        .bind(i64::try_from(test_now_ms()?)?).bind(child).execute(&fixture.pool).await?;
    sqlx::query(
        "UPDATE venue_order_mirrors SET mirror_state='terminal' WHERE child_client_order_id<>$1",
    )
    .bind(child)
    .execute(&fixture.pool)
    .await?;

    for case in [
        "running",
        "native",
        "accepted",
        "filled",
        "recent",
        "expired",
        "wrong_account",
        "wrong_client",
        "wrong_symbol",
        "wrong_window",
        "stale",
        "future",
        "complete",
    ] {
        let now = test_now_ms()?;
        let created = now - 120_000;
        sqlx::query("UPDATE venue_binance_commands SET command_state='reconcile_required',native_order_id=NULL,accepted_ms=NULL,created_ms=$1,sending_ms=$1,next_reconcile_ms=$2 WHERE command_id=$3")
            .bind(i64::try_from(created)?).bind(i64::try_from(now-1)?).bind(child).execute(&fixture.pool).await?;
        sqlx::query(
            "UPDATE venue_order_mirrors SET filled_quantity='0' WHERE child_client_order_id=$1",
        )
        .bind(child)
        .execute(&fixture.pool)
        .await?;
        sqlx::query("UPDATE venue_leader_bots SET bot_state='draining' WHERE bot_id=$1")
            .bind(bot)
            .execute(&fixture.pool)
            .await?;
        let mut fact = BinanceAbsentLimitOrder {
            trading_account_id: account.into(),
            symbol: "BTC/USDT".parse()?,
            client_order_id: child.into(),
            history_start_ms: created - 60_000,
            history_end_ms: now,
            snapshot_observed_ms: now,
            observed_ms: now,
        };
        match case {
            "running" => {
                sqlx::query("UPDATE venue_leader_bots SET bot_state='running' WHERE bot_id=$1")
                    .bind(bot)
                    .execute(&fixture.pool)
                    .await?;
            }
            "native" => {
                sqlx::query("UPDATE venue_binance_commands SET native_order_id='known-order' WHERE command_id=$1").bind(child).execute(&fixture.pool).await?;
            }
            "accepted" => {
                sqlx::query(
                    "UPDATE venue_binance_commands SET accepted_ms=created_ms WHERE command_id=$1",
                )
                .bind(child)
                .execute(&fixture.pool)
                .await?;
            }
            "filled" => {
                sqlx::query("UPDATE venue_order_mirrors SET filled_quantity='0.00001' WHERE child_client_order_id=$1").bind(child).execute(&fixture.pool).await?;
            }
            "recent" => {
                sqlx::query("UPDATE venue_binance_commands SET sending_ms=$1 WHERE command_id=$2")
                    .bind(i64::try_from(now)?)
                    .bind(child)
                    .execute(&fixture.pool)
                    .await?;
            }
            "expired" => {
                sqlx::query("UPDATE venue_binance_commands SET created_ms=$1 WHERE command_id=$2")
                    .bind(i64::try_from(now - 50 * 60 * 60 * 1000)?)
                    .bind(child)
                    .execute(&fixture.pool)
                    .await?;
            }
            "wrong_account" => fact.trading_account_id = "other".into(),
            "wrong_client" => fact.client_order_id = "other".into(),
            "wrong_symbol" => fact.symbol = "ETH/USDT".parse()?,
            "wrong_window" => fact.history_start_ms += 1,
            "stale" => fact.snapshot_observed_ms -= 3_001,
            "future" => fact.observed_ms += 10_000,
            _ => {}
        }
        let mut exchange = MockBinanceExecution::default();
        exchange.set_absent_limit(fact);
        let mut runtime = BinanceExecutorRuntime::new(
            PgExecutorStore::new(fixture.pool.clone()),
            exchange,
            ExecutorSecretProvider::new(
                fixture.pool.clone(),
                CredentialCipher::from_key(&[42; 32])?,
            ),
        );
        runtime.recover_once().await?;
        let state = command_state(&fixture.pool, child).await?;
        assert_eq!(
            state,
            if case == "complete" {
                "cancelled"
            } else {
                "reconcile_required"
            },
            "{case}"
        );
        if case == "complete" {
            assert_eq!(runtime.recover_once().await?, 0);
        }
    }
    let (state, native, evidence): (String,Option<String>,serde_json::Value) = sqlx::query_as(
        "SELECT command_state,native_order_id,mirror_stop_readback FROM venue_binance_commands WHERE command_id=$1")
        .bind(child).fetch_one(&fixture.pool).await?;
    assert_eq!(state, "cancelled");
    assert!(native.is_none());
    assert_eq!(evidence["kind"], "mirror_stop_confirmed_absent");
    assert_eq!(evidence["exact_absence_reads"], 2);
    assert!(
        sqlx::query(
            "UPDATE venue_binance_commands SET mirror_stop_readback=NULL WHERE command_id=$1"
        )
        .bind(child)
        .execute(&fixture.pool)
        .await
        .is_err()
    );
    assert!(
        sqlx::query(
            "UPDATE venue_binance_commands SET command_state='pending' WHERE command_id=$1"
        )
        .bind(child)
        .execute(&fixture.pool)
        .await
        .is_err()
    );
    venue_control::install_control_schema(&fixture.pool).await?;
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(venue_control::order_mirror::run_order_mirror(
        fixture.pool.clone(),
        venue_control::executor_runtime::CommandWake::new(),
        receiver,
    ));
    wait_count(
        &fixture.pool,
        "SELECT count(*) FROM venue_leader_bots WHERE bot_state='stopped'",
        1,
    )
    .await?;
    shutdown.send(true)?;
    task.await??;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM venue_binance_commands")
            .fetch_one(&fixture.pool)
            .await?,
        2
    );
    Ok(())
}
