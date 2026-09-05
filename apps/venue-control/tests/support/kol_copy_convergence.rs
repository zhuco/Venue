use super::*;
use venue_control::{
    executor_exchange::{MarketBaseline, MarketSettlement},
    kol_executor::{BinanceCommandLedgerError, ClaimedBinanceCommand, ClaimedBinanceOrder},
};

fn fill(trade: &str, quantity: Decimal, side: OrderSide, time: u64) -> KolSourceFill {
    KolSourceFill {
        leader_trading_account_id: id(502),
        native_symbol: "BTCUSDT".into(),
        native_trade_id: trade.into(),
        symbol: "BTC/USDT".into(),
        order_side: side,
        position_side: PositionSide::Long,
        quantity,
        price: Decimal::from(50_000),
        occurred_ms: time,
        observed_ms: time + 1,
        payload_digest: [53; 32],
    }
}

async fn settle(
    store: &PgExecutorStore,
    command: &ClaimedBinanceCommand,
    before: Decimal,
    executed: Decimal,
    after: Decimal,
    time: u64,
) -> Result<(), BinanceCommandLedgerError> {
    let ClaimedBinanceOrder::Market { quantity, .. } = command.order else {
        return Err(BinanceCommandLedgerError::Conflict);
    };
    store
        .persist_market_baseline(
            &command.command_id,
            &MarketBaseline {
                before_quantity: before,
                order_quantity: quantity,
                observed_ms: time,
                valid_until_ms: time + 1_000,
            },
        )
        .await?;
    store
        .transition_command_with_readback(
            &command.command_id,
            ExecutorCommandState::Accepted,
            time + 1,
            None,
            Some(&command.command_id),
        )
        .await?;
    store
        .reconcile_with_execution(
            &command.command_id,
            time + 2,
            Some(&command.command_id),
            Some(&MarketSettlement {
                executed_quantity: executed,
                position_quantity: after,
                observed_ms: time + 1,
            }),
        )
        .await
}

fn quantity(command: &ClaimedBinanceCommand) -> Option<(Decimal, bool)> {
    match command.order {
        ClaimedBinanceOrder::Market {
            quantity, reducing, ..
        } => Some((quantity, reducing)),
        _ => None,
    }
}

#[tokio::test]
async fn inflight_fills_partial_execution_and_close_converge_to_actual_quantity()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    fixture.migrate_twice().await?;
    let (relation, account, _) = kol_copy_lifecycle::seeded_copy(&fixture.pool).await?;
    let store = PgExecutorStore::new(fixture.pool.clone());
    let unit = Decimal::new(1, 3);
    let first = store
        .claim_next_command(&account, 20)
        .await?
        .ok_or("first")?;
    assert_eq!(quantity(&first), Some((unit, false)));
    let second_fill = fill("convergence-2", unit * Decimal::from(2), OrderSide::Buy, 21);
    assert!(
        store
            .record_source_fill_and_plan(&id(501), &second_fill, 22)
            .await?
            .is_empty()
    );
    assert!(store.claim_next_command(&account, 23).await?.is_none());
    settle(&store, &first, Decimal::ZERO, unit, unit, 24).await?;
    // Reopening the store loses no dirty work even if the process stopped before planning.
    let restarted = PgExecutorStore::new(fixture.pool.clone());
    assert_eq!(
        restarted.dirty_copy_accounts().await?,
        vec![account.clone()]
    );
    assert!(restarted.plan_dirty_copy_target(&account, 27).await?);
    let second = restarted
        .claim_next_command(&account, 28)
        .await?
        .ok_or("second")?;
    assert_eq!(quantity(&second), Some((unit * Decimal::from(2), false)));
    // A terminal partial fill records the actual one-unit delta, not the requested two units.
    settle(&restarted, &second, unit, unit, unit * Decimal::from(2), 29).await?;
    assert!(restarted.plan_dirty_copy_target(&account, 32).await?);
    let third = restarted
        .claim_next_command(&account, 33)
        .await?
        .ok_or("third")?;
    assert_eq!(quantity(&third), Some((unit, false)));
    let closing = fill(
        "convergence-3",
        unit * Decimal::from(3),
        OrderSide::Sell,
        34,
    );
    assert!(
        restarted
            .record_source_fill_and_plan(&id(501), &closing, 35)
            .await?
            .is_empty()
    );
    settle(
        &restarted,
        &third,
        unit * Decimal::from(2),
        unit,
        unit * Decimal::from(3),
        36,
    )
    .await?;
    assert!(restarted.plan_dirty_copy_target(&account, 39).await?);
    let close = restarted
        .claim_next_command(&account, 40)
        .await?
        .ok_or("close")?;
    assert_eq!(quantity(&close), Some((unit * Decimal::from(3), true)));
    settle(
        &restarted,
        &close,
        unit * Decimal::from(3),
        unit * Decimal::from(3),
        Decimal::ZERO,
        41,
    )
    .await?;
    assert!(!restarted.plan_dirty_copy_target(&account, 44).await?);
    assert!(
        restarted
            .record_source_fill_and_plan(&id(501), &closing, 45)
            .await?
            .is_empty()
    );
    let target: (String, String, String, bool) = sqlx::query_as("SELECT copyable_quantity,target_quantity,observed_quantity,dirty FROM venue_kol_copy_targets WHERE relation_id=$1")
        .bind(&relation).fetch_one(&fixture.pool).await?;
    assert_eq!(target.0.parse::<Decimal>()?, Decimal::ZERO);
    assert_eq!(target.1.parse::<Decimal>()?, Decimal::ZERO);
    assert_eq!(target.2.parse::<Decimal>()?, Decimal::ZERO);
    assert!(!target.3);
    let counts: (i64, i64) = sqlx::query_as("SELECT count(*),count(*) FILTER (WHERE command_state='reconciled') FROM venue_binance_commands WHERE relation_id=$1")
        .bind(&relation).fetch_one(&fixture.pool).await?;
    assert_eq!(counts, (4, 4));
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn copy_rejects_external_drift_and_cannot_settle_without_signed_execution()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    fixture.migrate_twice().await?;
    let (relation, account, _) = kol_copy_lifecycle::seeded_copy(&fixture.pool).await?;
    let store = PgExecutorStore::new(fixture.pool.clone());
    let command = store
        .claim_next_command(&account, 20)
        .await?
        .ok_or("first")?;
    assert!(
        store
            .persist_market_baseline(
                &command.command_id,
                &MarketBaseline {
                    before_quantity: Decimal::ONE,
                    order_quantity: Decimal::new(1, 3),
                    observed_ms: 20,
                    valid_until_ms: 1_020,
                }
            )
            .await
            .is_err()
    );
    store
        .transition_command_with_readback(
            &command.command_id,
            ExecutorCommandState::Accepted,
            21,
            None,
            Some("native"),
        )
        .await?;
    assert!(
        store
            .reconcile_with_execution(&command.command_id, 22, Some("native"), None)
            .await
            .is_err()
    );
    assert_eq!(
        command_state(&fixture.pool, &command.command_id).await?,
        "accepted"
    );
    let observed: String = sqlx::query_scalar(
        "SELECT observed_quantity FROM venue_kol_copy_targets WHERE relation_id=$1",
    )
    .bind(relation)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(observed.parse::<Decimal>()?, Decimal::ZERO);
    fixture.cleanup().await?;
    Ok(())
}
