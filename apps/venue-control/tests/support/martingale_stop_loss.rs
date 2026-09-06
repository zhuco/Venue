use super::*;
use venue_control_protocol::support_martingale::{
    MartingaleEntryMode, SupportMartingaleHealth, SupportMartingaleLifecycle,
};

#[tokio::test]
async fn stop_loss_pauses_increases_and_unknown_cannot_be_settled_or_reposted()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let (user, account, credential) = seed(&fixture.pool, VenueId::Bybit, "stop_loss").await?;
    let store = SupportMartingaleStore::new(fixture.pool.clone());
    let symbol: Symbol = "SOL/USDT".parse()?;
    let id = store
        .create(
            &user,
            SupportMartingaleCreateRequest {
                schema_version: SUPPORT_MARTINGALE_SCHEMA_VERSION,
                request_id: "create-stop-test".into(),
                credential_id: credential,
                config: SupportMartingaleConfig {
                    entry_mode: MartingaleEntryMode::Support,
                    symbol_parameters: vec![],
                    reference_venue: VenueId::Binance,
                    execution_venue: VenueId::Bybit,
                    symbols: vec![symbol.clone()],
                    total_budget: Decimal::from(1000),
                    first_order_notional: Decimal::from(5),
                    max_entries: 3,
                    size_multiplier: Decimal::ONE,
                    target_profit_rate: Decimal::new(5, 3),
                    minimum_profit_quote: Decimal::ZERO,
                    max_active_positions: 1,
                },
            },
            1000,
        )
        .await?;
    store
        .lifecycle(
            &user,
            SupportMartingaleLifecycleRequest {
                schema_version: SUPPORT_MARTINGALE_SCHEMA_VERSION,
                request_id: "start-stop-test".into(),
                instance_id: id.clone(),
                expected_revision: 1,
                action: SupportMartingaleAction::Start,
            },
            1001,
        )
        .await?;
    store
        .sync_symbol_facts(
            &user,
            &id,
            &symbol,
            Decimal::from(2),
            Some(Decimal::from(100)),
            Decimal::from(200),
            None,
            None,
            1002,
        )
        .await?;
    let command = ExecutionCommand::MarketReduce(venue_domain::MarketReduceCommand {
        command_id: CommandId::new("stop-test")?,
        client_order_id: CommandId::new("stop-test")?,
        risk_episode_id: CommandId::new("stop-episode")?,
        position_generation: 7,
        owner: OrderOwner {
            strategy_instance_id: id.clone(),
            run_id: "cycle".into(),
            exchange: "bybit".into(),
            account: account.clone(),
            symbol: symbol.clone(),
            purpose: OrderPurpose::Protection,
        },
        position_side: PositionSide::Long,
        side: OrderSide::Sell,
        quantity: Decimal::from(2),
    });
    store
        .enqueue_command(
            &user,
            &id,
            &symbol,
            SupportMartingaleCommandKind::StopLoss,
            Some("cycle"),
            None,
            None,
            "stop-test",
            Decimal::ZERO,
            command,
            1003,
        )
        .await?;
    assert_eq!(
        store.get(&user, &id).await?.lifecycle,
        SupportMartingaleLifecycle::IncreasePaused
    );
    sqlx::query("UPDATE venue_binance_commands SET command_state='reconcile_required' WHERE command_id='stop-test'").execute(&fixture.pool).await?;
    assert!(
        store
            .settle_command(&user, "stop-test", Decimal::ZERO, true, 1004)
            .await
            .is_err()
    );
    assert_eq!(
        store.load_runtime_state(&user, &id).await?.symbols[0]
            .pending_command_id
            .as_deref(),
        Some("stop-test")
    );
    sqlx::query(
        "UPDATE venue_binance_commands SET command_state='rejected' WHERE command_id='stop-test'",
    )
    .execute(&fixture.pool)
    .await?;
    store
        .settle_command(&user, "stop-test", Decimal::ZERO, true, 1005)
        .await?;
    let failed = store.get(&user, &id).await?;
    assert_eq!(failed.health, SupportMartingaleHealth::NeedsAttention);
    assert_eq!(failed.symbols[0].status, "sl_failed");
    assert_eq!(
        failed.symbols[0].health_reason.as_deref(),
        Some("stop_loss_rejected")
    );
    assert_eq!(failed.lifecycle, SupportMartingaleLifecycle::IncreasePaused);
    store
        .sync_symbol_facts(
            &user,
            &id,
            &symbol,
            Decimal::ONE,
            Some(Decimal::from(100)),
            Decimal::from(100),
            None,
            None,
            1006,
        )
        .await?;
    assert_eq!(store.get(&user, &id).await?.symbols[0].status, "sl_failed");
    store
        .mark_health(&id, SupportMartingaleHealth::Healthy, None, 1007)
        .await?;
    let still_failed = store.get(&user, &id).await?;
    assert_eq!(still_failed.health, SupportMartingaleHealth::NeedsAttention);
    assert_eq!(
        still_failed.symbols[0].health_reason.as_deref(),
        Some("stop_loss_rejected")
    );
    fixture.cleanup().await?;
    Ok(())
}
