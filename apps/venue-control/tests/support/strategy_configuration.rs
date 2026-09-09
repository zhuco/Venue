use super::*;
use venue_control::multi_venue_grid::{GridDepthUpdate, StrategyGridConfig, StrategyGridStore};
use venue_control_protocol::support_martingale::SupportMartingaleConfigUpdateRequest;

#[tokio::test]
async fn stopped_configuration_updates_preserve_scope_limits_and_revision()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let (user, _account, credential) = seed(&fixture.pool, VenueId::Gate, "grid_depth").await?;
    sqlx::query("UPDATE venue_api_credentials SET strategy_limits=$1 WHERE credential_id=$2")
        .bind(serde_json::json!({"max_order_notional":"10","max_symbol_notional":"100"}))
        .bind(&credential)
        .execute(&fixture.pool)
        .await?;
    let grids = StrategyGridStore::new(fixture.pool.clone());
    let config: StrategyGridConfig =
        serde_json::from_str(include_str!("../fixtures/multi_venue_grid.json"))?;
    let id = grids.create(&user, &credential, config, 1000).await?;
    assert!(
        grids
            .update_depth(
                &user,
                &id,
                GridDepthUpdate {
                    expected_revision: 1,
                    grid_count: 3
                },
                1001
            )
            .await
            .is_err()
    );
    sqlx::query("UPDATE venue_strategy_grids SET lifecycle='stopped' WHERE instance_id=$1")
        .bind(&id)
        .execute(&fixture.pool)
        .await?;
    assert!(
        grids
            .update_depth(
                "wrong-owner",
                &id,
                GridDepthUpdate {
                    expected_revision: 1,
                    grid_count: 3
                },
                1002
            )
            .await
            .is_err()
    );
    assert_eq!(
        grids
            .update_depth(
                &user,
                &id,
                GridDepthUpdate {
                    expected_revision: 1,
                    grid_count: 3
                },
                1003
            )
            .await?,
        2
    );
    assert_eq!(
        grids
            .update_depth(
                &user,
                &id,
                GridDepthUpdate {
                    expected_revision: 1,
                    grid_count: 3
                },
                1004
            )
            .await?,
        2
    );
    assert!(
        grids
            .update_depth(
                &user,
                &id,
                GridDepthUpdate {
                    expected_revision: 1,
                    grid_count: 2
                },
                1005
            )
            .await
            .is_err()
    );
    let (user, _account, credential) =
        seed(&fixture.pool, VenueId::Bitget, "martingale_config").await?;
    let store = SupportMartingaleStore::new(fixture.pool.clone());
    let config = SupportMartingaleConfig {
        entry_mode: Default::default(),
        allow_btc_neutral: false,
        symbol_parameters: vec![],
        reference_venue: VenueId::Binance,
        execution_venue: VenueId::Bitget,
        symbols: vec!["DOGE/USDT".parse()?],
        total_budget: Decimal::from(20),
        first_order_notional: Decimal::from(5),
        max_entries: 3,
        size_multiplier: Decimal::ONE,
        target_profit_rate: Decimal::new(5, 3),
        minimum_profit_quote: Decimal::new(2, 2),
        max_active_positions: 1,
    };
    let id = store
        .create(
            &user,
            SupportMartingaleCreateRequest {
                schema_version: SUPPORT_MARTINGALE_SCHEMA_VERSION,
                request_id: "create-config".into(),
                credential_id: credential,
                config: config.clone(),
            },
            1100,
        )
        .await?;
    let mut request = SupportMartingaleConfigUpdateRequest {
        schema_version: SUPPORT_MARTINGALE_SCHEMA_VERSION,
        request_id: "change-config".into(),
        instance_id: id.clone(),
        expected_revision: 1,
        config,
    };
    request.config.allow_btc_neutral = true;
    assert!(
        store
            .update_config("wrong-owner", request.clone(), 1101)
            .await
            .is_err()
    );
    sqlx::query(
        "UPDATE venue_support_martingale_symbol_states SET quantity=1 WHERE instance_id=$1",
    )
    .bind(&id)
    .execute(&fixture.pool)
    .await?;
    assert!(
        store
            .update_config(&user, request.clone(), 1102)
            .await
            .is_err()
    );
    sqlx::query(
        "UPDATE venue_support_martingale_symbol_states SET quantity=0 WHERE instance_id=$1",
    )
    .bind(&id)
    .execute(&fixture.pool)
    .await?;
    assert_eq!(store.update_config(&user, request.clone(), 1103).await?, 2);
    assert_eq!(store.update_config(&user, request.clone(), 1104).await?, 2);
    request.config.first_order_notional = Decimal::from(6);
    assert!(
        store
            .update_config(&user, request.clone(), 1105)
            .await
            .is_err()
    );
    request.request_id = "bad-size-change".into();
    request.expected_revision = 2;
    assert!(
        store
            .update_config(&user, request.clone(), 1106)
            .await
            .is_err()
    );
    request.config.first_order_notional = Decimal::from(5);
    request.config.allow_btc_neutral = false;
    sqlx::query(
        "UPDATE venue_support_martingale_instances SET lifecycle='running' WHERE instance_id=$1",
    )
    .bind(&id)
    .execute(&fixture.pool)
    .await?;
    assert!(store.update_config(&user, request, 1107).await.is_err());
    assert!(store.get(&user, &id).await?.config.allow_btc_neutral);
    fixture.cleanup().await?;
    Ok(())
}
