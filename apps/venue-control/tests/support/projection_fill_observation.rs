use super::*;

#[tokio::test]
async fn rest_fill_received_during_snapshot_repairs_only_invalid_observation()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    fixture.migrate_twice().await?;
    let result = exercise(&fixture.pool).await;
    fixture.cleanup().await?;
    result
}

async fn exercise(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let user = id(970);
    let account = id(971);
    let credential = id(972);
    seed_verified_account(pool, &user, &account, &credential, 73).await?;
    sqlx::query("UPDATE venue_api_credentials SET verification_json='{\"verification\":\"verified\"}'::jsonb WHERE credential_id=$1")
        .bind(&credential).execute(pool).await?;
    let symbol: venue_domain::Symbol = "BTC/USDT".parse()?;
    let source = ActiveProjectionSource {
        kol_user_id: None,
        owner_user_id: user,
        credential_id: credential,
        trading_account_id: account.clone(),
        symbols: [symbol.clone()].into_iter().collect(),
        previous_fills_cursor: None,
    };
    let store = BinancePrivateProjectionStore::new(pool.clone());
    store
        .subscribe(
            &source.owner_user_id,
            &source.credential_id,
            &[symbol.clone()],
            100,
        )
        .await?;
    // The REST fetch begins at 100, the fill occurs at 110, and the response arrives at 120.
    let event = private_stream_fill("during-fetch", 111, Decimal::new(2, 3), OrderState::Filled)?;
    let snapshot = SignedAccountSnapshot::complete_with_fills(
        GatewayBinding::new(VenueId::Binance, GatewayMode::Live, account.clone(), symbol)?,
        100,
        1,
        3,
        1,
        SignedAccountPositionMode::Hedge,
        Vec::new(),
        Vec::new(),
        vec![event.fill.clone()],
        "binance-fills-v1|BTCUSDT,100,111,110".to_owned(),
        Vec::new(),
    )?;
    assert_eq!(
        store.persist(&source, &snapshot, 109).await,
        Err(PrivateProjectionError::Invalid)
    );
    let projection = store.persist(&source, &snapshot, 120).await?;
    assert!(
        store
            .load_healthy_owned(&source.owner_user_id, &source.credential_id)
            .await?
            .is_some()
    );
    store.invalidate_stream(&source.credential_id).await?;
    assert!(
        store
            .load_healthy_owned(&source.owner_user_id, &source.credential_id)
            .await?
            .is_none()
    );
    assert!(
        store
            .load_owned(&source.owner_user_id, &source.credential_id)
            .await?
            .is_some(),
        "disconnection preserves read-only historical facts"
    );
    store.persist(&source, &snapshot, 120).await?;
    assert_eq!(
        store.stream_surface_settled(&source, &snapshot).await?,
        Some(true)
    );
    assert_eq!(
        projection.observed_ms, 100,
        "do not falsely freshen order/position evidence"
    );
    assert_eq!(observation(pool, &account).await?, 120);
    store.persist(&source, &snapshot, 130).await?;
    assert_eq!(
        observation(pool, &account).await?,
        120,
        "valid first observation is immutable"
    );
    // Simulate the historical bug, then let the signed replay repair it without Grid reset.
    sqlx::query(
        "UPDATE venue_binance_account_fills SET observed_ms=100 WHERE trading_account_id=$1",
    )
    .bind(&account)
    .execute(pool)
    .await?;
    let sources = store.active_sources(131).await?;
    assert_eq!(sources.len(), 1);
    assert_eq!(
        sources[0].previous_fills_cursor.as_deref(),
        Some("binance-fills-v1|BTCUSDT,109,,")
    );
    store.persist(&source, &snapshot, 140).await?;
    assert_eq!(observation(pool, &account).await?, 140);
    let sources = store.active_sources(141).await?;
    assert_eq!(
        sources[0].previous_fills_cursor.as_deref(),
        Some("binance-fills-v1|BTCUSDT,100,111,110")
    );
    // A later authenticated duplicate may add stream context but must not change allocation identity.
    // Older REST rows retain the exchange's trailing zeros; JSON string equality is not fill identity.
    sqlx::query("UPDATE venue_binance_account_fills SET fill_json=jsonb_set(jsonb_set(fill_json,'{price}','\"50000.0000\"'::jsonb),'{quantity}','\"0.001000\"'::jsonb) WHERE trading_account_id=$1")
        .bind(&account).execute(pool).await?;
    let mut later_event = event;
    later_event.received_at_ms = 150;
    store.persist_stream_fill(&source, &later_event).await?;
    assert_eq!(observation(pool, &account).await?, 140);
    later_event.received_at_ms = 160;
    store.persist_stream_fill(&source, &later_event).await?;
    assert_eq!(observation(pool, &account).await?, 140);
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM venue_binance_account_fills WHERE trading_account_id=$1",
    )
    .bind(&account)
    .fetch_one(pool)
    .await?;
    assert_eq!(count, 1);
    let context: Option<i64> = sqlx::query_scalar("SELECT stream_private_generation FROM venue_binance_account_fills WHERE trading_account_id=$1")
        .bind(&account).fetch_one(pool).await?;
    assert_eq!(context, Some(3));
    for field in ["quantity", "price", "time", "order", "side", "leg", "maker"] {
        let mut conflicting = later_event.clone();
        match field {
            "quantity" => conflicting.fill.quantity += Decimal::new(1, 3),
            "price" => conflicting.fill.price = Price::new(Decimal::new(50_001, 0))?,
            "time" => conflicting.fill.exchange_time_ms = Some(109),
            "order" => conflicting.fill.order_id = "other-order".to_owned(),
            "side" => conflicting.fill.side = OrderSide::Sell,
            "leg" => conflicting.fill.position_side = FieldState::Known(PositionSide::Short),
            "maker" => conflicting.fill.maker = FieldState::Known(false),
            _ => return Err("unknown test case".into()),
        }
        assert_eq!(
            store.persist_stream_fill(&source, &conflicting).await,
            Err(PrivateProjectionError::Invalid),
            "{field}"
        );
    }
    Ok(())
}

async fn observation(pool: &PgPool, account: &str) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT observed_ms FROM venue_binance_account_fills WHERE trading_account_id=$1",
    )
    .bind(account)
    .fetch_one(pool)
    .await
}
