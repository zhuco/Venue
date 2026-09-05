use super::*;
use venue_control::executor_exchange::AccountBaseline;

pub(super) fn baseline(
    account: &str,
    seed: u8,
    observed_ms: u64,
    quantity: Decimal,
) -> Result<AccountBaseline, Box<dyn std::error::Error>> {
    Ok(AccountBaseline {
        account_identity_hash: [seed; 32],
        snapshot: projection_snapshot(
            account.into(),
            "BTC/USDT".parse()?,
            observed_ms,
            1,
            "binance-fills-v1|BTCUSDT,30,,",
            quantity,
            Decimal::from(50_000),
            false,
        )?,
    })
}

#[tokio::test]
async fn activation_checks_actual_exposure_identity_and_retires_previous_targets()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    fixture.migrate_twice().await?;
    let (relation, account, command) = kol_copy_lifecycle::seeded_copy(&fixture.pool).await?;
    authorize_leader(&fixture.pool, &id(501), &id(502), &id(503)).await?;
    sqlx::query("UPDATE venue_binance_commands SET command_state='cancelled',terminal_ms=30,updated_ms=30 WHERE command_id=$1")
        .bind(&command).execute(&fixture.pool).await?;
    sqlx::query("UPDATE venue_kol_follow_relations SET relation_state='paused',active_slot=NULL,revision=2 WHERE relation_id=$1")
        .bind(&relation).execute(&fixture.pool).await?;
    sqlx::query("UPDATE venue_api_credentials SET verification_json='{\"verification\":\"verified\"}'::jsonb")
        .execute(&fixture.pool).await?;
    sqlx::query("INSERT INTO venue_kol_activation_requests (relation_id,request_id,relation_revision,request_state,requested_ms,updated_ms) VALUES ($1,$2,2,'pending',30,30)")
        .bind(&relation).bind(id(509)).execute(&fixture.pool).await?;
    let store = PgExecutorStore::new(fixture.pool.clone());
    let activation = store
        .pending_activations(30)
        .await?
        .pop()
        .ok_or("no activation")?;
    let leader = baseline(&id(502), 51, 30, Decimal::ONE)?;
    let flat = baseline(&account, 52, 30, Decimal::ZERO)?;
    let exposed = baseline(&account, 52, 30, Decimal::new(1, 3))?;
    assert!(
        store
            .complete_activation(&activation, &leader, &exposed, 31)
            .await
            .is_err()
    );
    let mut other_symbol = flat.clone();
    other_symbol.snapshot = SignedAccountSnapshot::complete(
        flat.snapshot.binding().clone(),
        30,
        1,
        1,
        1,
        SignedAccountPositionMode::Hedge,
        Vec::new(),
        vec![SignedAccountPositionFact {
            symbol: "ETH/USDT".parse()?,
            position_side: PositionSide::Short,
            quantity: Decimal::ONE,
            entry_price: Some(Decimal::from(2_000)),
            mark_price: Some(Decimal::from(2_000)),
        }],
        "cursor".into(),
        Vec::new(),
    )?;
    assert!(
        store
            .complete_activation(&activation, &leader, &other_symbol, 31)
            .await
            .is_err()
    );
    for family in [
        venue_domain::domain::NativeOrderFamily::UmOrder,
        venue_domain::domain::NativeOrderFamily::UmAlgo,
    ] {
        let mut ordered = flat.clone();
        ordered.snapshot = SignedAccountSnapshot::complete(
            flat.snapshot.binding().clone(),
            30,
            1,
            1,
            1,
            SignedAccountPositionMode::Hedge,
            vec![venue_execution::SignedAccountOrderFact {
                client_order_id: "external".into(),
                venue_order_id: Some("1".into()),
                symbol: "ETH/USDT".parse()?,
                family,
                side: OrderSide::Sell,
                position_side: PositionSide::Short,
                quantity: Decimal::ONE,
                limit_price: Some(Decimal::from(2_000)),
                time_in_force: None,
                created_at_ms: Some(1),
                reduce_only: false,
                owner: None,
                external: true,
                state: Some(OrderState::New),
                filled_quantity: Some(Decimal::ZERO),
            }],
            Vec::new(),
            "cursor".into(),
            Vec::new(),
        )?;
        assert!(
            store
                .complete_activation(&activation, &leader, &ordered, 31)
                .await
                .is_err()
        );
    }
    let wrong_identity = baseline(&account, 99, 30, Decimal::ZERO)?;
    assert!(
        store
            .complete_activation(&activation, &leader, &wrong_identity, 31)
            .await
            .is_err()
    );
    assert!(
        store
            .complete_activation(&activation, &leader, &flat, 31_000)
            .await
            .is_err()
    );
    let mut old_request = activation.clone();
    old_request.request_id = id(599);
    assert!(
        store
            .complete_activation(&old_request, &leader, &flat, 31)
            .await
            .is_err()
    );
    store
        .reject_activation(&old_request, 31, "baseline_failed")
        .await?;
    let request_state: String = sqlx::query_scalar(
        "SELECT request_state FROM venue_kol_activation_requests WHERE relation_id=$1",
    )
    .bind(&relation)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(request_state, "pending");
    let mut wrong_credential = activation.clone();
    wrong_credential.leader_credential_id = id(598);
    assert!(
        store
            .complete_activation(&wrong_credential, &leader, &flat, 31)
            .await
            .is_err()
    );
    sqlx::query("INSERT INTO venue_order_mirrors(mirror_id,bot_id,bot_revision,permission_revision,relation_id,relation_revision,source_order_id,source_client_order_id,symbol,source_order_json,child_sequence,child_client_order_id,child_quantity,mirror_state,created_ms,updated_ms) VALUES($1,$2,1,1,$3,1,'source','source-client','BTC/USDT','{}',1,$1,'1','live',1,1)")
        .bind(id(597)).bind(id(501)).bind(&relation).execute(&fixture.pool).await?;
    assert!(
        store
            .complete_activation(&activation, &leader, &flat, 31)
            .await
            .is_err()
    );
    sqlx::query("UPDATE venue_order_mirrors SET mirror_state='terminal' WHERE mirror_id=$1")
        .bind(id(597))
        .execute(&fixture.pool)
        .await?;
    // A nonzero KOL baseline is allowed and is not copied. Only the flat follower starts anew.
    store
        .complete_activation(&activation, &leader, &flat, 32)
        .await?;
    let target: (String, String, String, i64, bool) = sqlx::query_as("SELECT copyable_quantity,target_quantity,observed_quantity,target_revision,dirty FROM venue_kol_copy_targets WHERE relation_id=$1")
        .bind(&relation).fetch_one(&fixture.pool).await?;
    assert_eq!(target, ("0".into(), "0".into(), "0".into(), 2, false));
    assert_eq!(command_state(&fixture.pool, &command).await?, "cancelled");
    assert!(!store.plan_dirty_copy_target(&account, 33).await?);
    let persisted: serde_json::Value = sqlx::query_scalar(
        "SELECT baseline_json FROM venue_kol_follow_relations WHERE relation_id=$1",
    )
    .bind(&relation)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(persisted["target_model"], 2);
    assert_eq!(persisted["baseline_ms"], 32);
    assert!(
        persisted["leader_positions"]
            .as_array()
            .is_some_and(|values| !values.is_empty())
    );
    fixture.cleanup().await?;
    Ok(())
}

pub(super) async fn authorize_leader(
    pool: &PgPool,
    user: &str,
    account: &str,
    credential: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    venue_control::leader_bot_admin::set_permission(pool, user, true, 0, "fixture", 1).await?;
    sqlx::query("INSERT INTO venue_leader_bots(bot_id,owner_user_id,trading_account_id,credential_id,create_request_id,bot_name,bot_description,strategy_capital,bot_state,revision,permission_revision,started_ms,created_ms,updated_ms) VALUES ($1,$1,$2,$3,$1,'Fixture KOL','','100','running',1,1,1,1,1)")
        .bind(user).bind(account).bind(credential).execute(pool).await?;
    Ok(())
}
