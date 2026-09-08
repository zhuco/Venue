use super::*;

use venue_control::executor_store::PgExecutorStore;
use venue_control::inventory_mm::{InventoryMmStore, InventoryMmStoreError, MmCommandIntent};
use venue_control_protocol::inventory_mm::{
    InventoryMmAction, InventoryMmConfig, InventoryMmCreateRequest, InventoryMmLifecycleRequest,
};
use venue_domain::{OrderSide, PositionSide, Symbol};

fn config() -> InventoryMmConfig {
    InventoryMmConfig {
        symbol: "BTC/USDT".parse::<Symbol>().expect("fixture symbol"),
        order_notional: Decimal::new(100, 0),
        base_half_spread_bps: Decimal::new(5, 0),
        volatility_multiplier: Decimal::new(1, 0),
        inventory_skew_bps: Decimal::new(5, 0),
        max_leg_notional: Decimal::new(200, 0),
        max_gross_notional: Decimal::new(400, 0),
        max_net_notional: Decimal::new(200, 0),
        max_loss_quote: Decimal::new(100, 0),
        max_drawdown_quote: Decimal::new(100, 0),
        min_available_margin: Decimal::new(10, 0),
        required_leverage: 3,
        quote_refresh_ms: 1_000,
    }
}

#[tokio::test]
async fn inventory_mm_postgres_lifecycle_commands_and_fences()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&database_url).await?;
    fixture.migrate_twice().await?;
    let user = id(9101);
    let account = id(9201);
    let credential = id(9301);
    seed_verified_account(&fixture.pool, &user, &account, &credential, 241).await?;
    let now = test_now_ms()?;
    sqlx::query("UPDATE venue_api_credentials SET verification_json=verification_json || $2::jsonb WHERE credential_id=$1")
        .bind(&credential)
        .bind(serde_json::json!({"credential_id":credential,"label":"fixture","venue":"binance","masked_key":"***","trading_account_id":account,"verification":"verified","verified_ms":now-1,"expires_ms":now+60000,"api_reachable":true,"dual_position":true,"account_mode":"portfolio_margin","has_exposure":false,"equity":"1000","available_margin":"1000","balance_observed_ms":now-1}))
        .execute(&fixture.pool).await?;
    let projection = serde_json::json!({"fills_cursor":"fixture","stream_healthy":true,"projection":{"schema_version":1,"credential_id":credential,"trading_account_id":account,"observed_ms":now,"persisted_ms":now,"private_generation":7,"position_mode":"hedge","positions":[],"open_orders":[],"conditional_orders":[],"fills":[],"assets":[]}});
    sqlx::query("INSERT INTO venue_binance_account_projections(credential_id,owner_user_id,trading_account_id,observed_ms,persisted_ms,private_generation,projection_json) VALUES($1,$2,$3,$4,$4,7,$5)")
        .bind(&credential).bind(&user).bind(&account).bind(i64::try_from(now)?).bind(projection).execute(&fixture.pool).await?;

    let store = InventoryMmStore::new(fixture.pool.clone());
    let request = InventoryMmCreateRequest {
        schema_version: 1,
        request_id: id(9401),
        credential_id: credential.clone(),
        config: config(),
    };
    let instance = store.create(&user, &id(9501), &request, now).await?;
    assert_eq!(
        instance.state,
        venue_control_protocol::inventory_mm::InventoryMmState::Stopped
    );
    assert_eq!(
        store
            .create(&user, &id(9501), &request, now + 1)
            .await?
            .revision,
        1
    );

    let start = InventoryMmLifecycleRequest {
        schema_version: 1,
        request_id: id(9601),
        instance_id: instance.instance_id.clone(),
        expected_revision: 1,
        action: InventoryMmAction::Start,
    };
    let fresh_resume = InventoryMmLifecycleRequest {
        request_id: id(9640),
        action: InventoryMmAction::Resume,
        ..start.clone()
    };
    assert!(
        store
            .lifecycle(&user, &fresh_resume, Some((3, now)), now)
            .await
            .is_err(),
        "Resume cannot adopt inventory into an instance which has never run"
    );
    assert!(
        store
            .lifecycle(
                &user,
                &InventoryMmLifecycleRequest {
                    request_id: id(9600),
                    expected_revision: 1,
                    ..start.clone()
                },
                Some((2, now)),
                now
            )
            .await
            .is_err()
    );
    let pending = store.lifecycle(&user, &start, Some((3, now)), now).await?;
    assert_eq!(
        pending.state,
        venue_control_protocol::inventory_mm::InventoryMmState::StartPending
    );
    let projections =
        venue_control::private_projection::BinancePrivateProjectionStore::new(fixture.pool.clone());
    projections.invalidate_stream(&credential).await?;
    let mut runtime = venue_control::inventory_mm::InventoryMmRuntime::new(
        store.clone(),
        projections.clone(),
        venue_control::executor_secret::ExecutorSecretProvider::new(
            fixture.pool.clone(),
            CredentialCipher::from_key(&[42; 32])?,
        ),
        venue_gateway_binance::BinanceTransportLimits::new(
            std::time::Duration::from_secs(1),
            4096,
        )?,
        venue_control::executor_runtime::CommandWake::default(),
    );
    for _ in 0..2 {
        runtime.run_once().await?;
        let waiting = store.get(&user, &instance.instance_id).await?;
        assert_eq!(
            waiting.state, pending.state,
            "a transient stream gap must not latch startup"
        );
        assert_eq!(waiting.revision, pending.revision);
        assert!(waiting.attention.is_none());
        assert!(
            store.commands(&instance.instance_id).await?.is_empty(),
            "recovery must never enqueue quotes"
        );
        assert!(
            projections
                .load_healthy_owned(&user, &credential)
                .await?
                .is_none()
        );
    }
    sqlx::query("UPDATE venue_binance_account_projections SET projection_json=jsonb_set(projection_json,'{stream_healthy}','true'::jsonb) WHERE credential_id=$1")
        .bind(&credential).execute(&fixture.pool).await?;
    assert!(
        projections
            .load_healthy_owned(&user, &credential)
            .await?
            .is_some()
    );
    store
        .mark_running(&pending, Decimal::new(1000, 0), now + 1)
        .await?;
    let running = store.get(&user, &instance.instance_id).await?;
    assert!(
        store
            .preflight_resume(&user, &instance.instance_id, running.revision, now + 1)
            .await
            .is_err()
    );
    sqlx::query("UPDATE venue_inventory_mm_instances SET instance_state='needs_attention',attention='facts_or_planner_unavailable',last_quote_ms=$2 WHERE instance_id=$1")
        .bind(&instance.instance_id).bind(i64::try_from(now)?).execute(&fixture.pool).await?;
    let inventory = serde_json::json!([{"symbol":"BTC/USDT","position_side":"short","quantity":"0.001","entry_price":"20000","mark_price":"20000"}]);
    sqlx::query("UPDATE venue_binance_account_projections SET projection_json=jsonb_set(projection_json,'{projection,positions}',$2) WHERE credential_id=$1")
        .bind(&credential).bind(inventory).execute(&fixture.pool).await?;
    assert!(
        !store
            .preflight(&user, &instance.instance_id, running.revision, now + 1)
            .await?
            .ready,
        "ordinary Start must still require flat inventory"
    );
    assert!(
        store
            .preflight_resume(&user, &instance.instance_id, running.revision, now + 1)
            .await?
            .ready
    );
    let resume = InventoryMmLifecycleRequest {
        request_id: id(9641),
        action: InventoryMmAction::Resume,
        expected_revision: running.revision,
        ..start.clone()
    };
    assert!(
        store
            .lifecycle(&user, &resume, None, now + 1)
            .await
            .is_err()
    );
    assert!(
        store
            .lifecycle(&id(9991), &resume, Some((3, now)), now + 1)
            .await
            .is_err()
    );
    let resumed = store
        .lifecycle(&user, &resume, Some((3, now)), now + 1)
        .await?;
    assert_eq!(resumed.baseline_equity, running.baseline_equity);
    assert_eq!(resumed.peak_equity, running.peak_equity);
    assert_eq!(resumed.config, running.config);
    assert!(resumed.attention.is_none());
    assert_eq!(
        store
            .lifecycle(&user, &resume, Some((3, now)), now + 1)
            .await?,
        resumed,
        "the same resume identity is idempotent"
    );
    sqlx::query("UPDATE venue_binance_account_projections SET projection_json=jsonb_set(projection_json,'{projection,positions}','[]') WHERE credential_id=$1")
        .bind(&credential).execute(&fixture.pool).await?;
    store
        .mark_running(&resumed, Decimal::new(1000, 0), now + 1)
        .await?;
    let running = store.get(&user, &instance.instance_id).await?;

    // Strategy allocation is user-managed. A same-symbol Grid and second MM can be started
    // on this flat signed surface; no runtime or physical exchange is started by the fixture.
    start_peer_grid(
        &fixture.pool,
        &user,
        &account,
        &credential,
        &id(9511),
        now + 1,
    )
    .await?;
    let peer_request = InventoryMmCreateRequest {
        request_id: id(9411),
        ..request.clone()
    };
    let peer = store
        .create(&user, &id(9512), &peer_request, now + 1)
        .await?;
    let peer_started = store
        .lifecycle(
            &user,
            &InventoryMmLifecycleRequest {
                request_id: id(9612),
                instance_id: peer.instance_id.clone(),
                expected_revision: peer.revision,
                ..start.clone()
            },
            Some((3, now)),
            now + 1,
        )
        .await?;
    assert_eq!(
        peer_started.state,
        venue_control_protocol::inventory_mm::InventoryMmState::StartPending
    );

    let intents = [
        MmCommandIntent::Limit {
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::new(1, 3),
            price: Decimal::new(60000, 0),
            reducing: false,
        },
        MmCommandIntent::Limit {
            side: OrderSide::Sell,
            position_side: PositionSide::Short,
            quantity: Decimal::new(1, 3),
            price: Decimal::new(60010, 0),
            reducing: false,
        },
    ];
    assert!(!store.enqueue(&running, &intents, 99, now, now + 2).await?);
    assert!(store.enqueue(&running, &intents, 7, now, now + 2).await?);
    let commands = store.commands(&running.instance_id).await?;
    assert_eq!(commands.len(), 2);
    let first = &commands[0];
    let first_id = first.client_order_id.clone();
    // A temporary unknown result holds the account without permanently stopping MM.
    let current = store.get(&user, &running.instance_id).await?;
    let sent = now + 2;
    sqlx::query("UPDATE venue_binance_commands SET command_state='reconcile_required',sending_ms=$2 WHERE client_order_id=$1")
        .bind(&first_id).bind(i64::try_from(sent)?).execute(&fixture.pool).await?;
    // Keep this runtime probe offline; coexistence admission above is already verified.
    sqlx::query(
        "UPDATE venue_inventory_mm_instances SET instance_state='stopped' WHERE instance_id=$1",
    )
    .bind(&peer.instance_id)
    .execute(&fixture.pool)
    .await?;
    runtime.run_once().await?;
    assert_eq!(store.get(&user, &current.instance_id).await?, current);
    assert!(
        !store
            .latch_stalled_reconciliation(&current, sent + 119_999)
            .await?
    );
    assert_eq!(store.get(&user, &current.instance_id).await?, current);
    assert!(!store.enqueue(&current, &intents, 7, now, now + 3).await?);
    assert_eq!(store.commands(&current.instance_id).await?.len(), 2);

    // Signed recovery wins even if the timeout turn began with an old unknown snapshot.
    let mut recovery = fixture.pool.begin().await?;
    sqlx::query(
        "UPDATE venue_binance_commands SET command_state='reconciled' WHERE client_order_id=$1",
    )
    .bind(&first_id)
    .execute(&mut *recovery)
    .await?;
    let waiter_store = store.clone();
    let waiter_instance = current.clone();
    let mut waiter = tokio::spawn(async move {
        waiter_store
            .latch_stalled_reconciliation(&waiter_instance, sent + 120_000)
            .await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut waiter)
            .await
            .is_err()
    );
    recovery.commit().await?;
    assert!(!waiter.await??);
    assert_eq!(store.get(&user, &current.instance_id).await?, current);

    // Retry timestamps and a newly constructed store cannot postpone a genuinely stuck command.
    sqlx::query("UPDATE venue_binance_commands SET command_state='reconcile_required',updated_ms=$2 WHERE client_order_id=$1")
        .bind(&first_id).bind(i64::try_from(sent + 119_999)?).execute(&fixture.pool).await?;
    let restarted = InventoryMmStore::new(fixture.pool.clone());
    assert!(
        restarted
            .latch_stalled_reconciliation(&current, sent + 120_000)
            .await?
    );
    let latched = restarted.get(&user, &current.instance_id).await?;
    assert_eq!(
        latched.attention.as_deref(),
        Some("command_reconcile_timeout")
    );
    assert_eq!(
        latched.state,
        venue_control_protocol::inventory_mm::InventoryMmState::NeedsAttention
    );
    assert!(
        !restarted
            .latch_stalled_reconciliation(&latched, sent + 120_001)
            .await?
    );
    assert!(matches!(
        restarted.enqueue(&latched, &intents, 7, now, now + 3).await,
        Err(InventoryMmStoreError::Conflict)
    ));
    let original = restarted.commands(&current.instance_id).await?;
    assert_eq!(original.len(), 2);
    assert!(original.iter().any(|c| c.client_order_id == first_id
        && c.state == venue_control_protocol::kol::ExecutorCommandState::ReconcileRequired));

    // Explicit Stop remains Stop even while an original request exceeds the recovery deadline.
    sqlx::query("UPDATE venue_inventory_mm_instances SET instance_state='stop_pending',attention=NULL WHERE instance_id=$1")
        .bind(&current.instance_id).execute(&fixture.pool).await?;
    let stopping = store.get(&user, &current.instance_id).await?;
    assert!(
        !store
            .latch_stalled_reconciliation(&stopping, sent + 120_002)
            .await?
    );
    assert_eq!(store.get(&user, &current.instance_id).await?, stopping);
    // Restore this isolated fixture for the remaining lifecycle and dispatch cases.
    sqlx::query("UPDATE venue_inventory_mm_instances SET instance_state='running',attention=NULL WHERE instance_id=$1")
        .bind(&current.instance_id).execute(&fixture.pool).await?;
    sqlx::query("UPDATE venue_binance_commands SET command_state='reconciled',native_order_id='native-1',updated_ms=created_ms WHERE client_order_id=$1").bind(&first_id).execute(&fixture.pool).await?;
    let second_id = commands[1].client_order_id.clone();
    sqlx::query("UPDATE venue_binance_commands SET command_state='cancelled',updated_ms=created_ms WHERE client_order_id=$1").bind(&second_id).execute(&fixture.pool).await?;
    let running = store.get(&user, &running.instance_id).await?;
    assert!(matches!(
        store
            .enqueue(
                &running,
                &[MmCommandIntent::Cancel {
                    target_client_order_id: first_id.clone(),
                    native_order_id: Some("wrong-native".into())
                }],
                7,
                now,
                now + 3
            )
            .await,
        Err(InventoryMmStoreError::Conflict)
    ));
    assert!(
        store
            .enqueue(
                &running,
                &[MmCommandIntent::Cancel {
                    target_client_order_id: first_id.clone(),
                    native_order_id: Some("native-1".into())
                }],
                7,
                now,
                now + 3
            )
            .await?
    );
    let cancel = store
        .commands(&running.instance_id)
        .await?
        .into_iter()
        .find(|c| {
            c.state == venue_control_protocol::kol::ExecutorCommandState::Pending
                && matches!(c.intent, MmCommandIntent::Cancel { .. })
        })
        .expect("cancel command");
    sqlx::query("UPDATE venue_binance_commands SET command_state='reconciled',updated_ms=created_ms WHERE command_id=$1")
        .bind(cancel.command_id).execute(&fixture.pool).await?;
    // A reconciled cancel permits the next quote generation; a stale generation is rejected.
    let future = now + 10;
    sqlx::query("UPDATE venue_binance_account_projections SET observed_ms=$2,persisted_ms=$2,projection_json=jsonb_set(jsonb_set(projection_json,'{projection,observed_ms}',to_jsonb($2::bigint)),'{projection,persisted_ms}',to_jsonb($2::bigint)) WHERE credential_id=$1")
        .bind(&credential).bind(i64::try_from(future)?).execute(&fixture.pool).await?;
    let running = store.get(&user, &running.instance_id).await?;
    sqlx::query(
        "UPDATE venue_binance_commands SET command_state='accepted' WHERE client_order_id=$1",
    )
    .bind(&first_id)
    .execute(&fixture.pool)
    .await?;
    assert!(
        !store
            .enqueue(&running, &[intents[0].clone()], 7, future, future + 1)
            .await?
    );
    sqlx::query("UPDATE venue_binance_commands SET command_state='reconciled',updated_ms=created_ms WHERE client_order_id=$1")
        .bind(&first_id).execute(&fixture.pool).await?;
    let running = store.get(&user, &running.instance_id).await?;
    assert!(
        !store
            .enqueue(&running, &[intents[0].clone()], 6, future, future + 1)
            .await?
    );
    assert!(
        store
            .enqueue(&running, &[intents[0].clone()], 7, future, future + 1)
            .await?
    );
    let running = store.get(&user, &running.instance_id).await?;
    let quote = store
        .commands(&running.instance_id)
        .await?
        .into_iter()
        .find(|c| c.state == venue_control_protocol::kol::ExecutorCommandState::Pending)
        .expect("fresh quote");
    let executor = PgExecutorStore::new(fixture.pool.clone());
    sqlx::query("UPDATE venue_binance_commands SET command_state='reconciled',updated_ms=created_ms WHERE client_order_id=$1")
        .bind(&first_id).execute(&fixture.pool).await?;
    sqlx::query("UPDATE venue_binance_commands SET command_state='sending' WHERE command_id=$1")
        .bind(&quote.command_id)
        .execute(&fixture.pool)
        .await?;
    sqlx::query(
        "UPDATE venue_binance_commands SET command_state='accepted' WHERE client_order_id=$1",
    )
    .bind(&first_id)
    .execute(&fixture.pool)
    .await?;
    assert!(
        !executor
            .inventory_mm_dispatch_permitted(&quote.command_id, future + 2)
            .await?
    );
    sqlx::query("UPDATE venue_binance_commands SET command_state='reconciled',updated_ms=created_ms WHERE client_order_id=$1")
        .bind(&first_id).execute(&fixture.pool).await?;
    assert!(
        executor
            .inventory_mm_dispatch_permitted(&quote.command_id, future + 2)
            .await?
    );
    sqlx::query("UPDATE venue_binance_commands SET command_state='pending' WHERE command_id=$1")
        .bind(&quote.command_id)
        .execute(&fixture.pool)
        .await?;
    assert!(
        !executor
            .inventory_mm_dispatch_permitted(&quote.command_id, future + 2)
            .await?
    );
    sqlx::query("UPDATE venue_binance_commands SET command_state='sending' WHERE command_id=$1")
        .bind(&quote.command_id)
        .execute(&fixture.pool)
        .await?;
    assert!(
        !executor
            .inventory_mm_dispatch_permitted(&quote.command_id, future + 6_001)
            .await?
    );
    sqlx::query("UPDATE venue_binance_account_projections SET private_generation=8,projection_json=jsonb_set(projection_json,'{projection,private_generation}',to_jsonb(8::bigint)) WHERE credential_id=$1")
        .bind(&credential).execute(&fixture.pool).await?;
    assert!(
        !executor
            .inventory_mm_dispatch_permitted(&quote.command_id, future + 2)
            .await?
    );
    sqlx::query("UPDATE venue_binance_account_projections SET private_generation=7,projection_json=jsonb_set(projection_json,'{projection,private_generation}',to_jsonb(7::bigint)) WHERE credential_id=$1")
        .bind(&credential).execute(&fixture.pool).await?;
    sqlx::query("UPDATE venue_inventory_mm_instances SET instance_state='needs_attention' WHERE instance_id=$1")
        .bind(&running.instance_id).execute(&fixture.pool).await?;
    assert!(
        !executor
            .inventory_mm_dispatch_permitted(&quote.command_id, future + 2)
            .await?
    );
    sqlx::query("UPDATE venue_binance_commands SET command_state='reconciled',updated_ms=created_ms WHERE command_id=$1")
        .bind(&quote.command_id).execute(&fixture.pool).await?;
    let running = store.get(&user, &running.instance_id).await?;
    assert!(
        store
            .enqueue(
                &running,
                &[MmCommandIntent::Cancel {
                    target_client_order_id: first_id.clone(),
                    native_order_id: Some("native-1".into())
                }],
                7,
                future,
                future + 3
            )
            .await?
    );
    let cancel_gate = store
        .commands(&running.instance_id)
        .await?
        .into_iter()
        .find(|c| {
            c.state == venue_control_protocol::kol::ExecutorCommandState::Pending
                && matches!(c.intent, MmCommandIntent::Cancel { .. })
        })
        .expect("cancel gate command");
    sqlx::query("UPDATE venue_binance_commands SET command_state='sending' WHERE command_id=$1")
        .bind(&cancel_gate.command_id)
        .execute(&fixture.pool)
        .await?;
    assert!(
        executor
            .inventory_mm_dispatch_permitted(&cancel_gate.command_id, future + 4)
            .await?
    );
    sqlx::query("UPDATE venue_binance_commands SET command_state='reconciled',updated_ms=created_ms WHERE command_id=$1")
        .bind(&cancel_gate.command_id).execute(&fixture.pool).await?;
    let running = store.get(&user, &running.instance_id).await?;
    let next_observed = future + 5;
    sqlx::query("UPDATE venue_binance_account_projections SET observed_ms=$2,persisted_ms=$2,projection_json=jsonb_set(jsonb_set(projection_json,'{projection,observed_ms}',to_jsonb($2::bigint)),'{projection,persisted_ms}',to_jsonb($2::bigint)) WHERE credential_id=$1")
        .bind(&credential).bind(i64::try_from(next_observed)?).execute(&fixture.pool).await?;
    sqlx::query(
        "UPDATE venue_inventory_mm_instances SET instance_state='running' WHERE instance_id=$1",
    )
    .bind(&running.instance_id)
    .execute(&fixture.pool)
    .await?;
    let running = store.get(&user, &running.instance_id).await?;
    assert!(
        store
            .enqueue(
                &running,
                &[intents[0].clone()],
                7,
                next_observed,
                next_observed + 1
            )
            .await?
    );
    let running = store.get(&user, &running.instance_id).await?;
    let stop_quote = store
        .commands(&running.instance_id)
        .await?
        .into_iter()
        .find(|c| c.state == venue_control_protocol::kol::ExecutorCommandState::Pending)
        .expect("stop quote");
    sqlx::query(
        "UPDATE venue_inventory_mm_instances SET instance_state='stopped' WHERE instance_id=$1",
    )
    .bind(&running.instance_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query("UPDATE venue_binance_commands SET command_state='sending' WHERE command_id=$1")
        .bind(&stop_quote.command_id)
        .execute(&fixture.pool)
        .await?;
    assert!(
        !executor
            .inventory_mm_dispatch_permitted(&stop_quote.command_id, next_observed + 2)
            .await?
    );
    sqlx::query("UPDATE venue_binance_commands SET command_state='pending' WHERE command_id=$1")
        .bind(&stop_quote.command_id)
        .execute(&fixture.pool)
        .await?;
    sqlx::query(
        "UPDATE venue_inventory_mm_instances SET instance_state='running' WHERE instance_id=$1",
    )
    .bind(&running.instance_id)
    .execute(&fixture.pool)
    .await?;
    let running = store.get(&user, &running.instance_id).await?;
    // One sent/accepted order is retained while the fresh pending quote is cancelled by Stop.
    sqlx::query(
        "UPDATE venue_binance_commands SET command_state='accepted' WHERE client_order_id=$1",
    )
    .bind(&first_id)
    .execute(&fixture.pool)
    .await?;

    let stop = InventoryMmLifecycleRequest {
        schema_version: 1,
        request_id: id(9701),
        instance_id: running.instance_id.clone(),
        expected_revision: running.revision,
        action: InventoryMmAction::Stop,
    };
    let stopped_pending = store
        .lifecycle(&user, &stop, None, next_observed + 3)
        .await?;
    assert_eq!(
        stopped_pending.state,
        venue_control_protocol::inventory_mm::InventoryMmState::StopPending
    );
    let states: Vec<(String, String)> = sqlx::query_as("SELECT client_order_id,command_state FROM venue_binance_commands WHERE inventory_mm_instance_id=$1 ORDER BY client_order_id").bind(&running.instance_id).fetch_all(&fixture.pool).await?;
    assert!(
        states
            .iter()
            .any(|(client, state)| client == &first_id && state == "accepted")
    );
    assert!(
        states
            .iter()
            .any(|(client, state)| client == &stop_quote.client_order_id && state == "cancelled")
    );
    fixture.cleanup().await?;
    Ok(())
}

pub(super) async fn start_peer_grid(
    pool: &PgPool,
    owner: &str,
    account: &str,
    credential: &str,
    instance: &str,
    now: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    use venue_control_protocol::grid::*;
    let grid = venue_control::BinanceGridStore::new(pool.clone());
    let created = grid
        .create_instance(
            owner,
            account,
            instance,
            &GridInstanceCreateRequest {
                schema_version: GRID_SCHEMA_VERSION,
                request_id: instance.to_owned(),
                credential_id: credential.to_owned(),
                symbol: "BTC/USDT".parse()?,
                config: GridConfig {
                    order_notional: Decimal::from(5),
                    spacing_rate: Decimal::new(2, 3),
                    grid_levels: 10,
                    max_total_notional: Decimal::from(100),
                    inventory_risk: None,
                    required_leverage: None,
                    inventory_replenishment: GridInventoryReplenishment {
                        enabled: false,
                        minimum_inventory_notional: Decimal::from(5),
                        target_inventory_notional: Decimal::from(15),
                        max_single_replenishment_notional: Decimal::from(5),
                    },
                    profit_reduction: GridProfitReduction {
                        enabled: false,
                        inventory_equity_multiple: Decimal::from(3),
                        minimum_unrealized_profit_rate: Decimal::new(5, 2),
                        reduction_fraction: Decimal::new(3, 1),
                        max_single_reduce_notional: Decimal::from(25),
                    },
                    reset_policy: GridResetPolicy {
                        stale_market_ms: 5_000,
                        stale_private_ms: 15_000,
                        convergence_timeout_ms: 30_000,
                        max_consecutive_failures: 3,
                    },
                },
            },
            now,
        )
        .await?;
    let started = grid
        .request_lifecycle(
            owner,
            &GridLifecycleRequest {
                schema_version: GRID_SCHEMA_VERSION,
                request_id: instance.to_owned(),
                instance_id: instance.to_owned(),
                expected_revision: created.revision,
                action: GridLifecycleAction::Start,
                risk_confirmed: true,
                positions_remain_acknowledged: false,
            },
            now,
        )
        .await?;
    assert_eq!(started.state, GridInstanceState::StartPending);
    Ok(())
}
