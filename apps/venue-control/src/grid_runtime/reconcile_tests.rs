use super::*;
use crate::accounts::test_support::{Fixture, TestResult};
use crate::{BinanceCommandLedger, kol_executor::ClaimedBinanceOrder};
use venue_control_protocol::grid::{
    GRID_SCHEMA_VERSION, GridConfig, GridInstanceCreateRequest, GridInventoryReplenishment,
    GridLifecycleAction, GridLifecycleRequest, GridOrderSemanticKey, GridProfitReduction,
    GridResetPolicy,
};
use venue_control_protocol::kol::{
    TERMINAL_PROJECTION_SCHEMA_VERSION, TerminalOrderState, TerminalPosition,
};

const OWNER: &str = "00000000-0000-4000-8000-000000000001";
const ACCOUNT: &str = "00000000-0000-4000-8000-000000000002";
const CREDENTIAL: &str = "00000000-0000-4000-8000-000000000003";
const INSTANCE: &str = "00000000-0000-4000-8000-000000000004";

#[tokio::test]
async fn multiple_rejections_keep_repair_and_exact_cancel_active_until_deadline() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let result = exercise(&fixture.pool).await;
    fixture.cleanup().await?;
    result
}

async fn exercise(pool: &sqlx::PgPool) -> TestResult {
    sqlx::query("INSERT INTO venue_users (user_id,username,password_hash,created_ms) VALUES ($1,'grid-repair','fixture',1)")
        .bind(OWNER).execute(pool).await?;
    sqlx::query("INSERT INTO venue_user_trading_accounts (trading_account_id,user_id,venue,exchange_identity_hash) VALUES ($1,$2,'binance',$3)")
        .bind(ACCOUNT).bind(OWNER).bind(vec![2_u8;32]).execute(pool).await?;
    sqlx::query("INSERT INTO venue_api_credentials (credential_id,user_id,label,key_fingerprint,masked_key,encrypted_credentials,trading_account_id,verification_json,revision,created_ms) VALUES ($1,$2,'fixture','fixture','masked',$3,$4,'{\"verification\":\"verified\"}',1,1)")
        .bind(CREDENTIAL).bind(OWNER).bind(vec![3_u8;32]).bind(ACCOUNT).execute(pool).await?;
    let store = BinanceGridStore::new(pool.clone());
    let ledger = BinanceCommandLedger::new(pool.clone());
    let now = now_ms()? - 60_000;
    let created = store
        .create_instance(
            OWNER,
            ACCOUNT,
            INSTANCE,
            &GridInstanceCreateRequest {
                schema_version: GRID_SCHEMA_VERSION,
                request_id: "00000000-0000-4000-8000-000000000005".into(),
                credential_id: CREDENTIAL.into(),
                symbol: "BTC/USDT".parse()?,
                config: config(),
            },
            now,
        )
        .await?;
    let started = store
        .request_lifecycle(
            OWNER,
            &GridLifecycleRequest {
                schema_version: GRID_SCHEMA_VERSION,
                request_id: "00000000-0000-4000-8000-000000000006".into(),
                instance_id: INSTANCE.into(),
                expected_revision: created.revision,
                action: GridLifecycleAction::Start,
                risk_confirmed: true,
                positions_remain_acknowledged: false,
            },
            now + 1,
        )
        .await?;
    let running = store
        .settle_runtime_state(
            INSTANCE,
            started.state,
            GridInstanceState::Running,
            None,
            now + 2,
        )
        .await?;
    let initial_orders = (1..=3)
        .map(|sequence| GridDesiredOrder {
            key: GridOrderSemanticKey {
                position_side: PositionSide::Long,
                role: ProtocolOrderRole::Open,
                level: sequence as u16,
                sequence,
            },
            client_order_id: format!("repair-original-{sequence}"),
            quantity: Decimal::ONE,
            limit_price: Decimal::from(100 - sequence),
        })
        .collect::<Vec<_>>();
    let anchor = GridAnchor {
        revision: running.plan_revision,
        instrument_generation: 7,
        price: Decimal::from(100),
        price_step: Decimal::ONE,
        grid_quantity: Decimal::ONE,
        source_native_trade_id: None,
        observed_ms: now + 2,
    };
    let planned = store
        .commit_plan_surface(
            INSTANCE,
            running.revision,
            running.config_revision,
            running.plan_revision,
            running.plan_revision,
            Some(&anchor),
            [11; 32],
            &initial_orders,
            now + 2,
            now + 3,
        )
        .await?;
    for (index, order) in initial_orders.iter().enumerate() {
        let command = GridLedgerCommand {
            command_id: format!("repair-place-{index}"),
            client_order_id: order.client_order_id.clone(),
            instance_id: INSTANCE.into(),
            config_revision: planned.config_revision,
            plan_revision: planned.plan_revision,
            semantic_key: order.key.encoded(),
            rule_version: "binance-pm-um-grid-r7".into(),
            source_digest: [11; 32],
            intent: GridCommandIntent::LimitPostOnly {
                key: order.key.clone(),
                quantity: order.quantity,
                limit_price: order.limit_price,
            },
        };
        store.enqueue_command(&command, now + 4).await?;
        store
            .record_order_ownership(&GridOrderOwnership {
                instance_id: INSTANCE.into(),
                trading_account_id: ACCOUNT.into(),
                config_revision: planned.config_revision,
                plan_revision: planned.plan_revision,
                key: order.key.clone(),
                place_command_id: command.command_id.clone(),
                client_order_id: command.client_order_id.clone(),
                symbol: "BTC/USDT".parse()?,
                quantity: order.quantity,
                filled_quantity: Decimal::ZERO,
                limit_price: order.limit_price,
                native_order_id: None,
                state: GridOwnedOrderState::Working,
                first_seen_ms: now + 4,
                last_seen_ms: now + 4,
            })
            .await?;
        let claimed = ledger
            .claim_next(ACCOUNT, now + 5)
            .await?
            .ok_or("missing initial placement")?;
        assert_eq!(claimed.command_id, command.command_id);
        if index < 2 {
            ledger
                .settle(
                    &command.command_id,
                    ExecutorCommandState::Rejected,
                    now + 6,
                    Some("binance_-5022"),
                )
                .await?;
        } else {
            ledger
                .settle_with_readback(
                    &command.command_id,
                    ExecutorCommandState::Accepted,
                    now + 6,
                    None,
                    Some("native-survivor"),
                )
                .await?;
            ledger
                .settle_with_readback(
                    &command.command_id,
                    ExecutorCommandState::Reconciled,
                    now + 7,
                    None,
                    Some("native-survivor"),
                )
                .await?;
        }
    }
    let next_anchor = GridAnchor {
        revision: planned.plan_revision + 1,
        ..anchor
    };
    let current = store
        .commit_plan_surface(
            INSTANCE,
            planned.revision,
            planned.config_revision,
            planned.plan_revision,
            planned.plan_revision + 1,
            Some(&next_anchor),
            [12; 32],
            &initial_orders[..2],
            now + 8,
            now + 8,
        )
        .await?;
    let runtime = BinanceGridRuntime::new(
        store.clone(),
        BinancePrivateProjectionStore::new(pool.clone()),
        BinanceTransportLimits::new(std::time::Duration::from_secs(1), 1024)?,
    );
    let projection = projection(&initial_orders[2], now + 10)?;
    let mut record = GridRuntimeRecord {
        owner_user_id: OWNER.into(),
        instance: current,
        tail_batch_id: None,
    };
    let owners = store.load_owned_orders(INSTANCE).await?;
    let actual = runtime
        .synchronize_actual_surface(&record, &projection, owners, now + 10)
        .await?;
    let desired = store
        .load_desired_orders(INSTANCE)
        .await?
        .ok_or("missing desired")?;
    let result = runtime
        .reconcile_desired(&record, &projection, &actual, &desired, now + 10)
        .await?;
    let ReconcileResult::Failed { clients, .. } = &result else {
        return Err("missing rejected repairs".into());
    };
    assert_eq!(clients.len(), 2);
    runtime
        .finish_reconcile(&record, &projection, &desired, result, now + 10)
        .await?;
    record.instance = store
        .load_owned(OWNER, INSTANCE)
        .await?
        .ok_or("missing repaired plan")?;
    assert_eq!(record.instance.state, GridInstanceState::Running);
    let repaired = store
        .load_desired_orders(INSTANCE)
        .await?
        .ok_or("missing repaired desired")?;
    assert!(
        repaired
            .orders
            .iter()
            .zip(&desired.orders)
            .all(|(new, old)| new.client_order_id != old.client_order_id)
    );
    assert_eq!(
        runtime
            .reconcile_desired(&record, &projection, &actual, &repaired, now + 12)
            .await?,
        ReconcileResult::Pending
    );
    let batch = ledger
        .claim_next_batch(ACCOUNT, now + 13)
        .await?
        .ok_or("repair batch unavailable")?;
    assert_eq!(batch.commands.len(), 3);
    assert!(
        batch.commands[..2]
            .iter()
            .all(|command| matches!(command.order, ClaimedBinanceOrder::Limit { .. }))
    );
    let ClaimedBinanceOrder::CancelExact {
        target_client_order_id,
        ..
    } = &batch.commands[2].order
    else {
        return Err("repair batch must end with the exact owned cancel".into());
    };
    assert_eq!(
        target_client_order_id.as_deref(),
        Some(initial_orders[2].client_order_id.as_str())
    );
    // A second reconciliation observes Sending and cannot enqueue duplicate replacements.
    let actual = runtime
        .synchronize_actual_surface(
            &record,
            &projection,
            store.load_owned_orders(INSTANCE).await?,
            now + 14,
        )
        .await?;
    assert_eq!(
        runtime
            .reconcile_desired(&record, &projection, &actual, &repaired, now + 14)
            .await?,
        ReconcileResult::Pending
    );
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM venue_binance_commands WHERE grid_instance_id=$1")
            .bind(INSTANCE)
            .fetch_one(pool)
            .await?;
    assert_eq!(count, 6);
    // The real coordinator must start reset at the deadline even without a healthy projection.
    let restarted = BinanceGridRuntime::new(
        store.clone(),
        BinancePrivateProjectionStore::new(pool.clone()),
        BinanceTransportLimits::new(std::time::Duration::from_secs(1), 1024)?,
    );
    restarted.enforce_rejection_deadlines().await?;
    let reset = store
        .load_owned(OWNER, INSTANCE)
        .await?
        .ok_or("missing reset")?;
    assert_eq!(reset.state, GridInstanceState::ResetRequired);
    assert_eq!(
        reset.attention_code.as_deref(),
        Some("exchange_rejection_delay_elapsed")
    );
    let sending: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM venue_binance_commands WHERE grid_instance_id=$1 AND command_state='sending'")
        .bind(INSTANCE).fetch_one(pool).await?;
    assert_eq!(sending, 3);
    Ok(())
}

fn projection(
    order: &GridDesiredOrder,
    now: u64,
) -> Result<TerminalAccountProjection, Box<dyn std::error::Error>> {
    let symbol: venue_domain::Symbol = "BTC/USDT".parse()?;
    Ok(TerminalAccountProjection {
        schema_version: TERMINAL_PROJECTION_SCHEMA_VERSION,
        credential_id: CREDENTIAL.into(),
        trading_account_id: ACCOUNT.into(),
        observed_ms: now,
        persisted_ms: now,
        private_generation: 7,
        position_mode: TerminalPositionMode::Hedge,
        positions: [PositionSide::Long, PositionSide::Short]
            .into_iter()
            .map(|side| TerminalPosition {
                symbol: symbol.clone(),
                position_side: side,
                quantity: Decimal::from(10),
                entry_price: Some(Decimal::from(100)),
                mark_price: Some(Decimal::from(100)),
            })
            .collect(),
        open_orders: vec![TerminalOpenOrder {
            client_order_id: order.client_order_id.clone(),
            native_order_id: Some("native-survivor".into()),
            symbol,
            order_side: order.key.order_side(),
            position_side: order.key.position_side,
            quantity: order.quantity,
            filled_quantity: Some(Decimal::ZERO),
            limit_price: Some(order.limit_price),
            post_only: true,
            time_in_force: Some(venue_domain::LimitTimeInForce::PostOnly),
            reduce_only: false,
            state: TerminalOrderState::New,
            created_ms: Some(now - 5),
        }],
        position_history: Vec::new(),
        fills: Vec::new(),
        assets: Vec::new(),
    })
}

fn config() -> GridConfig {
    GridConfig {
        order_notional: Decimal::from(5),
        spacing_rate: Decimal::new(1, 2),
        grid_levels: 3,
        max_total_notional: Decimal::from(500),
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
    }
}
