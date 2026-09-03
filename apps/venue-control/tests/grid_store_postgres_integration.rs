use rust_decimal::Decimal;
use sqlx::{Executor, PgPool, postgres::PgPoolOptions};
use venue_control::accounts::MIGRATION_0015;
use venue_control::grid_store::{GridBatchPlacement, GridMutationBatch, GridPlanMutationBatch};
use venue_control::{
    BinanceCommandLedger, BinanceGridStore, GridCommandIntent, GridConvergenceUpdate,
    GridDesiredOrder, GridFillAllocation, GridLedgerCommand, GridOrderOwnership,
    GridOwnedOrderState, GridStoreError, MIGRATION_0001, MIGRATION_0017, MIGRATION_0018,
    MIGRATION_0019, MIGRATION_0020, MIGRATION_0021, MIGRATION_0022, MIGRATION_0023, MIGRATION_0024,
};
use venue_control_protocol::grid::{
    GRID_SCHEMA_VERSION, GridAnchor, GridConfig, GridConfigUpdateRequest,
    GridInstanceCreateRequest, GridInstanceState, GridInventoryReplenishment, GridLifecycleAction,
    GridLifecycleRequest, GridOrderRole, GridOrderSemanticKey, GridProfitReduction,
    GridResetPolicy,
};
use venue_control_protocol::kol::{ExecutorCommandOrigin, ExecutorCommandState, TerminalFill};
use venue_domain::{OrderSide, PositionSide};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn grid_store_is_idempotent_owned_and_restart_safe() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let result = exercise(&fixture.pool).await;
    fixture.cleanup().await?;
    result
}

#[tokio::test]
async fn grid_hot_batch_is_atomic_idempotent_ordered_and_fail_stopped() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let result = exercise_hot_batch(&fixture.pool).await;
    fixture.cleanup().await?;
    result
}

#[tokio::test]
async fn grid_single_command_replay_survives_a_full_shared_account_queue() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let result = exercise_grid_single_command_queue_limit(&fixture.pool).await;
    fixture.cleanup().await?;
    result
}

#[tokio::test]
async fn grid_batch_claim_only_marks_an_exact_current_projection_hot() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let result = exercise_grid_batch_projection_claim_fence(&fixture.pool).await;
    fixture.cleanup().await?;
    result
}

async fn exercise_grid_batch_projection_claim_fence(pool: &PgPool) -> TestResult {
    seed_verified_account(pool).await?;
    let store = BinanceGridStore::new(pool.clone());
    let ledger = BinanceCommandLedger::new(pool.clone());
    let owner = id(1);
    let account = id(2);
    let credential = id(3);
    let instance = id(95);
    let now = 1_900_300_000_000_u64;
    let private_observed_ms = now + 3;
    sqlx::query(
        "INSERT INTO venue_binance_account_projections \
         (credential_id,owner_user_id,trading_account_id,observed_ms,persisted_ms,\
          private_generation,projection_json) VALUES ($1,$2,$3,$4,$4,7,'{}'::jsonb)",
    )
    .bind(&credential)
    .bind(&owner)
    .bind(&account)
    .bind(i64::try_from(private_observed_ms)?)
    .execute(pool)
    .await?;
    let created = store
        .create_instance(
            &owner,
            &account,
            &instance,
            &GridInstanceCreateRequest {
                schema_version: GRID_SCHEMA_VERSION,
                request_id: id(96),
                credential_id: credential,
                symbol: "BTC/USDT".parse()?,
                config: config(),
            },
            now,
        )
        .await?;
    let started = store
        .request_lifecycle(
            &owner,
            &lifecycle(
                id(97),
                &instance,
                created.revision,
                GridLifecycleAction::Start,
            ),
            now + 1,
        )
        .await?;
    let running = store
        .settle_runtime_state(
            &instance,
            GridInstanceState::StartPending,
            GridInstanceState::Running,
            None,
            now + 2,
        )
        .await?;
    assert_eq!(running.revision, started.revision + 1);

    let exact = projection_fenced_grid_command(
        &instance,
        running.config_revision,
        running.plan_revision,
        "grid-hot-current",
        "inventory:hot-current",
        41,
    );
    store.enqueue_command(&exact, now + 4).await?;
    bind_hot_receipt(pool, &exact.command_id, 7, private_observed_ms, now + 5).await?;
    let claimed = ledger
        .claim_next_batch(&account, now + 6)
        .await?
        .ok_or("missing exact-projection Grid command")?;
    let exact_context = claimed
        .grid_context
        .as_ref()
        .ok_or("missing exact-projection Grid receipt")?;
    assert!(exact_context.private_projection_current);
    assert_eq!(exact_context.private_generation, 7);
    assert_eq!(exact_context.private_observed_ms, private_observed_ms);
    assert_eq!(exact_context.source_event_received_ms, Some(now + 5));
    ledger
        .settle(
            &claimed.commands[0].command_id,
            ExecutorCommandState::Rejected,
            now + 7,
            Some("fixture_rejected"),
        )
        .await?;

    sqlx::query(
        "UPDATE venue_binance_account_projections \
         SET observed_ms=$2,persisted_ms=$2,private_generation=8 WHERE credential_id=$1",
    )
    .bind(id(3))
    .bind(i64::try_from(now + 8)?)
    .execute(pool)
    .await?;
    let superseded = projection_fenced_grid_command(
        &instance,
        running.config_revision,
        running.plan_revision,
        "grid-hot-superseded",
        "inventory:hot-superseded",
        42,
    );
    store.enqueue_command(&superseded, now + 9).await?;
    bind_hot_receipt(
        pool,
        &superseded.command_id,
        7,
        private_observed_ms,
        now + 10,
    )
    .await?;
    let claimed = ledger
        .claim_next_batch(&account, now + 11)
        .await?
        .ok_or("missing superseded-projection Grid command")?;
    let stale_context = claimed
        .grid_context
        .as_ref()
        .ok_or("missing superseded-projection Grid receipt")?;
    assert!(!stale_context.private_projection_current);
    assert_eq!(stale_context.private_generation, 7);
    assert_eq!(stale_context.private_observed_ms, private_observed_ms);
    assert_eq!(stale_context.source_event_received_ms, Some(now + 10));
    Ok(())
}

fn projection_fenced_grid_command(
    instance_id: &str,
    config_revision: u64,
    plan_revision: u64,
    command_id: &str,
    semantic_key: &str,
    digest: u8,
) -> GridLedgerCommand {
    GridLedgerCommand {
        command_id: command_id.to_owned(),
        client_order_id: format!("vgm-{command_id}"),
        instance_id: instance_id.to_owned(),
        config_revision,
        plan_revision,
        semantic_key: semantic_key.to_owned(),
        rule_version: "binance-pm-um-grid-r1".to_owned(),
        source_digest: [digest; 32],
        intent: GridCommandIntent::Market {
            position_side: PositionSide::Long,
            role: GridOrderRole::Open,
            quantity: Decimal::ONE,
        },
    }
}

async fn bind_hot_receipt(
    pool: &PgPool,
    batch_id: &str,
    private_generation: u64,
    private_observed_ms: u64,
    source_event_received_ms: u64,
) -> TestResult {
    sqlx::query(
        "UPDATE venue_binance_grid_mutation_batches \
         SET private_generation=$2,private_observed_ms=$3,instrument_generation=13,\
             source_event_received_ms=$4 WHERE batch_id=$1",
    )
    .bind(batch_id)
    .bind(i64::try_from(private_generation)?)
    .bind(i64::try_from(private_observed_ms)?)
    .bind(i64::try_from(source_event_received_ms)?)
    .execute(pool)
    .await?;
    Ok(())
}

async fn exercise_grid_single_command_queue_limit(pool: &PgPool) -> TestResult {
    seed_verified_account(pool).await?;
    let store = BinanceGridStore::new(pool.clone());
    let owner = id(1);
    let account = id(2);
    let instance = id(90);
    let now = 1_900_200_000_000_u64;
    let created = store
        .create_instance(
            &owner,
            &account,
            &instance,
            &GridInstanceCreateRequest {
                schema_version: GRID_SCHEMA_VERSION,
                request_id: id(91),
                credential_id: id(3),
                symbol: "BTC/USDT".parse()?,
                config: config(),
            },
            now,
        )
        .await?;
    let start = store
        .request_lifecycle(
            &owner,
            &lifecycle(
                id(92),
                &instance,
                created.revision,
                GridLifecycleAction::Start,
            ),
            now + 1,
        )
        .await?;
    let running = store
        .settle_runtime_state(
            &instance,
            GridInstanceState::StartPending,
            GridInstanceState::Running,
            None,
            now + 2,
        )
        .await?;
    assert_eq!(running.revision, start.revision + 1);
    let first = GridLedgerCommand {
        command_id: "grid-queue-first".into(),
        client_order_id: "vgm-grid-queue-first".into(),
        instance_id: instance.clone(),
        config_revision: running.config_revision,
        plan_revision: running.plan_revision,
        semantic_key: "inventory:long".into(),
        rule_version: "binance-pm-um-grid-r1".into(),
        source_digest: [31_u8; 32],
        intent: GridCommandIntent::Market {
            position_side: PositionSide::Long,
            role: GridOrderRole::Open,
            quantity: Decimal::ONE,
        },
    };
    let inserted = store.enqueue_command(&first, now + 3).await?;
    for offset in 0_u64..15 {
        insert_terminal_close_reservation(pool, 100 + offset, "pending", now + 4 + offset).await?;
    }
    let replayed = store.enqueue_command(&first, now + 20).await?;
    assert_eq!(replayed.command_id, inserted.command_id);
    let overflow = GridLedgerCommand {
        command_id: "grid-queue-overflow".into(),
        client_order_id: "vgm-grid-queue-overflow".into(),
        semantic_key: "inventory:short".into(),
        source_digest: [32_u8; 32],
        intent: GridCommandIntent::Market {
            position_side: PositionSide::Short,
            role: GridOrderRole::Open,
            quantity: Decimal::ONE,
        },
        ..first
    };
    assert_eq!(
        store.enqueue_command(&overflow, now + 21).await,
        Err(GridStoreError::Conflict)
    );
    let unresolved: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM venue_binance_commands WHERE trading_account_id=$1 \
         AND command_state IN ('pending','sending','accepted','reconcile_required')",
    )
    .bind(account)
    .fetch_one(pool)
    .await?;
    assert_eq!(unresolved, 16);
    Ok(())
}

async fn exercise_hot_batch(pool: &PgPool) -> TestResult {
    seed_verified_account(pool).await?;
    let store = BinanceGridStore::new(pool.clone());
    let owner = id(1);
    let account = id(2);
    let credential = id(3);
    let instance = id(80);
    let now = 1_900_100_000_000_u64;
    sqlx::query(
        "INSERT INTO venue_binance_account_projections \
         (credential_id,owner_user_id,trading_account_id,observed_ms,persisted_ms,\
          private_generation,projection_json) VALUES ($1,$2,$3,$4,$4,$5,$6)",
    )
    .bind(&credential)
    .bind(&owner)
    .bind(&account)
    .bind(i64::try_from(now + 3)?)
    .bind(7_i64)
    .bind(serde_json::json!({}))
    .execute(pool)
    .await?;
    let created = store
        .create_instance(
            &owner,
            &account,
            &instance,
            &GridInstanceCreateRequest {
                schema_version: GRID_SCHEMA_VERSION,
                request_id: id(81),
                credential_id: credential,
                symbol: "BTC/USDT".parse()?,
                config: config(),
            },
            now,
        )
        .await?;
    let started = store
        .request_lifecycle(
            &owner,
            &lifecycle(
                id(82),
                &instance,
                created.revision,
                GridLifecycleAction::Start,
            ),
            now + 1,
        )
        .await?;
    let running = store
        .settle_runtime_state(
            &instance,
            GridInstanceState::StartPending,
            GridInstanceState::Running,
            None,
            now + 2,
        )
        .await?;
    assert_eq!(running.revision, started.revision + 1);

    let old_key = GridOrderSemanticKey {
        position_side: PositionSide::Long,
        role: GridOrderRole::Open,
        level: 1,
        sequence: 1,
    };
    let old_desired = GridDesiredOrder {
        key: old_key.clone(),
        client_order_id: "vgp-hot-old".into(),
        quantity: Decimal::ONE,
        limit_price: Decimal::from(99),
    };
    let old_surface = store
        .commit_plan_surface(
            &instance,
            running.revision,
            running.config_revision,
            running.plan_revision,
            running.plan_revision,
            None,
            [20; 32],
            std::slice::from_ref(&old_desired),
            now + 2,
            now + 3,
        )
        .await?;
    let old_command = GridLedgerCommand {
        command_id: "gp-hot-old".into(),
        client_order_id: old_desired.client_order_id.clone(),
        instance_id: instance.clone(),
        config_revision: old_surface.config_revision,
        plan_revision: old_surface.plan_revision,
        semantic_key: old_key.encoded(),
        rule_version: "binance-pm-um-grid-r7".into(),
        source_digest: [20; 32],
        intent: GridCommandIntent::LimitPostOnly {
            key: old_key.clone(),
            quantity: old_desired.quantity,
            limit_price: old_desired.limit_price,
        },
    };
    store.enqueue_command(&old_command, now + 4).await?;
    store
        .record_order_ownership(&GridOrderOwnership {
            instance_id: instance.clone(),
            trading_account_id: account.clone(),
            config_revision: old_surface.config_revision,
            plan_revision: old_surface.plan_revision,
            key: old_key,
            place_command_id: old_command.command_id.clone(),
            client_order_id: old_command.client_order_id.clone(),
            symbol: "BTC/USDT".parse()?,
            quantity: Decimal::ONE,
            filled_quantity: Decimal::ZERO,
            limit_price: Decimal::from(99),
            native_order_id: Some("native-hot-old".into()),
            state: GridOwnedOrderState::Working,
            first_seen_ms: now + 4,
            last_seen_ms: now + 4,
        })
        .await?;
    sqlx::query(
        "UPDATE venue_binance_commands SET command_state='reconciled',native_order_id=$1,\
         sending_ms=$2,accepted_ms=$2,terminal_ms=$2,updated_ms=$2 WHERE command_id=$3",
    )
    .bind("native-hot-old")
    .bind(i64::try_from(now + 5)?)
    .bind(&old_command.command_id)
    .execute(pool)
    .await?;

    let next_plan = old_surface.plan_revision + 1;
    let desired_digest = [21; 32];
    let desired = [
        desired_order(
            PositionSide::Long,
            GridOrderRole::Close,
            1,
            1,
            "vgp-hot-close",
            101,
        ),
        desired_order(
            PositionSide::Short,
            GridOrderRole::Open,
            1,
            2,
            "vgp-hot-open",
            101,
        ),
    ];
    let symbol: venue_domain::Symbol = "BTC/USDT".parse()?;
    let placements = desired
        .iter()
        .enumerate()
        .map(|(offset, order)| {
            let command_id = format!("gp-hot-{offset}");
            GridBatchPlacement {
                command: GridLedgerCommand {
                    command_id: command_id.clone(),
                    client_order_id: order.client_order_id.clone(),
                    instance_id: instance.clone(),
                    config_revision: old_surface.config_revision,
                    plan_revision: next_plan,
                    semantic_key: order.key.encoded(),
                    rule_version: "binance-pm-um-grid-r7".into(),
                    source_digest: desired_digest,
                    intent: GridCommandIntent::LimitPostOnly {
                        key: order.key.clone(),
                        quantity: order.quantity,
                        limit_price: order.limit_price,
                    },
                },
                ownership: GridOrderOwnership {
                    instance_id: instance.clone(),
                    trading_account_id: account.clone(),
                    config_revision: old_surface.config_revision,
                    plan_revision: next_plan,
                    key: order.key.clone(),
                    place_command_id: command_id,
                    client_order_id: order.client_order_id.clone(),
                    symbol: symbol.clone(),
                    quantity: order.quantity,
                    filled_quantity: Decimal::ZERO,
                    limit_price: order.limit_price,
                    native_order_id: None,
                    state: GridOwnedOrderState::Working,
                    first_seen_ms: now + 6,
                    last_seen_ms: now + 6,
                },
            }
        })
        .collect::<Vec<_>>();
    let cancel = GridLedgerCommand {
        command_id: "gc-hot-old".into(),
        client_order_id: "vgc-hot-old".into(),
        instance_id: instance.clone(),
        config_revision: old_surface.config_revision,
        plan_revision: next_plan,
        semantic_key: format!("cancel:{}", old_command.client_order_id),
        rule_version: "binance-pm-um-grid".into(),
        source_digest: desired_digest,
        intent: GridCommandIntent::Cancel {
            target_client_order_id: old_command.client_order_id.clone(),
        },
    };
    let plan = GridPlanMutationBatch {
        mutation: GridMutationBatch {
            batch_id: "gb-hot-plan-1".into(),
            instance_id: instance.clone(),
            expected_instance_revision: old_surface.revision,
            config_revision: old_surface.config_revision,
            plan_revision: next_plan,
            desired_digest,
            placements,
            cancellations: vec![cancel],
        },
        expected_plan_revision: old_surface.plan_revision,
        expected_desired_digest: Some([20; 32]),
        predecessor_batch_id: None,
        expected_private_generation: 7,
        expected_private_observed_ms: now + 3,
        source_event_received_ms: Some(now + 4),
        require_empty_account_queue: true,
        anchor: GridAnchor {
            revision: next_plan,
            instrument_generation: 7,
            price: Decimal::from(100),
            price_step: Decimal::ONE,
            grid_quantity: Decimal::ONE,
            source_native_trade_id: Some("hot-trade-1".into()),
            observed_ms: now + 6,
        },
        desired_orders: desired.to_vec(),
        fill_allocations: vec![GridFillAllocation {
            instance_id: instance.clone(),
            trading_account_id: account.clone(),
            config_revision: old_surface.config_revision,
            client_order_id: old_command.client_order_id.clone(),
            native_trade_id: "hot-trade-1".into(),
            symbol: "BTC/USDT".parse()?,
            position_side: PositionSide::Long,
            role: GridOrderRole::Open,
            quantity: Decimal::ONE,
            price: Decimal::from(99),
            maker: Some(true),
            occurred_ms: Some(now + 5),
            observed_ms: now + 6,
        }],
        last_facts_ms: now + 6,
    };
    let mut stale_projection = plan.clone();
    stale_projection.mutation.batch_id = "gb-hot-stale-private".into();
    stale_projection.expected_private_generation = 6;
    assert_eq!(
        store
            .commit_plan_mutation_batch(&stale_projection, now + 6)
            .await,
        Err(GridStoreError::Conflict)
    );
    let stale_receipts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM venue_binance_grid_mutation_batches WHERE batch_id=$1",
    )
    .bind(&stale_projection.mutation.batch_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(stale_receipts, 0);

    let committed = store.commit_plan_mutation_batch(&plan, now + 6).await?;
    assert!(committed.receipt.inserted);
    assert_eq!(committed.receipt.command_count, 3);
    let ordered: Vec<String> = sqlx::query_scalar(
        "SELECT order_kind FROM venue_binance_commands WHERE grid_batch_id=$1 \
         ORDER BY dispatch_sequence",
    )
    .bind(&plan.mutation.batch_id)
    .fetch_all(pool)
    .await?;
    assert_eq!(
        ordered,
        ["limit_post_only", "limit_post_only", "cancel_exact"]
    );
    let old_owner = store
        .load_owned_orders(&instance)
        .await?
        .into_iter()
        .find(|row| row.client_order_id == old_command.client_order_id)
        .ok_or("missing filled owner")?;
    assert_eq!(old_owner.filled_quantity, Decimal::ONE);
    assert_eq!(old_owner.state, GridOwnedOrderState::Terminal);

    let mut replay = plan.clone();
    for placement in &mut replay.mutation.placements {
        placement.ownership.first_seen_ms = now + 7;
        placement.ownership.last_seen_ms = now + 7;
    }
    let replayed = store.commit_plan_mutation_batch(&replay, now + 7).await?;
    assert!(!replayed.receipt.inserted);
    let mut conflict = replay.clone();
    conflict.mutation.cancellations[0].command_id = "gc-hot-conflict".into();
    assert_eq!(
        store.commit_plan_mutation_batch(&conflict, now + 7).await,
        Err(GridStoreError::Conflict)
    );
    let mut stale_cas = replay;
    stale_cas.mutation.batch_id = "gb-hot-stale-cas".into();
    assert_eq!(
        store.commit_plan_mutation_batch(&stale_cas, now + 7).await,
        Err(GridStoreError::Conflict)
    );

    let ledger = BinanceCommandLedger::new(pool.clone());
    let first = ledger
        .claim_next(&account, now + 8)
        .await?
        .ok_or("missing first batch place")?;
    ledger
        .settle(
            &first.command_id,
            ExecutorCommandState::Rejected,
            now + 9,
            Some("fixture_rejected"),
        )
        .await?;
    assert!(ledger.claim_next(&account, now + 10).await?.is_none());
    let current = store
        .load_owned(&owner, &instance)
        .await?
        .ok_or("missing hot grid")?;
    let stopped = store
        .request_lifecycle(
            &owner,
            &lifecycle(
                id(83),
                &instance,
                current.revision,
                GridLifecycleAction::Stop,
            ),
            now + 11,
        )
        .await?;
    assert_eq!(stopped.state, GridInstanceState::StopPending);
    let lifecycle_cancel = ledger
        .claim_next(&account, now + 12)
        .await?
        .ok_or("lifecycle did not release exact cancel")?;
    assert!(lifecycle_cancel.command_id.starts_with("gc-"));
    Ok(())
}

fn desired_order(
    side: PositionSide,
    role: GridOrderRole,
    level: u16,
    sequence: u64,
    client_order_id: &str,
    price: i64,
) -> GridDesiredOrder {
    GridDesiredOrder {
        key: GridOrderSemanticKey {
            position_side: side,
            role,
            level,
            sequence,
        },
        client_order_id: client_order_id.into(),
        quantity: Decimal::ONE,
        limit_price: Decimal::from(price),
    }
}

async fn exercise(pool: &PgPool) -> TestResult {
    seed_verified_account(pool).await?;
    let store = BinanceGridStore::new(pool.clone());
    let owner = id(1);
    let account = id(2);
    let credential = id(3);
    let first = id(4);
    let second = id(5);
    let now = 1_900_000_000_000_u64;
    let request = GridInstanceCreateRequest {
        schema_version: GRID_SCHEMA_VERSION,
        request_id: id(6),
        credential_id: credential.clone(),
        symbol: "BTC/USDT".parse()?,
        config: config(),
    };
    let created = store
        .create_instance(&owner, &account, &first, &request, now)
        .await?;
    assert_eq!(created.state, GridInstanceState::Draft);
    assert_eq!(created.plan_revision, 1);
    assert!(!created.dirty);
    assert_eq!(
        store
            .create_instance(&owner, &account, &first, &request, now + 1)
            .await?,
        created
    );

    let mut second_request = request.clone();
    second_request.request_id = id(7);
    store
        .create_instance(&owner, &account, &second, &second_request, now + 2)
        .await?;
    assert_eq!(store.list_owned(&owner).await?.len(), 2);

    sqlx::query(
        "INSERT INTO venue_control_strategy_scopes \
         (instance_id,venue,mode,trading_account_id,symbol,config_epoch,snapshot_generated_ms) \
         VALUES ('legacy-grid','binance','LIVE',$1,'BTC/USDT',1,$2)",
    )
    .bind(&account)
    .bind(i64::try_from(now)?)
    .execute(pool)
    .await?;
    let start = lifecycle(id(8), &first, created.revision, GridLifecycleAction::Start);
    assert_eq!(
        store.request_lifecycle(&owner, &start, now + 3).await,
        Err(GridStoreError::Conflict)
    );
    sqlx::query("DELETE FROM venue_control_strategy_scopes WHERE instance_id='legacy-grid'")
        .execute(pool)
        .await?;
    let pending = store.request_lifecycle(&owner, &start, now + 4).await?;
    assert_eq!(pending.state, GridInstanceState::StartPending);
    assert!(pending.dirty);
    let second_current = store
        .load_owned(&owner, &second)
        .await?
        .ok_or("missing second grid")?;
    let second_pending = store
        .request_lifecycle(
            &owner,
            &lifecycle(
                id(40),
                &second,
                second_current.revision,
                GridLifecycleAction::Start,
            ),
            now + 4,
        )
        .await?;
    let initial_key = GridOrderSemanticKey {
        position_side: PositionSide::Long,
        role: GridOrderRole::Open,
        level: 1,
        sequence: 1,
    };
    let initial_order = GridDesiredOrder {
        key: initial_key,
        client_order_id: "g-initial-long-open-1".into(),
        quantity: Decimal::new(15, 4),
        limit_price: Decimal::new(66_800, 0),
    };
    let planned = store
        .commit_plan_surface(
            &first,
            pending.revision,
            pending.config_revision,
            pending.plan_revision,
            pending.plan_revision,
            None,
            [11; 32],
            std::slice::from_ref(&initial_order),
            now + 4,
            now + 5,
        )
        .await?;
    let running = store
        .update_convergence(
            &GridConvergenceUpdate {
                instance_id: first.clone(),
                expected_instance_revision: planned.revision,
                expected_state: planned.state,
                expected_plan_revision: planned.plan_revision,
                next_plan_revision: planned.plan_revision,
                desired_digest: [11; 32],
                dirty: false,
                consecutive_failures: 0,
                last_facts_ms: now + 4,
            },
            now + 6,
        )
        .await?;
    assert_eq!(running.state, GridInstanceState::Running);
    let expected_digest = "0b".repeat(32);
    assert_eq!(
        running.desired_digest.as_deref(),
        Some(expected_digest.as_str())
    );

    let anchor = GridAnchor {
        revision: 1,
        instrument_generation: 7,
        price: Decimal::new(67_000, 0),
        price_step: Decimal::new(10, 1),
        grid_quantity: Decimal::new(15, 4),
        source_native_trade_id: Some("trade-anchor-1".into()),
        observed_ms: now + 6,
    };
    let running = store
        .commit_plan_surface(
            &first,
            running.revision,
            running.config_revision,
            running.plan_revision,
            running.plan_revision,
            Some(&anchor),
            [11; 32],
            std::slice::from_ref(&initial_order),
            now + 6,
            now + 7,
        )
        .await?;
    let running = store
        .commit_plan_surface(
            &first,
            running.revision,
            running.config_revision,
            running.plan_revision,
            running.plan_revision,
            Some(&anchor),
            [11; 32],
            std::slice::from_ref(&initial_order),
            now + 6,
            now + 8,
        )
        .await?;
    let mut conflicting_anchor = anchor.clone();
    conflicting_anchor.price += Decimal::ONE;
    assert_eq!(
        store
            .commit_plan_surface(
                &first,
                running.revision,
                running.config_revision,
                running.plan_revision,
                running.plan_revision,
                Some(&conflicting_anchor),
                [11; 32],
                std::slice::from_ref(&initial_order),
                now + 6,
                now + 8,
            )
            .await,
        Err(GridStoreError::Conflict)
    );
    assert_eq!(
        store
            .load_owned(&owner, &first)
            .await?
            .ok_or("missing grid after rolled-back surface conflict")?
            .revision,
        running.revision
    );

    let old_place = GridLedgerCommand {
        command_id: id(20),
        client_order_id: "g-initial-long-open-1".into(),
        instance_id: first.clone(),
        config_revision: running.config_revision,
        plan_revision: running.plan_revision,
        semantic_key: "long:open:1:1".into(),
        rule_version: "binance-pm-um-rules-7".into(),
        source_digest: [11; 32],
        intent: GridCommandIntent::LimitPostOnly {
            key: GridOrderSemanticKey {
                position_side: PositionSide::Long,
                role: GridOrderRole::Open,
                level: 1,
                sequence: 1,
            },
            quantity: Decimal::new(15, 4),
            limit_price: Decimal::new(66_800, 0),
        },
    };
    store.enqueue_command(&old_place, now + 8).await?;

    let mut changed_config = config();
    changed_config.spacing_rate = Decimal::new(3, 3);
    let configured = store
        .update_config(
            &owner,
            &GridConfigUpdateRequest {
                schema_version: GRID_SCHEMA_VERSION,
                request_id: id(9),
                instance_id: first.clone(),
                expected_revision: running.revision,
                config: changed_config,
            },
            now + 9,
        )
        .await?;
    assert_eq!(configured.state, GridInstanceState::Running);
    assert_eq!(configured.config_revision, 2);
    assert!(configured.dirty);
    assert!(configured.anchor.is_none());
    sqlx::query(
        "UPDATE venue_binance_grid_instances SET consecutive_failures=2 WHERE instance_id=$1",
    )
    .bind(&first)
    .execute(pool)
    .await?;
    let old_state: String =
        sqlx::query_scalar("SELECT command_state FROM venue_binance_commands WHERE command_id=$1")
            .bind(&old_place.command_id)
            .fetch_one(pool)
            .await?;
    assert_eq!(old_state, "cancelled");

    let key = GridOrderSemanticKey {
        position_side: PositionSide::Long,
        role: GridOrderRole::Open,
        level: 1,
        sequence: 1,
    };
    let configured_anchor = GridAnchor {
        revision: anchor.revision + 1,
        observed_ms: now + 9,
        ..anchor.clone()
    };
    let planned_config = store
        .commit_plan_surface(
            &first,
            configured.revision,
            configured.config_revision,
            configured.plan_revision,
            configured.plan_revision + 1,
            Some(&configured_anchor),
            [12; 32],
            &[GridDesiredOrder {
                key: key.clone(),
                client_order_id: "g-place-long-open-1".into(),
                quantity: Decimal::new(15, 4),
                limit_price: Decimal::new(66_900, 0),
            }],
            now + 9,
            now + 10,
        )
        .await?;
    assert_eq!(planned_config.consecutive_failures, 2);
    let desired = store
        .load_desired_orders(&first)
        .await?
        .ok_or("missing desired surface")?;
    assert_eq!(desired.plan_revision, planned_config.plan_revision);
    assert_eq!(desired.orders.len(), 1);
    let place = GridLedgerCommand {
        command_id: id(10),
        client_order_id: "g-place-long-open-1".into(),
        instance_id: first.clone(),
        config_revision: planned_config.config_revision,
        plan_revision: planned_config.plan_revision,
        semantic_key: key.encoded(),
        rule_version: "binance-pm-um-rules-7".into(),
        source_digest: [12; 32],
        intent: GridCommandIntent::LimitPostOnly {
            key: key.clone(),
            quantity: Decimal::new(15, 4),
            limit_price: Decimal::new(66_900, 0),
        },
    };
    let placed = store.enqueue_command(&place, now + 11).await?;
    assert_eq!(placed.instance_id, first);
    store
        .record_order_ownership(&GridOrderOwnership {
            instance_id: first.clone(),
            trading_account_id: account.clone(),
            config_revision: planned_config.config_revision,
            plan_revision: planned_config.plan_revision,
            key: key.clone(),
            place_command_id: place.command_id.clone(),
            client_order_id: place.client_order_id.clone(),
            symbol: "BTC/USDT".parse()?,
            quantity: Decimal::new(15, 4),
            filled_quantity: Decimal::ZERO,
            limit_price: Decimal::new(66_900, 0),
            native_order_id: Some("native-grid-1".into()),
            state: GridOwnedOrderState::Working,
            first_seen_ms: now + 12,
            last_seen_ms: now + 12,
        })
        .await?;
    assert_eq!(store.load_owned_orders(&first).await?.len(), 1);

    let market = GridLedgerCommand {
        command_id: id(11),
        client_order_id: "g-market-replenish-1".into(),
        instance_id: first.clone(),
        config_revision: planned_config.config_revision,
        plan_revision: planned_config.plan_revision,
        semantic_key: "inventory:long:1".into(),
        rule_version: "binance-pm-um-rules-7".into(),
        source_digest: [13; 32],
        intent: GridCommandIntent::Market {
            position_side: PositionSide::Long,
            role: GridOrderRole::Open,
            quantity: Decimal::new(2, 3),
        },
    };
    store.enqueue_command(&market, now + 13).await?;
    let cancel = GridLedgerCommand {
        command_id: id(12),
        client_order_id: "g-cancel-long-open-1".into(),
        instance_id: first.clone(),
        config_revision: planned_config.config_revision,
        plan_revision: planned_config.plan_revision,
        semantic_key: "cancel:long:open:1:1".into(),
        rule_version: "binance-pm-um-rules-7".into(),
        source_digest: [14; 32],
        intent: GridCommandIntent::Cancel {
            target_client_order_id: place.client_order_id.clone(),
        },
    };
    store.enqueue_command(&cancel, now + 14).await?;
    let selected: Option<String> = sqlx::query_scalar(
        "SELECT selected_native_order_id FROM venue_binance_commands WHERE command_id=$1",
    )
    .bind(&cancel.command_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(selected.as_deref(), Some("native-grid-1"));

    let other_close = GridLedgerCommand {
        command_id: id(41),
        client_order_id: "g-other-grid-close-1".into(),
        instance_id: second.clone(),
        config_revision: second_pending.config_revision,
        plan_revision: second_pending.plan_revision,
        semantic_key: "profit:short:close:1".into(),
        rule_version: "binance-pm-um-rules-7".into(),
        source_digest: [15; 32],
        intent: GridCommandIntent::Market {
            position_side: PositionSide::Short,
            role: GridOrderRole::Close,
            quantity: Decimal::new(2, 4),
        },
    };
    store.enqueue_command(&other_close, now + 14).await?;
    for (offset, state) in [
        "pending",
        "sending",
        "accepted",
        "reconcile_required",
        "reconciled",
    ]
    .into_iter()
    .enumerate()
    {
        let offset = u64::try_from(offset)?;
        insert_terminal_close_reservation(pool, offset, state, now + 14 + offset).await?;
    }
    let reservations = store
        .load_reduce_reservations(&account, &"BTC/USDT".parse()?)
        .await?;
    assert_eq!(reservations.len(), 6);
    assert!(reservations.iter().any(|reservation| {
        reservation.origin == ExecutorCommandOrigin::Grid
            && reservation.grid_instance_id.as_deref() == Some(second.as_str())
            && reservation.client_order_id == other_close.client_order_id
    }));
    for state in [
        ExecutorCommandState::Pending,
        ExecutorCommandState::Sending,
        ExecutorCommandState::Accepted,
        ExecutorCommandState::ReconcileRequired,
        ExecutorCommandState::Reconciled,
    ] {
        assert!(reservations.iter().any(|reservation| {
            reservation.origin == ExecutorCommandOrigin::Terminal
                && reservation.state == state
                && reservation.updated_ms >= now + 14
        }));
    }
    let second_reset = store
        .settle_runtime_state(
            &second,
            GridInstanceState::StartPending,
            GridInstanceState::ResetRequired,
            Some("surface_conflict"),
            now + 20,
        )
        .await?;
    assert_eq!(
        second_reset.config_revision,
        second_pending.config_revision + 1
    );
    let second_config_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM venue_binance_grid_config_revisions WHERE instance_id=$1",
    )
    .bind(&second)
    .fetch_one(pool)
    .await?;
    assert_eq!(second_config_rows, 2);
    assert_eq!(
        store
            .settle_runtime_state(
                &second,
                GridInstanceState::StartPending,
                GridInstanceState::ResetRequired,
                Some("surface_conflict"),
                now + 21,
            )
            .await,
        Err(GridStoreError::Conflict)
    );
    let second_config_rows_after: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM venue_binance_grid_config_revisions WHERE instance_id=$1",
    )
    .bind(&second)
    .fetch_one(pool)
    .await?;
    assert_eq!(second_config_rows_after, second_config_rows);

    let allocation = GridFillAllocation {
        instance_id: first.clone(),
        trading_account_id: account.clone(),
        config_revision: planned_config.config_revision,
        client_order_id: place.client_order_id.clone(),
        native_trade_id: "grid-trade-1".into(),
        symbol: "BTC/USDT".parse()?,
        position_side: PositionSide::Long,
        role: GridOrderRole::Open,
        quantity: Decimal::new(5, 4),
        price: Decimal::new(66_900, 0),
        maker: Some(true),
        occurred_ms: Some(now + 12),
        observed_ms: now + 13,
    };
    assert!(store.record_fill_allocation(&allocation).await?);
    assert!(!store.record_fill_allocation(&allocation).await?);

    let unallocated = TerminalFill {
        native_trade_id: "grid-trade-2".into(),
        native_order_id: "native-grid-1".into(),
        symbol: "BTC/USDT".parse()?,
        order_side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::new(5, 4),
        price: Decimal::new(66_900, 0),
        maker: Some(true),
        occurred_ms: Some(now + 13),
    };
    sqlx::query(
        "INSERT INTO venue_binance_account_fills \
         (trading_account_id,owner_user_id,native_trade_id,symbol,occurred_ms,observed_ms,fill_json) \
         VALUES ($1,$2,$3,'BTC/USDT',$4,$5,$6)",
    )
    .bind(&account)
    .bind(&owner)
    .bind(&unallocated.native_trade_id)
    .bind(i64::try_from(now + 13)?)
    .bind(i64::try_from(now + 14)?)
    .bind(serde_json::to_value(&unallocated)?)
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE venue_binance_commands SET command_state='reconciled',\
         native_order_id='native-grid-1',sending_ms=$1,accepted_ms=$1,terminal_ms=$1,updated_ms=$1 \
         WHERE command_id=$2",
    )
    .bind(i64::try_from(now + 14)?)
    .bind(&place.command_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE venue_binance_grid_order_owners SET native_order_id=NULL \
         WHERE place_command_id=$1",
    )
    .bind(&place.command_id)
    .execute(pool)
    .await?;
    let candidates = store.load_unallocated_fills(&first, 0, 100).await?;
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].native_trade_id, "grid-trade-2");
    assert_eq!(
        store
            .load_owned_orders(&first)
            .await?
            .first()
            .and_then(|owner| owner.native_order_id.as_deref()),
        Some("native-grid-1")
    );
    let fill_totals = store.load_grid_fill_totals(&first).await?;
    assert_eq!(
        fill_totals.get(&place.client_order_id),
        Some(&unallocated.quantity)
    );
    assert!(store.list_runtime_instances().await?.iter().any(|row| {
        row.instance.instance_id == first && row.instance.convergence_started_ms.is_some()
    }));

    // Existing rows must survive the repository's restart-time migration sequence.
    sqlx::raw_sql(MIGRATION_0020).execute(pool).await?;
    sqlx::raw_sql(MIGRATION_0021).execute(pool).await?;
    sqlx::raw_sql(MIGRATION_0022).execute(pool).await?;
    sqlx::raw_sql(MIGRATION_0023).execute(pool).await?;
    sqlx::raw_sql(MIGRATION_0024).execute(pool).await?;
    assert_eq!(store.load_owned_orders(&first).await?.len(), 1);

    let current = store
        .load_owned(&owner, &first)
        .await?
        .ok_or("missing grid")?;
    let config_rows_before: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM venue_binance_grid_config_revisions WHERE instance_id=$1",
    )
    .bind(&first)
    .fetch_one(pool)
    .await?;
    let reset_request = GridLifecycleRequest {
        schema_version: GRID_SCHEMA_VERSION,
        request_id: id(13),
        instance_id: first.clone(),
        expected_revision: current.revision,
        action: GridLifecycleAction::Reset,
        risk_confirmed: false,
        positions_remain_acknowledged: true,
    };
    let reset = store
        .request_lifecycle(&owner, &reset_request, now + 15)
        .await?;
    assert_eq!(reset.state, GridInstanceState::ResetRequired);
    assert_eq!(reset.config_revision, current.config_revision + 1);
    assert_eq!(reset.config, current.config);
    assert_eq!(
        reset.attention_code.as_deref(),
        Some("manual_reset_requested")
    );
    assert!(reset.anchor.is_none());
    let synthetic_request_id: String = sqlx::query_scalar(
        "SELECT request_id FROM venue_binance_grid_config_revisions \
         WHERE instance_id=$1 AND config_revision=$2",
    )
    .bind(&first)
    .bind(i64::try_from(reset.config_revision)?)
    .fetch_one(pool)
    .await?;
    assert_ne!(synthetic_request_id, reset_request.request_id);
    let replay = store
        .request_lifecycle(&owner, &reset_request, now + 16)
        .await?;
    assert_eq!(replay, reset);
    let config_rows_after: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM venue_binance_grid_config_revisions WHERE instance_id=$1",
    )
    .bind(&first)
    .fetch_one(pool)
    .await?;
    assert_eq!(config_rows_after, config_rows_before + 1);
    assert!(
        store
            .load_desired_orders(&first)
            .await?
            .is_some_and(|surface| surface.orders.is_empty())
    );
    assert!(
        store
            .has_nonterminal_grid_mutations(&first, Some(reset.plan_revision))
            .await?
    );
    let states = store
        .load_grid_commands(&first, reset.config_revision, reset.plan_revision)
        .await?;
    assert!(states.is_empty());
    let prior_states = store
        .load_grid_commands(&first, current.config_revision, reset.plan_revision)
        .await?;
    assert!(prior_states.iter().any(|command| {
        command.command_id == place.command_id
            && command.state == venue_control_protocol::kol::ExecutorCommandState::Reconciled
    }));
    assert!(prior_states.iter().any(|command| {
        command.command_id == market.command_id
            && command.state == venue_control_protocol::kol::ExecutorCommandState::Cancelled
    }));
    Ok(())
}

fn config() -> GridConfig {
    GridConfig {
        order_notional: Decimal::new(5, 0),
        spacing_rate: Decimal::new(2, 3),
        grid_levels: 20,
        max_total_notional: Decimal::new(500, 0),
        inventory_replenishment: GridInventoryReplenishment {
            enabled: true,
            minimum_inventory_notional: Decimal::new(5, 0),
            target_inventory_notional: Decimal::new(15, 0),
            max_single_replenishment_notional: Decimal::new(5, 0),
        },
        profit_reduction: GridProfitReduction {
            enabled: true,
            inventory_equity_multiple: Decimal::new(3, 0),
            minimum_unrealized_profit_rate: Decimal::new(5, 2),
            reduction_fraction: Decimal::new(3, 1),
            max_single_reduce_notional: Decimal::new(25, 0),
        },
        reset_policy: GridResetPolicy {
            stale_market_ms: 5_000,
            stale_private_ms: 15_000,
            convergence_timeout_ms: 30_000,
            max_consecutive_failures: 3,
        },
    }
}

fn lifecycle(
    request_id: String,
    instance_id: &str,
    revision: u64,
    action: GridLifecycleAction,
) -> GridLifecycleRequest {
    GridLifecycleRequest {
        schema_version: GRID_SCHEMA_VERSION,
        request_id,
        instance_id: instance_id.into(),
        expected_revision: revision,
        action,
        risk_confirmed: matches!(
            action,
            GridLifecycleAction::Start | GridLifecycleAction::Resume
        ),
        positions_remain_acknowledged: matches!(
            action,
            GridLifecycleAction::Stop | GridLifecycleAction::Reset
        ),
    }
}

async fn insert_terminal_close_reservation(
    pool: &PgPool,
    offset: u64,
    state: &str,
    now_ms: u64,
) -> TestResult {
    let sending_ms = (state != "pending").then_some(i64::try_from(now_ms)?);
    let accepted_ms = matches!(state, "accepted" | "reconciled").then_some(i64::try_from(now_ms)?);
    let terminal_ms = (state == "reconciled").then_some(i64::try_from(now_ms)?);
    sqlx::query(
        "INSERT INTO venue_binance_commands \
         (command_id,command_origin,request_id,owner_user_id,trading_account_id,credential_id,\
          symbol,position_side,command_phase,order_kind,order_side,requested_quantity,\
          rule_version,client_order_id,command_state,sending_ms,accepted_ms,terminal_ms,\
          created_ms,updated_ms) VALUES \
         ($1,'terminal',$2,$3,$4,$5,'BTC/USDT','long','close','market','sell','0.0002',\
          'reservation-fixture',$6,$7,$8,$9,$10,$11,$11)",
    )
    .bind(id(50 + offset))
    .bind(id(60 + offset))
    .bind(id(1))
    .bind(id(2))
    .bind(id(3))
    .bind(format!("terminal-close-{offset}"))
    .bind(state)
    .bind(sending_ms)
    .bind(accepted_ms)
    .bind(terminal_ms)
    .bind(i64::try_from(now_ms)?)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_verified_account(pool: &PgPool) -> TestResult {
    let owner = id(1);
    let account = id(2);
    sqlx::query(
        "INSERT INTO venue_users (user_id,username,password_hash,created_ms) \
         VALUES ($1,'grid-owner','fixture',1)",
    )
    .bind(&owner)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO venue_user_trading_accounts \
         (trading_account_id,user_id,venue,exchange_identity_hash) \
         VALUES ($1,$2,'binance',$3)",
    )
    .bind(&account)
    .bind(&owner)
    .bind(vec![2_u8; 32])
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO venue_api_credentials \
         (credential_id,user_id,label,key_fingerprint,masked_key,encrypted_credentials,\
          trading_account_id,verification_json,revision,created_ms) \
         VALUES ($1,$2,'grid','fixture-fingerprint','masked',$3,$4,\
          '{\"verification\":\"verified\"}'::jsonb,1,1)",
    )
    .bind(id(3))
    .bind(owner)
    .bind(vec![3_u8; 32])
    .bind(account)
    .execute(pool)
    .await?;
    Ok(())
}

fn id(value: u64) -> String {
    format!("00000000-0000-4000-8000-{value:012}")
}

struct Fixture {
    pool: PgPool,
    admin: PgPool,
    schema: String,
}

impl Fixture {
    async fn create() -> Result<Option<Self>, Box<dyn std::error::Error>> {
        let Some(url) = std::env::var("VENUE_CONTROL_TEST_DATABASE_URL").ok() else {
            if std::env::var("VENUE_CONTROL_POSTGRES_REQUIRED")
                .ok()
                .as_deref()
                == Some("1")
            {
                return Err("Grid PostgreSQL test database is required".into());
            }
            eprintln!("SKIP: Grid PostgreSQL test database is not configured");
            return Ok(None);
        };
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let schema = format!("venue_grid_{}_{nonce}", std::process::id());
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
            .connect(&url)
            .await?;
        for _ in 0..2 {
            for migration in [
                MIGRATION_0001,
                MIGRATION_0015,
                MIGRATION_0017,
                MIGRATION_0018,
                MIGRATION_0019,
                MIGRATION_0020,
                MIGRATION_0021,
                MIGRATION_0022,
                MIGRATION_0023,
                MIGRATION_0024,
            ] {
                sqlx::raw_sql(migration).execute(&pool).await?;
            }
        }
        Ok(Some(Self {
            pool,
            admin,
            schema,
        }))
    }

    async fn cleanup(self) -> TestResult {
        self.pool.close().await;
        self.admin
            .execute(format!("DROP SCHEMA {} CASCADE", self.schema).as_str())
            .await?;
        self.admin.close().await;
        Ok(())
    }
}
