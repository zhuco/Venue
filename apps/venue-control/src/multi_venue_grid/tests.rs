use super::store::GridOrderRow;
use super::*;
use rust_decimal::Decimal;
use venue_domain::{
    Amount, Asset, ExecutionCommand, Instrument, InstrumentMetadata, MarketKind, OrderState,
    PositionSide, Precision, Price, Symbol,
};
use venue_execution::{
    DurableMarketFacts, DurableOrderObservation, SignedAccountOrderFact, SignedAccountPositionFact,
    SignedAccountPositionMode, SignedAccountSnapshot,
};
use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};
use venue_strategies::hedged_grid::{GridPlannerConfig, GridResetPolicy};

#[test]
fn published_grid_configuration_fixture_is_valid() -> Result<(), Box<dyn std::error::Error>> {
    let config: StrategyGridConfig =
        serde_json::from_str(include_str!("../../tests/fixtures/multi_venue_grid.json"))?;
    config.planner.validate()?;
    assert!(config.net_direction.is_none());
    let mut invalid = config;
    invalid.planner.reset_policy.failure_threshold = 0;
    assert!(invalid.planner.validate().is_err());
    Ok(())
}

fn fixture() -> Result<
    (
        StrategyGridRecord,
        SignedAccountSnapshot,
        DurableMarketFacts,
    ),
    Box<dyn std::error::Error>,
> {
    let symbol = Symbol::new("BTC", "USDT")?;
    let quote = Asset::new("USDT")?;
    let binding = GatewayBinding::new(
        VenueId::Bybit,
        GatewayMode::Live,
        "00000000-0000-4000-8000-000000000001",
        symbol.clone(),
    )?;
    let record = StrategyGridRecord {
        instance_id: "grid1".into(),
        owner_user_id: "user1".into(),
        credential_id: "cred1".into(),
        trading_account_id: binding.trading_account_id.clone(),
        venue: VenueId::Bybit,
        symbol: symbol.clone(),
        lifecycle: "running".into(),
        revision: 1,
        plan_sequence: 0,
        rolling_anchor: None,
        blocked_reason: None,
        convergence_pending_since_ms: None,
        consecutive_failures: 0,
        last_failed_strategy_sequence: 0,
        config: StrategyGridConfig {
            net_direction: None,
            planner: GridPlannerConfig {
                instance_id: "grid1".into(),
                revision: 1,
                symbol: symbol.clone(),
                order_notional: Amount::new(quote.clone(), Decimal::from(5)),
                maximum_grid_notional: Amount::new(quote.clone(), Decimal::from(100)),
                inventory_risk: None,
                spacing_rate: Decimal::new(1, 2),
                grid_count: 2,
                replenishment: None,
                profit_reduction: None,
                reset_policy: GridResetPolicy {
                    max_market_age_ms: 5000,
                    max_private_age_ms: 5000,
                    convergence_timeout_ms: 30000,
                    failure_threshold: 3,
                },
            },
        },
    };
    let positions = vec![PositionSide::Long, PositionSide::Short]
        .into_iter()
        .map(|side| SignedAccountPositionFact {
            symbol: symbol.clone(),
            position_side: side,
            quantity: Decimal::ONE,
            entry_price: Some(Decimal::from(100)),
            mark_price: Some(Decimal::from(100)),
        })
        .collect();
    let snapshot = SignedAccountSnapshot::complete(
        binding.clone(),
        1000,
        1,
        1,
        1,
        SignedAccountPositionMode::Hedge,
        vec![],
        positions,
        "cursor".into(),
        vec![],
    )?;
    let metadata = InstrumentMetadata::new(
        Instrument {
            symbol,
            market: MarketKind::LinearPerpetual,
            settlement_asset: Some(quote.clone()),
            generation: 1,
            price_tick: Price::new(Decimal::ONE)?,
            quantity_step: Decimal::new(1, 2),
            minimum_notional: Amount::new(quote, Decimal::from(1)),
        },
        Precision::new(Decimal::ONE, Decimal::ONE)?,
        Precision::new(Decimal::new(1, 2), Decimal::new(1, 2))?,
        None,
        true,
    )?;
    Ok((
        record,
        snapshot,
        DurableMarketFacts {
            binding,
            metadata,
            reference_price: Price::new(Decimal::from(100))?,
            observed_at_ms: 1000,
            maximum_quantity: Some(Decimal::from(10)),
            maximum_price: Some(Price::new(Decimal::from(1000))?),
        },
    ))
}

fn resting(
    record: &StrategyGridRecord,
    snapshot: &SignedAccountSnapshot,
    market: &DurableMarketFacts,
) -> Result<
    (
        Vec<GridOrderRow>,
        Vec<DurableOrderObservation>,
        SignedAccountSnapshot,
    ),
    Box<dyn std::error::Error>,
> {
    let work = planner::plan(record, &[], snapshot, market, &[], 1001)?;
    let mut rows = Vec::new();
    let mut observed = Vec::new();
    let mut facts = Vec::new();
    for (i, (command, intent)) in work.commands.into_iter().enumerate() {
        let ExecutionCommand::PlaceLimit(order) = &command else {
            return Err("expected limit".into());
        };
        let native = format!("native{i}");
        observed.push(DurableOrderObservation {
            client_order_id: order.client_order_id.as_str().into(),
            native_order_id: native.clone(),
            state: OrderState::New,
            filled_quantity: Decimal::ZERO,
            average_price: venue_domain::domain::FieldState::Missing,
            cumulative_fee: venue_domain::domain::FieldState::Missing,
        });
        facts.push(SignedAccountOrderFact {
            client_order_id: order.client_order_id.as_str().into(),
            venue_order_id: Some(native.clone()),
            symbol: record.symbol.clone(),
            family: venue_domain::NativeOrderFamily::UmOrder,
            side: order.side,
            position_side: order.position_side,
            quantity: order.quantity,
            limit_price: Some(order.limit_price.value()),
            time_in_force: Some(order.time_in_force),
            created_at_ms: Some(1000),
            reduce_only: order.reduce_only,
            owner: Some(order.owner.clone()),
            external: false,
            state: Some(OrderState::New),
            filled_quantity: Some(Decimal::ZERO),
        });
        rows.push(GridOrderRow {
            command,
            native_id: Some(native),
            intent: intent.ok_or("missing intent")?,
            observed_filled: Decimal::ZERO,
            ledger_state: "reconciled".into(),
        });
    }
    let next = SignedAccountSnapshot::complete(
        snapshot.binding().clone(),
        1000,
        1,
        1,
        1,
        snapshot.position_mode(),
        facts,
        snapshot.positions().to_vec(),
        "cursor".into(),
        vec![],
    )?;
    Ok((rows, observed, next))
}

#[test]
fn grid_restart_keeps_surface_and_pause_only_cancels_owned_orders()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut record, snapshot, market) = fixture()?;
    let initial = planner::plan(&record, &[], &snapshot, &market, &[], 1001)?;
    record.rolling_anchor = initial.anchor;
    record.plan_sequence = 1;
    let (rows, observed, snapshot) = resting(&record, &snapshot, &market)?;
    let same = planner::plan(&record, &rows, &snapshot, &market, &observed, 1001)?;
    assert!(same.commands.is_empty());
    record.lifecycle = "pausing".into();
    let pause = planner::plan(&record, &rows, &snapshot, &market, &observed, 1001)?;
    assert_eq!(pause.commands.len(), rows.len());
    assert!(
        pause
            .commands
            .iter()
            .all(|(c, _)| matches!(c, ExecutionCommand::Cancel(_)))
    );
    assert!(pause.lifecycle.is_none());
    assert!(planner::plan(&record, &rows, &snapshot, &market, &[], 1001).is_err());
    Ok(())
}

#[test]
fn grid_reconnect_preserves_rules_identity_but_rule_changes_reset()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut record, snapshot, mut market) = fixture()?;
    let initial = planner::plan(&record, &[], &snapshot, &market, &[], 1001)?;
    let ids: std::collections::BTreeSet<_> = initial
        .commands
        .iter()
        .filter_map(|(command, _)| command.native_client_id())
        .map(|id| id.as_str())
        .collect();
    assert_eq!(ids.len(), initial.commands.len());
    assert!(ids.iter().all(|id| id.len() <= 28));
    record.rolling_anchor = initial.anchor;
    record.plan_sequence = 1;
    let (rows, observed, snapshot) = resting(&record, &snapshot, &market)?;
    market.metadata.instrument.generation = 1_788_769_968_345;
    market.reference_price = Price::new(Decimal::from(101))?;
    let same = planner::plan(&record, &rows, &snapshot, &market, &observed, 1001)?;
    assert!(same.commands.is_empty());
    assert_eq!(same.anchor, record.rolling_anchor);
    market.maximum_quantity = Some(Decimal::from(99));
    let changed = planner::plan(&record, &rows, &snapshot, &market, &observed, 1001)?;
    assert_eq!(changed.lifecycle.as_deref(), Some("resetting"));
    assert!(
        changed
            .commands
            .iter()
            .all(|(c, _)| matches!(c, ExecutionCommand::Cancel(_)))
    );
    Ok(())
}

#[test]
fn grid_complete_fill_rolls_and_unknown_order_never_becomes_a_fill()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut record, base, market) = fixture()?;
    record.rolling_anchor = planner::plan(&record, &[], &base, &market, &[], 1001)?.anchor;
    let (rows, mut observed, snapshot) = resting(&record, &base, &market)?;
    let index = rows
        .iter()
        .position(|r| !r.intent.reduce_only)
        .ok_or("missing open")?;
    observed[index].state = OrderState::Filled;
    observed[index].filled_quantity = rows[index].intent.quantity;
    let facts = snapshot
        .open_orders()
        .iter()
        .filter(|f| f.venue_order_id.as_deref() != Some(observed[index].native_order_id.as_str()))
        .cloned()
        .collect();
    let snapshot = SignedAccountSnapshot::complete(
        base.binding().clone(),
        1000,
        1,
        1,
        1,
        SignedAccountPositionMode::Hedge,
        facts,
        base.positions().to_vec(),
        "cursor2".into(),
        vec![],
    )?;
    let work = planner::plan(&record, &rows, &snapshot, &market, &observed, 1001)?;
    assert!(!work.commands.is_empty());
    assert!(work.observations.iter().any(|(_, _, terminal)| *terminal));
    observed[index].state = OrderState::Unknown;
    assert!(planner::plan(&record, &rows, &snapshot, &market, &observed, 1001).is_err());
    Ok(())
}

#[test]
fn partial_fill_is_consumed_once_without_replacing_remaining_order()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut record, base, market) = fixture()?;
    record.rolling_anchor = planner::plan(&record, &[], &base, &market, &[], 1001)?.anchor;
    let (mut rows, mut observed, snapshot) = resting(&record, &base, &market)?;
    let index = rows
        .iter()
        .position(|r| !r.intent.reduce_only)
        .ok_or("missing open")?;
    let filled = Decimal::new(1, 2);
    observed[index].state = OrderState::PartiallyFilled;
    observed[index].filled_quantity = filled;
    let mut facts = snapshot.open_orders().to_vec();
    facts[index].filled_quantity = Some(filled);
    facts[index].state = Some(OrderState::PartiallyFilled);
    let snapshot = SignedAccountSnapshot::complete(
        base.binding().clone(),
        1000,
        1,
        1,
        1,
        SignedAccountPositionMode::Hedge,
        facts,
        base.positions().to_vec(),
        "partial".into(),
        vec![],
    )?;
    let first = planner::plan(&record, &rows, &snapshot, &market, &observed, 1001)?;
    assert!(first.commands.is_empty());
    assert_eq!(first.observations[index].1, filled);
    rows[index].observed_filled = filled;
    let again = planner::plan(&record, &rows, &snapshot, &market, &observed, 1001)?;
    assert!(again.commands.is_empty());
    observed[index].filled_quantity = Decimal::ZERO;
    assert!(planner::plan(&record, &rows, &snapshot, &market, &observed, 1001).is_err());
    Ok(())
}

#[test]
fn net_grid_keeps_explicit_direction_and_rejects_opposite_inventory()
-> Result<(), Box<dyn std::error::Error>> {
    use venue_strategies::hedged_grid::GridPosition;
    let (mut record, _, mut market) = fixture()?;
    let symbol = Symbol::new("BTC", "USDC")?;
    let quote = Asset::new("USDC")?;
    record.venue = VenueId::Hyperliquid;
    record.symbol = symbol.clone();
    record.config.planner.symbol = symbol.clone();
    record.config.net_direction = Some(GridPosition::Long);
    record.config.planner.order_notional.asset = quote.clone();
    record.config.planner.maximum_grid_notional.asset = quote.clone();
    market.binding = GatewayBinding::new(
        record.venue,
        GatewayMode::Live,
        &record.trading_account_id,
        symbol.clone(),
    )?;
    market.metadata.instrument.symbol = symbol.clone();
    market.metadata.instrument.settlement_asset = Some(quote.clone());
    market.metadata.instrument.minimum_notional.asset = quote;
    let position = SignedAccountPositionFact {
        symbol,
        position_side: PositionSide::Net,
        quantity: Decimal::ONE,
        entry_price: Some(Decimal::from(100)),
        mark_price: Some(Decimal::from(100)),
    };
    let snapshot = SignedAccountSnapshot::complete(
        market.binding.clone(),
        1000,
        1,
        1,
        1,
        SignedAccountPositionMode::Net,
        vec![],
        vec![position.clone()],
        "net".into(),
        vec![],
    )?;
    let work = planner::plan(&record, &[], &snapshot, &market, &[], 1001)?;
    assert!(!work.commands.is_empty());
    for (command, intent) in work.commands {
        let ExecutionCommand::PlaceLimit(order) = command else {
            return Err("expected limit".into());
        };
        assert_eq!(order.position_side, PositionSide::Net);
        assert_eq!(
            intent.ok_or("intent missing")?.key.position,
            GridPosition::Long
        );
    }
    let mut reverse = position;
    reverse.quantity = -Decimal::ONE;
    let snapshot = SignedAccountSnapshot::complete(
        market.binding.clone(),
        1000,
        1,
        1,
        1,
        SignedAccountPositionMode::Net,
        vec![],
        vec![reverse],
        "net".into(),
        vec![],
    )?;
    assert!(planner::plan(&record, &[], &snapshot, &market, &[], 1001).is_err());
    Ok(())
}

#[tokio::test]
async fn grid_commands_and_observations_commit_atomically_with_lifecycle_fence()
-> Result<(), Box<dyn std::error::Error>> {
    use sqlx::{Executor, postgres::PgPoolOptions};
    let Ok(url) = std::env::var("VENUE_CONTROL_TEST_DATABASE_URL") else {
        if std::env::var("VENUE_CONTROL_POSTGRES_REQUIRED")
            .ok()
            .as_deref()
            == Some("1")
        {
            return Err("QA PostgreSQL is required".into());
        }
        eprintln!("SKIP: grid PostgreSQL QA URL not configured");
        return Ok(());
    };
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await?;
    let schema = format!(
        "venue_grid_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    );
    admin
        .execute(format!("CREATE SCHEMA {schema}").as_str())
        .await?;
    let scoped = schema.clone();
    let pool = PgPoolOptions::new()
        .max_connections(3)
        .after_connect(move |c, _| {
            let sql = format!("SET search_path TO {scoped}");
            Box::pin(async move {
                c.execute(sql.as_str()).await?;
                Ok(())
            })
        })
        .connect(&url)
        .await?;
    crate::install_control_schema(&pool).await?;
    let (record, snapshot, market) = fixture()?;
    sqlx::query("INSERT INTO venue_users(user_id,username,password_hash,created_ms) VALUES('user1','grid_user','fixture',1)").execute(&pool).await?;
    sqlx::query("INSERT INTO venue_user_trading_accounts(trading_account_id,user_id,venue,exchange_identity_hash) VALUES($1,'user1','bybit',decode('01','hex'))").bind(&record.trading_account_id).execute(&pool).await?;
    sqlx::query("INSERT INTO venue_api_credentials(credential_id,user_id,label,key_fingerprint,masked_key,encrypted_credentials,trading_account_id,venue,verification_json,strategy_limits,created_ms) VALUES('cred1','user1','fixture',decode('02','hex'),'***',decode('00','hex'),$1,'bybit','{\"verification\":\"verified\",\"strategy_execution\":true}','{\"max_order_notional\":\"100\",\"max_symbol_notional\":\"1000\"}',1)").bind(&record.trading_account_id).execute(&pool).await?;
    let store = StrategyGridStore::new(pool.clone());
    store
        .create("user1", "cred1", record.config.clone(), 1000)
        .await?;
    store.lifecycle("user1", "grid1", "start", 1001).await?;
    let running = store.get("user1", "grid1").await?;
    let work = planner::plan(&running, &[], &snapshot, &market, &[], 1001)?;
    let expected = i64::try_from(work.commands.len())?;
    store.apply(&running, work, 1001).await?;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM venue_strategy_grid_orders")
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, expected);
    let stale = planner::plan(&running, &[], &snapshot, &market, &[], 1001)?;
    assert!(store.apply(&running, stale, 1002).await.is_err());
    store.lifecycle("user1", "grid1", "pause", 1003).await?;
    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM venue_binance_commands WHERE command_state='pending'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(pending, 0);
    assert_eq!(store.get("user1", "grid1").await?.lifecycle, "pausing");

    let pausing = store.get("user1", "grid1").await?;
    let cancelled_orders = store.orders("grid1").await?;
    let paused_work = planner::plan(&pausing, &cancelled_orders, &snapshot, &market, &[], 1004)?;
    store.apply(&pausing, paused_work, 1004).await?;
    assert_eq!(store.get("user1", "grid1").await?.lifecycle, "paused");
    store.lifecycle("user1", "grid1", "resume", 1005).await?;
    let resumed = store.get("user1", "grid1").await?;
    assert_eq!(resumed.convergence_pending_since_ms, None);
    assert_eq!(resumed.consecutive_failures, 0);
    let resumed_work = planner::plan(&resumed, &[], &snapshot, &market, &[], 1006)?;
    store.apply(&resumed, resumed_work, 1006).await?;
    let converging = store.get("user1", "grid1").await?;
    assert_eq!(converging.convergence_pending_since_ms, Some(1006));
    sqlx::query("WITH failures AS (SELECT command_id FROM venue_binance_commands WHERE command_state='pending' ORDER BY command_id LIMIT 2) UPDATE venue_binance_commands c SET command_state='rejected' FROM failures f WHERE c.command_id=f.command_id")
        .execute(&pool).await?;
    assert_eq!(
        store.note_new_rejections(&converging, 1007).await?,
        (true, false)
    );
    let converging = store.get("user1", "grid1").await?;
    assert_eq!(converging.consecutive_failures, 2);
    assert_eq!(
        store.note_new_rejections(&converging, 1008).await?,
        (false, false)
    );
    sqlx::query("UPDATE venue_binance_commands SET command_state='rejected' WHERE command_id=(SELECT command_id FROM venue_binance_commands WHERE command_state='pending' ORDER BY command_id LIMIT 1)")
        .execute(&pool).await?;
    sqlx::query("UPDATE venue_binance_commands SET command_state='sending' WHERE command_id=(SELECT command_id FROM venue_binance_commands WHERE command_state='pending' ORDER BY command_id LIMIT 1)")
        .execute(&pool).await?;
    let sending: serde_json::Value = sqlx::query_scalar(
        "SELECT strategy_command FROM venue_binance_commands WHERE command_state='sending'",
    )
    .fetch_one(&pool)
    .await?;
    let sending: ExecutionCommand = serde_json::from_value(sending)?;
    let runtime = StrategyGridRuntime::new(
        pool.clone(),
        crate::multi_venue_credentials::StrategyCredentialStore::new(
            pool.clone(),
            crate::accounts::CredentialCipher::from_key(&[17; 32])?,
        ),
    );
    runtime.invalidate_pending(&sending, 1008).await?;
    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM venue_binance_commands WHERE command_state='pending'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(pending, 0);
    let (counted, paused) = store.note_new_rejections(&converging, 1009).await?;
    assert!(counted && paused);
    let gated = store.get("user1", "grid1").await?;
    assert_eq!(gated.lifecycle, "pausing");
    assert_eq!(gated.consecutive_failures, 3);
    let active_states: Vec<String> = sqlx::query_scalar("SELECT command_state FROM venue_binance_commands WHERE command_state IN ('pending','sending') ORDER BY command_state")
        .fetch_all(&pool).await?;
    assert_eq!(active_states, vec!["sending"]);

    for index in 2..=19 {
        let mut config = record.config.clone();
        config.planner.instance_id = format!("grid{index}");
        let base = format!("ASSET{index}");
        config.planner.symbol = Symbol::new(&base, "USDT")?;
        store.create("user1", "cred1", config, 1100 + index).await?;
    }
    let mut twentieth = record.config.clone();
    twentieth.planner.instance_id = "grid20".into();
    twentieth.planner.symbol = Symbol::new("ASSET20", "USDT")?;
    let mut twenty_first = record.config.clone();
    twenty_first.planner.instance_id = "grid21".into();
    twenty_first.planner.symbol = Symbol::new("ASSET21", "USDT")?;
    let store_a = store.clone();
    let store_b = store.clone();
    let (a, b) = tokio::join!(
        store_a.create("user1", "cred1", twentieth, 1200),
        store_b.create("user1", "cred1", twenty_first, 1200)
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    let admitted: i64 =
        sqlx::query_scalar("SELECT count(*) FROM venue_strategy_grids WHERE trading_account_id=$1")
            .bind(&record.trading_account_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(admitted, 20);
    pool.close().await;
    admin
        .execute(format!("DROP SCHEMA {schema} CASCADE").as_str())
        .await?;
    admin.close().await;
    Ok(())
}

#[test]
fn unpublished_upper_filters_use_aligned_bridge_limits() -> Result<(), Box<dyn std::error::Error>> {
    let (record, snapshot, mut market) = fixture()?;
    market.maximum_quantity = None;
    market.maximum_price = None;
    market.metadata.price = Precision::new(Decimal::new(25, 2), Decimal::new(25, 2))?;
    market.metadata.instrument.price_tick = Price::new(Decimal::new(25, 2))?;
    let work = planner::plan(&record, &[], &snapshot, &market, &[], 1001)?;
    assert!(!work.commands.is_empty());
    assert!(market.maximum_price.is_none() && market.maximum_quantity.is_none());
    Ok(())
}

#[test]
fn simultaneous_fills_fit_bounded_cancel_then_replace_queue()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut record, base, market) = fixture()?;
    record.config.planner.grid_count = 4;
    record.rolling_anchor = planner::plan(&record, &[], &base, &market, &[], 1001)?.anchor;
    let (rows, mut observations, snapshot) = resting(&record, &base, &market)?;
    let filled: Vec<usize> = rows
        .iter()
        .enumerate()
        .filter(|(_, r)| !r.intent.reduce_only && r.intent.key.level <= 3)
        .map(|(i, _)| i)
        .collect();
    assert_eq!(filled.len(), 6);
    for i in &filled {
        observations[*i].state = OrderState::Filled;
        observations[*i].filled_quantity = rows[*i].intent.quantity;
    }
    let facts = snapshot
        .open_orders()
        .iter()
        .enumerate()
        .filter(|(i, _)| !filled.contains(i))
        .map(|(_, f)| f.clone())
        .collect();
    let snapshot = SignedAccountSnapshot::complete(
        base.binding().clone(),
        1000,
        1,
        1,
        1,
        SignedAccountPositionMode::Hedge,
        facts,
        base.positions().to_vec(),
        "six_fills".into(),
        vec![],
    )?;
    let work = planner::plan(&record, &rows, &snapshot, &market, &observations, 1001)?;
    assert!(work.commands.len() > 16);
    assert!(work.commands.len() <= crate::multi_venue_store::MAX_STRATEGY_QUEUE_DEPTH);
    let first_place = work
        .commands
        .iter()
        .position(|(c, _)| matches!(c, ExecutionCommand::PlaceLimit(_)))
        .ok_or("no replacement")?;
    assert!(
        work.commands[..first_place]
            .iter()
            .all(|(c, _)| matches!(c, ExecutionCommand::Cancel(_)))
    );
    assert!(
        work.commands[first_place..]
            .iter()
            .all(|(c, _)| matches!(c, ExecutionCommand::PlaceLimit(_)))
    );
    Ok(())
}
