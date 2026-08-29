use rust_decimal::Decimal;
use tempfile::tempdir;
use venue::{
    domain::{DomainEvent, FieldState, PositionSide, Symbol},
    exchange::binance::{RecentFillsCursor, RecentFillsReadback},
    execution::{FillRecoveryBatch, FillRecoveryCoordinator, FillRecoveryError},
    storage::{FillCursor, FillCursorStore, Journal},
};

fn durable_cursor(
    generation: u64,
    observed_through_ms: u64,
    last_trade_id: Option<u64>,
    last_event_time_ms: Option<u64>,
) -> Result<FillCursor, Box<dyn std::error::Error>> {
    Ok(FillCursor {
        schema_version: 1,
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        symbol: "BTC/USDT".parse::<Symbol>()?,
        generation,
        connection_epoch: 1,
        observed_through_ms,
        last_trade_id,
        last_event_time_ms,
    })
}

fn hedge_fill_payload() -> &'static str {
    r#"[{"id":10,"orderId":20,"symbol":"BTCUSDT","side":"BUY","positionSide":"LONG","qty":"1","price":"100","commission":"0.01","commissionAsset":"USDT","realizedPnl":"0","maker":false,"time":100},{"id":11,"orderId":21,"symbol":"BTCUSDT","side":"SELL","positionSide":"SHORT","qty":"2","price":"101","commission":"0.01","commissionAsset":"USDT","realizedPnl":"0","maker":false,"time":101}]"#
}

fn long_fill_payload() -> &'static str {
    r#"[{"id":10,"orderId":20,"symbol":"BTCUSDT","side":"BUY","positionSide":"LONG","qty":"1","price":"100","commission":"0.01","commissionAsset":"USDT","realizedPnl":"0","maker":false,"time":100}]"#
}

fn eth_long_fill_payload() -> &'static str {
    r#"[{"id":10,"orderId":30,"symbol":"ETHUSDT","side":"BUY","positionSide":"LONG","qty":"1","price":"100","commission":"0.01","commissionAsset":"USDT","realizedPnl":"0","maker":false,"time":100}]"#
}

fn batch<'a>(
    symbol: &'a Symbol,
    payload: &str,
    cursor: RecentFillsCursor,
    pages: u32,
) -> FillRecoveryBatch<'a> {
    FillRecoveryBatch {
        exchange: "binance",
        account: "primary",
        symbol,
        readback: RecentFillsReadback {
            payload: payload.to_owned(),
            cursor,
            pages,
        },
        received_at_ms: 200,
        native_epoch: 1,
        hub_bootstrap_generation: 1,
    }
}

#[test]
fn facts_are_durable_before_the_fill_cursor_advances() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let mut facts = Journal::open(directory.path().join("facts.jsonl"))?;
    let cursor_store = FillCursorStore::new(directory.path().join("fill-cursor.json"));
    let initial = durable_cursor(1, 100, None, None)?;
    let symbol = initial.symbol.clone();
    cursor_store.compare_and_swap(None, &initial)?;
    let mut coordinator = FillRecoveryCoordinator::default();

    let report = coordinator.accept_batch(
        &mut facts,
        &cursor_store,
        batch(
            &symbol,
            hedge_fill_payload(),
            RecentFillsCursor {
                observed_through_ms: 200,
                last_trade_id: Some(11),
                last_event_time_ms: Some(101),
            },
            1,
        ),
    )?;

    assert_eq!(report.reconciliation.accepted, 2);
    assert_eq!(facts.recover()?.entries.len(), 2);
    assert_eq!(
        cursor_store.load()?,
        Some(durable_cursor(2, 200, Some(11), Some(101))?)
    );
    let recovered = facts.recover()?;
    let fill = match &recovered.entries[0].record.event {
        DomainEvent::Fill(fill) => fill,
        event => return Err(format!("unexpected fact: {event:?}").into()),
    };
    assert_eq!(fill.position_side, FieldState::Known(PositionSide::Long));
    assert_eq!(fill.quantity, Decimal::ONE);
    let short_fill = match &recovered.entries[1].record.event {
        DomainEvent::Fill(fill) => fill,
        event => return Err(format!("unexpected fact: {event:?}").into()),
    };
    assert_eq!(
        short_fill.position_side,
        FieldState::Known(PositionSide::Short)
    );
    Ok(())
}

#[test]
fn parse_or_watermark_failure_never_moves_the_cursor() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let mut facts = Journal::open(directory.path().join("facts.jsonl"))?;
    let cursor_store = FillCursorStore::new(directory.path().join("fill-cursor.json"));
    let initial = durable_cursor(1, 100, None, None)?;
    let symbol = initial.symbol.clone();
    cursor_store.compare_and_swap(None, &initial)?;
    let mut coordinator = FillRecoveryCoordinator::default();

    assert!(
        coordinator
            .accept_batch(
                &mut facts,
                &cursor_store,
                batch(
                    &symbol,
                    "not-json",
                    RecentFillsCursor {
                        observed_through_ms: 200,
                        last_trade_id: Some(11),
                        last_event_time_ms: Some(101),
                    },
                    1,
                ),
            )
            .is_err()
    );
    assert_eq!(cursor_store.load()?, Some(initial.clone()));
    assert!(facts.recover()?.entries.is_empty());

    assert!(matches!(
        coordinator.accept_batch(
            &mut facts,
            &cursor_store,
            batch(
                &symbol,
                "[]",
                RecentFillsCursor {
                    observed_through_ms: 99,
                    last_trade_id: None,
                    last_event_time_ms: None,
                },
                1,
            ),
        ),
        Err(FillRecoveryError::CursorRegression)
    ));
    assert_eq!(cursor_store.load()?, Some(initial));
    Ok(())
}

#[test]
fn repeated_batch_is_idempotent_and_preserves_its_durable_cursor()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let mut facts = Journal::open(directory.path().join("facts.jsonl"))?;
    let cursor_store = FillCursorStore::new(directory.path().join("fill-cursor.json"));
    let initial = durable_cursor(1, 100, None, None)?;
    let symbol = initial.symbol.clone();
    cursor_store.compare_and_swap(None, &initial)?;
    let mut coordinator = FillRecoveryCoordinator::default();
    let readback_cursor = RecentFillsCursor {
        observed_through_ms: 200,
        last_trade_id: Some(11),
        last_event_time_ms: Some(101),
    };

    coordinator.accept_batch(
        &mut facts,
        &cursor_store,
        batch(&symbol, hedge_fill_payload(), readback_cursor, 1),
    )?;
    let repeated = coordinator.accept_batch(
        &mut facts,
        &cursor_store,
        batch(&symbol, hedge_fill_payload(), readback_cursor, 1),
    )?;
    assert!(repeated.cursor_already_committed);
    assert_eq!(facts.recover()?.entries.len(), 2);
    assert_eq!(
        cursor_store.load()?,
        Some(durable_cursor(2, 200, Some(11), Some(101))?)
    );
    Ok(())
}

#[test]
fn cas_rejects_old_generation_and_previous_watermark() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let cursor_store = FillCursorStore::new(directory.path().join("fill-cursor.json"));
    let initial = durable_cursor(1, 100, None, None)?;
    cursor_store.compare_and_swap(None, &initial)?;

    let old_generation = durable_cursor(1, 200, Some(10), Some(100))?;
    assert!(matches!(
        cursor_store.compare_and_swap(Some(&initial), &old_generation),
        Err(venue::storage::FillCursorError::Generation)
    ));
    let old_watermark = durable_cursor(2, 99, None, None)?;
    assert!(matches!(
        cursor_store.compare_and_swap(Some(&initial), &old_watermark),
        Err(venue::storage::FillCursorError::Regression)
    ));
    assert_eq!(cursor_store.load()?, Some(initial));
    Ok(())
}

#[test]
fn conflicting_replay_fails_closed_without_cursor_advance() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempdir()?;
    let mut facts = Journal::open(directory.path().join("facts.jsonl"))?;
    let cursor_store = FillCursorStore::new(directory.path().join("fill-cursor.json"));
    let initial = durable_cursor(1, 100, None, None)?;
    let symbol = initial.symbol.clone();
    cursor_store.compare_and_swap(None, &initial)?;
    let mut coordinator = FillRecoveryCoordinator::default();
    coordinator.accept_batch(
        &mut facts,
        &cursor_store,
        batch(
            &symbol,
            long_fill_payload(),
            RecentFillsCursor {
                observed_through_ms: 200,
                last_trade_id: Some(10),
                last_event_time_ms: Some(100),
            },
            1,
        ),
    )?;
    let committed = cursor_store.load()?;
    assert!(coordinator
        .accept_batch(
            &mut facts,
            &cursor_store,
            batch(
                &symbol,
                r#"[{"id":10,"orderId":20,"symbol":"BTCUSDT","side":"BUY","positionSide":"LONG","qty":"1","price":"101","commission":"0.01","commissionAsset":"USDT","maker":false,"time":100}]"#,
                RecentFillsCursor {
                    observed_through_ms: 300,
                    last_trade_id: Some(11),
                    last_event_time_ms: Some(110),
                },
                1,
            ),
        )
        .is_err());
    assert_eq!(cursor_store.load()?, committed);
    assert_eq!(facts.recover()?.entries.len(), 1);
    Ok(())
}

#[test]
fn native_fill_ids_are_scoped_by_symbol() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let mut facts = Journal::open(directory.path().join("facts.jsonl"))?;
    let eth_store = FillCursorStore::new(directory.path().join("eth-cursor.json"));
    let btc_store = FillCursorStore::new(directory.path().join("btc-cursor.json"));
    let mut eth_initial = durable_cursor(1, 100, None, None)?;
    eth_initial.symbol = "ETH/USDT".parse::<Symbol>()?;
    let btc_initial = durable_cursor(1, 100, None, None)?;
    eth_store.compare_and_swap(None, &eth_initial)?;
    btc_store.compare_and_swap(None, &btc_initial)?;
    let mut coordinator = FillRecoveryCoordinator::default();
    let next = RecentFillsCursor {
        observed_through_ms: 200,
        last_trade_id: Some(10),
        last_event_time_ms: Some(100),
    };

    coordinator.accept_batch(
        &mut facts,
        &eth_store,
        batch(&eth_initial.symbol, eth_long_fill_payload(), next, 1),
    )?;
    coordinator.accept_batch(
        &mut facts,
        &btc_store,
        batch(&btc_initial.symbol, long_fill_payload(), next, 1),
    )?;

    assert_eq!(facts.recover()?.entries.len(), 2);
    Ok(())
}

#[test]
fn restart_maps_a_reset_native_epoch_above_the_durable_epoch_floor()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let mut facts = Journal::open(directory.path().join("facts.jsonl"))?;
    let cursor_store = FillCursorStore::new(directory.path().join("fill-cursor.json"));
    let initial = durable_cursor(1, 100, None, None)?;
    let symbol = initial.symbol.clone();
    cursor_store.compare_and_swap(None, &initial)?;

    let mut recovered = FillRecoveryCoordinator::recover(&facts, &cursor_store)?;
    recovered.accept_batch(
        &mut facts,
        &cursor_store,
        batch(
            &symbol,
            "[]",
            RecentFillsCursor {
                observed_through_ms: 200,
                last_trade_id: None,
                last_event_time_ms: None,
            },
            1,
        ),
    )?;
    let committed = cursor_store.load()?.ok_or("missing cursor")?;
    assert_eq!(committed.connection_epoch, 2);
    assert!(recovered.epoch_gate().allows_ready(1));
    assert_eq!(recovered.epoch_gate().hub_bootstrap_generation(), Some(1));
    Ok(())
}

#[test]
fn a_coordinator_without_the_durable_floor_cannot_overwrite_a_newer_epoch()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let mut facts = Journal::open(directory.path().join("facts.jsonl"))?;
    let cursor_store = FillCursorStore::new(directory.path().join("fill-cursor.json"));
    let mut current = durable_cursor(2, 200, None, None)?;
    current.connection_epoch = 2;
    let symbol = current.symbol.clone();
    cursor_store.compare_and_swap(None, &current)?;
    let mut stale = FillRecoveryCoordinator::default();

    assert!(matches!(
        stale.accept_batch(
            &mut facts,
            &cursor_store,
            batch(
                &symbol,
                "[]",
                RecentFillsCursor {
                    observed_through_ms: 300,
                    last_trade_id: None,
                    last_event_time_ms: None,
                },
                1,
            ),
        ),
        Err(FillRecoveryError::Epoch)
    ));
    assert_eq!(cursor_store.load()?, Some(current));
    Ok(())
}
