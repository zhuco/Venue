//! Stage 4 regression contracts distilled from the frozen VenueCore Binance PAPI tests.
//! These tests exercise only the new shell's public contracts; no legacy fixture or runtime
//! dependency is introduced.

use rust_decimal::Decimal;
use tempfile::tempdir;
use venue::{
    domain::{
        Amount, Asset, CancelCommand, CommandId, FieldState, Instrument, MarketKind,
        NativeOrderFamily, OrderCommand, OrderOwner, OrderPurpose, OrderSide, OrderState, Position,
        PositionSide, Price, StopMarketCloseAllCommand,
    },
    exchange::{
        binance_private::{parse_fills, parse_order},
        private_session::{
            PrivateEvidenceSession, PrivateSessionBinding, PrivateSessionError,
            PrivateSessionState, PrivateSignal,
        },
    },
    execution::{CommandJournal, CommandJournalError, CommandState, ReadbackBatch, Reconciler},
    risk::authorize_stop_market_close_all,
    storage::{Journal, PrivateEvidenceJournal},
};

fn instrument() -> Result<Instrument, Box<dyn std::error::Error>> {
    let asset: Asset = "USDT".parse()?;
    Ok(Instrument {
        symbol: "DOGE/USDT".parse()?,
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(asset.clone()),
        generation: 1,
        price_tick: Price::new(Decimal::new(1, 5))?,
        quantity_step: Decimal::ONE,
        minimum_notional: Amount::new(asset, Decimal::new(5, 0)),
    })
}

fn owner(purpose: OrderPurpose) -> Result<OrderOwner, Box<dyn std::error::Error>> {
    Ok(OrderOwner {
        strategy_instance_id: "hedged_grid_1".to_owned(),
        run_id: "canary_1".to_owned(),
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        symbol: "DOGE/USDT".parse()?,
        purpose,
    })
}

fn close_all(
    command_id: &str,
    client_strategy_id: &str,
    position_side: PositionSide,
) -> Result<StopMarketCloseAllCommand, Box<dyn std::error::Error>> {
    let side = match position_side {
        PositionSide::Long => OrderSide::Sell,
        PositionSide::Short => OrderSide::Buy,
        PositionSide::Net => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "close-all protection requires a hedge position side",
            )
            .into());
        }
    };
    Ok(StopMarketCloseAllCommand {
        command_id: CommandId::new(command_id)?,
        client_strategy_id: CommandId::new(client_strategy_id)?,
        owner: owner(OrderPurpose::Protection)?,
        side,
        position_side,
        stop_price: Price::new(Decimal::new(9, 2))?,
        position_generation: 7,
    })
}

fn position(side: PositionSide) -> Result<Position, Box<dyn std::error::Error>> {
    Ok(Position {
        symbol: "DOGE/USDT".parse()?,
        side,
        quantity: Decimal::new(50, 0),
        entry_price: Some(Price::new(Decimal::new(1, 1))?),
        mark_price: Some(Price::new(Decimal::new(11, 2))?),
    })
}

fn entry(client_order_id: &str) -> Result<OrderCommand, Box<dyn std::error::Error>> {
    Ok(OrderCommand {
        time_in_force: Default::default(),
        command_id: CommandId::new("entry_1")?,
        client_order_id: CommandId::new(client_order_id)?,
        owner: owner(OrderPurpose::Entry)?,
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::new(50, 0),
        limit_price: Price::new(Decimal::new(1, 1))?,
        reduce_only: false,
    })
}

// Historical PAPI hedge tests required close-all protection to name exactly the authoritative
// LONG or SHORT side. A net side or a direction that adds exposure must never pass risk.
#[test]
fn hedge_close_all_is_bound_to_the_matching_long_or_short_position()
-> Result<(), Box<dyn std::error::Error>> {
    let instrument = instrument()?;
    for side in [PositionSide::Long, PositionSide::Short] {
        let command = close_all(
            if side == PositionSide::Long {
                "protect_long"
            } else {
                "protect_short"
            },
            if side == PositionSide::Long {
                "strategy_long"
            } else {
                "strategy_short"
            },
            side,
        )?;
        command.validate()?;
        authorize_stop_market_close_all(&command, &instrument, &position(side)?)?;
    }

    let mut wrong_side = close_all("protect_wrong", "strategy_wrong", PositionSide::Long)?;
    wrong_side.side = OrderSide::Buy;
    assert!(wrong_side.validate().is_err());

    let net_position = position(PositionSide::Net)?;
    let long_command = close_all("protect_net", "strategy_net", PositionSide::Long)?;
    assert!(authorize_stop_market_close_all(&long_command, &instrument, &net_position).is_err());
    Ok(())
}

// A fill can arrive only after a later signed snapshot already showed the order filled. The
// immutable fact stream must retain that new fill while deduplicating the repeated order fact.
#[test]
fn partial_order_then_late_fill_is_preserved_without_duplicate_order_facts()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let mut journal = Journal::open(directory.path().join("facts.jsonl"))?;
    let mut reconciler = Reconciler::default();
    let symbol = "DOGE/USDT".parse()?;
    let partial = parse_order(
        r#"{"symbol":"DOGEUSDT","orderId":"42","clientOrderId":"entry_client","status":"PARTIALLY_FILLED","side":"BUY","positionSide":"LONG","origQty":"50","executedQty":"17","price":"0.1","avgPrice":"0.11","reduceOnly":false}"#,
        &symbol,
    )?;
    let filled = parse_order(
        r#"{"symbol":"DOGEUSDT","orderId":"42","clientOrderId":"entry_client","status":"FILLED","side":"BUY","positionSide":"LONG","origQty":"50","executedQty":"50","price":"0.1","avgPrice":"0.11","reduceOnly":false}"#,
        &symbol,
    )?;
    let late_fill = parse_fills(
        r#"[{"symbol":"DOGEUSDT","id":"9001","orderId":"42","side":"BUY","positionSide":"LONG","qty":"33","price":"0.11","commission":"0.002","commissionAsset":"USDT","maker":false,"time":2}]"#,
        &symbol,
    )?;

    assert_eq!(partial.state, OrderState::PartiallyFilled);
    assert_eq!(partial.position_side, FieldState::Known(PositionSide::Long));
    assert_eq!(
        late_fill[0].position_side,
        FieldState::Known(PositionSide::Long)
    );
    assert_eq!(partial.filled_quantity, Decimal::new(17, 0));
    assert_eq!(
        reconciler
            .accept_readback(
                &mut journal,
                ReadbackBatch {
                    generation: 1,
                    received_at_ms: 1,
                    balances: &[],
                    positions: &[],
                    orders: std::slice::from_ref(&partial),
                    fills: &[],
                },
            )?
            .accepted,
        1
    );

    let filled_report = reconciler.accept_readback(
        &mut journal,
        ReadbackBatch {
            generation: 1,
            received_at_ms: 2,
            balances: &[],
            positions: &[],
            orders: std::slice::from_ref(&filled),
            fills: &[],
        },
    )?;
    assert_eq!(filled_report.accepted, 1);
    assert_eq!(filled_report.duplicate, 0);

    let late_report = reconciler.accept_readback(
        &mut journal,
        ReadbackBatch {
            generation: 1,
            received_at_ms: 3,
            balances: &[],
            positions: &[],
            orders: std::slice::from_ref(&filled),
            fills: &late_fill,
        },
    )?;
    assert_eq!(late_report.accepted, 1);
    assert_eq!(late_report.duplicate, 1);
    assert_eq!(journal.recover()?.entries.len(), 3);
    Ok(())
}

// Frozen PAPI readback accepts the documented numeric JSON representation as
// well as Binance's usual decimal strings. Optional trade metadata remains
// explicit when malformed; it must not erase an otherwise identifiable fill.
#[test]
fn papi_numeric_fields_and_malformed_optional_fill_metadata_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let symbol = "DOGE/USDT".parse()?;
    let order = parse_order(
        r#"{"symbol":"DOGEUSDT","orderId":42,"clientOrderId":"numeric_client","status":"NEW","side":"BUY","positionSide":"LONG","origQty":50,"executedQty":0,"price":0.1,"avgPrice":0,"reduceOnly":false}"#,
        &symbol,
    )?;
    assert_eq!(order.order_id, "42");
    assert_eq!(order.quantity, Decimal::new(50, 0));

    let fills = parse_fills(
        r#"[{"symbol":"DOGEUSDT","id":9001,"orderId":42,"side":"BUY","positionSide":"LONG","qty":1,"price":0.1,"commission":0.002,"commissionAsset":"USDT","realizedPnl":"bad","marginAsset":"USDT","maker":false,"time":"7"}]"#,
        &symbol,
    )?;
    assert_eq!(fills[0].fill_id, "9001");
    assert_eq!(fills[0].order_id, "42");
    assert!(matches!(fills[0].fee, FieldState::Known(_)));
    assert!(matches!(
        fills[0].realized_pnl,
        FieldState::Unavailable {
            reason: venue::domain::UnknownReason::ParseFailure
        }
    ));
    assert_eq!(fills[0].exchange_time_ms, Some(7));
    Ok(())
}

// Frozen PAPI recovery rejected any response whose client identity differed from the WAL target.
#[test]
fn unknown_place_requires_an_exact_client_order_readback_match()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let mut commands = CommandJournal::open(directory.path().join("commands.jsonl"))?;
    let command = entry("entry_client")?;
    commands.prepare_place(command.clone())?;
    commands.transition(&command.command_id, CommandState::Submitted)?;
    commands.transition(
        &command.command_id,
        CommandState::Unknown {
            reason: "timeout".to_owned(),
        },
    )?;

    let mismatched = parse_order(
        r#"{"symbol":"DOGEUSDT","orderId":"42","clientOrderId":"other_client","status":"NEW","side":"BUY","origQty":"50","executedQty":"0","price":"0.1","avgPrice":"0","reduceOnly":false}"#,
        &"DOGE/USDT".parse()?,
    )?;
    assert!(matches!(
        mismatched.client_order_id,
        FieldState::Known(ref client_id) if client_id == "other_client"
    ));
    assert!(!Reconciler::default().resolve_unknown_place(
        &mut commands,
        &command.command_id,
        &mismatched,
    )?);
    assert!(matches!(
        commands
            .receipt(&command.command_id)
            .map(|receipt| &receipt.state),
        Some(CommandState::Unknown { .. })
    ));
    Ok(())
}

// Conditional close-all strategies and normal UM orders have different PAPI readback/cancel
// families. Their strategy identity is idempotent and no other owner may cancel it.
#[test]
fn conditional_close_all_cancel_stays_owner_scoped_and_family_scoped()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let mut commands = CommandJournal::open(directory.path().join("commands.jsonl"))?;
    let protection = close_all("protect_1", "strategy_1", PositionSide::Short)?;
    commands.prepare_stop_market_close_all(protection.clone())?;

    let identity = commands
        .order_identity(&protection.command_id)
        .ok_or("conditional identity missing")?;
    assert_eq!(identity.family, NativeOrderFamily::UmConditional);
    assert_eq!(identity.client_id, &protection.client_strategy_id);

    let duplicate_identity = StopMarketCloseAllCommand {
        command_id: CommandId::new("protect_2")?,
        ..protection.clone()
    };
    assert!(matches!(
        commands.prepare_stop_market_close_all(duplicate_identity),
        Err(CommandJournalError::ClientId)
    ));

    let allowed = CancelCommand {
        command_id: CommandId::new("cancel_1")?,
        owner: protection.owner.clone(),
        target_client_order_id: protection.client_strategy_id.clone(),
    };
    commands.prepare_cancel(allowed)?;

    let foreign = CancelCommand {
        command_id: CommandId::new("cancel_2")?,
        owner: OrderOwner {
            strategy_instance_id: "other_strategy".to_owned(),
            ..protection.owner
        },
        target_client_order_id: protection.client_strategy_id,
    };
    assert!(matches!(
        commands.prepare_cancel(foreign),
        Err(CommandJournalError::Owner)
    ));
    Ok(())
}

// A listen-key expiration revokes readiness, advances the evidence generation once, and bars an
// old signed readback from reaching authoritative facts after reconnection.
#[test]
fn listen_key_generation_rejects_stale_readback_until_current_generation_is_durable()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let evidence = PrivateEvidenceJournal::open(directory.path().join("private.jsonl"))?;
    let mut session = PrivateEvidenceSession::new(evidence);
    let mut facts = Journal::open(directory.path().join("facts.jsonl"))?;
    let mut reconciler = Reconciler::default();

    assert_eq!(
        session.ingest(1, r#"{"e":"listenKeyExpired"}"#.to_owned())?,
        PrivateSignal::StreamExpired { generation: 2 }
    );
    assert_eq!(session.state(), PrivateSessionState::Expired);
    session.on_reconnect()?;
    assert_eq!(session.generation(), 2);

    assert!(
        reconciler
            .accept_private_readback(
                &mut facts,
                &mut session,
                ReadbackBatch {
                    generation: 1,
                    received_at_ms: 2,
                    balances: &[],
                    positions: &[],
                    orders: &[],
                    fills: &[],
                },
            )
            .is_err()
    );
    assert!(facts.recover()?.entries.is_empty());

    reconciler.accept_private_readback(
        &mut facts,
        &mut session,
        ReadbackBatch {
            generation: 2,
            received_at_ms: 3,
            balances: &[],
            positions: &[],
            orders: &[],
            fills: &[],
        },
    )?;
    assert_eq!(session.state(), PrivateSessionState::Ready);
    assert_eq!(session.on_disconnect()?, 3);
    assert_eq!(session.state(), PrivateSessionState::Reconnecting);
    Ok(())
}

// Frozen VenueCore fenced parser failures and persisted open-permission generations before
// reconnect. A restarted or replaced worker must never reuse Ready or append stale socket data.
#[test]
fn private_worker_restart_and_parser_failure_are_durable_fences()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let evidence_path = directory.path().join("private.jsonl");
    let state_path = directory.path().join("private_state.json");
    let binding = PrivateSessionBinding {
        exchange: "binance".to_owned(),
        account: "portfolio_margin_um".to_owned(),
        symbol: "SOL/USDT".parse()?,
    };

    let mut stale = PrivateEvidenceSession::open_durable(
        PrivateEvidenceJournal::open(&evidence_path)?,
        &state_path,
        binding.clone(),
    )?;
    stale.on_reconnect()?;
    stale.confirm_readback(1)?;

    let mut current = PrivateEvidenceSession::open_durable(
        PrivateEvidenceJournal::open(&evidence_path)?,
        &state_path,
        binding.clone(),
    )?;
    assert_eq!(current.generation(), 2);
    assert_eq!(current.state(), PrivateSessionState::Reconnecting);
    assert!(matches!(
        stale.ingest(
            2,
            r#"{"e":"ORDER_TRADE_UPDATE","E":2,"T":2,"o":{}}"#.to_owned()
        ),
        Err(PrivateSessionError::Durable)
    ));
    assert!(current.journal().recover()?.is_empty());

    current.on_reconnect()?;
    assert!(matches!(
        current.ingest(3, r#"{"missing_event_name":true}"#.to_owned()),
        Err(PrivateSessionError::Payload)
    ));
    assert_eq!(current.generation(), 3);
    assert_eq!(current.state(), PrivateSessionState::Reconnecting);
    drop(current);

    let recovered = PrivateEvidenceSession::open_durable(
        PrivateEvidenceJournal::open(&evidence_path)?,
        &state_path,
        binding,
    )?;
    assert_eq!(recovered.generation(), 4);
    assert_eq!(recovered.state(), PrivateSessionState::Reconnecting);
    assert_eq!(recovered.journal().recover()?.len(), 1);
    Ok(())
}
