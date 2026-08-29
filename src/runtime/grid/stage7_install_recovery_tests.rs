use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use rust_decimal::Decimal;

use super::*;
use crate::{
    domain::{
        AccountBalance, Amount, Asset, Instrument, MarketKind, MarketReduceCommand, Order,
        OrderCommand, OrderOwner, OrderPurpose, OrderState,
    },
    exchange::grid::{GridOrderFamilyReadback, HedgedGridMutationClient},
    strategy::hedged_grid::{
        GridEpoch, GridOrderIntent, GridOrderKey, GridOrderRole, GridPosition,
        InventoryRecoveryState,
    },
};

#[derive(Clone)]
struct RecordingClient {
    reductions: Arc<Mutex<Vec<bool>>>,
    fail: bool,
}

impl HedgedGridMutationClient for RecordingClient {
    fn place_limit_post_only(&self, command: &OrderCommand) -> Result<String, GridVenueError> {
        self.reductions
            .lock()
            .map_err(|_| GridVenueError::PrivateReadbackRequired)?
            .push(command.reduce_only);
        if self.fail {
            Err(GridVenueError::PrivateReadbackRequired)
        } else {
            Ok(command.client_order_id.as_str().to_owned())
        }
    }

    fn place_market(&self, _command: &MarketOrderCommand) -> Result<String, GridVenueError> {
        Err(GridVenueError::PrivateReadbackRequired)
    }

    fn place_market_reduce(
        &self,
        _command: &MarketReduceCommand,
    ) -> Result<String, GridVenueError> {
        self.reductions
            .lock()
            .map_err(|_| GridVenueError::PrivateReadbackRequired)?
            .push(true);
        Err(GridVenueError::PrivateReadbackRequired)
    }

    fn cancel_by_client_id(&self, _command: &CancelCommand) -> Result<String, GridVenueError> {
        Err(GridVenueError::PrivateReadbackRequired)
    }
}

struct RecordingVenue {
    instrument: Instrument,
    reductions: Arc<Mutex<Vec<bool>>>,
    fail: bool,
    opening_book: Option<(Price, Price)>,
}

impl HedgedGridVenue for RecordingVenue {
    fn exchange(&self) -> &'static str {
        "gate"
    }

    fn instrument(&self) -> &Instrument {
        &self.instrument
    }

    fn minimum_quantity(&self) -> Decimal {
        Decimal::ONE
    }

    fn best_bid_ask(&self, _now_ms: u64) -> Result<(Price, Price), GridVenueError> {
        if let Some(book) = self.opening_book {
            return Ok(book);
        }
        Ok((
            Price::new(Decimal::new(10, 2)).map_err(|_| GridVenueError::PublicPayload)?,
            Price::new(Decimal::new(11, 2)).map_err(|_| GridVenueError::PublicPayload)?,
        ))
    }

    fn readback(&mut self) -> Result<GridVenueReadback, GridVenueError> {
        empty_readback()
    }

    fn connect_private_stream(&mut self) -> Result<(), GridVenueError> {
        Ok(())
    }

    fn next_private_event(&mut self) -> Result<Option<GridPrivateEvent>, GridVenueError> {
        Ok(None)
    }

    fn reset_private_stream(&mut self) {}

    fn mutation_client(&self) -> Arc<dyn HedgedGridMutationClient> {
        Arc::new(RecordingClient {
            reductions: self.reductions.clone(),
            fail: self.fail,
        })
    }

    fn order_by_client_id(&mut self, _client_order_id: &str) -> Result<Order, GridVenueError> {
        Err(GridVenueError::Bitget(
            crate::exchange::bitget::BitgetError::Payload,
        ))
    }

    fn verify_post_only_order(&mut self, _client_order_id: &str) -> Result<(), GridVenueError> {
        Ok(())
    }
}

#[test]
fn interrupted_wal_settlement_waits_for_blocked_deadline_but_stop_is_immediate()
-> Result<(), Box<dyn std::error::Error>> {
    let binding = binding()?;
    let mut checkpoint = checkpoint(&binding)?;
    let _ = checkpoint.state.install_epoch(GridEpoch {
        epoch: 1,
        anchor_price: Price::new(Decimal::new(105, 3))?,
        step: Price::new(Decimal::new(5, 3))?,
        grid_quantity: Decimal::new(50, 0),
        passive_book_fallback: None,
    })?;
    checkpoint.state.block_for_order_reconciliation()?;
    checkpoint.state.defer_blocked_reconciliation_until(100)?;

    assert!(!stage7_resident::interrupted_wal_settlement_due(
        HedgedGridControlTarget::Running,
        &checkpoint.state,
        99,
    ));
    assert!(stage7_resident::interrupted_wal_settlement_due(
        HedgedGridControlTarget::Running,
        &checkpoint.state,
        100,
    ));
    assert!(stage7_resident::interrupted_wal_settlement_due(
        HedgedGridControlTarget::Stop,
        &checkpoint.state,
        0,
    ));
    Ok(())
}

#[test]
fn expired_blocked_fence_resolves_an_absent_submitted_sibling_from_signed_facts()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = binding()?;
    let mut checkpoint = checkpoint(&binding)?;
    let _ = checkpoint.state.install_epoch(GridEpoch {
        epoch: 1,
        anchor_price: Price::new(Decimal::new(105, 3))?,
        step: Price::new(Decimal::new(5, 3))?,
        grid_quantity: Decimal::new(50, 0),
        passive_book_fallback: None,
    })?;
    checkpoint.state.block_for_order_reconciliation()?;
    checkpoint.state.defer_blocked_reconciliation_until(100)?;
    let GridMutation::Place(command) = place_command(&binding, &instrument()?, &intent(false)?)?
    else {
        return Err("expected place command".into());
    };
    let mut commands = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
    commands.prepare_place(command.clone())?;
    commands.transition(&command.command_id, CommandState::Submitted)?;
    let readback = empty_readback()?;

    assert!(!stage7_resident::settle_due_interrupted_wal(
        HedgedGridControlTarget::Running,
        &checkpoint.state,
        &mut commands,
        &binding,
        &readback,
        99,
    )?);
    assert!(commands.has_unresolved());
    assert!(stage7_resident::settle_due_interrupted_wal(
        HedgedGridControlTarget::Running,
        &checkpoint.state,
        &mut commands,
        &binding,
        &readback,
        100,
    )?);
    assert!(!commands.has_unresolved());
    assert!(matches!(
        commands
            .receipt(&command.command_id)
            .map(|receipt| &receipt.state),
        Some(CommandState::Rejected { reason })
            if reason == "absent_from_complete_signed_orders_and_fill_history"
    ));
    Ok(())
}

#[test]
fn fill_entrance_fsyncs_reanchor_pending_before_any_rebuild_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = binding()?;
    let mut checkpoint = checkpoint(&binding)?;
    let _ = checkpoint.state.install_epoch(GridEpoch {
        epoch: 1,
        anchor_price: Price::new(Decimal::new(105, 3))?,
        step: Price::new(Decimal::new(5, 3))?,
        grid_quantity: Decimal::new(50, 0),
        passive_book_fallback: None,
    })?;
    checkpoint.state.inventory_recovery = InventoryRecoveryState::AwaitingNextOwnedFill {
        armed_generation: 1,
    };
    let mut current_inventory = inventory()?;
    current_inventory.private_generation = 2;
    current_inventory.private_observed_at_ms = 2;
    let _ = checkpoint.state.observe_inventory(current_inventory)?;
    checkpoint.private_generation = 2;

    let intent = checkpoint
        .state
        .owned_orders
        .values()
        .find(|intent| {
            intent.key.position == GridPosition::Long
                && intent.key.role == GridOrderRole::Open
                && intent.key.level == 1
        })
        .cloned()
        .ok_or("missing owned opening")?;
    let fill_price = Price::new(Decimal::new(102_345, 3))?;
    let mut readback = empty_readback()?;
    readback.fills.push(GridVenueFill {
        fill: crate::domain::Fill {
            execution_sequence: FieldState::Known(1),
            fill_id: "reanchor-pending-crash-boundary".to_owned(),
            order_id: "venue-reanchor-pending".to_owned(),
            symbol: binding.symbol.clone(),
            side: intent.side,
            position_side: FieldState::Known(PositionSide::Long),
            quantity: intent.quantity,
            price: fill_price,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(true),
            exchange_time_ms: Some(2),
        },
        client_order_id: FieldState::Known(client_order_id(&intent.key)?.as_str().to_owned()),
    });
    let reductions = Arc::new(Mutex::new(Vec::new()));
    let mut venue = RecordingVenue {
        instrument: instrument()?,
        reductions: reductions.clone(),
        fail: false,
        opening_book: None,
    };
    let authority = authority(temporary.path(), &binding)?;
    let writer = authority.register_initial(1, 2)?;
    let mut commands = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
    let store = ProjectionStore::new(temporary.path().join("checkpoint.json"));

    assert_eq!(
        process_complete_owned_fills(
            &mut checkpoint,
            &mut commands,
            &mut venue,
            &authority,
            &writer,
            &binding,
            &readback,
            &store,
        )?,
        FillDriveOutcome::private_readback()
    );
    assert_eq!(
        checkpoint.state.inventory_recovery,
        InventoryRecoveryState::ReanchorPending {
            fill_id: "reanchor-pending-crash-boundary".to_owned(),
            fill_price,
        }
    );
    let restored = store
        .load::<Stage7GridCheckpoint>()?
        .ok_or("missing durable checkpoint")?;
    assert_eq!(restored, checkpoint);
    assert!(reductions.lock().map_err(|_| "lock poisoned")?.is_empty());
    assert!(commands.commands().next().is_none());
    Ok(())
}

#[test]
fn failed_closing_install_retires_undispatched_opening_and_blocks_for_reconciliation()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = binding()?;
    let mut checkpoint = checkpoint(&binding)?;
    let inventory = inventory()?;
    let reductions = Arc::new(Mutex::new(Vec::new()));
    let mut venue = RecordingVenue {
        instrument: instrument()?,
        reductions,
        fail: true,
        opening_book: None,
    };
    let mut commands = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
    let store = ProjectionStore::new(temporary.path().join("checkpoint.json"));
    let authority = authority(temporary.path(), &binding)?;
    let writer = authority.register_initial(1, 2)?;

    install_epoch(
        &mut checkpoint,
        &mut commands,
        &mut venue,
        &authority,
        &writer,
        &binding,
        &inventory,
        Price::new(Decimal::new(10, 2))?,
        Price::new(Decimal::new(11, 2))?,
        &store,
    )?;

    let mut rejected_openings = 0;
    for command in commands.commands() {
        let receipt = commands
            .receipt(command.command_id())
            .ok_or("missing receipt")?;
        if matches!(
            (&receipt.command, &receipt.state),
            (
                ExecutionCommand::PlaceLimit(command),
                CommandState::Rejected { reason }
            ) if !command.reduce_only && reason == "epoch install aborted before opening dispatch"
        ) {
            rejected_openings += 1;
        }
    }
    assert_eq!(rejected_openings, 6);
    assert_eq!(checkpoint.state.phase, GridPhase::BlockedUnknown);
    assert!(
        checkpoint
            .state
            .blocked_reconciliation_not_before_ms()
            .is_some()
    );
    Ok(())
}

#[test]
fn suppressed_replenishment_installs_inventory_bounded_grid_without_market_top_up()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = binding()?;
    let mut checkpoint = checkpoint(&binding)?;
    checkpoint.state.request_restart_without_replenishment()?;
    let low_inventory = GridInventory {
        private_generation: 2,
        private_observed_at_ms: 2,
        mark_price: Price::new(Decimal::new(105, 3))?,
        long_quantity: Decimal::ONE,
        short_quantity: Decimal::new(1_000, 0),
    };
    assert_eq!(
        checkpoint.state.observe_inventory(low_inventory.clone())?,
        GridDecision::Noop
    );
    let reductions = Arc::new(Mutex::new(Vec::new()));
    let mut venue = RecordingVenue {
        instrument: instrument()?,
        reductions: reductions.clone(),
        fail: false,
        opening_book: None,
    };
    let mut commands = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
    let store = ProjectionStore::new(temporary.path().join("checkpoint.json"));
    let authority = authority(temporary.path(), &binding)?;
    let writer = authority.register_initial(1, 2)?;

    install_epoch(
        &mut checkpoint,
        &mut commands,
        &mut venue,
        &authority,
        &writer,
        &binding,
        &low_inventory,
        Price::new(Decimal::new(10, 2))?,
        Price::new(Decimal::new(11, 2))?,
        &store,
    )?;

    let opening = checkpoint
        .state
        .owned_orders
        .values()
        .filter(|intent| intent.key.role == GridOrderRole::Open)
        .count();
    let long_closing = checkpoint
        .state
        .owned_orders
        .values()
        .filter(|intent| {
            intent.key.position == GridPosition::Long && intent.key.role == GridOrderRole::Close
        })
        .count();
    let short_closing = checkpoint
        .state
        .owned_orders
        .values()
        .filter(|intent| {
            intent.key.position == GridPosition::Short && intent.key.role == GridOrderRole::Close
        })
        .count();
    assert_eq!(checkpoint.state.phase, GridPhase::Running);
    assert_eq!(opening, 6);
    assert_eq!(long_closing, 0);
    assert_eq!(short_closing, 3);
    assert_eq!(
        reductions.lock().map_err(|_| "lock poisoned")?.as_slice(),
        [true, true, true, false, false, false, false, false, false]
    );
    assert_eq!(commands.commands().count(), 9);
    assert!(
        commands
            .commands()
            .all(|command| matches!(command, ExecutionCommand::PlaceLimit(_)))
    );
    Ok(())
}

#[test]
fn opening_wave_rechecks_the_live_book_after_closing_dispatch()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = binding()?;
    let mut checkpoint = checkpoint(&binding)?;
    let inventory = inventory()?;
    let reductions = Arc::new(Mutex::new(Vec::new()));
    let mut venue = RecordingVenue {
        instrument: instrument()?,
        reductions: reductions.clone(),
        fail: false,
        opening_book: Some((
            Price::new(Decimal::new(10_470, 5))?,
            Price::new(Decimal::new(10_475, 5))?,
        )),
    };
    let mut commands = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
    let store = ProjectionStore::new(temporary.path().join("checkpoint.json"));
    let authority = authority(temporary.path(), &binding)?;
    let writer = authority.register_initial(1, 2)?;

    install_epoch(
        &mut checkpoint,
        &mut commands,
        &mut venue,
        &authority,
        &writer,
        &binding,
        &inventory,
        Price::new(Decimal::new(10, 2))?,
        Price::new(Decimal::new(11, 2))?,
        &store,
    )?;

    assert_eq!(
        reductions.lock().map_err(|_| "lock poisoned")?.as_slice(),
        [true, true, true, true, true, true]
    );
    assert_eq!(checkpoint.state.phase, GridPhase::BlockedUnknown);
    let rejected_openings = commands
        .commands()
        .filter(|command| {
            commands
                .receipt(command.command_id())
                .is_some_and(|receipt| {
                    matches!(
                        (&receipt.command, &receipt.state),
                        (
                            ExecutionCommand::PlaceLimit(command),
                            CommandState::Rejected { reason }
                        ) if !command.reduce_only
                            && reason == "epoch install aborted before opening dispatch"
                    )
                })
        })
        .count();
    assert_eq!(rejected_openings, 6);
    Ok(())
}

#[test]
fn opening_wave_refreshes_public_frames_after_slow_closing_dispatch()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = binding()?;
    let mut checkpoint = checkpoint(&binding)?;
    let inventory = inventory()?;
    let reductions = Arc::new(Mutex::new(Vec::new()));
    let mut venue = RecordingVenue {
        instrument: instrument()?,
        reductions: reductions.clone(),
        fail: false,
        opening_book: Some((
            Price::new(Decimal::new(10_470, 5))?,
            Price::new(Decimal::new(10_475, 5))?,
        )),
    };
    let mut commands = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
    let store = ProjectionStore::new(temporary.path().join("checkpoint.json"));
    let authority = authority(temporary.path(), &binding)?;
    let writer = authority.register_initial(1, 2)?;
    let mut refresh_seen = false;

    install_epoch_with_public_refresh(
        &mut checkpoint,
        &mut commands,
        &mut venue,
        &authority,
        &writer,
        &binding,
        &inventory,
        Price::new(Decimal::new(10, 2))?,
        Price::new(Decimal::new(11, 2))?,
        &store,
        |venue| {
            if venue
                .reductions
                .lock()
                .map_err(|_| Stage7GridError::Command)?
                .len()
                != 6
            {
                return Err(Stage7GridError::Command);
            }
            refresh_seen = true;
            venue.opening_book = None;
            Ok(true)
        },
    )?;

    assert!(refresh_seen);
    assert_eq!(checkpoint.state.phase, GridPhase::Running);
    assert!(!commands.has_unresolved());
    assert_eq!(
        reductions.lock().map_err(|_| "lock poisoned")?.as_slice(),
        [
            true, true, true, true, true, true, false, false, false, false, false, false
        ]
    );
    Ok(())
}

#[test]
fn rebuilding_epoch_persists_bbo_midpoint_fallback_before_wal_when_fill_anchor_crosses()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = binding()?;
    let mut checkpoint = rebuilding_checkpoint(&binding)?;
    checkpoint.state.inventory_recovery = InventoryRecoveryState::Rebuilding {
        fill_id: "passive-preflight".to_owned(),
        fill_price: Price::new(Decimal::new(123_456, 6))?,
    };
    let inventory = inventory()?;
    let reductions = Arc::new(Mutex::new(Vec::new()));
    let mut venue = RecordingVenue {
        instrument: instrument()?,
        reductions: reductions.clone(),
        fail: false,
        opening_book: None,
    };
    let mut commands = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
    let store = ProjectionStore::new(temporary.path().join("checkpoint.json"));
    let authority = authority(temporary.path(), &binding)?;
    let writer = authority.register_initial(1, 2)?;

    install_epoch(
        &mut checkpoint,
        &mut commands,
        &mut venue,
        &authority,
        &writer,
        &binding,
        &inventory,
        Price::new(Decimal::new(10, 2))?,
        Price::new(Decimal::new(11, 2))?,
        &store,
    )?;

    assert_eq!(checkpoint.state.phase, GridPhase::Running);
    let epoch = checkpoint.state.epoch.as_ref().ok_or("missing epoch")?;
    assert_eq!(epoch.anchor_price, Price::new(Decimal::new(105, 3))?);
    let fallback = epoch
        .passive_book_fallback
        .as_ref()
        .ok_or("missing passive-book fallback")?;
    assert_eq!(fallback.fill_id, "passive-preflight");
    assert_eq!(fallback.fill_price, Price::new(Decimal::new(123_456, 6))?);
    assert_eq!(fallback.bid, Price::new(Decimal::new(10, 2))?);
    assert_eq!(fallback.ask, Price::new(Decimal::new(11, 2))?);
    assert_eq!(fallback.anchor_price, epoch.anchor_price);
    assert!(fallback.selected_at_ms > 0);
    assert!(!commands.has_unresolved());
    assert_eq!(commands.commands().count(), 12);
    assert_eq!(
        reductions.lock().map_err(|_| "lock poisoned")?.as_slice(),
        [
            true, true, true, true, true, true, false, false, false, false, false, false
        ]
    );
    let durable = store
        .load::<Stage7GridCheckpoint>()?
        .ok_or("missing durable fallback checkpoint")?;
    assert_eq!(durable, checkpoint);
    Ok(())
}

#[test]
fn rebuilding_epoch_keeps_exact_fill_anchor_when_complete_grid_is_passive()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = binding()?;
    let mut checkpoint = rebuilding_checkpoint(&binding)?;
    let fill_price = Price::new(Decimal::new(105, 3))?;
    checkpoint.state.inventory_recovery = InventoryRecoveryState::Rebuilding {
        fill_id: "passive-fill-anchor".to_owned(),
        fill_price,
    };
    let inventory = inventory()?;
    let reductions = Arc::new(Mutex::new(Vec::new()));
    let mut venue = RecordingVenue {
        instrument: instrument()?,
        reductions,
        fail: false,
        opening_book: None,
    };
    let mut commands = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
    let store = ProjectionStore::new(temporary.path().join("checkpoint.json"));
    let authority = authority(temporary.path(), &binding)?;
    let writer = authority.register_initial(1, 2)?;

    install_epoch(
        &mut checkpoint,
        &mut commands,
        &mut venue,
        &authority,
        &writer,
        &binding,
        &inventory,
        Price::new(Decimal::new(10, 2))?,
        Price::new(Decimal::new(11, 2))?,
        &store,
    )?;

    let epoch = checkpoint.state.epoch.as_ref().ok_or("missing epoch")?;
    assert_eq!(epoch.anchor_price, fill_price);
    assert!(epoch.passive_book_fallback.is_none());
    assert!(!commands.has_unresolved());
    Ok(())
}

#[test]
fn restart_resumes_prepared_closing_before_prepared_opening()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = binding()?;
    let reductions = Arc::new(Mutex::new(Vec::new()));
    let mut venue = RecordingVenue {
        instrument: instrument()?,
        reductions: reductions.clone(),
        fail: false,
        opening_book: None,
    };
    let mut commands = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
    for intent in [intent(true)?, intent(false)?] {
        Stage7Mutation::from_grid(place_command(&binding, venue.instrument(), &intent)?)
            .prepare(&mut commands)?;
    }
    let authority = authority(temporary.path(), &binding)?;
    let writer = authority.register_initial(1, 2)?;

    recover_unresolved(
        &mut commands,
        &mut venue,
        &authority,
        &writer,
        &binding,
        &empty_readback()?,
        true,
    )?;

    let calls = reductions
        .lock()
        .map_err(|_| "recording lock poisoned")?
        .clone();
    assert_eq!(calls, vec![true, false]);
    assert!(!commands.has_unresolved());
    Ok(())
}

#[test]
fn crash_before_cancel_restores_exact_rebuilding_trigger() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = tempfile::tempdir()?;
    let binding = binding()?;
    let checkpoint = rebuilding_checkpoint(&binding)?;
    let store = ProjectionStore::new(temporary.path().join("checkpoint.json"));
    store.save(&checkpoint)?;

    let restored = store
        .load::<Stage7GridCheckpoint>()?
        .ok_or("missing checkpoint")?;
    assert_eq!(restored.state.phase, GridPhase::ResettingGrid);
    assert!(matches!(
        restored.state.inventory_recovery,
        InventoryRecoveryState::Rebuilding { ref fill_id, fill_price }
            if fill_id == "reanchor-fill-7" && fill_price == Price::new(Decimal::new(123_456, 3))?
    ));
    assert!(!restored.state.owned_orders.is_empty());
    Ok(())
}

#[test]
fn crash_during_cancel_settles_same_wal_identity_without_placing()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = binding()?;
    let instrument = instrument()?;
    let reductions = Arc::new(Mutex::new(Vec::new()));
    let mut venue = RecordingVenue {
        instrument: instrument.clone(),
        reductions: reductions.clone(),
        fail: false,
        opening_book: None,
    };
    let mut commands = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
    let old = intent(true)?;
    let place_id = accept_place(&mut commands, &binding, &instrument, &old)?;
    let cancel = Stage7Mutation::from_grid(next_cancel_command(&commands, &binding, &old.key)?);
    let cancel_id = cancel.command_id().clone();
    cancel.prepare(&mut commands)?;
    commands.transition(&cancel_id, CommandState::Submitted)?;
    let authority = authority(temporary.path(), &binding)?;
    let writer = authority.register_initial(1, 2)?;

    recover_unresolved(
        &mut commands,
        &mut venue,
        &authority,
        &writer,
        &binding,
        &empty_readback()?,
        true,
    )?;

    assert!(matches!(
        commands.receipt(&cancel_id).map(|receipt| &receipt.state),
        Some(CommandState::Accepted { venue_order_id })
            if venue_order_id == "absent_in_signed_open_orders"
    ));
    assert!(matches!(
        commands.receipt(&place_id).map(|receipt| &receipt.state),
        Some(CommandState::Accepted { .. })
    ));
    assert!(
        reductions
            .lock()
            .map_err(|_| "recording lock poisoned")?
            .is_empty()
    );
    Ok(())
}

#[test]
fn crash_after_zero_old_orders_reuses_persisted_fill_anchor()
-> Result<(), Box<dyn std::error::Error>> {
    let binding = binding()?;
    let mut checkpoint = rebuilding_checkpoint(&binding)?;
    checkpoint.state.reset_orders_settled()?;
    assert!(checkpoint.state.owned_orders.is_empty());
    let venue = RecordingVenue {
        instrument: instrument()?,
        reductions: Arc::new(Mutex::new(Vec::new())),
        fail: false,
        opening_book: None,
    };
    let epoch = stage7_epoch(
        &checkpoint.state,
        &venue,
        Price::new(Decimal::new(90, 0))?,
        Price::new(Decimal::new(110, 0))?,
        1,
    )?;
    assert_eq!(epoch.anchor_price, Price::new(Decimal::new(123_456, 3))?);
    Ok(())
}

#[test]
fn crash_during_closing_wave_fences_prepared_opening_until_close_is_terminal()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = binding()?;
    let reductions = Arc::new(Mutex::new(Vec::new()));
    let mut venue = RecordingVenue {
        instrument: instrument()?,
        reductions: reductions.clone(),
        fail: false,
        opening_book: None,
    };
    let mut commands = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
    let closing =
        Stage7Mutation::from_grid(place_command(&binding, venue.instrument(), &intent(true)?)?);
    let opening = Stage7Mutation::from_grid(place_command(
        &binding,
        venue.instrument(),
        &intent(false)?,
    )?);
    let closing_id = closing.command_id().clone();
    let opening_id = opening.command_id().clone();
    closing.prepare(&mut commands)?;
    opening.prepare(&mut commands)?;
    commands.transition(&closing_id, CommandState::Submitted)?;
    let authority = authority(temporary.path(), &binding)?;
    let writer = authority.register_initial(1, 2)?;

    recover_unresolved(
        &mut commands,
        &mut venue,
        &authority,
        &writer,
        &binding,
        &empty_readback()?,
        true,
    )?;

    assert!(matches!(
        commands.receipt(&closing_id).map(|receipt| &receipt.state),
        Some(CommandState::Submitted)
    ));
    assert!(matches!(
        commands.receipt(&opening_id).map(|receipt| &receipt.state),
        Some(CommandState::Prepared)
    ));
    assert!(
        reductions
            .lock()
            .map_err(|_| "recording lock poisoned")?
            .is_empty()
    );
    Ok(())
}

#[test]
fn crash_during_opening_wave_resumes_only_prepared_opening()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = binding()?;
    let reductions = Arc::new(Mutex::new(Vec::new()));
    let mut venue = RecordingVenue {
        instrument: instrument()?,
        reductions: reductions.clone(),
        fail: false,
        opening_book: None,
    };
    let mut commands = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
    let closing =
        Stage7Mutation::from_grid(place_command(&binding, venue.instrument(), &intent(true)?)?);
    let opening = Stage7Mutation::from_grid(place_command(
        &binding,
        venue.instrument(),
        &intent(false)?,
    )?);
    let closing_id = closing.command_id().clone();
    let opening_id = opening.command_id().clone();
    closing.prepare(&mut commands)?;
    commands.transition(&closing_id, CommandState::Submitted)?;
    commands.transition(
        &closing_id,
        CommandState::Accepted {
            venue_order_id: "closing-live".to_owned(),
        },
    )?;
    opening.prepare(&mut commands)?;
    let authority = authority(temporary.path(), &binding)?;
    let writer = authority.register_initial(1, 2)?;

    recover_unresolved(
        &mut commands,
        &mut venue,
        &authority,
        &writer,
        &binding,
        &empty_readback()?,
        true,
    )?;

    assert!(matches!(
        commands.receipt(&opening_id).map(|receipt| &receipt.state),
        Some(CommandState::Accepted { .. })
    ));
    assert_eq!(
        reductions
            .lock()
            .map_err(|_| "recording lock poisoned")?
            .as_slice(),
        &[false]
    );
    Ok(())
}

#[test]
fn crash_before_final_verification_keeps_rebuilding_until_signed_ladder_is_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = binding()?;
    let instrument = instrument()?;
    let mut checkpoint = checkpoint(&binding)?;
    let _ = checkpoint.state.install_epoch(GridEpoch {
        epoch: 1,
        anchor_price: Price::new(Decimal::new(105, 3))?,
        step: Price::new(Decimal::new(5, 3))?,
        grid_quantity: Decimal::new(50, 0),
        passive_book_fallback: None,
    })?;
    checkpoint.state.inventory_recovery = InventoryRecoveryState::Rebuilding {
        fill_id: "reanchor-fill-7".to_owned(),
        fill_price: Price::new(Decimal::new(123_456, 3))?,
    };
    let mut commands = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
    let mut orders = Vec::new();
    for intent in checkpoint.state.owned_orders.values() {
        let GridMutation::Place(command) = place_command(&binding, &instrument, intent)? else {
            return Err("expected place command".into());
        };
        commands.prepare_place(command.clone())?;
        commands.transition(&command.command_id, CommandState::Submitted)?;
        commands.transition(
            &command.command_id,
            CommandState::Accepted {
                venue_order_id: command.client_order_id.as_str().to_owned(),
            },
        )?;
        orders.push(signed_order(&binding, intent, &command));
    }
    let mut incomplete = empty_readback()?;
    incomplete.orders = orders[..orders.len() - 1].to_vec();
    assert!(!signed_desired_ladder_is_complete(
        &checkpoint.state,
        &commands,
        &binding,
        &incomplete,
    )?);
    assert!(matches!(
        checkpoint.state.inventory_recovery,
        InventoryRecoveryState::Rebuilding { .. }
    ));

    let mut complete = empty_readback()?;
    complete.orders = orders;
    assert!(signed_desired_ladder_is_complete(
        &checkpoint.state,
        &commands,
        &binding,
        &complete,
    )?);
    checkpoint.state.complete_reanchor_rebuild()?;
    assert_eq!(
        checkpoint.state.inventory_recovery,
        InventoryRecoveryState::Inactive
    );
    Ok(())
}

#[test]
fn crash_after_risk_submit_keeps_same_episode_pending_without_second_reduction()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = binding()?;
    let reductions = Arc::new(Mutex::new(Vec::new()));
    let mut venue = RecordingVenue {
        instrument: instrument()?,
        reductions: reductions.clone(),
        fail: false,
        opening_book: None,
    };
    let command = MarketReduceCommand {
        command_id: CommandId::new("cmd-etp-l-crash-7")?,
        client_order_id: CommandId::new("ord-etp-l-crash-7")?,
        owner: OrderOwner {
            strategy_instance_id: binding.strategy_instance_id.clone(),
            run_id: binding.run_id.clone(),
            exchange: binding.exchange.clone(),
            account: binding.account.clone(),
            symbol: binding.symbol.clone(),
            purpose: OrderPurpose::ExposureTakeProfit,
        },
        position_side: PositionSide::Long,
        side: OrderSide::Sell,
        quantity: Decimal::new(10, 0),
        risk_episode_id: CommandId::new("etp-l-crash-7")?,
        position_generation: 7,
    };
    let mut commands = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
    commands.prepare_market_reduce(command.clone())?;
    commands.transition(&command.command_id, CommandState::Submitted)?;
    let authority = authority(temporary.path(), &binding)?;
    let writer = authority.register_initial(1, 2)?;

    recover_unresolved(
        &mut commands,
        &mut venue,
        &authority,
        &writer,
        &binding,
        &empty_readback()?,
        true,
    )?;

    assert!(matches!(
        commands
            .receipt(&command.command_id)
            .map(|receipt| &receipt.state),
        Some(CommandState::Submitted)
    ));
    assert!(
        reductions
            .lock()
            .map_err(|_| "recording lock poisoned")?
            .is_empty()
    );
    Ok(())
}

#[test]
fn signed_fill_without_client_id_uses_only_accepted_wal_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = binding()?;
    let instrument = instrument()?;
    let mut commands = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
    let GridMutation::Place(command) = place_command(&binding, &instrument, &intent(false)?)?
    else {
        return Err("expected place command".into());
    };
    let command_id = command.command_id.clone();
    commands.prepare_place(command.clone())?;
    commands.transition(&command_id, CommandState::Submitted)?;
    commands.transition(
        &command_id,
        CommandState::Accepted {
            venue_order_id: "native-order-7".to_owned(),
        },
    )?;
    let record = GridVenueFill {
        fill: crate::domain::Fill {
            execution_sequence: FieldState::Known(7),
            fill_id: "fill-7".to_owned(),
            order_id: "native-order-7".to_owned(),
            symbol: binding.symbol.clone(),
            side: command.side,
            position_side: FieldState::Known(command.position_side),
            quantity: command.quantity,
            price: command.limit_price,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(true),
            exchange_time_ms: Some(7),
        },
        client_order_id: FieldState::Missing,
    };

    let resolved = resolve_grid_fill_client_ids(&commands, &[record]);
    assert!(matches!(
        &resolved[0].client_order_id,
        FieldState::Known(value) if value == command.client_order_id.as_str()
    ));
    Ok(())
}

fn binding() -> Result<HedgedGridBinding, Box<dyn std::error::Error>> {
    Ok(HedgedGridBinding {
        strategy_instance_id: "hedged_grid_doge_usdt".to_owned(),
        run_id: "primary".to_owned(),
        exchange: "gate".to_owned(),
        account: "usdt_futures".to_owned(),
        symbol: "DOGE/USDT".parse()?,
        config_version: "test".to_owned(),
        owner_scope: "hedged_grid_doge_usdt_primary".to_owned(),
    })
}

fn instrument() -> Result<Instrument, Box<dyn std::error::Error>> {
    Ok(Instrument {
        symbol: "DOGE/USDT".parse()?,
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(Asset::new("USDT")?),
        generation: 1,
        price_tick: Price::new(Decimal::new(1, 5))?,
        quantity_step: Decimal::ONE,
        minimum_notional: Amount::new(Asset::new("USDT")?, Decimal::ZERO),
    })
}

fn inventory() -> Result<GridInventory, Box<dyn std::error::Error>> {
    Ok(GridInventory {
        private_generation: 1,
        private_observed_at_ms: 1,
        mark_price: Price::new(Decimal::new(105, 3))?,
        long_quantity: Decimal::new(1_000, 0),
        short_quantity: Decimal::new(1_000, 0),
    })
}

fn checkpoint(
    binding: &HedgedGridBinding,
) -> Result<Stage7GridCheckpoint, Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new_with_params(
        binding.clone(),
        HedgedGridParams::fixed_release(Asset::new("USDT")?, 3)?,
    )?;
    let _ = state.observe_inventory(inventory()?)?;
    Ok(Stage7GridCheckpoint {
        schema_version: 1,
        binding: binding.clone(),
        state,
        private_generation: 1,
        exposure_guard: None,
        pending_exposure_reduction: None,
        fill_history_start_ms: 1,
        order_health_fenced: false,
        last_order_health_checked_at_ms: 0,
    })
}

fn rebuilding_checkpoint(
    binding: &HedgedGridBinding,
) -> Result<Stage7GridCheckpoint, Box<dyn std::error::Error>> {
    let mut checkpoint = checkpoint(binding)?;
    let _ = checkpoint.state.install_epoch(GridEpoch {
        epoch: 1,
        anchor_price: Price::new(Decimal::new(105, 3))?,
        step: Price::new(Decimal::new(5, 3))?,
        grid_quantity: Decimal::new(50, 0),
        passive_book_fallback: None,
    })?;
    checkpoint.state.inventory_recovery = InventoryRecoveryState::ReanchorPending {
        fill_id: "reanchor-fill-7".to_owned(),
        fill_price: Price::new(Decimal::new(123_456, 3))?,
    };
    checkpoint.state.begin_reanchor_rebuild()?;
    Ok(checkpoint)
}

fn accept_place(
    commands: &mut CommandJournal,
    binding: &HedgedGridBinding,
    instrument: &Instrument,
    intent: &GridOrderIntent,
) -> Result<CommandId, Box<dyn std::error::Error>> {
    let GridMutation::Place(command) = place_command(binding, instrument, intent)? else {
        return Err("expected place command".into());
    };
    let command_id = command.command_id.clone();
    commands.prepare_place(command.clone())?;
    commands.transition(&command_id, CommandState::Submitted)?;
    commands.transition(
        &command_id,
        CommandState::Accepted {
            venue_order_id: command.client_order_id.as_str().to_owned(),
        },
    )?;
    Ok(command_id)
}

fn signed_order(
    binding: &HedgedGridBinding,
    intent: &GridOrderIntent,
    command: &OrderCommand,
) -> Order {
    Order {
        order_id: command.client_order_id.as_str().to_owned(),
        client_order_id: FieldState::Known(command.client_order_id.as_str().to_owned()),
        symbol: binding.symbol.clone(),
        side: command.side,
        position_side: FieldState::Known(command.position_side),
        purpose: FieldState::Known(if intent.reduce_only {
            OrderPurpose::Reduce
        } else {
            OrderPurpose::Entry
        }),
        state: OrderState::New,
        quantity: command.quantity,
        filled_quantity: Decimal::ZERO,
        limit_price: Some(command.limit_price),
        average_price: FieldState::Missing,
        reduce_only: command.reduce_only,
    }
}

fn authority(
    root: &Path,
    binding: &HedgedGridBinding,
) -> Result<WriterLeaseAuthority, Box<dyn std::error::Error>> {
    Ok(WriterLeaseAuthority::open(
        root.join("writer.json"),
        stage7_writer_scope(binding),
    )?)
}

fn intent(reduce_only: bool) -> Result<GridOrderIntent, Box<dyn std::error::Error>> {
    Ok(GridOrderIntent {
        key: GridOrderKey {
            epoch: 1,
            position: GridPosition::Long,
            role: if reduce_only {
                GridOrderRole::Close
            } else {
                GridOrderRole::Open
            },
            level: 1,
        },
        side: if reduce_only {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        },
        quantity: Decimal::ONE,
        price: Price::new(Decimal::ONE)?,
        reduce_only,
    })
}

fn empty_readback() -> Result<GridVenueReadback, GridVenueError> {
    Ok(GridVenueReadback {
        raw_private_payloads: vec!["{}".to_owned()],
        order_family_readback: Some(GridOrderFamilyReadback::regular_only_adapter_profile(
            Vec::new(),
            vec!["[]".to_owned()],
        )?),
        balance: AccountBalance {
            asset: Asset::new("USDT").map_err(|_| GridVenueError::PrivateReadbackRequired)?,
            wallet_balance: Decimal::ZERO,
            available_balance: Decimal::ZERO,
            initial_margin: Decimal::ZERO,
            maintenance_margin: Decimal::ZERO,
        },
        hedge_position: true,
        positions: Vec::new(),
        orders: Vec::new(),
        fills: Vec::new(),
    })
}
