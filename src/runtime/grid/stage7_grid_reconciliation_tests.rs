use super::*;

#[test]
fn opening_fill_without_opposite_cancel_capacity_enters_signed_reconciliation()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let params = release_params(&cfg, &binding)?;
    let mut state = HedgedGridState::new_with_params(binding.clone(), params)?;
    let _ = state.observe_inventory(GridInventory {
        private_generation: 1,
        private_observed_at_ms: 100,
        mark_price: Price::new(Decimal::new(100, 0))?,
        long_quantity: Decimal::new(20, 2),
        short_quantity: Decimal::new(20, 2),
    })?;
    let _ = state.install_epoch(GridEpoch {
        epoch: 1,
        anchor_price: Price::new(Decimal::new(100, 0))?,
        step: Price::new(Decimal::new(2, 1))?,
        grid_quantity: Decimal::new(5, 2),
        passive_book_fallback: None,
    })?;
    state
        .owned_orders
        .retain(|key, _| key.position != GridPosition::Long || key.role != GridOrderRole::Close);
    let source = state
        .owned_orders
        .values()
        .find(|order| {
            order.key.position == GridPosition::Long
                && order.key.role == GridOrderRole::Open
                && order.key.level == 1
        })
        .cloned()
        .ok_or("missing long opening intent")?;
    let mut fill = GridVenueFill {
        fill: Fill {
            execution_sequence: FieldState::Known(1),
            fill_id: "gate-batch-open".to_owned(),
            order_id: client_order_id(&source.key)?.as_str().to_owned(),
            symbol: binding.symbol.clone(),
            side: source.side,
            position_side: FieldState::Known(PositionSide::Long),
            quantity: source.quantity,
            price: source.price,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(true),
            exchange_time_ms: Some(1),
        },
        client_order_id: FieldState::Known(client_order_id(&source.key)?.as_str().to_owned()),
    };
    fill.fill.quantity = Decimal::new(2, 2);
    let mut remaining_fill = fill.clone();
    remaining_fill.fill.fill_id = "gate-batch-open-remaining".to_owned();
    remaining_fill.fill.execution_sequence = FieldState::Known(2);
    remaining_fill.fill.quantity = Decimal::new(3, 2);
    let mut venue = ShadowVenue {
        instrument: instrument()?,
        readbacks: VecDeque::new(),
        minimum_quantity: Decimal::ONE,
        stream_polls_before_error: None,
        stream_resets: 0,
        public_payloads: VecDeque::new(),
        accepted_public_at_ms: None,
        public_resets: 0,
        exact_order_outcomes: VecDeque::from([Err(GridVenueError::Bitget(
            crate::exchange::bitget::BitgetError::Payload,
        ))]),
    };
    let authority = WriterLeaseAuthority::open(
        temporary.path().join(WRITER_FILE),
        WriterScope {
            exchange: binding.exchange.clone(),
            account: binding.account.clone(),
            symbol: binding.symbol.clone(),
            owner_scope: binding.owner_scope.clone(),
        },
    )?;
    let writer = authority.register_initial(1, 2)?;
    let mut commands = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;
    let store = ProjectionStore::new(temporary.path().join(CHECKPOINT_FILE));
    let mut checkpoint = Stage7GridCheckpoint {
        schema_version: 1,
        binding: binding.clone(),
        state,
        private_generation: 2,
        exposure_guard: None,
        pending_exposure_reduction: None,
        fill_history_start_ms: 1,
        order_health_fenced: false,
        last_order_health_checked_at_ms: 0,
    };
    let mut readback = shadow_readback()?;
    readback.fills = vec![fill, remaining_fill];

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
        FillDriveOutcome::recenter()
    );
    assert_eq!(checkpoint.state.phase, GridPhase::ResettingGrid);
    assert_eq!(
        checkpoint.state.reset_reason,
        Some(GridResetReason::Reconciliation)
    );
    assert!(!commands.has_unresolved());
    Ok(())
}

#[test]
fn signed_fill_dispatches_without_cached_book_refresh_or_exact_order_readback()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::{Arc, Mutex, atomic::AtomicUsize};

    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let params = release_params(&cfg, &binding)?;
    let mut state = HedgedGridState::new_with_params(binding.clone(), params)?;
    let _ = state.observe_inventory(GridInventory {
        private_generation: 1,
        private_observed_at_ms: 100,
        mark_price: Price::new(Decimal::new(100, 0))?,
        long_quantity: Decimal::new(20, 2),
        short_quantity: Decimal::new(20, 2),
    })?;
    let _ = state.install_epoch(GridEpoch {
        epoch: 1,
        anchor_price: Price::new(Decimal::new(100, 0))?,
        step: Price::new(Decimal::new(2, 1))?,
        grid_quantity: Decimal::new(5, 2),
        passive_book_fallback: None,
    })?;
    let source = state
        .owned_orders
        .values()
        .find(|order| {
            order.key.position == GridPosition::Long
                && order.key.role == GridOrderRole::Close
                && order.key.level == 1
        })
        .cloned()
        .ok_or("missing long closing intent")?;
    let expected_before_fill = state.owned_orders.len();
    let mut commands = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;
    for owned in state.owned_orders.values() {
        let GridMutation::Place(original) = place_command(&binding, &instrument()?, owned)? else {
            return Err("owned intent did not create a place command".into());
        };
        let command_id = original.command_id.clone();
        let venue_order_id = original.client_order_id.as_str().to_owned();
        commands.prepare_place(original)?;
        commands.transition(&command_id, CommandState::Submitted)?;
        commands.transition(&command_id, CommandState::Accepted { venue_order_id })?;
    }
    let mut checkpoint = Stage7GridCheckpoint {
        schema_version: 1,
        binding: binding.clone(),
        state,
        private_generation: 2,
        exposure_guard: None,
        pending_exposure_reduction: None,
        fill_history_start_ms: 1,
        order_health_fenced: false,
        last_order_health_checked_at_ms: 0,
    };
    let mut readback = shadow_readback()?;
    readback.fills.push(GridVenueFill {
        fill: Fill {
            execution_sequence: FieldState::Known(1),
            fill_id: "fast-reversal-fill".to_owned(),
            order_id: client_order_id(&source.key)?.as_str().to_owned(),
            symbol: binding.symbol.clone(),
            side: source.side,
            position_side: FieldState::Known(PositionSide::Long),
            quantity: source.quantity,
            price: source.price,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(true),
            exchange_time_ms: Some(1),
        },
        client_order_id: FieldState::Known(client_order_id(&source.key)?.as_str().to_owned()),
    });
    let calls = Arc::new(Mutex::new(Vec::new()));
    let book_reads = Arc::new(AtomicUsize::new(0));
    let mut venue = StreamFillVenue {
        instrument: instrument()?,
        client: RecordingMutationClient {
            calls: Arc::clone(&calls),
        },
        readback_calls: Arc::new(AtomicUsize::new(0)),
        book_reads: Arc::clone(&book_reads),
        exact_order_outcomes: VecDeque::new(),
        book: (
            Price::new(Decimal::new(9_998, 2))?,
            Price::new(Decimal::new(9_999, 2))?,
        ),
    };
    let authority = WriterLeaseAuthority::open(
        temporary.path().join(WRITER_FILE),
        WriterScope {
            exchange: binding.exchange.clone(),
            account: binding.account.clone(),
            symbol: binding.symbol.clone(),
            owner_scope: binding.owner_scope.clone(),
        },
    )?;
    let writer = authority.register_initial(1, 2)?;
    let store = ProjectionStore::new(temporary.path().join(CHECKPOINT_FILE));
    let wal_len_before_dispatch = std::fs::metadata(temporary.path().join(COMMAND_FILE))?.len();

    let outcome = process_complete_owned_fills(
        &mut checkpoint,
        &mut commands,
        &mut venue,
        &authority,
        &writer,
        &binding,
        &readback,
        &store,
    )?;

    assert_eq!(outcome, FillDriveOutcome::dispatched());
    assert!(
        std::fs::metadata(temporary.path().join(COMMAND_FILE))?.len() > wal_len_before_dispatch
    );
    assert_eq!(book_reads.load(Ordering::SeqCst), 0);
    let mut recorded = calls.lock().map_err(|_| "mutation calls poisoned")?.clone();
    recorded.sort_unstable();
    assert_eq!(recorded, ["cancel", "place", "place"]);
    assert!(!commands.has_unresolved());
    assert_eq!(checkpoint.state.owned_orders.len(), expected_before_fill);
    assert!(checkpoint.state.pending_transactions.is_empty());
    Ok(())
}

#[test]
fn restart_abandons_a_durable_fill_candidate_only_when_replacements_have_no_wal()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let params = release_params(&cfg, &binding)?;
    let mut state = HedgedGridState::new_with_params(binding.clone(), params)?;
    let _ = state.observe_inventory(GridInventory {
        private_generation: 1,
        private_observed_at_ms: 100,
        mark_price: Price::new(Decimal::new(100, 0))?,
        long_quantity: Decimal::new(20, 2),
        short_quantity: Decimal::new(20, 2),
    })?;
    let _ = state.install_epoch(GridEpoch {
        epoch: 1,
        anchor_price: Price::new(Decimal::new(100, 0))?,
        step: Price::new(Decimal::new(2, 1))?,
        grid_quantity: Decimal::new(5, 2),
        passive_book_fallback: None,
    })?;
    let source = state
        .owned_orders
        .values()
        .find(|order| {
            order.key.position == GridPosition::Long
                && order.key.role == GridOrderRole::Close
                && order.key.level == 1
        })
        .cloned()
        .ok_or("missing crash candidate source")?;
    let owned_before_fill = state.owned_orders.len();
    let application = hedged_grid::apply_owned_grid_fill(
        &mut state,
        crate::strategy::hedged_grid::OwnedGridFill {
            fill_id: "stream-crash-before-wal".to_owned(),
            private_generation: 2,
            source_order: source.key.clone(),
            fill_price: source.price,
            complete: true,
            maker: FieldState::Known(true),
        },
        hedged_grid::GridFillProjection::ProjectStreamInventory,
    )?;
    assert!(matches!(
        application,
        hedged_grid::GridFillApplication::Rolling(_)
    ));
    assert_eq!(state.phase, GridPhase::Running);
    assert_eq!(state.pending_transactions.len(), 1);
    let mut checkpoint = Stage7GridCheckpoint {
        schema_version: 1,
        binding,
        state,
        private_generation: 2,
        exposure_guard: None,
        pending_exposure_reduction: None,
        fill_history_start_ms: 1,
        order_health_fenced: false,
        last_order_health_checked_at_ms: 0,
    };
    let commands = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;

    assert!(recover_interrupted_fill_transactions(
        &mut checkpoint,
        &commands
    )?);
    assert_eq!(checkpoint.state.phase, GridPhase::ResettingGrid);
    assert_eq!(
        checkpoint.state.reset_reason,
        Some(GridResetReason::Reconciliation)
    );
    assert!(checkpoint.state.pending_transactions.is_empty());
    assert!(!checkpoint.state.owned_orders.contains_key(&source.key));
    assert_eq!(checkpoint.state.owned_orders.len(), owned_before_fill - 1);
    Ok(())
}

#[test]
fn restart_fences_the_whole_fill_transaction_when_any_replacement_reached_wal()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let params = release_params(&cfg, &binding)?;
    let mut state = HedgedGridState::new_with_params(binding.clone(), params)?;
    let _ = state.observe_inventory(GridInventory {
        private_generation: 1,
        private_observed_at_ms: 100,
        mark_price: Price::new(Decimal::new(100, 0))?,
        long_quantity: Decimal::new(20, 2),
        short_quantity: Decimal::new(20, 2),
    })?;
    let _ = state.install_epoch(GridEpoch {
        epoch: 1,
        anchor_price: Price::new(Decimal::new(100, 0))?,
        step: Price::new(Decimal::new(2, 1))?,
        grid_quantity: Decimal::new(5, 2),
        passive_book_fallback: None,
    })?;
    let source = state
        .owned_orders
        .values()
        .find(|order| {
            order.key.position == GridPosition::Long
                && order.key.role == GridOrderRole::Close
                && order.key.level == 1
        })
        .cloned()
        .ok_or("missing WAL-bound crash source")?;
    let application = hedged_grid::apply_owned_grid_fill(
        &mut state,
        crate::strategy::hedged_grid::OwnedGridFill {
            fill_id: "stream-crash-after-first-wal".to_owned(),
            private_generation: 2,
            source_order: source.key,
            fill_price: source.price,
            complete: true,
            maker: FieldState::Known(true),
        },
        hedged_grid::GridFillProjection::ProjectStreamInventory,
    )?;
    let hedged_grid::GridFillApplication::Rolling(actions) = application else {
        return Err("missing WAL-bound rolling application".into());
    };
    let transaction = actions
        .iter()
        .find_map(|action| match action {
            GridAction::Dispatch(transaction) => Some(transaction.clone()),
            _ => None,
        })
        .ok_or("missing WAL-bound transaction")?;
    let mut checkpoint = Stage7GridCheckpoint {
        schema_version: 1,
        binding: binding.clone(),
        state,
        private_generation: 2,
        exposure_guard: None,
        pending_exposure_reduction: None,
        fill_history_start_ms: 1,
        order_health_fenced: false,
        last_order_health_checked_at_ms: 0,
    };
    let mut commands = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;
    let GridMutation::Place(first_replacement) =
        place_command(&binding, &instrument()?, &transaction.places[0])?
    else {
        return Err("replacement did not become a place command".into());
    };
    let first_command_id = first_replacement.command_id.clone();
    commands.prepare_place(first_replacement)?;

    assert_eq!(commands.fence_interrupted_dispatches()?, (1, 0));
    assert!(
        commands
            .receipt(&first_command_id)
            .is_some_and(|receipt| matches!(receipt.state, CommandState::Rejected { .. }))
    );
    assert!(recover_interrupted_fill_transactions(
        &mut checkpoint,
        &commands
    )?);
    assert_eq!(checkpoint.state.phase, GridPhase::BlockedUnknown);
    assert_eq!(checkpoint.state.pending_transactions.len(), 1);
    assert!(
        transaction
            .places
            .iter()
            .all(|replacement| checkpoint.state.owned_orders.contains_key(&replacement.key))
    );
    assert!(
        !checkpoint
            .state
            .owned_orders
            .contains_key(&transaction.cancel)
    );
    Ok(())
}
