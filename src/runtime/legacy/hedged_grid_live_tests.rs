use super::*;

#[test]
fn legacy_stop_preserves_the_existing_durable_account_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = std::fs::canonicalize(temporary.path())?;
    let mut binding = phase_one_binding()?;
    binding.account = "portfolio_margin_um".to_owned();
    let store = ProjectionStore::new(root.join(GRID_CONTROL_FILE));
    store.save(&HedgedGridControl {
        schema_version: 1,
        binding: binding.clone(),
        target: HedgedGridControlTarget::Running,
    })?;

    request_existing_hedged_grid_stop(&root)?;

    let stopped = store
        .load::<HedgedGridControl>()?
        .ok_or("missing stopped control")?;
    assert_eq!(stopped.binding, binding);
    assert_eq!(stopped.target, HedgedGridControlTarget::Stop);
    Ok(())
}

#[test]
fn configured_grid_count_is_loaded_for_a_new_checkpoint() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = tempfile::tempdir()?;
    let store = ProjectionStore::new(temporary.path().join("grid.json"));
    let params = HedgedGridParams::phase_one(3)?;

    let state = load_state(&store, &phase_one_binding()?, &params)?;

    assert_eq!(state.params.grid_count, 3);
    Ok(())
}

#[test]
fn missing_legacy_control_fails_closed_and_stopping_needs_explicit_reset()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = phase_one_binding()?;
    let store = ProjectionStore::new(temporary.path().join("control.json"));
    assert_eq!(
        read_control(&store, &binding)?,
        HedgedGridControlTarget::Stop
    );

    let mut state = HedgedGridState::new_with_params(binding, HedgedGridParams::phase_one(3)?)?;
    state.phase = GridPhase::Stopping;
    assert!(matches!(
        resume_stopping_state_if_requested(&mut state, HedgedGridControlTarget::Running),
        Err(HedgedGridLiveError::Stopped)
    ));
    assert_eq!(state.phase, GridPhase::Stopping);
    assert!(resume_stopping_state_if_requested(
        &mut state,
        HedgedGridControlTarget::Reset
    )?);
    assert_eq!(state.phase, GridPhase::Recovering);
    Ok(())
}

#[test]
fn changed_grid_count_requires_an_explicit_reset() -> Result<(), Box<dyn std::error::Error>> {
    let mut state =
        HedgedGridState::new_with_params(phase_one_binding()?, HedgedGridParams::phase_one(10)?)?;

    assert!(matches!(
        apply_release_params(&mut state, HedgedGridParams::phase_one(3)?, false),
        Err(HedgedGridLiveError::ParameterChangeRequiresReset)
    ));
    assert_eq!(state.params.grid_count, 10);
    assert!(apply_release_params(
        &mut state,
        HedgedGridParams::phase_one(3)?,
        true
    )?);
    assert_eq!(state.params.grid_count, 3);
    assert_eq!(state.phase, GridPhase::ResettingGrid);
    assert_eq!(state.reset_reason, Some(GridResetReason::Manual));
    Ok(())
}

#[test]
fn successful_inventory_replenishment_requests_immediate_private_confirmation() {
    assert!(should_reconcile_after_grid_mutation(
        true,
        false,
        GridPhase::ReplenishingInventory
    ));
    assert!(should_reconcile_after_grid_mutation(
        true,
        false,
        GridPhase::ResettingGrid
    ));
    assert!(!should_reconcile_after_grid_mutation(
        true,
        false,
        GridPhase::Running
    ));
    assert!(!should_reconcile_after_grid_mutation(
        false,
        false,
        GridPhase::ReplenishingInventory
    ));
}

#[test]
fn reconstructed_replenishment_round_cannot_reuse_old_market_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let binding = phase_one_binding()?;
    let instrument = binance_instrument()?;
    let previous_replenishment = crate::strategy::hedged_grid::GridReplenishment {
        round: 227,
        private_generation: 101,
        position: GridPosition::Long,
        target_notional: crate::domain::Amount::new("USDC".parse()?, Decimal::new(15, 0)),
    };
    let first_inventory = GridInventory {
        private_generation: 101,
        private_observed_at_ms: 1_001,
        mark_price: Price::new(Decimal::new(100, 0))?,
        long_quantity: Decimal::ZERO,
        short_quantity: Decimal::ONE,
    };
    let second_inventory = GridInventory {
        private_generation: 202,
        private_observed_at_ms: 2_002,
        mark_price: Price::new(Decimal::new(80, 0))?,
        ..first_inventory.clone()
    };

    let first = market_command(
        &binding,
        &previous_replenishment,
        &instrument,
        &first_inventory,
    )?;
    let temporary = tempfile::tempdir()?;
    let mut journal = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
    first.prepare(&mut journal)?;
    let mut reconstructed =
        HedgedGridState::new_with_params(binding.clone(), HedgedGridParams::phase_one(10)?)?;
    let _ = reconstructed.observe_inventory(second_inventory.clone())?;
    reconstructed
        .reconcile_replenishment_round(highest_durable_replenishment_round(&journal, &binding)?)?;
    let GridDecision::Actions(actions) = reconstructed.begin_replenishment()? else {
        return Err("expected replenishment".into());
    };
    let GridAction::Replenish(next_replenishment) = &actions[0] else {
        return Err("expected market replenishment".into());
    };
    assert_eq!(next_replenishment.round, 228);
    let second = market_command(&binding, next_replenishment, &instrument, &second_inventory)?;
    assert_ne!(first.command_id(), second.command_id());
    second.prepare(&mut journal)?;
    Ok(())
}

#[test]
fn malformed_owned_replenishment_identity_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let GridMutation::Market(mut command) = market_command(
        &phase_one_binding()?,
        &crate::strategy::hedged_grid::GridReplenishment {
            round: 1,
            private_generation: 1,
            position: GridPosition::Long,
            target_notional: crate::domain::Amount::new("USDC".parse()?, Decimal::new(15, 0)),
        },
        &binance_instrument()?,
        &GridInventory {
            private_generation: 1,
            private_observed_at_ms: 1,
            mark_price: Price::new(Decimal::new(100, 0))?,
            long_quantity: Decimal::ZERO,
            short_quantity: Decimal::ONE,
        },
    )?
    else {
        return Err("expected market command".into());
    };
    command.client_order_id = CommandId::new("hgm_rbad_long")?;
    command.command_id = CommandId::new("hgm_rbad_long_cmd")?;
    let temporary = tempfile::tempdir()?;
    let mut journal = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
    journal.prepare_market(command)?;
    assert!(matches!(
        highest_durable_replenishment_round(&journal, &phase_one_binding()?),
        Err(HedgedGridLiveError::Identifier)
    ));
    Ok(())
}

#[test]
fn recovered_inventory_skips_stale_replenishment_reason() -> Result<(), Box<dyn std::error::Error>>
{
    let mut state =
        HedgedGridState::new_with_params(phase_one_binding()?, HedgedGridParams::phase_one(10)?)?;
    let inventory = GridInventory {
        private_generation: 2,
        private_observed_at_ms: 200,
        mark_price: Price::new(Decimal::new(94, 0))?,
        long_quantity: Decimal::new(6, 2),
        short_quantity: Decimal::new(269, 2),
    };
    state.phase = GridPhase::ResettingGrid;
    state.reset_reason = Some(GridResetReason::InventoryLow);
    state.inventory = Some(inventory.clone());

    assert_eq!(
        effective_reset_reason(&state, &inventory)?,
        GridResetReason::InventoryReplenished
    );
    Ok(())
}

#[test]
fn transient_private_transport_startup_errors_are_retried() {
    assert!(retryable_private_transport_startup_failure(
        &PrivateFactsWorkerError::Private(PrivateError::Http)
    ));
    assert!(retryable_private_transport_startup_failure(
        &PrivateFactsWorkerError::Private(PrivateError::Clock)
    ));
    assert!(!retryable_private_transport_startup_failure(
        &PrivateFactsWorkerError::Private(PrivateError::Credentials)
    ));
}

#[test]
fn transient_public_instrument_startup_errors_are_retried() {
    assert!(retryable_public_startup_failure(
        &PublicError::TransportRetriesExhausted
    ));
    assert!(retryable_public_startup_failure(&PublicError::RateLimited));
    assert!(retryable_public_startup_failure(
        &PublicError::ServerFailure(503)
    ));
    assert!(!retryable_public_startup_failure(&PublicError::HttpStatus(
        400
    )));
    assert!(!retryable_public_startup_failure(&PublicError::Proxy));
}

#[test]
fn one_private_batch_reserves_both_mirrored_fills_before_dispatch()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let store = ProjectionStore::new(temporary.path().join("grid.json"));
    let mut state =
        HedgedGridState::new_with_params(phase_one_binding()?, HedgedGridParams::phase_one(10)?)?;
    let inventory = GridInventory {
        private_generation: 1,
        private_observed_at_ms: 100,
        mark_price: Price::new(Decimal::new(100, 0))?,
        long_quantity: Decimal::new(15, 2),
        short_quantity: Decimal::new(15, 2),
    };
    let _ = state.observe_inventory(inventory)?;
    let _ = state.install_epoch(GridEpoch {
        epoch: 1,
        anchor_price: Price::new(Decimal::new(100, 0))?,
        step: Price::new(Decimal::new(2, 1))?,
        grid_quantity: Decimal::new(5, 2),
        passive_book_fallback: None,
    })?;
    let fills = vec![
        OwnedGridFill {
            fill_id: "mirrored-short".to_owned(),
            private_generation: 2,
            source_order: GridOrderKey {
                epoch: 1,
                position: GridPosition::Short,
                role: GridOrderRole::Open,
                level: 1,
            },
            fill_price: Price::new(Decimal::new(1002, 1))?,
            complete: true,
            maker: FieldState::Known(true),
        },
        OwnedGridFill {
            fill_id: "mirrored-long".to_owned(),
            private_generation: 2,
            source_order: GridOrderKey {
                epoch: 1,
                position: GridPosition::Long,
                role: GridOrderRole::Close,
                level: 1,
            },
            fill_price: Price::new(Decimal::new(1002, 1))?,
            complete: true,
            maker: FieldState::Known(true),
        },
    ];

    let actions = reserve_confirmed_fills(&mut state, &store, fills)?;

    assert_eq!(actions.len(), 2);
    assert!(
        actions
            .iter()
            .all(|action| matches!(action, GridAction::Dispatch(_)))
    );
    let transactions = actions
        .iter()
        .filter_map(|action| match action {
            GridAction::Dispatch(transaction) => Some(transaction.clone()),
            GridAction::Reset { .. }
            | GridAction::Place(_)
            | GridAction::Replenish(_)
            | GridAction::ReanchorAtFill { .. } => None,
        })
        .collect::<Vec<_>>();
    let journal = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
    let mutations = unsettled_transaction_mutations(
        &journal,
        &phase_one_binding()?,
        &binance_instrument()?,
        &transactions,
    )?;
    assert_eq!(mutations.len(), 6);
    assert_eq!(
        mutations
            .iter()
            .filter(|mutation| matches!(mutation, GridMutation::Place(_)))
            .count(),
        4
    );
    assert_eq!(
        mutations
            .iter()
            .filter(|mutation| matches!(mutation, GridMutation::Cancel(_)))
            .count(),
        2
    );
    assert_eq!(state.pending_transactions.len(), 2);
    assert_eq!(state.owned_orders.len(), 26);
    let checkpoint = store
        .load::<HedgedGridCheckpoint>()?
        .ok_or("missing durable grid checkpoint")?;
    assert_eq!(checkpoint.state.pending_transactions.len(), 2);
    Ok(())
}

fn binance_instrument() -> Result<crate::domain::Instrument, Box<dyn std::error::Error>> {
    Ok(crate::domain::Instrument {
        symbol: "SOL/USDC".parse()?,
        market: crate::domain::MarketKind::LinearPerpetual,
        settlement_asset: Some("USDC".parse()?),
        generation: 1,
        price_tick: Price::new(Decimal::new(1, 2))?,
        quantity_step: Decimal::new(1, 2),
        minimum_notional: crate::domain::Amount::new("USDC".parse()?, Decimal::new(5, 0)),
    })
}

#[test]
fn opening_order_is_rounded_up_to_the_exchange_notional_floor()
-> Result<(), Box<dyn std::error::Error>> {
    let binding = phase_one_binding()?;
    let order = GridOrderIntent {
        key: GridOrderKey {
            epoch: 1,
            position: GridPosition::Long,
            role: GridOrderRole::Open,
            level: 10,
        },
        side: OrderSide::Buy,
        price: Price::new(Decimal::new(9821, 2))?,
        quantity: Decimal::new(5, 2),
        reduce_only: false,
    };

    let GridMutation::Place(command) = place_command(&binding, &binance_instrument()?, &order)?
    else {
        return Err("expected limit order".into());
    };

    assert_eq!(command.quantity, Decimal::new(6, 2));
    assert!(command.quantity * command.limit_price.value() >= Decimal::new(5, 0));
    Ok(())
}

#[test]
fn reduce_only_order_keeps_its_inventory_bounded_quantity() -> Result<(), Box<dyn std::error::Error>>
{
    let binding = phase_one_binding()?;
    let order = GridOrderIntent {
        key: GridOrderKey {
            epoch: 1,
            position: GridPosition::Long,
            role: GridOrderRole::Close,
            level: 1,
        },
        side: OrderSide::Sell,
        price: Price::new(Decimal::new(9821, 2))?,
        quantity: Decimal::new(5, 2),
        reduce_only: true,
    };

    let GridMutation::Place(command) = place_command(&binding, &binance_instrument()?, &order)?
    else {
        return Err("expected limit order".into());
    };

    assert_eq!(command.quantity, order.quantity);
    Ok(())
}

#[test]
fn adjacent_stream_fill_reserves_before_the_first_transaction_settles()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state =
        HedgedGridState::new_with_params(phase_one_binding()?, HedgedGridParams::phase_one(10)?)?;
    let _ = state.observe_inventory(GridInventory {
        private_generation: 1,
        private_observed_at_ms: 100,
        mark_price: Price::new(Decimal::new(100, 0))?,
        long_quantity: Decimal::new(65, 2),
        short_quantity: Decimal::new(65, 2),
    })?;
    let _ = state.install_epoch(GridEpoch {
        epoch: 1,
        anchor_price: Price::new(Decimal::new(100, 0))?,
        step: Price::new(Decimal::new(2, 1))?,
        grid_quantity: Decimal::new(5, 2),
        passive_book_fallback: None,
    })?;
    let GridDecision::Actions(first) = state.observe_stream_owned_fill(OwnedGridFill {
        fill_id: "first-stream-fill".to_owned(),
        private_generation: 2,
        source_order: GridOrderKey {
            epoch: 1,
            position: GridPosition::Short,
            role: GridOrderRole::Open,
            level: 1,
        },
        fill_price: Price::new(Decimal::new(902, 1))?,
        complete: true,
        maker: FieldState::Known(true),
    })?
    else {
        return Err("missing first transaction".into());
    };
    let GridDecision::Actions(second) = state.observe_stream_owned_fill(OwnedGridFill {
        fill_id: "second-stream-fill".to_owned(),
        private_generation: 2,
        source_order: GridOrderKey {
            epoch: 1,
            position: GridPosition::Long,
            role: GridOrderRole::Close,
            level: 1,
        },
        fill_price: Price::new(Decimal::new(902, 1))?,
        complete: true,
        maker: FieldState::Known(true),
    })?
    else {
        return Err("missing second transaction".into());
    };
    let GridAction::Dispatch(first) = first[0].clone() else {
        return Err("missing first dispatch".into());
    };
    let GridAction::Dispatch(second) = second[0].clone() else {
        return Err("missing second dispatch".into());
    };
    assert_ne!(first.id, second.id);
    assert_eq!(state.pending_transactions.len(), 2);
    Ok(())
}

#[test]
fn rolling_reduce_order_uses_uncommitted_inventory_quantity()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state =
        HedgedGridState::new_with_params(phase_one_binding()?, HedgedGridParams::phase_one(10)?)?;
    let initial = GridInventory {
        private_generation: 1,
        private_observed_at_ms: 100,
        mark_price: Price::new(Decimal::new(90, 0))?,
        long_quantity: Decimal::new(65, 2),
        short_quantity: Decimal::new(17, 2),
    };
    let _ = state.observe_inventory(initial)?;
    let _ = state.install_epoch(GridEpoch {
        epoch: 1,
        anchor_price: Price::new(Decimal::new(90, 0))?,
        step: Price::new(Decimal::new(2, 1))?,
        grid_quantity: Decimal::new(6, 2),
        passive_book_fallback: None,
    })?;
    let GridDecision::Actions(actions) = state.observe_stream_owned_fill(OwnedGridFill {
        fill_id: "short-close-fill".to_owned(),
        private_generation: 2,
        source_order: GridOrderKey {
            epoch: 1,
            position: GridPosition::Short,
            role: GridOrderRole::Close,
            level: 1,
        },
        fill_price: Price::new(Decimal::new(902, 1))?,
        complete: true,
        maker: FieldState::Known(true),
    })?
    else {
        return Err("missing direct stream actions".into());
    };
    let GridAction::Dispatch(transaction) = &actions[0] else {
        return Err("missing rolling transaction".into());
    };
    let close = transaction
        .places
        .iter()
        .find(|order| order.key.role == GridOrderRole::Close)
        .ok_or("missing close replacement")?;
    assert_eq!(close.quantity, Decimal::new(5, 2));
    Ok(())
}

#[test]
fn recovery_accepts_only_downsized_reduce_only_quantity() {
    let requested = Decimal::new(6, 2);
    let clipped = Decimal::new(5, 2);
    assert!(recovered_quantity_matches(requested, clipped, true));
    assert!(!recovered_quantity_matches(requested, clipped, false));
    assert!(!recovered_quantity_matches(
        requested,
        Decimal::new(7, 2),
        true
    ));
}

#[test]
fn signed_recovery_orders_terminal_fills_by_native_execution_sequence()
-> Result<(), Box<dyn std::error::Error>> {
    let fill =
        |fill_id: &str, role: GridOrderRole| -> Result<OwnedGridFill, Box<dyn std::error::Error>> {
            Ok(OwnedGridFill {
                fill_id: fill_id.to_owned(),
                private_generation: 3,
                source_order: GridOrderKey {
                    epoch: 1,
                    position: GridPosition::Long,
                    role,
                    level: 1,
                },
                fill_price: Price::new(Decimal::new(902, 1))?,
                complete: true,
                maker: FieldState::Known(true),
            })
        };
    let ordered = order_confirmed_fills(vec![
        (20, fill("later-open", GridOrderRole::Open)?),
        (10, fill("earlier-close", GridOrderRole::Close)?),
    ])?;
    assert_eq!(ordered[0].fill_id, "earlier-close");
    assert_eq!(ordered[1].fill_id, "later-open");
    assert!(matches!(
        order_confirmed_fills(vec![
            (10, fill("first", GridOrderRole::Open)?),
            (10, fill("conflict", GridOrderRole::Close)?),
        ]),
        Err(HedgedGridLiveError::FillLiquidityUnknown)
    ));
    Ok(())
}
