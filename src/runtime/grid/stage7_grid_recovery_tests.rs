#[test]
fn current_canary_evidence_is_required_before_live_grid_entry()
-> Result<(), Box<dyn std::error::Error>> {
    let binding = CapabilityBinding {
        exchange: "gate".to_owned(),
        account_binding: "usdt_futures_dual".to_owned(),
        symbol: "DOGE/USDT".to_owned(),
        api_key_sha256: "a".repeat(64),
    };
    let temporary = tempfile::tempdir()?;
    let store = CapabilityEvidenceStore::open(temporary.path().join(CAPABILITY_EVIDENCE_FILE))?;
    assert!(store.current(&binding, 1)?.is_empty());
    assert!(binding.validate().is_ok());
    Ok(())
}

#[test]
fn periodic_order_health_requires_the_signed_orders_to_match_three_opening_lanes()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let params = release_params(&cfg, &binding)?;
    let mut state = HedgedGridState::new_with_params(binding.clone(), params)?;
    state.phase = GridPhase::Running;
    let intents = [
        intent(1, GridPosition::Long, 1)?,
        intent(1, GridPosition::Long, 2)?,
        intent(1, GridPosition::Long, 3)?,
        intent(1, GridPosition::Short, 1)?,
        intent(1, GridPosition::Short, 2)?,
        intent(1, GridPosition::Short, 3)?,
    ];
    state.owned_orders = intents
        .iter()
        .cloned()
        .map(|intent| (intent.key.clone(), intent))
        .collect();
    let checkpoint = Stage7GridCheckpoint {
        schema_version: 1,
        binding: binding.clone(),
        state,
        private_generation: 1,
        exposure_guard: None,
        pending_exposure_reduction: None,
        fill_history_start_ms: 1,
        order_health_fenced: false,
        last_order_health_checked_at_ms: 0,
    };
    let mut readback = shadow_readback()?;
    readback.orders = intents
        .iter()
        .map(|intent| health_order(&binding, intent))
        .collect::<Result<Vec<_>, _>>()?;
    let commands = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;
    let report = stage7_health::evaluate(&checkpoint, &commands, &readback, 2, 100);
    assert!(report.is_healthy());
    assert_eq!(report.observed_long_opening, 3);
    assert_eq!(report.observed_short_opening, 3);
    stage7_health::persist(temporary.path(), &report)?;
    assert!(temporary.path().join(ORDER_HEALTH_FILE).exists());

    let mut persisted_schedule = checkpoint.clone();
    persisted_schedule.last_order_health_checked_at_ms = 100;
    assert!(!order_health_due(
        &persisted_schedule,
        100 + ORDER_HEALTH_INTERVAL_MS - 1
    ));
    assert!(order_health_due(
        &persisted_schedule,
        100 + ORDER_HEALTH_INTERVAL_MS
    ));

    readback.orders.pop();
    let unhealthy = stage7_health::evaluate(&checkpoint, &commands, &readback, 3, 200);
    assert_eq!(
        unhealthy.status,
        stage7_health::Stage7GridHealthStatus::Unhealthy
    );
    assert!(
        unhealthy
            .problems
            .contains(&"signed_open_orders_do_not_match_checkpoint".to_owned())
    );
    let transitioning = stage7_health::transitioning_after_dispatch(unhealthy.clone());
    assert_eq!(
        transitioning.status,
        stage7_health::Stage7GridHealthStatus::Transitioning
    );
    assert!(
        transitioning
            .problems
            .contains(&"fill_replacement_pending_fresh_readback".to_owned())
    );
    let mut transition_checkpoint = checkpoint.clone();
    transition_checkpoint.last_order_health_checked_at_ms = 100;
    let transition_store = ProjectionStore::new(temporary.path().join("transition_state.json"));
    let mut force_order_health_check = false;
    persist_order_health(
        temporary.path(),
        &transition_store,
        &mut transition_checkpoint,
        &commands,
        &readback,
        3,
        200,
        true,
        &mut force_order_health_check,
        "gate",
    )?;
    assert_eq!(transition_checkpoint.last_order_health_checked_at_ms, 100);
    assert!(force_order_health_check);

    let mut ambiguous = readback.orders[0].clone();
    ambiguous.client_order_id = FieldState::Known("hgo_e01_long_open_l01".to_owned());
    readback.orders = vec![ambiguous];
    let identity_mismatch = stage7_health::evaluate(&checkpoint, &commands, &readback, 4, 300);
    assert!(
        identity_mismatch
            .problems
            .contains(&"open_order_client_identity_is_not_exact".to_owned())
    );
    Ok(())
}

#[test]
fn periodic_order_health_accepts_configured_ten_level_opening_lanes()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = gate_binding(&config(10)?)?;
    let params = release_params(&config(10)?, &binding)?;
    let mut state = HedgedGridState::new_with_params(binding.clone(), params)?;
    state.phase = GridPhase::Running;
    let mut intents = Vec::new();
    for position in [GridPosition::Long, GridPosition::Short] {
        for level in 1..=10 {
            intents.push(intent(1, position, level)?);
        }
    }
    state.owned_orders = intents
        .iter()
        .cloned()
        .map(|intent| (intent.key.clone(), intent))
        .collect();
    let checkpoint = Stage7GridCheckpoint {
        schema_version: 1,
        binding: binding.clone(),
        state,
        private_generation: 1,
        exposure_guard: None,
        pending_exposure_reduction: None,
        fill_history_start_ms: 1,
        order_health_fenced: false,
        last_order_health_checked_at_ms: 0,
    };
    let mut readback = shadow_readback()?;
    readback.orders = intents
        .iter()
        .map(|intent| health_order(&binding, intent))
        .collect::<Result<Vec<_>, _>>()?;
    let commands = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;
    let report = stage7_health::evaluate(&checkpoint, &commands, &readback, 2, 100);
    assert!(report.is_healthy());
    assert_eq!(report.expected_long_opening, 10);
    assert_eq!(report.expected_short_opening, 10);
    Ok(())
}

#[test]
fn complete_signed_readback_rebuilds_missing_owned_order_without_waiting_for_health_timer()
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
    let mut commands = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;
    let mut readback = shadow_readback()?;
    for owned in state.owned_orders.values() {
        let GridMutation::Place(command) = place_command(&binding, &instrument()?, owned)? else {
            return Err("owned intent did not create a place command".into());
        };
        let command_id = command.command_id.clone();
        let venue_order_id = command.client_order_id.as_str().to_owned();
        readback.orders.push(health_order(&binding, owned)?);
        commands.prepare_place(command)?;
        commands.transition(&command_id, CommandState::Submitted)?;
        commands.transition(&command_id, CommandState::Accepted { venue_order_id })?;
    }
    let expected = state.owned_orders.len();
    readback.orders.pop();

    assert_eq!(
        reconcile_signed_order_loss(&mut state, &commands, &binding, &readback)?,
        Some((expected, expected - 1))
    );
    assert_eq!(state.phase, GridPhase::ResettingGrid);
    assert_eq!(state.owned_orders.len(), expected - 1);
    Ok(())
}

#[test]
fn signed_scope_requires_an_accepted_wal_and_exact_checkpoint_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let params = release_params(&cfg, &binding)?;
    let intent = intent(1, GridPosition::Long, 1)?;
    let GridMutation::Place(command) = place_command(&binding, &instrument()?, &intent)? else {
        return Err("grid intent did not create a limit command".into());
    };
    let command_id = command.command_id.clone();
    let mut commands = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;
    commands.prepare_place(command)?;
    commands.transition(&command_id, CommandState::Submitted)?;

    let mut state = HedgedGridState::new_with_params(binding.clone(), params)?;
    state.phase = GridPhase::Running;
    state
        .owned_orders
        .insert(intent.key.clone(), intent.clone());
    let mut readback = shadow_readback()?;
    readback.orders = vec![health_order(&binding, &intent)?];

    assert!(matches!(
        verify_readback_scope(&state, &commands, &readback, &binding),
        Err(Stage7GridError::Unresolved)
    ));
    commands.transition(
        &command_id,
        CommandState::Accepted {
            venue_order_id: client_order_id(&intent.key)?.as_str().to_owned(),
        },
    )?;
    verify_readback_scope(&state, &commands, &readback, &binding)?;

    let signed = readback.orders[0].clone();
    let mut foreign = signed.clone();
    foreign.side = OrderSide::Sell;
    readback.orders = vec![foreign];
    assert!(matches!(
        verify_readback_scope(&state, &commands, &readback, &binding),
        Err(Stage7GridError::ForeignOrders)
    ));
    let mut foreign = signed.clone();
    foreign.position_side = FieldState::Known(PositionSide::Short);
    readback.orders = vec![foreign];
    assert!(matches!(
        verify_readback_scope(&state, &commands, &readback, &binding),
        Err(Stage7GridError::ForeignOrders)
    ));
    let mut foreign = signed.clone();
    foreign.reduce_only = true;
    readback.orders = vec![foreign];
    assert!(matches!(
        verify_readback_scope(&state, &commands, &readback, &binding),
        Err(Stage7GridError::ForeignOrders)
    ));
    let mut foreign = signed.clone();
    foreign.quantity += Decimal::ONE;
    readback.orders = vec![foreign];
    assert!(matches!(
        verify_readback_scope(&state, &commands, &readback, &binding),
        Err(Stage7GridError::ForeignOrders)
    ));
    readback.orders = vec![signed];
    let signed_order_id = readback.orders[0].order_id.clone();
    readback.orders[0].order_id = "foreign-venue-order".to_owned();
    assert!(matches!(
        verify_readback_scope(&state, &commands, &readback, &binding),
        Err(Stage7GridError::ForeignOrders)
    ));
    readback.orders[0].order_id = signed_order_id;
    readback.orders[0].limit_price = Some(Price::new(Decimal::new(99, 0))?);
    assert!(matches!(
        verify_readback_scope(&state, &commands, &readback, &binding),
        Err(Stage7GridError::ForeignOrders)
    ));
    readback.orders[0].limit_price = Some(intent.price);
    readback.orders[0].state = OrderState::Filled;
    assert!(matches!(
        verify_readback_scope(&state, &commands, &readback, &binding),
        Err(Stage7GridError::Unresolved)
    ));
    Ok(())
}

#[test]
fn accepted_cancel_may_wait_for_exact_stale_open_order_but_not_foreign_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let params = release_params(&cfg, &binding)?;
    let intent = intent(1, GridPosition::Long, 1)?;
    let GridMutation::Place(command) = place_command(&binding, &instrument()?, &intent)? else {
        return Err("grid intent did not create a limit command".into());
    };
    let command_id = command.command_id.clone();
    let client_id = command.client_order_id.clone();
    let mut commands = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;
    commands.prepare_place(command)?;
    commands.transition(&command_id, CommandState::Submitted)?;
    commands.transition(
        &command_id,
        CommandState::Accepted {
            venue_order_id: client_id.as_str().to_owned(),
        },
    )?;
    let GridMutation::Cancel(cancel) = next_cancel_command(&commands, &binding, &intent.key)?
    else {
        return Err("grid intent did not create a cancel command".into());
    };
    let cancel_id = cancel.command_id.clone();
    commands.prepare_cancel(cancel)?;
    commands.transition(&cancel_id, CommandState::Submitted)?;
    commands.transition(
        &cancel_id,
        CommandState::Accepted {
            venue_order_id: client_id.as_str().to_owned(),
        },
    )?;

    let mut state = HedgedGridState::new_with_params(binding.clone(), params)?;
    state.phase = GridPhase::Running;
    let mut readback = shadow_readback()?;
    readback.orders = vec![health_order(&binding, &intent)?];
    assert!(signed_readback_contains_settling_owned_cancel(
        &state, &commands, &binding, &readback,
    ));

    readback.orders[0].quantity += Decimal::ONE;
    assert!(!signed_readback_contains_settling_owned_cancel(
        &state, &commands, &binding, &readback,
    ));
    state.owned_orders.insert(intent.key.clone(), intent);
    assert!(!signed_readback_contains_settling_owned_cancel(
        &state, &commands, &binding, &readback,
    ));
    Ok(())
}

#[test]
fn signed_scope_rejects_an_accepted_wal_from_another_owner_binding()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let params = release_params(&cfg, &binding)?;
    let intent = intent(1, GridPosition::Long, 1)?;
    let GridMutation::Place(mut command) = place_command(&binding, &instrument()?, &intent)? else {
        return Err("grid intent did not create a limit command".into());
    };
    command.owner.run_id = "foreign_run".to_owned();
    let command_id = command.command_id.clone();
    let venue_order_id = command.client_order_id.as_str().to_owned();
    let mut commands = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;
    commands.prepare_place(command)?;
    commands.transition(&command_id, CommandState::Submitted)?;
    commands.transition(&command_id, CommandState::Accepted { venue_order_id })?;
    let mut state = HedgedGridState::new_with_params(binding.clone(), params)?;
    state.phase = GridPhase::Running;
    state
        .owned_orders
        .insert(intent.key.clone(), intent.clone());
    let mut readback = shadow_readback()?;
    readback.orders = vec![health_order(&binding, &intent)?];

    assert!(matches!(
        verify_readback_scope(&state, &commands, &readback, &binding),
        Err(Stage7GridError::ForeignOrders)
    ));
    Ok(())
}

#[test]
fn cancel_refuses_a_same_identity_order_with_foreign_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let params = release_params(&cfg, &binding)?;
    let intent = intent(1, GridPosition::Long, 1)?;
    let GridMutation::Place(command) = place_command(&binding, &instrument()?, &intent)? else {
        return Err("grid intent did not create a limit command".into());
    };
    let command_id = command.command_id.clone();
    let mut commands = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;
    commands.prepare_place(command)?;
    commands.transition(&command_id, CommandState::Submitted)?;
    commands.transition(
        &command_id,
        CommandState::Accepted {
            venue_order_id: client_order_id(&intent.key)?.as_str().to_owned(),
        },
    )?;
    let mut state = HedgedGridState::new_with_params(binding.clone(), params)?;
    state.phase = GridPhase::Stopping;
    state
        .owned_orders
        .insert(intent.key.clone(), intent.clone());
    let mut visible = health_order(&binding, &intent)?;
    visible.reduce_only = true;

    let authority = WriterLeaseAuthority::open(
        temporary.path().join(WRITER_FILE),
        WriterScope {
            exchange: binding.exchange.clone(),
            account: binding.account.clone(),
            symbol: binding.symbol.clone(),
            owner_scope: binding.owner_scope.clone(),
        },
    )?;
    let writer = authority.register_initial(1, 1)?;
    let mut venue = ShadowVenue {
        instrument: instrument()?,
        readbacks: VecDeque::new(),
        minimum_quantity: Decimal::ONE,
        stream_polls_before_error: None,
        stream_resets: 0,
        public_payloads: VecDeque::new(),
        accepted_public_at_ms: None,
        public_resets: 0,
        exact_order_outcomes: VecDeque::new(),
    };
    assert!(matches!(
        cancel_visible_owned_orders(
            &mut commands,
            &mut venue,
            &authority,
            &writer,
            &binding,
            &state,
            &[visible],
        ),
        Err(Stage7GridError::ForeignOrders)
    ));
    Ok(())
}

#[test]
fn reduce_only_partial_remaining_quantity_is_the_only_quantity_relaxation()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let params = release_params(&cfg, &binding)?;
    let closing = GridOrderIntent {
        key: GridOrderKey {
            epoch: 1,
            position: GridPosition::Long,
            role: GridOrderRole::Close,
            level: 1,
        },
        side: GridPosition::Long.closing_side(),
        price: Price::new(Decimal::new(11, 0))?,
        quantity: Decimal::new(5, 0),
        reduce_only: true,
    };
    let GridMutation::Place(command) = place_command(&binding, &instrument()?, &closing)? else {
        return Err("grid intent did not create a limit command".into());
    };
    let command_id = command.command_id.clone();
    let mut commands = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;
    commands.prepare_place(command)?;
    commands.transition(&command_id, CommandState::Submitted)?;
    commands.transition(
        &command_id,
        CommandState::Accepted {
            venue_order_id: client_order_id(&closing.key)?.as_str().to_owned(),
        },
    )?;
    let mut state = HedgedGridState::new_with_params(binding.clone(), params)?;
    state.phase = GridPhase::Running;
    state
        .owned_orders
        .insert(closing.key.clone(), closing.clone());
    let mut order = health_order(&binding, &closing)?;
    order.state = OrderState::PartiallyFilled;
    order.quantity = Decimal::new(4, 0);
    order.filled_quantity = Decimal::ONE;
    let mut readback = shadow_readback()?;
    readback.orders = vec![order.clone()];
    verify_readback_scope(&state, &commands, &readback, &binding)?;

    order.state = OrderState::New;
    order.filled_quantity = Decimal::ZERO;
    readback.orders = vec![order];
    assert!(matches!(
        verify_readback_scope(&state, &commands, &readback, &binding),
        Err(Stage7GridError::ForeignOrders)
    ));
    Ok(())
}

#[test]
fn blocked_recovery_rebuilds_only_an_exact_accepted_order_projection()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let intent = intent(1, GridPosition::Long, 1)?;
    let GridMutation::Place(command) = place_command(&binding, &instrument()?, &intent)? else {
        return Err("grid intent did not create a limit command".into());
    };
    let command_id = command.command_id.clone();
    let mut commands = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;
    commands.prepare_place(command)?;
    commands.transition(&command_id, CommandState::Submitted)?;
    commands.transition(
        &command_id,
        CommandState::Accepted {
            venue_order_id: client_order_id(&intent.key)?.as_str().to_owned(),
        },
    )?;

    let mut readback = shadow_readback()?;
    readback.orders = vec![health_order(&binding, &intent)?];
    let recovered = recovered_owned_orders(&commands, &binding, &readback)?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered.get(&intent.key), Some(&intent));

    let mut unknown_client = readback.clone();
    unknown_client.orders[0].client_order_id = FieldState::Known("hgo_e1_long_open_l99".to_owned());
    assert!(matches!(
        recovered_owned_orders(&commands, &binding, &unknown_client),
        Err(Stage7GridError::Unresolved)
    ));
    Ok(())
}

#[test]
fn blocked_or_stopping_recovery_accepts_only_an_exact_wal_owned_retired_target()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let params = release_params(&cfg, &binding)?;
    let visible_intent = intent(1, GridPosition::Long, 1)?;
    let GridMutation::Place(command) = place_command(&binding, &instrument()?, &visible_intent)?
    else {
        return Err("grid intent did not create a limit command".into());
    };
    let command_id = command.command_id.clone();
    let venue_order_id = command.client_order_id.as_str().to_owned();
    let mut commands = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;
    commands.prepare_place(command)?;
    commands.transition(&command_id, CommandState::Submitted)?;
    commands.transition(&command_id, CommandState::Accepted { venue_order_id })?;

    let mut state = HedgedGridState::new_with_params(binding.clone(), params)?;
    state.phase = GridPhase::BlockedUnknown;
    let mut readback = shadow_readback()?;
    readback.orders = vec![health_order(&binding, &visible_intent)?];

    settle_signed_visible_order_receipts(&mut commands, &state, &binding, &readback)?;
    let recovered = recovered_owned_orders(&commands, &binding, &readback)?;
    assert_eq!(recovered.get(&visible_intent.key), Some(&visible_intent));

    state.phase = GridPhase::Stopping;
    state.reconcile_stopping_orders(recovered)?;
    verify_readback_scope(&state, &commands, &readback, &binding)?;

    state.owned_orders.clear();
    readback.orders[0].side = OrderSide::Sell;
    assert!(matches!(
        settle_signed_visible_order_receipts(&mut commands, &state, &binding, &readback),
        Err(Stage7GridError::ForeignOrders)
    ));
    Ok(())
}

#[test]
fn accepted_visible_order_without_a_checkpoint_intent_fails_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let params = release_params(&cfg, &binding)?;
    let visible_intent = intent(1, GridPosition::Long, 1)?;
    let stale_intent = intent(2, GridPosition::Short, 1)?;
    let GridMutation::Place(command) = place_command(&binding, &instrument()?, &visible_intent)?
    else {
        return Err("grid intent did not create a limit command".into());
    };
    let command_id = command.command_id.clone();
    let mut commands = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;
    commands.prepare_place(command)?;
    commands.transition(&command_id, CommandState::Submitted)?;
    commands.transition(
        &command_id,
        CommandState::Accepted {
            venue_order_id: client_order_id(&visible_intent.key)?.as_str().to_owned(),
        },
    )?;

    let mut state = HedgedGridState::new_with_params(binding.clone(), params)?;
    state.phase = GridPhase::Running;
    state
        .owned_orders
        .insert(stale_intent.key.clone(), stale_intent.clone());
    let mut readback = shadow_readback()?;
    readback.orders = vec![health_order(&binding, &visible_intent)?];

    assert!(matches!(
        reconcile_visible_order_drift(&mut state, &commands, &binding, &readback),
        Err(Stage7GridError::ForeignOrders)
    ));
    assert_eq!(state.phase, GridPhase::Running);
    assert_eq!(state.owned_orders.len(), 1);
    assert_eq!(
        state.owned_orders.get(&stale_intent.key),
        Some(&stale_intent)
    );
    Ok(())
}

#[test]
fn health_fence_survives_a_normal_restart_without_preparing_any_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let params = release_params(&cfg, &binding)?;
    ProjectionStore::new(temporary.path().join(CHECKPOINT_FILE)).save(&Stage7GridCheckpoint {
        schema_version: 1,
        binding: binding.clone(),
        state: HedgedGridState::new_with_params(binding.clone(), params)?,
        private_generation: 1,
        exposure_guard: None,
        pending_exposure_reduction: None,
        fill_history_start_ms: 1,
        order_health_fenced: true,
        last_order_health_checked_at_ms: 0,
    })?;
    let mut venue = ShadowVenue {
        instrument: instrument()?,
        readbacks: VecDeque::from([shadow_readback()?]),
        minimum_quantity: Decimal::ONE,
        stream_polls_before_error: None,
        stream_resets: 0,
        public_payloads: VecDeque::new(),
        accepted_public_at_ms: None,
        public_resets: 0,
        exact_order_outcomes: VecDeque::new(),
    };
    let result = run_stage7_grid(
        &cfg,
        Stage7GridRequest {
            artifacts_root: temporary.path().to_path_buf(),
            max_turns: Some(1),
            reset_on_start: false,
            skip_inventory_replenishment_until_recovered: false,
            confirm_mainnet_grid_mutations: true,
            shadow_only: false,
            stop_after_first_owned_fill: false,
            wall_clock_deadline_ms: None,
            force_order_health_check: false,
        },
        binding,
        &mut venue,
    );
    assert!(matches!(result, Err(Stage7GridError::OrderHealthFenced)));
    assert!(!temporary.path().join(COMMAND_FILE).exists());
    Ok(())
}

#[test]
fn blocked_grid_waits_for_its_deferred_signed_reconciliation_without_inventory_transition()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let params = release_params(&cfg, &binding)?;
    let mut state = HedgedGridState::new_with_params(binding.clone(), params)?;
    let inventory = GridInventory {
        private_generation: 1,
        private_observed_at_ms: 1,
        mark_price: Price::new(Decimal::new(100, 0))?,
        long_quantity: Decimal::new(20, 2),
        short_quantity: Decimal::new(20, 2),
    };
    let _ = state.observe_inventory(inventory)?;
    let _ = state.install_epoch(GridEpoch {
        epoch: 1,
        anchor_price: Price::new(Decimal::new(100, 0))?,
        step: Price::new(Decimal::new(2, 1))?,
        grid_quantity: Decimal::new(5, 2),
        passive_book_fallback: None,
    })?;
    state.block_for_order_reconciliation()?;
    let not_before_ms = wall_clock_ms()?.saturating_add(60_000);
    state.defer_blocked_reconciliation_until(not_before_ms)?;
    let readback = shadow_readback()?;
    ProjectionStore::new(temporary.path().join(CHECKPOINT_FILE)).save(&Stage7GridCheckpoint {
        schema_version: 1,
        binding: binding.clone(),
        state,
        private_generation: 1,
        exposure_guard: None,
        pending_exposure_reduction: None,
        fill_history_start_ms: 1,
        order_health_fenced: false,
        last_order_health_checked_at_ms: 0,
    })?;
    set_stage7_grid_control(&cfg, temporary.path(), HedgedGridControlTarget::Running)?;
    let mut venue = ShadowVenue {
        instrument: instrument()?,
        readbacks: VecDeque::from([readback]),
        minimum_quantity: Decimal::ONE,
        stream_polls_before_error: None,
        stream_resets: 0,
        public_payloads: VecDeque::new(),
        accepted_public_at_ms: None,
        public_resets: 0,
        exact_order_outcomes: VecDeque::new(),
    };
    let report = run_stage7_grid(
        &cfg,
        Stage7GridRequest {
            artifacts_root: temporary.path().to_path_buf(),
            max_turns: Some(1),
            reset_on_start: false,
            skip_inventory_replenishment_until_recovered: false,
            confirm_mainnet_grid_mutations: true,
            shadow_only: false,
            stop_after_first_owned_fill: false,
            wall_clock_deadline_ms: None,
            force_order_health_check: false,
        },
        binding,
        &mut venue,
    )?;
    assert_eq!(report.phase, GridPhase::BlockedUnknown);
    let restored = ProjectionStore::new(temporary.path().join(CHECKPOINT_FILE))
        .load::<Stage7GridCheckpoint>()?
        .ok_or("missing checkpoint")?;
    assert_eq!(
        restored.state.blocked_reconciliation_not_before_ms(),
        Some(not_before_ms)
    );
    assert!(!temporary.path().join(COMMAND_FILE).exists());
    Ok(())
}

#[test]
fn rejected_rolling_batch_preserves_its_deferred_reconciliation_fence()
-> Result<(), Box<dyn std::error::Error>> {
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let params = release_params(&cfg, &binding)?;
    let mut state = HedgedGridState::new_with_params(binding, params)?;
    let _ = state.observe_inventory(GridInventory {
        private_generation: 1,
        private_observed_at_ms: 1,
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
    state.block_for_order_reconciliation()?;
    state.defer_blocked_reconciliation_until(30_100)?;

    request_reconciliation_reset_unless_batch_is_blocked(&mut state)?;

    assert_eq!(state.phase, GridPhase::BlockedUnknown);
    assert_eq!(state.blocked_reconciliation_not_before_ms(), Some(30_100));
    Ok(())
}

#[test]
fn stage7_control_refuses_to_rebind_an_existing_artifacts_root()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let gate = config(3)?;
    set_stage7_grid_control(&gate, temporary.path(), HedgedGridControlTarget::Stop)?;
    let bitget = Config {
        log: LogLevel::Info,
        trading_account_id: "00000000-0000-4000-8000-000000000002".to_owned(),
        symbol: "DOGE/USDT".parse()?,
        binance: None,
        gate: None,
        bitget: Some(BitgetConfig {
            account_binding: BitgetAccountBinding::UtaUsdtFuturesHedge,
            private_custody_max_stale_ms: 5_000,
        }),
        hedged_grid: Some(HedgedGridConfig {
            grid_count: 3,
            exposure_take_profit: None,
        }),
    };
    assert!(matches!(
        set_stage7_grid_control(&bitget, temporary.path(), HedgedGridControlTarget::Stop),
        Err(Stage7GridError::Control)
    ));
    Ok(())
}

#[test]
fn incomplete_bitget_exact_order_fact_requires_a_full_signed_fill()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(incomplete_exact_order_query(&GridVenueError::Bitget(
        crate::exchange::bitget::BitgetError::Payload,
    )));
    assert!(incomplete_exact_order_query(&GridVenueError::Bitget(
        crate::exchange::bitget::BitgetError::Readback("order-info"),
    )));
    assert!(incomplete_exact_order_query(&GridVenueError::Bitget(
        crate::exchange::bitget::BitgetError::RejectedHttp {
            status: 400,
            code: "40725".to_owned(),
            message: "post only would cross".to_owned(),
        },
    )));
    assert!(!incomplete_exact_order_query(&GridVenueError::Bitget(
        crate::exchange::bitget::BitgetError::OrderAbsent
    ),));
    assert!(is_order_absent(&GridVenueError::Bitget(
        crate::exchange::bitget::BitgetError::OrderAbsent
    )));
    let complete = GridVenueFill {
        fill: Fill {
            execution_sequence: FieldState::Known(1),
            fill_id: "fill-1".to_owned(),
            order_id: "order-1".to_owned(),
            symbol: "DOGE/USDT".parse()?,
            side: OrderSide::Buy,
            position_side: FieldState::Known(PositionSide::Long),
            quantity: Decimal::new(54, 0),
            price: Price::new(Decimal::new(9_315, 5))?,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(true),
            exchange_time_ms: Some(1),
        },
        client_order_id: FieldState::Known("hgo_e1_long_open_l1".to_owned()),
    };
    assert!(signed_full_owned_fill(&complete, Decimal::new(54, 0)));
    assert!(!signed_full_owned_fill(&complete, Decimal::new(55, 0)));

    let mut split = complete.clone();
    split.fill.fill_id = "fill-2".to_owned();
    split.fill.quantity = Decimal::new(26, 0);
    let mut first = complete.clone();
    first.fill.quantity = Decimal::new(29, 0);
    let quantities = signed_owned_fill_quantities(&[first.clone(), split, first]);
    assert_eq!(
        quantities.get(&parse_grid_client_order_id("hgo_e1_long_open_l1")?),
        Some(&Decimal::new(55, 0))
    );
    Ok(())
}

#[test]
fn complete_signed_fill_needs_no_exact_order_rest_and_is_not_starved()
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
    let long = state
        .owned_orders
        .values()
        .find(|order| {
            order.key.position == GridPosition::Long
                && order.key.role == GridOrderRole::Open
                && order.key.level == 1
        })
        .cloned()
        .ok_or("missing long opening intent")?;
    let long_two = state
        .owned_orders
        .values()
        .find(|order| {
            order.key.position == GridPosition::Long
                && order.key.role == GridOrderRole::Open
                && order.key.level == 2
        })
        .cloned()
        .ok_or("missing second long opening intent")?;
    let fill = |id: &str, intent: &GridOrderIntent| GridVenueFill {
        fill: Fill {
            execution_sequence: FieldState::Known(1),
            fill_id: id.to_owned(),
            order_id: client_order_id(&intent.key)
                .map(|value| value.as_str().to_owned())
                .unwrap_or_default(),
            symbol: binding.symbol.clone(),
            side: intent.side,
            position_side: FieldState::Known(match intent.key.position {
                GridPosition::Long => PositionSide::Long,
                GridPosition::Short => PositionSide::Short,
            }),
            quantity: intent.quantity,
            price: intent.price,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(true),
            exchange_time_ms: Some(1),
        },
        client_order_id: FieldState::Known(
            client_order_id(&intent.key)
                .map(|value| value.as_str().to_owned())
                .unwrap_or_default(),
        ),
    };
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
    readback.fills = vec![fill("long-fill", &long), fill("long-fill-2", &long_two)];
    readback.fills[0].fill.quantity = Decimal::new(4, 2);
    readback.fills[1].fill.execution_sequence = FieldState::Known(2);
    let calls = Arc::new(Mutex::new(Vec::new()));
    let readback_calls = Arc::new(AtomicUsize::new(0));
    let mut venue = StreamFillVenue {
        instrument: instrument()?,
        client: RecordingMutationClient {
            calls: Arc::clone(&calls),
        },
        readback_calls,
        book_reads: Arc::new(AtomicUsize::new(0)),
        readbacks: VecDeque::new(),
        private_events: VecDeque::new(),
        private_empty_polls: 0,
        risk_client: None,
        exact_order_outcomes: VecDeque::new(),
        book: (
            Price::new(Decimal::new(9_999, 2))?,
            Price::new(Decimal::new(10_001, 2))?,
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
    let mut commands = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;
    for owned in checkpoint.state.owned_orders.values() {
        let GridMutation::Place(original) = place_command(&binding, &instrument()?, owned)? else {
            return Err("owned intent did not create a place command".into());
        };
        let command_id = original.command_id.clone();
        let venue_order_id = original.client_order_id.as_str().to_owned();
        commands.prepare_place(original)?;
        commands.transition(&command_id, CommandState::Submitted)?;
        commands.transition(&command_id, CommandState::Accepted { venue_order_id })?;
    }
    let original_command_ids = commands
        .commands()
        .map(|command| command.command_id().clone())
        .collect::<Vec<_>>();
    let store = ProjectionStore::new(temporary.path().join(CHECKPOINT_FILE));

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
        FillDriveOutcome::dispatched()
    );
    assert!(checkpoint.state.owned_orders.contains_key(&long.key));
    assert!(!checkpoint.state.owned_orders.contains_key(&long_two.key));
    assert!(checkpoint.state.pending_transactions.is_empty());
    let calls = calls.lock().map_err(|_| "mutation calls poisoned")?;
    assert_eq!(calls.len(), 3);
    assert_eq!(calls.iter().filter(|call| **call == "place").count(), 2);
    assert_eq!(calls.iter().filter(|call| **call == "cancel").count(), 1);
    let dispatched = commands
        .commands()
        .filter(|command| !original_command_ids.contains(command.command_id()))
        .collect::<Vec<_>>();
    assert_eq!(dispatched.len(), 3);
    assert_eq!(
        dispatched
            .iter()
            .filter(|command| matches!(command, ExecutionCommand::PlaceLimit(_)))
            .count(),
        2
    );
    assert_eq!(
        dispatched
            .iter()
            .filter(|command| matches!(command, ExecutionCommand::Cancel(_)))
            .count(),
        1
    );
    assert!(dispatched.iter().all(|command| {
        commands.receipt(command.command_id()).is_some_and(|receipt| {
            matches!(receipt.state, CommandState::Accepted { .. })
        })
    }));
    assert!(!commands.has_unresolved());
    assert!(store.load::<Stage7GridCheckpoint>()?.is_some());
    Ok(())
}

#[test]
fn isolated_partial_fill_waits_for_stream_or_periodic_readback_without_busy_looping()
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
        .next()
        .cloned()
        .ok_or("missing partial-fill source")?;
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
            fill_id: "partial-only".to_owned(),
            order_id: client_order_id(&source.key)?.as_str().to_owned(),
            symbol: binding.symbol.clone(),
            side: source.side,
            position_side: FieldState::Known(match source.key.position {
                GridPosition::Long => PositionSide::Long,
                GridPosition::Short => PositionSide::Short,
            }),
            quantity: source.quantity / Decimal::new(2, 0),
            price: source.price,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(true),
            exchange_time_ms: Some(1),
        },
        client_order_id: FieldState::Known(client_order_id(&source.key)?.as_str().to_owned()),
    });
    let mut venue = ShadowVenue {
        instrument: instrument()?,
        readbacks: VecDeque::new(),
        minimum_quantity: Decimal::ONE,
        stream_polls_before_error: None,
        stream_resets: 0,
        public_payloads: VecDeque::new(),
        accepted_public_at_ms: None,
        public_resets: 0,
        exact_order_outcomes: VecDeque::new(),
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
        FillDriveOutcome::idle()
    );
    assert!(checkpoint.state.owned_orders.contains_key(&source.key));
    Ok(())
}

#[path = "stage7_grid_reconciliation_tests.rs"]
mod reconciliation_tests;

fn health_order(
    binding: &HedgedGridBinding,
    intent: &GridOrderIntent,
) -> Result<Order, Box<dyn std::error::Error>> {
    let GridMutation::Place(command) = place_command(binding, &instrument()?, intent)? else {
        return Err("grid intent did not create a limit command".into());
    };
    Ok(Order {
        order_id: command.client_order_id.as_str().to_owned(),
        client_order_id: FieldState::Known(command.client_order_id.as_str().to_owned()),
        symbol: binding.symbol.clone(),
        side: intent.side,
        position_side: FieldState::Known(match intent.key.position {
            GridPosition::Long => PositionSide::Long,
            GridPosition::Short => PositionSide::Short,
        }),
        purpose: FieldState::Known(OrderPurpose::Entry),
        state: OrderState::New,
        quantity: intent.quantity,
        filled_quantity: Decimal::ZERO,
        limit_price: Some(intent.price),
        average_price: FieldState::Missing,
        reduce_only: intent.reduce_only,
    })
}
