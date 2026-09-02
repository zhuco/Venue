use rust_decimal::Decimal;
use venue_domain::domain::Price;
use venue_domain::domain::{Asset, Symbol};
use venue_strategies::hedged_grid::{
    GridEpoch, GridInventory, HedgedGridBinding, HedgedGridParams, HedgedGridState,
};

use super::*;

#[test]
fn bootstrap_quantity_covers_the_lowest_opening_level() -> Result<(), NodeError> {
    let anchor = Decimal::new(10_199, 2);
    let step = Decimal::new(20, 2);
    let quantity = minimum_grid_quantity(Decimal::new(5, 0), anchor, step, 10, Decimal::new(1, 2))?;
    let lowest_open = anchor - step * Decimal::from(10_u8);
    assert_eq!(quantity, Decimal::new(6, 2));
    assert!(quantity * lowest_open >= Decimal::new(5, 0));
    assert!(quantity * (anchor + step * Decimal::from(10_u8)) < Decimal::new(10, 0));
    Ok(())
}

#[test]
fn execution_profile_rounds_only_when_minimum_notional_requires_another_step()
-> Result<(), Box<dyn std::error::Error>> {
    let profile = GridExecutionProfile::new(
        Decimal::new(1, 2),
        Decimal::new(1, 2),
        Decimal::new(1_000, 0),
        Decimal::new(5, 0),
    )?;

    assert_eq!(
        profile.normalize_quantity(Decimal::new(5, 2), Decimal::new(9_919, 2))?,
        Decimal::new(6, 2)
    );
    assert_eq!(
        profile.normalize_quantity(Decimal::new(5, 2), Decimal::new(101, 0))?,
        Decimal::new(5, 2)
    );
    assert!(
        profile
            .normalize_quantity(Decimal::new(4, 2), Decimal::new(9_919, 2))
            .is_err()
    );
    Ok(())
}

fn initial() -> Result<HedgedGridState, Box<dyn std::error::Error>> {
    let binding = HedgedGridBinding {
        strategy_instance_id: "grid_doge".to_owned(),
        run_id: "run_a".to_owned(),
        exchange: "binance".to_owned(),
        account: "account_a".to_owned(),
        symbol: "DOGE/USDT".parse::<Symbol>()?,
        config_version: "abc123".to_owned(),
        owner_scope: "grid_doge".to_owned(),
    };
    Ok(HedgedGridState::new_with_params(
        binding,
        HedgedGridParams::fixed_release(Asset::new("USDT")?, 10)?,
    )?)
}

#[test]
fn recovery_requires_verified_checkpoint_or_explicit_first_bootstrap()
-> Result<(), Box<dyn std::error::Error>> {
    let initial = initial()?;
    assert!(
        GridBridgeState::restore_or_bootstrap(
            None,
            initial.clone(),
            NodeGridRecoveryPolicy::RequireExisting,
        )
        .is_err()
    );
    let bridge = GridBridgeState::restore_or_bootstrap(
        None,
        initial.clone(),
        NodeGridRecoveryPolicy::BootstrapWhenAbsent,
    )?;
    let restored = GridBridgeState::restore_or_bootstrap(
        Some(bridge.checkpoint_bytes()?),
        initial,
        NodeGridRecoveryPolicy::RequireExisting,
    )?;
    assert_eq!(restored.grid.binding.symbol, "DOGE/USDT".parse()?);
    Ok(())
}

#[test]
fn uninstalled_actor_checkpoint_rearms_only_before_an_epoch_is_planned()
-> Result<(), Box<dyn std::error::Error>> {
    let initial = initial()?;
    let bridge = GridBridgeState::bootstrap(initial.clone())?;
    let mut legacy_uninstalled: serde_json::Value =
        serde_json::from_slice(&bridge.checkpoint_bytes()?)?;
    legacy_uninstalled
        .as_object_mut()
        .ok_or("grid checkpoint object")?
        .remove("bootstrap_state");
    let restored = GridBridgeState::restore_or_bootstrap(
        Some(serde_json::to_vec(&legacy_uninstalled)?),
        initial.clone(),
        NodeGridRecoveryPolicy::BootstrapWhenAbsent,
    )?;
    assert!(restored.needs_initial_bootstrap());

    let mut planned = GridBridgeState::bootstrap(initial.clone())?;
    planned.mark_bootstrap_attempted()?;
    let plan = planned.install_initial_epoch(
        GridInventory {
            private_generation: 2,
            private_observed_at_ms: 10,
            mark_price: Price::new(Decimal::new(100, 0))?,
            long_quantity: Decimal::ONE,
            short_quantity: Decimal::ONE,
        },
        GridEpoch {
            epoch: 1,
            anchor_price: Price::new(Decimal::new(100, 0))?,
            step: Price::new(Decimal::ONE)?,
            grid_quantity: Decimal::new(5, 2),
            passive_book_fallback: None,
        },
    )?;
    assert!(!planned.needs_initial_bootstrap());
    assert!(planned.bootstrap_requires_reconciliation());
    let mut legacy_planned: serde_json::Value =
        serde_json::from_slice(&planned.checkpoint_bytes()?)?;
    legacy_planned
        .as_object_mut()
        .ok_or("grid checkpoint object")?
        .remove("bootstrap_state");
    let legacy_planned = GridBridgeState::restore_or_bootstrap(
        Some(serde_json::to_vec(&legacy_planned)?),
        initial.clone(),
        NodeGridRecoveryPolicy::BootstrapWhenAbsent,
    )?;
    assert!(!legacy_planned.needs_initial_bootstrap());
    assert!(legacy_planned.bootstrap_requires_reconciliation());
    let accepted = plan
        .accepted_routes
        .iter()
        .enumerate()
        .map(|(index, (_, _, command_id))| {
            (command_id.clone(), format!("confirmed-native-{index}"))
        })
        .collect::<Vec<_>>();
    planned.bind_accepted_plan(&plan, &accepted)?;
    planned.mark_bootstrap_confirmed()?;
    let confirmed = GridBridgeState::restore_or_bootstrap(
        Some(planned.checkpoint_bytes()?),
        initial,
        NodeGridRecoveryPolicy::BootstrapWhenAbsent,
    )?;
    assert!(!confirmed.needs_initial_bootstrap());
    assert!(!confirmed.bootstrap_requires_reconciliation());
    Ok(())
}

#[test]
fn signed_grid_surface_is_bijective_and_owner_purpose_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let mut bridge = GridBridgeState::bootstrap(initial()?)?;
    let _chosen = bridge.install_test_accepted_open_route("chosen-native")?;
    let mut orders = bridge
        .grid
        .owned_orders
        .iter()
        .map(|(key, desired)| {
            let route = bridge.routes.get(key).ok_or("route")?;
            Ok::<_, Box<dyn std::error::Error>>(SignedAccountOrderFact {
                client_order_id: route.client_order_id.as_str().to_owned(),
                venue_order_id: route.accepted_venue_order_id.clone(),
                symbol: bridge.grid.binding.symbol.clone(),
                family: venue_domain::NativeOrderFamily::UmOrder,
                side: desired.side,
                position_side: match desired.key.position {
                    GridPosition::Long => PositionSide::Long,
                    GridPosition::Short => PositionSide::Short,
                },
                quantity: desired.quantity,
                limit_price: Some(desired.price.value()),
                time_in_force: Some(venue_domain::LimitTimeInForce::PostOnly),
                created_at_ms: Some(1),
                reduce_only: desired.reduce_only,
                owner: Some(owner_for_order(&bridge.grid, desired)),
                external: false,
                state: Some(OrderState::New),
                filled_quantity: Some(Decimal::ZERO),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    assert!(bridge.signed_desired_matches(&orders));

    let mut wrong_purpose = orders.clone();
    wrong_purpose[0].owner.as_mut().ok_or("owner")?.purpose = OrderPurpose::Protection;
    assert!(!bridge.signed_desired_matches(&wrong_purpose));

    let extra = orders[0].clone();
    orders.push(extra);
    assert!(!bridge.signed_desired_matches(&orders));
    Ok(())
}

#[test]
fn route_checkpoint_accepts_only_the_legacy_empty_object_shape()
-> Result<(), Box<dyn std::error::Error>> {
    let initial = initial()?;
    let bridge = GridBridgeState::bootstrap(initial.clone())?;
    let mut legacy: serde_json::Value = serde_json::from_slice(&bridge.checkpoint_bytes()?)?;
    legacy
        .as_object_mut()
        .ok_or("grid checkpoint object")?
        .insert("routes".to_owned(), serde_json::json!({}));
    let empty_legacy = serde_json::to_vec(&legacy)?;
    assert!(
        GridBridgeState::restore_or_bootstrap(
            Some(empty_legacy),
            initial.clone(),
            NodeGridRecoveryPolicy::RequireExisting,
        )
        .is_ok()
    );
    legacy
        .as_object_mut()
        .ok_or("grid checkpoint object")?
        .insert("routes".to_owned(), serde_json::json!({"old": {}}));
    let nonempty_legacy = serde_json::to_vec(&legacy)?;
    assert!(
        GridBridgeState::restore_or_bootstrap(
            Some(nonempty_legacy),
            initial,
            NodeGridRecoveryPolicy::RequireExisting,
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn initial_install_preserves_closing_before_opening_and_refuses_low_inventory()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = initial()?;
    state.params.grid_count = 1;
    let mut bridge = GridBridgeState::bootstrap(state)?;
    bridge.mark_bootstrap_attempted()?;
    let inventory = GridInventory {
        private_generation: 2,
        private_observed_at_ms: 10,
        mark_price: Price::new(Decimal::new(100, 0))?,
        long_quantity: Decimal::ONE,
        short_quantity: Decimal::ONE,
    };
    let epoch = GridEpoch {
        epoch: 1,
        anchor_price: Price::new(Decimal::new(100, 0))?,
        step: Price::new(Decimal::ONE)?,
        grid_quantity: Decimal::new(5, 2),
        passive_book_fallback: None,
    };
    let plan = bridge.install_initial_epoch(inventory.clone(), epoch.clone())?;
    assert!(plan.commands.len() >= 4);
    let reduces =
        plan.commands.iter().take(2).all(
            |command| matches!(command, ExecutionCommand::PlaceLimit(order) if order.reduce_only),
        );
    assert!(reduces);
    let mut low = GridBridgeState::bootstrap(initial()?)?;
    low.mark_bootstrap_attempted()?;
    low.grid.params.grid_count = 1;
    let low_inventory = GridInventory {
        long_quantity: Decimal::ZERO,
        ..inventory
    };
    assert!(low.install_initial_epoch(low_inventory, epoch).is_err());
    Ok(())
}

fn bridge_with_accepted_order()
-> Result<(GridBridgeState, GridOrderKey, GridOrderIntent, String), Box<dyn std::error::Error>> {
    let mut state = initial()?;
    state.params.grid_count = 2;
    let mut bridge = GridBridgeState::bootstrap(state)?;
    bridge.set_execution_profile(GridExecutionProfile::new(
        Decimal::new(1, 2),
        Decimal::new(1, 2),
        Decimal::new(1_000, 0),
        Decimal::new(5, 0),
    )?)?;
    bridge.mark_bootstrap_attempted()?;
    let plan = bridge.install_initial_epoch(
        GridInventory {
            private_generation: 2,
            private_observed_at_ms: 10,
            mark_price: Price::new(Decimal::new(100, 0))?,
            long_quantity: Decimal::ONE,
            short_quantity: Decimal::ONE,
        },
        GridEpoch {
            epoch: 1,
            anchor_price: Price::new(Decimal::new(100, 0))?,
            step: Price::new(Decimal::ONE)?,
            grid_quantity: Decimal::new(5, 2),
            passive_book_fallback: None,
        },
    )?;
    let accepted = plan
        .accepted_routes
        .iter()
        .enumerate()
        .map(|(index, (_, _, command_id))| {
            (command_id.clone(), format!("native-grid-order-{index}"))
        })
        .collect::<Vec<_>>();
    bridge.bind_accepted_plan(&plan, &accepted)?;
    let (key, route) = bridge
        .routes
        .iter()
        .next()
        .map(|(key, route)| (key.clone(), route.clone()))
        .ok_or("accepted route")?;
    let source = bridge.require_owned(&key)?.clone();
    let native_order_id = route.accepted_venue_order_id.ok_or("native order id")?;
    Ok((bridge, key, source, native_order_id))
}

#[test]
fn legacy_running_checkpoint_without_execution_profile_cannot_plan_a_roll()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut bridge, _key, source, native_order_id) = bridge_with_accepted_order()?;
    bridge.mark_bootstrap_confirmed()?;
    let mut legacy: serde_json::Value = serde_json::from_slice(&bridge.checkpoint_bytes()?)?;
    legacy
        .as_object_mut()
        .ok_or("grid checkpoint object")?
        .remove("execution_profile");
    let mut expected = initial()?;
    expected.params.grid_count = 2;
    let mut restored = GridBridgeState::restore_or_bootstrap(
        Some(serde_json::to_vec(&legacy)?),
        expected,
        NodeGridRecoveryPolicy::BootstrapWhenAbsent,
    )?;
    let fill = owned_fill(
        "legacy-profile-fill",
        &native_order_id,
        &source,
        source.quantity,
        source.price,
    )?;
    let GridDecision::Actions(actions) = restored.observe_persisted_fill(&fill, 9)? else {
        return Err("legacy fill did not reserve a transaction".into());
    };
    assert!(matches!(
        restored.plan_dispatch(&actions[0]),
        Err(GridBridgeError::ExecutionProfile)
    ));
    Ok(())
}

fn owned_fill(
    fill_id: &str,
    order_id: &str,
    source: &GridOrderIntent,
    quantity: Decimal,
    price: Price,
) -> Result<Fill, Box<dyn std::error::Error>> {
    Ok(Fill {
        fill_id: fill_id.to_owned(),
        execution_sequence: FieldState::Known(1),
        order_id: order_id.to_owned(),
        symbol: "DOGE/USDT".parse()?,
        side: source.side,
        position_side: FieldState::Known(match source.key.position {
            GridPosition::Long => PositionSide::Long,
            GridPosition::Short => PositionSide::Short,
        }),
        quantity,
        price,
        fee: FieldState::Missing,
        realized_pnl: FieldState::Missing,
        maker: FieldState::Known(true),
        exchange_time_ms: Some(100),
    })
}

fn signed_orders_for(
    bridge: &GridBridgeState,
    expected: &BTreeMap<GridOrderKey, GridOrderIntent>,
) -> Result<Vec<SignedAccountOrderFact>, Box<dyn std::error::Error>> {
    expected
        .iter()
        .map(|(key, desired)| {
            let route = bridge.routes.get(key).ok_or("route")?;
            Ok(SignedAccountOrderFact {
                client_order_id: route.client_order_id.as_str().to_owned(),
                venue_order_id: route.accepted_venue_order_id.clone(),
                symbol: bridge.grid.binding.symbol.clone(),
                family: venue_domain::NativeOrderFamily::UmOrder,
                side: desired.side,
                position_side: match desired.key.position {
                    GridPosition::Long => PositionSide::Long,
                    GridPosition::Short => PositionSide::Short,
                },
                quantity: desired.quantity,
                limit_price: Some(desired.price.value()),
                time_in_force: Some(venue_domain::LimitTimeInForce::PostOnly),
                created_at_ms: Some(1),
                reduce_only: desired.reduce_only,
                owner: Some(owner_for_order(&bridge.grid, desired)),
                external: false,
                state: Some(OrderState::New),
                filled_quantity: Some(Decimal::ZERO),
            })
        })
        .collect()
}

#[test]
fn pending_pre_dispatch_surface_is_checkpoint_derived_and_shape_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut bridge, _key, source, native_order_id) = bridge_with_accepted_order()?;
    let fill = owned_fill(
        "resume-surface-fill",
        &native_order_id,
        &source,
        source.quantity,
        source.price,
    )?;
    let GridDecision::Actions(actions) = bridge.observe_persisted_fill(&fill, 9)? else {
        return Err("complete fill did not reserve a transaction".into());
    };
    let plan = bridge.plan_dispatch(&actions[0])?;
    let before_dispatch = bridge.pending_pre_dispatch_orders()?;
    let signed = signed_orders_for(&bridge, &before_dispatch)?;
    assert!(bridge.signed_pending_surface_matches(&signed));
    assert_eq!(
        bridge.expected_pending_signed_surface()?.len(),
        signed.len()
    );

    let mut wrong_native = signed.clone();
    wrong_native[0].venue_order_id = Some("wrong-native".to_owned());
    assert!(!bridge.signed_pending_surface_matches(&wrong_native));
    let mut wrong_shape = signed.clone();
    wrong_shape[0].quantity += Decimal::new(1, 2);
    assert!(!bridge.signed_pending_surface_matches(&wrong_shape));
    let mut wrong_filled_quantity = signed.clone();
    wrong_filled_quantity[0].filled_quantity = Some(Decimal::new(1, 2));
    assert!(!bridge.signed_pending_surface_matches(&wrong_filled_quantity));
    let mut missing = signed.clone();
    missing.pop();
    assert!(!bridge.signed_pending_surface_matches(&missing));
    let mut extra = signed.clone();
    extra.push(signed[0].clone());
    assert!(!bridge.signed_pending_surface_matches(&extra));

    let accepted = plan
        .accepted_routes
        .iter()
        .enumerate()
        .map(|(index, (_, _, command_id))| (command_id.clone(), format!("resume-native-{index}")))
        .collect::<Vec<_>>();
    bridge.bind_accepted_plan(&plan, &accepted)?;
    assert!(!bridge.signed_pending_surface_matches(&signed));
    Ok(())
}

#[test]
fn stopped_writer_extends_pending_surface_with_later_signed_fills()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut bridge, _first_key, first_source, first_native) = bridge_with_accepted_order()?;
    let accepted_before = bridge
        .routes
        .values()
        .filter(|route| route.accepted_venue_order_id.is_some())
        .count();
    let first = owned_fill(
        "stopped-writer-fill-1",
        &first_native,
        &first_source,
        first_source.quantity,
        first_source.price,
    )?;
    let GridDecision::Actions(first_actions) = bridge.observe_persisted_fill(&first, 9)? else {
        return Err("first stopped-writer fill did not reserve a transaction".into());
    };
    let _first_plan = bridge.plan_dispatch(&first_actions[0])?;
    let (cancel_source, cancel_native) = bridge
        .grid
        .pending_transactions
        .values()
        .next()
        .and_then(|transaction| {
            transaction.cancelled_order.as_ref().and_then(|source| {
                bridge
                    .routes
                    .get(&transaction.cancel)
                    .and_then(|route| route.accepted_venue_order_id.clone())
                    .map(|native| (source.clone(), native))
            })
        })
        .ok_or("pending cancellation route")?;
    let filled_cancel_target = owned_fill(
        "stopped-writer-cancel-target-fill",
        &cancel_native,
        &cancel_source,
        cancel_source.quantity,
        cancel_source.price,
    )?;
    assert!(
        bridge
            .signed_fill_application(&filled_cancel_target)
            .is_err()
    );

    let (second_source, second_native) = bridge
        .routes
        .iter()
        .find_map(|(key, route)| {
            route
                .accepted_venue_order_id
                .as_ref()
                .filter(|_| bridge.grid.owned_orders.contains_key(key))
                .and_then(|native| {
                    bridge
                        .grid
                        .owned_orders
                        .get(key)
                        .map(|source| (source.clone(), native.clone()))
                })
        })
        .ok_or("second accepted route")?;
    let second = owned_fill(
        "stopped-writer-fill-2",
        &second_native,
        &second_source,
        second_source.quantity,
        second_source.price,
    )?;
    let GridDecision::Actions(second_actions) = bridge.observe_persisted_fill(&second, 9)? else {
        return Err("second stopped-writer fill did not reserve a transaction".into());
    };
    let _second_plan = bridge.plan_dispatch(&second_actions[0])?;

    assert_eq!(bridge.pending_dispatch_plans()?.len(), 2);
    let before_dispatch = bridge.pending_pre_dispatch_orders()?;
    assert_eq!(before_dispatch.len(), accepted_before - 2);
    let signed = signed_orders_for(&bridge, &before_dispatch)?;
    assert!(bridge.signed_pending_surface_matches(&signed));
    Ok(())
}

#[test]
fn startup_reconciliation_is_durable_uses_fresh_attempt_ids_and_rebuilds_next_epoch()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut bridge, _filled_key, source, native_order_id) = bridge_with_accepted_order()?;
    bridge.mark_bootstrap_confirmed()?;
    let fill = owned_fill(
        "startup-reset-pending-fill",
        &native_order_id,
        &source,
        source.quantity,
        source.price,
    )?;
    let GridDecision::Actions(actions) = bridge.observe_persisted_fill(&fill, 9)? else {
        return Err("maker fill did not reserve a rolling transaction".into());
    };
    let _pending_plan = bridge.plan_dispatch(&actions[0])?;
    let signed_before_dispatch =
        signed_orders_for(&bridge, &bridge.pending_pre_dispatch_orders()?)?;
    let (pending_cancel_key, pending_cancel_source, pending_cancel_native) = bridge
        .grid
        .pending_transactions
        .values()
        .next()
        .and_then(|transaction| {
            transaction.cancelled_order.as_ref().and_then(|source| {
                bridge.routes.get(&transaction.cancel).and_then(|route| {
                    route
                        .accepted_venue_order_id
                        .as_ref()
                        .map(|native| (transaction.cancel.clone(), source.clone(), native.clone()))
                })
            })
        })
        .ok_or("pending cancellation target")?;
    let transaction_ids = bridge
        .pending_transaction_command_ids()?
        .into_iter()
        .map(|(transaction_id, _)| transaction_id)
        .collect::<Vec<_>>();
    bridge.abandon_pending_for_reconciliation(&transaction_ids)?;
    assert!(bridge.signed_desired_matches(&signed_before_dispatch));
    let pending_cancel_fill = owned_fill(
        "startup-reset-pending-cancel-fill",
        &pending_cancel_native,
        &pending_cancel_source,
        pending_cancel_source.quantity,
        pending_cancel_source.price,
    )?;
    assert_eq!(
        bridge.observe_persisted_fill(&pending_cancel_fill, 10)?,
        GridDecision::Noop
    );
    assert!(!bridge.routes.contains_key(&pending_cancel_key));

    let first_target = bridge.reconciliation_target()?.ok_or("first target")?;
    let first_attempt = bridge.advance_reconciliation_attempt(&first_target)?;
    let first_plan = bridge.reconciliation_cancel_plan(&first_target, first_attempt)?;
    assert!(matches!(
        first_plan.commands.as_slice(),
        [ExecutionCommand::Cancel(_)]
    ));
    let first_id = first_plan.commands[0].command_id().clone();
    let second_attempt = bridge.advance_reconciliation_attempt(&first_target)?;
    let second_plan = bridge.reconciliation_cancel_plan(&first_target, second_attempt)?;
    assert_ne!(first_id, *second_plan.commands[0].command_id());

    let checkpoint = bridge.checkpoint_bytes()?;
    let mut bridge = GridBridgeState::restore_or_bootstrap(
        Some(checkpoint),
        bridge.grid.clone(),
        NodeGridRecoveryPolicy::RequireExisting,
    )?;
    assert_eq!(bridge.reconciliation_attempt(&first_target)?, Some(2));
    assert_eq!(
        bridge
            .reconciliation_cancel_plan(&first_target, 2)?
            .commands,
        second_plan.commands
    );
    assert_eq!(bridge.advance_reconciliation_attempt(&first_target)?, 3);
    assert!(
        bridge
            .advance_reconciliation_attempt(&first_target)
            .is_err()
    );
    assert_eq!(bridge.reconciliation_attempt(&first_target)?, Some(3));
    bridge.settle_reconciliation_cancel(&first_target)?;

    let raced_target = bridge.reconciliation_target()?.ok_or("raced target")?;
    let raced_source = bridge.require_owned(&raced_target)?.clone();
    let raced_native = bridge
        .routes
        .get(&raced_target)
        .and_then(|route| route.accepted_venue_order_id.clone())
        .ok_or("raced native route")?;
    let raced_fill = owned_fill(
        "startup-reset-raced-fill",
        &raced_native,
        &raced_source,
        raced_source.quantity,
        raced_source.price,
    )?;
    assert_eq!(
        bridge.observe_persisted_fill(&raced_fill, 10)?,
        GridDecision::Noop
    );
    assert!(!bridge.routes.contains_key(&raced_target));
    assert!(bridge.grid.pending_transactions.is_empty());

    while let Some(target) = bridge.reconciliation_target()? {
        let attempt = bridge.advance_reconciliation_attempt(&target)?;
        let _plan = bridge.reconciliation_cancel_plan(&target, attempt)?;
        bridge.settle_reconciliation_cancel(&target)?;
    }
    assert!(bridge.needs_reconciliation_rebuild());
    assert_eq!(bridge.next_install_epoch()?, 2);
    let rebuilt = bridge.install_rebuilt_epoch(
        GridInventory {
            private_generation: 20,
            private_observed_at_ms: 20,
            mark_price: Price::new(Decimal::new(101, 0))?,
            long_quantity: Decimal::ONE,
            short_quantity: Decimal::ONE,
        },
        GridEpoch {
            epoch: 2,
            anchor_price: Price::new(Decimal::new(101, 0))?,
            step: Price::new(Decimal::ONE)?,
            grid_quantity: Decimal::new(5, 2),
            passive_book_fallback: None,
        },
    )?;
    let accepted = rebuilt
        .accepted_routes
        .iter()
        .enumerate()
        .map(|(index, (_, _, command_id))| (command_id.clone(), format!("rebuilt-native-{index}")))
        .collect::<Vec<_>>();
    bridge.bind_accepted_plan(&rebuilt, &accepted)?;
    bridge.confirm_installed_surface()?;
    assert!(!bridge.has_startup_reconciliation());
    assert_eq!(bridge.grid.epoch.as_ref().map(|epoch| epoch.epoch), Some(2));
    bridge.checkpoint_bytes()?;
    Ok(())
}

#[test]
fn partial_first_install_enters_cancel_drain_before_a_new_epoch()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = initial()?;
    state.params.grid_count = 2;
    let mut bridge = GridBridgeState::bootstrap(state)?;
    bridge.mark_bootstrap_attempted()?;
    let plan = bridge.install_initial_epoch(
        GridInventory {
            private_generation: 2,
            private_observed_at_ms: 10,
            mark_price: Price::new(Decimal::new(100, 0))?,
            long_quantity: Decimal::ONE,
            short_quantity: Decimal::ONE,
        },
        GridEpoch {
            epoch: 1,
            anchor_price: Price::new(Decimal::new(100, 0))?,
            step: Price::new(Decimal::ONE)?,
            grid_quantity: Decimal::new(5, 2),
            passive_book_fallback: None,
        },
    )?;
    assert_eq!(bridge.unconfirmed_install_plan()?.commands, plan.commands);
    let accepted_command = plan
        .accepted_routes
        .first()
        .map(|(_, _, command_id)| command_id.clone())
        .ok_or("accepted command")?;
    bridge.bind_accepted_install_routes(
        &plan,
        &[(accepted_command, "partial-install-native".to_owned())],
    )?;
    bridge.begin_unconfirmed_install_reconciliation()?;
    assert_eq!(bridge.grid.phase, GridPhase::ResettingGrid);
    assert_eq!(bridge.routes.len(), 1);
    assert_eq!(bridge.grid.owned_orders.len(), 1);
    let target = bridge.reconciliation_target()?.ok_or("partial target")?;
    let attempt = bridge.advance_reconciliation_attempt(&target)?;
    assert!(matches!(
        bridge
            .reconciliation_cancel_plan(&target, attempt)?
            .commands
            .as_slice(),
        [ExecutionCommand::Cancel(_)]
    ));
    bridge.settle_reconciliation_cancel(&target)?;
    assert!(bridge.needs_reconciliation_rebuild());
    assert_eq!(bridge.next_install_epoch()?, 2);

    let rebuilt = bridge.install_rebuilt_epoch(
        GridInventory {
            private_generation: 3,
            private_observed_at_ms: 20,
            mark_price: Price::new(Decimal::new(101, 0))?,
            long_quantity: Decimal::ONE,
            short_quantity: Decimal::ONE,
        },
        GridEpoch {
            epoch: 2,
            anchor_price: Price::new(Decimal::new(101, 0))?,
            step: Price::new(Decimal::ONE)?,
            grid_quantity: Decimal::new(5, 2),
            passive_book_fallback: None,
        },
    )?;
    let accepted = rebuilt
        .accepted_routes
        .iter()
        .enumerate()
        .map(|(index, (_, _, command_id))| {
            (
                command_id.clone(),
                format!("partial-rebuild-native-{index}"),
            )
        })
        .collect::<Vec<_>>();
    bridge.bind_accepted_plan(&rebuilt, &accepted)?;
    bridge.confirm_installed_surface()?;
    assert!(!bridge.has_startup_reconciliation());

    let (filled_key, route) = bridge.routes.iter().next().ok_or("rebuilt route")?;
    let source = bridge
        .grid
        .owned_orders
        .get(filled_key)
        .cloned()
        .ok_or("rebuilt order")?;
    let native = route
        .accepted_venue_order_id
        .clone()
        .ok_or("rebuilt native")?;
    let fill = owned_fill(
        "partial-rebuild-first-fill",
        &native,
        &source,
        source.quantity,
        source.price,
    )?;
    assert!(matches!(
        bridge.observe_persisted_fill(&fill, 4)?,
        GridDecision::Actions(_)
    ));
    Ok(())
}

#[test]
fn terminal_rejected_reconciliation_rebuild_rearms_once_per_durable_episode()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut bridge, _key, _source, _native) = bridge_with_accepted_order()?;
    bridge.mark_bootstrap_confirmed()?;
    bridge.begin_startup_reconciliation()?;
    while let Some(target) = bridge.reconciliation_target()? {
        let attempt = bridge.advance_reconciliation_attempt(&target)?;
        let _plan = bridge.reconciliation_cancel_plan(&target, attempt)?;
        bridge.settle_reconciliation_cancel(&target)?;
    }
    let _failed_places = bridge.install_rebuilt_epoch(
        GridInventory {
            private_generation: 20,
            private_observed_at_ms: 20,
            mark_price: Price::new(Decimal::new(101, 0))?,
            long_quantity: Decimal::ONE,
            short_quantity: Decimal::ONE,
        },
        GridEpoch {
            epoch: 2,
            anchor_price: Price::new(Decimal::new(101, 0))?,
            step: Price::new(Decimal::ONE)?,
            grid_quantity: Decimal::new(5, 2),
            passive_book_fallback: None,
        },
    )?;
    assert!(!bridge.needs_reconciliation_rebuild());
    let valid_running_checkpoint = bridge.checkpoint_bytes()?;
    let mut corrupt: serde_json::Value = serde_json::from_slice(&valid_running_checkpoint)?;
    corrupt["startup_reconciliation"]["rebuild_attempted"] = serde_json::Value::Bool(false);
    assert!(
        GridBridgeState::restore_or_bootstrap(
            Some(serde_json::to_vec(&corrupt)?),
            bridge.grid.clone(),
            NodeGridRecoveryPolicy::RequireExisting,
        )
        .is_err()
    );
    bridge.begin_unconfirmed_install_reconciliation()?;
    assert!(bridge.needs_reconciliation_rebuild());
    assert_eq!(bridge.next_install_epoch()?, 3);

    let checkpoint = bridge.checkpoint_bytes()?;
    let checkpoint_value: serde_json::Value = serde_json::from_slice(&checkpoint)?;
    assert_eq!(checkpoint_value["terminal_rebuild_rearm_version"], 1);
    let restored = GridBridgeState::restore_or_bootstrap(
        Some(checkpoint.clone()),
        bridge.grid.clone(),
        NodeGridRecoveryPolicy::RequireExisting,
    )?;
    assert!(restored.needs_reconciliation_rebuild());
    assert_eq!(restored.next_install_epoch()?, 3);

    let mut stranded: serde_json::Value = serde_json::from_slice(&checkpoint)?;
    stranded["startup_reconciliation"]["rebuild_attempted"] = serde_json::Value::Bool(true);
    stranded["terminal_rebuild_rearm_version"] = serde_json::Value::from(1);
    let mut stranded = GridBridgeState::restore_or_bootstrap(
        Some(serde_json::to_vec(&stranded)?),
        bridge.grid.clone(),
        NodeGridRecoveryPolicy::RequireExisting,
    )?;
    assert!(!stranded.needs_reconciliation_rebuild());
    assert!(stranded.rearm_terminally_drained_rebuild()?);
    assert!(stranded.needs_reconciliation_rebuild());
    assert_eq!(stranded.next_install_epoch()?, 3);
    assert!(!stranded.rearm_terminally_drained_rebuild()?);
    let rearmed_checkpoint: serde_json::Value =
        serde_json::from_slice(&stranded.checkpoint_bytes()?)?;
    assert_eq!(rearmed_checkpoint["terminal_rebuild_rearm_version"], 1);

    let _second_failed_places = bridge.install_rebuilt_epoch(
        GridInventory {
            private_generation: 21,
            private_observed_at_ms: 21,
            mark_price: Price::new(Decimal::new(102, 0))?,
            long_quantity: Decimal::ONE,
            short_quantity: Decimal::ONE,
        },
        GridEpoch {
            epoch: 3,
            anchor_price: Price::new(Decimal::new(102, 0))?,
            step: Price::new(Decimal::ONE)?,
            grid_quantity: Decimal::new(5, 2),
            passive_book_fallback: None,
        },
    )?;
    bridge.begin_unconfirmed_install_reconciliation()?;
    assert!(bridge.needs_reconciliation_rebuild());
    assert_eq!(bridge.next_install_epoch()?, 4);
    let second_checkpoint = bridge.checkpoint_bytes()?;
    let mut second = GridBridgeState::restore_or_bootstrap(
        Some(second_checkpoint),
        bridge.grid.clone(),
        NodeGridRecoveryPolicy::RequireExisting,
    )?;
    assert!(!second.rearm_terminally_drained_rebuild()?);
    assert!(second.needs_reconciliation_rebuild());
    assert_eq!(second.next_install_epoch()?, 4);
    Ok(())
}

#[test]
fn signed_empty_surface_prunes_routes_orphaned_by_fill_burst_before_rebuild()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut bridge, _key, _source, _native) = bridge_with_accepted_order()?;
    bridge.mark_bootstrap_confirmed()?;
    bridge.begin_startup_reconciliation()?;
    let orphaned = bridge.routes.len();
    assert!(orphaned > 0);

    // This is the durable shape produced when signed fill catch-up retires the reducer's last
    // owned orders while earlier startup cancels are still becoming absent at the venue.
    bridge.grid.owned_orders.clear();
    bridge.grid.reset_orders_settled()?;
    assert_eq!(
        bridge.settle_signed_absent_reconciliation_orders(&[])?,
        orphaned
    );
    assert!(bridge.routes.is_empty());
    assert!(bridge.needs_reconciliation_rebuild());
    assert_eq!(bridge.next_install_epoch()?, 2);
    bridge.checkpoint_bytes()?;
    Ok(())
}

#[test]
fn partial_fills_accumulate_across_checkpoint_and_retire_the_completed_route()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut bridge, key, source, native_order_id) = bridge_with_accepted_order()?;
    let first_quantity = source
        .quantity
        .checked_div(Decimal::new(2, 0))
        .ok_or("first quantity")?;
    let remaining_quantity = source
        .quantity
        .checked_sub(first_quantity)
        .ok_or("remaining quantity")?;
    let first = owned_fill(
        "partial-fill-1",
        &native_order_id,
        &source,
        first_quantity,
        source.price,
    )?;
    assert_eq!(
        bridge.signed_fill_application(&first)?,
        SignedGridFillApplication::Apply
    );
    assert_eq!(
        bridge.observe_persisted_fill(&first, 9)?,
        GridDecision::Noop
    );
    assert_eq!(
        bridge.signed_fill_application(&first)?,
        SignedGridFillApplication::ExactDuplicate
    );
    let checkpoint = bridge.checkpoint_bytes()?;
    let mut decoded: GridBridgeState = serde_json::from_slice(&checkpoint)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    assert_eq!(decoded, bridge);
    decoded.grid.migrate_checkpoint()?;
    decoded.validate()?;
    let mut bridge = GridBridgeState::restore_or_bootstrap(
        Some(checkpoint),
        bridge.grid.clone(),
        NodeGridRecoveryPolicy::RequireExisting,
    )?;
    assert_eq!(
        bridge.observe_persisted_fill(&first, 9)?,
        GridDecision::Noop
    );
    let conflicting_duplicate = owned_fill(
        "partial-fill-1",
        &native_order_id,
        &source,
        source.quantity,
        source.price,
    )?;
    assert!(
        bridge
            .signed_fill_application(&conflicting_duplicate)
            .is_err()
    );
    let mut maker_conflict = first.clone();
    maker_conflict.maker = FieldState::Known(false);
    assert!(bridge.signed_fill_application(&maker_conflict).is_err());
    assert!(
        bridge
            .observe_persisted_fill(&conflicting_duplicate, 9)
            .is_err()
    );
    let wrong_price = owned_fill(
        "partial-fill-2",
        &native_order_id,
        &source,
        remaining_quantity,
        Price::new(source.price.value() + Decimal::ONE)?,
    )?;
    assert!(bridge.signed_fill_application(&wrong_price).is_err());
    assert!(bridge.observe_persisted_fill(&wrong_price, 9).is_err());
    let completion = owned_fill(
        "partial-fill-2",
        &native_order_id,
        &source,
        remaining_quantity,
        source.price,
    )?;
    assert_eq!(
        bridge.signed_fill_application(&completion)?,
        SignedGridFillApplication::Apply
    );
    let decision = bridge.observe_persisted_fill(&completion, 9)?;
    assert_eq!(
        bridge.signed_fill_application(&completion)?,
        SignedGridFillApplication::ExactDuplicate
    );
    assert_eq!(
        bridge.signed_fill_application(&first)?,
        SignedGridFillApplication::Irrelevant
    );
    let accepted_command = bridge.place_command_for_order(&source)?;
    assert_eq!(
        bridge.signed_retired_fill_application(&first, &accepted_command)?,
        SignedGridFillApplication::ExactDuplicate
    );
    let mut retired_conflict = first.clone();
    retired_conflict.maker = FieldState::Known(false);
    assert!(
        bridge
            .signed_retired_fill_application(&retired_conflict, &accepted_command)
            .is_err()
    );
    let GridDecision::Actions(actions) = decision else {
        return Err("completed maker fill did not produce a rolling action".into());
    };
    for action in &actions {
        let plan = bridge.plan_dispatch(action)?;
        assert!(!bridge.grid.pending_transactions.is_empty());
        let checkpoint = bridge.checkpoint_bytes()?;
        let restored = GridBridgeState::restore_or_bootstrap(
            Some(checkpoint),
            bridge.grid.clone(),
            NodeGridRecoveryPolicy::RequireExisting,
        )?;
        let resumed = restored.pending_dispatch_plans()?;
        assert_eq!(resumed.len(), 1);
        assert_eq!(resumed[0].commands, plan.commands);
        bridge = restored;
        let accepted = plan
            .accepted_routes
            .iter()
            .enumerate()
            .map(|(index, (_, _, command_id))| {
                (command_id.clone(), format!("native-rolling-order-{index}"))
            })
            .collect::<Vec<_>>();
        bridge.bind_accepted_plan(&plan, &accepted)?;
    }
    assert!(bridge.grid.pending_transactions.is_empty());
    assert!(!bridge.routes.contains_key(&key));
    assert!(!bridge.partial_fills.contains_key(&native_order_id));
    bridge.checkpoint_bytes()?;
    Ok(())
}
