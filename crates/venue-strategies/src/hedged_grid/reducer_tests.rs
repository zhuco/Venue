use rust_decimal::Decimal;

use venue_domain::domain::{Asset, Price};

use super::*;

fn binding() -> Result<HedgedGridBinding, Box<dyn std::error::Error>> {
    Ok(HedgedGridBinding {
        strategy_instance_id: "hedged_grid_sol_usdc".to_owned(),
        run_id: "primary".to_owned(),
        exchange: "binance".to_owned(),
        account: "portfolio_margin_um".to_owned(),
        symbol: "SOL/USDC".parse()?,
        config_version: "phase1".to_owned(),
        owner_scope: "hedged_grid_sol_usdc_primary".to_owned(),
    })
}

fn inventory(
    generation: u64,
    long: Decimal,
    short: Decimal,
) -> Result<GridInventory, Box<dyn std::error::Error>> {
    Ok(GridInventory {
        private_generation: generation,
        private_observed_at_ms: generation.saturating_mul(100),
        mark_price: Price::new(Decimal::new(100, 0))?,
        long_quantity: long,
        short_quantity: short,
    })
}

fn epoch(number: u64) -> Result<GridEpoch, Box<dyn std::error::Error>> {
    Ok(GridEpoch {
        epoch: number,
        anchor_price: Price::new(Decimal::new(100, 0))?,
        step: Price::new(Decimal::new(2, 1))?,
        grid_quantity: Decimal::new(5, 2),
        passive_book_fallback: None,
    })
}

fn fill(
    state: &HedgedGridState,
    fill_id: &str,
    source_order: GridOrderKey,
    complete: bool,
    maker: FieldState<bool>,
) -> Result<OwnedGridFill, Box<dyn std::error::Error>> {
    let fill_price = state
        .owned_orders
        .get(&source_order)
        .map(|order| order.price)
        .or_else(|| {
            state
                .owned_fill_records
                .get(fill_id)
                .map(|record| record.fill_price)
        })
        .ok_or("missing source order")?;
    Ok(OwnedGridFill {
        fill_id: fill_id.to_owned(),
        private_generation: state.inventory.as_ref().map_or(1, |inventory| {
            inventory.private_generation.saturating_add(1)
        }),
        source_order,
        fill_price,
        complete,
        maker,
    })
}

#[test]
fn configured_grid_count_controls_opening_and_closing_depth()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new_with_params(binding()?, HedgedGridParams::phase_one(3)?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(15, 2), Decimal::new(15, 2))?)?;

    let GridDecision::Actions(actions) = state.install_epoch(epoch(1)?)? else {
        return Err("missing configured grid installation".into());
    };

    let orders = actions
        .iter()
        .filter_map(|action| match action {
            GridAction::Place(order) => Some(order),
            GridAction::Reset { .. }
            | GridAction::Replenish(_)
            | GridAction::Dispatch(_)
            | GridAction::ReanchorAtFill { .. } => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(orders.len(), 12);
    assert_eq!(
        orders
            .iter()
            .filter(|order| order.key.role == GridOrderRole::Open)
            .count(),
        6
    );
    assert!(orders.iter().all(|order| order.key.level <= 3));
    Ok(())
}

#[test]
fn gate_usdt_binding_uses_the_same_fixed_grid_release() -> Result<(), Box<dyn std::error::Error>> {
    let binding = HedgedGridBinding {
        strategy_instance_id: "hedged_grid_sol_usdt".to_owned(),
        run_id: "primary".to_owned(),
        exchange: "gate".to_owned(),
        account: "usdt_futures".to_owned(),
        symbol: "SOL/USDT".parse()?,
        config_version: "stage7".to_owned(),
        owner_scope: "hedged_grid_sol_usdt_primary".to_owned(),
    };
    let params = HedgedGridParams::fixed_release(Asset::new("USDT")?, 3)?;

    let state = HedgedGridState::new_with_params(binding, params)?;

    assert_eq!(state.params.order_notional.asset.as_str(), "USDT");
    assert_eq!(state.params.grid_count, 3);
    assert_eq!(state.params.replenish_notional().value, Decimal::new(15, 0));
    Ok(())
}

#[test]
fn release_quote_asset_must_match_the_deployment_symbol() -> Result<(), Box<dyn std::error::Error>>
{
    let binding = HedgedGridBinding {
        strategy_instance_id: "hedged_grid_sol_usdt".to_owned(),
        run_id: "primary".to_owned(),
        exchange: "bitget".to_owned(),
        account: "usdt_futures".to_owned(),
        symbol: "SOL/USDT".parse()?,
        config_version: "stage7".to_owned(),
        owner_scope: "hedged_grid_sol_usdt_primary".to_owned(),
    };

    assert!(matches!(
        HedgedGridState::new_with_params(
            binding,
            HedgedGridParams::fixed_release(Asset::new("USDC")?, 3)?
        ),
        Err(HedgedGridError::Params)
    ));
    Ok(())
}

#[test]
fn low_inventory_emits_three_grid_usdc_replenishment_per_missing_leg()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new(binding()?)?;
    let decision =
        state.observe_inventory(inventory(1, Decimal::new(4, 2), Decimal::new(4, 2))?)?;
    assert_eq!(
        decision,
        GridDecision::Actions(vec![GridAction::Reset {
            reason: GridResetReason::InventoryLow
        }])
    );
    let decision = state.begin_replenishment()?;
    assert!(matches!(decision, GridDecision::Actions(_)));
    let GridDecision::Actions(actions) = decision else {
        return Ok(());
    };
    assert_eq!(actions.len(), 2);
    assert!(
        actions
            .iter()
            .all(|action| matches!(action, GridAction::Replenish(_)))
    );
    let GridAction::Replenish(action) = &actions[0] else {
        return Ok(());
    };
    assert_eq!(action.position, GridPosition::Long);
    assert_eq!(action.target_notional.asset.as_str(), "USDC");
    assert_eq!(action.target_notional.value, Decimal::new(15, 0));
    let GridAction::Replenish(action) = &actions[1] else {
        return Ok(());
    };
    assert_eq!(action.position, GridPosition::Short);
    assert_eq!(action.target_notional.value, Decimal::new(15, 0));
    assert_eq!(state.phase, GridPhase::ReplenishingInventory);
    Ok(())
}

#[test]
fn one_sided_replenishment_settles_only_the_recorded_leg() -> Result<(), Box<dyn std::error::Error>>
{
    let mut state = HedgedGridState::new(binding()?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(4, 2), Decimal::new(15, 2))?)?;
    let decision = state.begin_replenishment()?;
    let GridDecision::Actions(actions) = decision else {
        return Ok(());
    };
    assert_eq!(actions.len(), 1);
    assert!(matches!(
        &actions[0],
        GridAction::Replenish(action) if action.position == GridPosition::Long
    ));

    let decision = state.settle_pending_replenishments()?;
    assert!(state.pending_replenishments.is_empty());
    assert_eq!(state.phase, GridPhase::ResettingGrid);
    assert_eq!(
        state.reset_reason,
        Some(GridResetReason::InventoryReplenished)
    );
    assert_eq!(
        decision,
        GridDecision::Actions(vec![GridAction::Reset {
            reason: GridResetReason::InventoryReplenished
        }])
    );
    Ok(())
}

#[test]
fn unknown_epoch_resumes_only_when_authoritative_orders_are_complete()
-> Result<(), Box<dyn std::error::Error>> {
    let mut complete = HedgedGridState::new(binding()?)?;
    let _ = complete.observe_inventory(inventory(1, Decimal::new(25, 2), Decimal::new(45, 2))?)?;
    let _ = complete.install_epoch(epoch(1)?)?;
    let authoritative = complete.owned_orders.clone();
    complete.block_for_order_reconciliation()?;
    complete.reconcile_blocked_orders(authoritative)?;
    assert_eq!(complete.phase, GridPhase::Running);

    let mut partial = complete.clone();
    partial.block_for_order_reconciliation()?;
    let mut authoritative = partial.owned_orders.clone();
    let key = authoritative
        .keys()
        .next()
        .cloned()
        .ok_or(HedgedGridError::Order)?;
    authoritative.remove(&key);
    partial.reconcile_blocked_orders(authoritative)?;
    assert_eq!(partial.phase, GridPhase::ResettingGrid);
    assert_eq!(partial.reset_reason, Some(GridResetReason::Reconciliation));
    Ok(())
}

#[test]
fn blocked_grid_waits_for_its_durable_reconciliation_deadline()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new(binding()?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(25, 2), Decimal::new(45, 2))?)?;
    let _ = state.install_epoch(epoch(1)?)?;
    state.block_for_order_reconciliation()?;
    state.defer_blocked_reconciliation_until(30_100)?;

    assert!(!state.blocked_reconciliation_is_due(30_099));
    let restored: HedgedGridState = serde_json::from_value(serde_json::to_value(&state)?)?;
    assert!(!restored.blocked_reconciliation_is_due(30_099));
    assert!(restored.blocked_reconciliation_is_due(30_100));
    Ok(())
}

#[test]
fn later_readback_in_same_private_generation_updates_inventory()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new(binding()?)?;
    let first = inventory(1, Decimal::new(25, 2), Decimal::new(45, 2))?;
    let _ = state.observe_inventory(first)?;
    let _ = state.install_epoch(epoch(1)?)?;
    let mut later = inventory(1, Decimal::new(29, 2), Decimal::new(41, 2))?;
    later.private_observed_at_ms = 101;
    let decision = state.observe_inventory(later.clone())?;
    assert_eq!(decision, GridDecision::Noop);
    assert_eq!(state.inventory, Some(later));
    assert_eq!(state.phase, GridPhase::Running);
    Ok(())
}

#[test]
fn operator_restart_can_rebuild_without_market_replenishment()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new(binding()?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(4, 2), Decimal::new(15, 2))?)?;
    state.request_restart_without_replenishment()?;
    let decision =
        state.observe_inventory(inventory(2, Decimal::new(4, 2), Decimal::new(15, 2))?)?;
    assert_eq!(decision, GridDecision::Noop);
    assert_eq!(state.reset_reason, Some(GridResetReason::Manual));
    let decision = state.install_epoch(epoch(1)?)?;
    let GridDecision::Actions(actions) = decision else {
        return Ok(());
    };
    assert_eq!(actions.len(), usize::from(state.params.grid_count) * 2 + 3);
    assert_eq!(state.phase, GridPhase::Running);

    let decision =
        state.observe_inventory(inventory(3, Decimal::new(4, 2), Decimal::new(15, 2))?)?;
    assert_eq!(decision, GridDecision::Noop);
    assert!(state.suppress_replenishment_until_inventory_recovers);
    let _ = state.observe_inventory(inventory(4, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    assert!(!state.suppress_replenishment_until_inventory_recovers);
    Ok(())
}

#[test]
fn stopped_instance_can_resume_only_after_owned_orders_are_empty()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new(binding()?)?;
    state.phase = GridPhase::Stopping;
    state.inventory_recovery = InventoryRecoveryState::AwaitingNextOwnedFill {
        armed_generation: 7,
    };
    state.resume_after_stop()?;
    assert_eq!(state.phase, GridPhase::Recovering);
    assert_eq!(state.inventory_recovery, InventoryRecoveryState::Inactive);

    let mut unsafe_state = HedgedGridState::new(binding()?)?;
    let _ =
        unsafe_state.observe_inventory(inventory(1, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    let _ = unsafe_state.install_epoch(GridEpoch {
        epoch: 1,
        anchor_price: Price::new(Decimal::new(100, 0))?,
        step: Price::new(Decimal::new(2, 1))?,
        grid_quantity: Decimal::new(5, 2),
        passive_book_fallback: None,
    })?;
    unsafe_state.phase = GridPhase::Stopping;
    assert_eq!(
        unsafe_state.resume_after_stop(),
        Err(HedgedGridError::Phase)
    );
    Ok(())
}

#[test]
fn phase_one_epoch_creates_ten_open_and_inventory_limited_close_orders_per_leg()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new(binding()?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    let decision = state.install_epoch(epoch(1)?)?;
    assert!(matches!(decision, GridDecision::Actions(_)));
    let GridDecision::Actions(actions) = decision else {
        return Ok(());
    };
    assert_eq!(state.phase, GridPhase::Running);
    assert_eq!(state.owned_orders.len(), 26);
    assert_eq!(actions.len(), 26);
    let long_open = state.owned_orders.get(&GridOrderKey {
        epoch: 1,
        position: GridPosition::Long,
        role: GridOrderRole::Open,
        level: 1,
    });
    assert_eq!(
        long_open.map(|order| order.price.value()),
        Some(Decimal::new(998, 1))
    );
    Ok(())
}

#[test]
fn running_grid_state_round_trips_through_json_projection() -> Result<(), Box<dyn std::error::Error>>
{
    let mut state = HedgedGridState::new(binding()?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    let _ = state.install_epoch(epoch(1)?)?;
    let encoded = serde_json::to_vec(&state)?;
    let restored: HedgedGridState = serde_json::from_slice(&encoded)?;
    assert_eq!(restored, state);
    Ok(())
}

#[test]
fn mirrored_full_fills_reserve_two_independent_two_place_one_cancel_transactions()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new(binding()?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    let _ = state.install_epoch(epoch(1)?)?;
    let short_open = GridOrderKey {
        epoch: 1,
        position: GridPosition::Short,
        role: GridOrderRole::Open,
        level: 1,
    };
    let long_close = GridOrderKey {
        epoch: 1,
        position: GridPosition::Long,
        role: GridOrderRole::Close,
        level: 1,
    };
    let event = fill(
        &state,
        "fill-short-open",
        short_open,
        true,
        FieldState::Known(true),
    )?;
    let first = state.observe_owned_fill(event)?;
    let event = fill(
        &state,
        "fill-long-close",
        long_close,
        true,
        FieldState::Known(true),
    )?;
    let second = state.observe_owned_fill(event)?;
    assert!(matches!(first, GridDecision::Actions(_)));
    assert!(matches!(second, GridDecision::Actions(_)));
    let GridDecision::Actions(first) = first else {
        return Ok(());
    };
    let GridDecision::Actions(second) = second else {
        return Ok(());
    };
    assert!(matches!(&first[0], GridAction::Dispatch(_)));
    assert!(matches!(&second[0], GridAction::Dispatch(_)));
    let GridAction::Dispatch(first) = &first[0] else {
        return Ok(());
    };
    let GridAction::Dispatch(second) = &second[0] else {
        return Ok(());
    };
    assert_ne!(first.id, second.id);
    assert_eq!(first.places.len(), 2);
    assert_eq!(second.places.len(), 2);
    assert_eq!(first.places[0].price.value(), Decimal::new(1022, 1));
    assert_eq!(first.places[1].price.value(), Decimal::new(1000, 1));
    assert_eq!(second.places[0].price.value(), Decimal::new(1000, 1));
    assert_eq!(second.places[1].price.value(), Decimal::new(1008, 1));
    assert_eq!(
        state
            .owned_orders
            .values()
            .filter(|order| order.side == OrderSide::Sell)
            .map(|order| order.price.value())
            .max(),
        Some(Decimal::new(1022, 1))
    );
    assert_eq!(state.pending_transactions.len(), 2);
    Ok(())
}

#[test]
fn unsubmitted_rolling_transaction_can_return_to_exact_reconciliation()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new(binding()?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    let _ = state.install_epoch(epoch(1)?)?;
    let source = GridOrderKey {
        epoch: 1,
        position: GridPosition::Short,
        role: GridOrderRole::Open,
        level: 1,
    };
    let event = fill(
        &state,
        "preflight-cap",
        source.clone(),
        true,
        FieldState::Known(true),
    )?;
    let GridDecision::Actions(actions) = state.observe_owned_fill(event)? else {
        return Err("missing rolling transaction".into());
    };
    let transaction = actions
        .into_iter()
        .find_map(|action| match action {
            GridAction::Dispatch(transaction) => Some(transaction),
            GridAction::Reset { .. }
            | GridAction::Place(_)
            | GridAction::Replenish(_)
            | GridAction::ReanchorAtFill { .. } => None,
        })
        .ok_or("missing dispatch")?;
    let cancelled_order = transaction
        .cancelled_order
        .clone()
        .ok_or("missing cancelled order")?;

    let decision = state.abandon_unsubmitted_transactions_for_reconciliation(
        std::slice::from_ref(&transaction.id),
    )?;

    assert_eq!(
        decision,
        GridDecision::Actions(vec![GridAction::Reset {
            reason: GridResetReason::Reconciliation
        }])
    );
    assert_eq!(state.phase, GridPhase::ResettingGrid);
    assert!(state.pending_transactions.is_empty());
    assert!(!state.owned_orders.contains_key(&source));
    assert_eq!(
        state.owned_orders.get(&transaction.cancel),
        Some(&cancelled_order)
    );
    assert!(
        transaction
            .places
            .iter()
            .all(|order| !state.owned_orders.contains_key(&order.key))
    );
    assert_eq!(state.seen_fill_ids.get("preflight-cap"), Some(&source));
    Ok(())
}

#[test]
fn adjacent_opposite_fills_never_cancel_an_unaccepted_pending_place()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new(binding()?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(65, 2), Decimal::new(65, 2))?)?;
    let _ = state.install_epoch(epoch(1)?)?;
    let first_source = GridOrderKey {
        epoch: 1,
        position: GridPosition::Short,
        role: GridOrderRole::Open,
        level: 1,
    };
    let event = fill(
        &state,
        "short-open-first",
        first_source,
        true,
        FieldState::Known(true),
    )?;
    let GridDecision::Actions(first) = state.observe_stream_owned_fill(event)? else {
        return Err("missing first rolling transaction".into());
    };
    let second_source = GridOrderKey {
        epoch: 1,
        position: GridPosition::Short,
        role: GridOrderRole::Close,
        level: 1,
    };
    let event = fill(
        &state,
        "short-close-second",
        second_source,
        true,
        FieldState::Known(true),
    )?;
    let GridDecision::Actions(second) = state.observe_stream_owned_fill(event)? else {
        return Err("missing second rolling transaction".into());
    };
    let GridAction::Dispatch(first) = &first[0] else {
        return Err("missing first dispatch".into());
    };
    let GridAction::Dispatch(second) = &second[0] else {
        return Err("missing second dispatch".into());
    };

    assert!(first.places.iter().all(|order| order.key != second.cancel));
    assert_eq!(state.pending_transactions.len(), 2);
    Ok(())
}

#[test]
fn historical_fill_epochs_never_overwrite_the_active_order_sequences()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new(binding()?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    let _ = state.install_epoch(epoch(12)?)?;
    state.seen_fill_ids.insert(
        "historical-fill".to_owned(),
        GridOrderKey {
            epoch: 10,
            position: GridPosition::Long,
            role: GridOrderRole::Open,
            level: 99,
        },
    );

    state.reconcile_order_sequences();
    let source = GridOrderKey {
        epoch: 12,
        position: GridPosition::Long,
        role: GridOrderRole::Close,
        level: 1,
    };
    let event = fill(
        &state,
        "current-fill",
        source,
        true,
        FieldState::Known(true),
    )?;
    let decision = state.observe_owned_fill(event)?;

    assert!(matches!(decision, GridDecision::Actions(_)));
    assert_eq!(state.order_sequences.epoch, 12);
    Ok(())
}

#[test]
fn buy_fill_moves_both_lanes_down_one_step() -> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new(binding()?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    let _ = state.install_epoch(epoch(1)?)?;
    let source = GridOrderKey {
        epoch: 1,
        position: GridPosition::Long,
        role: GridOrderRole::Open,
        level: 1,
    };
    let event = fill(
        &state,
        "fill-long-open",
        source,
        true,
        FieldState::Known(true),
    )?;
    let GridDecision::Actions(actions) = state.observe_owned_fill(event)? else {
        return Ok(());
    };
    let GridAction::Dispatch(transaction) = &actions[0] else {
        return Ok(());
    };
    assert_eq!(transaction.places[0].price.value(), Decimal::new(978, 1));
    assert_eq!(transaction.places[1].price.value(), Decimal::new(1000, 1));
    assert_eq!(
        state
            .owned_orders
            .values()
            .filter(|order| {
                order.key.position == GridPosition::Long && order.key.role == GridOrderRole::Close
            })
            .map(|order| order.price.value())
            .max(),
        Some(Decimal::new(1004, 1))
    );
    Ok(())
}

#[test]
fn highest_filled_identity_is_never_reused() -> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new(binding()?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    let _ = state.install_epoch(epoch(1)?)?;
    let source = GridOrderKey {
        epoch: 1,
        position: GridPosition::Long,
        role: GridOrderRole::Close,
        level: 3,
    };
    let event = fill(
        &state,
        "fill-highest-long-close",
        source,
        true,
        FieldState::Known(true),
    )?;
    let GridDecision::Actions(actions) = state.observe_owned_fill(event)? else {
        return Ok(());
    };
    let GridAction::Dispatch(transaction) = &actions[0] else {
        return Ok(());
    };
    assert_eq!(
        transaction.places[0].key.level,
        u64::from(state.params.grid_count) + 1
    );
    assert_eq!(transaction.places[1].key.level, 4);
    let _ = state.settle_transaction(&transaction.id, false)?;
    assert_eq!(state.phase, GridPhase::BlockedUnknown);
    assert!(state.pending_transactions.contains_key(&transaction.id));
    Ok(())
}

#[test]
fn partial_or_duplicate_fill_never_creates_another_transaction()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new(binding()?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    let _ = state.install_epoch(epoch(1)?)?;
    let source = GridOrderKey {
        epoch: 1,
        position: GridPosition::Long,
        role: GridOrderRole::Open,
        level: 1,
    };
    let partial = fill(
        &state,
        "fill-one",
        source.clone(),
        false,
        FieldState::Known(true),
    )?;
    assert_eq!(state.observe_owned_fill(partial)?, GridDecision::Noop);
    let complete = fill(
        &state,
        "fill-one",
        source.clone(),
        true,
        FieldState::Known(true),
    )?;
    let _ = state.observe_owned_fill(complete.clone())?;
    assert_eq!(state.observe_owned_fill(complete)?, GridDecision::Noop);
    assert_eq!(state.pending_transactions.len(), 1);
    Ok(())
}

#[test]
fn legacy_zero_fill_generation_loads_but_cannot_consume_reanchor_wait()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new_with_params(binding()?, HedgedGridParams::phase_one(3)?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(10, 2), Decimal::new(10, 2))?)?;
    let _ = state.install_epoch(epoch(1)?)?;
    let _ = state.observe_inventory(inventory(2, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    let source = GridOrderKey {
        epoch: 1,
        position: GridPosition::Long,
        role: GridOrderRole::Open,
        level: 1,
    };
    let unresolved = fill(
        &state,
        "legacy-generation",
        source,
        true,
        FieldState::Missing,
    )?;
    assert_eq!(
        state.observe_owned_fill(unresolved.clone())?,
        GridDecision::Noop
    );
    let mut checkpoint = serde_json::to_value(&state)?;
    checkpoint["owned_fill_records"]["legacy-generation"]
        .as_object_mut()
        .ok_or("historical fill was not an object")?
        .remove("private_generation")
        .ok_or("historical fill generation was not serialized")?;
    let mut state: HedgedGridState = serde_json::from_value(checkpoint)?;

    // Schema 2 defaults the missing legacy field to zero and accepts the historical fact.
    state.migrate_checkpoint()?;

    let mut maker_proof = unresolved;
    maker_proof.maker = FieldState::Known(true);
    let GridDecision::Actions(actions) = state.observe_owned_fill(maker_proof)? else {
        return Err("maker proof did not drive ordinary rolling".into());
    };
    assert!(
        actions
            .iter()
            .all(|action| matches!(action, GridAction::Dispatch(_)))
    );
    assert_eq!(
        state.inventory_recovery,
        InventoryRecoveryState::AwaitingNextOwnedFill {
            armed_generation: 2
        }
    );
    Ok(())
}

#[test]
fn schema_two_checkpoint_rejects_recovery_phase_epoch_and_fill_generation_conflicts()
-> Result<(), Box<dyn std::error::Error>> {
    let mut pending =
        HedgedGridState::new_with_params(binding()?, HedgedGridParams::phase_one(3)?)?;
    let _ = pending.observe_inventory(inventory(1, Decimal::new(10, 2), Decimal::new(10, 2))?)?;
    let _ = pending.install_epoch(epoch(1)?)?;
    let _ = pending.observe_inventory(inventory(2, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    let _ = pending.observe_inventory(inventory(3, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    let source = GridOrderKey {
        epoch: 1,
        position: GridPosition::Long,
        role: GridOrderRole::Open,
        level: 1,
    };
    let mut event = fill(
        &pending,
        "checkpoint-reanchor",
        source,
        true,
        FieldState::Known(true),
    )?;
    // A signed readback may contain both the completing fill and its resulting inventory in
    // generation 3. That same-generation pending boundary is valid and restartable.
    event.private_generation = 3;
    let _ = pending.observe_owned_fill(event)?;
    pending.migrate_checkpoint()?;

    let mut wrong_phase = pending.clone();
    wrong_phase.phase = GridPhase::ResettingGrid;
    wrong_phase.reset_reason = Some(GridResetReason::Manual);
    assert_eq!(
        wrong_phase.migrate_checkpoint(),
        Err(HedgedGridError::Checkpoint)
    );

    let mut missing_epoch = pending.clone();
    missing_epoch.epoch = None;
    assert_eq!(
        missing_epoch.migrate_checkpoint(),
        Err(HedgedGridError::Checkpoint)
    );

    let mut zero_fill_generation = pending.clone();
    zero_fill_generation
        .owned_fill_records
        .get_mut("checkpoint-reanchor")
        .ok_or("missing fill record")?
        .private_generation = 0;
    assert_eq!(
        zero_fill_generation.migrate_checkpoint(),
        Err(HedgedGridError::Checkpoint)
    );

    let mut same_generation = pending.clone();
    same_generation
        .owned_fill_records
        .get_mut("checkpoint-reanchor")
        .ok_or("missing fill record")?
        .private_generation = 3;
    same_generation.migrate_checkpoint()?;

    let mut future_trigger_generation = pending.clone();
    future_trigger_generation
        .owned_fill_records
        .get_mut("checkpoint-reanchor")
        .ok_or("missing fill record")?
        .private_generation = 4;
    assert_eq!(
        future_trigger_generation.migrate_checkpoint(),
        Err(HedgedGridError::Checkpoint)
    );

    let mut missing_trigger_record = pending.clone();
    missing_trigger_record
        .owned_fill_records
        .remove("checkpoint-reanchor");
    assert_eq!(
        missing_trigger_record.migrate_checkpoint(),
        Err(HedgedGridError::Checkpoint)
    );

    let mut rebuilding = pending;
    rebuilding.begin_reanchor_rebuild()?;
    rebuilding.migrate_checkpoint()?;
    Ok(())
}

#[test]
fn schema_two_drained_stop_retires_late_maker_as_replay_tombstone()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new_with_params(binding()?, HedgedGridParams::phase_one(3)?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    let _ = state.install_epoch(epoch(74)?)?;
    let source = GridOrderKey {
        epoch: 74,
        position: GridPosition::Long,
        role: GridOrderRole::Open,
        level: 1,
    };
    let mut late_maker = fill(
        &state,
        "227010969",
        source.clone(),
        true,
        FieldState::Missing,
    )?;
    assert_eq!(
        state.observe_owned_fill(late_maker.clone())?,
        GridDecision::Noop
    );
    let record = state
        .owned_fill_records
        .get_mut("227010969")
        .ok_or("missing late maker record")?;
    record.maker = Some(true);
    assert!(!record.grid_action_emitted);

    let mut early_schema_three = state.clone();
    early_schema_three.owned_orders.clear();
    early_schema_three.phase = GridPhase::Stopping;
    early_schema_three.migrate_checkpoint()?;
    let early_retired = early_schema_three
        .owned_fill_records
        .get("227010969")
        .ok_or("missing early schema-3 retired maker record")?;
    assert!(early_retired.retired_without_action);
    assert_eq!(
        early_schema_three.seen_fill_ids.get("227010969"),
        Some(&source)
    );

    state.owned_orders.clear();
    state.phase = GridPhase::Stopping;
    state.schema_version = 2;

    state.migrate_checkpoint()?;
    assert_eq!(
        state.schema_version,
        super::super::HEDGED_GRID_SCHEMA_VERSION
    );
    let retired = state
        .owned_fill_records
        .get("227010969")
        .ok_or("missing retired maker record")?;
    assert!(retired.retired_without_action);
    assert!(!retired.grid_action_emitted);
    assert_eq!(state.seen_fill_ids.get("227010969"), Some(&source));

    late_maker.maker = FieldState::Known(true);
    assert_eq!(state.observe_owned_fill(late_maker)?, GridDecision::Noop);
    let restored: HedgedGridState = serde_json::from_value(serde_json::to_value(&state)?)?;
    let mut restored = restored;
    restored.migrate_checkpoint()?;
    assert_eq!(restored, state);
    Ok(())
}

#[test]
fn installing_a_new_epoch_retires_superseded_unemitted_maker_facts()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new_with_params(binding()?, HedgedGridParams::phase_one(3)?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    let _ = state.install_epoch(epoch(74)?)?;
    let source = GridOrderKey {
        epoch: 74,
        position: GridPosition::Long,
        role: GridOrderRole::Open,
        level: 1,
    };
    let pending = fill(
        &state,
        "superseded-maker",
        source.clone(),
        true,
        FieldState::Missing,
    )?;
    assert_eq!(state.observe_owned_fill(pending)?, GridDecision::Noop);
    state
        .owned_fill_records
        .get_mut("superseded-maker")
        .ok_or("missing superseded maker")?
        .maker = Some(true);

    let _ = state.request_reset(GridResetReason::Manual)?;
    state.reset_orders_settled()?;
    let _ = state.observe_inventory(inventory(2, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    let _ = state.install_epoch(epoch(75)?)?;

    let record = state
        .owned_fill_records
        .get("superseded-maker")
        .ok_or("missing retired maker")?;
    assert!(record.retired_without_action);
    assert!(!record.grid_action_emitted);
    assert_eq!(state.seen_fill_ids.get("superseded-maker"), Some(&source));
    state.migrate_checkpoint()?;
    Ok(())
}

#[test]
fn startup_reset_fill_retires_the_old_order_without_rolling_actions()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new_with_params(binding()?, HedgedGridParams::phase_one(3)?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    let _ = state.install_epoch(epoch(1)?)?;
    let source = GridOrderKey {
        epoch: 1,
        position: GridPosition::Long,
        role: GridOrderRole::Open,
        level: 1,
    };
    let event = fill(
        &state,
        "startup-reset-fill",
        source.clone(),
        true,
        FieldState::Known(true),
    )?;
    let _ = state.request_reset(GridResetReason::Reconciliation)?;

    state.retire_owned_fill_during_reset(event.clone())?;
    assert!(!state.owned_orders.contains_key(&source));
    assert!(state.pending_transactions.is_empty());
    let record = state
        .owned_fill_records
        .get("startup-reset-fill")
        .ok_or("missing reset fill record")?;
    assert!(record.retired_without_action);
    assert!(!record.grid_action_emitted);
    assert_eq!(state.seen_fill_ids.get("startup-reset-fill"), Some(&source));
    state.retire_owned_fill_during_reset(event.clone())?;

    let mut conflict = event;
    conflict.maker = FieldState::Known(false);
    assert_eq!(
        state.retire_owned_fill_during_reset(conflict),
        Err(HedgedGridError::FillConflict)
    );
    state.migrate_checkpoint()?;
    Ok(())
}

#[test]
fn retired_fill_and_schema_two_migration_reject_tampering_or_live_debt()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new_with_params(binding()?, HedgedGridParams::phase_one(3)?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    let _ = state.install_epoch(epoch(74)?)?;
    let source = GridOrderKey {
        epoch: 74,
        position: GridPosition::Long,
        role: GridOrderRole::Open,
        level: 1,
    };
    let event = fill(&state, "227010969", source, true, FieldState::Missing)?;
    let _ = state.observe_owned_fill(event)?;
    state
        .owned_fill_records
        .get_mut("227010969")
        .ok_or("missing late maker record")?
        .maker = Some(true);
    state.schema_version = 2;

    let mut running = state.clone();
    running.phase = GridPhase::Running;
    assert_eq!(
        running.migrate_checkpoint(),
        Err(HedgedGridError::Checkpoint)
    );

    let mut stopping_with_orders = state.clone();
    stopping_with_orders.phase = GridPhase::Stopping;
    assert_eq!(
        stopping_with_orders.migrate_checkpoint(),
        Err(HedgedGridError::Checkpoint)
    );

    state.owned_orders.clear();
    state.phase = GridPhase::Stopping;
    state.migrate_checkpoint()?;
    let valid = state;

    let mut missing_seen = valid.clone();
    missing_seen.seen_fill_ids.remove("227010969");
    assert_eq!(
        missing_seen.migrate_checkpoint(),
        Err(HedgedGridError::Checkpoint)
    );

    let mut wrong_maker = valid.clone();
    wrong_maker
        .owned_fill_records
        .get_mut("227010969")
        .ok_or("missing retired record")?
        .maker = Some(false);
    assert_eq!(
        wrong_maker.migrate_checkpoint(),
        Err(HedgedGridError::Checkpoint)
    );

    let mut claimed_action = valid.clone();
    claimed_action
        .owned_fill_records
        .get_mut("227010969")
        .ok_or("missing retired record")?
        .grid_action_emitted = true;
    assert_eq!(
        claimed_action.migrate_checkpoint(),
        Err(HedgedGridError::Checkpoint)
    );

    let mut forged_schema_two = valid;
    forged_schema_two.schema_version = 2;
    assert_eq!(
        forged_schema_two.migrate_checkpoint(),
        Err(HedgedGridError::Checkpoint)
    );
    Ok(())
}

#[test]
fn schema_two_checkpoint_rejects_impossible_recovery_generations()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new_with_params(binding()?, HedgedGridParams::phase_one(3)?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(10, 2), Decimal::new(10, 2))?)?;
    let _ = state.install_epoch(epoch(1)?)?;
    state.inventory_recovery = InventoryRecoveryState::Deficient {
        legs: InventoryDeficiency {
            long: true,
            short: true,
        },
        first_seen_generation: 2,
    };
    assert_eq!(state.migrate_checkpoint(), Err(HedgedGridError::Checkpoint));

    state.inventory_recovery = InventoryRecoveryState::AwaitingNextOwnedFill {
        armed_generation: 2,
    };
    assert_eq!(state.migrate_checkpoint(), Err(HedgedGridError::Checkpoint));
    Ok(())
}
