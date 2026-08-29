use rust_decimal::Decimal;

use venue_domain::domain::{FieldState, Price};

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
fn configured_capacity_uses_grid_quantity_times_n_for_three_and_ten_levels()
-> Result<(), Box<dyn std::error::Error>> {
    for grid_count in [3_u8, 10_u8] {
        let required = Decimal::new(5, 2) * Decimal::from(grid_count);
        for (quantity, deficient) in [
            (required - Decimal::new(5, 2), true),
            (required, false),
            (required + Decimal::new(5, 2), false),
        ] {
            let mut state = HedgedGridState::new_with_params(
                binding()?,
                HedgedGridParams::phase_one(grid_count)?,
            )?;
            let _ = state.observe_inventory(inventory(1, quantity, quantity)?)?;
            let _ = state.install_epoch(epoch(1)?)?;
            assert_eq!(
                matches!(
                    state.inventory_recovery,
                    InventoryRecoveryState::Deficient { .. }
                ),
                deficient,
                "grid_count={grid_count}, quantity={quantity}"
            );
        }
    }
    Ok(())
}

#[test]
fn recovery_waits_for_complete_owned_maker_and_persists_exact_fill_price()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new_with_params(binding()?, HedgedGridParams::phase_one(3)?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(10, 2), Decimal::new(10, 2))?)?;
    let _ = state.install_epoch(epoch(1)?)?;
    assert!(matches!(
        state.inventory_recovery,
        InventoryRecoveryState::Deficient { .. }
    ));

    let _ = state.observe_inventory(inventory(2, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    assert_eq!(
        state.inventory_recovery,
        InventoryRecoveryState::AwaitingNextOwnedFill {
            armed_generation: 2
        }
    );

    let taker_source = GridOrderKey {
        epoch: 1,
        position: GridPosition::Long,
        role: GridOrderRole::Open,
        level: 1,
    };
    let taker = fill(
        &state,
        "taker",
        taker_source,
        true,
        FieldState::Known(false),
    )?;
    assert_eq!(state.observe_stream_owned_fill(taker)?, GridDecision::Noop);
    assert!(matches!(
        state.inventory_recovery,
        InventoryRecoveryState::AwaitingNextOwnedFill { .. }
    ));

    let maker_source = GridOrderKey {
        epoch: 1,
        position: GridPosition::Short,
        role: GridOrderRole::Open,
        level: 1,
    };
    let mut unresolved = fill(
        &state,
        "proved-later",
        maker_source,
        true,
        FieldState::Unavailable {
            reason: venue_domain::domain::UnknownReason::NotYetObserved,
        },
    )?;
    unresolved.fill_price = Price::new(Decimal::new(100_123, 3))?;
    assert_eq!(
        state.observe_stream_owned_fill(unresolved.clone())?,
        GridDecision::Noop
    );
    unresolved.maker = FieldState::Known(true);
    let decision = state.observe_stream_owned_fill(unresolved)?;
    assert_eq!(
        decision,
        GridDecision::Actions(vec![GridAction::ReanchorAtFill {
            fill_id: "proved-later".to_owned(),
            fill_price: Price::new(Decimal::new(100_123, 3))?,
        }])
    );
    assert!(state.pending_transactions.is_empty());

    let restored: HedgedGridState = serde_json::from_slice(&serde_json::to_vec(&state)?)?;
    assert_eq!(restored.inventory_recovery, state.inventory_recovery);
    Ok(())
}

#[test]
fn reanchor_pending_checkpoint_accepts_the_fill_and_inventory_from_the_same_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new_with_params(binding()?, HedgedGridParams::phase_one(3)?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(10, 2), Decimal::new(10, 2))?)?;
    let _ = state.install_epoch(epoch(1)?)?;
    let _ = state.observe_inventory(inventory(2, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    let _ = state.observe_inventory(inventory(3, Decimal::new(15, 2), Decimal::new(15, 2))?)?;

    let source = GridOrderKey {
        epoch: 1,
        position: GridPosition::Short,
        role: GridOrderRole::Open,
        level: 1,
    };
    let mut event = fill(
        &state,
        "same-generation-reanchor",
        source,
        true,
        FieldState::Known(true),
    )?;
    event.private_generation = 3;
    let _ = state.observe_owned_fill(event)?;
    assert!(matches!(
        state.inventory_recovery,
        InventoryRecoveryState::ReanchorPending { .. }
    ));

    let encoded = serde_json::to_vec(&state)?;
    let mut restored: HedgedGridState = serde_json::from_slice(&encoded)?;
    restored.migrate_checkpoint()?;

    let mut future_fill = restored;
    future_fill
        .owned_fill_records
        .get_mut("same-generation-reanchor")
        .ok_or("missing reanchor fill")?
        .private_generation = 4;
    assert_eq!(
        future_fill.migrate_checkpoint(),
        Err(HedgedGridError::Checkpoint)
    );
    Ok(())
}

#[test]
fn rebuilding_retains_trigger_across_restart_until_capacity_is_verified()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new_with_params(binding()?, HedgedGridParams::phase_one(3)?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(10, 2), Decimal::new(10, 2))?)?;
    let _ = state.install_epoch(epoch(1)?)?;
    let _ = state.observe_inventory(inventory(2, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    let source = GridOrderKey {
        epoch: 1,
        position: GridPosition::Short,
        role: GridOrderRole::Open,
        level: 1,
    };
    let event = fill(&state, "reanchor", source, true, FieldState::Known(true))?;
    let _ = state.observe_owned_fill(event)?;
    state.begin_reanchor_rebuild()?;

    let encoded = serde_json::to_vec(&state)?;
    let mut restored: HedgedGridState = serde_json::from_slice(&encoded)?;
    assert!(matches!(
        restored.inventory_recovery,
        InventoryRecoveryState::Rebuilding { .. }
    ));
    restored.reset_orders_settled()?;
    let _ = restored.observe_inventory(inventory(3, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    let _ = restored.install_epoch(GridEpoch {
        epoch: 2,
        anchor_price: Price::new(Decimal::new(100_123, 3))?,
        step: Price::new(Decimal::new(2, 1))?,
        grid_quantity: Decimal::new(5, 2),
        passive_book_fallback: None,
    })?;
    restored.complete_reanchor_rebuild()?;
    assert_eq!(
        restored.inventory_recovery,
        InventoryRecoveryState::Inactive
    );
    Ok(())
}

#[test]
fn awaiting_recovery_returns_to_deficient_if_either_leg_drops_again()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new_with_params(binding()?, HedgedGridParams::phase_one(3)?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(10, 2), Decimal::new(15, 2))?)?;
    let _ = state.install_epoch(epoch(1)?)?;
    let _ = state.observe_inventory(inventory(2, Decimal::new(15, 2), Decimal::new(15, 2))?)?;
    let _ = state.observe_inventory(inventory(3, Decimal::new(15, 2), Decimal::new(10, 2))?)?;
    assert_eq!(
        state.inventory_recovery,
        InventoryRecoveryState::Deficient {
            legs: InventoryDeficiency {
                long: false,
                short: true,
            },
            first_seen_generation: 3,
        }
    );
    Ok(())
}

#[test]
fn schema_one_checkpoint_defaults_and_migrates_deterministically()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = HedgedGridState::new_with_params(binding()?, HedgedGridParams::phase_one(3)?)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(10, 2), Decimal::new(10, 2))?)?;
    let _ = state.install_epoch(epoch(1)?)?;
    let mut legacy = serde_json::to_value(&state)?;
    legacy["schema_version"] = serde_json::json!(1);
    legacy
        .as_object_mut()
        .ok_or("state object")?
        .remove("inventory_recovery");
    legacy
        .as_object_mut()
        .ok_or("state object")?
        .remove("owned_fill_records");
    let mut restored: HedgedGridState = serde_json::from_value(legacy)?;
    assert_eq!(
        restored.inventory_recovery,
        InventoryRecoveryState::Inactive
    );
    restored.migrate_checkpoint()?;
    assert_eq!(
        restored.schema_version,
        super::super::HEDGED_GRID_SCHEMA_VERSION
    );
    assert!(matches!(
        restored.inventory_recovery,
        InventoryRecoveryState::Deficient { .. }
    ));
    let once = restored.clone();
    restored.migrate_checkpoint()?;
    assert_eq!(restored, once);
    Ok(())
}
