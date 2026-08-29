use rust_decimal::Decimal;

use super::{
    ExposureLadderRepairPlan, GridFillRoute, MarketReductionPlan, associate_reduction_fill,
    plan_market_reduction, plan_same_anchor_exposure_repair, route_grid_fill,
    summarize_reduction_fills,
};
use crate::{
    domain::{
        AccountRiskSnapshot, Amount, Asset, FieldState, Fill, Instrument, LegRiskSnapshot,
        MarketKind, OrderSide, PositionSide, Price, RiskSourceStatus,
    },
    strategy::hedged_grid::{
        GridEpoch, GridInventory, GridOrderKey, GridOrderRole, GridPosition, HedgedGridBinding,
        HedgedGridParams, HedgedGridState, InventoryRecoveryState, OwnedGridFill,
        ReduceProfitableExposure,
    },
};

fn running_state() -> Result<HedgedGridState, Box<dyn std::error::Error>> {
    let binding = HedgedGridBinding {
        strategy_instance_id: "repair-grid".to_owned(),
        run_id: "repair-run".to_owned(),
        exchange: "gate".to_owned(),
        account: "usdt_futures".to_owned(),
        symbol: "DOGE/USDT".parse()?,
        config_version: "test".to_owned(),
        owner_scope: "repair-grid-primary".to_owned(),
    };
    let mut state = HedgedGridState::new_with_params(
        binding,
        HedgedGridParams::fixed_release(Asset::new("USDT")?, 3)?,
    )?;
    let _ = state.observe_inventory(inventory(1, 15, 15)?)?;
    let _ = state.install_epoch(GridEpoch {
        epoch: 7,
        anchor_price: Price::new(Decimal::new(100, 0))?,
        step: Price::new(Decimal::new(2, 1))?,
        grid_quantity: Decimal::new(5, 2),
        passive_book_fallback: None,
    })?;
    Ok(state)
}

fn inventory(
    generation: u64,
    long_hundredths: i64,
    short_hundredths: i64,
) -> Result<GridInventory, Box<dyn std::error::Error>> {
    Ok(GridInventory {
        private_generation: generation,
        private_observed_at_ms: generation * 100,
        mark_price: Price::new(Decimal::new(100, 0))?,
        long_quantity: Decimal::new(long_hundredths, 2),
        short_quantity: Decimal::new(short_hundredths, 2),
    })
}

#[test]
fn settled_risk_reduction_repairs_only_unsupported_closing_at_same_anchor()
-> Result<(), Box<dyn std::error::Error>> {
    let state = running_state()?;
    let original_epoch = state.epoch.clone();
    let signed = state.owned_orders.clone();

    let ExposureLadderRepairPlan::Ready {
        target,
        cancel,
        place,
    } = plan_same_anchor_exposure_repair(&state, &inventory(2, 10, 15)?, &signed)?
    else {
        return Err("repair unexpectedly waited for maker replay".into());
    };

    assert_eq!(state.epoch, original_epoch);
    assert!(place.is_empty());
    assert_eq!(cancel.len(), 1);
    assert_eq!(cancel[0].position, GridPosition::Long);
    assert_eq!(cancel[0].role, GridOrderRole::Close);
    assert_eq!(target.len(), signed.len() - 1);
    assert_eq!(
        target
            .keys()
            .filter(|key| key.role == GridOrderRole::Open)
            .count(),
        6
    );
    Ok(())
}

#[test]
fn signed_missing_opening_and_supported_closing_are_planned_once()
-> Result<(), Box<dyn std::error::Error>> {
    let state = running_state()?;
    let missing_open = GridOrderKey {
        epoch: 7,
        position: GridPosition::Long,
        role: GridOrderRole::Open,
        level: 1,
    };
    let missing_close = GridOrderKey {
        epoch: 7,
        position: GridPosition::Short,
        role: GridOrderRole::Close,
        level: 2,
    };
    let mut signed = state.owned_orders.clone();
    signed.remove(&missing_open);
    signed.remove(&missing_close);

    let first = plan_same_anchor_exposure_repair(&state, &inventory(2, 15, 15)?, &signed)?;
    let second = plan_same_anchor_exposure_repair(&state, &inventory(2, 15, 15)?, &signed)?;
    assert_eq!(first, second);
    let ExposureLadderRepairPlan::Ready { place, .. } = first else {
        return Err("repair unexpectedly waited for maker replay".into());
    };
    assert_eq!(place.len(), 2);
    assert!(place.iter().any(|order| order.key == missing_open));
    assert!(place.iter().any(|order| order.key == missing_close));
    Ok(())
}

#[test]
fn partial_closing_is_retained_without_chasing_requested_quantity()
-> Result<(), Box<dyn std::error::Error>> {
    let state = running_state()?;
    let partial_key = GridOrderKey {
        epoch: 7,
        position: GridPosition::Long,
        role: GridOrderRole::Close,
        level: 1,
    };
    let mut signed = state.owned_orders.clone();
    signed
        .get_mut(&partial_key)
        .ok_or("missing close")?
        .quantity = Decimal::new(2, 2);

    let ExposureLadderRepairPlan::Ready { target, place, .. } =
        plan_same_anchor_exposure_repair(&state, &inventory(2, 12, 15)?, &signed)?
    else {
        return Err("repair unexpectedly waited for maker replay".into());
    };
    assert_eq!(target[&partial_key].quantity, Decimal::new(2, 2));
    assert!(!place.iter().any(|order| order.key == partial_key));
    assert!(
        target
            .iter()
            .filter(|(key, _)| {
                key.position == GridPosition::Long && key.role == GridOrderRole::Close
            })
            .map(|(_, order)| order.quantity)
            .sum::<Decimal>()
            <= Decimal::new(12, 2)
    );
    Ok(())
}

#[test]
fn signed_minimum_notional_opening_is_not_replaced() -> Result<(), Box<dyn std::error::Error>> {
    let state = running_state()?;
    let opening_key = GridOrderKey {
        epoch: 7,
        position: GridPosition::Long,
        role: GridOrderRole::Open,
        level: 1,
    };
    let mut signed = state.owned_orders.clone();
    signed
        .get_mut(&opening_key)
        .ok_or("missing opening")?
        .quantity = Decimal::new(6, 2);

    let ExposureLadderRepairPlan::Ready { target, place, .. } =
        plan_same_anchor_exposure_repair(&state, &inventory(2, 15, 15)?, &signed)?
    else {
        return Err("repair unexpectedly waited for maker replay".into());
    };
    assert_eq!(target[&opening_key].quantity, Decimal::new(6, 2));
    assert!(!place.iter().any(|order| order.key == opening_key));
    Ok(())
}

#[test]
fn maker_replay_and_awaiting_reanchor_remain_fenced_from_risk_repair()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = running_state()?;
    state.inventory_recovery = InventoryRecoveryState::AwaitingNextOwnedFill {
        armed_generation: 9,
    };
    let awaiting = state.inventory_recovery.clone();
    state.owned_orders.remove(&GridOrderKey {
        epoch: 7,
        position: GridPosition::Long,
        role: GridOrderRole::Open,
        level: 1,
    });

    assert_eq!(
        plan_same_anchor_exposure_repair(&state, &inventory(10, 15, 15)?, &state.owned_orders)?,
        ExposureLadderRepairPlan::AwaitingMakerReplay
    );
    assert_eq!(state.inventory_recovery, awaiting);
    assert_eq!(
        state.epoch.as_ref().map(|epoch| epoch.anchor_price),
        Some(Price::new(Decimal::new(100, 0))?)
    );
    Ok(())
}

#[test]
fn concurrent_maker_transaction_must_settle_before_risk_ladder_repair()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = running_state()?;
    let anchor = state.epoch.as_ref().ok_or("missing epoch")?.anchor_price;
    let source = GridOrderKey {
        epoch: 7,
        position: GridPosition::Long,
        role: GridOrderRole::Open,
        level: 1,
    };
    let decision = state.observe_stream_owned_fill(OwnedGridFill {
        fill_id: "maker-before-risk-settlement".to_owned(),
        private_generation: 2,
        source_order: source,
        fill_price: Price::new(Decimal::new(998, 1))?,
        complete: true,
        maker: FieldState::Known(true),
    })?;
    let transaction_id = match decision {
        crate::strategy::hedged_grid::GridDecision::Actions(actions) => actions
            .into_iter()
            .find_map(|action| match action {
                crate::strategy::hedged_grid::GridAction::Dispatch(transaction) => {
                    Some(transaction.id)
                }
                _ => None,
            })
            .ok_or("missing maker transaction")?,
        _ => return Err("maker did not reserve a transaction".into()),
    };

    assert_eq!(
        plan_same_anchor_exposure_repair(&state, &inventory(2, 15, 15)?, &state.owned_orders)?,
        ExposureLadderRepairPlan::AwaitingMakerReplay
    );
    state.settle_transaction(&transaction_id, true)?;
    assert!(matches!(
        plan_same_anchor_exposure_repair(&state, &inventory(2, 15, 15)?, &state.owned_orders)?,
        ExposureLadderRepairPlan::Ready { .. }
    ));
    assert_eq!(
        state.epoch.as_ref().ok_or("missing epoch")?.anchor_price,
        anchor
    );
    Ok(())
}

#[test]
fn market_reduce_taker_settlement_does_not_roll_anchor_or_consume_awaiting()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = running_state()?;
    state.inventory_recovery = InventoryRecoveryState::AwaitingNextOwnedFill {
        armed_generation: 9,
    };
    let before = state.clone();
    let currency = Asset::new("USDT")?;
    let account = AccountRiskSnapshot {
        exchange: state.binding.exchange.clone(),
        account: state.binding.account.clone(),
        risk_currency: currency.clone(),
        account_equity: Decimal::new(20, 0),
        private_generation: 10,
        observed_at_ms: 1_000,
        source_status: RiskSourceStatus::Complete,
    };
    let leg = LegRiskSnapshot {
        symbol: state.binding.symbol.clone(),
        position_side: PositionSide::Long,
        quantity: Decimal::new(60, 0),
        mark_price: Price::new(Decimal::ONE)?,
        contract_multiplier: Decimal::ONE,
        notional: Decimal::new(60, 0),
        unrealized_pnl: Decimal::new(2, 0),
        risk_currency: currency.clone(),
        private_generation: 10,
        observed_at_ms: 1_000,
    };
    let action = ReduceProfitableExposure {
        risk_episode_id: "etp-l-000000000000000a".to_owned(),
        position: GridPosition::Long,
        trigger_generation: 10,
        position_quantity: leg.quantity,
        position_notional: leg.notional,
        account_equity: account.account_equity,
        unrealized_pnl: leg.unrealized_pnl,
        reduce_ratio: Decimal::new(30, 2),
        risk_currency: currency.clone(),
    };
    let instrument = Instrument {
        symbol: state.binding.symbol.clone(),
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(currency.clone()),
        generation: 1,
        price_tick: Price::new(Decimal::new(1, 4))?,
        quantity_step: Decimal::ONE,
        minimum_notional: Amount::new(currency, Decimal::ONE),
    };
    let MarketReductionPlan::Authorized { command, .. } = plan_market_reduction(
        &state.binding,
        &action,
        &account,
        &leg,
        &instrument,
        1_000,
        3_000,
    )?
    else {
        return Err("risk reduction unexpectedly fell below the venue minimum".into());
    };
    assert_eq!(command.quantity, Decimal::new(18, 0));

    let fill = Fill {
        fill_id: "risk-taker-fill".to_owned(),
        execution_sequence: FieldState::Known(1),
        order_id: command.client_order_id.as_str().to_owned(),
        symbol: state.binding.symbol.clone(),
        side: OrderSide::Sell,
        position_side: FieldState::Known(PositionSide::Long),
        quantity: command.quantity,
        price: Price::new(Decimal::ONE)?,
        fee: FieldState::Missing,
        realized_pnl: FieldState::Missing,
        maker: FieldState::Known(false),
        exchange_time_ms: Some(1_001),
    };
    assert_eq!(route_grid_fill(&fill), GridFillRoute::TakerInventoryOnly);
    let associated = associate_reduction_fill(&command, fill);
    let audit = summarize_reduction_fills(&command, &action, &account, &leg, &[associated], 11)?;
    assert_eq!(audit.executed_reduce_quantity, Decimal::new(18, 0));
    assert_eq!(audit.executed_reduce_notional, Decimal::new(18, 0));

    // Exposure settlement is a separate shared pipeline and cannot mutate grid control state.
    assert_eq!(state, before);
    assert_eq!(
        state.inventory_recovery,
        InventoryRecoveryState::AwaitingNextOwnedFill {
            armed_generation: 9
        }
    );
    Ok(())
}
