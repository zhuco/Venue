use std::collections::BTreeMap;

use super::*;
use crate::{
    domain::{CommandId, Fill, OrderCommand},
    execution::{CommandJournal, CommandState},
    strategy::hedged_grid::{GridOrderRole, GridResetReason},
};
use tempfile::tempdir;

#[test]
fn signed_batch_uses_execution_sequence_instead_of_grid_role_order()
-> Result<(), Box<dyn std::error::Error>> {
    let make =
        |fill_id: &str, sequence: u64| -> Result<GridVenueFill, Box<dyn std::error::Error>> {
            Ok(GridVenueFill {
                fill: Fill {
                    execution_sequence: FieldState::Known(sequence),
                    fill_id: fill_id.to_owned(),
                    order_id: format!("order-{fill_id}"),
                    symbol: "DOGE/USDT".parse()?,
                    side: OrderSide::Buy,
                    position_side: FieldState::Known(PositionSide::Long),
                    quantity: Decimal::ONE,
                    price: Price::new(Decimal::ONE)?,
                    fee: FieldState::Missing,
                    realized_pnl: FieldState::Missing,
                    maker: FieldState::Known(true),
                    exchange_time_ms: Some(1),
                },
                client_order_id: FieldState::Missing,
            })
        };
    let later_open = make("later-open", 20)?;
    let earlier_close = make("earlier-close", 10)?;
    let open_key = parse_grid_client_order_id("hgo_e1_long_open_l1")?;
    let close_key = parse_grid_client_order_id("hgo_e1_long_close_l1")?;
    let mut candidates = vec![(&later_open, open_key), (&earlier_close, close_key)];

    sort_grid_fill_candidates_by_execution_sequence(&mut candidates)?;

    assert_eq!(candidates[0].0.fill.fill_id, "earlier-close");
    assert_eq!(candidates[1].0.fill.fill_id, "later-open");
    let duplicate = make("conflicting-sequence", 10)?;
    candidates.push((
        &duplicate,
        parse_grid_client_order_id("hgo_e1_short_open_l1")?,
    ));
    assert!(matches!(
        sort_grid_fill_candidates_by_execution_sequence(&mut candidates),
        Err(Stage7GridError::FillLiquidityUnknown)
    ));
    Ok(())
}

#[test]
fn exact_owned_fill_stops_a_lifecycle_canary_before_low_inventory_replenishment()
-> Result<(), Box<dyn std::error::Error>> {
    let order = intent(1, GridPosition::Long, 1)?;
    let mut owned = BTreeMap::new();
    owned.insert(order.key.clone(), order.clone());
    let mut fill = GridVenueFill {
        fill: Fill {
            execution_sequence: FieldState::Known(1),
            fill_id: "fill-1".to_owned(),
            order_id: "order-1".to_owned(),
            symbol: "DOGE/USDT".parse()?,
            side: OrderSide::Buy,
            position_side: FieldState::Known(PositionSide::Long),
            quantity: order.quantity,
            price: order.price,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(true),
            exchange_time_ms: Some(1),
        },
        client_order_id: FieldState::Known("hgo_e1_long_open_l1".to_owned()),
    };

    assert!(signed_complete_owned_fill_present(&owned, &[fill.clone()]));
    fill.fill.quantity -= Decimal::ONE;
    assert!(!signed_complete_owned_fill_present(&owned, &[fill]));

    let mut first = GridVenueFill {
        fill: Fill {
            execution_sequence: FieldState::Known(1),
            fill_id: "fill-split-1".to_owned(),
            order_id: "order-1".to_owned(),
            symbol: "DOGE/USDT".parse()?,
            side: OrderSide::Buy,
            position_side: FieldState::Known(PositionSide::Long),
            quantity: Decimal::new(2, 0),
            price: order.price,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(true),
            exchange_time_ms: Some(2),
        },
        client_order_id: FieldState::Known("hgo_e1_long_open_l1".to_owned()),
    };
    let mut second = first.clone();
    second.fill.fill_id = "fill-split-2".to_owned();
    second.fill.quantity = Decimal::new(3, 0);
    assert!(signed_complete_owned_fill_present(
        &owned,
        &[first.clone(), second]
    ));
    first.fill.fill_id = "fill-split-2".to_owned();
    assert!(!signed_complete_owned_fill_present(&owned, &[first]));
    Ok(())
}

#[test]
fn signed_fill_without_client_id_uses_durable_accepted_order_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempdir()?;
    let order = intent(1, GridPosition::Long, 1)?;
    let client_order_id = "hgo_e1_long_open_l1";
    let command_id = CommandId::new("place_e1_long_open_l1")?;
    let command = OrderCommand {
        command_id: command_id.clone(),
        client_order_id: CommandId::new(client_order_id)?,
        owner: OrderOwner {
            strategy_instance_id: "hedged_grid".to_owned(),
            run_id: "run_1".to_owned(),
            exchange: "binance".to_owned(),
            account: "portfolio_margin_um".to_owned(),
            symbol: "DOGE/USDT".parse()?,
            purpose: OrderPurpose::Entry,
        },
        side: order.side,
        position_side: PositionSide::Long,
        quantity: order.quantity,
        limit_price: order.price,
        reduce_only: false,
    };
    let mut commands = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
    commands.prepare_place(command)?;
    commands.transition(&command_id, CommandState::Submitted)?;
    commands.transition(
        &command_id,
        CommandState::Accepted {
            venue_order_id: "venue-order-1".to_owned(),
        },
    )?;
    let mut owned = BTreeMap::new();
    owned.insert(order.key.clone(), order.clone());
    let fill = GridVenueFill {
        fill: Fill {
            execution_sequence: FieldState::Known(1),
            fill_id: "fill-without-client-id".to_owned(),
            order_id: "venue-order-1".to_owned(),
            symbol: "DOGE/USDT".parse()?,
            side: order.side,
            position_side: FieldState::Known(PositionSide::Long),
            quantity: order.quantity,
            price: order.price,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(true),
            exchange_time_ms: Some(1),
        },
        client_order_id: FieldState::Missing,
    };

    assert!(signed_complete_owned_fill_present_resolved(
        &owned,
        &commands,
        &[fill]
    ));
    Ok(())
}

#[test]
fn signed_low_inventory_transition_stops_the_bounded_lifecycle_fill_wait() {
    assert!(canary_observed_owned_execution(
        GridPhase::Running,
        &[GridAction::Reset {
            reason: GridResetReason::InventoryLow
        }]
    ));
    assert!(!canary_observed_owned_execution(
        GridPhase::ResettingGrid,
        &[GridAction::Reset {
            reason: GridResetReason::InventoryLow
        }]
    ));
    assert!(!canary_observed_owned_execution(
        GridPhase::Running,
        &[GridAction::Reset {
            reason: GridResetReason::Manual
        }]
    ));
}

pub(super) fn intent(
    epoch: u64,
    position: GridPosition,
    level: u64,
) -> Result<GridOrderIntent, Box<dyn std::error::Error>> {
    let side = position.opening_side();
    Ok(GridOrderIntent {
        key: GridOrderKey {
            epoch,
            position,
            role: GridOrderRole::Open,
            level,
        },
        side,
        price: Price::new(Decimal::new(10, 0) + Decimal::from(level))?,
        quantity: Decimal::new(5, 0),
        reduce_only: false,
    })
}
