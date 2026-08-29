use rust_decimal::Decimal;

use super::*;
use crate::{
    domain::{AccountBalance, Amount, Asset, Fill, MarketReduceCommand, OrderOwner, PositionSide},
    exchange::grid::{GridOrderFamilyReadback, GridVenueFill},
};

fn command() -> Result<MarketReduceCommand, Box<dyn std::error::Error>> {
    Ok(MarketReduceCommand {
        command_id: CommandId::new("cmd-etp-l-1")?,
        client_order_id: CommandId::new("ord-etp-l-1")?,
        owner: OrderOwner {
            strategy_instance_id: "hedged_grid_1".to_owned(),
            run_id: "run_1".to_owned(),
            exchange: "gate".to_owned(),
            account: "usdt_futures".to_owned(),
            symbol: "DOGE/USDT".parse()?,
            purpose: OrderPurpose::ExposureTakeProfit,
        },
        position_side: PositionSide::Long,
        side: OrderSide::Sell,
        quantity: Decimal::new(10, 0),
        risk_episode_id: CommandId::new("etp-l-1")?,
        position_generation: 7,
    })
}

fn readback(fill: GridVenueFill) -> Result<GridVenueReadback, Box<dyn std::error::Error>> {
    Ok(GridVenueReadback {
        raw_private_payloads: vec!["{\"signed\":true}".to_owned()],
        order_family_readback: Some(GridOrderFamilyReadback::regular_only_adapter_profile(
            Vec::new(),
            vec!["[]".to_owned()],
        )?),
        balance: AccountBalance {
            asset: Asset::new("USDT")?,
            wallet_balance: Decimal::new(20, 0),
            available_balance: Decimal::new(20, 0),
            initial_margin: Decimal::ZERO,
            maintenance_margin: Decimal::ZERO,
        },
        hedge_position: true,
        positions: Vec::new(),
        orders: Vec::new(),
        fills: vec![fill],
    })
}

fn fill(
    sequence: u64,
    id: &str,
    quantity: Decimal,
) -> Result<GridVenueFill, Box<dyn std::error::Error>> {
    Ok(GridVenueFill {
        fill: Fill {
            fill_id: id.to_owned(),
            execution_sequence: FieldState::Known(sequence),
            order_id: "venue-risk-1".to_owned(),
            symbol: "DOGE/USDT".parse()?,
            side: OrderSide::Sell,
            position_side: FieldState::Known(PositionSide::Long),
            quantity,
            price: Price::new(Decimal::new(1, 1))?,
            fee: FieldState::Known(Amount::new(Asset::new("USDT")?, Decimal::ZERO)),
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(false),
            exchange_time_ms: Some(sequence),
        },
        client_order_id: FieldState::Known("ord-etp-l-1".to_owned()),
    })
}

#[test]
fn signed_unique_reduction_fills_settle_only_the_same_episode_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let command = command()?;
    let first = fill(1, "risk-fill-1", Decimal::new(2, 0))?;
    let mut facts = readback(first.clone())?;
    facts.fills.push(first);
    facts
        .fills
        .push(fill(2, "risk-fill-2", Decimal::new(3, 0))?);
    assert_eq!(
        exact_market_reduce_fill_recovery(&command, &facts),
        ExactMarketReduceFillRecovery::Proven {
            venue_order_id: "venue-risk-1".to_owned(),
            cumulative_quantity: Decimal::new(5, 0),
        }
    );

    let mut conflict = facts.clone();
    conflict.fills[1].fill.quantity = Decimal::new(4, 0);
    assert_eq!(
        exact_market_reduce_fill_recovery(&command, &conflict),
        ExactMarketReduceFillRecovery::Conflicting
    );
    let mut wrong_identity = facts.clone();
    wrong_identity.fills[2].client_order_id = FieldState::Known("other-episode".to_owned());
    assert_eq!(
        exact_market_reduce_fill_recovery(&command, &wrong_identity),
        ExactMarketReduceFillRecovery::Conflicting
    );
    let mut overfill = facts.clone();
    overfill.fills[2].fill.quantity = Decimal::new(9, 0);
    assert_eq!(
        exact_market_reduce_fill_recovery(&command, &overfill),
        ExactMarketReduceFillRecovery::Conflicting
    );
    let mut wrong_purpose = command;
    wrong_purpose.owner.purpose = OrderPurpose::Reduce;
    assert_eq!(
        exact_market_reduce_fill_recovery(&wrong_purpose, &facts),
        ExactMarketReduceFillRecovery::Conflicting
    );
    Ok(())
}
