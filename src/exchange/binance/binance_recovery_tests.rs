use rust_decimal::Decimal;

use super::{LimitTimeInForce, limit_order_parameters, parse_depth_best_prices};
use crate::domain::{
    CommandId, OrderCommand, OrderOwner, OrderPurpose, OrderSide, PositionSide, Price,
};

#[test]
fn post_only_limit_parameters_keep_hedge_identity_without_reduce_only()
-> Result<(), Box<dyn std::error::Error>> {
    for (position_side, expected_position_side, side) in [
        (PositionSide::Long, "LONG", OrderSide::Buy),
        (PositionSide::Short, "SHORT", OrderSide::Sell),
    ] {
        let command = OrderCommand {
            command_id: CommandId::new("post_only_command")?,
            client_order_id: CommandId::new("venue_post_only_1")?,
            owner: OrderOwner {
                strategy_instance_id: "scalping_probe".to_owned(),
                run_id: "canary_1".to_owned(),
                exchange: "binance".to_owned(),
                account: "primary".to_owned(),
                symbol: "DOGE/USDT".parse()?,
                purpose: OrderPurpose::Entry,
            },
            side,
            position_side,
            quantity: Decimal::new(50, 0),
            limit_price: Price::new(Decimal::new(1, 1))?,
            reduce_only: false,
        };
        let post_only = limit_order_parameters(&command, LimitTimeInForce::PostOnly)?;
        assert!(post_only.contains(&("timeInForce", "GTX".to_owned())));
        assert!(post_only.contains(&("positionSide", expected_position_side.to_owned())));
        assert!(post_only.contains(&("newClientOrderId", "venue_post_only_1".to_owned())));
        assert!(!post_only.iter().any(|(key, _)| *key == "reduceOnly"));

        let ordinary = limit_order_parameters(&command, LimitTimeInForce::GoodTillCancel)?;
        assert!(ordinary.contains(&("timeInForce", "GTC".to_owned())));
        assert_eq!(LimitTimeInForce::ImmediateOrCancel.as_papi(), "IOC");
    }
    Ok(())
}

#[test]
fn recovery_reduce_parameters_are_ioc_and_keep_explicit_hedge_reduction()
-> Result<(), Box<dyn std::error::Error>> {
    let command = OrderCommand {
        command_id: CommandId::new("recovery_reduce_1")?,
        client_order_id: CommandId::new("vrr_1")?,
        owner: OrderOwner {
            strategy_instance_id: "canary_recovery".to_owned(),
            run_id: "recovery_run_1".to_owned(),
            exchange: "binance".to_owned(),
            account: "portfolio_margin_um".to_owned(),
            symbol: "SOL/USDT".parse()?,
            purpose: OrderPurpose::Reduce,
        },
        side: OrderSide::Sell,
        position_side: PositionSide::Long,
        quantity: Decimal::new(5, 2),
        limit_price: Price::new(Decimal::new(100, 0))?,
        reduce_only: true,
    };
    let parameters = limit_order_parameters(&command, LimitTimeInForce::ImmediateOrCancel)?;
    assert!(parameters.contains(&("timeInForce", "IOC".to_owned())));
    assert!(parameters.contains(&("positionSide", "LONG".to_owned())));
    assert!(!parameters.iter().any(|(key, _)| *key == "reduceOnly"));
    assert!(parameters.contains(&("newClientOrderId", "vrr_1".to_owned())));
    Ok(())
}

#[test]
fn bounded_depth_parser_returns_only_a_non_crossed_top() -> Result<(), Box<dyn std::error::Error>> {
    let (bid, ask) =
        parse_depth_best_prices(r#"{"lastUpdateId":1,"bids":[["99","2"]],"asks":[["101","3"]]}"#)?;
    assert_eq!(bid.value(), Decimal::new(99, 0));
    assert_eq!(ask.value(), Decimal::new(101, 0));
    assert!(parse_depth_best_prices(r#"{"bids":[["101","2"]],"asks":[["100","3"]]}"#).is_err());
    Ok(())
}
