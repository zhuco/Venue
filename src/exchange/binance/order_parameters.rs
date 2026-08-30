use crate::domain::{
    MarketOrderCommand, MarketReduceCommand, OrderCommand, StopMarketCloseAllCommand,
    StopMarketFullPositionCommand,
};

use super::{PrivateError, native_symbol, validate_client_order_id};

#[derive(Clone, Copy)]
pub(super) enum LimitTimeInForce {
    GoodTillCancel,
    PostOnly,
    ImmediateOrCancel,
}

impl LimitTimeInForce {
    pub(super) const fn as_papi(self) -> &'static str {
        match self {
            Self::GoodTillCancel => "GTC",
            Self::PostOnly => "GTX",
            Self::ImmediateOrCancel => "IOC",
        }
    }
}

/// PAPI UM hedge orders require a concrete LONG or SHORT `positionSide`. `reduceOnly` is
/// deliberately absent because Binance rejects it in Hedge mode.
pub(super) fn limit_order_parameters(
    command: &OrderCommand,
    time_in_force: LimitTimeInForce,
) -> Result<Vec<(&'static str, String)>, PrivateError> {
    command.validate().map_err(PrivateError::Command)?;
    if command.owner.exchange != "binance" {
        return Err(PrivateError::Owner);
    }
    validate_client_order_id(command.client_order_id.as_str())?;
    let side = match command.side {
        crate::domain::OrderSide::Buy => "BUY",
        crate::domain::OrderSide::Sell => "SELL",
    };
    let position_side = native_position_side(command.position_side)?;
    Ok(vec![
        ("symbol", native_symbol(&command.owner.symbol)),
        ("side", side.to_owned()),
        ("type", "LIMIT".to_owned()),
        ("timeInForce", time_in_force.as_papi().to_owned()),
        ("quantity", command.quantity.to_string()),
        ("price", command.limit_price.value().to_string()),
        ("positionSide", position_side.to_owned()),
        ("newOrderRespType", "RESULT".to_owned()),
        (
            "newClientOrderId",
            command.client_order_id.as_str().to_owned(),
        ),
    ])
}

/// PAPI UM market entries use the same stable client identity and Hedge-mode position side as
/// limit entries, but deliberately omit price, time-in-force, and wire-level reduceOnly.
pub(super) fn market_order_parameters(
    command: &MarketOrderCommand,
) -> Result<Vec<(&'static str, String)>, PrivateError> {
    command.validate().map_err(PrivateError::Command)?;
    if command.owner.exchange != "binance" {
        return Err(PrivateError::Owner);
    }
    validate_client_order_id(command.client_order_id.as_str())?;
    let side = match command.side {
        crate::domain::OrderSide::Buy => "BUY",
        crate::domain::OrderSide::Sell => "SELL",
    };
    let position_side = native_position_side(command.position_side)?;
    Ok(vec![
        ("symbol", native_symbol(&command.owner.symbol)),
        ("side", side.to_owned()),
        ("type", "MARKET".to_owned()),
        ("quantity", command.quantity.to_string()),
        ("positionSide", position_side.to_owned()),
        ("newOrderRespType", "RESULT".to_owned()),
        (
            "newClientOrderId",
            command.client_order_id.as_str().to_owned(),
        ),
    ])
}

pub(super) fn market_reduce_parameters(
    command: &MarketReduceCommand,
) -> Result<Vec<(&'static str, String)>, PrivateError> {
    command.validate().map_err(PrivateError::Command)?;
    if command.owner.exchange != "binance" {
        return Err(PrivateError::Owner);
    }
    validate_client_order_id(command.client_order_id.as_str())?;
    let side = match command.side {
        crate::domain::OrderSide::Buy => "BUY",
        crate::domain::OrderSide::Sell => "SELL",
    };
    Ok(vec![
        ("symbol", native_symbol(&command.owner.symbol)),
        ("side", side.to_owned()),
        ("type", "MARKET".to_owned()),
        ("quantity", command.quantity.to_string()),
        (
            "positionSide",
            native_position_side(command.position_side)?.to_owned(),
        ),
        ("newOrderRespType", "RESULT".to_owned()),
        (
            "newClientOrderId",
            command.client_order_id.as_str().to_owned(),
        ),
    ])
}

pub(super) fn stop_market_close_all_parameters(
    command: &StopMarketCloseAllCommand,
) -> Result<Vec<(&'static str, String)>, PrivateError> {
    let side = match command.side {
        crate::domain::OrderSide::Buy => "BUY",
        crate::domain::OrderSide::Sell => "SELL",
    };
    Ok(vec![
        ("symbol", native_symbol(&command.owner.symbol)),
        ("side", side.to_owned()),
        ("strategyType", "STOP_MARKET".to_owned()),
        ("stopPrice", command.stop_price.value().to_string()),
        ("closePosition", "true".to_owned()),
        (
            "positionSide",
            native_position_side(command.position_side)?.to_owned(),
        ),
        ("workingType", "MARK_PRICE".to_owned()),
        ("priceProtect", "false".to_owned()),
        ("newOrderRespType", "RESULT".to_owned()),
        (
            "newClientStrategyId",
            command.client_strategy_id.as_str().to_owned(),
        ),
    ])
}

pub(super) fn stop_market_full_position_parameters(
    command: &StopMarketFullPositionCommand,
) -> Result<Vec<(&'static str, String)>, PrivateError> {
    let side = match command.side {
        crate::domain::OrderSide::Buy => "BUY",
        crate::domain::OrderSide::Sell => "SELL",
    };
    let order_type = match command.owner.purpose {
        crate::domain::OrderPurpose::Protection => "STOP_MARKET",
        crate::domain::OrderPurpose::TakeProfit => "TAKE_PROFIT_MARKET",
        _ => return Err(PrivateError::Owner),
    };
    Ok(vec![
        ("algoType", "CONDITIONAL".to_owned()),
        ("symbol", native_symbol(&command.owner.symbol)),
        ("side", side.to_owned()),
        ("type", order_type.to_owned()),
        ("quantity", command.quantity.to_string()),
        (
            "positionSide",
            native_position_side(command.position_side)?.to_owned(),
        ),
        ("triggerPrice", command.trigger_price.value().to_string()),
        ("workingType", "MARK_PRICE".to_owned()),
        ("priceProtect", "false".to_owned()),
        ("newOrderRespType", "RESULT".to_owned()),
        ("clientAlgoId", command.client_algo_id.as_str().to_owned()),
    ])
}

fn native_position_side(
    position_side: crate::domain::PositionSide,
) -> Result<&'static str, PrivateError> {
    match position_side {
        crate::domain::PositionSide::Long => Ok("LONG"),
        crate::domain::PositionSide::Short => Ok("SHORT"),
        crate::domain::PositionSide::Net => Err(PrivateError::Command(
            crate::domain::CommandError::PositionSide,
        )),
    }
}
