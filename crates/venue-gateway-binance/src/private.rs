use std::{collections::BTreeSet, str::FromStr};

use rust_decimal::Decimal;
use serde_json::{Map, Value};

use venue_domain::domain::{
    AccountBalance, Amount, Asset, FieldState, Fill, Order, OrderSide, OrderState, Position,
    PositionSide, Price, Symbol, UnknownReason,
};

#[path = "fill_pagination.rs"]
mod fill_pagination;
pub use fill_pagination::{
    RecentFillsCursor, RecentFillsPageRequest, RecentFillsPaginationError, RecentFillsReadback,
    USER_TRADES_PAGE_LIMIT, paginate_recent_fills,
};

/// Normalized private readback data. It is not authoritative until its caller journals it as
/// generation-fenced private facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrivateReadback {
    pub capabilities: PrivateAccountCapabilities,
    pub balances: Vec<AccountBalance>,
    pub positions: Vec<Position>,
    pub orders: Vec<Order>,
    pub fills: Vec<Fill>,
}

/// One execution from the PAPI UM private stream. The client identity remains adjacent to the
/// normalized fill so the shared runtime can prove ownership without re-parsing native JSON.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamFill {
    pub fill: Fill,
    pub client_order_id: FieldState<String>,
}

/// Explicit signed account capabilities required before an exchange-side protection command can
/// be sent. Unknown fields are never interpreted as permission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrivateAccountCapabilities {
    pub can_trade: bool,
    pub one_way_position: bool,
    pub hedge_position: bool,
}

/// PAPI conditional strategies are not physical UM orders. Their status is deliberately a
/// separate normalized value so execution cannot treat a trigger or finish as a cancellation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConditionalStrategyStatus {
    Current,
    Cancelled,
    NonCancelledTerminal,
    Rejected,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConditionalStrategyReadback {
    pub strategy_id: String,
    pub status: ConditionalStrategyStatus,
    pub side: FieldState<OrderSide>,
    pub position_side: FieldState<PositionSide>,
    pub stop_price: FieldState<Price>,
    pub close_position: FieldState<bool>,
}

/// Normalized identity and safety fields for the current PAPI UM Algo conditional family.
/// It stays separate from both physical UM orders and the legacy conditional-strategy family.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlgoOrderReadback {
    pub algo_id: String,
    pub client_algo_id: String,
    pub status: ConditionalStrategyStatus,
    pub order_type: FieldState<String>,
    pub side: FieldState<OrderSide>,
    pub position_side: FieldState<PositionSide>,
    pub quantity: FieldState<Decimal>,
    pub trigger_price: FieldState<Price>,
    pub working_type: FieldState<String>,
    pub close_position: FieldState<bool>,
    pub reduce_only: FieldState<bool>,
}

pub fn parse_account_capabilities(
    account_payload: &str,
    position_mode_payload: &str,
) -> Result<PrivateAccountCapabilities, PrivateParseError> {
    let can_trade = parse_can_trade(account_payload)?;
    let one_way_position = parse_portfolio_position_mode(position_mode_payload)?;
    Ok(PrivateAccountCapabilities {
        can_trade,
        one_way_position,
        hedge_position: !one_way_position,
    })
}

pub fn parse_can_trade(payload: &str) -> Result<bool, PrivateParseError> {
    let account: Value = serde_json::from_str(payload).map_err(|_| PrivateParseError::Payload)?;
    account
        .as_object()
        .and_then(|object| object.get("canTrade"))
        .and_then(Value::as_bool)
        .ok_or(PrivateParseError::Capability)
}

/// Parses only the PAPI conditional strategy identity. Conditional payloads are deliberately not
/// normalized as physical UM orders, because they can still be waiting for a trigger.
pub fn parse_conditional_strategy_id(
    payload: &str,
    expected_client_strategy_id: &str,
) -> Result<String, PrivateParseError> {
    let readback = parse_conditional_strategy(payload, expected_client_strategy_id)?;
    match readback.status {
        ConditionalStrategyStatus::Current
        | ConditionalStrategyStatus::Cancelled
        | ConditionalStrategyStatus::NonCancelledTerminal => Ok(readback.strategy_id),
        ConditionalStrategyStatus::Rejected | ConditionalStrategyStatus::Unknown => {
            Err(PrivateParseError::Payload)
        }
    }
}

/// Parses only the identity and lifecycle state shared by PAPI's current and history strategy
/// endpoints. A client ID mismatch is never accepted as evidence for a WAL command.
pub fn parse_conditional_strategy(
    payload: &str,
    expected_client_strategy_id: &str,
) -> Result<ConditionalStrategyReadback, PrivateParseError> {
    let value: Value = serde_json::from_str(payload).map_err(|_| PrivateParseError::Payload)?;
    let item = object(&value)?;
    let client_id = item
        .get("newClientStrategyId")
        .and_then(Value::as_str)
        .ok_or(PrivateParseError::Payload)?;
    if client_id != expected_client_strategy_id {
        return Err(PrivateParseError::Payload);
    }
    let strategy_id = match item.get("strategyId") {
        Some(Value::String(value)) if !value.is_empty() => value.clone(),
        Some(Value::Number(value)) => value.to_string(),
        _ => return Err(PrivateParseError::Payload),
    };
    let status = match item
        .get("strategyStatus")
        .and_then(Value::as_str)
        .ok_or(PrivateParseError::Payload)?
    {
        "NEW" => ConditionalStrategyStatus::Current,
        "CANCELED" | "CANCELLED" => ConditionalStrategyStatus::Cancelled,
        "TRIGGERED" | "FINISHED" | "EXPIRED" => ConditionalStrategyStatus::NonCancelledTerminal,
        "REJECTED" => ConditionalStrategyStatus::Rejected,
        _ => ConditionalStrategyStatus::Unknown,
    };
    Ok(ConditionalStrategyReadback {
        strategy_id,
        status,
        side: optional_order_side(item.get("side")),
        position_side: optional_position_side(item.get("positionSide")),
        stop_price: optional_price(item.get("stopPrice").or_else(|| item.get("triggerPrice"))),
        close_position: optional_bool(item.get("closePosition")),
    })
}

/// Parses either a single Algo response/current readback or an Algo history array and selects
/// exactly the caller-owned client identity. Missing and duplicate identities fail closed.
pub fn parse_algo_order(
    payload: &str,
    expected_symbol: &Symbol,
    expected_client_algo_id: &str,
) -> Result<AlgoOrderReadback, PrivateParseError> {
    let value: Value = serde_json::from_str(payload).map_err(|_| PrivateParseError::Payload)?;
    let selected = match &value {
        Value::Object(_) => &value,
        Value::Array(items) => {
            let mut matching = items.iter().filter(|item| {
                item.as_object()
                    .and_then(|object| object.get("clientAlgoId"))
                    .and_then(Value::as_str)
                    == Some(expected_client_algo_id)
            });
            let item = matching.next().ok_or(PrivateParseError::Payload)?;
            if matching.next().is_some() {
                return Err(PrivateParseError::Payload);
            }
            item
        }
        _ => return Err(PrivateParseError::Payload),
    };
    let item = object(selected)?;
    check_symbol(item, expected_symbol)?;
    let client_algo_id = text(item, "clientAlgoId")?;
    if client_algo_id != expected_client_algo_id {
        return Err(PrivateParseError::Payload);
    }
    let status = match text(item, "algoStatus")? {
        "NEW" | "ACTIVE" => ConditionalStrategyStatus::Current,
        "CANCELED" | "CANCELLED" => ConditionalStrategyStatus::Cancelled,
        "TRIGGERED" | "FINISHED" | "EXPIRED" => ConditionalStrategyStatus::NonCancelledTerminal,
        "REJECTED" => ConditionalStrategyStatus::Rejected,
        _ => ConditionalStrategyStatus::Unknown,
    };
    Ok(AlgoOrderReadback {
        algo_id: identifier(item, "algoId")?,
        client_algo_id: client_algo_id.to_owned(),
        status,
        order_type: optional_text(item.get("orderType")),
        side: optional_order_side(item.get("side")),
        position_side: optional_position_side(item.get("positionSide")),
        quantity: optional_decimal(item.get("quantity")),
        trigger_price: optional_price(item.get("triggerPrice")),
        working_type: optional_text(item.get("workingType")),
        close_position: optional_bool(item.get("closePosition")),
        reduce_only: optional_bool(item.get("reduceOnly")),
    })
}

/// Parses the complete open-Algo collection for one normalized symbol and returns only stable
/// client identities. Recovery resolves ownership through its durable command journal; raw Algo
/// protocol fields do not escape the exchange adapter.
pub fn parse_open_algo_client_ids(
    payload: &str,
    expected_symbol: &Symbol,
) -> Result<Vec<String>, PrivateParseError> {
    let value: Value = serde_json::from_str(payload).map_err(|_| PrivateParseError::Payload)?;
    let items = value.as_array().ok_or(PrivateParseError::Payload)?;
    let mut identities = Vec::with_capacity(items.len());
    for value in items {
        let item = object(value)?;
        check_symbol(item, expected_symbol)?;
        if !matches!(text(item, "algoStatus")?, "NEW" | "ACTIVE") {
            return Err(PrivateParseError::Payload);
        }
        let client_algo_id = text(item, "clientAlgoId")?;
        if client_algo_id.trim().is_empty() {
            return Err(PrivateParseError::Payload);
        }
        identities.push(client_algo_id.to_owned());
    }
    identities.sort_unstable();
    if identities.windows(2).any(|window| window[0] == window[1]) {
        return Err(PrivateParseError::Payload);
    }
    Ok(identities)
}

/// Parses the complete current Algo collection when a caller needs custody fields rather than
/// ownership identities alone. Each item is selected again through `parse_algo_order`, preserving
/// the same symbol, duplicate-identity, status, and field-state validation as exact readback.
pub fn parse_open_algo_orders(
    payload: &str,
    expected_symbol: &Symbol,
) -> Result<Vec<AlgoOrderReadback>, PrivateParseError> {
    let client_ids = parse_open_algo_client_ids(payload, expected_symbol)?;
    client_ids
        .iter()
        .map(|client_id| parse_algo_order(payload, expected_symbol, client_id))
        .collect()
}

/// The PAPI collection endpoint is the only admissible source for the full current conditional
/// family. Every row is normalized enough to identify custody; a close-all strategy without a
/// concrete quantity is intentionally rejected instead of being invented as a physical order.
pub fn parse_open_conditional_orders(
    payload: &str,
    expected_symbol: &Symbol,
) -> Result<Vec<Order>, PrivateParseError> {
    let mut order_ids = BTreeSet::new();
    let mut client_ids = BTreeSet::new();
    array(payload)?
        .iter()
        .map(|value| {
            let item = object(value)?;
            check_symbol(item, expected_symbol)?;
            if text(item, "strategyStatus")? != "NEW" {
                return Err(PrivateParseError::Payload);
            }
            let order_id = identifier(item, "strategyId")?;
            let client_order_id = text(item, "newClientStrategyId")?.to_owned();
            if !order_ids.insert(order_id.clone()) || !client_ids.insert(client_order_id.clone()) {
                return Err(PrivateParseError::Payload);
            }
            let side = parse_order_side(text(item, "side")?)?;
            let position_side = required_position_side(item.get("positionSide"))?;
            let reduce_only = required_reduce_only(item)?;
            let order = Order {
                order_id,
                client_order_id: FieldState::Known(client_order_id),
                symbol: expected_symbol.clone(),
                side,
                position_side: FieldState::Known(position_side),
                purpose: FieldState::Missing,
                state: OrderState::New,
                quantity: positive_decimal(item, "quantity")?,
                filled_quantity: Decimal::ZERO,
                limit_price: optional_limit_price(item.get("price"))?,
                average_price: FieldState::Missing,
                reduce_only,
            };
            order
                .validate()
                .map_err(PrivateParseError::OrderValidation)?;
            Ok(order)
        })
        .collect()
}

/// Algo rows are separately signed and carry a distinct identity namespace. They are converted
/// only after the existing exact parser has validated symbol, lifecycle, and duplicate IDs.
pub fn parse_open_algo_order_facts(
    payload: &str,
    expected_symbol: &Symbol,
) -> Result<Vec<Order>, PrivateParseError> {
    parse_open_algo_orders(payload, expected_symbol)?
        .into_iter()
        .map(|algo| {
            let side = known_order_side(algo.side)?;
            let position_side = known_position_side(algo.position_side)?;
            let quantity = known_positive_decimal(algo.quantity)?;
            let reduce_only = known_bool(algo.reduce_only)? || known_bool(algo.close_position)?;
            let order = Order {
                order_id: algo.algo_id,
                client_order_id: FieldState::Known(algo.client_algo_id),
                symbol: expected_symbol.clone(),
                side,
                position_side: FieldState::Known(position_side),
                purpose: FieldState::Missing,
                state: OrderState::New,
                quantity,
                filled_quantity: Decimal::ZERO,
                limit_price: None,
                average_price: FieldState::Missing,
                reduce_only,
            };
            order
                .validate()
                .map_err(PrivateParseError::OrderValidation)?;
            Ok(order)
        })
        .collect()
}

/// Parses the PAPI UM position-mode response without inferring trade permission from it.
pub fn parse_portfolio_position_mode(payload: &str) -> Result<bool, PrivateParseError> {
    let position_mode: Value =
        serde_json::from_str(payload).map_err(|_| PrivateParseError::Payload)?;
    let dual_side = position_mode
        .as_object()
        .and_then(|object| object.get("dualSidePosition"))
        .and_then(Value::as_bool)
        .ok_or(PrivateParseError::Capability)?;
    Ok(!dual_side)
}

pub fn parse_account(payload: &str) -> Result<Vec<AccountBalance>, PrivateParseError> {
    let root: Value = serde_json::from_str(payload).map_err(|_| PrivateParseError::Payload)?;
    let assets = object(&root)?
        .get("assets")
        .and_then(Value::as_array)
        .ok_or(PrivateParseError::Payload)?;
    assets.iter().map(balance).collect()
}

pub fn parse_positions(payload: &str, symbol: &Symbol) -> Result<Vec<Position>, PrivateParseError> {
    array(payload)?
        .iter()
        .map(|value| position(value, symbol))
        .collect()
}

pub fn parse_orders(payload: &str, symbol: &Symbol) -> Result<Vec<Order>, PrivateParseError> {
    array(payload)?
        .iter()
        .map(|value| order(value, symbol))
        .collect()
}

pub fn parse_order(payload: &str, symbol: &Symbol) -> Result<Order, PrivateParseError> {
    let root: Value = serde_json::from_str(payload).map_err(|_| PrivateParseError::Payload)?;
    order(&root, symbol)
}

pub fn parse_fills(payload: &str, symbol: &Symbol) -> Result<Vec<Fill>, PrivateParseError> {
    array(payload)?
        .iter()
        .map(|value| fill(value, symbol))
        .collect()
}

/// Parses only an actual TRADE execution. NEW/CANCELED/order-only updates are not fills. PAPI's
/// `m` flag is authoritative maker evidence; an absent or malformed flag remains unknown.
pub fn parse_stream_fill(
    payload: &str,
    symbol: &Symbol,
) -> Result<Option<StreamFill>, PrivateParseError> {
    let root: Value = serde_json::from_str(payload).map_err(|_| PrivateParseError::Payload)?;
    let event = object(&root)?;
    if text(event, "e")? != "ORDER_TRADE_UPDATE" {
        return Ok(None);
    }
    let order = event
        .get("o")
        .and_then(Value::as_object)
        .ok_or(PrivateParseError::Payload)?;
    if text(order, "x")? != "TRADE" {
        return Ok(None);
    }
    check_symbol_key(order, "s", symbol)?;
    let side = match text(order, "S")? {
        "BUY" => OrderSide::Buy,
        "SELL" => OrderSide::Sell,
        _ => return Err(PrivateParseError::Fill),
    };
    let fee = amount_state(order.get("n"), order.get("N"))?;
    let realized_pnl = amount_state(order.get("rp"), order.get("ma"))?;
    let fill_id = identifier(order, "t")?;
    let fill = Fill {
        execution_sequence: execution_sequence(&fill_id),
        fill_id,
        order_id: identifier(order, "i")?,
        symbol: symbol.clone(),
        side,
        position_side: optional_position_side(order.get("ps")),
        quantity: decimal(order, "l")?,
        price: price(order, "L")?,
        fee,
        realized_pnl,
        maker: optional_bool(order.get("m")),
        exchange_time_ms: optional_u64(event.get("T").or_else(|| event.get("E")))?,
    };
    fill.validate()
        .map_err(PrivateParseError::OrderValidation)?;
    Ok(Some(StreamFill {
        fill,
        client_order_id: optional_text(order.get("c")),
    }))
}

fn array(payload: &str) -> Result<Vec<Value>, PrivateParseError> {
    let value: Value = serde_json::from_str(payload).map_err(|_| PrivateParseError::Payload)?;
    value.as_array().cloned().ok_or(PrivateParseError::Payload)
}

fn balance(value: &Value) -> Result<AccountBalance, PrivateParseError> {
    let item = object(value)?;
    let result = AccountBalance {
        asset: text(item, "asset")?
            .parse()
            .map_err(|_| PrivateParseError::Payload)?,
        wallet_balance: decimal(item, "walletBalance")?,
        available_balance: decimal(item, "availableBalance")?,
        initial_margin: decimal(item, "initialMargin")?,
        maintenance_margin: decimal(item, "maintMargin")?,
    };
    result.validate().map_err(PrivateParseError::Account)?;
    Ok(result)
}

fn position(value: &Value, expected: &Symbol) -> Result<Position, PrivateParseError> {
    let item = object(value)?;
    check_symbol(item, expected)?;
    let quantity = decimal(item, "positionAmt")?;
    let side = match text(item, "positionSide")? {
        "BOTH" if quantity.is_sign_positive() => PositionSide::Long,
        "BOTH" if quantity.is_sign_negative() => PositionSide::Short,
        "BOTH" => PositionSide::Net,
        "LONG" => PositionSide::Long,
        "SHORT" => PositionSide::Short,
        _ => return Err(PrivateParseError::Position),
    };
    Ok(Position {
        symbol: expected.clone(),
        side,
        quantity: quantity.abs(),
        entry_price: known_price(item.get("entryPrice")),
        mark_price: known_price(item.get("markPrice")),
    })
}

fn order(value: &Value, expected: &Symbol) -> Result<Order, PrivateParseError> {
    let item = object(value)?;
    check_symbol(item, expected)?;
    let state = match text(item, "status")? {
        "NEW" => OrderState::New,
        "PARTIALLY_FILLED" => OrderState::PartiallyFilled,
        "FILLED" => OrderState::Filled,
        "CANCELED" | "CANCELLED" => OrderState::Cancelled,
        "EXPIRED" | "EXPIRED_IN_MATCH" => OrderState::Expired,
        "REJECTED" => OrderState::Rejected,
        _ => OrderState::Unknown,
    };
    let side = match text(item, "side")? {
        "BUY" => OrderSide::Buy,
        "SELL" => OrderSide::Sell,
        _ => return Err(PrivateParseError::Order),
    };
    let result = Order {
        order_id: identifier(item, "orderId")?,
        client_order_id: optional_text(item.get("clientOrderId")),
        symbol: expected.clone(),
        side,
        position_side: optional_position_side(item.get("positionSide")),
        purpose: FieldState::Missing,
        state,
        quantity: decimal(item, "origQty")?,
        filled_quantity: decimal(item, "executedQty")?,
        limit_price: known_price(item.get("price")),
        average_price: optional_price(item.get("avgPrice")),
        reduce_only: bool_value(item, "reduceOnly")?,
    };
    result
        .validate()
        .map_err(PrivateParseError::OrderValidation)?;
    Ok(result)
}

fn fill(value: &Value, expected: &Symbol) -> Result<Fill, PrivateParseError> {
    let item = object(value)?;
    check_symbol(item, expected)?;
    let side = match text(item, "side")? {
        "BUY" => OrderSide::Buy,
        "SELL" => OrderSide::Sell,
        _ => return Err(PrivateParseError::Fill),
    };
    let fee = amount_state(item.get("commission"), item.get("commissionAsset"))?;
    let realized_pnl = amount_state(item.get("realizedPnl"), item.get("marginAsset"))?;
    let fill_id = identifier(item, "id")?;
    let result = Fill {
        execution_sequence: execution_sequence(&fill_id),
        fill_id,
        order_id: identifier(item, "orderId")?,
        symbol: expected.clone(),
        side,
        position_side: optional_position_side(item.get("positionSide")),
        quantity: decimal(item, "qty")?,
        price: price(item, "price")?,
        fee,
        realized_pnl,
        maker: optional_bool(item.get("maker")),
        exchange_time_ms: optional_u64(item.get("time"))?,
    };
    result
        .validate()
        .map_err(PrivateParseError::OrderValidation)?;
    Ok(result)
}

fn execution_sequence(fill_id: &str) -> FieldState<u64> {
    fill_id
        .parse::<u64>()
        .map(FieldState::Known)
        .unwrap_or(FieldState::Unavailable {
            reason: UnknownReason::ParseFailure,
        })
}

fn optional_position_side(value: Option<&Value>) -> FieldState<PositionSide> {
    match value {
        None => FieldState::Missing,
        Some(Value::Null) => FieldState::Null,
        Some(Value::String(raw)) => match raw.as_str() {
            "BOTH" => FieldState::Known(PositionSide::Net),
            "LONG" => FieldState::Known(PositionSide::Long),
            "SHORT" => FieldState::Known(PositionSide::Short),
            _ => FieldState::Unavailable {
                reason: UnknownReason::Ambiguous,
            },
        },
        Some(_) => FieldState::Unavailable {
            reason: UnknownReason::ParseFailure,
        },
    }
}

fn optional_order_side(value: Option<&Value>) -> FieldState<OrderSide> {
    match value {
        None => FieldState::Missing,
        Some(Value::Null) => FieldState::Null,
        Some(Value::String(raw)) => match raw.as_str() {
            "BUY" => FieldState::Known(OrderSide::Buy),
            "SELL" => FieldState::Known(OrderSide::Sell),
            _ => FieldState::Unavailable {
                reason: UnknownReason::Ambiguous,
            },
        },
        Some(_) => FieldState::Unavailable {
            reason: UnknownReason::ParseFailure,
        },
    }
}

fn object(value: &Value) -> Result<&Map<String, Value>, PrivateParseError> {
    value.as_object().ok_or(PrivateParseError::Payload)
}

fn text<'a>(item: &'a Map<String, Value>, field: &str) -> Result<&'a str, PrivateParseError> {
    item.get(field)
        .and_then(Value::as_str)
        .ok_or(PrivateParseError::Payload)
}

fn decimal(item: &Map<String, Value>, field: &str) -> Result<Decimal, PrivateParseError> {
    decimal_value(item.get(field).ok_or(PrivateParseError::Payload)?)
}

fn decimal_value(value: &Value) -> Result<Decimal, PrivateParseError> {
    match value {
        Value::String(raw) => Decimal::from_str(raw).map_err(|_| PrivateParseError::Payload),
        Value::Number(raw) => {
            Decimal::from_str(&raw.to_string()).map_err(|_| PrivateParseError::Payload)
        }
        _ => Err(PrivateParseError::Payload),
    }
}

fn identifier(item: &Map<String, Value>, field: &str) -> Result<String, PrivateParseError> {
    match item.get(field) {
        Some(Value::String(value)) => Ok(value.clone()),
        Some(Value::Number(value)) if value.as_u64().is_some() => Ok(value.to_string()),
        _ => Err(PrivateParseError::Payload),
    }
}

fn price(item: &Map<String, Value>, field: &str) -> Result<Price, PrivateParseError> {
    Price::new(decimal(item, field)?).map_err(|_| PrivateParseError::Payload)
}

fn check_symbol(item: &Map<String, Value>, expected: &Symbol) -> Result<(), PrivateParseError> {
    check_symbol_key(item, "symbol", expected)
}

fn check_symbol_key(
    item: &Map<String, Value>,
    field: &str,
    expected: &Symbol,
) -> Result<(), PrivateParseError> {
    if text(item, field)? == crate::native_symbol(expected) {
        Ok(())
    } else {
        Err(PrivateParseError::Symbol)
    }
}

fn bool_value(item: &Map<String, Value>, field: &str) -> Result<bool, PrivateParseError> {
    item.get(field)
        .and_then(Value::as_bool)
        .ok_or(PrivateParseError::Payload)
}

fn optional_u64(value: Option<&Value>) -> Result<Option<u64>, PrivateParseError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value.as_u64().map(Some).ok_or(PrivateParseError::Payload),
        Some(Value::String(value)) => value
            .parse::<u64>()
            .map(Some)
            .map_err(|_| PrivateParseError::Payload),
        Some(_) => Err(PrivateParseError::Payload),
    }
}

fn optional_text(value: Option<&Value>) -> FieldState<String> {
    match value {
        None => FieldState::Missing,
        Some(Value::Null) => FieldState::Null,
        Some(Value::String(value)) if !value.is_empty() => FieldState::Known(value.clone()),
        Some(_) => FieldState::Unavailable {
            reason: UnknownReason::ParseFailure,
        },
    }
}

fn optional_bool(value: Option<&Value>) -> FieldState<bool> {
    match value {
        None => FieldState::Missing,
        Some(Value::Null) => FieldState::Null,
        Some(Value::Bool(value)) => FieldState::Known(*value),
        Some(_) => FieldState::Unavailable {
            reason: UnknownReason::ParseFailure,
        },
    }
}

fn parse_order_side(value: &str) -> Result<OrderSide, PrivateParseError> {
    match value {
        "BUY" => Ok(OrderSide::Buy),
        "SELL" => Ok(OrderSide::Sell),
        _ => Err(PrivateParseError::Payload),
    }
}

fn required_position_side(value: Option<&Value>) -> Result<PositionSide, PrivateParseError> {
    known_position_side(optional_position_side(value))
}

fn known_position_side(value: FieldState<PositionSide>) -> Result<PositionSide, PrivateParseError> {
    match value {
        FieldState::Known(value) => Ok(value),
        FieldState::Missing
        | FieldState::Null
        | FieldState::Unavailable { .. }
        | FieldState::NotApplicable => Err(PrivateParseError::Payload),
    }
}

fn known_order_side(value: FieldState<OrderSide>) -> Result<OrderSide, PrivateParseError> {
    match value {
        FieldState::Known(value) => Ok(value),
        FieldState::Missing
        | FieldState::Null
        | FieldState::Unavailable { .. }
        | FieldState::NotApplicable => Err(PrivateParseError::Payload),
    }
}

fn known_positive_decimal(value: FieldState<Decimal>) -> Result<Decimal, PrivateParseError> {
    match value {
        FieldState::Known(value) if value.is_sign_positive() && !value.is_zero() => Ok(value),
        FieldState::Known(_)
        | FieldState::Missing
        | FieldState::Null
        | FieldState::Unavailable { .. }
        | FieldState::NotApplicable => Err(PrivateParseError::Payload),
    }
}

fn known_bool(value: FieldState<bool>) -> Result<bool, PrivateParseError> {
    match value {
        FieldState::Known(value) => Ok(value),
        FieldState::Missing
        | FieldState::Null
        | FieldState::Unavailable { .. }
        | FieldState::NotApplicable => Err(PrivateParseError::Payload),
    }
}

fn positive_decimal(item: &Map<String, Value>, field: &str) -> Result<Decimal, PrivateParseError> {
    let value = decimal(item, field)?;
    if value.is_sign_positive() && !value.is_zero() {
        Ok(value)
    } else {
        Err(PrivateParseError::Payload)
    }
}

fn optional_limit_price(value: Option<&Value>) -> Result<Option<Price>, PrivateParseError> {
    match optional_price(value) {
        FieldState::Known(value) => Ok(Some(value)),
        FieldState::Missing | FieldState::Null => Ok(None),
        FieldState::Unavailable { .. } | FieldState::NotApplicable => {
            Err(PrivateParseError::Payload)
        }
    }
}

fn required_reduce_only(item: &Map<String, Value>) -> Result<bool, PrivateParseError> {
    let reduce_only = optional_bool(item.get("reduceOnly"));
    let close_position = optional_bool(item.get("closePosition"));
    match (reduce_only, close_position) {
        (FieldState::Known(reduce_only), FieldState::Known(close_position)) => {
            Ok(reduce_only || close_position)
        }
        (FieldState::Known(reduce_only), FieldState::Missing | FieldState::Null) => Ok(reduce_only),
        (FieldState::Missing | FieldState::Null, FieldState::Known(close_position)) => {
            Ok(close_position)
        }
        _ => Err(PrivateParseError::Payload),
    }
}

fn optional_decimal(value: Option<&Value>) -> FieldState<Decimal> {
    match value {
        None => FieldState::Missing,
        Some(Value::Null) => FieldState::Null,
        Some(value @ (Value::String(_) | Value::Number(_))) => match decimal_value(value) {
            Ok(value) => FieldState::Known(value),
            Err(_) => FieldState::Unavailable {
                reason: UnknownReason::ParseFailure,
            },
        },
        Some(_) => FieldState::Unavailable {
            reason: UnknownReason::ParseFailure,
        },
    }
}

fn known_price(value: Option<&Value>) -> Option<Price> {
    match optional_price(value) {
        FieldState::Known(price) => Some(price),
        FieldState::Missing
        | FieldState::Null
        | FieldState::Unavailable { .. }
        | FieldState::NotApplicable => None,
    }
}

fn optional_price(value: Option<&Value>) -> FieldState<Price> {
    match value {
        None => FieldState::Missing,
        Some(Value::Null) => FieldState::Null,
        Some(value @ (Value::String(_) | Value::Number(_))) => match decimal_value(value) {
            Ok(value) if value.is_sign_positive() && !value.is_zero() => Price::new(value)
                .map(FieldState::Known)
                .unwrap_or(FieldState::Unavailable {
                    reason: UnknownReason::ParseFailure,
                }),
            Ok(_) => FieldState::Unavailable {
                reason: UnknownReason::NotYetObserved,
            },
            Err(_) => FieldState::Unavailable {
                reason: UnknownReason::ParseFailure,
            },
        },
        Some(_) => FieldState::Unavailable {
            reason: UnknownReason::ParseFailure,
        },
    }
}

fn amount_state(
    value: Option<&Value>,
    asset: Option<&Value>,
) -> Result<FieldState<Amount>, PrivateParseError> {
    match (value, asset) {
        (None, _) | (_, None) => Ok(FieldState::Missing),
        (Some(Value::Null), _) | (_, Some(Value::Null)) => Ok(FieldState::Null),
        (Some(value), Some(Value::String(asset))) => {
            let amount = match decimal_value(value) {
                Ok(amount) => amount,
                Err(_) => {
                    return Ok(FieldState::Unavailable {
                        reason: UnknownReason::ParseFailure,
                    });
                }
            };
            let asset = match asset.parse::<Asset>() {
                Ok(asset) => asset,
                Err(_) => {
                    return Ok(FieldState::Unavailable {
                        reason: UnknownReason::ParseFailure,
                    });
                }
            };
            Ok(FieldState::Known(Amount::new(asset, amount)))
        }
        _ => Ok(FieldState::Unavailable {
            reason: UnknownReason::ParseFailure,
        }),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PrivateParseError {
    #[error("Binance private payload has an invalid shape")]
    Payload,
    #[error("Binance private payload symbol does not match readback scope")]
    Symbol,
    #[error("Binance private position side is unsupported")]
    Position,
    #[error("Binance private order is invalid")]
    Order,
    #[error("Binance private fill is invalid")]
    Fill,
    #[error("Binance private account capability is absent or invalid")]
    Capability,
    #[error("Binance quote-to-USD conversion evidence is absent, stale, or mismatched")]
    CurrencyEvidence,
    #[error("Binance risk snapshot is incomplete or internally inconsistent")]
    RiskSnapshot,
    #[error("normalized account balance is invalid: {0}")]
    Account(venue_domain::domain::AccountError),
    #[error("normalized order or fill is invalid: {0}")]
    OrderValidation(venue_domain::domain::OrderError),
}

pub fn validate_risk_readback_window(
    started_at_ms: u64,
    observed_at_ms: u64,
    max_age_ms: u64,
) -> Result<(), PrivateParseError> {
    if started_at_ms == 0
        || max_age_ms == 0
        || observed_at_ms < started_at_ms
        || observed_at_ms.saturating_sub(started_at_ms) > max_age_ms
    {
        return Err(PrivateParseError::RiskSnapshot);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use venue_domain::domain::{FieldState, OrderState, PositionSide};

    use super::*;

    #[test]
    fn risk_readback_window_uses_one_authoritative_clock() {
        assert!(validate_risk_readback_window(1_000, 4_000, 3_000).is_ok());
        assert!(matches!(
            validate_risk_readback_window(1_000, 4_001, 3_000),
            Err(PrivateParseError::RiskSnapshot)
        ));
        assert!(matches!(
            validate_risk_readback_window(2_000, 1_999, 3_000),
            Err(PrivateParseError::RiskSnapshot)
        ));
    }

    #[test]
    fn signed_readback_preserves_required_margin_and_position_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let balances = parse_account(
            r#"{"assets":[{"asset":"USDT","walletBalance":"12.5","availableBalance":"8","initialMargin":"3","maintMargin":"0.5"}]}"#,
        )?;
        let positions = parse_positions(
            r#"[{"symbol":"DOGEUSDT","positionAmt":"50","positionSide":"BOTH","entryPrice":"0.1","markPrice":"0.11"}]"#,
            &"DOGE/USDT".parse()?,
        )?;

        assert_eq!(balances[0].available_balance, Decimal::new(8, 0));
        assert_eq!(positions[0].side, PositionSide::Long);
        assert_eq!(positions[0].quantity, Decimal::new(50, 0));
        Ok(())
    }

    #[test]
    fn one_way_negative_position_amount_is_normalized_to_short()
    -> Result<(), Box<dyn std::error::Error>> {
        let positions = parse_positions(
            r#"[{"symbol":"DOGEUSDT","positionAmt":"-50","positionSide":"BOTH","entryPrice":"0.1","markPrice":"0.11"}]"#,
            &"DOGE/USDT".parse()?,
        )?;

        assert_eq!(positions[0].side, PositionSide::Short);
        assert_eq!(positions[0].quantity, Decimal::new(50, 0));
        Ok(())
    }

    #[test]
    fn order_and_fill_keep_unknown_values_unknown() -> Result<(), Box<dyn std::error::Error>> {
        let symbol: Symbol = "DOGE/USDT".parse()?;
        let order = parse_order(
            r#"{"symbol":"DOGEUSDT","orderId":"10","clientOrderId":"client_1","status":"NEW","side":"BUY","origQty":"50","executedQty":"0","price":"0","avgPrice":"0","reduceOnly":false}"#,
            &symbol,
        )?;
        let fills = parse_fills(
            r#"[{"symbol":"DOGEUSDT","id":"1","orderId":"10","side":"BUY","qty":"50","price":"0.1","commission":"0.002","commissionAsset":"USDT","maker":false,"time":1}]"#,
            &symbol,
        )?;

        assert_eq!(order.state, OrderState::New);
        assert!(matches!(
            order.average_price,
            FieldState::Unavailable {
                reason: UnknownReason::NotYetObserved
            }
        ));
        assert!(matches!(order.position_side, FieldState::Missing));
        assert!(matches!(fills[0].position_side, FieldState::Missing));
        assert!(matches!(fills[0].realized_pnl, FieldState::Missing));
        assert!(matches!(fills[0].fee, FieldState::Known(_)));
        Ok(())
    }

    #[test]
    fn partially_filled_order_keeps_its_executed_quantity() -> Result<(), Box<dyn std::error::Error>>
    {
        let symbol: Symbol = "DOGE/USDT".parse()?;
        let order = parse_order(
            r#"{"symbol":"DOGEUSDT","orderId":"10","clientOrderId":"client_1","status":"PARTIALLY_FILLED","side":"BUY","positionSide":"LONG","origQty":"50","executedQty":"17","price":"0.1","avgPrice":"0.11","reduceOnly":false}"#,
            &symbol,
        )?;

        assert_eq!(order.state, OrderState::PartiallyFilled);
        assert_eq!(order.position_side, FieldState::Known(PositionSide::Long));
        assert_eq!(order.filled_quantity, Decimal::new(17, 0));
        Ok(())
    }

    #[test]
    fn numeric_exchange_order_and_fill_identifiers_are_preserved()
    -> Result<(), Box<dyn std::error::Error>> {
        let symbol: Symbol = "DOGE/USDT".parse()?;
        let order = parse_order(
            r#"{"symbol":"DOGEUSDT","orderId":10,"clientOrderId":"client_1","status":"NEW","side":"BUY","origQty":"50","executedQty":"0","price":"0","avgPrice":"0","reduceOnly":false}"#,
            &symbol,
        )?;
        let fills = parse_fills(
            r#"[{"symbol":"DOGEUSDT","id":1,"orderId":10,"side":"BUY","qty":"1","price":"0.1","commission":"0.002","commissionAsset":"USDT","maker":false,"time":1}]"#,
            &symbol,
        )?;

        assert_eq!(order.order_id, "10");
        assert_eq!(fills[0].fill_id, "1");
        assert_eq!(fills[0].execution_sequence, FieldState::Known(1));
        assert_eq!(fills[0].order_id, "10");
        Ok(())
    }

    #[test]
    fn stream_and_signed_fill_preserve_the_same_price_and_maker_role()
    -> Result<(), Box<dyn std::error::Error>> {
        let symbol: Symbol = "SOL/USDC".parse()?;
        let cases = [
            (
                r#"{"e":"ORDER_TRADE_UPDATE","E":1000,"T":999,"o":{"s":"SOLUSDC","c":"hgo_e1_long_open_l1","x":"TRADE","S":"BUY","ps":"LONG","t":7,"i":11,"l":"0.25","L":"100.125","n":"0.001","N":"USDC","rp":"0","ma":"USDC","m":true}}"#,
                r#"[{"symbol":"SOLUSDC","id":7,"orderId":11,"side":"BUY","positionSide":"LONG","qty":"0.25","price":"100.125","commission":"0.001","commissionAsset":"USDC","realizedPnl":"0","marginAsset":"USDC","maker":true,"time":999}]"#,
                true,
            ),
            (
                r#"{"e":"ORDER_TRADE_UPDATE","E":1000,"T":999,"o":{"s":"SOLUSDC","c":"hgo_e1_long_open_l1","x":"TRADE","S":"BUY","ps":"LONG","t":7,"i":11,"l":"0.25","L":"100.125","n":"0.001","N":"USDC","rp":"0","ma":"USDC","m":false}}"#,
                r#"[{"symbol":"SOLUSDC","id":7,"orderId":11,"side":"BUY","positionSide":"LONG","qty":"0.25","price":"100.125","commission":"0.001","commissionAsset":"USDC","realizedPnl":"0","marginAsset":"USDC","maker":false,"time":999}]"#,
                false,
            ),
        ];
        for (stream_payload, signed_payload, expected_maker) in cases {
            let stream =
                parse_stream_fill(stream_payload, &symbol)?.ok_or("missing stream fill")?;
            let signed = parse_fills(signed_payload, &symbol)?;

            assert_eq!(stream.fill, signed[0]);
            assert_eq!(stream.fill.execution_sequence, FieldState::Known(7));
            assert_eq!(stream.fill.maker, FieldState::Known(expected_maker));
            assert_eq!(stream.fill.price.value(), Decimal::new(100125, 3));
            assert_eq!(
                stream.client_order_id,
                FieldState::Known("hgo_e1_long_open_l1".to_owned())
            );
        }
        Ok(())
    }

    #[test]
    fn stream_fill_never_guesses_missing_maker_evidence() -> Result<(), Box<dyn std::error::Error>>
    {
        let symbol: Symbol = "SOL/USDC".parse()?;
        let fill = parse_stream_fill(
            r#"{"e":"ORDER_TRADE_UPDATE","E":1000,"o":{"s":"SOLUSDC","c":"hgo_e1_long_open_l1","x":"TRADE","S":"BUY","ps":"LONG","t":7,"i":11,"l":"0.25","L":"100.125"}}"#,
            &symbol,
        )?
        .ok_or("missing stream fill")?;
        assert!(matches!(fill.fill.maker, FieldState::Missing));
        Ok(())
    }

    #[test]
    fn conditional_strategy_history_fixture_requires_exact_identity_and_known_status()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            parse_conditional_strategy(
                r#"{"newClientStrategyId":"protect_1","strategyId":10,"strategyStatus":"CANCELED"}"#,
                "protect_1",
            )?,
            ConditionalStrategyReadback {
                strategy_id: "10".to_owned(),
                status: ConditionalStrategyStatus::Cancelled,
                side: FieldState::Missing,
                position_side: FieldState::Missing,
                stop_price: FieldState::Missing,
                close_position: FieldState::Missing,
            }
        );
        assert!(
            parse_conditional_strategy(
                r#"{"newClientStrategyId":"another","strategyId":10,"strategyStatus":"TRIGGERED"}"#,
                "protect_1",
            )
            .is_err()
        );
        assert_eq!(
            parse_conditional_strategy(
                r#"{"newClientStrategyId":"protect_1","strategyId":"11","strategyStatus":"FINISHED"}"#,
                "protect_1",
            )?
            .status,
            ConditionalStrategyStatus::NonCancelledTerminal
        );
        assert_eq!(
            parse_conditional_strategy(
                r#"{"newClientStrategyId":"protect_1","strategyId":"12","strategyStatus":"CANCELLED"}"#,
                "protect_1",
            )?
            .status,
            ConditionalStrategyStatus::Cancelled
        );
        assert_eq!(
            parse_conditional_strategy(
                r#"{"newClientStrategyId":"protect_1","strategyId":"13","strategyStatus":"FUTURE_VALUE"}"#,
                "protect_1",
            )?
            .status,
            ConditionalStrategyStatus::Unknown
        );
        let exact = parse_conditional_strategy(
            r#"{"newClientStrategyId":"protect_1","strategyId":"14","strategyStatus":"NEW","side":"SELL","positionSide":"LONG","stopPrice":"0.09","closePosition":true}"#,
            "protect_1",
        )?;
        assert_eq!(exact.side, FieldState::Known(OrderSide::Sell));
        assert_eq!(exact.position_side, FieldState::Known(PositionSide::Long));
        assert_eq!(
            exact.stop_price,
            FieldState::Known(Price::new(Decimal::new(9, 2))?)
        );
        assert_eq!(exact.close_position, FieldState::Known(true));
        assert!(parse_conditional_strategy_id(
            r#"{"newClientStrategyId":"protect_1","strategyId":"15","strategyStatus":"REJECTED"}"#,
            "protect_1",
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn open_algo_collection_returns_only_unique_current_client_identities()
    -> Result<(), Box<dyn std::error::Error>> {
        let symbol: Symbol = "SOL/USDT".parse()?;
        assert_eq!(
            parse_open_algo_client_ids(
                r#"[{"symbol":"SOLUSDT","clientAlgoId":"protect_2","algoStatus":"ACTIVE"},{"symbol":"SOLUSDT","clientAlgoId":"protect_1","algoStatus":"NEW"}]"#,
                &symbol,
            )?,
            vec!["protect_1".to_owned(), "protect_2".to_owned()]
        );
        assert!(
            parse_open_algo_client_ids(
                r#"[{"symbol":"SOLUSDT","clientAlgoId":"protect_1","algoStatus":"NEW"},{"symbol":"SOLUSDT","clientAlgoId":"protect_1","algoStatus":"NEW"}]"#,
                &symbol,
            )
            .is_err()
        );
        assert!(
            parse_open_algo_client_ids(
                r#"[{"symbol":"BNBUSDT","clientAlgoId":"protect_1","algoStatus":"NEW"}]"#,
                &symbol,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn open_algo_collection_can_preserve_exact_custody_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let symbol: Symbol = "SOL/USDT".parse()?;
        let payload = r#"[{"symbol":"SOLUSDT","algoId":2,"clientAlgoId":"protect_short","algoStatus":"ACTIVE","orderType":"STOP_MARKET","side":"BUY","positionSide":"SHORT","quantity":"0.20","triggerPrice":"99","workingType":"MARK_PRICE","closePosition":false,"reduceOnly":true},{"symbol":"SOLUSDT","algoId":"1","clientAlgoId":"protect_long","algoStatus":"NEW","orderType":"STOP_MARKET","side":"SELL","positionSide":"LONG","quantity":"0.10","triggerPrice":"90","workingType":"MARK_PRICE","closePosition":false,"reduceOnly":true}]"#;

        let orders = parse_open_algo_orders(payload, &symbol)?;

        assert_eq!(orders.len(), 2);
        assert_eq!(orders[0].client_algo_id, "protect_long");
        assert_eq!(orders[0].algo_id, "1");
        assert_eq!(
            orders[0].position_side,
            FieldState::Known(PositionSide::Long)
        );
        assert_eq!(orders[0].quantity, FieldState::Known(Decimal::new(10, 2)));
        assert_eq!(orders[1].client_algo_id, "protect_short");
        assert_eq!(orders[1].algo_id, "2");
        assert_eq!(
            orders[1].position_side,
            FieldState::Known(PositionSide::Short)
        );
        let order_facts = parse_open_algo_order_facts(payload, &symbol)?;
        assert_eq!(order_facts.len(), 2);
        assert_eq!(order_facts[0].order_id, "1");
        assert_eq!(
            order_facts[0].client_order_id,
            FieldState::Known("protect_long".to_owned())
        );
        assert_eq!(order_facts[0].quantity, Decimal::new(10, 2));
        assert_eq!(order_facts[0].limit_price, None);
        assert!(order_facts[0].reduce_only);
        Ok(())
    }

    #[test]
    fn conditional_open_order_collection_requires_complete_current_custody_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let symbol: Symbol = "SOL/USDT".parse()?;
        let payload = r#"[{"symbol":"SOLUSDT","strategyId":"7","newClientStrategyId":"protect_conditional","strategyStatus":"NEW","side":"SELL","positionSide":"LONG","quantity":"0.10","price":"99","reduceOnly":true,"closePosition":false}]"#;

        let orders = parse_open_conditional_orders(payload, &symbol)?;

        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].order_id, "7");
        assert_eq!(
            orders[0].client_order_id,
            FieldState::Known("protect_conditional".to_owned())
        );
        assert_eq!(orders[0].side, OrderSide::Sell);
        assert_eq!(
            orders[0].position_side,
            FieldState::Known(PositionSide::Long)
        );
        assert_eq!(orders[0].quantity, Decimal::new(10, 2));
        assert_eq!(
            orders[0].limit_price,
            Some(Price::new(Decimal::new(99, 0))?)
        );
        assert!(orders[0].reduce_only);
        assert!(parse_open_conditional_orders(
            r#"[{"symbol":"SOLUSDT","strategyId":"7","newClientStrategyId":"protect_conditional","strategyStatus":"NEW","side":"SELL","positionSide":"LONG","price":"99","reduceOnly":true,"closePosition":false}]"#,
            &symbol,
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn account_capabilities_require_trading_and_an_explicit_position_mode()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            parse_account_capabilities(r#"{"canTrade":true}"#, r#"{"dualSidePosition":false}"#,)?,
            PrivateAccountCapabilities {
                can_trade: true,
                one_way_position: true,
                hedge_position: false,
            }
        );
        assert!(matches!(
            parse_account_capabilities(r#"{"canTrade":true}"#, r#"{}"#),
            Err(PrivateParseError::Capability)
        ));
        Ok(())
    }

    #[test]
    fn portfolio_position_mode_treats_dual_side_as_not_one_way()
    -> Result<(), Box<dyn std::error::Error>> {
        assert!(!parse_portfolio_position_mode(
            r#"{"dualSidePosition":true}"#
        )?);
        Ok(())
    }
}
