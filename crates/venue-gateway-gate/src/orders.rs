use std::{collections::BTreeSet, str::FromStr};

use rust_decimal::Decimal;
use serde_json::{Map, Value};
use venue_domain::domain::{
    Amount, Asset, FieldState, Fill, LimitTimeInForce, Order, OrderSide, OrderState, PositionSide,
    Price, Symbol, UnknownReason,
};

use crate::{GateContractRules, decimal, decimal_value, object, optional_price, text};

pub const GATE_PRIVATE_PAGE_LIMIT: usize = 100;
pub const GATE_PRIVATE_MAX_PAGES: usize = 1_000;

/// One normalized Gate fill plus the client identity that Gate carries outside the canonical
/// [`Fill`]. The untouched native identity remains in each readback's `raw_payloads`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateFillRecord {
    pub fill: Fill,
    pub client_order_id: FieldState<String>,
}

/// A complete regular-order readback. Construction requires a short terminal page, so a caller
/// cannot accidentally treat one full page as a complete signed family projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateRegularOrdersReadback {
    pub raw_payloads: Vec<String>,
    pub orders: Vec<Order>,
    pub last_native_id: Option<String>,
}

/// A complete private-fill readback with the same bounded, non-overlapping cursor contract as
/// regular orders.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateFillsReadback {
    pub raw_payloads: Vec<String>,
    pub fills: Vec<GateFillRecord>,
    pub last_native_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GateOrderPayloadError {
    #[error("Gate.io order or fill payload is invalid or incomplete")]
    Payload,
    #[error("Gate.io order or fill belongs to another symbol or product")]
    Symbol,
    #[error("Gate.io private pagination is overlapping, unbounded, or incomplete")]
    Pagination,
}

pub fn parse_regular_order(
    value: &Value,
    symbol: &Symbol,
    rules: &GateContractRules,
) -> Result<Order, GateOrderPayloadError> {
    validate_rules(symbol, rules)?;
    let item = object(value).map_err(|_| GateOrderPayloadError::Payload)?;
    require_native_symbol(item, rules)?;
    let signed_size = decimal(item, "size").map_err(|_| GateOrderPayloadError::Payload)?;
    let signed_left = decimal(item, "left").map_err(|_| GateOrderPayloadError::Payload)?;
    if signed_size.is_zero()
        || signed_left.is_sign_negative() != signed_size.is_sign_negative()
            && !signed_left.is_zero()
    {
        return Err(GateOrderPayloadError::Payload);
    }
    let quantity = checked_physical_quantity(signed_size.abs(), rules)?;
    let left = checked_physical_quantity_allow_zero(signed_left.abs(), rules)?;
    if left > quantity {
        return Err(GateOrderPayloadError::Payload);
    }
    let reduce_only = boolean(item, "is_reduce_only")?;
    let side = order_side(signed_size);
    let position_side = match (reduce_only, side) {
        (false, OrderSide::Buy) | (true, OrderSide::Sell) => PositionSide::Long,
        (false, OrderSide::Sell) | (true, OrderSide::Buy) => PositionSide::Short,
    };
    let order = Order {
        order_id: identifier(item.get("id"))?,
        client_order_id: client_order_id(item.get("text")),
        symbol: symbol.clone(),
        side,
        position_side: FieldState::Known(position_side),
        purpose: FieldState::Missing,
        state: order_state(item, quantity, left)?,
        quantity,
        filled_quantity: quantity
            .checked_sub(left)
            .ok_or(GateOrderPayloadError::Payload)?,
        limit_price: optional_price(item.get("price"))
            .map_err(|_| GateOrderPayloadError::Payload)?,
        time_in_force: optional_limit_time_in_force(item.get("tif")),
        average_price: optional_price_state(item.get("fill_price"))?,
        reduce_only,
    };
    order
        .validate()
        .map_err(|_| GateOrderPayloadError::Payload)?;
    Ok(order)
}

fn optional_limit_time_in_force(value: Option<&Value>) -> FieldState<LimitTimeInForce> {
    match value {
        None => FieldState::Missing,
        Some(Value::Null) => FieldState::Null,
        Some(Value::String(value)) => match value.as_str() {
            "poc" => FieldState::Known(LimitTimeInForce::PostOnly),
            "gtc" => FieldState::Known(LimitTimeInForce::Gtc),
            _ => FieldState::Unavailable {
                reason: UnknownReason::Ambiguous,
            },
        },
        Some(_) => FieldState::Unavailable {
            reason: UnknownReason::ParseFailure,
        },
    }
}

pub fn parse_fill_record(
    value: &Value,
    symbol: &Symbol,
    rules: &GateContractRules,
) -> Result<GateFillRecord, GateOrderPayloadError> {
    validate_rules(symbol, rules)?;
    let item = object(value).map_err(|_| GateOrderPayloadError::Payload)?;
    require_native_symbol(item, rules)?;
    let signed_size = decimal(item, "size").map_err(|_| GateOrderPayloadError::Payload)?;
    if signed_size.is_zero() {
        return Err(GateOrderPayloadError::Payload);
    }
    let fill_id = identifier(item.get("id"))?;
    let fill = Fill {
        execution_sequence: fill_id.parse::<u64>().map(FieldState::Known).unwrap_or(
            FieldState::Unavailable {
                reason: UnknownReason::ParseFailure,
            },
        ),
        fill_id,
        order_id: identifier(item.get("order_id"))?,
        symbol: symbol.clone(),
        side: order_side(signed_size),
        position_side: fill_position_side(item.get("text")),
        quantity: checked_physical_quantity(signed_size.abs(), rules)?,
        price: Price::new(decimal(item, "price").map_err(|_| GateOrderPayloadError::Payload)?)
            .map_err(|_| GateOrderPayloadError::Payload)?,
        fee: optional_usdt_amount(item.get("fee"))?,
        realized_pnl: optional_usdt_amount(item.get("pnl"))?,
        maker: optional_maker(item.get("role")),
        exchange_time_ms: optional_timestamp_ms(
            item.get("create_time_ms")
                .or_else(|| item.get("create_time")),
        )?,
    };
    fill.validate()
        .map_err(|_| GateOrderPayloadError::Payload)?;
    Ok(GateFillRecord {
        fill,
        client_order_id: client_order_id(item.get("text")),
    })
}

pub fn collect_regular_order_pages<I, S>(
    pages: I,
    symbol: &Symbol,
    rules: &GateContractRules,
) -> Result<GateRegularOrdersReadback, GateOrderPayloadError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let collected = collect_pages(pages, |value| parse_regular_order(value, symbol, rules))?;
    Ok(GateRegularOrdersReadback {
        raw_payloads: collected.raw_payloads,
        orders: collected.values,
        last_native_id: collected.last_native_id,
    })
}

pub fn collect_fill_pages<I, S>(
    pages: I,
    symbol: &Symbol,
    rules: &GateContractRules,
) -> Result<GateFillsReadback, GateOrderPayloadError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let collected = collect_pages(pages, |value| parse_fill_record(value, symbol, rules))?;
    Ok(GateFillsReadback {
        raw_payloads: collected.raw_payloads,
        fills: collected.values,
        last_native_id: collected.last_native_id,
    })
}

struct CollectedPages<T> {
    raw_payloads: Vec<String>,
    values: Vec<T>,
    last_native_id: Option<String>,
}

fn collect_pages<I, S, T, F>(
    pages: I,
    mut parse: F,
) -> Result<CollectedPages<T>, GateOrderPayloadError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
    F: FnMut(&Value) -> Result<T, GateOrderPayloadError>,
{
    let mut raw_payloads = Vec::new();
    let mut values = Vec::new();
    let mut seen_ids = BTreeSet::new();
    let mut last_native_id = None;
    let mut terminal_seen = false;
    for (index, page) in pages.into_iter().enumerate() {
        if index >= GATE_PRIVATE_MAX_PAGES || terminal_seen {
            return Err(GateOrderPayloadError::Pagination);
        }
        let raw = page.as_ref();
        let page_value: Value =
            serde_json::from_str(raw).map_err(|_| GateOrderPayloadError::Payload)?;
        let rows = page_value
            .as_array()
            .ok_or(GateOrderPayloadError::Payload)?;
        if rows.len() > GATE_PRIVATE_PAGE_LIMIT {
            return Err(GateOrderPayloadError::Pagination);
        }
        for row in rows {
            let item = object(row).map_err(|_| GateOrderPayloadError::Payload)?;
            let native_id = identifier(item.get("id"))?;
            if !seen_ids.insert(native_id.clone()) {
                return Err(GateOrderPayloadError::Pagination);
            }
            values.push(parse(row)?);
            last_native_id = Some(native_id);
        }
        raw_payloads.push(raw.to_owned());
        terminal_seen = rows.len() < GATE_PRIVATE_PAGE_LIMIT;
    }
    if raw_payloads.is_empty() || !terminal_seen {
        return Err(GateOrderPayloadError::Pagination);
    }
    Ok(CollectedPages {
        raw_payloads,
        values,
        last_native_id,
    })
}

fn validate_rules(symbol: &Symbol, rules: &GateContractRules) -> Result<(), GateOrderPayloadError> {
    if symbol != &rules.instrument.symbol
        || rules.native_symbol.trim().is_empty()
        || rules.quanto_multiplier <= Decimal::ZERO
        || rules.instrument.validate().is_err()
    {
        return Err(GateOrderPayloadError::Symbol);
    }
    Ok(())
}

fn require_native_symbol(
    item: &Map<String, Value>,
    rules: &GateContractRules,
) -> Result<(), GateOrderPayloadError> {
    if text(item, "contract").map_err(|_| GateOrderPayloadError::Payload)? != rules.native_symbol {
        return Err(GateOrderPayloadError::Symbol);
    }
    Ok(())
}

fn checked_physical_quantity(
    contracts: Decimal,
    rules: &GateContractRules,
) -> Result<Decimal, GateOrderPayloadError> {
    if contracts <= Decimal::ZERO {
        return Err(GateOrderPayloadError::Payload);
    }
    contracts
        .checked_mul(rules.quanto_multiplier)
        .filter(|value| *value > Decimal::ZERO)
        .ok_or(GateOrderPayloadError::Payload)
}

fn checked_physical_quantity_allow_zero(
    contracts: Decimal,
    rules: &GateContractRules,
) -> Result<Decimal, GateOrderPayloadError> {
    if contracts.is_sign_negative() {
        return Err(GateOrderPayloadError::Payload);
    }
    contracts
        .checked_mul(rules.quanto_multiplier)
        .ok_or(GateOrderPayloadError::Payload)
}

fn identifier(value: Option<&Value>) -> Result<String, GateOrderPayloadError> {
    match value {
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(value.to_owned()),
        Some(Value::Number(value)) => Ok(value.to_string()),
        _ => Err(GateOrderPayloadError::Payload),
    }
}

fn boolean(item: &Map<String, Value>, field: &str) -> Result<bool, GateOrderPayloadError> {
    item.get(field)
        .and_then(Value::as_bool)
        .ok_or(GateOrderPayloadError::Payload)
}

fn order_side(signed_size: Decimal) -> OrderSide {
    if signed_size.is_sign_positive() {
        OrderSide::Buy
    } else {
        OrderSide::Sell
    }
}

fn client_order_id(value: Option<&Value>) -> FieldState<String> {
    match value {
        None => FieldState::Missing,
        Some(Value::Null) => FieldState::Null,
        Some(Value::String(value)) => match value.strip_prefix("t-") {
            Some(value) if !value.is_empty() => FieldState::Known(value.to_owned()),
            _ => FieldState::Unavailable {
                reason: UnknownReason::Ambiguous,
            },
        },
        Some(_) => FieldState::Unavailable {
            reason: UnknownReason::ParseFailure,
        },
    }
}

fn order_state(
    item: &Map<String, Value>,
    quantity: Decimal,
    left: Decimal,
) -> Result<OrderState, GateOrderPayloadError> {
    match text(item, "status").map_err(|_| GateOrderPayloadError::Payload)? {
        "open" if left == quantity => Ok(OrderState::New),
        "open" if left > Decimal::ZERO => Ok(OrderState::PartiallyFilled),
        "open" => Err(GateOrderPayloadError::Payload),
        "finished" => match text(item, "finish_as").map_err(|_| GateOrderPayloadError::Payload)? {
            "filled" if left.is_zero() => Ok(OrderState::Filled),
            "filled" => Err(GateOrderPayloadError::Payload),
            "cancelled" | "ioc" | "poc" | "stp" | "reduce_only" | "position_closed"
            | "reduce_out" => Ok(OrderState::Cancelled),
            "rejected" => Ok(OrderState::Rejected),
            _ => Ok(OrderState::Unknown),
        },
        _ => Ok(OrderState::Unknown),
    }
}

fn optional_price_state(value: Option<&Value>) -> Result<FieldState<Price>, GateOrderPayloadError> {
    match value {
        None => Ok(FieldState::Missing),
        Some(Value::Null) => Ok(FieldState::Null),
        Some(Value::String(value)) if value.is_empty() => Ok(FieldState::Unavailable {
            reason: UnknownReason::VenueUnavailable,
        }),
        value => {
            let price = decimal_value(value).map_err(|_| GateOrderPayloadError::Payload)?;
            if price.is_zero() {
                Ok(FieldState::Unavailable {
                    reason: UnknownReason::VenueUnavailable,
                })
            } else {
                Price::new(price)
                    .map(FieldState::Known)
                    .map_err(|_| GateOrderPayloadError::Payload)
            }
        }
    }
}

fn optional_usdt_amount(
    value: Option<&Value>,
) -> Result<FieldState<Amount>, GateOrderPayloadError> {
    match value {
        None => Ok(FieldState::Missing),
        Some(Value::Null) => Ok(FieldState::Null),
        Some(Value::String(value)) if value.is_empty() => Ok(FieldState::Unavailable {
            reason: UnknownReason::VenueUnavailable,
        }),
        value => Ok(FieldState::Known(Amount::new(
            Asset::new("USDT").map_err(|_| GateOrderPayloadError::Payload)?,
            decimal_value(value).map_err(|_| GateOrderPayloadError::Payload)?,
        ))),
    }
}

fn optional_maker(value: Option<&Value>) -> FieldState<bool> {
    match value {
        None => FieldState::Missing,
        Some(Value::Null) => FieldState::Null,
        Some(Value::String(value)) => match value.as_str() {
            "maker" => FieldState::Known(true),
            "taker" => FieldState::Known(false),
            _ => FieldState::Unavailable {
                reason: UnknownReason::Ambiguous,
            },
        },
        Some(_) => FieldState::Unavailable {
            reason: UnknownReason::ParseFailure,
        },
    }
}

fn optional_timestamp_ms(value: Option<&Value>) -> Result<Option<u64>, GateOrderPayloadError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        value => timestamp_ms(value).map(Some),
    }
}

fn timestamp_ms(value: Option<&Value>) -> Result<u64, GateOrderPayloadError> {
    let raw = match value {
        Some(Value::String(value)) => {
            Decimal::from_str(value).map_err(|_| GateOrderPayloadError::Payload)?
        }
        Some(Value::Number(value)) => {
            Decimal::from_str(&value.to_string()).map_err(|_| GateOrderPayloadError::Payload)?
        }
        _ => return Err(GateOrderPayloadError::Payload),
    };
    if raw <= Decimal::ZERO {
        return Err(GateOrderPayloadError::Payload);
    }
    let milliseconds = if raw < Decimal::from(100_000_000_000_u64) {
        raw.checked_mul(Decimal::from(1_000_u16))
            .ok_or(GateOrderPayloadError::Payload)?
    } else {
        raw
    };
    milliseconds
        .trunc()
        .to_string()
        .parse()
        .map_err(|_| GateOrderPayloadError::Payload)
}

fn fill_position_side(value: Option<&Value>) -> FieldState<PositionSide> {
    let Some(value) = value else {
        return FieldState::Missing;
    };
    let Some(value) = value.as_str() else {
        return FieldState::Unavailable {
            reason: UnknownReason::ParseFailure,
        };
    };
    if value.starts_with("t-ord-etp-") {
        return canonical_exposure_position_side(value).map_or(
            FieldState::Unavailable {
                reason: UnknownReason::Ambiguous,
            },
            FieldState::Known,
        );
    }
    match (value.contains("_long_"), value.contains("_short_")) {
        (true, false) => FieldState::Known(PositionSide::Long),
        (false, true) => FieldState::Known(PositionSide::Short),
        _ => FieldState::Unavailable {
            reason: UnknownReason::Ambiguous,
        },
    }
}

fn canonical_exposure_position_side(value: &str) -> Option<PositionSide> {
    let (prefix, side) = if value.starts_with("t-ord-etp-l-") {
        ("t-ord-etp-l-", PositionSide::Long)
    } else if value.starts_with("t-ord-etp-s-") {
        ("t-ord-etp-s-", PositionSide::Short)
    } else {
        return None;
    };
    let suffix = value.strip_prefix(prefix)?;
    (suffix.len() == 16
        && suffix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')))
    .then_some(side)
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use venue_domain::domain::{Amount, Instrument, MarketKind};

    use super::*;

    fn rules() -> Result<GateContractRules, Box<dyn std::error::Error>> {
        let symbol: Symbol = "DOGE/USDT".parse()?;
        Ok(GateContractRules {
            native_symbol: "DOGE_USDT".to_owned(),
            instrument: Instrument {
                settlement_asset: Some("USDT".parse()?),
                minimum_notional: Amount::new("USDT".parse()?, Decimal::ZERO),
                symbol,
                market: MarketKind::LinearPerpetual,
                generation: 7,
                price_tick: Price::new(Decimal::new(1, 5))?,
                quantity_step: Decimal::new(1, 1),
            },
            quanto_multiplier: Decimal::new(1, 1),
            minimum_contracts: Decimal::ONE,
            decimal_contracts: false,
        })
    }

    #[test]
    fn fixture_pages_preserve_raw_evidence_and_normalize_both_identities()
    -> Result<(), Box<dyn std::error::Error>> {
        let rules = rules()?;
        let symbol = rules.instrument.symbol.clone();
        let order_payload = include_str!("../tests/fixtures/regular_orders.json");
        let fill_payload = include_str!("../tests/fixtures/fills.json");
        let orders = collect_regular_order_pages([order_payload], &symbol, &rules)?;
        let fills = collect_fill_pages([fill_payload], &symbol, &rules)?;

        assert_eq!(orders.raw_payloads, vec![order_payload]);
        assert_eq!(orders.last_native_id.as_deref(), Some("9002"));
        assert_eq!(orders.orders.len(), 2);
        assert_eq!(orders.orders[0].order_id, "9001");
        assert_eq!(orders.orders[0].quantity, Decimal::from(5));
        assert_eq!(orders.orders[0].filled_quantity, Decimal::from(2));
        assert_eq!(orders.orders[0].state, OrderState::PartiallyFilled);
        assert_eq!(
            orders.orders[0].position_side,
            FieldState::Known(PositionSide::Long)
        );
        assert_eq!(
            orders.orders[1].position_side,
            FieldState::Known(PositionSide::Short)
        );

        assert_eq!(fills.raw_payloads, vec![fill_payload]);
        assert_eq!(fills.last_native_id.as_deref(), Some("227262267"));
        assert_eq!(fills.fills[0].fill.order_id, "9001");
        assert_eq!(
            fills.fills[0].fill.execution_sequence,
            FieldState::Known(227262266)
        );
        assert_eq!(
            fills.fills[0].fill.position_side,
            FieldState::Known(PositionSide::Long)
        );
        assert_eq!(
            fills.fills[0].client_order_id,
            FieldState::Known("ord-etp-l-0000000000000001".to_owned())
        );
        assert!(matches!(
            fills.fills[1].fill.position_side,
            FieldState::Unavailable {
                reason: UnknownReason::Ambiguous
            }
        ));
        Ok(())
    }

    #[test]
    fn pagination_requires_one_short_terminal_page_and_unique_native_ids()
    -> Result<(), Box<dyn std::error::Error>> {
        let rules = rules()?;
        let symbol = rules.instrument.symbol.clone();
        let full = (0..GATE_PRIVATE_PAGE_LIMIT)
            .map(|id| {
                json!({
                    "id": id + 1, "contract":"DOGE_USDT", "size":"1", "left":"1",
                    "is_reduce_only":false, "status":"open", "price":"0.1",
                    "fill_price":"0", "text":format!("t-grid_{id}")
                })
            })
            .collect::<Vec<_>>();
        let full = serde_json::to_string(&full)?;
        assert_eq!(
            collect_regular_order_pages([full.as_str()], &symbol, &rules),
            Err(GateOrderPayloadError::Pagination)
        );

        let duplicate = serde_json::to_string(&vec![json!({
            "id": 100, "contract":"DOGE_USDT", "size":"1", "left":"1",
            "is_reduce_only":false, "status":"open", "price":"0.1",
            "fill_price":"0", "text":"t-grid_duplicate"
        })])?;
        assert_eq!(
            collect_regular_order_pages([full.as_str(), duplicate.as_str()], &symbol, &rules),
            Err(GateOrderPayloadError::Pagination)
        );
        Ok(())
    }

    #[test]
    fn parser_fails_closed_on_symbol_direction_and_terminal_page_ambiguity()
    -> Result<(), Box<dyn std::error::Error>> {
        let rules = rules()?;
        let symbol = rules.instrument.symbol.clone();
        assert_eq!(
            parse_regular_order(
                &json!({
                    "id":"1", "contract":"BTC_USDT", "size":"1", "left":"1",
                    "is_reduce_only":false, "status":"open", "price":"1"
                }),
                &symbol,
                &rules
            ),
            Err(GateOrderPayloadError::Symbol)
        );
        assert_eq!(
            parse_regular_order(
                &json!({
                    "id":"1", "contract":"DOGE_USDT", "size":"1", "left":"-1",
                    "is_reduce_only":false, "status":"open", "price":"1"
                }),
                &symbol,
                &rules
            ),
            Err(GateOrderPayloadError::Payload)
        );
        assert_eq!(
            collect_fill_pages(
                ["[]", include_str!("../tests/fixtures/fills.json")],
                &symbol,
                &rules
            ),
            Err(GateOrderPayloadError::Pagination)
        );
        Ok(())
    }
}
