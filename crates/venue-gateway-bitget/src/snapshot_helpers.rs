//! Strict parsing helpers for Bitget's signed account-wide snapshot faces.

use super::*;

pub(super) fn snapshot_fill_time(row: &Value) -> Result<u64, AccountHostValidationError> {
    let text = row
        .get("cTime")
        .or_else(|| row.get("createdTime"))
        .and_then(Value::as_str)
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    text.parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or(AccountHostValidationError::SignedSnapshot)
}

pub(super) fn snapshot_data(payload: &str) -> Result<Value, AccountHostValidationError> {
    let root: Value =
        serde_json::from_str(payload).map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    if root.get("code").and_then(Value::as_str) != Some("00000") {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    root.get("data")
        .cloned()
        .ok_or(AccountHostValidationError::SignedSnapshot)
}

pub(super) fn snapshot_data_rows(payload: &str) -> Result<Vec<Value>, AccountHostValidationError> {
    snapshot_data(payload)?
        .get("list")
        .and_then(Value::as_array)
        .cloned()
        .ok_or(AccountHostValidationError::SignedSnapshot)
}

pub(super) fn snapshot_symbol(value: Option<&Value>) -> Result<Symbol, AccountHostValidationError> {
    let raw = value
        .and_then(Value::as_str)
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    let base = raw
        .strip_suffix("USDT")
        .filter(|v| !v.is_empty())
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    Symbol::new(base, "USDT").map_err(|_| AccountHostValidationError::SignedSnapshot)
}

pub(super) fn snapshot_decimal(
    value: Option<&Value>,
) -> Result<Decimal, AccountHostValidationError> {
    super::decimal(value).map_err(|_| AccountHostValidationError::SignedSnapshot)
}

pub(super) fn snapshot_position_facts(
    rows: &[Value],
) -> Result<Vec<SignedAccountPositionFact>, AccountHostValidationError> {
    rows.iter()
        .map(|row| {
            let item = row
                .as_object()
                .ok_or(AccountHostValidationError::SignedSnapshot)?;
            super::require_usdt_perpetual(item)
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            super::require_text(item, "holdMode", "hedge_mode")
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            let side = match item.get("posSide").and_then(Value::as_str) {
                Some("long") => PositionSide::Long,
                Some("short") => PositionSide::Short,
                _ => return Err(AccountHostValidationError::SignedSnapshot),
            };
            Ok(SignedAccountPositionFact {
                symbol: snapshot_symbol(item.get("symbol"))?,
                position_side: side,
                quantity: snapshot_decimal(item.get("total"))?,
                entry_price: None,
                mark_price: match snapshot_decimal(item.get("markPrice"))? {
                    v if v > Decimal::ZERO => Some(v),
                    v if v.is_zero() => None,
                    _ => return Err(AccountHostValidationError::SignedSnapshot),
                },
            })
        })
        .collect()
}

pub(super) fn snapshot_order_facts(
    rows: &[Value],
) -> Result<Vec<SignedAccountOrderFact>, AccountHostValidationError> {
    rows.iter()
        .map(|row| {
            let item = row
                .as_object()
                .ok_or(AccountHostValidationError::SignedSnapshot)?;
            super::require_usdt_perpetual(item)
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            super::require_text(item, "delegateType", "normal")
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            let side = match item.get("side").and_then(Value::as_str) {
                Some("buy") => OrderSide::Buy,
                Some("sell") => OrderSide::Sell,
                _ => return Err(AccountHostValidationError::SignedSnapshot),
            };
            let position_side = match item.get("posSide").and_then(Value::as_str) {
                Some("long") => PositionSide::Long,
                Some("short") => PositionSide::Short,
                _ => return Err(AccountHostValidationError::SignedSnapshot),
            };
            let quantity = snapshot_decimal(item.get("qty"))?;
            let filled_quantity = snapshot_decimal(item.get("cumExecQty"))?;
            let remaining_quantity = quantity
                .checked_sub(filled_quantity)
                .ok_or(AccountHostValidationError::SignedSnapshot)?;
            if quantity <= Decimal::ZERO || remaining_quantity <= Decimal::ZERO {
                return Err(AccountHostValidationError::SignedSnapshot);
            }
            Ok(SignedAccountOrderFact {
                client_order_id: item
                    .get("clientOid")
                    .and_then(Value::as_str)
                    .filter(|v| !v.is_empty())
                    .ok_or(AccountHostValidationError::SignedSnapshot)?
                    .to_owned(),
                venue_order_id: Some(
                    item.get("orderId")
                        .and_then(Value::as_str)
                        .filter(|v| !v.is_empty())
                        .ok_or(AccountHostValidationError::SignedSnapshot)?
                        .to_owned(),
                ),
                symbol: snapshot_symbol(item.get("symbol"))?,
                family: NativeOrderFamily::UmOrder,
                side,
                position_side,
                quantity,
                limit_price: match snapshot_decimal(item.get("price"))? {
                    v if v > Decimal::ZERO => Some(v),
                    v if v.is_zero() => None,
                    _ => return Err(AccountHostValidationError::SignedSnapshot),
                },
                time_in_force: match item.get("timeInForce") {
                    Some(Value::String(value)) => match value.as_str() {
                        "post_only" => Some(LimitTimeInForce::PostOnly),
                        "gtc" => Some(LimitTimeInForce::Gtc),
                        "ioc" | "fok" => None,
                        _ => return Err(AccountHostValidationError::SignedSnapshot),
                    },
                    None | Some(Value::Null) => None,
                    Some(_) => return Err(AccountHostValidationError::SignedSnapshot),
                },
                created_at_ms: snapshot_order_created_at_ms(
                    item.get("cTime").or_else(|| item.get("createdTime")),
                )?,
                reduce_only: super::bitget_reduce_only(item)
                    .map_err(|_| AccountHostValidationError::SignedSnapshot)?,
                owner: None,
                external: true,
                state: Some(snapshot_order_state(
                    item.get("orderStatus").or_else(|| item.get("status")),
                )?),
                filled_quantity: Some(filled_quantity),
            })
        })
        .collect()
}

pub(super) fn snapshot_strategy_order_facts(
    payload: &[u8],
) -> Result<Vec<SignedAccountOrderFact>, AccountHostValidationError> {
    let root: Value =
        serde_json::from_slice(payload).map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    if root.get("code").and_then(Value::as_str) != Some("00000") {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    let rows = root
        .get("data")
        .and_then(Value::as_array)
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    rows.iter()
        .map(|row| {
            let item = row
                .as_object()
                .ok_or(AccountHostValidationError::SignedSnapshot)?;
            if item.get("category").and_then(Value::as_str) != Some("USDT-FUTURES")
                || item
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value != "tpsl")
            {
                return Err(AccountHostValidationError::SignedSnapshot);
            }
            let position_side = match item.get("posSide").and_then(Value::as_str) {
                Some("long") => PositionSide::Long,
                Some("short") => PositionSide::Short,
                _ => return Err(AccountHostValidationError::SignedSnapshot),
            };
            let side = match item.get("side").and_then(Value::as_str) {
                Some("buy") => OrderSide::Buy,
                Some("sell") => OrderSide::Sell,
                None if position_side == PositionSide::Long => OrderSide::Sell,
                None if position_side == PositionSide::Short => OrderSide::Buy,
                _ => return Err(AccountHostValidationError::SignedSnapshot),
            };
            let quantity = snapshot_decimal(item.get("qty"))?;
            let trigger = match (
                optional_positive_decimal(item.get("takeProfit"))?,
                optional_positive_decimal(item.get("stopLoss"))?,
            ) {
                (Some(value), None)
                    if item.get("tpOrderType").and_then(Value::as_str) == Some("market") =>
                {
                    value
                }
                (None, Some(value))
                    if item.get("slOrderType").and_then(Value::as_str) == Some("market") =>
                {
                    value
                }
                _ => return Err(AccountHostValidationError::SignedSnapshot),
            };
            if quantity <= Decimal::ZERO {
                return Err(AccountHostValidationError::SignedSnapshot);
            }
            if !native_reduce_only(item.get("reduceOnly"))? {
                return Err(AccountHostValidationError::SignedSnapshot);
            }
            Ok(SignedAccountOrderFact {
                client_order_id: required_text(item.get("clientOid"))?.to_owned(),
                venue_order_id: Some(required_text(item.get("orderId"))?.to_owned()),
                symbol: snapshot_symbol(item.get("symbol"))?,
                family: NativeOrderFamily::UmConditional,
                side,
                position_side,
                quantity,
                limit_price: Some(trigger),
                time_in_force: None,
                created_at_ms: snapshot_order_created_at_ms(item.get("createdTime"))?,
                reduce_only: true,
                owner: None,
                external: true,
                state: Some(match item.get("status").and_then(Value::as_str) {
                    Some("pending" | "submitting") => OrderState::New,
                    _ => return Err(AccountHostValidationError::SignedSnapshot),
                }),
                filled_quantity: Some(Decimal::ZERO),
            })
        })
        .collect()
}

fn optional_positive_decimal(
    value: Option<&Value>,
) -> Result<Option<Decimal>, AccountHostValidationError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.is_empty() => Ok(None),
        Some(value) => {
            let value = snapshot_decimal(Some(value))?;
            if value <= Decimal::ZERO {
                return Err(AccountHostValidationError::SignedSnapshot);
            }
            Ok(Some(value))
        }
    }
}

fn native_reduce_only(value: Option<&Value>) -> Result<bool, AccountHostValidationError> {
    match value {
        Some(Value::Bool(value)) => Ok(*value),
        Some(Value::String(value)) if value.eq_ignore_ascii_case("yes") => Ok(true),
        Some(Value::String(value)) if value.eq_ignore_ascii_case("no") => Ok(false),
        _ => Err(AccountHostValidationError::SignedSnapshot),
    }
}

fn required_text(value: Option<&Value>) -> Result<&str, AccountHostValidationError> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or(AccountHostValidationError::SignedSnapshot)
}

pub(super) fn snapshot_order_state(
    value: Option<&Value>,
) -> Result<OrderState, AccountHostValidationError> {
    match value.and_then(Value::as_str) {
        Some("live" | "new") => Ok(OrderState::New),
        Some("partially_filled" | "partially-filled") => Ok(OrderState::PartiallyFilled),
        Some("filled") => Ok(OrderState::Filled),
        Some("cancelled" | "canceled") => Ok(OrderState::Cancelled),
        Some("rejected") => Ok(OrderState::Rejected),
        Some("expired") => Ok(OrderState::Expired),
        _ => Err(AccountHostValidationError::SignedSnapshot),
    }
}

pub(super) fn snapshot_order_created_at_ms(
    value: Option<&Value>,
) -> Result<Option<u64>, AccountHostValidationError> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    let value = match value {
        Value::String(value) => value.parse::<u64>().ok(),
        Value::Number(value) => value.as_u64(),
        _ => return Err(AccountHostValidationError::SignedSnapshot),
    };
    value
        .filter(|value| *value > 0)
        .map(Some)
        .ok_or(AccountHostValidationError::SignedSnapshot)
}
