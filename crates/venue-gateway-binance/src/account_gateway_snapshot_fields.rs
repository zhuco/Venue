use serde_json::Value;
use venue_domain::domain::{LimitTimeInForce, OrderState};
use venue_execution::AccountHostValidationError;

pub(super) fn snapshot_order_state(value: &str) -> Result<OrderState, AccountHostValidationError> {
    match value {
        "NEW" | "ACTIVE" => Ok(OrderState::New),
        "PARTIALLY_FILLED" => Ok(OrderState::PartiallyFilled),
        "FILLED" => Ok(OrderState::Filled),
        "CANCELED" => Ok(OrderState::Cancelled),
        "EXPIRED" | "EXPIRED_IN_MATCH" => Ok(OrderState::Expired),
        "REJECTED" => Ok(OrderState::Rejected),
        _ => Err(AccountHostValidationError::SignedSnapshot),
    }
}

pub(super) fn snapshot_limit_time_in_force(
    row: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<Option<LimitTimeInForce>, AccountHostValidationError> {
    match row.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => match value.as_str() {
            "GTX" => Ok(Some(LimitTimeInForce::PostOnly)),
            "GTC" => Ok(Some(LimitTimeInForce::Gtc)),
            _ => Ok(None),
        },
        Some(_) => Err(AccountHostValidationError::SignedSnapshot),
    }
}

pub(super) fn snapshot_limit_policy(
    row: &serde_json::Map<String, Value>,
) -> Result<Option<LimitTimeInForce>, AccountHostValidationError> {
    // Price and GTC alone do not prove an ordinary limit order (e.g. STOP limits).
    if row.get("type").and_then(Value::as_str) == Some("LIMIT") {
        snapshot_limit_time_in_force(row, "timeInForce")
    } else {
        Ok(None)
    }
}
