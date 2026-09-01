use serde_json::{Map, Value};
use venue_execution::AccountHostValidationError;

pub(super) fn algo_order_rows(
    payload: &[u8],
) -> Result<Vec<Map<String, Value>>, AccountHostValidationError> {
    let value: Value =
        serde_json::from_slice(payload).map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let page = value
        .as_object()
        .ok_or(AccountHostValidationError::RiskEvidence)?;
    let orders = page
        .get("orders")
        .and_then(Value::as_array)
        .ok_or(AccountHostValidationError::RiskEvidence)?;
    let total = page
        .get("total")
        .and_then(Value::as_u64)
        .ok_or(AccountHostValidationError::RiskEvidence)?;
    if usize::try_from(total).ok() != Some(orders.len()) {
        return Err(AccountHostValidationError::RiskEvidence);
    }
    orders
        .iter()
        .map(|row| {
            row.as_object()
                .cloned()
                .ok_or(AccountHostValidationError::RiskEvidence)
        })
        .collect()
}
