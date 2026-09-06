use std::collections::BTreeSet;

use rust_decimal::Decimal;
use serde_json::Value;
use venue_execution::{AccountHostValidationError, SignedConditionalOrderFact};

use super::{
    snapshot_bool, snapshot_decimal, snapshot_identifier, snapshot_position_side, snapshot_rules,
    snapshot_side, snapshot_text,
};

pub(super) fn snapshot_conditional_order_facts(
    catalogue: &str,
    algo_rows: &[serde_json::Map<String, Value>],
    generation: u64,
) -> Result<Vec<SignedConditionalOrderFact>, AccountHostValidationError> {
    let mut identities = BTreeSet::new();
    let mut facts = Vec::with_capacity(algo_rows.len());
    for row in algo_rows {
        if snapshot_text(row, "orderType")? != "STOP_MARKET" || snapshot_bool(row, "closePosition")?
        {
            continue;
        }
        let native = snapshot_text(row, "symbol")?;
        let rules = snapshot_rules(catalogue, native, generation)?;
        let venue_order_id = snapshot_identifier(row, "algoId")?;
        let client_order_id = snapshot_text(row, "clientAlgoId")?.to_owned();
        if !identities.insert((rules.instrument.symbol.clone(), venue_order_id.clone())) {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        let quantity = snapshot_decimal(row, "quantity")?;
        let trigger_price = snapshot_decimal(row, "triggerPrice")?;
        if quantity <= Decimal::ZERO || trigger_price <= Decimal::ZERO {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        let created_at_ms = match row.get("createTime").or_else(|| row.get("time")) {
            None | Some(Value::Null) => None,
            Some(Value::Number(value)) => value.as_u64().filter(|value| *value > 0),
            _ => return Err(AccountHostValidationError::SignedSnapshot),
        };
        facts.push(SignedConditionalOrderFact {
            client_order_id,
            venue_order_id,
            symbol: rules.instrument.symbol,
            side: snapshot_side(row)?,
            position_side: snapshot_position_side(row)?,
            quantity,
            trigger_price,
            working_type: snapshot_text(row, "workingType")?.to_owned(),
            reduce_only: snapshot_bool(row, "reduceOnly")?,
            created_at_ms,
        });
    }
    Ok(facts)
}
