use super::*;

pub(super) fn normalize_priced_limit(
    intent: &AccountPricedLimitIntent,
    rules: &GateContractRules,
) -> Result<ExecutionCommand, AccountHostValidationError> {
    intent.validate()?;
    let base = &intent.intent;
    let price = intent.limit_price.value();
    if base.owner.symbol != rules.instrument.symbol
        || price % rules.instrument.price_tick.value() != Decimal::ZERO
    {
        return Err(AccountHostValidationError::Command);
    }
    let cap = intent.quantity_cap()?;
    let quantity = cap
        .checked_sub(cap % rules.quanto_multiplier)
        .filter(|value| *value > Decimal::ZERO && *value >= rules.minimum_quantity())
        .ok_or(AccountHostValidationError::Command)?;
    rules
        .native_order_contracts_checked(quantity)
        .map_err(|_| AccountHostValidationError::Command)?;
    let notional = quantity
        .checked_mul(price)
        .ok_or(AccountHostValidationError::Command)?;
    if notional > base.quote_delta || notional < rules.instrument.minimum_notional.value {
        return Err(AccountHostValidationError::Command);
    }
    let command = OrderCommand {
        time_in_force: intent.time_in_force,
        command_id: base.command_id.clone(),
        client_order_id: base.client_order_id.clone(),
        owner: base.owner.clone(),
        side: base.side,
        position_side: base.position_side,
        quantity,
        limit_price: intent.limit_price,
        reduce_only: base.reduce_only,
    };
    command
        .validate()
        .map_err(|_| AccountHostValidationError::Command)?;
    Ok(ExecutionCommand::PlaceLimit(command))
}
