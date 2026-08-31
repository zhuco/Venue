use super::*;

pub(super) fn normalize_fresh_limit(
    intent: &AccountLimitNormalizationIntent,
    rules: &BinanceInstrumentRules,
    binding: &GatewayBinding,
    payload: &[u8],
    now: u64,
) -> Result<ExecutionCommand, AccountHostValidationError> {
    intent.validate()?;
    if intent.owner.symbol != rules.instrument.symbol
        || intent.owner.symbol != binding.symbol
        || intent.owner.account != binding.trading_account_id
        || intent.owner.exchange != binding.venue.as_str()
    {
        return Err(AccountHostValidationError::Scope);
    }
    if !matches!(
        (intent.position_side, intent.side, intent.reduce_only),
        (PositionSide::Long, OrderSide::Buy, false)
            | (PositionSide::Short, OrderSide::Sell, false)
            | (PositionSide::Long, OrderSide::Sell, true)
            | (PositionSide::Short, OrderSide::Buy, true)
    ) {
        return Err(AccountHostValidationError::Command);
    }
    let bbo: Value =
        serde_json::from_slice(payload).map_err(|_| AccountHostValidationError::Command)?;
    let timestamp = bbo
        .get("time")
        .and_then(Value::as_u64)
        .filter(|time| *time > 0)
        .ok_or(AccountHostValidationError::Command)?;
    if bbo.get("symbol").and_then(Value::as_str) != Some(rules.native_symbol.as_str())
        || timestamp > now
        || now - timestamp > 3_000
    {
        return Err(AccountHostValidationError::Command);
    }
    let positive = |field: &str| {
        bbo.get(field)
            .and_then(Value::as_str)
            .and_then(|raw| raw.parse::<Decimal>().ok())
            .filter(|value| *value > Decimal::ZERO)
            .ok_or(AccountHostValidationError::Command)
    };
    let bid = positive("bidPrice")?;
    let ask = positive("askPrice")?;
    positive("bidQty")?;
    positive("askQty")?;
    if bid >= ask
        || [bid, ask].iter().any(|price| {
            *price < rules.minimum_price
                || *price > rules.maximum_price
                || *price % rules.instrument.price_tick.value() != Decimal::ZERO
        })
    {
        return Err(AccountHostValidationError::Command);
    }
    let price = match intent.side {
        OrderSide::Buy => bid,
        OrderSide::Sell => ask,
    };
    let quantity = intent
        .quote_delta
        .checked_div(price)
        .map(|value| value - value % rules.instrument.quantity_step)
        .filter(|value| *value >= rules.minimum_quantity && *value <= rules.maximum_quantity)
        .ok_or(AccountHostValidationError::Command)?;
    let notional = quantity
        .checked_mul(price)
        .ok_or(AccountHostValidationError::Command)?;
    if notional > intent.quote_delta
        || (!intent.reduce_only && notional < rules.instrument.minimum_notional.value)
    {
        return Err(AccountHostValidationError::Command);
    }
    Ok(ExecutionCommand::PlaceLimit(OrderCommand {
        command_id: intent.command_id.clone(),
        client_order_id: intent.client_order_id.clone(),
        owner: intent.owner.clone(),
        side: intent.side,
        position_side: intent.position_side,
        quantity,
        limit_price: Price::new(price).map_err(|_| AccountHostValidationError::Command)?,
        reduce_only: intent.reduce_only,
    }))
}
