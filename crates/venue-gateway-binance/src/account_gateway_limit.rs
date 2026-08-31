use super::*;

pub(super) fn readback_policy_matches_command(
    command: &ExecutionCommand,
    order: &venue_domain::domain::Order,
) -> bool {
    match command {
        ExecutionCommand::PlaceLimit(place) => {
            order.time_in_force == FieldState::Known(place.time_in_force)
        }
        ExecutionCommand::Cancel(_)
        | ExecutionCommand::PlaceMarket(_)
        | ExecutionCommand::MarketReduce(_)
        | ExecutionCommand::StopMarketCloseAll(_)
        | ExecutionCommand::StopMarketFullPosition(_) => true,
    }
}

pub(super) fn normalize_fresh_limit(
    intent: &AccountLimitNormalizationIntent,
    rules: &BinanceInstrumentRules,
    binding: &GatewayBinding,
    payload: &[u8],
    now: u64,
) -> Result<ExecutionCommand, AccountHostValidationError> {
    validate_limit_intent(intent, rules, binding)?;
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
        time_in_force: Default::default(),
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

pub(super) fn normalize_priced_limit(
    intent: &AccountPricedLimitIntent,
    rules: &BinanceInstrumentRules,
    binding: &GatewayBinding,
) -> Result<ExecutionCommand, AccountHostValidationError> {
    intent.validate()?;
    let base = &intent.intent;
    validate_limit_intent(base, rules, binding)?;
    let price = intent.limit_price.value();
    if price < rules.minimum_price
        || price > rules.maximum_price
        || price % rules.instrument.price_tick.value() != Decimal::ZERO
    {
        return Err(AccountHostValidationError::Command);
    }
    let cap = intent.quantity_cap()?;
    let quantity = cap
        .checked_sub(cap % rules.instrument.quantity_step)
        .filter(|value| *value >= rules.minimum_quantity && *value <= rules.maximum_quantity)
        .ok_or(AccountHostValidationError::Command)?;
    let notional = quantity
        .checked_mul(price)
        .ok_or(AccountHostValidationError::Command)?;
    if notional > base.quote_delta
        || (!base.reduce_only && notional < rules.instrument.minimum_notional.value)
    {
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

pub(super) fn normalize_priced_limit_intent(
    gateway: &mut BinanceAccountGateway,
    intent: &AccountPricedLimitIntent,
) -> Result<ExecutionCommand, AccountHostValidationError> {
    intent.validate()?;
    let binding = gateway.config.gateway_binding();
    if intent.intent.owner.exchange != binding.venue.as_str()
        || intent.intent.owner.account != binding.trading_account_id
    {
        return Err(AccountHostValidationError::Scope);
    }
    let rules = gateway
        .rules_by_symbol
        .get(&intent.intent.owner.symbol)
        .cloned()
        .ok_or(AccountHostValidationError::Scope)?;
    let current = gateway
        .runtime
        .block_on(fetch_rules_catalog(
            &gateway.transport,
            binding,
            &BTreeSet::from([intent.intent.owner.symbol.clone()]),
            rules.instrument.generation,
        ))
        .map_err(|_| AccountHostValidationError::Command)?;
    if current.get(&intent.intent.owner.symbol) != Some(&rules) {
        return Err(AccountHostValidationError::Command);
    }
    normalize_priced_limit(
        intent,
        &rules,
        &binding_for_symbol(binding, intent.intent.owner.symbol.clone())
            .map_err(|_| AccountHostValidationError::Scope)?,
    )
}

pub(super) fn snapshot_created_at_ms(
    row: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<Option<u64>, AccountHostValidationError> {
    match row.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_u64()
            .filter(|value| *value > 0)
            .map(Some)
            .ok_or(AccountHostValidationError::SignedSnapshot),
        Some(Value::String(value)) => value
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0)
            .map(Some)
            .ok_or(AccountHostValidationError::SignedSnapshot),
        Some(_) => Err(AccountHostValidationError::SignedSnapshot),
    }
}

pub(super) fn snapshot_regular_order_quantities(
    row: &serde_json::Map<String, Value>,
) -> Result<(Decimal, Decimal), AccountHostValidationError> {
    let quantity = snapshot_decimal(row, "origQty")?;
    let filled_quantity = snapshot_decimal(row, "executedQty")?;
    let remaining_quantity = quantity
        .checked_sub(filled_quantity)
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    if quantity <= Decimal::ZERO || remaining_quantity <= Decimal::ZERO {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    Ok((quantity, filled_quantity))
}

fn validate_limit_intent(
    intent: &AccountLimitNormalizationIntent,
    rules: &BinanceInstrumentRules,
    binding: &GatewayBinding,
) -> Result<(), AccountHostValidationError> {
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
    Ok(())
}
