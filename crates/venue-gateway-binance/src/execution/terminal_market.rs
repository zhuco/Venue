use super::*;

/// Manual Hedge operations reuse the executor's cached position cap. This does not fetch
/// private surfaces; account authorization and the one-shot ledger remain the caller's job.
pub fn prepare_terminal_market(
    rules: &BinanceInstrumentRules,
    scope: &BinancePrivateReadScope,
    intent: &BinanceMarketIntent,
) -> Result<BinancePreparedMutation, BinanceExecutionError> {
    validate_grid_binding(rules, scope, &intent.client_order_id)?;
    if intent.quantity <= Decimal::ZERO
        || intent.quantity % rules.instrument.quantity_step != Decimal::ZERO
    {
        return Err(BinanceExecutionError::Intent);
    }
    validate_direction(
        BinancePositionMode::Hedge,
        intent.position_side,
        intent.side,
        intent.reduce_only,
    )?;
    prepared_for_scope(
        rules,
        scope,
        BinanceMutationKind::PlaceMarket,
        market_parameters(rules, intent, BinancePositionMode::Hedge),
        intent.client_order_id.clone(),
    )
}
