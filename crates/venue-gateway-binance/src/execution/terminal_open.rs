use super::*;

/// Prepares an explicitly authorized manual Hedge opening order. The scope is request binding,
/// not a claim that account/position facts were refreshed. Binance decides its trading filters.
pub fn prepare_terminal_open_limit(
    rules: &BinanceInstrumentRules,
    scope: &BinancePrivateReadScope,
    intent: &BinancePlaceIntent,
) -> Result<BinancePreparedMutation, BinanceExecutionError> {
    validate_grid_binding(rules, scope, &intent.client_order_id)?;
    if intent.reduce_only
        || intent.time_in_force != BinanceTimeInForce::PostOnly
        || intent.quantity <= Decimal::ZERO
        || intent.quantity % rules.instrument.quantity_step != Decimal::ZERO
    {
        return Err(BinanceExecutionError::Intent);
    }
    validate_place_direction(BinancePositionMode::Hedge, intent)?;
    prepared_for_scope(
        rules,
        scope,
        BinanceMutationKind::PlaceLimit,
        place_limit_parameters(rules, intent, BinancePositionMode::Hedge),
        intent.client_order_id.clone(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BinanceAccountBinding, BinanceConfig, parse_instrument_rules};
    use venue_gateway_api::{GatewayMode, VenueId};

    #[test]
    fn manual_open_needs_no_position_and_leaves_notional_to_exchange()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = GatewayBinding::new(
            VenueId::Binance,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?;
        let config =
            BinanceConfig::for_binding(BinanceAccountBinding::PortfolioMarginUm, &binding)?;
        let rules = parse_instrument_rules(
            include_str!("../../tests/fixtures/exchange_info_btcusdt.json"),
            binding.symbol.clone(),
            1,
        )?;
        let scope = BinancePrivateReadScope::new(&config, &rules, 1, 1, 1000)?;
        let mut intent = BinancePlaceIntent {
            client_order_id: "manual-open-fixture".into(),
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: rules.instrument.quantity_step,
            limit_price: Price::new(Decimal::ONE)?,
            time_in_force: BinanceTimeInForce::PostOnly,
            reduce_only: false,
        };
        assert!(
            intent.quantity * intent.limit_price.value() < rules.instrument.minimum_notional.value
        );
        for (side, leg) in [
            (OrderSide::Buy, PositionSide::Long),
            (OrderSide::Sell, PositionSide::Short),
        ] {
            intent.side = side;
            intent.position_side = leg;
            let prepared = prepare_terminal_open_limit(&rules, &scope, &intent)?;
            assert!(
                !prepared
                    .parameters()
                    .iter()
                    .any(|(name, _)| name == "reduceOnly")
            );
            assert!(
                prepared
                    .parameters()
                    .iter()
                    .any(|(name, value)| name == "timeInForce" && value == "GTX")
            );
            assert!(
                prepared
                    .parameters()
                    .iter()
                    .any(|(name, value)| name == "newOrderRespType" && value == "RESULT")
            );
        }
        intent.reduce_only = true;
        assert!(prepare_terminal_open_limit(&rules, &scope, &intent).is_err());
        intent.reduce_only = false;
        intent.position_side = PositionSide::Net;
        assert!(prepare_terminal_open_limit(&rules, &scope, &intent).is_err());
        intent.position_side = PositionSide::Long;
        assert!(prepare_terminal_open_limit(&rules, &scope, &intent).is_err());
        Ok(())
    }
}
