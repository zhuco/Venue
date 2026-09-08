use super::*;
use venue_domain::domain::{Order, Price};
use venue_execution::{AccountQuoteToUsdtRate, AccountRiskAmount};

fn rules() -> Result<BinanceInstrumentRules, Box<dyn std::error::Error>> {
    let mut rules = parse_instrument_rules(
        include_str!(
            "../../../../crates/venue-gateway-binance/tests/fixtures/exchange_info_btcusdt.json"
        ),
        "BTC/USDT".parse()?,
        7,
    )?;
    rules.instrument.symbol = "DOGE/USDC".parse()?;
    rules.instrument.quantity_step = Decimal::ONE;
    rules.minimum_quantity = Decimal::ONE;
    rules.maximum_quantity = Decimal::from(30_000_000);
    rules.instrument.minimum_notional.value = Decimal::from(5);
    Ok(rules)
}

fn context() -> CopyRiskContext {
    CopyRiskContext {
        round_open_quantity_up: true,
        max_order_notional: Decimal::from(5),
        max_total_notional: Decimal::from(100),
        max_deviation_bps: 5000,
        source_price: Decimal::new(9111, 5),
        source_occurred_ms: 1000,
    }
}

#[test]
fn five_quote_units_produce_the_smallest_valid_doge_lot() -> Result<(), Box<dyn std::error::Error>>
{
    let rules = rules()?;
    let price = context().source_price;
    let requested = Decimal::from(5) / price;
    let quantity = normalize_mirror_open_quantity(requested, price, &rules)?;
    assert_eq!(quantity, Decimal::from(55));
    assert_eq!(quantity * price, Decimal::new(501105, 5));
    assert_eq!(
        normalize_mirror_open_quantity(quantity, price, &rules)?,
        quantity
    );
    assert_eq!(
        normalize_mirror_open_quantity(Decimal::ONE, price, &rules)?,
        quantity
    );
    assert_eq!(
        normalize_mirror_open_quantity(Decimal::from(50), Decimal::new(1, 1), &rules)?,
        Decimal::from(50)
    );
    Ok(())
}

#[test]
fn rounding_obeys_fractional_steps_minimum_lots_and_maximums()
-> Result<(), Box<dyn std::error::Error>> {
    let mut rules = rules()?;
    rules.instrument.quantity_step = Decimal::new(25, 2);
    rules.minimum_quantity = Decimal::new(75, 2);
    assert_eq!(
        normalize_mirror_open_quantity(Decimal::new(76, 2), Decimal::from(10), &rules)?,
        Decimal::ONE
    );
    assert_eq!(
        normalize_mirror_open_quantity(Decimal::new(1, 1), Decimal::from(10), &rules)?,
        Decimal::new(75, 2)
    );
    rules.maximum_quantity = Decimal::new(75, 2);
    assert!(
        normalize_mirror_open_quantity(Decimal::new(76, 2), Decimal::from(10), &rules).is_err()
    );
    assert!(normalize_mirror_open_quantity(Decimal::ZERO, Decimal::from(10), &rules).is_err());
    assert!(normalize_mirror_open_quantity(Decimal::ONE, Decimal::ZERO, &rules).is_err());
    rules.instrument.quantity_step = Decimal::ZERO;
    assert!(normalize_mirror_open_quantity(Decimal::ONE, Decimal::ONE, &rules).is_err());
    Ok(())
}

fn risk(
    total: Decimal,
    rate: Decimal,
) -> Result<(GatewayBinding, AccountRiskEvidence), Box<dyn std::error::Error>> {
    let binding = GatewayBinding::new(
        VenueId::Binance,
        GatewayMode::Live,
        "00000000-0000-4000-8000-000000000001",
        "DOGE/USDC".parse()?,
    )?;
    let evidence = AccountRiskEvidence::complete_with_usdt_valuation(
        binding.clone(),
        1000,
        7,
        vec![AccountRiskAmount {
            asset: Asset::new("USDT")?,
            value: total,
        }],
        vec![],
        vec![AccountQuoteToUsdtRate {
            asset: Asset::new("USDC")?,
            usdt_per_asset: rate,
            observed_at_ms: 1000,
            private_generation: 7,
        }],
    )?;
    Ok((binding, evidence))
}

#[test]
fn rounding_allowance_never_bypasses_total_budget_or_allows_an_extra_lot()
-> Result<(), Box<dyn std::error::Error>> {
    let rules = rules()?;
    let mut context = context();
    let requested = Decimal::from(5) / context.source_price;
    let (binding, evidence) = risk(Decimal::ZERO, Decimal::ONE)?;
    check_mirror_limit_risk(
        &context,
        &binding,
        &evidence,
        &rules,
        requested,
        context.source_price,
        1000,
    )?;
    assert_eq!(
        check_mirror_limit_risk(
            &context,
            &binding,
            &evidence,
            &rules,
            Decimal::from(56),
            context.source_price,
            1000
        ),
        Err(BinanceExecutionError::Risk(CopyRiskRejection::TotalLimit))
    );
    let (_, full) = risk(Decimal::from(95), Decimal::ONE)?;
    assert_eq!(
        check_mirror_limit_risk(
            &context,
            &binding,
            &full,
            &rules,
            requested,
            context.source_price,
            1000
        ),
        Err(BinanceExecutionError::Risk(CopyRiskRejection::TotalLimit))
    );
    let (_, fx) = risk(Decimal::ZERO, Decimal::new(125, 2))?;
    context.max_total_notional = Decimal::from(6);
    assert!(
        check_mirror_limit_risk(
            &context,
            &binding,
            &fx,
            &rules,
            requested,
            context.source_price,
            1000
        )
        .is_err()
    );
    context.max_total_notional = Decimal::MAX - Decimal::ONE;
    context.max_order_notional = Decimal::from(100_000_000);
    check_mirror_limit_risk(
        &context,
        &binding,
        &evidence,
        &rules,
        requested,
        context.source_price,
        1000,
    )?;
    Ok(())
}

#[test]
fn persisted_policy_keeps_legacy_recovery_and_close_quantities_unchanged()
-> Result<(), Box<dyn std::error::Error>> {
    let rules = rules()?;
    let context = context();
    let mut json = serde_json::to_value(&context)?;
    json.as_object_mut()
        .ok_or("risk is not an object")?
        .remove("round_open_quantity_up");
    let legacy: CopyRiskContext = serde_json::from_value(json)?;
    assert!(!legacy.round_open_quantity_up);
    let mut request = super::super::tests::grid_place_request(0)?;
    request.origin = venue_control_protocol::kol::ExecutorCommandOrigin::Copy;
    request.symbol = "DOGE/USDC".parse()?;
    request.order_kind = ExecutionOrderKind::Limit {
        time_in_force: LimitTimeInForce::PostOnly,
        side: OrderSide::Sell,
        position_side: PositionSide::Short,
        quantity: Decimal::from(5) / context.source_price,
        price: context.source_price,
        reducing: false,
    };
    request.copy_risk = Some(serde_json::from_value(serde_json::to_value(&context)?)?);
    let requested = place_shape(&request)?.2;
    assert_eq!(
        normalize_request_quantity(&request, requested, &rules)?,
        Decimal::from(55)
    );
    let mut order = Order {
        order_id: "native-child".into(),
        client_order_id: FieldState::Known(request.client_order_id.clone()),
        symbol: request.symbol.clone(),
        side: OrderSide::Sell,
        position_side: FieldState::Known(PositionSide::Short),
        purpose: FieldState::Missing,
        state: OrderState::New,
        quantity: Decimal::from(55),
        filled_quantity: Decimal::ZERO,
        limit_price: Some(Price::new(context.source_price)?),
        time_in_force: FieldState::Known(LimitTimeInForce::PostOnly),
        average_price: FieldState::Missing,
        reduce_only: false,
    };
    assert!(exact_place_matches(&request, &order, &rules)?);
    order.quantity = Decimal::from(56);
    assert!(!exact_place_matches(&request, &order, &rules)?);
    request.copy_risk = Some(legacy);
    assert_eq!(
        normalize_request_quantity(&request, requested, &rules)?,
        Decimal::from(54)
    );
    order.quantity = Decimal::from(54);
    assert!(exact_place_matches(&request, &order, &rules)?);
    request.copy_risk = Some(context);
    if let ExecutionOrderKind::Limit { reducing, side, .. } = &mut request.order_kind {
        *reducing = true;
        *side = OrderSide::Buy;
    }
    assert_eq!(
        normalize_request_quantity(&request, requested, &rules)?,
        Decimal::from(54)
    );
    assert!(normalize_request_quantity(&request, Decimal::new(5, 1), &rules).is_err());
    Ok(())
}
