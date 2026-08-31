use rust_decimal::Decimal;
use venue_domain::domain::{AggressorSide, FieldState, Price};
use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};
use venue_gateway_binance::{
    BinancePublicError, parse_bbo, parse_closed_bar, parse_depth_delta, parse_public_trade,
};

const BBO: &str = include_str!("fixtures/public-bbo.json");
const DELTA: &str = include_str!("fixtures/public-depth-delta.json");
const TRADE: &str = include_str!("fixtures/public-agg-trade.json");
const BAR: &str = include_str!("fixtures/public-kline-closed.json");

fn binding() -> Result<GatewayBinding, Box<dyn std::error::Error>> {
    Ok(GatewayBinding::new(
        VenueId::Binance,
        GatewayMode::Live,
        "00000000-0000-4000-8000-000000000001",
        "BTC/USDT".parse()?,
    )?)
}

#[test]
fn bbo_fixture_binds_native_symbol_times_and_positive_spread()
-> Result<(), Box<dyn std::error::Error>> {
    let envelope = parse_bbo(BBO, &binding()?, 7, 1_720_000_000_130)?;
    let fact = envelope.fact();
    assert_eq!(envelope.native_symbol(), "BTCUSDT");
    assert_eq!(envelope.generation(), 7);
    assert_eq!(envelope.exchange_event_time_ms(), 1_720_000_000_123);
    assert_eq!(envelope.transaction_time_ms(), Some(1_720_000_000_120));
    assert_eq!(envelope.raw_payload(), BBO);
    assert_eq!(fact.symbol.to_string(), "BTC/USDT");
    assert_eq!(fact.generation, 7);
    assert_eq!(fact.update_id, 400_900_217);
    assert_eq!(fact.bid_price, Price::new(Decimal::new(600_001, 1))?);
    assert_eq!(fact.ask_quantity, Decimal::new(750, 3));
    Ok(())
}

#[test]
fn depth_delta_preserves_zero_quantity_as_deletion_and_binds_update_range()
-> Result<(), Box<dyn std::error::Error>> {
    let envelope = parse_depth_delta(DELTA, &binding()?, 8)?;
    let fact = envelope.fact();
    assert_eq!(fact.first_sequence, 400_900_218);
    assert_eq!(fact.previous_sequence, Some(400_900_217));
    assert_eq!(fact.sequence, 400_900_220);
    assert_eq!(fact.exchange_time_ms, Some(1_720_000_000_223));
    assert_eq!(fact.bids[0].quantity, Decimal::ZERO);
    assert_eq!(fact.bids[1].quantity, Decimal::new(25, 1));
    Ok(())
}

#[test]
fn aggregate_trade_requires_complete_native_identity_and_times()
-> Result<(), Box<dyn std::error::Error>> {
    let envelope = parse_public_trade(TRADE, &binding()?, 9, 1_720_000_000_330)?;
    let fact = envelope.fact();
    assert_eq!(fact.aggregate_trade_id, 9_001_u64.into());
    assert_eq!(fact.first_trade_id, Some(8_101));
    assert_eq!(fact.last_trade_id, Some(8_103));
    assert_eq!(fact.exchange_time_ms, 1_720_000_000_323);
    assert_eq!(fact.transaction_time_ms, 1_720_000_000_320);
    assert_eq!(fact.quantity, Decimal::new(20, 3));
    assert_eq!(fact.quote_quantity, Decimal::new(12_000_030, 4));
    assert_eq!(fact.aggressor, FieldState::Known(AggressorSide::Buy));
    Ok(())
}

#[test]
fn closed_bar_keeps_the_complete_native_payload_beside_the_ohlc_subset()
-> Result<(), Box<dyn std::error::Error>> {
    let envelope = parse_closed_bar(BAR, &binding()?, 10, 1_720_000_020_010)?;
    let fact = envelope.fact();
    assert_eq!(envelope.raw_payload(), BAR);
    assert!(envelope.raw_payload().contains(r#""v":"2.50""#));
    assert!(envelope.raw_payload().contains(r#""n":20"#));
    assert_eq!(fact.open_time_ms, 1_719_999_960_000);
    assert_eq!(fact.close_time_ms, 1_720_000_019_999);
    assert_eq!(fact.interval_ms, 60_000);
    assert_eq!(fact.sequence, 28_666_667);
    assert_eq!(fact.high, Price::new(Decimal::new(600_200, 1))?);
    Ok(())
}

#[test]
fn symbol_generation_event_time_and_ranges_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let binding = binding()?;
    assert_eq!(
        parse_bbo(&BBO.replace("BTCUSDT", "ETHUSDT"), &binding, 7, 1),
        Err(BinancePublicError::Symbol)
    );
    assert_eq!(
        parse_bbo(BBO, &binding, 0, 1),
        Err(BinancePublicError::Generation)
    );
    assert_eq!(
        parse_bbo(&BBO.replace("1720000000123", "0"), &binding, 7, 1),
        Err(BinancePublicError::Value)
    );
    assert_eq!(
        parse_depth_delta(&DELTA.replace("400900217", "400900218"), &binding, 8),
        Err(BinancePublicError::Sequence)
    );
    assert_eq!(
        parse_public_trade(&TRADE.replace("\"l\":8103", "\"l\":8100"), &binding, 9, 1),
        Err(BinancePublicError::Sequence)
    );
    Ok(())
}

#[test]
fn invalid_prices_quantities_and_unclosed_or_incomplete_bars_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let binding = binding()?;
    assert_eq!(
        parse_bbo(
            &BBO.replace("\"B\":\"1.250\"", "\"B\":\"0\""),
            &binding,
            7,
            1
        ),
        Err(BinancePublicError::Value)
    );
    assert_eq!(
        parse_depth_delta(&DELTA.replace("\"2.5\"", "\"-2.5\""), &binding, 8),
        Err(BinancePublicError::Value)
    );
    assert_eq!(
        parse_public_trade(
            &TRADE.replace("\"q\":\"0.020\"", "\"q\":\"0\""),
            &binding,
            9,
            1
        ),
        Err(BinancePublicError::Value)
    );
    assert_eq!(
        parse_closed_bar(&BAR.replace("\"x\":true", "\"x\":false"), &binding, 10, 1),
        Err(BinancePublicError::BarNotClosed)
    );
    assert_eq!(
        parse_closed_bar(&BAR.replace(",\"V\":\"1.20\"", ""), &binding, 10, 1),
        Err(BinancePublicError::Payload)
    );
    Ok(())
}
