use rust_decimal::Decimal;
use venue_domain::domain::{MarketKind, Price, Symbol};
use venue_gateway_binance::{
    BinanceInstrumentError, parse_instrument_rules, parse_native_instrument_rules,
};

const EXCHANGE_INFO: &str = include_str!("fixtures/exchange_info_btcusdt.json");

#[test]
fn fixture_round_trips_native_identity_and_normalizes_safety_rules()
-> Result<(), Box<dyn std::error::Error>> {
    let expected: Symbol = "BTC/USDT".parse()?;
    let canonical = parse_instrument_rules(EXCHANGE_INFO, expected.clone(), 17)?;
    let native = parse_native_instrument_rules(EXCHANGE_INFO, "BTCUSDT", 17)?;

    assert_eq!(canonical, native);
    assert_eq!(native.native_symbol, "BTCUSDT");
    assert_eq!(native.instrument.symbol, expected);
    assert_eq!(native.instrument.market, MarketKind::LinearPerpetual);
    assert_eq!(
        native
            .instrument
            .settlement_asset
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("USDT")
    );
    assert_eq!(native.instrument.generation, 17);
    assert_eq!(
        native.instrument.price_tick,
        Price::new(Decimal::new(1, 1))?
    );
    assert_eq!(native.instrument.quantity_step, Decimal::new(1, 3));
    assert_eq!(native.minimum_quantity, Decimal::new(1, 3));
    assert_eq!(native.maximum_quantity, Decimal::new(1000, 0));
    assert_eq!(native.minimum_price, Decimal::new(1, 1));
    assert_eq!(native.maximum_price, Decimal::new(1_000_000, 0));
    assert_eq!(native.instrument.minimum_notional.value, Decimal::new(5, 0));
    Ok(())
}

#[test]
fn native_resolution_never_guesses_asset_boundaries() {
    assert_eq!(
        parse_native_instrument_rules(EXCHANGE_INFO, "BTCUSD", 17),
        Err(BinanceInstrumentError::Instrument)
    );
    assert_eq!(
        parse_native_instrument_rules(EXCHANGE_INFO, "btcusdt", 17),
        Err(BinanceInstrumentError::NativeSymbol)
    );

    let inconsistent = EXCHANGE_INFO.replacen("\"baseAsset\": \"BTC\"", "\"baseAsset\": \"BT\"", 1);
    assert_eq!(
        parse_native_instrument_rules(&inconsistent, "BTCUSDT", 17),
        Err(BinanceInstrumentError::Symbol)
    );
}

#[test]
fn inactive_or_non_perpetual_products_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let symbol: Symbol = "BTC/USDT".parse()?;
    let halted = EXCHANGE_INFO.replacen("\"status\": \"TRADING\"", "\"status\": \"BREAK\"", 1);
    assert_eq!(
        parse_instrument_rules(&halted, symbol.clone(), 17),
        Err(BinanceInstrumentError::Product)
    );
    let delivery = EXCHANGE_INFO.replacen(
        "\"contractType\": \"PERPETUAL\"",
        "\"contractType\": \"CURRENT_QUARTER\"",
        1,
    );
    assert_eq!(
        parse_instrument_rules(&delivery, symbol, 17),
        Err(BinanceInstrumentError::Product)
    );
    Ok(())
}

#[test]
fn missing_duplicate_or_misaligned_rules_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let symbol: Symbol = "BTC/USDT".parse()?;
    let missing = EXCHANGE_INFO.replacen(
        "{ \"filterType\": \"MIN_NOTIONAL\", \"notional\": \"5\" }",
        "{ \"filterType\": \"PERCENT_PRICE\" }",
        1,
    );
    assert_eq!(
        parse_instrument_rules(&missing, symbol.clone(), 17),
        Err(BinanceInstrumentError::Rule)
    );

    let duplicate = EXCHANGE_INFO.replacen(
        "{ \"filterType\": \"MIN_NOTIONAL\", \"notional\": \"5\" }",
        "{ \"filterType\": \"MIN_NOTIONAL\", \"notional\": \"5\" }, { \"filterType\": \"MIN_NOTIONAL\", \"notional\": \"5\" }",
        1,
    );
    assert_eq!(
        parse_instrument_rules(&duplicate, symbol.clone(), 17),
        Err(BinanceInstrumentError::Rule)
    );

    let misaligned = EXCHANGE_INFO.replacen("\"minQty\": \"0.001\"", "\"minQty\": \"0.0015\"", 1);
    assert_eq!(
        parse_instrument_rules(&misaligned, symbol.clone(), 17),
        Err(BinanceInstrumentError::Rule)
    );
    assert_eq!(
        parse_instrument_rules(EXCHANGE_INFO, symbol, 0),
        Err(BinanceInstrumentError::Rule)
    );
    Ok(())
}

#[test]
fn duplicate_native_entries_are_ambiguous() -> Result<(), Box<dyn std::error::Error>> {
    let mut duplicate = serde_json::from_str::<serde_json::Value>(EXCHANGE_INFO)?;
    let symbols = duplicate
        .get_mut("symbols")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or("fixture symbols must be an array")?;
    let first = symbols
        .first()
        .cloned()
        .ok_or("fixture must not be empty")?;
    symbols.push(first);
    let duplicate = serde_json::to_string(&duplicate)?;
    let symbol: Symbol = "BTC/USDT".parse()?;
    assert_eq!(
        parse_instrument_rules(&duplicate, symbol, 17),
        Err(BinanceInstrumentError::Instrument)
    );
    Ok(())
}

#[test]
fn inverted_or_missing_maximum_bounds_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let symbol: Symbol = "BTC/USDT".parse()?;
    for malformed in [
        EXCHANGE_INFO.replacen("\"maxQty\": \"1000\"", "\"maxQty\": \"0.0001\"", 1),
        EXCHANGE_INFO.replacen("\"maxPrice\": \"1000000\"", "\"maxPrice\": \"0.01\"", 1),
        EXCHANGE_INFO.replacen("\"maxQty\": \"1000\", ", "", 1),
    ] {
        assert_eq!(
            parse_instrument_rules(&malformed, symbol.clone(), 17),
            Err(BinanceInstrumentError::Rule)
        );
    }
    Ok(())
}
