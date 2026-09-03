use std::str::FromStr;

use rust_decimal::Decimal;
use serde_json::{Map, Value};
use venue_domain::domain::{
    Amount, Asset, Instrument, InstrumentMetadata, MarketKind, Precision, Price, Symbol,
};

use crate::native_symbol;

/// Binance-native rule evidence normalized around the shared canonical instrument.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceInstrumentRules {
    pub native_symbol: String,
    pub instrument: Instrument,
    pub minimum_quantity: Decimal,
    pub maximum_quantity: Decimal,
    pub minimum_price: Decimal,
    pub maximum_price: Decimal,
}

impl BinanceInstrumentRules {
    /// Converts Binance rule evidence into the exchange-neutral planner metadata. Native maximum
    /// bounds remain explicit fields on this type and must be passed alongside the metadata.
    pub fn metadata(&self) -> Result<InstrumentMetadata, BinanceInstrumentError> {
        InstrumentMetadata::new(
            self.instrument.clone(),
            Precision::new(self.instrument.price_tick.value(), self.minimum_price)
                .map_err(|_| BinanceInstrumentError::Rule)?,
            Precision::new(self.instrument.quantity_step, self.minimum_quantity)
                .map_err(|_| BinanceInstrumentError::Rule)?,
            None,
            true,
        )
        .map_err(|_| BinanceInstrumentError::Rule)
    }
}

/// Parses the one USDⓈ-M perpetual entry selected by a canonical symbol.
pub fn parse_instrument_rules(
    payload: &str,
    symbol: Symbol,
    generation: u64,
) -> Result<BinanceInstrumentRules, BinanceInstrumentError> {
    let expected_native = native_symbol(&symbol);
    let entry = select_entry(payload, &expected_native)?;
    parse_entry(&entry, Some(&symbol), generation)
}

/// Resolves a native Binance symbol through exchange-info evidence instead of guessing where the
/// base asset ends and the quote asset starts.
pub fn parse_native_instrument_rules(
    payload: &str,
    expected_native: &str,
    generation: u64,
) -> Result<BinanceInstrumentRules, BinanceInstrumentError> {
    if expected_native.is_empty()
        || !expected_native
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        return Err(BinanceInstrumentError::NativeSymbol);
    }
    let entry = select_entry(payload, expected_native)?;
    parse_entry(&entry, None, generation)
}

fn select_entry(
    payload: &str,
    expected_native: &str,
) -> Result<Map<String, Value>, BinanceInstrumentError> {
    let root: Value = serde_json::from_str(payload).map_err(|_| BinanceInstrumentError::Payload)?;
    let symbols = root
        .get("symbols")
        .and_then(Value::as_array)
        .ok_or(BinanceInstrumentError::Payload)?;
    let mut matches = symbols
        .iter()
        .filter(|entry| entry.get("symbol").and_then(Value::as_str) == Some(expected_native));
    let entry = matches
        .next()
        .and_then(Value::as_object)
        .ok_or(BinanceInstrumentError::Instrument)?;
    if matches.next().is_some() {
        return Err(BinanceInstrumentError::Instrument);
    }
    Ok(entry.clone())
}

fn parse_entry(
    entry: &Map<String, Value>,
    expected_symbol: Option<&Symbol>,
    generation: u64,
) -> Result<BinanceInstrumentRules, BinanceInstrumentError> {
    if entry.get("status").and_then(Value::as_str) != Some("TRADING")
        || entry.get("contractType").and_then(Value::as_str) != Some("PERPETUAL")
    {
        return Err(BinanceInstrumentError::Product);
    }

    let native = text(entry, "symbol")?;
    let symbol = Symbol::new(text(entry, "baseAsset")?, text(entry, "quoteAsset")?)
        .map_err(|_| BinanceInstrumentError::Symbol)?;
    if native_symbol(&symbol) != native
        || expected_symbol.is_some_and(|expected| expected != &symbol)
    {
        return Err(BinanceInstrumentError::Symbol);
    }

    let settlement_asset = text(entry, "marginAsset")?
        .parse::<Asset>()
        .map_err(|_| BinanceInstrumentError::Rule)?;
    let tick = filter_decimal(entry, "PRICE_FILTER", "tickSize")?;
    let step = filter_decimal(entry, "LOT_SIZE", "stepSize")?;
    let minimum_quantity = filter_decimal(entry, "LOT_SIZE", "minQty")?;
    let maximum_quantity = filter_decimal(entry, "LOT_SIZE", "maxQty")?;
    let minimum_price = filter_decimal(entry, "PRICE_FILTER", "minPrice")?;
    let maximum_price = filter_decimal(entry, "PRICE_FILTER", "maxPrice")?;
    let minimum_notional = filter_decimal(entry, "MIN_NOTIONAL", "notional")?;
    if minimum_quantity <= Decimal::ZERO
        || step <= Decimal::ZERO
        || minimum_notional <= Decimal::ZERO
        || minimum_quantity < step
        || minimum_quantity % step != Decimal::ZERO
        || maximum_quantity < minimum_quantity
        || maximum_quantity % step != Decimal::ZERO
        || minimum_price <= Decimal::ZERO
        || maximum_price < minimum_price
    {
        return Err(BinanceInstrumentError::Rule);
    }

    let instrument = Instrument {
        symbol,
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(settlement_asset.clone()),
        generation,
        price_tick: Price::new(tick).map_err(|_| BinanceInstrumentError::Rule)?,
        quantity_step: step,
        minimum_notional: Amount::new(settlement_asset, minimum_notional),
    };
    instrument
        .validate()
        .map_err(|_| BinanceInstrumentError::Rule)?;
    Ok(BinanceInstrumentRules {
        native_symbol: native.to_owned(),
        instrument,
        minimum_quantity,
        maximum_quantity,
        minimum_price,
        maximum_price,
    })
}

fn text<'a>(entry: &'a Map<String, Value>, field: &str) -> Result<&'a str, BinanceInstrumentError> {
    entry
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or(BinanceInstrumentError::Payload)
}

fn filter_decimal(
    entry: &Map<String, Value>,
    filter_type: &str,
    field: &str,
) -> Result<Decimal, BinanceInstrumentError> {
    let filters = entry
        .get("filters")
        .and_then(Value::as_array)
        .ok_or(BinanceInstrumentError::Rule)?;
    let mut matches = filters
        .iter()
        .filter(|filter| filter.get("filterType").and_then(Value::as_str) == Some(filter_type));
    let raw = matches
        .next()
        .and_then(|filter| filter.get(field))
        .and_then(Value::as_str)
        .ok_or(BinanceInstrumentError::Rule)?;
    if matches.next().is_some() {
        return Err(BinanceInstrumentError::Rule);
    }
    Decimal::from_str(raw).map_err(|_| BinanceInstrumentError::Rule)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BinanceInstrumentError {
    #[error("Binance exchange-info payload has an invalid shape")]
    Payload,
    #[error("Binance native symbol is not canonical")]
    NativeSymbol,
    #[error("Binance native and canonical symbols do not match")]
    Symbol,
    #[error("Binance instrument is not an active USDⓈ-M perpetual")]
    Product,
    #[error("Binance instrument rules are absent or invalid")]
    Rule,
    #[error("Binance instrument is absent or ambiguous")]
    Instrument,
}
