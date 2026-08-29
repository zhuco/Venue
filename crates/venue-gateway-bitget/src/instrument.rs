//! Pure Bitget UTA v3 linear-perpetual instrument metadata parsing.
//!
//! The parser consumes a request-bound public payload and emits canonical domain metadata. Native
//! identity remains inside this adapter. No transport, capability, writer, or mutation authority
//! is present in this module.

use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use venue_domain::domain::{
    Amount, Asset, Instrument, InstrumentMetadata, InstrumentSnapshot, MarketKind, Precision,
    Price, Symbol,
};
use venue_gateway_api::GatewayBinding;

use crate::{BitgetAccountBinding, public};

pub const BITGET_INSTRUMENT_PARSER_SCHEMA_VERSION: u16 = 1;
pub const BITGET_UTA_FUTURES_CATEGORY: &str = "USDT-FUTURES";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BitgetSymbolType {
    Crypto,
    Metal,
    Stock,
    Commodity,
}

/// Exact request scope and raw response suitable for caller-owned durable evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BitgetRawInstrumentPayload {
    pub parser_schema_version: u16,
    pub binding: GatewayBinding,
    pub requested_native_symbol: String,
    pub generation: u64,
    pub observed_at_ms: u64,
    pub expires_at_ms: u64,
    pub payload_sha256: String,
    pub payload: String,
}

impl BitgetRawInstrumentPayload {
    pub fn new(
        binding: GatewayBinding,
        generation: u64,
        observed_at_ms: u64,
        expires_at_ms: u64,
        payload: String,
    ) -> Result<Self, BitgetInstrumentError> {
        validate_binding(&binding)?;
        let requested_native_symbol = native_symbol(&binding.symbol)?;
        let payload_sha256 = payload_digest(&payload);
        let raw = Self {
            parser_schema_version: BITGET_INSTRUMENT_PARSER_SCHEMA_VERSION,
            binding,
            requested_native_symbol,
            generation,
            observed_at_ms,
            expires_at_ms,
            payload_sha256,
            payload,
        };
        raw.validate()?;
        Ok(raw)
    }

    pub fn validate(&self) -> Result<(), BitgetInstrumentError> {
        validate_binding(&self.binding)?;
        if self.parser_schema_version != BITGET_INSTRUMENT_PARSER_SCHEMA_VERSION
            || self.generation == 0
            || self.observed_at_ms == 0
            || self.expires_at_ms <= self.observed_at_ms
            || self.payload.is_empty()
            || self.requested_native_symbol != native_symbol(&self.binding.symbol)?
            || self.payload_sha256 != payload_digest(&self.payload)
        {
            return Err(BitgetInstrumentError::Metadata);
        }
        Ok(())
    }
}

/// Adapter-owned native mapping plus canonical, expiring instrument metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetInstrumentRules {
    pub raw: BitgetRawInstrumentPayload,
    native_symbol: String,
    pub symbol_type: BitgetSymbolType,
    pub snapshot: InstrumentSnapshot,
    pub maximum_order_quantity: Option<Decimal>,
    pub maximum_market_order_quantity: Option<Decimal>,
}

impl BitgetInstrumentRules {
    #[must_use]
    pub fn canonical_symbol(&self) -> &Symbol {
        &self.snapshot.metadata.instrument.symbol
    }

    #[must_use]
    pub fn native_symbol(&self) -> &str {
        &self.native_symbol
    }
}

/// Parses one exact UTA v3 `GET /api/v3/market/instruments` response.
///
/// The request is symbol-scoped, so zero or multiple rows are ambiguous and rejected. Bitget's
/// documented futures quantity is denominated in the base coin and the response has no separate
/// contract-lot value; consequently canonical metadata intentionally uses `contract=None`.
pub fn parse_instrument_rules(
    raw: BitgetRawInstrumentPayload,
    now_ms: u64,
) -> Result<BitgetInstrumentRules, BitgetInstrumentError> {
    raw.validate()?;
    if now_ms < raw.observed_at_ms {
        return Err(BitgetInstrumentError::NotYetObserved);
    }
    if now_ms >= raw.expires_at_ms {
        return Err(BitgetInstrumentError::Expired);
    }
    let root: Value =
        serde_json::from_str(&raw.payload).map_err(|_| BitgetInstrumentError::Payload)?;
    let root = object(&root)?;
    if root.get("code").and_then(Value::as_str) != Some("00000") {
        return Err(BitgetInstrumentError::Rejected);
    }
    let rows = root
        .get("data")
        .and_then(Value::as_array)
        .ok_or(BitgetInstrumentError::Payload)?;
    let [row] = rows.as_slice() else {
        return Err(BitgetInstrumentError::AmbiguousResponse);
    };
    let row = object(row)?;

    if text(row, "category")? != BITGET_UTA_FUTURES_CATEGORY || text(row, "type")? != "perpetual" {
        return Err(BitgetInstrumentError::Product);
    }
    if text(row, "status")? != "online" {
        return Err(BitgetInstrumentError::TradingStatus);
    }
    let symbol_type = parse_symbol_type(text(row, "symbolType")?)?;
    let base = exact_asset(text(row, "baseCoin")?)?;
    let quote = exact_asset(text(row, "quoteCoin")?)?;
    if quote.as_str() != "USDT" {
        return Err(BitgetInstrumentError::Product);
    }
    let canonical =
        Symbol::new(base.as_str(), quote.as_str()).map_err(|_| BitgetInstrumentError::Symbol)?;
    let payload_native = text(row, "symbol")?;
    let authoritative_native = format!("{}{}", base.as_str(), quote.as_str());
    if payload_native != authoritative_native
        || payload_native != raw.requested_native_symbol
        || canonical != raw.binding.symbol
        || native_symbol(&canonical)? != payload_native
    {
        return Err(BitgetInstrumentError::Symbol);
    }

    let price_digits = precision_digits(row, "pricePrecision")?;
    let quantity_digits = precision_digits(row, "quantityPrecision")?;
    let price_step = positive_decimal(row, "priceMultiplier")?;
    let quantity_step = positive_decimal(row, "quantityMultiplier")?;
    let minimum_quantity = positive_decimal(row, "minOrderQty")?;
    let minimum_notional = positive_decimal(row, "minOrderAmount")?;
    validate_decimal_precision(price_step, price_digits)?;
    validate_decimal_precision(quantity_step, quantity_digits)?;
    validate_decimal_precision(minimum_quantity, quantity_digits)?;

    let price =
        Precision::new(price_step, price_step).map_err(|_| BitgetInstrumentError::Precision)?;
    let quantity = Precision::new(quantity_step, minimum_quantity)
        .map_err(|_| BitgetInstrumentError::Precision)?;
    if !quantity
        .accepts(minimum_quantity)
        .map_err(|_| BitgetInstrumentError::Precision)?
    {
        return Err(BitgetInstrumentError::Precision);
    }
    let maximum_order_quantity =
        optional_maximum_quantity(row.get("maxOrderQty"), &quantity, quantity_digits)?;
    let maximum_market_order_quantity =
        optional_maximum_quantity(row.get("maxMarketOrderQty"), &quantity, quantity_digits)?;
    let instrument = Instrument {
        symbol: canonical,
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(quote.clone()),
        generation: raw.generation,
        price_tick: Price::new(price_step).map_err(|_| BitgetInstrumentError::Precision)?,
        quantity_step,
        minimum_notional: Amount::new(quote, minimum_notional),
    };
    let metadata = InstrumentMetadata::new(instrument, price, quantity, None, true)
        .map_err(|_| BitgetInstrumentError::Metadata)?;
    let snapshot = InstrumentSnapshot::new(metadata, raw.observed_at_ms, raw.expires_at_ms)
        .map_err(|_| BitgetInstrumentError::Metadata)?;
    let identity = snapshot.metadata.identity();
    snapshot
        .require(&identity, raw.generation, now_ms)
        .map_err(|_| BitgetInstrumentError::Metadata)?;
    Ok(BitgetInstrumentRules {
        native_symbol: payload_native.to_owned(),
        raw,
        symbol_type,
        snapshot,
        maximum_order_quantity,
        maximum_market_order_quantity,
    })
}

fn optional_maximum_quantity(
    value: Option<&Value>,
    precision: &Precision,
    digits: u32,
) -> Result<Option<Decimal>, BitgetInstrumentError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = decimal_value(value)?;
    if value.is_zero() {
        return Ok(None);
    }
    if value.is_sign_negative() {
        return Err(BitgetInstrumentError::Precision);
    }
    validate_decimal_precision(value, digits)?;
    if !precision
        .accepts(value)
        .map_err(|_| BitgetInstrumentError::Precision)?
    {
        return Err(BitgetInstrumentError::Precision);
    }
    Ok(Some(value))
}

fn precision_digits(
    object: &Map<String, Value>,
    field: &str,
) -> Result<u32, BitgetInstrumentError> {
    let digits = text(object, field)?
        .parse::<u32>()
        .map_err(|_| BitgetInstrumentError::Precision)?;
    if digits > Decimal::MAX_SCALE {
        return Err(BitgetInstrumentError::Precision);
    }
    Ok(digits)
}

fn validate_decimal_precision(value: Decimal, digits: u32) -> Result<(), BitgetInstrumentError> {
    if value.normalize().scale() > digits {
        return Err(BitgetInstrumentError::Precision);
    }
    Ok(())
}

fn positive_decimal(
    object: &Map<String, Value>,
    field: &str,
) -> Result<Decimal, BitgetInstrumentError> {
    let value = decimal_value(object.get(field).ok_or(BitgetInstrumentError::Payload)?)?;
    if value <= Decimal::ZERO {
        return Err(BitgetInstrumentError::Precision);
    }
    Ok(value)
}

fn decimal_value(value: &Value) -> Result<Decimal, BitgetInstrumentError> {
    match value {
        Value::String(value) => {
            Decimal::from_str(value).map_err(|_| BitgetInstrumentError::Precision)
        }
        Value::Number(value) => {
            Decimal::from_str(&value.to_string()).map_err(|_| BitgetInstrumentError::Precision)
        }
        _ => Err(BitgetInstrumentError::Payload),
    }
}

fn exact_asset(value: &str) -> Result<Asset, BitgetInstrumentError> {
    let asset = Asset::new(value).map_err(|_| BitgetInstrumentError::Symbol)?;
    if asset.as_str() != value {
        return Err(BitgetInstrumentError::Symbol);
    }
    Ok(asset)
}

fn parse_symbol_type(value: &str) -> Result<BitgetSymbolType, BitgetInstrumentError> {
    match value {
        "crypto" => Ok(BitgetSymbolType::Crypto),
        "metal" => Ok(BitgetSymbolType::Metal),
        "stock" => Ok(BitgetSymbolType::Stock),
        "commodity" => Ok(BitgetSymbolType::Commodity),
        _ => Err(BitgetInstrumentError::Product),
    }
}

fn validate_binding(binding: &GatewayBinding) -> Result<(), BitgetInstrumentError> {
    BitgetAccountBinding::UtaUsdtFuturesHedge
        .validate_gateway_binding(binding)
        .map_err(|_| BitgetInstrumentError::Binding)
}

fn native_symbol(symbol: &Symbol) -> Result<String, BitgetInstrumentError> {
    public::native_symbol(symbol).map_err(|_| BitgetInstrumentError::Symbol)
}

fn object(value: &Value) -> Result<&Map<String, Value>, BitgetInstrumentError> {
    value.as_object().ok_or(BitgetInstrumentError::Payload)
}

fn text<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a str, BitgetInstrumentError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or(BitgetInstrumentError::Payload)
}

fn payload_digest(payload: &str) -> String {
    Sha256::digest(payload.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BitgetInstrumentError {
    #[error("Bitget instrument binding is invalid")]
    Binding,
    #[error("Bitget instrument raw metadata is invalid")]
    Metadata,
    #[error("Bitget instrument response is not yet observable")]
    NotYetObserved,
    #[error("Bitget instrument response has expired")]
    Expired,
    #[error("Bitget rejected the instrument request")]
    Rejected,
    #[error("Bitget instrument payload is invalid or incomplete")]
    Payload,
    #[error("Bitget symbol-scoped instrument response is empty or ambiguous")]
    AmbiguousResponse,
    #[error("Bitget instrument is not the required UTA USDT linear perpetual")]
    Product,
    #[error("Bitget instrument is not online for trading")]
    TradingStatus,
    #[error("Bitget instrument canonical/native symbol mapping is inconsistent")]
    Symbol,
    #[error("Bitget instrument precision or quantity bounds are invalid")]
    Precision,
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use venue_gateway_api::{GatewayMode, VenueId};

    use super::*;

    const OFFICIAL_FIXTURE: &str =
        include_str!("../tests/fixtures/bitget_uta_btcusdt_instrument.json");

    fn binding(symbol: &str) -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Bitget,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            symbol.parse()?,
        )?)
    }

    fn raw(payload: String) -> Result<BitgetRawInstrumentPayload, Box<dyn std::error::Error>> {
        Ok(BitgetRawInstrumentPayload::new(
            binding("BTC/USDT")?,
            7,
            100,
            200,
            payload,
        )?)
    }

    fn fixture_value() -> Result<Value, serde_json::Error> {
        serde_json::from_str(OFFICIAL_FIXTURE)
    }

    #[test]
    fn official_uta_fixture_builds_canonical_metadata_without_fake_contract_lots()
    -> Result<(), Box<dyn std::error::Error>> {
        let rules = parse_instrument_rules(raw(OFFICIAL_FIXTURE.to_owned())?, 150)?;
        assert_eq!(rules.native_symbol(), "BTCUSDT");
        assert_eq!(rules.canonical_symbol(), &"BTC/USDT".parse()?);
        assert_eq!(rules.symbol_type, BitgetSymbolType::Crypto);
        assert_eq!(rules.snapshot.metadata.instrument.generation, 7);
        assert_eq!(
            rules.snapshot.metadata.instrument.price_tick.value(),
            Decimal::new(1, 1)
        );
        assert_eq!(
            rules.snapshot.metadata.instrument.quantity_step,
            Decimal::new(1, 4)
        );
        assert_eq!(rules.snapshot.metadata.quantity.minimum, Decimal::new(1, 4));
        assert_eq!(
            rules.snapshot.metadata.instrument.minimum_notional.value,
            Decimal::from(5)
        );
        assert_eq!(rules.snapshot.metadata.contract, None);
        assert_eq!(rules.maximum_order_quantity, Some(Decimal::from(1_200)));
        assert_eq!(
            rules.maximum_market_order_quantity,
            Some(Decimal::from(220))
        );
        Ok(())
    }

    #[test]
    fn exact_one_authoritative_row_and_inverse_symbol_mapping_are_required()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut empty = fixture_value()?;
        empty["data"] = json!([]);
        assert_eq!(
            parse_instrument_rules(raw(empty.to_string())?, 150),
            Err(BitgetInstrumentError::AmbiguousResponse)
        );
        let mut duplicate = fixture_value()?;
        let row = duplicate["data"][0].clone();
        duplicate["data"] = json!([row.clone(), row]);
        assert_eq!(
            parse_instrument_rules(raw(duplicate.to_string())?, 150),
            Err(BitgetInstrumentError::AmbiguousResponse)
        );
        let mut wrong_native = fixture_value()?;
        wrong_native["data"][0]["symbol"] = json!("ETHUSDT");
        assert_eq!(
            parse_instrument_rules(raw(wrong_native.to_string())?, 150),
            Err(BitgetInstrumentError::Symbol)
        );
        let wrong_binding = BitgetRawInstrumentPayload::new(
            binding("ETH/USDT")?,
            7,
            100,
            200,
            OFFICIAL_FIXTURE.to_owned(),
        )?;
        assert_eq!(
            parse_instrument_rules(wrong_binding, 150),
            Err(BitgetInstrumentError::Symbol)
        );
        Ok(())
    }

    #[test]
    fn product_status_and_symbol_type_are_exact() -> Result<(), Box<dyn std::error::Error>> {
        for (field, invalid, expected) in [
            ("category", "COIN-FUTURES", BitgetInstrumentError::Product),
            ("type", "delivery", BitgetInstrumentError::Product),
            ("status", "listed", BitgetInstrumentError::TradingStatus),
            ("symbolType", "unknown", BitgetInstrumentError::Product),
            ("quoteCoin", "USDC", BitgetInstrumentError::Product),
        ] {
            let mut value = fixture_value()?;
            value["data"][0][field] = json!(invalid);
            assert_eq!(
                parse_instrument_rules(raw(value.to_string())?, 150),
                Err(expected)
            );
        }
        Ok(())
    }

    #[test]
    fn generation_time_window_and_raw_hash_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
        assert!(
            BitgetRawInstrumentPayload::new(
                binding("BTC/USDT")?,
                0,
                100,
                200,
                OFFICIAL_FIXTURE.to_owned(),
            )
            .is_err()
        );
        assert_eq!(
            parse_instrument_rules(raw(OFFICIAL_FIXTURE.to_owned())?, 99),
            Err(BitgetInstrumentError::NotYetObserved)
        );
        assert_eq!(
            parse_instrument_rules(raw(OFFICIAL_FIXTURE.to_owned())?, 200),
            Err(BitgetInstrumentError::Expired)
        );
        let mut tampered = raw(OFFICIAL_FIXTURE.to_owned())?;
        tampered.payload.push(' ');
        assert_eq!(
            parse_instrument_rules(tampered, 150),
            Err(BitgetInstrumentError::Metadata)
        );
        Ok(())
    }

    #[test]
    fn precision_minimum_and_maximum_must_be_positive_aligned_and_representable()
    -> Result<(), Box<dyn std::error::Error>> {
        for (field, invalid) in [
            ("priceMultiplier", "0"),
            ("quantityMultiplier", "0"),
            ("minOrderQty", "0.00015"),
            ("minOrderAmount", "0"),
            ("maxOrderQty", "1200.00005"),
        ] {
            let mut value = fixture_value()?;
            value["data"][0][field] = json!(invalid);
            assert_eq!(
                parse_instrument_rules(raw(value.to_string())?, 150),
                Err(BitgetInstrumentError::Precision)
            );
        }
        let mut excess_scale = fixture_value()?;
        excess_scale["data"][0]["pricePrecision"] = json!("0");
        assert_eq!(
            parse_instrument_rules(raw(excess_scale.to_string())?, 150),
            Err(BitgetInstrumentError::Precision)
        );
        Ok(())
    }

    #[test]
    fn zero_maximum_means_unbounded_but_negative_or_malformed_is_not_accepted()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut zero = fixture_value()?;
        zero["data"][0]["maxOrderQty"] = json!("0");
        let rules = parse_instrument_rules(raw(zero.to_string())?, 150)?;
        assert_eq!(rules.maximum_order_quantity, None);

        let mut negative = fixture_value()?;
        negative["data"][0]["maxMarketOrderQty"] = json!("-1");
        assert_eq!(
            parse_instrument_rules(raw(negative.to_string())?, 150),
            Err(BitgetInstrumentError::Precision)
        );
        Ok(())
    }
}
