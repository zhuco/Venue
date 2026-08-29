//! Pure Bybit V5 USDT-linear public protocol parsing.
//!
//! This module has no transport, credentials, runtime, writer, or persistence side effects. Raw
//! payloads retain their exact bytes and the full gateway binding so a response cannot be
//! relabelled across venue, account, symbol, or TEST/LIVE mode before normalization.

use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use venue_domain::domain::{
    Amount, Asset, FieldState, Instrument, MarketKind, MarketLevel, MarketSnapshot, Price, Symbol,
    UnknownReason,
};
use venue_gateway_api::GatewayBinding;

use crate::{BybitGatewayBinding, endpoints};

pub const BYBIT_PUBLIC_PARSER_SCHEMA_VERSION: u16 = 1;
pub const BYBIT_LINEAR_CATEGORY: &str = "linear";
const BYBIT_LINEAR_CONTRACT_TYPE: &str = "LinearPerpetual";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BybitPublicSource {
    LinearInstrument,
    RestOrderBook,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BybitRawPublicPayload {
    pub parser_schema_version: u16,
    pub source: BybitPublicSource,
    pub binding: GatewayBinding,
    pub native_symbol: String,
    pub generation: u64,
    pub received_at_ms: u64,
    pub payload_sha256: String,
    pub payload: String,
}

impl BybitRawPublicPayload {
    pub fn new(
        binding: &BybitGatewayBinding,
        source: BybitPublicSource,
        generation: u64,
        received_at_ms: u64,
        payload: String,
    ) -> Result<Self, BybitPublicError> {
        if generation == 0 || received_at_ms == 0 || payload.is_empty() {
            return Err(BybitPublicError::Metadata);
        }
        let native_symbol = linear_native_symbol(&binding.gateway_binding().symbol)?;
        Ok(Self {
            parser_schema_version: BYBIT_PUBLIC_PARSER_SCHEMA_VERSION,
            source,
            binding: binding.gateway_binding().clone(),
            native_symbol,
            generation,
            received_at_ms,
            payload_sha256: payload_digest(&payload),
            payload,
        })
    }

    pub fn validate(
        &self,
        binding: &BybitGatewayBinding,
        source: BybitPublicSource,
    ) -> Result<(), BybitPublicError> {
        if self.parser_schema_version != BYBIT_PUBLIC_PARSER_SCHEMA_VERSION
            || self.source != source
            || &self.binding != binding.gateway_binding()
            || self.binding.validate().is_err()
            || self.generation == 0
            || self.received_at_ms == 0
            || self.payload.is_empty()
            || self.payload_sha256 != payload_digest(&self.payload)
            || self.native_symbol != linear_native_symbol(&self.binding.symbol)?
        {
            return Err(BybitPublicError::Metadata);
        }
        Ok(())
    }
}

#[must_use]
pub fn linear_native_symbol(symbol: &Symbol) -> Result<String, BybitPublicError> {
    if symbol.quote() != "USDT" {
        return Err(BybitPublicError::Product);
    }
    Ok(format!("{}{}", symbol.base(), symbol.quote()))
}

pub fn linear_instrument_path(binding: &BybitGatewayBinding) -> Result<String, BybitPublicError> {
    let native_symbol = linear_native_symbol(&binding.gateway_binding().symbol)?;
    Ok(format!(
        "{}?category={BYBIT_LINEAR_CATEGORY}&symbol={native_symbol}",
        endpoints::INSTRUMENTS
    ))
}

pub fn linear_bbo_path(binding: &BybitGatewayBinding) -> Result<String, BybitPublicError> {
    let native_symbol = linear_native_symbol(&binding.gateway_binding().symbol)?;
    Ok(format!(
        "{}?category={BYBIT_LINEAR_CATEGORY}&symbol={native_symbol}&limit=1",
        endpoints::ORDERBOOK
    ))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitLinearInstrumentRules {
    pub raw: BybitRawPublicPayload,
    pub response_time_ms: u64,
    pub native_symbol: String,
    pub instrument: Instrument,
    pub minimum_price: Price,
    pub maximum_price: Price,
    pub minimum_quantity: Decimal,
    pub maximum_limit_quantity: Decimal,
    pub maximum_market_quantity: Decimal,
    pub deprecated_post_only_maximum_quantity: FieldState<Decimal>,
}

pub fn parse_linear_instrument(
    binding: &BybitGatewayBinding,
    raw: BybitRawPublicPayload,
) -> Result<BybitLinearInstrumentRules, BybitPublicError> {
    raw.validate(binding, BybitPublicSource::LinearInstrument)?;
    let root_value = parse_json(&raw.payload)?;
    let root = object(&root_value)?;
    require_success(root)?;
    let response_time_ms = required_u64(root, "time")?;
    let result = required_object(root, "result")?;
    require_text(result, "category", BYBIT_LINEAR_CATEGORY)?;
    let cursor = required_string(result, "nextPageCursor")?;
    if !cursor.is_empty() {
        return Err(BybitPublicError::Pagination);
    }
    let rows = required_array(result, "list")?;
    let row = exact_one_object(rows)?;
    require_text(row, "symbol", &raw.native_symbol)?;
    require_text(row, "baseCoin", raw.binding.symbol.base())?;
    require_text(row, "quoteCoin", raw.binding.symbol.quote())?;
    require_text(row, "settleCoin", raw.binding.symbol.quote())?;
    require_text(row, "contractType", BYBIT_LINEAR_CONTRACT_TYPE)?;
    require_text(row, "status", "Trading")?;

    let price_filter = required_object(row, "priceFilter")?;
    let minimum_price = required_price(price_filter, "minPrice")?;
    let maximum_price = required_price(price_filter, "maxPrice")?;
    let price_tick = required_price(price_filter, "tickSize")?;
    if minimum_price > maximum_price || price_tick > maximum_price {
        return Err(BybitPublicError::Number);
    }

    let lot_filter = required_object(row, "lotSizeFilter")?;
    let quantity_step = required_positive_decimal(lot_filter, "qtyStep")?;
    let minimum_quantity = required_positive_decimal(lot_filter, "minOrderQty")?;
    let maximum_limit_quantity = required_positive_decimal(lot_filter, "maxOrderQty")?;
    let maximum_market_quantity = required_positive_decimal(lot_filter, "maxMktOrderQty")?;
    let minimum_notional = required_positive_decimal(lot_filter, "minNotionalValue")?;
    if minimum_quantity > maximum_limit_quantity || minimum_quantity > maximum_market_quantity {
        return Err(BybitPublicError::Number);
    }
    let deprecated_post_only_maximum_quantity =
        optional_positive_decimal(lot_filter.get("postOnlyMaxOrderQty"));

    let settlement =
        Asset::new(raw.binding.symbol.quote()).map_err(|_| BybitPublicError::Number)?;
    let instrument = Instrument {
        symbol: raw.binding.symbol.clone(),
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(settlement.clone()),
        generation: raw.generation,
        price_tick,
        quantity_step,
        minimum_notional: Amount::new(settlement, minimum_notional),
    };
    instrument
        .validate()
        .map_err(|_| BybitPublicError::Number)?;
    Ok(BybitLinearInstrumentRules {
        response_time_ms,
        native_symbol: raw.native_symbol.clone(),
        instrument,
        minimum_price,
        maximum_price,
        minimum_quantity,
        maximum_limit_quantity,
        maximum_market_quantity,
        deprecated_post_only_maximum_quantity,
        raw,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitRestBbo {
    pub raw: BybitRawPublicPayload,
    pub response_time_ms: u64,
    pub system_time_ms: u64,
    pub matching_engine_time_ms: u64,
    pub cross_sequence: u64,
    pub snapshot: MarketSnapshot,
}

pub fn parse_rest_bbo(
    binding: &BybitGatewayBinding,
    raw: BybitRawPublicPayload,
) -> Result<BybitRestBbo, BybitPublicError> {
    raw.validate(binding, BybitPublicSource::RestOrderBook)?;
    let root_value = parse_json(&raw.payload)?;
    let root = object(&root_value)?;
    require_success(root)?;
    let response_time_ms = required_u64(root, "time")?;
    let result = required_object(root, "result")?;
    require_text(result, "s", &raw.native_symbol)?;
    let system_time_ms = required_u64(result, "ts")?;
    let update_id = required_u64(result, "u")?;
    let cross_sequence = required_u64(result, "seq")?;
    let matching_engine_time_ms = required_u64(result, "cts")?;
    if matching_engine_time_ms > system_time_ms || system_time_ms > response_time_ms {
        return Err(BybitPublicError::Sequence);
    }
    let bids = parse_single_level(required_array(result, "b")?)?;
    let asks = parse_single_level(required_array(result, "a")?)?;
    let bid = bids.first().ok_or(BybitPublicError::Payload)?;
    let ask = asks.first().ok_or(BybitPublicError::Payload)?;
    if bid.price >= ask.price {
        return Err(BybitPublicError::Payload);
    }
    Ok(BybitRestBbo {
        response_time_ms,
        system_time_ms,
        matching_engine_time_ms,
        cross_sequence,
        snapshot: MarketSnapshot {
            symbol: raw.binding.symbol.clone(),
            generation: raw.generation,
            sequence: update_id,
            exchange_time_ms: Some(matching_engine_time_ms),
            bids,
            asks,
        },
        raw,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BybitBboSequenceStatus {
    Advanced,
    Duplicate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BybitBboSequenceGuard {
    generation: u64,
    last_update_id: Option<u64>,
    last_cross_sequence: Option<u64>,
}

impl BybitBboSequenceGuard {
    pub fn new(generation: u64) -> Result<Self, BybitPublicError> {
        if generation == 0 {
            return Err(BybitPublicError::Generation);
        }
        Ok(Self {
            generation,
            last_update_id: None,
            last_cross_sequence: None,
        })
    }

    pub fn accept(
        &mut self,
        bbo: &BybitRestBbo,
    ) -> Result<BybitBboSequenceStatus, BybitPublicError> {
        if bbo.snapshot.generation != self.generation {
            return Err(BybitPublicError::Generation);
        }
        match (self.last_update_id, self.last_cross_sequence) {
            (None, None) => {}
            (Some(update_id), Some(cross_sequence))
                if bbo.snapshot.sequence == update_id && bbo.cross_sequence == cross_sequence =>
            {
                return Ok(BybitBboSequenceStatus::Duplicate);
            }
            (Some(update_id), Some(cross_sequence))
                if bbo.snapshot.sequence <= update_id || bbo.cross_sequence <= cross_sequence =>
            {
                return Err(BybitPublicError::Sequence);
            }
            (Some(_), Some(_)) => {}
            _ => return Err(BybitPublicError::Sequence),
        }
        self.last_update_id = Some(bbo.snapshot.sequence);
        self.last_cross_sequence = Some(bbo.cross_sequence);
        Ok(BybitBboSequenceStatus::Advanced)
    }
}

fn parse_json(payload: &str) -> Result<Value, BybitPublicError> {
    serde_json::from_str(payload).map_err(|_| BybitPublicError::Payload)
}

fn object(value: &Value) -> Result<&Map<String, Value>, BybitPublicError> {
    value.as_object().ok_or(BybitPublicError::Payload)
}

fn require_success(root: &Map<String, Value>) -> Result<(), BybitPublicError> {
    match root.get("retCode").and_then(Value::as_i64) {
        Some(0) => Ok(()),
        Some(_) => Err(BybitPublicError::VenueRejected),
        None => Err(BybitPublicError::Payload),
    }
}

fn required_object<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a Map<String, Value>, BybitPublicError> {
    object
        .get(field)
        .and_then(Value::as_object)
        .ok_or(BybitPublicError::Payload)
}

fn required_array<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a Vec<Value>, BybitPublicError> {
    object
        .get(field)
        .and_then(Value::as_array)
        .ok_or(BybitPublicError::Payload)
}

fn required_string<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a str, BybitPublicError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or(BybitPublicError::Payload)
}

fn require_text(
    object: &Map<String, Value>,
    field: &str,
    expected: &str,
) -> Result<(), BybitPublicError> {
    if required_string(object, field)? == expected {
        Ok(())
    } else {
        Err(BybitPublicError::Binding)
    }
}

fn exact_one_object(values: &[Value]) -> Result<&Map<String, Value>, BybitPublicError> {
    if values.len() != 1 {
        return Err(BybitPublicError::Payload);
    }
    values
        .first()
        .and_then(Value::as_object)
        .ok_or(BybitPublicError::Payload)
}

fn required_u64(object: &Map<String, Value>, field: &str) -> Result<u64, BybitPublicError> {
    object
        .get(field)
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .ok_or(BybitPublicError::Payload)
}

fn required_decimal(object: &Map<String, Value>, field: &str) -> Result<Decimal, BybitPublicError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .and_then(|value| Decimal::from_str(value).ok())
        .ok_or(BybitPublicError::Number)
}

fn required_positive_decimal(
    object: &Map<String, Value>,
    field: &str,
) -> Result<Decimal, BybitPublicError> {
    let value = required_decimal(object, field)?;
    if value > Decimal::ZERO {
        Ok(value)
    } else {
        Err(BybitPublicError::Number)
    }
}

fn required_price(object: &Map<String, Value>, field: &str) -> Result<Price, BybitPublicError> {
    Price::new(required_decimal(object, field)?).map_err(|_| BybitPublicError::Number)
}

fn optional_positive_decimal(value: Option<&Value>) -> FieldState<Decimal> {
    match value {
        None => FieldState::Missing,
        Some(Value::Null) => FieldState::Null,
        Some(Value::String(value)) => match Decimal::from_str(value) {
            Ok(value) if value > Decimal::ZERO => FieldState::Known(value),
            _ => FieldState::Unavailable {
                reason: UnknownReason::ParseFailure,
            },
        },
        Some(_) => FieldState::Unavailable {
            reason: UnknownReason::ParseFailure,
        },
    }
}

fn parse_single_level(values: &[Value]) -> Result<Vec<MarketLevel>, BybitPublicError> {
    if values.len() != 1 {
        return Err(BybitPublicError::Payload);
    }
    let fields = values
        .first()
        .and_then(Value::as_array)
        .filter(|fields| fields.len() == 2)
        .ok_or(BybitPublicError::Payload)?;
    let price = fields
        .first()
        .and_then(Value::as_str)
        .and_then(|value| Decimal::from_str(value).ok())
        .and_then(|value| Price::new(value).ok())
        .ok_or(BybitPublicError::Number)?;
    let quantity = fields
        .get(1)
        .and_then(Value::as_str)
        .and_then(|value| Decimal::from_str(value).ok())
        .filter(|value| *value > Decimal::ZERO)
        .ok_or(BybitPublicError::Number)?;
    Ok(vec![MarketLevel { price, quantity }])
}

fn payload_digest(payload: &str) -> String {
    let digest = Sha256::digest(payload.as_bytes());
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BybitPublicError {
    #[error("Bybit public payload metadata is invalid or was relabelled")]
    Metadata,
    #[error("Bybit public payload has an invalid documented shape")]
    Payload,
    #[error("Bybit public payload does not match the exact gateway binding")]
    Binding,
    #[error("Bybit public request was rejected")]
    VenueRejected,
    #[error("Bybit public protocol batch is not closed")]
    Pagination,
    #[error("Bybit public sequence or event time is out of order")]
    Sequence,
    #[error("Bybit public numeric field is invalid")]
    Number,
    #[error("Bybit public gateway currently supports only USDT linear perpetuals")]
    Product,
    #[error("Bybit public generation is invalid")]
    Generation,
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_gateway_api::{GatewayMode, VenueId};

    const ACCOUNT: &str = "00000000-0000-4000-8000-000000000001";
    const INSTRUMENT_FIXTURE: &str = include_str!("../fixtures/instruments-linear.json");
    const BBO_FIXTURE: &str = include_str!("../fixtures/orderbook-linear-bbo.json");

    fn binding(symbol: &str) -> Result<BybitGatewayBinding, Box<dyn std::error::Error>> {
        Ok(BybitGatewayBinding::new(GatewayBinding::new(
            VenueId::Bybit,
            GatewayMode::Live,
            ACCOUNT,
            symbol.parse()?,
        )?)?)
    }

    fn raw(
        binding: &BybitGatewayBinding,
        source: BybitPublicSource,
        payload: &str,
    ) -> Result<BybitRawPublicPayload, BybitPublicError> {
        BybitRawPublicPayload::new(binding, source, 7, 1_716_863_719_400, payload.to_owned())
    }

    #[test]
    fn official_linear_instrument_normalizes_exact_rules() -> Result<(), Box<dyn std::error::Error>>
    {
        let binding = binding("BTC/USDT")?;
        assert_eq!(
            linear_instrument_path(&binding)?,
            "/v5/market/instruments-info?category=linear&symbol=BTCUSDT"
        );
        let rules = parse_linear_instrument(
            &binding,
            raw(
                &binding,
                BybitPublicSource::LinearInstrument,
                INSTRUMENT_FIXTURE,
            )?,
        )?;
        assert_eq!(rules.instrument.symbol.to_string(), "BTC/USDT");
        assert_eq!(rules.instrument.price_tick.value().to_string(), "0.10");
        assert_eq!(rules.instrument.quantity_step.to_string(), "0.001");
        assert_eq!(rules.minimum_quantity.to_string(), "0.001");
        assert_eq!(rules.instrument.minimum_notional.value.to_string(), "5");
        assert_eq!(
            rules.deprecated_post_only_maximum_quantity,
            FieldState::Known(Decimal::from(1190))
        );
        Ok(())
    }

    #[test]
    fn official_orderbook_normalizes_bbo_and_matching_engine_time()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding("BTC/USDT")?;
        assert_eq!(
            linear_bbo_path(&binding)?,
            "/v5/market/orderbook?category=linear&symbol=BTCUSDT&limit=1"
        );
        let bbo = parse_rest_bbo(
            &binding,
            raw(&binding, BybitPublicSource::RestOrderBook, BBO_FIXTURE)?,
        )?;
        assert_eq!(bbo.snapshot.bids[0].price.value().to_string(), "65485.47");
        assert_eq!(bbo.snapshot.asks[0].price.value().to_string(), "65557.7");
        assert_eq!(bbo.snapshot.exchange_time_ms, Some(1_716_863_718_905));
        assert_eq!(bbo.snapshot.sequence, 230_704);
        assert_eq!(bbo.cross_sequence, 1_432_604_333);
        Ok(())
    }

    #[test]
    fn wrong_binding_missing_field_and_unclosed_page_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let btc = binding("BTC/USDT")?;
        let eth = binding("ETH/USDT")?;
        assert_eq!(
            parse_linear_instrument(
                &eth,
                raw(
                    &eth,
                    BybitPublicSource::LinearInstrument,
                    INSTRUMENT_FIXTURE,
                )?,
            ),
            Err(BybitPublicError::Binding)
        );
        let unclosed = INSTRUMENT_FIXTURE.replace(
            "\"nextPageCursor\": \"\"",
            "\"nextPageCursor\": \"cursor-2\"",
        );
        assert_eq!(
            parse_linear_instrument(
                &btc,
                raw(&btc, BybitPublicSource::LinearInstrument, &unclosed)?,
            ),
            Err(BybitPublicError::Pagination)
        );
        let missing = BBO_FIXTURE.replace("\"a\": [", "\"missingA\": [");
        assert_eq!(
            parse_rest_bbo(&btc, raw(&btc, BybitPublicSource::RestOrderBook, &missing)?,),
            Err(BybitPublicError::Payload)
        );
        Ok(())
    }

    #[test]
    fn sequence_guard_rejects_regression_and_preserves_duplicates()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding("BTC/USDT")?;
        let first = parse_rest_bbo(
            &binding,
            raw(&binding, BybitPublicSource::RestOrderBook, BBO_FIXTURE)?,
        )?;
        let mut guard = BybitBboSequenceGuard::new(7)?;
        assert_eq!(guard.accept(&first)?, BybitBboSequenceStatus::Advanced);
        assert_eq!(guard.accept(&first)?, BybitBboSequenceStatus::Duplicate);
        let regressed = BBO_FIXTURE
            .replace("\"u\": 230704", "\"u\": 230703")
            .replace("\"seq\": 1432604333", "\"seq\": 1432604332");
        let regressed = parse_rest_bbo(
            &binding,
            raw(&binding, BybitPublicSource::RestOrderBook, &regressed)?,
        )?;
        assert_eq!(guard.accept(&regressed), Err(BybitPublicError::Sequence));
        Ok(())
    }

    #[test]
    fn tampering_crossed_bbo_and_non_usdt_product_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let btc = binding("BTC/USDT")?;
        let mut tampered = raw(&btc, BybitPublicSource::RestOrderBook, BBO_FIXTURE)?;
        tampered.payload.push(' ');
        assert_eq!(
            parse_rest_bbo(&btc, tampered),
            Err(BybitPublicError::Metadata)
        );
        let crossed = BBO_FIXTURE.replace("65485.47", "65558.00");
        assert_eq!(
            parse_rest_bbo(&btc, raw(&btc, BybitPublicSource::RestOrderBook, &crossed)?,),
            Err(BybitPublicError::Payload)
        );
        let usdc = binding("BTC/USDC")?;
        assert_eq!(linear_bbo_path(&usdc), Err(BybitPublicError::Product));
        Ok(())
    }
}
