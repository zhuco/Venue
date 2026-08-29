use std::{collections::BTreeSet, str::FromStr};

use rust_decimal::Decimal;
use serde_json::{Map, Value};
use venue_domain::domain::{
    AggressorSide, FieldState, MarketDelta, MarketLevel, Price, PublicBar, PublicTicker,
    PublicTrade,
};
use venue_gateway_api::GatewayBinding;

use crate::{BinanceAccountBinding, native_symbol};

const ONE_MINUTE_MS: u64 = 60_000;

/// Immutable adapter evidence binding one normalized fact to the exact native payload and times.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinancePublicEnvelope<T> {
    native_symbol: String,
    generation: u64,
    exchange_event_time_ms: u64,
    transaction_time_ms: Option<u64>,
    raw_payload: String,
    fact: T,
}

impl<T> BinancePublicEnvelope<T> {
    #[must_use]
    pub fn native_symbol(&self) -> &str {
        &self.native_symbol
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn exchange_event_time_ms(&self) -> u64 {
        self.exchange_event_time_ms
    }

    #[must_use]
    pub const fn transaction_time_ms(&self) -> Option<u64> {
        self.transaction_time_ms
    }

    #[must_use]
    pub fn raw_payload(&self) -> &str {
        &self.raw_payload
    }

    #[must_use]
    pub const fn fact(&self) -> &T {
        &self.fact
    }

    #[must_use]
    pub fn into_fact(self) -> T {
        self.fact
    }
}

pub fn parse_bbo(
    payload: &str,
    binding: &GatewayBinding,
    generation: u64,
    received_at_ms: u64,
) -> Result<BinancePublicEnvelope<PublicTicker>, BinancePublicError> {
    let (object, expected_native) = stream_object(payload, binding, generation, "bookTicker")?;
    let exchange_event_time_ms = positive_u64(object.get("E"))?;
    let transaction_time_ms = positive_u64(object.get("T"))?;
    let bid_price = positive_price(object.get("b"))?;
    let ask_price = positive_price(object.get("a"))?;
    let bid_quantity = positive_decimal(object.get("B"))?;
    let ask_quantity = positive_decimal(object.get("A"))?;
    if received_at_ms == 0 || bid_price >= ask_price {
        return Err(BinancePublicError::Value);
    }
    let fact = PublicTicker {
        symbol: binding.symbol.clone(),
        generation,
        received_at_ms,
        exchange_time_ms: exchange_event_time_ms,
        transaction_time_ms,
        update_id: positive_u64(object.get("u"))?,
        bid_price,
        bid_quantity,
        ask_price,
        ask_quantity,
    };
    Ok(envelope(
        payload,
        expected_native,
        generation,
        exchange_event_time_ms,
        Some(transaction_time_ms),
        fact,
    ))
}

pub fn parse_depth_delta(
    payload: &str,
    binding: &GatewayBinding,
    generation: u64,
) -> Result<BinancePublicEnvelope<MarketDelta>, BinancePublicError> {
    let (object, expected_native) = stream_object(payload, binding, generation, "depthUpdate")?;
    let exchange_event_time_ms = positive_u64(object.get("E"))?;
    let transaction_time_ms = positive_u64(object.get("T"))?;
    let first_sequence = positive_u64(object.get("U"))?;
    let sequence = positive_u64(object.get("u"))?;
    let previous_sequence = positive_u64(object.get("pu"))?;
    if sequence < first_sequence || previous_sequence >= first_sequence {
        return Err(BinancePublicError::Sequence);
    }
    let bids = levels(object.get("b"), true)?;
    let asks = levels(object.get("a"), true)?;
    if bids.is_empty() && asks.is_empty() {
        return Err(BinancePublicError::Value);
    }
    let fact = MarketDelta {
        symbol: binding.symbol.clone(),
        generation,
        first_sequence,
        previous_sequence: Some(previous_sequence),
        sequence,
        exchange_time_ms: Some(exchange_event_time_ms),
        bids,
        asks,
    };
    Ok(envelope(
        payload,
        expected_native,
        generation,
        exchange_event_time_ms,
        Some(transaction_time_ms),
        fact,
    ))
}

pub fn parse_public_trade(
    payload: &str,
    binding: &GatewayBinding,
    generation: u64,
    received_at_ms: u64,
) -> Result<BinancePublicEnvelope<PublicTrade>, BinancePublicError> {
    let (object, expected_native) = stream_object(payload, binding, generation, "aggTrade")?;
    let exchange_event_time_ms = positive_u64(object.get("E"))?;
    let transaction_time_ms = positive_u64(object.get("T"))?;
    let first_trade_id = positive_u64(object.get("f"))?;
    let last_trade_id = positive_u64(object.get("l"))?;
    if received_at_ms == 0 || last_trade_id < first_trade_id {
        return Err(BinancePublicError::Sequence);
    }
    let aggressor = match object.get("m") {
        Some(Value::Bool(true)) => AggressorSide::Sell,
        Some(Value::Bool(false)) => AggressorSide::Buy,
        _ => return Err(BinancePublicError::Payload),
    };
    let fact = PublicTrade {
        symbol: binding.symbol.clone(),
        generation,
        received_at_ms,
        exchange_time_ms: exchange_event_time_ms,
        transaction_time_ms,
        aggregate_trade_id: positive_u64(object.get("a"))?,
        first_trade_id,
        last_trade_id,
        price: positive_price(object.get("p"))?,
        quantity: positive_decimal(object.get("q"))?,
        quote_quantity: positive_decimal(object.get("nq"))?,
        aggressor: FieldState::Known(aggressor),
    };
    Ok(envelope(
        payload,
        expected_native,
        generation,
        exchange_event_time_ms,
        Some(transaction_time_ms),
        fact,
    ))
}

pub fn parse_closed_bar(
    payload: &str,
    binding: &GatewayBinding,
    generation: u64,
    received_at_ms: u64,
) -> Result<BinancePublicEnvelope<PublicBar>, BinancePublicError> {
    let (object, expected_native) = stream_object(payload, binding, generation, "kline")?;
    let exchange_event_time_ms = positive_u64(object.get("E"))?;
    let kline = object
        .get("k")
        .and_then(Value::as_object)
        .ok_or(BinancePublicError::Payload)?;
    check_symbol(kline.get("s"), &expected_native)?;
    if kline.get("i").and_then(Value::as_str) != Some("1m") {
        return Err(BinancePublicError::Payload);
    }
    if kline.get("x").and_then(Value::as_bool) != Some(true) {
        return Err(BinancePublicError::BarNotClosed);
    }
    let open_time_ms = positive_u64(kline.get("t"))?;
    let close_time_ms = positive_u64(kline.get("T"))?;
    let expected_close = open_time_ms
        .checked_add(ONE_MINUTE_MS - 1)
        .ok_or(BinancePublicError::Sequence)?;
    if open_time_ms % ONE_MINUTE_MS != 0
        || close_time_ms != expected_close
        || exchange_event_time_ms < close_time_ms
        || received_at_ms == 0
    {
        return Err(BinancePublicError::Sequence);
    }
    let evidence = complete_bar_evidence(kline)?;
    let open = positive_price(kline.get("o"))?;
    let high = positive_price(kline.get("h"))?;
    let low = positive_price(kline.get("l"))?;
    let close = positive_price(kline.get("c"))?;
    if high < open.max(close) || low > open.min(close) || high < low {
        return Err(BinancePublicError::Value);
    }
    let sequence = open_time_ms
        .checked_div(ONE_MINUTE_MS)
        .and_then(|value| value.checked_add(1))
        .ok_or(BinancePublicError::Sequence)?;
    let fact = PublicBar {
        symbol: binding.symbol.clone(),
        generation,
        received_at_ms,
        sequence,
        open_time_ms,
        close_time_ms,
        interval_ms: ONE_MINUTE_MS,
        open,
        high,
        low,
        close,
        base_volume: FieldState::Known(evidence.base_volume),
        quote_volume: FieldState::Known(evidence.quote_volume),
        trade_count: FieldState::Known(evidence.trade_count),
        taker_buy_base_volume: FieldState::Known(evidence.taker_buy_base_volume),
        taker_buy_quote_volume: FieldState::Known(evidence.taker_buy_quote_volume),
    };
    if !fact.is_valid() {
        return Err(BinancePublicError::Value);
    }
    Ok(envelope(
        payload,
        expected_native,
        generation,
        exchange_event_time_ms,
        None,
        fact,
    ))
}

fn stream_object(
    payload: &str,
    binding: &GatewayBinding,
    generation: u64,
    event: &str,
) -> Result<(Map<String, Value>, String), BinancePublicError> {
    BinanceAccountBinding::PortfolioMarginUm
        .validate_gateway_binding(binding)
        .map_err(|_| BinancePublicError::Binding)?;
    if generation == 0 {
        return Err(BinancePublicError::Generation);
    }
    let value: Value = serde_json::from_str(payload).map_err(|_| BinancePublicError::Payload)?;
    let object = value
        .as_object()
        .cloned()
        .ok_or(BinancePublicError::Payload)?;
    let expected_native = native_symbol(&binding.symbol);
    check_symbol(object.get("s"), &expected_native)?;
    if object.get("e").and_then(Value::as_str) != Some(event)
        || object.get("st").and_then(Value::as_u64) != Some(1)
    {
        return Err(BinancePublicError::Payload);
    }
    Ok((object, expected_native))
}

fn envelope<T>(
    payload: &str,
    native_symbol: String,
    generation: u64,
    exchange_event_time_ms: u64,
    transaction_time_ms: Option<u64>,
    fact: T,
) -> BinancePublicEnvelope<T> {
    BinancePublicEnvelope {
        native_symbol,
        generation,
        exchange_event_time_ms,
        transaction_time_ms,
        raw_payload: payload.to_owned(),
        fact,
    }
}

struct CompleteBarEvidence {
    trade_count: u64,
    base_volume: Decimal,
    quote_volume: Decimal,
    taker_buy_base_volume: Decimal,
    taker_buy_quote_volume: Decimal,
}

fn complete_bar_evidence(
    kline: &Map<String, Value>,
) -> Result<CompleteBarEvidence, BinancePublicError> {
    let first_trade_id = u64_value(kline.get("f"))?;
    let last_trade_id = u64_value(kline.get("L"))?;
    let trade_count = u64_value(kline.get("n"))?;
    let base_volume = non_negative_decimal(kline.get("v"))?;
    let quote_volume = non_negative_decimal(kline.get("q"))?;
    let taker_buy_base_volume = non_negative_decimal(kline.get("V"))?;
    let taker_buy_quote_volume = non_negative_decimal(kline.get("Q"))?;
    if taker_buy_base_volume > base_volume
        || taker_buy_quote_volume > quote_volume
        || (trade_count == 0
            && (first_trade_id != 0
                || last_trade_id != 0
                || !base_volume.is_zero()
                || !quote_volume.is_zero()
                || !taker_buy_base_volume.is_zero()
                || !taker_buy_quote_volume.is_zero()))
        || (trade_count > 0
            && (first_trade_id == 0
                || last_trade_id < first_trade_id
                || base_volume <= Decimal::ZERO
                || quote_volume <= Decimal::ZERO))
    {
        return Err(BinancePublicError::Value);
    }
    Ok(CompleteBarEvidence {
        trade_count,
        base_volume,
        quote_volume,
        taker_buy_base_volume,
        taker_buy_quote_volume,
    })
}

fn levels(
    value: Option<&Value>,
    allow_delete: bool,
) -> Result<Vec<MarketLevel>, BinancePublicError> {
    let rows = value
        .and_then(Value::as_array)
        .ok_or(BinancePublicError::Payload)?;
    let mut prices = BTreeSet::new();
    rows.iter()
        .map(|row| {
            let fields = row.as_array().ok_or(BinancePublicError::Payload)?;
            if fields.len() != 2 {
                return Err(BinancePublicError::Payload);
            }
            let price = positive_price(fields.first())?;
            let quantity = non_negative_decimal(fields.get(1))?;
            if (!allow_delete && quantity.is_zero()) || !prices.insert(price) {
                return Err(BinancePublicError::Value);
            }
            Ok(MarketLevel { price, quantity })
        })
        .collect()
}

fn check_symbol(value: Option<&Value>, expected: &str) -> Result<(), BinancePublicError> {
    if value.and_then(Value::as_str) == Some(expected) {
        Ok(())
    } else {
        Err(BinancePublicError::Symbol)
    }
}

fn positive_price(value: Option<&Value>) -> Result<Price, BinancePublicError> {
    Price::new(positive_decimal(value)?).map_err(|_| BinancePublicError::Value)
}

fn positive_decimal(value: Option<&Value>) -> Result<Decimal, BinancePublicError> {
    let value = decimal(value)?;
    if value > Decimal::ZERO {
        Ok(value)
    } else {
        Err(BinancePublicError::Value)
    }
}

fn non_negative_decimal(value: Option<&Value>) -> Result<Decimal, BinancePublicError> {
    let value = decimal(value)?;
    if value >= Decimal::ZERO {
        Ok(value)
    } else {
        Err(BinancePublicError::Value)
    }
}

fn decimal(value: Option<&Value>) -> Result<Decimal, BinancePublicError> {
    Decimal::from_str(
        value
            .and_then(Value::as_str)
            .ok_or(BinancePublicError::Payload)?,
    )
    .map_err(|_| BinancePublicError::Payload)
}

fn positive_u64(value: Option<&Value>) -> Result<u64, BinancePublicError> {
    u64_value(value).and_then(|value| {
        if value > 0 {
            Ok(value)
        } else {
            Err(BinancePublicError::Value)
        }
    })
}

fn u64_value(value: Option<&Value>) -> Result<u64, BinancePublicError> {
    value
        .and_then(Value::as_u64)
        .ok_or(BinancePublicError::Payload)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BinancePublicError {
    #[error("Binance public payload binding is invalid")]
    Binding,
    #[error("Binance public payload generation must be positive")]
    Generation,
    #[error("Binance public payload has an invalid or incomplete shape")]
    Payload,
    #[error("Binance public payload symbol does not match the canonical binding")]
    Symbol,
    #[error("Binance public payload sequence or event-time range is invalid")]
    Sequence,
    #[error("Binance public payload contains an invalid price or quantity")]
    Value,
    #[error("Binance kline is not closed")]
    BarNotClosed,
}
