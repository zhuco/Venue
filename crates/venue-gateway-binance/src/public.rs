use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use rust_decimal::Decimal;
use serde_json::{Map, Value};
use venue_domain::domain::{
    AggressorSide, FieldState, MarketDelta, MarketLevel, MarketSnapshot, Price, PublicBar,
    PublicTicker, PublicTrade, Symbol,
};
use venue_gateway_api::{GatewayBinding, PublicMarketBinding};

use crate::{BinanceAccountBinding, native_symbol};

const ONE_MINUTE_MS: u64 = 60_000;

/// Kline intervals intentionally exposed by the first local Binance public-market feed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinanceKlineInterval {
    OneMinute,
    FiveMinutes,
    FifteenMinutes,
    OneHour,
    FourHours,
    OneDay,
}

impl BinanceKlineInterval {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OneMinute => "1m",
            Self::FiveMinutes => "5m",
            Self::FifteenMinutes => "15m",
            Self::OneHour => "1h",
            Self::FourHours => "4h",
            Self::OneDay => "1d",
        }
    }

    #[must_use]
    pub const fn milliseconds(self) -> u64 {
        match self {
            Self::OneMinute => 60_000,
            Self::FiveMinutes => 300_000,
            Self::FifteenMinutes => 900_000,
            Self::OneHour => 3_600_000,
            Self::FourHours => 14_400_000,
            Self::OneDay => 86_400_000,
        }
    }

    fn parse(raw: &str) -> Result<Self, BinancePublicError> {
        match raw {
            "1m" => Ok(Self::OneMinute),
            "5m" => Ok(Self::FiveMinutes),
            "15m" => Ok(Self::FifteenMinutes),
            "1h" => Ok(Self::OneHour),
            "4h" => Ok(Self::FourHours),
            "1d" => Ok(Self::OneDay),
            _ => Err(BinancePublicError::Interval),
        }
    }
}

/// Adapter-only in-progress kline. It is intentionally not a domain [`PublicBar`], because
/// strategy facts may only contain an exchange-confirmed closed bar.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceFormingBar {
    pub symbol: Symbol,
    pub generation: u64,
    pub received_at_ms: u64,
    pub exchange_time_ms: u64,
    pub sequence: u64,
    pub open_time_ms: u64,
    pub close_time_ms: u64,
    pub interval: BinanceKlineInterval,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub base_volume: Decimal,
    pub quote_volume: Decimal,
    pub trade_count: u64,
    pub taker_buy_base_volume: Decimal,
    pub taker_buy_quote_volume: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BinancePublicKline {
    Forming(BinancePublicEnvelope<BinanceFormingBar>),
    Closed(BinancePublicEnvelope<PublicBar>),
}

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
        aggregate_trade_id: positive_u64(object.get("a"))?.into(),
        first_trade_id: Some(first_trade_id),
        last_trade_id: Some(last_trade_id),
        ordering: venue_domain::PublicTradeOrdering::NativeAggregateId,
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

/// Display-relevant public instrument rules from Binance USD-M `exchangeInfo`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinancePublicInstrument {
    pub symbol: Symbol,
    pub price_tick: Decimal,
    pub quantity_step: Decimal,
}

/// Parses the complete public catalog with exchange-authoritative price and quantity increments.
pub fn parse_public_exchange_catalog(
    payload: &str,
) -> Result<Vec<BinancePublicInstrument>, BinancePublicError> {
    let value: Value = serde_json::from_str(payload).map_err(|_| BinancePublicError::Payload)?;
    let rows = value
        .as_object()
        .and_then(|object| object.get("symbols"))
        .and_then(Value::as_array)
        .ok_or(BinancePublicError::Payload)?;
    let mut canonical = BTreeMap::new();
    let mut native = BTreeSet::new();
    for row in rows {
        let row = row.as_object().ok_or(BinancePublicError::Payload)?;
        let native_symbol_value = required_string(row.get("symbol"))?;
        let base = required_string(row.get("baseAsset"))?;
        let quote = required_string(row.get("quoteAsset"))?;
        let status = required_string(row.get("status"))?;
        let contract_type = required_string(row.get("contractType"))?;
        if status != "TRADING" || contract_type != "PERPETUAL" || !matches!(quote, "USDT" | "USDC")
        {
            continue;
        }
        let Ok(symbol) = Symbol::new(base, quote) else {
            continue;
        };
        let filters = row
            .get("filters")
            .and_then(Value::as_array)
            .ok_or(BinancePublicError::Payload)?;
        let filter_value = |kind: &str, field: &str| {
            filters
                .iter()
                .filter_map(Value::as_object)
                .find(|filter| filter.get("filterType").and_then(Value::as_str) == Some(kind))
                .and_then(|filter| filter.get(field))
        };
        let price_tick = positive_decimal(filter_value("PRICE_FILTER", "tickSize"))?.normalize();
        let quantity_step = positive_decimal(filter_value("LOT_SIZE", "stepSize"))?.normalize();
        if native_symbol(&symbol) != native_symbol_value
            || !native.insert(native_symbol_value.to_owned())
            || canonical
                .insert(
                    symbol.clone(),
                    BinancePublicInstrument {
                        symbol,
                        price_tick,
                        quantity_step,
                    },
                )
                .is_some()
        {
            return Err(BinancePublicError::Symbol);
        }
    }
    if canonical.is_empty() {
        return Err(BinancePublicError::Value);
    }
    Ok(canonical.into_values().collect())
}

/// Returns only canonical symbols for consumers that do not render exchange precision.
pub fn parse_public_exchange_info(payload: &str) -> Result<Vec<Symbol>, BinancePublicError> {
    parse_public_exchange_catalog(payload).map(|catalog| {
        catalog
            .into_iter()
            .map(|instrument| instrument.symbol)
            .collect()
    })
}

/// One credential-free all-market 24-hour ticker used by desktop discovery surfaces.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinancePublic24hTicker {
    pub symbol: Symbol,
    pub exchange_time_ms: u64,
    pub received_at_ms: u64,
    pub last_price: Price,
    pub price_change_percent: Decimal,
    pub quote_volume: Decimal,
}

/// Parses Binance's `!ticker@arr` combined stream against an exchangeInfo-derived catalog.
/// Native symbols absent from the current catalog are ignored, while duplicate catalog or frame
/// identities fail closed.
pub fn parse_public_market_ticker_array(
    payload: &str,
    catalog: &[Symbol],
    received_at_ms: u64,
) -> Result<Vec<BinancePublic24hTicker>, BinancePublicError> {
    if catalog.is_empty() || received_at_ms == 0 {
        return Err(BinancePublicError::Value);
    }
    let mut by_native = BTreeMap::new();
    for symbol in catalog {
        let native = native_symbol(symbol);
        if by_native.insert(native, symbol.clone()).is_some() {
            return Err(BinancePublicError::Symbol);
        }
    }
    let value: Value = serde_json::from_str(payload).map_err(|_| BinancePublicError::Payload)?;
    let mut wrapper = value
        .as_object()
        .cloned()
        .ok_or(BinancePublicError::Payload)?;
    if wrapper.get("stream").and_then(Value::as_str) != Some("!ticker@arr") {
        return Err(BinancePublicError::Payload);
    }
    let rows = wrapper
        .remove("data")
        .and_then(|value| value.as_array().cloned())
        .ok_or(BinancePublicError::Payload)?;
    let mut seen = BTreeSet::new();
    let mut tickers = Vec::with_capacity(rows.len().min(catalog.len()));
    for row in rows {
        let row = row.as_object().ok_or(BinancePublicError::Payload)?;
        if row.get("e").and_then(Value::as_str) != Some("24hrTicker") {
            return Err(BinancePublicError::Payload);
        }
        let native = required_string(row.get("s"))?;
        let Some(symbol) = by_native.get(native) else {
            continue;
        };
        if !seen.insert(native.to_owned()) {
            return Err(BinancePublicError::Symbol);
        }
        let exchange_time_ms = positive_u64(row.get("E"))?;
        if exchange_time_ms > received_at_ms {
            return Err(BinancePublicError::Value);
        }
        tickers.push(BinancePublic24hTicker {
            symbol: symbol.clone(),
            exchange_time_ms,
            received_at_ms,
            last_price: positive_price(row.get("c"))?,
            price_change_percent: decimal(row.get("P"))?,
            quote_volume: non_negative_decimal(row.get("q"))?,
        });
    }
    if tickers.is_empty() {
        return Err(BinancePublicError::Value);
    }
    Ok(tickers)
}

/// Parses the credential-free `GET /fapi/v1/ticker/24hr` all-market startup snapshot.
pub fn parse_public_market_ticker_snapshot(
    payload: &str,
    catalog: &[Symbol],
    received_at_ms: u64,
) -> Result<Vec<BinancePublic24hTicker>, BinancePublicError> {
    if catalog.is_empty() || received_at_ms == 0 {
        return Err(BinancePublicError::Value);
    }
    let by_native = catalog
        .iter()
        .map(|symbol| (native_symbol(symbol), symbol.clone()))
        .collect::<BTreeMap<_, _>>();
    if by_native.len() != catalog.len() {
        return Err(BinancePublicError::Symbol);
    }
    let rows = serde_json::from_str::<Value>(payload)
        .map_err(|_| BinancePublicError::Payload)?
        .as_array()
        .cloned()
        .ok_or(BinancePublicError::Payload)?;
    let mut seen = BTreeSet::new();
    let mut tickers = Vec::with_capacity(rows.len().min(catalog.len()));
    for row in rows {
        let row = row.as_object().ok_or(BinancePublicError::Payload)?;
        let native = required_string(row.get("symbol"))?;
        let Some(symbol) = by_native.get(native) else {
            continue;
        };
        if !seen.insert(native.to_owned()) {
            return Err(BinancePublicError::Symbol);
        }
        let exchange_time_ms = positive_u64(row.get("closeTime"))?;
        if exchange_time_ms > received_at_ms {
            return Err(BinancePublicError::Value);
        }
        tickers.push(BinancePublic24hTicker {
            symbol: symbol.clone(),
            exchange_time_ms,
            received_at_ms,
            last_price: positive_price(row.get("lastPrice"))?,
            price_change_percent: decimal(row.get("priceChangePercent"))?,
            quote_volume: non_negative_decimal(row.get("quoteVolume"))?,
        });
    }
    if tickers.is_empty() {
        return Err(BinancePublicError::Value);
    }
    Ok(tickers)
}

/// Parses a direct Binance `bookTicker` frame or a combined-stream wrapper for the local,
/// credential-free public market binding.
pub fn parse_public_market_bbo(
    payload: &str,
    binding: &PublicMarketBinding,
    generation: u64,
    received_at_ms: u64,
) -> Result<BinancePublicEnvelope<PublicTicker>, BinancePublicError> {
    let (object, expected_native) =
        public_market_stream_object(payload, binding, generation, "bookTicker")?;
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

/// Parses a direct Binance `aggTrade` frame or a combined-stream wrapper. Binance's public
/// aggregate trade frame has no quote quantity, so it is derived with checked decimal arithmetic.
pub fn parse_public_market_agg_trade(
    payload: &str,
    binding: &PublicMarketBinding,
    generation: u64,
    received_at_ms: u64,
) -> Result<BinancePublicEnvelope<PublicTrade>, BinancePublicError> {
    let (object, expected_native) =
        public_market_stream_object(payload, binding, generation, "aggTrade")?;
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
    let price = positive_price(object.get("p"))?;
    let quantity = positive_decimal(object.get("q"))?;
    let quote_quantity = price
        .value()
        .checked_mul(quantity)
        .ok_or(BinancePublicError::Value)?;
    let fact = PublicTrade {
        symbol: binding.symbol.clone(),
        generation,
        received_at_ms,
        exchange_time_ms: exchange_event_time_ms,
        transaction_time_ms,
        aggregate_trade_id: positive_u64(object.get("a"))?.into(),
        first_trade_id: Some(first_trade_id),
        last_trade_id: Some(last_trade_id),
        ordering: venue_domain::PublicTradeOrdering::NativeAggregateId,
        price,
        quantity,
        quote_quantity,
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

/// Parses one bounded `depth20` partial-book snapshot. It accepts the Binance websocket `b`/`a`
/// shape and the documented `bids`/`asks` spelling, but never treats a snapshot as a delta.
pub fn parse_public_market_depth20_snapshot(
    payload: &str,
    binding: &PublicMarketBinding,
    generation: u64,
) -> Result<BinancePublicEnvelope<MarketSnapshot>, BinancePublicError> {
    let (object, expected_native) =
        public_market_stream_object(payload, binding, generation, "depthUpdate")?;
    let exchange_event_time_ms = positive_u64(object.get("E"))?;
    let transaction_time_ms = positive_u64(object.get("T"))?;
    let sequence = positive_u64(object.get("lastUpdateId").or_else(|| object.get("u")))?;
    let bids = levels(object.get("b").or_else(|| object.get("bids")), false)?;
    let asks = levels(object.get("a").or_else(|| object.get("asks")), false)?;
    validate_depth20_snapshot(&bids, &asks)?;
    let fact = MarketSnapshot {
        symbol: binding.symbol.clone(),
        generation,
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

/// Parses one Binance USD-M diff-depth frame from the credential-free combined stream. It stays
/// distinct from the depth20 partial snapshot: a caller must still bridge this delta to a REST
/// snapshot before treating its book as synchronized.
pub fn parse_public_market_depth_delta(
    payload: &str,
    binding: &PublicMarketBinding,
    generation: u64,
) -> Result<BinancePublicEnvelope<MarketDelta>, BinancePublicError> {
    let (object, expected_native) =
        public_market_stream_object(payload, binding, generation, "depthUpdate")?;
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

/// Parses Binance USD-M `GET /fapi/v1/depth` into the initial book snapshot for a live
/// diff-depth bridge. REST has no exchange timestamp here, so the caller must require a queued
/// websocket delta to bridge this snapshot before treating the book as synchronized.
pub fn parse_public_market_rest_depth_snapshot(
    payload: &str,
    binding: &PublicMarketBinding,
    generation: u64,
) -> Result<MarketSnapshot, BinancePublicError> {
    binding
        .validate()
        .map_err(|_| BinancePublicError::Binding)?;
    if generation == 0 {
        return Err(BinancePublicError::Generation);
    }
    let object = serde_json::from_str::<Value>(payload)
        .map_err(|_| BinancePublicError::Payload)?
        .as_object()
        .cloned()
        .ok_or(BinancePublicError::Payload)?;
    let sequence = positive_u64(object.get("lastUpdateId"))?;
    let bids = levels(object.get("bids"), false)?;
    let asks = levels(object.get("asks"), false)?;
    if bids.is_empty() || asks.is_empty() || bids.len() > 1_000 || asks.len() > 1_000 {
        return Err(BinancePublicError::Value);
    }
    if bids.windows(2).any(|pair| pair[0].price <= pair[1].price)
        || asks.windows(2).any(|pair| pair[0].price >= pair[1].price)
        || bids[0].price >= asks[0].price
    {
        return Err(BinancePublicError::Value);
    }
    Ok(MarketSnapshot {
        symbol: binding.symbol.clone(),
        generation,
        sequence,
        exchange_time_ms: None,
        bids,
        asks,
    })
}

/// Parses a direct Binance `kline` frame or a combined-stream wrapper. A forming kline remains
/// adapter-local; only a frame carrying `x=true` becomes a normalized [`PublicBar`].
pub fn parse_public_market_kline(
    payload: &str,
    binding: &PublicMarketBinding,
    generation: u64,
    received_at_ms: u64,
) -> Result<BinancePublicKline, BinancePublicError> {
    let (object, expected_native) =
        public_market_stream_object(payload, binding, generation, "kline")?;
    let exchange_event_time_ms = positive_u64(object.get("E"))?;
    let kline = object
        .get("k")
        .and_then(Value::as_object)
        .ok_or(BinancePublicError::Payload)?;
    check_symbol(kline.get("s"), &expected_native)?;
    if received_at_ms == 0 {
        return Err(BinancePublicError::Value);
    }
    let values = parse_kline_values(kline)?;
    match kline.get("x").and_then(Value::as_bool) {
        Some(true) => {
            if exchange_event_time_ms < values.close_time_ms {
                return Err(BinancePublicError::Sequence);
            }
            Ok(BinancePublicKline::Closed(envelope(
                payload,
                expected_native,
                generation,
                exchange_event_time_ms,
                None,
                values.into_public_bar(binding.symbol.clone(), generation, received_at_ms),
            )))
        }
        Some(false) => Ok(BinancePublicKline::Forming(envelope(
            payload,
            expected_native,
            generation,
            exchange_event_time_ms,
            None,
            values.into_forming_bar(
                binding.symbol.clone(),
                generation,
                received_at_ms,
                exchange_event_time_ms,
            ),
        ))),
        None => Err(BinancePublicError::Payload),
    }
}

/// Parses the array returned by Binance USDⓈ-M `GET /fapi/v1/klines`. This endpoint does not
/// include a symbol in each row, so the caller must bind the HTTP request to `binding`; rows whose
/// close is not yet before the local receive time are intentionally filtered out as forming bars.
pub fn parse_public_market_rest_klines(
    payload: &str,
    binding: &PublicMarketBinding,
    generation: u64,
    received_at_ms: u64,
    interval: BinanceKlineInterval,
) -> Result<Vec<PublicBar>, BinancePublicError> {
    binding
        .validate()
        .map_err(|_| BinancePublicError::Binding)?;
    if generation == 0 {
        return Err(BinancePublicError::Generation);
    }
    if received_at_ms == 0 {
        return Err(BinancePublicError::Value);
    }
    let rows = serde_json::from_str::<Value>(payload)
        .map_err(|_| BinancePublicError::Payload)?
        .as_array()
        .cloned()
        .ok_or(BinancePublicError::Payload)?;
    let mut bars = Vec::with_capacity(rows.len());
    let mut previous_sequence = None;
    for row in rows {
        let fields = row.as_array().ok_or(BinancePublicError::Payload)?;
        if fields.len() < 11 {
            return Err(BinancePublicError::Payload);
        }
        let values = parse_rest_kline_values(fields, interval)?;
        if let Some(previous) = previous_sequence
            && values.sequence <= previous
        {
            return Err(BinancePublicError::Sequence);
        }
        previous_sequence = Some(values.sequence);
        if values.close_time_ms >= received_at_ms {
            continue;
        }
        let bar = values.into_public_bar(binding.symbol.clone(), generation, received_at_ms);
        if !bar.is_valid() {
            return Err(BinancePublicError::Value);
        }
        bars.push(bar);
    }
    Ok(bars)
}

fn public_market_stream_object(
    payload: &str,
    binding: &PublicMarketBinding,
    generation: u64,
    event: &str,
) -> Result<(Map<String, Value>, String), BinancePublicError> {
    binding
        .validate()
        .map_err(|_| BinancePublicError::Binding)?;
    if generation == 0 {
        return Err(BinancePublicError::Generation);
    }
    let value: Value = serde_json::from_str(payload).map_err(|_| BinancePublicError::Payload)?;
    let object = match value {
        Value::Object(mut wrapper) if wrapper.contains_key("data") => {
            let stream = wrapper
                .remove("stream")
                .and_then(|value| value.as_str().map(str::to_owned))
                .ok_or(BinancePublicError::Payload)?;
            let data = wrapper.remove("data").ok_or(BinancePublicError::Payload)?;
            let object = data
                .as_object()
                .cloned()
                .ok_or(BinancePublicError::Payload)?;
            let expected_prefix = native_symbol(&binding.symbol).to_ascii_lowercase();
            if !stream.starts_with(&expected_prefix) {
                return Err(BinancePublicError::Symbol);
            }
            object
        }
        Value::Object(object) => object,
        _ => return Err(BinancePublicError::Payload),
    };
    let expected_native = native_symbol(&binding.symbol);
    check_symbol(object.get("s"), &expected_native)?;
    if object.get("e").and_then(Value::as_str) != Some(event) {
        return Err(BinancePublicError::Payload);
    }
    Ok((object, expected_native))
}

fn validate_depth20_snapshot(
    bids: &[MarketLevel],
    asks: &[MarketLevel],
) -> Result<(), BinancePublicError> {
    if bids.is_empty() || asks.is_empty() || bids.len() > 20 || asks.len() > 20 {
        return Err(BinancePublicError::Value);
    }
    if bids.windows(2).any(|pair| pair[0].price <= pair[1].price)
        || asks.windows(2).any(|pair| pair[0].price >= pair[1].price)
        || bids[0].price >= asks[0].price
    {
        return Err(BinancePublicError::Value);
    }
    Ok(())
}

#[derive(Clone)]
struct KlineValues {
    interval: BinanceKlineInterval,
    sequence: u64,
    open_time_ms: u64,
    close_time_ms: u64,
    open: Price,
    high: Price,
    low: Price,
    close: Price,
    evidence: CompleteBarEvidence,
}

impl KlineValues {
    fn into_public_bar(self, symbol: Symbol, generation: u64, received_at_ms: u64) -> PublicBar {
        PublicBar {
            symbol,
            generation,
            received_at_ms,
            sequence: self.sequence,
            open_time_ms: self.open_time_ms,
            close_time_ms: self.close_time_ms,
            interval_ms: self.interval.milliseconds(),
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            base_volume: FieldState::Known(self.evidence.base_volume),
            quote_volume: FieldState::Known(self.evidence.quote_volume),
            trade_count: FieldState::Known(self.evidence.trade_count),
            taker_buy_base_volume: FieldState::Known(self.evidence.taker_buy_base_volume),
            taker_buy_quote_volume: FieldState::Known(self.evidence.taker_buy_quote_volume),
        }
    }

    fn into_forming_bar(
        self,
        symbol: Symbol,
        generation: u64,
        received_at_ms: u64,
        exchange_time_ms: u64,
    ) -> BinanceFormingBar {
        BinanceFormingBar {
            symbol,
            generation,
            received_at_ms,
            exchange_time_ms,
            sequence: self.sequence,
            open_time_ms: self.open_time_ms,
            close_time_ms: self.close_time_ms,
            interval: self.interval,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            base_volume: self.evidence.base_volume,
            quote_volume: self.evidence.quote_volume,
            trade_count: self.evidence.trade_count,
            taker_buy_base_volume: self.evidence.taker_buy_base_volume,
            taker_buy_quote_volume: self.evidence.taker_buy_quote_volume,
        }
    }
}

fn parse_kline_values(kline: &Map<String, Value>) -> Result<KlineValues, BinancePublicError> {
    let interval = BinanceKlineInterval::parse(
        kline
            .get("i")
            .and_then(Value::as_str)
            .ok_or(BinancePublicError::Payload)?,
    )?;
    let values = kline_values(
        interval,
        positive_u64(kline.get("t"))?,
        positive_u64(kline.get("T"))?,
        (
            positive_price(kline.get("o"))?,
            positive_price(kline.get("h"))?,
            positive_price(kline.get("l"))?,
            positive_price(kline.get("c"))?,
        ),
        complete_bar_evidence(kline)?,
    )?;
    Ok(values)
}

fn parse_rest_kline_values(
    fields: &[Value],
    interval: BinanceKlineInterval,
) -> Result<KlineValues, BinancePublicError> {
    let evidence = complete_rest_bar_evidence(
        u64_value(fields.get(8))?,
        non_negative_decimal(fields.get(5))?,
        non_negative_decimal(fields.get(7))?,
        non_negative_decimal(fields.get(9))?,
        non_negative_decimal(fields.get(10))?,
    )?;
    kline_values(
        interval,
        positive_u64(fields.first())?,
        positive_u64(fields.get(6))?,
        (
            positive_price(fields.get(1))?,
            positive_price(fields.get(2))?,
            positive_price(fields.get(3))?,
            positive_price(fields.get(4))?,
        ),
        evidence,
    )
}

fn kline_values(
    interval: BinanceKlineInterval,
    open_time_ms: u64,
    close_time_ms: u64,
    prices: (Price, Price, Price, Price),
    evidence: CompleteBarEvidence,
) -> Result<KlineValues, BinancePublicError> {
    let (open, high, low, close) = prices;
    let interval_ms = interval.milliseconds();
    let expected_close = open_time_ms
        .checked_add(interval_ms - 1)
        .ok_or(BinancePublicError::Sequence)?;
    if !open_time_ms.is_multiple_of(interval_ms)
        || close_time_ms != expected_close
        || high < open.max(close)
        || low > open.min(close)
        || high < low
        || !quote_volume_is_price_bounded(evidence.base_volume, evidence.quote_volume, low, high)
        || !quote_volume_is_price_bounded(
            evidence.taker_buy_base_volume,
            evidence.taker_buy_quote_volume,
            low,
            high,
        )
    {
        return Err(BinancePublicError::Value);
    }
    let sequence = open_time_ms
        .checked_div(interval_ms)
        .and_then(|value| value.checked_add(1))
        .ok_or(BinancePublicError::Sequence)?;
    Ok(KlineValues {
        interval,
        sequence,
        open_time_ms,
        close_time_ms,
        open,
        high,
        low,
        close,
        evidence,
    })
}

fn quote_volume_is_price_bounded(
    base_volume: Decimal,
    quote_volume: Decimal,
    low: Price,
    high: Price,
) -> bool {
    let Some(minimum) = base_volume.checked_mul(low.value()) else {
        return false;
    };
    let Some(maximum) = base_volume.checked_mul(high.value()) else {
        return false;
    };
    quote_volume >= minimum && quote_volume <= maximum
}

fn complete_rest_bar_evidence(
    trade_count: u64,
    base_volume: Decimal,
    quote_volume: Decimal,
    taker_buy_base_volume: Decimal,
    taker_buy_quote_volume: Decimal,
) -> Result<CompleteBarEvidence, BinancePublicError> {
    if taker_buy_base_volume > base_volume
        || taker_buy_quote_volume > quote_volume
        || (trade_count == 0
            && (!base_volume.is_zero()
                || !quote_volume.is_zero()
                || !taker_buy_base_volume.is_zero()
                || !taker_buy_quote_volume.is_zero()))
        || (trade_count > 0 && (base_volume <= Decimal::ZERO || quote_volume <= Decimal::ZERO))
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

#[derive(Clone)]
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

fn required_string(value: Option<&Value>) -> Result<&str, BinancePublicError> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or(BinancePublicError::Payload)
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
    #[error("Binance public kline interval is outside the local chart contract")]
    Interval,
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

#[cfg(test)]
mod public_market_tests {
    use super::*;
    use venue_gateway_api::{GatewayMode, VenueId};

    fn binding() -> Result<PublicMarketBinding, Box<dyn std::error::Error>> {
        Ok(PublicMarketBinding::new(
            VenueId::Binance,
            GatewayMode::Live,
            venue_domain::domain::MarketKind::LinearPerpetual,
            "BTC/USDT".parse()?,
        )?)
    }

    fn kline(closed: bool, event_time_ms: u64) -> String {
        format!(
            r#"{{"e":"kline","E":{event_time_ms},"s":"BTCUSDT","k":{{"t":60000,"T":119999,"s":"BTCUSDT","i":"1m","f":1,"L":2,"o":"100","h":"110","l":"90","c":"105","v":"2","n":2,"x":{closed},"q":"200","V":"1","Q":"100"}}}}"#
        )
    }

    #[test]
    fn public_market_parses_direct_and_combined_bbo_trade_and_depth20()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let bbo = parse_public_market_bbo(
            r#"{"e":"bookTicker","E":1000,"T":999,"s":"BTCUSDT","u":3,"b":"100","B":"2","a":"101","A":"3"}"#,
            &binding,
            7,
            1_001,
        )?;
        assert_eq!(bbo.fact().symbol.to_string(), "BTC/USDT");
        assert_eq!(bbo.fact().generation, 7);

        let trade = parse_public_market_agg_trade(
            r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1000,"T":999,"s":"BTCUSDT","a":11,"p":"100","q":"2","f":21,"l":22,"m":false}}"#,
            &binding,
            7,
            1_001,
        )?;
        assert_eq!(trade.fact().quote_quantity, Decimal::from(200));
        assert_eq!(
            trade.fact().aggressor,
            FieldState::Known(AggressorSide::Buy)
        );

        let depth = parse_public_market_depth20_snapshot(
            r#"{"stream":"btcusdt@depth20@100ms","data":{"e":"depthUpdate","E":1000,"T":999,"s":"BTCUSDT","u":10,"b":[["100","2"],["99","1"]],"a":[["101","3"],["102","4"]]}}"#,
            &binding,
            7,
        )?;
        assert_eq!(depth.fact().sequence, 10);
        assert_eq!(depth.fact().bids.len(), 2);
        assert_eq!(depth.fact().asks.len(), 2);
        Ok(())
    }

    #[test]
    fn exchange_info_returns_all_and_only_trading_usd_m_perpetuals()
    -> Result<(), Box<dyn std::error::Error>> {
        let payload = r#"{
            "timezone":"UTC",
            "symbols":[
                {"symbol":"BTCUSDT","baseAsset":"BTC","quoteAsset":"USDT","status":"TRADING","contractType":"PERPETUAL","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.10"},{"filterType":"LOT_SIZE","stepSize":"0.001"}]},
                {"symbol":"ETHUSDC","baseAsset":"ETH","quoteAsset":"USDC","status":"TRADING","contractType":"PERPETUAL","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.01"},{"filterType":"LOT_SIZE","stepSize":"0.001"}]},
                {"symbol":"SOLUSDT_250926","baseAsset":"SOL","quoteAsset":"USDT","status":"TRADING","contractType":"CURRENT_QUARTER"},
                {"symbol":"OLDUSDT","baseAsset":"OLD","quoteAsset":"USDT","status":"SETTLING","contractType":"PERPETUAL"},
                {"symbol":"BTCUSD","baseAsset":"BTC","quoteAsset":"USD","status":"TRADING","contractType":"PERPETUAL"}
            ]
        }"#;
        let symbols = parse_public_exchange_info(payload)?;
        assert_eq!(
            symbols
                .into_iter()
                .map(|symbol| symbol.to_string())
                .collect::<Vec<_>>(),
            ["BTC/USDT", "ETH/USDC"]
        );
        let catalog = parse_public_exchange_catalog(payload)?;
        assert_eq!(catalog[0].price_tick, Decimal::new(1, 1));
        assert_eq!(catalog[0].quantity_step, Decimal::new(1, 3));
        assert_eq!(
            parse_public_exchange_info(&payload.replace("BTCUSDT", "WRONG")),
            Err(BinancePublicError::Symbol)
        );
        Ok(())
    }

    #[test]
    fn public_market_kline_keeps_forming_data_out_of_domain_bar()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        match parse_public_market_kline(&kline(false, 70_000), &binding, 7, 70_001)? {
            BinancePublicKline::Forming(envelope) => {
                assert_eq!(envelope.fact().interval, BinanceKlineInterval::OneMinute);
                assert_eq!(envelope.fact().sequence, 2);
            }
            BinancePublicKline::Closed(_) => return Err("forming frame became a PublicBar".into()),
        }
        match parse_public_market_kline(&kline(true, 120_000), &binding, 7, 120_001)? {
            BinancePublicKline::Closed(envelope) => assert!(envelope.fact().is_valid()),
            BinancePublicKline::Forming(_) => return Err("closed frame was not promoted".into()),
        }
        Ok(())
    }

    #[test]
    fn public_market_rejects_wrong_symbol_old_generation_and_invalid_ohlc()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let bbo = r#"{"e":"bookTicker","E":1000,"T":999,"s":"BTCUSDT","u":3,"b":"100","B":"2","a":"101","A":"3"}"#;
        assert_eq!(
            parse_public_market_bbo(&bbo.replace("BTCUSDT", "ETHUSDT"), &binding, 7, 1),
            Err(BinancePublicError::Symbol)
        );
        assert_eq!(
            parse_public_market_bbo(bbo, &binding, 0, 1),
            Err(BinancePublicError::Generation)
        );
        assert_eq!(
            parse_public_market_kline(
                &kline(true, 120_000).replace("\"h\":\"110\"", "\"h\":\"99\""),
                &binding,
                7,
                120_001
            ),
            Err(BinancePublicError::Value)
        );
        Ok(())
    }

    #[test]
    fn rest_klines_filter_the_unclosed_tail_and_reject_bad_ordering()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let payload = r#"[
            [60000,"100","110","90","105","2",119999,"200",2,"1","100","0"],
            [120000,"105","115","100","110","2",179999,"210",2,"1","105","0"]
        ]"#;
        let bars = parse_public_market_rest_klines(
            payload,
            &binding,
            7,
            150_000,
            BinanceKlineInterval::OneMinute,
        )?;
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].open_time_ms, 60_000);
        assert!(bars[0].is_valid());
        assert_eq!(
            parse_public_market_rest_klines(
                r#"[[120000,"100","110","90","105","2",179999,"200",2,"1","100","0"],[60000,"100","110","90","105","2",119999,"200",2,"1","100","0"]]"#,
                &binding,
                7,
                200_000,
                BinanceKlineInterval::OneMinute,
            ),
            Err(BinancePublicError::Sequence)
        );
        Ok(())
    }

    #[test]
    fn chart_intervals_are_exactly_the_initial_contract() {
        assert_eq!(
            [
                BinanceKlineInterval::OneMinute,
                BinanceKlineInterval::FiveMinutes,
                BinanceKlineInterval::FifteenMinutes,
                BinanceKlineInterval::OneHour,
                BinanceKlineInterval::FourHours,
                BinanceKlineInterval::OneDay,
            ]
            .map(BinanceKlineInterval::as_str),
            ["1m", "5m", "15m", "1h", "4h", "1d"]
        );
        assert_eq!(
            BinanceKlineInterval::parse("3m"),
            Err(BinancePublicError::Interval)
        );
    }

    #[test]
    fn all_market_ticker_array_is_catalog_scoped_and_signed_change_is_preserved()
    -> Result<(), Box<dyn std::error::Error>> {
        let catalog = vec!["BTC/USDT".parse()?, "ETH/USDC".parse()?];
        let payload = r#"{"stream":"!ticker@arr","data":[
            {"e":"24hrTicker","E":1000,"s":"BTCUSDT","c":"101.5","P":"-2.25","q":"500000"},
            {"e":"24hrTicker","E":1001,"s":"ETHUSDC","c":"2500","P":"3.75","q":"800000"},
            {"e":"24hrTicker","E":1001,"s":"UNKNOWN","c":"1","P":"0","q":"1"}
        ]}"#;
        let tickers = parse_public_market_ticker_array(payload, &catalog, 1_002)?;
        assert_eq!(tickers.len(), 2);
        assert_eq!(tickers[0].symbol.to_string(), "BTC/USDT");
        assert_eq!(tickers[0].price_change_percent, Decimal::new(-225, 2));
        assert_eq!(tickers[1].last_price.value(), Decimal::from(2_500));
        let snapshot = r#"[
            {"symbol":"BTCUSDT","lastPrice":"102.5","priceChangePercent":"-1.5","quoteVolume":"900000","closeTime":1000},
            {"symbol":"ETHUSDC","lastPrice":"2510","priceChangePercent":"4.25","quoteVolume":"700000","closeTime":1001}
        ]"#;
        let tickers = parse_public_market_ticker_snapshot(snapshot, &catalog, 1_002)?;
        assert_eq!(tickers.len(), 2);
        assert_eq!(tickers[0].last_price.value(), Decimal::new(1_025, 1));
        assert_eq!(tickers[1].price_change_percent, Decimal::new(425, 2));
        Ok(())
    }
}
