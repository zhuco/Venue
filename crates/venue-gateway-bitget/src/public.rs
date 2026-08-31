//! Bitget UTA public-market protocol parsing.
//!
//! This module deliberately has no socket, HTTP, filesystem, credential, or mutation code.  A
//! caller records the exact [`BitgetRawPublicPayload`] first, then invokes one of the pure
//! parsers below.  The UTA REST order-book response contains no sequence or native symbol, so it
//! is never accepted as an incremental-book baseline.  Only a `books` WebSocket `snapshot`, whose
//! `seq` is documented by Bitget, may bridge subsequent `update` messages.

use std::{collections::BTreeSet, str::FromStr};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use venue_domain::domain::{
    AggressorSide, FieldState, MarketDelta, MarketEvent, MarketLevel, MarketSnapshot, Price,
    PublicTicker, PublicTrade, Symbol, UnknownReason,
};

pub const BITGET_PUBLIC_PARSER_SCHEMA_VERSION: u16 = 1;
pub const BITGET_UTA_FUTURES_CATEGORY: &str = "USDT-FUTURES";
pub const BITGET_UTA_FUTURES_INST_TYPE: &str = "usdt-futures";
#[cfg(test)]
const DEFAULT_PUBLIC_FRESHNESS_MS: u64 = 5_000;
const MAX_BOOK_LEVELS: usize = 1_000;

/// The source is durable metadata, not a transport capability.  The surrounding runtime owns
/// recording and retry/backoff effects.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BitgetPublicSource {
    RestOrderBook,
    RestTicker,
    WebSocketBooks,
    WebSocketPublicTrade,
}

/// A credential-free raw-payload envelope ready for a caller-owned durable journal.
///
/// `native_symbol` is reconstructed from the canonical `symbol`, rather than accepted from an
/// untrusted caller.  This prevents a response for one native instrument from being relabelled as
/// another canonical symbol before parsing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BitgetRawPublicPayload {
    pub parser_schema_version: u16,
    pub source: BitgetPublicSource,
    pub symbol: Symbol,
    pub native_symbol: String,
    pub generation: u64,
    pub received_at_ms: u64,
    pub payload_sha256: String,
    pub payload: String,
}

impl BitgetRawPublicPayload {
    pub fn new(
        source: BitgetPublicSource,
        symbol: Symbol,
        generation: u64,
        received_at_ms: u64,
        payload: String,
    ) -> Result<Self, BitgetPublicError> {
        if generation == 0 || received_at_ms == 0 || payload.is_empty() {
            return Err(BitgetPublicError::Metadata);
        }
        let native_symbol = native_symbol(&symbol)?;
        let payload_sha256 = payload_digest(&payload);
        Ok(Self {
            parser_schema_version: BITGET_PUBLIC_PARSER_SCHEMA_VERSION,
            source,
            symbol,
            native_symbol,
            generation,
            received_at_ms,
            payload_sha256,
            payload,
        })
    }

    pub fn validate(&self) -> Result<(), BitgetPublicError> {
        if self.parser_schema_version != BITGET_PUBLIC_PARSER_SCHEMA_VERSION
            || self.generation == 0
            || self.received_at_ms == 0
            || self.payload.is_empty()
            || self.payload_sha256 != payload_digest(&self.payload)
            || self.native_symbol != native_symbol(&self.symbol)?
        {
            return Err(BitgetPublicError::Metadata);
        }
        Ok(())
    }
}

/// Builds the only UTA REST order-book route accepted by this adapter.  It performs no I/O.
pub fn rest_orderbook_path(symbol: &Symbol, limit: u16) -> Result<String, BitgetPublicError> {
    if limit == 0 || usize::from(limit) > MAX_BOOK_LEVELS {
        return Err(BitgetPublicError::DepthLimit);
    }
    let native = native_symbol(symbol)?;
    Ok(format!(
        "/api/v3/market/orderbook?category={BITGET_UTA_FUTURES_CATEGORY}&symbol={native}&limit={limit}"
    ))
}

/// Builds the UTA ticker query used to obtain a BBO and mark-price reference.  It performs no
/// I/O and makes the query's requested native symbol explicit in the raw-payload binding.
pub fn rest_ticker_path(symbol: &Symbol) -> Result<String, BitgetPublicError> {
    let native = native_symbol(symbol)?;
    Ok(format!(
        "/api/v3/market/tickers?category={BITGET_UTA_FUTURES_CATEGORY}&symbol={native}"
    ))
}

/// The documented JSON subscriptions for the two UTA streams consumed by this parser.
pub fn public_subscriptions(symbol: &Symbol) -> Result<Value, BitgetPublicError> {
    let native = native_symbol(symbol)?;
    Ok(json!({
        "op": "subscribe",
        "args": [
            {
                "instType": BITGET_UTA_FUTURES_INST_TYPE,
                "topic": "books",
                "symbol": native,
            },
            {
                "instType": BITGET_UTA_FUTURES_INST_TYPE,
                "topic": "publicTrade",
                "symbol": native,
            },
        ],
    }))
}

/// The narrow subscription owned by the Scalping resident.  It intentionally excludes ticker
/// and trade channels: a sequenced `books` snapshot plus covering updates is the only public
/// input that may establish a strategy book.
pub fn scalping_book_subscription(symbol: &Symbol) -> Result<Value, BitgetPublicError> {
    let native = native_symbol(symbol)?;
    Ok(json!({
        "op": "subscribe",
        "args": [{
            "instType": BITGET_UTA_FUTURES_INST_TYPE,
            "topic": "books",
            "symbol": native,
        }],
    }))
}

/// A bounded REST depth snapshot.  Bitget's REST response has no `seq`, so this type intentionally
/// cannot be converted to a [`MarketSnapshot`] or submitted to [`BitgetBookSequencer`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetRestOrderBook {
    pub raw: BitgetRawPublicPayload,
    pub exchange_time_ms: u64,
    pub bids: Vec<MarketLevel>,
    pub asks: Vec<MarketLevel>,
}

#[cfg(test)]
impl BitgetRestOrderBook {
    pub fn best_bid_ask(&self) -> Result<(Price, Price), BitgetPublicError> {
        best_bid_ask(&self.bids, &self.asks)
    }

    pub fn fresh_at(&self, now_ms: u64, maximum_age_ms: u64) -> bool {
        fresh_at(self.raw.received_at_ms, now_ms, maximum_age_ms)
    }
}

/// Parses `GET /api/v3/market/orderbook`.  The response itself does not echo its symbol, so the
/// function first validates the request-bound raw envelope and then returns a non-bridgeable
/// reference snapshot.
pub fn parse_rest_orderbook(
    raw: BitgetRawPublicPayload,
) -> Result<BitgetRestOrderBook, BitgetPublicError> {
    require_source(&raw, BitgetPublicSource::RestOrderBook)?;
    let root_value = parse_success_envelope(&raw.payload)?;
    let root = object(&root_value)?;
    let data = object(root.get("data").ok_or(BitgetPublicError::Payload)?)?;
    let exchange_time_ms = timestamp(data.get("ts"))?;
    let bids = parse_levels(data.get("b"), BookLevels::Snapshot)?;
    let asks = parse_levels(data.get("a"), BookLevels::Snapshot)?;
    validate_complete_book(&bids, &asks)?;
    Ok(BitgetRestOrderBook {
        raw,
        exchange_time_ms,
        bids,
        asks,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BitgetBookAction {
    Snapshot,
    Update,
}

/// `seq` and `pseq` are native sequence metadata.  They stay inside the Bitget adapter until a
/// validated message is mapped to a normalized `MarketSnapshot` or `MarketDelta`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BitgetBookSequence {
    pub previous_sequence: u64,
    pub sequence: u64,
}

/// A parsed `books` frame.  Its raw envelope contains the exact native symbol and original JSON.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetBooksMessage {
    pub raw: BitgetRawPublicPayload,
    pub action: BitgetBookAction,
    pub sequence: BitgetBookSequence,
    pub exchange_time_ms: u64,
    pub maximum_depth: u16,
    pub bids: Vec<MarketLevel>,
    pub asks: Vec<MarketLevel>,
}

impl BitgetBooksMessage {
    /// Converts only after the caller's sequencer has assigned the active public generation.
    pub fn normalize(&self, generation: u64) -> Result<MarketEvent, BitgetPublicError> {
        if generation == 0 {
            return Err(BitgetPublicError::Generation);
        }
        match self.action {
            BitgetBookAction::Snapshot => Ok(MarketEvent::Snapshot(MarketSnapshot {
                symbol: self.raw.symbol.clone(),
                generation,
                sequence: self.sequence.sequence,
                exchange_time_ms: Some(self.exchange_time_ms),
                bids: self.bids.clone(),
                asks: self.asks.clone(),
            })),
            BitgetBookAction::Update => Ok(MarketEvent::Delta(MarketDelta {
                symbol: self.raw.symbol.clone(),
                generation,
                // Domain `first_sequence` is inclusive.  For Bitget an update covers the
                // snapshot iff `snapshot.seq` lies in the documented `[pseq, seq]` interval.
                first_sequence: self.sequence.previous_sequence,
                previous_sequence: Some(self.sequence.previous_sequence),
                sequence: self.sequence.sequence,
                exchange_time_ms: Some(self.exchange_time_ms),
                bids: self.bids.clone(),
                asks: self.asks.clone(),
            })),
        }
    }
}

#[cfg(test)]
impl BitgetBooksMessage {
    pub fn fresh_at(&self, now_ms: u64, maximum_age_ms: u64) -> bool {
        fresh_at(self.raw.received_at_ms, now_ms, maximum_age_ms)
    }
}

/// Parses one UTA `books` WebSocket `snapshot` or `update` frame with an exact `books`,
/// `usdt-futures`, and native-symbol binding.
pub fn parse_books_message(
    raw: BitgetRawPublicPayload,
) -> Result<BitgetBooksMessage, BitgetPublicError> {
    require_source(&raw, BitgetPublicSource::WebSocketBooks)?;
    let root_value = parse_json_object(&raw.payload)?;
    let root = object(&root_value)?;
    require_websocket_argument(root, &raw.native_symbol, "books")?;
    let action = match text(root, "action")? {
        "snapshot" => BitgetBookAction::Snapshot,
        "update" => BitgetBookAction::Update,
        _ => return Err(BitgetPublicError::Payload),
    };
    // The outer timestamp is part of the documented WebSocket frame.  The event's matching-engine
    // time is the inner `data[0].ts`, which is the timestamp exposed to normalized consumers.
    timestamp(root.get("ts"))?;
    let data = exact_one_data(root)?;
    let exchange_time_ms = timestamp(data.get("ts"))?;
    let book_sequence = BitgetBookSequence {
        previous_sequence: sequence(data.get("pseq"), true)?,
        sequence: sequence(data.get("seq"), false)?,
    };
    if book_sequence.sequence <= book_sequence.previous_sequence {
        return Err(BitgetPublicError::Sequence);
    }
    // Live UTA `books` frames currently spell this `maxdepth`; earlier captures and fixtures use
    // `maxDepth`. Both are the same documented depth field, so retain an exact two-spelling
    // compatibility boundary instead of weakening the payload schema generally.
    let maximum_depth = u16::try_from(sequence(
        data.get("maxDepth").or_else(|| data.get("maxdepth")),
        false,
    )?)
    .map_err(|_| BitgetPublicError::Payload)?;
    if maximum_depth == 0 || usize::from(maximum_depth) > MAX_BOOK_LEVELS {
        return Err(BitgetPublicError::Payload);
    }
    let mode = match action {
        BitgetBookAction::Snapshot => BookLevels::Snapshot,
        BitgetBookAction::Update => BookLevels::Update,
    };
    let bids = parse_levels(data.get("b"), mode)?;
    let asks = parse_levels(data.get("a"), mode)?;
    if matches!(action, BitgetBookAction::Snapshot) {
        if book_sequence.previous_sequence != 0 {
            return Err(BitgetPublicError::Sequence);
        }
        validate_complete_book(&bids, &asks)?;
    } else if bids.is_empty() && asks.is_empty() {
        return Err(BitgetPublicError::Payload);
    }
    Ok(BitgetBooksMessage {
        raw,
        action,
        sequence: book_sequence,
        exchange_time_ms,
        maximum_depth,
        bids,
        asks,
    })
}

/// The active `books` sequence state.  A `ResetRequired` result clears the active bridge; no
/// following update may be normalized until a new WebSocket `snapshot` is accepted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetBookSequencer {
    generation: u64,
    active: Option<ActiveBook>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveBook {
    symbol: Symbol,
    generation: u64,
    snapshot_sequence: u64,
    last_sequence: u64,
    bridged: bool,
}

impl Default for BitgetBookSequencer {
    fn default() -> Self {
        Self::new()
    }
}

impl BitgetBookSequencer {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            generation: 1,
            active: None,
        }
    }

    #[must_use]
    pub const fn next_generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn ready_generation(&self) -> Option<u64> {
        self.active
            .as_ref()
            .and_then(|active| active.bridged.then_some(active.generation))
    }

    /// Process restart must never reuse an already journaled public generation. This deliberately
    /// drops any local bridge: only a later native `snapshot` may make the new generation ready.
    pub fn reset_generation(&mut self, generation: u64) -> Result<(), BitgetPublicError> {
        if generation == 0 || generation < self.generation {
            return Err(BitgetPublicError::Generation);
        }
        self.generation = generation;
        self.active = None;
        Ok(())
    }

    /// Applies the documented `pseq` contract.  A first update must cover the snapshot's `seq`;
    /// later updates must name the immediately preceding `seq`.  A zero `pseq` on an update is an
    /// explicit exchange-reset signal, never an opportunity to keep an old book.
    pub fn accept(
        &mut self,
        message: &BitgetBooksMessage,
    ) -> Result<BitgetBookSequenceStatus, BitgetPublicError> {
        match message.action {
            BitgetBookAction::Snapshot => self.accept_snapshot(message),
            BitgetBookAction::Update => self.accept_update(message),
        }
    }

    fn accept_snapshot(
        &mut self,
        message: &BitgetBooksMessage,
    ) -> Result<BitgetBookSequenceStatus, BitgetPublicError> {
        let replaced_generation = self.active.as_ref().map(|active| active.generation);
        if replaced_generation.is_some() {
            self.advance_generation()?;
        }
        self.active = Some(ActiveBook {
            symbol: message.raw.symbol.clone(),
            generation: self.generation,
            snapshot_sequence: message.sequence.sequence,
            last_sequence: message.sequence.sequence,
            bridged: false,
        });
        Ok(BitgetBookSequenceStatus::Snapshot {
            generation: self.generation,
            replaced_generation,
        })
    }

    fn accept_update(
        &mut self,
        message: &BitgetBooksMessage,
    ) -> Result<BitgetBookSequenceStatus, BitgetPublicError> {
        if message.sequence.previous_sequence == 0 {
            return self.invalidate(BitgetBookSequenceFault::VenueReset);
        }
        let Some(active) = self.active.as_mut() else {
            return Ok(BitgetBookSequenceStatus::ResetRequired {
                generation: self.generation,
                reason: BitgetBookSequenceFault::MissingSnapshot,
            });
        };
        if active.symbol != message.raw.symbol {
            return self.invalidate(BitgetBookSequenceFault::SymbolMismatch);
        }
        if !active.bridged {
            let covers_snapshot = message.sequence.previous_sequence <= active.snapshot_sequence
                && active.snapshot_sequence <= message.sequence.sequence;
            if !covers_snapshot {
                return self.invalidate(BitgetBookSequenceFault::SnapshotNotCovered);
            }
            active.last_sequence = message.sequence.sequence;
            active.bridged = true;
            return Ok(BitgetBookSequenceStatus::Bridged {
                generation: active.generation,
                sequence: active.last_sequence,
            });
        }
        if message.sequence.previous_sequence != active.last_sequence {
            return self.invalidate(BitgetBookSequenceFault::PreviousSequenceMismatch);
        }
        active.last_sequence = message.sequence.sequence;
        Ok(BitgetBookSequenceStatus::Updated {
            generation: active.generation,
            sequence: active.last_sequence,
        })
    }

    fn invalidate(
        &mut self,
        reason: BitgetBookSequenceFault,
    ) -> Result<BitgetBookSequenceStatus, BitgetPublicError> {
        if self.active.take().is_some() {
            self.advance_generation()?;
        }
        Ok(BitgetBookSequenceStatus::ResetRequired {
            generation: self.generation,
            reason,
        })
    }

    fn advance_generation(&mut self) -> Result<(), BitgetPublicError> {
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or(BitgetPublicError::Generation)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BitgetBookSequenceFault {
    MissingSnapshot,
    SnapshotNotCovered,
    PreviousSequenceMismatch,
    VenueReset,
    SymbolMismatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BitgetBookSequenceStatus {
    Snapshot {
        generation: u64,
        replaced_generation: Option<u64>,
    },
    Bridged {
        generation: u64,
        sequence: u64,
    },
    Updated {
        generation: u64,
        sequence: u64,
    },
    ResetRequired {
        generation: u64,
        reason: BitgetBookSequenceFault,
    },
}

impl BitgetBookSequenceStatus {
    #[must_use]
    pub const fn active_generation(self) -> Option<u64> {
        match self {
            Self::Snapshot { generation, .. }
            | Self::Bridged { generation, .. }
            | Self::Updated { generation, .. } => Some(generation),
            Self::ResetRequired { .. } => None,
        }
    }

    #[must_use]
    pub const fn ready(self) -> bool {
        matches!(self, Self::Bridged { .. } | Self::Updated { .. })
    }
}

/// A ticker response carries the complete BBO and a current futures mark-price reference with one
/// matching-engine timestamp.  It is intentionally separate from `MarkFunding`: UTA ticker does
/// not provide a next funding time, so fabricating one would be incorrect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetTickerEvent {
    pub raw: BitgetRawPublicPayload,
    pub bbo: PublicTicker,
    pub mark: BitgetMarkPrice,
    pub last_price: Price,
}

#[cfg(test)]
impl BitgetTickerEvent {
    pub fn market_event(&self) -> MarketEvent {
        MarketEvent::Ticker(self.bbo.clone())
    }

    pub fn fresh_at(&self, now_ms: u64, maximum_age_ms: u64) -> bool {
        fresh_at(self.raw.received_at_ms, now_ms, maximum_age_ms)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetMarkPrice {
    pub symbol: Symbol,
    pub generation: u64,
    pub received_at_ms: u64,
    pub exchange_time_ms: u64,
    pub mark_price: Price,
    pub index_price: Price,
    pub funding_rate: Decimal,
}

/// Parses `GET /api/v3/market/tickers` for exactly the requested USDT-futures native symbol.
pub fn parse_rest_ticker(
    raw: BitgetRawPublicPayload,
) -> Result<BitgetTickerEvent, BitgetPublicError> {
    require_source(&raw, BitgetPublicSource::RestTicker)?;
    let root_value = parse_success_envelope(&raw.payload)?;
    let root = object(&root_value)?;
    let values = root
        .get("data")
        .and_then(Value::as_array)
        .ok_or(BitgetPublicError::Payload)?;
    let value = exact_one(values)?;
    let ticker = object(value)?;
    if text(ticker, "category")? != BITGET_UTA_FUTURES_CATEGORY
        || text(ticker, "symbol")? != raw.native_symbol.as_str()
    {
        return Err(BitgetPublicError::Symbol);
    }
    let exchange_time_ms = timestamp(ticker.get("ts"))?;
    let bid_price = price(ticker.get("bid1Price"))?;
    let ask_price = price(ticker.get("ask1Price"))?;
    if bid_price >= ask_price {
        return Err(BitgetPublicError::Payload);
    }
    let bbo = PublicTicker {
        symbol: raw.symbol.clone(),
        generation: raw.generation,
        received_at_ms: raw.received_at_ms,
        exchange_time_ms,
        transaction_time_ms: exchange_time_ms,
        // UTA ticker does not expose an order-book update ID.  Its matching-engine timestamp is
        // the only documented monotonic-ish watermark, and consumers must not infer strict order
        // from it.  `books` remains the authoritative BBO sequence path.
        update_id: exchange_time_ms,
        bid_price,
        bid_quantity: decimal(ticker.get("bid1Size"))?,
        ask_price,
        ask_quantity: decimal(ticker.get("ask1Size"))?,
    };
    let mark = BitgetMarkPrice {
        symbol: raw.symbol.clone(),
        generation: raw.generation,
        received_at_ms: raw.received_at_ms,
        exchange_time_ms,
        mark_price: price(ticker.get("markPrice"))?,
        index_price: price(ticker.get("indexPrice"))?,
        funding_rate: decimal(ticker.get("fundingRate"))?,
    };
    Ok(BitgetTickerEvent {
        raw,
        bbo,
        mark,
        last_price: price(ticker.get("lastPrice"))?,
    })
}

/// A public trade retains Bitget's execution and correlation identifiers in addition to the
/// shared domain trade fields.  Public-trade frames have no documented contiguous sequence; only
/// duplicate IDs inside one frame are rejected here.  Cross-frame durable dedupe belongs at the
/// common market/facts acceptance boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetPublicTradeEvent {
    pub raw: BitgetRawPublicPayload,
    pub trade: PublicTrade,
    pub correlation_id: u64,
    pub rpi: FieldState<bool>,
}

#[cfg(test)]
impl BitgetPublicTradeEvent {
    pub fn market_event(&self) -> MarketEvent {
        MarketEvent::Trade(self.trade.clone())
    }

    pub fn fresh_at(&self, now_ms: u64, maximum_age_ms: u64) -> bool {
        fresh_at(self.raw.received_at_ms, now_ms, maximum_age_ms)
    }
}

/// Parses a UTA `publicTrade` frame.  Each returned event carries the unchanged complete raw
/// frame so the caller can store one raw record and expose normalized per-trade facts.
pub fn parse_public_trade_message(
    raw: BitgetRawPublicPayload,
) -> Result<Vec<BitgetPublicTradeEvent>, BitgetPublicError> {
    require_source(&raw, BitgetPublicSource::WebSocketPublicTrade)?;
    let root_value = parse_json_object(&raw.payload)?;
    let root = object(&root_value)?;
    require_websocket_argument(root, &raw.native_symbol, "publicTrade")?;
    match text(root, "action")? {
        "snapshot" | "update" => {}
        _ => return Err(BitgetPublicError::Payload),
    }
    timestamp(root.get("ts"))?;
    let values = root
        .get("data")
        .and_then(Value::as_array)
        .ok_or(BitgetPublicError::Payload)?;
    if values.is_empty() {
        return Err(BitgetPublicError::Payload);
    }
    let mut ids = BTreeSet::new();
    values
        .iter()
        .map(|value| parse_public_trade(raw.clone(), value, &mut ids))
        .collect()
}

#[cfg(test)]
fn fresh_at(received_at_ms: u64, now_ms: u64, maximum_age_ms: u64) -> bool {
    maximum_age_ms > 0
        && received_at_ms > 0
        && now_ms >= received_at_ms
        && now_ms - received_at_ms <= maximum_age_ms
}

pub fn native_symbol(symbol: &Symbol) -> Result<String, BitgetPublicError> {
    if symbol.quote() != "USDT" {
        return Err(BitgetPublicError::Symbol);
    }
    Ok(format!("{}{}", symbol.base(), symbol.quote()))
}

fn parse_public_trade(
    raw: BitgetRawPublicPayload,
    value: &Value,
    ids: &mut BTreeSet<u64>,
) -> Result<BitgetPublicTradeEvent, BitgetPublicError> {
    let object = object(value)?;
    let execution_id = sequence(object.get("i"), false)?;
    if !ids.insert(execution_id) {
        return Err(BitgetPublicError::DuplicateTrade);
    }
    let correlation_id = sequence(object.get("L"), false)?;
    let price = price(object.get("p"))?;
    let quantity = decimal(object.get("v"))?;
    if quantity <= Decimal::ZERO {
        return Err(BitgetPublicError::Payload);
    }
    let aggressor = match text(object, "S")? {
        "buy" => FieldState::Known(AggressorSide::Buy),
        "sell" => FieldState::Known(AggressorSide::Sell),
        _ => return Err(BitgetPublicError::Payload),
    };
    let rpi = match object.get("isRPI") {
        None => FieldState::Missing,
        Some(Value::Null) => FieldState::Null,
        Some(Value::String(value)) if value == "yes" => FieldState::Known(true),
        Some(Value::String(value)) if value == "no" => FieldState::Known(false),
        Some(_) => FieldState::Unavailable {
            reason: UnknownReason::ParseFailure,
        },
    };
    let exchange_time_ms = timestamp(object.get("T"))?;
    let quote_quantity = price
        .value()
        .checked_mul(quantity)
        .ok_or(BitgetPublicError::Payload)?;
    let trade = PublicTrade {
        symbol: raw.symbol.clone(),
        generation: raw.generation,
        received_at_ms: raw.received_at_ms,
        exchange_time_ms,
        transaction_time_ms: exchange_time_ms,
        aggregate_trade_id: correlation_id,
        first_trade_id: execution_id,
        last_trade_id: execution_id,
        price,
        quantity,
        quote_quantity,
        aggressor,
    };
    Ok(BitgetPublicTradeEvent {
        raw,
        trade,
        correlation_id,
        rpi,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BookLevels {
    Snapshot,
    Update,
}

fn parse_levels(
    value: Option<&Value>,
    mode: BookLevels,
) -> Result<Vec<MarketLevel>, BitgetPublicError> {
    let values = value
        .and_then(Value::as_array)
        .ok_or(BitgetPublicError::Payload)?;
    if values.len() > MAX_BOOK_LEVELS {
        return Err(BitgetPublicError::Payload);
    }
    let mut seen = BTreeSet::new();
    values
        .iter()
        .map(|value| {
            let fields = value.as_array().ok_or(BitgetPublicError::Payload)?;
            if fields.len() != 2 {
                return Err(BitgetPublicError::Payload);
            }
            let price = price(fields.first())?;
            let quantity = decimal(fields.get(1))?;
            if quantity.is_sign_negative()
                || matches!(mode, BookLevels::Snapshot) && quantity.is_zero()
                || !seen.insert(price)
            {
                return Err(BitgetPublicError::Payload);
            }
            Ok(MarketLevel { price, quantity })
        })
        .collect()
}

fn validate_complete_book(
    bids: &[MarketLevel],
    asks: &[MarketLevel],
) -> Result<(), BitgetPublicError> {
    if bids.is_empty() || asks.is_empty() {
        return Err(BitgetPublicError::Payload);
    }
    if bids
        .windows(2)
        .any(|levels| levels[0].price <= levels[1].price)
        || asks
            .windows(2)
            .any(|levels| levels[0].price >= levels[1].price)
    {
        return Err(BitgetPublicError::Payload);
    }
    best_bid_ask(bids, asks).map(|_| ())
}

fn best_bid_ask(
    bids: &[MarketLevel],
    asks: &[MarketLevel],
) -> Result<(Price, Price), BitgetPublicError> {
    let bid = bids.first().ok_or(BitgetPublicError::Payload)?.price;
    let ask = asks.first().ok_or(BitgetPublicError::Payload)?.price;
    if bid >= ask {
        return Err(BitgetPublicError::Payload);
    }
    Ok((bid, ask))
}

fn require_source(
    raw: &BitgetRawPublicPayload,
    source: BitgetPublicSource,
) -> Result<(), BitgetPublicError> {
    raw.validate()?;
    if raw.source == source {
        Ok(())
    } else {
        Err(BitgetPublicError::Metadata)
    }
}

fn parse_success_envelope(payload: &str) -> Result<Value, BitgetPublicError> {
    let root = parse_json_object(payload)?;
    if text(object(&root)?, "code")? != "00000" {
        return Err(BitgetPublicError::VenueRejected);
    }
    Ok(root)
}

fn parse_json_object(payload: &str) -> Result<Value, BitgetPublicError> {
    let value = serde_json::from_str(payload).map_err(|_| BitgetPublicError::Payload)?;
    let _ = object(&value)?;
    Ok(value)
}

fn object(value: &Value) -> Result<&Map<String, Value>, BitgetPublicError> {
    value.as_object().ok_or(BitgetPublicError::Payload)
}

fn exact_one_data(root: &Map<String, Value>) -> Result<&Map<String, Value>, BitgetPublicError> {
    let values = root
        .get("data")
        .and_then(Value::as_array)
        .ok_or(BitgetPublicError::Payload)?;
    object(exact_one(values)?)
}

fn exact_one(values: &[Value]) -> Result<&Value, BitgetPublicError> {
    if values.len() == 1 {
        values.first().ok_or(BitgetPublicError::Payload)
    } else {
        Err(BitgetPublicError::Payload)
    }
}

fn require_websocket_argument(
    root: &Map<String, Value>,
    expected_native_symbol: &str,
    topic: &str,
) -> Result<(), BitgetPublicError> {
    let argument = root
        .get("arg")
        .and_then(Value::as_object)
        .ok_or(BitgetPublicError::Payload)?;
    if text(argument, "instType")? != BITGET_UTA_FUTURES_INST_TYPE
        || text(argument, "topic")? != topic
        || text(argument, "symbol")? != expected_native_symbol
    {
        return Err(BitgetPublicError::Symbol);
    }
    Ok(())
}

fn text<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a str, BitgetPublicError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or(BitgetPublicError::Payload)
}

fn sequence(value: Option<&Value>, zero_allowed: bool) -> Result<u64, BitgetPublicError> {
    let parsed = match value {
        Some(Value::String(value)) => value.parse::<u64>().ok(),
        Some(Value::Number(value)) => value.as_u64(),
        _ => None,
    }
    .ok_or(BitgetPublicError::Payload)?;
    if parsed == 0 && !zero_allowed {
        Err(BitgetPublicError::Payload)
    } else {
        Ok(parsed)
    }
}

fn timestamp(value: Option<&Value>) -> Result<u64, BitgetPublicError> {
    sequence(value, false)
}

fn decimal(value: Option<&Value>) -> Result<Decimal, BitgetPublicError> {
    match value {
        Some(Value::String(value)) if !value.is_empty() => {
            Decimal::from_str(value).map_err(|_| BitgetPublicError::Payload)
        }
        Some(Value::Number(value)) => {
            Decimal::from_str(&value.to_string()).map_err(|_| BitgetPublicError::Payload)
        }
        _ => Err(BitgetPublicError::Payload),
    }
}

fn price(value: Option<&Value>) -> Result<Price, BitgetPublicError> {
    Price::new(decimal(value)?).map_err(|_| BitgetPublicError::Payload)
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
pub enum BitgetPublicError {
    #[error("Bitget public payload metadata is invalid")]
    Metadata,
    #[error("Bitget public payload is malformed")]
    Payload,
    #[error("Bitget public payload belongs to another symbol or market")]
    Symbol,
    #[error("Bitget public request was rejected")]
    VenueRejected,
    #[error("Bitget public order-book depth limit is unsupported")]
    DepthLimit,
    #[error("Bitget public order-book sequence is malformed")]
    Sequence,
    #[error("Bitget public trade frame repeats an execution identifier")]
    DuplicateTrade,
    #[error("Bitget public generation is invalid or exhausted")]
    Generation,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symbol() -> Result<Symbol, BitgetPublicError> {
        "DOGE/USDT".parse().map_err(|_| BitgetPublicError::Symbol)
    }

    fn raw(
        source: BitgetPublicSource,
        generation: u64,
        payload: &str,
    ) -> Result<BitgetRawPublicPayload, BitgetPublicError> {
        BitgetRawPublicPayload::new(source, symbol()?, generation, 1_000, payload.to_owned())
    }

    fn snapshot_payload(sequence: u64) -> String {
        format!(
            r#"{{"arg":{{"instType":"usdt-futures","topic":"books","symbol":"DOGEUSDT"}},"action":"snapshot","ts":"1001","data":[{{"a":[["0.101","20"]],"b":[["0.100","10"]],"pseq":"0","seq":"{sequence}","maxdepth":"50","ts":"1000"}}]}}"#
        )
    }

    fn update_payload(previous: u64, sequence: u64) -> String {
        format!(
            r#"{{"arg":{{"instType":"usdt-futures","topic":"books","symbol":"DOGEUSDT"}},"action":"update","ts":"1002","data":[{{"a":[["0.102","20"]],"b":[],"pseq":"{previous}","seq":"{sequence}","maxdepth":"50","ts":"1002"}}]}}"#
        )
    }

    #[test]
    fn rest_snapshot_is_bound_but_cannot_forge_a_sequence() -> Result<(), BitgetPublicError> {
        let path = rest_orderbook_path(&symbol()?, 50)?;
        assert_eq!(
            path,
            "/api/v3/market/orderbook?category=USDT-FUTURES&symbol=DOGEUSDT&limit=50"
        );
        assert_eq!(
            rest_ticker_path(&symbol()?)?,
            "/api/v3/market/tickers?category=USDT-FUTURES&symbol=DOGEUSDT"
        );
        assert_eq!(
            rest_orderbook_path(&symbol()?, 0),
            Err(BitgetPublicError::DepthLimit)
        );
        let parsed = parse_rest_orderbook(raw(
            BitgetPublicSource::RestOrderBook,
            3,
            r#"{"code":"00000","data":{"a":[["0.101","20"]],"b":[["0.100","10"]],"ts":"999"}}"#,
        )?)?;
        assert_eq!(parsed.exchange_time_ms, 999);
        assert_eq!(parsed.best_bid_ask()?.0.value().to_string(), "0.100");
        assert!(parsed.fresh_at(6_000, DEFAULT_PUBLIC_FRESHNESS_MS));
        assert!(!parsed.fresh_at(6_001, DEFAULT_PUBLIC_FRESHNESS_MS));
        Ok(())
    }

    #[test]
    fn scalping_subscription_exposes_only_the_sequenced_book_channel()
    -> Result<(), BitgetPublicError> {
        let subscription = scalping_book_subscription(&symbol()?)?;
        let args = subscription
            .get("args")
            .and_then(Value::as_array)
            .ok_or(BitgetPublicError::Payload)?;
        assert_eq!(args.len(), 1);
        assert_eq!(
            args.first()
                .and_then(Value::as_object)
                .and_then(|arg| arg.get("topic"))
                .and_then(Value::as_str),
            Some("books")
        );
        Ok(())
    }

    #[test]
    fn books_snapshot_bridges_only_when_the_first_update_covers_it() -> Result<(), BitgetPublicError>
    {
        let legacy_spelling = parse_books_message(raw(
            BitgetPublicSource::WebSocketBooks,
            1,
            &snapshot_payload(99).replace("maxdepth", "maxDepth"),
        )?)?;
        assert_eq!(legacy_spelling.maximum_depth, 50);
        let snapshot = parse_books_message(raw(
            BitgetPublicSource::WebSocketBooks,
            1,
            &snapshot_payload(100),
        )?)?;
        let update = parse_books_message(raw(
            BitgetPublicSource::WebSocketBooks,
            1,
            &update_payload(99, 101),
        )?)?;
        let mut sequencer = BitgetBookSequencer::new();
        let snapshot_status = sequencer.accept(&snapshot)?;
        assert_eq!(
            snapshot_status,
            BitgetBookSequenceStatus::Snapshot {
                generation: 1,
                replaced_generation: None,
            }
        );
        assert_eq!(snapshot_status.active_generation(), Some(1));
        assert!(!snapshot_status.ready());
        assert!(snapshot.fresh_at(6_000, DEFAULT_PUBLIC_FRESHNESS_MS));
        assert_eq!(sequencer.next_generation(), 1);
        let bridge_status = sequencer.accept(&update)?;
        assert_eq!(
            bridge_status,
            BitgetBookSequenceStatus::Bridged {
                generation: 1,
                sequence: 101,
            }
        );
        assert_eq!(bridge_status.active_generation(), Some(1));
        assert!(bridge_status.ready());
        assert_eq!(sequencer.ready_generation(), Some(1));
        match update.normalize(1)? {
            MarketEvent::Delta(delta) => {
                assert_eq!(delta.first_sequence, 99);
                assert_eq!(delta.previous_sequence, Some(99));
            }
            _ => return Err(BitgetPublicError::Payload),
        }
        Ok(())
    }

    #[test]
    fn books_gap_duplicate_and_venue_reset_invalidate_the_generation()
    -> Result<(), BitgetPublicError> {
        let snapshot = parse_books_message(raw(
            BitgetPublicSource::WebSocketBooks,
            1,
            &snapshot_payload(100),
        )?)?;
        let bridge = parse_books_message(raw(
            BitgetPublicSource::WebSocketBooks,
            1,
            &update_payload(100, 101),
        )?)?;
        let duplicate = parse_books_message(raw(
            BitgetPublicSource::WebSocketBooks,
            1,
            &update_payload(100, 101),
        )?)?;
        let mut sequencer = BitgetBookSequencer::new();
        let _ = sequencer.accept(&snapshot)?;
        let _ = sequencer.accept(&bridge)?;
        assert_eq!(
            sequencer.accept(&duplicate)?,
            BitgetBookSequenceStatus::ResetRequired {
                generation: 2,
                reason: BitgetBookSequenceFault::PreviousSequenceMismatch,
            }
        );
        let reset = parse_books_message(raw(
            BitgetPublicSource::WebSocketBooks,
            2,
            &update_payload(0, 1),
        )?)?;
        assert_eq!(
            sequencer.accept(&reset)?,
            BitgetBookSequenceStatus::ResetRequired {
                generation: 2,
                reason: BitgetBookSequenceFault::VenueReset,
            }
        );
        Ok(())
    }

    #[test]
    fn recovered_public_generation_drops_the_prior_bridge() -> Result<(), BitgetPublicError> {
        let snapshot = parse_books_message(raw(
            BitgetPublicSource::WebSocketBooks,
            1,
            &snapshot_payload(100),
        )?)?;
        let mut sequencer = BitgetBookSequencer::new();
        let _ = sequencer.accept(&snapshot)?;
        sequencer.reset_generation(7)?;
        assert_eq!(sequencer.next_generation(), 7);
        assert_eq!(sequencer.ready_generation(), None);
        assert!(matches!(
            sequencer.accept(&snapshot)?,
            BitgetBookSequenceStatus::Snapshot { generation: 7, .. }
        ));
        Ok(())
    }

    #[test]
    fn books_reject_wrong_symbol_uncovered_snapshot_and_malformed_levels()
    -> Result<(), BitgetPublicError> {
        let wrong_symbol = raw(
            BitgetPublicSource::WebSocketBooks,
            1,
            &snapshot_payload(100).replace("DOGEUSDT", "BTCUSDT"),
        )?;
        assert_eq!(
            parse_books_message(wrong_symbol),
            Err(BitgetPublicError::Symbol)
        );

        let snapshot = parse_books_message(raw(
            BitgetPublicSource::WebSocketBooks,
            1,
            &snapshot_payload(100),
        )?)?;
        let uncovered = parse_books_message(raw(
            BitgetPublicSource::WebSocketBooks,
            1,
            &update_payload(101, 102),
        )?)?;
        let mut sequencer = BitgetBookSequencer::new();
        let _ = sequencer.accept(&snapshot)?;
        assert_eq!(
            sequencer.accept(&uncovered)?,
            BitgetBookSequenceStatus::ResetRequired {
                generation: 2,
                reason: BitgetBookSequenceFault::SnapshotNotCovered,
            }
        );

        let malformed = raw(
            BitgetPublicSource::WebSocketBooks,
            1,
            &snapshot_payload(100).replace("[\"0.101\",\"20\"]", "[\"0.101\",\"-1\"]"),
        )?;
        assert_eq!(
            parse_books_message(malformed),
            Err(BitgetPublicError::Payload)
        );
        Ok(())
    }

    #[test]
    fn ticker_requires_exact_futures_binding_and_exposes_bbo_and_mark()
    -> Result<(), BitgetPublicError> {
        let ticker = parse_rest_ticker(raw(
            BitgetPublicSource::RestTicker,
            4,
            r#"{"code":"00000","data":[{"category":"USDT-FUTURES","symbol":"DOGEUSDT","lastPrice":"0.1005","ask1Price":"0.101","bid1Price":"0.100","bid1Size":"10","ask1Size":"20","indexPrice":"0.1007","markPrice":"0.1006","fundingRate":"0.0001","ts":"999"}]}"#,
        )?)?;
        assert_eq!(ticker.bbo.bid_price.value().to_string(), "0.100");
        assert_eq!(ticker.mark.mark_price.value().to_string(), "0.1006");
        assert!(matches!(ticker.market_event(), MarketEvent::Ticker(_)));
        assert!(ticker.fresh_at(6_000, DEFAULT_PUBLIC_FRESHNESS_MS));
        assert!(!ticker.fresh_at(1_000, 0));
        Ok(())
    }

    #[test]
    fn public_trade_has_event_time_and_rejects_duplicates_inside_a_frame()
    -> Result<(), BitgetPublicError> {
        let payload = r#"{"arg":{"instType":"usdt-futures","topic":"publicTrade","symbol":"DOGEUSDT"},"action":"snapshot","ts":"1001","data":[{"p":"0.1","S":"buy","T":"999","v":"50","i":"123","L":"122","isRPI":"no"}]}"#;
        let trades =
            parse_public_trade_message(raw(BitgetPublicSource::WebSocketPublicTrade, 9, payload)?)?;
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].trade.exchange_time_ms, 999);
        assert_eq!(trades[0].trade.quote_quantity.to_string(), "5.0");
        assert_eq!(trades[0].rpi, FieldState::Known(false));
        assert!(matches!(trades[0].market_event(), MarketEvent::Trade(_)));
        assert!(trades[0].fresh_at(6_000, DEFAULT_PUBLIC_FRESHNESS_MS));

        let duplicate = payload.replace(
            "]}",
            ",{\"p\":\"0.1\",\"S\":\"sell\",\"T\":\"1000\",\"v\":\"50\",\"i\":\"123\",\"L\":\"123\"}]}"
        );
        assert_eq!(
            parse_public_trade_message(raw(
                BitgetPublicSource::WebSocketPublicTrade,
                9,
                &duplicate,
            )?),
            Err(BitgetPublicError::DuplicateTrade)
        );
        Ok(())
    }

    #[test]
    fn raw_metadata_hash_and_subscription_are_deterministic() -> Result<(), BitgetPublicError> {
        let payload = raw(BitgetPublicSource::RestTicker, 1, "{}")?;
        assert!(payload.validate().is_ok());
        let mut tampered = payload.clone();
        tampered.payload.push(' ');
        assert_eq!(tampered.validate(), Err(BitgetPublicError::Metadata));
        assert_eq!(
            public_subscriptions(&symbol()?)?,
            json!({
                "op": "subscribe",
                "args": [
                    {"instType":"usdt-futures","topic":"books","symbol":"DOGEUSDT"},
                    {"instType":"usdt-futures","topic":"publicTrade","symbol":"DOGEUSDT"},
                ],
            })
        );
        Ok(())
    }
}
