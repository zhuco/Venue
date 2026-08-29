//! Pure Gate.io USDT futures public-market protocol handling.
//!
//! This module deliberately has no socket, HTTP client, recorder, or runtime dependency.  A
//! caller supplies raw payload metadata after receiving a response or WebSocket frame, persists
//! it through the owning market recorder, then calls these deterministic parsers.  Keeping the
//! bridge here makes a depth gap unambiguous before the data can reach a strategy.

use std::{
    collections::{BTreeSet, VecDeque},
    str::FromStr,
};

use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use venue_domain::domain::{
    AggressorSide, FieldState, MarketDelta, MarketLevel, MarketSnapshot, Price, PublicTicker,
    PublicTrade, Symbol,
};

/// Parser contract revision for the documented Gate futures payload shapes accepted here.
pub const GATE_PUBLIC_PARSER_SCHEMA_VERSION: u16 = 1;

const CHANNEL_BOOK_DELTA: &str = "futures.order_book_update";
const CHANNEL_BOOK_TICKER: &str = "futures.book_ticker";
const CHANNEL_TICKERS: &str = "futures.tickers";
const CHANNEL_TRADES: &str = "futures.trades";
const MAX_ORDER_BOOK_DEPTH: u16 = 100;

/// The exact canonical/native and quantity-unit relationship chosen for one public stream.
///
/// Gate reports futures book and trade sizes in contracts. `base_quantity_per_contract` is taken
/// from the independently fetched, versioned contract rule, so normalized domain quantities are
/// never mistaken for raw contract counts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GatePublicBinding {
    pub symbol: Symbol,
    pub native_symbol: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub base_quantity_per_contract: Decimal,
}

impl GatePublicBinding {
    pub fn new(
        symbol: Symbol,
        native_symbol: impl Into<String>,
        base_quantity_per_contract: Decimal,
    ) -> Result<Self, GatePublicError> {
        let native_symbol = native_symbol.into();
        if native_symbol != native_symbol_for(&symbol)
            || base_quantity_per_contract <= Decimal::ZERO
        {
            return Err(GatePublicError::Binding);
        }
        Ok(Self {
            symbol,
            native_symbol,
            base_quantity_per_contract,
        })
    }

    fn contracts_to_base(&self, contracts: Decimal) -> Result<Decimal, GatePublicError> {
        contracts
            .checked_mul(self.base_quantity_per_contract)
            .ok_or(GatePublicError::Number)
    }
}

/// Gate's USDT futures native contract spelling for the canonical symbol.
#[must_use]
pub fn native_symbol_for(symbol: &Symbol) -> String {
    format!("{}_{}", symbol.base(), symbol.quote())
}

/// Builds the one REST snapshot request that carries Gate's update ID. The caller must use the
/// same `level` in the matching WS subscription; Gate documents mismatched depth as unsafe for
/// incremental reconstruction.
pub fn rest_order_book_path(
    binding: &GatePublicBinding,
    level: u16,
) -> Result<String, GatePublicError> {
    validate_depth(level)?;
    Ok(format!(
        "/futures/usdt/order_book?contract={}&interval=0&limit={level}&with_id=true",
        binding.native_symbol
    ))
}

/// Builds the documented public subscriptions for a single normalized binding. No I/O occurs
/// here; a transport must persist each raw response before invoking the corresponding parser.
#[allow(dead_code)]
pub fn public_subscriptions(
    binding: &GatePublicBinding,
    frequency_ms: u16,
    level: u16,
) -> Result<Value, GatePublicError> {
    validate_depth(level)?;
    if !matches!((frequency_ms, level), (20, 20) | (100, 20 | 50 | 100)) {
        return Err(GatePublicError::Subscription);
    }
    let frequency = format!("{frequency_ms}ms");
    Ok(json!([
        {
            "channel": CHANNEL_BOOK_DELTA,
            "event": "subscribe",
            "payload": [binding.native_symbol, frequency, level.to_string()],
        },
        {
            "channel": CHANNEL_BOOK_TICKER,
            "event": "subscribe",
            "payload": [binding.native_symbol],
        },
        {
            "channel": CHANNEL_TICKERS,
            "event": "subscribe",
            "payload": [binding.native_symbol],
        },
        {
            "channel": CHANNEL_TRADES,
            "event": "subscribe",
            "payload": [binding.native_symbol],
        }
    ]))
}

/// The hedged-grid contract consumes only a sequenced order book. Keeping its transport scope to
/// the depth channel prevents unrelated trade/ticker bursts from delaying the exact event-time
/// book that authorizes mutations; richer market-data consumers keep using `public_subscriptions`.
pub fn grid_public_subscriptions(
    binding: &GatePublicBinding,
    frequency_ms: u16,
    level: u16,
) -> Result<Value, GatePublicError> {
    validate_depth(level)?;
    if !matches!((frequency_ms, level), (20, 20) | (100, 20 | 50 | 100)) {
        return Err(GatePublicError::Subscription);
    }
    Ok(json!([{
        "channel": CHANNEL_BOOK_DELTA,
        "event": "subscribe",
        "payload": [
            binding.native_symbol,
            format!("{frequency_ms}ms"),
            level.to_string()
        ],
    }]))
}

/// The source carried by a raw public message. It has no persistence side effect by itself.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatePublicPayloadKind {
    RestOrderBookSnapshot,
    WebSocketOrderBookDelta,
    WebSocketBookTicker,
    WebSocketTicker,
    WebSocketTrade,
}

/// Raw-payload identity and timing supplied by a public capture worker.
///
/// This is intentionally only a value object: the caller owns durable recording and must append
/// this payload before exposing a parsed event. The content hash makes that later recording
/// independently auditable without coupling the adapter to a runtime writer.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GatePublicRawPayload {
    pub parser_schema_version: u16,
    pub kind: GatePublicPayloadKind,
    pub symbol: Symbol,
    pub native_symbol: String,
    pub generation: u64,
    pub received_at_ms: u64,
    pub payload_sha256: String,
    pub payload: String,
}

impl GatePublicRawPayload {
    pub fn new(
        binding: &GatePublicBinding,
        kind: GatePublicPayloadKind,
        generation: u64,
        received_at_ms: u64,
        payload: String,
    ) -> Result<Self, GatePublicError> {
        if generation == 0 || received_at_ms == 0 || payload.is_empty() {
            return Err(GatePublicError::Metadata);
        }
        Ok(Self {
            parser_schema_version: GATE_PUBLIC_PARSER_SCHEMA_VERSION,
            kind,
            symbol: binding.symbol.clone(),
            native_symbol: binding.native_symbol.clone(),
            generation,
            received_at_ms,
            payload_sha256: digest(&payload),
            payload,
        })
    }

    pub fn verify(&self, binding: &GatePublicBinding, kind: GatePublicPayloadKind) -> bool {
        self.parser_schema_version == GATE_PUBLIC_PARSER_SCHEMA_VERSION
            && self.kind == kind
            && self.symbol == binding.symbol
            && self.native_symbol == binding.native_symbol
            && self.generation != 0
            && self.received_at_ms != 0
            && !self.payload.is_empty()
            && self.payload_sha256 == digest(&self.payload)
    }
}

/// Receive and venue time attached to every parsed public event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GateFreshness {
    pub received_at_ms: u64,
    pub exchange_time_ms: u64,
}

#[cfg(test)]
impl GateFreshness {
    pub fn is_fresh_at(self, now_ms: u64, maximum_age_ms: u64) -> Result<bool, GatePublicError> {
        if maximum_age_ms == 0 || now_ms < self.received_at_ms {
            return Err(GatePublicError::Freshness);
        }
        Ok(now_ms - self.received_at_ms <= maximum_age_ms)
    }
}

/// A normalized payload always remains attached to the exact raw metadata it came from.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatePublicRecord<T> {
    pub raw: GatePublicRawPayload,
    pub freshness: GateFreshness,
    pub value: T,
}

#[cfg(test)]
impl GatePublicRecord<MarketSnapshot> {
    pub fn market_event(&self) -> venue_domain::domain::MarketEvent {
        venue_domain::domain::MarketEvent::Snapshot(self.value.clone())
    }
}

#[cfg(test)]
impl GatePublicRecord<MarketDelta> {
    pub fn market_event(&self) -> venue_domain::domain::MarketEvent {
        venue_domain::domain::MarketEvent::Delta(self.value.clone())
    }
}

#[cfg(test)]
impl GatePublicRecord<PublicTicker> {
    pub fn market_event(&self) -> venue_domain::domain::MarketEvent {
        venue_domain::domain::MarketEvent::Ticker(self.value.clone())
    }
}

#[cfg(test)]
impl GatePublicRecord<PublicTrade> {
    pub fn market_event(&self) -> venue_domain::domain::MarketEvent {
        venue_domain::domain::MarketEvent::Trade(self.value.clone())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateBookDelta {
    pub delta: MarketDelta,
    /// Gate may send a complete depth replacement on the incremental channel.
    pub full: bool,
}

/// Gate ticker data that has enough fields to prove a fresh mark/index observation. It remains
/// separate from `MarkFunding`, because this channel does not include a next-funding timestamp.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateMarkPrice {
    pub symbol: Symbol,
    pub generation: u64,
    pub received_at_ms: u64,
    pub exchange_time_ms: u64,
    pub mark_price: Price,
    pub index_price: Price,
    #[allow(clippy::struct_field_names)]
    pub funding_rate: Decimal,
    pub price_type: GateTickerPriceType,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GateTickerPriceType {
    Last,
    Mark,
    Index,
}

/// Parses the documented `GET /futures/usdt/order_book?...&with_id=true` response.
///
/// That response has no contract field. Its native symbol is therefore bound to the exact
/// requested contract in `GatePublicRawPayload`; a response carrying a contract field is checked
/// too, rather than silently accepting a mismatched fixture or proxy response.
pub fn parse_rest_snapshot(
    binding: &GatePublicBinding,
    raw: GatePublicRawPayload,
) -> Result<GatePublicRecord<MarketSnapshot>, GatePublicError> {
    verify_raw(binding, &raw, GatePublicPayloadKind::RestOrderBookSnapshot)?;
    let value = parse_json(&raw.payload)?;
    let object = value.as_object().ok_or(GatePublicError::Payload)?;
    optional_native_symbol(object, binding)?;
    let exchange_time_ms = seconds_to_ms(required_decimal(object, "current")?)?;
    let sequence = required_u64(object, "id")?;
    let bids = parse_levels(binding, required_value(object, "bids")?, false)?;
    let asks = parse_levels(binding, required_value(object, "asks")?, false)?;
    validate_snapshot_levels(&bids, &asks)?;
    Ok(GatePublicRecord {
        freshness: GateFreshness {
            received_at_ms: raw.received_at_ms,
            exchange_time_ms,
        },
        value: MarketSnapshot {
            symbol: binding.symbol.clone(),
            generation: raw.generation,
            sequence,
            exchange_time_ms: Some(exchange_time_ms),
            bids,
            asks,
        },
        raw,
    })
}

/// Parses the `futures.order_book_update` WebSocket notification. The bridge below decides
/// whether this normalized delta is stale, is the snapshot bridge, or proves a sequence gap.
pub fn parse_ws_delta(
    binding: &GatePublicBinding,
    raw: GatePublicRawPayload,
) -> Result<GatePublicRecord<GateBookDelta>, GatePublicError> {
    verify_raw(
        binding,
        &raw,
        GatePublicPayloadKind::WebSocketOrderBookDelta,
    )?;
    let value = parse_json(&raw.payload)?;
    let object = websocket_result(&value, CHANNEL_BOOK_DELTA)?;
    require_native_symbol(object, binding)?;
    let first_sequence = required_u64(object, "U")?;
    let sequence = required_u64(object, "u")?;
    if sequence < first_sequence {
        return Err(GatePublicError::Sequence);
    }
    let exchange_time_ms = required_u64(object, "t")?;
    let bids = parse_levels(binding, required_value(object, "b")?, true)?;
    let asks = parse_levels(binding, required_value(object, "a")?, true)?;
    let full = match object.get("full") {
        None => false,
        Some(Value::Bool(value)) => *value,
        Some(_) => return Err(GatePublicError::Payload),
    };
    if full {
        validate_snapshot_levels(&bids, &asks)?;
    }
    Ok(GatePublicRecord {
        freshness: GateFreshness {
            received_at_ms: raw.received_at_ms,
            exchange_time_ms,
        },
        value: GateBookDelta {
            delta: MarketDelta {
                symbol: binding.symbol.clone(),
                generation: raw.generation,
                first_sequence,
                previous_sequence: None,
                sequence,
                exchange_time_ms: Some(exchange_time_ms),
                bids,
                asks,
            },
            full,
        },
        raw,
    })
}

/// Parses a fresh best-bid/ask message from `futures.book_ticker`.
pub fn parse_ws_book_ticker(
    binding: &GatePublicBinding,
    raw: GatePublicRawPayload,
) -> Result<GatePublicRecord<PublicTicker>, GatePublicError> {
    verify_raw(binding, &raw, GatePublicPayloadKind::WebSocketBookTicker)?;
    let value = parse_json(&raw.payload)?;
    let object = websocket_result(&value, CHANNEL_BOOK_TICKER)?;
    require_native_symbol(object, binding)?;
    let exchange_time_ms = required_u64(object, "t")?;
    let update_id = required_u64(object, "u")?;
    let bid_price = required_price(object, "b")?;
    let ask_price = required_price(object, "a")?;
    let bid_contracts = required_nonnegative_decimal(object, "B")?;
    let ask_contracts = required_nonnegative_decimal(object, "A")?;
    if bid_price >= ask_price || bid_contracts.is_zero() || ask_contracts.is_zero() {
        return Err(GatePublicError::Payload);
    }
    Ok(GatePublicRecord {
        freshness: GateFreshness {
            received_at_ms: raw.received_at_ms,
            exchange_time_ms,
        },
        value: PublicTicker {
            symbol: binding.symbol.clone(),
            generation: raw.generation,
            received_at_ms: raw.received_at_ms,
            exchange_time_ms,
            transaction_time_ms: exchange_time_ms,
            update_id,
            bid_price,
            bid_quantity: binding.contracts_to_base(bid_contracts)?,
            ask_price,
            ask_quantity: binding.contracts_to_base(ask_contracts)?,
        },
        raw,
    })
}

/// Parses the selected-contract `futures.tickers` mark/index observation.
pub fn parse_ws_mark_price(
    binding: &GatePublicBinding,
    raw: GatePublicRawPayload,
) -> Result<GatePublicRecord<GateMarkPrice>, GatePublicError> {
    verify_raw(binding, &raw, GatePublicPayloadKind::WebSocketTicker)?;
    let value = parse_json(&raw.payload)?;
    let result = websocket_result_value(&value, CHANNEL_TICKERS)?
        .as_array()
        .ok_or(GatePublicError::Payload)?;
    if result.len() != 1 {
        return Err(GatePublicError::Payload);
    }
    let object = result
        .first()
        .and_then(Value::as_object)
        .ok_or(GatePublicError::Payload)?;
    require_native_contract(object, "contract", binding)?;
    let exchange_time_ms = required_u64(object, "t")?;
    let price_type = match required_string(object, "price_type")? {
        "last" => GateTickerPriceType::Last,
        "mark" => GateTickerPriceType::Mark,
        "index" => GateTickerPriceType::Index,
        _ => return Err(GatePublicError::Payload),
    };
    Ok(GatePublicRecord {
        freshness: GateFreshness {
            received_at_ms: raw.received_at_ms,
            exchange_time_ms,
        },
        value: GateMarkPrice {
            symbol: binding.symbol.clone(),
            generation: raw.generation,
            received_at_ms: raw.received_at_ms,
            exchange_time_ms,
            mark_price: required_price(object, "mark_price")?,
            index_price: required_price(object, "index_price")?,
            funding_rate: required_decimal(object, "funding_rate")?,
            price_type,
        },
        raw,
    })
}

/// Parses an ordered batch of fresh public trades from `futures.trades`.
///
/// A duplicate or decreasing trade ID in one WebSocket notification is rejected; allowing it
/// would erase an otherwise explicit replay/order invariant. The caller can independently retain
/// cross-message IDs if it needs a longer deduplication window.
pub fn parse_ws_trades(
    binding: &GatePublicBinding,
    raw: GatePublicRawPayload,
) -> Result<Vec<GatePublicRecord<PublicTrade>>, GatePublicError> {
    verify_raw(binding, &raw, GatePublicPayloadKind::WebSocketTrade)?;
    let value = parse_json(&raw.payload)?;
    let envelope = websocket_envelope(&value, CHANNEL_TRADES)?;
    let exchange_time_ms = required_u64(envelope, "time_ms")?;
    let results = required_value(envelope, "result")?
        .as_array()
        .ok_or(GatePublicError::Payload)?;
    if results.is_empty() {
        return Err(GatePublicError::Payload);
    }
    let mut previous_id = None;
    let mut records = Vec::with_capacity(results.len());
    for result in results {
        let object = result.as_object().ok_or(GatePublicError::Payload)?;
        require_native_contract(object, "contract", binding)?;
        let trade_id = required_u64(object, "id")?;
        if previous_id.is_some_and(|previous| trade_id <= previous) {
            return Err(GatePublicError::TradeOrder);
        }
        previous_id = Some(trade_id);
        let signed_contracts = required_decimal(object, "size")?;
        if signed_contracts.is_zero() {
            return Err(GatePublicError::Payload);
        }
        let aggressor = if signed_contracts.is_sign_positive() {
            AggressorSide::Buy
        } else {
            AggressorSide::Sell
        };
        let quantity = binding.contracts_to_base(signed_contracts.abs())?;
        let price = required_price(object, "price")?;
        let transaction_time_ms = trade_time_ms(object)?;
        records.push(GatePublicRecord {
            raw: raw.clone(),
            freshness: GateFreshness {
                received_at_ms: raw.received_at_ms,
                exchange_time_ms,
            },
            value: PublicTrade {
                symbol: binding.symbol.clone(),
                generation: raw.generation,
                received_at_ms: raw.received_at_ms,
                exchange_time_ms,
                transaction_time_ms,
                aggregate_trade_id: trade_id,
                first_trade_id: trade_id,
                last_trade_id: trade_id,
                price,
                quantity,
                quote_quantity: price
                    .value()
                    .checked_mul(quantity)
                    .ok_or(GatePublicError::Number)?,
                aggressor: FieldState::Known(aggressor),
            },
        });
    }
    Ok(records)
}

/// Holds only a bounded, one-generation order-book bridge. It never opens a connection and does
/// not mutate a recorder. Every gap invalidates the generation and makes the caller fetch a new
/// REST snapshot before publishing further depth.
#[derive(Clone, Debug)]
pub struct GateOrderBookBridge {
    binding: GatePublicBinding,
    generation: u64,
    maximum_buffered_deltas: usize,
    state: GateBookBridgeState,
}

#[derive(Clone, Debug)]
enum GateBookBridgeState {
    AwaitingSnapshot {
        buffered: VecDeque<GatePublicRecord<GateBookDelta>>,
    },
    AwaitingBridge {
        snapshot: GatePublicRecord<MarketSnapshot>,
    },
    Ready {
        sequence: u64,
    },
    Invalid,
}

/// The caller must apply these actions atomically in the order returned. `ReplaceSnapshot` alone
/// is not readiness: only a bridge delta, or a documented `full=true` replacement, is ready.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GateBookBridgeAction {
    Buffered,
    IgnoredStale,
    ReplaceSnapshot(GatePublicRecord<MarketSnapshot>),
    ApplyDelta(GatePublicRecord<MarketDelta>),
}

impl GateOrderBookBridge {
    pub fn new(
        binding: GatePublicBinding,
        generation: u64,
        maximum_buffered_deltas: usize,
    ) -> Result<Self, GatePublicError> {
        if generation == 0 || maximum_buffered_deltas == 0 {
            return Err(GatePublicError::Bridge);
        }
        Ok(Self {
            binding,
            generation,
            maximum_buffered_deltas,
            state: GateBookBridgeState::AwaitingSnapshot {
                buffered: VecDeque::new(),
            },
        })
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        matches!(self.state, GateBookBridgeState::Ready { .. })
    }

    /// Drops an old generation and all unbridged deltas. Reusing a depth update across a reconnect
    /// is forbidden even when its numeric sequence looks plausible.
    pub fn reset_generation(&mut self, generation: u64) -> Result<(), GatePublicError> {
        if generation == 0 || generation <= self.generation {
            return Err(GatePublicError::Generation);
        }
        self.generation = generation;
        self.state = GateBookBridgeState::AwaitingSnapshot {
            buffered: VecDeque::new(),
        };
        Ok(())
    }

    pub fn receive_delta(
        &mut self,
        delta: GatePublicRecord<GateBookDelta>,
    ) -> Result<Vec<GateBookBridgeAction>, GatePublicError> {
        self.validate_delta(&delta)?;
        if delta.value.full {
            if matches!(self.state, GateBookBridgeState::Invalid) {
                return Err(GatePublicError::BridgeInvalid);
            }
            let snapshot = snapshot_from_full_delta(delta)?;
            self.state = GateBookBridgeState::Ready {
                sequence: snapshot.value.sequence,
            };
            return Ok(vec![GateBookBridgeAction::ReplaceSnapshot(snapshot)]);
        }
        let state = std::mem::replace(&mut self.state, GateBookBridgeState::Invalid);
        match state {
            GateBookBridgeState::AwaitingSnapshot { mut buffered } => {
                if buffered.len() == self.maximum_buffered_deltas {
                    return Err(self.invalidate(GatePublicError::BufferFull));
                }
                buffered.push_back(delta);
                self.state = GateBookBridgeState::AwaitingSnapshot { buffered };
                Ok(vec![GateBookBridgeAction::Buffered])
            }
            GateBookBridgeState::AwaitingBridge { snapshot } => {
                let expected = next_sequence(snapshot.value.sequence)?;
                if delta.value.delta.sequence < expected {
                    self.state = GateBookBridgeState::AwaitingBridge { snapshot };
                    return Ok(vec![GateBookBridgeAction::IgnoredStale]);
                }
                if !covers(
                    delta.value.delta.first_sequence,
                    delta.value.delta.sequence,
                    expected,
                ) {
                    return Err(self.invalidate(GatePublicError::SnapshotBehind));
                }
                let normalized = take_delta(delta);
                self.state = GateBookBridgeState::Ready {
                    sequence: normalized.value.sequence,
                };
                Ok(vec![GateBookBridgeAction::ApplyDelta(normalized)])
            }
            GateBookBridgeState::Ready { sequence } => {
                let expected = next_sequence(sequence)?;
                if delta.value.delta.sequence < expected {
                    self.state = GateBookBridgeState::Ready { sequence };
                    return Ok(vec![GateBookBridgeAction::IgnoredStale]);
                }
                if delta.value.delta.first_sequence != expected {
                    return Err(self.invalidate(GatePublicError::SequenceGap));
                }
                let normalized = take_delta(delta);
                self.state = GateBookBridgeState::Ready {
                    sequence: normalized.value.sequence,
                };
                Ok(vec![GateBookBridgeAction::ApplyDelta(normalized)])
            }
            GateBookBridgeState::Invalid => Err(GatePublicError::BridgeInvalid),
        }
    }

    pub fn receive_snapshot(
        &mut self,
        snapshot: GatePublicRecord<MarketSnapshot>,
    ) -> Result<Vec<GateBookBridgeAction>, GatePublicError> {
        self.validate_snapshot(&snapshot)?;
        let state = std::mem::replace(&mut self.state, GateBookBridgeState::Invalid);
        let GateBookBridgeState::AwaitingSnapshot { mut buffered } = state else {
            return Err(GatePublicError::Bridge);
        };
        let snapshot_sequence = snapshot.value.sequence;
        let expected = next_sequence(snapshot_sequence)?;
        while buffered
            .front()
            .is_some_and(|delta| delta.value.delta.sequence < expected)
        {
            let _ = buffered.pop_front();
        }
        let mut actions = vec![GateBookBridgeAction::ReplaceSnapshot(snapshot.clone())];
        let Some(first) = buffered.pop_front() else {
            self.state = GateBookBridgeState::AwaitingBridge { snapshot };
            return Ok(actions);
        };
        if !covers(
            first.value.delta.first_sequence,
            first.value.delta.sequence,
            expected,
        ) {
            return Err(self.invalidate(GatePublicError::SnapshotBehind));
        }
        let first = take_delta(first);
        let mut sequence = first.value.sequence;
        actions.push(GateBookBridgeAction::ApplyDelta(first));
        while let Some(next) = buffered.pop_front() {
            let expected = next_sequence(sequence)?;
            if next.value.delta.sequence < expected {
                continue;
            }
            if next.value.delta.first_sequence != expected {
                return Err(self.invalidate(GatePublicError::SequenceGap));
            }
            let next = take_delta(next);
            sequence = next.value.sequence;
            actions.push(GateBookBridgeAction::ApplyDelta(next));
        }
        self.state = GateBookBridgeState::Ready { sequence };
        Ok(actions)
    }

    fn validate_snapshot(
        &self,
        snapshot: &GatePublicRecord<MarketSnapshot>,
    ) -> Result<(), GatePublicError> {
        if !snapshot
            .raw
            .verify(&self.binding, GatePublicPayloadKind::RestOrderBookSnapshot)
            || snapshot.value.symbol != self.binding.symbol
            || snapshot.value.generation != self.generation
            || snapshot.value.sequence == 0
        {
            return Err(GatePublicError::Generation);
        }
        Ok(())
    }

    fn validate_delta(
        &self,
        delta: &GatePublicRecord<GateBookDelta>,
    ) -> Result<(), GatePublicError> {
        if !delta.raw.verify(
            &self.binding,
            GatePublicPayloadKind::WebSocketOrderBookDelta,
        ) || delta.value.delta.symbol != self.binding.symbol
            || delta.value.delta.generation != self.generation
        {
            return Err(GatePublicError::Generation);
        }
        Ok(())
    }

    fn invalidate(&mut self, error: GatePublicError) -> GatePublicError {
        self.state = GateBookBridgeState::Invalid;
        error
    }
}

fn take_delta(record: GatePublicRecord<GateBookDelta>) -> GatePublicRecord<MarketDelta> {
    GatePublicRecord {
        raw: record.raw,
        freshness: record.freshness,
        value: record.value.delta,
    }
}

fn snapshot_from_full_delta(
    record: GatePublicRecord<GateBookDelta>,
) -> Result<GatePublicRecord<MarketSnapshot>, GatePublicError> {
    if !record.value.full {
        return Err(GatePublicError::Bridge);
    }
    let delta = record.value.delta;
    validate_snapshot_levels(&delta.bids, &delta.asks)?;
    Ok(GatePublicRecord {
        raw: record.raw,
        freshness: record.freshness,
        value: MarketSnapshot {
            symbol: delta.symbol,
            generation: delta.generation,
            sequence: delta.sequence,
            exchange_time_ms: delta.exchange_time_ms,
            bids: delta.bids,
            asks: delta.asks,
        },
    })
}

fn verify_raw(
    binding: &GatePublicBinding,
    raw: &GatePublicRawPayload,
    kind: GatePublicPayloadKind,
) -> Result<(), GatePublicError> {
    raw.verify(binding, kind)
        .then_some(())
        .ok_or(GatePublicError::Metadata)
}

fn parse_json(payload: &str) -> Result<Value, GatePublicError> {
    serde_json::from_str(payload).map_err(|_| GatePublicError::Payload)
}

fn websocket_result<'a>(
    value: &'a Value,
    channel: &str,
) -> Result<&'a Map<String, Value>, GatePublicError> {
    websocket_result_value(value, channel)?
        .as_object()
        .ok_or(GatePublicError::Payload)
}

fn websocket_result_value<'a>(
    value: &'a Value,
    channel: &str,
) -> Result<&'a Value, GatePublicError> {
    let envelope = websocket_envelope(value, channel)?;
    required_value(envelope, "result")
}

fn websocket_envelope<'a>(
    value: &'a Value,
    channel: &str,
) -> Result<&'a Map<String, Value>, GatePublicError> {
    let object = value.as_object().ok_or(GatePublicError::Payload)?;
    if object.get("channel").and_then(Value::as_str) != Some(channel)
        || object.get("event").and_then(Value::as_str) != Some("update")
        || !matches!(object.get("error"), None | Some(Value::Null))
    {
        return Err(GatePublicError::Payload);
    }
    Ok(object)
}

fn optional_native_symbol(
    object: &Map<String, Value>,
    binding: &GatePublicBinding,
) -> Result<(), GatePublicError> {
    match object.get("contract").or_else(|| object.get("s")) {
        None => Ok(()),
        Some(value) => {
            if value.as_str() == Some(binding.native_symbol.as_str()) {
                Ok(())
            } else {
                Err(GatePublicError::Symbol)
            }
        }
    }
}

fn require_native_symbol(
    object: &Map<String, Value>,
    binding: &GatePublicBinding,
) -> Result<(), GatePublicError> {
    require_native_contract(object, "s", binding)
}

fn require_native_contract(
    object: &Map<String, Value>,
    field: &str,
    binding: &GatePublicBinding,
) -> Result<(), GatePublicError> {
    if object.get(field).and_then(Value::as_str) == Some(binding.native_symbol.as_str()) {
        Ok(())
    } else {
        Err(GatePublicError::Symbol)
    }
}

fn required_value<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a Value, GatePublicError> {
    object.get(field).ok_or(GatePublicError::Payload)
}

fn required_string<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a str, GatePublicError> {
    required_value(object, field)?
        .as_str()
        .ok_or(GatePublicError::Payload)
}

fn required_decimal(object: &Map<String, Value>, field: &str) -> Result<Decimal, GatePublicError> {
    decimal(required_value(object, field)?)
}

fn required_nonnegative_decimal(
    object: &Map<String, Value>,
    field: &str,
) -> Result<Decimal, GatePublicError> {
    let value = required_decimal(object, field)?;
    if value.is_sign_negative() {
        return Err(GatePublicError::Number);
    }
    Ok(value)
}

fn required_u64(object: &Map<String, Value>, field: &str) -> Result<u64, GatePublicError> {
    u64_value(required_value(object, field)?)
}

fn required_price(object: &Map<String, Value>, field: &str) -> Result<Price, GatePublicError> {
    let value = required_decimal(object, field)?;
    Price::new(value).map_err(|_| GatePublicError::Number)
}

fn decimal(value: &Value) -> Result<Decimal, GatePublicError> {
    match value {
        Value::String(value) => Decimal::from_str(value).map_err(|_| GatePublicError::Number),
        Value::Number(value) => {
            Decimal::from_str(&value.to_string()).map_err(|_| GatePublicError::Number)
        }
        _ => Err(GatePublicError::Payload),
    }
}

fn u64_value(value: &Value) -> Result<u64, GatePublicError> {
    let value = decimal(value)?;
    if value <= Decimal::ZERO || value.fract() != Decimal::ZERO {
        return Err(GatePublicError::Number);
    }
    value.to_u64().ok_or(GatePublicError::Number)
}

fn seconds_to_ms(value: Decimal) -> Result<u64, GatePublicError> {
    if value <= Decimal::ZERO {
        return Err(GatePublicError::Number);
    }
    let milliseconds = value
        .checked_mul(Decimal::from(1_000_u16))
        .ok_or(GatePublicError::Number)?;
    if milliseconds.fract() != Decimal::ZERO {
        return Err(GatePublicError::Number);
    }
    milliseconds.to_u64().ok_or(GatePublicError::Number)
}

fn trade_time_ms(object: &Map<String, Value>) -> Result<u64, GatePublicError> {
    let milliseconds = object.get("create_time_ms").map(u64_value).transpose()?;
    let seconds = object.get("create_time").map(decimal).transpose()?;
    match (milliseconds, seconds) {
        (Some(milliseconds), Some(seconds)) if !same_trade_time(seconds, milliseconds)? => {
            Err(GatePublicError::Payload)
        }
        (Some(milliseconds), _) => Ok(milliseconds),
        (_, Some(seconds)) => seconds_to_ms(seconds),
        (None, None) => Err(GatePublicError::Payload),
    }
}

fn same_trade_time(seconds: Decimal, milliseconds: u64) -> Result<bool, GatePublicError> {
    if seconds <= Decimal::ZERO {
        return Err(GatePublicError::Number);
    }
    if seconds.fract().is_zero() {
        return Ok(milliseconds / 1_000 == seconds.to_u64().ok_or(GatePublicError::Number)?);
    }
    Ok(seconds_to_ms(seconds)? == milliseconds)
}

fn parse_levels(
    binding: &GatePublicBinding,
    value: &Value,
    allow_zero: bool,
) -> Result<Vec<MarketLevel>, GatePublicError> {
    let values = value.as_array().ok_or(GatePublicError::Payload)?;
    let mut prices = BTreeSet::new();
    let mut levels = Vec::with_capacity(values.len());
    for value in values {
        let object = value.as_object().ok_or(GatePublicError::Payload)?;
        let price = required_price(object, "p")?;
        if !prices.insert(price) {
            return Err(GatePublicError::DuplicateLevel);
        }
        let contracts = required_nonnegative_decimal(object, "s")?;
        if contracts.is_zero() && !allow_zero {
            return Err(GatePublicError::Payload);
        }
        levels.push(MarketLevel {
            price,
            quantity: binding.contracts_to_base(contracts)?,
        });
    }
    Ok(levels)
}

fn validate_snapshot_levels(
    bids: &[MarketLevel],
    asks: &[MarketLevel],
) -> Result<(), GatePublicError> {
    let best_bid = bids.first().ok_or(GatePublicError::Payload)?.price;
    let best_ask = asks.first().ok_or(GatePublicError::Payload)?.price;
    if best_bid >= best_ask {
        return Err(GatePublicError::Payload);
    }
    Ok(())
}

fn next_sequence(sequence: u64) -> Result<u64, GatePublicError> {
    sequence.checked_add(1).ok_or(GatePublicError::Sequence)
}

fn covers(first: u64, last: u64, expected: u64) -> bool {
    first <= expected && expected <= last
}

fn validate_depth(level: u16) -> Result<(), GatePublicError> {
    if matches!(level, 20 | 50 | 100) && level <= MAX_ORDER_BOOK_DEPTH {
        Ok(())
    } else {
        Err(GatePublicError::Depth)
    }
}

fn digest(payload: &str) -> String {
    let bytes = Sha256::digest(payload.as_bytes());
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GatePublicError {
    #[error("Gate public binding is not an exact USDT futures symbol and positive multiplier")]
    Binding,
    #[error("Gate public payload metadata is invalid or does not match its binding")]
    Metadata,
    #[error("Gate public payload has an invalid documented shape")]
    Payload,
    #[error("Gate public payload carries a different native contract")]
    Symbol,
    #[error("Gate public payload has an invalid numeric field")]
    Number,
    #[error("Gate public order-book sequence is invalid")]
    Sequence,
    #[error("Gate public order-book delta buffer is full")]
    BufferFull,
    #[error("Gate public REST order-book snapshot is behind the delta stream")]
    SnapshotBehind,
    #[error("Gate public order-book delta stream has a sequence gap")]
    SequenceGap,
    #[error("Gate public order-book bridge is unavailable until a new snapshot")]
    BridgeInvalid,
    #[error("Gate public order-book bridge transition is invalid")]
    Bridge,
    #[error("Gate public event belongs to a different or stale generation")]
    Generation,
    #[error("Gate public trade IDs are duplicate or out of order")]
    TradeOrder,
    #[error("Gate public depth has a duplicate price level")]
    DuplicateLevel,
    #[error("Gate public depth must be one of the documented incremental levels")]
    Depth,
    #[error("Gate public depth subscription is not a documented frequency/level combination")]
    Subscription,
    #[cfg(test)]
    #[error("Gate public freshness inputs are invalid")]
    Freshness,
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_domain::domain::MarketEvent;

    fn binding() -> Result<GatePublicBinding, GatePublicError> {
        GatePublicBinding::new(
            "DOGE/USDT".parse().map_err(|_| GatePublicError::Binding)?,
            "DOGE_USDT",
            Decimal::from_str("10").map_err(|_| GatePublicError::Number)?,
        )
    }

    fn raw(
        kind: GatePublicPayloadKind,
        payload: &str,
    ) -> Result<GatePublicRawPayload, GatePublicError> {
        GatePublicRawPayload::new(&binding()?, kind, 7, 1_000, payload.to_owned())
    }

    fn snapshot_payload(id: u64) -> String {
        format!(
            r#"{{"id":{id},"current":1700000000.123,"update":1700000000.120,"bids":[{{"p":"0.100","s":"2"}}],"asks":[{{"p":"0.101","s":"3"}}]}}"#
        )
    }

    fn delta_payload(first: u64, last: u64, full: bool) -> String {
        let full = full.then_some(",\"full\":true").unwrap_or_default();
        format!(
            r#"{{"time_ms":1700000001000,"channel":"futures.order_book_update","event":"update","result":{{"t":1700000001001,"s":"DOGE_USDT","U":{first},"u":{last},"b":[{{"p":"0.100","s":"2"}}],"a":[{{"p":"0.101","s":"3"}}]{full}}}}}"#
        )
    }

    #[test]
    fn rest_snapshot_and_ws_delta_bridge_exactly() -> Result<(), GatePublicError> {
        let binding = binding()?;
        assert_eq!(
            rest_order_book_path(&binding, 20)?,
            "/futures/usdt/order_book?contract=DOGE_USDT&interval=0&limit=20&with_id=true"
        );
        let subscriptions = public_subscriptions(&binding, 100, 20)?;
        assert_eq!(
            subscriptions
                .as_array()
                .and_then(|values| values.first())
                .and_then(|value| value.get("channel"))
                .and_then(Value::as_str),
            Some(CHANNEL_BOOK_DELTA)
        );
        let grid_subscriptions = grid_public_subscriptions(&binding, 100, 20)?;
        assert_eq!(grid_subscriptions.as_array().map(Vec::len), Some(1));
        assert_eq!(
            grid_subscriptions
                .as_array()
                .and_then(|values| values.first())
                .and_then(|value| value.get("channel"))
                .and_then(Value::as_str),
            Some(CHANNEL_BOOK_DELTA)
        );
        assert_eq!(
            rest_order_book_path(&binding, 10),
            Err(GatePublicError::Depth)
        );
        assert_eq!(
            public_subscriptions(&binding, 20, 50),
            Err(GatePublicError::Subscription)
        );
        let delta = parse_ws_delta(
            &binding,
            raw(
                GatePublicPayloadKind::WebSocketOrderBookDelta,
                &delta_payload(100, 102, false),
            )?,
        )?;
        let snapshot = parse_rest_snapshot(
            &binding,
            raw(
                GatePublicPayloadKind::RestOrderBookSnapshot,
                &snapshot_payload(100),
            )?,
        )?;
        assert_eq!(snapshot.value.exchange_time_ms, Some(1_700_000_000_123));
        assert_eq!(snapshot.value.bids[0].quantity, Decimal::from(20));
        assert!(matches!(snapshot.market_event(), MarketEvent::Snapshot(_)));

        let mut bridge = GateOrderBookBridge::new(binding, 7, 4)?;
        assert_eq!(bridge.generation(), 7);
        assert_eq!(
            bridge.receive_delta(delta)?,
            vec![GateBookBridgeAction::Buffered]
        );
        let actions = bridge.receive_snapshot(snapshot)?;
        assert!(bridge.is_ready());
        assert!(matches!(
            actions.as_slice(),
            [
                GateBookBridgeAction::ReplaceSnapshot(_),
                GateBookBridgeAction::ApplyDelta(_)
            ]
        ));
        if let GateBookBridgeAction::ApplyDelta(delta) = &actions[1] {
            assert!(matches!(delta.market_event(), MarketEvent::Delta(_)));
        } else {
            return Err(GatePublicError::Bridge);
        }
        bridge.reset_generation(8)?;
        assert_eq!(bridge.generation(), 8);
        assert!(!bridge.is_ready());
        Ok(())
    }

    #[test]
    fn bridge_rejects_snapshot_behind_the_first_delta() -> Result<(), GatePublicError> {
        let binding = binding()?;
        let mut bridge = GateOrderBookBridge::new(binding.clone(), 7, 4)?;
        let delta = parse_ws_delta(
            &binding,
            raw(
                GatePublicPayloadKind::WebSocketOrderBookDelta,
                &delta_payload(103, 104, false),
            )?,
        )?;
        let _ = bridge.receive_delta(delta)?;
        let snapshot = parse_rest_snapshot(
            &binding,
            raw(
                GatePublicPayloadKind::RestOrderBookSnapshot,
                &snapshot_payload(100),
            )?,
        )?;
        assert_eq!(
            bridge.receive_snapshot(snapshot),
            Err(GatePublicError::SnapshotBehind)
        );
        assert!(!bridge.is_ready());
        Ok(())
    }

    #[test]
    fn bridge_rejects_a_post_bridge_gap_and_drops_readiness() -> Result<(), GatePublicError> {
        let binding = binding()?;
        let mut bridge = GateOrderBookBridge::new(binding.clone(), 7, 4)?;
        let snapshot = parse_rest_snapshot(
            &binding,
            raw(
                GatePublicPayloadKind::RestOrderBookSnapshot,
                &snapshot_payload(100),
            )?,
        )?;
        let _ = bridge.receive_snapshot(snapshot)?;
        let first = parse_ws_delta(
            &binding,
            raw(
                GatePublicPayloadKind::WebSocketOrderBookDelta,
                &delta_payload(101, 101, false),
            )?,
        )?;
        let _ = bridge.receive_delta(first)?;
        let gap = parse_ws_delta(
            &binding,
            raw(
                GatePublicPayloadKind::WebSocketOrderBookDelta,
                &delta_payload(103, 103, false),
            )?,
        )?;
        assert_eq!(bridge.receive_delta(gap), Err(GatePublicError::SequenceGap));
        assert!(!bridge.is_ready());
        Ok(())
    }

    #[test]
    fn full_delta_replaces_the_book_without_publishing_an_increment() -> Result<(), GatePublicError>
    {
        let binding = binding()?;
        let mut bridge = GateOrderBookBridge::new(binding.clone(), 7, 4)?;
        let delta = parse_ws_delta(
            &binding,
            raw(
                GatePublicPayloadKind::WebSocketOrderBookDelta,
                &delta_payload(130, 132, true),
            )?,
        )?;
        let actions = bridge.receive_delta(delta)?;
        assert!(bridge.is_ready());
        assert!(matches!(
            actions.as_slice(),
            [GateBookBridgeAction::ReplaceSnapshot(snapshot)] if snapshot.value.sequence == 132
        ));
        Ok(())
    }

    #[test]
    fn parser_rejects_wrong_symbol_tampered_metadata_and_duplicate_levels()
    -> Result<(), GatePublicError> {
        let binding = binding()?;
        let wrong_symbol = r#"{"time_ms":1,"channel":"futures.order_book_update","event":"update","result":{"t":2,"s":"BTC_USDT","U":1,"u":1,"b":[],"a":[]}}"#;
        assert_eq!(
            parse_ws_delta(
                &binding,
                raw(GatePublicPayloadKind::WebSocketOrderBookDelta, wrong_symbol)?,
            ),
            Err(GatePublicError::Symbol)
        );
        let mut tampered = raw(
            GatePublicPayloadKind::RestOrderBookSnapshot,
            &snapshot_payload(1),
        )?;
        tampered.payload.push(' ');
        assert_eq!(
            parse_rest_snapshot(&binding, tampered),
            Err(GatePublicError::Metadata)
        );
        let duplicate = r#"{"id":1,"current":1.001,"bids":[{"p":"0.1","s":"1"},{"p":"0.1","s":"2"}],"asks":[{"p":"0.2","s":"1"}]}"#;
        assert_eq!(
            parse_rest_snapshot(
                &binding,
                raw(GatePublicPayloadKind::RestOrderBookSnapshot, duplicate)?,
            ),
            Err(GatePublicError::DuplicateLevel)
        );
        Ok(())
    }

    #[test]
    fn ticker_mark_and_trade_carry_freshness_and_normalized_quantity() -> Result<(), GatePublicError>
    {
        let binding = binding()?;
        let ticker = parse_ws_book_ticker(
            &binding,
            raw(
                GatePublicPayloadKind::WebSocketBookTicker,
                r#"{"channel":"futures.book_ticker","event":"update","result":{"t":2000,"u":19,"s":"DOGE_USDT","b":"0.100","B":2,"a":"0.101","A":"3"}}"#,
            )?,
        )?;
        assert_eq!(ticker.value.bid_quantity, Decimal::from(20));
        assert!(matches!(ticker.market_event(), MarketEvent::Ticker(_)));
        assert!(ticker.freshness.is_fresh_at(1_001, 1)?);
        assert!(!ticker.freshness.is_fresh_at(1_002, 1)?);

        let mark = parse_ws_mark_price(
            &binding,
            raw(
                GatePublicPayloadKind::WebSocketTicker,
                r#"{"channel":"futures.tickers","event":"update","result":[{"contract":"DOGE_USDT","t":2001,"mark_price":"0.1005","index_price":"0.1004","funding_rate":"0.0001","price_type":"mark"}]}"#,
            )?,
        )?;
        assert_eq!(mark.value.price_type, GateTickerPriceType::Mark);

        let trades = parse_ws_trades(
            &binding,
            raw(
                GatePublicPayloadKind::WebSocketTrade,
                r#"{"time_ms":2002,"channel":"futures.trades","event":"update","result":[{"id":21,"create_time":2.002,"create_time_ms":2002,"contract":"DOGE_USDT","size":"-2","price":"0.100"}]}"#,
            )?,
        )?;
        assert_eq!(trades[0].value.quantity, Decimal::from(20));
        assert_eq!(
            trades[0].value.aggressor,
            FieldState::Known(AggressorSide::Sell)
        );
        assert!(matches!(trades[0].market_event(), MarketEvent::Trade(_)));
        Ok(())
    }

    #[test]
    fn trades_reject_duplicate_or_out_of_order_ids_and_conflicting_time()
    -> Result<(), GatePublicError> {
        let binding = binding()?;
        let duplicate = r#"{"time_ms":2002,"channel":"futures.trades","event":"update","result":[{"id":21,"create_time_ms":2002,"contract":"DOGE_USDT","size":"2","price":"0.100"},{"id":21,"create_time_ms":2003,"contract":"DOGE_USDT","size":"2","price":"0.100"}]}"#;
        assert_eq!(
            parse_ws_trades(
                &binding,
                raw(GatePublicPayloadKind::WebSocketTrade, duplicate)?,
            ),
            Err(GatePublicError::TradeOrder)
        );
        let mismatch = r#"{"time_ms":2002,"channel":"futures.trades","event":"update","result":[{"id":21,"create_time":2,"create_time_ms":3001,"contract":"DOGE_USDT","size":"2","price":"0.100"}]}"#;
        assert_eq!(
            parse_ws_trades(
                &binding,
                raw(GatePublicPayloadKind::WebSocketTrade, mismatch)?,
            ),
            Err(GatePublicError::Payload)
        );
        Ok(())
    }
}
