//! Credential-free Bybit V5 linear order-book receiver.
//!
//! Bybit documents `u` as an update identifier and `seq` only as a cross-depth ordering value;
//! it does not publish a predecessor or a promise that either value is adjacent.  Consequently
//! this adapter rejects regression and repetition, applies each delta locally, and emits the
//! reconstructed book as a complete snapshot rather than manufacturing a delta predecessor.

use std::{
    collections::BTreeMap,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::{Map, Value, json};
use thiserror::Error;
use tokio::time::{Instant, timeout};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use venue_domain::domain::{MarketEvent, MarketLevel, MarketSnapshot, Price};
use venue_gateway_api::{GatewayBinding, VenueId};

use crate::{BybitGatewayBinding, linear_native_symbol};

const BOOK_DEPTH: u16 = 50;
// A depth-50 delta can transiently introduce a new price before the displaced level is removed.
// Keep room for normal reordering, but never let a malformed stream grow the local book without
// bound. Overflow closes this receiver rather than silently truncating a snapshot.
const MAX_LOCAL_BOOK_LEVELS: usize = 200;
const MAX_BODY_BYTES: usize = 2 * 1_024 * 1_024;
const MAX_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
const HEARTBEAT_SEND_TIMEOUT: Duration = Duration::from_secs(1);

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// A bounded public receiver for exactly one Bybit LIVE USDT-linear symbol. It owns no
/// credentials and has no path to a private or mutation transport.
pub struct BybitScalpingPublicReceiver {
    socket: Socket,
    bridge: BybitBookBridge,
    connect_timeout: Duration,
    maximum_body_bytes: usize,
    next_heartbeat_at: Instant,
    heartbeat_sequence: u64,
    heartbeat_pending: bool,
}

impl BybitScalpingPublicReceiver {
    pub async fn connect(
        binding: GatewayBinding,
        connect_timeout: Duration,
        max_body_bytes: usize,
    ) -> Result<Self, BybitPublicWsError> {
        validate_limits(connect_timeout, max_body_bytes)?;
        binding
            .validate()
            .map_err(|_| BybitPublicWsError::Binding)?;
        if binding.venue != VenueId::Bybit {
            return Err(BybitPublicWsError::Binding);
        }
        let binding = BybitGatewayBinding::new(binding).map_err(|_| BybitPublicWsError::Binding)?;
        let native_symbol = linear_native_symbol(&binding.gateway_binding().symbol)
            .map_err(|_| BybitPublicWsError::Binding)?;
        let generation = now_ms()?;
        let websocket = WebSocketConfig::default()
            .max_message_size(Some(max_body_bytes))
            .max_frame_size(Some(max_body_bytes));
        let deadline = Instant::now() + connect_timeout;
        let (mut socket, _) = timeout(
            remaining(deadline)?,
            connect_async_with_config(binding.config().public_ws(), Some(websocket), true),
        )
        .await
        .map_err(|_| BybitPublicWsError::Timeout)?
        .map_err(|_| BybitPublicWsError::Disconnected)?;
        let topic = book_topic(&native_symbol);
        let subscribe = json!({"op":"subscribe","args":[topic]}).to_string();
        timeout(
            remaining(deadline)?,
            socket.send(Message::Text(subscribe.into())),
        )
        .await
        .map_err(|_| BybitPublicWsError::Timeout)?
        .map_err(|_| BybitPublicWsError::Disconnected)?;
        Ok(Self {
            bridge: BybitBookBridge::new(binding.gateway_binding(), native_symbol, generation)?,
            socket,
            connect_timeout,
            maximum_body_bytes: max_body_bytes,
            next_heartbeat_at: Instant::now() + HEARTBEAT_INTERVAL,
            heartbeat_sequence: 0,
            heartbeat_pending: false,
        })
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.bridge.generation
    }

    /// Reads at most one frame. Idle, successful subscription acknowledgements, and heartbeat
    /// traffic do not become strategy facts. Any malformed, rejected, crossed, or out-of-order
    /// book frame is terminal for this receiver; callers must discard the generation.
    pub async fn next(
        &mut self,
        wait: Duration,
    ) -> Result<Option<(u64, MarketEvent)>, BybitPublicWsError> {
        self.send_heartbeat_if_due().await?;
        let Some(frame) = (match timeout(wait, self.socket.next()).await {
            Ok(frame) => frame,
            Err(_) => return Ok(None),
        }) else {
            return Err(BybitPublicWsError::Disconnected);
        };
        let frame = frame.map_err(|_| BybitPublicWsError::Disconnected)?;
        match frame {
            Message::Text(value) => self.parse_text(value.to_string()),
            Message::Binary(value) => String::from_utf8(value.to_vec())
                .map_err(|_| BybitPublicWsError::Protocol)
                .and_then(|value| self.parse_text(value)),
            Message::Ping(value) => {
                timeout(
                    self.connect_timeout.min(HEARTBEAT_SEND_TIMEOUT),
                    self.socket.send(Message::Pong(value)),
                )
                .await
                .map_err(|_| BybitPublicWsError::Timeout)?
                .map_err(|_| BybitPublicWsError::Disconnected)?;
                Ok(None)
            }
            Message::Pong(_) => Ok(None),
            Message::Close(_) => Err(BybitPublicWsError::Disconnected),
            Message::Frame(_) => Err(BybitPublicWsError::Protocol),
        }
    }

    async fn send_heartbeat_if_due(&mut self) -> Result<(), BybitPublicWsError> {
        if Instant::now() < self.next_heartbeat_at {
            return Ok(());
        }
        if self.heartbeat_pending {
            return Err(BybitPublicWsError::Heartbeat);
        }
        self.heartbeat_sequence = self
            .heartbeat_sequence
            .checked_add(1)
            .ok_or(BybitPublicWsError::Protocol)?;
        let request = json!({
            "req_id": format!("venue-public-{}-{}", self.generation(), self.heartbeat_sequence),
            "op": "ping",
        })
        .to_string();
        timeout(
            self.connect_timeout.min(HEARTBEAT_SEND_TIMEOUT),
            self.socket.send(Message::Text(request.into())),
        )
        .await
        .map_err(|_| BybitPublicWsError::Timeout)?
        .map_err(|_| BybitPublicWsError::Disconnected)?;
        self.next_heartbeat_at = Instant::now() + HEARTBEAT_INTERVAL;
        self.heartbeat_pending = true;
        Ok(())
    }

    fn parse_text(
        &mut self,
        payload: String,
    ) -> Result<Option<(u64, MarketEvent)>, BybitPublicWsError> {
        if payload.is_empty() || payload.len() > self.maximum_body_bytes {
            return Err(BybitPublicWsError::BodyTooLarge);
        }
        let value: Value =
            serde_json::from_str(&payload).map_err(|_| BybitPublicWsError::Protocol)?;
        let root = object(&value)?;
        if let Some(op) = root.get("op").and_then(Value::as_str) {
            if valid_heartbeat_reply(op, root) {
                self.heartbeat_pending = false;
            }
            return self.handle_control(op, root);
        }
        let received_at_ms = now_ms()?;
        self.bridge.accept(root, received_at_ms).map(Some)
    }

    fn handle_control(
        &mut self,
        op: &str,
        root: &Map<String, Value>,
    ) -> Result<Option<(u64, MarketEvent)>, BybitPublicWsError> {
        validate_control(op, root)?;
        Ok(None)
    }
}

struct BybitBookBridge {
    binding: GatewayBinding,
    native_symbol: String,
    generation: u64,
    last_update_id: Option<u64>,
    last_cross_sequence: Option<u64>,
    bids: BTreeMap<Price, Decimal>,
    asks: BTreeMap<Price, Decimal>,
}

impl BybitBookBridge {
    fn new(
        binding: &GatewayBinding,
        native_symbol: String,
        generation: u64,
    ) -> Result<Self, BybitPublicWsError> {
        if generation == 0 || native_symbol.is_empty() {
            return Err(BybitPublicWsError::Clock);
        }
        Ok(Self {
            binding: binding.clone(),
            native_symbol,
            generation,
            last_update_id: None,
            last_cross_sequence: None,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        })
    }

    fn accept(
        &mut self,
        root: &Map<String, Value>,
        received_at_ms: u64,
    ) -> Result<(u64, MarketEvent), BybitPublicWsError> {
        let topic = required_text(root, "topic")?;
        if topic != book_topic(&self.native_symbol) {
            return Err(BybitPublicWsError::Binding);
        }
        let kind = required_text(root, "type")?;
        let ts = required_u64(root, "ts")?;
        let cts = required_u64(root, "cts")?;
        if cts > ts || received_at_ms == 0 {
            return Err(BybitPublicWsError::Protocol);
        }
        let data = required_object(root, "data")?;
        if required_text(data, "s")? != self.native_symbol {
            return Err(BybitPublicWsError::Binding);
        }
        let update_id = required_u64(data, "u")?;
        let cross_sequence = required_u64(data, "seq")?;
        let bids = parse_levels(required_array(data, "b")?, kind == "snapshot", true)?;
        let asks = parse_levels(required_array(data, "a")?, kind == "snapshot", false)?;
        match kind {
            "snapshot" => self.replace_snapshot(update_id, cross_sequence, cts, bids, asks),
            "delta" => self.apply_delta(update_id, cross_sequence, cts, bids, asks),
            _ => Err(BybitPublicWsError::Protocol),
        }
        .map(|event| (received_at_ms, event))
    }

    fn replace_snapshot(
        &mut self,
        update_id: u64,
        cross_sequence: u64,
        exchange_time_ms: u64,
        bids: Vec<MarketLevel>,
        asks: Vec<MarketLevel>,
    ) -> Result<MarketEvent, BybitPublicWsError> {
        if let Some((last_update_id, last_cross_sequence)) =
            self.last_update_id.zip(self.last_cross_sequence)
        {
            // `u=1` is Bybit's documented service-restart snapshot. Other snapshots still must
            // be newer than the active stream, so a delayed old snapshot cannot revive it.
            if update_id != 1
                && (update_id <= last_update_id || cross_sequence <= last_cross_sequence)
            {
                return Err(BybitPublicWsError::Sequence);
            }
            self.generation = self
                .generation
                .checked_add(1)
                .ok_or(BybitPublicWsError::Sequence)?;
        }
        self.bids = levels_to_book(&bids)?;
        self.asks = levels_to_book(&asks)?;
        validate_book(&self.bids, &self.asks)?;
        self.last_update_id = Some(update_id);
        self.last_cross_sequence = Some(cross_sequence);
        Ok(self.current_snapshot(update_id, exchange_time_ms))
    }

    fn apply_delta(
        &mut self,
        update_id: u64,
        cross_sequence: u64,
        exchange_time_ms: u64,
        bids: Vec<MarketLevel>,
        asks: Vec<MarketLevel>,
    ) -> Result<MarketEvent, BybitPublicWsError> {
        let (last_update_id, last_cross_sequence) = self
            .last_update_id
            .zip(self.last_cross_sequence)
            .ok_or(BybitPublicWsError::Sequence)?;
        if update_id <= last_update_id || cross_sequence <= last_cross_sequence {
            return Err(BybitPublicWsError::Sequence);
        }
        apply_levels(&mut self.bids, &bids)?;
        apply_levels(&mut self.asks, &asks)?;
        validate_book(&self.bids, &self.asks)?;
        self.last_update_id = Some(update_id);
        self.last_cross_sequence = Some(cross_sequence);
        // V5 supplies neither a previous nor a range sequence for this topic. Publishing the
        // fully rebuilt local book makes the watermark usable without falsely claiming a delta
        // can satisfy a consumer's predecessor-continuity contract.
        Ok(self.current_snapshot(update_id, exchange_time_ms))
    }

    fn current_snapshot(&self, update_id: u64, exchange_time_ms: u64) -> MarketEvent {
        MarketEvent::Snapshot(MarketSnapshot {
            symbol: self.binding.symbol.clone(),
            generation: self.generation,
            sequence: update_id,
            exchange_time_ms: Some(exchange_time_ms),
            bids: sorted_bids(&self.bids),
            asks: sorted_asks(&self.asks),
        })
    }
}

fn validate_limits(
    connect_timeout: Duration,
    maximum_body_bytes: usize,
) -> Result<(), BybitPublicWsError> {
    if connect_timeout.is_zero()
        || connect_timeout > MAX_CONNECT_TIMEOUT
        || maximum_body_bytes == 0
        || maximum_body_bytes > MAX_BODY_BYTES
    {
        Err(BybitPublicWsError::Limits)
    } else {
        Ok(())
    }
}

fn remaining(deadline: Instant) -> Result<Duration, BybitPublicWsError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|duration| !duration.is_zero())
        .ok_or(BybitPublicWsError::Timeout)
}

fn book_topic(native_symbol: &str) -> String {
    format!("orderbook.{BOOK_DEPTH}.{native_symbol}")
}

fn validate_control(op: &str, root: &Map<String, Value>) -> Result<(), BybitPublicWsError> {
    match op {
        "subscribe" if root.get("success").and_then(Value::as_bool) == Some(true) => Ok(()),
        "subscribe" => Err(BybitPublicWsError::SubscriptionRejected),
        "pong" => Ok(()),
        // Linear/inverse sends the application ping response as `op: ping`, `success: true`,
        // and `ret_msg: pong`; it is not a market input.
        "ping" if valid_heartbeat_reply(op, root) => Ok(()),
        _ if root.get("success").and_then(Value::as_bool) == Some(false) => {
            Err(BybitPublicWsError::SubscriptionRejected)
        }
        _ => Err(BybitPublicWsError::Protocol),
    }
}

fn valid_heartbeat_reply(op: &str, root: &Map<String, Value>) -> bool {
    op == "pong"
        || (op == "ping"
            && root.get("success").and_then(Value::as_bool) == Some(true)
            && root.get("ret_msg").and_then(Value::as_str) == Some("pong"))
}

fn parse_levels(
    values: &[Value],
    snapshot: bool,
    descending: bool,
) -> Result<Vec<MarketLevel>, BybitPublicWsError> {
    let mut levels = Vec::with_capacity(values.len());
    let mut seen = BTreeMap::new();
    for value in values {
        let fields = value
            .as_array()
            .filter(|fields| fields.len() == 2)
            .ok_or(BybitPublicWsError::Protocol)?;
        let price = Price::new(decimal(fields.first())?).map_err(|_| BybitPublicWsError::Book)?;
        let quantity = decimal(fields.get(1))?;
        if quantity.is_sign_negative()
            || (snapshot && quantity.is_zero())
            || seen.insert(price, ()).is_some()
        {
            return Err(BybitPublicWsError::Book);
        }
        levels.push(MarketLevel { price, quantity });
    }
    if snapshot && values.is_empty() {
        return Err(BybitPublicWsError::Book);
    }
    if snapshot
        && levels.windows(2).any(|levels| {
            if descending {
                levels[0].price <= levels[1].price
            } else {
                levels[0].price >= levels[1].price
            }
        })
    {
        return Err(BybitPublicWsError::Book);
    }
    Ok(levels)
}

fn levels_to_book(levels: &[MarketLevel]) -> Result<BTreeMap<Price, Decimal>, BybitPublicWsError> {
    let mut book = BTreeMap::new();
    for level in levels {
        if level.quantity <= Decimal::ZERO || book.insert(level.price, level.quantity).is_some() {
            return Err(BybitPublicWsError::Book);
        }
    }
    if book.len() > MAX_LOCAL_BOOK_LEVELS {
        return Err(BybitPublicWsError::Book);
    }
    Ok(book)
}

fn apply_levels(
    book: &mut BTreeMap<Price, Decimal>,
    levels: &[MarketLevel],
) -> Result<(), BybitPublicWsError> {
    for level in levels {
        if level.quantity.is_sign_negative() {
            return Err(BybitPublicWsError::Book);
        }
        if level.quantity.is_zero() {
            book.remove(&level.price);
        } else {
            book.insert(level.price, level.quantity);
        }
    }
    if book.len() > MAX_LOCAL_BOOK_LEVELS {
        return Err(BybitPublicWsError::Book);
    }
    Ok(())
}

fn validate_book(
    bids: &BTreeMap<Price, Decimal>,
    asks: &BTreeMap<Price, Decimal>,
) -> Result<(), BybitPublicWsError> {
    let bid = bids
        .last_key_value()
        .map(|(price, _)| price)
        .ok_or(BybitPublicWsError::Book)?;
    let ask = asks
        .first_key_value()
        .map(|(price, _)| price)
        .ok_or(BybitPublicWsError::Book)?;
    if bid >= ask {
        Err(BybitPublicWsError::Book)
    } else {
        Ok(())
    }
}

fn sorted_bids(book: &BTreeMap<Price, Decimal>) -> Vec<MarketLevel> {
    book.iter()
        .rev()
        .map(|(price, quantity)| MarketLevel {
            price: *price,
            quantity: *quantity,
        })
        .collect()
}

fn sorted_asks(book: &BTreeMap<Price, Decimal>) -> Vec<MarketLevel> {
    book.iter()
        .map(|(price, quantity)| MarketLevel {
            price: *price,
            quantity: *quantity,
        })
        .collect()
}

fn object(value: &Value) -> Result<&Map<String, Value>, BybitPublicWsError> {
    value.as_object().ok_or(BybitPublicWsError::Protocol)
}

fn required_object<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a Map<String, Value>, BybitPublicWsError> {
    object
        .get(field)
        .and_then(Value::as_object)
        .ok_or(BybitPublicWsError::Protocol)
}

fn required_array<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a Vec<Value>, BybitPublicWsError> {
    object
        .get(field)
        .and_then(Value::as_array)
        .ok_or(BybitPublicWsError::Protocol)
}

fn required_text<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a str, BybitPublicWsError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or(BybitPublicWsError::Protocol)
}

fn required_u64(object: &Map<String, Value>, field: &str) -> Result<u64, BybitPublicWsError> {
    object
        .get(field)
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .ok_or(BybitPublicWsError::Protocol)
}

fn decimal(value: Option<&Value>) -> Result<Decimal, BybitPublicWsError> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .and_then(|value| Decimal::from_str(value).ok())
        .ok_or(BybitPublicWsError::Book)
}

fn now_ms() -> Result<u64, BybitPublicWsError> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| BybitPublicWsError::Clock)?
            .as_millis(),
    )
    .map_err(|_| BybitPublicWsError::Clock)
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BybitPublicWsError {
    #[error("Bybit public receiver binding is invalid")]
    Binding,
    #[error("Bybit public receiver limits are invalid")]
    Limits,
    #[error("Bybit public receiver clock is invalid")]
    Clock,
    #[error("Bybit public receiver operation timed out")]
    Timeout,
    #[error("Bybit public receiver disconnected")]
    Disconnected,
    #[error("Bybit public receiver heartbeat was not acknowledged")]
    Heartbeat,
    #[error("Bybit public receiver frame is malformed")]
    Protocol,
    #[error("Bybit public receiver body exceeds its configured limit")]
    BodyTooLarge,
    #[error("Bybit public receiver subscription was rejected")]
    SubscriptionRejected,
    #[error("Bybit public order-book sequence is repeated, regressed, or unbridged")]
    Sequence,
    #[error("Bybit public order-book is empty, crossed, or structurally invalid")]
    Book,
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_gateway_api::{GatewayMode, VenueId};

    const ACCOUNT: &str = "00000000-0000-4000-8000-000000000001";
    const SNAPSHOT: &str = include_str!("../fixtures/public-ws-orderbook-snapshot.json");

    fn binding() -> Result<GatewayBinding, BybitPublicWsError> {
        GatewayBinding::new(
            VenueId::Bybit,
            GatewayMode::Live,
            ACCOUNT,
            "DOGE/USDT"
                .parse()
                .map_err(|_| BybitPublicWsError::Binding)?,
        )
        .map_err(|_| BybitPublicWsError::Binding)
    }

    fn bridge() -> Result<BybitBookBridge, BybitPublicWsError> {
        let binding = binding()?;
        BybitBookBridge::new(&binding, "DOGEUSDT".to_owned(), 7)
    }

    fn root(payload: &str) -> Result<Map<String, Value>, BybitPublicWsError> {
        let value: Value =
            serde_json::from_str(payload).map_err(|_| BybitPublicWsError::Protocol)?;
        Ok(object(&value)?.clone())
    }

    #[test]
    fn fixture_snapshot_and_legal_u_jumps_emit_complete_rebuilt_snapshots()
    -> Result<(), BybitPublicWsError> {
        let mut bridge = bridge()?;
        let (_, snapshot) = bridge.accept(&root(SNAPSHOT)?, 1_000)?;
        assert!(matches!(snapshot, MarketEvent::Snapshot(_)));
        let delta = SNAPSHOT
            .replace("\"type\": \"snapshot\"", "\"type\": \"delta\"")
            .replace("\"u\": 100", "\"u\": 112")
            .replace("\"seq\": 200", "\"seq\": 215")
            .replace("[\"0.1000\", \"9\"]", "[\"0.1000\", \"8\"]");
        let (_, event) = bridge.accept(&root(&delta)?, 1_001)?;
        let MarketEvent::Snapshot(snapshot) = event else {
            return Err(BybitPublicWsError::Protocol);
        };
        assert_eq!(snapshot.sequence, 112);
        assert_eq!(snapshot.generation, 7);
        assert_eq!(snapshot.bids[0].quantity.to_string(), "8");
        let later_delta = delta
            .replace("\"u\": 112", "\"u\": 143")
            .replace("\"seq\": 215", "\"seq\": 216")
            .replace("\"8\"", "\"7\"");
        let (_, event) = bridge.accept(&root(&later_delta)?, 1_002)?;
        let MarketEvent::Snapshot(snapshot) = event else {
            return Err(BybitPublicWsError::Protocol);
        };
        assert_eq!(snapshot.sequence, 143);
        assert_eq!(snapshot.bids[0].quantity.to_string(), "7");
        Ok(())
    }

    #[test]
    fn new_snapshot_advances_generation_and_bad_book_or_sequence_is_terminal()
    -> Result<(), BybitPublicWsError> {
        let mut stream_bridge = bridge()?;
        let _ = stream_bridge.accept(&root(SNAPSHOT)?, 1_000)?;
        let (_, reset) = stream_bridge.accept(
            &root(
                &SNAPSHOT
                    .replace("\"u\": 100", "\"u\": 1")
                    .replace("\"seq\": 200", "\"seq\": 201"),
            )?,
            1_001,
        )?;
        let MarketEvent::Snapshot(reset) = reset else {
            return Err(BybitPublicWsError::Protocol);
        };
        assert_eq!(reset.generation, 8);
        let duplicate = SNAPSHOT.replace("\"type\": \"snapshot\"", "\"type\": \"delta\"");
        assert_eq!(
            stream_bridge.accept(&root(&duplicate)?, 1_002),
            Err(BybitPublicWsError::Sequence)
        );
        let mut bridge = bridge()?;
        assert_eq!(
            bridge.accept(&root(&SNAPSHOT.replace("0.1010", "0.0990"))?, 1_000),
            Err(BybitPublicWsError::Book)
        );
        Ok(())
    }

    #[test]
    fn rejects_wrong_topic_negative_size_and_subscription_rejection()
    -> Result<(), BybitPublicWsError> {
        let mut bridge = bridge()?;
        assert_eq!(
            bridge.accept(&root(&SNAPSHOT.replace("DOGEUSDT", "BTCUSDT"))?, 1_000),
            Err(BybitPublicWsError::Binding)
        );
        assert_eq!(
            bridge.accept(&root(&SNAPSHOT.replace("\"9\"", "\"-9\""))?, 1_000),
            Err(BybitPublicWsError::Book)
        );
        assert_eq!(
            validate_limits(Duration::ZERO, 1),
            Err(BybitPublicWsError::Limits)
        );
        assert_eq!(
            validate_control("subscribe", &root(r#"{"op":"subscribe","success":false}"#)?,),
            Err(BybitPublicWsError::SubscriptionRejected)
        );
        assert_eq!(
            validate_control(
                "ping",
                &root(r#"{"op":"ping","success":true,"ret_msg":"pong"}"#)?,
            ),
            Ok(())
        );
        Ok(())
    }

    #[test]
    fn delta_new_prices_do_not_gain_an_undocumented_fifty_level_rejection()
    -> Result<(), BybitPublicWsError> {
        let mut book = BTreeMap::new();
        for value in 1_i64..=50 {
            let price = Price::new(Decimal::from(value)).map_err(|_| BybitPublicWsError::Book)?;
            book.insert(price, Decimal::ONE);
        }
        let new_price = Price::new(Decimal::from(51)).map_err(|_| BybitPublicWsError::Book)?;
        apply_levels(
            &mut book,
            &[MarketLevel {
                price: new_price,
                quantity: Decimal::ONE,
            }],
        )?;
        assert_eq!(book.len(), 51);
        Ok(())
    }

    #[test]
    fn local_book_cap_fails_closed_instead_of_accumulating_unbounded_deltas()
    -> Result<(), BybitPublicWsError> {
        let mut book = BTreeMap::new();
        for value in
            1_i64..=i64::try_from(MAX_LOCAL_BOOK_LEVELS).map_err(|_| BybitPublicWsError::Book)?
        {
            let price = Price::new(Decimal::from(value)).map_err(|_| BybitPublicWsError::Book)?;
            book.insert(price, Decimal::ONE);
        }
        let overflow = Price::new(Decimal::from(
            i64::try_from(MAX_LOCAL_BOOK_LEVELS + 1).map_err(|_| BybitPublicWsError::Book)?,
        ))
        .map_err(|_| BybitPublicWsError::Book)?;
        assert_eq!(
            apply_levels(
                &mut book,
                &[MarketLevel {
                    price: overflow,
                    quantity: Decimal::ONE,
                }],
            ),
            Err(BybitPublicWsError::Book)
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a short unauthenticated Bybit mainnet public WebSocket connection"]
    async fn mainnet_public_orderbook_probe_emits_a_complete_snapshot()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut receiver =
            BybitScalpingPublicReceiver::connect(binding()?, Duration::from_secs(10), 64 * 1_024)
                .await?;
        for _ in 0..5 {
            if let Some((received_at_ms, MarketEvent::Snapshot(snapshot))) =
                receiver.next(Duration::from_secs(2)).await?
            {
                assert!(received_at_ms > 0);
                assert!(snapshot.sequence > 0);
                assert!(snapshot.exchange_time_ms.is_some());
                assert!(!snapshot.bids.is_empty());
                assert!(!snapshot.asks.is_empty());
                assert!(snapshot.bids[0].price < snapshot.asks[0].price);
                return Ok(());
            }
        }
        Err("Bybit public orderbook did not arrive before probe deadline".into())
    }
}
