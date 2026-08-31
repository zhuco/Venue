//! Credential-free Hyperliquid perpetual L2 snapshot receiver.
//!
//! Hyperliquid's `l2Book` stream publishes complete books with a source `time`, but no
//! documented incrementing book sequence.  Consequently each accepted `MarketSnapshot` uses
//! that source time only as a snapshot watermark; this receiver never manufactures a delta or
//! claims contiguous incremental recovery.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use thiserror::Error;
use tokio::time::timeout;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use venue_domain::domain::{
    MarketEvent, MarketLevel, MarketSnapshot, Price, PublicTrade, PublicTradeId,
};
use venue_gateway_api::GatewayBinding;

use crate::{
    HyperliquidConfig, HyperliquidError, HyperliquidGatewayBinding,
    models::{BookData, EventEnvelope},
    parse_public_trades,
    protocol::resolve_perp_meta,
    public::parse_1m_candle,
};

const CONNECT_CAP: Duration = Duration::from_secs(10);
const HEARTBEAT_AFTER: Duration = Duration::from_secs(50);
const HEARTBEAT_REPLY_CAP: Duration = Duration::from_secs(10);
const SOCKET_WRITE_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_TRACKED_FACTS: usize = 1_024;
const MAX_STARTUP_FRAMES: usize = 64;

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

struct CapturedFrame {
    received_at_ms: u64,
    payload: String,
}

/// A bounded read-only receiver for one actual Hyperliquid `BASE/USDC` perpetual.
///
/// The receiver owns neither account credentials nor mutation capability.  A fatal error fences
/// the instance, so a caller must create a fresh receiver rather than consume an old generation.
pub struct HyperliquidScalpingPublicReceiver {
    binding: GatewayBinding,
    native_coin: String,
    socket: Socket,
    generation: u64,
    max_body_bytes: usize,
    heartbeat_after: Duration,
    last_sent_at: tokio::time::Instant,
    heartbeat_deadline: Option<tokio::time::Instant>,
    last_snapshot: Option<(u64, MarketSnapshot)>,
    startup_frames: VecDeque<CapturedFrame>,
    trades: BTreeMap<PublicTradeId, PublicTrade>,
    trade_order: VecDeque<PublicTradeId>,
    failed: bool,
}

impl HyperliquidScalpingPublicReceiver {
    /// Connects only to Hyperliquid mainnet, subscribes to its documented `l2Book` channel, and
    /// waits for the exact subscription acknowledgement within one capped total connection budget.
    pub async fn connect(
        binding: GatewayBinding,
        connect_timeout: Duration,
        max_body_bytes: usize,
    ) -> Result<Self, HyperliquidPublicWsError> {
        let gateway = HyperliquidGatewayBinding::new(binding.clone())
            .map_err(|_| HyperliquidPublicWsError::Binding)?;
        if max_body_bytes == 0 {
            return Err(HyperliquidPublicWsError::BodyLimit);
        }
        Self::connect_to(
            binding,
            HyperliquidConfig::for_binding(&gateway).websocket(),
            connect_timeout,
            max_body_bytes,
            HEARTBEAT_AFTER,
        )
        .await
    }

    #[cfg(test)]
    async fn connect_for_test(
        binding: GatewayBinding,
        endpoint: &str,
        connect_timeout: Duration,
        max_body_bytes: usize,
        heartbeat_after: Duration,
    ) -> Result<Self, HyperliquidPublicWsError> {
        HyperliquidGatewayBinding::new(binding.clone())
            .map_err(|_| HyperliquidPublicWsError::Binding)?;
        Self::connect_to(
            binding,
            endpoint,
            connect_timeout,
            max_body_bytes,
            heartbeat_after,
        )
        .await
    }

    async fn connect_to(
        binding: GatewayBinding,
        endpoint: &str,
        connect_timeout: Duration,
        max_body_bytes: usize,
        heartbeat_after: Duration,
    ) -> Result<Self, HyperliquidPublicWsError> {
        if max_body_bytes == 0 || heartbeat_after.is_zero() {
            return Err(HyperliquidPublicWsError::BodyLimit);
        }
        let budget = connect_timeout.min(CONNECT_CAP);
        if budget.is_zero() {
            return Err(HyperliquidPublicWsError::Timeout);
        }
        let websocket = WebSocketConfig::default()
            .max_message_size(Some(max_body_bytes))
            .max_frame_size(Some(max_body_bytes));
        let generation = now_ms()?;
        let (socket, _, native_coin, startup_frames) = timeout(budget, async {
            let (mut socket, response) = connect_async_with_config(endpoint, Some(websocket), true)
                .await
                .map_err(|_| HyperliquidPublicWsError::Disconnected)?;
            let metadata_request = metadata_request()?;
            socket
                .send(Message::Text(metadata_request.into()))
                .await
                .map_err(|_| HyperliquidPublicWsError::Disconnected)?;
            let metadata = socket
                .next()
                .await
                .ok_or(HyperliquidPublicWsError::Disconnected)?
                .map_err(|_| HyperliquidPublicWsError::Disconnected)?;
            let native_coin = validate_metadata(metadata, &binding, max_body_bytes)?;
            for kind in ["l2Book", "trades", "candle"] {
                let request = subscription(&native_coin, kind)?;
                socket
                    .send(Message::Text(request.into()))
                    .await
                    .map_err(|_| HyperliquidPublicWsError::Disconnected)?;
            }
            let mut acknowledged = BTreeSet::new();
            let mut startup_frames = VecDeque::new();
            while acknowledged.len() < 3 {
                let frame = socket
                    .next()
                    .await
                    .ok_or(HyperliquidPublicWsError::Disconnected)?
                    .map_err(|_| HyperliquidPublicWsError::Disconnected)?;
                match frame {
                    Message::Ping(value) => socket
                        .send(Message::Pong(value))
                        .await
                        .map_err(|_| HyperliquidPublicWsError::Disconnected)?,
                    Message::Pong(_) => {}
                    Message::Close(_) => return Err(HyperliquidPublicWsError::Disconnected),
                    Message::Text(value) => capture_startup_frame(
                        &mut startup_frames,
                        &mut acknowledged,
                        value.to_string(),
                        &native_coin,
                        max_body_bytes,
                    )?,
                    Message::Binary(value) => {
                        let payload = String::from_utf8(value.to_vec())
                            .map_err(|_| HyperliquidPublicWsError::Protocol)?;
                        capture_startup_frame(
                            &mut startup_frames,
                            &mut acknowledged,
                            payload,
                            &native_coin,
                            max_body_bytes,
                        )?;
                    }
                    _ => return Err(HyperliquidPublicWsError::Protocol),
                }
            }
            Ok::<_, HyperliquidPublicWsError>((socket, response, native_coin, startup_frames))
        })
        .await
        .map_err(|_| HyperliquidPublicWsError::Timeout)??;
        Ok(Self {
            binding,
            native_coin,
            socket,
            generation,
            max_body_bytes,
            heartbeat_after,
            last_sent_at: tokio::time::Instant::now(),
            heartbeat_deadline: None,
            last_snapshot: None,
            startup_frames,
            trades: BTreeMap::new(),
            trade_order: VecDeque::new(),
            failed: false,
        })
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Receives at most one bounded normalized batch. Idle polls, subscription acknowledgements,
    /// and websocket/application pongs produce an empty vector. Any other invalid, closed, or rejected
    /// input terminally fences this receiver.
    pub async fn next(
        &mut self,
        wait: Duration,
    ) -> Result<Vec<(u64, MarketEvent)>, HyperliquidPublicWsError> {
        if self.failed {
            return Err(HyperliquidPublicWsError::Fenced);
        }
        if let Some(frame) = self.startup_frames.pop_front() {
            return self.consume_text(frame.payload, frame.received_at_ms);
        }
        if self.heartbeat_deadline.is_none() && self.last_sent_at.elapsed() >= self.heartbeat_after
        {
            self.send_heartbeat().await?;
        }
        let heartbeat_deadline = self.heartbeat_deadline;
        let bounded_wait = if let Some(deadline) = heartbeat_deadline {
            let Some(remaining) = deadline.checked_duration_since(tokio::time::Instant::now())
            else {
                return self.fail(HyperliquidPublicWsError::Timeout);
            };
            wait.min(remaining)
        } else {
            wait.min(
                self.heartbeat_after
                    .saturating_sub(self.last_sent_at.elapsed()),
            )
        };
        let frame = match timeout(bounded_wait, self.socket.next()).await {
            Err(_) => {
                if heartbeat_deadline
                    .is_some_and(|deadline| deadline <= tokio::time::Instant::now())
                {
                    return self.fail(HyperliquidPublicWsError::Timeout);
                }
                if heartbeat_deadline.is_none()
                    && self.last_sent_at.elapsed() >= self.heartbeat_after
                {
                    self.send_heartbeat().await?;
                }
                return Ok(Vec::new());
            }
            Ok(None) => return self.fail(HyperliquidPublicWsError::Disconnected),
            Ok(Some(Err(_))) => return self.fail(HyperliquidPublicWsError::Disconnected),
            Ok(Some(Ok(frame))) => frame,
        };
        let received_at_ms = match now_ms() {
            Ok(value) => value,
            Err(error) => return self.fail(error),
        };
        match frame {
            Message::Text(value) => self.consume_text(value.to_string(), received_at_ms),
            Message::Binary(value) => match String::from_utf8(value.to_vec()) {
                Ok(value) => self.consume_text(value, received_at_ms),
                Err(_) => self.fail(HyperliquidPublicWsError::Protocol),
            },
            Message::Ping(value) => {
                match timeout(SOCKET_WRITE_TIMEOUT, self.socket.send(Message::Pong(value))).await {
                    Err(_) => return self.fail(HyperliquidPublicWsError::Timeout),
                    Ok(Err(_)) => return self.fail(HyperliquidPublicWsError::Disconnected),
                    Ok(Ok(())) => {}
                }
                self.last_sent_at = tokio::time::Instant::now();
                Ok(Vec::new())
            }
            Message::Pong(_) => Ok(Vec::new()),
            Message::Close(_) => self.fail(HyperliquidPublicWsError::Disconnected),
            _ => self.fail(HyperliquidPublicWsError::Protocol),
        }
    }

    async fn send_heartbeat(&mut self) -> Result<(), HyperliquidPublicWsError> {
        match timeout(
            SOCKET_WRITE_TIMEOUT,
            self.socket
                .send(Message::Text(r#"{"method":"ping"}"#.into())),
        )
        .await
        {
            Err(_) => return self.fail(HyperliquidPublicWsError::Timeout),
            Ok(Err(_)) => return self.fail(HyperliquidPublicWsError::Disconnected),
            Ok(Ok(())) => {}
        }
        self.last_sent_at = tokio::time::Instant::now();
        self.heartbeat_deadline =
            Some(self.last_sent_at + self.heartbeat_after.min(HEARTBEAT_REPLY_CAP));
        Ok(())
    }

    fn consume_text(
        &mut self,
        payload: String,
        received_at_ms: u64,
    ) -> Result<Vec<(u64, MarketEvent)>, HyperliquidPublicWsError> {
        if payload.len() > self.max_body_bytes {
            return self.fail(HyperliquidPublicWsError::BodyTooLarge);
        }
        if is_exact_pong(&payload) {
            self.heartbeat_deadline = None;
            return Ok(Vec::new());
        }
        if ["l2Book", "trades", "candle"]
            .into_iter()
            .any(|kind| is_subscription_ack(&payload, &self.native_coin, kind))
        {
            return Ok(Vec::new());
        }
        if is_venue_error(&payload) {
            return self.fail(HyperliquidPublicWsError::Rejected);
        }
        if let Ok(trades) = parse_public_trades(
            &payload,
            &self.binding,
            &self.native_coin,
            self.generation,
            received_at_ms,
        ) {
            return self.accept_trades(trades);
        }
        if parse_1m_candle(
            &payload,
            &self.binding,
            &self.native_coin,
            self.generation,
            received_at_ms,
        )
        .is_ok()
        {
            // The public candle stream has no closure flag. Do not publish a forming candle
            // until an authoritative candleSnapshot path confirms this exact 1m bucket.
            return Ok(Vec::new());
        }
        let snapshot =
            match parse_snapshot(&payload, &self.binding, &self.native_coin, self.generation) {
                Ok(snapshot) => snapshot,
                Err(error) => return self.fail(error),
            };
        let watermark = snapshot
            .exchange_time_ms
            .ok_or(HyperliquidPublicWsError::Protocol)?;
        if let Some((prior_watermark, prior)) = &self.last_snapshot {
            if watermark < *prior_watermark || (watermark == *prior_watermark && prior != &snapshot)
            {
                return self.fail(HyperliquidPublicWsError::Watermark);
            }
            if watermark == *prior_watermark {
                return Ok(Vec::new());
            }
        }
        self.last_snapshot = Some((watermark, snapshot.clone()));
        Ok(vec![(received_at_ms, MarketEvent::Snapshot(snapshot))])
    }

    fn fail<T>(&mut self, error: HyperliquidPublicWsError) -> Result<T, HyperliquidPublicWsError> {
        self.failed = true;
        Err(error)
    }

    fn accept_trades(
        &mut self,
        trades: Vec<PublicTrade>,
    ) -> Result<Vec<(u64, MarketEvent)>, HyperliquidPublicWsError> {
        let mut events = Vec::with_capacity(trades.len());
        for trade in trades {
            if let Some(previous) = self.trades.get(&trade.aggregate_trade_id) {
                if same_trade(previous, &trade) {
                    continue;
                }
                return self.fail(HyperliquidPublicWsError::Watermark);
            }
            let id = trade.aggregate_trade_id.clone();
            let received = trade.received_at_ms;
            let _ = self.trades.insert(id.clone(), trade.clone());
            self.trade_order.push_back(id);
            if self.trade_order.len() > MAX_TRACKED_FACTS {
                if let Some(oldest) = self.trade_order.pop_front() {
                    let _ = self.trades.remove(&oldest);
                }
            }
            events.push((received, MarketEvent::Trade(trade)));
        }
        Ok(events)
    }
}

fn subscription(native_coin: &str, kind: &str) -> Result<String, HyperliquidPublicWsError> {
    if native_coin.is_empty()
        || native_coin.contains(['/', ':', '@'])
        || !matches!(kind, "l2Book" | "trades" | "candle")
    {
        return Err(HyperliquidPublicWsError::Binding);
    }
    serde_json::to_string(&serde_json::json!({
        "method": "subscribe",
        "subscription": if kind == "candle" { serde_json::json!({"type": kind, "coin": native_coin, "interval": "1m"}) } else { serde_json::json!({"type": kind, "coin": native_coin}) },
    }))
    .map_err(|_| HyperliquidPublicWsError::Protocol)
}

fn metadata_request() -> Result<String, HyperliquidPublicWsError> {
    serde_json::to_string(&serde_json::json!({
        "method": "post",
        "id": 1,
        "request": {"type": "info", "payload": {"type": "meta"}},
    }))
    .map_err(|_| HyperliquidPublicWsError::Protocol)
}

fn validate_metadata(
    frame: Message,
    binding: &GatewayBinding,
    max_body_bytes: usize,
) -> Result<String, HyperliquidPublicWsError> {
    let payload = frame_text(frame, max_body_bytes)?;
    if is_venue_error(&payload) {
        return Err(HyperliquidPublicWsError::Rejected);
    }
    let envelope: MetadataEnvelope =
        serde_json::from_str(&payload).map_err(|_| HyperliquidPublicWsError::Protocol)?;
    if envelope.channel != "post" || envelope.data.id != 1 {
        return Err(HyperliquidPublicWsError::Protocol);
    }
    if envelope.data.response.kind == "error" {
        return Err(HyperliquidPublicWsError::Rejected);
    }
    if envelope.data.response.kind != "info" {
        return Err(HyperliquidPublicWsError::Protocol);
    }
    let raw = serde_json::to_vec(&envelope.data.response.payload)
        .map_err(|_| HyperliquidPublicWsError::Protocol)?;
    let metadata = resolve_perp_meta(&raw, binding.symbol.clone()).map_err(map_meta_error)?;
    if !metadata.trading_enabled {
        return Err(HyperliquidPublicWsError::Binding);
    }
    Ok(metadata.native_coin)
}

fn capture_startup_frame(
    startup_frames: &mut VecDeque<CapturedFrame>,
    acknowledged: &mut BTreeSet<&'static str>,
    payload: String,
    native_coin: &str,
    max_body_bytes: usize,
) -> Result<(), HyperliquidPublicWsError> {
    if payload.len() > max_body_bytes {
        return Err(HyperliquidPublicWsError::BodyTooLarge);
    }
    if is_venue_error(&payload) {
        return Err(HyperliquidPublicWsError::Rejected);
    }
    if let Some(kind) = ["l2Book", "trades", "candle"]
        .into_iter()
        .find(|kind| is_subscription_ack(&payload, native_coin, kind))
    {
        return acknowledged
            .insert(kind)
            .then_some(())
            .ok_or(HyperliquidPublicWsError::Protocol);
    }
    if is_subscription_response(&payload) {
        return Err(HyperliquidPublicWsError::Protocol);
    }
    if startup_frames.len() >= MAX_STARTUP_FRAMES {
        return Err(HyperliquidPublicWsError::StartupOverflow);
    }
    startup_frames.push_back(CapturedFrame {
        received_at_ms: now_ms()?,
        payload,
    });
    Ok(())
}

fn frame_text(frame: Message, max_body_bytes: usize) -> Result<String, HyperliquidPublicWsError> {
    let payload = match frame {
        Message::Text(value) => value.to_string(),
        Message::Binary(value) => {
            String::from_utf8(value.to_vec()).map_err(|_| HyperliquidPublicWsError::Protocol)?
        }
        Message::Ping(_) | Message::Pong(_) => return Err(HyperliquidPublicWsError::Protocol),
        Message::Close(_) => return Err(HyperliquidPublicWsError::Disconnected),
        _ => return Err(HyperliquidPublicWsError::Protocol),
    };
    if payload.len() > max_body_bytes {
        return Err(HyperliquidPublicWsError::BodyTooLarge);
    }
    Ok(payload)
}

fn map_meta_error(error: HyperliquidError) -> HyperliquidPublicWsError {
    match error {
        HyperliquidError::Binding => HyperliquidPublicWsError::Binding,
        _ => HyperliquidPublicWsError::Protocol,
    }
}

#[derive(serde::Deserialize)]
struct MetadataEnvelope {
    channel: String,
    data: MetadataData,
}

#[derive(serde::Deserialize)]
struct MetadataData {
    id: u64,
    response: MetadataResponse,
}

#[derive(serde::Deserialize)]
struct MetadataResponse {
    #[serde(rename = "type")]
    kind: String,
    payload: serde_json::Value,
}

fn is_subscription_ack(payload: &str, native_coin: &str, kind: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(payload)
        .ok()
        .is_some_and(|value| {
            value.get("channel").and_then(serde_json::Value::as_str) == Some("subscriptionResponse")
                && value
                    .get("data")
                    .and_then(serde_json::Value::as_object)
                    .is_some_and(|data| {
                        data.get("method").and_then(serde_json::Value::as_str) == Some("subscribe")
                            && data
                                .get("subscription")
                                .and_then(serde_json::Value::as_object)
                                .is_some_and(|subscription| {
                                    subscription.get("type").and_then(serde_json::Value::as_str)
                                        == Some(kind)
                                        && subscription
                                            .get("coin")
                                            .and_then(serde_json::Value::as_str)
                                            == Some(native_coin)
                                        && (kind != "candle"
                                            || subscription
                                                .get("interval")
                                                .and_then(serde_json::Value::as_str)
                                                == Some("1m"))
                                        && data.get("error").is_none_or(serde_json::Value::is_null)
                                })
                    })
        })
}

fn is_subscription_response(payload: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(payload)
        .ok()
        .is_some_and(|value| {
            value.get("channel").and_then(serde_json::Value::as_str) == Some("subscriptionResponse")
        })
}

fn is_venue_error(payload: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(payload)
        .ok()
        .is_some_and(|value| {
            value.get("channel").and_then(serde_json::Value::as_str) == Some("error")
                || value.get("error").is_some_and(|error| !error.is_null())
                || value
                    .get("data")
                    .and_then(serde_json::Value::as_object)
                    .and_then(|data| data.get("error"))
                    .is_some_and(|error| !error.is_null())
        })
}

fn is_exact_pong(payload: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(payload)
        .ok()
        .is_some_and(|value| {
            value.as_object().is_some_and(|object| {
                object.len() == 1
                    && object.get("channel").and_then(serde_json::Value::as_str) == Some("pong")
            })
        })
}

fn parse_snapshot(
    payload: &str,
    binding: &GatewayBinding,
    native_coin: &str,
    generation: u64,
) -> Result<MarketSnapshot, HyperliquidPublicWsError> {
    let envelope: EventEnvelope =
        serde_json::from_str(payload).map_err(|_| HyperliquidPublicWsError::Protocol)?;
    if envelope.channel != "l2Book" {
        return Err(HyperliquidPublicWsError::Protocol);
    }
    let data: BookData =
        serde_json::from_value(envelope.data).map_err(|_| HyperliquidPublicWsError::Protocol)?;
    if data.coin != native_coin || data.time == 0 {
        return Err(HyperliquidPublicWsError::Protocol);
    }
    if data.levels[0]
        .len()
        .checked_add(data.levels[1].len())
        .is_none_or(|count| count > MAX_TRACKED_FACTS)
    {
        return Err(HyperliquidPublicWsError::Protocol);
    }
    let bids = normalize_side(data.levels[0].iter(), true)?;
    let asks = normalize_side(data.levels[1].iter(), false)?;
    if bids
        .first()
        .zip(asks.first())
        .is_none_or(|(bid, ask)| bid.price >= ask.price)
    {
        return Err(HyperliquidPublicWsError::Protocol);
    }
    Ok(MarketSnapshot {
        symbol: binding.symbol.clone(),
        generation,
        // This is a source-time snapshot watermark, not a venue incremental sequence.
        sequence: data.time,
        exchange_time_ms: Some(data.time),
        bids,
        asks,
    })
}

fn normalize_side<'a>(
    levels: impl Iterator<Item = &'a crate::models::BookLevel>,
    descending: bool,
) -> Result<Vec<MarketLevel>, HyperliquidPublicWsError> {
    let mut seen = BTreeSet::new();
    let mut normalized = Vec::new();
    for level in levels {
        let price = Price::new(
            level
                .px
                .parse::<Decimal>()
                .map_err(|_| HyperliquidPublicWsError::Protocol)?,
        )
        .map_err(|_| HyperliquidPublicWsError::Protocol)?;
        let quantity = level
            .sz
            .parse::<Decimal>()
            .map_err(|_| HyperliquidPublicWsError::Protocol)?;
        if level.n == 0 || quantity <= Decimal::ZERO || !seen.insert(price) {
            return Err(HyperliquidPublicWsError::Protocol);
        }
        normalized.push(MarketLevel { price, quantity });
    }
    if normalized.is_empty() {
        return Err(HyperliquidPublicWsError::Protocol);
    }
    if descending {
        normalized.sort_by_key(|level| Reverse(level.price));
    } else {
        normalized.sort_by_key(|level| level.price);
    }
    Ok(normalized)
}

fn now_ms() -> Result<u64, HyperliquidPublicWsError> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| HyperliquidPublicWsError::Clock)?
            .as_millis(),
    )
    .map_err(|_| HyperliquidPublicWsError::Clock)
}

fn same_trade(left: &PublicTrade, right: &PublicTrade) -> bool {
    left.symbol == right.symbol
        && left.generation == right.generation
        && left.exchange_time_ms == right.exchange_time_ms
        && left.transaction_time_ms == right.transaction_time_ms
        && left.aggregate_trade_id == right.aggregate_trade_id
        && left.first_trade_id == right.first_trade_id
        && left.last_trade_id == right.last_trade_id
        && left.ordering == right.ordering
        && left.price == right.price
        && left.quantity == right.quantity
        && left.quote_quantity == right.quote_quantity
        && left.aggressor == right.aggressor
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum HyperliquidPublicWsError {
    #[error("Hyperliquid public receiver binding is not a LIVE BASE/USDC perpetual")]
    Binding,
    #[error("Hyperliquid public receiver message limit is invalid")]
    BodyLimit,
    #[error("Hyperliquid public receiver message exceeded its configured limit")]
    BodyTooLarge,
    #[error("Hyperliquid public receiver clock is invalid")]
    Clock,
    #[error("Hyperliquid public receiver stream disconnected")]
    Disconnected,
    #[error("Hyperliquid public receiver cannot be reused after a fatal frame")]
    Fenced,
    #[error("Hyperliquid public receiver frame or subscription is invalid")]
    Protocol,
    #[error("Hyperliquid public subscription was rejected")]
    Rejected,
    #[error("Hyperliquid public receiver startup buffer overflowed")]
    StartupOverflow,
    #[error("Hyperliquid public receiver timed out")]
    Timeout,
    #[error("Hyperliquid snapshot watermark regressed or conflicted")]
    Watermark,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{net::TcpListener, task::JoinHandle};
    use tokio_tungstenite::accept_async;
    use venue_gateway_api::{GatewayMode, VenueId};

    const L2_BOOK: &str = include_str!("../fixtures/public-l2-book-ws.json");
    const META: &str = r#"{"universe":[{"name":"BTC","szDecimals":5,"maxLeverage":50}]}"#;

    fn binding(symbol: &str) -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Hyperliquid,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            symbol.parse()?,
        )?)
    }

    async fn server(
        frames: Vec<Message>,
    ) -> Result<(String, JoinHandle<()>), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let respond_heartbeat = !frames.is_empty();
        let task = tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let Ok(mut socket) = accept_async(stream).await else {
                return;
            };
            let Some(Ok(Message::Text(metadata_request))) = socket.next().await else {
                return;
            };
            let Ok(metadata_value) =
                serde_json::from_str::<serde_json::Value>(metadata_request.as_str())
            else {
                return;
            };
            if metadata_value
                .pointer("/request/payload/type")
                .and_then(serde_json::Value::as_str)
                != Some("meta")
            {
                return;
            }
            let metadata = serde_json::json!({"channel":"post","data":{"id":1,"response":{"type":"info","payload":serde_json::from_str::<serde_json::Value>(META).ok()}}});
            if socket
                .send(Message::Text(metadata.to_string().into()))
                .await
                .is_err()
            {
                return;
            }
            for kind in ["l2Book", "trades", "candle"] {
                let Some(Ok(Message::Text(request))) = socket.next().await else {
                    return;
                };
                let Ok(value) = serde_json::from_str::<serde_json::Value>(request.as_str()) else {
                    return;
                };
                let coin = value
                    .pointer("/subscription/coin")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("BTC");
                if value
                    .pointer("/subscription/type")
                    .and_then(serde_json::Value::as_str)
                    != Some(kind)
                {
                    return;
                }
                let mut subscription = serde_json::json!({"type":kind,"coin":coin});
                if kind == "candle" {
                    subscription["interval"] = serde_json::Value::String("1m".to_owned());
                }
                let ack = serde_json::json!({"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":subscription}});
                if socket
                    .send(Message::Text(ack.to_string().into()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            for frame in frames {
                if socket.send(frame).await.is_err() {
                    return;
                }
            }
            let Ok(Some(Ok(Message::Text(ping)))) =
                tokio::time::timeout(Duration::from_millis(100), socket.next()).await
            else {
                return;
            };
            if respond_heartbeat && ping.as_str() == r#"{"method":"ping"}"# {
                let _ = socket
                    .send(Message::Text(r#"{"channel":"pong"}"#.into()))
                    .await;
            } else if !respond_heartbeat {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });
        Ok((format!("ws://{address}"), task))
    }

    fn frame(time: u64) -> Message {
        Message::Text(
            L2_BOOK
                .replace("\"time\": 1000", &format!("\"time\": {time}"))
                .into(),
        )
    }

    #[test]
    fn book_rejects_more_than_1024_levels_per_frame() -> Result<(), Box<dyn std::error::Error>> {
        let level = serde_json::json!({"px":"100","sz":"1","n":1});
        let payload = serde_json::json!({"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[vec![level; 1025], [serde_json::json!({"px":"101","sz":"1","n":1})]]}}).to_string();
        assert_eq!(
            parse_snapshot(&payload, &binding("BTC/USDC")?, "BTC", 1),
            Err(HyperliquidPublicWsError::Protocol)
        );
        Ok(())
    }

    #[tokio::test]
    async fn startup_buffers_interleaved_public_data_until_all_exact_acks()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        let task = tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let Ok(mut socket) = accept_async(stream).await else {
                return;
            };
            let Some(Ok(Message::Text(_))) = socket.next().await else {
                return;
            };
            let metadata = serde_json::json!({"channel":"post","data":{"id":1,"response":{"type":"info","payload":serde_json::from_str::<serde_json::Value>(META).ok()}}});
            if socket
                .send(Message::Text(metadata.to_string().into()))
                .await
                .is_err()
            {
                return;
            }
            for expected in ["l2Book", "trades", "candle"] {
                let Some(Ok(Message::Text(request))) = socket.next().await else {
                    return;
                };
                let Ok(value) = serde_json::from_str::<serde_json::Value>(request.as_str()) else {
                    return;
                };
                if value
                    .pointer("/subscription/type")
                    .and_then(serde_json::Value::as_str)
                    != Some(expected)
                {
                    return;
                }
            }
            let ack = |kind: &str| {
                let mut subscription = serde_json::json!({"type":kind,"coin":"BTC"});
                if kind == "candle" {
                    subscription["interval"] = serde_json::Value::String("1m".to_owned());
                }
                Message::Text(serde_json::json!({"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":subscription}}).to_string().into())
            };
            for message in [
                frame(1000), ack("l2Book"),
                Message::Text(r#"{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"100","sz":"1","time":1001,"tid":7}]}"#.into()), ack("trades"),
                Message::Text(include_str!("../fixtures/public-candle-1m-ws.json").into()), ack("candle"),
            ] {
                if socket.send(message).await.is_err() { return; }
            }
        });
        let mut receiver = HyperliquidScalpingPublicReceiver::connect_for_test(
            binding("BTC/USDC")?,
            &endpoint,
            Duration::from_secs(1),
            4096,
            Duration::from_secs(1),
        )
        .await?;
        assert_eq!(receiver.startup_frames.len(), 3);
        let first_received = receiver
            .startup_frames
            .front()
            .ok_or("startup")?
            .received_at_ms;
        let first = receiver.next(Duration::from_millis(1)).await?;
        assert_eq!(first.first().map(|event| event.0), Some(first_received));
        assert!(matches!(
            first.first().map(|event| &event.1),
            Some(MarketEvent::Snapshot(_))
        ));
        assert!(matches!(
            receiver
                .next(Duration::from_millis(1))
                .await?
                .first()
                .map(|event| &event.1),
            Some(MarketEvent::Trade(_))
        ));
        assert!(receiver.next(Duration::from_millis(1)).await?.is_empty());
        task.await?;
        Ok(())
    }

    #[tokio::test]
    async fn receiver_emits_complete_snapshot_and_ignores_ack_pong_and_duplicate()
    -> Result<(), Box<dyn std::error::Error>> {
        let duplicate = frame(1000);
        let (endpoint, task) = server(vec![
            Message::Text(r#"{"channel":"pong"}"#.into()),
            frame(1000),
            duplicate,
        ])
        .await?;
        let mut receiver = HyperliquidScalpingPublicReceiver::connect_for_test(
            binding("BTC/USDC")?,
            &endpoint,
            Duration::from_secs(1),
            4096,
            Duration::from_millis(1),
        )
        .await?;
        assert_eq!(receiver.next(Duration::from_millis(50)).await?, Vec::new());
        let Some((received, MarketEvent::Snapshot(snapshot))) = receiver
            .next(Duration::from_millis(50))
            .await?
            .into_iter()
            .next()
        else {
            return Err("snapshot".into());
        };
        assert!(received > 0);
        assert_eq!(snapshot.sequence, 1000);
        assert_eq!(snapshot.bids.len(), 2);
        assert_eq!(snapshot.asks.len(), 2);
        assert_eq!(receiver.next(Duration::from_millis(50)).await?, Vec::new());
        assert_eq!(receiver.next(Duration::from_millis(1)).await?, Vec::new());
        assert_eq!(receiver.next(Duration::from_millis(50)).await?, Vec::new());
        task.await?;
        Ok(())
    }

    #[tokio::test]
    async fn heartbeat_reply_has_a_single_deadline_without_repeated_pings()
    -> Result<(), Box<dyn std::error::Error>> {
        let (endpoint, task) = server(Vec::new()).await?;
        let mut receiver = HyperliquidScalpingPublicReceiver::connect_for_test(
            binding("BTC/USDC")?,
            &endpoint,
            Duration::from_secs(1),
            4096,
            Duration::from_millis(30),
        )
        .await?;
        assert_eq!(receiver.next(Duration::from_millis(100)).await?, Vec::new());
        assert_eq!(receiver.next(Duration::from_millis(1)).await?, Vec::new());
        assert_eq!(
            receiver.next(Duration::from_millis(20)).await,
            Err(HyperliquidPublicWsError::Timeout)
        );
        assert_eq!(
            receiver.next(Duration::from_millis(1)).await,
            Err(HyperliquidPublicWsError::Fenced)
        );
        task.await?;
        Ok(())
    }

    #[tokio::test]
    async fn receiver_rejects_bad_ack_and_subscription_error()
    -> Result<(), Box<dyn std::error::Error>> {
        async fn one(
            first: Message,
        ) -> Result<(String, JoinHandle<()>), Box<dyn std::error::Error>> {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let task = tokio::spawn(async move {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let Ok(mut socket) = accept_async(stream).await else {
                    return;
                };
                let _ = socket.next().await;
                let metadata = serde_json::json!({"channel":"post","data":{"id":1,"response":{"type":"info","payload":serde_json::from_str::<serde_json::Value>(META).ok()}}});
                if socket
                    .send(Message::Text(metadata.to_string().into()))
                    .await
                    .is_err()
                {
                    return;
                }
                let _ = socket.next().await;
                let _ = socket.send(first).await;
            });
            Ok((format!("ws://{address}"), task))
        }
        let (endpoint, task) = one(Message::Text(r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"ETH"}}}"#.into())).await?;
        assert!(matches!(
            HyperliquidScalpingPublicReceiver::connect_for_test(
                binding("BTC/USDC")?,
                &endpoint,
                Duration::from_secs(1),
                4096,
                Duration::from_secs(1)
            )
            .await,
            Err(HyperliquidPublicWsError::Protocol)
        ));
        task.await?;
        let (endpoint, task) = one(Message::Text(
            r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"},"error":"rejected"}}"#.into(),
        ))
        .await?;
        assert!(matches!(
            HyperliquidScalpingPublicReceiver::connect_for_test(
                binding("BTC/USDC")?,
                &endpoint,
                Duration::from_secs(1),
                4096,
                Duration::from_secs(1)
            )
            .await,
            Err(HyperliquidPublicWsError::Rejected)
        ));
        task.await?;
        Ok(())
    }

    #[tokio::test]
    async fn receiver_fences_regressing_conflicting_and_malformed_books()
    -> Result<(), Box<dyn std::error::Error>> {
        let conflict = Message::Text(serde_json::json!({"channel":"l2Book","data":{"coin":"BTC","time":1000,"levels":[[{"px":"100","sz":"2","n":1}],[{"px":"102","sz":"4","n":1}]]}}).to_string().into());
        let (endpoint, task) = server(vec![frame(1000), conflict]).await?;
        let mut receiver = HyperliquidScalpingPublicReceiver::connect_for_test(
            binding("BTC/USDC")?,
            &endpoint,
            Duration::from_secs(1),
            4096,
            Duration::from_secs(1),
        )
        .await?;
        assert!(!receiver.next(Duration::from_millis(50)).await?.is_empty());
        assert_eq!(
            receiver.next(Duration::from_millis(50)).await,
            Err(HyperliquidPublicWsError::Watermark)
        );
        assert_eq!(
            receiver.next(Duration::from_millis(50)).await,
            Err(HyperliquidPublicWsError::Fenced)
        );
        task.await?;

        for payload in [
            r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[],[{"px":"101","sz":"1","n":1}]]}}"#,
            r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[{"px":"100","sz":"1","n":1},{"px":"100","sz":"1","n":1}],[{"px":"101","sz":"1","n":1}]]}}"#,
            r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[{"px":"101","sz":"1","n":1}],[{"px":"100","sz":"1","n":1}]]}}"#,
            r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[{"px":"100","sz":"0","n":1}],[{"px":"101","sz":"1","n":1}]]}}"#,
        ] {
            let (endpoint, task) = server(vec![Message::Text(payload.into())]).await?;
            let mut receiver = HyperliquidScalpingPublicReceiver::connect_for_test(
                binding("BTC/USDC")?,
                &endpoint,
                Duration::from_secs(1),
                4096,
                Duration::from_secs(1),
            )
            .await?;
            assert_eq!(
                receiver.next(Duration::from_millis(50)).await,
                Err(HyperliquidPublicWsError::Protocol)
            );
            task.await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn receiver_enforces_message_limit_and_connect_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let (endpoint, task) = server(vec![Message::Text("x".repeat(512).into())]).await?;
        let mut receiver = HyperliquidScalpingPublicReceiver::connect_for_test(
            binding("BTC/USDC")?,
            &endpoint,
            Duration::from_secs(1),
            256,
            Duration::from_secs(1),
        )
        .await?;
        assert!(matches!(
            receiver.next(Duration::from_millis(50)).await,
            Err(HyperliquidPublicWsError::BodyTooLarge | HyperliquidPublicWsError::Disconnected)
        ));
        task.await?;

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        let task = tokio::spawn(async move {
            let Ok((_stream, _)) = listener.accept().await else {
                return;
            };
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        assert!(matches!(
            HyperliquidScalpingPublicReceiver::connect_for_test(
                binding("BTC/USDC")?,
                &endpoint,
                Duration::from_millis(20),
                4096,
                Duration::from_secs(1)
            )
            .await,
            Err(HyperliquidPublicWsError::Timeout)
        ));
        task.await?;
        Ok(())
    }
}
