//! Credential-free, continuous OKX `books` receiver for the scalping resident.
//!
//! `books` is the public 100 ms incremental channel.  Its `checksum` is deliberately ignored:
//! OKX fixed it to zero in June 2026, so continuity is proved only by `seqId`/`prevSeqId`.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use thiserror::Error;
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use venue_domain::domain::{MarketDelta, MarketEvent, MarketLevel, MarketSnapshot, Price};
use venue_gateway_api::{GatewayBinding, VenueId};

use crate::{
    OkxConfig, OkxError, OkxHttpTransport, OkxInstrument, OkxTransportError,
    parse_closed_1m_candle, parse_instrument, parse_trades,
};

const MAX_BOOK_LEVELS: usize = 400;
const MAX_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HEARTBEAT_IDLE: Duration = Duration::from_secs(25);
const HEARTBEAT_PONG_DEADLINE: Duration = Duration::from_secs(5);
const HEARTBEAT_SEND_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_TRACKED_TRADES: usize = 1_024;
static LAST_PUBLIC_GENERATION: AtomicU64 = AtomicU64::new(0);

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// One bounded public connection for exactly one OKX LIVE linear perpetual. It owns neither
/// credentials nor any mutation handle. A protocol failure is terminal for this instance.
pub struct OkxScalpingPublicReceiver {
    instrument: OkxInstrument,
    generation: u64,
    public_socket: Socket,
    business_socket: Socket,
    connect_timeout: Duration,
    book: BookBridge,
    public_heartbeat: HeartbeatState,
    business_heartbeat: HeartbeatState,
    next_socket: PublicSocket,
    bars: ClosedBarGuard,
    trades: TradeGuard,
    failed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicSocket {
    Public,
    Business,
}

impl OkxScalpingPublicReceiver {
    pub async fn connect(
        binding: GatewayBinding,
        connect_timeout: Duration,
        max_body_bytes: usize,
    ) -> Result<Self, OkxPublicWsError> {
        validate_limits(connect_timeout, max_body_bytes)?;
        timeout(
            connect_timeout,
            Self::connect_with_budget(binding, connect_timeout, max_body_bytes),
        )
        .await
        .map_err(|_| OkxPublicWsError::Timeout)?
    }

    async fn connect_with_budget(
        binding: GatewayBinding,
        connect_timeout: Duration,
        max_body_bytes: usize,
    ) -> Result<Self, OkxPublicWsError> {
        binding.validate().map_err(|_| OkxPublicWsError::Binding)?;
        if binding.venue != VenueId::Okx {
            return Err(OkxPublicWsError::Binding);
        }
        let config =
            OkxConfig::for_binding(binding.clone()).map_err(|_| OkxPublicWsError::Binding)?;
        let generation = next_generation()?;
        // Rules are public and are mandatory because OKX book sizes are contracts, not base coins.
        let transport = OkxHttpTransport::new(config.clone(), connect_timeout, max_body_bytes)
            .map_err(map_transport)?;
        let rules = transport
            .fetch_instrument(generation)
            .await
            .map_err(map_transport)?;
        if rules.binding != binding || rules.instrument_generation != generation {
            return Err(OkxPublicWsError::Binding);
        }
        let instrument = parse_instrument(&rules.body, &config, generation)
            .map_err(|_| OkxPublicWsError::Protocol)?;
        let websocket = WebSocketConfig::default()
            .max_message_size(Some(max_body_bytes))
            .max_frame_size(Some(max_body_bytes));
        let (mut public_socket, _) = timeout(
            connect_timeout,
            connect_async_with_config(config.public_ws(), Some(websocket), false),
        )
        .await
        .map_err(|_| OkxPublicWsError::Timeout)?
        .map_err(|_| OkxPublicWsError::Disconnected)?;
        let request = public_subscription(&instrument)?;
        send(
            &mut public_socket,
            Message::Text(request.into()),
            connect_timeout,
        )
        .await?;
        await_subscription_acks(
            &mut public_socket,
            &instrument,
            &["books", "trades"],
            connect_timeout,
            max_body_bytes,
        )
        .await?;
        let (mut business_socket, _) = timeout(
            connect_timeout,
            connect_async_with_config(config.business_ws(), Some(websocket), false),
        )
        .await
        .map_err(|_| OkxPublicWsError::Timeout)?
        .map_err(|_| OkxPublicWsError::Disconnected)?;
        let request = business_subscription(&instrument)?;
        send(
            &mut business_socket,
            Message::Text(request.into()),
            connect_timeout,
        )
        .await?;
        await_subscription_acks(
            &mut business_socket,
            &instrument,
            &["candle1m"],
            connect_timeout,
            max_body_bytes,
        )
        .await?;
        Ok(Self {
            instrument: instrument.clone(),
            generation,
            public_socket,
            business_socket,
            connect_timeout,
            book: BookBridge::new(instrument, generation)?,
            public_heartbeat: HeartbeatState::new(Instant::now()),
            business_heartbeat: HeartbeatState::new(Instant::now()),
            next_socket: PublicSocket::Public,
            bars: ClosedBarGuard::new(generation),
            trades: TradeGuard::default(),
            failed: false,
        })
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Delivers one fully bound public batch. The public and business sockets are alternate-first
    /// so sustained book traffic cannot starve the completed-bar feed.
    pub async fn next(
        &mut self,
        wait: Duration,
    ) -> Result<Vec<(u64, MarketEvent)>, OkxPublicWsError> {
        if self.failed {
            return Err(OkxPublicWsError::Terminal);
        }
        let socket = self.next_socket;
        self.next_socket = match socket {
            PublicSocket::Public => PublicSocket::Business,
            PublicSocket::Business => PublicSocket::Public,
        };
        let result = match socket {
            PublicSocket::Public => self.next_public(wait).await,
            PublicSocket::Business => self.next_business(wait).await,
        };
        match result {
            Ok(events) => Ok(events),
            Err(error) => self.fail(error),
        }
    }

    async fn next_public(
        &mut self,
        wait: Duration,
    ) -> Result<Vec<(u64, MarketEvent)>, OkxPublicWsError> {
        let Some(message) = receive(
            &mut self.public_socket,
            &mut self.public_heartbeat,
            wait,
            self.connect_timeout,
        )
        .await?
        else {
            return Ok(Vec::new());
        };
        let Message::Text(payload) = message else {
            return Err(OkxPublicWsError::Protocol);
        };
        let payload = payload.as_ref();
        if payload == "pong"
            || is_subscription_ack(payload, "books", self.instrument.native_id())
            || is_subscription_ack(payload, "trades", self.instrument.native_id())
        {
            return Ok(Vec::new());
        }
        if is_subscription_rejection(payload) {
            return Err(OkxPublicWsError::SubscriptionRejected);
        }
        let received_at_ms = now_ms()?;
        match stream_channel(payload)?.as_str() {
            "books" => self.book.accept(payload, received_at_ms).map(|event| {
                event
                    .into_iter()
                    .map(|event| (received_at_ms, event))
                    .collect()
            }),
            "trades" => parse_trades(payload.as_bytes(), &self.instrument, received_at_ms)
                .and_then(|events| self.trades.accept(events))
                .map(|events| {
                    events
                        .into_iter()
                        .map(|event| (received_at_ms, MarketEvent::Trade(event)))
                        .collect()
                })
                .map_err(|_| OkxPublicWsError::Protocol),
            _ => Err(OkxPublicWsError::Protocol),
        }
    }

    async fn next_business(
        &mut self,
        wait: Duration,
    ) -> Result<Vec<(u64, MarketEvent)>, OkxPublicWsError> {
        let Some(message) = receive(
            &mut self.business_socket,
            &mut self.business_heartbeat,
            wait,
            self.connect_timeout,
        )
        .await?
        else {
            return Ok(Vec::new());
        };
        let Message::Text(payload) = message else {
            return Err(OkxPublicWsError::Protocol);
        };
        let payload = payload.as_ref();
        if payload == "pong"
            || is_subscription_ack(payload, "candle1m", self.instrument.native_id())
        {
            return Ok(Vec::new());
        }
        if is_subscription_rejection(payload) {
            return Err(OkxPublicWsError::SubscriptionRejected);
        }
        if !is_closed_candle(payload, self.instrument.native_id())? {
            return Ok(Vec::new());
        }
        let received_at_ms = now_ms()?;
        let bar = parse_closed_1m_candle(payload.as_bytes(), &self.instrument, received_at_ms)
            .map_err(|_| OkxPublicWsError::Protocol)?;
        self.bars.accept(bar).map(|bar| {
            bar.into_iter()
                .map(|bar| (received_at_ms, MarketEvent::Bar(bar)))
                .collect()
        })
    }

    fn fail<T>(&mut self, error: OkxPublicWsError) -> Result<T, OkxPublicWsError> {
        self.failed = true;
        Err(error)
    }
}

/// The candle stream can replay the final bucket. Replays must not become extra strategy bars;
/// a changed payload for that bucket and a lower bucket are both terminal ordering failures.
struct ClosedBarGuard {
    generation: u64,
    latest: Option<venue_domain::domain::PublicBar>,
}

impl ClosedBarGuard {
    const fn new(generation: u64) -> Self {
        Self {
            generation,
            latest: None,
        }
    }

    fn accept(
        &mut self,
        bar: venue_domain::domain::PublicBar,
    ) -> Result<Option<venue_domain::domain::PublicBar>, OkxPublicWsError> {
        if bar.generation != self.generation {
            self.generation = bar.generation;
            self.latest = None;
        }
        let Some(previous) = self.latest.as_ref() else {
            self.latest = Some(bar.clone());
            return Ok(Some(bar));
        };
        if bar.sequence < previous.sequence {
            return Err(OkxPublicWsError::Sequence);
        }
        if bar.sequence == previous.sequence {
            return if same_bar(previous, &bar) {
                Ok(None)
            } else {
                Err(OkxPublicWsError::Protocol)
            };
        }
        self.latest = Some(bar.clone());
        Ok(Some(bar))
    }
}

#[derive(Default)]
struct TradeGuard {
    latest_by_id: BTreeMap<u64, venue_domain::domain::PublicTrade>,
    order: VecDeque<u64>,
}

impl TradeGuard {
    fn accept(
        &mut self,
        trades: Vec<venue_domain::domain::PublicTrade>,
    ) -> Result<Vec<venue_domain::domain::PublicTrade>, OkxError> {
        let mut accepted = Vec::with_capacity(trades.len());
        for trade in trades {
            let venue_domain::domain::PublicTradeId::Numeric(id) = &trade.aggregate_trade_id else {
                return Err(OkxError::Sequence);
            };
            let id = *id;
            if let Some(previous) = self.latest_by_id.get(&id) {
                if same_trade(previous, &trade) {
                    continue;
                }
                return Err(OkxError::Sequence);
            }
            let _ = self.latest_by_id.insert(id, trade.clone());
            self.order.push_back(id);
            if self.latest_by_id.len() > MAX_TRACKED_TRADES {
                if let Some(oldest) = self.order.pop_front() {
                    let _ = self.latest_by_id.remove(&oldest);
                }
            }
            accepted.push(trade);
        }
        Ok(accepted)
    }
}

fn same_bar(
    left: &venue_domain::domain::PublicBar,
    right: &venue_domain::domain::PublicBar,
) -> bool {
    left.symbol == right.symbol
        && left.generation == right.generation
        && left.sequence == right.sequence
        && left.open_time_ms == right.open_time_ms
        && left.close_time_ms == right.close_time_ms
        && left.interval_ms == right.interval_ms
        && left.open == right.open
        && left.high == right.high
        && left.low == right.low
        && left.close == right.close
        && left.base_volume == right.base_volume
        && left.quote_volume == right.quote_volume
        && left.trade_count == right.trade_count
        && left.taker_buy_base_volume == right.taker_buy_base_volume
        && left.taker_buy_quote_volume == right.taker_buy_quote_volume
}

fn same_trade(
    left: &venue_domain::domain::PublicTrade,
    right: &venue_domain::domain::PublicTrade,
) -> bool {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HeartbeatAction {
    None,
    Ping,
    Expired,
}

/// A read proves the peer is live just as a text `pong` does. This avoids fencing a healthy,
/// busy feed merely because it did not separately echo an application-level ping.
#[derive(Clone, Debug)]
struct HeartbeatState {
    last_received: Instant,
    last_ping: Option<Instant>,
}

impl HeartbeatState {
    const fn new(now: Instant) -> Self {
        Self {
            last_received: now,
            last_ping: None,
        }
    }

    fn on_received(&mut self, now: Instant) {
        self.last_received = now;
        self.last_ping = None;
    }

    fn on_idle(&mut self, now: Instant) -> HeartbeatAction {
        if let Some(last_ping) = self.last_ping {
            return if now.duration_since(last_ping) >= HEARTBEAT_PONG_DEADLINE {
                HeartbeatAction::Expired
            } else {
                HeartbeatAction::None
            };
        }
        if now.duration_since(self.last_received) < HEARTBEAT_IDLE {
            return HeartbeatAction::None;
        }
        self.last_ping = Some(now);
        HeartbeatAction::Ping
    }
}

/// The active sequence bridge and local depth mirror. Keeping this inside the adapter lets it
/// reject crossed or empty books before an event can enter the shared runtime.
#[derive(Clone, Debug)]
struct BookBridge {
    instrument: OkxInstrument,
    generation: u64,
    last_sequence: Option<u64>,
    bids: BTreeMap<Price, Decimal>,
    asks: BTreeMap<Price, Decimal>,
}

impl BookBridge {
    fn new(instrument: OkxInstrument, generation: u64) -> Result<Self, OkxPublicWsError> {
        if generation == 0 {
            return Err(OkxPublicWsError::Protocol);
        }
        Ok(Self {
            instrument,
            generation,
            last_sequence: None,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        })
    }

    fn accept(
        &mut self,
        payload: &str,
        received_at_ms: u64,
    ) -> Result<Option<MarketEvent>, OkxPublicWsError> {
        if received_at_ms == 0 {
            return Err(OkxPublicWsError::Clock);
        }
        let frame: BookFrame =
            serde_json::from_str(payload).map_err(|_| OkxPublicWsError::Protocol)?;
        if frame.arg.channel != "books" || frame.arg.inst_id != self.instrument.native_id() {
            return Err(OkxPublicWsError::Binding);
        }
        let [row] = frame.data.as_slice() else {
            return Err(OkxPublicWsError::Protocol);
        };
        let exchange_time_ms = positive_u64(&row.ts)?;
        if exchange_time_ms > received_at_ms {
            return Err(OkxPublicWsError::Protocol);
        }
        let sequence = row.seq_id;
        match frame.action.as_str() {
            "snapshot" => self.accept_snapshot(row, sequence, exchange_time_ms),
            "update" => self.accept_update(row, sequence, exchange_time_ms),
            _ => Err(OkxPublicWsError::Protocol),
        }
    }

    fn accept_snapshot(
        &mut self,
        row: &BookRow,
        sequence: u64,
        exchange_time_ms: u64,
    ) -> Result<Option<MarketEvent>, OkxPublicWsError> {
        if self.last_sequence.is_some() || row.prev_seq_id != -1 {
            return Err(OkxPublicWsError::Sequence);
        }
        let bids = parse_levels(&row.bids, &self.instrument, false)?;
        let asks = parse_levels(&row.asks, &self.instrument, false)?;
        self.replace_book(&bids, &asks)?;
        self.last_sequence = Some(sequence);
        Ok(Some(MarketEvent::Snapshot(MarketSnapshot {
            symbol: self.instrument.instrument().symbol.clone(),
            generation: self.generation,
            sequence,
            exchange_time_ms: Some(exchange_time_ms),
            bids,
            asks,
        })))
    }

    fn accept_update(
        &mut self,
        row: &BookRow,
        sequence: u64,
        exchange_time_ms: u64,
    ) -> Result<Option<MarketEvent>, OkxPublicWsError> {
        let previous = u64::try_from(row.prev_seq_id).map_err(|_| OkxPublicWsError::Sequence)?;
        let Some(last) = self.last_sequence else {
            return Err(OkxPublicWsError::Sequence);
        };
        // The documented no-change heartbeat is the sole legal equal-sequence frame.
        if row.bids.is_empty() && row.asks.is_empty() && sequence == last && previous == last {
            return Ok(None);
        }
        if previous != last || sequence <= previous || (row.bids.is_empty() && row.asks.is_empty())
        {
            return Err(OkxPublicWsError::Sequence);
        }
        let bids = parse_levels(&row.bids, &self.instrument, true)?;
        let asks = parse_levels(&row.asks, &self.instrument, true)?;
        self.apply_levels(&bids, true)?;
        self.apply_levels(&asks, false)?;
        self.validate_book()?;
        self.last_sequence = Some(sequence);
        Ok(Some(MarketEvent::Delta(MarketDelta {
            symbol: self.instrument.instrument().symbol.clone(),
            generation: self.generation,
            first_sequence: previous,
            previous_sequence: Some(previous),
            sequence,
            exchange_time_ms: Some(exchange_time_ms),
            bids,
            asks,
        })))
    }

    fn replace_book(
        &mut self,
        bids: &[MarketLevel],
        asks: &[MarketLevel],
    ) -> Result<(), OkxPublicWsError> {
        self.bids.clear();
        self.asks.clear();
        self.apply_levels(bids, true)?;
        self.apply_levels(asks, false)?;
        self.validate_book()
    }

    fn apply_levels(&mut self, levels: &[MarketLevel], bids: bool) -> Result<(), OkxPublicWsError> {
        let target = if bids { &mut self.bids } else { &mut self.asks };
        for level in levels {
            if level.quantity.is_zero() {
                let _ = target.remove(&level.price);
            } else {
                let _ = target.insert(level.price, level.quantity);
            }
        }
        if target.len() > MAX_BOOK_LEVELS {
            return Err(OkxPublicWsError::Protocol);
        }
        Ok(())
    }

    fn validate_book(&self) -> Result<(), OkxPublicWsError> {
        let (Some((best_bid, _)), Some((best_ask, _))) =
            (self.bids.last_key_value(), self.asks.first_key_value())
        else {
            return Err(OkxPublicWsError::Protocol);
        };
        if best_bid >= best_ask {
            return Err(OkxPublicWsError::Protocol);
        }
        Ok(())
    }
}

fn parse_levels(
    values: &[Vec<String>],
    instrument: &OkxInstrument,
    zero_allowed: bool,
) -> Result<Vec<MarketLevel>, OkxPublicWsError> {
    if values.len() > MAX_BOOK_LEVELS {
        return Err(OkxPublicWsError::Protocol);
    }
    let mut seen = BTreeSet::new();
    values
        .iter()
        .map(|value| {
            let [price, contracts, _liquidation_orders, _count] = value.as_slice() else {
                return Err(OkxPublicWsError::Protocol);
            };
            let price = Price::new(decimal(price)?).map_err(|_| OkxPublicWsError::Protocol)?;
            if !seen.insert(price) {
                return Err(OkxPublicWsError::Protocol);
            }
            let contracts = decimal(contracts)?;
            if contracts.is_sign_negative() || (!zero_allowed && contracts.is_zero()) {
                return Err(OkxPublicWsError::Protocol);
            }
            let quantity = instrument
                .contracts_to_base(contracts)
                .map_err(|_| OkxPublicWsError::Protocol)?;
            Ok(MarketLevel { price, quantity })
        })
        .collect()
}

fn public_subscription(instrument: &OkxInstrument) -> Result<String, OkxPublicWsError> {
    serde_json::to_string(&serde_json::json!({
        "op": "subscribe",
        "args": [
            {"channel": "books", "instId": instrument.native_id()},
            {"channel": "trades", "instId": instrument.native_id()},
        ],
    }))
    .map_err(|_| OkxPublicWsError::Protocol)
}

fn business_subscription(instrument: &OkxInstrument) -> Result<String, OkxPublicWsError> {
    serde_json::to_string(&serde_json::json!({
        "op": "subscribe",
        "args": [{"channel": "candle1m", "instId": instrument.native_id()}],
    }))
    .map_err(|_| OkxPublicWsError::Protocol)
}

async fn await_subscription_acks(
    socket: &mut Socket,
    instrument: &OkxInstrument,
    channels: &[&str],
    operation_timeout: Duration,
    max_body_bytes: usize,
) -> Result<(), OkxPublicWsError> {
    let mut acknowledged = BTreeSet::new();
    while acknowledged.len() != channels.len() {
        let text = receive_text(socket, operation_timeout, max_body_bytes).await?;
        if let Some(channel) = subscription_ack_channel(&text, instrument.native_id()) {
            if channels.contains(&channel.as_str()) {
                let _ = acknowledged.insert(channel);
                continue;
            }
            return Err(OkxPublicWsError::Protocol);
        }
        if is_subscription_rejection(&text) {
            return Err(OkxPublicWsError::SubscriptionRejected);
        }
        if text == "pong" {
            continue;
        }
        return Err(OkxPublicWsError::Protocol);
    }
    Ok(())
}

async fn receive(
    socket: &mut Socket,
    heartbeat: &mut HeartbeatState,
    wait: Duration,
    connect_timeout: Duration,
) -> Result<Option<Message>, OkxPublicWsError> {
    let frame = match timeout(wait, socket.next()).await {
        Ok(Some(Ok(frame))) => frame,
        Ok(Some(Err(_))) | Ok(None) => return Err(OkxPublicWsError::Disconnected),
        Err(_) => {
            match heartbeat.on_idle(Instant::now()) {
                HeartbeatAction::None => {}
                HeartbeatAction::Expired => return Err(OkxPublicWsError::HeartbeatTimeout),
                HeartbeatAction::Ping => {
                    send(
                        socket,
                        Message::Text("ping".into()),
                        connect_timeout.min(HEARTBEAT_SEND_TIMEOUT),
                    )
                    .await?;
                }
            }
            return Ok(None);
        }
    };
    heartbeat.on_received(Instant::now());
    match frame {
        Message::Text(_) => Ok(Some(frame)),
        Message::Ping(payload) => {
            send(
                socket,
                Message::Pong(payload),
                connect_timeout.min(HEARTBEAT_SEND_TIMEOUT),
            )
            .await?;
            Ok(None)
        }
        Message::Pong(_) => Ok(None),
        Message::Close(_) => Err(OkxPublicWsError::Disconnected),
        Message::Binary(_) | Message::Frame(_) => Err(OkxPublicWsError::Protocol),
    }
}

async fn send(
    socket: &mut Socket,
    message: Message,
    wait: Duration,
) -> Result<(), OkxPublicWsError> {
    timeout(wait, socket.send(message))
        .await
        .map_err(|_| OkxPublicWsError::Timeout)?
        .map_err(|_| OkxPublicWsError::Disconnected)
}

async fn receive_text(
    socket: &mut Socket,
    wait: Duration,
    max_body_bytes: usize,
) -> Result<String, OkxPublicWsError> {
    timeout(wait, async {
        loop {
            let message = socket
                .next()
                .await
                .ok_or(OkxPublicWsError::Disconnected)?
                .map_err(|_| OkxPublicWsError::Disconnected)?;
            match message {
                Message::Text(value) if value.len() <= max_body_bytes => {
                    return Ok(value.to_string());
                }
                Message::Text(_) => return Err(OkxPublicWsError::Protocol),
                Message::Ping(payload) => socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|_| OkxPublicWsError::Disconnected)?,
                Message::Pong(_) => {}
                Message::Close(_) => return Err(OkxPublicWsError::Disconnected),
                Message::Binary(_) | Message::Frame(_) => return Err(OkxPublicWsError::Protocol),
            }
        }
    })
    .await
    .map_err(|_| OkxPublicWsError::Timeout)?
}

fn subscription_ack_channel(payload: &str, native_id: &str) -> Option<String> {
    serde_json::from_str::<SubscriptionEvent>(payload)
        .ok()
        .and_then(|event| {
            (event.event.as_deref() == Some("subscribe"))
                .then_some(event.arg)
                .flatten()
                .filter(|arg| arg.inst_id == native_id)
                .map(|arg| arg.channel)
        })
}

fn is_subscription_ack(payload: &str, channel: &str, native_id: &str) -> bool {
    subscription_ack_channel(payload, native_id).as_deref() == Some(channel)
}

fn is_subscription_rejection(payload: &str) -> bool {
    serde_json::from_str::<SubscriptionEvent>(payload)
        .ok()
        .is_some_and(|event| event.event.as_deref() == Some("error"))
}

fn stream_channel(payload: &str) -> Result<String, OkxPublicWsError> {
    serde_json::from_str::<StreamEnvelope>(payload)
        .map(|value| value.arg.channel)
        .map_err(|_| OkxPublicWsError::Protocol)
}

fn is_closed_candle(payload: &str, native_id: &str) -> Result<bool, OkxPublicWsError> {
    let value: CandleEnvelope =
        serde_json::from_str(payload).map_err(|_| OkxPublicWsError::Protocol)?;
    if value.arg.channel != "candle1m" || value.arg.inst_id != native_id {
        return Err(OkxPublicWsError::Binding);
    }
    let [row] = value.data.as_slice() else {
        return Err(OkxPublicWsError::Protocol);
    };
    let Some(confirm) = row.get(8) else {
        return Err(OkxPublicWsError::Protocol);
    };
    match confirm.as_str() {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err(OkxPublicWsError::Protocol),
    }
}

fn decimal(value: &str) -> Result<Decimal, OkxPublicWsError> {
    value.parse().map_err(|_| OkxPublicWsError::Protocol)
}

fn positive_u64(value: &str) -> Result<u64, OkxPublicWsError> {
    value
        .parse()
        .map_err(|_| OkxPublicWsError::Protocol)
        .and_then(|value| {
            (value > 0)
                .then_some(value)
                .ok_or(OkxPublicWsError::Protocol)
        })
}

fn now_ms() -> Result<u64, OkxPublicWsError> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| OkxPublicWsError::Clock)?
            .as_millis(),
    )
    .map_err(|_| OkxPublicWsError::Clock)
}

fn next_generation() -> Result<u64, OkxPublicWsError> {
    let wall_clock = now_ms()?;
    let mut observed = LAST_PUBLIC_GENERATION.load(Ordering::Relaxed);
    loop {
        let candidate = wall_clock.max(observed.checked_add(1).ok_or(OkxPublicWsError::Protocol)?);
        match LAST_PUBLIC_GENERATION.compare_exchange_weak(
            observed,
            candidate,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return Ok(candidate),
            Err(current) => observed = current,
        }
    }
}

fn validate_limits(wait: Duration, bytes: usize) -> Result<(), OkxPublicWsError> {
    if wait.is_zero() || wait > MAX_CONNECT_TIMEOUT || bytes == 0 || bytes > 2 * 1024 * 1024 {
        return Err(OkxPublicWsError::Configuration);
    }
    Ok(())
}

fn map_transport(error: OkxTransportError) -> OkxPublicWsError {
    match error {
        OkxTransportError::Timeout => OkxPublicWsError::Timeout,
        OkxTransportError::Binding | OkxTransportError::Configuration => OkxPublicWsError::Binding,
        _ => OkxPublicWsError::Transport,
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BookFrame {
    arg: BookArg,
    action: String,
    data: Vec<BookRow>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BookArg {
    channel: String,
    inst_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BookRow {
    asks: Vec<Vec<String>>,
    bids: Vec<Vec<String>>,
    ts: String,
    seq_id: u64,
    prev_seq_id: i64,
}

#[derive(Deserialize)]
struct SubscriptionEvent {
    event: Option<String>,
    arg: Option<BookArg>,
}

#[derive(Deserialize)]
struct StreamEnvelope {
    arg: BookArg,
}

#[derive(Deserialize)]
struct CandleEnvelope {
    arg: BookArg,
    data: Vec<Vec<String>>,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum OkxPublicWsError {
    #[error("OKX public receiver binding is invalid")]
    Binding,
    #[error("OKX public receiver limits are invalid")]
    Configuration,
    #[error("OKX public receiver clock is invalid")]
    Clock,
    #[error("OKX public receiver disconnected")]
    Disconnected,
    #[error("OKX public receiver protocol frame is invalid")]
    Protocol,
    #[error("OKX public receiver sequence is reset, duplicated, reversed, or discontinuous")]
    Sequence,
    #[error("OKX public receiver subscription was rejected")]
    SubscriptionRejected,
    #[error("OKX public receiver bounded public rules request failed")]
    Transport,
    #[error("OKX public receiver operation timed out")]
    Timeout,
    #[error("OKX public receiver heartbeat did not receive a pong or data in time")]
    HeartbeatTimeout,
    #[error("OKX public receiver is terminal after an earlier failure")]
    Terminal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{OkxConfig, parse_instrument};
    use venue_gateway_api::{GatewayMode, VenueId};

    const INSTRUMENT: &[u8] = include_bytes!("../fixtures/linear-swap-instrument.json");
    const SNAPSHOT: &str = include_str!("../fixtures/books-snapshot.json");
    const UPDATE: &str = include_str!("../fixtures/books-update.json");
    const TRADES: &[u8] = include_bytes!("../fixtures/public-ws-trades.json");
    const CLOSED_CANDLE: &[u8] = include_bytes!("../fixtures/business-ws-candle-1m-closed.json");

    fn bridge() -> Result<BookBridge, Box<dyn std::error::Error>> {
        let binding = GatewayBinding::new(
            VenueId::Okx,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?;
        let config = OkxConfig::for_binding(binding)?;
        Ok(BookBridge::new(
            parse_instrument(INSTRUMENT, &config, 7)?,
            7,
        )?)
    }

    #[test]
    fn books_fixture_bridges_snapshot_and_delta_in_base_quantity()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut bridge = bridge()?;
        let snapshot = bridge.accept(SNAPSHOT, 1000)?.ok_or("missing snapshot")?;
        match snapshot {
            MarketEvent::Snapshot(value) => {
                assert_eq!(value.sequence, 100);
                assert_eq!(value.bids[0].quantity.to_string(), "2.00");
            }
            _ => return Err("expected snapshot".into()),
        }
        let update = bridge.accept(UPDATE, 1001)?.ok_or("missing update")?;
        match update {
            MarketEvent::Delta(value) => {
                assert_eq!(value.previous_sequence, Some(100));
                assert_eq!(value.sequence, 101);
                assert_eq!(value.asks[0].quantity.to_string(), "1.50");
            }
            _ => return Err("expected delta".into()),
        }
        Ok(())
    }

    #[test]
    fn books_rejects_gap_duplicate_reset_empty_and_crossed_book()
    -> Result<(), Box<dyn std::error::Error>> {
        let cases = [
            UPDATE.replace("\"prevSeqId\":100", "\"prevSeqId\":99"),
            SNAPSHOT.replace("\"prevSeqId\":-1", "\"prevSeqId\":0"),
            UPDATE.replace("\"seqId\":101", "\"seqId\":100"),
            UPDATE.replace(
                "[\"60000.1\",\"15\",\"0\",\"1\"]",
                "[\"59999.9\",\"15\",\"0\",\"1\"]",
            ),
        ];
        for payload in cases {
            let mut bridge = bridge()?;
            if payload.contains("prevSeqId\":0") {
                assert!(bridge.accept(&payload, 1000).is_err());
            } else {
                let _ = bridge.accept(SNAPSHOT, 1000)?;
                assert!(bridge.accept(&payload, 1001).is_err());
            }
        }
        let mut bridge = bridge()?;
        let _ = bridge.accept(SNAPSHOT, 1000)?;
        let heartbeat = UPDATE
            .replace("\"asks\":[[\"60000.1\",\"15\",\"0\",\"1\"]]", "\"asks\":[]")
            .replace("\"bids\":[[\"60000.0\",\"20\",\"0\",\"1\"]]", "\"bids\":[]")
            .replace("\"seqId\":101", "\"seqId\":100");
        assert_eq!(bridge.accept(&heartbeat, 1001)?, None);
        Ok(())
    }

    #[test]
    fn subscription_ack_is_exact_and_error_is_rejected() {
        assert!(is_subscription_ack(
            r#"{"event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT-SWAP"}}"#,
            "books",
            "BTC-USDT-SWAP"
        ));
        assert!(!is_subscription_ack(
            r#"{"event":"subscribe","arg":{"channel":"books5","instId":"BTC-USDT-SWAP"}}"#,
            "books",
            "BTC-USDT-SWAP"
        ));
        assert!(is_subscription_rejection(
            r#"{"event":"error","code":"60012"}"#
        ));
    }

    #[test]
    fn heartbeat_allows_one_ping_then_requires_pong_or_data() {
        let start = Instant::now();
        let mut heartbeat = HeartbeatState::new(start);
        assert_eq!(
            heartbeat.on_idle(start + Duration::from_secs(24)),
            HeartbeatAction::None
        );
        assert_eq!(
            heartbeat.on_idle(start + HEARTBEAT_IDLE),
            HeartbeatAction::Ping
        );
        assert_eq!(
            heartbeat.on_idle(start + HEARTBEAT_IDLE + Duration::from_millis(5)),
            HeartbeatAction::None
        );
        assert_eq!(
            heartbeat.on_idle(start + HEARTBEAT_IDLE + HEARTBEAT_PONG_DEADLINE),
            HeartbeatAction::Expired
        );

        let mut heartbeat = HeartbeatState::new(start);
        assert_eq!(
            heartbeat.on_idle(start + HEARTBEAT_IDLE),
            HeartbeatAction::Ping
        );
        heartbeat.on_received(start + HEARTBEAT_IDLE + Duration::from_millis(1));
        assert_eq!(
            heartbeat.on_idle(start + HEARTBEAT_IDLE + Duration::from_secs(1)),
            HeartbeatAction::None
        );
    }

    #[test]
    fn trades_and_bars_deduplicate_replays_and_fence_conflicts()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = GatewayBinding::new(
            VenueId::Okx,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?;
        let config = OkxConfig::for_binding(binding)?;
        let instrument = parse_instrument(INSTRUMENT, &config, 7)?;
        let trade = crate::parse_trades(TRADES, &instrument, 1_630_048_898_000)?;
        let mut trades = TradeGuard::default();
        assert_eq!(trades.accept(trade.clone())?.len(), 1);
        assert!(trades.accept(trade.clone())?.is_empty());
        let mut changed_trade = trade;
        changed_trade[0].quantity += Decimal::ONE;
        assert_eq!(trades.accept(changed_trade), Err(crate::OkxError::Sequence));

        let bar = crate::parse_closed_1m_candle(CLOSED_CANDLE, &instrument, 1_630_048_860_000)?;
        let mut bars = ClosedBarGuard::new(7);
        assert!(bars.accept(bar.clone())?.is_some());
        let mut replay = bar.clone();
        replay.received_at_ms += 1;
        assert_eq!(bars.accept(replay)?, None);
        let mut changed_bar = bar.clone();
        changed_bar.close = Price::new(Decimal::new(60_021, 0))?;
        assert_eq!(bars.accept(changed_bar), Err(OkxPublicWsError::Protocol));
        let mut older = bar;
        older.sequence -= 1;
        assert_eq!(bars.accept(older), Err(OkxPublicWsError::Sequence));
        Ok(())
    }

    #[test]
    fn connect_timeout_is_bounded_to_ten_seconds() {
        assert!(validate_limits(MAX_CONNECT_TIMEOUT, 1024).is_ok());
        assert_eq!(
            validate_limits(MAX_CONNECT_TIMEOUT + Duration::from_millis(1), 1024),
            Err(OkxPublicWsError::Configuration)
        );
    }
}
