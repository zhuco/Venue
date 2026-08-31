//! Lifecycle-owned, credential-free Bitget Scalping public receiver.
//!
//! Native WebSocket framing remains in the adapter. Consumers receive only already-bound public
//! facts; `books` must still pass through its account-local sequencer before it can establish a
//! strategy book.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::time::timeout;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use venue_gateway_api::{GatewayBinding, VenueId};

use crate::{
    BitgetConfig, BitgetTransportLimits,
    public::{
        BitgetBooksMessage, BitgetFormingBar, BitgetPublicBarBatch, BitgetPublicSource,
        BitgetPublicTradeBatch, BitgetRawPublicPayload, parse_books_message,
        parse_public_forming_bar_batch, parse_public_trade_batch, scalping_book_subscription,
        scalping_public_subscription,
    },
};

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
const CLIENT_PING_INTERVAL: Duration = Duration::from_secs(30);

/// One validated native public frame. A subscription acknowledgement is deliberately not exposed
/// as market data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BitgetScalpingPublicFrame {
    Books(BitgetBooksMessage),
    Trades(BitgetPublicTradeBatch),
    ClosedBars(BitgetPublicBarBatch),
}

/// The receiver's subscription is fixed before connecting so a books-only consumer cannot
/// accidentally accept facts from a broader public stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BitgetPublicSubscription {
    Scalping,
    BooksOnly,
}

impl BitgetPublicSubscription {
    fn allows_topic(self, topic: &str) -> bool {
        match self {
            Self::Scalping => matches!(topic, "books" | "publicTrade" | "kline"),
            Self::BooksOnly => topic == "books",
        }
    }
}

/// Bounded public receiver for exactly one LIVE UTA symbol. It owns no account credential and
/// cannot perform any private or mutation operation.
pub struct BitgetScalpingPublicReceiver {
    binding: GatewayBinding,
    limits: BitgetTransportLimits,
    socket: Socket,
    generation: u64,
    subscription: BitgetPublicSubscription,
    forming_bar: Option<BitgetFormingBar>,
    last_client_ping: Instant,
    awaiting_client_pong: bool,
}

impl BitgetScalpingPublicReceiver {
    pub async fn connect(
        binding: GatewayBinding,
        limits: BitgetTransportLimits,
    ) -> Result<Self, BitgetPublicWsError> {
        Self::connect_with_subscription(binding, limits, BitgetPublicSubscription::Scalping).await
    }

    /// Connects a receiver that admits only the sequenced `books` stream used to establish a
    /// continuous public book. Trade and kline acknowledgements or payloads are protocol violations.
    pub async fn connect_books_only(
        binding: GatewayBinding,
        limits: BitgetTransportLimits,
    ) -> Result<Self, BitgetPublicWsError> {
        Self::connect_with_subscription(binding, limits, BitgetPublicSubscription::BooksOnly).await
    }

    async fn connect_with_subscription(
        binding: GatewayBinding,
        limits: BitgetTransportLimits,
        subscription: BitgetPublicSubscription,
    ) -> Result<Self, BitgetPublicWsError> {
        binding
            .validate()
            .map_err(|_| BitgetPublicWsError::Binding)?;
        if binding.venue != VenueId::Bitget {
            return Err(BitgetPublicWsError::Binding);
        }
        let generation = now_ms()?;
        let endpoint = BitgetConfig::for_mode(binding.mode).public_ws();
        let websocket = WebSocketConfig::default()
            .max_message_size(Some(limits.maximum_body_bytes()))
            .max_frame_size(Some(limits.maximum_body_bytes()));
        let (mut socket, _) = timeout(
            limits.operation_timeout(),
            connect_async_with_config(endpoint, Some(websocket), true),
        )
        .await
        .map_err(|_| BitgetPublicWsError::Timeout)?
        .map_err(|_| BitgetPublicWsError::Disconnected)?;
        let request = match subscription {
            BitgetPublicSubscription::Scalping => scalping_public_subscription(&binding.symbol),
            BitgetPublicSubscription::BooksOnly => scalping_book_subscription(&binding.symbol),
        }
        .map_err(|_| BitgetPublicWsError::Protocol)?;
        timeout(
            limits.operation_timeout(),
            socket.send(Message::Text(request.to_string().into())),
        )
        .await
        .map_err(|_| BitgetPublicWsError::Timeout)?
        .map_err(|_| BitgetPublicWsError::Disconnected)?;
        Ok(Self {
            binding,
            limits,
            socket,
            generation,
            subscription,
            forming_bar: None,
            last_client_ping: Instant::now(),
            awaiting_client_pong: false,
        })
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Reads at most one frame. Timeout is an idle poll rather than a connection failure; all
    /// malformed, crossed-symbol, close, and wire errors force the Node to fence/restart.
    pub async fn next(
        &mut self,
        wait: Duration,
    ) -> Result<Option<BitgetScalpingPublicFrame>, BitgetPublicWsError> {
        self.send_client_ping_if_due().await?;
        let Some(frame) = timeout(wait, self.socket.next())
            .await
            .map_err(|_| BitgetPublicWsError::Idle)?
        else {
            return Err(BitgetPublicWsError::Disconnected);
        };
        let frame = frame.map_err(|_| BitgetPublicWsError::Disconnected)?;
        match frame {
            Message::Text(value) => self.parse_text(value.to_string()),
            Message::Binary(value) => String::from_utf8(value.to_vec())
                .map_err(|_| BitgetPublicWsError::Protocol)
                .and_then(|value| self.parse_text(value)),
            Message::Ping(value) => {
                timeout(
                    self.limits.operation_timeout(),
                    self.socket.send(Message::Pong(value)),
                )
                .await
                .map_err(|_| BitgetPublicWsError::Timeout)?
                .map_err(|_| BitgetPublicWsError::Disconnected)?;
                Ok(None)
            }
            Message::Pong(_) => Ok(None),
            Message::Close(_) => Err(BitgetPublicWsError::Disconnected),
            _ => Err(BitgetPublicWsError::Protocol),
        }
    }

    fn parse_text(
        &mut self,
        payload: String,
    ) -> Result<Option<BitgetScalpingPublicFrame>, BitgetPublicWsError> {
        if payload == "pong" {
            self.awaiting_client_pong = false;
            return Ok(None);
        }
        if is_subscription_ack(&payload, self.subscription) {
            return Ok(None);
        }
        let source = parse_subscription_source(&payload, self.subscription)?;
        let raw = BitgetRawPublicPayload::new(
            source,
            self.binding.symbol.clone(),
            self.generation,
            now_ms()?,
            payload,
        )
        .map_err(|_| BitgetPublicWsError::Protocol)?;
        match source {
            BitgetPublicSource::WebSocketBooks => parse_books_message(raw)
                .map(BitgetScalpingPublicFrame::Books)
                .map(Some)
                .map_err(|_| BitgetPublicWsError::Protocol),
            BitgetPublicSource::WebSocketPublicTrade => parse_public_trade_batch(raw)
                .map(BitgetScalpingPublicFrame::Trades)
                .map(Some)
                .map_err(|_| BitgetPublicWsError::Protocol),
            BitgetPublicSource::WebSocketKline => {
                let forming = parse_public_forming_bar_batch(raw)
                    .map_err(|_| BitgetPublicWsError::Protocol)?;
                for candidate in forming.bars {
                    if let Some(previous) = self.forming_bar.as_ref() {
                        if candidate.open_time_ms < previous.open_time_ms {
                            return Err(BitgetPublicWsError::Protocol);
                        }
                    }
                    self.forming_bar = Some(candidate);
                }
                // UTA kline documents only snapshot/update cadence; it does not certify that a
                // later start makes the earlier mutable candle final. Keep one formation for
                // diagnostics but publish no closed bar without an authoritative close signal.
                Ok(None)
            }
            _ => Err(BitgetPublicWsError::Protocol),
        }
    }

    async fn send_client_ping_if_due(&mut self) -> Result<(), BitgetPublicWsError> {
        if self.last_client_ping.elapsed() < CLIENT_PING_INTERVAL {
            return Ok(());
        }
        if self.awaiting_client_pong {
            return Err(BitgetPublicWsError::Protocol);
        }
        timeout(
            self.limits.operation_timeout(),
            self.socket.send(Message::Text("ping".into())),
        )
        .await
        .map_err(|_| BitgetPublicWsError::Timeout)?
        .map_err(|_| BitgetPublicWsError::Disconnected)?;
        self.last_client_ping = Instant::now();
        self.awaiting_client_pong = true;
        Ok(())
    }
}

fn is_subscription_ack(payload: &str, subscription: BitgetPublicSubscription) -> bool {
    serde_json::from_str::<serde_json::Value>(payload)
        .ok()
        .is_some_and(|value| {
            value.get("event").and_then(serde_json::Value::as_str) == Some("subscribe")
                && value
                    .get("arg")
                    .and_then(serde_json::Value::as_object)
                    .is_some_and(|arg| {
                        arg.get("instType").and_then(serde_json::Value::as_str)
                            == Some("usdt-futures")
                            && arg
                                .get("topic")
                                .and_then(serde_json::Value::as_str)
                                .is_some_and(|topic| subscription.allows_topic(topic))
                    })
        })
}

fn parse_subscription_source(
    payload: &str,
    subscription: BitgetPublicSubscription,
) -> Result<BitgetPublicSource, BitgetPublicWsError> {
    let topic = serde_json::from_str::<serde_json::Value>(payload)
        .ok()
        .and_then(|value| value.get("arg")?.get("topic")?.as_str().map(str::to_owned))
        .ok_or(BitgetPublicWsError::Protocol)?;
    if !subscription.allows_topic(&topic) {
        return Err(BitgetPublicWsError::Protocol);
    }
    match topic.as_str() {
        "books" => Ok(BitgetPublicSource::WebSocketBooks),
        "publicTrade" => Ok(BitgetPublicSource::WebSocketPublicTrade),
        "kline" => Ok(BitgetPublicSource::WebSocketKline),
        _ => Err(BitgetPublicWsError::Protocol),
    }
}

fn now_ms() -> Result<u64, BitgetPublicWsError> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| BitgetPublicWsError::Clock)?
            .as_millis(),
    )
    .map_err(|_| BitgetPublicWsError::Clock)
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BitgetPublicWsError {
    #[error("Bitget public receiver binding is invalid")]
    Binding,
    #[error("Bitget public receiver clock is invalid")]
    Clock,
    #[error("Bitget public receiver connection or stream disconnected")]
    Disconnected,
    #[error("Bitget public receiver idle poll elapsed")]
    Idle,
    #[error("Bitget public receiver frame or subscription is invalid")]
    Protocol,
    #[error("Bitget public receiver operation timed out")]
    Timeout,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_ack_is_not_market_input() {
        assert!(is_subscription_ack(
            r#"{"event":"subscribe","arg":{"instType":"usdt-futures","topic":"books","symbol":"BTCUSDT"}}"#,
            BitgetPublicSubscription::Scalping,
        ));
        assert!(!is_subscription_ack(
            r#"{"event":"subscribe","arg":{"instType":"usdt-futures","topic":"ticker","symbol":"BTCUSDT"}}"#,
            BitgetPublicSubscription::Scalping,
        ));
        assert!(is_subscription_ack(
            r#"{"event":"subscribe","arg":{"instType":"usdt-futures","topic":"kline","symbol":"BTCUSDT","interval":"1m"}}"#,
            BitgetPublicSubscription::Scalping,
        ));
    }

    #[test]
    fn books_only_subscription_rejects_non_book_acknowledgements() {
        let books_ack = r#"{"event":"subscribe","arg":{"instType":"usdt-futures","topic":"books","symbol":"BTCUSDT"}}"#;
        let trades_ack = r#"{"event":"subscribe","arg":{"instType":"usdt-futures","topic":"publicTrade","symbol":"BTCUSDT"}}"#;
        let kline_ack = r#"{"event":"subscribe","arg":{"instType":"usdt-futures","topic":"kline","symbol":"BTCUSDT","interval":"1m"}}"#;
        assert!(is_subscription_ack(
            books_ack,
            BitgetPublicSubscription::BooksOnly,
        ));
        assert!(!is_subscription_ack(
            trades_ack,
            BitgetPublicSubscription::BooksOnly,
        ));
        assert!(!is_subscription_ack(
            kline_ack,
            BitgetPublicSubscription::BooksOnly,
        ));
    }

    #[test]
    fn books_only_topic_parser_rejects_trade_and_kline_payloads() {
        let books = r#"{"arg":{"topic":"books"}}"#;
        let trades = r#"{"arg":{"topic":"publicTrade"}}"#;
        let kline = r#"{"arg":{"topic":"kline"}}"#;
        assert_eq!(
            parse_subscription_source(books, BitgetPublicSubscription::BooksOnly),
            Ok(BitgetPublicSource::WebSocketBooks)
        );
        assert_eq!(
            parse_subscription_source(trades, BitgetPublicSubscription::BooksOnly),
            Err(BitgetPublicWsError::Protocol)
        );
        assert_eq!(
            parse_subscription_source(kline, BitgetPublicSubscription::BooksOnly),
            Err(BitgetPublicWsError::Protocol)
        );
    }
}
