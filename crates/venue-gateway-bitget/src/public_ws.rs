//! Lifecycle-owned, credential-free Bitget public book receiver.
//!
//! Native WebSocket framing remains in the adapter.  Consumers receive only the already-bound
//! `books` parser value and must still pass it through their account-local sequencer.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
        BitgetBooksMessage, BitgetPublicSource, BitgetRawPublicPayload, parse_books_message,
        scalping_book_subscription,
    },
};

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// One validated native book frame.  A subscription acknowledgement is deliberately not exposed
/// as market data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BitgetScalpingBookFrame {
    Books(BitgetBooksMessage),
}

/// Bounded public receiver for exactly one LIVE UTA symbol. It owns no account credential and
/// cannot perform any private or mutation operation.
pub struct BitgetScalpingPublicReceiver {
    binding: GatewayBinding,
    limits: BitgetTransportLimits,
    socket: Socket,
    generation: u64,
}

impl BitgetScalpingPublicReceiver {
    pub async fn connect(
        binding: GatewayBinding,
        limits: BitgetTransportLimits,
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
        let request = scalping_book_subscription(&binding.symbol)
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
    ) -> Result<Option<BitgetScalpingBookFrame>, BitgetPublicWsError> {
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
        &self,
        payload: String,
    ) -> Result<Option<BitgetScalpingBookFrame>, BitgetPublicWsError> {
        if is_subscription_ack(&payload) {
            return Ok(None);
        }
        let raw = BitgetRawPublicPayload::new(
            BitgetPublicSource::WebSocketBooks,
            self.binding.symbol.clone(),
            self.generation,
            now_ms()?,
            payload,
        )
        .map_err(|_| BitgetPublicWsError::Protocol)?;
        parse_books_message(raw)
            .map(BitgetScalpingBookFrame::Books)
            .map(Some)
            .map_err(|_| BitgetPublicWsError::Protocol)
    }
}

fn is_subscription_ack(payload: &str) -> bool {
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
                            && arg.get("topic").and_then(serde_json::Value::as_str) == Some("books")
                    })
        })
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
            r#"{"event":"subscribe","arg":{"instType":"usdt-futures","topic":"books","symbol":"BTCUSDT"}}"#
        ));
        assert!(!is_subscription_ack(
            r#"{"event":"subscribe","arg":{"instType":"usdt-futures","topic":"ticker","symbol":"BTCUSDT"}}"#
        ));
    }
}
