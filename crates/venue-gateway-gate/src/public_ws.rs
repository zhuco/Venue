//! Lifecycle-owned, credential-free Gate.io public depth receiver.
//!
//! The receiver subscribes before fetching the REST baseline, then exposes only the existing
//! adapter-normalized snapshot and incremental depth records. The caller owns the shared-runtime
//! bridge and must never substitute ticker data for a missing bridge.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::time::timeout;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use venue_domain::domain::MarketSnapshot;
use venue_gateway_api::{GatewayBinding, VenueId};

use crate::{
    GateConfig, GateContractRules, GateGatewayBinding, GateHttpTransport, GateOrderBookBridge,
    GatePublicBinding, GatePublicPayloadKind, GatePublicRawPayload, GateTransportLimits,
    grid_public_subscriptions, parse_contract_rules, parse_rest_snapshot, parse_ws_delta,
    rest_order_book_path,
};

const BOOK_DEPTH: u16 = 20;
const BOOK_FREQUENCY_MS: u16 = 20;
const MAX_BUFFERED_DELTAS: usize = 256;

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// The two records which may enter Gate's existing snapshot-plus-delta bridge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GateScalpingBookFrame {
    Snapshot(crate::GatePublicRecord<MarketSnapshot>),
    Delta(crate::GatePublicRecord<crate::GateBookDelta>),
}

/// One bounded, read-only Gate public connection for a fixed LIVE symbol.
pub struct GateScalpingPublicReceiver {
    public_binding: GatePublicBinding,
    limits: GateTransportLimits,
    socket: Socket,
    generation: u64,
    initial_snapshot: Option<crate::GatePublicRecord<MarketSnapshot>>,
}

impl GateScalpingPublicReceiver {
    /// Starts the socket subscription before acquiring the REST baseline. Frames received while
    /// the snapshot is in flight remain queued by the socket and are bridged by the resident's
    /// `GateOrderBookBridge`; no standalone snapshot is treated as ready.
    pub async fn connect(
        binding: GatewayBinding,
        limits: GateTransportLimits,
    ) -> Result<Self, GatePublicWsError> {
        binding.validate().map_err(|_| GatePublicWsError::Binding)?;
        if binding.venue != VenueId::Gate {
            return Err(GatePublicWsError::Binding);
        }
        let gate_binding =
            GateGatewayBinding::new(binding.clone()).map_err(|_| GatePublicWsError::Binding)?;
        let generation = now_ms()?;
        let transport = GateHttpTransport::new(&gate_binding, generation, limits)
            .map_err(|_| GatePublicWsError::Transport)?;
        let rules = selected_rules(&transport, &binding, generation).await?;
        let public_binding = GatePublicBinding::new(
            binding.symbol.clone(),
            rules.native_symbol,
            rules.quanto_multiplier,
        )
        .map_err(|_| GatePublicWsError::Protocol)?;
        let endpoint = GateConfig::for_mode(binding.mode).usdt_futures_ws();
        let websocket = WebSocketConfig::default()
            .max_message_size(Some(limits.maximum_body_bytes()))
            .max_frame_size(Some(limits.maximum_body_bytes()));
        let (mut socket, _) = timeout(
            limits.operation_timeout(),
            connect_async_with_config(endpoint, Some(websocket), true),
        )
        .await
        .map_err(|_| GatePublicWsError::Timeout)?
        .map_err(|_| GatePublicWsError::Disconnected)?;
        let subscription =
            grid_public_subscriptions(&public_binding, BOOK_FREQUENCY_MS, BOOK_DEPTH)
                .map_err(|_| GatePublicWsError::Protocol)?;
        timeout(
            limits.operation_timeout(),
            socket.send(Message::Text(subscription.to_string().into())),
        )
        .await
        .map_err(|_| GatePublicWsError::Timeout)?
        .map_err(|_| GatePublicWsError::Disconnected)?;
        let path = rest_order_book_path(&public_binding, BOOK_DEPTH)
            .map_err(|_| GatePublicWsError::Protocol)?;
        let payload = transport
            .fetch_public_order_book(&path)
            .await
            .map_err(|_| GatePublicWsError::Transport)?;
        let raw = GatePublicRawPayload::new(
            &public_binding,
            GatePublicPayloadKind::RestOrderBookSnapshot,
            generation,
            now_ms()?,
            payload,
        )
        .map_err(|_| GatePublicWsError::Protocol)?;
        let initial_snapshot =
            parse_rest_snapshot(&public_binding, raw).map_err(|_| GatePublicWsError::Protocol)?;
        Ok(Self {
            public_binding,
            limits,
            socket,
            generation,
            initial_snapshot: Some(initial_snapshot),
        })
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub fn new_book_bridge(&self) -> Result<GateOrderBookBridge, GatePublicWsError> {
        GateOrderBookBridge::new(
            self.public_binding.clone(),
            self.generation,
            MAX_BUFFERED_DELTAS,
        )
        .map_err(|_| GatePublicWsError::Protocol)
    }

    /// Produces at most one adapter-normalized frame. An idle timeout has no semantic meaning;
    /// malformed frames, missing connection, or an unexpected channel are terminal so that the
    /// caller cannot continue a stale public generation.
    pub async fn next(
        &mut self,
        wait: Duration,
    ) -> Result<Option<GateScalpingBookFrame>, GatePublicWsError> {
        if let Some(snapshot) = self.initial_snapshot.take() {
            return Ok(Some(GateScalpingBookFrame::Snapshot(snapshot)));
        }
        let Some(frame) = timeout(wait, self.socket.next())
            .await
            .map_err(|_| GatePublicWsError::Idle)?
        else {
            return Err(GatePublicWsError::Disconnected);
        };
        let frame = frame.map_err(|_| GatePublicWsError::Disconnected)?;
        match frame {
            Message::Text(value) => self.parse_text(value.to_string()),
            Message::Binary(value) => String::from_utf8(value.to_vec())
                .map_err(|_| GatePublicWsError::Protocol)
                .and_then(|value| self.parse_text(value)),
            Message::Ping(value) => {
                timeout(
                    self.limits.operation_timeout(),
                    self.socket.send(Message::Pong(value)),
                )
                .await
                .map_err(|_| GatePublicWsError::Timeout)?
                .map_err(|_| GatePublicWsError::Disconnected)?;
                Ok(None)
            }
            Message::Pong(_) => Ok(None),
            Message::Close(_) => Err(GatePublicWsError::Disconnected),
            _ => Err(GatePublicWsError::Protocol),
        }
    }

    fn parse_text(
        &self,
        payload: String,
    ) -> Result<Option<GateScalpingBookFrame>, GatePublicWsError> {
        if is_subscription_ack(&payload) {
            return Ok(None);
        }
        let raw = GatePublicRawPayload::new(
            &self.public_binding,
            GatePublicPayloadKind::WebSocketOrderBookDelta,
            self.generation,
            now_ms()?,
            payload,
        )
        .map_err(|_| GatePublicWsError::Protocol)?;
        parse_ws_delta(&self.public_binding, raw)
            .map(GateScalpingBookFrame::Delta)
            .map(Some)
            .map_err(|_| GatePublicWsError::Protocol)
    }
}

async fn selected_rules(
    transport: &GateHttpTransport,
    binding: &GatewayBinding,
    generation: u64,
) -> Result<GateContractRules, GatePublicWsError> {
    let catalogue = transport
        .fetch_public_contracts()
        .await
        .map_err(|_| GatePublicWsError::Transport)?;
    let values = serde_json::from_str::<serde_json::Value>(&catalogue)
        .map_err(|_| GatePublicWsError::Protocol)?;
    let expected = crate::native_symbol_for(&binding.symbol);
    let value = values
        .as_array()
        .and_then(|rows| {
            rows.iter().find(|row| {
                row.get("name").and_then(serde_json::Value::as_str) == Some(expected.as_str())
            })
        })
        .ok_or(GatePublicWsError::Protocol)?;
    parse_contract_rules(value, binding.symbol.clone(), generation)
        .map_err(|_| GatePublicWsError::Protocol)
}

fn is_subscription_ack(payload: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(payload)
        .ok()
        .is_some_and(|value| {
            value.get("channel").and_then(serde_json::Value::as_str)
                == Some("futures.order_book_update")
                && value.get("event").and_then(serde_json::Value::as_str) == Some("subscribe")
                && value.get("error").is_none_or(serde_json::Value::is_null)
        })
}

fn now_ms() -> Result<u64, GatePublicWsError> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| GatePublicWsError::Clock)?
            .as_millis(),
    )
    .map_err(|_| GatePublicWsError::Clock)
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum GatePublicWsError {
    #[error("Gate public receiver binding is invalid")]
    Binding,
    #[error("Gate public receiver clock is invalid")]
    Clock,
    #[error("Gate public receiver connection or stream disconnected")]
    Disconnected,
    #[error("Gate public receiver idle poll elapsed")]
    Idle,
    #[error("Gate public receiver frame, subscription, or rules are invalid")]
    Protocol,
    #[error("Gate public receiver operation timed out")]
    Timeout,
    #[error("Gate public receiver bounded transport failed")]
    Transport,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_ack_is_not_market_input() {
        assert!(is_subscription_ack(
            r#"{"channel":"futures.order_book_update","event":"subscribe","error":null}"#
        ));
        assert!(!is_subscription_ack(
            r#"{"channel":"futures.book_ticker","event":"subscribe"}"#
        ));
    }
}
