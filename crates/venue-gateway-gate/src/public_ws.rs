//! Lifecycle-owned, credential-free Gate.io Scalping public receiver.
//!
//! The receiver subscribes before fetching the REST baseline, then exposes only the existing
//! adapter-normalized book, trade, and closed-bar records. The caller owns the shared-runtime
//! bridge and must never substitute ticker data for a missing book bridge.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::time::timeout;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use venue_domain::domain::{MarketSnapshot, PublicBar};
use venue_gateway_api::{GatewayBinding, VenueId};

use crate::{
    GateConfig, GateContractRules, GateGatewayBinding, GateHttpTransport, GateOrderBookBridge,
    GatePublicBinding, GatePublicPayloadKind, GatePublicRawPayload, GateTransportLimits,
    parse_contract_rules, parse_rest_snapshot, parse_ws_delta, parse_ws_forming_bar_batch,
    parse_ws_trade_batch, rest_order_book_path, scalping_public_subscriptions,
};

const BOOK_DEPTH: u16 = 20;
const BOOK_FREQUENCY_MS: u16 = 20;
const MAX_BUFFERED_DELTAS: usize = 256;

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// Public records produced by the fixed Gate Scalping subscription.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GateScalpingPublicFrame {
    Snapshot(crate::GatePublicRecord<MarketSnapshot>),
    Delta(crate::GatePublicRecord<crate::GateBookDelta>),
    Trades(crate::GatePublicTradeBatch),
    ClosedBars(crate::GatePublicBarBatch),
}

/// One bounded, read-only Gate public connection for a fixed LIVE symbol.
pub struct GateScalpingPublicReceiver {
    public_binding: GatePublicBinding,
    limits: GateTransportLimits,
    socket: Socket,
    generation: u64,
    initial_snapshot: Option<crate::GatePublicRecord<MarketSnapshot>>,
    forming_bar: Option<crate::GateFormingBar>,
    last_closed_open_time_ms: Option<u64>,
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
            scalping_public_subscriptions(&public_binding, BOOK_FREQUENCY_MS, BOOK_DEPTH)
                .map_err(|_| GatePublicWsError::Protocol)?;
        for request in subscription {
            timeout(
                limits.operation_timeout(),
                socket.send(Message::Text(request.to_string().into())),
            )
            .await
            .map_err(|_| GatePublicWsError::Timeout)?
            .map_err(|_| GatePublicWsError::Disconnected)?;
        }
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
            forming_bar: None,
            last_closed_open_time_ms: None,
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
    ) -> Result<Option<GateScalpingPublicFrame>, GatePublicWsError> {
        if let Some(snapshot) = self.initial_snapshot.take() {
            return Ok(Some(GateScalpingPublicFrame::Snapshot(snapshot)));
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
        &mut self,
        payload: String,
    ) -> Result<Option<GateScalpingPublicFrame>, GatePublicWsError> {
        if is_subscription_ack(&payload) {
            return Ok(None);
        }
        let channel = serde_json::from_str::<serde_json::Value>(&payload)
            .ok()
            .and_then(|value| {
                value
                    .get("channel")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .ok_or(GatePublicWsError::Protocol)?;
        let kind = match channel.as_str() {
            "futures.pong" => return Ok(None),
            "futures.order_book_update" => GatePublicPayloadKind::WebSocketOrderBookDelta,
            "futures.trades" => GatePublicPayloadKind::WebSocketTrade,
            "futures.candlesticks" => GatePublicPayloadKind::WebSocketCandlestick,
            _ => return Err(GatePublicWsError::Protocol),
        };
        let raw = GatePublicRawPayload::new(
            &self.public_binding,
            kind,
            self.generation,
            now_ms()?,
            payload,
        )
        .map_err(|_| GatePublicWsError::Protocol)?;
        match kind {
            GatePublicPayloadKind::WebSocketOrderBookDelta => {
                parse_ws_delta(&self.public_binding, raw)
                    .map(GateScalpingPublicFrame::Delta)
                    .map(Some)
                    .map_err(|_| GatePublicWsError::Protocol)
            }
            GatePublicPayloadKind::WebSocketTrade => {
                parse_ws_trade_batch(&self.public_binding, raw)
                    .map(GateScalpingPublicFrame::Trades)
                    .map(Some)
                    .map_err(|_| GatePublicWsError::Protocol)
            }
            GatePublicPayloadKind::WebSocketCandlestick => {
                let forming = parse_ws_forming_bar_batch(&self.public_binding, raw)
                    .map_err(|_| GatePublicWsError::Protocol)?;
                let mut closed = Vec::new();
                for candidate in forming.bars {
                    if let Some(bar) = advance_forming_bar(
                        &mut self.forming_bar,
                        &mut self.last_closed_open_time_ms,
                        candidate,
                        &self.public_binding,
                        self.generation,
                        forming.raw.received_at_ms,
                    )? {
                        closed.push(bar);
                    }
                }
                Ok(
                    (!closed.is_empty()).then_some(GateScalpingPublicFrame::ClosedBars(
                        crate::GatePublicBarBatch {
                            raw: forming.raw,
                            freshness: forming.freshness,
                            bars: closed,
                        },
                    )),
                )
            }
            _ => Err(GatePublicWsError::Protocol),
        }
    }
}

fn advance_forming_bar(
    previous: &mut Option<crate::GateFormingBar>,
    last_closed_open_time_ms: &mut Option<u64>,
    candidate: crate::GateFormingBar,
    binding: &GatePublicBinding,
    generation: u64,
    received_at_ms: u64,
) -> Result<Option<PublicBar>, GatePublicWsError> {
    if let Some(current) = previous.as_ref() {
        if candidate.open_time_ms < current.open_time_ms {
            return Err(GatePublicWsError::Protocol);
        }
        if candidate.open_time_ms == current.open_time_ms {
            if last_closed_open_time_ms == &Some(candidate.open_time_ms) {
                return (current == &candidate)
                    .then_some(None)
                    .ok_or(GatePublicWsError::Protocol);
            }
            if candidate.window_closed != Some(true) {
                *previous = Some(candidate);
                return Ok(None);
            }
        }
    }
    if last_closed_open_time_ms.is_some_and(|closed| candidate.open_time_ms < closed) {
        return Err(GatePublicWsError::Protocol);
    }
    if candidate.window_closed == Some(true) {
        let open_time_ms = candidate.open_time_ms;
        let bar = candidate
            .clone()
            .into_closed(binding.symbol.clone(), generation, received_at_ms)
            .map_err(|_| GatePublicWsError::Protocol)?;
        *previous = Some(candidate);
        *last_closed_open_time_ms = Some(open_time_ms);
        return Ok(Some(bar));
    }
    *previous = Some(candidate);
    Ok(None)
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
            value
                .get("channel")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|channel| {
                    matches!(
                        channel,
                        "futures.order_book_update" | "futures.trades" | "futures.candlesticks"
                    )
                })
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
        assert!(is_subscription_ack(
            r#"{"channel":"futures.candlesticks","event":"subscribe","error":null}"#
        ));
        assert!(!is_subscription_ack(
            r#"{"channel":"futures.book_ticker","event":"subscribe"}"#
        ));
    }

    #[test]
    fn only_documented_close_marker_emits_a_closed_bar() -> Result<(), Box<dyn std::error::Error>> {
        let binding = crate::GatePublicBinding::new("DOGE/USDT".parse()?, "DOGE_USDT", 10.into())?;
        let first = crate::parse_ws_forming_bar_batch(&binding, crate::GatePublicRawPayload::new(
            &binding, crate::GatePublicPayloadKind::WebSocketCandlestick, 7, 1_000,
            r#"{"time_ms":120001,"channel":"futures.candlesticks","event":"update","result":[{"t":120,"v":"1","o":"0.1","h":"0.2","l":"0.1","c":"0.15","n":"1m_DOGE_USDT"}]}"#.to_owned(),
        )?)?.bars.remove(0);
        let closed_candidate = crate::parse_ws_forming_bar_batch(&binding, crate::GatePublicRawPayload::new(
            &binding, crate::GatePublicPayloadKind::WebSocketCandlestick, 7, 61_000,
            r#"{"time_ms":120002,"channel":"futures.candlesticks","event":"update","result":[{"t":120,"v":"2","o":"0.1","h":"0.2","l":"0.1","c":"0.16","n":"1m_DOGE_USDT","w":true}]}"#.to_owned(),
        )?)?.bars.remove(0);
        let mut cached = None;
        let mut last_closed = None;
        assert!(
            advance_forming_bar(&mut cached, &mut last_closed, first, &binding, 7, 1_000)?
                .is_none()
        );
        let closed = advance_forming_bar(
            &mut cached,
            &mut last_closed,
            closed_candidate,
            &binding,
            7,
            61_000,
        )?
        .ok_or(crate::GatePublicError::Payload)?;
        assert_eq!(closed.received_at_ms, 61_000);
        assert_eq!(closed.close.value().to_string(), "0.16");
        let duplicate = crate::parse_ws_forming_bar_batch(&binding, crate::GatePublicRawPayload::new(
            &binding, crate::GatePublicPayloadKind::WebSocketCandlestick, 7, 62_000,
            r#"{"time_ms":120003,"channel":"futures.candlesticks","event":"update","result":[{"t":120,"v":"2","o":"0.1","h":"0.2","l":"0.1","c":"0.16","n":"1m_DOGE_USDT","w":true}]}"#.to_owned(),
        )?)?.bars.remove(0);
        assert!(
            advance_forming_bar(
                &mut cached,
                &mut last_closed,
                duplicate,
                &binding,
                7,
                62_000,
            )?
            .is_none()
        );
        let conflict = crate::parse_ws_forming_bar_batch(&binding, crate::GatePublicRawPayload::new(
            &binding, crate::GatePublicPayloadKind::WebSocketCandlestick, 7, 63_000,
            r#"{"time_ms":120004,"channel":"futures.candlesticks","event":"update","result":[{"t":120,"v":"2","o":"0.1","h":"0.2","l":"0.1","c":"0.17","n":"1m_DOGE_USDT","w":true}]}"#.to_owned(),
        )?)?.bars.remove(0);
        assert_eq!(
            advance_forming_bar(&mut cached, &mut last_closed, conflict, &binding, 7, 63_000),
            Err(GatePublicWsError::Protocol)
        );
        let rollback = crate::parse_ws_forming_bar_batch(&binding, crate::GatePublicRawPayload::new(
            &binding, crate::GatePublicPayloadKind::WebSocketCandlestick, 7, 64_000,
            r#"{"time_ms":60005,"channel":"futures.candlesticks","event":"update","result":[{"t":60,"v":"1","o":"0.1","h":"0.1","l":"0.1","c":"0.1","n":"1m_DOGE_USDT","w":true}]}"#.to_owned(),
        )?)?.bars.remove(0);
        assert_eq!(
            advance_forming_bar(&mut cached, &mut last_closed, rollback, &binding, 7, 64_000),
            Err(GatePublicWsError::Protocol)
        );
        Ok(())
    }
}
