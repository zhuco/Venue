use std::{
    collections::VecDeque,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use reqwest::header::CONTENT_TYPE;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

use crate::{
    HyperliquidConfig, HyperliquidError, HyperliquidExchangeRequest, HyperliquidInfoRequest,
    HyperliquidPrivateStreamBinding, HyperliquidPrivateStreamDecoder,
    HyperliquidPrivateSubscriptionKind, HyperliquidReadBinding, build_private_subscription,
};

const MAX_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_REQUEST_BYTES: usize = 256 * 1024;
const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;
const MAX_PRELIVE_FRAMES: usize = 64;
const PRIVATE_HEARTBEAT_IDLE: Duration = Duration::from_secs(30);
const PRIVATE_PING: &str = r#"{"method":"ping"}"#;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidHttpResponse {
    pub binding: HyperliquidReadBinding,
    pub body: Bytes,
    pub received_at_ms: u64,
}

#[derive(Clone)]
pub struct HyperliquidHttpTransport {
    client: reqwest::Client,
    timeout: Duration,
    max_body_bytes: usize,
    #[cfg(test)]
    origin_override: Option<String>,
}

impl std::fmt::Debug for HyperliquidHttpTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HyperliquidHttpTransport")
            .field("timeout", &self.timeout)
            .field("max_body_bytes", &self.max_body_bytes)
            .field("endpoint", &"[BOUND]")
            .finish()
    }
}

impl HyperliquidHttpTransport {
    pub fn new(
        timeout: Duration,
        max_body_bytes: usize,
    ) -> Result<Self, HyperliquidTransportError> {
        validate_limits(timeout, max_body_bytes)?;
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(timeout)
            .timeout(timeout)
            .build()
            .map_err(|_| HyperliquidTransportError::Configuration)?;
        Ok(Self {
            client,
            timeout,
            max_body_bytes,
            #[cfg(test)]
            origin_override: None,
        })
    }

    #[cfg(test)]
    fn for_test(
        timeout: Duration,
        max_body_bytes: usize,
        origin: String,
    ) -> Result<Self, HyperliquidTransportError> {
        let mut value = Self::new(timeout, max_body_bytes)?;
        value.origin_override = Some(origin);
        Ok(value)
    }

    pub async fn post_info(
        &self,
        expected_binding: &HyperliquidReadBinding,
        request: &HyperliquidInfoRequest,
    ) -> Result<HyperliquidHttpResponse, HyperliquidTransportError> {
        let config = HyperliquidConfig::for_binding(expected_binding.gateway());
        if request.binding() != expected_binding
            || request.mode() != config.mode()
            || request.rest_origin() != config.rest_origin()
            || request.endpoint() != "/info"
        {
            return Err(HyperliquidTransportError::Binding);
        }
        if request.body().is_empty() || request.body().len() > MAX_REQUEST_BYTES {
            return Err(HyperliquidTransportError::Protocol);
        }
        self.post_json(
            expected_binding,
            request.rest_origin(),
            request.endpoint(),
            request.body(),
        )
        .await
    }

    /// Dispatches exactly one already-signed request. The client policy is explicitly `never`, so
    /// timeout, disconnect, and 5xx results return to the WAL owner as UNKNOWN/rejected evidence
    /// instead of replaying a nonce behind its back.
    pub async fn post_exchange(
        &self,
        expected_binding: &HyperliquidReadBinding,
        request: &HyperliquidExchangeRequest,
    ) -> Result<HyperliquidHttpResponse, HyperliquidTransportError> {
        let config = HyperliquidConfig::for_binding(expected_binding.gateway());
        if request.binding() != expected_binding
            || request.mode() != config.mode()
            || request.source().mode() != config.mode()
            || request.rest_origin() != config.rest_origin()
            || request.endpoint() != "/exchange"
        {
            return Err(HyperliquidTransportError::Binding);
        }
        if request.body().is_empty() || request.body().len() > MAX_REQUEST_BYTES {
            return Err(HyperliquidTransportError::Protocol);
        }
        self.post_json(
            expected_binding,
            request.rest_origin(),
            request.endpoint(),
            request.body(),
        )
        .await
    }

    async fn post_json(
        &self,
        expected_binding: &HyperliquidReadBinding,
        rest_origin: &str,
        endpoint: &str,
        body_bytes: &[u8],
    ) -> Result<HyperliquidHttpResponse, HyperliquidTransportError> {
        #[cfg(test)]
        let origin = self.origin_override.as_deref().unwrap_or(rest_origin);
        #[cfg(not(test))]
        let origin = rest_origin;
        let url = format!("{}{}", origin.trim_end_matches('/'), endpoint);
        let send = self
            .client
            .post(url)
            .header(CONTENT_TYPE, "application/json")
            .body(body_bytes.to_vec())
            .send();
        let mut response = tokio::time::timeout(self.timeout, send)
            .await
            .map_err(|_| HyperliquidTransportError::Timeout)?
            .map_err(map_http_error)?;
        if !response.status().is_success() {
            return Err(HyperliquidTransportError::HttpStatus(
                response.status().as_u16(),
            ));
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.max_body_bytes as u64)
        {
            return Err(HyperliquidTransportError::BodyTooLarge);
        }
        let mut body = BytesMut::new();
        while let Some(chunk) = tokio::time::timeout(self.timeout, response.chunk())
            .await
            .map_err(|_| HyperliquidTransportError::Timeout)?
            .map_err(map_http_error)?
        {
            if body.len().saturating_add(chunk.len()) > self.max_body_bytes {
                return Err(HyperliquidTransportError::BodyTooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(HyperliquidHttpResponse {
            binding: expected_binding.clone(),
            body: body.freeze(),
            received_at_ms: now_ms()?,
        })
    }
}

fn map_http_error(error: reqwest::Error) -> HyperliquidTransportError {
    if error.is_timeout() {
        HyperliquidTransportError::Timeout
    } else {
        HyperliquidTransportError::Http
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReceivedPrivateFrame {
    pub binding: HyperliquidPrivateStreamBinding,
    pub payload: Bytes,
    pub received_at_ms: u64,
}

struct BufferedFrame {
    payload: Bytes,
    received_at_ms: u64,
}

type HyperliquidSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct HyperliquidPrivateWsTransport {
    binding: HyperliquidPrivateStreamBinding,
    decoder: HyperliquidPrivateStreamDecoder,
    socket: HyperliquidSocket,
    timeout: Duration,
    heartbeat_idle: Duration,
    max_frame_bytes: usize,
    pending: VecDeque<BufferedFrame>,
    pending_bytes: usize,
}

impl std::fmt::Debug for HyperliquidPrivateWsTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HyperliquidPrivateWsTransport")
            .field("binding", &self.binding)
            .field("transport", &"[BOUND]")
            .field("pending_frames", &self.pending.len())
            .finish()
    }
}

impl HyperliquidPrivateWsTransport {
    pub async fn connect(
        binding: HyperliquidPrivateStreamBinding,
        timeout: Duration,
        max_frame_bytes: usize,
    ) -> Result<Self, HyperliquidTransportError> {
        let config = HyperliquidConfig::for_binding(binding.scope().binding().gateway());
        Self::connect_target(
            binding,
            timeout,
            PRIVATE_HEARTBEAT_IDLE,
            max_frame_bytes,
            config.websocket(),
        )
        .await
    }

    #[cfg(test)]
    async fn connect_for_test(
        binding: HyperliquidPrivateStreamBinding,
        timeout: Duration,
        max_frame_bytes: usize,
        target: &str,
    ) -> Result<Self, HyperliquidTransportError> {
        Self::connect_target(
            binding,
            timeout,
            PRIVATE_HEARTBEAT_IDLE,
            max_frame_bytes,
            target,
        )
        .await
    }

    #[cfg(test)]
    async fn connect_for_heartbeat_test(
        binding: HyperliquidPrivateStreamBinding,
        timeout: Duration,
        heartbeat_idle: Duration,
        max_frame_bytes: usize,
        target: &str,
    ) -> Result<Self, HyperliquidTransportError> {
        Self::connect_target(binding, timeout, heartbeat_idle, max_frame_bytes, target).await
    }

    async fn connect_target(
        binding: HyperliquidPrivateStreamBinding,
        timeout: Duration,
        heartbeat_idle: Duration,
        max_frame_bytes: usize,
        target: &str,
    ) -> Result<Self, HyperliquidTransportError> {
        validate_limits(timeout, max_frame_bytes)?;
        if heartbeat_idle.is_zero() || heartbeat_idle >= Duration::from_secs(60) {
            return Err(HyperliquidTransportError::Configuration);
        }
        let websocket = WebSocketConfig::default()
            .max_message_size(Some(max_frame_bytes))
            .max_frame_size(Some(max_frame_bytes));
        let connection = connect_async_with_config(target, Some(websocket), true);
        let (socket, _) = tokio::time::timeout(timeout, connection)
            .await
            .map_err(|_| HyperliquidTransportError::Timeout)?
            .map_err(|_| HyperliquidTransportError::WebSocket)?;
        let decoder = HyperliquidPrivateStreamDecoder::new(binding.clone());
        let mut value = Self {
            binding,
            decoder,
            socket,
            timeout,
            heartbeat_idle,
            max_frame_bytes,
            pending: VecDeque::new(),
            pending_bytes: 0,
        };
        let subscribed = tokio::time::timeout(timeout, value.subscribe_all()).await;
        match subscribed {
            Ok(Ok(())) => Ok(value),
            Ok(Err(error)) => value.fail(error).await,
            Err(_) => value.fail(HyperliquidTransportError::Timeout).await,
        }
    }

    #[must_use]
    pub const fn binding(&self) -> &HyperliquidPrivateStreamBinding {
        &self.binding
    }

    pub async fn next_frame(
        &mut self,
        expected_binding: &HyperliquidPrivateStreamBinding,
    ) -> Result<ReceivedPrivateFrame, HyperliquidTransportError> {
        if expected_binding != &self.binding {
            return self.fail(HyperliquidTransportError::Binding).await;
        }
        let buffered = if let Some(buffered) = self.pending.pop_front() {
            self.pending_bytes = self.pending_bytes.saturating_sub(buffered.payload.len());
            buffered
        } else {
            match self.read_private_data().await {
                Ok(frame) => frame,
                Err(error) => return self.fail(error).await,
            }
        };
        if let Err(error) = self.decoder.decode(
            &buffered.payload,
            self.binding.generation(),
            buffered.received_at_ms,
        ) {
            let mapped = match error {
                HyperliquidError::Binding => HyperliquidTransportError::Binding,
                _ => HyperliquidTransportError::Protocol,
            };
            return self.fail(mapped).await;
        }
        Ok(ReceivedPrivateFrame {
            binding: self.binding.clone(),
            payload: buffered.payload,
            received_at_ms: buffered.received_at_ms,
        })
    }

    pub async fn close(&mut self) -> Result<(), HyperliquidTransportError> {
        self.pending.clear();
        self.pending_bytes = 0;
        self.send_close().await
    }

    async fn subscribe_all(&mut self) -> Result<(), HyperliquidTransportError> {
        for kind in [
            HyperliquidPrivateSubscriptionKind::OrderUpdates,
            HyperliquidPrivateSubscriptionKind::UserFills,
            HyperliquidPrivateSubscriptionKind::UserEvents,
        ] {
            let request = build_private_subscription(&self.binding, kind)
                .map_err(|_| HyperliquidTransportError::Binding)?;
            if request.binding() != &self.binding {
                return Err(HyperliquidTransportError::Binding);
            }
            let body = std::str::from_utf8(request.body())
                .map_err(|_| HyperliquidTransportError::Protocol)?;
            let request_value: serde_json::Value = serde_json::from_slice(request.body())
                .map_err(|_| HyperliquidTransportError::Protocol)?;
            let expected = request_value
                .get("subscription")
                .cloned()
                .ok_or(HyperliquidTransportError::Protocol)?;
            self.socket
                .send(Message::Text(body.to_owned().into()))
                .await
                .map_err(|_| HyperliquidTransportError::WebSocket)?;
            self.await_ack(&expected).await?;
        }
        Ok(())
    }

    async fn await_ack(
        &mut self,
        expected_subscription: &serde_json::Value,
    ) -> Result<(), HyperliquidTransportError> {
        loop {
            let message = self
                .socket
                .next()
                .await
                .ok_or(HyperliquidTransportError::Closed)?
                .map_err(|_| HyperliquidTransportError::WebSocket)?;
            match message {
                Message::Text(value) => {
                    let payload = self.checked_bytes(value.as_bytes())?;
                    if ack_matches(&payload, expected_subscription)? {
                        return Ok(());
                    }
                    self.buffer_private(payload)?;
                }
                Message::Binary(value) => {
                    let payload = self.checked_bytes(&value)?;
                    if ack_matches(&payload, expected_subscription)? {
                        return Ok(());
                    }
                    self.buffer_private(payload)?;
                }
                Message::Ping(value) => self
                    .socket
                    .send(Message::Pong(value))
                    .await
                    .map_err(|_| HyperliquidTransportError::WebSocket)?,
                Message::Pong(_) => {}
                Message::Close(_) => return Err(HyperliquidTransportError::Closed),
                Message::Frame(_) => return Err(HyperliquidTransportError::Protocol),
            }
        }
    }

    async fn read_private_data(&mut self) -> Result<BufferedFrame, HyperliquidTransportError> {
        loop {
            let message = match tokio::time::timeout(self.heartbeat_idle, self.socket.next()).await
            {
                Ok(Some(Ok(message))) => message,
                Ok(Some(Err(_))) => return Err(HyperliquidTransportError::WebSocket),
                Ok(None) => return Err(HyperliquidTransportError::Closed),
                Err(_) => {
                    self.send_application_ping().await?;
                    self.await_application_pong().await?;
                    if let Some(buffered) = self.pending.pop_front() {
                        self.pending_bytes =
                            self.pending_bytes.saturating_sub(buffered.payload.len());
                        return Ok(buffered);
                    }
                    continue;
                }
            };
            match message {
                Message::Text(value) => {
                    let payload = self.checked_bytes(value.as_bytes())?;
                    if is_application_pong(&payload)? {
                        return Err(HyperliquidTransportError::Protocol);
                    }
                    return self.private_frame(&payload);
                }
                Message::Binary(value) => return self.private_frame(&value),
                Message::Ping(value) => self
                    .socket
                    .send(Message::Pong(value))
                    .await
                    .map_err(|_| HyperliquidTransportError::WebSocket)?,
                Message::Pong(_) => {}
                Message::Close(_) => return Err(HyperliquidTransportError::Closed),
                Message::Frame(_) => return Err(HyperliquidTransportError::Protocol),
            }
        }
    }

    async fn send_application_ping(&mut self) -> Result<(), HyperliquidTransportError> {
        tokio::time::timeout(
            self.timeout,
            self.socket.send(Message::Text(PRIVATE_PING.into())),
        )
        .await
        .map_err(|_| HyperliquidTransportError::Timeout)?
        .map_err(|_| HyperliquidTransportError::WebSocket)
    }

    async fn await_application_pong(&mut self) -> Result<(), HyperliquidTransportError> {
        tokio::time::timeout(self.timeout, async {
            loop {
                let message = self
                    .socket
                    .next()
                    .await
                    .ok_or(HyperliquidTransportError::Closed)?
                    .map_err(|_| HyperliquidTransportError::WebSocket)?;
                match message {
                    Message::Text(value) => {
                        let payload = self.checked_bytes(value.as_bytes())?;
                        if is_application_pong(&payload)? {
                            return Ok(());
                        }
                        if !is_private_frame(&payload)? {
                            return Err(HyperliquidTransportError::Protocol);
                        }
                        self.buffer_private(payload)?;
                    }
                    Message::Binary(value) => {
                        let payload = self.checked_bytes(&value)?;
                        if is_application_pong(&payload)? || !is_private_frame(&payload)? {
                            return Err(HyperliquidTransportError::Protocol);
                        }
                        self.buffer_private(payload)?;
                    }
                    Message::Frame(_) => return Err(HyperliquidTransportError::Protocol),
                    Message::Ping(value) => self
                        .socket
                        .send(Message::Pong(value))
                        .await
                        .map_err(|_| HyperliquidTransportError::WebSocket)?,
                    Message::Pong(_) => {}
                    Message::Close(_) => return Err(HyperliquidTransportError::Closed),
                }
            }
        })
        .await
        .map_err(|_| HyperliquidTransportError::Timeout)?
    }

    fn private_frame(&self, value: &[u8]) -> Result<BufferedFrame, HyperliquidTransportError> {
        let payload = self.checked_bytes(value)?;
        if is_ack(&payload)? || !is_private_frame(&payload)? {
            return Err(HyperliquidTransportError::Protocol);
        }
        Ok(BufferedFrame {
            payload,
            received_at_ms: now_ms()?,
        })
    }

    fn checked_bytes(&self, value: &[u8]) -> Result<Bytes, HyperliquidTransportError> {
        if value.is_empty() || value.len() > self.max_frame_bytes {
            return Err(HyperliquidTransportError::BodyTooLarge);
        }
        Ok(Bytes::copy_from_slice(value))
    }

    fn buffer_private(&mut self, payload: Bytes) -> Result<(), HyperliquidTransportError> {
        if is_ack(&payload)? || !is_private_frame(&payload)? {
            return Err(HyperliquidTransportError::Ack);
        }
        if self.pending.len() >= MAX_PRELIVE_FRAMES
            || self.pending_bytes.saturating_add(payload.len()) > self.max_frame_bytes
        {
            return Err(HyperliquidTransportError::BodyTooLarge);
        }
        self.pending_bytes += payload.len();
        self.pending.push_back(BufferedFrame {
            payload,
            received_at_ms: now_ms()?,
        });
        Ok(())
    }

    async fn fail<T>(
        &mut self,
        error: HyperliquidTransportError,
    ) -> Result<T, HyperliquidTransportError> {
        self.pending.clear();
        self.pending_bytes = 0;
        let _ = self.send_close().await;
        Err(error)
    }

    async fn send_close(&mut self) -> Result<(), HyperliquidTransportError> {
        tokio::time::timeout(self.timeout, self.socket.send(Message::Close(None)))
            .await
            .map_err(|_| HyperliquidTransportError::Timeout)?
            .map_err(|_| HyperliquidTransportError::Closed)
    }
}

fn ack_matches(
    payload: &[u8],
    expected_subscription: &serde_json::Value,
) -> Result<bool, HyperliquidTransportError> {
    let value: serde_json::Value =
        serde_json::from_slice(payload).map_err(|_| HyperliquidTransportError::Protocol)?;
    if value.get("channel").and_then(serde_json::Value::as_str) != Some("subscriptionResponse") {
        return Ok(false);
    }
    let data = value
        .get("data")
        .and_then(serde_json::Value::as_object)
        .ok_or(HyperliquidTransportError::Ack)?;
    if data.get("method").and_then(serde_json::Value::as_str) != Some("subscribe")
        || data.get("subscription") != Some(expected_subscription)
    {
        return Err(HyperliquidTransportError::Ack);
    }
    Ok(true)
}

fn is_ack(payload: &[u8]) -> Result<bool, HyperliquidTransportError> {
    let value: serde_json::Value =
        serde_json::from_slice(payload).map_err(|_| HyperliquidTransportError::Protocol)?;
    Ok(value.get("channel").and_then(serde_json::Value::as_str) == Some("subscriptionResponse"))
}

fn is_private_frame(payload: &[u8]) -> Result<bool, HyperliquidTransportError> {
    let value: serde_json::Value =
        serde_json::from_slice(payload).map_err(|_| HyperliquidTransportError::Protocol)?;
    Ok(matches!(
        value.get("channel").and_then(serde_json::Value::as_str),
        Some("orderUpdates" | "userFills" | "userEvents" | "user")
    ))
}

fn is_application_pong(payload: &[u8]) -> Result<bool, HyperliquidTransportError> {
    let value: serde_json::Value =
        serde_json::from_slice(payload).map_err(|_| HyperliquidTransportError::Protocol)?;
    let object = value
        .as_object()
        .ok_or(HyperliquidTransportError::Protocol)?;
    Ok(object.len() == 1
        && object.get("channel").and_then(serde_json::Value::as_str) == Some("pong"))
}

fn validate_limits(
    timeout: Duration,
    max_body_bytes: usize,
) -> Result<(), HyperliquidTransportError> {
    if timeout.is_zero()
        || timeout > MAX_TIMEOUT
        || max_body_bytes == 0
        || max_body_bytes > MAX_BODY_BYTES
    {
        Err(HyperliquidTransportError::Configuration)
    } else {
        Ok(())
    }
}

fn now_ms() -> Result<u64, HyperliquidTransportError> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| HyperliquidTransportError::Clock)?
            .as_millis(),
    )
    .map_err(|_| HyperliquidTransportError::Clock)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum HyperliquidTransportError {
    #[error("Hyperliquid transport configuration is invalid")]
    Configuration,
    #[error("Hyperliquid transport binding or generation does not match")]
    Binding,
    #[error("Hyperliquid transport timed out")]
    Timeout,
    #[error("Hyperliquid HTTP request or response failed")]
    Http,
    #[error("Hyperliquid HTTP returned non-success status {0}")]
    HttpStatus(u16),
    #[error("Hyperliquid response or frame exceeds the configured bound")]
    BodyTooLarge,
    #[error("Hyperliquid websocket connection failed")]
    WebSocket,
    #[error("Hyperliquid websocket subscription acknowledgement is invalid")]
    Ack,
    #[error("Hyperliquid websocket closed or reached EOF")]
    Closed,
    #[error("Hyperliquid transport received an invalid protocol frame")]
    Protocol,
    #[error("system clock cannot produce a Unix millisecond timestamp")]
    Clock,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        HyperliquidAloOrder, HyperliquidCredentials, HyperliquidGatewayBinding,
        HyperliquidNonceStore, HyperliquidPerpMeta, NonceCheckpoint, build_alo_place_request,
        reserve_next_nonce,
    };
    use rust_decimal::Decimal;
    use std::io::{Error as IoError, ErrorKind};
    use tokio::{net::TcpListener, task::JoinHandle};
    use tokio_tungstenite::{accept_async, tungstenite::Message};
    use venue_domain::domain::OrderSide;
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

    const USER: &str = "0x0000000000000000000000000000000000000001";
    const OTHER_USER: &str = "0x3333333333333333333333333333333333333333";
    const AGENT: &str = "0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a";
    const AGENT_KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    const PRIVATE_FRAME: &[u8] = br#"{"channel":"orderUpdates","data":[{"order":{"coin":"BTC","side":"B","limitPx":"65000.5","sz":"0.4","oid":101,"timestamp":1700000000000,"origSz":"1.0","cloid":"0x00000000000000000000000000000001"},"status":"open","statusTimestamp":1700000000001}]}"#;

    fn read_binding(
        mode: GatewayMode,
        user: &str,
    ) -> Result<HyperliquidReadBinding, Box<dyn std::error::Error>> {
        let gateway = crate::HyperliquidGatewayBinding::new(GatewayBinding::new(
            VenueId::Hyperliquid,
            mode,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDC".parse()?,
        )?)?;
        Ok(HyperliquidReadBinding::new(gateway, user)?)
    }

    fn private_binding(
        mode: GatewayMode,
        generation: u64,
    ) -> Result<HyperliquidPrivateStreamBinding, Box<dyn std::error::Error>> {
        private_binding_for(mode, generation, USER, "BTC/USDC", "BTC")
    }

    fn private_binding_for(
        mode: GatewayMode,
        generation: u64,
        user: &str,
        symbol: &str,
        native_coin: &str,
    ) -> Result<HyperliquidPrivateStreamBinding, Box<dyn std::error::Error>> {
        let gateway = crate::HyperliquidGatewayBinding::new(GatewayBinding::new(
            VenueId::Hyperliquid,
            mode,
            "00000000-0000-4000-8000-000000000001",
            symbol.parse()?,
        )?)?;
        let read = HyperliquidReadBinding::new(gateway, user)?;
        let payload = serde_json::to_vec(&serde_json::json!({
            "universe": [{"name": native_coin, "szDecimals": 5, "maxLeverage": 50}]
        }))?;
        let meta = crate::parse_perp_meta(&payload, &read)?;
        Ok(HyperliquidPrivateStreamBinding::new(&meta, generation)?)
    }

    fn meta(mode: GatewayMode) -> Result<HyperliquidPerpMeta, Box<dyn std::error::Error>> {
        let gateway = HyperliquidGatewayBinding::new(GatewayBinding::new(
            VenueId::Hyperliquid,
            mode,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDC".parse()?,
        )?)?;
        let read = HyperliquidReadBinding::new(gateway, USER)?;
        Ok(crate::parse_perp_meta(
            br#"{"universe":[{"name":"BTC","szDecimals":5,"maxLeverage":50}]}"#,
            &read,
        )?)
    }

    #[derive(Default)]
    struct MemoryNonceStore(Option<NonceCheckpoint>);

    impl HyperliquidNonceStore for MemoryNonceStore {
        fn load(
            &mut self,
            _agent_address: &str,
        ) -> Result<Option<NonceCheckpoint>, HyperliquidError> {
            Ok(self.0.clone())
        }

        fn persist(&mut self, checkpoint: &NonceCheckpoint) -> Result<(), HyperliquidError> {
            self.0 = Some(checkpoint.clone());
            Ok(())
        }
    }

    enum HttpMock {
        Response { status: u16, body: Vec<u8> },
        Delayed(Duration),
        Disconnect,
    }

    async fn spawn_http_mock(
        behavior: HttpMock,
    ) -> Result<(String, JoinHandle<()>), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let task = tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            if read_headers(&stream).await.is_err() {
                return;
            }
            match behavior {
                HttpMock::Response { status, body } => {
                    let reason = if status == 200 { "OK" } else { "ERROR" };
                    let header = format!(
                        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let mut response = header.into_bytes();
                    response.extend_from_slice(&body);
                    let _ = write_all(&stream, &response).await;
                }
                HttpMock::Delayed(delay) => tokio::time::sleep(delay).await,
                HttpMock::Disconnect => {}
            }
        });
        Ok((format!("http://{address}"), task))
    }

    async fn spawn_counting_http_error_mock()
    -> Result<(String, JoinHandle<usize>), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let task = tokio::spawn(async move {
            let mut accepted = 0;
            for _ in 0..2 {
                let Ok(Ok((stream, _))) =
                    tokio::time::timeout(Duration::from_millis(300), listener.accept()).await
                else {
                    break;
                };
                accepted += 1;
                if read_headers(&stream).await.is_ok() {
                    let _ = write_all(
                        &stream,
                        b"HTTP/1.1 503 ERROR\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await;
                }
            }
            accepted
        });
        Ok((format!("http://{address}"), task))
    }

    async fn read_headers(stream: &TcpStream) -> Result<(), IoError> {
        let mut received = Vec::new();
        let mut buffer = [0_u8; 1024];
        while !received.windows(4).any(|window| window == b"\r\n\r\n") {
            stream.readable().await?;
            match stream.try_read(&mut buffer) {
                Ok(0) => return Err(IoError::new(ErrorKind::UnexpectedEof, "request EOF")),
                Ok(count) => received.extend_from_slice(&buffer[..count]),
                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                Err(error) => return Err(error),
            }
            if received.len() > 16 * 1024 {
                return Err(IoError::new(ErrorKind::InvalidData, "headers too large"));
            }
        }
        Ok(())
    }

    async fn write_all(stream: &TcpStream, bytes: &[u8]) -> Result<(), IoError> {
        let mut offset = 0;
        while offset < bytes.len() {
            stream.writable().await?;
            match stream.try_write(&bytes[offset..]) {
                Ok(0) => return Err(IoError::new(ErrorKind::WriteZero, "response write zero")),
                Ok(count) => offset += count,
                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn http_transport_enforces_binding_status_size_timeout_and_disconnect()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = read_binding(GatewayMode::Test, USER)?;
        let request = crate::build_meta_request(&binding)?;
        let (origin, task) = spawn_http_mock(HttpMock::Response {
            status: 200,
            body: br#"{"universe":[]}"#.to_vec(),
        })
        .await?;
        let transport =
            HyperliquidHttpTransport::for_test(Duration::from_millis(250), 128, origin)?;
        let response = transport.post_info(&binding, &request).await?;
        assert_eq!(response.binding, binding);
        assert_eq!(response.body, Bytes::from_static(br#"{"universe":[]}"#));
        assert!(response.received_at_ms > 0);
        task.await?;

        let wrong = read_binding(GatewayMode::Test, OTHER_USER)?;
        assert_eq!(
            transport.post_info(&wrong, &request).await,
            Err(HyperliquidTransportError::Binding)
        );

        let (origin, task) = spawn_http_mock(HttpMock::Response {
            status: 503,
            body: Vec::new(),
        })
        .await?;
        let status = HyperliquidHttpTransport::for_test(Duration::from_millis(250), 128, origin)?;
        assert_eq!(
            status.post_info(&binding, &request).await,
            Err(HyperliquidTransportError::HttpStatus(503))
        );
        task.await?;

        let (origin, task) = spawn_http_mock(HttpMock::Response {
            status: 200,
            body: vec![b'x'; 129],
        })
        .await?;
        let oversized =
            HyperliquidHttpTransport::for_test(Duration::from_millis(250), 128, origin)?;
        assert_eq!(
            oversized.post_info(&binding, &request).await,
            Err(HyperliquidTransportError::BodyTooLarge)
        );
        task.await?;

        let (origin, task) = spawn_http_mock(HttpMock::Delayed(Duration::from_millis(100))).await?;
        let delayed = HyperliquidHttpTransport::for_test(Duration::from_millis(20), 128, origin)?;
        assert_eq!(
            delayed.post_info(&binding, &request).await,
            Err(HyperliquidTransportError::Timeout)
        );
        task.await?;

        let (origin, task) = spawn_http_mock(HttpMock::Disconnect).await?;
        let disconnected =
            HyperliquidHttpTransport::for_test(Duration::from_millis(250), 128, origin)?;
        assert_eq!(
            disconnected.post_info(&binding, &request).await,
            Err(HyperliquidTransportError::Http)
        );
        task.await?;
        Ok(())
    }

    #[tokio::test]
    async fn exchange_transport_dispatches_once_without_automatic_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let meta = meta(GatewayMode::Test)?;
        let credentials =
            HyperliquidCredentials::from_values(USER, USER, None, "venue-agent", AGENT, AGENT_KEY)?;
        let mut nonce_store = MemoryNonceStore::default();
        let nonce = reserve_next_nonce(&mut nonce_store, AGENT, 1_700_000_000_000)?;
        let order = HyperliquidAloOrder::new(
            &meta,
            OrderSide::Buy,
            Decimal::new(65_000, 0),
            Decimal::new(1, 1),
            false,
            "0x00000000000000000000000000000001",
        )?;
        let request = build_alo_place_request(&credentials, nonce, order, None)?;
        let (origin, server) = spawn_counting_http_error_mock().await?;
        let transport =
            HyperliquidHttpTransport::for_test(Duration::from_millis(500), 128, origin)?;
        assert_eq!(
            transport
                .post_exchange(meta.scope.binding(), &request)
                .await,
            Err(HyperliquidTransportError::HttpStatus(503))
        );
        assert_eq!(server.await?, 1);
        Ok(())
    }

    #[derive(Clone, Copy)]
    enum WsMock {
        Success,
        AckFailure,
        AckTimeout,
        DisconnectAfterAck,
        HeartbeatSuccess,
        HeartbeatTimeout,
        HeartbeatWrongPong,
    }

    async fn spawn_ws_mock(
        behavior: WsMock,
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
            if matches!(behavior, WsMock::AckTimeout) {
                tokio::time::sleep(Duration::from_millis(100)).await;
                return;
            }
            for index in 0..3 {
                let Some(Ok(Message::Text(request))) = socket.next().await else {
                    return;
                };
                let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(request.as_bytes())
                else {
                    return;
                };
                if index == 0
                    && matches!(behavior, WsMock::Success)
                    && socket
                        .send(Message::Binary(Bytes::from_static(PRIVATE_FRAME)))
                        .await
                        .is_err()
                {
                    return;
                }
                if index == 0 && matches!(behavior, WsMock::AckFailure) {
                    value["subscription"]["user"] = serde_json::json!(OTHER_USER);
                }
                let ack = serde_json::json!({
                    "channel":"subscriptionResponse",
                    "data":value
                });
                if socket
                    .send(Message::Text(ack.to_string().into()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            if matches!(behavior, WsMock::DisconnectAfterAck) {
                let _ = socket.send(Message::Close(None)).await;
            }
            if matches!(
                behavior,
                WsMock::HeartbeatSuccess | WsMock::HeartbeatTimeout | WsMock::HeartbeatWrongPong
            ) {
                let Some(Ok(Message::Text(ping))) = socket.next().await else {
                    return;
                };
                if ping.as_str() != PRIVATE_PING {
                    return;
                }
                match behavior {
                    WsMock::HeartbeatSuccess => {
                        if socket
                            .send(Message::Text(r#"{"channel":"pong"}"#.into()))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        let _ = socket
                            .send(Message::Binary(Bytes::from_static(PRIVATE_FRAME)))
                            .await;
                    }
                    WsMock::HeartbeatTimeout => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    WsMock::HeartbeatWrongPong => {
                        let _ = socket
                            .send(Message::Text(
                                r#"{"channel":"pong","unexpected":true}"#.into(),
                            ))
                            .await;
                    }
                    _ => {}
                }
            }
        });
        Ok((format!("ws://{address}"), task))
    }

    #[tokio::test]
    async fn websocket_requires_all_exact_acks_before_delivering_bound_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = private_binding(GatewayMode::Test, 31)?;
        let (target, server) = spawn_ws_mock(WsMock::Success).await?;
        let mut transport = HyperliquidPrivateWsTransport::connect_for_test(
            binding.clone(),
            Duration::from_millis(500),
            64 * 1024,
            &target,
        )
        .await?;
        assert_eq!(transport.binding(), &binding);
        let frame = transport.next_frame(&binding).await?;
        assert_eq!(frame.payload, Bytes::from_static(PRIVATE_FRAME));
        assert_eq!(frame.binding, binding);
        assert!(frame.received_at_ms > 0);
        let _ = transport.close().await;
        server.await?;
        Ok(())
    }

    #[tokio::test]
    async fn websocket_fails_closed_on_generation_ack_timeout_and_eof()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = private_binding(GatewayMode::Live, 41)?;
        let (target, server) = spawn_ws_mock(WsMock::Success).await?;
        let mut transport = HyperliquidPrivateWsTransport::connect_for_test(
            binding.clone(),
            Duration::from_millis(500),
            64 * 1024,
            &target,
        )
        .await?;
        let stale = private_binding(GatewayMode::Live, 42)?;
        assert_eq!(
            transport.next_frame(&stale).await,
            Err(HyperliquidTransportError::Binding)
        );
        server.await?;

        let (target, server) = spawn_ws_mock(WsMock::Success).await?;
        let mut transport = HyperliquidPrivateWsTransport::connect_for_test(
            binding.clone(),
            Duration::from_millis(500),
            64 * 1024,
            &target,
        )
        .await?;
        let wrong_scope =
            private_binding_for(GatewayMode::Test, 41, OTHER_USER, "ETH/USDC", "ETH")?;
        assert_eq!(
            transport.next_frame(&wrong_scope).await,
            Err(HyperliquidTransportError::Binding)
        );
        server.await?;

        let (target, server) = spawn_ws_mock(WsMock::AckFailure).await?;
        assert!(matches!(
            HyperliquidPrivateWsTransport::connect_for_test(
                binding.clone(),
                Duration::from_millis(500),
                64 * 1024,
                &target
            )
            .await,
            Err(HyperliquidTransportError::Ack)
        ));
        server.await?;

        let (target, server) = spawn_ws_mock(WsMock::AckTimeout).await?;
        assert!(matches!(
            HyperliquidPrivateWsTransport::connect_for_test(
                binding.clone(),
                Duration::from_millis(20),
                64 * 1024,
                &target
            )
            .await,
            Err(HyperliquidTransportError::Timeout)
        ));
        server.await?;

        let (target, server) = spawn_ws_mock(WsMock::DisconnectAfterAck).await?;
        let mut transport = HyperliquidPrivateWsTransport::connect_for_test(
            binding.clone(),
            Duration::from_millis(500),
            64 * 1024,
            &target,
        )
        .await?;
        assert_eq!(
            transport.next_frame(&binding).await,
            Err(HyperliquidTransportError::Closed)
        );
        server.await?;
        Ok(())
    }

    #[tokio::test]
    async fn websocket_idle_heartbeat_consumes_only_exact_pong_and_stays_bound()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = private_binding(GatewayMode::Live, 51)?;
        let (target, server) = spawn_ws_mock(WsMock::HeartbeatSuccess).await?;
        let mut transport = HyperliquidPrivateWsTransport::connect_for_heartbeat_test(
            binding.clone(),
            Duration::from_millis(50),
            Duration::from_millis(10),
            64 * 1024,
            &target,
        )
        .await?;
        let frame = transport.next_frame(&binding).await?;
        assert_eq!(frame.binding, binding);
        assert_eq!(frame.payload, Bytes::from_static(PRIVATE_FRAME));
        server.await?;

        for (behavior, expected) in [
            (WsMock::HeartbeatTimeout, HyperliquidTransportError::Timeout),
            (
                WsMock::HeartbeatWrongPong,
                HyperliquidTransportError::Protocol,
            ),
        ] {
            let (target, server) = spawn_ws_mock(behavior).await?;
            let mut transport = HyperliquidPrivateWsTransport::connect_for_heartbeat_test(
                private_binding(GatewayMode::Test, 52)?,
                Duration::from_millis(20),
                Duration::from_millis(10),
                64 * 1024,
                &target,
            )
            .await?;
            let expected_binding = transport.binding().clone();
            assert_eq!(transport.next_frame(&expected_binding).await, Err(expected));
            server.await?;
        }
        Ok(())
    }
}
