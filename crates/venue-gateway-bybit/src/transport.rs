use std::{
    collections::VecDeque,
    fmt,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time::{Instant, sleep_until, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Error as WebSocketError, Message, protocol::WebSocketConfig},
};
use venue_gateway_api::GatewayBinding;

use crate::sign::ws_auth_signature;
use crate::{
    BybitCredentials, BybitExecutionError, BybitGatewayBinding, BybitOrderAck,
    BybitPreparedPrivateRequest, BybitPreparedRequest, BybitPrivateStreamProbeEvidence,
    BybitPublicSource, BybitRawPrivatePayload, BybitRawPublicPayload, linear_bbo_path,
    linear_instrument_path, parse_order_ack, sign_prepared_request, sign_private_request,
};

const PRIVATE_TOPICS: [&str; 4] = [
    "order.linear",
    "execution.linear",
    "position.linear",
    "wallet",
];
const MAX_PRE_LIVE_FRAMES: usize = 256;
const MAX_PRE_LIVE_BYTES: usize = 1_048_576;
const MAX_TRANSPORT_BODY_BYTES: usize = 2 * 1_024 * 1_024;
const PRIVATE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BybitTransportLimits {
    operation_timeout: Duration,
    maximum_body_bytes: usize,
}

impl BybitTransportLimits {
    pub fn new(
        operation_timeout: Duration,
        maximum_body_bytes: usize,
    ) -> Result<Self, BybitTransportError> {
        if operation_timeout.is_zero()
            || operation_timeout > Duration::from_secs(60)
            || maximum_body_bytes == 0
            || maximum_body_bytes > MAX_TRANSPORT_BODY_BYTES
        {
            return Err(BybitTransportError::Limits);
        }
        Ok(Self {
            operation_timeout,
            maximum_body_bytes,
        })
    }

    #[must_use]
    pub const fn operation_timeout(self) -> Duration {
        self.operation_timeout
    }

    #[must_use]
    pub const fn maximum_body_bytes(self) -> usize {
        self.maximum_body_bytes
    }
}

pub struct BybitHttpTransport {
    client: reqwest::Client,
    binding: GatewayBinding,
    generation: u64,
    endpoint: String,
    limits: BybitTransportLimits,
}

impl BybitHttpTransport {
    /// Rebinds the same authenticated account transport resources to another canonical symbol.
    /// The returned value carries a distinct binding, so every existing request validation stays
    /// exact; cloning this read client never creates a mutation writer or credential namespace.
    pub(crate) fn clone_with_binding(
        &self,
        binding: &BybitGatewayBinding,
    ) -> Result<Self, BybitTransportError> {
        if binding.gateway_binding().venue != self.binding.venue
            || binding.gateway_binding().mode != self.binding.mode
            || binding.gateway_binding().trading_account_id != self.binding.trading_account_id
        {
            return Err(BybitTransportError::Binding);
        }
        Ok(Self {
            client: self.client.clone(),
            binding: binding.gateway_binding().clone(),
            generation: self.generation,
            endpoint: self.endpoint.clone(),
            limits: self.limits,
        })
    }
    pub fn new(
        binding: &BybitGatewayBinding,
        generation: u64,
        limits: BybitTransportLimits,
    ) -> Result<Self, BybitTransportError> {
        Self::build(
            binding,
            generation,
            binding.config().rest_origin().to_owned(),
            limits,
            false,
        )
    }

    #[cfg(test)]
    pub(crate) fn with_endpoint(
        binding: &BybitGatewayBinding,
        generation: u64,
        endpoint: String,
        limits: BybitTransportLimits,
    ) -> Result<Self, BybitTransportError> {
        Self::build(binding, generation, endpoint, limits, true)
    }

    fn build(
        binding: &BybitGatewayBinding,
        generation: u64,
        endpoint: String,
        limits: BybitTransportLimits,
        disable_proxy: bool,
    ) -> Result<Self, BybitTransportError> {
        if generation == 0 || endpoint.is_empty() {
            return Err(BybitTransportError::Binding);
        }
        let mut builder = reqwest::Client::builder()
            .connect_timeout(limits.operation_timeout)
            .redirect(reqwest::redirect::Policy::none());
        if disable_proxy {
            builder = builder.no_proxy();
        }
        let client = builder.build().map_err(|_| BybitTransportError::Http)?;
        Ok(Self {
            client,
            binding: binding.gateway_binding().clone(),
            generation,
            endpoint,
            limits,
        })
    }

    pub(crate) async fn execute_order(
        &self,
        binding: &BybitGatewayBinding,
        credentials: &BybitCredentials,
        request: &BybitPreparedRequest,
        timestamp_ms: u64,
    ) -> Result<BybitOrderAck, BybitTransportError> {
        binding
            .validate_request_binding(&self.binding)
            .map_err(|_| BybitTransportError::Binding)?;
        request
            .validate(binding)
            .map_err(|_| BybitTransportError::Binding)?;
        if request.generation != self.generation
            || request.origin != binding.config().rest_origin()
            || self.binding != request.binding
        {
            return Err(BybitTransportError::Binding);
        }
        if request.body.len() > self.limits.maximum_body_bytes {
            return Err(BybitTransportError::BodyTooLarge);
        }
        let signed = sign_prepared_request(credentials, binding, request, timestamp_ms)
            .map_err(|_| BybitTransportError::Signing)?;
        let url = format!("{}{}", self.endpoint, request.path);
        let operation = async {
            let mut builder = self
                .client
                .post(url)
                .header("content-type", "application/json")
                .body(request.body.clone());
            for name in [
                "X-BAPI-API-KEY",
                "X-BAPI-SIGN",
                "X-BAPI-SIGN-TYPE",
                "X-BAPI-TIMESTAMP",
                "X-BAPI-RECV-WINDOW",
            ] {
                let value = signed.get(name).ok_or(BybitTransportError::Signing)?;
                builder = builder.header(name, value);
            }
            let mut response = builder.send().await.map_err(map_reqwest)?;
            if !response.status().is_success() {
                return Err(BybitTransportError::HttpStatus);
            }
            if response
                .content_length()
                .is_some_and(|length| length > self.limits.maximum_body_bytes as u64)
            {
                return Err(BybitTransportError::BodyTooLarge);
            }
            let mut body = BytesMut::new();
            while let Some(chunk) = response.chunk().await.map_err(map_reqwest)? {
                let new_length = body
                    .len()
                    .checked_add(chunk.len())
                    .ok_or(BybitTransportError::BodyTooLarge)?;
                if new_length > self.limits.maximum_body_bytes {
                    return Err(BybitTransportError::BodyTooLarge);
                }
                body.extend_from_slice(&chunk);
            }
            let received_at_ms = unix_ms()?;
            match parse_order_ack(binding, request, &body, received_at_ms) {
                Ok(ack) => Ok(ack),
                Err(BybitExecutionError::VenueRejected) => Err(BybitTransportError::Rejected),
                Err(_) => Err(BybitTransportError::Ack),
            }
        };
        timeout(self.limits.operation_timeout, operation)
            .await
            .map_err(|_| BybitTransportError::Timeout)?
    }

    pub(crate) async fn fetch_linear_instrument(
        &self,
        binding: &BybitGatewayBinding,
    ) -> Result<BybitRawPublicPayload, BybitTransportError> {
        binding
            .validate_request_binding(&self.binding)
            .map_err(|_| BybitTransportError::Binding)?;
        let path = linear_instrument_path(binding).map_err(|_| BybitTransportError::Binding)?;
        let url = format!("{}{}", self.endpoint, path);
        let operation = async {
            let mut response = self.client.get(url).send().await.map_err(map_reqwest)?;
            if !response.status().is_success() {
                return Err(BybitTransportError::HttpStatus);
            }
            if response
                .content_length()
                .is_some_and(|length| length > self.limits.maximum_body_bytes as u64)
            {
                return Err(BybitTransportError::BodyTooLarge);
            }
            let mut body = BytesMut::new();
            while let Some(chunk) = response.chunk().await.map_err(map_reqwest)? {
                let new_length = body
                    .len()
                    .checked_add(chunk.len())
                    .ok_or(BybitTransportError::BodyTooLarge)?;
                if new_length > self.limits.maximum_body_bytes {
                    return Err(BybitTransportError::BodyTooLarge);
                }
                body.extend_from_slice(&chunk);
            }
            let received_at_ms = unix_ms()?;
            let payload =
                String::from_utf8(body.to_vec()).map_err(|_| BybitTransportError::Protocol)?;
            BybitRawPublicPayload::new(
                binding,
                BybitPublicSource::LinearInstrument,
                self.generation,
                received_at_ms,
                payload,
            )
            .map_err(|_| BybitTransportError::Protocol)
        };
        timeout(self.limits.operation_timeout, operation)
            .await
            .map_err(|_| BybitTransportError::Timeout)?
    }

    /// Fetches a one-level live book immediately before a market reduction.  This is public
    /// data only; it is used to prove the current minimum-notional check, never as a price.
    pub(crate) async fn fetch_linear_bbo(
        &self,
        binding: &BybitGatewayBinding,
    ) -> Result<BybitRawPublicPayload, BybitTransportError> {
        binding
            .validate_request_binding(&self.binding)
            .map_err(|_| BybitTransportError::Binding)?;
        let path = linear_bbo_path(binding).map_err(|_| BybitTransportError::Binding)?;
        let url = format!("{}{}", self.endpoint, path);
        let operation = async {
            let mut response = self.client.get(url).send().await.map_err(map_reqwest)?;
            if !response.status().is_success() {
                return Err(BybitTransportError::HttpStatus);
            }
            if response
                .content_length()
                .is_some_and(|length| length > self.limits.maximum_body_bytes as u64)
            {
                return Err(BybitTransportError::BodyTooLarge);
            }
            let mut body = BytesMut::new();
            while let Some(chunk) = response.chunk().await.map_err(map_reqwest)? {
                let new_length = body
                    .len()
                    .checked_add(chunk.len())
                    .ok_or(BybitTransportError::BodyTooLarge)?;
                if new_length > self.limits.maximum_body_bytes {
                    return Err(BybitTransportError::BodyTooLarge);
                }
                body.extend_from_slice(&chunk);
            }
            let received_at_ms = unix_ms()?;
            let payload =
                String::from_utf8(body.to_vec()).map_err(|_| BybitTransportError::Protocol)?;
            BybitRawPublicPayload::new(
                binding,
                BybitPublicSource::RestOrderBook,
                self.generation,
                received_at_ms,
                payload,
            )
            .map_err(|_| BybitTransportError::Protocol)
        };
        timeout(self.limits.operation_timeout, operation)
            .await
            .map_err(|_| BybitTransportError::Timeout)?
    }

    pub async fn execute_private_read(
        &self,
        binding: &BybitGatewayBinding,
        credentials: &BybitCredentials,
        request: &BybitPreparedPrivateRequest,
        timestamp_ms: u64,
    ) -> Result<BybitRawPrivatePayload, BybitTransportError> {
        binding
            .validate_request_binding(&self.binding)
            .map_err(|_| BybitTransportError::Binding)?;
        request
            .validate(binding)
            .map_err(|_| BybitTransportError::Binding)?;
        if request.generation != self.generation
            || request.origin != binding.config().rest_origin()
            || self.binding != request.binding
        {
            return Err(BybitTransportError::Binding);
        }
        if request.query.len() > self.limits.maximum_body_bytes {
            return Err(BybitTransportError::BodyTooLarge);
        }
        let signed = sign_private_request(credentials, binding, request, timestamp_ms)
            .map_err(|_| BybitTransportError::Signing)?;
        let url = if request.query.is_empty() {
            format!("{}{}", self.endpoint, request.path)
        } else {
            format!("{}{}?{}", self.endpoint, request.path, request.query)
        };
        let operation = async {
            let mut builder = self.client.get(url);
            for name in [
                "X-BAPI-API-KEY",
                "X-BAPI-SIGN",
                "X-BAPI-SIGN-TYPE",
                "X-BAPI-TIMESTAMP",
                "X-BAPI-RECV-WINDOW",
            ] {
                let value = signed.get(name).ok_or(BybitTransportError::Signing)?;
                builder = builder.header(name, value);
            }
            let mut response = builder.send().await.map_err(map_reqwest)?;
            if !response.status().is_success() {
                return Err(BybitTransportError::HttpStatus);
            }
            if response
                .content_length()
                .is_some_and(|length| length > self.limits.maximum_body_bytes as u64)
            {
                return Err(BybitTransportError::BodyTooLarge);
            }
            let mut body = BytesMut::new();
            while let Some(chunk) = response.chunk().await.map_err(map_reqwest)? {
                let new_length = body
                    .len()
                    .checked_add(chunk.len())
                    .ok_or(BybitTransportError::BodyTooLarge)?;
                if new_length > self.limits.maximum_body_bytes {
                    return Err(BybitTransportError::BodyTooLarge);
                }
                body.extend_from_slice(&chunk);
            }
            BybitRawPrivatePayload::from_response(
                binding,
                request,
                timestamp_ms,
                unix_ms()?,
                body.to_vec(),
            )
            .map_err(|_| BybitTransportError::Ack)
        };
        timeout(self.limits.operation_timeout, operation)
            .await
            .map_err(|_| BybitTransportError::Timeout)?
    }
}

fn map_reqwest(error: reqwest::Error) -> BybitTransportError {
    if error.is_timeout() {
        BybitTransportError::Timeout
    } else {
        BybitTransportError::Disconnected
    }
}

fn map_websocket(error: WebSocketError) -> BybitTransportError {
    if matches!(error, WebSocketError::Capacity(_)) {
        BybitTransportError::BodyTooLarge
    } else {
        BybitTransportError::Disconnected
    }
}

pub struct BybitPrivateWsTransport<S = MaybeTlsStream<TcpStream>> {
    stream: WebSocketStream<S>,
    binding: GatewayBinding,
    connection_generation: u64,
    private_generation: u64,
    recovery_generations_independently_bound: bool,
    endpoint: String,
    connection_id: String,
    authenticated_at_ms: u64,
    limits: BybitTransportLimits,
    pre_live_frames: VecDeque<BybitRawPrivateFrame>,
    buffered_bytes: usize,
    heartbeat_interval: Duration,
    next_heartbeat_at: Instant,
    heartbeat_sequence: u64,
}

impl<S> BybitPrivateWsTransport<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.private_generation
    }

    #[must_use]
    pub const fn connection_generation(&self) -> u64 {
        self.connection_generation
    }

    #[must_use]
    pub const fn private_generation(&self) -> u64 {
        self.private_generation
    }

    #[must_use]
    pub const fn recovery_generations_independently_bound(&self) -> bool {
        self.recovery_generations_independently_bound
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[must_use]
    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }

    #[must_use]
    pub const fn authenticated_at_ms(&self) -> u64 {
        self.authenticated_at_ms
    }

    pub(crate) async fn recovery_liveness_check(&mut self) -> Result<(), BybitTransportError> {
        self.exchange_application_heartbeat().await
    }

    pub fn capability_probe_evidence(
        &self,
        observed_at_ms: u64,
        expires_at_ms: u64,
    ) -> Result<BybitPrivateStreamProbeEvidence, BybitTransportError> {
        BybitPrivateStreamProbeEvidence::authenticated(
            self.binding.clone(),
            self.connection_generation,
            self.private_generation,
            self.authenticated_at_ms,
            observed_at_ms,
            expires_at_ms,
            &self.connection_id,
        )
        .map_err(|_| BybitTransportError::Binding)
    }

    pub async fn next_raw_frame(&mut self) -> Result<BybitRawPrivateFrame, BybitTransportError> {
        loop {
            if Instant::now() >= self.next_heartbeat_at {
                self.exchange_application_heartbeat().await?;
            }
            if let Some(frame) = self.pop_buffered_frame() {
                return Ok(frame);
            }
            let deadline = self.next_heartbeat_at;
            tokio::select! {
                () = sleep_until(deadline) => self.exchange_application_heartbeat().await?,
                message = self.stream.next() => {
                    let message = message
                        .ok_or(BybitTransportError::EndOfStream)?
                        .map_err(map_websocket)?;
                    match message {
                        Message::Text(value) => {
                            return make_raw_frame(
                                &self.binding,
                                self.private_generation,
                                Bytes::copy_from_slice(value.as_bytes()),
                                self.limits.maximum_body_bytes,
                                unix_ms()?,
                            );
                        }
                        Message::Binary(_) => return Err(BybitTransportError::Protocol),
                        Message::Ping(value) => send_message(
                            &mut self.stream,
                            Message::Pong(value),
                            self.limits.operation_timeout,
                        ).await?,
                        Message::Pong(_) => {}
                        Message::Close(_) => return Err(BybitTransportError::EndOfStream),
                        Message::Frame(_) => return Err(BybitTransportError::Protocol),
                    }
                }
            }
        }
    }

    async fn exchange_application_heartbeat(&mut self) -> Result<(), BybitTransportError> {
        self.heartbeat_sequence = self
            .heartbeat_sequence
            .checked_add(1)
            .ok_or(BybitTransportError::Heartbeat)?;
        let request_id = format!(
            "venueping{}-{}-{}",
            self.connection_generation, self.private_generation, self.heartbeat_sequence
        );
        let payload = serde_json::to_string(&PingFrame {
            request_id: &request_id,
            op: "ping",
        })
        .map_err(|_| BybitTransportError::Protocol)?;
        if payload.len() > self.limits.maximum_body_bytes {
            return Err(BybitTransportError::BodyTooLarge);
        }
        send_message(
            &mut self.stream,
            Message::Text(payload.into()),
            self.limits.operation_timeout,
        )
        .await?;

        let operation = async {
            loop {
                let message = self
                    .stream
                    .next()
                    .await
                    .ok_or(BybitTransportError::EndOfStream)?
                    .map_err(map_websocket)?;
                match message {
                    Message::Text(value) => {
                        let payload = Bytes::copy_from_slice(value.as_bytes());
                        if payload.is_empty() || payload.len() > self.limits.maximum_body_bytes {
                            return Err(BybitTransportError::BodyTooLarge);
                        }
                        let value: Value = serde_json::from_slice(&payload)
                            .map_err(|_| BybitTransportError::Heartbeat)?;
                        if value.get("topic").is_some() {
                            let frame = make_raw_frame(
                                &self.binding,
                                self.private_generation,
                                payload,
                                self.limits.maximum_body_bytes,
                                unix_ms()?,
                            )?;
                            push_buffered_frame(
                                &mut self.pre_live_frames,
                                &mut self.buffered_bytes,
                                frame,
                            )?;
                            continue;
                        }
                        validate_pong(value, &request_id, &self.connection_id)?;
                        break;
                    }
                    Message::Binary(_) => return Err(BybitTransportError::Protocol),
                    Message::Ping(value) => {
                        send_message(
                            &mut self.stream,
                            Message::Pong(value),
                            self.limits.operation_timeout,
                        )
                        .await?
                    }
                    Message::Pong(_) => {}
                    Message::Close(_) => return Err(BybitTransportError::EndOfStream),
                    Message::Frame(_) => return Err(BybitTransportError::Protocol),
                }
            }
            Ok(())
        };
        timeout(self.limits.operation_timeout, operation)
            .await
            .map_err(|_| BybitTransportError::Timeout)??;
        self.next_heartbeat_at = Instant::now() + self.heartbeat_interval;
        Ok(())
    }

    fn pop_buffered_frame(&mut self) -> Option<BybitRawPrivateFrame> {
        let frame = self.pre_live_frames.pop_front()?;
        self.buffered_bytes = self.buffered_bytes.saturating_sub(frame.payload.len());
        Some(frame)
    }
}

fn push_buffered_frame(
    frames: &mut VecDeque<BybitRawPrivateFrame>,
    buffered_bytes: &mut usize,
    frame: BybitRawPrivateFrame,
) -> Result<(), BybitTransportError> {
    let total = buffered_bytes
        .checked_add(frame.payload.len())
        .ok_or(BybitTransportError::PreLiveBufferOverflow)?;
    if frames.len() >= MAX_PRE_LIVE_FRAMES || total > MAX_PRE_LIVE_BYTES {
        return Err(BybitTransportError::PreLiveBufferOverflow);
    }
    *buffered_bytes = total;
    frames.push_back(frame);
    Ok(())
}

fn validate_pong(
    value: Value,
    request_id: &str,
    connection_id: &str,
) -> Result<(), BybitTransportError> {
    let pong: PongFrame =
        serde_json::from_value(value).map_err(|_| BybitTransportError::Heartbeat)?;
    if pong.op != "pong"
        || pong.request_id != request_id
        || pong.connection_id != connection_id
        || pong.args.len() != 1
        || pong.args[0]
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0)
            .is_none()
    {
        return Err(BybitTransportError::Heartbeat);
    }
    Ok(())
}

fn make_raw_frame(
    binding: &GatewayBinding,
    generation: u64,
    payload: Bytes,
    maximum_body_bytes: usize,
    received_at_ms: u64,
) -> Result<BybitRawPrivateFrame, BybitTransportError> {
    if payload.is_empty() || payload.len() > maximum_body_bytes {
        return Err(BybitTransportError::BodyTooLarge);
    }
    let value: Value =
        serde_json::from_slice(&payload).map_err(|_| BybitTransportError::Protocol)?;
    let topic = value
        .get("topic")
        .and_then(Value::as_str)
        .ok_or(BybitTransportError::Protocol)?;
    if !PRIVATE_TOPICS.contains(&topic) {
        return Err(BybitTransportError::Protocol);
    }
    Ok(BybitRawPrivateFrame {
        binding: binding.clone(),
        generation,
        received_at_ms,
        payload,
    })
}

pub async fn connect_private_ws(
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    generation: u64,
    now_ms: u64,
    limits: BybitTransportLimits,
) -> Result<BybitPrivateWsTransport, BybitTransportError> {
    connect_private_ws_inner(
        binding,
        credentials,
        generation,
        generation,
        false,
        now_ms,
        limits,
    )
    .await
}

pub async fn connect_private_ws_for_generations(
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    connection_generation: u64,
    private_generation: u64,
    now_ms: u64,
    limits: BybitTransportLimits,
) -> Result<BybitPrivateWsTransport, BybitTransportError> {
    connect_private_ws_inner(
        binding,
        credentials,
        connection_generation,
        private_generation,
        true,
        now_ms,
        limits,
    )
    .await
}

async fn connect_private_ws_inner(
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    connection_generation: u64,
    private_generation: u64,
    recovery_generations_independently_bound: bool,
    now_ms: u64,
    limits: BybitTransportLimits,
) -> Result<BybitPrivateWsTransport, BybitTransportError> {
    if connection_generation == 0 || private_generation == 0 || now_ms == 0 {
        return Err(BybitTransportError::Binding);
    }
    let endpoint = binding.config().private_ws();
    let (stream, _) = connect_websocket(endpoint, limits).await?;
    authenticate_private_stream(
        stream,
        endpoint.to_owned(),
        binding,
        credentials,
        connection_generation,
        private_generation,
        recovery_generations_independently_bound,
        now_ms,
        limits,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn authenticate_private_stream<S>(
    mut stream: WebSocketStream<S>,
    endpoint: String,
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    connection_generation: u64,
    private_generation: u64,
    recovery_generations_independently_bound: bool,
    now_ms: u64,
    limits: BybitTransportLimits,
) -> Result<BybitPrivateWsTransport<S>, BybitTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if connection_generation == 0 || private_generation == 0 || now_ms == 0 || endpoint.is_empty() {
        return Err(BybitTransportError::Binding);
    }
    let expires_at_ms = now_ms
        .checked_add(5_000)
        .ok_or(BybitTransportError::Clock)?;
    let signature =
        ws_auth_signature(credentials, expires_at_ms).map_err(|_| BybitTransportError::Signing)?;
    let auth_request_id = format!("venueauth{connection_generation}-{private_generation}");
    let auth = AuthFrame {
        request_id: &auth_request_id,
        op: "auth",
        args: AuthArgs(
            credentials.api_key.expose_secret(),
            expires_at_ms,
            signature.expose_secret(),
        ),
    };
    let auth_payload = SecretString::from(
        serde_json::to_string(&auth).map_err(|_| BybitTransportError::Protocol)?,
    );
    send_secret(&mut stream, &auth_payload, limits.operation_timeout).await?;
    let auth_ack = read_ack(&mut stream, limits).await?;
    validate_ack(&auth_ack, "auth", &auth_request_id, None)?;

    let subscribe_request_id = format!("venuesub{connection_generation}-{private_generation}");
    let subscribe = SubscribeFrame {
        request_id: &subscribe_request_id,
        op: "subscribe",
        args: PRIVATE_TOPICS,
    };
    let subscribe_payload =
        serde_json::to_string(&subscribe).map_err(|_| BybitTransportError::Protocol)?;
    send_message(
        &mut stream,
        Message::Text(subscribe_payload.into()),
        limits.operation_timeout,
    )
    .await?;
    let (subscribe_ack, pre_live_frames) = read_subscribe_ack(
        &mut stream,
        binding.gateway_binding(),
        private_generation,
        limits,
        &subscribe_request_id,
        auth_ack.connection_id.as_str(),
    )
    .await?;
    let buffered_bytes = pre_live_frames.iter().try_fold(0_usize, |total, frame| {
        total
            .checked_add(frame.payload.len())
            .ok_or(BybitTransportError::PreLiveBufferOverflow)
    })?;
    Ok(BybitPrivateWsTransport {
        stream,
        binding: binding.gateway_binding().clone(),
        connection_generation,
        private_generation,
        recovery_generations_independently_bound,
        endpoint,
        connection_id: subscribe_ack.connection_id,
        authenticated_at_ms: now_ms,
        limits,
        pre_live_frames,
        buffered_bytes,
        heartbeat_interval: PRIVATE_HEARTBEAT_INTERVAL,
        next_heartbeat_at: Instant::now() + PRIVATE_HEARTBEAT_INTERVAL,
        heartbeat_sequence: 0,
    })
}

async fn connect_websocket(
    endpoint: &str,
    limits: BybitTransportLimits,
) -> Result<
    (
        WebSocketStream<MaybeTlsStream<TcpStream>>,
        tokio_tungstenite::tungstenite::handshake::client::Response,
    ),
    BybitTransportError,
> {
    let websocket = WebSocketConfig::default()
        .max_message_size(Some(limits.maximum_body_bytes))
        .max_frame_size(Some(limits.maximum_body_bytes));
    timeout(
        limits.operation_timeout,
        connect_async_with_config(endpoint, Some(websocket), true),
    )
    .await
    .map_err(|_| BybitTransportError::Timeout)?
    .map_err(map_websocket)
}

async fn read_subscribe_ack<S>(
    stream: &mut WebSocketStream<S>,
    binding: &GatewayBinding,
    generation: u64,
    limits: BybitTransportLimits,
    request_id: &str,
    connection_id: &str,
) -> Result<(WsAck, VecDeque<BybitRawPrivateFrame>), BybitTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let operation = async {
        let mut frames = VecDeque::new();
        let mut buffered_bytes = 0_usize;
        loop {
            let message = stream
                .next()
                .await
                .ok_or(BybitTransportError::EndOfStream)?
                .map_err(map_websocket)?;
            let payload = match message {
                Message::Text(value) => Bytes::copy_from_slice(value.as_bytes()),
                Message::Binary(_) => return Err(BybitTransportError::Protocol),
                Message::Ping(value) => {
                    stream
                        .send(Message::Pong(value))
                        .await
                        .map_err(map_websocket)?;
                    continue;
                }
                Message::Pong(_) => continue,
                Message::Close(_) => return Err(BybitTransportError::EndOfStream),
                Message::Frame(_) => return Err(BybitTransportError::Protocol),
            };
            if payload.is_empty() || payload.len() > limits.maximum_body_bytes {
                return Err(BybitTransportError::BodyTooLarge);
            }
            let value: Value =
                serde_json::from_slice(&payload).map_err(|_| BybitTransportError::Ack)?;
            if value.get("topic").is_some() {
                let received_at_ms = unix_ms()?;
                let frame = make_raw_frame(
                    binding,
                    generation,
                    payload,
                    limits.maximum_body_bytes,
                    received_at_ms,
                )?;
                buffered_bytes = buffered_bytes
                    .checked_add(frame.payload.len())
                    .ok_or(BybitTransportError::PreLiveBufferOverflow)?;
                if frames.len() >= MAX_PRE_LIVE_FRAMES || buffered_bytes > MAX_PRE_LIVE_BYTES {
                    return Err(BybitTransportError::PreLiveBufferOverflow);
                }
                frames.push_back(frame);
                continue;
            }
            let ack: WsAck = serde_json::from_value(value).map_err(|_| BybitTransportError::Ack)?;
            validate_ack(&ack, "subscribe", request_id, Some(connection_id))?;
            return Ok((ack, frames));
        }
    };
    timeout(limits.operation_timeout, operation)
        .await
        .map_err(|_| BybitTransportError::Timeout)?
}

async fn send_secret<S>(
    stream: &mut WebSocketStream<S>,
    payload: &SecretString,
    operation_timeout: Duration,
) -> Result<(), BybitTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    send_message(
        stream,
        Message::Text(payload.expose_secret().to_owned().into()),
        operation_timeout,
    )
    .await
}

async fn send_message<S>(
    stream: &mut WebSocketStream<S>,
    message: Message,
    operation_timeout: Duration,
) -> Result<(), BybitTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    timeout(operation_timeout, stream.send(message))
        .await
        .map_err(|_| BybitTransportError::Timeout)?
        .map_err(map_websocket)
}

async fn read_ack<S>(
    stream: &mut WebSocketStream<S>,
    limits: BybitTransportLimits,
) -> Result<WsAck, BybitTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let operation = async {
        loop {
            let message = stream
                .next()
                .await
                .ok_or(BybitTransportError::EndOfStream)?
                .map_err(map_websocket)?;
            let payload = match message {
                Message::Text(value) => Bytes::copy_from_slice(value.as_bytes()),
                Message::Binary(_) => return Err(BybitTransportError::Protocol),
                Message::Ping(value) => {
                    stream
                        .send(Message::Pong(value))
                        .await
                        .map_err(map_websocket)?;
                    continue;
                }
                Message::Pong(_) => continue,
                Message::Close(_) => return Err(BybitTransportError::EndOfStream),
                Message::Frame(_) => return Err(BybitTransportError::Protocol),
            };
            if payload.is_empty() || payload.len() > limits.maximum_body_bytes {
                return Err(BybitTransportError::BodyTooLarge);
            }
            return serde_json::from_slice(&payload).map_err(|_| BybitTransportError::Ack);
        }
    };
    timeout(limits.operation_timeout, operation)
        .await
        .map_err(|_| BybitTransportError::Timeout)?
}

fn validate_ack(
    ack: &WsAck,
    operation: &str,
    request_id: &str,
    expected_connection_id: Option<&str>,
) -> Result<(), BybitTransportError> {
    if !ack.success
        || !ack.return_message.is_empty()
        || ack.operation != operation
        || ack
            .request_id
            .as_deref()
            .is_some_and(|actual| actual != request_id)
        || ack.connection_id.is_empty()
        || expected_connection_id.is_some_and(|expected| expected != ack.connection_id)
    {
        return Err(BybitTransportError::Ack);
    }
    Ok(())
}

#[derive(Serialize)]
struct AuthFrame<'a> {
    #[serde(rename = "req_id")]
    request_id: &'a str,
    op: &'static str,
    args: AuthArgs<'a>,
}

#[derive(Serialize)]
struct AuthArgs<'a>(&'a str, u64, &'a str);

#[derive(Serialize)]
struct SubscribeFrame<'a> {
    #[serde(rename = "req_id")]
    request_id: &'a str,
    op: &'static str,
    args: [&'static str; 4],
}

#[derive(Serialize)]
struct PingFrame<'a> {
    #[serde(rename = "req_id")]
    request_id: &'a str,
    op: &'static str,
}

#[derive(Deserialize)]
struct PongFrame {
    #[serde(rename = "req_id")]
    request_id: String,
    op: String,
    args: Vec<String>,
    #[serde(rename = "conn_id")]
    connection_id: String,
}

#[derive(Deserialize)]
struct WsAck {
    success: bool,
    #[serde(rename = "ret_msg")]
    return_message: String,
    #[serde(rename = "op")]
    operation: String,
    #[serde(rename = "req_id")]
    request_id: Option<String>,
    #[serde(rename = "conn_id")]
    connection_id: String,
}

#[derive(Clone, Eq, PartialEq)]
pub struct BybitRawPrivateFrame {
    pub binding: GatewayBinding,
    pub generation: u64,
    pub received_at_ms: u64,
    pub payload: Bytes,
}

impl fmt::Debug for BybitRawPrivateFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BybitRawPrivateFrame")
            .field("binding", &self.binding)
            .field("generation", &self.generation)
            .field("received_at_ms", &self.received_at_ms)
            .field("payload", &"[REDACTED PRIVATE FRAME]")
            .finish()
    }
}

pub(crate) fn unix_ms() -> Result<u64, BybitTransportError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BybitTransportError::Clock)?
        .as_millis();
    u64::try_from(millis).map_err(|_| BybitTransportError::Clock)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BybitTransportError {
    #[error("Bybit transport limits are invalid")]
    Limits,
    #[error("Bybit transport binding or generation does not match")]
    Binding,
    #[error("Bybit transport operation timed out")]
    Timeout,
    #[error("Bybit HTTP request failed")]
    Http,
    #[error("Bybit HTTP returned a non-success status")]
    HttpStatus,
    #[error("Bybit transport body exceeded the configured bound")]
    BodyTooLarge,
    #[error("Bybit private pre-live frame buffer exceeded its hard bound")]
    PreLiveBufferOverflow,
    #[error("Bybit transport connection ended unexpectedly")]
    Disconnected,
    #[error("Bybit WebSocket reached explicit EOF")]
    EndOfStream,
    #[error("Bybit transport protocol frame is invalid")]
    Protocol,
    #[error("Bybit transport acknowledgement is invalid or rejected")]
    Ack,
    #[error("Bybit rejected the physical mutation request")]
    Rejected,
    #[error("Bybit private websocket heartbeat is invalid or missing")]
    Heartbeat,
    #[error("Bybit transport signing failed")]
    Signing,
    #[error("Bybit transport clock is invalid")]
    Clock,
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;
    use crate::{
        BybitAccountIdentity, BybitLinearInstrumentRules, BybitOrderKind, BybitPlaceIntent,
        BybitPrivateSource, BybitPublicSource, BybitRawPrivatePayload, BybitRawPublicPayload,
        BybitTimeInForce, parse_account_identity, parse_linear_instrument, prepare_private_request,
    };
    use rust_decimal::Decimal;
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_async, client_async};
    use venue_domain::domain::{OrderSide, PositionSide, Price};
    use venue_gateway_api::{GatewayMode, VenueId};

    const ACCOUNT_ID: &str = "00000000-0000-4000-8000-000000000001";
    const ACCOUNT: &[u8] = include_bytes!("../fixtures/account-info-uta2.json");
    const INSTRUMENT: &str = include_str!("../fixtures/instruments-linear.json");
    const POSITIONS: &str = include_str!("../fixtures/positions-linear.json");
    const PLACE_ACK: &str = include_str!("../fixtures/place-order-ack.json");

    struct Facts {
        binding: BybitGatewayBinding,
        credentials: BybitCredentials,
        identity: BybitAccountIdentity,
        rules: BybitLinearInstrumentRules,
    }

    type TestError = Box<dyn std::error::Error + Send + Sync>;

    fn facts(mode: GatewayMode) -> Result<Facts, TestError> {
        let binding = BybitGatewayBinding::new(GatewayBinding::new(
            VenueId::Bybit,
            mode,
            ACCOUNT_ID,
            "BTC/USDT".parse()?,
        )?)?;
        let account_request = prepare_private_request(
            &binding,
            7,
            11,
            0,
            BybitPrivateSource::AccountInfo,
            None,
            None,
            None,
        )?;
        let account = BybitRawPrivatePayload::from_response(
            &binding,
            &account_request,
            1_670_000_000_000,
            1_716_863_719_400,
            ACCOUNT.to_vec(),
        )?;
        let identity = parse_account_identity(&binding, &account)?;
        let instrument = BybitRawPublicPayload::new(
            &binding,
            BybitPublicSource::LinearInstrument,
            7,
            1_716_863_719_400,
            INSTRUMENT.to_owned(),
        )?;
        let rules = parse_linear_instrument(&binding, instrument)?;
        Ok(Facts {
            binding,
            credentials: BybitCredentials::from_values("test", "secret")?,
            identity,
            rules,
        })
    }

    fn request(facts: &Facts) -> Result<BybitPreparedRequest, TestError> {
        Ok(crate::prepare_place_request(
            &facts.binding,
            &facts.identity,
            &facts.rules,
            &BybitPlaceIntent {
                client_order_id: "MANAGED_CLIENT_ID".to_owned(),
                side: OrderSide::Buy,
                position_side: PositionSide::Long,
                kind: BybitOrderKind::Limit,
                quantity: Decimal::new(1, 3),
                limit_price: Some(Price::new(Decimal::from(60_000))?),
                time_in_force: BybitTimeInForce::GoodTillCancelled,
                reduce_only: false,
            },
            1_716_863_719_500,
            None,
        )?)
    }

    async fn http_mock(
        response: Option<Vec<u8>>,
        delay: Duration,
    ) -> Result<(String, tokio::task::JoinHandle<io::Result<Vec<u8>>>), io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut request = Vec::new();
            let mut buffer = [0_u8; 4096];
            loop {
                stream.readable().await?;
                match stream.try_read(&mut buffer) {
                    Ok(0) => break,
                    Ok(read) => {
                        request.extend_from_slice(&buffer[..read]);
                        if complete_http_request(&request) {
                            break;
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(error) => return Err(error),
                }
            }
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            if let Some(response) = response {
                let mut written = 0;
                while written < response.len() {
                    stream.writable().await?;
                    match stream.try_write(&response[written..]) {
                        Ok(0) => {
                            return Err(io::Error::new(io::ErrorKind::WriteZero, "mock write"));
                        }
                        Ok(count) => written += count,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                        Err(error) => return Err(error),
                    }
                }
            }
            Ok(request)
        });
        Ok((format!("http://{address}"), task))
    }

    fn complete_http_request(request: &[u8]) -> bool {
        let Some(headers_end) = request.windows(4).position(|window| window == b"\r\n\r\n") else {
            return false;
        };
        let headers_end = headers_end + 4;
        let headers = String::from_utf8_lossy(&request[..headers_end]);
        if headers.starts_with("GET ") {
            return true;
        }
        let content_length = headers.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        });
        content_length.is_some_and(|length| request.len() >= headers_end + length)
    }

    fn http_response(status: &str, body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    #[test]
    fn transport_body_limit_accepts_exactly_two_mib_and_rejects_the_next_byte()
    -> Result<(), TestError> {
        let limits = BybitTransportLimits::new(Duration::from_secs(2), MAX_TRANSPORT_BODY_BYTES)?;
        assert_eq!(limits.maximum_body_bytes(), MAX_TRANSPORT_BODY_BYTES);
        assert_eq!(
            BybitTransportLimits::new(Duration::from_secs(2), MAX_TRANSPORT_BODY_BYTES + 1,),
            Err(BybitTransportError::Limits)
        );

        let facts = facts(GatewayMode::Live)?;
        let mut exact = br#"{"topic":"order.linear","data":[]}"#.to_vec();
        exact.resize(MAX_TRANSPORT_BODY_BYTES, b' ');
        assert!(
            make_raw_frame(
                facts.binding.gateway_binding(),
                7,
                Bytes::from(exact.clone()),
                MAX_TRANSPORT_BODY_BYTES,
                1,
            )
            .is_ok()
        );
        exact.push(b' ');
        assert_eq!(
            make_raw_frame(
                facts.binding.gateway_binding(),
                7,
                Bytes::from(exact),
                MAX_TRANSPORT_BODY_BYTES,
                1,
            ),
            Err(BybitTransportError::BodyTooLarge)
        );
        Ok(())
    }

    #[tokio::test]
    async fn http_sends_only_bound_prepared_request_and_parses_ack() -> Result<(), TestError> {
        let facts = facts(GatewayMode::Live)?;
        let request = request(&facts)?;
        let (endpoint, server) =
            http_mock(Some(http_response("200 OK", PLACE_ACK)), Duration::ZERO).await?;
        let limits = BybitTransportLimits::new(Duration::from_secs(2), 16 * 1024)?;
        let transport = BybitHttpTransport::with_endpoint(&facts.binding, 7, endpoint, limits)?;
        let ack = transport
            .execute_order(
                &facts.binding,
                &facts.credentials,
                &request,
                1_670_000_000_000,
            )
            .await?;
        assert_eq!(ack.order_id.as_deref(), Some("20"));
        let sent = server.await??;
        let sent = String::from_utf8(sent)?;
        assert!(sent.starts_with("POST /v5/order/create HTTP/1.1"));
        assert!(sent.contains("x-bapi-sign:"));
        assert!(sent.ends_with(std::str::from_utf8(&request.body)?));
        Ok(())
    }

    #[tokio::test]
    async fn http_get_signs_the_exact_query_and_returns_request_bound_raw_evidence()
    -> Result<(), TestError> {
        let facts = facts(GatewayMode::Live)?;
        let request = prepare_private_request(
            &facts.binding,
            7,
            11,
            0,
            BybitPrivateSource::Positions,
            None,
            None,
            None,
        )?;
        let (endpoint, server) =
            http_mock(Some(http_response("200 OK", POSITIONS)), Duration::ZERO).await?;
        let limits = BybitTransportLimits::new(Duration::from_secs(2), 16 * 1024)?;
        let transport = BybitHttpTransport::with_endpoint(&facts.binding, 7, endpoint, limits)?;
        let raw = transport
            .execute_private_read(
                &facts.binding,
                &facts.credentials,
                &request,
                1_670_000_000_000,
            )
            .await?;
        assert_eq!(raw.source, BybitPrivateSource::Positions);
        assert_eq!(raw.request_query, request.query);
        assert_eq!(raw.request_timestamp_ms, 1_670_000_000_000);
        assert!(!raw.payload_sha256.is_empty());
        let sent = String::from_utf8(server.await??)?;
        assert!(sent.starts_with(
            "GET /v5/position/list?category=linear&symbol=BTCUSDT&limit=200 HTTP/1.1"
        ));
        assert!(sent.contains("x-bapi-sign:"));
        Ok(())
    }

    #[tokio::test]
    async fn http_rejects_cross_account_timeout_and_disconnect() -> Result<(), TestError> {
        let live = facts(GatewayMode::Live)?;
        let mut wrong_request = request(&live)?;
        wrong_request.binding.trading_account_id =
            "00000000-0000-4000-8000-000000000002".to_owned();
        let limits = BybitTransportLimits::new(Duration::from_millis(40), 16 * 1024)?;
        let transport = BybitHttpTransport::new(&live.binding, 7, limits)?;
        assert_eq!(
            transport
                .execute_order(
                    &live.binding,
                    &live.credentials,
                    &wrong_request,
                    1_670_000_000_000
                )
                .await,
            Err(BybitTransportError::Binding)
        );

        let request = request(&live)?;
        let mut oversized_request = request.clone();
        oversized_request.body = vec![b'x'; limits.maximum_body_bytes + 1];
        assert_eq!(
            transport
                .execute_order(
                    &live.binding,
                    &live.credentials,
                    &oversized_request,
                    1_670_000_000_000
                )
                .await,
            Err(BybitTransportError::BodyTooLarge)
        );
        let (endpoint, delayed) = http_mock(
            Some(http_response("200 OK", PLACE_ACK)),
            Duration::from_millis(200),
        )
        .await?;
        let transport = BybitHttpTransport::with_endpoint(&live.binding, 7, endpoint, limits)?;
        assert_eq!(
            transport
                .execute_order(
                    &live.binding,
                    &live.credentials,
                    &request,
                    1_670_000_000_000
                )
                .await,
            Err(BybitTransportError::Timeout)
        );
        delayed.abort();

        let (endpoint, disconnected) = http_mock(None, Duration::ZERO).await?;
        let transport = BybitHttpTransport::with_endpoint(&live.binding, 7, endpoint, limits)?;
        assert_eq!(
            transport
                .execute_order(
                    &live.binding,
                    &live.credentials,
                    &request,
                    1_670_000_000_000
                )
                .await,
            Err(BybitTransportError::Disconnected)
        );
        let _ = disconnected.await?;
        Ok(())
    }

    #[tokio::test]
    async fn disconnected_mutation_is_not_automatically_replayed() -> Result<(), TestError> {
        let facts = facts(GatewayMode::Live)?;
        let request = request(&facts)?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut request = Vec::new();
            let mut buffer = [0_u8; 4_096];
            loop {
                stream.readable().await?;
                match stream.try_read(&mut buffer) {
                    Ok(0) => break,
                    Ok(read) => {
                        request.extend_from_slice(&buffer[..read]);
                        if complete_http_request(&request) {
                            break;
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(error) => return Err(error),
                }
            }
            drop(stream);
            Ok::<bool, io::Error>(
                tokio::time::timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_ok(),
            )
        });
        let limits = BybitTransportLimits::new(Duration::from_secs(1), 16 * 1_024)?;
        let transport = BybitHttpTransport::with_endpoint(&facts.binding, 7, endpoint, limits)?;
        assert_eq!(
            transport
                .execute_order(
                    &facts.binding,
                    &facts.credentials,
                    &request,
                    1_670_000_000_000,
                )
                .await,
            Err(BybitTransportError::Disconnected)
        );
        assert!(!server.await??);
        Ok(())
    }

    #[tokio::test]
    async fn http_rejects_unbounded_timeout_and_does_not_follow_redirects() -> Result<(), TestError>
    {
        assert_eq!(
            BybitTransportLimits::new(Duration::from_secs(61), 16 * 1024),
            Err(BybitTransportError::Limits)
        );

        let facts = facts(GatewayMode::Live)?;
        let request = request(&facts)?;
        let redirect_target = TcpListener::bind("127.0.0.1:0").await?;
        let location = format!("http://{}", redirect_target.local_addr()?);
        let response = format!(
            "HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        )
        .into_bytes();
        let (endpoint, redirect_source) = http_mock(Some(response), Duration::ZERO).await?;
        let limits = BybitTransportLimits::new(Duration::from_secs(2), 16 * 1024)?;
        let transport = BybitHttpTransport::with_endpoint(&facts.binding, 7, endpoint, limits)?;

        assert_eq!(
            transport
                .execute_order(
                    &facts.binding,
                    &facts.credentials,
                    &request,
                    1_670_000_000_000
                )
                .await,
            Err(BybitTransportError::HttpStatus)
        );
        let source_request = String::from_utf8(redirect_source.await??)?;
        assert!(source_request.contains("x-bapi-api-key:"));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), redirect_target.accept())
                .await
                .is_err()
        );
        Ok(())
    }

    async fn ws_listener() -> Result<(TcpListener, String), io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        Ok((listener, endpoint))
    }

    fn ack(operation: &str, request_id: &str, success: bool) -> String {
        serde_json::json!({
            "success": success,
            "ret_msg": if success { "" } else { "rejected" },
            "op": operation,
            "req_id": request_id,
            "conn_id": "connection-1"
        })
        .to_string()
    }

    fn ack_without_request_id(operation: &str) -> String {
        serde_json::json!({
            "success": true,
            "ret_msg": "",
            "op": operation,
            "conn_id": "connection-1"
        })
        .to_string()
    }

    #[tokio::test]
    async fn websocket_waits_for_auth_and_subscription_ack_before_raw_delivery()
    -> Result<(), TestError> {
        let facts = facts(GatewayMode::Live)?;
        let (listener, endpoint) = ws_listener().await?;
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await?;
            let mut ws = accept_async(tcp).await?;
            let auth = ws.next().await.ok_or("missing auth")??.into_text()?;
            let auth: Value = serde_json::from_str(auth.as_str())?;
            if auth["op"] != "auth"
                || auth["args"][1] != 1_005_000_u64
                || auth["args"][2]
                    != "52a330b528ba5d1f30b016fd9e60e4416cbad46e63b1a83ae28e480b11e2b358"
            {
                return Err::<(), Box<dyn std::error::Error + Send + Sync>>("bad auth".into());
            }
            ws.send(Message::Text(ack_without_request_id("auth").into()))
                .await?;
            let subscribe = ws.next().await.ok_or("missing subscribe")??.into_text()?;
            let subscribe: Value = serde_json::from_str(subscribe.as_str())?;
            if subscribe["args"].as_array().map(Vec::len) != Some(4) {
                return Err("bad topics".into());
            }
            ws.send(Message::Text(
                r#"{"topic":"order.linear","creationTime":1000,"data":[{"symbol":"BTCUSDT"}]}"#
                    .into(),
            ))
            .await?;
            ws.send(Message::Text(ack_without_request_id("subscribe").into()))
                .await?;
            ws.send(Message::Close(None)).await?;
            Ok(())
        });
        let tcp = TcpStream::connect(endpoint.trim_start_matches("ws://")).await?;
        let (stream, _) = client_async(&endpoint, tcp).await?;
        let limits = BybitTransportLimits::new(Duration::from_secs(2), 16 * 1024)?;
        let mut transport = authenticate_private_stream(
            stream,
            endpoint,
            &facts.binding,
            &facts.credentials,
            7,
            9,
            true,
            1_000_000,
            limits,
        )
        .await?;
        assert_eq!(transport.connection_generation(), 7);
        assert_eq!(transport.private_generation(), 9);
        assert!(transport.recovery_generations_independently_bound());
        let cached_received_at_ms = transport
            .pre_live_frames
            .front()
            .map(|frame| frame.received_at_ms)
            .ok_or("missing pre-live frame")?;
        let frame = transport.next_raw_frame().await?;
        assert_eq!(frame.binding, *facts.binding.gateway_binding());
        assert_eq!(frame.generation, 9);
        assert_eq!(frame.received_at_ms, cached_received_at_ms);
        assert_eq!(
            transport.next_raw_frame().await,
            Err(BybitTransportError::EndOfStream)
        );
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn websocket_text_heartbeat_requires_exact_pong_before_delivering_data()
    -> Result<(), TestError> {
        let facts = facts(GatewayMode::Live)?;
        let (listener, endpoint) = ws_listener().await?;
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await?;
            let mut ws = accept_async(tcp).await?;
            let _auth = ws.next().await.ok_or("missing auth")??;
            ws.send(Message::Text(ack_without_request_id("auth").into()))
                .await?;
            let _subscribe = ws.next().await.ok_or("missing subscribe")??;
            ws.send(Message::Text(ack_without_request_id("subscribe").into()))
                .await?;
            let ping = ws
                .next()
                .await
                .ok_or("missing application ping")??
                .into_text()?;
            let ping: Value = serde_json::from_str(ping.as_str())?;
            if ping["op"] != "ping" {
                return Err::<(), Box<dyn std::error::Error + Send + Sync>>(
                    "unexpected heartbeat".into(),
                );
            }
            let request_id = ping["req_id"].as_str().ok_or("missing ping id")?;
            ws.send(Message::Text(
                r#"{"topic":"execution.linear","creationTime":1000,"data":[{"symbol":"BTCUSDT"}]}"#
                    .into(),
            ))
            .await?;
            ws.send(Message::Text(
                serde_json::json!({
                    "req_id": request_id,
                    "op": "pong",
                    "args": ["1675418560633"],
                    "conn_id": "connection-1"
                })
                .to_string()
                .into(),
            ))
            .await?;
            Ok(())
        });
        let tcp = TcpStream::connect(endpoint.trim_start_matches("ws://")).await?;
        let (stream, _) = client_async(&endpoint, tcp).await?;
        let limits = BybitTransportLimits::new(Duration::from_secs(2), 16 * 1024)?;
        let mut transport = authenticate_private_stream(
            stream,
            endpoint,
            &facts.binding,
            &facts.credentials,
            7,
            7,
            true,
            1_000_000,
            limits,
        )
        .await?;
        transport.heartbeat_interval = Duration::from_millis(5);
        transport.next_heartbeat_at = Instant::now() + transport.heartbeat_interval;
        let frame = transport.next_raw_frame().await?;
        assert!(frame.payload.starts_with(br#"{"topic":"execution.linear""#));
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn websocket_config_rejects_oversized_frame_before_protocol_parsing()
    -> Result<(), TestError> {
        let facts = facts(GatewayMode::Live)?;
        let (listener, endpoint) = ws_listener().await?;
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await?;
            let mut ws = accept_async(tcp).await?;
            let _auth = ws.next().await.ok_or("missing auth")??;
            ws.send(Message::Text("x".repeat(1_024).into())).await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        let limits = BybitTransportLimits::new(Duration::from_secs(2), 256)?;
        let (stream, _) = connect_websocket(&endpoint, limits).await?;
        assert_eq!(
            authenticate_private_stream(
                stream,
                endpoint,
                &facts.binding,
                &facts.credentials,
                7,
                7,
                true,
                1_000_000,
                limits
            )
            .await
            .err(),
            Some(BybitTransportError::BodyTooLarge)
        );
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn websocket_pre_live_buffer_fails_closed_at_the_hard_frame_limit()
    -> Result<(), TestError> {
        let facts = facts(GatewayMode::Live)?;
        let (listener, endpoint) = ws_listener().await?;
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await?;
            let mut ws = accept_async(tcp).await?;
            let auth = ws.next().await.ok_or("missing auth")??.into_text()?;
            let auth: Value = serde_json::from_str(auth.as_str())?;
            let auth_id = auth["req_id"].as_str().ok_or("missing auth id")?;
            ws.send(Message::Text(ack("auth", auth_id, true).into()))
                .await?;
            let _subscribe = ws.next().await.ok_or("missing subscribe")??;
            for sequence in 0..=MAX_PRE_LIVE_FRAMES {
                let frame = serde_json::json!({
                    "topic": "execution.linear",
                    "creationTime": sequence,
                    "data": [{"symbol": "BTCUSDT", "execId": sequence.to_string()}]
                });
                ws.send(Message::Text(frame.to_string().into())).await?;
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        let tcp = TcpStream::connect(endpoint.trim_start_matches("ws://")).await?;
        let (stream, _) = client_async(&endpoint, tcp).await?;
        let limits = BybitTransportLimits::new(Duration::from_secs(2), 16 * 1024)?;
        assert_eq!(
            authenticate_private_stream(
                stream,
                endpoint,
                &facts.binding,
                &facts.credentials,
                7,
                7,
                true,
                1_000_000,
                limits
            )
            .await
            .err(),
            Some(BybitTransportError::PreLiveBufferOverflow)
        );
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn websocket_pre_live_buffer_fails_closed_at_the_hard_byte_limit() -> Result<(), TestError>
    {
        let facts = facts(GatewayMode::Live)?;
        let (listener, endpoint) = ws_listener().await?;
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await?;
            let mut ws = accept_async(tcp).await?;
            let auth = ws.next().await.ok_or("missing auth")??.into_text()?;
            let auth: Value = serde_json::from_str(auth.as_str())?;
            let auth_id = auth["req_id"].as_str().ok_or("missing auth id")?;
            ws.send(Message::Text(ack("auth", auth_id, true).into()))
                .await?;
            let _subscribe = ws.next().await.ok_or("missing subscribe")??;
            let padding = "x".repeat(600_000);
            for sequence in 0..2 {
                let frame = serde_json::json!({
                    "topic": "execution.linear",
                    "creationTime": sequence,
                    "data": [{
                        "symbol": "BTCUSDT",
                        "execId": sequence.to_string(),
                        "padding": padding.as_str()
                    }]
                });
                ws.send(Message::Text(frame.to_string().into())).await?;
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        let tcp = TcpStream::connect(endpoint.trim_start_matches("ws://")).await?;
        let (stream, _) = client_async(&endpoint, tcp).await?;
        let limits = BybitTransportLimits::new(Duration::from_secs(5), 700_000)?;
        assert_eq!(
            authenticate_private_stream(
                stream,
                endpoint,
                &facts.binding,
                &facts.credentials,
                7,
                7,
                true,
                1_000_000,
                limits
            )
            .await
            .err(),
            Some(BybitTransportError::PreLiveBufferOverflow)
        );
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn websocket_auth_ack_failure_is_closed_before_subscription() -> Result<(), TestError> {
        let facts = facts(GatewayMode::Live)?;
        let (listener, endpoint) = ws_listener().await?;
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await?;
            let mut ws = accept_async(tcp).await?;
            let auth = ws.next().await.ok_or("missing auth")??.into_text()?;
            let auth: Value = serde_json::from_str(auth.as_str())?;
            let request_id = auth["req_id"].as_str().ok_or("missing auth id")?;
            ws.send(Message::Text(ack("auth", request_id, false).into()))
                .await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        let tcp = TcpStream::connect(endpoint.trim_start_matches("ws://")).await?;
        let (stream, _) = client_async(&endpoint, tcp).await?;
        let limits = BybitTransportLimits::new(Duration::from_secs(2), 16 * 1024)?;
        assert_eq!(
            authenticate_private_stream(
                stream,
                endpoint,
                &facts.binding,
                &facts.credentials,
                7,
                7,
                true,
                1_000_000,
                limits
            )
            .await
            .err(),
            Some(BybitTransportError::Ack)
        );
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn websocket_rejects_binary_ack_and_binary_private_frame() -> Result<(), TestError> {
        let facts = facts(GatewayMode::Live)?;
        let limits = BybitTransportLimits::new(Duration::from_secs(2), 16 * 1024)?;

        let (listener, endpoint) = ws_listener().await?;
        let binary_ack_server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await?;
            let mut ws = accept_async(tcp).await?;
            let auth = ws.next().await.ok_or("missing auth")??.into_text()?;
            let auth: Value = serde_json::from_str(auth.as_str())?;
            let request_id = auth["req_id"].as_str().ok_or("missing auth id")?;
            ws.send(Message::Binary(Bytes::from(ack("auth", request_id, true))))
                .await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        let tcp = TcpStream::connect(endpoint.trim_start_matches("ws://")).await?;
        let (stream, _) = client_async(&endpoint, tcp).await?;
        assert_eq!(
            authenticate_private_stream(
                stream,
                endpoint,
                &facts.binding,
                &facts.credentials,
                7,
                7,
                true,
                1_000_000,
                limits
            )
            .await
            .err(),
            Some(BybitTransportError::Protocol)
        );
        binary_ack_server.await??;

        let (listener, endpoint) = ws_listener().await?;
        let binary_private_server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await?;
            let mut ws = accept_async(tcp).await?;
            let _auth = ws.next().await.ok_or("missing auth")??;
            ws.send(Message::Text(ack_without_request_id("auth").into()))
                .await?;
            let _subscribe = ws.next().await.ok_or("missing subscribe")??;
            ws.send(Message::Text(ack_without_request_id("subscribe").into()))
                .await?;
            ws.send(Message::Binary(Bytes::from_static(
                br#"{"topic":"order.linear","creationTime":1000,"data":[{"symbol":"BTCUSDT"}]}"#,
            )))
            .await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        let tcp = TcpStream::connect(endpoint.trim_start_matches("ws://")).await?;
        let (stream, _) = client_async(&endpoint, tcp).await?;
        let mut transport = authenticate_private_stream(
            stream,
            endpoint,
            &facts.binding,
            &facts.credentials,
            7,
            7,
            true,
            1_000_000,
            limits,
        )
        .await?;
        assert_eq!(
            transport.next_raw_frame().await,
            Err(BybitTransportError::Protocol)
        );
        binary_private_server.await??;
        Ok(())
    }
}
