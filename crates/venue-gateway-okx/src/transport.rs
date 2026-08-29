use std::{
    collections::VecDeque,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, Method, header::HeaderValue, redirect::Policy};
use secrecy::ExposeSecret;
use serde::Deserialize;
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use venue_gateway_api::GatewayBinding;

use crate::{
    OkxAcceptedCancel, OkxAcceptedOrder, OkxAccountProfile, OkxActivePrivateSubscription,
    OkxCancelRequest, OkxConfig, OkxCredentials, OkxInstrument, OkxPlaceRequest,
    OkxPrivateReadRequest, OkxPrivateRequest, OkxPrivateSubscription, OkxPrivateWsScope,
    OkxTradeMode, OkxUnknownCancelReadbackRequest, OkxUnknownOrderReadbackRequest, OkxWsLoginFrame,
    SignedHeaders, activate_private_subscription, build_private_subscribe,
    build_unknown_cancel_readback_request, build_unknown_order_readback_request_after,
    build_ws_login, parse_cancel_ack, parse_place_ack, parse_ws_login_ack,
};

const HEADER_NAMES: [&str; 5] = [
    "OK-ACCESS-KEY",
    "OK-ACCESS-SIGN",
    "OK-ACCESS-TIMESTAMP",
    "OK-ACCESS-PASSPHRASE",
    "Content-Type",
];
const MAX_OPERATION_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_TRANSPORT_BYTES: usize = 2 * 1024 * 1024;
const MAX_PENDING_PUSH_FRAMES: usize = 32;
const HEARTBEAT_IDLE: Duration = Duration::from_secs(25);

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OkxTransportError {
    #[error("OKX transport configuration is invalid")]
    Configuration,
    #[error("OKX transport binding does not match the request")]
    Binding,
    #[error("OKX transport operation timed out")]
    Timeout,
    #[error("OKX HTTP transport failed")]
    Http,
    #[error("OKX HTTP response status is not successful: {0}")]
    HttpStatus(u16),
    #[error("OKX response exceeded its configured byte limit")]
    BodyTooLarge,
    #[error("OKX WebSocket transport reached EOF")]
    Eof,
    #[error("OKX WebSocket transport failed")]
    WebSocket,
    #[error("OKX WebSocket received an unexpected frame")]
    UnexpectedFrame,
    #[error("OKX private protocol handshake or frame was rejected")]
    Protocol,
    #[error("system clock is unavailable")]
    Clock,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxHttpResponse {
    pub binding: GatewayBinding,
    pub instrument_generation: u64,
    pub received_at_ms: u64,
    pub body: Bytes,
}

pub struct OkxHttpTransport {
    client: Client,
    config: OkxConfig,
    origin: String,
    operation_timeout: Duration,
    max_body_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OkxPlaceOnceOutcome {
    Acknowledged(Box<OkxAcceptedOrder>),
    Unknown {
        readback: Box<OkxUnknownOrderReadbackRequest>,
        transport_error: Option<OkxTransportError>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OkxCancelOnceOutcome {
    Acknowledged(Box<OkxAcceptedCancel>),
    Unknown {
        readback: Box<OkxUnknownCancelReadbackRequest>,
        transport_error: Option<OkxTransportError>,
    },
}

impl OkxHttpTransport {
    pub fn new(
        config: OkxConfig,
        operation_timeout: Duration,
        max_body_bytes: usize,
    ) -> Result<Self, OkxTransportError> {
        Self::build(
            config.clone(),
            config.rest_origin(),
            operation_timeout,
            max_body_bytes,
            false,
        )
    }

    #[cfg(test)]
    pub(crate) fn with_origin(
        config: OkxConfig,
        origin: &str,
        operation_timeout: Duration,
        max_body_bytes: usize,
    ) -> Result<Self, OkxTransportError> {
        Self::build(config, origin, operation_timeout, max_body_bytes, true)
    }

    fn build(
        config: OkxConfig,
        origin: &str,
        operation_timeout: Duration,
        max_body_bytes: usize,
        disable_proxy: bool,
    ) -> Result<Self, OkxTransportError> {
        if !valid_limits(operation_timeout, max_body_bytes)
            || !origin.starts_with("http")
            || origin.ends_with('/')
        {
            return Err(OkxTransportError::Configuration);
        }
        let mut builder = Client::builder()
            .connect_timeout(operation_timeout)
            .timeout(operation_timeout)
            .redirect(Policy::none());
        if disable_proxy {
            builder = builder.no_proxy();
        }
        let client = builder
            .build()
            .map_err(|_| OkxTransportError::Configuration)?;
        Ok(Self {
            client,
            config,
            origin: origin.to_owned(),
            operation_timeout,
            max_body_bytes,
        })
    }

    pub async fn execute<R: OkxPrivateRequest + ?Sized>(
        &self,
        credentials: &OkxCredentials,
        request: &R,
        timestamp: &str,
    ) -> Result<OkxHttpResponse, OkxTransportError> {
        // One call performs one physical dispatch. A timeout or disconnect is UNKNOWN to this
        // adapter and must be resolved by a separately signed readback request.
        if request.scope().gateway_binding() != self.config.gateway_binding() {
            return Err(OkxTransportError::Binding);
        }
        let signed = request
            .signed_headers(credentials, &self.config, timestamp)
            .map_err(|_| OkxTransportError::Binding)?;
        self.execute_bound(
            request.scope().gateway_binding(),
            request.scope().instrument_generation(),
            request.method(),
            request.request_path(),
            request.body(),
            &signed,
        )
        .await
    }

    /// Executes a request-bound authenticated read. This does not collect pages or grant any
    /// capability; callers must wrap the response in `OkxRawPrivatePage` and close the attempt.
    pub async fn execute_read(
        &self,
        credentials: &OkxCredentials,
        request: &OkxPrivateReadRequest,
        timestamp: &str,
    ) -> Result<OkxHttpResponse, OkxTransportError> {
        if request.scope().gateway_binding() != self.config.gateway_binding() {
            return Err(OkxTransportError::Binding);
        }
        let signed = request
            .signed_headers(credentials, &self.config, timestamp)
            .map_err(|_| OkxTransportError::Binding)?;
        self.execute_bound(
            request.scope().gateway_binding(),
            request.scope().instrument_generation(),
            request.method(),
            request.request_path(),
            &[],
            &signed,
        )
        .await
    }

    /// Consumes one place request and performs at most one mutation call. Any untrustworthy or
    /// missing ACK returns only an exact GET readback handle; the submitted request is not returned.
    pub async fn place_once(
        &self,
        credentials: &OkxCredentials,
        instrument: &OkxInstrument,
        profile: &OkxAccountProfile,
        request: OkxPlaceRequest,
        timestamp: &str,
    ) -> Result<OkxPlaceOnceOutcome, OkxTransportError> {
        request
            .signed_headers(credentials, &self.config, timestamp)
            .map_err(|_| OkxTransportError::Binding)?;
        let dispatched_at_ms = received_at_ms()?;
        let readback = build_unknown_order_readback_request_after(
            &self.config,
            instrument,
            profile,
            &request,
            dispatched_at_ms,
        )
        .map_err(|_| OkxTransportError::Binding)?;
        match self.execute(credentials, &request, timestamp).await {
            Ok(response) => match parse_place_ack(response, &request) {
                Ok(accepted) => Ok(OkxPlaceOnceOutcome::Acknowledged(Box::new(accepted))),
                Err(_) => Ok(OkxPlaceOnceOutcome::Unknown {
                    readback: Box::new(readback),
                    transport_error: None,
                }),
            },
            Err(error) => Ok(OkxPlaceOnceOutcome::Unknown {
                readback: Box::new(readback),
                transport_error: Some(error),
            }),
        }
    }

    /// The reduce path accepts only the canonical exposure-reduction request and consumes it once.
    pub async fn reduce_once(
        &self,
        credentials: &OkxCredentials,
        instrument: &OkxInstrument,
        profile: &OkxAccountProfile,
        request: OkxPlaceRequest,
        timestamp: &str,
    ) -> Result<OkxPlaceOnceOutcome, OkxTransportError> {
        if !request.is_reduce_once() {
            return Err(OkxTransportError::Binding);
        }
        self.place_once(credentials, instrument, profile, request, timestamp)
            .await
    }

    /// Consumes one cancel request. UNKNOWN never yields a second cancel surface, only order detail.
    pub async fn cancel_once(
        &self,
        credentials: &OkxCredentials,
        instrument: &OkxInstrument,
        profile: &OkxAccountProfile,
        accepted_order: &OkxAcceptedOrder,
        request: OkxCancelRequest,
        timestamp: &str,
    ) -> Result<OkxCancelOnceOutcome, OkxTransportError> {
        request
            .signed_headers(credentials, &self.config, timestamp)
            .map_err(|_| OkxTransportError::Binding)?;
        let dispatched_at_ms = received_at_ms()?;
        let readback = build_unknown_cancel_readback_request(
            &self.config,
            instrument,
            profile,
            &request,
            accepted_order,
            dispatched_at_ms,
        )
        .map_err(|_| OkxTransportError::Binding)?;
        match self.execute(credentials, &request, timestamp).await {
            Ok(response) => match parse_cancel_ack(response, &request) {
                Ok(accepted) => Ok(OkxCancelOnceOutcome::Acknowledged(Box::new(accepted))),
                Err(_) => Ok(OkxCancelOnceOutcome::Unknown {
                    readback: Box::new(readback),
                    transport_error: None,
                }),
            },
            Err(error) => Ok(OkxCancelOnceOutcome::Unknown {
                readback: Box::new(readback),
                transport_error: Some(error),
            }),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_bound(
        &self,
        binding: &GatewayBinding,
        instrument_generation: u64,
        method: &str,
        request_path: &str,
        body: &[u8],
        signed: &SignedHeaders,
    ) -> Result<OkxHttpResponse, OkxTransportError> {
        if binding != self.config.gateway_binding() {
            return Err(OkxTransportError::Binding);
        }
        if body.len() > self.max_body_bytes {
            return Err(OkxTransportError::BodyTooLarge);
        }
        let method = match method {
            "GET" => Method::GET,
            "POST" => Method::POST,
            _ => return Err(OkxTransportError::Configuration),
        };
        let mut builder = self
            .client
            .request(method, format!("{}{}", self.origin, request_path));
        for name in HEADER_NAMES {
            let value = signed.get(name).ok_or(OkxTransportError::Configuration)?;
            let value =
                HeaderValue::from_str(value).map_err(|_| OkxTransportError::Configuration)?;
            builder = builder.header(name, value);
        }
        if let Some(value) = signed.get("x-simulated-trading") {
            builder = builder.header("x-simulated-trading", value);
        }
        if !body.is_empty() {
            builder = builder.body(body.to_vec());
        }
        let response = timeout(self.operation_timeout, builder.send())
            .await
            .map_err(|_| OkxTransportError::Timeout)?
            .map_err(|error| {
                if error.is_timeout() {
                    OkxTransportError::Timeout
                } else {
                    OkxTransportError::Http
                }
            })?;
        if !response.status().is_success() {
            return Err(OkxTransportError::HttpStatus(response.status().as_u16()));
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.max_body_bytes as u64)
        {
            return Err(OkxTransportError::BodyTooLarge);
        }
        let body = read_bounded_body(response, self.operation_timeout, self.max_body_bytes).await?;
        Ok(OkxHttpResponse {
            binding: self.config.gateway_binding().clone(),
            instrument_generation,
            received_at_ms: received_at_ms()?,
            body,
        })
    }
}

async fn read_bounded_body(
    mut response: reqwest::Response,
    operation_timeout: Duration,
    max_body_bytes: usize,
) -> Result<Bytes, OkxTransportError> {
    timeout(operation_timeout, async {
        let mut body = BytesMut::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| OkxTransportError::Http)?
        {
            let next_len = body
                .len()
                .checked_add(chunk.len())
                .ok_or(OkxTransportError::BodyTooLarge)?;
            if next_len > max_body_bytes {
                return Err(OkxTransportError::BodyTooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body.freeze())
    })
    .await
    .map_err(|_| OkxTransportError::Timeout)?
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxReceivedPrivateFrame {
    pub scope: OkxPrivateWsScope,
    pub account_profile: OkxAccountProfile,
    pub binding: GatewayBinding,
    pub instrument_generation: u64,
    pub private_generation: u64,
    pub connection_id: String,
    pub subscription_request_id: String,
    pub received_at_ms: u64,
    pub payload: Bytes,
}

type PrivateSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct OkxPrivateWsTransport {
    socket: PrivateSocket,
    active: OkxActivePrivateSubscription,
    pending_frames: VecDeque<BufferedPrivateFrame>,
    operation_timeout: Duration,
    max_frame_bytes: usize,
    heartbeat_idle: Duration,
}

struct BufferedPrivateFrame {
    payload: Bytes,
    received_at_ms: u64,
}

impl OkxPrivateWsTransport {
    #[allow(clippy::too_many_arguments)]
    pub async fn connect(
        config: &OkxConfig,
        instrument: &OkxInstrument,
        profile: &OkxAccountProfile,
        trade_mode: OkxTradeMode,
        private_generation: u64,
        credentials: &OkxCredentials,
        timestamp_seconds: &str,
        request_id: &str,
        operation_timeout: Duration,
        max_frame_bytes: usize,
    ) -> Result<Self, OkxTransportError> {
        Self::connect_to(
            config.private_ws(),
            config,
            instrument,
            profile,
            trade_mode,
            private_generation,
            credentials,
            timestamp_seconds,
            request_id,
            operation_timeout,
            max_frame_bytes,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn connect_to(
        dial_endpoint: &str,
        config: &OkxConfig,
        instrument: &OkxInstrument,
        profile: &OkxAccountProfile,
        trade_mode: OkxTradeMode,
        private_generation: u64,
        credentials: &OkxCredentials,
        timestamp_seconds: &str,
        request_id: &str,
        operation_timeout: Duration,
        max_frame_bytes: usize,
    ) -> Result<Self, OkxTransportError> {
        if !valid_limits(operation_timeout, max_frame_bytes) {
            return Err(OkxTransportError::Configuration);
        }
        let login = build_ws_login(
            config,
            instrument,
            profile,
            trade_mode,
            private_generation,
            credentials,
            timestamp_seconds,
        )
        .map_err(|_| OkxTransportError::Binding)?;
        if login.endpoint() != config.private_ws()
            || login.scope().gateway_binding() != config.gateway_binding()
            || login.scope().instrument_generation() != instrument.instrument().generation
        {
            return Err(OkxTransportError::Binding);
        }
        let websocket_config = WebSocketConfig::default()
            .max_message_size(Some(max_frame_bytes))
            .max_frame_size(Some(max_frame_bytes));
        let (mut socket, _) = timeout(
            operation_timeout,
            connect_async_with_config(dial_endpoint, Some(websocket_config), false),
        )
        .await
        .map_err(|_| OkxTransportError::Timeout)?
        .map_err(|_| OkxTransportError::WebSocket)?;
        send_login(&mut socket, &login, operation_timeout).await?;
        let login_ack = receive_text(&mut socket, operation_timeout, max_frame_bytes).await?;
        let session =
            parse_ws_login_ack(&login_ack, &login).map_err(|_| OkxTransportError::Protocol)?;
        let subscription =
            build_private_subscribe(&session, config, instrument, profile, request_id)
                .map_err(|_| OkxTransportError::Binding)?;
        send_subscription(&mut socket, &subscription, operation_timeout).await?;
        let (acknowledgements, pending_frames) = timeout(
            operation_timeout,
            collect_subscription_frames(
                &mut socket,
                operation_timeout,
                max_frame_bytes,
                subscription.scope(),
            ),
        )
        .await
        .map_err(|_| OkxTransportError::Timeout)??;
        let acknowledgement_refs = acknowledgements
            .iter()
            .map(Bytes::as_ref)
            .collect::<Vec<_>>();
        let active = activate_private_subscription(
            &acknowledgement_refs,
            &subscription,
            &session,
            config,
            instrument,
            profile,
        )
        .map_err(|_| OkxTransportError::Protocol)?;
        Ok(Self {
            socket,
            active,
            pending_frames,
            operation_timeout,
            max_frame_bytes,
            heartbeat_idle: HEARTBEAT_IDLE,
        })
    }

    #[must_use]
    pub const fn active_subscription(&self) -> &OkxActivePrivateSubscription {
        &self.active
    }

    pub async fn next_frame(&mut self) -> Result<OkxReceivedPrivateFrame, OkxTransportError> {
        loop {
            let buffered = if let Some(buffered) = self.pending_frames.pop_front() {
                buffered
            } else {
                match timeout(
                    self.heartbeat_idle,
                    receive_text(
                        &mut self.socket,
                        self.operation_timeout,
                        self.max_frame_bytes,
                    ),
                )
                .await
                {
                    Ok(result) => BufferedPrivateFrame {
                        payload: result?,
                        received_at_ms: received_at_ms()?,
                    },
                    Err(_) => {
                        self.heartbeat().await?;
                        continue;
                    }
                }
            };
            if buffered.payload.as_ref() == b"pong" {
                continue;
            }
            validate_private_push(&buffered.payload, self.active.scope())?;
            return Ok(OkxReceivedPrivateFrame {
                scope: self.active.scope().clone(),
                account_profile: self.active.account_profile().clone(),
                binding: self.active.scope().gateway_binding().clone(),
                instrument_generation: self.active.scope().instrument_generation(),
                private_generation: self.active.scope().private_generation(),
                connection_id: self.active.connection_id().to_owned(),
                subscription_request_id: self.active.request_id().to_owned(),
                received_at_ms: buffered.received_at_ms,
                payload: buffered.payload,
            });
        }
    }

    async fn heartbeat(&mut self) -> Result<(), OkxTransportError> {
        send_message(
            &mut self.socket,
            Message::Text("ping".into()),
            self.operation_timeout,
        )
        .await?;
        timeout(self.operation_timeout, async {
            loop {
                let payload = receive_text(
                    &mut self.socket,
                    self.operation_timeout,
                    self.max_frame_bytes,
                )
                .await?;
                let received_at_ms = received_at_ms()?;
                if payload.as_ref() == b"pong" {
                    return Ok(());
                }
                validate_private_push(&payload, self.active.scope())?;
                buffer_private_frame(
                    &mut self.pending_frames,
                    BufferedPrivateFrame {
                        payload,
                        received_at_ms,
                    },
                    self.max_frame_bytes,
                )?;
            }
        })
        .await
        .map_err(|_| OkxTransportError::Timeout)?
    }
}

async fn send_message(
    socket: &mut PrivateSocket,
    message: Message,
    operation_timeout: Duration,
) -> Result<(), OkxTransportError> {
    timeout(operation_timeout, socket.send(message))
        .await
        .map_err(|_| OkxTransportError::Timeout)?
        .map_err(|_| OkxTransportError::WebSocket)
}

async fn send_login(
    socket: &mut PrivateSocket,
    login: &OkxWsLoginFrame,
    operation_timeout: Duration,
) -> Result<(), OkxTransportError> {
    let frame = Message::Text(login.secret_payload().expose_secret().to_owned().into());
    send_message(socket, frame, operation_timeout).await
}

async fn send_subscription(
    socket: &mut PrivateSocket,
    subscription: &OkxPrivateSubscription,
    operation_timeout: Duration,
) -> Result<(), OkxTransportError> {
    let text =
        std::str::from_utf8(subscription.payload()).map_err(|_| OkxTransportError::Protocol)?;
    send_message(
        socket,
        Message::Text(text.to_owned().into()),
        operation_timeout,
    )
    .await
}

async fn receive_text(
    socket: &mut PrivateSocket,
    operation_timeout: Duration,
    max_frame_bytes: usize,
) -> Result<Bytes, OkxTransportError> {
    timeout(operation_timeout, async {
        loop {
            let next = socket
                .next()
                .await
                .ok_or(OkxTransportError::Eof)?
                .map_err(|_| OkxTransportError::WebSocket)?;
            match next {
                Message::Text(text) => {
                    if text.len() > max_frame_bytes {
                        return Err(OkxTransportError::BodyTooLarge);
                    }
                    return Ok(Bytes::copy_from_slice(text.as_bytes()));
                }
                Message::Ping(payload) => socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|_| OkxTransportError::WebSocket)?,
                Message::Pong(_) => {}
                Message::Close(_) => return Err(OkxTransportError::Eof),
                Message::Binary(_) | Message::Frame(_) => {
                    return Err(OkxTransportError::UnexpectedFrame);
                }
            }
        }
    })
    .await
    .map_err(|_| OkxTransportError::Timeout)?
}

async fn collect_subscription_frames(
    socket: &mut PrivateSocket,
    operation_timeout: Duration,
    max_frame_bytes: usize,
    scope: &OkxPrivateWsScope,
) -> Result<(Vec<Bytes>, VecDeque<BufferedPrivateFrame>), OkxTransportError> {
    let mut acknowledgements = Vec::with_capacity(3);
    let mut pending_frames = VecDeque::new();
    while acknowledgements.len() < 3 {
        let payload = receive_text(socket, operation_timeout, max_frame_bytes).await?;
        let value: Value =
            serde_json::from_slice(&payload).map_err(|_| OkxTransportError::Protocol)?;
        if value.get("event").and_then(Value::as_str) == Some("subscribe") {
            acknowledgements.push(payload);
            continue;
        }
        validate_private_push(&payload, scope)?;
        let buffered = BufferedPrivateFrame {
            payload,
            received_at_ms: received_at_ms()?,
        };
        buffer_private_frame(&mut pending_frames, buffered, max_frame_bytes)?;
    }
    Ok((acknowledgements, pending_frames))
}

fn buffer_private_frame(
    frames: &mut VecDeque<BufferedPrivateFrame>,
    frame: BufferedPrivateFrame,
    max_frame_bytes: usize,
) -> Result<(), OkxTransportError> {
    let buffered_bytes = frames.iter().try_fold(0_usize, |total, candidate| {
        total.checked_add(candidate.payload.len())
    });
    let next_bytes = buffered_bytes
        .and_then(|total| total.checked_add(frame.payload.len()))
        .ok_or(OkxTransportError::BodyTooLarge)?;
    if frames.len() >= MAX_PENDING_PUSH_FRAMES || next_bytes > max_frame_bytes {
        return Err(OkxTransportError::BodyTooLarge);
    }
    frames.push_back(frame);
    Ok(())
}

#[derive(Deserialize)]
struct TransportPush {
    arg: TransportPushArg,
    data: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransportPushArg {
    channel: String,
    uid: String,
    #[serde(default)]
    inst_type: String,
    #[serde(default)]
    inst_id: String,
    #[serde(default)]
    ccy: String,
}

fn validate_private_push(
    payload: &[u8],
    scope: &OkxPrivateWsScope,
) -> Result<(), OkxTransportError> {
    let push: TransportPush =
        serde_json::from_slice(payload).map_err(|_| OkxTransportError::Protocol)?;
    if !push.data.is_array() || push.arg.uid != scope.uid() {
        return Err(OkxTransportError::Binding);
    }
    let instrument_scoped = matches!(push.arg.channel.as_str(), "orders" | "positions");
    let valid_scope = if instrument_scoped {
        push.arg.inst_type == "SWAP"
            && push.arg.inst_id == scope.native_instrument_id()
            && push.arg.ccy.is_empty()
    } else {
        push.arg.channel == "account"
            && push.arg.inst_type.is_empty()
            && push.arg.inst_id.is_empty()
            && (push.arg.ccy.is_empty() || push.arg.ccy == scope.gateway_binding().symbol.quote())
    };
    if !valid_scope {
        return Err(OkxTransportError::Binding);
    }
    Ok(())
}

fn received_at_ms() -> Result<u64, OkxTransportError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| OkxTransportError::Clock)?
        .as_millis();
    u64::try_from(millis).map_err(|_| OkxTransportError::Clock)
}

fn valid_limits(operation_timeout: Duration, byte_limit: usize) -> bool {
    !operation_timeout.is_zero()
        && operation_timeout <= MAX_OPERATION_TIMEOUT
        && byte_limit > 0
        && byte_limit <= MAX_TRANSPORT_BYTES
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use futures_util::{SinkExt, StreamExt};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::Mutex,
    };
    use tokio_tungstenite::{accept_async, tungstenite::Message};
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

    use super::*;

    const INSTRUMENT: &[u8] = include_bytes!("../fixtures/linear-swap-instrument.json");
    const PROFILE: &[u8] = include_bytes!("../fixtures/account-config.json");

    struct OversizedRequest {
        scope: crate::OkxExecutionScope,
        body: Vec<u8>,
    }

    impl OkxPrivateRequest for OversizedRequest {
        fn scope(&self) -> &crate::OkxExecutionScope {
            &self.scope
        }

        fn method(&self) -> &'static str {
            "POST"
        }

        fn request_path(&self) -> &str {
            "/api/v5/trade/order"
        }

        fn body(&self) -> &[u8] {
            &self.body
        }
    }

    fn scope(
        mode: GatewayMode,
        generation: u64,
    ) -> Result<(OkxConfig, OkxInstrument, OkxAccountProfile), Box<dyn std::error::Error>> {
        let config = OkxConfig::for_binding(GatewayBinding::new(
            VenueId::Okx,
            mode,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)?;
        let instrument = crate::parse_instrument(INSTRUMENT, &config, generation)?;
        let profile = crate::parse_account_profile(PROFILE, crate::OkxPositionMode::LongShort)?;
        Ok((config, instrument, profile))
    }

    async fn http_server(
        response: &'static [u8],
        delay: Duration,
    ) -> Result<(String, Arc<Mutex<Vec<u8>>>), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let captured = Arc::new(Mutex::new(Vec::new()));
        let captured_server = Arc::clone(&captured);
        tokio::spawn(async move {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let mut request = vec![0_u8; 8192];
            let Ok(read) = stream.read(&mut request).await else {
                return;
            };
            request.truncate(read);
            *captured_server.lock().await = request;
            tokio::time::sleep(delay).await;
            let _ = stream.write_all(response).await;
        });
        Ok((format!("http://{address}"), captured))
    }

    #[tokio::test]
    async fn http_is_signed_bounded_and_binding_pinned() -> Result<(), Box<dyn std::error::Error>> {
        use venue_domain::domain::{
            CommandId, OrderCommand, OrderOwner, OrderPurpose, OrderSide, PositionSide, Price,
        };

        let (config, instrument, profile) = scope(GatewayMode::Test, 7)?;
        let command = OrderCommand {
            command_id: CommandId::new("place3")?,
            client_order_id: CommandId::new("00000000000000000000000000000003")?,
            owner: OrderOwner {
                strategy_instance_id: "grid1".to_owned(),
                run_id: "run1".to_owned(),
                exchange: "okx".to_owned(),
                account: config.gateway_binding().trading_account_id.to_string(),
                symbol: config.gateway_binding().symbol.clone(),
                purpose: OrderPurpose::Reduce,
            },
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            quantity: rust_decimal::Decimal::new(2, 1),
            limit_price: Price::new(rust_decimal::Decimal::new(60_000, 0))?,
            reduce_only: true,
        };
        let request = crate::build_place_request(
            &config,
            &instrument,
            &profile,
            crate::OkxTradeMode::Cross,
            crate::OkxPlaceIntent::Limit(&command),
        )?;
        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}";
        let (origin, captured) = http_server(response, Duration::ZERO).await?;
        let transport =
            OkxHttpTransport::with_origin(config.clone(), &origin, Duration::from_secs(1), 256)?;
        let received = transport
            .execute(
                &OkxCredentials::from_values("key", "secret", "pass")?,
                &request,
                "2026-08-29T01:02:03.000Z",
            )
            .await?;
        assert_eq!(received.body, Bytes::from_static(b"{}"));
        assert_eq!(received.instrument_generation, 7);
        let wire = String::from_utf8(captured.lock().await.clone())?;
        assert!(wire.contains("x-simulated-trading: 1"));
        assert!(wire.contains("ok-access-sign:"));

        let (live, _, _) = scope(GatewayMode::Live, 7)?;
        let wrong = OkxHttpTransport::with_origin(live, &origin, Duration::from_secs(1), 256)?;
        assert_eq!(
            wrong
                .execute(
                    &OkxCredentials::from_values("key", "secret", "pass")?,
                    &request,
                    "2026-08-29T01:02:03.000Z"
                )
                .await,
            Err(OkxTransportError::Binding)
        );

        let oversized = OversizedRequest {
            scope: request.scope().clone(),
            body: vec![b'x'; 257],
        };
        assert_eq!(
            transport
                .execute(
                    &OkxCredentials::from_values("key", "secret", "pass")?,
                    &oversized,
                    "2026-08-29T01:02:03.000Z"
                )
                .await,
            Err(OkxTransportError::BodyTooLarge)
        );
        Ok(())
    }

    #[tokio::test]
    async fn http_timeout_disconnect_status_and_size_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        use venue_domain::domain::{
            CommandId, OrderCommand, OrderOwner, OrderPurpose, OrderSide, PositionSide, Price,
        };
        let (config, instrument, profile) = scope(GatewayMode::Live, 7)?;
        let command = OrderCommand {
            command_id: CommandId::new("place3")?,
            client_order_id: CommandId::new("00000000000000000000000000000003")?,
            owner: OrderOwner {
                strategy_instance_id: "grid1".to_owned(),
                run_id: "run1".to_owned(),
                exchange: "okx".to_owned(),
                account: config.gateway_binding().trading_account_id.to_string(),
                symbol: config.gateway_binding().symbol.clone(),
                purpose: OrderPurpose::Reduce,
            },
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            quantity: rust_decimal::Decimal::new(2, 1),
            limit_price: Price::new(rust_decimal::Decimal::new(60_000, 0))?,
            reduce_only: true,
        };
        let request = crate::build_place_request(
            &config,
            &instrument,
            &profile,
            crate::OkxTradeMode::Cross,
            crate::OkxPlaceIntent::Limit(&command),
        )?;
        let credentials = OkxCredentials::from_values("key", "secret", "pass")?;
        let cases: [(&[u8], Duration, OkxTransportError); 4] = [
            (
                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}",
                Duration::from_millis(100),
                OkxTransportError::Timeout,
            ),
            (b"", Duration::ZERO, OkxTransportError::Http),
            (
                b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\n\r\n",
                Duration::ZERO,
                OkxTransportError::HttpStatus(503),
            ),
            (
                b"HTTP/1.1 200 OK\r\nContent-Length: 999\r\n\r\n",
                Duration::ZERO,
                OkxTransportError::BodyTooLarge,
            ),
        ];
        for (wire, delay, expected) in cases {
            let (origin, _) = http_server(wire, delay).await?;
            let operation_timeout = if delay.is_zero() {
                Duration::from_secs(1)
            } else {
                Duration::from_millis(20)
            };
            let transport =
                OkxHttpTransport::with_origin(config.clone(), &origin, operation_timeout, 256)?;
            assert_eq!(
                transport
                    .execute(&credentials, &request, "2026-08-29T01:02:03.000Z")
                    .await,
                Err(expected)
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn mutation_timeout_is_dispatched_once_without_transport_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        use venue_domain::domain::{
            CommandId, OrderCommand, OrderOwner, OrderPurpose, OrderSide, PositionSide, Price,
        };
        let (config, instrument, profile) = scope(GatewayMode::Test, 7)?;
        let command = OrderCommand {
            command_id: CommandId::new("place-once")?,
            client_order_id: CommandId::new("placeonce7")?,
            owner: OrderOwner {
                strategy_instance_id: "grid1".to_owned(),
                run_id: "run1".to_owned(),
                exchange: "okx".to_owned(),
                account: config.gateway_binding().trading_account_id.to_string(),
                symbol: config.gateway_binding().symbol.clone(),
                purpose: OrderPurpose::Reduce,
            },
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            quantity: rust_decimal::Decimal::new(2, 1),
            limit_price: Price::new(rust_decimal::Decimal::new(60_000, 0))?,
            reduce_only: true,
        };
        let request = crate::build_place_request(
            &config,
            &instrument,
            &profile,
            crate::OkxTradeMode::Cross,
            crate::OkxPlaceIntent::Limit(&command),
        )?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let accepts = Arc::new(AtomicUsize::new(0));
        let accepts_server = Arc::clone(&accepts);
        tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + Duration::from_millis(120);
            while let Ok(Ok((mut stream, _))) = timeout(
                deadline.saturating_duration_since(tokio::time::Instant::now()),
                listener.accept(),
            )
            .await
            {
                accepts_server.fetch_add(1, Ordering::SeqCst);
                let mut request_bytes = [0_u8; 2048];
                let _ = stream.read(&mut request_bytes).await;
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(120)).await;
                    drop(stream);
                });
            }
        });
        let transport =
            OkxHttpTransport::with_origin(config, &origin, Duration::from_millis(20), 256)?;
        let outcome = transport
            .place_once(
                &OkxCredentials::from_values("key", "secret", "pass")?,
                &instrument,
                &profile,
                request,
                "2026-08-29T01:02:03.000Z",
            )
            .await?;
        let OkxPlaceOnceOutcome::Unknown {
            readback,
            transport_error,
        } = outcome
        else {
            return Err("timeout must stay UNKNOWN".into());
        };
        assert_eq!(transport_error, Some(OkxTransportError::Timeout));
        assert_eq!(readback.method(), "GET");
        assert!(readback.body().is_empty());
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(accepts.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[test]
    fn transport_limits_reject_near_unbounded_waits_and_buffers()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, _, _) = scope(GatewayMode::Live, 7)?;
        assert!(matches!(
            OkxHttpTransport::new(config.clone(), Duration::from_secs(61), 16),
            Err(OkxTransportError::Configuration)
        ));
        assert!(matches!(
            OkxHttpTransport::new(config, Duration::from_secs(1), MAX_TRANSPORT_BYTES + 1),
            Err(OkxTransportError::Configuration)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn signed_http_redirect_is_not_followed() -> Result<(), Box<dyn std::error::Error>> {
        use venue_domain::domain::{
            CommandId, OrderCommand, OrderOwner, OrderPurpose, OrderSide, PositionSide, Price,
        };
        let (config, instrument, profile) = scope(GatewayMode::Live, 7)?;
        let command = OrderCommand {
            command_id: CommandId::new("place3")?,
            client_order_id: CommandId::new("00000000000000000000000000000003")?,
            owner: OrderOwner {
                strategy_instance_id: "grid1".to_owned(),
                run_id: "run1".to_owned(),
                exchange: "okx".to_owned(),
                account: config.gateway_binding().trading_account_id.to_string(),
                symbol: config.gateway_binding().symbol.clone(),
                purpose: OrderPurpose::Reduce,
            },
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            quantity: rust_decimal::Decimal::new(2, 1),
            limit_price: Price::new(rust_decimal::Decimal::new(60_000, 0))?,
            reduce_only: true,
        };
        let request = crate::build_place_request(
            &config,
            &instrument,
            &profile,
            crate::OkxTradeMode::Cross,
            crate::OkxPlaceIntent::Limit(&command),
        )?;
        let response =
            b"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/leak\r\nContent-Length: 0\r\n\r\n";
        let (origin, _) = http_server(response, Duration::ZERO).await?;
        let transport =
            OkxHttpTransport::with_origin(config, &origin, Duration::from_secs(1), 256)?;
        assert_eq!(
            transport
                .execute(
                    &OkxCredentials::from_values("key", "secret", "pass")?,
                    &request,
                    "2026-08-29T01:02:03.000Z"
                )
                .await,
            Err(OkxTransportError::HttpStatus(302))
        );
        Ok(())
    }

    async fn ws_server(
        acknowledgements: [&'static str; 4],
        early_frame: Option<Message>,
        final_frame: Option<Message>,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let Ok(mut socket) = accept_async(stream).await else {
                return;
            };
            if socket.next().await.is_none() {
                return;
            }
            if socket
                .send(Message::Ping(Bytes::from_static(b"probe")))
                .await
                .is_err()
            {
                return;
            }
            if socket
                .send(Message::Text(acknowledgements[0].into()))
                .await
                .is_err()
            {
                return;
            }
            loop {
                match socket.next().await {
                    Some(Ok(Message::Text(_))) => break,
                    Some(Ok(Message::Pong(_))) => {}
                    _ => return,
                }
            }
            for (index, acknowledgement) in acknowledgements[1..].iter().enumerate() {
                if socket
                    .send(Message::Text((*acknowledgement).into()))
                    .await
                    .is_err()
                {
                    return;
                }
                if index == 0
                    && let Some(frame) = early_frame.clone()
                    && socket.send(frame).await.is_err()
                {
                    return;
                }
            }
            if let Some(frame) = final_frame {
                let _ = socket.send(frame).await;
            }
        });
        Ok(format!("ws://{address}"))
    }

    fn acknowledgements() -> [&'static str; 4] {
        [
            r#"{"event":"login","code":"0","msg":"","connId":"connection1"}"#,
            r#"{"id":"request1","event":"subscribe","arg":{"channel":"orders","instType":"SWAP","instId":"BTC-USDT-SWAP"},"connId":"connection1"}"#,
            r#"{"id":"request1","event":"subscribe","arg":{"channel":"account","ccy":"USDT"},"connId":"connection1"}"#,
            r#"{"id":"request1","event":"subscribe","arg":{"channel":"positions","instType":"SWAP","instId":"BTC-USDT-SWAP"},"connId":"connection1"}"#,
        ]
    }

    async fn complete_server_handshake(
        socket: &mut WebSocketStream<TcpStream>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let _login = socket.next().await.ok_or("missing login")??;
        socket
            .send(Message::Text(acknowledgements()[0].into()))
            .await?;
        let _subscribe = socket.next().await.ok_or("missing subscribe")??;
        for acknowledgement in &acknowledgements()[1..] {
            socket
                .send(Message::Text((*acknowledgement).into()))
                .await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn ws_delivers_only_after_login_and_three_exact_acks()
    -> Result<(), Box<dyn std::error::Error>> {
        let push = Message::Text(
            r#"{"arg":{"channel":"orders","uid":"fixture-sub-account","instType":"SWAP","instId":"BTC-USDT-SWAP"},"data":[]}"#
                .into(),
        );
        let endpoint = ws_server(acknowledgements(), Some(push), None).await?;
        let (config, instrument, profile) = scope(GatewayMode::Test, 9)?;
        let mut transport = OkxPrivateWsTransport::connect_to(
            &endpoint,
            &config,
            &instrument,
            &profile,
            OkxTradeMode::Cross,
            17,
            &OkxCredentials::from_values("key", "secret", "pass")?,
            "1538054050",
            "request1",
            Duration::from_secs(1),
            4096,
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let dequeue_at_ms = received_at_ms()?;
        let frame = transport.next_frame().await?;
        let first_received_at_ms = frame.received_at_ms;
        assert_eq!(frame.instrument_generation, 9);
        assert_eq!(frame.private_generation, 17);
        assert!(first_received_at_ms > 0);
        assert_eq!(frame.binding, *config.gateway_binding());
        assert_eq!(frame.scope, *transport.active_subscription().scope());
        assert_eq!(frame.account_profile, profile);
        assert_eq!(frame.scope.uid(), "fixture-sub-account");
        assert_eq!(
            transport
                .active_subscription()
                .scope()
                .instrument_generation(),
            9
        );
        assert!(first_received_at_ms < dequeue_at_ms);
        Ok(())
    }

    #[tokio::test]
    async fn ws_ack_failure_disconnect_and_binary_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, instrument, profile) = scope(GatewayMode::Live, 9)?;
        let credentials = OkxCredentials::from_values("key", "secret", "pass")?;
        let mut wrong = acknowledgements();
        wrong[3] = r#"{"id":"request1","event":"subscribe","arg":{"channel":"positions","instType":"SWAP","instId":"ETH-USDT-SWAP"},"connId":"connection1"}"#;
        let endpoint = ws_server(wrong, None, None).await?;
        assert!(matches!(
            OkxPrivateWsTransport::connect_to(
                &endpoint,
                &config,
                &instrument,
                &profile,
                OkxTradeMode::Cross,
                17,
                &credentials,
                "1538054050",
                "request1",
                Duration::from_secs(1),
                4096,
            )
            .await,
            Err(OkxTransportError::Protocol)
        ));

        let endpoint = ws_server(acknowledgements(), None, None).await?;
        let mut disconnected = OkxPrivateWsTransport::connect_to(
            &endpoint,
            &config,
            &instrument,
            &profile,
            OkxTradeMode::Cross,
            17,
            &credentials,
            "1538054050",
            "request1",
            Duration::from_secs(1),
            4096,
        )
        .await?;
        assert!(matches!(
            disconnected.next_frame().await,
            Err(OkxTransportError::Eof | OkxTransportError::WebSocket)
        ));

        let endpoint = ws_server(
            acknowledgements(),
            None,
            Some(Message::Binary(vec![1, 2, 3].into())),
        )
        .await?;
        let mut binary = OkxPrivateWsTransport::connect_to(
            &endpoint,
            &config,
            &instrument,
            &profile,
            OkxTradeMode::Cross,
            17,
            &credentials,
            "1538054050",
            "request1",
            Duration::from_secs(1),
            4096,
        )
        .await?;
        assert_eq!(
            binary.next_frame().await,
            Err(OkxTransportError::UnexpectedFrame)
        );
        Ok(())
    }

    #[tokio::test]
    async fn ws_login_timeout_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let Ok(mut socket) = accept_async(stream).await else {
                return;
            };
            let _ = socket.next().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        let (config, instrument, profile) = scope(GatewayMode::Test, 9)?;
        assert!(matches!(
            OkxPrivateWsTransport::connect_to(
                &endpoint,
                &config,
                &instrument,
                &profile,
                OkxTradeMode::Cross,
                17,
                &OkxCredentials::from_values("key", "secret", "pass")?,
                "1538054050",
                "request1",
                Duration::from_millis(10),
                4096,
            )
            .await,
            Err(OkxTransportError::Timeout)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn ws_text_heartbeat_consumes_pong_and_delivers_bound_account_push()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            complete_server_handshake(&mut socket).await?;
            let ping = socket.next().await.ok_or("missing text ping")??;
            assert_eq!(ping, Message::Text("ping".into()));
            socket.send(Message::Text("pong".into())).await?;
            socket
                .send(Message::Text(
                    r#"{"arg":{"channel":"account","uid":"fixture-sub-account"},"data":[]}"#.into(),
                ))
                .await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        let (config, instrument, profile) = scope(GatewayMode::Test, 9)?;
        let mut transport = OkxPrivateWsTransport::connect_to(
            &endpoint,
            &config,
            &instrument,
            &profile,
            OkxTradeMode::Cross,
            17,
            &OkxCredentials::from_values("key", "secret", "pass")?,
            "1538054050",
            "request1",
            Duration::from_millis(100),
            4096,
        )
        .await?;
        transport.heartbeat_idle = Duration::from_millis(10);
        let frame = transport.next_frame().await?;
        let value: Value = serde_json::from_slice(&frame.payload)?;
        assert_eq!(value["arg"]["channel"], "account");
        assert_eq!(frame.scope, *transport.active_subscription().scope());
        assert!(server.await?.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn ws_heartbeat_without_text_pong_times_out() -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let Ok(mut socket) = accept_async(stream).await else {
                return;
            };
            if complete_server_handshake(&mut socket).await.is_err() {
                return;
            }
            let _ = socket.next().await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        });
        let (config, instrument, profile) = scope(GatewayMode::Test, 9)?;
        let mut transport = OkxPrivateWsTransport::connect_to(
            &endpoint,
            &config,
            &instrument,
            &profile,
            OkxTradeMode::Cross,
            17,
            &OkxCredentials::from_values("key", "secret", "pass")?,
            "1538054050",
            "request1",
            Duration::from_millis(40),
            4096,
        )
        .await?;
        transport.heartbeat_idle = Duration::from_millis(10);
        assert_eq!(
            transport.next_frame().await,
            Err(OkxTransportError::Timeout)
        );
        Ok(())
    }

    #[tokio::test]
    async fn ws_rejects_wrong_uid_and_native_instrument_before_delivery()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, instrument, profile) = scope(GatewayMode::Live, 9)?;
        let credentials = OkxCredentials::from_values("key", "secret", "pass")?;
        for payload in [
            r#"{"arg":{"channel":"orders","uid":"wrong","instType":"SWAP","instId":"BTC-USDT-SWAP"},"data":[]}"#,
            r#"{"arg":{"channel":"positions","uid":"fixture-sub-account","instType":"SWAP","instId":"ETH-USDT-SWAP"},"data":[]}"#,
        ] {
            let endpoint = ws_server(
                acknowledgements(),
                None,
                Some(Message::Text(payload.into())),
            )
            .await?;
            let mut transport = OkxPrivateWsTransport::connect_to(
                &endpoint,
                &config,
                &instrument,
                &profile,
                OkxTradeMode::Cross,
                17,
                &credentials,
                "1538054050",
                "request1",
                Duration::from_secs(1),
                4096,
            )
            .await?;
            assert_eq!(
                transport.next_frame().await,
                Err(OkxTransportError::Binding)
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn websocket_config_rejects_oversized_message_before_json_allocation()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let Ok(mut socket) = accept_async(stream).await else {
                return;
            };
            if socket.next().await.is_some() {
                let _ = socket.send(Message::Text("x".repeat(1024).into())).await;
            }
        });
        let (config, instrument, profile) = scope(GatewayMode::Test, 9)?;
        assert!(matches!(
            OkxPrivateWsTransport::connect_to(
                &endpoint,
                &config,
                &instrument,
                &profile,
                OkxTradeMode::Cross,
                17,
                &OkxCredentials::from_values("key", "secret", "pass")?,
                "1538054050",
                "request1",
                Duration::from_secs(1),
                256,
            )
            .await,
            Err(OkxTransportError::WebSocket)
        ));
        Ok(())
    }
}
