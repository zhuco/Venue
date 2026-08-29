use std::{
    collections::{BTreeSet, VecDeque},
    fmt,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use secrecy::{ExposeSecret, SecretString};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time::{Instant, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{
        Error as WebSocketError, Message, client::IntoClientRequest, http::HeaderValue,
        protocol::WebSocketConfig,
    },
};
use venue_gateway_api::GatewayBinding;

use crate::execution::{mutation_unknown, parse_mutation_ack};
use crate::{
    GateAcceptedMutation, GateContractRules, GateCredentials, GateDispatchUnknown,
    GateExactOrderReadback, GateExactReadbackRequest, GateGatewayBinding, GateMutationKind,
    GatePreparedMutation, GatePreparedPrivateRead, GatePrivateChannel,
    GatePrivateReadbackCandidate, GateRawPrivateResponse,
};

const MAX_TRANSPORT_BODY_BYTES: usize = 2 * 1_024 * 1_024;
const MAX_PRE_LIVE_FRAMES: usize = 256;
const MAX_PRE_LIVE_BYTES: usize = 1_048_576;
const PRIVATE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const PRIVATE_CHANNELS: [GatePrivateChannel; 4] = [
    GatePrivateChannel::Orders,
    GatePrivateChannel::UserTrades,
    GatePrivateChannel::Positions,
    GatePrivateChannel::Balances,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GateTransportLimits {
    operation_timeout: Duration,
    maximum_body_bytes: usize,
}

impl GateTransportLimits {
    pub fn new(
        operation_timeout: Duration,
        maximum_body_bytes: usize,
    ) -> Result<Self, GateTransportError> {
        if operation_timeout.is_zero()
            || operation_timeout > Duration::from_secs(60)
            || maximum_body_bytes == 0
            || maximum_body_bytes > MAX_TRANSPORT_BODY_BYTES
        {
            return Err(GateTransportError::Limits);
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GateMutationDispatch {
    Accepted(GateAcceptedMutation),
    Unknown(GateDispatchUnknown),
}

pub struct GateHttpTransport {
    client: reqwest::Client,
    binding: GatewayBinding,
    generation: u64,
    endpoint: String,
    limits: GateTransportLimits,
}

impl GateHttpTransport {
    pub fn new(
        binding: &GateGatewayBinding,
        generation: u64,
        limits: GateTransportLimits,
    ) -> Result<Self, GateTransportError> {
        Self::with_endpoint(
            binding,
            generation,
            binding.config().rest_origin().to_owned(),
            limits,
        )
    }

    fn with_endpoint(
        binding: &GateGatewayBinding,
        generation: u64,
        endpoint: String,
        limits: GateTransportLimits,
    ) -> Result<Self, GateTransportError> {
        if generation == 0 || endpoint.is_empty() {
            return Err(GateTransportError::Binding);
        }
        let client = reqwest::Client::builder()
            .connect_timeout(limits.operation_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .map_err(|_| GateTransportError::Http)?;
        Ok(Self {
            client,
            binding: binding.gateway_binding().clone(),
            generation,
            endpoint,
            limits,
        })
    }

    pub async fn execute_private_read(
        &self,
        binding: &GateGatewayBinding,
        credentials: &GateCredentials,
        rules: &GateContractRules,
        request: &GatePreparedPrivateRead,
        timestamp_ms: u64,
    ) -> Result<GateRawPrivateResponse, GateTransportError> {
        self.validate_binding(binding, rules, request.generation)?;
        request
            .validate(binding, rules)
            .map_err(|_| GateTransportError::Binding)?;
        let timestamp_sec = timestamp_sec(timestamp_ms)?;
        let signed = crate::sign_rest(
            credentials,
            timestamp_sec,
            "GET",
            &request.endpoint,
            &request.query,
            &[],
        )
        .map_err(|_| GateTransportError::Signing)?;
        let url = if request.query.is_empty() {
            format!("{}{}", self.endpoint, request.endpoint)
        } else {
            format!("{}{}?{}", self.endpoint, request.endpoint, request.query)
        };
        let body = timeout(self.limits.operation_timeout, async {
            let response = add_signed_headers(self.client.get(url), &signed)?
                .send()
                .await
                .map_err(map_reqwest)?;
            read_response(response, self.limits.maximum_body_bytes).await
        })
        .await
        .map_err(|_| GateTransportError::Timeout)??;
        let payload = String::from_utf8(body.to_vec()).map_err(|_| GateTransportError::Protocol)?;
        GateRawPrivateResponse::from_response(
            binding,
            rules,
            request,
            timestamp_ms,
            unix_ms()?,
            payload,
        )
        .map_err(|_| GateTransportError::Protocol)
    }

    /// Consumes the one-shot mutation. Timeout or disconnect returns only an exact signed
    /// readback plan; it never returns the prepared request and never retries it.
    pub async fn execute_mutation(
        &self,
        binding: &GateGatewayBinding,
        credentials: &GateCredentials,
        rules: &GateContractRules,
        request: GatePreparedMutation,
        timestamp_ms: u64,
    ) -> Result<GateMutationDispatch, GateTransportError> {
        self.validate_binding(binding, rules, rules.instrument.generation)?;
        request
            .validate(binding, rules)
            .map_err(|_| GateTransportError::Binding)?;
        if request.body().len() > self.limits.maximum_body_bytes {
            return Err(GateTransportError::BodyTooLarge);
        }
        let timestamp_sec = timestamp_sec(timestamp_ms)?;
        let signed = request
            .sign(credentials, timestamp_sec)
            .map_err(|_| GateTransportError::Signing)?;
        let url = format!("{}{}", self.endpoint, request.endpoint());
        let method = match request.kind() {
            GateMutationKind::PlacePostOnly | GateMutationKind::ReduceOnce => reqwest::Method::POST,
            GateMutationKind::Cancel => reqwest::Method::DELETE,
        };
        let mut builder = self.client.request(method, url);
        if !request.body().is_empty() {
            builder = builder
                .header("content-type", "application/json")
                .body(request.body().to_vec());
        }
        let builder = add_signed_headers(builder, &signed)?;
        let operation = async {
            let response = builder.send().await.map_err(map_reqwest)?;
            read_response(response, self.limits.maximum_body_bytes).await
        };
        let body = match timeout(self.limits.operation_timeout, operation).await {
            Err(_) => {
                return mutation_unknown(request, unix_ms()?)
                    .map(GateMutationDispatch::Unknown)
                    .map_err(|_| GateTransportError::Protocol);
            }
            Ok(Err(GateTransportError::Disconnected | GateTransportError::Timeout)) => {
                return mutation_unknown(request, unix_ms()?)
                    .map(GateMutationDispatch::Unknown)
                    .map_err(|_| GateTransportError::Protocol);
            }
            Ok(Err(error)) => return Err(error),
            Ok(Ok(body)) => body,
        };
        parse_mutation_ack(binding, rules, request, &body, unix_ms()?)
            .map(GateMutationDispatch::Accepted)
            .map_err(|error| match error {
                crate::GateExecutionError::VenueRejected => GateTransportError::VenueRejected,
                _ => GateTransportError::Ack,
            })
    }

    pub async fn execute_exact_readback(
        &self,
        binding: &GateGatewayBinding,
        credentials: &GateCredentials,
        rules: &GateContractRules,
        request: &GateExactReadbackRequest,
        timestamp_ms: u64,
    ) -> Result<GateExactOrderReadback, GateTransportError> {
        self.validate_binding(binding, rules, request.generation)?;
        request
            .validate(binding, rules)
            .map_err(|_| GateTransportError::Binding)?;
        if timestamp_ms < request.not_before_ms {
            return Err(GateTransportError::Binding);
        }
        let signed = request
            .sign(credentials, timestamp_sec(timestamp_ms)?)
            .map_err(|_| GateTransportError::Signing)?;
        let url = format!("{}{}", self.endpoint, request.endpoint);
        let body = timeout(self.limits.operation_timeout, async {
            let response = add_signed_headers(self.client.get(url), &signed)?
                .send()
                .await
                .map_err(map_reqwest)?;
            read_response(response, self.limits.maximum_body_bytes).await
        })
        .await
        .map_err(|_| GateTransportError::Timeout)??;
        let payload = String::from_utf8(body.to_vec()).map_err(|_| GateTransportError::Protocol)?;
        GateExactOrderReadback::from_response(
            binding,
            rules,
            request,
            timestamp_ms,
            unix_ms()?,
            payload,
        )
        .map_err(|_| GateTransportError::Protocol)
    }

    fn validate_binding(
        &self,
        binding: &GateGatewayBinding,
        rules: &GateContractRules,
        generation: u64,
    ) -> Result<(), GateTransportError> {
        binding
            .validate_request_binding(&self.binding)
            .map_err(|_| GateTransportError::Binding)?;
        if self.binding != *binding.gateway_binding()
            || self.generation != generation
            || rules.instrument.generation != generation
            || rules.instrument.symbol != binding.gateway_binding().symbol
        {
            return Err(GateTransportError::Binding);
        }
        Ok(())
    }
}

fn add_signed_headers(
    mut builder: reqwest::RequestBuilder,
    signed: &crate::GateRestSignedHeaders,
) -> Result<reqwest::RequestBuilder, GateTransportError> {
    builder = builder
        .header("accept", "application/json")
        .header("content-type", "application/json")
        .header("X-Gate-Size-Decimal", "1");
    for name in ["KEY", "Timestamp", "SIGN"] {
        let value = signed.get(name).ok_or(GateTransportError::Signing)?;
        builder = builder.header(name, value);
    }
    Ok(builder)
}

async fn read_response(
    mut response: reqwest::Response,
    maximum_body_bytes: usize,
) -> Result<Bytes, GateTransportError> {
    if !response.status().is_success() {
        return Err(GateTransportError::HttpStatus);
    }
    if response
        .content_length()
        .is_some_and(|length| length > maximum_body_bytes as u64)
    {
        return Err(GateTransportError::BodyTooLarge);
    }
    let mut body = BytesMut::new();
    while let Some(chunk) = response.chunk().await.map_err(map_reqwest)? {
        let next = body
            .len()
            .checked_add(chunk.len())
            .ok_or(GateTransportError::BodyTooLarge)?;
        if next > maximum_body_bytes {
            return Err(GateTransportError::BodyTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

fn map_reqwest(error: reqwest::Error) -> GateTransportError {
    if error.is_timeout() {
        GateTransportError::Timeout
    } else {
        GateTransportError::Disconnected
    }
}

pub struct GatePrivateWsTransport<S = MaybeTlsStream<TcpStream>> {
    stream: WebSocketStream<S>,
    binding: GatewayBinding,
    generation: u64,
    endpoint: String,
    limits: GateTransportLimits,
    buffered: VecDeque<GatePrivateWsFrame>,
    buffered_bytes: usize,
    next_heartbeat_at: Instant,
}

impl<S> GatePrivateWsTransport<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub async fn next_raw_frame(&mut self) -> Result<GatePrivateWsFrame, GateTransportError> {
        if let Some(frame) = self.buffered.pop_front() {
            self.buffered_bytes = self.buffered_bytes.saturating_sub(frame.payload.len());
            return Ok(frame);
        }
        loop {
            if Instant::now() >= self.next_heartbeat_at {
                self.send_heartbeat().await?;
            }
            let message = timeout(self.limits.operation_timeout, self.stream.next())
                .await
                .map_err(|_| GateTransportError::Timeout)?
                .ok_or(GateTransportError::EndOfStream)?
                .map_err(map_websocket)?;
            match message {
                Message::Text(text) => {
                    if private_pong(&text)? {
                        continue;
                    }
                    return make_private_frame(
                        &self.binding,
                        self.generation,
                        Bytes::from(text.to_string()),
                        self.limits.maximum_body_bytes,
                        unix_ms()?,
                    );
                }
                Message::Ping(payload) => self
                    .stream
                    .send(Message::Pong(payload))
                    .await
                    .map_err(map_websocket)?,
                Message::Pong(_) => {}
                Message::Binary(_) | Message::Frame(_) => {
                    return Err(GateTransportError::Protocol);
                }
                Message::Close(_) => return Err(GateTransportError::EndOfStream),
            }
        }
    }

    async fn send_heartbeat(&mut self) -> Result<(), GateTransportError> {
        let timestamp_sec = timestamp_sec(unix_ms()?)?;
        timeout(
            self.limits.operation_timeout,
            self.stream.send(Message::Ping(Bytes::new())),
        )
        .await
        .map_err(|_| GateTransportError::Timeout)?
        .map_err(map_websocket)?;
        timeout(
            self.limits.operation_timeout,
            self.stream.send(Message::Text(
                json!({"time":timestamp_sec,"channel":"futures.ping"})
                    .to_string()
                    .into(),
            )),
        )
        .await
        .map_err(|_| GateTransportError::Timeout)?
        .map_err(map_websocket)?;
        self.next_heartbeat_at = Instant::now() + PRIVATE_HEARTBEAT_INTERVAL;
        Ok(())
    }
}

pub async fn connect_private_ws(
    binding: &GateGatewayBinding,
    credentials: &GateCredentials,
    rules: &GateContractRules,
    private: &GatePrivateReadbackCandidate,
    limits: GateTransportLimits,
) -> Result<GatePrivateWsTransport, GateTransportError> {
    if private.binding != *binding.gateway_binding()
        || private.generation != rules.instrument.generation
        || private.user_id.is_empty()
        || rules.instrument.symbol != binding.gateway_binding().symbol
    {
        return Err(GateTransportError::Binding);
    }
    let endpoint = binding.config().usdt_futures_ws().to_owned();
    let mut request = endpoint
        .clone()
        .into_client_request()
        .map_err(|_| GateTransportError::Binding)?;
    request
        .headers_mut()
        .insert("X-Gate-Size-Decimal", HeaderValue::from_static("1"));
    let websocket = WebSocketConfig::default()
        .max_message_size(Some(limits.maximum_body_bytes))
        .max_frame_size(Some(limits.maximum_body_bytes));
    let (stream, _) = timeout(
        limits.operation_timeout,
        connect_async_with_config(request, Some(websocket), false),
    )
    .await
    .map_err(|_| GateTransportError::Timeout)?
    .map_err(map_websocket)?;
    authenticate_private_stream(
        stream,
        endpoint,
        binding,
        credentials,
        rules,
        &private.user_id,
        private.generation,
        limits,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn authenticate_private_stream<S>(
    mut stream: WebSocketStream<S>,
    endpoint: String,
    binding: &GateGatewayBinding,
    credentials: &GateCredentials,
    rules: &GateContractRules,
    user_id: &str,
    generation: u64,
    limits: GateTransportLimits,
) -> Result<GatePrivateWsTransport<S>, GateTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if generation == 0
        || generation != rules.instrument.generation
        || rules.instrument.symbol != binding.gateway_binding().symbol
        || user_id.is_empty()
    {
        return Err(GateTransportError::Binding);
    }
    let timestamp_sec = timestamp_sec(unix_ms()?)?;
    for channel in PRIVATE_CHANNELS {
        let auth = crate::sign_websocket_subscription(credentials, channel, timestamp_sec)
            .map_err(|_| GateTransportError::Signing)?;
        let payload = match channel {
            GatePrivateChannel::Orders | GatePrivateChannel::UserTrades => {
                json!([user_id, rules.native_symbol])
            }
            GatePrivateChannel::Positions => json!([user_id, rules.native_symbol]),
            GatePrivateChannel::Balances => json!([user_id]),
        };
        let message = json!({
            "time": timestamp_sec,
            "channel": channel.as_str(),
            "event": "subscribe",
            "payload": payload,
            "auth": {
                "method": auth.get("method").ok_or(GateTransportError::Signing)?,
                "KEY": auth.get("KEY").ok_or(GateTransportError::Signing)?,
                "SIGN": auth.get("SIGN").ok_or(GateTransportError::Signing)?,
            }
        });
        let secret = SecretString::from(
            serde_json::to_string(&message).map_err(|_| GateTransportError::Protocol)?,
        );
        timeout(
            limits.operation_timeout,
            stream.send(Message::Text(secret.expose_secret().into())),
        )
        .await
        .map_err(|_| GateTransportError::Timeout)?
        .map_err(map_websocket)?;
    }
    let (buffered, buffered_bytes) =
        read_subscription_acks(&mut stream, binding, generation, limits).await?;
    Ok(GatePrivateWsTransport {
        stream,
        binding: binding.gateway_binding().clone(),
        generation,
        endpoint,
        limits,
        buffered,
        buffered_bytes,
        next_heartbeat_at: Instant::now() + PRIVATE_HEARTBEAT_INTERVAL,
    })
}

async fn read_subscription_acks<S>(
    stream: &mut WebSocketStream<S>,
    binding: &GateGatewayBinding,
    generation: u64,
    limits: GateTransportLimits,
) -> Result<(VecDeque<GatePrivateWsFrame>, usize), GateTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let expected = PRIVATE_CHANNELS
        .iter()
        .map(|channel| channel.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    let mut acknowledged = BTreeSet::new();
    let mut buffered = VecDeque::new();
    let mut buffered_bytes = 0_usize;
    while acknowledged != expected {
        let message = timeout(limits.operation_timeout, stream.next())
            .await
            .map_err(|_| GateTransportError::Timeout)?
            .ok_or(GateTransportError::EndOfStream)?
            .map_err(map_websocket)?;
        match message {
            Message::Text(text) => {
                let payload = Bytes::from(text.to_string());
                if payload.len() > limits.maximum_body_bytes {
                    return Err(GateTransportError::BodyTooLarge);
                }
                let value: Value =
                    serde_json::from_slice(&payload).map_err(|_| GateTransportError::Ack)?;
                let channel = value
                    .get("channel")
                    .and_then(Value::as_str)
                    .ok_or(GateTransportError::Ack)?;
                if value.get("event").and_then(Value::as_str) == Some("subscribe") {
                    let status = value
                        .get("result")
                        .and_then(Value::as_object)
                        .and_then(|result| result.get("status"))
                        .and_then(Value::as_str);
                    if !expected.contains(channel)
                        || status != Some("success")
                        || !acknowledged.insert(channel.to_owned())
                    {
                        return Err(GateTransportError::Ack);
                    }
                } else {
                    let frame = make_private_frame(
                        binding.gateway_binding(),
                        generation,
                        payload,
                        limits.maximum_body_bytes,
                        unix_ms()?,
                    )?;
                    buffered_bytes = buffered_bytes
                        .checked_add(frame.payload.len())
                        .ok_or(GateTransportError::PreLiveBufferOverflow)?;
                    if buffered.len() >= MAX_PRE_LIVE_FRAMES || buffered_bytes > MAX_PRE_LIVE_BYTES
                    {
                        return Err(GateTransportError::PreLiveBufferOverflow);
                    }
                    buffered.push_back(frame);
                }
            }
            Message::Ping(payload) => stream
                .send(Message::Pong(payload))
                .await
                .map_err(map_websocket)?,
            Message::Pong(_) => {}
            Message::Binary(_) | Message::Frame(_) => return Err(GateTransportError::Protocol),
            Message::Close(_) => return Err(GateTransportError::EndOfStream),
        }
    }
    Ok((buffered, buffered_bytes))
}

#[derive(Clone, Eq, PartialEq)]
pub struct GatePrivateWsFrame {
    pub binding: GatewayBinding,
    pub generation: u64,
    pub received_at_ms: u64,
    pub channel: String,
    pub payload: Bytes,
}

impl fmt::Debug for GatePrivateWsFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatePrivateWsFrame")
            .field("binding", &self.binding)
            .field("generation", &self.generation)
            .field("received_at_ms", &self.received_at_ms)
            .field("channel", &self.channel)
            .field("payload", &"[REDACTED PRIVATE FRAME]")
            .finish()
    }
}

fn make_private_frame(
    binding: &GatewayBinding,
    generation: u64,
    payload: Bytes,
    maximum_body_bytes: usize,
    received_at_ms: u64,
) -> Result<GatePrivateWsFrame, GateTransportError> {
    if payload.len() > maximum_body_bytes || received_at_ms == 0 {
        return Err(GateTransportError::BodyTooLarge);
    }
    let value: Value =
        serde_json::from_slice(&payload).map_err(|_| GateTransportError::Protocol)?;
    let channel = value
        .get("channel")
        .and_then(Value::as_str)
        .ok_or(GateTransportError::Protocol)?;
    if !PRIVATE_CHANNELS
        .iter()
        .any(|expected| expected.as_str() == channel)
        || value.get("event").and_then(Value::as_str) != Some("update")
        || value.get("result").is_none()
    {
        return Err(GateTransportError::Protocol);
    }
    Ok(GatePrivateWsFrame {
        binding: binding.clone(),
        generation,
        received_at_ms,
        channel: channel.to_owned(),
        payload,
    })
}

fn private_pong(payload: &str) -> Result<bool, GateTransportError> {
    let value: Value = serde_json::from_str(payload).map_err(|_| GateTransportError::Protocol)?;
    Ok(value.get("channel").and_then(Value::as_str) == Some("futures.pong"))
}

fn map_websocket(error: WebSocketError) -> GateTransportError {
    if matches!(error, WebSocketError::Capacity(_)) {
        GateTransportError::BodyTooLarge
    } else {
        GateTransportError::Disconnected
    }
}

fn timestamp_sec(timestamp_ms: u64) -> Result<i64, GateTransportError> {
    i64::try_from(timestamp_ms / 1_000)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(GateTransportError::Clock)
}

fn unix_ms() -> Result<u64, GateTransportError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| GateTransportError::Clock)?
        .as_millis();
    u64::try_from(millis).map_err(|_| GateTransportError::Clock)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GateTransportError {
    #[error("Gate transport limits are invalid")]
    Limits,
    #[error("Gate transport binding, mode, or generation does not match")]
    Binding,
    #[error("Gate transport operation timed out")]
    Timeout,
    #[error("Gate HTTP client could not be created")]
    Http,
    #[error("Gate HTTP returned a non-success status")]
    HttpStatus,
    #[error("Gate transport body exceeded its configured bound")]
    BodyTooLarge,
    #[error("Gate private pre-live frame buffer exceeded its hard bound")]
    PreLiveBufferOverflow,
    #[error("Gate transport disconnected; a mutation outcome is UNKNOWN")]
    Disconnected,
    #[error("Gate private WebSocket reached explicit EOF")]
    EndOfStream,
    #[error("Gate transport payload or frame is invalid")]
    Protocol,
    #[error("Gate transport acknowledgement is invalid")]
    Ack,
    #[error("Gate rejected the mutation request")]
    VenueRejected,
    #[error("Gate transport signing failed")]
    Signing,
    #[error("Gate transport clock is invalid")]
    Clock,
}

#[cfg(test)]
mod tests {
    use std::io;

    use rust_decimal::Decimal;
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_async, client_async};
    use venue_domain::domain::{
        Amount, CommandId, Instrument, MarketKind, OrderCommand, OrderOwner, OrderPurpose,
        OrderSide, PositionSide, Price,
    };
    use venue_gateway_api::{GatewayMode, VenueId};

    use super::*;

    const ACCOUNT: &str = "00000000-0000-4000-8000-000000000001";
    type TestError = Box<dyn std::error::Error + Send + Sync>;

    fn facts() -> Result<
        (
            GateGatewayBinding,
            GateCredentials,
            GateContractRules,
            OrderCommand,
        ),
        TestError,
    > {
        let binding = GateGatewayBinding::new(GatewayBinding::new(
            VenueId::Gate,
            GatewayMode::Test,
            ACCOUNT,
            "DOGE/USDT".parse()?,
        )?)?;
        let rules = GateContractRules {
            native_symbol: "DOGE_USDT".to_owned(),
            instrument: Instrument {
                symbol: "DOGE/USDT".parse()?,
                market: MarketKind::LinearPerpetual,
                settlement_asset: Some("USDT".parse()?),
                generation: 7,
                price_tick: Price::new(Decimal::new(1, 5))?,
                quantity_step: Decimal::new(1, 1),
                minimum_notional: Amount::new("USDT".parse()?, Decimal::ZERO),
            },
            quanto_multiplier: Decimal::new(1, 1),
            minimum_contracts: Decimal::ONE,
            decimal_contracts: false,
        };
        let command = OrderCommand {
            command_id: CommandId::new("command")?,
            client_order_id: CommandId::new("grid_long_1")?,
            owner: OrderOwner {
                strategy_instance_id: "grid".to_owned(),
                run_id: "run".to_owned(),
                exchange: "gate".to_owned(),
                account: ACCOUNT.to_owned(),
                symbol: "DOGE/USDT".parse()?,
                purpose: OrderPurpose::Entry,
            },
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Price::new(Decimal::new(1, 1))?,
            reduce_only: false,
        };
        Ok((
            binding,
            GateCredentials::from_values("key", "secret")?,
            rules,
            command,
        ))
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
                        if request.windows(4).any(|window| window == b"\r\n\r\n")
                            && request_body_complete(&request)
                        {
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
                stream.writable().await?;
                let _ = stream.try_write(&response)?;
            }
            Ok(request)
        });
        Ok((format!("http://{address}"), task))
    }

    fn request_body_complete(request: &[u8]) -> bool {
        let Some(end) = request.windows(4).position(|window| window == b"\r\n\r\n") else {
            return false;
        };
        let body_start = end + 4;
        let headers = String::from_utf8_lossy(&request[..body_start]);
        let length = headers.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        });
        length.is_none_or(|length| request.len() >= body_start + length)
    }

    fn response(body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    fn ack() -> &'static str {
        r#"{"id":"9001","contract":"DOGE_USDT","size":"10","left":"10","is_reduce_only":false,"status":"open","finish_as":"","price":"0.1","fill_price":"0","text":"t-grid_long_1"}"#
    }

    #[tokio::test]
    async fn signed_test_http_never_changes_mode_and_ack_returns_exact_readback()
    -> Result<(), TestError> {
        let (binding, credentials, rules, command) = facts()?;
        let request = crate::prepare_limit_post_only(&binding, &rules, &command)?;
        let expected_body = std::str::from_utf8(request.body())?.to_owned();
        let (endpoint, server) = http_mock(Some(response(ack())), Duration::ZERO).await?;
        let limits = GateTransportLimits::new(Duration::from_secs(2), 16 * 1024)?;
        let transport = GateHttpTransport::with_endpoint(&binding, 7, endpoint, limits)?;
        let dispatch = transport
            .execute_mutation(&binding, &credentials, &rules, request, 1_700_000_000_000)
            .await?;
        let GateMutationDispatch::Accepted(accepted) = dispatch else {
            return Err("expected ack".into());
        };
        assert_eq!(accepted.readback.endpoint, "/futures/usdt/orders/9001");
        let sent = String::from_utf8(server.await??)?;
        assert!(sent.starts_with("POST /futures/usdt/orders HTTP/1.1"));
        assert!(sent.contains("sign:"));
        assert!(sent.contains("x-gate-size-decimal: 1"));
        assert!(sent.ends_with(&expected_body));
        Ok(())
    }

    #[tokio::test]
    async fn timeout_and_disconnect_return_unknown_without_a_second_dispatch()
    -> Result<(), TestError> {
        let (binding, credentials, rules, command) = facts()?;
        let limits = GateTransportLimits::new(Duration::from_millis(30), 16 * 1024)?;
        let (endpoint, delayed) =
            http_mock(Some(response(ack())), Duration::from_millis(200)).await?;
        let transport = GateHttpTransport::with_endpoint(&binding, 7, endpoint, limits)?;
        let dispatch = transport
            .execute_mutation(
                &binding,
                &credentials,
                &rules,
                crate::prepare_limit_post_only(&binding, &rules, &command)?,
                1_700_000_000_000,
            )
            .await?;
        assert!(matches!(dispatch, GateMutationDispatch::Unknown(_)));
        delayed.abort();

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            drop(stream);
            Ok::<bool, io::Error>(
                timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_ok(),
            )
        });
        let transport = GateHttpTransport::with_endpoint(&binding, 7, endpoint, limits)?;
        let dispatch = transport
            .execute_mutation(
                &binding,
                &credentials,
                &rules,
                crate::prepare_limit_post_only(&binding, &rules, &command)?,
                1_700_000_000_000,
            )
            .await?;
        assert!(matches!(dispatch, GateMutationDispatch::Unknown(_)));
        assert!(!server.await??);
        Ok(())
    }

    #[tokio::test]
    async fn websocket_buffers_updates_until_all_four_signed_subscriptions_ack()
    -> Result<(), TestError> {
        let (binding, credentials, rules, _) = facts()?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut ws = accept_async(stream).await?;
            let mut subscriptions = Vec::new();
            for _ in 0..4 {
                let message = ws.next().await.ok_or("closed")??;
                let Message::Text(text) = message else {
                    return Err::<(), TestError>("expected text".into());
                };
                let value: Value = serde_json::from_str(&text)?;
                subscriptions.push(
                    value
                        .get("channel")
                        .and_then(Value::as_str)
                        .ok_or("channel")?
                        .to_owned(),
                );
            }
            ws.send(Message::Text(
                r#"{"channel":"futures.orders","event":"update","result":[]}"#.into(),
            ))
            .await?;
            for channel in subscriptions {
                ws.send(Message::Text(
                    json!({"channel":channel,"event":"subscribe","result":{"status":"success"}})
                        .to_string()
                        .into(),
                ))
                .await?;
            }
            Ok::<(), TestError>(())
        });
        let stream = TcpStream::connect(address).await?;
        let (client, _) = client_async(format!("ws://{address}"), stream).await?;
        let limits = GateTransportLimits::new(Duration::from_secs(2), 16 * 1024)?;
        let mut private = authenticate_private_stream(
            client,
            format!("ws://{address}"),
            &binding,
            &credentials,
            &rules,
            "42",
            7,
            limits,
        )
        .await?;
        let frame = private.next_raw_frame().await?;
        assert_eq!(frame.channel, "futures.orders");
        assert_eq!(frame.binding.mode, GatewayMode::Test);
        server.await??;
        Ok(())
    }
}
