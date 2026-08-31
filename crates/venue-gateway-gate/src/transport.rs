use std::{
    collections::{BTreeSet, VecDeque},
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use secrecy::{ExposeSecret, SecretString};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
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
    GateAcceptedMutation, GateAuthenticatedRecoverySession, GateAuthenticatedRecoverySessionLease,
    GateContractRules, GateCredentials, GateDispatchUnknown, GateExactOrderReadback,
    GateExactReadbackRequest, GateFreshRecoveryError, GateGatewayBinding, GateMutationKind,
    GatePreparedMutation, GatePreparedPrivateRead, GatePrivateChannel,
    GatePrivateReadbackCandidate, GateRawPrivateResponse, GateRecoverySymbolScope,
    GateRuntimeRecoveryAwaitGuard, GateRuntimeRecoveryScope,
};

const MAX_TRANSPORT_BODY_BYTES: usize = 2 * 1_024 * 1_024;
const MAX_PRE_LIVE_FRAMES: usize = 256;
const MAX_PRE_LIVE_BYTES: usize = 1_048_576;
const PRIVATE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
static NEXT_RECOVERY_CONTROL_NONCE: AtomicU64 = AtomicU64::new(1);
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

    pub(crate) fn with_endpoint(
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
        self.execute_private_read_inner(binding, credentials, rules, request, timestamp_ms, None)
            .await
    }

    /// Reads the public USDT contract catalogue from Gate's fixed LIVE origin. This supplies the
    /// real multiplier/tick evidence required to normalize every account-wide position or order.
    pub async fn fetch_public_contracts(&self) -> Result<String, GateTransportError> {
        let url = format!("{}{}", self.endpoint, crate::endpoints::FUTURES_CONTRACTS);
        let body = self.send_bounded(self.client.get(url)).await?;
        String::from_utf8(body.to_vec()).map_err(|_| GateTransportError::Protocol)
    }

    /// Reads a deliberately shallow, sequenced public book from the same fixed LIVE origin as
    /// the contract catalogue.  Parsing remains at the account boundary because only it owns
    /// the current rule generation and the normalization freshness decision.
    pub async fn fetch_public_order_book(&self, path: &str) -> Result<String, GateTransportError> {
        if !path.starts_with("/futures/usdt/order_book?") {
            return Err(GateTransportError::Binding);
        }
        let url = format!("{}{}", self.endpoint, path);
        let body = self.send_bounded(self.client.get(url)).await?;
        String::from_utf8(body.to_vec()).map_err(|_| GateTransportError::Protocol)
    }

    /// Performs a signed account-wide GET for the risk/snapshot collector only. It is deliberately
    /// not a mutation surface and does not expose raw signing material to Node.
    pub async fn execute_account_risk_read(
        &self,
        binding: &GateGatewayBinding,
        credentials: &GateCredentials,
        rules: &GateContractRules,
        endpoint: &str,
        query: &str,
        timestamp_ms: u64,
    ) -> Result<String, GateTransportError> {
        self.validate_binding(binding, rules, rules.instrument.generation)?;
        if !matches!(
            endpoint,
            crate::endpoints::FUTURES_ACCOUNT
                | crate::endpoints::POSITIONS
                | crate::endpoints::FUTURES_OPEN_ORDERS
                | crate::endpoints::FUTURES_FILLS
        ) || timestamp_ms == 0
        {
            return Err(GateTransportError::Binding);
        }
        let signed = crate::sign_rest(
            credentials,
            timestamp_sec(timestamp_ms)?,
            "GET",
            endpoint,
            query,
            &[],
        )
        .map_err(|_| GateTransportError::Signing)?;
        let url = if query.is_empty() {
            format!("{}{}", self.endpoint, endpoint)
        } else {
            format!("{}{}?{query}", self.endpoint, endpoint)
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
        String::from_utf8(body.to_vec()).map_err(|_| GateTransportError::Protocol)
    }

    pub(crate) async fn execute_private_read_guarded(
        &self,
        binding: &GateGatewayBinding,
        credentials: &GateCredentials,
        rules: &GateContractRules,
        request: &GatePreparedPrivateRead,
        timestamp_ms: u64,
        runtime_guard: Option<GateRuntimeRecoveryAwaitGuard<'_>>,
    ) -> Result<GateRawPrivateResponse, GateTransportError> {
        self.execute_private_read_inner(
            binding,
            credentials,
            rules,
            request,
            timestamp_ms,
            runtime_guard,
        )
        .await
    }

    async fn execute_private_read_inner(
        &self,
        binding: &GateGatewayBinding,
        credentials: &GateCredentials,
        rules: &GateContractRules,
        request: &GatePreparedPrivateRead,
        timestamp_ms: u64,
        runtime_guard: Option<GateRuntimeRecoveryAwaitGuard<'_>>,
    ) -> Result<GateRawPrivateResponse, GateTransportError> {
        self.validate_binding(binding, rules, request.generation)?;
        revalidate_runtime_guard(runtime_guard)?;
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
            revalidate_runtime_guard(runtime_guard)?;
            read_response_guarded(response, self.limits.maximum_body_bytes, runtime_guard).await
        })
        .await
        .map_err(|_| GateTransportError::Timeout)??;
        revalidate_runtime_guard(runtime_guard)?;
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

    pub(crate) const fn limits(&self) -> GateTransportLimits {
        self.limits
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
        {
            return Err(GateTransportError::Binding);
        }
        Ok(())
    }

    async fn send_bounded(
        &self,
        builder: reqwest::RequestBuilder,
    ) -> Result<Bytes, GateTransportError> {
        timeout(self.limits.operation_timeout, async {
            let response = builder.send().await.map_err(map_reqwest)?;
            read_response(response, self.limits.maximum_body_bytes).await
        })
        .await
        .map_err(|_| GateTransportError::Timeout)?
    }

    pub(crate) fn matches_recovery_session(
        &self,
        mode: venue_gateway_api::GatewayMode,
        trading_account_id: &str,
        rest_origin: &str,
        limits: GateTransportLimits,
        request_generation: u64,
        request_binding: &GatewayBinding,
    ) -> bool {
        self.binding.mode == mode
            && self.binding.trading_account_id == trading_account_id
            && request_binding.mode == mode
            && request_binding.trading_account_id == trading_account_id
            && self.binding == *request_binding
            && self.generation == request_generation
            && (cfg!(test) || self.endpoint == rest_origin)
            && self.limits == limits
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
    response: reqwest::Response,
    maximum_body_bytes: usize,
) -> Result<Bytes, GateTransportError> {
    read_response_guarded(response, maximum_body_bytes, None).await
}

async fn read_response_guarded(
    mut response: reqwest::Response,
    maximum_body_bytes: usize,
    runtime_guard: Option<GateRuntimeRecoveryAwaitGuard<'_>>,
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
        revalidate_runtime_guard(runtime_guard)?;
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

fn revalidate_runtime_guard(
    runtime_guard: Option<GateRuntimeRecoveryAwaitGuard<'_>>,
) -> Result<(), GateTransportError> {
    runtime_guard
        .map(GateRuntimeRecoveryAwaitGuard::revalidate)
        .transpose()
        .map(|_| ())
        .map_err(|_| GateTransportError::RuntimeScope)
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
    recovery_session: Option<GateAuthenticatedRecoverySessionLease>,
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

    /// Freezes one bounded, single-use recovery universe on this authenticated private transport.
    pub fn begin_recovery_session<I>(
        &self,
        symbols: I,
        deadline_at_ms: u64,
        maximum_total_bytes: usize,
        maximum_total_pages: u32,
    ) -> Result<GateAuthenticatedRecoverySession, GateFreshRecoveryError>
    where
        I: IntoIterator<Item = GateRecoverySymbolScope>,
    {
        self.recovery_session
            .as_ref()
            .ok_or(GateFreshRecoveryError::AuthenticatedSessionRequired)
            .and_then(|lease| {
                lease.begin(
                    symbols,
                    deadline_at_ms,
                    maximum_total_bytes,
                    maximum_total_pages,
                )
            })
    }

    /// Freezes the shared-runtime registry/root/Owner/Unknown commitment into the authenticated
    /// four-ACK private session before any signed recovery GET can be prepared.
    pub fn begin_runtime_recovery_session<I>(
        &self,
        runtime_scope: GateRuntimeRecoveryScope,
        symbols: I,
        deadline_at_ms: u64,
        maximum_total_bytes: usize,
        maximum_total_pages: u32,
    ) -> Result<GateAuthenticatedRecoverySession, GateFreshRecoveryError>
    where
        I: IntoIterator<Item = GateRecoverySymbolScope>,
    {
        self.recovery_session
            .as_ref()
            .ok_or(GateFreshRecoveryError::AuthenticatedSessionRequired)
            .and_then(|lease| {
                lease.begin_runtime(
                    runtime_scope,
                    symbols,
                    deadline_at_ms,
                    maximum_total_bytes,
                    maximum_total_pages,
                )
            })
    }

    /// Proves the exact private socket is still alive. Only the unique binary pong for this call
    /// succeeds; stale binary pongs and Gate text pongs are ignored.
    pub async fn revalidate_recovery_session(
        &mut self,
        session: &GateAuthenticatedRecoverySession,
    ) -> Result<(), GateTransportError> {
        self.revalidate_recovery_session_guarded(session, None)
            .await
    }

    pub(crate) async fn revalidate_recovery_session_guarded(
        &mut self,
        session: &GateAuthenticatedRecoverySession,
        runtime_guard: Option<GateRuntimeRecoveryAwaitGuard<'_>>,
    ) -> Result<(), GateTransportError> {
        self.validate_recovery_session_identity(session)?;
        revalidate_runtime_guard(runtime_guard)?;
        let nonce = recovery_control_nonce(session)?;
        let result = async {
            timeout(
                self.limits.operation_timeout,
                self.stream
                    .send(Message::Ping(Bytes::copy_from_slice(&nonce))),
            )
            .await
            .map_err(|_| GateTransportError::Timeout)?
            .map_err(map_websocket)?;
            revalidate_runtime_guard(runtime_guard)?;
            loop {
                let message = timeout(self.limits.operation_timeout, self.stream.next())
                    .await
                    .map_err(|_| GateTransportError::Timeout)?
                    .ok_or(GateTransportError::EndOfStream)?
                    .map_err(map_websocket)?;
                revalidate_runtime_guard(runtime_guard)?;
                match message {
                    Message::Pong(payload) if recovery_control_pong_matches(&payload, &nonce) => {
                        break;
                    }
                    Message::Pong(_) => continue,
                    Message::Text(text) => {
                        if private_pong(&text)? {
                            continue;
                        }
                        self.buffer_recovery_frame(Bytes::from(text.to_string()))?;
                    }
                    Message::Ping(payload) => self
                        .stream
                        .send(Message::Pong(payload))
                        .await
                        .map_err(map_websocket)
                        .and_then(|()| revalidate_runtime_guard(runtime_guard))?,
                    Message::Binary(_) | Message::Frame(_) => {
                        return Err(GateTransportError::Protocol);
                    }
                    Message::Close(_) => return Err(GateTransportError::EndOfStream),
                }
            }
            revalidate_runtime_guard(runtime_guard)?;
            self.validate_recovery_session_identity(session)
        }
        .await;
        if result.is_err() {
            session.revoke();
        }
        result
    }

    fn validate_recovery_session_identity(
        &self,
        session: &GateAuthenticatedRecoverySession,
    ) -> Result<(), GateTransportError> {
        session
            .validate_current()
            .map_err(|_| GateTransportError::Session)?;
        if self.binding.mode != session.mode()
            || self.binding.trading_account_id != session.trading_account_id()
            || self.generation != session.request_generation()
            || self.endpoint != session.private_ws_endpoint()
            || self.limits != session.transport_limits()
        {
            return Err(GateTransportError::Session);
        }
        Ok(())
    }

    fn buffer_recovery_frame(&mut self, payload: Bytes) -> Result<(), GateTransportError> {
        let frame = make_private_frame(
            &self.binding,
            self.generation,
            payload,
            self.limits.maximum_body_bytes,
            unix_ms()?,
        )?;
        self.buffered_bytes = self
            .buffered_bytes
            .checked_add(frame.payload.len())
            .ok_or(GateTransportError::PreLiveBufferOverflow)?;
        if self.buffered.len() >= MAX_PRE_LIVE_FRAMES || self.buffered_bytes > MAX_PRE_LIVE_BYTES {
            return Err(GateTransportError::PreLiveBufferOverflow);
        }
        self.buffered.push_back(frame);
        Ok(())
    }

    pub async fn next_raw_frame(&mut self) -> Result<GatePrivateWsFrame, GateTransportError> {
        let result = self.next_raw_frame_inner().await;
        if result.is_err()
            && let Some(session) = &self.recovery_session
        {
            session.revoke();
        }
        result
    }

    async fn next_raw_frame_inner(&mut self) -> Result<GatePrivateWsFrame, GateTransportError> {
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

impl<S> Drop for GatePrivateWsTransport<S> {
    fn drop(&mut self) {
        if let Some(session) = &self.recovery_session {
            session.revoke();
        }
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
        Some(binding.config().rest_origin().to_owned()),
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
    recovery_rest_origin: Option<String>,
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
    let recovery_session = recovery_rest_origin
        .map(|rest_origin| {
            GateAuthenticatedRecoverySessionLease::issue(
                binding,
                rest_origin,
                endpoint.clone(),
                generation,
                limits,
                credentials,
            )
        })
        .transpose()
        .map_err(|_| GateTransportError::Session)?;
    Ok(GatePrivateWsTransport {
        stream,
        binding: binding.gateway_binding().clone(),
        generation,
        endpoint,
        limits,
        buffered,
        buffered_bytes,
        next_heartbeat_at: Instant::now() + PRIVATE_HEARTBEAT_INTERVAL,
        recovery_session,
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

fn recovery_control_nonce(
    session: &GateAuthenticatedRecoverySession,
) -> Result<[u8; 32], GateTransportError> {
    let serial = NEXT_RECOVERY_CONTROL_NONCE
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(1)
        })
        .map_err(|_| GateTransportError::Session)?;
    let mut digest = Sha256::new();
    digest.update(b"venue-gate-recovery-control-nonce-v1");
    digest.update(session.request_universe_sha256());
    digest.update(session.connection_generation().to_be_bytes());
    digest.update(session.private_generation().to_be_bytes());
    digest.update(session.attempt_id().to_be_bytes());
    digest.update(serial.to_be_bytes());
    Ok(digest.finalize().into())
}

fn recovery_control_pong_matches(payload: &Bytes, nonce: &[u8; 32]) -> bool {
    payload.as_ref() == nonce
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
    #[error("Gate runtime recovery scope drifted across a network await")]
    RuntimeScope,
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
    #[error("Gate authenticated recovery session could not be issued")]
    Session,
}

#[cfg(test)]
mod tests {
    use std::io;

    use rust_decimal::Decimal;
    use tokio::{io::AsyncWriteExt, net::TcpListener};
    use tokio_tungstenite::{accept_async, client_async};
    use venue_domain::domain::{
        Amount, CommandId, Instrument, MarketKind, OrderCommand, OrderOwner, OrderPurpose,
        OrderSide, PositionSide, Price,
    };
    use venue_gateway_api::{GatewayMode, VenueId};

    use super::*;

    const ACCOUNT: &str = "00000000-0000-4000-8000-000000000001";
    type TestError = Box<dyn std::error::Error + Send + Sync>;

    struct StaticRuntimeScope([u8; 32]);

    impl crate::GateRuntimeRecoveryRevalidator for StaticRuntimeScope {
        fn current_scope_sha256(&self) -> Option<[u8; 32]> {
            Some(self.0)
        }
    }

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
            GatewayMode::Live,
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

    async fn recovery_http_mock(
        bodies: [String; 4],
    ) -> Result<(String, tokio::task::JoinHandle<io::Result<Vec<Vec<u8>>>>), io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let task = tokio::spawn(async move {
            let mut requests = Vec::new();
            for body in bodies {
                let (mut stream, _) = listener.accept().await?;
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
                stream.write_all(&response(&body)).await?;
                requests.push(request);
            }
            Ok(requests)
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

    #[test]
    fn recovery_liveness_accepts_only_the_exact_binary_pong() {
        let nonce = [7_u8; 32];
        assert!(recovery_control_pong_matches(
            &Bytes::copy_from_slice(&nonce),
            &nonce
        ));
        assert!(!recovery_control_pong_matches(
            &Bytes::from_static(b"stale-pong"),
            &nonce
        ));
        assert!(!recovery_control_pong_matches(&Bytes::new(), &nonce));
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
            let ping = ws.next().await.ok_or("missing recovery ping")??;
            let Message::Ping(control_nonce) = ping else {
                return Err::<(), TestError>("recovery control must use binary ping".into());
            };
            ws.send(Message::Pong(Bytes::from_static(b"stale-pong")))
                .await?;
            ws.send(Message::Text(
                r#"{"channel":"futures.pong","event":"update","result":{}}"#.into(),
            ))
            .await?;
            ws.send(Message::Text(
                r#"{"channel":"futures.positions","event":"update","result":[]}"#.into(),
            ))
            .await?;
            ws.send(Message::Pong(control_nonce)).await?;
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
            Some("http://signed-gate-recovery.test".to_owned()),
        )
        .await?;
        let session = private.begin_recovery_session(
            [GateRecoverySymbolScope::verified(
                binding.clone(),
                rules.clone(),
                crate::GateFillsCursor::default(),
            )?],
            unix_ms()?.saturating_add(2_000),
            64 * 1024,
            1,
        )?;
        assert!(session.is_current());
        assert_eq!(session.mode(), GatewayMode::Live);
        assert_eq!(session.trading_account_id(), ACCOUNT);
        assert_eq!(session.rest_origin(), "http://signed-gate-recovery.test");
        assert_eq!(session.private_ws_endpoint(), format!("ws://{address}"));
        assert_eq!(session.request_generation(), 7);
        private.revalidate_recovery_session(&session).await?;
        let frame = private.next_raw_frame().await?;
        assert_eq!(frame.channel, "futures.orders");
        assert_eq!(frame.binding.mode, GatewayMode::Live);
        assert_eq!(private.next_raw_frame().await?.channel, "futures.positions");
        let account = crate::prepare_private_read(
            &binding,
            &rules,
            7,
            session.attempt_id(),
            crate::GatePrivateReadSource::Account,
            crate::GateFillsCursor::default(),
        )?;
        session.reserve_get(&account, limits.maximum_body_bytes())?;
        session.settle_get(&account, 2, None)?;
        let positions = crate::prepare_private_read(
            &binding,
            &rules,
            7,
            session.attempt_id(),
            crate::GatePrivateReadSource::DualPositions,
            crate::GateFillsCursor::default(),
        )?;
        assert!(matches!(
            session.reserve_get(&positions, limits.maximum_body_bytes()),
            Err(GateFreshRecoveryError::Budget)
        ));
        let replacement = private.begin_recovery_session(
            [GateRecoverySymbolScope::verified(
                binding.clone(),
                rules.clone(),
                crate::GateFillsCursor::default(),
            )?],
            unix_ms()?.saturating_add(2_000),
            64 * 1024,
            8,
        )?;
        assert!(!session.is_current());
        assert!(replacement.is_current());
        server.await??;
        assert!(matches!(
            private.next_raw_frame().await,
            Err(GateTransportError::EndOfStream | GateTransportError::Disconnected)
        ));
        assert!(!replacement.is_current());
        Ok(())
    }

    #[tokio::test]
    async fn runtime_authenticated_collection_reads_all_faces_before_committing_one_bundle()
    -> Result<(), TestError> {
        let (_, credentials, rules, _) = facts()?;
        // The authenticated-session registry fences by account. Keep this end-to-end test on a
        // distinct account so concurrently running socket tests cannot deliberately supersede it.
        let binding = GateGatewayBinding::new(GatewayBinding::new(
            VenueId::Gate,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000099",
            rules.instrument.symbol.clone(),
        )?)?;
        let (http_endpoint, http_server) = recovery_http_mock([
            r#"{"position_mode":"dual","total":"10","available":"9"}"#.to_owned(),
            r#"[{"user":42,"contract":"DOGE_USDT","mode":"dual_long","size":"0","entry_price":"0","mark_price":"0"},{"user":42,"contract":"DOGE_USDT","mode":"dual_short","size":"2","entry_price":"0.1","mark_price":"0.11"}]"#.to_owned(),
            include_str!("../tests/fixtures/regular_orders.json").to_owned(),
            include_str!("../tests/fixtures/fills.json").to_owned(),
        ])
        .await?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let websocket_server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut ws = accept_async(stream).await?;
            let mut subscriptions = Vec::new();
            for _ in 0..4 {
                let message = ws.next().await.ok_or("closed")??;
                let Message::Text(text) = message else {
                    return Err::<(), TestError>("expected subscription".into());
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
            for channel in subscriptions {
                ws.send(Message::Text(
                    json!({"channel":channel,"event":"subscribe","result":{"status":"success"}})
                        .to_string()
                        .into(),
                ))
                .await?;
            }
            for _ in 0..8 {
                let message = ws.next().await.ok_or("missing recovery ping")??;
                let Message::Ping(nonce) = message else {
                    return Err::<(), TestError>("recovery liveness must use binary ping".into());
                };
                ws.send(Message::Pong(nonce)).await?;
            }
            Ok::<(), TestError>(())
        });
        let limits = GateTransportLimits::new(Duration::from_secs(2), 16 * 1024)?;
        let stream = TcpStream::connect(address).await?;
        let (client, _) = client_async(format!("ws://{address}"), stream).await?;
        let mut private = authenticate_private_stream(
            client,
            format!("ws://{address}"),
            &binding,
            &credentials,
            &rules,
            "42",
            7,
            limits,
            Some(http_endpoint.clone()),
        )
        .await?;
        let runtime_scope =
            crate::GateRuntimeRecoveryScope::verified(crate::GateRuntimeRecoveryScopeInput {
                mode: GatewayMode::Live,
                trading_account_id: binding.gateway_binding().trading_account_id.clone(),
                config_digest: "gate_runtime_test".to_owned(),
                config_epoch: 3,
                connection_generation: 71,
                recovered_private_generation: 0,
                position_mode: crate::GateRuntimePositionMode::Hedge,
                order_profile: crate::GateRuntimeOrderProfile::stage7_regular_only(),
                recovery_session_sha256: [7; 32],
                authority_roots: crate::GateRecoveryAuthorityRoots::verified(
                    [1; 32], [2; 32], [3; 32],
                )?,
                registrations: vec![crate::GateRuntimeRecoveryRegistration::verified(
                    rules.instrument.symbol.clone(),
                    "grid",
                    "grid_doge",
                    "gate_runtime_test",
                    3,
                )?],
                owner_routes: Vec::new(),
                structured_unknowns: Vec::new(),
            })?;
        let revalidator = StaticRuntimeScope(*runtime_scope.commitment_sha256());
        let session = private.begin_runtime_recovery_session(
            runtime_scope,
            [GateRecoverySymbolScope::verified(
                binding.clone(),
                rules.clone(),
                crate::GateFillsCursor::default(),
            )?],
            unix_ms()?.saturating_add(2_000),
            64 * 1024,
            4,
        )?;
        let transport = GateHttpTransport::with_endpoint(&binding, 7, http_endpoint, limits)?;
        let bundle = crate::GateFreshRecoveryCollector::collect_runtime_authenticated(
            session,
            &transport,
            &mut private,
            &credentials,
            &revalidator,
        )
        .await?;
        let candidate = bundle.candidate();
        assert_eq!(candidate.scope().connection_generation(), 71);
        assert_eq!(candidate.symbol_readbacks().len(), 1);
        assert_eq!(candidate.unknown_open_orders().len(), 2);
        assert!(matches!(
            candidate
                .surface(crate::GateRecoverySurface::ConditionalOrders)
                .ok_or("conditional coverage")?
                .coverage(),
            crate::GateRecoveryCoverage::Unsupported { .. }
        ));
        let requests = http_server.await??;
        assert_eq!(requests.len(), 4);
        for expected in [
            "GET /futures/usdt/accounts HTTP/1.1",
            "GET /futures/usdt/dual_comp/positions/DOGE_USDT?holding=false HTTP/1.1",
            "GET /futures/usdt/orders?contract=DOGE_USDT&limit=100&status=open HTTP/1.1",
            "GET /futures/usdt/my_trades?contract=DOGE_USDT&limit=100 HTTP/1.1",
        ] {
            assert!(
                requests
                    .iter()
                    .any(|request| String::from_utf8_lossy(request).starts_with(expected)),
                "missing {expected}; requests={:?}",
                requests
                    .iter()
                    .map(|request| String::from_utf8_lossy(request).into_owned())
                    .collect::<Vec<_>>()
            );
        }
        websocket_server.await??;
        Ok(())
    }
}
