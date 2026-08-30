//! Authenticated UTA v3 private WebSocket with ACK-gated raw delivery.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time::{Instant, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Error as WebSocketError, Message, protocol::WebSocketConfig},
};
use venue_domain::domain::Symbol;
use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

use crate::{
    BitgetAccountBinding, BitgetConfig, BitgetCredentials, BitgetTransportError,
    BitgetTransportLimits, transport::unix_ms, ws_sign,
};

const PRIVATE_TOPICS: [&str; 3] = ["account", "position", "order"];
const MAX_PRE_LIVE_FRAMES: usize = 256;
const MAX_PRE_LIVE_BYTES: usize = 1024 * 1024;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
const MAX_RECOVERY_SYMBOLS: usize = 256;
const MAX_RECOVERY_WINDOW_MS: u64 = 60_000;
const MAX_RECOVERY_TOTAL_BYTES: usize = 64 * 1024 * 1024;
const MAX_RECOVERY_TOTAL_PAGES: u32 = 10_000;

static NEXT_CONNECTION_GENERATION: AtomicU64 = AtomicU64::new(1);
static NEXT_RECOVERY_ATTEMPT: AtomicU64 = AtomicU64::new(1);
static NEXT_CONTROL_NONCE: AtomicU64 = AtomicU64::new(1);
static ACTIVE_PRIVATE_SESSIONS: OnceLock<Mutex<BTreeMap<String, Weak<PrivateSessionSeal>>>> =
    OnceLock::new();

#[derive(Debug)]
struct PrivateSessionSeal {
    active: AtomicBool,
    collection_epoch: AtomicU64,
    credential_identity: [u8; 32],
}

impl PrivateSessionSeal {
    fn new(credential_identity: [u8; 32]) -> Self {
        Self {
            active: AtomicBool::new(true),
            collection_epoch: AtomicU64::new(0),
            credential_identity,
        }
    }

    fn revoke(&self) {
        self.active.store(false, Ordering::Release);
    }
}

/// One immutable per-symbol recovery request specification. All five signed Bitget surfaces plus
/// the current execution-profile unsupported families are implicit and committed by the session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetRecoverySymbolRequest {
    symbol: Symbol,
    requested_fill_start_ms: Option<u64>,
    fills_target_through_ms: u64,
}

impl BitgetRecoverySymbolRequest {
    pub fn verified(
        symbol: Symbol,
        requested_fill_start_ms: Option<u64>,
        fills_target_through_ms: u64,
    ) -> Result<Self, BitgetTransportError> {
        if symbol.quote() != "USDT"
            || fills_target_through_ms == 0
            || requested_fill_start_ms.is_some_and(|start| start > fills_target_through_ms)
        {
            return Err(BitgetTransportError::RecoverySession);
        }
        Ok(Self {
            symbol,
            requested_fill_start_ms,
            fills_target_through_ms,
        })
    }

    #[must_use]
    pub const fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    #[must_use]
    pub const fn requested_fill_start_ms(&self) -> Option<u64> {
        self.requested_fill_start_ms
    }

    #[must_use]
    pub const fn fills_target_through_ms(&self) -> u64 {
        self.fills_target_through_ms
    }
}

#[derive(Debug)]
struct RecoveryCollectionState {
    remaining_symbols: BTreeSet<Symbol>,
    in_flight_symbol: Option<Symbol>,
    committed: bool,
    used_pages: u32,
    used_bytes: usize,
}

/// Opaque proof that this adapter completed the exact private WS login and subscription handshake.
///
/// The value deliberately has no `Clone`, `Serialize`, or `Deserialize` implementation. It is a
/// read-only transport session, not a runtime recovery authority, capability, writer, WAL handle,
/// or mutation permit.
pub struct BitgetAuthenticatedRecoverySession {
    seal: Arc<PrivateSessionSeal>,
    collection_epoch: u64,
    mode: GatewayMode,
    trading_account_id: String,
    rest_origin: &'static str,
    private_ws_endpoint: String,
    symbols: BTreeSet<Symbol>,
    requests: BTreeMap<Symbol, BitgetRecoverySymbolRequest>,
    connection_generation: u64,
    private_generation: u64,
    attempt_id: u64,
    started_at_ms: u64,
    deadline_at_ms: u64,
    maximum_total_bytes: usize,
    maximum_total_pages: u32,
    transport_limits: BitgetTransportLimits,
    request_universe_sha256: [u8; 32],
    collection: Mutex<RecoveryCollectionState>,
}

impl fmt::Debug for BitgetAuthenticatedRecoverySession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BitgetAuthenticatedRecoverySession")
            .field("mode", &self.mode)
            .field("trading_account_id", &self.trading_account_id)
            .field("rest_origin", &self.rest_origin)
            .field("private_ws_endpoint", &self.private_ws_endpoint)
            .field("symbols", &self.symbols)
            .field("connection_generation", &self.connection_generation)
            .field("private_generation", &self.private_generation)
            .field("attempt_id", &self.attempt_id)
            .field("started_at_ms", &self.started_at_ms)
            .field("deadline_at_ms", &self.deadline_at_ms)
            .field("maximum_total_bytes", &self.maximum_total_bytes)
            .field("maximum_total_pages", &self.maximum_total_pages)
            .finish_non_exhaustive()
    }
}

impl BitgetAuthenticatedRecoverySession {
    #[must_use]
    pub const fn mode(&self) -> GatewayMode {
        self.mode
    }

    #[must_use]
    pub fn trading_account_id(&self) -> &str {
        &self.trading_account_id
    }

    #[must_use]
    pub const fn rest_origin(&self) -> &str {
        self.rest_origin
    }

    #[must_use]
    pub fn private_ws_endpoint(&self) -> &str {
        &self.private_ws_endpoint
    }

    #[must_use]
    pub const fn symbols(&self) -> &BTreeSet<Symbol> {
        &self.symbols
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
    pub const fn attempt_id(&self) -> u64 {
        self.attempt_id
    }

    #[must_use]
    pub const fn started_at_ms(&self) -> u64 {
        self.started_at_ms
    }

    #[must_use]
    pub const fn deadline_at_ms(&self) -> u64 {
        self.deadline_at_ms
    }

    #[must_use]
    pub const fn request_universe_sha256(&self) -> &[u8; 32] {
        &self.request_universe_sha256
    }

    #[must_use]
    pub fn is_committed(&self) -> bool {
        self.collection
            .lock()
            .map(|state| state.committed)
            .unwrap_or(false)
    }

    pub(crate) const fn transport_limits(&self) -> BitgetTransportLimits {
        self.transport_limits
    }

    #[cfg(test)]
    pub(crate) fn with_rest_origin_for_test(
        mut self,
        rest_origin: &'static str,
    ) -> Result<Self, BitgetTransportError> {
        if rest_origin.is_empty() {
            return Err(BitgetTransportError::RecoverySession);
        }
        self.rest_origin = rest_origin;
        self.request_universe_sha256 = recovery_request_universe_commitment(
            self.mode,
            &self.trading_account_id,
            self.rest_origin,
            &self.private_ws_endpoint,
            self.connection_generation,
            self.private_generation,
            self.attempt_id,
            self.started_at_ms,
            self.deadline_at_ms,
            self.maximum_total_bytes,
            self.maximum_total_pages,
            &self.seal.credential_identity,
            &self.requests,
        );
        Ok(self)
    }

    pub(crate) fn validate_credentials(
        &self,
        credentials: &BitgetCredentials,
    ) -> Result<(), BitgetTransportError> {
        if self.seal.credential_identity != credentials.identity_commitment() {
            return Err(BitgetTransportError::RecoverySession);
        }
        Ok(())
    }

    pub(crate) fn begin_symbol(
        &self,
        symbol: &Symbol,
    ) -> Result<BitgetRecoverySymbolRequest, BitgetTransportError> {
        let mut state = self
            .collection
            .lock()
            .map_err(|_| BitgetTransportError::RecoverySession)?;
        if state.committed
            || state.in_flight_symbol.is_some()
            || !state.remaining_symbols.contains(symbol)
        {
            return Err(BitgetTransportError::RecoverySession);
        }
        state.in_flight_symbol = Some(symbol.clone());
        self.requests
            .get(symbol)
            .cloned()
            .ok_or(BitgetTransportError::RecoverySession)
    }

    pub(crate) fn commit_symbol(&self, symbol: &Symbol) -> Result<(), BitgetTransportError> {
        let mut state = self
            .collection
            .lock()
            .map_err(|_| BitgetTransportError::RecoverySession)?;
        if state.in_flight_symbol.as_ref() != Some(symbol)
            || !state.remaining_symbols.remove(symbol)
        {
            return Err(BitgetTransportError::RecoverySession);
        }
        state.in_flight_symbol = None;
        state.committed = state.remaining_symbols.is_empty();
        Ok(())
    }

    pub(crate) fn reserve_get(
        &self,
        symbol: &Symbol,
        maximum_response_bytes: usize,
    ) -> Result<(), BitgetTransportError> {
        let mut state = self
            .collection
            .lock()
            .map_err(|_| BitgetTransportError::RecoverySession)?;
        let next_pages = state
            .used_pages
            .checked_add(1)
            .ok_or(BitgetTransportError::Pages)?;
        let next_bytes = state
            .used_bytes
            .checked_add(maximum_response_bytes)
            .ok_or(BitgetTransportError::BodyTooLarge)?;
        if state.committed
            || state.in_flight_symbol.as_ref() != Some(symbol)
            || next_pages > self.maximum_total_pages
            || next_bytes > self.maximum_total_bytes
        {
            return Err(BitgetTransportError::RecoverySession);
        }
        state.used_pages = next_pages;
        state.used_bytes = next_bytes;
        Ok(())
    }

    pub(crate) fn settle_get(
        &self,
        maximum_response_bytes: usize,
        actual_response_bytes: usize,
    ) -> Result<(), BitgetTransportError> {
        if actual_response_bytes > maximum_response_bytes {
            return Err(BitgetTransportError::BodyTooLarge);
        }
        let mut state = self
            .collection
            .lock()
            .map_err(|_| BitgetTransportError::RecoverySession)?;
        state.used_bytes = state
            .used_bytes
            .checked_sub(maximum_response_bytes - actual_response_bytes)
            .ok_or(BitgetTransportError::RecoverySession)?;
        Ok(())
    }

    pub(crate) fn validate(
        &self,
        binding: &GatewayBinding,
        now_ms: u64,
    ) -> Result<(), BitgetTransportError> {
        if !self.seal.active.load(Ordering::Acquire)
            || self.collection_epoch == 0
            || self.collection_epoch != self.seal.collection_epoch.load(Ordering::Acquire)
            || binding.venue != VenueId::Bitget
            || binding.mode != self.mode
            || binding.trading_account_id != self.trading_account_id
            || !self.symbols.contains(&binding.symbol)
            || self.connection_generation == 0
            || self.private_generation == 0
            || self.attempt_id == 0
            || now_ms < self.started_at_ms
            || now_ms >= self.deadline_at_ms
        {
            return Err(BitgetTransportError::RecoverySession);
        }
        Ok(())
    }

    pub(crate) fn revoke(&self) {
        self.seal.revoke();
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct BitgetRawPrivateFrame {
    pub binding: GatewayBinding,
    pub generation: u64,
    pub topic: String,
    pub received_at_ms: u64,
    pub payload: Bytes,
}

impl fmt::Debug for BitgetRawPrivateFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BitgetRawPrivateFrame")
            .field("binding", &self.binding)
            .field("generation", &self.generation)
            .field("topic", &self.topic)
            .field("received_at_ms", &self.received_at_ms)
            .field("payload_bytes", &self.payload.len())
            .finish()
    }
}

pub struct BitgetPrivateWsTransport<S = MaybeTlsStream<TcpStream>> {
    stream: WebSocketStream<S>,
    binding: GatewayBinding,
    generation: u64,
    endpoint: String,
    connection_id: String,
    limits: BitgetTransportLimits,
    pre_live_frames: VecDeque<BitgetRawPrivateFrame>,
    buffered_bytes: usize,
    next_heartbeat_at: Instant,
    session_seal: Arc<PrivateSessionSeal>,
    recovery_generation_issued: bool,
}

impl<S> BitgetPrivateWsTransport<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[must_use]
    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }

    /// Freezes one bounded account recovery universe on this authenticated connection. A later
    /// collection on the same connection invalidates the earlier handle.
    pub fn begin_recovery_session<I>(
        &self,
        requests: I,
        deadline_at_ms: u64,
        maximum_total_bytes: usize,
        maximum_total_pages: u32,
    ) -> Result<BitgetAuthenticatedRecoverySession, BitgetTransportError>
    where
        I: IntoIterator<Item = BitgetRecoverySymbolRequest>,
    {
        let supplied_requests = requests.into_iter().collect::<Vec<_>>();
        let mut requests = BTreeMap::new();
        for request in supplied_requests {
            if requests.insert(request.symbol.clone(), request).is_some() {
                return Err(BitgetTransportError::RecoverySession);
            }
        }
        let symbols = requests.keys().cloned().collect::<BTreeSet<_>>();
        let started_at_ms = unix_ms()?;
        if !self.recovery_generation_issued
            || !self.session_seal.active.load(Ordering::Acquire)
            || symbols.is_empty()
            || symbols.len() > MAX_RECOVERY_SYMBOLS
            || !symbols.contains(&self.binding.symbol)
            || symbols.iter().any(|symbol| symbol.quote() != "USDT")
            || requests
                .values()
                .any(|request| request.fills_target_through_ms > started_at_ms)
            || deadline_at_ms <= started_at_ms
            || deadline_at_ms.saturating_sub(started_at_ms) > MAX_RECOVERY_WINDOW_MS
            || maximum_total_bytes < self.limits.maximum_body_bytes()
            || maximum_total_bytes > MAX_RECOVERY_TOTAL_BYTES
            || maximum_total_pages == 0
            || maximum_total_pages > MAX_RECOVERY_TOTAL_PAGES
        {
            return Err(BitgetTransportError::RecoverySession);
        }
        for symbol in &symbols {
            let binding = GatewayBinding::new(
                VenueId::Bitget,
                self.binding.mode,
                self.binding.trading_account_id.clone(),
                symbol.clone(),
            )
            .map_err(|_| BitgetTransportError::RecoverySession)?;
            BitgetAccountBinding::UtaUsdtFuturesHedge
                .validate_gateway_binding(&binding)
                .map_err(|_| BitgetTransportError::RecoverySession)?;
        }
        let collection_epoch = next_epoch(&self.session_seal.collection_epoch)?;
        let private_generation = next_serial(&NEXT_RECOVERY_ATTEMPT)?;
        // Existing Bitget private candidates define the authenticated turn attempt as the private
        // generation; keeping one adapter-issued value prevents cross-attempt face splicing.
        let attempt_id = private_generation;
        let config = BitgetConfig::for_mode(self.binding.mode);
        let request_universe_sha256 = recovery_request_universe_commitment(
            self.binding.mode,
            &self.binding.trading_account_id,
            config.rest_origin(),
            &self.endpoint,
            self.generation,
            private_generation,
            attempt_id,
            started_at_ms,
            deadline_at_ms,
            maximum_total_bytes,
            maximum_total_pages,
            &self.session_seal.credential_identity,
            &requests,
        );
        Ok(BitgetAuthenticatedRecoverySession {
            seal: Arc::clone(&self.session_seal),
            collection_epoch,
            mode: self.binding.mode,
            trading_account_id: self.binding.trading_account_id.clone(),
            rest_origin: config.rest_origin(),
            private_ws_endpoint: self.endpoint.clone(),
            symbols: symbols.clone(),
            requests,
            connection_generation: self.generation,
            private_generation,
            attempt_id,
            started_at_ms,
            deadline_at_ms,
            maximum_total_bytes,
            maximum_total_pages,
            transport_limits: self.limits,
            request_universe_sha256,
            collection: Mutex::new(RecoveryCollectionState {
                remaining_symbols: symbols,
                in_flight_symbol: None,
                committed: false,
                used_pages: 0,
                used_bytes: 0,
            }),
        })
    }

    /// Performs a private-socket round trip and rechecks the same seal/generation. Private data
    /// received while waiting for `pong` is retained in the normal bounded delivery queue.
    pub async fn revalidate_recovery_session(
        &mut self,
        session: &BitgetAuthenticatedRecoverySession,
    ) -> Result<(), BitgetTransportError> {
        self.validate_recovery_session_identity(session, unix_ms()?)?;
        let control_nonce = recovery_control_nonce(session)?;
        let result = async {
            send_message(
                &mut self.stream,
                Message::Ping(Bytes::copy_from_slice(&control_nonce)),
                self.limits.operation_timeout(),
            )
            .await?;
            loop {
                let message = timeout(self.limits.operation_timeout(), self.stream.next())
                    .await
                    .map_err(|_| BitgetTransportError::Timeout)?
                    .ok_or(BitgetTransportError::Disconnected)?
                    .map_err(map_websocket)?;
                match message {
                    Message::Text(value) if value.as_str() == "pong" => continue,
                    Message::Text(value) if value.as_str() == "ping" => {
                        send_message(
                            &mut self.stream,
                            Message::Text("pong".into()),
                            self.limits.operation_timeout(),
                        )
                        .await?;
                    }
                    Message::Text(value) => {
                        self.buffer_recovery_frame(Bytes::copy_from_slice(value.as_bytes()))?
                    }
                    Message::Ping(value) => {
                        send_message(
                            &mut self.stream,
                            Message::Pong(value),
                            self.limits.operation_timeout(),
                        )
                        .await?;
                    }
                    Message::Pong(value)
                        if recovery_control_pong_matches(&value, &control_nonce) =>
                    {
                        break;
                    }
                    Message::Pong(_) => continue,
                    Message::Binary(_) | Message::Frame(_) => {
                        return Err(BitgetTransportError::Protocol);
                    }
                    Message::Close(_) => return Err(BitgetTransportError::Disconnected),
                }
            }
            self.validate_recovery_session_identity(session, unix_ms()?)
        }
        .await;
        if result.is_err() {
            self.session_seal.revoke();
        }
        result
    }

    fn validate_recovery_session_identity(
        &self,
        session: &BitgetAuthenticatedRecoverySession,
        now_ms: u64,
    ) -> Result<(), BitgetTransportError> {
        if !Arc::ptr_eq(&self.session_seal, &session.seal)
            || self.generation != session.connection_generation
            || self.endpoint != session.private_ws_endpoint
        {
            return Err(BitgetTransportError::RecoverySession);
        }
        session.validate(&self.binding, now_ms)
    }

    fn buffer_recovery_frame(&mut self, payload: Bytes) -> Result<(), BitgetTransportError> {
        let frame = make_raw_frame(
            &self.binding,
            self.generation,
            payload,
            self.limits.maximum_body_bytes(),
            unix_ms()?,
        )?;
        self.buffered_bytes = self
            .buffered_bytes
            .checked_add(frame.payload.len())
            .ok_or(BitgetTransportError::BodyTooLarge)?;
        if self.pre_live_frames.len() >= MAX_PRE_LIVE_FRAMES
            || self.buffered_bytes > MAX_PRE_LIVE_BYTES
        {
            return Err(BitgetTransportError::BodyTooLarge);
        }
        self.pre_live_frames.push_back(frame);
        Ok(())
    }

    pub async fn next_frame(&mut self) -> Result<BitgetRawPrivateFrame, BitgetTransportError> {
        if let Some(frame) = self.pre_live_frames.pop_front() {
            self.buffered_bytes = self.buffered_bytes.saturating_sub(frame.payload.len());
            return Ok(frame);
        }
        if Instant::now() >= self.next_heartbeat_at {
            send_message(
                &mut self.stream,
                Message::Text("ping".into()),
                self.limits.operation_timeout(),
            )
            .await?;
            self.next_heartbeat_at = Instant::now() + HEARTBEAT_INTERVAL;
        }
        let operation = async {
            loop {
                let message = self
                    .stream
                    .next()
                    .await
                    .ok_or(BitgetTransportError::Disconnected)?
                    .map_err(map_websocket)?;
                match message {
                    Message::Text(value) if value.as_str() == "pong" => continue,
                    Message::Text(value) if value.as_str() == "ping" => {
                        self.stream
                            .send(Message::Text("pong".into()))
                            .await
                            .map_err(map_websocket)?;
                    }
                    Message::Text(value) => {
                        return make_raw_frame(
                            &self.binding,
                            self.generation,
                            Bytes::copy_from_slice(value.as_bytes()),
                            self.limits.maximum_body_bytes(),
                            unix_ms()?,
                        );
                    }
                    Message::Ping(value) => {
                        self.stream
                            .send(Message::Pong(value))
                            .await
                            .map_err(map_websocket)?;
                    }
                    Message::Pong(_) => {}
                    Message::Binary(_) | Message::Frame(_) => {
                        return Err(BitgetTransportError::Protocol);
                    }
                    Message::Close(_) => return Err(BitgetTransportError::Disconnected),
                }
            }
        };
        let result = match timeout(self.limits.operation_timeout(), operation).await {
            Ok(result) => result,
            Err(_) => {
                self.session_seal.revoke();
                return Err(BitgetTransportError::Timeout);
            }
        };
        if result.is_err() {
            self.session_seal.revoke();
        }
        result
    }
}

pub async fn connect_private_ws(
    binding: GatewayBinding,
    credentials: &BitgetCredentials,
    generation: u64,
    now_ms: u64,
    limits: BitgetTransportLimits,
) -> Result<BitgetPrivateWsTransport, BitgetTransportError> {
    let config = BitgetConfig::for_mode(binding.mode);
    validate_binding(&binding, &config, generation)?;
    let endpoint = config.private_ws();
    let websocket = WebSocketConfig::default()
        .max_message_size(Some(limits.maximum_body_bytes()))
        .max_frame_size(Some(limits.maximum_body_bytes()));
    let (stream, _) = timeout(
        limits.operation_timeout(),
        connect_async_with_config(endpoint, Some(websocket), true),
    )
    .await
    .map_err(|_| BitgetTransportError::Timeout)?
    .map_err(map_websocket)?;
    authenticate_private_stream(
        stream,
        endpoint.to_owned(),
        binding,
        config,
        credentials,
        generation,
        now_ms,
        limits,
        false,
    )
    .await
}

/// Connects an authenticated private stream whose recovery connection generation is issued inside
/// the adapter after the caller has no opportunity to choose or relabel it.
pub async fn connect_authenticated_private_ws(
    binding: GatewayBinding,
    credentials: &BitgetCredentials,
    now_ms: u64,
    limits: BitgetTransportLimits,
) -> Result<BitgetPrivateWsTransport, BitgetTransportError> {
    let config = BitgetConfig::for_mode(binding.mode);
    let generation = next_serial(&NEXT_CONNECTION_GENERATION)?;
    validate_binding(&binding, &config, generation)?;
    let endpoint = config.private_ws();
    let websocket = WebSocketConfig::default()
        .max_message_size(Some(limits.maximum_body_bytes()))
        .max_frame_size(Some(limits.maximum_body_bytes()));
    let (stream, _) = timeout(
        limits.operation_timeout(),
        connect_async_with_config(endpoint, Some(websocket), true),
    )
    .await
    .map_err(|_| BitgetTransportError::Timeout)?
    .map_err(map_websocket)?;
    authenticate_private_stream(
        stream,
        endpoint.to_owned(),
        binding,
        config,
        credentials,
        generation,
        now_ms,
        limits,
        true,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn authenticate_private_stream<S>(
    mut stream: WebSocketStream<S>,
    endpoint: String,
    binding: GatewayBinding,
    config: BitgetConfig,
    credentials: &BitgetCredentials,
    generation: u64,
    now_ms: u64,
    limits: BitgetTransportLimits,
    recovery_generation_issued: bool,
) -> Result<BitgetPrivateWsTransport<S>, BitgetTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    validate_binding(&binding, &config, generation)?;
    if now_ms < 1_000 || endpoint.is_empty() {
        return Err(BitgetTransportError::Clock);
    }
    let timestamp_s = now_ms / 1_000;
    let signature = ws_sign(credentials, timestamp_s).map_err(|_| BitgetTransportError::Signing)?;
    let login = LoginFrame {
        operation: "login",
        args: [LoginArg {
            api_key: credentials.api_key.expose_secret(),
            passphrase: credentials.passphrase.expose_secret(),
            timestamp: timestamp_s.to_string(),
            sign: &signature,
        }],
    };
    let login_payload = SecretString::from(
        serde_json::to_string(&login).map_err(|_| BitgetTransportError::Protocol)?,
    );
    send_secret(&mut stream, &login_payload, limits.operation_timeout()).await?;
    let login_ack = read_json(&mut stream, limits).await?;
    validate_login_ack(&login_ack)?;

    let subscribe = SubscribeFrame {
        operation: "subscribe",
        args: PRIVATE_TOPICS.map(|topic| SubscriptionArg {
            instrument_type: "UTA",
            topic,
        }),
    };
    let payload = serde_json::to_string(&subscribe).map_err(|_| BitgetTransportError::Protocol)?;
    send_message(
        &mut stream,
        Message::Text(payload.into()),
        limits.operation_timeout(),
    )
    .await?;
    let (connection_id, pre_live_frames, buffered_bytes) =
        read_subscription_acks(&mut stream, &binding, generation, limits).await?;
    let session_seal = register_private_session(&binding, credentials)?;
    Ok(BitgetPrivateWsTransport {
        stream,
        binding,
        generation,
        endpoint,
        connection_id,
        limits,
        pre_live_frames,
        buffered_bytes,
        next_heartbeat_at: Instant::now() + HEARTBEAT_INTERVAL,
        session_seal,
        recovery_generation_issued,
    })
}

#[cfg(test)]
pub(crate) async fn authenticate_private_stream_for_test<S>(
    stream: WebSocketStream<S>,
    endpoint: String,
    binding: GatewayBinding,
    config: BitgetConfig,
    credentials: &BitgetCredentials,
    now_ms: u64,
    limits: BitgetTransportLimits,
) -> Result<BitgetPrivateWsTransport<S>, BitgetTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let generation = next_serial(&NEXT_CONNECTION_GENERATION)?;
    authenticate_private_stream(
        stream,
        endpoint,
        binding,
        config,
        credentials,
        generation,
        now_ms,
        limits,
        true,
    )
    .await
}

impl<S> Drop for BitgetPrivateWsTransport<S> {
    fn drop(&mut self) {
        self.session_seal.revoke();
    }
}

fn next_serial(counter: &AtomicU64) -> Result<u64, BitgetTransportError> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(1)
        })
        .map_err(|_| BitgetTransportError::RecoverySession)
}

fn next_epoch(counter: &AtomicU64) -> Result<u64, BitgetTransportError> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(1)
        })
        .map_err(|_| BitgetTransportError::RecoverySession)?
        .checked_add(1)
        .ok_or(BitgetTransportError::RecoverySession)
}

fn recovery_control_nonce(
    session: &BitgetAuthenticatedRecoverySession,
) -> Result<[u8; 32], BitgetTransportError> {
    let serial = next_serial(&NEXT_CONTROL_NONCE)?;
    let mut digest = Sha256::new();
    digest.update(b"venue-bitget-recovery-control-ping-v1");
    digest.update(session.request_universe_sha256);
    digest.update(session.connection_generation.to_be_bytes());
    digest.update(session.collection_epoch.to_be_bytes());
    digest.update(serial.to_be_bytes());
    Ok(digest.finalize().into())
}

fn recovery_control_pong_matches(payload: &Bytes, expected: &[u8; 32]) -> bool {
    payload.as_ref() == expected.as_slice()
}

#[allow(clippy::too_many_arguments)]
fn recovery_request_universe_commitment(
    mode: GatewayMode,
    trading_account_id: &str,
    rest_origin: &str,
    private_ws_endpoint: &str,
    connection_generation: u64,
    private_generation: u64,
    attempt_id: u64,
    started_at_ms: u64,
    deadline_at_ms: u64,
    maximum_total_bytes: usize,
    maximum_total_pages: u32,
    credential_identity: &[u8; 32],
    requests: &BTreeMap<Symbol, BitgetRecoverySymbolRequest>,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"venue-bitget-authenticated-request-universe-v1");
    digest.update([match mode {
        GatewayMode::Test => 1,
        GatewayMode::Live => 2,
    }]);
    for value in [trading_account_id, rest_origin, private_ws_endpoint] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    for value in [
        connection_generation,
        private_generation,
        attempt_id,
        started_at_ms,
        deadline_at_ms,
        maximum_total_bytes as u64,
        u64::from(maximum_total_pages),
    ] {
        digest.update(value.to_be_bytes());
    }
    digest.update(credential_identity);
    // Account, Settings, Positions, regular UmOrder, profile-bound unsupported conditional/Algo,
    // and terminal FillsCursor are the immutable recovery faces for every symbol.
    digest.update([1, 2, 3, 4, 5, 6, 7]);
    for request in requests.values() {
        let symbol = request.symbol.to_string();
        digest.update((symbol.len() as u64).to_be_bytes());
        digest.update(symbol.as_bytes());
        match request.requested_fill_start_ms {
            Some(start_ms) => {
                digest.update([1]);
                digest.update(start_ms.to_be_bytes());
            }
            None => digest.update([0]),
        }
        digest.update(request.fills_target_through_ms.to_be_bytes());
    }
    digest.finalize().into()
}

fn register_private_session(
    binding: &GatewayBinding,
    credentials: &BitgetCredentials,
) -> Result<Arc<PrivateSessionSeal>, BitgetTransportError> {
    let seal = Arc::new(PrivateSessionSeal::new(credentials.identity_commitment()));
    let key = format!("{}:{}", binding.mode.as_str(), binding.trading_account_id);
    let registry = ACTIVE_PRIVATE_SESSIONS.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut sessions = registry
        .lock()
        .map_err(|_| BitgetTransportError::RecoverySession)?;
    if let Some(previous) = sessions.insert(key, Arc::downgrade(&seal))
        && let Some(previous) = previous.upgrade()
    {
        previous.revoke();
    }
    Ok(seal)
}

async fn read_subscription_acks<S>(
    stream: &mut WebSocketStream<S>,
    binding: &GatewayBinding,
    generation: u64,
    limits: BitgetTransportLimits,
) -> Result<(String, VecDeque<BitgetRawPrivateFrame>, usize), BitgetTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let operation = async {
        let mut pending = PRIVATE_TOPICS.into_iter().collect::<Vec<_>>();
        let mut connection_id = None;
        let mut frames = VecDeque::new();
        let mut buffered_bytes = 0_usize;
        while !pending.is_empty() {
            let message = stream
                .next()
                .await
                .ok_or(BitgetTransportError::Disconnected)?
                .map_err(map_websocket)?;
            let payload = match message {
                Message::Text(value) if value.as_str() == "ping" => {
                    stream
                        .send(Message::Text("pong".into()))
                        .await
                        .map_err(map_websocket)?;
                    continue;
                }
                Message::Text(value) => Bytes::copy_from_slice(value.as_bytes()),
                Message::Ping(value) => {
                    stream
                        .send(Message::Pong(value))
                        .await
                        .map_err(map_websocket)?;
                    continue;
                }
                Message::Pong(_) => continue,
                Message::Binary(_) | Message::Frame(_) => {
                    return Err(BitgetTransportError::Protocol);
                }
                Message::Close(_) => return Err(BitgetTransportError::Disconnected),
            };
            if payload.len() > limits.maximum_body_bytes() {
                return Err(BitgetTransportError::BodyTooLarge);
            }
            let value: Value =
                serde_json::from_slice(&payload).map_err(|_| BitgetTransportError::Protocol)?;
            if value.get("arg").is_some() && value.get("data").is_some() {
                let frame = make_raw_frame(
                    binding,
                    generation,
                    payload,
                    limits.maximum_body_bytes(),
                    unix_ms()?,
                )?;
                buffered_bytes = buffered_bytes
                    .checked_add(frame.payload.len())
                    .ok_or(BitgetTransportError::BodyTooLarge)?;
                if frames.len() >= MAX_PRE_LIVE_FRAMES || buffered_bytes > MAX_PRE_LIVE_BYTES {
                    return Err(BitgetTransportError::BodyTooLarge);
                }
                frames.push_back(frame);
                continue;
            }
            let ack: EventAck =
                serde_json::from_value(value).map_err(|_| BitgetTransportError::Protocol)?;
            let topic = validate_subscription_ack(&ack)?;
            let Some(index) = pending.iter().position(|candidate| candidate == &topic) else {
                return Err(BitgetTransportError::Protocol);
            };
            pending.swap_remove(index);
            if let Some(actual) = ack.connection_id {
                if actual.is_empty()
                    || connection_id
                        .as_ref()
                        .is_some_and(|expected| expected != &actual)
                {
                    return Err(BitgetTransportError::Protocol);
                }
                connection_id = Some(actual);
            }
        }
        Ok((
            connection_id.ok_or(BitgetTransportError::Protocol)?,
            frames,
            buffered_bytes,
        ))
    };
    timeout(limits.operation_timeout(), operation)
        .await
        .map_err(|_| BitgetTransportError::Timeout)?
}

fn make_raw_frame(
    binding: &GatewayBinding,
    generation: u64,
    payload: Bytes,
    maximum_body_bytes: usize,
    received_at_ms: u64,
) -> Result<BitgetRawPrivateFrame, BitgetTransportError> {
    if generation == 0
        || received_at_ms == 0
        || payload.is_empty()
        || payload.len() > maximum_body_bytes
    {
        return Err(BitgetTransportError::Protocol);
    }
    let value: Value =
        serde_json::from_slice(&payload).map_err(|_| BitgetTransportError::Protocol)?;
    let argument = value
        .get("arg")
        .and_then(Value::as_object)
        .ok_or(BitgetTransportError::Protocol)?;
    if argument.get("instType").and_then(Value::as_str) != Some("UTA")
        || !value.get("data").is_some_and(Value::is_array)
    {
        return Err(BitgetTransportError::Protocol);
    }
    let topic = argument
        .get("topic")
        .and_then(Value::as_str)
        .ok_or(BitgetTransportError::Protocol)?;
    if !PRIVATE_TOPICS.contains(&topic) {
        return Err(BitgetTransportError::Protocol);
    }
    Ok(BitgetRawPrivateFrame {
        binding: binding.clone(),
        generation,
        topic: topic.to_owned(),
        received_at_ms,
        payload,
    })
}

async fn read_json<S>(
    stream: &mut WebSocketStream<S>,
    limits: BitgetTransportLimits,
) -> Result<Value, BitgetTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let operation = async {
        loop {
            let message = stream
                .next()
                .await
                .ok_or(BitgetTransportError::Disconnected)?
                .map_err(map_websocket)?;
            match message {
                Message::Text(value) => {
                    if value.len() > limits.maximum_body_bytes() {
                        return Err(BitgetTransportError::BodyTooLarge);
                    }
                    return serde_json::from_str(&value)
                        .map_err(|_| BitgetTransportError::Protocol);
                }
                Message::Ping(value) => {
                    stream
                        .send(Message::Pong(value))
                        .await
                        .map_err(map_websocket)?;
                }
                Message::Pong(_) => {}
                Message::Binary(_) | Message::Frame(_) => {
                    return Err(BitgetTransportError::Protocol);
                }
                Message::Close(_) => return Err(BitgetTransportError::Disconnected),
            }
        }
    };
    timeout(limits.operation_timeout(), operation)
        .await
        .map_err(|_| BitgetTransportError::Timeout)?
}

fn validate_login_ack(value: &Value) -> Result<(), BitgetTransportError> {
    let object = value.as_object().ok_or(BitgetTransportError::Protocol)?;
    if object.get("event").and_then(Value::as_str) != Some("login")
        || object.get("code").and_then(Value::as_str) != Some("0")
        || object
            .get("msg")
            .and_then(Value::as_str)
            .is_some_and(|message| !message.is_empty())
    {
        return Err(BitgetTransportError::Protocol);
    }
    Ok(())
}

fn validate_subscription_ack(ack: &EventAck) -> Result<String, BitgetTransportError> {
    if ack.event != "subscribe"
        || ack
            .code
            .as_deref()
            .is_some_and(|code| code != "0" && code != "00000")
        || ack
            .message
            .as_deref()
            .is_some_and(|message| !message.is_empty())
        || ack.argument.instrument_type != "UTA"
        || !PRIVATE_TOPICS.contains(&ack.argument.topic.as_str())
    {
        return Err(BitgetTransportError::Protocol);
    }
    Ok(ack.argument.topic.clone())
}

fn validate_binding(
    binding: &GatewayBinding,
    config: &BitgetConfig,
    generation: u64,
) -> Result<(), BitgetTransportError> {
    BitgetAccountBinding::UtaUsdtFuturesHedge
        .validate_gateway_binding(binding)
        .map_err(|_| BitgetTransportError::Binding)?;
    if binding.mode != config.mode() || generation == 0 {
        return Err(BitgetTransportError::Binding);
    }
    Ok(())
}

async fn send_secret<S>(
    stream: &mut WebSocketStream<S>,
    payload: &SecretString,
    operation_timeout: Duration,
) -> Result<(), BitgetTransportError>
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
) -> Result<(), BitgetTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    timeout(operation_timeout, stream.send(message))
        .await
        .map_err(|_| BitgetTransportError::Timeout)?
        .map_err(map_websocket)
}

fn map_websocket(error: WebSocketError) -> BitgetTransportError {
    if matches!(error, WebSocketError::Capacity(_)) {
        BitgetTransportError::BodyTooLarge
    } else {
        BitgetTransportError::Disconnected
    }
}

#[derive(Serialize)]
struct LoginFrame<'a> {
    #[serde(rename = "op")]
    operation: &'static str,
    args: [LoginArg<'a>; 1],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LoginArg<'a> {
    api_key: &'a str,
    passphrase: &'a str,
    timestamp: String,
    sign: &'a str,
}

#[derive(Serialize)]
struct SubscribeFrame {
    #[serde(rename = "op")]
    operation: &'static str,
    args: [SubscriptionArg; 3],
}

#[derive(Clone, Copy, Serialize)]
struct SubscriptionArg {
    #[serde(rename = "instType")]
    instrument_type: &'static str,
    topic: &'static str,
}

#[derive(Deserialize)]
struct EventAck {
    event: String,
    code: Option<String>,
    #[serde(rename = "msg")]
    message: Option<String>,
    #[serde(rename = "arg")]
    argument: EventArgument,
    #[serde(rename = "connId")]
    connection_id: Option<String>,
}

#[derive(Deserialize)]
struct EventArgument {
    #[serde(rename = "instType")]
    instrument_type: String,
    topic: String,
}

#[cfg(test)]
mod tests {
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_async, tungstenite::Message};
    use venue_gateway_api::{GatewayMode, VenueId};

    use super::*;

    fn binding(
        mode: GatewayMode,
    ) -> Result<GatewayBinding, Box<dyn std::error::Error + Send + Sync>> {
        binding_for_account(mode, "00000000-0000-4000-8000-000000000001")
    }

    fn binding_for_account(
        mode: GatewayMode,
        account: &str,
    ) -> Result<GatewayBinding, Box<dyn std::error::Error + Send + Sync>> {
        Ok(GatewayBinding::new(
            VenueId::Bitget,
            mode,
            account,
            "BTC/USDT".parse()?,
        )?)
    }

    fn limits() -> Result<BitgetTransportLimits, BitgetTransportError> {
        BitgetTransportLimits::new(Duration::from_secs(1), 64 * 1024)
    }

    fn recovery_request(
        symbol: Symbol,
        fills_target_through_ms: u64,
    ) -> Result<BitgetRecoverySymbolRequest, BitgetTransportError> {
        BitgetRecoverySymbolRequest::verified(symbol, Some(1), fills_target_through_ms)
    }

    #[tokio::test]
    async fn private_delivery_waits_for_all_three_exact_subscription_acks()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await?;
            let mut websocket = accept_async(socket).await?;
            let login = websocket.next().await.ok_or("missing login")??;
            assert!(login.into_text()?.contains("\"op\":\"login\""));
            websocket
                .send(Message::Text(
                    r#"{"event":"login","code":"0","msg":""}"#.into(),
                ))
                .await?;
            let subscribe = websocket.next().await.ok_or("missing subscribe")??;
            assert!(subscribe.into_text()?.contains("\"topic\":\"order\""));
            websocket
                .send(Message::Text(
                    r#"{"action":"snapshot","arg":{"instType":"UTA","topic":"account"},"data":[]}"#
                        .into(),
                ))
                .await?;
            for topic in ["account", "position", "order"] {
                websocket
                    .send(Message::Text(
                        format!(
                            r#"{{"event":"subscribe","arg":{{"instType":"UTA","topic":"{topic}"}},"connId":"connection-1"}}"#
                        )
                        .into(),
                    ))
                    .await?;
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
        let binding = binding(GatewayMode::Test)?;
        let config = BitgetConfig::for_mode(GatewayMode::Test);
        let (stream, _) = tokio_tungstenite::connect_async(&endpoint).await?;
        let mut transport = authenticate_private_stream(
            stream,
            endpoint,
            binding.clone(),
            config,
            &BitgetCredentials::from_values("key", "secret", "pass")?,
            7,
            1_700_000_000_000,
            limits()?,
            false,
        )
        .await?;
        assert_eq!(transport.connection_id(), "connection-1");
        let frame = transport.next_frame().await?;
        assert_eq!(frame.binding, binding);
        assert_eq!(frame.generation, 7);
        assert_eq!(frame.topic, "account");
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_or_wrong_topic_ack_fails_closed()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await?;
            let mut websocket = accept_async(socket).await?;
            let _ = websocket.next().await;
            websocket
                .send(Message::Text(
                    r#"{"event":"login","code":"0","msg":""}"#.into(),
                ))
                .await?;
            let _ = websocket.next().await;
            for _ in 0..2 {
                websocket
                    .send(Message::Text(
                        r#"{"event":"subscribe","arg":{"instType":"UTA","topic":"account"},"connId":"c"}"#.into(),
                    ))
                    .await?;
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
        let binding = binding(GatewayMode::Live)?;
        let config = BitgetConfig::for_mode(GatewayMode::Live);
        let (stream, _) = tokio_tungstenite::connect_async(&endpoint).await?;
        assert!(
            authenticate_private_stream(
                stream,
                endpoint,
                binding,
                config,
                &BitgetCredentials::from_values("key", "secret", "pass")?,
                7,
                1_700_000_000_000,
                limits()?,
                false,
            )
            .await
            .is_err()
        );
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn adapter_issued_recovery_session_round_trips_and_buffers_private_data()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await?;
            let mut websocket = accept_async(socket).await?;
            let _ = websocket.next().await.ok_or("missing login")??;
            websocket
                .send(Message::Text(
                    r#"{"event":"login","code":"0","msg":""}"#.into(),
                ))
                .await?;
            let _ = websocket.next().await.ok_or("missing subscribe")??;
            for topic in ["account", "position", "order"] {
                websocket
                    .send(Message::Text(
                        format!(
                            r#"{{"event":"subscribe","arg":{{"instType":"UTA","topic":"{topic}"}},"connId":"recovery-1"}}"#
                        )
                        .into(),
                    ))
                    .await?;
            }
            let ping = websocket.next().await.ok_or("missing recovery ping")??;
            let Message::Ping(control_nonce) = ping else {
                return Err("recovery control must use a nonce ping".into());
            };
            websocket
                .send(Message::Pong(Bytes::from_static(b"stale-pong")))
                .await?;
            websocket.send(Message::Text("pong".into())).await?;
            websocket
                .send(Message::Text(
                    r#"{"action":"update","arg":{"instType":"UTA","topic":"order"},"data":[]}"#
                        .into(),
                ))
                .await?;
            websocket.send(Message::Pong(control_nonce)).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
        let binding =
            binding_for_account(GatewayMode::Test, "00000000-0000-4000-8000-000000000011")?;
        let config = BitgetConfig::for_mode(GatewayMode::Test);
        let (stream, _) = tokio_tungstenite::connect_async(&endpoint).await?;
        let generation = next_serial(&NEXT_CONNECTION_GENERATION)?;
        let mut transport = authenticate_private_stream(
            stream,
            endpoint,
            binding.clone(),
            config,
            &BitgetCredentials::from_values("key", "secret", "pass")?,
            generation,
            1_700_000_000_000,
            limits()?,
            true,
        )
        .await?;
        let started_at_ms = unix_ms()?;
        let session = transport.begin_recovery_session(
            [recovery_request(binding.symbol.clone(), started_at_ms)?],
            started_at_ms + 30_000,
            8 * 1024 * 1024,
            100,
        )?;
        assert_eq!(session.connection_generation(), generation);
        assert_eq!(session.mode(), GatewayMode::Test);
        assert_eq!(session.symbols().len(), 1);
        assert_eq!(session.private_generation(), session.attempt_id());
        assert!(
            session
                .validate_credentials(&BitgetCredentials::from_values("other", "secret", "pass")?)
                .is_err()
        );
        transport.revalidate_recovery_session(&session).await?;
        let frame = transport.next_frame().await?;
        assert_eq!(frame.topic, "order");
        let replacement = transport.begin_recovery_session(
            [recovery_request(binding.symbol.clone(), started_at_ms)?],
            started_at_ms + 30_000,
            64 * 1024,
            1,
        )?;
        assert_eq!(
            session.validate(&binding, unix_ms()?),
            Err(BitgetTransportError::RecoverySession)
        );
        replacement.validate(&binding, unix_ms()?)?;
        assert!(matches!(
            transport.begin_recovery_session(
                [
                    recovery_request(binding.symbol.clone(), started_at_ms)?,
                    recovery_request(binding.symbol.clone(), started_at_ms)?,
                ],
                started_at_ms + 30_000,
                8 * 1024 * 1024,
                100,
            ),
            Err(BitgetTransportError::RecoverySession)
        ));
        let request = replacement.begin_symbol(&binding.symbol)?;
        assert_eq!(request.symbol(), &binding.symbol);
        replacement.reserve_get(&binding.symbol, 64 * 1024)?;
        assert!(replacement.reserve_get(&binding.symbol, 64 * 1024).is_err());
        replacement.settle_get(64 * 1024, 128)?;
        replacement.commit_symbol(&binding.symbol)?;
        assert!(replacement.is_committed());
        assert!(replacement.begin_symbol(&binding.symbol).is_err());
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn caller_generation_transport_cannot_issue_recovery_session()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await?;
            let mut websocket = accept_async(socket).await?;
            let _ = websocket.next().await;
            websocket
                .send(Message::Text(
                    r#"{"event":"login","code":"0","msg":""}"#.into(),
                ))
                .await?;
            let _ = websocket.next().await;
            for topic in ["account", "position", "order"] {
                websocket
                    .send(Message::Text(
                        format!(
                            r#"{{"event":"subscribe","arg":{{"instType":"UTA","topic":"{topic}"}},"connId":"legacy"}}"#
                        )
                        .into(),
                    ))
                    .await?;
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
        let binding =
            binding_for_account(GatewayMode::Live, "00000000-0000-4000-8000-000000000012")?;
        let config = BitgetConfig::for_mode(GatewayMode::Live);
        let (stream, _) = tokio_tungstenite::connect_async(&endpoint).await?;
        let transport = authenticate_private_stream(
            stream,
            endpoint,
            binding.clone(),
            config,
            &BitgetCredentials::from_values("key", "secret", "pass")?,
            999,
            1_700_000_000_000,
            limits()?,
            false,
        )
        .await?;
        let started_at_ms = unix_ms()?;
        assert!(matches!(
            transport.begin_recovery_session(
                [recovery_request(binding.symbol.clone(), started_at_ms)?],
                started_at_ms + 30_000,
                8 * 1024 * 1024,
                100,
            ),
            Err(BitgetTransportError::RecoverySession)
        ));
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn post_await_disconnect_revokes_the_authenticated_session()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await?;
            let mut websocket = accept_async(socket).await?;
            let _ = websocket.next().await;
            websocket
                .send(Message::Text(
                    r#"{"event":"login","code":"0","msg":""}"#.into(),
                ))
                .await?;
            let _ = websocket.next().await;
            for topic in ["account", "position", "order"] {
                websocket
                    .send(Message::Text(
                        format!(
                            r#"{{"event":"subscribe","arg":{{"instType":"UTA","topic":"{topic}"}},"connId":"disconnect"}}"#
                        )
                        .into(),
                    ))
                    .await?;
            }
            websocket.close(None).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
        let binding =
            binding_for_account(GatewayMode::Test, "00000000-0000-4000-8000-000000000013")?;
        let config = BitgetConfig::for_mode(GatewayMode::Test);
        let (stream, _) = tokio_tungstenite::connect_async(&endpoint).await?;
        let generation = next_serial(&NEXT_CONNECTION_GENERATION)?;
        let mut transport = authenticate_private_stream(
            stream,
            endpoint,
            binding.clone(),
            config,
            &BitgetCredentials::from_values("key", "secret", "pass")?,
            generation,
            1_700_000_000_000,
            limits()?,
            true,
        )
        .await?;
        let started_at_ms = unix_ms()?;
        let session = transport.begin_recovery_session(
            [recovery_request(binding.symbol.clone(), started_at_ms)?],
            started_at_ms + 30_000,
            8 * 1024 * 1024,
            100,
        )?;
        assert!(
            transport
                .revalidate_recovery_session(&session)
                .await
                .is_err()
        );
        assert_eq!(
            session.validate(&binding, unix_ms()?),
            Err(BitgetTransportError::RecoverySession)
        );
        server.await??;
        Ok(())
    }

    #[test]
    fn demo_and_live_private_endpoints_never_alias() {
        let demo = BitgetConfig::for_mode(GatewayMode::Test);
        let live = BitgetConfig::for_mode(GatewayMode::Live);
        assert!(demo.private_ws().contains("wspap.bitget.com"));
        assert!(!live.private_ws().contains("wspap"));
        assert_ne!(demo.private_ws(), live.private_ws());
    }

    #[test]
    fn recovery_control_pong_requires_the_exact_nonce() {
        let expected = [7_u8; 32];
        let mut other = expected;
        other[0] ^= 1;
        assert!(recovery_control_pong_matches(
            &Bytes::copy_from_slice(&expected),
            &expected
        ));
        assert!(!recovery_control_pong_matches(
            &Bytes::copy_from_slice(&other),
            &expected
        ));
        assert!(!recovery_control_pong_matches(
            &Bytes::from_static(b"stale-pong"),
            &expected
        ));
    }

    #[test]
    fn successful_relogin_registry_revokes_the_previous_account_seal()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let binding =
            binding_for_account(GatewayMode::Live, "00000000-0000-4000-8000-000000000014")?;
        let credentials = BitgetCredentials::from_values("key", "secret", "pass")?;
        let previous = register_private_session(&binding, &credentials)?;
        assert!(previous.active.load(Ordering::Acquire));
        let replacement = register_private_session(&binding, &credentials)?;
        assert!(!previous.active.load(Ordering::Acquire));
        assert!(replacement.active.load(Ordering::Acquire));
        Ok(())
    }
}
