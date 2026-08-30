//! Bounded async signed REST transport. Mutation requests are consumed exactly once.

use std::{
    collections::BTreeSet,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::BytesMut;
use venue_gateway_api::GatewayBinding;

use crate::{
    BitgetAccountBinding, BitgetAuthenticatedRecoverySession, BitgetConfig, BitgetCredentials,
    BitgetPrivateWsTransport, SignInput, endpoints,
    execution::{
        BitgetExactOrderReadback, BitgetExactReadbackRequest, BitgetExecutionError,
        BitgetMutationOutcome, BitgetPreparedMutation, BitgetUnknownReason, into_unknown,
        parse_exact_order_readback, parse_mutation_ack, sign_prepared_mutation,
    },
    private::{
        BITGET_MAX_FILL_HISTORY_WINDOW_MS, BITGET_MAX_PRIVATE_PAGES, BITGET_UTA_FUTURES_CATEGORY,
        BitgetPrivateFace, BitgetPrivateGenerationCandidate, BitgetPrivateSurface,
        BitgetRawPrivatePage, complete_private_turn, fill_history_query, parse_account_face,
        parse_fill_page, parse_positions_face, parse_regular_order_page, parse_settings_face,
        regular_orders_query,
    },
    sign,
};

const MAX_HTTP_BODY_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BitgetTransportLimits {
    operation_timeout: Duration,
    maximum_body_bytes: usize,
}

impl BitgetTransportLimits {
    pub fn new(
        operation_timeout: Duration,
        maximum_body_bytes: usize,
    ) -> Result<Self, BitgetTransportError> {
        if operation_timeout.is_zero()
            || operation_timeout > Duration::from_secs(60)
            || maximum_body_bytes == 0
            || maximum_body_bytes > MAX_HTTP_BODY_BYTES
        {
            return Err(BitgetTransportError::Limits);
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
pub struct BitgetPrivateReadRequest {
    pub binding: GatewayBinding,
    pub surface: BitgetPrivateSurface,
    pub attempt_id: u64,
    pub generation: u64,
    pub page_index: u32,
    pub request_cursor: Option<String>,
    pub fill_history_start_ms: Option<u64>,
    pub(crate) path: &'static str,
    pub(crate) query: String,
}

impl BitgetPrivateReadRequest {
    fn validate(&self, config: &BitgetConfig) -> Result<(), BitgetTransportError> {
        validate_binding(&self.binding, config)?;
        let expected_path = match self.surface {
            BitgetPrivateSurface::Account => endpoints::BALANCES,
            BitgetPrivateSurface::Settings => endpoints::ACCOUNT_SETTINGS,
            BitgetPrivateSurface::Positions => endpoints::POSITIONS,
            BitgetPrivateSurface::RegularOrders => endpoints::OPEN_ORDERS,
            BitgetPrivateSurface::Fills => endpoints::FILLS,
        };
        if self.path != expected_path
            || self.attempt_id == 0
            || self.generation == 0
            || self
                .request_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.is_empty())
            || (self.surface != BitgetPrivateSurface::Fills && self.fill_history_start_ms.is_some())
        {
            return Err(BitgetTransportError::Binding);
        }
        Ok(())
    }
}

pub fn build_account_read_request(
    binding: &GatewayBinding,
    attempt_id: u64,
    generation: u64,
) -> BitgetPrivateReadRequest {
    singleton_request(
        binding,
        BitgetPrivateSurface::Account,
        endpoints::BALANCES,
        attempt_id,
        generation,
        String::new(),
    )
}

pub fn build_settings_read_request(
    binding: &GatewayBinding,
    attempt_id: u64,
    generation: u64,
) -> BitgetPrivateReadRequest {
    singleton_request(
        binding,
        BitgetPrivateSurface::Settings,
        endpoints::ACCOUNT_SETTINGS,
        attempt_id,
        generation,
        String::new(),
    )
}

pub fn build_positions_read_request(
    binding: &GatewayBinding,
    attempt_id: u64,
    generation: u64,
) -> Result<BitgetPrivateReadRequest, BitgetTransportError> {
    let native =
        crate::public::native_symbol(&binding.symbol).map_err(|_| BitgetTransportError::Binding)?;
    Ok(singleton_request(
        binding,
        BitgetPrivateSurface::Positions,
        endpoints::POSITIONS,
        attempt_id,
        generation,
        format!("category={BITGET_UTA_FUTURES_CATEGORY}&symbol={native}"),
    ))
}

pub fn build_regular_orders_read_request(
    binding: &GatewayBinding,
    attempt_id: u64,
    generation: u64,
    page_index: u32,
    cursor: Option<&str>,
) -> Result<BitgetPrivateReadRequest, BitgetTransportError> {
    Ok(BitgetPrivateReadRequest {
        binding: binding.clone(),
        surface: BitgetPrivateSurface::RegularOrders,
        attempt_id,
        generation,
        page_index,
        request_cursor: cursor.map(str::to_owned),
        fill_history_start_ms: None,
        path: endpoints::OPEN_ORDERS,
        query: regular_orders_query(&binding.symbol, cursor)
            .map_err(|_| BitgetTransportError::Binding)?,
    })
}

#[allow(clippy::too_many_arguments)]
pub fn build_fills_read_request(
    binding: &GatewayBinding,
    attempt_id: u64,
    generation: u64,
    page_index: u32,
    cursor: Option<&str>,
    effective_start_ms: Option<u64>,
    server_now_ms: u64,
) -> Result<BitgetPrivateReadRequest, BitgetTransportError> {
    Ok(BitgetPrivateReadRequest {
        binding: binding.clone(),
        surface: BitgetPrivateSurface::Fills,
        attempt_id,
        generation,
        page_index,
        request_cursor: cursor.map(str::to_owned),
        fill_history_start_ms: effective_start_ms,
        path: endpoints::FILLS,
        query: fill_history_query(effective_start_ms, cursor, server_now_ms)
            .map_err(|_| BitgetTransportError::Clock)?,
    })
}

fn singleton_request(
    binding: &GatewayBinding,
    surface: BitgetPrivateSurface,
    path: &'static str,
    attempt_id: u64,
    generation: u64,
    query: String,
) -> BitgetPrivateReadRequest {
    BitgetPrivateReadRequest {
        binding: binding.clone(),
        surface,
        attempt_id,
        generation,
        page_index: 0,
        request_cursor: None,
        fill_history_start_ms: None,
        path,
        query,
    }
}

pub struct BitgetHttpTransport {
    client: reqwest::Client,
    binding: GatewayBinding,
    config: BitgetConfig,
    generation: u64,
    endpoint: String,
    limits: BitgetTransportLimits,
    dispatched: tokio::sync::Mutex<BTreeSet<(u64, u64, String)>>,
}

struct RecoverySessionUse<'a> {
    session: &'a BitgetAuthenticatedRecoverySession,
    completed: bool,
}

impl<'a> RecoverySessionUse<'a> {
    fn new(session: &'a BitgetAuthenticatedRecoverySession) -> Self {
        Self {
            session,
            completed: false,
        }
    }

    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for RecoverySessionUse<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.session.revoke();
        }
    }
}

impl BitgetHttpTransport {
    pub fn new(
        binding: GatewayBinding,
        generation: u64,
        limits: BitgetTransportLimits,
    ) -> Result<Self, BitgetTransportError> {
        let config = BitgetConfig::for_mode(binding.mode);
        let endpoint = config.rest_origin().to_owned();
        Self::with_endpoint(binding, generation, config, endpoint, limits)
    }

    fn with_endpoint(
        binding: GatewayBinding,
        generation: u64,
        config: BitgetConfig,
        endpoint: String,
        limits: BitgetTransportLimits,
    ) -> Result<Self, BitgetTransportError> {
        validate_binding(&binding, &config)?;
        if generation == 0 || endpoint.is_empty() {
            return Err(BitgetTransportError::Binding);
        }
        let client = reqwest::Client::builder()
            .connect_timeout(limits.operation_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .map_err(|_| BitgetTransportError::Http)?;
        Ok(Self {
            client,
            binding,
            config,
            generation,
            endpoint,
            limits,
            dispatched: tokio::sync::Mutex::new(BTreeSet::new()),
        })
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn config(&self) -> &BitgetConfig {
        &self.config
    }

    pub async fn execute_private_read(
        &self,
        credentials: &BitgetCredentials,
        request: &BitgetPrivateReadRequest,
        timestamp_ms: u64,
    ) -> Result<BitgetRawPrivatePage, BitgetTransportError> {
        request.validate(&self.config)?;
        self.validate_scope(&request.binding, request.generation)?;
        let headers = sign(
            credentials,
            &self.config,
            &SignInput {
                timestamp_ms,
                method: "GET",
                request_path: request.path,
                query: &request.query,
                body: &[],
            },
        )
        .map_err(|_| BitgetTransportError::Signing)?;
        let requested_at_ms = unix_ms()?;
        let body = tokio::time::timeout(
            self.limits.operation_timeout,
            self.send("GET", request.path, &request.query, &[], &headers),
        )
        .await
        .map_err(|_| BitgetTransportError::Timeout)??;
        let received_at_ms = unix_ms()?.max(requested_at_ms);
        BitgetRawPrivatePage::new_with_generation(
            request.surface,
            request.binding.clone(),
            request.attempt_id,
            request.generation,
            request.page_index,
            request.request_cursor.clone(),
            request.fill_history_start_ms,
            received_at_ms,
            String::from_utf8(body).map_err(|_| BitgetTransportError::Protocol)?,
        )
        .map_err(|_| BitgetTransportError::Protocol)
    }

    /// Collects all five signed surfaces. Any failed face discards the local turn candidate.
    pub async fn collect_private_turn(
        &self,
        credentials: &BitgetCredentials,
        attempt_id: u64,
        generation: u64,
        requested_fill_start_ms: Option<u64>,
        server_now_ms: u64,
    ) -> Result<BitgetPrivateGenerationCandidate, BitgetTransportError> {
        self.validate_scope(&self.binding, generation)?;
        if attempt_id == 0 || server_now_ms == 0 {
            return Err(BitgetTransportError::Binding);
        }
        let account = self
            .execute_private_read(
                credentials,
                &build_account_read_request(&self.binding, attempt_id, generation),
                unix_ms()?,
            )
            .await?;
        let settings = self
            .execute_private_read(
                credentials,
                &build_settings_read_request(&self.binding, attempt_id, generation),
                unix_ms()?,
            )
            .await?;
        let positions = self
            .execute_private_read(
                credentials,
                &build_positions_read_request(&self.binding, attempt_id, generation)?,
                unix_ms()?,
            )
            .await?;

        let mut order_pages = Vec::new();
        let mut order_cursor = None;
        for page_index in 0..BITGET_MAX_PRIVATE_PAGES {
            let page_index = u32::try_from(page_index).map_err(|_| BitgetTransportError::Pages)?;
            let request = build_regular_orders_read_request(
                &self.binding,
                attempt_id,
                generation,
                page_index,
                order_cursor.as_deref(),
            )?;
            let page = parse_regular_order_page(
                self.execute_private_read(credentials, &request, unix_ms()?)
                    .await?,
            )
            .map_err(|_| BitgetTransportError::Protocol)?;
            order_cursor = page.next_cursor.clone();
            order_pages.push(page);
            if order_cursor.is_none() {
                break;
            }
        }
        if order_cursor.is_some() {
            return Err(BitgetTransportError::Pages);
        }

        let effective_start_ms = requested_fill_start_ms.map(|start| {
            start.max(server_now_ms.saturating_sub(BITGET_MAX_FILL_HISTORY_WINDOW_MS))
        });
        let mut fill_pages = Vec::new();
        let mut fill_cursor = None;
        for page_index in 0..BITGET_MAX_PRIVATE_PAGES {
            let page_index = u32::try_from(page_index).map_err(|_| BitgetTransportError::Pages)?;
            let request = build_fills_read_request(
                &self.binding,
                attempt_id,
                generation,
                page_index,
                fill_cursor.as_deref(),
                effective_start_ms,
                server_now_ms,
            )?;
            let page = parse_fill_page(
                self.execute_private_read(credentials, &request, unix_ms()?)
                    .await?,
            )
            .map_err(|_| BitgetTransportError::Protocol)?;
            fill_cursor = page.next_cursor.clone();
            fill_pages.push(page);
            if fill_cursor.is_none() {
                break;
            }
        }
        if fill_cursor.is_some() {
            return Err(BitgetTransportError::Pages);
        }
        complete_private_turn(vec![
            BitgetPrivateFace::Account(
                parse_account_face(account).map_err(|_| BitgetTransportError::Protocol)?,
            ),
            BitgetPrivateFace::Settings(
                parse_settings_face(settings).map_err(|_| BitgetTransportError::Protocol)?,
            ),
            BitgetPrivateFace::Positions(
                parse_positions_face(positions).map_err(|_| BitgetTransportError::Protocol)?,
            ),
            BitgetPrivateFace::RegularOrders(order_pages),
            BitgetPrivateFace::Fills(fill_pages),
        ])
        .map_err(|_| BitgetTransportError::Protocol)
    }

    /// Dispatches one consumed mutation. Timeout, disconnect, HTTP ambiguity, or malformed ACK is
    /// returned as UNKNOWN and this API provides no retry surface for the consumed request.
    pub async fn execute_mutation_once(
        &self,
        credentials: &BitgetCredentials,
        request: BitgetPreparedMutation,
        timestamp_ms: u64,
    ) -> Result<BitgetMutationOutcome, BitgetTransportError> {
        request
            .validate(&self.config)
            .map_err(|_| BitgetTransportError::Binding)?;
        self.validate_scope(&request.binding, request.generation)?;
        let headers = sign_prepared_mutation(credentials, &self.config, &request, timestamp_ms)
            .map_err(|_| BitgetTransportError::Signing)?;
        let dispatch_key = mutation_dispatch_key(&request)?;
        if !self.dispatched.lock().await.insert(dispatch_key) {
            return Err(BitgetTransportError::AlreadyDispatched);
        }
        let dispatched_at_ms = unix_ms()?;
        let sent = tokio::time::timeout(
            self.limits.operation_timeout,
            self.send("POST", request.path, "", &request.body, &headers),
        )
        .await;
        let body = match sent {
            Err(_) => {
                return Ok(BitgetMutationOutcome::Unknown(into_unknown(
                    request,
                    dispatched_at_ms,
                    BitgetUnknownReason::Timeout,
                )));
            }
            Ok(Err(BitgetTransportError::Disconnected | BitgetTransportError::Timeout)) => {
                return Ok(BitgetMutationOutcome::Unknown(into_unknown(
                    request,
                    dispatched_at_ms,
                    BitgetUnknownReason::Disconnected,
                )));
            }
            Ok(Err(_)) => {
                return Ok(BitgetMutationOutcome::Unknown(into_unknown(
                    request,
                    dispatched_at_ms,
                    BitgetUnknownReason::AmbiguousResponse,
                )));
            }
            Ok(Ok(body)) => body,
        };
        let received_at_ms = unix_ms()?.max(dispatched_at_ms);
        match parse_mutation_ack(&self.config, &request, &body, received_at_ms) {
            Ok(ack) => Ok(BitgetMutationOutcome::Acknowledged(ack)),
            Err(BitgetExecutionError::VenueRejected) => Ok(BitgetMutationOutcome::Rejected),
            Err(_) => Ok(BitgetMutationOutcome::Unknown(into_unknown(
                request,
                dispatched_at_ms,
                BitgetUnknownReason::AmbiguousResponse,
            ))),
        }
    }

    pub async fn execute_exact_readback(
        &self,
        credentials: &BitgetCredentials,
        request: BitgetExactReadbackRequest,
        timestamp_ms: u64,
    ) -> Result<BitgetExactOrderReadback, BitgetTransportError> {
        self.validate_scope(&request.binding, request.generation)?;
        let headers = sign(
            credentials,
            &self.config,
            &SignInput {
                timestamp_ms,
                method: "GET",
                request_path: endpoints::ORDER_DETAIL,
                query: &request.query,
                body: &[],
            },
        )
        .map_err(|_| BitgetTransportError::Signing)?;
        let requested_at_ms = unix_ms()?;
        if requested_at_ms < request.not_before_ms {
            return Err(BitgetTransportError::Clock);
        }
        let body = tokio::time::timeout(
            self.limits.operation_timeout,
            self.send(
                "GET",
                endpoints::ORDER_DETAIL,
                &request.query,
                &[],
                &headers,
            ),
        )
        .await
        .map_err(|_| BitgetTransportError::Timeout)??;
        let received_at_ms = unix_ms()?.max(requested_at_ms);
        parse_exact_order_readback(&self.config, request, requested_at_ms, received_at_ms, body)
            .map_err(|_| BitgetTransportError::Protocol)
    }

    /// Executes one recovery GET only under an adapter-issued authenticated private session. The
    /// private socket is round-tripped after the HTTP await; a disconnect, replacement login,
    /// generation drift, deadline expiry, endpoint mismatch, or universe drift discards the page.
    pub(crate) async fn execute_authenticated_recovery_read<S>(
        &self,
        private_ws: &mut BitgetPrivateWsTransport<S>,
        session: &BitgetAuthenticatedRecoverySession,
        credentials: &BitgetCredentials,
        request: &BitgetPrivateReadRequest,
    ) -> Result<BitgetRawPrivatePage, BitgetTransportError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let mut session_use = RecoverySessionUse::new(session);
        let now_ms = unix_ms()?;
        session.validate_credentials(credentials)?;
        self.validate_authenticated_recovery_scope(session, request, now_ms)?;
        let maximum_response_bytes = self.limits.maximum_body_bytes();
        session.reserve_get(&request.binding.symbol, maximum_response_bytes)?;
        let page = self
            .execute_private_read(credentials, request, now_ms)
            .await?;
        private_ws.revalidate_recovery_session(session).await?;
        self.validate_authenticated_recovery_scope(session, request, unix_ms()?)?;
        if page.received_at_ms < session.started_at_ms()
            || page.received_at_ms >= session.deadline_at_ms()
        {
            return Err(BitgetTransportError::RecoverySession);
        }
        session.settle_get(maximum_response_bytes, page.payload.len())?;
        session_use.complete();
        Ok(page)
    }

    /// Collects the signed Bitget account/settings/positions/regular-orders/fills turn for this
    /// symbol. All pages use the session-issued attempt and connection generation. Unsupported
    /// conditional/Algo families remain the existing execution-profile evidence and are not
    /// synthesized from HTTP 404 or an empty regular page.
    pub async fn collect_authenticated_private_turn<S>(
        &self,
        private_ws: &mut BitgetPrivateWsTransport<S>,
        session: &BitgetAuthenticatedRecoverySession,
        credentials: &BitgetCredentials,
    ) -> Result<BitgetPrivateGenerationCandidate, BitgetTransportError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let mut session_use = RecoverySessionUse::new(session);
        session.validate_credentials(credentials)?;
        let request_spec = session.begin_symbol(&self.binding.symbol)?;
        let attempt_id = session.attempt_id();
        let generation = session.connection_generation();
        let probe = build_account_read_request(&self.binding, attempt_id, generation);
        self.validate_authenticated_recovery_scope(session, &probe, unix_ms()?)?;
        let requested_fill_start_ms = request_spec.requested_fill_start_ms();
        let server_now_ms = request_spec.fills_target_through_ms();
        let account = self
            .execute_authenticated_recovery_read(private_ws, session, credentials, &probe)
            .await?;
        let settings_request = build_settings_read_request(&self.binding, attempt_id, generation);
        let settings = self
            .execute_authenticated_recovery_read(
                private_ws,
                session,
                credentials,
                &settings_request,
            )
            .await?;
        let positions_request =
            build_positions_read_request(&self.binding, attempt_id, generation)?;
        let positions = self
            .execute_authenticated_recovery_read(
                private_ws,
                session,
                credentials,
                &positions_request,
            )
            .await?;

        let mut order_pages = Vec::new();
        let mut order_cursor = None;
        for page_index in 0..BITGET_MAX_PRIVATE_PAGES {
            let page_index = u32::try_from(page_index).map_err(|_| BitgetTransportError::Pages)?;
            let request = build_regular_orders_read_request(
                &self.binding,
                attempt_id,
                generation,
                page_index,
                order_cursor.as_deref(),
            )?;
            let page = parse_regular_order_page(
                self.execute_authenticated_recovery_read(
                    private_ws,
                    session,
                    credentials,
                    &request,
                )
                .await?,
            )
            .map_err(|_| BitgetTransportError::Protocol)?;
            order_cursor = page.next_cursor.clone();
            order_pages.push(page);
            if order_cursor.is_none() {
                break;
            }
        }
        if order_cursor.is_some() {
            return Err(BitgetTransportError::Pages);
        }

        let effective_start_ms = requested_fill_start_ms.map(|start| {
            start.max(server_now_ms.saturating_sub(BITGET_MAX_FILL_HISTORY_WINDOW_MS))
        });
        let mut fill_pages = Vec::new();
        let mut fill_cursor = None;
        for page_index in 0..BITGET_MAX_PRIVATE_PAGES {
            let page_index = u32::try_from(page_index).map_err(|_| BitgetTransportError::Pages)?;
            let request = build_fills_read_request(
                &self.binding,
                attempt_id,
                generation,
                page_index,
                fill_cursor.as_deref(),
                effective_start_ms,
                server_now_ms,
            )?;
            let page = parse_fill_page(
                self.execute_authenticated_recovery_read(
                    private_ws,
                    session,
                    credentials,
                    &request,
                )
                .await?,
            )
            .map_err(|_| BitgetTransportError::Protocol)?;
            fill_cursor = page.next_cursor.clone();
            fill_pages.push(page);
            if fill_cursor.is_none() {
                break;
            }
        }
        if fill_cursor.is_some() {
            return Err(BitgetTransportError::Pages);
        }
        let candidate = complete_private_turn(vec![
            BitgetPrivateFace::Account(
                parse_account_face(account).map_err(|_| BitgetTransportError::Protocol)?,
            ),
            BitgetPrivateFace::Settings(
                parse_settings_face(settings).map_err(|_| BitgetTransportError::Protocol)?,
            ),
            BitgetPrivateFace::Positions(
                parse_positions_face(positions).map_err(|_| BitgetTransportError::Protocol)?,
            ),
            BitgetPrivateFace::RegularOrders(order_pages),
            BitgetPrivateFace::Fills(fill_pages),
        ])
        .map_err(|_| BitgetTransportError::Protocol)?;
        session.commit_symbol(&self.binding.symbol)?;
        session_use.complete();
        Ok(candidate)
    }

    fn validate_scope(
        &self,
        binding: &GatewayBinding,
        generation: u64,
    ) -> Result<(), BitgetTransportError> {
        if binding != &self.binding || generation != self.generation {
            return Err(BitgetTransportError::Binding);
        }
        validate_binding(binding, &self.config)
    }

    fn validate_authenticated_recovery_scope(
        &self,
        session: &BitgetAuthenticatedRecoverySession,
        request: &BitgetPrivateReadRequest,
        now_ms: u64,
    ) -> Result<(), BitgetTransportError> {
        session.validate(&request.binding, now_ms)?;
        if self.endpoint != session.rest_origin()
            || self.config.mode() != session.mode()
            || self.binding.trading_account_id != session.trading_account_id()
            || self.generation != session.connection_generation()
            || self.limits != session.transport_limits()
            || request.attempt_id != session.attempt_id()
            || request.generation != session.connection_generation()
        {
            return Err(BitgetTransportError::RecoverySession);
        }
        Ok(())
    }

    async fn send(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: &[u8],
        headers: &crate::SignedHeaders,
    ) -> Result<Vec<u8>, BitgetTransportError> {
        if body.len() > self.limits.maximum_body_bytes {
            return Err(BitgetTransportError::BodyTooLarge);
        }
        let url = if query.is_empty() {
            format!("{}{}", self.endpoint, path)
        } else {
            format!("{}{}?{}", self.endpoint, path, query)
        };
        let mut builder = match method {
            "GET" => self.client.get(url),
            "POST" => self.client.post(url).body(body.to_vec()),
            _ => return Err(BitgetTransportError::Protocol),
        };
        for name in [
            "ACCESS-KEY",
            "ACCESS-SIGN",
            "ACCESS-TIMESTAMP",
            "ACCESS-PASSPHRASE",
            "Content-Type",
            "locale",
        ] {
            builder = builder.header(
                name,
                headers.get(name).ok_or(BitgetTransportError::Signing)?,
            );
        }
        let mut response = builder.send().await.map_err(map_reqwest)?;
        if !response.status().is_success() {
            return Err(BitgetTransportError::HttpStatus);
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.limits.maximum_body_bytes as u64)
        {
            return Err(BitgetTransportError::BodyTooLarge);
        }
        let mut bytes = BytesMut::new();
        while let Some(chunk) = response.chunk().await.map_err(map_reqwest)? {
            let length = bytes
                .len()
                .checked_add(chunk.len())
                .ok_or(BitgetTransportError::BodyTooLarge)?;
            if length > self.limits.maximum_body_bytes {
                return Err(BitgetTransportError::BodyTooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes.to_vec())
    }
}

fn validate_binding(
    binding: &GatewayBinding,
    config: &BitgetConfig,
) -> Result<(), BitgetTransportError> {
    BitgetAccountBinding::UtaUsdtFuturesHedge
        .validate_gateway_binding(binding)
        .map_err(|_| BitgetTransportError::Binding)?;
    if binding.mode != config.mode() {
        return Err(BitgetTransportError::Binding);
    }
    Ok(())
}

fn map_reqwest(error: reqwest::Error) -> BitgetTransportError {
    if error.is_timeout() {
        BitgetTransportError::Timeout
    } else {
        BitgetTransportError::Disconnected
    }
}

fn mutation_dispatch_key(
    request: &BitgetPreparedMutation,
) -> Result<(u64, u64, String), BitgetTransportError> {
    let identity = request
        .expected_order_id
        .as_deref()
        .or(request.expected_client_order_id.as_deref())
        .ok_or(BitgetTransportError::Binding)?;
    Ok((
        request.attempt_id,
        request.generation,
        format!("{:?}:{identity}", request.kind),
    ))
}

pub(crate) fn unix_ms() -> Result<u64, BitgetTransportError> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| BitgetTransportError::Clock)?
            .as_millis(),
    )
    .map_err(|_| BitgetTransportError::Clock)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BitgetTransportError {
    #[error("Bitget transport limits are invalid")]
    Limits,
    #[error("Bitget transport binding, mode, attempt, or generation does not match")]
    Binding,
    #[error("Bitget request signing failed")]
    Signing,
    #[error("Bitget HTTP client failed")]
    Http,
    #[error("Bitget HTTP response status is not successful")]
    HttpStatus,
    #[error("Bitget transport operation timed out")]
    Timeout,
    #[error("Bitget transport disconnected after dispatch")]
    Disconnected,
    #[error("Bitget transport refuses a second dispatch of the same attempt identity")]
    AlreadyDispatched,
    #[error("Bitget response body exceeds the configured bound")]
    BodyTooLarge,
    #[error("Bitget response protocol is invalid or ambiguous")]
    Protocol,
    #[error("Bitget private pagination exceeds its fixed bound")]
    Pages,
    #[error("Bitget transport clock is invalid")]
    Clock,
    #[error(
        "Bitget authenticated recovery session is revoked, expired, cross-generation, outside its frozen universe, or not bound to the exact LIVE endpoint"
    )]
    RecoverySession,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures_util::{SinkExt, StreamExt};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::{Mutex, oneshot},
    };
    use tokio_tungstenite::{accept_async, tungstenite::Message};
    use venue_gateway_api::{GatewayMode, VenueId};

    use super::*;
    use crate::{
        BitgetCancelIntent, BitgetRecoverySymbolRequest, prepare_cancel_request,
        private_ws::authenticate_private_stream_for_test,
    };

    const ACCOUNT_BODY: &str = r#"{"code":"00000","data":{"imr":"0","mmr":"0","assets":[{"coin":"USDT","balance":"20","available":"20"}]}}"#;
    const SETTINGS_BODY: &str = r#"{"code":"00000","data":{"holdMode":"hedge_mode"}}"#;
    const POSITIONS_BODY: &str = r#"{"code":"00000","data":{"list":[]}}"#;
    const PAGE_BODY: &str = r#"{"code":"00000","data":{"list":[],"cursor":null}}"#;
    const FIRST_SYMBOL_BODY_BYTES: usize =
        ACCOUNT_BODY.len() + SETTINGS_BODY.len() + POSITIONS_BODY.len() + 2 * PAGE_BODY.len();

    fn binding(mode: GatewayMode) -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Bitget,
            mode,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)
    }

    fn limits(timeout_ms: u64) -> Result<BitgetTransportLimits, BitgetTransportError> {
        BitgetTransportLimits::new(Duration::from_millis(timeout_ms), 64 * 1024)
    }

    #[tokio::test]
    async fn timed_out_mutation_becomes_unknown_and_is_sent_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let accepts = Arc::new(Mutex::new(0_u32));
        let server_accepts = Arc::clone(&accepts);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            *server_accepts.lock().await += 1;
            let mut buffer = [0_u8; 4096];
            let _ = socket.read(&mut buffer).await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok::<_, std::io::Error>(())
        });
        let binding = binding(GatewayMode::Live)?;
        let config = BitgetConfig::for_mode(GatewayMode::Live);
        let transport =
            BitgetHttpTransport::with_endpoint(binding.clone(), 7, config, endpoint, limits(20)?)?;
        let request = prepare_cancel_request(
            &binding,
            &config,
            7,
            9,
            &BitgetCancelIntent {
                order_id: Some("123".to_owned()),
                client_order_id: None,
            },
        )?;
        let outcome = transport
            .execute_mutation_once(
                &BitgetCredentials::from_values("key", "secret", "pass")?,
                request,
                unix_ms()?,
            )
            .await?;
        assert!(matches!(outcome, BitgetMutationOutcome::Unknown(_)));
        server.await??;
        assert_eq!(*accepts.lock().await, 1);
        let duplicate = prepare_cancel_request(
            &binding,
            &config,
            7,
            9,
            &BitgetCancelIntent {
                order_id: Some("123".to_owned()),
                client_order_id: None,
            },
        )?;
        assert_eq!(
            transport
                .execute_mutation_once(
                    &BitgetCredentials::from_values("key", "secret", "pass")?,
                    duplicate,
                    unix_ms()?,
                )
                .await,
            Err(BitgetTransportError::AlreadyDispatched)
        );
        Ok(())
    }

    #[tokio::test]
    async fn live_request_uses_only_standard_authenticated_headers()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut buffer = vec![0_u8; 8192];
            let length = socket.read(&mut buffer).await?;
            let request = String::from_utf8_lossy(&buffer[..length]).to_ascii_lowercase();
            assert!(request.contains("access-key: key"));
            assert!(request.contains("access-sign:"));
            assert!(!request.contains("paptrading"));
            let body = r#"{"code":"00000","requestTime":1,"data":{"orderId":"123","clientOid":"venue_1"}}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                    .as_bytes(),
                )
                .await?;
            Ok::<_, std::io::Error>(())
        });
        let binding = binding(GatewayMode::Live)?;
        let config = BitgetConfig::for_mode(GatewayMode::Live);
        let transport =
            BitgetHttpTransport::with_endpoint(binding.clone(), 7, config, endpoint, limits(500)?)?;
        let request = prepare_cancel_request(
            &binding,
            &config,
            7,
            9,
            &BitgetCancelIntent {
                order_id: None,
                client_order_id: Some("venue_1".to_owned()),
            },
        )?;
        assert!(matches!(
            transport
                .execute_mutation_once(
                    &BitgetCredentials::from_values("key", "secret", "pass")?,
                    request,
                    1,
                )
                .await?,
            BitgetMutationOutcome::Acknowledged(_)
        ));
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn live_collects_five_same_generation_faces_from_loopback_fixture()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            for _ in 0..5 {
                let (mut socket, _) = listener.accept().await?;
                let mut buffer = vec![0_u8; 8192];
                let length = socket.read(&mut buffer).await?;
                let request = String::from_utf8_lossy(&buffer[..length]);
                let body = if request.contains(endpoints::BALANCES) {
                    r#"{"code":"00000","data":{"imr":"0","mmr":"0","assets":[{"coin":"USDT","balance":"20","available":"20"}]}}"#
                } else if request.contains(endpoints::ACCOUNT_SETTINGS) {
                    r#"{"code":"00000","data":{"holdMode":"hedge_mode"}}"#
                } else if request.contains(endpoints::POSITIONS) {
                    r#"{"code":"00000","data":{"list":[]}}"#
                } else if request.contains(endpoints::OPEN_ORDERS)
                    || request.contains(endpoints::FILLS)
                {
                    r#"{"code":"00000","data":{"list":[],"cursor":null}}"#
                } else {
                    return Err(std::io::Error::other("unexpected request path"));
                };
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .as_bytes(),
                    )
                    .await?;
            }
            Ok::<_, std::io::Error>(())
        });
        let binding = binding(GatewayMode::Live)?;
        let config = BitgetConfig::for_mode(GatewayMode::Live);
        let transport =
            BitgetHttpTransport::with_endpoint(binding, 7, config, endpoint, limits(500)?)?;
        let candidate = transport
            .collect_private_turn(
                &BitgetCredentials::from_values("key", "secret", "pass")?,
                9,
                7,
                Some(10),
                1_000,
            )
            .await?;
        assert_eq!(candidate.attempt_id, 9);
        assert_eq!(candidate.generation, 7);
        assert_eq!(candidate.raw_pages.len(), 5);
        assert_eq!(candidate.positions.len(), 2);
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn authenticated_multi_symbol_collector_preflights_global_page_and_byte_budgets()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        run_authenticated_budget_case(5, 8 * 1024 * 1024).await?;
        run_authenticated_budget_case(6, 64 * 1024 + FIRST_SYMBOL_BODY_BYTES.saturating_sub(1))
            .await
    }

    async fn run_authenticated_budget_case(
        maximum_total_pages: u32,
        maximum_total_bytes: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let http_listener = TcpListener::bind("127.0.0.1:0").await?;
        let http_origin: &'static str =
            Box::leak(format!("http://{}", http_listener.local_addr()?).into_boxed_str());
        let (second_symbol_done, wait_for_second_symbol) = oneshot::channel();
        let http_server = tokio::spawn(async move {
            for _ in 0..5 {
                let (mut socket, _) = http_listener.accept().await?;
                let mut buffer = vec![0_u8; 8192];
                let length = socket.read(&mut buffer).await?;
                let request = String::from_utf8_lossy(&buffer[..length]);
                let lower = request.to_ascii_lowercase();
                if !lower.contains("access-key: key")
                    || !lower.contains("access-sign:")
                    || !lower.contains("access-timestamp:")
                    || !lower.contains("access-passphrase: pass")
                    || request.contains("ETHUSDT")
                {
                    return Err(std::io::Error::other(
                        "recovery GET was unsigned or crossed symbols",
                    ));
                }
                let body = recovery_body(&request)?;
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .as_bytes(),
                    )
                    .await?;
            }
            wait_for_second_symbol.await.map_err(|_| {
                std::io::Error::other("second-symbol completion signal was dropped")
            })?;
            if tokio::time::timeout(Duration::from_millis(50), http_listener.accept())
                .await
                .is_ok()
            {
                return Err(std::io::Error::other(
                    "second symbol reached HTTP after the global budget was exhausted",
                ));
            }
            Ok::<_, std::io::Error>(())
        });

        let ws_listener = TcpListener::bind("127.0.0.1:0").await?;
        let ws_endpoint = format!("ws://{}", ws_listener.local_addr()?);
        let ws_server = tokio::spawn(async move {
            let (socket, _) = ws_listener.accept().await?;
            let mut websocket = accept_async(socket).await?;
            let _login = websocket.next().await.ok_or("missing login")??;
            websocket
                .send(Message::Text(
                    r#"{"event":"login","code":"0","msg":""}"#.into(),
                ))
                .await?;
            let _subscribe = websocket.next().await.ok_or("missing subscribe")??;
            for topic in ["account", "position", "order"] {
                websocket
                    .send(Message::Text(
                        format!(
                            r#"{{"event":"subscribe","arg":{{"instType":"UTA","topic":"{topic}"}},"connId":"budget"}}"#
                        )
                        .into(),
                    ))
                    .await?;
            }
            for _ in 0..5 {
                let ping = websocket.next().await.ok_or("missing recovery ping")??;
                let Message::Ping(nonce) = ping else {
                    return Err("recovery liveness did not use a nonce ping".into());
                };
                websocket.send(Message::Pong(nonce)).await?;
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });

        let account_id = "00000000-0000-4000-8000-000000000021";
        let first_binding = GatewayBinding::new(
            VenueId::Bitget,
            GatewayMode::Live,
            account_id,
            "BTC/USDT".parse()?,
        )?;
        let second_binding = GatewayBinding::new(
            VenueId::Bitget,
            GatewayMode::Live,
            account_id,
            "ETH/USDT".parse()?,
        )?;
        let config = BitgetConfig::for_mode(GatewayMode::Live);
        let credentials = BitgetCredentials::from_values("key", "secret", "pass")?;
        let transport_limits = limits(1_000)?;
        let (stream, _) = tokio_tungstenite::connect_async(&ws_endpoint).await?;
        let mut private_ws = authenticate_private_stream_for_test(
            stream,
            ws_endpoint,
            first_binding.clone(),
            config,
            &credentials,
            1_700_000_000_000,
            transport_limits,
        )
        .await?;
        let started_at_ms = unix_ms()?;
        let session = private_ws
            .begin_recovery_session(
                [
                    BitgetRecoverySymbolRequest::verified(
                        first_binding.symbol.clone(),
                        Some(1),
                        started_at_ms,
                    )?,
                    BitgetRecoverySymbolRequest::verified(
                        second_binding.symbol.clone(),
                        Some(1),
                        started_at_ms,
                    )?,
                ],
                started_at_ms + 30_000,
                maximum_total_bytes,
                maximum_total_pages,
            )?
            .with_rest_origin_for_test(http_origin)?;
        let generation = session.connection_generation();
        let first = BitgetHttpTransport::with_endpoint(
            first_binding,
            generation,
            config,
            http_origin.to_owned(),
            transport_limits,
        )?;
        let second = BitgetHttpTransport::with_endpoint(
            second_binding,
            generation,
            config,
            http_origin.to_owned(),
            transport_limits,
        )?;
        assert_eq!(
            first
                .collect_authenticated_private_turn(&mut private_ws, &session, &credentials)
                .await?
                .raw_pages
                .len(),
            5
        );
        assert_eq!(
            second
                .collect_authenticated_private_turn(&mut private_ws, &session, &credentials)
                .await,
            Err(BitgetTransportError::RecoverySession)
        );
        second_symbol_done
            .send(())
            .map_err(|_| "HTTP budget observer exited before the second symbol")?;
        http_server.await??;
        ws_server.await??;
        Ok(())
    }

    fn recovery_body(request: &str) -> Result<&'static str, std::io::Error> {
        if request.contains(endpoints::BALANCES) {
            Ok(ACCOUNT_BODY)
        } else if request.contains(endpoints::ACCOUNT_SETTINGS) {
            Ok(SETTINGS_BODY)
        } else if request.contains(endpoints::POSITIONS) {
            Ok(POSITIONS_BODY)
        } else if request.contains(endpoints::OPEN_ORDERS) || request.contains(endpoints::FILLS) {
            Ok(PAGE_BODY)
        } else {
            Err(std::io::Error::other("unexpected recovery request path"))
        }
    }

    #[test]
    fn fill_cursor_keeps_one_effective_window_across_pages()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding(GatewayMode::Live)?;
        let first = build_fills_read_request(&binding, 9, 7, 0, None, Some(10), 100)?;
        let second = build_fills_read_request(&binding, 9, 7, 1, Some("next"), Some(10), 100)?;
        assert_eq!(first.fill_history_start_ms, second.fill_history_start_ms);
        assert!(second.query.ends_with("&cursor=next"));
        assert_eq!(crate::private::BITGET_PRIVATE_PAGE_SIZE, 100);
        Ok(())
    }
}
