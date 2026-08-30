use std::{
    collections::BTreeSet,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use sha2::{Digest, Sha256};
use tokio::time::timeout;
use venue_domain::domain::{
    FieldState, NativeOrderFamily, Order, OrderOwner, OrderPurpose, Symbol,
};
use venue_gateway_api::GatewayMode;

use crate::private::{
    RecentFillsCursor, RecentFillsPageRequest, USER_TRADES_MAX_PAGES, USER_TRADES_PAGE_LIMIT,
    USER_TRADES_WINDOW_MS, advance_recent_fills_page, validate_recent_fills_range,
};
use crate::{
    BINANCE_EXECUTION_PROFILE_VERSION, BinanceConfig, BinanceCredentials, BinanceHttpTransport,
    BinanceInstrumentRules, BinancePositionMode, BinancePrivateReadScope,
    BinancePrivateReadbackCandidate, BinancePrivateSurface, BinanceRawPrivatePage,
    build_account_config_request, build_account_request, build_algo_orders_request,
    build_fills_request, build_position_mode_request, build_positions_request,
    build_regular_orders_request, complete_private_readback,
};

const RECOVERY_FACES: [BinanceRecoveryFace; 6] = [
    BinanceRecoveryFace::Account,
    BinanceRecoveryFace::Positions,
    BinanceRecoveryFace::UmOrder,
    BinanceRecoveryFace::UmConditional,
    BinanceRecoveryFace::UmAlgo,
    BinanceRecoveryFace::FillsCursor,
];
const BINANCE_RECOVERY_MAX_FRESHNESS_MS: u64 = 30_000;
const BINANCE_RECOVERY_MAX_SYMBOLS: usize = 256;
const BINANCE_RECOVERY_MAX_TOTAL_BYTES: usize = 64 * 1024 * 1024;
const BINANCE_RECOVERY_MAX_TOTAL_PAGES: u32 = 10_000;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum BinanceRecoveryFace {
    Account,
    Positions,
    UmOrder,
    UmConditional,
    UmAlgo,
    FillsCursor,
}

impl BinanceRecoveryFace {
    const fn tag(self) -> u8 {
        match self {
            Self::Account => 1,
            Self::Positions => 2,
            Self::UmOrder => 3,
            Self::UmConditional => 4,
            Self::UmAlgo => 5,
            Self::FillsCursor => 6,
        }
    }
}

/// Runtime commitments captured before any recovery request is issued. These are digest-only
/// evidence anchors: they neither open durable state nor confer writer or mutation authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceRuntimeRecoveryCommitments {
    owner: [u8; 32],
    wal: [u8; 32],
    unknown: [u8; 32],
    runtime_scope_sha256: [u8; 32],
}

impl BinanceRuntimeRecoveryCommitments {
    /// Creates a digest-only handoff value. The shared Runtime must compare all four digests with
    /// its sealed session before admitting a bundle; this adapter never treats caller input as
    /// writer, WAL, or recovery authority.
    pub fn verified(
        owner: [u8; 32],
        wal: [u8; 32],
        unknown: [u8; 32],
        runtime_scope_sha256: [u8; 32],
    ) -> Result<Self, BinanceRecoveryCollectorError> {
        if [owner, wal, unknown, runtime_scope_sha256]
            .iter()
            .any(|digest| digest.iter().all(|byte| *byte == 0))
        {
            return Err(BinanceRecoveryCollectorError::RuntimeCommitment);
        }
        Ok(Self {
            owner,
            wal,
            unknown,
            runtime_scope_sha256,
        })
    }

    #[must_use]
    pub const fn owner(&self) -> &[u8; 32] {
        &self.owner
    }

    #[must_use]
    pub const fn wal(&self) -> &[u8; 32] {
        &self.wal
    }

    #[must_use]
    pub const fn unknown(&self) -> &[u8; 32] {
        &self.unknown
    }

    #[must_use]
    pub const fn runtime_scope_sha256(&self) -> &[u8; 32] {
        &self.runtime_scope_sha256
    }
}

/// Runtime-owned freshness probe used only while collecting authenticated read evidence. The
/// adapter compares this digest after every network await; the probe cannot grant capability,
/// writer ownership, WAL access, or a dispatch permit.
pub trait BinanceRuntimeRecoveryScopeProbe: Send + Sync {
    fn current_runtime_scope_sha256(&self) -> [u8; 32];
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceRecoveryScopeInput {
    pub config_digest: String,
    pub config_epoch: u64,
    pub recovered_private_generation: u64,
    pub private_generation: u64,
    pub attempt_id: u64,
    pub started_at_ms: u64,
    pub deadline_at_ms: u64,
    pub maximum_total_bytes: usize,
    pub maximum_total_pages: u32,
    pub runtime_commitments: BinanceRuntimeRecoveryCommitments,
    pub symbol_universe: BTreeSet<Symbol>,
}

/// Immutable scope sealed at collector start. It deliberately has no serde representation, so a
/// persisted or historical adapter probe cannot be edited into a fresh recovery turn.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceRecoveryCollectionScope {
    mode: GatewayMode,
    trading_account_id: String,
    account_binding: &'static str,
    portfolio_rest_origin: &'static str,
    usd_m_public_rest_origin: &'static str,
    public_stream_origin: &'static str,
    private_stream_origin: &'static str,
    config_digest: String,
    config_epoch: u64,
    recovered_private_generation: u64,
    private_generation: u64,
    attempt_id: u64,
    started_at_ms: u64,
    deadline_at_ms: u64,
    maximum_total_bytes: usize,
    maximum_total_pages: u32,
    runtime_commitments: BinanceRuntimeRecoveryCommitments,
    symbol_universe: BTreeSet<Symbol>,
    commitment_sha256: [u8; 32],
}

impl BinanceRecoveryCollectionScope {
    pub fn verified(
        config: &BinanceConfig,
        input: BinanceRecoveryScopeInput,
    ) -> Result<Self, BinanceRecoveryCollectorError> {
        Self::verified_inner(config, input)
    }

    fn verified_inner(
        config: &BinanceConfig,
        input: BinanceRecoveryScopeInput,
    ) -> Result<Self, BinanceRecoveryCollectorError> {
        let binding = config.gateway_binding();
        if !valid_config_digest(&input.config_digest)
            || input.config_epoch == 0
            || input.private_generation <= input.recovered_private_generation
            || input.attempt_id == 0
            || input.started_at_ms == 0
            || input.deadline_at_ms <= input.started_at_ms
            || input.deadline_at_ms - input.started_at_ms > BINANCE_RECOVERY_MAX_FRESHNESS_MS
            || input.maximum_total_bytes == 0
            || input.maximum_total_bytes > BINANCE_RECOVERY_MAX_TOTAL_BYTES
            || input.maximum_total_pages == 0
            || input.maximum_total_pages > BINANCE_RECOVERY_MAX_TOTAL_PAGES
            || input.symbol_universe.is_empty()
            || input.symbol_universe.len() > BINANCE_RECOVERY_MAX_SYMBOLS
            || !input.symbol_universe.contains(&binding.symbol)
        {
            return Err(BinanceRecoveryCollectorError::Scope);
        }
        let endpoint_scope = BinanceRecoveryEndpointScope::from_config(config);
        let commitment_sha256 = scope_commitment(
            binding.mode,
            &binding.trading_account_id,
            &endpoint_scope,
            &input,
        );
        Ok(Self {
            mode: binding.mode,
            trading_account_id: binding.trading_account_id.clone(),
            account_binding: endpoint_scope.account_binding,
            portfolio_rest_origin: endpoint_scope.portfolio_rest_origin,
            usd_m_public_rest_origin: endpoint_scope.usd_m_public_rest_origin,
            public_stream_origin: endpoint_scope.public_stream_origin,
            private_stream_origin: endpoint_scope.private_stream_origin,
            config_digest: input.config_digest,
            config_epoch: input.config_epoch,
            recovered_private_generation: input.recovered_private_generation,
            private_generation: input.private_generation,
            attempt_id: input.attempt_id,
            started_at_ms: input.started_at_ms,
            deadline_at_ms: input.deadline_at_ms,
            maximum_total_bytes: input.maximum_total_bytes,
            maximum_total_pages: input.maximum_total_pages,
            runtime_commitments: input.runtime_commitments,
            symbol_universe: input.symbol_universe,
            commitment_sha256,
        })
    }

    #[must_use]
    pub const fn mode(&self) -> GatewayMode {
        self.mode
    }

    #[must_use]
    pub fn trading_account_id(&self) -> &str {
        &self.trading_account_id
    }

    #[must_use]
    pub fn config_digest(&self) -> &str {
        &self.config_digest
    }

    #[must_use]
    pub const fn config_epoch(&self) -> u64 {
        self.config_epoch
    }

    #[must_use]
    pub const fn recovered_private_generation(&self) -> u64 {
        self.recovered_private_generation
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
    pub const fn maximum_total_bytes(&self) -> usize {
        self.maximum_total_bytes
    }

    #[must_use]
    pub const fn maximum_total_pages(&self) -> u32 {
        self.maximum_total_pages
    }

    #[must_use]
    pub const fn runtime_commitments(&self) -> &BinanceRuntimeRecoveryCommitments {
        &self.runtime_commitments
    }

    #[must_use]
    pub const fn symbol_universe(&self) -> &BTreeSet<Symbol> {
        &self.symbol_universe
    }

    #[must_use]
    pub const fn commitment_sha256(&self) -> &[u8; 32] {
        &self.commitment_sha256
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceRecoveryOwnerRoute {
    family: NativeOrderFamily,
    venue_order_id: String,
    client_order_id: String,
    owner: OrderOwner,
}

impl BinanceRecoveryOwnerRoute {
    pub fn verified(
        family: NativeOrderFamily,
        venue_order_id: impl Into<String>,
        client_order_id: impl Into<String>,
        owner: OrderOwner,
    ) -> Result<Self, BinanceRecoveryCollectorError> {
        let venue_order_id = venue_order_id.into();
        let client_order_id = client_order_id.into();
        owner
            .validate()
            .map_err(|_| BinanceRecoveryCollectorError::OwnerRoute)?;
        if family == NativeOrderFamily::UmConditional
            || venue_order_id.trim().is_empty()
            || client_order_id.trim().is_empty()
        {
            return Err(BinanceRecoveryCollectorError::OwnerRoute);
        }
        Ok(Self {
            family,
            venue_order_id,
            client_order_id,
            owner,
        })
    }

    #[must_use]
    pub const fn family(&self) -> NativeOrderFamily {
        self.family
    }

    #[must_use]
    pub fn venue_order_id(&self) -> &str {
        &self.venue_order_id
    }

    #[must_use]
    pub fn client_order_id(&self) -> &str {
        &self.client_order_id
    }

    #[must_use]
    pub const fn owner(&self) -> &OrderOwner {
        &self.owner
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinanceRecoveryUnknownReason {
    MissingClientIdentity,
    MissingOwnerRoute,
    ConflictingOwnerIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BinanceRecoveryOrderCustody {
    ExactOwner {
        family: NativeOrderFamily,
        venue_order_id: String,
        client_order_id: String,
        owner: OrderOwner,
    },
    Unknown {
        family: NativeOrderFamily,
        venue_order_id: String,
        client_order_id: Option<String>,
        reason: BinanceRecoveryUnknownReason,
    },
}

/// One symbol-specific, read-only transport source. Construction verifies only static adapter
/// shape; a collector session is created later and only after Binance accepts a real signed
/// Account GET on this exact transport instance.
pub struct BinanceRecoverySymbolSource<'a> {
    transport: &'a BinanceHttpTransport,
    rules: BinanceInstrumentRules,
    initial_fills_cursor: RecentFillsCursor,
    fills_target_through_ms: u64,
}

impl<'a> BinanceRecoverySymbolSource<'a> {
    pub fn verified(
        transport: &'a BinanceHttpTransport,
        rules: BinanceInstrumentRules,
        initial_fills_cursor: RecentFillsCursor,
        fills_target_through_ms: u64,
    ) -> Result<Self, BinanceRecoveryCollectorError> {
        Self::verified_inner(
            transport,
            rules,
            initial_fills_cursor,
            fills_target_through_ms,
            false,
        )
    }

    fn verified_inner(
        transport: &'a BinanceHttpTransport,
        rules: BinanceInstrumentRules,
        initial_fills_cursor: RecentFillsCursor,
        fills_target_through_ms: u64,
        allow_fixture_endpoint: bool,
    ) -> Result<Self, BinanceRecoveryCollectorError> {
        rules
            .instrument
            .validate()
            .map_err(|_| BinanceRecoveryCollectorError::RequestUniverse)?;
        validate_recent_fills_range::<BinanceRecoveryCollectorError>(
            initial_fills_cursor,
            fills_target_through_ms,
        )?;
        if initial_fills_cursor.observed_through_ms == fills_target_through_ms
            || rules.instrument.symbol != transport.config().gateway_binding().symbol
            || rules.native_symbol != crate::native_symbol(&rules.instrument.symbol)
            || rules.instrument.generation != transport.recovery_instrument_generation()
            || !transport.recovery_uses_fixed_endpoint() && !allow_fixture_endpoint
        {
            return Err(
                if !transport.recovery_uses_fixed_endpoint() && !allow_fixture_endpoint {
                    BinanceRecoveryCollectorError::TransportEndpoint
                } else {
                    BinanceRecoveryCollectorError::RequestUniverse
                },
            );
        }
        Ok(Self {
            transport,
            rules,
            initial_fills_cursor,
            fills_target_through_ms,
        })
    }
}

/// Opaque, non-serializable session sealed by one successful signed Account GET. It is deliberately
/// private: the session proves authenticated continuity of this adapter transport only; its
/// generation numbers remain observed labels and are not runtime authority.
struct BinanceAuthenticatedCollectorSession<'a> {
    source: BinanceRecoverySymbolSource<'a>,
    credentials: &'a BinanceCredentials,
    read_scope: BinancePrivateReadScope,
    authenticated_account: BinanceRawPrivatePage,
    transport_instance_serial: u64,
    instrument_generation: u64,
    private_generation: u64,
    request_universe_sha256: [u8; 32],
    seal_sha256: [u8; 32],
    raw_pages: Vec<BinanceRawPrivatePage>,
    total_bytes: usize,
    total_pages: u32,
}

impl<'a> BinanceAuthenticatedCollectorSession<'a> {
    async fn authenticate(
        source: BinanceRecoverySymbolSource<'a>,
        credentials: &'a BinanceCredentials,
        scope: &BinanceRecoveryCollectionScope,
        request_universe_sha256: [u8; 32],
        consumed_bytes: usize,
        consumed_pages: u32,
        runtime_scope_probe: Option<&dyn BinanceRuntimeRecoveryScopeProbe>,
    ) -> Result<Self, BinanceRecoveryCollectorError> {
        validate_runtime_scope_probe(scope, runtime_scope_probe)?;
        validate_source_scope(scope, &source)?;
        if consumed_pages >= scope.maximum_total_pages {
            return Err(BinanceRecoveryCollectorError::PageLimit);
        }
        let read_scope = BinancePrivateReadScope::new(
            source.transport.config(),
            &source.rules,
            scope.private_generation,
            scope.attempt_id,
            scope.started_at_ms,
        )
        .map_err(|_| BinanceRecoveryCollectorError::RequestUniverse)?;
        let request = build_account_request(&read_scope)
            .map_err(|_| BinanceRecoveryCollectorError::RequestUniverse)?;
        let authenticated_account = execute_bounded_read(
            source.transport,
            credentials,
            &request,
            scope.deadline_at_ms,
        )
        .await
        .map_err(|error| {
            if error == BinanceRecoveryCollectorError::TransportRead {
                BinanceRecoveryCollectorError::Authentication
            } else {
                error
            }
        })?;
        validate_runtime_scope_probe(scope, runtime_scope_probe)?;
        validate_authenticated_page(scope, source.transport, &read_scope, &authenticated_account)?;
        let account_payload = std::str::from_utf8(&authenticated_account.payload)
            .map_err(|_| BinanceRecoveryCollectorError::Authentication)?;
        crate::portfolio::parse_account_balance(account_payload)
            .map_err(|_| BinanceRecoveryCollectorError::Authentication)?;
        let transport_instance_serial = source.transport.recovery_instance_serial();
        let instrument_generation = source.transport.recovery_instrument_generation();
        let private_generation = source.transport.recovery_private_generation();
        let seal_sha256 = authenticated_session_seal(
            scope,
            transport_instance_serial,
            instrument_generation,
            private_generation,
            &request_universe_sha256,
            &authenticated_account,
        );
        let mut session = Self {
            source,
            credentials,
            read_scope,
            authenticated_account: authenticated_account.clone(),
            transport_instance_serial,
            instrument_generation,
            private_generation,
            request_universe_sha256,
            seal_sha256,
            raw_pages: Vec::new(),
            total_bytes: consumed_bytes,
            total_pages: consumed_pages,
        };
        session.push_page(scope, authenticated_account)?;
        session.validate(scope)?;
        Ok(session)
    }

    async fn read(
        &mut self,
        scope: &BinanceRecoveryCollectionScope,
        request: crate::BinancePrivateReadRequest,
        runtime_scope_probe: Option<&dyn BinanceRuntimeRecoveryScopeProbe>,
    ) -> Result<BinanceRawPrivatePage, BinanceRecoveryCollectorError> {
        validate_runtime_scope_probe(scope, runtime_scope_probe)?;
        self.validate(scope)?;
        self.preflight_page_budget(scope)?;
        if request.scope() != &self.read_scope {
            return Err(BinanceRecoveryCollectorError::SessionDrift);
        }
        let page = execute_bounded_read(
            self.source.transport,
            self.credentials,
            &request,
            scope.deadline_at_ms,
        )
        .await?;
        validate_runtime_scope_probe(scope, runtime_scope_probe)?;
        self.validate(scope)?;
        validate_authenticated_page(scope, self.source.transport, &self.read_scope, &page)?;
        Ok(page)
    }

    fn preflight_page_budget(
        &self,
        scope: &BinanceRecoveryCollectionScope,
    ) -> Result<(), BinanceRecoveryCollectorError> {
        if self.total_pages >= scope.maximum_total_pages {
            return Err(BinanceRecoveryCollectorError::PageLimit);
        }
        Ok(())
    }

    fn push_page(
        &mut self,
        scope: &BinanceRecoveryCollectionScope,
        page: BinanceRawPrivatePage,
    ) -> Result<(), BinanceRecoveryCollectorError> {
        let next_bytes = self
            .total_bytes
            .checked_add(page.payload.len())
            .ok_or(BinanceRecoveryCollectorError::SizeLimit)?;
        let next_pages = self
            .total_pages
            .checked_add(1)
            .ok_or(BinanceRecoveryCollectorError::PageLimit)?;
        if next_bytes > scope.maximum_total_bytes
            || next_pages > scope.maximum_total_pages
            || page.payload.len() > self.source.transport.recovery_limits().maximum_body_bytes()
        {
            return Err(if next_bytes > scope.maximum_total_bytes {
                BinanceRecoveryCollectorError::SizeLimit
            } else {
                BinanceRecoveryCollectorError::PageLimit
            });
        }
        self.total_bytes = next_bytes;
        self.total_pages = next_pages;
        self.raw_pages.push(page);
        Ok(())
    }

    fn validate(
        &self,
        scope: &BinanceRecoveryCollectionScope,
    ) -> Result<(), BinanceRecoveryCollectorError> {
        if self.transport_instance_serial != self.source.transport.recovery_instance_serial()
            || self.instrument_generation != self.source.transport.recovery_instrument_generation()
            || self.private_generation != self.source.transport.recovery_private_generation()
            || self.private_generation != scope.private_generation
            || self.seal_sha256
                != authenticated_session_seal(
                    scope,
                    self.transport_instance_serial,
                    self.instrument_generation,
                    self.private_generation,
                    &self.request_universe_sha256,
                    &self.authenticated_account,
                )
            || unix_ms()? > scope.deadline_at_ms
        {
            return Err(BinanceRecoveryCollectorError::SessionDrift);
        }
        Ok(())
    }

    async fn collect_remaining(
        &mut self,
        scope: &BinanceRecoveryCollectionScope,
        runtime_scope_probe: Option<&dyn BinanceRuntimeRecoveryScopeProbe>,
    ) -> Result<(), BinanceRecoveryCollectorError> {
        for request in [
            build_account_config_request(&self.read_scope),
            build_position_mode_request(&self.read_scope),
            build_positions_request(&self.read_scope),
            build_regular_orders_request(&self.read_scope),
            build_algo_orders_request(&self.read_scope),
        ] {
            let request = request.map_err(|_| BinanceRecoveryCollectorError::RequestUniverse)?;
            let page = self.read(scope, request, runtime_scope_probe).await?;
            self.push_page(scope, page)?;
        }
        self.collect_fills(scope, runtime_scope_probe).await
    }

    async fn collect_fills(
        &mut self,
        scope: &BinanceRecoveryCollectionScope,
        runtime_scope_probe: Option<&dyn BinanceRuntimeRecoveryScopeProbe>,
    ) -> Result<(), BinanceRecoveryCollectorError> {
        let mut cursor = self.source.initial_fills_cursor;
        let mut window_start = cursor.observed_through_ms;
        let mut fill_page_index = 0_u32;
        while window_start < self.source.fills_target_through_ms {
            let window_end = window_start
                .saturating_add(USER_TRADES_WINDOW_MS)
                .min(self.source.fills_target_through_ms);
            loop {
                fill_page_index = fill_page_index
                    .checked_add(1)
                    .ok_or(BinanceRecoveryCollectorError::PageLimit)?;
                if fill_page_index > USER_TRADES_MAX_PAGES {
                    return Err(BinanceRecoveryCollectorError::PageLimit);
                }
                let request_shape = RecentFillsPageRequest {
                    start_time_ms: window_start,
                    end_time_ms: window_end,
                    from_id: match cursor.last_trade_id {
                        Some(value) => Some(
                            value
                                .checked_add(1)
                                .ok_or(BinanceRecoveryCollectorError::Cursor)?,
                        ),
                        None => None,
                    },
                    limit: USER_TRADES_PAGE_LIMIT,
                };
                let request = build_fills_request(
                    &self.read_scope,
                    fill_page_index,
                    cursor,
                    window_start,
                    window_end,
                )
                .map_err(|_| BinanceRecoveryCollectorError::Cursor)?;
                let page = self.read(scope, request, runtime_scope_probe).await?;
                let payload = std::str::from_utf8(&page.payload)
                    .map_err(|_| BinanceRecoveryCollectorError::Cursor)?;
                let (_, terminal) = advance_recent_fills_page::<BinanceRecoveryCollectorError>(
                    &mut cursor,
                    request_shape,
                    payload,
                )?;
                self.push_page(scope, page)?;
                if terminal {
                    cursor.observed_through_ms = window_end;
                    break;
                }
            }
            window_start = window_end;
        }
        Ok(())
    }

    fn into_replay(self) -> BinanceRecoveryReplay {
        BinanceRecoveryReplay {
            config: self.source.transport.config().clone(),
            rules: self.source.rules,
            initial_fills_cursor: self.source.initial_fills_cursor,
            fills_target_through_ms: self.source.fills_target_through_ms,
            raw_pages: self.raw_pages,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceRecoveryReplay {
    config: BinanceConfig,
    rules: BinanceInstrumentRules,
    initial_fills_cursor: RecentFillsCursor,
    fills_target_through_ms: u64,
    raw_pages: Vec<BinanceRawPrivatePage>,
}

#[cfg(test)]
impl BinanceRecoveryReplay {
    #[must_use]
    pub fn new(
        config: BinanceConfig,
        rules: BinanceInstrumentRules,
        initial_fills_cursor: RecentFillsCursor,
        fills_target_through_ms: u64,
        raw_pages: Vec<BinanceRawPrivatePage>,
    ) -> Self {
        Self {
            config,
            rules,
            initial_fills_cursor,
            fills_target_through_ms,
            raw_pages,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceRecoveryFaceCommitment {
    face: BinanceRecoveryFace,
    evidence_sha256: [u8; 32],
    record_count: u64,
    unsupported_profile_version: Option<u64>,
}

impl BinanceRecoveryFaceCommitment {
    #[must_use]
    pub const fn face(&self) -> BinanceRecoveryFace {
        self.face
    }

    #[must_use]
    pub const fn evidence_sha256(&self) -> &[u8; 32] {
        &self.evidence_sha256
    }

    #[must_use]
    pub const fn record_count(&self) -> u64 {
        self.record_count
    }

    #[must_use]
    pub const fn unsupported_profile_version(&self) -> Option<u64> {
        self.unsupported_profile_version
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceRecoverySymbolProjection {
    symbol: Symbol,
    private: BinancePrivateReadbackCandidate,
    order_custody: Vec<BinanceRecoveryOrderCustody>,
}

impl BinanceRecoverySymbolProjection {
    #[must_use]
    pub const fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    #[must_use]
    pub const fn private(&self) -> &BinancePrivateReadbackCandidate {
        &self.private
    }

    #[must_use]
    pub fn order_custody(&self) -> &[BinanceRecoveryOrderCustody] {
        &self.order_custody
    }
}

/// Fresh, scope-bound six-face candidate. It is evidence only and intentionally exposes no
/// capability, writer, WAL, network client, or dispatch method.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceFreshRecoveryCandidate {
    scope: BinanceRecoveryCollectionScope,
    completed_at_ms: u64,
    owner_routes: Vec<BinanceRecoveryOwnerRoute>,
    replays: Vec<BinanceRecoveryReplay>,
    projections: Vec<BinanceRecoverySymbolProjection>,
    faces: Vec<BinanceRecoveryFaceCommitment>,
    request_universe_sha256: [u8; 32],
    projection_commitment_sha256: [u8; 32],
}

/// Complete Binance collection evidence that can be consumed by the shared Runtime bridge.
/// Construction rejects every unresolved custody record, so a returned bundle cannot silently
/// turn an unknown or unmanaged order into a recoverable owned order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceRuntimeRecoveryBundle {
    candidate: BinanceFreshRecoveryCandidate,
    position_mode: BinancePositionMode,
}

impl BinanceRuntimeRecoveryBundle {
    #[must_use]
    pub const fn scope(&self) -> &BinanceRecoveryCollectionScope {
        self.candidate.scope()
    }

    #[must_use]
    pub fn projections(&self) -> &[BinanceRecoverySymbolProjection] {
        self.candidate.projections()
    }

    #[must_use]
    pub fn faces(&self) -> &[BinanceRecoveryFaceCommitment] {
        self.candidate.faces()
    }

    #[must_use]
    pub const fn request_universe_sha256(&self) -> &[u8; 32] {
        self.candidate.request_universe_sha256()
    }

    #[must_use]
    pub const fn projection_commitment_sha256(&self) -> &[u8; 32] {
        self.candidate.projection_commitment_sha256()
    }

    #[must_use]
    pub const fn attempt_id(&self) -> u64 {
        self.candidate.scope.attempt_id
    }

    #[must_use]
    pub const fn private_generation(&self) -> u64 {
        self.candidate.scope.private_generation
    }

    #[must_use]
    pub const fn deadline_at_ms(&self) -> u64 {
        self.candidate.scope.deadline_at_ms
    }

    #[must_use]
    pub const fn completed_at_ms(&self) -> u64 {
        self.candidate.completed_at_ms
    }

    #[must_use]
    pub const fn symbol_universe(&self) -> &BTreeSet<Symbol> {
        self.candidate.scope.symbol_universe()
    }

    #[must_use]
    pub fn position_mode(&self) -> BinancePositionMode {
        self.position_mode
    }

    #[must_use]
    pub const fn execution_profile_version(&self) -> u64 {
        BINANCE_EXECUTION_PROFILE_VERSION
    }

    pub fn verify_fresh(
        &self,
        expected_scope: &BinanceRecoveryCollectionScope,
        observed_at_ms: u64,
    ) -> Result<(), BinanceRecoveryCollectorError> {
        self.candidate
            .verify_runtime_bundle(expected_scope, observed_at_ms)
    }
}

impl BinanceFreshRecoveryCandidate {
    #[must_use]
    pub const fn scope(&self) -> &BinanceRecoveryCollectionScope {
        &self.scope
    }

    #[must_use]
    pub const fn completed_at_ms(&self) -> u64 {
        self.completed_at_ms
    }

    #[must_use]
    pub fn projections(&self) -> &[BinanceRecoverySymbolProjection] {
        &self.projections
    }

    #[must_use]
    pub fn faces(&self) -> &[BinanceRecoveryFaceCommitment] {
        &self.faces
    }

    #[must_use]
    pub const fn request_universe_sha256(&self) -> &[u8; 32] {
        &self.request_universe_sha256
    }

    #[must_use]
    pub const fn projection_commitment_sha256(&self) -> &[u8; 32] {
        &self.projection_commitment_sha256
    }

    pub fn verify_fresh(
        &self,
        expected_scope: &BinanceRecoveryCollectionScope,
        observed_at_ms: u64,
    ) -> Result<(), BinanceRecoveryCollectorError> {
        if expected_scope != &self.scope {
            return Err(BinanceRecoveryCollectorError::Relabelled);
        }
        if observed_at_ms < self.completed_at_ms || observed_at_ms > self.scope.deadline_at_ms {
            return Err(BinanceRecoveryCollectorError::Expired);
        }
        let rebuilt = build_candidate(
            self.scope.clone(),
            self.owner_routes.clone(),
            self.completed_at_ms,
            self.replays.clone(),
        )?;
        if rebuilt.projections != self.projections
            || rebuilt.faces != self.faces
            || rebuilt.request_universe_sha256 != self.request_universe_sha256
            || rebuilt.projection_commitment_sha256 != self.projection_commitment_sha256
        {
            return Err(BinanceRecoveryCollectorError::ProjectionCommitment);
        }
        Ok(())
    }

    /// Converts fresh raw collection evidence into the only form exposed to the shared Runtime
    /// bridge. Unknown regular or Algo custody is intentionally terminal for this attempt.
    pub fn into_runtime_bundle(
        self,
        expected_scope: &BinanceRecoveryCollectionScope,
        observed_at_ms: u64,
    ) -> Result<BinanceRuntimeRecoveryBundle, BinanceRecoveryCollectorError> {
        self.verify_runtime_bundle(expected_scope, observed_at_ms)?;
        let position_mode = self
            .projections
            .first()
            .ok_or(BinanceRecoveryCollectorError::SymbolUniverse)?
            .private
            .position_mode();
        Ok(BinanceRuntimeRecoveryBundle {
            candidate: self,
            position_mode,
        })
    }

    fn verify_runtime_bundle(
        &self,
        expected_scope: &BinanceRecoveryCollectionScope,
        observed_at_ms: u64,
    ) -> Result<(), BinanceRecoveryCollectorError> {
        self.verify_fresh(expected_scope, observed_at_ms)?;
        if self.projections.iter().any(|projection| {
            projection
                .order_custody
                .iter()
                .any(|custody| matches!(custody, BinanceRecoveryOrderCustody::Unknown { .. }))
        }) {
            return Err(BinanceRecoveryCollectorError::UnmanagedOrder);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceFreshRecoveryCollector {
    scope: BinanceRecoveryCollectionScope,
    owner_routes: Vec<BinanceRecoveryOwnerRoute>,
}

impl BinanceFreshRecoveryCollector {
    pub async fn collect_authenticated<'a>(
        scope: BinanceRecoveryCollectionScope,
        credentials: &'a BinanceCredentials,
        sources: Vec<BinanceRecoverySymbolSource<'a>>,
    ) -> Result<BinanceFreshRecoveryCandidate, BinanceRecoveryCollectorError> {
        Self::collect_authenticated_inner(scope, credentials, sources, Vec::new(), None).await
    }

    /// Collects the production Runtime bridge in one bounded authenticated turn. Durable Owner
    /// routes are inputs to custody verification only; neither they nor the Runtime scope probe
    /// grant this adapter a mutation capability.
    pub async fn collect_runtime_bundle_authenticated<'a>(
        scope: BinanceRecoveryCollectionScope,
        credentials: &'a BinanceCredentials,
        sources: Vec<BinanceRecoverySymbolSource<'a>>,
        owner_routes: Vec<BinanceRecoveryOwnerRoute>,
        runtime_scope_probe: &dyn BinanceRuntimeRecoveryScopeProbe,
    ) -> Result<BinanceRuntimeRecoveryBundle, BinanceRecoveryCollectorError> {
        let expected_scope = scope.clone();
        let candidate = Self::collect_authenticated_inner(
            scope,
            credentials,
            sources,
            owner_routes,
            Some(runtime_scope_probe),
        )
        .await?;
        let observed_at_ms = unix_ms()?;
        validate_runtime_scope_probe(&expected_scope, Some(runtime_scope_probe))?;
        candidate.into_runtime_bundle(&expected_scope, observed_at_ms)
    }

    async fn collect_authenticated_inner<'a>(
        scope: BinanceRecoveryCollectionScope,
        credentials: &'a BinanceCredentials,
        mut sources: Vec<BinanceRecoverySymbolSource<'a>>,
        mut owner_routes: Vec<BinanceRecoveryOwnerRoute>,
        runtime_scope_probe: Option<&dyn BinanceRuntimeRecoveryScopeProbe>,
    ) -> Result<BinanceFreshRecoveryCandidate, BinanceRecoveryCollectorError> {
        validate_owner_routes(&scope, &owner_routes)?;
        owner_routes.sort_by(|left, right| owner_route_key(left).cmp(&owner_route_key(right)));
        sources.sort_by(|left, right| {
            left.transport
                .config()
                .gateway_binding()
                .symbol
                .cmp(&right.transport.config().gateway_binding().symbol)
        });
        let actual_symbols = sources
            .iter()
            .map(|source| source.transport.config().gateway_binding().symbol.clone())
            .collect::<BTreeSet<_>>();
        if actual_symbols != scope.symbol_universe || sources.len() != actual_symbols.len() {
            return Err(BinanceRecoveryCollectorError::RequestUniverse);
        }
        let mut replays = Vec::with_capacity(sources.len());
        let request_universe_sha256 = request_universe_commitment_from_sources(&scope, &sources);
        let mut total_bytes = 0_usize;
        let mut total_pages = 0_u32;
        for source in sources {
            let mut session = BinanceAuthenticatedCollectorSession::authenticate(
                source,
                credentials,
                &scope,
                request_universe_sha256,
                total_bytes,
                total_pages,
                runtime_scope_probe,
            )
            .await?;
            session
                .collect_remaining(&scope, runtime_scope_probe)
                .await?;
            total_bytes = session.total_bytes;
            total_pages = session.total_pages;
            replays.push(session.into_replay());
        }
        let completed_at_ms = unix_ms()?;
        validate_runtime_scope_probe(&scope, runtime_scope_probe)?;
        let candidate = build_candidate(scope, owner_routes, completed_at_ms, replays)?;
        if candidate.request_universe_sha256 != request_universe_sha256 {
            return Err(BinanceRecoveryCollectorError::RequestUniverse);
        }
        Ok(candidate)
    }

    #[cfg(test)]
    async fn collect_authenticated_fixture<'a>(
        scope: BinanceRecoveryCollectionScope,
        credentials: &'a BinanceCredentials,
        transports: Vec<(
            &'a BinanceHttpTransport,
            BinanceInstrumentRules,
            RecentFillsCursor,
            u64,
        )>,
        owner_routes: Vec<BinanceRecoveryOwnerRoute>,
    ) -> Result<BinanceFreshRecoveryCandidate, BinanceRecoveryCollectorError> {
        let mut sources = Vec::with_capacity(transports.len());
        for (transport, rules, cursor, target) in transports {
            sources.push(BinanceRecoverySymbolSource::verified_inner(
                transport, rules, cursor, target, true,
            )?);
        }
        Self::collect_authenticated_inner(scope, credentials, sources, owner_routes, None).await
    }

    #[cfg(test)]
    pub fn begin(
        scope: BinanceRecoveryCollectionScope,
        mut owner_routes: Vec<BinanceRecoveryOwnerRoute>,
    ) -> Result<Self, BinanceRecoveryCollectorError> {
        validate_owner_routes(&scope, &owner_routes)?;
        owner_routes.sort_by(|left, right| owner_route_key(left).cmp(&owner_route_key(right)));
        Ok(Self {
            scope,
            owner_routes,
        })
    }

    #[cfg(test)]
    pub fn finish(
        self,
        completed_at_ms: u64,
        replays: Vec<BinanceRecoveryReplay>,
    ) -> Result<BinanceFreshRecoveryCandidate, BinanceRecoveryCollectorError> {
        build_candidate(self.scope, self.owner_routes, completed_at_ms, replays)
    }
}

fn build_candidate(
    scope: BinanceRecoveryCollectionScope,
    owner_routes: Vec<BinanceRecoveryOwnerRoute>,
    completed_at_ms: u64,
    mut replays: Vec<BinanceRecoveryReplay>,
) -> Result<BinanceFreshRecoveryCandidate, BinanceRecoveryCollectorError> {
    if completed_at_ms < scope.started_at_ms || completed_at_ms > scope.deadline_at_ms {
        return Err(BinanceRecoveryCollectorError::Expired);
    }
    replays.sort_by(|left, right| {
        left.config
            .gateway_binding()
            .symbol
            .cmp(&right.config.gateway_binding().symbol)
    });
    let actual_symbols = replays
        .iter()
        .map(|replay| replay.config.gateway_binding().symbol.clone())
        .collect::<BTreeSet<_>>();
    if actual_symbols != scope.symbol_universe || actual_symbols.len() != replays.len() {
        return Err(BinanceRecoveryCollectorError::SymbolUniverse);
    }

    let mut projections = Vec::with_capacity(replays.len());
    for replay in &replays {
        validate_replay_scope(&scope, completed_at_ms, replay)?;
        let private = complete_private_readback(
            &replay.config,
            &replay.rules,
            &replay.raw_pages[0].scope,
            replay.initial_fills_cursor,
            replay.fills_target_through_ms,
            replay.raw_pages.clone(),
        )
        .map_err(|_| BinanceRecoveryCollectorError::Replay)?;
        let order_custody = classify_orders(&scope, &owner_routes, &private)?;
        projections.push(BinanceRecoverySymbolProjection {
            symbol: replay.config.gateway_binding().symbol.clone(),
            private,
            order_custody,
        });
    }
    validate_account_projection(&projections)?;
    let faces = build_face_commitments(&scope, &replays, &projections);
    let request_universe_sha256 = request_universe_commitment(&scope, &replays);
    let projection_commitment_sha256 =
        projection_commitment(&scope, &request_universe_sha256, &faces, &projections);
    Ok(BinanceFreshRecoveryCandidate {
        scope,
        completed_at_ms,
        owner_routes,
        replays,
        projections,
        faces,
        request_universe_sha256,
        projection_commitment_sha256,
    })
}

fn validate_source_scope(
    scope: &BinanceRecoveryCollectionScope,
    source: &BinanceRecoverySymbolSource<'_>,
) -> Result<(), BinanceRecoveryCollectorError> {
    let config = source.transport.config();
    let binding = config.gateway_binding();
    if binding.mode != scope.mode
        || binding.trading_account_id != scope.trading_account_id
        || !scope.symbol_universe.contains(&binding.symbol)
        || config.account_binding().as_str() != scope.account_binding
        || config.portfolio_rest_origin() != scope.portfolio_rest_origin
        || config.usd_m_public_rest_origin() != scope.usd_m_public_rest_origin
        || config.public_stream_origin() != scope.public_stream_origin
        || config.private_stream_origin() != scope.private_stream_origin
        || source.transport.recovery_private_generation() != scope.private_generation
        || source.rules.instrument.symbol != binding.symbol
        || source.rules.instrument.generation != source.transport.recovery_instrument_generation()
        || source.fills_target_through_ms > scope.started_at_ms
    {
        return Err(BinanceRecoveryCollectorError::RequestUniverse);
    }
    Ok(())
}

async fn execute_bounded_read(
    transport: &BinanceHttpTransport,
    credentials: &BinanceCredentials,
    request: &crate::BinancePrivateReadRequest,
    deadline_at_ms: u64,
) -> Result<BinanceRawPrivatePage, BinanceRecoveryCollectorError> {
    let before_ms = unix_ms()?;
    let remaining_ms = deadline_at_ms
        .checked_sub(before_ms)
        .filter(|remaining| *remaining > 0)
        .ok_or(BinanceRecoveryCollectorError::Expired)?;
    let page = timeout(
        Duration::from_millis(remaining_ms),
        transport.execute_read(credentials, request, before_ms),
    )
    .await
    .map_err(|_| BinanceRecoveryCollectorError::Expired)?
    .map_err(|_| BinanceRecoveryCollectorError::TransportRead)?;
    if unix_ms()? > deadline_at_ms {
        return Err(BinanceRecoveryCollectorError::Expired);
    }
    Ok(page)
}

fn validate_authenticated_page(
    scope: &BinanceRecoveryCollectionScope,
    transport: &BinanceHttpTransport,
    read_scope: &BinancePrivateReadScope,
    page: &BinanceRawPrivatePage,
) -> Result<(), BinanceRecoveryCollectorError> {
    if &page.scope != read_scope
        || read_scope.binding() != transport.config().gateway_binding()
        || read_scope.instrument_generation() != transport.recovery_instrument_generation()
        || read_scope.private_generation() != transport.recovery_private_generation()
        || read_scope.private_generation() != scope.private_generation
        || read_scope.attempt_id() != scope.attempt_id
        || read_scope.requested_at_ms() != scope.started_at_ms
        || page.received_at_ms > scope.deadline_at_ms
    {
        return Err(BinanceRecoveryCollectorError::SessionDrift);
    }
    Ok(())
}

fn authenticated_session_seal(
    scope: &BinanceRecoveryCollectionScope,
    transport_instance_serial: u64,
    instrument_generation: u64,
    private_generation: u64,
    request_universe_sha256: &[u8; 32],
    account_page: &BinanceRawPrivatePage,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    commit_bytes(
        &mut digest,
        b"venue-binance-authenticated-recovery-session-v1",
    );
    commit_bytes(&mut digest, scope.commitment_sha256());
    commit_u64(&mut digest, transport_instance_serial);
    commit_u64(&mut digest, instrument_generation);
    commit_u64(&mut digest, private_generation);
    commit_bytes(&mut digest, request_universe_sha256);
    commit_page(&mut digest, account_page);
    digest.finalize().into()
}

fn unix_ms() -> Result<u64, BinanceRecoveryCollectorError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BinanceRecoveryCollectorError::Clock)?
        .as_millis();
    u64::try_from(millis).map_err(|_| BinanceRecoveryCollectorError::Clock)
}

fn validate_runtime_scope_probe(
    scope: &BinanceRecoveryCollectionScope,
    runtime_scope_probe: Option<&dyn BinanceRuntimeRecoveryScopeProbe>,
) -> Result<(), BinanceRecoveryCollectorError> {
    if runtime_scope_probe.is_some_and(|probe| {
        probe.current_runtime_scope_sha256() != *scope.runtime_commitments.runtime_scope_sha256()
    }) {
        return Err(BinanceRecoveryCollectorError::RuntimeScopeDrift);
    }
    Ok(())
}

fn validate_replay_scope(
    scope: &BinanceRecoveryCollectionScope,
    completed_at_ms: u64,
    replay: &BinanceRecoveryReplay,
) -> Result<(), BinanceRecoveryCollectorError> {
    let binding = replay.config.gateway_binding();
    if replay.raw_pages.is_empty()
        || binding.mode != scope.mode
        || binding.trading_account_id != scope.trading_account_id
        || replay.config.account_binding().as_str() != scope.account_binding
        || replay.config.portfolio_rest_origin() != scope.portfolio_rest_origin
        || replay.config.usd_m_public_rest_origin() != scope.usd_m_public_rest_origin
        || replay.config.public_stream_origin() != scope.public_stream_origin
        || replay.config.private_stream_origin() != scope.private_stream_origin
        || !scope.symbol_universe.contains(&binding.symbol)
        || replay.rules.instrument.symbol != binding.symbol
        || replay.rules.instrument.generation == 0
        || replay.fills_target_through_ms > completed_at_ms
    {
        return Err(BinanceRecoveryCollectorError::Scope);
    }
    for page in &replay.raw_pages {
        let page_scope = &page.scope;
        if page_scope.binding() != binding
            || page_scope.private_generation() != scope.private_generation
            || page_scope.attempt_id() != scope.attempt_id
            || page_scope.requested_at_ms() != scope.started_at_ms
            || page.requested_at_ms < scope.started_at_ms
            || page.received_at_ms > completed_at_ms
            || page.received_at_ms > scope.deadline_at_ms
        {
            return Err(BinanceRecoveryCollectorError::AttemptDrift);
        }
    }
    Ok(())
}

fn validate_owner_routes(
    scope: &BinanceRecoveryCollectionScope,
    routes: &[BinanceRecoveryOwnerRoute],
) -> Result<(), BinanceRecoveryCollectorError> {
    let mut venue_ids = BTreeSet::new();
    let mut client_ids = BTreeSet::new();
    for route in routes {
        if route.owner.exchange != "binance"
            || route.owner.account != scope.trading_account_id
            || !scope.symbol_universe.contains(&route.owner.symbol)
            || !venue_ids.insert((route.family, route.venue_order_id.as_str()))
            || !client_ids.insert((route.family, route.client_order_id.as_str()))
        {
            return Err(BinanceRecoveryCollectorError::OwnerRoute);
        }
    }
    Ok(())
}

fn classify_orders(
    scope: &BinanceRecoveryCollectionScope,
    routes: &[BinanceRecoveryOwnerRoute],
    private: &BinancePrivateReadbackCandidate,
) -> Result<Vec<BinanceRecoveryOrderCustody>, BinanceRecoveryCollectorError> {
    let mut result = Vec::new();
    for (family, orders) in [
        (NativeOrderFamily::UmOrder, &private.regular().orders),
        (NativeOrderFamily::UmAlgo, &private.algo().orders),
    ] {
        for order in orders {
            result.push(classify_order(scope, routes, family, order)?);
        }
    }
    result.sort_by(|left, right| custody_key(left).cmp(&custody_key(right)));
    Ok(result)
}

fn classify_order(
    scope: &BinanceRecoveryCollectionScope,
    routes: &[BinanceRecoveryOwnerRoute],
    family: NativeOrderFamily,
    order: &Order,
) -> Result<BinanceRecoveryOrderCustody, BinanceRecoveryCollectorError> {
    if !scope.symbol_universe.contains(&order.symbol) {
        return Err(BinanceRecoveryCollectorError::OwnerRoute);
    }
    let client_order_id = match &order.client_order_id {
        FieldState::Known(value) if !value.is_empty() => value,
        _ => {
            return Ok(BinanceRecoveryOrderCustody::Unknown {
                family,
                venue_order_id: order.order_id.clone(),
                client_order_id: None,
                reason: BinanceRecoveryUnknownReason::MissingClientIdentity,
            });
        }
    };
    if let Some(route) = routes.iter().find(|route| {
        route.family == family
            && route.venue_order_id == order.order_id
            && route.client_order_id == *client_order_id
    }) {
        if route.owner.symbol != order.symbol {
            return Err(BinanceRecoveryCollectorError::OwnerRoute);
        }
        return Ok(BinanceRecoveryOrderCustody::ExactOwner {
            family,
            venue_order_id: order.order_id.clone(),
            client_order_id: client_order_id.clone(),
            owner: route.owner.clone(),
        });
    }
    let conflict = routes.iter().any(|route| {
        route.family == family
            && (route.venue_order_id == order.order_id || route.client_order_id == *client_order_id)
    });
    Ok(BinanceRecoveryOrderCustody::Unknown {
        family,
        venue_order_id: order.order_id.clone(),
        client_order_id: Some(client_order_id.clone()),
        reason: if conflict {
            BinanceRecoveryUnknownReason::ConflictingOwnerIdentity
        } else {
            BinanceRecoveryUnknownReason::MissingOwnerRoute
        },
    })
}

fn validate_account_projection(
    projections: &[BinanceRecoverySymbolProjection],
) -> Result<(), BinanceRecoveryCollectorError> {
    let first = projections
        .first()
        .ok_or(BinanceRecoveryCollectorError::SymbolUniverse)?;
    if projections.iter().skip(1).any(|projection| {
        projection.private.position_mode() != first.private.position_mode()
            || projection.private.capabilities() != first.private.capabilities()
            || projection.private.balances() != first.private.balances()
    }) {
        return Err(BinanceRecoveryCollectorError::ProjectionCommitment);
    }
    Ok(())
}

fn build_face_commitments(
    scope: &BinanceRecoveryCollectionScope,
    replays: &[BinanceRecoveryReplay],
    projections: &[BinanceRecoverySymbolProjection],
) -> Vec<BinanceRecoveryFaceCommitment> {
    RECOVERY_FACES
        .into_iter()
        .map(|face| {
            let record_count = projections
                .iter()
                .map(|projection| face_record_count(face, &projection.private))
                .sum();
            let mut digest = Sha256::new();
            commit_bytes(&mut digest, b"venue-binance-recovery-face-v1");
            commit_bytes(&mut digest, &scope.commitment_sha256);
            commit_bytes(&mut digest, &[face.tag()]);
            for replay in replays {
                commit_str(
                    &mut digest,
                    &replay.config.gateway_binding().symbol.to_string(),
                );
                for page in replay
                    .raw_pages
                    .iter()
                    .filter(|page| face_contains_surface(face, page.surface))
                {
                    commit_page(&mut digest, page);
                }
            }
            if face == BinanceRecoveryFace::UmConditional {
                commit_u64(&mut digest, BINANCE_EXECUTION_PROFILE_VERSION);
            }
            for projection in projections {
                if matches!(
                    face,
                    BinanceRecoveryFace::UmOrder | BinanceRecoveryFace::UmAlgo
                ) {
                    for custody in &projection.order_custody {
                        if custody_family(custody) == face_family(face) {
                            commit_custody(&mut digest, custody);
                        }
                    }
                }
            }
            BinanceRecoveryFaceCommitment {
                face,
                evidence_sha256: digest.finalize().into(),
                record_count,
                unsupported_profile_version: (face == BinanceRecoveryFace::UmConditional)
                    .then_some(BINANCE_EXECUTION_PROFILE_VERSION),
            }
        })
        .collect()
}

fn face_record_count(face: BinanceRecoveryFace, private: &BinancePrivateReadbackCandidate) -> u64 {
    let count = match face {
        BinanceRecoveryFace::Account => private.balances().len(),
        BinanceRecoveryFace::Positions => private.positions().len(),
        BinanceRecoveryFace::UmOrder => private.regular().orders.len(),
        BinanceRecoveryFace::UmConditional => 0,
        BinanceRecoveryFace::UmAlgo => private.algo().orders.len(),
        BinanceRecoveryFace::FillsCursor => private.fills().len(),
    };
    u64::try_from(count).unwrap_or(u64::MAX)
}

fn face_contains_surface(face: BinanceRecoveryFace, surface: BinancePrivateSurface) -> bool {
    match face {
        BinanceRecoveryFace::Account => matches!(
            surface,
            BinancePrivateSurface::Account
                | BinancePrivateSurface::AccountConfig
                | BinancePrivateSurface::PositionMode
        ),
        BinanceRecoveryFace::Positions => surface == BinancePrivateSurface::Positions,
        BinanceRecoveryFace::UmOrder => surface == BinancePrivateSurface::RegularOrders,
        BinanceRecoveryFace::UmConditional => false,
        BinanceRecoveryFace::UmAlgo => surface == BinancePrivateSurface::AlgoOrders,
        BinanceRecoveryFace::FillsCursor => surface == BinancePrivateSurface::Fills,
    }
}

fn projection_commitment(
    scope: &BinanceRecoveryCollectionScope,
    request_universe_sha256: &[u8; 32],
    faces: &[BinanceRecoveryFaceCommitment],
    projections: &[BinanceRecoverySymbolProjection],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    commit_bytes(&mut digest, b"venue-binance-recovery-projection-v1");
    commit_bytes(&mut digest, &scope.commitment_sha256);
    commit_bytes(&mut digest, request_universe_sha256);
    for face in faces {
        commit_bytes(&mut digest, &[face.face.tag()]);
        commit_bytes(&mut digest, &face.evidence_sha256);
        commit_u64(&mut digest, face.record_count);
        commit_u64(
            &mut digest,
            face.unsupported_profile_version.unwrap_or_default(),
        );
    }
    for projection in projections {
        commit_str(&mut digest, &projection.symbol.to_string());
        commit_bytes(&mut digest, &projection.private.raw_payload_digest());
        let cursor = projection.private.fills_cursor();
        commit_u64(&mut digest, cursor.observed_through_ms);
        commit_u64(&mut digest, cursor.last_trade_id.unwrap_or_default());
        commit_u64(&mut digest, cursor.last_event_time_ms.unwrap_or_default());
        for custody in &projection.order_custody {
            commit_custody(&mut digest, custody);
        }
    }
    digest.finalize().into()
}

fn request_universe_commitment(
    scope: &BinanceRecoveryCollectionScope,
    replays: &[BinanceRecoveryReplay],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    commit_bytes(&mut digest, b"venue-binance-recovery-request-universe-v1");
    commit_bytes(&mut digest, scope.commitment_sha256());
    for replay in replays {
        commit_request_spec(
            &mut digest,
            &replay.config,
            &replay.rules,
            replay.initial_fills_cursor,
            replay.fills_target_through_ms,
        );
    }
    digest.finalize().into()
}

fn request_universe_commitment_from_sources(
    scope: &BinanceRecoveryCollectionScope,
    sources: &[BinanceRecoverySymbolSource<'_>],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    commit_bytes(&mut digest, b"venue-binance-recovery-request-universe-v1");
    commit_bytes(&mut digest, scope.commitment_sha256());
    for source in sources {
        commit_request_spec(
            &mut digest,
            source.transport.config(),
            &source.rules,
            source.initial_fills_cursor,
            source.fills_target_through_ms,
        );
    }
    digest.finalize().into()
}

fn commit_request_spec(
    digest: &mut Sha256,
    config: &BinanceConfig,
    rules: &BinanceInstrumentRules,
    initial_fills_cursor: RecentFillsCursor,
    fills_target_through_ms: u64,
) {
    commit_str(digest, &config.gateway_binding().symbol.to_string());
    commit_str(digest, &rules.native_symbol);
    commit_u64(digest, rules.instrument.generation);
    commit_str(digest, &rules.instrument.price_tick.value().to_string());
    commit_str(digest, &rules.instrument.quantity_step.to_string());
    commit_str(digest, &rules.minimum_quantity.to_string());
    commit_str(digest, rules.instrument.minimum_notional.asset.as_str());
    commit_str(digest, &rules.instrument.minimum_notional.value.to_string());
    commit_u64(digest, initial_fills_cursor.observed_through_ms);
    commit_u64(
        digest,
        initial_fills_cursor.last_trade_id.unwrap_or_default(),
    );
    commit_u64(
        digest,
        initial_fills_cursor.last_event_time_ms.unwrap_or_default(),
    );
    commit_u64(digest, fills_target_through_ms);
    for surface in [
        BinancePrivateSurface::Account,
        BinancePrivateSurface::AccountConfig,
        BinancePrivateSurface::PositionMode,
        BinancePrivateSurface::Positions,
        BinancePrivateSurface::RegularOrders,
        BinancePrivateSurface::AlgoOrders,
        BinancePrivateSurface::Fills,
    ] {
        commit_bytes(digest, &[surface as u8]);
    }
}

fn scope_commitment(
    mode: GatewayMode,
    trading_account_id: &str,
    endpoint_scope: &BinanceRecoveryEndpointScope,
    input: &BinanceRecoveryScopeInput,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    commit_bytes(&mut digest, b"venue-binance-recovery-scope-v1");
    commit_bytes(
        &mut digest,
        &[match mode {
            GatewayMode::Live => 2,
        }],
    );
    commit_str(&mut digest, trading_account_id);
    for value in [
        endpoint_scope.account_binding,
        endpoint_scope.portfolio_rest_origin,
        endpoint_scope.usd_m_public_rest_origin,
        endpoint_scope.public_stream_origin,
        endpoint_scope.private_stream_origin,
    ] {
        commit_str(&mut digest, value);
    }
    commit_str(&mut digest, &input.config_digest);
    for value in [
        input.config_epoch,
        input.recovered_private_generation,
        input.private_generation,
        input.attempt_id,
        input.started_at_ms,
        input.deadline_at_ms,
        u64::try_from(input.maximum_total_bytes).unwrap_or(u64::MAX),
        u64::from(input.maximum_total_pages),
    ] {
        commit_u64(&mut digest, value);
    }
    commit_bytes(&mut digest, input.runtime_commitments.owner());
    commit_bytes(&mut digest, input.runtime_commitments.wal());
    commit_bytes(&mut digest, input.runtime_commitments.unknown());
    commit_bytes(
        &mut digest,
        input.runtime_commitments.runtime_scope_sha256(),
    );
    for symbol in &input.symbol_universe {
        commit_str(&mut digest, &symbol.to_string());
    }
    digest.finalize().into()
}

struct BinanceRecoveryEndpointScope {
    account_binding: &'static str,
    portfolio_rest_origin: &'static str,
    usd_m_public_rest_origin: &'static str,
    public_stream_origin: &'static str,
    private_stream_origin: &'static str,
}

impl BinanceRecoveryEndpointScope {
    const fn from_config(config: &BinanceConfig) -> Self {
        Self {
            account_binding: config.account_binding().as_str(),
            portfolio_rest_origin: config.portfolio_rest_origin(),
            usd_m_public_rest_origin: config.usd_m_public_rest_origin(),
            public_stream_origin: config.public_stream_origin(),
            private_stream_origin: config.private_stream_origin(),
        }
    }
}

fn commit_page(digest: &mut Sha256, page: &BinanceRawPrivatePage) {
    commit_bytes(digest, &[page.surface as u8]);
    commit_u64(digest, u64::from(page.page_index));
    commit_u64(digest, page.requested_at_ms);
    commit_u64(digest, page.received_at_ms);
    for (key, value) in page.request_parameters() {
        commit_str(digest, key);
        commit_str(digest, value);
    }
    commit_bytes(digest, &page.payload);
}

fn commit_custody(digest: &mut Sha256, custody: &BinanceRecoveryOrderCustody) {
    match custody {
        BinanceRecoveryOrderCustody::ExactOwner {
            family,
            venue_order_id,
            client_order_id,
            owner,
        } => {
            commit_bytes(digest, &[1, family_tag(*family)]);
            commit_str(digest, venue_order_id);
            commit_str(digest, client_order_id);
            commit_owner(digest, owner);
        }
        BinanceRecoveryOrderCustody::Unknown {
            family,
            venue_order_id,
            client_order_id,
            reason,
        } => {
            commit_bytes(
                digest,
                &[2, family_tag(*family), unknown_reason_tag(*reason)],
            );
            commit_str(digest, venue_order_id);
            commit_str(digest, client_order_id.as_deref().unwrap_or_default());
        }
    }
}

fn commit_owner(digest: &mut Sha256, owner: &OrderOwner) {
    for value in [
        owner.strategy_instance_id.as_str(),
        owner.run_id.as_str(),
        owner.exchange.as_str(),
        owner.account.as_str(),
        &owner.symbol.to_string(),
    ] {
        commit_str(digest, value);
    }
    commit_bytes(digest, &[purpose_tag(owner.purpose)]);
}

fn owner_route_key(route: &BinanceRecoveryOwnerRoute) -> (u8, &str, &str) {
    (
        family_tag(route.family),
        route.venue_order_id.as_str(),
        route.client_order_id.as_str(),
    )
}

fn custody_key(custody: &BinanceRecoveryOrderCustody) -> (u8, &str, &str) {
    match custody {
        BinanceRecoveryOrderCustody::ExactOwner {
            family,
            venue_order_id,
            client_order_id,
            ..
        } => (family_tag(*family), venue_order_id, client_order_id),
        BinanceRecoveryOrderCustody::Unknown {
            family,
            venue_order_id,
            client_order_id,
            ..
        } => (
            family_tag(*family),
            venue_order_id,
            client_order_id.as_deref().unwrap_or_default(),
        ),
    }
}

const fn custody_family(custody: &BinanceRecoveryOrderCustody) -> NativeOrderFamily {
    match custody {
        BinanceRecoveryOrderCustody::ExactOwner { family, .. }
        | BinanceRecoveryOrderCustody::Unknown { family, .. } => *family,
    }
}

const fn face_family(face: BinanceRecoveryFace) -> NativeOrderFamily {
    match face {
        BinanceRecoveryFace::UmOrder => NativeOrderFamily::UmOrder,
        BinanceRecoveryFace::UmConditional => NativeOrderFamily::UmConditional,
        BinanceRecoveryFace::UmAlgo => NativeOrderFamily::UmAlgo,
        BinanceRecoveryFace::Account
        | BinanceRecoveryFace::Positions
        | BinanceRecoveryFace::FillsCursor => NativeOrderFamily::UmOrder,
    }
}

const fn family_tag(family: NativeOrderFamily) -> u8 {
    match family {
        NativeOrderFamily::UmOrder => 1,
        NativeOrderFamily::UmConditional => 2,
        NativeOrderFamily::UmAlgo => 3,
    }
}

const fn unknown_reason_tag(reason: BinanceRecoveryUnknownReason) -> u8 {
    match reason {
        BinanceRecoveryUnknownReason::MissingClientIdentity => 1,
        BinanceRecoveryUnknownReason::MissingOwnerRoute => 2,
        BinanceRecoveryUnknownReason::ConflictingOwnerIdentity => 3,
    }
}

const fn purpose_tag(purpose: OrderPurpose) -> u8 {
    match purpose {
        OrderPurpose::Entry => 1,
        OrderPurpose::Protection => 2,
        OrderPurpose::TakeProfit => 3,
        OrderPurpose::Reduce => 4,
        OrderPurpose::ExposureTakeProfit => 5,
    }
}

fn valid_config_digest(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
}

fn commit_str(digest: &mut Sha256, value: &str) {
    commit_bytes(digest, value.as_bytes());
}

fn commit_u64(digest: &mut Sha256, value: u64) {
    commit_bytes(digest, &value.to_be_bytes());
}

fn commit_bytes(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BinanceRecoveryCollectorError {
    #[error("Binance recovery scope is incomplete or invalid")]
    Scope,
    #[error("Binance recovery Runtime scope, Owner, WAL, and Unknown commitments must be nonzero")]
    RuntimeCommitment,
    #[error("Binance recovery Runtime scope changed during an authenticated collection await")]
    RuntimeScopeDrift,
    #[error("Binance recovery Owner route is invalid, ambiguous, or out of scope")]
    OwnerRoute,
    #[error("Binance recovery symbol universe is incomplete or duplicated")]
    SymbolUniverse,
    #[error("Binance recovery pages crossed attempt, generation, binding, or deadline")]
    AttemptDrift,
    #[error("Binance recovery raw response replay is incomplete or invalid")]
    Replay,
    #[error("Binance recovery request universe is incomplete, duplicated, or inconsistent")]
    RequestUniverse,
    #[error("Binance recovery production transport does not use the fixed LIVE endpoint")]
    TransportEndpoint,
    #[error("Binance recovery could not establish a session from a real signed Account GET")]
    Authentication,
    #[error("Binance recovery signed read transport failed after authentication")]
    TransportRead,
    #[error(
        "Binance recovery authenticated session seal, transport instance, or generation drifted"
    )]
    SessionDrift,
    #[error("Binance recovery exceeded its frozen total response-size budget")]
    SizeLimit,
    #[error("Binance recovery exceeded its frozen page budget")]
    PageLimit,
    #[error("Binance recovery fills cursor is invalid, regressed, or incomplete")]
    Cursor,
    #[error("Binance recovery clock is invalid or regressed")]
    Clock,
    #[error("Binance recovery account projections disagree across symbols")]
    ProjectionCommitment,
    #[error("Binance recovery candidate was relabelled under another scope")]
    Relabelled,
    #[error("Binance recovery contains an unknown or unmanaged regular/Algo order")]
    UnmanagedOrder,
    #[error("Binance recovery candidate is stale or outside its collection deadline")]
    Expired,
}

impl From<crate::private::RecentFillsPaginationError> for BinanceRecoveryCollectorError {
    fn from(_: crate::private::RecentFillsPaginationError) -> Self {
        Self::Cursor
    }
}

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod tests;
