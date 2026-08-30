use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::Value;
use sha2::{Digest, Sha256};
use venue_domain::domain::{AccountBalance, CommandId, FieldState, OrderOwner, Symbol};
use venue_gateway_api::{GatewayMode, VenueId};

use crate::{
    GATE_PRIVATE_PAGE_LIMIT, GATE_STAGE7_ORDER_PROFILE_VERSION, GateAuthenticatedRecoverySession,
    GateConfig, GateContractRules, GateCredentials, GateFillsCursor, GateGatewayBinding,
    GateHttpTransport, GatePreparedPrivateRead, GatePrivateReadError, GatePrivateReadSource,
    GatePrivateReadbackCandidate, GatePrivateWsTransport, GateRawPrivateResponse,
    GateRuntimeRecoveryAwaitGuard, GateRuntimeRecoveryBundle, GateRuntimeRecoveryRevalidator,
    GateRuntimeRecoveryScope, GateTransportError, prepare_private_read, validate_private_readback,
};

const MAX_CONFIG_DIGEST_LEN: usize = 128;
const MAX_COLLECTION_WINDOW_MS: u64 = 3_000;

/// Opaque recovery roots frozen before any Gate request is prepared. These values do not open a
/// journal, acquire a writer, or prove mutation authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateRecoveryAuthorityRoots {
    owner: [u8; 32],
    wal: [u8; 32],
    unknown: [u8; 32],
}

impl GateRecoveryAuthorityRoots {
    /// Leaves durable authority roots intentionally unbound at the adapter collection layer.
    /// Runtime/Node admission must bind and verify those roots before installing this candidate.
    #[must_use]
    pub const fn unbound() -> Self {
        Self {
            owner: [0; 32],
            wal: [0; 32],
            unknown: [0; 32],
        }
    }

    pub fn verified(
        owner: [u8; 32],
        wal: [u8; 32],
        unknown: [u8; 32],
    ) -> Result<Self, GateFreshRecoveryError> {
        if [owner, wal, unknown].iter().any(is_zero_digest) {
            return Err(GateFreshRecoveryError::AuthorityRoot);
        }
        Ok(Self {
            owner,
            wal,
            unknown,
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
}

/// Account facts frozen at the beginning of exactly one bounded collection attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateRecoveryCollectionStart {
    pub mode: GatewayMode,
    pub trading_account_id: String,
    pub config_digest: String,
    pub config_epoch: u64,
    pub connection_generation: u64,
    pub recovered_private_generation: u64,
    pub attempt_id: u64,
    pub started_at_ms: u64,
    pub deadline_at_ms: u64,
    pub authority_roots: GateRecoveryAuthorityRoots,
}

/// Per-symbol rules and fill cursor captured before collection. The binding remains immutable and
/// must share the account/mode selected by [`GateRecoveryCollectionStart`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateRecoverySymbolScope {
    binding: GateGatewayBinding,
    rules: GateContractRules,
    fills_cursor: GateFillsCursor,
}

impl GateRecoverySymbolScope {
    pub fn verified(
        binding: GateGatewayBinding,
        rules: GateContractRules,
        fills_cursor: GateFillsCursor,
    ) -> Result<Self, GateFreshRecoveryError> {
        if binding.gateway_binding().symbol != rules.instrument.symbol
            || rules.instrument.generation == 0
            || rules.instrument.validate().is_err()
            || rules.native_symbol.trim().is_empty()
        {
            return Err(GateFreshRecoveryError::SymbolScope);
        }
        Ok(Self {
            binding,
            rules,
            fills_cursor,
        })
    }

    #[must_use]
    pub const fn binding(&self) -> &GateGatewayBinding {
        &self.binding
    }

    #[must_use]
    pub const fn rules(&self) -> &GateContractRules {
        &self.rules
    }

    #[must_use]
    pub const fn fills_cursor(&self) -> &GateFillsCursor {
        &self.fills_cursor
    }
}

/// Exact regular-order Owner projection recovered before network collection starts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateRecoveryOwnerRoute {
    client_order_id: CommandId,
    venue_order_id: String,
    owner: OrderOwner,
}

impl GateRecoveryOwnerRoute {
    pub fn verified(
        client_order_id: CommandId,
        venue_order_id: impl Into<String>,
        owner: OrderOwner,
    ) -> Result<Self, GateFreshRecoveryError> {
        let venue_order_id = venue_order_id.into();
        owner
            .validate()
            .map_err(|_| GateFreshRecoveryError::OwnerRoute)?;
        if owner.exchange != VenueId::Gate.as_str() || !valid_native_id(&venue_order_id) {
            return Err(GateFreshRecoveryError::OwnerRoute);
        }
        Ok(Self {
            client_order_id,
            venue_order_id,
            owner,
        })
    }

    #[must_use]
    pub const fn client_order_id(&self) -> &CommandId {
        &self.client_order_id
    }

    #[must_use]
    pub fn venue_order_id(&self) -> &str {
        &self.venue_order_id
    }

    #[must_use]
    pub const fn owner(&self) -> &OrderOwner {
        &self.owner
    }
}

/// Immutable account scope committed into every prepared read and returned candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateRecoveryScope {
    mode: GatewayMode,
    rest_origin: &'static str,
    private_ws_endpoint: &'static str,
    trading_account_id: String,
    config_digest: String,
    config_epoch: u64,
    connection_generation: u64,
    recovered_private_generation: u64,
    private_generation: u64,
    attempt_id: u64,
    started_at_ms: u64,
    deadline_at_ms: u64,
    authority_roots: GateRecoveryAuthorityRoots,
    symbol_universe: Vec<Symbol>,
    request_universe_sha256: [u8; 32],
    runtime_scope_sha256: Option<[u8; 32]>,
    commitment_sha256: [u8; 32],
}

impl GateRecoveryScope {
    #[must_use]
    pub const fn mode(&self) -> GatewayMode {
        self.mode
    }

    #[must_use]
    pub const fn rest_origin(&self) -> &'static str {
        self.rest_origin
    }

    #[must_use]
    pub const fn private_ws_endpoint(&self) -> &'static str {
        self.private_ws_endpoint
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
    pub const fn connection_generation(&self) -> u64 {
        self.connection_generation
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
    pub const fn authority_roots(&self) -> &GateRecoveryAuthorityRoots {
        &self.authority_roots
    }

    #[must_use]
    pub fn symbol_universe(&self) -> &[Symbol] {
        &self.symbol_universe
    }

    #[must_use]
    pub const fn request_universe_sha256(&self) -> &[u8; 32] {
        &self.request_universe_sha256
    }

    #[must_use]
    pub const fn runtime_scope_sha256(&self) -> Option<&[u8; 32]> {
        self.runtime_scope_sha256.as_ref()
    }

    #[must_use]
    pub const fn commitment_sha256(&self) -> &[u8; 32] {
        &self.commitment_sha256
    }
}

/// Scope-bound request wrapper. The embedded request can be signed and sent, but the wrapper must
/// be retained so the response cannot later be attached to a different generation/root scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateRecoveryPreparedRead {
    recovery_scope_sha256: [u8; 32],
    symbol: Symbol,
    request: GatePreparedPrivateRead,
    rest_url: String,
}

impl GateRecoveryPreparedRead {
    #[must_use]
    pub const fn request(&self) -> &GatePreparedPrivateRead {
        &self.request
    }

    #[must_use]
    pub fn rest_url(&self) -> &str {
        &self.rest_url
    }

    #[must_use]
    pub const fn symbol(&self) -> &Symbol {
        &self.symbol
    }
}

/// Raw signed response that carries the immutable recovery-scope commitment from its prepared
/// request. Its fields are deliberately private to prevent response relabelling.
#[derive(Clone, Eq, PartialEq)]
pub struct GateFreshRecoveryRawResponse {
    recovery_scope_sha256: [u8; 32],
    symbol: Symbol,
    raw: GateRawPrivateResponse,
}

impl std::fmt::Debug for GateFreshRecoveryRawResponse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GateFreshRecoveryRawResponse")
            .field("recovery_scope_sha256", &self.recovery_scope_sha256)
            .field("symbol", &self.symbol)
            .field("raw", &self.raw)
            .finish()
    }
}

impl GateFreshRecoveryRawResponse {
    /// Returns the exact continuation cursor for a full regular/fills page, or `None` only for a
    /// terminal short page. Callers remain responsible for the exported hard page-count bound.
    pub fn next_page_cursor(&self) -> Result<Option<GateFillsCursor>, GateFreshRecoveryError> {
        if !matches!(
            self.raw.source,
            GatePrivateReadSource::RegularOrders | GatePrivateReadSource::Fills
        ) {
            return Err(GateFreshRecoveryError::Cursor);
        }
        let value: Value = serde_json::from_str(&self.raw.payload)
            .map_err(|_| GateFreshRecoveryError::PrivateRead(GatePrivateReadError::Payload))?;
        let rows = value.as_array().ok_or(GateFreshRecoveryError::PrivateRead(
            GatePrivateReadError::Payload,
        ))?;
        if rows.len() < GATE_PRIVATE_PAGE_LIMIT {
            return Ok(None);
        }
        if rows.len() != GATE_PRIVATE_PAGE_LIMIT {
            return Err(GateFreshRecoveryError::Cursor);
        }
        let id = rows
            .last()
            .and_then(|row| row.get("id"))
            .and_then(|value| match value {
                Value::String(value) => Some(value.clone()),
                Value::Number(value) => Some(value.to_string()),
                _ => None,
            })
            .ok_or(GateFreshRecoveryError::Cursor)?;
        Ok(Some(GateFillsCursor::new(Some(id))?))
    }

    fn from_authenticated_response(
        prepared: &GateRecoveryPreparedRead,
        raw: GateRawPrivateResponse,
    ) -> Result<Self, GateFreshRecoveryError> {
        if raw.binding != prepared.request.binding
            || raw.generation != prepared.request.generation
            || raw.attempt != prepared.request.attempt
            || raw.source != prepared.request.source
            || raw.endpoint != prepared.request.endpoint
            || raw.query != prepared.request.query
            || raw.cursor_before != prepared.request.cursor_before
        {
            return Err(GateFreshRecoveryError::ScopeDrift);
        }
        Ok(Self {
            recovery_scope_sha256: prepared.recovery_scope_sha256,
            symbol: prepared.symbol.clone(),
            raw,
        })
    }

    #[cfg(test)]
    pub fn from_response(
        prepared: &GateRecoveryPreparedRead,
        symbol_scope: &GateRecoverySymbolScope,
        requested_at_ms: u64,
        received_at_ms: u64,
        payload: String,
    ) -> Result<Self, GateFreshRecoveryError> {
        if prepared.symbol != symbol_scope.binding.gateway_binding().symbol {
            return Err(GateFreshRecoveryError::ScopeDrift);
        }
        let raw = GateRawPrivateResponse::from_response(
            &symbol_scope.binding,
            &symbol_scope.rules,
            &prepared.request,
            requested_at_ms,
            received_at_ms,
            payload,
        )?;
        Self::from_authenticated_response(prepared, raw)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum GateRecoverySurface {
    Account,
    Positions,
    RegularOrders,
    ConditionalOrders,
    AlgoOrders,
    FillsCursor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GateRecoveryCoverage {
    Complete { record_count: u64 },
    Unsupported { profile_version: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateRecoverySurfaceCommitment {
    surface: GateRecoverySurface,
    coverage: GateRecoveryCoverage,
    raw_commitment_sha256: [u8; 32],
}

impl GateRecoverySurfaceCommitment {
    #[must_use]
    pub const fn surface(&self) -> GateRecoverySurface {
        self.surface
    }

    #[must_use]
    pub const fn coverage(&self) -> &GateRecoveryCoverage {
        &self.coverage
    }

    #[must_use]
    pub const fn raw_commitment_sha256(&self) -> &[u8; 32] {
        &self.raw_commitment_sha256
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateOwnedOpenOrder {
    pub symbol: Symbol,
    pub client_order_id: CommandId,
    pub venue_order_id: String,
    pub owner: OrderOwner,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GateUnknownOpenOrderReason {
    ClientIdentityUnavailable,
    OwnerRouteMissing,
    NativeIdentityMismatch,
    DuplicateNativeIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateUnknownOpenOrder {
    pub symbol: Symbol,
    pub venue_order_id: String,
    pub client_order_id: Option<String>,
    pub reason: GateUnknownOpenOrderReason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateRecoverySymbolReadback {
    symbol: Symbol,
    candidate: GatePrivateReadbackCandidate,
}

impl GateRecoverySymbolReadback {
    #[must_use]
    pub const fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    #[must_use]
    pub const fn candidate(&self) -> &GatePrivateReadbackCandidate {
        &self.candidate
    }
}

/// Fresh, scope-bound six-surface Gate recovery candidate. It grants no capability, writer, WAL,
/// private connection, or dispatch handle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateFreshRecoveryCandidate {
    scope: GateRecoveryScope,
    account: AccountBalance,
    symbol_readbacks: Vec<GateRecoverySymbolReadback>,
    surfaces: BTreeMap<GateRecoverySurface, GateRecoverySurfaceCommitment>,
    owned_open_orders: Vec<GateOwnedOpenOrder>,
    unknown_open_orders: Vec<GateUnknownOpenOrder>,
    commitment_sha256: [u8; 32],
}

impl GateFreshRecoveryCandidate {
    #[must_use]
    pub const fn scope(&self) -> &GateRecoveryScope {
        &self.scope
    }

    #[must_use]
    pub const fn account(&self) -> &AccountBalance {
        &self.account
    }

    #[must_use]
    pub fn symbol_readbacks(&self) -> &[GateRecoverySymbolReadback] {
        &self.symbol_readbacks
    }

    #[must_use]
    pub fn surface(&self, surface: GateRecoverySurface) -> Option<&GateRecoverySurfaceCommitment> {
        self.surfaces.get(&surface)
    }

    #[must_use]
    pub fn owned_open_orders(&self) -> &[GateOwnedOpenOrder] {
        &self.owned_open_orders
    }

    #[must_use]
    pub fn unknown_open_orders(&self) -> &[GateUnknownOpenOrder] {
        &self.unknown_open_orders
    }

    #[must_use]
    pub const fn commitment_sha256(&self) -> &[u8; 32] {
        &self.commitment_sha256
    }
}

/// Single-use collection planner. `finish` consumes the collector, so an old validated probe
/// cannot be reissued under a new scope; only raw responses prepared by this exact scope enter it.
pub struct GateFreshRecoveryCollector {
    scope: GateRecoveryScope,
    symbols: BTreeMap<Symbol, GateRecoverySymbolScope>,
    owner_routes: BTreeMap<(Symbol, String), GateRecoveryOwnerRoute>,
    authenticated_session: Option<GateAuthenticatedRecoverySession>,
    runtime_scope: Option<GateRuntimeRecoveryScope>,
}

impl GateFreshRecoveryCollector {
    /// Caller-only recovery facts never establish an authenticated collection session.
    pub fn start<I, J>(
        start: GateRecoveryCollectionStart,
        symbols: I,
        owner_routes: J,
    ) -> Result<Self, GateFreshRecoveryError>
    where
        I: IntoIterator<Item = GateRecoverySymbolScope>,
        J: IntoIterator<Item = GateRecoveryOwnerRoute>,
    {
        let _ = (start, symbols.into_iter(), owner_routes.into_iter());
        Err(GateFreshRecoveryError::AuthenticatedSessionRequired)
    }

    /// Starts one production read-only attempt from a live, crate-issued private-session seal.
    ///
    /// The sealed session is the sole source of generations, attempt, deadline, rules, cursors,
    /// request universe, and budgets. A runtime-bound session additionally carries its immutable
    /// registry, durable-root, Owner, and structured-Unknown scope.
    pub fn start_authenticated(
        authenticated_session: GateAuthenticatedRecoverySession,
    ) -> Result<Self, GateFreshRecoveryError> {
        authenticated_session.validate_current()?;
        let runtime_scope = authenticated_session.runtime_scope().cloned();
        let recovered_private_generation = runtime_scope
            .as_ref()
            .map_or_else(
                || authenticated_session.private_generation().checked_sub(1),
                |scope| Some(scope.recovered_private_generation()),
            )
            .ok_or(GateFreshRecoveryError::Generation)?;
        let start = GateRecoveryCollectionStart {
            mode: authenticated_session.mode(),
            trading_account_id: authenticated_session.trading_account_id().to_owned(),
            config_digest: runtime_scope.as_ref().map_or_else(
                || "gate_authenticated_recovery_v1".to_owned(),
                |scope| scope.config_digest().to_owned(),
            ),
            config_epoch: runtime_scope.as_ref().map_or_else(
                || authenticated_session.collection_epoch(),
                GateRuntimeRecoveryScope::config_epoch,
            ),
            connection_generation: runtime_scope.as_ref().map_or_else(
                || authenticated_session.connection_generation(),
                GateRuntimeRecoveryScope::connection_generation,
            ),
            recovered_private_generation,
            attempt_id: authenticated_session.attempt_id(),
            started_at_ms: authenticated_session.started_at_ms(),
            deadline_at_ms: authenticated_session.deadline_at_ms(),
            authority_roots: runtime_scope
                .as_ref()
                .map_or_else(GateRecoveryAuthorityRoots::unbound, |scope| {
                    scope.authority_roots().clone()
                }),
        };
        let symbols = authenticated_session.symbol_scopes().collect::<Vec<_>>();
        let owner_routes = runtime_scope
            .as_ref()
            .map(|scope| scope.owner_routes().to_vec())
            .unwrap_or_default();
        let mut collector = Self::start_verified(start.clone(), symbols, owner_routes)?;
        if (!cfg!(test)
            && (collector.scope.rest_origin != authenticated_session.rest_origin()
                || collector.scope.private_ws_endpoint
                    != authenticated_session.private_ws_endpoint()))
            || collector.scope.private_generation != authenticated_session.private_generation()
        {
            return Err(GateFreshRecoveryError::AuthenticatedSessionEndpoint);
        }
        collector.scope.request_universe_sha256 = *authenticated_session.request_universe_sha256();
        collector.scope.runtime_scope_sha256 = runtime_scope
            .as_ref()
            .map(|scope| *scope.commitment_sha256());
        collector.scope.commitment_sha256 = scope_commitment(
            &start,
            collector.scope.rest_origin,
            collector.scope.private_ws_endpoint,
            collector.scope.private_generation,
            &collector.scope.symbol_universe,
            &collector.scope.request_universe_sha256,
            collector.scope.runtime_scope_sha256.as_ref(),
        );
        collector.authenticated_session = Some(authenticated_session);
        collector.runtime_scope = runtime_scope;
        Ok(collector)
    }

    /// Collects every Gate recovery face in the frozen authenticated universe. Regular orders and
    /// fills are followed through their terminal short page; the session enforces the single
    /// global page/byte budget and rejects any cursor substitution between awaits.
    ///
    /// This is evidence collection only. The returned candidate does not expose a writer, WAL,
    /// capability, or mutation handle.
    pub async fn collect_authenticated<S>(
        authenticated_session: GateAuthenticatedRecoverySession,
        transport: &GateHttpTransport,
        private_ws: &mut GatePrivateWsTransport<S>,
        credentials: &GateCredentials,
    ) -> Result<GateFreshRecoveryCandidate, GateFreshRecoveryError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let collector = Self::start_authenticated(authenticated_session)?;
        let result = collector
            .collect_authenticated_responses(transport, private_ws, credentials)
            .await;
        let responses = match result {
            Ok(responses) => responses,
            Err(error) => {
                collector.revoke_authenticated_session();
                return Err(error);
            }
        };
        let validated_at_ms = match unix_ms() {
            Ok(value) => value,
            Err(error) => {
                collector.revoke_authenticated_session();
                return Err(error);
            }
        };
        collector.finish(validated_at_ms, responses)
    }

    /// Collects a runtime-bound Gate recovery bundle. The runtime commitment is revalidated
    /// before and after every private-WebSocket and HTTP await; any drift revokes the session.
    pub async fn collect_runtime_authenticated<S>(
        authenticated_session: GateAuthenticatedRecoverySession,
        transport: &GateHttpTransport,
        private_ws: &mut GatePrivateWsTransport<S>,
        credentials: &GateCredentials,
        revalidator: &dyn GateRuntimeRecoveryRevalidator,
    ) -> Result<GateRuntimeRecoveryBundle, GateFreshRecoveryError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let collector = Self::start_authenticated(authenticated_session)?;
        if collector.runtime_scope.is_none() {
            collector.revoke_authenticated_session();
            return Err(GateFreshRecoveryError::RuntimeScopeRequired);
        }
        let result = collector
            .collect_runtime_authenticated_responses(
                transport,
                private_ws,
                credentials,
                revalidator,
            )
            .await;
        let responses = match result {
            Ok(responses) => responses,
            Err(error) => {
                collector.revoke_authenticated_session();
                return Err(error);
            }
        };
        let validated_at_ms = match unix_ms() {
            Ok(value) => value,
            Err(error) => {
                collector.revoke_authenticated_session();
                return Err(error);
            }
        };
        collector.finish_runtime(validated_at_ms, responses, revalidator)
    }

    async fn collect_authenticated_responses<S>(
        &self,
        transport: &GateHttpTransport,
        private_ws: &mut GatePrivateWsTransport<S>,
        credentials: &GateCredentials,
    ) -> Result<Vec<GateFreshRecoveryRawResponse>, GateFreshRecoveryError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let symbols = self.scope.symbol_universe.clone();
        let mut responses = Vec::new();
        for symbol in symbols {
            for source in [
                GatePrivateReadSource::Account,
                GatePrivateReadSource::DualPositions,
            ] {
                let prepared = self.prepare_read(&symbol, source, GateFillsCursor::default())?;
                responses.push(
                    self.execute_read(transport, private_ws, credentials, &prepared)
                        .await?,
                );
            }
            for source in [
                GatePrivateReadSource::RegularOrders,
                GatePrivateReadSource::Fills,
            ] {
                let mut cursor = match source {
                    GatePrivateReadSource::RegularOrders => GateFillsCursor::default(),
                    GatePrivateReadSource::Fills => self
                        .symbols
                        .get(&symbol)
                        .ok_or(GateFreshRecoveryError::SymbolUniverse)?
                        .fills_cursor
                        .clone(),
                    GatePrivateReadSource::Account | GatePrivateReadSource::DualPositions => {
                        return Err(GateFreshRecoveryError::ScopeDrift);
                    }
                };
                loop {
                    let prepared = self.prepare_read(&symbol, source, cursor)?;
                    let response = self
                        .execute_read(transport, private_ws, credentials, &prepared)
                        .await?;
                    let next = response.next_page_cursor()?;
                    responses.push(response);
                    let Some(next) = next else {
                        break;
                    };
                    cursor = next;
                }
            }
        }
        Ok(responses)
    }

    async fn collect_runtime_authenticated_responses<S>(
        &self,
        transport: &GateHttpTransport,
        private_ws: &mut GatePrivateWsTransport<S>,
        credentials: &GateCredentials,
        revalidator: &dyn GateRuntimeRecoveryRevalidator,
    ) -> Result<Vec<GateFreshRecoveryRawResponse>, GateFreshRecoveryError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let symbols = self.scope.symbol_universe.clone();
        let mut responses = Vec::new();
        for symbol in symbols {
            for source in [
                GatePrivateReadSource::Account,
                GatePrivateReadSource::DualPositions,
            ] {
                let prepared = self.prepare_read(&symbol, source, GateFillsCursor::default())?;
                responses.push(
                    self.execute_runtime_read(
                        transport,
                        private_ws,
                        credentials,
                        &prepared,
                        revalidator,
                    )
                    .await?,
                );
            }
            for source in [
                GatePrivateReadSource::RegularOrders,
                GatePrivateReadSource::Fills,
            ] {
                let mut cursor = match source {
                    GatePrivateReadSource::RegularOrders => GateFillsCursor::default(),
                    GatePrivateReadSource::Fills => self
                        .symbols
                        .get(&symbol)
                        .ok_or(GateFreshRecoveryError::SymbolUniverse)?
                        .fills_cursor
                        .clone(),
                    GatePrivateReadSource::Account | GatePrivateReadSource::DualPositions => {
                        return Err(GateFreshRecoveryError::ScopeDrift);
                    }
                };
                loop {
                    let prepared = self.prepare_read(&symbol, source, cursor)?;
                    let response = self
                        .execute_runtime_read(
                            transport,
                            private_ws,
                            credentials,
                            &prepared,
                            revalidator,
                        )
                        .await?;
                    let next = response.next_page_cursor()?;
                    responses.push(response);
                    let Some(next) = next else {
                        break;
                    };
                    cursor = next;
                }
            }
        }
        Ok(responses)
    }

    fn revoke_authenticated_session(&self) {
        if let Some(session) = &self.authenticated_session {
            session.revoke();
        }
    }

    #[cfg(test)]
    fn start_fixture<I, J>(
        start: GateRecoveryCollectionStart,
        symbols: I,
        owner_routes: J,
    ) -> Result<Self, GateFreshRecoveryError>
    where
        I: IntoIterator<Item = GateRecoverySymbolScope>,
        J: IntoIterator<Item = GateRecoveryOwnerRoute>,
    {
        Self::start_verified(start, symbols, owner_routes)
    }

    fn start_verified<I, J>(
        start: GateRecoveryCollectionStart,
        symbols: I,
        owner_routes: J,
    ) -> Result<Self, GateFreshRecoveryError>
    where
        I: IntoIterator<Item = GateRecoverySymbolScope>,
        J: IntoIterator<Item = GateRecoveryOwnerRoute>,
    {
        validate_start(&start)?;
        let config = GateConfig::for_mode(start.mode);
        let mut by_symbol = BTreeMap::new();
        for symbol_scope in symbols {
            let binding = symbol_scope.binding.gateway_binding();
            if binding.mode != start.mode
                || binding.trading_account_id != start.trading_account_id
                || binding.symbol != symbol_scope.rules.instrument.symbol
                || by_symbol
                    .insert(binding.symbol.clone(), symbol_scope)
                    .is_some()
            {
                return Err(GateFreshRecoveryError::SymbolScope);
            }
        }
        if by_symbol.is_empty() {
            return Err(GateFreshRecoveryError::SymbolUniverse);
        }
        let symbol_universe = by_symbol.keys().cloned().collect::<Vec<_>>();
        let private_generation = start
            .recovered_private_generation
            .checked_add(1)
            .ok_or(GateFreshRecoveryError::Generation)?;
        let commitment_sha256 = scope_commitment(
            &start,
            config.rest_origin(),
            config.usdt_futures_ws(),
            private_generation,
            &symbol_universe,
            &[0; 32],
            None,
        );
        let scope = GateRecoveryScope {
            mode: start.mode,
            rest_origin: config.rest_origin(),
            private_ws_endpoint: config.usdt_futures_ws(),
            trading_account_id: start.trading_account_id,
            config_digest: start.config_digest,
            config_epoch: start.config_epoch,
            connection_generation: start.connection_generation,
            recovered_private_generation: start.recovered_private_generation,
            private_generation,
            attempt_id: start.attempt_id,
            started_at_ms: start.started_at_ms,
            deadline_at_ms: start.deadline_at_ms,
            authority_roots: start.authority_roots,
            symbol_universe,
            request_universe_sha256: [0; 32],
            runtime_scope_sha256: None,
            commitment_sha256,
        };

        let mut routes = BTreeMap::new();
        for route in owner_routes {
            let symbol = route.owner.symbol.clone();
            if route.owner.account != scope.trading_account_id
                || !by_symbol.contains_key(&symbol)
                || routes
                    .insert((symbol, route.client_order_id.as_str().to_owned()), route)
                    .is_some()
            {
                return Err(GateFreshRecoveryError::OwnerRoute);
            }
        }
        Ok(Self {
            scope,
            symbols: by_symbol,
            owner_routes: routes,
            authenticated_session: None,
            runtime_scope: None,
        })
    }

    #[must_use]
    pub const fn scope(&self) -> &GateRecoveryScope {
        &self.scope
    }

    pub fn prepare_read(
        &self,
        symbol: &Symbol,
        source: GatePrivateReadSource,
        cursor: GateFillsCursor,
    ) -> Result<GateRecoveryPreparedRead, GateFreshRecoveryError> {
        let symbol_scope = self
            .symbols
            .get(symbol)
            .ok_or(GateFreshRecoveryError::SymbolUniverse)?;
        if source == GatePrivateReadSource::Fills
            && cursor.last_native_id().is_none()
            && symbol_scope.fills_cursor.last_native_id().is_some()
        {
            return Err(GateFreshRecoveryError::Cursor);
        }
        let request = prepare_private_read(
            &symbol_scope.binding,
            &symbol_scope.rules,
            symbol_scope.rules.instrument.generation,
            self.scope.attempt_id,
            source,
            cursor,
        )?;
        let rest_url = GateConfig::for_mode(self.scope.mode)
            .rest_url(&request.endpoint)
            .map_err(|_| GateFreshRecoveryError::Endpoint)?;
        if !rest_url.starts_with(self.scope.rest_origin) {
            return Err(GateFreshRecoveryError::Endpoint);
        }
        Ok(GateRecoveryPreparedRead {
            recovery_scope_sha256: self.scope.commitment_sha256,
            symbol: symbol.clone(),
            request,
            rest_url,
        })
    }

    /// Executes one prepared signed GET within the authenticated attempt. The private-session seal
    /// is checked before the network await and again after it returns. The outer deadline bounds the
    /// await independently of the HTTP client's operation timeout.
    pub async fn execute_read<S>(
        &self,
        transport: &GateHttpTransport,
        private_ws: &mut GatePrivateWsTransport<S>,
        credentials: &GateCredentials,
        prepared: &GateRecoveryPreparedRead,
    ) -> Result<GateFreshRecoveryRawResponse, GateFreshRecoveryError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        if self.runtime_scope.is_some() {
            return Err(GateFreshRecoveryError::RuntimeRevalidationRequired);
        }
        let session = self
            .authenticated_session
            .as_ref()
            .ok_or(GateFreshRecoveryError::AuthenticatedSessionRequired)?;
        let result = self
            .execute_read_inner(transport, private_ws, session, credentials, prepared, None)
            .await;
        if result.is_err() {
            session.revoke();
        }
        result
    }

    /// Executes a runtime-bound read and revalidates the frozen runtime commitment after every
    /// network await in the Gate HTTP and private-WebSocket recovery path.
    pub async fn execute_runtime_read<S>(
        &self,
        transport: &GateHttpTransport,
        private_ws: &mut GatePrivateWsTransport<S>,
        credentials: &GateCredentials,
        prepared: &GateRecoveryPreparedRead,
        revalidator: &dyn GateRuntimeRecoveryRevalidator,
    ) -> Result<GateFreshRecoveryRawResponse, GateFreshRecoveryError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let session = self
            .authenticated_session
            .as_ref()
            .ok_or(GateFreshRecoveryError::AuthenticatedSessionRequired)?;
        let runtime_scope = self
            .runtime_scope
            .as_ref()
            .ok_or(GateFreshRecoveryError::RuntimeScopeRequired)?;
        let guard = GateRuntimeRecoveryAwaitGuard::new(runtime_scope, revalidator)?;
        let result = self
            .execute_read_inner(
                transport,
                private_ws,
                session,
                credentials,
                prepared,
                Some(guard),
            )
            .await;
        if result.is_err() {
            session.revoke();
        }
        result
    }

    async fn execute_read_inner<S>(
        &self,
        transport: &GateHttpTransport,
        private_ws: &mut GatePrivateWsTransport<S>,
        session: &GateAuthenticatedRecoverySession,
        credentials: &GateCredentials,
        prepared: &GateRecoveryPreparedRead,
        runtime_guard: Option<GateRuntimeRecoveryAwaitGuard<'_>>,
    ) -> Result<GateFreshRecoveryRawResponse, GateFreshRecoveryError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        self.validate_session_and_transport(session, transport, credentials, prepared)?;
        if let Some(guard) = runtime_guard {
            guard.revalidate()?;
        }
        session.reserve_get(&prepared.request, transport.limits().maximum_body_bytes())?;
        private_ws
            .revalidate_recovery_session_guarded(session, runtime_guard)
            .await?;
        let symbol_scope = self
            .symbols
            .get(&prepared.symbol)
            .ok_or(GateFreshRecoveryError::SymbolUniverse)?;
        let requested_at_ms = unix_ms()?;
        if requested_at_ms < self.scope.started_at_ms
            || requested_at_ms >= self.scope.deadline_at_ms
        {
            return Err(GateFreshRecoveryError::Deadline);
        }
        let remaining = self.scope.deadline_at_ms - requested_at_ms;
        let raw = tokio::time::timeout(
            Duration::from_millis(remaining),
            transport.execute_private_read_guarded(
                &symbol_scope.binding,
                credentials,
                &symbol_scope.rules,
                &prepared.request,
                requested_at_ms,
                runtime_guard,
            ),
        )
        .await
        .map_err(|_| GateFreshRecoveryError::Deadline)??;
        if let Some(guard) = runtime_guard {
            guard.revalidate()?;
        }
        private_ws
            .revalidate_recovery_session_guarded(session, runtime_guard)
            .await?;
        self.validate_session_and_transport(session, transport, credentials, prepared)?;
        let validated_at_ms = unix_ms()?;
        if validated_at_ms >= self.scope.deadline_at_ms
            || raw.received_at_ms >= self.scope.deadline_at_ms
        {
            return Err(GateFreshRecoveryError::Deadline);
        }
        let response = GateFreshRecoveryRawResponse::from_authenticated_response(prepared, raw)?;
        let next_cursor = match prepared.request.source {
            GatePrivateReadSource::RegularOrders | GatePrivateReadSource::Fills => response
                .next_page_cursor()?
                .and_then(|cursor| cursor.last_native_id().map(str::to_owned)),
            GatePrivateReadSource::Account | GatePrivateReadSource::DualPositions => None,
        };
        session.settle_get(&prepared.request, response.raw.payload.len(), next_cursor)?;
        Ok(response)
    }

    fn validate_session_and_transport(
        &self,
        session: &GateAuthenticatedRecoverySession,
        transport: &GateHttpTransport,
        credentials: &GateCredentials,
        prepared: &GateRecoveryPreparedRead,
    ) -> Result<(), GateFreshRecoveryError> {
        session.validate_credentials(credentials)?;
        session.validate_request(&prepared.request, unix_ms()?)?;
        if prepared.recovery_scope_sha256 != self.scope.commitment_sha256
            || prepared.symbol != prepared.request.binding.symbol
            || prepared.request.attempt != self.scope.attempt_id
            || !transport.matches_recovery_session(
                session.mode(),
                session.trading_account_id(),
                session.rest_origin(),
                session.transport_limits(),
                session.request_generation(),
                &prepared.request.binding,
            )
        {
            return Err(GateFreshRecoveryError::AuthenticatedSessionScope);
        }
        Ok(())
    }

    pub fn finish<I>(
        self,
        validated_at_ms: u64,
        responses: I,
    ) -> Result<GateFreshRecoveryCandidate, GateFreshRecoveryError>
    where
        I: IntoIterator<Item = GateFreshRecoveryRawResponse>,
    {
        if self.runtime_scope.is_some() {
            return Err(GateFreshRecoveryError::RuntimeBundleRequired);
        }
        self.finish_inner(validated_at_ms, responses)
    }

    pub fn finish_runtime<I>(
        self,
        validated_at_ms: u64,
        responses: I,
        revalidator: &dyn GateRuntimeRecoveryRevalidator,
    ) -> Result<GateRuntimeRecoveryBundle, GateFreshRecoveryError>
    where
        I: IntoIterator<Item = GateFreshRecoveryRawResponse>,
    {
        let runtime_scope = self
            .runtime_scope
            .clone()
            .ok_or(GateFreshRecoveryError::RuntimeScopeRequired)?;
        GateRuntimeRecoveryAwaitGuard::new(&runtime_scope, revalidator)?.revalidate()?;
        let candidate = self.finish_inner(validated_at_ms, responses)?;
        GateRuntimeRecoveryBundle::from_candidate(runtime_scope, candidate)
    }

    fn finish_inner<I>(
        self,
        validated_at_ms: u64,
        responses: I,
    ) -> Result<GateFreshRecoveryCandidate, GateFreshRecoveryError>
    where
        I: IntoIterator<Item = GateFreshRecoveryRawResponse>,
    {
        if let Some(session) = &self.authenticated_session {
            session.validate_current()?;
        }
        if validated_at_ms < self.scope.started_at_ms
            || validated_at_ms >= self.scope.deadline_at_ms
        {
            return Err(GateFreshRecoveryError::Deadline);
        }
        let mut grouped = BTreeMap::<Symbol, Vec<GateRawPrivateResponse>>::new();
        for response in responses {
            if response.recovery_scope_sha256 != self.scope.commitment_sha256
                || !self.symbols.contains_key(&response.symbol)
                || response.raw.requested_at_ms < self.scope.started_at_ms
                || response.raw.received_at_ms >= self.scope.deadline_at_ms
            {
                return Err(GateFreshRecoveryError::ScopeDrift);
            }
            grouped
                .entry(response.symbol)
                .or_default()
                .push(response.raw);
        }
        if grouped.keys().collect::<BTreeSet<_>>() != self.symbols.keys().collect::<BTreeSet<_>>() {
            return Err(GateFreshRecoveryError::SymbolUniverse);
        }

        let mut account_raw_digest = None;
        let mut account_user_id = None;
        let mut account = None;
        let mut readbacks = Vec::with_capacity(self.symbols.len());
        let mut raw_by_symbol = BTreeMap::new();
        for (symbol, symbol_scope) in &self.symbols {
            let raw = grouped
                .remove(symbol)
                .ok_or(GateFreshRecoveryError::SymbolUniverse)?;
            let account_digest = singleton_raw_digest(&raw, GatePrivateReadSource::Account)?;
            if account_raw_digest.is_some_and(|expected| expected != account_digest) {
                return Err(GateFreshRecoveryError::RawDivergence);
            }
            account_raw_digest = Some(account_digest);
            let candidate = validate_private_readback(
                &symbol_scope.binding,
                &symbol_scope.rules,
                GATE_STAGE7_ORDER_PROFILE_VERSION,
                self.scope.deadline_at_ms,
                validated_at_ms,
                raw.clone(),
            )?;
            if candidate.fills_cursor_before != symbol_scope.fills_cursor {
                return Err(GateFreshRecoveryError::Cursor);
            }
            if account_user_id
                .as_ref()
                .is_some_and(|expected| expected != &candidate.user_id)
            {
                return Err(GateFreshRecoveryError::AccountDivergence);
            }
            account_user_id = Some(candidate.user_id.clone());
            if account
                .as_ref()
                .is_some_and(|expected| expected != &candidate.balance)
            {
                return Err(GateFreshRecoveryError::AccountDivergence);
            }
            account = Some(candidate.balance.clone());
            raw_by_symbol.insert(symbol.clone(), raw);
            readbacks.push(GateRecoverySymbolReadback {
                symbol: symbol.clone(),
                candidate,
            });
        }

        let (owned_open_orders, unknown_open_orders) =
            classify_open_orders(&readbacks, &self.owner_routes)?;
        let surfaces = surface_commitments(&self.scope, &readbacks, &raw_by_symbol)?;
        let commitment_sha256 = candidate_commitment(
            &self.scope,
            &surfaces,
            &owned_open_orders,
            &unknown_open_orders,
        );
        if let Some(session) = &self.authenticated_session {
            if self.runtime_scope.is_none() && !owned_open_orders.is_empty() {
                return Err(GateFreshRecoveryError::OwnerRoute);
            }
            session.commit_collection()?;
        }
        Ok(GateFreshRecoveryCandidate {
            scope: self.scope,
            account: account.ok_or(GateFreshRecoveryError::MissingSurface)?,
            symbol_readbacks: readbacks,
            surfaces,
            owned_open_orders,
            unknown_open_orders,
            commitment_sha256,
        })
    }
}

fn validate_start(start: &GateRecoveryCollectionStart) -> Result<(), GateFreshRecoveryError> {
    if start.trading_account_id.trim().is_empty()
        || start.config_epoch == 0
        || start.connection_generation == 0
        || start.attempt_id == 0
        || start.started_at_ms == 0
        || start.deadline_at_ms <= start.started_at_ms
        || start.deadline_at_ms - start.started_at_ms > MAX_COLLECTION_WINDOW_MS
    {
        return Err(GateFreshRecoveryError::Scope);
    }
    if start.config_digest.is_empty()
        || start.config_digest.len() > MAX_CONFIG_DIGEST_LEN
        || !start
            .config_digest
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(GateFreshRecoveryError::Configuration);
    }
    Ok(())
}

fn classify_open_orders(
    readbacks: &[GateRecoverySymbolReadback],
    routes: &BTreeMap<(Symbol, String), GateRecoveryOwnerRoute>,
) -> Result<(Vec<GateOwnedOpenOrder>, Vec<GateUnknownOpenOrder>), GateFreshRecoveryError> {
    let mut owned = Vec::new();
    let mut unknown = Vec::new();
    let mut native_ids = BTreeSet::new();
    for readback in readbacks {
        for order in &readback.candidate.order_families.regular().orders {
            if !native_ids.insert(order.order_id.clone()) {
                unknown.push(GateUnknownOpenOrder {
                    symbol: readback.symbol.clone(),
                    venue_order_id: order.order_id.clone(),
                    client_order_id: known_client_id(&order.client_order_id).map(str::to_owned),
                    reason: GateUnknownOpenOrderReason::DuplicateNativeIdentity,
                });
                continue;
            }
            let Some(client_id) = known_client_id(&order.client_order_id) else {
                unknown.push(GateUnknownOpenOrder {
                    symbol: readback.symbol.clone(),
                    venue_order_id: order.order_id.clone(),
                    client_order_id: None,
                    reason: GateUnknownOpenOrderReason::ClientIdentityUnavailable,
                });
                continue;
            };
            let Some(route) = routes.get(&(readback.symbol.clone(), client_id.to_owned())) else {
                unknown.push(GateUnknownOpenOrder {
                    symbol: readback.symbol.clone(),
                    venue_order_id: order.order_id.clone(),
                    client_order_id: Some(client_id.to_owned()),
                    reason: GateUnknownOpenOrderReason::OwnerRouteMissing,
                });
                continue;
            };
            if route.venue_order_id != order.order_id {
                unknown.push(GateUnknownOpenOrder {
                    symbol: readback.symbol.clone(),
                    venue_order_id: order.order_id.clone(),
                    client_order_id: Some(client_id.to_owned()),
                    reason: GateUnknownOpenOrderReason::NativeIdentityMismatch,
                });
                continue;
            }
            owned.push(GateOwnedOpenOrder {
                symbol: readback.symbol.clone(),
                client_order_id: route.client_order_id.clone(),
                venue_order_id: route.venue_order_id.clone(),
                owner: route.owner.clone(),
            });
        }
    }
    Ok((owned, unknown))
}

fn known_client_id(value: &FieldState<String>) -> Option<&str> {
    match value {
        FieldState::Known(value) if !value.is_empty() => Some(value),
        _ => None,
    }
}

fn singleton_raw_digest(
    raw: &[GateRawPrivateResponse],
    source: GatePrivateReadSource,
) -> Result<[u8; 32], GateFreshRecoveryError> {
    let mut matching = raw.iter().filter(|response| response.source == source);
    let digest = matching
        .next()
        .ok_or(GateFreshRecoveryError::MissingSurface)?
        .payload_sha256;
    if matching.next().is_some() {
        return Err(GateFreshRecoveryError::DuplicateSurface);
    }
    Ok(digest)
}

fn surface_commitments(
    scope: &GateRecoveryScope,
    readbacks: &[GateRecoverySymbolReadback],
    raw_by_symbol: &BTreeMap<Symbol, Vec<GateRawPrivateResponse>>,
) -> Result<BTreeMap<GateRecoverySurface, GateRecoverySurfaceCommitment>, GateFreshRecoveryError> {
    let regular_count = readbacks.iter().try_fold(0_u64, |total, readback| {
        let count = u64::try_from(readback.candidate.order_families.regular().orders.len())
            .map_err(|_| GateFreshRecoveryError::RecordCount)?;
        total
            .checked_add(count)
            .ok_or(GateFreshRecoveryError::RecordCount)
    })?;
    let fill_count = readbacks.iter().try_fold(0_u64, |total, readback| {
        let count = u64::try_from(readback.candidate.fills.fills.len())
            .map_err(|_| GateFreshRecoveryError::RecordCount)?;
        total
            .checked_add(count)
            .ok_or(GateFreshRecoveryError::RecordCount)
    })?;
    let symbol_count =
        u64::try_from(readbacks.len()).map_err(|_| GateFreshRecoveryError::RecordCount)?;
    let position_count = symbol_count
        .checked_mul(2)
        .ok_or(GateFreshRecoveryError::RecordCount)?;
    let definitions = [
        (
            GateRecoverySurface::Account,
            GateRecoveryCoverage::Complete { record_count: 1 },
            Some(GatePrivateReadSource::Account),
        ),
        (
            GateRecoverySurface::Positions,
            GateRecoveryCoverage::Complete {
                record_count: position_count,
            },
            Some(GatePrivateReadSource::DualPositions),
        ),
        (
            GateRecoverySurface::RegularOrders,
            GateRecoveryCoverage::Complete {
                record_count: regular_count,
            },
            Some(GatePrivateReadSource::RegularOrders),
        ),
        (
            GateRecoverySurface::ConditionalOrders,
            GateRecoveryCoverage::Unsupported {
                profile_version: GATE_STAGE7_ORDER_PROFILE_VERSION,
            },
            None,
        ),
        (
            GateRecoverySurface::AlgoOrders,
            GateRecoveryCoverage::Unsupported {
                profile_version: GATE_STAGE7_ORDER_PROFILE_VERSION,
            },
            None,
        ),
        (
            GateRecoverySurface::FillsCursor,
            GateRecoveryCoverage::Complete {
                record_count: fill_count,
            },
            Some(GatePrivateReadSource::Fills),
        ),
    ];
    let mut surfaces = BTreeMap::new();
    for (surface, coverage, source) in definitions {
        let raw_commitment_sha256 = surface_commitment(scope, surface, source, raw_by_symbol);
        surfaces.insert(
            surface,
            GateRecoverySurfaceCommitment {
                surface,
                coverage,
                raw_commitment_sha256,
            },
        );
    }
    Ok(surfaces)
}

fn scope_commitment(
    start: &GateRecoveryCollectionStart,
    rest_origin: &str,
    private_ws_endpoint: &str,
    private_generation: u64,
    symbols: &[Symbol],
    request_universe_sha256: &[u8; 32],
    runtime_scope_sha256: Option<&[u8; 32]>,
) -> [u8; 32] {
    let mut digest = tagged_digest(b"venue-gate-fresh-recovery-scope-v1");
    update_string(&mut digest, start.mode.as_str());
    update_string(&mut digest, rest_origin);
    update_string(&mut digest, private_ws_endpoint);
    update_string(&mut digest, &start.trading_account_id);
    update_string(&mut digest, &start.config_digest);
    digest.update(start.config_epoch.to_be_bytes());
    digest.update(start.connection_generation.to_be_bytes());
    digest.update(start.recovered_private_generation.to_be_bytes());
    digest.update(private_generation.to_be_bytes());
    digest.update(start.attempt_id.to_be_bytes());
    digest.update(start.started_at_ms.to_be_bytes());
    digest.update(start.deadline_at_ms.to_be_bytes());
    digest.update(start.authority_roots.owner);
    digest.update(start.authority_roots.wal);
    digest.update(start.authority_roots.unknown);
    digest.update(request_universe_sha256);
    match runtime_scope_sha256 {
        Some(runtime_scope_sha256) => {
            digest.update([1]);
            digest.update(runtime_scope_sha256);
        }
        None => digest.update([0]),
    }
    for symbol in symbols {
        update_string(&mut digest, &symbol.to_string());
    }
    digest.finalize().into()
}

fn surface_commitment(
    scope: &GateRecoveryScope,
    surface: GateRecoverySurface,
    source: Option<GatePrivateReadSource>,
    raw_by_symbol: &BTreeMap<Symbol, Vec<GateRawPrivateResponse>>,
) -> [u8; 32] {
    let mut digest = tagged_digest(b"venue-gate-fresh-recovery-surface-v1");
    digest.update(scope.commitment_sha256);
    digest.update([surface_tag(surface)]);
    digest.update(GATE_STAGE7_ORDER_PROFILE_VERSION.to_be_bytes());
    for (symbol, raw) in raw_by_symbol {
        update_string(&mut digest, &symbol.to_string());
        if let Some(source) = source {
            for response in raw.iter().filter(|response| response.source == source) {
                update_string(&mut digest, &response.endpoint);
                update_string(&mut digest, &response.query);
                update_optional_string(&mut digest, response.cursor_before.as_deref());
                digest.update(response.requested_at_ms.to_be_bytes());
                digest.update(response.received_at_ms.to_be_bytes());
                digest.update(response.payload_sha256);
            }
        }
    }
    digest.finalize().into()
}

fn candidate_commitment(
    scope: &GateRecoveryScope,
    surfaces: &BTreeMap<GateRecoverySurface, GateRecoverySurfaceCommitment>,
    owned: &[GateOwnedOpenOrder],
    unknown: &[GateUnknownOpenOrder],
) -> [u8; 32] {
    let mut digest = tagged_digest(b"venue-gate-fresh-recovery-candidate-v1");
    digest.update(scope.commitment_sha256);
    for (surface, commitment) in surfaces {
        digest.update([surface_tag(*surface)]);
        digest.update(commitment.raw_commitment_sha256);
    }
    for order in owned {
        update_string(&mut digest, &order.symbol.to_string());
        update_string(&mut digest, order.client_order_id.as_str());
        update_string(&mut digest, &order.venue_order_id);
        update_owner(&mut digest, &order.owner);
    }
    for order in unknown {
        update_string(&mut digest, &order.symbol.to_string());
        update_string(&mut digest, &order.venue_order_id);
        update_optional_string(&mut digest, order.client_order_id.as_deref());
        digest.update([unknown_reason_tag(order.reason)]);
    }
    digest.finalize().into()
}

fn update_owner(digest: &mut Sha256, owner: &OrderOwner) {
    update_string(digest, &owner.strategy_instance_id);
    update_string(digest, &owner.run_id);
    update_string(digest, &owner.exchange);
    update_string(digest, &owner.account);
    update_string(digest, &owner.symbol.to_string());
    update_string(digest, &format!("{:?}", owner.purpose));
}

fn tagged_digest(tag: &[u8]) -> Sha256 {
    let mut digest = Sha256::new();
    digest.update(tag);
    digest.update([0]);
    digest
}

fn update_string(digest: &mut Sha256, value: &str) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value.as_bytes());
}

fn update_optional_string(digest: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            digest.update([1]);
            update_string(digest, value);
        }
        None => digest.update([0]),
    }
}

const fn surface_tag(surface: GateRecoverySurface) -> u8 {
    match surface {
        GateRecoverySurface::Account => 1,
        GateRecoverySurface::Positions => 2,
        GateRecoverySurface::RegularOrders => 3,
        GateRecoverySurface::ConditionalOrders => 4,
        GateRecoverySurface::AlgoOrders => 5,
        GateRecoverySurface::FillsCursor => 6,
    }
}

const fn unknown_reason_tag(reason: GateUnknownOpenOrderReason) -> u8 {
    match reason {
        GateUnknownOpenOrderReason::ClientIdentityUnavailable => 1,
        GateUnknownOpenOrderReason::OwnerRouteMissing => 2,
        GateUnknownOpenOrderReason::NativeIdentityMismatch => 3,
        GateUnknownOpenOrderReason::DuplicateNativeIdentity => 4,
    }
}

fn is_zero_digest(value: &[u8; 32]) -> bool {
    *value == [0; 32]
}

fn valid_native_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn unix_ms() -> Result<u64, GateFreshRecoveryError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| GateFreshRecoveryError::Clock)?
        .as_millis();
    u64::try_from(millis).map_err(|_| GateFreshRecoveryError::Clock)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GateFreshRecoveryError {
    #[error("Gate production recovery collection requires a crate-issued authenticated session")]
    AuthenticatedSessionRequired,
    #[error("Gate authenticated recovery session issuer is unavailable")]
    AuthenticatedSessionIssuer,
    #[error("Gate authenticated recovery session endpoint is not the exact selected endpoint")]
    AuthenticatedSessionEndpoint,
    #[error("Gate authenticated recovery session does not match the attempt scope or transport")]
    AuthenticatedSessionScope,
    #[error("Gate authenticated recovery session was revoked by disconnect or replacement")]
    AuthenticatedSessionRevoked,
    #[error("Gate recovery scope, attempt, or collection window is invalid")]
    Scope,
    #[error("Gate recovery configuration digest or epoch is invalid")]
    Configuration,
    #[error("Gate Owner, WAL, and Unknown roots must all be nonzero")]
    AuthorityRoot,
    #[error("Gate recovery private generation overflowed")]
    Generation,
    #[error("Gate recovery symbol universe is empty or incomplete")]
    SymbolUniverse,
    #[error("Gate recovery symbol binding or rules are inconsistent")]
    SymbolScope,
    #[error("Gate recovery Owner route is invalid, duplicated, or outside the symbol universe")]
    OwnerRoute,
    #[error("Gate recovery endpoint does not match LIVE")]
    Endpoint,
    #[error("Gate recovery fill cursor is inconsistent with the frozen scope")]
    Cursor,
    #[error("Gate recovery response belongs to another scope, root, or generation")]
    ScopeDrift,
    #[error("Gate recovery validation missed the frozen deadline")]
    Deadline,
    #[error("Gate recovery is missing a signed surface")]
    MissingSurface,
    #[error("Gate recovery repeats a singleton signed surface")]
    DuplicateSurface,
    #[error("Gate account raw payload forked inside one attempt")]
    RawDivergence,
    #[error("Gate account identity or balance diverged inside one attempt")]
    AccountDivergence,
    #[error("Gate recovery record count exceeds the supported range")]
    RecordCount,
    #[error("Gate recovery global page or byte budget was exhausted before a GET")]
    Budget,
    #[error("Gate recovery collection epoch or request face was already consumed")]
    SessionConsumed,
    #[error("Gate recovery requires a verified runtime recovery scope")]
    RuntimeScopeRequired,
    #[error("Gate runtime recovery scope, Hedge profile, or registry universe is invalid")]
    RuntimeScope,
    #[error("Gate runtime recovery registry universe does not match the authenticated session")]
    RuntimeUniverse,
    #[error("Gate runtime recovery supports only regular-family structured Unknown commands")]
    RuntimeUnknown,
    #[error("Gate runtime recovery Hedge side or regular-only profile drifted")]
    RuntimeProfile,
    #[error("Gate runtime recovery scope drifted across a network await")]
    RuntimeScopeDrift,
    #[error("Gate runtime-bound reads require post-await scope revalidation")]
    RuntimeRevalidationRequired,
    #[error("Gate runtime-bound collection must finish as a runtime recovery bundle")]
    RuntimeBundleRequired,
    #[error("Gate recovery clock is invalid")]
    Clock,
    #[error(transparent)]
    Transport(#[from] GateTransportError),
    #[error(transparent)]
    PrivateRead(#[from] GatePrivateReadError),
}

#[cfg(test)]
#[path = "recovery/tests.rs"]
mod tests;
