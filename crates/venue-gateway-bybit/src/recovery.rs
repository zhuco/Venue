use std::{collections::BTreeSet, fmt, time::Duration};

use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time::timeout,
};
use venue_domain::domain::{NativeOrderFamily, Symbol};
use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

use crate::transport::unix_ms;
use crate::{
    BYBIT_LINEAR_ORDER_PROFILE_VERSION, BYBIT_PRIVATE_MAX_PAGES, BybitAccountReadback,
    BybitCapabilityCandidate, BybitCredentials, BybitError, BybitExecutionPage, BybitFillReadback,
    BybitGatewayBinding, BybitHistoryWindow, BybitHttpTransport, BybitOpenOrdersReadback,
    BybitOrderHistoryReadback, BybitPositionPage, BybitPositionReadback,
    BybitPreparedPrivateRequest, BybitPrivateSource, BybitPrivateWsTransport,
    BybitRawPrivatePayload, BybitTransportError, BybitTransportLimits, complete_execution_pages,
    complete_open_order_pages, complete_order_history_pages, complete_position_pages,
    parse_api_key_evidence, parse_execution_page, parse_open_order_page, parse_order_history_page,
    parse_position_page, prepare_private_request, replay_capability_candidate,
};

const RECOVERY_SCOPE_SCHEMA: &[u8] = b"venue-bybit-fresh-recovery-scope-v1";
const RECOVERY_EVIDENCE_SCHEMA: &[u8] = b"venue-bybit-fresh-recovery-evidence-v1";
pub const BYBIT_RECOVERY_MAX_SYMBOLS: usize = 64;
pub const BYBIT_RECOVERY_MAX_DEADLINE_MS: u64 = 120_000;

/// Caller-supplied root candidates. Nonzero digests alone are deliberately not recovery authority;
/// only an opaque `BybitRecoveryAuthorityReceipt` can open scope construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitRecoveryRootCandidates {
    owner: [u8; 32],
    wal: [u8; 32],
    unknown: [u8; 32],
}

impl BybitRecoveryRootCandidates {
    pub fn verified(
        owner: [u8; 32],
        wal: [u8; 32],
        unknown: [u8; 32],
    ) -> Result<Self, BybitFreshRecoveryError> {
        if [owner, wal, unknown]
            .iter()
            .any(|digest| digest.iter().all(|byte| *byte == 0))
        {
            return Err(BybitFreshRecoveryError::AuthorityRoots);
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

/// Opaque proof that a host replayed the complete Owner routes, WAL projection, and unresolved
/// command projection. The production crate intentionally provides no constructor until those
/// runtime-owned projections are integrated. A digest supplied by an ordinary caller is therefore
/// only a candidate and cannot open this collector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitRecoveryAuthorityReceipt {
    roots: BybitRecoveryRootCandidates,
    owner_route_count: u64,
    wal_record_count: u64,
    unresolved_count: u64,
    projection_sha256: [u8; 32],
}

impl BybitRecoveryAuthorityReceipt {
    #[must_use]
    pub const fn roots(&self) -> &BybitRecoveryRootCandidates {
        &self.roots
    }

    #[must_use]
    pub const fn owner_route_count(&self) -> u64 {
        self.owner_route_count
    }

    #[must_use]
    pub const fn wal_record_count(&self) -> u64 {
        self.wal_record_count
    }

    #[must_use]
    pub const fn unresolved_count(&self) -> u64 {
        self.unresolved_count
    }

    #[must_use]
    pub const fn projection_sha256(&self) -> &[u8; 32] {
        &self.projection_sha256
    }

    /// Explicitly test-only construction. Enabling `test-fixtures` cannot enable writer,
    /// capability, dispatch, or production recovery integration.
    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn explicit_test_fixture(
        roots: BybitRecoveryRootCandidates,
        owner_route_count: u64,
        wal_record_count: u64,
        unresolved_count: u64,
        projection_sha256: [u8; 32],
    ) -> Result<Self, BybitFreshRecoveryError> {
        if projection_sha256.iter().all(|byte| *byte == 0) {
            return Err(BybitFreshRecoveryError::AuthorityRoots);
        }
        Ok(Self {
            roots,
            owner_route_count,
            wal_record_count,
            unresolved_count,
            projection_sha256,
        })
    }
}

/// Immutable recovery scope bound directly to a currently authenticated private transport.
/// There is intentionally no constructor from capability-probe JSON or caller-provided endpoint
/// text, so a persisted or relabelled probe cannot become fresh recovery evidence.
#[derive(Clone, Eq, PartialEq)]
pub struct BybitFreshRecoveryScope {
    anchor_binding: GatewayBinding,
    rest_endpoint: &'static str,
    private_endpoint: &'static str,
    credential_namespace_hmac: [u8; 32],
    connection_id_sha256: [u8; 32],
    config_digest: String,
    config_epoch: u64,
    connection_generation: u64,
    private_generation: u64,
    recovered_connection_generation: u64,
    recovered_private_generation: u64,
    attempt_id: u64,
    bound_at_ms: u64,
    deadline_at_ms: u64,
    history_window: BybitHistoryWindow,
    authority: BybitRecoveryAuthorityReceipt,
    symbols: BTreeSet<Symbol>,
    commitment_sha256: [u8; 32],
}

impl fmt::Debug for BybitFreshRecoveryScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BybitFreshRecoveryScope")
            .field("anchor_binding", &self.anchor_binding)
            .field("rest_endpoint", &self.rest_endpoint)
            .field("private_endpoint", &self.private_endpoint)
            .field("credential_namespace_hmac", &"<hmac-sha256>")
            .field("connection_id_sha256", &"<sha256>")
            .field("config_digest", &self.config_digest)
            .field("config_epoch", &self.config_epoch)
            .field("connection_generation", &self.connection_generation)
            .field("private_generation", &self.private_generation)
            .field(
                "recovered_connection_generation",
                &self.recovered_connection_generation,
            )
            .field(
                "recovered_private_generation",
                &self.recovered_private_generation,
            )
            .field("attempt_id", &self.attempt_id)
            .field("bound_at_ms", &self.bound_at_ms)
            .field("deadline_at_ms", &self.deadline_at_ms)
            .field("symbols", &self.symbols)
            .finish_non_exhaustive()
    }
}

impl BybitFreshRecoveryScope {
    #[allow(clippy::too_many_arguments)]
    pub fn bind<S, I>(
        anchor: BybitGatewayBinding,
        credentials: &BybitCredentials,
        private_stream: &BybitPrivateWsTransport<S>,
        config_digest: impl Into<String>,
        config_epoch: u64,
        recovered_connection_generation: u64,
        recovered_private_generation: u64,
        attempt_id: u64,
        bound_at_ms: u64,
        deadline_at_ms: u64,
        history_window: BybitHistoryWindow,
        authority: BybitRecoveryAuthorityReceipt,
        symbols: I,
    ) -> Result<Self, BybitFreshRecoveryError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
        I: IntoIterator<Item = Symbol>,
    {
        let config_digest = config_digest.into();
        let symbols = symbols.into_iter().collect::<BTreeSet<_>>();
        let binding = anchor.gateway_binding();
        if binding.venue != VenueId::Bybit
            || private_stream.binding() != binding
            || private_stream.endpoint() != anchor.config().private_ws()
            || private_stream.connection_generation() == 0
            || private_stream.private_generation() == 0
            || !private_stream.recovery_generations_independently_bound()
            || private_stream.connection_generation() <= recovered_connection_generation
            || private_stream.private_generation() <= recovered_private_generation
            || private_stream.authenticated_at_ms() == 0
            || private_stream.authenticated_at_ms() > bound_at_ms
            || config_epoch == 0
            || attempt_id == 0
            || bound_at_ms == 0
            || history_window.end_ms > bound_at_ms
            || !is_digest_text(&config_digest)
            || !recovery_bounds_valid(symbols.len(), bound_at_ms, deadline_at_ms)
            || !symbols.contains(&binding.symbol)
        {
            return Err(BybitFreshRecoveryError::Scope);
        }
        for symbol in &symbols {
            GatewayBinding::new(
                VenueId::Bybit,
                binding.mode,
                binding.trading_account_id.clone(),
                symbol.clone(),
            )
            .map_err(|_| BybitFreshRecoveryError::Scope)?;
        }

        let connection_generation = private_stream.connection_generation();
        let private_generation = private_stream.private_generation();
        let connection_id_sha256 = Sha256::digest(private_stream.connection_id()).into();
        let precredential = scope_precredential_bytes(
            binding,
            anchor.config().rest_origin(),
            anchor.config().private_ws(),
            &config_digest,
            config_epoch,
            connection_generation,
            private_generation,
            recovered_connection_generation,
            recovered_private_generation,
            attempt_id,
            bound_at_ms,
            deadline_at_ms,
            &history_window,
            &authority,
            &symbols,
            &connection_id_sha256,
        );
        let credential_namespace_hmac = credentials
            .recovery_namespace_hmac(&precredential)
            .map_err(|_| BybitFreshRecoveryError::Credentials)?;
        let commitment_sha256 = scope_commitment(&precredential, &credential_namespace_hmac);
        Ok(Self {
            anchor_binding: binding.clone(),
            rest_endpoint: anchor.config().rest_origin(),
            private_endpoint: anchor.config().private_ws(),
            credential_namespace_hmac,
            connection_id_sha256,
            config_digest,
            config_epoch,
            connection_generation,
            private_generation,
            recovered_connection_generation,
            recovered_private_generation,
            attempt_id,
            bound_at_ms,
            deadline_at_ms,
            history_window,
            authority,
            symbols,
            commitment_sha256,
        })
    }

    #[must_use]
    pub const fn mode(&self) -> GatewayMode {
        self.anchor_binding.mode
    }

    #[must_use]
    pub fn trading_account_id(&self) -> &str {
        &self.anchor_binding.trading_account_id
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
    pub const fn private_generation(&self) -> u64 {
        self.private_generation
    }

    #[must_use]
    pub const fn recovered_private_generation(&self) -> u64 {
        self.recovered_private_generation
    }

    #[must_use]
    pub const fn recovered_connection_generation(&self) -> u64 {
        self.recovered_connection_generation
    }

    #[must_use]
    pub const fn attempt_id(&self) -> u64 {
        self.attempt_id
    }

    #[must_use]
    pub const fn deadline_at_ms(&self) -> u64 {
        self.deadline_at_ms
    }

    #[must_use]
    pub const fn history_window(&self) -> &BybitHistoryWindow {
        &self.history_window
    }

    #[must_use]
    pub const fn authority(&self) -> &BybitRecoveryAuthorityReceipt {
        &self.authority
    }

    #[must_use]
    pub const fn symbols(&self) -> &BTreeSet<Symbol> {
        &self.symbols
    }

    #[must_use]
    pub const fn commitment_sha256(&self) -> &[u8; 32] {
        &self.commitment_sha256
    }

    fn binding_for(&self, symbol: &Symbol) -> Result<BybitGatewayBinding, BybitFreshRecoveryError> {
        let binding = GatewayBinding::new(
            VenueId::Bybit,
            self.anchor_binding.mode,
            self.anchor_binding.trading_account_id.clone(),
            symbol.clone(),
        )
        .map_err(|_| BybitFreshRecoveryError::Scope)?;
        BybitGatewayBinding::new(binding).map_err(|_| BybitFreshRecoveryError::Scope)
    }

    fn verify_credentials(&self, credentials: &BybitCredentials) -> bool {
        let precredential = scope_precredential_bytes(
            &self.anchor_binding,
            self.rest_endpoint,
            self.private_endpoint,
            &self.config_digest,
            self.config_epoch,
            self.connection_generation,
            self.private_generation,
            self.recovered_connection_generation,
            self.recovered_private_generation,
            self.attempt_id,
            self.bound_at_ms,
            self.deadline_at_ms,
            &self.history_window,
            &self.authority,
            &self.symbols,
            &self.connection_id_sha256,
        );
        credentials
            .recovery_namespace_hmac(&precredential)
            .is_ok_and(|hmac| hmac == self.credential_namespace_hmac)
            && scope_commitment(&precredential, &self.credential_namespace_hmac)
                == self.commitment_sha256
    }

    fn stream_matches<S>(&self, stream: &BybitPrivateWsTransport<S>) -> bool
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        stream.binding() == &self.anchor_binding
            && stream.endpoint() == self.private_endpoint
            && stream.recovery_generations_independently_bound()
            && stream.connection_generation() == self.connection_generation
            && stream.private_generation() == self.private_generation
            && Sha256::digest(stream.connection_id()).as_slice() == self.connection_id_sha256
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum BybitRecoverySurface {
    Account,
    Positions,
    UmOrder,
    UmConditional,
    UmAlgo,
    FillsCursor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BybitRecoveryCoverage {
    Complete { record_count: u64 },
    Unsupported { profile_version: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitRecoverySurfaceEvidence {
    surface: BybitRecoverySurface,
    coverage: BybitRecoveryCoverage,
    evidence_sha256: [u8; 32],
}

impl BybitRecoverySurfaceEvidence {
    #[must_use]
    pub const fn surface(&self) -> BybitRecoverySurface {
        self.surface
    }

    #[must_use]
    pub const fn coverage(&self) -> &BybitRecoveryCoverage {
        &self.coverage
    }

    #[must_use]
    pub const fn evidence_sha256(&self) -> &[u8; 32] {
        &self.evidence_sha256
    }
}

/// One symbol's six-face projection. API-key permission evidence remains private and is not
/// serialized or exposed, while each returned readback retains the raw signed REST responses used
/// to produce its normalized projection.
#[derive(Clone, Eq, PartialEq)]
pub struct BybitFreshSymbolRecovery {
    candidate: BybitCapabilityCandidate,
}

impl fmt::Debug for BybitFreshSymbolRecovery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BybitFreshSymbolRecovery")
            .field("binding", &self.candidate.scope.binding)
            .field("generation", &self.candidate.scope.generation)
            .field("attempt_id", &self.candidate.scope.attempt_id)
            .field(
                "account_records",
                &self.candidate.account.raw_payloads.len(),
            )
            .field("positions", &self.candidate.positions.positions.len())
            .field("fills", &self.candidate.fills.fills.len())
            .finish_non_exhaustive()
    }
}

impl BybitFreshSymbolRecovery {
    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.candidate.scope.binding
    }

    #[must_use]
    pub const fn account(&self) -> &BybitAccountReadback {
        &self.candidate.account
    }

    #[must_use]
    pub const fn positions(&self) -> &BybitPositionReadback {
        &self.candidate.positions
    }

    #[must_use]
    pub const fn regular_orders(&self) -> &crate::BybitCompleteOrderFamilyEvidence {
        self.candidate.order_families.regular()
    }

    #[must_use]
    pub const fn conditional_orders(&self) -> &crate::BybitCompleteOrderFamilyEvidence {
        self.candidate.order_families.conditional()
    }

    #[must_use]
    pub const fn fills(&self) -> &BybitFillReadback {
        &self.candidate.fills
    }
}

/// All-or-nothing fresh readback. This value carries no credentials, transport, capability
/// snapshot, writer lease, WAL handle, or mutation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitFreshRecoveryEvidence {
    scope: BybitFreshRecoveryScope,
    symbols: Vec<BybitFreshSymbolRecovery>,
    surfaces: [BybitRecoverySurfaceEvidence; 6],
    collected_at_ms: u64,
    commitment_sha256: [u8; 32],
}

impl BybitFreshRecoveryEvidence {
    #[must_use]
    pub const fn scope(&self) -> &BybitFreshRecoveryScope {
        &self.scope
    }

    #[must_use]
    pub fn symbols(&self) -> &[BybitFreshSymbolRecovery] {
        &self.symbols
    }

    #[must_use]
    pub fn surface(&self, surface: BybitRecoverySurface) -> &BybitRecoverySurfaceEvidence {
        &self.surfaces[usize::from(surface_tag(surface) - 1)]
    }

    #[must_use]
    pub const fn collected_at_ms(&self) -> u64 {
        self.collected_at_ms
    }

    #[must_use]
    pub const fn commitment_sha256(&self) -> &[u8; 32] {
        &self.commitment_sha256
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BybitRecoveryUnknownReason {
    Deadline,
    Disconnected,
    Transport,
    GenerationChanged,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitFreshRecoveryUnknown {
    scope_sha256: [u8; 32],
    attempt_id: u64,
    connection_generation: u64,
    private_generation: u64,
    observed_private_generation: Option<u64>,
    received_raw_count: u64,
    received_raw_sha256: [u8; 32],
    native_identity_sha256: [u8; 32],
    reason: BybitRecoveryUnknownReason,
    commitment_sha256: [u8; 32],
}

impl BybitFreshRecoveryUnknown {
    #[must_use]
    pub const fn scope_sha256(&self) -> &[u8; 32] {
        &self.scope_sha256
    }

    #[must_use]
    pub const fn attempt_id(&self) -> u64 {
        self.attempt_id
    }

    #[must_use]
    pub const fn private_generation(&self) -> u64 {
        self.private_generation
    }

    #[must_use]
    pub const fn connection_generation(&self) -> u64 {
        self.connection_generation
    }

    #[must_use]
    pub const fn observed_private_generation(&self) -> Option<u64> {
        self.observed_private_generation
    }

    #[must_use]
    pub const fn received_raw_count(&self) -> u64 {
        self.received_raw_count
    }

    #[must_use]
    pub const fn received_raw_sha256(&self) -> &[u8; 32] {
        &self.received_raw_sha256
    }

    #[must_use]
    pub const fn native_identity_sha256(&self) -> &[u8; 32] {
        &self.native_identity_sha256
    }

    #[must_use]
    pub const fn reason(&self) -> BybitRecoveryUnknownReason {
        self.reason
    }

    #[must_use]
    pub const fn commitment_sha256(&self) -> &[u8; 32] {
        &self.commitment_sha256
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BybitFreshRecoveryOutcome {
    Complete(Box<BybitFreshRecoveryEvidence>),
    Unknown(BybitFreshRecoveryUnknown),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BybitFreshRecoveryError {
    #[error("Bybit production recovery integration is unavailable")]
    IntegrationUnavailable,
    #[error("Bybit recovery scope is invalid or stale")]
    Scope,
    #[error("Bybit recovery requires exact nonzero Owner, WAL, and Unknown roots")]
    AuthorityRoots,
    #[error("Bybit recovery credential namespace does not match")]
    Credentials,
    #[error("Bybit recovery API key lacks exact readback permissions")]
    Permissions,
    #[error("Bybit recovery raw evidence or normalized projection is invalid")]
    Projection,
}

/// Production remains unavailable until the runtime can issue the opaque authority receipt from
/// full Owner/WAL/Unknown projections. This status does not inspect credentials or touch network.
pub const fn bybit_fresh_recovery_integration_status() -> Result<(), BybitFreshRecoveryError> {
    Err(BybitFreshRecoveryError::IntegrationUnavailable)
}

#[derive(Clone)]
struct CollectionProgress {
    received_raw_count: u64,
    observed_private_generation: Option<u64>,
    raw_digest: Sha256,
    native_identity_digest: Sha256,
}

impl CollectionProgress {
    fn new(scope: &BybitFreshRecoveryScope) -> Self {
        let mut raw_digest = Sha256::new();
        raw_digest.update(b"venue-bybit-recovery-partial-raw-v1");
        raw_digest.update(scope.commitment_sha256);
        let mut native_identity_digest = Sha256::new();
        native_identity_digest.update(b"venue-bybit-recovery-partial-native-identity-v1");
        native_identity_digest.update(scope.commitment_sha256);
        Self {
            received_raw_count: 0,
            observed_private_generation: None,
            raw_digest,
            native_identity_digest,
        }
    }

    fn observe_generation(&mut self, generation: u64) {
        self.observed_private_generation = Some(generation);
    }

    fn observe_raw(&mut self, raw: &BybitRawPrivatePayload) {
        self.received_raw_count = self.received_raw_count.saturating_add(1);
        commit_raw(&mut self.raw_digest, raw);
        commit_str(&mut self.native_identity_digest, &raw.native_symbol);
        commit_str(&mut self.native_identity_digest, &raw.request_path);
        commit_str(&mut self.native_identity_digest, &raw.request_query);
        commit_u64(&mut self.native_identity_digest, raw.generation);
        commit_u64(&mut self.native_identity_digest, raw.attempt_id);
        commit_u64(&mut self.native_identity_digest, raw.page_index.into());
        commit_str(&mut self.native_identity_digest, &raw.payload_sha256);
    }

    fn observe_candidate(&mut self, candidate: &BybitCapabilityCandidate) {
        for order in candidate
            .order_families
            .regular()
            .open_orders
            .orders
            .iter()
            .chain(
                candidate
                    .order_families
                    .conditional()
                    .open_orders
                    .orders
                    .iter(),
            )
        {
            commit_str(&mut self.native_identity_digest, &order.order.order_id);
        }
        for fill in &candidate.fills.fills {
            commit_str(&mut self.native_identity_digest, &fill.fill.fill_id);
            commit_str(&mut self.native_identity_digest, &fill.fill.order_id);
        }
    }

    fn raw_sha256(&self) -> [u8; 32] {
        self.raw_digest.clone().finalize().into()
    }

    fn native_identity_sha256(&self) -> [u8; 32] {
        self.native_identity_digest.clone().finalize().into()
    }
}

/// Collects all registered symbols under one deadline and one live private generation. Any
/// transport ambiguity returns a structured Unknown. Scope, permission, or projection failures are
/// rejected and cannot be relabelled into a different attempt because all output constructors are
/// private to this module.
pub async fn collect_bybit_fresh_recovery<S>(
    scope: BybitFreshRecoveryScope,
    credentials: &BybitCredentials,
    private_stream: &mut BybitPrivateWsTransport<S>,
    limits: BybitTransportLimits,
) -> Result<BybitFreshRecoveryOutcome, BybitFreshRecoveryError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut progress = CollectionProgress::new(&scope);
    if !scope.verify_credentials(credentials) {
        return Err(BybitFreshRecoveryError::Credentials);
    }
    if !scope.stream_matches(private_stream) {
        return Ok(unknown(
            &scope,
            BybitRecoveryUnknownReason::GenerationChanged,
            &progress,
        ));
    }
    let now_ms = unix_ms().map_err(|_| BybitFreshRecoveryError::Scope)?;
    let remaining_ms = scope.deadline_at_ms.saturating_sub(now_ms);
    if remaining_ms == 0 {
        return Ok(unknown(
            &scope,
            BybitRecoveryUnknownReason::Deadline,
            &progress,
        ));
    }
    let collection =
        collect_under_deadline(&scope, credentials, private_stream, limits, &mut progress);
    match timeout(Duration::from_millis(remaining_ms), collection).await {
        Err(_) => Ok(unknown(
            &scope,
            BybitRecoveryUnknownReason::Deadline,
            &progress,
        )),
        Ok(Err(CollectFailure::Unknown(reason))) => Ok(unknown(&scope, reason, &progress)),
        Ok(Err(CollectFailure::Rejected(error))) => Err(error),
        Ok(Ok((symbols, collected_at_ms))) => {
            let surfaces = surface_evidence(&scope, &symbols)?;
            let commitment_sha256 =
                evidence_commitment(scope.commitment_sha256(), collected_at_ms, &surfaces);
            Ok(BybitFreshRecoveryOutcome::Complete(Box::new(
                BybitFreshRecoveryEvidence {
                    scope,
                    symbols,
                    surfaces,
                    collected_at_ms,
                    commitment_sha256,
                },
            )))
        }
    }
}

async fn collect_under_deadline<S>(
    scope: &BybitFreshRecoveryScope,
    credentials: &BybitCredentials,
    private_stream: &mut BybitPrivateWsTransport<S>,
    limits: BybitTransportLimits,
    progress: &mut CollectionProgress,
) -> Result<(Vec<BybitFreshSymbolRecovery>, u64), CollectFailure>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    live_check(scope, private_stream, progress).await?;
    let mut recovered = Vec::with_capacity(scope.symbols.len());
    for symbol in &scope.symbols {
        let binding = scope
            .binding_for(symbol)
            .map_err(CollectFailure::Rejected)?;
        let transport = BybitHttpTransport::new(&binding, scope.private_generation, limits)
            .map_err(map_transport)?;
        let candidate = collect_symbol(scope, credentials, &binding, &transport, progress).await?;
        progress.observe_candidate(&candidate);
        recovered.push(BybitFreshSymbolRecovery { candidate });
        live_check(scope, private_stream, progress).await?;
    }
    let collected_at_ms = checked_now(scope)?;
    Ok((recovered, collected_at_ms))
}

async fn collect_symbol(
    scope: &BybitFreshRecoveryScope,
    credentials: &BybitCredentials,
    binding: &BybitGatewayBinding,
    transport: &BybitHttpTransport,
    progress: &mut CollectionProgress,
) -> Result<BybitCapabilityCandidate, CollectFailure> {
    let api_raw = fetch_one(
        scope,
        credentials,
        binding,
        transport,
        BybitPrivateSource::ApiKeyInfo,
        progress,
    )
    .await?;
    let api_key = parse_api_key_evidence(binding, credentials, &api_raw).map_err(map_projection)?;
    if !api_key.contract_order || !api_key.contract_position || api_key.withdraw {
        return Err(CollectFailure::Rejected(
            BybitFreshRecoveryError::Permissions,
        ));
    }

    let account_raw = fetch_one(
        scope,
        credentials,
        binding,
        transport,
        BybitPrivateSource::AccountInfo,
        progress,
    )
    .await?;
    let wallet_raw = fetch_one(
        scope,
        credentials,
        binding,
        transport,
        BybitPrivateSource::WalletBalance,
        progress,
    )
    .await?;
    let position_raws = fetch_positions(scope, credentials, binding, transport, progress).await?;
    let regular_open = fetch_open_orders(
        scope,
        credentials,
        binding,
        transport,
        NativeOrderFamily::UmOrder,
        progress,
    )
    .await?;
    let regular_history = fetch_order_history(
        scope,
        credentials,
        binding,
        transport,
        NativeOrderFamily::UmOrder,
        progress,
    )
    .await?;
    let conditional_open = fetch_open_orders(
        scope,
        credentials,
        binding,
        transport,
        NativeOrderFamily::UmConditional,
        progress,
    )
    .await?;
    let conditional_history = fetch_order_history(
        scope,
        credentials,
        binding,
        transport,
        NativeOrderFamily::UmConditional,
        progress,
    )
    .await?;

    let order_details = regular_history
        .orders
        .iter()
        .chain(conditional_history.orders.iter())
        .cloned()
        .collect::<Vec<_>>();
    let execution_raws = fetch_executions(
        scope,
        credentials,
        binding,
        transport,
        &order_details,
        progress,
    )
    .await?;

    let mut raw_payloads = vec![api_raw, account_raw, wallet_raw];
    raw_payloads.extend(position_raws);
    raw_payloads.extend(regular_open.raw_pages.iter().cloned());
    raw_payloads.extend(regular_history.raw_pages.iter().cloned());
    raw_payloads.extend(conditional_open.raw_pages.iter().cloned());
    raw_payloads.extend(conditional_history.raw_pages.iter().cloned());
    raw_payloads.extend(execution_raws);

    let observed_at_ms = raw_payloads
        .iter()
        .map(|raw| raw.received_at_ms)
        .max()
        .ok_or(CollectFailure::Rejected(
            BybitFreshRecoveryError::Projection,
        ))?;
    if observed_at_ms >= scope.deadline_at_ms {
        return Err(CollectFailure::Unknown(
            BybitRecoveryUnknownReason::Deadline,
        ));
    }
    let candidate_scope = crate::BybitOrderFamilyScope {
        binding: binding.gateway_binding().clone(),
        profile_version: BYBIT_LINEAR_ORDER_PROFILE_VERSION,
        attempt_id: scope.attempt_id,
        generation: scope.private_generation,
        observed_at_ms,
        expires_at_ms: scope.deadline_at_ms,
    };
    replay_capability_candidate(
        binding,
        credentials,
        candidate_scope,
        observed_at_ms,
        &raw_payloads,
    )
    .map_err(map_projection)
}

async fn fetch_one(
    scope: &BybitFreshRecoveryScope,
    credentials: &BybitCredentials,
    binding: &BybitGatewayBinding,
    transport: &BybitHttpTransport,
    source: BybitPrivateSource,
    progress: &mut CollectionProgress,
) -> Result<BybitRawPrivatePayload, CollectFailure> {
    let request = prepare_private_request(
        binding,
        scope.private_generation,
        scope.attempt_id,
        0,
        source,
        None,
        None,
        None,
    )
    .map_err(map_projection)?;
    execute_read(scope, credentials, binding, transport, &request, progress).await
}

async fn fetch_positions(
    scope: &BybitFreshRecoveryScope,
    credentials: &BybitCredentials,
    binding: &BybitGatewayBinding,
    transport: &BybitHttpTransport,
    progress: &mut CollectionProgress,
) -> Result<Vec<BybitRawPrivatePayload>, CollectFailure> {
    let mut raws = Vec::new();
    let mut cursor = None;
    loop {
        let index = page_index(raws.len())?;
        let request = prepare_private_request(
            binding,
            scope.private_generation,
            scope.attempt_id,
            index,
            BybitPrivateSource::Positions,
            cursor.as_deref(),
            None,
            None,
        )
        .map_err(map_projection)?;
        let raw = execute_read(scope, credentials, binding, transport, &request, progress).await?;
        let page = parse_position_page(binding, &raw).map_err(map_projection)?;
        cursor = page.meta.next_cursor.clone();
        raws.push(raw);
        if cursor.is_none() {
            let pages = raws
                .iter()
                .map(|raw| parse_position_page(binding, raw))
                .collect::<Result<Vec<BybitPositionPage>, _>>()
                .map_err(map_projection)?;
            complete_position_pages(binding, &pages).map_err(map_projection)?;
            return Ok(raws);
        }
    }
}

async fn fetch_open_orders(
    scope: &BybitFreshRecoveryScope,
    credentials: &BybitCredentials,
    binding: &BybitGatewayBinding,
    transport: &BybitHttpTransport,
    family: NativeOrderFamily,
    progress: &mut CollectionProgress,
) -> Result<BybitOpenOrdersReadback, CollectFailure> {
    let mut pages = Vec::new();
    let mut cursor = None;
    loop {
        let index = page_index(pages.len())?;
        let request = prepare_private_request(
            binding,
            scope.private_generation,
            scope.attempt_id,
            index,
            BybitPrivateSource::OpenOrders(family),
            cursor.as_deref(),
            None,
            None,
        )
        .map_err(map_projection)?;
        let raw = execute_read(scope, credentials, binding, transport, &request, progress).await?;
        let page = parse_open_order_page(binding, &raw).map_err(map_projection)?;
        cursor = page.meta.next_cursor.clone();
        pages.push(page);
        if cursor.is_none() {
            return complete_open_order_pages(binding, family, &pages).map_err(map_projection);
        }
    }
}

async fn fetch_order_history(
    scope: &BybitFreshRecoveryScope,
    credentials: &BybitCredentials,
    binding: &BybitGatewayBinding,
    transport: &BybitHttpTransport,
    family: NativeOrderFamily,
    progress: &mut CollectionProgress,
) -> Result<BybitOrderHistoryReadback, CollectFailure> {
    let mut pages = Vec::new();
    let mut cursor = None;
    loop {
        let index = page_index(pages.len())?;
        let request = prepare_private_request(
            binding,
            scope.private_generation,
            scope.attempt_id,
            index,
            BybitPrivateSource::OrderHistory(family),
            cursor.as_deref(),
            Some(scope.history_window.clone()),
            None,
        )
        .map_err(map_projection)?;
        let raw = execute_read(scope, credentials, binding, transport, &request, progress).await?;
        let page = parse_order_history_page(binding, &raw).map_err(map_projection)?;
        cursor = page.meta.next_cursor.clone();
        pages.push(page);
        if cursor.is_none() {
            return complete_order_history_pages(binding, family, &pages).map_err(map_projection);
        }
    }
}

async fn fetch_executions(
    scope: &BybitFreshRecoveryScope,
    credentials: &BybitCredentials,
    binding: &BybitGatewayBinding,
    transport: &BybitHttpTransport,
    order_details: &[crate::BybitOrderEvidence],
    progress: &mut CollectionProgress,
) -> Result<Vec<BybitRawPrivatePayload>, CollectFailure> {
    let mut raws = Vec::new();
    let mut cursor = None;
    loop {
        let index = page_index(raws.len())?;
        let request = prepare_private_request(
            binding,
            scope.private_generation,
            scope.attempt_id,
            index,
            BybitPrivateSource::Executions,
            cursor.as_deref(),
            Some(scope.history_window.clone()),
            None,
        )
        .map_err(map_projection)?;
        let raw = execute_read(scope, credentials, binding, transport, &request, progress).await?;
        let page = parse_execution_page(binding, &raw, order_details).map_err(map_projection)?;
        cursor = page.meta.next_cursor.clone();
        raws.push(raw);
        if cursor.is_none() {
            let pages = raws
                .iter()
                .map(|raw| parse_execution_page(binding, raw, order_details))
                .collect::<Result<Vec<BybitExecutionPage>, _>>()
                .map_err(map_projection)?;
            complete_execution_pages(binding, &pages, order_details).map_err(map_projection)?;
            return Ok(raws);
        }
    }
}

async fn execute_read(
    scope: &BybitFreshRecoveryScope,
    credentials: &BybitCredentials,
    binding: &BybitGatewayBinding,
    transport: &BybitHttpTransport,
    request: &BybitPreparedPrivateRequest,
    progress: &mut CollectionProgress,
) -> Result<BybitRawPrivatePayload, CollectFailure> {
    let timestamp_ms = checked_now(scope)?;
    let raw = transport
        .execute_private_read(binding, credentials, request, timestamp_ms)
        .await
        .map_err(map_transport)?;
    progress.observe_raw(&raw);
    if raw.generation != scope.private_generation
        || raw.attempt_id != scope.attempt_id
        || raw.received_at_ms >= scope.deadline_at_ms
    {
        return Err(CollectFailure::Unknown(
            BybitRecoveryUnknownReason::GenerationChanged,
        ));
    }
    Ok(raw)
}

async fn live_check<S>(
    scope: &BybitFreshRecoveryScope,
    stream: &mut BybitPrivateWsTransport<S>,
    progress: &mut CollectionProgress,
) -> Result<(), CollectFailure>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if !scope.stream_matches(stream) {
        return Err(CollectFailure::Unknown(
            BybitRecoveryUnknownReason::GenerationChanged,
        ));
    }
    checked_now(scope)?;
    stream
        .recovery_liveness_check()
        .await
        .map_err(map_transport)?;
    progress.observe_generation(stream.private_generation());
    if !scope.stream_matches(stream) {
        return Err(CollectFailure::Unknown(
            BybitRecoveryUnknownReason::GenerationChanged,
        ));
    }
    Ok(())
}

fn page_index(length: usize) -> Result<u32, CollectFailure> {
    if length >= BYBIT_PRIVATE_MAX_PAGES {
        return Err(CollectFailure::Rejected(
            BybitFreshRecoveryError::Projection,
        ));
    }
    u32::try_from(length).map_err(|_| CollectFailure::Rejected(BybitFreshRecoveryError::Projection))
}

fn checked_now(scope: &BybitFreshRecoveryScope) -> Result<u64, CollectFailure> {
    let now = unix_ms().map_err(map_transport)?;
    if now < scope.bound_at_ms || now >= scope.deadline_at_ms {
        Err(CollectFailure::Unknown(
            BybitRecoveryUnknownReason::Deadline,
        ))
    } else {
        Ok(now)
    }
}

fn surface_evidence(
    scope: &BybitFreshRecoveryScope,
    symbols: &[BybitFreshSymbolRecovery],
) -> Result<[BybitRecoverySurfaceEvidence; 6], BybitFreshRecoveryError> {
    let actual = symbols
        .iter()
        .map(|item| item.binding().symbol.clone())
        .collect::<BTreeSet<_>>();
    if actual != scope.symbols || symbols.len() != scope.symbols.len() {
        return Err(BybitFreshRecoveryError::Projection);
    }
    Ok([
        complete_surface(scope, symbols, BybitRecoverySurface::Account),
        complete_surface(scope, symbols, BybitRecoverySurface::Positions),
        complete_surface(scope, symbols, BybitRecoverySurface::UmOrder),
        complete_surface(scope, symbols, BybitRecoverySurface::UmConditional),
        unsupported_algo_surface(scope),
        complete_surface(scope, symbols, BybitRecoverySurface::FillsCursor),
    ])
}

fn complete_surface(
    scope: &BybitFreshRecoveryScope,
    symbols: &[BybitFreshSymbolRecovery],
    surface: BybitRecoverySurface,
) -> BybitRecoverySurfaceEvidence {
    let mut digest = Sha256::new();
    digest.update(RECOVERY_EVIDENCE_SCHEMA);
    digest.update(scope.commitment_sha256);
    digest.update([surface_tag(surface)]);
    let mut record_count = 0_u64;
    for symbol in symbols {
        commit_str(&mut digest, &symbol.binding().symbol.to_string());
        let (raws, count): (Vec<&BybitRawPrivatePayload>, usize) = match surface {
            BybitRecoverySurface::Account => (
                std::iter::once(&symbol.candidate.api_key.raw)
                    .chain(symbol.account().raw_payloads.iter())
                    .collect(),
                symbol.account().raw_payloads.len() + 1,
            ),
            BybitRecoverySurface::Positions => (
                symbol.positions().raw_pages.iter().collect(),
                symbol.positions().positions.len(),
            ),
            BybitRecoverySurface::UmOrder => (
                symbol
                    .regular_orders()
                    .open_orders
                    .raw_pages
                    .iter()
                    .chain(symbol.regular_orders().order_history.raw_pages.iter())
                    .collect(),
                symbol.regular_orders().open_orders.orders.len()
                    + symbol.regular_orders().order_history.orders.len(),
            ),
            BybitRecoverySurface::UmConditional => (
                symbol
                    .conditional_orders()
                    .open_orders
                    .raw_pages
                    .iter()
                    .chain(symbol.conditional_orders().order_history.raw_pages.iter())
                    .collect(),
                symbol.conditional_orders().open_orders.orders.len()
                    + symbol.conditional_orders().order_history.orders.len(),
            ),
            BybitRecoverySurface::FillsCursor => (
                symbol.fills().raw_pages.iter().collect(),
                symbol.fills().fills.len(),
            ),
            BybitRecoverySurface::UmAlgo => (Vec::new(), 0),
        };
        record_count = record_count.saturating_add(count as u64);
        for raw in raws {
            commit_raw(&mut digest, raw);
        }
    }
    BybitRecoverySurfaceEvidence {
        surface,
        coverage: BybitRecoveryCoverage::Complete { record_count },
        evidence_sha256: digest.finalize().into(),
    }
}

fn unsupported_algo_surface(scope: &BybitFreshRecoveryScope) -> BybitRecoverySurfaceEvidence {
    let mut digest = Sha256::new();
    digest.update(RECOVERY_EVIDENCE_SCHEMA);
    digest.update(scope.commitment_sha256);
    digest.update([surface_tag(BybitRecoverySurface::UmAlgo)]);
    commit_u64(&mut digest, BYBIT_LINEAR_ORDER_PROFILE_VERSION);
    commit_str(
        &mut digest,
        "Bybit V5 linear exposes no distinct admitted algo namespace",
    );
    BybitRecoverySurfaceEvidence {
        surface: BybitRecoverySurface::UmAlgo,
        coverage: BybitRecoveryCoverage::Unsupported {
            profile_version: BYBIT_LINEAR_ORDER_PROFILE_VERSION,
        },
        evidence_sha256: digest.finalize().into(),
    }
}

fn evidence_commitment(
    scope: &[u8; 32],
    collected_at_ms: u64,
    surfaces: &[BybitRecoverySurfaceEvidence; 6],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(RECOVERY_EVIDENCE_SCHEMA);
    digest.update(scope);
    commit_u64(&mut digest, collected_at_ms);
    for surface in surfaces {
        digest.update([surface_tag(surface.surface)]);
        digest.update(surface.evidence_sha256);
        match surface.coverage {
            BybitRecoveryCoverage::Complete { record_count } => {
                digest.update([1]);
                commit_u64(&mut digest, record_count);
            }
            BybitRecoveryCoverage::Unsupported { profile_version } => {
                digest.update([2]);
                commit_u64(&mut digest, profile_version);
            }
        }
    }
    digest.finalize().into()
}

fn unknown(
    scope: &BybitFreshRecoveryScope,
    reason: BybitRecoveryUnknownReason,
    progress: &CollectionProgress,
) -> BybitFreshRecoveryOutcome {
    let received_raw_sha256 = progress.raw_sha256();
    let native_identity_sha256 = progress.native_identity_sha256();
    let mut digest = Sha256::new();
    digest.update(b"venue-bybit-fresh-recovery-unknown-v1");
    digest.update(scope.commitment_sha256);
    commit_u64(&mut digest, scope.attempt_id);
    commit_u64(&mut digest, scope.connection_generation);
    commit_u64(&mut digest, scope.private_generation);
    commit_u64(
        &mut digest,
        progress.observed_private_generation.unwrap_or(0),
    );
    commit_u64(&mut digest, progress.received_raw_count);
    digest.update(received_raw_sha256);
    digest.update(native_identity_sha256);
    digest.update([unknown_reason_tag(reason)]);
    let commitment_sha256 = digest.finalize().into();
    BybitFreshRecoveryOutcome::Unknown(BybitFreshRecoveryUnknown {
        scope_sha256: scope.commitment_sha256,
        attempt_id: scope.attempt_id,
        connection_generation: scope.connection_generation,
        private_generation: scope.private_generation,
        observed_private_generation: progress.observed_private_generation,
        received_raw_count: progress.received_raw_count,
        received_raw_sha256,
        native_identity_sha256,
        reason,
        commitment_sha256,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CollectFailure {
    Unknown(BybitRecoveryUnknownReason),
    Rejected(BybitFreshRecoveryError),
}

fn map_transport(error: BybitTransportError) -> CollectFailure {
    match error {
        BybitTransportError::Timeout => {
            CollectFailure::Unknown(BybitRecoveryUnknownReason::Deadline)
        }
        BybitTransportError::Disconnected
        | BybitTransportError::EndOfStream
        | BybitTransportError::Heartbeat => {
            CollectFailure::Unknown(BybitRecoveryUnknownReason::Disconnected)
        }
        BybitTransportError::Http | BybitTransportError::HttpStatus => {
            CollectFailure::Unknown(BybitRecoveryUnknownReason::Transport)
        }
        BybitTransportError::Signing => {
            CollectFailure::Rejected(BybitFreshRecoveryError::Credentials)
        }
        BybitTransportError::Binding => {
            CollectFailure::Unknown(BybitRecoveryUnknownReason::GenerationChanged)
        }
        BybitTransportError::Limits
        | BybitTransportError::BodyTooLarge
        | BybitTransportError::PreLiveBufferOverflow
        | BybitTransportError::Protocol
        | BybitTransportError::Ack
        | BybitTransportError::Rejected
        | BybitTransportError::Clock => {
            CollectFailure::Rejected(BybitFreshRecoveryError::Projection)
        }
    }
}

fn map_projection(error: BybitError) -> CollectFailure {
    match error {
        BybitError::Credentials | BybitError::SigningInput => {
            CollectFailure::Rejected(BybitFreshRecoveryError::Credentials)
        }
        BybitError::Rejected => CollectFailure::Rejected(BybitFreshRecoveryError::Permissions),
        BybitError::Binding => {
            CollectFailure::Unknown(BybitRecoveryUnknownReason::GenerationChanged)
        }
        BybitError::Payload
        | BybitError::Pagination
        | BybitError::Clock
        | BybitError::OrderFamily
        | BybitError::Projection
        | BybitError::Capability => CollectFailure::Rejected(BybitFreshRecoveryError::Projection),
    }
}

#[allow(clippy::too_many_arguments)]
fn scope_precredential_bytes(
    binding: &GatewayBinding,
    rest_endpoint: &str,
    private_endpoint: &str,
    config_digest: &str,
    config_epoch: u64,
    connection_generation: u64,
    private_generation: u64,
    recovered_connection_generation: u64,
    recovered_private_generation: u64,
    attempt_id: u64,
    bound_at_ms: u64,
    deadline_at_ms: u64,
    history_window: &BybitHistoryWindow,
    authority: &BybitRecoveryAuthorityReceipt,
    symbols: &BTreeSet<Symbol>,
    connection_id_sha256: &[u8; 32],
) -> Vec<u8> {
    let mut bytes = Vec::new();
    append_bytes(&mut bytes, RECOVERY_SCOPE_SCHEMA);
    append_bytes(&mut bytes, &[mode_tag(binding.mode)]);
    append_str(&mut bytes, &binding.trading_account_id);
    append_str(&mut bytes, rest_endpoint);
    append_str(&mut bytes, private_endpoint);
    append_str(&mut bytes, config_digest);
    append_u64(&mut bytes, config_epoch);
    append_u64(&mut bytes, connection_generation);
    append_u64(&mut bytes, private_generation);
    append_u64(&mut bytes, recovered_connection_generation);
    append_u64(&mut bytes, recovered_private_generation);
    append_u64(&mut bytes, attempt_id);
    append_u64(&mut bytes, bound_at_ms);
    append_u64(&mut bytes, deadline_at_ms);
    append_u64(&mut bytes, history_window.start_ms);
    append_u64(&mut bytes, history_window.end_ms);
    append_bytes(&mut bytes, authority.roots.owner());
    append_bytes(&mut bytes, authority.roots.wal());
    append_bytes(&mut bytes, authority.roots.unknown());
    append_u64(&mut bytes, authority.owner_route_count);
    append_u64(&mut bytes, authority.wal_record_count);
    append_u64(&mut bytes, authority.unresolved_count);
    append_bytes(&mut bytes, &authority.projection_sha256);
    append_bytes(&mut bytes, connection_id_sha256);
    append_u64(&mut bytes, symbols.len() as u64);
    for symbol in symbols {
        append_str(&mut bytes, &symbol.to_string());
    }
    bytes
}

fn scope_commitment(precredential: &[u8], credential_hmac: &[u8; 32]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(RECOVERY_SCOPE_SCHEMA);
    commit_bytes(&mut digest, precredential);
    commit_bytes(&mut digest, credential_hmac);
    digest.finalize().into()
}

fn is_digest_text(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

const fn recovery_bounds_valid(symbol_count: usize, bound_at_ms: u64, deadline_at_ms: u64) -> bool {
    symbol_count > 0
        && symbol_count <= BYBIT_RECOVERY_MAX_SYMBOLS
        && deadline_at_ms > bound_at_ms
        && deadline_at_ms.saturating_sub(bound_at_ms) <= BYBIT_RECOVERY_MAX_DEADLINE_MS
}

const fn mode_tag(mode: GatewayMode) -> u8 {
    match mode {
        GatewayMode::Test => 1,
        GatewayMode::Live => 2,
    }
}

const fn surface_tag(surface: BybitRecoverySurface) -> u8 {
    match surface {
        BybitRecoverySurface::Account => 1,
        BybitRecoverySurface::Positions => 2,
        BybitRecoverySurface::UmOrder => 3,
        BybitRecoverySurface::UmConditional => 4,
        BybitRecoverySurface::UmAlgo => 5,
        BybitRecoverySurface::FillsCursor => 6,
    }
}

const fn unknown_reason_tag(reason: BybitRecoveryUnknownReason) -> u8 {
    match reason {
        BybitRecoveryUnknownReason::Deadline => 1,
        BybitRecoveryUnknownReason::Disconnected => 2,
        BybitRecoveryUnknownReason::Transport => 3,
        BybitRecoveryUnknownReason::GenerationChanged => 4,
    }
}

fn append_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn append_bytes(bytes: &mut Vec<u8>, value: &[u8]) {
    append_u64(bytes, value.len() as u64);
    bytes.extend_from_slice(value);
}

fn append_str(bytes: &mut Vec<u8>, value: &str) {
    append_bytes(bytes, value.as_bytes());
}

fn commit_u64(digest: &mut Sha256, value: u64) {
    digest.update(value.to_be_bytes());
}

fn commit_bytes(digest: &mut Sha256, value: &[u8]) {
    commit_u64(digest, value.len() as u64);
    digest.update(value);
}

fn commit_str(digest: &mut Sha256, value: &str) {
    commit_bytes(digest, value.as_bytes());
}

fn commit_raw(digest: &mut Sha256, raw: &BybitRawPrivatePayload) {
    commit_str(digest, &raw.binding.symbol.to_string());
    commit_str(digest, &raw.request_path);
    commit_str(digest, &raw.request_query);
    commit_u64(digest, raw.generation);
    commit_u64(digest, raw.attempt_id);
    commit_u64(digest, raw.page_index.into());
    commit_u64(digest, raw.request_timestamp_ms);
    commit_u64(digest, raw.received_at_ms);
    commit_str(digest, &raw.payload_sha256);
    commit_bytes(digest, &raw.payload);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authority_roots_reject_every_missing_projection() {
        let nonzero = [1_u8; 32];
        let zero = [0_u8; 32];
        assert_eq!(
            BybitRecoveryRootCandidates::verified(zero, nonzero, nonzero),
            Err(BybitFreshRecoveryError::AuthorityRoots)
        );
        assert_eq!(
            BybitRecoveryRootCandidates::verified(nonzero, zero, nonzero),
            Err(BybitFreshRecoveryError::AuthorityRoots)
        );
        assert_eq!(
            BybitRecoveryRootCandidates::verified(nonzero, nonzero, zero),
            Err(BybitFreshRecoveryError::AuthorityRoots)
        );
    }

    #[test]
    fn production_integration_stays_unavailable_and_candidate_roots_are_not_authority()
    -> Result<(), BybitFreshRecoveryError> {
        assert_eq!(
            bybit_fresh_recovery_integration_status(),
            Err(BybitFreshRecoveryError::IntegrationUnavailable)
        );
        let roots = BybitRecoveryRootCandidates::verified([1_u8; 32], [2_u8; 32], [3_u8; 32])?;
        let authority =
            BybitRecoveryAuthorityReceipt::explicit_test_fixture(roots, 0, 0, 0, [4_u8; 32])?;
        assert_eq!(authority.owner_route_count(), 0);
        assert_eq!(authority.wal_record_count(), 0);
        assert_eq!(authority.unresolved_count(), 0);
        Ok(())
    }

    #[test]
    fn credential_namespace_commitment_is_secret_free_and_scope_bound()
    -> Result<(), Box<dyn std::error::Error>> {
        let credentials = BybitCredentials::from_values("api-key", "api-secret")?;
        let first = credentials.recovery_namespace_hmac(b"scope-a")?;
        let second = credentials.recovery_namespace_hmac(b"scope-b")?;
        assert_ne!(first, second);
        let rendered = format!("{first:?}");
        assert!(!rendered.contains("api-key"));
        assert!(!rendered.contains("api-secret"));
        Ok(())
    }

    #[test]
    fn six_surface_tags_are_stable_and_distinct() {
        let surfaces = [
            BybitRecoverySurface::Account,
            BybitRecoverySurface::Positions,
            BybitRecoverySurface::UmOrder,
            BybitRecoverySurface::UmConditional,
            BybitRecoverySurface::UmAlgo,
            BybitRecoverySurface::FillsCursor,
        ];
        assert_eq!(
            surfaces
                .into_iter()
                .map(surface_tag)
                .collect::<BTreeSet<_>>()
                .len(),
            6
        );
    }

    #[test]
    fn recovery_symbol_and_deadline_bounds_are_hard() {
        assert!(recovery_bounds_valid(
            BYBIT_RECOVERY_MAX_SYMBOLS,
            1,
            1 + BYBIT_RECOVERY_MAX_DEADLINE_MS
        ));
        assert!(!recovery_bounds_valid(0, 1, 2));
        assert!(!recovery_bounds_valid(BYBIT_RECOVERY_MAX_SYMBOLS + 1, 1, 2));
        assert!(!recovery_bounds_valid(
            1,
            1,
            2 + BYBIT_RECOVERY_MAX_DEADLINE_MS
        ));
    }

    #[test]
    fn unknown_commits_observed_generation_raw_progress_native_identity_and_reason()
    -> Result<(), Box<dyn std::error::Error>> {
        let gateway_binding = GatewayBinding::new(
            VenueId::Bybit,
            GatewayMode::Test,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?;
        let binding = BybitGatewayBinding::new(gateway_binding.clone())?;
        let roots = BybitRecoveryRootCandidates::verified([1_u8; 32], [2_u8; 32], [3_u8; 32])?;
        let authority =
            BybitRecoveryAuthorityReceipt::explicit_test_fixture(roots, 1, 2, 3, [4_u8; 32])?;
        let scope = BybitFreshRecoveryScope {
            anchor_binding: gateway_binding,
            rest_endpoint: binding.config().rest_origin(),
            private_endpoint: binding.config().private_ws(),
            credential_namespace_hmac: [5_u8; 32],
            connection_id_sha256: [6_u8; 32],
            config_digest: "7".repeat(64),
            config_epoch: 1,
            connection_generation: 8,
            private_generation: 9,
            recovered_connection_generation: 7,
            recovered_private_generation: 8,
            attempt_id: 10,
            bound_at_ms: 100,
            deadline_at_ms: 200,
            history_window: BybitHistoryWindow::new(1, 99)?,
            authority,
            symbols: BTreeSet::from(["BTC/USDT".parse()?]),
            commitment_sha256: [11_u8; 32],
        };
        let request = prepare_private_request(
            &binding,
            9,
            10,
            0,
            BybitPrivateSource::Positions,
            None,
            None,
            None,
        )?;
        let raw =
            BybitRawPrivatePayload::from_response(&binding, &request, 101, 102, b"{}".to_vec())?;
        let mut progress = CollectionProgress::new(&scope);
        progress.observe_generation(9);
        progress.observe_raw(&raw);
        let deadline = unknown(&scope, BybitRecoveryUnknownReason::Deadline, &progress);
        let disconnected = unknown(&scope, BybitRecoveryUnknownReason::Disconnected, &progress);
        let BybitFreshRecoveryOutcome::Unknown(deadline) = deadline else {
            return Err("expected unknown".into());
        };
        let BybitFreshRecoveryOutcome::Unknown(disconnected) = disconnected else {
            return Err("expected unknown".into());
        };
        assert_eq!(deadline.observed_private_generation(), Some(9));
        assert_eq!(deadline.received_raw_count(), 1);
        assert_ne!(deadline.received_raw_sha256(), &[0_u8; 32]);
        assert_ne!(deadline.native_identity_sha256(), &[0_u8; 32]);
        assert_ne!(
            deadline.commitment_sha256(),
            disconnected.commitment_sha256()
        );
        Ok(())
    }
}
