use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use sha2::{Digest, Sha256};
use venue_domain::domain::Symbol;
use venue_gateway_api::{GatewayMode, VenueId};

use crate::{
    GATE_STAGE7_ORDER_PROFILE_VERSION, GateCredentials, GateFreshRecoveryError, GateGatewayBinding,
    GatePreparedPrivateRead, GatePrivateReadSource, GateRecoverySymbolScope,
    GateRuntimeRecoveryScope, GateTransportLimits,
};

const MAX_RECOVERY_SYMBOLS: usize = 256;
const MAX_RECOVERY_WINDOW_MS: u64 = 3_000;
const MAX_RECOVERY_TOTAL_BYTES: usize = 64 * 1_024 * 1_024;
const MAX_RECOVERY_TOTAL_PAGES: u32 = 10_000;

static NEXT_CONNECTION_GENERATION: AtomicU64 = AtomicU64::new(1);
static NEXT_RECOVERY_ATTEMPT: AtomicU64 = AtomicU64::new(1);
type SessionRegistry = Mutex<BTreeMap<(GatewayMode, String), Weak<GatePrivateSessionSeal>>>;
static AUTHENTICATED_SESSIONS: OnceLock<SessionRegistry> = OnceLock::new();

struct GatePrivateSessionSeal {
    active: AtomicBool,
    collection_epoch: AtomicU64,
    mode: GatewayMode,
    trading_account_id: String,
    rest_origin: String,
    private_ws_endpoint: String,
    connection_generation: u64,
    request_generation: u64,
    limits: GateTransportLimits,
    credential_identity_sha256: [u8; 32],
}

#[derive(Debug)]
struct SymbolCollectionState {
    account_done: bool,
    positions_done: bool,
    regular_done: bool,
    regular_cursor: Option<String>,
    fills_done: bool,
    fills_cursor: Option<String>,
}

#[derive(Debug)]
struct InFlightRead {
    symbol: Symbol,
    source: GatePrivateReadSource,
    cursor: Option<String>,
    maximum_response_bytes: usize,
}

#[derive(Debug)]
struct RecoveryCollectionState {
    symbols: BTreeMap<Symbol, SymbolCollectionState>,
    in_flight: Option<InFlightRead>,
    used_pages: u32,
    used_bytes: usize,
    committed: bool,
}

/// Opaque, single-epoch read-only recovery collection issued by an authenticated Gate transport.
/// It implements neither Serde nor any writer, WAL, capability, or mutation trait.
pub struct GateAuthenticatedRecoverySession {
    seal: Arc<GatePrivateSessionSeal>,
    collection_epoch: u64,
    private_generation: u64,
    attempt_id: u64,
    started_at_ms: u64,
    deadline_at_ms: u64,
    maximum_total_bytes: usize,
    maximum_total_pages: u32,
    symbols: BTreeMap<Symbol, GateRecoverySymbolScope>,
    request_universe_sha256: [u8; 32],
    runtime_scope: Option<GateRuntimeRecoveryScope>,
    collection: Mutex<RecoveryCollectionState>,
}

impl std::fmt::Debug for GateAuthenticatedRecoverySession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GateAuthenticatedRecoverySession")
            .field("mode", &self.seal.mode)
            .field("trading_account_id", &self.seal.trading_account_id)
            .field("connection_generation", &self.seal.connection_generation)
            .field("request_generation", &self.seal.request_generation)
            .field("private_generation", &self.private_generation)
            .field("attempt_id", &self.attempt_id)
            .field("collection_epoch", &self.collection_epoch)
            .field("started_at_ms", &self.started_at_ms)
            .field("deadline_at_ms", &self.deadline_at_ms)
            .field("maximum_total_bytes", &self.maximum_total_bytes)
            .field("maximum_total_pages", &self.maximum_total_pages)
            .field(
                "runtime_scope_sha256",
                &self
                    .runtime_scope
                    .as_ref()
                    .map(GateRuntimeRecoveryScope::commitment_sha256),
            )
            .finish_non_exhaustive()
    }
}

impl GateAuthenticatedRecoverySession {
    #[must_use]
    pub fn mode(&self) -> GatewayMode {
        self.seal.mode
    }

    #[must_use]
    pub fn trading_account_id(&self) -> &str {
        &self.seal.trading_account_id
    }

    #[must_use]
    pub fn rest_origin(&self) -> &str {
        &self.seal.rest_origin
    }

    #[must_use]
    pub fn private_ws_endpoint(&self) -> &str {
        &self.seal.private_ws_endpoint
    }

    #[must_use]
    pub fn connection_generation(&self) -> u64 {
        self.seal.connection_generation
    }

    #[must_use]
    pub fn private_generation(&self) -> u64 {
        self.private_generation
    }

    #[must_use]
    pub fn attempt_id(&self) -> u64 {
        self.attempt_id
    }

    #[must_use]
    pub fn started_at_ms(&self) -> u64 {
        self.started_at_ms
    }

    #[must_use]
    pub fn deadline_at_ms(&self) -> u64 {
        self.deadline_at_ms
    }

    #[must_use]
    pub fn request_universe_sha256(&self) -> &[u8; 32] {
        &self.request_universe_sha256
    }

    pub(crate) fn collection_epoch(&self) -> u64 {
        self.collection_epoch
    }

    #[must_use]
    pub fn is_current(&self) -> bool {
        self.seal.active.load(Ordering::Acquire)
            && self.collection_epoch != 0
            && self.collection_epoch == self.seal.collection_epoch.load(Ordering::Acquire)
    }

    pub(crate) fn validate_current(&self) -> Result<(), GateFreshRecoveryError> {
        if self.is_current() {
            Ok(())
        } else {
            Err(GateFreshRecoveryError::AuthenticatedSessionRevoked)
        }
    }

    pub(crate) fn validate_credentials(
        &self,
        credentials: &GateCredentials,
    ) -> Result<(), GateFreshRecoveryError> {
        if credentials.identity_commitment() == self.seal.credential_identity_sha256 {
            Ok(())
        } else {
            Err(GateFreshRecoveryError::AuthenticatedSessionScope)
        }
    }

    pub(crate) fn validate_request(
        &self,
        request: &GatePreparedPrivateRead,
        now_ms: u64,
    ) -> Result<(), GateFreshRecoveryError> {
        self.validate_current()?;
        let scope = self
            .symbols
            .get(&request.binding.symbol)
            .ok_or(GateFreshRecoveryError::SymbolUniverse)?;
        if request.binding.venue != VenueId::Gate
            || request.binding.mode != self.mode()
            || request.binding.trading_account_id != self.trading_account_id()
            || request.binding != *scope.binding().gateway_binding()
            || request.generation != self.seal.request_generation
            || request.generation != scope.rules().instrument.generation
            || request.attempt != self.attempt_id
            || now_ms < self.started_at_ms
            || now_ms >= self.deadline_at_ms
        {
            return Err(GateFreshRecoveryError::AuthenticatedSessionScope);
        }
        Ok(())
    }

    pub(crate) fn symbol_scopes(&self) -> impl Iterator<Item = GateRecoverySymbolScope> + '_ {
        self.symbols.values().cloned()
    }

    pub(crate) fn transport_limits(&self) -> GateTransportLimits {
        self.seal.limits
    }

    pub(crate) fn request_generation(&self) -> u64 {
        self.seal.request_generation
    }

    pub(crate) const fn runtime_scope(&self) -> Option<&GateRuntimeRecoveryScope> {
        self.runtime_scope.as_ref()
    }

    pub(crate) fn reserve_get(
        &self,
        request: &GatePreparedPrivateRead,
        maximum_response_bytes: usize,
    ) -> Result<(), GateFreshRecoveryError> {
        let mut collection = self
            .collection
            .lock()
            .map_err(|_| GateFreshRecoveryError::AuthenticatedSessionIssuer)?;
        let symbol = collection
            .symbols
            .get(&request.binding.symbol)
            .ok_or(GateFreshRecoveryError::SymbolUniverse)?;
        let expected_cursor = match request.source {
            GatePrivateReadSource::Account if !symbol.account_done => None,
            GatePrivateReadSource::DualPositions if !symbol.positions_done => None,
            GatePrivateReadSource::RegularOrders if !symbol.regular_done => {
                symbol.regular_cursor.clone()
            }
            GatePrivateReadSource::Fills if !symbol.fills_done => symbol.fills_cursor.clone(),
            _ => return Err(GateFreshRecoveryError::SessionConsumed),
        };
        let next_pages = collection
            .used_pages
            .checked_add(1)
            .ok_or(GateFreshRecoveryError::Budget)?;
        let next_bytes = collection
            .used_bytes
            .checked_add(maximum_response_bytes)
            .ok_or(GateFreshRecoveryError::Budget)?;
        if collection.committed
            || collection.in_flight.is_some()
            || request.cursor_before != expected_cursor
            || next_pages > self.maximum_total_pages
            || next_bytes > self.maximum_total_bytes
        {
            return Err(GateFreshRecoveryError::Budget);
        }
        collection.used_pages = next_pages;
        collection.used_bytes = next_bytes;
        collection.in_flight = Some(InFlightRead {
            symbol: request.binding.symbol.clone(),
            source: request.source,
            cursor: request.cursor_before.clone(),
            maximum_response_bytes,
        });
        Ok(())
    }

    pub(crate) fn settle_get(
        &self,
        request: &GatePreparedPrivateRead,
        actual_response_bytes: usize,
        next_cursor: Option<String>,
    ) -> Result<(), GateFreshRecoveryError> {
        let mut collection = self
            .collection
            .lock()
            .map_err(|_| GateFreshRecoveryError::AuthenticatedSessionIssuer)?;
        let in_flight = collection
            .in_flight
            .take()
            .ok_or(GateFreshRecoveryError::SessionConsumed)?;
        if in_flight.symbol != request.binding.symbol
            || in_flight.source != request.source
            || in_flight.cursor != request.cursor_before
            || actual_response_bytes > in_flight.maximum_response_bytes
        {
            return Err(GateFreshRecoveryError::SessionConsumed);
        }
        collection.used_bytes = collection
            .used_bytes
            .checked_sub(in_flight.maximum_response_bytes - actual_response_bytes)
            .ok_or(GateFreshRecoveryError::Budget)?;
        let symbol = collection
            .symbols
            .get_mut(&request.binding.symbol)
            .ok_or(GateFreshRecoveryError::SymbolUniverse)?;
        match request.source {
            GatePrivateReadSource::Account => symbol.account_done = true,
            GatePrivateReadSource::DualPositions => symbol.positions_done = true,
            GatePrivateReadSource::RegularOrders => {
                symbol.regular_done = next_cursor.is_none();
                symbol.regular_cursor = next_cursor;
            }
            GatePrivateReadSource::Fills => {
                symbol.fills_done = next_cursor.is_none();
                symbol.fills_cursor = next_cursor;
            }
        }
        Ok(())
    }

    pub(crate) fn commit_collection(&self) -> Result<(), GateFreshRecoveryError> {
        let mut collection = self
            .collection
            .lock()
            .map_err(|_| GateFreshRecoveryError::AuthenticatedSessionIssuer)?;
        if collection.committed
            || collection.in_flight.is_some()
            || collection.symbols.values().any(|symbol| {
                !symbol.account_done
                    || !symbol.positions_done
                    || !symbol.regular_done
                    || !symbol.fills_done
            })
        {
            return Err(GateFreshRecoveryError::SessionConsumed);
        }
        collection.committed = true;
        Ok(())
    }

    pub(crate) fn revoke(&self) {
        self.seal.active.store(false, Ordering::Release);
    }
}

impl Drop for GateAuthenticatedRecoverySession {
    fn drop(&mut self) {
        // A caller that abandons an incomplete collection must not leave a reusable authenticated
        // epoch behind. A newer epoch has already fenced this one, so it must remain untouched.
        if self.is_current() {
            let committed = self
                .collection
                .lock()
                .map(|collection| collection.committed)
                .unwrap_or(false);
            if !committed {
                self.revoke();
            }
        }
    }
}

pub(crate) struct GateAuthenticatedRecoverySessionLease {
    seal: Arc<GatePrivateSessionSeal>,
}

impl GateAuthenticatedRecoverySessionLease {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn issue(
        binding: &GateGatewayBinding,
        rest_origin: String,
        private_ws_endpoint: String,
        request_generation: u64,
        limits: GateTransportLimits,
        credentials: &GateCredentials,
    ) -> Result<Self, GateFreshRecoveryError> {
        let expected = binding.config();
        if request_generation == 0
            || rest_origin.is_empty()
            || private_ws_endpoint.is_empty()
            || (!cfg!(test)
                && (rest_origin != expected.rest_origin()
                    || private_ws_endpoint != expected.usdt_futures_ws()))
        {
            return Err(GateFreshRecoveryError::AuthenticatedSessionEndpoint);
        }
        let connection_generation = next_serial(&NEXT_CONNECTION_GENERATION)?;
        let seal = Arc::new(GatePrivateSessionSeal {
            active: AtomicBool::new(true),
            collection_epoch: AtomicU64::new(0),
            mode: binding.gateway_binding().mode,
            trading_account_id: binding.gateway_binding().trading_account_id.clone(),
            rest_origin,
            private_ws_endpoint,
            connection_generation,
            request_generation,
            limits,
            credential_identity_sha256: credentials.identity_commitment(),
        });
        let key = (seal.mode, seal.trading_account_id.clone());
        let sessions = AUTHENTICATED_SESSIONS.get_or_init(|| Mutex::new(BTreeMap::new()));
        let mut sessions = sessions
            .lock()
            .map_err(|_| GateFreshRecoveryError::AuthenticatedSessionIssuer)?;
        if let Some(previous) = sessions.insert(key, Arc::downgrade(&seal))
            && let Some(previous) = previous.upgrade()
        {
            previous.active.store(false, Ordering::Release);
        }
        Ok(Self { seal })
    }

    pub(crate) fn begin<I>(
        &self,
        symbols: I,
        deadline_at_ms: u64,
        maximum_total_bytes: usize,
        maximum_total_pages: u32,
    ) -> Result<GateAuthenticatedRecoverySession, GateFreshRecoveryError>
    where
        I: IntoIterator<Item = GateRecoverySymbolScope>,
    {
        self.begin_inner(
            symbols,
            deadline_at_ms,
            maximum_total_bytes,
            maximum_total_pages,
            None,
        )
    }

    pub(crate) fn begin_runtime<I>(
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
        self.begin_inner(
            symbols,
            deadline_at_ms,
            maximum_total_bytes,
            maximum_total_pages,
            Some(runtime_scope),
        )
    }

    fn begin_inner<I>(
        &self,
        symbols: I,
        deadline_at_ms: u64,
        maximum_total_bytes: usize,
        maximum_total_pages: u32,
        runtime_scope: Option<GateRuntimeRecoveryScope>,
    ) -> Result<GateAuthenticatedRecoverySession, GateFreshRecoveryError>
    where
        I: IntoIterator<Item = GateRecoverySymbolScope>,
    {
        let mut scopes = BTreeMap::new();
        for scope in symbols {
            let binding = scope.binding().gateway_binding();
            if binding.mode != self.seal.mode
                || binding.trading_account_id != self.seal.trading_account_id
                || binding.symbol != scope.rules().instrument.symbol
                || scope.rules().instrument.generation != self.seal.request_generation
                || scopes.insert(binding.symbol.clone(), scope).is_some()
            {
                return Err(GateFreshRecoveryError::SymbolScope);
            }
        }
        let started_at_ms = unix_ms()?;
        if !self.seal.active.load(Ordering::Acquire)
            || scopes.is_empty()
            || scopes.len() > MAX_RECOVERY_SYMBOLS
            || deadline_at_ms <= started_at_ms
            || deadline_at_ms - started_at_ms > MAX_RECOVERY_WINDOW_MS
            || maximum_total_bytes < self.seal.limits.maximum_body_bytes()
            || maximum_total_bytes > MAX_RECOVERY_TOTAL_BYTES
            || maximum_total_pages == 0
            || maximum_total_pages > MAX_RECOVERY_TOTAL_PAGES
        {
            return Err(GateFreshRecoveryError::Scope);
        }
        if let Some(runtime_scope) = &runtime_scope {
            runtime_scope.validate_authenticated_universe(
                self.seal.mode,
                &self.seal.trading_account_id,
                scopes.keys(),
            )?;
        }
        let collection_epoch = next_epoch(&self.seal.collection_epoch)?;
        let attempt_id = next_serial(&NEXT_RECOVERY_ATTEMPT)?;
        let private_generation = runtime_scope
            .as_ref()
            .map_or(attempt_id, GateRuntimeRecoveryScope::private_generation);
        let request_universe_sha256 = universe_commitment(
            &self.seal,
            collection_epoch,
            private_generation,
            attempt_id,
            started_at_ms,
            deadline_at_ms,
            maximum_total_bytes,
            maximum_total_pages,
            &scopes,
            runtime_scope.as_ref(),
        );
        let collection_symbols = scopes
            .iter()
            .map(|(symbol, scope)| {
                (
                    symbol.clone(),
                    SymbolCollectionState {
                        account_done: false,
                        positions_done: false,
                        regular_done: false,
                        regular_cursor: None,
                        fills_done: false,
                        fills_cursor: scope.fills_cursor().last_native_id().map(str::to_owned),
                    },
                )
            })
            .collect();
        Ok(GateAuthenticatedRecoverySession {
            seal: Arc::clone(&self.seal),
            collection_epoch,
            private_generation,
            attempt_id,
            started_at_ms,
            deadline_at_ms,
            maximum_total_bytes,
            maximum_total_pages,
            symbols: scopes,
            request_universe_sha256,
            runtime_scope,
            collection: Mutex::new(RecoveryCollectionState {
                symbols: collection_symbols,
                in_flight: None,
                used_pages: 0,
                used_bytes: 0,
                committed: false,
            }),
        })
    }

    pub(crate) fn revoke(&self) {
        self.seal.active.store(false, Ordering::Release);
    }
}

impl Drop for GateAuthenticatedRecoverySessionLease {
    fn drop(&mut self) {
        self.revoke();
    }
}

#[allow(clippy::too_many_arguments)]
fn universe_commitment(
    seal: &GatePrivateSessionSeal,
    collection_epoch: u64,
    private_generation: u64,
    attempt_id: u64,
    started_at_ms: u64,
    deadline_at_ms: u64,
    maximum_total_bytes: usize,
    maximum_total_pages: u32,
    scopes: &BTreeMap<Symbol, GateRecoverySymbolScope>,
    runtime_scope: Option<&GateRuntimeRecoveryScope>,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"venue-gate-authenticated-recovery-universe-v1");
    update_part(&mut digest, format!("{:?}", seal.mode).as_bytes());
    update_part(&mut digest, seal.trading_account_id.as_bytes());
    update_part(&mut digest, seal.rest_origin.as_bytes());
    update_part(&mut digest, seal.private_ws_endpoint.as_bytes());
    digest.update(seal.connection_generation.to_be_bytes());
    digest.update(seal.request_generation.to_be_bytes());
    digest.update(collection_epoch.to_be_bytes());
    digest.update(private_generation.to_be_bytes());
    digest.update(attempt_id.to_be_bytes());
    digest.update(started_at_ms.to_be_bytes());
    digest.update(deadline_at_ms.to_be_bytes());
    digest.update((maximum_total_bytes as u64).to_be_bytes());
    digest.update(maximum_total_pages.to_be_bytes());
    digest.update(seal.credential_identity_sha256);
    match runtime_scope {
        Some(runtime_scope) => {
            digest.update([1]);
            digest.update(runtime_scope.commitment_sha256());
        }
        None => digest.update([0]),
    }
    for source in [
        GatePrivateReadSource::Account,
        GatePrivateReadSource::DualPositions,
        GatePrivateReadSource::RegularOrders,
        GatePrivateReadSource::Fills,
    ] {
        digest.update([source_tag(source)]);
    }
    update_part(&mut digest, b"conditional-orders:unsupported");
    update_part(&mut digest, b"algo-orders:unsupported");
    digest.update(GATE_STAGE7_ORDER_PROFILE_VERSION.to_be_bytes());
    for (symbol, scope) in scopes {
        update_part(&mut digest, symbol.to_string().as_bytes());
        update_part(&mut digest, format!("{:?}", scope.rules()).as_bytes());
        update_part(&mut digest, scope.rules().native_symbol.as_bytes());
        digest.update(scope.rules().instrument.generation.to_be_bytes());
        update_part(
            &mut digest,
            scope
                .fills_cursor()
                .last_native_id()
                .unwrap_or_default()
                .as_bytes(),
        );
    }
    digest.finalize().into()
}

fn update_part(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

const fn source_tag(source: GatePrivateReadSource) -> u8 {
    match source {
        GatePrivateReadSource::Account => 1,
        GatePrivateReadSource::DualPositions => 2,
        GatePrivateReadSource::RegularOrders => 3,
        GatePrivateReadSource::Fills => 4,
    }
}

fn next_serial(counter: &AtomicU64) -> Result<u64, GateFreshRecoveryError> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(1)
        })
        .map_err(|_| GateFreshRecoveryError::Generation)
}

fn next_epoch(counter: &AtomicU64) -> Result<u64, GateFreshRecoveryError> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(1)
        })
        .map(|previous| previous + 1)
        .map_err(|_| GateFreshRecoveryError::Generation)
}

fn unix_ms() -> Result<u64, GateFreshRecoveryError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| GateFreshRecoveryError::Clock)?
        .as_millis();
    u64::try_from(millis).map_err(|_| GateFreshRecoveryError::Clock)
}
