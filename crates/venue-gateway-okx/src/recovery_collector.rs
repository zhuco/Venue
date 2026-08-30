//! Fresh, scope-bound OKX physical recovery collection.
//!
//! The collector freezes account/configuration, position/trade modes, the active private
//! connection generation, Owner/WAL/Unknown commitments, and the complete symbol universe before
//! issuing a signed request. It consumes one shared deadline while collecting all six canonical
//! recovery faces. The result is read-only evidence: it owns no writer, WAL handle, dispatch
//! permit, capability promotion, or mutation surface.

#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::time::{Instant, timeout_at};
use venue_domain::domain::{FieldState, NativeOrderFamily, OrderOwner, Symbol};
use venue_gateway_api::{GatewayBinding, VenueId};

use crate::{
    OkxAccountLevel, OkxActivePrivateSubscription, OkxAlgoOrderKind, OkxConfig, OkxCredentials,
    OkxError, OkxHttpTransport, OkxInstrument, OkxPositionMode, OkxPrivatePageAdvance,
    OkxPrivateReadRequest, OkxPrivateReadScope, OkxPrivateReadbackCandidate, OkxPrivateSurface,
    OkxRawPrivatePage, OkxTradeMode, OkxTransportError, advance_private_page,
    build_account_config_request, build_algo_orders_request, build_balance_request,
    build_fills_request, build_positions_request, build_regular_orders_request,
    complete_private_readback,
};

pub const OKX_FRESH_RECOVERY_SCHEMA_VERSION: u16 = 1;
#[cfg(test)]
const MAX_COLLECTION_DEADLINE: Duration = Duration::from_secs(60);
const MAX_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;
#[cfg(test)]
static NEXT_ATTEMPT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OkxOwnerRoute {
    pub family: NativeOrderFamily,
    pub client_order_id: String,
    pub venue_order_id: String,
    pub owner: OrderOwner,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxRecoveryAuthoritySnapshot {
    owner_routes: Vec<OkxOwnerRoute>,
    owner_root: [u8; 32],
    wal_root: [u8; 32],
    unknown_root: [u8; 32],
}

impl OkxRecoveryAuthoritySnapshot {
    /// Builds the three roots from frozen source material. No digest can be swapped in after the
    /// network turn has started.
    pub fn capture(
        mut owner_routes: Vec<OkxOwnerRoute>,
        wal_snapshot: &[u8],
        unknown_snapshot: &[u8],
    ) -> Result<Self, OkxFreshRecoveryError> {
        if wal_snapshot.is_empty()
            || unknown_snapshot.is_empty()
            || wal_snapshot.len() > MAX_SNAPSHOT_BYTES
            || unknown_snapshot.len() > MAX_SNAPSHOT_BYTES
        {
            return Err(OkxFreshRecoveryError::Authority);
        }
        owner_routes.sort_by(|left, right| {
            (
                left.family,
                left.client_order_id.as_str(),
                left.venue_order_id.as_str(),
            )
                .cmp(&(
                    right.family,
                    right.client_order_id.as_str(),
                    right.venue_order_id.as_str(),
                ))
        });
        let mut client_ids = BTreeSet::new();
        let mut venue_ids = BTreeSet::new();
        for route in &owner_routes {
            if route.owner.validate().is_err()
                || !valid_client_id(&route.client_order_id)
                || !valid_venue_id(&route.venue_order_id)
                || !client_ids.insert((route.family, route.client_order_id.clone()))
                || !venue_ids.insert((route.family, route.venue_order_id.clone()))
            {
                return Err(OkxFreshRecoveryError::Authority);
            }
        }
        let owner_bytes =
            serde_json::to_vec(&owner_routes).map_err(|_| OkxFreshRecoveryError::Authority)?;
        if owner_bytes.len() > MAX_SNAPSHOT_BYTES {
            return Err(OkxFreshRecoveryError::Authority);
        }
        Ok(Self {
            owner_routes,
            owner_root: digest(&owner_bytes),
            wal_root: digest(wal_snapshot),
            unknown_root: digest(unknown_snapshot),
        })
    }

    #[must_use]
    pub const fn owner_root(&self) -> &[u8; 32] {
        &self.owner_root
    }

    #[must_use]
    pub const fn wal_root(&self) -> &[u8; 32] {
        &self.wal_root
    }

    #[must_use]
    pub const fn unknown_root(&self) -> &[u8; 32] {
        &self.unknown_root
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxRecoveryConfiguration {
    config_epoch: u64,
    config_digest: [u8; 32],
    expected_position_mode: OkxPositionMode,
    trade_mode: OkxTradeMode,
    symbol_universe: BTreeSet<Symbol>,
}

impl OkxRecoveryConfiguration {
    pub fn capture(
        config_epoch: u64,
        config_document: &[u8],
        expected_position_mode: OkxPositionMode,
        trade_mode: OkxTradeMode,
        symbol_universe: BTreeSet<Symbol>,
    ) -> Result<Self, OkxFreshRecoveryError> {
        if config_epoch == 0
            || config_document.is_empty()
            || config_document.len() > MAX_SNAPSHOT_BYTES
            || symbol_universe.is_empty()
        {
            return Err(OkxFreshRecoveryError::Configuration);
        }
        Ok(Self {
            config_epoch,
            config_digest: digest(config_document),
            expected_position_mode,
            trade_mode,
            symbol_universe,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxFreshRecoveryScope {
    schema_version: u16,
    binding: GatewayBinding,
    native_instrument_id: String,
    instrument_generation: u64,
    rest_origin: String,
    public_ws_endpoint: String,
    private_ws_endpoint: String,
    config_epoch: u64,
    config_digest: [u8; 32],
    position_mode: OkxPositionMode,
    trade_mode: OkxTradeMode,
    connection_generation: u64,
    recovered_private_generation: u64,
    private_generation: u64,
    private_connection_id: String,
    private_subscription_id: String,
    private_uid: String,
    private_main_uid: String,
    private_account_level: u8,
    private_can_read: bool,
    private_can_trade: bool,
    private_can_withdraw: bool,
    authority: OkxRecoveryAuthoritySnapshot,
    symbol_universe: BTreeSet<Symbol>,
    attempt_id: u64,
    started_at_ms: u64,
    expires_at_ms: u64,
    commitment_sha256: [u8; 32],
}

impl OkxFreshRecoveryScope {
    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub fn rest_origin(&self) -> &str {
        &self.rest_origin
    }

    #[must_use]
    pub fn public_ws_endpoint(&self) -> &str {
        &self.public_ws_endpoint
    }

    #[must_use]
    pub fn private_ws_endpoint(&self) -> &str {
        &self.private_ws_endpoint
    }

    #[must_use]
    pub const fn position_mode(&self) -> OkxPositionMode {
        self.position_mode
    }

    #[must_use]
    pub const fn trade_mode(&self) -> OkxTradeMode {
        self.trade_mode
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
    pub const fn expires_at_ms(&self) -> u64 {
        self.expires_at_ms
    }

    #[must_use]
    pub const fn commitment_sha256(&self) -> &[u8; 32] {
        &self.commitment_sha256
    }

    #[must_use]
    pub const fn authority(&self) -> &OkxRecoveryAuthoritySnapshot {
        &self.authority
    }

    #[must_use]
    pub const fn symbol_universe(&self) -> &BTreeSet<Symbol> {
        &self.symbol_universe
    }

    fn validate_at(&self, now_ms: u64) -> Result<(), OkxFreshRecoveryError> {
        if self.schema_version != OKX_FRESH_RECOVERY_SCHEMA_VERSION
            || self.binding.venue != VenueId::Okx
            || self.instrument_generation == 0
            || self.rest_origin.is_empty()
            || self.public_ws_endpoint.is_empty()
            || self.private_ws_endpoint.is_empty()
            || self.config_epoch == 0
            || self.connection_generation == 0
            || self.private_generation == 0
            || self.private_generation <= self.recovered_private_generation
            || self.private_connection_id.is_empty()
            || self.private_subscription_id.is_empty()
            || self.private_uid.is_empty()
            || self.private_main_uid.is_empty()
            || self.attempt_id == 0
            || self.started_at_ms == 0
            || self.expires_at_ms <= self.started_at_ms
            || now_ms < self.started_at_ms
            || now_ms >= self.expires_at_ms
            || !self.symbol_universe.contains(&self.binding.symbol)
            || self.commitment_sha256 != scope_commitment(self)?
        {
            return Err(OkxFreshRecoveryError::ExpiredOrStale);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum OkxFreshRecoverySurface {
    Account,
    Positions,
    UmOrder,
    UmConditional,
    UmAlgo,
    FillsCursor,
}

impl OkxFreshRecoverySurface {
    const ALL: [Self; 6] = [
        Self::Account,
        Self::Positions,
        Self::UmOrder,
        Self::UmConditional,
        Self::UmAlgo,
        Self::FillsCursor,
    ];

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxFreshRecoveryFace {
    raw_pages: Vec<OkxRawPrivatePage>,
    raw_sha256: [u8; 32],
    projection_sha256: [u8; 32],
    record_count: u64,
}

impl OkxFreshRecoveryFace {
    #[must_use]
    pub fn raw_pages(&self) -> &[OkxRawPrivatePage] {
        &self.raw_pages
    }

    #[must_use]
    pub const fn raw_sha256(&self) -> &[u8; 32] {
        &self.raw_sha256
    }

    #[must_use]
    pub const fn projection_sha256(&self) -> &[u8; 32] {
        &self.projection_sha256
    }

    #[must_use]
    pub const fn record_count(&self) -> u64 {
        self.record_count
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxFreshRecoveryEvidence {
    scope: OkxFreshRecoveryScope,
    readback: OkxPrivateReadbackCandidate,
    faces: BTreeMap<OkxFreshRecoverySurface, OkxFreshRecoveryFace>,
    observed_at_ms: u64,
}

impl OkxFreshRecoveryEvidence {
    #[must_use]
    pub const fn scope(&self) -> &OkxFreshRecoveryScope {
        &self.scope
    }

    #[must_use]
    pub const fn readback(&self) -> &OkxPrivateReadbackCandidate {
        &self.readback
    }

    #[must_use]
    pub fn face(&self, surface: OkxFreshRecoverySurface) -> &OkxFreshRecoveryFace {
        &self.faces[&surface]
    }

    pub fn validate_at(&self, now_ms: u64) -> Result<(), OkxFreshRecoveryError> {
        self.scope.validate_at(now_ms)?;
        if self.observed_at_ms < self.scope.started_at_ms
            || self.observed_at_ms >= self.scope.expires_at_ms
            || self.faces.keys().copied().collect::<BTreeSet<_>>()
                != BTreeSet::from(OkxFreshRecoverySurface::ALL)
        {
            return Err(OkxFreshRecoveryError::RawFork);
        }
        let read_scope = self.readback.scope();
        if read_scope.gateway_binding() != &self.scope.binding
            || read_scope.native_instrument_id() != self.scope.native_instrument_id
            || read_scope.instrument_generation() != self.scope.instrument_generation
            || read_scope.expected_position_mode() != self.scope.position_mode
            || read_scope.trade_mode() != self.scope.trade_mode
            || read_scope.attempt_id() != self.scope.attempt_id
        {
            return Err(OkxFreshRecoveryError::RawFork);
        }
        validate_faces(&self.faces, &self.readback)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OkxFreshRecoveryUnknownKind {
    MissingOwner,
    AmbiguousOwner,
    OwnerProjectionMismatch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxFreshRecoveryUnknownIssue {
    pub kind: OkxFreshRecoveryUnknownKind,
    pub family: Option<NativeOrderFamily>,
    pub venue_order_id: String,
    pub client_order_id: Option<String>,
    pub fill_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxFreshRecoveryUnknown {
    evidence: OkxFreshRecoveryEvidence,
    issues: Vec<OkxFreshRecoveryUnknownIssue>,
}

impl OkxFreshRecoveryUnknown {
    #[must_use]
    pub const fn evidence(&self) -> &OkxFreshRecoveryEvidence {
        &self.evidence
    }

    #[must_use]
    pub fn issues(&self) -> &[OkxFreshRecoveryUnknownIssue] {
        &self.issues
    }

    pub fn validate_at(&self, now_ms: u64) -> Result<(), OkxFreshRecoveryError> {
        self.evidence.validate_at(now_ms)?;
        let expected = owner_issues(&self.evidence)?;
        if expected.is_empty() || expected != self.issues {
            return Err(OkxFreshRecoveryError::RawFork);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OkxFreshRecoveryOutcome {
    Complete(Box<OkxFreshRecoveryEvidence>),
    Unknown(Box<OkxFreshRecoveryUnknown>),
}

pub struct OkxFreshRecoveryCollector {
    instrument: OkxInstrument,
    credentials: OkxCredentials,
    transport: OkxHttpTransport,
    scope: OkxFreshRecoveryScope,
    read_scope: OkxPrivateReadScope,
    deadline: Instant,
}

impl OkxFreshRecoveryCollector {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: OkxConfig,
        instrument: OkxInstrument,
        active_private: &OkxActivePrivateSubscription,
        credentials: OkxCredentials,
        recovery_config: OkxRecoveryConfiguration,
        connection_generation: u64,
        recovered_private_generation: u64,
        authority: OkxRecoveryAuthoritySnapshot,
        collection_deadline: Duration,
        max_body_bytes: usize,
    ) -> Result<Self, OkxFreshRecoveryError> {
        let _ = (
            config,
            instrument,
            active_private,
            credentials,
            recovery_config,
            connection_generation,
            recovered_private_generation,
            authority,
            collection_deadline,
            max_body_bytes,
        );
        Err(OkxFreshRecoveryError::IntegrationUnavailable)
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    fn with_origin(
        config: OkxConfig,
        instrument: OkxInstrument,
        active_private: &OkxActivePrivateSubscription,
        credentials: OkxCredentials,
        recovery_config: OkxRecoveryConfiguration,
        connection_generation: u64,
        recovered_private_generation: u64,
        authority: OkxRecoveryAuthoritySnapshot,
        collection_deadline: Duration,
        max_body_bytes: usize,
        origin: &str,
    ) -> Result<Self, OkxFreshRecoveryError> {
        let transport = OkxHttpTransport::with_origin(
            config.clone(),
            origin,
            MAX_COLLECTION_DEADLINE,
            max_body_bytes,
        )?;
        Self::build(
            config,
            instrument,
            active_private,
            credentials,
            recovery_config,
            connection_generation,
            recovered_private_generation,
            authority,
            collection_deadline,
            transport,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[cfg(test)]
    fn build(
        config: OkxConfig,
        instrument: OkxInstrument,
        active_private: &OkxActivePrivateSubscription,
        credentials: OkxCredentials,
        recovery_config: OkxRecoveryConfiguration,
        connection_generation: u64,
        recovered_private_generation: u64,
        authority: OkxRecoveryAuthoritySnapshot,
        collection_deadline: Duration,
        transport: OkxHttpTransport,
    ) -> Result<Self, OkxFreshRecoveryError> {
        instrument
            .validate_scope(&config)
            .map_err(|_| OkxFreshRecoveryError::Binding)?;
        if collection_deadline.is_zero()
            || collection_deadline > MAX_COLLECTION_DEADLINE
            || connection_generation == 0
            || !recovery_config
                .symbol_universe
                .contains(&config.gateway_binding().symbol)
        {
            return Err(OkxFreshRecoveryError::Configuration);
        }
        let active_scope = active_private.scope();
        let active_profile = active_private.account_profile();
        if active_scope.gateway_binding() != config.gateway_binding()
            || active_scope.native_instrument_id() != instrument.native_id()
            || active_scope.instrument_generation() != instrument.instrument().generation
            || active_scope.trade_mode() != recovery_config.trade_mode
            || active_profile.position_mode() != recovery_config.expected_position_mode
            || active_profile.uid() != active_scope.uid()
            || active_profile.main_uid() != active_scope.main_uid()
        {
            return Err(OkxFreshRecoveryError::Mode);
        }
        if active_scope.private_generation() <= recovered_private_generation {
            return Err(OkxFreshRecoveryError::Generation);
        }
        validate_authority_scope(
            &authority,
            config.gateway_binding(),
            &recovery_config.symbol_universe,
        )?;
        let started_at_ms = unix_ms()?;
        let ttl_ms = u64::try_from(collection_deadline.as_millis())
            .map_err(|_| OkxFreshRecoveryError::Configuration)?;
        let expires_at_ms = started_at_ms
            .checked_add(ttl_ms)
            .ok_or(OkxFreshRecoveryError::Configuration)?;
        if ttl_ms == 0 {
            return Err(OkxFreshRecoveryError::Configuration);
        }
        let attempt_id = next_attempt_id(started_at_ms)?;
        let mut scope = OkxFreshRecoveryScope {
            schema_version: OKX_FRESH_RECOVERY_SCHEMA_VERSION,
            binding: config.gateway_binding().clone(),
            native_instrument_id: instrument.native_id().to_owned(),
            instrument_generation: instrument.instrument().generation,
            rest_origin: config.rest_origin().to_owned(),
            public_ws_endpoint: config.public_ws().to_owned(),
            private_ws_endpoint: config.private_ws().to_owned(),
            config_epoch: recovery_config.config_epoch,
            config_digest: recovery_config.config_digest,
            position_mode: recovery_config.expected_position_mode,
            trade_mode: recovery_config.trade_mode,
            connection_generation,
            recovered_private_generation,
            private_generation: active_scope.private_generation(),
            private_connection_id: active_private.connection_id().to_owned(),
            private_subscription_id: active_private.request_id().to_owned(),
            private_uid: active_profile.uid().to_owned(),
            private_main_uid: active_profile.main_uid().to_owned(),
            private_account_level: account_level_tag(active_profile.level()),
            private_can_read: active_profile.can_read(),
            private_can_trade: active_profile.can_trade(),
            private_can_withdraw: active_profile.can_withdraw(),
            authority,
            symbol_universe: recovery_config.symbol_universe,
            attempt_id,
            started_at_ms,
            expires_at_ms,
            commitment_sha256: [0; 32],
        };
        scope.commitment_sha256 = scope_commitment(&scope)?;
        let read_scope = OkxPrivateReadScope::new(
            &config,
            &instrument,
            scope.position_mode,
            scope.trade_mode,
            scope.attempt_id,
        )?;
        let deadline = Instant::now()
            .checked_add(collection_deadline)
            .ok_or(OkxFreshRecoveryError::Configuration)?;
        Ok(Self {
            instrument,
            credentials,
            transport,
            scope,
            read_scope,
            deadline,
        })
    }

    #[must_use]
    pub const fn scope(&self) -> &OkxFreshRecoveryScope {
        &self.scope
    }

    pub async fn collect(mut self) -> Result<OkxFreshRecoveryOutcome, OkxFreshRecoveryError> {
        self.scope.validate_at(unix_ms()?)?;
        let deadline = self.deadline;
        let raw_pages = timeout_at(deadline, self.collect_pages())
            .await
            .map_err(|_| OkxFreshRecoveryError::Deadline)??;
        self.finish(raw_pages, unix_ms()?)
    }

    async fn collect_pages(&mut self) -> Result<Vec<OkxRawPrivatePage>, OkxFreshRecoveryError> {
        let mut pages = Vec::new();
        for request in [
            build_account_config_request(&self.read_scope)?,
            build_balance_request(&self.read_scope)?,
            build_positions_request(&self.read_scope)?,
        ] {
            pages.push(self.execute_read(request).await?);
        }
        self.collect_paginated(
            build_regular_orders_request(&self.read_scope, 0, None)?,
            &mut pages,
        )
        .await?;
        for kind in OkxAlgoOrderKind::ALL {
            self.collect_paginated(
                build_algo_orders_request(&self.read_scope, kind, 0, None)?,
                &mut pages,
            )
            .await?;
        }
        self.collect_paginated(build_fills_request(&self.read_scope, 0, None)?, &mut pages)
            .await?;
        Ok(pages)
    }

    async fn collect_paginated(
        &self,
        mut request: OkxPrivateReadRequest,
        pages: &mut Vec<OkxRawPrivatePage>,
    ) -> Result<(), OkxFreshRecoveryError> {
        loop {
            let page = self.execute_read(request).await?;
            let advance = advance_private_page(&page)?;
            pages.push(page);
            match advance {
                OkxPrivatePageAdvance::Closed => return Ok(()),
                OkxPrivatePageAdvance::More(next) => request = next,
            }
        }
    }

    async fn execute_read(
        &self,
        request: OkxPrivateReadRequest,
    ) -> Result<OkxRawPrivatePage, OkxFreshRecoveryError> {
        let timestamp = okx_timestamp(SystemTime::now())?;
        let response = self
            .transport
            .execute_read(&self.credentials, &request, &timestamp)
            .await?;
        let page = OkxRawPrivatePage::from_http_response(&request, response)?;
        if page.received_at_ms < self.scope.started_at_ms
            || page.received_at_ms >= self.scope.expires_at_ms
        {
            return Err(OkxFreshRecoveryError::ExpiredOrStale);
        }
        Ok(page)
    }

    fn finish(
        self,
        raw_pages: Vec<OkxRawPrivatePage>,
        completed_at_ms: u64,
    ) -> Result<OkxFreshRecoveryOutcome, OkxFreshRecoveryError> {
        self.scope.validate_at(completed_at_ms)?;
        let readback = complete_private_readback(&self.read_scope, &self.instrument, raw_pages)?;
        let profile = &readback.profile;
        if profile.uid() != self.scope.private_uid
            || profile.main_uid() != self.scope.private_main_uid
            || account_level_tag(profile.level()) != self.scope.private_account_level
            || profile.position_mode() != self.scope.position_mode
            || profile.can_read() != self.scope.private_can_read
            || profile.can_trade() != self.scope.private_can_trade
            || profile.can_withdraw() != self.scope.private_can_withdraw
        {
            return Err(OkxFreshRecoveryError::Mode);
        }
        let faces = build_faces(&readback)?;
        let evidence = OkxFreshRecoveryEvidence {
            scope: self.scope,
            observed_at_ms: readback.observed_at_ms,
            readback,
            faces,
        };
        evidence.validate_at(completed_at_ms)?;
        let issues = owner_issues(&evidence)?;
        if issues.is_empty() {
            Ok(OkxFreshRecoveryOutcome::Complete(Box::new(evidence)))
        } else {
            Ok(OkxFreshRecoveryOutcome::Unknown(Box::new(
                OkxFreshRecoveryUnknown { evidence, issues },
            )))
        }
    }
}

fn build_faces(
    readback: &OkxPrivateReadbackCandidate,
) -> Result<BTreeMap<OkxFreshRecoverySurface, OkxFreshRecoveryFace>, OkxFreshRecoveryError> {
    let family = |family| {
        readback
            .order_family(family)
            .ok_or(OkxFreshRecoveryError::RawFork)
    };
    let definitions = [
        (
            OkxFreshRecoverySurface::Account,
            readback
                .raw_pages
                .iter()
                .filter(|page| {
                    matches!(
                        page.surface,
                        OkxPrivateSurface::AccountConfig | OkxPrivateSurface::Balance
                    )
                })
                .cloned()
                .collect::<Vec<_>>(),
            1,
        ),
        (
            OkxFreshRecoverySurface::Positions,
            readback
                .raw_pages
                .iter()
                .filter(|page| page.surface == OkxPrivateSurface::Positions)
                .cloned()
                .collect(),
            readback.positions.len(),
        ),
        (
            OkxFreshRecoverySurface::UmOrder,
            family(NativeOrderFamily::UmOrder)?.raw_pages.clone(),
            family(NativeOrderFamily::UmOrder)?.orders.len(),
        ),
        (
            OkxFreshRecoverySurface::UmConditional,
            family(NativeOrderFamily::UmConditional)?.raw_pages.clone(),
            family(NativeOrderFamily::UmConditional)?.orders.len(),
        ),
        (
            OkxFreshRecoverySurface::UmAlgo,
            family(NativeOrderFamily::UmAlgo)?.raw_pages.clone(),
            family(NativeOrderFamily::UmAlgo)?.orders.len(),
        ),
        (
            OkxFreshRecoverySurface::FillsCursor,
            readback
                .raw_pages
                .iter()
                .filter(|page| page.surface == OkxPrivateSurface::Fills)
                .cloned()
                .collect(),
            readback.fills.len(),
        ),
    ];
    definitions
        .into_iter()
        .map(|(surface, raw_pages, count)| {
            let record_count = u64::try_from(count).map_err(|_| OkxFreshRecoveryError::RawFork)?;
            let raw_sha256 = raw_pages_digest(&raw_pages)?;
            let projection_sha256 = projection_digest(surface, raw_sha256, record_count);
            Ok((
                surface,
                OkxFreshRecoveryFace {
                    raw_pages,
                    raw_sha256,
                    projection_sha256,
                    record_count,
                },
            ))
        })
        .collect()
}

fn validate_faces(
    faces: &BTreeMap<OkxFreshRecoverySurface, OkxFreshRecoveryFace>,
    readback: &OkxPrivateReadbackCandidate,
) -> Result<(), OkxFreshRecoveryError> {
    if faces != &build_faces(readback)? {
        return Err(OkxFreshRecoveryError::RawFork);
    }
    Ok(())
}

fn owner_issues(
    evidence: &OkxFreshRecoveryEvidence,
) -> Result<Vec<OkxFreshRecoveryUnknownIssue>, OkxFreshRecoveryError> {
    let routes = &evidence.scope.authority.owner_routes;
    let mut issues = Vec::new();
    for family in [
        NativeOrderFamily::UmOrder,
        NativeOrderFamily::UmConditional,
        NativeOrderFamily::UmAlgo,
    ] {
        let readback = evidence
            .readback
            .order_family(family)
            .ok_or(OkxFreshRecoveryError::RawFork)?;
        for canonical in &readback.orders {
            let client_id = match &canonical.order.client_order_id {
                FieldState::Known(value) => Some(value.as_str()),
                _ => None,
            };
            let matches = routes
                .iter()
                .filter(|route| {
                    route.family == family
                        && route.venue_order_id == canonical.order.order_id
                        && client_id.is_some_and(|value| route.client_order_id == value)
                })
                .collect::<Vec<_>>();
            let kind = match matches.as_slice() {
                [] => Some(OkxFreshRecoveryUnknownKind::MissingOwner),
                [route]
                    if route.owner.symbol != canonical.order.symbol
                        || matches!(
                            canonical.order.purpose,
                            FieldState::Known(purpose) if route.owner.purpose != purpose
                        ) =>
                {
                    Some(OkxFreshRecoveryUnknownKind::OwnerProjectionMismatch)
                }
                [_] => None,
                _ => Some(OkxFreshRecoveryUnknownKind::AmbiguousOwner),
            };
            if let Some(kind) = kind {
                issues.push(OkxFreshRecoveryUnknownIssue {
                    kind,
                    family: Some(family),
                    venue_order_id: canonical.order.order_id.clone(),
                    client_order_id: client_id.map(str::to_owned),
                    fill_id: None,
                });
            }
        }
    }
    for fill in &evidence.readback.fills {
        let client_id = match &fill.client_order_id {
            FieldState::Known(value) => Some(value.as_str()),
            _ => None,
        };
        let matches = routes
            .iter()
            .filter(|route| {
                route.venue_order_id == fill.fill.order_id
                    && client_id.is_some_and(|value| route.client_order_id == value)
            })
            .collect::<Vec<_>>();
        let kind = match matches.as_slice() {
            [] => Some(OkxFreshRecoveryUnknownKind::MissingOwner),
            [route] if route.owner.symbol != fill.fill.symbol => {
                Some(OkxFreshRecoveryUnknownKind::OwnerProjectionMismatch)
            }
            [_] => None,
            _ => Some(OkxFreshRecoveryUnknownKind::AmbiguousOwner),
        };
        if let Some(kind) = kind {
            issues.push(OkxFreshRecoveryUnknownIssue {
                kind,
                family: matches.first().map(|route| route.family),
                venue_order_id: fill.fill.order_id.clone(),
                client_order_id: client_id.map(str::to_owned),
                fill_id: Some(fill.fill.fill_id.clone()),
            });
        }
    }
    Ok(issues)
}

#[cfg(test)]
fn validate_authority_scope(
    authority: &OkxRecoveryAuthoritySnapshot,
    binding: &GatewayBinding,
    universe: &BTreeSet<Symbol>,
) -> Result<(), OkxFreshRecoveryError> {
    for route in &authority.owner_routes {
        if route.owner.exchange != "okx"
            || route.owner.account != binding.trading_account_id
            || !universe.contains(&route.owner.symbol)
        {
            return Err(OkxFreshRecoveryError::Authority);
        }
    }
    Ok(())
}

#[derive(Serialize)]
struct ScopeCommitment<'a> {
    schema_version: u16,
    binding: &'a GatewayBinding,
    native_instrument_id: &'a str,
    instrument_generation: u64,
    rest_origin: &'a str,
    public_ws_endpoint: &'a str,
    private_ws_endpoint: &'a str,
    config_epoch: u64,
    config_digest: &'a [u8; 32],
    position_mode: OkxPositionMode,
    trade_mode: OkxTradeMode,
    connection_generation: u64,
    recovered_private_generation: u64,
    private_generation: u64,
    private_connection_id: &'a str,
    private_subscription_id: &'a str,
    private_uid: &'a str,
    private_main_uid: &'a str,
    private_account_level: u8,
    private_can_read: bool,
    private_can_trade: bool,
    private_can_withdraw: bool,
    owner_root: &'a [u8; 32],
    wal_root: &'a [u8; 32],
    unknown_root: &'a [u8; 32],
    symbol_universe: Vec<String>,
    attempt_id: u64,
    started_at_ms: u64,
    expires_at_ms: u64,
}

fn scope_commitment(scope: &OkxFreshRecoveryScope) -> Result<[u8; 32], OkxFreshRecoveryError> {
    let stable = ScopeCommitment {
        schema_version: scope.schema_version,
        binding: &scope.binding,
        native_instrument_id: &scope.native_instrument_id,
        instrument_generation: scope.instrument_generation,
        rest_origin: &scope.rest_origin,
        public_ws_endpoint: &scope.public_ws_endpoint,
        private_ws_endpoint: &scope.private_ws_endpoint,
        config_epoch: scope.config_epoch,
        config_digest: &scope.config_digest,
        position_mode: scope.position_mode,
        trade_mode: scope.trade_mode,
        connection_generation: scope.connection_generation,
        recovered_private_generation: scope.recovered_private_generation,
        private_generation: scope.private_generation,
        private_connection_id: &scope.private_connection_id,
        private_subscription_id: &scope.private_subscription_id,
        private_uid: &scope.private_uid,
        private_main_uid: &scope.private_main_uid,
        private_account_level: scope.private_account_level,
        private_can_read: scope.private_can_read,
        private_can_trade: scope.private_can_trade,
        private_can_withdraw: scope.private_can_withdraw,
        owner_root: &scope.authority.owner_root,
        wal_root: &scope.authority.wal_root,
        unknown_root: &scope.authority.unknown_root,
        symbol_universe: scope
            .symbol_universe
            .iter()
            .map(ToString::to_string)
            .collect(),
        attempt_id: scope.attempt_id,
        started_at_ms: scope.started_at_ms,
        expires_at_ms: scope.expires_at_ms,
    };
    let encoded = serde_json::to_vec(&stable).map_err(|_| OkxFreshRecoveryError::RawFork)?;
    Ok(digest(&encoded))
}

fn raw_pages_digest(pages: &[OkxRawPrivatePage]) -> Result<[u8; 32], OkxFreshRecoveryError> {
    if pages.is_empty() {
        return Err(OkxFreshRecoveryError::RawFork);
    }
    let mut stable = pages.to_vec();
    stable.sort_by_key(|page| (page.surface, page.page_index));
    let encoded = serde_json::to_vec(&stable).map_err(|_| OkxFreshRecoveryError::RawFork)?;
    Ok(digest(&encoded))
}

fn projection_digest(
    surface: OkxFreshRecoverySurface,
    raw_sha256: [u8; 32],
    record_count: u64,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update([surface.tag()]);
    hasher.update(raw_sha256);
    hasher.update(record_count.to_be_bytes());
    hasher.finalize().into()
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn valid_client_id(value: &str) -> bool {
    (1..=32).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_alphanumeric())
}

fn valid_venue_id(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

const fn account_level_tag(level: OkxAccountLevel) -> u8 {
    match level {
        OkxAccountLevel::Futures => 1,
        OkxAccountLevel::MultiCurrencyMargin => 2,
        OkxAccountLevel::PortfolioMargin => 3,
    }
}

#[cfg(test)]
fn next_attempt_id(now_ms: u64) -> Result<u64, OkxFreshRecoveryError> {
    NEXT_ATTEMPT_ID
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
            current.max(now_ms).checked_add(1)
        })
        .map(|previous| previous.max(now_ms).saturating_add(1))
        .map_err(|_| OkxFreshRecoveryError::Generation)
}

fn unix_ms() -> Result<u64, OkxFreshRecoveryError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| OkxFreshRecoveryError::Clock)?
        .as_millis();
    u64::try_from(millis).map_err(|_| OkxFreshRecoveryError::Clock)
}

fn okx_timestamp(now: SystemTime) -> Result<String, OkxFreshRecoveryError> {
    let duration = now
        .duration_since(UNIX_EPOCH)
        .map_err(|_| OkxFreshRecoveryError::Clock)?;
    let seconds = i64::try_from(duration.as_secs()).map_err(|_| OkxFreshRecoveryError::Clock)?;
    let days = seconds.div_euclid(86_400);
    let day_seconds = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days)?;
    let hour = day_seconds / 3_600;
    let minute = day_seconds % 3_600 / 60;
    let second = day_seconds % 60;
    Ok(format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{:03}Z",
        duration.subsec_millis()
    ))
}

fn civil_from_days(days_since_epoch: i64) -> Result<(i64, i64, i64), OkxFreshRecoveryError> {
    let z = days_since_epoch
        .checked_add(719_468)
        .ok_or(OkxFreshRecoveryError::Clock)?;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    if !(0..=9_999).contains(&year) {
        return Err(OkxFreshRecoveryError::Clock);
    }
    Ok((year, month, day))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OkxFreshRecoveryError {
    #[error(
        "OKX production fresh recovery collector is unavailable until authenticated session, complete universe, and durable roots are integrated"
    )]
    IntegrationUnavailable,
    #[error("OKX fresh recovery configuration or deadline is invalid")]
    Configuration,
    #[error("OKX fresh recovery binding or symbol universe does not match")]
    Binding,
    #[error("OKX fresh recovery account, tdMode/mgnMode, or Net/Hedge mode does not match")]
    Mode,
    #[error("OKX fresh recovery private or connection generation is stale")]
    Generation,
    #[error("OKX fresh recovery Owner/WAL/Unknown source snapshot is invalid")]
    Authority,
    #[error("OKX fresh recovery shared deadline elapsed")]
    Deadline,
    #[error("OKX fresh recovery evidence is stale or expired")]
    ExpiredOrStale,
    #[error("OKX fresh recovery raw evidence or projection forked")]
    RawFork,
    #[error("OKX fresh recovery transport failed closed")]
    Transport,
    #[error("system clock is unavailable")]
    Clock,
}

impl From<OkxTransportError> for OkxFreshRecoveryError {
    fn from(_: OkxTransportError) -> Self {
        Self::Transport
    }
}

impl From<OkxError> for OkxFreshRecoveryError {
    fn from(error: OkxError) -> Self {
        match error {
            OkxError::Binding | OkxError::Identity => Self::Binding,
            OkxError::PositionMode => Self::Mode,
            OkxError::Sequence | OkxError::Pagination | OkxError::Payload => Self::RawFork,
            OkxError::Credentials | OkxError::SigningInput | OkxError::Rejected => Self::Transport,
            OkxError::Precision | OkxError::Capability | OkxError::Persistence => Self::RawFork,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        task::JoinHandle,
    };
    use venue_domain::domain::OrderPurpose;
    use venue_gateway_api::{CapabilityFlags, GatewayMode, VenueId};

    use super::*;
    use crate::{
        OKX_LEGACY_CAPABILITY_PROBE_EVIDENCE_CLASS, OkxPrivateWsSession,
        activate_private_subscription, build_private_subscribe, build_ws_login,
        parse_account_profile, parse_instrument, parse_ws_login_ack,
    };

    const INSTRUMENT: &[u8] = include_bytes!("../fixtures/linear-swap-instrument.json");
    const ORDER_ACK: &[u8] = br#"{"id":"request1","event":"subscribe","arg":{"channel":"orders","instType":"SWAP","instId":"BTC-USDT-SWAP"},"connId":"connection1"}"#;
    const ACCOUNT_ACK: &[u8] = br#"{"id":"request1","event":"subscribe","arg":{"channel":"account","ccy":"USDT"},"connId":"connection1"}"#;
    const POSITION_ACK: &[u8] = br#"{"id":"request1","event":"subscribe","arg":{"channel":"positions","instType":"SWAP","instId":"BTC-USDT-SWAP"},"connId":"connection1"}"#;

    struct TestScope {
        config: OkxConfig,
        instrument: OkxInstrument,
        active: OkxActivePrivateSubscription,
    }

    fn profile_payload(mode: OkxPositionMode) -> Vec<u8> {
        let wire_mode = match mode {
            OkxPositionMode::Net => "net_mode",
            OkxPositionMode::LongShort => "long_short_mode",
        };
        format!(
            r#"{{"code":"0","msg":"","data":[{{"uid":"fixture-sub-account","mainUid":"fixture-main-account","acctLv":"3","posMode":"{wire_mode}","perm":"read_only,trade"}}]}}"#
        )
        .into_bytes()
    }

    fn test_scope(
        gateway_mode: GatewayMode,
        position_mode: OkxPositionMode,
    ) -> Result<TestScope, Box<dyn Error>> {
        let config = OkxConfig::for_binding(GatewayBinding::new(
            VenueId::Okx,
            gateway_mode,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)?;
        let instrument = parse_instrument(INSTRUMENT, &config, 9)?;
        let profile = parse_account_profile(&profile_payload(position_mode), position_mode)?;
        let credentials = credentials()?;
        let login = build_ws_login(
            &config,
            &instrument,
            &profile,
            OkxTradeMode::Cross,
            17,
            &credentials,
            "1538054050",
        )?;
        let session: OkxPrivateWsSession = parse_ws_login_ack(
            br#"{"event":"login","code":"0","msg":"","connId":"connection1"}"#,
            &login,
        )?;
        let subscription =
            build_private_subscribe(&session, &config, &instrument, &profile, "request1")?;
        let active = activate_private_subscription(
            &[ORDER_ACK, ACCOUNT_ACK, POSITION_ACK],
            &subscription,
            &session,
            &config,
            &instrument,
            &profile,
        )?;
        Ok(TestScope {
            config,
            instrument,
            active,
        })
    }

    fn credentials() -> Result<OkxCredentials, OkxError> {
        OkxCredentials::from_values("key", "secret", "pass")
    }

    fn recovery_config(
        mode: OkxPositionMode,
    ) -> Result<OkxRecoveryConfiguration, OkxFreshRecoveryError> {
        OkxRecoveryConfiguration::capture(
            4,
            b"immutable config bytes",
            mode,
            OkxTradeMode::Cross,
            BTreeSet::from(["BTC/USDT"
                .parse()
                .map_err(|_| OkxFreshRecoveryError::Configuration)?]),
        )
    }

    fn authority(
        routes: Vec<OkxOwnerRoute>,
    ) -> Result<OkxRecoveryAuthoritySnapshot, OkxFreshRecoveryError> {
        OkxRecoveryAuthoritySnapshot::capture(routes, b"wal snapshot", b"unknown snapshot")
    }

    fn owner_route() -> Result<OkxOwnerRoute, Box<dyn Error>> {
        Ok(OkxOwnerRoute {
            family: NativeOrderFamily::UmOrder,
            client_order_id: "client1".to_owned(),
            venue_order_id: "9003".to_owned(),
            owner: OrderOwner {
                strategy_instance_id: "strategy1".to_owned(),
                run_id: "run1".to_owned(),
                exchange: "okx".to_owned(),
                account: "00000000-0000-4000-8000-000000000001".to_owned(),
                symbol: "BTC/USDT".parse()?,
                purpose: OrderPurpose::Entry,
            },
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn collector(
        scope: TestScope,
        recovery_mode: OkxPositionMode,
        recovered_private_generation: u64,
        routes: Vec<OkxOwnerRoute>,
        deadline: Duration,
        origin: &str,
    ) -> Result<OkxFreshRecoveryCollector, OkxFreshRecoveryError> {
        OkxFreshRecoveryCollector::with_origin(
            scope.config,
            scope.instrument,
            &scope.active,
            credentials().map_err(|_| OkxFreshRecoveryError::Configuration)?,
            recovery_config(recovery_mode)?,
            23,
            recovered_private_generation,
            authority(routes)?,
            deadline,
            1024 * 1024,
            origin,
        )
    }

    async fn server(
        position_mode: OkxPositionMode,
        regular_order: bool,
        delay: Duration,
    ) -> Result<(String, JoinHandle<Result<(), std::io::Error>>), Box<dyn Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let profile = profile_payload(position_mode);
        let handle = tokio::spawn(async move {
            for _ in 0..12 {
                let (mut socket, _) = listener.accept().await?;
                let mut request = Vec::new();
                loop {
                    let mut buffer = [0_u8; 2048];
                    let read = socket.read(&mut buffer).await?;
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                tokio::time::sleep(delay).await;
                let first_line = String::from_utf8_lossy(&request)
                    .lines()
                    .next()
                    .unwrap_or_default()
                    .to_owned();
                let path = first_line.split_whitespace().nth(1).unwrap_or_default();
                let body = response_body(path, &profile, regular_order);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                socket.write_all(response.as_bytes()).await?;
            }
            Ok(())
        });
        Ok((format!("http://{address}"), handle))
    }

    fn response_body(path: &str, profile: &[u8], regular_order: bool) -> String {
        if path == "/api/v5/account/config" {
            return String::from_utf8_lossy(profile).into_owned();
        }
        if path.starts_with("/api/v5/account/balance") {
            return r#"{"code":"0","msg":"","data":[{"uTime":"1","details":[{"ccy":"USDT","eq":"1000","availBal":"900","imr":"50","mmr":"10","uTime":"1"}]}]}"#.to_owned();
        }
        if path.starts_with("/api/v5/account/positions") {
            return r#"{"code":"0","msg":"","data":[]}"#.to_owned();
        }
        if regular_order && path.starts_with("/api/v5/trade/orders-pending") {
            return r#"{"code":"0","msg":"","data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","tdMode":"cross","category":"normal","ordType":"post_only","ordId":"9003","clOrdId":"client1","side":"buy","posSide":"long","sz":"2","accFillSz":"0","px":"60000","avgPx":"","reduceOnly":"false","state":"live","uTime":"1"}]}"#.to_owned();
        }
        r#"{"code":"0","msg":"","data":[]}"#.to_owned()
    }

    #[tokio::test]
    async fn test_and_live_collect_fresh_six_face_hedge_evidence() -> Result<(), Box<dyn Error>> {
        let TestScope {
            config,
            instrument,
            active,
        } = test_scope(GatewayMode::Test, OkxPositionMode::LongShort)?;
        assert!(matches!(
            OkxFreshRecoveryCollector::new(
                config,
                instrument,
                &active,
                credentials()?,
                recovery_config(OkxPositionMode::LongShort)?,
                23,
                16,
                authority(Vec::new())?,
                Duration::from_secs(5),
                1024 * 1024,
            ),
            Err(OkxFreshRecoveryError::IntegrationUnavailable)
        ));
        for gateway_mode in [GatewayMode::Test, GatewayMode::Live] {
            let (origin, server) =
                server(OkxPositionMode::LongShort, false, Duration::ZERO).await?;
            let result = collector(
                test_scope(gateway_mode, OkxPositionMode::LongShort)?,
                OkxPositionMode::LongShort,
                16,
                Vec::new(),
                Duration::from_secs(5),
                &origin,
            )?
            .collect()
            .await?;
            let OkxFreshRecoveryOutcome::Complete(evidence) = result else {
                return Err("empty account unexpectedly unresolved".into());
            };
            assert_eq!(evidence.scope().binding().mode, gateway_mode);
            assert_eq!(evidence.scope().private_generation(), 17);
            assert_eq!(evidence.scope().connection_generation(), 23);
            assert_eq!(evidence.scope().rest_origin(), "https://www.okx.com");
            assert_eq!(
                evidence.scope().private_ws_endpoint(),
                match gateway_mode {
                    GatewayMode::Test => "wss://wspap.okx.com:8443/ws/v5/private",
                    GatewayMode::Live => "wss://ws.okx.com:8443/ws/v5/private",
                }
            );
            assert!(evidence.scope().public_ws_endpoint().contains("/public"));
            assert_eq!(evidence.readback().positions.len(), 2);
            assert_eq!(evidence.faces.len(), 6);
            assert_eq!(
                evidence
                    .face(OkxFreshRecoverySurface::Account)
                    .raw_pages()
                    .len(),
                2
            );
            assert_eq!(crate::capabilities(), CapabilityFlags::empty());
            server.await??;
        }
        Ok(())
    }

    #[tokio::test]
    async fn net_mode_collects_one_signed_zero_leg_and_mode_drift_rejects()
    -> Result<(), Box<dyn Error>> {
        let (origin, server) = server(OkxPositionMode::Net, false, Duration::ZERO).await?;
        let result = collector(
            test_scope(GatewayMode::Test, OkxPositionMode::Net)?,
            OkxPositionMode::Net,
            16,
            Vec::new(),
            Duration::from_secs(5),
            &origin,
        )?
        .collect()
        .await?;
        let OkxFreshRecoveryOutcome::Complete(evidence) = result else {
            return Err("net empty account unexpectedly unresolved".into());
        };
        assert_eq!(evidence.readback().positions.len(), 1);
        assert_eq!(evidence.scope().position_mode(), OkxPositionMode::Net);
        server.await??;

        let scope = test_scope(GatewayMode::Test, OkxPositionMode::LongShort)?;
        assert!(matches!(
            collector(
                scope,
                OkxPositionMode::Net,
                16,
                Vec::new(),
                Duration::from_secs(5),
                "http://127.0.0.1:9"
            ),
            Err(OkxFreshRecoveryError::Mode)
        ));
        let scope = test_scope(GatewayMode::Test, OkxPositionMode::LongShort)?;
        assert!(matches!(
            collector(
                scope,
                OkxPositionMode::LongShort,
                17,
                Vec::new(),
                Duration::from_secs(5),
                "http://127.0.0.1:9"
            ),
            Err(OkxFreshRecoveryError::Generation)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn missing_owner_is_structured_unknown_and_exact_owner_closes_it()
    -> Result<(), Box<dyn Error>> {
        let (origin, first_server) =
            server(OkxPositionMode::LongShort, true, Duration::ZERO).await?;
        let result = collector(
            test_scope(GatewayMode::Test, OkxPositionMode::LongShort)?,
            OkxPositionMode::LongShort,
            16,
            Vec::new(),
            Duration::from_secs(5),
            &origin,
        )?
        .collect()
        .await?;
        let OkxFreshRecoveryOutcome::Unknown(unknown) = result else {
            return Err("missing Owner was not fenced".into());
        };
        assert_eq!(unknown.issues().len(), 1);
        assert_eq!(
            unknown.issues()[0].kind,
            OkxFreshRecoveryUnknownKind::MissingOwner
        );
        first_server.await??;

        let (origin, second_server) =
            server(OkxPositionMode::LongShort, true, Duration::ZERO).await?;
        let result = collector(
            test_scope(GatewayMode::Test, OkxPositionMode::LongShort)?,
            OkxPositionMode::LongShort,
            16,
            vec![owner_route()?],
            Duration::from_secs(5),
            &origin,
        )?
        .collect()
        .await?;
        assert!(matches!(result, OkxFreshRecoveryOutcome::Complete(_)));
        second_server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn raw_fork_and_expiry_invalidate_previously_complete_evidence()
    -> Result<(), Box<dyn Error>> {
        let (origin, server) = server(OkxPositionMode::LongShort, false, Duration::ZERO).await?;
        let result = collector(
            test_scope(GatewayMode::Test, OkxPositionMode::LongShort)?,
            OkxPositionMode::LongShort,
            16,
            Vec::new(),
            Duration::from_secs(5),
            &origin,
        )?
        .collect()
        .await?;
        let OkxFreshRecoveryOutcome::Complete(evidence) = result else {
            return Err("empty account unexpectedly unresolved".into());
        };
        server.await??;
        let now = unix_ms()?;
        evidence.validate_at(now)?;
        assert_eq!(
            evidence.validate_at(evidence.scope().expires_at_ms()),
            Err(OkxFreshRecoveryError::ExpiredOrStale)
        );
        let mut fork = (*evidence).clone();
        fork.readback.raw_pages[0].payload.push(b' ');
        assert_eq!(fork.validate_at(now), Err(OkxFreshRecoveryError::RawFork));
        let mut endpoint_fork = (*evidence).clone();
        endpoint_fork.scope.private_ws_endpoint.push_str("/fork");
        assert_eq!(
            endpoint_fork.validate_at(now),
            Err(OkxFreshRecoveryError::ExpiredOrStale)
        );
        Ok(())
    }

    #[tokio::test]
    async fn one_deadline_covers_the_entire_collection() -> Result<(), Box<dyn Error>> {
        let (origin, server) =
            server(OkxPositionMode::LongShort, false, Duration::from_millis(20)).await?;
        let result = collector(
            test_scope(GatewayMode::Test, OkxPositionMode::LongShort)?,
            OkxPositionMode::LongShort,
            16,
            Vec::new(),
            Duration::from_millis(5),
            &origin,
        )?
        .collect()
        .await;
        assert_eq!(result, Err(OkxFreshRecoveryError::Deadline));
        server.abort();
        Ok(())
    }

    #[test]
    fn old_probe_is_explicitly_relabelled_and_timestamp_is_rfc3339() -> Result<(), Box<dyn Error>> {
        assert_eq!(
            OKX_LEGACY_CAPABILITY_PROBE_EVIDENCE_CLASS,
            "legacy_non_authoritative_capability_probe"
        );
        assert_eq!(
            okx_timestamp(UNIX_EPOCH + Duration::from_millis(1_787_911_200_123))?,
            "2026-08-28T10:00:00.123Z"
        );
        Ok(())
    }
}
