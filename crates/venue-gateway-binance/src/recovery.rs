use std::collections::BTreeSet;

use sha2::{Digest, Sha256};
use venue_domain::domain::{
    FieldState, NativeOrderFamily, Order, OrderOwner, OrderPurpose, Symbol,
};
use venue_gateway_api::GatewayMode;

use crate::private::RecentFillsCursor;
use crate::{
    BINANCE_EXECUTION_PROFILE_VERSION, BinanceConfig, BinanceInstrumentRules,
    BinancePrivateReadbackCandidate, BinancePrivateSurface, BinanceRawPrivatePage,
    complete_private_readback,
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

/// Opaque journal commitments captured before any recovery request is issued. These values do not
/// open the journals or confer mutation authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceRecoveryAuthorityRoots {
    owner: [u8; 32],
    wal: [u8; 32],
    unknown: [u8; 32],
}

impl BinanceRecoveryAuthorityRoots {
    pub fn verified(
        owner: [u8; 32],
        wal: [u8; 32],
        unknown: [u8; 32],
    ) -> Result<Self, BinanceRecoveryCollectorError> {
        if [owner, wal, unknown]
            .iter()
            .any(|digest| digest.iter().all(|byte| *byte == 0))
        {
            return Err(BinanceRecoveryCollectorError::AuthorityRoot);
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceRecoveryScopeInput {
    pub config_digest: String,
    pub config_epoch: u64,
    pub connection_generation: u64,
    pub recovered_private_generation: u64,
    pub private_generation: u64,
    pub attempt_id: u64,
    pub started_at_ms: u64,
    pub deadline_at_ms: u64,
    pub authority_roots: BinanceRecoveryAuthorityRoots,
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
    connection_generation: u64,
    recovered_private_generation: u64,
    private_generation: u64,
    attempt_id: u64,
    started_at_ms: u64,
    deadline_at_ms: u64,
    authority_roots: BinanceRecoveryAuthorityRoots,
    symbol_universe: BTreeSet<Symbol>,
    commitment_sha256: [u8; 32],
}

impl BinanceRecoveryCollectionScope {
    pub fn verified(
        config: &BinanceConfig,
        input: BinanceRecoveryScopeInput,
    ) -> Result<Self, BinanceRecoveryCollectorError> {
        let binding = config.gateway_binding();
        if !valid_config_digest(&input.config_digest)
            || input.config_epoch == 0
            || input.connection_generation == 0
            || input.private_generation <= input.recovered_private_generation
            || input.attempt_id == 0
            || input.started_at_ms == 0
            || input.deadline_at_ms <= input.started_at_ms
            || input.deadline_at_ms - input.started_at_ms > BINANCE_RECOVERY_MAX_FRESHNESS_MS
            || input.symbol_universe.is_empty()
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
            connection_generation: input.connection_generation,
            recovered_private_generation: input.recovered_private_generation,
            private_generation: input.private_generation,
            attempt_id: input.attempt_id,
            started_at_ms: input.started_at_ms,
            deadline_at_ms: input.deadline_at_ms,
            authority_roots: input.authority_roots,
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
    pub const fn authority_roots(&self) -> &BinanceRecoveryAuthorityRoots {
        &self.authority_roots
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceRecoveryReplay {
    connection_generation: u64,
    config: BinanceConfig,
    rules: BinanceInstrumentRules,
    initial_fills_cursor: RecentFillsCursor,
    fills_target_through_ms: u64,
    raw_pages: Vec<BinanceRawPrivatePage>,
}

impl BinanceRecoveryReplay {
    #[must_use]
    pub fn new(
        connection_generation: u64,
        config: BinanceConfig,
        rules: BinanceInstrumentRules,
        initial_fills_cursor: RecentFillsCursor,
        fills_target_through_ms: u64,
        raw_pages: Vec<BinanceRawPrivatePage>,
    ) -> Self {
        Self {
            connection_generation,
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
    projection_commitment_sha256: [u8; 32],
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
            || rebuilt.projection_commitment_sha256 != self.projection_commitment_sha256
        {
            return Err(BinanceRecoveryCollectorError::ProjectionCommitment);
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
    let projection_commitment_sha256 = projection_commitment(&scope, &faces, &projections);
    Ok(BinanceFreshRecoveryCandidate {
        scope,
        completed_at_ms,
        owner_routes,
        replays,
        projections,
        faces,
        projection_commitment_sha256,
    })
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
        || replay.connection_generation != scope.connection_generation
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
    faces: &[BinanceRecoveryFaceCommitment],
    projections: &[BinanceRecoverySymbolProjection],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    commit_bytes(&mut digest, b"venue-binance-recovery-projection-v1");
    commit_bytes(&mut digest, &scope.commitment_sha256);
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
            GatewayMode::Test => 1,
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
        input.connection_generation,
        input.recovered_private_generation,
        input.private_generation,
        input.attempt_id,
        input.started_at_ms,
        input.deadline_at_ms,
    ] {
        commit_u64(&mut digest, value);
    }
    commit_bytes(&mut digest, input.authority_roots.owner());
    commit_bytes(&mut digest, input.authority_roots.wal());
    commit_bytes(&mut digest, input.authority_roots.unknown());
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
    #[error("Binance recovery Owner, WAL, and Unknown roots must be nonzero")]
    AuthorityRoot,
    #[error("Binance recovery Owner route is invalid, ambiguous, or out of scope")]
    OwnerRoute,
    #[error("Binance recovery symbol universe is incomplete or duplicated")]
    SymbolUniverse,
    #[error("Binance recovery pages crossed attempt, generation, binding, or deadline")]
    AttemptDrift,
    #[error("Binance recovery raw response replay is incomplete or invalid")]
    Replay,
    #[error("Binance recovery account projections disagree across symbols")]
    ProjectionCommitment,
    #[error("Binance recovery candidate was relabelled under another scope")]
    Relabelled,
    #[error("Binance recovery candidate is stale or outside its collection deadline")]
    Expired,
}

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod tests;
