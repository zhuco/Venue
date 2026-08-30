//! Fresh, scope-bound Bitget recovery collection candidate.
//!
//! The production transport can now issue and post-await revalidate an authenticated read-only
//! session. Installation remains unavailable until the runtime can pass its sealed complete
//! universe and durable Owner/WAL/structured-Unknown projection without a `venue-runtime`
//! dependency or caller-supplied digest. Test-only fixtures exercise that final six-face fold. This
//! module grants no capability, writer, WAL, or dispatch authority.

/// The final six-face production fold remains unavailable at the adapter/runtime boundary. Use
/// `connect_authenticated_private_ws`, `begin_recovery_session`, and
/// `collect_authenticated_private_turn` for the real read-only transport portion.
#[derive(Debug, Eq, PartialEq)]
pub struct BitgetFreshRecoveryCollector {
    _sealed: (),
}

impl BitgetFreshRecoveryCollector {
    pub fn begin() -> Result<Self, BitgetFreshRecoveryCollectorError> {
        Err(BitgetFreshRecoveryCollectorError::ProductionUnavailable(
            BitgetRecoveryProductionGap::RuntimeSealedUniverseBridge,
        ))
    }

    #[must_use]
    pub const fn production_gaps() -> &'static [BitgetRecoveryProductionGap] {
        &BitgetRecoveryProductionGap::ALL
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BitgetRecoveryProductionGap {
    /// `venue-runtime` owns the complete registry/config/root scope, but this crate intentionally
    /// does not depend on it and no opaque cross-crate adapter handle exists yet.
    RuntimeSealedUniverseBridge,
    /// Visible orders still need the runtime's replayed exact Owner routes, WAL head, and
    /// structured Unknown set; a caller-provided digest or route list is not durable evidence.
    DurableOwnerWalUnknownProjection,
}

impl BitgetRecoveryProductionGap {
    pub const ALL: [Self; 2] = [
        Self::RuntimeSealedUniverseBridge,
        Self::DurableOwnerWalUnknownProjection,
    ];
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BitgetFreshRecoveryCollectorError {
    #[error("Bitget fresh physical recovery fold is unavailable at production gap {0:?}")]
    ProductionUnavailable(BitgetRecoveryProductionGap),
}

#[cfg(test)]
#[allow(dead_code)]
mod fixture {

    use std::collections::{BTreeMap, BTreeSet};

    use sha2::{Digest, Sha256};
    use venue_domain::domain::{
        CommandId, FieldState, NativeOrderFamily, OrderOwner, OrderPurpose, Symbol,
    };
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

    use crate::{
        BITGET_ORDER_PROFILE_VERSION, BitgetAccountBinding, BitgetConfig, BitgetNodeBridgeError,
        BitgetNodeReadbackCandidate, BitgetOrderFamilyEvidence, BitgetOrderFamilyScope,
        BitgetUnsupportedEvidence,
        instrument::BitgetInstrumentRules,
        private::{BitgetPrivateGenerationCandidate, BitgetPrivateSurface},
    };

    const MAX_CONFIG_DIGEST_LEN: usize = 128;
    const MAX_RECOVERY_SYMBOLS: usize = 256;
    const MAX_RECOVERY_WINDOW_MS: u64 = 60_000;

    /// Exact endpoint and Demo-header identity observed by the read-only collection transport.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct BitgetRecoveryEndpoint {
        mode: GatewayMode,
        rest_origin: String,
        public_ws: String,
        private_ws: String,
        paper_trading: bool,
    }

    impl BitgetRecoveryEndpoint {
        #[must_use]
        pub fn for_mode(mode: GatewayMode) -> Self {
            let config = BitgetConfig::for_mode(mode);
            Self {
                mode,
                rest_origin: config.rest_origin().to_owned(),
                public_ws: config.public_ws().to_owned(),
                private_ws: config.private_ws().to_owned(),
                paper_trading: config.paper_trading(),
            }
        }

        pub fn verified(
            mode: GatewayMode,
            rest_origin: impl Into<String>,
            public_ws: impl Into<String>,
            private_ws: impl Into<String>,
            paper_trading: bool,
        ) -> Result<Self, BitgetRecoveryError> {
            let endpoint = Self {
                mode,
                rest_origin: rest_origin.into(),
                public_ws: public_ws.into(),
                private_ws: private_ws.into(),
                paper_trading,
            };
            if endpoint != Self::for_mode(mode) {
                return Err(BitgetRecoveryError::Endpoint);
            }
            Ok(endpoint)
        }

        #[must_use]
        pub const fn mode(&self) -> GatewayMode {
            self.mode
        }

        #[must_use]
        pub fn rest_origin(&self) -> &str {
            &self.rest_origin
        }

        #[must_use]
        pub fn public_ws(&self) -> &str {
            &self.public_ws
        }

        #[must_use]
        pub fn private_ws(&self) -> &str {
            &self.private_ws
        }

        #[must_use]
        pub const fn paper_trading(&self) -> bool {
            self.paper_trading
        }
    }

    /// Opaque roots recovered before collection starts. The adapter commits them without opening them.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct BitgetRecoveryAuthorityRoots {
        owner: [u8; 32],
        wal: [u8; 32],
        unknown: [u8; 32],
    }

    impl BitgetRecoveryAuthorityRoots {
        pub fn verified(
            owner: [u8; 32],
            wal: [u8; 32],
            unknown: [u8; 32],
        ) -> Result<Self, BitgetRecoveryError> {
            if [owner, wal, unknown]
                .iter()
                .any(|digest| digest.iter().all(|byte| *byte == 0))
            {
                return Err(BitgetRecoveryError::AuthorityRoots);
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

    /// Connection and authenticated private generation are intentionally separate values.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct BitgetRecoveryGenerationWitness {
        connected: bool,
        connection_generation: u64,
        private_generation: u64,
    }

    impl BitgetRecoveryGenerationWitness {
        pub fn connected(
            connection_generation: u64,
            private_generation: u64,
        ) -> Result<Self, BitgetRecoveryError> {
            Self::new(true, connection_generation, private_generation)
        }

        pub fn disconnected(
            connection_generation: u64,
            private_generation: u64,
        ) -> Result<Self, BitgetRecoveryError> {
            Self::new(false, connection_generation, private_generation)
        }

        fn new(
            connected: bool,
            connection_generation: u64,
            private_generation: u64,
        ) -> Result<Self, BitgetRecoveryError> {
            if connection_generation == 0 || private_generation == 0 {
                return Err(BitgetRecoveryError::Generation);
            }
            Ok(Self {
                connected,
                connection_generation,
                private_generation,
            })
        }

        #[must_use]
        pub const fn is_connected(self) -> bool {
            self.connected
        }

        #[must_use]
        pub const fn connection_generation(self) -> u64 {
            self.connection_generation
        }

        #[must_use]
        pub const fn private_generation(self) -> u64 {
            self.private_generation
        }
    }

    /// Immutable scope frozen before the first signed request is issued.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct BitgetRecoveryUnresolvedCommand {
        command_id: CommandId,
        family: NativeOrderFamily,
        symbol: Symbol,
        client_order_id: CommandId,
        venue_order_id: Option<String>,
    }

    impl BitgetRecoveryUnresolvedCommand {
        pub fn verified(
            command_id: impl Into<String>,
            family: NativeOrderFamily,
            symbol: Symbol,
            client_order_id: impl Into<String>,
            venue_order_id: Option<String>,
        ) -> Result<Self, BitgetRecoveryError> {
            if family != NativeOrderFamily::UmOrder
                || venue_order_id
                    .as_ref()
                    .is_some_and(|identity| identity.trim().is_empty())
            {
                return Err(BitgetRecoveryError::UnknownProjection);
            }
            Ok(Self {
                command_id: CommandId::new(command_id.into())
                    .map_err(|_| BitgetRecoveryError::UnknownProjection)?,
                family,
                symbol,
                client_order_id: CommandId::new(client_order_id.into())
                    .map_err(|_| BitgetRecoveryError::UnknownProjection)?,
                venue_order_id,
            })
        }

        #[must_use]
        pub const fn command_id(&self) -> &CommandId {
            &self.command_id
        }

        #[must_use]
        pub const fn family(&self) -> NativeOrderFamily {
            self.family
        }

        #[must_use]
        pub const fn symbol(&self) -> &Symbol {
            &self.symbol
        }

        #[must_use]
        pub const fn client_order_id(&self) -> &CommandId {
            &self.client_order_id
        }

        #[must_use]
        pub fn venue_order_id(&self) -> Option<&str> {
            self.venue_order_id.as_deref()
        }
    }

    /// Immutable fixture scope frozen before the first synthetic signed response is observed.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct BitgetFreshRecoveryScope {
        endpoint: BitgetRecoveryEndpoint,
        account_binding: BitgetAccountBinding,
        trading_account_id: String,
        config_digest: String,
        config_epoch: u64,
        authority_roots: BitgetRecoveryAuthorityRoots,
        unresolved_commands: Vec<BitgetRecoveryUnresolvedCommand>,
        symbols: BTreeSet<Symbol>,
        connection_generation: u64,
        recovered_private_generation: u64,
        private_generation: u64,
        started_at_ms: u64,
        deadline_ms: u64,
        commitment_sha256: [u8; 32],
    }

    impl BitgetFreshRecoveryScope {
        #[allow(clippy::too_many_arguments)]
        pub fn verified<I>(
            endpoint: BitgetRecoveryEndpoint,
            account_binding: BitgetAccountBinding,
            trading_account_id: impl Into<String>,
            config_digest: impl Into<String>,
            config_epoch: u64,
            authority_roots: BitgetRecoveryAuthorityRoots,
            mut unresolved_commands: Vec<BitgetRecoveryUnresolvedCommand>,
            symbols: I,
            connection_generation: u64,
            recovered_private_generation: u64,
            private_generation: u64,
            started_at_ms: u64,
            deadline_ms: u64,
        ) -> Result<Self, BitgetRecoveryError>
        where
            I: IntoIterator<Item = Symbol>,
        {
            let trading_account_id = trading_account_id.into();
            let config_digest = config_digest.into();
            let symbols = symbols.into_iter().collect::<BTreeSet<_>>();
            if symbols.is_empty() || symbols.len() > MAX_RECOVERY_SYMBOLS {
                return Err(BitgetRecoveryError::SymbolUniverse);
            }
            if config_epoch == 0 || !valid_config_digest(&config_digest) {
                return Err(BitgetRecoveryError::Configuration);
            }
            if connection_generation == 0
                || private_generation == 0
                || private_generation <= recovered_private_generation
            {
                return Err(BitgetRecoveryError::Generation);
            }
            if started_at_ms == 0
                || deadline_ms <= started_at_ms
                || deadline_ms.saturating_sub(started_at_ms) > MAX_RECOVERY_WINDOW_MS
            {
                return Err(BitgetRecoveryError::Deadline);
            }
            for symbol in &symbols {
                if symbol.quote() != "USDT" {
                    return Err(BitgetRecoveryError::SymbolUniverse);
                }
                let binding = GatewayBinding::new(
                    VenueId::Bitget,
                    endpoint.mode,
                    trading_account_id.clone(),
                    symbol.clone(),
                )
                .map_err(|_| BitgetRecoveryError::Account)?;
                account_binding
                    .validate_gateway_binding(&binding)
                    .map_err(|_| BitgetRecoveryError::Account)?;
            }
            unresolved_commands.sort_by(|left, right| {
                left.command_id
                    .cmp(&right.command_id)
                    .then_with(|| left.symbol.cmp(&right.symbol))
                    .then_with(|| left.client_order_id.cmp(&right.client_order_id))
            });
            let mut command_ids = BTreeSet::new();
            let mut native_identities = BTreeSet::new();
            for unresolved in &unresolved_commands {
                if !symbols.contains(&unresolved.symbol)
                    || unresolved.family != NativeOrderFamily::UmOrder
                    || !command_ids.insert(unresolved.command_id.clone())
                    || !native_identities.insert((
                        unresolved.family,
                        unresolved.symbol.clone(),
                        unresolved.client_order_id.clone(),
                        unresolved.venue_order_id.clone(),
                    ))
                {
                    return Err(BitgetRecoveryError::UnknownProjection);
                }
            }
            let mut scope = Self {
                endpoint,
                account_binding,
                trading_account_id,
                config_digest,
                config_epoch,
                authority_roots,
                unresolved_commands,
                symbols,
                connection_generation,
                recovered_private_generation,
                private_generation,
                started_at_ms,
                deadline_ms,
                commitment_sha256: [0; 32],
            };
            scope.commitment_sha256 = scope_commitment(&scope);
            Ok(scope)
        }

        #[must_use]
        pub const fn endpoint(&self) -> &BitgetRecoveryEndpoint {
            &self.endpoint
        }

        #[must_use]
        pub const fn account_binding(&self) -> BitgetAccountBinding {
            self.account_binding
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
        pub const fn authority_roots(&self) -> &BitgetRecoveryAuthorityRoots {
            &self.authority_roots
        }

        #[must_use]
        pub fn unresolved_commands(&self) -> &[BitgetRecoveryUnresolvedCommand] {
            &self.unresolved_commands
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
        pub const fn recovered_private_generation(&self) -> u64 {
            self.recovered_private_generation
        }

        #[must_use]
        pub const fn private_generation(&self) -> u64 {
            self.private_generation
        }

        #[must_use]
        pub const fn started_at_ms(&self) -> u64 {
            self.started_at_ms
        }

        #[must_use]
        pub const fn deadline_ms(&self) -> u64 {
            self.deadline_ms
        }

        #[must_use]
        pub const fn commitment_sha256(&self) -> &[u8; 32] {
            &self.commitment_sha256
        }

        fn binding_for(&self, symbol: &Symbol) -> Result<GatewayBinding, BitgetRecoveryError> {
            GatewayBinding::new(
                VenueId::Bitget,
                self.endpoint.mode,
                self.trading_account_id.clone(),
                symbol.clone(),
            )
            .map_err(|_| BitgetRecoveryError::Account)
        }
    }

    /// Exact persisted Owner projection for one visible regular order.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct BitgetRecoveryOwnerRoute {
        client_order_id: String,
        venue_order_id: String,
        owner: OrderOwner,
    }

    impl BitgetRecoveryOwnerRoute {
        pub fn verified(
            client_order_id: impl Into<String>,
            venue_order_id: impl Into<String>,
            owner: OrderOwner,
        ) -> Result<Self, BitgetRecoveryError> {
            let client_order_id = client_order_id.into();
            let venue_order_id = venue_order_id.into();
            if client_order_id.trim().is_empty()
                || venue_order_id.trim().is_empty()
                || owner.validate().is_err()
            {
                return Err(BitgetRecoveryError::OwnerRoute);
            }
            Ok(Self {
                client_order_id,
                venue_order_id,
                owner,
            })
        }

        #[must_use]
        pub fn client_order_id(&self) -> &str {
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

    #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
    pub enum BitgetRecoverySurface {
        Account,
        Positions,
        UmOrder,
        UmConditional,
        UmAlgo,
        FillsCursor,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub enum BitgetRecoveryCoverage {
        Complete {
            evidence_sha256: [u8; 32],
            record_count: u64,
        },
        Unsupported {
            evidence_sha256: [u8; 32],
            profile_version: u64,
        },
    }

    impl BitgetRecoveryCoverage {
        #[must_use]
        pub const fn evidence_sha256(&self) -> &[u8; 32] {
            match self {
                Self::Complete {
                    evidence_sha256, ..
                }
                | Self::Unsupported {
                    evidence_sha256, ..
                } => evidence_sha256,
            }
        }
    }

    /// Completed read-only candidate. Private fields and no Deserialize implementation prevent relabel.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct BitgetFreshRecoveryCandidate {
        scope: BitgetFreshRecoveryScope,
        completed_at_ms: u64,
        readbacks: BTreeMap<Symbol, BitgetNodeReadbackCandidate>,
        owner_routes: Vec<BitgetRecoveryOwnerRoute>,
        surfaces: BTreeMap<BitgetRecoverySurface, BitgetRecoveryCoverage>,
        raw_commitment_sha256: [u8; 32],
        commitment_sha256: [u8; 32],
    }

    impl BitgetFreshRecoveryCandidate {
        #[must_use]
        pub const fn scope(&self) -> &BitgetFreshRecoveryScope {
            &self.scope
        }

        #[must_use]
        pub const fn completed_at_ms(&self) -> u64 {
            self.completed_at_ms
        }

        #[must_use]
        pub fn readback(&self, symbol: &Symbol) -> Option<&BitgetNodeReadbackCandidate> {
            self.readbacks.get(symbol)
        }

        #[must_use]
        pub fn owner_routes(&self) -> &[BitgetRecoveryOwnerRoute] {
            &self.owner_routes
        }

        #[must_use]
        pub fn coverage(&self, surface: BitgetRecoverySurface) -> &BitgetRecoveryCoverage {
            &self.surfaces[&surface]
        }

        #[must_use]
        pub const fn raw_commitment_sha256(&self) -> &[u8; 32] {
            &self.raw_commitment_sha256
        }

        #[must_use]
        pub const fn commitment_sha256(&self) -> &[u8; 32] {
            &self.commitment_sha256
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum BitgetRecoveryUnknownReason {
        Disconnected,
        ConnectionGenerationChanged,
        PrivateGenerationChanged,
    }

    /// Terminal read-only uncertainty. It cannot be resumed or converted into a completed candidate.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct BitgetRecoveryUnknown {
        scope_commitment_sha256: [u8; 32],
        evidence_commitment_sha256: [u8; 32],
        reason: BitgetRecoveryUnknownReason,
        unresolved_commands: Vec<BitgetRecoveryUnresolvedCommand>,
        observed_connection_generation: u64,
        observed_private_generation: u64,
    }

    impl BitgetRecoveryUnknown {
        #[must_use]
        pub const fn reason(&self) -> BitgetRecoveryUnknownReason {
            self.reason
        }

        #[must_use]
        pub const fn scope_commitment_sha256(&self) -> &[u8; 32] {
            &self.scope_commitment_sha256
        }

        #[must_use]
        pub const fn evidence_commitment_sha256(&self) -> &[u8; 32] {
            &self.evidence_commitment_sha256
        }

        #[must_use]
        pub fn unresolved_commands(&self) -> &[BitgetRecoveryUnresolvedCommand] {
            &self.unresolved_commands
        }

        #[must_use]
        pub const fn observed_connection_generation(&self) -> u64 {
            self.observed_connection_generation
        }

        #[must_use]
        pub const fn observed_private_generation(&self) -> u64 {
            self.observed_private_generation
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub enum BitgetRecoveryCollectionOutcome {
        Complete(Box<BitgetFreshRecoveryCandidate>),
        Unknown(BitgetRecoveryUnknown),
    }

    /// One-shot collector. Any returned error or Unknown consumes the collection turn.
    #[derive(Debug)]
    pub struct BitgetFreshRecoveryCollector {
        scope: BitgetFreshRecoveryScope,
        readbacks: BTreeMap<Symbol, BitgetNodeReadbackCandidate>,
    }

    impl BitgetFreshRecoveryCollector {
        pub fn begin(
            scope: BitgetFreshRecoveryScope,
            observed_endpoint: &BitgetRecoveryEndpoint,
            generation: BitgetRecoveryGenerationWitness,
            now_ms: u64,
        ) -> Result<Self, BitgetRecoveryError> {
            if observed_endpoint != &scope.endpoint {
                return Err(BitgetRecoveryError::Endpoint);
            }
            if !generation.connected
                || generation.connection_generation != scope.connection_generation
                || generation.private_generation != scope.private_generation
            {
                return Err(BitgetRecoveryError::Generation);
            }
            validate_time(&scope, now_ms)?;
            Ok(Self {
                scope,
                readbacks: BTreeMap::new(),
            })
        }

        /// Adds one symbol's signed five-face readback and folds it into six canonical recovery faces.
        pub fn push_symbol(
            &mut self,
            rules: &BitgetInstrumentRules,
            private: BitgetPrivateGenerationCandidate,
            now_ms: u64,
        ) -> Result<(), BitgetRecoveryError> {
            validate_time(&self.scope, now_ms)?;
            let symbol = private.binding.symbol.clone();
            if !self.scope.symbols.contains(&symbol)
                || private.binding != self.scope.binding_for(&symbol)?
                || private.attempt_id != self.scope.private_generation
                || private.generation != self.scope.connection_generation
                || private.observed_at_ms < self.scope.started_at_ms
                || private.observed_at_ms > now_ms
                || private.raw_pages.iter().any(|raw| {
                    raw.received_at_ms < self.scope.started_at_ms
                        || raw.received_at_ms > now_ms
                        || raw.received_at_ms >= self.scope.deadline_ms
                })
            {
                return Err(BitgetRecoveryError::EvidenceScope);
            }
            if rules.raw.binding != private.binding
                || rules.raw.expires_at_ms < self.scope.deadline_ms
                || rules.raw.observed_at_ms > now_ms
            {
                return Err(BitgetRecoveryError::EvidenceScope);
            }
            let candidate = BitgetNodeReadbackCandidate::validate(
                BitgetOrderFamilyScope {
                    binding: private.binding.clone(),
                    profile_version: BITGET_ORDER_PROFILE_VERSION,
                    attempt_id: self.scope.private_generation,
                    generation: self.scope.connection_generation,
                    observed_at_ms: private.observed_at_ms,
                    expires_at_ms: self.scope.deadline_ms,
                },
                rules,
                now_ms,
                [
                    BitgetOrderFamilyEvidence::Regular(Box::new(private)),
                    BitgetOrderFamilyEvidence::Unsupported(BitgetUnsupportedEvidence::conditional(
                        BITGET_ORDER_PROFILE_VERSION,
                    )),
                    BitgetOrderFamilyEvidence::Unsupported(BitgetUnsupportedEvidence::algo(
                        BITGET_ORDER_PROFILE_VERSION,
                    )),
                ],
            )?;
            if self.readbacks.insert(symbol, candidate).is_some() {
                return Err(BitgetRecoveryError::DuplicateSymbol);
            }
            Ok(())
        }

        pub fn finish<I>(
            self,
            observed_endpoint: &BitgetRecoveryEndpoint,
            generation: BitgetRecoveryGenerationWitness,
            owner_routes: I,
            now_ms: u64,
        ) -> Result<BitgetRecoveryCollectionOutcome, BitgetRecoveryError>
        where
            I: IntoIterator<Item = BitgetRecoveryOwnerRoute>,
        {
            validate_time(&self.scope, now_ms)?;
            if observed_endpoint != &self.scope.endpoint {
                return Err(BitgetRecoveryError::Endpoint);
            }
            if !generation.connected {
                return Ok(BitgetRecoveryCollectionOutcome::Unknown(
                    self.unknown(BitgetRecoveryUnknownReason::Disconnected, generation),
                ));
            }
            if generation.connection_generation != self.scope.connection_generation {
                return Ok(BitgetRecoveryCollectionOutcome::Unknown(self.unknown(
                    BitgetRecoveryUnknownReason::ConnectionGenerationChanged,
                    generation,
                )));
            }
            if generation.private_generation != self.scope.private_generation {
                return Ok(BitgetRecoveryCollectionOutcome::Unknown(self.unknown(
                    BitgetRecoveryUnknownReason::PrivateGenerationChanged,
                    generation,
                )));
            }
            if self.readbacks.keys().collect::<BTreeSet<_>>()
                != self.scope.symbols.iter().collect::<BTreeSet<_>>()
            {
                return Err(BitgetRecoveryError::MissingSymbol);
            }
            validate_account_projection(&self.readbacks)?;
            let owner_routes = validate_owner_routes(&self.scope, &self.readbacks, owner_routes)?;
            let raw_commitment_sha256 = raw_commitment(&self.scope, &self.readbacks);
            let surfaces = surface_coverages(&self.scope, &self.readbacks)?;
            let commitment_sha256 = candidate_commitment(
                &self.scope,
                now_ms,
                &raw_commitment_sha256,
                &surfaces,
                &owner_routes,
            );
            Ok(BitgetRecoveryCollectionOutcome::Complete(Box::new(
                BitgetFreshRecoveryCandidate {
                    scope: self.scope,
                    completed_at_ms: now_ms,
                    readbacks: self.readbacks,
                    owner_routes,
                    surfaces,
                    raw_commitment_sha256,
                    commitment_sha256,
                },
            )))
        }

        fn unknown(
            &self,
            reason: BitgetRecoveryUnknownReason,
            observed: BitgetRecoveryGenerationWitness,
        ) -> BitgetRecoveryUnknown {
            BitgetRecoveryUnknown {
                scope_commitment_sha256: self.scope.commitment_sha256,
                evidence_commitment_sha256: raw_commitment(&self.scope, &self.readbacks),
                reason,
                unresolved_commands: self.scope.unresolved_commands.clone(),
                observed_connection_generation: observed.connection_generation,
                observed_private_generation: observed.private_generation,
            }
        }
    }

    fn validate_time(
        scope: &BitgetFreshRecoveryScope,
        now_ms: u64,
    ) -> Result<(), BitgetRecoveryError> {
        if now_ms < scope.started_at_ms {
            return Err(BitgetRecoveryError::Clock);
        }
        if now_ms >= scope.deadline_ms {
            return Err(BitgetRecoveryError::Expired);
        }
        Ok(())
    }

    fn validate_account_projection(
        readbacks: &BTreeMap<Symbol, BitgetNodeReadbackCandidate>,
    ) -> Result<(), BitgetRecoveryError> {
        let mut values = readbacks.values();
        let first = values.next().ok_or(BitgetRecoveryError::MissingSymbol)?;
        if values.any(|candidate| {
            candidate.private().balance != first.private().balance
                || candidate.private().hedge_mode != first.private().hedge_mode
        }) {
            return Err(BitgetRecoveryError::AccountProjection);
        }
        Ok(())
    }

    fn validate_owner_routes<I>(
        scope: &BitgetFreshRecoveryScope,
        readbacks: &BTreeMap<Symbol, BitgetNodeReadbackCandidate>,
        owner_routes: I,
    ) -> Result<Vec<BitgetRecoveryOwnerRoute>, BitgetRecoveryError>
    where
        I: IntoIterator<Item = BitgetRecoveryOwnerRoute>,
    {
        let mut supplied = BTreeMap::new();
        for route in owner_routes {
            let key = (
                route.owner.symbol.clone(),
                route.client_order_id.clone(),
                route.venue_order_id.clone(),
            );
            if route.owner.exchange != VenueId::Bitget.as_str()
                || route.owner.account != scope.trading_account_id
                || !scope.symbols.contains(&route.owner.symbol)
                || supplied.insert(key, route).is_some()
            {
                return Err(BitgetRecoveryError::OwnerRoute);
            }
        }
        let mut expected = BTreeSet::new();
        for (symbol, candidate) in readbacks {
            for order in &candidate.private().orders {
                let FieldState::Known(client_order_id) = &order.client_order_id else {
                    return Err(BitgetRecoveryError::OwnerRoute);
                };
                expected.insert((
                    symbol.clone(),
                    client_order_id.clone(),
                    order.order_id.clone(),
                ));
            }
        }
        if supplied.keys().cloned().collect::<BTreeSet<_>>() != expected {
            return Err(BitgetRecoveryError::OwnerRoute);
        }
        Ok(supplied.into_values().collect())
    }

    fn surface_coverages(
        scope: &BitgetFreshRecoveryScope,
        readbacks: &BTreeMap<Symbol, BitgetNodeReadbackCandidate>,
    ) -> Result<BTreeMap<BitgetRecoverySurface, BitgetRecoveryCoverage>, BitgetRecoveryError> {
        let position_count = readbacks.values().try_fold(0_u64, |count, candidate| {
            add_count(count, candidate.private().positions.len())
        })?;
        let order_count = readbacks.values().try_fold(0_u64, |count, candidate| {
            add_count(count, candidate.private().orders.len())
        })?;
        let fill_count = readbacks.values().try_fold(0_u64, |count, candidate| {
            add_count(count, candidate.private().fills.len())
        })?;
        let mut surfaces = BTreeMap::new();
        for (surface, count) in [
            (BitgetRecoverySurface::Account, 1),
            (BitgetRecoverySurface::Positions, position_count),
            (BitgetRecoverySurface::UmOrder, order_count),
            (BitgetRecoverySurface::FillsCursor, fill_count),
        ] {
            surfaces.insert(
                surface,
                BitgetRecoveryCoverage::Complete {
                    evidence_sha256: surface_commitment(scope, readbacks, surface),
                    record_count: count,
                },
            );
        }
        for surface in [
            BitgetRecoverySurface::UmConditional,
            BitgetRecoverySurface::UmAlgo,
        ] {
            surfaces.insert(
                surface,
                BitgetRecoveryCoverage::Unsupported {
                    evidence_sha256: surface_commitment(scope, readbacks, surface),
                    profile_version: BITGET_ORDER_PROFILE_VERSION,
                },
            );
        }
        Ok(surfaces)
    }

    fn add_count(count: u64, value: usize) -> Result<u64, BitgetRecoveryError> {
        let value = u64::try_from(value).map_err(|_| BitgetRecoveryError::Coverage)?;
        count
            .checked_add(value)
            .ok_or(BitgetRecoveryError::Coverage)
    }

    fn scope_commitment(scope: &BitgetFreshRecoveryScope) -> [u8; 32] {
        let mut digest = Sha256::new();
        commit_bytes(&mut digest, b"venue-bitget-fresh-recovery-scope-v1");
        commit_bytes(&mut digest, scope.endpoint.mode.as_str().as_bytes());
        commit_str(&mut digest, &scope.endpoint.rest_origin);
        commit_str(&mut digest, &scope.endpoint.public_ws);
        commit_str(&mut digest, &scope.endpoint.private_ws);
        commit_bool(&mut digest, scope.endpoint.paper_trading);
        commit_str(&mut digest, scope.account_binding.as_str());
        commit_str(&mut digest, &scope.trading_account_id);
        commit_str(&mut digest, &scope.config_digest);
        commit_u64(&mut digest, scope.config_epoch);
        commit_bytes(&mut digest, &scope.authority_roots.owner);
        commit_bytes(&mut digest, &scope.authority_roots.wal);
        commit_bytes(&mut digest, &scope.authority_roots.unknown);
        commit_u64(&mut digest, scope.unresolved_commands.len() as u64);
        for unresolved in &scope.unresolved_commands {
            commit_str(&mut digest, unresolved.command_id.as_str());
            commit_bytes(&mut digest, &[native_family_tag(unresolved.family)]);
            commit_str(&mut digest, &unresolved.symbol.to_string());
            commit_str(&mut digest, unresolved.client_order_id.as_str());
            commit_option_str(&mut digest, unresolved.venue_order_id.as_deref());
        }
        commit_u64(&mut digest, scope.symbols.len() as u64);
        for symbol in &scope.symbols {
            commit_str(&mut digest, &symbol.to_string());
        }
        commit_u64(&mut digest, scope.connection_generation);
        commit_u64(&mut digest, scope.recovered_private_generation);
        commit_u64(&mut digest, scope.private_generation);
        commit_u64(&mut digest, scope.started_at_ms);
        commit_u64(&mut digest, scope.deadline_ms);
        digest.finalize().into()
    }

    fn raw_commitment(
        scope: &BitgetFreshRecoveryScope,
        readbacks: &BTreeMap<Symbol, BitgetNodeReadbackCandidate>,
    ) -> [u8; 32] {
        let mut digest = Sha256::new();
        commit_bytes(&mut digest, b"venue-bitget-fresh-recovery-raw-v1");
        commit_bytes(&mut digest, &scope.commitment_sha256);
        commit_u64(&mut digest, readbacks.len() as u64);
        for (symbol, candidate) in readbacks {
            commit_str(&mut digest, &symbol.to_string());
            commit_str(&mut digest, candidate.commitment_sha256());
            commit_u64(&mut digest, candidate.private().raw_pages.len() as u64);
            for raw in &candidate.private().raw_pages {
                commit_bytes(&mut digest, &[private_surface_tag(raw.surface)]);
                commit_u64(&mut digest, raw.attempt_id);
                commit_u64(&mut digest, raw.generation);
                commit_u64(&mut digest, u64::from(raw.page_index));
                commit_option_str(&mut digest, raw.request_cursor.as_deref());
                commit_option_u64(&mut digest, raw.fill_history_start_ms);
                commit_u64(&mut digest, raw.received_at_ms);
                commit_str(&mut digest, &raw.payload_sha256);
                commit_str(&mut digest, &raw.payload);
            }
        }
        digest.finalize().into()
    }

    fn surface_commitment(
        scope: &BitgetFreshRecoveryScope,
        readbacks: &BTreeMap<Symbol, BitgetNodeReadbackCandidate>,
        surface: BitgetRecoverySurface,
    ) -> [u8; 32] {
        let mut digest = Sha256::new();
        commit_bytes(&mut digest, b"venue-bitget-fresh-recovery-surface-v1");
        commit_bytes(&mut digest, &scope.commitment_sha256);
        commit_bytes(&mut digest, &[recovery_surface_tag(surface)]);
        if matches!(
            surface,
            BitgetRecoverySurface::UmConditional | BitgetRecoverySurface::UmAlgo
        ) {
            commit_u64(&mut digest, BITGET_ORDER_PROFILE_VERSION);
        }
        for (symbol, candidate) in readbacks {
            commit_str(&mut digest, &symbol.to_string());
            for raw in candidate
                .private()
                .raw_pages
                .iter()
                .filter(|raw| raw_surface_belongs_to_recovery_surface(raw.surface, surface))
            {
                commit_bytes(&mut digest, &[private_surface_tag(raw.surface)]);
                commit_u64(&mut digest, u64::from(raw.page_index));
                commit_str(&mut digest, &raw.payload_sha256);
            }
        }
        digest.finalize().into()
    }

    fn candidate_commitment(
        scope: &BitgetFreshRecoveryScope,
        completed_at_ms: u64,
        raw_commitment_sha256: &[u8; 32],
        surfaces: &BTreeMap<BitgetRecoverySurface, BitgetRecoveryCoverage>,
        owner_routes: &[BitgetRecoveryOwnerRoute],
    ) -> [u8; 32] {
        let mut digest = Sha256::new();
        commit_bytes(&mut digest, b"venue-bitget-fresh-recovery-candidate-v1");
        commit_bytes(&mut digest, &scope.commitment_sha256);
        commit_u64(&mut digest, completed_at_ms);
        commit_bytes(&mut digest, raw_commitment_sha256);
        for (surface, coverage) in surfaces {
            commit_bytes(&mut digest, &[recovery_surface_tag(*surface)]);
            match coverage {
                BitgetRecoveryCoverage::Complete {
                    evidence_sha256,
                    record_count,
                } => {
                    commit_bytes(&mut digest, &[1]);
                    commit_bytes(&mut digest, evidence_sha256);
                    commit_u64(&mut digest, *record_count);
                }
                BitgetRecoveryCoverage::Unsupported {
                    evidence_sha256,
                    profile_version,
                } => {
                    commit_bytes(&mut digest, &[2]);
                    commit_bytes(&mut digest, evidence_sha256);
                    commit_u64(&mut digest, *profile_version);
                }
            }
        }
        commit_u64(&mut digest, owner_routes.len() as u64);
        for route in owner_routes {
            commit_str(&mut digest, &route.client_order_id);
            commit_str(&mut digest, &route.venue_order_id);
            commit_str(&mut digest, &route.owner.strategy_instance_id);
            commit_str(&mut digest, &route.owner.run_id);
            commit_str(&mut digest, &route.owner.exchange);
            commit_str(&mut digest, &route.owner.account);
            commit_str(&mut digest, &route.owner.symbol.to_string());
            commit_bytes(&mut digest, &[order_purpose_tag(route.owner.purpose)]);
        }
        digest.finalize().into()
    }

    fn raw_surface_belongs_to_recovery_surface(
        raw: BitgetPrivateSurface,
        recovery: BitgetRecoverySurface,
    ) -> bool {
        match recovery {
            BitgetRecoverySurface::Account => matches!(
                raw,
                BitgetPrivateSurface::Account | BitgetPrivateSurface::Settings
            ),
            BitgetRecoverySurface::Positions => raw == BitgetPrivateSurface::Positions,
            BitgetRecoverySurface::UmOrder => raw == BitgetPrivateSurface::RegularOrders,
            BitgetRecoverySurface::FillsCursor => raw == BitgetPrivateSurface::Fills,
            BitgetRecoverySurface::UmConditional | BitgetRecoverySurface::UmAlgo => false,
        }
    }

    const fn private_surface_tag(surface: BitgetPrivateSurface) -> u8 {
        match surface {
            BitgetPrivateSurface::Account => 1,
            BitgetPrivateSurface::Settings => 2,
            BitgetPrivateSurface::Positions => 3,
            BitgetPrivateSurface::RegularOrders => 4,
            BitgetPrivateSurface::Fills => 5,
        }
    }

    const fn recovery_surface_tag(surface: BitgetRecoverySurface) -> u8 {
        match surface {
            BitgetRecoverySurface::Account => 1,
            BitgetRecoverySurface::Positions => 2,
            BitgetRecoverySurface::UmOrder => 3,
            BitgetRecoverySurface::UmConditional => 4,
            BitgetRecoverySurface::UmAlgo => 5,
            BitgetRecoverySurface::FillsCursor => 6,
        }
    }

    const fn order_purpose_tag(purpose: OrderPurpose) -> u8 {
        match purpose {
            OrderPurpose::Entry => 1,
            OrderPurpose::Protection => 2,
            OrderPurpose::TakeProfit => 3,
            OrderPurpose::Reduce => 4,
            OrderPurpose::ExposureTakeProfit => 5,
        }
    }

    const fn native_family_tag(family: NativeOrderFamily) -> u8 {
        match family {
            NativeOrderFamily::UmOrder => 1,
            NativeOrderFamily::UmConditional => 2,
            NativeOrderFamily::UmAlgo => 3,
        }
    }

    fn valid_config_digest(value: &str) -> bool {
        !value.is_empty()
            && value.len() <= MAX_CONFIG_DIGEST_LEN
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    }

    fn commit_option_str(digest: &mut Sha256, value: Option<&str>) {
        match value {
            Some(value) => {
                commit_bool(digest, true);
                commit_str(digest, value);
            }
            None => commit_bool(digest, false),
        }
    }

    fn commit_option_u64(digest: &mut Sha256, value: Option<u64>) {
        match value {
            Some(value) => {
                commit_bool(digest, true);
                commit_u64(digest, value);
            }
            None => commit_bool(digest, false),
        }
    }

    fn commit_bool(digest: &mut Sha256, value: bool) {
        commit_bytes(digest, &[u8::from(value)]);
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
    pub enum BitgetRecoveryError {
        #[error("Bitget recovery endpoint does not exactly match Demo(TEST) or LIVE configuration")]
        Endpoint,
        #[error("Bitget recovery account binding is invalid")]
        Account,
        #[error("Bitget recovery configuration digest or epoch is invalid")]
        Configuration,
        #[error("Bitget recovery Owner, WAL, and Unknown roots must all be nonzero")]
        AuthorityRoots,
        #[error("Bitget recovery symbol universe is empty, excessive, or outside USDT futures")]
        SymbolUniverse,
        #[error("Bitget recovery connection/private generation is invalid or stale")]
        Generation,
        #[error("Bitget recovery deadline is invalid or exceeds the bounded collection window")]
        Deadline,
        #[error("Bitget recovery collection clock precedes its frozen start")]
        Clock,
        #[error("Bitget recovery collection expired before all six faces completed")]
        Expired,
        #[error(
            "Bitget recovery evidence is outside the frozen account, symbol, or generation scope"
        )]
        EvidenceScope,
        #[error("Bitget recovery repeats a symbol readback")]
        DuplicateSymbol,
        #[error("Bitget recovery omits at least one frozen symbol")]
        MissingSymbol,
        #[error("Bitget recovery account projections disagree across symbols")]
        AccountProjection,
        #[error("Bitget recovery lacks exact Owner coverage for visible regular orders")]
        OwnerRoute,
        #[error(
            "Bitget recovery unresolved projection lacks exact command/native/family/symbol identity"
        )]
        UnknownProjection,
        #[error("Bitget recovery record coverage overflowed")]
        Coverage,
        #[error("Bitget recovery raw evidence does not replay to the supplied projection")]
        Readback(#[from] BitgetNodeBridgeError),
    }

    #[cfg(test)]
    mod tests {
        use rust_decimal::Decimal;
        use serde_json::{Value, json};
        use venue_domain::domain::OrderPurpose;

        use super::*;
        use crate::{
            instrument::{BitgetRawInstrumentPayload, parse_instrument_rules},
            private::{
                BitgetPrivateFace, BitgetRawPrivatePage, complete_private_turn, parse_account_face,
                parse_fill_page, parse_positions_face, parse_regular_order_page,
                parse_settings_face,
            },
        };

        const ACCOUNT_ID: &str = "00000000-0000-4000-8000-000000000001";
        const STARTED_AT_MS: u64 = 100;
        const DEADLINE_MS: u64 = 900;
        const CONNECTION_GENERATION: u64 = 7;
        const PRIVATE_GENERATION: u64 = 9;

        fn symbol(value: &str) -> Result<Symbol, Box<dyn std::error::Error>> {
            Ok(value.parse()?)
        }

        fn binding(
            mode: GatewayMode,
            symbol: &Symbol,
        ) -> Result<GatewayBinding, Box<dyn std::error::Error>> {
            Ok(GatewayBinding::new(
                VenueId::Bitget,
                mode,
                ACCOUNT_ID,
                symbol.clone(),
            )?)
        }

        fn roots() -> Result<BitgetRecoveryAuthorityRoots, BitgetRecoveryError> {
            BitgetRecoveryAuthorityRoots::verified([1; 32], [2; 32], [3; 32])
        }

        fn scope(
            mode: GatewayMode,
            symbols: Vec<Symbol>,
        ) -> Result<BitgetFreshRecoveryScope, Box<dyn std::error::Error>> {
            let unresolved_symbol = symbols
                .first()
                .cloned()
                .ok_or(BitgetRecoveryError::SymbolUniverse)?;
            Ok(BitgetFreshRecoveryScope::verified(
                BitgetRecoveryEndpoint::for_mode(mode),
                BitgetAccountBinding::UtaUsdtFuturesHedge,
                ACCOUNT_ID,
                "bitget_config_1",
                3,
                roots()?,
                vec![BitgetRecoveryUnresolvedCommand::verified(
                    "unknown_1",
                    NativeOrderFamily::UmOrder,
                    unresolved_symbol,
                    "venue_unknown_1",
                    Some("native_unknown_1".to_owned()),
                )?],
                symbols,
                CONNECTION_GENERATION,
                PRIVATE_GENERATION - 1,
                PRIVATE_GENERATION,
                STARTED_AT_MS,
                DEADLINE_MS,
            )?)
        }

        fn generation() -> Result<BitgetRecoveryGenerationWitness, BitgetRecoveryError> {
            BitgetRecoveryGenerationWitness::connected(CONNECTION_GENERATION, PRIVATE_GENERATION)
        }

        fn instrument_payload(symbol: &Symbol) -> Value {
            json!({
                "code":"00000",
                "data":[{
                    "symbol":format!("{}USDT", symbol.base()),
                    "category":"USDT-FUTURES", "baseCoin":symbol.base(), "quoteCoin":"USDT",
                    "type":"perpetual", "status":"online", "symbolType":"crypto",
                    "pricePrecision":"1", "quantityPrecision":"4",
                    "priceMultiplier":"0.1", "quantityMultiplier":"0.0001",
                    "minOrderQty":"0.0001", "minOrderAmount":"5",
                    "maxOrderQty":"1200", "maxMarketOrderQty":"220"
                }]
            })
        }

        fn rules(
            mode: GatewayMode,
            symbol: &Symbol,
        ) -> Result<BitgetInstrumentRules, Box<dyn std::error::Error>> {
            Ok(parse_instrument_rules(
                BitgetRawInstrumentPayload::new(
                    binding(mode, symbol)?,
                    CONNECTION_GENERATION,
                    50,
                    1_000,
                    instrument_payload(symbol).to_string(),
                )?,
                60,
            )?)
        }

        fn raw(
            mode: GatewayMode,
            symbol: &Symbol,
            surface: BitgetPrivateSurface,
            data: Value,
        ) -> Result<BitgetRawPrivatePage, Box<dyn std::error::Error>> {
            Ok(BitgetRawPrivatePage::new_with_generation(
                surface,
                binding(mode, symbol)?,
                PRIVATE_GENERATION,
                CONNECTION_GENERATION,
                0,
                None,
                (surface == BitgetPrivateSurface::Fills).then_some(10),
                200,
                json!({"code":"00000", "data":data}).to_string(),
            )?)
        }

        fn private(
            mode: GatewayMode,
            symbol: &Symbol,
            regular_rows: Value,
        ) -> Result<BitgetPrivateGenerationCandidate, Box<dyn std::error::Error>> {
            Ok(complete_private_turn(vec![
                BitgetPrivateFace::Account(parse_account_face(raw(
                    mode,
                    symbol,
                    BitgetPrivateSurface::Account,
                    json!({
                        "imr":"0", "mmr":"0",
                        "assets":[{"coin":"USDT", "balance":"20", "available":"20"}]
                    }),
                )?)?),
                BitgetPrivateFace::Settings(parse_settings_face(raw(
                    mode,
                    symbol,
                    BitgetPrivateSurface::Settings,
                    json!({"holdMode":"hedge_mode"}),
                )?)?),
                BitgetPrivateFace::Positions(parse_positions_face(raw(
                    mode,
                    symbol,
                    BitgetPrivateSurface::Positions,
                    json!({"list":[]}),
                )?)?),
                BitgetPrivateFace::RegularOrders(vec![parse_regular_order_page(raw(
                    mode,
                    symbol,
                    BitgetPrivateSurface::RegularOrders,
                    json!({"list":regular_rows, "cursor":null}),
                )?)?]),
                BitgetPrivateFace::Fills(vec![parse_fill_page(raw(
                    mode,
                    symbol,
                    BitgetPrivateSurface::Fills,
                    json!({"list":[], "cursor":null}),
                )?)?]),
            ])?)
        }

        fn new_collector(
            mode: GatewayMode,
            symbols: Vec<Symbol>,
        ) -> Result<BitgetFreshRecoveryCollector, Box<dyn std::error::Error>> {
            let scope = scope(mode, symbols)?;
            let endpoint = scope.endpoint().clone();
            Ok(BitgetFreshRecoveryCollector::begin(
                scope,
                &endpoint,
                generation()?,
                STARTED_AT_MS,
            )?)
        }

        fn complete(
            collector: BitgetFreshRecoveryCollector,
            routes: Vec<BitgetRecoveryOwnerRoute>,
        ) -> Result<BitgetFreshRecoveryCandidate, Box<dyn std::error::Error>> {
            let endpoint = collector.scope.endpoint().clone();
            let outcome = collector.finish(&endpoint, generation()?, routes, 300)?;
            let BitgetRecoveryCollectionOutcome::Complete(candidate) = outcome else {
                return Err(Box::new(BitgetRecoveryError::Generation));
            };
            Ok(*candidate)
        }

        #[test]
        fn demo_and_live_bind_exact_endpoints_and_keep_generations_distinct()
        -> Result<(), Box<dyn std::error::Error>> {
            for mode in [GatewayMode::Test, GatewayMode::Live] {
                let btc = symbol("BTC/USDT")?;
                let mut collector = new_collector(mode, vec![btc.clone()])?;
                collector.push_symbol(&rules(mode, &btc)?, private(mode, &btc, json!([]))?, 250)?;
                let candidate = complete(collector, Vec::new())?;
                assert_eq!(candidate.scope().endpoint().mode(), mode);
                assert_eq!(
                    candidate.scope().endpoint().paper_trading(),
                    mode == GatewayMode::Test
                );
                assert_eq!(
                    candidate.scope().connection_generation(),
                    CONNECTION_GENERATION
                );
                assert_eq!(candidate.scope().private_generation(), PRIVATE_GENERATION);
                assert_ne!(
                    candidate.scope().connection_generation(),
                    candidate.scope().private_generation()
                );
                assert!(crate::capabilities().is_empty());
                assert!(
                    candidate
                        .raw_commitment_sha256()
                        .iter()
                        .any(|byte| *byte != 0)
                );
                for surface in [
                    BitgetRecoverySurface::Account,
                    BitgetRecoverySurface::Positions,
                    BitgetRecoverySurface::UmOrder,
                    BitgetRecoverySurface::FillsCursor,
                ] {
                    assert!(matches!(
                        candidate.coverage(surface),
                        BitgetRecoveryCoverage::Complete { .. }
                    ));
                }
                for surface in [
                    BitgetRecoverySurface::UmConditional,
                    BitgetRecoverySurface::UmAlgo,
                ] {
                    assert!(matches!(
                        candidate.coverage(surface),
                        BitgetRecoveryCoverage::Unsupported {
                            profile_version: BITGET_ORDER_PROFILE_VERSION,
                            ..
                        }
                    ));
                }
            }
            let test = BitgetRecoveryEndpoint::for_mode(GatewayMode::Test);
            assert_eq!(
                BitgetRecoveryEndpoint::verified(
                    GatewayMode::Live,
                    test.rest_origin(),
                    test.public_ws(),
                    test.private_ws(),
                    test.paper_trading(),
                ),
                Err(BitgetRecoveryError::Endpoint)
            );
            Ok(())
        }

        #[test]
        fn reconnect_and_private_advance_finish_unknown_without_relabel()
        -> Result<(), Box<dyn std::error::Error>> {
            let btc = symbol("BTC/USDT")?;
            for (witness, reason) in [
                (
                    BitgetRecoveryGenerationWitness::connected(
                        CONNECTION_GENERATION + 1,
                        PRIVATE_GENERATION,
                    )?,
                    BitgetRecoveryUnknownReason::ConnectionGenerationChanged,
                ),
                (
                    BitgetRecoveryGenerationWitness::connected(
                        CONNECTION_GENERATION,
                        PRIVATE_GENERATION + 1,
                    )?,
                    BitgetRecoveryUnknownReason::PrivateGenerationChanged,
                ),
                (
                    BitgetRecoveryGenerationWitness::disconnected(
                        CONNECTION_GENERATION,
                        PRIVATE_GENERATION,
                    )?,
                    BitgetRecoveryUnknownReason::Disconnected,
                ),
            ] {
                let mut collector = new_collector(GatewayMode::Live, vec![btc.clone()])?;
                collector.push_symbol(
                    &rules(GatewayMode::Live, &btc)?,
                    private(GatewayMode::Live, &btc, json!([]))?,
                    250,
                )?;
                let endpoint = collector.scope.endpoint().clone();
                let outcome = collector.finish(&endpoint, witness, Vec::new(), 300)?;
                let BitgetRecoveryCollectionOutcome::Unknown(unknown) = outcome else {
                    return Err(Box::new(BitgetRecoveryError::Generation));
                };
                assert_eq!(unknown.reason(), reason);
                assert_ne!(unknown.evidence_commitment_sha256(), &[0; 32]);
                let [unresolved] = unknown.unresolved_commands() else {
                    return Err(Box::new(BitgetRecoveryError::UnknownProjection));
                };
                assert_eq!(unresolved.command_id().as_str(), "unknown_1");
                assert_eq!(unresolved.family(), NativeOrderFamily::UmOrder);
                assert_eq!(unresolved.symbol(), &btc);
                assert_eq!(unresolved.client_order_id().as_str(), "venue_unknown_1");
                assert_eq!(unresolved.venue_order_id(), Some("native_unknown_1"));
            }
            Ok(())
        }

        #[test]
        fn production_collector_is_unavailable_and_grants_no_capability() {
            assert_eq!(
                super::super::BitgetFreshRecoveryCollector::begin(),
                Err(
                    super::super::BitgetFreshRecoveryCollectorError::ProductionUnavailable(
                        super::super::BitgetRecoveryProductionGap::RuntimeSealedUniverseBridge,
                    )
                )
            );
            assert_eq!(
                super::super::BitgetFreshRecoveryCollector::production_gaps(),
                &[
                    super::super::BitgetRecoveryProductionGap::RuntimeSealedUniverseBridge,
                    super::super::BitgetRecoveryProductionGap::DurableOwnerWalUnknownProjection,
                ]
            );
            assert!(crate::capabilities().is_empty());
        }

        #[test]
        fn missing_face_or_symbol_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
            let btc = symbol("BTC/USDT")?;
            let eth = symbol("ETH/USDT")?;
            let mut missing_face = private(GatewayMode::Test, &btc, json!([]))?;
            missing_face
                .raw_pages
                .retain(|raw| raw.surface != BitgetPrivateSurface::Fills);
            let mut collector = new_collector(GatewayMode::Test, vec![btc.clone()])?;
            assert!(matches!(
                collector.push_symbol(&rules(GatewayMode::Test, &btc)?, missing_face, 250),
                Err(BitgetRecoveryError::Readback(_))
            ));

            let mut collector = new_collector(GatewayMode::Test, vec![btc.clone(), eth])?;
            collector.push_symbol(
                &rules(GatewayMode::Test, &btc)?,
                private(GatewayMode::Test, &btc, json!([]))?,
                250,
            )?;
            let endpoint = collector.scope.endpoint().clone();
            assert_eq!(
                collector.finish(&endpoint, generation()?, Vec::new(), 300),
                Err(BitgetRecoveryError::MissingSymbol)
            );
            Ok(())
        }

        #[test]
        fn exact_owner_is_required_for_every_visible_normal_order()
        -> Result<(), Box<dyn std::error::Error>> {
            let btc = symbol("BTC/USDT")?;
            let order = json!({
                "orderId":"9001", "clientOid":"venue_open_1",
                "category":"USDT-FUTURES", "symbol":"BTCUSDT",
                "orderStatus":"live", "side":"buy", "posSide":"long",
                "holdMode":"hedge_mode", "tradeSide":"open_long",
                "qty":"0.001", "cumExecQty":"0", "price":"100000",
                "avgPrice":"0", "delegateType":"normal"
            });
            let mut collector = new_collector(GatewayMode::Test, vec![btc.clone()])?;
            collector.push_symbol(
                &rules(GatewayMode::Test, &btc)?,
                private(GatewayMode::Test, &btc, json!([order]))?,
                250,
            )?;
            let endpoint = collector.scope.endpoint().clone();
            assert_eq!(
                collector.finish(&endpoint, generation()?, Vec::new(), 300),
                Err(BitgetRecoveryError::OwnerRoute)
            );

            let mut collector = new_collector(GatewayMode::Test, vec![btc.clone()])?;
            collector.push_symbol(
                &rules(GatewayMode::Test, &btc)?,
                private(
                    GatewayMode::Test,
                    &btc,
                    json!([{
                        "orderId":"9001", "clientOid":"venue_open_1",
                        "category":"USDT-FUTURES", "symbol":"BTCUSDT",
                        "orderStatus":"live", "side":"buy", "posSide":"long",
                        "holdMode":"hedge_mode", "tradeSide":"open_long",
                        "qty":"0.001", "cumExecQty":"0", "price":"100000",
                        "avgPrice":"0", "delegateType":"normal"
                    }]),
                )?,
                250,
            )?;
            let route = BitgetRecoveryOwnerRoute::verified(
                "venue_open_1",
                "9001",
                OrderOwner {
                    strategy_instance_id: "grid_1".to_owned(),
                    run_id: "run_1".to_owned(),
                    exchange: "bitget".to_owned(),
                    account: ACCOUNT_ID.to_owned(),
                    symbol: btc,
                    purpose: OrderPurpose::Entry,
                },
            )?;
            let candidate = complete(collector, vec![route])?;
            assert_eq!(candidate.owner_routes().len(), 1);
            Ok(())
        }

        #[test]
        fn projection_tamper_and_expiry_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
            let btc = symbol("BTC/USDT")?;
            let mut tampered = private(GatewayMode::Live, &btc, json!([]))?;
            tampered.balance.available_balance -= Decimal::ONE;
            let mut collector = new_collector(GatewayMode::Live, vec![btc.clone()])?;
            assert!(matches!(
                collector.push_symbol(&rules(GatewayMode::Live, &btc)?, tampered, 250),
                Err(BitgetRecoveryError::Readback(_))
            ));

            let mut raw_tampered = private(GatewayMode::Live, &btc, json!([]))?;
            raw_tampered.raw_pages[0].payload.push(' ');
            let mut collector = new_collector(GatewayMode::Live, vec![btc.clone()])?;
            assert!(matches!(
                collector.push_symbol(&rules(GatewayMode::Live, &btc)?, raw_tampered, 250),
                Err(BitgetRecoveryError::Readback(_))
            ));

            let mut collector = new_collector(GatewayMode::Live, vec![btc.clone()])?;
            collector.push_symbol(
                &rules(GatewayMode::Live, &btc)?,
                private(GatewayMode::Live, &btc, json!([]))?,
                250,
            )?;
            let endpoint = collector.scope.endpoint().clone();
            assert_eq!(
                collector.finish(&endpoint, generation()?, Vec::new(), DEADLINE_MS),
                Err(BitgetRecoveryError::Expired)
            );
            Ok(())
        }
    }
}
