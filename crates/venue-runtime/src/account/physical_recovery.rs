#![allow(
    clippy::too_many_arguments,
    clippy::type_complexity,
    reason = "physical recovery binds every account authority and coverage dimension explicitly"
)]

use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};
use venue_domain::domain::{NativeOrderFamily, PositionSide, Symbol};
use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

use super::{
    AccountKey, AccountPositionMode, StrategyBinding, StrategyKind, model::validate_config_digest,
};

const REQUIRED_SURFACES: [PhysicalReadbackSurface; 6] = [
    PhysicalReadbackSurface::Account,
    PhysicalReadbackSurface::Positions,
    PhysicalReadbackSurface::UmOrder,
    PhysicalReadbackSurface::UmConditional,
    PhysicalReadbackSurface::UmAlgo,
    PhysicalReadbackSurface::FillsCursor,
];

/// Every physical readback face required before the recovered account kernel may consume a new
/// private generation. An empty result is still represented by its face and signed evidence root.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PhysicalReadbackSurface {
    Account,
    Positions,
    UmOrder,
    UmConditional,
    UmAlgo,
    FillsCursor,
}

impl PhysicalReadbackSurface {
    const fn is_order_family(self) -> bool {
        matches!(self, Self::UmOrder | Self::UmConditional | Self::UmAlgo)
    }

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

/// Explicit coverage for one readback face. `Complete` permits zero records so a genuinely empty
/// account remains distinguishable from an omitted request. Unsupported is restricted to native
/// order families and must carry the selected execution profile's durable evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalReadbackCoverage {
    Complete {
        evidence_sha256: [u8; 32],
        record_count: u64,
        covered_symbols: BTreeSet<Symbol>,
        covered_position_legs: BTreeSet<(Symbol, PositionSide)>,
    },
    Unsupported {
        evidence_sha256: [u8; 32],
        profile_version: u64,
        covered_symbols: BTreeSet<Symbol>,
    },
}

impl PhysicalReadbackCoverage {
    fn validate(&self, surface: PhysicalReadbackSurface) -> bool {
        match self {
            Self::Complete {
                evidence_sha256,
                covered_position_legs,
                ..
            } => {
                nonzero_digest(evidence_sha256)
                    && (surface == PhysicalReadbackSurface::Positions
                        || covered_position_legs.is_empty())
            }
            Self::Unsupported {
                evidence_sha256,
                profile_version,
                ..
            } => {
                surface.is_order_family() && *profile_version > 0 && nonzero_digest(evidence_sha256)
            }
        }
    }

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

/// Roots recovered before connectivity starts. They are opaque commitments: this module never
/// opens a journal, acquires a writer, or infers mutation authority from them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalRecoveryAuthorityRoots {
    owner: [u8; 32],
    wal: [u8; 32],
    unknown: [u8; 32],
    structured_unknowns: BTreeSet<PhysicalRecoveryUnknown>,
    structured_unknowns_bound: bool,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct PhysicalRecoveryUnknown {
    command_id: String,
    native_client_id: String,
    family: NativeOrderFamily,
    symbol: Symbol,
    reason: PhysicalRecoveryUnknownReason,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum PhysicalRecoveryUnknownReason {
    DurableWalUnresolved,
}

impl PhysicalRecoveryUnknown {
    pub(super) fn durable_wal_unresolved(
        command_id: impl Into<String>,
        native_client_id: impl Into<String>,
        family: NativeOrderFamily,
        symbol: Symbol,
    ) -> Self {
        Self {
            command_id: command_id.into(),
            native_client_id: native_client_id.into(),
            family,
            symbol,
            reason: PhysicalRecoveryUnknownReason::DurableWalUnresolved,
        }
    }
}

impl PhysicalRecoveryAuthorityRoots {
    pub fn verified(
        owner: [u8; 32],
        wal: [u8; 32],
        unknown: [u8; 32],
    ) -> Result<Self, PhysicalRecoveryManifestError> {
        if !nonzero_digest(&owner) || !nonzero_digest(&wal) || !nonzero_digest(&unknown) {
            return Err(PhysicalRecoveryManifestError::AuthorityRoot);
        }
        Ok(Self {
            owner,
            wal,
            unknown,
            structured_unknowns: BTreeSet::new(),
            structured_unknowns_bound: false,
        })
    }

    pub(super) fn verified_recovered(
        owner: [u8; 32],
        wal: [u8; 32],
        unknown: [u8; 32],
        structured_unknowns: BTreeSet<PhysicalRecoveryUnknown>,
    ) -> Result<Self, PhysicalRecoveryManifestError> {
        let mut roots = Self::verified(owner, wal, unknown)?;
        roots.structured_unknowns = structured_unknowns;
        roots.structured_unknowns_bound = true;
        Ok(roots)
    }

    pub(super) fn refreshed_owner(
        &self,
        owner: [u8; 32],
    ) -> Result<Self, PhysicalRecoveryManifestError> {
        if !nonzero_digest(&owner) {
            return Err(PhysicalRecoveryManifestError::AuthorityRoot);
        }
        let mut refreshed = self.clone();
        refreshed.owner = owner;
        Ok(refreshed)
    }

    pub(super) fn same_unknown_authority(&self, other: &Self) -> bool {
        self.unknown == other.unknown
            && self.structured_unknowns == other.structured_unknowns
            && self.structured_unknowns_bound == other.structured_unknowns_bound
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

    /// Number of unresolved durable commands included in this recovered root. This is evidence
    /// metadata only; it does not expose the commands or confer reconciliation authority.
    #[must_use]
    pub fn unresolved_count(&self) -> u64 {
        self.structured_unknowns.len() as u64
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalRecoveryUniverseEntry {
    binding: StrategyBinding,
    config_epoch: u64,
}

impl PhysicalRecoveryUniverseEntry {
    #[must_use]
    pub const fn symbol(&self) -> &Symbol {
        &self.binding.key.symbol
    }

    #[must_use]
    pub fn config_digest(&self) -> &str {
        &self.binding.config_digest
    }

    #[must_use]
    pub const fn config_epoch(&self) -> u64 {
        self.config_epoch
    }

    #[must_use]
    pub const fn binding(&self) -> &StrategyBinding {
        &self.binding
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PhysicalRecoveryAccountAuthority {
    account: AccountKey,
    mode: GatewayMode,
    registrations: Vec<PhysicalRecoveryUniverseEntry>,
    position_mode: AccountPositionMode,
    family_support: BTreeMap<NativeOrderFamily, bool>,
    profile_version: u64,
}

/// Immutable recovery anchor captured before the physical readback attempt. Every returned face
/// commits this entire scope, making binding, configuration, or journal drift invalidate the turn.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalRecoveryScope {
    binding: GatewayBinding,
    config_digest: String,
    config_epoch: u64,
    connection_generation: u64,
    recovered_private_generation: u64,
    authority_roots: PhysicalRecoveryAuthorityRoots,
    account_authority: Option<PhysicalRecoveryAccountAuthority>,
    recovery_session_sha256: Option<[u8; 32]>,
    commitment_sha256: [u8; 32],
}

impl PhysicalRecoveryScope {
    pub fn verified(
        binding: GatewayBinding,
        config_digest: impl Into<String>,
        config_epoch: u64,
        connection_generation: u64,
        recovered_private_generation: u64,
        authority_roots: PhysicalRecoveryAuthorityRoots,
    ) -> Result<Self, PhysicalRecoveryManifestError> {
        let config_digest = config_digest.into();
        binding
            .validate()
            .map_err(|_| PhysicalRecoveryManifestError::Binding)?;
        if config_epoch == 0 || validate_config_digest(&config_digest).is_err() {
            return Err(PhysicalRecoveryManifestError::Configuration);
        }
        if connection_generation == 0 {
            return Err(PhysicalRecoveryManifestError::ConnectionGeneration);
        }
        let commitment_sha256 = scope_commitment(
            &binding,
            &config_digest,
            config_epoch,
            connection_generation,
            recovered_private_generation,
            &authority_roots,
        );
        Ok(Self {
            binding,
            config_digest,
            config_epoch,
            connection_generation,
            recovered_private_generation,
            authority_roots,
            account_authority: None,
            recovery_session_sha256: None,
            commitment_sha256,
        })
    }

    pub(super) fn verified_account<I>(
        binding: GatewayBinding,
        account: AccountKey,
        registrations: I,
        position_mode: AccountPositionMode,
        family_support: BTreeMap<NativeOrderFamily, bool>,
        profile_version: u64,
        connection_generation: u64,
        recovered_private_generation: u64,
        authority_roots: PhysicalRecoveryAuthorityRoots,
    ) -> Result<Self, PhysicalRecoveryManifestError>
    where
        I: IntoIterator<Item = (StrategyBinding, u64)>,
    {
        Self::verified_account_with_session(
            binding,
            account,
            registrations,
            position_mode,
            family_support,
            profile_version,
            connection_generation,
            recovered_private_generation,
            authority_roots,
            None,
        )
    }

    pub(super) fn verified_account_session<I>(
        binding: GatewayBinding,
        account: AccountKey,
        registrations: I,
        position_mode: AccountPositionMode,
        family_support: BTreeMap<NativeOrderFamily, bool>,
        profile_version: u64,
        connection_generation: u64,
        recovered_private_generation: u64,
        authority_roots: PhysicalRecoveryAuthorityRoots,
        recovery_session_sha256: [u8; 32],
    ) -> Result<Self, PhysicalRecoveryManifestError>
    where
        I: IntoIterator<Item = (StrategyBinding, u64)>,
    {
        if !nonzero_digest(&recovery_session_sha256) {
            return Err(PhysicalRecoveryManifestError::RecoverySession);
        }
        Self::verified_account_with_session(
            binding,
            account,
            registrations,
            position_mode,
            family_support,
            profile_version,
            connection_generation,
            recovered_private_generation,
            authority_roots,
            Some(recovery_session_sha256),
        )
    }

    fn verified_account_with_session<I>(
        binding: GatewayBinding,
        account: AccountKey,
        registrations: I,
        position_mode: AccountPositionMode,
        family_support: BTreeMap<NativeOrderFamily, bool>,
        profile_version: u64,
        connection_generation: u64,
        recovered_private_generation: u64,
        authority_roots: PhysicalRecoveryAuthorityRoots,
        recovery_session_sha256: Option<[u8; 32]>,
    ) -> Result<Self, PhysicalRecoveryManifestError>
    where
        I: IntoIterator<Item = (StrategyBinding, u64)>,
    {
        binding
            .validate()
            .map_err(|_| PhysicalRecoveryManifestError::Binding)?;
        if binding.venue.as_str() != account.exchange.as_str()
            || binding.trading_account_id != account.account
            || profile_version == 0
            || connection_generation == 0
            || !authority_roots.structured_unknowns_bound
            || family_support.keys().copied().collect::<BTreeSet<_>>()
                != BTreeSet::from([
                    NativeOrderFamily::UmOrder,
                    NativeOrderFamily::UmConditional,
                    NativeOrderFamily::UmAlgo,
                ])
        {
            return Err(PhysicalRecoveryManifestError::AccountAuthority);
        }
        let mut registrations = registrations
            .into_iter()
            .map(|(registration, config_epoch)| {
                if registration.key.account != account
                    || config_epoch == 0
                    || validate_config_digest(&registration.config_digest).is_err()
                {
                    return Err(PhysicalRecoveryManifestError::Configuration);
                }
                Ok(PhysicalRecoveryUniverseEntry {
                    binding: registration,
                    config_epoch,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        registrations.sort_by(|left, right| left.symbol().cmp(right.symbol()));
        if registrations
            .windows(2)
            .any(|pair| pair[0].symbol() == pair[1].symbol())
            || registrations
                .first()
                .is_some_and(|registration| registration.symbol() != &binding.symbol)
        {
            return Err(PhysicalRecoveryManifestError::Registry);
        }
        let account_authority = PhysicalRecoveryAccountAuthority {
            account,
            mode: binding.mode,
            registrations,
            position_mode,
            family_support,
            profile_version,
        };
        let commitment_sha256 = account_scope_commitment(
            &binding,
            connection_generation,
            recovered_private_generation,
            &authority_roots,
            &account_authority,
            recovery_session_sha256.as_ref(),
        );
        Ok(Self {
            binding,
            config_digest: "account_registry".to_owned(),
            config_epoch: 1,
            connection_generation,
            recovered_private_generation,
            authority_roots,
            account_authority: Some(account_authority),
            recovery_session_sha256,
            commitment_sha256,
        })
    }

    pub(super) fn matches_account_authority<I>(
        &self,
        account: &AccountKey,
        mode: GatewayMode,
        registrations: I,
        position_mode: AccountPositionMode,
        family_support: &BTreeMap<NativeOrderFamily, bool>,
        profile_version: u64,
    ) -> bool
    where
        I: IntoIterator<Item = (StrategyBinding, u64)>,
    {
        let Some(expected) = &self.account_authority else {
            return false;
        };
        let mut registrations = registrations
            .into_iter()
            .map(|(binding, config_epoch)| PhysicalRecoveryUniverseEntry {
                binding,
                config_epoch,
            })
            .collect::<Vec<_>>();
        registrations.sort_by(|left, right| left.symbol().cmp(right.symbol()));
        expected.account == *account
            && expected.mode == mode
            && expected.registrations == registrations
            && expected.position_mode == position_mode
            && expected.family_support == *family_support
            && expected.profile_version == profile_version
    }

    fn expected_targets(
        &self,
        surface: PhysicalReadbackSurface,
    ) -> Result<(BTreeSet<Symbol>, BTreeSet<(Symbol, PositionSide)>), PhysicalRecoveryManifestError>
    {
        let authority = self
            .account_authority
            .as_ref()
            .ok_or(PhysicalRecoveryManifestError::AccountAuthority)?;
        let symbols = authority
            .registrations
            .iter()
            .map(|registration| registration.symbol().clone())
            .collect::<BTreeSet<_>>();
        let covered_symbols = if surface == PhysicalReadbackSurface::Account {
            BTreeSet::new()
        } else {
            symbols.clone()
        };
        let covered_position_legs = if surface == PhysicalReadbackSurface::Positions {
            let sides = match authority.position_mode {
                AccountPositionMode::Net => [Some(PositionSide::Net), None],
                AccountPositionMode::Hedge => [Some(PositionSide::Long), Some(PositionSide::Short)],
            };
            symbols
                .into_iter()
                .flat_map(|symbol| {
                    sides
                        .iter()
                        .flatten()
                        .cloned()
                        .map(move |side| (symbol.clone(), side))
                })
                .collect()
        } else {
            BTreeSet::new()
        };
        Ok((covered_symbols, covered_position_legs))
    }

    fn validate_surface_coverage(
        &self,
        surfaces: &BTreeMap<PhysicalReadbackSurface, PhysicalReadbackCoverage>,
    ) -> Result<(), PhysicalRecoveryManifestError> {
        let Some(authority) = &self.account_authority else {
            return Ok(());
        };
        for (surface, coverage) in surfaces {
            let (expected_symbols, expected_legs) = self.expected_targets(*surface)?;
            let expected_supported = surface_family(*surface)
                .and_then(|family| authority.family_support.get(&family).copied());
            let valid = match coverage {
                PhysicalReadbackCoverage::Complete {
                    covered_symbols,
                    covered_position_legs,
                    ..
                } => {
                    expected_supported != Some(false)
                        && *covered_symbols == expected_symbols
                        && *covered_position_legs == expected_legs
                }
                PhysicalReadbackCoverage::Unsupported {
                    profile_version,
                    covered_symbols,
                    ..
                } => {
                    expected_supported == Some(false)
                        && *profile_version == authority.profile_version
                        && *covered_symbols == expected_symbols
                        && expected_legs.is_empty()
                }
            };
            if !valid {
                return Err(PhysicalRecoveryManifestError::SymbolCoverage);
            }
        }
        Ok(())
    }

    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
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
    pub const fn authority_roots(&self) -> &PhysicalRecoveryAuthorityRoots {
        &self.authority_roots
    }

    #[must_use]
    pub fn account_universe(&self) -> &[PhysicalRecoveryUniverseEntry] {
        self.account_authority
            .as_ref()
            .map_or(&[], |authority| authority.registrations.as_slice())
    }

    #[must_use]
    pub fn position_mode(&self) -> Option<AccountPositionMode> {
        self.account_authority
            .as_ref()
            .map(|authority| authority.position_mode)
    }

    #[must_use]
    pub fn profile_version(&self) -> Option<u64> {
        self.account_authority
            .as_ref()
            .map(|authority| authority.profile_version)
    }

    #[must_use]
    pub const fn commitment_sha256(&self) -> &[u8; 32] {
        &self.commitment_sha256
    }
}

/// Adapter-issued opaque receipt for one signed face. Keeping the recovery-scope commitment on
/// every face prevents a caller from joining responses collected across configuration/root drift.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalReadbackReceipt {
    surface: PhysicalReadbackSurface,
    attempt_id: u64,
    private_generation: u64,
    recovery_scope_sha256: [u8; 32],
    coverage: PhysicalReadbackCoverage,
}

impl PhysicalReadbackReceipt {
    pub fn verified_complete(
        scope: &PhysicalRecoveryScope,
        surface: PhysicalReadbackSurface,
        attempt_id: u64,
        private_generation: u64,
        evidence_sha256: [u8; 32],
        record_count: u64,
    ) -> Result<Self, PhysicalRecoveryManifestError> {
        Self::verified(
            scope,
            surface,
            attempt_id,
            private_generation,
            PhysicalReadbackCoverage::Complete {
                evidence_sha256,
                record_count,
                covered_symbols: BTreeSet::new(),
                covered_position_legs: BTreeSet::new(),
            },
        )
    }

    pub fn verified_unsupported_order_family(
        scope: &PhysicalRecoveryScope,
        surface: PhysicalReadbackSurface,
        attempt_id: u64,
        private_generation: u64,
        evidence_sha256: [u8; 32],
        profile_version: u64,
    ) -> Result<Self, PhysicalRecoveryManifestError> {
        Self::verified(
            scope,
            surface,
            attempt_id,
            private_generation,
            PhysicalReadbackCoverage::Unsupported {
                evidence_sha256,
                profile_version,
                covered_symbols: BTreeSet::new(),
            },
        )
    }

    pub(super) fn verified_complete_account(
        scope: &PhysicalRecoveryScope,
        surface: PhysicalReadbackSurface,
        attempt_id: u64,
        private_generation: u64,
        evidence_sha256: [u8; 32],
        record_count: u64,
    ) -> Result<Self, PhysicalRecoveryManifestError> {
        let (covered_symbols, covered_position_legs) = scope.expected_targets(surface)?;
        Self::verified(
            scope,
            surface,
            attempt_id,
            private_generation,
            PhysicalReadbackCoverage::Complete {
                evidence_sha256,
                record_count,
                covered_symbols,
                covered_position_legs,
            },
        )
    }

    pub(super) fn verified_unsupported_order_family_account(
        scope: &PhysicalRecoveryScope,
        surface: PhysicalReadbackSurface,
        attempt_id: u64,
        private_generation: u64,
        evidence_sha256: [u8; 32],
    ) -> Result<Self, PhysicalRecoveryManifestError> {
        let (covered_symbols, covered_position_legs) = scope.expected_targets(surface)?;
        if !covered_position_legs.is_empty() {
            return Err(PhysicalRecoveryManifestError::Coverage);
        }
        let profile_version = scope
            .account_authority
            .as_ref()
            .ok_or(PhysicalRecoveryManifestError::AccountAuthority)?
            .profile_version;
        Self::verified(
            scope,
            surface,
            attempt_id,
            private_generation,
            PhysicalReadbackCoverage::Unsupported {
                evidence_sha256,
                profile_version,
                covered_symbols,
            },
        )
    }

    #[cfg(test)]
    pub(super) fn verified_complete_account_targets(
        scope: &PhysicalRecoveryScope,
        surface: PhysicalReadbackSurface,
        attempt_id: u64,
        private_generation: u64,
        evidence_sha256: [u8; 32],
        covered_symbols: BTreeSet<Symbol>,
        covered_position_legs: BTreeSet<(Symbol, PositionSide)>,
    ) -> Result<Self, PhysicalRecoveryManifestError> {
        Self::verified(
            scope,
            surface,
            attempt_id,
            private_generation,
            PhysicalReadbackCoverage::Complete {
                evidence_sha256,
                record_count: 0,
                covered_symbols,
                covered_position_legs,
            },
        )
    }

    fn verified(
        scope: &PhysicalRecoveryScope,
        surface: PhysicalReadbackSurface,
        attempt_id: u64,
        private_generation: u64,
        coverage: PhysicalReadbackCoverage,
    ) -> Result<Self, PhysicalRecoveryManifestError> {
        if attempt_id == 0 {
            return Err(PhysicalRecoveryManifestError::Attempt);
        }
        if private_generation == 0 {
            return Err(PhysicalRecoveryManifestError::Generation);
        }
        if !coverage.validate(surface) {
            return Err(PhysicalRecoveryManifestError::Coverage);
        }
        Ok(Self {
            surface,
            attempt_id,
            private_generation,
            recovery_scope_sha256: scope.commitment_sha256,
            coverage,
        })
    }

    #[must_use]
    pub const fn surface(&self) -> PhysicalReadbackSurface {
        self.surface
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
    pub const fn coverage(&self) -> &PhysicalReadbackCoverage {
        &self.coverage
    }
}

/// Complete post-recovery signed readback turn. Construction is all-or-nothing and does not grant
/// a network handle, journal handle, writer lease, or physical mutation capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalRecoveryReadbackManifest {
    scope: PhysicalRecoveryScope,
    attempt_id: u64,
    private_generation: u64,
    surfaces: BTreeMap<PhysicalReadbackSurface, PhysicalReadbackCoverage>,
    commitment_sha256: [u8; 32],
}

impl PhysicalRecoveryReadbackManifest {
    pub fn verified<I>(
        scope: PhysicalRecoveryScope,
        receipts: I,
    ) -> Result<Self, PhysicalRecoveryManifestError>
    where
        I: IntoIterator<Item = PhysicalReadbackReceipt>,
    {
        let mut attempt_id = None;
        let mut private_generation = None;
        let mut surfaces = BTreeMap::new();
        for receipt in receipts {
            if receipt.recovery_scope_sha256 != scope.commitment_sha256 {
                return Err(PhysicalRecoveryManifestError::ScopeDrift);
            }
            match attempt_id {
                Some(expected) if expected != receipt.attempt_id => {
                    return Err(PhysicalRecoveryManifestError::AttemptDrift);
                }
                None => attempt_id = Some(receipt.attempt_id),
                _ => {}
            }
            match private_generation {
                Some(expected) if expected != receipt.private_generation => {
                    return Err(PhysicalRecoveryManifestError::GenerationDrift);
                }
                None => private_generation = Some(receipt.private_generation),
                _ => {}
            }
            if surfaces.insert(receipt.surface, receipt.coverage).is_some() {
                return Err(PhysicalRecoveryManifestError::DuplicateSurface);
            }
        }

        if surfaces.keys().copied().collect::<BTreeSet<_>>() != BTreeSet::from(REQUIRED_SURFACES) {
            return Err(PhysicalRecoveryManifestError::MissingSurface);
        }
        let attempt_id = attempt_id.ok_or(PhysicalRecoveryManifestError::MissingSurface)?;
        let private_generation =
            private_generation.ok_or(PhysicalRecoveryManifestError::MissingSurface)?;
        if private_generation <= scope.recovered_private_generation {
            return Err(PhysicalRecoveryManifestError::StaleGeneration);
        }
        scope.validate_surface_coverage(&surfaces)?;
        let commitment_sha256 = manifest_commitment(
            &scope.commitment_sha256,
            attempt_id,
            private_generation,
            &surfaces,
        );
        Ok(Self {
            scope,
            attempt_id,
            private_generation,
            surfaces,
            commitment_sha256,
        })
    }

    #[must_use]
    pub const fn scope(&self) -> &PhysicalRecoveryScope {
        &self.scope
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
    pub fn coverage(&self, surface: PhysicalReadbackSurface) -> &PhysicalReadbackCoverage {
        &self.surfaces[&surface]
    }

    #[must_use]
    pub const fn commitment_sha256(&self) -> &[u8; 32] {
        &self.commitment_sha256
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PhysicalRecoveryManifestError {
    #[error("physical recovery binding is invalid")]
    Binding,
    #[error("physical recovery configuration digest or epoch is invalid")]
    Configuration,
    #[error("Owner, WAL, and Unknown recovery roots must all be nonzero digests")]
    AuthorityRoot,
    #[error("physical recovery connection generation must be positive")]
    ConnectionGeneration,
    #[error("physical readback attempt must be positive")]
    Attempt,
    #[error("physical readback private generation must be positive")]
    Generation,
    #[error("physical readback coverage or evidence digest is invalid")]
    Coverage,
    #[error("physical readback scope changed during collection")]
    ScopeDrift,
    #[error("physical readback faces came from different attempts")]
    AttemptDrift,
    #[error("physical readback faces came from different private generations")]
    GenerationDrift,
    #[error("physical readback contains a duplicate face")]
    DuplicateSurface,
    #[error("physical readback omitted a required face")]
    MissingSurface,
    #[error("physical readback private generation is not newer than recovered state")]
    StaleGeneration,
    #[error("physical recovery account authority is incomplete")]
    AccountAuthority,
    #[error("physical recovery registry authority is incomplete or duplicated")]
    Registry,
    #[error("physical recovery omitted a registered symbol, position leg, or order-family face")]
    SymbolCoverage,
    #[error("physical recovery scope is missing its runtime-issued recovery session")]
    RecoverySession,
}

fn scope_commitment(
    binding: &GatewayBinding,
    config_digest: &str,
    config_epoch: u64,
    connection_generation: u64,
    recovered_private_generation: u64,
    roots: &PhysicalRecoveryAuthorityRoots,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    commit_bytes(&mut digest, b"venue-physical-recovery-scope-v1");
    commit_bytes(&mut digest, &[venue_tag(binding.venue)]);
    commit_bytes(&mut digest, &[mode_tag(binding.mode)]);
    commit_str(&mut digest, &binding.trading_account_id);
    commit_str(&mut digest, &binding.symbol.to_string());
    commit_str(&mut digest, config_digest);
    commit_u64(&mut digest, config_epoch);
    commit_u64(&mut digest, connection_generation);
    commit_u64(&mut digest, recovered_private_generation);
    commit_bytes(&mut digest, &roots.owner);
    commit_bytes(&mut digest, &roots.wal);
    commit_bytes(&mut digest, &roots.unknown);
    digest.finalize().into()
}

fn account_scope_commitment(
    binding: &GatewayBinding,
    connection_generation: u64,
    recovered_private_generation: u64,
    roots: &PhysicalRecoveryAuthorityRoots,
    authority: &PhysicalRecoveryAccountAuthority,
    recovery_session_sha256: Option<&[u8; 32]>,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    commit_bytes(&mut digest, b"venue-physical-recovery-account-scope-v4");
    commit_bytes(
        &mut digest,
        &[venue_tag(binding.venue), mode_tag(authority.mode)],
    );
    commit_str(&mut digest, &binding.trading_account_id);
    commit_str(&mut digest, &binding.symbol.to_string());
    commit_str(&mut digest, &authority.account.account);
    commit_bytes(&mut digest, &[position_mode_tag(authority.position_mode)]);
    commit_u64(&mut digest, connection_generation);
    commit_u64(&mut digest, recovered_private_generation);
    commit_u64(&mut digest, authority.registrations.len() as u64);
    for registration in &authority.registrations {
        commit_bytes(
            &mut digest,
            &[strategy_kind_tag(registration.binding.key.strategy_kind)],
        );
        commit_str(&mut digest, &registration.binding.key.instance_id);
        commit_str(&mut digest, &registration.binding.key.symbol.to_string());
        commit_str(&mut digest, &registration.binding.run_id);
        commit_str(&mut digest, &registration.binding.config_digest);
        commit_u64(&mut digest, registration.config_epoch);
    }
    for family in [
        NativeOrderFamily::UmOrder,
        NativeOrderFamily::UmConditional,
        NativeOrderFamily::UmAlgo,
    ] {
        commit_bytes(&mut digest, &[family_tag(family)]);
        commit_bytes(&mut digest, &[u8::from(authority.family_support[&family])]);
    }
    commit_u64(&mut digest, authority.profile_version);
    commit_bytes(&mut digest, &roots.owner);
    commit_bytes(&mut digest, &roots.wal);
    commit_bytes(&mut digest, &roots.unknown);
    commit_u64(&mut digest, roots.structured_unknowns.len() as u64);
    for unknown in &roots.structured_unknowns {
        commit_str(&mut digest, &unknown.command_id);
        commit_str(&mut digest, &unknown.native_client_id);
        commit_bytes(&mut digest, &[family_tag(unknown.family)]);
        commit_str(&mut digest, &unknown.symbol.to_string());
        commit_bytes(&mut digest, &[unknown_reason_tag(unknown.reason)]);
    }
    match recovery_session_sha256 {
        Some(session) => {
            commit_bytes(&mut digest, &[1]);
            commit_bytes(&mut digest, session);
        }
        None => commit_bytes(&mut digest, &[0]),
    }
    digest.finalize().into()
}

fn manifest_commitment(
    scope_sha256: &[u8; 32],
    attempt_id: u64,
    private_generation: u64,
    surfaces: &BTreeMap<PhysicalReadbackSurface, PhysicalReadbackCoverage>,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    commit_bytes(&mut digest, b"venue-physical-recovery-readback-manifest-v1");
    commit_bytes(&mut digest, scope_sha256);
    commit_u64(&mut digest, attempt_id);
    commit_u64(&mut digest, private_generation);
    commit_u64(&mut digest, surfaces.len() as u64);
    for (surface, coverage) in surfaces {
        commit_bytes(&mut digest, &[surface.tag()]);
        match coverage {
            PhysicalReadbackCoverage::Complete {
                evidence_sha256,
                record_count,
                covered_symbols,
                covered_position_legs,
            } => {
                commit_bytes(&mut digest, &[1]);
                commit_bytes(&mut digest, evidence_sha256);
                commit_u64(&mut digest, *record_count);
                commit_symbols(&mut digest, covered_symbols);
                commit_u64(&mut digest, covered_position_legs.len() as u64);
                for (symbol, side) in covered_position_legs {
                    commit_str(&mut digest, &symbol.to_string());
                    commit_bytes(&mut digest, &[position_side_tag(*side)]);
                }
            }
            PhysicalReadbackCoverage::Unsupported {
                evidence_sha256,
                profile_version,
                covered_symbols,
            } => {
                commit_bytes(&mut digest, &[2]);
                commit_bytes(&mut digest, evidence_sha256);
                commit_u64(&mut digest, *profile_version);
                commit_symbols(&mut digest, covered_symbols);
            }
        }
    }
    digest.finalize().into()
}

fn commit_symbols(digest: &mut Sha256, symbols: &BTreeSet<Symbol>) {
    commit_u64(digest, symbols.len() as u64);
    for symbol in symbols {
        commit_str(digest, &symbol.to_string());
    }
}

const fn surface_family(surface: PhysicalReadbackSurface) -> Option<NativeOrderFamily> {
    match surface {
        PhysicalReadbackSurface::UmOrder => Some(NativeOrderFamily::UmOrder),
        PhysicalReadbackSurface::UmConditional => Some(NativeOrderFamily::UmConditional),
        PhysicalReadbackSurface::UmAlgo => Some(NativeOrderFamily::UmAlgo),
        _ => None,
    }
}

const fn family_tag(family: NativeOrderFamily) -> u8 {
    match family {
        NativeOrderFamily::UmOrder => 1,
        NativeOrderFamily::UmConditional => 2,
        NativeOrderFamily::UmAlgo => 3,
    }
}

const fn position_mode_tag(mode: AccountPositionMode) -> u8 {
    match mode {
        AccountPositionMode::Net => 1,
        AccountPositionMode::Hedge => 2,
    }
}

const fn position_side_tag(side: PositionSide) -> u8 {
    match side {
        PositionSide::Net => 1,
        PositionSide::Long => 2,
        PositionSide::Short => 3,
    }
}

const fn unknown_reason_tag(reason: PhysicalRecoveryUnknownReason) -> u8 {
    match reason {
        PhysicalRecoveryUnknownReason::DurableWalUnresolved => 1,
    }
}

const fn strategy_kind_tag(kind: StrategyKind) -> u8 {
    match kind {
        StrategyKind::HedgedGrid => 1,
        StrategyKind::Scalping => 2,
        StrategyKind::Copy => 3,
    }
}

const fn venue_tag(venue: VenueId) -> u8 {
    match venue {
        VenueId::Binance => 1,
        VenueId::Bitget => 2,
        VenueId::Bybit => 3,
        VenueId::Gate => 4,
        VenueId::Hyperliquid => 5,
        VenueId::Okx => 6,
    }
}

const fn mode_tag(mode: GatewayMode) -> u8 {
    match mode {
        GatewayMode::Live => 2,
    }
}

#[cfg(test)]
mod live_mode_compatibility_tests {
    use super::*;

    #[test]
    fn live_physical_recovery_scope_tag_remains_two() {
        assert_eq!(mode_tag(GatewayMode::Live), 2);
    }
}

fn nonzero_digest(value: &[u8; 32]) -> bool {
    value.iter().any(|byte| *byte != 0)
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

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;
    use crate::StrategyInstanceKey;

    const ACCOUNT_ID: &str = "00000000-0000-4000-8000-000000000001";

    fn hash(seed: u8) -> [u8; 32] {
        [seed; 32]
    }

    fn scope_with(
        config_epoch: u64,
        recovered_private_generation: u64,
        roots: PhysicalRecoveryAuthorityRoots,
    ) -> Result<PhysicalRecoveryScope, Box<dyn Error>> {
        Ok(PhysicalRecoveryScope::verified(
            GatewayBinding::new(
                VenueId::Binance,
                GatewayMode::Live,
                ACCOUNT_ID,
                "BTC/USDT".parse()?,
            )?,
            "config_1",
            config_epoch,
            1,
            recovered_private_generation,
            roots,
        )?)
    }

    fn scope() -> Result<PhysicalRecoveryScope, Box<dyn Error>> {
        scope_with(
            7,
            10,
            PhysicalRecoveryAuthorityRoots::verified(hash(1), hash(2), hash(3))?,
        )
    }

    fn account_scope() -> Result<PhysicalRecoveryScope, Box<dyn Error>> {
        account_scope_with_anchor("BTC/USDT")
    }

    fn account_scope_with_anchor(anchor: &str) -> Result<PhysicalRecoveryScope, Box<dyn Error>> {
        account_scope_with_anchor_and_roots(
            anchor,
            PhysicalRecoveryAuthorityRoots::verified_recovered(
                hash(1),
                hash(2),
                hash(3),
                BTreeSet::new(),
            )?,
        )
    }

    fn account_scope_with_anchor_and_roots(
        anchor: &str,
        authority_roots: PhysicalRecoveryAuthorityRoots,
    ) -> Result<PhysicalRecoveryScope, Box<dyn Error>> {
        let account = AccountKey::new(crate::domain::ExchangeId::Binance, ACCOUNT_ID)?;
        let registrations = [
            (
                StrategyBinding::new(
                    StrategyInstanceKey::new(
                        account.clone(),
                        StrategyKind::HedgedGrid,
                        "grid_btc",
                        "BTC/USDT".parse()?,
                    )?,
                    "run_btc",
                    "btc_config",
                )?,
                7,
            ),
            (
                StrategyBinding::new(
                    StrategyInstanceKey::new(
                        account.clone(),
                        StrategyKind::Scalping,
                        "scalp_eth",
                        "ETH/USDT".parse()?,
                    )?,
                    "run_eth",
                    "eth_config",
                )?,
                9,
            ),
        ];
        Ok(PhysicalRecoveryScope::verified_account(
            GatewayBinding::new(
                VenueId::Binance,
                GatewayMode::Live,
                ACCOUNT_ID,
                anchor.parse()?,
            )?,
            account,
            registrations,
            AccountPositionMode::Hedge,
            all_family_support(),
            1,
            4,
            10,
            authority_roots,
        )?)
    }

    fn all_family_support() -> BTreeMap<NativeOrderFamily, bool> {
        BTreeMap::from([
            (NativeOrderFamily::UmOrder, true),
            (NativeOrderFamily::UmConditional, true),
            (NativeOrderFamily::UmAlgo, true),
        ])
    }

    fn account_receipts(
        scope: &PhysicalRecoveryScope,
        generation: u64,
    ) -> Result<Vec<PhysicalReadbackReceipt>, PhysicalRecoveryManifestError> {
        REQUIRED_SURFACES
            .iter()
            .enumerate()
            .map(|(index, surface)| {
                PhysicalReadbackReceipt::verified_complete_account(
                    scope,
                    *surface,
                    51,
                    generation,
                    hash(u8::try_from(index).unwrap_or(u8::MAX).saturating_add(41)),
                    0,
                )
            })
            .collect()
    }

    fn receipts(
        scope: &PhysicalRecoveryScope,
        attempt_id: u64,
        generation: u64,
    ) -> Result<Vec<PhysicalReadbackReceipt>, PhysicalRecoveryManifestError> {
        REQUIRED_SURFACES
            .iter()
            .enumerate()
            .map(|(index, surface)| {
                PhysicalReadbackReceipt::verified_complete(
                    scope,
                    *surface,
                    attempt_id,
                    generation,
                    hash(u8::try_from(index).unwrap_or(u8::MAX).saturating_add(11)),
                    0,
                )
            })
            .collect()
    }

    #[test]
    fn explicit_empty_account_builds_complete_new_generation_manifest() -> Result<(), Box<dyn Error>>
    {
        let scope = scope()?;
        let receipts = receipts(&scope, 41, 11)?;
        let manifest = PhysicalRecoveryReadbackManifest::verified(scope, receipts)?;

        assert_eq!(manifest.attempt_id(), 41);
        assert_eq!(manifest.private_generation(), 11);
        assert_eq!(manifest.scope().config_epoch(), 7);
        assert_eq!(manifest.scope().connection_generation(), 1);
        assert!(nonzero_digest(manifest.commitment_sha256()));
        for surface in REQUIRED_SURFACES {
            assert!(matches!(
                manifest.coverage(surface),
                PhysicalReadbackCoverage::Complete {
                    record_count: 0,
                    ..
                }
            ));
        }
        Ok(())
    }

    #[test]
    fn every_face_is_mandatory_even_when_all_results_are_empty() -> Result<(), Box<dyn Error>> {
        for missing in REQUIRED_SURFACES {
            let scope = scope()?;
            let receipts = receipts(&scope, 41, 11)?
                .into_iter()
                .filter(|receipt| receipt.surface() != missing);
            assert_eq!(
                PhysicalRecoveryReadbackManifest::verified(scope, receipts),
                Err(PhysicalRecoveryManifestError::MissingSurface)
            );
        }
        Ok(())
    }

    #[test]
    fn cross_attempt_and_generation_stitching_fail_closed() -> Result<(), Box<dyn Error>> {
        let scope = scope()?;
        let mut cross_attempt = receipts(&scope, 41, 11)?;
        cross_attempt[2] = PhysicalReadbackReceipt::verified_complete(
            &scope,
            PhysicalReadbackSurface::UmOrder,
            42,
            11,
            hash(20),
            0,
        )?;
        assert_eq!(
            PhysicalRecoveryReadbackManifest::verified(scope.clone(), cross_attempt),
            Err(PhysicalRecoveryManifestError::AttemptDrift)
        );

        let mut cross_generation = receipts(&scope, 41, 11)?;
        cross_generation[5] = PhysicalReadbackReceipt::verified_complete(
            &scope,
            PhysicalReadbackSurface::FillsCursor,
            41,
            12,
            hash(21),
            0,
        )?;
        assert_eq!(
            PhysicalRecoveryReadbackManifest::verified(scope, cross_generation),
            Err(PhysicalRecoveryManifestError::GenerationDrift)
        );
        Ok(())
    }

    #[test]
    fn binding_configuration_and_authority_root_drift_fail_closed() -> Result<(), Box<dyn Error>> {
        let expected = scope()?;
        let binding_drift = PhysicalRecoveryScope::verified(
            GatewayBinding::new(
                VenueId::Binance,
                GatewayMode::Live,
                "00000000-0000-4000-8000-000000000002",
                "BTC/USDT".parse()?,
            )?,
            "config_1",
            7,
            1,
            10,
            PhysicalRecoveryAuthorityRoots::verified(hash(1), hash(2), hash(3))?,
        )?;
        let config_drift = scope_with(
            8,
            10,
            PhysicalRecoveryAuthorityRoots::verified(hash(1), hash(2), hash(3))?,
        )?;
        let root_drift = scope_with(
            7,
            10,
            PhysicalRecoveryAuthorityRoots::verified(hash(9), hash(2), hash(3))?,
        )?;

        for drifted in [binding_drift, config_drift, root_drift] {
            let mut mixed = receipts(&expected, 41, 11)?;
            mixed[0] = PhysicalReadbackReceipt::verified_complete(
                &drifted,
                PhysicalReadbackSurface::Account,
                41,
                11,
                hash(30),
                0,
            )?;
            assert_eq!(
                PhysicalRecoveryReadbackManifest::verified(expected.clone(), mixed),
                Err(PhysicalRecoveryManifestError::ScopeDrift)
            );
        }
        Ok(())
    }

    #[test]
    fn stale_generation_duplicate_and_invalid_coverage_fail_closed() -> Result<(), Box<dyn Error>> {
        let scope = scope()?;
        assert_eq!(
            PhysicalRecoveryReadbackManifest::verified(scope.clone(), receipts(&scope, 41, 10)?),
            Err(PhysicalRecoveryManifestError::StaleGeneration)
        );

        let mut duplicate = receipts(&scope, 41, 11)?;
        duplicate.push(PhysicalReadbackReceipt::verified_complete(
            &scope,
            PhysicalReadbackSurface::Account,
            41,
            11,
            hash(31),
            0,
        )?);
        assert_eq!(
            PhysicalRecoveryReadbackManifest::verified(scope.clone(), duplicate),
            Err(PhysicalRecoveryManifestError::DuplicateSurface)
        );
        assert_eq!(
            PhysicalReadbackReceipt::verified_complete(
                &scope,
                PhysicalReadbackSurface::Account,
                41,
                11,
                [0; 32],
                0,
            ),
            Err(PhysicalRecoveryManifestError::Coverage)
        );
        assert_eq!(
            PhysicalReadbackReceipt::verified_unsupported_order_family(
                &scope,
                PhysicalReadbackSurface::Positions,
                41,
                11,
                hash(32),
                1,
            ),
            Err(PhysicalRecoveryManifestError::Coverage)
        );
        assert_eq!(
            PhysicalRecoveryAuthorityRoots::verified([0; 32], hash(2), hash(3)),
            Err(PhysicalRecoveryManifestError::AuthorityRoot)
        );
        Ok(())
    }

    #[test]
    fn multi_symbol_empty_faces_cover_every_symbol_and_hedge_leg() -> Result<(), Box<dyn Error>> {
        let scope = account_scope()?;
        let manifest = PhysicalRecoveryReadbackManifest::verified(
            scope.clone(),
            account_receipts(&scope, 11)?,
        )?;
        assert!(matches!(
            manifest.coverage(PhysicalReadbackSurface::Positions),
            PhysicalReadbackCoverage::Complete {
                record_count: 0,
                covered_symbols,
                covered_position_legs,
                ..
            } if covered_symbols.len() == 2 && covered_position_legs.len() == 4
        ));
        assert!(matches!(
            manifest.coverage(PhysicalReadbackSurface::UmOrder),
            PhysicalReadbackCoverage::Complete {
                record_count: 0,
                covered_symbols,
                ..
            } if covered_symbols.len() == 2
        ));
        Ok(())
    }

    #[test]
    fn missing_symbol_or_position_leg_fails_closed() -> Result<(), Box<dyn Error>> {
        let scope = account_scope()?;
        let btc: Symbol = "BTC/USDT".parse()?;
        let eth: Symbol = "ETH/USDT".parse()?;
        let mut missing_leg = account_receipts(&scope, 11)?;
        missing_leg[1] = PhysicalReadbackReceipt::verified_complete_account_targets(
            &scope,
            PhysicalReadbackSurface::Positions,
            51,
            11,
            hash(70),
            BTreeSet::from([btc.clone(), eth]),
            BTreeSet::from([
                (btc.clone(), PositionSide::Long),
                (btc, PositionSide::Short),
            ]),
        )?;
        assert_eq!(
            PhysicalRecoveryReadbackManifest::verified(scope.clone(), missing_leg),
            Err(PhysicalRecoveryManifestError::SymbolCoverage)
        );

        let mut missing_symbol = account_receipts(&scope, 11)?;
        missing_symbol[2] = PhysicalReadbackReceipt::verified_complete_account_targets(
            &scope,
            PhysicalReadbackSurface::UmOrder,
            51,
            11,
            hash(71),
            BTreeSet::from(["BTC/USDT".parse()?]),
            BTreeSet::new(),
        )?;
        assert_eq!(
            PhysicalRecoveryReadbackManifest::verified(scope, missing_symbol),
            Err(PhysicalRecoveryManifestError::SymbolCoverage)
        );
        Ok(())
    }

    #[test]
    fn wrong_scope_and_old_private_generation_cannot_be_relabelled() -> Result<(), Box<dyn Error>> {
        let expected = account_scope()?;
        let wrong_scope = account_scope_with_anchor_and_roots(
            "BTC/USDT",
            PhysicalRecoveryAuthorityRoots::verified_recovered(
                hash(4),
                hash(2),
                hash(3),
                BTreeSet::new(),
            )?,
        )?;
        let mut mixed = account_receipts(&expected, 11)?;
        mixed[0] = PhysicalReadbackReceipt::verified_complete_account(
            &wrong_scope,
            PhysicalReadbackSurface::Account,
            51,
            11,
            hash(72),
            0,
        )?;
        assert_eq!(
            PhysicalRecoveryReadbackManifest::verified(expected.clone(), mixed),
            Err(PhysicalRecoveryManifestError::ScopeDrift)
        );
        assert_eq!(
            PhysicalRecoveryReadbackManifest::verified(
                expected.clone(),
                account_receipts(&expected, 10)?,
            ),
            Err(PhysicalRecoveryManifestError::StaleGeneration)
        );
        Ok(())
    }

    #[test]
    fn native_trading_account_must_equal_the_account_authority() -> Result<(), Box<dyn Error>> {
        let account = AccountKey::new(crate::domain::ExchangeId::Binance, ACCOUNT_ID)?;
        let registration = StrategyBinding::new(
            StrategyInstanceKey::new(
                account.clone(),
                StrategyKind::HedgedGrid,
                "grid_btc",
                "BTC/USDT".parse()?,
            )?,
            "run_btc",
            "btc_config",
        )?;
        let result = PhysicalRecoveryScope::verified_account(
            GatewayBinding::new(
                VenueId::Binance,
                GatewayMode::Live,
                "00000000-0000-4000-8000-000000000002",
                "BTC/USDT".parse()?,
            )?,
            account,
            [(registration, 1)],
            AccountPositionMode::Hedge,
            all_family_support(),
            1,
            1,
            0,
            PhysicalRecoveryAuthorityRoots::verified_recovered(
                hash(1),
                hash(2),
                hash(3),
                BTreeSet::new(),
            )?,
        );
        assert_eq!(result, Err(PhysicalRecoveryManifestError::AccountAuthority));
        Ok(())
    }

    #[test]
    fn native_anchor_must_equal_the_canonical_account_universe_symbol() -> Result<(), Box<dyn Error>>
    {
        let Err(error) = account_scope_with_anchor("ETH/USDT") else {
            return Err("non-canonical native anchor was accepted".into());
        };
        assert_eq!(
            error.downcast_ref::<PhysicalRecoveryManifestError>(),
            Some(&PhysicalRecoveryManifestError::Registry)
        );
        Ok(())
    }
}
