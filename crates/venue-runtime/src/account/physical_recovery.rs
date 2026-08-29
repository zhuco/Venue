use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};
use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

use super::model::validate_config_digest;

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
    },
    Unsupported {
        evidence_sha256: [u8; 32],
        profile_version: u64,
    },
}

impl PhysicalReadbackCoverage {
    fn validate(&self, surface: PhysicalReadbackSurface) -> bool {
        match self {
            Self::Complete {
                evidence_sha256, ..
            } => nonzero_digest(evidence_sha256),
            Self::Unsupported {
                evidence_sha256,
                profile_version,
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

/// Immutable recovery anchor captured before the physical readback attempt. Every returned face
/// commits this entire scope, making binding, configuration, or journal drift invalidate the turn.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalRecoveryScope {
    binding: GatewayBinding,
    config_digest: String,
    config_epoch: u64,
    recovered_private_generation: u64,
    authority_roots: PhysicalRecoveryAuthorityRoots,
    commitment_sha256: [u8; 32],
}

impl PhysicalRecoveryScope {
    pub fn verified(
        binding: GatewayBinding,
        config_digest: impl Into<String>,
        config_epoch: u64,
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
        let commitment_sha256 = scope_commitment(
            &binding,
            &config_digest,
            config_epoch,
            recovered_private_generation,
            &authority_roots,
        );
        Ok(Self {
            binding,
            config_digest,
            config_epoch,
            recovered_private_generation,
            authority_roots,
            commitment_sha256,
        })
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
    pub const fn recovered_private_generation(&self) -> u64 {
        self.recovered_private_generation
    }

    #[must_use]
    pub const fn authority_roots(&self) -> &PhysicalRecoveryAuthorityRoots {
        &self.authority_roots
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
}

fn scope_commitment(
    binding: &GatewayBinding,
    config_digest: &str,
    config_epoch: u64,
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
    commit_u64(&mut digest, recovered_private_generation);
    commit_bytes(&mut digest, &roots.owner);
    commit_bytes(&mut digest, &roots.wal);
    commit_bytes(&mut digest, &roots.unknown);
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
            } => {
                commit_bytes(&mut digest, &[1]);
                commit_bytes(&mut digest, evidence_sha256);
                commit_u64(&mut digest, *record_count);
            }
            PhysicalReadbackCoverage::Unsupported {
                evidence_sha256,
                profile_version,
            } => {
                commit_bytes(&mut digest, &[2]);
                commit_bytes(&mut digest, evidence_sha256);
                commit_u64(&mut digest, *profile_version);
            }
        }
    }
    digest.finalize().into()
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
        GatewayMode::Test => 1,
        GatewayMode::Live => 2,
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
                GatewayMode::Test,
                ACCOUNT_ID,
                "BTC/USDT".parse()?,
            )?,
            "config_1",
            7,
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
}
