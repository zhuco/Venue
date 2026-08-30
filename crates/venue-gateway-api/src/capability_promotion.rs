use serde::{Deserialize, Serialize};
use venue_domain::domain::NativeOrderFamily;

use crate::{
    CapabilityFlags, CapabilitySnapshot, GatewayApiError, GatewayBinding, MutationCapability,
};

/// Host-admitted capabilities are intentionally shorter-lived than adapter probe artifacts.
pub const MAX_PROMOTION_TTL_MS: u64 = 30_000;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EvidenceCommitment(String);

impl EvidenceCommitment {
    pub fn new(value: impl Into<String>) -> Result<Self, CapabilityPromotionError> {
        let value = value.into();
        if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(CapabilityPromotionError::Commitment);
        }
        Ok(Self(value.to_ascii_lowercase()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for EvidenceCommitment {
    type Error = CapabilityPromotionError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<EvidenceCommitment> for String {
    fn from(value: EvidenceCommitment) -> Self {
        value.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderFamilySupport {
    Complete,
    ExplicitlyUnsupported,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OrderFamilyEvidence {
    support: OrderFamilySupport,
    commitment: EvidenceCommitment,
}

impl OrderFamilyEvidence {
    #[must_use]
    pub const fn new(support: OrderFamilySupport, commitment: EvidenceCommitment) -> Self {
        Self {
            support,
            commitment,
        }
    }

    #[must_use]
    pub const fn support(&self) -> OrderFamilySupport {
        self.support
    }

    #[must_use]
    pub const fn commitment(&self) -> &EvidenceCommitment {
        &self.commitment
    }
}

/// Exact coverage for every canonical native order family. A family may be unsupported only when
/// the adapter supplied an explicit, committed unsupported receipt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompleteOrderFamilyEvidence {
    um_order: OrderFamilyEvidence,
    um_conditional: OrderFamilyEvidence,
    um_algo: OrderFamilyEvidence,
}

impl CompleteOrderFamilyEvidence {
    #[must_use]
    pub const fn new(
        um_order: OrderFamilyEvidence,
        um_conditional: OrderFamilyEvidence,
        um_algo: OrderFamilyEvidence,
    ) -> Self {
        Self {
            um_order,
            um_conditional,
            um_algo,
        }
    }

    #[must_use]
    pub const fn get(&self, family: NativeOrderFamily) -> &OrderFamilyEvidence {
        match family {
            NativeOrderFamily::UmOrder => &self.um_order,
            NativeOrderFamily::UmConditional => &self.um_conditional,
            NativeOrderFamily::UmAlgo => &self.um_algo,
        }
    }
}

/// Adapter output that is safe to persist or pass to the host, but has no authorization method.
/// Converting an adapter snapshot into this type deliberately removes direct mutation authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CapabilityProbeCandidate {
    binding: GatewayBinding,
    capability_version: u64,
    observed_ms: u64,
    expires_ms: u64,
    connection_generation: u64,
    private_generation: u64,
    flags: CapabilityFlags,
    order_families: CompleteOrderFamilyEvidence,
    probe_commitment: EvidenceCommitment,
}

impl CapabilityProbeCandidate {
    pub fn from_snapshot(
        snapshot: CapabilitySnapshot,
        connection_generation: u64,
        private_generation: u64,
        order_families: CompleteOrderFamilyEvidence,
        probe_commitment: EvidenceCommitment,
    ) -> Result<Self, CapabilityPromotionError> {
        snapshot.binding.validate()?;
        if snapshot.version == 0 || connection_generation == 0 || private_generation == 0 {
            return Err(CapabilityPromotionError::Scope);
        }
        if snapshot.observed_ms == 0 || snapshot.expires_ms <= snapshot.observed_ms {
            return Err(CapabilityPromotionError::Freshness);
        }
        Ok(Self {
            binding: snapshot.binding,
            capability_version: snapshot.version,
            observed_ms: snapshot.observed_ms,
            expires_ms: snapshot.expires_ms,
            connection_generation,
            private_generation,
            flags: snapshot.flags,
            order_families,
            probe_commitment,
        })
    }

    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub const fn capability_version(&self) -> u64 {
        self.capability_version
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
    pub const fn order_families(&self) -> &CompleteOrderFamilyEvidence {
        &self.order_families
    }

    #[must_use]
    pub const fn probe_commitment(&self) -> &EvidenceCommitment {
        &self.probe_commitment
    }

    #[must_use]
    pub const fn flags(&self) -> CapabilityFlags {
        self.flags
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PromotionScope {
    binding: GatewayBinding,
    config_epoch: u64,
    connection_generation: u64,
    private_generation: u64,
}

impl PromotionScope {
    pub fn new(
        binding: GatewayBinding,
        config_epoch: u64,
        connection_generation: u64,
        private_generation: u64,
    ) -> Result<Self, CapabilityPromotionError> {
        binding.validate()?;
        if config_epoch == 0 || connection_generation == 0 || private_generation == 0 {
            return Err(CapabilityPromotionError::Scope);
        }
        Ok(Self {
            binding,
            config_epoch,
            connection_generation,
            private_generation,
        })
    }

    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlState {
    Active,
    Paused,
    Stopping,
    Flattening,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlAppliedReceipt {
    scope: PromotionScope,
    state: ControlState,
    durable_sequence: u64,
    observed_ms: u64,
    commitment: EvidenceCommitment,
}

impl ControlAppliedReceipt {
    pub fn new(
        scope: PromotionScope,
        state: ControlState,
        durable_sequence: u64,
        observed_ms: u64,
        commitment: EvidenceCommitment,
    ) -> Result<Self, CapabilityPromotionError> {
        if durable_sequence == 0 || observed_ms == 0 {
            return Err(CapabilityPromotionError::Control);
        }
        Ok(Self {
            scope,
            state,
            durable_sequence,
            observed_ms,
            commitment,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnerRecoveryReceipt {
    scope: PromotionScope,
    owner_revision: u64,
    observed_ms: u64,
    commitment: EvidenceCommitment,
}

impl OwnerRecoveryReceipt {
    pub fn new(
        scope: PromotionScope,
        owner_revision: u64,
        observed_ms: u64,
        commitment: EvidenceCommitment,
    ) -> Result<Self, CapabilityPromotionError> {
        if owner_revision == 0 || observed_ms == 0 {
            return Err(CapabilityPromotionError::OwnerWal);
        }
        Ok(Self {
            scope,
            owner_revision,
            observed_ms,
            commitment,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalRecoveryReceipt {
    scope: PromotionScope,
    tail_sequence: u64,
    pending_commands: u32,
    unknown_commands: u32,
    observed_ms: u64,
    commitment: EvidenceCommitment,
}

impl WalRecoveryReceipt {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        scope: PromotionScope,
        tail_sequence: u64,
        pending_commands: u32,
        unknown_commands: u32,
        observed_ms: u64,
        commitment: EvidenceCommitment,
    ) -> Result<Self, CapabilityPromotionError> {
        if tail_sequence == 0 || observed_ms == 0 {
            return Err(CapabilityPromotionError::OwnerWal);
        }
        Ok(Self {
            scope,
            tail_sequence,
            pending_commands,
            unknown_commands,
            observed_ms,
            commitment,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriterFenceReceipt {
    scope: PromotionScope,
    writer_generation: u64,
    fence_revision: u64,
    observed_ms: u64,
    commitment: EvidenceCommitment,
}

impl WriterFenceReceipt {
    pub fn new(
        scope: PromotionScope,
        writer_generation: u64,
        fence_revision: u64,
        observed_ms: u64,
        commitment: EvidenceCommitment,
    ) -> Result<Self, CapabilityPromotionError> {
        if writer_generation == 0 || fence_revision == 0 || observed_ms == 0 {
            return Err(CapabilityPromotionError::WriterFence);
        }
        Ok(Self {
            scope,
            writer_generation,
            fence_revision,
            observed_ms,
            commitment,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanaryAdmissionReceipt {
    scope: PromotionScope,
    canary_sequence: u64,
    capability_version: u64,
    confirmed_ms: u64,
    expires_ms: u64,
    commitment: EvidenceCommitment,
}

impl CanaryAdmissionReceipt {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        scope: PromotionScope,
        canary_sequence: u64,
        capability_version: u64,
        confirmed_ms: u64,
        expires_ms: u64,
        commitment: EvidenceCommitment,
    ) -> Result<Self, CapabilityPromotionError> {
        if canary_sequence == 0
            || capability_version == 0
            || confirmed_ms == 0
            || expires_ms <= confirmed_ms
        {
            return Err(CapabilityPromotionError::Canary);
        }
        Ok(Self {
            scope,
            canary_sequence,
            capability_version,
            confirmed_ms,
            expires_ms,
            commitment,
        })
    }
}

/// Complete host-side state that must remain byte-for-byte stable for the admitted token's life.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostAdmissionEvidence {
    scope: PromotionScope,
    promotion_expires_ms: u64,
    order_families: CompleteOrderFamilyEvidence,
    control: ControlAppliedReceipt,
    owner: OwnerRecoveryReceipt,
    wal: WalRecoveryReceipt,
    writer: WriterFenceReceipt,
    canary: CanaryAdmissionReceipt,
}

impl HostAdmissionEvidence {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        scope: PromotionScope,
        promotion_expires_ms: u64,
        order_families: CompleteOrderFamilyEvidence,
        control: ControlAppliedReceipt,
        owner: OwnerRecoveryReceipt,
        wal: WalRecoveryReceipt,
        writer: WriterFenceReceipt,
        canary: CanaryAdmissionReceipt,
    ) -> Result<Self, CapabilityPromotionError> {
        if control.scope != scope
            || owner.scope != scope
            || wal.scope != scope
            || writer.scope != scope
            || canary.scope != scope
        {
            return Err(CapabilityPromotionError::Scope);
        }
        if control.state != ControlState::Active {
            return Err(CapabilityPromotionError::Control);
        }
        if wal.pending_commands != 0 || wal.unknown_commands != 0 {
            return Err(CapabilityPromotionError::OwnerWal);
        }
        Ok(Self {
            scope,
            promotion_expires_ms,
            order_families,
            control,
            owner,
            wal,
            writer,
            canary,
        })
    }

    #[must_use]
    pub const fn scope(&self) -> &PromotionScope {
        &self.scope
    }

    #[must_use]
    pub const fn promotion_expires_ms(&self) -> u64 {
        self.promotion_expires_ms
    }
}

/// Opaque in-process authority. Its fields are private, it has no public constructor and it is not
/// serializable, so persisted adapter evidence cannot be relabeled as admitted authority.
#[derive(Debug, Eq, PartialEq)]
pub struct HostAdmittedCapability {
    evidence: HostAdmissionEvidence,
    capability_version: u64,
    issued_ms: u64,
    expires_ms: u64,
    flags: CapabilityFlags,
    probe_commitment: EvidenceCommitment,
    authority_epoch: u64,
    issuance_serial: u64,
}

impl HostAdmittedCapability {
    /// Ordinary callers cannot revalidate host authority by echoing evidence back to this type.
    /// Production authorization must use the internal authority that issued the capability.
    pub fn authorize(
        &self,
        _current_evidence: &HostAdmissionEvidence,
        _now_ms: u64,
        _mutation: MutationCapability,
    ) -> Result<(), CapabilityPromotionError> {
        Err(CapabilityPromotionError::AuthorityUnavailable)
    }

    #[must_use]
    pub const fn scope(&self) -> &PromotionScope {
        &self.evidence.scope
    }

    #[must_use]
    pub const fn capability_version(&self) -> u64 {
        self.capability_version
    }

    #[must_use]
    pub const fn expires_ms(&self) -> u64 {
        self.expires_ms
    }

    #[must_use]
    pub const fn probe_commitment(&self) -> &EvidenceCommitment {
        &self.probe_commitment
    }
}

/// Compatibility entry point for ordinary probe/evidence callers. Evidence values are deliberately
/// not authority: only the crate-internal verifier can promote them.
pub fn promote_capability(
    _candidate: &CapabilityProbeCandidate,
    _evidence: HostAdmissionEvidence,
    _now_ms: u64,
) -> Result<HostAdmittedCapability, CapabilityPromotionError> {
    Err(CapabilityPromotionError::AuthorityUnavailable)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct IssuanceWatermark {
    config_epoch: u64,
    connection_generation: u64,
    private_generation: u64,
    capability_version: u64,
    control_sequence: u64,
    owner_revision: u64,
    wal_tail_sequence: u64,
    writer_generation: u64,
    fence_revision: u64,
    canary_sequence: u64,
}

impl IssuanceWatermark {
    fn from_evidence(
        candidate: &CapabilityProbeCandidate,
        evidence: &HostAdmissionEvidence,
    ) -> Self {
        Self {
            config_epoch: evidence.scope.config_epoch,
            connection_generation: evidence.scope.connection_generation,
            private_generation: evidence.scope.private_generation,
            capability_version: candidate.capability_version,
            control_sequence: evidence.control.durable_sequence,
            owner_revision: evidence.owner.owner_revision,
            wal_tail_sequence: evidence.wal.tail_sequence,
            writer_generation: evidence.writer.writer_generation,
            fence_revision: evidence.writer.fence_revision,
            canary_sequence: evidence.canary.canary_sequence,
        }
    }

    fn is_strict_successor_of(&self, previous: &Self) -> bool {
        let no_rollback = self.config_epoch >= previous.config_epoch
            && self.connection_generation >= previous.connection_generation
            && self.private_generation >= previous.private_generation
            && self.capability_version >= previous.capability_version
            && self.control_sequence >= previous.control_sequence
            && self.owner_revision >= previous.owner_revision
            && self.wal_tail_sequence >= previous.wal_tail_sequence
            && self.writer_generation >= previous.writer_generation
            && self.fence_revision >= previous.fence_revision
            && self.canary_sequence >= previous.canary_sequence;
        no_rollback && self != previous
    }
}

/// Single in-process issuer owned by the host verification boundary. It is deliberately private,
/// non-serializable, and non-cloneable; ordinary probe/evidence callers cannot construct it.
#[allow(dead_code)]
#[derive(Debug)]
struct CapabilityPromotionAuthority {
    scope: PromotionScope,
    authority_epoch: u64,
    issuance_serial: u64,
    last_issuance: Option<IssuanceWatermark>,
}

#[allow(dead_code)]
impl CapabilityPromotionAuthority {
    fn establish(
        scope: PromotionScope,
        authority_epoch: u64,
    ) -> Result<Self, CapabilityPromotionError> {
        if authority_epoch == 0 {
            return Err(CapabilityPromotionError::Scope);
        }
        Ok(Self {
            scope,
            authority_epoch,
            issuance_serial: 0,
            last_issuance: None,
        })
    }

    fn promote(
        &mut self,
        candidate: &CapabilityProbeCandidate,
        evidence: HostAdmissionEvidence,
        now_ms: u64,
    ) -> Result<HostAdmittedCapability, CapabilityPromotionError> {
        if evidence.scope != self.scope
            || candidate.binding != self.scope.binding
            || candidate.connection_generation != self.scope.connection_generation
            || candidate.private_generation != self.scope.private_generation
        {
            return Err(CapabilityPromotionError::Scope);
        }
        validate_promotion(candidate, &evidence, now_ms)?;

        let watermark = IssuanceWatermark::from_evidence(candidate, &evidence);
        if self
            .last_issuance
            .as_ref()
            .is_some_and(|previous| !watermark.is_strict_successor_of(previous))
        {
            return Err(CapabilityPromotionError::Replay);
        }
        let issuance_serial = self
            .issuance_serial
            .checked_add(1)
            .ok_or(CapabilityPromotionError::Replay)?;

        let admitted = HostAdmittedCapability {
            capability_version: candidate.capability_version,
            issued_ms: now_ms,
            expires_ms: evidence.promotion_expires_ms,
            flags: candidate.flags,
            probe_commitment: candidate.probe_commitment.clone(),
            authority_epoch: self.authority_epoch,
            issuance_serial,
            evidence,
        };
        self.issuance_serial = issuance_serial;
        self.last_issuance = Some(watermark);
        Ok(admitted)
    }

    fn authorize(
        &self,
        capability: &HostAdmittedCapability,
        current_evidence: &HostAdmissionEvidence,
        now_ms: u64,
        mutation: MutationCapability,
    ) -> Result<(), CapabilityPromotionError> {
        if capability.authority_epoch != self.authority_epoch
            || capability.issuance_serial != self.issuance_serial
        {
            return Err(CapabilityPromotionError::Replay);
        }
        if current_evidence != &capability.evidence {
            return Err(CapabilityPromotionError::Drift);
        }
        if now_ms < capability.issued_ms || now_ms >= capability.expires_ms {
            return Err(CapabilityPromotionError::Freshness);
        }
        validate_flags(capability.flags, mutation)
    }
}

fn validate_promotion(
    candidate: &CapabilityProbeCandidate,
    evidence: &HostAdmissionEvidence,
    now_ms: u64,
) -> Result<(), CapabilityPromotionError> {
    if candidate.binding != evidence.scope.binding
        || candidate.connection_generation != evidence.scope.connection_generation
        || candidate.private_generation != evidence.scope.private_generation
        || candidate.order_families != evidence.order_families
        || candidate.capability_version != evidence.canary.capability_version
    {
        return Err(CapabilityPromotionError::Scope);
    }
    if candidate.observed_ms == 0
        || now_ms < candidate.observed_ms
        || now_ms >= candidate.expires_ms
        || evidence.control.observed_ms > now_ms
        || evidence.owner.observed_ms > now_ms
        || evidence.wal.observed_ms > now_ms
        || evidence.writer.observed_ms > now_ms
        || evidence.canary.confirmed_ms > now_ms
        || now_ms >= evidence.canary.expires_ms
    {
        return Err(CapabilityPromotionError::Freshness);
    }
    let ttl = evidence
        .promotion_expires_ms
        .checked_sub(now_ms)
        .ok_or(CapabilityPromotionError::Freshness)?;
    if ttl == 0
        || ttl > MAX_PROMOTION_TTL_MS
        || evidence.promotion_expires_ms > candidate.expires_ms
        || evidence.promotion_expires_ms > evidence.canary.expires_ms
    {
        return Err(CapabilityPromotionError::Freshness);
    }
    if candidate
        .order_families
        .get(NativeOrderFamily::UmOrder)
        .support()
        != OrderFamilySupport::Complete
    {
        return Err(CapabilityPromotionError::Denied);
    }
    validate_flags(candidate.flags, MutationCapability::Cancel)?;
    Ok(())
}

fn validate_flags(
    flags: CapabilityFlags,
    mutation: MutationCapability,
) -> Result<(), CapabilityPromotionError> {
    let required_reads = CapabilityFlags::READ_ACCOUNT
        | CapabilityFlags::READ_ORDERS
        | CapabilityFlags::READ_FILLS
        | CapabilityFlags::PRIVATE_STREAM;
    if !flags.contains(required_reads)
        || !flags.contains(CapabilityFlags::TRADE)
        || flags.contains(CapabilityFlags::WITHDRAW)
        || !flags.contains(mutation.flag())
    {
        return Err(CapabilityPromotionError::Denied);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CapabilityPromotionError {
    #[error("capability promotion requires the internal host verification authority")]
    AuthorityUnavailable,
    #[error("capability promotion scope, generation, epoch, or version does not match")]
    Scope,
    #[error("capability promotion evidence is stale, future-dated, or exceeds the short TTL")]
    Freshness,
    #[error("capability promotion evidence commitment must be exactly 32 bytes of hexadecimal")]
    Commitment,
    #[error("Control has not durably applied an active mutation state")]
    Control,
    #[error("Owner or WAL recovery evidence is incomplete or unsettled")]
    OwnerWal,
    #[error("the unique writer fence evidence is incomplete")]
    WriterFence,
    #[error("Canary evidence is incomplete or does not bind this capability version")]
    Canary,
    #[error("host-admitted capability evidence drifted after promotion")]
    Drift,
    #[error("capability promotion or authorization replayed or rolled back authority state")]
    Replay,
    #[error("candidate flags do not authorize the requested mutation or include withdrawal")]
    Denied,
    #[error(transparent)]
    Gateway(#[from] GatewayApiError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GatewayMode, VenueId};

    const ACCOUNT: &str = "00000000-0000-0000-0000-000000000001";
    const NOW_MS: u64 = 10_000;

    fn commitment(byte: char) -> Result<EvidenceCommitment, CapabilityPromotionError> {
        EvidenceCommitment::new(byte.to_string().repeat(64))
    }

    fn binding() -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Bybit,
            GatewayMode::Live,
            ACCOUNT,
            "BTC/USDT".parse()?,
        )?)
    }

    fn authority(
        binding: GatewayBinding,
    ) -> Result<CapabilityPromotionAuthority, CapabilityPromotionError> {
        CapabilityPromotionAuthority::establish(PromotionScope::new(binding, 5, 11, 13)?, 41)
    }

    fn families() -> Result<CompleteOrderFamilyEvidence, CapabilityPromotionError> {
        Ok(CompleteOrderFamilyEvidence::new(
            OrderFamilyEvidence::new(OrderFamilySupport::Complete, commitment('1')?),
            OrderFamilyEvidence::new(OrderFamilySupport::ExplicitlyUnsupported, commitment('2')?),
            OrderFamilyEvidence::new(OrderFamilySupport::ExplicitlyUnsupported, commitment('3')?),
        ))
    }

    fn candidate(
        binding: GatewayBinding,
    ) -> Result<CapabilityProbeCandidate, CapabilityPromotionError> {
        CapabilityProbeCandidate::from_snapshot(
            CapabilitySnapshot {
                binding,
                version: 7,
                observed_ms: 9_000,
                expires_ms: 30_000,
                flags: CapabilityFlags::READ_ACCOUNT
                    | CapabilityFlags::READ_ORDERS
                    | CapabilityFlags::READ_FILLS
                    | CapabilityFlags::PRIVATE_STREAM
                    | CapabilityFlags::TRADE
                    | CapabilityFlags::PLACE_LIMIT
                    | CapabilityFlags::CANCEL,
            },
            11,
            13,
            families()?,
            commitment('4')?,
        )
    }

    fn evidence(
        binding: GatewayBinding,
        control_sequence: u64,
        control_commitment: char,
        expires_ms: u64,
    ) -> Result<HostAdmissionEvidence, CapabilityPromotionError> {
        let scope = PromotionScope::new(binding, 5, 11, 13)?;
        HostAdmissionEvidence::new(
            scope.clone(),
            expires_ms,
            families()?,
            ControlAppliedReceipt::new(
                scope.clone(),
                ControlState::Active,
                control_sequence,
                9_100,
                commitment(control_commitment)?,
            )?,
            OwnerRecoveryReceipt::new(scope.clone(), 23, 9_200, commitment('6')?)?,
            WalRecoveryReceipt::new(scope.clone(), 29, 0, 0, 9_300, commitment('7')?)?,
            WriterFenceReceipt::new(scope.clone(), 19, 31, 9_400, commitment('8')?)?,
            CanaryAdmissionReceipt::new(scope, 37, 7, 9_500, 25_000, commitment('9')?)?,
        )
    }

    fn rebind_evidence_scope(evidence: &mut HostAdmissionEvidence, scope: PromotionScope) {
        evidence.scope = scope.clone();
        evidence.control.scope = scope.clone();
        evidence.owner.scope = scope.clone();
        evidence.wal.scope = scope.clone();
        evidence.writer.scope = scope.clone();
        evidence.canary.scope = scope;
    }

    #[test]
    fn internal_authority_issues_and_checks_an_exact_capability()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let candidate = candidate(binding.clone())?;
        let evidence = evidence(binding.clone(), 17, '5', 20_000)?;
        let mut authority = authority(binding)?;
        let capability = authority.promote(&candidate, evidence.clone(), NOW_MS)?;

        assert_eq!(capability.capability_version(), 7);
        assert_eq!(capability.expires_ms(), 20_000);
        assert_eq!(
            authority.authorize(
                &capability,
                &evidence,
                19_999,
                MutationCapability::PlaceLimit,
            ),
            Ok(())
        );
        Ok(())
    }

    #[test]
    fn internal_promotion_rejects_withdrawal_and_overlong_ttl()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let exact_evidence = evidence(binding.clone(), 17, '5', 20_000)?;
        let mut withdrawal_candidate = candidate(binding.clone())?;
        withdrawal_candidate.flags.insert(CapabilityFlags::WITHDRAW);
        let mut authority = authority(binding.clone())?;
        assert_eq!(
            authority.promote(&withdrawal_candidate, exact_evidence, NOW_MS),
            Err(CapabilityPromotionError::Denied)
        );

        let mut long_candidate = candidate(binding.clone())?;
        long_candidate.expires_ms = 50_000;
        let mut long_evidence = evidence(binding, 17, '5', 20_000)?;
        long_evidence.promotion_expires_ms = NOW_MS + MAX_PROMOTION_TTL_MS + 1;
        long_evidence.canary.expires_ms = 50_000;
        assert_eq!(
            authority.promote(&long_candidate, long_evidence, NOW_MS),
            Err(CapabilityPromotionError::Freshness)
        );
        Ok(())
    }

    #[test]
    fn internal_promotion_rejects_scope_generation_and_family_mismatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let exact_candidate = candidate(binding.clone())?;
        let exact_evidence = evidence(binding.clone(), 17, '5', 20_000)?;
        let mut authority = authority(binding)?;

        let mut generation_drift = exact_candidate.clone();
        generation_drift.private_generation += 1;
        assert_eq!(
            authority.promote(&generation_drift, exact_evidence.clone(), NOW_MS),
            Err(CapabilityPromotionError::Scope)
        );

        let mut family_drift = exact_candidate.clone();
        family_drift.order_families.um_order.commitment = commitment('a')?;
        assert_eq!(
            authority.promote(&family_drift, exact_evidence.clone(), NOW_MS),
            Err(CapabilityPromotionError::Scope)
        );

        let mut scope_drift = exact_evidence;
        let drifted_scope = PromotionScope::new(
            scope_drift.scope.binding.clone(),
            scope_drift.scope.config_epoch + 1,
            scope_drift.scope.connection_generation,
            scope_drift.scope.private_generation,
        )?;
        rebind_evidence_scope(&mut scope_drift, drifted_scope);
        assert_eq!(
            authority.promote(&exact_candidate, scope_drift, NOW_MS),
            Err(CapabilityPromotionError::Scope)
        );
        Ok(())
    }

    #[test]
    fn admission_constructor_rejects_paused_and_unsettled_wal()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let exact = evidence(binding.clone(), 17, '5', 20_000)?;
        let mut paused_control = exact.control.clone();
        paused_control.state = ControlState::Paused;
        assert_eq!(
            HostAdmissionEvidence::new(
                exact.scope.clone(),
                exact.promotion_expires_ms,
                exact.order_families.clone(),
                paused_control,
                exact.owner.clone(),
                exact.wal.clone(),
                exact.writer.clone(),
                exact.canary.clone(),
            ),
            Err(CapabilityPromotionError::Control)
        );

        for (pending_commands, unknown_commands) in [(1, 0), (0, 1)] {
            let exact = evidence(binding.clone(), 17, '5', 20_000)?;
            let mut unsettled_wal = exact.wal.clone();
            unsettled_wal.pending_commands = pending_commands;
            unsettled_wal.unknown_commands = unknown_commands;
            assert_eq!(
                HostAdmissionEvidence::new(
                    exact.scope,
                    exact.promotion_expires_ms,
                    exact.order_families,
                    exact.control,
                    exact.owner,
                    unsettled_wal,
                    exact.writer,
                    exact.canary,
                ),
                Err(CapabilityPromotionError::OwnerWal)
            );
        }
        Ok(())
    }

    #[test]
    fn verified_evidence_drift_invalidates_the_issued_capability()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let candidate = candidate(binding.clone())?;
        let exact_evidence = evidence(binding.clone(), 17, '5', 20_000)?;
        let drifted = evidence(binding.clone(), 17, 'a', 20_000)?;
        let mut authority = authority(binding)?;
        let capability = authority.promote(&candidate, exact_evidence, NOW_MS)?;

        assert_eq!(
            authority.authorize(&capability, &drifted, 15_000, MutationCapability::Cancel,),
            Err(CapabilityPromotionError::Drift)
        );
        Ok(())
    }

    #[test]
    fn every_critical_receipt_and_scope_drift_invalidates_authority()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let candidate = candidate(binding.clone())?;
        let exact = evidence(binding.clone(), 17, '5', 20_000)?;
        let mut authority = authority(binding)?;
        let capability = authority.promote(&candidate, exact.clone(), NOW_MS)?;

        let mut drifts = Vec::new();
        let mut control = exact.clone();
        control.control.durable_sequence += 1;
        drifts.push(control);
        let mut owner = exact.clone();
        owner.owner.owner_revision += 1;
        drifts.push(owner);
        let mut wal = exact.clone();
        wal.wal.tail_sequence += 1;
        drifts.push(wal);
        let mut writer = exact.clone();
        writer.writer.fence_revision += 1;
        drifts.push(writer);
        let mut canary = exact.clone();
        canary.canary.canary_sequence += 1;
        drifts.push(canary);
        let mut family = exact.clone();
        family.order_families.um_order.commitment = commitment('a')?;
        drifts.push(family);

        for drifted_scope in [
            PromotionScope::new(
                exact.scope.binding.clone(),
                exact.scope.config_epoch + 1,
                exact.scope.connection_generation,
                exact.scope.private_generation,
            )?,
            PromotionScope::new(
                exact.scope.binding.clone(),
                exact.scope.config_epoch,
                exact.scope.connection_generation + 1,
                exact.scope.private_generation,
            )?,
            PromotionScope::new(
                exact.scope.binding.clone(),
                exact.scope.config_epoch,
                exact.scope.connection_generation,
                exact.scope.private_generation + 1,
            )?,
        ] {
            let mut scope = exact.clone();
            rebind_evidence_scope(&mut scope, drifted_scope);
            drifts.push(scope);
        }

        for drift in drifts {
            assert_eq!(
                authority.authorize(&capability, &drift, 15_000, MutationCapability::Cancel,),
                Err(CapabilityPromotionError::Drift)
            );
        }
        Ok(())
    }

    #[test]
    fn authority_rejects_receipt_replay_and_revokes_the_previous_serial()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let candidate = candidate(binding.clone())?;
        let first_evidence = evidence(binding.clone(), 17, '5', 20_000)?;
        let mut authority = authority(binding.clone())?;
        let first = authority.promote(&candidate, first_evidence.clone(), NOW_MS)?;

        assert_eq!(
            authority.promote(&candidate, first_evidence.clone(), NOW_MS + 1),
            Err(CapabilityPromotionError::Replay)
        );

        let successor_evidence = evidence(binding, 18, 'a', 20_000)?;
        let _successor = authority.promote(&candidate, successor_evidence, NOW_MS + 1)?;
        assert_eq!(
            authority.authorize(&first, &first_evidence, 15_000, MutationCapability::Cancel,),
            Err(CapabilityPromotionError::Replay)
        );
        Ok(())
    }

    #[test]
    fn issued_capability_expires_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let candidate = candidate(binding.clone())?;
        let evidence = evidence(binding.clone(), 17, '5', 20_000)?;
        let mut authority = authority(binding)?;
        let capability = authority.promote(&candidate, evidence.clone(), NOW_MS)?;

        assert_eq!(
            authority.authorize(&capability, &evidence, 20_000, MutationCapability::Cancel,),
            Err(CapabilityPromotionError::Freshness)
        );
        Ok(())
    }
}
