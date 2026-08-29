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
}

impl HostAdmittedCapability {
    pub fn authorize(
        &self,
        current_evidence: &HostAdmissionEvidence,
        now_ms: u64,
        mutation: MutationCapability,
    ) -> Result<(), CapabilityPromotionError> {
        if current_evidence != &self.evidence {
            return Err(CapabilityPromotionError::Drift);
        }
        if now_ms < self.issued_ms || now_ms >= self.expires_ms {
            return Err(CapabilityPromotionError::Freshness);
        }
        validate_flags(self.flags, mutation)?;
        Ok(())
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

pub fn promote_capability(
    candidate: &CapabilityProbeCandidate,
    evidence: HostAdmissionEvidence,
    now_ms: u64,
) -> Result<HostAdmittedCapability, CapabilityPromotionError> {
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
    Ok(HostAdmittedCapability {
        capability_version: candidate.capability_version,
        issued_ms: now_ms,
        expires_ms: evidence.promotion_expires_ms,
        flags: candidate.flags,
        probe_commitment: candidate.probe_commitment.clone(),
        evidence,
    })
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
    #[error("candidate flags do not authorize the requested mutation or include withdrawal")]
    Denied,
    #[error(transparent)]
    Gateway(#[from] GatewayApiError),
}
