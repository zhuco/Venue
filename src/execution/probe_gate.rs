use std::collections::BTreeMap;

use rust_decimal::Decimal;

use crate::domain::{OrderCommand, OrderPurpose, OrderSide, PositionSide};

use super::{
    CanaryEvidenceBinding, CanaryPreflightApproval, Capability, CapabilityEvidence, WriterSession,
    gate::command_fingerprint,
};

const MAX_PROBE_TTL_MS: u64 = 3_000;

/// Read-only status required at the exact boundary where a first place/cancel probe would be
/// journaled. This is evidence, never a mutation handle or a success capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProbeExecutionState {
    pub command_wal_clean: bool,
    pub reconciliation_clean: bool,
    pub reconciliation_generation: u64,
    pub reconciliation_valid_until_ms: u64,
}

/// All scope and freshness inputs for one first place/cancel probe permit.
#[derive(Clone, Copy, Debug)]
pub struct ProbePermitInput<'a> {
    pub kind: ProbeKind,
    pub now_ms: u64,
    pub probe_ttl_ms: u64,
    pub binding: &'a CanaryEvidenceBinding,
    pub preflight: &'a CanaryPreflightApproval,
    pub writer: &'a WriterSession,
    pub command: &'a OrderCommand,
    pub execution: ProbeExecutionState,
    pub capabilities: &'a BTreeMap<Capability, CapabilityEvidence>,
}

/// A command-bound, short-lived proof. It has no conversion from `GateDecision`, and contains
/// neither a client nor any place/cancel/reconciliation authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProbePermit {
    kind: ProbeKind,
    command_sha256: [u8; 32],
    valid_until_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProbeKind {
    PostOnlyPlaceCancel,
    ProtectionEntry,
}

impl ProbePermit {
    pub const fn valid_until_ms(self) -> u64 {
        self.valid_until_ms
    }

    pub fn command_sha256_hex(self) -> String {
        self.command_sha256
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
}

/// Issues a permit only for the smallest preflight-approved hedge entry and the four independently
/// verified read-only capabilities needed to assess its outcome.
pub fn authorize_probe_permit(input: ProbePermitInput<'_>) -> Result<ProbePermit, ProbeGateError> {
    validate_binding(input.binding)?;
    validate_writer(input.writer, input.binding, input.now_ms)?;
    validate_command(input.command, input.binding, input.preflight)?;
    validate_preflight(input.preflight, input.writer, input.now_ms)?;
    validate_execution(input.execution, input.writer, input.now_ms)?;
    let capability_valid_until_ms =
        validate_capabilities(input.kind, input.capabilities, input.now_ms)?;

    if input.probe_ttl_ms == 0 || input.probe_ttl_ms > MAX_PROBE_TTL_MS {
        return Err(ProbeGateError::ProbeTtl);
    }
    let ttl_valid_until_ms = input
        .now_ms
        .checked_add(input.probe_ttl_ms)
        .ok_or(ProbeGateError::ProbeTtl)?;
    let valid_until_ms = [
        input.binding.valid_until_ms,
        input.preflight.valid_until_ms,
        input.writer.valid_until_ms,
        input.execution.reconciliation_valid_until_ms,
        capability_valid_until_ms,
        ttl_valid_until_ms,
    ]
    .into_iter()
    .min()
    .ok_or(ProbeGateError::Capability(Capability::InstrumentRules))?;
    if valid_until_ms <= input.now_ms {
        return Err(ProbeGateError::EvidenceExpired);
    }
    Ok(ProbePermit {
        kind: input.kind,
        command_sha256: command_fingerprint(input.command),
        valid_until_ms,
    })
}

/// Rechecks the exact command at the WAL boundary. A permit cannot authorize a modified command,
/// and expiration is strict so equal timestamps are already invalid.
pub fn validate_probe_permit(
    permit: ProbePermit,
    expected_kind: ProbeKind,
    command: &OrderCommand,
    now_ms: u64,
) -> Result<(), ProbeGateError> {
    if permit.kind != expected_kind {
        return Err(ProbeGateError::ProbeKind);
    }
    if permit.command_sha256 != command_fingerprint(command) {
        return Err(ProbeGateError::CommandFingerprint);
    }
    if permit.valid_until_ms <= now_ms {
        return Err(ProbeGateError::PermitExpired);
    }
    Ok(())
}

fn validate_binding(binding: &CanaryEvidenceBinding) -> Result<(), ProbeGateError> {
    if binding.canary_id.trim().is_empty()
        || binding.exchange.trim().is_empty()
        || binding.account.trim().is_empty()
        || binding.owner_scope.trim().is_empty()
        || binding.release_id.trim().is_empty()
        || !matches!(
            binding.position_side,
            PositionSide::Long | PositionSide::Short
        )
        || binding.quote_cap.asset.as_str() != "USDT"
        || binding.quote_cap.asset != binding.risk_cap.asset
        || binding.symbol.quote() != binding.quote_cap.asset.as_str()
        || !positive_within_cap(binding.quote_cap.value)
        || !positive_within_cap(binding.risk_cap.value)
        || binding.risk_cap.value > binding.quote_cap.value
    {
        return Err(ProbeGateError::Binding);
    }
    Ok(())
}

fn validate_writer(
    writer: &WriterSession,
    binding: &CanaryEvidenceBinding,
    now_ms: u64,
) -> Result<(), ProbeGateError> {
    if writer.scope.exchange != binding.exchange
        || writer.scope.account != binding.account
        || writer.scope.symbol != binding.symbol
        || writer.scope.owner_scope != binding.owner_scope
        || writer.token.trim().is_empty()
        || writer.generation == 0
        || writer.revision == 0
        || writer.readback_generation == 0
    {
        return Err(ProbeGateError::Scope);
    }
    if writer.valid_until_ms <= now_ms {
        return Err(ProbeGateError::WriterExpired);
    }
    Ok(())
}

fn validate_command(
    command: &OrderCommand,
    binding: &CanaryEvidenceBinding,
    preflight: &CanaryPreflightApproval,
) -> Result<(), ProbeGateError> {
    command
        .owner
        .validate()
        .map_err(|_| ProbeGateError::Command)?;
    let expected_side = match binding.position_side {
        PositionSide::Long => OrderSide::Buy,
        PositionSide::Short => OrderSide::Sell,
        PositionSide::Net => return Err(ProbeGateError::Command),
    };
    if command.owner.exchange != binding.exchange
        || command.owner.account != binding.account
        || command.owner.symbol != binding.symbol
        || command.owner.purpose != OrderPurpose::Entry
        || command.position_side != binding.position_side
        || command.side != expected_side
        || command.reduce_only
        || !command.quantity.is_sign_positive()
        || command.quantity.is_zero()
        || command.quantity != preflight.quantity
    {
        return Err(ProbeGateError::Command);
    }
    let expected_notional = command.quantity * command.limit_price.value();
    if preflight.notional.asset != binding.quote_cap.asset
        || !preflight.notional.value.is_sign_positive()
        || preflight.notional.value != expected_notional
        || preflight.notional.value > binding.quote_cap.value
    {
        return Err(ProbeGateError::Notional);
    }
    Ok(())
}

fn validate_preflight(
    preflight: &CanaryPreflightApproval,
    writer: &WriterSession,
    now_ms: u64,
) -> Result<(), ProbeGateError> {
    if preflight.final_generation == 0 || preflight.final_generation != writer.readback_generation {
        return Err(ProbeGateError::Generation);
    }
    if preflight.valid_until_ms <= now_ms {
        return Err(ProbeGateError::PreflightExpired);
    }
    Ok(())
}

fn validate_execution(
    execution: ProbeExecutionState,
    writer: &WriterSession,
    now_ms: u64,
) -> Result<(), ProbeGateError> {
    if !execution.command_wal_clean {
        return Err(ProbeGateError::CommandWal);
    }
    if !execution.reconciliation_clean {
        return Err(ProbeGateError::Reconciliation);
    }
    if execution.reconciliation_generation == 0
        || execution.reconciliation_generation != writer.readback_generation
    {
        return Err(ProbeGateError::Generation);
    }
    if execution.reconciliation_valid_until_ms <= now_ms {
        return Err(ProbeGateError::ReconciliationExpired);
    }
    Ok(())
}

fn validate_capabilities(
    kind: ProbeKind,
    capabilities: &BTreeMap<Capability, CapabilityEvidence>,
    now_ms: u64,
) -> Result<u64, ProbeGateError> {
    let mut earliest = None;
    for capability in required_capabilities(kind) {
        let evidence = capabilities
            .get(capability)
            .ok_or(ProbeGateError::Capability(*capability))?;
        if !valid_sha256(&evidence.evidence_hash)
            || evidence.generation == 0
            || evidence.verified_at_ms == 0
            || evidence.verified_at_ms > now_ms
            || evidence.valid_until_ms <= now_ms
        {
            return Err(ProbeGateError::Capability(*capability));
        }
        earliest = Some(earliest.map_or(evidence.valid_until_ms, |value: u64| {
            value.min(evidence.valid_until_ms)
        }));
    }
    earliest.ok_or(ProbeGateError::Capability(Capability::InstrumentRules))
}

fn required_capabilities(kind: ProbeKind) -> &'static [Capability] {
    const READ_ONLY: &[Capability] = &[
        Capability::InstrumentRules,
        Capability::PublicMarket,
        Capability::PrivateReadback,
        Capability::PrivateStream,
    ];
    const PROTECTION_ENTRY: &[Capability] = &[
        Capability::InstrumentRules,
        Capability::PublicMarket,
        Capability::PrivateReadback,
        Capability::PrivateStream,
        Capability::PlaceLimit,
        Capability::Cancel,
        Capability::Reconciliation,
    ];
    match kind {
        ProbeKind::PostOnlyPlaceCancel => READ_ONLY,
        ProbeKind::ProtectionEntry => PROTECTION_ENTRY,
    }
}

fn positive_within_cap(value: Decimal) -> bool {
    value.is_sign_positive()
        && !value.is_zero()
        && value <= Decimal::new(super::CANARY_MAX_ENTRY_NOTIONAL_USDT, 0)
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProbeGateError {
    #[error("Canary evidence binding is incomplete or exceeds the 10 USDT envelope")]
    Binding,
    #[error("writer scope does not exactly match the Canary binding")]
    Scope,
    #[error("writer session is expired")]
    WriterExpired,
    #[error("entry command does not exactly match the hedge Canary scope")]
    Command,
    #[error("entry command notional does not match the approved quote envelope")]
    Notional,
    #[error("preflight, writer, or reconciliation generation is inconsistent")]
    Generation,
    #[error("preflight approval is expired")]
    PreflightExpired,
    #[error("execution WAL is not clean")]
    CommandWal,
    #[error("reconciliation is not clean")]
    Reconciliation,
    #[error("reconciliation status is expired")]
    ReconciliationExpired,
    #[error("read-only capability {0:?} is missing, invalid, or expired")]
    Capability(Capability),
    #[error("probe TTL must be positive and no more than 3000 ms")]
    ProbeTtl,
    #[error("one of the evidence inputs is expired")]
    EvidenceExpired,
    #[error("permit was issued for a different command")]
    CommandFingerprint,
    #[error("probe permit was issued for a different mutation kind")]
    ProbeKind,
    #[error("probe permit is expired")]
    PermitExpired,
}
