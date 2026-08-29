use std::collections::{BTreeMap, BTreeSet, VecDeque};

use sha2::{Digest, Sha256};
use venue_execution::execution_command_sha256;

use crate::domain::{
    AccountKey, AccountOrderCapabilityEvidence, AppliedStrategyTurnReceipt, CommandId,
    ExecutionCommand, NativeOrderFamily, StrategyBinding, StrategyInstanceKey,
};

const MAX_QUEUED_ACCOUNT_MUTATIONS: usize = 16_384;
const MAX_CRITICAL_BURST: usize = 64;
const MAX_FILL_REPAIR_BURST: usize = 16;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AccountLanePriority {
    Critical,
    FillRepair,
    Normal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExposureEffect {
    Increase,
    Neutral,
    Reduce,
}

/// Capability proven by the currently held durable writer lease. A protection-only predecessor
/// may keep cancelling or reducing risk, but can never authorize an entry-capable mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountWriterCapability {
    EntryAndRiskReduction,
    RiskReductionOnly,
}

/// Opaque result of allocating stable command/native identities in the durable account journal.
/// The allocator binds instance, config epoch and native order family before a strategy may ask
/// the runtime to admit the semantic mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandIdentityReceipt {
    target: StrategyInstanceKey,
    connection_generation: u64,
    private_generation: u64,
    config_digest: String,
    config_epoch: u64,
    turn_sequence: u64,
    command_id: CommandId,
    native_client_id: CommandId,
    native_order_family: NativeOrderFamily,
    command_sha256: [u8; 32],
    allocation_sequence: u64,
    allocation_record_sha256: [u8; 32],
}

impl CommandIdentityReceipt {
    /// Called only by the durable account-journal allocator after its output record is fsynced.
    /// Runtime and strategy modules cannot invoke this constructor in production builds.
    pub(super) fn persisted_output_allocation(
        applied: &AppliedStrategyTurnReceipt,
        command: &ExecutionCommand,
        cancel_target_family: Option<NativeOrderFamily>,
        allocation_sequence: u64,
        allocation_record_sha256: [u8; 32],
    ) -> Result<Self, AccountLaneError> {
        let token = applied.token();
        let (native_client_id, native_order_family) = match command {
            ExecutionCommand::Cancel(cancel) => (
                cancel.target_client_order_id.clone(),
                cancel_target_family.ok_or(AccountLaneError::IdentityReceipt)?,
            ),
            _ => (
                command
                    .native_client_id()
                    .cloned()
                    .ok_or(AccountLaneError::IdentityReceipt)?,
                command
                    .native_order_family()
                    .ok_or(AccountLaneError::IdentityReceipt)?,
            ),
        };
        if token.private_generation() == 0
            || allocation_sequence == 0
            || allocation_record_sha256.iter().all(|byte| *byte == 0)
            || (matches!(command, ExecutionCommand::Cancel(_)) != cancel_target_family.is_some())
            || command.validate().is_err()
        {
            return Err(AccountLaneError::IdentityReceipt);
        }
        Ok(Self {
            target: token.target().clone(),
            connection_generation: token.connection_generation(),
            private_generation: token.private_generation(),
            config_digest: token.config_digest().to_owned(),
            config_epoch: token.config_epoch(),
            turn_sequence: token.turn_sequence(),
            command_id: command.command_id().clone(),
            native_client_id,
            native_order_family,
            command_sha256: command_sha256(command)?,
            allocation_sequence,
            allocation_record_sha256,
        })
    }

    #[cfg(test)]
    pub(crate) fn test_persisted_output_allocation(
        applied: &AppliedStrategyTurnReceipt,
        command: &ExecutionCommand,
        cancel_target_family: Option<NativeOrderFamily>,
        allocation_sequence: u64,
    ) -> Result<Self, AccountLaneError> {
        Self::persisted_output_allocation(
            applied,
            command,
            cancel_target_family,
            allocation_sequence,
            [0xA5; 32],
        )
    }
}

/// A semantic mutation can only be derived from a persisted actor turn and a durable identity
/// allocation receipt. All four authority fields are runtime-issued rather than strategy input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountExecutionIntent {
    target: StrategyInstanceKey,
    priority: AccountLanePriority,
    command: ExecutionCommand,
    admission_connection_generation: u64,
    admission_private_generation: u64,
    config_digest: String,
    config_epoch: u64,
    turn_sequence: u64,
    native_client_id: CommandId,
    native_order_family: NativeOrderFamily,
    command_sha256: [u8; 32],
    allocation_sequence: u64,
    allocation_record_sha256: [u8; 32],
}

impl AccountExecutionIntent {
    pub(crate) fn from_applied_turn(
        applied: &AppliedStrategyTurnReceipt,
        priority: AccountLanePriority,
        command: ExecutionCommand,
        identity: CommandIdentityReceipt,
    ) -> Result<Self, AccountLaneError> {
        let token = applied.token();
        if token.private_generation() == 0
            || identity.target != *token.target()
            || identity.connection_generation != token.connection_generation()
            || identity.private_generation != token.private_generation()
            || identity.config_digest != token.config_digest()
            || identity.config_epoch != token.config_epoch()
            || identity.turn_sequence != token.turn_sequence()
            || identity.command_id != *command.command_id()
            || identity.command_sha256 != command_sha256(&command)?
            || command.validate().is_err()
        {
            return Err(AccountLaneError::Authority);
        }
        Ok(Self {
            target: token.target().clone(),
            priority,
            command,
            admission_connection_generation: token.connection_generation(),
            admission_private_generation: token.private_generation(),
            config_digest: token.config_digest().to_owned(),
            config_epoch: token.config_epoch(),
            turn_sequence: token.turn_sequence(),
            native_client_id: identity.native_client_id,
            native_order_family: identity.native_order_family,
            command_sha256: identity.command_sha256,
            allocation_sequence: identity.allocation_sequence,
            allocation_record_sha256: identity.allocation_record_sha256,
        })
    }

    #[must_use]
    pub const fn target(&self) -> &StrategyInstanceKey {
        &self.target
    }

    #[must_use]
    pub const fn priority(&self) -> AccountLanePriority {
        self.priority
    }

    #[must_use]
    pub const fn command(&self) -> &ExecutionCommand {
        &self.command
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
    pub const fn admission_connection_generation(&self) -> u64 {
        self.admission_connection_generation
    }

    #[must_use]
    pub const fn admission_private_generation(&self) -> u64 {
        self.admission_private_generation
    }

    #[must_use]
    pub const fn turn_sequence(&self) -> u64 {
        self.turn_sequence
    }

    #[must_use]
    pub const fn native_order_family(&self) -> NativeOrderFamily {
        self.native_order_family
    }

    #[must_use]
    pub const fn native_client_id(&self) -> &CommandId {
        &self.native_client_id
    }

    #[must_use]
    pub const fn exposure(&self) -> ExposureEffect {
        execution_exposure(&self.command)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountExecutionRequest {
    target: StrategyInstanceKey,
    priority: AccountLanePriority,
    command: ExecutionCommand,
    admission_connection_generation: u64,
    admission_private_generation: u64,
    config_digest: String,
    config_epoch: u64,
    turn_sequence: u64,
    native_client_id: CommandId,
    native_order_family: NativeOrderFamily,
    command_sha256: [u8; 32],
    allocation_sequence: u64,
    allocation_record_sha256: [u8; 32],
}

impl AccountExecutionRequest {
    pub(crate) fn authorize(intent: AccountExecutionIntent) -> Result<Self, AccountLaneError> {
        if intent.admission_connection_generation == 0
            || intent.admission_private_generation == 0
            || intent.config_digest.is_empty()
            || intent.config_epoch == 0
            || intent.turn_sequence == 0
        {
            return Err(AccountLaneError::Authority);
        }
        Ok(Self {
            target: intent.target,
            priority: intent.priority,
            command: intent.command,
            admission_connection_generation: intent.admission_connection_generation,
            admission_private_generation: intent.admission_private_generation,
            config_digest: intent.config_digest,
            config_epoch: intent.config_epoch,
            turn_sequence: intent.turn_sequence,
            native_client_id: intent.native_client_id,
            native_order_family: intent.native_order_family,
            command_sha256: intent.command_sha256,
            allocation_sequence: intent.allocation_sequence,
            allocation_record_sha256: intent.allocation_record_sha256,
        })
    }

    #[must_use]
    pub const fn target(&self) -> &StrategyInstanceKey {
        &self.target
    }

    #[must_use]
    pub const fn priority(&self) -> AccountLanePriority {
        self.priority
    }

    #[must_use]
    pub const fn command(&self) -> &ExecutionCommand {
        &self.command
    }

    #[must_use]
    pub fn command_id(&self) -> &CommandId {
        self.command.command_id()
    }

    #[must_use]
    pub const fn admission_connection_generation(&self) -> u64 {
        self.admission_connection_generation
    }

    #[must_use]
    pub const fn admission_private_generation(&self) -> u64 {
        self.admission_private_generation
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
    pub const fn turn_sequence(&self) -> u64 {
        self.turn_sequence
    }

    #[must_use]
    pub const fn native_order_family(&self) -> NativeOrderFamily {
        self.native_order_family
    }

    #[must_use]
    pub const fn native_client_id(&self) -> &CommandId {
        &self.native_client_id
    }

    #[must_use]
    pub const fn exposure(&self) -> ExposureEffect {
        execution_exposure(&self.command)
    }

    /// Stable commitment consumed by the durable recovery manifest. It includes every authority
    /// and allocation field, so recovery cannot substitute a plausible command sharing only IDs.
    pub(crate) fn canonical_recovery_commitment(&self) -> Result<[u8; 32], AccountLaneError> {
        let priority = match self.priority {
            AccountLanePriority::Critical => 0_u8,
            AccountLanePriority::FillRepair => 1,
            AccountLanePriority::Normal => 2,
        };
        let encoded = serde_json::to_vec(&(
            &self.target,
            priority,
            &self.command,
            self.admission_connection_generation,
            self.admission_private_generation,
            &self.config_digest,
            self.config_epoch,
            self.turn_sequence,
            &self.native_client_id,
            self.native_order_family,
            self.command_sha256,
            self.allocation_sequence,
            self.allocation_record_sha256,
        ))
        .map_err(|_| AccountLaneError::CommandEncoding)?;
        Ok(Sha256::digest(encoded).into())
    }
}

#[derive(Clone, Debug)]
struct PreWalCandidateState {
    request: AccountExecutionRequest,
    dispatch_revision: u64,
    revoked: bool,
}

/// A borrowed view of one scheduler candidate. It is deliberately neither `Clone` nor an
/// execution authority: the only production consumer is the durable WAL adapter, which turns it
/// into a request-bound receipt. Dropping the view leaves the candidate revocable in the lane.
#[derive(Debug)]
pub struct PreWalCandidate<'a> {
    account: &'a AccountKey,
    state: &'a PreWalCandidateState,
}

impl PreWalCandidate<'_> {
    #[must_use]
    pub const fn target(&self) -> &StrategyInstanceKey {
        &self.state.request.target
    }

    #[must_use]
    pub fn command_id(&self) -> &CommandId {
        self.state.request.command_id()
    }

    #[must_use]
    pub const fn exposure(&self) -> ExposureEffect {
        self.state.request.exposure()
    }
}

/// Exact durable preparation of a pre-WAL candidate. The receipt commits the complete semantic
/// command plus the runtime dispatch revision observed by the candidate; it is still not a
/// physical mutation permit.
#[derive(Debug, Eq, PartialEq)]
pub struct PersistedWalPreparedReceipt {
    account: AccountKey,
    target: StrategyInstanceKey,
    command_id: CommandId,
    native_client_id: CommandId,
    native_order_family: NativeOrderFamily,
    command_sha256: [u8; 32],
    allocation_sequence: u64,
    allocation_record_sha256: [u8; 32],
    dispatch_revision: u64,
    wal_sequence: u64,
    wal_record_sha256: [u8; 32],
}

impl PersistedWalPreparedReceipt {
    /// Called only after the exact candidate command record is fsynced in the account WAL.
    pub(super) fn persisted(
        candidate: PreWalCandidate<'_>,
        wal_sequence: u64,
        wal_record_sha256: [u8; 32],
    ) -> Result<Self, AccountLaneError> {
        if candidate.state.dispatch_revision == 0
            || wal_sequence == 0
            || wal_record_sha256.iter().all(|byte| *byte == 0)
        {
            return Err(AccountLaneError::WalPreparedReceipt);
        }
        let request = &candidate.state.request;
        Ok(Self {
            account: candidate.account.clone(),
            target: request.target.clone(),
            command_id: request.command_id().clone(),
            native_client_id: request.native_client_id.clone(),
            native_order_family: request.native_order_family,
            command_sha256: request.command_sha256,
            allocation_sequence: request.allocation_sequence,
            allocation_record_sha256: request.allocation_record_sha256,
            dispatch_revision: candidate.state.dispatch_revision,
            wal_sequence,
            wal_record_sha256,
        })
    }

    #[cfg(test)]
    pub(crate) fn test_persisted(
        candidate: PreWalCandidate<'_>,
        wal_sequence: u64,
    ) -> Result<Self, AccountLaneError> {
        Self::persisted(candidate, wal_sequence, [0xE9; 32])
    }
}

/// Opaque proof emitted only after the existing durable writer authority was re-read under its
/// exclusive guard. The pure account kernel consumes this proof; it never creates a writer or a
/// competing writer file.
#[derive(Debug, Eq, PartialEq)]
pub struct PersistedWriterLeaseReceipt {
    account: AccountKey,
    target: StrategyInstanceKey,
    command_id: CommandId,
    dispatch_revision: u64,
    capability: AccountWriterCapability,
    writer_generation: u64,
    writer_revision: u64,
    lease_record_sha256: [u8; 32],
}

impl PersistedWriterLeaseReceipt {
    /// Called by the existing writer-lease adapter while its exact exclusive dispatch guard is
    /// current. This records evidence; it does not create or acquire another writer authority.
    pub(super) fn verified_current(
        wal: &PersistedWalPreparedReceipt,
        capability: AccountWriterCapability,
        writer_generation: u64,
        writer_revision: u64,
        lease_record_sha256: [u8; 32],
    ) -> Result<Self, AccountLaneError> {
        if writer_generation == 0
            || writer_revision == 0
            || lease_record_sha256.iter().all(|byte| *byte == 0)
        {
            return Err(AccountLaneError::WriterLeaseReceipt);
        }
        Ok(Self {
            account: wal.account.clone(),
            target: wal.target.clone(),
            command_id: wal.command_id.clone(),
            dispatch_revision: wal.dispatch_revision,
            capability,
            writer_generation,
            writer_revision,
            lease_record_sha256,
        })
    }

    #[cfg(test)]
    pub(crate) fn test_verified_current(
        wal: &PersistedWalPreparedReceipt,
        capability: AccountWriterCapability,
        writer_revision: u64,
    ) -> Result<Self, AccountLaneError> {
        Self::verified_current(wal, capability, 1, writer_revision, [0xFA; 32])
    }
}

/// One-shot authority produced only after WAL durability, exact writer proof and a second runtime
/// fence check. A caller may dispatch only by consuming this permit; a pre-WAL candidate never
/// exposes an `AccountExecutionRequest`.
#[derive(Debug)]
pub struct AccountDispatchPermit {
    request: AccountExecutionRequest,
    dispatch_revision: u64,
    wal_sequence: u64,
    wal_record_sha256: [u8; 32],
    writer_generation: u64,
    writer_revision: u64,
    lease_record_sha256: [u8; 32],
}

impl AccountDispatchPermit {
    #[must_use]
    pub const fn target(&self) -> &StrategyInstanceKey {
        &self.request.target
    }

    #[must_use]
    pub const fn command(&self) -> &ExecutionCommand {
        &self.request.command
    }

    #[must_use]
    pub fn command_id(&self) -> &CommandId {
        self.request.command_id()
    }

    #[must_use]
    pub const fn native_client_id(&self) -> &CommandId {
        &self.request.native_client_id
    }

    #[must_use]
    pub const fn native_order_family(&self) -> NativeOrderFamily {
        self.request.native_order_family
    }
}

/// Durable WAL exists, but the second runtime/writer fence refused physical dispatch. This token
/// is not a permit; it can only be consumed by the WAL adapter to persist `NotDispatched` or
/// `Unknown`, after which the lane may settle the prepared command.
#[derive(Debug)]
pub struct AccountWalPreparedFence {
    request: AccountExecutionRequest,
    dispatch_revision: u64,
    wal_sequence: u64,
    wal_record_sha256: [u8; 32],
    writer_generation: u64,
    writer_revision: u64,
    lease_record_sha256: [u8; 32],
}

impl AccountWalPreparedFence {
    #[must_use]
    pub fn command_id(&self) -> &CommandId {
        self.request.command_id()
    }
}

#[derive(Debug)]
pub enum AccountDispatchDecision {
    Permit(AccountDispatchPermit),
    Fenced(AccountWalPreparedFence),
}

#[derive(Clone, Debug)]
struct InFlightMutation {
    request: AccountExecutionRequest,
    dispatch_revision: u64,
    wal_sequence: u64,
    wal_record_sha256: [u8; 32],
    writer_generation: u64,
    writer_revision: u64,
    lease_record_sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountMutationOutcome {
    Confirmed,
    Rejected,
    Unknown,
    Transient,
    NotDispatched,
}

/// Durable WAL classification for the exact in-flight command. Transport code cannot advance the
/// lane with a bare enum or command ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedMutationOutcomeReceipt {
    account: AccountKey,
    target: StrategyInstanceKey,
    command_id: CommandId,
    native_client_id: CommandId,
    native_order_family: NativeOrderFamily,
    command_sha256: [u8; 32],
    dispatch_revision: u64,
    prepared_wal_sequence: u64,
    prepared_wal_record_sha256: [u8; 32],
    writer_generation: u64,
    writer_revision: u64,
    lease_record_sha256: [u8; 32],
    dispatch_permitted: bool,
    outcome: AccountMutationOutcome,
    outcome_sequence: u64,
    outcome_record_sha256: [u8; 32],
}

impl PersistedMutationOutcomeReceipt {
    /// Called only after the exact mutation outcome record is durable in the account WAL.
    pub(super) fn persisted(
        permit: &AccountDispatchPermit,
        outcome: AccountMutationOutcome,
        outcome_sequence: u64,
        outcome_record_sha256: [u8; 32],
    ) -> Result<Self, AccountLaneError> {
        if outcome == AccountMutationOutcome::NotDispatched
            || outcome_sequence == 0
            || outcome_record_sha256.iter().all(|byte| *byte == 0)
        {
            return Err(AccountLaneError::OutcomeReceipt);
        }
        let request = &permit.request;
        Ok(Self {
            account: request.target.account.clone(),
            target: request.target.clone(),
            command_id: request.command_id().clone(),
            native_client_id: request.native_client_id.clone(),
            native_order_family: request.native_order_family,
            command_sha256: request.command_sha256,
            dispatch_revision: permit.dispatch_revision,
            prepared_wal_sequence: permit.wal_sequence,
            prepared_wal_record_sha256: permit.wal_record_sha256,
            writer_generation: permit.writer_generation,
            writer_revision: permit.writer_revision,
            lease_record_sha256: permit.lease_record_sha256,
            dispatch_permitted: true,
            outcome,
            outcome_sequence,
            outcome_record_sha256,
        })
    }

    /// Persists the only legal terminal classifications for a WAL-prepared command that never
    /// received a dispatch permit. Uncertain adapter state remains `Unknown`, never a retry.
    pub(super) fn persisted_without_dispatch(
        fence: &AccountWalPreparedFence,
        outcome: AccountMutationOutcome,
        outcome_sequence: u64,
        outcome_record_sha256: [u8; 32],
    ) -> Result<Self, AccountLaneError> {
        if !matches!(
            outcome,
            AccountMutationOutcome::NotDispatched | AccountMutationOutcome::Unknown
        ) || outcome_sequence == 0
            || outcome_record_sha256.iter().all(|byte| *byte == 0)
        {
            return Err(AccountLaneError::OutcomeReceipt);
        }
        let request = &fence.request;
        Ok(Self {
            account: request.target.account.clone(),
            target: request.target.clone(),
            command_id: request.command_id().clone(),
            native_client_id: request.native_client_id.clone(),
            native_order_family: request.native_order_family,
            command_sha256: request.command_sha256,
            dispatch_revision: fence.dispatch_revision,
            prepared_wal_sequence: fence.wal_sequence,
            prepared_wal_record_sha256: fence.wal_record_sha256,
            writer_generation: fence.writer_generation,
            writer_revision: fence.writer_revision,
            lease_record_sha256: fence.lease_record_sha256,
            dispatch_permitted: false,
            outcome,
            outcome_sequence,
            outcome_record_sha256,
        })
    }

    #[cfg(test)]
    pub(crate) fn test_persisted(
        permit: &AccountDispatchPermit,
        outcome: AccountMutationOutcome,
        outcome_sequence: u64,
    ) -> Result<Self, AccountLaneError> {
        Self::persisted(permit, outcome, outcome_sequence, [0xB6; 32])
    }

    #[cfg(test)]
    pub(crate) fn test_persisted_without_dispatch(
        fence: &AccountWalPreparedFence,
        outcome: AccountMutationOutcome,
        outcome_sequence: u64,
    ) -> Result<Self, AccountLaneError> {
        Self::persisted_without_dispatch(fence, outcome, outcome_sequence, [0xB7; 32])
    }
}

/// Proof from the WAL adapter that preparation definitely did not happen. An uncertain filesystem
/// result must instead be persisted as UNKNOWN.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalNotPreparedReceipt {
    account: AccountKey,
    target: StrategyInstanceKey,
    command_id: CommandId,
    native_client_id: CommandId,
    native_order_family: NativeOrderFamily,
    command_sha256: [u8; 32],
    dispatch_revision: u64,
    probe_sequence: u64,
    journal_root_sha256: [u8; 32],
}

impl WalNotPreparedReceipt {
    /// Called only by the WAL adapter after an authoritative absence probe.
    pub(super) fn verified(
        candidate: PreWalCandidate<'_>,
        probe_sequence: u64,
        journal_root_sha256: [u8; 32],
    ) -> Result<Self, AccountLaneError> {
        if probe_sequence == 0 || journal_root_sha256.iter().all(|byte| *byte == 0) {
            return Err(AccountLaneError::WalAbsenceReceipt);
        }
        let request = &candidate.state.request;
        Ok(Self {
            account: candidate.account.clone(),
            target: request.target.clone(),
            command_id: request.command_id().clone(),
            native_client_id: request.native_client_id.clone(),
            native_order_family: request.native_order_family,
            command_sha256: request.command_sha256,
            dispatch_revision: candidate.state.dispatch_revision,
            probe_sequence,
            journal_root_sha256,
        })
    }

    #[cfg(test)]
    pub(crate) fn test_verified(
        candidate: PreWalCandidate<'_>,
        probe_sequence: u64,
    ) -> Result<Self, AccountLaneError> {
        Self::verified(candidate, probe_sequence, [0xC7; 32])
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnknownResolution {
    ProvenAccepted,
    ProvenRejected,
    ProvenAbsent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnknownReadbackProof {
    command_id: CommandId,
    target: StrategyInstanceKey,
    native_client_id: CommandId,
    native_order_family: Option<NativeOrderFamily>,
    connection_generation: u64,
    readback_generation: u64,
    resolution: UnknownResolution,
    signed_readback_sha256: [u8; 32],
}

impl UnknownReadbackProof {
    /// Only an exchange-specific complete signed-readback verifier may call this constructor.
    #[allow(
        clippy::too_many_arguments,
        reason = "unknown settlement must bind the full command and signed readback proof"
    )]
    pub(super) fn verified(
        command_id: CommandId,
        target: StrategyInstanceKey,
        native_client_id: CommandId,
        native_order_family: Option<NativeOrderFamily>,
        connection_generation: u64,
        readback_generation: u64,
        resolution: UnknownResolution,
        signed_readback_sha256: [u8; 32],
    ) -> Result<Self, AccountLaneError> {
        if connection_generation == 0
            || readback_generation == 0
            || signed_readback_sha256.iter().all(|byte| *byte == 0)
        {
            return Err(AccountLaneError::UnknownProof);
        }
        Ok(Self {
            command_id,
            target,
            native_client_id,
            native_order_family,
            connection_generation,
            readback_generation,
            resolution,
            signed_readback_sha256,
        })
    }

    #[cfg(test)]
    pub(crate) fn test_verified(
        command_id: CommandId,
        target: StrategyInstanceKey,
        native_client_id: CommandId,
        native_order_family: Option<NativeOrderFamily>,
        connection_generation: u64,
        readback_generation: u64,
        resolution: UnknownResolution,
    ) -> Result<Self, AccountLaneError> {
        Self::verified(
            command_id,
            target,
            native_client_id,
            native_order_family,
            connection_generation,
            readback_generation,
            resolution,
            [0xD8; 32],
        )
    }

    #[must_use]
    pub const fn command_id(&self) -> &CommandId {
        &self.command_id
    }

    pub(crate) const fn connection_generation(&self) -> u64 {
        self.connection_generation
    }

    pub(crate) const fn readback_generation(&self) -> u64 {
        self.readback_generation
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AccountLaneFollowUp {
    None,
    ReconcileUnknown {
        command_id: CommandId,
        target: StrategyInstanceKey,
    },
    StrategyReplanRequired {
        command_id: CommandId,
        target: StrategyInstanceKey,
        reason: AccountReplanReason,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountReplanReason {
    Transient,
    WalNotPrepared,
    DispatchFenced,
    ProvenAbsent,
}

#[derive(Clone, Debug, Default)]
struct FairPriorityQueue {
    per_instance: BTreeMap<StrategyInstanceKey, VecDeque<AccountExecutionRequest>>,
    rotation: VecDeque<StrategyInstanceKey>,
}

impl FairPriorityQueue {
    fn push(&mut self, request: AccountExecutionRequest) {
        let target = request.target.clone();
        let queue = self.per_instance.entry(target.clone()).or_default();
        if queue.is_empty() {
            self.rotation.push_back(target);
        }
        queue.push_back(request);
    }

    fn pop_eligible(
        &mut self,
        is_eligible: &mut impl FnMut(&AccountExecutionRequest) -> bool,
    ) -> Option<AccountExecutionRequest> {
        let candidates = self.rotation.len();
        for _ in 0..candidates {
            let Some(target) = self.rotation.pop_front() else {
                break;
            };
            let Some(queue) = self.per_instance.get_mut(&target) else {
                continue;
            };
            let queued = queue.len();
            let mut request = None;
            for _ in 0..queued {
                let Some(candidate) = queue.pop_front() else {
                    break;
                };
                if is_eligible(&candidate) {
                    request = Some(candidate);
                    break;
                }
                queue.push_back(candidate);
            }
            if queue.is_empty() {
                self.per_instance.remove(&target);
            } else {
                self.rotation.push_back(target);
            }
            if request.is_some() {
                return request;
            }
        }
        None
    }

    fn drain_matching(
        &mut self,
        mut predicate: impl FnMut(&AccountExecutionRequest) -> bool,
    ) -> Vec<AccountExecutionRequest> {
        let targets: Vec<StrategyInstanceKey> = self.per_instance.keys().cloned().collect();
        let mut drained = Vec::new();
        for target in targets {
            let Some(mut queue) = self.per_instance.remove(&target) else {
                continue;
            };
            let mut retained = VecDeque::new();
            while let Some(request) = queue.pop_front() {
                if predicate(&request) {
                    drained.push(request);
                } else {
                    retained.push_back(request);
                }
            }
            if retained.is_empty() {
                self.rotation.retain(|candidate| candidate != &target);
            } else {
                self.per_instance.insert(target, retained);
            }
        }
        drained
    }
}

/// Pure account-level scheduler. It intentionally owns no writer lease, journal, transport or
/// exchange client: a returned request still requires the existing durable WAL and exact writer
/// guard before dispatch. This prevents the migration core from becoming a second live writer.
#[derive(Clone, Debug)]
pub(crate) struct AccountExecutionLane {
    account: AccountKey,
    capability_evidence: AccountOrderCapabilityEvidence,
    critical: FairPriorityQueue,
    fill_repair: FairPriorityQueue,
    normal: FairPriorityQueue,
    active_command_ids: BTreeSet<CommandId>,
    pre_wal: Option<PreWalCandidateState>,
    wal_prepared: Option<InFlightMutation>,
    in_flight: Option<InFlightMutation>,
    unresolved: BTreeMap<CommandId, AccountExecutionRequest>,
    unresolved_by_instance: BTreeMap<StrategyInstanceKey, usize>,
    critical_burst: usize,
    fill_repair_burst: usize,
}

impl AccountExecutionLane {
    #[must_use]
    pub fn new(account: AccountKey) -> Self {
        let capability_evidence = AccountOrderCapabilityEvidence::for_account(account.clone());
        Self {
            account,
            capability_evidence,
            critical: FairPriorityQueue::default(),
            fill_repair: FairPriorityQueue::default(),
            normal: FairPriorityQueue::default(),
            active_command_ids: BTreeSet::new(),
            pre_wal: None,
            wal_prepared: None,
            in_flight: None,
            unresolved: BTreeMap::new(),
            unresolved_by_instance: BTreeMap::new(),
            critical_burst: 0,
            fill_repair_burst: 0,
        }
    }

    pub fn enqueue(
        &mut self,
        request: AccountExecutionRequest,
        binding: &StrategyBinding,
    ) -> Result<(), AccountLaneError> {
        if !self
            .capability_evidence
            .supports(request.native_order_family)
        {
            return Err(AccountLaneError::UnsupportedOrderFamily);
        }
        if request.target.account != self.account
            || request.admission_connection_generation == 0
            || request.admission_private_generation == 0
            || request.config_epoch == 0
            || request.target != binding.key
            || !binding.matches_owner(request.command.mutation_owner())
            || request.command.validate().is_err()
            || !request_identity_matches(&request)
        {
            return Err(AccountLaneError::Owner);
        }
        if request.priority == AccountLanePriority::Critical
            && request.exposure() == ExposureEffect::Increase
        {
            return Err(AccountLaneError::Priority);
        }
        if request.exposure() == ExposureEffect::Increase
            && self.unresolved_by_instance.contains_key(&request.target)
        {
            return Err(AccountLaneError::UnknownFence);
        }
        if self.active_command_ids.len() >= MAX_QUEUED_ACCOUNT_MUTATIONS {
            return Err(AccountLaneError::Capacity);
        }
        if !self.active_command_ids.insert(request.command_id().clone()) {
            return Err(AccountLaneError::DuplicateCommand);
        }
        match request.priority {
            AccountLanePriority::Critical => self.critical.push(request),
            AccountLanePriority::FillRepair => self.fill_repair.push(request),
            AccountLanePriority::Normal => self.normal.push(request),
        }
        Ok(())
    }

    /// Selects one revocable candidate without granting dispatch authority. The returned borrowed
    /// view cannot outlive this mutable lane borrow or be cloned into an executable request.
    pub(crate) fn next_for_wal(
        &mut self,
        dispatch_revision: u64,
    ) -> Result<Option<PreWalCandidate<'_>>, AccountLaneError> {
        self.next_for_wal_matching(dispatch_revision, |_| true)
    }

    /// The account runtime supplies its current lifecycle fence at dispatch time. Requests that
    /// became ineligible after enqueue remain ordered but cannot block eligible sibling lanes.
    pub(crate) fn next_for_wal_matching(
        &mut self,
        dispatch_revision: u64,
        mut runtime_allows: impl FnMut(&AccountExecutionRequest) -> bool,
    ) -> Result<Option<PreWalCandidate<'_>>, AccountLaneError> {
        if dispatch_revision == 0 {
            return Err(AccountLaneError::DispatchRevision);
        }
        if self.in_flight.is_some() {
            return Err(AccountLaneError::InFlight);
        }
        if self.wal_prepared.is_some() {
            return Err(AccountLaneError::WalPreparedPending);
        }
        if self.pre_wal.is_some() {
            return Err(AccountLaneError::PreWalCandidateActive);
        }
        let unresolved_by_instance = &self.unresolved_by_instance;
        let mut is_eligible = |request: &AccountExecutionRequest| {
            let unknown_fenced = request.exposure() == ExposureEffect::Increase
                && unresolved_by_instance.contains_key(&request.target);
            !unknown_fenced && runtime_allows(request)
        };
        let request = if self.critical_burst >= MAX_CRITICAL_BURST {
            if self.fill_repair_burst >= MAX_FILL_REPAIR_BURST {
                self.normal
                    .pop_eligible(&mut is_eligible)
                    .or_else(|| self.fill_repair.pop_eligible(&mut is_eligible))
                    .or_else(|| self.critical.pop_eligible(&mut is_eligible))
            } else {
                self.fill_repair
                    .pop_eligible(&mut is_eligible)
                    .or_else(|| self.normal.pop_eligible(&mut is_eligible))
                    .or_else(|| self.critical.pop_eligible(&mut is_eligible))
            }
        } else if self.fill_repair_burst >= MAX_FILL_REPAIR_BURST {
            self.critical
                .pop_eligible(&mut is_eligible)
                .or_else(|| self.normal.pop_eligible(&mut is_eligible))
                .or_else(|| self.fill_repair.pop_eligible(&mut is_eligible))
        } else {
            self.critical
                .pop_eligible(&mut is_eligible)
                .or_else(|| self.fill_repair.pop_eligible(&mut is_eligible))
                .or_else(|| self.normal.pop_eligible(&mut is_eligible))
        };
        if let Some(request) = &request {
            match request.priority {
                AccountLanePriority::Critical => {
                    self.critical_burst = self.critical_burst.saturating_add(1);
                }
                AccountLanePriority::FillRepair => {
                    self.critical_burst = 0;
                    self.fill_repair_burst = self.fill_repair_burst.saturating_add(1);
                }
                AccountLanePriority::Normal => {
                    self.critical_burst = 0;
                    self.fill_repair_burst = 0;
                }
            }
        }
        self.pre_wal = request.map(|request| PreWalCandidateState {
            request,
            dispatch_revision,
            revoked: false,
        });
        Ok(self.pre_wal.as_ref().map(|state| PreWalCandidate {
            account: &self.account,
            state,
        }))
    }

    /// Converts the exact pre-WAL candidate into the sole in-flight mutation only after both
    /// durable receipts match it and the account runtime repeats every current authority check.
    pub(crate) fn authorize_dispatch(
        &mut self,
        wal: PersistedWalPreparedReceipt,
        writer: PersistedWriterLeaseReceipt,
        dispatch_revision: u64,
        runtime_allows: impl FnOnce(&AccountExecutionRequest) -> bool,
    ) -> Result<AccountDispatchDecision, AccountLaneError> {
        if self.in_flight.is_some() {
            return Err(AccountLaneError::InFlight);
        }
        if self.wal_prepared.is_some() {
            return Err(AccountLaneError::WalPreparedPending);
        }
        let candidate = self
            .pre_wal
            .as_ref()
            .ok_or(AccountLaneError::PreWalCandidateMissing)?;
        let request = &candidate.request;
        let exact_wal = wal.account == self.account
            && wal.target == request.target
            && wal.command_id == *request.command_id()
            && wal.native_client_id == request.native_client_id
            && wal.native_order_family == request.native_order_family
            && wal.command_sha256 == request.command_sha256
            && wal.allocation_sequence == request.allocation_sequence
            && wal.allocation_record_sha256 == request.allocation_record_sha256
            && wal.dispatch_revision == candidate.dispatch_revision
            && wal.wal_sequence > 0
            && !wal.wal_record_sha256.iter().all(|byte| *byte == 0);
        let exact_writer = writer.account == wal.account
            && writer.target == wal.target
            && writer.command_id == wal.command_id
            && writer.dispatch_revision == wal.dispatch_revision
            && writer.writer_generation > 0
            && writer.writer_revision > 0
            && !writer.lease_record_sha256.iter().all(|byte| *byte == 0);
        let capability_allows = request.exposure() != ExposureEffect::Increase
            || writer.capability == AccountWriterCapability::EntryAndRiskReduction;
        if !exact_wal {
            return Err(AccountLaneError::DispatchAuthority);
        }
        let runtime_authorized = dispatch_revision > 0
            && dispatch_revision == candidate.dispatch_revision
            && !candidate.revoked
            && exact_writer
            && capability_allows
            && runtime_allows(request);

        let candidate = self
            .pre_wal
            .take()
            .ok_or(AccountLaneError::PreWalCandidateMissing)?;
        let request = candidate.request;
        let prepared = InFlightMutation {
            request: request.clone(),
            dispatch_revision: candidate.dispatch_revision,
            wal_sequence: wal.wal_sequence,
            wal_record_sha256: wal.wal_record_sha256,
            writer_generation: writer.writer_generation,
            writer_revision: writer.writer_revision,
            lease_record_sha256: writer.lease_record_sha256,
        };
        if runtime_authorized {
            let permit = AccountDispatchPermit {
                request,
                dispatch_revision: prepared.dispatch_revision,
                wal_sequence: prepared.wal_sequence,
                wal_record_sha256: prepared.wal_record_sha256,
                writer_generation: prepared.writer_generation,
                writer_revision: prepared.writer_revision,
                lease_record_sha256: prepared.lease_record_sha256,
            };
            self.in_flight = Some(prepared);
            Ok(AccountDispatchDecision::Permit(permit))
        } else {
            let fence = AccountWalPreparedFence {
                request,
                dispatch_revision: prepared.dispatch_revision,
                wal_sequence: prepared.wal_sequence,
                wal_record_sha256: prepared.wal_record_sha256,
                writer_generation: prepared.writer_generation,
                writer_revision: prepared.writer_revision,
                lease_record_sha256: prepared.lease_record_sha256,
            };
            self.wal_prepared = Some(prepared);
            Ok(AccountDispatchDecision::Fenced(fence))
        }
    }

    pub(crate) fn record_outcome(
        &mut self,
        receipt: PersistedMutationOutcomeReceipt,
    ) -> Result<AccountLaneFollowUp, AccountLaneError> {
        let in_flight = if receipt.dispatch_permitted {
            self.in_flight.take().ok_or(AccountLaneError::InFlight)?
        } else {
            self.wal_prepared
                .take()
                .ok_or(AccountLaneError::WalPreparedPending)?
        };
        let request = &in_flight.request;
        if receipt.account != self.account
            || receipt.target != request.target
            || receipt.command_id != *request.command_id()
            || receipt.native_client_id != request.native_client_id
            || receipt.native_order_family != request.native_order_family
            || receipt.command_sha256 != request.command_sha256
            || receipt.dispatch_revision != in_flight.dispatch_revision
            || receipt.prepared_wal_sequence != in_flight.wal_sequence
            || receipt.prepared_wal_record_sha256 != in_flight.wal_record_sha256
            || receipt.writer_generation != in_flight.writer_generation
            || receipt.writer_revision != in_flight.writer_revision
            || receipt.lease_record_sha256 != in_flight.lease_record_sha256
            || receipt.outcome_sequence == 0
            || receipt.outcome_record_sha256.iter().all(|byte| *byte == 0)
        {
            if receipt.dispatch_permitted {
                self.in_flight = Some(in_flight);
            } else {
                self.wal_prepared = Some(in_flight);
            }
            return Err(AccountLaneError::InFlightIdentity);
        }
        let command_id = receipt.command_id;
        match receipt.outcome {
            AccountMutationOutcome::Confirmed | AccountMutationOutcome::Rejected => {
                self.active_command_ids.remove(&command_id);
                Ok(AccountLaneFollowUp::None)
            }
            AccountMutationOutcome::Transient | AccountMutationOutcome::NotDispatched => {
                self.active_command_ids.remove(&command_id);
                let target = request.target.clone();
                Ok(AccountLaneFollowUp::StrategyReplanRequired {
                    command_id: command_id.clone(),
                    target,
                    reason: if receipt.outcome == AccountMutationOutcome::NotDispatched {
                        AccountReplanReason::DispatchFenced
                    } else {
                        AccountReplanReason::Transient
                    },
                })
            }
            AccountMutationOutcome::Unknown => {
                let target = request.target.clone();
                self.unresolved.insert(command_id.clone(), request.clone());
                let count = self
                    .unresolved_by_instance
                    .entry(target.clone())
                    .or_default();
                *count = count.checked_add(1).ok_or(AccountLaneError::Overflow)?;
                Ok(AccountLaneFollowUp::ReconcileUnknown {
                    command_id: command_id.clone(),
                    target,
                })
            }
        }
    }

    /// Releases a request only when the caller can prove WAL preparation did not happen. If the
    /// filesystem result is uncertain, the caller must classify it as UNKNOWN instead.
    pub(crate) fn abort_before_wal(
        &mut self,
        receipt: WalNotPreparedReceipt,
    ) -> Result<AccountLaneFollowUp, AccountLaneError> {
        let candidate = self
            .pre_wal
            .take()
            .ok_or(AccountLaneError::PreWalCandidateMissing)?;
        let request = &candidate.request;
        if receipt.account != self.account
            || receipt.target != request.target
            || receipt.command_id != *request.command_id()
            || receipt.native_client_id != request.native_client_id
            || receipt.native_order_family != request.native_order_family
            || receipt.command_sha256 != request.command_sha256
            || receipt.dispatch_revision != candidate.dispatch_revision
            || receipt.probe_sequence == 0
            || receipt.journal_root_sha256.iter().all(|byte| *byte == 0)
        {
            self.pre_wal = Some(candidate);
            return Err(AccountLaneError::InFlightIdentity);
        }
        let command_id = receipt.command_id;
        let target = request.target.clone();
        self.active_command_ids.remove(&command_id);
        Ok(AccountLaneFollowUp::StrategyReplanRequired {
            command_id,
            target,
            reason: AccountReplanReason::WalNotPrepared,
        })
    }

    pub fn resolve_unknown(
        &mut self,
        proof: UnknownReadbackProof,
    ) -> Result<AccountLaneFollowUp, AccountLaneError> {
        let request = self
            .unresolved
            .get(&proof.command_id)
            .cloned()
            .ok_or(AccountLaneError::UnknownMissing)?;
        if request.target != proof.target
            || request.native_client_id != proof.native_client_id
            || Some(request.native_order_family) != proof.native_order_family
            || proof.connection_generation < request.admission_connection_generation
            || (proof.connection_generation == request.admission_connection_generation
                && proof.readback_generation <= request.admission_private_generation)
        {
            return Err(AccountLaneError::UnknownProof);
        }
        self.unresolved.remove(&proof.command_id);
        let count = self
            .unresolved_by_instance
            .get_mut(&request.target)
            .ok_or(AccountLaneError::UnknownMissing)?;
        *count = count.checked_sub(1).ok_or(AccountLaneError::Overflow)?;
        if *count == 0 {
            self.unresolved_by_instance.remove(&request.target);
        }
        match proof.resolution {
            UnknownResolution::ProvenAccepted | UnknownResolution::ProvenRejected => {
                self.active_command_ids.remove(&proof.command_id);
                Ok(AccountLaneFollowUp::None)
            }
            UnknownResolution::ProvenAbsent => {
                self.active_command_ids.remove(&proof.command_id);
                Ok(AccountLaneFollowUp::StrategyReplanRequired {
                    command_id: proof.command_id,
                    target: request.target,
                    reason: AccountReplanReason::ProvenAbsent,
                })
            }
        }
    }

    /// Installs a mutation whose WAL ended in UNKNOWN before process restart. The caller must
    /// build this request from a verified journal recovery snapshot, never from strategy input.
    pub(crate) fn recover_unknown(
        &mut self,
        request: AccountExecutionRequest,
        binding: &StrategyBinding,
    ) -> Result<(), AccountLaneError> {
        if !self
            .capability_evidence
            .supports(request.native_order_family)
        {
            return Err(AccountLaneError::UnsupportedOrderFamily);
        }
        if request.target.account != self.account
            || request.admission_connection_generation == 0
            || request.admission_private_generation == 0
            || request.config_epoch == 0
            || request.target != binding.key
            || !binding.matches_owner(request.command.mutation_owner())
            || request.command.validate().is_err()
            || !request_identity_matches(&request)
        {
            return Err(AccountLaneError::Owner);
        }
        if self.active_command_ids.len() >= MAX_QUEUED_ACCOUNT_MUTATIONS {
            return Err(AccountLaneError::Capacity);
        }
        let command_id = request.command_id().clone();
        if !self.active_command_ids.insert(command_id.clone()) {
            return Err(AccountLaneError::DuplicateCommand);
        }
        let target = request.target.clone();
        self.unresolved.insert(command_id, request);
        let count = self.unresolved_by_instance.entry(target).or_default();
        *count = count.checked_add(1).ok_or(AccountLaneError::Overflow)?;
        Ok(())
    }

    #[must_use]
    pub(crate) fn instance_has_dispatched_or_unknown(&self, key: &StrategyInstanceKey) -> bool {
        self.in_flight
            .as_ref()
            .is_some_and(|in_flight| &in_flight.request.target == key)
            || self
                .wal_prepared
                .as_ref()
                .is_some_and(|prepared| &prepared.request.target == key)
            || self.unresolved_by_instance.contains_key(key)
    }

    #[must_use]
    pub(crate) const fn has_in_flight(&self) -> bool {
        self.pre_wal.is_some() || self.wal_prepared.is_some() || self.in_flight.is_some()
    }

    pub(crate) fn discard_all_queued(&mut self) -> Vec<AccountExecutionRequest> {
        self.discard_queued_matching(|_| true)
    }

    pub(crate) fn discard_queued_instance(
        &mut self,
        key: &StrategyInstanceKey,
    ) -> Vec<AccountExecutionRequest> {
        self.discard_queued_matching(|request| &request.target == key)
    }

    pub(crate) fn discard_queued_risk_increases(&mut self) -> Vec<AccountExecutionRequest> {
        self.discard_queued_matching(|request| request.exposure() == ExposureEffect::Increase)
    }

    pub(crate) fn discard_queued_instance_risk_increases(
        &mut self,
        key: &StrategyInstanceKey,
    ) -> Vec<AccountExecutionRequest> {
        self.discard_queued_matching(|request| {
            &request.target == key && request.exposure() == ExposureEffect::Increase
        })
    }

    /// Revocation does not delete the candidate: the WAL adapter may already have fsynced its
    /// receipt. It must subsequently prove WAL absence or consume the receipt into the
    /// WAL-prepared state, where only a durable outcome/UNKNOWN can settle it.
    pub(crate) fn revoke_pre_wal_candidate(&mut self) {
        if let Some(candidate) = &mut self.pre_wal {
            candidate.revoked = true;
        }
    }

    fn discard_queued_matching(
        &mut self,
        mut predicate: impl FnMut(&AccountExecutionRequest) -> bool,
    ) -> Vec<AccountExecutionRequest> {
        let mut discarded = Vec::new();
        if self
            .pre_wal
            .as_ref()
            .is_some_and(|candidate| predicate(&candidate.request))
            && let Some(candidate) = &mut self.pre_wal
        {
            candidate.revoked = true;
        }
        discarded.extend(self.critical.drain_matching(&mut predicate));
        discarded.extend(self.fill_repair.drain_matching(&mut predicate));
        discarded.extend(self.normal.drain_matching(predicate));
        for request in &discarded {
            self.active_command_ids.remove(request.command_id());
        }
        discarded
    }

    /// Retiring a strategy is allowed only after every dispatched/UNKNOWN command settled. Any
    /// remaining queued work is discarded before the registry can release the symbol.
    pub fn retire_instance(
        &mut self,
        key: &StrategyInstanceKey,
    ) -> Result<Vec<AccountExecutionRequest>, AccountLaneError> {
        if self
            .in_flight
            .as_ref()
            .is_some_and(|in_flight| &in_flight.request.target == key)
            || self
                .wal_prepared
                .as_ref()
                .is_some_and(|prepared| &prepared.request.target == key)
            || self
                .pre_wal
                .as_ref()
                .is_some_and(|candidate| &candidate.request.target == key)
            || self.unresolved_by_instance.contains_key(key)
        {
            return Err(AccountLaneError::InstanceBusy);
        }
        Ok(self.discard_queued_instance(key))
    }
}

const fn execution_exposure(command: &ExecutionCommand) -> ExposureEffect {
    match command {
        ExecutionCommand::PlaceLimit(command) => {
            if command.reduce_only {
                ExposureEffect::Reduce
            } else {
                ExposureEffect::Increase
            }
        }
        ExecutionCommand::PlaceMarket(command) => {
            if command.reduce_only {
                ExposureEffect::Reduce
            } else {
                ExposureEffect::Increase
            }
        }
        ExecutionCommand::MarketReduce(_)
        | ExecutionCommand::StopMarketCloseAll(_)
        | ExecutionCommand::StopMarketFullPosition(_) => ExposureEffect::Reduce,
        ExecutionCommand::Cancel(_) => ExposureEffect::Neutral,
    }
}

fn request_identity_matches(request: &AccountExecutionRequest) -> bool {
    let native_matches = match &request.command {
        ExecutionCommand::Cancel(command) => {
            command.target_client_order_id == request.native_client_id
        }
        command => {
            command.native_client_id() == Some(&request.native_client_id)
                && command.native_order_family() == Some(request.native_order_family)
        }
    };
    native_matches
        && request.allocation_sequence > 0
        && !request
            .allocation_record_sha256
            .iter()
            .all(|byte| *byte == 0)
        && command_sha256(&request.command).ok() == Some(request.command_sha256)
}

fn command_sha256(command: &ExecutionCommand) -> Result<[u8; 32], AccountLaneError> {
    execution_command_sha256(command).map_err(|_| AccountLaneError::CommandEncoding)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AccountLaneError {
    #[error("execution request owner does not match its account strategy binding")]
    Owner,
    #[error("native order family is unsupported by this account capability evidence")]
    UnsupportedOrderFamily,
    #[error("command identity is already queued, in flight, or unresolved")]
    DuplicateCommand,
    #[error("critical priority is reserved for non-risk-increasing mutations")]
    Priority,
    #[error("risk-increasing mutation is fenced until this instance resolves UNKNOWN")]
    UnknownFence,
    #[error("another account mutation is already in flight")]
    InFlight,
    #[error("a revocable pre-WAL candidate is already selected")]
    PreWalCandidateActive,
    #[error("the revocable pre-WAL candidate is missing or was withdrawn")]
    PreWalCandidateMissing,
    #[error("a durable WAL-prepared command must settle before another candidate is selected")]
    WalPreparedPending,
    #[error("runtime dispatch revision is zero or exhausted")]
    DispatchRevision,
    #[error("WAL, writer capability, or current runtime fences do not authorize dispatch")]
    DispatchAuthority,
    #[error("reported mutation result does not match the in-flight command")]
    InFlightIdentity,
    #[error("UNKNOWN command is not pending reconciliation")]
    UnknownMissing,
    #[error("UNKNOWN resolution proof is stale or does not match the exact command owner")]
    UnknownProof,
    #[error("strategy still has an in-flight or UNKNOWN mutation")]
    InstanceBusy,
    #[error("account execution lane counter overflowed")]
    Overflow,
    #[error("execution authority must be stamped by a current account runtime")]
    Authority,
    #[error("command identity was not allocated by the durable account journal")]
    IdentityReceipt,
    #[error("execution command could not be encoded for its durable semantic commitment")]
    CommandEncoding,
    #[error("mutation outcome is not bound to a durable account WAL record")]
    OutcomeReceipt,
    #[error("WAL preparation receipt is not bound to the exact pre-WAL candidate")]
    WalPreparedReceipt,
    #[error("writer lease receipt is not a current durable authority for this exact candidate")]
    WriterLeaseReceipt,
    #[error("WAL absence proof is not bound to an authoritative journal probe")]
    WalAbsenceReceipt,
    #[error("account execution lane reached its bounded mutation capacity")]
    Capacity,
}
