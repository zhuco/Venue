mod account_lane;
mod canary_evidence;
mod canary_preflight;
mod canary_run_state;
mod canary_sequence;
mod capability_evidence;
mod emergency_flatten;
mod engine;
mod external_algo_cleanup;
mod fill_recovery;
mod gate;
mod journal;
mod private_projection;
mod probe_gate;
mod protection_custody;
mod reconcile;
mod recovery_writer;
mod scalping_entry_quote;
mod writer_lease;

pub use account_lane::{
    AccountDispatchDecision, AccountDispatchPermit, AccountExecutionIntent,
    AccountExecutionRequest, AccountLaneError, AccountLaneFollowUp, AccountLanePriority,
    AccountMutationOutcome, AccountReplanReason, AccountWalPreparedFence, AccountWriterCapability,
    CommandIdentityReceipt, ExposureEffect, PersistedMutationOutcomeReceipt,
    PersistedWalPreparedReceipt, PersistedWriterLeaseReceipt, PreWalCandidate,
    UnknownReadbackProof, UnknownResolution, WalNotPreparedReceipt,
};
pub use canary_evidence::{
    CANARY_EVIDENCE_SCHEMA_VERSION, CanaryEvidenceBinding, CanaryEvidenceError,
    CanaryEvidenceHeader, CanaryEvidenceJournal, CanaryEvidenceRecord, CanaryEvidenceRecovery,
    CanaryEvidenceStage, CanaryEvidenceTerminal, CanaryTerminalState,
    recover as recover_canary_evidence, recover_discovered as recover_discovered_canary_evidence,
};
pub use canary_preflight::{
    CanaryBinding, CanaryPosition, CanaryPreflightApproval, CanaryPreflightError,
    CanaryPreflightInput, CanarySnapshot, authorize_canary_preflight,
};
pub use canary_run_state::{
    CanaryRunBinding, CanaryRunPhase, CanaryRunState, CanaryRunStateError, MAX_UNPROTECTED_MS,
};
pub use canary_sequence::{
    BnbMutationPermit, BnbMutationRequest, CanaryCampaignBinding, CanarySequenceError,
    CanarySequenceGate, SolCompletionReceipt,
};
pub use capability_evidence::{
    CapabilityBinding, CapabilityEvidenceError, CapabilityEvidenceStore, CapabilityProbe,
    sha256_hex,
};
pub use emergency_flatten::{
    EMERGENCY_FLATTEN_PERMIT_TTL_MS, EmergencyDispatchState, EmergencyFlattenAuthorization,
    EmergencyFlattenError, EmergencyFlattenInput, EmergencyFlattenPermit, EmergencyRiskEnvelope,
    authorize_emergency_flatten, validate_emergency_flatten_permit,
};
pub use engine::{
    EntryPreflight, ExecutionError, ExecutionReceipt, PostOnlyProbePreflight, ProtectionPreflight,
    StrategyEntryPreflight, StrategyProtectionPreflight, StrategyReductionPreflight,
    prepare_limit_entry, prepare_strategy_limit_entry, resolve_unknown_order_by_readback,
    submit_emergency_flatten, submit_limit_entry, submit_post_only_probe,
    submit_protection_probe_entry, submit_stop_market_close_all, submit_stop_market_full_position,
    submit_strategy_limit_entry, submit_strategy_reduce, submit_strategy_stop_market_full_position,
    submit_strategy_take_profit_market_full_position,
};
pub(crate) use engine::{submit_cancel, submit_recovery_cancel, submit_recovery_reduce};
pub(crate) use external_algo_cleanup::submit_external_algo_cancel;
pub use external_algo_cleanup::{
    ExternalAlgoCancelCommand, ExternalAlgoCleanupError, ExternalAlgoCleanupJournal,
    ExternalAlgoCleanupRecord, ExternalAlgoCleanupState, ExternalAlgoCustody,
};
pub use fill_recovery::{
    FillEpochGate, FillRecoveryBatch, FillRecoveryCoordinator, FillRecoveryError,
    FillRecoveryReport,
};
pub use gate::{
    CANARY_MAX_ENTRY_NOTIONAL_USDT, Capability, CapabilityEvidence, GateDecision, GateError,
    GateInput, RunMode, evaluate_gate, validate_canary_permit,
};
pub use journal::{CommandJournal, CommandJournalError, CommandReceipt, CommandState};
pub use private_projection::{PrivateProjectionResolverInput, resolve_private_facts_projection};
pub use probe_gate::{
    ProbeExecutionState, ProbeGateError, ProbeKind, ProbePermit, ProbePermitInput,
    authorize_probe_permit, validate_probe_permit,
};
pub use protection_custody::{
    AlgoProtectionCustody, AlgoProtectionCustodyInput, CustodyWriterRole, ProtectionCustody,
    ProtectionCustodyError, ProtectionCustodyInput, ProtectionEvidence,
    prove_algo_protection_custody, prove_protection_custody,
};
pub use reconcile::{ReadbackBatch, Reconciler, ReconciliationError, ReconciliationReport};
pub use recovery_writer::{
    ExternalAlgoCancelAuthorization, ExternalAlgoCancelInput, RecoveryCancelAuthorization,
    RecoveryCancelInput, RecoveryDispatchGuard, RecoveryObservationProof,
    RecoveryReduceAuthorization, RecoveryReduceInput, RecoveryWriterAuthority, RecoveryWriterError,
    RecoveryWriterScope, authorize_external_algo_cancel, authorize_recovery_cancel,
    authorize_recovery_reduce,
};
pub use scalping_entry_quote::{
    SCALPING_ENTRY_QUOTE_SCHEMA_VERSION, ScalpingAdmissionFacts, ScalpingBoundExposure,
    ScalpingBoundLimits, ScalpingBoundQuoteAmount, ScalpingBoundRiskLimit, ScalpingEntryQuote,
    ScalpingEntryQuoteError, ScalpingPrivateAdmission, ScalpingQuoteAuthority,
    scalping_entry_quote_digest, validate_scalping_bound_limits, validate_scalping_entry_quote,
};
pub use writer_lease::{
    DispatchGuard, ExecutableHandoffReceipt, FlatReceipt, ProtectedReceipt, WRITER_LEASE_TTL_MS,
    WriterLeaseAuthority, WriterLeaseError, WriterScope, WriterSession,
};
