pub mod account;
#[path = "legacy/canary.rs"]
mod canary;
#[path = "legacy/canary_recovery.rs"]
mod canary_recovery;
#[path = "legacy/canary_recovery_runtime.rs"]
mod canary_recovery_runtime;
#[path = "legacy/canary_sequence_runtime.rs"]
mod canary_sequence_runtime;
mod hedged_grid;
#[path = "legacy/hedged_grid_hot_path.rs"]
mod hedged_grid_hot_path;
#[path = "legacy/hedged_grid_live.rs"]
mod hedged_grid_live;
#[path = "shared/private_entry_gate.rs"]
mod private_entry_gate;
#[path = "shared/private_facts_worker.rs"]
mod private_facts_worker;
#[path = "shared/recovery.rs"]
mod recovery;
#[path = "scalping/scalping_applied_risk_owner.rs"]
mod scalping_applied_risk_owner;
#[path = "scalping/scalping_candidate_evidence.rs"]
mod scalping_candidate_evidence;
#[path = "scalping/scalping_control.rs"]
mod scalping_control;
#[path = "scalping/scalping_coordinator.rs"]
mod scalping_coordinator;
#[path = "scalping/scalping_core_ingress.rs"]
mod scalping_core_ingress;
#[path = "scalping/scalping_core_quote_receipt.rs"]
mod scalping_core_quote_receipt;
#[path = "scalping/scalping_deadline_scheduler.rs"]
mod scalping_deadline_scheduler;
#[path = "scalping/scalping_entry_evidence.rs"]
mod scalping_entry_evidence;
#[path = "scalping/scalping_episode_deadline_owner.rs"]
mod scalping_episode_deadline_owner;
#[path = "scalping/scalping_episode_observation.rs"]
mod scalping_episode_observation;
#[path = "scalping/scalping_evidence_source.rs"]
mod scalping_evidence_source;
#[path = "scalping/scalping_live_driver.rs"]
mod scalping_live_driver;
#[path = "scalping/scalping_live_exit.rs"]
mod scalping_live_exit;
#[path = "scalping/scalping_live_gateway.rs"]
mod scalping_live_gateway;
#[path = "scalping/scalping_market_evidence_assembler.rs"]
mod scalping_market_evidence_assembler;
#[path = "scalping/scalping_owner_risk_inbox.rs"]
mod scalping_owner_risk_inbox;
#[path = "scalping/scalping_owner_risk_source.rs"]
mod scalping_owner_risk_source;
#[path = "scalping/scalping_public_market_worker.rs"]
mod scalping_public_market_worker;
#[path = "scalping/scalping_resident.rs"]
mod scalping_resident;
#[path = "scalping/scalping_resident_process.rs"]
mod scalping_resident_process;
#[path = "scalping/scalping_resident_sources.rs"]
mod scalping_resident_sources;
#[path = "scalping/scalping_risk_producer.rs"]
mod scalping_risk_producer;
#[path = "scalping/scalping_shadow.rs"]
mod scalping_shadow;
#[path = "scalping/scalping_shadow_host.rs"]
mod scalping_shadow_host;
#[path = "grid/stage7_grid.rs"]
mod stage7_grid;
#[path = "grid/stage7_public_journal.rs"]
mod stage7_public_journal;
#[path = "grid/stage7_writer_registry.rs"]
mod stage7_writer_registry;
pub mod strategy;
#[path = "shared/supervisor.rs"]
mod supervisor;

pub use canary::{
    BinanceCanaryError, BinanceCanaryPhase, BinanceCanaryReport, BinanceCanaryRequest,
    run_binance_canary,
};
pub use canary_recovery::{
    AlgoOrderReadback, CANARY_RECOVERY_SCHEMA_VERSION, CanaryRecoveryCandidate, CanaryRecoveryPlan,
    EmergencyFlattenLeg, ExactAlgoCancel, ExactOrdinaryCancel, HedgePositionReadback,
    OrdinaryOrderReadback, ProtectionDebtState, RecoveryAlgoOrder, RecoveryOrdinaryOrder,
    RemainFencedReason, SignedCanaryReadback, plan_canary_recovery, scan_unfinished,
};
pub use canary_recovery_runtime::{
    BinanceCanaryRecoveryError, BinanceCanaryRecoveryReport, run_binance_canary_recovery,
};
pub use hedged_grid_live::{
    HedgedGridControlTarget, HedgedGridLiveError, HedgedGridLiveReport, HedgedGridLiveRequest,
    run_hedged_grid_live, set_hedged_grid_control,
};
pub use private_entry_gate::{
    ExecutionProjection, OwnerProjection, PrivateEntryGate, PrivateEntryGateInput,
    PrivateEntryGateReport, PrivateFactsProjectionInput, PrivateProjection, ProtectionProjection,
    RiskBudgetProjection,
};
pub use private_facts_worker::{
    BinancePrivateFactsTransport, BinancePrivateFactsWorker, BinancePrivateFactsWorkerConfig,
    BinancePrivateProjectionAuthorityConfig, PrivateBootstrapScope, PrivateExposure,
    PrivateFactsClockRoot, PrivateFactsCommitReport, PrivateFactsEffect, PrivateFactsFailureStage,
    PrivateFactsReadiness, PrivateFactsSnapshot, PrivateFactsTurn, PrivateFactsWorkerError,
    PrivateFactsWorkerState, PrivateReadbackTicket, drive_binance_private_facts_turn,
    run_binance_private_facts_worker,
};
pub use recovery::{
    AnonymousProtectionCustody, RUNTIME_RECOVERY_SCHEMA_VERSION, RecoveryFactValue,
    RuntimeReconciliationFacts, RuntimeRecoveryDirective, RuntimeRecoveryError,
    RuntimeRecoveryIdentity, RuntimeRecoveryPhase, RuntimeRecoveryState, RuntimeTakeoverReceipt,
    TakeoverCoverage,
};
pub use scalping_applied_risk_owner::{
    AppliedRiskFenceReason, AppliedRiskOwnerCheckpoint, AppliedRiskOwnerTurn,
    SCALPING_APPLIED_RISK_OWNER_SCHEMA_VERSION, ScalpingAppliedRiskOwner,
    ScalpingAppliedRiskOwnerError,
};
pub use scalping_candidate_evidence::{
    SCALPING_CANDIDATE_EVIDENCE_SCHEMA_VERSION, ScalpingCandidateAppliedRiskCheckpoint,
    ScalpingCandidateEvidenceCheckpoint, ScalpingCandidateEvidenceConfig,
    ScalpingCandidateEvidenceCoordinator, ScalpingCandidateEvidenceError,
    ScalpingCandidateEvidenceFrameCursor,
};
pub use scalping_control::{
    MAX_STAGE6_ENTRY_AUTHORITY_TTL_MS, ScalpingControlError, ScalpingControlReport,
    ScalpingControlRequest, apply_scalping_control,
};
pub use scalping_coordinator::{
    CustodyStatus, EpisodeDeadlineCompletion, EpisodeDeadlineOutcome, EpisodeObservation,
    EpisodeProjectionReceipt, PrivateFacts, SCALPING_COORDINATOR_SCHEMA_VERSION,
    ScalpingCoordinatorCheckpoint, ScalpingCoordinatorError, ScalpingCoordinatorOutput,
    ScalpingInput, ScalpingMarketDeliveryReceipt, ScalpingShadowCoordinator, ShadowDisposition,
    episode_observation_fact_id, scalping_market_delivery_receipt,
};
pub use scalping_core_ingress::{
    SCALPING_CORE_OWNER_RISK_PAGES_FILE, SCALPING_CORE_QUOTE_RECEIPTS_FILE,
    ScalpingCoreIngressError, ScalpingCoreOwnerRiskCommitReport,
    ScalpingCoreOwnerRiskCommitRequest, ScalpingCoreQuoteCommitReport,
    ScalpingCoreQuoteCommitRequest, commit_scalping_core_owner_risk_page,
    commit_scalping_core_quote_receipt,
};
pub use scalping_core_quote_receipt::{
    SCALPING_CORE_QUOTE_RECEIPT_SCHEMA_VERSION, ScalpingCoreQuoteReceipt,
    ScalpingCoreQuoteReceiptError, ScalpingCoreQuoteReceiptJournal, ScalpingCoreQuoteReceiptRecord,
    ScalpingCoreQuoteReceiptSource, scalping_candidate_digest, scalping_core_quote_receipt_digest,
};
pub use scalping_deadline_scheduler::{
    DeadlineClockObservation, DeadlineSchedulerOutcome, ScalpingDeadlineScheduler,
    ScalpingDeadlineSchedulerError, ScheduledDeadline, earliest_deadline,
};
pub use scalping_entry_evidence::{
    AppliedRiskReceipt, SCALPING_ENTRY_EVIDENCE_SCHEMA_VERSION, ScalpingEntryEvidenceError,
    ScalpingEntryEvidenceProjection, project_scalping_entry_evidence,
};
pub use scalping_episode_deadline_owner::{
    EpisodeDeadlineOwnerCheckpoint, EpisodeDeadlineOwnerCursor, EpisodeDeadlineOwnerError,
    EpisodeDeadlineOwnerTurn, PendingEpisodeDeadlineCompletion,
    SCALPING_EPISODE_DEADLINE_OWNER_SCHEMA_VERSION, ScalpingEpisodeDeadlineOwner,
};
pub use scalping_episode_observation::{
    EpisodeObservationCursor, EpisodeObservationInput, EpisodeObservationSourceCheckpoint,
    EpisodeObservationSourceConfig, EpisodeObservationSourceReceipt, EpisodeObservationSourceTurn,
    SCALPING_EPISODE_OBSERVATION_SCHEMA_VERSION, ScalpingEpisodeObservationSource,
    ScalpingEpisodeObservationSourceError,
};
pub use scalping_evidence_source::{ScalpingEvidenceSource, ScalpingEvidenceSourceError};
pub use scalping_live_driver::ScalpingLiveDriver;
pub use scalping_live_exit::{
    ScalpingLiveExitAction, ScalpingLiveExitDriveReport, ScalpingLiveExitSettlement,
};
pub use scalping_live_gateway::{
    ScalpingLiveEntryOutcome, ScalpingLiveGateway, ScalpingLiveGatewayConfig,
    ScalpingLiveGatewayError, ScalpingLiveSettlement, ScalpingLiveSettlementAction,
    ScalpingProtectedGateway, ScalpingWriterReconciliation, reconcile_scalping_writer,
    recover_absent_unknown_scalping_entry, recover_unknown_scalping_cancels,
};
pub use scalping_market_evidence_assembler::{
    ScalpingMarketEvidenceAssembler, ScalpingMarketEvidenceAssemblerError,
    ScalpingMarketEvidenceFence,
};
pub use scalping_owner_risk_inbox::{
    ScalpingOwnerRiskInboxError, ScalpingOwnerRiskInboxJournal, ScalpingOwnerRiskInboxReader,
    ScalpingOwnerRiskInboxRecord, scalping_owner_risk_page_digest,
};
pub use scalping_owner_risk_source::{
    SCALPING_OWNER_RISK_SOURCE_SCHEMA_VERSION, ScalpingOwnerRiskPage, ScalpingOwnerRiskSource,
    ScalpingOwnerRiskSourceCheckpoint, ScalpingOwnerRiskSourceError,
    ScalpingOwnerRiskSourceFenceReason, ScalpingOwnerRiskTurn,
    scalping_owner_risk_source_checkpoint_path,
};
pub use scalping_public_market_worker::{
    BinancePublicCaptureTransport, DEPTH_SNAPSHOT_LIMIT, PUBLIC_STREAMS, PublicCaptureCompletion,
    PublicCaptureEffect, PublicCaptureEffectExecutor, PublicCaptureFault, PublicCaptureOutput,
    PublicCaptureTransportError, PublicCaptureWorkerError, ScalpingPublicMarketWorker,
    transport_error_completion,
};
pub use scalping_resident::{
    SCALPING_RESIDENT_LEGACY_PRIORITY, SCALPING_RESIDENT_PRIORITY, ScalpingResidentCycle,
    ScalpingResidentCycleReport, ScalpingResidentMarket, ScalpingResidentRuntime,
    ScalpingResidentRuntimeError,
};
pub use scalping_resident_process::{
    ScalpingLiveResidentError, ScalpingLiveResidentReport, ScalpingLiveResidentRequest,
    ScalpingShadowResidentError, ScalpingShadowResidentReport, ScalpingShadowResidentRequest,
    run_scalping_live_resident, run_scalping_shadow_resident,
};
pub use scalping_resident_sources::{
    DeadlinePreflight, SCALPING_PUBLIC_MAXIMUM_HISTORY, SCALPING_RESIDENT_SOURCES_SCHEMA_VERSION,
    ScalpingAppliedRiskDriveReport, ScalpingResidentSources, ScalpingResidentSourcesConfig,
    ScalpingResidentSourcesError, ScalpingResidentSourcesStatus, ScalpingResidentSourcesTurnReport,
};
pub use scalping_risk_producer::{
    BoundRiskRevaluation, MAX_RISK_FACTS_PER_PAGE, MAX_RISK_REPLAY_PAGES, RiskProofClock,
    RiskRevaluationProducer, RiskRevaluationProducerError,
};
pub use scalping_shadow::{
    ShadowReplayContext, ShadowReplayError, ShadowReplayResult, replay_scalping_shadow,
    replay_scalping_shadow_with_context, replay_scalping_shadow_with_evidence,
    replay_scalping_shadow_with_evidence_and_risk_revaluation, replay_scalping_shadow_with_journal,
    replay_scalping_shadow_with_journal_and_risk_revaluation,
};
pub use scalping_shadow_host::{
    DeadlineTick, ScalpingShadowHost, ScalpingShadowHostError, ScalpingShadowHostReport,
};
pub use stage7_grid::{
    BinanceLegacyStage7BridgeReport, BinanceLegacyStage7BridgeRequest,
    BinanceLegacyStage7StopRequest, ExposureShadowLaneReport, ExposureShadowVerificationError,
    ExposureShadowVerificationReport, ExposureShadowVerifiedDecision, ExposureShadowVerifiedReason,
    InventoryRecoveryAcceptanceReport, InventoryRecoveryEvidenceError, Stage7CanaryRecoveryReport,
    Stage7CanaryReport, Stage7CanaryRequest, Stage7ExecutableHandoffReport,
    Stage7ExecutableHandoffRequest, Stage7ExternalAlgoCleanupReport,
    Stage7ExternalAlgoCleanupRequest, Stage7FlattenReport, Stage7FlattenRequest,
    Stage7GridCanaryReport, Stage7GridError, Stage7GridReport, Stage7GridRequest,
    Stage7PrivateEvidenceRecoveryReport, Stage7PrivateEvidenceRecoveryRequest,
    Stage7PublicEvidenceRecoveryReport, Stage7PublicEvidenceRecoveryRequest,
    VerifiedRawRiskEvidenceRef, recover_stage7_private_evidence, recover_stage7_public_evidence,
    request_binance_legacy_stage7_stop, run_binance_legacy_stage7_bridge,
    run_binance_shared_grid_shadow, run_binance_stage7_canary, run_binance_stage7_canary_recovery,
    run_binance_stage7_executable_handoff, run_binance_stage7_external_algo_cleanup,
    run_binance_stage7_flatten, run_binance_stage7_grid, run_binance_stage7_grid_canary,
    run_bitget_stage7_canary, run_bitget_stage7_canary_recovery,
    run_bitget_stage7_executable_handoff, run_bitget_stage7_flatten, run_bitget_stage7_grid,
    run_bitget_stage7_grid_canary, run_gate_stage7_canary, run_gate_stage7_canary_recovery,
    run_gate_stage7_executable_handoff, run_gate_stage7_flatten, run_gate_stage7_grid,
    run_gate_stage7_grid_canary, set_stage7_grid_control, verify_stage7_exposure_shadow_evidence,
    verify_stage7_inventory_recovery_evidence,
};
pub use supervisor::{
    ActionKind, ControlDisposition, EntryDisposition, InstanceKey, InstanceShutdownDecision,
    InstanceShutdownReport, LifecycleFault, LifecycleInput, LifecycleReport, ParallelCompletion,
    RejectedAction, ShutdownState, SubmissionOutcome, SubmissionReport, SupervisorAction,
    SupervisorError, SupervisorFailure, TerminationReport, TransientAction, report_lifecycle,
    report_parallel_submission, report_shutdown_convergence, report_termination,
    request_instance_shutdown,
};
#[path = "scalping/binance_auto_shadow.rs"]
mod binance_auto_shadow;
#[path = "scalping/binance_market_scan.rs"]
mod binance_market_scan;
pub use binance_auto_shadow::{
    BinanceAutoLiveReport, BinanceAutoLiveRequest, BinanceAutoShadowError, BinanceAutoShadowReport,
    BinanceAutoShadowRequest, run_binance_auto_live, run_binance_auto_shadow,
};
pub use binance_market_scan::{
    BinanceMarketScanError, BinanceMarketScanRecord, BinanceMarketScanReport,
    scan_binance_usdt_perpetuals,
};
