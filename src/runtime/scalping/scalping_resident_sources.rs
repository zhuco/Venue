use std::{fs, num::NonZeroUsize, path::PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    controller::ScalpingControllerUpdate,
    domain::{MarkFunding, MarketEvent},
    indicator::{FeatureFrame, FeatureState, PublicMarketSourceError, ScalpingPublicMarketSource},
    market::{SessionError, SessionState},
    storage::ScalpingEvidenceError,
    storage::{ProjectionStore, StorageError},
    strategy::scalping::{
        CandidateEvidence, CandidatePreparation, ScalpingParams, ScalpingState, StrategyBinding,
    },
};

use super::{
    AppliedRiskOwnerTurn, AppliedRiskReceipt, BoundRiskRevaluation, DeadlineClockObservation,
    EpisodeDeadlineOwnerError, EpisodeDeadlineOwnerTurn, EpisodeObservationInput,
    EpisodeObservationSourceConfig, EpisodeObservationSourceTurn, PrivateEntryGateReport,
    PrivateFacts, PublicCaptureEffect, PublicCaptureEffectExecutor, PublicCaptureOutput,
    PublicCaptureWorkerError, ScalpingAppliedRiskOwner, ScalpingAppliedRiskOwnerError,
    ScalpingCandidateEvidenceCoordinator, ScalpingCandidateEvidenceError,
    ScalpingEpisodeDeadlineOwner, ScalpingEpisodeObservationSource,
    ScalpingEpisodeObservationSourceError, ScalpingEvidenceSource,
    ScalpingMarketEvidenceAssemblerError, ScalpingPublicMarketWorker, ScalpingResidentCycle,
    ScalpingResidentCycleReport, ScalpingResidentMarket, ScalpingResidentRuntime,
    ScalpingResidentRuntimeError, scalping_market_delivery_receipt, transport_error_completion,
};

pub const SCALPING_RESIDENT_SOURCES_SCHEMA_VERSION: u16 = 4;
pub const SCALPING_PUBLIC_MAXIMUM_HISTORY: usize = 2_048;

#[derive(Clone, Debug)]
pub struct ScalpingResidentSourcesConfig {
    pub artifacts_root: PathBuf,
    pub binding: StrategyBinding,
    pub params: ScalpingParams,
    pub mark_stale_after_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ScalpingResidentSourcesCheckpoint {
    schema_version: u16,
    binding_digest: String,
    active_episode_id: Option<String>,
    latest_private: Option<PrivateFacts>,
    pending_mark: Option<MarkFunding>,
    pending_preparation_id: Option<String>,
    #[serde(default)]
    pending_public_frame: Option<PendingPublicFrame>,
    state_digest: String,
}

/// A completed public frame held until the coordinator and then host each acknowledge their exact
/// durable portion. The raw capture was already durable when this is sealed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct PendingPublicFrame {
    frame: FeatureFrame,
    decision_at_ms: u64,
    evidence: Vec<CandidateEvidence>,
    phase: PendingPublicFramePhase,
    authority: PendingPublicAuthority,
}

/// Private and applied-risk authority observed after the resident's higher-priority phases and
/// before the public frame was captured. A pending frame is never transferable across either.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct PendingPublicAuthority {
    private_generation: u64,
    private_observed_at_ms: u64,
    private_root_cause_fact_id: String,
    applied_risk_receipt: Option<AppliedRiskReceipt>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PendingPublicFramePhase {
    Assemble,
    Deliver,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeadlinePreflight {
    Clear,
    AppliedAndAcknowledged { completion_fact_id: String },
    Acknowledged { completion_fact_id: String },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ScalpingResidentSourcesTurnReport {
    pub resident: Option<ScalpingResidentCycleReport>,
    pub public_effect: Option<PublicCaptureEffect>,
    pub public_output: Option<PublicCaptureOutput>,
    pub public_fault_backoff: bool,
    pub episode_observation_applied: bool,
    pub deadline_persisted: bool,
    pub market_evidence_count: usize,
    pub candidate_evidence_retry_pending: bool,
}

/// Exact durable application result. Only `receipt` may be passed to a downstream evidence
/// assembler; a host report without this receipt is deliberately not an applied-risk authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingAppliedRiskDriveReport {
    pub receipt: AppliedRiskReceipt,
    pub resident: Option<ScalpingResidentCycleReport>,
    pub recovered_after_host_apply: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingResidentSourcesStatus {
    pub public_generation: u64,
    pub public_session_state: SessionState,
    pub public_feature_state: FeatureState,
    pub public_in_flight: bool,
    pub deadline_pending: bool,
    pub awaiting_private_recovery: bool,
    pub latest_private: bool,
    pub pending_mark: bool,
    pub pending_preparation: bool,
    pub active_episode: bool,
    pub control_stopped: bool,
    pub applied_risk_ack_proof_id: Option<String>,
    pub applied_risk_ack_cursor_sequence: Option<u64>,
    pub applied_risk_fenced: bool,
}

/// Zero-mutation source composition for the resident Shadow host. It owns public capture
/// scheduling and durable local projections. A caller may attach the existing candidate-evidence
/// coordinator, but this type never creates Core quotes, calibration, risk facts, or evidence.
#[derive(Debug)]
pub struct ScalpingResidentSources {
    resident: ScalpingResidentRuntime,
    public_worker: ScalpingPublicMarketWorker,
    deadline_owner: ScalpingEpisodeDeadlineOwner,
    applied_risk_owner: ScalpingAppliedRiskOwner,
    candidate_evidence: Option<ScalpingCandidateEvidenceCoordinator>,
    episode_source: Option<ScalpingEpisodeObservationSource>,
    config: ScalpingResidentSourcesConfig,
    episode_root: PathBuf,
    store: ProjectionStore,
    checkpoint: ScalpingResidentSourcesCheckpoint,
}

impl ScalpingResidentSources {
    pub fn open_recovered(
        resident: ScalpingResidentRuntime,
        config: ScalpingResidentSourcesConfig,
    ) -> Result<Self, ScalpingResidentSourcesError> {
        validate_config(&config)?;
        let public_root = config.artifacts_root.join("public");
        fs::create_dir_all(&public_root).map_err(|source| ScalpingResidentSourcesError::Io {
            path: public_root.clone(),
            source,
        })?;
        let source = ScalpingPublicMarketSource::new(
            config.binding.symbol.clone(),
            config.params.feature_profile.clone(),
            config.params.feature_digest.clone(),
            config.params.max_data_age_ms,
            NonZeroUsize::new(SCALPING_PUBLIC_MAXIMUM_HISTORY)
                .ok_or(ScalpingResidentSourcesError::Config)?,
        )?;
        let worker = ScalpingPublicMarketWorker::open_recovered(
            config.binding.symbol.clone(),
            public_root.join("raw_market.jsonl"),
            source,
        )?;
        Self::open_with_worker(resident, worker, config)
    }

    pub(crate) fn open_with_worker(
        resident: ScalpingResidentRuntime,
        public_worker: ScalpingPublicMarketWorker,
        config: ScalpingResidentSourcesConfig,
    ) -> Result<Self, ScalpingResidentSourcesError> {
        validate_config(&config)?;
        fs::create_dir_all(&config.artifacts_root).map_err(|source| {
            ScalpingResidentSourcesError::Io {
                path: config.artifacts_root.clone(),
                source,
            }
        })?;
        let episode_root = config.artifacts_root.join("episodes");
        fs::create_dir_all(&episode_root).map_err(|source| ScalpingResidentSourcesError::Io {
            path: episode_root.clone(),
            source,
        })?;
        let store =
            ProjectionStore::new(config.artifacts_root.join("scalping_resident_sources.json"));
        let binding_digest = config.binding.digest();
        let checkpoint = match store.load::<ScalpingResidentSourcesCheckpoint>()? {
            Some(checkpoint) => {
                validate_checkpoint(&checkpoint, &binding_digest, &config.binding)?;
                checkpoint
            }
            None => seal_checkpoint(ScalpingResidentSourcesCheckpoint {
                schema_version: SCALPING_RESIDENT_SOURCES_SCHEMA_VERSION,
                binding_digest,
                active_episode_id: None,
                latest_private: None,
                pending_mark: None,
                pending_preparation_id: None,
                pending_public_frame: None,
                state_digest: String::new(),
            })?,
        };
        let deadline_owner = ScalpingEpisodeDeadlineOwner::open_or_restore(
            config.artifacts_root.join("episode_deadline_owner.json"),
            config.binding.clone(),
        )?;
        let applied_risk_owner = ScalpingAppliedRiskOwner::open_or_restore(
            config.artifacts_root.join("applied_risk_owner.json"),
            config.binding.clone(),
            config.params.risk_per_episode.unit.clone(),
        )?;
        let mut sources = Self {
            resident,
            public_worker,
            deadline_owner,
            applied_risk_owner,
            candidate_evidence: None,
            episode_source: None,
            config,
            episode_root,
            store,
            checkpoint,
        };
        sources.reconcile_restored_state()?;
        Ok(sources)
    }

    /// Adds the optional, already-opened candidate evidence boundary. Missing calibration is
    /// represented by not calling this method; a fenced coordinator is never silently downgraded
    /// to empty evidence because that would hide a durable integrity fault.
    pub fn attach_candidate_evidence(
        &mut self,
        mut coordinator: ScalpingCandidateEvidenceCoordinator,
    ) -> Result<(), ScalpingResidentSourcesError> {
        if coordinator.is_fenced() {
            return Err(ScalpingResidentSourcesError::CandidateEvidenceFenced);
        }
        if self.candidate_evidence.is_some() {
            return Err(ScalpingResidentSourcesError::CandidateEvidenceAttached);
        }
        if coordinator.checkpoint().binding != self.config.binding {
            return Err(ScalpingResidentSourcesError::CandidateEvidenceBinding);
        }
        let delivery_already_assembled =
            self.checkpoint
                .pending_public_frame
                .as_ref()
                .is_some_and(|pending| {
                    coordinator
                        .recover_assembled_market(pending.frame.clone(), pending.decision_at_ms)
                        .is_ok()
                });
        let preparation = if delivery_already_assembled {
            None
        } else {
            pending_host_preparation(&self.resident.host().checkpoint())
        };
        if !delivery_already_assembled {
            coordinator.recover_host_preparation(preparation.clone())?;
        }
        self.checkpoint.pending_preparation_id = preparation.map(|value| value.preparation_id);
        self.persist()?;
        self.candidate_evidence = Some(coordinator);
        Ok(())
    }

    /// Startup preflight. A durable owner pending is applied and acknowledged without polling
    /// private or public transports. The completion reuses only its persisted clock/root.
    pub fn drain_pending_deadline(
        &mut self,
    ) -> Result<DeadlinePreflight, ScalpingResidentSourcesError> {
        let outcome = if let Some(pending) = self.deadline_owner.checkpoint().pending {
            let completion = pending.completion;
            let completion_fact_id = completion.completion_fact_id.clone();
            let clock = DeadlineClockObservation {
                now_ms: completion.completed_at_ms,
                root_cause_fact_id: completion.private_root_cause_fact_id.clone(),
            };
            let already_applied = self
                .resident
                .host()
                .checkpoint()
                .last_episode_deadline_completion
                .as_ref()
                == Some(&completion);
            if !already_applied {
                if self.resident.host().awaiting_private_recovery() {
                    self.resident
                        .recover_pending_episode_deadline_completion(completion.clone())?;
                } else {
                    self.resident.drive_cycle(ScalpingResidentCycle {
                        episode_deadline_completion: Some(completion.clone()),
                        ..ScalpingResidentCycle::default()
                    })?;
                }
            }
            let turn = self
                .deadline_owner
                .turn(&self.resident.host().checkpoint(), clock)?;
            if !matches!(
                turn,
                EpisodeDeadlineOwnerTurn::Acknowledged { completion_fact_id: ref ack }
                    if ack == &completion_fact_id
            ) {
                return Err(ScalpingResidentSourcesError::DeadlineAck);
            }
            if already_applied {
                DeadlinePreflight::Acknowledged { completion_fact_id }
            } else {
                DeadlinePreflight::AppliedAndAcknowledged { completion_fact_id }
            }
        } else {
            DeadlinePreflight::Clear
        };
        self.sync_episode()?;
        Ok(outcome)
    }

    /// Persists controller/private inputs before any episode/public work. A forwarded private is
    /// retained after host persistence regardless of entry readiness.
    pub fn drive_control_private(
        &mut self,
        controller: Option<ScalpingControllerUpdate>,
        private_gate: Option<PrivateEntryGateReport>,
        deadline_clock: Option<DeadlineClockObservation>,
    ) -> Result<ScalpingResidentSourcesTurnReport, ScalpingResidentSourcesError> {
        let mut report = self.drive_control_private_phase(controller, private_gate)?;
        let later = self.drive_episode_deadline(deadline_clock)?;
        merge_turn_report(&mut report, later);
        Ok(report)
    }

    /// First half of the resident priority contract. It durably applies controller and forwarded
    /// private facts, then returns before risk, episode, timer, or public work. A caller may place
    /// one Core-owned terminal risk proof immediately after this phase.
    pub fn drive_control_private_phase(
        &mut self,
        controller: Option<ScalpingControllerUpdate>,
        private_gate: Option<PrivateEntryGateReport>,
    ) -> Result<ScalpingResidentSourcesTurnReport, ScalpingResidentSourcesError> {
        self.require_no_pending()?;
        let forwarded = private_gate
            .as_ref()
            .and_then(|report| report.forwarded_private.clone());
        let clear_private = private_gate
            .as_ref()
            .is_some_and(|report| report.forwarded_private.is_none() && report.control.is_some());
        let clears_preparation = controller.as_ref().is_some_and(|update| {
            update.control() != Some(crate::controller::ControlTarget::Running)
                || update.authorization().is_none()
        }) || private_gate
            .as_ref()
            .is_some_and(|report| report.control.is_some() || !report.entry_ready);
        let resident_report = self.resident.drive_cycle(ScalpingResidentCycle {
            controller,
            private_gate,
            ..ScalpingResidentCycle::default()
        })?;
        if let Some(private) = forwarded {
            self.checkpoint.latest_private = Some(private);
        } else if clear_private {
            self.checkpoint.latest_private = None;
            self.checkpoint.pending_mark = None;
        }
        if clears_preparation {
            self.checkpoint.pending_preparation_id = None;
            self.checkpoint.pending_public_frame = None;
            if let Some(coordinator) = self.candidate_evidence.as_mut() {
                coordinator.record_preparation(None)?;
            }
        }
        self.sync_episode()?;
        self.persist()?;

        Ok(ScalpingResidentSourcesTurnReport {
            resident: Some(resident_report),
            ..ScalpingResidentSourcesTurnReport::default()
        })
    }

    /// Completes the post-private resident work only after the caller had an opportunity to apply
    /// the bounded risk proof. It preserves the legacy episode/timer ordering and never fetches
    /// owner-risk input itself.
    pub fn drive_episode_deadline(
        &mut self,
        deadline_clock: Option<DeadlineClockObservation>,
    ) -> Result<ScalpingResidentSourcesTurnReport, ScalpingResidentSourcesError> {
        self.require_no_pending()?;
        let mut report = ScalpingResidentSourcesTurnReport::default();
        if self.checkpoint.pending_mark.is_some() {
            report.episode_observation_applied = self.consume_pending_mark()?;
        }
        report.deadline_persisted = self.advance_deadline_owner(deadline_clock.as_ref())?;
        if !report.deadline_persisted {
            if let Some(clock) = deadline_clock {
                let timer = self.resident.drive_cycle(ScalpingResidentCycle {
                    deadline_clock: Some(clock),
                    ..ScalpingResidentCycle::default()
                })?;
                merge_resident_report(&mut report.resident, timer);
            }
        }
        self.sync_episode()?;
        self.persist()?;
        Ok(report)
    }

    /// Applies at most one complete owner-risk proof after private reconciliation. The host save
    /// completes first; only then may the applied-risk owner save and publish its exact receipt.
    /// No external owner-risk page is fetched here, so an absent caller proof remains absent.
    pub fn drive_applied_risk(
        &mut self,
        bound: BoundRiskRevaluation,
    ) -> Result<ScalpingAppliedRiskDriveReport, ScalpingResidentSourcesError> {
        self.require_no_pending()?;
        if self.resident.host().awaiting_private_recovery() {
            return Err(ScalpingResidentSourcesError::AwaitingPrivate);
        }
        self.require_current_private()?;
        match self
            .applied_risk_owner
            .turn(&self.resident.host().checkpoint(), &bound)?
        {
            AppliedRiskOwnerTurn::ApplyRequired => {
                let resident = self.resident.drive_cycle(ScalpingResidentCycle {
                    risk: Some(bound.clone()),
                    ..ScalpingResidentCycle::default()
                })?;
                let AppliedRiskOwnerTurn::Persisted(receipt) = self
                    .applied_risk_owner
                    .turn(&self.resident.host().checkpoint(), &bound)?
                else {
                    return Err(ScalpingResidentSourcesError::RiskReceipt);
                };
                let report = ScalpingAppliedRiskDriveReport {
                    receipt,
                    resident: Some(resident),
                    recovered_after_host_apply: false,
                };
                self.record_candidate_applied_risk(&bound, &report.receipt)?;
                Ok(report)
            }
            AppliedRiskOwnerTurn::Persisted(receipt) => {
                let report = ScalpingAppliedRiskDriveReport {
                    receipt,
                    resident: None,
                    recovered_after_host_apply: true,
                };
                self.record_candidate_applied_risk(&bound, &report.receipt)?;
                Ok(report)
            }
            AppliedRiskOwnerTurn::Duplicate(receipt) => {
                let report = ScalpingAppliedRiskDriveReport {
                    receipt,
                    resident: None,
                    recovered_after_host_apply: false,
                };
                self.record_candidate_applied_risk(&bound, &report.receipt)?;
                Ok(report)
            }
        }
    }

    /// Recovery-only crash-window bridge. It can backfill a missing receipt while the restored
    /// host is AwaitingPrivate, but cannot apply a proof or clear that recovery fence.
    pub fn recover_applied_risk_receipt(
        &mut self,
        bound: &BoundRiskRevaluation,
    ) -> Result<ScalpingAppliedRiskDriveReport, ScalpingResidentSourcesError> {
        self.require_no_pending()?;
        match self
            .applied_risk_owner
            .turn(&self.resident.host().checkpoint(), bound)?
        {
            AppliedRiskOwnerTurn::Persisted(receipt) => {
                let report = ScalpingAppliedRiskDriveReport {
                    receipt,
                    resident: None,
                    recovered_after_host_apply: true,
                };
                self.record_candidate_applied_risk(bound, &report.receipt)?;
                Ok(report)
            }
            AppliedRiskOwnerTurn::Duplicate(receipt) => {
                let report = ScalpingAppliedRiskDriveReport {
                    receipt,
                    resident: None,
                    recovered_after_host_apply: false,
                };
                self.record_candidate_applied_risk(bound, &report.receipt)?;
                Ok(report)
            }
            AppliedRiskOwnerTurn::ApplyRequired => {
                Err(ScalpingResidentSourcesError::RiskHostNotApplied)
            }
        }
    }

    /// This is the only applied-proof cursor that an external `ScalpingOwnerRiskSource` may use
    /// for replay recovery. The host's last proof is intentionally not exposed as that ack.
    #[must_use]
    pub fn applied_risk_last_ack_proof_id(&self) -> Option<&str> {
        self.applied_risk_owner.last_ack_proof_id()
    }

    /// Executes at most one public effect and one matching completion. Transport/DataGap faults
    /// remain local backoff; storage, source identity, and host failures fail closed.
    pub fn drive_public_once<E: PublicCaptureEffectExecutor>(
        &mut self,
        executor: &mut E,
        now_ms: u64,
        deadline_clock: Option<&DeadlineClockObservation>,
    ) -> Result<ScalpingResidentSourcesTurnReport, ScalpingResidentSourcesError> {
        self.require_no_pending()?;
        if self.resident.host().awaiting_private_recovery() {
            return Err(ScalpingResidentSourcesError::AwaitingPrivate);
        }
        if self.checkpoint.pending_public_frame.is_some() {
            let mut report = ScalpingResidentSourcesTurnReport::default();
            self.retry_pending_public_frame(&mut report)?;
            self.sync_episode()?;
            self.persist()?;
            return Ok(report);
        }
        let Some(effect) = self.public_worker.next_effect(now_ms) else {
            return Ok(ScalpingResidentSourcesTurnReport::default());
        };
        let (completion, transport_failed) = match executor.execute_effect(effect, now_ms) {
            Ok(completion) => (completion, false),
            Err(error) => (transport_error_completion(effect, &error, now_ms), true),
        };
        let mut report = ScalpingResidentSourcesTurnReport {
            public_effect: Some(effect),
            public_fault_backoff: transport_failed,
            ..ScalpingResidentSourcesTurnReport::default()
        };
        let output = match self.public_worker.complete(completion) {
            Ok(output) => output,
            Err(error) if recoverable_public_fault(&error) => {
                report.public_fault_backoff = true;
                self.clear_market_work()?;
                return Ok(report);
            }
            Err(error) => return Err(error.into()),
        };
        let Some(output) = output else {
            report.public_fault_backoff |=
                self.public_worker.session_state() == SessionState::Backoff;
            if report.public_fault_backoff {
                self.clear_market_work()?;
            }
            return Ok(report);
        };
        report.public_output = Some(output.clone());
        let requires_durable_route =
            output.frame.is_some() || matches!(&output.event.event, MarketEvent::MarkFunding(_));
        if requires_durable_route {
            self.route_public_output(output, now_ms, deadline_clock, &mut report)?;
            self.sync_episode()?;
            self.persist()?;
        }
        Ok(report)
    }

    fn route_public_output(
        &mut self,
        output: PublicCaptureOutput,
        now_ms: u64,
        deadline_clock: Option<&DeadlineClockObservation>,
        report: &mut ScalpingResidentSourcesTurnReport,
    ) -> Result<(), ScalpingResidentSourcesError> {
        match &output.event.event {
            MarketEvent::MarkFunding(mark) => {
                self.checkpoint.pending_mark = Some(mark.clone());
                self.persist()?;
                report.episode_observation_applied = self.consume_pending_mark()?;
                report.deadline_persisted = self.advance_deadline_owner(deadline_clock)?;
            }
            _ => {
                if let Some(frame) = output.frame {
                    if self.candidate_evidence.is_some() {
                        self.checkpoint.pending_public_frame = Some(PendingPublicFrame {
                            frame,
                            decision_at_ms: now_ms,
                            evidence: Vec::new(),
                            phase: PendingPublicFramePhase::Assemble,
                            authority: self.current_public_delivery_authority()?,
                        });
                        self.persist()?;
                        self.retry_pending_public_frame(report)?;
                    } else {
                        self.drive_market(
                            ScalpingResidentMarket {
                                frame,
                                decision_at_ms: now_ms,
                                evidence: Vec::new(),
                                direct_admission: true,
                            },
                            report,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    fn clear_market_work(&mut self) -> Result<(), ScalpingResidentSourcesError> {
        self.checkpoint.pending_preparation_id = None;
        self.checkpoint.pending_public_frame = None;
        if let Some(coordinator) = self.candidate_evidence.as_mut() {
            coordinator.record_preparation(None)?;
        }
        self.persist()
    }

    #[must_use]
    pub fn status(&self) -> ScalpingResidentSourcesStatus {
        ScalpingResidentSourcesStatus {
            public_generation: self.public_worker.generation(),
            public_session_state: self.public_worker.session_state(),
            public_feature_state: self.public_worker.feature_state(),
            public_in_flight: self.public_worker.has_in_flight(),
            deadline_pending: self.deadline_owner.checkpoint().pending.is_some(),
            awaiting_private_recovery: self.resident.host().awaiting_private_recovery(),
            latest_private: self.checkpoint.latest_private.is_some(),
            pending_mark: self.checkpoint.pending_mark.is_some(),
            pending_preparation: self.checkpoint.pending_preparation_id.is_some()
                || self.checkpoint.pending_public_frame.is_some(),
            active_episode: self.checkpoint.active_episode_id.is_some(),
            control_stopped: self.resident.host().checkpoint().control_stopped,
            applied_risk_ack_proof_id: self
                .applied_risk_owner
                .last_ack()
                .map(|receipt| receipt.proof_id.clone()),
            applied_risk_ack_cursor_sequence: self
                .applied_risk_owner
                .last_ack()
                .map(|receipt| receipt.cursor_sequence),
            applied_risk_fenced: self.applied_risk_owner.is_fenced(),
        }
    }

    #[must_use]
    pub fn resident(&self) -> &ScalpingResidentRuntime {
        &self.resident
    }

    pub fn resident_mut(&mut self) -> &mut ScalpingResidentRuntime {
        &mut self.resident
    }

    /// Records the host-side semantic confirmation only after a separately owned gateway has
    /// installed protection. It has no gateway, writer, or exchange capability itself.
    pub fn confirm_live_entry(
        &mut self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<crate::runtime::ScalpingCoordinatorOutput, ScalpingResidentSourcesError> {
        self.require_no_pending()?;
        let output = self
            .resident
            .confirm_live_entry(intent_id, observed_at_ms)?;
        self.sync_episode()?;
        self.persist()?;
        Ok(output)
    }

    /// Records the host-side semantic rejection only after durable flat reconciliation; source
    /// polling never infers an IOC result from a REST response.
    pub fn reject_live_entry(
        &mut self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<crate::runtime::ScalpingCoordinatorOutput, ScalpingResidentSourcesError> {
        self.require_no_pending()?;
        let output = self.resident.reject_live_entry(intent_id, observed_at_ms)?;
        self.sync_episode()?;
        self.persist()?;
        Ok(output)
    }

    fn record_candidate_applied_risk(
        &mut self,
        bound: &BoundRiskRevaluation,
        receipt: &AppliedRiskReceipt,
    ) -> Result<(), ScalpingResidentSourcesError> {
        if let Some(coordinator) = self.candidate_evidence.as_mut() {
            coordinator.record_applied_risk(bound.clone(), receipt.clone())?;
        }
        Ok(())
    }

    /// Retries only a frame whose capture completion was sealed before a retryable evidence
    /// source/join failure. No second public transport effect is issued for that frame.
    fn retry_pending_public_frame(
        &mut self,
        report: &mut ScalpingResidentSourcesTurnReport,
    ) -> Result<(), ScalpingResidentSourcesError> {
        let mut pending = self
            .checkpoint
            .pending_public_frame
            .clone()
            .ok_or(ScalpingResidentSourcesError::Checkpoint)?;
        if pending.phase == PendingPublicFramePhase::Assemble {
            self.require_pending_public_authority(&pending)?;
            let market = {
                let coordinator = self
                    .candidate_evidence
                    .as_mut()
                    .ok_or(ScalpingResidentSourcesError::CandidateEvidenceFenced)?;
                if let Ok(market) = coordinator
                    .recover_assembled_market(pending.frame.clone(), pending.decision_at_ms)
                {
                    market
                } else {
                    match (|| {
                        coordinator.refresh_core_quote_source()?;
                        coordinator.refresh_evidence_source()?;
                        coordinator.assemble(pending.frame.clone(), pending.decision_at_ms)
                    })() {
                        Ok(market) => market,
                        Err(error) if is_retryable_candidate_error(&error) => {
                            report.candidate_evidence_retry_pending = true;
                            return Ok(());
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            };
            pending.evidence = market.evidence;
            pending.phase = PendingPublicFramePhase::Deliver;
            self.checkpoint.pending_public_frame = Some(pending.clone());
            self.persist()?;
        }

        let market = ScalpingResidentMarket {
            frame: pending.frame.clone(),
            decision_at_ms: pending.decision_at_ms,
            evidence: pending.evidence.clone(),
            direct_admission: false,
        };
        let receipt = scalping_market_delivery_receipt(
            &market.frame,
            market.decision_at_ms,
            &market.evidence,
        )
        .map_err(|_| ScalpingResidentSourcesError::Checkpoint)?;
        if self
            .resident
            .host()
            .checkpoint()
            .last_market_delivery
            .as_ref()
            != Some(&receipt)
        {
            self.require_pending_public_authority(&pending)?;
            self.drive_market(market, report)?;
            if self
                .resident
                .host()
                .checkpoint()
                .last_market_delivery
                .as_ref()
                != Some(&receipt)
            {
                report.candidate_evidence_retry_pending = true;
                return Ok(());
            }
        }
        // The host checkpoint is the authoritative delivery acknowledgement.  Recover its
        // preparation before acknowledging this source-side delivery, because a crash after the
        // host save but before `drive_market` records it must still preserve the N -> N+1 join.
        let preparation =
            pending_host_preparation(&self.resident.host().checkpoint()).filter(|preparation| {
                preparation.frame_generation == pending.frame.generation
                    && preparation.watermark_ms == pending.frame.watermark_ms
            });
        if let Some(coordinator) = self.candidate_evidence.as_mut() {
            coordinator.recover_host_preparation(preparation.clone())?;
        }
        self.checkpoint.pending_preparation_id = preparation
            .as_ref()
            .map(|preparation| preparation.preparation_id.clone());
        self.checkpoint.pending_public_frame = None;
        self.persist()?;
        Ok(())
    }

    fn drive_market(
        &mut self,
        market: ScalpingResidentMarket,
        report: &mut ScalpingResidentSourcesTurnReport,
    ) -> Result<(), ScalpingResidentSourcesError> {
        report.market_evidence_count = market.evidence.len();
        let cycle = self.resident.drive_cycle(ScalpingResidentCycle {
            market: Some(market),
            ..ScalpingResidentCycle::default()
        })?;
        let preparation = cycle
            .market
            .as_ref()
            .and_then(|market| market.preparation.as_ref())
            .cloned();
        self.checkpoint.pending_preparation_id = preparation
            .as_ref()
            .map(|preparation| preparation.preparation_id.clone());
        if let Some(coordinator) = self.candidate_evidence.as_mut() {
            coordinator.record_preparation(preparation)?;
        }
        report.resident = Some(cycle);
        Ok(())
    }

    fn reconcile_restored_state(&mut self) -> Result<(), ScalpingResidentSourcesError> {
        let host = self.resident.host().checkpoint();
        let host_episode = host
            .strategy
            .episode
            .as_ref()
            .map(|episode| episode.episode_id.clone());
        let private_matches = self
            .checkpoint
            .latest_private
            .as_ref()
            .is_some_and(|private| {
                host.last_private_generation == Some(private.generation)
                    && host.last_private_observed_at_ms == Some(private.observed_at_ms)
                    && host.last_private_root_cause_fact_id.as_deref()
                        == Some(private.root_cause_fact_id.as_str())
            });
        if !private_matches {
            self.checkpoint.latest_private = None;
            self.checkpoint.pending_mark = None;
        } else if self.resident.host().awaiting_private_recovery() {
            // The host checkpoint does not retain complete safety/custody. Keep the durable mark,
            // but require a strictly newer private fact before deriving another observation.
            self.checkpoint.latest_private = None;
        }
        if self.checkpoint.active_episode_id != host_episode {
            self.checkpoint.active_episode_id = host_episode;
            self.checkpoint.pending_mark = None;
            self.checkpoint.pending_preparation_id = None;
            self.checkpoint.pending_public_frame = None;
        }
        self.episode_source = None;
        self.ensure_episode_source()?;
        self.persist()
    }

    fn consume_pending_mark(&mut self) -> Result<bool, ScalpingResidentSourcesError> {
        if self.resident.host().awaiting_private_recovery() {
            return Ok(false);
        }
        let Some(private) = self.checkpoint.latest_private.clone() else {
            return Ok(false);
        };
        if self.checkpoint.active_episode_id.is_none() {
            self.checkpoint.pending_mark = None;
            self.persist()?;
            return Ok(false);
        }
        let Some(mark) = self.checkpoint.pending_mark.clone() else {
            return Ok(false);
        };
        if mark.received_at_ms > private.observed_at_ms {
            return Ok(false);
        }
        if private.observed_at_ms.saturating_sub(mark.received_at_ms)
            > self.config.mark_stale_after_ms
        {
            self.checkpoint.pending_mark = None;
            self.persist()?;
            return Ok(false);
        }
        self.ensure_episode_source()?;
        let source = self
            .episode_source
            .as_mut()
            .ok_or(ScalpingResidentSourcesError::EpisodeSource)?;
        let turn = source.turn(EpisodeObservationInput {
            private,
            market_event: MarketEvent::MarkFunding(mark),
        })?;
        let observation = match turn {
            EpisodeObservationSourceTurn::Applied(observation)
            | EpisodeObservationSourceTurn::Duplicate(observation) => observation,
        };
        self.resident.drive_cycle(ScalpingResidentCycle {
            episode_observation: Some(observation),
            ..ScalpingResidentCycle::default()
        })?;
        self.checkpoint.pending_mark = None;
        self.sync_episode()?;
        self.persist()?;
        Ok(true)
    }

    fn advance_deadline_owner(
        &mut self,
        clock: Option<&DeadlineClockObservation>,
    ) -> Result<bool, ScalpingResidentSourcesError> {
        if self.deadline_owner.checkpoint().pending.is_some() {
            return Ok(true);
        }
        let Some(clock) = clock else {
            return Ok(false);
        };
        let host = self.resident.host().checkpoint();
        let Some(receipt) = host.last_episode_projection.as_ref() else {
            return Ok(false);
        };
        if receipt.private_root_cause_fact_id != clock.root_cause_fact_id {
            return Ok(false);
        }
        match self.deadline_owner.turn(&host, clock.clone())? {
            EpisodeDeadlineOwnerTurn::Persisted(_) => Ok(true),
            EpisodeDeadlineOwnerTurn::NoDeadlineAction
            | EpisodeDeadlineOwnerTurn::Acknowledged { .. } => Ok(false),
            EpisodeDeadlineOwnerTurn::PendingReplay(_) => {
                Err(ScalpingResidentSourcesError::PendingPriority)
            }
        }
    }

    fn sync_episode(&mut self) -> Result<(), ScalpingResidentSourcesError> {
        let active = self
            .resident
            .host()
            .checkpoint()
            .strategy
            .episode
            .as_ref()
            .map(|episode| episode.episode_id.clone());
        if self.checkpoint.active_episode_id != active {
            self.checkpoint.active_episode_id = active;
            self.checkpoint.pending_mark = None;
            self.checkpoint.pending_preparation_id = None;
            self.checkpoint.pending_public_frame = None;
            self.episode_source = None;
        }
        self.ensure_episode_source()
    }

    fn ensure_episode_source(&mut self) -> Result<(), ScalpingResidentSourcesError> {
        if self.episode_source.is_some() {
            return Ok(());
        }
        let Some(episode_id) = self.checkpoint.active_episode_id.clone() else {
            return Ok(());
        };
        let path = self.episode_root.join(format!(
            "{}.json",
            episode_path_digest(&self.config.binding, &episode_id)?
        ));
        self.episode_source = Some(ScalpingEpisodeObservationSource::open_or_restore(
            path,
            EpisodeObservationSourceConfig {
                binding: self.config.binding.clone(),
                active_episode_id: episode_id,
                mark_stale_after_ms: self.config.mark_stale_after_ms,
            },
        )?);
        Ok(())
    }

    fn require_no_pending(&self) -> Result<(), ScalpingResidentSourcesError> {
        if self.deadline_owner.checkpoint().pending.is_some() {
            Err(ScalpingResidentSourcesError::PendingPriority)
        } else {
            Ok(())
        }
    }

    fn require_current_private(&self) -> Result<(), ScalpingResidentSourcesError> {
        let host = self.resident.host().checkpoint();
        let matches = self
            .checkpoint
            .latest_private
            .as_ref()
            .is_some_and(|private| {
                host.last_private_generation == Some(private.generation)
                    && host.last_private_observed_at_ms == Some(private.observed_at_ms)
                    && host.last_private_root_cause_fact_id.as_deref()
                        == Some(private.root_cause_fact_id.as_str())
            });
        if matches {
            Ok(())
        } else {
            Err(ScalpingResidentSourcesError::RiskBeforePrivate)
        }
    }

    fn current_public_delivery_authority(
        &self,
    ) -> Result<PendingPublicAuthority, ScalpingResidentSourcesError> {
        self.require_current_private()
            .map_err(|_| ScalpingResidentSourcesError::PendingDeliveryAuthority)?;
        let private = self
            .checkpoint
            .latest_private
            .as_ref()
            .ok_or(ScalpingResidentSourcesError::PendingDeliveryAuthority)?;
        Ok(PendingPublicAuthority {
            private_generation: private.generation,
            private_observed_at_ms: private.observed_at_ms,
            private_root_cause_fact_id: private.root_cause_fact_id.clone(),
            applied_risk_receipt: self.applied_risk_owner.last_ack().cloned(),
        })
    }

    fn require_pending_public_authority(
        &self,
        pending: &PendingPublicFrame,
    ) -> Result<(), ScalpingResidentSourcesError> {
        if self.current_public_delivery_authority()? == pending.authority {
            Ok(())
        } else {
            Err(ScalpingResidentSourcesError::PendingDeliveryAuthority)
        }
    }

    fn persist(&mut self) -> Result<(), ScalpingResidentSourcesError> {
        let sealed = seal_checkpoint(self.checkpoint.clone())?;
        self.store.save(&sealed)?;
        self.checkpoint = sealed;
        Ok(())
    }
}

fn validate_config(
    config: &ScalpingResidentSourcesConfig,
) -> Result<(), ScalpingResidentSourcesError> {
    config
        .binding
        .validate()
        .map_err(|_| ScalpingResidentSourcesError::Config)?;
    config
        .params
        .validate_for(&config.binding)
        .map_err(|_| ScalpingResidentSourcesError::Config)?;
    if !config.artifacts_root.is_absolute() || config.mark_stale_after_ms == 0 {
        return Err(ScalpingResidentSourcesError::Config);
    }
    Ok(())
}

fn validate_checkpoint(
    checkpoint: &ScalpingResidentSourcesCheckpoint,
    binding_digest: &str,
    binding: &StrategyBinding,
) -> Result<(), ScalpingResidentSourcesError> {
    if checkpoint.schema_version != SCALPING_RESIDENT_SOURCES_SCHEMA_VERSION
        || checkpoint.binding_digest != binding_digest
        || checkpoint.state_digest != seal_checkpoint(checkpoint.clone())?.state_digest
        || checkpoint
            .active_episode_id
            .as_ref()
            .is_some_and(|id| id.trim().is_empty())
        || checkpoint
            .pending_preparation_id
            .as_ref()
            .is_some_and(|id| id.trim().is_empty())
        || checkpoint.latest_private.as_ref().is_some_and(|private| {
            private.generation == 0
                || private.observed_at_ms == 0
                || private.root_cause_fact_id.trim().is_empty()
        })
        || checkpoint.pending_mark.as_ref().is_some_and(|mark| {
            mark.symbol != binding.symbol
                || mark.generation == 0
                || mark.received_at_ms == 0
                || mark.exchange_time_ms == 0
        })
        || checkpoint
            .pending_public_frame
            .as_ref()
            .is_some_and(|pending| {
                pending.frame.symbol != binding.symbol
                    || pending.frame.generation == 0
                    || pending.frame.watermark_ms == 0
                    || pending.decision_at_ms < pending.frame.watermark_ms
                    || pending.authority.private_generation == 0
                    || pending.authority.private_observed_at_ms == 0
                    || pending
                        .authority
                        .private_root_cause_fact_id
                        .trim()
                        .is_empty()
                    || pending
                        .authority
                        .applied_risk_receipt
                        .as_ref()
                        .is_some_and(|receipt| {
                            receipt.binding != *binding
                                || receipt.proof_id.trim().is_empty()
                                || receipt.risk_revaluation_digest.len() != 64
                                || receipt.target_generation == 0
                                || receipt.valuation_generation == 0
                        })
                    || (pending.phase == PendingPublicFramePhase::Assemble
                        && !pending.evidence.is_empty())
            })
    {
        return Err(ScalpingResidentSourcesError::Checkpoint);
    }
    Ok(())
}

fn seal_checkpoint(
    mut checkpoint: ScalpingResidentSourcesCheckpoint,
) -> Result<ScalpingResidentSourcesCheckpoint, ScalpingResidentSourcesError> {
    checkpoint.state_digest.clear();
    checkpoint.state_digest = digest(&checkpoint)?;
    Ok(checkpoint)
}

fn episode_path_digest(
    binding: &StrategyBinding,
    episode_id: &str,
) -> Result<String, ScalpingResidentSourcesError> {
    digest(&(binding.digest(), episode_id))
}

fn digest(value: &impl Serialize) -> Result<String, ScalpingResidentSourcesError> {
    let encoded = serde_json::to_vec(value).map_err(ScalpingResidentSourcesError::Encode)?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

fn pending_host_preparation(
    checkpoint: &super::ScalpingCoordinatorCheckpoint,
) -> Option<CandidatePreparation> {
    match &checkpoint.strategy.state {
        ScalpingState::CandidatePending(preparation) => Some((**preparation).clone()),
        _ => None,
    }
}

fn is_retryable_candidate_error(error: &ScalpingCandidateEvidenceError) -> bool {
    match error {
        ScalpingCandidateEvidenceError::Journal(ScalpingEvidenceError::Io { .. }) => true,
        ScalpingCandidateEvidenceError::Source(error) => {
            ScalpingEvidenceSource::is_retryable_error(error)
        }
        ScalpingCandidateEvidenceError::Assembler(
            ScalpingMarketEvidenceAssemblerError::Evidence(error),
        ) => ScalpingEvidenceSource::is_retryable_error(error),
        _ => false,
    }
}

fn recoverable_public_fault(error: &PublicCaptureWorkerError) -> bool {
    matches!(
        error,
        PublicCaptureWorkerError::Session(
            SessionError::Normalize(_)
                | SessionError::Book(_)
                | SessionError::Transport(_)
                | SessionError::UnexpectedEvent
                | SessionError::BufferFull
                | SessionError::Stale
                | SessionError::Backoff
                | SessionError::Generation
        ) | PublicCaptureWorkerError::Source(
            PublicMarketSourceError::Sequence
                | PublicMarketSourceError::Generation
                | PublicMarketSourceError::DataGap
        )
    )
}

fn merge_resident_report(
    current: &mut Option<ScalpingResidentCycleReport>,
    timer: ScalpingResidentCycleReport,
) {
    if let Some(current) = current {
        current.deadline = timer.deadline;
    } else {
        *current = Some(timer);
    }
}

fn merge_turn_report(
    current: &mut ScalpingResidentSourcesTurnReport,
    later: ScalpingResidentSourcesTurnReport,
) {
    if let Some(later_resident) = later.resident {
        if let Some(current_resident) = current.resident.as_mut() {
            if later_resident.controller.is_some() {
                current_resident.controller = later_resident.controller;
            }
            if later_resident.controller_control.is_some() {
                current_resident.controller_control = later_resident.controller_control;
            }
            if later_resident.private_gate.is_some() {
                current_resident.private_gate = later_resident.private_gate;
            }
            if later_resident.risk.is_some() {
                current_resident.risk = later_resident.risk;
            }
            if later_resident.episode_deadline_completion.is_some() {
                current_resident.episode_deadline_completion =
                    later_resident.episode_deadline_completion;
            }
            if later_resident.episode.is_some() {
                current_resident.episode = later_resident.episode;
            }
            if later_resident.deadline.is_some() {
                current_resident.deadline = later_resident.deadline;
            }
            if later_resident.market.is_some() {
                current_resident.market = later_resident.market;
            }
        } else {
            current.resident = Some(later_resident);
        }
    }
    current.public_effect = later.public_effect.or(current.public_effect.take());
    current.public_output = later.public_output.or(current.public_output.take());
    current.public_fault_backoff |= later.public_fault_backoff;
    current.episode_observation_applied |= later.episode_observation_applied;
    current.deadline_persisted |= later.deadline_persisted;
    current.market_evidence_count = current
        .market_evidence_count
        .saturating_add(later.market_evidence_count);
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingResidentSourcesError {
    #[error("resident sources configuration is invalid")]
    Config,
    #[error("resident sources checkpoint is invalid or tampered")]
    Checkpoint,
    #[error("resident sources must drain durable deadline pending before other work")]
    PendingPriority,
    #[error("resident sources are awaiting a strictly newer private generation")]
    AwaitingPrivate,
    #[error("deadline owner did not acknowledge the exact applied completion")]
    DeadlineAck,
    #[error("active episode source is unavailable")]
    EpisodeSource,
    #[error("risk proof cannot run before the current private fact is durably reconciled")]
    RiskBeforePrivate,
    #[error("pending public delivery authority no longer matches the current private/risk state")]
    PendingDeliveryAuthority,
    #[error("recovery-only applied-risk receipt requested for a proof absent from the host")]
    RiskHostNotApplied,
    #[error("host applied risk but the durable receipt owner did not publish the exact receipt")]
    RiskReceipt,
    #[error("candidate evidence coordinator is already attached")]
    CandidateEvidenceAttached,
    #[error("candidate evidence coordinator recovered in a durable fenced state")]
    CandidateEvidenceFenced,
    #[error("candidate evidence coordinator binding differs from resident sources")]
    CandidateEvidenceBinding,
    #[error("resident sources filesystem failed for {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("resident sources identity encoding failed: {0}")]
    Encode(serde_json::Error),
    #[error("resident sources storage failed: {0}")]
    Storage(#[from] StorageError),
    #[error("resident public capture failed: {0}")]
    Public(#[from] PublicCaptureWorkerError),
    #[error("resident public feature source failed: {0}")]
    PublicSource(#[from] PublicMarketSourceError),
    #[error("resident episode observation source failed: {0}")]
    Observation(#[from] ScalpingEpisodeObservationSourceError),
    #[error("resident deadline owner failed: {0}")]
    DeadlineOwner(#[from] EpisodeDeadlineOwnerError),
    #[error("resident applied-risk receipt owner failed: {0}")]
    AppliedRiskOwner(#[from] ScalpingAppliedRiskOwnerError),
    #[error("resident candidate evidence coordinator failed: {0}")]
    CandidateEvidence(#[from] ScalpingCandidateEvidenceError),
    #[error("resident host orchestration failed: {0}")]
    Runtime(#[from] ScalpingResidentRuntimeError),
}

#[cfg(test)]
#[path = "scalping_resident_sources/tests.rs"]
mod tests;
