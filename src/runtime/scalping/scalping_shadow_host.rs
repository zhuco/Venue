use std::path::Path;

use crate::{
    controller::{ControlTarget, EntryAuthorization},
    indicator::FeatureFrame,
    runtime::{
        BoundRiskRevaluation, EpisodeDeadlineCompletion, EpisodeObservation,
        PrivateEntryGateReport, SCALPING_COORDINATOR_SCHEMA_VERSION, ScalpingCoordinatorCheckpoint,
        ScalpingCoordinatorError, ScalpingCoordinatorOutput, ScalpingInput,
        ScalpingShadowCoordinator, ShadowDisposition,
    },
    storage::{ProjectionStore, StorageError},
    strategy::scalping::{
        CandidateEvidence, DeadlineFired, RiskUnit, ScalpingParams, ScalpingStrategy,
        StrategyBinding,
    },
};

/// A clock observation that was produced by an authoritative private-facts path. It is only a
/// semantic input: the host cannot read an exchange clock or create a protection command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadlineTick {
    pub now_ms: u64,
    pub root_cause_fact_id: String,
}

/// Persisted state is committed before this report is returned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingShadowHostReport {
    pub disposition: ShadowDisposition,
    pub deadline_fired: bool,
    pub checkpoint: ScalpingCoordinatorCheckpoint,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecoveryFence {
    Ready,
    AwaitingPrivate { prior_generation: Option<u64> },
}

/// Minimal durable host for a Shadow scalping instance. It owns no exchange client, execution
/// writer, pending mutation, or private-facts source; callers provide entry-gate reports and
/// authoritative clock observations.
#[derive(Debug)]
pub struct ScalpingShadowHost {
    store: ProjectionStore,
    binding: StrategyBinding,
    risk_unit: RiskUnit,
    coordinator: ScalpingShadowCoordinator,
    fence: RecoveryFence,
    deadline_recovery_pending: bool,
    entry_armed: bool,
    poisoned: bool,
}

impl ScalpingShadowHost {
    pub fn open_or_restore(
        path: impl AsRef<Path>,
        binding: StrategyBinding,
        params: ScalpingParams,
    ) -> Result<Self, ScalpingShadowHostError> {
        let store = ProjectionStore::new(path.as_ref().to_path_buf());
        let risk_unit = params.risk_per_episode.unit.clone();
        let host_binding = binding.clone();
        match store.load::<ScalpingCoordinatorCheckpoint>()? {
            Some(checkpoint) => {
                if checkpoint.schema_version != SCALPING_COORDINATOR_SCHEMA_VERSION {
                    return Err(ScalpingShadowHostError::Checkpoint);
                }
                let prior_generation = checkpoint.last_private_generation;
                let deadline_recovery_pending = has_active_deadline(&checkpoint);
                let coordinator = ScalpingShadowCoordinator::restore(binding, params, checkpoint)?;
                Ok(Self {
                    store,
                    binding: host_binding,
                    risk_unit,
                    coordinator,
                    fence: RecoveryFence::AwaitingPrivate { prior_generation },
                    deadline_recovery_pending,
                    entry_armed: false,
                    poisoned: false,
                })
            }
            None => {
                let strategy = ScalpingStrategy::new(binding, params)?;
                Ok(Self {
                    store,
                    binding: host_binding,
                    risk_unit,
                    coordinator: ScalpingShadowCoordinator::new(strategy),
                    fence: RecoveryFence::Ready,
                    deadline_recovery_pending: false,
                    entry_armed: false,
                    poisoned: false,
                })
            }
        }
    }

    /// Consumes only the entry gate's coordinator inputs. A restored host does not clear its
    /// fence unless the report forwards a strictly newer authoritative private generation.
    pub fn on_private_gate(
        &mut self,
        report: &PrivateEntryGateReport,
    ) -> Result<ScalpingShadowHostReport, ScalpingShadowHostError> {
        self.ensure_healthy()?;
        self.entry_armed = false;
        let forwarded_private = report.forwarded_private.as_ref();
        if let RecoveryFence::AwaitingPrivate { prior_generation } = self.fence {
            match forwarded_private {
                Some(facts) => {
                    if prior_generation.is_some_and(|prior| facts.generation <= prior) {
                        return Err(ScalpingShadowHostError::RecoveryGeneration);
                    }
                }
                None if report.control.is_some() => {}
                None => return Err(ScalpingShadowHostError::RecoveryGeneration),
            }
        }
        let inputs = report.coordinator_inputs();
        let mut disposition = self.coordinator.current_disposition();
        let mut checkpoint = self.coordinator.checkpoint();
        if inputs.is_empty() {
            self.persist(&checkpoint)?;
        }
        for input in inputs {
            let mut outputs = match self.coordinator.process(vec![input]) {
                Ok(outputs) => outputs,
                Err(error) => return self.poison_coordinator(error),
            };
            let output = outputs
                .pop()
                .ok_or(ScalpingShadowHostError::CoordinatorOutput)?;
            disposition = output.disposition;
            checkpoint = output.checkpoint;
            // Gate reports are safety ordered. Persist each control/private transition before
            // attempting the next one so a later stale private fact cannot erase a durable stop.
            self.persist(&checkpoint)?;
        }
        if forwarded_private.is_some() {
            self.fence = RecoveryFence::Ready;
        }
        self.entry_armed = report.control.is_none()
            && report.entry_ready
            && self.fence == RecoveryFence::Ready
            && disposition == ShadowDisposition::ShadowOnly;
        Ok(ScalpingShadowHostReport {
            disposition,
            deadline_fired: false,
            checkpoint,
        })
    }

    /// Applies a target already validated by the resident controller source. This crate-private
    /// entrypoint prevents callers from presenting Running without its paired authorization.
    pub(crate) fn on_controller_control(
        &mut self,
        target: ControlTarget,
    ) -> Result<ScalpingShadowHostReport, ScalpingShadowHostError> {
        self.ensure_healthy()?;
        if target != ControlTarget::Running {
            self.entry_armed = false;
        }
        let mut outputs = match self
            .coordinator
            .process(vec![ScalpingInput::Control(target)])
        {
            Ok(outputs) => outputs,
            Err(error) => return self.poison_coordinator(error),
        };
        let output = outputs
            .pop()
            .ok_or(ScalpingShadowHostError::CoordinatorOutput)?;
        self.persist(&output.checkpoint)?;
        Ok(ScalpingShadowHostReport {
            disposition: output.disposition,
            deadline_fired: false,
            checkpoint: output.checkpoint,
        })
    }

    /// Projects one exact private-generation-bound observation for an existing episode. This path
    /// is intentionally independent of entry authorization and persists before actions escape.
    pub fn on_episode_observation(
        &mut self,
        observation: EpisodeObservation,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingShadowHostError> {
        self.ensure_healthy()?;
        if self.fence != RecoveryFence::Ready {
            return Err(ScalpingShadowHostError::RecoveryGeneration);
        }
        let output = match self.coordinator.project_episode(observation) {
            Ok(output) => output,
            Err(error) => return self.poison_coordinator(error),
        };
        self.persist(&output.checkpoint)?;
        self.deadline_recovery_pending = has_active_deadline(&output.checkpoint);
        Ok(output)
    }

    /// Commits an exact external deadline arm/cancel completion. The host never allocates a
    /// deadline identity or infers completion from elapsed time.
    pub fn on_episode_deadline_completion(
        &mut self,
        completion: EpisodeDeadlineCompletion,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingShadowHostError> {
        self.ensure_healthy()?;
        if self.fence != RecoveryFence::Ready {
            return Err(ScalpingShadowHostError::RecoveryGeneration);
        }
        let output = match self.coordinator.complete_episode_deadline(completion) {
            Ok(output) => output,
            Err(error) => return self.poison_coordinator(error),
        };
        self.persist(&output.checkpoint)?;
        self.deadline_recovery_pending = has_active_deadline(&output.checkpoint);
        Ok(output)
    }

    /// Applies a completion that became durable in the deadline owner immediately before this
    /// host crashed. The recovery fence and disarmed entry state remain unchanged after success.
    pub fn recover_pending_episode_deadline_completion(
        &mut self,
        completion: EpisodeDeadlineCompletion,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingShadowHostError> {
        self.ensure_healthy()?;
        if !matches!(self.fence, RecoveryFence::AwaitingPrivate { .. }) {
            self.poisoned = true;
            return Err(ScalpingShadowHostError::DeadlineRecovery);
        }
        self.entry_armed = false;
        let output = match self
            .coordinator
            .recover_pending_episode_deadline(completion)
        {
            Ok(output) => output,
            Err(error) => return self.poison_coordinator(error),
        };
        self.persist(&output.checkpoint)?;
        self.deadline_recovery_pending = has_active_deadline(&output.checkpoint);
        Ok(output)
    }

    /// Advances a Shadow decision only while the most recent gate report has armed entry. This
    /// remains a semantic path: it delegates to the coordinator and exposes no writer or permit.
    pub fn on_market(
        &mut self,
        frame: FeatureFrame,
        decision_at_ms: u64,
        authorization: EntryAuthorization,
        evidence: Vec<CandidateEvidence>,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingShadowHostError> {
        self.on_market_with_mode(frame, decision_at_ms, authorization, evidence, false)
    }

    /// A distinct API prevents an empty legacy evidence vector from silently becoming direct
    /// admission in replay and calibration paths.
    pub fn on_direct_market(
        &mut self,
        frame: FeatureFrame,
        decision_at_ms: u64,
        authorization: EntryAuthorization,
        evidence: Vec<CandidateEvidence>,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingShadowHostError> {
        self.on_market_with_mode(frame, decision_at_ms, authorization, evidence, true)
    }

    /// Persists the semantic confirmation emitted only after the external gateway has completed
    /// its protected-entry chain. This host still has no execution client or writer.
    pub fn confirm_live_entry(
        &mut self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingShadowHostError> {
        self.ensure_healthy()?;
        let output = match self
            .coordinator
            .confirm_live_entry(intent_id, observed_at_ms)
        {
            Ok(output) => output,
            Err(error) => return self.poison_coordinator(error),
        };
        self.persist(&output.checkpoint)?;
        Ok(output)
    }

    /// Persists the semantic rejection emitted only after a no-fill was reconciled to a newer
    /// durable flat private generation and the scoped writer was retired.
    pub fn reject_live_entry(
        &mut self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingShadowHostError> {
        self.ensure_healthy()?;
        let output = match self
            .coordinator
            .reject_live_entry(intent_id, observed_at_ms)
        {
            Ok(output) => output,
            Err(error) => return self.poison_coordinator(error),
        };
        self.persist(&output.checkpoint)?;
        Ok(output)
    }

    fn on_market_with_mode(
        &mut self,
        frame: FeatureFrame,
        decision_at_ms: u64,
        authorization: EntryAuthorization,
        evidence: Vec<CandidateEvidence>,
        direct_admission: bool,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingShadowHostError> {
        self.ensure_healthy()?;
        if self.fence != RecoveryFence::Ready || self.deadline_recovery_pending || !self.entry_armed
        {
            let checkpoint = self.coordinator.checkpoint();
            self.persist(&checkpoint)?;
            return Ok(ScalpingCoordinatorOutput {
                disposition: ShadowDisposition::RemainFenced,
                requested_control: None,
                decision: None,
                preparation: None,
                episode_actions: Vec::new(),
                checkpoint,
            });
        }
        let input = if direct_admission {
            ScalpingInput::DirectMarket {
                frame: Box::new(frame),
                decision_at_ms,
                authorization,
                evidence,
            }
        } else {
            ScalpingInput::Market {
                frame: Box::new(frame),
                decision_at_ms,
                authorization,
                evidence,
            }
        };
        let mut outputs = match self.coordinator.process(vec![input]) {
            Ok(outputs) => outputs,
            Err(error) => return self.poison_coordinator(error),
        };
        let output = outputs
            .pop()
            .ok_or(ScalpingShadowHostError::CoordinatorOutput)?;
        self.persist(&output.checkpoint)?;
        Ok(output)
    }

    /// Applies one producer-bound logical-risk replay. Binding and watermark checks happen before
    /// the coordinator changes strategy state; any coordinator failure poisons this host.
    pub fn on_bound_risk_revaluation(
        &mut self,
        bound: BoundRiskRevaluation,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingShadowHostError> {
        self.ensure_healthy()?;
        self.entry_armed = false;
        self.validate_bound_risk(&bound)?;
        let checkpoint = self.coordinator.checkpoint();
        let exact_duplicate = checkpoint.last_risk_cursor_sequence == Some(bound.cursor_sequence)
            && checkpoint.last_risk_proof_id.as_deref() == Some(bound.proof.proof_id.as_str());
        if !exact_duplicate {
            let observed_at_ms = checkpoint
                .strategy
                .risk
                .last_event_time_ms
                .unwrap_or_default()
                .max(bound.proof.complete_through_ms);
            let fence = match self
                .coordinator
                .begin_bound_risk_revaluation(observed_at_ms)
            {
                Ok(output) => output,
                Err(error) => return self.poison_coordinator(error),
            };
            // Legacy runtime persisted the closed gate before attempting a proof. If proof
            // validation, application, or the process itself fails after this point, restart
            // must recover GenerationMismatch rather than the prior Open risk snapshot.
            self.persist(&fence.checkpoint)?;
        }
        let output = match self.coordinator.apply_bound_risk(&bound) {
            Ok(output) => output,
            Err(error) => {
                let fenced = self.coordinator.enforce_current_risk_gate();
                self.persist(&fenced.checkpoint)?;
                return self.poison_coordinator(error);
            }
        };
        self.persist(&output.checkpoint)?;
        Ok(output)
    }

    /// Delivers the first persisted deadline that is overdue at this authoritative clock point.
    /// A fresh private generation is mandatory after recovery; a host clock never manufactures a
    /// root fact identity.
    pub fn tick(
        &mut self,
        tick: DeadlineTick,
    ) -> Result<ScalpingShadowHostReport, ScalpingShadowHostError> {
        self.ensure_healthy()?;
        if tick.now_ms == 0 || tick.root_cause_fact_id.trim().is_empty() {
            return Err(ScalpingShadowHostError::Tick);
        }
        if self.fence != RecoveryFence::Ready {
            return Err(ScalpingShadowHostError::RecoveryGeneration);
        }
        let checkpoint = self.coordinator.checkpoint();
        if checkpoint
            .last_private_observed_at_ms
            .is_some_and(|observed_at_ms| tick.now_ms < observed_at_ms)
        {
            return Err(ScalpingShadowHostError::ClockRegression);
        }
        if checkpoint.last_private_root_cause_fact_id.as_deref()
            != Some(tick.root_cause_fact_id.as_str())
        {
            return Err(ScalpingShadowHostError::Tick);
        }
        if checkpoint
            .strategy
            .episode
            .as_ref()
            .is_some_and(|episode| episode.fault.is_some())
        {
            self.persist(&checkpoint)?;
            self.deadline_recovery_pending = false;
            self.entry_armed = false;
            return Ok(ScalpingShadowHostReport {
                disposition: ShadowDisposition::StopAndProtect,
                deadline_fired: false,
                checkpoint,
            });
        }
        let Some((deadline_id, generation)) = overdue_deadline(&checkpoint, tick.now_ms) else {
            self.persist(&checkpoint)?;
            self.deadline_recovery_pending = false;
            return Ok(ScalpingShadowHostReport {
                disposition: self.coordinator.current_disposition(),
                deadline_fired: false,
                checkpoint,
            });
        };
        let output = match self.coordinator.apply_fault_deadline(&DeadlineFired {
            deadline_id,
            generation,
            fired_at_ms: tick.now_ms,
            root_cause_fact_id: tick.root_cause_fact_id,
        }) {
            Ok(output) => output,
            Err(error) => return self.poison_coordinator(error),
        };
        self.persist(&output.checkpoint)?;
        self.deadline_recovery_pending = false;
        self.entry_armed = false;
        Ok(ScalpingShadowHostReport {
            disposition: output.disposition,
            deadline_fired: true,
            checkpoint: output.checkpoint,
        })
    }

    #[must_use]
    pub fn checkpoint(&self) -> ScalpingCoordinatorCheckpoint {
        self.coordinator.checkpoint()
    }

    #[must_use]
    pub const fn awaiting_private_recovery(&self) -> bool {
        matches!(self.fence, RecoveryFence::AwaitingPrivate { .. })
    }

    fn ensure_healthy(&self) -> Result<(), ScalpingShadowHostError> {
        if self.poisoned {
            Err(ScalpingShadowHostError::Poisoned)
        } else {
            Ok(())
        }
    }

    fn persist(
        &mut self,
        checkpoint: &ScalpingCoordinatorCheckpoint,
    ) -> Result<(), ScalpingShadowHostError> {
        if let Err(error) = self.store.save(checkpoint) {
            self.poisoned = true;
            return Err(error.into());
        }
        Ok(())
    }

    fn poison_coordinator<T>(
        &mut self,
        error: ScalpingCoordinatorError,
    ) -> Result<T, ScalpingShadowHostError> {
        self.poisoned = true;
        Err(error.into())
    }

    fn validate_bound_risk(
        &self,
        bound: &BoundRiskRevaluation,
    ) -> Result<(), ScalpingShadowHostError> {
        let risk_binding = &bound.binding;
        let proof = &bound.proof;
        if risk_binding.exchange != self.binding.exchange
            || risk_binding.account != self.binding.account
            || risk_binding.strategy_instance_id != self.binding.strategy_instance_id
            || risk_binding.run_id != self.binding.run_id
            || risk_binding.symbol != self.binding.symbol
            || risk_binding.owner_scope != self.binding.owner_scope
            || risk_binding.parameter_release_id != self.binding.parameter_release_id
            || risk_binding.risk_unit != self.risk_unit
        {
            return Err(ScalpingShadowHostError::RiskBinding);
        }
        if bound.cursor_sequence == 0
            || proof.proof_id.trim().is_empty()
            || proof.target_generation == 0
            || proof.target_generation != risk_binding.valuation_generation
            || proof.risk_unit != self.risk_unit
            || proof.complete_through_ms == 0
            || proof.window_start_ms > proof.complete_through_ms
        {
            return Err(ScalpingShadowHostError::RiskProof);
        }
        let checkpoint = self.coordinator.checkpoint();
        if checkpoint
            .last_private_observed_at_ms
            .is_some_and(|observed_at_ms| proof.complete_through_ms < observed_at_ms)
            || checkpoint
                .strategy
                .last_watermark_ms
                .is_some_and(|watermark_ms| proof.complete_through_ms < watermark_ms)
        {
            return Err(ScalpingShadowHostError::RiskWatermark);
        }
        Ok(())
    }
}

fn has_active_deadline(checkpoint: &ScalpingCoordinatorCheckpoint) -> bool {
    checkpoint.strategy.episode.as_ref().is_some_and(|episode| {
        episode.fault.is_none()
            && (episode.episode_fault_deadline.is_some()
                || episode.control_fault_deadline.is_some())
    })
}

fn overdue_deadline(
    checkpoint: &ScalpingCoordinatorCheckpoint,
    now_ms: u64,
) -> Option<(String, u64)> {
    let episode = checkpoint.strategy.episode.as_ref()?;
    if episode.fault.is_some() {
        return None;
    }
    let episode_deadline = episode
        .episode_fault_deadline
        .as_ref()
        .map(|armed| &armed.deadline);
    let control_deadline = episode.control_fault_deadline.as_ref();
    [episode_deadline, control_deadline]
        .into_iter()
        .flatten()
        .filter(|deadline| deadline.expires_at_ms <= now_ms)
        .min_by_key(|deadline| (deadline.expires_at_ms, deadline.deadline_id.as_str()))
        .map(|deadline| (deadline.deadline_id.clone(), deadline.generation))
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingShadowHostError {
    #[error("scalping shadow host checkpoint is incompatible")]
    Checkpoint,
    #[error("restored scalping shadow host requires a newer private generation")]
    RecoveryGeneration,
    #[error("scalping shadow host is poisoned after an unsuccessful checkpoint save; reopen it")]
    Poisoned,
    #[error("pending episode deadline recovery is only valid before restored private recovery")]
    DeadlineRecovery,
    #[error("deadline tick lacks an authoritative time or root fact identity")]
    Tick,
    #[error("deadline tick regresses behind the persisted private observation watermark")]
    ClockRegression,
    #[error("bound risk proof does not match the host identity or logical risk unit")]
    RiskBinding,
    #[error("bound risk proof has an invalid cursor, generation, unit, or window")]
    RiskProof,
    #[error("bound risk proof predates the persisted private or market watermark")]
    RiskWatermark,
    #[error("scalping coordinator emitted no output")]
    CoordinatorOutput,
    #[error("scalping shadow host storage failed: {0}")]
    Storage(#[from] StorageError),
    #[error("scalping shadow host coordinator failed: {0}")]
    Coordinator(#[from] ScalpingCoordinatorError),
    #[error("scalping shadow host strategy failed: {0}")]
    Strategy(#[from] crate::strategy::scalping::ScalpingError),
}
