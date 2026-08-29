use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::scalping_risk_producer::BoundRiskRevaluation;
use crate::{
    controller::{ControlTarget, EntryAuthorization},
    domain::{Price, Symbol},
    indicator::FeatureFrame,
    strategy::scalping::{
        CandidateEvidence, CandidatePreparation, EpisodeAction, ProtectionState, RiskFact,
        RiskGate, SafetyProjection, ScalpingCheckpoint, ScalpingDecision, ScalpingError,
        ScalpingParams, ScalpingStrategy, StrategyBinding,
    },
};

pub const SCALPING_COORDINATOR_SCHEMA_VERSION: u16 = 7;

/// Exact durable acknowledgement that a public market payload reached the host. It contains no
/// venue capability or mutation authority; sources use it only to avoid re-delivering a frame
/// after a crash between host persistence and their local acknowledgement.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScalpingMarketDeliveryReceipt {
    pub frame_generation: u64,
    pub watermark_ms: u64,
    pub decision_at_ms: u64,
    pub payload_digest: String,
}

/// One anonymous observation of an already admitted episode. Its generation and watermark must
/// exactly match the private facts accepted immediately before it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EpisodeObservation {
    pub binding_digest: String,
    pub episode_id: String,
    pub generation: u64,
    pub observed_at_ms: u64,
    pub private_root_cause_fact_id: String,
    pub observation_fact_id: String,
    pub mark_symbol: Symbol,
    pub mark_generation: u64,
    pub mark_received_at_ms: u64,
    pub mark_exchange_time_ms: u64,
    pub mark_price: Price,
}

/// The external deadline owner supplies the durable identity created or cancelled for a
/// previously persisted semantic request. This value carries no timer, task, order, or gateway.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EpisodeDeadlineCompletion {
    pub episode_id: String,
    pub observation_generation: u64,
    pub observation_observed_at_ms: u64,
    pub private_root_cause_fact_id: String,
    pub observation_fact_id: String,
    pub completion_fact_id: String,
    pub completed_at_ms: u64,
    pub outcome: EpisodeDeadlineOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome")]
pub enum EpisodeDeadlineOutcome {
    Armed {
        kind: crate::strategy::scalping::EpisodeFaultKind,
        deadline: crate::strategy::scalping::SafetyDeadline,
    },
    Cancelled {
        deadline_id: String,
        deadline_generation: u64,
    },
}

/// Durable receipt proving which observation produced the semantic actions returned by the host.
/// Exact duplicate observations replay this receipt; a different fact at the same watermark is
/// rejected.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EpisodeProjectionReceipt {
    pub binding_digest: String,
    pub episode_id: String,
    pub generation: u64,
    pub observed_at_ms: u64,
    pub private_root_cause_fact_id: String,
    pub observation_fact_id: String,
    pub mark_symbol: Symbol,
    pub mark_generation: u64,
    pub mark_received_at_ms: u64,
    pub mark_exchange_time_ms: u64,
    pub mark_price: Price,
    pub actions: Vec<EpisodeAction>,
}

impl EpisodeProjectionReceipt {
    #[must_use]
    pub fn matches(&self, observation: &EpisodeObservation) -> bool {
        self.binding_digest == observation.binding_digest
            && self.episode_id == observation.episode_id
            && self.generation == observation.generation
            && self.observed_at_ms == observation.observed_at_ms
            && self.private_root_cause_fact_id == observation.private_root_cause_fact_id
            && self.observation_fact_id == observation.observation_fact_id
            && self.mark_symbol == observation.mark_symbol
            && self.mark_generation == observation.mark_generation
            && self.mark_received_at_ms == observation.mark_received_at_ms
            && self.mark_exchange_time_ms == observation.mark_exchange_time_ms
            && self.mark_price == observation.mark_price
    }
}

/// Private account facts are the only source allowed to clear the coordinator fence. Custody is
/// deliberately a small semantic status: this layer cannot invent an order, position, or ID.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PrivateFacts {
    pub generation: u64,
    pub observed_at_ms: u64,
    pub root_cause_fact_id: String,
    pub safety: SafetyProjection,
    pub custody: CustodyStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CustodyStatus {
    Complete,
    Incomplete,
    Unknown,
}

/// The input order is intentionally not trusted for priority. `process` always handles control,
/// then risk facts in their supplied order, then private facts, then market frames, so an old
/// frame cannot outrun account reconciliation or an active risk fence.
pub enum ScalpingInput {
    Control(ControlTarget),
    RequireRiskRevaluation {
        observed_at_ms: u64,
    },
    RiskFact(RiskFact),
    Private(PrivateFacts),
    Market {
        frame: Box<FeatureFrame>,
        decision_at_ms: u64,
        authorization: EntryAuthorization,
        evidence: Vec<CandidateEvidence>,
    },
    DirectMarket {
        frame: Box<FeatureFrame>,
        decision_at_ms: u64,
        authorization: EntryAuthorization,
        evidence: Vec<CandidateEvidence>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShadowDisposition {
    ShadowOnly,
    StopAndProtect,
    RemainFenced,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScalpingCoordinatorCheckpoint {
    pub schema_version: u16,
    pub strategy: ScalpingCheckpoint,
    pub last_private_generation: Option<u64>,
    pub last_private_observed_at_ms: Option<u64>,
    pub last_private_root_cause_fact_id: Option<String>,
    #[serde(default)]
    pub last_risk_cursor_sequence: Option<u64>,
    #[serde(default)]
    pub last_risk_proof_id: Option<String>,
    #[serde(default)]
    pub risk_control_target: Option<ControlTarget>,
    #[serde(default = "running_control_target")]
    pub control_target: ControlTarget,
    #[serde(default)]
    pub last_episode_projection: Option<EpisodeProjectionReceipt>,
    #[serde(default)]
    pub last_episode_deadline_completion: Option<EpisodeDeadlineCompletion>,
    #[serde(default)]
    pub last_market_delivery: Option<ScalpingMarketDeliveryReceipt>,
    pub control_stopped: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingCoordinatorOutput {
    pub disposition: ShadowDisposition,
    pub requested_control: Option<ControlTarget>,
    pub decision: Option<ScalpingDecision>,
    pub preparation: Option<CandidatePreparation>,
    pub episode_actions: Vec<EpisodeAction>,
    pub checkpoint: ScalpingCoordinatorCheckpoint,
}

#[derive(Debug)]
pub struct ScalpingShadowCoordinator {
    strategy: ScalpingStrategy,
    private: Option<PrivateFacts>,
    last_private_generation: Option<u64>,
    last_private_observed_at_ms: Option<u64>,
    last_private_root_cause_fact_id: Option<String>,
    last_risk_cursor_sequence: Option<u64>,
    last_risk_proof_id: Option<String>,
    risk_control_target: Option<ControlTarget>,
    control_target: ControlTarget,
    last_episode_projection: Option<EpisodeProjectionReceipt>,
    last_episode_deadline_completion: Option<EpisodeDeadlineCompletion>,
    last_market_delivery: Option<ScalpingMarketDeliveryReceipt>,
    requires_new_private_generation: bool,
    control_stopped: bool,
}

impl ScalpingShadowCoordinator {
    pub fn new(strategy: ScalpingStrategy) -> Self {
        Self {
            strategy,
            private: None,
            last_private_generation: None,
            last_private_observed_at_ms: None,
            last_private_root_cause_fact_id: None,
            last_risk_cursor_sequence: None,
            last_risk_proof_id: None,
            risk_control_target: None,
            control_target: ControlTarget::Running,
            last_episode_projection: None,
            last_episode_deadline_completion: None,
            last_market_delivery: None,
            requires_new_private_generation: false,
            control_stopped: false,
        }
    }

    pub fn restore(
        binding: StrategyBinding,
        params: ScalpingParams,
        checkpoint: ScalpingCoordinatorCheckpoint,
    ) -> Result<Self, ScalpingCoordinatorError> {
        if checkpoint.schema_version != SCALPING_COORDINATOR_SCHEMA_VERSION {
            return Err(ScalpingCoordinatorError::Checkpoint);
        }
        if checkpoint
            .last_private_generation
            .is_some_and(|generation| generation == 0)
            || checkpoint
                .last_private_observed_at_ms
                .is_some_and(|observed_at_ms| observed_at_ms == 0)
            || checkpoint.last_private_generation.is_some()
                != checkpoint.last_private_observed_at_ms.is_some()
            || checkpoint.last_private_generation.is_some()
                != checkpoint.last_private_root_cause_fact_id.is_some()
            || checkpoint
                .last_private_root_cause_fact_id
                .as_ref()
                .is_some_and(|fact_id| fact_id.trim().is_empty())
            || checkpoint.last_risk_cursor_sequence.is_some()
                != checkpoint.last_risk_proof_id.is_some()
            || checkpoint
                .last_risk_cursor_sequence
                .is_some_and(|sequence| sequence == 0)
            || checkpoint
                .last_risk_proof_id
                .as_ref()
                .is_some_and(|proof_id| proof_id.trim().is_empty())
            || checkpoint.last_risk_proof_id != checkpoint.strategy.risk.last_revaluation_id
            || checkpoint
                .risk_control_target
                .is_some_and(|target| target == ControlTarget::Running)
            || (checkpoint.risk_control_target.is_some() && !checkpoint.control_stopped)
            || (checkpoint.strategy.episode.is_some()
                && checkpoint.last_private_generation.is_none())
            || checkpoint
                .last_episode_projection
                .as_ref()
                .is_some_and(|receipt| {
                    receipt.binding_digest.trim().is_empty()
                        || receipt.episode_id.trim().is_empty()
                        || receipt.generation == 0
                        || receipt.observed_at_ms == 0
                        || receipt.private_root_cause_fact_id.trim().is_empty()
                        || receipt.observation_fact_id.trim().is_empty()
                        || receipt.mark_generation == 0
                        || receipt.mark_received_at_ms == 0
                        || receipt.mark_exchange_time_ms == 0
                        || receipt.mark_exchange_time_ms > receipt.mark_received_at_ms
                        || receipt.mark_received_at_ms > receipt.observed_at_ms
                        || receipt.mark_symbol != binding.symbol
                        || checkpoint
                            .strategy
                            .episode
                            .as_ref()
                            .map(|episode| &episode.episode_id)
                            != Some(&receipt.episode_id)
                        || checkpoint
                            .last_private_generation
                            .is_none_or(|generation| receipt.generation > generation)
                        || checkpoint
                            .last_private_observed_at_ms
                            .is_none_or(|watermark| receipt.observed_at_ms > watermark)
                        || episode_observation_fact_id(&receipt_observation(receipt)).ok()
                            != Some(receipt.observation_fact_id.clone())
                        || (checkpoint.last_private_generation == Some(receipt.generation)
                            && checkpoint.last_private_observed_at_ms
                                == Some(receipt.observed_at_ms)
                            && checkpoint.last_private_root_cause_fact_id.as_deref()
                                != Some(receipt.private_root_cause_fact_id.as_str()))
                })
            || checkpoint
                .last_episode_deadline_completion
                .as_ref()
                .is_some_and(|completion| {
                    !deadline_completion_shape_valid(completion)
                        || checkpoint
                            .last_episode_projection
                            .as_ref()
                            .is_none_or(|receipt| {
                                receipt.episode_id != completion.episode_id
                                    || receipt.generation != completion.observation_generation
                                    || receipt.observed_at_ms
                                        != completion.observation_observed_at_ms
                                    || receipt.private_root_cause_fact_id
                                        != completion.private_root_cause_fact_id
                                    || receipt.observation_fact_id != completion.observation_fact_id
                            })
                        || checkpoint.last_private_generation
                            != Some(completion.observation_generation)
                        || checkpoint.last_private_observed_at_ms
                            != Some(completion.observation_observed_at_ms)
                        || checkpoint.last_private_root_cause_fact_id.as_deref()
                            != Some(completion.private_root_cause_fact_id.as_str())
                })
            || checkpoint
                .last_market_delivery
                .as_ref()
                .is_some_and(|receipt| !valid_market_delivery_receipt(receipt))
        {
            return Err(ScalpingCoordinatorError::Checkpoint);
        }
        let risk_observed_at_ms = checkpoint
            .strategy
            .risk
            .last_event_time_ms
            .unwrap_or_default();
        let risk_was_initialized = checkpoint.strategy.risk.generation_mismatch
            || checkpoint.strategy.risk.valuation_generation.is_some();
        let strategy = ScalpingStrategy::restore(binding, params, checkpoint.strategy)?;
        let requires_new_private_generation = strategy.episode().is_some();
        let mut risk_control_target = checkpoint.risk_control_target;
        if risk_control_target.is_none() && strategy.episode().is_some() && risk_was_initialized {
            risk_control_target =
                risk_target_for_gate(strategy.risk_snapshot(risk_observed_at_ms).gate);
        }
        let control_stopped = checkpoint.control_stopped || risk_control_target.is_some();
        Ok(Self {
            strategy,
            private: None,
            last_private_generation: checkpoint.last_private_generation,
            last_private_observed_at_ms: checkpoint.last_private_observed_at_ms,
            last_private_root_cause_fact_id: checkpoint.last_private_root_cause_fact_id,
            last_risk_cursor_sequence: checkpoint.last_risk_cursor_sequence,
            last_risk_proof_id: checkpoint.last_risk_proof_id,
            risk_control_target,
            control_target: checkpoint.control_target,
            last_episode_projection: checkpoint.last_episode_projection,
            last_episode_deadline_completion: checkpoint.last_episode_deadline_completion,
            last_market_delivery: checkpoint.last_market_delivery,
            requires_new_private_generation,
            control_stopped,
        })
    }

    #[must_use]
    pub fn checkpoint(&self) -> ScalpingCoordinatorCheckpoint {
        ScalpingCoordinatorCheckpoint {
            schema_version: SCALPING_COORDINATOR_SCHEMA_VERSION,
            strategy: self.strategy.checkpoint(),
            last_private_generation: self.last_private_generation,
            last_private_observed_at_ms: self.last_private_observed_at_ms,
            last_private_root_cause_fact_id: self.last_private_root_cause_fact_id.clone(),
            last_risk_cursor_sequence: self.last_risk_cursor_sequence,
            last_risk_proof_id: self.last_risk_proof_id.clone(),
            risk_control_target: self.risk_control_target,
            control_target: self.control_target,
            last_episode_projection: self.last_episode_projection.clone(),
            last_episode_deadline_completion: self.last_episode_deadline_completion.clone(),
            last_market_delivery: self.last_market_delivery.clone(),
            control_stopped: self.control_stopped,
        }
    }

    #[must_use]
    pub fn current_disposition(&self) -> ShadowDisposition {
        if self.risk_control_target.is_some()
            || self
                .strategy
                .episode()
                .is_some_and(|episode| episode.fault.is_some())
        {
            ShadowDisposition::StopAndProtect
        } else {
            self.private_disposition()
        }
    }

    /// Records a previously armed deadline firing as a semantic Shadow stop. The coordinator
    /// neither creates the firing fact nor performs the requested protection work.
    pub fn apply_fault_deadline(
        &mut self,
        fired: &crate::strategy::scalping::DeadlineFired,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingCoordinatorError> {
        let changed = self.strategy.apply_fault_deadline(fired)?;
        let faulted = self
            .strategy
            .episode()
            .is_some_and(|episode| episode.fault.is_some());
        if changed || faulted {
            self.control_stopped = true;
        }
        Ok(ScalpingCoordinatorOutput {
            disposition: self.current_disposition(),
            requested_control: self.requested_control(),
            decision: None,
            preparation: None,
            episode_actions: Vec::new(),
            checkpoint: self.checkpoint(),
        })
    }

    /// Projects one observation of an existing episode. Entry authorization is intentionally not
    /// consulted: an admitted episode must keep consuming safety and mark facts while entry is
    /// fenced. The returned actions are tied to a durable receipt in the checkpoint.
    pub fn project_episode(
        &mut self,
        observation: EpisodeObservation,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingCoordinatorError> {
        if observation.binding_digest.trim().is_empty()
            || observation.episode_id.trim().is_empty()
            || observation.generation == 0
            || observation.observed_at_ms == 0
            || observation.private_root_cause_fact_id.trim().is_empty()
            || observation.observation_fact_id.trim().is_empty()
            || observation.mark_generation == 0
            || observation.mark_received_at_ms == 0
            || observation.mark_exchange_time_ms == 0
            || observation.mark_exchange_time_ms > observation.mark_received_at_ms
            || observation.mark_received_at_ms > observation.observed_at_ms
            || observation.binding_digest != self.strategy.checkpoint().binding_digest
            || observation.mark_symbol != self.strategy.binding().symbol
            || episode_observation_fact_id(&observation).ok()
                != Some(observation.observation_fact_id.clone())
        {
            return Err(ScalpingCoordinatorError::EpisodeObservation);
        }
        let private = self
            .private
            .as_ref()
            .ok_or(ScalpingCoordinatorError::EpisodeObservation)?;
        if private.generation != observation.generation
            || private.observed_at_ms != observation.observed_at_ms
            || private.root_cause_fact_id != observation.private_root_cause_fact_id
            || self
                .strategy
                .episode()
                .is_none_or(|episode| episode.episode_id != observation.episode_id)
        {
            return Err(ScalpingCoordinatorError::EpisodeObservation);
        }
        if let Some(receipt) = &self.last_episode_projection {
            if receipt.matches(&observation) {
                return Ok(self.episode_output(receipt.actions.clone()));
            }
            if receipt.generation == observation.generation
                && receipt.observed_at_ms == observation.observed_at_ms
                && receipt.mark_generation == observation.mark_generation
                && receipt.mark_received_at_ms == observation.mark_received_at_ms
                && receipt.mark_exchange_time_ms == observation.mark_exchange_time_ms
            {
                return Err(ScalpingCoordinatorError::EpisodeObservation);
            }
        }
        let mut safety = private.safety.clone();
        safety.protection = match private.custody {
            CustodyStatus::Complete => safety.protection,
            CustodyStatus::Incomplete => ProtectionState::Gap,
            CustodyStatus::Unknown => ProtectionState::Unknown,
        };
        let target = self.risk_control_target.unwrap_or(self.control_target);
        self.last_episode_deadline_completion = None;
        let actions = self.strategy.project_episode(
            target,
            &safety,
            observation.mark_price,
            observation.observed_at_ms,
            &observation.observation_fact_id,
        )?;
        self.last_episode_projection = Some(EpisodeProjectionReceipt {
            binding_digest: observation.binding_digest,
            episode_id: observation.episode_id,
            generation: observation.generation,
            observed_at_ms: observation.observed_at_ms,
            private_root_cause_fact_id: observation.private_root_cause_fact_id,
            observation_fact_id: observation.observation_fact_id,
            mark_symbol: observation.mark_symbol,
            mark_generation: observation.mark_generation,
            mark_received_at_ms: observation.mark_received_at_ms,
            mark_exchange_time_ms: observation.mark_exchange_time_ms,
            mark_price: observation.mark_price,
            actions: actions.clone(),
        });
        Ok(self.episode_output(actions))
    }

    /// Applies only an external completion for the exact persisted deadline action. Stale,
    /// cross-generation, late-after-new-observation, or conflicting completions remain fenced.
    pub fn complete_episode_deadline(
        &mut self,
        completion: EpisodeDeadlineCompletion,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingCoordinatorError> {
        let private = self
            .private
            .as_ref()
            .ok_or(ScalpingCoordinatorError::DeadlineCompletion)?;
        let generation = private.generation;
        let observed_at_ms = private.observed_at_ms;
        let root_cause_fact_id = private.root_cause_fact_id.clone();
        self.complete_episode_deadline_for_identity(
            completion,
            generation,
            observed_at_ms,
            &root_cause_fact_id,
        )
    }

    /// Recovery-only completion path. It uses the exact private identity already sealed in the
    /// host checkpoint and never reconstructs account safety or clears the new-private fence.
    pub(crate) fn recover_pending_episode_deadline(
        &mut self,
        completion: EpisodeDeadlineCompletion,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingCoordinatorError> {
        if self.private.is_some() || !self.requires_new_private_generation {
            return Err(ScalpingCoordinatorError::DeadlineCompletion);
        }
        let generation = self
            .last_private_generation
            .ok_or(ScalpingCoordinatorError::DeadlineCompletion)?;
        let observed_at_ms = self
            .last_private_observed_at_ms
            .ok_or(ScalpingCoordinatorError::DeadlineCompletion)?;
        let root_cause_fact_id = self
            .last_private_root_cause_fact_id
            .clone()
            .ok_or(ScalpingCoordinatorError::DeadlineCompletion)?;
        self.complete_episode_deadline_for_identity(
            completion,
            generation,
            observed_at_ms,
            &root_cause_fact_id,
        )
    }

    fn complete_episode_deadline_for_identity(
        &mut self,
        completion: EpisodeDeadlineCompletion,
        private_generation: u64,
        private_observed_at_ms: u64,
        private_root_cause_fact_id: &str,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingCoordinatorError> {
        if !deadline_completion_shape_valid(&completion) {
            return Err(ScalpingCoordinatorError::DeadlineCompletion);
        }
        if let Some(previous) = &self.last_episode_deadline_completion {
            if previous == &completion {
                if !self.deadline_completion_is_applied(&completion) {
                    return Err(ScalpingCoordinatorError::DeadlineCompletion);
                }
                return Ok(self.episode_output(Vec::new()));
            }
            if previous.completion_fact_id == completion.completion_fact_id {
                return Err(ScalpingCoordinatorError::DeadlineCompletion);
            }
        }
        if private_generation != completion.observation_generation
            || private_observed_at_ms != completion.observation_observed_at_ms
            || private_root_cause_fact_id != completion.private_root_cause_fact_id
        {
            return Err(ScalpingCoordinatorError::DeadlineCompletion);
        }
        let receipt = self
            .last_episode_projection
            .as_mut()
            .ok_or(ScalpingCoordinatorError::DeadlineCompletion)?;
        if receipt.episode_id != completion.episode_id
            || receipt.generation != completion.observation_generation
            || receipt.observed_at_ms != completion.observation_observed_at_ms
            || receipt.private_root_cause_fact_id != completion.private_root_cause_fact_id
            || receipt.observation_fact_id != completion.observation_fact_id
        {
            return Err(ScalpingCoordinatorError::DeadlineCompletion);
        }
        match &completion.outcome {
            EpisodeDeadlineOutcome::Armed { kind, deadline } => {
                let no_later_than_ms = receipt
                    .actions
                    .iter()
                    .find_map(|action| match action {
                        EpisodeAction::ArmFaultDeadline {
                            kind: requested_kind,
                            no_later_than_ms,
                        } if requested_kind == kind => Some(no_later_than_ms),
                        _ => None,
                    })
                    .copied();
                if no_later_than_ms.is_none_or(|limit| deadline.expires_at_ms > limit)
                    || deadline.generation != completion.observation_generation
                    || deadline.armed_at_ms != completion.observation_observed_at_ms
                    || completion.completed_at_ms < deadline.armed_at_ms
                    || completion.completed_at_ms >= deadline.expires_at_ms
                {
                    return Err(ScalpingCoordinatorError::DeadlineCompletion);
                }
                self.strategy
                    .arm_episode_fault_deadline(*kind, deadline.clone())?;
                receipt.actions.retain(|action| {
                    !matches!(
                        action,
                        EpisodeAction::ArmFaultDeadline {
                            kind: requested_kind,
                            ..
                        } if requested_kind == kind
                    )
                });
            }
            EpisodeDeadlineOutcome::Cancelled {
                deadline_id,
                deadline_generation,
            } => {
                if !receipt.actions.iter().any(|action| {
                    matches!(
                        action,
                        EpisodeAction::CancelFaultDeadline {
                            deadline_id: requested,
                        } if requested == deadline_id
                    )
                }) {
                    return Err(ScalpingCoordinatorError::DeadlineCompletion);
                }
                let active = self
                    .strategy
                    .episode()
                    .and_then(|episode| episode.episode_fault_deadline.as_ref())
                    .ok_or(ScalpingCoordinatorError::DeadlineCompletion)?;
                if active.deadline.deadline_id != *deadline_id
                    || active.deadline.generation != *deadline_generation
                    || completion.completed_at_ms >= active.deadline.expires_at_ms
                {
                    return Err(ScalpingCoordinatorError::DeadlineCompletion);
                }
                self.strategy.cancel_episode_fault_deadline(
                    deadline_id,
                    *deadline_generation,
                    completion.completed_at_ms,
                )?;
                receipt.actions.retain(|action| {
                    !matches!(
                        action,
                        EpisodeAction::CancelFaultDeadline {
                            deadline_id: requested,
                        } if requested == deadline_id
                    )
                });
            }
        }
        self.last_episode_deadline_completion = Some(completion);
        Ok(self.episode_output(Vec::new()))
    }

    fn deadline_completion_is_applied(&self, completion: &EpisodeDeadlineCompletion) -> bool {
        let Some(receipt) = &self.last_episode_projection else {
            return false;
        };
        let Some(episode) = self.strategy.episode() else {
            return false;
        };
        match &completion.outcome {
            EpisodeDeadlineOutcome::Armed { kind, deadline } => {
                episode
                    .episode_fault_deadline
                    .as_ref()
                    .is_some_and(|armed| armed.kind == *kind && armed.deadline == *deadline)
                    && !receipt.actions.iter().any(|action| {
                        matches!(
                            action,
                            EpisodeAction::ArmFaultDeadline {
                                kind: requested_kind,
                                ..
                            } if requested_kind == kind
                        )
                    })
            }
            EpisodeDeadlineOutcome::Cancelled { deadline_id, .. } => {
                episode.episode_fault_deadline.is_none()
                    && !receipt.actions.iter().any(|action| {
                        matches!(
                            action,
                            EpisodeAction::CancelFaultDeadline {
                                deadline_id: requested,
                            } if requested == deadline_id
                        )
                    })
            }
        }
    }

    fn episode_output(&self, actions: Vec<EpisodeAction>) -> ScalpingCoordinatorOutput {
        ScalpingCoordinatorOutput {
            disposition: self.current_disposition(),
            requested_control: self.requested_control(),
            decision: None,
            preparation: None,
            episode_actions: actions,
            checkpoint: self.checkpoint(),
        }
    }

    /// Commits only the strategy's semantic transition from reserved to protected-open. The
    /// execution gateway and private readback remain external owners of physical facts.
    pub fn confirm_live_entry(
        &mut self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingCoordinatorError> {
        self.strategy
            .confirm_live_entry(intent_id, observed_at_ms)?;
        Ok(self.episode_output(Vec::new()))
    }

    /// Commits a durably reconciled IOC no-fill as a semantic rejection. Like confirmation,
    /// this changes only strategy state after the writer has already retired through readback.
    pub fn reject_live_entry(
        &mut self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingCoordinatorError> {
        self.strategy
            .reject_reserved_entry(intent_id, observed_at_ms)?;
        Ok(self.episode_output(Vec::new()))
    }

    /// Applies a producer-bound replay after the Shadow host has durably persisted the
    /// generation-mismatch fence. The binding itself is checked by the host before it can reach
    /// this coordinator.
    pub fn apply_bound_risk(
        &mut self,
        bound: &BoundRiskRevaluation,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingCoordinatorError> {
        if bound.cursor_sequence == 0 || bound.proof.proof_id.trim().is_empty() {
            return Err(ScalpingCoordinatorError::RiskCursor);
        }
        if let Some(last_sequence) = self.last_risk_cursor_sequence {
            if bound.cursor_sequence < last_sequence
                || (bound.cursor_sequence == last_sequence
                    && self.last_risk_proof_id.as_deref() != Some(bound.proof.proof_id.as_str()))
            {
                return Err(ScalpingCoordinatorError::RiskCursor);
            }
            if bound.cursor_sequence == last_sequence {
                return Ok(ScalpingCoordinatorOutput {
                    disposition: self.current_disposition(),
                    requested_control: self.requested_control(),
                    decision: None,
                    preparation: None,
                    episode_actions: Vec::new(),
                    checkpoint: self.checkpoint(),
                });
            }
        }
        let snapshot = self.strategy.apply_risk_revaluation(bound.proof.clone())?;
        self.enforce_risk_gate(snapshot.gate);
        self.last_risk_cursor_sequence = Some(bound.cursor_sequence);
        self.last_risk_proof_id = Some(bound.proof.proof_id.clone());
        Ok(ScalpingCoordinatorOutput {
            disposition: self.current_disposition(),
            requested_control: self.requested_control(),
            decision: None,
            preparation: None,
            episode_actions: Vec::new(),
            checkpoint: self.checkpoint(),
        })
    }

    /// Begins the legacy two-phase risk application without issuing a control action yet. The
    /// host persists this mismatch checkpoint before attempting the complete proof.
    pub fn begin_bound_risk_revaluation(
        &mut self,
        observed_at_ms: u64,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingCoordinatorError> {
        self.strategy.require_risk_revaluation(observed_at_ms)?;
        Ok(ScalpingCoordinatorOutput {
            disposition: self.current_disposition(),
            requested_control: self.requested_control(),
            decision: None,
            preparation: None,
            episode_actions: Vec::new(),
            checkpoint: self.checkpoint(),
        })
    }

    /// Converts the current risk gate into the legacy semantic control after proof failure or an
    /// independently observed risk change.
    pub fn enforce_current_risk_gate(&mut self) -> ScalpingCoordinatorOutput {
        let risk = self.strategy.checkpoint().risk;
        let snapshot = self
            .strategy
            .risk_snapshot(risk.last_event_time_ms.unwrap_or_default());
        self.enforce_risk_gate(snapshot.gate);
        ScalpingCoordinatorOutput {
            disposition: self.current_disposition(),
            requested_control: self.requested_control(),
            decision: None,
            preparation: None,
            episode_actions: Vec::new(),
            checkpoint: self.checkpoint(),
        }
    }

    /// Applies a batch in safety order and emits only semantic Shadow output. No branch creates a
    /// writer, client order ID, exchange request, or execution command.
    pub fn process(
        &mut self,
        mut inputs: Vec<ScalpingInput>,
    ) -> Result<Vec<ScalpingCoordinatorOutput>, ScalpingCoordinatorError> {
        inputs.sort_by_key(input_priority);
        let mut outputs = Vec::with_capacity(inputs.len());
        for input in inputs {
            outputs.push(self.process_one(input)?);
        }
        Ok(outputs)
    }

    fn process_one(
        &mut self,
        input: ScalpingInput,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingCoordinatorError> {
        let (disposition, decision, preparation) = match input {
            ScalpingInput::Control(target) => {
                self.control_target = target;
                if target == ControlTarget::Running
                    && self.risk_control_target.is_some()
                    && self.strategy.episode().is_none()
                {
                    let risk = self.strategy.checkpoint().risk;
                    if self
                        .strategy
                        .risk_snapshot(risk.last_event_time_ms.unwrap_or_default())
                        .gate
                        == RiskGate::Open
                    {
                        self.risk_control_target = None;
                    }
                }
                self.control_stopped =
                    !matches!(target, ControlTarget::Running) || self.risk_control_target.is_some();
                let disposition = if target == ControlTarget::Running {
                    if self.risk_control_target.is_some() {
                        ShadowDisposition::StopAndProtect
                    } else if self.private.as_ref().is_some_and(private_is_safe) {
                        ShadowDisposition::ShadowOnly
                    } else {
                        ShadowDisposition::RemainFenced
                    }
                } else {
                    self.private = None;
                    stop_disposition(target)
                };
                (disposition, None, None)
            }
            ScalpingInput::RequireRiskRevaluation { observed_at_ms } => {
                let snapshot = self.strategy.require_risk_revaluation(observed_at_ms)?;
                self.enforce_risk_gate(snapshot.gate);
                (self.current_disposition(), None, None)
            }
            ScalpingInput::RiskFact(fact) => {
                let snapshot = self.strategy.record_risk(fact)?;
                self.enforce_risk_gate(snapshot.gate);
                (self.current_disposition(), None, None)
            }
            ScalpingInput::Private(facts) => {
                self.accept_private(facts)?;
                (self.private_disposition(), None, None)
            }
            ScalpingInput::Market {
                frame,
                decision_at_ms,
                authorization,
                evidence,
            } => {
                let receipt = scalping_market_delivery_receipt(&frame, decision_at_ms, &evidence)?;
                let output =
                    self.evaluate_market(*frame, decision_at_ms, authorization, &evidence, false)?;
                self.last_market_delivery = Some(receipt);
                output
            }
            ScalpingInput::DirectMarket {
                frame,
                decision_at_ms,
                authorization,
                evidence,
            } => {
                let receipt = scalping_market_delivery_receipt(&frame, decision_at_ms, &evidence)?;
                let output =
                    self.evaluate_market(*frame, decision_at_ms, authorization, &evidence, true)?;
                self.last_market_delivery = Some(receipt);
                output
            }
        };
        Ok(ScalpingCoordinatorOutput {
            disposition,
            requested_control: self.requested_control(),
            preparation,
            decision,
            episode_actions: Vec::new(),
            checkpoint: self.checkpoint(),
        })
    }

    fn accept_private(&mut self, facts: PrivateFacts) -> Result<(), ScalpingCoordinatorError> {
        if facts.generation == 0
            || facts.observed_at_ms == 0
            || facts.root_cause_fact_id.trim().is_empty()
        {
            return Err(ScalpingCoordinatorError::PrivateFacts);
        }
        if self.requires_new_private_generation
            && self
                .last_private_generation
                .is_some_and(|previous| facts.generation <= previous)
        {
            return Err(ScalpingCoordinatorError::RecoveryGeneration);
        }
        if let Some(previous_generation) = self.last_private_generation
            && (facts.generation < previous_generation
                || (facts.generation == previous_generation
                    && facts.observed_at_ms <= self.last_private_observed_at_ms.unwrap_or(0)))
        {
            return Err(ScalpingCoordinatorError::PrivateGeneration);
        }
        self.last_private_generation = Some(facts.generation);
        self.last_private_observed_at_ms = Some(facts.observed_at_ms);
        self.last_private_root_cause_fact_id = Some(facts.root_cause_fact_id.clone());
        self.private = Some(facts);
        if self.requires_new_private_generation {
            self.requires_new_private_generation = false;
        }
        Ok(())
    }

    fn evaluate_market(
        &mut self,
        frame: FeatureFrame,
        decision_at_ms: u64,
        authorization: EntryAuthorization,
        evidence: &[CandidateEvidence],
        direct_admission: bool,
    ) -> Result<
        (
            ShadowDisposition,
            Option<ScalpingDecision>,
            Option<CandidatePreparation>,
        ),
        ScalpingCoordinatorError,
    > {
        if self.control_stopped {
            return Ok((ShadowDisposition::RemainFenced, None, None));
        }
        let Some(private) = self.private.as_ref() else {
            return Ok((ShadowDisposition::RemainFenced, None, None));
        };
        if self.requires_new_private_generation {
            return Err(ScalpingCoordinatorError::RecoveryGeneration);
        }
        if !private_is_safe(private) {
            return Ok((ShadowDisposition::StopAndProtect, None, None));
        }
        if authorization.authority_generation() != private.generation {
            return Err(ScalpingCoordinatorError::Generation);
        }

        if !evidence.is_empty() {
            if !authorization.is_allowed() {
                return Ok((ShadowDisposition::RemainFenced, None, None));
            }
            self.strategy
                .validate_admission_context(&frame, &authorization)?;
            let decision = self.strategy.admit(evidence, frame.watermark_ms)?;
            return Ok((ShadowDisposition::ShadowOnly, Some(decision), None));
        }

        let decision =
            self.strategy
                .evaluate_at(&frame, &private.safety, &authorization, decision_at_ms)?;
        let decision = match decision {
            ScalpingDecision::Prepared(_) if direct_admission => {
                self.strategy.admit_direct(frame.watermark_ms)?
            }
            decision => decision,
        };
        let preparation = match &decision {
            ScalpingDecision::Prepared(preparation) => Some((**preparation).clone()),
            ScalpingDecision::Intent(_) | ScalpingDecision::Noop(_) => None,
        };
        Ok((ShadowDisposition::ShadowOnly, Some(decision), preparation))
    }

    fn private_disposition(&self) -> ShadowDisposition {
        if self.risk_control_target.is_some() {
            ShadowDisposition::StopAndProtect
        } else if self.control_stopped {
            ShadowDisposition::RemainFenced
        } else if self.private.as_ref().is_some_and(private_is_safe) {
            ShadowDisposition::ShadowOnly
        } else {
            ShadowDisposition::StopAndProtect
        }
    }

    fn enforce_risk_gate(&mut self, gate: RiskGate) {
        if self.strategy.episode().is_none() {
            return;
        }
        let Some(target) = risk_target_for_gate(gate) else {
            return;
        };
        self.risk_control_target = Some(target);
        self.control_stopped = true;
    }

    fn requested_control(&self) -> Option<ControlTarget> {
        self.risk_control_target
    }
}

fn risk_target_for_gate(gate: RiskGate) -> Option<ControlTarget> {
    match gate {
        RiskGate::GenerationMismatch => Some(ControlTarget::StopAndProtect),
        RiskGate::LossWindow | RiskGate::Drawdown | RiskGate::LossStreak | RiskGate::Cooldown => {
            Some(ControlTarget::EmergencyStop)
        }
        RiskGate::Open => None,
    }
}

fn input_priority(input: &ScalpingInput) -> u8 {
    match input {
        ScalpingInput::Control(_) => 0,
        ScalpingInput::RequireRiskRevaluation { .. } | ScalpingInput::RiskFact(_) => 1,
        ScalpingInput::Private(_) => 2,
        ScalpingInput::Market { .. } | ScalpingInput::DirectMarket { .. } => 3,
    }
}

fn private_is_safe(facts: &PrivateFacts) -> bool {
    facts.safety.private_snapshot_ready
        && !facts.safety.execution_unknown
        && !facts.safety.owner_conflict
        && facts.safety.protection == ProtectionState::Complete
        && facts.custody == CustodyStatus::Complete
}

fn stop_disposition(target: ControlTarget) -> ShadowDisposition {
    match target {
        ControlTarget::Running => ShadowDisposition::RemainFenced,
        ControlTarget::StopAndProtect
        | ControlTarget::FlattenAndStop
        | ControlTarget::EmergencyStop => ShadowDisposition::StopAndProtect,
    }
}

/// Produces the exact host-delivery identity for a frame and its already assembled evidence.
pub fn scalping_market_delivery_receipt(
    frame: &FeatureFrame,
    decision_at_ms: u64,
    evidence: &[CandidateEvidence],
) -> Result<ScalpingMarketDeliveryReceipt, ScalpingCoordinatorError> {
    if frame.generation == 0 || frame.watermark_ms == 0 || decision_at_ms < frame.watermark_ms {
        return Err(ScalpingCoordinatorError::MarketDelivery);
    }
    let encoded = serde_json::to_vec(&(frame, decision_at_ms, evidence))
        .map_err(|_| ScalpingCoordinatorError::MarketDelivery)?;
    Ok(ScalpingMarketDeliveryReceipt {
        frame_generation: frame.generation,
        watermark_ms: frame.watermark_ms,
        decision_at_ms,
        payload_digest: format!("{:x}", Sha256::digest(encoded)),
    })
}

fn valid_market_delivery_receipt(receipt: &ScalpingMarketDeliveryReceipt) -> bool {
    receipt.frame_generation != 0
        && receipt.watermark_ms != 0
        && receipt.decision_at_ms >= receipt.watermark_ms
        && receipt.payload_digest.len() == 64
        && receipt
            .payload_digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
}

const fn running_control_target() -> ControlTarget {
    ControlTarget::Running
}

fn deadline_completion_shape_valid(completion: &EpisodeDeadlineCompletion) -> bool {
    if completion.episode_id.trim().is_empty()
        || completion.observation_generation == 0
        || completion.observation_observed_at_ms == 0
        || completion.private_root_cause_fact_id.trim().is_empty()
        || completion.observation_fact_id.trim().is_empty()
        || completion.completion_fact_id.trim().is_empty()
        || completion.completed_at_ms < completion.observation_observed_at_ms
    {
        return false;
    }
    match &completion.outcome {
        EpisodeDeadlineOutcome::Armed { deadline, .. } => {
            !deadline.deadline_id.trim().is_empty()
                && deadline.generation != 0
                && deadline.armed_at_ms != 0
                && deadline.expires_at_ms > deadline.armed_at_ms
        }
        EpisodeDeadlineOutcome::Cancelled {
            deadline_id,
            deadline_generation,
        } => !deadline_id.trim().is_empty() && *deadline_generation != 0,
    }
}

fn receipt_observation(receipt: &EpisodeProjectionReceipt) -> EpisodeObservation {
    EpisodeObservation {
        binding_digest: receipt.binding_digest.clone(),
        episode_id: receipt.episode_id.clone(),
        generation: receipt.generation,
        observed_at_ms: receipt.observed_at_ms,
        private_root_cause_fact_id: receipt.private_root_cause_fact_id.clone(),
        observation_fact_id: receipt.observation_fact_id.clone(),
        mark_symbol: receipt.mark_symbol.clone(),
        mark_generation: receipt.mark_generation,
        mark_received_at_ms: receipt.mark_received_at_ms,
        mark_exchange_time_ms: receipt.mark_exchange_time_ms,
        mark_price: receipt.mark_price,
    }
}

pub fn episode_observation_fact_id(
    observation: &EpisodeObservation,
) -> Result<String, ScalpingCoordinatorError> {
    let canonical = serde_json::to_vec(&(
        &observation.binding_digest,
        &observation.episode_id,
        observation.generation,
        observation.observed_at_ms,
        &observation.private_root_cause_fact_id,
        &observation.mark_symbol,
        observation.mark_generation,
        observation.mark_received_at_ms,
        observation.mark_exchange_time_ms,
        observation.mark_price,
    ))
    .map_err(|_| ScalpingCoordinatorError::EpisodeObservation)?;
    Ok(format!(
        "episode-observation:{:x}",
        Sha256::digest(canonical)
    ))
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingCoordinatorError {
    #[error("coordinator checkpoint is incompatible")]
    Checkpoint,
    #[error("private facts are incomplete or have an invalid timestamp")]
    PrivateFacts,
    #[error("private fact generation is stale or repeated")]
    PrivateGeneration,
    #[error("pending episode requires a newer private reconciliation generation")]
    RecoveryGeneration,
    #[error("market authorization generation does not match private facts")]
    Generation,
    #[error("episode observation does not match the active episode or latest private identity")]
    EpisodeObservation,
    #[error("episode deadline completion is stale, conflicting, or not bound to a pending action")]
    DeadlineCompletion,
    #[error("risk replay cursor is stale or conflicts with its durable proof")]
    RiskCursor,
    #[error("market delivery identity is invalid")]
    MarketDelivery,
    #[error("strategy evaluation failed: {0}")]
    Strategy(#[from] ScalpingError),
}
