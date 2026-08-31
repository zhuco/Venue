// Fault projection lives alongside this pure reducer.

use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use venue_indicators::{BreakoutDirection, FeatureFrame, SourceCursor};

use super::{
    BlockingReason, CandidateEvidence, CandidateMemory, CandidateMemoryRejection,
    CandidatePreparation, Direction, EntryStyle, EpisodeProjection, EpisodeState, ExitTemplate,
    Expert, ExposureState, MarketRegime, NoopReason, ProtectionState, RiskGate, RiskLedger,
    SafetyProjection, ScalpingCheckpoint, ScalpingDecision, ScalpingError, ScalpingParams,
    ScalpingState, SemanticIntent, SemanticPurpose, StrategyBinding, robust_value,
};

/// Host-owned authorization projected into the pure reducer.  It carries no writer, permit, or
/// physical-order capability and is implemented by the root controller's `EntryAuthorization`.
pub trait LifecycleAuthorization {
    fn is_allowed(&self) -> bool;
    fn matches_at(&self, binding: &StrategyBinding, decision_at_ms: u64) -> bool;
    fn revision(&self) -> u64;
    fn authority_generation(&self) -> u64;
}

/// The first migrated strategy is intentionally a synchronous pure state machine. It may produce
/// a semantic intent in Shadow, but it has no execution, network, journal, or permit dependency.
#[derive(Clone, Debug)]
pub struct ScalpingStrategy {
    binding: StrategyBinding,
    binding_digest: String,
    pub(super) params: ScalpingParams,
    params_digest: String,
    controller_revision: u64,
    authority_generation: u64,
    requires_fresh_authorization: bool,
    regime: Option<MarketRegime>,
    regime_entered_at_ms: Option<u64>,
    pub(super) state: ScalpingState,
    pub(super) episode: Option<EpisodeProjection>,
    last_frame_generation: Option<u64>,
    last_watermark_ms: Option<u64>,
    cursors: BTreeMap<String, SourceCursor>,
    candidate_memory: CandidateMemory,
    pub(super) risk: RiskLedger,
    recovery_rewarm_pending: bool,
    recovery_rewarm_until_ms: Option<u64>,
}

impl ScalpingStrategy {
    pub fn new(binding: StrategyBinding, params: ScalpingParams) -> Result<Self, ScalpingError> {
        binding.validate()?;
        params.validate_for(&binding)?;
        let params_digest = digest_params(&params)?;
        let binding_digest = digest_strategy_binding(&binding);
        let risk = RiskLedger::new(&params);
        Ok(Self {
            binding,
            binding_digest,
            params,
            params_digest,
            controller_revision: 0,
            authority_generation: 0,
            requires_fresh_authorization: false,
            regime: None,
            regime_entered_at_ms: None,
            state: ScalpingState::Bootstrapping,
            episode: None,
            last_frame_generation: None,
            last_watermark_ms: None,
            cursors: BTreeMap::new(),
            candidate_memory: CandidateMemory::default(),
            risk,
            recovery_rewarm_pending: false,
            recovery_rewarm_until_ms: None,
        })
    }

    pub fn restore(
        binding: StrategyBinding,
        params: ScalpingParams,
        checkpoint: ScalpingCheckpoint,
    ) -> Result<Self, ScalpingError> {
        let mut strategy = Self::new(binding, params)?;
        if checkpoint.schema_version != super::SCALPING_CHECKPOINT_SCHEMA
            || checkpoint.strategy_kind != strategy.binding.strategy_kind()
            || checkpoint.binding_digest != strategy.binding_digest
            || checkpoint.params_digest != strategy.params_digest
        {
            return Err(ScalpingError::Checkpoint);
        }
        validate_checkpoint_progress(&checkpoint, &strategy.params.required_sources)?;
        if let Some(episode) = &checkpoint.episode {
            episode.validate_persisted()?;
            if episode.frozen_intent.symbol != strategy.binding.symbol
                || episode.frozen_evidence.as_ref().is_some_and(|evidence| {
                    evidence.binding_digest != strategy.binding_digest
                        || evidence.calibration_model_version
                            != strategy.params.calibration_model_version
                        || evidence.calibration_digest != strategy.params.calibration_model_digest
                })
            {
                return Err(ScalpingError::Checkpoint);
            }
        }
        strategy
            .candidate_memory
            .restore_state(checkpoint.candidate_memory)?;
        strategy.risk.restore_state(checkpoint.risk)?;
        // Strategy state never restores a prior entry authorization. The controller must issue a
        // new authorization after its own recovery and a fresh authority projection. A prepared
        // candidate is retained solely so its separately durable evidence coordinator can finish
        // the N→N+1 hand-off; it remains unusable until that fresh authorization is checked.
        strategy.state = match checkpoint.state {
            ScalpingState::Cooldown { until_ms } => ScalpingState::Cooldown { until_ms },
            ScalpingState::Bootstrapping
            | ScalpingState::Ready
            | ScalpingState::Reserved(_)
            | ScalpingState::Blocked(_) => ScalpingState::Bootstrapping,
            ScalpingState::CandidatePending(preparation) => {
                ScalpingState::CandidatePending(preparation)
            }
        };
        strategy.episode = checkpoint.episode;
        strategy.last_watermark_ms = checkpoint.last_watermark_ms;
        strategy.last_frame_generation = checkpoint.last_frame_generation;
        strategy.cursors = checkpoint.cursors;
        strategy.controller_revision = checkpoint.controller_revision;
        strategy.authority_generation = checkpoint.authority_generation;
        strategy.requires_fresh_authorization = true;
        strategy.regime = checkpoint.regime;
        strategy.regime_entered_at_ms = checkpoint.regime_entered_at_ms;
        strategy.recovery_rewarm_pending = strategy.last_frame_generation.is_some();
        strategy.recovery_rewarm_until_ms = None;
        Ok(strategy)
    }

    pub fn state(&self) -> &ScalpingState {
        &self.state
    }

    pub fn checkpoint(&self) -> ScalpingCheckpoint {
        ScalpingCheckpoint {
            schema_version: super::SCALPING_CHECKPOINT_SCHEMA,
            strategy_kind: self.binding.strategy_kind(),
            binding_digest: self.binding_digest.clone(),
            params_digest: self.params_digest.clone(),
            controller_revision: self.controller_revision,
            authority_generation: self.authority_generation,
            regime: self.regime,
            regime_entered_at_ms: self.regime_entered_at_ms,
            state: self.state.clone(),
            episode: self.episode.clone(),
            last_frame_generation: self.last_frame_generation,
            last_watermark_ms: self.last_watermark_ms,
            cursors: self.cursors.clone(),
            candidate_memory: self.candidate_memory.export_state(),
            risk: self.risk.export_state(),
        }
    }

    pub fn binding(&self) -> &StrategyBinding {
        &self.binding
    }

    /// Evaluates one fresh, coherent frame. Every unsafe condition is represented as a Noop and
    /// persists a blocking state; neither outcome can be converted into a physical order here.
    pub fn evaluate<A: LifecycleAuthorization>(
        &mut self,
        frame: &FeatureFrame,
        safety: &SafetyProjection,
        authorization: &A,
    ) -> Result<ScalpingDecision, ScalpingError> {
        self.evaluate_at(frame, safety, authorization, frame.watermark_ms)
    }

    /// The coordinator supplies its decision clock so a delayed frame cannot create a new
    /// candidate. The pure convenience entrypoint above remains replay-deterministic.
    pub fn evaluate_at<A: LifecycleAuthorization>(
        &mut self,
        frame: &FeatureFrame,
        safety: &SafetyProjection,
        authorization: &A,
        decision_at_ms: u64,
    ) -> Result<ScalpingDecision, ScalpingError> {
        if let Some(decision) = self.validate_and_record_frame(frame, decision_at_ms)? {
            return Ok(decision);
        }

        if !authorization.is_allowed() {
            return Ok(self.block(BlockingReason::ControlStopped));
        }
        if !authorization.matches_at(&self.binding, decision_at_ms) {
            return Err(ScalpingError::Authorization);
        }
        if authorization.revision() < self.controller_revision
            || authorization.authority_generation() < self.authority_generation
            || (self.requires_fresh_authorization
                && authorization.authority_generation() <= self.authority_generation)
        {
            return Ok(self.block(BlockingReason::RecoveryAuthorization));
        }
        self.controller_revision = authorization.revision();
        self.authority_generation = authorization.authority_generation();
        self.requires_fresh_authorization = false;
        self.prepare_after_lifecycle(frame, safety)
    }

    /// Evaluates only the candidate-preparation phase after a caller has already established a
    /// fresh lifecycle and signed bootstrap projection.  It can never admit evidence or emit an
    /// executable semantic intent; `admit` remains the sole Intent transition.
    pub fn prepare_from_lifecycle(
        &mut self,
        frame: &FeatureFrame,
        safety: &SafetyProjection,
        controller_revision: u64,
        authority_generation: u64,
        decision_at_ms: u64,
    ) -> Result<ScalpingDecision, ScalpingError> {
        if controller_revision == 0
            || authority_generation == 0
            || controller_revision < self.controller_revision
            || authority_generation < self.authority_generation
            || (self.requires_fresh_authorization
                && authority_generation <= self.authority_generation)
        {
            return Ok(self.block(BlockingReason::RecoveryAuthorization));
        }
        if let Some(decision) = self.validate_and_record_frame(frame, decision_at_ms)? {
            return Ok(decision);
        }
        self.controller_revision = controller_revision;
        self.authority_generation = authority_generation;
        self.requires_fresh_authorization = false;
        self.prepare_after_lifecycle(frame, safety)
    }

    fn validate_and_record_frame(
        &mut self,
        frame: &FeatureFrame,
        decision_at_ms: u64,
    ) -> Result<Option<ScalpingDecision>, ScalpingError> {
        if frame.symbol != self.binding.symbol {
            return Err(ScalpingError::Symbol);
        }
        frame
            .validate(&self.params.required_sources, self.params.max_data_age_ms)
            .map_err(|error| ScalpingError::Feature {
                detail: error.to_string(),
            })?;
        if frame
            .feature_versions
            .get("_feature_profile")
            .is_none_or(|value| value != &self.params.feature_profile)
            || frame
                .feature_versions
                .get("_feature_profile_digest")
                .is_none_or(|value| value != &self.params.feature_digest)
        {
            return Err(ScalpingError::FeatureProfile);
        }
        if decision_at_ms < frame.watermark_ms
            || decision_at_ms.saturating_sub(frame.watermark_ms)
                > self.params.max_decision_latency_ms
        {
            return Ok(Some(ScalpingDecision::Noop(NoopReason::DecisionExpired)));
        }
        self.validate_progress(frame)?;
        self.record_frame(frame);
        Ok(None)
    }

    fn prepare_after_lifecycle(
        &mut self,
        frame: &FeatureFrame,
        safety: &SafetyProjection,
    ) -> Result<ScalpingDecision, ScalpingError> {
        if self
            .episode
            .as_ref()
            .is_some_and(|episode| episode.state == EpisodeState::StoppedFlat)
        {
            self.episode = None;
            self.state = ScalpingState::Ready;
        }
        if self
            .episode
            .as_ref()
            .is_some_and(|episode| episode.state != EpisodeState::Cooldown)
        {
            return Ok(ScalpingDecision::Noop(NoopReason::ActiveEpisode));
        }
        if let Some(reason) = unsafe_reason(safety) {
            return Ok(self.block(reason));
        }
        if !self.risk.is_pristine() && self.risk.snapshot(frame.watermark_ms).gate != RiskGate::Open
        {
            return Ok(self.block(BlockingReason::StrategyRisk));
        }
        let regime = self.observe_regime(frame);
        if !self.regime_is_admissible(frame, regime) {
            self.state = ScalpingState::Ready;
            return Ok(ScalpingDecision::Noop(NoopReason::RegimeAmbiguous));
        }
        if self.recovery_rewarming(frame.watermark_ms) {
            self.state = ScalpingState::Bootstrapping;
            return Ok(ScalpingDecision::Noop(NoopReason::RecoveryWarmup));
        }
        if let ScalpingState::CandidatePending(preparation) = &self.state {
            if frame.watermark_ms < preparation.valid_until_ms {
                return Ok(ScalpingDecision::Noop(NoopReason::CandidatePending));
            }
            self.state = ScalpingState::Cooldown {
                until_ms: frame.watermark_ms.saturating_add(self.params.cooldown_ms),
            };
        }
        if matches!(self.state, ScalpingState::Reserved(_)) {
            return Ok(ScalpingDecision::Noop(NoopReason::CandidatePending));
        }
        if let ScalpingState::Cooldown { until_ms } = self.state {
            if frame.watermark_ms < until_ms {
                return Ok(ScalpingDecision::Noop(NoopReason::Cooldown));
            }
            self.episode = None;
            self.state = ScalpingState::Ready;
        }
        if frame.values.spread_bps > self.params.max_spread_bps
            || frame.values.depth_quote < self.params.min_depth_quote
            || frame.values.toxicity > self.params.max_toxicity
        {
            self.state = ScalpingState::Ready;
            return Ok(ScalpingDecision::Noop(NoopReason::NoSignal));
        }

        let deviation_bps = (frame.values.mid_price.value() - frame.values.fair_price.value())
            / frame.values.fair_price.value()
            * Decimal::new(10_000, 0);
        let plans = self.select_candidates(frame, deviation_bps, regime);
        if plans.is_empty() {
            self.state = ScalpingState::Ready;
            return Ok(ScalpingDecision::Noop(NoopReason::NoSignal));
        }
        let intents = plans
            .into_iter()
            .map(|(direction, expert, entry_style, exit_template)| {
                self.intent(
                    frame,
                    direction,
                    expert,
                    entry_style,
                    exit_template,
                    deviation_bps,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let market_regime = self.regime.ok_or(ScalpingError::Evidence)?;
        let valid_until_ms = intents
            .iter()
            .map(|intent| intent.valid_until_ms)
            .min()
            .ok_or(ScalpingError::Evidence)?;
        let preparation = CandidatePreparation {
            preparation_id: preparation_seed(&self.binding_digest, &self.params_digest, frame),
            binding_digest: self.binding_digest.clone(),
            controller_revision: self.controller_revision,
            authority_generation: self.authority_generation,
            market_regime,
            frame_generation: frame.generation,
            watermark_ms: frame.watermark_ms,
            valid_until_ms,
            candidates: intents,
        };
        self.state = ScalpingState::CandidatePending(Box::new(preparation.clone()));
        Ok(ScalpingDecision::Prepared(Box::new(preparation)))
    }

    /// Admits exactly one pending candidate using anonymous projections. Missing, stale, or
    /// non-admissible evidence fails closed and never creates a reservation or semantic intent.
    pub fn admit(
        &mut self,
        evidence: &[CandidateEvidence],
        observed_at_ms: u64,
    ) -> Result<ScalpingDecision, ScalpingError> {
        let ScalpingState::CandidatePending(preparation) = &self.state else {
            return Ok(ScalpingDecision::Noop(NoopReason::EvidenceUnavailable));
        };
        if observed_at_ms > preparation.valid_until_ms {
            self.state = ScalpingState::Cooldown {
                until_ms: observed_at_ms.saturating_add(self.params.cooldown_ms),
            };
            return Ok(ScalpingDecision::Noop(NoopReason::EvidenceUnavailable));
        }
        let mut seen = BTreeSet::new();
        let mut eligible = Vec::new();
        for item in evidence {
            if !seen.insert(item.candidate_id.as_str())
                || item.preparation_id != preparation.preparation_id
                || item.binding_digest != preparation.binding_digest
                || item.frame_generation != preparation.frame_generation
                || item.watermark_ms != preparation.watermark_ms
                || item.valid_until_ms < preparation.watermark_ms
                || item.calibration_model_version != self.params.calibration_model_version
                || item.calibration_digest != self.params.calibration_model_digest
                || !digest_is_valid(&item.calibration_digest)
                || !digest_is_valid(&item.cost_digest)
                || !digest_is_valid(&item.risk_digest)
            {
                return Err(ScalpingError::Evidence);
            }
            if let Some(candidate) = preparation
                .candidates
                .iter()
                .find(|candidate| candidate.intent_id == item.candidate_id)
            {
                if !candidate.risk_plan.admits_worst_loss(&item.worst_loss) {
                    return Err(ScalpingError::Evidence);
                }
                if item.admissible && item.valid_until_ms >= observed_at_ms {
                    let robust_value = robust_value(item, self.params.uncertainty_multiplier)?;
                    if robust_value >= self.params.min_net_ev_bps {
                        eligible.push((robust_value, item.uncertainty_bps, candidate, item));
                    }
                }
            } else {
                return Err(ScalpingError::Evidence);
            }
        }
        eligible.sort_by(|left, right| {
            right
                .0
                .cmp(&left.0)
                .then_with(|| left.1.cmp(&right.1))
                .then_with(|| left.2.expert.cmp(&right.2.expert))
                .then_with(|| left.2.opportunity_key.cmp(&right.2.opportunity_key))
        });
        let Some((_, _, candidate, winner_evidence)) = eligible.first() else {
            self.state = ScalpingState::Ready;
            return Ok(ScalpingDecision::Noop(NoopReason::EvidenceUnavailable));
        };
        let winner_direction = candidate.direction;
        if eligible.iter().skip(1).any(|(robust_value, _, other, _)| {
            other.direction != winner_direction
                && eligible[0].0 - *robust_value < self.params.conflict_margin_bps
        }) {
            self.state = ScalpingState::Ready;
            return Ok(ScalpingDecision::Noop(NoopReason::EvidenceUnavailable));
        }
        let reserved = (**candidate).clone();
        match self
            .candidate_memory
            .check_and_record(&reserved, preparation.watermark_ms)
        {
            Ok(()) => {}
            Err(CandidateMemoryRejection::Duplicate) => {
                self.state = ScalpingState::Ready;
                return Ok(ScalpingDecision::Noop(NoopReason::DuplicateOpportunity));
            }
            Err(CandidateMemoryRejection::Capacity) => {
                self.state = ScalpingState::Ready;
                return Ok(ScalpingDecision::Noop(NoopReason::CandidateMemoryFull));
            }
        }
        self.episode = Some(EpisodeProjection::reserve(
            reserved.clone(),
            Some((**winner_evidence).clone()),
            observed_at_ms,
        ));
        self.state = ScalpingState::Reserved(Box::new(reserved.clone()));
        Ok(ScalpingDecision::Intent(Box::new(reserved)))
    }

    /// Converts a prepared strategy proposal into one semantic intent without Core quote/risk
    /// valuation, sealed calibration, or an evidence bundle.  It retains duplicate prevention and
    /// refuses competing long/short candidates rather than choosing a direction by accident.
    pub fn admit_direct(&mut self, observed_at_ms: u64) -> Result<ScalpingDecision, ScalpingError> {
        let ScalpingState::CandidatePending(preparation) = &self.state else {
            return Ok(ScalpingDecision::Noop(NoopReason::EvidenceUnavailable));
        };
        if observed_at_ms > preparation.valid_until_ms {
            self.state = ScalpingState::Cooldown {
                until_ms: observed_at_ms.saturating_add(self.params.cooldown_ms),
            };
            return Ok(ScalpingDecision::Noop(NoopReason::DecisionExpired));
        }
        let mut candidates = preparation.candidates.clone();
        candidates.sort_by(|left, right| {
            left.expert
                .cmp(&right.expert)
                .then_with(|| left.opportunity_key.cmp(&right.opportunity_key))
                .then_with(|| left.intent_id.cmp(&right.intent_id))
        });
        let Some(candidate) = candidates.first().cloned() else {
            self.state = ScalpingState::Ready;
            return Ok(ScalpingDecision::Noop(NoopReason::NoSignal));
        };
        if candidates
            .iter()
            .skip(1)
            .any(|other| other.direction != candidate.direction)
        {
            self.state = ScalpingState::Ready;
            return Ok(ScalpingDecision::Noop(NoopReason::RegimeAmbiguous));
        }
        match self
            .candidate_memory
            .check_and_record(&candidate, preparation.watermark_ms)
        {
            Ok(()) => {}
            Err(CandidateMemoryRejection::Duplicate) => {
                self.state = ScalpingState::Ready;
                return Ok(ScalpingDecision::Noop(NoopReason::DuplicateOpportunity));
            }
            Err(CandidateMemoryRejection::Capacity) => {
                self.state = ScalpingState::Ready;
                return Ok(ScalpingDecision::Noop(NoopReason::CandidateMemoryFull));
            }
        }
        self.episode = Some(EpisodeProjection::reserve(
            candidate.clone(),
            None,
            observed_at_ms,
        ));
        self.state = ScalpingState::Reserved(Box::new(candidate.clone()));
        Ok(ScalpingDecision::Intent(Box::new(candidate)))
    }

    /// Validates the fresh frame and controller authority that accompany an evidence admission.
    /// The pending candidate remains frozen; this check cannot create or replace candidates.
    pub fn validate_admission_context<A: LifecycleAuthorization>(
        &self,
        frame: &FeatureFrame,
        authorization: &A,
    ) -> Result<(), ScalpingError> {
        if frame.symbol != self.binding.symbol {
            return Err(ScalpingError::Symbol);
        }
        frame
            .validate(&self.params.required_sources, self.params.max_data_age_ms)
            .map_err(|error| ScalpingError::Feature {
                detail: error.to_string(),
            })?;
        if frame
            .feature_versions
            .get("_feature_profile")
            .is_none_or(|value| value != &self.params.feature_profile)
            || frame
                .feature_versions
                .get("_feature_profile_digest")
                .is_none_or(|value| value != &self.params.feature_digest)
        {
            return Err(ScalpingError::FeatureProfile);
        }
        self.validate_progress(frame)?;
        if !authorization.matches_at(&self.binding, frame.watermark_ms)
            || authorization.revision() < self.controller_revision
            || authorization.authority_generation() < self.authority_generation
            || (self.requires_fresh_authorization
                && authorization.authority_generation() <= self.authority_generation)
        {
            return Err(ScalpingError::Authorization);
        }
        Ok(())
    }

    /// Shadow consumers acknowledge observation only. This creates a cooldown but has no route
    /// to a permit, journal, or exchange mutation.
    pub fn acknowledge_shadow_intent(
        &mut self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<(), ScalpingError> {
        let ScalpingState::Reserved(intent) = &self.state else {
            return Err(ScalpingError::ShadowOutcome);
        };
        if intent.intent_id != intent_id || observed_at_ms < self.last_watermark_ms.unwrap_or(0) {
            return Err(ScalpingError::ShadowOutcome);
        }
        let episode = self.episode.as_mut().ok_or(ScalpingError::Episode)?;
        if episode.episode_id != intent_id || observed_at_ms < episode.last_observed_at_ms {
            return Err(ScalpingError::ShadowOutcome);
        }
        episode.state = EpisodeState::Cooldown;
        episode.retry_not_before_ms = None;
        episode.last_observed_at_ms = observed_at_ms;
        self.state = ScalpingState::Cooldown {
            until_ms: observed_at_ms.saturating_add(self.params.cooldown_ms),
        };
        Ok(())
    }

    /// Records that a separately owned gateway durably confirmed the reserved intent as an
    /// exchange-protected position. This is semantic state only: it carries no order ID, writer,
    /// quantity, or mutation permit.
    pub fn confirm_live_entry(
        &mut self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<(), ScalpingError> {
        // The host save precedes settlement acknowledgement. If a crash occurs between those two
        // saves, replaying the exact outcome must not poison an already-open episode.
        if self.episode.as_ref().is_some_and(|episode| {
            episode.episode_id == intent_id && episode.state == EpisodeState::Open
        }) {
            return Ok(());
        }
        let ScalpingState::Reserved(intent) = &self.state else {
            return Err(ScalpingError::ShadowOutcome);
        };
        if intent.intent_id != intent_id || observed_at_ms < self.last_watermark_ms.unwrap_or(0) {
            return Err(ScalpingError::ShadowOutcome);
        }
        let episode = self.episode.as_mut().ok_or(ScalpingError::Episode)?;
        if episode.episode_id != intent_id
            || episode.state != EpisodeState::Reserved
            || observed_at_ms < episode.last_observed_at_ms
        {
            return Err(ScalpingError::ShadowOutcome);
        }
        episode.opened_at_ms = Some(observed_at_ms);
        episode.state = EpisodeState::Open;
        episode.last_observed_at_ms = observed_at_ms;
        Ok(())
    }

    /// Permanently rejects the current reservation and starts the strategy cooldown. Repeating a
    /// reconciled no-fill is harmless when recovery observes the same durable cooldown episode.
    pub fn reject_reserved_entry(
        &mut self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<(), ScalpingError> {
        if self.episode.as_ref().is_some_and(|episode| {
            episode.episode_id == intent_id
                && episode.state == EpisodeState::Cooldown
                && matches!(self.state, ScalpingState::Cooldown { .. })
        }) {
            return Ok(());
        }
        let episode = self.episode.as_mut().ok_or(ScalpingError::Episode)?;
        if !matches!(
            episode.state,
            EpisodeState::Reserved | EpisodeState::EntryRetryWait
        ) || episode.episode_id != intent_id
            || observed_at_ms < episode.last_observed_at_ms
        {
            return Err(ScalpingError::Episode);
        }
        episode.state = EpisodeState::Cooldown;
        episode.retry_not_before_ms = None;
        episode.last_observed_at_ms = observed_at_ms;
        self.state = ScalpingState::Cooldown {
            until_ms: observed_at_ms.saturating_add(self.params.cooldown_ms),
        };
        Ok(())
    }

    #[must_use]
    pub fn episode(&self) -> Option<&EpisodeProjection> {
        self.episode.as_ref()
    }

    /// Re-scores the frozen passive candidate with a fresh anonymous cost projection. Calibration,
    /// risk, opportunity, and candidate identity cannot change, and this check does not consume
    /// candidate memory or create another episode.
    pub fn validate_reprice(
        &self,
        fresh: &CandidateEvidence,
        observed_at_ms: u64,
    ) -> Result<(), ScalpingError> {
        let episode = self.episode.as_ref().ok_or(ScalpingError::Reprice)?;
        let frozen = episode
            .frozen_evidence
            .as_ref()
            .ok_or(ScalpingError::Reprice)?;
        if !matches!(
            episode.state,
            EpisodeState::Reserved | EpisodeState::EntryRetryWait
        ) || episode.frozen_intent.entry_style != EntryStyle::PassiveMaker
            || episode.frozen_intent.max_reprices == 0
            || observed_at_ms == 0
            || observed_at_ms >= episode.frozen_intent.valid_until_ms
            || fresh.valid_until_ms < observed_at_ms
            || !same_frozen_evidence_identity(frozen, fresh)
            || !digest_is_valid(&fresh.cost_digest)
            || robust_value(fresh, self.params.uncertainty_multiplier)? < self.params.min_net_ev_bps
        {
            return Err(ScalpingError::Reprice);
        }
        Ok(())
    }

    /// Schedules another attempt of the same frozen episode. Exhaustion never regenerates a
    /// candidate: it converges to the normal cooldown and returns `false`.
    pub fn retry_reserved_entry(
        &mut self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<bool, ScalpingError> {
        let episode = self.episode.as_mut().ok_or(ScalpingError::Episode)?;
        if episode.state != EpisodeState::Reserved
            || episode.episode_id != intent_id
            || observed_at_ms < episode.last_observed_at_ms
        {
            return Err(ScalpingError::Episode);
        }
        episode.last_observed_at_ms = observed_at_ms;
        if episode.attempts_started >= episode.frozen_intent.attempt_cap {
            episode.state = EpisodeState::Cooldown;
            episode.retry_not_before_ms = None;
            self.state = ScalpingState::Cooldown {
                until_ms: observed_at_ms.saturating_add(self.params.cooldown_ms),
            };
            return Ok(false);
        }
        episode.state = EpisodeState::EntryRetryWait;
        episode.retry_not_before_ms =
            Some(observed_at_ms.saturating_add(self.params.entry_retry_cooldown_ms));
        Ok(true)
    }

    /// Consumes one fresh feature frame while a retry timer is armed. The candidate remains
    /// frozen; the frame only advances durable progress and releases the next allowed attempt.
    pub fn advance_entry_retry_frame(
        &mut self,
        frame: &FeatureFrame,
    ) -> Result<bool, ScalpingError> {
        if self
            .episode
            .as_ref()
            .is_none_or(|episode| episode.state != EpisodeState::EntryRetryWait)
        {
            return Ok(false);
        }
        if frame.symbol != self.binding.symbol {
            return Err(ScalpingError::Symbol);
        }
        frame
            .validate(&self.params.required_sources, self.params.max_data_age_ms)
            .map_err(|error| ScalpingError::Feature {
                detail: error.to_string(),
            })?;
        if frame
            .feature_versions
            .get("_feature_profile")
            .is_none_or(|value| value != &self.params.feature_profile)
            || frame
                .feature_versions
                .get("_feature_profile_digest")
                .is_none_or(|value| value != &self.params.feature_digest)
        {
            return Err(ScalpingError::FeatureProfile);
        }
        self.validate_progress(frame)?;
        self.record_frame(frame);

        let episode = self.episode.as_mut().ok_or(ScalpingError::Episode)?;
        let retry_at = episode.retry_not_before_ms.ok_or(ScalpingError::Episode)?;
        if frame.watermark_ms < retry_at {
            return Ok(false);
        }
        if frame.watermark_ms >= episode.frozen_intent.valid_until_ms
            || episode.attempts_started >= episode.frozen_intent.attempt_cap
        {
            episode.state = EpisodeState::Cooldown;
            episode.retry_not_before_ms = None;
            episode.last_observed_at_ms = frame.watermark_ms;
            self.state = ScalpingState::Cooldown {
                until_ms: frame.watermark_ms.saturating_add(self.params.cooldown_ms),
            };
            return Ok(false);
        }
        episode.attempts_started += 1;
        episode.state = EpisodeState::Reserved;
        episode.retry_not_before_ms = None;
        episode.last_observed_at_ms = frame.watermark_ms;
        self.state = ScalpingState::Reserved(Box::new(episode.frozen_intent.clone()));
        Ok(true)
    }

    fn block(&mut self, reason: BlockingReason) -> ScalpingDecision {
        self.state = ScalpingState::Blocked(reason);
        ScalpingDecision::Noop(NoopReason::Blocked(reason))
    }

    fn validate_progress(&self, frame: &FeatureFrame) -> Result<(), ScalpingError> {
        let Some(previous_generation) = self.last_frame_generation else {
            return Ok(());
        };
        let previous_watermark = self
            .last_watermark_ms
            .ok_or(ScalpingError::FeatureProgress)?;
        if frame.generation < previous_generation
            || (frame.generation == previous_generation && frame.watermark_ms < previous_watermark)
        {
            return Err(ScalpingError::FeatureProgress);
        }
        if frame.generation == previous_generation {
            let mut advanced = false;
            let invalid = self.params.required_sources.iter().any(|source| {
                let (Some(previous), Some(current)) =
                    (self.cursors.get(source), frame.cursors.get(source))
                else {
                    return true;
                };
                let regressed = current.sequence < previous.sequence
                    || current.event_time_ms < previous.event_time_ms;
                let inconsistent = current.sequence == previous.sequence
                    && current.event_time_ms != previous.event_time_ms;
                advanced |= current.sequence > previous.sequence;
                regressed || inconsistent
            });
            if invalid || !advanced {
                return Err(ScalpingError::FeatureProgress);
            }
        } else if frame.watermark_ms <= previous_watermark
            || self.params.required_sources.iter().any(|source| {
                let Some(previous) = self.cursors.get(source) else {
                    return true;
                };
                frame
                    .cursors
                    .get(source)
                    .is_none_or(|current| current.event_time_ms <= previous.event_time_ms)
            })
        {
            return Err(ScalpingError::FeatureProgress);
        }
        Ok(())
    }

    fn record_frame(&mut self, frame: &FeatureFrame) {
        self.last_frame_generation = Some(frame.generation);
        self.last_watermark_ms = Some(frame.watermark_ms);
        self.cursors = frame.cursors.clone();
    }

    fn intent(
        &self,
        frame: &FeatureFrame,
        direction: Direction,
        expert: Expert,
        entry_style: EntryStyle,
        exit_template: ExitTemplate,
        deviation_bps: Decimal,
    ) -> Result<SemanticIntent, ScalpingError> {
        let target_quote = if self.params.quote_cap.value <= self.binding.risk_budget.value {
            self.params.quote_cap.clone()
        } else {
            self.binding.risk_budget.clone()
        };
        let (opportunity_key, breakout_cursor) =
            opportunity_identity(frame, direction, expert, entry_style)?;
        let idempotency_seed = intent_seed(
            &self.binding_digest,
            &self.params_digest,
            frame,
            direction,
            expert,
            entry_style,
            &opportunity_key,
        );
        let (hard_stop_distance_bps, target_distance_bps) = self
            .params
            .exit_distance_policy
            .distances_bps(frame.values.expected_move_bps);
        Ok(SemanticIntent {
            intent_id: format!("scalping-{idempotency_seed}"),
            symbol: self.binding.symbol.clone(),
            direction,
            purpose: SemanticPurpose::Entry,
            expert,
            entry_style,
            exit_template,
            attempt_cap: match expert {
                Expert::RangeFade => self.params.max_order_attempts.min(2),
                Expert::TrendPullback => self.params.max_order_attempts,
                Expert::BreakoutContinuation => 1,
            },
            max_reprices: if entry_style == EntryStyle::PassiveMaker {
                self.params.max_reprices
            } else {
                0
            },
            risk_plan: super::RiskPlan {
                risk_per_episode: self.params.risk_per_episode.clone(),
                quote_cap: self.params.quote_cap.clone(),
                max_episode_loss: self.params.max_episode_loss.clone(),
            },
            target_quote,
            reference_price: if self.binding.parameter_release_id
                == super::PHASE8_ATR14_PARAMETER_RELEASE_ID
            {
                frame.values.fair_price
            } else {
                frame.values.mid_price
            },
            max_slippage_bps: self.params.max_entry_slippage_bps,
            valid_until_ms: frame
                .watermark_ms
                .saturating_add(self.params.candidate_ttl_ms),
            entry_ttl_ms: self.params.entry_ttl_ms,
            hard_stop_distance_bps,
            target_distance_bps,
            max_hold_ms: self.params.max_hold_ms,
            max_unprotected_ms: self.params.max_unprotected_ms,
            requires_server_protection: true,
            opportunity_key,
            breakout_cursor,
            idempotency_seed: format!("{idempotency_seed}:{deviation_bps}"),
        })
    }

    fn select_candidates(
        &self,
        frame: &FeatureFrame,
        deviation_bps: Decimal,
        regime: MarketRegime,
    ) -> Vec<(Direction, Expert, EntryStyle, ExitTemplate)> {
        if regime == MarketRegime::RegimeUnknown
            || (regime == MarketRegime::Shock && !self.phase8_shock_reversal_enabled())
        {
            return Vec::new();
        }
        let dwell_complete = regime == MarketRegime::Range
            || self.regime_entered_at_ms.is_some_and(|entered| {
                frame.watermark_ms.saturating_sub(entered) >= self.params.regime_dwell_ms
            });
        if !dwell_complete {
            return Vec::new();
        }
        match regime {
            MarketRegime::Shock
                if self.phase8_shock_reversal_enabled()
                    && self.params.enabled_experts.contains(&Expert::RangeFade)
                    && self
                        .params
                        .enabled_entry_styles
                        .contains(&EntryStyle::PassiveMaker) =>
            {
                let direction = if deviation_bps <= -self.params.min_deviation_bps
                    && frame.values.short_return_bps > Decimal::ZERO
                    && frame.values.book_imbalance > Decimal::ZERO
                    && frame.values.trade_imbalance > Decimal::ZERO
                {
                    Some(Direction::Long)
                } else if deviation_bps >= self.params.min_deviation_bps
                    && frame.values.short_return_bps < Decimal::ZERO
                    && frame.values.book_imbalance < Decimal::ZERO
                    && frame.values.trade_imbalance < Decimal::ZERO
                {
                    Some(Direction::Short)
                } else {
                    None
                };
                direction.map_or_else(Vec::new, |direction| {
                    vec![(
                        direction,
                        Expert::RangeFade,
                        EntryStyle::PassiveMaker,
                        ExitTemplate::FairValue,
                    )]
                })
            }
            MarketRegime::Range
                if self.params.enabled_experts.contains(&Expert::RangeFade)
                    && self
                        .params
                        .enabled_entry_styles
                        .contains(&EntryStyle::PassiveMaker) =>
            {
                let direction = if deviation_bps <= -self.params.min_deviation_bps
                    && frame.values.book_imbalance > Decimal::ZERO
                    && frame.values.trade_imbalance > Decimal::ZERO
                {
                    Some(Direction::Long)
                } else if deviation_bps >= self.params.min_deviation_bps
                    && frame.values.book_imbalance < Decimal::ZERO
                    && frame.values.trade_imbalance < Decimal::ZERO
                {
                    Some(Direction::Short)
                } else {
                    None
                };
                direction.map_or_else(Vec::new, |direction| {
                    vec![(
                        direction,
                        Expert::RangeFade,
                        EntryStyle::PassiveMaker,
                        ExitTemplate::FairValue,
                    )]
                })
            }
            MarketRegime::TrendUp | MarketRegime::TrendDown
                if self.params.enabled_experts.contains(&Expert::TrendPullback) =>
            {
                let direction = match regime {
                    MarketRegime::TrendUp if frame.values.short_return_bps < Decimal::ZERO => {
                        Direction::Long
                    }
                    MarketRegime::TrendDown if frame.values.short_return_bps > Decimal::ZERO => {
                        Direction::Short
                    }
                    _ => return Vec::new(),
                };
                entry_styles(&self.params)
                    .into_iter()
                    .map(|entry_style| {
                        (
                            direction,
                            Expert::TrendPullback,
                            entry_style,
                            ExitTemplate::TrendTrail,
                        )
                    })
                    .collect()
            }
            MarketRegime::ExpansionUp | MarketRegime::ExpansionDown
                if self
                    .params
                    .enabled_experts
                    .contains(&Expert::BreakoutContinuation)
                    && self
                        .params
                        .enabled_entry_styles
                        .contains(&EntryStyle::MarketableLimit) =>
            {
                let direction = if regime == MarketRegime::ExpansionUp {
                    Direction::Long
                } else {
                    Direction::Short
                };
                let breakout_matches = frame.breakout.as_ref().is_some_and(|breakout| {
                    matches!(
                        (breakout.direction, direction),
                        (BreakoutDirection::Long, Direction::Long)
                            | (BreakoutDirection::Short, Direction::Short)
                    )
                });
                if !breakout_matches {
                    return Vec::new();
                }
                vec![(
                    direction,
                    Expert::BreakoutContinuation,
                    EntryStyle::MarketableLimit,
                    ExitTemplate::Breakout,
                )]
            }
            _ => Vec::new(),
        }
    }

    fn observe_regime(&mut self, frame: &FeatureFrame) -> MarketRegime {
        let regime = classify_regime(frame, &self.params);
        if self.regime != Some(regime) {
            self.regime = Some(regime);
            self.regime_entered_at_ms = Some(frame.watermark_ms);
        }
        regime
    }

    /// The legacy router divided a normalized efficiency by ten and then floored it at 0.6, so
    /// the default confidence gate could never reject. This migration repair keeps confidence
    /// reachable while using only the already-normalized feature frame.
    fn regime_is_admissible(&self, frame: &FeatureFrame, regime: MarketRegime) -> bool {
        if regime == MarketRegime::RegimeUnknown {
            return false;
        }
        if regime == MarketRegime::Shock {
            return self.phase8_shock_reversal_enabled();
        }
        let efficiency = frame.values.trend_efficiency.abs();
        let confidence = match regime {
            MarketRegime::Range => Decimal::ONE - efficiency,
            MarketRegime::TrendUp
            | MarketRegime::TrendDown
            | MarketRegime::ExpansionUp
            | MarketRegime::ExpansionDown => efficiency,
            MarketRegime::Shock | MarketRegime::RegimeUnknown => Decimal::ZERO,
        }
        .max(Decimal::ZERO)
        .min(Decimal::ONE);
        let runner_up_confidence = Decimal::ONE - confidence;
        confidence >= self.params.regime_min_confidence
            && confidence - runner_up_confidence >= self.params.regime_confidence_margin
    }

    fn phase8_shock_reversal_enabled(&self) -> bool {
        self.binding.parameter_release_id == super::PHASE8_ATR14_PARAMETER_RELEASE_ID
    }

    fn recovery_rewarming(&mut self, watermark_ms: u64) -> bool {
        if self.recovery_rewarm_pending {
            self.recovery_rewarm_until_ms =
                Some(watermark_ms.saturating_add(self.params.regime_dwell_ms));
            self.recovery_rewarm_pending = false;
        }
        if self
            .recovery_rewarm_until_ms
            .is_some_and(|deadline| watermark_ms < deadline)
        {
            return true;
        }
        self.recovery_rewarm_until_ms = None;
        false
    }
}

fn same_frozen_evidence_identity(frozen: &CandidateEvidence, fresh: &CandidateEvidence) -> bool {
    fresh.candidate_id == frozen.candidate_id
        && fresh.preparation_id == frozen.preparation_id
        && fresh.binding_digest == frozen.binding_digest
        && fresh.frame_generation == frozen.frame_generation
        && fresh.watermark_ms == frozen.watermark_ms
        && fresh.calibration_model_version == frozen.calibration_model_version
        && fresh.calibration_digest == frozen.calibration_digest
        && fresh.risk_digest == frozen.risk_digest
        && fresh.fill_probability == frozen.fill_probability
        && fresh.fill_distribution == frozen.fill_distribution
        && fresh.outcomes == frozen.outcomes
        && fresh.target_pnl_bps == frozen.target_pnl_bps
        && fresh.stop_pnl_bps == frozen.stop_pnl_bps
        && fresh.other_pnl_bps == frozen.other_pnl_bps
        && fresh.outcome_expected_value_bps == frozen.outcome_expected_value_bps
        && fresh.uncertainty_bps == frozen.uncertainty_bps
        && fresh.admissible == frozen.admissible
}

fn unsafe_reason(safety: &SafetyProjection) -> Option<BlockingReason> {
    if !safety.private_snapshot_ready {
        Some(BlockingReason::PrivateSnapshot)
    } else if safety.exposure != ExposureState::Flat {
        Some(BlockingReason::ExposureNotFlat)
    } else if safety.execution_unknown {
        Some(BlockingReason::ExecutionUnknown)
    } else if safety.protection != ProtectionState::Complete {
        Some(BlockingReason::ProtectionGap)
    } else if safety.owner_conflict {
        Some(BlockingReason::OwnerConflict)
    } else if !safety.risk_budget_available {
        Some(BlockingReason::RiskBudget)
    } else {
        None
    }
}

fn digest_params(params: &ScalpingParams) -> Result<String, ScalpingError> {
    serde_json::to_vec(params)
        .map(|bytes| format!("{:x}", Sha256::digest(bytes)))
        .map_err(|_| ScalpingError::Parameters)
}

fn intent_seed(
    binding_digest: &str,
    params_digest: &str,
    frame: &FeatureFrame,
    direction: Direction,
    expert: Expert,
    entry_style: EntryStyle,
    opportunity_key: &str,
) -> String {
    let mut digest = Sha256::new();
    for part in [
        binding_digest.as_bytes(),
        params_digest.as_bytes(),
        match direction {
            Direction::Long => b"long".as_slice(),
            Direction::Short => b"short".as_slice(),
        },
        format!("{expert:?}").as_bytes(),
        format!("{entry_style:?}").as_bytes(),
        opportunity_key.as_bytes(),
        &frame.generation.to_be_bytes(),
        &frame.watermark_ms.to_be_bytes(),
    ] {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    for (source, cursor) in &frame.cursors {
        for part in [
            source.as_bytes(),
            &cursor.generation.to_be_bytes(),
            &cursor.sequence.to_be_bytes(),
            &cursor.event_time_ms.to_be_bytes(),
        ] {
            digest.update((part.len() as u64).to_be_bytes());
            digest.update(part);
        }
    }
    format!("{:x}", digest.finalize())
}

/// Stable binding identity used by the pure reducer and host-owned evidence coordinators.
pub fn digest_strategy_binding(binding: &StrategyBinding) -> String {
    binding.digest()
}

fn opportunity_identity(
    frame: &FeatureFrame,
    direction: Direction,
    expert: Expert,
    entry_style: EntryStyle,
) -> Result<(String, Option<super::BreakoutCursor>), ScalpingError> {
    if expert != Expert::BreakoutContinuation {
        return Ok((
            format!(
                "{}:{expert:?}:{direction:?}:{entry_style:?}",
                frame.watermark_ms
            )
            .to_lowercase(),
            None,
        ));
    }
    let opportunity = frame.breakout.as_ref().ok_or(ScalpingError::Evidence)?;
    let direction_matches = matches!(
        (opportunity.direction, direction),
        (BreakoutDirection::Long, Direction::Long) | (BreakoutDirection::Short, Direction::Short)
    );
    if !direction_matches {
        return Err(ScalpingError::Evidence);
    }
    Ok((
        format!(
            "breakout:g{}:b{}:{}:c{}:{}:{direction:?}",
            opportunity.generation,
            opportunity.boundary_sequence,
            opportunity.boundary_id,
            opportunity.compression_cycle_sequence,
            opportunity.compression_cycle_id,
        )
        .to_lowercase(),
        Some(super::BreakoutCursor {
            feature_generation: opportunity.generation,
            boundary_sequence: opportunity.boundary_sequence,
            compression_cycle_sequence: opportunity.compression_cycle_sequence,
        }),
    ))
}

fn classify_regime(frame: &FeatureFrame, params: &ScalpingParams) -> MarketRegime {
    if frame.values.short_return_bps.abs() >= params.shock_return_bps {
        return MarketRegime::Shock;
    }
    if frame.values.bandwidth_expansion >= params.breakout_threshold
        && frame.values.trend_efficiency >= params.trend_threshold
    {
        return MarketRegime::ExpansionUp;
    }
    if frame.values.bandwidth_expansion >= params.breakout_threshold
        && frame.values.trend_efficiency <= -params.trend_threshold
    {
        return MarketRegime::ExpansionDown;
    }
    if frame.values.trend_efficiency >= params.trend_threshold {
        return MarketRegime::TrendUp;
    }
    if frame.values.trend_efficiency <= -params.trend_threshold {
        return MarketRegime::TrendDown;
    }
    MarketRegime::Range
}

fn entry_styles(params: &ScalpingParams) -> Vec<EntryStyle> {
    params.enabled_entry_styles.iter().copied().collect()
}

fn preparation_seed(binding_digest: &str, params_digest: &str, frame: &FeatureFrame) -> String {
    let mut digest = Sha256::new();
    for part in [
        binding_digest.as_bytes(),
        params_digest.as_bytes(),
        &frame.generation.to_be_bytes(),
        &frame.watermark_ms.to_be_bytes(),
    ] {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    for (source, cursor) in &frame.cursors {
        for part in [
            source.as_bytes(),
            &cursor.generation.to_be_bytes(),
            &cursor.sequence.to_be_bytes(),
        ] {
            digest.update((part.len() as u64).to_be_bytes());
            digest.update(part);
        }
    }
    format!("scalping-preparation-{:x}", digest.finalize())
}

fn digest_is_valid(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
#[path = "engine_tests.rs"]
mod tests;

fn validate_checkpoint_progress(
    checkpoint: &ScalpingCheckpoint,
    required_sources: &BTreeSet<String>,
) -> Result<(), ScalpingError> {
    match (
        checkpoint.last_frame_generation,
        checkpoint.last_watermark_ms,
    ) {
        (None, None) if checkpoint.cursors.is_empty() => Ok(()),
        (Some(generation), Some(watermark))
            if required_sources
                .iter()
                .all(|source| checkpoint.cursors.contains_key(source))
                && checkpoint.cursors.values().all(|cursor| {
                    cursor.generation == generation
                        && cursor.sequence > 0
                        && cursor.event_time_ms > 0
                        && cursor.event_time_ms <= watermark
                }) =>
        {
            Ok(())
        }
        _ => Err(ScalpingError::Checkpoint),
    }
}
