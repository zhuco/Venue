use crate::{
    controller::{EntryAuthorization, ScalpingControllerUpdate},
    indicator::FeatureFrame,
    strategy::scalping::CandidateEvidence,
};

use super::{
    BoundRiskRevaluation, DeadlineClockObservation, DeadlineSchedulerOutcome,
    EpisodeDeadlineCompletion, EpisodeObservation, PrivateEntryGateReport,
    ScalpingCoordinatorOutput, ScalpingDeadlineScheduler, ScalpingDeadlineSchedulerError,
    ScalpingShadowHost, ScalpingShadowHostError, ScalpingShadowHostReport,
};

pub const SCALPING_RESIDENT_LEGACY_PRIORITY: [&str; 4] = [
    "command",
    "private_reconcile_signal",
    "timer_reconcile",
    "market_frame",
];

pub const SCALPING_RESIDENT_PRIORITY: [&str; 7] = [
    "controller_command",
    "private_gate_control",
    "private_reconcile_signal",
    "risk_proof",
    "episode_projection",
    "timer_reconcile",
    "market_frame",
];

/// One already-authorized public decision input. The resident runtime never manufactures the
/// controller authorization or evidence carried by this value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingResidentMarket {
    pub frame: FeatureFrame,
    pub decision_at_ms: u64,
    pub evidence: Vec<CandidateEvidence>,
    /// Only the no-Core resident path may request direct semantic admission. Empty evidence in a
    /// retained replay/calibration path still means "prepare and wait", never implicit admission.
    pub direct_admission: bool,
}

/// At most one item from each source is admitted per cycle. A caller may keep polling its own
/// sources, but cannot make market work jump ahead of command/risk, private, or deadline
/// reconciliation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ScalpingResidentCycle {
    pub controller: Option<ScalpingControllerUpdate>,
    pub private_gate: Option<PrivateEntryGateReport>,
    pub risk: Option<BoundRiskRevaluation>,
    pub episode_deadline_completion: Option<EpisodeDeadlineCompletion>,
    pub episode_observation: Option<EpisodeObservation>,
    pub deadline_clock: Option<DeadlineClockObservation>,
    pub market: Option<ScalpingResidentMarket>,
}

/// Every returned transition was durably committed by the Shadow host before publication.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ScalpingResidentCycleReport {
    pub controller: Option<ScalpingControllerUpdate>,
    pub controller_control: Option<ScalpingShadowHostReport>,
    pub private_gate: Option<ScalpingShadowHostReport>,
    pub risk: Option<ScalpingCoordinatorOutput>,
    pub episode_deadline_completion: Option<ScalpingCoordinatorOutput>,
    pub episode: Option<ScalpingCoordinatorOutput>,
    pub deadline: Option<DeadlineSchedulerOutcome>,
    pub market: Option<ScalpingCoordinatorOutput>,
}

/// Mutation-free orchestration for one scalping Shadow instance. Network transports, clocks,
/// projection producers, evidence producers, shutdown, and sleeping remain external owners.
#[derive(Debug)]
pub struct ScalpingResidentRuntime {
    host: ScalpingShadowHost,
    deadline_scheduler: ScalpingDeadlineScheduler,
    controller_authorization: Option<EntryAuthorization>,
}

impl ScalpingResidentRuntime {
    #[must_use]
    pub fn new(host: ScalpingShadowHost) -> Self {
        Self {
            host,
            deadline_scheduler: ScalpingDeadlineScheduler::new(),
            controller_authorization: None,
        }
    }

    /// Refines the legacy command/private/timer/market priority: controller and gate control are
    /// persisted first, then forwarded private facts before their terminal risk proof, followed by
    /// deadline completion/episode projection, timer dispatch, and public market work. An error
    /// aborts the remainder of the cycle, so lower-priority work cannot pass a failed save.
    pub fn drive_cycle(
        &mut self,
        cycle: ScalpingResidentCycle,
    ) -> Result<ScalpingResidentCycleReport, ScalpingResidentRuntimeError> {
        let ScalpingResidentCycle {
            controller,
            mut private_gate,
            risk,
            episode_deadline_completion,
            episode_observation,
            deadline_clock,
            market,
        } = cycle;
        let mut report = ScalpingResidentCycleReport::default();
        if let Some(controller) = controller {
            self.controller_authorization = controller.authorization().cloned();
            if let Some(target) = controller.control() {
                if target == crate::controller::ControlTarget::Running
                    && self.controller_authorization.is_none()
                {
                    return Err(ScalpingResidentRuntimeError::ControllerInput);
                }
                report.controller_control = Some(self.host.on_controller_control(target)?);
            }
            report.controller = Some(controller);
        }
        if private_gate
            .as_ref()
            .is_some_and(|private_gate| private_gate.control.is_some())
            && let Some(control_gate) = private_gate.take()
        {
            report.private_gate = Some(self.host.on_private_gate(&control_gate)?);
        }
        if let Some(private_gate) = private_gate.as_ref() {
            report.private_gate = Some(self.host.on_private_gate(private_gate)?);
        }
        if let Some(risk) = risk {
            report.risk = Some(self.host.on_bound_risk_revaluation(risk)?);
        }
        if let Some(completion) = episode_deadline_completion {
            report.episode_deadline_completion =
                Some(self.host.on_episode_deadline_completion(completion)?);
        }
        if let Some(observation) = episode_observation {
            report.episode = Some(self.host.on_episode_observation(observation)?);
        }
        if let Some(deadline_clock) = deadline_clock {
            report.deadline = Some(
                self.deadline_scheduler
                    .observe(&mut self.host, deadline_clock)?,
            );
        }
        if let Some(market) = market {
            let authorization = self
                .controller_authorization
                .as_ref()
                .filter(|authorization| authorization.is_valid_at(market.decision_at_ms))
                .cloned();
            match authorization {
                Some(authorization) => {
                    report.market = Some(if market.direct_admission {
                        self.host.on_direct_market(
                            market.frame,
                            market.decision_at_ms,
                            authorization,
                            market.evidence,
                        )?
                    } else {
                        self.host.on_market(
                            market.frame,
                            market.decision_at_ms,
                            authorization,
                            market.evidence,
                        )?
                    });
                }
                None => self.controller_authorization = None,
            }
        }
        Ok(report)
    }

    #[must_use]
    pub fn host(&self) -> &ScalpingShadowHost {
        &self.host
    }

    /// Recovery-only bridge used before source polling. It does not clear the resident's cached
    /// authorization or the host's AwaitingPrivate fence.
    pub fn recover_pending_episode_deadline_completion(
        &mut self,
        completion: EpisodeDeadlineCompletion,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingResidentRuntimeError> {
        Ok(self
            .host
            .recover_pending_episode_deadline_completion(completion)?)
    }

    /// Applies the durable semantic acknowledgement of a gateway-confirmed protected entry.
    pub fn confirm_live_entry(
        &mut self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingResidentRuntimeError> {
        Ok(self.host.confirm_live_entry(intent_id, observed_at_ms)?)
    }

    /// Applies the durable semantic rejection of a reconciled IOC no-fill.
    pub fn reject_live_entry(
        &mut self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<ScalpingCoordinatorOutput, ScalpingResidentRuntimeError> {
        Ok(self.host.reject_live_entry(intent_id, observed_at_ms)?)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingResidentRuntimeError {
    #[error("resident received Running without a controller-source authorization")]
    ControllerInput,
    #[error("resident Shadow host transition failed: {0}")]
    Host(#[from] ScalpingShadowHostError),
    #[error("resident deadline scheduling failed: {0}")]
    Deadline(#[from] ScalpingDeadlineSchedulerError),
}
