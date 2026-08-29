use crate::{
    controller::ControlTarget,
    domain::Price,
    strategy::scalping::{
        ArmedEpisodeFaultDeadline, DeadlineFired, Direction, EpisodeAction, EpisodeExitReason,
        EpisodeFaultKind, EpisodeProjection, EpisodeState, ExposureState, FaultProjection,
        FaultRecoveryAuthorization, FaultScope, ProtectionState, RiskFact, RiskRevaluation,
        RiskSnapshot, SafetyDeadline, SafetyProjection, ScalpingError, ScalpingState,
    },
};

use super::ScalpingStrategy;

impl ScalpingStrategy {
    pub fn record_risk(&mut self, fact: RiskFact) -> Result<RiskSnapshot, ScalpingError> {
        self.risk.record(fact)
    }

    pub fn require_risk_revaluation(
        &mut self,
        observed_at_ms: u64,
    ) -> Result<RiskSnapshot, ScalpingError> {
        self.risk.require_revaluation(observed_at_ms)
    }

    pub fn apply_risk_revaluation(
        &mut self,
        proof: RiskRevaluation,
    ) -> Result<RiskSnapshot, ScalpingError> {
        self.risk.apply_revaluation(proof)
    }

    #[must_use]
    pub fn risk_snapshot(&self, observed_at_ms: u64) -> RiskSnapshot {
        self.risk.snapshot(observed_at_ms)
    }

    /// Projects anonymous exposure and controller target facts into semantic risk actions. The
    /// result carries no order identifier, venue field, or direct mutation capability.
    pub fn project_episode(
        &mut self,
        target: ControlTarget,
        safety: &SafetyProjection,
        mark_price: Price,
        observed_at_ms: u64,
        fact_id: &str,
    ) -> Result<Vec<EpisodeAction>, ScalpingError> {
        let episode = self.episode.as_mut().ok_or(ScalpingError::Episode)?;
        if fact_id.trim().is_empty() || observed_at_ms < episode.last_observed_at_ms {
            return Err(ScalpingError::Episode);
        }
        if episode.last_fact_id.as_deref() == Some(fact_id) {
            return if observed_at_ms == episode.last_observed_at_ms {
                Ok(Vec::new())
            } else {
                Err(ScalpingError::Episode)
            };
        }
        episode.last_observed_at_ms = observed_at_ms;
        episode.last_fact_id = Some(fact_id.to_owned());

        if let Some(fault) = &episode.fault {
            return Ok(if safety.exposure == ExposureState::Open {
                vec![EpisodeAction::Exit {
                    direction: episode.frozen_intent.direction,
                    reason: if fault.scope
                        == FaultScope::Episode(EpisodeFaultKind::UnprotectedExposure)
                    {
                        EpisodeExitReason::UnprotectedDeadline
                    } else {
                        EpisodeExitReason::SafetyProjectionLost
                    },
                    opportunity_key: episode.frozen_intent.opportunity_key.clone(),
                }]
            } else {
                Vec::new()
            });
        }

        let had_exposure = episode.opened_at_ms.is_some()
            || matches!(
                episode.state,
                EpisodeState::Open | EpisodeState::ExitPending | EpisodeState::StoppedProtected
            );
        let safety_lost = !safety.private_snapshot_ready
            || safety.exposure == ExposureState::Unknown
            || safety.execution_unknown
            || safety.owner_conflict
            || !safety.risk_budget_available;
        if safety_lost {
            episode.state = EpisodeState::ExitPending;
            return Ok(vec![if had_exposure {
                EpisodeAction::Exit {
                    direction: episode.frozen_intent.direction,
                    reason: EpisodeExitReason::SafetyProjectionLost,
                    opportunity_key: episode.frozen_intent.opportunity_key.clone(),
                }
            } else {
                EpisodeAction::CancelEntry {
                    reason: EpisodeExitReason::SafetyProjectionLost,
                }
            }]);
        }
        if safety.exposure == ExposureState::Flat {
            if target == ControlTarget::Running
                && (had_exposure || episode.state == EpisodeState::ExitPending)
            {
                episode.state = EpisodeState::Cooldown;
                episode.retry_not_before_ms = None;
                self.state = ScalpingState::Cooldown {
                    until_ms: observed_at_ms.saturating_add(self.params.cooldown_ms),
                };
                return Ok(Vec::new());
            }
            if target != ControlTarget::Running {
                episode.state = EpisodeState::StoppedFlat;
                return Ok(if had_exposure {
                    Vec::new()
                } else {
                    vec![EpisodeAction::CancelEntry {
                        reason: control_exit_reason(target),
                    }]
                });
            }
            return Ok(Vec::new());
        }

        episode.opened_at_ms.get_or_insert(observed_at_ms);
        if episode.state == EpisodeState::ExitPending {
            return Ok(Vec::new());
        }
        if safety.protection != ProtectionState::Complete
            && !matches!(
                target,
                ControlTarget::FlattenAndStop | ControlTarget::EmergencyStop
            )
        {
            episode.state = EpisodeState::Open;
            return Ok(if episode.episode_fault_deadline.is_none() {
                vec![EpisodeAction::ArmFaultDeadline {
                    kind: EpisodeFaultKind::UnprotectedExposure,
                    no_later_than_ms: observed_at_ms
                        .saturating_add(episode.frozen_intent.max_unprotected_ms),
                }]
            } else {
                Vec::new()
            });
        }
        let cancelled_deadline = episode.episode_fault_deadline.as_ref().map(|armed| {
            EpisodeAction::CancelFaultDeadline {
                deadline_id: armed.deadline.deadline_id.clone(),
            }
        });
        let mut actions = cancelled_deadline.into_iter().collect::<Vec<_>>();
        match target {
            ControlTarget::StopAndProtect => {
                episode.state = EpisodeState::StoppedProtected;
                actions.push(EpisodeAction::MaintainProtection {
                    direction: episode.frozen_intent.direction,
                    hard_stop_distance_bps: episode.frozen_intent.hard_stop_distance_bps,
                });
                Ok(actions)
            }
            ControlTarget::FlattenAndStop | ControlTarget::EmergencyStop => {
                episode.state = EpisodeState::ExitPending;
                actions.push(EpisodeAction::Exit {
                    direction: episode.frozen_intent.direction,
                    reason: control_exit_reason(target),
                    opportunity_key: episode.frozen_intent.opportunity_key.clone(),
                });
                Ok(actions)
            }
            ControlTarget::Running => {
                episode.state = EpisodeState::Open;
                let reason = exit_reason_at(episode, mark_price, observed_at_ms);
                if let Some(reason) = reason {
                    episode.state = EpisodeState::ExitPending;
                    actions.push(EpisodeAction::Exit {
                        direction: episode.frozen_intent.direction,
                        reason,
                        opportunity_key: episode.frozen_intent.opportunity_key.clone(),
                    });
                }
                Ok(actions)
            }
        }
    }

    pub fn arm_episode_fault_deadline(
        &mut self,
        kind: EpisodeFaultKind,
        deadline: SafetyDeadline,
    ) -> Result<(), ScalpingError> {
        deadline.validate()?;
        let episode = self.episode.as_mut().ok_or(ScalpingError::Episode)?;
        if episode.fault.is_some()
            || deadline.armed_at_ms < episode.last_observed_at_ms
            || deadline.expires_at_ms
                > deadline
                    .armed_at_ms
                    .saturating_add(episode.frozen_intent.max_unprotected_ms)
            || episode
                .control_fault_deadline
                .as_ref()
                .is_some_and(|control| control.deadline_id == deadline.deadline_id)
        {
            return Err(ScalpingError::Fault);
        }
        let armed = ArmedEpisodeFaultDeadline { kind, deadline };
        if let Some(existing) = &episode.episode_fault_deadline {
            return if existing == &armed {
                Ok(())
            } else {
                Err(ScalpingError::Fault)
            };
        }
        episode.episode_fault_deadline = Some(armed);
        Ok(())
    }

    pub fn arm_control_fault_deadline(
        &mut self,
        deadline: SafetyDeadline,
    ) -> Result<(), ScalpingError> {
        deadline.validate()?;
        let episode = self.episode.as_mut().ok_or(ScalpingError::Episode)?;
        if episode.fault.is_some()
            || deadline.armed_at_ms < episode.last_observed_at_ms
            || episode
                .episode_fault_deadline
                .as_ref()
                .is_some_and(|armed| armed.deadline.deadline_id == deadline.deadline_id)
        {
            return Err(ScalpingError::Fault);
        }
        if let Some(existing) = &episode.control_fault_deadline {
            return if existing == &deadline {
                Ok(())
            } else {
                Err(ScalpingError::Fault)
            };
        }
        episode.control_fault_deadline = Some(deadline);
        Ok(())
    }

    /// Records only an exact external cancellation completion. Projecting complete protection
    /// requests cancellation but cannot remove the persisted deadline by itself.
    pub fn cancel_episode_fault_deadline(
        &mut self,
        deadline_id: &str,
        generation: u64,
        completed_at_ms: u64,
    ) -> Result<bool, ScalpingError> {
        let episode = self.episode.as_mut().ok_or(ScalpingError::Episode)?;
        if deadline_id.trim().is_empty()
            || generation == 0
            || completed_at_ms < episode.last_observed_at_ms
        {
            return Err(ScalpingError::Fault);
        }
        let Some(armed) = &episode.episode_fault_deadline else {
            return Ok(false);
        };
        if armed.deadline.deadline_id != deadline_id
            || armed.deadline.generation != generation
            || completed_at_ms < armed.deadline.armed_at_ms
        {
            return Err(ScalpingError::Fault);
        }
        episode.episode_fault_deadline = None;
        episode.last_observed_at_ms = completed_at_ms;
        Ok(true)
    }

    /// Applies only the exact persisted deadline. Duplicate delivery is idempotent; a stale,
    /// early, or differently generated firing cannot fault the episode.
    pub fn apply_fault_deadline(&mut self, fired: &DeadlineFired) -> Result<bool, ScalpingError> {
        let episode = self.episode.as_mut().ok_or(ScalpingError::Episode)?;
        if fired.deadline_id.trim().is_empty()
            || fired.generation == 0
            || fired.fired_at_ms == 0
            || fired.root_cause_fact_id.trim().is_empty()
        {
            return Err(ScalpingError::Fault);
        }
        if episode.last_deadline_fired_id.as_deref() == Some(fired.deadline_id.as_str()) {
            return if episode.last_deadline_fired_generation == Some(fired.generation) {
                Ok(false)
            } else {
                Err(ScalpingError::Fault)
            };
        }
        let (scope, expires_at_ms) = if let Some(armed) = &episode.episode_fault_deadline {
            if armed.deadline.deadline_id == fired.deadline_id
                && armed.deadline.generation == fired.generation
            {
                (
                    FaultScope::Episode(armed.kind),
                    armed.deadline.expires_at_ms,
                )
            } else if let Some(control) = &episode.control_fault_deadline {
                if control.deadline_id == fired.deadline_id
                    && control.generation == fired.generation
                {
                    (FaultScope::Control, control.expires_at_ms)
                } else {
                    return Err(ScalpingError::Fault);
                }
            } else {
                return Err(ScalpingError::Fault);
            }
        } else if let Some(control) = &episode.control_fault_deadline {
            if control.deadline_id == fired.deadline_id && control.generation == fired.generation {
                (FaultScope::Control, control.expires_at_ms)
            } else {
                return Err(ScalpingError::Fault);
            }
        } else {
            return Err(ScalpingError::Fault);
        };
        if fired.fired_at_ms < expires_at_ms || episode.fault.is_some() {
            return Err(ScalpingError::Fault);
        }
        match scope {
            FaultScope::Episode(_) => episode.episode_fault_deadline = None,
            FaultScope::Control => episode.control_fault_deadline = None,
        }
        episode.fault = Some(FaultProjection {
            scope,
            deadline_id: fired.deadline_id.clone(),
            generation: fired.generation,
            root_cause_fact_id: fired.root_cause_fact_id.clone(),
            faulted_at_ms: fired.fired_at_ms,
        });
        episode.last_deadline_fired_id = Some(fired.deadline_id.clone());
        episode.last_deadline_fired_generation = Some(fired.generation);
        episode.last_observed_at_ms = episode.last_observed_at_ms.max(fired.fired_at_ms);
        episode.state = match scope {
            FaultScope::Episode(_) => EpisodeState::EpisodeFaulted,
            FaultScope::Control => EpisodeState::ControlFaulted,
        };
        Ok(true)
    }

    pub fn recover_episode_fault(
        &mut self,
        authorization: &FaultRecoveryAuthorization,
        safety: &SafetyProjection,
        observed_at_ms: u64,
        root_cause_fact_id: &str,
    ) -> Result<(), ScalpingError> {
        let episode = self.episode.as_mut().ok_or(ScalpingError::Episode)?;
        if episode.last_recovery_authorization_id.as_deref()
            == Some(authorization.authorization_id.as_str())
            && episode.fault.is_none()
        {
            return Ok(());
        }
        let fault = episode.fault.as_ref().ok_or(ScalpingError::Fault)?;
        if !matches!(fault.scope, FaultScope::Episode(_))
            || !fault_recovery_matches(
                episode,
                fault,
                authorization,
                safety,
                observed_at_ms,
                root_cause_fact_id,
            )
        {
            return Err(ScalpingError::Fault);
        }
        episode.fault = None;
        episode.last_recovery_authorization_id = Some(authorization.authorization_id.clone());
        episode.last_observed_at_ms = observed_at_ms;
        episode.state = match safety.exposure {
            ExposureState::Open => EpisodeState::Open,
            ExposureState::Flat => EpisodeState::Cooldown,
            ExposureState::Unknown => return Err(ScalpingError::Fault),
        };
        if episode.state == EpisodeState::Cooldown {
            self.state = ScalpingState::Cooldown {
                until_ms: observed_at_ms.saturating_add(self.params.cooldown_ms),
            };
        }
        Ok(())
    }

    pub fn recover_control_fault(
        &mut self,
        authorization: &FaultRecoveryAuthorization,
        safety: &SafetyProjection,
        target: ControlTarget,
        observed_at_ms: u64,
        root_cause_fact_id: &str,
    ) -> Result<(), ScalpingError> {
        let episode = self.episode.as_mut().ok_or(ScalpingError::Episode)?;
        if episode.last_recovery_authorization_id.as_deref()
            == Some(authorization.authorization_id.as_str())
            && episode.fault.is_none()
        {
            return Ok(());
        }
        let fault = episode.fault.as_ref().ok_or(ScalpingError::Fault)?;
        if fault.scope != FaultScope::Control
            || !fault_recovery_matches(
                episode,
                fault,
                authorization,
                safety,
                observed_at_ms,
                root_cause_fact_id,
            )
            || (safety.exposure == ExposureState::Open
                && matches!(
                    target,
                    ControlTarget::FlattenAndStop | ControlTarget::EmergencyStop
                ))
        {
            return Err(ScalpingError::Fault);
        }
        episode.fault = None;
        episode.last_recovery_authorization_id = Some(authorization.authorization_id.clone());
        episode.last_observed_at_ms = observed_at_ms;
        episode.state = match (target, safety.exposure) {
            (_, ExposureState::Flat) => EpisodeState::StoppedFlat,
            (ControlTarget::Running, ExposureState::Open) => EpisodeState::Open,
            (ControlTarget::StopAndProtect, ExposureState::Open) => EpisodeState::StoppedProtected,
            (_, ExposureState::Open | ExposureState::Unknown) => return Err(ScalpingError::Fault),
        };
        Ok(())
    }
}

fn fault_recovery_matches(
    episode: &EpisodeProjection,
    fault: &FaultProjection,
    authorization: &FaultRecoveryAuthorization,
    safety: &SafetyProjection,
    observed_at_ms: u64,
    root_cause_fact_id: &str,
) -> bool {
    !authorization.authorization_id.trim().is_empty()
        && authorization.episode_id == episode.episode_id
        && authorization.scope == fault.scope
        && authorization.fault_generation == fault.generation
        && authorization.root_cause_fact_id == fault.root_cause_fact_id
        && root_cause_fact_id == fault.root_cause_fact_id
        && authorization.valid_until_ms > observed_at_ms
        && observed_at_ms >= fault.faulted_at_ms
        && observed_at_ms >= episode.last_observed_at_ms
        && safety.private_snapshot_ready
        && safety.exposure != ExposureState::Unknown
        && !safety.execution_unknown
        && !safety.owner_conflict
        && safety.risk_budget_available
        && (safety.exposure == ExposureState::Flat
            || safety.protection == ProtectionState::Complete)
}

fn control_exit_reason(target: ControlTarget) -> EpisodeExitReason {
    match target {
        ControlTarget::StopAndProtect => EpisodeExitReason::StopAndProtect,
        ControlTarget::FlattenAndStop => EpisodeExitReason::FlattenAndStop,
        ControlTarget::EmergencyStop => EpisodeExitReason::EmergencyStop,
        ControlTarget::Running => EpisodeExitReason::SafetyProjectionLost,
    }
}

fn exit_reason_at(
    episode: &EpisodeProjection,
    mark_price: Price,
    observed_at_ms: u64,
) -> Option<EpisodeExitReason> {
    let reference = episode.frozen_intent.reference_price.value();
    let mark = mark_price.value();
    let scale = rust_decimal::Decimal::new(10_000, 0);
    let stop_ratio = episode.frozen_intent.hard_stop_distance_bps / scale;
    let target_ratio = episode.frozen_intent.target_distance_bps / scale;
    let (hard_stop_hit, target_hit) = match episode.frozen_intent.direction {
        Direction::Long => (
            mark <= reference * (rust_decimal::Decimal::ONE - stop_ratio),
            mark >= reference * (rust_decimal::Decimal::ONE + target_ratio),
        ),
        Direction::Short => (
            mark >= reference * (rust_decimal::Decimal::ONE + stop_ratio),
            mark <= reference * (rust_decimal::Decimal::ONE - target_ratio),
        ),
    };
    if hard_stop_hit {
        Some(EpisodeExitReason::HardStop)
    } else if target_hit {
        Some(EpisodeExitReason::TargetReached)
    } else if episode.opened_at_ms.is_some_and(|opened_at_ms| {
        observed_at_ms >= opened_at_ms.saturating_add(episode.frozen_intent.max_hold_ms)
    }) {
        Some(EpisodeExitReason::MaxHoldElapsed)
    } else {
        None
    }
}
