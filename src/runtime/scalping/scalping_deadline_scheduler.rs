use super::{
    DeadlineTick, ScalpingCoordinatorCheckpoint, ScalpingShadowHost, ScalpingShadowHostError,
    ScalpingShadowHostReport,
};

/// An externally observed clock point. The scheduler cannot manufacture either time or the
/// private-facts identity that explains why the deadline was evaluated.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadlineClockObservation {
    pub now_ms: u64,
    pub root_cause_fact_id: String,
}

/// A persisted deadline selected without changing host state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledDeadline {
    pub deadline_id: String,
    pub generation: u64,
    pub expires_at_ms: u64,
}

/// Result of one clock observation. `Waiting` has also delegated the authoritative clock into the
/// host, but no deadline transition was due and the persisted checkpoint remains unchanged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeadlineSchedulerOutcome {
    NoDeadline,
    Waiting(ScheduledDeadline),
    Fired(Box<ScalpingShadowHostReport>),
}

/// A pure deadline dispatcher for one Shadow host. It remembers only the accepted monotonic
/// observation watermark; deadlines and all resulting state remain owned by the host checkpoint.
#[derive(Debug, Default)]
pub struct ScalpingDeadlineScheduler {
    last_observed_now_ms: Option<u64>,
}

impl ScalpingDeadlineScheduler {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Reads the host checkpoint and delegates every observation for an active deadline into the
    /// host. This closes the restart fence even when the deadline is still in the future. An
    /// accepted observation is consumed even when the host rejects it, so the same authoritative
    /// instant cannot be replayed as a second dispatch attempt.
    pub fn observe(
        &mut self,
        host: &mut ScalpingShadowHost,
        observation: DeadlineClockObservation,
    ) -> Result<DeadlineSchedulerOutcome, ScalpingDeadlineSchedulerError> {
        if observation.now_ms == 0 || observation.root_cause_fact_id.trim().is_empty() {
            return Err(ScalpingDeadlineSchedulerError::Observation);
        }
        if self
            .last_observed_now_ms
            .is_some_and(|previous| observation.now_ms <= previous)
        {
            return Err(ScalpingDeadlineSchedulerError::ClockMonotonic);
        }
        self.last_observed_now_ms = Some(observation.now_ms);

        let checkpoint = host.checkpoint();
        let Some(deadline) = earliest_deadline(&checkpoint) else {
            return Ok(DeadlineSchedulerOutcome::NoDeadline);
        };
        let due = observation.now_ms >= deadline.expires_at_ms;
        match host.tick(DeadlineTick {
            now_ms: observation.now_ms,
            root_cause_fact_id: observation.root_cause_fact_id,
        }) {
            Ok(report) if due => Ok(DeadlineSchedulerOutcome::Fired(Box::new(report))),
            Ok(_) => Ok(DeadlineSchedulerOutcome::Waiting(deadline)),
            Err(ScalpingShadowHostError::RecoveryGeneration) => {
                Err(ScalpingDeadlineSchedulerError::RecoveryFenced)
            }
            Err(error) => Err(ScalpingDeadlineSchedulerError::Host(error)),
        }
    }
}

/// Finds the single earliest active deadline. Ties are stable on deadline ID, exactly like the
/// host's overdue selection, so a dispatcher cannot select a different deadline than `tick`.
#[must_use]
pub fn earliest_deadline(checkpoint: &ScalpingCoordinatorCheckpoint) -> Option<ScheduledDeadline> {
    let episode = checkpoint.strategy.episode.as_ref()?;
    if episode.fault.is_some() {
        return None;
    }
    [
        episode
            .episode_fault_deadline
            .as_ref()
            .map(|armed| &armed.deadline),
        episode.control_fault_deadline.as_ref(),
    ]
    .into_iter()
    .flatten()
    .min_by_key(|deadline| (deadline.expires_at_ms, deadline.deadline_id.as_str()))
    .map(|deadline| ScheduledDeadline {
        deadline_id: deadline.deadline_id.clone(),
        generation: deadline.generation,
        expires_at_ms: deadline.expires_at_ms,
    })
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingDeadlineSchedulerError {
    #[error("deadline scheduler observation lacks an authoritative time or root fact identity")]
    Observation,
    #[error("deadline scheduler observation repeats or regresses its monotonic clock")]
    ClockMonotonic,
    #[error("restored shadow host has not accepted a newer private generation")]
    RecoveryFenced,
    #[error("shadow host rejected deadline dispatch: {0}")]
    Host(ScalpingShadowHostError),
}
