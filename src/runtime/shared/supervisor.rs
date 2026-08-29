/// Pure decision contract for supervising one strategy instance. It does not own a gateway,
/// sibling instance, task, or writer; composition code must execute its reported follow-ups.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstanceKey {
    pub strategy_instance_id: String,
    pub run_id: String,
}

impl InstanceKey {
    pub fn validate(&self) -> Result<(), SupervisorError> {
        if self.strategy_instance_id.trim().is_empty() || self.run_id.trim().is_empty() {
            return Err(SupervisorError::Instance);
        }
        Ok(())
    }
}

/// This carries only the target instance identity. Gateway shutdown and sibling shutdown are not
/// representable by the supervisor contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstanceShutdownDecision {
    pub instance: InstanceKey,
}

pub fn request_instance_shutdown(
    instance: InstanceKey,
) -> Result<InstanceShutdownDecision, SupervisorError> {
    instance.validate()?;
    Ok(InstanceShutdownDecision { instance })
}

/// A shutdown remains pending until authoritative composition reports either a flat account or
/// continuous protection custody. A normal instance exit cannot treat a submitted stop as done.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownState {
    Pending,
    StoppedFlat,
    StoppedProtected,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstanceShutdownReport {
    pub decision: InstanceShutdownDecision,
    pub state: ShutdownState,
}

impl InstanceShutdownReport {
    pub fn converged(&self) -> bool {
        matches!(
            self.state,
            ShutdownState::StoppedFlat | ShutdownState::StoppedProtected
        )
    }
}

pub fn report_shutdown_convergence(
    decision: InstanceShutdownDecision,
    state: ShutdownState,
) -> InstanceShutdownReport {
    InstanceShutdownReport { decision, state }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActionKind {
    Place,
    Cancel,
    Reduce,
    Protect,
}

/// An action is identified before parallel submission. The original slice position, rather than
/// completion order, is the only ordering used when committing results to strategy state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupervisorAction {
    pub action_id: String,
    pub kind: ActionKind,
}

impl SupervisorAction {
    fn validate(&self) -> Result<(), SupervisorError> {
        if self.action_id.trim().is_empty() {
            return Err(SupervisorError::Action);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubmissionOutcome {
    Confirmed,
    Rejected { reason: String },
    Unknown,
    Transient { reason: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParallelCompletion {
    pub action_index: usize,
    pub outcome: SubmissionOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RejectedAction {
    pub action: SupervisorAction,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransientAction {
    pub action: SupervisorAction,
    pub reason: String,
}

/// A deterministic reconciliation of concurrently completed submissions. `committed` and
/// `rejected` retain original action order. `compensate_confirmed_places` is non-empty only when
/// an UNKNOWN or transient result requires recovery, and never includes an unconfirmed action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubmissionReport {
    pub committed: Vec<SupervisorAction>,
    pub rejected: Vec<RejectedAction>,
    pub fenced_unknown: Vec<SupervisorAction>,
    pub transient: Vec<TransientAction>,
    pub compensate_confirmed_places: Vec<SupervisorAction>,
}

impl SubmissionReport {
    pub fn recovery_required(&self) -> bool {
        !self.fenced_unknown.is_empty() || !self.transient.is_empty()
    }
}

/// Accepts parallel completion results and produces deterministic strategy-facing effects. It
/// neither submits actions nor emits compensations, so it cannot acquire mutation authority.
pub fn report_parallel_submission(
    actions: &[SupervisorAction],
    completions: &[ParallelCompletion],
) -> Result<SubmissionReport, SupervisorError> {
    let mut action_ids = std::collections::BTreeSet::new();
    let mut ordered = Vec::with_capacity(actions.len());
    for action in actions {
        action.validate()?;
        if !action_ids.insert(&action.action_id) {
            return Err(SupervisorError::DuplicateAction);
        }
        ordered.push(None);
    }
    for completion in completions {
        let slot = ordered
            .get_mut(completion.action_index)
            .ok_or(SupervisorError::CompletionIndex)?;
        if slot.replace(completion.outcome.clone()).is_some() {
            return Err(SupervisorError::DuplicateCompletion);
        }
    }
    if ordered.iter().any(Option::is_none) {
        return Err(SupervisorError::MissingCompletion);
    }

    let mut committed = Vec::new();
    let mut rejected = Vec::new();
    let mut fenced_unknown = Vec::new();
    let mut transient = Vec::new();
    for (action, outcome) in actions.iter().cloned().zip(ordered) {
        match outcome.ok_or(SupervisorError::MissingCompletion)? {
            SubmissionOutcome::Confirmed => committed.push(action),
            SubmissionOutcome::Rejected { reason } => {
                rejected.push(RejectedAction { action, reason })
            }
            SubmissionOutcome::Unknown => fenced_unknown.push(action),
            SubmissionOutcome::Transient { reason } => {
                transient.push(TransientAction { action, reason })
            }
        }
    }
    let recovery_required = !fenced_unknown.is_empty() || !transient.is_empty();
    let compensate_confirmed_places = if recovery_required {
        committed
            .iter()
            .filter(|action| action.kind == ActionKind::Place)
            .cloned()
            .collect()
    } else {
        Vec::new()
    };
    Ok(SubmissionReport {
        committed,
        rejected,
        fenced_unknown,
        transient,
        compensate_confirmed_places,
    })
}

/// A lifecycle fault is classified before a composition layer decides whether it must reconcile,
/// apply a control target, or merely keep entry disarmed. It contains no market, account, or
/// exchange detail.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleFault {
    CapabilityGap,
    CapabilityGenerationChanged,
    PrivateReconciliationTransient,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LifecycleInput {
    pub active_episode: bool,
    pub entry_armed: bool,
    pub now_ms: u64,
    pub admission_deadline_ms: Option<u64>,
    pub fault: Option<LifecycleFault>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntryDisposition {
    Armed,
    Disarmed,
    Cooldown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlDisposition {
    None,
    StopAndProtect,
}

/// Deterministic, fail-closed convergence for the lifecycle states that the frozen scalper kept
/// nonfatal: an expired admission cools down; capability loss on an active episode requests
/// protection; a transient private reconciliation window disarms entry without guessing facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LifecycleReport {
    pub entry: EntryDisposition,
    pub control: ControlDisposition,
}

pub fn report_lifecycle(input: LifecycleInput) -> LifecycleReport {
    match input.fault {
        Some(LifecycleFault::CapabilityGap | LifecycleFault::CapabilityGenerationChanged)
            if input.active_episode =>
        {
            LifecycleReport {
                entry: EntryDisposition::Disarmed,
                control: ControlDisposition::StopAndProtect,
            }
        }
        Some(
            LifecycleFault::CapabilityGap
            | LifecycleFault::CapabilityGenerationChanged
            | LifecycleFault::PrivateReconciliationTransient,
        ) => LifecycleReport {
            entry: EntryDisposition::Disarmed,
            control: ControlDisposition::None,
        },
        None if input
            .admission_deadline_ms
            .is_some_and(|deadline| deadline <= input.now_ms) =>
        {
            LifecycleReport {
                entry: EntryDisposition::Cooldown,
                control: ControlDisposition::None,
            }
        }
        None if input.entry_armed => LifecycleReport {
            entry: EntryDisposition::Armed,
            control: ControlDisposition::None,
        },
        None => LifecycleReport {
            entry: EntryDisposition::Disarmed,
            control: ControlDisposition::None,
        },
    }
}

/// A normalized failure supplied by the runtime or its cleanup path. The supervisor preserves
/// both channels because cleanup loss must not hide the original runtime fault.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupervisorFailure {
    pub message: String,
}

impl SupervisorFailure {
    pub fn new(message: impl Into<String>) -> Result<Self, SupervisorError> {
        let message = message.into();
        if message.trim().is_empty() {
            return Err(SupervisorError::Failure);
        }
        Ok(Self { message })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TerminationReport {
    pub runtime_error: Option<SupervisorFailure>,
    pub cleanup_errors: Vec<SupervisorFailure>,
}

pub fn report_termination(
    runtime_error: Option<SupervisorFailure>,
    cleanup_errors: Vec<SupervisorFailure>,
) -> TerminationReport {
    TerminationReport {
        runtime_error,
        cleanup_errors,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SupervisorError {
    #[error("runtime supervisor instance identity is invalid")]
    Instance,
    #[error("runtime supervisor action identity is invalid")]
    Action,
    #[error("runtime supervisor batch reuses an action identity")]
    DuplicateAction,
    #[error("parallel completion refers to an action outside the submitted batch")]
    CompletionIndex,
    #[error("parallel completion contains the same action more than once")]
    DuplicateCompletion,
    #[error("parallel completion is missing an action result")]
    MissingCompletion,
    #[error("runtime supervisor failure message is invalid")]
    Failure,
}
