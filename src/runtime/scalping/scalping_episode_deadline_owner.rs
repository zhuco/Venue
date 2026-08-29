use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    runtime::{
        DeadlineClockObservation, EpisodeDeadlineCompletion, EpisodeDeadlineOutcome,
        EpisodeObservation, EpisodeProjectionReceipt, SCALPING_COORDINATOR_SCHEMA_VERSION,
        ScalpingCoordinatorCheckpoint,
    },
    storage::{ProjectionStore, StorageError},
    strategy::scalping::{EpisodeAction, SafetyDeadline, StrategyBinding},
};

use super::scalping_coordinator::episode_observation_fact_id;

pub const SCALPING_EPISODE_DEADLINE_OWNER_SCHEMA_VERSION: u16 = 2;
const MAX_EPISODE_ACTIONS: usize = 8;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EpisodeDeadlineOwnerCursor {
    pub episode_id: String,
    pub generation: u64,
    pub observed_at_ms: u64,
    pub mark_generation: u64,
    pub mark_received_at_ms: u64,
    pub mark_exchange_time_ms: u64,
    pub private_root_cause_fact_id: String,
    pub observation_fact_id: String,
    pub receipt_identity_digest: String,
    pub deadline_action_id: Option<String>,
    pub ignored_actions_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PendingEpisodeDeadlineCompletion {
    pub action_id: String,
    pub completion: EpisodeDeadlineCompletion,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EpisodeDeadlineOwnerCheckpoint {
    pub schema_version: u16,
    pub binding_digest: String,
    pub last_clock_ms: Option<u64>,
    pub cursor: Option<EpisodeDeadlineOwnerCursor>,
    pub pending: Option<PendingEpisodeDeadlineCompletion>,
    pub last_acked_completion_fact_id: Option<String>,
    pub state_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EpisodeDeadlineOwnerTurn {
    NoDeadlineAction,
    Persisted(EpisodeDeadlineCompletion),
    PendingReplay(EpisodeDeadlineCompletion),
    Acknowledged { completion_fact_id: String },
}

#[derive(Debug)]
pub struct ScalpingEpisodeDeadlineOwner {
    store: ProjectionStore,
    binding: StrategyBinding,
    checkpoint: EpisodeDeadlineOwnerCheckpoint,
}

impl ScalpingEpisodeDeadlineOwner {
    pub fn open_or_restore(
        path: impl AsRef<Path>,
        binding: StrategyBinding,
    ) -> Result<Self, EpisodeDeadlineOwnerError> {
        binding
            .validate()
            .map_err(|_| EpisodeDeadlineOwnerError::Binding)?;
        let store = ProjectionStore::new(path.as_ref().to_path_buf());
        let binding_digest = binding.digest();
        let checkpoint = match store.load::<EpisodeDeadlineOwnerCheckpoint>()? {
            Some(checkpoint) => {
                validate_owner_checkpoint(&checkpoint, &binding_digest)?;
                checkpoint
            }
            None => {
                let checkpoint = seal_checkpoint(EpisodeDeadlineOwnerCheckpoint {
                    schema_version: SCALPING_EPISODE_DEADLINE_OWNER_SCHEMA_VERSION,
                    binding_digest,
                    last_clock_ms: None,
                    cursor: None,
                    pending: None,
                    last_acked_completion_fact_id: None,
                    state_digest: String::new(),
                })?;
                store.save(&checkpoint)?;
                checkpoint
            }
        };
        Ok(Self {
            store,
            binding,
            checkpoint,
        })
    }

    /// Processes at most one arm/cancel action. A newly created completion is durable before it
    /// is returned; a pending completion remains the only output until the host checkpoint
    /// acknowledges the exact fact.
    pub fn turn(
        &mut self,
        host: &ScalpingCoordinatorCheckpoint,
        clock: DeadlineClockObservation,
    ) -> Result<EpisodeDeadlineOwnerTurn, EpisodeDeadlineOwnerError> {
        let receipt = validate_host_checkpoint(&self.binding, host)?;
        validate_clock(&clock, receipt, self.checkpoint.last_clock_ms)?;
        let selected = select_deadline_action(receipt)?;
        let ignored_actions_digest = ignored_actions_digest(&receipt.actions)?;
        let receipt_identity_digest = receipt_identity_digest(receipt)?;

        if let Some(pending) = self.checkpoint.pending.clone() {
            return self.resolve_pending(
                host,
                receipt,
                selected.as_ref(),
                &ignored_actions_digest,
                &receipt_identity_digest,
                pending,
                clock,
            );
        }

        self.validate_cursor_progress(
            host,
            receipt,
            selected.as_ref(),
            &ignored_actions_digest,
            &receipt_identity_digest,
        )?;
        if self.same_cursor(receipt, &receipt_identity_digest) {
            self.advance_clock(clock.now_ms)?;
            return Ok(EpisodeDeadlineOwnerTurn::NoDeadlineAction);
        }
        if host.last_episode_deadline_completion.is_some() {
            return Err(EpisodeDeadlineOwnerError::HostCheckpoint);
        }

        let deadline_action_id = selected.as_ref().map(|selected| selected.action_id.clone());
        self.checkpoint.cursor = Some(EpisodeDeadlineOwnerCursor {
            episode_id: receipt.episode_id.clone(),
            generation: receipt.generation,
            observed_at_ms: receipt.observed_at_ms,
            mark_generation: receipt.mark_generation,
            mark_received_at_ms: receipt.mark_received_at_ms,
            mark_exchange_time_ms: receipt.mark_exchange_time_ms,
            private_root_cause_fact_id: receipt.private_root_cause_fact_id.clone(),
            observation_fact_id: receipt.observation_fact_id.clone(),
            receipt_identity_digest,
            deadline_action_id,
            ignored_actions_digest,
        });
        self.checkpoint.last_clock_ms = Some(clock.now_ms);
        self.checkpoint.last_acked_completion_fact_id = None;
        let Some(selected) = selected else {
            self.persist()?;
            return Ok(EpisodeDeadlineOwnerTurn::NoDeadlineAction);
        };
        let completion = build_completion(host, receipt, &selected, &clock)?;
        self.checkpoint.pending = Some(PendingEpisodeDeadlineCompletion {
            action_id: selected.action_id,
            completion: completion.clone(),
        });
        self.persist()?;
        Ok(EpisodeDeadlineOwnerTurn::Persisted(completion))
    }

    #[must_use]
    pub fn checkpoint(&self) -> EpisodeDeadlineOwnerCheckpoint {
        self.checkpoint.clone()
    }

    #[allow(clippy::too_many_arguments)]
    fn resolve_pending(
        &mut self,
        host: &ScalpingCoordinatorCheckpoint,
        receipt: &EpisodeProjectionReceipt,
        selected: Option<&SelectedDeadlineAction>,
        ignored_actions_digest: &str,
        receipt_identity_digest: &str,
        pending: PendingEpisodeDeadlineCompletion,
        clock: DeadlineClockObservation,
    ) -> Result<EpisodeDeadlineOwnerTurn, EpisodeDeadlineOwnerError> {
        let cursor = self
            .checkpoint
            .cursor
            .as_ref()
            .ok_or(EpisodeDeadlineOwnerError::OwnerCheckpoint)?;
        if !cursor_matches_receipt(cursor, receipt, receipt_identity_digest)
            || cursor.ignored_actions_digest != ignored_actions_digest
            || cursor.deadline_action_id.as_deref() != Some(pending.action_id.as_str())
            || clock.now_ms < pending.completion.completed_at_ms
        {
            return Err(EpisodeDeadlineOwnerError::PendingConflict);
        }
        if host.last_episode_deadline_completion.as_ref() == Some(&pending.completion) {
            if selected.is_some() || !completion_applied_to_host(host, &pending.completion) {
                return Err(EpisodeDeadlineOwnerError::PendingConflict);
            }
            self.checkpoint.pending = None;
            self.checkpoint.last_clock_ms = Some(clock.now_ms);
            self.checkpoint.last_acked_completion_fact_id =
                Some(pending.completion.completion_fact_id.clone());
            self.persist()?;
            return Ok(EpisodeDeadlineOwnerTurn::Acknowledged {
                completion_fact_id: pending.completion.completion_fact_id,
            });
        }
        if host.last_episode_deadline_completion.is_some()
            || selected.is_none_or(|action| action.action_id != pending.action_id)
        {
            return Err(EpisodeDeadlineOwnerError::PendingConflict);
        }
        self.checkpoint.last_clock_ms = Some(clock.now_ms);
        self.persist()?;
        Ok(EpisodeDeadlineOwnerTurn::PendingReplay(pending.completion))
    }

    fn validate_cursor_progress(
        &self,
        host: &ScalpingCoordinatorCheckpoint,
        receipt: &EpisodeProjectionReceipt,
        selected: Option<&SelectedDeadlineAction>,
        ignored_actions_digest: &str,
        receipt_identity_digest: &str,
    ) -> Result<(), EpisodeDeadlineOwnerError> {
        let Some(cursor) = &self.checkpoint.cursor else {
            return Ok(());
        };
        if private_cursor_regressed(cursor, receipt) || mark_cursor_regressed(cursor, receipt) {
            return Err(EpisodeDeadlineOwnerError::CursorRegression);
        }
        let same_private = same_private_watermark(cursor, receipt);
        let same_mark = same_mark_watermark(cursor, receipt);
        if same_private && cursor.private_root_cause_fact_id != receipt.private_root_cause_fact_id {
            return Err(EpisodeDeadlineOwnerError::Equivocation);
        }
        if same_private && same_mark {
            if !cursor_matches_receipt(cursor, receipt, receipt_identity_digest)
                || cursor.ignored_actions_digest != ignored_actions_digest
            {
                return Err(EpisodeDeadlineOwnerError::Equivocation);
            }
            match (&cursor.deadline_action_id, selected) {
                (None, None) => {}
                (Some(_), None)
                    if host.last_episode_deadline_completion.as_ref().is_some_and(
                        |completion| {
                            self.checkpoint.last_acked_completion_fact_id.as_deref()
                                == Some(completion.completion_fact_id.as_str())
                        },
                    ) => {}
                _ => return Err(EpisodeDeadlineOwnerError::Equivocation),
            }
            return Ok(());
        }
        if self.checkpoint.last_acked_completion_fact_id.is_none()
            && cursor.deadline_action_id.is_some()
        {
            return Err(EpisodeDeadlineOwnerError::CursorRegression);
        }
        Ok(())
    }

    fn same_cursor(&self, receipt: &EpisodeProjectionReceipt, identity_digest: &str) -> bool {
        self.checkpoint.cursor.as_ref().is_some_and(|cursor| {
            cursor.generation == receipt.generation
                && cursor.observed_at_ms == receipt.observed_at_ms
                && cursor.mark_generation == receipt.mark_generation
                && cursor.mark_received_at_ms == receipt.mark_received_at_ms
                && cursor.mark_exchange_time_ms == receipt.mark_exchange_time_ms
                && cursor.receipt_identity_digest == identity_digest
        })
    }

    fn advance_clock(&mut self, now_ms: u64) -> Result<(), EpisodeDeadlineOwnerError> {
        self.checkpoint.last_clock_ms = Some(now_ms);
        self.persist()
    }

    fn persist(&mut self) -> Result<(), EpisodeDeadlineOwnerError> {
        let sealed = seal_checkpoint(self.checkpoint.clone())?;
        self.store.save(&sealed)?;
        self.checkpoint = sealed;
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct SelectedDeadlineAction {
    action_id: String,
    action: EpisodeAction,
}

fn validate_host_checkpoint<'a>(
    binding: &StrategyBinding,
    host: &'a ScalpingCoordinatorCheckpoint,
) -> Result<&'a EpisodeProjectionReceipt, EpisodeDeadlineOwnerError> {
    let binding_digest = binding.digest();
    let receipt = host
        .last_episode_projection
        .as_ref()
        .ok_or(EpisodeDeadlineOwnerError::HostCheckpoint)?;
    let episode = host
        .strategy
        .episode
        .as_ref()
        .ok_or(EpisodeDeadlineOwnerError::HostCheckpoint)?;
    if host.schema_version != SCALPING_COORDINATOR_SCHEMA_VERSION
        || host.strategy.binding_digest != binding_digest
        || receipt.binding_digest != binding_digest
        || receipt.episode_id != episode.episode_id
        || receipt.generation == 0
        || receipt.observed_at_ms == 0
        || receipt.private_root_cause_fact_id.trim().is_empty()
        || receipt.observation_fact_id.trim().is_empty()
        || receipt.mark_symbol != binding.symbol
        || receipt.mark_generation == 0
        || receipt.mark_received_at_ms == 0
        || receipt.mark_exchange_time_ms == 0
        || receipt.mark_exchange_time_ms > receipt.mark_received_at_ms
        || receipt.mark_received_at_ms > receipt.observed_at_ms
        || host.last_private_generation != Some(receipt.generation)
        || host.last_private_observed_at_ms != Some(receipt.observed_at_ms)
        || host.last_private_root_cause_fact_id.as_deref()
            != Some(receipt.private_root_cause_fact_id.as_str())
        || receipt.actions.len() > MAX_EPISODE_ACTIONS
        || episode_observation_fact_id(&receipt_observation(receipt)).ok()
            != Some(receipt.observation_fact_id.clone())
    {
        return Err(EpisodeDeadlineOwnerError::HostCheckpoint);
    }
    if let Some(completion) = &host.last_episode_deadline_completion
        && (completion.episode_id != receipt.episode_id
            || completion.observation_generation != receipt.generation
            || completion.observation_observed_at_ms != receipt.observed_at_ms
            || completion.private_root_cause_fact_id != receipt.private_root_cause_fact_id
            || completion.observation_fact_id != receipt.observation_fact_id
            || completion.completion_fact_id.trim().is_empty())
    {
        return Err(EpisodeDeadlineOwnerError::HostCheckpoint);
    }
    Ok(receipt)
}

fn validate_clock(
    clock: &DeadlineClockObservation,
    receipt: &EpisodeProjectionReceipt,
    last_clock_ms: Option<u64>,
) -> Result<(), EpisodeDeadlineOwnerError> {
    if clock.now_ms == 0
        || clock.root_cause_fact_id != receipt.private_root_cause_fact_id
        || clock.now_ms < receipt.observed_at_ms
        || last_clock_ms.is_some_and(|last| clock.now_ms < last)
    {
        return Err(EpisodeDeadlineOwnerError::Clock);
    }
    Ok(())
}

fn select_deadline_action(
    receipt: &EpisodeProjectionReceipt,
) -> Result<Option<SelectedDeadlineAction>, EpisodeDeadlineOwnerError> {
    let mut selected = None;
    for (index, action) in receipt.actions.iter().enumerate() {
        if !matches!(
            action,
            EpisodeAction::ArmFaultDeadline { .. } | EpisodeAction::CancelFaultDeadline { .. }
        ) {
            continue;
        }
        if selected.is_some() {
            return Err(EpisodeDeadlineOwnerError::ActionBound);
        }
        selected = Some(SelectedDeadlineAction {
            action_id: action_id(receipt, index, action)?,
            action: action.clone(),
        });
    }
    Ok(selected)
}

fn build_completion(
    host: &ScalpingCoordinatorCheckpoint,
    receipt: &EpisodeProjectionReceipt,
    selected: &SelectedDeadlineAction,
    clock: &DeadlineClockObservation,
) -> Result<EpisodeDeadlineCompletion, EpisodeDeadlineOwnerError> {
    let outcome = match &selected.action {
        EpisodeAction::ArmFaultDeadline {
            kind,
            no_later_than_ms,
        } => {
            if *no_later_than_ms <= receipt.observed_at_ms
                || clock.now_ms >= *no_later_than_ms
                || host
                    .strategy
                    .episode
                    .as_ref()
                    .is_none_or(|episode| episode.episode_fault_deadline.is_some())
            {
                return Err(EpisodeDeadlineOwnerError::LateOrConflictingAction);
            }
            EpisodeDeadlineOutcome::Armed {
                kind: *kind,
                deadline: SafetyDeadline {
                    deadline_id: format!("episode-fault-deadline:{}", selected.action_id),
                    generation: receipt.generation,
                    armed_at_ms: receipt.observed_at_ms,
                    expires_at_ms: *no_later_than_ms,
                },
            }
        }
        EpisodeAction::CancelFaultDeadline { deadline_id } => {
            let active = host
                .strategy
                .episode
                .as_ref()
                .and_then(|episode| episode.episode_fault_deadline.as_ref())
                .ok_or(EpisodeDeadlineOwnerError::LateOrConflictingAction)?;
            if active.deadline.deadline_id != *deadline_id
                || active.deadline.generation == 0
                || clock.now_ms >= active.deadline.expires_at_ms
            {
                return Err(EpisodeDeadlineOwnerError::LateOrConflictingAction);
            }
            EpisodeDeadlineOutcome::Cancelled {
                deadline_id: deadline_id.clone(),
                deadline_generation: active.deadline.generation,
            }
        }
        _ => return Err(EpisodeDeadlineOwnerError::ActionBound),
    };
    let completion_fact_id = completion_fact_id(&selected.action_id, &outcome)?;
    Ok(EpisodeDeadlineCompletion {
        episode_id: receipt.episode_id.clone(),
        observation_generation: receipt.generation,
        observation_observed_at_ms: receipt.observed_at_ms,
        private_root_cause_fact_id: receipt.private_root_cause_fact_id.clone(),
        observation_fact_id: receipt.observation_fact_id.clone(),
        completion_fact_id,
        completed_at_ms: clock.now_ms,
        outcome,
    })
}

fn completion_applied_to_host(
    host: &ScalpingCoordinatorCheckpoint,
    completion: &EpisodeDeadlineCompletion,
) -> bool {
    let Some(episode) = &host.strategy.episode else {
        return false;
    };
    match &completion.outcome {
        EpisodeDeadlineOutcome::Armed { kind, deadline } => episode
            .episode_fault_deadline
            .as_ref()
            .is_some_and(|armed| armed.kind == *kind && armed.deadline == *deadline),
        EpisodeDeadlineOutcome::Cancelled { .. } => episode.episode_fault_deadline.is_none(),
    }
}

fn cursor_matches_receipt(
    cursor: &EpisodeDeadlineOwnerCursor,
    receipt: &EpisodeProjectionReceipt,
    receipt_identity_digest: &str,
) -> bool {
    cursor.episode_id == receipt.episode_id
        && cursor.generation == receipt.generation
        && cursor.observed_at_ms == receipt.observed_at_ms
        && cursor.mark_generation == receipt.mark_generation
        && cursor.mark_received_at_ms == receipt.mark_received_at_ms
        && cursor.mark_exchange_time_ms == receipt.mark_exchange_time_ms
        && cursor.private_root_cause_fact_id == receipt.private_root_cause_fact_id
        && cursor.observation_fact_id == receipt.observation_fact_id
        && cursor.receipt_identity_digest == receipt_identity_digest
}

fn same_private_watermark(
    cursor: &EpisodeDeadlineOwnerCursor,
    receipt: &EpisodeProjectionReceipt,
) -> bool {
    cursor.generation == receipt.generation && cursor.observed_at_ms == receipt.observed_at_ms
}

fn same_mark_watermark(
    cursor: &EpisodeDeadlineOwnerCursor,
    receipt: &EpisodeProjectionReceipt,
) -> bool {
    cursor.mark_generation == receipt.mark_generation
        && cursor.mark_received_at_ms == receipt.mark_received_at_ms
        && cursor.mark_exchange_time_ms == receipt.mark_exchange_time_ms
}

fn private_cursor_regressed(
    cursor: &EpisodeDeadlineOwnerCursor,
    receipt: &EpisodeProjectionReceipt,
) -> bool {
    receipt.generation < cursor.generation || receipt.observed_at_ms < cursor.observed_at_ms
}

fn mark_cursor_regressed(
    cursor: &EpisodeDeadlineOwnerCursor,
    receipt: &EpisodeProjectionReceipt,
) -> bool {
    receipt.mark_generation < cursor.mark_generation
        || receipt.mark_received_at_ms < cursor.mark_received_at_ms
        || receipt.mark_exchange_time_ms < cursor.mark_exchange_time_ms
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

fn receipt_identity_digest(
    receipt: &EpisodeProjectionReceipt,
) -> Result<String, EpisodeDeadlineOwnerError> {
    digest(&(
        &receipt.binding_digest,
        &receipt.episode_id,
        receipt.generation,
        receipt.observed_at_ms,
        &receipt.private_root_cause_fact_id,
        &receipt.observation_fact_id,
        &receipt.mark_symbol,
        receipt.mark_generation,
        receipt.mark_received_at_ms,
        receipt.mark_exchange_time_ms,
        receipt.mark_price,
    ))
}

fn action_id(
    receipt: &EpisodeProjectionReceipt,
    index: usize,
    action: &EpisodeAction,
) -> Result<String, EpisodeDeadlineOwnerError> {
    Ok(format!(
        "episode-deadline-action:{}",
        digest(&(receipt_identity_digest(receipt)?, index, action))?
    ))
}

fn ignored_actions_digest(actions: &[EpisodeAction]) -> Result<String, EpisodeDeadlineOwnerError> {
    let ignored = actions
        .iter()
        .filter(|action| {
            !matches!(
                action,
                EpisodeAction::ArmFaultDeadline { .. } | EpisodeAction::CancelFaultDeadline { .. }
            )
        })
        .collect::<Vec<_>>();
    digest(&ignored)
}

fn completion_fact_id(
    action_id: &str,
    outcome: &EpisodeDeadlineOutcome,
) -> Result<String, EpisodeDeadlineOwnerError> {
    Ok(format!(
        "episode-deadline-completion:{}",
        digest(&(action_id, outcome))?
    ))
}

fn digest(value: &impl Serialize) -> Result<String, EpisodeDeadlineOwnerError> {
    let encoded = serde_json::to_vec(value).map_err(EpisodeDeadlineOwnerError::Encode)?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

fn seal_checkpoint(
    mut checkpoint: EpisodeDeadlineOwnerCheckpoint,
) -> Result<EpisodeDeadlineOwnerCheckpoint, EpisodeDeadlineOwnerError> {
    checkpoint.state_digest.clear();
    checkpoint.state_digest = digest(&checkpoint)?;
    Ok(checkpoint)
}

fn validate_owner_checkpoint(
    checkpoint: &EpisodeDeadlineOwnerCheckpoint,
    binding_digest: &str,
) -> Result<(), EpisodeDeadlineOwnerError> {
    if checkpoint.schema_version != SCALPING_EPISODE_DEADLINE_OWNER_SCHEMA_VERSION
        || checkpoint.binding_digest != binding_digest
        || checkpoint.state_digest != seal_checkpoint(checkpoint.clone())?.state_digest
        || checkpoint.last_clock_ms.is_some_and(|value| value == 0)
        || checkpoint.pending.is_some() && checkpoint.cursor.is_none()
        || checkpoint.pending.as_ref().is_some_and(|pending| {
            pending.action_id.trim().is_empty()
                || pending.completion.completion_fact_id.trim().is_empty()
                || checkpoint.cursor.as_ref().is_none_or(|cursor| {
                    cursor.deadline_action_id.as_deref() != Some(pending.action_id.as_str())
                })
        })
        || checkpoint.cursor.as_ref().is_some_and(|cursor| {
            cursor.episode_id.trim().is_empty()
                || cursor.generation == 0
                || cursor.observed_at_ms == 0
                || cursor.mark_generation == 0
                || cursor.mark_received_at_ms == 0
                || cursor.mark_exchange_time_ms == 0
                || cursor.mark_exchange_time_ms > cursor.mark_received_at_ms
                || cursor.mark_received_at_ms > cursor.observed_at_ms
                || cursor.private_root_cause_fact_id.trim().is_empty()
                || cursor.observation_fact_id.trim().is_empty()
                || !digest_is_valid(&cursor.receipt_identity_digest)
                || !digest_is_valid(&cursor.ignored_actions_digest)
                || cursor
                    .deadline_action_id
                    .as_ref()
                    .is_some_and(|value| value.trim().is_empty())
        })
    {
        return Err(EpisodeDeadlineOwnerError::OwnerCheckpoint);
    }
    Ok(())
}

fn digest_is_valid(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[derive(Debug, thiserror::Error)]
pub enum EpisodeDeadlineOwnerError {
    #[error("episode deadline owner binding is invalid")]
    Binding,
    #[error("episode deadline owner checkpoint is invalid or tampered")]
    OwnerCheckpoint,
    #[error("host episode checkpoint or projection receipt is invalid")]
    HostCheckpoint,
    #[error("deadline clock is missing, regressed, or bound to another private root")]
    Clock,
    #[error("episode deadline action list exceeds the one-action owner boundary")]
    ActionBound,
    #[error("episode deadline action arrived after its deadline or conflicts with active state")]
    LateOrConflictingAction,
    #[error("pending deadline completion conflicts with the current host checkpoint")]
    PendingConflict,
    #[error("episode projection changed at the same generation and watermark")]
    Equivocation,
    #[error("episode projection generation or watermark regressed")]
    CursorRegression,
    #[error("episode deadline identity encoding failed: {0}")]
    Encode(serde_json::Error),
    #[error("episode deadline owner storage failed: {0}")]
    Storage(#[from] StorageError),
}
