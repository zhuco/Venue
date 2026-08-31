use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use venue_control_protocol::{AccountDeliveryBinding, CopyRelationRecord};
use venue_copy::{
    AuthoritativePositionSnapshot, CopyExecutionPhase, CopyExecutionRequest, CopyExecutionResult,
};
use venue_domain::Fill;
use venue_storage::OpaqueJournal;

use crate::PersistedCopyActorTurn;

const SCHEMA_VERSION: u16 = 2;
const ROTATE_BYTES: u64 = 5 * 1024 * 1024;
const HARD_LIMIT_BYTES: u64 = 10 * 1024 * 1024;

/// Local recovery state for Copy jobs. It supplements, but never replaces, the Control inbox,
/// Actor-applied store, or account WAL: its sole purpose is retaining an immutable request and
/// signed convergence evidence across a process restart.
#[derive(Debug)]
pub(crate) struct CopyDeliveryJournal {
    journal: OpaqueJournal,
    path: PathBuf,
    checkpoint_path: PathBuf,
    binding: AccountDeliveryBinding,
    segment: u64,
    next_sequence: u64,
    relations: BTreeMap<String, CopyRelationRecord>,
    jobs: BTreeMap<String, DurableCopyJob>,
}

// This durable on-disk schema deliberately keeps historical variants byte-compatible; changing
// their shape solely for stack size would be a recovery-format migration, not a lint cleanup.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct DurableCopyJob {
    pub turn: PersistedCopyActorTurn,
    pub relation: CopyRelationRecord,
    pub request: Option<CopyExecutionRequest>,
    /// Actor Applied is durable in Runtime; this local marker only proves Node recorded that it
    /// may safely turn the exact receipt into a Control completion after restart.
    pub actor_applied_ms: Option<u64>,
    /// A zero delta has no child command/WAL to reconcile. This marker records that fact without
    /// presenting semantic Applied as an execution result.
    pub no_physical_delta: bool,
    pub execution: Option<CopyExecutionResult>,
    /// Latest command-specific signed position fact returned by recovery. It is retained with
    /// the normalized fills so a later projection never substitutes a new generic snapshot.
    pub position: Option<AuthoritativePositionSnapshot>,
    pub fills: Vec<Fill>,
    /// Completed phase evidence is never overwritten when a cross-zero target begins its
    /// separately admitted Adjust phase.
    pub prior_phases: Vec<CompletedCopyPhase>,
    /// Exact Control projection echoes that were observed after the raw result was durable. This
    /// is delivery bookkeeping in the existing recovery journal, never execution authority.
    #[serde(default)]
    pub projected_executions: Vec<ProjectedCopyExecution>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct CompletedCopyPhase {
    pub request: CopyExecutionRequest,
    pub execution: CopyExecutionResult,
    pub position: AuthoritativePositionSnapshot,
    pub fills: Vec<Fill>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ProjectedCopyExecution {
    pub phase: CopyExecutionPhase,
    pub result_sha256: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct StoredRecord {
    schema_version: u16,
    event: Event,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct StoredCheckpoint {
    schema_version: u16,
    binding: AccountDeliveryBinding,
    /// State at the beginning of this active segment. Older sealed segments remain immutable
    /// audit material; this checkpoint is a recovery index, not a second command authority.
    segment: u64,
    relations: BTreeMap<String, CopyRelationRecord>,
    jobs: BTreeMap<String, DurableCopyJob>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
// This durable tag schema is replayed from existing journals. Repacking variants changes the
// representation pressure but not recovery semantics, so keep its proven wire layout intact.
#[allow(clippy::large_enum_variant)]
enum Event {
    Root {
        binding: AccountDeliveryBinding,
        segment: u64,
    },
    Relation {
        relation: CopyRelationRecord,
    },
    Delivery {
        job_id: String,
        turn: PersistedCopyActorTurn,
        relation: CopyRelationRecord,
    },
    Request {
        job_id: String,
        request: CopyExecutionRequest,
    },
    ActorApplied {
        job_id: String,
        observed_ms: u64,
    },
    NoPhysicalDelta {
        job_id: String,
    },
    NextAdjustPhase {
        job_id: String,
        request: CopyExecutionRequest,
    },
    Execution {
        job_id: String,
        execution: CopyExecutionResult,
    },
    Reconciliation {
        job_id: String,
        execution: CopyExecutionResult,
        position: AuthoritativePositionSnapshot,
        fills: Vec<Fill>,
    },
    ExecutionProjected {
        job_id: String,
        phase: CopyExecutionPhase,
        result_sha256: [u8; 32],
    },
}

impl CopyDeliveryJournal {
    pub(crate) fn recover(
        path: impl Into<PathBuf>,
        binding: AccountDeliveryBinding,
    ) -> Result<Self, CopyDeliveryJournalError> {
        let path = path.into();
        let checkpoint_path = checkpoint_path(&path)?;
        let mut journal =
            OpaqueJournal::open(&path).map_err(|_| CopyDeliveryJournalError::Storage)?;
        let records = journal
            .recover()
            .map_err(|_| CopyDeliveryJournalError::Storage)?;
        let empty_checkpoint = (records.is_empty() && checkpoint_path.exists())
            .then(|| load_latest_checkpoint(&checkpoint_path, &binding))
            .transpose()?;
        let root_segment = match records.first() {
            Some(record) => match parse_stored_record(record)?.event {
                Event::Root {
                    binding: root,
                    segment,
                } if root == binding && segment > 0 => segment,
                _ => return Err(CopyDeliveryJournalError::Corrupt),
            },
            None => empty_checkpoint
                .as_ref()
                .map_or(1, |checkpoint| checkpoint.segment),
        };
        let checkpoint = match empty_checkpoint {
            Some(checkpoint) => Some(checkpoint),
            None => load_checkpoint(&checkpoint_path, &binding, root_segment)?,
        };
        let mut recovered = Self {
            journal,
            path,
            checkpoint_path,
            binding,
            segment: root_segment,
            next_sequence: 1,
            relations: checkpoint
                .as_ref()
                .map_or_else(BTreeMap::new, |state| state.relations.clone()),
            jobs: checkpoint.map_or_else(BTreeMap::new, |state| state.jobs),
        };
        if records.is_empty() {
            recovered.append(Event::Root {
                binding: recovered.binding.clone(),
                segment: recovered.segment,
            })?;
        } else {
            for record in records {
                if record.sequence != recovered.next_sequence {
                    return Err(CopyDeliveryJournalError::Corrupt);
                }
                let stored = parse_stored_record(&record)?;
                if record.sequence == 1 {
                    if stored.event
                        != (Event::Root {
                            binding: recovered.binding.clone(),
                            segment: recovered.segment,
                        })
                    {
                        return Err(CopyDeliveryJournalError::Corrupt);
                    }
                } else {
                    recovered.apply(stored.event)?;
                }
                recovered.next_sequence = recovered
                    .next_sequence
                    .checked_add(1)
                    .ok_or(CopyDeliveryJournalError::Storage)?;
            }
        }
        Ok(recovered)
    }

    pub(crate) fn retain_delivery(
        &mut self,
        job_id: String,
        turn: PersistedCopyActorTurn,
        relation: CopyRelationRecord,
    ) -> Result<(), CopyDeliveryJournalError> {
        if self.jobs.get(&job_id).is_some_and(|existing| {
            existing.turn.same_delivery(&turn) && existing.relation == relation
        }) {
            return Ok(());
        }
        self.observe_relation(relation.clone())?;
        self.append(Event::Delivery {
            job_id,
            turn,
            relation,
        })
    }

    /// A relation revision is durable configuration input. It may advance one revision at a
    /// time, but cannot regress, fork, or skip a locally unseen update.
    pub(crate) fn observe_relation(
        &mut self,
        relation: CopyRelationRecord,
    ) -> Result<(), CopyDeliveryJournalError> {
        let relation_id = relation.relation.relation_id.clone();
        if self
            .relations
            .get(&relation_id)
            .is_some_and(|existing| existing == &relation)
        {
            return Ok(());
        }
        self.append(Event::Relation { relation })
    }

    /// This must happen in the resident callback immediately before any Actor/WAL operation.
    pub(crate) fn persist_request(
        &mut self,
        job_id: String,
        request: CopyExecutionRequest,
    ) -> Result<(), CopyDeliveryJournalError> {
        if self
            .jobs
            .get(&job_id)
            .and_then(|job| job.request.as_ref())
            .is_some_and(|existing| existing == &request)
        {
            return Ok(());
        }
        self.append(Event::Request { job_id, request })
    }

    pub(crate) fn persist_execution(
        &mut self,
        job_id: String,
        execution: CopyExecutionResult,
    ) -> Result<(), CopyDeliveryJournalError> {
        if self
            .jobs
            .get(&job_id)
            .and_then(|job| job.execution.as_ref())
            .is_some_and(|existing| existing == &execution)
        {
            return Ok(());
        }
        self.append(Event::Execution { job_id, execution })
    }

    pub(crate) fn persist_actor_applied(
        &mut self,
        job_id: String,
        observed_ms: u64,
    ) -> Result<(), CopyDeliveryJournalError> {
        if observed_ms == 0 {
            return Err(CopyDeliveryJournalError::Corrupt);
        }
        if self
            .jobs
            .get(&job_id)
            .is_some_and(|job| job.actor_applied_ms.is_some())
        {
            return Ok(());
        }
        self.append(Event::ActorApplied {
            job_id,
            observed_ms,
        })
    }

    pub(crate) fn persist_no_physical_delta(
        &mut self,
        job_id: String,
    ) -> Result<(), CopyDeliveryJournalError> {
        if self
            .jobs
            .get(&job_id)
            .is_some_and(|job| job.no_physical_delta)
        {
            return Ok(());
        }
        self.append(Event::NoPhysicalDelta { job_id })
    }

    /// The next phase is retained before its own Actor Applied/WAL callback. The completed
    /// reduce facts remain immutable in `prior_phases`.
    pub(crate) fn persist_next_adjust_phase(
        &mut self,
        job_id: String,
        request: CopyExecutionRequest,
    ) -> Result<(), CopyDeliveryJournalError> {
        self.append(Event::NextAdjustPhase { job_id, request })
    }

    /// Fills are canonical normalized facts, never raw wire payloads. Duplicate fill identities
    /// must be byte-for-byte equal; a conflict stops recovery rather than inventing a cursor.
    pub(crate) fn persist_reconciliation(
        &mut self,
        job_id: String,
        execution: CopyExecutionResult,
        position: AuthoritativePositionSnapshot,
        fills: Vec<Fill>,
    ) -> Result<(), CopyDeliveryJournalError> {
        if let Some(job) = self.jobs.get(&job_id) {
            let incoming = fills
                .iter()
                .map(|fill| (fill.fill_id.clone(), fill))
                .collect::<BTreeMap<_, _>>();
            let durable = job
                .fills
                .iter()
                .map(|fill| (fill.fill_id.clone(), fill))
                .collect::<BTreeMap<_, _>>();
            if job.execution.as_ref() == Some(&execution)
                && job.position.as_ref() == Some(&position)
                && incoming
                    .iter()
                    .all(|(id, fill)| durable.get(id) == Some(fill))
            {
                return Ok(());
            }
        }
        self.append(Event::Reconciliation {
            job_id,
            execution,
            position,
            fills,
        })
    }

    #[must_use]
    pub(crate) fn jobs(&self) -> &BTreeMap<String, DurableCopyJob> {
        &self.jobs
    }

    pub(crate) fn mark_execution_projected(
        &mut self,
        job_id: String,
        phase: CopyExecutionPhase,
        result_sha256: [u8; 32],
    ) -> Result<(), CopyDeliveryJournalError> {
        if result_sha256 == [0; 32] {
            return Err(CopyDeliveryJournalError::Corrupt);
        }
        let current = self
            .jobs
            .get(&job_id)
            .and_then(|job| copy_execution_result_for_phase(job, phase))
            .ok_or(CopyDeliveryJournalError::Corrupt)?;
        if copy_execution_result_sha256(current)? != result_sha256 {
            // A later durable state for this same phase superseded an envelope still in the
            // outbox. Its echoed acknowledgement is harmless, but cannot acknowledge the newer
            // bytes; leave those unprojected for the next envelope.
            return Ok(());
        }
        if self.jobs.get(&job_id).is_some_and(|job| {
            job.projected_executions.iter().any(|projected| {
                projected.phase == phase && projected.result_sha256 == result_sha256
            })
        }) {
            return Ok(());
        }
        let prior_reduce_missing = if phase == CopyExecutionPhase::Adjust {
            let Some(job) = self.jobs.get(&job_id) else {
                return Err(CopyDeliveryJournalError::Corrupt);
            };
            let mut missing = false;
            for completed in &job.prior_phases {
                if completed.request.phase != CopyExecutionPhase::ReduceToZero {
                    continue;
                }
                let expected = copy_execution_result_sha256(&completed.execution)?;
                if !job.projected_executions.iter().any(|projected| {
                    projected.phase == CopyExecutionPhase::ReduceToZero
                        && projected.result_sha256 == expected
                }) {
                    missing = true;
                    break;
                }
            }
            missing
        } else {
            false
        };
        if prior_reduce_missing {
            return Err(CopyDeliveryJournalError::Conflict);
        }
        self.append(Event::ExecutionProjected {
            job_id,
            phase,
            result_sha256,
        })
    }

    fn append(&mut self, event: Event) -> Result<(), CopyDeliveryJournalError> {
        let encoded = serde_json::to_vec(&StoredRecord {
            schema_version: SCHEMA_VERSION,
            event: event.clone(),
        })
        .map_err(|_| CopyDeliveryJournalError::Storage)?;
        self.rotate_if_due(encoded.len())?;
        // Validate against a candidate state before append, then expose that state only after
        // OpaqueJournal has accepted the record. A failed fsync must not leave an in-memory
        // request that was never durable enough to suppress replanning after a restart.
        let mut candidate = self.jobs.clone();
        let mut candidate_relations = self.relations.clone();
        Self::apply_event(
            &self.binding,
            &mut candidate_relations,
            &mut candidate,
            event.clone(),
        )?;
        self.journal
            .append(self.next_sequence, &encoded)
            .map_err(|_| CopyDeliveryJournalError::Storage)?;
        self.jobs = candidate;
        self.relations = candidate_relations;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(CopyDeliveryJournalError::Storage)?;
        Ok(())
    }

    /// A checkpoint preserves the full active recovery set before sealing the old segment, so an
    /// Unknown can keep reconciling across a 5 MiB boundary without being discarded or replayed.
    fn rotate_if_due(&mut self, next_record_len: usize) -> Result<(), CopyDeliveryJournalError> {
        let existing = fs::metadata(&self.path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let estimated_record = estimate_opaque_append_bytes(next_record_len)?;
        if estimated_record > HARD_LIMIT_BYTES {
            return Err(CopyDeliveryJournalError::Storage);
        }
        let next = existing
            .checked_add(estimated_record)
            .ok_or(CopyDeliveryJournalError::Storage)?;
        if next <= ROTATE_BYTES {
            return Ok(());
        }
        let next_segment = self
            .segment
            .checked_add(1)
            .ok_or(CopyDeliveryJournalError::Storage)?;
        persist_checkpoint(
            &self.checkpoint_path,
            &StoredCheckpoint {
                schema_version: SCHEMA_VERSION,
                binding: self.binding.clone(),
                segment: next_segment,
                relations: self.relations.clone(),
                jobs: self.jobs.clone(),
            },
        )?;
        let archive = archive_path(&self.path, self.segment)?;
        fs::rename(&self.path, archive).map_err(|_| CopyDeliveryJournalError::Storage)?;
        self.journal =
            OpaqueJournal::open(&self.path).map_err(|_| CopyDeliveryJournalError::Storage)?;
        self.segment = next_segment;
        self.next_sequence = 1;
        self.append(Event::Root {
            binding: self.binding.clone(),
            segment: self.segment,
        })?;
        Ok(())
    }

    fn apply(&mut self, event: Event) -> Result<(), CopyDeliveryJournalError> {
        Self::apply_event(&self.binding, &mut self.relations, &mut self.jobs, event)
    }

    fn apply_event(
        binding: &AccountDeliveryBinding,
        relations: &mut BTreeMap<String, CopyRelationRecord>,
        jobs: &mut BTreeMap<String, DurableCopyJob>,
        event: Event,
    ) -> Result<(), CopyDeliveryJournalError> {
        match event {
            Event::Root { binding: root, .. } if root == *binding => Ok(()),
            Event::Root { .. } => Err(CopyDeliveryJournalError::Corrupt),
            Event::Relation { relation } => {
                relation
                    .validate()
                    .map_err(|_| CopyDeliveryJournalError::Corrupt)?;
                let relation_id = relation.relation.relation_id.clone();
                match relations.get(&relation_id) {
                    Some(existing) if existing == &relation => Ok(()),
                    Some(existing) if relation.revision == existing.revision.saturating_add(1) => {
                        relations.insert(relation_id, relation);
                        Ok(())
                    }
                    Some(_) => Err(CopyDeliveryJournalError::Conflict),
                    // A fresh Node has no predecessor history; the loopback Control record is
                    // its first durable baseline. Once recorded, every advancement is strict.
                    None => {
                        relations.insert(relation_id, relation);
                        Ok(())
                    }
                }
            }
            Event::Delivery {
                job_id,
                turn,
                relation,
            } => {
                relation
                    .validate()
                    .map_err(|_| CopyDeliveryJournalError::Corrupt)?;
                if relations.get(&relation.relation.relation_id) != Some(&relation) {
                    return Err(CopyDeliveryJournalError::Corrupt);
                }
                match jobs.get(&job_id) {
                    Some(existing)
                        if existing.turn.same_delivery(&turn) && existing.relation == relation =>
                    {
                        Ok(())
                    }
                    Some(_) => Err(CopyDeliveryJournalError::Conflict),
                    None => {
                        jobs.insert(
                            job_id,
                            DurableCopyJob {
                                turn,
                                relation,
                                request: None,
                                actor_applied_ms: None,
                                no_physical_delta: false,
                                execution: None,
                                position: None,
                                fills: Vec::new(),
                                prior_phases: Vec::new(),
                                projected_executions: Vec::new(),
                            },
                        );
                        Ok(())
                    }
                }
            }
            Event::Request { job_id, request } => {
                let job = jobs
                    .get_mut(&job_id)
                    .ok_or(CopyDeliveryJournalError::Corrupt)?;
                match &job.request {
                    Some(existing) if existing == &request => Ok(()),
                    Some(_) => Err(CopyDeliveryJournalError::Conflict),
                    None => {
                        job.request = Some(request);
                        Ok(())
                    }
                }
            }
            Event::ActorApplied {
                job_id,
                observed_ms,
            } => {
                let job = jobs
                    .get_mut(&job_id)
                    .ok_or(CopyDeliveryJournalError::Corrupt)?;
                if job.request.is_none() || observed_ms == 0 {
                    return Err(CopyDeliveryJournalError::Corrupt);
                }
                match job.actor_applied_ms {
                    Some(existing) if existing != observed_ms => {
                        Err(CopyDeliveryJournalError::Conflict)
                    }
                    Some(_) => Ok(()),
                    None => {
                        job.actor_applied_ms = Some(observed_ms);
                        Ok(())
                    }
                }
            }
            Event::Execution { job_id, execution } => {
                let job = jobs
                    .get_mut(&job_id)
                    .ok_or(CopyDeliveryJournalError::Corrupt)?;
                if job.no_physical_delta || job.request.as_ref() != Some(&execution.request) {
                    return Err(CopyDeliveryJournalError::Conflict);
                }
                match &job.execution {
                    Some(existing) if existing == &execution => Ok(()),
                    Some(_) => Err(CopyDeliveryJournalError::Conflict),
                    None => {
                        job.execution = Some(execution);
                        Ok(())
                    }
                }
            }
            Event::NoPhysicalDelta { job_id } => {
                let job = jobs
                    .get_mut(&job_id)
                    .ok_or(CopyDeliveryJournalError::Corrupt)?;
                if job
                    .request
                    .as_ref()
                    .is_none_or(|request| !request.requested_delta_exposure.value.is_zero())
                    || job.execution.is_some()
                {
                    return Err(CopyDeliveryJournalError::Conflict);
                }
                job.no_physical_delta = true;
                Ok(())
            }
            Event::NextAdjustPhase { job_id, request } => {
                let job = jobs
                    .get_mut(&job_id)
                    .ok_or(CopyDeliveryJournalError::Corrupt)?;
                let (Some(previous), Some(execution), Some(position)) =
                    (&job.request, &job.execution, &job.position)
                else {
                    return Err(CopyDeliveryJournalError::Conflict);
                };
                if job.no_physical_delta
                    || execution.state != venue_copy::CopyExecutionState::Reconciled
                    || previous.phase != venue_copy::CopyExecutionPhase::ReduceToZero
                    || !position.exposure.value.is_zero()
                    || request.phase != venue_copy::CopyExecutionPhase::Adjust
                    || request.job_id != previous.job_id
                    || request.delivery_digest != previous.delivery_digest
                    || request.binding != previous.binding
                    || request.target_exposure != previous.target_exposure
                    || !request.current_exposure.value.is_zero()
                    || request.requested_delta_exposure.value.is_zero()
                {
                    return Err(CopyDeliveryJournalError::Conflict);
                }
                job.prior_phases.push(CompletedCopyPhase {
                    request: previous.clone(),
                    execution: execution.clone(),
                    position: position.clone(),
                    fills: job.fills.clone(),
                });
                job.request = Some(request);
                job.actor_applied_ms = None;
                job.execution = None;
                job.position = None;
                job.fills.clear();
                Ok(())
            }
            Event::Reconciliation {
                job_id,
                execution,
                position,
                fills,
            } => {
                let job = jobs
                    .get_mut(&job_id)
                    .ok_or(CopyDeliveryJournalError::Corrupt)?;
                if job.no_physical_delta
                    || job.request.as_ref() != Some(&execution.request)
                    || position.binding != execution.request.binding
                    || fills.iter().any(|fill| fill.validate().is_err())
                {
                    return Err(CopyDeliveryJournalError::Conflict);
                }
                let mut all_fills = BTreeMap::new();
                for fill in job.fills.iter().chain(fills.iter()) {
                    match all_fills.insert(fill.fill_id.clone(), fill.clone()) {
                        Some(existing) if existing != *fill => {
                            return Err(CopyDeliveryJournalError::Conflict);
                        }
                        _ => {}
                    }
                }
                job.fills = all_fills.into_values().collect();
                job.execution = Some(execution);
                job.position = Some(position);
                Ok(())
            }
            Event::ExecutionProjected {
                job_id,
                phase,
                result_sha256,
            } => {
                let job = jobs
                    .get_mut(&job_id)
                    .ok_or(CopyDeliveryJournalError::Corrupt)?;
                if result_sha256 == [0; 32]
                    || !copy_execution_result_for_phase(job, phase).is_some_and(|result| {
                        copy_execution_result_sha256(result)
                            .is_ok_and(|actual| actual == result_sha256)
                    })
                {
                    return Err(CopyDeliveryJournalError::Conflict);
                }
                if !job.projected_executions.iter().any(|projected| {
                    projected.phase == phase && projected.result_sha256 == result_sha256
                }) {
                    job.projected_executions.push(ProjectedCopyExecution {
                        phase,
                        result_sha256,
                    });
                }
                Ok(())
            }
        }
    }
}

fn copy_execution_result_for_phase(
    job: &DurableCopyJob,
    phase: CopyExecutionPhase,
) -> Option<&CopyExecutionResult> {
    job.prior_phases
        .iter()
        .find(|completed| completed.request.phase == phase)
        .map(|completed| &completed.execution)
        .or_else(|| {
            job.request
                .as_ref()
                .filter(|request| request.phase == phase)
                .and(job.execution.as_ref())
        })
}

fn copy_execution_result_sha256(
    result: &CopyExecutionResult,
) -> Result<[u8; 32], CopyDeliveryJournalError> {
    let encoded = serde_json::to_string(result).map_err(|_| CopyDeliveryJournalError::Corrupt)?;
    Ok(Sha256::digest(encoded.as_bytes()).into())
}

fn parse_stored_record(
    record: &venue_storage::OpaqueJournalRecord,
) -> Result<StoredRecord, CopyDeliveryJournalError> {
    let stored: StoredRecord =
        serde_json::from_slice(&record.payload).map_err(|_| CopyDeliveryJournalError::Corrupt)?;
    (stored.schema_version == SCHEMA_VERSION)
        .then_some(stored)
        .ok_or(CopyDeliveryJournalError::Corrupt)
}

fn checkpoint_path(path: &Path) -> Result<PathBuf, CopyDeliveryJournalError> {
    let parent = path.parent().ok_or(CopyDeliveryJournalError::Storage)?;
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or(CopyDeliveryJournalError::Storage)?;
    Ok(parent.join(format!("{stem}.checkpoint.json")))
}

fn load_checkpoint(
    path: &Path,
    binding: &AccountDeliveryBinding,
    segment: u64,
) -> Result<Option<StoredCheckpoint>, CopyDeliveryJournalError> {
    if segment == 1 {
        return Ok(None);
    }
    let encoded = fs::read(path).map_err(|_| CopyDeliveryJournalError::Corrupt)?;
    let checkpoint: StoredCheckpoint =
        serde_json::from_slice(&encoded).map_err(|_| CopyDeliveryJournalError::Corrupt)?;
    if checkpoint.schema_version != SCHEMA_VERSION
        || checkpoint.binding != *binding
        || checkpoint.segment != segment
    {
        return Err(CopyDeliveryJournalError::Corrupt);
    }
    Ok(Some(checkpoint))
}

fn load_latest_checkpoint(
    path: &Path,
    binding: &AccountDeliveryBinding,
) -> Result<StoredCheckpoint, CopyDeliveryJournalError> {
    let encoded = fs::read(path).map_err(|_| CopyDeliveryJournalError::Corrupt)?;
    let checkpoint: StoredCheckpoint =
        serde_json::from_slice(&encoded).map_err(|_| CopyDeliveryJournalError::Corrupt)?;
    if checkpoint.schema_version != SCHEMA_VERSION
        || checkpoint.binding != *binding
        || checkpoint.segment < 2
    {
        return Err(CopyDeliveryJournalError::Corrupt);
    }
    Ok(checkpoint)
}

fn persist_checkpoint(
    path: &Path,
    checkpoint: &StoredCheckpoint,
) -> Result<(), CopyDeliveryJournalError> {
    let encoded = serde_json::to_vec(checkpoint).map_err(|_| CopyDeliveryJournalError::Storage)?;
    if encoded.len() > ROTATE_BYTES as usize {
        return Err(CopyDeliveryJournalError::Storage);
    }
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, encoded).map_err(|_| CopyDeliveryJournalError::Storage)?;
    fs::OpenOptions::new()
        .write(true)
        .open(&temporary)
        .and_then(|file| file.sync_all())
        .map_err(|_| CopyDeliveryJournalError::Storage)?;
    // Windows does not replace an existing destination with `rename`. The fresh temporary
    // checkpoint is already synced; a crash in the narrow remove/rename interval still leaves
    // the active segment intact, so recovery can rebuild from that segment instead.
    if path.exists() {
        fs::remove_file(path).map_err(|_| CopyDeliveryJournalError::Storage)?;
    }
    fs::rename(&temporary, path).map_err(|_| CopyDeliveryJournalError::Storage)
}

fn estimate_opaque_append_bytes(payload_len: usize) -> Result<u64, CopyDeliveryJournalError> {
    // OpaqueJournal wraps payload bytes as JSON (base64 plus hash/sequence fields). This
    // conservative bound prevents a 2 MiB payload from bypassing the file-level 10 MiB cap.
    let payload = u64::try_from(payload_len).map_err(|_| CopyDeliveryJournalError::Storage)?;
    payload
        .checked_mul(2)
        .and_then(|value| value.checked_add(1024))
        .ok_or(CopyDeliveryJournalError::Storage)
}

fn archive_path(path: &Path, segment: u64) -> Result<PathBuf, CopyDeliveryJournalError> {
    let parent = path.parent().ok_or(CopyDeliveryJournalError::Storage)?;
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or(CopyDeliveryJournalError::Storage)?;
    let archive = parent.join(format!("{stem}.segment-{segment}.jsonl"));
    if archive.exists() {
        return Err(CopyDeliveryJournalError::Storage);
    }
    Ok(archive)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CopyDeliveryJournalError {
    #[error("copy delivery recovery journal is unavailable")]
    Storage,
    #[error("copy delivery recovery journal is corrupt")]
    Corrupt,
    #[error("copy delivery recovery journal conflicts with an immutable job")]
    Conflict,
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use venue_control_protocol::{
        CopyLifecyclePolicy, CopyRelationBinding, CopyRelationConfig, CopyRiskPolicy, GatewayMode,
        VenueId,
    };
    use venue_copy::{
        CopyAction, CopyExecutionPhase, CopyExecutionRequest, CopyExecutionResult,
        CopyExecutionState, CopyIdentityInput, DeliveryBinding, RelationCommitment,
        derive_copy_identities,
    };
    use venue_domain::{Amount, Asset, InstrumentIdentity, MarketKind, Symbol};

    use super::*;

    fn binding() -> Result<AccountDeliveryBinding, Box<dyn std::error::Error>> {
        Ok(AccountDeliveryBinding {
            venue: VenueId::Bybit,
            mode: GatewayMode::Live,
            trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "DOGE/USDT".parse()?,
            instance_id: "copy-doge".to_owned(),
            config_epoch: 1,
        })
    }

    fn durable_unknown_job() -> Result<DurableCopyJob, Box<dyn std::error::Error>> {
        let identities = derive_copy_identities(&CopyIdentityInput {
            event_id: [1; 16],
            source_event_id: [2; 16],
            follower_account_id: [3; 16],
            follower_binding_id: [4; 16],
            leader_order_id: [5; 16],
            revision: 1,
            action: CopyAction::New,
        })?;
        let relation_ids = derive_copy_identities(&CopyIdentityInput {
            event_id: [6; 16],
            source_event_id: [7; 16],
            follower_account_id: [8; 16],
            follower_binding_id: [9; 16],
            leader_order_id: [10; 16],
            revision: 1,
            action: CopyAction::New,
        })?;
        let symbol: Symbol = "DOGE/USDT".parse()?;
        let asset = Asset::new("USDT")?;
        let request = CopyExecutionRequest {
            job_id: identities.job_id,
            delivery_digest: [7; 32],
            binding: DeliveryBinding {
                relation: RelationCommitment {
                    relation_id: relation_ids.job_id,
                    revision: 1,
                    policy_digest: [9; 32],
                },
                leader_id: relation_ids.planning_snapshot_id,
                follower_id: relation_ids.child_order_id,
                follower_binding_id: identities.planning_snapshot_id,
                follower_instance_id: "copy-doge".to_owned(),
                account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                instrument: InstrumentIdentity {
                    symbol: symbol.clone(),
                    market: MarketKind::LinearPerpetual,
                    settlement_asset: Some(asset.clone()),
                },
                policy_id: relation_ids.child_order_id,
            },
            target_generation: 3,
            position_generation: 2,
            target_exposure: Amount::new(asset.clone(), Decimal::ONE),
            current_exposure: Amount::new(asset.clone(), Decimal::ZERO),
            requested_delta_exposure: Amount::new(asset, Decimal::ONE),
            phase: CopyExecutionPhase::Adjust,
        };
        let relation = CopyRelationRecord {
            revision: 1,
            relation: CopyRelationConfig {
                relation_id: relation_ids.job_id.to_string(),
                leader: CopyRelationBinding {
                    venue: VenueId::Bybit,
                    mode: GatewayMode::Live,
                    trading_account_id: "00000000-0000-4000-8000-000000000002".to_owned(),
                    instance_id: "leader-doge".to_owned(),
                    symbol: symbol.clone(),
                },
                follower: CopyRelationBinding {
                    venue: VenueId::Bybit,
                    mode: GatewayMode::Live,
                    trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                    instance_id: "copy-doge".to_owned(),
                    symbol,
                },
                allocated_capital: Decimal::ONE,
                multiplier: Decimal::ONE,
                safety_reserve_rate: Decimal::ZERO,
                risk: CopyRiskPolicy {
                    max_total_notional: Decimal::ONE,
                    max_order_notional: Decimal::ONE,
                    max_leverage: Decimal::ONE,
                },
                lifecycle: CopyLifecyclePolicy::Active,
            },
        };
        let turn = serde_json::from_value(serde_json::json!({
            "lease": {"schema_version": 2, "delivery_id": "copy-delivery", "binding": {"venue": "bybit", "mode": "LIVE", "trading_account_id": "00000000-0000-4000-8000-000000000001", "symbol": "DOGE/USDT", "instance_id": "copy-doge", "config_epoch": 1}, "node_id": "node-copy", "lease_epoch": 1, "leased_at_ms": 1, "expires_at_ms": 2, "purpose": "install"},
            "job": {"job_id": "copy-job", "job_digest": vec![1_u8; 32], "symbol": "DOGE/USDT", "manifest": {}, "semantic_job": {}, "created_at_ms": 1, "expires_at_ms": 2},
            "durable_inbox_digest": vec![2_u8; 32], "durable_inbox_sequence": 1, "durable_inbox_root_digest": vec![3_u8; 32]
        }))?;
        Ok(DurableCopyJob {
            turn,
            relation,
            request: Some(request.clone()),
            actor_applied_ms: Some(2),
            no_physical_delta: false,
            execution: Some(CopyExecutionResult {
                request,
                state: CopyExecutionState::Unknown,
                command_id: Some("copy-child".to_owned()),
                fact_digest: [4; 32],
                reconciled_position: None,
                observed_at_ms: 2,
            }),
            position: None,
            fills: Vec::new(),
            prior_phases: Vec::new(),
            projected_executions: Vec::new(),
        })
    }

    #[test]
    fn checkpointed_segment_rotation_recovers_after_new_segment_root()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("copy-jobs.jsonl");
        let scope = binding()?;
        let mut journal = CopyDeliveryJournal::recover(&path, scope.clone())
            .map_err(|error| format!("initial: {error}"))?;
        journal
            .rotate_if_due((ROTATE_BYTES / 2) as usize)
            .map_err(|error| format!("rotate: {error}"))?;
        assert_eq!(journal.segment, 2);
        assert!(temporary.path().join("copy-jobs.segment-1.jsonl").exists());
        drop(journal);

        let recovered = CopyDeliveryJournal::recover(&path, scope)
            .map_err(|error| format!("recovered: {error}"))?;
        assert_eq!(recovered.segment, 2);
        assert!(recovered.jobs().is_empty());
        Ok(())
    }

    #[test]
    fn checkpoint_recovers_if_rotation_crashes_before_new_root()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("copy-jobs.jsonl");
        let scope = binding()?;
        let mut journal = CopyDeliveryJournal::recover(&path, scope.clone())
            .map_err(|error| format!("initial: {error}"))?;
        journal
            .rotate_if_due((ROTATE_BYTES / 2) as usize)
            .map_err(|error| format!("rotate: {error}"))?;
        drop(journal);
        fs::remove_file(&path)?;

        let recovered = CopyDeliveryJournal::recover(&path, scope)
            .map_err(|error| format!("recovered: {error}"))?;
        assert_eq!(recovered.segment, 2);
        assert!(recovered.jobs().is_empty());
        Ok(())
    }

    #[test]
    fn checkpoint_preserves_unknown_copy_phase_across_rotation()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("copy-jobs.jsonl");
        let scope = binding()?;
        let mut journal = CopyDeliveryJournal::recover(&path, scope.clone())?;
        let job = durable_unknown_job()?;
        journal.jobs.insert("copy-job".to_owned(), job.clone());
        journal.rotate_if_due((ROTATE_BYTES / 2) as usize)?;
        drop(journal);

        let recovered = CopyDeliveryJournal::recover(&path, scope)?;
        let actual = recovered.jobs().get("copy-job").ok_or("missing job")?;
        assert_eq!(actual, &job);
        assert_eq!(
            actual.execution.as_ref().map(|execution| execution.state),
            Some(CopyExecutionState::Unknown)
        );
        assert_eq!(
            actual.request.as_ref().map(|request| request.phase),
            Some(CopyExecutionPhase::Adjust)
        );
        Ok(())
    }
}
