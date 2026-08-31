use std::{fs, path::PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    ProjectionStore, StorageError,
    journal::{DurableJsonl, JsonlSnapshot},
};

const RECORD_SCHEMA_VERSION: u16 = 1;
const CHECKPOINT_SCHEMA_VERSION: u16 = 1;
const EMPTY_JOURNAL_ROOT: [u8; 32] = [0; 32];
const MAX_REPLAY_STATE_BYTES: usize = 2 * 1024 * 1024;

/// Opaque commitments to the runtime's canonical actor and Owner identities.
///
/// Storage deliberately does not redefine those identities. The runtime supplies their stable
/// canonical commitments, and every durable record retains both commitments exactly.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActorAppliedScope {
    actor_sha256: [u8; 32],
    owner_sha256: [u8; 32],
}

impl ActorAppliedScope {
    pub fn new(actor_sha256: [u8; 32], owner_sha256: [u8; 32]) -> Result<Self, ActorAppliedError> {
        if is_zero_digest(&actor_sha256) || is_zero_digest(&owner_sha256) {
            return Err(ActorAppliedError::InvalidScope);
        }
        Ok(Self {
            actor_sha256,
            owner_sha256,
        })
    }

    #[must_use]
    pub const fn actor_sha256(&self) -> [u8; 32] {
        self.actor_sha256
    }

    #[must_use]
    pub const fn owner_sha256(&self) -> [u8; 32] {
        self.owner_sha256
    }

    fn validate(self) -> Result<(), ActorAppliedError> {
        Self::new(self.actor_sha256, self.owner_sha256).map(|_| ())
    }
}

/// Epochs and generations bound to one applied Actor turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActorAppliedGenerations {
    config_epoch: u64,
    connection_generation: u64,
    private_generation: u64,
}

impl ActorAppliedGenerations {
    pub fn new(
        config_epoch: u64,
        connection_generation: u64,
        private_generation: u64,
    ) -> Result<Self, ActorAppliedError> {
        if config_epoch == 0 || connection_generation == 0 || private_generation == 0 {
            return Err(ActorAppliedError::InvalidGeneration);
        }
        Ok(Self {
            config_epoch,
            connection_generation,
            private_generation,
        })
    }

    #[must_use]
    pub const fn config_epoch(&self) -> u64 {
        self.config_epoch
    }

    #[must_use]
    pub const fn connection_generation(&self) -> u64 {
        self.connection_generation
    }

    #[must_use]
    pub const fn private_generation(&self) -> u64 {
        self.private_generation
    }

    fn validate(self) -> Result<(), ActorAppliedError> {
        Self::new(
            self.config_epoch,
            self.connection_generation,
            self.private_generation,
        )
        .map(|_| ())
    }

    fn does_not_regress_from(self, previous: Self) -> bool {
        self.config_epoch >= previous.config_epoch
            && self.connection_generation >= previous.connection_generation
            && self.private_generation >= previous.private_generation
    }
}

/// Exact durable head of the mutation WAL observed before the Actor checkpoint was committed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DurableWalHead {
    root_sha256: [u8; 32],
    tail_sequence: u64,
    record_count: u64,
    #[serde(default)]
    format_version: DurableWalHeadFormat,
}

/// Digest algorithm used by a durable command-WAL head. Missing fields in historical actor
/// checkpoints deserialize as V1, preserving prefix validation across the incremental V2 move.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableWalHeadFormat {
    #[default]
    V1,
    V2,
}

impl DurableWalHead {
    pub fn new(
        root_sha256: [u8; 32],
        tail_sequence: u64,
        record_count: u64,
    ) -> Result<Self, ActorAppliedError> {
        if is_zero_digest(&root_sha256)
            || (tail_sequence == 0) != (record_count == 0)
            || record_count > tail_sequence
        {
            return Err(ActorAppliedError::InvalidWalHead);
        }
        Ok(Self {
            root_sha256,
            tail_sequence,
            record_count,
            format_version: DurableWalHeadFormat::V1,
        })
    }

    pub fn new_v2(
        root_sha256: [u8; 32],
        tail_sequence: u64,
        record_count: u64,
    ) -> Result<Self, ActorAppliedError> {
        let mut head = Self::new(root_sha256, tail_sequence, record_count)?;
        head.format_version = DurableWalHeadFormat::V2;
        Ok(head)
    }

    #[must_use]
    pub const fn root_sha256(&self) -> [u8; 32] {
        self.root_sha256
    }

    #[must_use]
    pub const fn tail_sequence(&self) -> u64 {
        self.tail_sequence
    }

    #[must_use]
    pub const fn record_count(&self) -> u64 {
        self.record_count
    }

    #[must_use]
    pub const fn format_version(&self) -> DurableWalHeadFormat {
        self.format_version
    }

    fn validate(self) -> Result<(), ActorAppliedError> {
        let result = match self.format_version {
            DurableWalHeadFormat::V1 => {
                Self::new(self.root_sha256, self.tail_sequence, self.record_count)
            }
            DurableWalHeadFormat::V2 => {
                Self::new_v2(self.root_sha256, self.tail_sequence, self.record_count)
            }
        };
        result.map(|_| ())
    }

    fn does_not_drift_from(self, previous: Self) -> bool {
        let pair_changed = (self.tail_sequence, self.record_count)
            != (previous.tail_sequence, previous.record_count);
        let format_changed = self.format_version != previous.format_version;
        self.tail_sequence >= previous.tail_sequence
            && self.record_count >= previous.record_count
            && ((pair_changed || format_changed) == (self.root_sha256 != previous.root_sha256))
    }
}

/// Opaque replay projection owned by the Actor runtime rather than storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActorAppliedReplayState {
    revision: u64,
    applied_private_sequence: u64,
    bytes: Vec<u8>,
}

impl ActorAppliedReplayState {
    pub fn new(
        revision: u64,
        applied_private_sequence: u64,
        bytes: Vec<u8>,
    ) -> Result<Self, ActorAppliedError> {
        if revision == 0 || bytes.is_empty() || bytes.len() > MAX_REPLAY_STATE_BYTES {
            return Err(ActorAppliedError::InvalidReplayState);
        }
        Ok(Self {
            revision,
            applied_private_sequence,
            bytes,
        })
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub const fn applied_private_sequence(&self) -> u64 {
        self.applied_private_sequence
    }

    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    fn validate(&self) -> Result<(), ActorAppliedError> {
        if self.revision == 0 || self.bytes.is_empty() || self.bytes.len() > MAX_REPLAY_STATE_BYTES
        {
            return Err(ActorAppliedError::InvalidReplayState);
        }
        Ok(())
    }
}

/// Candidate state for one Actor turn. This value is not an authority receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActorAppliedCommit {
    scope: ActorAppliedScope,
    generations: ActorAppliedGenerations,
    turn_sequence: u64,
    wal: DurableWalHead,
    replay: ActorAppliedReplayState,
}

impl ActorAppliedCommit {
    pub fn new(
        scope: ActorAppliedScope,
        generations: ActorAppliedGenerations,
        turn_sequence: u64,
        wal: DurableWalHead,
        replay: ActorAppliedReplayState,
    ) -> Result<Self, ActorAppliedError> {
        if turn_sequence == 0 {
            return Err(ActorAppliedError::InvalidTurnSequence);
        }
        scope.validate()?;
        generations.validate()?;
        wal.validate()?;
        replay.validate()?;
        Ok(Self {
            scope,
            generations,
            turn_sequence,
            wal,
            replay,
        })
    }
}

/// Caller-persisted expectation used to reject missing artifacts and internally consistent
/// rollback across process restarts.
///
/// This value is intentionally serializable so a higher layer can retain it outside these two
/// artifacts. It is not an authority: it only states the durable head that the caller expects.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActorAppliedAnchor {
    journal_root_sha256: [u8; 32],
    journal_tail_sequence: u64,
    journal_record_count: u64,
    checkpoint_sha256: [u8; 32],
}

impl ActorAppliedAnchor {
    pub fn new(
        journal_root_sha256: [u8; 32],
        journal_tail_sequence: u64,
        journal_record_count: u64,
        checkpoint_sha256: [u8; 32],
    ) -> Result<Self, ActorAppliedError> {
        if is_zero_digest(&journal_root_sha256)
            || is_zero_digest(&checkpoint_sha256)
            || journal_tail_sequence == 0
            || journal_record_count != journal_tail_sequence
        {
            return Err(ActorAppliedError::InvalidAnchor);
        }
        Ok(Self {
            journal_root_sha256,
            journal_tail_sequence,
            journal_record_count,
            checkpoint_sha256,
        })
    }

    #[must_use]
    pub const fn journal_root_sha256(&self) -> [u8; 32] {
        self.journal_root_sha256
    }

    #[must_use]
    pub const fn journal_tail_sequence(&self) -> u64 {
        self.journal_tail_sequence
    }

    #[must_use]
    pub const fn journal_record_count(&self) -> u64 {
        self.journal_record_count
    }

    #[must_use]
    pub const fn checkpoint_sha256(&self) -> [u8; 32] {
        self.checkpoint_sha256
    }

    fn validate(self) -> Result<(), ActorAppliedError> {
        Self::new(
            self.journal_root_sha256,
            self.journal_tail_sequence,
            self.journal_record_count,
            self.checkpoint_sha256,
        )
        .map(|_| ())
    }
}

/// Non-deserializable storage-durability receipt issued only after the journal append and matching
/// checkpoint are durable.
///
/// The Actor/Owner commitments and WAL head are caller assertions. This receipt proves only that
/// those exact assertions and replay bytes crossed the storage boundary. It cannot grant runtime,
/// writer, WAL, or production mutation authority; that requires a later integration which derives
/// canonical identity commitments and reads the real durable WAL head. A receipt must also be
/// revalidated against the store before use across an await or process boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActorAppliedReceipt {
    scope: ActorAppliedScope,
    generations: ActorAppliedGenerations,
    turn_sequence: u64,
    wal: DurableWalHead,
    replay_revision: u64,
    applied_private_sequence: u64,
    replay_state_sha256: [u8; 32],
    checkpoint_sha256: [u8; 32],
    journal_root_sha256: [u8; 32],
    journal_tail_sequence: u64,
    journal_record_count: u64,
}

impl ActorAppliedReceipt {
    #[must_use]
    pub const fn scope(&self) -> ActorAppliedScope {
        self.scope
    }

    #[must_use]
    pub const fn generations(&self) -> ActorAppliedGenerations {
        self.generations
    }

    #[must_use]
    pub const fn turn_sequence(&self) -> u64 {
        self.turn_sequence
    }

    #[must_use]
    pub const fn wal(&self) -> DurableWalHead {
        self.wal
    }

    #[must_use]
    pub const fn replay_revision(&self) -> u64 {
        self.replay_revision
    }

    #[must_use]
    pub const fn applied_private_sequence(&self) -> u64 {
        self.applied_private_sequence
    }

    #[must_use]
    pub const fn replay_state_sha256(&self) -> [u8; 32] {
        self.replay_state_sha256
    }

    #[must_use]
    pub const fn checkpoint_sha256(&self) -> [u8; 32] {
        self.checkpoint_sha256
    }

    #[must_use]
    pub const fn journal_root_sha256(&self) -> [u8; 32] {
        self.journal_root_sha256
    }

    #[must_use]
    pub const fn journal_tail_sequence(&self) -> u64 {
        self.journal_tail_sequence
    }

    #[must_use]
    pub const fn journal_record_count(&self) -> u64 {
        self.journal_record_count
    }

    /// Returns a serializable expected-head value for a later anchored reopen. The anchor is not
    /// an authority receipt and must itself be retained outside the journal/checkpoint pair.
    #[must_use]
    pub const fn anchor(&self) -> ActorAppliedAnchor {
        ActorAppliedAnchor {
            journal_root_sha256: self.journal_root_sha256,
            journal_tail_sequence: self.journal_tail_sequence,
            journal_record_count: self.journal_record_count,
            checkpoint_sha256: self.checkpoint_sha256,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredActorApplied {
    receipt: ActorAppliedReceipt,
    replay_state: Vec<u8>,
}

impl RecoveredActorApplied {
    #[must_use]
    pub const fn receipt(&self) -> &ActorAppliedReceipt {
        &self.receipt
    }

    #[must_use]
    pub fn replay_state(&self) -> &[u8] {
        &self.replay_state
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct StoredAppliedRecord {
    schema_version: u16,
    sequence: u64,
    previous_sha256: [u8; 32],
    scope: ActorAppliedScope,
    generations: ActorAppliedGenerations,
    turn_sequence: u64,
    wal: DurableWalHead,
    replay_revision: u64,
    applied_private_sequence: u64,
    replay_state_sha256: [u8; 32],
    checkpoint_sha256: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct StoredAppliedCheckpoint {
    schema_version: u16,
    journal_root_sha256: [u8; 32],
    journal_tail_sequence: u64,
    journal_record_count: u64,
    record: StoredAppliedRecord,
    replay_state: Vec<u8>,
}

#[derive(Debug)]
struct JournalReplay {
    records: Vec<StoredAppliedRecord>,
    root_sha256: [u8; 32],
}

impl JournalReplay {
    fn receipt(&self) -> Result<Option<ActorAppliedReceipt>, ActorAppliedError> {
        let Some(record) = self.records.last() else {
            return Ok(None);
        };
        Ok(Some(receipt_from_record(
            record,
            self.root_sha256,
            u64::try_from(self.records.len()).map_err(|_| ActorAppliedError::SequenceExhausted)?,
        )))
    }
}

/// Coupled append-only Actor-applied journal and atomically replaced replay checkpoint.
///
/// This is a storage durability boundary, not a production authority issuer. Callers choose the
/// identity commitments and WAL head supplied to [`Self::commit`]; storage only validates their
/// persistence and monotonic relationship. Runtime integration must derive canonical commitments
/// and obtain the real durable WAL head before these receipts can participate in admission.
#[derive(Debug)]
pub struct ActorAppliedStore {
    journal: DurableJsonl,
    checkpoint: ProjectionStore,
    expected: Option<ActorAppliedReceipt>,
}

impl ActorAppliedStore {
    /// Opens a genuinely new pair. Both paths must be absent; existing empty or partial artifacts
    /// are rejected rather than interpreted as a new authority root.
    pub fn create_new(
        journal_path: impl Into<PathBuf>,
        checkpoint_path: impl Into<PathBuf>,
    ) -> Result<Self, ActorAppliedError> {
        let journal_path = journal_path.into();
        let checkpoint_path = checkpoint_path.into();
        validate_distinct_paths(&journal_path, &checkpoint_path)?;
        if artifact_exists(&journal_path)? || artifact_exists(&checkpoint_path)? {
            return Err(ActorAppliedError::CreateConflict);
        }
        Ok(Self {
            journal: DurableJsonl::new(journal_path),
            checkpoint: ProjectionStore::new(checkpoint_path),
            expected: None,
        })
    }

    /// Reopens an existing pair only when its complete replay exactly matches an external expected
    /// head. The anchor detects pair deletion and rollback, but is not itself an authority.
    pub fn open_existing(
        journal_path: impl Into<PathBuf>,
        checkpoint_path: impl Into<PathBuf>,
        anchor: ActorAppliedAnchor,
    ) -> Result<Self, ActorAppliedError> {
        anchor.validate()?;
        let journal_path = journal_path.into();
        let checkpoint_path = checkpoint_path.into();
        validate_distinct_paths(&journal_path, &checkpoint_path)?;
        let mut store = Self {
            journal: DurableJsonl::new(journal_path),
            checkpoint: ProjectionStore::new(checkpoint_path),
            expected: None,
        };
        let recovered = store
            .load_verified()?
            .ok_or(ActorAppliedError::MissingArtifacts)?;
        if recovered.receipt.anchor() != anchor {
            return Err(ActorAppliedError::AnchorMismatch);
        }
        store.expected = Some(recovered.receipt);
        Ok(store)
    }

    pub fn recover(&self) -> Result<Option<RecoveredActorApplied>, ActorAppliedError> {
        self.load_verified()
    }

    /// Replays both artifacts and proves that `receipt` is still the exact durable head.
    pub fn verify_current(&self, receipt: &ActorAppliedReceipt) -> Result<(), ActorAppliedError> {
        let current = self
            .load_verified()?
            .ok_or(ActorAppliedError::StaleReceipt)?;
        if current.receipt != *receipt {
            return Err(ActorAppliedError::StaleReceipt);
        }
        Ok(())
    }

    /// Persists one direct successor. The receipt is returned only after the journal and its exact
    /// replay checkpoint have both crossed their durability boundaries.
    ///
    /// A crash after journal fsync and before checkpoint replacement deliberately leaves an
    /// unrecoverable mismatch. Subsequent opens fail closed; this API never repairs, truncates, or
    /// promotes that orphan journal record. The caller-provided identity and WAL commitments are
    /// assertions, not production authority evidence.
    pub fn commit(
        &mut self,
        commit: ActorAppliedCommit,
    ) -> Result<ActorAppliedReceipt, ActorAppliedError> {
        let current = self.load_verified()?;
        if current.as_ref().map(|value| &value.receipt) != self.expected.as_ref() {
            return Err(ActorAppliedError::DurableHeadDrift);
        }
        validate_successor(current.as_ref().map(|value| &value.receipt), &commit)?;

        let replay_state_sha256: [u8; 32] = Sha256::digest(commit.replay.bytes()).into();
        let checkpoint_sha256 = checkpoint_commitment(&commit, replay_state_sha256);
        let expected = self.expected.clone();
        let replay_revision = commit.replay.revision;
        let applied_private_sequence = commit.replay.applied_private_sequence;
        let replay_state = commit.replay.bytes;
        let append = self.journal.append(|snapshot| {
            let replay = replay_journal(snapshot)?;
            if replay.receipt()? != expected {
                return Err(ActorAppliedError::DurableHeadDrift);
            }
            let sequence = u64::try_from(replay.records.len())
                .map_err(|_| ActorAppliedError::SequenceExhausted)?
                .checked_add(1)
                .ok_or(ActorAppliedError::SequenceExhausted)?;
            let record = StoredAppliedRecord {
                schema_version: RECORD_SCHEMA_VERSION,
                sequence,
                previous_sha256: replay.root_sha256,
                scope: commit.scope,
                generations: commit.generations,
                turn_sequence: commit.turn_sequence,
                wal: commit.wal,
                replay_revision,
                applied_private_sequence,
                replay_state_sha256,
                checkpoint_sha256,
            };
            let encoded = serde_json::to_vec(&record).map_err(ActorAppliedError::Encode)?;
            let root_sha256 = stored_record_digest(&record);
            Ok(((record, root_sha256), encoded))
        })?;
        let (record, journal_root_sha256) = append;
        let checkpoint = StoredAppliedCheckpoint {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            journal_root_sha256,
            journal_tail_sequence: record.sequence,
            journal_record_count: record.sequence,
            record: record.clone(),
            replay_state,
        };
        self.checkpoint.save(&checkpoint)?;
        let receipt = receipt_from_record(
            &record,
            journal_root_sha256,
            checkpoint.journal_record_count,
        );
        self.expected = Some(receipt.clone());
        Ok(receipt)
    }

    fn load_verified(&self) -> Result<Option<RecoveredActorApplied>, ActorAppliedError> {
        let replay = self.journal.recover(false, replay_journal)?;
        let checkpoint: Option<StoredAppliedCheckpoint> = self.checkpoint.load()?;
        match (replay.records.last(), checkpoint) {
            (None, None) => Ok(None),
            (None, Some(_)) => Err(ActorAppliedError::MissingJournal),
            (Some(_), None) => Err(ActorAppliedError::MissingCheckpoint),
            (Some(record), Some(checkpoint)) => {
                verify_checkpoint(&replay, record, &checkpoint)?;
                let receipt = receipt_from_record(
                    record,
                    replay.root_sha256,
                    u64::try_from(replay.records.len())
                        .map_err(|_| ActorAppliedError::SequenceExhausted)?,
                );
                Ok(Some(RecoveredActorApplied {
                    receipt,
                    replay_state: checkpoint.replay_state,
                }))
            }
        }
    }
}

fn validate_distinct_paths(
    journal_path: &std::path::Path,
    checkpoint_path: &std::path::Path,
) -> Result<(), ActorAppliedError> {
    if journal_path == checkpoint_path {
        return Err(ActorAppliedError::InvalidArtifactPaths);
    }
    Ok(())
}

fn artifact_exists(path: &std::path::Path) -> Result<bool, ActorAppliedError> {
    match fs::metadata(path) {
        Ok(_) => Ok(true),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(StorageError::Io {
            path: path.to_path_buf(),
            source,
        }
        .into()),
    }
}

fn validate_successor(
    previous: Option<&ActorAppliedReceipt>,
    next: &ActorAppliedCommit,
) -> Result<(), ActorAppliedError> {
    next.scope.validate()?;
    next.generations.validate()?;
    next.wal.validate()?;
    next.replay.validate()?;
    if next.turn_sequence == 0 {
        return Err(ActorAppliedError::InvalidTurnSequence);
    }
    let Some(previous) = previous else {
        return Ok(());
    };
    if next.scope != previous.scope {
        return Err(ActorAppliedError::ScopeDrift);
    }
    if !next.generations.does_not_regress_from(previous.generations) {
        return Err(ActorAppliedError::StaleGeneration);
    }
    if previous.turn_sequence.checked_add(1) != Some(next.turn_sequence) {
        return Err(ActorAppliedError::StaleTurn);
    }
    if previous.replay_revision.checked_add(1) != Some(next.replay.revision)
        || next.replay.applied_private_sequence < previous.applied_private_sequence
    {
        return Err(ActorAppliedError::StaleReplay);
    }
    if !next.wal.does_not_drift_from(previous.wal) {
        return Err(ActorAppliedError::WalDrift);
    }
    Ok(())
}

fn replay_journal(snapshot: &JsonlSnapshot) -> Result<JournalReplay, ActorAppliedError> {
    if snapshot.truncated_tail() {
        return Err(ActorAppliedError::TruncatedJournal);
    }
    let mut records = Vec::new();
    let mut root_sha256 = EMPTY_JOURNAL_ROOT;
    for line in snapshot.lines() {
        if line.is_empty() {
            return Err(ActorAppliedError::CorruptJournal);
        }
        let record: StoredAppliedRecord =
            serde_json::from_slice(line).map_err(ActorAppliedError::Decode)?;
        let expected_sequence = u64::try_from(records.len())
            .map_err(|_| ActorAppliedError::SequenceExhausted)?
            .checked_add(1)
            .ok_or(ActorAppliedError::SequenceExhausted)?;
        if record.schema_version != RECORD_SCHEMA_VERSION
            || record.sequence != expected_sequence
            || record.previous_sha256 != root_sha256
            || is_zero_digest(&record.replay_state_sha256)
            || is_zero_digest(&record.checkpoint_sha256)
        {
            return Err(ActorAppliedError::CorruptJournal);
        }
        record.scope.validate()?;
        record.generations.validate()?;
        record.wal.validate()?;
        if record.turn_sequence == 0 || record.replay_revision == 0 {
            return Err(ActorAppliedError::CorruptJournal);
        }
        if let Some(previous) = records.last() {
            validate_stored_successor(previous, &record)?;
        }
        root_sha256 = stored_record_digest(&record);
        records.push(record);
    }
    Ok(JournalReplay {
        records,
        root_sha256,
    })
}

fn validate_stored_successor(
    previous: &StoredAppliedRecord,
    next: &StoredAppliedRecord,
) -> Result<(), ActorAppliedError> {
    if next.scope != previous.scope
        || !next.generations.does_not_regress_from(previous.generations)
        || previous.turn_sequence.checked_add(1) != Some(next.turn_sequence)
        || previous.replay_revision.checked_add(1) != Some(next.replay_revision)
        || next.applied_private_sequence < previous.applied_private_sequence
        || !next.wal.does_not_drift_from(previous.wal)
    {
        return Err(ActorAppliedError::CorruptJournal);
    }
    Ok(())
}

fn verify_checkpoint(
    replay: &JournalReplay,
    record: &StoredAppliedRecord,
    checkpoint: &StoredAppliedCheckpoint,
) -> Result<(), ActorAppliedError> {
    let record_count =
        u64::try_from(replay.records.len()).map_err(|_| ActorAppliedError::SequenceExhausted)?;
    if checkpoint.schema_version != CHECKPOINT_SCHEMA_VERSION
        || checkpoint.journal_root_sha256 != replay.root_sha256
        || checkpoint.journal_tail_sequence != record.sequence
        || checkpoint.journal_record_count != record_count
        || checkpoint.record != *record
        || checkpoint.replay_state.is_empty()
        || checkpoint.replay_state.len() > MAX_REPLAY_STATE_BYTES
        || record.replay_state_sha256 != Sha256::digest(&checkpoint.replay_state).as_slice()
    {
        return Err(ActorAppliedError::CheckpointDrift);
    }
    let replay = ActorAppliedReplayState {
        revision: record.replay_revision,
        applied_private_sequence: record.applied_private_sequence,
        bytes: checkpoint.replay_state.clone(),
    };
    let commit = ActorAppliedCommit {
        scope: record.scope,
        generations: record.generations,
        turn_sequence: record.turn_sequence,
        wal: record.wal,
        replay,
    };
    if checkpoint_commitment(&commit, record.replay_state_sha256) != record.checkpoint_sha256 {
        return Err(ActorAppliedError::CheckpointDrift);
    }
    Ok(())
}

fn receipt_from_record(
    record: &StoredAppliedRecord,
    journal_root_sha256: [u8; 32],
    journal_record_count: u64,
) -> ActorAppliedReceipt {
    ActorAppliedReceipt {
        scope: record.scope,
        generations: record.generations,
        turn_sequence: record.turn_sequence,
        wal: record.wal,
        replay_revision: record.replay_revision,
        applied_private_sequence: record.applied_private_sequence,
        replay_state_sha256: record.replay_state_sha256,
        checkpoint_sha256: record.checkpoint_sha256,
        journal_root_sha256,
        journal_tail_sequence: record.sequence,
        journal_record_count,
    }
}

fn checkpoint_commitment(commit: &ActorAppliedCommit, replay_state_sha256: [u8; 32]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"venue.actor-applied.checkpoint.v1");
    digest.update(commit.scope.actor_sha256);
    digest.update(commit.scope.owner_sha256);
    digest.update(commit.generations.config_epoch.to_le_bytes());
    digest.update(commit.generations.connection_generation.to_le_bytes());
    digest.update(commit.generations.private_generation.to_le_bytes());
    digest.update(commit.turn_sequence.to_le_bytes());
    digest.update(commit.wal.root_sha256);
    digest.update(commit.wal.tail_sequence.to_le_bytes());
    digest.update(commit.wal.record_count.to_le_bytes());
    digest.update(commit.replay.revision.to_le_bytes());
    digest.update(commit.replay.applied_private_sequence.to_le_bytes());
    digest.update(replay_state_sha256);
    digest.finalize().into()
}

fn stored_record_digest(record: &StoredAppliedRecord) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"venue.actor-applied.journal.v1");
    digest.update(record.schema_version.to_le_bytes());
    digest.update(record.sequence.to_le_bytes());
    digest.update(record.previous_sha256);
    digest.update(record.scope.actor_sha256);
    digest.update(record.scope.owner_sha256);
    digest.update(record.generations.config_epoch.to_le_bytes());
    digest.update(record.generations.connection_generation.to_le_bytes());
    digest.update(record.generations.private_generation.to_le_bytes());
    digest.update(record.turn_sequence.to_le_bytes());
    digest.update(record.wal.root_sha256);
    digest.update(record.wal.tail_sequence.to_le_bytes());
    digest.update(record.wal.record_count.to_le_bytes());
    digest.update(record.replay_revision.to_le_bytes());
    digest.update(record.applied_private_sequence.to_le_bytes());
    digest.update(record.replay_state_sha256);
    digest.update(record.checkpoint_sha256);
    digest.finalize().into()
}

fn is_zero_digest(digest: &[u8; 32]) -> bool {
    digest.iter().all(|byte| *byte == 0)
}

#[derive(Debug, thiserror::Error)]
pub enum ActorAppliedError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("Actor-applied journal encoding failed: {0}")]
    Encode(serde_json::Error),
    #[error("Actor-applied journal decoding failed: {0}")]
    Decode(serde_json::Error),
    #[error("Actor and Owner commitments must both be nonzero")]
    InvalidScope,
    #[error("Actor-applied epoch and generations must all be nonzero")]
    InvalidGeneration,
    #[error("Actor-applied turn sequence must be nonzero")]
    InvalidTurnSequence,
    #[error("Actor-applied WAL head is invalid")]
    InvalidWalHead,
    #[error("Actor replay state is empty, oversized, or has no revision")]
    InvalidReplayState,
    #[error("Actor-applied anchor root, checkpoint, or boundary is invalid")]
    InvalidAnchor,
    #[error("Actor-applied journal and checkpoint paths must be distinct")]
    InvalidArtifactPaths,
    #[error("Actor-applied create-new requires both artifact paths to be absent")]
    CreateConflict,
    #[error("Actor-applied journal sequence space is exhausted")]
    SequenceExhausted,
    #[error("Actor-applied journal has an incomplete tail")]
    TruncatedJournal,
    #[error("Actor-applied journal is corrupt")]
    CorruptJournal,
    #[error("Actor-applied checkpoint exists without its journal")]
    MissingJournal,
    #[error("Actor-applied journal exists without its checkpoint")]
    MissingCheckpoint,
    #[error("anchored Actor-applied reopen requires both existing artifacts")]
    MissingArtifacts,
    #[error("Actor-applied artifacts do not match the external expected head")]
    AnchorMismatch,
    #[error("Actor-applied checkpoint does not match the complete journal replay")]
    CheckpointDrift,
    #[error("Actor or Owner binding changed within one applied journal")]
    ScopeDrift,
    #[error("Actor-applied epoch or generation is stale")]
    StaleGeneration,
    #[error("Actor turn is stale or not the direct successor")]
    StaleTurn,
    #[error("Actor replay state is stale or not the direct successor")]
    StaleReplay,
    #[error("durable WAL root or boundary regressed or drifted")]
    WalDrift,
    #[error("Actor-applied durable head changed since this store opened")]
    DurableHeadDrift,
    #[error("Actor-applied receipt is no longer the current durable head")]
    StaleReceipt,
}

#[cfg(test)]
mod tests {
    use std::{fs, fs::OpenOptions, io::Write};

    use tempfile::tempdir;

    use super::*;

    fn digest(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    fn commit(
        turn_sequence: u64,
        config_epoch: u64,
        connection_generation: u64,
        private_generation: u64,
        wal_tail: u64,
        replay_revision: u64,
        applied_private_sequence: u64,
    ) -> Result<ActorAppliedCommit, ActorAppliedError> {
        let wal_count = wal_tail;
        ActorAppliedCommit::new(
            ActorAppliedScope::new(digest(1), digest(2))?,
            ActorAppliedGenerations::new(config_epoch, connection_generation, private_generation)?,
            turn_sequence,
            DurableWalHead::new(digest((wal_tail + 10) as u8), wal_tail, wal_count)?,
            ActorAppliedReplayState::new(
                replay_revision,
                applied_private_sequence,
                format!("state-{replay_revision}").into_bytes(),
            )?,
        )
    }

    #[test]
    fn historical_wal_head_without_a_format_field_deserializes_as_v1()
    -> Result<(), Box<dyn std::error::Error>> {
        let head: DurableWalHead = serde_json::from_value(serde_json::json!({
            "root_sha256": digest(9),
            "tail_sequence": 4,
            "record_count": 3,
        }))?;
        assert_eq!(head.format_version(), DurableWalHeadFormat::V1);
        assert_eq!(head.tail_sequence(), 4);
        assert_eq!(head.record_count(), 3);
        Ok(())
    }

    #[test]
    fn receipt_binds_scope_generations_wal_and_replay_across_restart()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let journal = directory.path().join("actor-applied.jsonl");
        let checkpoint = directory.path().join("actor-checkpoint.json");
        let receipt = {
            let mut store = ActorAppliedStore::create_new(&journal, &checkpoint)?;
            let receipt = store.commit(commit(7, 3, 8, 11, 4, 6, 9)?)?;
            assert_eq!(receipt.scope().actor_sha256(), digest(1));
            assert_eq!(receipt.scope().owner_sha256(), digest(2));
            assert_eq!(receipt.generations().config_epoch(), 3);
            assert_eq!(receipt.generations().connection_generation(), 8);
            assert_eq!(receipt.generations().private_generation(), 11);
            assert_eq!(receipt.turn_sequence(), 7);
            assert_eq!(receipt.wal().root_sha256(), digest(14));
            assert_eq!(receipt.wal().tail_sequence(), 4);
            assert_eq!(receipt.wal().record_count(), 4);
            assert_eq!(receipt.replay_revision(), 6);
            assert_eq!(receipt.applied_private_sequence(), 9);
            assert_eq!(receipt.journal_tail_sequence(), 1);
            assert_eq!(receipt.journal_record_count(), 1);
            receipt
        };

        let reopened = ActorAppliedStore::open_existing(journal, checkpoint, receipt.anchor())?;
        let recovery = reopened.recover()?.ok_or("missing recovery")?;
        assert_eq!(recovery.receipt(), &receipt);
        assert_eq!(recovery.replay_state(), b"state-6");
        reopened.verify_current(&receipt)?;
        Ok(())
    }

    #[test]
    fn anchored_reopen_rejects_missing_pair_and_create_new_rejects_existing_pair()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let journal = directory.path().join("actor-applied.jsonl");
        let checkpoint = directory.path().join("actor-checkpoint.json");
        let mut store = ActorAppliedStore::create_new(&journal, &checkpoint)?;
        let receipt = store.commit(commit(1, 1, 1, 1, 0, 1, 0)?)?;
        assert!(matches!(
            ActorAppliedStore::create_new(&journal, &checkpoint),
            Err(ActorAppliedError::CreateConflict)
        ));
        drop(store);

        fs::remove_file(&journal)?;
        fs::remove_file(&checkpoint)?;
        assert!(matches!(
            ActorAppliedStore::open_existing(&journal, &checkpoint, receipt.anchor()),
            Err(ActorAppliedError::MissingArtifacts)
        ));
        Ok(())
    }

    #[test]
    fn missing_peer_artifact_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        for missing_checkpoint in [true, false] {
            let directory = tempdir()?;
            let journal = directory.path().join("actor-applied.jsonl");
            let checkpoint = directory.path().join("actor-checkpoint.json");
            let mut store = ActorAppliedStore::create_new(&journal, &checkpoint)?;
            let receipt = store.commit(commit(1, 1, 1, 1, 0, 1, 0)?)?;
            drop(store);
            if missing_checkpoint {
                fs::remove_file(&checkpoint)?;
                assert!(matches!(
                    ActorAppliedStore::open_existing(&journal, &checkpoint, receipt.anchor()),
                    Err(ActorAppliedError::MissingCheckpoint)
                ));
            } else {
                fs::remove_file(&journal)?;
                assert!(matches!(
                    ActorAppliedStore::open_existing(&journal, &checkpoint, receipt.anchor()),
                    Err(ActorAppliedError::MissingJournal)
                ));
            }
        }
        Ok(())
    }

    #[test]
    fn incomplete_journal_tail_is_never_repaired() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let journal = directory.path().join("actor-applied.jsonl");
        let checkpoint = directory.path().join("actor-checkpoint.json");
        let mut store = ActorAppliedStore::create_new(&journal, &checkpoint)?;
        let receipt = store.commit(commit(1, 1, 1, 1, 0, 1, 0)?)?;
        drop(store);
        let mut file = OpenOptions::new().append(true).open(&journal)?;
        file.write_all(b"{\"schema_version\":1")?;
        file.sync_all()?;
        drop(file);
        let corrupted = fs::read(&journal)?;

        assert!(matches!(
            ActorAppliedStore::open_existing(&journal, &checkpoint, receipt.anchor()),
            Err(ActorAppliedError::TruncatedJournal)
        ));
        assert_eq!(fs::read(journal)?, corrupted);
        Ok(())
    }

    #[test]
    fn checkpoint_replay_drift_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let journal = directory.path().join("actor-applied.jsonl");
        let checkpoint = directory.path().join("actor-checkpoint.json");
        let mut store = ActorAppliedStore::create_new(&journal, &checkpoint)?;
        let receipt = store.commit(commit(1, 1, 1, 1, 0, 1, 0)?)?;
        drop(store);

        let bytes = fs::read(&checkpoint)?;
        let mut value: serde_json::Value = serde_json::from_slice(&bytes)?;
        value["replay_state"][0] = serde_json::json!(0);
        fs::write(&checkpoint, serde_json::to_vec(&value)?)?;
        assert!(matches!(
            ActorAppliedStore::open_existing(journal, checkpoint, receipt.anchor()),
            Err(ActorAppliedError::CheckpointDrift)
        ));
        Ok(())
    }

    #[test]
    fn truncated_checkpoint_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let journal = directory.path().join("actor-applied.jsonl");
        let checkpoint = directory.path().join("actor-checkpoint.json");
        let mut store = ActorAppliedStore::create_new(&journal, &checkpoint)?;
        let receipt = store.commit(commit(1, 1, 1, 1, 0, 1, 0)?)?;
        drop(store);
        let bytes = fs::read(&checkpoint)?;
        fs::write(&checkpoint, &bytes[..bytes.len() / 2])?;

        assert!(matches!(
            ActorAppliedStore::open_existing(journal, checkpoint, receipt.anchor()),
            Err(ActorAppliedError::Storage(StorageError::Decode(_)))
        ));
        Ok(())
    }

    #[test]
    fn journal_fsync_without_checkpoint_replacement_stays_failed_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let journal = directory.path().join("actor-applied.jsonl");
        let checkpoint = directory.path().join("actor-checkpoint.json");
        let mut store = ActorAppliedStore::create_new(&journal, &checkpoint)?;
        store.commit(commit(1, 1, 1, 1, 0, 1, 0)?)?;
        let old_checkpoint = fs::read(&checkpoint)?;
        let latest = store.commit(commit(2, 1, 2, 2, 1, 2, 1)?)?;
        let durable_journal = fs::read(&journal)?;
        drop(store);

        // This is the observable crash state after record 2 is durable but checkpoint replacement
        // did not occur: the journal is at N while the checkpoint remains at N-1.
        fs::write(&checkpoint, old_checkpoint)?;
        assert!(matches!(
            ActorAppliedStore::open_existing(&journal, &checkpoint, latest.anchor()),
            Err(ActorAppliedError::CheckpointDrift)
        ));
        assert_eq!(fs::read(journal)?, durable_journal);
        Ok(())
    }

    #[test]
    fn external_anchor_rejects_an_internally_consistent_old_pair()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let journal = directory.path().join("actor-applied.jsonl");
        let checkpoint = directory.path().join("actor-checkpoint.json");
        let mut store = ActorAppliedStore::create_new(&journal, &checkpoint)?;
        let old_receipt = store.commit(commit(1, 1, 1, 1, 0, 1, 0)?)?;
        let old_journal = fs::read(&journal)?;
        let old_checkpoint = fs::read(&checkpoint)?;
        let latest = store.commit(commit(2, 1, 2, 2, 1, 2, 1)?)?;
        assert_ne!(old_receipt.anchor(), latest.anchor());
        drop(store);

        fs::write(&journal, old_journal)?;
        fs::write(&checkpoint, old_checkpoint)?;
        assert!(matches!(
            ActorAppliedStore::open_existing(journal, checkpoint, latest.anchor()),
            Err(ActorAppliedError::AnchorMismatch)
        ));
        Ok(())
    }

    #[test]
    fn complete_journal_tampering_fails_closed_without_rewriting()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let journal = directory.path().join("actor-applied.jsonl");
        let checkpoint = directory.path().join("actor-checkpoint.json");
        let mut store = ActorAppliedStore::create_new(&journal, &checkpoint)?;
        store.commit(commit(1, 1, 1, 1, 0, 1, 0)?)?;
        let receipt = store.commit(commit(2, 1, 2, 2, 1, 2, 1)?)?;
        drop(store);

        let bytes = fs::read(&journal)?;
        let mut lines = bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(serde_json::from_slice::<serde_json::Value>)
            .collect::<Result<Vec<_>, _>>()?;
        lines[1]["previous_sha256"][0] = serde_json::json!(99);
        let mut tampered = Vec::new();
        for line in lines {
            serde_json::to_writer(&mut tampered, &line)?;
            tampered.push(b'\n');
        }
        fs::write(&journal, &tampered)?;

        assert!(matches!(
            ActorAppliedStore::open_existing(&journal, &checkpoint, receipt.anchor()),
            Err(ActorAppliedError::CorruptJournal)
        ));
        assert_eq!(fs::read(journal)?, tampered);
        Ok(())
    }

    #[test]
    fn stale_generation_replay_wal_and_turn_are_rejected_without_writes()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let journal = directory.path().join("actor-applied.jsonl");
        let checkpoint = directory.path().join("actor-checkpoint.json");
        let mut store = ActorAppliedStore::create_new(&journal, &checkpoint)?;
        store.commit(commit(4, 2, 5, 7, 3, 9, 10)?)?;
        let journal_before = fs::read(&journal)?;
        let checkpoint_before = fs::read(&checkpoint)?;

        assert!(matches!(
            store.commit(commit(5, 2, 4, 7, 3, 10, 10)?),
            Err(ActorAppliedError::StaleGeneration)
        ));
        assert!(matches!(
            store.commit(commit(6, 2, 5, 7, 3, 10, 10)?),
            Err(ActorAppliedError::StaleTurn)
        ));
        assert!(matches!(
            store.commit(commit(5, 2, 5, 7, 3, 11, 10)?),
            Err(ActorAppliedError::StaleReplay)
        ));
        let mut wal_regression = commit(5, 2, 5, 7, 2, 10, 10)?;
        wal_regression.wal.root_sha256 = digest(12);
        assert!(matches!(
            store.commit(wal_regression),
            Err(ActorAppliedError::WalDrift)
        ));
        let mut same_boundary_changed_root = commit(5, 2, 5, 7, 3, 10, 10)?;
        same_boundary_changed_root.wal.root_sha256 = digest(99);
        assert!(matches!(
            store.commit(same_boundary_changed_root),
            Err(ActorAppliedError::WalDrift)
        ));
        let mut changed_boundary_same_root = commit(5, 2, 5, 7, 4, 10, 10)?;
        changed_boundary_same_root.wal.root_sha256 = digest(13);
        assert!(matches!(
            store.commit(changed_boundary_same_root),
            Err(ActorAppliedError::WalDrift)
        ));
        let mut scope_drift = commit(5, 2, 5, 7, 3, 10, 10)?;
        scope_drift.scope = ActorAppliedScope::new(digest(3), digest(2))?;
        assert!(matches!(
            store.commit(scope_drift),
            Err(ActorAppliedError::ScopeDrift)
        ));
        assert_eq!(fs::read(journal)?, journal_before);
        assert_eq!(fs::read(checkpoint)?, checkpoint_before);
        Ok(())
    }

    #[test]
    fn stale_store_and_stale_receipt_are_fenced() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let journal = directory.path().join("actor-applied.jsonl");
        let checkpoint = directory.path().join("actor-checkpoint.json");
        let mut first = ActorAppliedStore::create_new(&journal, &checkpoint)?;
        let mut stale = ActorAppliedStore::create_new(&journal, &checkpoint)?;
        let first_receipt = first.commit(commit(1, 1, 1, 1, 0, 1, 0)?)?;
        assert!(matches!(
            stale.commit(commit(2, 1, 1, 1, 1, 2, 1)?),
            Err(ActorAppliedError::DurableHeadDrift)
        ));
        let second_receipt = first.commit(commit(2, 1, 2, 2, 1, 2, 1)?)?;
        assert!(matches!(
            first.verify_current(&first_receipt),
            Err(ActorAppliedError::StaleReceipt)
        ));
        first.verify_current(&second_receipt)?;
        Ok(())
    }
}
