use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    domain::Symbol,
    strategy::scalping::{RiskFact, RiskUnit},
};

/// The immutable identity shared by one owner/release logical-risk stream.
/// A valuation generation is deliberately part of every record, rather than a mutable journal
/// setting: a generation change must remain visible to recovery.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScalpingRiskBinding {
    pub exchange: String,
    pub account: String,
    pub owner_scope: String,
    pub strategy_instance_id: String,
    pub run_id: String,
    pub parameter_release_id: String,
    pub symbol: Symbol,
    pub risk_unit: RiskUnit,
    pub valuation_generation: u64,
}

impl ScalpingRiskBinding {
    fn validate(&self) -> Result<(), ScalpingRiskError> {
        if self.exchange.trim().is_empty()
            || self.account.trim().is_empty()
            || self.owner_scope.trim().is_empty()
            || self.strategy_instance_id.trim().is_empty()
            || self.run_id.trim().is_empty()
            || self.parameter_release_id.trim().is_empty()
            || self.risk_unit.as_str().is_empty()
            || self.valuation_generation == 0
        {
            return Err(ScalpingRiskError::Binding);
        }
        Ok(())
    }

    fn scope(&self) -> ScalpingRiskScope {
        ScalpingRiskScope {
            exchange: self.exchange.clone(),
            account: self.account.clone(),
            owner_scope: self.owner_scope.clone(),
            strategy_instance_id: self.strategy_instance_id.clone(),
            run_id: self.run_id.clone(),
            parameter_release_id: self.parameter_release_id.clone(),
            symbol: self.symbol.clone(),
            risk_unit: self.risk_unit.clone(),
        }
    }
}

/// A logical risk fact already valued by the authoritative producer. This journal never derives
/// an amount from quotes, balances, fills, or exchange quantities.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScalpingRiskFact {
    pub binding: ScalpingRiskBinding,
    pub fact: RiskFact,
}

impl ScalpingRiskFact {
    fn validate(&self) -> Result<(), ScalpingRiskError> {
        self.binding.validate()?;
        if self.fact.fact_id.trim().is_empty()
            || self.fact.event_time_ms == 0
            || self.fact.valuation_generation != self.binding.valuation_generation
            || self.fact.risk_unit != self.binding.risk_unit
        {
            return Err(ScalpingRiskError::Fact);
        }
        Ok(())
    }
}

/// A source watermark. Its listed fact ids are committed only after every listed fact is durable.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScalpingRiskCursor {
    pub cursor_id: String,
    pub binding: ScalpingRiskBinding,
    pub source_sequence: u64,
    pub complete_from_ms: u64,
    pub observed_through_ms: u64,
    pub has_more: bool,
    pub source_fact_ids: Vec<String>,
}

impl ScalpingRiskCursor {
    fn validate(&self) -> Result<(), ScalpingRiskError> {
        self.binding.validate()?;
        if self.cursor_id.trim().is_empty()
            || self.observed_through_ms == 0
            || self.complete_from_ms > self.observed_through_ms
            || self
                .source_fact_ids
                .iter()
                .any(|fact_id| fact_id.trim().is_empty())
            || self.source_fact_ids.iter().collect::<BTreeSet<_>>().len()
                != self.source_fact_ids.len()
        {
            return Err(ScalpingRiskError::Cursor);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "payload")]
pub enum ScalpingRiskEntry {
    Fact(ScalpingRiskFact),
    Cursor(ScalpingRiskCursor),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScalpingRiskRecord {
    pub sequence: u64,
    pub content_sha256: String,
    pub entry: ScalpingRiskEntry,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingRiskRecovery {
    pub records: Vec<ScalpingRiskRecord>,
    pub truncated_tail: bool,
}

/// Result of a facts-first page commit. Fact sequences align with the input facts, including
/// already durable idempotent retries; the cursor sequence is written last.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingRiskCommit {
    pub fact_sequences: Vec<u64>,
    pub cursor_sequence: u64,
}

/// One replay page that became visible only when its cursor was durably appended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingRiskReplay {
    pub cursor: ScalpingRiskCursor,
    pub cursor_sequence: u64,
    pub facts: Vec<ScalpingRiskFact>,
}

#[derive(Debug)]
pub struct ScalpingRiskJournal {
    path: PathBuf,
    next_sequence: u64,
    state: JournalState,
}

impl ScalpingRiskJournal {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, ScalpingRiskError> {
        let path = path.into();
        let recovery = recover_file(&path)?;
        let state = JournalState::from_records(&recovery.records)?;
        let next_sequence = recovery
            .records
            .last()
            .map(|record| {
                record
                    .sequence
                    .checked_add(1)
                    .ok_or(ScalpingRiskError::Sequence)
            })
            .transpose()?
            .unwrap_or(1);
        if recovery.truncated_tail {
            truncate_tail(
                &path,
                complete_length(&fs::read(&path).map_err(|source| ScalpingRiskError::Io {
                    path: path.clone(),
                    source,
                })?),
            )?;
        }
        Ok(Self {
            path,
            next_sequence,
            state,
        })
    }

    /// Writes every fact with fsync before it writes the page cursor. Retrying the same page
    /// after a crash returns the original sequences and completes a missing cursor write.
    pub fn append_page(
        &mut self,
        facts: Vec<ScalpingRiskFact>,
        cursor: ScalpingRiskCursor,
    ) -> Result<ScalpingRiskCommit, ScalpingRiskError> {
        cursor.validate()?;
        let cursor_digest = digest_entry(&ScalpingRiskEntry::Cursor(cursor.clone()))?;
        let cursor_existing = self.state.cursor_ids.get(&cursor.cursor_id).cloned();
        if let Some(existing) = &cursor_existing {
            if existing.digest != cursor_digest {
                return Err(ScalpingRiskError::ConflictingCursor);
            }
        } else {
            self.state.validate_scope(&cursor.binding)?;
            self.state.validate_cursor_successor(&cursor)?;
        }

        let supplied_ids = facts
            .iter()
            .map(|fact| fact.fact.fact_id.clone())
            .collect::<BTreeSet<_>>();
        let cursor_ids = cursor
            .source_fact_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if !supplied_ids.is_subset(&cursor_ids) {
            return Err(ScalpingRiskError::CursorFacts);
        }
        if supplied_ids.len() != facts.len() {
            return Err(ScalpingRiskError::DuplicatePageFact);
        }
        let mut supplied_digests = BTreeMap::new();
        for fact in &facts {
            fact.validate()?;
            if fact.binding != cursor.binding
                || fact.fact.event_time_ms < cursor.complete_from_ms
                || fact.fact.event_time_ms > cursor.observed_through_ms
            {
                return Err(ScalpingRiskError::CursorFacts);
            }
            let digest = digest_entry(&ScalpingRiskEntry::Fact(fact.clone()))?;
            match supplied_digests.insert(fact.fact.fact_id.clone(), digest.clone()) {
                Some(previous) if previous != digest => {
                    return Err(ScalpingRiskError::ConflictingFact);
                }
                _ => {}
            }
            if self
                .state
                .fact_ids
                .get(&FactKey::from_fact(&fact.fact))
                .is_some_and(|existing| existing.digest != digest)
            {
                return Err(ScalpingRiskError::ConflictingFact);
            }
        }

        let mut fact_sequences = Vec::with_capacity(facts.len());
        for fact in facts {
            fact_sequences.push(self.append_fact(fact)?);
        }
        self.state.validate_cursor_sources(&cursor)?;
        let cursor_sequence = match cursor_existing {
            Some(existing) => existing.sequence,
            None => self.append_cursor(cursor, cursor_digest)?,
        };
        Ok(ScalpingRiskCommit {
            fact_sequences,
            cursor_sequence,
        })
    }

    pub fn recover(&self) -> Result<ScalpingRiskRecovery, ScalpingRiskError> {
        recover_file(&self.path)
    }

    /// Returns only pages whose cursor is durable. Facts written before a crash without their
    /// cursor remain in the journal for retry deduplication but are not delivered to consumers.
    pub fn committed_replays(&self) -> Result<Vec<ScalpingRiskReplay>, ScalpingRiskError> {
        self.recover_committed_replays()
    }

    pub fn recover_committed_replays(&self) -> Result<Vec<ScalpingRiskReplay>, ScalpingRiskError> {
        let recovery = self.recover()?;
        JournalState::from_records(&recovery.records)?;
        let mut facts = BTreeMap::new();
        let mut replayed_cursors = BTreeSet::new();
        let mut replays = Vec::new();
        for record in recovery.records {
            match record.entry {
                ScalpingRiskEntry::Fact(fact) => {
                    facts.entry(FactKey::from_fact(&fact.fact)).or_insert(fact);
                }
                ScalpingRiskEntry::Cursor(cursor) => {
                    if !replayed_cursors.insert(cursor.cursor_id.clone()) {
                        continue;
                    }
                    let replay_facts = cursor
                        .source_fact_ids
                        .iter()
                        .map(|fact_id| {
                            facts
                                .get(&FactKey::from_cursor(&cursor.binding, fact_id))
                                .cloned()
                                .ok_or(ScalpingRiskError::CursorFacts)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    if replay_facts.iter().any(|fact| {
                        fact.binding != cursor.binding
                            || fact.fact.event_time_ms < cursor.complete_from_ms
                            || fact.fact.event_time_ms > cursor.observed_through_ms
                    }) {
                        return Err(ScalpingRiskError::CursorFacts);
                    }
                    replays.push(ScalpingRiskReplay {
                        cursor,
                        cursor_sequence: record.sequence,
                        facts: replay_facts,
                    });
                }
            }
        }
        Ok(replays)
    }

    fn append_fact(&mut self, fact: ScalpingRiskFact) -> Result<u64, ScalpingRiskError> {
        let entry = ScalpingRiskEntry::Fact(fact.clone());
        let digest = digest_entry(&entry)?;
        if let Some(existing) = self.state.fact_ids.get(&FactKey::from_fact(&fact.fact)) {
            if existing.digest == digest {
                return Ok(existing.sequence);
            }
            return Err(ScalpingRiskError::ConflictingFact);
        }
        self.state.validate_scope(&fact.binding)?;
        let sequence = self.append_entry(entry, digest.clone())?;
        self.state.insert_fact(fact, digest, sequence)?;
        Ok(sequence)
    }

    fn append_cursor(
        &mut self,
        cursor: ScalpingRiskCursor,
        digest: String,
    ) -> Result<u64, ScalpingRiskError> {
        let sequence =
            self.append_entry(ScalpingRiskEntry::Cursor(cursor.clone()), digest.clone())?;
        self.state.insert_cursor(cursor, digest, sequence)?;
        Ok(sequence)
    }

    fn append_entry(
        &mut self,
        entry: ScalpingRiskEntry,
        content_sha256: String,
    ) -> Result<u64, ScalpingRiskError> {
        let following_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(ScalpingRiskError::Sequence)?;
        let record = ScalpingRiskRecord {
            sequence: self.next_sequence,
            content_sha256,
            entry,
        };
        let encoded = serde_json::to_vec(&record).map_err(ScalpingRiskError::Encode)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|source| ScalpingRiskError::Io {
                path: self.path.clone(),
                source,
            })?;
        file.write_all(&encoded)
            .and_then(|()| file.write_all(b"\n"))
            .and_then(|()| file.sync_data())
            .map_err(|source| ScalpingRiskError::Io {
                path: self.path.clone(),
                source,
            })?;
        let sequence = self.next_sequence;
        self.next_sequence = following_sequence;
        Ok(sequence)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ScalpingRiskScope {
    exchange: String,
    account: String,
    owner_scope: String,
    strategy_instance_id: String,
    run_id: String,
    parameter_release_id: String,
    symbol: Symbol,
    risk_unit: RiskUnit,
}

#[derive(Clone, Debug)]
struct Identity {
    digest: String,
    sequence: u64,
    binding: ScalpingRiskBinding,
    event_time_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct FactKey {
    valuation_generation: u64,
    fact_id: String,
}

impl FactKey {
    fn from_fact(fact: &RiskFact) -> Self {
        Self {
            valuation_generation: fact.valuation_generation,
            fact_id: fact.fact_id.clone(),
        }
    }

    fn from_cursor(binding: &ScalpingRiskBinding, fact_id: &str) -> Self {
        Self {
            valuation_generation: binding.valuation_generation,
            fact_id: fact_id.to_owned(),
        }
    }
}

#[derive(Default, Debug)]
struct JournalState {
    scope: Option<ScalpingRiskScope>,
    fact_ids: BTreeMap<FactKey, Identity>,
    cursor_ids: BTreeMap<String, Identity>,
    last_cursors: BTreeMap<u64, ScalpingRiskCursor>,
}

impl JournalState {
    fn from_records(records: &[ScalpingRiskRecord]) -> Result<Self, ScalpingRiskError> {
        let mut state = Self::default();
        let mut expected = 1_u64;
        for record in records {
            if record.sequence != expected || record.content_sha256 != digest_entry(&record.entry)?
            {
                return Err(ScalpingRiskError::Sequence);
            }
            expected = expected.checked_add(1).ok_or(ScalpingRiskError::Sequence)?;
            match &record.entry {
                ScalpingRiskEntry::Fact(fact) => {
                    fact.validate()?;
                    state.insert_fact(
                        fact.clone(),
                        record.content_sha256.clone(),
                        record.sequence,
                    )?;
                }
                ScalpingRiskEntry::Cursor(cursor) => {
                    cursor.validate()?;
                    state.validate_cursor_sources(cursor)?;
                    state.insert_cursor(
                        cursor.clone(),
                        record.content_sha256.clone(),
                        record.sequence,
                    )?;
                }
            }
        }
        Ok(state)
    }

    fn validate_scope(&self, binding: &ScalpingRiskBinding) -> Result<(), ScalpingRiskError> {
        binding.validate()?;
        let scope = binding.scope();
        match &self.scope {
            Some(current) if current != &scope => Err(ScalpingRiskError::Scope),
            Some(_) | None => Ok(()),
        }
    }

    fn insert_fact(
        &mut self,
        fact: ScalpingRiskFact,
        digest: String,
        sequence: u64,
    ) -> Result<(), ScalpingRiskError> {
        let key = FactKey::from_fact(&fact.fact);
        match self.fact_ids.get(&key) {
            Some(existing) if existing.digest == digest => Ok(()),
            Some(_) => Err(ScalpingRiskError::ConflictingFact),
            None => {
                self.validate_scope(&fact.binding)?;
                if self.scope.is_none() {
                    self.scope = Some(fact.binding.scope());
                }
                self.fact_ids.insert(
                    key,
                    Identity {
                        digest,
                        sequence,
                        binding: fact.binding,
                        event_time_ms: Some(fact.fact.event_time_ms),
                    },
                );
                Ok(())
            }
        }
    }

    fn validate_cursor_sources(
        &self,
        cursor: &ScalpingRiskCursor,
    ) -> Result<(), ScalpingRiskError> {
        if cursor.source_fact_ids.iter().any(|fact_id| {
            self.fact_ids
                .get(&FactKey::from_cursor(&cursor.binding, fact_id))
                .is_none_or(|identity| {
                    identity.binding != cursor.binding
                        || identity.event_time_ms.is_none_or(|event_time_ms| {
                            event_time_ms < cursor.complete_from_ms
                                || event_time_ms > cursor.observed_through_ms
                        })
                })
        }) {
            return Err(ScalpingRiskError::CursorFacts);
        }
        Ok(())
    }

    fn validate_cursor_successor(
        &self,
        cursor: &ScalpingRiskCursor,
    ) -> Result<(), ScalpingRiskError> {
        let Some(previous) = self.last_cursors.get(&cursor.binding.valuation_generation) else {
            return Ok(());
        };
        if cursor.source_sequence < previous.source_sequence
            || cursor.observed_through_ms < previous.observed_through_ms
            || (cursor.source_sequence == previous.source_sequence
                && (cursor.has_more || !previous.has_more))
        {
            return Err(ScalpingRiskError::CursorRegression);
        }
        Ok(())
    }

    fn insert_cursor(
        &mut self,
        cursor: ScalpingRiskCursor,
        digest: String,
        sequence: u64,
    ) -> Result<(), ScalpingRiskError> {
        match self.cursor_ids.get(&cursor.cursor_id) {
            Some(existing) if existing.digest == digest => return Ok(()),
            Some(_) => return Err(ScalpingRiskError::ConflictingCursor),
            None => {}
        }
        self.validate_scope(&cursor.binding)?;
        self.validate_cursor_successor(&cursor)?;
        if self.scope.is_none() {
            self.scope = Some(cursor.binding.scope());
        }
        self.cursor_ids.insert(
            cursor.cursor_id.clone(),
            Identity {
                digest,
                sequence,
                binding: cursor.binding.clone(),
                event_time_ms: None,
            },
        );
        self.last_cursors
            .insert(cursor.binding.valuation_generation, cursor);
        Ok(())
    }
}

fn digest_entry(entry: &ScalpingRiskEntry) -> Result<String, ScalpingRiskError> {
    let bytes = serde_json::to_vec(entry).map_err(ScalpingRiskError::Encode)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn recover_file(path: &Path) -> Result<ScalpingRiskRecovery, ScalpingRiskError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ScalpingRiskRecovery {
                records: Vec::new(),
                truncated_tail: false,
            });
        }
        Err(source) => {
            return Err(ScalpingRiskError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    let complete = complete_length(&bytes);
    let mut records = Vec::new();
    for line in bytes[..complete]
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        records.push(serde_json::from_slice(line).map_err(ScalpingRiskError::Decode)?);
    }
    Ok(ScalpingRiskRecovery {
        records,
        truncated_tail: complete != bytes.len(),
    })
}

fn complete_length(bytes: &[u8]) -> usize {
    bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1)
}

fn truncate_tail(path: &Path, length: usize) -> Result<(), ScalpingRiskError> {
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|source| ScalpingRiskError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    file.set_len(u64::try_from(length).map_err(|_| ScalpingRiskError::Sequence)?)
        .and_then(|()| file.sync_data())
        .map_err(|source| ScalpingRiskError::Io {
            path: path.to_path_buf(),
            source,
        })
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingRiskError {
    #[error("scalping risk binding is invalid")]
    Binding,
    #[error("scalping risk fact is invalid or incompatible with its binding")]
    Fact,
    #[error("scalping risk cursor is invalid")]
    Cursor,
    #[error("scalping risk cursor does not exactly follow its durable facts")]
    CursorFacts,
    #[error("scalping risk cursor regresses")]
    CursorRegression,
    #[error(
        "scalping risk journal mixes exchange, account, strategy, owner, release, symbol, or logical risk unit"
    )]
    Scope,
    #[error("scalping risk fact id was reused with different content")]
    ConflictingFact,
    #[error("scalping risk page repeats a fact id")]
    DuplicatePageFact,
    #[error("scalping risk cursor id was reused with different content")]
    ConflictingCursor,
    #[error("scalping risk journal sequence or hash is invalid")]
    Sequence,
    #[error("scalping risk journal I/O failed for {path}: {source}", path = path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("scalping risk journal encoding failed: {0}")]
    Encode(serde_json::Error),
    #[error("scalping risk journal decoding failed: {0}")]
    Decode(serde_json::Error),
}
