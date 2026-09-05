use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    StorageError,
    checkpoint::sync_parent,
    journal::{DurableJsonl, JsonlSnapshot},
};

const RECORD_SCHEMA_VERSION: u16 = 1;
const FIRST_RECORD_HASH: [u8; 32] = [0; 32];
const MAX_PAYLOAD_BYTES: usize = 2 * 1024 * 1024;

/// An opaque record recovered from the durable control-delivery journal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpaqueJournalRecord {
    pub sequence: u64,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct StoredRecord {
    schema_version: u16,
    sequence: u64,
    previous_sha256: [u8; 32],
    payload_sha256: [u8; 32],
    payload: Vec<u8>,
}

#[derive(Debug)]
struct Replay {
    records: Vec<OpaqueJournalRecord>,
    next_sequence: u64,
    tail_sha256: [u8; 32],
}

/// A file-backed, sequenced journal for payloads whose schema belongs to the caller.
///
/// The journal owns only durability, sequence fencing, and integrity. An append succeeds only
/// after the exact expected sequence has been checked under the file lock and the new record has
/// crossed `sync_data`.
#[derive(Debug)]
pub struct OpaqueJournal {
    jsonl: DurableJsonl,
    path: PathBuf,
}

impl OpaqueJournal {
    pub fn open(path: impl Into<std::path::PathBuf>) -> Result<Self, OpaqueJournalError> {
        let path = path.into();
        let _operation = operation_guard(&path)?;
        recover_interrupted_compaction(&path)?;
        let journal = Self {
            jsonl: DurableJsonl::new(path.clone()),
            path,
        };
        journal.jsonl.recover(true, replay)?;
        discard_completed_compaction_files(&journal.path)?;
        Ok(journal)
    }

    pub fn recover(&mut self) -> Result<Vec<OpaqueJournalRecord>, OpaqueJournalError> {
        let _operation = operation_guard(&self.path)?;
        Ok(self.jsonl.recover(true, replay)?.records)
    }

    pub fn append(
        &mut self,
        expected_sequence: u64,
        payload: &[u8],
    ) -> Result<u64, OpaqueJournalError> {
        self.append_inner(expected_sequence, payload, None)
    }

    pub fn append_bounded(
        &mut self,
        expected_sequence: u64,
        payload: &[u8],
        maximum_file_bytes: u64,
    ) -> Result<u64, OpaqueJournalError> {
        self.append_inner(expected_sequence, payload, Some(maximum_file_bytes))
    }

    fn append_inner(
        &mut self,
        expected_sequence: u64,
        payload: &[u8],
        maximum_file_bytes: Option<u64>,
    ) -> Result<u64, OpaqueJournalError> {
        if payload.len() > MAX_PAYLOAD_BYTES {
            return Err(OpaqueJournalError::RecordTooLarge);
        }
        let _operation = operation_guard(&self.path)?;
        self.jsonl.append(|snapshot| {
            let replay = replay(snapshot)?;
            if replay.next_sequence != expected_sequence {
                return Err(OpaqueJournalError::SequenceConflict {
                    expected: expected_sequence,
                    actual: replay.next_sequence,
                });
            }
            if expected_sequence == u64::MAX {
                return Err(OpaqueJournalError::SequenceExhausted);
            }
            let stored = StoredRecord {
                schema_version: RECORD_SCHEMA_VERSION,
                sequence: expected_sequence,
                previous_sha256: replay.tail_sha256,
                payload_sha256: Sha256::digest(payload).into(),
                payload: payload.to_vec(),
            };
            let encoded = serde_json::to_vec(&stored).map_err(OpaqueJournalError::Encode)?;
            if maximum_file_bytes.is_some_and(|maximum| {
                u64::try_from(encoded.len())
                    .ok()
                    .and_then(|length| length.checked_add(1))
                    .and_then(|length| snapshot.complete_length().checked_add(length))
                    .is_none_or(|length| length > maximum)
            }) {
                return Err(OpaqueJournalError::FileLimitExceeded);
            }
            Ok((expected_sequence, encoded))
        })
    }

    /// Replaces a fully acknowledged transport history with a caller-supplied minimal replay
    /// state. The exact old next sequence fences the rewrite. Stable sibling files make each
    /// crash point recoverable: before the swap the old journal wins; after the swap the fully
    /// fsynced replacement wins.
    pub fn compact(
        &mut self,
        expected_next_sequence: u64,
        payloads: &[Vec<u8>],
    ) -> Result<(), OpaqueJournalError> {
        self.compact_inner(expected_next_sequence, payloads, None)
    }

    pub fn compact_bounded(
        &mut self,
        expected_next_sequence: u64,
        payloads: &[Vec<u8>],
        maximum_file_bytes: u64,
    ) -> Result<(), OpaqueJournalError> {
        self.compact_inner(expected_next_sequence, payloads, Some(maximum_file_bytes))
    }

    fn compact_inner(
        &mut self,
        expected_next_sequence: u64,
        payloads: &[Vec<u8>],
        maximum_file_bytes: Option<u64>,
    ) -> Result<(), OpaqueJournalError> {
        if payloads.is_empty()
            || payloads
                .iter()
                .any(|payload| payload.len() > MAX_PAYLOAD_BYTES)
        {
            return Err(OpaqueJournalError::RecordTooLarge);
        }
        let _operation = operation_guard(&self.path)?;
        let current = self.jsonl.recover(true, replay)?;
        if current.next_sequence != expected_next_sequence {
            return Err(OpaqueJournalError::SequenceConflict {
                expected: expected_next_sequence,
                actual: current.next_sequence,
            });
        }
        let next = compaction_path(&self.path, "next");
        let previous = compaction_path(&self.path, "previous");
        if next.exists() || previous.exists() {
            return Err(OpaqueJournalError::Corrupt);
        }
        let encoded = encode_replacement(payloads)?;
        if maximum_file_bytes.is_some_and(|maximum| {
            u64::try_from(encoded.len()).map_or(true, |length| length > maximum)
        }) {
            return Err(OpaqueJournalError::FileLimitExceeded);
        }
        let write_result = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&next)
                .map_err(|source| io_error(&next, source))?;
            file.write_all(&encoded)
                .and_then(|()| file.sync_all())
                .map_err(|source| io_error(&next, source))?;
            sync_parent(&next)?;
            fs::rename(&self.path, &previous).map_err(|source| io_error(&self.path, source))?;
            if let Err(source) = fs::rename(&next, &self.path) {
                let _ = fs::rename(&previous, &self.path);
                return Err(io_error(&self.path, source));
            }
            sync_parent(&self.path)?;
            fs::remove_file(&previous).map_err(|source| io_error(&previous, source))?;
            sync_parent(&self.path)
        })();
        if write_result.is_err() && self.path.exists() {
            let _ = fs::remove_file(&next);
        }
        write_result?;
        self.jsonl.recover(true, replay)?;
        Ok(())
    }

    pub fn len(&self) -> Result<u64, OpaqueJournalError> {
        match fs::metadata(&self.path) {
            Ok(metadata) if metadata.is_file() => Ok(metadata.len()),
            Ok(_) => Err(OpaqueJournalError::Corrupt),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(source) => Err(io_error(&self.path, source).into()),
        }
    }

    pub fn is_empty(&self) -> Result<bool, OpaqueJournalError> {
        self.len().map(|length| length == 0)
    }
}

fn encode_replacement(payloads: &[Vec<u8>]) -> Result<Vec<u8>, OpaqueJournalError> {
    let mut encoded = Vec::new();
    let mut previous_sha256 = FIRST_RECORD_HASH;
    for (offset, payload) in payloads.iter().enumerate() {
        let sequence = u64::try_from(offset)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or(OpaqueJournalError::SequenceExhausted)?;
        let stored = StoredRecord {
            schema_version: RECORD_SCHEMA_VERSION,
            sequence,
            previous_sha256,
            payload_sha256: Sha256::digest(payload).into(),
            payload: payload.clone(),
        };
        let line = serde_json::to_vec(&stored).map_err(OpaqueJournalError::Encode)?;
        previous_sha256 = stored_record_digest(&stored);
        encoded.extend_from_slice(&line);
        encoded.push(b'\n');
    }
    Ok(encoded)
}

fn recover_interrupted_compaction(path: &Path) -> Result<(), OpaqueJournalError> {
    let next = compaction_path(path, "next");
    let previous = compaction_path(path, "previous");
    if !path.exists() && previous.exists() {
        if next.exists() {
            fs::remove_file(&next).map_err(|source| io_error(&next, source))?;
        }
        fs::rename(&previous, path).map_err(|source| io_error(path, source))?;
        sync_parent(path)?;
    } else if !path.exists() && next.exists() {
        return Err(OpaqueJournalError::Corrupt);
    }
    Ok(())
}

fn discard_completed_compaction_files(path: &Path) -> Result<(), OpaqueJournalError> {
    for candidate in [
        compaction_path(path, "next"),
        compaction_path(path, "previous"),
    ] {
        match fs::remove_file(&candidate) {
            Ok(()) => sync_parent(path)?,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => return Err(io_error(&candidate, source).into()),
        }
    }
    Ok(())
}

fn compaction_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(format!(".compact.{suffix}"));
    PathBuf::from(value)
}

fn operation_guard(path: &Path) -> Result<File, OpaqueJournalError> {
    let lock_path = compaction_path(path, "lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|source| io_error(&lock_path, source))?;
    file.try_lock_exclusive()
        .map_err(|source| io_error(&lock_path, source))?;
    Ok(file)
}

fn io_error(path: &Path, source: std::io::Error) -> StorageError {
    StorageError::Io {
        path: path.to_path_buf(),
        source,
    }
}

fn replay(snapshot: &JsonlSnapshot) -> Result<Replay, OpaqueJournalError> {
    let mut records = Vec::new();
    let mut next_sequence = 1_u64;
    let mut tail_sha256 = FIRST_RECORD_HASH;
    for line in snapshot.lines() {
        if line.is_empty() {
            return Err(OpaqueJournalError::Corrupt);
        }
        let stored: StoredRecord =
            serde_json::from_slice(line).map_err(OpaqueJournalError::Decode)?;
        if stored.schema_version != RECORD_SCHEMA_VERSION
            || stored.sequence != next_sequence
            || stored.previous_sha256 != tail_sha256
            || stored.payload.len() > MAX_PAYLOAD_BYTES
            || stored.payload_sha256 != Sha256::digest(&stored.payload).as_slice()
        {
            return Err(OpaqueJournalError::Corrupt);
        }
        tail_sha256 = stored_record_digest(&stored);
        records.push(OpaqueJournalRecord {
            sequence: stored.sequence,
            payload: stored.payload,
        });
        next_sequence = next_sequence
            .checked_add(1)
            .ok_or(OpaqueJournalError::SequenceExhausted)?;
    }
    Ok(Replay {
        records,
        next_sequence,
        tail_sha256,
    })
}

fn stored_record_digest(record: &StoredRecord) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(record.schema_version.to_le_bytes());
    digest.update(record.sequence.to_le_bytes());
    digest.update(record.previous_sha256);
    digest.update(record.payload_sha256);
    digest.update(&record.payload);
    digest.finalize().into()
}

#[derive(Debug, thiserror::Error)]
pub enum OpaqueJournalError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("opaque journal encoding failed: {0}")]
    Encode(serde_json::Error),
    #[error("opaque journal decoding failed: {0}")]
    Decode(serde_json::Error),
    #[error("opaque journal is corrupt")]
    Corrupt,
    #[error("opaque journal expected sequence {expected}, but durable next sequence is {actual}")]
    SequenceConflict { expected: u64, actual: u64 },
    #[error("opaque journal sequence space is exhausted")]
    SequenceExhausted,
    #[error("opaque journal payload exceeds the 2 MiB bound")]
    RecordTooLarge,
    #[error("opaque journal append or replacement exceeds its physical file bound")]
    FileLimitExceeded,
}

#[cfg(test)]
#[path = "control_delivery_tests.rs"]
mod tests;
