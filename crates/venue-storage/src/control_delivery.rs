use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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
    path: PathBuf,
}

impl OpaqueJournal {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, OpaqueJournalError> {
        let path = path.into();
        let journal = Self { path };
        journal.with_locked_file(|file| replay_and_repair(file, &journal.path).map(|_| ()))?;
        Ok(journal)
    }

    pub fn recover(&mut self) -> Result<Vec<OpaqueJournalRecord>, OpaqueJournalError> {
        self.with_locked_file(|file| {
            replay_and_repair(file, &self.path).map(|replay| replay.records)
        })
    }

    pub fn append(
        &mut self,
        expected_sequence: u64,
        payload: &[u8],
    ) -> Result<u64, OpaqueJournalError> {
        if payload.len() > MAX_PAYLOAD_BYTES {
            return Err(OpaqueJournalError::RecordTooLarge);
        }
        self.with_locked_file(|file| {
            let replay = replay_and_repair(file, &self.path)?;
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
            file.seek(SeekFrom::End(0))
                .and_then(|_| file.write_all(&encoded))
                .and_then(|()| file.write_all(b"\n"))
                .and_then(|()| file.sync_data())
                .map_err(|source| OpaqueJournalError::Io {
                    path: self.path.clone(),
                    source,
                })?;
            Ok(expected_sequence)
        })
    }

    fn with_locked_file<T>(
        &self,
        operation: impl FnOnce(&mut File) -> Result<T, OpaqueJournalError>,
    ) -> Result<T, OpaqueJournalError> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&self.path)
            .map_err(|source| OpaqueJournalError::Io {
                path: self.path.clone(),
                source,
            })?;
        file.try_lock().map_err(|source| OpaqueJournalError::Lock {
            path: self.path.clone(),
            source: source.into(),
        })?;
        operation(&mut file)
    }
}

fn replay_and_repair(file: &mut File, path: &Path) -> Result<Replay, OpaqueJournalError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|source| OpaqueJournalError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|source| OpaqueJournalError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    let complete_length = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1);
    if complete_length != bytes.len() {
        let durable_length =
            u64::try_from(complete_length).map_err(|_| OpaqueJournalError::SequenceExhausted)?;
        file.set_len(durable_length)
            .and_then(|()| file.sync_data())
            .map_err(|source| OpaqueJournalError::Io {
                path: path.to_path_buf(),
                source,
            })?;
        bytes.truncate(complete_length);
    }
    replay_complete_lines(&bytes)
}

fn replay_complete_lines(bytes: &[u8]) -> Result<Replay, OpaqueJournalError> {
    let mut records = Vec::new();
    let mut next_sequence = 1_u64;
    let mut tail_sha256 = FIRST_RECORD_HASH;
    let complete = bytes.strip_suffix(b"\n").unwrap_or(bytes);
    if complete.is_empty() {
        return Ok(Replay {
            records,
            next_sequence,
            tail_sha256,
        });
    }
    for line in complete.split(|byte| *byte == b'\n') {
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
    #[error("opaque journal I/O failed for {path}: {source}", path = path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("opaque journal lock is held for {path}: {source}", path = path.display())]
    Lock {
        path: PathBuf,
        source: std::io::Error,
    },
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
}

#[cfg(test)]
#[path = "control_delivery_tests.rs"]
mod tests;
