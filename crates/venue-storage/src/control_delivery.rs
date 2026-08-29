use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    StorageError,
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
}

impl OpaqueJournal {
    pub fn open(path: impl Into<std::path::PathBuf>) -> Result<Self, OpaqueJournalError> {
        let journal = Self {
            jsonl: DurableJsonl::new(path),
        };
        journal.jsonl.recover(true, replay)?;
        Ok(journal)
    }

    pub fn recover(&mut self) -> Result<Vec<OpaqueJournalRecord>, OpaqueJournalError> {
        Ok(self.jsonl.recover(true, replay)?.records)
    }

    pub fn append(
        &mut self,
        expected_sequence: u64,
        payload: &[u8],
    ) -> Result<u64, OpaqueJournalError> {
        if payload.len() > MAX_PAYLOAD_BYTES {
            return Err(OpaqueJournalError::RecordTooLarge);
        }
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
            Ok((expected_sequence, encoded))
        })
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
}

#[cfg(test)]
#[path = "control_delivery_tests.rs"]
mod tests;
