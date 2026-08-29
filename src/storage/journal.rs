use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::domain::FactRecord;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct JournalEntry {
    pub sequence: u64,
    pub record: FactRecord,
}

#[derive(Debug)]
pub struct Journal {
    path: PathBuf,
    next_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalRecovery {
    pub entries: Vec<JournalEntry>,
    pub truncated_tail: bool,
}

impl Journal {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let path = path.into();
        let recovery = recover_repaired_file(&path)?;
        let next_sequence = next_sequence(&recovery.entries)?;
        Ok(Self {
            path,
            next_sequence,
        })
    }

    pub fn append(&mut self, record: FactRecord) -> Result<u64, StorageError> {
        record
            .header
            .validate()
            .map_err(StorageError::InvalidRecord)?;
        let recovery = recover_repaired_file(&self.path)?;
        if next_sequence(&recovery.entries)? != self.next_sequence {
            return Err(StorageError::Sequence);
        }
        let sequence = self.next_sequence;
        let next_sequence = sequence
            .checked_add(1)
            .ok_or(StorageError::SequenceExhausted)?;
        let entry = JournalEntry { sequence, record };
        let encoded = serde_json::to_vec(&entry).map_err(StorageError::Encode)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|source| StorageError::Io {
                path: self.path.clone(),
                source,
            })?;
        file.write_all(&encoded)
            .map_err(|source| StorageError::Io {
                path: self.path.clone(),
                source,
            })?;
        file.write_all(b"\n").map_err(|source| StorageError::Io {
            path: self.path.clone(),
            source,
        })?;
        file.sync_data().map_err(|source| StorageError::Io {
            path: self.path.clone(),
            source,
        })?;
        self.next_sequence = next_sequence;
        Ok(sequence)
    }

    pub fn recover(&self) -> Result<JournalRecovery, StorageError> {
        recover_file(&self.path)
    }
}

fn recover_repaired_file(path: &Path) -> Result<JournalRecovery, StorageError> {
    let recovery = recover_file(path)?;
    if !recovery.truncated_tail {
        return Ok(recovery);
    }
    truncate_incomplete_tail(path)?;
    let repaired = recover_file(path)?;
    if repaired.truncated_tail {
        return Err(StorageError::TailRepair);
    }
    Ok(repaired)
}

fn next_sequence(entries: &[JournalEntry]) -> Result<u64, StorageError> {
    match entries.last() {
        Some(entry) => entry
            .sequence
            .checked_add(1)
            .ok_or(StorageError::SequenceExhausted),
        None => Ok(1),
    }
}

fn truncate_incomplete_tail(path: &Path) -> Result<(), StorageError> {
    let bytes = fs::read(path).map_err(|source| StorageError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let complete_length = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1);
    if complete_length == bytes.len() {
        return Ok(());
    }
    let complete_length = u64::try_from(complete_length).map_err(|_| StorageError::TailRepair)?;
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    file.set_len(complete_length)
        .and_then(|()| file.sync_all())
        .map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
        })
}

fn recover_file(path: &Path) -> Result<JournalRecovery, StorageError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(JournalRecovery {
                entries: Vec::new(),
                truncated_tail: false,
            });
        }
        Err(source) => {
            return Err(StorageError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    let complete_length = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1);
    let truncated_tail = complete_length != bytes.len();
    let complete = &bytes[..complete_length];
    let mut entries = Vec::new();
    if let Some(without_final_newline) = complete.strip_suffix(b"\n") {
        for line in without_final_newline.split(|byte| *byte == b'\n') {
            let entry: JournalEntry = serde_json::from_slice(line).map_err(StorageError::Decode)?;
            entries.push(entry);
        }
    }
    if entries.first().is_some_and(|entry| entry.sequence != 1) {
        return Err(StorageError::Sequence);
    }
    for pair in entries.windows(2) {
        if pair[0].sequence.checked_add(1) != Some(pair[1].sequence) {
            return Err(StorageError::Sequence);
        }
    }
    Ok(JournalRecovery {
        entries,
        truncated_tail,
    })
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("storage I/O failed for {path}: {source}", path = path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("storage encoding failed: {0}")]
    Encode(serde_json::Error),
    #[error("storage decoding failed: {0}")]
    Decode(serde_json::Error),
    #[error("journal sequence is not contiguous")]
    Sequence,
    #[error("journal sequence space is exhausted")]
    SequenceExhausted,
    #[error("journal incomplete tail could not be repaired")]
    TailRepair,
    #[error("record is invalid: {0}")]
    InvalidRecord(crate::domain::EventIdError),
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    use crate::domain::{Amount, Asset, DomainEvent, EventHeader, EventId, EventSource};

    use super::*;

    #[test]
    fn append_repairs_crash_tail_before_extending_the_sequence_chain()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("facts.jsonl");
        let first = fact("first", 1)?;
        let second = fact("second", 2)?;
        let third = fact("third", 3)?;
        let mut journal = Journal::open(&path)?;
        assert_eq!(journal.append(first.clone())?, 1);
        let durable_prefix = fs::read(&path)?;
        let durable_prefix_hash = Sha256::digest(&durable_prefix);

        let crashed_entry = JournalEntry {
            sequence: 2,
            record: fact("crashed", 2)?,
        };
        let crashed_bytes = serde_json::to_vec(&crashed_entry)?;
        let mut file = OpenOptions::new().append(true).open(&path)?;
        file.write_all(&crashed_bytes[..crashed_bytes.len() / 2])?;
        file.sync_all()?;

        assert_eq!(journal.append(second.clone())?, 2);
        let repaired_bytes = fs::read(&path)?;
        assert_eq!(
            Sha256::digest(&repaired_bytes[..durable_prefix.len()]),
            durable_prefix_hash
        );
        assert!(repaired_bytes.ends_with(b"\n"));

        let mut restarted = Journal::open(&path)?;
        assert_eq!(restarted.append(third.clone())?, 3);
        let recovery = restarted.recover()?;
        assert!(!recovery.truncated_tail);
        assert_eq!(
            recovery
                .entries
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(
            recovery
                .entries
                .into_iter()
                .map(|entry| entry.record)
                .collect::<Vec<_>>(),
            vec![first, second, third]
        );
        Ok(())
    }

    #[test]
    fn append_fails_closed_on_a_complete_corrupt_record() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempdir()?;
        let path = directory.path().join("facts.jsonl");
        let mut journal = Journal::open(&path)?;
        journal.append(fact("first", 1)?)?;
        let mut file = OpenOptions::new().append(true).open(&path)?;
        file.write_all(b"{not-json}\n")?;
        file.sync_all()?;
        let corrupted = fs::read(&path)?;

        assert!(matches!(
            journal.append(fact("second", 2)?),
            Err(StorageError::Decode(_))
        ));
        assert_eq!(fs::read(path)?, corrupted);
        Ok(())
    }

    #[test]
    fn append_fails_closed_on_a_complete_sequence_fork() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("facts.jsonl");
        let mut journal = Journal::open(&path)?;
        journal.append(fact("first", 1)?)?;
        let fork = JournalEntry {
            sequence: 3,
            record: fact("fork", 3)?,
        };
        let mut file = OpenOptions::new().append(true).open(&path)?;
        file.write_all(&serde_json::to_vec(&fork)?)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        let forked = fs::read(&path)?;

        assert!(matches!(
            journal.append(fact("second", 2)?),
            Err(StorageError::Sequence)
        ));
        assert_eq!(fs::read(path)?, forked);
        Ok(())
    }

    fn fact(event_id: &str, generation: u64) -> Result<FactRecord, Box<dyn std::error::Error>> {
        Ok(FactRecord {
            header: EventHeader {
                schema_version: 1,
                event_id: EventId::new(event_id)?,
                source: EventSource::Recovery,
                source_sequence: Some(generation),
                received_at_ms: generation,
                generation,
            },
            event: DomainEvent::Funding(Amount::new(
                Asset::new("USDT")?,
                Decimal::from(generation),
            )),
        })
    }
}
