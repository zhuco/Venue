use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use venue_domain::FactRecord;

use crate::checkpoint::sync_parent;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct JournalEntry {
    pub sequence: u64,
    pub record: FactRecord,
}

#[derive(Debug)]
pub struct Journal {
    jsonl: DurableJsonl,
    next_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalRecovery {
    pub entries: Vec<JournalEntry>,
    pub truncated_tail: bool,
}

impl Journal {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let jsonl = DurableJsonl::new(path);
        let recovery = jsonl.recover(true, recover_snapshot)?;
        let next_sequence = next_sequence(&recovery.entries)?;
        Ok(Self {
            jsonl,
            next_sequence,
        })
    }

    pub fn append(&mut self, record: FactRecord) -> Result<u64, StorageError> {
        record
            .header
            .validate()
            .map_err(StorageError::InvalidRecord)?;
        let sequence = self.next_sequence;
        let following_sequence = sequence
            .checked_add(1)
            .ok_or(StorageError::SequenceExhausted)?;
        let entry = JournalEntry { sequence, record };
        let encoded = serde_json::to_vec(&entry).map_err(StorageError::Encode)?;
        self.jsonl.append(|snapshot| {
            let recovery = recover_snapshot(snapshot)?;
            if next_sequence(&recovery.entries)? != self.next_sequence {
                return Err(StorageError::Sequence);
            }
            Ok(((), encoded))
        })?;
        self.next_sequence = following_sequence;
        Ok(sequence)
    }

    pub fn recover(&self) -> Result<JournalRecovery, StorageError> {
        self.jsonl.recover(false, recover_snapshot)
    }
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

fn recover_snapshot(snapshot: &JsonlSnapshot) -> Result<JournalRecovery, StorageError> {
    let mut entries = Vec::new();
    for line in snapshot.lines() {
        let entry: JournalEntry = serde_json::from_slice(line).map_err(StorageError::Decode)?;
        entries.push(entry);
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
        truncated_tail: snapshot.truncated_tail(),
    })
}

/// The only file-format-independent JSONL durability boundary in `venue-storage`.
#[derive(Debug)]
pub(crate) struct DurableJsonl {
    path: PathBuf,
}

impl DurableJsonl {
    pub(crate) fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub(crate) fn recover<T, E>(
        &self,
        repair: bool,
        inspect: impl FnOnce(&JsonlSnapshot) -> Result<T, E>,
    ) -> Result<T, E>
    where
        E: From<StorageError>,
    {
        let Some(mut file) = self.open_existing().map_err(E::from)? else {
            return inspect(&JsonlSnapshot::empty());
        };
        lock(&file, &self.path).map_err(E::from)?;
        let snapshot = read_complete_lines(&mut file, &self.path).map_err(E::from)?;
        let value = inspect(&snapshot)?;
        if repair {
            repair_tail(&file, &self.path, &snapshot).map_err(E::from)?;
        }
        Ok(value)
    }

    pub(crate) fn append<T, E>(
        &self,
        make_line: impl FnOnce(&JsonlSnapshot) -> Result<(T, Vec<u8>), E>,
    ) -> Result<T, E>
    where
        E: From<StorageError>,
    {
        require_parent(&self.path).map_err(E::from)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&self.path)
            .map_err(|source| {
                E::from(StorageError::Io {
                    path: self.path.clone(),
                    source,
                })
            })?;
        lock(&file, &self.path).map_err(E::from)?;
        let snapshot = read_complete_lines(&mut file, &self.path).map_err(E::from)?;
        let (value, encoded) = make_line(&snapshot)?;
        repair_tail(&file, &self.path, &snapshot).map_err(E::from)?;
        file.seek(SeekFrom::End(0))
            .and_then(|_| file.write_all(&encoded))
            .and_then(|()| file.write_all(b"\n"))
            .and_then(|()| file.sync_data())
            .map_err(|source| {
                E::from(StorageError::Io {
                    path: self.path.clone(),
                    source,
                })
            })?;
        sync_parent(&self.path).map_err(E::from)?;
        Ok(value)
    }

    fn open_existing(&self) -> Result<Option<File>, StorageError> {
        match OpenOptions::new().read(true).write(true).open(&self.path) {
            Ok(file) => Ok(Some(file)),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(StorageError::Io {
                path: self.path.clone(),
                source,
            }),
        }
    }
}

fn require_parent(path: &Path) -> Result<&Path, StorageError> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| StorageError::Io {
            path: path.to_path_buf(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "journal path has no parent directory",
            ),
        })
}

#[derive(Debug)]
pub(crate) struct JsonlSnapshot {
    lines: Vec<Vec<u8>>,
    truncated_tail: bool,
    complete_length: u64,
}

impl JsonlSnapshot {
    fn empty() -> Self {
        Self {
            lines: Vec::new(),
            truncated_tail: false,
            complete_length: 0,
        }
    }

    pub(crate) fn lines(&self) -> impl Iterator<Item = &[u8]> {
        self.lines.iter().map(Vec::as_slice)
    }

    pub(crate) const fn truncated_tail(&self) -> bool {
        self.truncated_tail
    }
}

fn lock(file: &File, path: &Path) -> Result<(), StorageError> {
    file.try_lock().map_err(|source| StorageError::Io {
        path: path.to_path_buf(),
        source: source.into(),
    })
}

fn read_complete_lines(file: &mut File, path: &Path) -> Result<JsonlSnapshot, StorageError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    let complete_length = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1);
    let truncated_tail = complete_length != bytes.len();
    let complete = &bytes[..complete_length];
    let lines = if complete.is_empty() {
        Vec::new()
    } else {
        complete
            .strip_suffix(b"\n")
            .ok_or(StorageError::TailRepair)?
            .split(|byte| *byte == b'\n')
            .map(<[u8]>::to_vec)
            .collect()
    };
    Ok(JsonlSnapshot {
        lines,
        truncated_tail,
        complete_length: u64::try_from(complete_length).map_err(|_| StorageError::TailRepair)?,
    })
}

fn repair_tail(file: &File, path: &Path, snapshot: &JsonlSnapshot) -> Result<(), StorageError> {
    if !snapshot.truncated_tail {
        return Ok(());
    }
    file.set_len(snapshot.complete_length)
        .and_then(|()| file.sync_all())
        .map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
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
    InvalidRecord(venue_domain::EventIdError),
}

#[cfg(test)]
mod tests {
    use std::fs;

    use rust_decimal::Decimal;
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;
    use venue_domain::{Amount, Asset, DomainEvent, EventHeader, EventId, EventSource};

    use super::*;

    #[test]
    fn fact_journal_preserves_the_legacy_jsonl_wire_format()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("facts.jsonl");
        let record = fact("first", 1)?;
        let mut journal = Journal::open(&path)?;
        assert_eq!(journal.append(record.clone())?, 1);

        let mut expected = serde_json::to_vec(&JournalEntry {
            sequence: 1,
            record,
        })?;
        expected.push(b'\n');
        assert_eq!(fs::read(path)?, expected);
        Ok(())
    }

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
    fn append_fails_closed_on_complete_corruption_or_empty_line()
    -> Result<(), Box<dyn std::error::Error>> {
        for suffix in [b"{not-json}\n".as_slice(), b"\n"] {
            let directory = tempdir()?;
            let path = directory.path().join("facts.jsonl");
            let mut journal = Journal::open(&path)?;
            journal.append(fact("first", 1)?)?;
            let mut file = OpenOptions::new().append(true).open(&path)?;
            file.write_all(suffix)?;
            file.sync_all()?;
            let corrupted = fs::read(&path)?;
            assert!(matches!(
                journal.append(fact("second", 2)?),
                Err(StorageError::Decode(_))
            ));
            assert_eq!(fs::read(path)?, corrupted);
        }
        Ok(())
    }

    #[test]
    fn corrupt_complete_prefix_is_not_changed_when_a_bad_tail_is_present()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("facts.jsonl");
        let mut journal = Journal::open(&path)?;
        journal.append(fact("first", 1)?)?;
        let mut file = OpenOptions::new().append(true).open(&path)?;
        file.write_all(b"{not-json}\npartial-tail")?;
        file.sync_all()?;
        drop(file);
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
