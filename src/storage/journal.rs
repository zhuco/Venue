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
        let mut recovery = recover_file(&path)?;
        if recovery.truncated_tail {
            truncate_incomplete_tail(&path)?;
            recovery = recover_file(&path)?;
            if recovery.truncated_tail {
                return Err(StorageError::TailRepair);
            }
        }
        let next_sequence = match recovery.entries.last() {
            Some(entry) => entry
                .sequence
                .checked_add(1)
                .ok_or(StorageError::SequenceExhausted)?,
            None => 1,
        };
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
        let sequence = self.next_sequence;
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
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(StorageError::SequenceExhausted)?;
        Ok(sequence)
    }

    pub fn recover(&self) -> Result<JournalRecovery, StorageError> {
        recover_file(&self.path)
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
    for line in complete
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let entry: JournalEntry = serde_json::from_slice(line).map_err(StorageError::Decode)?;
        entries.push(entry);
    }
    for pair in entries.windows(2) {
        if pair[1].sequence != pair[0].sequence + 1 {
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
