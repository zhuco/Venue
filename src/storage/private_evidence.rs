use std::{
    fmt,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PrivateEvidence {
    pub sequence: u64,
    pub generation: u64,
    pub received_at_ms: u64,
    pub payload_sha256: String,
    pub payload: String,
}

/// Proof that a raw private payload crossed the journal's durable append boundary.
///
/// The wrapped record is intentionally private and this type is not deserializable. Code outside
/// this module can only obtain a receipt after `sync_data` succeeds or after journal recovery has
/// validated the complete append-only file.
#[derive(Clone, Eq, PartialEq)]
pub struct PersistedPrivateEvidence {
    evidence: PrivateEvidence,
}

impl fmt::Debug for PersistedPrivateEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PersistedPrivateEvidence")
            .field("sequence", &self.sequence())
            .field("generation", &self.generation())
            .field("received_at_ms", &self.received_at_ms())
            .field("payload_sha256", &self.payload_sha256())
            .finish_non_exhaustive()
    }
}

impl PersistedPrivateEvidence {
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.evidence.sequence
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.evidence.generation
    }

    #[must_use]
    pub const fn received_at_ms(&self) -> u64 {
        self.evidence.received_at_ms
    }

    #[must_use]
    pub fn payload_sha256(&self) -> &str {
        &self.evidence.payload_sha256
    }

    #[must_use]
    pub fn payload(&self) -> &str {
        &self.evidence.payload
    }
}

impl PrivateEvidence {
    pub fn new(
        generation: u64,
        received_at_ms: u64,
        payload: String,
    ) -> Result<Self, PrivateEvidenceError> {
        if generation == 0 || payload.is_empty() {
            return Err(PrivateEvidenceError::Invalid);
        }
        Ok(Self {
            sequence: 0,
            generation,
            received_at_ms,
            payload_sha256: digest(&payload),
            payload,
        })
    }

    pub fn valid_hash(&self) -> bool {
        self.payload_sha256 == digest(&self.payload)
    }
}

/// Append-only private wire evidence. A partial tail is fatal because it can conceal a fill.
#[derive(Debug)]
pub struct PrivateEvidenceJournal {
    path: PathBuf,
    next_sequence: u64,
    last_generation: u64,
}

impl PrivateEvidenceJournal {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, PrivateEvidenceError> {
        let path = path.into();
        let records = recover(&path)?;
        let next_sequence = records
            .last()
            .map(|record| {
                record
                    .sequence
                    .checked_add(1)
                    .ok_or(PrivateEvidenceError::Sequence)
            })
            .transpose()?
            .unwrap_or(1);
        let last_generation = records
            .iter()
            .map(|record| record.generation)
            .max()
            .unwrap_or(0);
        Ok(Self {
            path,
            next_sequence,
            last_generation,
        })
    }

    /// Legacy sequence-only append API. New normalization paths should retain the durable receipt
    /// returned by [`Self::append_persisted`].
    pub fn append(&mut self, evidence: PrivateEvidence) -> Result<u64, PrivateEvidenceError> {
        Ok(self.append_persisted(evidence)?.sequence())
    }

    pub fn append_persisted(
        &mut self,
        mut evidence: PrivateEvidence,
    ) -> Result<PersistedPrivateEvidence, PrivateEvidenceError> {
        if evidence.generation == 0 || evidence.payload.is_empty() {
            return Err(PrivateEvidenceError::Invalid);
        }
        if !evidence.valid_hash() {
            return Err(PrivateEvidenceError::Hash);
        }
        evidence.sequence = self.next_sequence;
        let following_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(PrivateEvidenceError::Sequence)?;
        let encoded = serde_json::to_vec(&evidence).map_err(PrivateEvidenceError::Encode)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|source| PrivateEvidenceError::Io {
                path: self.path.clone(),
                source,
            })?;
        file.write_all(&encoded)
            .and_then(|()| file.write_all(b"\n"))
            .and_then(|()| file.sync_data())
            .map_err(|source| PrivateEvidenceError::Io {
                path: self.path.clone(),
                source,
            })?;
        self.next_sequence = following_sequence;
        self.last_generation = self.last_generation.max(evidence.generation);
        Ok(PersistedPrivateEvidence { evidence })
    }

    /// Legacy raw recovery API. Runtime normalization should use [`Self::recover_persisted`].
    pub fn recover(&self) -> Result<Vec<PrivateEvidence>, PrivateEvidenceError> {
        recover(&self.path)
    }

    pub fn recover_persisted(&self) -> Result<Vec<PersistedPrivateEvidence>, PrivateEvidenceError> {
        Ok(recover(&self.path)?
            .into_iter()
            .map(|evidence| PersistedPrivateEvidence { evidence })
            .collect())
    }

    pub const fn last_sequence(&self) -> u64 {
        self.next_sequence.saturating_sub(1)
    }

    pub const fn last_generation(&self) -> u64 {
        self.last_generation
    }
}

fn recover(path: &Path) -> Result<Vec<PrivateEvidence>, PrivateEvidenceError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(PrivateEvidenceError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        return Err(PrivateEvidenceError::Truncated);
    }
    let mut records = Vec::new();
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let evidence: PrivateEvidence =
            serde_json::from_slice(line).map_err(PrivateEvidenceError::Decode)?;
        if evidence.generation == 0 || evidence.payload.is_empty() {
            return Err(PrivateEvidenceError::Invalid);
        }
        if !evidence.valid_hash() {
            return Err(PrivateEvidenceError::Hash);
        }
        if evidence.sequence != records.len() as u64 + 1 {
            return Err(PrivateEvidenceError::Sequence);
        }
        records.push(evidence);
    }
    Ok(records)
}

fn digest(payload: &str) -> String {
    Sha256::digest(payload.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Debug, thiserror::Error)]
pub enum PrivateEvidenceError {
    #[error("private evidence is invalid")]
    Invalid,
    #[error("private evidence hash does not match payload")]
    Hash,
    #[error("private evidence journal has a truncated tail")]
    Truncated,
    #[error("private evidence sequence is invalid or exhausted")]
    Sequence,
    #[error("private evidence I/O failed for {path}: {source}", path = path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("private evidence encoding failed: {0}")]
    Encode(serde_json::Error),
    #[error("private evidence decoding failed: {0}")]
    Decode(serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_generation_is_cached_across_append_and_reopen() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("private.jsonl");
        let mut journal = PrivateEvidenceJournal::open(&path)?;
        assert_eq!(journal.last_generation(), 0);
        journal.append(PrivateEvidence::new(7, 10, "first".to_owned())?)?;
        journal.append(PrivateEvidence::new(9, 11, "second".to_owned())?)?;
        assert_eq!(journal.last_generation(), 9);
        assert_eq!(PrivateEvidenceJournal::open(path)?.last_generation(), 9);
        Ok(())
    }

    #[test]
    fn durable_receipts_are_returned_after_append_and_validated_recovery()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("private.jsonl");
        let mut journal = PrivateEvidenceJournal::open(&path)?;
        let appended =
            journal.append_persisted(PrivateEvidence::new(3, 21, "payload".to_owned())?)?;

        assert_eq!(appended.sequence(), 1);
        assert_eq!(appended.generation(), 3);
        assert_eq!(appended.received_at_ms(), 21);
        assert_eq!(appended.payload(), "payload");

        let recovered = PrivateEvidenceJournal::open(path)?.recover_persisted()?;
        assert_eq!(recovered, vec![appended]);
        Ok(())
    }

    #[test]
    fn invalid_public_record_cannot_receive_a_durable_receipt()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("private.jsonl");
        let mut journal = PrivateEvidenceJournal::open(&path)?;
        let mut invalid = PrivateEvidence::new(1, 21, "payload".to_owned())?;
        invalid.generation = 0;

        assert!(matches!(
            journal.append_persisted(invalid),
            Err(PrivateEvidenceError::Invalid)
        ));
        assert!(journal.recover()?.is_empty());
        Ok(())
    }
}
