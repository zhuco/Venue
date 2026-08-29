use std::{
    collections::BTreeSet,
    fmt::Debug,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

/// Narrow strategy-owned view required to validate a three-owner evidence bundle.
pub trait EvidenceBundle: Clone + Debug + Eq + Serialize + DeserializeOwned {
    fn evidence_identities(&self) -> [(u16, &str); 3];
}

/// Durable, append-only audit input for a Shadow evidence replay. It contains only anonymous
/// calibration, cost, and risk projections; physical trading facts remain in the main journal.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(bound(deserialize = "B: DeserializeOwned"))]
pub struct ScalpingEvidenceRecord<B> {
    pub sequence: u64,
    pub content_sha256: String,
    pub bundle: B,
}

#[derive(Debug)]
pub struct ScalpingEvidenceJournal<B> {
    path: PathBuf,
    next_sequence: u64,
    records: Vec<ScalpingEvidenceRecord<B>>,
}

impl<B: EvidenceBundle> ScalpingEvidenceJournal<B> {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, ScalpingEvidenceError> {
        let path = path.into();
        let records = recover(&path)?;
        let next_sequence = records
            .last()
            .map(|record| {
                record
                    .sequence
                    .checked_add(1)
                    .ok_or(ScalpingEvidenceError::Sequence)
            })
            .transpose()?
            .unwrap_or(1);
        Ok(Self {
            path,
            next_sequence,
            records,
        })
    }

    pub fn append(&mut self, bundle: B) -> Result<u64, ScalpingEvidenceError> {
        let content_sha256 = digest_bundle(&bundle)?;
        if let Some(sequence) = classify_bundle(&self.records, &bundle, &content_sha256)? {
            return Ok(sequence);
        }
        let record = ScalpingEvidenceRecord {
            sequence: self.next_sequence,
            content_sha256,
            bundle,
        };
        let encoded = serde_json::to_vec(&record).map_err(ScalpingEvidenceError::Encode)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|source| ScalpingEvidenceError::Io {
                path: self.path.clone(),
                source,
            })?;
        file.write_all(&encoded)
            .and_then(|()| file.write_all(b"\n"))
            .and_then(|()| file.sync_data())
            .map_err(|source| ScalpingEvidenceError::Io {
                path: self.path.clone(),
                source,
            })?;
        self.records.push(record.clone());
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(ScalpingEvidenceError::Sequence)?;
        Ok(record.sequence)
    }

    pub fn recover(&self) -> Result<Vec<ScalpingEvidenceRecord<B>>, ScalpingEvidenceError> {
        recover(&self.path)
    }
}

fn recover<B: EvidenceBundle>(
    path: &Path,
) -> Result<Vec<ScalpingEvidenceRecord<B>>, ScalpingEvidenceError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(ScalpingEvidenceError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        return Err(ScalpingEvidenceError::Truncated);
    }
    let mut records = Vec::new();
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let record: ScalpingEvidenceRecord<B> =
            serde_json::from_slice(line).map_err(ScalpingEvidenceError::Decode)?;
        let expected = u64::try_from(records.len())
            .map_err(|_| ScalpingEvidenceError::Sequence)?
            .checked_add(1)
            .ok_or(ScalpingEvidenceError::Sequence)?;
        if record.sequence != expected {
            return Err(ScalpingEvidenceError::Sequence);
        }
        if record.content_sha256 != digest_bundle(&record.bundle)? {
            return Err(ScalpingEvidenceError::Hash);
        }
        classify_bundle(&records, &record.bundle, &record.content_sha256)?;
        records.push(record);
    }
    Ok(records)
}

/// Returns the original sequence for an exact durable retry. Any partial identity reuse must
/// remain distinguishable from an idempotent retry, so a caller cannot replace one projection
/// under another component's evidence id.
fn classify_bundle<B: EvidenceBundle>(
    records: &[ScalpingEvidenceRecord<B>],
    bundle: &B,
    content_sha256: &str,
) -> Result<Option<u64>, ScalpingEvidenceError> {
    let ids = bundle_ids(bundle)?;
    let mut exact_sequence = None;
    for record in records {
        if record.bundle == *bundle && record.content_sha256 == content_sha256 {
            exact_sequence.get_or_insert(record.sequence);
            continue;
        }
        let existing_ids = bundle_ids(&record.bundle)?;
        if !ids.is_disjoint(&existing_ids) {
            return Err(ScalpingEvidenceError::Conflicting);
        }
    }
    Ok(exact_sequence)
}

fn bundle_ids<B: EvidenceBundle>(bundle: &B) -> Result<BTreeSet<&str>, ScalpingEvidenceError> {
    let mut evidence_ids = BTreeSet::new();
    for (schema_version, evidence_id) in bundle.evidence_identities() {
        if schema_version == 0 || evidence_id.trim().is_empty() {
            return Err(ScalpingEvidenceError::Identity);
        }
        if !evidence_ids.insert(evidence_id) {
            return Err(ScalpingEvidenceError::Duplicate);
        }
    }
    Ok(evidence_ids)
}

fn digest_bundle<B: Serialize>(bundle: &B) -> Result<String, ScalpingEvidenceError> {
    let bytes = serde_json::to_vec(bundle).map_err(ScalpingEvidenceError::Encode)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingEvidenceError {
    #[error("scalping evidence identity is invalid")]
    Identity,
    #[error("scalping evidence identity is duplicated")]
    Duplicate,
    #[error("scalping evidence identity is reused by conflicting bundle content")]
    Conflicting,
    #[error("scalping evidence journal has a truncated tail")]
    Truncated,
    #[error("scalping evidence journal sequence is invalid or exhausted")]
    Sequence,
    #[error("scalping evidence record content hash does not match")]
    Hash,
    #[error("scalping evidence I/O failed for {path}: {source}", path = path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("scalping evidence encoding failed: {0}")]
    Encode(serde_json::Error),
    #[error("scalping evidence decoding failed: {0}")]
    Decode(serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    struct Bundle {
        identities: [(u16, String); 3],
        content: String,
    }

    impl EvidenceBundle for Bundle {
        fn evidence_identities(&self) -> [(u16, &str); 3] {
            [
                (self.identities[0].0, &self.identities[0].1),
                (self.identities[1].0, &self.identities[1].1),
                (self.identities[2].0, &self.identities[2].1),
            ]
        }
    }

    fn bundle() -> Bundle {
        Bundle {
            identities: [
                (1, "calibration-1".to_owned()),
                (1, "cost-1".to_owned()),
                (1, "risk-1".to_owned()),
            ],
            content: "projection".to_owned(),
        }
    }

    #[test]
    fn exact_retry_is_idempotent_and_partial_identity_reuse_fails_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("scalping-evidence.jsonl");
        let mut journal = ScalpingEvidenceJournal::open(&path)?;
        let original = bundle();
        assert_eq!(journal.append(original.clone())?, 1);
        assert_eq!(journal.append(original)?, 1);
        assert_eq!(journal.recover()?.len(), 1);

        let mut conflict = bundle();
        conflict.identities[0].1 = "calibration-2".to_owned();
        conflict.content = "different".to_owned();
        assert!(matches!(
            journal.append(conflict),
            Err(ScalpingEvidenceError::Conflicting)
        ));
        Ok(())
    }

    #[test]
    fn reopen_validates_schema_sequence_hash_and_truncated_tail()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("scalping-evidence.jsonl");
        let mut journal = ScalpingEvidenceJournal::open(&path)?;
        journal.append(bundle())?;
        assert_eq!(
            ScalpingEvidenceJournal::<Bundle>::open(&path)?
                .recover()?
                .len(),
            1
        );

        let truncated = directory.path().join("truncated.jsonl");
        fs::write(&truncated, b"{")?;
        assert!(matches!(
            ScalpingEvidenceJournal::<Bundle>::open(truncated),
            Err(ScalpingEvidenceError::Truncated)
        ));

        let mut invalid = bundle();
        invalid.identities[0].0 = 0;
        let invalid_path = directory.path().join("invalid.jsonl");
        let record = ScalpingEvidenceRecord {
            sequence: 1,
            content_sha256: digest_bundle(&invalid)?,
            bundle: invalid,
        };
        let mut bytes = serde_json::to_vec(&record)?;
        bytes.push(b'\n');
        fs::write(&invalid_path, bytes)?;
        assert!(matches!(
            ScalpingEvidenceJournal::<Bundle>::open(invalid_path),
            Err(ScalpingEvidenceError::Identity)
        ));
        Ok(())
    }
}
