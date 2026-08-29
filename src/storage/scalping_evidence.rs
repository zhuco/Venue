use std::{
    collections::BTreeSet,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::strategy::scalping::CandidateEvidenceBundle;

/// Durable, append-only audit input for a Shadow evidence replay. It contains only anonymous
/// calibration, cost, and risk projections; physical trading facts remain in the main journal.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScalpingEvidenceRecord {
    pub sequence: u64,
    pub content_sha256: String,
    pub bundle: CandidateEvidenceBundle,
}

#[derive(Debug)]
pub struct ScalpingEvidenceJournal {
    path: PathBuf,
    next_sequence: u64,
    records: Vec<ScalpingEvidenceRecord>,
}

impl ScalpingEvidenceJournal {
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

    pub fn append(
        &mut self,
        bundle: CandidateEvidenceBundle,
    ) -> Result<u64, ScalpingEvidenceError> {
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

    pub fn recover(&self) -> Result<Vec<ScalpingEvidenceRecord>, ScalpingEvidenceError> {
        recover(&self.path)
    }
}

fn recover(path: &Path) -> Result<Vec<ScalpingEvidenceRecord>, ScalpingEvidenceError> {
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
        let record: ScalpingEvidenceRecord =
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
fn classify_bundle(
    records: &[ScalpingEvidenceRecord],
    bundle: &CandidateEvidenceBundle,
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

fn bundle_ids(bundle: &CandidateEvidenceBundle) -> Result<BTreeSet<&str>, ScalpingEvidenceError> {
    let mut evidence_ids = BTreeSet::new();
    for identity in [
        &bundle.calibration.identity,
        &bundle.costs.identity,
        &bundle.risk.identity,
    ] {
        if identity.schema_version == 0 || identity.evidence_id.trim().is_empty() {
            return Err(ScalpingEvidenceError::Identity);
        }
        if !evidence_ids.insert(identity.evidence_id.as_str()) {
            return Err(ScalpingEvidenceError::Duplicate);
        }
    }
    Ok(evidence_ids)
}

fn digest_bundle(bundle: &CandidateEvidenceBundle) -> Result<String, ScalpingEvidenceError> {
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
    use rust_decimal::Decimal;
    use tempfile::tempdir;

    use crate::strategy::scalping::{
        CalibrationEvidence, CostEvidence, EvidenceIdentity, RiskEvidence,
    };

    use super::*;

    fn identity(kind: &str) -> EvidenceIdentity {
        EvidenceIdentity {
            schema_version: 1,
            evidence_id: format!("{kind}-1"),
            candidate_id: "candidate-1".to_owned(),
            preparation_id: "preparation-1".to_owned(),
            binding_digest: "a".repeat(64),
            frame_generation: 1,
            watermark_ms: 100,
            producer_generation: 1,
            release_digest: "b".repeat(64),
            valid_until_ms: 200,
        }
    }

    fn bundle() -> CandidateEvidenceBundle {
        CandidateEvidenceBundle {
            calibration: CalibrationEvidence {
                identity: identity("calibration"),
                model_version: "scalping-shadow-calibration-v1".to_owned(),
                fill_distribution: vec![crate::strategy::scalping::FillSlice {
                    fill_ratio: Decimal::ONE,
                    probability: Decimal::ONE,
                }],
                outcomes: crate::strategy::scalping::OutcomeProbabilities {
                    target: Decimal::ONE,
                    stop: Decimal::ZERO,
                    other: Decimal::ZERO,
                },
                target_pnl_bps: Decimal::ONE,
                stop_pnl_bps: -Decimal::ONE,
                other_pnl_bps: Decimal::ZERO,
                uncertainty_bps: Decimal::ZERO,
            },
            costs: CostEvidence {
                identity: identity("cost"),
                entry_cost_bps: Decimal::ZERO,
                exit_cost_bps: Decimal::ZERO,
                funding_cost_bps: Decimal::ZERO,
                nonfill_cost_bps: Decimal::ZERO,
                opportunity_cost_bps: Decimal::ZERO,
            },
            risk: RiskEvidence {
                identity: identity("risk"),
                policy_digest: "c".repeat(64),
                worst_loss: crate::strategy::scalping::RiskLimit::new(
                    crate::strategy::scalping::RiskUnit::shadow(),
                    Decimal::ONE,
                ),
                admissible: true,
            },
        }
    }

    #[test]
    fn exact_append_retry_returns_original_sequence_without_new_record()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("scalping-evidence.jsonl");
        let item = bundle();
        let mut journal = ScalpingEvidenceJournal::open(&path)?;
        assert_eq!(journal.append(item.clone())?, 1);
        assert_eq!(journal.recover()?[0].bundle, item);
        assert_eq!(journal.append(bundle())?, 1);
        assert_eq!(journal.recover()?.len(), 1);
        Ok(())
    }

    #[test]
    fn partial_identity_reuse_is_conflicting_during_append_and_recovery()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("scalping-evidence.jsonl");
        let first = bundle();
        let mut conflict = bundle();
        conflict.calibration.identity.evidence_id = "calibration-2".to_owned();
        conflict.risk.identity.evidence_id = "risk-2".to_owned();
        conflict.costs.entry_cost_bps = Decimal::ONE;
        let mut journal = ScalpingEvidenceJournal::open(&path)?;
        assert_eq!(journal.append(first)?, 1);
        assert!(matches!(
            journal.append(conflict.clone()),
            Err(ScalpingEvidenceError::Conflicting)
        ));

        let record = ScalpingEvidenceRecord {
            sequence: 2,
            content_sha256: digest_bundle(&conflict)?,
            bundle: conflict,
        };
        let mut bytes = std::fs::read(&path)?;
        bytes.extend(serde_json::to_vec(&record)?);
        bytes.push(b'\n');
        std::fs::write(&path, bytes)?;
        assert!(matches!(
            ScalpingEvidenceJournal::open(&path),
            Err(ScalpingEvidenceError::Conflicting)
        ));
        Ok(())
    }

    #[test]
    fn reopen_retries_the_durable_record_at_its_original_sequence()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("scalping-evidence.jsonl");
        let item = bundle();
        {
            let mut journal = ScalpingEvidenceJournal::open(&path)?;
            assert_eq!(journal.append(item.clone())?, 1);
        }
        let mut reopened = ScalpingEvidenceJournal::open(&path)?;
        assert_eq!(reopened.append(item)?, 1);
        assert_eq!(reopened.recover()?.len(), 1);
        Ok(())
    }

    #[test]
    fn recovery_keeps_truncated_and_sequence_rejections() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempdir()?;
        let truncated = directory.path().join("truncated.jsonl");
        std::fs::write(&truncated, b"{")?;
        assert!(matches!(
            ScalpingEvidenceJournal::open(&truncated),
            Err(ScalpingEvidenceError::Truncated)
        ));

        let sequence = directory.path().join("sequence.jsonl");
        let item = bundle();
        let record = ScalpingEvidenceRecord {
            sequence: 2,
            content_sha256: digest_bundle(&item)?,
            bundle: item,
        };
        let mut bytes = serde_json::to_vec(&record)?;
        bytes.push(b'\n');
        std::fs::write(&sequence, bytes)?;
        assert!(matches!(
            ScalpingEvidenceJournal::open(&sequence),
            Err(ScalpingEvidenceError::Sequence)
        ));
        Ok(())
    }
}
