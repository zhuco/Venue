use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{Capability, CapabilityEvidence};

const SCHEMA_VERSION: u16 = 1;

/// Non-secret deployment identity for a capability probe. The API-key fingerprint is a SHA-256
/// digest and is never rendered by the CLI or used as an authentication token.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CapabilityBinding {
    pub exchange: String,
    pub account_binding: String,
    pub symbol: String,
    pub api_key_sha256: String,
}

impl CapabilityBinding {
    pub fn validate(&self) -> Result<(), CapabilityEvidenceError> {
        let supported_account_binding = matches!(
            (self.exchange.as_str(), self.account_binding.as_str()),
            ("binance", "portfolio_margin_um")
                | ("gate", "usdt_futures_dual")
                | ("bitget", "uta_usdt_futures_hedge")
        );
        if !supported_account_binding
            || self.symbol.is_empty()
            || !valid_sha256(&self.api_key_sha256)
        {
            return Err(CapabilityEvidenceError::Invalid);
        }
        Ok(())
    }
}

/// A probe output is intentionally digest-only: raw responses remain in their owning journals,
/// while this journal only establishes that the named capability was recently verified.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityProbe {
    pub capability: Capability,
    pub probe: String,
    pub evidence_hash: String,
    pub valid_until_ms: u64,
}

impl CapabilityProbe {
    pub fn new(
        capability: Capability,
        probe: impl Into<String>,
        evidence_hash: String,
        valid_until_ms: u64,
    ) -> Result<Self, CapabilityEvidenceError> {
        let probe = probe.into();
        if probe.is_empty() || !valid_sha256(&evidence_hash) || valid_until_ms == 0 {
            return Err(CapabilityEvidenceError::Invalid);
        }
        Ok(Self {
            capability,
            probe,
            evidence_hash,
            valid_until_ms,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct CapabilityEvidenceRecord {
    schema_version: u16,
    sequence: u64,
    binding: CapabilityBinding,
    capability: Capability,
    probe: String,
    evidence_hash: String,
    verified_at_ms: u64,
    valid_until_ms: u64,
    success: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct CapabilityEvidenceBatchRecord {
    schema_version: u16,
    kind: String,
    records: Vec<CapabilityEvidenceRecord>,
    batch_sha256: String,
}

impl CapabilityEvidenceBatchRecord {
    fn new(records: Vec<CapabilityEvidenceRecord>) -> Result<Self, CapabilityEvidenceError> {
        let batch_sha256 =
            sha256_hex(serde_json::to_vec(&records).map_err(CapabilityEvidenceError::Encode)?);
        let batch = Self {
            schema_version: SCHEMA_VERSION,
            kind: "capability_success_batch".to_owned(),
            records,
            batch_sha256,
        };
        batch.validate()?;
        Ok(batch)
    }

    fn validate(&self) -> Result<(), CapabilityEvidenceError> {
        if self.schema_version != SCHEMA_VERSION
            || self.kind != "capability_success_batch"
            || self.records.is_empty()
            || !valid_sha256(&self.batch_sha256)
        {
            return Err(CapabilityEvidenceError::Invalid);
        }
        let first = self
            .records
            .first()
            .ok_or(CapabilityEvidenceError::Invalid)?;
        for (offset, record) in self.records.iter().enumerate() {
            record.validate()?;
            if record.binding != first.binding
                || record.verified_at_ms != first.verified_at_ms
                || record.sequence
                    != first
                        .sequence
                        .checked_add(offset as u64)
                        .ok_or(CapabilityEvidenceError::Sequence)?
            {
                return Err(CapabilityEvidenceError::Invalid);
            }
        }
        let expected =
            sha256_hex(serde_json::to_vec(&self.records).map_err(CapabilityEvidenceError::Encode)?);
        if self.batch_sha256 != expected {
            return Err(CapabilityEvidenceError::Invalid);
        }
        Ok(())
    }
}

impl CapabilityEvidenceRecord {
    fn validate(&self) -> Result<(), CapabilityEvidenceError> {
        self.binding.validate()?;
        if self.schema_version != SCHEMA_VERSION
            || self.sequence == 0
            || self.probe.is_empty()
            || !valid_sha256(&self.evidence_hash)
            || self.verified_at_ms == 0
            || self.valid_until_ms <= self.verified_at_ms
            || !self.success
        {
            return Err(CapabilityEvidenceError::Invalid);
        }
        Ok(())
    }
}

/// Append-only evidence journal. A malformed tail fails closed: it could conceal a newer expired
/// or differently bound probe result.
#[derive(Debug)]
pub struct CapabilityEvidenceStore {
    path: PathBuf,
    records: Vec<CapabilityEvidenceRecord>,
    next_sequence: u64,
}

impl CapabilityEvidenceStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, CapabilityEvidenceError> {
        let path = path.into();
        let records = recover(&path)?;
        let next_sequence = records
            .last()
            .map(|record| {
                record
                    .sequence
                    .checked_add(1)
                    .ok_or(CapabilityEvidenceError::Sequence)
            })
            .transpose()?
            .unwrap_or(1);
        Ok(Self {
            path,
            records,
            next_sequence,
        })
    }

    /// Appends a complete successful probe batch only after its caller has completed every
    /// requested check. The store never writes a success record for a failed doctor invocation.
    pub fn append_successes(
        &mut self,
        binding: &CapabilityBinding,
        verified_at_ms: u64,
        probes: &[CapabilityProbe],
    ) -> Result<(), CapabilityEvidenceError> {
        binding.validate()?;
        if verified_at_ms == 0 || probes.is_empty() {
            return Err(CapabilityEvidenceError::Invalid);
        }
        let mut pending = Vec::with_capacity(probes.len());
        for probe in probes {
            if probe.valid_until_ms <= verified_at_ms {
                return Err(CapabilityEvidenceError::Invalid);
            }
            let record = CapabilityEvidenceRecord {
                schema_version: SCHEMA_VERSION,
                sequence: self
                    .next_sequence
                    .checked_add(pending.len() as u64)
                    .ok_or(CapabilityEvidenceError::Sequence)?,
                binding: binding.clone(),
                capability: probe.capability,
                probe: probe.probe.clone(),
                evidence_hash: probe.evidence_hash.clone(),
                verified_at_ms,
                valid_until_ms: probe.valid_until_ms,
                success: true,
            };
            record.validate()?;
            pending.push(record);
        }
        let batch = CapabilityEvidenceBatchRecord::new(pending.clone())?;
        let encoded = serde_json::to_vec(&batch).map_err(CapabilityEvidenceError::Encode)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|source| CapabilityEvidenceError::Io {
                path: self.path.clone(),
                source,
            })?;
        file.write_all(&encoded)
            .and_then(|()| file.write_all(b"\n"))
            .map_err(|source| CapabilityEvidenceError::Io {
                path: self.path.clone(),
                source,
            })?;
        file.sync_data()
            .map_err(|source| CapabilityEvidenceError::Io {
                path: self.path.clone(),
                source,
            })?;
        self.next_sequence = self
            .next_sequence
            .checked_add(pending.len() as u64)
            .ok_or(CapabilityEvidenceError::Sequence)?;
        self.records.extend(pending);
        Ok(())
    }

    /// Returns the newest successful evidence for every capability of this exact binding that is
    /// still valid at `now_ms`. Other accounts, symbols and expired probes never cross the gate.
    pub fn current(
        &self,
        binding: &CapabilityBinding,
        now_ms: u64,
    ) -> Result<BTreeMap<Capability, CapabilityEvidence>, CapabilityEvidenceError> {
        binding.validate()?;
        if now_ms == 0 {
            return Err(CapabilityEvidenceError::Invalid);
        }
        let mut latest = BTreeMap::new();
        for record in &self.records {
            if &record.binding == binding && record.success && record.verified_at_ms <= now_ms {
                latest.insert(record.capability, record);
            }
        }
        let mut current = BTreeMap::new();
        for (capability, record) in latest {
            if record.valid_until_ms > now_ms {
                current.insert(
                    capability,
                    CapabilityEvidence {
                        evidence_hash: record.evidence_hash.clone(),
                        generation: record.sequence,
                        verified_at_ms: record.verified_at_ms,
                        valid_until_ms: record.valid_until_ms,
                    },
                );
            }
        }
        Ok(current)
    }
}

pub fn sha256_hex(value: impl AsRef<[u8]>) -> String {
    Sha256::digest(value.as_ref())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn recover(path: &Path) -> Result<Vec<CapabilityEvidenceRecord>, CapabilityEvidenceError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(CapabilityEvidenceError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        return Err(CapabilityEvidenceError::Truncated);
    }
    let mut records = Vec::new();
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let value: serde_json::Value =
            serde_json::from_slice(line).map_err(CapabilityEvidenceError::Decode)?;
        if value.get("kind").and_then(serde_json::Value::as_str) == Some("capability_success_batch")
        {
            let batch: CapabilityEvidenceBatchRecord =
                serde_json::from_value(value).map_err(CapabilityEvidenceError::Decode)?;
            batch.validate()?;
            for record in batch.records {
                if record.sequence != records.len() as u64 + 1 {
                    return Err(CapabilityEvidenceError::Sequence);
                }
                records.push(record);
            }
        } else {
            // Schema-1 journals wrote one already-synced success per line. They remain readable,
            // while every new multi-probe append is a single committed batch line.
            let record: CapabilityEvidenceRecord =
                serde_json::from_value(value).map_err(CapabilityEvidenceError::Decode)?;
            record.validate()?;
            if record.sequence != records.len() as u64 + 1 {
                return Err(CapabilityEvidenceError::Sequence);
            }
            records.push(record);
        }
    }
    Ok(records)
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[derive(Debug, thiserror::Error)]
pub enum CapabilityEvidenceError {
    #[error("capability evidence is invalid")]
    Invalid,
    #[error("capability evidence journal has a truncated tail")]
    Truncated,
    #[error("capability evidence sequence is invalid or exhausted")]
    Sequence,
    #[error("capability evidence I/O failed for {path}: {source}", path = path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("capability evidence encoding failed: {0}")]
    Encode(serde_json::Error),
    #[error("capability evidence decoding failed: {0}")]
    Decode(serde_json::Error),
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    fn binding() -> CapabilityBinding {
        CapabilityBinding {
            exchange: "binance".to_owned(),
            account_binding: "portfolio_margin_um".to_owned(),
            symbol: "DOGE/USDT".to_owned(),
            api_key_sha256: sha256_hex("key"),
        }
    }

    #[test]
    fn recovery_uses_only_current_success_for_the_exact_binding()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("capabilities.jsonl");
        let mut store = CapabilityEvidenceStore::open(&path)?;
        store.append_successes(
            &binding(),
            10,
            &[CapabilityProbe::new(
                Capability::PrivateReadback,
                "papi_readback_v1",
                sha256_hex("readback"),
                20,
            )?],
        )?;
        assert!(
            store
                .current(&binding(), 15)?
                .contains_key(&Capability::PrivateReadback)
        );
        store.append_successes(
            &binding(),
            16,
            &[CapabilityProbe::new(
                Capability::PrivateReadback,
                "papi_readback_v1",
                sha256_hex("newer_readback"),
                17,
            )?],
        )?;
        assert!(store.current(&binding(), 18)?.is_empty());
        assert!(store.current(&binding(), 20)?.is_empty());
        let mut other = binding();
        other.symbol = "BTC/USDT".to_owned();
        assert!(store.current(&other, 15)?.is_empty());
        assert!(
            CapabilityEvidenceStore::open(path)?
                .current(&binding(), 15)?
                .contains_key(&Capability::PrivateReadback)
        );
        Ok(())
    }

    #[test]
    fn a_multi_probe_success_is_one_hash_bound_journal_line()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("capabilities.jsonl");
        let mut store = CapabilityEvidenceStore::open(&path)?;
        store.append_successes(
            &binding(),
            10,
            &[
                CapabilityProbe::new(
                    Capability::PrivateReadback,
                    "papi_readback_v1",
                    sha256_hex("readback"),
                    20,
                )?,
                CapabilityProbe::new(Capability::PlaceLimit, "place_v1", sha256_hex("place"), 20)?,
            ],
        )?;
        let bytes = fs::read(&path)?;
        assert_eq!(bytes.split(|byte| *byte == b'\n').count(), 2);
        let recovered = CapabilityEvidenceStore::open(path)?;
        assert_eq!(recovered.current(&binding(), 15)?.len(), 2);
        Ok(())
    }
}
