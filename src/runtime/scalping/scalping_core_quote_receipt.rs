use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    execution::{
        ScalpingBoundLimits, ScalpingEntryQuote, ScalpingEntryQuoteError, ScalpingPrivateAdmission,
        ScalpingQuoteAuthority, validate_scalping_entry_quote,
    },
    strategy::scalping::{CandidatePreparation, SemanticIntent, StrategyBinding},
};

pub const SCALPING_CORE_QUOTE_RECEIPT_SCHEMA_VERSION: u16 = 1;

/// Complete immutable Core quote input. Every valuation is supplied by Core; this receipt source
/// only validates identities and persists the exact payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalpingCoreQuoteReceipt {
    pub schema_version: u16,
    pub binding: StrategyBinding,
    pub preparation_id: String,
    pub candidate_id: String,
    pub candidate_digest: String,
    pub preparation: CandidatePreparation,
    pub candidate: SemanticIntent,
    pub limits: ScalpingBoundLimits,
    pub private: ScalpingPrivateAdmission,
    pub quote_authority: ScalpingQuoteAuthority,
    pub quote: ScalpingEntryQuote,
    pub issued_at_ms: u64,
    pub received_at_ms: u64,
    pub expires_at_ms: u64,
    pub core_sequence: u64,
}

/// Local durable envelope. `sequence` is the journal order; `core_sequence` remains inside the
/// receipt as the externally supplied Core watermark.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalpingCoreQuoteReceiptRecord {
    pub sequence: u64,
    pub content_sha256: String,
    pub receipt: ScalpingCoreQuoteReceipt,
}

#[derive(Debug)]
pub struct ScalpingCoreQuoteReceiptJournal {
    path: PathBuf,
    binding: StrategyBinding,
    next_sequence: u64,
    records: Vec<ScalpingCoreQuoteReceiptRecord>,
}

impl ScalpingCoreQuoteReceiptJournal {
    pub fn open(
        path: impl Into<PathBuf>,
        binding: StrategyBinding,
    ) -> Result<Self, ScalpingCoreQuoteReceiptError> {
        binding
            .validate()
            .map_err(|_| ScalpingCoreQuoteReceiptError::Binding)?;
        let path = path.into();
        let records = recover(&path, &binding)?;
        let next_sequence = records
            .last()
            .map(|record| {
                record
                    .sequence
                    .checked_add(1)
                    .ok_or(ScalpingCoreQuoteReceiptError::Sequence)
            })
            .transpose()?
            .unwrap_or(1);
        Ok(Self {
            path,
            binding,
            next_sequence,
            records,
        })
    }

    /// Persists one already-complete Core quote and fsyncs it before returning. An exact retry
    /// returns its original envelope; this method never derives a quote field or valuation.
    pub fn append(
        &mut self,
        receipt: ScalpingCoreQuoteReceipt,
    ) -> Result<ScalpingCoreQuoteReceiptRecord, ScalpingCoreQuoteReceiptError> {
        validate_receipt(&receipt, &self.binding, receipt.received_at_ms)?;
        let content_sha256 = scalping_core_quote_receipt_digest(&receipt)?;
        if let Some(record) = classify_receipt(&self.records, &receipt, &content_sha256)? {
            return Ok(record.clone());
        }
        if let Some(last) = self.records.last() {
            if receipt.core_sequence <= last.receipt.core_sequence {
                return Err(ScalpingCoreQuoteReceiptError::CoreSequence);
            }
            if receipt.issued_at_ms < last.receipt.issued_at_ms
                || receipt.received_at_ms < last.receipt.received_at_ms
            {
                return Err(ScalpingCoreQuoteReceiptError::Timing);
            }
        }
        let record = ScalpingCoreQuoteReceiptRecord {
            sequence: self.next_sequence,
            content_sha256,
            receipt,
        };
        let encoded = serde_json::to_vec(&record).map_err(ScalpingCoreQuoteReceiptError::Encode)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|source| ScalpingCoreQuoteReceiptError::Io {
                path: self.path.clone(),
                source,
            })?;
        file.write_all(&encoded)
            .and_then(|()| file.write_all(b"\n"))
            .and_then(|()| file.sync_all())
            .map_err(|source| ScalpingCoreQuoteReceiptError::Io {
                path: self.path.clone(),
                source,
            })?;
        self.records.push(record.clone());
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(ScalpingCoreQuoteReceiptError::Sequence)?;
        Ok(record)
    }
}

/// Read-only, fully recovered quote source. Reopening is the refresh boundary; a missing journal
/// is an empty source, while any damaged or contradictory history rejects the open.
#[derive(Debug)]
pub struct ScalpingCoreQuoteReceiptSource {
    binding: StrategyBinding,
    records: Vec<ScalpingCoreQuoteReceiptRecord>,
}

impl ScalpingCoreQuoteReceiptSource {
    pub fn open(
        path: impl AsRef<Path>,
        binding: StrategyBinding,
    ) -> Result<Self, ScalpingCoreQuoteReceiptError> {
        binding
            .validate()
            .map_err(|_| ScalpingCoreQuoteReceiptError::Binding)?;
        let records = recover(path.as_ref(), &binding)?;
        Ok(Self { binding, records })
    }

    /// Returns only the newest exact receipt for this preparation/candidate. Missing or expired
    /// authority is `None`; an identity mismatch is an error and never falls back to an older
    /// quote or to public/Canary data.
    pub fn lookup(
        &self,
        preparation: &CandidatePreparation,
        candidate: &SemanticIntent,
        observed_at_ms: u64,
    ) -> Result<Option<ScalpingCoreQuoteReceiptRecord>, ScalpingCoreQuoteReceiptError> {
        if observed_at_ms == 0 || preparation.binding_digest != self.binding.digest() {
            return Err(ScalpingCoreQuoteReceiptError::Identity);
        }
        let candidate_digest = scalping_candidate_digest(candidate)?;
        let matching = self.records.iter().rev().find(|record| {
            record.receipt.preparation_id == preparation.preparation_id
                && record.receipt.candidate_id == candidate.intent_id
        });
        let Some(record) = matching else {
            return Ok(None);
        };
        if record.receipt.preparation != *preparation
            || record.receipt.candidate != *candidate
            || record.receipt.candidate_digest != candidate_digest
        {
            return Err(ScalpingCoreQuoteReceiptError::Conflict);
        }
        if observed_at_ms > record.receipt.expires_at_ms {
            return Ok(None);
        }
        validate_receipt(&record.receipt, &self.binding, observed_at_ms)?;
        Ok(Some(record.clone()))
    }
}

pub fn scalping_candidate_digest(
    candidate: &SemanticIntent,
) -> Result<String, ScalpingCoreQuoteReceiptError> {
    let encoded = serde_json::to_vec(candidate).map_err(ScalpingCoreQuoteReceiptError::Encode)?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

pub fn scalping_core_quote_receipt_digest(
    receipt: &ScalpingCoreQuoteReceipt,
) -> Result<String, ScalpingCoreQuoteReceiptError> {
    let encoded = serde_json::to_vec(receipt).map_err(ScalpingCoreQuoteReceiptError::Encode)?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

fn recover(
    path: &Path,
    binding: &StrategyBinding,
) -> Result<Vec<ScalpingCoreQuoteReceiptRecord>, ScalpingCoreQuoteReceiptError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(ScalpingCoreQuoteReceiptError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        return Err(ScalpingCoreQuoteReceiptError::Truncated);
    }
    let mut records = Vec::new();
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let record: ScalpingCoreQuoteReceiptRecord =
            serde_json::from_slice(line).map_err(ScalpingCoreQuoteReceiptError::Decode)?;
        let expected_sequence = u64::try_from(records.len())
            .map_err(|_| ScalpingCoreQuoteReceiptError::Sequence)?
            .checked_add(1)
            .ok_or(ScalpingCoreQuoteReceiptError::Sequence)?;
        if record.sequence != expected_sequence {
            return Err(ScalpingCoreQuoteReceiptError::Sequence);
        }
        let expected_digest = scalping_core_quote_receipt_digest(&record.receipt)?;
        if record.content_sha256 != expected_digest {
            return Err(ScalpingCoreQuoteReceiptError::Hash);
        }
        validate_receipt(&record.receipt, binding, record.receipt.received_at_ms)?;
        if classify_receipt(&records, &record.receipt, &expected_digest)?.is_some() {
            return Err(ScalpingCoreQuoteReceiptError::CoreSequence);
        }
        if let Some(last) = records.last() {
            if record.receipt.core_sequence <= last.receipt.core_sequence {
                return Err(ScalpingCoreQuoteReceiptError::CoreSequence);
            }
            if record.receipt.issued_at_ms < last.receipt.issued_at_ms
                || record.receipt.received_at_ms < last.receipt.received_at_ms
            {
                return Err(ScalpingCoreQuoteReceiptError::Timing);
            }
        }
        records.push(record);
    }
    Ok(records)
}

fn validate_receipt(
    receipt: &ScalpingCoreQuoteReceipt,
    binding: &StrategyBinding,
    observed_at_ms: u64,
) -> Result<(), ScalpingCoreQuoteReceiptError> {
    let candidate_digest = scalping_candidate_digest(&receipt.candidate)?;
    let private_valid_until_ms = receipt
        .private
        .observed_at_ms
        .checked_add(receipt.quote_authority.max_private_stale_ms)
        .ok_or(ScalpingCoreQuoteReceiptError::Timing)?;
    let exact_expiry = receipt
        .preparation
        .valid_until_ms
        .min(receipt.candidate.valid_until_ms)
        .min(receipt.quote.valid_until_ms)
        .min(private_valid_until_ms);
    if receipt.schema_version != SCALPING_CORE_QUOTE_RECEIPT_SCHEMA_VERSION
        || receipt.binding != *binding
        || receipt.preparation_id.trim().is_empty()
        || receipt.preparation_id != receipt.preparation.preparation_id
        || receipt.candidate_id.trim().is_empty()
        || receipt.candidate_id != receipt.candidate.intent_id
        || !digest_is_valid(&receipt.candidate_digest)
        || receipt.candidate_digest != candidate_digest
        || receipt.preparation.binding_digest != binding.digest()
        || receipt.issued_at_ms == 0
        || receipt.received_at_ms < receipt.issued_at_ms
        || receipt.issued_at_ms < receipt.preparation.watermark_ms
        || receipt.issued_at_ms < receipt.private.observed_at_ms
        || receipt.expires_at_ms != exact_expiry
        || receipt.expires_at_ms < receipt.received_at_ms
        || receipt.core_sequence == 0
        || observed_at_ms < receipt.received_at_ms
    {
        return Err(ScalpingCoreQuoteReceiptError::Identity);
    }
    validate_scalping_entry_quote(
        &receipt.preparation,
        &receipt.candidate,
        &receipt.limits,
        &receipt.private,
        &receipt.quote_authority,
        &receipt.quote,
        observed_at_ms,
    )?;
    Ok(())
}

fn classify_receipt<'a>(
    records: &'a [ScalpingCoreQuoteReceiptRecord],
    receipt: &ScalpingCoreQuoteReceipt,
    content_sha256: &str,
) -> Result<Option<&'a ScalpingCoreQuoteReceiptRecord>, ScalpingCoreQuoteReceiptError> {
    if let Some(exact) = records
        .iter()
        .find(|record| record.receipt == *receipt && record.content_sha256 == content_sha256)
    {
        return Ok(Some(exact));
    }
    for record in records {
        let existing = &record.receipt;
        let same_pair = existing.preparation_id == receipt.preparation_id
            && existing.candidate_id == receipt.candidate_id;
        if existing.core_sequence == receipt.core_sequence
            || existing.quote.quote_id == receipt.quote.quote_id
            || (existing.preparation_id == receipt.preparation_id
                && existing.preparation != receipt.preparation)
            || (existing.candidate_id == receipt.candidate_id
                && (existing.preparation_id != receipt.preparation_id
                    || existing.candidate_digest != receipt.candidate_digest))
            || (same_pair && existing.quote.generation >= receipt.quote.generation)
        {
            return Err(ScalpingCoreQuoteReceiptError::Conflict);
        }
    }
    Ok(None)
}

fn digest_is_valid(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingCoreQuoteReceiptError {
    #[error("Core quote receipt binding is invalid or cross-bound")]
    Binding,
    #[error("Core quote receipt identity is incomplete or inconsistent")]
    Identity,
    #[error("Core quote receipt timing or private freshness overflows")]
    Timing,
    #[error("Core quote receipt reuses an identity with different content")]
    Conflict,
    #[error("Core quote receipt journal has a truncated tail")]
    Truncated,
    #[error("Core quote receipt journal sequence is invalid or exhausted")]
    Sequence,
    #[error("Core quote sequence regressed or conflicted")]
    CoreSequence,
    #[error("Core quote receipt content hash does not match")]
    Hash,
    #[error("Core quote validation failed: {0}")]
    Quote(#[from] ScalpingEntryQuoteError),
    #[error("Core quote receipt I/O failed for {path}: {source}", path = path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("Core quote receipt encoding failed: {0}")]
    Encode(serde_json::Error),
    #[error("Core quote receipt JSON is invalid: {0}")]
    Decode(serde_json::Error),
}
