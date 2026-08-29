use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::domain::{Amount, PositionSide, Symbol};

pub const CANARY_EVIDENCE_SCHEMA_VERSION: u16 = 1;

/// Immutable identity and hard envelope for exactly one Canary scope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanaryEvidenceBinding {
    pub canary_id: String,
    pub exchange: String,
    pub account: String,
    pub symbol: Symbol,
    pub owner_scope: String,
    pub release_id: String,
    pub position_side: PositionSide,
    pub quote_cap: Amount,
    pub risk_cap: Amount,
    pub valid_until_ms: u64,
}

impl CanaryEvidenceBinding {
    fn validate(&self, created_at_ms: u64) -> Result<(), CanaryEvidenceError> {
        if [
            self.canary_id.as_str(),
            self.exchange.as_str(),
            self.account.as_str(),
            self.owner_scope.as_str(),
            self.release_id.as_str(),
        ]
        .iter()
        .any(|value| value.trim().is_empty())
            || self.position_side == PositionSide::Net
            || !self.quote_cap.value.is_sign_positive()
            || self.quote_cap.value.is_zero()
            || !self.risk_cap.value.is_sign_positive()
            || self.risk_cap.value.is_zero()
            || self.quote_cap.asset.as_str() != "USDT"
            || self.quote_cap.asset != self.risk_cap.asset
            || self.symbol.quote() != self.quote_cap.asset.as_str()
            || self.quote_cap.value > Decimal::new(super::CANARY_MAX_ENTRY_NOTIONAL_USDT, 0)
            || self.risk_cap.value > Decimal::new(super::CANARY_MAX_ENTRY_NOTIONAL_USDT, 0)
            || self.risk_cap.value > self.quote_cap.value
            || created_at_ms == 0
            || self.valid_until_ms <= created_at_ms
        {
            return Err(CanaryEvidenceError::Binding);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanaryEvidenceHeader {
    pub schema_version: u16,
    pub sequence: u64,
    pub created_at_ms: u64,
    pub binding: CanaryEvidenceBinding,
    pub binding_sha256: String,
    pub record_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanaryEvidenceStage {
    pub schema_version: u16,
    pub sequence: u64,
    pub observed_at_ms: u64,
    pub binding_sha256: String,
    pub name: String,
    pub evidence: BTreeMap<String, String>,
    pub previous_sha256: String,
    pub content_sha256: String,
    pub record_sha256: String,
}

/// A terminal record proves either an exact flat scope or active, exact protection custody.
/// It deliberately has no "passed" state: an admitted first order is never terminal evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    rename_all = "snake_case",
    tag = "state",
    content = "detail",
    deny_unknown_fields
)]
pub enum CanaryTerminalState {
    Flat { exact_readback_sha256: String },
    Protected { exact_readback_sha256: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanaryEvidenceTerminal {
    pub schema_version: u16,
    pub sequence: u64,
    pub observed_at_ms: u64,
    pub binding_sha256: String,
    pub terminal: CanaryTerminalState,
    pub previous_sha256: String,
    pub content_sha256: String,
    pub record_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    rename_all = "snake_case",
    tag = "record",
    content = "value",
    deny_unknown_fields
)]
pub enum CanaryEvidenceRecord {
    Header(CanaryEvidenceHeader),
    Stage(CanaryEvidenceStage),
    Terminal(CanaryEvidenceTerminal),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanaryEvidenceRecovery {
    pub(crate) header: CanaryEvidenceHeader,
    pub(crate) stages: Vec<CanaryEvidenceStage>,
    pub(crate) terminal: Option<CanaryEvidenceTerminal>,
}

impl CanaryEvidenceRecovery {
    pub const fn header(&self) -> &CanaryEvidenceHeader {
        &self.header
    }

    pub fn stages(&self) -> &[CanaryEvidenceStage] {
        &self.stages
    }

    pub const fn terminal(&self) -> Option<&CanaryEvidenceTerminal> {
        self.terminal.as_ref()
    }
}

/// Concrete append-only evidence writer. It owns no credentials, exchange client, order, or
/// mutation permission; its only side effect is durable local evidence persistence.
#[derive(Debug)]
pub struct CanaryEvidenceJournal {
    path: PathBuf,
    binding: CanaryEvidenceBinding,
    binding_sha256: String,
    next_sequence: u64,
    previous_sha256: String,
    last_observed_at_ms: u64,
    terminal_written: bool,
}

impl CanaryEvidenceJournal {
    /// Creates a new evidence file and refuses any pre-existing target, including a prior failed
    /// Canary receipt. The header is the first fsync'd record.
    pub fn create_new(
        path: impl Into<PathBuf>,
        binding: CanaryEvidenceBinding,
        created_at_ms: u64,
    ) -> Result<Self, CanaryEvidenceError> {
        binding.validate(created_at_ms)?;
        let path = path.into();
        let binding_sha256 = digest_json(&binding)?;
        let mut header = CanaryEvidenceHeader {
            schema_version: CANARY_EVIDENCE_SCHEMA_VERSION,
            sequence: 1,
            created_at_ms,
            binding: binding.clone(),
            binding_sha256,
            record_sha256: String::new(),
        };
        header.record_sha256 = header_digest(&header)?;
        validate_header(&header)?;

        let encoded = encode_record(&CanaryEvidenceRecord::Header(header.clone()))?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|source| CanaryEvidenceError::Io {
                path: path.clone(),
                source,
            })?;
        write_and_sync(&mut file, &encoded, &path)?;

        Ok(Self {
            path,
            binding,
            binding_sha256: header.binding_sha256.clone(),
            next_sequence: 2,
            previous_sha256: header.record_sha256,
            last_observed_at_ms: created_at_ms,
            terminal_written: false,
        })
    }

    /// Opens a prior evidence file only when its immutable binding is exactly the caller's scope.
    pub fn open_existing(
        path: impl Into<PathBuf>,
        expected_binding: &CanaryEvidenceBinding,
    ) -> Result<Self, CanaryEvidenceError> {
        let path = path.into();
        let recovery = recover(&path, expected_binding)?;
        if recovery.terminal.is_some() {
            return Err(CanaryEvidenceError::Terminal);
        }
        let previous_sha256 = recovery
            .stages
            .last()
            .map(|stage| stage.record_sha256.clone())
            .unwrap_or_else(|| recovery.header.record_sha256.clone());
        let next_sequence = recovery
            .stages
            .last()
            .map(|stage| stage.sequence)
            .unwrap_or(recovery.header.sequence)
            .checked_add(1)
            .ok_or(CanaryEvidenceError::Sequence)?;
        let last_observed_at_ms = recovery
            .stages
            .last()
            .map(|stage| stage.observed_at_ms)
            .unwrap_or(recovery.header.created_at_ms);
        Ok(Self {
            path,
            binding: expected_binding.clone(),
            binding_sha256: recovery.header.binding_sha256,
            next_sequence,
            previous_sha256,
            last_observed_at_ms,
            terminal_written: false,
        })
    }

    pub fn append_stage(
        &mut self,
        name: impl Into<String>,
        observed_at_ms: u64,
        evidence: BTreeMap<String, String>,
    ) -> Result<u64, CanaryEvidenceError> {
        if self.terminal_written {
            return Err(CanaryEvidenceError::Terminal);
        }
        if observed_at_ms < self.last_observed_at_ms || observed_at_ms > self.binding.valid_until_ms
        {
            return Err(CanaryEvidenceError::Stage);
        }
        let name = name.into();
        validate_stage_input(&name, observed_at_ms, &evidence)?;
        let mut stage = CanaryEvidenceStage {
            schema_version: CANARY_EVIDENCE_SCHEMA_VERSION,
            sequence: self.next_sequence,
            observed_at_ms,
            binding_sha256: self.binding_sha256.clone(),
            name,
            evidence,
            previous_sha256: self.previous_sha256.clone(),
            content_sha256: String::new(),
            record_sha256: String::new(),
        };
        stage.content_sha256 = stage_content_digest(&stage)?;
        stage.record_sha256 = stage_record_digest(&stage)?;
        validate_stage(
            &stage,
            &self.binding_sha256,
            self.next_sequence,
            &self.previous_sha256,
        )?;
        let sequence = stage.sequence;
        self.append_record(CanaryEvidenceRecord::Stage(stage))?;
        Ok(sequence)
    }

    /// Seals this file once. The sole allowed terminal facts are exact flat or exact protected
    /// readback summaries; callers cannot represent entry admission as a successful terminal.
    pub fn seal_terminal(
        &mut self,
        observed_at_ms: u64,
        terminal: CanaryTerminalState,
    ) -> Result<u64, CanaryEvidenceError> {
        if self.terminal_written {
            return Err(CanaryEvidenceError::Terminal);
        }
        validate_terminal_state(&terminal)?;
        if observed_at_ms < self.last_observed_at_ms || observed_at_ms > self.binding.valid_until_ms
        {
            return Err(CanaryEvidenceError::Terminal);
        }
        let mut record = CanaryEvidenceTerminal {
            schema_version: CANARY_EVIDENCE_SCHEMA_VERSION,
            sequence: self.next_sequence,
            observed_at_ms,
            binding_sha256: self.binding_sha256.clone(),
            terminal,
            previous_sha256: self.previous_sha256.clone(),
            content_sha256: String::new(),
            record_sha256: String::new(),
        };
        record.content_sha256 = terminal_content_digest(&record)?;
        record.record_sha256 = terminal_record_digest(&record)?;
        validate_terminal(
            &record,
            &self.binding_sha256,
            self.next_sequence,
            &self.previous_sha256,
        )?;
        let sequence = record.sequence;
        self.append_record(CanaryEvidenceRecord::Terminal(record))?;
        self.terminal_written = true;
        Ok(sequence)
    }

    pub fn binding(&self) -> &CanaryEvidenceBinding {
        &self.binding
    }

    pub fn recover(&self) -> Result<CanaryEvidenceRecovery, CanaryEvidenceError> {
        recover(&self.path, &self.binding)
    }

    fn append_record(&mut self, record: CanaryEvidenceRecord) -> Result<(), CanaryEvidenceError> {
        let encoded = encode_record(&record)?;
        let record_sha256 = match &record {
            CanaryEvidenceRecord::Header(_) => return Err(CanaryEvidenceError::Header),
            CanaryEvidenceRecord::Stage(stage) => stage.record_sha256.clone(),
            CanaryEvidenceRecord::Terminal(terminal) => terminal.record_sha256.clone(),
        };
        let mut file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(|source| CanaryEvidenceError::Io {
                path: self.path.clone(),
                source,
            })?;
        write_and_sync(&mut file, &encoded, &self.path)?;
        self.previous_sha256 = record_sha256;
        self.last_observed_at_ms = match &record {
            CanaryEvidenceRecord::Header(_) => return Err(CanaryEvidenceError::Header),
            CanaryEvidenceRecord::Stage(stage) => stage.observed_at_ms,
            CanaryEvidenceRecord::Terminal(terminal) => terminal.observed_at_ms,
        };
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(CanaryEvidenceError::Sequence)?;
        Ok(())
    }
}

pub fn recover(
    path: &Path,
    expected_binding: &CanaryEvidenceBinding,
) -> Result<CanaryEvidenceRecovery, CanaryEvidenceError> {
    let bytes = fs::read(path).map_err(|source| CanaryEvidenceError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    if bytes.is_empty() {
        return Err(CanaryEvidenceError::Header);
    }
    if !bytes.ends_with(b"\n") {
        return Err(CanaryEvidenceError::Truncated);
    }
    let mut records = bytes[..bytes.len() - 1].split(|byte| *byte == b'\n');
    let Some(first) = records.next() else {
        return Err(CanaryEvidenceError::Header);
    };
    if first.is_empty() {
        return Err(CanaryEvidenceError::Header);
    }
    let CanaryEvidenceRecord::Header(header) = decode_record(first)? else {
        return Err(CanaryEvidenceError::Header);
    };
    validate_header(&header)?;
    if &header.binding != expected_binding {
        return Err(CanaryEvidenceError::Binding);
    }

    let mut stages = Vec::new();
    let mut terminal = None;
    let mut sequence = 2_u64;
    let mut previous_sha256 = header.record_sha256.clone();
    let mut last_observed_at_ms = header.created_at_ms;
    for line in records {
        if line.is_empty() {
            return Err(CanaryEvidenceError::Truncated);
        }
        match decode_record(line)? {
            CanaryEvidenceRecord::Header(_) => return Err(CanaryEvidenceError::Header),
            CanaryEvidenceRecord::Stage(stage) => {
                if terminal.is_some() {
                    return Err(CanaryEvidenceError::Terminal);
                }
                validate_stage(&stage, &header.binding_sha256, sequence, &previous_sha256)?;
                if stage.observed_at_ms < last_observed_at_ms
                    || stage.observed_at_ms > header.binding.valid_until_ms
                {
                    return Err(CanaryEvidenceError::Stage);
                }
                previous_sha256 = stage.record_sha256.clone();
                last_observed_at_ms = stage.observed_at_ms;
                sequence = sequence
                    .checked_add(1)
                    .ok_or(CanaryEvidenceError::Sequence)?;
                stages.push(stage);
            }
            CanaryEvidenceRecord::Terminal(value) => {
                if terminal.is_some() {
                    return Err(CanaryEvidenceError::Terminal);
                }
                validate_terminal(&value, &header.binding_sha256, sequence, &previous_sha256)?;
                if value.observed_at_ms < last_observed_at_ms
                    || value.observed_at_ms > header.binding.valid_until_ms
                {
                    return Err(CanaryEvidenceError::Terminal);
                }
                previous_sha256 = value.record_sha256.clone();
                last_observed_at_ms = value.observed_at_ms;
                sequence = sequence
                    .checked_add(1)
                    .ok_or(CanaryEvidenceError::Sequence)?;
                terminal = Some(value);
            }
        }
    }
    Ok(CanaryEvidenceRecovery {
        header,
        stages,
        terminal,
    })
}

/// Discovers the immutable binding from the journal header, then performs the same full-chain
/// recovery as [`recover`]. This is intended for bounded artifact discovery; callers still receive
/// no mutation authority from the recovered value.
pub fn recover_discovered(path: &Path) -> Result<CanaryEvidenceRecovery, CanaryEvidenceError> {
    let bytes = fs::read(path).map_err(|source| CanaryEvidenceError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    if bytes.is_empty() || !bytes.ends_with(b"\n") {
        return Err(if bytes.is_empty() {
            CanaryEvidenceError::Header
        } else {
            CanaryEvidenceError::Truncated
        });
    }
    let first = bytes
        .split(|byte| *byte == b'\n')
        .next()
        .filter(|line| !line.is_empty())
        .ok_or(CanaryEvidenceError::Header)?;
    let CanaryEvidenceRecord::Header(header) = decode_record(first)? else {
        return Err(CanaryEvidenceError::Header);
    };
    validate_header(&header)?;
    recover(path, &header.binding)
}

fn write_and_sync(
    file: &mut std::fs::File,
    encoded: &[u8],
    path: &Path,
) -> Result<(), CanaryEvidenceError> {
    file.write_all(encoded)
        .and_then(|()| file.write_all(b"\n"))
        .and_then(|()| file.sync_data())
        .map_err(|source| CanaryEvidenceError::Io {
            path: path.to_path_buf(),
            source,
        })
}

fn validate_header(header: &CanaryEvidenceHeader) -> Result<(), CanaryEvidenceError> {
    if header.schema_version != CANARY_EVIDENCE_SCHEMA_VERSION || header.sequence != 1 {
        return Err(CanaryEvidenceError::Header);
    }
    header.binding.validate(header.created_at_ms)?;
    if !is_digest(&header.binding_sha256)
        || header.binding_sha256 != digest_json(&header.binding)?
        || !is_digest(&header.record_sha256)
        || header.record_sha256 != header_digest(header)?
    {
        return Err(CanaryEvidenceError::Hash);
    }
    Ok(())
}

fn validate_stage_input(
    name: &str,
    observed_at_ms: u64,
    evidence: &BTreeMap<String, String>,
) -> Result<(), CanaryEvidenceError> {
    if name.trim().is_empty()
        || observed_at_ms == 0
        || evidence.is_empty()
        || evidence.iter().any(|(key, value)| {
            key.trim().is_empty() || value.trim().is_empty() || sensitive_key(key)
        })
    {
        return Err(CanaryEvidenceError::Stage);
    }
    Ok(())
}

fn validate_stage(
    stage: &CanaryEvidenceStage,
    binding_sha256: &str,
    sequence: u64,
    previous_sha256: &str,
) -> Result<(), CanaryEvidenceError> {
    validate_stage_input(&stage.name, stage.observed_at_ms, &stage.evidence)?;
    if stage.schema_version != CANARY_EVIDENCE_SCHEMA_VERSION
        || stage.sequence != sequence
        || stage.binding_sha256 != binding_sha256
        || stage.previous_sha256 != previous_sha256
        || !is_digest(&stage.binding_sha256)
        || !is_digest(&stage.previous_sha256)
        || !is_digest(&stage.content_sha256)
        || !is_digest(&stage.record_sha256)
        || stage.content_sha256 != stage_content_digest(stage)?
        || stage.record_sha256 != stage_record_digest(stage)?
    {
        return Err(CanaryEvidenceError::Hash);
    }
    Ok(())
}

fn validate_terminal_state(terminal: &CanaryTerminalState) -> Result<(), CanaryEvidenceError> {
    let digest = match terminal {
        CanaryTerminalState::Flat {
            exact_readback_sha256,
        }
        | CanaryTerminalState::Protected {
            exact_readback_sha256,
        } => exact_readback_sha256,
    };
    if !is_digest(digest) {
        return Err(CanaryEvidenceError::Terminal);
    }
    Ok(())
}

fn validate_terminal(
    terminal: &CanaryEvidenceTerminal,
    binding_sha256: &str,
    sequence: u64,
    previous_sha256: &str,
) -> Result<(), CanaryEvidenceError> {
    validate_terminal_state(&terminal.terminal)?;
    if terminal.schema_version != CANARY_EVIDENCE_SCHEMA_VERSION
        || terminal.sequence != sequence
        || terminal.observed_at_ms == 0
        || terminal.binding_sha256 != binding_sha256
        || terminal.previous_sha256 != previous_sha256
        || !is_digest(&terminal.binding_sha256)
        || !is_digest(&terminal.previous_sha256)
        || !is_digest(&terminal.content_sha256)
        || !is_digest(&terminal.record_sha256)
        || terminal.content_sha256 != terminal_content_digest(terminal)?
        || terminal.record_sha256 != terminal_record_digest(terminal)?
    {
        return Err(CanaryEvidenceError::Hash);
    }
    Ok(())
}

fn header_digest(header: &CanaryEvidenceHeader) -> Result<String, CanaryEvidenceError> {
    digest_json(&(
        "venue.canary.evidence.header.v1",
        header.schema_version,
        header.sequence,
        header.created_at_ms,
        &header.binding,
        &header.binding_sha256,
    ))
}

fn stage_content_digest(stage: &CanaryEvidenceStage) -> Result<String, CanaryEvidenceError> {
    digest_json(&(
        "venue.canary.evidence.stage.content.v1",
        stage.schema_version,
        stage.sequence,
        stage.observed_at_ms,
        &stage.binding_sha256,
        &stage.name,
        &stage.evidence,
    ))
}

fn stage_record_digest(stage: &CanaryEvidenceStage) -> Result<String, CanaryEvidenceError> {
    digest_json(&(
        "venue.canary.evidence.stage.record.v1",
        &stage.previous_sha256,
        &stage.content_sha256,
    ))
}

fn terminal_content_digest(
    terminal: &CanaryEvidenceTerminal,
) -> Result<String, CanaryEvidenceError> {
    digest_json(&(
        "venue.canary.evidence.terminal.content.v1",
        terminal.schema_version,
        terminal.sequence,
        terminal.observed_at_ms,
        &terminal.binding_sha256,
        &terminal.terminal,
    ))
}

fn terminal_record_digest(
    terminal: &CanaryEvidenceTerminal,
) -> Result<String, CanaryEvidenceError> {
    digest_json(&(
        "venue.canary.evidence.terminal.record.v1",
        &terminal.previous_sha256,
        &terminal.content_sha256,
    ))
}

fn encode_record(record: &CanaryEvidenceRecord) -> Result<Vec<u8>, CanaryEvidenceError> {
    serde_json::to_vec(record).map_err(CanaryEvidenceError::Encode)
}

fn decode_record(bytes: &[u8]) -> Result<CanaryEvidenceRecord, CanaryEvidenceError> {
    serde_json::from_slice(bytes).map_err(CanaryEvidenceError::Decode)
}

fn digest_json<T: Serialize>(value: &T) -> Result<String, CanaryEvidenceError> {
    let canonical = serde_json::to_vec(value).map_err(CanaryEvidenceError::Encode)?;
    Ok(format!("{:x}", Sha256::digest(canonical)))
}

fn is_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    [
        "token",
        "bearer",
        "secret",
        "credential",
        "api_key",
        "access_key",
        "private_key",
        "client_secret",
        "password",
        "authorization",
        "signature",
        "cookie",
    ]
    .iter()
    .any(|needle| key.contains(needle))
}

#[derive(Debug, thiserror::Error)]
pub enum CanaryEvidenceError {
    #[error("Canary evidence binding is invalid or differs from the expected scope")]
    Binding,
    #[error("Canary evidence header is invalid")]
    Header,
    #[error("Canary evidence stage is invalid or contains a sensitive field name")]
    Stage,
    #[error("Canary evidence terminal is invalid or already sealed")]
    Terminal,
    #[error("Canary evidence is truncated")]
    Truncated,
    #[error("Canary evidence sequence is invalid or exhausted")]
    Sequence,
    #[error("Canary evidence digest or hash chain is invalid")]
    Hash,
    #[error("Canary evidence I/O failed for {path}: {source}", path = path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("Canary evidence encoding failed: {0}")]
    Encode(serde_json::Error),
    #[error("Canary evidence decoding failed: {0}")]
    Decode(serde_json::Error),
}
