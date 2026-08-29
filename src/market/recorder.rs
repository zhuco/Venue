use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::domain::Symbol;

pub const RAW_SCHEMA_VERSION: u16 = 1;
const RAW_SYNC_BATCH_RECORDS: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawSource {
    RestSnapshot,
    RestKline,
    WebSocketDelta,
    WebSocketTrade,
    WebSocketKline,
    WebSocketTicker,
    WebSocketMarkFunding,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RawMarketRecord {
    pub schema_version: u16,
    pub parser_schema_version: u16,
    pub capture_sequence: u64,
    pub source: RawSource,
    pub symbol: Symbol,
    pub generation: u64,
    pub received_at_ms: u64,
    pub payload_sha256: String,
    pub payload: String,
}

impl RawMarketRecord {
    pub fn new(
        source: RawSource,
        symbol: Symbol,
        generation: u64,
        received_at_ms: u64,
        payload: String,
    ) -> Result<Self, RawError> {
        if generation == 0 || received_at_ms == 0 || payload.is_empty() {
            return Err(RawError::Invalid);
        }
        Ok(Self {
            schema_version: RAW_SCHEMA_VERSION,
            parser_schema_version: crate::exchange::binance::PARSER_SCHEMA_VERSION,
            capture_sequence: 0,
            source,
            symbol,
            generation,
            received_at_ms,
            payload_sha256: digest(&payload),
            payload,
        })
    }

    pub fn verify_hash(&self) -> bool {
        self.payload_sha256 == digest(&self.payload)
    }
}

#[derive(Debug)]
pub struct RawMarketRecorder {
    path: PathBuf,
    file: File,
    next_sequence: u64,
    symbol: Option<Symbol>,
    last_generation: Option<u64>,
    unsynced_records: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawRecovery {
    pub records: Vec<RawMarketRecord>,
}

impl RawMarketRecorder {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, RawError> {
        let path = path.into();
        let recovery = recover(&path, None)?;
        Self::from_recovery(path, recovery, None)
    }

    /// Opens a journal scoped to one canonical symbol. Recovery is fail-closed before any new
    /// record can be appended, so a process cannot accidentally continue another symbol's log.
    pub fn open_for_symbol(path: impl Into<PathBuf>, symbol: Symbol) -> Result<Self, RawError> {
        let path = path.into();
        let recovery = recover(&path, Some(&symbol))?;
        Self::from_recovery(path, recovery, Some(symbol))
    }

    fn from_recovery(
        path: PathBuf,
        recovery: RawRecovery,
        symbol: Option<Symbol>,
    ) -> Result<Self, RawError> {
        let (next_sequence, last_generation) = match recovery.records.last() {
            Some(record) => (
                record
                    .capture_sequence
                    .checked_add(1)
                    .ok_or(RawError::Sequence)?,
                Some(record.generation),
            ),
            None => (1, None),
        };
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|source| RawError::Io {
                path: path.clone(),
                source,
            })?;
        Ok(Self {
            path,
            file,
            next_sequence,
            symbol,
            last_generation,
            unsynced_records: 0,
        })
    }

    pub fn append(&mut self, mut record: RawMarketRecord) -> Result<u64, RawError> {
        validate_record_shape(&record, self.symbol.as_ref())?;
        if !record.verify_hash() {
            return Err(RawError::Hash);
        }
        if self
            .last_generation
            .is_some_and(|generation| record.generation < generation)
        {
            return Err(RawError::Generation);
        }
        let next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(RawError::Sequence)?;
        record.capture_sequence = self.next_sequence;
        let encoded = serde_json::to_vec(&record).map_err(RawError::Encode)?;
        self.file
            .write_all(&encoded)
            .map_err(|source| RawError::Io {
                path: self.path.clone(),
                source,
            })?;
        self.file.write_all(b"\n").map_err(|source| RawError::Io {
            path: self.path.clone(),
            source,
        })?;
        self.next_sequence = next_sequence;
        self.last_generation = Some(record.generation);
        self.unsynced_records = self.unsynced_records.saturating_add(1);
        if self.unsynced_records >= RAW_SYNC_BATCH_RECORDS {
            self.sync_pending()?;
        }
        Ok(record.capture_sequence)
    }

    /// Syncs every record written since the last successful batch boundary. Callers must invoke
    /// this before exposing any frame or observation that can influence business state.
    pub fn sync_pending(&mut self) -> Result<(), RawError> {
        if self.unsynced_records == 0 {
            return Ok(());
        }
        self.file.sync_data().map_err(|source| RawError::Io {
            path: self.path.clone(),
            source,
        })?;
        self.unsynced_records = 0;
        Ok(())
    }

    pub fn recover(&self) -> Result<RawRecovery, RawError> {
        self.file.sync_data().map_err(|source| RawError::Io {
            path: self.path.clone(),
            source,
        })?;
        recover(&self.path, self.symbol.as_ref())
    }

    pub const fn next_capture_sequence(&self) -> u64 {
        self.next_sequence
    }

    pub const fn last_generation(&self) -> Option<u64> {
        self.last_generation
    }

    pub const fn pending_sync_count(&self) -> usize {
        self.unsynced_records
    }

    pub fn bind_symbol(&mut self, symbol: &Symbol) -> Result<(), RawError> {
        self.sync_pending()?;
        let recovery = recover(&self.path, Some(symbol))?;
        self.symbol = Some(symbol.clone());
        if let Some(record) = recovery.records.last() {
            self.last_generation = Some(record.generation);
        }
        Ok(())
    }
}

impl Drop for RawMarketRecorder {
    fn drop(&mut self) {
        let _ = self.sync_pending();
    }
}

fn recover(path: &Path, expected_symbol: Option<&Symbol>) -> Result<RawRecovery, RawError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RawRecovery {
                records: Vec::new(),
            });
        }
        Err(source) => {
            return Err(RawError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        return Err(RawError::Truncated);
    }
    let mut records = Vec::new();
    let mut previous_generation = None;
    let mut expected_sequence = 1_u64;
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let record: RawMarketRecord = serde_json::from_slice(line).map_err(RawError::Decode)?;
        validate_record_shape(&record, expected_symbol)?;
        if !record.verify_hash() {
            return Err(RawError::Hash);
        }
        if record.capture_sequence != expected_sequence {
            return Err(RawError::Sequence);
        }
        if previous_generation.is_some_and(|generation| record.generation < generation) {
            return Err(RawError::Generation);
        }
        previous_generation = Some(record.generation);
        records.push(record);
        expected_sequence = expected_sequence.checked_add(1).ok_or(RawError::Sequence)?;
    }
    Ok(RawRecovery { records })
}

fn validate_record_shape(
    record: &RawMarketRecord,
    expected_symbol: Option<&Symbol>,
) -> Result<(), RawError> {
    if record.schema_version != RAW_SCHEMA_VERSION {
        return Err(RawError::Schema);
    }
    if record.parser_schema_version != crate::exchange::binance::PARSER_SCHEMA_VERSION {
        return Err(RawError::ParserSchema);
    }
    if record.generation == 0 || record.received_at_ms == 0 || record.payload.is_empty() {
        return Err(RawError::Invalid);
    }
    if expected_symbol.is_some_and(|symbol| symbol != &record.symbol) {
        return Err(RawError::Symbol);
    }
    Ok(())
}

fn digest(payload: &str) -> String {
    Sha256::digest(payload.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Debug, thiserror::Error)]
pub enum RawError {
    #[error("raw market record is invalid")]
    Invalid,
    #[error("raw market record hash does not match its payload")]
    Hash,
    #[error("raw market record schema is unsupported")]
    Schema,
    #[error("raw market record parser schema is unsupported")]
    ParserSchema,
    #[error("raw market record symbol does not match the journal scope")]
    Symbol,
    #[error("raw market record generation regressed")]
    Generation,
    #[error("raw market journal has a truncated tail")]
    Truncated,
    #[error("raw market capture sequence is invalid or exhausted")]
    Sequence,
    #[error("raw market storage I/O failed for {path}: {source}", path = path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("raw market record encoding failed: {0}")]
    Encode(serde_json::Error),
    #[error("raw market record decoding failed: {0}")]
    Decode(serde_json::Error),
}
