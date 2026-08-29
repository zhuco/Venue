use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::domain::Symbol;

const SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Stage7PublicSource {
    RestSnapshot,
    RestTicker,
    WebSocketDepth,
    WebSocketBbo,
    WebSocketTrade,
    WebSocketMark,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Stage7PublicRecord {
    pub schema_version: u16,
    pub capture_sequence: u64,
    pub exchange: String,
    pub symbol: Symbol,
    pub generation: u64,
    pub source: Stage7PublicSource,
    pub received_at_ms: u64,
    pub payload_sha256: String,
    pub payload: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Stage7PublicBinding {
    pub exchange: String,
    pub symbol: Symbol,
}

pub(super) struct Stage7PublicJournal {
    path: PathBuf,
    file: File,
    binding: Stage7PublicBinding,
    next_capture_sequence: u64,
    max_generation: u64,
    repair_len: Option<u64>,
}

impl Stage7PublicJournal {
    pub(super) fn max_generation_at_path(
        path: PathBuf,
        binding: Stage7PublicBinding,
    ) -> Result<u64, Stage7PublicJournalError> {
        Ok(recover(&path, &binding)?
            .iter()
            .map(|record| record.generation)
            .max()
            .unwrap_or(0))
    }

    pub(super) fn open(
        path: PathBuf,
        binding: Stage7PublicBinding,
    ) -> Result<Self, Stage7PublicJournalError> {
        if binding.exchange.trim().is_empty() {
            return Err(Stage7PublicJournalError::Binding);
        }
        let records = recover(&path, &binding)?;
        let next_capture_sequence = records
            .last()
            .map(|record| {
                record
                    .capture_sequence
                    .checked_add(1)
                    .ok_or(Stage7PublicJournalError::Sequence)
            })
            .transpose()?
            .unwrap_or(1);
        let max_generation = records
            .iter()
            .map(|record| record.generation)
            .max()
            .unwrap_or(0);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)
            .map_err(|source| Stage7PublicJournalError::Io {
                path: path.clone(),
                source,
            })?;
        Ok(Self {
            path,
            file,
            binding,
            next_capture_sequence,
            max_generation,
            repair_len: None,
        })
    }

    pub(super) fn append(
        &mut self,
        generation: u64,
        source: Stage7PublicSource,
        received_at_ms: u64,
        payload: String,
    ) -> Result<Stage7PublicRecord, Stage7PublicJournalError> {
        self.append_batch(vec![(generation, source, received_at_ms, payload)])?
            .pop()
            .ok_or(Stage7PublicJournalError::Record)
    }

    /// Commits one drained transport batch with one durability barrier. No record from the batch
    /// may reach normalization until this method succeeds, so batching reduces fsync pressure
    /// without weakening the raw-before-use boundary.
    pub(super) fn append_batch(
        &mut self,
        payloads: Vec<(u64, Stage7PublicSource, u64, String)>,
    ) -> Result<Vec<Stage7PublicRecord>, Stage7PublicJournalError> {
        self.repair_if_needed()?;
        if payloads.is_empty() {
            return Err(Stage7PublicJournalError::Record);
        }
        let mut records = Vec::with_capacity(payloads.len());
        let mut encoded = Vec::new();
        for (index, (generation, source, received_at_ms, payload)) in
            payloads.into_iter().enumerate()
        {
            let offset = u64::try_from(index).map_err(|_| Stage7PublicJournalError::Sequence)?;
            let capture_sequence = self
                .next_capture_sequence
                .checked_add(offset)
                .ok_or(Stage7PublicJournalError::Sequence)?;
            if generation == 0 || received_at_ms == 0 || payload.is_empty() {
                return Err(Stage7PublicJournalError::Record);
            }
            let record = Stage7PublicRecord {
                schema_version: SCHEMA_VERSION,
                capture_sequence,
                exchange: self.binding.exchange.clone(),
                symbol: self.binding.symbol.clone(),
                generation,
                source,
                received_at_ms,
                payload_sha256: digest(&payload),
                payload,
            };
            validate_record(&record, &self.binding, capture_sequence)?;
            serde_json::to_writer(&mut encoded, &record)
                .map_err(Stage7PublicJournalError::Encode)?;
            encoded.push(b'\n');
            records.push(record);
        }
        let original_len = self
            .file
            .metadata()
            .map_err(|source| self.io_error(source))?
            .len();
        let append_result = (|| {
            self.file
                .write_all(&encoded)
                .map_err(|source| self.io_error(source))?;
            self.file
                .write_all(b"\n")
                .map_err(|source| self.io_error(source))?;
            self.file
                .sync_data()
                .map_err(|source| self.io_error(source))
        })();
        if let Err(error) = append_result {
            // The record has not crossed the durable capture boundary, so it must never reach
            // normalization. Remember the exact pre-append length and roll back a partial tail;
            // if Windows is temporarily out of mapped-file resources, the next turn retries the
            // same repair before accepting any later frame.
            self.repair_len = Some(original_len);
            let _ = self.repair_if_needed();
            return Err(error);
        }
        let count = u64::try_from(records.len()).map_err(|_| Stage7PublicJournalError::Sequence)?;
        self.next_capture_sequence = self
            .next_capture_sequence
            .checked_add(count)
            .ok_or(Stage7PublicJournalError::Sequence)?;
        self.max_generation = self.max_generation.max(
            records
                .iter()
                .map(|record| record.generation)
                .max()
                .unwrap_or(0),
        );
        Ok(records)
    }

    pub(super) fn max_generation(&self) -> u64 {
        self.max_generation
    }

    pub(super) fn recover(&mut self) -> Result<Vec<Stage7PublicRecord>, Stage7PublicJournalError> {
        self.repair_if_needed()?;
        self.file
            .sync_data()
            .map_err(|source| Stage7PublicJournalError::Io {
                path: self.path.clone(),
                source,
            })?;
        let records = recover(&self.path, &self.binding)?;
        self.max_generation = records
            .iter()
            .map(|record| record.generation)
            .max()
            .unwrap_or(0);
        Ok(records)
    }

    fn repair_if_needed(&mut self) -> Result<(), Stage7PublicJournalError> {
        let Some(original_len) = self.repair_len else {
            return Ok(());
        };
        let repair = OpenOptions::new()
            .write(true)
            .open(&self.path)
            .map_err(|source| self.io_error(source))?;
        repair
            .set_len(original_len)
            .map_err(|source| self.io_error(source))?;
        repair.sync_data().map_err(|source| self.io_error(source))?;
        self.repair_len = None;
        Ok(())
    }

    fn io_error(&self, source: std::io::Error) -> Stage7PublicJournalError {
        Stage7PublicJournalError::Io {
            path: self.path.clone(),
            source,
        }
    }

    #[cfg(test)]
    pub(super) fn replay<T>(
        &mut self,
        mut normalize: impl FnMut(&Stage7PublicRecord) -> Result<T, Stage7PublicJournalError>,
    ) -> Result<Vec<T>, Stage7PublicJournalError> {
        self.recover()?.iter().map(&mut normalize).collect()
    }
}

fn recover(
    path: &Path,
    binding: &Stage7PublicBinding,
) -> Result<Vec<Stage7PublicRecord>, Stage7PublicJournalError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(Stage7PublicJournalError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        return Err(Stage7PublicJournalError::Truncated);
    }
    let mut expected_sequence = 1_u64;
    let mut records = Vec::new();
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let record = serde_json::from_slice::<Stage7PublicRecord>(line)
            .map_err(Stage7PublicJournalError::Decode)?;
        validate_record(&record, binding, expected_sequence)?;
        // `generation` belongs to a connection-local book bridge. A prior process could have
        // recorded an older local generation; capture sequence and the raw payload hash remain
        // the durable replay order. The next resident seeds above the maximum below and must
        // still obtain a new snapshot before its book becomes usable.
        records.push(record);
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or(Stage7PublicJournalError::Sequence)?;
    }
    Ok(records)
}

fn validate_record(
    record: &Stage7PublicRecord,
    binding: &Stage7PublicBinding,
    expected_sequence: u64,
) -> Result<(), Stage7PublicJournalError> {
    if record.schema_version != SCHEMA_VERSION
        || record.capture_sequence != expected_sequence
        || record.exchange != binding.exchange
        || record.symbol != binding.symbol
        || record.generation == 0
        || record.received_at_ms == 0
        || record.payload.is_empty()
        || record.payload_sha256 != digest(&record.payload)
    {
        return Err(Stage7PublicJournalError::Record);
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
pub enum Stage7PublicJournalError {
    #[error("stage-7 public journal binding is invalid")]
    Binding,
    #[error("stage-7 public journal record is invalid or does not match its binding")]
    Record,
    #[error("stage-7 public journal capture sequence overflowed or is discontinuous")]
    Sequence,
    #[error("stage-7 public journal has a truncated tail")]
    Truncated,
    #[error("stage-7 public journal serialization failed")]
    Encode(#[source] serde_json::Error),
    #[error("stage-7 public journal decoding failed")]
    Decode(#[source] serde_json::Error),
    #[error("stage-7 public journal I/O failed for {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
}

#[cfg(test)]
mod tests {
    use crate::domain::Symbol;

    use super::*;

    fn binding() -> Result<Stage7PublicBinding, Stage7PublicJournalError> {
        Ok(Stage7PublicBinding {
            exchange: "gate".to_owned(),
            symbol: "DOGE/USDT"
                .parse::<Symbol>()
                .map_err(|_| Stage7PublicJournalError::Binding)?,
        })
    }

    #[test]
    fn journal_fsyncs_raw_records_and_replays_them_in_capture_order()
    -> Result<(), Stage7PublicJournalError> {
        let temporary = tempfile::tempdir().map_err(|source| Stage7PublicJournalError::Io {
            path: PathBuf::new(),
            source,
        })?;
        let mut journal =
            Stage7PublicJournal::open(temporary.path().join("public.jsonl"), binding()?)?;
        let committed = journal.append_batch(vec![
            (
                1,
                Stage7PublicSource::RestSnapshot,
                1,
                r#"{"id":1}"#.to_owned(),
            ),
            (
                1,
                Stage7PublicSource::WebSocketDepth,
                2,
                r#"{"id":2}"#.to_owned(),
            ),
        ])?;
        assert_eq!(
            committed
                .iter()
                .map(|record| record.capture_sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        let replay = journal.replay(|record| Ok(record.payload.clone()))?;
        assert_eq!(replay, vec![r#"{"id":1}"#, r#"{"id":2}"#]);
        Ok(())
    }

    #[test]
    fn journal_rejects_a_cross_exchange_or_tampered_record() -> Result<(), Stage7PublicJournalError>
    {
        let temporary = tempfile::tempdir().map_err(|source| Stage7PublicJournalError::Io {
            path: PathBuf::new(),
            source,
        })?;
        let path = temporary.path().join("public.jsonl");
        let mut journal = Stage7PublicJournal::open(path.clone(), binding()?)?;
        journal.append(
            1,
            Stage7PublicSource::RestSnapshot,
            1,
            r#"{"id":1}"#.to_owned(),
        )?;
        drop(journal);
        let mut content =
            fs::read_to_string(&path).map_err(|source| Stage7PublicJournalError::Io {
                path: path.clone(),
                source,
            })?;
        content = content.replacen("gate", "bitget", 1);
        fs::write(&path, content).map_err(|source| Stage7PublicJournalError::Io {
            path: path.clone(),
            source,
        })?;
        assert!(matches!(
            Stage7PublicJournal::open(path, binding()?),
            Err(Stage7PublicJournalError::Record)
        ));
        Ok(())
    }

    #[test]
    fn journal_preserves_capture_order_across_connection_local_generation_reset()
    -> Result<(), Stage7PublicJournalError> {
        let temporary = tempfile::tempdir().map_err(|source| Stage7PublicJournalError::Io {
            path: PathBuf::new(),
            source,
        })?;
        let path = temporary.path().join("public.jsonl");
        let mut journal = Stage7PublicJournal::open(path.clone(), binding()?)?;
        journal.append(
            4,
            Stage7PublicSource::WebSocketDepth,
            1,
            r#"{"id":1}"#.to_owned(),
        )?;
        journal.append(
            1,
            Stage7PublicSource::WebSocketDepth,
            2,
            r#"{"id":2}"#.to_owned(),
        )?;
        drop(journal);
        let mut recovered = Stage7PublicJournal::open(path, binding()?)?;
        assert_eq!(
            recovered.replay(|record| Ok((record.capture_sequence, record.generation)))?,
            vec![(1, 4), (2, 1)]
        );
        Ok(())
    }

    #[test]
    fn failed_append_tail_is_rolled_back_before_any_later_replay()
    -> Result<(), Stage7PublicJournalError> {
        let temporary = tempfile::tempdir().map_err(|source| Stage7PublicJournalError::Io {
            path: PathBuf::new(),
            source,
        })?;
        let path = temporary.path().join("public.jsonl");
        let mut journal = Stage7PublicJournal::open(path, binding()?)?;
        journal.append(
            1,
            Stage7PublicSource::RestSnapshot,
            1,
            r#"{"id":1}"#.to_owned(),
        )?;
        let durable_len = journal
            .file
            .metadata()
            .map_err(|source| journal.io_error(source))?
            .len();
        journal
            .file
            .write_all(b"partial-undurable-record")
            .map_err(|source| journal.io_error(source))?;
        journal.repair_len = Some(durable_len);
        journal.repair_if_needed()?;
        assert_eq!(journal.recover()?.len(), 1);
        Ok(())
    }
}
