use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;
use serde::{Deserialize, Serialize};

use crate::{
    exchange::binance::{
        BinanceError, PublicError, PublicRest, parse_usdt_perpetual_market_rank_samples,
    },
    execution::sha256_hex,
    market::{MarketScannerError, MarketScannerParams, MarketSelection, select_liquid_movers},
};

const SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BinanceMarketScanRecord {
    pub schema_version: u16,
    pub scan_sequence: u64,
    pub captured_at_ms: u64,
    pub input_count: usize,
    pub previous_sha256: String,
    pub selection: MarketSelection,
    pub content_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceMarketScanReport {
    pub record: BinanceMarketScanRecord,
    pub journal_path: PathBuf,
}

/// Fetches one complete Binance USDT-perpetual universe and fsyncs the full deterministic
/// selection. This boundary is public/read-only and owns neither bindings nor mutations.
pub fn scan_binance_usdt_perpetuals(
    artifacts_root: &Path,
) -> Result<BinanceMarketScanReport, BinanceMarketScanError> {
    if !artifacts_root.is_absolute() {
        return Err(BinanceMarketScanError::ArtifactsRoot);
    }
    fs::create_dir_all(artifacts_root).map_err(|source| BinanceMarketScanError::Io {
        path: artifacts_root.to_path_buf(),
        source,
    })?;
    let journal_path = artifacts_root.join("binance_market_selections.jsonl");
    let lock_path = artifacts_root.join("binance_market_scan.lock");
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|source| BinanceMarketScanError::Io {
            path: lock_path.clone(),
            source,
        })?;
    lock.try_lock_exclusive()
        .map_err(|_| BinanceMarketScanError::Locked)?;
    let recovered = recover(&journal_path)?;
    let scan_sequence = u64::try_from(recovered.len())
        .ok()
        .and_then(|value| value.checked_add(1))
        .ok_or(BinanceMarketScanError::Sequence)?;
    let captured_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BinanceMarketScanError::Clock)?
        .as_millis()
        .try_into()
        .map_err(|_| BinanceMarketScanError::Clock)?;
    let client = PublicRest::production()?;
    let exchange_info = client.exchange_info()?;
    let tickers = client.ticker_24hr()?;
    let samples = parse_usdt_perpetual_market_rank_samples(
        &exchange_info,
        &tickers,
        captured_at_ms,
        scan_sequence,
    )?;
    let input_count = samples.len();
    let selection = select_liquid_movers(&MarketScannerParams::phase8(), samples)?;
    let mut record = BinanceMarketScanRecord {
        schema_version: SCHEMA_VERSION,
        scan_sequence,
        captured_at_ms,
        input_count,
        previous_sha256: recovered
            .last()
            .map(|record| record.content_sha256.clone())
            .unwrap_or_else(|| "0".repeat(64)),
        selection,
        content_sha256: String::new(),
    };
    record.content_sha256 = record_digest(&record)?;
    append(&journal_path, &record)?;
    Ok(BinanceMarketScanReport {
        record,
        journal_path,
    })
}

fn recover(path: &Path) -> Result<Vec<BinanceMarketScanRecord>, BinanceMarketScanError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(BinanceMarketScanError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    let mut records = Vec::new();
    for line in BufReader::new(file).lines() {
        let line = line.map_err(|source| BinanceMarketScanError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        if line.trim().is_empty() {
            return Err(BinanceMarketScanError::Journal);
        }
        let record: BinanceMarketScanRecord =
            serde_json::from_str(&line).map_err(|_| BinanceMarketScanError::Journal)?;
        let expected_sequence = u64::try_from(records.len())
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or(BinanceMarketScanError::Sequence)?;
        let previous = records
            .last()
            .map(|record: &BinanceMarketScanRecord| record.content_sha256.clone())
            .unwrap_or_else(|| "0".repeat(64));
        if record.schema_version != SCHEMA_VERSION
            || record.scan_sequence != expected_sequence
            || record.previous_sha256 != previous
            || record.content_sha256 != record_digest(&record)?
            || record.input_count
                != record.selection.selected.len() + record.selection.rejected.len()
        {
            return Err(BinanceMarketScanError::Journal);
        }
        records.push(record);
    }
    Ok(records)
}

fn record_digest(record: &BinanceMarketScanRecord) -> Result<String, BinanceMarketScanError> {
    let mut canonical = record.clone();
    canonical.content_sha256.clear();
    let bytes = serde_json::to_vec(&canonical).map_err(|_| BinanceMarketScanError::Journal)?;
    Ok(sha256_hex(&bytes))
}

fn append(path: &Path, record: &BinanceMarketScanRecord) -> Result<(), BinanceMarketScanError> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|source| BinanceMarketScanError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    let encoded = serde_json::to_vec(record).map_err(|_| BinanceMarketScanError::Journal)?;
    file.write_all(&encoded)
        .and_then(|_| file.write_all(b"\n"))
        .and_then(|_| file.sync_all())
        .map_err(|source| BinanceMarketScanError::Io {
            path: path.to_path_buf(),
            source,
        })
}

#[derive(Debug, thiserror::Error)]
pub enum BinanceMarketScanError {
    #[error("market scan artifacts root must be absolute")]
    ArtifactsRoot,
    #[error("another market scan writer is active")]
    Locked,
    #[error("market scan clock is invalid")]
    Clock,
    #[error("market scan sequence overflow")]
    Sequence,
    #[error("market scan journal is corrupt")]
    Journal,
    #[error("market scan I/O failed for {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Public(#[from] PublicError),
    #[error(transparent)]
    Binance(#[from] BinanceError),
    #[error(transparent)]
    Scanner(#[from] MarketScannerError),
}

#[cfg(test)]
mod tests {
    use super::{BinanceMarketScanRecord, record_digest, recover};
    use crate::market::MarketSelection;

    #[test]
    fn digest_excludes_only_its_own_field() -> Result<(), Box<dyn std::error::Error>> {
        let record = BinanceMarketScanRecord {
            schema_version: 1,
            scan_sequence: 1,
            captured_at_ms: 1,
            input_count: 0,
            previous_sha256: "0".repeat(64),
            selection: MarketSelection {
                algorithm_version: "v1".to_owned(),
                selection_watermark_ms: 0,
                selected: Vec::new(),
                rejected: Vec::new(),
            },
            content_sha256: "ignored".to_owned(),
        };
        assert_eq!(record_digest(&record)?, record_digest(&record)?);
        Ok(())
    }

    #[test]
    fn missing_journal_recovers_empty() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        assert!(recover(&directory.path().join("missing.jsonl"))?.is_empty());
        Ok(())
    }
}
