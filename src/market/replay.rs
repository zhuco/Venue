use sha2::{Digest, Sha256};

use crate::{
    domain::MarketEvent,
    exchange::binance,
    market::{BookError, OrderBook, RawMarketRecord},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayResult {
    pub normalized_hash: String,
    pub final_sequence: Option<u64>,
}

pub fn replay_binance(
    records: &[RawMarketRecord],
    expected_native_symbol: &str,
) -> Result<ReplayResult, ReplayError> {
    let mut book = OrderBook::default();
    let mut material = Vec::new();
    let mut previous = 0;
    for record in records {
        if record.capture_sequence != previous + 1 {
            return Err(ReplayError::Sequence(record.capture_sequence));
        }
        previous = record.capture_sequence;
        let event = binance::normalize(record, expected_native_symbol).map_err(|source| {
            ReplayError::Normalize {
                sequence: record.capture_sequence,
                source,
            }
        })?;
        material.extend(serde_json::to_vec(&event).map_err(ReplayError::Encode)?);
        match event {
            MarketEvent::Snapshot(snapshot) => book.apply_snapshot(snapshot),
            MarketEvent::Delta(delta) => {
                book.apply_delta(delta)
                    .map_err(|source| ReplayError::Book {
                        sequence: record.capture_sequence,
                        source,
                    })?
            }
            MarketEvent::Trade(_)
            | MarketEvent::Bar(_)
            | MarketEvent::Ticker(_)
            | MarketEvent::MarkFunding(_) => {}
        }
    }
    Ok(ReplayResult {
        normalized_hash: hex(&Sha256::digest(material)),
        final_sequence: book.sequence(),
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error("raw replay sequence is invalid at capture {0}")]
    Sequence(u64),
    #[error("cannot normalize capture {sequence}: {source}")]
    Normalize {
        sequence: u64,
        source: binance::BinanceError,
    },
    #[error("book rejected capture {sequence}: {source}")]
    Book { sequence: u64, source: BookError },
    #[error("cannot encode normalized market event: {0}")]
    Encode(serde_json::Error),
}
