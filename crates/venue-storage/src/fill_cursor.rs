use std::{cmp::Ordering, path::PathBuf};

use serde::{Deserialize, Serialize};
use venue_domain::Symbol;

use crate::{ProjectionStore, StorageError};

/// Durable native-trade watermark for one exchange/account/symbol binding.
/// The generation is advanced only after the corresponding facts are durable.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FillCursor {
    pub schema_version: u16,
    pub exchange: String,
    pub account: String,
    pub symbol: Symbol,
    pub generation: u64,
    #[serde(default = "initial_connection_epoch")]
    pub connection_epoch: u64,
    pub observed_through_ms: u64,
    pub last_trade_id: Option<u64>,
    pub last_event_time_ms: Option<u64>,
}

impl FillCursor {
    pub fn validate(&self) -> Result<(), FillCursorError> {
        if self.schema_version == 0
            || self.exchange.trim().is_empty()
            || self.account.trim().is_empty()
            || self.generation == 0
            || self.connection_epoch == 0
            || self.observed_through_ms == 0
            || self.last_trade_id.is_some() != self.last_event_time_ms.is_some()
            || self
                .last_event_time_ms
                .is_some_and(|value| value > self.observed_through_ms)
        {
            return Err(FillCursorError::Invalid);
        }
        Ok(())
    }

    pub fn same_binding(&self, other: &Self) -> bool {
        self.exchange == other.exchange
            && self.account == other.account
            && self.symbol == other.symbol
    }

    /// A successor may advance the time watermark without a trade, or advance the native trade
    /// watermark. It may never remove an observed native id/time pair.
    pub fn position_ordering(&self, next: &Self) -> Result<Ordering, FillCursorError> {
        if !self.same_binding(next) {
            return Err(FillCursorError::Binding);
        }
        self.validate()?;
        next.validate()?;
        if next.observed_through_ms < self.observed_through_ms
            || next.connection_epoch < self.connection_epoch
            || next.last_event_time_ms < self.last_event_time_ms
            || matches!((self.last_trade_id, next.last_trade_id), (Some(_), None))
            || matches!((self.last_trade_id, next.last_trade_id), (Some(a), Some(b)) if b < a)
        {
            return Ok(Ordering::Less);
        }
        if next.observed_through_ms == self.observed_through_ms
            && next.connection_epoch == self.connection_epoch
            && next.last_trade_id == self.last_trade_id
            && next.last_event_time_ms == self.last_event_time_ms
        {
            return Ok(Ordering::Equal);
        }
        Ok(Ordering::Greater)
    }
}

const fn initial_connection_epoch() -> u64 {
    1
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FillCursorCommit {
    Committed,
    AlreadyCommitted,
}

#[derive(Debug)]
pub struct FillCursorStore {
    store: ProjectionStore,
}

impl FillCursorStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            store: ProjectionStore::new(path),
        }
    }

    pub fn load(&self) -> Result<Option<FillCursor>, FillCursorError> {
        let cursor: Option<FillCursor> = self.store.load()?;
        if let Some(cursor) = &cursor {
            cursor.validate()?;
        }
        Ok(cursor)
    }

    /// Atomically installs the first durable watermark or commits the exact next generation.
    /// `expected` is compared as a complete record, so an old generation or old watermark cannot
    /// overwrite a newer recovery result.
    pub fn compare_and_swap(
        &self,
        expected: Option<&FillCursor>,
        next: &FillCursor,
    ) -> Result<FillCursorCommit, FillCursorError> {
        let current = self.load()?;
        if current.as_ref() == Some(next) {
            return Ok(FillCursorCommit::AlreadyCommitted);
        }
        Self::require_successor(current.as_ref(), expected, next)?;
        self.store.save(next)?;
        Ok(FillCursorCommit::Committed)
    }

    /// Checks a recovery attempt before it appends facts. This lookup is only an optimization:
    /// `compare_and_swap` repeats the same check after journal durability is established.
    pub fn already_committed(
        &self,
        expected: Option<&FillCursor>,
        next: &FillCursor,
    ) -> Result<bool, FillCursorError> {
        let current = self.load()?;
        if current.as_ref() == Some(next) {
            return Ok(true);
        }
        Self::require_successor(current.as_ref(), expected, next)?;
        Ok(false)
    }

    fn require_successor(
        current: Option<&FillCursor>,
        expected: Option<&FillCursor>,
        next: &FillCursor,
    ) -> Result<(), FillCursorError> {
        next.validate()?;
        if current != expected {
            return Err(FillCursorError::CompareAndSwap);
        }
        if let Some(previous) = expected {
            previous.validate()?;
            if !previous.same_binding(next) {
                return Err(FillCursorError::Binding);
            }
            let expected_generation = previous
                .generation
                .checked_add(1)
                .ok_or(FillCursorError::Generation)?;
            if next.generation != expected_generation {
                return Err(FillCursorError::Generation);
            }
            if previous.position_ordering(next)? != Ordering::Greater {
                return Err(FillCursorError::Regression);
            }
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FillCursorError {
    #[error("fill cursor identity or watermark is invalid")]
    Invalid,
    #[error("fill cursor binding differs from its predecessor")]
    Binding,
    #[error("fill cursor compare-and-swap expected a different current value")]
    CompareAndSwap,
    #[error("fill cursor generation is not the direct successor")]
    Generation,
    #[error("fill cursor watermark regresses or makes no progress")]
    Regression,
    #[error("fill cursor storage failed: {0}")]
    Storage(#[from] StorageError),
}
