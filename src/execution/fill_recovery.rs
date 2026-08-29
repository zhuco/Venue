use std::{cmp::Ordering, collections::BTreeMap};

use crate::{
    domain::{DomainEvent, Fill, Symbol},
    exchange::{
        binance::{RecentFillsCursor, RecentFillsReadback},
        binance_private,
    },
    storage::{FillCursor, FillCursorError, FillCursorStore, Journal},
};

use super::{ReadbackBatch, Reconciler, ReconciliationError, ReconciliationReport};

/// A bounded PAPI readback plus the durable binding that owns its watermark.
pub struct FillRecoveryBatch<'a> {
    pub exchange: &'a str,
    pub account: &'a str,
    pub symbol: &'a Symbol,
    pub readback: RecentFillsReadback,
    pub received_at_ms: u64,
    pub native_epoch: u64,
    pub hub_bootstrap_generation: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FillRecoveryReport {
    pub reconciliation: ReconciliationReport,
    pub cursor_already_committed: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FillEpochGate {
    native_epoch: Option<u64>,
    durable_epoch: u64,
    hub_bootstrap_generation: Option<u64>,
    complete: bool,
}

impl FillEpochGate {
    pub const fn with_durable_floor(durable_epoch: u64) -> Self {
        Self {
            native_epoch: None,
            durable_epoch,
            hub_bootstrap_generation: None,
            complete: false,
        }
    }

    pub fn observe_native_epoch(&mut self, native_epoch: u64) -> Option<u64> {
        if native_epoch == 0 {
            self.fence();
            return None;
        }
        if self.native_epoch != Some(native_epoch) {
            self.native_epoch = Some(native_epoch);
            self.durable_epoch = self.durable_epoch.saturating_add(1).max(1);
            self.hub_bootstrap_generation = None;
            self.complete = false;
        }
        Some(self.durable_epoch)
    }

    pub fn mark_complete(
        &mut self,
        native_epoch: u64,
        durable_epoch: u64,
        hub_bootstrap_generation: u64,
    ) -> Result<(), FillRecoveryError> {
        if self.native_epoch != Some(native_epoch)
            || self.durable_epoch != durable_epoch
            || hub_bootstrap_generation == 0
        {
            return Err(FillRecoveryError::Epoch);
        }
        self.hub_bootstrap_generation = Some(hub_bootstrap_generation);
        self.complete = true;
        Ok(())
    }

    pub fn allows_ready(&self, native_epoch: u64) -> bool {
        self.complete && self.native_epoch == Some(native_epoch)
    }

    pub const fn hub_bootstrap_generation(&self) -> Option<u64> {
        if self.complete {
            self.hub_bootstrap_generation
        } else {
            None
        }
    }

    pub fn fence(&mut self) {
        self.native_epoch = None;
        self.hub_bootstrap_generation = None;
        self.complete = false;
    }
}

#[derive(Debug, Default)]
pub struct FillRecoveryCoordinator {
    reconciler: Reconciler,
    epoch_gate: FillEpochGate,
}

impl FillRecoveryCoordinator {
    /// Rebuilds the stable native-fill identity index from the durable fact journal.
    pub fn recover(
        facts_journal: &Journal,
        cursor_store: &FillCursorStore,
    ) -> Result<Self, FillRecoveryError> {
        let durable_floor = cursor_store
            .load()?
            .map_or(0, |cursor| cursor.connection_epoch);
        Ok(Self {
            reconciler: Reconciler::recover(facts_journal)?,
            epoch_gate: FillEpochGate::with_durable_floor(durable_floor),
        })
    }

    /// Parses and validates a bounded PAPI result, journals new fill facts, then advances the
    /// durable cursor. A failed CAS leaves the already durable facts in place for a retry.
    pub fn accept_batch(
        &mut self,
        facts_journal: &mut Journal,
        cursor_store: &FillCursorStore,
        batch: FillRecoveryBatch<'_>,
    ) -> Result<FillRecoveryReport, FillRecoveryError> {
        if batch.exchange.trim().is_empty()
            || batch.account.trim().is_empty()
            || batch.received_at_ms == 0
            || batch.native_epoch == 0
            || batch.hub_bootstrap_generation == 0
        {
            return Err(FillRecoveryError::Scope);
        }
        let current = cursor_store
            .load()?
            .ok_or(FillRecoveryError::MissingCursor)?;
        if current.exchange != batch.exchange
            || current.account != batch.account
            || current.symbol != *batch.symbol
        {
            return Err(FillRecoveryError::Scope);
        }

        let durable_epoch = self
            .epoch_gate
            .observe_native_epoch(batch.native_epoch)
            .ok_or(FillRecoveryError::Epoch)?;
        if durable_epoch < current.connection_epoch {
            self.epoch_gate.fence();
            return Err(FillRecoveryError::Epoch);
        }

        let next = cursor_from_readback(&current, &batch.readback.cursor, durable_epoch)?;
        let ordering = current.position_ordering(&next)?;
        if ordering == Ordering::Less {
            return Err(FillRecoveryError::CursorRegression);
        }
        if ordering == Ordering::Greater && batch.readback.pages == 0 {
            return Err(FillRecoveryError::InvalidReadback);
        }

        let fills = binance_private::parse_fills(&batch.readback.payload, batch.symbol)
            .map_err(FillRecoveryError::Parse)?;
        if next.last_trade_id != current.last_trade_id
            && !fills.iter().any(|fill| {
                fill.fill_id
                    .parse::<u64>()
                    .ok()
                    .zip(next.last_trade_id)
                    .is_some_and(|(fill_id, cursor_id)| fill_id == cursor_id)
            })
        {
            return Err(FillRecoveryError::CursorGap);
        }
        let (new_fills, duplicate_count) = self.select_idempotent_fills(&fills)?;
        validate_fill_watermark(&current, &next, &new_fills)?;

        if ordering == Ordering::Equal {
            if !new_fills.is_empty() {
                return Err(FillRecoveryError::CursorNoProgress);
            }
            self.epoch_gate.mark_complete(
                batch.native_epoch,
                durable_epoch,
                batch.hub_bootstrap_generation,
            )?;
            return Ok(FillRecoveryReport {
                reconciliation: ReconciliationReport {
                    duplicate: duplicate_count,
                    ..ReconciliationReport::default()
                },
                cursor_already_committed: true,
            });
        }

        let mut reconciliation = self.reconciler.accept_readback(
            facts_journal,
            ReadbackBatch {
                generation: next.generation,
                received_at_ms: batch.received_at_ms,
                balances: &[],
                positions: &[],
                orders: &[],
                fills: &new_fills,
            },
        )?;
        reconciliation.duplicate = reconciliation.duplicate.saturating_add(duplicate_count);
        let cursor_already_committed = cursor_store.compare_and_swap(Some(&current), &next)?
            == crate::storage::FillCursorCommit::AlreadyCommitted;
        self.epoch_gate.mark_complete(
            batch.native_epoch,
            durable_epoch,
            batch.hub_bootstrap_generation,
        )?;
        Ok(FillRecoveryReport {
            reconciliation,
            cursor_already_committed,
        })
    }

    pub fn reconciler(&self) -> &Reconciler {
        &self.reconciler
    }

    pub const fn epoch_gate(&self) -> &FillEpochGate {
        &self.epoch_gate
    }

    /// Appends non-fill account facts through the same recovered identity index used by fills.
    pub(crate) fn accept_account_readback(
        &mut self,
        facts_journal: &mut Journal,
        batch: ReadbackBatch<'_>,
    ) -> Result<ReconciliationReport, FillRecoveryError> {
        self.reconciler
            .accept_readback(facts_journal, batch)
            .map_err(Into::into)
    }

    /// Revokes readiness immediately when the owning private session loses custody.
    pub(crate) fn fence_epoch(&mut self) {
        self.epoch_gate.fence();
    }

    fn select_idempotent_fills(
        &self,
        fills: &[Fill],
    ) -> Result<(Vec<Fill>, u32), FillRecoveryError> {
        let mut known = BTreeMap::<(String, String), Vec<u8>>::new();
        for record in self.reconciler.facts().records() {
            if let DomainEvent::Fill(fill) = &record.event {
                known.insert(
                    (fill.symbol.to_string(), fill.fill_id.clone()),
                    serde_json::to_vec(fill)?,
                );
            }
        }

        let mut selected = Vec::new();
        let mut duplicates = 0_u32;
        for fill in fills {
            let encoded = serde_json::to_vec(fill)?;
            let key = (fill.symbol.to_string(), fill.fill_id.clone());
            match known.get(&key) {
                Some(previous) if previous == &encoded => duplicates = duplicates.saturating_add(1),
                Some(_) => return Err(FillRecoveryError::ConflictingFill(fill.fill_id.clone())),
                None => {
                    known.insert(key, encoded);
                    selected.push(fill.clone());
                }
            }
        }
        Ok((selected, duplicates))
    }
}

fn cursor_from_readback(
    current: &FillCursor,
    cursor: &RecentFillsCursor,
    connection_epoch: u64,
) -> Result<FillCursor, FillRecoveryError> {
    if cursor.observed_through_ms == 0
        || cursor.last_trade_id.is_some() != cursor.last_event_time_ms.is_some()
        || cursor
            .last_event_time_ms
            .is_some_and(|time| time > cursor.observed_through_ms)
    {
        return Err(FillRecoveryError::InvalidReadback);
    }
    Ok(FillCursor {
        schema_version: current.schema_version,
        exchange: current.exchange.clone(),
        account: current.account.clone(),
        symbol: current.symbol.clone(),
        generation: current
            .generation
            .checked_add(1)
            .ok_or(FillCursorError::Generation)?,
        connection_epoch,
        observed_through_ms: cursor.observed_through_ms,
        last_trade_id: cursor.last_trade_id,
        last_event_time_ms: cursor.last_event_time_ms,
    })
}

fn validate_fill_watermark(
    current: &FillCursor,
    next: &FillCursor,
    fills: &[Fill],
) -> Result<(), FillRecoveryError> {
    for fill in fills {
        let trade_id = fill
            .fill_id
            .parse::<u64>()
            .map_err(|_| FillRecoveryError::InvalidReadback)?;
        let Some(last_trade_id) = next.last_trade_id else {
            return Err(FillRecoveryError::InvalidReadback);
        };
        if trade_id > last_trade_id {
            return Err(FillRecoveryError::CursorGap);
        }
        if current.last_trade_id.is_some_and(|last| trade_id <= last) {
            return Err(FillRecoveryError::CursorGap);
        }
        if fill
            .exchange_time_ms
            .is_some_and(|time| time > next.observed_through_ms)
        {
            return Err(FillRecoveryError::InvalidReadback);
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum FillRecoveryError {
    #[error("fill cursor binding or receive time is invalid")]
    Scope,
    #[error("durable fill cursor is missing")]
    MissingCursor,
    #[error("fill cursor failed: {0}")]
    Cursor(#[from] FillCursorError),
    #[error("fill payload could not be normalized: {0}")]
    Parse(binance_private::PrivateParseError),
    #[error("authoritative fill fact acceptance failed: {0}")]
    Reconciliation(#[from] ReconciliationError),
    #[error("fill payload cursor is invalid or does not cover its fills")]
    InvalidReadback,
    #[error("fill payload cursor regresses the durable watermark")]
    CursorRegression,
    #[error("fill payload has no cursor progress")]
    CursorNoProgress,
    #[error("fill payload contains an old unseen native fill")]
    CursorGap,
    #[error("fill recovery socket epoch is stale, incomplete, or unbound")]
    Epoch,
    #[error("the same native fill id contains conflicting immutable facts: {0}")]
    ConflictingFill(String),
    #[error("fill fact could not be serialized: {0}")]
    Encode(serde_json::Error),
}

impl From<serde_json::Error> for FillRecoveryError {
    fn from(error: serde_json::Error) -> Self {
        Self::Encode(error)
    }
}
