use std::collections::BTreeMap;

use rust_decimal::Decimal;

use crate::domain::{MarketDelta, MarketLevel, MarketSnapshot, Price, Symbol};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OrderBook {
    symbol: Option<Symbol>,
    generation: u64,
    sequence: u64,
    synchronized: bool,
    bridged: bool,
    bids: BTreeMap<Price, Decimal>,
    asks: BTreeMap<Price, Decimal>,
}

impl OrderBook {
    pub fn apply_snapshot(&mut self, snapshot: MarketSnapshot) {
        self.symbol = Some(snapshot.symbol);
        self.generation = snapshot.generation;
        self.sequence = snapshot.sequence;
        self.synchronized = true;
        self.bridged = false;
        self.bids.clear();
        self.asks.clear();
        apply_levels(&mut self.bids, snapshot.bids);
        apply_levels(&mut self.asks, snapshot.asks);
    }

    pub fn apply_delta(&mut self, delta: MarketDelta) -> Result<(), BookError> {
        self.apply_delta_if_fresh(delta).map(|_| ())
    }

    /// Applies one delta, returning false when it is an already-covered queued update after a
    /// snapshot. The first non-stale update bridges either by covering the next snapshot update
    /// or by naming the snapshot as its `pu`; all later updates require strict `pu` continuity.
    /// Binance Futures may aggregate non-contiguous update-id ranges inside adjacent events, so
    /// `U == previous_u + 1` is not a valid additional requirement when `pu` already matches.
    pub fn apply_delta_if_fresh(&mut self, delta: MarketDelta) -> Result<bool, BookError> {
        if !self.synchronized
            || self.symbol.as_ref() != Some(&delta.symbol)
            || delta.generation != self.generation
        {
            self.reset();
            return Err(BookError::Desynchronized);
        }
        if !self.bridged && delta.sequence <= self.sequence {
            return Ok(false);
        }
        let expected = self
            .sequence
            .checked_add(1)
            .ok_or(BookError::Desynchronized)?;
        let first = if !self.bridged {
            (delta.first_sequence <= expected && expected <= delta.sequence)
                || delta.previous_sequence == Some(self.sequence)
        } else {
            delta.previous_sequence == Some(self.sequence)
        };
        if !first {
            self.reset();
            return Err(BookError::Desynchronized);
        }
        apply_levels(&mut self.bids, delta.bids);
        apply_levels(&mut self.asks, delta.asks);
        self.sequence = delta.sequence;
        self.bridged = true;
        Ok(true)
    }

    pub fn synchronized(&self) -> bool {
        self.synchronized
    }
    pub fn bridged(&self) -> bool {
        self.synchronized && self.bridged
    }
    pub fn symbol(&self) -> Option<&Symbol> {
        self.synchronized.then_some(self.symbol.as_ref()).flatten()
    }
    pub fn generation(&self) -> Option<u64> {
        self.synchronized.then_some(self.generation)
    }
    pub fn sequence(&self) -> Option<u64> {
        self.synchronized.then_some(self.sequence)
    }
    pub fn bids(&self) -> Vec<MarketLevel> {
        self.bids
            .iter()
            .rev()
            .map(|(price, quantity)| MarketLevel {
                price: *price,
                quantity: *quantity,
            })
            .collect()
    }
    pub fn asks(&self) -> Vec<MarketLevel> {
        self.asks
            .iter()
            .map(|(price, quantity)| MarketLevel {
                price: *price,
                quantity: *quantity,
            })
            .collect()
    }

    fn reset(&mut self) {
        self.symbol = None;
        self.generation = 0;
        self.sequence = 0;
        self.synchronized = false;
        self.bridged = false;
        self.bids.clear();
        self.asks.clear();
    }
}

fn apply_levels(book: &mut BTreeMap<Price, Decimal>, levels: Vec<MarketLevel>) {
    for level in levels {
        if level.quantity.is_zero() {
            book.remove(&level.price);
        } else {
            book.insert(level.price, level.quantity);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BookError {
    #[error("market book lost snapshot/delta synchronization")]
    Desynchronized,
}
