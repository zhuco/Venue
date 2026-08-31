use std::collections::BTreeMap;

use rust_decimal::Decimal;
use venue_domain::{MarketDelta, MarketLevel, MarketSnapshot, Price, Symbol};

use crate::PublicBook;

/// Deterministic normalized order book assembled from one snapshot and its delta stream.
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
        if !valid_delta(&delta)
            || !self.synchronized
            || self.symbol.as_ref() != Some(&delta.symbol)
            || delta.generation != self.generation
        {
            self.reset();
            return Err(BookError::Desynchronized);
        }
        if !self.bridged && delta.sequence <= self.sequence {
            return Ok(false);
        }
        // Once bridged, `pu` proves only the predecessor identity. It cannot make a duplicate
        // or regressing update sequence safe to apply.
        if delta.sequence <= self.sequence {
            self.reset();
            return Err(BookError::Desynchronized);
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

impl PublicBook for OrderBook {
    fn synchronized(&self) -> bool {
        self.synchronized()
    }

    fn bridged(&self) -> bool {
        self.bridged()
    }

    fn symbol(&self) -> Option<&Symbol> {
        self.symbol()
    }

    fn generation(&self) -> Option<u64> {
        self.generation()
    }

    fn sequence(&self) -> Option<u64> {
        self.sequence()
    }

    fn bids(&self) -> Vec<MarketLevel> {
        self.bids()
    }

    fn asks(&self) -> Vec<MarketLevel> {
        self.asks()
    }
}

fn valid_delta(delta: &MarketDelta) -> bool {
    delta.first_sequence != 0 && delta.sequence >= delta.first_sequence
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

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use venue_domain::{MarketDelta, MarketLevel, MarketSnapshot, Price, Symbol};

    use super::OrderBook;

    fn snapshot() -> Result<MarketSnapshot, Box<dyn std::error::Error>> {
        Ok(MarketSnapshot {
            symbol: "BTC/USDT".parse()?,
            generation: 7,
            sequence: 10,
            exchange_time_ms: None,
            bids: vec![MarketLevel {
                price: Price::new(Decimal::from(100))?,
                quantity: Decimal::ONE,
            }],
            asks: vec![MarketLevel {
                price: Price::new(Decimal::from(101))?,
                quantity: Decimal::ONE,
            }],
        })
    }

    fn delta(
        symbol: Symbol,
        first_sequence: u64,
        previous_sequence: Option<u64>,
        sequence: u64,
    ) -> MarketDelta {
        MarketDelta {
            symbol,
            generation: 7,
            first_sequence,
            previous_sequence,
            sequence,
            exchange_time_ms: None,
            bids: vec![],
            asks: vec![],
        }
    }

    #[test]
    fn post_bridge_delta_requires_strictly_increasing_sequence()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut book = OrderBook::default();
        let snapshot = snapshot()?;
        let symbol = snapshot.symbol.clone();
        book.apply_snapshot(snapshot);
        assert!(book.apply_delta_if_fresh(delta(symbol.clone(), 11, Some(10), 11))?);

        // `pu` names the current sequence, but `u` must still advance after the bridge.
        assert!(
            book.apply_delta_if_fresh(delta(symbol, 11, Some(11), 11))
                .is_err()
        );
        assert!(!book.synchronized());
        Ok(())
    }

    #[test]
    fn malformed_delta_fails_closed_before_stale_filtering()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut book = OrderBook::default();
        let snapshot = snapshot()?;
        let symbol = snapshot.symbol.clone();
        book.apply_snapshot(snapshot);

        // A stale event cannot be silently dropped when its normalized sequence range is invalid.
        assert!(
            book.apply_delta_if_fresh(delta(symbol, 12, Some(10), 11))
                .is_err()
        );
        assert!(!book.synchronized());
        Ok(())
    }
}
