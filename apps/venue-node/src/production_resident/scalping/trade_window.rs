use std::collections::{BTreeMap, VecDeque};

use venue_domain::{PublicTrade, PublicTradeId, PublicTradeOrdering};

use crate::NodeError;

const MAX_IDENTITIES: usize = 4_096;

/// Ordering for the locally observed, losslessly delivered session, not an invented exchange
/// sequence. This window is intentionally rebuilt after restart or a new book generation.
#[derive(Default)]
pub(super) struct ObservedTradeWindow {
    generation: Option<u64>,
    sequence: u64,
    last_trade_time: Option<u64>,
    retired_through: Option<u64>,
    identities: BTreeMap<PublicTradeId, PublicTrade>,
    arrival_order: VecDeque<PublicTradeId>,
}

impl ObservedTradeWindow {
    pub(super) fn accept(
        &mut self,
        mut trade: PublicTrade,
    ) -> Result<Option<PublicTrade>, NodeError> {
        if !trade.is_valid() || trade.ordering != PublicTradeOrdering::Unsequenced {
            return Err(NodeError::ResidentRuntime);
        }
        match self.generation {
            Some(current) if trade.generation < current => return Err(NodeError::ResidentRuntime),
            Some(current) if trade.generation == current => {}
            _ => {
                *self = Self {
                    generation: Some(trade.generation),
                    ..Self::default()
                };
            }
        }
        if let Some(previous) = self.identities.get(&trade.aggregate_trade_id) {
            return if same_execution(previous, &trade) {
                Ok(None)
            } else {
                Err(NodeError::ResidentRuntime)
            };
        }
        if self
            .last_trade_time
            .is_some_and(|time| trade.transaction_time_ms < time)
            || self
                .retired_through
                .is_some_and(|time| trade.transaction_time_ms <= time)
        {
            // An identity outside the bounded replay window cannot be proved new. Do not count
            // a late replay as another distinct trade merely because its ID was evicted.
            return Err(NodeError::ResidentRuntime);
        }
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or(NodeError::ResidentRuntime)?;
        self.last_trade_time = Some(trade.transaction_time_ms);
        self.identities
            .insert(trade.aggregate_trade_id.clone(), trade.clone());
        self.arrival_order
            .push_back(trade.aggregate_trade_id.clone());
        if self.arrival_order.len() > MAX_IDENTITIES {
            let id = self
                .arrival_order
                .pop_front()
                .ok_or(NodeError::ResidentRuntime)?;
            let retired = self
                .identities
                .remove(&id)
                .ok_or(NodeError::ResidentRuntime)?;
            self.retired_through = Some(retired.transaction_time_ms);
        }
        self.sequence = sequence;
        trade.ordering = PublicTradeOrdering::Session { sequence };
        Ok(Some(trade))
    }
}

fn same_execution(left: &PublicTrade, right: &PublicTrade) -> bool {
    left.symbol == right.symbol
        && left.generation == right.generation
        && left.aggregate_trade_id == right.aggregate_trade_id
        && left.first_trade_id == right.first_trade_id
        && left.last_trade_id == right.last_trade_id
        && left.transaction_time_ms == right.transaction_time_ms
        && left.price == right.price
        && left.quantity == right.quantity
        && left.quote_quantity == right.quote_quantity
        && left.aggressor == right.aggressor
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use venue_domain::{AggressorSide, FieldState, Price, Symbol};

    fn trade(id: PublicTradeId, time: u64) -> Result<PublicTrade, Box<dyn std::error::Error>> {
        Ok(PublicTrade {
            symbol: Symbol::new("DOGE", "USDT")?,
            generation: 10,
            received_at_ms: time,
            exchange_time_ms: time,
            transaction_time_ms: time,
            aggregate_trade_id: id,
            first_trade_id: None,
            last_trade_id: None,
            ordering: PublicTradeOrdering::Unsequenced,
            price: Price::new(Decimal::ONE)?,
            quantity: Decimal::ONE,
            quote_quantity: Decimal::ONE,
            aggressor: FieldState::Known(AggressorSide::Buy),
        })
    }

    #[test]
    fn sparse_and_opaque_ids_use_local_cursor_without_faking_native_ids()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut window = ObservedTradeWindow::default();
        for (index, id) in [
            PublicTradeId::Numeric(5),
            PublicTradeId::Numeric(500),
            PublicTradeId::Opaque("a-uuid".into()),
        ]
        .into_iter()
        .enumerate()
        {
            let result = window
                .accept(trade(id.clone(), 100 + index as u64)?)?
                .ok_or("missing trade")?;
            assert_eq!(result.aggregate_trade_id, id);
            assert_eq!(
                result.ordering,
                PublicTradeOrdering::Session {
                    sequence: index as u64 + 1
                }
            );
        }
        Ok(())
    }

    #[test]
    fn duplicate_is_noop_conflict_and_time_regression_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut window = ObservedTradeWindow::default();
        let original = trade(7.into(), 100)?;
        assert!(window.accept(original.clone())?.is_some());
        let mut duplicate = original.clone();
        duplicate.received_at_ms = 101;
        duplicate.exchange_time_ms = 101;
        assert!(window.accept(duplicate.clone())?.is_none());
        duplicate.quantity = Decimal::TWO;
        assert!(window.accept(duplicate).is_err());
        assert!(window.accept(trade(8.into(), 99)?).is_err());
        assert_eq!(window.sequence, 1);
        let mut next = original;
        next.generation = 11;
        assert_eq!(
            window.accept(next)?.ok_or("missing trade")?.sequence(),
            Some(1)
        );
        assert!(window.accept(trade(8.into(), 102)?).is_err());
        Ok(())
    }

    #[test]
    fn eviction_never_makes_an_old_identity_new_again() -> Result<(), Box<dyn std::error::Error>> {
        let mut window = ObservedTradeWindow::default();
        for id in 1..=(MAX_IDENTITIES as u64 + 1) {
            window.accept(trade(id.into(), id)?)?;
        }
        assert_eq!(window.identities.len(), MAX_IDENTITIES);
        assert!(window.accept(trade(1.into(), 1)?).is_err());
        assert!(window.accept(trade(2.into(), 2)?)?.is_none());
        Ok(())
    }
}
