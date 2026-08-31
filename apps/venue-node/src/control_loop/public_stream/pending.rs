use std::collections::VecDeque;

use venue_domain::{MarketEvent, Symbol};
use venue_runtime::strategy::AccountMarketEvent;

use crate::NodeError;

const MAX_PUBLIC_BATCH: usize = 1_024;

/// At most one native frame is queued per receiver. Facts are drained individually so one
/// trade burst cannot consume the entire account turn before private work is serviced again.
#[derive(Default)]
pub(super) struct PendingPublicFacts(VecDeque<(u64, MarketEvent)>);

impl PendingPublicFacts {
    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(super) fn install(
        &mut self,
        symbol: &Symbol,
        events: impl IntoIterator<Item = (u64, MarketEvent)>,
    ) -> Result<(), NodeError> {
        if !self.0.is_empty() {
            return Err(NodeError::ResidentRuntime);
        }
        let events = events
            .into_iter()
            .take(MAX_PUBLIC_BATCH + 1)
            .collect::<Vec<_>>();
        if events.len() > MAX_PUBLIC_BATCH {
            return Err(NodeError::ResidentRuntime);
        }
        // Validate the whole frame before publishing any prefix; an invalid suffix is not a
        // partially successful delivery and must not advance this receiver's queued state.
        let events = events
            .into_iter()
            .map(|(time, event)| {
                // Raw unsequenced trades acquire a local session cursor only after the resident
                // validates the book generation and deduplicates the canonical execution.
                if let MarketEvent::Trade(trade) = &event {
                    if !trade.is_valid()
                        || time == 0
                        || trade.received_at_ms != time
                        || &trade.symbol != symbol
                    {
                        return Err(NodeError::ResidentRuntime);
                    }
                } else {
                    let validated = AccountMarketEvent::new(time, event.clone())
                        .map_err(|_| NodeError::ResidentRuntime)?;
                    if validated.symbol() != symbol {
                        return Err(NodeError::ResidentRuntime);
                    }
                }
                Ok((time, event))
            })
            .collect::<Result<VecDeque<_>, _>>()?;
        self.0 = events;
        Ok(())
    }

    pub(super) fn pop(&mut self) -> Option<(u64, MarketEvent)> {
        self.0.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_domain::MarketSnapshot;

    fn snapshot(symbol: &Symbol, sequence: u64) -> MarketEvent {
        MarketEvent::Snapshot(MarketSnapshot {
            symbol: symbol.clone(),
            generation: 1,
            sequence,
            exchange_time_ms: Some(1),
            bids: Vec::new(),
            asks: Vec::new(),
        })
    }

    #[test]
    fn pending_batch_validates_before_install_and_drains_without_overwriting()
    -> Result<(), Box<dyn std::error::Error>> {
        let symbol = Symbol::new("DOGE", "USDT")?;
        let other = Symbol::new("BTC", "USDT")?;
        let mut pending = PendingPublicFacts::default();
        assert!(
            pending
                .install(
                    &symbol,
                    [(1, snapshot(&symbol, 1)), (1, snapshot(&other, 2))]
                )
                .is_err()
        );
        assert!(pending.is_empty());
        assert!(
            pending
                .install(&symbol, (1..=1_025).map(|id| (1, snapshot(&symbol, id))))
                .is_err()
        );
        assert!(pending.is_empty());
        pending.install(
            &symbol,
            [(1, snapshot(&symbol, 1)), (1, snapshot(&symbol, 2))],
        )?;
        assert!(pending.install(&symbol, []).is_err());
        for sequence in 1..=2 {
            let (_, event) = pending.pop().ok_or("missing event")?;
            assert!(matches!(event, MarketEvent::Snapshot(value) if value.sequence==sequence));
        }
        assert!(pending.is_empty());
        Ok(())
    }
}
