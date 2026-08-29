use std::collections::BTreeMap;

use crate::{
    domain::{MarketEvent, Price, Symbol},
    runtime::strategy::{AccountMarketEvent, MarketEventKind},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BestBidOffer {
    pub symbol: Symbol,
    pub source_kind: MarketEventKind,
    pub generation: u64,
    pub sequence: u64,
    pub received_at_ms: u64,
    pub exchange_time_ms: Option<u64>,
    pub bid: Price,
    pub ask: Price,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketPublish {
    pub event: AccountMarketEvent,
    pub updated_bbo: Option<BestBidOffer>,
}

/// Connection ownership remains outside this pure hub. It validates one normalized stream per
/// symbol and maintains the local BBO used by post-only checks.
#[derive(Clone, Debug, Default)]
pub struct MarketHub {
    symbol_generations: BTreeMap<Symbol, u64>,
    watermarks: BTreeMap<(Symbol, MarketEventKind), (u64, u64)>,
    bbo: BTreeMap<Symbol, BestBidOffer>,
}

impl MarketHub {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn publish(&mut self, event: AccountMarketEvent) -> Result<MarketPublish, MarketHubError> {
        let symbol = event.symbol().clone();
        let generation = event.generation();
        match self.symbol_generations.get(&symbol).copied() {
            Some(current_generation) if generation < current_generation => {
                return Err(MarketHubError::StaleOrDuplicate);
            }
            Some(current_generation) if generation > current_generation => {
                self.watermarks
                    .retain(|(watermark_symbol, _), _| watermark_symbol != &symbol);
                self.bbo.remove(&symbol);
                self.symbol_generations.insert(symbol.clone(), generation);
            }
            None => {
                self.symbol_generations.insert(symbol.clone(), generation);
            }
            Some(_) => {}
        }

        let stream_key = (symbol.clone(), event.kind());
        let watermark = (generation, event.sequence());
        if let Some((last_generation, last_sequence)) = self.watermarks.get(&stream_key).copied()
            && (watermark.0 < last_generation
                || (watermark.0 == last_generation && watermark.1 <= last_sequence))
        {
            return Err(MarketHubError::StaleOrDuplicate);
        }

        let bbo_candidate = bbo_from_event(&event)?;
        self.watermarks.insert(stream_key, watermark);
        let updated_bbo = bbo_candidate.filter(|candidate| {
            self.bbo
                .get(&symbol)
                .is_none_or(|existing| bbo_is_newer(candidate, existing))
        });
        if let Some(value) = updated_bbo.clone() {
            self.bbo.insert(symbol, value);
        }
        Ok(MarketPublish { event, updated_bbo })
    }

    #[must_use]
    pub fn bbo(&self, symbol: &Symbol, generation: u64) -> Option<&BestBidOffer> {
        (self.symbol_generations.get(symbol).copied() == Some(generation))
            .then(|| self.bbo.get(symbol))
            .flatten()
            .filter(|bbo| bbo.generation == generation)
    }
}

fn bbo_is_newer(candidate: &BestBidOffer, existing: &BestBidOffer) -> bool {
    if candidate.generation != existing.generation {
        return candidate.generation > existing.generation;
    }
    match (candidate.exchange_time_ms, existing.exchange_time_ms) {
        (Some(candidate_time), Some(existing_time)) if candidate_time != existing_time => {
            candidate_time > existing_time
        }
        _ if candidate.source_kind == existing.source_kind => {
            candidate.sequence > existing.sequence
        }
        _ => false,
    }
}

fn bbo_from_event(event: &AccountMarketEvent) -> Result<Option<BestBidOffer>, MarketHubError> {
    let (bid, ask, exchange_time_ms) = match &event.event {
        MarketEvent::Ticker(ticker) => (
            ticker.bid_price,
            ticker.ask_price,
            Some(ticker.exchange_time_ms),
        ),
        MarketEvent::Snapshot(snapshot) => {
            let Some(bid) = snapshot.bids.first().map(|level| level.price) else {
                return Ok(None);
            };
            let Some(ask) = snapshot.asks.first().map(|level| level.price) else {
                return Ok(None);
            };
            (bid, ask, snapshot.exchange_time_ms)
        }
        _ => return Ok(None),
    };
    if bid >= ask {
        return Err(MarketHubError::CrossedBook);
    }
    Ok(Some(BestBidOffer {
        symbol: event.symbol().clone(),
        source_kind: event.kind(),
        generation: event.generation(),
        sequence: event.sequence(),
        received_at_ms: event.received_at_ms,
        exchange_time_ms,
        bid,
        ask,
    }))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum MarketHubError {
    #[error("market event is stale or duplicated")]
    StaleOrDuplicate,
    #[error("normalized best bid is not below best ask")]
    CrossedBook,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use rust_decimal::Decimal;

    use super::*;
    use crate::domain::{PublicBar, PublicTicker};

    fn bbo(
        generation: u64,
        sequence: u64,
        received_at_ms: u64,
        exchange_time_ms: Option<u64>,
    ) -> Result<BestBidOffer, Box<dyn Error>> {
        Ok(BestBidOffer {
            symbol: Symbol::new("SOL", "USDT")?,
            source_kind: MarketEventKind::Ticker,
            generation,
            sequence,
            received_at_ms,
            exchange_time_ms,
            bid: Price::new(Decimal::new(99, 1))?,
            ask: Price::new(Decimal::new(101, 1))?,
        })
    }

    #[test]
    fn bbo_uses_sequence_when_generation_and_event_time_tie() -> Result<(), Box<dyn Error>> {
        let existing = bbo(7, 10, 10_000, Some(500))?;
        let later_sequence_with_older_receive_time = bbo(7, 11, 1, Some(500))?;
        let earlier_sequence_with_newer_receive_time = bbo(7, 9, 20_000, Some(500))?;

        assert!(bbo_is_newer(
            &later_sequence_with_older_receive_time,
            &existing
        ));
        assert!(!bbo_is_newer(
            &earlier_sequence_with_newer_receive_time,
            &existing
        ));
        Ok(())
    }

    #[test]
    fn bbo_never_uses_receive_time_as_freshness() -> Result<(), Box<dyn Error>> {
        let existing = bbo(7, 10, 10_000, Some(500))?;
        let same_event_with_newer_receive_time = bbo(7, 10, 20_000, Some(500))?;
        let older_event_with_newer_receive_time = bbo(7, 99, 20_000, Some(499))?;
        let missing_time_with_later_sequence = bbo(7, 11, 1, None)?;
        let missing_time_with_earlier_sequence = bbo(7, 9, 20_000, None)?;
        let mut incomparable_snapshot = bbo(7, 999, 30_000, None)?;
        incomparable_snapshot.source_kind = MarketEventKind::Snapshot;

        assert!(!bbo_is_newer(
            &same_event_with_newer_receive_time,
            &existing
        ));
        assert!(!bbo_is_newer(
            &older_event_with_newer_receive_time,
            &existing
        ));
        assert!(bbo_is_newer(&missing_time_with_later_sequence, &existing));
        assert!(!bbo_is_newer(
            &missing_time_with_earlier_sequence,
            &existing
        ));
        assert!(!bbo_is_newer(&incomparable_snapshot, &existing));
        Ok(())
    }

    fn ticker_event(
        symbol: &Symbol,
        generation: u64,
        sequence: u64,
        bid: i64,
        ask: i64,
    ) -> Result<AccountMarketEvent, Box<dyn Error>> {
        let received_at_ms = generation * 10_000 + sequence;
        Ok(AccountMarketEvent::new(
            received_at_ms,
            MarketEvent::Ticker(PublicTicker {
                symbol: symbol.clone(),
                generation,
                received_at_ms,
                exchange_time_ms: received_at_ms - 1,
                transaction_time_ms: received_at_ms - 1,
                update_id: sequence,
                bid_price: Price::new(Decimal::new(bid, 0))?,
                bid_quantity: Decimal::new(1, 0),
                ask_price: Price::new(Decimal::new(ask, 0))?,
                ask_quantity: Decimal::new(1, 0),
            }),
        )?)
    }

    fn bar_event(
        symbol: &Symbol,
        generation: u64,
        sequence: u64,
    ) -> Result<AccountMarketEvent, Box<dyn Error>> {
        let received_at_ms = generation * 10_000 + sequence;
        Ok(AccountMarketEvent::new(
            received_at_ms,
            MarketEvent::Bar(PublicBar {
                symbol: symbol.clone(),
                generation,
                received_at_ms,
                sequence,
                open_time_ms: received_at_ms - 100,
                close_time_ms: received_at_ms - 1,
                interval_ms: 100,
                open: Price::new(Decimal::new(100, 0))?,
                high: Price::new(Decimal::new(101, 0))?,
                low: Price::new(Decimal::new(99, 0))?,
                close: Price::new(Decimal::new(100, 0))?,
                base_volume: crate::domain::FieldState::Unavailable {
                    reason: crate::domain::UnknownReason::SourceOmitted,
                },
                quote_volume: crate::domain::FieldState::Unavailable {
                    reason: crate::domain::UnknownReason::SourceOmitted,
                },
                trade_count: crate::domain::FieldState::Unavailable {
                    reason: crate::domain::UnknownReason::SourceOmitted,
                },
                taker_buy_base_volume: crate::domain::FieldState::Unavailable {
                    reason: crate::domain::UnknownReason::SourceOmitted,
                },
                taker_buy_quote_volume: crate::domain::FieldState::Unavailable {
                    reason: crate::domain::UnknownReason::SourceOmitted,
                },
            }),
        )?)
    }

    #[test]
    fn generation_fence_clears_every_kind_and_invalidates_old_bbo() -> Result<(), Box<dyn Error>> {
        let symbol = Symbol::new("SOL", "USDT")?;
        let mut hub = MarketHub::new();

        hub.publish(ticker_event(&symbol, 1, 100, 99, 101)?)?;
        hub.publish(bar_event(&symbol, 1, 100)?)?;
        assert!(hub.bbo(&symbol, 1).is_some());
        assert!(hub.bbo(&symbol, 2).is_none());

        hub.publish(bar_event(&symbol, 2, 1)?)?;
        assert!(hub.bbo(&symbol, 1).is_none());
        assert!(hub.bbo(&symbol, 2).is_none());
        assert_eq!(hub.watermarks.len(), 1);

        assert!(matches!(
            hub.publish(ticker_event(&symbol, 1, 101, 99, 101)?),
            Err(MarketHubError::StaleOrDuplicate)
        ));
        assert!(matches!(
            hub.publish(bar_event(&symbol, 1, 101)?),
            Err(MarketHubError::StaleOrDuplicate)
        ));

        hub.publish(ticker_event(&symbol, 2, 1, 100, 102)?)?;
        assert!(hub.bbo(&symbol, 1).is_none());
        assert_eq!(hub.bbo(&symbol, 2).map(|bbo| bbo.sequence), Some(1));
        Ok(())
    }

    #[test]
    fn rejected_new_generation_book_still_invalidates_previous_generation()
    -> Result<(), Box<dyn Error>> {
        let symbol = Symbol::new("SOL", "USDT")?;
        let mut hub = MarketHub::new();

        hub.publish(ticker_event(&symbol, 7, 10, 99, 101)?)?;
        assert!(hub.bbo(&symbol, 7).is_some());

        assert!(matches!(
            hub.publish(ticker_event(&symbol, 8, 1, 101, 101)?),
            Err(MarketHubError::CrossedBook)
        ));
        assert!(hub.bbo(&symbol, 7).is_none());
        assert!(hub.bbo(&symbol, 8).is_none());
        assert!(matches!(
            hub.publish(bar_event(&symbol, 7, 999)?),
            Err(MarketHubError::StaleOrDuplicate)
        ));
        Ok(())
    }
}
