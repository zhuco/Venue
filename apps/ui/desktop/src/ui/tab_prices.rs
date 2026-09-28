use crate::{market_prices::PriceObservation, model::MarketServer};
use std::collections::BTreeMap;

#[derive(Clone, Default)]
pub(super) struct TabPrices {
    venue: Option<MarketServer>,
    prices: BTreeMap<String, PriceObservation>,
}

impl TabPrices {
    pub fn retain_tabs(&mut self, venue: MarketServer, symbols: &[String]) {
        if self.venue != Some(venue) {
            self.prices.clear();
            self.venue = Some(venue);
        }
        self.prices.retain(|symbol, _| symbols.contains(symbol));
    }

    pub fn observe(
        &mut self,
        symbol: &str,
        latest: Option<PriceObservation>,
    ) -> Option<PriceObservation> {
        if let Some(latest) = latest
            && self.prices.get(symbol).is_none_or(|old| {
                (latest.event_ms, latest.received_ms) > (old.event_ms, old.received_ms)
            })
        {
            self.prices.insert(symbol.into(), latest);
        }
        self.prices.get(symbol).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reconnect_retains_last_trade_but_venue_switch_and_closed_tabs_do_not() {
        let mut cache = TabPrices::default();
        cache.retain_tabs(
            MarketServer::Binance,
            &["BTC/USDC".into(), "DOGE/USDC".into()],
        );
        let last = PriceObservation {
            value: 100.into(),
            event_ms: 1000,
            received_ms: 1001,
        };
        assert_eq!(cache.observe("BTC/USDC", Some(last)), Some(last));
        assert_eq!(cache.observe("BTC/USDC", None), Some(last));
        assert_eq!(
            cache.observe(
                "BTC/USDC",
                Some(PriceObservation {
                    value: 1.into(),
                    event_ms: 900,
                    ..last
                })
            ),
            Some(last)
        );
        assert_eq!(cache.observe("DOGE/USDC", None), None);
        cache.retain_tabs(MarketServer::Bybit, &["BTC/USDC".into()]);
        assert_eq!(cache.observe("BTC/USDC", None), None);
        cache.observe("BTC/USDC", Some(last));
        cache.retain_tabs(MarketServer::Bybit, &[]);
        assert!(cache.prices.is_empty());
    }
}
