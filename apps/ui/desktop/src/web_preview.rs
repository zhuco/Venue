//! Local browser preview transports normalized public data only, never account state.
use crate::chart::ChartInterval;
use serde::{Deserialize, Serialize};
use venue_control_protocol::{UiBar, UiBookLevel, UiTrade};

#[cfg(target_arch = "wasm32")]
pub(crate) mod browser;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod server;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Series {
    pub generation: u64,
    pub revision: u64,
    pub last_price_event_ms: Option<u64>,
    pub last_price_received_ms: Option<u64>,
    pub book_event_ms: Option<u64>,
    pub book_received_ms: Option<u64>,
    pub depth_event_ms: Option<u64>,
    pub depth_received_ms: Option<u64>,
    pub symbol: String,
    pub interval: ChartInterval,
    pub bars: Vec<UiBar>,
    pub last: Option<rust_decimal::Decimal>,
    pub bid: Option<rust_decimal::Decimal>,
    pub ask: Option<rust_decimal::Decimal>,
    pub bids: Vec<UiBookLevel>,
    pub asks: Vec<UiBookLevel>,
    pub trades: Vec<UiTrade>,
    pub change_percent_24h: Option<rust_decimal::Decimal>,
    pub status: String,
    #[serde(default)]
    pub price_tick: Option<rust_decimal::Decimal>,
    pub price_scale: usize,
    pub quantity_scale: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Snapshot {
    pub captured_at_ms: u64,
    pub selections: Vec<(String, ChartInterval)>,
    pub symbols: Vec<String>,
    pub series: Vec<Series>,
}

impl Series {
    pub fn observe_prices(&self, prices: &mut crate::market_prices::MarketPrices, now: u64) {
        if let (Some(last), Some(event), Some(received)) = (
            self.last,
            self.last_price_event_ms,
            self.last_price_received_ms,
        ) {
            prices.observe_last(last, event, received, now);
        }
        prices.observe_book(
            self.bid,
            self.ask,
            self.book_event_ms,
            self.book_received_ms,
            now,
        );
    }

    pub fn depth_is_fresh(&self, now: u64) -> bool {
        self.depth_event_ms
            .zip(self.depth_received_ms)
            .is_some_and(|(event, received)| crate::market_prices::fresh(event, received, now))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn preview_keeps_independent_source_clocks_and_does_not_refresh_old_prices()
    -> Result<(), Box<dyn std::error::Error>> {
        let series = Series {
            generation: 1,
            revision: 3,
            last_price_event_ms: Some(1_000),
            last_price_received_ms: Some(1_010),
            book_event_ms: Some(5_000),
            book_received_ms: Some(5_010),
            depth_event_ms: Some(2_000),
            depth_received_ms: Some(2_010),
            symbol: "BTC/USDT".into(),
            interval: ChartInterval::OneMinute,
            bars: Vec::new(),
            status: "Live".into(),
            price_tick: None,
            price_scale: 2,
            quantity_scale: 3,
            last: Some(Decimal::from(100)),
            bid: Some(Decimal::from(99)),
            ask: Some(Decimal::from(101)),
            change_percent_24h: None,
            bids: Vec::new(),
            asks: Vec::new(),
            trades: Vec::new(),
        };
        let restored: Series = serde_json::from_str(&serde_json::to_string(&series)?)?;
        let mut prices = crate::market_prices::MarketPrices::default();
        restored.observe_prices(&mut prices, 8_000);
        assert!(prices.reference_price().is_none());
        assert_eq!(prices.bid, Some(Decimal::from(99)));
        assert!(!restored.depth_is_fresh(8_000));
        let mut expired = crate::market_prices::MarketPrices::default();
        restored.observe_prices(&mut expired, 10_011);
        assert!(expired.bid.is_none());
        let mut other_interval = restored.clone();
        other_interval.interval = ChartInterval::FiveMinutes;
        other_interval.last = Some(Decimal::from(102));
        other_interval.last_price_event_ms = Some(7_000);
        other_interval.last_price_received_ms = Some(7_010);
        other_interval.observe_prices(&mut prices, 8_000);
        restored.observe_prices(&mut prices, 8_000);
        assert_eq!(prices.reference_price(), Some(Decimal::from(102)));
        assert!(restored.change_percent_24h.is_none());
        Ok(())
    }
}
