use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::domain::{FieldState, Price, Symbol, UnknownReason};

pub const BOOK_SOURCE: &str = "book";
pub const TRADES_SOURCE: &str = "trades";
pub const BARS_SOURCE: &str = "bars";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MarketLevel {
    pub price: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MarketSnapshot {
    pub symbol: Symbol,
    pub generation: u64,
    pub sequence: u64,
    pub exchange_time_ms: Option<u64>,
    pub bids: Vec<MarketLevel>,
    pub asks: Vec<MarketLevel>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MarketDelta {
    pub symbol: Symbol,
    pub generation: u64,
    pub first_sequence: u64,
    pub previous_sequence: Option<u64>,
    pub sequence: u64,
    pub exchange_time_ms: Option<u64>,
    pub bids: Vec<MarketLevel>,
    pub asks: Vec<MarketLevel>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggressorSide {
    Buy,
    Sell,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublicTrade {
    pub symbol: Symbol,
    pub generation: u64,
    pub received_at_ms: u64,
    pub exchange_time_ms: u64,
    pub transaction_time_ms: u64,
    pub aggregate_trade_id: u64,
    pub first_trade_id: u64,
    pub last_trade_id: u64,
    pub price: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub quote_quantity: Decimal,
    pub aggressor: FieldState<AggressorSide>,
}

/// One completed normalized public bar. Strategies consume only closed bars; an in-progress
/// exchange kline is never promoted into this domain fact.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublicBar {
    pub symbol: Symbol,
    pub generation: u64,
    pub received_at_ms: u64,
    pub sequence: u64,
    pub open_time_ms: u64,
    pub close_time_ms: u64,
    pub interval_ms: u64,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub base_volume: FieldState<Decimal>,
    pub quote_volume: FieldState<Decimal>,
    pub trade_count: FieldState<u64>,
    pub taker_buy_base_volume: FieldState<Decimal>,
    pub taker_buy_quote_volume: FieldState<Decimal>,
}

impl PublicBar {
    /// Validates only facts provable from normalized fields; unknown source fields remain unknown.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        let Some(span_ms) = self.close_time_ms.checked_sub(self.open_time_ms) else {
            return false;
        };
        if self.generation == 0
            || self.received_at_ms == 0
            || self.sequence == 0
            || self.interval_ms == 0
            || (span_ms != self.interval_ms && span_ms.checked_add(1) != Some(self.interval_ms))
            || self.high < self.open.max(self.close)
            || self.low > self.open.min(self.close)
            || self.high < self.low
            || !explicit(&self.base_volume)
            || !explicit(&self.quote_volume)
            || !explicit(&self.trade_count)
            || !explicit(&self.taker_buy_base_volume)
            || !explicit(&self.taker_buy_quote_volume)
            || !non_negative(&self.base_volume)
            || !non_negative(&self.quote_volume)
            || !non_negative(&self.taker_buy_base_volume)
            || !non_negative(&self.taker_buy_quote_volume)
            || !known_lte(&self.taker_buy_base_volume, &self.base_volume)
            || !known_lte(&self.taker_buy_quote_volume, &self.quote_volume)
            || !quote_is_price_bounded(&self.base_volume, &self.quote_volume, self.low, self.high)
            || !quote_is_price_bounded(
                &self.taker_buy_base_volume,
                &self.taker_buy_quote_volume,
                self.low,
                self.high,
            )
        {
            return false;
        }
        match self.trade_count {
            FieldState::Known(0) => [
                &self.base_volume,
                &self.quote_volume,
                &self.taker_buy_base_volume,
                &self.taker_buy_quote_volume,
            ]
            .into_iter()
            .all(known_zero),
            FieldState::Known(_) => {
                known_positive(&self.base_volume) && known_positive(&self.quote_volume)
            }
            _ => true,
        }
    }
}

fn explicit<T>(value: &FieldState<T>) -> bool {
    matches!(value, FieldState::Known(_) | FieldState::Unavailable { .. })
}

fn non_negative(value: &FieldState<Decimal>) -> bool {
    !matches!(value, FieldState::Known(value) if value.is_sign_negative())
}

fn known_lte(left: &FieldState<Decimal>, right: &FieldState<Decimal>) -> bool {
    match (left, right) {
        (FieldState::Known(left), FieldState::Known(right)) => left <= right,
        (FieldState::Known(_), _) => false,
        _ => true,
    }
}

fn quote_is_price_bounded(
    base: &FieldState<Decimal>,
    quote: &FieldState<Decimal>,
    low: Price,
    high: Price,
) -> bool {
    let quote = match quote {
        FieldState::Known(quote) => quote,
        _ => return true,
    };
    let FieldState::Known(base) = base else {
        return false;
    };
    let Some(minimum) = base.checked_mul(low.value()) else {
        return false;
    };
    let Some(maximum) = base.checked_mul(high.value()) else {
        return false;
    };
    *quote >= minimum && *quote <= maximum
}

fn known_positive(value: &FieldState<Decimal>) -> bool {
    matches!(value, FieldState::Known(value) if *value > Decimal::ZERO)
}

fn known_zero(value: &FieldState<Decimal>) -> bool {
    matches!(value, FieldState::Known(value) if value.is_zero())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublicTicker {
    pub symbol: Symbol,
    pub generation: u64,
    pub received_at_ms: u64,
    pub exchange_time_ms: u64,
    pub transaction_time_ms: u64,
    pub update_id: u64,
    pub bid_price: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub bid_quantity: Decimal,
    pub ask_price: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub ask_quantity: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MarkFunding {
    pub symbol: Symbol,
    pub generation: u64,
    pub received_at_ms: u64,
    pub exchange_time_ms: u64,
    pub next_funding_time_ms: u64,
    pub mark_price: Price,
    pub index_price: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub funding_rate: Decimal,
    pub estimated_settle_price: FieldState<Price>,
    pub predicted_funding_rate: FieldState<Decimal>,
    pub unknown_reason: Option<UnknownReason>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "payload")]
pub enum MarketEvent {
    Snapshot(MarketSnapshot),
    Delta(MarketDelta),
    Trade(PublicTrade),
    Bar(PublicBar),
    Ticker(PublicTicker),
    MarkFunding(MarkFunding),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bar() -> Result<PublicBar, Box<dyn std::error::Error>> {
        Ok(PublicBar {
            symbol: "BTC/USDT".parse()?,
            generation: 1,
            received_at_ms: 60_000,
            sequence: 1,
            open_time_ms: 0,
            close_time_ms: 59_999,
            interval_ms: 60_000,
            open: Price::new(Decimal::from(100))?,
            high: Price::new(Decimal::from(110))?,
            low: Price::new(Decimal::from(90))?,
            close: Price::new(Decimal::from(105))?,
            base_volume: FieldState::Known(Decimal::from(10)),
            quote_volume: FieldState::Known(Decimal::from(1_000)),
            trade_count: FieldState::Known(5),
            taker_buy_base_volume: FieldState::Known(Decimal::from(4)),
            taker_buy_quote_volume: FieldState::Known(Decimal::from(400)),
        })
    }

    #[test]
    fn completed_bar_accepts_known_or_explicitly_unknown_volume()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut value = bar()?;
        assert!(value.is_valid());
        value.quote_volume = FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        };
        value.trade_count = FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        };
        value.taker_buy_quote_volume = FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        };
        assert!(value.is_valid());
        let serialized = serde_json::to_value(&value)?;
        assert_eq!(serialized["quote_volume"]["state"], "unavailable");
        Ok(())
    }

    #[test]
    fn completed_bar_rejects_unprovable_volume_and_time_claims()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut value = bar()?;
        value.taker_buy_base_volume = FieldState::Known(Decimal::from(11));
        assert!(!value.is_valid());

        value = bar()?;
        value.quote_volume = FieldState::Known(Decimal::from(2_000));
        assert!(!value.is_valid());

        value = bar()?;
        value.trade_count = FieldState::Known(0);
        assert!(!value.is_valid());

        value = bar()?;
        value.base_volume = FieldState::Known(-Decimal::ONE);
        assert!(!value.is_valid());

        value = bar()?;
        value.base_volume = FieldState::Known(Decimal::ZERO);
        value.quote_volume = FieldState::Known(Decimal::ZERO);
        value.trade_count = FieldState::Known(0);
        value.taker_buy_base_volume = FieldState::Known(Decimal::ZERO);
        value.taker_buy_quote_volume = FieldState::Known(Decimal::ZERO);
        assert!(value.is_valid());

        value = bar()?;
        value.open = Price::new(Decimal::MAX)?;
        value.high = Price::new(Decimal::MAX)?;
        value.low = Price::new(Decimal::MAX)?;
        value.close = Price::new(Decimal::MAX)?;
        value.base_volume = FieldState::Known(Decimal::MAX);
        value.quote_volume = FieldState::Known(Decimal::MAX);
        assert!(!value.is_valid());

        value = bar()?;
        value.base_volume = FieldState::Missing;
        assert!(!value.is_valid());

        value = bar()?;
        value.received_at_ms = 1;
        assert!(value.is_valid());
        Ok(())
    }
}
