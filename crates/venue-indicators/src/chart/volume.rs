use rust_decimal::Decimal;
use venue_domain::{FieldState, PublicBar};

use super::{ChartIndicatorError, validate_bar};

/// One candle's actual traded quote/base ratio, distinct from cumulative typical-price VWAP.
pub fn bar_vwap(bar: &PublicBar) -> Option<Decimal> {
    let (FieldState::Known(base), FieldState::Known(quote)) = (&bar.base_volume, &bar.quote_volume)
    else {
        return None;
    };
    if *base <= Decimal::ZERO || *quote <= Decimal::ZERO {
        return None;
    }
    quote
        .checked_div(*base)
        .filter(|price| *price >= bar.low.value() && *price <= bar.high.value())
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Vwap {
    cumulative_price_volume: Decimal,
    cumulative_volume: Decimal,
}

impl Vwap {
    pub fn new() -> Result<Self, ChartIndicatorError> {
        Ok(Self::default())
    }

    pub(crate) fn totals(&self) -> (Decimal, Decimal) {
        (self.cumulative_price_volume, self.cumulative_volume)
    }

    pub(crate) fn from_totals(price_volume: Decimal, volume: Decimal) -> Self {
        Self { cumulative_price_volume: price_volume, cumulative_volume: volume }
    }

    pub(crate) fn current_value(&self) -> Result<Option<Decimal>, ChartIndicatorError> {
        if self.cumulative_volume.is_zero() { return Ok(None); }
        self.cumulative_price_volume.checked_div(self.cumulative_volume)
            .map(Some).ok_or(ChartIndicatorError::Arithmetic)
    }

    pub fn update(&mut self, bar: &PublicBar) -> Result<Option<Decimal>, ChartIndicatorError> {
        validate_bar(bar)?;
        let FieldState::Known(volume) = &bar.base_volume else {
            return Err(ChartIndicatorError::VolumeUnavailable);
        };
        let high_low = bar
            .high
            .value()
            .checked_add(bar.low.value())
            .ok_or(ChartIndicatorError::Arithmetic)?;
        let price_sum = high_low
            .checked_add(bar.close.value())
            .ok_or(ChartIndicatorError::Arithmetic)?;
        let typical_price = price_sum
            .checked_div(Decimal::from(3_u32))
            .ok_or(ChartIndicatorError::Arithmetic)?;
        let contribution = typical_price
            .checked_mul(*volume)
            .ok_or(ChartIndicatorError::Arithmetic)?;
        let cumulative_price_volume = self
            .cumulative_price_volume
            .checked_add(contribution)
            .ok_or(ChartIndicatorError::Arithmetic)?;
        let cumulative_volume = self
            .cumulative_volume
            .checked_add(*volume)
            .ok_or(ChartIndicatorError::Arithmetic)?;
        let value = Self::from_totals(cumulative_price_volume, cumulative_volume).current_value()?;
        self.cumulative_price_volume = cumulative_price_volume;
        self.cumulative_volume = cumulative_volume;
        Ok(value)
    }

    pub fn reset(&mut self) {
        self.cumulative_price_volume = Decimal::ZERO;
        self.cumulative_volume = Decimal::ZERO;
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rust_decimal::Decimal;
    use venue_domain::{FieldState, Price, PublicBar, UnknownReason};

    use super::Vwap;
    use crate::chart::ChartIndicatorError;

    #[test]
    fn actual_bar_vwap_uses_quote_volume_and_does_not_invent_missing_data()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut input = bar(1, 110, 90, 105, Some(10))?;
        input.quote_volume = FieldState::Known(Decimal::from(1_020));
        assert_eq!(super::bar_vwap(&input), Some(Decimal::from(102)));
        input.quote_volume = FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        };
        assert!(super::bar_vwap(&input).is_none());
        Ok(())
    }

    fn bar(
        sequence: u64,
        high: i64,
        low: i64,
        close: i64,
        volume: Option<i64>,
    ) -> Result<PublicBar, Box<dyn std::error::Error>> {
        let open_time_ms = sequence
            .checked_sub(1)
            .and_then(|value| value.checked_mul(60_000))
            .ok_or("time")?;
        let close_time_ms = open_time_ms.checked_add(59_999).ok_or("time")?;
        let base_volume = volume.map_or(
            FieldState::Unavailable {
                reason: UnknownReason::SourceOmitted,
            },
            |value| FieldState::Known(Decimal::from(value)),
        );
        let quote_volume = volume.map_or(
            FieldState::Unavailable {
                reason: UnknownReason::SourceOmitted,
            },
            |value| FieldState::Known(Decimal::from(value * close)),
        );
        let trade_count = volume.map_or(
            FieldState::Unavailable {
                reason: UnknownReason::SourceOmitted,
            },
            |_| FieldState::Known(1),
        );
        let taker_buy_base_volume = volume.map_or(
            FieldState::Unavailable {
                reason: UnknownReason::SourceOmitted,
            },
            |_| FieldState::Known(Decimal::ZERO),
        );
        let taker_buy_quote_volume = volume.map_or(
            FieldState::Unavailable {
                reason: UnknownReason::SourceOmitted,
            },
            |_| FieldState::Known(Decimal::ZERO),
        );
        Ok(PublicBar {
            symbol: "BTC/USDT".parse()?,
            generation: 1,
            received_at_ms: close_time_ms.checked_add(1).ok_or("time")?,
            sequence,
            open_time_ms,
            close_time_ms,
            interval_ms: 60_000,
            open: Price::new(Decimal::from(close))?,
            high: Price::new(Decimal::from(high))?,
            low: Price::new(Decimal::from(low))?,
            close: Price::new(Decimal::from(close))?,
            base_volume,
            quote_volume,
            trade_count,
            taker_buy_base_volume,
            taker_buy_quote_volume,
        })
    }

    #[test]
    fn accumulates_typical_price_weighted_by_base_volume() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut vwap = Vwap::new()?;
        let first = vwap
            .update(&bar(1, 12, 9, 9, Some(2))?)?
            .ok_or("first VWAP")?;
        assert_eq!(first, Decimal::from(10));
        let second = vwap
            .update(&bar(2, 24, 18, 18, Some(1))?)?
            .ok_or("second VWAP")?;
        assert_eq!(second, Decimal::from_str("13.333333333333333333333333333")?);
        Ok(())
    }

    #[test]
    fn missing_volume_fails_without_mutating_accumulator() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut vwap = Vwap::new()?;
        assert_eq!(
            vwap.update(&bar(1, 12, 9, 9, None)?),
            Err(ChartIndicatorError::VolumeUnavailable)
        );
        assert_eq!(
            vwap.update(&bar(2, 12, 9, 9, Some(2))?)?,
            Some(Decimal::from(10))
        );
        Ok(())
    }

    #[test]
    fn reset_starts_a_new_cumulative_anchor() -> Result<(), Box<dyn std::error::Error>> {
        let mut vwap = Vwap::new()?;
        let _ = vwap.update(&bar(1, 12, 9, 9, Some(2))?)?;
        vwap.reset();
        assert_eq!(
            vwap.update(&bar(2, 24, 18, 18, Some(1))?)?,
            Some(Decimal::from(20))
        );
        Ok(())
    }
}
