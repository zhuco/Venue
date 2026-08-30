use std::collections::VecDeque;

use rust_decimal::{Decimal, prelude::ToPrimitive};
use venue_domain::PublicBar;

use super::{ChartIndicatorError, validate_bar};

#[derive(Clone, Debug)]
pub struct Atr {
    period: usize,
    period_decimal: Decimal,
    previous_close: Option<Decimal>,
    seed_sum: Decimal,
    value: Option<Decimal>,
    samples: usize,
}

impl Atr {
    pub fn new(period: usize) -> Result<Self, ChartIndicatorError> {
        Ok(Self {
            period,
            period_decimal: period_decimal(period)?,
            previous_close: None,
            seed_sum: Decimal::ZERO,
            value: None,
            samples: 0,
        })
    }

    pub fn update(&mut self, bar: &PublicBar) -> Result<Option<Decimal>, ChartIndicatorError> {
        validate_bar(bar)?;
        let range = true_range(bar, self.previous_close)?;
        self.previous_close = Some(bar.close.value());
        self.samples = self.samples.saturating_add(1);
        if self.samples <= self.period {
            self.seed_sum = self
                .seed_sum
                .checked_add(range)
                .ok_or(ChartIndicatorError::Arithmetic)?;
            if self.samples == self.period {
                let seeded = self
                    .seed_sum
                    .checked_div(self.period_decimal)
                    .ok_or(ChartIndicatorError::Arithmetic)?;
                self.value = Some(seeded);
                return Ok(Some(seeded));
            }
            return Ok(None);
        }
        let previous = self.value.ok_or(ChartIndicatorError::Arithmetic)?;
        let retained = previous
            .checked_mul(Decimal::from(
                u64::try_from(self.period.saturating_sub(1))
                    .map_err(|_| ChartIndicatorError::InvalidParameters)?,
            ))
            .ok_or(ChartIndicatorError::Arithmetic)?;
        let value = retained
            .checked_add(range)
            .ok_or(ChartIndicatorError::Arithmetic)?
            .checked_div(self.period_decimal)
            .ok_or(ChartIndicatorError::Arithmetic)?;
        self.value = Some(value);
        Ok(Some(value))
    }

    pub fn reset(&mut self) {
        self.previous_close = None;
        self.seed_sum = Decimal::ZERO;
        self.value = None;
        self.samples = 0;
    }

    pub const fn samples(&self) -> usize {
        self.samples
    }

    pub const fn warmup_period(&self) -> usize {
        self.period
    }

    pub const fn is_ready(&self) -> bool {
        self.samples >= self.period
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BollingerValue {
    pub upper: Decimal,
    pub middle: Decimal,
    pub lower: Decimal,
    pub standard_deviation: Decimal,
    pub bandwidth: Decimal,
    pub percent_b: Decimal,
}

#[derive(Clone, Debug)]
pub struct BollingerBands {
    period: usize,
    period_decimal: Decimal,
    multiplier: Decimal,
    closes: VecDeque<Decimal>,
    sum: Decimal,
    samples: usize,
}

impl BollingerBands {
    pub fn new(period: usize, multiplier: Decimal) -> Result<Self, ChartIndicatorError> {
        if multiplier <= Decimal::ZERO {
            return Err(ChartIndicatorError::InvalidParameters);
        }
        Ok(Self {
            period,
            period_decimal: period_decimal(period)?,
            multiplier,
            closes: VecDeque::with_capacity(period),
            sum: Decimal::ZERO,
            samples: 0,
        })
    }

    pub fn update(
        &mut self,
        bar: &PublicBar,
    ) -> Result<Option<BollingerValue>, ChartIndicatorError> {
        validate_bar(bar)?;
        self.samples = self.samples.saturating_add(1);
        self.sum = self
            .sum
            .checked_add(bar.close.value())
            .ok_or(ChartIndicatorError::Arithmetic)?;
        self.closes.push_back(bar.close.value());
        if self.closes.len() > self.period {
            let oldest = self
                .closes
                .pop_front()
                .ok_or(ChartIndicatorError::Arithmetic)?;
            self.sum = self
                .sum
                .checked_sub(oldest)
                .ok_or(ChartIndicatorError::Arithmetic)?;
        }
        if !self.is_ready() {
            return Ok(None);
        }
        let middle = self
            .sum
            .checked_div(self.period_decimal)
            .ok_or(ChartIndicatorError::Arithmetic)?;
        let mut squared_deviation_sum = Decimal::ZERO;
        for close in &self.closes {
            let deviation = close
                .checked_sub(middle)
                .ok_or(ChartIndicatorError::Arithmetic)?;
            let square = deviation
                .checked_mul(deviation)
                .ok_or(ChartIndicatorError::Arithmetic)?;
            squared_deviation_sum = squared_deviation_sum
                .checked_add(square)
                .ok_or(ChartIndicatorError::Arithmetic)?;
        }
        let variance = squared_deviation_sum
            .checked_div(self.period_decimal)
            .ok_or(ChartIndicatorError::Arithmetic)?;
        let standard_deviation = Decimal::from_f64_retain(
            variance
                .to_f64()
                .ok_or(ChartIndicatorError::Arithmetic)?
                .sqrt(),
        )
        .ok_or(ChartIndicatorError::Arithmetic)?;
        let offset = self
            .multiplier
            .checked_mul(standard_deviation)
            .ok_or(ChartIndicatorError::Arithmetic)?;
        let upper = middle
            .checked_add(offset)
            .ok_or(ChartIndicatorError::Arithmetic)?;
        let lower = middle
            .checked_sub(offset)
            .ok_or(ChartIndicatorError::Arithmetic)?;
        let width = upper
            .checked_sub(lower)
            .ok_or(ChartIndicatorError::Arithmetic)?;
        let bandwidth = if middle.is_zero() {
            Decimal::ZERO
        } else {
            width
                .checked_div(middle)
                .ok_or(ChartIndicatorError::Arithmetic)?
        };
        let percent_b = if width.is_zero() {
            Decimal::new(5, 1)
        } else {
            bar.close
                .value()
                .checked_sub(lower)
                .ok_or(ChartIndicatorError::Arithmetic)?
                .checked_div(width)
                .ok_or(ChartIndicatorError::Arithmetic)?
        };
        Ok(Some(BollingerValue {
            upper,
            middle,
            lower,
            standard_deviation,
            bandwidth,
            percent_b,
        }))
    }

    pub fn reset(&mut self) {
        self.closes.clear();
        self.sum = Decimal::ZERO;
        self.samples = 0;
    }

    pub const fn samples(&self) -> usize {
        self.samples
    }

    pub const fn warmup_period(&self) -> usize {
        self.period
    }

    pub const fn is_ready(&self) -> bool {
        self.samples >= self.period
    }
}

fn true_range(
    bar: &PublicBar,
    previous_close: Option<Decimal>,
) -> Result<Decimal, ChartIndicatorError> {
    let high_low = bar
        .high
        .value()
        .checked_sub(bar.low.value())
        .ok_or(ChartIndicatorError::Arithmetic)?;
    let Some(previous_close) = previous_close else {
        return Ok(high_low);
    };
    let high_gap = bar
        .high
        .value()
        .checked_sub(previous_close)
        .ok_or(ChartIndicatorError::Arithmetic)?
        .abs();
    let low_gap = bar
        .low
        .value()
        .checked_sub(previous_close)
        .ok_or(ChartIndicatorError::Arithmetic)?
        .abs();
    Ok(high_low.max(high_gap).max(low_gap))
}

fn period_decimal(period: usize) -> Result<Decimal, ChartIndicatorError> {
    let period = u64::try_from(period).map_err(|_| ChartIndicatorError::InvalidParameters)?;
    if period == 0 {
        return Err(ChartIndicatorError::InvalidParameters);
    }
    Ok(Decimal::from(period))
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use venue_domain::{FieldState, Price, PublicBar};

    use super::{Atr, BollingerBands};

    fn bar(
        high: i64,
        low: i64,
        close: i64,
        sequence: u64,
    ) -> Result<PublicBar, Box<dyn std::error::Error>> {
        let high = Decimal::from(high);
        let low = Decimal::from(low);
        let close = Decimal::from(close);
        Ok(PublicBar {
            symbol: "BTC/USDT".parse()?,
            generation: 1,
            received_at_ms: sequence * 60_000,
            sequence,
            open_time_ms: (sequence - 1) * 60_000,
            close_time_ms: sequence * 60_000 - 1,
            interval_ms: 60_000,
            open: Price::new(close)?,
            high: Price::new(high)?,
            low: Price::new(low)?,
            close: Price::new(close)?,
            base_volume: FieldState::Known(Decimal::ONE),
            quote_volume: FieldState::Known(close),
            trade_count: FieldState::Known(1),
            taker_buy_base_volume: FieldState::Known(Decimal::ZERO),
            taker_buy_quote_volume: FieldState::Known(Decimal::ZERO),
        })
    }

    #[test]
    fn atr_uses_wilder_seed_and_recursion() -> Result<(), Box<dyn std::error::Error>> {
        let mut atr = Atr::new(2)?;
        assert_eq!(atr.update(&bar(11, 9, 10, 1)?)?, None);
        assert_eq!(atr.update(&bar(14, 12, 13, 2)?)?, Some(Decimal::from(3)));
        assert_eq!(atr.update(&bar(13, 11, 12, 3)?)?, Some(Decimal::new(25, 1)));
        Ok(())
    }

    #[test]
    fn flat_bollinger_has_zero_width_and_center_percent_b() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut bands = BollingerBands::new(3, Decimal::from(2))?;
        assert_eq!(bands.update(&bar(10, 10, 10, 1)?)?, None);
        assert_eq!(bands.update(&bar(10, 10, 10, 2)?)?, None);
        let value = bands
            .update(&bar(10, 10, 10, 3)?)?
            .ok_or("expected bollinger value")?;
        assert_eq!(value.upper, Decimal::from(10));
        assert_eq!(value.lower, Decimal::from(10));
        assert_eq!(value.percent_b, Decimal::new(5, 1));
        Ok(())
    }
}
