use std::collections::VecDeque;

use rust_decimal::Decimal;
use venue_domain::PublicBar;

use super::{ChartIndicatorError, validate_bar};

#[derive(Clone, Debug)]
pub struct Sma {
    period: usize,
    period_decimal: Decimal,
    closes: VecDeque<Decimal>,
    sum: Decimal,
    samples: usize,
}

impl Sma {
    pub fn new(period: usize) -> Result<Self, ChartIndicatorError> {
        let period_decimal = period_decimal(period)?;
        Ok(Self {
            period,
            period_decimal,
            closes: VecDeque::with_capacity(period),
            sum: Decimal::ZERO,
            samples: 0,
        })
    }

    pub fn update(&mut self, bar: &PublicBar) -> Result<Option<Decimal>, ChartIndicatorError> {
        validate_bar(bar)?;
        self.update_value(bar.close.value())
    }

    pub(super) fn update_value(
        &mut self,
        value: Decimal,
    ) -> Result<Option<Decimal>, ChartIndicatorError> {
        self.samples = self.samples.saturating_add(1);
        self.sum = self
            .sum
            .checked_add(value)
            .ok_or(ChartIndicatorError::Arithmetic)?;
        self.closes.push_back(value);
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
        self.is_ready()
            .then(|| {
                self.sum
                    .checked_div(self.period_decimal)
                    .ok_or(ChartIndicatorError::Arithmetic)
            })
            .transpose()
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

#[derive(Clone, Debug)]
pub struct Ema {
    period: usize,
    alpha: Decimal,
    value: Option<Decimal>,
    samples: usize,
}

impl Ema {
    pub fn new(period: usize) -> Result<Self, ChartIndicatorError> {
        let denominator = period_decimal(period)?
            .checked_add(Decimal::ONE)
            .ok_or(ChartIndicatorError::Arithmetic)?;
        let alpha = Decimal::from(2_u8)
            .checked_div(denominator)
            .ok_or(ChartIndicatorError::Arithmetic)?;
        Ok(Self {
            period,
            alpha,
            value: None,
            samples: 0,
        })
    }

    pub fn update(&mut self, bar: &PublicBar) -> Result<Option<Decimal>, ChartIndicatorError> {
        validate_bar(bar)?;
        self.samples = self.samples.saturating_add(1);
        let close = bar.close.value();
        let value = match self.value {
            Some(previous) => {
                let change = close
                    .checked_sub(previous)
                    .ok_or(ChartIndicatorError::Arithmetic)?;
                let adjustment = self
                    .alpha
                    .checked_mul(change)
                    .ok_or(ChartIndicatorError::Arithmetic)?;
                previous
                    .checked_add(adjustment)
                    .ok_or(ChartIndicatorError::Arithmetic)?
            }
            None => close,
        };
        self.value = Some(value);
        Ok(self.is_ready().then_some(value))
    }

    pub fn reset(&mut self) {
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

    use super::{Ema, Sma};

    fn bar(close: i64, sequence: u64) -> Result<PublicBar, Box<dyn std::error::Error>> {
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
            high: Price::new(close)?,
            low: Price::new(close)?,
            close: Price::new(close)?,
            base_volume: FieldState::Known(Decimal::ONE),
            quote_volume: FieldState::Known(close),
            trade_count: FieldState::Known(1),
            taker_buy_base_volume: FieldState::Known(Decimal::ZERO),
            taker_buy_quote_volume: FieldState::Known(Decimal::ZERO),
        })
    }

    #[test]
    fn sma_warms_then_rolls_exactly() -> Result<(), Box<dyn std::error::Error>> {
        let mut sma = Sma::new(3)?;
        assert_eq!(sma.update(&bar(1, 1)?)?, None);
        assert_eq!(sma.update(&bar(2, 2)?)?, None);
        assert_eq!(sma.update(&bar(3, 3)?)?, Some(Decimal::from(2)));
        assert_eq!(sma.update(&bar(4, 4)?)?, Some(Decimal::from(3)));
        assert!(sma.is_ready());
        sma.reset();
        assert_eq!(sma.samples(), 0);
        Ok(())
    }

    #[test]
    fn ema_exposes_only_ready_values() -> Result<(), Box<dyn std::error::Error>> {
        let mut ema = Ema::new(3)?;
        assert_eq!(ema.update(&bar(10, 1)?)?, None);
        assert_eq!(ema.update(&bar(14, 2)?)?, None);
        assert_eq!(ema.update(&bar(14, 3)?)?, Some(Decimal::from(13)));
        Ok(())
    }
}
