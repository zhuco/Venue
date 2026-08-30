use rust_decimal::Decimal;
use venue_domain::PublicBar;

use super::{ChartIndicatorError, validate_bar};

#[derive(Clone, Debug)]
pub struct Rsi {
    period: usize,
    period_decimal: Decimal,
    previous_close: Option<Decimal>,
    seed_gain: Decimal,
    seed_loss: Decimal,
    changes: usize,
    average_gain: Option<Decimal>,
    average_loss: Option<Decimal>,
}

impl Rsi {
    pub fn new(period: usize) -> Result<Self, ChartIndicatorError> {
        let period_decimal = decimal_period(period)?;
        Ok(Self {
            period,
            period_decimal,
            previous_close: None,
            seed_gain: Decimal::ZERO,
            seed_loss: Decimal::ZERO,
            changes: 0,
            average_gain: None,
            average_loss: None,
        })
    }

    pub fn update(&mut self, bar: &PublicBar) -> Result<Option<Decimal>, ChartIndicatorError> {
        validate_bar(bar)?;
        let mut next = self.clone();
        let value = next.update_close(bar.close.value())?;
        *self = next;
        Ok(value)
    }

    pub fn reset(&mut self) {
        self.previous_close = None;
        self.seed_gain = Decimal::ZERO;
        self.seed_loss = Decimal::ZERO;
        self.changes = 0;
        self.average_gain = None;
        self.average_loss = None;
    }

    fn update_close(&mut self, close: Decimal) -> Result<Option<Decimal>, ChartIndicatorError> {
        let Some(previous) = self.previous_close.replace(close) else {
            return Ok(None);
        };
        let change = close
            .checked_sub(previous)
            .ok_or(ChartIndicatorError::Arithmetic)?;
        let (gain, loss) = if change.is_sign_negative() {
            (
                Decimal::ZERO,
                Decimal::ZERO
                    .checked_sub(change)
                    .ok_or(ChartIndicatorError::Arithmetic)?,
            )
        } else {
            (change, Decimal::ZERO)
        };
        self.changes = self
            .changes
            .checked_add(1)
            .ok_or(ChartIndicatorError::Arithmetic)?;

        if self.changes <= self.period {
            self.seed_gain = self
                .seed_gain
                .checked_add(gain)
                .ok_or(ChartIndicatorError::Arithmetic)?;
            self.seed_loss = self
                .seed_loss
                .checked_add(loss)
                .ok_or(ChartIndicatorError::Arithmetic)?;
            if self.changes < self.period {
                return Ok(None);
            }
            self.average_gain = Some(
                self.seed_gain
                    .checked_div(self.period_decimal)
                    .ok_or(ChartIndicatorError::Arithmetic)?,
            );
            self.average_loss = Some(
                self.seed_loss
                    .checked_div(self.period_decimal)
                    .ok_or(ChartIndicatorError::Arithmetic)?,
            );
        } else {
            let multiplier = self
                .period_decimal
                .checked_sub(Decimal::ONE)
                .ok_or(ChartIndicatorError::Arithmetic)?;
            let previous_gain = self.average_gain.ok_or(ChartIndicatorError::Arithmetic)?;
            let previous_loss = self.average_loss.ok_or(ChartIndicatorError::Arithmetic)?;
            self.average_gain = Some(wilder_average(
                previous_gain,
                gain,
                multiplier,
                self.period_decimal,
            )?);
            self.average_loss = Some(wilder_average(
                previous_loss,
                loss,
                multiplier,
                self.period_decimal,
            )?);
        }

        rsi(
            self.average_gain.ok_or(ChartIndicatorError::Arithmetic)?,
            self.average_loss.ok_or(ChartIndicatorError::Arithmetic)?,
        )
        .map(Some)
    }
}

fn wilder_average(
    previous: Decimal,
    current: Decimal,
    multiplier: Decimal,
    divisor: Decimal,
) -> Result<Decimal, ChartIndicatorError> {
    previous
        .checked_mul(multiplier)
        .and_then(|value| value.checked_add(current))
        .and_then(|value| value.checked_div(divisor))
        .ok_or(ChartIndicatorError::Arithmetic)
}

fn rsi(average_gain: Decimal, average_loss: Decimal) -> Result<Decimal, ChartIndicatorError> {
    let hundred = Decimal::from(100_u32);
    if average_loss.is_zero() {
        return Ok(hundred);
    }
    if average_gain.is_zero() {
        return Ok(Decimal::ZERO);
    }
    let relative_strength = average_gain
        .checked_div(average_loss)
        .ok_or(ChartIndicatorError::Arithmetic)?;
    let denominator = Decimal::ONE
        .checked_add(relative_strength)
        .ok_or(ChartIndicatorError::Arithmetic)?;
    let adjustment = hundred
        .checked_div(denominator)
        .ok_or(ChartIndicatorError::Arithmetic)?;
    hundred
        .checked_sub(adjustment)
        .ok_or(ChartIndicatorError::Arithmetic)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MacdValue {
    pub macd: Decimal,
    pub signal: Decimal,
    pub histogram: Decimal,
}

#[derive(Clone, Debug)]
pub struct Macd {
    fast: DecimalEma,
    slow: DecimalEma,
    signal: DecimalEma,
}

impl Macd {
    pub fn new(
        fast_period: usize,
        slow_period: usize,
        signal_period: usize,
    ) -> Result<Self, ChartIndicatorError> {
        if fast_period == 0 || slow_period <= fast_period || signal_period == 0 {
            return Err(ChartIndicatorError::InvalidParameters);
        }
        Ok(Self {
            fast: DecimalEma::new(fast_period)?,
            slow: DecimalEma::new(slow_period)?,
            signal: DecimalEma::new(signal_period)?,
        })
    }

    pub fn update(&mut self, bar: &PublicBar) -> Result<Option<MacdValue>, ChartIndicatorError> {
        validate_bar(bar)?;
        let mut next = self.clone();
        let value = next.update_close(bar.close.value())?;
        *self = next;
        Ok(value)
    }

    pub fn reset(&mut self) {
        self.fast.reset();
        self.slow.reset();
        self.signal.reset();
    }

    fn update_close(&mut self, close: Decimal) -> Result<Option<MacdValue>, ChartIndicatorError> {
        let fast = self.fast.update(close)?;
        let slow = self.slow.update(close)?;
        let (Some(fast), Some(slow)) = (fast, slow) else {
            return Ok(None);
        };
        let macd = fast
            .checked_sub(slow)
            .ok_or(ChartIndicatorError::Arithmetic)?;
        let Some(signal) = self.signal.update(macd)? else {
            return Ok(None);
        };
        let histogram = macd
            .checked_sub(signal)
            .ok_or(ChartIndicatorError::Arithmetic)?;
        Ok(Some(MacdValue {
            macd,
            signal,
            histogram,
        }))
    }
}

#[derive(Clone, Debug)]
struct DecimalEma {
    period: usize,
    alpha: Decimal,
    samples: usize,
    value: Option<Decimal>,
}

impl DecimalEma {
    fn new(period: usize) -> Result<Self, ChartIndicatorError> {
        let period_decimal = decimal_period(period)?;
        let denominator = period_decimal
            .checked_add(Decimal::ONE)
            .ok_or(ChartIndicatorError::Arithmetic)?;
        let alpha = Decimal::TWO
            .checked_div(denominator)
            .ok_or(ChartIndicatorError::Arithmetic)?;
        Ok(Self {
            period,
            alpha,
            samples: 0,
            value: None,
        })
    }

    fn update(&mut self, input: Decimal) -> Result<Option<Decimal>, ChartIndicatorError> {
        self.samples = self
            .samples
            .checked_add(1)
            .ok_or(ChartIndicatorError::Arithmetic)?;
        let value = match self.value {
            Some(previous) => {
                let delta = input
                    .checked_sub(previous)
                    .ok_or(ChartIndicatorError::Arithmetic)?;
                let adjustment = self
                    .alpha
                    .checked_mul(delta)
                    .ok_or(ChartIndicatorError::Arithmetic)?;
                previous
                    .checked_add(adjustment)
                    .ok_or(ChartIndicatorError::Arithmetic)?
            }
            None => input,
        };
        self.value = Some(value);
        Ok((self.samples >= self.period).then_some(value))
    }

    fn reset(&mut self) {
        self.samples = 0;
        self.value = None;
    }
}

fn decimal_period(period: usize) -> Result<Decimal, ChartIndicatorError> {
    if period == 0 {
        return Err(ChartIndicatorError::InvalidParameters);
    }
    let period = u64::try_from(period).map_err(|_| ChartIndicatorError::InvalidParameters)?;
    Ok(Decimal::from(period))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rust_decimal::Decimal;
    use venue_domain::{FieldState, Price, PublicBar};

    use super::{Macd, Rsi};

    fn bar(sequence: u64, close: Decimal) -> Result<PublicBar, Box<dyn std::error::Error>> {
        let open_time_ms = sequence
            .checked_sub(1)
            .and_then(|value| value.checked_mul(60_000))
            .ok_or("time")?;
        let close_time_ms = open_time_ms.checked_add(59_999).ok_or("time")?;
        Ok(PublicBar {
            symbol: "BTC/USDT".parse()?,
            generation: 1,
            received_at_ms: close_time_ms.checked_add(1).ok_or("time")?,
            sequence,
            open_time_ms,
            close_time_ms,
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
    fn rsi_matches_wilder_reference_series() -> Result<(), Box<dyn std::error::Error>> {
        let closes = [
            "44.34", "44.09", "44.15", "43.61", "44.33", "44.83", "45.10", "45.42", "45.84",
            "46.08", "45.89", "46.03", "45.61", "46.28", "46.28", "46.00",
        ];
        let mut rsi = Rsi::new(14)?;
        let mut values = Vec::new();
        for (index, close) in closes.into_iter().enumerate() {
            let close = Decimal::from_str(close)?;
            if let Some(value) = rsi.update(&bar(u64::try_from(index + 1)?, close)?)? {
                values.push(value.round_dp(6));
            }
        }
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], Decimal::from_str("70.464135")?);
        assert_eq!(values[1], Decimal::from_str("66.249619")?);
        Ok(())
    }

    #[test]
    fn rsi_reset_discards_wilder_seed() -> Result<(), Box<dyn std::error::Error>> {
        let mut rsi = Rsi::new(2)?;
        assert!(rsi.update(&bar(1, Decimal::from(10))?)?.is_none());
        assert!(rsi.update(&bar(2, Decimal::from(11))?)?.is_none());
        assert_eq!(
            rsi.update(&bar(3, Decimal::from(12))?)?,
            Some(Decimal::from(100))
        );
        rsi.reset();
        assert!(rsi.update(&bar(1, Decimal::from(10))?)?.is_none());
        Ok(())
    }

    #[test]
    fn macd_uses_recursive_emas_and_signal_warmup() -> Result<(), Box<dyn std::error::Error>> {
        let mut macd = Macd::new(12, 26, 9)?;
        let mut output = None;
        for sequence in 1_u64..=34 {
            output = macd.update(&bar(sequence, Decimal::from(10))?)?;
        }
        let output = output.ok_or("MACD warmup")?;
        assert_eq!(output.macd.round_dp(6), Decimal::ZERO);
        assert_eq!(output.signal.round_dp(6), Decimal::ZERO);
        assert_eq!(output.histogram.round_dp(6), Decimal::ZERO);
        let output = macd
            .update(&bar(35, Decimal::from(20))?)?
            .ok_or("MACD output")?;
        assert_eq!(output.macd.round_dp(6), Decimal::from_str("0.797721")?);
        assert_eq!(output.signal.round_dp(6), Decimal::from_str("0.159544")?);
        assert_eq!(output.histogram.round_dp(6), Decimal::from_str("0.638177")?);
        Ok(())
    }

    #[test]
    fn rejects_invalid_periods() {
        assert!(Rsi::new(0).is_err());
        assert!(Macd::new(0, 26, 9).is_err());
        assert!(Macd::new(26, 12, 9).is_err());
        assert!(Macd::new(12, 26, 0).is_err());
    }
}
