use rust_decimal::Decimal;
use venue_domain::domain::Price;
use venue_indicators::chart::Ema;

use super::MmError;

/// EWMA absolute midpoint return in basis points; deliberately not an estimate of standard deviation.
/// The host samples on a fixed cadence and warms this transient market signal after a restart.
#[derive(Clone, Debug)]
pub struct MmVolatility {
    previous: Option<(u64, Price)>,
    average: Ema,
}

impl MmVolatility {
    pub fn new(period: usize) -> Result<Self, MmError> {
        if !(2..=10_000).contains(&period) {
            return Err(MmError::Config);
        }
        Ok(Self {
            previous: None,
            average: Ema::new(period).map_err(|_| MmError::Config)?,
        })
    }

    pub fn update(
        &mut self,
        observed_at_ms: u64,
        midpoint: Price,
    ) -> Result<Option<Decimal>, MmError> {
        if observed_at_ms == 0 || self.previous.is_some_and(|(at, _)| at >= observed_at_ms) {
            return Err(MmError::Facts);
        }
        let next = match self.previous {
            Some((_, previous)) => {
                let change = midpoint
                    .value()
                    .checked_sub(previous.value())
                    .ok_or(MmError::Arithmetic)?;
                let rate = change
                    .checked_div(previous.value())
                    .ok_or(MmError::Arithmetic)?;
                let bps = rate
                    .abs()
                    .checked_mul(Decimal::from(10_000))
                    .ok_or(MmError::Arithmetic)?;
                self.average
                    .update_value(bps)
                    .map_err(|_| MmError::Arithmetic)?
            }
            None => None,
        };
        self.previous = Some((observed_at_ms, midpoint));
        Ok(next)
    }
}
