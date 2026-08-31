//! Momentum and oscillator indicators.

use super::core::{
    Bar, IndicatorError, IndicatorResult, LegacyBarIndicator, Reset, Warmup, ensure_period,
};
use super::series::{Ema, Rma, RollingMax, RollingMean, RollingMin, Window};

/// Price momentum over a fixed lookback.
#[derive(Debug, Clone)]
pub struct Momentum {
    period: usize,
    closes: Window<f64>,
    samples: usize,
}

impl Momentum {
    /// Create a momentum indicator.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        Ok(Self {
            period,
            closes: Window::new(period + 1)?,
            samples: 0,
        })
    }
}

impl Reset for Momentum {
    fn reset(&mut self) {
        self.closes.clear();
        self.samples = 0;
    }
}

impl Warmup for Momentum {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period + 1
    }
}

impl LegacyBarIndicator for Momentum {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        self.closes.push(bar.close);
        if self.closes.is_full() {
            Some(bar.close - *self.closes.front()?)
        } else {
            None
        }
    }
}

/// Percentage rate of change.
#[derive(Debug, Clone)]
pub struct Roc {
    period: usize,
    closes: Window<f64>,
    samples: usize,
}

impl Roc {
    /// Create a rate-of-change indicator.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        Ok(Self {
            period,
            closes: Window::new(period + 1)?,
            samples: 0,
        })
    }
}

impl Reset for Roc {
    fn reset(&mut self) {
        self.closes.clear();
        self.samples = 0;
    }
}

impl Warmup for Roc {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period + 1
    }
}

impl LegacyBarIndicator for Roc {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        self.closes.push(bar.close);
        if !self.closes.is_full() {
            return None;
        }
        let oldest = *self.closes.front()?;
        Some(100.0 * (bar.close / oldest - 1.0))
    }
}

/// Wilder relative strength index.
#[derive(Debug, Clone)]
pub struct Rsi {
    period: usize,
    gains: Rma,
    losses: Rma,
    previous_close: Option<f64>,
    samples: usize,
}

impl Rsi {
    /// Create an RSI.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        Ok(Self {
            period,
            gains: Rma::new(period)?,
            losses: Rma::new(period)?,
            previous_close: None,
            samples: 0,
        })
    }
}

impl Reset for Rsi {
    fn reset(&mut self) {
        self.gains.reset();
        self.losses.reset();
        self.previous_close = None;
        self.samples = 0;
    }
}

impl Warmup for Rsi {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period + 1
    }
}

impl LegacyBarIndicator for Rsi {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let previous = self.previous_close.replace(bar.close)?;
        let change = bar.close - previous;
        let average_gain = self.gains.update(change.max(0.0));
        let average_loss = self.losses.update((-change).max(0.0));
        if !self.is_ready() {
            return None;
        }
        Some(if average_gain == 0.0 && average_loss == 0.0 {
            50.0
        } else if average_loss == 0.0 {
            100.0
        } else {
            let relative_strength = average_gain / average_loss;
            100.0 - 100.0 / (1.0 + relative_strength)
        })
    }
}

/// Stochastic oscillator output.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StochasticOutput {
    /// Fast %K.
    pub k: f64,
    /// Smoothed %D.
    pub d: f64,
}

/// Stochastic oscillator.
#[derive(Debug, Clone)]
pub struct Stochastic {
    k_period: usize,
    d_period: usize,
    highs: RollingMax,
    lows: RollingMin,
    d: RollingMean,
    samples: usize,
}

impl Stochastic {
    /// Create a stochastic oscillator.
    pub fn new(k_period: usize, d_period: usize) -> IndicatorResult<Self> {
        let k_period = ensure_period(k_period)?;
        let d_period = ensure_period(d_period)?;
        Ok(Self {
            k_period,
            d_period,
            highs: RollingMax::new(k_period)?,
            lows: RollingMin::new(k_period)?,
            d: RollingMean::new(d_period)?,
            samples: 0,
        })
    }
}

impl Reset for Stochastic {
    fn reset(&mut self) {
        self.highs.reset();
        self.lows.reset();
        self.d.reset();
        self.samples = 0;
    }
}

impl Warmup for Stochastic {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.k_period + self.d_period - 1
    }
}

impl LegacyBarIndicator for Stochastic {
    type Output = StochasticOutput;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let highest = self.highs.update(bar.high);
        let lowest = self.lows.update(bar.low);
        let k = highest.zip(lowest).map(|(highest, lowest)| {
            if highest == lowest {
                50.0
            } else {
                100.0 * (bar.close - lowest) / (highest - lowest)
            }
        });
        let d = k.and_then(|value| self.d.update(value));
        k.zip(d).map(|(k, d)| StochasticOutput { k, d })
    }
}

/// Stochastic RSI output.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StochRsiOutput {
    /// Stochastic RSI %K.
    pub k: f64,
    /// Smoothed %D.
    pub d: f64,
}

/// Stochastic RSI.
#[derive(Debug, Clone)]
pub struct StochRsi {
    rsi_period: usize,
    stoch_period: usize,
    d_period: usize,
    rsi: Rsi,
    highs: RollingMax,
    lows: RollingMin,
    d: RollingMean,
    samples: usize,
}

impl StochRsi {
    /// Create stochastic RSI.
    pub fn new(rsi_period: usize, stoch_period: usize, d_period: usize) -> IndicatorResult<Self> {
        let rsi_period = ensure_period(rsi_period)?;
        let stoch_period = ensure_period(stoch_period)?;
        let d_period = ensure_period(d_period)?;
        Ok(Self {
            rsi_period,
            stoch_period,
            d_period,
            rsi: Rsi::new(rsi_period)?,
            highs: RollingMax::new(stoch_period)?,
            lows: RollingMin::new(stoch_period)?,
            d: RollingMean::new(d_period)?,
            samples: 0,
        })
    }
}

impl Reset for StochRsi {
    fn reset(&mut self) {
        self.rsi.reset();
        self.highs.reset();
        self.lows.reset();
        self.d.reset();
        self.samples = 0;
    }
}

impl Warmup for StochRsi {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.rsi_period + self.stoch_period + self.d_period - 1
    }
}

impl LegacyBarIndicator for StochRsi {
    type Output = StochRsiOutput;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let rsi = self.rsi.update_legacy(bar)?;
        let highest = self.highs.update(rsi);
        let lowest = self.lows.update(rsi);
        let k = highest.zip(lowest).map(|(highest, lowest)| {
            if highest == lowest {
                50.0
            } else {
                100.0 * (rsi - lowest) / (highest - lowest)
            }
        });
        let d = k.and_then(|value| self.d.update(value));
        k.zip(d).map(|(k, d)| StochRsiOutput { k, d })
    }
}

/// Commodity channel index.
#[derive(Debug, Clone)]
pub struct Cci {
    period: usize,
    typical_prices: Window<f64>,
    samples: usize,
}

impl Cci {
    /// Create CCI.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        Ok(Self {
            period,
            typical_prices: Window::new(period)?,
            samples: 0,
        })
    }
}

impl Reset for Cci {
    fn reset(&mut self) {
        self.typical_prices.clear();
        self.samples = 0;
    }
}

impl Warmup for Cci {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period
    }
}

impl LegacyBarIndicator for Cci {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let current = bar.typical_price();
        self.typical_prices.push(current);
        if !self.typical_prices.is_full() {
            return None;
        }
        let mean = self.typical_prices.iter().sum::<f64>() / self.period as f64;
        let mean_deviation = self
            .typical_prices
            .iter()
            .map(|value| (value - mean).abs())
            .sum::<f64>()
            / self.period as f64;
        Some(if mean_deviation == 0.0 {
            0.0
        } else {
            (current - mean) / (0.015 * mean_deviation)
        })
    }
}

/// Williams %R oscillator.
#[derive(Debug, Clone)]
pub struct WilliamsR {
    highs: RollingMax,
    lows: RollingMin,
}

impl WilliamsR {
    /// Create Williams %R.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            highs: RollingMax::new(period)?,
            lows: RollingMin::new(period)?,
        })
    }
}

impl Reset for WilliamsR {
    fn reset(&mut self) {
        self.highs.reset();
        self.lows.reset();
    }
}

impl Warmup for WilliamsR {
    fn samples(&self) -> usize {
        self.highs.samples()
    }

    fn warmup_period(&self) -> usize {
        self.highs.warmup_period()
    }
}

impl LegacyBarIndicator for WilliamsR {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        let highest = self.highs.update(bar.high);
        let lowest = self.lows.update(bar.low);
        highest.zip(lowest).map(|(highest, lowest)| {
            if highest == lowest {
                -50.0
            } else {
                -100.0 * (highest - bar.close) / (highest - lowest)
            }
        })
    }
}

/// Money flow index.
#[derive(Debug, Clone)]
pub struct Mfi {
    period: usize,
    flows: Window<(f64, f64)>,
    previous_typical: Option<f64>,
    samples: usize,
}

impl Mfi {
    /// Create MFI.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        Ok(Self {
            period,
            flows: Window::new(period)?,
            previous_typical: None,
            samples: 0,
        })
    }
}

impl Reset for Mfi {
    fn reset(&mut self) {
        self.flows.clear();
        self.previous_typical = None;
        self.samples = 0;
    }
}

impl Warmup for Mfi {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period + 1
    }
}

impl LegacyBarIndicator for Mfi {
    type Output = f64;

    const REQUIRES_VOLUME: bool = true;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let typical = bar.typical_price();
        let previous = self.previous_typical.replace(typical)?;
        let raw_flow = typical * bar.volume;
        let flow = if typical > previous {
            (raw_flow, 0.0)
        } else if typical < previous {
            (0.0, raw_flow)
        } else {
            (0.0, 0.0)
        };
        self.flows.push(flow);
        if !self.flows.is_full() {
            return None;
        }
        let positive = self.flows.iter().map(|item| item.0).sum::<f64>();
        let negative = self.flows.iter().map(|item| item.1).sum::<f64>();
        Some(if positive == 0.0 && negative == 0.0 {
            50.0
        } else if negative == 0.0 {
            100.0
        } else {
            100.0 - 100.0 / (1.0 + positive / negative)
        })
    }
}

/// Moving-average convergence/divergence output.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MacdOutput {
    /// Difference between fast and slow EMAs.
    pub macd: f64,
    /// EMA of the MACD line.
    pub signal: f64,
    /// Difference between MACD and signal lines.
    pub histogram: f64,
}

/// Moving-average convergence/divergence oscillator.
#[derive(Debug, Clone)]
pub struct Macd {
    slow_period: usize,
    signal_period: usize,
    fast: Ema,
    slow: Ema,
    signal: Ema,
    samples: usize,
}

impl Macd {
    /// Create MACD. Common parameters are `(12, 26, 9)`.
    pub fn new(
        fast_period: usize,
        slow_period: usize,
        signal_period: usize,
    ) -> IndicatorResult<Self> {
        let fast_period = ensure_period(fast_period)?;
        let slow_period = ensure_period(slow_period)?;
        let signal_period = ensure_period(signal_period)?;
        if fast_period >= slow_period {
            return Err(IndicatorError::InvalidParameter {
                name: "fast_period",
                reason: "must be smaller than slow_period",
            });
        }
        Ok(Self {
            slow_period,
            signal_period,
            fast: Ema::new(fast_period)?,
            slow: Ema::new(slow_period)?,
            signal: Ema::new(signal_period)?,
            samples: 0,
        })
    }
}

impl Reset for Macd {
    fn reset(&mut self) {
        self.fast.reset();
        self.slow.reset();
        self.signal.reset();
        self.samples = 0;
    }
}

impl Warmup for Macd {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.slow_period
            .saturating_add(self.signal_period)
            .saturating_sub(1)
    }
}

impl LegacyBarIndicator for Macd {
    type Output = MacdOutput;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let macd = self.fast.update(bar.close) - self.slow.update(bar.close);
        let signal = self.signal.update(macd);
        self.is_ready().then_some(MacdOutput {
            macd,
            signal,
            histogram: macd - signal,
        })
    }
}

/// True strength index.
#[derive(Debug, Clone)]
pub struct Tsi {
    long_period: usize,
    short_period: usize,
    momentum_long: Ema,
    momentum_short: Ema,
    absolute_long: Ema,
    absolute_short: Ema,
    previous_close: Option<f64>,
    samples: usize,
}

impl Tsi {
    /// Create TSI. Common parameters are `(25, 13)`.
    pub fn new(long_period: usize, short_period: usize) -> IndicatorResult<Self> {
        let long_period = ensure_period(long_period)?;
        let short_period = ensure_period(short_period)?;
        if long_period < short_period {
            return Err(IndicatorError::InvalidParameter {
                name: "long_period",
                reason: "must be greater than or equal to short_period",
            });
        }
        Ok(Self {
            long_period,
            short_period,
            momentum_long: Ema::new(long_period)?,
            momentum_short: Ema::new(short_period)?,
            absolute_long: Ema::new(long_period)?,
            absolute_short: Ema::new(short_period)?,
            previous_close: None,
            samples: 0,
        })
    }
}

impl Reset for Tsi {
    fn reset(&mut self) {
        self.momentum_long.reset();
        self.momentum_short.reset();
        self.absolute_long.reset();
        self.absolute_short.reset();
        self.previous_close = None;
        self.samples = 0;
    }
}

impl Warmup for Tsi {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.long_period + self.short_period + 1
    }
}

impl LegacyBarIndicator for Tsi {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let previous = self.previous_close.replace(bar.close)?;
        let momentum = bar.close - previous;
        let smoothed_momentum = self
            .momentum_short
            .update(self.momentum_long.update(momentum));
        let smoothed_absolute = self
            .absolute_short
            .update(self.absolute_long.update(momentum.abs()));
        if !self.is_ready() {
            return None;
        }
        Some(if smoothed_absolute == 0.0 {
            0.0
        } else {
            100.0 * smoothed_momentum / smoothed_absolute
        })
    }
}

/// Difference between fast and slow simple averages of median price.
#[derive(Debug, Clone)]
pub struct AwesomeOscillator {
    slow_period: usize,
    fast: RollingMean,
    slow: RollingMean,
    samples: usize,
}

impl AwesomeOscillator {
    /// Create an Awesome Oscillator. Common parameters are `(5, 34)`.
    pub fn new(fast_period: usize, slow_period: usize) -> IndicatorResult<Self> {
        let fast_period = ensure_period(fast_period)?;
        let slow_period = ensure_period(slow_period)?;
        if fast_period >= slow_period {
            return Err(IndicatorError::InvalidParameter {
                name: "fast_period",
                reason: "must be smaller than slow_period",
            });
        }
        Ok(Self {
            slow_period,
            fast: RollingMean::new(fast_period)?,
            slow: RollingMean::new(slow_period)?,
            samples: 0,
        })
    }
}

impl Reset for AwesomeOscillator {
    fn reset(&mut self) {
        self.fast.reset();
        self.slow.reset();
        self.samples = 0;
    }
}

impl Warmup for AwesomeOscillator {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.slow_period
    }
}

impl LegacyBarIndicator for AwesomeOscillator {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let median = bar.median_price();
        self.fast
            .update(median)
            .zip(self.slow.update(median))
            .map(|(fast, slow)| fast - slow)
    }
}
