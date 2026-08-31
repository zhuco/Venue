//! Trend-following averages and adaptive filters.

use super::core::{
    Bar, IndicatorError, IndicatorResult, LegacyBarIndicator, Reset, Warmup, ensure_period,
};
use super::series::{Ema as EmaCore, Rma as RmaCore, RollingMean, Window, Wma as WmaCore};

use super::volatility::Atr;

/// Percentage rate of change of a triple-smoothed EMA.
#[derive(Debug, Clone)]
pub struct Trix {
    period: usize,
    first: EmaCore,
    second: EmaCore,
    third: EmaCore,
    previous: Option<f64>,
    samples: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TrixOutput {
    pub line: f64,
    pub rate_of_change: f64,
}

impl Trix {
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        Ok(Self {
            period,
            first: EmaCore::new(period)?,
            second: EmaCore::new(period)?,
            third: EmaCore::new(period)?,
            previous: None,
            samples: 0,
        })
    }
}

impl Reset for Trix {
    fn reset(&mut self) {
        self.first.reset();
        self.second.reset();
        self.third.reset();
        self.previous = None;
        self.samples = 0;
    }
}

impl Warmup for Trix {
    fn samples(&self) -> usize {
        self.samples
    }
    fn warmup_period(&self) -> usize {
        self.period.saturating_mul(3).saturating_add(1)
    }
}

impl LegacyBarIndicator for Trix {
    type Output = TrixOutput;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let triple = self
            .third
            .update(self.second.update(self.first.update(bar.close)));
        let previous = self.previous.replace(triple)?;
        if !self.is_ready() {
            return None;
        }
        Some(TrixOutput {
            line: triple,
            rate_of_change: if previous == 0.0 {
                0.0
            } else {
                100.0 * (triple / previous - 1.0)
            },
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ParabolicSarOutput {
    pub value: f64,
    pub rising: bool,
}

/// Wilder parabolic stop-and-reverse with configurable acceleration.
#[derive(Debug, Clone)]
pub struct ParabolicSar {
    step: f64,
    maximum: f64,
    acceleration: f64,
    rising: bool,
    sar: Option<f64>,
    extreme: Option<f64>,
    previous_high: Option<f64>,
    previous_low: Option<f64>,
    samples: usize,
}

impl ParabolicSar {
    pub fn new(step: f64, maximum: f64) -> IndicatorResult<Self> {
        if !step.is_finite() || !maximum.is_finite() || step <= 0.0 || maximum < step {
            return Err(IndicatorError::InvalidParameter {
                name: "acceleration",
                reason: "step must be positive and maximum must be at least step",
            });
        }
        Ok(Self {
            step,
            maximum,
            acceleration: step,
            rising: true,
            sar: None,
            extreme: None,
            previous_high: None,
            previous_low: None,
            samples: 0,
        })
    }
}

impl Reset for ParabolicSar {
    fn reset(&mut self) {
        self.acceleration = self.step;
        self.rising = true;
        self.sar = None;
        self.extreme = None;
        self.previous_high = None;
        self.previous_low = None;
        self.samples = 0;
    }
}

impl Warmup for ParabolicSar {
    fn samples(&self) -> usize {
        self.samples
    }
    fn warmup_period(&self) -> usize {
        2
    }
}

impl LegacyBarIndicator for ParabolicSar {
    type Output = ParabolicSarOutput;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let Some(mut sar) = self.sar else {
            self.sar = Some(bar.low);
            self.extreme = Some(bar.high);
            self.previous_high = Some(bar.high);
            self.previous_low = Some(bar.low);
            return None;
        };
        let mut extreme = self.extreme?;
        sar += self.acceleration * (extreme - sar);
        if self.rising {
            sar = sar.min(self.previous_low.unwrap_or(bar.low));
            if bar.low < sar {
                self.rising = false;
                sar = extreme;
                extreme = bar.low;
                self.acceleration = self.step;
            } else if bar.high > extreme {
                extreme = bar.high;
                self.acceleration = (self.acceleration + self.step).min(self.maximum);
            }
        } else {
            sar = sar.max(self.previous_high.unwrap_or(bar.high));
            if bar.high > sar {
                self.rising = true;
                sar = extreme;
                extreme = bar.high;
                self.acceleration = self.step;
            } else if bar.low < extreme {
                extreme = bar.low;
                self.acceleration = (self.acceleration + self.step).min(self.maximum);
            }
        }
        self.sar = Some(sar);
        self.extreme = Some(extreme);
        self.previous_high = Some(bar.high);
        self.previous_low = Some(bar.low);
        Some(ParabolicSarOutput {
            value: sar,
            rising: self.rising,
        })
    }
}

/// Simple moving average of closing prices.
#[derive(Debug, Clone)]
pub struct Sma {
    mean: RollingMean,
}

impl Sma {
    /// Create an SMA.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            mean: RollingMean::new(period)?,
        })
    }
}

impl Reset for Sma {
    fn reset(&mut self) {
        self.mean.reset();
    }
}

impl Warmup for Sma {
    fn samples(&self) -> usize {
        self.mean.samples()
    }

    fn warmup_period(&self) -> usize {
        self.mean.warmup_period()
    }
}

impl LegacyBarIndicator for Sma {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.mean.update(bar.close)
    }
}

/// Exponential moving average of closing prices.
#[derive(Debug, Clone)]
pub struct Ema {
    ema: EmaCore,
}

impl Ema {
    /// Create an EMA.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            ema: EmaCore::new(period)?,
        })
    }
}

impl Reset for Ema {
    fn reset(&mut self) {
        self.ema.reset();
    }
}

impl Warmup for Ema {
    fn samples(&self) -> usize {
        self.ema.samples()
    }

    fn warmup_period(&self) -> usize {
        self.ema.warmup_period()
    }
}

impl LegacyBarIndicator for Ema {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        let value = self.ema.update(bar.close);
        self.is_ready().then_some(value)
    }
}

/// Wilder moving average (RMA/SMMA) of closing prices.
#[derive(Debug, Clone)]
pub struct Rma {
    rma: RmaCore,
}

impl Rma {
    /// Create an RMA.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            rma: RmaCore::new(period)?,
        })
    }
}

impl Reset for Rma {
    fn reset(&mut self) {
        self.rma.reset();
    }
}

impl Warmup for Rma {
    fn samples(&self) -> usize {
        self.rma.samples()
    }

    fn warmup_period(&self) -> usize {
        self.rma.warmup_period()
    }
}

impl LegacyBarIndicator for Rma {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        let value = self.rma.update(bar.close);
        self.is_ready().then_some(value)
    }
}

/// Linearly weighted moving average of closing prices.
#[derive(Debug, Clone)]
pub struct Wma {
    wma: WmaCore,
}

impl Wma {
    /// Create a WMA.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            wma: WmaCore::new(period)?,
        })
    }
}

impl Reset for Wma {
    fn reset(&mut self) {
        self.wma.reset();
    }
}

impl Warmup for Wma {
    fn samples(&self) -> usize {
        self.wma.samples()
    }

    fn warmup_period(&self) -> usize {
        self.wma.warmup_period()
    }
}

impl LegacyBarIndicator for Wma {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.wma.update(bar.close)
    }
}

/// Double exponential moving average.
#[derive(Debug, Clone)]
pub struct Dema {
    first: EmaCore,
    second: EmaCore,
    samples: usize,
    warmup: usize,
}

impl Dema {
    /// Create a DEMA.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        Ok(Self {
            first: EmaCore::new(period)?,
            second: EmaCore::new(period)?,
            samples: 0,
            warmup: period.saturating_mul(2),
        })
    }
}

impl Reset for Dema {
    fn reset(&mut self) {
        self.first.reset();
        self.second.reset();
        self.samples = 0;
    }
}

impl Warmup for Dema {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.warmup
    }
}

impl LegacyBarIndicator for Dema {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let first = self.first.update(bar.close);
        let second = self.second.update(first);
        self.is_ready().then_some(2.0 * first - second)
    }
}

/// Triple exponential moving average.
#[derive(Debug, Clone)]
pub struct Tema {
    first: EmaCore,
    second: EmaCore,
    third: EmaCore,
    samples: usize,
    warmup: usize,
}

impl Tema {
    /// Create a TEMA.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        Ok(Self {
            first: EmaCore::new(period)?,
            second: EmaCore::new(period)?,
            third: EmaCore::new(period)?,
            samples: 0,
            warmup: period.saturating_mul(3),
        })
    }
}

impl Reset for Tema {
    fn reset(&mut self) {
        self.first.reset();
        self.second.reset();
        self.third.reset();
        self.samples = 0;
    }
}

impl Warmup for Tema {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.warmup
    }
}

impl LegacyBarIndicator for Tema {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let first = self.first.update(bar.close);
        let second = self.second.update(first);
        let third = self.third.update(second);
        self.is_ready()
            .then_some(3.0 * first - 3.0 * second + third)
    }
}

/// Hull moving average.
#[derive(Debug, Clone)]
pub struct Hma {
    half: WmaCore,
    full: WmaCore,
    smooth: WmaCore,
    samples: usize,
    warmup: usize,
}

impl Hma {
    /// Create an HMA.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        let half = (period / 2).max(1);
        let sqrt = (period as f64).sqrt().round() as usize;
        Ok(Self {
            half: WmaCore::new(half)?,
            full: WmaCore::new(period)?,
            smooth: WmaCore::new(sqrt.max(1))?,
            samples: 0,
            warmup: period.saturating_add(sqrt.max(1)).saturating_sub(1),
        })
    }
}

impl Reset for Hma {
    fn reset(&mut self) {
        self.half.reset();
        self.full.reset();
        self.smooth.reset();
        self.samples = 0;
    }
}

impl Warmup for Hma {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.warmup
    }
}

impl LegacyBarIndicator for Hma {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let half = self.half.update(bar.close);
        let full = self.full.update(bar.close);
        let raw = half.zip(full).map(|(half, full)| 2.0 * half - full);
        let output = raw.and_then(|value| self.smooth.update(value));
        output.filter(|_| self.is_ready())
    }
}

/// Kaufman's adaptive moving average.
#[derive(Debug, Clone)]
pub struct Kama {
    period: usize,
    prices: Window<f64>,
    fast_alpha: f64,
    slow_alpha: f64,
    value: Option<f64>,
    samples: usize,
}

impl Kama {
    /// Create KAMA with standard fast=2 and slow=30 smoothing periods.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Self::with_smoothing(period, 2, 30)
    }

    /// Create KAMA with explicit fast and slow smoothing periods.
    pub fn with_smoothing(period: usize, fast: usize, slow: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        let fast = ensure_period(fast)?;
        let slow = ensure_period(slow)?;
        if fast >= slow {
            return Err(IndicatorError::InvalidParameter {
                name: "fast",
                reason: "must be smaller than slow",
            });
        }
        Ok(Self {
            period,
            prices: Window::new(period + 1)?,
            fast_alpha: 2.0 / (fast as f64 + 1.0),
            slow_alpha: 2.0 / (slow as f64 + 1.0),
            value: None,
            samples: 0,
        })
    }
}

impl Reset for Kama {
    fn reset(&mut self) {
        self.prices.clear();
        self.value = None;
        self.samples = 0;
    }
}

impl Warmup for Kama {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period + 1
    }
}

impl LegacyBarIndicator for Kama {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        self.prices.push(bar.close);
        let previous = self.value.unwrap_or(bar.close);
        if !self.prices.is_full() {
            self.value = Some(previous);
            return None;
        }
        let oldest = *self.prices.front()?;
        let change = (bar.close - oldest).abs();
        let volatility = self
            .prices
            .iter()
            .zip(self.prices.iter().skip(1))
            .map(|(left, right)| (right - left).abs())
            .sum::<f64>();
        let efficiency = if volatility == 0.0 {
            0.0
        } else {
            change / volatility
        };
        let smoothing =
            (efficiency * (self.fast_alpha - self.slow_alpha) + self.slow_alpha).powi(2);
        let next = previous + smoothing * (bar.close - previous);
        self.value = Some(next);
        Some(next)
    }
}

/// Zero-lag exponential moving average.
#[derive(Debug, Clone)]
pub struct Zlema {
    lag: usize,
    prices: Window<f64>,
    ema: EmaCore,
    samples: usize,
    warmup: usize,
}

impl Zlema {
    /// Create a ZLEMA.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        let lag = period.saturating_sub(1) / 2;
        Ok(Self {
            lag,
            prices: Window::new(lag + 1)?,
            ema: EmaCore::new(period)?,
            samples: 0,
            warmup: period.saturating_add(lag),
        })
    }
}

impl Reset for Zlema {
    fn reset(&mut self) {
        self.prices.clear();
        self.ema.reset();
        self.samples = 0;
    }
}

impl Warmup for Zlema {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.warmup
    }
}

impl LegacyBarIndicator for Zlema {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        self.prices.push(bar.close);
        if self.prices.len() <= self.lag {
            return None;
        }
        let lagged = *self.prices.front()?;
        let adjusted = bar.close + (bar.close - lagged);
        let value = self.ema.update(adjusted);
        self.is_ready().then_some(value)
    }
}

/// Aroon trend-timing output.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AroonOutput {
    /// Recency of the highest high, scaled to `[0, 100]`.
    pub up: f64,
    /// Recency of the lowest low, scaled to `[0, 100]`.
    pub down: f64,
    /// Difference `up - down`, in `[-100, 100]`.
    pub oscillator: f64,
}

/// Aroon indicator over `period + 1` bars.
///
/// Equal extrema use the most recent occurrence.
#[derive(Debug, Clone)]
pub struct Aroon {
    period: usize,
    highs: Window<f64>,
    lows: Window<f64>,
    samples: usize,
}

impl Aroon {
    /// Create an Aroon indicator.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        let window = period
            .checked_add(1)
            .ok_or(IndicatorError::InvalidParameter {
                name: "period",
                reason: "is too large",
            })?;
        Ok(Self {
            period,
            highs: Window::new(window)?,
            lows: Window::new(window)?,
            samples: 0,
        })
    }

    fn newest_high_index(&self) -> usize {
        let mut extreme = f64::NEG_INFINITY;
        let mut newest = 0;
        for (index, value) in self.highs.iter().copied().enumerate() {
            if value >= extreme {
                extreme = value;
                newest = index;
            }
        }
        newest
    }

    fn newest_low_index(&self) -> usize {
        let mut extreme = f64::INFINITY;
        let mut newest = 0;
        for (index, value) in self.lows.iter().copied().enumerate() {
            if value <= extreme {
                extreme = value;
                newest = index;
            }
        }
        newest
    }
}

impl Reset for Aroon {
    fn reset(&mut self) {
        self.highs.clear();
        self.lows.clear();
        self.samples = 0;
    }
}

impl Warmup for Aroon {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period.saturating_add(1)
    }
}

impl LegacyBarIndicator for Aroon {
    type Output = AroonOutput;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        self.highs.push(bar.high);
        self.lows.push(bar.low);
        if !self.highs.is_full() {
            return None;
        }
        let scale = 100.0 / self.period as f64;
        let up = self.newest_high_index() as f64 * scale;
        let down = self.newest_low_index() as f64 * scale;
        Some(AroonOutput {
            up,
            down,
            oscillator: up - down,
        })
    }
}

/// Directional movement and average directional index output.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DmiOutput {
    /// Positive directional indicator.
    pub plus_di: f64,
    /// Negative directional indicator.
    pub minus_di: f64,
    /// Average directional index.
    pub adx: f64,
}

/// Wilder directional movement index and ADX.
#[derive(Debug, Clone)]
pub struct Dmi {
    period: usize,
    true_range: RmaCore,
    plus_movement: RmaCore,
    minus_movement: RmaCore,
    adx: RmaCore,
    previous: Option<(f64, f64, f64)>,
    samples: usize,
}

impl Dmi {
    /// Create DMI/ADX.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        Ok(Self {
            period,
            true_range: RmaCore::new(period)?,
            plus_movement: RmaCore::new(period)?,
            minus_movement: RmaCore::new(period)?,
            adx: RmaCore::new(period)?,
            previous: None,
            samples: 0,
        })
    }
}

impl Reset for Dmi {
    fn reset(&mut self) {
        self.true_range.reset();
        self.plus_movement.reset();
        self.minus_movement.reset();
        self.adx.reset();
        self.previous = None;
        self.samples = 0;
    }
}

impl Warmup for Dmi {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period.saturating_mul(2)
    }
}

impl LegacyBarIndicator for Dmi {
    type Output = DmiOutput;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let previous = self.previous.replace((bar.high, bar.low, bar.close))?;
        let (previous_high, previous_low, previous_close) = previous;
        let upward = bar.high - previous_high;
        let downward = previous_low - bar.low;
        let plus_movement = if upward > downward && upward > 0.0 {
            upward
        } else {
            0.0
        };
        let minus_movement = if downward > upward && downward > 0.0 {
            downward
        } else {
            0.0
        };
        let true_range = (bar.high - bar.low)
            .max((bar.high - previous_close).abs())
            .max((bar.low - previous_close).abs());
        let average_range = self.true_range.update(true_range);
        let average_plus = self.plus_movement.update(plus_movement);
        let average_minus = self.minus_movement.update(minus_movement);
        if !self.true_range.is_ready() {
            return None;
        }
        let (plus_di, minus_di) = if average_range == 0.0 {
            (0.0, 0.0)
        } else {
            (
                100.0 * average_plus / average_range,
                100.0 * average_minus / average_range,
            )
        };
        let total = plus_di + minus_di;
        let directional_index = if total == 0.0 {
            0.0
        } else {
            100.0 * (plus_di - minus_di).abs() / total
        };
        let adx = self.adx.update(directional_index);
        self.adx.is_ready().then_some(DmiOutput {
            plus_di,
            minus_di,
            adx,
        })
    }
}

/// `SuperTrend` output.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SuperTrendOutput {
    /// Active trailing line.
    pub value: f64,
    /// `1` for bullish trend and `-1` for bearish trend.
    pub direction: i8,
    /// Final upper band.
    pub upper: f64,
    /// Final lower band.
    pub lower: f64,
}

/// ATR-based `SuperTrend`.
#[derive(Debug, Clone)]
pub struct SuperTrend {
    atr: Atr,
    multiplier: f64,
    previous_close: Option<f64>,
    final_upper: Option<f64>,
    final_lower: Option<f64>,
    direction: i8,
    samples: usize,
}

impl SuperTrend {
    /// Create a `SuperTrend`.
    pub fn new(period: usize, multiplier: f64) -> IndicatorResult<Self> {
        if !multiplier.is_finite() || multiplier <= 0.0 {
            return Err(IndicatorError::InvalidParameter {
                name: "multiplier",
                reason: "must be finite and positive",
            });
        }
        Ok(Self {
            atr: Atr::new(period)?,
            multiplier,
            previous_close: None,
            final_upper: None,
            final_lower: None,
            direction: 1,
            samples: 0,
        })
    }
}

impl Reset for SuperTrend {
    fn reset(&mut self) {
        self.atr.reset();
        self.previous_close = None;
        self.final_upper = None;
        self.final_lower = None;
        self.direction = 1;
        self.samples = 0;
    }
}

impl Warmup for SuperTrend {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.atr.warmup_period()
    }
}

impl LegacyBarIndicator for SuperTrend {
    type Output = SuperTrendOutput;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let atr = self.atr.update_legacy(bar);
        let previous_close = self.previous_close.replace(bar.close);
        let atr = atr?;
        let midpoint = bar.median_price();
        let basic_upper = midpoint + self.multiplier * atr;
        let basic_lower = midpoint - self.multiplier * atr;

        let prior_upper = self.final_upper.unwrap_or(basic_upper);
        let prior_lower = self.final_lower.unwrap_or(basic_lower);
        let reference_close = previous_close.unwrap_or(bar.close);
        let final_upper = if basic_upper < prior_upper || reference_close > prior_upper {
            basic_upper
        } else {
            prior_upper
        };
        let final_lower = if basic_lower > prior_lower || reference_close < prior_lower {
            basic_lower
        } else {
            prior_lower
        };

        if self.direction > 0 && bar.close < final_lower {
            self.direction = -1;
        } else if self.direction < 0 && bar.close > final_upper {
            self.direction = 1;
        }
        self.final_upper = Some(final_upper);
        self.final_lower = Some(final_lower);
        Some(SuperTrendOutput {
            value: if self.direction > 0 {
                final_lower
            } else {
                final_upper
            },
            direction: self.direction,
            upper: final_upper,
            lower: final_lower,
        })
    }
}
