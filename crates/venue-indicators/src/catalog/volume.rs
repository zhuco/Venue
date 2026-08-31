//! Volume and money-flow indicators.

use super::core::{
    Bar, IndicatorError, IndicatorResult, LegacyBarIndicator, Reset, Warmup, ensure_period,
};
use super::series::{Ema, RollingSum};

/// Cumulative exchange-reported quote amount divided by cumulative base amount.
#[derive(Debug, Clone, Copy, Default)]
pub struct AverageValueLine {
    base_sum: f64,
    quote_sum: f64,
    samples: usize,
}

impl AverageValueLine {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Reset for AverageValueLine {
    fn reset(&mut self) {
        self.base_sum = 0.0;
        self.quote_sum = 0.0;
        self.samples = 0;
    }
}

impl Warmup for AverageValueLine {
    fn samples(&self) -> usize {
        self.samples
    }
    fn warmup_period(&self) -> usize {
        1
    }
}

impl LegacyBarIndicator for AverageValueLine {
    type Output = f64;
    const REQUIRES_VOLUME: bool = true;
    const REQUIRES_QUOTE_VOLUME: bool = true;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        self.base_sum += bar.volume;
        self.quote_sum += bar.quote_volume;
        (self.base_sum > 0.0).then_some(self.quote_sum / self.base_sum)
    }
}

/// Ease of movement smoothed over a rolling period.
#[derive(Debug, Clone)]
pub struct EaseOfMovement {
    average: super::series::RollingMean,
    previous_midpoint: Option<f64>,
    samples: usize,
}

impl EaseOfMovement {
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            average: super::series::RollingMean::new(period)?,
            previous_midpoint: None,
            samples: 0,
        })
    }
}

impl Reset for EaseOfMovement {
    fn reset(&mut self) {
        self.average.reset();
        self.previous_midpoint = None;
        self.samples = 0;
    }
}

impl Warmup for EaseOfMovement {
    fn samples(&self) -> usize {
        self.samples
    }
    fn warmup_period(&self) -> usize {
        self.average.warmup_period().saturating_add(1)
    }
}

impl LegacyBarIndicator for EaseOfMovement {
    type Output = f64;
    const REQUIRES_VOLUME: bool = true;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let midpoint = bar.median_price();
        let previous = self.previous_midpoint.replace(midpoint)?;
        let range = bar.high - bar.low;
        let raw = if bar.volume <= 0.0 {
            0.0
        } else {
            (midpoint - previous) * range / bar.volume
        };
        self.average.update(raw)
    }
}

/// On-balance volume.
#[derive(Debug, Clone, Copy, Default)]
pub struct Obv {
    previous_close: Option<f64>,
    value: f64,
    samples: usize,
}

impl Obv {
    /// Create OBV.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Reset for Obv {
    fn reset(&mut self) {
        self.previous_close = None;
        self.value = 0.0;
        self.samples = 0;
    }
}

impl Warmup for Obv {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        1
    }
}

impl LegacyBarIndicator for Obv {
    type Output = f64;

    const REQUIRES_VOLUME: bool = true;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        if let Some(previous) = self.previous_close {
            if bar.close > previous {
                self.value += bar.volume;
            } else if bar.close < previous {
                self.value -= bar.volume;
            }
        }
        self.previous_close = Some(bar.close);
        Some(self.value)
    }
}

/// Accumulation/distribution line.
#[derive(Debug, Clone, Copy, Default)]
pub struct Adl {
    value: f64,
    samples: usize,
}

impl Adl {
    /// Create ADL.
    pub fn new() -> Self {
        Self::default()
    }

    fn money_flow_volume(bar: &Bar) -> f64 {
        let range = bar.high - bar.low;
        if range == 0.0 {
            0.0
        } else {
            let multiplier = ((bar.close - bar.low) - (bar.high - bar.close)) / range;
            multiplier * bar.volume
        }
    }
}

impl Reset for Adl {
    fn reset(&mut self) {
        self.value = 0.0;
        self.samples = 0;
    }
}

impl Warmup for Adl {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        1
    }
}

impl LegacyBarIndicator for Adl {
    type Output = f64;

    const REQUIRES_VOLUME: bool = true;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        self.value += Self::money_flow_volume(bar);
        Some(self.value)
    }
}

/// Chaikin money flow.
#[derive(Debug, Clone)]
pub struct Cmf {
    money_flow: RollingSum,
    volume: RollingSum,
}

impl Cmf {
    /// Create CMF.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            money_flow: RollingSum::new(period)?,
            volume: RollingSum::new(period)?,
        })
    }
}

impl Reset for Cmf {
    fn reset(&mut self) {
        self.money_flow.reset();
        self.volume.reset();
    }
}

impl Warmup for Cmf {
    fn samples(&self) -> usize {
        self.volume.samples()
    }

    fn warmup_period(&self) -> usize {
        self.volume.warmup_period()
    }
}

impl LegacyBarIndicator for Cmf {
    type Output = f64;

    const REQUIRES_VOLUME: bool = true;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        let money_flow = self.money_flow.update(Adl::money_flow_volume(bar));
        let volume = self.volume.update(bar.volume);
        money_flow.zip(volume).map(|(money_flow, volume)| {
            if volume == 0.0 {
                0.0
            } else {
                money_flow / volume
            }
        })
    }
}

/// Chaikin oscillator, the difference between fast and slow EMA of ADL.
#[derive(Debug, Clone)]
pub struct ChaikinOscillator {
    slow_period: usize,
    adl: Adl,
    fast: Ema,
    slow: Ema,
    samples: usize,
}

impl ChaikinOscillator {
    /// Create Chaikin oscillator. Common parameters are `(3, 10)`.
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
            adl: Adl::new(),
            fast: Ema::new(fast_period)?,
            slow: Ema::new(slow_period)?,
            samples: 0,
        })
    }
}

impl Reset for ChaikinOscillator {
    fn reset(&mut self) {
        self.adl.reset();
        self.fast.reset();
        self.slow.reset();
        self.samples = 0;
    }
}

impl Warmup for ChaikinOscillator {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.slow_period
    }
}

impl LegacyBarIndicator for ChaikinOscillator {
    type Output = f64;

    const REQUIRES_VOLUME: bool = true;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let line = self.adl.update_legacy(bar)?;
        let fast = self.fast.update(line);
        let slow = self.slow.update(line);
        self.is_ready().then_some(fast - slow)
    }
}

/// Price-volume trend.
#[derive(Debug, Clone, Copy, Default)]
pub struct Pvt {
    previous_close: Option<f64>,
    value: f64,
    samples: usize,
}

impl Pvt {
    /// Create PVT.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Reset for Pvt {
    fn reset(&mut self) {
        self.previous_close = None;
        self.value = 0.0;
        self.samples = 0;
    }
}

impl Warmup for Pvt {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        2
    }
}

impl LegacyBarIndicator for Pvt {
    type Output = f64;

    const REQUIRES_VOLUME: bool = true;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let previous = self.previous_close.replace(bar.close)?;
        self.value += (bar.close - previous) / previous * bar.volume;
        Some(self.value)
    }
}

/// Session or anchored volume-weighted average price.
#[derive(Debug, Clone, Copy, Default)]
pub struct Vwap {
    weighted_sum: f64,
    volume_sum: f64,
    samples: usize,
}

impl Vwap {
    /// Create VWAP. Call [`Reset::reset`] at a session or custom anchor boundary.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Reset for Vwap {
    fn reset(&mut self) {
        self.weighted_sum = 0.0;
        self.volume_sum = 0.0;
        self.samples = 0;
    }
}

impl Warmup for Vwap {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        1
    }
}

impl LegacyBarIndicator for Vwap {
    type Output = f64;

    const REQUIRES_VOLUME: bool = true;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        self.weighted_sum += bar.typical_price() * bar.volume;
        self.volume_sum += bar.volume;
        (self.volume_sum > 0.0).then_some(self.weighted_sum / self.volume_sum)
    }
}

/// Rolling volume-weighted moving average of closing price.
#[derive(Debug, Clone)]
pub struct Vwma {
    weighted_close: RollingSum,
    volume: RollingSum,
}

impl Vwma {
    /// Create a rolling VWMA.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            weighted_close: RollingSum::new(period)?,
            volume: RollingSum::new(period)?,
        })
    }
}

impl Reset for Vwma {
    fn reset(&mut self) {
        self.weighted_close.reset();
        self.volume.reset();
    }
}

impl Warmup for Vwma {
    fn samples(&self) -> usize {
        self.volume.samples()
    }

    fn warmup_period(&self) -> usize {
        self.volume.warmup_period()
    }
}

impl LegacyBarIndicator for Vwma {
    type Output = f64;

    const REQUIRES_VOLUME: bool = true;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        let weighted_close = self.weighted_close.update(bar.close * bar.volume);
        let volume = self.volume.update(bar.volume);
        weighted_close
            .zip(volume)
            .and_then(|(weighted_close, volume)| (volume > 0.0).then_some(weighted_close / volume))
    }
}

/// EMA-smoothed force index.
#[derive(Debug, Clone)]
pub struct ForceIndex {
    period: usize,
    average: Ema,
    previous_close: Option<f64>,
    samples: usize,
}

impl ForceIndex {
    /// Create force index.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        Ok(Self {
            period,
            average: Ema::new(period)?,
            previous_close: None,
            samples: 0,
        })
    }
}

impl Reset for ForceIndex {
    fn reset(&mut self) {
        self.average.reset();
        self.previous_close = None;
        self.samples = 0;
    }
}

impl Warmup for ForceIndex {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period + 1
    }
}

impl LegacyBarIndicator for ForceIndex {
    type Output = f64;

    const REQUIRES_VOLUME: bool = true;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let previous = self.previous_close.replace(bar.close)?;
        let value = self.average.update((bar.close - previous) * bar.volume);
        self.is_ready().then_some(value)
    }
}

/// Percentage difference between fast and slow EMA of volume.
#[derive(Debug, Clone)]
pub struct VolumeOscillator {
    slow_period: usize,
    fast: Ema,
    slow: Ema,
    samples: usize,
}

impl VolumeOscillator {
    /// Create volume oscillator.
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
            fast: Ema::new(fast_period)?,
            slow: Ema::new(slow_period)?,
            samples: 0,
        })
    }
}

impl Reset for VolumeOscillator {
    fn reset(&mut self) {
        self.fast.reset();
        self.slow.reset();
        self.samples = 0;
    }
}

impl Warmup for VolumeOscillator {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.slow_period
    }
}

impl LegacyBarIndicator for VolumeOscillator {
    type Output = f64;

    const REQUIRES_VOLUME: bool = true;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let fast = self.fast.update(bar.volume);
        let slow = self.slow.update(bar.volume);
        if !self.is_ready() {
            return None;
        }
        Some(if slow == 0.0 {
            0.0
        } else {
            100.0 * (fast - slow) / slow
        })
    }
}
