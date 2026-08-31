//! Volatility, range, and envelope indicators.

use super::core::{
    Bar, IndicatorError, IndicatorResult, LegacyBarIndicator, Reset, Warmup, ensure_period,
};
use super::series::{
    Ema, Rma, RollingMax, RollingMean, RollingMin, RollingStdDev as RollingStdDevCore, RollingSum,
};

/// True range.
#[derive(Debug, Clone, Copy, Default)]
pub struct TrueRange {
    previous_close: Option<f64>,
    samples: usize,
}

impl TrueRange {
    /// Create true range.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Reset for TrueRange {
    fn reset(&mut self) {
        self.previous_close = None;
        self.samples = 0;
    }
}

impl Warmup for TrueRange {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        1
    }
}

impl LegacyBarIndicator for TrueRange {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let previous = self.previous_close.replace(bar.close);
        Some(previous.map_or(bar.high - bar.low, |close| {
            (bar.high - bar.low)
                .max((bar.high - close).abs())
                .max((bar.low - close).abs())
        }))
    }
}

/// Average true range using Wilder smoothing.
#[derive(Debug, Clone)]
pub struct Atr {
    period: usize,
    true_range: TrueRange,
    average: Rma,
    samples: usize,
}

impl Atr {
    /// Create ATR.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        Ok(Self {
            period,
            true_range: TrueRange::new(),
            average: Rma::new(period)?,
            samples: 0,
        })
    }
}

impl Reset for Atr {
    fn reset(&mut self) {
        self.true_range.reset();
        self.average.reset();
        self.samples = 0;
    }
}

impl Warmup for Atr {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period
    }
}

impl LegacyBarIndicator for Atr {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let range = self.true_range.update_legacy(bar)?;
        let value = self.average.update(range);
        self.is_ready().then_some(value)
    }
}

/// Normalized ATR as a percentage of closing price.
#[derive(Debug, Clone)]
pub struct Natr {
    atr: Atr,
}

impl Natr {
    /// Create NATR.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            atr: Atr::new(period)?,
        })
    }
}

impl Reset for Natr {
    fn reset(&mut self) {
        self.atr.reset();
    }
}

impl Warmup for Natr {
    fn samples(&self) -> usize {
        self.atr.samples()
    }

    fn warmup_period(&self) -> usize {
        self.atr.warmup_period()
    }
}

impl LegacyBarIndicator for Natr {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.atr
            .update_legacy(bar)
            .map(|atr| 100.0 * atr / bar.close)
    }
}

/// Rolling population standard deviation of closing prices.
#[derive(Debug, Clone)]
pub struct RollingStdDev {
    inner: RollingStdDevCore,
}

impl RollingStdDev {
    /// Create rolling standard deviation.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            inner: RollingStdDevCore::new(period)?,
        })
    }
}

impl Reset for RollingStdDev {
    fn reset(&mut self) {
        self.inner.reset();
    }
}

impl Warmup for RollingStdDev {
    fn samples(&self) -> usize {
        self.inner.samples()
    }

    fn warmup_period(&self) -> usize {
        self.inner.warmup_period()
    }
}

impl LegacyBarIndicator for RollingStdDev {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.inner.update(bar.close)
    }
}

/// Annualized historical volatility of logarithmic returns.
#[derive(Debug, Clone)]
pub struct HistoricalVolatility {
    period: usize,
    annualization: f64,
    returns: RollingStdDevCore,
    previous_close: Option<f64>,
    samples: usize,
}

impl HistoricalVolatility {
    /// Create historical volatility. For daily bars, annualization is commonly 252.
    pub fn new(period: usize, annualization: f64) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        if !annualization.is_finite() || annualization <= 0.0 {
            return Err(IndicatorError::InvalidParameter {
                name: "annualization",
                reason: "must be finite and positive",
            });
        }
        Ok(Self {
            period,
            annualization,
            returns: RollingStdDevCore::new(period)?,
            previous_close: None,
            samples: 0,
        })
    }
}

impl Reset for HistoricalVolatility {
    fn reset(&mut self) {
        self.returns.reset();
        self.previous_close = None;
        self.samples = 0;
    }
}

impl Warmup for HistoricalVolatility {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period + 1
    }
}

impl LegacyBarIndicator for HistoricalVolatility {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let previous = self.previous_close.replace(bar.close)?;
        let log_return = (bar.close / previous).ln();
        self.returns
            .update(log_return)
            .map(|std_dev| std_dev * self.annualization.sqrt())
    }
}

/// Bollinger-band output.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BollingerOutput {
    /// Upper band.
    pub upper: f64,
    /// Rolling mean.
    pub middle: f64,
    /// Lower band.
    pub lower: f64,
    /// Population standard deviation.
    pub standard_deviation: f64,
    /// Relative width `(upper-lower)/middle`.
    pub bandwidth: f64,
    /// Position in the band, where 0 is lower and 1 is upper.
    pub percent_b: f64,
}

/// Bollinger bands.
#[derive(Debug, Clone)]
pub struct BollingerBands {
    mean: RollingMean,
    standard_deviation: RollingStdDevCore,
    multiplier: f64,
}

impl BollingerBands {
    /// Create Bollinger bands.
    pub fn new(period: usize, multiplier: f64) -> IndicatorResult<Self> {
        if !multiplier.is_finite() || multiplier <= 0.0 {
            return Err(IndicatorError::InvalidParameter {
                name: "multiplier",
                reason: "must be finite and positive",
            });
        }
        Ok(Self {
            mean: RollingMean::new(period)?,
            standard_deviation: RollingStdDevCore::new(period)?,
            multiplier,
        })
    }
}

impl Reset for BollingerBands {
    fn reset(&mut self) {
        self.mean.reset();
        self.standard_deviation.reset();
    }
}

impl Warmup for BollingerBands {
    fn samples(&self) -> usize {
        self.mean.samples()
    }

    fn warmup_period(&self) -> usize {
        self.mean.warmup_period()
    }
}

impl LegacyBarIndicator for BollingerBands {
    type Output = BollingerOutput;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        let middle = self.mean.update(bar.close);
        let std_dev = self.standard_deviation.update(bar.close);
        middle.zip(std_dev).map(|(middle, standard_deviation)| {
            let offset = self.multiplier * standard_deviation;
            let upper = middle + offset;
            let lower = middle - offset;
            let width = upper - lower;
            BollingerOutput {
                upper,
                middle,
                lower,
                standard_deviation,
                bandwidth: if middle == 0.0 { 0.0 } else { width / middle },
                percent_b: if width == 0.0 {
                    0.5
                } else {
                    (bar.close - lower) / width
                },
            }
        })
    }
}

/// Bollinger bandwidth as a standalone scalar.
#[derive(Debug, Clone)]
pub struct BollingerBandwidth {
    bands: BollingerBands,
}

impl BollingerBandwidth {
    /// Create Bollinger bandwidth.
    pub fn new(period: usize, multiplier: f64) -> IndicatorResult<Self> {
        Ok(Self {
            bands: BollingerBands::new(period, multiplier)?,
        })
    }
}

impl Reset for BollingerBandwidth {
    fn reset(&mut self) {
        self.bands.reset();
    }
}

impl Warmup for BollingerBandwidth {
    fn samples(&self) -> usize {
        self.bands.samples()
    }

    fn warmup_period(&self) -> usize {
        self.bands.warmup_period()
    }
}

impl LegacyBarIndicator for BollingerBandwidth {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.bands.update_legacy(bar).map(|output| output.bandwidth)
    }
}

/// Keltner-channel output.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct KeltnerOutput {
    /// Upper channel.
    pub upper: f64,
    /// EMA center line.
    pub middle: f64,
    /// Lower channel.
    pub lower: f64,
}

/// Keltner channel using EMA of typical price and Wilder ATR.
#[derive(Debug, Clone)]
pub struct KeltnerChannel {
    ema_period: usize,
    atr_period: usize,
    middle: Ema,
    atr: Atr,
    multiplier: f64,
    samples: usize,
}

impl KeltnerChannel {
    /// Create a Keltner channel.
    pub fn new(ema_period: usize, atr_period: usize, multiplier: f64) -> IndicatorResult<Self> {
        let ema_period = ensure_period(ema_period)?;
        let atr_period = ensure_period(atr_period)?;
        if !multiplier.is_finite() || multiplier <= 0.0 {
            return Err(IndicatorError::InvalidParameter {
                name: "multiplier",
                reason: "must be finite and positive",
            });
        }
        Ok(Self {
            ema_period,
            atr_period,
            middle: Ema::new(ema_period)?,
            atr: Atr::new(atr_period)?,
            multiplier,
            samples: 0,
        })
    }
}

impl Reset for KeltnerChannel {
    fn reset(&mut self) {
        self.middle.reset();
        self.atr.reset();
        self.samples = 0;
    }
}

impl Warmup for KeltnerChannel {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.ema_period.max(self.atr_period)
    }
}

impl LegacyBarIndicator for KeltnerChannel {
    type Output = KeltnerOutput;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let middle = self.middle.update(bar.typical_price());
        let atr = self.atr.update_legacy(bar);
        if !self.is_ready() {
            return None;
        }
        atr.map(|atr| KeltnerOutput {
            upper: middle + self.multiplier * atr,
            middle,
            lower: middle - self.multiplier * atr,
        })
    }
}

/// Choppiness index.
#[derive(Debug, Clone)]
pub struct ChoppinessIndex {
    period: usize,
    true_range: TrueRange,
    ranges: RollingSum,
    highs: RollingMax,
    lows: RollingMin,
    samples: usize,
}

impl ChoppinessIndex {
    /// Create the choppiness index. Period must be at least 2.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        if period < 2 {
            return Err(IndicatorError::InvalidParameter {
                name: "period",
                reason: "must be at least 2",
            });
        }
        Ok(Self {
            period,
            true_range: TrueRange::new(),
            ranges: RollingSum::new(period)?,
            highs: RollingMax::new(period)?,
            lows: RollingMin::new(period)?,
            samples: 0,
        })
    }
}

impl Reset for ChoppinessIndex {
    fn reset(&mut self) {
        self.true_range.reset();
        self.ranges.reset();
        self.highs.reset();
        self.lows.reset();
        self.samples = 0;
    }
}

impl Warmup for ChoppinessIndex {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period
    }
}

impl LegacyBarIndicator for ChoppinessIndex {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let range = self.true_range.update_legacy(bar)?;
        let sum = self.ranges.update(range);
        let high = self.highs.update(bar.high);
        let low = self.lows.update(bar.low);
        sum.zip(high.zip(low)).map(|(sum, (high, low))| {
            let span = high - low;
            if span <= 0.0 || sum <= 0.0 {
                0.0
            } else {
                (100.0 * (sum / span).log10() / (self.period as f64).log10()).clamp(0.0, 100.0)
            }
        })
    }
}
