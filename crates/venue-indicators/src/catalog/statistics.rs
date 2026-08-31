//! Statistical and market-regime indicators.

use super::core::{
    Bar, IndicatorError, IndicatorResult, LegacyBarIndicator, LegacyScalarPairIndicator, Reset,
    Warmup, ensure_period,
};
use super::series::{LinearRegressionWindow, RegressionOutput, Window};

fn population_moments(values: impl Iterator<Item = f64>) -> (f64, f64) {
    let mut count = 0.0;
    let mut mean = 0.0;
    let mut squared_deviation = 0.0;
    for value in values {
        count += 1.0;
        let delta = value - mean;
        mean += delta / count;
        squared_deviation += delta * (value - mean);
    }
    let variance = if count == 0.0 {
        0.0
    } else {
        squared_deviation / count
    };
    (mean, variance.max(0.0))
}

/// Rolling z-score of closing price.
#[derive(Debug, Clone)]
pub struct ZScore {
    period: usize,
    values: Window<f64>,
    samples: usize,
}

impl ZScore {
    /// Create a z-score.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        Ok(Self {
            period,
            values: Window::new(period)?,
            samples: 0,
        })
    }
}

impl Reset for ZScore {
    fn reset(&mut self) {
        self.values.clear();
        self.samples = 0;
    }
}

impl Warmup for ZScore {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period
    }
}

impl LegacyBarIndicator for ZScore {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        self.values.push(bar.close);
        if !self.values.is_full() {
            return None;
        }
        let (mean, variance) = population_moments(self.values.iter().copied());
        let standard_deviation = variance.sqrt();
        Some(if standard_deviation == 0.0 {
            0.0
        } else {
            (bar.close - mean) / standard_deviation
        })
    }
}

/// Least-squares regression of closing price against sample index.
#[derive(Debug, Clone)]
pub struct LinearRegression {
    inner: LinearRegressionWindow,
}

impl LinearRegression {
    /// Create rolling linear regression.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            inner: LinearRegressionWindow::new(period)?,
        })
    }
}

impl Reset for LinearRegression {
    fn reset(&mut self) {
        self.inner.reset();
    }
}

impl Warmup for LinearRegression {
    fn samples(&self) -> usize {
        self.inner.samples()
    }

    fn warmup_period(&self) -> usize {
        self.inner.warmup_period()
    }
}

impl LegacyBarIndicator for LinearRegression {
    type Output = RegressionOutput;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.inner.update(bar.close)
    }
}

/// Rolling Pearson correlation for paired scalar streams.
#[derive(Debug, Clone)]
pub struct PearsonCorrelation {
    period: usize,
    values: Window<(f64, f64)>,
    samples: usize,
}

impl PearsonCorrelation {
    /// Create paired-stream Pearson correlation.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        Ok(Self {
            period,
            values: Window::new(period)?,
            samples: 0,
        })
    }
}

impl LegacyScalarPairIndicator for PearsonCorrelation {
    type Output = f64;

    fn update_legacy(&mut self, x: f64, y: f64) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        self.values.push((x, y));
        if !self.values.is_full() {
            return None;
        }
        let mean_x = self.values.iter().map(|pair| pair.0).sum::<f64>() / self.period as f64;
        let mean_y = self.values.iter().map(|pair| pair.1).sum::<f64>() / self.period as f64;
        let mut covariance = 0.0;
        let mut variance_x = 0.0;
        let mut variance_y = 0.0;
        for (x, y) in self.values.iter().copied() {
            let dx = x - mean_x;
            let dy = y - mean_y;
            covariance += dx * dy;
            variance_x += dx * dx;
            variance_y += dy * dy;
        }
        let denominator = (variance_x * variance_y).sqrt();
        Some(if denominator == 0.0 {
            0.0
        } else {
            (covariance / denominator).clamp(-1.0, 1.0)
        })
    }
}

impl Reset for PearsonCorrelation {
    fn reset(&mut self) {
        self.values.clear();
        self.samples = 0;
    }
}

impl Warmup for PearsonCorrelation {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period
    }
}

/// Kaufman efficiency ratio of closing prices.
#[derive(Debug, Clone)]
pub struct EfficiencyRatio {
    period: usize,
    values: Window<f64>,
    samples: usize,
}

impl EfficiencyRatio {
    /// Create efficiency ratio.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        Ok(Self {
            period,
            values: Window::new(period + 1)?,
            samples: 0,
        })
    }
}

impl Reset for EfficiencyRatio {
    fn reset(&mut self) {
        self.values.clear();
        self.samples = 0;
    }
}

impl Warmup for EfficiencyRatio {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period + 1
    }
}

impl LegacyBarIndicator for EfficiencyRatio {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        self.values.push(bar.close);
        if !self.values.is_full() {
            return None;
        }
        let oldest = *self.values.front()?;
        let change = (bar.close - oldest).abs();
        let volatility = self
            .values
            .iter()
            .zip(self.values.iter().skip(1))
            .map(|(left, right)| (right - left).abs())
            .sum::<f64>();
        Some(if volatility == 0.0 {
            0.0
        } else {
            (change / volatility).clamp(0.0, 1.0)
        })
    }
}

/// Mean absolute deviation from the rolling arithmetic mean.
#[derive(Debug, Clone)]
pub struct MeanAbsoluteDeviation {
    period: usize,
    values: Window<f64>,
    samples: usize,
}

impl MeanAbsoluteDeviation {
    /// Create mean absolute deviation.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        Ok(Self {
            period,
            values: Window::new(period)?,
            samples: 0,
        })
    }
}

impl Reset for MeanAbsoluteDeviation {
    fn reset(&mut self) {
        self.values.clear();
        self.samples = 0;
    }
}

impl Warmup for MeanAbsoluteDeviation {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period
    }
}

impl LegacyBarIndicator for MeanAbsoluteDeviation {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        self.values.push(bar.close);
        if !self.values.is_full() {
            return None;
        }
        let mean = self.values.iter().sum::<f64>() / self.period as f64;
        Some(
            self.values
                .iter()
                .map(|value| (value - mean).abs())
                .sum::<f64>()
                / self.period as f64,
        )
    }
}

/// Rolling coefficient of variation, reported as a percentage.
#[derive(Debug, Clone)]
pub struct CoefficientOfVariation {
    period: usize,
    values: Window<f64>,
    samples: usize,
}

impl CoefficientOfVariation {
    /// Create coefficient of variation.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        Ok(Self {
            period,
            values: Window::new(period)?,
            samples: 0,
        })
    }
}

impl Reset for CoefficientOfVariation {
    fn reset(&mut self) {
        self.values.clear();
        self.samples = 0;
    }
}

impl Warmup for CoefficientOfVariation {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period
    }
}

impl LegacyBarIndicator for CoefficientOfVariation {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        self.values.push(bar.close);
        if !self.values.is_full() {
            return None;
        }
        let (mean, variance) = population_moments(self.values.iter().copied());
        Some(if mean == 0.0 {
            0.0
        } else {
            100.0 * variance.sqrt() / mean.abs()
        })
    }
}

/// Rolling autocorrelation of closing prices at a fixed lag.
#[derive(Debug, Clone)]
pub struct Autocorrelation {
    period: usize,
    lag: usize,
    values: Window<f64>,
    samples: usize,
}

impl Autocorrelation {
    /// Create autocorrelation. Lag must be smaller than period.
    pub fn new(period: usize, lag: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        let lag = ensure_period(lag)?;
        if lag >= period {
            return Err(IndicatorError::InvalidParameter {
                name: "lag",
                reason: "must be smaller than period",
            });
        }
        Ok(Self {
            period,
            lag,
            values: Window::new(period)?,
            samples: 0,
        })
    }
}

impl Reset for Autocorrelation {
    fn reset(&mut self) {
        self.values.clear();
        self.samples = 0;
    }
}

impl Warmup for Autocorrelation {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period
    }
}

impl LegacyBarIndicator for Autocorrelation {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        self.values.push(bar.close);
        if !self.values.is_full() {
            return None;
        }
        let values = self.values.iter().copied().collect::<Vec<_>>();
        let left = &values[..values.len() - self.lag];
        let right = &values[self.lag..];
        let count = left.len() as f64;
        let mean_left = left.iter().sum::<f64>() / count;
        let mean_right = right.iter().sum::<f64>() / count;
        let mut covariance = 0.0;
        let mut variance_left = 0.0;
        let mut variance_right = 0.0;
        for (&left, &right) in left.iter().zip(right) {
            let dl = left - mean_left;
            let dr = right - mean_right;
            covariance += dl * dr;
            variance_left += dl * dl;
            variance_right += dr * dr;
        }
        let denominator = (variance_left * variance_right).sqrt();
        Some(if denominator == 0.0 {
            0.0
        } else {
            (covariance / denominator).clamp(-1.0, 1.0)
        })
    }
}
