//! Reusable rolling windows and smoothing primitives.

#![forbid(unsafe_code)]
#![allow(dead_code)]

use std::collections::VecDeque;

use super::core::{IndicatorError, IndicatorResult, Reset, Warmup, ensure_period};

/// Fixed-capacity FIFO window.
#[derive(Debug, Clone)]
pub struct Window<T> {
    period: usize,
    values: VecDeque<T>,
}

impl<T> Window<T> {
    /// Create a new window.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            period: ensure_period(period)?,
            values: VecDeque::with_capacity(period),
        })
    }

    /// Push a value and return the evicted item, when full.
    pub fn push(&mut self, value: T) -> Option<T> {
        let removed = if self.values.len() == self.period {
            self.values.pop_front()
        } else {
            None
        };
        self.values.push_back(value);
        removed
    }

    /// Number of values currently retained.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether no values are retained.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Configured capacity.
    pub fn period(&self) -> usize {
        self.period
    }

    /// Whether the window is full.
    pub fn is_full(&self) -> bool {
        self.values.len() == self.period
    }

    /// Iterate from oldest to newest.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &T> {
        self.values.iter()
    }

    /// Most recent value.
    pub fn back(&self) -> Option<&T> {
        self.values.back()
    }

    /// Oldest value.
    pub fn front(&self) -> Option<&T> {
        self.values.front()
    }

    /// Clear retained values.
    pub fn clear(&mut self) {
        self.values.clear();
    }
}

/// Rolling sum over a fixed number of values.
#[derive(Debug, Clone)]
pub struct RollingSum {
    window: Window<f64>,
    sum: f64,
    samples: usize,
}

impl RollingSum {
    /// Create a rolling sum.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            window: Window::new(period)?,
            sum: 0.0,
            samples: 0,
        })
    }

    /// Push a value and return the sum once the window is full.
    pub fn update(&mut self, value: f64) -> Option<f64> {
        self.samples = self.samples.saturating_add(1);
        if let Some(old) = self.window.push(value) {
            self.sum -= old;
        }
        self.sum += value;
        self.window.is_full().then_some(self.sum)
    }

    /// Current partial or complete sum.
    pub fn current(&self) -> f64 {
        self.sum
    }

    /// Configured period.
    pub fn period(&self) -> usize {
        self.window.period()
    }
}

impl Reset for RollingSum {
    fn reset(&mut self) {
        self.window.clear();
        self.sum = 0.0;
        self.samples = 0;
    }
}

impl Warmup for RollingSum {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period()
    }
}

/// Rolling arithmetic mean.
#[derive(Debug, Clone)]
pub struct RollingMean {
    sum: RollingSum,
}

impl RollingMean {
    /// Create a rolling mean.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            sum: RollingSum::new(period)?,
        })
    }

    /// Push a value and return the mean once ready.
    pub fn update(&mut self, value: f64) -> Option<f64> {
        self.sum
            .update(value)
            .map(|sum| sum / self.sum.period() as f64)
    }

    /// Configured period.
    pub fn period(&self) -> usize {
        self.sum.period()
    }
}

impl Reset for RollingMean {
    fn reset(&mut self) {
        self.sum.reset();
    }
}

impl Warmup for RollingMean {
    fn samples(&self) -> usize {
        self.sum.samples()
    }

    fn warmup_period(&self) -> usize {
        self.sum.warmup_period()
    }
}

/// Rolling minimum. The implementation favors determinism and simplicity over a monotonic deque.
#[derive(Debug, Clone)]
pub struct RollingMin {
    window: Window<f64>,
    samples: usize,
}

impl RollingMin {
    /// Create a rolling minimum.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            window: Window::new(period)?,
            samples: 0,
        })
    }

    /// Push a value and return the minimum once ready.
    pub fn update(&mut self, value: f64) -> Option<f64> {
        self.samples = self.samples.saturating_add(1);
        self.window.push(value);
        self.window
            .is_full()
            .then(|| self.window.iter().copied().fold(f64::INFINITY, f64::min))
    }

    /// Configured period.
    pub fn period(&self) -> usize {
        self.window.period()
    }
}

impl Reset for RollingMin {
    fn reset(&mut self) {
        self.window.clear();
        self.samples = 0;
    }
}

impl Warmup for RollingMin {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period()
    }
}

/// Rolling maximum.
#[derive(Debug, Clone)]
pub struct RollingMax {
    window: Window<f64>,
    samples: usize,
}

impl RollingMax {
    /// Create a rolling maximum.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            window: Window::new(period)?,
            samples: 0,
        })
    }

    /// Push a value and return the maximum once ready.
    pub fn update(&mut self, value: f64) -> Option<f64> {
        self.samples = self.samples.saturating_add(1);
        self.window.push(value);
        self.window.is_full().then(|| {
            self.window
                .iter()
                .copied()
                .fold(f64::NEG_INFINITY, f64::max)
        })
    }

    /// Configured period.
    pub fn period(&self) -> usize {
        self.window.period()
    }
}

impl Reset for RollingMax {
    fn reset(&mut self) {
        self.window.clear();
        self.samples = 0;
    }
}

impl Warmup for RollingMax {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period()
    }
}

/// Rolling population variance and standard deviation.
#[derive(Debug, Clone)]
pub struct RollingVariance {
    window: Window<f64>,
    samples: usize,
}

impl RollingVariance {
    /// Create a rolling variance.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            window: Window::new(period)?,
            samples: 0,
        })
    }

    /// Push a value and return population variance once ready.
    pub fn update(&mut self, value: f64) -> Option<f64> {
        self.samples = self.samples.saturating_add(1);
        self.window.push(value);
        if !self.window.is_full() {
            return None;
        }
        let n = self.window.period() as f64;
        let mean = self.window.iter().sum::<f64>() / n;
        let variance = self
            .window
            .iter()
            .map(|item| {
                let delta = *item - mean;
                delta * delta
            })
            .sum::<f64>()
            / n;
        Some(variance.max(0.0))
    }

    /// Configured period.
    pub fn period(&self) -> usize {
        self.window.period()
    }

    /// Iterate current values.
    pub fn values(&self) -> impl Iterator<Item = f64> + '_ {
        self.window.iter().copied()
    }
}

impl Reset for RollingVariance {
    fn reset(&mut self) {
        self.window.clear();
        self.samples = 0;
    }
}

impl Warmup for RollingVariance {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period()
    }
}

/// Rolling population standard deviation.
#[derive(Debug, Clone)]
pub struct RollingStdDev {
    variance: RollingVariance,
}

impl RollingStdDev {
    /// Create a rolling standard deviation.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            variance: RollingVariance::new(period)?,
        })
    }

    /// Push a value and return standard deviation once ready.
    pub fn update(&mut self, value: f64) -> Option<f64> {
        self.variance.update(value).map(f64::sqrt)
    }

    /// Configured period.
    pub fn period(&self) -> usize {
        self.variance.period()
    }

    /// Iterate current values.
    pub fn values(&self) -> impl Iterator<Item = f64> + '_ {
        self.variance.values()
    }
}

impl Reset for RollingStdDev {
    fn reset(&mut self) {
        self.variance.reset();
    }
}

impl Warmup for RollingStdDev {
    fn samples(&self) -> usize {
        self.variance.samples()
    }

    fn warmup_period(&self) -> usize {
        self.variance.warmup_period()
    }
}

/// Exponential moving average initialized from the first sample.
#[derive(Debug, Clone)]
pub struct Ema {
    period: usize,
    alpha: f64,
    value: Option<f64>,
    samples: usize,
}

impl Ema {
    /// Create an EMA with `alpha = 2 / (period + 1)`.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        Ok(Self {
            period,
            alpha: 2.0 / (period as f64 + 1.0),
            value: None,
            samples: 0,
        })
    }

    /// Create an EMA with an explicit alpha in `(0, 1]`.
    pub fn with_alpha(period: usize, alpha: f64) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        if !(alpha.is_finite() && 0.0 < alpha && alpha <= 1.0) {
            return Err(IndicatorError::InvalidParameter {
                name: "alpha",
                reason: "must be finite and in (0, 1]",
            });
        }
        Ok(Self {
            period,
            alpha,
            value: None,
            samples: 0,
        })
    }

    /// Push one value. Outputs are available immediately; readiness is tracked separately.
    pub fn update(&mut self, value: f64) -> f64 {
        self.samples = self.samples.saturating_add(1);
        let next = self
            .value
            .map_or(value, |current| current + self.alpha * (value - current));
        self.value = Some(next);
        next
    }

    /// Current value.
    pub fn current(&self) -> Option<f64> {
        self.value
    }

    /// Configured period.
    pub fn period(&self) -> usize {
        self.period
    }
}

impl Reset for Ema {
    fn reset(&mut self) {
        self.value = None;
        self.samples = 0;
    }
}

impl Warmup for Ema {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period
    }
}

/// Wilder's moving average, also known as RMA/SMMA.
///
/// The first ready value is seeded with the arithmetic mean of the first `period` samples.
#[derive(Debug, Clone)]
pub struct Rma {
    period: usize,
    seed_sum: f64,
    value: Option<f64>,
    samples: usize,
}

impl Rma {
    /// Create an RMA using Wilder's `alpha = 1 / period`.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        let period = ensure_period(period)?;
        Ok(Self {
            period,
            seed_sum: 0.0,
            value: None,
            samples: 0,
        })
    }

    /// Push one value. Before readiness, the return value is the partial arithmetic mean.
    pub fn update(&mut self, value: f64) -> f64 {
        self.samples = self.samples.saturating_add(1);
        if self.samples <= self.period {
            self.seed_sum += value;
            let partial = self.seed_sum / self.samples as f64;
            if self.samples == self.period {
                let seeded = self.seed_sum / self.period as f64;
                self.value = Some(seeded);
                return seeded;
            }
            return partial;
        }

        let previous = self.value.unwrap_or(value);
        let next = (previous * (self.period - 1) as f64 + value) / self.period as f64;
        self.value = Some(next);
        next
    }

    /// Current ready value. Returns `None` during seed accumulation.
    pub fn current(&self) -> Option<f64> {
        self.value
    }

    /// Configured period.
    pub fn period(&self) -> usize {
        self.period
    }
}

impl Reset for Rma {
    fn reset(&mut self) {
        self.seed_sum = 0.0;
        self.value = None;
        self.samples = 0;
    }
}

impl Warmup for Rma {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period
    }
}

/// Linearly weighted moving average with newest sample having the largest weight.
#[derive(Debug, Clone)]
pub struct Wma {
    window: Window<f64>,
    samples: usize,
}

impl Wma {
    /// Create a WMA.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            window: Window::new(period)?,
            samples: 0,
        })
    }

    /// Push one value and return the WMA once ready.
    pub fn update(&mut self, value: f64) -> Option<f64> {
        self.samples = self.samples.saturating_add(1);
        self.window.push(value);
        if !self.window.is_full() {
            return None;
        }
        let denominator = (self.window.period() * (self.window.period() + 1) / 2) as f64;
        let weighted = self
            .window
            .iter()
            .enumerate()
            .map(|(index, item)| (index + 1) as f64 * item)
            .sum::<f64>();
        Some(weighted / denominator)
    }

    /// Configured period.
    pub fn period(&self) -> usize {
        self.window.period()
    }
}

impl Reset for Wma {
    fn reset(&mut self) {
        self.window.clear();
        self.samples = 0;
    }
}

impl Warmup for Wma {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period()
    }
}

/// Least-squares regression over an evenly spaced rolling window.
#[derive(Debug, Clone)]
pub struct LinearRegressionWindow {
    window: Window<f64>,
    samples: usize,
}

/// Regression coefficients and fit quality.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RegressionOutput {
    /// Slope per sample.
    pub slope: f64,
    /// Intercept at x = 0.
    pub intercept: f64,
    /// Coefficient of determination.
    pub r_squared: f64,
    /// Predicted value at the newest sample.
    pub prediction: f64,
}

impl LinearRegressionWindow {
    /// Create a rolling regression window.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            window: Window::new(period)?,
            samples: 0,
        })
    }

    /// Push one value and return regression statistics once ready.
    pub fn update(&mut self, value: f64) -> Option<RegressionOutput> {
        self.samples = self.samples.saturating_add(1);
        self.window.push(value);
        if !self.window.is_full() {
            return None;
        }
        let n = self.window.period() as f64;
        let mean_x = (n - 1.0) / 2.0;
        let mean_y = self.window.iter().sum::<f64>() / n;
        let mut covariance = 0.0;
        let mut variance_x = 0.0;
        let mut variance_y = 0.0;
        for (index, y) in self.window.iter().enumerate() {
            let dx = index as f64 - mean_x;
            let dy = *y - mean_y;
            covariance += dx * dy;
            variance_x += dx * dx;
            variance_y += dy * dy;
        }
        let slope = if variance_x == 0.0 {
            0.0
        } else {
            covariance / variance_x
        };
        let intercept = mean_y - slope * mean_x;
        let r_squared = if variance_x == 0.0 || variance_y == 0.0 {
            1.0
        } else {
            (covariance * covariance / (variance_x * variance_y)).clamp(0.0, 1.0)
        };
        Some(RegressionOutput {
            slope,
            intercept,
            r_squared,
            prediction: intercept + slope * (n - 1.0),
        })
    }

    /// Configured period.
    pub fn period(&self) -> usize {
        self.window.period()
    }
}

impl Reset for LinearRegressionWindow {
    fn reset(&mut self) {
        self.window.clear();
        self.samples = 0;
    }
}

impl Warmup for LinearRegressionWindow {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period()
    }
}
