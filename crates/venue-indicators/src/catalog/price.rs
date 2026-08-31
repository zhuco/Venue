//! Price transforms and rolling channels.

use super::core::{Bar, IndicatorResult, LegacyBarIndicator, Reset, Warmup};
use super::series::{RollingMax, RollingMin};

macro_rules! price_transform {
    ($name:ident, $method:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, Copy, Default)]
        pub struct $name {
            samples: usize,
        }

        impl $name {
            /// Create the indicator.
            pub fn new() -> Self {
                Self::default()
            }
        }

        impl Reset for $name {
            fn reset(&mut self) {
                self.samples = 0;
            }
        }

        impl Warmup for $name {
            fn samples(&self) -> usize {
                self.samples
            }

            fn warmup_period(&self) -> usize {
                1
            }
        }

        impl LegacyBarIndicator for $name {
            type Output = f64;

            fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
                self.samples = self.samples.saturating_add(1);
                Some((*bar).$method())
            }
        }
    };
}

price_transform!(
    TypicalPrice,
    typical_price,
    "Typical price `(H + L + C) / 3`."
);
price_transform!(MedianPrice, median_price, "Median price `(H + L) / 2`.");
price_transform!(
    WeightedClose,
    weighted_close,
    "Weighted close `(H + L + 2C) / 4`."
);

/// Highest high over a rolling period.
#[derive(Debug, Clone)]
pub struct HighestHigh {
    max: RollingMax,
}

impl HighestHigh {
    /// Create a highest-high channel.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            max: RollingMax::new(period)?,
        })
    }
}

impl Reset for HighestHigh {
    fn reset(&mut self) {
        self.max.reset();
    }
}

impl Warmup for HighestHigh {
    fn samples(&self) -> usize {
        self.max.samples()
    }

    fn warmup_period(&self) -> usize {
        self.max.warmup_period()
    }
}

impl LegacyBarIndicator for HighestHigh {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.max.update(bar.high)
    }
}

/// Lowest low over a rolling period.
#[derive(Debug, Clone)]
pub struct LowestLow {
    min: RollingMin,
}

impl LowestLow {
    /// Create a lowest-low channel.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            min: RollingMin::new(period)?,
        })
    }
}

impl Reset for LowestLow {
    fn reset(&mut self) {
        self.min.reset();
    }
}

impl Warmup for LowestLow {
    fn samples(&self) -> usize {
        self.min.samples()
    }

    fn warmup_period(&self) -> usize {
        self.min.warmup_period()
    }
}

impl LegacyBarIndicator for LowestLow {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.min.update(bar.low)
    }
}

/// Midpoint between rolling highest high and lowest low.
#[derive(Debug, Clone)]
pub struct Midpoint {
    high: RollingMax,
    low: RollingMin,
}

impl Midpoint {
    /// Create a midpoint indicator.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            high: RollingMax::new(period)?,
            low: RollingMin::new(period)?,
        })
    }
}

impl Reset for Midpoint {
    fn reset(&mut self) {
        self.high.reset();
        self.low.reset();
    }
}

impl Warmup for Midpoint {
    fn samples(&self) -> usize {
        self.high.samples()
    }

    fn warmup_period(&self) -> usize {
        self.high.warmup_period()
    }
}

impl LegacyBarIndicator for Midpoint {
    type Output = f64;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        let high = self.high.update(bar.high);
        let low = self.low.update(bar.low);
        high.zip(low).map(|(high, low)| high.midpoint(low))
    }
}

/// Donchian channel output.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DonchianOutput {
    /// Highest high.
    pub upper: f64,
    /// Lowest low.
    pub lower: f64,
    /// Channel midpoint.
    pub middle: f64,
}

/// Donchian channel.
#[derive(Debug, Clone)]
pub struct DonchianChannel {
    high: RollingMax,
    low: RollingMin,
}

impl DonchianChannel {
    /// Create a Donchian channel.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            high: RollingMax::new(period)?,
            low: RollingMin::new(period)?,
        })
    }
}

impl Reset for DonchianChannel {
    fn reset(&mut self) {
        self.high.reset();
        self.low.reset();
    }
}

impl Warmup for DonchianChannel {
    fn samples(&self) -> usize {
        self.high.samples()
    }

    fn warmup_period(&self) -> usize {
        self.high.warmup_period()
    }
}

impl LegacyBarIndicator for DonchianChannel {
    type Output = DonchianOutput;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        let upper = self.high.update(bar.high);
        let lower = self.low.update(bar.low);
        upper.zip(lower).map(|(upper, lower)| DonchianOutput {
            upper,
            lower,
            middle: upper.midpoint(lower),
        })
    }
}

/// Classic floor-trader pivot levels.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PivotOutput {
    /// Central pivot.
    pub pivot: f64,
    /// First resistance.
    pub resistance_1: f64,
    /// First support.
    pub support_1: f64,
    /// Second resistance.
    pub resistance_2: f64,
    /// Second support.
    pub support_2: f64,
    /// Third resistance.
    pub resistance_3: f64,
    /// Third support.
    pub support_3: f64,
}

/// Classic pivot points calculated from each completed bar.
#[derive(Debug, Clone, Copy, Default)]
pub struct PivotPoints {
    samples: usize,
}

impl PivotPoints {
    /// Create classic pivot points.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Reset for PivotPoints {
    fn reset(&mut self) {
        self.samples = 0;
    }
}

impl Warmup for PivotPoints {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        1
    }
}

impl LegacyBarIndicator for PivotPoints {
    type Output = PivotOutput;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let pivot = (bar.high + bar.low + bar.close) / 3.0;
        let range = bar.high - bar.low;
        Some(PivotOutput {
            pivot,
            resistance_1: 2.0 * pivot - bar.low,
            support_1: 2.0 * pivot - bar.high,
            resistance_2: pivot + range,
            support_2: pivot - range,
            resistance_3: bar.high + 2.0 * (pivot - bar.low),
            support_3: bar.low - 2.0 * (bar.high - pivot),
        })
    }
}
