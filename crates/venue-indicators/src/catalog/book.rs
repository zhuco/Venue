//! Indicators consuming normalized order-book snapshots.

use super::core::{
    BookLevel, IndicatorError, IndicatorResult, LegacyBookIndicator, OrderBook, Reset, Warmup,
    ensure_period,
};

/// Top-of-book spread output.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SpreadOutput {
    /// Absolute price spread.
    pub absolute: f64,
    /// Spread in basis points relative to midpoint.
    pub basis_points: f64,
}

/// Absolute and relative top-of-book spread.
#[derive(Debug, Clone, Copy, Default)]
pub struct Spread {
    samples: usize,
}

impl Spread {
    /// Create spread indicator.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Reset for Spread {
    fn reset(&mut self) {
        self.samples = 0;
    }
}

impl Warmup for Spread {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        1
    }
}

impl LegacyBookIndicator for Spread {
    type Output = SpreadOutput;

    fn update_legacy(&mut self, book: &OrderBook) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let absolute = book.spread();
        Some(SpreadOutput {
            absolute,
            basis_points: 10_000.0 * absolute / book.mid_price(),
        })
    }
}

/// Top-of-book arithmetic midpoint.
#[derive(Debug, Clone, Copy, Default)]
pub struct MidPrice {
    samples: usize,
}

impl MidPrice {
    /// Create midpoint indicator.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Reset for MidPrice {
    fn reset(&mut self) {
        self.samples = 0;
    }
}

impl Warmup for MidPrice {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        1
    }
}

impl LegacyBookIndicator for MidPrice {
    type Output = f64;

    fn update_legacy(&mut self, book: &OrderBook) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        Some(book.mid_price())
    }
}

/// Same-side size-weighted top-of-book price.
#[derive(Debug, Clone, Copy, Default)]
pub struct WeightedMid {
    samples: usize,
}

impl WeightedMid {
    /// Create weighted midpoint.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Reset for WeightedMid {
    fn reset(&mut self) {
        self.samples = 0;
    }
}

impl Warmup for WeightedMid {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        1
    }
}

impl LegacyBookIndicator for WeightedMid {
    type Output = f64;

    fn update_legacy(&mut self, book: &OrderBook) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let bid = book.best_bid();
        let ask = book.best_ask();
        let total = bid.quantity + ask.quantity;
        Some(if total == 0.0 {
            book.mid_price()
        } else {
            (bid.price * bid.quantity + ask.price * ask.quantity) / total
        })
    }
}

/// Cross-size weighted microprice.
#[derive(Debug, Clone, Copy, Default)]
pub struct Microprice {
    samples: usize,
}

impl Microprice {
    /// Create microprice.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Reset for Microprice {
    fn reset(&mut self) {
        self.samples = 0;
    }
}

impl Warmup for Microprice {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        1
    }
}

impl LegacyBookIndicator for Microprice {
    type Output = f64;

    fn update_legacy(&mut self, book: &OrderBook) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let bid = book.best_bid();
        let ask = book.best_ask();
        let total = bid.quantity + ask.quantity;
        Some(if total == 0.0 {
            book.mid_price()
        } else {
            (ask.price * bid.quantity + bid.price * ask.quantity) / total
        })
    }
}

/// Equal-weight depth imbalance across the first N levels.
#[derive(Debug, Clone)]
pub struct BookImbalance {
    levels: usize,
    samples: usize,
}

impl BookImbalance {
    /// Create depth imbalance.
    pub fn new(levels: usize) -> IndicatorResult<Self> {
        Ok(Self {
            levels: ensure_period(levels)?,
            samples: 0,
        })
    }
}

impl Reset for BookImbalance {
    fn reset(&mut self) {
        self.samples = 0;
    }
}

impl Warmup for BookImbalance {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        1
    }
}

impl LegacyBookIndicator for BookImbalance {
    type Output = f64;

    fn update_legacy(&mut self, book: &OrderBook) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let bid = book
            .bids
            .iter()
            .take(self.levels)
            .map(|level| level.quantity)
            .sum::<f64>();
        let ask = book
            .asks
            .iter()
            .take(self.levels)
            .map(|level| level.quantity)
            .sum::<f64>();
        let total = bid + ask;
        Some(if total == 0.0 {
            0.0
        } else {
            ((bid - ask) / total).clamp(-1.0, 1.0)
        })
    }
}

/// Exponentially depth-weighted imbalance across the first N levels.
#[derive(Debug, Clone)]
pub struct DepthWeightedImbalance {
    levels: usize,
    decay: f64,
    samples: usize,
}

impl DepthWeightedImbalance {
    /// Create weighted imbalance. Decay must be in `(0, 1]`.
    pub fn new(levels: usize, decay: f64) -> IndicatorResult<Self> {
        let levels = ensure_period(levels)?;
        if !(decay.is_finite() && 0.0 < decay && decay <= 1.0) {
            return Err(IndicatorError::InvalidParameter {
                name: "decay",
                reason: "must be finite and in (0, 1]",
            });
        }
        Ok(Self {
            levels,
            decay,
            samples: 0,
        })
    }
}

impl Reset for DepthWeightedImbalance {
    fn reset(&mut self) {
        self.samples = 0;
    }
}

impl Warmup for DepthWeightedImbalance {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        1
    }
}

impl LegacyBookIndicator for DepthWeightedImbalance {
    type Output = f64;

    fn update_legacy(&mut self, book: &OrderBook) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let weighted = |levels: &[BookLevel]| {
            levels
                .iter()
                .take(self.levels)
                .scan(1.0, |weight, level| {
                    let contribution = *weight * level.quantity;
                    *weight *= self.decay;
                    Some(contribution)
                })
                .sum::<f64>()
        };
        let bid = weighted(&book.bids);
        let ask = weighted(&book.asks);
        let total = bid + ask;
        Some(if total == 0.0 {
            0.0
        } else {
            ((bid - ask) / total).clamp(-1.0, 1.0)
        })
    }
}

/// Depth-slope output.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DepthSlopeOutput {
    /// Bid cumulative-depth slope per basis point from midpoint.
    pub bid_slope: f64,
    /// Ask cumulative-depth slope per basis point from midpoint.
    pub ask_slope: f64,
    /// Normalized bid-vs-ask slope imbalance.
    pub imbalance: f64,
}

/// Linear slope of cumulative depth against distance from midpoint.
#[derive(Debug, Clone)]
pub struct DepthSlope {
    levels: usize,
    samples: usize,
}

impl DepthSlope {
    /// Create depth slope.
    pub fn new(levels: usize) -> IndicatorResult<Self> {
        let levels = ensure_period(levels)?;
        if levels < 2 {
            return Err(IndicatorError::InvalidParameter {
                name: "levels",
                reason: "must be at least 2",
            });
        }
        Ok(Self { levels, samples: 0 })
    }

    fn slope(book: &OrderBook, bid_side: bool, levels: usize) -> f64 {
        let midpoint = book.mid_price();
        let side = if bid_side { &book.bids } else { &book.asks };
        let points = side
            .iter()
            .take(levels)
            .scan(0.0, |cumulative, level| {
                *cumulative += level.quantity;
                let distance = if bid_side {
                    10_000.0 * (midpoint - level.price) / midpoint
                } else {
                    10_000.0 * (level.price - midpoint) / midpoint
                };
                Some((distance, *cumulative))
            })
            .collect::<Vec<_>>();
        if points.len() < 2 {
            return 0.0;
        }
        let count = points.len() as f64;
        let mean_x = points.iter().map(|point| point.0).sum::<f64>() / count;
        let mean_y = points.iter().map(|point| point.1).sum::<f64>() / count;
        let covariance = points
            .iter()
            .map(|point| (point.0 - mean_x) * (point.1 - mean_y))
            .sum::<f64>();
        let variance_x = points
            .iter()
            .map(|point| (point.0 - mean_x).powi(2))
            .sum::<f64>();
        if variance_x == 0.0 {
            0.0
        } else {
            covariance / variance_x
        }
    }
}

impl Reset for DepthSlope {
    fn reset(&mut self) {
        self.samples = 0;
    }
}

impl Warmup for DepthSlope {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        1
    }
}

impl LegacyBookIndicator for DepthSlope {
    type Output = DepthSlopeOutput;

    fn update_legacy(&mut self, book: &OrderBook) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let bid_slope = Self::slope(book, true, self.levels);
        let ask_slope = Self::slope(book, false, self.levels);
        let total = bid_slope.abs() + ask_slope.abs();
        Some(DepthSlopeOutput {
            bid_slope,
            ask_slope,
            imbalance: if total == 0.0 {
                0.0
            } else {
                ((bid_slope.abs() - ask_slope.abs()) / total).clamp(-1.0, 1.0)
            },
        })
    }
}

/// Top-of-book order-flow imbalance between consecutive snapshots.
#[derive(Debug, Clone, Default)]
pub struct BookOrderFlowImbalance {
    previous: Option<(f64, f64, f64, f64)>,
    samples: usize,
}

impl BookOrderFlowImbalance {
    /// Create order-flow imbalance.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Reset for BookOrderFlowImbalance {
    fn reset(&mut self) {
        self.previous = None;
        self.samples = 0;
    }
}

impl Warmup for BookOrderFlowImbalance {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        2
    }
}

impl LegacyBookIndicator for BookOrderFlowImbalance {
    type Output = f64;

    fn update_legacy(&mut self, book: &OrderBook) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        let bid = book.best_bid();
        let ask = book.best_ask();
        let current = (bid.price, bid.quantity, ask.price, ask.quantity);
        let previous = self.previous.replace(current)?;
        let (previous_bid_price, previous_bid_quantity, previous_ask_price, previous_ask_quantity) =
            previous;

        let bid_event = if bid.price > previous_bid_price {
            bid.quantity
        } else if bid.price == previous_bid_price {
            bid.quantity - previous_bid_quantity
        } else {
            -previous_bid_quantity
        };
        let ask_event = if ask.price < previous_ask_price {
            -ask.quantity
        } else if ask.price == previous_ask_price {
            previous_ask_quantity - ask.quantity
        } else {
            previous_ask_quantity
        };
        Some(bid_event + ask_event)
    }
}
