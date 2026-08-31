//! Indicators consuming normalized public trades.

use super::core::{
    AggressorSide, IndicatorError, IndicatorResult, LegacyTradeIndicator, Reset, Trade, Warmup,
    ensure_period,
};
use super::series::{RollingMean, RollingSum, Window};

fn side_volumes(trade: &Trade) -> (f64, f64) {
    match trade.aggressor {
        AggressorSide::Buy => (trade.quantity, 0.0),
        AggressorSide::Sell => (0.0, trade.quantity),
        AggressorSide::Unknown => (0.0, 0.0),
    }
}

/// Rolling aggressive-buy minus aggressive-sell base volume.
#[derive(Debug, Clone)]
pub struct VolumeDelta {
    delta: RollingSum,
}

impl VolumeDelta {
    /// Create rolling volume delta over a number of trades.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            delta: RollingSum::new(period)?,
        })
    }
}

impl Reset for VolumeDelta {
    fn reset(&mut self) {
        self.delta.reset();
    }
}

impl Warmup for VolumeDelta {
    fn samples(&self) -> usize {
        self.delta.samples()
    }

    fn warmup_period(&self) -> usize {
        self.delta.warmup_period()
    }
}

impl LegacyTradeIndicator for VolumeDelta {
    type Output = f64;

    fn update_legacy(&mut self, trade: &Trade) -> Option<Self::Output> {
        self.delta.update(trade.signed_quantity())
    }
}

/// Cumulative volume delta since the last reset/anchor.
#[derive(Debug, Clone, Copy, Default)]
pub struct CumulativeVolumeDelta {
    value: f64,
    samples: usize,
}

impl CumulativeVolumeDelta {
    /// Create cumulative volume delta.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Reset for CumulativeVolumeDelta {
    fn reset(&mut self) {
        self.value = 0.0;
        self.samples = 0;
    }
}

impl Warmup for CumulativeVolumeDelta {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        1
    }
}

impl LegacyTradeIndicator for CumulativeVolumeDelta {
    type Output = f64;

    fn update_legacy(&mut self, trade: &Trade) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        self.value += trade.signed_quantity();
        Some(self.value)
    }
}

/// Rolling share of classified volume initiated by buyers, in `[0, 1]`.
#[derive(Debug, Clone)]
pub struct AggressorRatio {
    buy: RollingSum,
    sell: RollingSum,
}

impl AggressorRatio {
    /// Create aggressor ratio over a number of trades.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            buy: RollingSum::new(period)?,
            sell: RollingSum::new(period)?,
        })
    }
}

impl Reset for AggressorRatio {
    fn reset(&mut self) {
        self.buy.reset();
        self.sell.reset();
    }
}

impl Warmup for AggressorRatio {
    fn samples(&self) -> usize {
        self.buy.samples()
    }

    fn warmup_period(&self) -> usize {
        self.buy.warmup_period()
    }
}

impl LegacyTradeIndicator for AggressorRatio {
    type Output = f64;

    fn update_legacy(&mut self, trade: &Trade) -> Option<Self::Output> {
        let (buy_volume, sell_volume) = side_volumes(trade);
        let rolling_buy = self.buy.update(buy_volume);
        let rolling_sell = self.sell.update(sell_volume);
        rolling_buy
            .zip(rolling_sell)
            .map(|(buy_total, sell_total)| {
                let total = buy_total + sell_total;
                if total == 0.0 { 0.5 } else { buy_total / total }
            })
    }
}

/// Rolling normalized trade imbalance `(buy-sell)/(buy+sell)` in `[-1, 1]`.
#[derive(Debug, Clone)]
pub struct TradeImbalance {
    buy: RollingSum,
    sell: RollingSum,
}

impl TradeImbalance {
    /// Create trade imbalance over a number of trades.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            buy: RollingSum::new(period)?,
            sell: RollingSum::new(period)?,
        })
    }
}

impl Reset for TradeImbalance {
    fn reset(&mut self) {
        self.buy.reset();
        self.sell.reset();
    }
}

impl Warmup for TradeImbalance {
    fn samples(&self) -> usize {
        self.buy.samples()
    }

    fn warmup_period(&self) -> usize {
        self.buy.warmup_period()
    }
}

impl LegacyTradeIndicator for TradeImbalance {
    type Output = f64;

    fn update_legacy(&mut self, trade: &Trade) -> Option<Self::Output> {
        let (buy_volume, sell_volume) = side_volumes(trade);
        let rolling_buy = self.buy.update(buy_volume);
        let rolling_sell = self.sell.update(sell_volume);
        rolling_buy
            .zip(rolling_sell)
            .map(|(buy_total, sell_total)| {
                let total = buy_total + sell_total;
                if total == 0.0 {
                    0.0
                } else {
                    ((buy_total - sell_total) / total).clamp(-1.0, 1.0)
                }
            })
    }
}

/// Rolling trade-arrival rate measured in trades per second.
#[derive(Debug, Clone)]
pub struct TradeIntensity {
    period: usize,
    timestamps: Window<i64>,
    samples: usize,
}

impl TradeIntensity {
    /// Create trade intensity over a number of arrivals.
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
            timestamps: Window::new(period)?,
            samples: 0,
        })
    }
}

impl Reset for TradeIntensity {
    fn reset(&mut self) {
        self.timestamps.clear();
        self.samples = 0;
    }
}

impl Warmup for TradeIntensity {
    fn samples(&self) -> usize {
        self.samples
    }

    fn warmup_period(&self) -> usize {
        self.period
    }
}

impl LegacyTradeIndicator for TradeIntensity {
    type Output = f64;

    fn update_legacy(&mut self, trade: &Trade) -> Option<Self::Output> {
        self.samples = self.samples.saturating_add(1);
        self.timestamps.push(trade.timestamp);
        if !self.timestamps.is_full() {
            return None;
        }
        let first = *self.timestamps.front()?;
        let last = *self.timestamps.back()?;
        let duration_seconds = (last - first) as f64 / 1_000.0;
        Some(if duration_seconds <= 0.0 {
            0.0
        } else {
            (self.period - 1) as f64 / duration_seconds
        })
    }
}

/// Rolling average base quantity per trade.
#[derive(Debug, Clone)]
pub struct AverageTradeSize {
    mean: RollingMean,
}

impl AverageTradeSize {
    /// Create average trade size.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            mean: RollingMean::new(period)?,
        })
    }
}

impl Reset for AverageTradeSize {
    fn reset(&mut self) {
        self.mean.reset();
    }
}

impl Warmup for AverageTradeSize {
    fn samples(&self) -> usize {
        self.mean.samples()
    }

    fn warmup_period(&self) -> usize {
        self.mean.warmup_period()
    }
}

impl LegacyTradeIndicator for AverageTradeSize {
    type Output = f64;

    fn update_legacy(&mut self, trade: &Trade) -> Option<Self::Output> {
        self.mean.update(trade.quantity)
    }
}

/// Rolling share of quote notional contributed by trades above a threshold.
#[derive(Debug, Clone)]
pub struct LargeTradeRatio {
    threshold_notional: f64,
    large: RollingSum,
    total: RollingSum,
}

impl LargeTradeRatio {
    /// Create large-trade notional ratio.
    pub fn new(period: usize, threshold_notional: f64) -> IndicatorResult<Self> {
        if !threshold_notional.is_finite() || threshold_notional <= 0.0 {
            return Err(IndicatorError::InvalidParameter {
                name: "threshold_notional",
                reason: "must be finite and positive",
            });
        }
        Ok(Self {
            threshold_notional,
            large: RollingSum::new(period)?,
            total: RollingSum::new(period)?,
        })
    }
}

impl Reset for LargeTradeRatio {
    fn reset(&mut self) {
        self.large.reset();
        self.total.reset();
    }
}

impl Warmup for LargeTradeRatio {
    fn samples(&self) -> usize {
        self.total.samples()
    }

    fn warmup_period(&self) -> usize {
        self.total.warmup_period()
    }
}

impl LegacyTradeIndicator for LargeTradeRatio {
    type Output = f64;

    fn update_legacy(&mut self, trade: &Trade) -> Option<Self::Output> {
        let notional = trade.notional();
        let large = self.large.update(if notional >= self.threshold_notional {
            notional
        } else {
            0.0
        });
        let total = self.total.update(notional);
        large
            .zip(total)
            .map(|(large, total)| if total == 0.0 { 0.0 } else { large / total })
    }
}

/// Rolling aggressive signed quote notional.
#[derive(Debug, Clone)]
pub struct SignedNotional {
    sum: RollingSum,
}

impl SignedNotional {
    /// Create rolling signed notional.
    pub fn new(period: usize) -> IndicatorResult<Self> {
        Ok(Self {
            sum: RollingSum::new(period)?,
        })
    }
}

impl Reset for SignedNotional {
    fn reset(&mut self) {
        self.sum.reset();
    }
}

impl Warmup for SignedNotional {
    fn samples(&self) -> usize {
        self.sum.samples()
    }

    fn warmup_period(&self) -> usize {
        self.sum.warmup_period()
    }
}

impl LegacyTradeIndicator for SignedNotional {
    type Output = f64;

    fn update_legacy(&mut self, trade: &Trade) -> Option<Self::Output> {
        let value = match trade.aggressor {
            AggressorSide::Buy => trade.notional(),
            AggressorSide::Sell => -trade.notional(),
            AggressorSide::Unknown => 0.0,
        };
        self.sum.update(value)
    }
}
