use rust_decimal::{Decimal, prelude::ToPrimitive};
use venue_domain::{AggressorSide as DomainAggressorSide, FieldState, PublicBar, PublicTrade};

use crate::PublicBook;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum IndicatorError {
    #[error("indicator parameter {name} {reason}")]
    InvalidParameter {
        name: &'static str,
        reason: &'static str,
    },
    #[error("normalized decimal cannot be represented by the indicator engine")]
    DecimalConversion,
    #[error("required normalized volume is unavailable")]
    VolumeUnavailable,
    #[error("normalized public bar is invalid")]
    InvalidBar,
    #[error("order book is not synchronized, valid, or non-empty")]
    InvalidBook,
}

pub type IndicatorResult<T> = Result<T, IndicatorError>;

pub trait Reset {
    fn reset(&mut self);
}

pub trait Warmup {
    fn samples(&self) -> usize;
    fn warmup_period(&self) -> usize;

    fn is_ready(&self) -> bool {
        self.samples() >= self.warmup_period()
    }
}

pub trait BarIndicator: Reset + Warmup {
    type Output;

    fn update(&mut self, bar: &PublicBar) -> IndicatorResult<Option<Self::Output>>;
}

pub trait TradeIndicator: Reset + Warmup {
    type Output;

    fn update(&mut self, trade: &PublicTrade) -> IndicatorResult<Option<Self::Output>>;
}

pub trait BookIndicator: Reset + Warmup {
    type Output;

    fn update<B: PublicBook>(&mut self, book: &B) -> IndicatorResult<Option<Self::Output>>;
}

pub trait ScalarPairIndicator: Reset + Warmup {
    type Output;

    fn update(&mut self, x: Decimal, y: Decimal) -> IndicatorResult<Option<Self::Output>>;
}

pub trait LegacyBarIndicator: Reset + Warmup {
    type Output;
    const REQUIRES_VOLUME: bool = false;
    const REQUIRES_QUOTE_VOLUME: bool = false;

    fn update_legacy(&mut self, bar: &Bar) -> Option<Self::Output>;
}

impl<T> BarIndicator for T
where
    T: LegacyBarIndicator,
{
    type Output = T::Output;

    fn update(&mut self, bar: &PublicBar) -> IndicatorResult<Option<Self::Output>> {
        let sample = Bar::from_public(bar, T::REQUIRES_VOLUME, T::REQUIRES_QUOTE_VOLUME)?;
        Ok(self.update_legacy(&sample))
    }
}

pub trait LegacyTradeIndicator: Reset + Warmup {
    type Output;

    fn update_legacy(&mut self, trade: &Trade) -> Option<Self::Output>;
}

impl<T> TradeIndicator for T
where
    T: LegacyTradeIndicator,
{
    type Output = T::Output;

    fn update(&mut self, trade: &PublicTrade) -> IndicatorResult<Option<Self::Output>> {
        let sample = Trade::from_public(trade)?;
        Ok(self.update_legacy(&sample))
    }
}

pub trait LegacyBookIndicator: Reset + Warmup {
    type Output;

    fn update_legacy(&mut self, book: &OrderBook) -> Option<Self::Output>;
}

impl<T> BookIndicator for T
where
    T: LegacyBookIndicator,
{
    type Output = T::Output;

    fn update<B: PublicBook>(&mut self, book: &B) -> IndicatorResult<Option<Self::Output>> {
        let sample = OrderBook::from_public(book)?;
        Ok(self.update_legacy(&sample))
    }
}

pub trait LegacyScalarPairIndicator: Reset + Warmup {
    type Output;

    fn update_legacy(&mut self, x: f64, y: f64) -> Option<Self::Output>;
}

impl<T> ScalarPairIndicator for T
where
    T: LegacyScalarPairIndicator,
{
    type Output = T::Output;

    fn update(&mut self, x: Decimal, y: Decimal) -> IndicatorResult<Option<Self::Output>> {
        Ok(self.update_legacy(decimal(x)?, decimal(y)?))
    }
}

pub(crate) fn ensure_period(period: usize) -> IndicatorResult<usize> {
    if period == 0 {
        Err(IndicatorError::InvalidParameter {
            name: "period",
            reason: "must be positive",
        })
    } else if period > 100_000 {
        Err(IndicatorError::InvalidParameter {
            name: "period",
            reason: "must not exceed 100000",
        })
    } else {
        Ok(period)
    }
}

fn decimal(value: Decimal) -> IndicatorResult<f64> {
    value
        .to_f64()
        .filter(|value| value.is_finite())
        .ok_or(IndicatorError::DecimalConversion)
}

#[derive(Clone, Copy, Debug)]
pub struct Bar {
    #[allow(dead_code)]
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
}

impl Bar {
    fn from_public(
        bar: &PublicBar,
        requires_volume: bool,
        requires_quote_volume: bool,
    ) -> IndicatorResult<Self> {
        if !bar.is_valid() {
            return Err(IndicatorError::InvalidBar);
        }
        let volume = match &bar.base_volume {
            FieldState::Known(value) => decimal(*value)?,
            _ if requires_volume => return Err(IndicatorError::VolumeUnavailable),
            _ => 0.0,
        };
        let quote_volume = match &bar.quote_volume {
            FieldState::Known(value) => decimal(*value)?,
            _ if requires_quote_volume => return Err(IndicatorError::VolumeUnavailable),
            _ => 0.0,
        };
        Ok(Self {
            open: decimal(bar.open.value())?,
            high: decimal(bar.high.value())?,
            low: decimal(bar.low.value())?,
            close: decimal(bar.close.value())?,
            volume,
            quote_volume,
        })
    }

    pub fn typical_price(self) -> f64 {
        (self.high + self.low + self.close) / 3.0
    }

    pub fn median_price(self) -> f64 {
        (self.high + self.low) / 2.0
    }

    pub fn weighted_close(self) -> f64 {
        (self.high + self.low + 2.0 * self.close) / 4.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggressorSide {
    Buy,
    Sell,
    Unknown,
}

#[derive(Clone, Copy, Debug)]
pub struct Trade {
    pub timestamp: i64,
    pub price: f64,
    pub quantity: f64,
    pub aggressor: AggressorSide,
}

impl Trade {
    fn from_public(trade: &PublicTrade) -> IndicatorResult<Self> {
        let timestamp = i64::try_from(trade.exchange_time_ms).map_err(|_| {
            IndicatorError::InvalidParameter {
                name: "exchange_time_ms",
                reason: "must fit signed milliseconds",
            }
        })?;
        let aggressor = match trade.aggressor {
            FieldState::Known(DomainAggressorSide::Buy) => AggressorSide::Buy,
            FieldState::Known(DomainAggressorSide::Sell) => AggressorSide::Sell,
            _ => AggressorSide::Unknown,
        };
        Ok(Self {
            timestamp,
            price: decimal(trade.price.value())?,
            quantity: decimal(trade.quantity)?,
            aggressor,
        })
    }

    pub fn notional(self) -> f64 {
        self.price * self.quantity
    }

    pub fn signed_quantity(self) -> f64 {
        match self.aggressor {
            AggressorSide::Buy => self.quantity,
            AggressorSide::Sell => -self.quantity,
            AggressorSide::Unknown => 0.0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct BookLevel {
    pub price: f64,
    pub quantity: f64,
}

#[derive(Clone, Debug)]
pub struct OrderBook {
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
}

impl OrderBook {
    fn from_public<B: PublicBook>(book: &B) -> IndicatorResult<Self> {
        if !book.synchronized() || !book.bridged() {
            return Err(IndicatorError::InvalidBook);
        }
        let convert = |levels: Vec<venue_domain::MarketLevel>| -> IndicatorResult<Vec<BookLevel>> {
            levels
                .into_iter()
                .map(|level| {
                    Ok(BookLevel {
                        price: decimal(level.price.value())?,
                        quantity: decimal(level.quantity)?,
                    })
                })
                .collect()
        };
        let bids = convert(book.bids())?;
        let asks = convert(book.asks())?;
        let result = Self { bids, asks };
        let valid = result
            .bids
            .first()
            .zip(result.asks.first())
            .is_some_and(|(bid, ask)| {
                bid.price < ask.price
                    && bid.quantity >= 0.0
                    && ask.quantity >= 0.0
                    && result
                        .bids
                        .iter()
                        .chain(&result.asks)
                        .all(|level| level.price.is_finite() && level.quantity.is_finite())
            });
        valid.then_some(result).ok_or(IndicatorError::InvalidBook)
    }

    pub fn best_bid(&self) -> &BookLevel {
        &self.bids[0]
    }

    pub fn best_ask(&self) -> &BookLevel {
        &self.asks[0]
    }

    pub fn mid_price(&self) -> f64 {
        (self.best_bid().price + self.best_ask().price) / 2.0
    }

    pub fn spread(&self) -> f64 {
        self.best_ask().price - self.best_bid().price
    }
}
