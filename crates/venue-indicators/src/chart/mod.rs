//! Deterministic chart studies over normalized, completed public bars.
//!
//! Canonical state accepts a closed bar once. A forming bar is evaluated by cloning the
//! state through [`ChartStudyEngine::preview`], so UI previews cannot mutate strategy facts.

mod momentum;
mod registry;
mod trend;
mod volatility;
mod volume;

use rust_decimal::Decimal;
use venue_domain::{PublicBar, Symbol};

pub use momentum::{Macd, MacdValue, Rsi};
pub use registry::{
    ChartIndicatorDescriptor, ChartIndicatorId, ChartIndicatorPlacement, ChartIndicatorRegistry,
    ChartParameterDescriptor,
};
pub use trend::{Ema, Sma};
pub use volatility::{Atr, BollingerBands, BollingerValue};
pub use volume::Vwap;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ChartIndicatorError {
    #[error("indicator parameters are invalid")]
    InvalidParameters,
    #[error("public bar is invalid")]
    InvalidBar,
    #[error("bar scope changed; reset the indicator engine before ingesting it")]
    ScopeChanged,
    #[error("closed bars must be contiguous and strictly increasing")]
    DiscontinuousBar,
    #[error("required volume is unavailable")]
    VolumeUnavailable,
    #[error("decimal arithmetic overflow")]
    Arithmetic,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChartStudyValues {
    pub sma: Option<Decimal>,
    pub ema: Option<Decimal>,
    pub bollinger: Option<BollingerValue>,
    pub vwap: Option<Decimal>,
    pub rsi: Option<Decimal>,
    pub macd: Option<MacdValue>,
    pub atr: Option<Decimal>,
}

/// Fixed first-batch study set used by the chart and future explicit adapters.
#[derive(Clone, Debug)]
pub struct ChartStudyEngine {
    scope: Option<ChartScope>,
    last_close_time_ms: Option<u64>,
    sma: Sma,
    ema: Ema,
    bollinger: BollingerBands,
    vwap: Vwap,
    rsi: Rsi,
    macd: Macd,
    atr: Atr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ChartScope {
    symbol: Symbol,
    generation: u64,
    interval_ms: u64,
}

impl ChartStudyEngine {
    pub fn standard() -> Result<Self, ChartIndicatorError> {
        Ok(Self {
            scope: None,
            last_close_time_ms: None,
            sma: Sma::new(20)?,
            ema: Ema::new(20)?,
            bollinger: BollingerBands::new(20, Decimal::from(2))?,
            vwap: Vwap::new()?,
            rsi: Rsi::new(14)?,
            macd: Macd::new(12, 26, 9)?,
            atr: Atr::new(14)?,
        })
    }

    pub fn reset(&mut self) {
        self.scope = None;
        self.last_close_time_ms = None;
        self.sma.reset();
        self.ema.reset();
        self.bollinger.reset();
        self.vwap.reset();
        self.rsi.reset();
        self.macd.reset();
        self.atr.reset();
    }

    pub fn ingest_closed(
        &mut self,
        bar: &PublicBar,
    ) -> Result<ChartStudyValues, ChartIndicatorError> {
        self.validate_next(bar)?;
        let values = self.update_unchecked(bar)?;
        self.scope = Some(ChartScope {
            symbol: bar.symbol.clone(),
            generation: bar.generation,
            interval_ms: bar.interval_ms,
        });
        self.last_close_time_ms = Some(bar.close_time_ms);
        Ok(values)
    }

    /// Computes an uncommitted forming-bar preview without advancing canonical state.
    pub fn preview(&self, bar: &PublicBar) -> Result<ChartStudyValues, ChartIndicatorError> {
        self.validate_preview(bar)?;
        let mut preview = self.clone();
        preview.update_unchecked(bar)
    }

    fn update_unchecked(
        &mut self,
        bar: &PublicBar,
    ) -> Result<ChartStudyValues, ChartIndicatorError> {
        Ok(ChartStudyValues {
            sma: self.sma.update(bar)?,
            ema: self.ema.update(bar)?,
            bollinger: self.bollinger.update(bar)?,
            vwap: self.vwap.update(bar)?,
            rsi: self.rsi.update(bar)?,
            macd: self.macd.update(bar)?,
            atr: self.atr.update(bar)?,
        })
    }

    fn validate_next(&self, bar: &PublicBar) -> Result<(), ChartIndicatorError> {
        validate_bar(bar)?;
        if let Some(scope) = &self.scope
            && (scope.symbol != bar.symbol
                || scope.generation != bar.generation
                || scope.interval_ms != bar.interval_ms)
        {
            return Err(ChartIndicatorError::ScopeChanged);
        }
        if let Some(last) = self.last_close_time_ms {
            let expected_open = last.checked_add(1).ok_or(ChartIndicatorError::Arithmetic)?;
            if bar.open_time_ms != expected_open && bar.open_time_ms != last {
                return Err(ChartIndicatorError::DiscontinuousBar);
            }
        }
        Ok(())
    }

    fn validate_preview(&self, bar: &PublicBar) -> Result<(), ChartIndicatorError> {
        validate_bar(bar)?;
        let Some(scope) = &self.scope else {
            return Ok(());
        };
        if scope.symbol != bar.symbol
            || scope.generation != bar.generation
            || scope.interval_ms != bar.interval_ms
        {
            return Err(ChartIndicatorError::ScopeChanged);
        }
        if let Some(last) = self.last_close_time_ms
            && bar.open_time_ms < last
        {
            return Err(ChartIndicatorError::DiscontinuousBar);
        }
        Ok(())
    }
}

pub(super) fn validate_bar(bar: &PublicBar) -> Result<(), ChartIndicatorError> {
    if bar.is_valid() {
        Ok(())
    } else {
        Err(ChartIndicatorError::InvalidBar)
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use venue_domain::{FieldState, Price, PublicBar};

    use super::{ChartIndicatorError, ChartStudyEngine};

    fn bar(sequence: u64) -> Result<PublicBar, Box<dyn std::error::Error>> {
        let open_time_ms = sequence * 60_000;
        let close = Decimal::from(100 + i64::try_from(sequence)?);
        let volume = Decimal::from(10);
        Ok(PublicBar {
            symbol: "BTC/USDT".parse()?,
            generation: 7,
            received_at_ms: open_time_ms + 60_000,
            sequence,
            open_time_ms,
            close_time_ms: open_time_ms + 59_999,
            interval_ms: 60_000,
            open: Price::new(close - Decimal::ONE)?,
            high: Price::new(close + Decimal::ONE)?,
            low: Price::new(close - Decimal::from(2))?,
            close: Price::new(close)?,
            base_volume: FieldState::Known(volume),
            quote_volume: FieldState::Known(volume * close),
            trade_count: FieldState::Known(1),
            taker_buy_base_volume: FieldState::Known(Decimal::ZERO),
            taker_buy_quote_volume: FieldState::Known(Decimal::ZERO),
        })
    }

    #[test]
    fn forming_preview_never_advances_canonical_state() -> Result<(), Box<dyn std::error::Error>> {
        let mut engine = ChartStudyEngine::standard()?;
        for sequence in 1..=40 {
            engine.ingest_closed(&bar(sequence)?)?;
        }
        let next = bar(41)?;
        let preview = engine.preview(&next)?;
        let committed = engine.ingest_closed(&next)?;
        assert_eq!(preview, committed);
        assert!(committed.sma.is_some());
        assert!(committed.rsi.is_some());
        assert!(committed.macd.is_some());
        Ok(())
    }

    #[test]
    fn scope_and_time_gaps_require_an_explicit_reset() -> Result<(), Box<dyn std::error::Error>> {
        let mut engine = ChartStudyEngine::standard()?;
        engine.ingest_closed(&bar(1)?)?;
        assert_eq!(
            engine.ingest_closed(&bar(3)?),
            Err(ChartIndicatorError::DiscontinuousBar)
        );
        let mut next_generation = bar(2)?;
        next_generation.generation = 8;
        assert_eq!(
            engine.ingest_closed(&next_generation),
            Err(ChartIndicatorError::ScopeChanged)
        );
        engine.reset();
        assert!(engine.ingest_closed(&next_generation).is_ok());
        Ok(())
    }
}
