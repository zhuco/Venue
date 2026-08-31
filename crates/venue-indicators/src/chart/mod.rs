//! Deterministic chart studies over normalized, completed public bars.
//!
//! Canonical state accepts a closed bar once. A forming bar is evaluated by cloning the
//! state through [`ChartStudyEngine::preview`], so UI previews cannot mutate strategy facts.

mod common;
mod custom_ema_adx;
mod momentum;
mod registry;
mod trend;
mod volatility;
mod volume;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::{PublicBar, Symbol};

pub use common::{CommonStudyValues, DirectionalValue, DmiValue, KdjValue, PairValue, TripleValue};
pub use custom_ema_adx::{EmaAdxConfig, EmaAdxSignal, EmaAdxValues};
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
    pub custom_ema_adx: Option<EmaAdxValues>,
    pub sma: Option<Decimal>,
    pub ema: Option<Decimal>,
    pub bollinger: Option<BollingerValue>,
    pub vwap: Option<Decimal>,
    pub rsi: Option<Decimal>,
    pub macd: Option<MacdValue>,
    pub atr: Option<Decimal>,
    pub common: CommonStudyValues,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChartStudyConfig {
    pub custom_ema_adx: Option<EmaAdxConfig>,
    pub sma_period: usize,
    pub ema_period: usize,
    pub bollinger_period: usize,
    pub bollinger_multiplier: Decimal,
    pub rsi_period: usize,
    pub macd_fast_period: usize,
    pub macd_slow_period: usize,
    pub macd_signal_period: usize,
    pub atr_period: usize,
    pub sma_second_period: usize,
    pub sma_third_period: usize,
    pub ema_second_period: usize,
    pub ema_third_period: usize,
    pub wma_period: usize,
    pub wma_second_period: usize,
    pub wma_third_period: usize,
    pub trix_period: usize,
    pub sar_step: Decimal,
    pub sar_maximum: Decimal,
    pub supertrend_period: usize,
    pub supertrend_multiplier: Decimal,
    pub mfi_period: usize,
    pub kdj_period: usize,
    pub kdj_signal_period: usize,
    pub cci_period: usize,
    pub stoch_rsi_period: usize,
    pub stoch_rsi_stochastic_period: usize,
    pub stoch_rsi_signal_period: usize,
    pub williams_r_period: usize,
    pub dmi_period: usize,
    pub momentum_period: usize,
    pub emv_period: usize,
}

impl Default for ChartStudyConfig {
    fn default() -> Self {
        Self {
            custom_ema_adx: None,
            sma_period: 7,
            ema_period: 7,
            bollinger_period: 20,
            bollinger_multiplier: Decimal::from(2),
            rsi_period: 14,
            macd_fast_period: 12,
            macd_slow_period: 26,
            macd_signal_period: 9,
            atr_period: 14,
            sma_second_period: 25,
            sma_third_period: 99,
            ema_second_period: 25,
            ema_third_period: 99,
            wma_period: 7,
            wma_second_period: 25,
            wma_third_period: 99,
            trix_period: 12,
            sar_step: Decimal::new(2, 2),
            sar_maximum: Decimal::new(20, 2),
            supertrend_period: 10,
            supertrend_multiplier: Decimal::from(3),
            mfi_period: 14,
            kdj_period: 9,
            kdj_signal_period: 3,
            cci_period: 20,
            stoch_rsi_period: 14,
            stoch_rsi_stochastic_period: 14,
            stoch_rsi_signal_period: 3,
            williams_r_period: 14,
            dmi_period: 14,
            momentum_period: 10,
            emv_period: 14,
        }
    }
}

impl ChartStudyConfig {
    pub fn validate(&self) -> Result<(), ChartIndicatorError> {
        if let Some(config) = &self.custom_ema_adx {
            config.validate()?;
        }
        const MAX_PERIOD: usize = 100_000;
        let periods = [
            self.sma_period,
            self.ema_period,
            self.bollinger_period,
            self.rsi_period,
            self.macd_fast_period,
            self.macd_slow_period,
            self.macd_signal_period,
            self.atr_period,
            self.sma_second_period,
            self.sma_third_period,
            self.ema_second_period,
            self.ema_third_period,
            self.wma_period,
            self.wma_second_period,
            self.wma_third_period,
            self.trix_period,
            self.supertrend_period,
            self.mfi_period,
            self.kdj_period,
            self.kdj_signal_period,
            self.cci_period,
            self.stoch_rsi_period,
            self.stoch_rsi_stochastic_period,
            self.stoch_rsi_signal_period,
            self.williams_r_period,
            self.dmi_period,
            self.momentum_period,
            self.emv_period,
        ];
        if periods
            .iter()
            .any(|period| *period == 0 || *period > MAX_PERIOD)
            || self.macd_fast_period >= self.macd_slow_period
            || self.bollinger_multiplier <= Decimal::ZERO
            || self.bollinger_multiplier > Decimal::from(1_000)
            || self.sar_step <= Decimal::ZERO
            || self.sar_maximum < self.sar_step
            || self.supertrend_multiplier <= Decimal::ZERO
        {
            return Err(ChartIndicatorError::InvalidParameters);
        }
        Ok(())
    }
}

/// Fixed first-batch study set used by the chart and future explicit adapters.
#[derive(Clone, Debug)]
pub struct ChartStudyEngine {
    custom_ema_adx: Option<custom_ema_adx::EmaAdxStudy>,
    scope: Option<ChartScope>,
    last_close_time_ms: Option<u64>,
    sma: Sma,
    ema: Ema,
    bollinger: BollingerBands,
    vwap: Vwap,
    rsi: Rsi,
    macd: Macd,
    atr: Atr,
    common: common::CommonStudyEngine,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ChartScope {
    symbol: Symbol,
    generation: u64,
    interval_ms: u64,
}

impl ChartStudyEngine {
    pub fn standard() -> Result<Self, ChartIndicatorError> {
        Self::with_config(&ChartStudyConfig::default())
    }

    pub fn with_config(config: &ChartStudyConfig) -> Result<Self, ChartIndicatorError> {
        config.validate()?;
        Ok(Self {
            custom_ema_adx: config
                .custom_ema_adx
                .as_ref()
                .map(custom_ema_adx::EmaAdxStudy::new)
                .transpose()?,
            scope: None,
            last_close_time_ms: None,
            sma: Sma::new(config.sma_period)?,
            ema: Ema::new(config.ema_period)?,
            bollinger: BollingerBands::new(config.bollinger_period, config.bollinger_multiplier)?,
            vwap: Vwap::new()?,
            rsi: Rsi::new(config.rsi_period)?,
            macd: Macd::new(
                config.macd_fast_period,
                config.macd_slow_period,
                config.macd_signal_period,
            )?,
            atr: Atr::new(config.atr_period)?,
            common: common::CommonStudyEngine::new(config)?,
        })
    }

    pub fn reset(&mut self) {
        if let Some(custom) = &mut self.custom_ema_adx {
            custom.reset();
        }
        self.scope = None;
        self.last_close_time_ms = None;
        self.sma.reset();
        self.ema.reset();
        self.bollinger.reset();
        self.vwap.reset();
        self.rsi.reset();
        self.macd.reset();
        self.atr.reset();
        self.common.reset();
    }

    pub fn ingest_closed(
        &mut self,
        bar: &PublicBar,
    ) -> Result<ChartStudyValues, ChartIndicatorError> {
        self.validate_next(bar)?;
        // A failed composite study must not leave partially advanced EMA or signal state.
        let mut next = self.clone();
        let values = next.update_unchecked(bar)?;
        next.scope = Some(ChartScope {
            symbol: bar.symbol.clone(),
            generation: bar.generation,
            interval_ms: bar.interval_ms,
        });
        next.last_close_time_ms = Some(bar.close_time_ms);
        *self = next;
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
            custom_ema_adx: self
                .custom_ema_adx
                .as_mut()
                .map(|study| study.update(bar))
                .transpose()?,
            sma: self.sma.update(bar)?,
            ema: self.ema.update(bar)?,
            bollinger: self.bollinger.update(bar)?,
            vwap: self.vwap.update(bar)?,
            rsi: self.rsi.update(bar)?,
            macd: self.macd.update(bar)?,
            atr: self.atr.update(bar)?,
            common: self.common.update(bar)?,
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

    use super::{ChartIndicatorError, ChartStudyConfig, ChartStudyEngine};

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
    fn commercial_indicator_set_is_ready_after_default_warmup()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut engine = ChartStudyEngine::standard()?;
        let mut latest = None;
        for sequence in 1..=140 {
            latest = Some(engine.ingest_closed(&bar(sequence)?)?);
        }
        let values = latest.ok_or("default warmup must emit a value")?;
        assert!(values.sma.is_some());
        assert!(values.ema.is_some());
        assert!(values.bollinger.is_some());
        assert!(values.vwap.is_some());
        assert!(values.rsi.is_some());
        assert!(values.macd.is_some());
        assert!(values.atr.is_some());
        assert!(values.common.sma_extra.second.is_some());
        assert!(values.common.sma_extra.third.is_some());
        assert!(values.common.ema_extra.second.is_some());
        assert!(values.common.ema_extra.third.is_some());
        assert!(values.common.wma.first.is_some());
        assert!(values.common.wma.second.is_some());
        assert!(values.common.wma.third.is_some());
        assert!(values.common.avl.is_some());
        assert!(values.common.trix.is_some());
        assert!(values.common.sar.is_some());
        assert!(values.common.supertrend.is_some());
        assert!(values.common.mfi.is_some());
        assert!(values.common.kdj.is_some());
        assert!(values.common.obv.is_some());
        assert!(values.common.cci.is_some());
        assert!(values.common.stoch_rsi.is_some());
        assert!(values.common.williams_r.is_some());
        assert!(values.common.dmi.is_some());
        assert!(values.common.momentum.is_some());
        assert!(values.common.emv.is_some());
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

    #[test]
    fn configurable_engine_rejects_ambiguous_periods() {
        let config = ChartStudyConfig {
            macd_fast_period: 26,
            ..ChartStudyConfig::default()
        };
        assert_eq!(
            ChartStudyEngine::with_config(&config).map(|_| ()),
            Err(ChartIndicatorError::InvalidParameters)
        );
        let config = ChartStudyConfig {
            sma_period: 0,
            ..ChartStudyConfig::default()
        };
        assert_eq!(
            ChartStudyEngine::with_config(&config).map(|_| ()),
            Err(ChartIndicatorError::InvalidParameters)
        );
    }
}
