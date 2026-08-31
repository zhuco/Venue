use rust_decimal::Decimal;
use venue_domain::PublicBar;

use crate::catalog::{
    BarIndicator as _, IndicatorError, Reset as _,
    momentum::{Cci, Mfi, Momentum, StochRsi, Stochastic, WilliamsR},
    trend::{Dmi, ParabolicSar, SuperTrend, Trix, Wma},
    volume::{AverageValueLine, EaseOfMovement, Obv},
};

use super::{ChartIndicatorError, Ema, Sma};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TripleValue {
    pub first: Option<Decimal>,
    pub second: Option<Decimal>,
    pub third: Option<Decimal>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KdjValue {
    pub k: Decimal,
    pub d: Decimal,
    pub j: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PairValue {
    pub first: Decimal,
    pub second: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DmiValue {
    pub plus_di: Decimal,
    pub minus_di: Decimal,
    pub adx: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectionalValue {
    pub value: Decimal,
    pub rising: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommonStudyValues {
    pub sma_extra: PairValueOption,
    pub ema_extra: PairValueOption,
    pub wma: TripleValue,
    pub avl: Option<Decimal>,
    pub trix: Option<Decimal>,
    pub sar: Option<DirectionalValue>,
    pub supertrend: Option<DirectionalValue>,
    pub mfi: Option<Decimal>,
    pub kdj: Option<KdjValue>,
    pub obv: Option<Decimal>,
    pub cci: Option<Decimal>,
    pub stoch_rsi: Option<PairValue>,
    pub williams_r: Option<Decimal>,
    pub dmi: Option<DmiValue>,
    pub momentum: Option<Decimal>,
    pub emv: Option<Decimal>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PairValueOption {
    pub second: Option<Decimal>,
    pub third: Option<Decimal>,
}

#[derive(Clone, Debug)]
pub(super) struct CommonStudyEngine {
    sma_second: Sma,
    sma_third: Sma,
    ema_second: Ema,
    ema_third: Ema,
    wma_first: Wma,
    wma_second: Wma,
    wma_third: Wma,
    avl: AverageValueLine,
    trix: Trix,
    sar: ParabolicSar,
    supertrend: SuperTrend,
    mfi: Mfi,
    kdj: Stochastic,
    obv: Obv,
    cci: Cci,
    stoch_rsi: StochRsi,
    williams_r: WilliamsR,
    dmi: Dmi,
    momentum: Momentum,
    emv: EaseOfMovement,
}

impl CommonStudyEngine {
    pub fn new(config: &super::ChartStudyConfig) -> Result<Self, ChartIndicatorError> {
        Ok(Self {
            sma_second: Sma::new(config.sma_second_period)?,
            sma_third: Sma::new(config.sma_third_period)?,
            ema_second: Ema::new(config.ema_second_period)?,
            ema_third: Ema::new(config.ema_third_period)?,
            wma_first: Wma::new(config.wma_period).map_err(map_catalog_error)?,
            wma_second: Wma::new(config.wma_second_period).map_err(map_catalog_error)?,
            wma_third: Wma::new(config.wma_third_period).map_err(map_catalog_error)?,
            avl: AverageValueLine::new(),
            trix: Trix::new(config.trix_period).map_err(map_catalog_error)?,
            sar: ParabolicSar::new(
                decimal_to_f64(config.sar_step)?,
                decimal_to_f64(config.sar_maximum)?,
            )
            .map_err(map_catalog_error)?,
            supertrend: SuperTrend::new(
                config.supertrend_period,
                decimal_to_f64(config.supertrend_multiplier)?,
            )
            .map_err(map_catalog_error)?,
            mfi: Mfi::new(config.mfi_period).map_err(map_catalog_error)?,
            kdj: Stochastic::new(config.kdj_period, config.kdj_signal_period)
                .map_err(map_catalog_error)?,
            obv: Obv::new(),
            cci: Cci::new(config.cci_period).map_err(map_catalog_error)?,
            stoch_rsi: StochRsi::new(
                config.stoch_rsi_period,
                config.stoch_rsi_stochastic_period,
                config.stoch_rsi_signal_period,
            )
            .map_err(map_catalog_error)?,
            williams_r: WilliamsR::new(config.williams_r_period).map_err(map_catalog_error)?,
            dmi: Dmi::new(config.dmi_period).map_err(map_catalog_error)?,
            momentum: Momentum::new(config.momentum_period).map_err(map_catalog_error)?,
            emv: EaseOfMovement::new(config.emv_period).map_err(map_catalog_error)?,
        })
    }

    pub fn reset(&mut self) {
        self.sma_second.reset();
        self.sma_third.reset();
        self.ema_second.reset();
        self.ema_third.reset();
        self.wma_first.reset();
        self.wma_second.reset();
        self.wma_third.reset();
        self.avl.reset();
        self.trix.reset();
        self.sar.reset();
        self.supertrend.reset();
        self.mfi.reset();
        self.kdj.reset();
        self.obv.reset();
        self.cci.reset();
        self.stoch_rsi.reset();
        self.williams_r.reset();
        self.dmi.reset();
        self.momentum.reset();
        self.emv.reset();
    }

    pub fn update(&mut self, bar: &PublicBar) -> Result<CommonStudyValues, ChartIndicatorError> {
        let sma_extra = PairValueOption {
            second: self.sma_second.update(bar)?,
            third: self.sma_third.update(bar)?,
        };
        let ema_extra = PairValueOption {
            second: self.ema_second.update(bar)?,
            third: self.ema_third.update(bar)?,
        };
        let wma = TripleValue {
            first: scalar(self.wma_first.update(bar).map_err(map_catalog_error)?)?,
            second: scalar(self.wma_second.update(bar).map_err(map_catalog_error)?)?,
            third: scalar(self.wma_third.update(bar).map_err(map_catalog_error)?)?,
        };
        let sar = self
            .sar
            .update(bar)
            .map_err(map_catalog_error)?
            .map(|value| {
                Ok(DirectionalValue {
                    value: decimal(value.value)?,
                    rising: value.rising,
                })
            })
            .transpose()?;
        let supertrend = self
            .supertrend
            .update(bar)
            .map_err(map_catalog_error)?
            .map(|value| {
                Ok(DirectionalValue {
                    value: decimal(value.value)?,
                    rising: value.direction > 0,
                })
            })
            .transpose()?;
        let kdj = self
            .kdj
            .update(bar)
            .map_err(map_catalog_error)?
            .map(|value| {
                let k = decimal(value.k)?;
                let d = decimal(value.d)?;
                Ok(KdjValue {
                    k,
                    d,
                    j: k * Decimal::from(3) - d * Decimal::from(2),
                })
            })
            .transpose()?;
        let stoch_rsi = self
            .stoch_rsi
            .update(bar)
            .map_err(map_catalog_error)?
            .map(|value| {
                Ok(PairValue {
                    first: decimal(value.k)?,
                    second: decimal(value.d)?,
                })
            })
            .transpose()?;
        let dmi = self
            .dmi
            .update(bar)
            .map_err(map_catalog_error)?
            .map(|value| {
                Ok(DmiValue {
                    plus_di: decimal(value.plus_di)?,
                    minus_di: decimal(value.minus_di)?,
                    adx: decimal(value.adx)?,
                })
            })
            .transpose()?;
        Ok(CommonStudyValues {
            sma_extra,
            ema_extra,
            wma,
            avl: scalar(self.avl.update(bar).map_err(map_catalog_error)?)?,
            trix: self
                .trix
                .update(bar)
                .map_err(map_catalog_error)?
                .map(|value| decimal(value.line))
                .transpose()?,
            sar,
            supertrend,
            mfi: scalar(self.mfi.update(bar).map_err(map_catalog_error)?)?,
            kdj,
            obv: scalar(self.obv.update(bar).map_err(map_catalog_error)?)?,
            cci: scalar(self.cci.update(bar).map_err(map_catalog_error)?)?,
            stoch_rsi,
            williams_r: scalar(self.williams_r.update(bar).map_err(map_catalog_error)?)?,
            dmi,
            momentum: scalar(self.momentum.update(bar).map_err(map_catalog_error)?)?,
            emv: scalar(self.emv.update(bar).map_err(map_catalog_error)?)?,
        })
    }
}

fn scalar(value: Option<f64>) -> Result<Option<Decimal>, ChartIndicatorError> {
    value.map(decimal).transpose()
}

fn decimal(value: f64) -> Result<Decimal, ChartIndicatorError> {
    Decimal::from_f64_retain(value).ok_or(ChartIndicatorError::Arithmetic)
}

fn decimal_to_f64(value: Decimal) -> Result<f64, ChartIndicatorError> {
    value
        .to_string()
        .parse()
        .map_err(|_| ChartIndicatorError::Arithmetic)
}

fn map_catalog_error(error: IndicatorError) -> ChartIndicatorError {
    match error {
        IndicatorError::InvalidParameter { .. } => ChartIndicatorError::InvalidParameters,
        IndicatorError::VolumeUnavailable => ChartIndicatorError::VolumeUnavailable,
        IndicatorError::DecimalConversion
        | IndicatorError::InvalidBar
        | IndicatorError::InvalidBook => ChartIndicatorError::Arithmetic,
    }
}
