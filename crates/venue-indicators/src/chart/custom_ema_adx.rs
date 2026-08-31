//! Read-only translation of the user-supplied AIScript v2 EMA/ADX/MACD study.
//! Raw-signal cooldown is deliberately literal, not rewritten as an execution cooldown.
use std::collections::VecDeque;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::{FieldState, PublicBar};

use super::{ChartIndicatorError as Error, Ema, Macd, Sma};
use crate::catalog::{BarIndicator as _, Reset as _, trend::Dmi};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EmaAdxConfig {
    pub ema_periods: [usize; 3],
    pub di_period: usize,
    pub adx_period: usize,
    pub adx_min: Decimal,
    pub macd_periods: [usize; 3],
    pub atr_period: usize,
    pub min_range_atr: Decimal,
    pub macd_atr: Decimal,
    pub exit_buffer_atr: Decimal,
    pub breakout_buffer_atr: Decimal,
    pub breakout_lookback: usize,
    pub cooldown_atr: Decimal,
    pub min_bars_between: usize,
    pub volume_filter: bool,
    pub volume_period: usize,
    pub volume_multiplier: Decimal,
}

impl Default for EmaAdxConfig {
    fn default() -> Self {
        Self {
            ema_periods: [9, 21, 55],
            di_period: 14,
            adx_period: 6,
            adx_min: Decimal::from(15),
            macd_periods: [12, 26, 9],
            atr_period: 14,
            min_range_atr: Decimal::new(35, 2),
            macd_atr: Decimal::new(5, 2),
            exit_buffer_atr: Decimal::new(20, 2),
            breakout_buffer_atr: Decimal::new(10, 2),
            breakout_lookback: 5,
            cooldown_atr: Decimal::ONE,
            min_bars_between: 6,
            volume_filter: false,
            volume_period: 20,
            volume_multiplier: Decimal::new(12, 1),
        }
    }
}

impl EmaAdxConfig {
    pub fn validate(&self) -> Result<(), Error> {
        let periods = self
            .ema_periods
            .into_iter()
            .chain(self.macd_periods)
            .chain([
                self.di_period,
                self.adx_period,
                self.atr_period,
                self.breakout_lookback,
                self.volume_period,
            ]);
        if periods.into_iter().any(|p| !(1..=10_000).contains(&p))
            || self.macd_periods[0] >= self.macd_periods[1]
            || self.min_bars_between > 10_000
            || self.adx_min < Decimal::ZERO
            || self.adx_min > Decimal::from(100)
            || [
                self.min_range_atr,
                self.macd_atr,
                self.exit_buffer_atr,
                self.breakout_buffer_atr,
                self.cooldown_atr,
                self.volume_multiplier,
            ]
            .into_iter()
            .any(|v| v < Decimal::ZERO || v > Decimal::from(1_000))
        {
            return Err(Error::InvalidParameters);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EmaAdxSignal {
    LongEntry,
    ShortEntry,
    LongExit,
    ShortExit,
    BullStart,
    BearStart,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EmaAdxValues {
    pub ema: [Option<Decimal>; 3],
    pub atr: Option<Decimal>,
    pub adx: Option<Decimal>,
    pub histogram: Option<Decimal>,
    pub trend: i8,
    pub virtual_position: i8,
    pub signals: Vec<EmaAdxSignal>,
}

#[derive(Clone, Debug)]
pub(super) struct EmaAdxStudy {
    config: EmaAdxConfig,
    emas: [Ema; 3],
    dmi: Dmi,
    macd: Macd,
    atr_mean: Sma,
    volume_mean: Sma,
    history: VecDeque<(Decimal, Decimal)>,
    previous_close: Option<Decimal>,
    previous_mid: Option<Decimal>,
    previous_hist: Option<Decimal>,
    previous_environment: [bool; 2],
    previous_raw: [bool; 2],
    position: i8,
    bar_index: usize,
    last_raw: Option<(usize, Decimal)>,
}

impl EmaAdxStudy {
    pub fn new(config: &EmaAdxConfig) -> Result<Self, Error> {
        config.validate()?;
        Ok(Self {
            config: config.clone(),
            emas: [
                Ema::new(config.ema_periods[0])?,
                Ema::new(config.ema_periods[1])?,
                Ema::new(config.ema_periods[2])?,
            ],
            dmi: Dmi::with_periods(config.di_period, config.adx_period)
                .map_err(|_| Error::InvalidParameters)?,
            macd: Macd::new(
                config.macd_periods[0],
                config.macd_periods[1],
                config.macd_periods[2],
            )?,
            atr_mean: Sma::new(config.atr_period)?,
            volume_mean: Sma::new(config.volume_period)?,
            history: VecDeque::new(),
            previous_close: None,
            previous_mid: None,
            previous_hist: None,
            previous_environment: [false; 2],
            previous_raw: [false; 2],
            position: 0,
            bar_index: 0,
            last_raw: None,
        })
    }

    pub fn reset(&mut self) {
        for ema in &mut self.emas {
            ema.reset();
        }
        self.dmi.reset();
        self.macd.reset();
        self.atr_mean.reset();
        self.volume_mean.reset();
        self.history.clear();
        self.previous_close = None;
        self.previous_mid = None;
        self.previous_hist = None;
        self.previous_environment = [false; 2];
        self.previous_raw = [false; 2];
        self.position = 0;
        self.bar_index = 0;
        self.last_raw = None;
    }

    pub fn update(&mut self, bar: &PublicBar) -> Result<EmaAdxValues, Error> {
        let close = bar.close.value();
        let high = bar.high.value();
        let low = bar.low.value();
        let ema = [
            self.emas[0].update(bar)?,
            self.emas[1].update(bar)?,
            self.emas[2].update(bar)?,
        ];
        let atr = if let Some(previous) = self.previous_close {
            let tr = sub(high, low)?
                .max(sub(high, previous)?.abs())
                .max(sub(low, previous)?.abs());
            self.atr_mean.update_value(tr)?
        } else {
            None
        };
        let volume = match bar.base_volume {
            FieldState::Known(v) => Some(v),
            _ => None,
        };
        let volume_average = if let Some(v) = volume {
            self.volume_mean.update_value(v)?
        } else {
            self.volume_mean.reset();
            None
        };
        let dmi = self.dmi.update(bar).map_err(|_| Error::Arithmetic)?;
        // AIScript MACD uses twice DIF-DEA; the shared chart MACD exposes DIF-DEA.
        let histogram = self
            .macd
            .update(bar)?
            .map(|v| mul(v.histogram, Decimal::TWO))
            .transpose()?;
        let adx = dmi
            .as_ref()
            .map(|v| Decimal::from_f64_retain(v.adx).ok_or(Error::Arithmetic))
            .transpose()?;
        let mut out = EmaAdxValues {
            ema,
            atr,
            adx,
            histogram,
            virtual_position: self.position,
            ..Default::default()
        };
        let mut environment = [false; 2];
        let mut raw = [false; 2];
        if let (Some(fast), Some(mid), Some(slow), Some(prev_mid)) =
            (ema[0], ema[1], ema[2], self.previous_mid)
        {
            let bull = fast > mid && mid > slow && mid > prev_mid;
            let bear = fast < mid && mid < slow && mid < prev_mid;
            out.trend = if bull {
                1
            } else if bear {
                -1
            } else {
                0
            };
            if let (Some(atr), Some(adx), Some(dmi), Some(hist), Some(prev_hist)) =
                (atr, adx, dmi, histogram, self.previous_hist)
            {
                let threshold = mul(atr, self.config.macd_atr)?;
                environment = [
                    bull && adx >= self.config.adx_min
                        && dmi.plus_di > dmi.minus_di
                        && hist > threshold
                        && hist > prev_hist,
                    bear && adx >= self.config.adx_min
                        && dmi.minus_di > dmi.plus_di
                        && hist < -threshold
                        && hist < prev_hist,
                ];
                let range_ok = sub(high, low)? >= mul(atr, self.config.min_range_atr)?;
                let volume_ok = if self.config.volume_filter {
                    match (volume, volume_average) {
                        (Some(v), Some(avg)) => v > mul(avg, self.config.volume_multiplier)?,
                        _ => false,
                    }
                } else {
                    true
                };
                if self.history.len() == self.config.breakout_lookback {
                    let highest = self
                        .history
                        .iter()
                        .map(|p| p.0)
                        .max()
                        .ok_or(Error::Arithmetic)?;
                    let lowest = self
                        .history
                        .iter()
                        .map(|p| p.1)
                        .min()
                        .ok_or(Error::Arithmetic)?;
                    let buffer = mul(atr, self.config.breakout_buffer_atr)?;
                    raw = [
                        environment[0]
                            && close > add(highest, buffer)?
                            && close > bar.open.value()
                            && range_ok
                            && volume_ok
                            && self.position != 1,
                        environment[1]
                            && close < sub(lowest, buffer)?
                            && close < bar.open.value()
                            && range_ok
                            && volume_ok
                            && self.position != -1,
                    ];
                }
                let cooldown = self.raw_cooldown(raw, close, atr)?;
                let entries = [
                    raw[0] && !self.previous_raw[0] && cooldown,
                    raw[1] && !self.previous_raw[1] && cooldown,
                ];
                let previous_close = self.previous_close.ok_or(Error::Arithmetic)?;
                let exit_buffer = mul(atr, self.config.exit_buffer_atr)?;
                let exits = [
                    self.position == 1
                        && ((previous_close >= prev_mid
                            && close < mid
                            && sub(mid, close)? > exit_buffer)
                            || (hist < Decimal::ZERO && prev_hist >= Decimal::ZERO)),
                    self.position == -1
                        && ((previous_close <= prev_mid
                            && close > mid
                            && sub(close, mid)? > exit_buffer)
                            || (hist > Decimal::ZERO && prev_hist <= Decimal::ZERO)),
                ];
                out.signals.extend(signal_events(
                    entries,
                    exits,
                    environment,
                    self.previous_environment,
                ));
                self.position = next_position(self.position, entries, exits);
                out.virtual_position = self.position;
            }
        }
        self.previous_close = Some(close);
        self.previous_mid = ema[1];
        self.previous_hist = histogram;
        self.previous_environment = environment;
        self.previous_raw = raw;
        self.history.push_back((high, low));
        if self.history.len() > self.config.breakout_lookback {
            self.history.pop_front();
        }
        self.bar_index = self.bar_index.saturating_add(1);
        Ok(out)
    }

    fn raw_cooldown(
        &mut self,
        raw: [bool; 2],
        close: Decimal,
        atr: Decimal,
    ) -> Result<bool, Error> {
        // valuewhen(..., 1) and barslast include the current true raw signal.
        // Preserve the supplied formula, even when positive cooldowns suppress all entries.
        if raw[0] || raw[1] {
            self.last_raw = Some((self.bar_index, close));
        }
        if atr <= Decimal::ZERO {
            return Ok(false);
        }
        let Some((index, price)) = self.last_raw else {
            return Ok(false);
        };
        let distance = sub(close, price)?
            .abs()
            .checked_div(atr)
            .ok_or(Error::Arithmetic)?;
        Ok(distance >= self.config.cooldown_atr
            && self.bar_index.saturating_sub(index) >= self.config.min_bars_between)
    }
}

fn next_position(previous: i8, entries: [bool; 2], exits: [bool; 2]) -> i8 {
    if entries[0] {
        1
    } else if entries[1] {
        -1
    } else if (previous == 1 && exits[0]) || (previous == -1 && exits[1]) {
        0
    } else {
        previous
    }
}

fn signal_events(
    entries: [bool; 2],
    exits: [bool; 2],
    environment: [bool; 2],
    previous: [bool; 2],
) -> Vec<EmaAdxSignal> {
    use EmaAdxSignal::*;
    [
        (entries[0], LongEntry),
        (entries[1], ShortEntry),
        (exits[0], LongExit),
        (exits[1], ShortExit),
        (environment[0] && !previous[0], BullStart),
        (environment[1] && !previous[1], BearStart),
    ]
    .into_iter()
    .filter_map(|(enabled, signal)| enabled.then_some(signal))
    .collect()
}

fn add(a: Decimal, b: Decimal) -> Result<Decimal, Error> {
    a.checked_add(b).ok_or(Error::Arithmetic)
}
fn sub(a: Decimal, b: Decimal) -> Result<Decimal, Error> {
    a.checked_sub(b).ok_or(Error::Arithmetic)
}
fn mul(a: Decimal, b: Decimal) -> Result<Decimal, Error> {
    a.checked_mul(b).ok_or(Error::Arithmetic)
}

#[cfg(test)]
#[path = "custom_ema_adx_tests.rs"]
mod tests;
