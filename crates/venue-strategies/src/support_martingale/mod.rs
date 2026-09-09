//! Pure rules for the support based staged long strategy.
//!
//! The module consumes normalized, closed public bars and signed execution facts supplied by a
//! host. It deliberately has no clock, network, credential, persistence, or order-client access.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::{FieldState, Price, PublicBar, Symbol};
use venue_indicators::chart::{Atr, Ema, Rsi};

pub const SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SupportMartingaleConfig {
    pub symbol: Symbol,
    pub first_order_notional: Decimal,
    pub max_entries: u8,
    pub size_multiplier: Decimal,
    pub target_profit_rate: Decimal,
    pub minimum_profit_quote: Decimal,
    pub environment_period: u64,
    pub support_period: u64,
    pub callback_period: u64,
    pub environment_ema_fast: usize,
    pub environment_ema_slow: usize,
    pub rsi_period: usize,
    pub atr_period: usize,
    pub oversold_rsi: Decimal,
    pub volume_multiple: Decimal,
    pub callback_max_bars: usize,
    pub support_tolerance_atr: Decimal,
    pub max_signal_age_ms: u64,
    pub cooldown_ms: u64,
}

impl SupportMartingaleConfig {
    pub fn research_default() -> Result<Self, StrategyError> {
        Ok(Self {
            symbol: Symbol::new("BTC", "USDT")
                .map_err(|_| StrategyError::InvalidConfig("default symbol"))?,
            first_order_notional: Decimal::from(5),
            max_entries: 10,
            size_multiplier: Decimal::new(125, 2),
            target_profit_rate: Decimal::new(5, 3),
            minimum_profit_quote: Decimal::ZERO,
            environment_period: 14_400_000,
            support_period: 3_600_000,
            callback_period: 900_000,
            environment_ema_fast: 20,
            environment_ema_slow: 60,
            rsi_period: 14,
            atr_period: 14,
            oversold_rsi: Decimal::from(35),
            volume_multiple: Decimal::new(18, 1),
            callback_max_bars: 4,
            support_tolerance_atr: Decimal::new(25, 2),
            max_signal_age_ms: 30_000,
            cooldown_ms: 1_800_000,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StrategyError {
    #[error("invalid configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("bar is invalid, not closed, out of order, or has a different symbol")]
    InvalidBar,
    #[error("indicator is not warmed up")]
    Warmup,
    #[error("decimal arithmetic overflow")]
    Arithmetic,
    #[error("required quote volume is unavailable")]
    MissingVolume,
    #[error("price is outside instrument limits")]
    PriceBlocked,
}

impl SupportMartingaleConfig {
    pub fn validate(&self) -> Result<(), StrategyError> {
        if self.first_order_notional <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "first_order_notional must be positive",
            ));
        }
        if !(1..=100).contains(&self.max_entries) {
            return Err(StrategyError::InvalidConfig("max_entries must be 1..=100"));
        }
        if self.size_multiplier < Decimal::ONE {
            return Err(StrategyError::InvalidConfig(
                "size_multiplier must be at least one",
            ));
        }
        if self.target_profit_rate < Decimal::ZERO || self.minimum_profit_quote < Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "profit thresholds must be non-negative",
            ));
        }
        if self.environment_ema_fast == 0
            || self.environment_ema_fast >= self.environment_ema_slow
            || self.environment_ema_slow > 1000
        {
            return Err(StrategyError::InvalidConfig("EMA periods are invalid"));
        }
        if self.rsi_period == 0 || self.atr_period == 0 || self.callback_max_bars == 0 {
            return Err(StrategyError::InvalidConfig(
                "indicator periods are invalid",
            ));
        }
        if self.oversold_rsi <= Decimal::ZERO
            || self.oversold_rsi >= Decimal::from(50)
            || self.volume_multiple <= Decimal::ONE
        {
            return Err(StrategyError::InvalidConfig(
                "callback thresholds are invalid",
            ));
        }
        if self.support_tolerance_atr <= Decimal::ZERO || self.max_signal_age_ms == 0 {
            return Err(StrategyError::InvalidConfig(
                "support and signal limits are invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketEnvironment {
    Up,
    Down,
    Range,
    Neutral,
    Warmup,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EnvironmentSignal {
    pub environment: MarketEnvironment,
    pub close: Price,
    pub ema_fast: Option<Decimal>,
    pub ema_slow: Option<Decimal>,
    pub atr: Option<Decimal>,
    pub observed_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SupportZone {
    pub id: String,
    pub lower: Price,
    pub upper: Price,
    pub source_price: Price,
    pub confirmed_at_ms: u64,
    pub version: u64,
}

impl SupportZone {
    pub fn contains(&self, price: Decimal) -> bool {
        price >= self.lower.value() && price <= self.upper.value()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    First,
    Add,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CallbackSignal {
    pub kind: EntryKind,
    pub support_id: String,
    pub confirmed_at_ms: u64,
    pub confirmation_low: Price,
    pub reference_price: Price,
    pub rsi: Decimal,
    pub relative_volume: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PositionCost {
    pub quantity: Decimal,
    pub buy_notional: Decimal,
    pub entry_fee: Decimal,
    pub funding: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TakeProfitInput {
    pub position: PositionCost,
    pub target_profit_rate: Decimal,
    pub minimum_profit_quote: Decimal,
    pub exit_fee_rate: Option<Decimal>,
    pub tick_size: Decimal,
    pub minimum_price: Option<Price>,
    pub maximum_price: Option<Price>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TakeProfitResult {
    Ready {
        price: Price,
        estimated_net_profit: Decimal,
        estimated: bool,
    },
    Blocked {
        reason: TakeProfitBlock,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TakeProfitBlock {
    ZeroQuantity,
    MissingExitFee,
    InvalidTick,
    PriceOutOfRange,
    Arithmetic,
}

pub fn classify_environment(
    bars: &[PublicBar],
    config: &SupportMartingaleConfig,
    now_ms: u64,
) -> Result<EnvironmentSignal, StrategyError> {
    config.validate()?;
    validate_series(bars, &config.symbol, config.environment_period, now_ms)?;
    let mut fast = Ema::new(config.environment_ema_fast).map_err(|_| StrategyError::Arithmetic)?;
    let mut slow = Ema::new(config.environment_ema_slow).map_err(|_| StrategyError::Arithmetic)?;
    let mut atr = Atr::new(config.atr_period).map_err(|_| StrategyError::Arithmetic)?;
    let mut fast_values = Vec::with_capacity(bars.len());
    let mut slow_values = Vec::with_capacity(bars.len());
    let mut atr_value = None;
    for bar in bars {
        fast_values.push(fast.update(bar).map_err(|_| StrategyError::Arithmetic)?);
        slow_values.push(slow.update(bar).map_err(|_| StrategyError::Arithmetic)?);
        atr_value = atr.update(bar).map_err(|_| StrategyError::Arithmetic)?;
    }
    let Some(ef) = fast_values.last().copied().flatten() else {
        return Ok(EnvironmentSignal {
            environment: MarketEnvironment::Warmup,
            close: bars.last().ok_or(StrategyError::Warmup)?.close,
            ema_fast: None,
            ema_slow: None,
            atr: None,
            observed_at_ms: bars.last().ok_or(StrategyError::Warmup)?.close_time_ms,
        });
    };
    let Some(es) = slow_values.last().copied().flatten() else {
        return Ok(EnvironmentSignal {
            environment: MarketEnvironment::Warmup,
            close: bars.last().ok_or(StrategyError::Warmup)?.close,
            ema_fast: Some(ef),
            ema_slow: None,
            atr: atr_value,
            observed_at_ms: bars.last().ok_or(StrategyError::Warmup)?.close_time_ms,
        });
    };
    let ago = fast_values
        .get(fast_values.len().saturating_sub(4))
        .copied()
        .flatten()
        .unwrap_or(ef);
    let close = bars.last().ok_or(StrategyError::Warmup)?.close;
    let swing_highs = confirmed_swing_highs(bars);
    let swing_lows = confirmed_swing_lows(bars);
    let up_structure_holds = swing_lows
        .last()
        .is_some_and(|(_, low)| close.value() >= *low);
    let down_structure_holds =
        descending_last_two(&swing_highs) && descending_last_two(&swing_lows);
    let environment = if atr_value.is_none() {
        MarketEnvironment::Warmup
    } else if close.value() > ef && ef > es && ef > ago && up_structure_holds {
        MarketEnvironment::Up
    } else if close.value() < ef && ef < es && ef < ago && down_structure_holds {
        MarketEnvironment::Down
    } else if atr_value.is_some_and(|value| confirmed_range(bars, ef, ago, value)) {
        MarketEnvironment::Range
    } else {
        MarketEnvironment::Neutral
    };
    Ok(EnvironmentSignal {
        environment,
        close,
        ema_fast: Some(ef),
        ema_slow: Some(es),
        atr: atr_value,
        observed_at_ms: bars.last().ok_or(StrategyError::Warmup)?.close_time_ms,
    })
}

fn confirmed_swing_highs(bars: &[PublicBar]) -> Vec<(usize, Decimal)> {
    (2..bars.len().saturating_sub(2))
        .filter(|index| {
            let value = bars[*index].high.value();
            value > bars[*index - 1].high.value()
                && value > bars[*index - 2].high.value()
                && value >= bars[*index + 1].high.value()
                && value >= bars[*index + 2].high.value()
        })
        .map(|index| (index, bars[index].high.value()))
        .collect()
}

fn confirmed_swing_lows(bars: &[PublicBar]) -> Vec<(usize, Decimal)> {
    (2..bars.len().saturating_sub(2))
        .filter(|index| {
            let value = bars[*index].low.value();
            value < bars[*index - 1].low.value()
                && value < bars[*index - 2].low.value()
                && value <= bars[*index + 1].low.value()
                && value <= bars[*index + 2].low.value()
        })
        .map(|index| (index, bars[index].low.value()))
        .collect()
}

fn descending_last_two(points: &[(usize, Decimal)]) -> bool {
    points
        .get(points.len().saturating_sub(2)..)
        .is_some_and(|points| points.len() == 2 && points[1].1 < points[0].1)
}

fn confirmed_range(
    bars: &[PublicBar],
    ema_fast: Decimal,
    prior_ema: Decimal,
    atr: Decimal,
) -> bool {
    if bars.len() < 20 || atr <= Decimal::ZERO {
        return false;
    }
    let window = &bars[bars.len() - 20..];
    let Some(high) = window.iter().map(|bar| bar.high.value()).max() else {
        return false;
    };
    let Some(low) = window.iter().map(|bar| bar.low.value()).min() else {
        return false;
    };
    if high - low > atr * Decimal::from(6)
        || (ema_fast - prior_ema).abs() > atr * Decimal::new(1, 1)
    {
        return false;
    }
    let tolerance = atr * Decimal::new(25, 2);
    boundary_has_two_confirmations(&confirmed_swing_highs(window), high, tolerance)
        && boundary_has_two_confirmations(&confirmed_swing_lows(window), low, tolerance)
}

fn boundary_has_two_confirmations(
    points: &[(usize, Decimal)],
    boundary: Decimal,
    tolerance: Decimal,
) -> bool {
    let matches = points
        .iter()
        .filter(|(_, value)| (*value - boundary).abs() <= tolerance)
        .map(|(index, _)| *index)
        .collect::<Vec<_>>();
    matches
        .iter()
        .enumerate()
        .any(|(left, first)| matches[left + 1..].iter().any(|second| second - first >= 3))
}

pub fn detect_supports(
    bars: &[PublicBar],
    config: &SupportMartingaleConfig,
    now_ms: u64,
) -> Result<Vec<SupportZone>, StrategyError> {
    validate_series(bars, &config.symbol, config.support_period, now_ms)?;
    if bars.len() < 5 {
        return Ok(Vec::new());
    }
    let mut atr = Atr::new(config.atr_period).map_err(|_| StrategyError::Arithmetic)?;
    let mut atrs = Vec::with_capacity(bars.len());
    for bar in bars {
        atrs.push(atr.update(bar).map_err(|_| StrategyError::Arithmetic)?);
    }
    let mut result = Vec::new();
    let swing_highs = confirmed_swing_highs(bars);
    for i in 2..bars.len().saturating_sub(2) {
        if bars[i].low > bars[i - 1].low
            || bars[i].low > bars[i - 2].low
            || bars[i].low > bars[i + 1].low
            || bars[i].low > bars[i + 2].low
        {
            continue;
        }
        let Some((_, prior_high)) = swing_highs.iter().rev().find(|(index, _)| *index < i) else {
            continue;
        };
        let Some(breakout) = bars[i + 2..]
            .iter()
            .find(|bar| bar.close.value() > *prior_high)
        else {
            continue;
        };
        let width = atrs[i].unwrap_or(bars[i].high.value() - bars[i].low.value())
            * config.support_tolerance_atr;
        let lower = (bars[i].low.value() - width).max(Decimal::ZERO);
        let upper = bars[i].low.value() + width;
        let confirmed = breakout.close_time_ms;
        let id = format!("{}-{}", bars[i].symbol, confirmed);
        result.push(SupportZone {
            id,
            lower: Price::new(lower).map_err(|_| StrategyError::Arithmetic)?,
            upper: Price::new(upper).map_err(|_| StrategyError::Arithmetic)?,
            source_price: bars[i].low,
            confirmed_at_ms: confirmed,
            version: confirmed,
        });
    }
    Ok(result)
}

pub fn evaluate_callback(
    bars: &[PublicBar],
    support: &SupportZone,
    config: &SupportMartingaleConfig,
    kind: EntryKind,
    now_ms: u64,
) -> Result<Option<CallbackSignal>, StrategyError> {
    validate_series(bars, &config.symbol, config.callback_period, now_ms)?;
    if bars.is_empty() {
        return Ok(None);
    }
    let mut rsi = Rsi::new(config.rsi_period).map_err(|_| StrategyError::Arithmetic)?;
    let mut values = Vec::with_capacity(bars.len());
    for bar in bars {
        values.push(rsi.update(bar).map_err(|_| StrategyError::Arithmetic)?);
    }
    let end = bars.len();
    let start = end.saturating_sub(config.callback_max_bars);
    let mut touched_at = None;
    for i in start..end {
        if bars[i].close.value() < support.lower.value() {
            touched_at = None;
            continue;
        }
        if touched_at.is_none()
            && bars[i].low.value() <= support.upper.value()
            && bars[i].high.value() >= support.lower.value()
        {
            touched_at = Some(i);
        }
        let Some(touch) = touched_at else {
            continue;
        };
        let Some(r) = values[i] else {
            continue;
        };
        if r <= config.oversold_rsi {
            continue;
        }
        if i == 0
            || bars[i].close.value() <= support.upper.value()
            || bars[i].close.value() <= bars[i - 1].high.value()
            || now_ms < bars[i].close_time_ms
            || now_ms.saturating_sub(bars[i].close_time_ms) > config.max_signal_age_ms
        {
            continue;
        }
        if !values[touch..=i]
            .iter()
            .flatten()
            .any(|value| *value < config.oversold_rsi)
        {
            continue;
        }
        let mut event_volume = Decimal::ZERO;
        for event_index in touch..=i {
            event_volume = event_volume.max(relative_volume(bars, event_index)?);
        }
        if event_volume < config.volume_multiple {
            continue;
        }
        return Ok(Some(CallbackSignal {
            kind,
            support_id: support.id.clone(),
            confirmed_at_ms: bars[i].close_time_ms,
            confirmation_low: bars[i].low,
            reference_price: bars[i].close,
            rsi: r,
            relative_volume: event_volume,
        }));
    }
    Ok(None)
}

pub fn calculate_take_profit(input: &TakeProfitInput) -> TakeProfitResult {
    if input.position.quantity <= Decimal::ZERO {
        return TakeProfitResult::Blocked {
            reason: TakeProfitBlock::ZeroQuantity,
        };
    }
    if input.tick_size <= Decimal::ZERO {
        return TakeProfitResult::Blocked {
            reason: TakeProfitBlock::InvalidTick,
        };
    }
    let Some(exit_fee) = input.exit_fee_rate else {
        return TakeProfitResult::Blocked {
            reason: TakeProfitBlock::MissingExitFee,
        };
    };
    if exit_fee < Decimal::ZERO || exit_fee >= Decimal::ONE {
        return TakeProfitResult::Blocked {
            reason: TakeProfitBlock::MissingExitFee,
        };
    }
    let target = input
        .position
        .buy_notional
        .checked_add(input.position.entry_fee)
        .and_then(|v| v.checked_add(input.position.funding))
        .and_then(|v| {
            v.checked_add(
                input
                    .position
                    .buy_notional
                    .checked_mul(input.target_profit_rate)?,
            )
        })
        .and_then(|v| v.checked_add(input.minimum_profit_quote));
    let Some(target) = target else {
        return TakeProfitResult::Blocked {
            reason: TakeProfitBlock::Arithmetic,
        };
    };
    let Some(net_rate) = Decimal::ONE.checked_sub(exit_fee) else {
        return TakeProfitResult::Blocked {
            reason: TakeProfitBlock::Arithmetic,
        };
    };
    let Some(denom) = input.position.quantity.checked_mul(net_rate) else {
        return TakeProfitResult::Blocked {
            reason: TakeProfitBlock::Arithmetic,
        };
    };
    let Some(raw) = target.checked_div(denom) else {
        return TakeProfitResult::Blocked {
            reason: TakeProfitBlock::Arithmetic,
        };
    };
    let ticks = (raw / input.tick_size).ceil();
    let Some(price_value) = ticks.checked_mul(input.tick_size) else {
        return TakeProfitResult::Blocked {
            reason: TakeProfitBlock::Arithmetic,
        };
    };
    let Ok(price) = Price::new(price_value) else {
        return TakeProfitResult::Blocked {
            reason: TakeProfitBlock::PriceOutOfRange,
        };
    };
    if input.minimum_price.is_some_and(|p| price < p)
        || input.maximum_price.is_some_and(|p| price > p)
    {
        return TakeProfitResult::Blocked {
            reason: TakeProfitBlock::PriceOutOfRange,
        };
    }
    let net = price_value
        .checked_mul(input.position.quantity)
        .and_then(|v| v.checked_sub(input.position.buy_notional))
        .and_then(|v| v.checked_sub(input.position.entry_fee))
        .and_then(|v| v.checked_sub(input.position.funding))
        .and_then(|v| {
            v.checked_sub(
                price_value
                    .checked_mul(input.position.quantity)?
                    .checked_mul(exit_fee)?,
            )
        });
    match net {
        Some(value) => TakeProfitResult::Ready {
            price,
            estimated_net_profit: value,
            estimated: false,
        },
        None => TakeProfitResult::Blocked {
            reason: TakeProfitBlock::Arithmetic,
        },
    }
}

fn validate_series(
    bars: &[PublicBar],
    symbol: &Symbol,
    interval: u64,
    now_ms: u64,
) -> Result<(), StrategyError> {
    if bars.is_empty() {
        return Err(StrategyError::Warmup);
    }
    let mut previous = None;
    for bar in bars {
        if &bar.symbol != symbol
            || bar.interval_ms != interval
            || !bar.is_valid()
            || bar.close_time_ms > now_ms
            || previous.is_some_and(|p| bar.open_time_ms <= p)
        {
            return Err(StrategyError::InvalidBar);
        }
        previous = Some(bar.open_time_ms);
    }
    Ok(())
}

fn relative_volume(bars: &[PublicBar], index: usize) -> Result<Decimal, StrategyError> {
    let FieldState::Known(current) = &bars[index].quote_volume else {
        return Err(StrategyError::MissingVolume);
    };
    if index < 20 {
        return Ok(Decimal::ZERO);
    }
    let mut values = bars[index - 20..index]
        .iter()
        .map(|bar| match &bar.quote_volume {
            FieldState::Known(value) => Ok(*value),
            _ => Err(StrategyError::MissingVolume),
        })
        .collect::<Result<Vec<_>, _>>()?;
    values.sort();
    let median = values[values.len() / 2];
    if median <= Decimal::ZERO {
        return Err(StrategyError::MissingVolume);
    }
    current.checked_div(median).ok_or(StrategyError::Arithmetic)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_unclassified_environment_is_neutral_and_short_history_is_warmup()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut bars = callback_bars()?;
        let mut config = SupportMartingaleConfig::research_default()?;
        config.environment_period = 900_000;
        config.environment_ema_fast = 2;
        config.environment_ema_slow = 3;
        for bar in &mut bars {
            bar.open = Price::new(Decimal::from(100))?;
            bar.close = bar.open;
            bar.high = Price::new(Decimal::from(101))?;
            bar.low = Price::new(Decimal::from(99))?;
            bar.base_volume = FieldState::Known(Decimal::ONE);
            bar.quote_volume = FieldState::Known(Decimal::from(100));
            bar.taker_buy_base_volume = bar.base_volume.clone();
            bar.taker_buy_quote_volume = bar.quote_volume.clone();
        }
        let now = bars.last().ok_or("no bar")?.close_time_ms + 1;
        let ready = classify_environment(&bars, &config, now)?;
        assert_eq!(ready.environment, MarketEnvironment::Neutral);
        assert!(ready.ema_fast.is_some() && ready.ema_slow.is_some() && ready.atr.is_some());
        config.environment_ema_slow = 60;
        assert_eq!(
            classify_environment(&bars, &config, now)?.environment,
            MarketEnvironment::Warmup
        );
        Ok(())
    }

    fn callback_bars() -> Result<Vec<PublicBar>, Box<dyn std::error::Error>> {
        let symbol = Symbol::new("BTC", "USDT")?;
        let interval = 900_000_u64;
        let mut closes = vec![Decimal::from(100); 22];
        closes.extend([Decimal::from(90), Decimal::from(80), Decimal::from(95)]);
        let mut previous = Decimal::from(100);
        closes
            .into_iter()
            .enumerate()
            .map(|(index, close)| {
                let open_time_ms = u64::try_from(index)?
                    .checked_mul(interval)
                    .ok_or("time overflow")?
                    .checked_add(interval)
                    .ok_or("time overflow")?;
                let high = previous.max(close) + Decimal::ONE;
                let low = previous.min(close) - Decimal::ONE;
                let volume = if index == 23 {
                    Decimal::from(200)
                } else {
                    Decimal::from(100)
                };
                let base_volume = volume.checked_div(close).ok_or("volume overflow")?;
                let bar = PublicBar {
                    symbol: symbol.clone(),
                    generation: 1,
                    received_at_ms: open_time_ms + interval,
                    sequence: u64::try_from(index)? + 1,
                    open_time_ms,
                    close_time_ms: open_time_ms + interval - 1,
                    interval_ms: interval,
                    open: Price::new(previous)?,
                    high: Price::new(high)?,
                    low: Price::new(low)?,
                    close: Price::new(close)?,
                    base_volume: FieldState::Known(base_volume),
                    quote_volume: FieldState::Known(volume),
                    trade_count: FieldState::Known(1),
                    taker_buy_base_volume: FieldState::Known(base_volume),
                    taker_buy_quote_volume: FieldState::Known(volume),
                };
                previous = close;
                Ok(bar)
            })
            .collect()
    }

    fn callback_fixture()
    -> Result<(Vec<PublicBar>, SupportZone, SupportMartingaleConfig), Box<dyn std::error::Error>>
    {
        let bars = callback_bars()?;
        let mut config = SupportMartingaleConfig::research_default()?;
        config.rsi_period = 2;
        let support = SupportZone {
            id: "support-1".into(),
            lower: Price::new(Decimal::from(79))?,
            upper: Price::new(Decimal::from(81))?,
            source_price: Price::new(Decimal::from(80))?,
            confirmed_at_ms: bars[20].close_time_ms,
            version: 1,
        };
        Ok((bars, support, config))
    }
    #[test]
    fn config_rejects_zero_budget() {
        let mut c = match SupportMartingaleConfig::research_default() {
            Ok(value) => value,
            Err(_) => return,
        };
        c.first_order_notional = Decimal::ZERO;
        assert!(c.validate().is_err());
    }
    #[test]
    fn tp_rounds_up_and_includes_exit_fee() {
        let r = calculate_take_profit(&TakeProfitInput {
            position: PositionCost {
                quantity: Decimal::ONE,
                buy_notional: Decimal::from(100),
                entry_fee: Decimal::ONE,
                funding: Decimal::ZERO,
            },
            target_profit_rate: Decimal::new(5, 3),
            minimum_profit_quote: Decimal::ZERO,
            exit_fee_rate: Some(Decimal::new(1, 3)),
            tick_size: Decimal::new(1, 1),
            minimum_price: None,
            maximum_price: None,
        });
        assert!(
            matches!(r, TakeProfitResult::Ready { price, .. } if price.value() >= Decimal::new(1005, 1))
        );
    }
    #[test]
    fn missing_exit_fee_blocks_tp() {
        let r = calculate_take_profit(&TakeProfitInput {
            position: PositionCost {
                quantity: Decimal::ONE,
                buy_notional: Decimal::ONE,
                entry_fee: Decimal::ZERO,
                funding: Decimal::ZERO,
            },
            target_profit_rate: Decimal::ZERO,
            minimum_profit_quote: Decimal::ZERO,
            exit_fee_rate: None,
            tick_size: Decimal::ONE,
            minimum_price: None,
            maximum_price: None,
        });
        assert_eq!(
            r,
            TakeProfitResult::Blocked {
                reason: TakeProfitBlock::MissingExitFee
            }
        );
    }

    #[test]
    fn callback_requires_touch_prior_oversold_volume_and_fresh_breakout()
    -> Result<(), Box<dyn std::error::Error>> {
        let (bars, support, config) = callback_fixture()?;
        let now = bars.last().ok_or("missing bar")?.close_time_ms + 1_000;
        let signal = evaluate_callback(&bars, &support, &config, EntryKind::Add, now)?
            .ok_or("signal missing")?;
        assert_eq!(signal.support_id, "support-1");
        assert_eq!(signal.kind, EntryKind::Add);
        assert!(signal.relative_volume >= config.volume_multiple);
        assert!(
            evaluate_callback(
                &bars,
                &support,
                &config,
                EntryKind::Add,
                now + config.max_signal_age_ms + 1,
            )?
            .is_none()
        );
        Ok(())
    }

    #[test]
    fn callback_does_not_treat_a_rebound_without_oversold_as_a_signal()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut bars, mut support, mut config) = callback_fixture()?;
        for index in 22..=23 {
            bars[index].open = Price::new(Decimal::from(100))?;
            bars[index].high = Price::new(Decimal::from(101))?;
            bars[index].low = Price::new(Decimal::from(99))?;
            bars[index].close = Price::new(Decimal::from(100))?;
            bars[index].base_volume = FieldState::Known(if index == 23 {
                Decimal::from(2)
            } else {
                Decimal::ONE
            });
            bars[index].quote_volume = FieldState::Known(if index == 23 {
                Decimal::from(200)
            } else {
                Decimal::from(100)
            });
            bars[index].taker_buy_base_volume = bars[index].base_volume.clone();
            bars[index].taker_buy_quote_volume = bars[index].quote_volume.clone();
        }
        bars[24].open = Price::new(Decimal::from(100))?;
        bars[24].high = Price::new(Decimal::from(103))?;
        bars[24].low = Price::new(Decimal::from(100))?;
        bars[24].close = Price::new(Decimal::from(102))?;
        bars[24].base_volume = FieldState::Known(Decimal::ONE);
        bars[24].quote_volume = FieldState::Known(Decimal::from(101));
        bars[24].taker_buy_base_volume = FieldState::Known(Decimal::ONE);
        bars[24].taker_buy_quote_volume = FieldState::Known(Decimal::from(101));
        support.lower = Price::new(Decimal::from(99))?;
        support.upper = Price::new(Decimal::from(100))?;
        support.source_price = Price::new(Decimal::from(100))?;
        config.oversold_rsi = Decimal::ONE;
        let now = bars.last().ok_or("missing bar")?.close_time_ms + 1_000;
        assert!(evaluate_callback(&bars, &support, &config, EntryKind::First, now)?.is_none());
        Ok(())
    }
}
