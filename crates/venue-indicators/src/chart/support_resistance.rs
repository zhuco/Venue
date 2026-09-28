//! Time-causal swing zones; scores rank local structure and are not probabilities.

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

const WINDOW: usize = 3;
const MAX_ZONES: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ZoneBar {
    pub open_time_ms: u64,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub atr: Option<Decimal>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ZoneRole {
    Support,
    Resistance,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ZoneState {
    Active,
    Broken,
    Retired,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Zone {
    pub id: u64,
    pub role: ZoneRole,
    pub state: ZoneState,
    pub low: Decimal,
    pub high: Decimal,
    pub center: Decimal,
    pub source_time_ms: u64,
    pub confirmed_at_ms: u64,
    pub retired_at_ms: Option<u64>,
    pub touch_count: u16,
    pub first_touch_ms: Option<u64>,
    pub last_touch_ms: Option<u64>,
    pub score: f64,
    prominence: f64,
    atr: Decimal,
    last_validation_index: usize,
    inside: bool,
    outside_bars: u8,
    break_count: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ZoneError {
    #[error("invalid or noncontiguous zone source")]
    InvalidBars,
    #[error("invalid tick size")]
    InvalidTick,
}

pub fn calculate(
    bars: &[ZoneBar],
    interval_ms: u64,
    tick: Decimal,
) -> Result<Vec<Zone>, ZoneError> {
    if tick <= Decimal::ZERO {
        return Err(ZoneError::InvalidTick);
    }
    if interval_ms == 0
        || bars
            .windows(2)
            .any(|pair| pair[0].open_time_ms.checked_add(interval_ms) != Some(pair[1].open_time_ms))
        || bars.iter().any(|bar| {
            bar.open_time_ms % interval_ms != 0
                || bar.high < bar.low
                || bar.close < bar.low
                || bar.close > bar.high
                || bar.low <= Decimal::ZERO
        })
    {
        return Err(ZoneError::InvalidBars);
    }
    let mut zones: Vec<Zone> = Vec::new();
    for (index, bar) in bars.iter().enumerate() {
        for zone in &mut zones {
            if zone.state == ZoneState::Retired {
                continue;
            }
            if index.saturating_sub(zone.last_validation_index) > 500 {
                zone.state = ZoneState::Retired;
                zone.retired_at_ms = Some(bar.open_time_ms);
                continue;
            }
            let touched = bar.high >= zone.low && bar.low <= zone.high;
            if touched {
                if !zone.inside && zone.outside_bars >= 1 {
                    zone.touch_count = zone.touch_count.saturating_add(1);
                    zone.first_touch_ms.get_or_insert(bar.open_time_ms);
                    zone.last_touch_ms = Some(bar.open_time_ms);
                    zone.last_validation_index = index;
                }
                zone.inside = true;
                zone.outside_bars = 0;
            } else {
                zone.inside = false;
                zone.outside_bars = zone.outside_bars.saturating_add(1);
            }
            let threshold = zone.atr / Decimal::from(4);
            let beyond = match zone.role {
                ZoneRole::Resistance => bar.close > zone.high + threshold,
                ZoneRole::Support => bar.close < zone.low - threshold,
            };
            if zone.state == ZoneState::Active {
                zone.break_count = if beyond {
                    zone.break_count.saturating_add(1)
                } else {
                    0
                };
                if zone.break_count >= 2 {
                    zone.state = ZoneState::Broken;
                    zone.last_validation_index = index;
                }
            } else if touched && !beyond {
                let confirmed_flip = match zone.role {
                    ZoneRole::Resistance => bar.close >= zone.center,
                    ZoneRole::Support => bar.close <= zone.center,
                };
                if confirmed_flip {
                    zone.role = match zone.role {
                        ZoneRole::Resistance => ZoneRole::Support,
                        ZoneRole::Support => ZoneRole::Resistance,
                    };
                    zone.state = ZoneState::Active;
                    zone.break_count = 0;
                    zone.last_validation_index = index;
                }
            }
            zone.score = score(zone, index);
        }
        if index < WINDOW * 2 {
            continue;
        }
        let center_index = index - WINDOW;
        let center = bars[center_index];
        let Some(atr) = bar.atr.filter(|atr| *atr > Decimal::ZERO) else {
            continue;
        };
        let left = &bars[center_index - WINDOW..center_index];
        let right = &bars[center_index + 1..=index];
        let high_swing = left.iter().all(|item| item.high < center.high)
            && right.iter().all(|item| item.high <= center.high);
        let low_swing = left.iter().all(|item| item.low > center.low)
            && right.iter().all(|item| item.low >= center.low);
        for role in [ZoneRole::Resistance, ZoneRole::Support] {
            if (role == ZoneRole::Resistance && !high_swing)
                || (role == ZoneRole::Support && !low_swing)
            {
                continue;
            }
            let prominence = match role {
                ZoneRole::Resistance => {
                    let left_low = left.iter().map(|bar| bar.low).min().unwrap_or(center.low);
                    let right_low = right.iter().map(|bar| bar.low).min().unwrap_or(center.low);
                    center.high - left_low.max(right_low)
                }
                ZoneRole::Support => {
                    let left_high = left.iter().map(|bar| bar.high).max().unwrap_or(center.high);
                    let right_high = right
                        .iter()
                        .map(|bar| bar.high)
                        .max()
                        .unwrap_or(center.high);
                    left_high.min(right_high) - center.low
                }
            };
            let prominence = decimal_ratio(prominence, atr * Decimal::from(2)).clamp(0.0, 1.0);
            let price = if role == ZoneRole::Resistance {
                center.high
            } else {
                center.low
            };
            let half = (tick * Decimal::from(2)).max(atr / Decimal::from(4));
            let (low, high) = (price - half, price + half);
            if let Some(zone) = zones.iter_mut().find(|zone| {
                zone.state == ZoneState::Active
                    && zone.role == role
                    && (zone.low <= high && low <= zone.high
                        || (zone.center - price).abs()
                            <= half + (zone.high - zone.low) / Decimal::from(2))
                    && zone.high.max(high) - zone.low.min(low) <= atr
            }) {
                zone.low = zone.low.min(low);
                zone.high = zone.high.max(high);
                zone.center = (zone.low + zone.high) / Decimal::from(2);
                zone.prominence = zone.prominence.max(prominence);
                zone.last_validation_index = index;
                zone.score = score(zone, index);
                continue;
            }
            let id = center
                .open_time_ms
                .saturating_mul(2)
                .saturating_add(u64::from(role == ZoneRole::Resistance));
            let mut zone = Zone {
                id,
                role,
                state: ZoneState::Active,
                low,
                high,
                center: price,
                source_time_ms: center.open_time_ms,
                confirmed_at_ms: bar.open_time_ms.saturating_add(interval_ms),
                retired_at_ms: None,
                touch_count: 0,
                first_touch_ms: None,
                last_touch_ms: None,
                score: 0.0,
                prominence,
                atr,
                last_validation_index: index,
                inside: false,
                outside_bars: 1,
                break_count: 0,
            };
            zone.score = score(&zone, index);
            zones.push(zone);
        }
        if zones.len() > MAX_ZONES {
            zones.retain(|zone| zone.state != ZoneState::Retired);
            if zones.len() > MAX_ZONES {
                zones.sort_by(|a, b| {
                    b.score
                        .total_cmp(&a.score)
                        .then_with(|| b.confirmed_at_ms.cmp(&a.confirmed_at_ms))
                });
                zones.truncate(MAX_ZONES);
            }
        }
    }
    Ok(zones)
}

fn decimal_ratio(value: Decimal, denominator: Decimal) -> f64 {
    if denominator <= Decimal::ZERO {
        return 0.0;
    }
    value
        .checked_div(denominator)
        .and_then(|ratio| ratio.to_f64())
        .unwrap_or(0.0)
}

fn score(zone: &Zone, index: usize) -> f64 {
    let touches = (f64::from(zone.touch_count) / 5.0).min(1.0);
    let recency = 2.0_f64.powf(-(index.saturating_sub(zone.last_validation_index) as f64) / 100.0);
    100.0 * (0.40 * zone.prominence + 0.35 * touches + 0.25 * recency)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bar(index: u64, low: i64, high: i64, close: i64) -> ZoneBar {
        ZoneBar {
            open_time_ms: index * 60_000,
            low: Decimal::from(low),
            high: Decimal::from(high),
            close: Decimal::from(close),
            atr: Some(Decimal::from(2)),
        }
    }

    #[test]
    fn swing_is_visible_only_after_three_right_bars_close() -> Result<(), ZoneError> {
        let bars = [
            bar(0, 5, 7, 6),
            bar(1, 6, 8, 7),
            bar(2, 6, 9, 8),
            bar(3, 7, 12, 10),
            bar(4, 6, 9, 8),
            bar(5, 5, 8, 7),
            bar(6, 5, 7, 6),
        ];
        let tick = Decimal::new(1, 2);
        assert!(calculate(&bars[..6], 60_000, tick)?.is_empty());
        let zones = calculate(&bars, 60_000, tick)?;
        let resistance = zones
            .iter()
            .find(|zone| zone.role == ZoneRole::Resistance && zone.source_time_ms == 180_000)
            .ok_or(ZoneError::InvalidBars)?;
        assert_eq!(resistance.confirmed_at_ms, 420_000);
        assert_eq!(resistance.state, ZoneState::Active);
        Ok(())
    }
}
