//! Deterministic OHLCV estimate of volume at price over a half-open time range.

use std::collections::BTreeMap;

use rust_decimal::{Decimal, prelude::ToPrimitive};
use venue_domain::{FieldState, PublicBar, Symbol};

const MAX_BUCKETS: i64 = 4_096;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProfileError {
    #[error("invalid profile range or price step")]
    InvalidRange,
    #[error("profile requires more than 4096 price buckets")]
    TooManyBuckets,
    #[error("invalid or mixed public bar scope")]
    InvalidBars,
    #[error("decimal arithmetic overflow")]
    Arithmetic,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VolumeBucket {
    pub price_low: Decimal,
    pub price_high: Decimal,
    pub base_volume: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VolumeProfile {
    pub requested_start_ms: u64,
    pub requested_end_ms: u64,
    pub covered_start_ms: Option<u64>,
    pub covered_end_ms: Option<u64>,
    pub complete: bool,
    pub missing_volume_bars: usize,
    pub buckets: Vec<VolumeBucket>,
    pub poc: Option<Decimal>,
    pub val: Option<Decimal>,
    pub vah: Option<Decimal>,
    pub total_base_volume: Decimal,
}

/// Allocates each completed bar's base volume uniformly across its high-low span.
/// A flat bar contributes to one bucket. This is an estimate, not trade-at-price data.
pub fn estimate(
    bars: &[PublicBar],
    start_ms: u64,
    end_ms: u64,
    price_step: Decimal,
    value_area_percent: u8,
) -> Result<VolumeProfile, ProfileError> {
    if start_ms >= end_ms || price_step <= Decimal::ZERO || !(1..=100).contains(&value_area_percent)
    {
        return Err(ProfileError::InvalidRange);
    }
    let mut totals = BTreeMap::<i64, Decimal>::new();
    let mut profile = VolumeProfile {
        requested_start_ms: start_ms,
        requested_end_ms: end_ms,
        covered_start_ms: None,
        covered_end_ms: None,
        complete: true,
        missing_volume_bars: 0,
        buckets: Vec::new(),
        poc: None,
        val: None,
        vah: None,
        total_base_volume: Decimal::ZERO,
    };
    let mut scope: Option<(&Symbol, u64, u64)> = None;
    let mut previous_open = None;
    for bar in bars {
        if !bar.is_valid() {
            return Err(ProfileError::InvalidBars);
        }
        if previous_open.is_some_and(|time| bar.open_time_ms <= time) {
            return Err(ProfileError::InvalidBars);
        }
        previous_open = Some(bar.open_time_ms);
        let this_scope = (&bar.symbol, bar.generation, bar.interval_ms);
        if scope.is_some_and(|previous| previous != this_scope) {
            return Err(ProfileError::InvalidBars);
        }
        scope = Some(this_scope);
        let bar_end = bar
            .close_time_ms
            .checked_add(1)
            .ok_or(ProfileError::Arithmetic)?;
        if bar_end <= start_ms || bar.open_time_ms >= end_ms {
            continue;
        }
        if bar.open_time_ms < start_ms || bar_end > end_ms {
            profile.complete = false;
            continue;
        }
        if profile
            .covered_end_ms
            .is_some_and(|previous| previous != bar.open_time_ms)
        {
            profile.complete = false;
        }
        profile.covered_start_ms.get_or_insert(bar.open_time_ms);
        profile.covered_end_ms = Some(bar_end);
        let FieldState::Known(volume) = &bar.base_volume else {
            profile.missing_volume_bars += 1;
            profile.complete = false;
            continue;
        };
        if volume.is_zero() {
            continue;
        }
        distribute_bar(&mut totals, bar, *volume, price_step)?;
        profile.total_base_volume = profile
            .total_base_volume
            .checked_add(*volume)
            .ok_or(ProfileError::Arithmetic)?;
    }
    profile.complete &=
        profile.covered_start_ms == Some(start_ms) && profile.covered_end_ms == Some(end_ms);
    if let (Some((&first, _)), Some((&last, _))) =
        (totals.first_key_value(), totals.last_key_value())
    {
        if last
            .checked_sub(first)
            .is_none_or(|span| span >= MAX_BUCKETS)
        {
            return Err(ProfileError::TooManyBuckets);
        }
        for index in first..=last {
            let low = Decimal::from(index)
                .checked_mul(price_step)
                .ok_or(ProfileError::Arithmetic)?;
            let high = low
                .checked_add(price_step)
                .ok_or(ProfileError::Arithmetic)?;
            profile.buckets.push(VolumeBucket {
                price_low: low,
                price_high: high,
                base_volume: totals.get(&index).copied().unwrap_or(Decimal::ZERO),
            });
        }
    }
    if profile.total_base_volume > Decimal::ZERO {
        let poc_index = profile
            .buckets
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| {
                a.base_volume
                    .cmp(&b.base_volume)
                    .then_with(|| b.price_low.cmp(&a.price_low))
            })
            .map(|(index, _)| index)
            .ok_or(ProfileError::Arithmetic)?;
        let target = profile
            .total_base_volume
            .checked_mul(Decimal::from(value_area_percent))
            .and_then(|value| value.checked_div(Decimal::from(100)))
            .ok_or(ProfileError::Arithmetic)?;
        let (mut left, mut right) = (poc_index, poc_index);
        let mut included = profile.buckets[poc_index].base_volume;
        while included < target && (left > 0 || right + 1 < profile.buckets.len()) {
            let left_volume = left
                .checked_sub(1)
                .map(|index| profile.buckets[index].base_volume);
            let right_volume = profile
                .buckets
                .get(right + 1)
                .map(|bucket| bucket.base_volume);
            if left_volume.is_some() && (right_volume.is_none() || left_volume >= right_volume) {
                left -= 1;
                included = included
                    .checked_add(profile.buckets[left].base_volume)
                    .ok_or(ProfileError::Arithmetic)?;
            } else {
                right += 1;
                included = included
                    .checked_add(profile.buckets[right].base_volume)
                    .ok_or(ProfileError::Arithmetic)?;
            }
        }
        profile.poc = Some(profile.buckets[poc_index].price_low);
        profile.val = Some(profile.buckets[left].price_low);
        profile.vah = Some(profile.buckets[right].price_high);
    }
    Ok(profile)
}

fn distribute_bar(
    totals: &mut BTreeMap<i64, Decimal>,
    bar: &PublicBar,
    volume: Decimal,
    step: Decimal,
) -> Result<(), ProfileError> {
    let low = bar.low.value();
    let high = bar.high.value();
    let first = low
        .checked_div(step)
        .and_then(|v| v.floor().to_i64())
        .ok_or(ProfileError::Arithmetic)?;
    let last = if high == low {
        first
    } else {
        high.checked_div(step)
            .and_then(|v| v.ceil().to_i64())
            .and_then(|v| v.checked_sub(1))
            .ok_or(ProfileError::Arithmetic)?
    };
    if last < first || last - first >= MAX_BUCKETS {
        return Err(ProfileError::TooManyBuckets);
    }
    if first == last {
        add(totals, first, volume)?;
        return Ok(());
    }
    let span = high.checked_sub(low).ok_or(ProfileError::Arithmetic)?;
    let mut pieces = Vec::with_capacity((last - first + 1) as usize);
    let mut largest = (Decimal::ZERO, first);
    for index in first..=last {
        let bucket_low = Decimal::from(index)
            .checked_mul(step)
            .ok_or(ProfileError::Arithmetic)?;
        let bucket_high = bucket_low
            .checked_add(step)
            .ok_or(ProfileError::Arithmetic)?;
        let overlap = high
            .min(bucket_high)
            .checked_sub(low.max(bucket_low))
            .ok_or(ProfileError::Arithmetic)?;
        if overlap > largest.0 {
            largest = (overlap, index);
        }
        pieces.push((index, overlap));
    }
    let mut allocated = Decimal::ZERO;
    for &(index, overlap) in &pieces {
        if index == largest.1 || overlap.is_zero() {
            continue;
        }
        let remainder = volume
            .checked_sub(allocated)
            .ok_or(ProfileError::Arithmetic)?;
        // Decimal division can round several sub-quantum pieces upward. Never
        // spend more than remains before assigning the residual to the largest overlap.
        let part = volume
            .checked_mul(overlap)
            .and_then(|v| v.checked_div(span))
            .ok_or(ProfileError::Arithmetic)?
            .min(remainder);
        add(totals, index, part)?;
        allocated = allocated
            .checked_add(part)
            .ok_or(ProfileError::Arithmetic)?;
    }
    add(
        totals,
        largest.1,
        volume
            .checked_sub(allocated)
            .ok_or(ProfileError::Arithmetic)?,
    )?;
    if totals.len() > MAX_BUCKETS as usize {
        return Err(ProfileError::TooManyBuckets);
    }
    Ok(())
}

fn add(
    totals: &mut BTreeMap<i64, Decimal>,
    index: i64,
    volume: Decimal,
) -> Result<(), ProfileError> {
    let previous = totals.get(&index).copied().unwrap_or(Decimal::ZERO);
    totals.insert(
        index,
        previous
            .checked_add(volume)
            .ok_or(ProfileError::Arithmetic)?,
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_domain::{Price, UnknownReason};

    fn bar(
        minute: u64,
        low: i64,
        high: i64,
        volume: i64,
    ) -> Result<PublicBar, Box<dyn std::error::Error>> {
        let open_time_ms = minute * 60_000;
        let missing = FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        };
        Ok(PublicBar {
            symbol: "DOGE/USDC".parse()?,
            generation: 1,
            received_at_ms: open_time_ms + 60_000,
            sequence: minute + 1,
            open_time_ms,
            close_time_ms: open_time_ms + 59_999,
            interval_ms: 60_000,
            open: Price::new(Decimal::from(low))?,
            high: Price::new(Decimal::from(high))?,
            low: Price::new(Decimal::from(low))?,
            close: Price::new(Decimal::from(high))?,
            base_volume: FieldState::Known(Decimal::from(volume)),
            quote_volume: missing.clone(),
            trade_count: FieldState::Unavailable {
                reason: UnknownReason::SourceOmitted,
            },
            taker_buy_base_volume: missing.clone(),
            taker_buy_quote_volume: missing,
        })
    }

    #[test]
    fn smallest_volumes_never_allocate_negative_buckets() -> Result<(), Box<dyn std::error::Error>>
    {
        for mantissa in 1..=16 {
            let mut candle = bar(0, 10, 16, 1)?;
            let volume = Decimal::new(mantissa, 28);
            candle.base_volume = FieldState::Known(volume);
            let result = estimate(&[candle], 0, 60_000, Decimal::ONE, 70)?;
            assert!(
                result
                    .buckets
                    .iter()
                    .all(|bucket| bucket.base_volume >= Decimal::ZERO),
                "negative allocation for {volume}"
            );
            assert_eq!(
                result
                    .buckets
                    .iter()
                    .map(|bucket| bucket.base_volume)
                    .sum::<Decimal>(),
                volume
            );
        }
        Ok(())
    }

    #[test]
    fn conservation_poc_tie_and_value_area_are_deterministic()
    -> Result<(), Box<dyn std::error::Error>> {
        let bars = [bar(0, 10, 12, 10)?, bar(1, 10, 10, 5)?];
        let result = estimate(&bars, 0, 120_000, Decimal::ONE, 70)?;
        assert!(result.complete);
        assert_eq!(result.total_base_volume, Decimal::from(15));
        assert_eq!(
            result
                .buckets
                .iter()
                .map(|bucket| bucket.base_volume)
                .sum::<Decimal>(),
            Decimal::from(15)
        );
        assert_eq!(result.poc, Some(Decimal::from(10)));
        assert_eq!(result.val, Some(Decimal::from(10)));
        assert_eq!(result.vah, Some(Decimal::from(12)));
        Ok(())
    }

    #[test]
    fn missing_history_is_partial_and_wrong_scope_is_rejected()
    -> Result<(), Box<dyn std::error::Error>> {
        let bars = [bar(0, 10, 11, 1)?, bar(2, 10, 11, 1)?];
        let result = estimate(&bars, 0, 180_000, Decimal::ONE, 70)?;
        assert!(!result.complete);
        assert_eq!(result.covered_end_ms, Some(180_000));
        let mut other = bars[1].clone();
        other.symbol = "DOGE/USDT".parse()?;
        assert_eq!(
            estimate(&[bars[0].clone(), other], 0, 180_000, Decimal::ONE, 70),
            Err(ProfileError::InvalidBars)
        );
        Ok(())
    }
}
