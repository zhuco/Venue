//! Comparable open-interest changes use completed samples from one exact market scope.

use rust_decimal::Decimal;
use venue_domain::{FieldState, OpenInterestSample, PublicBar};

pub const CHANGE_WINDOWS_MS: [u64; 5] = [300_000, 900_000, 3_600_000, 14_400_000, 86_400_000];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OpenInterestChange {
    pub window_ms: u64,
    pub latest_time_ms: u64,
    pub baseline_time_ms: u64,
    pub change_percent: Decimal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PriceOiState { PriceUpOiUp, PriceUpOiDown, PriceDownOiUp, PriceDownOiDown }

pub fn price_oi_state(
    change: OpenInterestChange,
    minutes: &[PublicBar],
) -> Option<PriceOiState> {
    let price_at = |time: u64| minutes.binary_search_by_key(&time.checked_sub(60_000)?, |bar| bar.open_time_ms)
        .ok().and_then(|index| minutes.get(index))
        .filter(|bar| bar.is_valid() && bar.interval_ms == 60_000 && bar.close_time_ms.checked_add(1) == Some(time))
        .map(|bar| bar.close.value());
    let latest = price_at(change.latest_time_ms)?;
    let baseline = price_at(change.baseline_time_ms)?;
    if latest == baseline || change.change_percent == Decimal::ZERO { return None; }
    Some(match (latest > baseline, change.change_percent > Decimal::ZERO) {
        (true, true) => PriceOiState::PriceUpOiUp,
        (true, false) => PriceOiState::PriceUpOiDown,
        (false, true) => PriceOiState::PriceDownOiUp,
        (false, false) => PriceOiState::PriceDownOiDown,
    })
}

/// Returns only windows with an actual completed baseline no older than one source period.
pub fn changes(samples: &[OpenInterestSample]) -> [Option<OpenInterestChange>; 5] {
    let mut output = [None; 5];
    let Some(latest) = samples.last().filter(|sample| sample.sampling_interval_ms.is_some()) else {
        return output;
    };
    let (FieldState::Known(latest_quantity), Some(period)) = (&latest.base_quantity, latest.sampling_interval_ms) else {
        return output;
    };
    if !latest.is_valid() || *latest_quantity < Decimal::ZERO {
        return output;
    }
    for (slot, window_ms) in CHANGE_WINDOWS_MS.into_iter().enumerate() {
        let Some(target) = latest.exchange_time_ms.checked_sub(window_ms) else { continue };
        let index = samples.partition_point(|sample| sample.exchange_time_ms <= target);
        let Some(baseline) = index.checked_sub(1).and_then(|index| samples.get(index)) else { continue };
        if baseline.symbol != latest.symbol
            || baseline.generation != latest.generation
            || baseline.sampling_interval_ms != Some(period)
            || !baseline.is_valid()
            || target.saturating_sub(baseline.exchange_time_ms) > period
        {
            continue;
        }
        let FieldState::Known(previous) = &baseline.base_quantity else { continue };
        if *previous <= Decimal::ZERO { continue }
        let Some(percent) = latest_quantity.checked_sub(*previous)
            .and_then(|difference| difference.checked_mul(Decimal::from(100)))
            .and_then(|numerator| numerator.checked_div(*previous))
        else { continue };
        output[slot] = Some(OpenInterestChange {
            window_ms,
            latest_time_ms: latest.exchange_time_ms,
            baseline_time_ms: baseline.exchange_time_ms,
            change_percent: percent,
        });
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_domain::OpenInterestUnit;

    fn sample(time: u64, quantity: i64) -> Result<OpenInterestSample, Box<dyn std::error::Error>> {
        Ok(OpenInterestSample {
            symbol: "DOGE/USDC".parse()?, generation: 1, received_at_ms: time + 1,
            exchange_time_ms: time, time_source: venue_domain::MarketTimeSource::Exchange, sampling_interval_ms: Some(300_000),
            native_quantity: Decimal::from(quantity), native_unit: OpenInterestUnit::BaseAsset,
            base_quantity: FieldState::Known(Decimal::from(quantity)),
            quote_notional: FieldState::Unavailable { reason: venue_domain::UnknownReason::SourceOmitted },
            quote_asset: None,
        })
    }

    #[test]
    fn windows_use_completed_same_scope_samples_without_interpolation() -> Result<(), Box<dyn std::error::Error>> {
        let start = 300_000;
        let mut samples = (0..=288).map(|index| sample(start + index * 300_000, 100))
            .collect::<Result<Vec<_>, _>>()?;
        samples[287] = sample(start + 287 * 300_000, 80)?;
        let values = changes(&samples);
        assert_eq!(values[0].ok_or("5m")?.change_percent, Decimal::from(25));
        assert_eq!(values[4].ok_or("24h")?.change_percent, Decimal::ZERO);
        samples.remove(287);
        samples.remove(286);
        assert_eq!(changes(&samples)[0], None);
        samples[0].symbol = "DOGE/USDT".parse()?;
        assert_eq!(changes(&samples)[4], None);
        Ok(())
    }
}
