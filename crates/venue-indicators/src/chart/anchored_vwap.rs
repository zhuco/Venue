//! Time-anchored VWAP from normalized 1m public bars.

use rust_decimal::Decimal;
use venue_domain::PublicBar;
use venue_domain::domain::Symbol;

use super::{ChartIndicatorError, Vwap};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnchoredVwap {
    pub anchor_open_time_ms: u64,
    pub complete: bool,
    pub values: Vec<(u64, Option<Decimal>)>,
    engine: Vwap,
    scope: Option<(Symbol, u64)>,
    last_end_ms: Option<u64>,
}

impl AnchoredVwap {
    /// Calculate a replaceable preview without advancing the confirmed accumulator.
    pub fn preview(&self, forming: &PublicBar) -> Result<Option<Decimal>, ChartIndicatorError> {
        if forming.open_time_ms < self.anchor_open_time_ms {
            return Ok(None);
        }
        if let Some((symbol, generation)) = &self.scope
            && (symbol != &forming.symbol || *generation != forming.generation)
        {
            return Err(ChartIndicatorError::ScopeChanged);
        }
        if self
            .last_end_ms
            .is_some_and(|end| end != forming.open_time_ms)
            || (self.last_end_ms.is_none() && forming.open_time_ms != self.anchor_open_time_ms)
        {
            return Ok(None);
        }
        match self.engine.clone().update(forming) {
            Ok(value) => Ok(value),
            Err(ChartIndicatorError::VolumeUnavailable) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

/// One set of cumulative minute facts serves every anchor on the same chart.
/// Missing volume preserves the previous totals but never becomes a synthetic zero-volume value.
#[derive(Clone, Debug)]
pub struct AnchoredVwapIndex {
    entries: Vec<VwapPrefix>,
    scope: Option<(Symbol, u64)>,
    last_end_ms: Option<u64>,
}

#[derive(Clone, Debug)]
struct VwapPrefix {
    open_time_ms: u64,
    price_volume: Decimal,
    volume: Decimal,
    missing_volume: u32,
    gaps: u32,
}

impl AnchoredVwapIndex {
    pub fn build(bars: &[PublicBar]) -> Result<Self, ChartIndicatorError> {
        let mut entries = Vec::with_capacity(bars.len());
        let mut engine = Vwap::new()?;
        let mut scope: Option<(Symbol, u64)> = None;
        let mut previous_end = None;
        let mut missing_volume = 0_u32;
        let mut gaps = 0_u32;
        for bar in bars {
            if bar.interval_ms != 60_000
                || !bar.is_valid()
                || previous_end.is_some_and(|end| bar.open_time_ms < end)
            {
                return Err(ChartIndicatorError::InvalidBar);
            }
            if scope.as_ref().is_some_and(|(symbol, generation)| {
                symbol != &bar.symbol || *generation != bar.generation
            }) {
                return Err(ChartIndicatorError::ScopeChanged);
            }
            if scope.is_none() {
                scope = Some((bar.symbol.clone(), bar.generation));
            }
            if previous_end.is_some_and(|end| end != bar.open_time_ms) {
                gaps = gaps.checked_add(1).ok_or(ChartIndicatorError::Arithmetic)?;
            }
            match engine.update(bar) {
                Ok(_) => {}
                Err(ChartIndicatorError::VolumeUnavailable) => {
                    missing_volume = missing_volume
                        .checked_add(1)
                        .ok_or(ChartIndicatorError::Arithmetic)?;
                }
                Err(error) => return Err(error),
            }
            let (price_volume, volume) = engine.totals();
            entries.push(VwapPrefix {
                open_time_ms: bar.open_time_ms,
                price_volume,
                volume,
                missing_volume,
                gaps,
            });
            previous_end = Some(
                bar.close_time_ms
                    .checked_add(1)
                    .ok_or(ChartIndicatorError::Arithmetic)?,
            );
        }
        Ok(Self {
            entries,
            scope,
            last_end_ms: previous_end,
        })
    }

    pub fn memory_bytes(&self) -> usize {
        self.entries.capacity() * std::mem::size_of::<VwapPrefix>()
    }

    fn anchor_index(&self, anchor_open_time_ms: u64) -> Option<usize> {
        self.entries
            .binary_search_by_key(&anchor_open_time_ms, |entry| entry.open_time_ms)
            .ok()
    }

    pub fn contains_anchor(&self, anchor_open_time_ms: u64) -> bool {
        self.anchor_index(anchor_open_time_ms).is_some()
    }

    pub fn complete(&self, anchor_open_time_ms: u64) -> bool {
        let Some(anchor) = self.anchor_index(anchor_open_time_ms) else {
            return false;
        };
        let Some(last) = self.entries.last() else {
            return false;
        };
        let before_missing = anchor
            .checked_sub(1)
            .and_then(|index| self.entries.get(index))
            .map_or(0, |entry| entry.missing_volume);
        last.gaps == self.entries[anchor].gaps && last.missing_volume == before_missing
    }

    fn before_anchor(&self, anchor: usize) -> (Decimal, Decimal) {
        anchor
            .checked_sub(1)
            .and_then(|index| self.entries.get(index))
            .map_or((Decimal::ZERO, Decimal::ZERO), |entry| {
                (entry.price_volume, entry.volume)
            })
    }

    pub fn value_before(
        &self,
        anchor_open_time_ms: u64,
        end_ms: u64,
    ) -> Result<Option<Decimal>, ChartIndicatorError> {
        let Some(anchor) = self.anchor_index(anchor_open_time_ms) else {
            return Ok(None);
        };
        let Some(last_open) = end_ms.checked_sub(60_000) else {
            return Ok(None);
        };
        let upto = self
            .entries
            .partition_point(|entry| entry.open_time_ms <= last_open);
        let Some(last_index) = upto.checked_sub(1).filter(|index| *index >= anchor) else {
            return Ok(None);
        };
        let last = &self.entries[last_index];
        let previous_missing = last_index
            .checked_sub(1)
            .and_then(|index| self.entries.get(index))
            .map_or(0, |entry| entry.missing_volume);
        if last.missing_volume != previous_missing {
            return Ok(None);
        }
        let (prior_price_volume, prior_volume) = self.before_anchor(anchor);
        let price_volume = last
            .price_volume
            .checked_sub(prior_price_volume)
            .ok_or(ChartIndicatorError::Arithmetic)?;
        let volume = last
            .volume
            .checked_sub(prior_volume)
            .ok_or(ChartIndicatorError::Arithmetic)?;
        Vwap::from_totals(price_volume, volume).current_value()
    }

    /// A displayed bucket must contain a real completed sample, not a carried-forward value.
    pub fn sample_in_range(
        &self,
        anchor_open_time_ms: u64,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<Option<(u64, Decimal)>, ChartIndicatorError> {
        let Some(last_open) = end_ms.checked_sub(60_000) else {
            return Ok(None);
        };
        let upto = self
            .entries
            .partition_point(|entry| entry.open_time_ms <= last_open);
        let Some(last) = upto
            .checked_sub(1)
            .and_then(|index| self.entries.get(index))
            .filter(|entry| entry.open_time_ms >= start_ms)
        else {
            return Ok(None);
        };
        Ok(self
            .value_before(anchor_open_time_ms, end_ms)?
            .map(|value| (last.open_time_ms + 60_000, value)))
    }

    pub fn continuous_range(&self, start_ms: u64, end_ms: u64) -> bool {
        let Some(first_index) = self.anchor_index(start_ms) else {
            return false;
        };
        let Some(last_index) = end_ms
            .checked_sub(60_000)
            .and_then(|open| self.anchor_index(open))
            .filter(|index| *index >= first_index)
        else {
            return false;
        };
        let first = &self.entries[first_index];
        let last = &self.entries[last_index];
        let prior_missing = first_index
            .checked_sub(1)
            .and_then(|index| self.entries.get(index))
            .map_or(0, |entry| entry.missing_volume);
        last.gaps == first.gaps && last.missing_volume == prior_missing
    }

    pub fn preview(
        &self,
        anchor_open_time_ms: u64,
        forming: &PublicBar,
    ) -> Result<Option<Decimal>, ChartIndicatorError> {
        if forming.open_time_ms < anchor_open_time_ms {
            return Ok(None);
        }
        if let Some((symbol, generation)) = &self.scope
            && (symbol != &forming.symbol || *generation != forming.generation)
        {
            return Err(ChartIndicatorError::ScopeChanged);
        }
        let mut engine = if let Some(anchor) = self.anchor_index(anchor_open_time_ms) {
            let Some(last) = self.entries.last() else {
                return Ok(None);
            };
            if self.last_end_ms != Some(forming.open_time_ms) {
                return Ok(None);
            }
            let (prior_price_volume, prior_volume) = self.before_anchor(anchor);
            Vwap::from_totals(
                last.price_volume
                    .checked_sub(prior_price_volume)
                    .ok_or(ChartIndicatorError::Arithmetic)?,
                last.volume
                    .checked_sub(prior_volume)
                    .ok_or(ChartIndicatorError::Arithmetic)?,
            )
        } else if forming.open_time_ms == anchor_open_time_ms {
            Vwap::new()?
        } else {
            return Ok(None);
        };
        match engine.update(forming) {
            Ok(value) => Ok(value),
            Err(ChartIndicatorError::VolumeUnavailable) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

pub fn calculate(
    bars: &[PublicBar],
    anchor_open_time_ms: u64,
) -> Result<AnchoredVwap, ChartIndicatorError> {
    let mut result = AnchoredVwap {
        anchor_open_time_ms,
        complete: bars
            .first()
            .is_some_and(|bar| bar.open_time_ms <= anchor_open_time_ms),
        values: Vec::new(),
        engine: Vwap::new()?,
        scope: None,
        last_end_ms: None,
    };
    let mut engine = Vwap::new()?;
    let mut previous_end = None;
    let mut scope = None;
    for bar in bars {
        if !bar.is_valid()
            || bar.interval_ms != 60_000
            || previous_end.is_some_and(|end| bar.open_time_ms < end)
        {
            return Err(ChartIndicatorError::InvalidBar);
        }
        let identity = (&bar.symbol, bar.generation);
        if scope.is_some_and(|prior| prior != identity) {
            return Err(ChartIndicatorError::ScopeChanged);
        }
        scope = Some(identity);
        if result.scope.is_none() {
            result.scope = Some((bar.symbol.clone(), bar.generation));
        }
        if bar.open_time_ms < anchor_open_time_ms {
            previous_end = Some(
                bar.close_time_ms
                    .checked_add(1)
                    .ok_or(ChartIndicatorError::Arithmetic)?,
            );
            continue;
        }
        if result.values.is_empty() && bar.open_time_ms != anchor_open_time_ms {
            result.complete = false;
        }
        if !result.values.is_empty() && previous_end.is_some_and(|end| end != bar.open_time_ms) {
            result.complete = false;
        }
        let value = match engine.update(bar) {
            Ok(value) => value,
            Err(ChartIndicatorError::VolumeUnavailable) => {
                result.complete = false;
                None
            }
            Err(error) => return Err(error),
        };
        result.values.push((bar.open_time_ms, value));
        previous_end = Some(
            bar.close_time_ms
                .checked_add(1)
                .ok_or(ChartIndicatorError::Arithmetic)?,
        );
    }
    result.complete &= result
        .values
        .first()
        .is_some_and(|(time, _)| *time == anchor_open_time_ms);
    result.engine = engine;
    result.last_end_ms = previous_end;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_domain::{FieldState, Price, UnknownReason};

    fn bar(minute: u64, price: i64) -> Result<PublicBar, Box<dyn std::error::Error>> {
        let value = Price::new(Decimal::from(price))?;
        Ok(PublicBar {
            symbol: "DOGE/USDC".parse()?,
            generation: 1,
            received_at_ms: minute * 60_000 + 60_000,
            sequence: minute + 1,
            open_time_ms: minute * 60_000,
            close_time_ms: minute * 60_000 + 59_999,
            interval_ms: 60_000,
            open: value,
            high: value,
            low: value,
            close: value,
            base_volume: FieldState::Known(Decimal::ONE),
            quote_volume: FieldState::Known(Decimal::from(price)),
            trade_count: FieldState::Known(1),
            taker_buy_base_volume: FieldState::Known(Decimal::ONE),
            taker_buy_quote_volume: FieldState::Known(Decimal::from(price)),
        })
    }

    #[test]
    fn anchor_starts_at_exact_minute_and_marks_missing_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let bars = [bar(0, 100)?, bar(1, 200)?, bar(2, 300)?];
        let result = calculate(&bars, 60_000)?;
        assert!(result.complete);
        assert_eq!(
            result.values,
            vec![
                (60_000, Some(Decimal::from(200))),
                (120_000, Some(Decimal::from(250)))
            ]
        );
        assert!(!calculate(&bars[2..], 60_000)?.complete);
        Ok(())
    }

    #[test]
    fn forming_preview_replaces_itself_without_advancing_confirmed_vwap()
    -> Result<(), Box<dyn std::error::Error>> {
        let confirmed = [bar(0, 100)?, bar(1, 200)?];
        let result = calculate(&confirmed, 60_000)?;
        assert_eq!(result.preview(&bar(2, 300)?)?, Some(Decimal::from(250)));
        assert_eq!(result.preview(&bar(2, 400)?)?, Some(Decimal::from(300)));
        assert_eq!(result.values, vec![(60_000, Some(Decimal::from(200)))]);
        assert_eq!(result.preview(&bar(3, 400)?)?, None);
        let mut wrong = bar(2, 300)?;
        wrong.symbol = "BTC/USDC".parse()?;
        assert_eq!(
            result.preview(&wrong),
            Err(ChartIndicatorError::ScopeChanged)
        );
        Ok(())
    }

    #[test]
    fn shared_index_matches_each_anchor_without_retaining_per_anchor_history()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut bars = (0..30)
            .map(|minute| bar(minute, 100 + minute as i64))
            .collect::<Result<Vec<_>, _>>()?;
        bars[2].high = Price::new(Decimal::new(1_234, 1))?;
        bars[7].low = Price::new(Decimal::new(805, 1))?;
        let index = AnchoredVwapIndex::build(&bars)?;
        for anchor in [0_usize, 5, 17] {
            let time = bars[anchor].open_time_ms;
            let direct = calculate(&bars[anchor..], time)?;
            assert_eq!(index.complete(time), direct.complete);
            for end in [anchor + 1, anchor + 2, 29, 30] {
                if end > bars.len() {
                    continue;
                }
                let expected = direct
                    .values
                    .iter()
                    .rev()
                    .find(|(minute, _)| *minute < end as u64 * 60_000)
                    .and_then(|(_, value)| *value);
                assert_eq!(index.value_before(time, end as u64 * 60_000)?, expected);
            }
            let preview = bar(30, 170)?;
            assert_eq!(index.preview(time, &preview)?, direct.preview(&preview)?);
        }
        assert_eq!(std::mem::size_of::<VwapPrefix>(), 48);
        assert!(index.memory_bytes() < 3 * 30 * std::mem::size_of::<(u64, Option<Decimal>)>());
        Ok(())
    }

    #[test]
    fn shared_index_preserves_missing_volume_and_gap_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut missing = bar(1, 110)?;
        missing.base_volume = FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        };
        missing.quote_volume = FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        };
        missing.trade_count = FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        };
        missing.taker_buy_base_volume = FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        };
        missing.taker_buy_quote_volume = FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        };
        let bars = [bar(0, 100)?, missing, bar(3, 130)?];
        let index = AnchoredVwapIndex::build(&bars)?;
        assert!(!index.complete(0));
        assert_eq!(index.value_before(0, 120_000)?, None);
        let direct = calculate(&bars, 0)?;
        assert_eq!(index.value_before(0, 240_000)?, direct.values[2].1);
        assert!(!index.contains_anchor(120_000));
        assert_eq!(index.value_before(120_000, 240_000)?, None);
        assert!(index.complete(180_000));
        let forming_anchor = bar(4, 140)?;
        assert_eq!(
            index.preview(240_000, &forming_anchor)?,
            Some(Decimal::from(140))
        );
        let mut wrong = forming_anchor.clone();
        wrong.symbol = "BTC/USDC".parse()?;
        assert_eq!(
            index.preview(240_000, &wrong),
            Err(ChartIndicatorError::ScopeChanged)
        );
        let mut mixed = bars.to_vec();
        mixed[1].generation = 2;
        assert!(matches!(
            AnchoredVwapIndex::build(&mixed),
            Err(ChartIndicatorError::ScopeChanged)
        ));
        Ok(())
    }

    #[test]
    fn sampling_never_looks_ahead_or_fills_missing_buckets()
    -> Result<(), Box<dyn std::error::Error>> {
        let index = AnchoredVwapIndex::build(&[bar(0, 100)?, bar(1, 120)?, bar(3, 140)?])?;
        assert_eq!(index.value_before(0, 30_000)?, None);
        assert_eq!(index.value_before(0, 90_000)?, Some(Decimal::from(100)));
        assert_eq!(index.sample_in_range(0, 120_000, 180_000)?, None);
        assert_eq!(index.sample_in_range(0, 240_000, 300_000)?, None);
        assert_eq!(
            index.sample_in_range(0, 180_000, 240_000)?,
            Some((240_000, Decimal::from(120)))
        );
        assert!(index.continuous_range(0, 120_000));
        assert!(!index.continuous_range(60_000, 240_000));
        assert!(!index.continuous_range(120_000, 240_000));
        assert!(index.continuous_range(180_000, 240_000));
        Ok(())
    }

    #[test]
    fn valid_maximum_close_time_returns_arithmetic_error_in_both_paths()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut latest = bar(0, 100)?;
        latest.open_time_ms = u64::MAX - 59_999;
        latest.close_time_ms = u64::MAX;
        latest.received_at_ms = u64::MAX;
        assert!(latest.is_valid());
        assert_eq!(
            calculate(&[latest.clone()], latest.open_time_ms),
            Err(ChartIndicatorError::Arithmetic)
        );
        assert!(matches!(
            AnchoredVwapIndex::build(&[latest]),
            Err(ChartIndicatorError::Arithmetic)
        ));
        Ok(())
    }
}
