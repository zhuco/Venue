use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::domain::Symbol;

/// The account-level maximum is an invariant, not a user-tunable ranking preference.
pub const MAX_CONCURRENT_SCALPING_SYMBOLS: usize = 3;

/// A normalized 24-hour market observation supplied by an exchange adapter.
///
/// Native ticker payloads, exchange names, and transport state deliberately remain outside this
/// value so the ranking is deterministic and replayable.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MarketRankSample {
    pub symbol: Symbol,
    pub observed_at_ms: u64,
    pub source_generation: u64,
    #[serde(with = "rust_decimal::serde::str")]
    pub change_24h_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub quote_volume: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MarketScannerParams {
    pub algorithm_version: String,
    pub max_selected_symbols: usize,
}

impl MarketScannerParams {
    pub fn phase8() -> Self {
        Self {
            algorithm_version: "binance-usdt-perps-movers-volume-v1".to_owned(),
            max_selected_symbols: MAX_CONCURRENT_SCALPING_SYMBOLS,
        }
    }

    fn validate(&self) -> Result<(), MarketScannerError> {
        if self.algorithm_version.trim().is_empty()
            || !(1..=MAX_CONCURRENT_SCALPING_SYMBOLS).contains(&self.max_selected_symbols)
        {
            return Err(MarketScannerError::Parameters);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketRejectReason {
    DuplicateSymbol,
    NonPositiveQuoteVolume,
    NoTwentyFourHourMove,
    BelowHighVolumeFloor,
    BelowRankCutoff,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RejectedMarketSample {
    pub sample: MarketRankSample,
    pub reason: MarketRejectReason,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SelectedMarket {
    pub sample: MarketRankSample,
    pub rank: usize,
}

/// The complete, durable-friendly ranking result. `selected` plus `rejected` accounts for every
/// input observation and carries all watermarks needed to replay the choice.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MarketSelection {
    pub algorithm_version: String,
    pub selection_watermark_ms: u64,
    pub selected: Vec<SelectedMarket>,
    pub rejected: Vec<RejectedMarketSample>,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum MarketScannerError {
    #[error("invalid market scanner parameters")]
    Parameters,
}

/// Selects liquid movers deterministically.
///
/// "High volume" is defined relative to the observed universe: only the upper half of valid
/// quote volumes may progress. The remaining symbols rank by absolute 24-hour movement, then by
/// quote volume, then canonical symbol. This gives both sharp rises and sharp falls a chance to
/// feed the later mean-reversion strategy without silently introducing a fiat threshold.
pub fn select_liquid_movers(
    params: &MarketScannerParams,
    samples: Vec<MarketRankSample>,
) -> Result<MarketSelection, MarketScannerError> {
    params.validate()?;

    let selection_watermark_ms = samples
        .iter()
        .map(|sample| sample.observed_at_ms)
        .max()
        .unwrap_or_default();
    let duplicate_symbols = duplicate_symbols(&samples);
    let mut rejected = Vec::new();
    let mut eligible = Vec::new();

    for sample in samples {
        let reason = if duplicate_symbols.contains(&sample.symbol) {
            Some(MarketRejectReason::DuplicateSymbol)
        } else if sample.quote_volume <= Decimal::ZERO {
            Some(MarketRejectReason::NonPositiveQuoteVolume)
        } else if sample.change_24h_bps.is_zero() {
            Some(MarketRejectReason::NoTwentyFourHourMove)
        } else {
            None
        };
        if let Some(reason) = reason {
            rejected.push(RejectedMarketSample { sample, reason });
        } else {
            eligible.push(sample);
        }
    }

    let high_volume_floor = eligible
        .iter()
        .map(|sample| sample.quote_volume)
        .collect::<Vec<_>>();
    let high_volume_floor = median_floor(high_volume_floor);
    let mut high_volume = Vec::new();
    for sample in eligible {
        if sample.quote_volume < high_volume_floor {
            rejected.push(RejectedMarketSample {
                sample,
                reason: MarketRejectReason::BelowHighVolumeFloor,
            });
        } else {
            high_volume.push(sample);
        }
    }

    high_volume.sort_by(|left, right| {
        right
            .change_24h_bps
            .abs()
            .cmp(&left.change_24h_bps.abs())
            .then_with(|| right.quote_volume.cmp(&left.quote_volume))
            .then_with(|| left.symbol.cmp(&right.symbol))
    });
    let selected_count = high_volume.len().min(params.max_selected_symbols);
    let remaining = high_volume.split_off(selected_count);
    rejected.extend(remaining.into_iter().map(|sample| RejectedMarketSample {
        sample,
        reason: MarketRejectReason::BelowRankCutoff,
    }));

    rejected.sort_by(|left, right| left.sample.symbol.cmp(&right.sample.symbol));
    Ok(MarketSelection {
        algorithm_version: params.algorithm_version.clone(),
        selection_watermark_ms,
        selected: high_volume
            .into_iter()
            .enumerate()
            .map(|(index, sample)| SelectedMarket {
                sample,
                rank: index + 1,
            })
            .collect(),
        rejected,
    })
}

fn duplicate_symbols(samples: &[MarketRankSample]) -> BTreeSet<Symbol> {
    let mut counts = BTreeMap::new();
    for sample in samples {
        *counts.entry(sample.symbol.clone()).or_insert(0_usize) += 1;
    }
    counts
        .into_iter()
        .filter_map(|(symbol, count)| (count > 1).then_some(symbol))
        .collect()
}

fn median_floor(mut values: Vec<Decimal>) -> Decimal {
    values.sort();
    values
        .get(values.len() / 2)
        .copied()
        .unwrap_or(Decimal::ZERO)
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::{MarketRankSample, MarketRejectReason, MarketScannerParams, select_liquid_movers};

    fn sample(symbol: &str, change_24h_bps: i64, quote_volume: i64) -> MarketRankSample {
        MarketRankSample {
            symbol: symbol.parse().expect("test symbol"),
            observed_at_ms: 100,
            source_generation: 7,
            change_24h_bps: Decimal::new(change_24h_bps, 0),
            quote_volume: Decimal::new(quote_volume, 0),
        }
    }

    #[test]
    fn selects_at_most_three_high_volume_absolute_movers() -> Result<(), Box<dyn std::error::Error>>
    {
        let selection = select_liquid_movers(
            &MarketScannerParams::phase8(),
            vec![
                sample("BTC/USDT", 400, 500),
                sample("ETH/USDT", -900, 450),
                sample("SOL/USDT", 800, 400),
                sample("XRP/USDT", -1_200, 300),
                sample("DOGE/USDT", 5_000, 10),
            ],
        )?;

        assert_eq!(
            selection
                .selected
                .iter()
                .map(|selected| selected.sample.symbol.to_string())
                .collect::<Vec<_>>(),
            vec!["ETH/USDT", "SOL/USDT", "BTC/USDT"]
        );
        assert!(selection.rejected.iter().any(|rejected| {
            rejected.sample.symbol.to_string() == "XRP/USDT"
                && rejected.reason == MarketRejectReason::BelowHighVolumeFloor
        }));
        assert!(selection.rejected.iter().any(|rejected| {
            rejected.sample.symbol.to_string() == "DOGE/USDT"
                && rejected.reason == MarketRejectReason::BelowHighVolumeFloor
        }));
        Ok(())
    }

    #[test]
    fn rejects_ambiguous_or_incomplete_observations() -> Result<(), Box<dyn std::error::Error>> {
        let selection = select_liquid_movers(
            &MarketScannerParams::phase8(),
            vec![
                sample("BTC/USDT", 100, 100),
                sample("BTC/USDT", 200, 200),
                sample("ETH/USDT", 0, 100),
                sample("SOL/USDT", 100, 0),
            ],
        )?;

        assert!(selection.selected.is_empty());
        assert_eq!(selection.rejected.len(), 4);
        assert!(
            selection
                .rejected
                .iter()
                .any(|rejected| rejected.reason == MarketRejectReason::DuplicateSymbol)
        );
        assert!(
            selection
                .rejected
                .iter()
                .any(|rejected| rejected.reason == MarketRejectReason::NoTwentyFourHourMove)
        );
        assert!(
            selection
                .rejected
                .iter()
                .any(|rejected| { rejected.reason == MarketRejectReason::NonPositiveQuoteVolume })
        );
        Ok(())
    }
}
