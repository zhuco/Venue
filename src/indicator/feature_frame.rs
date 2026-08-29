use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::domain::{Price, Symbol};

pub const BOOK_SOURCE: &str = "book";
pub const TRADES_SOURCE: &str = "trades";
pub const BARS_SOURCE: &str = "bars";
pub const FEATURE_PROFILE_KEY: &str = "_feature_profile";
pub const FEATURE_PROFILE_DIGEST_KEY: &str = "_feature_profile_digest";
pub const BREAKOUT_OPPORTUNITY_VERSION_KEY: &str = "_breakout_opportunity";

/// A source cursor is the provenance fence for one derived feature input.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceCursor {
    pub generation: u64,
    pub sequence: u64,
    pub event_time_ms: u64,
    pub fresh: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureState {
    Warmup,
    Ready,
    Stale,
    DataGap,
    Rebuilding,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BreakoutDirection {
    Long,
    Short,
}

/// Versioned evidence that an expansion belongs to one observed boundary and compression cycle.
/// Indicator produces this identity; strategy only validates and consumes it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BreakoutOpportunity {
    pub schema_version: u16,
    pub generation: u64,
    pub feature_version: String,
    pub direction: BreakoutDirection,
    pub boundary_id: String,
    pub boundary_sequence: u64,
    pub compression_cycle_id: String,
    pub compression_cycle_sequence: u64,
    pub detected_at_ms: u64,
    pub valid_until_ms: u64,
}

/// The smallest lossless feature set consumed by the first scalping strategy.
/// It intentionally excludes fees, fills, order quantities, and venue-native values.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FeatureValues {
    pub mid_price: Price,
    pub fair_price: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub spread_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub depth_quote: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub book_imbalance: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub trade_imbalance: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub short_return_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub trend_efficiency: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub bandwidth_expansion: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub expected_move_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub toxicity: Decimal,
}

/// An atomic versioned snapshot used by one strategy decision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FeatureFrame {
    pub symbol: Symbol,
    pub schema_version: u16,
    pub generation: u64,
    pub watermark_ms: u64,
    pub state: FeatureState,
    pub cursors: BTreeMap<String, SourceCursor>,
    pub feature_versions: BTreeMap<String, String>,
    pub values: FeatureValues,
    pub breakout: Option<BreakoutOpportunity>,
}

impl FeatureFrame {
    pub fn validate(
        &self,
        required_sources: &BTreeSet<String>,
        max_data_age_ms: u64,
    ) -> Result<(), FeatureFrameError> {
        if self.schema_version == 0
            || self.generation == 0
            || self.watermark_ms == 0
            || self.state != FeatureState::Ready
            || required_sources.is_empty()
            || required_sources.iter().any(|source| {
                source.is_empty()
                    || !self.cursors.contains_key(source)
                    || self
                        .feature_versions
                        .get(source)
                        .is_none_or(|version| version.is_empty())
            })
        {
            return Err(FeatureFrameError::Identity);
        }
        if self.cursors.values().any(|cursor| {
            !cursor.fresh
                || cursor.generation != self.generation
                || cursor.sequence == 0
                || cursor.event_time_ms == 0
                || cursor.event_time_ms > self.watermark_ms
                || self.watermark_ms.saturating_sub(cursor.event_time_ms) > max_data_age_ms
        }) {
            return Err(FeatureFrameError::Provenance);
        }
        let values = &self.values;
        if values.mid_price.value() <= Decimal::ZERO
            || values.fair_price.value() <= Decimal::ZERO
            || values.spread_bps < Decimal::ZERO
            || values.depth_quote < Decimal::ZERO
            || values.book_imbalance < -Decimal::ONE
            || values.book_imbalance > Decimal::ONE
            || values.trade_imbalance < -Decimal::ONE
            || values.trade_imbalance > Decimal::ONE
            || values.toxicity < Decimal::ZERO
            || values.toxicity > Decimal::ONE
            || values.trend_efficiency < -Decimal::ONE
            || values.trend_efficiency > Decimal::ONE
            || values.bandwidth_expansion < -Decimal::ONE
            || values.expected_move_bps < Decimal::ZERO
        {
            return Err(FeatureFrameError::Values);
        }
        if let Some(opportunity) = &self.breakout {
            opportunity.validate_for(self)?;
        }
        Ok(())
    }
}

impl BreakoutOpportunity {
    pub fn validate_for(&self, frame: &FeatureFrame) -> Result<(), FeatureFrameError> {
        if self.schema_version == 0
            || self.generation != frame.generation
            || self.feature_version.is_empty()
            || frame
                .feature_versions
                .get(BREAKOUT_OPPORTUNITY_VERSION_KEY)
                .is_none_or(|version| version != &self.feature_version)
            || self.boundary_id.is_empty()
            || self.boundary_sequence == 0
            || self.compression_cycle_id.is_empty()
            || self.compression_cycle_sequence == 0
            || self.detected_at_ms == 0
            || self.detected_at_ms > frame.watermark_ms
            || frame.watermark_ms > self.valid_until_ms
        {
            return Err(FeatureFrameError::Breakout);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum FeatureFrameError {
    #[error("feature frame identity, readiness, or required-source version is invalid")]
    Identity,
    #[error("feature frame sources are stale, mixed-generation, or non-monotonic")]
    Provenance,
    #[error("feature frame values are outside their normalized range")]
    Values,
    #[error("breakout opportunity lacks compatible boundary or compression-cycle evidence")]
    Breakout,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use rust_decimal::Decimal;

    use super::*;

    fn frame() -> Result<FeatureFrame, Box<dyn std::error::Error>> {
        Ok(FeatureFrame {
            symbol: "BTC/USDT".parse()?,
            schema_version: 1,
            generation: 4,
            watermark_ms: 100,
            state: FeatureState::Ready,
            cursors: [BOOK_SOURCE, TRADES_SOURCE, BARS_SOURCE]
                .into_iter()
                .map(|source| {
                    (
                        source.to_owned(),
                        SourceCursor {
                            generation: 4,
                            sequence: 1,
                            event_time_ms: 100,
                            fresh: true,
                        },
                    )
                })
                .collect(),
            feature_versions: [BOOK_SOURCE, TRADES_SOURCE, BARS_SOURCE]
                .into_iter()
                .map(|source| (source.to_owned(), "v1".to_owned()))
                .collect::<BTreeMap<_, _>>(),
            values: FeatureValues {
                mid_price: Price::new(Decimal::new(100, 0))?,
                fair_price: Price::new(Decimal::new(101, 0))?,
                spread_bps: Decimal::ONE,
                depth_quote: Decimal::new(1_000, 0),
                book_imbalance: -Decimal::ONE,
                trade_imbalance: -Decimal::ONE,
                short_return_bps: Decimal::ZERO,
                trend_efficiency: Decimal::ZERO,
                bandwidth_expansion: Decimal::ZERO,
                expected_move_bps: Decimal::ZERO,
                toxicity: Decimal::ZERO,
            },
            breakout: None,
        })
    }

    #[test]
    fn ready_frame_requires_complete_fresh_provenance() -> Result<(), Box<dyn std::error::Error>> {
        let required = BTreeSet::from([
            BOOK_SOURCE.to_owned(),
            TRADES_SOURCE.to_owned(),
            BARS_SOURCE.to_owned(),
        ]);
        let current = frame()?;
        current.validate(&required, 10)?;

        let mut stale = current;
        stale.state = FeatureState::Stale;
        assert!(matches!(
            stale.validate(&required, 10),
            Err(FeatureFrameError::Identity)
        ));
        Ok(())
    }

    #[test]
    fn breakout_identity_is_generation_and_version_scoped() -> Result<(), Box<dyn std::error::Error>>
    {
        let required = BTreeSet::from([
            BOOK_SOURCE.to_owned(),
            TRADES_SOURCE.to_owned(),
            BARS_SOURCE.to_owned(),
        ]);
        let mut current = frame()?;
        current.feature_versions.insert(
            BREAKOUT_OPPORTUNITY_VERSION_KEY.to_owned(),
            "pulse-breakout-opportunity-v1".to_owned(),
        );
        current.breakout = Some(BreakoutOpportunity {
            schema_version: 1,
            generation: 4,
            feature_version: "pulse-breakout-opportunity-v1".to_owned(),
            direction: BreakoutDirection::Long,
            boundary_id: "boundary-1".to_owned(),
            boundary_sequence: 1,
            compression_cycle_id: "compression-1".to_owned(),
            compression_cycle_sequence: 1,
            detected_at_ms: 100,
            valid_until_ms: 200,
        });
        current.validate(&required, 10)?;

        current
            .breakout
            .as_mut()
            .ok_or("breakout fixture")?
            .generation = 3;
        assert!(matches!(
            current.validate(&required, 10),
            Err(FeatureFrameError::Breakout)
        ));
        Ok(())
    }
}
