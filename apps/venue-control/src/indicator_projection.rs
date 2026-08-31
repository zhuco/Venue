use std::{collections::BTreeSet, num::NonZeroUsize};

use tokio::sync::Mutex;
use venue_control_protocol::{
    CONTROL_SCHEMA_VERSION, IndicatorBinding, IndicatorEvent, IndicatorFeatureValues,
    IndicatorFrameProjection, IndicatorProvenance, IndicatorSnapshot, ProtocolError,
};
use venue_domain::{AggressorSide, FieldState, MarketEvent, PublicBar, PublicTrade, Symbol};
use venue_indicators::{
    BARS_SOURCE, BOOK_SOURCE, FeatureFrame, PublicBook, PublicMarketSourceError,
    RecordedPublicEvent, ScalpingPublicMarketSource, TRADES_SOURCE,
};

pub const MAX_INDICATOR_EVENT_PAGE: u32 = 256;
const DEFAULT_INDICATOR_EVENT_RETENTION: usize = 1_024;

/// Translates one continuous, normalized public market stream into an account-bound, read-only
/// projection. The caller continues to own transport and the synchronized book; this type has no
/// credentials, artifact paths, WAL, writer, or mutation surface.
#[derive(Debug)]
pub struct IndicatorProjector {
    binding: IndicatorBinding,
    source: ScalpingPublicMarketSource,
    required_sources: BTreeSet<String>,
    maximum_age_ms: u64,
}

impl IndicatorProjector {
    pub fn new(
        binding: IndicatorBinding,
        profile: impl Into<String>,
        profile_digest: impl Into<String>,
        maximum_age_ms: u64,
        maximum_history: NonZeroUsize,
    ) -> Result<Self, IndicatorProjectionError> {
        binding.validate()?;
        if maximum_age_ms == 0 {
            return Err(IndicatorProjectionError::Age);
        }
        let symbol = binding.symbol.clone();
        let source = ScalpingPublicMarketSource::new(
            symbol,
            profile,
            profile_digest,
            maximum_age_ms,
            maximum_history,
        )?;
        Ok(Self {
            binding,
            source,
            required_sources: BTreeSet::from([
                BOOK_SOURCE.to_owned(),
                TRADES_SOURCE.to_owned(),
                BARS_SOURCE.to_owned(),
            ]),
            maximum_age_ms,
        })
    }

    #[must_use]
    pub const fn binding(&self) -> &IndicatorBinding {
        &self.binding
    }

    /// Consumes exactly one already-normalized public input. A non-ready, stale, malformed, or
    /// cross-generation source returns no projection (or an error); it is never coerced into a
    /// partial value for Control.
    pub fn consume(
        &mut self,
        input: RecordedPublicEvent,
        book: &impl PublicBook,
        observed_ms: u64,
    ) -> Result<Option<IndicatorFrameProjection>, IndicatorProjectionError> {
        if observed_ms == 0 {
            return Err(IndicatorProjectionError::Age);
        }
        validate_projection_input(&self.binding.symbol, &input, book)?;
        let output = self.source.consume(input, book, observed_ms)?;
        output
            .frame
            .map(|frame| self.project(frame, observed_ms))
            .transpose()
    }

    /// Fences a broken stream until a strictly newer synchronized snapshot rebuilds it.
    pub fn fence(&mut self) {
        self.source.fence();
    }

    fn project(
        &self,
        frame: FeatureFrame,
        observed_ms: u64,
    ) -> Result<IndicatorFrameProjection, IndicatorProjectionError> {
        if frame.symbol != self.binding.symbol || frame.watermark_ms > observed_ms {
            return Err(IndicatorProjectionError::Binding);
        }
        frame.validate(&self.required_sources, self.maximum_age_ms)?;
        let provenance = self
            .required_sources
            .iter()
            .map(|source| {
                let cursor = frame
                    .cursors
                    .get(source)
                    .ok_or(IndicatorProjectionError::Frame)?;
                let feature_version = frame
                    .feature_versions
                    .get(source)
                    .filter(|version| !version.trim().is_empty())
                    .cloned()
                    .ok_or(IndicatorProjectionError::Frame)?;
                let age_ms = observed_ms
                    .checked_sub(cursor.event_time_ms)
                    .ok_or(IndicatorProjectionError::Age)?;
                Ok(IndicatorProvenance {
                    source: source.clone(),
                    generation: cursor.generation,
                    sequence: cursor.sequence,
                    event_time_ms: cursor.event_time_ms,
                    age_ms,
                    feature_version,
                })
            })
            .collect::<Result<Vec<_>, IndicatorProjectionError>>()?;
        let projection = IndicatorFrameProjection {
            schema_version: CONTROL_SCHEMA_VERSION,
            binding: self.binding.clone(),
            generation: frame.generation,
            watermark_ms: frame.watermark_ms,
            observed_ms,
            maximum_age_ms: self.maximum_age_ms,
            provenance,
            values: IndicatorFeatureValues {
                mid_price: frame.values.mid_price.value(),
                fair_price: frame.values.fair_price.value(),
                spread_bps: frame.values.spread_bps,
                depth_quote: frame.values.depth_quote,
                book_imbalance: frame.values.book_imbalance,
                trade_imbalance: frame.values.trade_imbalance,
                short_return_bps: frame.values.short_return_bps,
                trend_efficiency: frame.values.trend_efficiency,
                bandwidth_expansion: frame.values.bandwidth_expansion,
                expected_move_bps: frame.values.expected_move_bps,
                toxicity: frame.values.toxicity,
            },
        };
        projection.validate_at(observed_ms)?;
        Ok(projection)
    }
}

fn validate_projection_input(
    symbol: &Symbol,
    input: &RecordedPublicEvent,
    book: &impl PublicBook,
) -> Result<(), IndicatorProjectionError> {
    let (event_symbol, generation) = match &input.event {
        MarketEvent::Snapshot(value) => (&value.symbol, value.generation),
        MarketEvent::Delta(value) => (&value.symbol, value.generation),
        MarketEvent::Trade(value) => (&value.symbol, value.generation),
        MarketEvent::Bar(value) => (&value.symbol, value.generation),
        MarketEvent::Ticker(value) => (&value.symbol, value.generation),
        MarketEvent::MarkFunding(value) => (&value.symbol, value.generation),
    };
    if event_symbol != symbol || generation == 0 || input.capture_sequence == 0 {
        return Err(IndicatorProjectionError::Binding);
    }
    match &input.event {
        MarketEvent::Snapshot(_) | MarketEvent::Delta(_) => {
            let bids = book.bids();
            let asks = book.asks();
            if !book.synchronized()
                || book.symbol() != Some(symbol)
                || book.generation() != Some(generation)
                || bids.iter().chain(asks.iter()).any(|level| {
                    level.price.value() <= rust_decimal::Decimal::ZERO
                        || level.quantity <= rust_decimal::Decimal::ZERO
                })
            {
                return Err(IndicatorProjectionError::Input);
            }
        }
        MarketEvent::Trade(trade) if !complete_trade(trade) => {
            return Err(IndicatorProjectionError::Input);
        }
        MarketEvent::Bar(bar) if !complete_bar(bar) => {
            return Err(IndicatorProjectionError::Input);
        }
        MarketEvent::Trade(_)
        | MarketEvent::Bar(_)
        | MarketEvent::Ticker(_)
        | MarketEvent::MarkFunding(_) => {}
    }
    Ok(())
}

fn complete_trade(trade: &PublicTrade) -> bool {
    trade.is_valid()
        && trade.sequence().is_some()
        && matches!(
            trade.aggressor,
            FieldState::Known(AggressorSide::Buy | AggressorSide::Sell)
        )
}

fn complete_bar(bar: &PublicBar) -> bool {
    bar.is_valid()
        && matches!(
            (
                &bar.base_volume,
                &bar.quote_volume,
                &bar.trade_count,
                &bar.taker_buy_base_volume,
                &bar.taker_buy_quote_volume,
            ),
            (
                FieldState::Known(_),
                FieldState::Known(_),
                FieldState::Known(_),
                FieldState::Known(_),
                FieldState::Known(_),
            )
        )
}

#[derive(Clone, Debug, PartialEq)]
pub struct StoredIndicatorEvent {
    pub sequence: i64,
    pub event: IndicatorEvent,
}

#[derive(Debug)]
struct IndicatorState {
    snapshot: Option<IndicatorSnapshot>,
    events: Vec<StoredIndicatorEvent>,
    next_sequence: i64,
}

/// Bounded in-memory query projection for Control. Losing this cache merely removes read-only
/// market visibility; it cannot affect recovery, actor state, or execution authority.
#[derive(Debug)]
pub struct IndicatorProjectionStore {
    retention: usize,
    state: Mutex<IndicatorState>,
}

impl Default for IndicatorProjectionStore {
    fn default() -> Self {
        Self {
            retention: DEFAULT_INDICATOR_EVENT_RETENTION,
            state: Mutex::new(IndicatorState {
                snapshot: None,
                events: Vec::new(),
                next_sequence: 1,
            }),
        }
    }
}

impl IndicatorProjectionStore {
    #[must_use]
    pub fn new(retention: NonZeroUsize) -> Self {
        Self {
            retention: retention.get(),
            state: Mutex::new(IndicatorState {
                snapshot: None,
                events: Vec::new(),
                next_sequence: 1,
            }),
        }
    }

    pub async fn snapshot(&self) -> Result<Option<IndicatorSnapshot>, IndicatorProjectionError> {
        let snapshot = self.state.lock().await.snapshot.clone();
        if let Some(snapshot) = &snapshot {
            snapshot.validate()?;
        }
        Ok(snapshot)
    }

    /// Replaces only the exact binding's current frame, drops any frame that has become stale at
    /// this observation time, and appends one cursor-addressable snapshot event.
    pub async fn publish(
        &self,
        frame: IndicatorFrameProjection,
    ) -> Result<StoredIndicatorEvent, IndicatorProjectionError> {
        frame.validate_at(frame.observed_ms)?;
        let mut state = self.state.lock().await;
        if state
            .snapshot
            .as_ref()
            .is_some_and(|snapshot| frame.observed_ms <= snapshot.generated_ms)
        {
            return Err(IndicatorProjectionError::Monotonic);
        }
        let mut frames = state
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.frames.clone())
            .unwrap_or_default();
        frames.retain(|existing| existing.validate_at(frame.observed_ms).is_ok());
        if let Some(index) = frames
            .iter()
            .position(|existing| existing.binding == frame.binding)
        {
            frames[index] = frame;
        } else {
            frames.push(frame);
        }
        let snapshot = IndicatorSnapshot {
            schema_version: CONTROL_SCHEMA_VERSION,
            generated_ms: frames
                .iter()
                .map(|candidate| candidate.observed_ms)
                .max()
                .ok_or(IndicatorProjectionError::Frame)?,
            frames,
        };
        snapshot.validate()?;
        let sequence = state.next_sequence;
        state.next_sequence = state
            .next_sequence
            .checked_add(1)
            .ok_or(IndicatorProjectionError::Sequence)?;
        let stored = StoredIndicatorEvent {
            sequence,
            event: IndicatorEvent::Snapshot(snapshot.clone()),
        };
        state.snapshot = Some(snapshot);
        state.events.push(stored.clone());
        let overflow = state.events.len().saturating_sub(self.retention);
        if overflow != 0 {
            state.events.drain(..overflow);
        }
        Ok(stored)
    }

    /// Returns the retained suffix after a client cursor. An evicted cursor is rejected rather
    /// than silently skipping a read-only state transition.
    pub async fn events(
        &self,
        after_sequence: i64,
        limit: u32,
    ) -> Result<Vec<StoredIndicatorEvent>, IndicatorProjectionError> {
        if after_sequence < 0 || !(1..=MAX_INDICATOR_EVENT_PAGE).contains(&limit) {
            return Err(IndicatorProjectionError::Cursor);
        }
        let state = self.state.lock().await;
        if let Some(first) = state.events.first()
            && after_sequence < first.sequence.saturating_sub(1)
        {
            return Err(IndicatorProjectionError::CursorExpired);
        }
        Ok(state
            .events
            .iter()
            .filter(|event| event.sequence > after_sequence)
            .take(limit as usize)
            .cloned()
            .collect())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IndicatorProjectionError {
    #[error("indicator binding does not match the normalized source")]
    Binding,
    #[error("indicator input is missing a usable timestamp or has an invalid age")]
    Age,
    #[error(
        "indicator public input is incomplete, non-positive, or cannot prove its volume semantics"
    )]
    Input,
    #[error("indicator frame is not a complete current projection")]
    Frame,
    #[error("indicator event cursor is invalid")]
    Cursor,
    #[error("indicator event cursor has expired from the bounded replay buffer")]
    CursorExpired,
    #[error("indicator projection observation time is not strictly monotonic")]
    Monotonic,
    #[error("indicator event sequence is exhausted")]
    Sequence,
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error(transparent)]
    Source(#[from] PublicMarketSourceError),
    #[error(transparent)]
    Feature(#[from] venue_indicators::FeatureFrameError),
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, num::NonZeroUsize};

    use rust_decimal::Decimal;
    use venue_control_protocol::{GatewayMode, VenueId};
    use venue_domain::{
        AggressorSide, FieldState, MarketDelta, MarketEvent, MarketLevel, MarketSnapshot, Price,
        PublicBar, PublicTrade, Symbol,
    };
    use venue_indicators::{FeatureState, FeatureValues, SourceCursor};

    use super::*;

    #[derive(Clone)]
    struct TestBook {
        symbol: Symbol,
        generation: u64,
        sequence: u64,
        bids: Vec<MarketLevel>,
        asks: Vec<MarketLevel>,
    }

    impl PublicBook for TestBook {
        fn synchronized(&self) -> bool {
            true
        }

        fn bridged(&self) -> bool {
            true
        }

        fn symbol(&self) -> Option<&Symbol> {
            Some(&self.symbol)
        }

        fn generation(&self) -> Option<u64> {
            Some(self.generation)
        }

        fn sequence(&self) -> Option<u64> {
            Some(self.sequence)
        }

        fn bids(&self) -> Vec<MarketLevel> {
            self.bids.clone()
        }

        fn asks(&self) -> Vec<MarketLevel> {
            self.asks.clone()
        }
    }

    fn book() -> Result<TestBook, Box<dyn std::error::Error>> {
        Ok(TestBook {
            symbol: "BTC/USDT".parse()?,
            generation: 7,
            sequence: 1,
            bids: vec![MarketLevel {
                price: Price::new(Decimal::from(99))?,
                quantity: Decimal::ONE,
            }],
            asks: vec![MarketLevel {
                price: Price::new(Decimal::from(101))?,
                quantity: Decimal::ONE,
            }],
        })
    }

    fn binding() -> Result<IndicatorBinding, Box<dyn std::error::Error>> {
        Ok(IndicatorBinding {
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "BTC/USDT".parse()?,
        })
    }

    fn feature_frame(observed_ms: u64) -> Result<FeatureFrame, Box<dyn std::error::Error>> {
        let event_time_ms = observed_ms.checked_sub(1).ok_or("test observation time")?;
        let cursors = [BOOK_SOURCE, TRADES_SOURCE, BARS_SOURCE]
            .into_iter()
            .map(|source| {
                (
                    source.to_owned(),
                    SourceCursor {
                        generation: 7,
                        sequence: 9,
                        event_time_ms,
                        fresh: true,
                    },
                )
            })
            .collect();
        Ok(FeatureFrame {
            symbol: "BTC/USDT".parse()?,
            schema_version: 1,
            generation: 7,
            watermark_ms: event_time_ms,
            state: FeatureState::Ready,
            cursors,
            feature_versions: [BOOK_SOURCE, TRADES_SOURCE, BARS_SOURCE]
                .into_iter()
                .map(|source| (source.to_owned(), "v1".to_owned()))
                .collect::<BTreeMap<_, _>>(),
            values: FeatureValues {
                mid_price: Price::new(Decimal::from(100))?,
                fair_price: Price::new(Decimal::from(100))?,
                spread_bps: Decimal::ONE,
                depth_quote: Decimal::from(1_000),
                book_imbalance: Decimal::ZERO,
                trade_imbalance: Decimal::ZERO,
                short_return_bps: Decimal::ZERO,
                trend_efficiency: Decimal::ZERO,
                bandwidth_expansion: Decimal::ZERO,
                expected_move_bps: Decimal::ONE,
                toxicity: Decimal::ZERO,
            },
            breakout: None,
        })
    }

    fn projection(
        observed_ms: u64,
    ) -> Result<IndicatorFrameProjection, Box<dyn std::error::Error>> {
        let projector = IndicatorProjector::new(
            binding()?,
            "control-read-only-v1",
            "0".repeat(64),
            100,
            NonZeroUsize::new(64).ok_or("non-zero history")?,
        )?;
        Ok(projector.project(feature_frame(observed_ms)?, observed_ms)?)
    }

    #[test]
    fn normalized_book_trade_and_bar_stream_emits_one_bound_ready_projection()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut projector = IndicatorProjector::new(
            binding()?,
            "control-read-only-v1",
            "0".repeat(64),
            2_000_000,
            NonZeroUsize::new(64).ok_or("non-zero history")?,
        )?;
        let mut book = book()?;
        let mut capture_sequence = 1;
        assert!(
            projector
                .consume(
                    RecordedPublicEvent {
                        capture_sequence,
                        received_at_ms: 1,
                        event: MarketEvent::Snapshot(MarketSnapshot {
                            symbol: book.symbol.clone(),
                            generation: book.generation,
                            sequence: book.sequence,
                            exchange_time_ms: Some(1),
                            bids: book.bids.clone(),
                            asks: book.asks.clone(),
                        }),
                    },
                    &book,
                    1,
                )?
                .is_none()
        );
        capture_sequence += 1;
        book.sequence = 2;
        assert!(
            projector
                .consume(
                    RecordedPublicEvent {
                        capture_sequence,
                        received_at_ms: 2,
                        event: MarketEvent::Delta(MarketDelta {
                            symbol: book.symbol.clone(),
                            generation: book.generation,
                            first_sequence: 2,
                            previous_sequence: Some(1),
                            sequence: book.sequence,
                            exchange_time_ms: Some(2),
                            bids: book.bids.clone(),
                            asks: book.asks.clone(),
                        }),
                    },
                    &book,
                    2,
                )?
                .is_none()
        );
        for trade_id in 1..=64 {
            capture_sequence += 1;
            assert!(
                projector
                    .consume(
                        RecordedPublicEvent {
                            capture_sequence,
                            received_at_ms: trade_id + 1,
                            event: MarketEvent::Trade(PublicTrade {
                                symbol: book.symbol.clone(),
                                generation: book.generation,
                                received_at_ms: trade_id + 1,
                                exchange_time_ms: trade_id + 1,
                                transaction_time_ms: trade_id + 1,
                                aggregate_trade_id: trade_id.into(),
                                first_trade_id: Some(trade_id),
                                last_trade_id: Some(trade_id),
                                ordering: venue_domain::PublicTradeOrdering::NativeAggregateId,
                                price: Price::new(Decimal::from(100))?,
                                quantity: Decimal::ONE,
                                quote_quantity: Decimal::from(100),
                                aggressor: FieldState::Known(AggressorSide::Buy),
                            }),
                        },
                        &book,
                        trade_id + 1,
                    )?
                    .is_none()
            );
        }
        let mut emitted = None;
        for bar_sequence in 1..=21 {
            capture_sequence += 1;
            let close_time_ms = bar_sequence * 60_000 - 1;
            emitted = projector.consume(
                RecordedPublicEvent {
                    capture_sequence,
                    received_at_ms: close_time_ms,
                    event: MarketEvent::Bar(PublicBar {
                        symbol: book.symbol.clone(),
                        generation: book.generation,
                        received_at_ms: close_time_ms,
                        sequence: bar_sequence,
                        open_time_ms: (bar_sequence - 1) * 60_000,
                        close_time_ms,
                        interval_ms: 60_000,
                        open: Price::new(Decimal::from(100))?,
                        high: Price::new(Decimal::from(101))?,
                        low: Price::new(Decimal::from(99))?,
                        close: Price::new(Decimal::from(100))?,
                        base_volume: FieldState::Known(Decimal::from(10)),
                        quote_volume: FieldState::Known(Decimal::from(1_000)),
                        trade_count: FieldState::Known(5),
                        taker_buy_base_volume: FieldState::Known(Decimal::from(4)),
                        taker_buy_quote_volume: FieldState::Known(Decimal::from(400)),
                    }),
                },
                &book,
                close_time_ms,
            )?;
        }
        let emitted = emitted.ok_or("expected ready feature frame")?;
        assert_eq!(emitted.binding, binding()?);
        assert_eq!(emitted.generation, 7);
        assert_eq!(emitted.provenance.len(), 3);
        Ok(())
    }

    #[test]
    fn projection_boundary_rejects_incomplete_volume_negative_quantity_and_taker_excess()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut projector = IndicatorProjector::new(
            binding()?,
            "control-read-only-v1",
            "0".repeat(64),
            1_000,
            NonZeroUsize::new(64).ok_or("non-zero history")?,
        )?;
        let book = book()?;
        let base_bar = PublicBar {
            symbol: book.symbol.clone(),
            generation: book.generation,
            received_at_ms: 60_000,
            sequence: 1,
            open_time_ms: 0,
            close_time_ms: 59_999,
            interval_ms: 60_000,
            open: Price::new(Decimal::from(100))?,
            high: Price::new(Decimal::from(101))?,
            low: Price::new(Decimal::from(99))?,
            close: Price::new(Decimal::from(100))?,
            base_volume: FieldState::Known(Decimal::from(10)),
            quote_volume: FieldState::Known(Decimal::from(1_000)),
            trade_count: FieldState::Known(5),
            taker_buy_base_volume: FieldState::Known(Decimal::from(4)),
            taker_buy_quote_volume: FieldState::Known(Decimal::from(400)),
        };
        for base_volume in [
            FieldState::Missing,
            FieldState::Null,
            FieldState::NotApplicable,
        ] {
            let mut invalid = base_bar.clone();
            invalid.base_volume = base_volume;
            assert!(matches!(
                projector.consume(
                    RecordedPublicEvent {
                        capture_sequence: 1,
                        received_at_ms: 60_000,
                        event: MarketEvent::Bar(invalid),
                    },
                    &book,
                    60_000,
                ),
                Err(IndicatorProjectionError::Input)
            ));
        }
        let mut taker_excess = base_bar;
        taker_excess.taker_buy_base_volume = FieldState::Known(Decimal::from(11));
        assert!(matches!(
            projector.consume(
                RecordedPublicEvent {
                    capture_sequence: 1,
                    received_at_ms: 60_000,
                    event: MarketEvent::Bar(taker_excess),
                },
                &book,
                60_000,
            ),
            Err(IndicatorProjectionError::Input)
        ));
        assert!(matches!(
            projector.consume(
                RecordedPublicEvent {
                    capture_sequence: 1,
                    received_at_ms: 1,
                    event: MarketEvent::Trade(PublicTrade {
                        symbol: book.symbol.clone(),
                        generation: book.generation,
                        received_at_ms: 1,
                        exchange_time_ms: 1,
                        transaction_time_ms: 1,
                        aggregate_trade_id: 1_u64.into(),
                        first_trade_id: Some(1),
                        last_trade_id: Some(1),
                        ordering: venue_domain::PublicTradeOrdering::NativeAggregateId,
                        price: Price::new(Decimal::from(100))?,
                        quantity: -Decimal::ONE,
                        quote_quantity: Decimal::from(100),
                        aggressor: FieldState::Known(AggressorSide::Buy),
                    }),
                },
                &book,
                1,
            ),
            Err(IndicatorProjectionError::Input)
        ));
        let mut negative_book = book.clone();
        negative_book.bids.push(MarketLevel {
            price: Price::new(Decimal::from(98))?,
            quantity: -Decimal::ONE,
        });
        assert!(matches!(
            projector.consume(
                RecordedPublicEvent {
                    capture_sequence: 1,
                    received_at_ms: 1,
                    event: MarketEvent::Snapshot(MarketSnapshot {
                        symbol: negative_book.symbol.clone(),
                        generation: negative_book.generation,
                        sequence: negative_book.sequence,
                        exchange_time_ms: Some(1),
                        bids: negative_book.bids.clone(),
                        asks: negative_book.asks.clone(),
                    }),
                },
                &negative_book,
                1,
            ),
            Err(IndicatorProjectionError::Input)
        ));
        Ok(())
    }

    #[test]
    fn projection_is_exactly_binding_generation_provenance_and_age_scoped()
    -> Result<(), Box<dyn std::error::Error>> {
        let projection = projection(100)?;
        projection.validate_at(100)?;
        assert!(!projection.grants_mutation_authority());
        assert_eq!(projection.provenance.len(), 3);

        let mut cross_generation = projection.clone();
        cross_generation.provenance[0].generation = 6;
        assert!(matches!(
            cross_generation.validate_at(100),
            Err(ProtocolError::IndicatorProvenance)
        ));
        let mut stale = projection;
        stale.provenance[0].age_ms = 101;
        assert!(matches!(
            stale.validate_at(100),
            Err(ProtocolError::IndicatorProvenance)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn bounded_indicator_sse_replays_current_cursor_and_rejects_evicted_cursor()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = IndicatorProjectionStore::new(NonZeroUsize::new(2).ok_or("retention")?);
        assert_eq!(store.publish(projection(100)?).await?.sequence, 1);
        assert_eq!(store.publish(projection(101)?).await?.sequence, 2);
        assert_eq!(store.publish(projection(102)?).await?.sequence, 3);

        let resumed = store.events(2, 10).await?;
        assert_eq!(
            resumed
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            [3]
        );
        assert!(matches!(
            store.events(0, 10).await,
            Err(IndicatorProjectionError::CursorExpired)
        ));
        let snapshot = store.snapshot().await?.ok_or("projection snapshot")?;
        assert_eq!(snapshot.generated_ms, 102);
        assert_eq!(snapshot.frames.len(), 1);
        assert!(!snapshot.grants_mutation_authority());
        Ok(())
    }
}
