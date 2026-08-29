use std::num::NonZeroUsize;

use crate::{
    domain::{MarketEvent, Symbol},
    indicator::{FeatureBuildError, FeatureFrame, FeatureState, ScalpingFeatureBuilder},
    market::OrderBook,
};

const FRAME_EMIT_INTERVAL_MS: u64 = 250;

/// One already-recorded and normalized public event. Capture sequence is only a public
/// provenance fence; it is never an authority, controller generation, or private watermark.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordedPublicEvent {
    pub capture_sequence: u64,
    pub received_at_ms: u64,
    pub event: MarketEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicMarketSourceOutput {
    pub capture_sequence: u64,
    pub generation: Option<u64>,
    pub state: FeatureState,
    pub frame: Option<FeatureFrame>,
}

/// Bounded, mutation-free public market source for a resident scalping loop.
///
/// The caller supplies at most one event per call. The caller-owned public order book is supplied
/// by reference; this source owns only indicator state and provenance fences. Authorization,
/// evidence, private generation, and any mutation capability remain outside this module.
#[derive(Debug)]
pub struct ScalpingPublicMarketSource {
    symbol: Symbol,
    builder: ScalpingFeatureBuilder,
    generation: Option<u64>,
    last_capture_sequence: Option<u64>,
    fenced_generation: Option<u64>,
    state: FeatureState,
    last_emitted_frame_at_ms: Option<u64>,
}

impl ScalpingPublicMarketSource {
    pub fn new(
        symbol: Symbol,
        profile: impl Into<String>,
        profile_digest: impl Into<String>,
        max_data_age_ms: u64,
        maximum_history: NonZeroUsize,
    ) -> Result<Self, PublicMarketSourceError> {
        Ok(Self {
            symbol,
            builder: ScalpingFeatureBuilder::new(
                profile,
                profile_digest,
                max_data_age_ms,
                maximum_history,
            )?,
            generation: None,
            last_capture_sequence: None,
            fenced_generation: None,
            state: FeatureState::Warmup,
            last_emitted_frame_at_ms: None,
        })
    }

    #[must_use]
    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    /// Consumes exactly one recorded public event and never creates a controller/private input.
    pub fn consume(
        &mut self,
        input: RecordedPublicEvent,
        book: &OrderBook,
        now_ms: u64,
    ) -> Result<PublicMarketSourceOutput, PublicMarketSourceError> {
        self.consume_with_emission(input, book, now_ms, true)
    }

    /// Batch ingestion preserves every provenance and feature update while deferring frame
    /// sampling until the full ordered batch has been consumed.
    pub(crate) fn consume_batched(
        &mut self,
        input: RecordedPublicEvent,
        book: &OrderBook,
        now_ms: u64,
    ) -> Result<PublicMarketSourceOutput, PublicMarketSourceError> {
        self.consume_with_emission(input, book, now_ms, false)
    }

    fn consume_with_emission(
        &mut self,
        input: RecordedPublicEvent,
        book: &OrderBook,
        now_ms: u64,
        allow_emission: bool,
    ) -> Result<PublicMarketSourceOutput, PublicMarketSourceError> {
        if let Err(error) = self.validate_capture_identity(&input) {
            self.state = FeatureState::DataGap;
            let event_generation = event_generation(&input.event);
            let fence_generation = self.generation.unwrap_or_default().max(event_generation);
            self.fenced_generation = (fence_generation != 0).then_some(fence_generation);
            self.last_capture_sequence = Some(input.capture_sequence);
            return Err(error);
        }
        let event_generation = event_generation(&input.event);
        if event_generation == 0 || event_symbol(&input.event) != &self.symbol {
            return self.reject(
                input.capture_sequence,
                event_generation,
                PublicMarketSourceError::Identity,
            );
        }
        if self
            .fenced_generation
            .is_some_and(|generation| event_generation <= generation)
        {
            return self.reject(
                input.capture_sequence,
                event_generation,
                PublicMarketSourceError::DataGap,
            );
        }
        if self
            .generation
            .is_some_and(|generation| event_generation < generation)
        {
            return self.reject(
                input.capture_sequence,
                event_generation,
                PublicMarketSourceError::Generation,
            );
        }
        let capture_gap = self.capture_sequence_gap(input.capture_sequence);
        if capture_gap
            && !(matches!(input.event, MarketEvent::Snapshot(_))
                && self
                    .generation
                    .is_none_or(|generation| event_generation > generation))
        {
            return self.reject(
                input.capture_sequence,
                event_generation,
                PublicMarketSourceError::Sequence,
            );
        }
        if self.generation.is_some_and(|generation| {
            event_generation > generation && !matches!(input.event, MarketEvent::Snapshot(_))
        }) {
            return self.reject(
                input.capture_sequence,
                event_generation,
                PublicMarketSourceError::Generation,
            );
        }

        let capture_sequence = input.capture_sequence;
        let mut feature_updated = false;
        match input.event {
            MarketEvent::Snapshot(snapshot) => {
                if self.generation.is_none()
                    || self
                        .generation
                        .is_some_and(|generation| event_generation > generation)
                {
                    self.reset_generation(event_generation);
                }
                if book.generation() != Some(snapshot.generation) || !book.synchronized() {
                    return self.reject(
                        capture_sequence,
                        event_generation,
                        PublicMarketSourceError::Generation,
                    );
                }
                if !book.bridged() {
                    self.last_capture_sequence = Some(capture_sequence);
                    self.state = FeatureState::Warmup;
                    return Ok(PublicMarketSourceOutput {
                        capture_sequence,
                        generation: self.generation,
                        state: self.state,
                        frame: None,
                    });
                }
                if let Err(source) = self.builder.ingest_book(book, input.received_at_ms) {
                    return Err(self.feature_failure(capture_sequence, source));
                }
                feature_updated = true;
            }
            MarketEvent::Delta(_delta) => {
                if self.generation != Some(event_generation)
                    || book.generation() != Some(event_generation)
                    || !book.bridged()
                {
                    return self.reject(
                        capture_sequence,
                        event_generation,
                        PublicMarketSourceError::Generation,
                    );
                }
                if let Err(source) = self.builder.ingest_book(book, input.received_at_ms) {
                    return Err(self.feature_failure(capture_sequence, source));
                }
                feature_updated = true;
            }
            MarketEvent::Trade(trade) => {
                if self.generation == Some(event_generation)
                    && book.generation() == Some(event_generation)
                    && book.bridged()
                {
                    if let Err(source) = self.builder.ingest_trade(&trade) {
                        return Err(self.feature_failure(capture_sequence, source));
                    }
                    feature_updated = true;
                }
            }
            MarketEvent::Bar(bar) => {
                if self.generation == Some(event_generation)
                    && book.generation() == Some(event_generation)
                    && book.bridged()
                {
                    if let Err(source) = self.builder.ingest_bar(&bar) {
                        return Err(self.feature_failure(capture_sequence, source));
                    }
                    feature_updated = true;
                }
            }
            MarketEvent::Ticker(_) | MarketEvent::MarkFunding(_) => {}
        }
        self.last_capture_sequence = Some(capture_sequence);
        if !book.bridged() {
            self.state = FeatureState::Warmup;
            return Ok(PublicMarketSourceOutput {
                capture_sequence,
                generation: self.generation,
                state: self.state,
                frame: None,
            });
        }
        let frame = match self.builder.frame(now_ms) {
            Ok(frame) => {
                self.state = frame.state;
                Some(frame)
            }
            Err(FeatureBuildError::Book) if !book.synchronized() => {
                self.state = FeatureState::Warmup;
                None
            }
            Err(source) => return Err(self.feature_failure(capture_sequence, source)),
        };
        let frame = if allow_emission
            && feature_updated
            && self.state == FeatureState::Ready
            && self
                .last_emitted_frame_at_ms
                .is_none_or(|last| now_ms.saturating_sub(last) >= FRAME_EMIT_INTERVAL_MS)
        {
            self.last_emitted_frame_at_ms = Some(now_ms);
            frame
        } else {
            None
        };
        Ok(PublicMarketSourceOutput {
            capture_sequence,
            generation: self.generation,
            state: self.state,
            frame,
        })
    }

    /// Samples the fully ingested batch at most once under the same 250ms output cadence.
    pub(crate) fn sample_batched_frame(
        &mut self,
        now_ms: u64,
    ) -> Result<Option<FeatureFrame>, PublicMarketSourceError> {
        let capture_sequence = self.last_capture_sequence.unwrap_or_default();
        let frame = self
            .builder
            .frame(now_ms)
            .map_err(|source| self.feature_failure(capture_sequence, source))?;
        self.state = frame.state;
        if self.state == FeatureState::Ready
            && self
                .last_emitted_frame_at_ms
                .is_none_or(|last| now_ms.saturating_sub(last) >= FRAME_EMIT_INTERVAL_MS)
        {
            self.last_emitted_frame_at_ms = Some(now_ms);
            Ok(Some(frame))
        } else {
            Ok(None)
        }
    }

    #[must_use]
    pub const fn state(&self) -> FeatureState {
        self.state
    }

    #[must_use]
    pub const fn generation(&self) -> Option<u64> {
        self.generation
    }

    /// Advances provenance for a normalized event that the order-book owner intentionally
    /// ignored as stale. It never feeds that event into the feature builder.
    pub fn observe_ignored_capture(
        &mut self,
        capture_sequence: u64,
    ) -> Result<(), PublicMarketSourceError> {
        if capture_sequence == 0 {
            self.state = FeatureState::DataGap;
            if let Some(generation) = self.generation {
                self.fenced_generation = Some(generation);
            }
            return Err(PublicMarketSourceError::Identity);
        }
        if self.capture_sequence_gap(capture_sequence) {
            return self.reject(
                capture_sequence,
                self.generation.unwrap_or_default(),
                PublicMarketSourceError::Sequence,
            );
        }
        self.last_capture_sequence = Some(capture_sequence);
        Ok(())
    }

    /// Revokes the current public generation after a transport, storage, or normalized-event
    /// fault. A strictly newer snapshot is required before the source can become Ready again.
    pub fn fence(&mut self) {
        self.state = FeatureState::DataGap;
        self.last_emitted_frame_at_ms = None;
        if let Some(generation) = self.generation {
            self.fenced_generation = Some(generation);
        }
    }

    fn validate_capture_identity(
        &self,
        input: &RecordedPublicEvent,
    ) -> Result<(), PublicMarketSourceError> {
        if input.capture_sequence == 0 || input.received_at_ms == 0 {
            return Err(PublicMarketSourceError::Identity);
        }
        Ok(())
    }

    fn capture_sequence_gap(&self, capture_sequence: u64) -> bool {
        self.last_capture_sequence
            .is_some_and(|previous| capture_sequence != previous.saturating_add(1))
    }

    fn reset_generation(&mut self, generation: u64) {
        self.generation = Some(generation);
        self.fenced_generation = None;
        self.state = FeatureState::Rebuilding;
        self.last_emitted_frame_at_ms = None;
    }

    fn reject<T>(
        &mut self,
        capture_sequence: u64,
        generation: u64,
        error: PublicMarketSourceError,
    ) -> Result<T, PublicMarketSourceError> {
        self.state = FeatureState::DataGap;
        let fence_generation = self.generation.unwrap_or_default().max(generation);
        if fence_generation != 0 {
            self.fenced_generation = Some(fence_generation);
        }
        self.last_capture_sequence = Some(capture_sequence);
        Err(error)
    }

    fn feature_failure(
        &mut self,
        capture_sequence: u64,
        source: FeatureBuildError,
    ) -> PublicMarketSourceError {
        self.state = FeatureState::DataGap;
        self.last_capture_sequence = Some(capture_sequence);
        if let Some(generation) = self.generation {
            self.fenced_generation = Some(generation);
        }
        PublicMarketSourceError::Feature(source)
    }
}

fn event_generation(event: &MarketEvent) -> u64 {
    match event {
        MarketEvent::Snapshot(value) => value.generation,
        MarketEvent::Delta(value) => value.generation,
        MarketEvent::Trade(value) => value.generation,
        MarketEvent::Bar(value) => value.generation,
        MarketEvent::Ticker(value) => value.generation,
        MarketEvent::MarkFunding(value) => value.generation,
    }
}

fn event_symbol(event: &MarketEvent) -> &Symbol {
    match event {
        MarketEvent::Snapshot(value) => &value.symbol,
        MarketEvent::Delta(value) => &value.symbol,
        MarketEvent::Trade(value) => &value.symbol,
        MarketEvent::Bar(value) => &value.symbol,
        MarketEvent::Ticker(value) => &value.symbol,
        MarketEvent::MarkFunding(value) => &value.symbol,
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PublicMarketSourceError {
    #[error("public market source identity is invalid")]
    Identity,
    #[error("public capture sequence is not strictly contiguous")]
    Sequence,
    #[error("public market generation moved backwards or lacks a new snapshot")]
    Generation,
    #[error("public market source is fenced by a data gap")]
    DataGap,
    #[error("public feature build failed: {0}")]
    Feature(FeatureBuildError),
}

impl From<FeatureBuildError> for PublicMarketSourceError {
    fn from(source: FeatureBuildError) -> Self {
        Self::Feature(source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_starts_warmup_without_private_inputs() -> Result<(), Box<dyn std::error::Error>> {
        let symbol: Symbol = "BTC/USDT".parse()?;
        let source = ScalpingPublicMarketSource::new(
            symbol,
            "scalping-shadow-v1",
            "0".repeat(64),
            65_000,
            NonZeroUsize::new(2_048).ok_or("history")?,
        )?;
        assert_eq!(source.state(), FeatureState::Warmup);
        assert_eq!(source.generation(), None);
        Ok(())
    }
}
