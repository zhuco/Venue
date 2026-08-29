use crate::{
    backoff::jittered_exponential_delay_ms,
    domain::{MarketDelta, MarketEvent, Symbol},
    exchange::binance,
    market::{BookError, OrderBook, RawError, RawMarketRecord, RawMarketRecorder, RawSource},
};
use std::path::Path;

const MAX_BUFFERED_DELTAS: usize = 1_024;
const FRESHNESS_MS: u64 = 5_000;
const BACKOFF_BASE_MS: u64 = 1_000;
const BACKOFF_CAP_MS: u64 = 30_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionState {
    Snapshotting,
    Ready,
    Backoff,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportFault {
    Disconnected,
    RateLimited,
    ServerFailure,
    RulesChanged,
}

/// A single writer owns raw capture, ordering and readiness for one symbol.
/// A non-ready generation never publishes an order book to downstream users.
#[derive(Debug)]
pub struct MarketSession {
    symbol: Symbol,
    generation: u64,
    recorder: RawMarketRecorder,
    book: OrderBook,
    state: SessionState,
    buffered_deltas: Vec<MarketDelta>,
    failures: u8,
    retry_at_ms: Option<u64>,
    last_received_at_ms: Option<u64>,
}

/// One normalized event whose raw capture has been appended in order. The public worker syncs
/// pending raw records before any frame or observation can affect durable business state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturedMarketEvent {
    pub record: RawMarketRecord,
    pub event: MarketEvent,
    pub applied: bool,
}

impl MarketSession {
    pub fn new(symbol: Symbol, recorder: RawMarketRecorder) -> Self {
        Self {
            symbol,
            generation: 1,
            recorder,
            book: OrderBook::default(),
            state: SessionState::Snapshotting,
            buffered_deltas: Vec::new(),
            failures: 0,
            retry_at_ms: None,
            last_received_at_ms: None,
        }
    }

    /// Reconstructs a fresh process generation from a symbol-scoped journal. The old book and
    /// buffered deltas are intentionally not restored; the worker must reconnect and snapshot.
    pub fn recover(symbol: Symbol, mut recorder: RawMarketRecorder) -> Result<Self, SessionError> {
        recorder.bind_symbol(&symbol).map_err(SessionError::Raw)?;
        let generation = recorder.last_generation().map_or(Ok(1), |last| {
            last.checked_add(1).ok_or(SessionError::Generation)
        })?;
        Ok(Self {
            symbol,
            generation,
            recorder,
            book: OrderBook::default(),
            state: SessionState::Snapshotting,
            buffered_deltas: Vec::new(),
            failures: 0,
            retry_at_ms: None,
            last_received_at_ms: None,
        })
    }

    pub fn open_recovered(symbol: Symbol, path: impl AsRef<Path>) -> Result<Self, SessionError> {
        let recorder =
            RawMarketRecorder::open_for_symbol(path.as_ref().to_path_buf(), symbol.clone())
                .map_err(SessionError::Raw)?;
        Self::recover(symbol, recorder)
    }

    pub fn ingest_snapshot(
        &mut self,
        received_at_ms: u64,
        payload: String,
    ) -> Result<(), SessionError> {
        self.ingest_snapshot_captured(received_at_ms, payload)
            .map(|_| ())
    }

    pub fn ingest_snapshot_captured(
        &mut self,
        received_at_ms: u64,
        payload: String,
    ) -> Result<CapturedMarketEvent, SessionError> {
        self.ensure_accepting()?;
        match self.capture(RawSource::RestSnapshot, received_at_ms, payload)? {
            CapturedMarketEvent {
                record,
                event: MarketEvent::Snapshot(snapshot),
                ..
            } => {
                self.book.apply_snapshot(snapshot.clone());
                self.state = SessionState::Ready;
                self.last_received_at_ms = Some(received_at_ms);
                let buffered = std::mem::take(&mut self.buffered_deltas);
                for delta in buffered {
                    self.apply_delta(delta, received_at_ms)?;
                }
                Ok(CapturedMarketEvent {
                    record,
                    event: MarketEvent::Snapshot(snapshot),
                    applied: true,
                })
            }
            _ => self.fail(SessionError::UnexpectedEvent, received_at_ms),
        }
    }

    pub fn ingest_delta(
        &mut self,
        received_at_ms: u64,
        payload: String,
    ) -> Result<(), SessionError> {
        self.ingest_delta_captured_or_ignored(received_at_ms, payload)
            .map(|_| ())
    }

    pub fn ingest_delta_captured(
        &mut self,
        received_at_ms: u64,
        payload: String,
    ) -> Result<CapturedMarketEvent, SessionError> {
        self.ingest_delta_captured_or_ignored(received_at_ms, payload)
    }

    pub fn ingest_delta_captured_or_ignored(
        &mut self,
        received_at_ms: u64,
        payload: String,
    ) -> Result<CapturedMarketEvent, SessionError> {
        self.ensure_accepting()?;
        match self.capture(RawSource::WebSocketDelta, received_at_ms, payload)? {
            CapturedMarketEvent {
                record,
                event: MarketEvent::Delta(delta),
                ..
            } if self.state == SessionState::Snapshotting => {
                if self.buffered_deltas.len() == MAX_BUFFERED_DELTAS {
                    self.fail(SessionError::BufferFull, received_at_ms)
                } else {
                    self.buffered_deltas.push(delta.clone());
                    self.last_received_at_ms = Some(received_at_ms);
                    Ok(CapturedMarketEvent {
                        record,
                        event: MarketEvent::Delta(delta),
                        applied: false,
                    })
                }
            }
            CapturedMarketEvent {
                record,
                event: MarketEvent::Delta(delta),
                ..
            } => {
                let applied = self.apply_delta(delta.clone(), received_at_ms)?;
                Ok(CapturedMarketEvent {
                    record,
                    event: MarketEvent::Delta(delta),
                    applied,
                })
            }
            _ => self.fail(SessionError::UnexpectedEvent, received_at_ms),
        }
    }

    /// Captures non-book public data without allowing it to change book readiness.
    /// Callers must use the dedicated snapshot and delta paths for book sequencing.
    pub fn ingest_auxiliary(
        &mut self,
        source: RawSource,
        received_at_ms: u64,
        payload: String,
    ) -> Result<MarketEvent, SessionError> {
        self.ingest_auxiliary_captured(source, received_at_ms, payload)
            .map(|captured| captured.event)
    }

    pub fn ingest_auxiliary_captured(
        &mut self,
        source: RawSource,
        received_at_ms: u64,
        payload: String,
    ) -> Result<CapturedMarketEvent, SessionError> {
        if !matches!(
            source,
            RawSource::RestKline
                | RawSource::WebSocketTrade
                | RawSource::WebSocketKline
                | RawSource::WebSocketTicker
                | RawSource::WebSocketMarkFunding
        ) {
            return self.fail(SessionError::UnexpectedEvent, received_at_ms);
        }
        self.ensure_accepting()?;
        match self.capture(source, received_at_ms, payload)? {
            captured @ CapturedMarketEvent {
                event:
                    MarketEvent::Trade(_)
                    | MarketEvent::Bar(_)
                    | MarketEvent::Ticker(_)
                    | MarketEvent::MarkFunding(_),
                ..
            } => {
                self.last_received_at_ms = Some(received_at_ms);
                Ok(captured)
            }
            _ => self.fail(SessionError::UnexpectedEvent, received_at_ms),
        }
    }

    pub fn on_transport_fault(
        &mut self,
        now_ms: u64,
        fault: TransportFault,
    ) -> Result<(), SessionError> {
        self.fail(SessionError::Transport(fault), now_ms)
    }

    pub fn begin_retry(&mut self, now_ms: u64) -> bool {
        if self.state == SessionState::Backoff
            && self.retry_at_ms.is_some_and(|retry_at| now_ms >= retry_at)
        {
            self.state = SessionState::Snapshotting;
            self.retry_at_ms = None;
            return true;
        }
        false
    }

    pub fn ensure_fresh(&mut self, now_ms: u64) -> Result<(), SessionError> {
        if self.state == SessionState::Ready
            && self
                .last_received_at_ms
                .is_none_or(|last| now_ms.saturating_sub(last) > FRESHNESS_MS)
        {
            return self.fail(SessionError::Stale, now_ms);
        }
        Ok(())
    }

    pub fn ready(&self) -> bool {
        self.state == SessionState::Ready && self.book.synchronized()
    }
    pub const fn generation(&self) -> u64 {
        self.generation
    }
    pub const fn state(&self) -> SessionState {
        self.state
    }
    pub const fn retry_at_ms(&self) -> Option<u64> {
        self.retry_at_ms
    }
    pub fn recorder(&self) -> &RawMarketRecorder {
        &self.recorder
    }

    pub(crate) fn sync_raw_capture(&mut self) -> Result<(), SessionError> {
        self.recorder.sync_pending().map_err(SessionError::Raw)
    }

    pub fn book(&self) -> &OrderBook {
        &self.book
    }

    fn apply_delta(
        &mut self,
        delta: MarketDelta,
        received_at_ms: u64,
    ) -> Result<bool, SessionError> {
        match self.book.apply_delta_if_fresh(delta) {
            Ok(applied) => {
                self.last_received_at_ms = Some(received_at_ms);
                Ok(applied)
            }
            Err(source) => self.fail(SessionError::Book(source), received_at_ms),
        }
    }

    fn capture(
        &mut self,
        source: RawSource,
        received_at_ms: u64,
        payload: String,
    ) -> Result<CapturedMarketEvent, SessionError> {
        let mut record = RawMarketRecord::new(
            source,
            self.symbol.clone(),
            self.generation,
            received_at_ms,
            payload,
        )
        .map_err(SessionError::Raw)?;
        record.capture_sequence = self
            .recorder
            .append(record.clone())
            .map_err(SessionError::Raw)?;
        let native_symbol = binance::native_symbol(&self.symbol);
        match binance::normalize(&record, &native_symbol) {
            Ok(event) => Ok(CapturedMarketEvent {
                record,
                event,
                applied: true,
            }),
            Err(source) => self.fail(SessionError::Normalize(source), received_at_ms),
        }
    }

    fn ensure_accepting(&self) -> Result<(), SessionError> {
        if self.state == SessionState::Backoff {
            Err(SessionError::Backoff)
        } else {
            Ok(())
        }
    }

    fn fail<T>(&mut self, error: SessionError, now_ms: u64) -> Result<T, SessionError> {
        self.book = OrderBook::default();
        self.buffered_deltas.clear();
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or(SessionError::Generation)?;
        self.failures = self.failures.saturating_add(1);
        let delay_ms = jittered_exponential_delay_ms(
            BACKOFF_BASE_MS,
            BACKOFF_CAP_MS,
            self.failures,
            &self.symbol.to_string(),
            now_ms,
        );
        self.retry_at_ms = Some(now_ms.saturating_add(delay_ms));
        self.state = SessionState::Backoff;
        Err(error)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("raw capture failed: {0}")]
    Raw(RawError),
    #[error("Binance normalization failed: {0}")]
    Normalize(binance::BinanceError),
    #[error("order book failed: {0}")]
    Book(BookError),
    #[error("market source emitted the wrong event kind")]
    UnexpectedEvent,
    #[error("market delta buffer is full")]
    BufferFull,
    #[error("market data is stale")]
    Stale,
    #[error("market transport fault: {0:?}")]
    Transport(TransportFault),
    #[error("market session is backing off")]
    Backoff,
    #[error("market generation is exhausted")]
    Generation,
}
