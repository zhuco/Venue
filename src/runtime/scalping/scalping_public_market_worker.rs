use crate::{
    domain::Symbol,
    exchange::binance::{PublicError, PublicRest, PublicStream, PublicStreamSocket},
    indicator::{
        FeatureFrame, FeatureState, PublicMarketSourceError, RecordedPublicEvent,
        ScalpingPublicMarketSource,
    },
    market::{
        CapturedMarketEvent, MarketSession, RawSource, SessionError, SessionState, TransportFault,
    },
};

pub const PUBLIC_STREAMS: [PublicStream; 4] = [
    PublicStream::DiffDepth,
    PublicStream::AggTrade,
    PublicStream::Kline1m,
    PublicStream::MarkFunding,
];
const PUBLIC_READ_SCHEDULE: [PublicStream; 8] = [
    PublicStream::DiffDepth,
    PublicStream::AggTrade,
    PublicStream::DiffDepth,
    PublicStream::AggTrade,
    PublicStream::DiffDepth,
    PublicStream::Kline1m,
    PublicStream::DiffDepth,
    PublicStream::MarkFunding,
];
const MAX_DIFF_DEPTH_PER_EFFECT: usize = 256;
const MAX_AGG_TRADES_PER_EFFECT: usize = 128;
pub const DEPTH_SNAPSHOT_LIMIT: u16 = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicCaptureEffect {
    FetchDepthSnapshot { limit: u16 },
    FetchClosedKlineBootstrap,
    Connect { stream: PublicStream },
    Read { stream: PublicStream },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PublicCaptureCompletion {
    DepthSnapshot {
        received_at_ms: u64,
        payload: String,
    },
    ClosedKlineBootstrap {
        received_at_ms: u64,
        payload: String,
    },
    StreamConnected {
        stream: PublicStream,
    },
    StreamFrame {
        stream: PublicStream,
        received_at_ms: u64,
        payload: String,
    },
    StreamFrames {
        stream: PublicStream,
        received_at_ms: u64,
        payloads: Vec<String>,
    },
    StreamReady {
        stream: PublicStream,
    },
    Fault {
        stream: Option<PublicStream>,
        fault: PublicCaptureFault,
        now_ms: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicCaptureFault {
    Disconnected,
    RateLimited,
    ServerFailure,
    Parse,
    Sequence,
    Storage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicCaptureOutput {
    pub event: RecordedPublicEvent,
    pub generation: u64,
    pub state: FeatureState,
    pub frame: Option<FeatureFrame>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerPhase {
    Connecting(usize),
    Snapshot,
    KlineBootstrap,
    BridgingDepth,
    Reading(usize),
    Backoff,
}

/// One-step, effect-driven Binance public capture worker. It owns the single MarketSession and
/// feeds its durable normalized captures into the public FeatureFrame source; no resident,
/// controller, evidence, risk, or mutation capability crosses this boundary.
#[derive(Debug)]
pub struct ScalpingPublicMarketWorker {
    session: MarketSession,
    source: ScalpingPublicMarketSource,
    phase: WorkerPhase,
    in_flight: Option<PublicCaptureEffect>,
}

impl ScalpingPublicMarketWorker {
    #[must_use]
    pub fn new(session: MarketSession, source: ScalpingPublicMarketSource) -> Self {
        Self {
            session,
            source,
            phase: WorkerPhase::Connecting(0),
            in_flight: None,
        }
    }

    /// Builds a worker for a recovered process. The source must be a fresh Warmup instance and
    /// every persisted generation segment must begin with its REST snapshot; the session itself
    /// already owns the checked `last_generation + 1` decision.
    pub(crate) fn new_recovered(
        session: MarketSession,
        source: ScalpingPublicMarketSource,
    ) -> Result<Self, PublicCaptureWorkerError> {
        let expected_generation = session
            .recorder()
            .last_generation()
            .map_or(Some(1), |last| last.checked_add(1));
        if source.state() != FeatureState::Warmup
            || source.generation().is_some()
            || session.state() != SessionState::Snapshotting
            || session.book().synchronized()
            || session.book().sequence().is_some()
            || expected_generation != Some(session.generation())
        {
            return Err(PublicCaptureWorkerError::RecoveredSourceNotFresh);
        }
        validate_recovered_journal(&session)?;
        Ok(Self::new(session, source))
    }

    /// Opens the symbol-scoped journal and creates the recovered worker without exposing a
    /// generation knob to the caller.
    pub fn open_recovered(
        symbol: Symbol,
        path: impl AsRef<std::path::Path>,
        source: ScalpingPublicMarketSource,
    ) -> Result<Self, PublicCaptureWorkerError> {
        let session = MarketSession::open_recovered(symbol, path)
            .map_err(PublicCaptureWorkerError::Session)?;
        Self::new_recovered(session, source)
    }

    /// Returns one network effect. Until its matching completion arrives, no second effect can
    /// be issued, so a caller cannot accidentally loop over a socket or REST response.
    pub fn next_effect(&mut self, now_ms: u64) -> Option<PublicCaptureEffect> {
        if self.in_flight.is_some() {
            return None;
        }
        if self.phase == WorkerPhase::Backoff {
            if self.session.state() != SessionState::Backoff
                || self
                    .session
                    .retry_at_ms()
                    .is_none_or(|retry_at| now_ms < retry_at)
            {
                return None;
            }
            if !self.session.begin_retry(now_ms) {
                return None;
            }
            self.phase = WorkerPhase::Connecting(0);
        }
        let effect = match self.phase {
            WorkerPhase::Snapshot => PublicCaptureEffect::FetchDepthSnapshot {
                limit: DEPTH_SNAPSHOT_LIMIT,
            },
            WorkerPhase::KlineBootstrap => PublicCaptureEffect::FetchClosedKlineBootstrap,
            WorkerPhase::Connecting(index) => PublicCaptureEffect::Connect {
                stream: PUBLIC_STREAMS[index],
            },
            WorkerPhase::BridgingDepth => PublicCaptureEffect::Read {
                stream: PublicStream::DiffDepth,
            },
            WorkerPhase::Reading(index) => PublicCaptureEffect::Read {
                stream: PUBLIC_READ_SCHEDULE[index],
            },
            WorkerPhase::Backoff => return None,
        };
        self.in_flight = Some(effect);
        Some(effect)
    }

    pub fn complete(
        &mut self,
        completion: PublicCaptureCompletion,
    ) -> Result<Option<PublicCaptureOutput>, PublicCaptureWorkerError> {
        let effect = self
            .in_flight
            .ok_or(PublicCaptureWorkerError::CompletionWithoutEffect)?;
        if !completion_matches(effect, &completion) {
            return Err(PublicCaptureWorkerError::UnexpectedCompletion);
        }
        self.in_flight = None;
        match (effect, completion) {
            (
                PublicCaptureEffect::FetchDepthSnapshot { .. },
                PublicCaptureCompletion::DepthSnapshot {
                    received_at_ms,
                    payload,
                },
            ) => {
                let captured = self
                    .session
                    .ingest_snapshot_captured(received_at_ms, payload)
                    .map_err(|source| self.fail_session(source, received_at_ms))?;
                // The feature source deliberately rejects bars until the depth snapshot has
                // been bridged. Bootstrap only after that fence is satisfied so the ATR history
                // is actually consumed rather than durably recorded and silently ignored.
                self.phase = WorkerPhase::BridgingDepth;
                self.publish(captured, received_at_ms).map(Some)
            }
            (
                PublicCaptureEffect::FetchClosedKlineBootstrap,
                PublicCaptureCompletion::ClosedKlineBootstrap {
                    received_at_ms,
                    payload,
                },
            ) => {
                let rows = crate::exchange::binance::split_closed_kline_bootstrap(
                    &payload,
                    received_at_ms,
                )
                .map_err(|source| {
                    self.fail_session(SessionError::Normalize(source), received_at_ms)
                })?;
                let mut output = None;
                for row in rows {
                    let captured = self
                        .session
                        .ingest_auxiliary_captured(RawSource::RestKline, received_at_ms, row)
                        .map_err(|source| self.fail_session(source, received_at_ms))?;
                    output = Some(self.publish(captured, received_at_ms)?);
                }
                self.phase = WorkerPhase::Connecting(1);
                Ok(output)
            }
            (
                PublicCaptureEffect::Connect { stream },
                PublicCaptureCompletion::StreamConnected { .. },
            ) => {
                self.phase = if stream == PublicStream::DiffDepth {
                    WorkerPhase::Snapshot
                } else {
                    let next = stream_index(stream).saturating_add(1);
                    if next < PUBLIC_STREAMS.len() {
                        WorkerPhase::Connecting(next)
                    } else {
                        WorkerPhase::Reading(0)
                    }
                };
                Ok(None)
            }
            (
                PublicCaptureEffect::Read { stream },
                PublicCaptureCompletion::StreamFrame {
                    received_at_ms,
                    payload,
                    ..
                },
            ) => {
                let captured = self
                    .ingest_stream_frame(stream, received_at_ms, payload)
                    .map_err(|source| self.fail_session(source, received_at_ms))?;
                let Some(captured) = captured else {
                    self.advance_after_read(stream, false);
                    return Ok(None);
                };
                self.advance_after_read(stream, captured.applied);
                if !captured.applied {
                    self.source
                        .observe_ignored_capture(captured.record.capture_sequence)
                        .map_err(|source| self.fail_source(source, received_at_ms))?;
                    return Ok(None);
                }
                let output = self.publish(captured, received_at_ms)?;
                if output.state == FeatureState::Stale {
                    self.enter_backoff(PublicCaptureFault::Disconnected, received_at_ms)?;
                }
                Ok(Some(output))
            }
            (
                PublicCaptureEffect::Read { stream },
                PublicCaptureCompletion::StreamFrames {
                    received_at_ms,
                    payloads,
                    ..
                },
            ) => {
                let mut output = None;
                let mut applied = false;
                let mut needs_sync = false;
                for payload in payloads {
                    let captured = self
                        .ingest_stream_frame(stream, received_at_ms, payload)
                        .map_err(|source| self.fail_session(source, received_at_ms))?;
                    let Some(captured) = captured else {
                        continue;
                    };
                    if !captured.applied {
                        self.source
                            .observe_ignored_capture(captured.record.capture_sequence)
                            .map_err(|source| self.fail_source(source, received_at_ms))?;
                        continue;
                    }
                    applied = true;
                    let published = self.publish_batched_without_sync(captured, received_at_ms)?;
                    output = Some(published);
                }
                self.advance_after_read(stream, applied);
                if let Some(output) = output.as_mut() {
                    output.frame = self
                        .source
                        .sample_batched_frame(received_at_ms)
                        .map_err(|source| self.fail_source(source, received_at_ms))?;
                    output.state = self.source.state();
                    needs_sync = output.frame.is_some();
                }
                if needs_sync {
                    self.session
                        .sync_raw_capture()
                        .map_err(|source| self.fail_session(source, received_at_ms))?;
                }
                if output
                    .as_ref()
                    .is_some_and(|output| output.state == FeatureState::Stale)
                {
                    self.enter_backoff(PublicCaptureFault::Disconnected, received_at_ms)?;
                }
                Ok(output)
            }
            (PublicCaptureEffect::Read { stream }, PublicCaptureCompletion::StreamReady { .. }) => {
                self.advance_after_read(stream, false);
                Ok(None)
            }
            (_effect, PublicCaptureCompletion::Fault { fault, now_ms, .. }) => {
                self.enter_backoff(fault, now_ms)?;
                Ok(None)
            }
            _ => Err(PublicCaptureWorkerError::UnexpectedCompletion),
        }
    }

    #[must_use]
    pub const fn session_state(&self) -> SessionState {
        self.session.state()
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.session.generation()
    }

    #[must_use]
    pub const fn session_retry_at_ms(&self) -> Option<u64> {
        self.session.retry_at_ms()
    }

    #[must_use]
    pub const fn feature_state(&self) -> FeatureState {
        self.source.state()
    }

    #[must_use]
    pub const fn has_in_flight(&self) -> bool {
        self.in_flight.is_some()
    }

    pub fn recorder(&self) -> &crate::market::RawMarketRecorder {
        self.session.recorder()
    }

    fn ingest_stream_frame(
        &mut self,
        stream: PublicStream,
        received_at_ms: u64,
        payload: String,
    ) -> Result<Option<CapturedMarketEvent>, SessionError> {
        match stream {
            PublicStream::DiffDepth => self
                .session
                .ingest_delta_captured_or_ignored(received_at_ms, payload)
                .map(Some),
            PublicStream::AggTrade => self
                .session
                .ingest_auxiliary_captured(RawSource::WebSocketTrade, received_at_ms, payload)
                .map(Some),
            PublicStream::Kline1m => {
                let native = crate::exchange::binance::native_symbol(self.source.symbol());
                if !crate::exchange::binance::kline_payload_is_closed(&payload, &native)
                    .map_err(SessionError::Normalize)?
                {
                    return Ok(None);
                }
                self.session
                    .ingest_auxiliary_captured(RawSource::WebSocketKline, received_at_ms, payload)
                    .map(Some)
            }
            PublicStream::MarkFunding => self
                .session
                .ingest_auxiliary_captured(RawSource::WebSocketMarkFunding, received_at_ms, payload)
                .map(Some),
            PublicStream::BookTicker => Err(SessionError::UnexpectedEvent),
        }
    }

    fn publish(
        &mut self,
        captured: CapturedMarketEvent,
        now_ms: u64,
    ) -> Result<PublicCaptureOutput, PublicCaptureWorkerError> {
        let output = self.publish_without_sync(captured, now_ms)?;
        if output.frame.is_some()
            || matches!(
                &output.event.event,
                crate::domain::MarketEvent::MarkFunding(_)
            )
        {
            self.session
                .sync_raw_capture()
                .map_err(|source| self.fail_session(source, now_ms))?;
        }
        Ok(output)
    }

    fn publish_without_sync(
        &mut self,
        captured: CapturedMarketEvent,
        now_ms: u64,
    ) -> Result<PublicCaptureOutput, PublicCaptureWorkerError> {
        let recorded = RecordedPublicEvent {
            capture_sequence: captured.record.capture_sequence,
            received_at_ms: captured.record.received_at_ms,
            event: captured.event,
        };
        let source_output = self
            .source
            .consume(recorded.clone(), self.session.book(), now_ms)
            .map_err(|source| self.fail_source(source, now_ms))?;
        Ok(PublicCaptureOutput {
            event: recorded,
            generation: self.session.generation(),
            state: source_output.state,
            frame: source_output.frame,
        })
    }

    fn publish_batched_without_sync(
        &mut self,
        captured: CapturedMarketEvent,
        now_ms: u64,
    ) -> Result<PublicCaptureOutput, PublicCaptureWorkerError> {
        let recorded = RecordedPublicEvent {
            capture_sequence: captured.record.capture_sequence,
            received_at_ms: captured.record.received_at_ms,
            event: captured.event,
        };
        let source_output = self
            .source
            .consume_batched(recorded.clone(), self.session.book(), now_ms)
            .map_err(|source| self.fail_source(source, now_ms))?;
        Ok(PublicCaptureOutput {
            event: recorded,
            generation: self.session.generation(),
            state: source_output.state,
            frame: None,
        })
    }

    fn advance_after_read(&mut self, stream: PublicStream, captured_applied: bool) {
        if self.phase == WorkerPhase::BridgingDepth {
            if captured_applied && self.session.book().synchronized() {
                self.phase = WorkerPhase::KlineBootstrap;
            }
            return;
        }
        let WorkerPhase::Reading(index) = self.phase else {
            return;
        };
        if PUBLIC_READ_SCHEDULE[index] != stream {
            self.phase = WorkerPhase::Backoff;
            return;
        }
        self.phase = WorkerPhase::Reading((index + 1) % PUBLIC_READ_SCHEDULE.len());
    }

    fn fail_session(&mut self, source: SessionError, now_ms: u64) -> PublicCaptureWorkerError {
        let fault = match &source {
            SessionError::Normalize(_) => PublicCaptureFault::Parse,
            SessionError::Book(_) => PublicCaptureFault::Sequence,
            SessionError::Raw(_) => PublicCaptureFault::Storage,
            SessionError::Transport(TransportFault::Disconnected) => {
                PublicCaptureFault::Disconnected
            }
            SessionError::Transport(TransportFault::RateLimited) => PublicCaptureFault::RateLimited,
            SessionError::Transport(TransportFault::ServerFailure) => {
                PublicCaptureFault::ServerFailure
            }
            SessionError::Transport(TransportFault::RulesChanged) => PublicCaptureFault::Sequence,
            SessionError::UnexpectedEvent
            | SessionError::BufferFull
            | SessionError::Stale
            | SessionError::Backoff
            | SessionError::Generation => PublicCaptureFault::Storage,
        };
        let _ = self.enter_backoff(fault, now_ms);
        PublicCaptureWorkerError::Session(source)
    }

    fn fail_source(
        &mut self,
        source: PublicMarketSourceError,
        now_ms: u64,
    ) -> PublicCaptureWorkerError {
        let fault = match &source {
            PublicMarketSourceError::Sequence
            | PublicMarketSourceError::Generation
            | PublicMarketSourceError::DataGap => PublicCaptureFault::Sequence,
            PublicMarketSourceError::Identity | PublicMarketSourceError::Feature(_) => {
                PublicCaptureFault::Parse
            }
        };
        let _ = self.enter_backoff(fault, now_ms);
        PublicCaptureWorkerError::Source(source)
    }

    fn enter_backoff(
        &mut self,
        fault: PublicCaptureFault,
        now_ms: u64,
    ) -> Result<(), PublicCaptureWorkerError> {
        self.session
            .sync_raw_capture()
            .map_err(PublicCaptureWorkerError::Session)?;
        self.source.fence();
        if self.session.state() != SessionState::Backoff {
            match self
                .session
                .on_transport_fault(now_ms, transport_fault(fault))
            {
                Err(SessionError::Transport(_)) => {}
                Err(error) => return Err(PublicCaptureWorkerError::Session(error)),
                Ok(()) => {}
            }
        }
        self.phase = WorkerPhase::Backoff;
        Ok(())
    }
}

fn completion_matches(effect: PublicCaptureEffect, completion: &PublicCaptureCompletion) -> bool {
    match (effect, completion) {
        (
            PublicCaptureEffect::FetchDepthSnapshot { .. },
            PublicCaptureCompletion::DepthSnapshot { .. },
        ) => true,
        (
            PublicCaptureEffect::FetchClosedKlineBootstrap,
            PublicCaptureCompletion::ClosedKlineBootstrap { .. },
        ) => true,
        (
            PublicCaptureEffect::Connect { stream: left },
            PublicCaptureCompletion::StreamConnected { stream: right },
        ) => left == *right,
        (
            PublicCaptureEffect::Read { stream: left },
            PublicCaptureCompletion::StreamFrame { stream: right, .. },
        ) => left == *right,
        (
            PublicCaptureEffect::Read { stream: left },
            PublicCaptureCompletion::StreamFrames { stream: right, .. },
        ) => left == *right,
        (
            PublicCaptureEffect::Read { stream: left },
            PublicCaptureCompletion::StreamReady { stream: right },
        ) => left == *right,
        (
            PublicCaptureEffect::FetchDepthSnapshot { .. },
            PublicCaptureCompletion::Fault { stream: None, .. },
        ) => true,
        (
            PublicCaptureEffect::FetchClosedKlineBootstrap,
            PublicCaptureCompletion::Fault { stream: None, .. },
        ) => true,
        (
            PublicCaptureEffect::Connect { stream: expected },
            PublicCaptureCompletion::Fault {
                stream: Some(actual),
                ..
            },
        )
        | (
            PublicCaptureEffect::Read { stream: expected },
            PublicCaptureCompletion::Fault {
                stream: Some(actual),
                ..
            },
        ) => expected == *actual,
        _ => false,
    }
}

/// Converts a transport failure into the only completion shape that can fence the pending effect.
/// Drivers must submit this completion even when the transport itself returned an error.
#[must_use]
pub fn transport_error_completion(
    effect: PublicCaptureEffect,
    error: &PublicCaptureTransportError,
    now_ms: u64,
) -> PublicCaptureCompletion {
    let stream = match effect {
        PublicCaptureEffect::FetchDepthSnapshot { .. }
        | PublicCaptureEffect::FetchClosedKlineBootstrap => None,
        PublicCaptureEffect::Connect { stream } | PublicCaptureEffect::Read { stream } => {
            Some(stream)
        }
    };
    PublicCaptureCompletion::Fault {
        stream,
        fault: transport_error_fault(error),
        now_ms,
    }
}

fn transport_error_fault(error: &PublicCaptureTransportError) -> PublicCaptureFault {
    match error {
        PublicCaptureTransportError::Public(PublicError::RateLimited) => {
            PublicCaptureFault::RateLimited
        }
        PublicCaptureTransportError::Public(PublicError::ServerFailure(_)) => {
            PublicCaptureFault::ServerFailure
        }
        PublicCaptureTransportError::Public(PublicError::DepthLimit) => PublicCaptureFault::Parse,
        PublicCaptureTransportError::Public(
            PublicError::Http(_)
            | PublicError::TransportRetriesExhausted
            | PublicError::Proxy
            | PublicError::WebSocket(_)
            | PublicError::HttpStatus(_)
            | PublicError::Closed,
        )
        | PublicCaptureTransportError::NotConnected => PublicCaptureFault::Disconnected,
    }
}

fn stream_index(stream: PublicStream) -> usize {
    match stream {
        PublicStream::DiffDepth => 0,
        PublicStream::AggTrade => 1,
        PublicStream::Kline1m => 2,
        PublicStream::MarkFunding => 3,
        PublicStream::BookTicker => 0,
    }
}

fn transport_fault(fault: PublicCaptureFault) -> TransportFault {
    match fault {
        PublicCaptureFault::Disconnected => TransportFault::Disconnected,
        PublicCaptureFault::RateLimited => TransportFault::RateLimited,
        PublicCaptureFault::ServerFailure | PublicCaptureFault::Storage => {
            TransportFault::ServerFailure
        }
        PublicCaptureFault::Parse | PublicCaptureFault::Sequence => TransportFault::RulesChanged,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PublicCaptureWorkerError {
    #[error("public capture completion arrived without a pending effect")]
    CompletionWithoutEffect,
    #[error("public capture completion does not match the pending effect")]
    UnexpectedCompletion,
    #[error("public market session failed: {0}")]
    Session(SessionError),
    #[error("public feature source failed: {0}")]
    Source(PublicMarketSourceError),
    #[error("recovered public feature source is not fresh Warmup")]
    RecoveredSourceNotFresh,
    #[error("recovered public journal generation does not begin with a snapshot")]
    RecoveredJournal,
}

fn validate_recovered_journal(session: &MarketSession) -> Result<(), PublicCaptureWorkerError> {
    let records = session
        .recorder()
        .recover()
        .map_err(|source| PublicCaptureWorkerError::Session(SessionError::Raw(source)))?;
    let mut generation = None;
    for record in records.records {
        if generation != Some(record.generation) {
            if record.source != RawSource::RestSnapshot {
                return Err(PublicCaptureWorkerError::RecoveredJournal);
            }
            generation = Some(record.generation);
        }
    }
    Ok(())
}

/// Optional production-side effect executor. Calling `execute` performs exactly one REST request,
/// connect, or socket read; tests can replace it with a fake without opening a network socket.
pub struct BinancePublicCaptureTransport {
    symbol: Symbol,
    rest: PublicRest,
    sockets: [Option<PublicStreamSocket>; 4],
}

/// One-effect execution boundary. Production performs one REST/socket step; tests inject a fake
/// and never construct or call the production transport.
pub trait PublicCaptureEffectExecutor {
    fn execute_effect(
        &mut self,
        effect: PublicCaptureEffect,
        received_at_ms: u64,
    ) -> Result<PublicCaptureCompletion, PublicCaptureTransportError>;
}

impl BinancePublicCaptureTransport {
    pub fn new(symbol: Symbol, rest: PublicRest) -> Self {
        Self {
            symbol,
            rest,
            sockets: [None, None, None, None],
        }
    }

    pub fn execute(
        &mut self,
        effect: PublicCaptureEffect,
        received_at_ms: u64,
    ) -> Result<PublicCaptureCompletion, PublicCaptureTransportError> {
        match effect {
            PublicCaptureEffect::FetchDepthSnapshot { limit } => self
                .rest
                .depth_snapshot(&self.symbol, limit)
                .map(|payload| PublicCaptureCompletion::DepthSnapshot {
                    received_at_ms,
                    payload,
                })
                .map_err(PublicCaptureTransportError::Public),
            PublicCaptureEffect::FetchClosedKlineBootstrap => self
                .rest
                .closed_kline_bootstrap(&self.symbol)
                .map(|payload| PublicCaptureCompletion::ClosedKlineBootstrap {
                    received_at_ms,
                    payload,
                })
                .map_err(PublicCaptureTransportError::Public),
            PublicCaptureEffect::Connect { stream } => {
                let socket = PublicStreamSocket::connect(&self.symbol, stream)
                    .map_err(PublicCaptureTransportError::Public)?;
                self.sockets[stream_index(stream)] = Some(socket);
                Ok(PublicCaptureCompletion::StreamConnected { stream })
            }
            PublicCaptureEffect::Read { stream } => {
                let socket = self.sockets[stream_index(stream)]
                    .as_mut()
                    .ok_or(PublicCaptureTransportError::NotConnected)?;
                if matches!(stream, PublicStream::DiffDepth | PublicStream::AggTrade) {
                    let max_frames = if stream == PublicStream::DiffDepth {
                        MAX_DIFF_DEPTH_PER_EFFECT
                    } else {
                        MAX_AGG_TRADES_PER_EFFECT
                    };
                    let payloads = socket
                        .next_text_batch_when_ready(max_frames)
                        .map_err(PublicCaptureTransportError::Public)?;
                    return if payloads.is_empty() {
                        Ok(PublicCaptureCompletion::StreamReady { stream })
                    } else {
                        Ok(PublicCaptureCompletion::StreamFrames {
                            stream,
                            received_at_ms,
                            payloads,
                        })
                    };
                }
                let payload = if stream == PublicStream::Kline1m {
                    socket.next_kline_text_when_ready()
                } else {
                    socket.next_text_when_ready()
                }
                .map_err(PublicCaptureTransportError::Public)?;
                match payload {
                    Some(payload) => Ok(PublicCaptureCompletion::StreamFrame {
                        stream,
                        received_at_ms,
                        payload,
                    }),
                    None => Ok(PublicCaptureCompletion::StreamReady { stream }),
                }
            }
        }
    }
}

impl PublicCaptureEffectExecutor for BinancePublicCaptureTransport {
    fn execute_effect(
        &mut self,
        effect: PublicCaptureEffect,
        received_at_ms: u64,
    ) -> Result<PublicCaptureCompletion, PublicCaptureTransportError> {
        self.execute(effect, received_at_ms)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PublicCaptureTransportError {
    #[error("Binance public transport failed: {0}")]
    Public(PublicError),
    #[error("public stream was read before its connect completion")]
    NotConnected,
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn recovered_constructor_rejects_a_ready_session() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let symbol: Symbol = "BTC/USDT".parse()?;
        let recorder = crate::market::RawMarketRecorder::open(directory.path().join("raw.jsonl"))?;
        let mut session = MarketSession::new(symbol.clone(), recorder);
        session.ingest_snapshot(
            1,
            r#"{"lastUpdateId":10,"bids":[["100.0","1.0"]],"asks":[["101.0","1.0"]]}"#.to_owned(),
        )?;
        let source = ScalpingPublicMarketSource::new(
            symbol,
            "scalping-shadow-v1",
            "0".repeat(64),
            65_000,
            NonZeroUsize::new(2_048).ok_or("history")?,
        )?;
        assert!(matches!(
            ScalpingPublicMarketWorker::new_recovered(session, source),
            Err(PublicCaptureWorkerError::RecoveredSourceNotFresh)
        ));
        Ok(())
    }
}
