//! Scalping consumes only the feature source's normalized frames and durably checkpoints its pure
//! reducer. This module deliberately does not turn a reference price into an order: current Node
//! lacks signed per-instance safety and exchange StopMarket protection, so every entry is blocked.

use std::{
    num::NonZeroUsize,
    time::{SystemTime, UNIX_EPOCH},
};

use venue_domain::domain::MarketEvent;
use venue_runtime::{
    AccountPhysicalGateway, StrategyBinding, StrategyKind, account::InstanceLifecycle,
    strategy::AccountMarketEvent,
};
use venue_strategies::scalping::{
    LifecycleAuthorization, ProtectionState, SafetyProjection, ScalpingCheckpoint, ScalpingParams,
    ScalpingStrategy, StrategyBinding as ScalpingStrategyBinding,
};

use super::{NodeError, ProductionResident, persist_anchor, resident_error};

mod full_snapshot_book;
mod trade_window;

#[derive(Clone, Copy)]
enum BookFeed {
    SequencedDelta,
    CompleteWebSocketImages,
}

#[cfg(feature = "bitget")]
pub(crate) struct BitgetScalpingBookBridge {
    sequencer: venue_gateway_bitget::public::BitgetBookSequencer,
    pending_snapshot: Option<venue_gateway_bitget::public::BitgetBooksMessage>,
    session_generation: Option<u64>,
}

#[cfg(feature = "bitget")]
impl BitgetScalpingBookBridge {
    fn new() -> Self {
        Self {
            sequencer: venue_gateway_bitget::public::BitgetBookSequencer::new(),
            pending_snapshot: None,
            session_generation: None,
        }
    }

    /// Bitget's snapshot alone is deliberately withheld.  The first update must prove it covers
    /// the snapshot before either record can enter MarketHub; a REST BBO cannot take this path.
    fn accept(
        &mut self,
        message: venue_gateway_bitget::public::BitgetBooksMessage,
    ) -> Result<Vec<(u64, MarketEvent)>, NodeError> {
        use venue_gateway_bitget::public::BitgetBookSequenceStatus;

        match self.session_generation {
            None => {
                self.sequencer
                    .reset_generation(message.raw.generation)
                    .map_err(|_| NodeError::ResidentRuntime)?;
                self.session_generation = Some(message.raw.generation);
            }
            Some(generation) if generation != message.raw.generation => {
                return Err(NodeError::ResidentRuntime);
            }
            Some(_) => {}
        }
        let status = self
            .sequencer
            .accept(&message)
            .map_err(|_| NodeError::ResidentRuntime)?;
        match status {
            BitgetBookSequenceStatus::Snapshot { .. } => {
                self.pending_snapshot = Some(message);
                Ok(Vec::new())
            }
            BitgetBookSequenceStatus::Bridged { generation, .. } => {
                let snapshot = self
                    .pending_snapshot
                    .take()
                    .ok_or(NodeError::ResidentRuntime)?;
                let snapshot_event = snapshot
                    .normalize(generation)
                    .map_err(|_| NodeError::ResidentRuntime)?;
                let delta_event = message
                    .normalize(generation)
                    .map_err(|_| NodeError::ResidentRuntime)?;
                Ok(vec![
                    (snapshot.raw.received_at_ms, snapshot_event),
                    (message.raw.received_at_ms, delta_event),
                ])
            }
            BitgetBookSequenceStatus::Updated { generation, .. } => Ok(vec![(
                message.raw.received_at_ms,
                message
                    .normalize(generation)
                    .map_err(|_| NodeError::ResidentRuntime)?,
            )]),
            BitgetBookSequenceStatus::ResetRequired { .. } => {
                self.pending_snapshot = None;
                Ok(Vec::new())
            }
        }
    }
}

#[cfg(feature = "gate")]
pub(crate) struct GateScalpingBookBridge {
    bridge: venue_gateway_gate::GateOrderBookBridge,
    pending_snapshot:
        Option<venue_gateway_gate::GatePublicRecord<venue_domain::domain::MarketSnapshot>>,
}

#[cfg(feature = "gate")]
impl GateScalpingBookBridge {
    fn new(bridge: venue_gateway_gate::GateOrderBookBridge) -> Self {
        Self {
            bridge,
            pending_snapshot: None,
        }
    }

    /// `GateOrderBookBridge` deliberately labels a replacement snapshot as not-ready until a
    /// delta covers it. Keep that snapshot local until the bridge itself reaches Ready.
    fn receive_snapshot(
        &mut self,
        snapshot: venue_gateway_gate::GatePublicRecord<venue_domain::domain::MarketSnapshot>,
    ) -> Result<Vec<(u64, MarketEvent)>, NodeError> {
        let actions = self
            .bridge
            .receive_snapshot(snapshot)
            .map_err(|_| NodeError::ResidentRuntime)?;
        self.events_from_actions(actions)
    }

    fn receive_delta(
        &mut self,
        delta: venue_gateway_gate::GatePublicRecord<venue_gateway_gate::GateBookDelta>,
    ) -> Result<Vec<(u64, MarketEvent)>, NodeError> {
        let actions = self
            .bridge
            .receive_delta(delta)
            .map_err(|_| NodeError::ResidentRuntime)?;
        self.events_from_actions(actions)
    }

    fn events_from_actions(
        &mut self,
        actions: Vec<venue_gateway_gate::GateBookBridgeAction>,
    ) -> Result<Vec<(u64, MarketEvent)>, NodeError> {
        use venue_gateway_gate::GateBookBridgeAction;

        let ready = self.bridge.is_ready();
        let mut events = Vec::new();
        for action in actions {
            match action {
                GateBookBridgeAction::Buffered | GateBookBridgeAction::IgnoredStale => {}
                GateBookBridgeAction::ReplaceSnapshot(snapshot) if ready => {
                    events.push((
                        snapshot.freshness.received_at_ms,
                        MarketEvent::Snapshot(snapshot.value),
                    ));
                }
                GateBookBridgeAction::ReplaceSnapshot(snapshot) => {
                    self.pending_snapshot = Some(snapshot);
                }
                GateBookBridgeAction::ApplyDelta(delta) => {
                    if let Some(snapshot) = self.pending_snapshot.take() {
                        events.push((
                            snapshot.freshness.received_at_ms,
                            MarketEvent::Snapshot(snapshot.value),
                        ));
                    }
                    if !ready {
                        return Err(NodeError::ResidentRuntime);
                    }
                    events.push((
                        delta.freshness.received_at_ms,
                        MarketEvent::Delta(delta.value),
                    ));
                }
            }
        }
        Ok(events)
    }
}

/// Node-owned wrapper around the pure reducer. Physical orders, private facts and execution
/// authority remain in Runtime/Host; this state is only the reducer's validated checkpoint.
pub(crate) struct ScalpingBridgeState {
    engine: ScalpingStrategy,
    trade_window: trade_window::ObservedTradeWindow,
}

impl ScalpingBridgeState {
    fn restore_or_bootstrap(
        checkpoint: Option<Vec<u8>>,
        binding: ScalpingStrategyBinding,
        params: ScalpingParams,
    ) -> Result<Self, NodeError> {
        let engine = match checkpoint {
            Some(bytes) => {
                let checkpoint = serde_json::from_slice::<ScalpingCheckpoint>(&bytes)
                    .map_err(|_| NodeError::ResidentRuntime)?;
                ScalpingStrategy::restore(binding, params.clone(), checkpoint)
                    .map_err(|_| NodeError::ResidentRuntime)?
            }
            None => ScalpingStrategy::new(binding, params.clone())
                .map_err(|_| NodeError::ResidentRuntime)?,
        };
        Ok(Self {
            engine,
            trade_window: Default::default(),
        })
    }

    fn checkpoint_bytes(&self) -> Result<Vec<u8>, NodeError> {
        serde_json::to_vec(&self.engine.checkpoint()).map_err(|_| NodeError::ResidentRuntime)
    }

    fn binding(&self) -> &ScalpingStrategyBinding {
        self.engine.binding()
    }
}

/// Runtime lifecycle is only one input to the pure reducer. It cannot imply that private signed
/// safety and exchange protection are present, so the latter are projected independently below.
struct RuntimeScalpingAuthorization {
    binding: ScalpingStrategyBinding,
    running: bool,
}

impl LifecycleAuthorization for RuntimeScalpingAuthorization {
    fn is_allowed(&self) -> bool {
        self.running
    }

    fn matches_at(&self, binding: &ScalpingStrategyBinding, _decision_at_ms: u64) -> bool {
        binding == &self.binding
    }

    fn revision(&self) -> u64 {
        1
    }

    fn authority_generation(&self) -> u64 {
        1
    }
}

impl<G: AccountPhysicalGateway> ProductionResident<G> {
    /// Registers the Runtime actor and restores the exact pure reducer checkpoint it owns. A
    /// generic Runtime binding cannot select a parameter release or substitute checkpoint state.
    pub fn register_scalping_actor(
        &mut self,
        binding: StrategyBinding,
        scalping_binding: ScalpingStrategyBinding,
    ) -> Result<(), NodeError> {
        if binding.key.strategy_kind != StrategyKind::Scalping
            || self.scalping_bindings.contains_key(&binding.key)
            || self.scalping_bridges.contains_key(&binding.key)
            || !scalping_binding_matches_runtime(&scalping_binding, &binding)
        {
            return Err(NodeError::ResidentRuntime);
        }
        let params = ScalpingParams::for_binding(&scalping_binding);
        params
            .validate_for(&scalping_binding)
            .map_err(|_| NodeError::ResidentRuntime)?;
        self.register_actor(binding.clone())?;
        let checkpoint = self
            .runtime
            .resident_actor_checkpoint(&binding)
            .map_err(resident_error)?;
        let bridge = ScalpingBridgeState::restore_or_bootstrap(
            checkpoint,
            scalping_binding,
            params.clone(),
        )?;
        if self
            .scalping_bindings
            .insert(binding.key.clone(), binding.clone())
            .is_some()
        {
            return Err(NodeError::ResidentRuntime);
        }
        self.scalping_bridges.insert(binding.key.clone(), bridge);
        self.scalping_books
            .insert(binding.key.clone(), venue_indicators::OrderBook::default());
        self.scalping_features
            .insert(binding.key.clone(), feature_source(&binding, &params)?);
        Ok(())
    }

    /// Feeds one normalized public event into Runtime's account-local MarketHub and the same
    /// strategy mailbox used by every resident.  The caller cannot inject a BBO or bypass the
    /// hub's generation, sequence, and crossed-book checks.
    pub fn publish_scalping_market(
        &mut self,
        binding: &StrategyBinding,
        event: AccountMarketEvent,
    ) -> Result<bool, NodeError> {
        if binding.key.strategy_kind != StrategyKind::Scalping
            || binding.key.account != *self.runtime.account()
            || event.symbol() != &binding.key.symbol
        {
            return Err(NodeError::ResidentRuntime);
        }
        if !self.prepare_scalping_book(binding, &event.event)? {
            return Ok(false);
        }
        self.runtime.publish_market(event).map_err(resident_error)
    }

    /// Until Runtime supplies signed per-instance safety and installed-protection facts, every
    /// registered Scalping reducer is intentionally blocked from entry. This is read-only status
    /// for Control projection; it neither changes lifecycle nor grants a capability.
    #[must_use]
    pub(crate) fn scalping_entry_safety_unwired(&self, binding: &StrategyBinding) -> bool {
        binding.key.strategy_kind == StrategyKind::Scalping
            && self.scalping_bindings.get(&binding.key) == Some(binding)
            && self.scalping_bridges.contains_key(&binding.key)
    }

    /// Only the fixed receiver/bridge feeds this ingress, never an unsequenced REST/BBO poll.
    #[cfg_attr(
        not(any(test, feature = "bitget", feature = "gate", feature = "okx")),
        allow(dead_code)
    )]
    pub(crate) fn publish_sequenced_scalping_book(
        &mut self,
        binding: &StrategyBinding,
        received_at_ms: u64,
        event: MarketEvent,
    ) -> Result<bool, NodeError> {
        self.publish_stream_book(binding, received_at_ms, event, BookFeed::SequencedDelta)
    }

    #[cfg_attr(
        not(any(test, feature = "bybit", feature = "hyperliquid")),
        allow(dead_code)
    )]
    pub(crate) fn publish_full_snapshot_scalping_book(
        &mut self,
        binding: &StrategyBinding,
        received_at_ms: u64,
        event: MarketEvent,
    ) -> Result<bool, NodeError> {
        if !matches!(
            binding.key.account.exchange,
            venue_gateway_api::VenueId::Bybit | venue_gateway_api::VenueId::Hyperliquid
        ) || !matches!(event, MarketEvent::Snapshot(_))
        {
            return Err(NodeError::ResidentRuntime);
        }
        self.publish_stream_book(
            binding,
            received_at_ms,
            event,
            BookFeed::CompleteWebSocketImages,
        )
    }

    fn publish_stream_book(
        &mut self,
        binding: &StrategyBinding,
        received_at_ms: u64,
        event: MarketEvent,
        feed: BookFeed,
    ) -> Result<bool, NodeError> {
        self.require_registered_scalping_binding(binding, binding.key.account.exchange)?;
        if !matches!(event, MarketEvent::Snapshot(_) | MarketEvent::Delta(_)) {
            return Err(NodeError::ResidentRuntime);
        }
        let event = AccountMarketEvent::new(received_at_ms, event)
            .map_err(|_| NodeError::ResidentRuntime)?;
        let published = self.publish_scalping_market(binding, event.clone())?;
        if published {
            self.drive_features(binding, event, feed)?;
        }
        Ok(published)
    }

    /// Non-book facts retain their own source times and identities. They share the book's
    /// canonical synchronization generation, but can neither bridge nor revive a missing book.
    #[cfg_attr(
        not(any(
            test,
            feature = "bitget",
            feature = "gate",
            feature = "bybit",
            feature = "okx",
            feature = "hyperliquid"
        )),
        allow(dead_code)
    )]
    pub(crate) fn publish_scalping_stream_fact(
        &mut self,
        binding: &StrategyBinding,
        received_at_ms: u64,
        mut event: MarketEvent,
    ) -> Result<bool, NodeError> {
        self.require_registered_scalping_binding(binding, binding.key.account.exchange)?;
        if !matches!(event, MarketEvent::Trade(_) | MarketEvent::Bar(_)) {
            return Err(NodeError::ResidentRuntime);
        }
        #[cfg(feature = "bitget")]
        if binding.key.account.exchange == venue_gateway_api::VenueId::Bitget {
            let bridge = self
                .scalping_bitget_books
                .get(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?;
            let generation = match &mut event {
                MarketEvent::Trade(trade) => &mut trade.generation,
                MarketEvent::Bar(bar) => &mut bar.generation,
                _ => return Err(NodeError::ResidentRuntime),
            };
            if bridge
                .session_generation
                .is_some_and(|session| session != *generation)
            {
                return Err(NodeError::ResidentRuntime);
            }
            let Some(ready_generation) = bridge.sequencer.ready_generation() else {
                return Ok(false);
            };
            // This is the same observed socket session, not a fact imported from a newer socket.
            // Only the existing book sequencer can advance its canonical synchronization epoch.
            *generation = ready_generation;
        }
        let feed = match binding.key.account.exchange {
            venue_gateway_api::VenueId::Bybit | venue_gateway_api::VenueId::Hyperliquid => {
                BookFeed::CompleteWebSocketImages
            }
            _ => BookFeed::SequencedDelta,
        };
        let book = self
            .scalping_books
            .get(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?;
        let generation = match &event {
            MarketEvent::Trade(trade) => trade.generation,
            MarketEvent::Bar(bar) => bar.generation,
            _ => return Err(NodeError::ResidentRuntime),
        };
        if book.generation() != Some(generation)
            || (matches!(feed, BookFeed::SequencedDelta) && !book.bridged())
        {
            return Ok(false);
        }
        if let MarketEvent::Trade(trade) = event {
            let Some(trade) = self
                .scalping_bridges
                .get_mut(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?
                .trade_window
                .accept(trade)?
            else {
                return Ok(false);
            };
            event = MarketEvent::Trade(trade);
        }
        let event = AccountMarketEvent::new(received_at_ms, event)
            .map_err(|_| NodeError::ResidentRuntime)?;
        let published = self.publish_scalping_market(binding, event.clone())?;
        if published {
            self.drive_features(binding, event, feed)?;
        }
        Ok(published)
    }
}

impl<G: AccountPhysicalGateway> ProductionResident<G> {
    /// Continuity is checked before an event can enter MarketHub or an Actor mailbox. Keeping
    /// the fenced source retains its generation floor; a same-generation snapshot cannot revive it.
    fn prepare_scalping_book(
        &mut self,
        binding: &StrategyBinding,
        event: &MarketEvent,
    ) -> Result<bool, NodeError> {
        let source = self
            .scalping_features
            .get_mut(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?;
        if source.state() == venue_indicators::FeatureState::DataGap {
            match event {
                MarketEvent::Snapshot(snapshot)
                    if source
                        .generation()
                        .is_none_or(|generation| snapshot.generation > generation) => {}
                _ => return Ok(false),
            }
        }
        let book = self
            .scalping_books
            .get_mut(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?;
        match event {
            MarketEvent::Snapshot(snapshot) => book.apply_snapshot(snapshot.clone()),
            MarketEvent::Delta(delta) => match book.apply_delta_if_fresh(delta.clone()) {
                Ok(true) => {}
                Ok(false) => return Ok(false),
                Err(_) => {
                    source.fence();
                    return Ok(false);
                }
            },
            _ => {}
        }
        Ok(true)
    }
}

#[cfg(feature = "bitget")]
impl<G: AccountPhysicalGateway> ProductionResident<G> {
    /// Installs the only Bitget public path accepted for Scalping.  The bridge accepts only the
    /// adapter's sequenced `books` WebSocket messages; REST order-book and ticker/BBO records do
    /// not have an API at this boundary.
    pub fn register_bitget_scalping_book_bridge(
        &mut self,
        binding: &StrategyBinding,
    ) -> Result<(), NodeError> {
        self.require_registered_scalping_binding(binding, venue_gateway_api::VenueId::Bitget)?;
        if self.scalping_bitget_books.contains_key(&binding.key) {
            return Err(NodeError::ResidentRuntime);
        }
        self.scalping_bitget_books
            .insert(binding.key.clone(), BitgetScalpingBookBridge::new());
        Ok(())
    }

    /// Applies one already-validated Bitget `books` WebSocket record.  A snapshot remains held
    /// until `BitgetBookSequencer` proves the first delta covers it, then both are forwarded in
    /// order through the resident's shared MarketHub.
    pub fn ingest_bitget_scalping_book(
        &mut self,
        binding: &StrategyBinding,
        message: venue_gateway_bitget::public::BitgetBooksMessage,
    ) -> Result<bool, NodeError> {
        self.require_registered_scalping_binding(binding, venue_gateway_api::VenueId::Bitget)?;
        let events = self
            .scalping_bitget_books
            .get_mut(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?
            .accept(message)?;
        if self
            .scalping_bitget_books
            .get(&binding.key)
            .is_none_or(|bridge| bridge.sequencer.ready_generation().is_none())
        {
            if let Some(source) = self.scalping_features.get_mut(&binding.key) {
                source.fence();
            }
        }
        let mut published = false;
        for (received_at_ms, event) in events {
            published |= self.publish_sequenced_scalping_book(binding, received_at_ms, event)?;
        }
        Ok(published)
    }
}

#[cfg(feature = "gate")]
impl<G: AccountPhysicalGateway> ProductionResident<G> {
    /// Installs one caller-owned Gate snapshot-plus-delta bridge for this registered actor.  Gate
    /// ticker/BBO data is intentionally absent: only `GateOrderBookBridge` actions may reach
    /// the MarketHub path below.
    pub fn register_gate_scalping_book_bridge(
        &mut self,
        binding: &StrategyBinding,
        bridge: venue_gateway_gate::GateOrderBookBridge,
    ) -> Result<(), NodeError> {
        self.require_registered_scalping_binding(binding, venue_gateway_api::VenueId::Gate)?;
        if self.scalping_gate_books.contains_key(&binding.key) {
            return Err(NodeError::ResidentRuntime);
        }
        self.scalping_gate_books
            .insert(binding.key.clone(), GateScalpingBookBridge::new(bridge));
        Ok(())
    }

    /// Applies a validated Gate REST depth snapshot only through the existing Gate bridge.  The
    /// bridge does not become ready until a matching WebSocket delta covers its sequence.
    pub fn ingest_gate_scalping_snapshot(
        &mut self,
        binding: &StrategyBinding,
        snapshot: venue_gateway_gate::GatePublicRecord<venue_domain::domain::MarketSnapshot>,
    ) -> Result<bool, NodeError> {
        self.require_registered_scalping_binding(binding, venue_gateway_api::VenueId::Gate)?;
        let events = self
            .scalping_gate_books
            .get_mut(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?
            .receive_snapshot(snapshot)?;
        self.publish_gate_scalping_events(binding, events)
    }

    /// Applies a validated Gate order-book WebSocket delta through the existing bridge.  Sequence
    /// gaps remain fenced in the adapter bridge and publish no speculative replacement.
    pub fn ingest_gate_scalping_delta(
        &mut self,
        binding: &StrategyBinding,
        delta: venue_gateway_gate::GatePublicRecord<venue_gateway_gate::GateBookDelta>,
    ) -> Result<bool, NodeError> {
        self.require_registered_scalping_binding(binding, venue_gateway_api::VenueId::Gate)?;
        let events = self
            .scalping_gate_books
            .get_mut(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?
            .receive_delta(delta)?;
        self.publish_gate_scalping_events(binding, events)
    }

    fn publish_gate_scalping_events(
        &mut self,
        binding: &StrategyBinding,
        events: Vec<(u64, MarketEvent)>,
    ) -> Result<bool, NodeError> {
        let mut published = false;
        for (received_at_ms, event) in events {
            published |= self.publish_sequenced_scalping_book(binding, received_at_ms, event)?;
        }
        Ok(published)
    }
}
impl<G: AccountPhysicalGateway> ProductionResident<G> {
    #[cfg_attr(not(any(feature = "bitget", feature = "gate")), allow(dead_code))]
    fn require_registered_scalping_binding(
        &self,
        binding: &StrategyBinding,
        venue: venue_gateway_api::VenueId,
    ) -> Result<(), NodeError> {
        if self.host.binding().venue != venue
            || binding.key.strategy_kind != StrategyKind::Scalping
            || self.scalping_bindings.get(&binding.key) != Some(binding)
        {
            return Err(NodeError::ResidentRuntime);
        }
        Ok(())
    }
}

#[cfg(feature = "binance")]
impl ProductionResident<venue_gateway_binance::BinanceAccountGateway> {
    /// Reads one adapter-normalized public Binance event and publishes it through the shared
    /// MarketHub. The socket is acquired only when this account has an explicit Scalping actor;
    /// it never creates a candidate or an execution command by itself.
    pub(crate) fn poll_binance_scalping_public_once(&mut self) -> Result<bool, NodeError> {
        let binding = match self.scalping_bindings.values().next().cloned() {
            Some(value) if self.scalping_bindings.len() == 1 => value,
            Some(_) => return Err(NodeError::ResidentRuntime),
            None => return Ok(false),
        };
        let public = self
            .host
            .with_gateway_read(|gateway| gateway.poll_public_market())
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: error.to_string(),
            })?;
        let Some(public) = public else {
            return Ok(false);
        };
        let (received_at_ms, event) = match public {
            venue_gateway_binance::BinancePublicMarketEvent::DepthSnapshot(value) => {
                (received_ms()?, MarketEvent::Snapshot(value))
            }
            venue_gateway_binance::BinancePublicMarketEvent::Bbo(value) => {
                (value.received_at_ms, MarketEvent::Ticker(value))
            }
            venue_gateway_binance::BinancePublicMarketEvent::Depth(value) => {
                // Deltas deliberately retain their transport-independent exchange sequence; the
                // adapter has already rejected wrong-symbol and malformed public frames.
                (received_ms()?, MarketEvent::Delta(value))
            }
            venue_gateway_binance::BinancePublicMarketEvent::Trade(value) => {
                (value.received_at_ms, MarketEvent::Trade(value))
            }
            venue_gateway_binance::BinancePublicMarketEvent::ClosedBar(value) => {
                (value.received_at_ms, MarketEvent::Bar(value))
            }
        };
        let event = AccountMarketEvent::new(received_at_ms, event)
            .map_err(|_| NodeError::ResidentRuntime)?;
        let published = self.publish_scalping_market(&binding, event.clone())?;
        if published {
            self.drive_features(&binding, event, BookFeed::SequencedDelta)?;
        }
        Ok(published)
    }
}

impl<G: AccountPhysicalGateway> ProductionResident<G> {
    #[cfg_attr(
        not(any(feature = "binance", feature = "bitget", feature = "gate")),
        allow(dead_code)
    )]
    fn drive_features(
        &mut self,
        binding: &StrategyBinding,
        event: AccountMarketEvent,
        feed: BookFeed,
    ) -> Result<(), NodeError> {
        let output = {
            let (scalping_books, scalping_features, scalping_capture_sequence) = (
                &mut self.scalping_books,
                &mut self.scalping_features,
                &mut self.scalping_capture_sequence,
            );
            let book = scalping_books
                .get_mut(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?;
            // Each feature source owns a capture cursor; other symbols must not look like gaps.
            let capture_sequence = scalping_capture_sequence
                .entry(binding.key.clone())
                .or_insert(0);
            *capture_sequence = capture_sequence
                .checked_add(1)
                .ok_or(NodeError::ResidentRuntime)?;
            let source = scalping_features
                .get_mut(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?;
            let input = venue_indicators::RecordedPublicEvent {
                capture_sequence: *capture_sequence,
                received_at_ms: event.received_at_ms,
                event: event.event,
            };
            let current_ms = received_ms()?;
            match feed {
                BookFeed::SequencedDelta => source.consume(input, book, current_ms),
                BookFeed::CompleteWebSocketImages => source.consume(
                    input,
                    &full_snapshot_book::FullSnapshotBook(book),
                    current_ms,
                ),
            }
            .map_err(|_| NodeError::ResidentRuntime)?
        };
        if let Some(frame) = output.frame {
            self.evaluate_scalping_frame(binding, frame)?;
        }
        Ok(())
    }

    fn evaluate_scalping_frame(
        &mut self,
        binding: &StrategyBinding,
        frame: venue_indicators::FeatureFrame,
    ) -> Result<(), NodeError> {
        let pure_binding = self
            .scalping_bridges
            .get(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?
            .binding()
            .clone();
        let authorization = RuntimeScalpingAuthorization {
            binding: pure_binding,
            running: self.strategy_lifecycle(binding) == Some(InstanceLifecycle::Running),
        };
        // There is no current Node projection proving this instance is flat, has no competing
        // owner, or has a server-side StopMarket. `Running` therefore cannot become an entry
        // authorization: the pure reducer records this frame but blocks before preparation.
        let safety = SafetyProjection {
            private_snapshot_ready: false,
            exposure: venue_strategies::scalping::ExposureState::Unknown,
            execution_unknown: self.has_unresolved(),
            protection: ProtectionState::Gap,
            owner_conflict: true,
            risk_budget_available: false,
        };
        let replay = {
            let bridge = self
                .scalping_bridges
                .get_mut(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?;
            let _decision = bridge
                .engine
                .evaluate(&frame, &safety, &authorization)
                .map_err(|_| NodeError::ResidentRuntime)?;
            bridge.checkpoint_bytes()?
        };
        let applied = self
            .runtime
            .persist_resident_semantic_turn(binding, replay)
            .map_err(resident_error)?;
        persist_anchor(&self.artifacts_root, binding, &applied)?;
        Ok(())
    }
}

#[cfg_attr(
    not(any(feature = "binance", feature = "bitget", feature = "gate")),
    allow(dead_code)
)]
fn received_ms() -> Result<u64, NodeError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| NodeError::ResidentRuntime)?
        .as_millis();
    u64::try_from(millis).map_err(|_| NodeError::ResidentRuntime)
}

fn feature_source(
    binding: &StrategyBinding,
    params: &ScalpingParams,
) -> Result<venue_indicators::ScalpingPublicMarketSource, NodeError> {
    venue_indicators::ScalpingPublicMarketSource::new(
        binding.key.symbol.clone(),
        params.feature_profile.clone(),
        params.feature_digest.clone(),
        params.max_data_age_ms,
        NonZeroUsize::new(256).ok_or(NodeError::ResidentRuntime)?,
    )
    .map_err(|_| NodeError::ResidentRuntime)
}

fn scalping_binding_matches_runtime(
    scalping: &ScalpingStrategyBinding,
    runtime: &StrategyBinding,
) -> bool {
    scalping.strategy_instance_id == runtime.key.instance_id
        && scalping.run_id == runtime.run_id
        && scalping.exchange == runtime.key.account.exchange.as_str()
        && scalping.account == runtime.key.account.account
        && scalping.symbol == runtime.key.symbol
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::{Arc, Mutex},
        time::{SystemTime, UNIX_EPOCH},
    };

    use rust_decimal::Decimal;
    use venue_domain::domain::{
        AggressorSide, Amount, Asset, ExecutionCommand, FieldState, MarketDelta, OrderCommand,
        PositionSide, Price, PublicBar, PublicTrade, Symbol,
    };
    use venue_gateway_api::{GatewayBinding, VenueId};
    use venue_runtime::{
        AccountDispatchPermit, AccountGatewayResult, AccountHostValidationError,
        AccountLimitNormalizationIntent, AccountPhysicalGateway, AccountRecoveryReport,
        AccountRecoveryRequest, AccountRiskEvidence, SignedAccountBalance,
        SignedAccountPositionMode, SignedAccountSnapshot,
    };

    use super::*;
    use crate::NodeLaunch;

    const ACCOUNT: &str = "00000000-0000-4000-8000-000000000001";

    #[derive(Default)]
    struct State {
        dispatches: usize,
        generation: u64,
        commands: Vec<ExecutionCommand>,
    }

    struct Gateway {
        binding: GatewayBinding,
        state: Arc<Mutex<State>>,
    }

    impl AccountPhysicalGateway for Gateway {
        type Error = io::Error;

        fn binding(&self) -> &GatewayBinding {
            &self.binding
        }

        fn reconcile(
            &mut self,
            _request: &AccountRecoveryRequest,
        ) -> Result<AccountRecoveryReport, Self::Error> {
            AccountRecoveryReport::new(
                self.binding.clone(),
                now().map_err(io::Error::other)?,
                Vec::new(),
            )
            .map_err(io::Error::other)
        }

        fn risk_evidence(&mut self) -> Result<AccountRiskEvidence, AccountHostValidationError> {
            AccountRiskEvidence::complete(
                self.binding.clone(),
                now().map_err(|_| AccountHostValidationError::RiskEvidence)?,
                1,
                Vec::new(),
                Vec::new(),
            )
        }

        fn normalize_limit_intent(
            &mut self,
            intent: &AccountLimitNormalizationIntent,
        ) -> Result<ExecutionCommand, AccountHostValidationError> {
            Ok(ExecutionCommand::PlaceLimit(OrderCommand {
                time_in_force: Default::default(),
                command_id: intent.command_id.clone(),
                client_order_id: intent.client_order_id.clone(),
                owner: intent.owner.clone(),
                side: intent.side,
                position_side: intent.position_side,
                quantity: intent.quote_delta,
                limit_price: Price::new(Decimal::ONE)
                    .map_err(|_| AccountHostValidationError::Command)?,
                reduce_only: intent.reduce_only,
            }))
        }

        fn signed_account_snapshot(
            &mut self,
            request: &AccountRecoveryRequest,
        ) -> Result<SignedAccountSnapshot, AccountHostValidationError> {
            let mut state = self
                .state
                .lock()
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            state.generation = state.generation.saturating_add(1);
            SignedAccountSnapshot::complete_with_fills(
                self.binding.clone(),
                now().map_err(|_| AccountHostValidationError::SignedSnapshot)?,
                1,
                state.generation,
                1,
                SignedAccountPositionMode::Hedge,
                Vec::new(),
                vec![
                    venue_runtime::SignedAccountPositionFact {
                        symbol: self.binding.symbol.clone(),
                        position_side: PositionSide::Long,
                        quantity: Decimal::ZERO,
                        entry_price: None,
                        mark_price: Some(Decimal::ONE),
                    },
                    venue_runtime::SignedAccountPositionFact {
                        symbol: self.binding.symbol.clone(),
                        position_side: PositionSide::Short,
                        quantity: Decimal::ZERO,
                        entry_price: None,
                        mark_price: Some(Decimal::ONE),
                    },
                ],
                Vec::new(),
                format!("cursor:{}", state.generation),
                request
                    .unresolved()
                    .iter()
                    .map(|command| venue_runtime::SignedUnknownFact {
                        command_id: command.command_id().clone(),
                        result: venue_runtime::SignedUnknownResult::Unknown,
                    })
                    .collect(),
            )?
            .with_balances(vec![SignedAccountBalance {
                asset: Asset::new("USDT")
                    .map_err(|_| AccountHostValidationError::SignedSnapshot)?,
                equity: Decimal::new(100, 0),
                available_margin: Some(Decimal::new(100, 0)),
            }])
        }

        fn dispatch(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
            let Ok(mut state) = self.state.lock() else {
                return AccountGatewayResult::Unknown;
            };
            state.dispatches = state.dispatches.saturating_add(1);
            state.commands.push(permit.command().clone());
            AccountGatewayResult::Accepted {
                venue_order_id: "scalp-native-1".to_owned(),
            }
        }
    }

    fn now() -> Result<u64, &'static str> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "clock")?
            .as_millis()
            .try_into()
            .map_err(|_| "clock")
    }

    #[allow(clippy::type_complexity)]
    fn setup(
        root: &std::path::Path,
    ) -> Result<
        (
            ProductionResident<Gateway>,
            Arc<Mutex<State>>,
            StrategyBinding,
        ),
        Box<dyn std::error::Error>,
    > {
        setup_for(root, VenueId::Okx)
    }

    #[allow(clippy::type_complexity)]
    fn setup_for(
        root: &std::path::Path,
        venue: VenueId,
    ) -> Result<
        (
            ProductionResident<Gateway>,
            Arc<Mutex<State>>,
            StrategyBinding,
        ),
        Box<dyn std::error::Error>,
    > {
        let launch = NodeLaunch::try_parse_from(
            venue,
            [
                "venue-node-okx",
                "--mode",
                "LIVE",
                "--trading-account-id",
                ACCOUNT,
                "--symbol",
                "DOGE/USDT",
                "--artifacts-base",
                root.to_str().ok_or("non-utf8 root")?,
            ],
        )?;
        let state = Arc::new(Mutex::new(State::default()));
        let gateway = Gateway {
            binding: launch.binding().clone(),
            state: state.clone(),
        };
        let mut resident = ProductionResident::open(&launch, gateway)?;
        let account = venue_runtime::AccountKey::new(venue, ACCOUNT)?;
        let binding = StrategyBinding::new(
            venue_runtime::StrategyInstanceKey::new(
                account,
                StrategyKind::Scalping,
                "scalp-1",
                Symbol::new("DOGE", "USDT")?,
            )?,
            "run-1",
            "scalp-config",
        )?;
        resident.register_scalping_actor(binding.clone(), scalping_binding(&binding)?)?;
        Ok((resident, state, binding))
    }

    fn scalping_binding(
        binding: &StrategyBinding,
    ) -> Result<ScalpingStrategyBinding, Box<dyn std::error::Error>> {
        Ok(ScalpingStrategyBinding {
            strategy_kind: venue_strategies::scalping::StrategyKind::Scalping,
            strategy_instance_id: binding.key.instance_id.clone(),
            run_id: binding.run_id.clone(),
            exchange: binding.key.account.exchange.as_str().to_owned(),
            account: binding.key.account.account.clone(),
            symbol: binding.key.symbol.clone(),
            parameter_release_id: "scalping-shadow-v1".to_owned(),
            owner_scope: binding.key.instance_id.clone(),
            risk_budget: Amount::new(Asset::new("USDT")?, Decimal::TEN),
        })
    }

    #[test]
    fn registered_scalping_is_writer_inert_without_a_safety_projection()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let (resident, state, binding) = setup(directory.path())?;
        assert!(resident.scalping_entry_safety_unwired(&binding));
        let state = state.lock().map_err(|_| "state lock")?;
        assert_eq!(state.dispatches, 0);
        assert!(state.commands.is_empty());
        Ok(())
    }

    #[test]
    fn malformed_or_other_release_checkpoint_cannot_bootstrap_scalping()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let (_, _, binding) = setup(directory.path())?;
        let pure_binding = scalping_binding(&binding)?;
        let params = ScalpingParams::for_binding(&pure_binding);
        assert!(
            ScalpingBridgeState::restore_or_bootstrap(
                Some(br#"not-a-checkpoint"#.to_vec()),
                pure_binding.clone(),
                params.clone(),
            )
            .is_err()
        );
        let other_binding = ScalpingStrategyBinding {
            parameter_release_id: "other-release".to_owned(),
            ..pure_binding.clone()
        };
        assert!(
            ScalpingBridgeState::restore_or_bootstrap(
                Some(
                    ScalpingBridgeState::restore_or_bootstrap(None, pure_binding, params)?
                        .checkpoint_bytes()?
                ),
                other_binding.clone(),
                ScalpingParams::for_binding(&other_binding),
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn actual_feature_source_frame_blocks_and_durably_checkpoints_the_reducer()
    -> Result<(), Box<dyn std::error::Error>> {
        feature_checkpoint_with_ordering(false)
    }

    #[test]
    fn observed_sparse_trade_session_reaches_the_same_safe_durable_reducer()
    -> Result<(), Box<dyn std::error::Error>> {
        feature_checkpoint_with_ordering(true)
    }

    fn feature_checkpoint_with_ordering(session: bool) -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let (mut resident, state, binding) = setup(directory.path())?;
        let now_ms = now()?;
        let publish = |resident: &mut ProductionResident<Gateway>, event: MarketEvent| {
            let event =
                AccountMarketEvent::new(now_ms, event).map_err(|_| NodeError::ResidentRuntime)?;
            assert!(resident.publish_scalping_market(&binding, event.clone())?);
            resident.drive_features(&binding, event, BookFeed::SequencedDelta)
        };
        publish(
            &mut resident,
            stream_image(binding.key.symbol.clone(), 10, now_ms)?,
        )?;
        publish(
            &mut resident,
            MarketEvent::Delta(MarketDelta {
                symbol: binding.key.symbol.clone(),
                generation: 1,
                first_sequence: 11,
                previous_sequence: Some(10),
                sequence: 11,
                exchange_time_ms: Some(now_ms),
                bids: Vec::new(),
                asks: Vec::new(),
            }),
        )?;
        publish(
            &mut resident,
            MarketEvent::Delta(MarketDelta {
                symbol: binding.key.symbol.clone(),
                generation: 1,
                first_sequence: 12,
                previous_sequence: Some(11),
                sequence: 12,
                exchange_time_ms: Some(now_ms),
                bids: Vec::new(),
                asks: Vec::new(),
            }),
        )?;
        for sequence in 1..=21_u64 {
            let close_time_ms = now_ms.saturating_sub((21_u64.saturating_sub(sequence)) * 60_000);
            publish(
                &mut resident,
                MarketEvent::Bar(PublicBar {
                    symbol: binding.key.symbol.clone(),
                    generation: 1,
                    received_at_ms: now_ms,
                    sequence,
                    open_time_ms: close_time_ms.saturating_sub(59_999),
                    close_time_ms,
                    interval_ms: 60_000,
                    open: Price::new(Decimal::ONE)?,
                    high: Price::new(Decimal::new(101, 2))?,
                    low: Price::new(Decimal::new(99, 2))?,
                    close: Price::new(Decimal::ONE)?,
                    base_volume: FieldState::Known(Decimal::TEN),
                    quote_volume: FieldState::Known(Decimal::TEN),
                    trade_count: FieldState::Known(10),
                    taker_buy_base_volume: FieldState::Known(Decimal::ONE),
                    taker_buy_quote_volume: FieldState::Known(Decimal::ONE),
                }),
            )?;
        }
        for aggregate_trade_id in 1..=64_u64 {
            let trade = MarketEvent::Trade(PublicTrade {
                symbol: binding.key.symbol.clone(),
                generation: 1,
                received_at_ms: now_ms,
                exchange_time_ms: now_ms,
                transaction_time_ms: now_ms,
                aggregate_trade_id: (aggregate_trade_id * 100).into(),
                first_trade_id: None,
                last_trade_id: None,
                ordering: venue_domain::PublicTradeOrdering::Unsequenced,
                price: Price::new(Decimal::ONE)?,
                quantity: Decimal::ONE,
                quote_quantity: Decimal::ONE,
                aggressor: FieldState::Known(AggressorSide::Buy),
            });
            if session {
                assert!(resident.publish_scalping_stream_fact(&binding, now_ms, trade.clone())?);
                // A replay does not consume a session cursor or checkpoint another reducer turn.
                let before = resident.runtime.resident_actor_checkpoint(&binding)?;
                assert!(!resident.publish_scalping_stream_fact(&binding, now_ms, trade)?);
                assert_eq!(
                    resident.runtime.resident_actor_checkpoint(&binding)?,
                    before
                );
            } else if let MarketEvent::Trade(mut trade) = trade {
                trade.aggregate_trade_id = aggregate_trade_id.into();
                trade.first_trade_id = Some(aggregate_trade_id);
                trade.last_trade_id = Some(aggregate_trade_id);
                trade.ordering = venue_domain::PublicTradeOrdering::NativeAggregateId;
                publish(&mut resident, MarketEvent::Trade(trade))?;
            }
        }
        let checkpoint = resident
            .runtime
            .resident_actor_checkpoint(&binding)?
            .ok_or("missing scalping checkpoint")?;
        let checkpoint = serde_json::from_slice::<ScalpingCheckpoint>(&checkpoint)?;
        assert_eq!(
            checkpoint
                .cursors
                .get("trades")
                .map(|cursor| cursor.sequence),
            Some(64)
        );
        assert!(matches!(
            resident
                .scalping_bridges
                .get(&binding.key)
                .ok_or("missing bridge")?
                .engine
                .state(),
            venue_strategies::scalping::ScalpingState::Blocked(
                venue_strategies::scalping::BlockingReason::PrivateSnapshot
            )
        ));
        assert_eq!(state.lock().map_err(|_| "state lock")?.dispatches, 0);

        drop(resident);
        let (restarted, restarted_state, restarted_binding) = setup(directory.path())?;
        assert_eq!(restarted_binding, binding);
        assert!(matches!(
            restarted
                .scalping_bridges
                .get(&binding.key)
                .ok_or("missing restored bridge")?
                .engine
                .state(),
            venue_strategies::scalping::ScalpingState::Bootstrapping
        ));
        assert_eq!(
            restarted_state.lock().map_err(|_| "state lock")?.dispatches,
            0
        );
        Ok(())
    }

    #[cfg(feature = "bitget")]
    #[test]
    fn bitget_scalping_requires_a_websocket_snapshot_and_covering_delta()
    -> Result<(), Box<dyn std::error::Error>> {
        use venue_gateway_bitget::public::{
            BitgetPublicSource, BitgetRawPublicPayload, parse_books_message,
        };

        fn raw(payload: String) -> Result<BitgetRawPublicPayload, Box<dyn std::error::Error>> {
            Ok(BitgetRawPublicPayload::new(
                BitgetPublicSource::WebSocketBooks,
                "DOGE/USDT".parse()?,
                1_700,
                1_000,
                payload,
            )?)
        }
        let snapshot = parse_books_message(raw(
            r#"{"arg":{"instType":"usdt-futures","topic":"books","symbol":"DOGEUSDT"},"action":"snapshot","ts":"1001","data":[{"a":[["0.101","20"]],"b":[["0.100","10"]],"pseq":"0","seq":"100","maxdepth":"50","ts":"1000"}]}"#.to_owned(),
        )?)?;
        let update = parse_books_message(raw(
            r#"{"arg":{"instType":"usdt-futures","topic":"books","symbol":"DOGEUSDT"},"action":"update","ts":"1002","data":[{"a":[["0.102","20"]],"b":[],"pseq":"99","seq":"101","maxdepth":"50","ts":"1002"}]}"#.to_owned(),
        )?)?;
        let mut bridge = BitgetScalpingBookBridge::new();
        assert!(bridge.accept(snapshot.clone())?.is_empty());
        let events = bridge.accept(update.clone())?;
        assert!(matches!(
            events.as_slice(),
            [(_, MarketEvent::Snapshot(_)), (_, MarketEvent::Delta(_))]
        ));
        assert_eq!(bridge.sequencer.ready_generation(), Some(1_700));
        assert_eq!(bridge.session_generation, Some(1_700));
        assert!(bridge.accept(snapshot)?.is_empty());
        assert!(bridge.sequencer.ready_generation().is_none());
        let next = bridge.accept(update.clone())?;
        assert!(matches!(&next[0].1, MarketEvent::Snapshot(value) if value.generation == 1_701));
        assert_eq!(bridge.session_generation, Some(1_700));
        let mut other_session = update;
        other_session.raw.generation = 1_702;
        assert!(bridge.accept(other_session).is_err());
        Ok(())
    }

    #[cfg(feature = "gate")]
    #[test]
    fn gate_scalping_holds_the_rest_snapshot_until_a_websocket_delta_bridges_it()
    -> Result<(), Box<dyn std::error::Error>> {
        use venue_gateway_gate::{
            GateOrderBookBridge, GatePublicBinding, GatePublicPayloadKind, GatePublicRawPayload,
            parse_rest_snapshot, parse_ws_delta,
        };

        let public = GatePublicBinding::new("DOGE/USDT".parse()?, "DOGE_USDT", Decimal::ONE)?;
        let snapshot = parse_rest_snapshot(
            &public,
            GatePublicRawPayload::new(
                &public,
                GatePublicPayloadKind::RestOrderBookSnapshot,
                7,
                1_000,
                r#"{"id":100,"current":1700000000.123,"bids":[{"p":"0.100","s":"2"}],"asks":[{"p":"0.101","s":"3"}]}"#.to_owned(),
            )?,
        )?;
        let delta = parse_ws_delta(
            &public,
            GatePublicRawPayload::new(
                &public,
                GatePublicPayloadKind::WebSocketOrderBookDelta,
                7,
                1_001,
                r#"{"time_ms":1700000001000,"channel":"futures.order_book_update","event":"update","result":{"t":1700000001001,"s":"DOGE_USDT","U":100,"u":101,"b":[{"p":"0.100","s":"2"}],"a":[{"p":"0.101","s":"3"}]}}"#.to_owned(),
            )?,
        )?;
        let mut bridge = GateScalpingBookBridge::new(GateOrderBookBridge::new(public, 7, 4)?);
        assert!(bridge.receive_snapshot(snapshot)?.is_empty());
        let events = bridge.receive_delta(delta)?;
        assert!(matches!(
            events.as_slice(),
            [(_, MarketEvent::Snapshot(_)), (_, MarketEvent::Delta(_))]
        ));
        Ok(())
    }

    #[test]
    fn public_book_bridge_requires_covering_delta_and_resets_on_gap_or_reverse()
    -> Result<(), Box<dyn std::error::Error>> {
        use venue_domain::domain::{MarketDelta, MarketLevel, MarketSnapshot};
        let symbol: Symbol = "BTC/USDT".parse()?;
        let snapshot = MarketSnapshot {
            symbol: symbol.clone(),
            generation: 1,
            sequence: 10,
            exchange_time_ms: None,
            bids: vec![MarketLevel {
                price: Price::new(Decimal::new(100, 0))?,
                quantity: Decimal::ONE,
            }],
            asks: vec![MarketLevel {
                price: Price::new(Decimal::new(101, 0))?,
                quantity: Decimal::ONE,
            }],
        };
        let mut book = venue_indicators::OrderBook::default();
        book.apply_snapshot(snapshot);
        assert!(!book.bridged());
        let continuous = MarketDelta {
            symbol: symbol.clone(),
            generation: 1,
            first_sequence: 11,
            previous_sequence: Some(10),
            sequence: 11,
            exchange_time_ms: Some(1),
            bids: vec![],
            asks: vec![],
        };
        assert!(book.apply_delta_if_fresh(continuous)?);
        assert!(book.bridged());
        let gap = MarketDelta {
            symbol: symbol.clone(),
            generation: 1,
            first_sequence: 14,
            previous_sequence: Some(13),
            sequence: 14,
            exchange_time_ms: Some(2),
            bids: vec![],
            asks: vec![],
        };
        assert!(book.apply_delta_if_fresh(gap).is_err());
        assert!(!book.synchronized());
        book.apply_snapshot(MarketSnapshot {
            symbol,
            generation: 2,
            sequence: 20,
            exchange_time_ms: None,
            bids: vec![MarketLevel {
                price: Price::new(100.into())?,
                quantity: Decimal::ONE,
            }],
            asks: vec![MarketLevel {
                price: Price::new(101.into())?,
                quantity: Decimal::ONE,
            }],
        });
        let reverse = MarketDelta {
            symbol: "BTC/USDT".parse()?,
            generation: 2,
            first_sequence: 19,
            previous_sequence: Some(18),
            sequence: 19,
            exchange_time_ms: Some(3),
            bids: vec![],
            asks: vec![],
        };
        assert!(!book.apply_delta_if_fresh(reverse)?);
        assert!(!book.bridged());
        Ok(())
    }

    fn stream_image(
        symbol: Symbol,
        sequence: u64,
        time: u64,
    ) -> Result<MarketEvent, Box<dyn std::error::Error>> {
        use venue_domain::{MarketLevel, MarketSnapshot};
        Ok(MarketEvent::Snapshot(MarketSnapshot {
            symbol,
            generation: 1,
            sequence,
            exchange_time_ms: Some(time),
            bids: vec![MarketLevel {
                price: Price::new(Decimal::ONE)?,
                quantity: Decimal::TEN,
            }],
            asks: vec![MarketLevel {
                price: Price::new(Decimal::new(101, 2))?,
                quantity: Decimal::TEN,
            }],
        }))
    }

    #[test]
    fn complete_ws_images_reach_features_without_inventing_delta_authority()
    -> Result<(), Box<dyn std::error::Error>> {
        for venue in [VenueId::Bybit, VenueId::Hyperliquid] {
            let directory = tempfile::tempdir()?;
            let (mut resident, state, binding) = setup_for(directory.path(), venue)?;
            let time = now()?;
            for sequence in [10, 70, 100] {
                assert!(resident.publish_full_snapshot_scalping_book(
                    &binding,
                    time,
                    stream_image(binding.key.symbol.clone(), sequence, time)?
                )?);
            }
            let source = resident
                .scalping_features
                .get(&binding.key)
                .ok_or("missing source")?;
            assert_eq!(source.generation(), Some(1));
            assert_ne!(source.state(), venue_indicators::FeatureState::DataGap);
            assert_eq!(
                resident.scalping_capture_sequence.get(&binding.key),
                Some(&3)
            );
            let book = resident
                .scalping_books
                .get(&binding.key)
                .ok_or("missing book")?;
            assert_eq!(book.sequence(), Some(100));
            assert!(
                !book.bridged(),
                "full image ingestion must not forge delta continuity"
            );
            assert!(venue_indicators::PublicBook::bridged(
                &full_snapshot_book::FullSnapshotBook(book)
            ));
            assert_eq!(state.lock().map_err(|_| "state lock")?.dispatches, 0);

            let mut stale_binding = binding.clone();
            stale_binding.config_digest = "old-config".to_owned();
            assert!(
                resident
                    .publish_full_snapshot_scalping_book(
                        &stale_binding,
                        time,
                        stream_image(binding.key.symbol.clone(), 101, time)?
                    )
                    .is_err()
            );
            assert!(
                resident
                    .publish_full_snapshot_scalping_book(
                        &binding,
                        time,
                        stream_image("BTC/USDT".parse()?, 101, time)?
                    )
                    .is_err()
            );
            assert_eq!(
                resident.scalping_capture_sequence.get(&binding.key),
                Some(&3)
            );
        }
        Ok(())
    }

    #[test]
    fn okx_stream_keeps_real_predecessor_bridge_and_fences_gap()
    -> Result<(), Box<dyn std::error::Error>> {
        use venue_domain::MarketDelta;
        let directory = tempfile::tempdir()?;
        let (mut resident, state, binding) = setup(directory.path())?;
        let time = now()?;
        assert!(
            resident
                .publish_full_snapshot_scalping_book(
                    &binding,
                    time,
                    stream_image(binding.key.symbol.clone(), 10, time)?
                )
                .is_err()
        );
        assert!(resident.publish_sequenced_scalping_book(
            &binding,
            time,
            stream_image(binding.key.symbol.clone(), 10, time)?
        )?);
        let delta = |previous, sequence| {
            MarketEvent::Delta(MarketDelta {
                symbol: binding.key.symbol.clone(),
                generation: 1,
                first_sequence: sequence,
                previous_sequence: Some(previous),
                sequence,
                exchange_time_ms: Some(time),
                bids: Vec::new(),
                asks: Vec::new(),
            })
        };
        assert!(resident.publish_sequenced_scalping_book(&binding, time, delta(10, 90))?);
        assert!(
            resident
                .scalping_books
                .get(&binding.key)
                .ok_or("book")?
                .bridged()
        );
        let capture = resident
            .scalping_capture_sequence
            .get(&binding.key)
            .copied();
        assert!(!resident.publish_sequenced_scalping_book(&binding, time, delta(99, 100))?);
        assert!(!resident.publish_sequenced_scalping_book(&binding, time, delta(100, 101))?);
        assert_eq!(
            resident
                .scalping_features
                .get(&binding.key)
                .ok_or("source")?
                .state(),
            venue_indicators::FeatureState::DataGap
        );
        assert_eq!(
            resident
                .scalping_capture_sequence
                .get(&binding.key)
                .copied(),
            capture
        );
        assert!(!resident.publish_sequenced_scalping_book(
            &binding,
            time,
            stream_image(binding.key.symbol.clone(), 200, time)?
        )?);
        let MarketEvent::Snapshot(mut replacement) =
            stream_image(binding.key.symbol.clone(), 300, time)?
        else {
            return Err("snapshot".into());
        };
        replacement.generation = 2;
        assert!(resident.publish_sequenced_scalping_book(
            &binding,
            time,
            MarketEvent::Snapshot(replacement)
        )?);
        assert_eq!(state.lock().map_err(|_| "state lock")?.dispatches, 0);
        Ok(())
    }

    #[test]
    fn interleaved_symbols_keep_independent_feature_capture_cursors()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let (mut resident, state, first) = setup_for(directory.path(), VenueId::Bybit)?;
        let mut second = first.clone();
        second.key.symbol = "BTC/USDT".parse()?;
        second.key.instance_id = "scalp-2".to_owned();
        // Exercise only per-actor public feature state, without installing any execution route.
        resident
            .scalping_books
            .insert(second.key.clone(), venue_indicators::OrderBook::default());
        let second_scalping_binding = scalping_binding(&second)?;
        let second_params = ScalpingParams::for_binding(&second_scalping_binding);
        resident.scalping_bridges.insert(
            second.key.clone(),
            ScalpingBridgeState::restore_or_bootstrap(
                None,
                second_scalping_binding,
                second_params.clone(),
            )?,
        );
        resident
            .scalping_features
            .insert(second.key.clone(), feature_source(&second, &second_params)?);
        let time = now()?;
        for sequence in [10, 90] {
            for binding in [&first, &second] {
                let event = stream_image(binding.key.symbol.clone(), sequence, time)?;
                assert!(resident.prepare_scalping_book(binding, &event)?);
                resident.drive_features(
                    binding,
                    AccountMarketEvent::new(time, event)?,
                    BookFeed::CompleteWebSocketImages,
                )?;
            }
        }
        for binding in [&first, &second] {
            assert_eq!(
                resident.scalping_capture_sequence.get(&binding.key),
                Some(&2)
            );
            assert_ne!(
                resident
                    .scalping_features
                    .get(&binding.key)
                    .ok_or("source")?
                    .state(),
                venue_indicators::FeatureState::DataGap
            );
        }
        assert_eq!(state.lock().map_err(|_| "state lock")?.dispatches, 0);
        Ok(())
    }
}
