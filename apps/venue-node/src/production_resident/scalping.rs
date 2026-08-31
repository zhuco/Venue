//! Scalping's production handoff starts only after the feature owner has produced one semantic
//! proposal. This module deliberately does not turn a reference price into an order: Runtime's
//! MarketHub has no authority to choose rules, size, hedge leg, or a post-only price. The only
//! physical translation is the account Host's adapter-owned normalization.

use std::{
    num::NonZeroUsize,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use sha2::{Digest, Sha256};
use venue_domain::domain::MarketEvent;
use venue_domain::domain::{CommandId, OrderOwner, OrderSide, PositionSide};
use venue_runtime::{
    AccountLimitNormalizationIntent, AccountPhysicalGateway, StrategyBinding, StrategyKind,
    account::AccountLanePriority, strategy::AccountMarketEvent,
};
use venue_strategies::scalping::{Direction, SemanticIntent, SemanticPurpose};

use super::{NodeError, ProductionResident, persist_anchor, resident_error};
use crate::ResidentSemanticIntent;

mod full_snapshot_book;

#[derive(Clone, Copy)]
enum BookFeed {
    SequencedDelta,
    CompleteWebSocketImages,
}

#[cfg(feature = "bitget")]
pub(crate) struct BitgetScalpingBookBridge {
    sequencer: venue_gateway_bitget::public::BitgetBookSequencer,
    pending_snapshot: Option<venue_gateway_bitget::public::BitgetBooksMessage>,
}

#[cfg(feature = "bitget")]
impl BitgetScalpingBookBridge {
    fn new() -> Self {
        Self {
            sequencer: venue_gateway_bitget::public::BitgetBookSequencer::new(),
            pending_snapshot: None,
        }
    }

    /// Bitget's snapshot alone is deliberately withheld.  The first update must prove it covers
    /// the snapshot before either record can enter MarketHub; a REST BBO cannot take this path.
    fn accept(
        &mut self,
        message: venue_gateway_bitget::public::BitgetBooksMessage,
    ) -> Result<Vec<(u64, MarketEvent)>, NodeError> {
        use venue_gateway_bitget::public::BitgetBookSequenceStatus;

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

#[derive(Serialize)]
struct ScalpingSemanticReplay<'a> {
    intent: &'a SemanticIntent,
    observed_at_ms: u64,
}

impl<G: AccountPhysicalGateway> ProductionResident<G> {
    /// Registers the Runtime actor that owns the one Scalping semantic route.  It has no gateway
    /// access; subsequent physical work is only available through [`submit_scalping_intent`].
    pub fn register_scalping_actor(&mut self, binding: StrategyBinding) -> Result<(), NodeError> {
        if binding.key.strategy_kind != StrategyKind::Scalping {
            return Err(NodeError::ResidentRuntime);
        }
        self.register_actor(binding.clone())?;
        if self
            .scalping_bindings
            .insert(binding.key.clone(), binding.clone())
            .is_some()
        {
            return Err(NodeError::ResidentRuntime);
        }
        self.scalping_books
            .insert(binding.key.clone(), venue_indicators::OrderBook::default());
        self.scalping_features.insert(
            binding.key.clone(),
            venue_indicators::ScalpingPublicMarketSource::new(
                binding.key.symbol.clone(),
                "node_scalping_v1",
                feature_profile_digest(&binding),
                1_000,
                NonZeroUsize::new(256).ok_or(NodeError::ResidentRuntime)?,
            )
            .map_err(|_| NodeError::ResidentRuntime)?,
        );
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
        self.runtime.publish_market(event).map_err(resident_error)
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

    /// Commits one reducer-produced semantic entry as an Actor Applied turn, then asks the sole
    /// Host to obtain current adapter rules/BBO and normalize its bounded quote exposure before
    /// the usual WAL/lane dispatch. No Node code selects a physical price or quantity.
    pub fn submit_scalping_intent(
        &mut self,
        binding: &StrategyBinding,
        intent: SemanticIntent,
        observed_at_ms: u64,
    ) -> Result<CommandId, NodeError> {
        validate_submission(binding, &intent, observed_at_ms)?;
        let normalization = normalization_intent(binding, &intent)?;
        let command_id = normalization.command_id.clone();
        let replay = serde_json::to_vec(&ScalpingSemanticReplay {
            intent: &intent,
            observed_at_ms,
        })
        .map_err(|_| NodeError::ResidentRuntime)?;
        let applied = self
            .runtime
            .persist_resident_semantic_turn(binding, replay)
            .map_err(resident_error)?;
        persist_anchor(&self.artifacts_root, binding, &applied)?;
        self.host
            .normalize_and_prepare_limit(
                &mut self.runtime,
                binding,
                &applied,
                AccountLanePriority::Normal,
                &normalization,
            )
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: error.to_string(),
            })?;
        self.runtime
            .dispatch_next_with_host(&mut self.host)
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: error.to_string(),
            })?;
        Ok(command_id)
    }

    /// Consumes the exact semantic output dequeued from the shared resident actor.  This is the
    /// production handoff for a candidate producer: a Grid, Copy, or caller-constructed semantic
    /// variant cannot be reinterpreted as a Scalping entry.
    pub fn dispatch_resident_scalping_intent(
        &mut self,
        semantic: ResidentSemanticIntent,
        observed_at_ms: u64,
    ) -> Result<CommandId, NodeError> {
        let ResidentSemanticIntent::Scalping { binding, intent } = semantic else {
            return Err(NodeError::ResidentRuntime);
        };
        self.submit_scalping_intent(&binding, intent, observed_at_ms)
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
        if !matches!(event.event, MarketEvent::Snapshot(_))
            && !self.scalping_features.contains_key(&binding.key)
        {
            // A gap fenced this actor.  Only a new snapshot may recreate its source; later
            // trade/bar/delta facts are ignored and cannot make it ready or create intent.
            return Ok(());
        }
        let (scalping_books, scalping_features, scalping_capture_sequence) = (
            &mut self.scalping_books,
            &mut self.scalping_features,
            &mut self.scalping_capture_sequence,
        );
        let book = scalping_books
            .get_mut(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?;
        match &event.event {
            MarketEvent::Snapshot(v) => {
                book.apply_snapshot(v.clone());
                if !scalping_features.contains_key(&binding.key) {
                    scalping_features.insert(
                        binding.key.clone(),
                        venue_indicators::ScalpingPublicMarketSource::new(
                            binding.key.symbol.clone(),
                            "node_scalping_v1",
                            feature_profile_digest(binding),
                            1_000,
                            NonZeroUsize::new(256).ok_or(NodeError::ResidentRuntime)?,
                        )
                        .map_err(|_| NodeError::ResidentRuntime)?,
                    );
                }
            }
            MarketEvent::Delta(v) if book.apply_delta_if_fresh(v.clone()).is_err() => {
                scalping_features.remove(&binding.key);
                return Ok(());
            }
            _ => {}
        }
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
        .map_err(|_| NodeError::ResidentRuntime)?;
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

fn feature_profile_digest(binding: &StrategyBinding) -> String {
    let mut digest = Sha256::new();
    digest.update(b"venue.node.scalping.feature-profile.v1");
    digest.update((binding.config_digest.len() as u64).to_be_bytes());
    digest.update(binding.config_digest.as_bytes());
    format!("{:x}", digest.finalize())
}

fn validate_submission(
    binding: &StrategyBinding,
    intent: &SemanticIntent,
    observed_at_ms: u64,
) -> Result<(), NodeError> {
    if binding.key.strategy_kind != StrategyKind::Scalping
        || observed_at_ms == 0
        || intent.intent_id.trim().is_empty()
        || intent.symbol != binding.key.symbol
        || intent.purpose != SemanticPurpose::Entry
        || intent.valid_until_ms < observed_at_ms
        || intent.entry_ttl_ms == 0
        || intent.target_quote.asset.as_str() != binding.key.symbol.quote()
        || intent.risk_plan.quote_cap.asset.as_str() != binding.key.symbol.quote()
        || intent.target_quote.value <= rust_decimal::Decimal::ZERO
        || intent.target_quote.value > intent.risk_plan.quote_cap.value
        || intent.max_slippage_bps <= rust_decimal::Decimal::ZERO
    {
        return Err(NodeError::ResidentRuntime);
    }
    Ok(())
}

fn normalization_intent(
    binding: &StrategyBinding,
    intent: &SemanticIntent,
) -> Result<AccountLimitNormalizationIntent, NodeError> {
    let (side, position_side) = match intent.direction {
        Direction::Long => (OrderSide::Buy, PositionSide::Long),
        Direction::Short => (OrderSide::Sell, PositionSide::Short),
    };
    Ok(AccountLimitNormalizationIntent {
        command_id: stable_id(b"command", binding, intent)?,
        client_order_id: stable_id(b"client", binding, intent)?,
        owner: OrderOwner {
            strategy_instance_id: binding.key.instance_id.clone(),
            run_id: binding.run_id.clone(),
            exchange: binding.key.account.exchange.as_str().to_owned(),
            account: binding.key.account.account.clone(),
            symbol: binding.key.symbol.clone(),
            purpose: venue_domain::domain::OrderPurpose::Entry,
        },
        side,
        position_side,
        quote_delta: intent.target_quote.value,
        reduce_only: false,
    })
}

fn stable_id(
    label: &[u8],
    binding: &StrategyBinding,
    intent: &SemanticIntent,
) -> Result<CommandId, NodeError> {
    let mut digest = Sha256::new();
    digest.update(b"venue.node.scalping.entry.v1");
    for field in [
        label,
        binding.key.account.exchange.as_str().as_bytes(),
        binding.key.account.account.as_bytes(),
        binding.key.instance_id.as_bytes(),
        binding.key.symbol.to_string().as_bytes(),
        binding.run_id.as_bytes(),
        binding.config_digest.as_bytes(),
        intent.intent_id.as_bytes(),
        intent.idempotency_seed.as_bytes(),
    ] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    let raw = format!("sc-{:x}", digest.finalize());
    CommandId::new(raw[..35].to_owned()).map_err(|_| NodeError::ResidentRuntime)
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::{Arc, Mutex},
        time::{SystemTime, UNIX_EPOCH},
    };

    use rust_decimal::Decimal;
    use venue_domain::domain::{Amount, Asset, ExecutionCommand, OrderCommand, Price, Symbol};
    use venue_gateway_api::{GatewayBinding, VenueId};
    use venue_runtime::{
        AccountDispatchPermit, AccountGatewayResult, AccountHostValidationError,
        AccountLimitNormalizationIntent, AccountPhysicalGateway, AccountRecoveryReport,
        AccountRecoveryRequest, AccountRiskEvidence, SignedAccountBalance,
        SignedAccountPositionMode, SignedAccountSnapshot,
    };

    use super::*;
    use crate::{NodeLaunch, ResidentFact, ResidentLoop, ScalpingResidentActor};

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
        resident.register_scalping_actor(binding.clone())?;
        Ok((resident, state, binding))
    }

    fn intent(now_ms: u64) -> Result<SemanticIntent, Box<dyn std::error::Error>> {
        let asset = Asset::new("USDT")?;
        Ok(SemanticIntent {
            intent_id: "entry-1".to_owned(),
            symbol: Symbol::new("DOGE", "USDT")?,
            direction: Direction::Long,
            purpose: SemanticPurpose::Entry,
            expert: venue_strategies::scalping::Expert::RangeFade,
            entry_style: venue_strategies::scalping::EntryStyle::PassiveMaker,
            exit_template: venue_strategies::scalping::ExitTemplate::FairValue,
            attempt_cap: 1,
            max_reprices: 0,
            risk_plan: venue_strategies::scalping::RiskPlan {
                risk_per_episode: venue_strategies::scalping::RiskLimit::new(
                    venue_strategies::scalping::RiskUnit::shadow(),
                    Decimal::ONE,
                ),
                quote_cap: Amount::new(asset.clone(), Decimal::TEN),
                max_episode_loss: venue_strategies::scalping::RiskLimit::new(
                    venue_strategies::scalping::RiskUnit::shadow(),
                    Decimal::ONE,
                ),
            },
            target_quote: Amount::new(asset, Decimal::TEN),
            reference_price: Price::new(Decimal::ONE)?,
            max_slippage_bps: Decimal::new(100, 0),
            valid_until_ms: now_ms.saturating_add(1_000),
            entry_ttl_ms: 1_000,
            hard_stop_distance_bps: Decimal::ONE,
            target_distance_bps: Decimal::ONE,
            max_hold_ms: 1_000,
            max_unprotected_ms: 100,
            requires_server_protection: false,
            opportunity_key: "opportunity-1".to_owned(),
            breakout_cursor: None,
            idempotency_seed: "seed-1".to_owned(),
        })
    }

    #[test]
    fn semantic_entry_reaches_the_shared_host_wal_and_writer()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let (mut resident, state, binding) = setup(directory.path())?;
        let observed_at_ms = now()?;
        let mut resident_loop = ResidentLoop::new(binding.key.account.clone());
        resident_loop.register_scalping(ScalpingResidentActor::new(
            binding.clone(),
            &venue_strategies::scalping::ScalpingParams::phase8(Amount::new(
                Asset::new("USDT")?,
                Decimal::TEN,
            )),
        ))?;
        resident_loop.consume(ResidentFact::MarketScalpingCandidate {
            target: binding.key.clone(),
            intent: intent(observed_at_ms)?,
        })?;
        let semantic = resident_loop
            .next_intent()
            .ok_or("semantic candidate missing")?;
        resident.dispatch_resident_scalping_intent(semantic, observed_at_ms)?;
        let state = state.lock().map_err(|_| "state lock")?;
        assert_eq!(state.dispatches, 1);
        assert_eq!(state.commands.len(), 1);
        let actor_journal = directory
            .path()
            .join("okx")
            .join("LIVE")
            .join(ACCOUNT)
            .join("strategies")
            .join("scalp-1")
            .join("actor-applied.jsonl");
        assert!(!std::fs::read(actor_journal)?.is_empty());
        Ok(())
    }

    #[test]
    fn expired_semantic_intent_is_rejected_before_the_host_wal()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let (mut resident, state, binding) = setup(directory.path())?;
        let observed_at_ms = now()?;
        let mut expired = intent(observed_at_ms)?;
        expired.valid_until_ms = observed_at_ms.saturating_sub(1);
        assert!(
            resident
                .submit_scalping_intent(&binding, expired, observed_at_ms)
                .is_err()
        );
        assert_eq!(state.lock().map_err(|_| "state lock")?.dispatches, 0);
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
                1,
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
        assert!(bridge.accept(snapshot)?.is_empty());
        let events = bridge.accept(update)?;
        assert!(matches!(
            events.as_slice(),
            [(_, MarketEvent::Snapshot(_)), (_, MarketEvent::Delta(_))]
        ));
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
        resident.publish_sequenced_scalping_book(&binding, time, delta(99, 100))?;
        assert!(!resident.scalping_features.contains_key(&binding.key));
        resident.publish_sequenced_scalping_book(&binding, time, delta(100, 101))?;
        assert!(!resident.scalping_features.contains_key(&binding.key));
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
        resident.scalping_features.insert(
            second.key.clone(),
            venue_indicators::ScalpingPublicMarketSource::new(
                second.key.symbol.clone(),
                "node_scalping_v1",
                feature_profile_digest(&second),
                1_000,
                NonZeroUsize::new(256).ok_or("history")?,
            )?,
        );
        let time = now()?;
        for sequence in [10, 90] {
            for binding in [&first, &second] {
                resident.drive_features(
                    binding,
                    AccountMarketEvent::new(
                        time,
                        stream_image(binding.key.symbol.clone(), sequence, time)?,
                    )?,
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
