use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc,
        mpsc::{self, Receiver, Sender, TryRecvError},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rust_decimal::Decimal;
use serde_json::Value;

use crate::{
    domain::{
        AccountBalance, AccountRiskSnapshot, CancelCommand, ExecutionCommand, FieldState, Fill,
        Instrument, LegRiskSnapshot, MarketEvent, MarketOrderCommand, MarketReduceCommand,
        NativeOrderFamily, Order, OrderCommand, Position, Price, Symbol,
    },
    exchange::binance::{
        BinanceContractRules, BinanceError, BinanceGridPrivateReadback, BinanceRulesError,
        PrivateError as BinancePrivateError, PrivateListenKey as BinancePrivateListenKey,
        PrivateReadbackError as BinancePrivateReadbackError, PrivateRest as BinancePrivateRest,
        PrivateStreamSocket as BinancePrivateStream, PublicError as BinancePublicError,
        PublicRest as BinancePublicRest, PublicStream as BinancePublicStreamKind,
        PublicStreamSocket as BinancePublicStream,
    },
    exchange::bitget::{
        BitgetContractRules, BitgetCredentials, BitgetError, BitgetPrivateRest,
        BitgetPrivateStream, BitgetPublicRest, BitgetPublicStream,
    },
    exchange::bitget_public::{
        BitgetBookSequenceStatus, BitgetBookSequencer, BitgetPublicSource, BitgetRawPublicPayload,
        parse_books_message, parse_public_trade_message, parse_rest_orderbook, parse_rest_ticker,
        rest_orderbook_path as bitget_orderbook_path, rest_ticker_path as bitget_ticker_path,
    },
    exchange::gate::{
        GateContractRules, GateCredentials, GateError, GatePrivateEvent, GatePrivateReadback,
        GatePrivateRest, GatePrivateStream, GatePublicRest, GatePublicStream,
    },
    exchange::gate_public::{
        GateBookBridgeAction, GateOrderBookBridge, GatePublicBinding, GatePublicPayloadKind,
        GatePublicRawPayload, parse_rest_snapshot, parse_ws_book_ticker, parse_ws_delta,
        parse_ws_mark_price, parse_ws_trades, rest_order_book_path as gate_orderbook_path,
    },
    market::{OrderBook, RawMarketRecord, RawSource},
};

/// The deliberately small contract consumed by the stage-7 grid runtime. It is not a public SDK:
/// it contains only the public rules/top, signed private reconciliation, exact order identity, and
/// mutation operations required by this one strategy.
pub(crate) trait HedgedGridVenue {
    fn exchange(&self) -> &'static str;
    fn instrument(&self) -> &Instrument;
    fn minimum_quantity(&self) -> Decimal;
    /// Re-fetches the selected symbol's current public rules and proves that every execution
    /// field still equals the startup snapshot. Production venues override this; the fail-closed
    /// default keeps transport-free test venues source-compatible without granting proof.
    #[allow(dead_code)] // The Stage-7 admission caller is intentionally wired in a separate change.
    fn verify_current_instrument_rules(&mut self) -> Result<(), GridVenueError> {
        Err(GridVenueError::InstrumentRulesUnavailable)
    }
    /// A recovered raw-public journal owns the monotonic transport generation across process
    /// boundaries. Implementations may seed an empty local bridge before its first connection.
    fn seed_public_generation(&mut self, _minimum_generation: u64) -> Result<(), GridVenueError> {
        Ok(())
    }
    fn connect_public_stream(&mut self) -> Result<(), GridVenueError> {
        Ok(())
    }
    fn next_public_payload(&mut self) -> Result<Option<GridPublicPayload>, GridVenueError> {
        Ok(None)
    }
    /// The runtime calls this only after the raw payload has been fsynced in its own journal.
    fn accept_public_payload(&mut self, _payload: GridPublicPayload) -> Result<(), GridVenueError> {
        Ok(())
    }
    fn reset_public_stream(&mut self) {}
    fn best_bid_ask(&self, now_ms: u64) -> Result<(Price, Price), GridVenueError>;
    /// The runtime supplies an artifacts-root-local lower bound for account-wide fill history.
    /// Exchanges whose fill endpoints are symbol-scoped may ignore it.
    fn set_fill_history_start_ms(&mut self, _start_ms: u64) {}
    fn readback(&mut self) -> Result<GridVenueReadback, GridVenueError>;
    /// `account` is the canonical runtime deployment identity and must be copied into the
    /// normalized snapshot. Exchange-native account-mode labels remain adapter configuration.
    fn risk_readback(
        &mut self,
        _account: &str,
        _private_generation: u64,
    ) -> Result<GridRiskReadback, GridVenueError> {
        Err(GridVenueError::RiskReadbackUnsupported)
    }
    /// A request-only risk client lets the resident acquire a periodic snapshot outside the
    /// private-fill turn. It owns no strategy state, writer lease, journal, or mutation method.
    fn risk_readback_client(&self) -> Option<Arc<dyn HedgedGridRiskReadbackClient>> {
        None
    }
    fn connect_private_stream(&mut self) -> Result<(), GridVenueError>;
    fn next_private_event(&mut self) -> Result<Option<GridPrivateEvent>, GridVenueError>;
    /// Drops only the transport generation. The runtime follows this with a fresh signed
    /// readback before it considers any further mutation.
    fn reset_private_stream(&mut self);
    /// A request-only client with no strategy state. A dispatch wave holds the sole writer lease
    /// and may call it concurrently, so every exchange request is independently idempotent.
    fn mutation_client(&self) -> Arc<dyn HedgedGridMutationClient>;
    /// Production venues override this exchange-native preflight. The permissive default exists
    /// only so transport-free test venues can focus on strategy/recovery behavior.
    fn validate_client_order_id(&self, _client_order_id: &str) -> Result<(), GridVenueError> {
        Ok(())
    }
    /// This is narrower than an absent-order inference: it proves that an older Unknown command
    /// failed the adapter's local identity check before any HTTP request could begin.
    fn proves_never_dispatched(&self, _command: &ExecutionCommand, _unknown_reason: &str) -> bool {
        false
    }
    fn order_by_client_id(&mut self, client_order_id: &str) -> Result<Order, GridVenueError>;
    fn verify_post_only_order(&mut self, client_order_id: &str) -> Result<(), GridVenueError>;
}

pub(crate) trait HedgedGridMutationClient: Send + Sync {
    fn place_limit_post_only(&self, command: &OrderCommand) -> Result<String, GridVenueError>;
    fn place_market(&self, command: &MarketOrderCommand) -> Result<String, GridVenueError>;
    fn place_market_reduce(
        &self,
        _command: &MarketReduceCommand,
    ) -> Result<String, GridVenueError> {
        Err(GridVenueError::MarketReduceUnsupported)
    }
    fn cancel_by_client_id(&self, command: &CancelCommand) -> Result<String, GridVenueError>;
    fn cancel_algo_by_client_id(&self, _client_algo_id: &str) -> Result<String, GridVenueError> {
        Err(GridVenueError::MutationUnsupported)
    }
}

pub(crate) trait HedgedGridRiskReadbackClient: Send + Sync {
    fn risk_readback(
        &self,
        account: &str,
        private_generation: u64,
    ) -> Result<GridRiskReadback, GridVenueError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GridVenueReadback {
    pub raw_private_payloads: Vec<String>,
    /// `None` is an un-migrated adapter result and must be rejected by every live admission
    /// path. A populated value proves every native family was read or explicitly unsupported.
    pub order_family_readback: Option<GridOrderFamilyReadback>,
    pub balance: AccountBalance,
    pub hedge_position: bool,
    pub positions: Vec<Position>,
    pub orders: Vec<Order>,
    pub fills: Vec<GridVenueFill>,
}

/// The only native order families that may appear in a Stage-7 account readback.
pub(crate) const GRID_ORDER_FAMILIES: [NativeOrderFamily; 3] = [
    NativeOrderFamily::UmOrder,
    NativeOrderFamily::UmConditional,
    NativeOrderFamily::UmAlgo,
];

/// One canonical family is either represented by a complete signed endpoint response or is
/// unavailable under the exact adapter profile that also constrains mutation admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GridOrderFamilySnapshot {
    Complete {
        orders: Vec<Order>,
        signed_payloads: Vec<String>,
    },
    ExplicitlyUnsupported,
}

/// Complete coverage cannot be fabricated from an omitted endpoint: every canonical family has
/// an explicit entry. The raw authenticated payloads remain in `GridVenueReadback` for durable
/// evidence capture; this structure keeps their family completeness meaning intact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GridOrderFamilyReadback {
    snapshots: BTreeMap<NativeOrderFamily, GridOrderFamilySnapshot>,
}

impl GridOrderFamilyReadback {
    pub(crate) fn regular_only_adapter_profile(
        orders: Vec<Order>,
        signed_payloads: Vec<String>,
    ) -> Result<Self, GridVenueError> {
        if signed_payloads.is_empty() {
            return Err(GridVenueError::PrivateReadbackIncomplete);
        }
        Ok(Self {
            snapshots: BTreeMap::from([
                (
                    NativeOrderFamily::UmOrder,
                    GridOrderFamilySnapshot::Complete {
                        orders,
                        signed_payloads,
                    },
                ),
                (
                    NativeOrderFamily::UmConditional,
                    GridOrderFamilySnapshot::ExplicitlyUnsupported,
                ),
                (
                    NativeOrderFamily::UmAlgo,
                    GridOrderFamilySnapshot::ExplicitlyUnsupported,
                ),
            ]),
        })
    }

    #[cfg(test)]
    pub(crate) fn complete_adapter_profile(
        regular_orders: Vec<Order>,
        regular_signed_payloads: Vec<String>,
        conditional_orders: Vec<Order>,
        conditional_signed_payloads: Vec<String>,
        algo_orders: Vec<Order>,
        algo_signed_payloads: Vec<String>,
    ) -> Result<Self, GridVenueError> {
        let snapshots = BTreeMap::from([
            (
                NativeOrderFamily::UmOrder,
                GridOrderFamilySnapshot::Complete {
                    orders: regular_orders,
                    signed_payloads: regular_signed_payloads,
                },
            ),
            (
                NativeOrderFamily::UmConditional,
                GridOrderFamilySnapshot::Complete {
                    orders: conditional_orders,
                    signed_payloads: conditional_signed_payloads,
                },
            ),
            (
                NativeOrderFamily::UmAlgo,
                GridOrderFamilySnapshot::Complete {
                    orders: algo_orders,
                    signed_payloads: algo_signed_payloads,
                },
            ),
        ]);
        let profile = Self { snapshots };
        profile.validate_complete()?;
        Ok(profile)
    }

    /// Binance retired its legacy UM conditional collection. The current Algo collection is the
    /// only signed source for all live conditional strategies, while the retired namespace has
    /// no Stage-7 mutation surface.
    pub(crate) fn regular_and_algo_adapter_profile(
        regular_orders: Vec<Order>,
        regular_signed_payloads: Vec<String>,
        algo_orders: Vec<Order>,
        algo_signed_payloads: Vec<String>,
    ) -> Result<Self, GridVenueError> {
        let snapshots = BTreeMap::from([
            (
                NativeOrderFamily::UmOrder,
                GridOrderFamilySnapshot::Complete {
                    orders: regular_orders,
                    signed_payloads: regular_signed_payloads,
                },
            ),
            (
                NativeOrderFamily::UmConditional,
                GridOrderFamilySnapshot::ExplicitlyUnsupported,
            ),
            (
                NativeOrderFamily::UmAlgo,
                GridOrderFamilySnapshot::Complete {
                    orders: algo_orders,
                    signed_payloads: algo_signed_payloads,
                },
            ),
        ]);
        let profile = Self { snapshots };
        profile.validate_complete()?;
        Ok(profile)
    }

    #[must_use]
    pub(crate) fn snapshot(&self, family: NativeOrderFamily) -> Option<&GridOrderFamilySnapshot> {
        self.snapshots.get(&family)
    }

    #[must_use]
    pub(crate) fn covers_all_families(&self) -> bool {
        self.snapshots.len() == GRID_ORDER_FAMILIES.len()
            && GRID_ORDER_FAMILIES
                .iter()
                .all(|family| self.snapshots.contains_key(family))
    }

    pub(crate) fn validate_complete(&self) -> Result<(), GridVenueError> {
        if !self.covers_all_families() {
            return Err(GridVenueError::PrivateReadbackIncomplete);
        }
        for family in GRID_ORDER_FAMILIES {
            match self.snapshot(family) {
                Some(GridOrderFamilySnapshot::Complete {
                    signed_payloads, ..
                }) if !signed_payloads.is_empty() => {}
                Some(GridOrderFamilySnapshot::ExplicitlyUnsupported) => {}
                _ => return Err(GridVenueError::PrivateReadbackIncomplete),
            }
        }
        Ok(())
    }

    /// An explicit-unsupported family has no admitted mutation surface. Every supported family
    /// must have supplied its complete signed open-order page before retirement can call it empty.
    pub(crate) fn open_orders_are_empty(&self) -> Result<bool, GridVenueError> {
        self.validate_complete()?;
        Ok(GRID_ORDER_FAMILIES
            .iter()
            .all(|family| match self.snapshot(*family) {
                Some(GridOrderFamilySnapshot::ExplicitlyUnsupported) => true,
                Some(GridOrderFamilySnapshot::Complete { orders, .. }) => orders.is_empty(),
                None => false,
            }))
    }
}

impl GridVenueReadback {
    /// The regular-order projection feeds the existing Stage-7 WAL reconciler, so it must be the
    /// exact normal-family snapshot rather than a second, potentially incomplete endpoint view.
    pub(crate) fn validate_order_family_readback(&self) -> Result<(), GridVenueError> {
        let family_readback = self
            .order_family_readback
            .as_ref()
            .ok_or(GridVenueError::PrivateReadbackIncomplete)?;
        family_readback.validate_complete()?;
        match family_readback.snapshot(NativeOrderFamily::UmOrder) {
            Some(GridOrderFamilySnapshot::Complete { orders, .. }) if orders == &self.orders => {
                Ok(())
            }
            _ => Err(GridVenueError::PrivateReadbackIncomplete),
        }
    }

    /// Stop, Canary admission, and executable custody use all declared native families rather
    /// than the legacy regular-order projection. An omitted family is never interpreted as empty.
    pub(crate) fn all_order_families_empty(&self) -> Result<bool, GridVenueError> {
        self.validate_order_family_readback()?;
        self.order_family_readback
            .as_ref()
            .ok_or(GridVenueError::PrivateReadbackIncomplete)?
            .open_orders_are_empty()
    }

    /// Stage 7 has no conditional/Algo mutation owner. A signed row in either family therefore
    /// remains external custody and blocks its regular-order writer before it can issue a command.
    /// An adapter's explicit profile exclusion is not guessed empty: it denotes a family the
    /// admitted execution surface cannot create.
    pub(crate) fn unmanaged_order_families_are_empty(&self) -> Result<bool, GridVenueError> {
        self.validate_order_family_readback()?;
        let families = self
            .order_family_readback
            .as_ref()
            .ok_or(GridVenueError::PrivateReadbackIncomplete)?;
        Ok(
            [NativeOrderFamily::UmConditional, NativeOrderFamily::UmAlgo]
                .iter()
                .all(|family| match families.snapshot(*family) {
                    Some(GridOrderFamilySnapshot::Complete { orders, .. }) => orders.is_empty(),
                    Some(GridOrderFamilySnapshot::ExplicitlyUnsupported) => true,
                    None => false,
                }),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GridRiskReadback {
    pub raw_private_payloads: Vec<String>,
    pub account: AccountRiskSnapshot,
    pub legs: Vec<LegRiskSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GridVenueFill {
    pub fill: Fill,
    pub client_order_id: FieldState<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GridPrivateEvent {
    Fill {
        fill: Fill,
        client_order_id: FieldState<String>,
        raw_payload: String,
    },
    Reconcile {
        raw_payload: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GridPublicPayloadSource {
    RestSnapshot,
    RestTicker,
    WebSocketDepth,
    WebSocketBbo,
    WebSocketTrade,
    WebSocketMark,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GridPublicPayload {
    pub generation: u64,
    pub source: GridPublicPayloadSource,
    pub received_at_ms: u64,
    pub payload: String,
}

impl GridPublicPayload {
    fn new(
        generation: u64,
        source: GridPublicPayloadSource,
        received_at_ms: u64,
        payload: String,
    ) -> Result<Self, GridVenueError> {
        if generation == 0 || received_at_ms == 0 || payload.is_empty() {
            return Err(GridVenueError::PublicPayload);
        }
        Ok(Self {
            generation,
            source,
            received_at_ms,
            payload,
        })
    }
}

const PUBLIC_FRESHNESS_MS: u64 = 5_000;
const BINANCE_PUBLIC_DEPTH: u16 = 100;
const BINANCE_RISK_MAX_AGE_MS: u64 = 3_000;
const BINANCE_PRIVATE_READINESS_TIMEOUT: Duration = Duration::from_millis(1);
const GATE_PUBLIC_DEPTH: u16 = 20;
const GATE_PUBLIC_MAX_BUFFERED_DELTAS: usize = 1_024;

#[path = "public_market.rs"]
mod grid_public_market;
use grid_public_market::*;
pub(crate) struct BinanceGridVenue {
    symbol: Symbol,
    rules: BinanceContractRules,
    public: BinancePublicRest,
    public_market: BinancePublicMarket,
    public_stream: Option<BinancePublicStream>,
    pending_public: VecDeque<GridPublicPayload>,
    private: Arc<BinancePrivateRest>,
    private_listen_key: Option<BinancePrivateListenKey>,
    private_stream: Option<BinancePrivateStream>,
    private_keepalive: Option<BinancePrivateKeepalive>,
}

impl BinanceGridVenue {
    pub(crate) fn production(
        symbol: Symbol,
        instrument_generation: u64,
    ) -> Result<Self, GridVenueError> {
        let public = BinancePublicRest::production()?;
        let rules = public.contract_rules(&symbol, instrument_generation)?;
        let public_market = BinancePublicMarket::new(symbol.clone());
        let private = Arc::new(BinancePrivateRest::production(
            crate::exchange::binance::PrivateCredentials::from_environment()?,
            crate::config::BinanceAccountBinding::PortfolioMarginUm,
        )?);
        Ok(Self {
            symbol,
            rules,
            public,
            public_market,
            public_stream: None,
            pending_public: VecDeque::new(),
            private,
            private_listen_key: None,
            private_stream: None,
            private_keepalive: None,
        })
    }

    pub(crate) fn capability_binding(&self) -> crate::execution::CapabilityBinding {
        binance_capability_binding(&self.symbol, self.private.recovery_signer_sha256())
    }

    pub(crate) fn sign_recovery_payload_sha256(
        &self,
        payload_sha256: &str,
    ) -> Result<String, GridVenueError> {
        self.private
            .sign_recovery_payload_sha256(payload_sha256)
            .map_err(Into::into)
    }

    pub(crate) fn verify_recovery_payload_signature(
        &self,
        payload_sha256: &str,
        signature_sha256: &str,
    ) -> bool {
        self.private
            .verify_recovery_payload_signature(payload_sha256, signature_sha256)
    }

    pub(crate) fn place_market_reduce(
        &mut self,
        command: &MarketReduceCommand,
    ) -> Result<String, GridVenueError> {
        let payload = self.private.place_market_reduce(command)?;
        binance_accepted_order_id(&payload, command.client_order_id.as_str())
    }
}

fn binance_capability_binding(
    symbol: &Symbol,
    api_key_sha256: String,
) -> crate::execution::CapabilityBinding {
    crate::execution::CapabilityBinding {
        exchange: "binance".to_owned(),
        account_binding: "portfolio_margin_um".to_owned(),
        symbol: symbol.to_string(),
        api_key_sha256,
    }
}

impl HedgedGridVenue for BinanceGridVenue {
    fn exchange(&self) -> &'static str {
        "binance"
    }

    fn instrument(&self) -> &Instrument {
        &self.rules.instrument
    }

    fn minimum_quantity(&self) -> Decimal {
        self.rules.minimum_quantity
    }

    fn verify_current_instrument_rules(&mut self) -> Result<(), GridVenueError> {
        let current = self
            .public
            .contract_rules(&self.symbol, self.rules.instrument.generation)
            .map_err(|_| GridVenueError::InstrumentRulesUnavailable)?;
        verify_binance_instrument_rules(&self.rules, &current)
    }

    fn seed_public_generation(&mut self, minimum_generation: u64) -> Result<(), GridVenueError> {
        self.public_market.seed_generation(minimum_generation)
    }

    fn connect_public_stream(&mut self) -> Result<(), GridVenueError> {
        if self.public_stream.is_some() {
            return Ok(());
        }
        let stream =
            BinancePublicStream::connect(&self.symbol, BinancePublicStreamKind::DiffDepth)?;
        let snapshot = self
            .public
            .depth_snapshot(&self.symbol, BINANCE_PUBLIC_DEPTH)?;
        self.pending_public.push_back(GridPublicPayload::new(
            self.public_market.generation,
            GridPublicPayloadSource::RestSnapshot,
            wall_clock_ms()?,
            snapshot,
        )?);
        self.public_stream = Some(stream);
        Ok(())
    }

    fn next_public_payload(&mut self) -> Result<Option<GridPublicPayload>, GridVenueError> {
        if let Some(payload) = self.pending_public.pop_front() {
            return Ok(Some(payload));
        }
        let stream = self
            .public_stream
            .as_mut()
            .ok_or(GridVenueError::PublicNotReady)?;
        let Some(payload) = stream.next_text_when_ready()? else {
            return Ok(None);
        };
        Ok(Some(GridPublicPayload::new(
            self.public_market.generation,
            GridPublicPayloadSource::WebSocketDepth,
            wall_clock_ms()?,
            payload,
        )?))
    }

    fn accept_public_payload(&mut self, payload: GridPublicPayload) -> Result<(), GridVenueError> {
        self.public_market.accept(payload)
    }

    fn reset_public_stream(&mut self) {
        self.public_stream = None;
        self.pending_public.clear();
        let _ = self.public_market.reset();
    }

    fn best_bid_ask(&self, now_ms: u64) -> Result<(Price, Price), GridVenueError> {
        self.public_market.best_bid_ask(now_ms)
    }

    fn readback(&mut self) -> Result<GridVenueReadback, GridVenueError> {
        binance_grid_readback(self.private.grid_readback(&self.symbol)?)
    }

    fn risk_readback(
        &mut self,
        account: &str,
        private_generation: u64,
    ) -> Result<GridRiskReadback, GridVenueError> {
        let readback = self.private.grid_risk_readback(
            &self.symbol,
            account,
            private_generation,
            BINANCE_RISK_MAX_AGE_MS,
        )?;
        Ok(GridRiskReadback {
            raw_private_payloads: readback.raw_private_payloads,
            account: readback.account,
            legs: readback.legs,
        })
    }

    fn risk_readback_client(&self) -> Option<Arc<dyn HedgedGridRiskReadbackClient>> {
        Some(Arc::new(BinanceGridRiskReadbackClient {
            private: Arc::clone(&self.private),
            symbol: self.symbol.clone(),
        }))
    }

    fn connect_private_stream(&mut self) -> Result<(), GridVenueError> {
        if self.private_stream.is_some() {
            return Ok(());
        }
        let listen_key = self.private.create_user_stream()?;
        let mut stream = BinancePrivateStream::connect(&listen_key)?;
        // Connection setup may use the transport timeout, but an idle account socket must yield
        // to public capture and risk supervision. A real fill wakes this read immediately.
        stream.set_read_timeout(BINANCE_PRIVATE_READINESS_TIMEOUT)?;
        let keepalive = BinancePrivateKeepalive::start(Arc::clone(&self.private));
        self.private_listen_key = Some(listen_key);
        self.private_stream = Some(stream);
        self.private_keepalive = Some(keepalive);
        Ok(())
    }

    fn next_private_event(&mut self) -> Result<Option<GridPrivateEvent>, GridVenueError> {
        if self
            .private_keepalive
            .as_ref()
            .is_some_and(BinancePrivateKeepalive::failed)
        {
            return Err(BinancePrivateError::StreamClosed.into());
        }
        let raw_payload = {
            let stream = self
                .private_stream
                .as_mut()
                .ok_or(GridVenueError::PrivateReadbackRequired)?;
            stream.next_text_when_ready()?
        };
        let Some(raw_payload) = raw_payload else {
            return Ok(None);
        };
        let listen_key = self
            .private_listen_key
            .as_ref()
            .ok_or(GridVenueError::PrivateReadbackRequired)?;
        let raw_payload = crate::exchange::binance::sanitize_private_stream_payload_for_transport(
            listen_key,
            raw_payload,
        )?;
        if binance_private_stream_expired(&raw_payload) {
            return Err(BinancePrivateError::StreamClosed.into());
        }
        Ok(Some(binance_private_event(raw_payload, &self.symbol)?))
    }

    fn reset_private_stream(&mut self) {
        self.private_stream = None;
        self.private_listen_key = None;
        self.private_keepalive = None;
    }

    fn mutation_client(&self) -> Arc<dyn HedgedGridMutationClient> {
        Arc::new(BinanceGridMutationClient {
            private: Arc::clone(&self.private),
        })
    }

    fn validate_client_order_id(&self, client_order_id: &str) -> Result<(), GridVenueError> {
        if crate::exchange::binance::client_order_id_is_valid(client_order_id) {
            Ok(())
        } else {
            Err(BinancePrivateError::ClientOrderId.into())
        }
    }

    fn proves_never_dispatched(&self, command: &ExecutionCommand, unknown_reason: &str) -> bool {
        unknown_reason == BinancePrivateError::ClientOrderId.to_string()
            && execution_client_order_id(command)
                .is_some_and(|value| !crate::exchange::binance::client_order_id_is_valid(value))
    }

    fn order_by_client_id(&mut self, client_order_id: &str) -> Result<Order, GridVenueError> {
        let payload = self
            .private
            .order_by_client_id(&self.symbol, client_order_id)?;
        crate::exchange::binance_private::parse_order(&payload, &self.symbol).map_err(Into::into)
    }

    fn verify_post_only_order(&mut self, client_order_id: &str) -> Result<(), GridVenueError> {
        self.private
            .verify_post_only_order_by_client_id(&self.symbol, client_order_id)
            .map_err(Into::into)
    }
}

struct BinanceGridMutationClient {
    private: Arc<BinancePrivateRest>,
}

struct BinanceGridRiskReadbackClient {
    private: Arc<BinancePrivateRest>,
    symbol: Symbol,
}

impl HedgedGridRiskReadbackClient for BinanceGridRiskReadbackClient {
    fn risk_readback(
        &self,
        account: &str,
        private_generation: u64,
    ) -> Result<GridRiskReadback, GridVenueError> {
        let readback = self.private.grid_risk_readback(
            &self.symbol,
            account,
            private_generation,
            BINANCE_RISK_MAX_AGE_MS,
        )?;
        Ok(GridRiskReadback {
            raw_private_payloads: readback.raw_private_payloads,
            account: readback.account,
            legs: readback.legs,
        })
    }
}

impl HedgedGridMutationClient for BinanceGridMutationClient {
    fn place_limit_post_only(&self, command: &OrderCommand) -> Result<String, GridVenueError> {
        let payload = self.private.place_limit_post_only(command)?;
        binance_accepted_order_id(&payload, command.client_order_id.as_str())
    }

    fn place_market(&self, command: &MarketOrderCommand) -> Result<String, GridVenueError> {
        let payload = self.private.place_market(command)?;
        binance_accepted_order_id(&payload, command.client_order_id.as_str())
    }

    fn place_market_reduce(&self, command: &MarketReduceCommand) -> Result<String, GridVenueError> {
        let payload = self.private.place_market_reduce(command)?;
        binance_accepted_order_id(&payload, command.client_order_id.as_str())
    }

    fn cancel_by_client_id(&self, command: &CancelCommand) -> Result<String, GridVenueError> {
        let payload = self.private.cancel_by_client_id(
            &command.owner.symbol,
            command.target_client_order_id.as_str(),
        )?;
        binance_accepted_order_id(&payload, command.target_client_order_id.as_str())
    }

    fn cancel_algo_by_client_id(&self, client_algo_id: &str) -> Result<String, GridVenueError> {
        self.private
            .cancel_algo_by_client_algo_id(client_algo_id)
            .map_err(Into::into)
    }
}

fn binance_accepted_order_id(
    payload: &str,
    expected_client_order_id: &str,
) -> Result<String, GridVenueError> {
    let value: Value =
        serde_json::from_str(payload).map_err(|_| GridVenueError::MutationResponse)?;
    if value.get("clientOrderId").and_then(Value::as_str) != Some(expected_client_order_id) {
        return Err(GridVenueError::MutationResponse);
    }
    match value.get("orderId") {
        Some(Value::String(order_id)) if !order_id.is_empty() => Ok(order_id.clone()),
        Some(Value::Number(order_id)) => Ok(order_id.to_string()),
        _ => Err(GridVenueError::MutationResponse),
    }
}

fn binance_grid_readback(
    readback: BinanceGridPrivateReadback,
) -> Result<GridVenueReadback, GridVenueError> {
    if !readback.normalized.capabilities.can_trade
        || !readback.normalized.capabilities.hedge_position
        || readback.normalized.capabilities.one_way_position
        || readback.normalized.balances.len() != 1
    {
        return Err(GridVenueError::PrivateReadbackIncomplete);
    }
    let balance = readback
        .normalized
        .balances
        .first()
        .cloned()
        .ok_or(GridVenueError::PrivateReadbackIncomplete)?;
    let order_family_readback = GridOrderFamilyReadback::regular_and_algo_adapter_profile(
        readback.normalized.orders.clone(),
        readback.signed_regular_order_payloads,
        readback.algo_orders,
        readback.signed_algo_order_payloads,
    )?;
    Ok(GridVenueReadback {
        raw_private_payloads: readback.raw_private_payloads,
        order_family_readback: Some(order_family_readback),
        balance,
        hedge_position: true,
        positions: readback.normalized.positions,
        orders: readback.normalized.orders,
        fills: readback
            .normalized
            .fills
            .into_iter()
            .map(|fill| GridVenueFill {
                fill,
                client_order_id: FieldState::Missing,
            })
            .collect(),
    })
}

pub(crate) fn binance_private_reconcile_event(raw_payload: String) -> GridPrivateEvent {
    GridPrivateEvent::Reconcile { raw_payload }
}

pub(crate) fn binance_private_event(
    raw_payload: String,
    symbol: &Symbol,
) -> Result<GridPrivateEvent, GridVenueError> {
    let parsed = crate::exchange::binance_private::parse_stream_fill(&raw_payload, symbol)?;
    Ok(match parsed {
        Some(stream) => GridPrivateEvent::Fill {
            fill: stream.fill,
            client_order_id: stream.client_order_id,
            raw_payload,
        },
        None => binance_private_reconcile_event(raw_payload),
    })
}

fn binance_private_stream_expired(raw_payload: &str) -> bool {
    serde_json::from_str::<Value>(raw_payload)
        .ok()
        .and_then(|value| value.get("e").and_then(Value::as_str).map(str::to_owned))
        .is_some_and(|event| event == "listenKeyExpired")
}

const BINANCE_PRIVATE_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30 * 60);

struct BinancePrivateKeepalive {
    stop: Sender<()>,
    failed: Receiver<()>,
    worker: Option<thread::JoinHandle<()>>,
}

impl BinancePrivateKeepalive {
    fn start(private: Arc<BinancePrivateRest>) -> Self {
        let (stop, stop_requested) = mpsc::channel();
        let (failure, failed) = mpsc::channel();
        let worker = thread::spawn(move || {
            loop {
                match stop_requested.recv_timeout(BINANCE_PRIVATE_KEEPALIVE_INTERVAL) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        if private.keepalive_user_stream().is_err() {
                            let _ = failure.send(());
                            break;
                        }
                    }
                }
            }
        });
        Self {
            stop,
            failed,
            worker: Some(worker),
        }
    }

    fn failed(&self) -> bool {
        match self.failed.try_recv() {
            Ok(()) | Err(TryRecvError::Disconnected) => true,
            Err(TryRecvError::Empty) => false,
        }
    }
}

impl Drop for BinancePrivateKeepalive {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

pub(crate) struct GateGridVenue {
    symbol: Symbol,
    rules: GateContractRules,
    public: GatePublicRest,
    public_market: GatePublicMarket,
    public_stream: Option<GatePublicStream>,
    pending_public: VecDeque<GridPublicPayload>,
    private: GatePrivateRest,
    last_user_id: Option<String>,
    stream: Option<GatePrivateStream>,
    pending_events: VecDeque<GridPrivateEvent>,
}

impl GateGridVenue {
    pub(crate) fn production(
        symbol: Symbol,
        instrument_generation: u64,
    ) -> Result<Self, GridVenueError> {
        let public = GatePublicRest::production()?;
        let rules = public.contract_rules(&symbol, instrument_generation)?;
        let public_market = GatePublicMarket::new(&rules)?;
        let private = GatePrivateRest::production(GateCredentials::from_environment()?)?;
        Ok(Self {
            symbol,
            rules,
            public,
            public_market,
            public_stream: None,
            pending_public: VecDeque::new(),
            private,
            last_user_id: None,
            stream: None,
            pending_events: VecDeque::new(),
        })
    }

    pub(crate) fn capability_binding(&self) -> crate::execution::CapabilityBinding {
        crate::execution::CapabilityBinding {
            exchange: "gate".to_owned(),
            account_binding: "usdt_futures_dual".to_owned(),
            symbol: self.symbol.to_string(),
            api_key_sha256: self.private.credentials_api_key_sha256(),
        }
    }

    pub(crate) fn minimum_quantity(&self) -> Decimal {
        self.rules.minimum_quantity()
    }

    pub(crate) fn place_market_reduce(
        &mut self,
        command: &MarketReduceCommand,
    ) -> Result<String, GridVenueError> {
        self.private
            .place_market_reduce(command, &self.rules)
            .map_err(Into::into)
    }
}

impl HedgedGridVenue for GateGridVenue {
    fn exchange(&self) -> &'static str {
        "gate"
    }

    fn instrument(&self) -> &Instrument {
        &self.rules.instrument
    }

    fn minimum_quantity(&self) -> Decimal {
        self.minimum_quantity()
    }

    fn verify_current_instrument_rules(&mut self) -> Result<(), GridVenueError> {
        let current = self
            .public
            .contract_rules(&self.symbol, self.rules.instrument.generation)
            .map_err(|_| GridVenueError::InstrumentRulesUnavailable)?;
        verify_gate_instrument_rules(&self.rules, &current)
    }

    fn seed_public_generation(&mut self, minimum_generation: u64) -> Result<(), GridVenueError> {
        self.public_market.seed_generation(minimum_generation)
    }

    fn connect_public_stream(&mut self) -> Result<(), GridVenueError> {
        if self.public_stream.is_some() {
            return Ok(());
        }
        let stream = GatePublicStream::connect(&self.public_market.binding)?;
        let snapshot_path = gate_orderbook_path(&self.public_market.binding, GATE_PUBLIC_DEPTH)
            .map_err(|_| GridVenueError::PublicPayload)?;
        let snapshot = self.public.order_book_snapshot_raw(&snapshot_path)?;
        self.pending_public.push_back(GridPublicPayload::new(
            self.public_market.generation(),
            GridPublicPayloadSource::RestSnapshot,
            wall_clock_ms()?,
            snapshot,
        )?);
        self.public_stream = Some(stream);
        Ok(())
    }

    fn next_public_payload(&mut self) -> Result<Option<GridPublicPayload>, GridVenueError> {
        if let Some(payload) = self.pending_public.pop_front() {
            return Ok(Some(payload));
        }
        let stream = self
            .public_stream
            .as_mut()
            .ok_or(GridVenueError::PublicNotReady)?;
        let Some(payload) = stream.next_raw_event()? else {
            return Ok(None);
        };
        let source = gate_public_source(&payload)?;
        Ok(Some(GridPublicPayload::new(
            self.public_market.generation(),
            source,
            wall_clock_ms()?,
            payload,
        )?))
    }

    fn accept_public_payload(&mut self, payload: GridPublicPayload) -> Result<(), GridVenueError> {
        self.public_market.accept(payload)
    }

    fn reset_public_stream(&mut self) {
        self.public_stream = None;
        self.pending_public.clear();
        let _ = self.public_market.reset();
    }

    fn best_bid_ask(&self, now_ms: u64) -> Result<(Price, Price), GridVenueError> {
        self.public_market.best_bid_ask(now_ms)
    }

    fn readback(&mut self) -> Result<GridVenueReadback, GridVenueError> {
        let readback =
            self.private
                .readback(&self.symbol, &self.rules)
                .map_err(|error| match error {
                    GateError::Rejected { label } => GateError::PrivateReadbackRejected { label },
                    error => error,
                })?;
        self.last_user_id = Some(readback.user_id.clone());
        gate_grid_readback(readback)
    }

    fn risk_readback(
        &mut self,
        account: &str,
        private_generation: u64,
    ) -> Result<GridRiskReadback, GridVenueError> {
        let readback =
            self.private
                .risk_readback(&self.symbol, &self.rules, account, private_generation)?;
        Ok(GridRiskReadback {
            raw_private_payloads: readback.raw_payloads,
            account: readback.account,
            legs: readback.legs,
        })
    }

    fn risk_readback_client(&self) -> Option<Arc<dyn HedgedGridRiskReadbackClient>> {
        Some(Arc::new(GateGridRiskReadbackClient {
            private: self.private.clone(),
            rules: self.rules.clone(),
            symbol: self.symbol.clone(),
        }))
    }

    fn connect_private_stream(&mut self) -> Result<(), GridVenueError> {
        if self.stream.is_some() {
            return Ok(());
        }
        let user_id = self
            .last_user_id
            .as_deref()
            .ok_or(GridVenueError::PrivateReadbackRequired)?;
        self.stream = Some(self.private.connect_private_stream(user_id, &self.symbol)?);
        Ok(())
    }

    fn next_private_event(&mut self) -> Result<Option<GridPrivateEvent>, GridVenueError> {
        if let Some(event) = self.pending_events.pop_front() {
            return Ok(Some(event));
        }
        let stream = self
            .stream
            .as_mut()
            .ok_or(GridVenueError::PrivateReadbackRequired)?;
        let Some(event) = stream.next_event_when_ready()? else {
            return Ok(None);
        };
        match event {
            GatePrivateEvent::Fill { value, raw_payload } => {
                let values = value.as_array().cloned().unwrap_or_else(|| vec![value]);
                for value in values {
                    let fill =
                        crate::exchange::gate::parse_fill(&value, &self.symbol, &self.rules)?;
                    self.pending_events.push_back(GridPrivateEvent::Fill {
                        fill,
                        client_order_id: crate::exchange::gate::parse_fill_client_order_id(&value)?,
                        raw_payload: raw_payload.clone(),
                    });
                }
            }
            GatePrivateEvent::Order { raw_payload }
            | GatePrivateEvent::Position { raw_payload }
            | GatePrivateEvent::Balance { raw_payload } => {
                self.pending_events
                    .push_back(GridPrivateEvent::Reconcile { raw_payload });
            }
        }
        self.pending_events
            .pop_front()
            .map_or(Ok(None), |event| Ok(Some(event)))
    }

    fn reset_private_stream(&mut self) {
        self.stream = None;
        self.pending_events.clear();
    }

    fn mutation_client(&self) -> Arc<dyn HedgedGridMutationClient> {
        Arc::new(GateGridMutationClient {
            private: self.private.clone(),
            rules: self.rules.clone(),
        })
    }

    fn validate_client_order_id(&self, client_order_id: &str) -> Result<(), GridVenueError> {
        if crate::exchange::gate::client_order_id_is_valid(client_order_id) {
            Ok(())
        } else {
            Err(GateError::ClientOrderId.into())
        }
    }

    fn proves_never_dispatched(&self, command: &ExecutionCommand, unknown_reason: &str) -> bool {
        unknown_reason == GateError::ClientOrderId.to_string()
            && execution_client_order_id(command)
                .is_some_and(|value| !crate::exchange::gate::client_order_id_is_valid(value))
    }

    fn order_by_client_id(&mut self, client_order_id: &str) -> Result<Order, GridVenueError> {
        let payload = self
            .private
            .order_by_client_id(&self.symbol, client_order_id)?;
        crate::exchange::gate::parse_order(&payload, &self.symbol, &self.rules).map_err(Into::into)
    }

    fn verify_post_only_order(&mut self, client_order_id: &str) -> Result<(), GridVenueError> {
        self.private
            .verify_post_only_order_by_client_id(&self.symbol, client_order_id)
            .map_err(Into::into)
    }
}

pub(crate) fn gate_grid_readback(
    readback: GatePrivateReadback,
) -> Result<GridVenueReadback, GridVenueError> {
    let order_family_readback = GridOrderFamilyReadback::regular_only_adapter_profile(
        readback.orders.clone(),
        readback.signed_regular_order_payloads.clone(),
    )?;
    Ok(GridVenueReadback {
        raw_private_payloads: readback.raw_payloads,
        order_family_readback: Some(order_family_readback),
        balance: readback.balance,
        hedge_position: readback.dual_position_mode,
        positions: readback.positions,
        orders: readback.orders,
        fills: readback
            .fills
            .into_iter()
            .map(|fill| GridVenueFill {
                fill: fill.fill,
                client_order_id: fill.client_order_id,
            })
            .collect(),
    })
}

struct GateGridMutationClient {
    private: GatePrivateRest,
    rules: GateContractRules,
}

struct GateGridRiskReadbackClient {
    private: GatePrivateRest,
    rules: GateContractRules,
    symbol: Symbol,
}

impl HedgedGridRiskReadbackClient for GateGridRiskReadbackClient {
    fn risk_readback(
        &self,
        account: &str,
        private_generation: u64,
    ) -> Result<GridRiskReadback, GridVenueError> {
        let readback =
            self.private
                .risk_readback(&self.symbol, &self.rules, account, private_generation)?;
        Ok(GridRiskReadback {
            raw_private_payloads: readback.raw_payloads,
            account: readback.account,
            legs: readback.legs,
        })
    }
}

impl HedgedGridMutationClient for GateGridMutationClient {
    fn place_limit_post_only(&self, command: &OrderCommand) -> Result<String, GridVenueError> {
        self.private
            .place_limit_post_only(command, &self.rules)
            .map_err(Into::into)
    }

    fn place_market(&self, command: &MarketOrderCommand) -> Result<String, GridVenueError> {
        self.private
            .place_market(command, &self.rules)
            .map_err(Into::into)
    }

    fn place_market_reduce(&self, command: &MarketReduceCommand) -> Result<String, GridVenueError> {
        self.private
            .place_market_reduce(command, &self.rules)
            .map_err(Into::into)
    }

    fn cancel_by_client_id(&self, command: &CancelCommand) -> Result<String, GridVenueError> {
        self.private
            .cancel_by_client_id(
                &command.owner.symbol,
                command.target_client_order_id.as_str(),
            )
            .map_err(Into::into)
    }
}

pub(crate) struct BitgetGridVenue {
    symbol: Symbol,
    rules: BitgetContractRules,
    public: BitgetPublicRest,
    public_market: BitgetPublicMarket,
    public_stream: Option<BitgetPublicStream>,
    pending_public: VecDeque<GridPublicPayload>,
    private: BitgetPrivateRest,
    stream: Option<BitgetPrivateStream>,
    pending_events: VecDeque<GridPrivateEvent>,
    fill_history_start_ms: Option<u64>,
}

impl BitgetGridVenue {
    pub(crate) fn production(
        symbol: Symbol,
        instrument_generation: u64,
    ) -> Result<Self, GridVenueError> {
        let public = BitgetPublicRest::production()?;
        let rules = public.contract_rules(&symbol, instrument_generation)?;
        let public_market = BitgetPublicMarket::new(symbol.clone());
        let private = BitgetPrivateRest::production(BitgetCredentials::from_environment()?)?;
        Ok(Self {
            symbol,
            rules,
            public,
            public_market,
            public_stream: None,
            pending_public: VecDeque::new(),
            private,
            stream: None,
            pending_events: VecDeque::new(),
            fill_history_start_ms: None,
        })
    }

    pub(crate) fn capability_binding(&self) -> crate::execution::CapabilityBinding {
        crate::execution::CapabilityBinding {
            exchange: "bitget".to_owned(),
            account_binding: "uta_usdt_futures_hedge".to_owned(),
            symbol: self.symbol.to_string(),
            api_key_sha256: self.private.credentials_api_key_sha256(),
        }
    }

    pub(crate) fn minimum_quantity(&self) -> Decimal {
        self.rules.minimum_quantity
    }

    pub(crate) fn place_market_reduce(
        &mut self,
        command: &MarketReduceCommand,
    ) -> Result<String, GridVenueError> {
        self.private
            .place_market_reduce(command, &self.rules)
            .map_err(Into::into)
    }
}

impl HedgedGridVenue for BitgetGridVenue {
    fn exchange(&self) -> &'static str {
        "bitget"
    }

    fn instrument(&self) -> &Instrument {
        &self.rules.instrument
    }

    fn minimum_quantity(&self) -> Decimal {
        self.minimum_quantity()
    }

    fn verify_current_instrument_rules(&mut self) -> Result<(), GridVenueError> {
        let current = self
            .public
            .contract_rules(&self.symbol, self.rules.instrument.generation)
            .map_err(|_| GridVenueError::InstrumentRulesUnavailable)?;
        verify_bitget_instrument_rules(&self.rules, &current)
    }

    fn seed_public_generation(&mut self, minimum_generation: u64) -> Result<(), GridVenueError> {
        self.public_market.seed_generation(minimum_generation)
    }

    fn connect_public_stream(&mut self) -> Result<(), GridVenueError> {
        if self.public_stream.is_some() {
            return Ok(());
        }
        let stream = BitgetPublicStream::connect(&self.symbol)?;
        let generation = self.public_market.generation();
        let orderbook_path =
            bitget_orderbook_path(&self.symbol, 50).map_err(|_| GridVenueError::PublicPayload)?;
        let ticker_path =
            bitget_ticker_path(&self.symbol).map_err(|_| GridVenueError::PublicPayload)?;
        let snapshot = self.public.market_payload_raw(&orderbook_path)?;
        let ticker = self.public.market_payload_raw(&ticker_path)?;
        let received_at_ms = wall_clock_ms()?;
        self.pending_public.push_back(GridPublicPayload::new(
            generation,
            GridPublicPayloadSource::RestSnapshot,
            received_at_ms,
            snapshot,
        )?);
        self.pending_public.push_back(GridPublicPayload::new(
            generation,
            GridPublicPayloadSource::RestTicker,
            received_at_ms,
            ticker,
        )?);
        self.public_stream = Some(stream);
        Ok(())
    }

    fn next_public_payload(&mut self) -> Result<Option<GridPublicPayload>, GridVenueError> {
        if let Some(payload) = self.pending_public.pop_front() {
            return Ok(Some(payload));
        }
        let stream = self
            .public_stream
            .as_mut()
            .ok_or(GridVenueError::PublicNotReady)?;
        let Some(payload) = stream.next_raw_event()? else {
            return Ok(None);
        };
        let source = bitget_public_source(&payload)?;
        Ok(Some(GridPublicPayload::new(
            self.public_market.generation(),
            source,
            wall_clock_ms()?,
            payload,
        )?))
    }

    fn accept_public_payload(&mut self, payload: GridPublicPayload) -> Result<(), GridVenueError> {
        self.public_market.accept(payload)
    }

    fn reset_public_stream(&mut self) {
        self.public_stream = None;
        self.pending_public.clear();
        self.public_market.reset();
    }

    fn best_bid_ask(&self, now_ms: u64) -> Result<(Price, Price), GridVenueError> {
        self.public_market.best_bid_ask(now_ms)
    }

    fn set_fill_history_start_ms(&mut self, start_ms: u64) {
        self.fill_history_start_ms = Some(start_ms);
    }

    fn readback(&mut self) -> Result<GridVenueReadback, GridVenueError> {
        let readback =
            self.private
                .readback(&self.symbol, &self.rules, self.fill_history_start_ms)?;
        bitget_grid_readback(readback)
    }

    fn risk_readback(
        &mut self,
        account: &str,
        private_generation: u64,
    ) -> Result<GridRiskReadback, GridVenueError> {
        let readback = self
            .private
            .risk_readback(&self.symbol, account, private_generation)?;
        Ok(GridRiskReadback {
            raw_private_payloads: readback.raw_payloads,
            account: readback.account,
            legs: readback.legs,
        })
    }

    fn risk_readback_client(&self) -> Option<Arc<dyn HedgedGridRiskReadbackClient>> {
        Some(Arc::new(BitgetGridRiskReadbackClient {
            private: self.private.clone(),
            symbol: self.symbol.clone(),
        }))
    }

    fn connect_private_stream(&mut self) -> Result<(), GridVenueError> {
        if self.stream.is_none() {
            self.stream = Some(self.private.connect_private_stream(&self.symbol)?);
        }
        Ok(())
    }

    fn next_private_event(&mut self) -> Result<Option<GridPrivateEvent>, GridVenueError> {
        if let Some(event) = self.pending_events.pop_front() {
            return Ok(Some(event));
        }
        let stream = self
            .stream
            .as_mut()
            .ok_or(GridVenueError::PrivateReadbackRequired)?;
        let Some(raw_payload) = stream.next_raw_event()? else {
            return Ok(None);
        };
        let fills =
            crate::exchange::bitget::parse_private_fill_message(&raw_payload, &self.symbol)?;
        if fills.is_empty() {
            return Ok(Some(GridPrivateEvent::Reconcile { raw_payload }));
        }
        for fill in fills {
            self.pending_events.push_back(GridPrivateEvent::Fill {
                fill: fill.fill,
                client_order_id: fill.client_order_id,
                raw_payload: raw_payload.clone(),
            });
        }
        Ok(self.pending_events.pop_front())
    }

    fn reset_private_stream(&mut self) {
        self.stream = None;
        self.pending_events.clear();
    }

    fn mutation_client(&self) -> Arc<dyn HedgedGridMutationClient> {
        Arc::new(BitgetGridMutationClient {
            private: self.private.clone(),
            rules: self.rules.clone(),
        })
    }

    fn validate_client_order_id(&self, client_order_id: &str) -> Result<(), GridVenueError> {
        if crate::exchange::bitget::client_order_id_is_valid(client_order_id) {
            Ok(())
        } else {
            Err(BitgetError::ClientOrderId.into())
        }
    }

    fn proves_never_dispatched(&self, command: &ExecutionCommand, unknown_reason: &str) -> bool {
        unknown_reason == BitgetError::ClientOrderId.to_string()
            && execution_client_order_id(command)
                .is_some_and(|value| !crate::exchange::bitget::client_order_id_is_valid(value))
    }

    fn order_by_client_id(&mut self, client_order_id: &str) -> Result<Order, GridVenueError> {
        let payload = self
            .private
            .order_by_client_id(&self.symbol, client_order_id)?;
        crate::exchange::bitget::parse_order(&payload, &self.symbol).map_err(Into::into)
    }

    fn verify_post_only_order(&mut self, client_order_id: &str) -> Result<(), GridVenueError> {
        self.private
            .verify_post_only_order_by_client_id(&self.symbol, client_order_id)
            .map_err(Into::into)
    }
}

fn bitget_grid_readback(
    readback: crate::exchange::bitget::BitgetPrivateReadback,
) -> Result<GridVenueReadback, GridVenueError> {
    let order_family_readback = GridOrderFamilyReadback::regular_only_adapter_profile(
        readback.orders.clone(),
        readback.signed_regular_order_payloads.clone(),
    )?;
    Ok(GridVenueReadback {
        raw_private_payloads: readback.raw_payloads,
        order_family_readback: Some(order_family_readback),
        balance: readback.balance,
        hedge_position: readback.hedge_position,
        positions: readback.positions,
        orders: readback.orders,
        fills: readback
            .fills
            .into_iter()
            .map(|fill| GridVenueFill {
                fill: fill.fill,
                client_order_id: fill.client_order_id,
            })
            .collect(),
    })
}

struct BitgetGridMutationClient {
    private: BitgetPrivateRest,
    rules: BitgetContractRules,
}

struct BitgetGridRiskReadbackClient {
    private: BitgetPrivateRest,
    symbol: Symbol,
}

impl HedgedGridRiskReadbackClient for BitgetGridRiskReadbackClient {
    fn risk_readback(
        &self,
        account: &str,
        private_generation: u64,
    ) -> Result<GridRiskReadback, GridVenueError> {
        let readback = self
            .private
            .risk_readback(&self.symbol, account, private_generation)?;
        Ok(GridRiskReadback {
            raw_private_payloads: readback.raw_payloads,
            account: readback.account,
            legs: readback.legs,
        })
    }
}

impl HedgedGridMutationClient for BitgetGridMutationClient {
    fn place_limit_post_only(&self, command: &OrderCommand) -> Result<String, GridVenueError> {
        self.private
            .place_limit_post_only(command, &self.rules)
            .map_err(Into::into)
    }

    fn place_market(&self, command: &MarketOrderCommand) -> Result<String, GridVenueError> {
        self.private
            .place_market(command, &self.rules)
            .map_err(Into::into)
    }

    fn place_market_reduce(&self, command: &MarketReduceCommand) -> Result<String, GridVenueError> {
        self.private
            .place_market_reduce(command, &self.rules)
            .map_err(Into::into)
    }

    fn cancel_by_client_id(&self, command: &CancelCommand) -> Result<String, GridVenueError> {
        self.private
            .cancel_by_client_id(command.target_client_order_id.as_str())
            .map_err(Into::into)
    }
}

fn execution_client_order_id(command: &ExecutionCommand) -> Option<&str> {
    match command {
        ExecutionCommand::PlaceLimit(command) => Some(command.client_order_id.as_str()),
        ExecutionCommand::PlaceMarket(command) => Some(command.client_order_id.as_str()),
        ExecutionCommand::MarketReduce(command) => Some(command.client_order_id.as_str()),
        ExecutionCommand::Cancel(command) => Some(command.target_client_order_id.as_str()),
        ExecutionCommand::StopMarketCloseAll(_) | ExecutionCommand::StopMarketFullPosition(_) => {
            None
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum GridVenueError {
    #[error(transparent)]
    Binance(#[from] BinanceError),
    #[error(transparent)]
    BinancePublic(#[from] BinancePublicError),
    #[error(transparent)]
    BinancePrivate(#[from] BinancePrivateError),
    #[error(transparent)]
    BinancePrivateReadback(#[from] BinancePrivateReadbackError),
    #[error(transparent)]
    BinancePrivateParse(#[from] crate::exchange::binance_private::PrivateParseError),
    #[error(transparent)]
    BinanceRules(#[from] BinanceRulesError),
    #[error(transparent)]
    Bitget(#[from] BitgetError),
    #[error(transparent)]
    Gate(#[from] GateError),
    #[error("a complete private readback is required before connecting the private stream")]
    PrivateReadbackRequired,
    #[error("signed private readback did not prove one tradable Portfolio Margin Hedge account")]
    PrivateReadbackIncomplete,
    #[error("current stage-7 instrument rules could not be obtained and proven complete")]
    #[allow(dead_code)] // Constructed once the Stage-7 admission caller is wired.
    InstrumentRulesUnavailable,
    #[error("exchange adapter does not support a signed risk readback")]
    RiskReadbackUnsupported,
    #[error("exchange adapter does not support the dedicated market-reduction command")]
    MarketReduceUnsupported,
    #[error("exchange adapter does not support exact external Algo cancellation")]
    MutationUnsupported,
    #[error("current stage-7 instrument rules differ from the startup snapshot")]
    #[allow(dead_code)] // Constructed once the Stage-7 admission caller is wired.
    InstrumentRulesDrift,
    #[error("stage-7 public market is not synchronized and fresh")]
    PublicNotReady,
    #[error("stage-7 public market payload is malformed, mismatched, or has a sequence gap")]
    PublicPayload,
    #[error("stage-7 public market payload could not be normalized")]
    PublicParse,
    #[error("stage-7 public market sequence bridge rejected the frame")]
    PublicSequence,
    #[error("stage-7 exchange mutation response omitted or mismatched its exact order identity")]
    MutationResponse,
    #[error(
        "stage-7 public market book rejected delta generation={delta_generation} first={first_sequence} sequence={sequence} after generation={book_generation:?} sequence={book_sequence:?}"
    )]
    PublicBook {
        book_generation: Option<u64>,
        book_sequence: Option<u64>,
        delta_generation: u64,
        first_sequence: u64,
        sequence: u64,
    },
    #[error("stage-7 public market clock is unavailable")]
    Clock,
}

pub(crate) fn physical_notional(quantity: Decimal, price: Price) -> Decimal {
    quantity * price.value()
}

fn verify_binance_instrument_rules(
    startup: &BinanceContractRules,
    current: &BinanceContractRules,
) -> Result<(), GridVenueError> {
    if startup == current {
        Ok(())
    } else {
        Err(GridVenueError::InstrumentRulesDrift)
    }
}

#[allow(dead_code)] // Called by the production trait method once its admission call site is wired.
fn verify_gate_instrument_rules(
    startup: &GateContractRules,
    current: &GateContractRules,
) -> Result<(), GridVenueError> {
    if startup == current {
        Ok(())
    } else {
        Err(GridVenueError::InstrumentRulesDrift)
    }
}

#[allow(dead_code)] // Called by the production trait method once its admission call site is wired.
fn verify_bitget_instrument_rules(
    startup: &BitgetContractRules,
    current: &BitgetContractRules,
) -> Result<(), GridVenueError> {
    if startup == current {
        Ok(())
    } else {
        Err(GridVenueError::InstrumentRulesDrift)
    }
}

fn gate_public_source(payload: &str) -> Result<GridPublicPayloadSource, GridVenueError> {
    let value =
        serde_json::from_str::<Value>(payload).map_err(|_| GridVenueError::PublicPayload)?;
    let channel = value
        .as_object()
        .and_then(|object| object.get("channel"))
        .and_then(Value::as_str)
        .ok_or(GridVenueError::PublicPayload)?;
    match channel {
        "futures.order_book_update" => Ok(GridPublicPayloadSource::WebSocketDepth),
        "futures.book_ticker" => Ok(GridPublicPayloadSource::WebSocketBbo),
        "futures.tickers" => Ok(GridPublicPayloadSource::WebSocketMark),
        "futures.trades" => Ok(GridPublicPayloadSource::WebSocketTrade),
        _ => Err(GridVenueError::PublicPayload),
    }
}

fn bitget_public_source(payload: &str) -> Result<GridPublicPayloadSource, GridVenueError> {
    let value =
        serde_json::from_str::<Value>(payload).map_err(|_| GridVenueError::PublicPayload)?;
    let topic = value
        .as_object()
        .and_then(|object| object.get("arg"))
        .and_then(Value::as_object)
        .and_then(|argument| argument.get("topic"))
        .and_then(Value::as_str)
        .ok_or(GridVenueError::PublicPayload)?;
    match topic {
        "books" => Ok(GridPublicPayloadSource::WebSocketDepth),
        "publicTrade" => Ok(GridPublicPayloadSource::WebSocketTrade),
        _ => Err(GridVenueError::PublicPayload),
    }
}

fn wall_clock_ms() -> Result<u64, GridVenueError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| GridVenueError::Clock)
        .and_then(|duration| u64::try_from(duration.as_millis()).map_err(|_| GridVenueError::Clock))
}

#[cfg(test)]
#[path = "adapter_tests.rs"]
mod adapter_tests;

#[cfg(test)]
#[path = "event_time_tests.rs"]
mod event_time_tests;
