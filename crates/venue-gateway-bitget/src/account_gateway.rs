use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    str,
    time::{SystemTime, UNIX_EPOCH},
};

use rust_decimal::Decimal;
use serde_json::Value;
use tokio::runtime::{Builder, Runtime};
use venue_domain::domain::{
    Amount, ExecutionCommand, FieldState, Fill, LimitTimeInForce, NativeOrderFamily, OrderCommand,
    OrderSide, OrderState, PositionSide, Price, Symbol,
};
use venue_execution::{
    AccountDispatchPermit, AccountGatewayResult, AccountHostValidationError,
    AccountInstrumentIdentity, AccountLimitNormalizationIntent, AccountPhysicalGateway,
    AccountPricedLimitIntent, AccountRecoveryOutcome, AccountRecoveryReport,
    AccountRecoveryRequest, AccountRiskEvidence, SignedAccountBalance, SignedAccountOrderFact,
    SignedAccountPositionFact, SignedAccountPositionMode, SignedAccountSnapshot, SignedUnknownFact,
    SignedUnknownResult, command_matches_readback_order,
};
use venue_gateway_api::GatewayBinding;

use crate::{
    BITGET_ORDER_PROFILE_VERSION, BitgetAccountBinding, BitgetConfig, BitgetCredentials,
    BitgetExactOrderReadback, BitgetHttpTransport, BitgetMutationKind, BitgetMutationOutcome,
    BitgetNodeReadbackCandidate, BitgetOrderFamilyEvidence, BitgetOrderFamilyScope,
    BitgetPrivateWsTransport, BitgetRawPrivateFrame, BitgetTransportError, BitgetTransportLimits,
    BitgetUnsupportedEvidence, build_account_wide_open_orders_read_request,
    build_account_wide_positions_read_request, build_ack_readback_request,
    build_fills_read_request, build_unknown_recovery_readback_request,
    connect_authenticated_private_ws,
    instrument::{BitgetInstrumentRules, BitgetRawInstrumentPayload, parse_instrument_rules},
    prepare_node_mutation,
    private::{
        BITGET_MAX_FILL_HISTORY_WINDOW_MS, BITGET_MAX_PRIVATE_PAGES, BITGET_MAX_STREAM_FILLS,
        parse_stream_fills,
    },
    public::{BitgetPublicSource, BitgetRawPublicPayload, BitgetTickerEvent, parse_rest_ticker},
    settle_ack_readback, settle_unknown_readback,
};

const MAX_LIMIT_TICKER_AGE_MS: u64 = 5_000;

/// The Bitget UTA account writer. Startup and every dispatched command collect fresh signed
/// facts; the only physical mutation consumes the runtime host's linear permit.
pub struct BitgetAccountGateway {
    runtime: Runtime,
    config: BitgetConfig,
    credentials: BitgetCredentials,
    transport: BitgetHttpTransport,
    rules: BitgetInstrumentRules,
    rules_catalog: BTreeMap<Symbol, BitgetInstrumentRules>,
    private: BitgetNodeReadbackCandidate,
    /// The most recent complete signed snapshot attempt admitted by the account host. This is not
    /// a websocket connection generation and is the only generation attached to stream fills.
    private_generation: u64,
    next_attempt_id: u64,
    private_stream: Option<BitgetPrivateWsTransport>,
    private_stream_attempt: Option<u64>,
    pending_private_fills: VecDeque<BitgetPrivateFillEvent>,
}

/// Sanitized execution evidence from one UTA `fill` update. The authenticated websocket frame
/// and its native payload remain inside the adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetPrivateFillEvent {
    /// The adapter-local signed snapshot attempt that authorized this socket's delivery.
    pub source_private_generation: u64,
    pub received_at_ms: u64,
    pub fill: Fill,
    pub client_order_id: FieldState<String>,
}

impl BitgetAccountGateway {
    pub fn connect_from_environment(
        binding: GatewayBinding,
        limits: BitgetTransportLimits,
    ) -> Result<Self, BitgetAccountGatewayError> {
        BitgetAccountBinding::UtaUsdtFuturesHedge
            .validate_gateway_binding(&binding)
            .map_err(|_| BitgetAccountGatewayError::Binding)?;
        let credentials = BitgetCredentials::from_environment()
            .map_err(|_| BitgetAccountGatewayError::Credentials)?;
        let runtime = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|_| BitgetAccountGatewayError::Runtime)?;
        let generation = now_ms()?;
        let transport = BitgetHttpTransport::new(binding, generation, limits)
            .map_err(BitgetAccountGatewayError::Transport)?;
        let rules = runtime.block_on(fetch_rules(&transport, generation))?;
        let private = runtime.block_on(fetch_private(&transport, &credentials, &rules, 1))?;
        let rules_catalog = BTreeMap::from([(rules.canonical_symbol().clone(), rules.clone())]);
        Ok(Self {
            runtime,
            config: BitgetConfig::for_mode(transport.config().mode()),
            credentials,
            transport,
            rules,
            rules_catalog,
            private,
            private_generation: 0,
            next_attempt_id: 2,
            private_stream: None,
            private_stream_attempt: None,
            pending_private_fills: VecDeque::new(),
        })
    }

    /// Opens one authenticated UTA stream only after a complete signed snapshot has installed an
    /// exact attempt. Any refresh, disconnect, malformed frame, or bounded-queue overflow tears
    /// the stream down; the caller must wait for another signed snapshot before retrying.
    pub fn poll_private_fill(
        &mut self,
    ) -> Result<Option<BitgetPrivateFillEvent>, BitgetAccountGatewayError> {
        if let Some(fill) = self.pending_private_fills.pop_front() {
            return Ok(Some(fill));
        }
        if self.private_generation == 0
            || self.private.private_generation() != self.private_generation
        {
            self.poison_private_stream();
            return Err(BitgetAccountGatewayError::PrivateStream);
        }
        if self.private_stream.is_none() {
            let stream = match self.runtime.block_on(connect_authenticated_private_ws(
                self.transport_binding().clone(),
                &self.credentials,
                now_ms()?,
                self.transport.limits(),
            )) {
                Ok(stream) => stream,
                Err(error) => {
                    self.poison_private_stream();
                    return Err(BitgetAccountGatewayError::Transport(error));
                }
            };
            self.private_stream = Some(stream);
            self.private_stream_attempt = Some(self.private_generation);
        }
        if self.private_stream_attempt != Some(self.private_generation) {
            self.poison_private_stream();
            return Err(BitgetAccountGatewayError::PrivateStream);
        }
        let frame = match self.private_stream.as_mut() {
            Some(stream) => self.runtime.block_on(stream.next_frame()),
            None => return Err(BitgetAccountGatewayError::PrivateStream),
        };
        let events = match frame {
            Ok(frame) => normalize_private_stream_fill(
                frame,
                self.transport_binding(),
                self.private_generation,
            ),
            Err(error) => Err(BitgetAccountGatewayError::Transport(error)),
        };
        let events = match events {
            Ok(events) => events,
            Err(error) => {
                self.poison_private_stream();
                return Err(error);
            }
        };
        if events.len() > BITGET_MAX_STREAM_FILLS
            || self
                .pending_private_fills
                .len()
                .saturating_add(events.len())
                > BITGET_MAX_STREAM_FILLS
        {
            self.poison_private_stream();
            return Err(BitgetAccountGatewayError::PrivateStream);
        }
        self.pending_private_fills.extend(events);
        Ok(self.pending_private_fills.pop_front())
    }

    fn next_attempt_id(&mut self) -> Result<u64, BitgetAccountGatewayError> {
        let current = self.next_attempt_id;
        self.next_attempt_id = self
            .next_attempt_id
            .checked_add(1)
            .ok_or(BitgetAccountGatewayError::Attempt)?;
        Ok(current)
    }

    fn binding_for(&self, symbol: &Symbol) -> GatewayBinding {
        let mut binding = self.transport_binding().clone();
        binding.symbol = symbol.clone();
        binding
    }

    fn refresh_rules_for_symbols<I>(&mut self, symbols: I) -> Result<(), BitgetAccountGatewayError>
    where
        I: IntoIterator<Item = Symbol>,
    {
        let mut required = self.rules_catalog.keys().cloned().collect::<BTreeSet<_>>();
        required.extend(symbols);
        required.insert(self.transport_binding().symbol.clone());
        let generation = self.transport.generation();
        let mut refreshed = BTreeMap::new();
        for symbol in required {
            let binding = self.binding_for(&symbol);
            let rules =
                self.runtime
                    .block_on(fetch_rules_for(&self.transport, &binding, generation))?;
            if rules.canonical_symbol() != &symbol {
                return Err(BitgetAccountGatewayError::Rules);
            }
            refreshed.insert(symbol, rules);
        }
        self.rules_catalog = refreshed;
        self.rules = catalog_rule(&self.rules_catalog, &self.transport_binding().symbol)?;
        Ok(())
    }

    fn registered_rules(
        &self,
        symbol: &Symbol,
    ) -> Result<BitgetInstrumentRules, BitgetAccountGatewayError> {
        catalog_rule(&self.rules_catalog, symbol)
    }

    fn refresh_private_for(&mut self, symbol: &Symbol) -> Result<(), BitgetAccountGatewayError> {
        // Even a rejected refresh proves the old candidate is no longer safe to label stream
        // facts. Only a later complete Host snapshot can reinstall this authorization.
        self.poison_private_stream();
        if !self.rules_catalog.contains_key(symbol) {
            return Err(BitgetAccountGatewayError::Rules);
        }
        self.refresh_rules_for_symbols(std::iter::empty())?;
        let rules = self.registered_rules(symbol)?;
        let attempt = self.next_attempt_id()?;
        let binding = self.binding_for(symbol);
        let private = self.runtime.block_on(fetch_private_for(
            &self.transport,
            &self.credentials,
            &binding,
            &rules,
            attempt,
        ))?;
        if private.private_generation() != attempt {
            return Err(BitgetAccountGatewayError::Readback);
        }
        self.private = private;
        Ok(())
    }

    fn clear_private_stream(&mut self) {
        self.private_stream = None;
        self.private_stream_attempt = None;
        self.pending_private_fills.clear();
    }

    fn poison_private_stream(&mut self) {
        self.clear_private_stream();
        self.private_generation = 0;
    }

    fn dispatch_permit(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
        if permit.binding() != self.transport_binding() {
            return rejected("bitget_preflight_failed");
        }
        let symbol = permit.command().mutation_owner().symbol.clone();
        if self.refresh_private_for(&symbol).is_err() {
            return rejected("bitget_preflight_failed");
        }
        let rules = match self.registered_rules(&symbol) {
            Ok(value) => value,
            Err(_) => return rejected("bitget_symbol_unconfigured"),
        };
        let attempt = match self.next_attempt_id() {
            Ok(value) => value,
            Err(_) => return rejected("bitget_attempt_exhausted"),
        };
        let now = match now_ms() {
            Ok(value) => value,
            Err(_) => return rejected("bitget_clock"),
        };
        let prepared = match prepare_node_mutation(
            &self.private,
            &rules,
            &self.config,
            permit.command(),
            attempt,
            now,
        ) {
            Ok(value) => value,
            Err(_) => return rejected("bitget_intent_rejected"),
        };
        match self.runtime.block_on(self.transport.execute_mutation_once(
            &self.credentials,
            prepared.into_mutation(),
            now,
        )) {
            Ok(BitgetMutationOutcome::Acknowledged(ack)) => {
                let request = match build_ack_readback_request(&ack) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let readback = match now_ms().ok().and_then(|timestamp| {
                    self.runtime
                        .block_on(self.transport.execute_exact_readback(
                            &self.credentials,
                            request,
                            timestamp,
                        ))
                        .ok()
                }) {
                    Some(value) => value,
                    None => return AccountGatewayResult::Unknown,
                };
                match settle_ack_readback(&ack, &readback) {
                    Ok(settlement) => match settlement.order {
                        Some(order) => AccountGatewayResult::Accepted {
                            venue_order_id: order.order_id,
                        },
                        None => AccountGatewayResult::Unknown,
                    },
                    Err(_) => AccountGatewayResult::Unknown,
                }
            }
            Ok(BitgetMutationOutcome::Rejected) => rejected("bitget_venue_rejected"),
            // `execute_mutation_once` consumes the request. WAL UNKNOWN recovery below performs
            // an exact signed lookup only and never rebuilds this mutation.
            Ok(BitgetMutationOutcome::Unknown(_)) | Err(_) => AccountGatewayResult::Unknown,
        }
    }

    fn transport_binding(&self) -> &GatewayBinding {
        self.transport.binding()
    }
}

impl AccountPhysicalGateway for BitgetAccountGateway {
    type Error = BitgetAccountGatewayError;

    fn binding(&self) -> &GatewayBinding {
        self.transport_binding()
    }

    fn current_instrument(
        &mut self,
    ) -> Result<AccountInstrumentIdentity, AccountHostValidationError> {
        self.current_instrument_for(&self.transport_binding().symbol.clone())
    }

    fn current_instrument_for(
        &mut self,
        symbol: &Symbol,
    ) -> Result<AccountInstrumentIdentity, AccountHostValidationError> {
        if !self.rules_catalog.contains_key(symbol) {
            return Err(AccountHostValidationError::Instrument);
        }
        self.refresh_rules_for_symbols(std::iter::empty())
            .map_err(|_| AccountHostValidationError::Instrument)?;
        let current = self
            .registered_rules(symbol)
            .map_err(|_| AccountHostValidationError::Instrument)?;
        let instrument = current.snapshot.metadata.instrument.clone();
        instrument
            .validate()
            .map_err(|_| AccountHostValidationError::Instrument)?;
        Ok(AccountInstrumentIdentity {
            identity: instrument.identity(),
            rules_generation: instrument.generation,
        })
    }

    fn reconcile(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<AccountRecoveryReport, Self::Error> {
        if request.binding() != self.transport_binding() {
            return Err(BitgetAccountGatewayError::Binding);
        }
        self.refresh_rules_for_symbols(request.configured_symbols().iter().cloned())?;
        let observed_at_ms = now_ms()?;
        let readback_attempt = self.next_attempt_id()?;
        let mut outcomes = Vec::with_capacity(request.unresolved().len());
        for command in request.unresolved() {
            let symbol = &command.mutation_owner().symbol;
            let rules = self.registered_rules(symbol)?;
            let binding = self.binding_for(symbol);
            let client_order_id = match command {
                ExecutionCommand::Cancel(cancel) => cancel.target_client_order_id.as_str(),
                _ => command
                    .native_client_id()
                    .ok_or(BitgetAccountGatewayError::Readback)?
                    .as_str(),
            };
            let unknown = crate::BitgetUnknownMutation {
                binding,
                attempt_id: readback_attempt,
                generation: rules.snapshot.metadata.instrument.generation,
                kind: command_kind(command),
                order_id: None,
                client_order_id: Some(client_order_id.to_owned()),
                dispatched_at_ms: observed_at_ms,
                reason: crate::BitgetUnknownReason::AmbiguousResponse,
                expected_time_in_force: match command {
                    ExecutionCommand::PlaceLimit(command) => Some(
                        crate::BitgetTimeInForce::from_limit_time_in_force(command.time_in_force),
                    ),
                    _ => None,
                },
            };
            let result = build_unknown_recovery_readback_request(
                &unknown,
                rules.snapshot.metadata.instrument.generation,
            )
            .ok()
            .and_then(|exact| {
                now_ms().ok().and_then(|timestamp| {
                    self.runtime
                        .block_on(self.transport.execute_exact_readback(
                            &self.credentials,
                            exact,
                            timestamp,
                        ))
                        .ok()
                })
            });
            let outcome = match result {
                Some(readback) if settle_unknown_readback(&unknown, &readback).is_ok() => {
                    exact_outcome(command, readback)
                }
                _ => AccountRecoveryOutcome::still_unknown(command.command_id().clone()),
            };
            outcomes.push(outcome);
        }
        AccountRecoveryReport::new(self.transport_binding().clone(), observed_at_ms, outcomes)
            .map_err(|_| BitgetAccountGatewayError::Readback)
    }

    fn risk_evidence(&mut self) -> Result<AccountRiskEvidence, AccountHostValidationError> {
        self.refresh_rules_for_symbols(std::iter::empty())
            .map_err(|_| AccountHostValidationError::RiskEvidence)?;
        let attempt = self
            .next_attempt_id()
            .map_err(|_| AccountHostValidationError::RiskEvidence)?;
        self.runtime.block_on(fetch_account_wide_risk(
            &self.transport,
            &self.credentials,
            attempt,
        ))
    }

    fn signed_account_snapshot(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<SignedAccountSnapshot, AccountHostValidationError> {
        self.poison_private_stream();
        if request.binding() != self.transport_binding() {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        self.refresh_rules_for_symbols(request.configured_symbols().iter().cloned())
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        let attempt = self
            .next_attempt_id()
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        let snapshot = self.runtime.block_on(fetch_account_wide_snapshot(
            &self.transport,
            &self.credentials,
            &self.rules_catalog,
            attempt,
            request,
        ))?;
        if snapshot.private_generation() != attempt {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        let private = self
            .runtime
            .block_on(fetch_private(
                &self.transport,
                &self.credentials,
                &self.rules,
                attempt,
            ))
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        if private.private_generation() != snapshot.private_generation()
            || private.connection_generation() != snapshot.rules_generation()
        {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        self.private = private;
        self.private_generation = snapshot.private_generation();
        self.clear_private_stream();
        Ok(snapshot)
    }

    fn normalize_limit_intent(
        &mut self,
        intent: &AccountLimitNormalizationIntent,
    ) -> Result<ExecutionCommand, AccountHostValidationError> {
        intent.validate()?;
        if intent.owner.exchange != self.transport_binding().venue.as_str()
            || intent.owner.account != self.transport_binding().trading_account_id
            || !self.rules_catalog.contains_key(&intent.owner.symbol)
        {
            return Err(AccountHostValidationError::Scope);
        }
        self.refresh_rules_for_symbols(std::iter::empty())
            .map_err(|_| AccountHostValidationError::Command)?;
        let rules = self
            .registered_rules(&intent.owner.symbol)
            .map_err(|_| AccountHostValidationError::Scope)?;
        let binding = self.binding_for(&intent.owner.symbol);
        let response = self
            .runtime
            .block_on(self.transport.fetch_ticker_for(&binding))
            .map_err(|_| AccountHostValidationError::Command)?;
        let payload =
            String::from_utf8(response.payload).map_err(|_| AccountHostValidationError::Command)?;
        let ticker = parse_rest_ticker(
            BitgetRawPublicPayload::new(
                BitgetPublicSource::RestTicker,
                rules.canonical_symbol().clone(),
                rules.snapshot.metadata.instrument.generation,
                response.received_at_ms,
                payload,
            )
            .map_err(|_| AccountHostValidationError::Command)?,
        )
        .map_err(|_| AccountHostValidationError::Command)?;
        let normalized = normalize_limit_from_ticker(
            intent,
            &rules,
            &ticker,
            now_ms().map_err(|_| AccountHostValidationError::Command)?,
        )?;
        Ok(normalized)
    }

    fn normalize_priced_limit_intent(
        &mut self,
        intent: &AccountPricedLimitIntent,
    ) -> Result<ExecutionCommand, AccountHostValidationError> {
        intent.validate()?;
        if intent.intent.owner.exchange != self.transport_binding().venue.as_str()
            || intent.intent.owner.account != self.transport_binding().trading_account_id
            || !self.rules_catalog.contains_key(&intent.intent.owner.symbol)
        {
            return Err(AccountHostValidationError::Scope);
        }
        self.refresh_rules_for_symbols(std::iter::empty())
            .map_err(|_| AccountHostValidationError::Command)?;
        let rules = self
            .registered_rules(&intent.intent.owner.symbol)
            .map_err(|_| AccountHostValidationError::Scope)?;
        normalize_priced_limit(intent, &rules)
    }

    fn dispatch(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
        self.dispatch_permit(permit)
    }
}

fn normalize_limit_from_ticker(
    intent: &AccountLimitNormalizationIntent,
    rules: &BitgetInstrumentRules,
    ticker: &BitgetTickerEvent,
    now_ms: u64,
) -> Result<ExecutionCommand, AccountHostValidationError> {
    intent.validate()?;
    let metadata = &rules.snapshot.metadata;
    if intent.owner.symbol != *rules.canonical_symbol()
        || ticker.bbo.symbol != *rules.canonical_symbol()
        || intent.position_side == PositionSide::Net
        || !valid_hedge_limit_direction(intent.position_side, intent.side, intent.reduce_only)
        || ticker.bbo.exchange_time_ms == 0
        || ticker.bbo.exchange_time_ms > now_ms
        || now_ms - ticker.bbo.exchange_time_ms > MAX_LIMIT_TICKER_AGE_MS
    {
        return Err(AccountHostValidationError::Command);
    }
    let price = match intent.side {
        OrderSide::Buy => ticker.bbo.bid_price,
        OrderSide::Sell => ticker.bbo.ask_price,
    };
    if ticker.bbo.bid_price >= ticker.bbo.ask_price
        || !metadata
            .price
            .accepts(price.value())
            .map_err(|_| AccountHostValidationError::Command)?
    {
        return Err(AccountHostValidationError::Command);
    }
    let quantity = metadata
        .quantity_for_quote_notional(
            &Amount::new(
                metadata.instrument.minimum_notional.asset.clone(),
                intent.quote_delta,
            ),
            Some(price),
        )
        .map_err(|_| AccountHostValidationError::Command)?;
    if rules
        .maximum_order_quantity
        .is_some_and(|maximum| quantity > maximum)
    {
        return Err(AccountHostValidationError::Command);
    }
    Ok(ExecutionCommand::PlaceLimit(OrderCommand {
        time_in_force: Default::default(),
        command_id: intent.command_id.clone(),
        client_order_id: intent.client_order_id.clone(),
        owner: intent.owner.clone(),
        side: intent.side,
        position_side: intent.position_side,
        quantity,
        limit_price: price,
        reduce_only: intent.reduce_only,
    }))
}

fn normalize_priced_limit(
    intent: &AccountPricedLimitIntent,
    rules: &BitgetInstrumentRules,
) -> Result<ExecutionCommand, AccountHostValidationError> {
    intent.validate()?;
    let base = &intent.intent;
    let metadata = &rules.snapshot.metadata;
    if base.owner.symbol != *rules.canonical_symbol()
        || base.position_side == PositionSide::Net
        || !valid_hedge_limit_direction(base.position_side, base.side, base.reduce_only)
        || !metadata
            .price
            .accepts(intent.limit_price.value())
            .map_err(|_| AccountHostValidationError::Command)?
    {
        return Err(AccountHostValidationError::Command);
    }
    let cap = intent.quantity_cap()?;
    let cap = rules
        .maximum_order_quantity
        .map_or(cap, |maximum| cap.min(maximum));
    let quantity = metadata
        .quantity
        .floor(cap)
        .map_err(|_| AccountHostValidationError::Command)?;
    let notional = metadata
        .quote_notional(quantity, Some(intent.limit_price))
        .map_err(|_| AccountHostValidationError::Command)?;
    if notional.value > base.quote_delta {
        return Err(AccountHostValidationError::Command);
    }
    let command = OrderCommand {
        time_in_force: intent.time_in_force,
        command_id: base.command_id.clone(),
        client_order_id: base.client_order_id.clone(),
        owner: base.owner.clone(),
        side: base.side,
        position_side: base.position_side,
        quantity,
        limit_price: intent.limit_price,
        reduce_only: base.reduce_only,
    };
    command
        .validate()
        .map_err(|_| AccountHostValidationError::Command)?;
    Ok(ExecutionCommand::PlaceLimit(command))
}

fn normalize_private_stream_fill(
    frame: BitgetRawPrivateFrame,
    binding: &GatewayBinding,
    private_generation: u64,
) -> Result<Vec<BitgetPrivateFillEvent>, BitgetAccountGatewayError> {
    if frame.binding != *binding || frame.generation == 0 || frame.received_at_ms == 0 {
        return Err(BitgetAccountGatewayError::PrivateStream);
    }
    if frame.topic != "fill" {
        return Ok(Vec::new());
    }
    if private_generation == 0 {
        return Err(BitgetAccountGatewayError::PrivateStream);
    }
    let payload =
        str::from_utf8(&frame.payload).map_err(|_| BitgetAccountGatewayError::PrivateStream)?;
    let fills = parse_stream_fills(payload, &binding.symbol)
        .map_err(|_| BitgetAccountGatewayError::PrivateStream)?;
    if fills.len() > BITGET_MAX_STREAM_FILLS {
        return Err(BitgetAccountGatewayError::PrivateStream);
    }
    Ok(fills
        .into_iter()
        .map(|fill| BitgetPrivateFillEvent {
            source_private_generation: private_generation,
            received_at_ms: frame.received_at_ms,
            fill: fill.fill,
            client_order_id: fill.client_order_id,
        })
        .collect())
}

const fn valid_hedge_limit_direction(
    position_side: PositionSide,
    side: OrderSide,
    reduce_only: bool,
) -> bool {
    matches!(
        (position_side, side, reduce_only),
        (PositionSide::Long, OrderSide::Buy, false)
            | (PositionSide::Long, OrderSide::Sell, true)
            | (PositionSide::Short, OrderSide::Sell, false)
            | (PositionSide::Short, OrderSide::Buy, true)
    )
}

async fn fetch_rules(
    transport: &BitgetHttpTransport,
    generation: u64,
) -> Result<BitgetInstrumentRules, BitgetAccountGatewayError> {
    fetch_rules_for(transport, transport_binding(transport), generation).await
}

async fn fetch_rules_for(
    transport: &BitgetHttpTransport,
    binding: &GatewayBinding,
    generation: u64,
) -> Result<BitgetInstrumentRules, BitgetAccountGatewayError> {
    let observed_at_ms = now_ms()?;
    let expires_at_ms = observed_at_ms
        .checked_add(60_000)
        .ok_or(BitgetAccountGatewayError::Clock)?;
    let payload = transport
        .fetch_instrument_for(binding)
        .await
        .map_err(BitgetAccountGatewayError::Transport)?;
    let payload = String::from_utf8(payload).map_err(|_| BitgetAccountGatewayError::Rules)?;
    parse_instrument_rules(
        BitgetRawInstrumentPayload::new(
            binding.clone(),
            generation,
            observed_at_ms,
            expires_at_ms,
            payload,
        )
        .map_err(|_| BitgetAccountGatewayError::Rules)?,
        now_ms()?,
    )
    .map_err(|_| BitgetAccountGatewayError::Rules)
}

async fn fetch_private(
    transport: &BitgetHttpTransport,
    credentials: &BitgetCredentials,
    rules: &BitgetInstrumentRules,
    attempt_id: u64,
) -> Result<BitgetNodeReadbackCandidate, BitgetAccountGatewayError> {
    fetch_private_for(
        transport,
        credentials,
        transport_binding(transport),
        rules,
        attempt_id,
    )
    .await
}

async fn fetch_private_for(
    transport: &BitgetHttpTransport,
    credentials: &BitgetCredentials,
    binding: &GatewayBinding,
    rules: &BitgetInstrumentRules,
    attempt_id: u64,
) -> Result<BitgetNodeReadbackCandidate, BitgetAccountGatewayError> {
    let candidate = transport
        .collect_private_turn_for(
            credentials,
            binding,
            attempt_id,
            rules.snapshot.metadata.instrument.generation,
            None,
            now_ms()?,
        )
        .await
        .map_err(BitgetAccountGatewayError::Transport)?;
    let expires_at_ms = candidate
        .observed_at_ms
        .checked_add(60_000)
        .ok_or(BitgetAccountGatewayError::Clock)?;
    BitgetNodeReadbackCandidate::validate(
        BitgetOrderFamilyScope {
            binding: candidate.binding.clone(),
            profile_version: BITGET_ORDER_PROFILE_VERSION,
            attempt_id: candidate.attempt_id,
            generation: candidate.generation,
            observed_at_ms: candidate.observed_at_ms,
            expires_at_ms,
        },
        rules,
        now_ms()?,
        [
            BitgetOrderFamilyEvidence::Regular(Box::new(candidate)),
            BitgetOrderFamilyEvidence::Unsupported(BitgetUnsupportedEvidence::conditional(
                BITGET_ORDER_PROFILE_VERSION,
            )),
            BitgetOrderFamilyEvidence::Unsupported(BitgetUnsupportedEvidence::algo(
                BITGET_ORDER_PROFILE_VERSION,
            )),
        ],
    )
    .map_err(|_| BitgetAccountGatewayError::Readback)
}

async fn fetch_account_wide_risk(
    transport: &BitgetHttpTransport,
    credentials: &BitgetCredentials,
    attempt_id: u64,
) -> Result<AccountRiskEvidence, AccountHostValidationError> {
    let observed_at_ms = now_ms().map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let positions = transport
        .execute_private_read(
            credentials,
            &build_account_wide_positions_read_request(
                transport_binding(transport),
                attempt_id,
                transport.generation(),
            ),
            observed_at_ms,
        )
        .await
        .map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let orders = fetch_account_wide_open_orders(transport, credentials, attempt_id).await?;
    let position_notionals = position_notionals(&positions.payload)?;
    let order_notionals = entry_order_notionals(&orders)?;
    // Bitget UTA v3 `/trade/unfilled-orders` is the complete current-order surface: it returns
    // every delegate type on the queried account/category, not only normal orders.  We accept
    // risk evidence only after this account-wide pagination has classified every row; an
    // unreviewed conditional/algo row remains a hard failure rather than an omitted exposure.
    AccountRiskEvidence::complete(
        transport_binding(transport).clone(),
        observed_at_ms,
        transport.generation(),
        position_notionals,
        order_notionals,
    )
}

async fn fetch_account_wide_snapshot(
    transport: &BitgetHttpTransport,
    credentials: &BitgetCredentials,
    rules_catalog: &BTreeMap<Symbol, BitgetInstrumentRules>,
    attempt_id: u64,
    recovery: &AccountRecoveryRequest,
) -> Result<SignedAccountSnapshot, AccountHostValidationError> {
    let observed_at_ms = now_ms().map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    let account = transport
        .execute_private_read(
            credentials,
            &crate::build_account_read_request(
                transport_binding(transport),
                attempt_id,
                transport.generation(),
            ),
            observed_at_ms,
        )
        .await
        .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    let positions = transport
        .execute_private_read(
            credentials,
            &build_account_wide_positions_read_request(
                transport_binding(transport),
                attempt_id,
                transport.generation(),
            ),
            observed_at_ms,
        )
        .await
        .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    let position_rows = snapshot_data_rows(&positions.payload)?;
    let orders = snapshot_orders(transport, credentials, attempt_id).await?;
    let previous_fills_cursor = parse_snapshot_fills_cursor(recovery.previous_fills_cursor())?;
    let (fills, cursor) = snapshot_fills(
        transport,
        credentials,
        attempt_id,
        observed_at_ms,
        previous_fills_cursor,
    )
    .await?;
    let unknown_results = snapshot_unknowns(
        transport,
        credentials,
        rules_catalog,
        recovery,
        attempt_id,
        observed_at_ms,
    )
    .await?;
    let balance = crate::account::parse_balance(&snapshot_data(&account.payload)?)
        .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    SignedAccountSnapshot::complete_with_fills(
        transport_binding(transport).clone(),
        observed_at_ms,
        transport.generation(),
        attempt_id,
        transport.generation(),
        SignedAccountPositionMode::Hedge,
        snapshot_order_facts(&orders)?,
        snapshot_position_facts(&position_rows)?,
        fills,
        cursor,
        unknown_results,
    )
    .and_then(|snapshot| {
        snapshot.with_balances(vec![SignedAccountBalance {
            asset: balance.asset,
            equity: balance.wallet_balance,
            available_margin: Some(balance.available_balance),
        }])
    })
    .map_err(|_| AccountHostValidationError::SignedSnapshot)
}

async fn snapshot_orders(
    transport: &BitgetHttpTransport,
    credentials: &BitgetCredentials,
    attempt: u64,
) -> Result<Vec<Value>, AccountHostValidationError> {
    fetch_account_wide_open_orders(transport, credentials, attempt)
        .await
        .map_err(|_| AccountHostValidationError::SignedSnapshot)
}

async fn snapshot_fills(
    transport: &BitgetHttpTransport,
    credentials: &BitgetCredentials,
    attempt: u64,
    now: u64,
    previous_cursor_ms: Option<u64>,
) -> Result<(Vec<Fill>, String), AccountHostValidationError> {
    let mut cursor = None;
    let mut result = Vec::new();
    let mut seen = BTreeSet::new();
    let requested_start_ms = previous_cursor_ms
        .map(|value| value.saturating_sub(BITGET_FILL_CURSOR_OVERLAP_MS).max(1))
        .unwrap_or_else(|| now.saturating_sub(BITGET_FILL_CURSOR_OVERLAP_MS).max(1));
    if now.saturating_sub(requested_start_ms) > BITGET_MAX_FILL_HISTORY_WINDOW_MS {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    let mut max_fill_time_ms = previous_cursor_ms.unwrap_or(0);
    for index in 0..BITGET_MAX_PRIVATE_PAGES {
        let request = build_fills_read_request(
            transport_binding(transport),
            attempt,
            transport.generation(),
            u32::try_from(index).map_err(|_| AccountHostValidationError::SignedSnapshot)?,
            cursor.as_deref(),
            Some(requested_start_ms),
            now,
        )
        .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        let raw = transport
            .execute_private_read(credentials, &request, now)
            .await
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        let data = snapshot_data(&raw.payload)?;
        let list = data
            .get("list")
            .and_then(Value::as_array)
            .ok_or(AccountHostValidationError::SignedSnapshot)?;
        for row in list {
            let time_ms = snapshot_fill_time(row)?;
            if time_ms < requested_start_ms || time_ms > now {
                return Err(AccountHostValidationError::SignedSnapshot);
            }
            max_fill_time_ms = max_fill_time_ms.max(time_ms);
            let fill = snapshot_fill(row)?;
            if !seen.insert((fill.symbol.clone(), fill.fill_id.clone())) {
                return Err(AccountHostValidationError::SignedSnapshot);
            }
            result.push(fill);
        }
        cursor = match data.get("cursor") {
            None | Some(Value::Null) => None,
            Some(Value::String(value)) if !value.is_empty() => Some(value.clone()),
            _ => return Err(AccountHostValidationError::SignedSnapshot),
        };
        if cursor.is_none() {
            let observed_through_ms = now.max(max_fill_time_ms);
            return Ok((result, format!("bitget-fills-v1|{observed_through_ms}")));
        }
    }
    Err(AccountHostValidationError::SignedSnapshot)
}

const BITGET_FILL_CURSOR_OVERLAP_MS: u64 = 60_000;

fn parse_snapshot_fills_cursor(
    value: Option<&str>,
) -> Result<Option<u64>, AccountHostValidationError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value
        .strip_prefix("bitget-fills-v1|")
        // A SHA only commits bytes already read; it cannot select the next account-wide page.
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    let watermark = value
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    Ok(Some(watermark))
}

fn snapshot_fill_time(row: &Value) -> Result<u64, AccountHostValidationError> {
    let text = row
        .get("cTime")
        .or_else(|| row.get("createdTime"))
        .and_then(Value::as_str)
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    text.parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or(AccountHostValidationError::SignedSnapshot)
}

fn snapshot_data(payload: &str) -> Result<Value, AccountHostValidationError> {
    let root: Value =
        serde_json::from_str(payload).map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    if root.get("code").and_then(Value::as_str) != Some("00000") {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    root.get("data")
        .cloned()
        .ok_or(AccountHostValidationError::SignedSnapshot)
}
fn snapshot_data_rows(payload: &str) -> Result<Vec<Value>, AccountHostValidationError> {
    snapshot_data(payload)?
        .get("list")
        .and_then(Value::as_array)
        .cloned()
        .ok_or(AccountHostValidationError::SignedSnapshot)
}
fn snapshot_symbol(value: Option<&Value>) -> Result<Symbol, AccountHostValidationError> {
    let raw = value
        .and_then(Value::as_str)
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    let base = raw
        .strip_suffix("USDT")
        .filter(|v| !v.is_empty())
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    Symbol::new(base, "USDT").map_err(|_| AccountHostValidationError::SignedSnapshot)
}
fn snapshot_decimal(value: Option<&Value>) -> Result<Decimal, AccountHostValidationError> {
    decimal(value).map_err(|_| AccountHostValidationError::SignedSnapshot)
}
fn snapshot_position_facts(
    rows: &[Value],
) -> Result<Vec<SignedAccountPositionFact>, AccountHostValidationError> {
    rows.iter()
        .map(|row| {
            let item = row
                .as_object()
                .ok_or(AccountHostValidationError::SignedSnapshot)?;
            require_usdt_perpetual(item).map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            require_text(item, "holdMode", "hedge_mode")
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            let side = match item.get("posSide").and_then(Value::as_str) {
                Some("long") => PositionSide::Long,
                Some("short") => PositionSide::Short,
                _ => return Err(AccountHostValidationError::SignedSnapshot),
            };
            Ok(SignedAccountPositionFact {
                symbol: snapshot_symbol(item.get("symbol"))?,
                position_side: side,
                quantity: snapshot_decimal(item.get("total"))?,
                entry_price: None,
                mark_price: match snapshot_decimal(item.get("markPrice"))? {
                    v if v > Decimal::ZERO => Some(v),
                    v if v.is_zero() => None,
                    _ => return Err(AccountHostValidationError::SignedSnapshot),
                },
            })
        })
        .collect()
}
fn snapshot_order_facts(
    rows: &[Value],
) -> Result<Vec<SignedAccountOrderFact>, AccountHostValidationError> {
    rows.iter()
        .map(|row| {
            let item = row
                .as_object()
                .ok_or(AccountHostValidationError::SignedSnapshot)?;
            require_usdt_perpetual(item).map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            require_text(item, "delegateType", "normal")
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            let side = match item.get("side").and_then(Value::as_str) {
                Some("buy") => OrderSide::Buy,
                Some("sell") => OrderSide::Sell,
                _ => return Err(AccountHostValidationError::SignedSnapshot),
            };
            let position_side = match item.get("posSide").and_then(Value::as_str) {
                Some("long") => PositionSide::Long,
                Some("short") => PositionSide::Short,
                _ => return Err(AccountHostValidationError::SignedSnapshot),
            };
            let quantity = snapshot_decimal(item.get("qty"))?;
            let filled_quantity = snapshot_decimal(item.get("cumExecQty"))?;
            let remaining_quantity = quantity
                .checked_sub(filled_quantity)
                .ok_or(AccountHostValidationError::SignedSnapshot)?;
            if quantity <= Decimal::ZERO || remaining_quantity <= Decimal::ZERO {
                return Err(AccountHostValidationError::SignedSnapshot);
            }
            Ok(SignedAccountOrderFact {
                client_order_id: item
                    .get("clientOid")
                    .and_then(Value::as_str)
                    .filter(|v| !v.is_empty())
                    .ok_or(AccountHostValidationError::SignedSnapshot)?
                    .to_owned(),
                venue_order_id: Some(
                    item.get("orderId")
                        .and_then(Value::as_str)
                        .filter(|v| !v.is_empty())
                        .ok_or(AccountHostValidationError::SignedSnapshot)?
                        .to_owned(),
                ),
                symbol: snapshot_symbol(item.get("symbol"))?,
                family: NativeOrderFamily::UmOrder,
                side,
                position_side,
                quantity,
                limit_price: match snapshot_decimal(item.get("price"))? {
                    v if v > Decimal::ZERO => Some(v),
                    v if v.is_zero() => None,
                    _ => return Err(AccountHostValidationError::SignedSnapshot),
                },
                time_in_force: match item.get("timeInForce") {
                    Some(Value::String(value)) => match value.as_str() {
                        "post_only" => Some(LimitTimeInForce::PostOnly),
                        "gtc" => Some(LimitTimeInForce::Gtc),
                        // IOC/FOK remain native capabilities but have no canonical limit-policy
                        // variant. Preserve that absence rather than reclassifying either as maker.
                        "ioc" | "fok" => None,
                        _ => return Err(AccountHostValidationError::SignedSnapshot),
                    },
                    None | Some(Value::Null) => None,
                    Some(_) => return Err(AccountHostValidationError::SignedSnapshot),
                },
                created_at_ms: snapshot_order_created_at_ms(
                    item.get("cTime").or_else(|| item.get("createdTime")),
                )?,
                reduce_only: bitget_reduce_only(item)
                    .map_err(|_| AccountHostValidationError::SignedSnapshot)?,
                owner: None,
                external: true,
                state: Some(snapshot_order_state(
                    item.get("orderStatus").or_else(|| item.get("status")),
                )?),
                filled_quantity: Some(filled_quantity),
            })
        })
        .collect()
}

fn snapshot_order_state(value: Option<&Value>) -> Result<OrderState, AccountHostValidationError> {
    match value.and_then(Value::as_str) {
        Some("live" | "new") => Ok(OrderState::New),
        Some("partially_filled" | "partially-filled") => Ok(OrderState::PartiallyFilled),
        Some("filled") => Ok(OrderState::Filled),
        Some("cancelled" | "canceled") => Ok(OrderState::Cancelled),
        Some("rejected") => Ok(OrderState::Rejected),
        Some("expired") => Ok(OrderState::Expired),
        _ => Err(AccountHostValidationError::SignedSnapshot),
    }
}

fn snapshot_order_created_at_ms(
    value: Option<&Value>,
) -> Result<Option<u64>, AccountHostValidationError> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    let value = match value {
        Value::String(value) => value.parse::<u64>().ok(),
        Value::Number(value) => value.as_u64(),
        _ => return Err(AccountHostValidationError::SignedSnapshot),
    };
    value
        .filter(|value| *value > 0)
        .map(Some)
        .ok_or(AccountHostValidationError::SignedSnapshot)
}
fn snapshot_fill(row: &Value) -> Result<Fill, AccountHostValidationError> {
    let item = row
        .as_object()
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    require_usdt_perpetual(item).map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    let id = item
        .get("execId")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .ok_or(AccountHostValidationError::SignedSnapshot)?
        .to_owned();
    let side = match item.get("side").and_then(Value::as_str) {
        Some("buy") => OrderSide::Buy,
        Some("sell") => OrderSide::Sell,
        _ => return Err(AccountHostValidationError::SignedSnapshot),
    };
    Ok(Fill {
        execution_sequence: id
            .parse()
            .map(FieldState::Known)
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?,
        fill_id: id,
        order_id: item
            .get("orderId")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
            .ok_or(AccountHostValidationError::SignedSnapshot)?
            .to_owned(),
        symbol: snapshot_symbol(item.get("symbol"))?,
        side,
        position_side: FieldState::Missing,
        quantity: snapshot_decimal(item.get("execQty"))?,
        price: Price::new(snapshot_decimal(item.get("execPrice"))?)
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?,
        fee: FieldState::Missing,
        realized_pnl: FieldState::Missing,
        maker: FieldState::Missing,
        exchange_time_ms: None,
    })
}
async fn snapshot_unknowns(
    transport: &BitgetHttpTransport,
    credentials: &BitgetCredentials,
    rules_catalog: &BTreeMap<Symbol, BitgetInstrumentRules>,
    recovery: &AccountRecoveryRequest,
    attempt: u64,
    now: u64,
) -> Result<Vec<SignedUnknownFact>, AccountHostValidationError> {
    let mut results = Vec::new();
    for command in recovery.unresolved() {
        let symbol = &command.mutation_owner().symbol;
        let rules = catalog_rule(rules_catalog, symbol)
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        let binding = binding_for_symbol(transport_binding(transport), symbol);
        let id = match command {
            ExecutionCommand::Cancel(v) => v.target_client_order_id.as_str(),
            _ => command
                .native_client_id()
                .ok_or(AccountHostValidationError::SignedSnapshot)?
                .as_str(),
        };
        let unknown = crate::BitgetUnknownMutation {
            binding,
            attempt_id: attempt,
            generation: rules.snapshot.metadata.instrument.generation,
            kind: command_kind(command),
            order_id: None,
            client_order_id: Some(id.to_owned()),
            dispatched_at_ms: now,
            reason: crate::BitgetUnknownReason::AmbiguousResponse,
            expected_time_in_force: match command {
                ExecutionCommand::PlaceLimit(command) => Some(
                    crate::BitgetTimeInForce::from_limit_time_in_force(command.time_in_force),
                ),
                _ => None,
            },
        };
        let result = match build_unknown_recovery_readback_request(
            &unknown,
            rules.snapshot.metadata.instrument.generation,
        )
        .ok()
        {
            Some(request) => match transport
                .execute_exact_readback(credentials, request, now)
                .await
            {
                Ok(readback)
                    if settle_unknown_readback(&unknown, &readback).is_ok()
                        && readback.order.as_ref().is_some_and(|order| {
                            command_matches_readback_order(command, order)
                        }) =>
                {
                    match readback.order {
                        Some(order) if order.state == OrderState::Rejected => {
                            SignedUnknownResult::Rejected {
                                reason: "bitget_rejected".to_owned(),
                            }
                        }
                        Some(order) => SignedUnknownResult::Accepted {
                            venue_order_id: order.order_id,
                        },
                        None => SignedUnknownResult::Unknown,
                    }
                }
                _ => SignedUnknownResult::Unknown,
            },
            None => SignedUnknownResult::Unknown,
        };
        results.push(SignedUnknownFact {
            command_id: command.command_id().clone(),
            result,
        });
    }
    Ok(results)
}

async fn fetch_account_wide_open_orders(
    transport: &BitgetHttpTransport,
    credentials: &BitgetCredentials,
    attempt_id: u64,
) -> Result<Vec<Value>, AccountHostValidationError> {
    let mut cursor = None;
    let mut rows = Vec::new();
    for page_index in 0..BITGET_MAX_PRIVATE_PAGES {
        let page_index =
            u32::try_from(page_index).map_err(|_| AccountHostValidationError::RiskEvidence)?;
        let request = build_account_wide_open_orders_read_request(
            transport_binding(transport),
            attempt_id,
            transport.generation(),
            page_index,
            cursor.as_deref(),
        )
        .map_err(|_| AccountHostValidationError::RiskEvidence)?;
        let raw = transport
            .execute_private_read(
                credentials,
                &request,
                now_ms().map_err(|_| AccountHostValidationError::RiskEvidence)?,
            )
            .await
            .map_err(|_| AccountHostValidationError::RiskEvidence)?;
        let data = success_data(&raw.payload)?;
        let list = data
            .get("list")
            .and_then(Value::as_array)
            .ok_or(AccountHostValidationError::RiskEvidence)?;
        rows.extend(list.iter().cloned());
        cursor = match data.get("cursor") {
            Some(Value::Null) | None => None,
            Some(Value::String(value)) if !value.is_empty() => Some(value.clone()),
            _ => return Err(AccountHostValidationError::RiskEvidence),
        };
        if cursor.is_none() {
            return Ok(rows);
        }
    }
    Err(AccountHostValidationError::RiskEvidence)
}

fn position_notionals(payload: &str) -> Result<Vec<Decimal>, AccountHostValidationError> {
    let data = success_data(payload)?;
    let rows = data
        .get("list")
        .and_then(Value::as_array)
        .ok_or(AccountHostValidationError::RiskEvidence)?;
    rows.iter()
        .map(|row| {
            let item = row
                .as_object()
                .ok_or(AccountHostValidationError::RiskEvidence)?;
            require_usdt_perpetual(item)?;
            require_text(item, "holdMode", "hedge_mode")?;
            let position_side = item
                .get("posSide")
                .and_then(Value::as_str)
                .ok_or(AccountHostValidationError::RiskEvidence)?;
            if !matches!(position_side, "long" | "short") {
                return Err(AccountHostValidationError::RiskEvidence);
            }
            let quantity = decimal(item.get("total"))?;
            if quantity.is_sign_negative() {
                return Err(AccountHostValidationError::RiskEvidence);
            }
            if quantity.is_zero() {
                return Ok(Decimal::ZERO);
            }
            let mark = decimal(item.get("markPrice"))?;
            if mark <= Decimal::ZERO {
                return Err(AccountHostValidationError::RiskEvidence);
            }
            quantity
                .checked_mul(mark)
                .ok_or(AccountHostValidationError::Notional)
        })
        .filter(|value| !matches!(value, Ok(value) if value.is_zero()))
        .collect()
}

fn entry_order_notionals(rows: &[Value]) -> Result<Vec<Decimal>, AccountHostValidationError> {
    rows.iter()
        .map(|row| {
            let item = row
                .as_object()
                .ok_or(AccountHostValidationError::RiskEvidence)?;
            require_usdt_perpetual(item)?;
            // The UTA endpoint also returns active trigger/strategy delegates.  This runtime
            // cannot price their latent entry risk, so seeing one rejects admission instead of
            // pretending a normal-order page proves it absent.
            require_text(item, "delegateType", "normal")?;
            let position_side = match item.get("posSide").and_then(Value::as_str) {
                Some("long") => PositionSide::Long,
                Some("short") => PositionSide::Short,
                _ => return Err(AccountHostValidationError::RiskEvidence),
            };
            let side = match item.get("side").and_then(Value::as_str) {
                Some("buy") => OrderSide::Buy,
                Some("sell") => OrderSide::Sell,
                _ => return Err(AccountHostValidationError::RiskEvidence),
            };
            let reduce_only = bitget_reduce_only(item)?;
            let entry_direction = matches!(
                (position_side, side),
                (PositionSide::Long, OrderSide::Buy) | (PositionSide::Short, OrderSide::Sell)
            );
            if let Some(trade_side) = item.get("tradeSide").and_then(Value::as_str) {
                let consistent = match trade_side {
                    "open_long" => position_side == PositionSide::Long && side == OrderSide::Buy,
                    "open_short" => position_side == PositionSide::Short && side == OrderSide::Sell,
                    "close_long" => position_side == PositionSide::Long && side == OrderSide::Sell,
                    "close_short" => position_side == PositionSide::Short && side == OrderSide::Buy,
                    _ => false,
                };
                if !consistent {
                    return Err(AccountHostValidationError::RiskEvidence);
                }
            }
            let quantity = decimal(item.get("qty"))?;
            let filled = decimal(item.get("cumExecQty"))?;
            let remaining = quantity
                .checked_sub(filled)
                .ok_or(AccountHostValidationError::RiskEvidence)?;
            if quantity <= Decimal::ZERO || remaining <= Decimal::ZERO {
                return Err(AccountHostValidationError::RiskEvidence);
            }
            if reduce_only {
                return Ok(Decimal::ZERO);
            }
            if !entry_direction {
                return Err(AccountHostValidationError::RiskEvidence);
            }
            let price = decimal(item.get("price"))?;
            if price <= Decimal::ZERO {
                return Err(AccountHostValidationError::RiskEvidence);
            }
            remaining
                .checked_mul(price)
                .ok_or(AccountHostValidationError::Notional)
        })
        .filter(|value| !matches!(value, Ok(value) if value.is_zero()))
        .collect()
}

fn bitget_reduce_only(
    item: &serde_json::Map<String, Value>,
) -> Result<bool, AccountHostValidationError> {
    match item.get("reduceOnly") {
        Some(Value::Bool(value)) => Ok(*value),
        Some(Value::String(value)) if value == "YES" => Ok(true),
        Some(Value::String(value)) if value == "NO" => Ok(false),
        _ => Err(AccountHostValidationError::RiskEvidence),
    }
}

fn success_data(payload: &str) -> Result<Value, AccountHostValidationError> {
    let root: Value =
        serde_json::from_str(payload).map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let object = root
        .as_object()
        .ok_or(AccountHostValidationError::RiskEvidence)?;
    if object.get("code").and_then(Value::as_str) != Some("00000") {
        return Err(AccountHostValidationError::RiskEvidence);
    }
    object
        .get("data")
        .cloned()
        .ok_or(AccountHostValidationError::RiskEvidence)
}

fn require_usdt_perpetual(
    item: &serde_json::Map<String, Value>,
) -> Result<(), AccountHostValidationError> {
    require_text(item, "marginCoin", "USDT")?;
    let native = item
        .get("symbol")
        .and_then(Value::as_str)
        .ok_or(AccountHostValidationError::RiskEvidence)?;
    if !native.ends_with("USDT") || native.len() <= 4 {
        return Err(AccountHostValidationError::RiskEvidence);
    }
    Ok(())
}

fn require_text(
    item: &serde_json::Map<String, Value>,
    field: &str,
    expected: &str,
) -> Result<(), AccountHostValidationError> {
    if item.get(field).and_then(Value::as_str) == Some(expected) {
        Ok(())
    } else {
        Err(AccountHostValidationError::RiskEvidence)
    }
}

fn decimal(value: Option<&Value>) -> Result<Decimal, AccountHostValidationError> {
    match value {
        Some(Value::String(value)) => value
            .parse()
            .map_err(|_| AccountHostValidationError::RiskEvidence),
        Some(Value::Number(value)) => value
            .to_string()
            .parse()
            .map_err(|_| AccountHostValidationError::RiskEvidence),
        _ => Err(AccountHostValidationError::RiskEvidence),
    }
}

fn command_kind(command: &ExecutionCommand) -> BitgetMutationKind {
    match command {
        ExecutionCommand::Cancel(_) => BitgetMutationKind::Cancel,
        ExecutionCommand::MarketReduce(_) => BitgetMutationKind::ReduceOnce,
        _ => BitgetMutationKind::Place,
    }
}

fn exact_outcome(
    command: &ExecutionCommand,
    readback: BitgetExactOrderReadback,
) -> AccountRecoveryOutcome {
    match readback.order {
        Some(order) if order.state == OrderState::Rejected => AccountRecoveryOutcome::rejected(
            command.command_id().clone(),
            "bitget_rejected".to_owned(),
        ),
        Some(order) => {
            AccountRecoveryOutcome::accepted(command.command_id().clone(), order.order_id)
        }
        None => AccountRecoveryOutcome::still_unknown(command.command_id().clone()),
    }
}

fn rejected(reason: &str) -> AccountGatewayResult {
    AccountGatewayResult::Rejected {
        reason: reason.to_owned(),
    }
}

fn now_ms() -> Result<u64, BitgetAccountGatewayError> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| BitgetAccountGatewayError::Clock)?
            .as_millis(),
    )
    .map_err(|_| BitgetAccountGatewayError::Clock)
}

fn transport_binding(transport: &BitgetHttpTransport) -> &GatewayBinding {
    transport.binding()
}

fn binding_for_symbol(binding: &GatewayBinding, symbol: &Symbol) -> GatewayBinding {
    let mut scoped = binding.clone();
    scoped.symbol = symbol.clone();
    scoped
}

fn catalog_rule(
    catalog: &BTreeMap<Symbol, BitgetInstrumentRules>,
    symbol: &Symbol,
) -> Result<BitgetInstrumentRules, BitgetAccountGatewayError> {
    catalog
        .get(symbol)
        .cloned()
        .ok_or(BitgetAccountGatewayError::Rules)
}

#[derive(Debug, thiserror::Error)]
pub enum BitgetAccountGatewayError {
    #[error("Bitget gateway binding is invalid")]
    Binding,
    #[error("Bitget credentials are unavailable")]
    Credentials,
    #[error("Bitget runtime could not start")]
    Runtime,
    #[error("Bitget clock is invalid")]
    Clock,
    #[error("Bitget account attempt counter overflowed")]
    Attempt,
    #[error("Bitget public instrument rules are invalid")]
    Rules,
    #[error("Bitget signed account readback is invalid")]
    Readback,
    #[error("Bitget private fill stream is malformed or no longer bound to its signed snapshot")]
    PrivateStream,
    #[error(transparent)]
    Transport(#[from] BitgetTransportError),
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use venue_domain::domain::{CommandId, OrderOwner, OrderPurpose};
    use venue_gateway_api::{GatewayMode, VenueId};

    use super::*;

    fn binding() -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Bitget,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)
    }

    const INSTRUMENT_FIXTURE: &str =
        include_str!("../tests/fixtures/bitget_uta_btcusdt_instrument.json");
    const TICKER_FIXTURE: &str = include_str!("../tests/fixtures/bitget_uta_btcusdt_ticker.json");

    fn rules() -> Result<BitgetInstrumentRules, Box<dyn std::error::Error>> {
        Ok(parse_instrument_rules(
            BitgetRawInstrumentPayload::new(
                binding()?,
                7,
                999_000,
                1_001_000,
                INSTRUMENT_FIXTURE.to_owned(),
            )?,
            1_000_000,
        )?)
    }

    fn ticker(payload: String) -> Result<BitgetTickerEvent, Box<dyn std::error::Error>> {
        Ok(parse_rest_ticker(BitgetRawPublicPayload::new(
            BitgetPublicSource::RestTicker,
            "BTC/USDT".parse()?,
            7,
            1_000_000,
            payload,
        )?)?)
    }

    #[test]
    fn private_stream_fill_uses_the_signed_snapshot_attempt_not_socket_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let payload = json!({
            "action": "update",
            "arg": {"instType": "UTA", "topic": "fill"},
            "data": [{
                "execId": "1001", "orderId": "9001", "clientOid": "owned-9001",
                "category": "usdt-futures", "symbol": "BTCUSDT", "side": "buy",
                "holdSide": "long", "execQty": "0.001", "execPrice": "100000",
                "feeDetail": [{"feeCoin": "USDT", "fee": "-0.01"}],
                "execPnl": "0", "tradeScope": "maker", "execTime": "1700000000000"
            }]
        })
        .to_string();
        let frame = BitgetRawPrivateFrame {
            binding: binding.clone(),
            generation: 91,
            topic: "fill".to_owned(),
            received_at_ms: 1_700_000_000_001,
            payload: bytes::Bytes::from(payload),
        };
        let events = normalize_private_stream_fill(frame, &binding, 7)?;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source_private_generation, 7);
        assert_eq!(events[0].fill.fill_id, "1001");
        Ok(())
    }

    #[test]
    fn two_symbol_catalog_routes_only_registered_owner_symbol_rules()
    -> Result<(), Box<dyn std::error::Error>> {
        let btc: Symbol = "BTC/USDT".parse()?;
        let eth: Symbol = "ETH/USDT".parse()?;
        let mut eth_instrument: Value = serde_json::from_str(INSTRUMENT_FIXTURE)?;
        eth_instrument["data"][0]["symbol"] = json!("ETHUSDT");
        eth_instrument["data"][0]["baseCoin"] = json!("ETH");
        let eth_binding = binding_for_symbol(&binding()?, &eth);
        let eth_rules = parse_instrument_rules(
            BitgetRawInstrumentPayload::new(
                eth_binding,
                7,
                999_000,
                1_001_000,
                eth_instrument.to_string(),
            )?,
            1_000_000,
        )?;
        let catalog = BTreeMap::from([(btc.clone(), rules()?), (eth.clone(), eth_rules)]);
        assert_eq!(catalog_rule(&catalog, &btc)?.canonical_symbol(), &btc);
        let eth_rules = catalog_rule(&catalog, &eth)?;
        assert_eq!(eth_rules.canonical_symbol(), &eth);
        assert!(matches!(
            catalog_rule(&catalog, &"SOL/USDT".parse()?),
            Err(BitgetAccountGatewayError::Rules)
        ));

        let mut eth_ticker: Value = serde_json::from_str(TICKER_FIXTURE)?;
        eth_ticker["data"][0]["symbol"] = json!("ETHUSDT");
        let ticker = parse_rest_ticker(BitgetRawPublicPayload::new(
            BitgetPublicSource::RestTicker,
            eth.clone(),
            7,
            1_000_000,
            eth_ticker.to_string(),
        )?)?;
        let mut intent = intent(Decimal::from(10), OrderSide::Buy, PositionSide::Long, false)?;
        intent.owner.symbol = eth;
        assert!(matches!(
            normalize_limit_from_ticker(&intent, &eth_rules, &ticker, 1_000_001)?,
            ExecutionCommand::PlaceLimit(command) if command.owner.symbol == intent.owner.symbol
        ));
        Ok(())
    }

    fn intent(
        quote_delta: Decimal,
        side: OrderSide,
        position_side: PositionSide,
        reduce_only: bool,
    ) -> Result<AccountLimitNormalizationIntent, Box<dyn std::error::Error>> {
        Ok(AccountLimitNormalizationIntent {
            command_id: CommandId::new("command_1")?,
            client_order_id: CommandId::new("client_1")?,
            owner: OrderOwner {
                strategy_instance_id: "strategy_1".to_owned(),
                run_id: "run_1".to_owned(),
                exchange: "bitget".to_owned(),
                account: "account_1".to_owned(),
                symbol: "BTC/USDT".parse()?,
                purpose: OrderPurpose::Entry,
            },
            side,
            position_side,
            quote_delta,
            reduce_only,
        })
    }

    #[test]
    fn normalizer_uses_post_only_bbo_floors_size_and_preserves_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let intent = intent(Decimal::from(10), OrderSide::Buy, PositionSide::Long, false)?;
        let command = normalize_limit_from_ticker(
            &intent,
            &rules()?,
            &ticker(TICKER_FIXTURE.to_owned())?,
            1_000_001,
        )?;
        let ExecutionCommand::PlaceLimit(command) = command else {
            return Err("normalizer must return PlaceLimit".into());
        };
        assert_eq!(command.command_id, intent.command_id);
        assert_eq!(command.client_order_id, intent.client_order_id);
        assert_eq!(command.owner, intent.owner);
        assert_eq!(command.side, OrderSide::Buy);
        assert_eq!(command.position_side, PositionSide::Long);
        assert!(!command.reduce_only);
        assert_eq!(command.limit_price.value(), Decimal::from(100_000));
        assert_eq!(command.quantity, Decimal::new(1, 4));
        Ok(())
    }

    #[test]
    fn priced_limit_uses_explicit_gtc_price_and_quantity_cap()
    -> Result<(), Box<dyn std::error::Error>> {
        let priced = AccountPricedLimitIntent {
            intent: intent(Decimal::from(10), OrderSide::Buy, PositionSide::Long, false)?,
            limit_price: Price::new(Decimal::from(100_000))?,
            time_in_force: LimitTimeInForce::Gtc,
            maximum_quantity: Some(Decimal::new(15, 5)),
        };
        let ExecutionCommand::PlaceLimit(command) = normalize_priced_limit(&priced, &rules()?)?
        else {
            return Err("expected limit".into());
        };
        assert_eq!(command.limit_price, priced.limit_price);
        assert_eq!(command.time_in_force, LimitTimeInForce::Gtc);
        assert_eq!(command.quantity, Decimal::new(1, 4));
        assert!(command.quantity * command.limit_price.value() <= priced.intent.quote_delta);

        let mut unaligned = priced.clone();
        unaligned.limit_price = Price::new(Decimal::new(1_000_000_005, 4))?;
        assert_eq!(
            normalize_priced_limit(&unaligned, &rules()?),
            Err(AccountHostValidationError::Command)
        );
        Ok(())
    }

    #[test]
    fn normalizer_rejects_wrong_symbol_stale_empty_crossed_and_unaligned_tickers()
    -> Result<(), Box<dyn std::error::Error>> {
        let intent = intent(Decimal::from(10), OrderSide::Buy, PositionSide::Long, false)?;
        let rules = rules()?;
        let mut wrong_symbol: serde_json::Value = serde_json::from_str(TICKER_FIXTURE)?;
        wrong_symbol["data"][0]["symbol"] = json!("ETHUSDT");
        assert!(ticker(wrong_symbol.to_string()).is_err());

        let stale = ticker(TICKER_FIXTURE.to_owned())?;
        assert_eq!(
            normalize_limit_from_ticker(&intent, &rules, &stale, 1_005_001),
            Err(AccountHostValidationError::Command)
        );

        let mut empty: serde_json::Value = serde_json::from_str(TICKER_FIXTURE)?;
        empty["data"] = json!([]);
        assert!(ticker(empty.to_string()).is_err());

        let mut crossed: serde_json::Value = serde_json::from_str(TICKER_FIXTURE)?;
        crossed["data"][0]["ask1Price"] = json!("100000.0");
        assert!(ticker(crossed.to_string()).is_err());

        let mut unaligned: serde_json::Value = serde_json::from_str(TICKER_FIXTURE)?;
        unaligned["data"][0]["bid1Price"] = json!("100000.05");
        assert_eq!(
            normalize_limit_from_ticker(
                &intent,
                &rules,
                &ticker(unaligned.to_string())?,
                1_000_001,
            ),
            Err(AccountHostValidationError::Command)
        );
        Ok(())
    }

    #[test]
    fn normalizer_refuses_minimum_shortfall_wrong_identity_and_invalid_hedge_direction()
    -> Result<(), Box<dyn std::error::Error>> {
        let ticker = ticker(TICKER_FIXTURE.to_owned())?;
        let rules = rules()?;
        let minimum_shortfall =
            intent(Decimal::from(5), OrderSide::Buy, PositionSide::Long, false)?;
        assert_eq!(
            normalize_limit_from_ticker(&minimum_shortfall, &rules, &ticker, 1_000_001),
            Err(AccountHostValidationError::Command)
        );

        let mut wrong_identity =
            intent(Decimal::from(10), OrderSide::Buy, PositionSide::Long, false)?;
        wrong_identity.owner.symbol = "ETH/USDT".parse()?;
        assert_eq!(
            normalize_limit_from_ticker(&wrong_identity, &rules, &ticker, 1_000_001),
            Err(AccountHostValidationError::Command)
        );

        let invalid_direction =
            intent(Decimal::from(10), OrderSide::Buy, PositionSide::Long, true)?;
        assert_eq!(
            normalize_limit_from_ticker(&invalid_direction, &rules, &ticker, 1_000_001),
            Err(AccountHostValidationError::Command)
        );
        Ok(())
    }

    #[test]
    fn signed_account_risk_includes_every_usdt_position() -> Result<(), Box<dyn std::error::Error>>
    {
        let payload = json!({
            "code": "00000",
            "data": {"list": [
                {"category":"USDT-FUTURES", "marginCoin":"USDT", "symbol":"BTCUSDT", "holdMode":"hedge_mode", "posSide":"long", "total":"0.1", "markPrice":"100000"},
                {"category":"USDT-FUTURES", "marginCoin":"USDT", "symbol":"ETHUSDT", "holdMode":"hedge_mode", "posSide":"short", "total":"2", "markPrice":"2000"},
                {"category":"USDT-FUTURES", "marginCoin":"USDT", "symbol":"DOGEUSDT", "holdMode":"hedge_mode", "posSide":"long", "total":"0", "markPrice":"0"}
            ]}
        })
        .to_string();
        assert_eq!(
            position_notionals(&payload)?,
            vec![Decimal::from(10_000), Decimal::from(4_000)]
        );
        Ok(())
    }

    #[test]
    fn entry_risk_reserves_only_remaining_open_entries() -> Result<(), Box<dyn std::error::Error>> {
        let rows = vec![
            json!({"category":"USDT-FUTURES", "marginCoin":"USDT", "symbol":"BTCUSDT", "delegateType":"normal", "tradeSide":"open_long", "posSide":"long", "side":"buy", "reduceOnly":"NO", "qty":"0.2", "cumExecQty":"0.05", "price":"100000"}),
            json!({"category":"USDT-FUTURES", "marginCoin":"USDT", "symbol":"ETHUSDT", "delegateType":"normal", "tradeSide":"open_short", "posSide":"short", "side":"sell", "reduceOnly":"NO", "qty":"2", "cumExecQty":"0", "price":"2000"}),
            json!({"category":"USDT-FUTURES", "marginCoin":"USDT", "symbol":"DOGEUSDT", "delegateType":"normal", "tradeSide":"close_long", "posSide":"long", "side":"sell", "reduceOnly":"YES", "qty":"1", "cumExecQty":"0", "price":"1"}),
        ];
        assert_eq!(
            entry_order_notionals(&rows)?,
            vec![Decimal::from(15_000), Decimal::from(4_000)]
        );
        Ok(())
    }

    #[test]
    fn signed_snapshot_partial_regular_order_keeps_original_quantity_and_filled_amount()
    -> Result<(), Box<dyn std::error::Error>> {
        let rows = vec![json!({
            "category":"USDT-FUTURES", "marginCoin":"USDT", "symbol":"BTCUSDT",
            "delegateType":"normal", "posSide":"long", "side":"buy",
            "reduceOnly":"NO", "qty":"0.2", "cumExecQty":"0.05", "price":"100000",
            "clientOid":"partial-regular-1", "orderId":"501", "orderStatus":"partially_filled"
        })];
        let facts = snapshot_order_facts(&rows)?;
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].quantity, Decimal::new(2, 1));
        assert_eq!(facts[0].filled_quantity, Some(Decimal::new(5, 2)));
        assert_eq!(facts[0].state, Some(OrderState::PartiallyFilled));
        Ok(())
    }

    #[test]
    fn account_wide_order_request_never_filters_to_the_selected_symbol()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = build_account_wide_open_orders_read_request(&binding()?, 7, 9, 0, None)?;
        assert_eq!(request.query, "category=USDT-FUTURES&limit=100");
        assert!(!request.query.contains("symbol="));
        assert!(!request.query.contains("delegateType="));
        let next = build_account_wide_open_orders_read_request(
            &binding()?,
            7,
            9,
            1,
            Some("oldest order/id"),
        )?;
        assert_eq!(
            next.query,
            "category=USDT-FUTURES&limit=100&cursor=oldest%20order%2Fid"
        );
        Ok(())
    }

    #[test]
    fn signed_snapshot_fill_cursor_is_time_watermark_and_legacy_digest_fails_closed() {
        assert_eq!(
            parse_snapshot_fills_cursor(Some("bitget-fills-v1|1720000000000")),
            Ok(Some(1_720_000_000_000))
        );
        assert!(
            parse_snapshot_fills_cursor(Some(
                "4983f3d75db0d72aeb1e68c57d9f171d981edc3ef31b8ca16c4d5f1caa26dce5"
            ))
            .is_err()
        );
    }

    #[test]
    fn non_usdt_or_non_regular_rows_reject_the_complete_risk_turn() {
        assert!(entry_order_notionals(&[json!({
            "category":"USDT-FUTURES", "marginCoin":"USDC", "symbol":"BTCUSDC", "delegateType":"normal", "tradeSide":"open_long", "qty":"1", "cumExecQty":"0", "price":"1"
        })])
        .is_err());
        assert!(entry_order_notionals(&[json!({
            "category":"USDT-FUTURES", "marginCoin":"USDT", "symbol":"BTCUSDT", "delegateType":"conditional", "tradeSide":"open_long", "qty":"1", "cumExecQty":"0", "price":"1"
        })])
        .is_err());
    }

    #[test]
    fn unfilled_strategy_delegate_blocks_risk_instead_of_being_treated_as_absent() {
        assert!(
            entry_order_notionals(&[json!({
                "category":"USDT-FUTURES", "marginCoin":"USDT", "symbol":"BTCUSDT",
                "delegateType":"plan_limit", "posSide":"long", "side":"buy",
                "reduceOnly":"NO", "qty":"1", "cumExecQty":"0", "price":"100000"
            })])
            .is_err()
        );
    }

    #[test]
    fn current_uta_normal_order_shape_without_legacy_trade_side_is_risk_counted()
    -> Result<(), Box<dyn std::error::Error>> {
        let rows = vec![json!({
            "category":"USDT-FUTURES", "marginCoin":"USDT", "symbol":"BTCUSDT",
            "delegateType":"normal", "posSide":"short", "side":"sell",
            "reduceOnly":"NO", "qty":"0.1", "cumExecQty":"0", "price":"100000"
        })];
        assert_eq!(entry_order_notionals(&rows)?, vec![Decimal::from(10_000)]);
        Ok(())
    }

    #[test]
    fn signed_snapshot_rejects_visible_strategy_delegate_instead_of_hiding_it() {
        let rows = vec![json!({
            "category":"USDT-FUTURES", "marginCoin":"USDT", "symbol":"BTCUSDT",
            "delegateType":"plan_limit", "posSide":"long", "side":"buy",
            "reduceOnly":"NO", "qty":"1", "cumExecQty":"0", "price":"100000",
            "clientOid":"strategy-visible", "orderId":"strategy-order", "orderStatus":"live"
        })];
        assert!(snapshot_order_facts(&rows).is_err());
    }

    #[test]
    fn signed_snapshot_preserves_time_in_force_without_defaulting()
    -> Result<(), Box<dyn std::error::Error>> {
        let row = |time_in_force: Option<&str>| {
            let mut value = json!({
                "category":"USDT-FUTURES", "marginCoin":"USDT", "symbol":"BTCUSDT",
                "delegateType":"normal", "posSide":"long", "side":"buy",
                "reduceOnly":"NO", "qty":"0.1", "cumExecQty":"0", "price":"100000",
                "clientOid":"venue-1", "orderId":"1", "orderStatus":"live"
            });
            if let Some(time_in_force) = time_in_force {
                value["timeInForce"] = Value::String(time_in_force.to_owned());
            }
            value
        };
        let gtc = snapshot_order_facts(&[row(Some("gtc"))])?;
        assert_eq!(
            gtc.first().and_then(|fact| fact.time_in_force),
            Some(LimitTimeInForce::Gtc)
        );
        let missing = snapshot_order_facts(&[row(None)])?;
        assert_eq!(missing.first().and_then(|fact| fact.time_in_force), None);
        let native_only = snapshot_order_facts(&[row(Some("ioc"))])?;
        assert_eq!(
            native_only.first().and_then(|fact| fact.time_in_force),
            None
        );
        assert!(snapshot_order_facts(&[row(Some("unexpected"))]).is_err());
        Ok(())
    }

    #[test]
    fn signed_snapshot_uses_ctime_not_last_update_time() -> Result<(), Box<dyn std::error::Error>> {
        let row = json!({
            "category":"USDT-FUTURES", "marginCoin":"USDT", "symbol":"BTCUSDT",
            "delegateType":"normal", "posSide":"long", "side":"buy",
            "reduceOnly":"NO", "qty":"0.1", "cumExecQty":"0", "price":"100000",
            "clientOid":"venue-1", "orderId":"1", "orderStatus":"live",
            "cTime":"1700000000123", "uTime":"1700000999999"
        });
        assert_eq!(
            snapshot_order_facts(&[row])?[0].created_at_ms,
            Some(1_700_000_000_123)
        );

        let no_creation = json!({
            "category":"USDT-FUTURES", "marginCoin":"USDT", "symbol":"BTCUSDT",
            "delegateType":"normal", "posSide":"long", "side":"buy",
            "reduceOnly":"NO", "qty":"0.1", "cumExecQty":"0", "price":"100000",
            "clientOid":"venue-2", "orderId":"2", "orderStatus":"live",
            "uTime":"1700000999999"
        });
        assert_eq!(snapshot_order_facts(&[no_creation])?[0].created_at_ms, None);
        Ok(())
    }
}
