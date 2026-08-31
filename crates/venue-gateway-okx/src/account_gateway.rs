use std::{
    collections::BTreeMap,
    str::FromStr,
    time::{Duration, SystemTime},
};

use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use venue_domain::domain::{
    Amount, Asset, ExecutionCommand, FieldState, Fill, LimitTimeInForce, MarketReduceCommand,
    NativeOrderFamily, OrderCommand, OrderSide, OrderState, PositionSide, Price, Symbol,
};
use venue_execution::{
    AccountDispatchPermit, AccountGatewayResult, AccountHostValidationError,
    AccountInstrumentIdentity, AccountLimitNormalizationIntent, AccountPhysicalGateway,
    AccountPricedLimitIntent, AccountRecoveryOutcome, AccountRecoveryReport,
    AccountRecoveryRequest, AccountRecoveryState, AccountRiskEvidence, SignedAccountBalance,
    SignedAccountOrderFact, SignedAccountPositionFact, SignedAccountPositionMode,
    SignedAccountSnapshot, SignedUnknownFact, SignedUnknownResult,
};
use venue_gateway_api::GatewayBinding;

use crate::execution::{
    OkxAcceptedOrder, OkxPlaceIntent, build_host_cancel_request, build_host_order_lookup_request,
    build_order_readback_request, build_place_request, parse_host_cancel_ack,
    parse_host_order_lookup, parse_order_detail, parse_place_ack,
};
use crate::recovery_collector::okx_timestamp;
use crate::{
    OkxAccountProfile, OkxConfig, OkxCredentials, OkxError, OkxHttpTransport, OkxInstrument,
    OkxPositionMode, OkxPrivateReadRequest, OkxPrivateReadScope, OkxRawPrivatePage,
    OkxTimedPosition, OkxTradeMode, OkxTransportError, advance_private_page,
    build_account_config_request, build_algo_orders_request, build_balance_request,
    build_fills_request, build_fills_resume_request, build_positions_request,
    build_regular_orders_request, parse_account_profile, parse_instrument, parse_positions,
};

/// Production OKX adapter for the lightweight account host. Base quantities remain canonical in
/// the WAL; `build_place_request` converts them to contracts using ctVal × ctMult exactly once.
const LIMIT_BBO_MAX_AGE_MS: u64 = 1_000;

pub struct OkxAccountGateway {
    runtime: Runtime,
    config: OkxConfig,
    credentials: OkxCredentials,
    transport: OkxHttpTransport,
    instrument: OkxInstrument,
    profile: OkxAccountProfile,
    positions: Vec<OkxTimedPosition>,
    trade_mode: OkxTradeMode,
    next_attempt_id: u64,
}

impl OkxAccountGateway {
    /// Performs a public instrument read plus signed account-mode, permission, and position reads.
    /// It never issues an order mutation during construction.
    pub fn connect_from_environment(
        binding: GatewayBinding,
        trade_mode: OkxTradeMode,
        operation_timeout: Duration,
        max_body_bytes: usize,
    ) -> Result<Self, OkxAccountGatewayError> {
        let credentials =
            OkxCredentials::from_environment().map_err(|_| OkxAccountGatewayError::Credentials)?;
        Self::connect(
            binding,
            credentials,
            trade_mode,
            operation_timeout,
            max_body_bytes,
        )
    }

    fn connect(
        binding: GatewayBinding,
        credentials: OkxCredentials,
        trade_mode: OkxTradeMode,
        operation_timeout: Duration,
        max_body_bytes: usize,
    ) -> Result<Self, OkxAccountGatewayError> {
        let config =
            OkxConfig::for_binding(binding).map_err(|_| OkxAccountGatewayError::Binding)?;
        let transport = OkxHttpTransport::new(config.clone(), operation_timeout, max_body_bytes)
            .map_err(OkxAccountGatewayError::Transport)?;
        Self::connect_with_transport(config, credentials, transport, trade_mode)
    }

    fn connect_with_transport(
        config: OkxConfig,
        credentials: OkxCredentials,
        transport: OkxHttpTransport,
        trade_mode: OkxTradeMode,
    ) -> Result<Self, OkxAccountGatewayError> {
        let runtime = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|_| OkxAccountGatewayError::Runtime)?;
        let generation = unix_ms()?;
        let response = runtime
            .block_on(transport.fetch_instrument(generation))
            .map_err(OkxAccountGatewayError::Transport)?;
        let instrument = parse_instrument(&response.body, &config, generation)
            .map_err(|_| OkxAccountGatewayError::Instrument)?;
        let (profile, positions) = runtime.block_on(fetch_private_state(
            &config,
            &credentials,
            &transport,
            &instrument,
            trade_mode,
            1,
        ))?;
        Ok(Self {
            runtime,
            config,
            credentials,
            transport,
            instrument,
            profile,
            positions,
            trade_mode,
            next_attempt_id: 2,
        })
    }

    fn take_attempt_id(&mut self) -> Result<u64, OkxAccountGatewayError> {
        let attempt_id = self.next_attempt_id;
        self.next_attempt_id = self
            .next_attempt_id
            .checked_add(1)
            .ok_or(OkxAccountGatewayError::Attempt)?;
        Ok(attempt_id)
    }

    fn refresh_private(&mut self) -> Result<(), OkxAccountGatewayError> {
        let attempt_id = self.take_attempt_id()?;
        let (profile, positions) = self.runtime.block_on(fetch_private_state(
            &self.config,
            &self.credentials,
            &self.transport,
            &self.instrument,
            self.trade_mode,
            attempt_id,
        ))?;
        self.profile = profile;
        self.positions = positions;
        Ok(())
    }

    fn refresh_instrument(&mut self) -> Result<(), OkxAccountGatewayError> {
        let generation = self.instrument.instrument().generation;
        let response = self
            .runtime
            .block_on(self.transport.fetch_instrument(generation))
            .map_err(OkxAccountGatewayError::Transport)?;
        let refreshed = parse_instrument(&response.body, &self.config, generation)
            .map_err(|_| OkxAccountGatewayError::Instrument)?;
        // A query attempt is not a new rules generation. A real rule change invalidates the
        // running binding and must be recovered before another mutation is admitted.
        if refreshed != self.instrument {
            return Err(OkxAccountGatewayError::Instrument);
        }
        Ok(())
    }

    fn current_market_bbo(&mut self) -> Result<OkxLimitBbo, OkxAccountGatewayError> {
        let generation = self.instrument.instrument().generation;
        let response = self
            .runtime
            .block_on(self.transport.fetch_bbo(generation))
            .map_err(OkxAccountGatewayError::Transport)?;
        parse_limit_bbo(&response, &self.config, &self.instrument, unix_ms()?)
    }

    fn collect_account_wide(
        &mut self,
        previous_fills_cursor: Option<&str>,
    ) -> Result<AccountWideCandidate, OkxAccountGatewayError> {
        let started_at_ms = unix_ms()?;
        self.refresh_instrument()?;
        let generation = self.take_attempt_id()?;
        let catalogue = self
            .runtime
            .block_on(self.transport.fetch_swap_instruments(generation))
            .map_err(OkxAccountGatewayError::Transport)?;
        let rules = parse_account_wide_rules(&catalogue.body)?;
        let scope = OkxPrivateReadScope::account_wide(
            &self.config,
            &self.instrument,
            self.profile.position_mode(),
            self.trade_mode,
            generation,
        )
        .map_err(|_| OkxAccountGatewayError::Account)?;
        let pages = self.runtime.block_on(collect_complete_pages(
            &self.credentials,
            &self.transport,
            &scope,
            previous_fills_cursor,
        ))?;
        let mut candidate = account_wide_candidate(
            &pages,
            &rules,
            &self.config,
            self.profile.position_mode(),
            generation,
            previous_fills_cursor,
        )?;
        // Later rule/fill pages cannot refresh the age of positions collected earlier.
        candidate.observed_at_ms = candidate.observed_at_ms.min(started_at_ms);
        Ok(candidate)
    }

    fn dispatch_permit(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
        if permit.binding() != self.config.gateway_binding() {
            return rejected("okx_permit_binding");
        }
        if self.refresh_instrument().is_err() || self.refresh_private().is_err() {
            return rejected("okx_preflight_failed");
        }
        let timestamp = match okx_timestamp(SystemTime::now()) {
            Ok(value) => value,
            Err(_) => return rejected("okx_clock"),
        };
        match permit.command() {
            ExecutionCommand::PlaceLimit(command) => {
                if !command.reduce_only
                    && self
                        .positions
                        .iter()
                        .any(|position| !position.position.quantity.is_zero())
                {
                    return rejected("okx_existing_position");
                }
                let request = match build_place_request(
                    &self.config,
                    &self.instrument,
                    &self.profile,
                    self.trade_mode,
                    OkxPlaceIntent::Limit(command),
                ) {
                    Ok(value) => value,
                    Err(_) => return rejected("okx_intent_rejected"),
                };
                match self.runtime.block_on(self.transport.execute(
                    &self.credentials,
                    &request,
                    &timestamp,
                )) {
                    Ok(response) => match parse_place_ack(response.clone(), &request) {
                        Ok(accepted) => self.settle_accepted_place(&accepted, "okx_limit_rejected"),
                        Err(OkxError::Rejected) => rejected_response(&response.body),
                        Err(_) => AccountGatewayResult::Unknown,
                    },
                    Err(error) => map_transport_dispatch(error),
                }
            }
            ExecutionCommand::Cancel(command) => {
                let request = match build_host_cancel_request(
                    &self.config,
                    &self.instrument,
                    &self.profile,
                    self.trade_mode,
                    command,
                ) {
                    Ok(value) => value,
                    Err(_) => return rejected("okx_cancel_intent_rejected"),
                };
                match self.runtime.block_on(self.transport.execute(
                    &self.credentials,
                    &request,
                    &timestamp,
                )) {
                    Ok(response) => match parse_host_cancel_ack(response.clone(), &request) {
                        Ok(venue_order_id) => AccountGatewayResult::Accepted { venue_order_id },
                        Err(OkxError::Rejected) => rejected_response(&response.body),
                        Err(_) => AccountGatewayResult::Unknown,
                    },
                    Err(error) => map_transport_dispatch(error),
                }
            }
            ExecutionCommand::MarketReduce(command) => {
                if validate_market_reduce_position(command, &self.positions).is_err() {
                    return rejected("okx_market_reduce_position");
                }
                let request = match build_place_request(
                    &self.config,
                    &self.instrument,
                    &self.profile,
                    self.trade_mode,
                    OkxPlaceIntent::MarketReduce(command),
                ) {
                    Ok(value) => value,
                    Err(_) => return rejected("okx_market_reduce_rules"),
                };
                match self.runtime.block_on(self.transport.execute(
                    &self.credentials,
                    &request,
                    &timestamp,
                )) {
                    Ok(response) => match parse_place_ack(response.clone(), &request) {
                        Ok(accepted) => {
                            self.settle_accepted_place(&accepted, "okx_market_reduce_rejected")
                        }
                        Err(OkxError::Rejected) => rejected_response(&response.body),
                        Err(_) => AccountGatewayResult::Unknown,
                    },
                    Err(error) => map_transport_dispatch(error),
                }
            }
            ExecutionCommand::PlaceMarket(_)
            | ExecutionCommand::StopMarketCloseAll(_)
            | ExecutionCommand::StopMarketFullPosition(_) => {
                rejected("okx_initial_profile_unsupported_command")
            }
        }
    }

    fn settle_accepted_place(
        &self,
        accepted: &OkxAcceptedOrder,
        rejected_reason: &str,
    ) -> AccountGatewayResult {
        let readback = match build_order_readback_request(
            &self.config,
            &self.instrument,
            &self.profile,
            accepted,
        ) {
            Ok(value) => value,
            Err(_) => return AccountGatewayResult::Unknown,
        };
        let timestamp = match okx_timestamp(SystemTime::now()) {
            Ok(value) => value,
            Err(_) => return AccountGatewayResult::Unknown,
        };
        let response = self.runtime.block_on(self.transport.execute(
            &self.credentials,
            &readback,
            &timestamp,
        ));
        let Ok(response) = response else {
            return AccountGatewayResult::Unknown;
        };
        match parse_order_detail(response, &readback) {
            Ok(value) if value.order.order.state == OrderState::Rejected => {
                rejected(rejected_reason)
            }
            Ok(value) => AccountGatewayResult::Accepted {
                venue_order_id: value.order.order.order_id,
            },
            Err(_) => AccountGatewayResult::Unknown,
        }
    }
}

fn validate_market_reduce_position(
    command: &MarketReduceCommand,
    positions: &[OkxTimedPosition],
) -> Result<(), ()> {
    let position = positions
        .iter()
        .find(|value| value.position.side == command.position_side)
        .map(|value| &value.position)
        .ok_or(())?;
    validate_market_reduce_against_position(command, position)
}

fn validate_market_reduce_against_position(
    command: &MarketReduceCommand,
    position: &venue_domain::domain::Position,
) -> Result<(), ()> {
    command
        .validate_with_authoritative_position(position)
        .map_err(|_| ())?;
    if position.quantity.is_zero() || command.quantity > position.quantity {
        return Err(());
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OkxLimitBbo {
    bid: Price,
    ask: Price,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OkxLimitBboEnvelope {
    code: String,
    data: Vec<OkxLimitBboRow>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OkxLimitBboRow {
    inst_id: String,
    bids: Vec<Vec<String>>,
    asks: Vec<Vec<String>>,
    ts: String,
}

fn parse_limit_bbo(
    response: &crate::OkxHttpResponse,
    config: &OkxConfig,
    instrument: &OkxInstrument,
    now_ms: u64,
) -> Result<OkxLimitBbo, OkxAccountGatewayError> {
    if response.binding != *config.gateway_binding()
        || response.instrument_generation != instrument.instrument().generation
        || response.received_at_ms == 0
        || now_ms < response.received_at_ms
        || now_ms.saturating_sub(response.received_at_ms) > LIMIT_BBO_MAX_AGE_MS
    {
        return Err(OkxAccountGatewayError::Instrument);
    }
    let envelope: OkxLimitBboEnvelope =
        serde_json::from_slice(&response.body).map_err(|_| OkxAccountGatewayError::Instrument)?;
    if envelope.code != "0" {
        return Err(OkxAccountGatewayError::Instrument);
    }
    let [row] = envelope.data.as_slice() else {
        return Err(OkxAccountGatewayError::Instrument);
    };
    if row.inst_id != instrument.native_id() {
        return Err(OkxAccountGatewayError::Instrument);
    }
    let exchange_time_ms = row
        .ts
        .parse::<u64>()
        .map_err(|_| OkxAccountGatewayError::Instrument)?;
    if exchange_time_ms == 0
        || exchange_time_ms > response.received_at_ms
        || now_ms.saturating_sub(exchange_time_ms) > LIMIT_BBO_MAX_AGE_MS
    {
        return Err(OkxAccountGatewayError::Instrument);
    }
    let bid = bbo_level_price(&row.bids)?;
    let ask = bbo_level_price(&row.asks)?;
    if bid >= ask {
        return Err(OkxAccountGatewayError::Instrument);
    }
    Ok(OkxLimitBbo { bid, ask })
}

fn bbo_level_price(levels: &[Vec<String>]) -> Result<Price, OkxAccountGatewayError> {
    let [price, ..] = levels
        .first()
        .ok_or(OkxAccountGatewayError::Instrument)?
        .as_slice()
    else {
        return Err(OkxAccountGatewayError::Instrument);
    };
    Price::new(Decimal::from_str(price).map_err(|_| OkxAccountGatewayError::Instrument)?)
        .map_err(|_| OkxAccountGatewayError::Instrument)
}

fn normalize_limit_from_bbo(
    config: &OkxConfig,
    instrument: &OkxInstrument,
    intent: &AccountLimitNormalizationIntent,
    bbo: OkxLimitBbo,
) -> Result<ExecutionCommand, AccountHostValidationError> {
    intent.validate()?;
    if intent.owner.exchange != config.gateway_binding().venue.as_str()
        || intent.owner.account != config.gateway_binding().trading_account_id
        || intent.owner.symbol != config.gateway_binding().symbol
        || !matches!(
            (intent.position_side, intent.side, intent.reduce_only),
            (PositionSide::Long, OrderSide::Buy, false)
                | (PositionSide::Long, OrderSide::Sell, true)
                | (PositionSide::Short, OrderSide::Sell, false)
                | (PositionSide::Short, OrderSide::Buy, true)
        )
    {
        return Err(AccountHostValidationError::Command);
    }
    let raw_price = match intent.side {
        OrderSide::Buy => bbo.bid.value(),
        OrderSide::Sell => bbo.ask.value(),
    };
    let price = Price::new(floor_to_step(
        raw_price,
        instrument.instrument().price_tick.value(),
    )?)
    .map_err(|_| AccountHostValidationError::Command)?;
    let quote = Asset::new(config.gateway_binding().symbol.quote())
        .map_err(|_| AccountHostValidationError::Command)?;
    let size = instrument
        .size_for_quote_notional(&Amount::new(quote, intent.quote_delta), price)
        .map_err(|_| AccountHostValidationError::Command)?;
    if instrument
        .maximum_limit_contracts()
        .is_none_or(|maximum| size.contracts() > maximum)
        || size.quote_notional().value > intent.quote_delta
    {
        return Err(AccountHostValidationError::Command);
    }
    let command = ExecutionCommand::PlaceLimit(OrderCommand {
        time_in_force: Default::default(),
        command_id: intent.command_id.clone(),
        client_order_id: intent.client_order_id.clone(),
        owner: intent.owner.clone(),
        side: intent.side,
        position_side: intent.position_side,
        quantity: size.base_quantity(),
        limit_price: price,
        reduce_only: intent.reduce_only,
    });
    command
        .validate()
        .map_err(|_| AccountHostValidationError::Command)?;
    Ok(command)
}

fn normalize_priced_limit(
    config: &OkxConfig,
    instrument: &OkxInstrument,
    priced: &AccountPricedLimitIntent,
) -> Result<ExecutionCommand, AccountHostValidationError> {
    let intent = &priced.intent;
    priced.validate()?;
    if intent.owner.exchange != config.gateway_binding().venue.as_str()
        || intent.owner.account != config.gateway_binding().trading_account_id
        || intent.owner.symbol != config.gateway_binding().symbol
        || !matches!(
            (intent.position_side, intent.side, intent.reduce_only),
            (PositionSide::Long, OrderSide::Buy, false)
                | (PositionSide::Long, OrderSide::Sell, true)
                | (PositionSide::Short, OrderSide::Sell, false)
                | (PositionSide::Short, OrderSide::Buy, true)
        )
        || priced.limit_price.value() % instrument.instrument().price_tick.value() != Decimal::ZERO
    {
        return Err(AccountHostValidationError::Command);
    }
    let cap = priced
        .quantity_cap()?
        .checked_mul(priced.limit_price.value())
        .ok_or(AccountHostValidationError::Notional)?;
    let quote = Asset::new(config.gateway_binding().symbol.quote())
        .map_err(|_| AccountHostValidationError::Command)?;
    let size = instrument
        .size_for_quote_notional(&Amount::new(quote, cap), priced.limit_price)
        .map_err(|_| AccountHostValidationError::Command)?;
    if instrument
        .maximum_limit_contracts()
        .is_none_or(|maximum| size.contracts() > maximum)
        || size.base_quantity() > priced.quantity_cap()?
        || size.quote_notional().value > intent.quote_delta
    {
        return Err(AccountHostValidationError::Command);
    }
    let command = ExecutionCommand::PlaceLimit(OrderCommand {
        time_in_force: priced.time_in_force,
        command_id: intent.command_id.clone(),
        client_order_id: intent.client_order_id.clone(),
        owner: intent.owner.clone(),
        side: intent.side,
        position_side: intent.position_side,
        quantity: size.base_quantity(),
        limit_price: priced.limit_price,
        reduce_only: intent.reduce_only,
    });
    command
        .validate()
        .map_err(|_| AccountHostValidationError::Command)?;
    Ok(command)
}

fn floor_to_step(value: Decimal, step: Decimal) -> Result<Decimal, AccountHostValidationError> {
    if value <= Decimal::ZERO || step <= Decimal::ZERO {
        return Err(AccountHostValidationError::Command);
    }
    let floored = value - value % step;
    if floored <= Decimal::ZERO {
        return Err(AccountHostValidationError::Command);
    }
    Ok(floored)
}

impl AccountPhysicalGateway for OkxAccountGateway {
    type Error = OkxAccountGatewayError;

    fn binding(&self) -> &GatewayBinding {
        self.config.gateway_binding()
    }

    fn current_instrument(
        &mut self,
    ) -> Result<AccountInstrumentIdentity, AccountHostValidationError> {
        self.refresh_instrument()
            .map_err(|_| AccountHostValidationError::Instrument)?;
        let instrument = self.instrument.instrument().clone();
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
        if request.binding() != self.config.gateway_binding() {
            return Err(OkxAccountGatewayError::Binding);
        }
        self.refresh_private()?;
        let observed_at_ms = unix_ms()?;
        let mut outcomes = Vec::with_capacity(request.unresolved().len());
        for command in request.unresolved() {
            let client_id = match command {
                ExecutionCommand::Cancel(cancel) => cancel.target_client_order_id.as_str(),
                _ => command
                    .native_client_id()
                    .ok_or(OkxAccountGatewayError::Readback)?
                    .as_str(),
            };
            let lookup = build_host_order_lookup_request(
                &self.config,
                &self.instrument,
                &self.profile,
                self.trade_mode,
                client_id,
            )
            .map_err(|_| OkxAccountGatewayError::Readback)?;
            let timestamp =
                okx_timestamp(SystemTime::now()).map_err(|_| OkxAccountGatewayError::Clock)?;
            let response = self
                .runtime
                .block_on(
                    self.transport
                        .execute(&self.credentials, &lookup, &timestamp),
                )
                .map_err(OkxAccountGatewayError::Transport)?;
            let found = parse_host_order_lookup(response, &lookup)
                .map_err(|_| OkxAccountGatewayError::Readback)?;
            outcomes.push(okx_recovery_outcome(command, found));
        }
        AccountRecoveryReport::new(
            self.config.gateway_binding().clone(),
            observed_at_ms,
            outcomes,
        )
        .map_err(|_| OkxAccountGatewayError::Readback)
    }

    fn risk_evidence(&mut self) -> Result<AccountRiskEvidence, AccountHostValidationError> {
        let candidate = self
            .collect_account_wide(None)
            .map_err(|_| AccountHostValidationError::RiskEvidence)?;
        AccountRiskEvidence::complete(
            self.config.gateway_binding().clone(),
            candidate.observed_at_ms,
            candidate.generation,
            candidate.position_notionals,
            candidate.entry_order_notionals,
        )
    }

    fn signed_account_snapshot(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<SignedAccountSnapshot, AccountHostValidationError> {
        if request.binding() != self.config.gateway_binding() {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        let candidate = self
            .collect_account_wide(request.previous_fills_cursor())
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        let recovery = self
            .reconcile(request)
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        let unknown_results = recovery
            .outcomes()
            .iter()
            .map(|outcome| SignedUnknownFact {
                command_id: outcome.command_id().clone(),
                result: match outcome.state() {
                    AccountRecoveryState::Accepted { venue_order_id } => {
                        SignedUnknownResult::Accepted {
                            venue_order_id: venue_order_id.clone(),
                        }
                    }
                    AccountRecoveryState::Rejected { reason } => SignedUnknownResult::Rejected {
                        reason: reason.clone(),
                    },
                    AccountRecoveryState::StillUnknown => SignedUnknownResult::Unknown,
                },
            })
            .collect();
        let mode = match candidate.position_mode {
            OkxPositionMode::Net => SignedAccountPositionMode::Net,
            OkxPositionMode::LongShort => SignedAccountPositionMode::Hedge,
        };
        SignedAccountSnapshot::complete_with_fills(
            self.config.gateway_binding().clone(),
            candidate.observed_at_ms,
            self.instrument.instrument().generation,
            candidate.generation,
            self.instrument.instrument().generation,
            mode,
            candidate.orders,
            candidate.positions,
            candidate.fills,
            candidate.fills_cursor,
            unknown_results,
        )?
        .with_balances(candidate.balances)
    }

    fn normalize_limit_intent(
        &mut self,
        intent: &AccountLimitNormalizationIntent,
    ) -> Result<ExecutionCommand, AccountHostValidationError> {
        self.refresh_instrument()
            .map_err(|_| AccountHostValidationError::Command)?;
        let bbo = self
            .current_market_bbo()
            .map_err(|_| AccountHostValidationError::Command)?;
        normalize_limit_from_bbo(&self.config, &self.instrument, intent, bbo)
    }

    fn normalize_priced_limit_intent(
        &mut self,
        intent: &AccountPricedLimitIntent,
    ) -> Result<ExecutionCommand, AccountHostValidationError> {
        self.refresh_instrument()
            .map_err(|_| AccountHostValidationError::Command)?;
        normalize_priced_limit(&self.config, &self.instrument, intent)
    }

    fn dispatch(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
        self.dispatch_permit(permit)
    }
}

async fn collect_complete_pages(
    credentials: &OkxCredentials,
    transport: &OkxHttpTransport,
    scope: &OkxPrivateReadScope,
    previous_fills_cursor: Option<&str>,
) -> Result<Vec<OkxRawPrivatePage>, OkxAccountGatewayError> {
    let mut pages = Vec::new();
    for request in [
        build_account_config_request(scope),
        build_balance_request(scope),
        build_positions_request(scope),
    ] {
        pages.push(
            execute_signed_read(
                credentials,
                transport,
                request.map_err(|_| OkxAccountGatewayError::Account)?,
            )
            .await?,
        );
    }
    collect_signed_pages(
        credentials,
        transport,
        build_regular_orders_request(scope, 0, None)
            .map_err(|_| OkxAccountGatewayError::Account)?,
        &mut pages,
    )
    .await?;
    for kind in crate::OkxAlgoOrderKind::ALL {
        collect_signed_pages(
            credentials,
            transport,
            build_algo_orders_request(scope, kind, 0, None)
                .map_err(|_| OkxAccountGatewayError::Account)?,
            &mut pages,
        )
        .await?;
    }
    let fills_request = match previous_fills_cursor {
        None | Some("okx-bill:empty") => build_fills_request(scope, 0, None),
        Some(cursor) => build_fills_resume_request(
            scope,
            cursor
                .strip_prefix("okx-bill:")
                .filter(|value| !value.is_empty())
                .ok_or(OkxAccountGatewayError::Account)?,
        ),
    }
    .map_err(|_| OkxAccountGatewayError::Account)?;
    collect_signed_pages(credentials, transport, fills_request, &mut pages).await?;
    Ok(pages)
}

async fn collect_signed_pages(
    credentials: &OkxCredentials,
    transport: &OkxHttpTransport,
    mut request: OkxPrivateReadRequest,
    pages: &mut Vec<OkxRawPrivatePage>,
) -> Result<(), OkxAccountGatewayError> {
    loop {
        let page = execute_signed_read(credentials, transport, request).await?;
        let advance = advance_private_page(&page).map_err(|_| OkxAccountGatewayError::Account)?;
        pages.push(page);
        match advance {
            crate::OkxPrivatePageAdvance::Closed => return Ok(()),
            crate::OkxPrivatePageAdvance::More(next) => request = *next,
        }
    }
}

async fn execute_signed_read(
    credentials: &OkxCredentials,
    transport: &OkxHttpTransport,
    request: OkxPrivateReadRequest,
) -> Result<OkxRawPrivatePage, OkxAccountGatewayError> {
    let timestamp = okx_timestamp(SystemTime::now()).map_err(|_| OkxAccountGatewayError::Clock)?;
    let response = transport
        .execute_read(credentials, &request, &timestamp)
        .await
        .map_err(OkxAccountGatewayError::Transport)?;
    OkxRawPrivatePage::from_http_response(&request, response)
        .map_err(|_| OkxAccountGatewayError::Account)
}

struct AccountWideRule {
    symbol: Symbol,
    base_per_contract: Decimal,
    lot_size: Decimal,
    minimum_size: Decimal,
}

struct AccountWideCandidate {
    observed_at_ms: u64,
    generation: u64,
    position_mode: OkxPositionMode,
    positions: Vec<SignedAccountPositionFact>,
    orders: Vec<SignedAccountOrderFact>,
    position_notionals: Vec<Decimal>,
    entry_order_notionals: Vec<Decimal>,
    fills: Vec<Fill>,
    fills_cursor: String,
    balances: Vec<SignedAccountBalance>,
}

fn parse_account_wide_rules(
    body: &[u8],
) -> Result<BTreeMap<String, AccountWideRule>, OkxAccountGatewayError> {
    let rows = response_rows(body)?;
    let mut rules = BTreeMap::new();
    for row in &rows {
        if text(row, "instType")? != "SWAP"
            || text(row, "ctType")? != "linear"
            || text(row, "settleCcy")? != "USDT"
            || text(row, "state")? != "live"
        {
            continue;
        }
        let native = text(row, "instId")?;
        let base = native
            .strip_suffix("-USDT-SWAP")
            .filter(|value| !value.is_empty())
            .ok_or(OkxAccountGatewayError::Account)?;
        if text(row, "ctValCcy")? != base {
            return Err(OkxAccountGatewayError::Account);
        }
        let base_per_contract = positive(row, "ctVal")?
            .checked_mul(positive(row, "ctMult")?)
            .filter(|value| *value > Decimal::ZERO)
            .ok_or(OkxAccountGatewayError::Account)?;
        let lot_size = positive(row, "lotSz")?;
        let minimum_size = positive(row, "minSz")?;
        let symbol = Symbol::from_str(&format!("{base}/USDT"))
            .map_err(|_| OkxAccountGatewayError::Account)?;
        if rules
            .insert(
                native.to_owned(),
                AccountWideRule {
                    symbol,
                    base_per_contract,
                    lot_size,
                    minimum_size,
                },
            )
            .is_some()
        {
            return Err(OkxAccountGatewayError::Account);
        }
    }
    if rules.is_empty() {
        return Err(OkxAccountGatewayError::Account);
    }
    Ok(rules)
}

fn account_wide_candidate(
    pages: &[OkxRawPrivatePage],
    rules: &BTreeMap<String, AccountWideRule>,
    config: &OkxConfig,
    expected_mode: OkxPositionMode,
    generation: u64,
    previous_fills_cursor: Option<&str>,
) -> Result<AccountWideCandidate, OkxAccountGatewayError> {
    let account = pages
        .iter()
        .find(|page| page.surface == crate::OkxPrivateSurface::AccountConfig)
        .ok_or(OkxAccountGatewayError::Account)?;
    let profile = parse_account_profile(&account.payload, expected_mode)
        .map_err(|_| OkxAccountGatewayError::Account)?;
    if !profile.can_read() || !profile.can_trade() || profile.can_withdraw() {
        return Err(OkxAccountGatewayError::Permissions);
    }
    let balance_page = pages
        .iter()
        .find(|page| page.surface == crate::OkxPrivateSurface::Balance)
        .ok_or(OkxAccountGatewayError::Account)?;
    let balance = crate::parse_balance(&balance_page.payload, config, &profile)
        .map_err(|_| OkxAccountGatewayError::Account)?;
    let balances = vec![SignedAccountBalance {
        asset: balance.balance.asset,
        equity: balance.balance.wallet_balance,
        available_margin: Some(balance.balance.available_balance),
    }];
    let mut positions = Vec::new();
    let mut position_notionals = Vec::new();
    let mut orders = Vec::new();
    let mut entry_order_notionals = Vec::new();
    let observed_at_ms = oldest_page_observation(pages)?;
    for page in pages {
        match page.surface {
            crate::OkxPrivateSurface::Positions => {
                for row in &response_rows(&page.payload)? {
                    let rule = row_rule(row, rules)?;
                    let contracts = decimal(row, "pos")?;
                    let side = position_side_for(
                        profile.position_mode(),
                        text(row, "posSide")?,
                        contracts,
                    )?;
                    let contracts = if side == PositionSide::Net {
                        contracts
                    } else {
                        contracts.abs()
                    };
                    let quantity = contracts
                        .checked_mul(rule.base_per_contract)
                        .ok_or(OkxAccountGatewayError::Account)?;
                    let entry_price = optional_decimal(row, "avgPx")?;
                    let mark_price = optional_decimal(row, "markPx")?;
                    if !quantity.is_zero() {
                        let price = mark_price
                            .or(entry_price)
                            .ok_or(OkxAccountGatewayError::Account)?;
                        position_notionals.push(
                            quantity
                                .abs()
                                .checked_mul(price)
                                .ok_or(OkxAccountGatewayError::Account)?,
                        );
                    }
                    positions.push(SignedAccountPositionFact {
                        symbol: rule.symbol.clone(),
                        position_side: side,
                        quantity,
                        entry_price,
                        mark_price,
                    });
                }
            }
            crate::OkxPrivateSurface::RegularOrders => collect_wide_orders(
                &page.payload,
                NativeOrderFamily::UmOrder,
                &profile,
                rules,
                &mut orders,
                &mut entry_order_notionals,
            )?,
            crate::OkxPrivateSurface::AlgoOrders(_) => collect_wide_orders(
                &page.payload,
                NativeOrderFamily::UmAlgo,
                &profile,
                rules,
                &mut orders,
                &mut entry_order_notionals,
            )?,
            _ => {}
        }
    }
    let (fills, fills_cursor) = snapshot_fills(pages, rules, &profile, previous_fills_cursor)?;
    Ok(AccountWideCandidate {
        observed_at_ms,
        generation,
        position_mode: profile.position_mode(),
        positions,
        orders,
        position_notionals,
        entry_order_notionals,
        fills,
        fills_cursor,
        balances,
    })
}

fn oldest_page_observation(pages: &[OkxRawPrivatePage]) -> Result<u64, OkxAccountGatewayError> {
    pages
        .iter()
        .map(|page| page.received_at_ms)
        .min()
        .filter(|time| *time > 0)
        .ok_or(OkxAccountGatewayError::Account)
}

fn snapshot_fills(
    pages: &[OkxRawPrivatePage],
    rules: &BTreeMap<String, AccountWideRule>,
    profile: &OkxAccountProfile,
    previous_fills_cursor: Option<&str>,
) -> Result<(Vec<Fill>, String), OkxAccountGatewayError> {
    let fills_pages = pages
        .iter()
        .filter(|page| page.surface == crate::OkxPrivateSurface::Fills)
        .collect::<Vec<_>>();
    if fills_pages.is_empty() || fills_pages.len() > crate::OKX_PRIVATE_MAX_PAGES {
        return Err(OkxAccountGatewayError::Account);
    }
    let mut seen = BTreeMap::new();
    let mut fills = Vec::new();
    let mut previous_time = None;
    let mut cursor = None;
    for (index, page) in fills_pages.iter().enumerate() {
        if usize::try_from(page.page_index).ok() != Some(index) {
            return Err(OkxAccountGatewayError::Account);
        }
        for row in response_rows(&page.payload)? {
            let rule = row_rule(&row, rules)?;
            let fill_id = text(&row, "billId")?.to_owned();
            if fill_id.is_empty() || !fill_id.bytes().all(|value| value.is_ascii_digit()) {
                return Err(OkxAccountGatewayError::Account);
            }
            let time = text(&row, "fillTime")?
                .parse::<u64>()
                .ok()
                .filter(|value| *value > 0 && *value <= page.received_at_ms)
                .ok_or(OkxAccountGatewayError::Account)?;
            let side = match text(&row, "side")? {
                "buy" => OrderSide::Buy,
                "sell" => OrderSide::Sell,
                _ => return Err(OkxAccountGatewayError::Account),
            };
            let position_side = position_side_for(
                profile.position_mode(),
                text(&row, "posSide")?,
                Decimal::ONE,
            )?;
            let maker = match text(&row, "execType")? {
                "M" => true,
                "T" => false,
                _ => return Err(OkxAccountGatewayError::Account),
            };
            let quantity = positive(&row, "fillSz")?
                .checked_mul(rule.base_per_contract)
                .filter(|value| *value > Decimal::ZERO)
                .ok_or(OkxAccountGatewayError::Account)?;
            let fill = Fill {
                fill_id: fill_id.clone(),
                execution_sequence: FieldState::Missing,
                order_id: text(&row, "ordId")?.to_owned(),
                symbol: rule.symbol.clone(),
                side,
                position_side: FieldState::Known(position_side),
                quantity,
                price: Price::new(positive(&row, "fillPx")?)
                    .map_err(|_| OkxAccountGatewayError::Account)?,
                fee: FieldState::Known(Amount::new(
                    Asset::new(text(&row, "feeCcy")?)
                        .map_err(|_| OkxAccountGatewayError::Account)?,
                    decimal(&row, "fee")?.abs(),
                )),
                realized_pnl: FieldState::Missing,
                maker: FieldState::Known(maker),
                exchange_time_ms: Some(time),
            };
            fill.validate()
                .map_err(|_| OkxAccountGatewayError::Account)?;
            match seen.get(&fill_id) {
                Some(previous) if previous != &fill => return Err(OkxAccountGatewayError::Account),
                Some(_) => {}
                None => {
                    if previous_time.is_some_and(|previous| time > previous) {
                        return Err(OkxAccountGatewayError::Account);
                    }
                    previous_time = Some(time);
                    if cursor.is_none() {
                        cursor = Some(format!("okx-bill:{fill_id}"));
                    }
                    seen.insert(fill_id, fill.clone());
                    fills.push(fill);
                }
            }
        }
    }
    Ok((
        fills,
        cursor
            .or_else(|| previous_fills_cursor.map(str::to_owned))
            .unwrap_or_else(|| "okx-bill:empty".to_owned()),
    ))
}

fn collect_wide_orders(
    payload: &[u8],
    default_family: NativeOrderFamily,
    profile: &OkxAccountProfile,
    rules: &BTreeMap<String, AccountWideRule>,
    orders: &mut Vec<SignedAccountOrderFact>,
    entries: &mut Vec<Decimal>,
) -> Result<(), OkxAccountGatewayError> {
    for row in &response_rows(payload)? {
        let rule = row_rule(row, rules)?;
        let algo = row.get("algoId").is_some();
        let venue_order_id = if algo {
            text(row, "algoId")?
        } else {
            text(row, "ordId")?
        };
        let client_key = if algo { "algoClOrdId" } else { "clOrdId" };
        let client_order_id = optional_text(row, client_key)?
            .map(str::to_owned)
            .unwrap_or_else(|| venue_order_id.to_owned());
        let raw_contracts = positive(row, "sz")?;
        if raw_contracts < rule.minimum_size || raw_contracts % rule.lot_size != Decimal::ZERO {
            return Err(OkxAccountGatewayError::Account);
        }
        let quantity = raw_contracts
            .checked_mul(rule.base_per_contract)
            .ok_or(OkxAccountGatewayError::Account)?;
        let side = match text(row, "side")? {
            "buy" => OrderSide::Buy,
            "sell" => OrderSide::Sell,
            _ => return Err(OkxAccountGatewayError::Account),
        };
        let position_side =
            position_side_for(profile.position_mode(), text(row, "posSide")?, Decimal::ONE)?;
        let reduce_only = match text(row, "reduceOnly")? {
            "true" => true,
            "false" => false,
            _ => return Err(OkxAccountGatewayError::Account),
        };
        let price = optional_decimal(row, if algo { "orderPx" } else { "px" })?;
        let family = if algo && matches!(text(row, "ordType")?, "conditional" | "oco") {
            NativeOrderFamily::UmConditional
        } else {
            default_family
        };
        if !reduce_only {
            // A trigger market order has no bounded USDT value in this signed surface.  Treating
            // it as zero would understate aggregate account risk, so the whole observation fails.
            let price = price.ok_or(OkxAccountGatewayError::Account)?;
            entries.push(
                quantity
                    .checked_mul(price)
                    .ok_or(OkxAccountGatewayError::Account)?,
            );
        }
        orders.push(SignedAccountOrderFact {
            client_order_id,
            venue_order_id: Some(venue_order_id.to_owned()),
            symbol: rule.symbol.clone(),
            family,
            side,
            position_side,
            quantity,
            limit_price: price,
            time_in_force: signed_limit_time_in_force(row, family),
            reduce_only,
            owner: None,
            external: true,
            state: Some(okx_order_state(text(row, "state")?)?),
            filled_quantity: Some(
                positive_or_zero(row, "accFillSz")?
                    .checked_mul(rule.base_per_contract)
                    .ok_or(OkxAccountGatewayError::Account)?,
            ),
            created_at_ms: optional_order_created_at_ms(row)?,
        });
    }
    Ok(())
}

fn optional_order_created_at_ms(
    row: &serde_json::Value,
) -> Result<Option<u64>, OkxAccountGatewayError> {
    match row.get("cTime") {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(value)) if value.is_empty() => Ok(None),
        Some(serde_json::Value::String(value)) => value
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or(OkxAccountGatewayError::Account)
            .map(Some),
        Some(_) => Err(OkxAccountGatewayError::Account),
    }
}

fn signed_limit_time_in_force(
    row: &serde_json::Value,
    family: NativeOrderFamily,
) -> Option<LimitTimeInForce> {
    if family != NativeOrderFamily::UmOrder {
        return None;
    }
    match row.get("ordType").and_then(serde_json::Value::as_str) {
        Some("post_only") => Some(LimitTimeInForce::PostOnly),
        Some("limit") => Some(LimitTimeInForce::Gtc),
        _ => None,
    }
}

#[cfg(test)]
mod signed_limit_policy_tests {
    use super::*;

    #[test]
    fn signed_snapshot_keeps_absent_or_unsupported_policy_unknown() {
        assert_eq!(
            signed_limit_time_in_force(
                &serde_json::json!({"ordType":"post_only"}),
                NativeOrderFamily::UmOrder,
            ),
            Some(LimitTimeInForce::PostOnly)
        );
        assert_eq!(
            signed_limit_time_in_force(
                &serde_json::json!({"ordType":"limit"}),
                NativeOrderFamily::UmOrder,
            ),
            Some(LimitTimeInForce::Gtc)
        );
        assert_eq!(
            signed_limit_time_in_force(&serde_json::json!({}), NativeOrderFamily::UmOrder),
            None
        );
    }

    #[test]
    fn signed_snapshot_uses_native_creation_time_only() {
        assert!(matches!(
            optional_order_created_at_ms(&serde_json::json!({"cTime":"1800","uTime":"1900"})),
            Ok(Some(1800))
        ));
        assert!(matches!(
            optional_order_created_at_ms(&serde_json::json!({"uTime":"1900"})),
            Ok(None)
        ));
    }
}

fn okx_order_state(value: &str) -> Result<OrderState, OkxAccountGatewayError> {
    match value {
        "live" => Ok(OrderState::New),
        "partially_filled" => Ok(OrderState::PartiallyFilled),
        "filled" => Ok(OrderState::Filled),
        "canceled" | "mmp_canceled" => Ok(OrderState::Cancelled),
        "expired" => Ok(OrderState::Expired),
        "order_failed" => Ok(OrderState::Rejected),
        _ => Err(OkxAccountGatewayError::Account),
    }
}

fn positive_or_zero(
    row: &serde_json::Value,
    name: &str,
) -> Result<Decimal, OkxAccountGatewayError> {
    let value = decimal(row, name)?;
    (value >= Decimal::ZERO)
        .then_some(value)
        .ok_or(OkxAccountGatewayError::Account)
}

fn row_rule<'a>(
    row: &serde_json::Value,
    rules: &'a BTreeMap<String, AccountWideRule>,
) -> Result<&'a AccountWideRule, OkxAccountGatewayError> {
    if text(row, "instType")? != "SWAP" {
        return Err(OkxAccountGatewayError::Account);
    }
    rules
        .get(text(row, "instId")?)
        .ok_or(OkxAccountGatewayError::Account)
}

fn response_rows(body: &[u8]) -> Result<Vec<serde_json::Value>, OkxAccountGatewayError> {
    let root: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| OkxAccountGatewayError::Account)?;
    if root.get("code").and_then(serde_json::Value::as_str) != Some("0") {
        return Err(OkxAccountGatewayError::Account);
    }
    root.get("data")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .ok_or(OkxAccountGatewayError::Account)
}

fn text<'a>(row: &'a serde_json::Value, name: &str) -> Result<&'a str, OkxAccountGatewayError> {
    row.get(name)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty() && *value == value.trim())
        .ok_or(OkxAccountGatewayError::Account)
}

fn optional_text<'a>(
    row: &'a serde_json::Value,
    name: &str,
) -> Result<Option<&'a str>, OkxAccountGatewayError> {
    match row.get(name) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(value)) if value.is_empty() => Ok(None),
        Some(serde_json::Value::String(value)) if value == value.trim() => Ok(Some(value)),
        _ => Err(OkxAccountGatewayError::Account),
    }
}

fn decimal(row: &serde_json::Value, name: &str) -> Result<Decimal, OkxAccountGatewayError> {
    Decimal::from_str(text(row, name)?).map_err(|_| OkxAccountGatewayError::Account)
}

fn positive(row: &serde_json::Value, name: &str) -> Result<Decimal, OkxAccountGatewayError> {
    let value = decimal(row, name)?;
    if value > Decimal::ZERO {
        Ok(value)
    } else {
        Err(OkxAccountGatewayError::Account)
    }
}

fn optional_decimal(
    row: &serde_json::Value,
    name: &str,
) -> Result<Option<Decimal>, OkxAccountGatewayError> {
    match row.get(name).and_then(serde_json::Value::as_str) {
        None | Some("") | Some("0") => Ok(None),
        Some(value) => Decimal::from_str(value)
            .ok()
            .filter(|value| value.is_sign_positive())
            .ok_or(OkxAccountGatewayError::Account)
            .map(Some),
    }
}

fn position_side_for(
    mode: OkxPositionMode,
    raw: &str,
    contracts: Decimal,
) -> Result<PositionSide, OkxAccountGatewayError> {
    match (mode, raw) {
        (OkxPositionMode::Net, "net") => Ok(PositionSide::Net),
        (OkxPositionMode::LongShort, "long") if !contracts.is_sign_negative() => {
            Ok(PositionSide::Long)
        }
        (OkxPositionMode::LongShort, "short") if !contracts.is_sign_negative() => {
            Ok(PositionSide::Short)
        }
        _ => Err(OkxAccountGatewayError::Account),
    }
}

async fn fetch_private_state(
    config: &OkxConfig,
    credentials: &OkxCredentials,
    transport: &OkxHttpTransport,
    instrument: &OkxInstrument,
    trade_mode: OkxTradeMode,
    attempt_id: u64,
) -> Result<(OkxAccountProfile, Vec<OkxTimedPosition>), OkxAccountGatewayError> {
    let scope = OkxPrivateReadScope::new(
        config,
        instrument,
        OkxPositionMode::LongShort,
        trade_mode,
        attempt_id,
    )
    .map_err(|_| OkxAccountGatewayError::Account)?;
    let account_request =
        build_account_config_request(&scope).map_err(|_| OkxAccountGatewayError::Account)?;
    let timestamp = okx_timestamp(SystemTime::now()).map_err(|_| OkxAccountGatewayError::Clock)?;
    let account = transport
        .execute_read(credentials, &account_request, &timestamp)
        .await
        .map_err(OkxAccountGatewayError::Transport)?;
    let profile = parse_account_profile(&account.body, OkxPositionMode::LongShort)
        .map_err(|_| OkxAccountGatewayError::Account)?;
    if !profile.can_read() || !profile.can_trade() || profile.can_withdraw() {
        return Err(OkxAccountGatewayError::Permissions);
    }
    let positions_request =
        build_positions_request(&scope).map_err(|_| OkxAccountGatewayError::Account)?;
    let timestamp = okx_timestamp(SystemTime::now()).map_err(|_| OkxAccountGatewayError::Clock)?;
    let positions = transport
        .execute_read(credentials, &positions_request, &timestamp)
        .await
        .map_err(OkxAccountGatewayError::Transport)?;
    let positions = parse_positions(&positions.body, config, instrument, &profile)
        .map_err(|_| OkxAccountGatewayError::Account)?;
    Ok((profile, positions))
}

fn okx_recovery_outcome(
    command: &ExecutionCommand,
    found: Option<(String, OrderState, Option<LimitTimeInForce>)>,
) -> AccountRecoveryOutcome {
    let Some((venue_order_id, state, time_in_force)) = found else {
        return AccountRecoveryOutcome::still_unknown(command.command_id().clone());
    };
    if matches!(command, ExecutionCommand::PlaceLimit(_)) {
        if !recovery_limit_time_in_force_matches(command, time_in_force) {
            return AccountRecoveryOutcome::still_unknown(command.command_id().clone());
        }
    }
    if matches!(command, ExecutionCommand::Cancel(_)) {
        return match state {
            OrderState::Cancelled => {
                AccountRecoveryOutcome::accepted(command.command_id().clone(), venue_order_id)
            }
            OrderState::Filled | OrderState::Expired | OrderState::Rejected => {
                AccountRecoveryOutcome::rejected(
                    command.command_id().clone(),
                    "okx_target_terminal_without_cancel".to_owned(),
                )
            }
            _ => AccountRecoveryOutcome::still_unknown(command.command_id().clone()),
        };
    }
    if state == OrderState::Rejected {
        AccountRecoveryOutcome::rejected(
            command.command_id().clone(),
            "okx_order_rejected".to_owned(),
        )
    } else {
        AccountRecoveryOutcome::accepted(command.command_id().clone(), venue_order_id)
    }
}

fn recovery_limit_time_in_force_matches(
    command: &ExecutionCommand,
    observed: Option<LimitTimeInForce>,
) -> bool {
    match command {
        ExecutionCommand::PlaceLimit(order) => observed == Some(order.time_in_force),
        _ => true,
    }
}

fn map_transport_dispatch(error: OkxTransportError) -> AccountGatewayResult {
    match error {
        OkxTransportError::Configuration
        | OkxTransportError::Binding
        | OkxTransportError::BodyTooLarge
        | OkxTransportError::Clock => rejected("okx_pre_send_rejected"),
        _ => AccountGatewayResult::Unknown,
    }
}

fn rejected(reason: &str) -> AccountGatewayResult {
    AccountGatewayResult::Rejected {
        reason: reason.to_owned(),
    }
}

fn rejected_response(body: &[u8]) -> AccountGatewayResult {
    let value = serde_json::from_slice::<serde_json::Value>(body).ok();
    let envelope_code = value
        .as_ref()
        .and_then(|value| value.get("code"))
        .and_then(serde_json::Value::as_str)
        .filter(|value| valid_response_code(value));
    let row = value
        .as_ref()
        .and_then(|value| value.get("data"))
        .and_then(serde_json::Value::as_array)
        .and_then(|rows| rows.first());
    let code = row
        .and_then(|row| row.get("sCode"))
        .and_then(serde_json::Value::as_str)
        .filter(|value| valid_response_code(value));
    // This helper is reached only after the normal ACK parser failed. A row-level success code
    // therefore proves that a physical order may exist, but does not prove that the ACK identity
    // and timestamp were valid. Persist UNKNOWN so signed order readback, rather than a retry, is
    // the only path that can settle the command.
    if code == Some("0") {
        return AccountGatewayResult::Unknown;
    }
    let authoritative_code = code.or(envelope_code.filter(|value| *value != "0"));
    let Some(authoritative_code) = authoritative_code else {
        return AccountGatewayResult::Unknown;
    };
    let message = row
        .and_then(|row| row.get("sMsg"))
        .and_then(serde_json::Value::as_str)
        .filter(|value| {
            !value.is_empty() && value.len() <= 160 && !value.chars().any(char::is_control)
        })
        .unwrap_or("venue rejected request");
    AccountGatewayResult::Rejected {
        reason: format!("okx_{authoritative_code}: {message}"),
    }
}

fn valid_response_code(value: &str) -> bool {
    !value.is_empty() && value.len() <= 16 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn unix_ms() -> Result<u64, OkxAccountGatewayError> {
    let millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| OkxAccountGatewayError::Clock)?
        .as_millis();
    u64::try_from(millis).map_err(|_| OkxAccountGatewayError::Clock)
}

#[derive(Debug, thiserror::Error)]
pub enum OkxAccountGatewayError {
    #[error("OKX account gateway binding is invalid")]
    Binding,
    #[error("OKX account gateway credentials are unavailable")]
    Credentials,
    #[error("OKX account gateway runtime could not be created")]
    Runtime,
    #[error("OKX account gateway attempt identity overflowed")]
    Attempt,
    #[error("OKX public instrument or contract conversion is invalid")]
    Instrument,
    #[error("OKX API key lacks exact read/trade permissions or permits withdrawal")]
    Permissions,
    #[error("OKX account-mode or signed position preflight failed")]
    Account,
    #[error("OKX exact signed order readback failed")]
    Readback,
    #[error("OKX timestamp clock is invalid")]
    Clock,
    #[error("OKX transport failed")]
    Transport(#[source] OkxTransportError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_domain::domain::{CommandId, OrderOwner, OrderPurpose, Position};

    #[test]
    fn failed_ack_parse_with_row_success_stays_unknown() {
        for payload in [
            br#"{"code":"0","msg":"Order placed","data":[{"sCode":"0","sMsg":"Order placed"}]}"#
                .as_slice(),
            br#"{"code":"1","msg":"partial result","data":[{"sCode":"0","sMsg":"Order placed"}]}"#
                .as_slice(),
        ] {
            assert_eq!(rejected_response(payload), AccountGatewayResult::Unknown);
        }
    }

    #[test]
    fn explicit_nonzero_okx_code_is_terminal_rejection() {
        assert_eq!(
            rejected_response(
                br#"{"code":"1","msg":"failed","data":[{"sCode":"51000","sMsg":"parameter error"}]}"#,
            ),
            AccountGatewayResult::Rejected {
                reason: "okx_51000: parameter error".to_owned(),
            }
        );
    }

    #[test]
    fn malformed_ack_failure_stays_unknown() {
        assert_eq!(
            rejected_response(br#"{"code":"0","msg":"","data":[]}"#),
            AccountGatewayResult::Unknown
        );
    }

    #[test]
    fn timestamp_format_is_exact_utc_milliseconds() -> Result<(), Box<dyn std::error::Error>> {
        let timestamp =
            okx_timestamp(SystemTime::UNIX_EPOCH + Duration::from_millis(1_607_418_537_715))?;
        assert_eq!(timestamp, "2020-12-08T09:08:57.715Z");
        Ok(())
    }

    fn reduce(quantity: Decimal) -> Result<MarketReduceCommand, Box<dyn std::error::Error>> {
        Ok(MarketReduceCommand {
            command_id: CommandId::new("okx_reduce")?,
            client_order_id: CommandId::new("okx_reduce_client")?,
            owner: OrderOwner {
                strategy_instance_id: "grid1".to_owned(),
                run_id: "run1".to_owned(),
                exchange: "okx".to_owned(),
                account: "00000000-0000-4000-8000-000000000001".to_owned(),
                symbol: "BTC/USDT".parse()?,
                purpose: OrderPurpose::ExposureTakeProfit,
            },
            position_side: PositionSide::Long,
            side: OrderSide::Sell,
            quantity,
            risk_episode_id: CommandId::new("okx_episode")?,
            position_generation: 3,
        })
    }

    fn recovery_limit_command(
        time_in_force: LimitTimeInForce,
    ) -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
        Ok(ExecutionCommand::PlaceLimit(OrderCommand {
            command_id: CommandId::new("okx_recovery_limit")?,
            client_order_id: CommandId::new("okx_recovery_client")?,
            owner: OrderOwner {
                strategy_instance_id: "grid1".to_owned(),
                run_id: "run1".to_owned(),
                exchange: "okx".to_owned(),
                account: "00000000-0000-4000-8000-000000000001".to_owned(),
                symbol: "BTC/USDT".parse()?,
                purpose: OrderPurpose::Entry,
            },
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Price::new(Decimal::new(60_000, 0))?,
            time_in_force,
            reduce_only: false,
        }))
    }

    #[test]
    fn recovery_limit_policy_mismatch_or_absence_stays_unknown()
    -> Result<(), Box<dyn std::error::Error>> {
        let command = recovery_limit_command(LimitTimeInForce::Gtc)?;
        let accepted = okx_recovery_outcome(
            &command,
            Some((
                "7003".to_owned(),
                OrderState::New,
                Some(LimitTimeInForce::Gtc),
            )),
        );
        assert!(matches!(
            accepted.state(),
            AccountRecoveryState::Accepted { .. }
        ));
        for observed in [Some(LimitTimeInForce::PostOnly), None] {
            let outcome = okx_recovery_outcome(
                &command,
                Some(("7003".to_owned(), OrderState::New, observed)),
            );
            assert!(matches!(
                outcome.state(),
                AccountRecoveryState::StillUnknown
            ));
        }
        Ok(())
    }

    #[test]
    fn market_reduce_rejects_flat_crossing_and_wrong_leg_positions()
    -> Result<(), Box<dyn std::error::Error>> {
        let position = Position {
            symbol: "BTC/USDT".parse()?,
            side: PositionSide::Long,
            quantity: Decimal::ONE,
            entry_price: None,
            mark_price: None,
        };
        assert!(validate_market_reduce_against_position(&reduce(Decimal::ONE)?, &position).is_ok());
        assert!(
            validate_market_reduce_against_position(&reduce(Decimal::new(1001, 3))?, &position)
                .is_err()
        );
        let mut flat = position.clone();
        flat.quantity = Decimal::ZERO;
        assert!(validate_market_reduce_against_position(&reduce(Decimal::ONE)?, &flat).is_err());
        let mut wrong_leg = position;
        wrong_leg.side = PositionSide::Short;
        assert!(
            validate_market_reduce_against_position(&reduce(Decimal::ONE)?, &wrong_leg).is_err()
        );
        Ok(())
    }

    const INSTRUMENT: &[u8] = include_bytes!("../fixtures/linear-swap-instrument.json");
    const LIMIT_BBO: &[u8] = include_bytes!("../fixtures/limit-bbo.json");
    const ACCOUNT_CONFIG: &[u8] = include_bytes!("../fixtures/account-config.json");
    const FILLS: &[u8] = include_bytes!("../fixtures/fills-history-page.json");

    fn limit_config_and_instrument()
    -> Result<(OkxConfig, OkxInstrument), Box<dyn std::error::Error>> {
        let config = OkxConfig::for_binding(GatewayBinding::new(
            venue_gateway_api::VenueId::Okx,
            venue_gateway_api::GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)?;
        let instrument = parse_instrument(INSTRUMENT, &config, 7)?;
        Ok((config, instrument))
    }

    fn limit_bbo(config: &OkxConfig) -> crate::OkxHttpResponse {
        crate::OkxHttpResponse {
            binding: config.gateway_binding().clone(),
            instrument_generation: 7,
            received_at_ms: 10_000,
            body: bytes::Bytes::from_static(LIMIT_BBO),
        }
    }

    fn limit_intent(
        quote_delta: Decimal,
    ) -> Result<AccountLimitNormalizationIntent, Box<dyn std::error::Error>> {
        Ok(AccountLimitNormalizationIntent {
            command_id: CommandId::new("okx_limit")?,
            client_order_id: CommandId::new("okx_limit_client")?,
            owner: OrderOwner {
                strategy_instance_id: "grid1".to_owned(),
                run_id: "run1".to_owned(),
                exchange: "okx".to_owned(),
                account: "00000000-0000-4000-8000-000000000001".to_owned(),
                symbol: "BTC/USDT".parse()?,
                purpose: OrderPurpose::Entry,
            },
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quote_delta,
            reduce_only: false,
        })
    }

    fn fill_pages(
        config: &OkxConfig,
        instrument: &OkxInstrument,
        payload: Vec<u8>,
    ) -> Result<Vec<OkxRawPrivatePage>, Box<dyn std::error::Error>> {
        let scope = OkxPrivateReadScope::account_wide(
            config,
            instrument,
            OkxPositionMode::LongShort,
            OkxTradeMode::Cross,
            1,
        )?;
        let request = build_fills_request(&scope, 0, None)?;
        Ok(vec![OkxRawPrivatePage::new(
            &request,
            1_787_911_202_000,
            payload,
        )?])
    }

    #[test]
    fn limit_normalization_uses_contract_size_and_preserves_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, instrument) = limit_config_and_instrument()?;
        let bbo = parse_limit_bbo(&limit_bbo(&config), &config, &instrument, 10_010)?;
        let intent = limit_intent(Decimal::new(6001, 0))?;
        let ExecutionCommand::PlaceLimit(command) =
            normalize_limit_from_bbo(&config, &instrument, &intent, bbo)?
        else {
            return Err("expected limit".into());
        };
        assert_eq!(command.limit_price.value(), Decimal::new(600_000, 1));
        assert_eq!(command.quantity, Decimal::new(1, 1));
        assert_eq!(command.command_id, intent.command_id);
        assert_eq!(command.client_order_id, intent.client_order_id);
        assert_eq!(command.owner, intent.owner);
        Ok(())
    }

    #[test]
    fn limit_normalization_rejects_stale_empty_minimum_and_wrong_scope()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, instrument) = limit_config_and_instrument()?;
        assert!(parse_limit_bbo(&limit_bbo(&config), &config, &instrument, 11_001).is_err());
        let empty = crate::OkxHttpResponse {
            body: bytes::Bytes::from_static(br#"{"code":"0","data":[]}"#),
            ..limit_bbo(&config)
        };
        assert!(parse_limit_bbo(&empty, &config, &instrument, 10_010).is_err());
        let bbo = parse_limit_bbo(&limit_bbo(&config), &config, &instrument, 10_010)?;
        assert!(
            normalize_limit_from_bbo(
                &config,
                &instrument,
                &limit_intent(Decimal::new(5999, 0))?,
                bbo,
            )
            .is_err()
        );
        let mut wrong_scope = limit_intent(Decimal::new(6001, 0))?;
        wrong_scope.owner.symbol = "ETH/USDT".parse()?;
        assert!(normalize_limit_from_bbo(&config, &instrument, &wrong_scope, bbo).is_err());
        let mut wrong_account = limit_intent(Decimal::new(6001, 0))?;
        wrong_account.owner.account = "00000000-0000-4000-8000-000000000002".to_owned();
        assert!(normalize_limit_from_bbo(&config, &instrument, &wrong_account, bbo).is_err());
        let mut wrong_leg = limit_intent(Decimal::new(6001, 0))?;
        wrong_leg.position_side = PositionSide::Net;
        assert!(normalize_limit_from_bbo(&config, &instrument, &wrong_leg, bbo).is_err());
        Ok(())
    }

    #[test]
    fn priced_limit_keeps_user_price_policy_and_contract_cap()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, instrument) = limit_config_and_instrument()?;
        let priced = AccountPricedLimitIntent {
            intent: limit_intent(Decimal::new(6001, 0))?,
            limit_price: Price::new(Decimal::new(600_000, 1))?,
            time_in_force: LimitTimeInForce::Gtc,
            maximum_quantity: Some(Decimal::new(1, 1)),
        };
        let ExecutionCommand::PlaceLimit(command) =
            normalize_priced_limit(&config, &instrument, &priced)?
        else {
            return Err("expected limit".into());
        };
        assert_eq!(command.limit_price, priced.limit_price);
        assert_eq!(command.time_in_force, LimitTimeInForce::Gtc);
        assert_eq!(command.quantity, Decimal::new(1, 1));
        assert!(command.quantity <= priced.quantity_cap()?);
        assert!(command.quantity * command.limit_price.value() <= priced.intent.quote_delta);

        let mut off_tick = priced;
        off_tick.limit_price = Price::new(Decimal::new(6_000_001, 2))?;
        assert!(normalize_priced_limit(&config, &instrument, &off_tick).is_err());
        Ok(())
    }

    #[test]
    fn snapshot_fills_normalizes_contracts_and_uses_bill_cursor()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, instrument) = limit_config_and_instrument()?;
        let rules = parse_account_wide_rules(INSTRUMENT)?;
        let profile = parse_account_profile(ACCOUNT_CONFIG, OkxPositionMode::LongShort)?;
        let (fills, cursor) = snapshot_fills(
            &fill_pages(&config, &instrument, FILLS.to_vec())?,
            &rules,
            &profile,
            None,
        )?;
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].fill_id, "9002");
        assert_eq!(fills[0].quantity, Decimal::new(2, 1));
        assert_eq!(fills[0].execution_sequence, FieldState::Missing);
        assert_eq!(fills[0].exchange_time_ms, Some(1_787_911_201_400));
        assert_eq!(cursor, "okx-bill:9002");
        Ok(())
    }

    #[test]
    fn account_observation_uses_oldest_page_and_rejects_undated_collections()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, instrument) = limit_config_and_instrument()?;
        let mut pages = fill_pages(&config, &instrument, FILLS.to_vec())?;
        let mut later = pages[0].clone();
        later.received_at_ms += 60_001;
        pages.push(later);
        assert_eq!(oldest_page_observation(&pages)?, pages[0].received_at_ms);
        pages[0].received_at_ms = 0;
        assert!(oldest_page_observation(&pages).is_err());
        assert!(oldest_page_observation(&[]).is_err());
        Ok(())
    }

    #[test]
    fn snapshot_fills_rejects_missing_account_symbol_and_future_time()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, instrument) = limit_config_and_instrument()?;
        let rules = parse_account_wide_rules(INSTRUMENT)?;
        let profile = parse_account_profile(ACCOUNT_CONFIG, OkxPositionMode::LongShort)?;
        let unknown = String::from_utf8(FILLS.to_vec())?.replace("BTC-USDT-SWAP", "ETH-USDT-SWAP");
        assert!(
            snapshot_fills(
                &fill_pages(&config, &instrument, unknown.into_bytes())?,
                &rules,
                &profile,
                None,
            )
            .is_err()
        );
        let future = String::from_utf8(FILLS.to_vec())?.replace("1787911201400", "1787911203000");
        assert!(
            snapshot_fills(
                &fill_pages(&config, &instrument, future.into_bytes())?,
                &rules,
                &profile,
                None,
            )
            .is_err()
        );
        Ok(())
    }
}
